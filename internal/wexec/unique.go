package wexec

import (
	"fmt"
	"slices"
	"unsafe"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
)

const (
	uniqueUint64IntMapPoolMaxLen = 16 << 10
	uniqueBatchFieldMaxLen       = 256
	uniqueBatchMapInitialCap     = 8
	uniqueBatchMapMaxLen         = 4 << 10
)

var uniqueUint64IntMapPool = pooled.Maps[uint64, int]{
	NewCap: 256,
	MaxLen: uniqueUint64IntMapPoolMaxLen,
}

type UniqueContext struct {
	Schema          *schema.Schema
	Current         func() *snapshot.View
	UniqueViolation error
}

type uniqueBatchCheckState struct {
	leaving      []map[keycodec.IndexLookupKey]posting.List
	seen         []map[keycodec.IndexLookupKey]uint64
	leavingReady bool
}

type uniqueLeavingTouch struct {
	pos   int
	key   keycodec.IndexLookupKey
	added bool
}

type uniqueSeenWrite struct {
	pos int
	key keycodec.IndexLookupKey
}

func (state *uniqueBatchCheckState) prepare(n int) {
	if cap(state.leaving) < n {
		next := make([]map[keycodec.IndexLookupKey]posting.List, n)
		copy(next, state.leaving[:cap(state.leaving)])
		state.leaving = next
	} else {
		state.leaving = state.leaving[:n]
	}
	if cap(state.seen) < n {
		next := make([]map[keycodec.IndexLookupKey]uint64, n)
		copy(next, state.seen[:cap(state.seen)])
		state.seen = next
	} else {
		state.seen = state.seen[:n]
	}
	state.leavingReady = false
}

func (state *uniqueBatchCheckState) cleanup() {
	for i := 0; i < len(state.leaving); i++ {
		m := state.leaving[i]
		if m == nil {
			continue
		}
		for _, ids := range m {
			ids.Release()
		}
		if len(m) > uniqueBatchMapMaxLen {
			state.leaving[i] = nil
		} else {
			clear(m)
		}
	}
	for i := 0; i < len(state.seen); i++ {
		m := state.seen[i]
		if m == nil {
			continue
		}
		if len(m) > uniqueBatchMapMaxLen {
			state.seen[i] = nil
		} else {
			clear(m)
		}
	}
	if cap(state.leaving) > uniqueBatchFieldMaxLen {
		state.leaving = nil
	} else {
		state.leaving = state.leaving[:0]
	}
	if cap(state.seen) > uniqueBatchFieldMaxLen {
		state.seen = nil
	} else {
		state.seen = state.seen[:0]
	}
	state.leavingReady = false
}

func (state *uniqueBatchCheckState) ensureLeaving(pos int) map[keycodec.IndexLookupKey]posting.List {
	m := state.leaving[pos]
	if m == nil {
		m = make(map[keycodec.IndexLookupKey]posting.List, uniqueBatchMapInitialCap)
		state.leaving[pos] = m
	}
	return m
}

func (state *uniqueBatchCheckState) ensureSeen(pos int) map[keycodec.IndexLookupKey]uint64 {
	m := state.seen[pos]
	if m == nil {
		m = make(map[keycodec.IndexLookupKey]uint64, uniqueBatchMapInitialCap)
		state.seen[pos] = m
	}
	return m
}

func markUniqueBatchLeaving(state *uniqueBatchCheckState, pos int, key keycodec.IndexLookupKey, idx uint64) bool {
	fm := state.ensureLeaving(pos)
	bm := fm[key]
	var added bool
	bm, added = bm.BuildAddedChecked(idx)
	if !added {
		return false
	}
	fm[key] = bm
	return true
}

func appendUniqueBatchLeavingTouch(state *uniqueBatchCheckState, touched []uniqueLeavingTouch, pos int, key keycodec.IndexLookupKey, idx uint64) []uniqueLeavingTouch {
	added := markUniqueBatchLeaving(state, pos, key, idx)
	return append(touched, uniqueLeavingTouch{pos: pos, key: key, added: added})
}

func rollbackUniqueBatchLeaving(state *uniqueBatchCheckState, touched []uniqueLeavingTouch, idx uint64) {
	for i := len(touched) - 1; i >= 0; i-- {
		t := touched[i]
		if !t.added {
			continue
		}
		fm := state.leaving[t.pos]
		if fm == nil {
			continue
		}
		ids := fm[t.key]
		if ids.IsEmpty() {
			continue
		}
		ids = ids.BuildRemoved(idx)
		if ids.IsEmpty() {
			delete(fm, t.key)
		} else {
			fm[t.key] = ids
		}
	}
}

func (ctx UniqueContext) checkBatchCandidateAndCollectSeen(
	state *uniqueBatchCheckState,
	pos int,
	idx uint64,
	acc schema.IndexedFieldAccessor,
	ptr unsafe.Pointer,
	seenWrites []uniqueSeenWrite,
) ([]uniqueSeenWrite, error) {
	single, ok, isNil := acc.UniqueGetter(ptr)
	if !ok || isNil {
		return seenWrites, nil
	}

	if sm := state.seen[pos]; sm != nil {
		if prev, ok := sm[single]; ok && prev != idx {
			return seenWrites, fmt.Errorf("%w: duplicate value for field %v within batch", ctx.UniqueViolation, acc.Name)
		}
	}

	ids := ctx.Current().FieldLookupPostingRetainedKey(acc.Name, single)
	if ids.IsEmpty() {
		return append(seenWrites, uniqueSeenWrite{pos: pos, key: single}), nil
	}

	var lv posting.List
	if fm := state.leaving[pos]; fm != nil {
		lv = fm[single]
	}

	if lv.IsEmpty() {
		if ids.Cardinality() == 1 && ids.Contains(idx) {
			return append(seenWrites, uniqueSeenWrite{pos: pos, key: single}), nil
		}
		return seenWrites, fmt.Errorf("%w: value for field %v already exists", ctx.UniqueViolation, acc.Name)
	}

	iter := ids.Iter()
	defer iter.Release()

	for iter.HasNext() {
		other := iter.Next()
		if other == idx {
			continue
		}
		if lv.Contains(other) {
			continue
		}
		return seenWrites, fmt.Errorf("%w: value for field %v already exists", ctx.UniqueViolation, acc.Name)
	}

	return append(seenWrites, uniqueSeenWrite{pos: pos, key: single}), nil
}

func (ctx UniqueContext) checkBatchAppend(state *uniqueBatchCheckState, idx uint64, oldVal, newVal unsafe.Pointer) error {
	unique := ctx.Schema.Unique
	if len(unique) == 0 {
		return nil
	}
	var (
		touchedInline [8]uniqueLeavingTouch
		touched       = touchedInline[:0]
	)
	if oldVal != nil {
		if newVal == nil {
			for pos := range unique {
				acc := unique[pos]
				single, ok, isNil := acc.UniqueGetter(oldVal)
				if !ok || isNil {
					continue
				}
				touched = appendUniqueBatchLeavingTouch(state, touched, pos, single, idx)
			}
		} else {
			for pos := range unique {
				acc := unique[pos]
				if !acc.Modified(oldVal, newVal) {
					continue
				}
				single, ok, isNil := acc.UniqueGetter(oldVal)
				if !ok || isNil {
					continue
				}
				touched = appendUniqueBatchLeavingTouch(state, touched, pos, single, idx)
			}
		}
	}

	if newVal == nil {
		return nil
	}

	var (
		seenWritesInline [8]uniqueSeenWrite
		seenWrites       = seenWritesInline[:0]
	)

	if oldVal == nil {
		for pos := range unique {
			acc := unique[pos]
			var err error
			seenWrites, err = ctx.checkBatchCandidateAndCollectSeen(state, pos, idx, acc, newVal, seenWrites)
			if err != nil {
				rollbackUniqueBatchLeaving(state, touched, idx)
				return err
			}
		}
	} else {
		for pos := range unique {
			acc := unique[pos]
			if !acc.Modified(oldVal, newVal) {
				continue
			}
			var err error
			seenWrites, err = ctx.checkBatchCandidateAndCollectSeen(state, pos, idx, acc, newVal, seenWrites)
			if err != nil {
				rollbackUniqueBatchLeaving(state, touched, idx)
				return err
			}
		}
	}

	for _, w := range seenWrites {
		sm := state.ensureSeen(w.pos)
		sm[w.key] = idx
	}
	return nil
}

func collapseUniqueWriteMulti(idxs []uint64, oldVals, newVals []unsafe.Pointer, pos map[uint64]int) ([]uint64, []unsafe.Pointer, []unsafe.Pointer) {
	n := 0
	for i, idx := range idxs {
		p := pos[idx]
		if p != 0 {
			if newVals != nil {
				newVals[p-1] = newVals[i]
			}
			continue
		}
		pos[idx] = n + 1
		if n != i {
			idxs[n] = idx
			oldVals[n] = oldVals[i]
			if newVals != nil {
				newVals[n] = newVals[i]
			}
		}
		n++
	}
	idxs = idxs[:n]
	oldVals = oldVals[:n]
	if newVals != nil {
		newVals = newVals[:n]
	}
	return idxs, oldVals, newVals
}

func (ctx UniqueContext) checkOnWriteMulti(state *uniqueBatchCheckState, idxs []uint64, oldVals, newVals []unsafe.Pointer) error {
	unique := ctx.Schema.Unique
	if len(idxs) == 0 || len(unique) == 0 {
		return nil
	}

	ordered := true
	for i := 1; i < len(idxs); i++ {
		if idxs[i] <= idxs[i-1] {
			ordered = false
			break
		}
	}
	if !ordered {
		pos := uniqueUint64IntMapPool.Get()
		idxs, oldVals, newVals = collapseUniqueWriteMulti(idxs, oldVals, newVals, pos)
		uniqueUint64IntMapPool.Put(pos)
	}

	if newVals == nil {
		return nil
	}

	current := ctx.Current()

	for i, idx := range idxs {
		oldVal := oldVals[i]
		newVal := newVals[i]
		if newVal == nil {
			continue
		}
		for pos := range unique {
			acc := unique[pos]
			if oldVal != nil && !acc.Modified(oldVal, newVal) {
				continue
			}
			needLeaving, err := ctx.checkBatchCandidateMulti(current, state, pos, idx, newVal, acc)
			if err != nil {
				return err
			}
			if !needLeaving {
				continue
			}
			state.leavingReady = true
			for j, old := range oldVals {
				if old == nil {
					continue
				}
				next := newVals[j]
				if next == nil {
					for leavingPos := range unique {
						leavingAcc := unique[leavingPos]
						single, ok, isNil := leavingAcc.UniqueGetter(old)
						if !ok || isNil {
							continue
						}
						m := state.ensureLeaving(leavingPos)
						ids := m[single]
						ids = ids.BuildAdded(idxs[j])
						m[single] = ids
					}
					continue
				}
				for leavingPos := range unique {
					leavingAcc := unique[leavingPos]
					if !leavingAcc.Modified(old, next) {
						continue
					}
					single, ok, isNil := leavingAcc.UniqueGetter(old)
					if !ok || isNil {
						continue
					}
					m := state.ensureLeaving(leavingPos)
					ids := m[single]
					ids = ids.BuildAdded(idxs[j])
					m[single] = ids
				}
			}
			_, err = ctx.checkBatchCandidateMulti(current, state, pos, idx, newVal, acc)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (ctx UniqueContext) checkBatchCandidateMulti(
	current *snapshot.View,
	state *uniqueBatchCheckState,
	pos int,
	idx uint64,
	ptr unsafe.Pointer,
	acc schema.IndexedFieldAccessor,
) (bool, error) {

	single, ok, isNil := acc.UniqueGetter(ptr)
	if !ok || isNil {
		return false, nil
	}

	sm := state.ensureSeen(pos)
	if prev, ok := sm[single]; ok && prev != idx {
		return false, fmt.Errorf("%w: duplicate value for field %v within batch", ctx.UniqueViolation, acc.Name)
	}
	sm[single] = idx

	ids := current.FieldLookupPostingRetainedKey(acc.Name, single)
	if ids.IsEmpty() {
		return false, nil
	}

	if !state.leavingReady {
		if ids.Cardinality() == 1 && ids.Contains(idx) {
			return false, nil
		}
		return true, nil
	}

	var lv posting.List
	if fm := state.leaving[pos]; fm != nil {
		lv = fm[single]
	}

	if lv.IsEmpty() {
		if ids.Cardinality() == 1 && ids.Contains(idx) {
			return false, nil
		}
		return false, fmt.Errorf("%w: value for field %v already exists", ctx.UniqueViolation, acc.Name)
	}

	iter := ids.Iter()
	defer iter.Release()

	for iter.HasNext() {
		other := iter.Next()
		if other == idx {
			continue
		}
		if lv.Contains(other) {
			continue
		}
		return false, fmt.Errorf("%w: value for field %v already exists", ctx.UniqueViolation, acc.Name)
	}
	return false, nil
}

func (b *Batcher) filterAccepted(att *attemptState, atomicAll bool) error {
	att.accepted = att.prepared
	att.acceptedSnapshots = att.preparedSnapshots
	if b.unique.Schema == nil || len(b.unique.Schema.Unique) == 0 {
		return nil
	}

	if atomicAll {
		att.uniqueIdxs = att.uniqueIdxs[:len(att.prepared)]
		att.uniqueOldVals = att.uniqueOldVals[:len(att.prepared)]
		att.uniqueNewVals = att.uniqueNewVals[:len(att.prepared)]
		for i := range att.prepared {
			op := att.prepared[i]
			att.uniqueIdxs[i] = op.idx
			att.uniqueOldVals[i] = op.oldVal
			att.uniqueNewVals[i] = op.newVal
		}
		if err := b.unique.checkOnWriteMulti(&att.uniqueState, att.uniqueIdxs, att.uniqueOldVals, att.uniqueNewVals); err != nil {
			if b.sched.stats.Enabled {
				b.sched.stats.UniqueRejected.Add(1)
			}
			for _, op := range att.prepared {
				op.req.Err = err
			}
			return err
		}
		return nil
	}

	if cap(att.accepted) < len(att.prepared) {
		att.accepted = slices.Grow(att.accepted, len(att.prepared))
	}
	if cap(att.acceptedSnapshots) < len(att.preparedSnapshots) {
		att.acceptedSnapshots = slices.Grow(att.acceptedSnapshots, len(att.preparedSnapshots))
	}
	att.accepted = att.accepted[:0]
	att.acceptedSnapshots = att.acceptedSnapshots[:0]
	uniqueState := &att.uniqueState

	for i := range att.prepared {
		op := att.prepared[i]
		if err := b.unique.checkBatchAppend(uniqueState, op.idx, op.oldVal, op.newVal); err != nil {
			if b.sched.stats.Enabled {
				b.sched.stats.UniqueRejected.Add(1)
			}
			op.req.Err = err
			if b.strKey && op.idxNew && op.oldVal == nil && op.newVal != nil {
				b.strMap.RollbackCreated(op.req.id.String(), op.idx)
			}
			continue
		}
		att.accepted = append(att.accepted, op)
		att.acceptedSnapshots = append(att.acceptedSnapshots, att.preparedSnapshots[i])
	}
	return nil
}
