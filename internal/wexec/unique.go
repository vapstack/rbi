package wexec

import (
	"fmt"
	"slices"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
)

const uniqueUint64IntMapPoolMaxLen = 16 << 10

var uniqueUint64IntMapPool = pooled.Maps[uint64, int]{
	NewCap: 8,
	MaxLen: uniqueUint64IntMapPoolMaxLen,
	Cleanup: func(m map[uint64]int) {
		clear(m)
	},
}

var uniqueLeavingOuterPool = pooled.Maps[string, map[keycodec.IndexLookupKey]posting.List]{
	NewCap: 8,
	MaxLen: 256,
	Cleanup: func(m map[string]map[keycodec.IndexLookupKey]posting.List) {
		for _, inner := range m {
			uniqueLeavingInnerPool.Put(inner)
		}
		clear(m)
	},
}

var uniqueLeavingInnerPool = pooled.Maps[keycodec.IndexLookupKey, posting.List]{
	NewCap: 8,
	MaxLen: 4 << 10,
	Cleanup: func(m map[keycodec.IndexLookupKey]posting.List) {
		for _, ids := range m {
			ids.Release()
		}
		clear(m)
	},
}

var uniqueSeenOuterPool = pooled.Maps[string, map[keycodec.IndexLookupKey]uint64]{
	NewCap: 8,
	MaxLen: 256,
	Cleanup: func(m map[string]map[keycodec.IndexLookupKey]uint64) {
		for _, inner := range m {
			uniqueSeenInnerPool.Put(inner)
		}
		clear(m)
	},
}

var uniqueSeenInnerPool = pooled.Maps[keycodec.IndexLookupKey, uint64]{
	NewCap: 8,
	MaxLen: 4 << 10,
	Cleanup: func(m map[keycodec.IndexLookupKey]uint64) {
		clear(m)
	},
}

type UniqueContext struct {
	Schema          *schema.Runtime
	Current         func() *snapshot.View
	UniqueViolation error
}

type uniqueBatchCheckState struct {
	leaving map[string]map[keycodec.IndexLookupKey]posting.List
	seen    map[string]map[keycodec.IndexLookupKey]uint64
}

type uniqueLeavingTouch struct {
	field string
	key   keycodec.IndexLookupKey
	added bool
}

type uniqueSeenWrite struct {
	field string
	key   keycodec.IndexLookupKey
}

func markUniqueBatchLeaving(state uniqueBatchCheckState, field string, key keycodec.IndexLookupKey, idx uint64) bool {
	fm := state.leaving[field]
	if fm == nil {
		fm = uniqueLeavingInnerPool.Get()
		state.leaving[field] = fm
	}
	bm := fm[key]
	var added bool
	bm, added = bm.BuildAddedChecked(idx)
	if !added {
		return false
	}
	fm[key] = bm
	return true
}

func appendUniqueBatchLeavingTouch(state uniqueBatchCheckState, touched []uniqueLeavingTouch, field string, key keycodec.IndexLookupKey, idx uint64) []uniqueLeavingTouch {
	added := markUniqueBatchLeaving(state, field, key, idx)
	return append(touched, uniqueLeavingTouch{field: field, key: key, added: added})
}

func rollbackUniqueBatchLeaving(state uniqueBatchCheckState, touched []uniqueLeavingTouch, idx uint64) {
	for i := len(touched) - 1; i >= 0; i-- {
		t := touched[i]
		if !t.added {
			continue
		}
		fm := state.leaving[t.field]
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
		if len(fm) == 0 {
			delete(state.leaving, t.field)
			uniqueLeavingInnerPool.Put(fm)
		}
	}
}

func (ctx UniqueContext) checkBatchCandidateAndCollectSeen(
	state uniqueBatchCheckState,
	idx uint64,
	acc schema.IndexedFieldAccessor,
	ptr unsafe.Pointer,
	seenWrites []uniqueSeenWrite,
) ([]uniqueSeenWrite, error) {
	single, ok, isNil := acc.UniqueGetter(ptr)
	if !ok || isNil {
		return seenWrites, nil
	}

	if sm := state.seen[acc.Name]; sm != nil {
		if prev, ok := sm[single]; ok && prev != idx {
			return seenWrites, fmt.Errorf("%w: duplicate value for field %v within batch", ctx.UniqueViolation, acc.Name)
		}
	}

	ids := ctx.Current().FieldLookupPostingRetainedKey(acc.Name, single)
	if ids.IsEmpty() {
		return append(seenWrites, uniqueSeenWrite{field: acc.Name, key: single}), nil
	}

	var lv posting.List
	if fm := state.leaving[acc.Name]; fm != nil {
		lv = fm[single]
	}

	if lv.IsEmpty() {
		if ids.Cardinality() == 1 && ids.Contains(idx) {
			return append(seenWrites, uniqueSeenWrite{field: acc.Name, key: single}), nil
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

	return append(seenWrites, uniqueSeenWrite{field: acc.Name, key: single}), nil
}

func (ctx UniqueContext) checkBatchAppend(state uniqueBatchCheckState, idx uint64, oldVal, newVal unsafe.Pointer) error {
	if len(ctx.Schema.Unique) == 0 {
		return nil
	}
	var (
		touchedInline [8]uniqueLeavingTouch
		touched       = touchedInline[:0]
	)
	if oldVal != nil {
		if newVal == nil {
			for _, acc := range ctx.Schema.Unique {
				single, ok, isNil := acc.UniqueGetter(oldVal)
				if !ok || isNil {
					continue
				}
				touched = appendUniqueBatchLeavingTouch(state, touched, acc.Name, single, idx)
			}
		} else {
			for _, acc := range ctx.Schema.Unique {
				if !acc.Modified(oldVal, newVal) {
					continue
				}
				single, ok, isNil := acc.UniqueGetter(oldVal)
				if !ok || isNil {
					continue
				}
				touched = appendUniqueBatchLeavingTouch(state, touched, acc.Name, single, idx)
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
		for _, acc := range ctx.Schema.Unique {
			var err error
			seenWrites, err = ctx.checkBatchCandidateAndCollectSeen(state, idx, acc, newVal, seenWrites)
			if err != nil {
				rollbackUniqueBatchLeaving(state, touched, idx)
				return err
			}
		}
	} else {
		for _, acc := range ctx.Schema.Unique {
			if !acc.Modified(oldVal, newVal) {
				continue
			}
			var err error
			seenWrites, err = ctx.checkBatchCandidateAndCollectSeen(state, idx, acc, newVal, seenWrites)
			if err != nil {
				rollbackUniqueBatchLeaving(state, touched, idx)
				return err
			}
		}
	}

	for _, w := range seenWrites {
		sm := state.seen[w.field]
		if sm == nil {
			sm = uniqueSeenInnerPool.Get()
			state.seen[w.field] = sm
		}
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

func (ctx UniqueContext) checkOnWriteMulti(idxs []uint64, oldVals, newVals []unsafe.Pointer) error {
	if len(idxs) == 0 || len(ctx.Schema.Unique) == 0 {
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
		pos := uniqueUint64IntMapPool.Get(len(idxs))
		idxs, oldVals, newVals = collapseUniqueWriteMulti(idxs, oldVals, newVals, pos)
		uniqueUint64IntMapPool.Put(pos)
	}

	if newVals == nil {
		return nil
	}

	seen := uniqueSeenOuterPool.Get()
	var leaving map[string]map[keycodec.IndexLookupKey]posting.List
	current := ctx.Current()

	for i, idx := range idxs {
		oldVal := oldVals[i]
		newVal := newVals[i]
		if newVal == nil {
			continue
		}
		for _, acc := range ctx.Schema.Unique {
			if oldVal != nil && !acc.Modified(oldVal, newVal) {
				continue
			}
			needLeaving, err := ctx.checkBatchCandidateMulti(current, idx, newVal, acc, seen, leaving)
			if err != nil {
				uniqueSeenOuterPool.Put(seen)
				if leaving != nil {
					uniqueLeavingOuterPool.Put(leaving)
				}
				return err
			}
			if !needLeaving {
				continue
			}
			leaving = uniqueLeavingOuterPool.Get()
			for j, old := range oldVals {
				if old == nil {
					continue
				}
				next := newVals[j]
				if next == nil {
					for _, leavingAcc := range ctx.Schema.Unique {
						single, ok, isNil := leavingAcc.UniqueGetter(old)
						if !ok || isNil {
							continue
						}
						m := leaving[leavingAcc.Name]
						if m == nil {
							m = uniqueLeavingInnerPool.Get()
							leaving[leavingAcc.Name] = m
						}
						ids := m[single]
						ids = ids.BuildAdded(idxs[j])
						m[single] = ids
					}
					continue
				}
				for _, leavingAcc := range ctx.Schema.Unique {
					if !leavingAcc.Modified(old, next) {
						continue
					}
					single, ok, isNil := leavingAcc.UniqueGetter(old)
					if !ok || isNil {
						continue
					}
					m := leaving[leavingAcc.Name]
					if m == nil {
						m = uniqueLeavingInnerPool.Get()
						leaving[leavingAcc.Name] = m
					}
					ids := m[single]
					ids = ids.BuildAdded(idxs[j])
					m[single] = ids
				}
			}
			_, err = ctx.checkBatchCandidateMulti(current, idx, newVal, acc, seen, leaving)
			if err != nil {
				uniqueSeenOuterPool.Put(seen)
				uniqueLeavingOuterPool.Put(leaving)
				return err
			}
		}
	}
	uniqueSeenOuterPool.Put(seen)
	if leaving != nil {
		uniqueLeavingOuterPool.Put(leaving)
	}
	return nil
}

func (ctx UniqueContext) checkBatchCandidateMulti(
	current *snapshot.View,
	idx uint64,
	ptr unsafe.Pointer,
	acc schema.IndexedFieldAccessor,
	seen map[string]map[keycodec.IndexLookupKey]uint64,
	leaving map[string]map[keycodec.IndexLookupKey]posting.List,
) (bool, error) {

	single, ok, isNil := acc.UniqueGetter(ptr)
	if !ok || isNil {
		return false, nil
	}

	sm := seen[acc.Name]
	if sm == nil {
		sm = uniqueSeenInnerPool.Get()
		seen[acc.Name] = sm
	}
	if prev, ok := sm[single]; ok && prev != idx {
		return false, fmt.Errorf("%w: duplicate value for field %v within batch", ctx.UniqueViolation, acc.Name)
	}
	sm[single] = idx

	ids := current.FieldLookupPostingRetainedKey(acc.Name, single)
	if ids.IsEmpty() {
		return false, nil
	}

	if leaving == nil {
		if ids.Cardinality() == 1 && ids.Contains(idx) {
			return false, nil
		}
		return true, nil
	}

	var lv posting.List
	if fm := leaving[acc.Name]; fm != nil {
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

func (ctx UniqueContext) checkBatchCandidate(
	idx uint64,
	ptr unsafe.Pointer,
	acc schema.IndexedFieldAccessor,
	seen map[string]map[keycodec.IndexLookupKey]uint64,
	leaving map[string]map[keycodec.IndexLookupKey]posting.List,
) error {

	single, ok, isNil := acc.UniqueGetter(ptr)
	if !ok || isNil {
		return nil
	}

	sm := seen[acc.Name]
	if sm == nil {
		sm = uniqueSeenInnerPool.Get()
		seen[acc.Name] = sm
	}
	if prev, ok := sm[single]; ok && prev != idx {
		return fmt.Errorf("%w: duplicate value for field %v within batch", ctx.UniqueViolation, acc.Name)
	}
	sm[single] = idx

	ids := ctx.Current().FieldLookupPostingRetainedKey(acc.Name, single)
	if ids.IsEmpty() {
		return nil
	}

	var lv posting.List
	if fm := leaving[acc.Name]; fm != nil {
		lv = fm[single]
	}

	if lv.IsEmpty() {
		if ids.Cardinality() == 1 && ids.Contains(idx) {
			return nil
		}
		return fmt.Errorf("%w: value for field %v already exists", ctx.UniqueViolation, acc.Name)
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
		return fmt.Errorf("%w: value for field %v already exists", ctx.UniqueViolation, acc.Name)
	}
	return nil
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
		if err := b.unique.checkOnWriteMulti(att.uniqueIdxs, att.uniqueOldVals, att.uniqueNewVals); err != nil {
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
	uniqueState := uniqueBatchCheckState{
		leaving: uniqueLeavingOuterPool.Get(),
		seen:    uniqueSeenOuterPool.Get(),
	}
	defer uniqueLeavingOuterPool.Put(uniqueState.leaving)
	defer uniqueSeenOuterPool.Put(uniqueState.seen)

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
