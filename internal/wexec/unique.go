package wexec

import (
	"fmt"
	"unsafe"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/rbierrors"
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
	Schema  *schema.Schema
	Current func() *snapshot.View
}

type uniqueBatchCheckState struct {
	leaving      []map[keycodec.IndexLookupKey]posting.List
	seen         []map[keycodec.IndexLookupKey]uint64
	base         *uniqueBatchCheckState
	leavingReady bool
	leavingAny   bool
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
	state.leavingAny = false
	state.base = nil
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
	state.leavingAny = false
	state.base = nil
}

func (state *uniqueBatchCheckState) hasLeaving(pos int, key keycodec.IndexLookupKey, idx uint64) bool {
	if fm := state.leaving[pos]; fm != nil {
		if ids := fm[key]; !ids.IsEmpty() && ids.Contains(idx) {
			return true
		}
	}
	if state.base != nil {
		return state.base.hasLeaving(pos, key, idx)
	}
	return false
}

func (state *uniqueBatchCheckState) seenOwner(pos int, key keycodec.IndexLookupKey) (uint64, bool, bool) {
	if sm := state.seen[pos]; sm != nil {
		prev, ok := sm[key]
		if ok {
			return prev, true, false
		}
	}
	if state.base != nil {
		prev, ok, needLeaving := state.base.seenOwner(pos, key)
		if ok {
			if fm := state.leaving[pos]; fm != nil {
				if ids := fm[key]; !ids.IsEmpty() && ids.Contains(prev) {
					// Current departures shadow base arrivals so a frame can
					// replace a unique value accepted by an earlier unit.
					return 0, false, false
				}
			}
			return prev, true, needLeaving || !state.leavingReady
		}
	}
	return 0, false, false
}

func (state *uniqueBatchCheckState) hasDepartures() bool {
	return state.leavingAny || state.base != nil && state.base.hasDepartures()
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

func markUniqueBatchDeparture(state *uniqueBatchCheckState, pos int, key keycodec.IndexLookupKey, idx uint64) {
	if sm := state.seen[pos]; sm != nil {
		if prev, ok := sm[key]; ok && prev == idx {
			delete(sm, key)
		}
	}
	markUniqueBatchLeaving(state, pos, key, idx)
	state.leavingAny = true
}

func removeUniqueBatchLeaving(state *uniqueBatchCheckState, pos int, key keycodec.IndexLookupKey, idx uint64) {
	if fm := state.leaving[pos]; fm != nil {
		if bm := fm[key]; !bm.IsEmpty() {
			bm = bm.BuildRemoved(idx)
			if bm.IsEmpty() {
				delete(fm, key)
			} else {
				fm[key] = bm
			}
		}
	}
}

func markUniqueBatchArrival(state *uniqueBatchCheckState, pos int, key keycodec.IndexLookupKey, idx uint64) {
	removeUniqueBatchLeaving(state, pos, key, idx)
	state.ensureSeen(pos)[key] = idx
}

func (state *uniqueBatchCheckState) mergeFrom(src *uniqueBatchCheckState) {
	if src.leavingReady {
		state.leavingReady = true
	}
	if src.leavingAny {
		state.leavingAny = true
	}
	for pos, leaving := range src.leaving {
		if leaving == nil {
			continue
		}
		if sm := state.seen[pos]; sm != nil {
			for key, ids := range leaving {
				if prev, ok := sm[key]; ok && ids.Contains(prev) {
					delete(sm, key)
				}
			}
		}
		dst := state.leaving[pos]
		if dst == nil {
			// Move the map whole; its posting.List values must stay owned by one
			// cleanup path.
			state.leaving[pos] = leaving
			src.leaving[pos] = nil
			continue
		}
		for key, ids := range leaving {
			bm := dst[key]
			if bm.IsEmpty() {
				dst[key] = ids
				delete(leaving, key)
				continue
			}
			iter := ids.Iter()
			for iter.HasNext() {
				bm, _ = bm.BuildAddedChecked(iter.Next())
			}
			iter.Release()
			ids.Release()
			dst[key] = bm
			delete(leaving, key)
		}
	}
	for pos, seen := range src.seen {
		if seen == nil {
			continue
		}
		if fm := state.leaving[pos]; fm != nil {
			for key, idx := range seen {
				if bm := fm[key]; !bm.IsEmpty() {
					bm = bm.BuildRemoved(idx)
					if bm.IsEmpty() {
						delete(fm, key)
					} else {
						fm[key] = bm
					}
				}
			}
		}
		dst := state.seen[pos]
		if dst == nil {
			// Move the map whole for the same reason as leaving: no split owner
			// for entries that survive into the accepted state.
			state.seen[pos] = seen
			src.seen[pos] = nil
			continue
		}
		for key, idx := range seen {
			dst[key] = idx
		}
	}
}

func (ctx UniqueContext) collectUniqueBatchDepartures(state *uniqueBatchCheckState, idxs []uint64, oldVals, newVals []unsafe.Pointer) {
	if state.leavingReady {
		return
	}
	unique := ctx.Schema.Unique
	for j, old := range oldVals {
		if old == nil {
			continue
		}
		next := newVals[j]
		if next == nil {
			for pos := range unique {
				acc := unique[pos]
				single, ok, isNil := acc.UniqueGetter(old)
				if !ok || isNil {
					continue
				}
				markUniqueBatchDeparture(state, pos, single, idxs[j])
			}
			continue
		}
		for pos := range unique {
			acc := unique[pos]
			if !acc.Modified(old, next) {
				continue
			}
			single, ok, isNil := acc.UniqueGetter(old)
			if !ok || isNil {
				continue
			}
			markUniqueBatchDeparture(state, pos, single, idxs[j])
		}
	}
	state.leavingReady = true
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
			// The no-departure fast path uses cardinality only. If it reports a
			// possible collision, pay for the leaving-aware posting scan once.
			ctx.collectUniqueBatchDepartures(state, idxs, oldVals, newVals)
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

	if prev, ok, needLeaving := state.seenOwner(pos, single); ok && prev != idx {
		if needLeaving {
			return true, nil
		}
		return false, fmt.Errorf("%w: duplicate value for field %v within batch", rbierrors.ErrUniqueViolation, acc.Name)
	}
	markUniqueBatchArrival(state, pos, single, idx)

	ids := current.FieldLookupPostingRetainedKey(acc.Name, single)
	if ids.IsEmpty() {
		return false, nil
	}

	if !state.hasDepartures() {
		if ids.Cardinality() == 1 && ids.Contains(idx) {
			return false, nil
		}
		if state.leavingReady {
			return false, fmt.Errorf("%w: value for field %v already exists", rbierrors.ErrUniqueViolation, acc.Name)
		}
		return true, nil
	}

	iter := ids.Iter()
	defer iter.Release()

	for iter.HasNext() {
		other := iter.Next()
		if other == idx {
			continue
		}
		if state.hasLeaving(pos, single, other) {
			continue
		}
		if !state.leavingReady {
			return true, nil
		}
		return false, fmt.Errorf("%w: value for field %v already exists", rbierrors.ErrUniqueViolation, acc.Name)
	}
	return false, nil
}

func (b *Executor) filterAccepted(att *attemptState) error {
	att.accepted = att.prepared
	att.acceptedSnapshots = att.preparedSnapshots
	if b.unique.Schema == nil || len(b.unique.Schema.Unique) == 0 {
		return nil
	}

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
		if b.stats.Enabled {
			b.stats.UniqueRejected.Add(1)
		}
		assignPreparedErr(att.prepared, err)
		return err
	}
	return nil
}
