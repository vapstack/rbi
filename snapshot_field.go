package rbi

import (
	"slices"
	"unsafe"

	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

type snapshotFieldOverlayState struct {
	index   map[string]posting.List
	fixed   map[uint64]posting.List
	lengths map[uint32]posting.List
	nils    map[string]posting.List
	changed bool
}

type snapshotFieldInsertState struct {
	index   map[string]uint32
	fixed   map[uint64]uint32
	nils    map[string]uint32
	arena   *insertPostingAccumArena
	lengths *lenFieldPostingDelta
	changed bool

	indexHint   int
	fixedHint   int
	nilsHint    int
	lengthsHint int
}

var snapshotFieldInsertStateSlicePool = pooled.Slices[snapshotFieldInsertState]{
	Clear: true,
}

type fieldWriteScratch struct {
	strings []string
	fixed   []uint64
	ok      bool
	isNil   bool
	length  int
}

type snapshotFieldBatchState struct {
	index   map[string]uint32
	fixed   map[uint64]uint32
	nils    map[string]uint32
	arena   *batchPostingAccumArena
	lengths *lenFieldPostingDelta
	changed bool
	old     fieldWriteScratch
	new     fieldWriteScratch
}

var snapshotFieldBatchStateSlicePool = pooled.Slices[snapshotFieldBatchState]{
	Clear: true,
}

func (s *fieldWriteScratch) reset() {
	if s == nil {
		return
	}
	s.strings = s.strings[:0]
	s.fixed = s.fixed[:0]
	s.ok = false
	s.isNil = false
	s.length = 0
}

func (s *fieldWriteScratch) setNil() {
	if s == nil {
		return
	}
	s.ok = true
	s.isNil = true
}

func (s *fieldWriteScratch) setLen(length int) {
	if s == nil {
		return
	}
	s.ok = true
	s.length = length
}

func (s *fieldWriteScratch) addString(key string) {
	if s == nil {
		return
	}
	s.ok = true
	s.strings = append(s.strings, key)
}

func (s *fieldWriteScratch) addFixed(key uint64) {
	if s == nil {
		return
	}
	s.ok = true
	s.fixed = append(s.fixed, key)
}

func (s *fieldWriteScratch) sortForField(f *field) {
	if s == nil || f == nil || !f.Slice {
		return
	}
	if f.KeyKind == fieldWriteKeysOrderedU64 {
		if len(s.fixed) > 1 {
			slices.Sort(s.fixed)
		}
		return
	}
	if len(s.strings) > 1 {
		slices.Sort(s.strings)
	}
}

type snapshotOverlayWriteSink struct {
	state *snapshotFieldOverlayState
	idx   uint64
}

func (s snapshotOverlayWriteSink) setNil() {
	if s.state == nil {
		return
	}
	s.state.nils = addFieldPostingListHint(s.state.nils, nilIndexEntryKey, s.idx, 0)
	s.state.changed = true
}

func (s snapshotOverlayWriteSink) setLen(length int) {
	if s.state == nil || length < 0 {
		return
	}
	if s.state.lengths == nil {
		s.state.lengths = make(map[uint32]posting.List, 8)
	}
	ln := uint32(length)
	ids := s.state.lengths[ln]
	ids = ids.BuildAdded(s.idx)
	s.state.lengths[ln] = ids
	s.state.changed = true
}

func (s snapshotOverlayWriteSink) addString(key string) {
	if s.state == nil {
		return
	}
	s.state.index = addFieldPostingListHint(s.state.index, key, s.idx, 0)
	s.state.changed = true
}

func (s snapshotOverlayWriteSink) addFixed(key uint64) {
	if s.state == nil {
		return
	}
	s.state.fixed = addFixedFieldPostingListHint(s.state.fixed, key, s.idx, 0)
	s.state.changed = true
}

type snapshotInsertWriteSink struct {
	state             *snapshotFieldInsertState
	idx               uint64
	useZeroComplement bool
}

func (s snapshotInsertWriteSink) setNil() {
	if s.state == nil {
		return
	}
	s.state.nils = addInsertPostingAccum(s.state.nils, &s.state.arena, nilIndexEntryKey, s.idx, s.state.nilsHint)
	s.state.changed = true
}

func (s snapshotInsertWriteSink) setLen(length int) {
	if s.state == nil {
		return
	}
	var changed bool
	s.state.lengths, changed = collectFieldBatchLenDiff(s.state.lengths, s.idx, batchLenDiff{
		newExists: true,
		newLen:    length,
	}, s.useZeroComplement)
	if changed {
		s.state.changed = true
	}
}

func (s snapshotInsertWriteSink) addString(key string) {
	if s.state == nil {
		return
	}
	s.state.index = addInsertPostingAccum(s.state.index, &s.state.arena, key, s.idx, s.state.indexHint)
	s.state.changed = true
}

func (s snapshotInsertWriteSink) addFixed(key uint64) {
	if s.state == nil {
		return
	}
	s.state.fixed = addFixedInsertPostingAccum(s.state.fixed, &s.state.arena, key, s.idx, s.state.fixedHint)
	s.state.changed = true
}

func (acc indexedFieldAccessor) collectSnapshotOverlayValue(ptr unsafe.Pointer, idx uint64, state *snapshotFieldOverlayState) {
	if state == nil || acc.writeOverlay == nil {
		return
	}
	acc.writeOverlay(ptr, snapshotOverlayWriteSink{state: state, idx: idx})
}

func (acc indexedFieldAccessor) collectSnapshotInsertValue(
	ptr unsafe.Pointer,
	idx uint64,
	useZeroComplement bool,
	state *snapshotFieldInsertState,
) {
	if state == nil || acc.writeInsert == nil {
		return
	}
	acc.writeInsert(ptr, snapshotInsertWriteSink{
		state:             state,
		idx:               idx,
		useZeroComplement: useZeroComplement,
	})
}

func initSnapshotFieldInsertStateHints(
	states *pooled.SliceBuf[snapshotFieldInsertState],
	access []indexedFieldAccessor,
	prev *indexSnapshot,
	batchHint int,
) {
	if prev == nil || batchHint <= 0 {
		return
	}
	for i := range access {
		acc := access[i]
		state := states.GetPtr(i)
		var indexHint int
		if prev.index != nil && acc.ordinal < prev.index.Len() {
			indexHint = snapshotFieldStorageHint(prev.index.Get(acc.ordinal), batchHint)
		}
		if acc.field != nil && acc.field.KeyKind == fieldWriteKeysOrderedU64 {
			state.fixedHint = indexHint
		} else {
			state.indexHint = indexHint
		}
		if prev.nilIndex != nil && acc.ordinal < prev.nilIndex.Len() {
			state.nilsHint = snapshotFieldStorageHint(prev.nilIndex.Get(acc.ordinal), batchHint)
		}
		if acc.field != nil && acc.field.Slice && prev.lenIndex != nil && acc.ordinal < prev.lenIndex.Len() {
			state.lengthsHint = snapshotFieldStorageHint(prev.lenIndex.Get(acc.ordinal), batchHint)
		}
	}
}

func snapshotFieldStorageHint(base fieldIndexStorage, batchHint int) int {
	if batchHint <= 0 {
		return 0
	}
	hint := base.keyCount()
	if hint < 8 {
		hint = 8
	}
	if hint > batchHint {
		hint = batchHint
	}
	return hint
}

func (acc indexedFieldAccessor) materializeSnapshotOverlayStorageOwned(state *snapshotFieldOverlayState) fieldIndexStorage {
	if state == nil {
		return fieldIndexStorage{}
	}
	if acc.field != nil && acc.field.KeyKind == fieldWriteKeysOrderedU64 {
		fixed := state.fixed
		state.fixed = nil
		return newRegularFieldIndexStorageFromFixedPostingMapOwned(fixed)
	}
	i := state.index
	state.index = nil
	return newRegularFieldIndexStorageFromPostingMapOwned(i, false)
}

func (acc indexedFieldAccessor) materializeSnapshotOverlayNilStorageOwned(state *snapshotFieldOverlayState) fieldIndexStorage {
	if state == nil {
		return fieldIndexStorage{}
	}
	nils := state.nils
	state.nils = nil
	return newFlatFieldIndexStorageFromPostingMapOwned(nils, false)
}

func (acc indexedFieldAccessor) mergeSnapshotInsertStorageOwned(
	base fieldIndexStorage,
	state *snapshotFieldInsertState,
	allowChunk bool,
) fieldIndexStorage {
	if state == nil {
		return base
	}
	if acc.field != nil && acc.field.KeyKind == fieldWriteKeysOrderedU64 {
		fixed := state.fixed
		state.fixed = nil
		return mergeInsertOnlyFixedFieldStorageOwned(base, fixed, state.arena, allowChunk)
	}
	i := state.index
	state.index = nil
	return mergeInsertOnlyFieldStorageOwned(base, i, state.arena, false, allowChunk)
}

func (acc indexedFieldAccessor) mergeSnapshotInsertNilStorageOwned(base fieldIndexStorage, state *snapshotFieldInsertState) fieldIndexStorage {
	if state == nil {
		return base
	}
	nils := state.nils
	state.nils = nil
	return mergeInsertOnlyFieldStorageOwned(base, nils, state.arena, false, false)
}

func (state *snapshotFieldInsertState) releaseOwned() {
	insertPostingMapPool.Put(state.index)
	fixedInsertPostingMapPool.Put(state.fixed)
	insertPostingMapPool.Put(state.nils)
	state.index = nil
	state.fixed = nil
	state.nils = nil
	releaseInsertPostingAccumArena(state.arena)
	state.arena = nil
	if state.lengths != nil {
		releaseLenFieldPostingDeltaOwned(state.lengths)
		state.lengths = nil
	}
}

func (acc indexedFieldAccessor) collectSnapshotBatchDiff(
	idx uint64,
	oldPtr, newPtr unsafe.Pointer,
	useZeroComplement bool,
	state *snapshotFieldBatchState,
) {
	if state == nil || acc.writeScratch == nil {
		return
	}

	state.old.reset()
	state.new.reset()
	if oldPtr != nil {
		acc.writeScratch(oldPtr, &state.old)
	}
	if newPtr != nil {
		acc.writeScratch(newPtr, &state.new)
	}
	state.old.sortForField(acc.field)
	state.new.sortForField(acc.field)

	if state.old.isNil != state.new.isNil {
		if state.old.isNil {
			state.nils = addFieldBatchPostingAccum(state.nils, &state.arena, nilIndexEntryKey, idx, false, 0)
		}
		if state.new.isNil {
			state.nils = addFieldBatchPostingAccum(state.nils, &state.arena, nilIndexEntryKey, idx, true, 0)
		}
		state.changed = true
	}

	oldOK := state.old.ok && !state.old.isNil
	newOK := state.new.ok && !state.new.isNil

	var changed bool
	if acc.field != nil && acc.field.KeyKind == fieldWriteKeysOrderedU64 {
		if acc.field.Slice {
			var oldMulti, newMulti []uint64
			if oldOK {
				oldMulti = state.old.fixed
			}
			if newOK {
				newMulti = state.new.fixed
			}
			state.fixed, changed = collectFixedFieldBatchPostingDiff(state.fixed, &state.arena, idx, oldMulti, newMulti)
		} else {
			var oldSingle, newSingle uint64
			if oldOK && len(state.old.fixed) > 0 {
				oldSingle = state.old.fixed[0]
			}
			if newOK && len(state.new.fixed) > 0 {
				newSingle = state.new.fixed[0]
			}
			state.fixed, changed = collectScalarFixedFieldBatchPostingDiff(
				state.fixed,
				&state.arena,
				idx,
				oldOK,
				oldSingle,
				newOK,
				newSingle,
			)
		}
	} else if acc.field != nil && acc.field.Slice {
		var oldMulti, newMulti []string
		if oldOK {
			oldMulti = state.old.strings
		}
		if newOK {
			newMulti = state.new.strings
		}
		state.index, changed = collectFieldBatchPostingDiff(state.index, &state.arena, idx, oldMulti, newMulti)
	} else {
		var oldSingle, newSingle string
		if oldOK && len(state.old.strings) > 0 {
			oldSingle = state.old.strings[0]
		}
		if newOK && len(state.new.strings) > 0 {
			newSingle = state.new.strings[0]
		}
		state.index, changed = collectScalarFieldBatchPostingDiff(
			state.index,
			&state.arena,
			idx,
			oldOK,
			oldSingle,
			newOK,
			newSingle,
		)
	}
	if changed {
		state.changed = true
	}

	if acc.field == nil || !acc.field.Slice {
		return
	}

	state.lengths, changed = collectFieldBatchLenDiff(state.lengths, idx, batchLenDiff{
		oldExists: oldPtr != nil && state.old.ok,
		oldLen:    state.old.length,
		newExists: newPtr != nil && state.new.ok,
		newLen:    state.new.length,
	}, useZeroComplement)
	if changed {
		state.changed = true
	}
}

func (acc indexedFieldAccessor) applySnapshotBatchStorageOwned(
	base fieldIndexStorage,
	state *snapshotFieldBatchState,
	allowChunk bool,
) fieldIndexStorage {
	if state == nil {
		return base
	}
	if acc.field != nil && acc.field.KeyKind == fieldWriteKeysOrderedU64 {
		deltas := state.fixed
		state.fixed = nil
		return applyFixedFieldPostingDiffStorageOwned(base, deltas, state.arena, allowChunk)
	}
	deltas := state.index
	state.index = nil
	return applyFieldPostingDiffStorageOwned(base, deltas, state.arena, false, allowChunk)
}

func (acc indexedFieldAccessor) applySnapshotBatchNilStorageOwned(base fieldIndexStorage, state *snapshotFieldBatchState) fieldIndexStorage {
	if state == nil {
		return base
	}
	deltas := state.nils
	state.nils = nil
	return applyFieldPostingDiffStorageOwned(base, deltas, state.arena, false, false)
}

func (state *snapshotFieldBatchState) releaseOwned() {
	batchPostingAccumMapPool.Put(state.index)
	fixedBatchPostingAccumMapPool.Put(state.fixed)
	batchPostingAccumMapPool.Put(state.nils)
	state.index = nil
	state.fixed = nil
	state.nils = nil
	releaseBatchPostingAccumArena(state.arena)
	state.arena = nil
	if state.lengths != nil {
		releaseLenFieldPostingDeltaOwned(state.lengths)
		state.lengths = nil
	}
}
