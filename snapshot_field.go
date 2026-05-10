package rbi

import (
	"slices"
	"unsafe"

	"github.com/vapstack/rbi/internal/indexdata"
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
	arena   *indexdata.PostingAddArena
	lengths *indexdata.LenPostingDiff
	changed bool

	indexHint   int
	fixedHint   int
	nilsHint    int
	lengthsHint int
}

const (
	snapshotFieldStateSlicePoolMaxCap = 64 << 10
)

var snapshotFieldInsertStateSlicePool = pooled.NewSlicePool[snapshotFieldInsertState](
	snapshotFieldStateSlicePoolMaxCap,
	pooled.NoClear,
	cleanupSnapshotFieldInsertStateSlice,
)

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
	arena   *indexdata.PostingDiffArena
	lengths *indexdata.LenPostingDiff
	changed bool
	old     fieldWriteScratch
	new     fieldWriteScratch
}

var snapshotFieldBatchStateSlicePool = pooled.NewSlicePool[snapshotFieldBatchState](
	snapshotFieldStateSlicePoolMaxCap,
	pooled.NoClear,
	cleanupSnapshotFieldBatchStateSlice,
)

func (s *fieldWriteScratch) reset() {
	if s.strings != nil {
		if cap(s.strings) > 64<<10 {
			s.strings = nil
		} else {
			clear(s.strings)
			s.strings = s.strings[:0]
		}
	}
	if s.fixed != nil {
		if cap(s.fixed) > 64<<10 {
			s.fixed = nil
		} else {
			s.fixed = s.fixed[:0]
		}
	}
	s.ok = false
	s.isNil = false
	s.length = 0
}

func (s *fieldWriteScratch) discard() {
	s.strings = nil
	s.fixed = nil
	s.ok = false
	s.isNil = false
	s.length = 0
}

func (s *fieldWriteScratch) setNil() {
	s.ok = true
	s.isNil = true
}

func (s *fieldWriteScratch) setLen(length int) {
	s.ok = true
	s.length = length
}

func (s *fieldWriteScratch) addString(key string) {
	if s.strings == nil {
		s.strings = make([]string, 0, 1)
	}
	s.ok = true
	s.strings = append(s.strings, key)
}

func (s *fieldWriteScratch) addFixed(key uint64) {
	if s.fixed == nil {
		s.fixed = make([]uint64, 0, 1)
	}
	s.ok = true
	s.fixed = append(s.fixed, key)
}

func (s *fieldWriteScratch) stringLen() int {
	return len(s.strings)
}

func (s *fieldWriteScratch) fixedLen() int {
	if s.fixed == nil {
		return 0
	}
	return len(s.fixed)
}

func (s *fieldWriteScratch) stringAt(i int) string {
	return s.strings[i]
}

func (s *fieldWriteScratch) fixedAt(i int) uint64 {
	return s.fixed[i]
}

func (s *fieldWriteScratch) sortForField(f *field) {
	if f == nil || !f.Slice {
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
	s.state.nils = addFieldPostingList(s.state.nils, nilIndexEntryKey, s.idx)
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
	s.state.index = addFieldPostingList(s.state.index, key, s.idx)
	s.state.changed = true
}

func (s snapshotOverlayWriteSink) addFixed(key uint64) {
	if s.state == nil {
		return
	}
	s.state.fixed = addFixedFieldPostingList(s.state.fixed, key, s.idx)
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
	s.state.nils = indexdata.AddStringPostingAddOwned(s.state.nils, &s.state.arena, nilIndexEntryKey, s.idx, s.state.nilsHint)
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
	s.state.index = indexdata.AddStringPostingAddOwned(s.state.index, &s.state.arena, key, s.idx, s.state.indexHint)
	s.state.changed = true
}

func (s snapshotInsertWriteSink) addFixed(key uint64) {
	if s.state == nil {
		return
	}
	s.state.fixed = indexdata.AddFixedPostingAddOwned(s.state.fixed, &s.state.arena, key, s.idx, s.state.fixedHint)
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
	states []snapshotFieldInsertState,
	access []indexedFieldAccessor,
	prev *indexSnapshot,
	batchHint int,
) {
	if prev == nil || batchHint <= 0 {
		return
	}
	for i := range access {
		acc := access[i]
		state := &states[i]
		var indexHint int
		if prev.index != nil && acc.ordinal < len(prev.index) {
			indexHint = snapshotFieldStorageHint(prev.index[acc.ordinal], batchHint)
		}
		if acc.field != nil && acc.field.KeyKind == fieldWriteKeysOrderedU64 {
			state.fixedHint = indexHint
		} else {
			state.indexHint = indexHint
		}
		if prev.nilIndex != nil && acc.ordinal < len(prev.nilIndex) {
			state.nilsHint = snapshotFieldStorageHint(prev.nilIndex[acc.ordinal], batchHint)
		}
		if acc.field != nil && acc.field.Slice && prev.lenIndex != nil && acc.ordinal < len(prev.lenIndex) {
			state.lengthsHint = snapshotFieldStorageHint(prev.lenIndex[acc.ordinal], batchHint)
		}
	}
}

func snapshotFieldStorageHint(base indexdata.FieldStorage, batchHint int) int {
	if batchHint <= 0 {
		return 0
	}
	hint := base.KeyCount()
	if hint < 8 {
		hint = 8
	}
	if hint > batchHint {
		hint = batchHint
	}
	return hint
}

func (acc indexedFieldAccessor) mergeSnapshotInsertStorageOwned(
	base indexdata.FieldStorage,
	state *snapshotFieldInsertState,
	allowChunk bool,
) indexdata.FieldStorage {
	if state == nil {
		return base
	}
	if acc.field != nil && acc.field.KeyKind == fieldWriteKeysOrderedU64 {
		return base.MergeFixedPostingAddsRetainMapOwned(state.fixed, state.arena, allowChunk)
	}
	return base.MergeStringPostingAddsRetainMapOwned(state.index, state.arena, false, allowChunk)
}

func (acc indexedFieldAccessor) mergeSnapshotInsertNilStorageOwned(base indexdata.FieldStorage, state *snapshotFieldInsertState) indexdata.FieldStorage {
	if state == nil {
		return base
	}
	return base.MergeStringPostingAddsRetainMapOwned(state.nils, state.arena, false, false)
}

func (state *snapshotFieldInsertState) reset() {
	if len(state.index) > 4<<10 {
		state.index = nil
	} else {
		clear(state.index)
	}
	if len(state.fixed) > 4<<10 {
		state.fixed = nil
	} else {
		clear(state.fixed)
	}
	if len(state.nils) > 4<<10 {
		state.nils = nil
	} else {
		clear(state.nils)
	}
	state.arena.Reset()
	if state.lengths != nil {
		state.lengths.Reset()
	}
	state.changed = false
	state.indexHint = 0
	state.fixedHint = 0
	state.nilsHint = 0
	state.lengthsHint = 0
}

func (state *snapshotFieldInsertState) discard() {
	state.index = nil
	state.fixed = nil
	state.nils = nil
	state.arena.Reset()
	state.arena = nil
	if state.lengths != nil {
		state.lengths.Reset()
		state.lengths = nil
	}
	state.changed = false
	state.indexHint = 0
	state.fixedHint = 0
	state.nilsHint = 0
	state.lengthsHint = 0
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
			state.nils = indexdata.AddStringPostingDiffOwned(state.nils, &state.arena, nilIndexEntryKey, idx, false)
		}
		if state.new.isNil {
			state.nils = indexdata.AddStringPostingDiffOwned(state.nils, &state.arena, nilIndexEntryKey, idx, true)
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
			state.fixed, changed = collectFixedFieldBatchPostingDiffBuf(state.fixed, &state.arena, idx, oldMulti, newMulti)
		} else {
			var oldSingle, newSingle uint64
			if oldOK && state.old.fixedLen() > 0 {
				oldSingle = state.old.fixedAt(0)
			}
			if newOK && state.new.fixedLen() > 0 {
				newSingle = state.new.fixedAt(0)
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
		state.index, changed = collectFieldBatchPostingDiffBuf(state.index, &state.arena, idx, oldMulti, newMulti)
	} else {
		var oldSingle, newSingle string
		if oldOK && state.old.stringLen() > 0 {
			oldSingle = state.old.stringAt(0)
		}
		if newOK && state.new.stringLen() > 0 {
			newSingle = state.new.stringAt(0)
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
	base indexdata.FieldStorage,
	state *snapshotFieldBatchState,
	allowChunk bool,
) indexdata.FieldStorage {
	if state == nil {
		return base
	}
	if acc.field != nil && acc.field.KeyKind == fieldWriteKeysOrderedU64 {
		return base.ApplyFixedPostingDiffRetainMapOwned(state.fixed, state.arena, allowChunk)
	}
	return base.ApplyStringPostingDiffRetainMapOwned(state.index, state.arena, false, allowChunk)
}

func (acc indexedFieldAccessor) applySnapshotBatchNilStorageOwned(base indexdata.FieldStorage, state *snapshotFieldBatchState) indexdata.FieldStorage {
	if state == nil {
		return base
	}
	return base.ApplyStringPostingDiffRetainMapOwned(state.nils, state.arena, false, false)
}

func (state *snapshotFieldBatchState) reset() {
	state.old.reset()
	state.new.reset()
	if len(state.index) > 4<<10 {
		state.index = nil
	} else {
		clear(state.index)
	}
	if len(state.fixed) > 4<<10 {
		state.fixed = nil
	} else {
		clear(state.fixed)
	}
	if len(state.nils) > 4<<10 {
		state.nils = nil
	} else {
		clear(state.nils)
	}
	state.arena.Reset()
	if state.lengths != nil {
		state.lengths.Reset()
	}
	state.changed = false
}

func (state *snapshotFieldBatchState) discard() {
	state.old.discard()
	state.new.discard()
	state.index = nil
	state.fixed = nil
	state.nils = nil
	state.arena.Reset()
	state.arena = nil
	if state.lengths != nil {
		state.lengths.Reset()
		state.lengths = nil
	}
	state.changed = false
}

func cleanupSnapshotFieldInsertStateSlice(states []snapshotFieldInsertState) {
	if cap(states) <= snapshotFieldStateSlicePoolMaxCap {
		return
	}
	states = states[:cap(states)]
	for i := range states {
		states[i].discard()
	}
}

func cleanupSnapshotFieldBatchStateSlice(states []snapshotFieldBatchState) {
	if cap(states) <= snapshotFieldStateSlicePoolMaxCap {
		return
	}
	states = states[:cap(states)]
	for i := range states {
		states[i].discard()
	}
}
