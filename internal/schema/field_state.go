package schema

import (
	"slices"
	"unsafe"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/posting"
)

type IndexState struct {
	index   map[string]posting.List
	fixed   map[uint64]posting.List
	lengths map[uint32]posting.List
	nils    map[string]posting.List
	changed bool
}

type InsertState struct {
	index   map[string]uint32
	fixed   map[uint64]uint32
	nils    map[string]uint32
	arena   indexdata.PostingAddArena
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

type batchLenDiff struct {
	oldExists bool
	oldLen    int
	newExists bool
	newLen    int
}

var snapshotFieldInsertStateSlicePool = pooled.Slices[InsertState]{
	MaxCap:  snapshotFieldStateSlicePoolMaxCap,
	Clear:   pooled.NoClear,
	Cleanup: cleanupSnapshotFieldInsertStateSlice,
}

var snapshotFieldIndexStateSlicePool = pooled.Slices[IndexState]{
	MaxCap:  snapshotFieldStateSlicePoolMaxCap,
	Clear:   pooled.ClearCap,
	Cleanup: cleanupSnapshotFieldIndexStateSlice,
}

type WriteScratch struct {
	strings []string
	fixed   []uint64
	ok      bool
	isNil   bool
	length  int
}

type BatchState struct {
	index   map[string]uint32
	fixed   map[uint64]uint32
	nils    map[string]uint32
	arena   indexdata.PostingDiffArena
	lengths *indexdata.LenPostingDiff
	changed bool
	old     WriteScratch
	new     WriteScratch
}

var snapshotFieldBatchStateSlicePool = pooled.Slices[BatchState]{
	MaxCap:  snapshotFieldStateSlicePoolMaxCap,
	Clear:   pooled.NoClear,
	Cleanup: cleanupSnapshotFieldBatchStateSlice,
}

func GetInsertStates(n int) []InsertState {
	states := snapshotFieldInsertStateSlicePool.Get(n)
	return states[:n]
}

func GetIndexStates(n int) []IndexState {
	states := snapshotFieldIndexStateSlicePool.Get(n)
	return states[:n]
}

func ReleaseIndexStates(states []IndexState) {
	snapshotFieldIndexStateSlicePool.Put(states)
}

func ReleaseInsertStates(states []InsertState) {
	snapshotFieldInsertStateSlicePool.Put(states)
}

func GetBatchStates(n int) []BatchState {
	states := snapshotFieldBatchStateSlicePool.Get(n)
	return states[:n]
}

func ReleaseBatchStates(states []BatchState) {
	snapshotFieldBatchStateSlicePool.Put(states)
}

func (state *IndexState) Changed() bool {
	return state.changed
}

func (state *IndexState) MaterializeStorage(numeric bool) indexdata.FieldStorage {
	if numeric {
		fixed := state.fixed
		state.fixed = nil
		return indexdata.NewRegularFieldStorageFromFixedPostingMapOwned(fixed)
	}
	idx := state.index
	state.index = nil
	return indexdata.NewRegularFieldStorageFromPostingMapOwned(idx, false)
}

func (state *IndexState) MaterializeNilStorage() indexdata.FieldStorage {
	nils := state.nils
	state.nils = nil
	return indexdata.NewFlatFieldStorageFromPostingMapOwned(nils, false)
}

func (state *IndexState) MaterializeLenStorage(universe posting.List) (indexdata.FieldStorage, bool) {
	lengths := state.lengths
	state.lengths = nil
	storage, useZeroComplement := indexdata.NewLenFieldStorageFromMapOwned(universe, lengths)
	indexdata.ReleaseLenPostingMap(lengths)
	return storage, useZeroComplement
}

func (state *IndexState) Reset() {
	if state.index != nil {
		indexdata.ReleasePostingMap(state.index)
		state.index = nil
	}
	if state.fixed != nil {
		indexdata.ReleaseFixedPostingMap(state.fixed)
		state.fixed = nil
	}
	if state.lengths != nil {
		posting.ReleaseMapU32(state.lengths)
		indexdata.ReleaseLenPostingMap(state.lengths)
		state.lengths = nil
	}
	if state.nils != nil {
		indexdata.ReleasePostingMap(state.nils)
		state.nils = nil
	}
	state.changed = false
}

func (state *InsertState) LenDiff() *indexdata.LenPostingDiff {
	return state.lengths
}

func (state *InsertState) Changed() bool {
	return state.changed
}

func (state *BatchState) LenDiff() *indexdata.LenPostingDiff {
	return state.lengths
}

func (state *BatchState) Changed() bool {
	return state.changed
}

func (s *WriteScratch) reset() {
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

func (s *WriteScratch) discard() {
	s.strings = nil
	s.fixed = nil
	s.ok = false
	s.isNil = false
	s.length = 0
}

func (s *WriteScratch) setNil() {
	s.ok = true
	s.isNil = true
}

func (s *WriteScratch) setLen(length int) {
	s.ok = true
	s.length = length
}

func (s *WriteScratch) addString(key string) {
	if s.strings == nil {
		s.strings = make([]string, 0, 1)
	}
	s.ok = true
	s.strings = append(s.strings, key)
}

func (s *WriteScratch) addFixed(key uint64) {
	if s.fixed == nil {
		s.fixed = make([]uint64, 0, 1)
	}
	s.ok = true
	s.fixed = append(s.fixed, key)
}

func (s *WriteScratch) stringLen() int {
	return len(s.strings)
}

func (s *WriteScratch) fixedLen() int {
	return len(s.fixed)
}

func (s *WriteScratch) stringAt(i int) string {
	return s.strings[i]
}

func (s *WriteScratch) fixedAt(i int) uint64 {
	return s.fixed[i]
}

func (s *WriteScratch) sortForField(f *Field) {
	if !f.Slice {
		return
	}
	if f.KeyKind == FieldWriteKeysOrderedU64 {
		if len(s.fixed) > 1 {
			slices.Sort(s.fixed)
		}
		return
	}
	if len(s.strings) > 1 {
		slices.Sort(s.strings)
	}
}

type IndexSink struct {
	state *IndexState
	idx   uint64
}

func (s IndexSink) setNil() {
	s.state.nils = addFieldPostingList(s.state.nils, indexdata.NilIndexEntryKey, s.idx)
	s.state.changed = true
}

func (s IndexSink) setLen(length int) {
	if s.state.lengths == nil {
		s.state.lengths = indexdata.GetLenPostingMap()
	}
	ln := uint32(length)
	ids := s.state.lengths[ln]
	ids = ids.BuildAdded(s.idx)
	s.state.lengths[ln] = ids
	s.state.changed = true
}

func (s IndexSink) addString(key string) {
	s.state.index = addFieldPostingList(s.state.index, key, s.idx)
	s.state.changed = true
}

func (s IndexSink) addFixed(key uint64) {
	s.state.fixed = addFixedFieldPostingList(s.state.fixed, key, s.idx)
	s.state.changed = true
}

type InsertSink struct {
	state             *InsertState
	idx               uint64
	useZeroComplement bool
}

func (s InsertSink) setNil() {
	s.state.nils = indexdata.AddStringPostingAdd(s.state.nils, &s.state.arena, indexdata.NilIndexEntryKey, s.idx, s.state.nilsHint)
	s.state.changed = true
}

func (s InsertSink) setLen(length int) {
	var changed bool
	s.state.lengths, changed = collectFieldBatchLenDiff(s.state.lengths, s.idx, batchLenDiff{
		newExists: true,
		newLen:    length,
	}, s.useZeroComplement)
	if changed {
		s.state.changed = true
	}
}

func (s InsertSink) addString(key string) {
	s.state.index = indexdata.AddStringPostingAdd(s.state.index, &s.state.arena, key, s.idx, s.state.indexHint)
	s.state.changed = true
}

func (s InsertSink) addFixed(key uint64) {
	s.state.fixed = indexdata.AddFixedPostingAdd(s.state.fixed, &s.state.arena, key, s.idx, s.state.fixedHint)
	s.state.changed = true
}

func (acc IndexedFieldAccessor) CollectIndexValue(ptr unsafe.Pointer, idx uint64, state *IndexState) {
	acc.WriteIndex(ptr, IndexSink{state: state, idx: idx})
}

func (acc IndexedFieldAccessor) CollectInsertValue(
	ptr unsafe.Pointer,
	idx uint64,
	useZeroComplement bool,
	state *InsertState,
) {
	acc.WriteInsert(ptr, InsertSink{
		state:             state,
		idx:               idx,
		useZeroComplement: useZeroComplement,
	})
}

func InitInsertStateHints(states []InsertState, access []IndexedFieldAccessor, prevIndex, prevNilIndex, prevLenIndex []indexdata.FieldStorage, batchHint int) {
	if batchHint <= 0 {
		return
	}
	for i := range access {
		acc := access[i]
		state := &states[i]
		var indexHint int
		if prevIndex != nil && acc.Ordinal < len(prevIndex) {
			indexHint = snapshotFieldStorageHint(prevIndex[acc.Ordinal], batchHint)
		}
		if acc.Field.KeyKind == FieldWriteKeysOrderedU64 {
			state.fixedHint = indexHint
		} else {
			state.indexHint = indexHint
		}
		if prevNilIndex != nil && acc.Ordinal < len(prevNilIndex) {
			state.nilsHint = snapshotFieldStorageHint(prevNilIndex[acc.Ordinal], batchHint)
		}
		if acc.Field.Slice && prevLenIndex != nil && acc.Ordinal < len(prevLenIndex) {
			state.lengthsHint = snapshotFieldStorageHint(prevLenIndex[acc.Ordinal], batchHint)
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

func (acc IndexedFieldAccessor) MergeInsertStorageOwned(base indexdata.FieldStorage, state *InsertState, allowChunk bool) indexdata.FieldStorage {
	if acc.Field.KeyKind == FieldWriteKeysOrderedU64 {
		return base.MergeFixedPostingAdds(state.fixed, &state.arena, allowChunk)
	}
	return base.MergeStringPostingAdds(state.index, &state.arena, false, allowChunk)
}

func (acc IndexedFieldAccessor) MergeInsertNilStorageOwned(base indexdata.FieldStorage, state *InsertState) indexdata.FieldStorage {
	return base.MergeStringPostingAdds(state.nils, &state.arena, false, false)
}

func (state *InsertState) Reset() {
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

func (state *InsertState) discard() {
	state.index = nil
	state.fixed = nil
	state.nils = nil
	state.arena = indexdata.PostingAddArena{}
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

func (acc IndexedFieldAccessor) CollectBatchDiff(idx uint64, oldPtr, newPtr unsafe.Pointer, useZeroComplement bool, state *BatchState) {
	state.old.reset()
	state.new.reset()
	if oldPtr != nil {
		acc.WriteScratch(oldPtr, &state.old)
	}
	if newPtr != nil {
		acc.WriteScratch(newPtr, &state.new)
	}
	state.old.sortForField(acc.Field)
	state.new.sortForField(acc.Field)

	if state.old.isNil != state.new.isNil {
		if state.old.isNil {
			state.nils = indexdata.AddStringPostingDiff(state.nils, &state.arena, indexdata.NilIndexEntryKey, idx, false)
		}
		if state.new.isNil {
			state.nils = indexdata.AddStringPostingDiff(state.nils, &state.arena, indexdata.NilIndexEntryKey, idx, true)
		}
		state.changed = true
	}

	oldOK := state.old.ok && !state.old.isNil
	newOK := state.new.ok && !state.new.isNil

	var changed bool
	if acc.Field.KeyKind == FieldWriteKeysOrderedU64 {

		if acc.Field.Slice {
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

	} else if acc.Field.Slice {
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

	if !acc.Field.Slice {
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

func (acc IndexedFieldAccessor) ApplyBatchStorageOwned(base indexdata.FieldStorage, state *BatchState, allowChunk bool) indexdata.FieldStorage {
	if acc.Field.KeyKind == FieldWriteKeysOrderedU64 {
		return base.ApplyFixedPostingDiff(state.fixed, &state.arena, allowChunk)
	}
	return base.ApplyStringPostingDiff(state.index, &state.arena, false, allowChunk)
}

func (acc IndexedFieldAccessor) ApplyBatchNilStorageOwned(base indexdata.FieldStorage, state *BatchState) indexdata.FieldStorage {
	return base.ApplyStringPostingDiff(state.nils, &state.arena, false, false)
}

func (state *BatchState) Reset() {
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

func (state *BatchState) discard() {
	state.old.discard()
	state.new.discard()
	state.index = nil
	state.fixed = nil
	state.nils = nil
	state.arena = indexdata.PostingDiffArena{}
	if state.lengths != nil {
		state.lengths.Reset()
		state.lengths = nil
	}
	state.changed = false
}

func addFieldPostingList(fieldMap map[string]posting.List, key string, idx uint64) map[string]posting.List {
	if fieldMap == nil {
		fieldMap = indexdata.GetPostingMap()
	}
	p := fieldMap[key]
	p = p.BuildAdded(idx)
	fieldMap[key] = p
	return fieldMap
}

func addFixedFieldPostingList(fieldMap map[uint64]posting.List, key uint64, idx uint64) map[uint64]posting.List {
	if fieldMap == nil {
		fieldMap = indexdata.GetFixedPostingMap()
	}
	p := fieldMap[key]
	p = p.BuildAdded(idx)
	fieldMap[key] = p
	return fieldMap
}

func collectFieldBatchPostingDiffBuf(fieldDelta map[string]uint32, arena *indexdata.PostingDiffArena, idx uint64, oldVals, newVals []string) (map[string]uint32, bool) {
	oldLen := len(oldVals)
	newLen := len(newVals)
	if oldLen == 0 && newLen == 0 {
		return fieldDelta, false
	}

	var changed bool

	i, j := 0, 0
	for i < oldLen && j < newLen {
		oldVal := oldVals[i]
		newVal := newVals[j]
		switch {
		case oldVal < newVal:
			fieldDelta = indexdata.AddStringPostingDiff(fieldDelta, arena, oldVal, idx, false)
			changed = true
			i++
		case oldVal > newVal:
			fieldDelta = indexdata.AddStringPostingDiff(fieldDelta, arena, newVal, idx, true)
			changed = true
			j++
		default:
			i++
			j++
		}
	}

	for ; i < oldLen; i++ {
		fieldDelta = indexdata.AddStringPostingDiff(fieldDelta, arena, oldVals[i], idx, false)
		changed = true
	}
	for ; j < newLen; j++ {
		fieldDelta = indexdata.AddStringPostingDiff(fieldDelta, arena, newVals[j], idx, true)
		changed = true
	}

	return fieldDelta, changed
}

func collectFixedFieldBatchPostingDiffBuf(fieldDelta map[uint64]uint32, arena *indexdata.PostingDiffArena, idx uint64, oldVals, newVals []uint64) (map[uint64]uint32, bool) {
	oldLen := len(oldVals)
	newLen := len(newVals)
	if oldLen == 0 && newLen == 0 {
		return fieldDelta, false
	}

	var changed bool

	i, j := 0, 0
	for i < oldLen && j < newLen {
		oldVal := oldVals[i]
		newVal := newVals[j]
		switch {
		case oldVal < newVal:
			fieldDelta = indexdata.AddFixedPostingDiff(fieldDelta, arena, oldVal, idx, false)
			changed = true
			i++
		case oldVal > newVal:
			fieldDelta = indexdata.AddFixedPostingDiff(fieldDelta, arena, newVal, idx, true)
			changed = true
			j++
		default:
			i++
			j++
		}
	}

	for ; i < oldLen; i++ {
		fieldDelta = indexdata.AddFixedPostingDiff(fieldDelta, arena, oldVals[i], idx, false)
		changed = true
	}
	for ; j < newLen; j++ {
		fieldDelta = indexdata.AddFixedPostingDiff(fieldDelta, arena, newVals[j], idx, true)
		changed = true
	}

	return fieldDelta, changed
}

func collectScalarFieldBatchPostingDiff(
	fieldDelta map[string]uint32,
	arena *indexdata.PostingDiffArena,
	idx uint64,
	oldOK bool,
	oldVal string,
	newOK bool,
	newVal string,
) (map[string]uint32, bool) {

	switch {

	case oldOK && newOK:
		if oldVal == newVal {
			return fieldDelta, false
		}
		fieldDelta = indexdata.AddStringPostingDiff(fieldDelta, arena, oldVal, idx, false)
		fieldDelta = indexdata.AddStringPostingDiff(fieldDelta, arena, newVal, idx, true)
		return fieldDelta, true

	case oldOK:
		fieldDelta = indexdata.AddStringPostingDiff(fieldDelta, arena, oldVal, idx, false)
		return fieldDelta, true

	case newOK:
		fieldDelta = indexdata.AddStringPostingDiff(fieldDelta, arena, newVal, idx, true)
		return fieldDelta, true

	default:
		return fieldDelta, false
	}
}

func collectScalarFixedFieldBatchPostingDiff(
	fieldDelta map[uint64]uint32,
	arena *indexdata.PostingDiffArena,
	idx uint64,
	oldOK bool,
	oldVal uint64,
	newOK bool,
	newVal uint64,
) (map[uint64]uint32, bool) {

	switch {

	case oldOK && newOK:
		if oldVal == newVal {
			return fieldDelta, false
		}
		fieldDelta = indexdata.AddFixedPostingDiff(fieldDelta, arena, oldVal, idx, false)
		fieldDelta = indexdata.AddFixedPostingDiff(fieldDelta, arena, newVal, idx, true)
		return fieldDelta, true

	case oldOK:
		fieldDelta = indexdata.AddFixedPostingDiff(fieldDelta, arena, oldVal, idx, false)
		return fieldDelta, true

	case newOK:
		fieldDelta = indexdata.AddFixedPostingDiff(fieldDelta, arena, newVal, idx, true)
		return fieldDelta, true

	default:
		return fieldDelta, false
	}
}

func collectFieldBatchLenDiff(fieldDelta *indexdata.LenPostingDiff, idx uint64, diff batchLenDiff, useZeroComplement bool) (*indexdata.LenPostingDiff, bool) {
	logicalChange := diff.oldExists != diff.newExists || diff.oldLen != diff.newLen
	if !logicalChange {
		return fieldDelta, false
	}

	if !useZeroComplement {
		var changed bool
		if diff.oldExists {
			fieldDelta = indexdata.AddLenPostingBucket(fieldDelta, idx, diff.oldLen, false)
			changed = true
		}
		if diff.newExists {
			fieldDelta = indexdata.AddLenPostingBucket(fieldDelta, idx, diff.newLen, true)
			changed = true
		}
		return fieldDelta, changed
	}

	if diff.oldExists {
		if diff.oldLen > 0 {
			fieldDelta = indexdata.AddLenPostingBucket(fieldDelta, idx, diff.oldLen, false)
		}
		if diff.oldLen > 0 && (!diff.newExists || diff.newLen == 0) {
			fieldDelta = indexdata.AddLenPostingNonEmpty(fieldDelta, idx, false)
		}
	}

	if diff.newExists {
		if diff.newLen > 0 {
			fieldDelta = indexdata.AddLenPostingBucket(fieldDelta, idx, diff.newLen, true)
		}
		if diff.newLen > 0 && (!diff.oldExists || diff.oldLen == 0) {
			fieldDelta = indexdata.AddLenPostingNonEmpty(fieldDelta, idx, true)
		}
	}

	return fieldDelta, true
}

func cleanupSnapshotFieldInsertStateSlice(states []InsertState) {
	if cap(states) <= snapshotFieldStateSlicePoolMaxCap {
		return
	}
	states = states[:cap(states)]
	for i := range states {
		states[i].discard()
	}
}

func cleanupSnapshotFieldIndexStateSlice(states []IndexState) {
	for i := range states {
		states[i].Reset()
	}
}

func cleanupSnapshotFieldBatchStateSlice(states []BatchState) {
	if cap(states) <= snapshotFieldStateSlicePoolMaxCap {
		return
	}
	states = states[:cap(states)]
	for i := range states {
		states[i].discard()
	}
}
