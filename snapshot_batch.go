package rbi

import (
	"sort"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/pools"
	"github.com/vapstack/rbi/internal/posting"
)

type snapshotBatchEntry struct {
	idx       uint64
	oldVal    unsafe.Pointer
	newVal    unsafe.Pointer
	patch     []Field
	patchOnly bool
}

type batchPostingDelta struct {
	add    posting.List
	remove posting.List
}

type keyedBatchPostingDelta struct {
	key   keycodec.IndexKey
	delta batchPostingDelta
}

type keyedBatchPostingDeltaBufOrder struct {
	buf *pooled.Slice[keyedBatchPostingDelta]
}

func (s keyedBatchPostingDeltaBufOrder) Len() int { return s.buf.Len() }

func (s keyedBatchPostingDeltaBufOrder) Swap(i, j int) {
	a := s.buf.Get(i)
	s.buf.Set(i, s.buf.Get(j))
	s.buf.Set(j, a)
}

func (s keyedBatchPostingDeltaBufOrder) Less(i, j int) bool {
	return keycodec.Compare(s.buf.Get(i).key, s.buf.Get(j).key) < 0
}

func sortKeyedBatchPostingDeltasBuf(buf *pooled.Slice[keyedBatchPostingDelta]) {
	if buf == nil || buf.Len() <= 1 {
		return
	}
	sort.Sort(keyedBatchPostingDeltaBufOrder{buf: buf})
}

func takeKeyedBatchPostingDeltaBuf(buf *pooled.Slice[keyedBatchPostingDelta], i int) keyedBatchPostingDelta {
	delta := buf.Get(i)
	buf.Set(i, keyedBatchPostingDelta{})
	return delta
}

type batchLenDiff struct {
	oldExists bool
	oldLen    int
	newExists bool
	newLen    int
}

type lenFieldPostingDelta struct {
	lengths     map[uint64]batchPostingDelta
	nonEmpty    batchPostingDelta
	hasNonEmpty bool
}

var lenFieldPostingDeltaPool = pooled.Pointers[lenFieldPostingDelta]{
	Clear: true,
}

func putLenFieldPostingDelta(delta *lenFieldPostingDelta) {
	if delta == nil {
		return
	}
	if delta.lengths != nil {
		fixedBatchPostingDeltaMapPool.Put(delta.lengths)
		delta.lengths = nil
	}
	lenFieldPostingDeltaPool.Put(delta)
}

func releaseLenFieldPostingDeltaOwned(delta *lenFieldPostingDelta) {
	if delta == nil {
		return
	}
	if delta.lengths != nil {
		for _, lengthDelta := range delta.lengths {
			lengthDelta.add.Release()
			lengthDelta.remove.Release()
		}
		fixedBatchPostingDeltaMapPool.Put(delta.lengths)
		delta.lengths = nil
	}
	delta.nonEmpty.add.Release()
	delta.nonEmpty.remove.Release()
	lenFieldPostingDeltaPool.Put(delta)
}

func ensureLenFieldPostingDelta(delta **lenFieldPostingDelta) *lenFieldPostingDelta {
	if *delta == nil {
		*delta = lenFieldPostingDeltaPool.Get()
		if (*delta).lengths == nil {
			(*delta).lengths = fixedBatchPostingDeltaMapPool.Get()
		}
	}
	return *delta
}

func addLenFieldPostingDeltaBucket(fieldDelta **lenFieldPostingDelta, idx uint64, length int, isAdd bool) {
	delta := ensureLenFieldPostingDelta(fieldDelta)
	delta.lengths = addFixedFieldBatchPostingDelta(delta.lengths, uint64(length), idx, isAdd)
}

func addLenFieldPostingNonEmptyDelta(fieldDelta **lenFieldPostingDelta, idx uint64, isAdd bool) {
	delta := ensureLenFieldPostingDelta(fieldDelta)
	delta.hasNonEmpty = true
	if isAdd {
		delta.nonEmpty.add = delta.nonEmpty.add.BuildAdded(idx)
	} else {
		delta.nonEmpty.remove = delta.nonEmpty.remove.BuildAdded(idx)
	}
}

func nextFieldPostingDiffBaseEntry(
	base *fieldIndexChunkedRoot,
	endChunk int,
	chunkIdx int,
	entryIdx int,
) (index, bool) {
	for chunkIdx < endChunk {
		ref, _ := base.refAtChunk(chunkIdx)
		chunk := ref.chunk
		if entryIdx < chunk.keyCount() {
			return index{Key: chunk.keyAt(entryIdx), IDs: chunk.postingAt(entryIdx)}, true
		}
		chunkIdx++
		entryIdx = 0
	}
	return index{}, false
}

func advanceFieldPostingDiffBaseEntry(base *fieldIndexChunkedRoot, endChunk int, chunkIdx *int, entryIdx *int) {
	*entryIdx += 1
	for *chunkIdx < endChunk {
		ref, _ := base.refAtChunk(*chunkIdx)
		if *entryIdx < ref.chunk.keyCount() {
			return
		}
		*chunkIdx += 1
		*entryIdx = 0
	}
}

func cloneFieldIndexStorageSlots(src *pooled.Slice[fieldIndexStorage], size int) *pooled.Slice[fieldIndexStorage] {
	out := fieldIndexStorageSlicePool.Get()
	out.SetLen(size)
	if src == nil {
		return out
	}
	limit := min(size, src.Len())
	for i := 0; i < limit; i++ {
		out.Set(i, src.Get(i))
	}
	return out
}

func cloneFieldIndexBoolSlots(src []bool, size int) []bool {
	out := pools.GetBoolSlice(size)[:size]
	clear(out)
	copy(out, src[:min(size, len(src))])
	return out
}

func ensureSnapshotUniverseOwned(next *indexSnapshot, universeOwned *bool) {
	if *universeOwned {
		return
	}
	next.universe = next.universe.Clone()
	next.universeOwner = nil
	*universeOwned = true
}

type indexedFieldBatchDeltas struct {
	fields  *pooled.Slice[snapshotFieldBatchState]
	touched []int
	changed []bool
}

func (deltas *indexedFieldBatchDeltas) markTouched(ordinal int) {
	if deltas.changed[ordinal] {
		return
	}
	deltas.changed[ordinal] = true
	deltas.touched = append(deltas.touched, ordinal)
}

func normalizePreparedBatchForSnapshot(entries []snapshotBatchEntry) []snapshotBatchEntry {
	if len(entries) == 0 {
		return nil
	}
	if len(entries) == 1 {
		op := entries[0]
		if op.oldVal == nil && op.newVal == nil {
			return nil
		}
		return entries[:1]
	}

	pos := uint64IntMapPool.Get(len(entries))
	defer uint64IntMapPool.Put(pos)

	n := 0
	for i := range entries {
		op := entries[i]
		if p := pos[op.idx]; p != 0 {
			// Repeated-id entries collapse to first oldVal -> last newVal.
			// Patch metadata from any individual request is no longer sufficient
			// to describe the aggregated diff across the whole chain, so disable
			// the patch-only fast path for this normalized entry.
			entries[p-1].patch = nil
			entries[p-1].patchOnly = false
			entries[p-1].newVal = op.newVal
			continue
		}
		pos[op.idx] = n + 1
		if n != i {
			entries[n] = op
		}
		n++
	}

	write := 0
	for i := 0; i < n; i++ {
		if entries[i].oldVal == nil && entries[i].newVal == nil {
			continue
		}
		if write != i {
			entries[write] = entries[i]
		}
		write++
	}
	return entries[:write]
}

func addFixedFieldBatchPostingDelta(fieldDelta map[uint64]batchPostingDelta, key uint64, idx uint64, isAdd bool) map[uint64]batchPostingDelta {
	if fieldDelta == nil {
		fieldDelta = fixedBatchPostingDeltaMapPool.Get()
	}
	delta := fieldDelta[key]
	if isAdd {
		delta.add = delta.add.BuildAdded(idx)
	} else {
		delta.remove = delta.remove.BuildAdded(idx)
	}
	fieldDelta[key] = delta
	return fieldDelta
}

func collectFieldBatchPostingDiffBuf(
	fieldDelta map[string]uint32,
	arena **batchPostingAccumArena,
	idx uint64,
	oldVals, newVals []string,
) (map[string]uint32, bool) {
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
			fieldDelta = addFieldBatchPostingAccum(fieldDelta, arena, oldVal, idx, false, 0)
			changed = true
			i++
		case oldVal > newVal:
			fieldDelta = addFieldBatchPostingAccum(fieldDelta, arena, newVal, idx, true, 0)
			changed = true
			j++
		default:
			i++
			j++
		}
	}
	for ; i < oldLen; i++ {
		fieldDelta = addFieldBatchPostingAccum(fieldDelta, arena, oldVals[i], idx, false, 0)
		changed = true
	}
	for ; j < newLen; j++ {
		fieldDelta = addFieldBatchPostingAccum(fieldDelta, arena, newVals[j], idx, true, 0)
		changed = true
	}
	return fieldDelta, changed
}

func collectFixedFieldBatchPostingDiffBuf(
	fieldDelta map[uint64]uint32,
	arena **batchPostingAccumArena,
	idx uint64,
	oldVals, newVals []uint64,
) (map[uint64]uint32, bool) {
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
			fieldDelta = addFixedFieldBatchPostingAccum(fieldDelta, arena, oldVal, idx, false, 0)
			changed = true
			i++
		case oldVal > newVal:
			fieldDelta = addFixedFieldBatchPostingAccum(fieldDelta, arena, newVal, idx, true, 0)
			changed = true
			j++
		default:
			i++
			j++
		}
	}
	for ; i < oldLen; i++ {
		fieldDelta = addFixedFieldBatchPostingAccum(fieldDelta, arena, oldVals[i], idx, false, 0)
		changed = true
	}
	for ; j < newLen; j++ {
		fieldDelta = addFixedFieldBatchPostingAccum(fieldDelta, arena, newVals[j], idx, true, 0)
		changed = true
	}
	return fieldDelta, changed
}

func collectScalarFieldBatchPostingDiff(
	fieldDelta map[string]uint32,
	arena **batchPostingAccumArena,
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
		fieldDelta = addFieldBatchPostingAccum(fieldDelta, arena, oldVal, idx, false, 0)
		fieldDelta = addFieldBatchPostingAccum(fieldDelta, arena, newVal, idx, true, 0)
		return fieldDelta, true
	case oldOK:
		fieldDelta = addFieldBatchPostingAccum(fieldDelta, arena, oldVal, idx, false, 0)
		return fieldDelta, true
	case newOK:
		fieldDelta = addFieldBatchPostingAccum(fieldDelta, arena, newVal, idx, true, 0)
		return fieldDelta, true
	default:
		return fieldDelta, false
	}
}

func collectScalarFixedFieldBatchPostingDiff(
	fieldDelta map[uint64]uint32,
	arena **batchPostingAccumArena,
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
		fieldDelta = addFixedFieldBatchPostingAccum(fieldDelta, arena, oldVal, idx, false, 0)
		fieldDelta = addFixedFieldBatchPostingAccum(fieldDelta, arena, newVal, idx, true, 0)
		return fieldDelta, true
	case oldOK:
		fieldDelta = addFixedFieldBatchPostingAccum(fieldDelta, arena, oldVal, idx, false, 0)
		return fieldDelta, true
	case newOK:
		fieldDelta = addFixedFieldBatchPostingAccum(fieldDelta, arena, newVal, idx, true, 0)
		return fieldDelta, true
	default:
		return fieldDelta, false
	}
}

func collectFieldBatchLenDiff(
	fieldDelta *lenFieldPostingDelta,
	idx uint64,
	diff batchLenDiff,
	useZeroComplement bool,
) (*lenFieldPostingDelta, bool) {
	logicalChange := diff.oldExists != diff.newExists || diff.oldLen != diff.newLen
	if !logicalChange {
		return fieldDelta, false
	}

	var changed bool

	if !useZeroComplement {
		if diff.oldExists {
			addLenFieldPostingDeltaBucket(&fieldDelta, idx, diff.oldLen, false)
			changed = true
		}
		if diff.newExists {
			addLenFieldPostingDeltaBucket(&fieldDelta, idx, diff.newLen, true)
			changed = true
		}
		return fieldDelta, changed
	}

	if diff.oldExists {
		if diff.oldLen > 0 {
			addLenFieldPostingDeltaBucket(&fieldDelta, idx, diff.oldLen, false)
			changed = true
		}
		if diff.oldLen > 0 && (!diff.newExists || diff.newLen == 0) {
			addLenFieldPostingNonEmptyDelta(&fieldDelta, idx, false)
			changed = true
		}
	}
	if diff.newExists {
		if diff.newLen > 0 {
			addLenFieldPostingDeltaBucket(&fieldDelta, idx, diff.newLen, true)
			changed = true
		}
		if diff.newLen > 0 && (!diff.oldExists || diff.oldLen == 0) {
			addLenFieldPostingNonEmptyDelta(&fieldDelta, idx, true)
			changed = true
		}
	}

	return fieldDelta, changed || logicalChange
}

func batchPostingDeltaMutationCardinality(delta batchPostingDelta) uint64 {
	return delta.add.Cardinality() + delta.remove.Cardinality()
}

func postingListLooksRunFriendly(ids posting.List) bool {
	if ids.IsEmpty() {
		return false
	}
	if _, ok := ids.TrySingle(); ok {
		return false
	}
	card := ids.Cardinality()
	if card < 2 {
		return false
	}
	minID, ok := ids.Minimum()
	if !ok {
		return false
	}
	maxID, ok := ids.Maximum()
	if !ok || maxID < minID {
		return false
	}
	span := maxID - minID + 1
	return span <= card*2
}

func shouldOptimizeAfterBatchDelta(base posting.List, delta batchPostingDelta, out posting.List) bool {
	if out.IsEmpty() {
		return false
	}
	if _, ok := out.TrySingle(); ok {
		return false
	}
	if postingListLooksRunFriendly(delta.add) || postingListLooksRunFriendly(delta.remove) || postingListLooksRunFriendly(out) {
		return true
	}
	if base.IsEmpty() {
		return false
	}
	if _, ok := base.TrySingle(); ok {
		return false
	}
	mutation := batchPostingDeltaMutationCardinality(delta)
	if mutation == 0 {
		return false
	}
	baseCard := base.Cardinality()
	return baseCard > 0 && mutation*4 >= baseCard
}

func applyBatchPostingDeltaOwned(base posting.List, delta *batchPostingDelta) posting.List {
	if delta == nil {
		return base
	}
	deltaValue := *delta
	out := base
	changed := false
	releaseRemove := !deltaValue.remove.IsEmpty()
	releaseAdd := !deltaValue.add.IsEmpty()

	if !deltaValue.remove.IsEmpty() && !out.IsEmpty() {
		out = out.Clone()
		out = out.BuildAndNot(deltaValue.remove)
		changed = true
	}

	if !deltaValue.add.IsEmpty() {
		if out.IsEmpty() {
			out = deltaValue.add
			releaseAdd = false
			changed = true
		} else {
			if !changed {
				out = out.Clone()
				changed = true
			}
			out = out.BuildOr(deltaValue.add)
		}
	}

	if changed && shouldOptimizeAfterBatchDelta(base, deltaValue, out) {
		out = out.BuildOptimized()
	}
	if releaseRemove {
		deltaValue.remove.Release()
	}
	if releaseAdd {
		deltaValue.add.Release()
	}
	delta.remove = posting.List{}
	delta.add = posting.List{}
	return out
}

func sortedBatchPostingDeltasBufOwned(
	deltas map[string]uint32,
	arena *batchPostingAccumArena,
	fixed8 bool,
) *pooled.Slice[keyedBatchPostingDelta] {
	if len(deltas) == 0 {
		batchPostingAccumMapPool.Put(deltas)
		return nil
	}

	buf := keyedBatchPostingDeltaSlicePool.Get()
	buf.Grow(len(deltas))

	for raw, ref := range deltas {
		delta := arena.accum(ref).materializeOwned()
		if delta.add.IsEmpty() && delta.remove.IsEmpty() {
			continue
		}
		buf.Append(keyedBatchPostingDelta{
			key:   keycodec.FromStoredString(raw, fixed8),
			delta: delta,
		})
	}
	batchPostingAccumMapPool.Put(deltas)
	if buf.Len() == 0 {
		keyedBatchPostingDeltaSlicePool.Put(buf)
		return nil
	}

	sortKeyedBatchPostingDeltasBuf(buf)
	return buf
}

func sortedFixedBatchPostingDeltasBufOwned(
	deltas map[uint64]uint32,
	arena *batchPostingAccumArena,
) *pooled.Slice[keyedBatchPostingDelta] {
	if len(deltas) == 0 {
		fixedBatchPostingAccumMapPool.Put(deltas)
		return nil
	}

	buf := keyedBatchPostingDeltaSlicePool.Get()
	buf.Grow(len(deltas))

	for raw, ref := range deltas {
		delta := arena.accum(ref).materializeOwned()
		if delta.add.IsEmpty() && delta.remove.IsEmpty() {
			continue
		}
		buf.Append(keyedBatchPostingDelta{
			key:   keycodec.FromU64(raw),
			delta: delta,
		})
	}
	fixedBatchPostingAccumMapPool.Put(deltas)
	if buf.Len() == 0 {
		keyedBatchPostingDeltaSlicePool.Put(buf)
		return nil
	}

	sortKeyedBatchPostingDeltasBuf(buf)
	return buf
}

func applyFieldPostingDiffSorted(base *[]index, deltaKeys []keyedBatchPostingDelta) *[]index {
	if len(deltaKeys) == 0 {
		return base
	}
	if len(deltaKeys) == 1 {
		return applySingleFieldPostingDiffSorted(base, deltaKeys[0])
	}

	var src []index
	if base != nil {
		src = *base
	}
	out := make([]index, 0, len(src)+len(deltaKeys))

	i, j := 0, 0
	for i < len(src) || j < len(deltaKeys) {
		switch {
		case j >= len(deltaKeys):
			out = appendBorrowedIndexEntries(out, src[i:])
			i = len(src)
		case i >= len(src):
			ids := applyBatchPostingDeltaOwned(posting.List{}, &deltaKeys[j].delta)
			if !ids.IsEmpty() {
				out = append(out, index{Key: deltaKeys[j].key, IDs: ids})
			}
			j++
		default:
			cmp := keycodec.Compare(src[i].Key, deltaKeys[j].key)
			switch {
			case cmp < 0:
				out = append(out, borrowedFieldIndexEntry(src[i]))
				i++
			case cmp > 0:
				ids := applyBatchPostingDeltaOwned(posting.List{}, &deltaKeys[j].delta)
				if !ids.IsEmpty() {
					out = append(out, index{Key: deltaKeys[j].key, IDs: ids})
				}
				j++
			default:
				ids := applyBatchPostingDeltaOwned(src[i].IDs, &deltaKeys[j].delta)
				if !ids.IsEmpty() {
					out = append(out, index{Key: src[i].Key, IDs: ids})
				}
				i++
				j++
			}
		}
	}

	if len(out) == 0 {
		return nil
	}
	return &out
}

func applyFieldPostingDiffSortedBuf(base *[]index, deltaKeys *pooled.Slice[keyedBatchPostingDelta]) *[]index {
	if deltaKeys == nil || deltaKeys.Len() == 0 {
		return base
	}
	if deltaKeys.Len() == 1 {
		return applySingleFieldPostingDiffSorted(base, takeKeyedBatchPostingDeltaBuf(deltaKeys, 0))
	}

	var src []index
	if base != nil {
		src = *base
	}
	out := make([]index, 0, len(src)+deltaKeys.Len())

	i, j := 0, 0
	for i < len(src) || j < deltaKeys.Len() {
		switch {
		case j >= deltaKeys.Len():
			out = appendBorrowedIndexEntries(out, src[i:])
			i = len(src)
		case i >= len(src):
			delta := takeKeyedBatchPostingDeltaBuf(deltaKeys, j)
			ids := applyBatchPostingDeltaOwned(posting.List{}, &delta.delta)
			if !ids.IsEmpty() {
				out = append(out, index{Key: delta.key, IDs: ids})
			}
			j++
		default:
			delta := deltaKeys.Get(j)
			cmp := keycodec.Compare(src[i].Key, delta.key)
			switch {
			case cmp < 0:
				out = append(out, borrowedFieldIndexEntry(src[i]))
				i++
			case cmp > 0:
				delta = takeKeyedBatchPostingDeltaBuf(deltaKeys, j)
				ids := applyBatchPostingDeltaOwned(posting.List{}, &delta.delta)
				if !ids.IsEmpty() {
					out = append(out, index{Key: delta.key, IDs: ids})
				}
				j++
			default:
				delta = takeKeyedBatchPostingDeltaBuf(deltaKeys, j)
				ids := applyBatchPostingDeltaOwned(src[i].IDs, &delta.delta)
				if !ids.IsEmpty() {
					out = append(out, index{Key: src[i].Key, IDs: ids})
				}
				i++
				j++
			}
		}
	}

	return &out
}

func applySingleFieldPostingDiffSorted(base *[]index, delta keyedBatchPostingDelta) *[]index {
	var src []index
	if base != nil {
		src = *base
	}
	if len(src) == 0 {
		ids := applyBatchPostingDeltaOwned(posting.List{}, &delta.delta)
		if ids.IsEmpty() {
			return nil
		}
		out := []index{{Key: delta.key, IDs: ids}}
		return &out
	}

	pos := lowerBoundIndexEntriesKey(src, delta.key)
	if pos >= len(src) || keycodec.Compare(src[pos].Key, delta.key) > 0 {
		ids := applyBatchPostingDeltaOwned(posting.List{}, &delta.delta)
		if ids.IsEmpty() {
			return base
		}
		out := make([]index, len(src)+1)
		copyBorrowedIndexEntries(out[:pos], src[:pos])
		out[pos] = index{Key: delta.key, IDs: ids}
		copyBorrowedIndexEntries(out[pos+1:], src[pos:])
		return &out
	}

	ids := applyBatchPostingDeltaOwned(src[pos].IDs, &delta.delta)
	if ids.IsEmpty() {
		if len(src) == 1 {
			return nil
		}
		out := make([]index, len(src)-1)
		copyBorrowedIndexEntries(out[:pos], src[:pos])
		copyBorrowedIndexEntries(out[pos:], src[pos+1:])
		return &out
	}

	out := make([]index, len(src))
	copyBorrowedIndexEntries(out, src)
	out[pos].IDs = ids
	return &out
}

func appendFieldPostingDiffFlatSorted(builder *fieldIndexChunkBuilder, base *[]index, deltaKeys []keyedBatchPostingDelta) {
	if builder == nil {
		return
	}

	var src []index
	if base != nil {
		src = *base
	}
	if len(src) == 0 && len(deltaKeys) == 0 {
		return
	}

	numeric := false
	if len(src) > 0 {
		numeric = src[0].Key.IsNumeric()
	} else if len(deltaKeys) > 0 {
		numeric = deltaKeys[0].key.IsNumeric()
	}
	out := newFieldIndexChunkStreamBuilder(builder, numeric)

	i, j := 0, 0
	for i < len(src) || j < len(deltaKeys) {
		switch {
		case j >= len(deltaKeys):
			out.append(src[i].Key, src[i].IDs.Borrow())
			i++
		case i >= len(src):
			ids := applyBatchPostingDeltaOwned(posting.List{}, &deltaKeys[j].delta)
			if !ids.IsEmpty() {
				out.append(deltaKeys[j].key, ids)
			}
			j++
		default:
			cmp := keycodec.Compare(src[i].Key, deltaKeys[j].key)
			switch {
			case cmp < 0:
				out.append(src[i].Key, src[i].IDs.Borrow())
				i++
			case cmp > 0:
				ids := applyBatchPostingDeltaOwned(posting.List{}, &deltaKeys[j].delta)
				if !ids.IsEmpty() {
					out.append(deltaKeys[j].key, ids)
				}
				j++
			default:
				ids := applyBatchPostingDeltaOwned(src[i].IDs, &deltaKeys[j].delta)
				if !ids.IsEmpty() {
					out.append(src[i].Key, ids)
				}
				i++
				j++
			}
		}
	}
	out.finish()
}

func appendFieldPostingDiffFlatSortedBuf(builder *fieldIndexChunkBuilder, base *[]index, deltaKeys *pooled.Slice[keyedBatchPostingDelta]) {
	if builder == nil {
		return
	}

	var src []index
	if base != nil {
		src = *base
	}
	if len(src) == 0 && (deltaKeys == nil || deltaKeys.Len() == 0) {
		return
	}

	numeric := false
	if len(src) > 0 {
		numeric = src[0].Key.IsNumeric()
	} else if deltaKeys != nil && deltaKeys.Len() > 0 {
		numeric = deltaKeys.Get(0).key.IsNumeric()
	}
	out := newFieldIndexChunkStreamBuilder(builder, numeric)

	i, j := 0, 0
	for i < len(src) || (deltaKeys != nil && j < deltaKeys.Len()) {
		switch {
		case deltaKeys == nil || j >= deltaKeys.Len():
			out.append(src[i].Key, src[i].IDs.Borrow())
			i++
		case i >= len(src):
			delta := takeKeyedBatchPostingDeltaBuf(deltaKeys, j)
			ids := applyBatchPostingDeltaOwned(posting.List{}, &delta.delta)
			if !ids.IsEmpty() {
				out.append(delta.key, ids)
			}
			j++
		default:
			delta := deltaKeys.Get(j)
			cmp := keycodec.Compare(src[i].Key, delta.key)
			switch {
			case cmp < 0:
				out.append(src[i].Key, src[i].IDs.Borrow())
				i++
			case cmp > 0:
				delta = takeKeyedBatchPostingDeltaBuf(deltaKeys, j)
				ids := applyBatchPostingDeltaOwned(posting.List{}, &delta.delta)
				if !ids.IsEmpty() {
					out.append(delta.key, ids)
				}
				j++
			default:
				delta = takeKeyedBatchPostingDeltaBuf(deltaKeys, j)
				ids := applyBatchPostingDeltaOwned(src[i].IDs, &delta.delta)
				if !ids.IsEmpty() {
					out.append(src[i].Key, ids)
				}
				i++
				j++
			}
		}
	}
	out.finish()
}

func applyFieldPostingDiffFlatMaybeChunked(base *[]index, deltaKeys []keyedBatchPostingDelta) fieldIndexStorage {
	est := len(deltaKeys)
	if base != nil {
		est += len(*base)
	}
	builder := newFieldIndexChunkBuilder(est)
	appendFieldPostingDiffFlatSorted(&builder, base, deltaKeys)
	root := builder.root()
	if root == nil {
		return fieldIndexStorage{}
	}
	if root.keyCount < fieldIndexChunkThreshold {
		return newFlatFieldIndexStorage(flattenChunkedFieldIndexRoot(root))
	}
	return newChunkedFieldIndexStorage(root)
}

func applyFieldPostingDiffFlatMaybeChunkedBuf(base *[]index, deltaKeys *pooled.Slice[keyedBatchPostingDelta]) fieldIndexStorage {
	est := 0
	if deltaKeys != nil {
		est = deltaKeys.Len()
	}
	if base != nil {
		est += len(*base)
	}
	builder := newFieldIndexChunkBuilder(est)
	appendFieldPostingDiffFlatSortedBuf(&builder, base, deltaKeys)
	root := builder.root()
	if root == nil {
		return fieldIndexStorage{}
	}
	if root.keyCount < fieldIndexChunkThreshold {
		return newFlatFieldIndexStorage(flattenChunkedFieldIndexRoot(root))
	}
	return newChunkedFieldIndexStorage(root)
}

func appendFieldPostingDiffChunkRangeSorted(
	builder *fieldIndexChunkBuilder,
	base *fieldIndexChunkedRoot,
	startChunk, endChunk int,
	deltaKeys []keyedBatchPostingDelta,
) {
	if builder == nil || base == nil || startChunk < 0 || endChunk < startChunk || endChunk > base.chunkCount {
		return
	}
	if len(deltaKeys) == 0 {
		builder.appendRefsRange(base, startChunk, endChunk)
		return
	}

	numeric := false
	if startChunk < endChunk {
		if ref, ok := base.refAtChunk(startChunk); ok && ref.chunk != nil && ref.chunk.hasNumericKeys() {
			numeric = true
		}
	} else if len(deltaKeys) > 0 {
		numeric = deltaKeys[0].key.IsNumeric()
	}
	out := newFieldIndexChunkStreamBuilder(builder, numeric)

	chunkIdx := startChunk
	entryIdx := 0
	j := 0

	for {
		baseEnt, hasBase := nextFieldPostingDiffBaseEntry(base, endChunk, chunkIdx, entryIdx)
		switch {
		case !hasBase && j >= len(deltaKeys):
			out.finish()
			return
		case !hasBase:
			ids := applyBatchPostingDeltaOwned(posting.List{}, &deltaKeys[j].delta)
			if !ids.IsEmpty() {
				out.append(deltaKeys[j].key, ids)
			}
			j++
		case j >= len(deltaKeys):
			out.append(baseEnt.Key, baseEnt.IDs)
			advanceFieldPostingDiffBaseEntry(base, endChunk, &chunkIdx, &entryIdx)
		default:
			cmp := keycodec.Compare(baseEnt.Key, deltaKeys[j].key)
			switch {
			case cmp < 0:
				out.append(baseEnt.Key, baseEnt.IDs)
				advanceFieldPostingDiffBaseEntry(base, endChunk, &chunkIdx, &entryIdx)
			case cmp > 0:
				ids := applyBatchPostingDeltaOwned(posting.List{}, &deltaKeys[j].delta)
				if !ids.IsEmpty() {
					out.append(deltaKeys[j].key, ids)
				}
				j++
			default:
				ids := applyBatchPostingDeltaOwned(baseEnt.IDs, &deltaKeys[j].delta)
				if !ids.IsEmpty() {
					out.append(baseEnt.Key, ids)
				}
				advanceFieldPostingDiffBaseEntry(base, endChunk, &chunkIdx, &entryIdx)
				j++
			}
		}
	}
}

func appendFieldPostingDiffChunkRangeSortedBuf(
	builder *fieldIndexChunkBuilder,
	base *fieldIndexChunkedRoot,
	startChunk, endChunk int,
	deltaKeys *pooled.Slice[keyedBatchPostingDelta],
	deltaStart int,
	deltaEnd int,
) {
	if builder == nil || base == nil || startChunk < 0 || endChunk < startChunk || endChunk > base.chunkCount {
		return
	}
	if deltaKeys == nil || deltaStart >= deltaEnd {
		builder.appendRefsRange(base, startChunk, endChunk)
		return
	}

	numeric := false
	if startChunk < endChunk {
		if ref, ok := base.refAtChunk(startChunk); ok && ref.chunk != nil && ref.chunk.hasNumericKeys() {
			numeric = true
		}
	} else {
		numeric = deltaKeys.Get(deltaStart).key.IsNumeric()
	}
	out := newFieldIndexChunkStreamBuilder(builder, numeric)

	chunkIdx := startChunk
	entryIdx := 0
	j := deltaStart

	for {
		baseEnt, hasBase := nextFieldPostingDiffBaseEntry(base, endChunk, chunkIdx, entryIdx)
		switch {
		case !hasBase && j >= deltaEnd:
			out.finish()
			return
		case !hasBase:
			delta := takeKeyedBatchPostingDeltaBuf(deltaKeys, j)
			ids := applyBatchPostingDeltaOwned(posting.List{}, &delta.delta)
			if !ids.IsEmpty() {
				out.append(delta.key, ids)
			}
			j++
		case j >= deltaEnd:
			out.append(baseEnt.Key, baseEnt.IDs)
			advanceFieldPostingDiffBaseEntry(base, endChunk, &chunkIdx, &entryIdx)
		default:
			delta := deltaKeys.Get(j)
			cmp := keycodec.Compare(baseEnt.Key, delta.key)
			switch {
			case cmp < 0:
				out.append(baseEnt.Key, baseEnt.IDs)
				advanceFieldPostingDiffBaseEntry(base, endChunk, &chunkIdx, &entryIdx)
			case cmp > 0:
				delta = takeKeyedBatchPostingDeltaBuf(deltaKeys, j)
				ids := applyBatchPostingDeltaOwned(posting.List{}, &delta.delta)
				if !ids.IsEmpty() {
					out.append(delta.key, ids)
				}
				j++
			default:
				delta = takeKeyedBatchPostingDeltaBuf(deltaKeys, j)
				ids := applyBatchPostingDeltaOwned(baseEnt.IDs, &delta.delta)
				if !ids.IsEmpty() {
					out.append(baseEnt.Key, ids)
				}
				advanceFieldPostingDiffBaseEntry(base, endChunk, &chunkIdx, &entryIdx)
				j++
			}
		}
	}
}

func applyFieldPostingDiffStorageOwned(
	base fieldIndexStorage,
	deltas map[string]uint32,
	arena *batchPostingAccumArena,
	fixed8 bool,
	allowChunk bool,
) fieldIndexStorage {
	buf := sortedBatchPostingDeltasBufOwned(deltas, arena, fixed8)
	if buf == nil {
		return base
	}
	defer keyedBatchPostingDeltaSlicePool.Put(buf)
	if !allowChunk {
		return newFlatFieldIndexStorage(applyFieldPostingDiffSortedBuf(base.flatSlice(), buf))
	}
	if base.chunked != nil {
		return applyFieldPostingDiffChunkedBuf(base.chunked, buf)
	}
	flat := base.flatSlice()
	baseCount := 0
	if flat != nil {
		baseCount = len(*flat)
	}
	if buf.Len()+baseCount >= fieldIndexChunkThreshold {
		return applyFieldPostingDiffFlatMaybeChunkedBuf(flat, buf)
	}
	return newRegularFieldIndexStorage(applyFieldPostingDiffSortedBuf(flat, buf))
}

func applyFixedFieldPostingDiffStorageOwned(
	base fieldIndexStorage,
	deltas map[uint64]uint32,
	arena *batchPostingAccumArena,
	allowChunk bool,
) fieldIndexStorage {
	buf := sortedFixedBatchPostingDeltasBufOwned(deltas, arena)
	if buf == nil {
		return base
	}
	defer keyedBatchPostingDeltaSlicePool.Put(buf)
	if !allowChunk {
		return newFlatFieldIndexStorage(applyFieldPostingDiffSortedBuf(base.flatSlice(), buf))
	}
	if base.chunked != nil {
		return applyFieldPostingDiffChunkedBuf(base.chunked, buf)
	}
	flat := base.flatSlice()
	baseCount := 0
	if flat != nil {
		baseCount = len(*flat)
	}
	if buf.Len()+baseCount >= fieldIndexChunkThreshold {
		return applyFieldPostingDiffFlatMaybeChunkedBuf(flat, buf)
	}
	return newRegularFieldIndexStorage(applyFieldPostingDiffSortedBuf(flat, buf))
}

func sortedLenFieldPostingDeltasBufOwned(deltas *lenFieldPostingDelta) *pooled.Slice[keyedBatchPostingDelta] {
	if deltas == nil {
		return nil
	}
	count := len(deltas.lengths)
	if deltas.hasNonEmpty {
		count++
	}
	if count == 0 {
		putLenFieldPostingDelta(deltas)
		return nil
	}

	buf := keyedBatchPostingDeltaSlicePool.Get()
	buf.Grow(count)
	for raw, delta := range deltas.lengths {
		if delta.add.IsEmpty() && delta.remove.IsEmpty() {
			continue
		}
		buf.Append(keyedBatchPostingDelta{
			key:   keycodec.FromU64(raw),
			delta: delta,
		})
	}
	if deltas.hasNonEmpty && (!deltas.nonEmpty.add.IsEmpty() || !deltas.nonEmpty.remove.IsEmpty()) {
		buf.Append(keyedBatchPostingDelta{
			key:   keycodec.FromString(lenIndexNonEmptyKey),
			delta: deltas.nonEmpty,
		})
	}
	putLenFieldPostingDelta(deltas)
	if buf.Len() == 0 {
		keyedBatchPostingDeltaSlicePool.Put(buf)
		return nil
	}
	sortKeyedBatchPostingDeltasBuf(buf)
	return buf
}

func lenFieldPostingDeltaCount(deltas *lenFieldPostingDelta) int {
	if deltas == nil {
		return 0
	}
	count := 0
	for _, delta := range deltas.lengths {
		if delta.add.IsEmpty() && delta.remove.IsEmpty() {
			continue
		}
		count++
	}
	if deltas.hasNonEmpty && (!deltas.nonEmpty.add.IsEmpty() || !deltas.nonEmpty.remove.IsEmpty()) {
		count++
	}
	return count
}

func takeLenFieldPostingDeltasOwned(deltas *lenFieldPostingDelta, dst []keyedBatchPostingDelta) int {
	if deltas == nil || len(dst) == 0 {
		return 0
	}
	n := 0
	for raw, delta := range deltas.lengths {
		if delta.add.IsEmpty() && delta.remove.IsEmpty() {
			continue
		}
		dst[n] = keyedBatchPostingDelta{
			key:   keycodec.FromU64(raw),
			delta: delta,
		}
		n++
		if n == len(dst) {
			break
		}
	}
	if n < len(dst) && deltas.hasNonEmpty && (!deltas.nonEmpty.add.IsEmpty() || !deltas.nonEmpty.remove.IsEmpty()) {
		dst[n] = keyedBatchPostingDelta{
			key:   keycodec.FromString(lenIndexNonEmptyKey),
			delta: deltas.nonEmpty,
		}
		n++
	}
	putLenFieldPostingDelta(deltas)
	return n
}

func applyLenFieldPostingDiffStorageOwned(base fieldIndexStorage, deltas *lenFieldPostingDelta) fieldIndexStorage {
	count := lenFieldPostingDeltaCount(deltas)
	if count == 0 {
		putLenFieldPostingDelta(deltas)
		return base
	}
	if count == 1 {
		delta, ok := takeSingleLenFieldPostingDeltaOwned(deltas)
		if !ok {
			return base
		}
		var storage fieldIndexStorage
		if base.chunked != nil {
			storage = applySingleFieldPostingDiffChunked(base.chunked, delta)
		} else {
			storage = newFlatFieldIndexStorage(applySingleFieldPostingDiffSorted(base.flatSlice(), delta))
		}
		if storage.flat != nil || storage.chunked != nil {
			return storage
		}
		empty := make([]index, 0)
		return newFlatFieldIndexStorage(&empty)
	}
	var deltaKeys []keyedBatchPostingDelta
	var inline [2]keyedBatchPostingDelta
	var buf *pooled.Slice[keyedBatchPostingDelta]
	if count <= len(inline) {
		n := takeLenFieldPostingDeltasOwned(deltas, inline[:count])
		deltaKeys = inline[:n]
		if len(deltaKeys) > 1 && keycodec.Compare(deltaKeys[0].key, deltaKeys[1].key) > 0 {
			deltaKeys[0], deltaKeys[1] = deltaKeys[1], deltaKeys[0]
		}
	} else {
		buf = sortedLenFieldPostingDeltasBufOwned(deltas)
		if buf == nil {
			return base
		}
		defer keyedBatchPostingDeltaSlicePool.Put(buf)
	}

	var storage fieldIndexStorage
	if buf != nil {
		storage = newFlatFieldIndexStorage(applyFieldPostingDiffSortedBuf(base.flatSlice(), buf))
	} else {
		storage = newFlatFieldIndexStorage(applyFieldPostingDiffSorted(base.flatSlice(), deltaKeys))
	}
	if storage.flat != nil || storage.chunked != nil {
		return storage
	}
	empty := make([]index, 0)
	return newFlatFieldIndexStorage(&empty)
}

func takeSingleLenFieldPostingDeltaOwned(deltas *lenFieldPostingDelta) (keyedBatchPostingDelta, bool) {
	if deltas == nil {
		return keyedBatchPostingDelta{}, false
	}
	for raw, delta := range deltas.lengths {
		if delta.add.IsEmpty() && delta.remove.IsEmpty() {
			continue
		}
		putLenFieldPostingDelta(deltas)
		return keyedBatchPostingDelta{
			key:   keycodec.FromU64(raw),
			delta: delta,
		}, true
	}
	if deltas.hasNonEmpty && (!deltas.nonEmpty.add.IsEmpty() || !deltas.nonEmpty.remove.IsEmpty()) {
		delta := deltas.nonEmpty
		putLenFieldPostingDelta(deltas)
		return keyedBatchPostingDelta{
			key:   keycodec.FromString(lenIndexNonEmptyKey),
			delta: delta,
		}, true
	}
	putLenFieldPostingDelta(deltas)
	return keyedBatchPostingDelta{}, false
}

func applyFieldPostingDiffChunked(
	base *fieldIndexChunkedRoot,
	deltaKeys []keyedBatchPostingDelta,
) fieldIndexStorage {
	if base == nil || base.keyCount == 0 {
		return applyFieldPostingDiffFlatMaybeChunked(nil, deltaKeys)
	}
	if len(deltaKeys) == 0 {
		return newChunkedFieldIndexStorage(base)
	}
	if len(deltaKeys) == 1 {
		return applySingleFieldPostingDiffChunked(base, deltaKeys[0])
	}

	builder := newFieldIndexChunkBuilder(base.keyCount + len(deltaKeys))
	chunkIdx := 0
	deltaPos := 0
	touchIdx := 0
	for deltaPos < len(deltaKeys) {
		touchIdx = base.touchChunkIndexFrom(max(touchIdx, chunkIdx), deltaKeys[deltaPos].key)
		if touchIdx < 0 {
			break
		}
		if chunkIdx < touchIdx {
			builder.appendRefsRange(base, chunkIdx, touchIdx)
		}

		runStart := touchIdx
		runEnd := touchIdx + 1
		deltaStart := deltaPos
		deltaPos++
		for deltaPos < len(deltaKeys) {
			touchIdx = base.touchChunkIndexFrom(touchIdx, deltaKeys[deltaPos].key)
			if touchIdx > runEnd {
				break
			}
			if touchIdx == runEnd {
				runEnd++
			}
			deltaPos++
		}

		appendFieldPostingDiffChunkRangeSorted(&builder, base, runStart, runEnd, deltaKeys[deltaStart:deltaPos])
		chunkIdx = runEnd
	}
	if chunkIdx < base.chunkCount {
		builder.appendRefsRange(base, chunkIdx, base.chunkCount)
	}

	root := builder.root()
	if root == nil {
		return fieldIndexStorage{}
	}
	if root.keyCount < fieldIndexChunkThreshold {
		return newFlatFieldIndexStorage(flattenChunkedFieldIndexRoot(root))
	}
	return newChunkedFieldIndexStorage(root)
}

func applyFieldPostingDiffChunkedBuf(
	base *fieldIndexChunkedRoot,
	deltaKeys *pooled.Slice[keyedBatchPostingDelta],
) fieldIndexStorage {
	if base == nil || base.keyCount == 0 {
		return applyFieldPostingDiffFlatMaybeChunkedBuf(nil, deltaKeys)
	}
	if deltaKeys == nil || deltaKeys.Len() == 0 {
		return newChunkedFieldIndexStorage(base)
	}
	if deltaKeys.Len() == 1 {
		return applySingleFieldPostingDiffChunked(base, takeKeyedBatchPostingDeltaBuf(deltaKeys, 0))
	}

	builder := newFieldIndexChunkBuilder(base.keyCount + deltaKeys.Len())
	chunkIdx := 0
	deltaPos := 0
	touchIdx := 0
	for deltaPos < deltaKeys.Len() {
		touchIdx = base.touchChunkIndexFrom(max(touchIdx, chunkIdx), deltaKeys.Get(deltaPos).key)
		if touchIdx < 0 {
			break
		}
		if chunkIdx < touchIdx {
			builder.appendRefsRange(base, chunkIdx, touchIdx)
		}

		runStart := touchIdx
		runEnd := touchIdx + 1
		deltaStart := deltaPos
		deltaPos++
		for deltaPos < deltaKeys.Len() {
			touchIdx = base.touchChunkIndexFrom(touchIdx, deltaKeys.Get(deltaPos).key)
			if touchIdx > runEnd {
				break
			}
			if touchIdx == runEnd {
				runEnd++
			}
			deltaPos++
		}

		appendFieldPostingDiffChunkRangeSortedBuf(&builder, base, runStart, runEnd, deltaKeys, deltaStart, deltaPos)
		chunkIdx = runEnd
	}
	if chunkIdx < base.chunkCount {
		builder.appendRefsRange(base, chunkIdx, base.chunkCount)
	}

	root := builder.root()
	if root == nil {
		return fieldIndexStorage{}
	}
	if root.keyCount < fieldIndexChunkThreshold {
		return newFlatFieldIndexStorage(flattenChunkedFieldIndexRoot(root))
	}
	return newChunkedFieldIndexStorage(root)
}

func fieldIndexChunkEntriesBorrowed(chunk *fieldIndexChunk) []index {
	if chunk == nil || chunk.keyCount() == 0 {
		return nil
	}
	entries := make([]index, 0, chunk.keyCount())
	for i := 0; i < chunk.keyCount(); i++ {
		entries = append(entries, index{
			Key: chunk.keyAt(i),
			IDs: chunk.postingAt(i),
		})
	}
	return entries
}

func rebuildChunkedRootWithOwnedPageRefsReplaced(
	base *fieldIndexChunkedRoot,
	page int,
	replRefs *pooled.Slice[fieldIndexChunkRef],
) *fieldIndexChunkedRoot {
	if base == nil || base.pages == nil || page < 0 || page >= base.pages.Len() {
		if replRefs != nil {
			fieldIndexChunkRefSlicePool.Put(replRefs)
		}
		return nil
	}
	est := base.keyCount - base.pages.Get(page).keyCount()
	if replRefs != nil {
		for i := 0; i < replRefs.Len(); i++ {
			if chunk := replRefs.Get(i).chunk; chunk != nil {
				est += chunk.keyCount()
			}
		}
	}
	if est < 0 {
		est = 0
	}
	builder := newFieldIndexChunkBuilder(est)
	for i := 0; i < page; i++ {
		builder.appendPage(base.pages.Get(i))
	}
	if replRefs != nil {
		for i := 0; i < replRefs.Len(); i++ {
			builder.appendOwnedRef(replRefs.Get(i))
		}
		fieldIndexChunkRefSlicePool.Put(replRefs)
	}
	for i := page + 1; i < base.pages.Len(); i++ {
		builder.appendPage(base.pages.Get(i))
	}
	return builder.root()
}

func newFieldIndexChunkRefBufWithReplacedRef(
	page *fieldIndexChunkDirPage,
	off int,
	replRefs []fieldIndexChunkRef,
) *pooled.Slice[fieldIndexChunkRef] {
	if page == nil || off < 0 || off >= page.refs.Len() {
		return nil
	}
	total := page.refs.Len() - 1 + len(replRefs)
	if total <= 0 {
		return nil
	}
	refs := newFieldIndexChunkRefBuf(total)
	for i := 0; i < off; i++ {
		refs.Append(retainedFieldIndexChunkRef(page.refs.Get(i)))
	}
	for i := range replRefs {
		refs.Append(replRefs[i])
	}
	for i := off + 1; i < page.refs.Len(); i++ {
		refs.Append(retainedFieldIndexChunkRef(page.refs.Get(i)))
	}
	return refs
}

func applySingleFieldPostingDiffChunked(base *fieldIndexChunkedRoot, delta keyedBatchPostingDelta) fieldIndexStorage {
	if base == nil || base.keyCount == 0 {
		return applyFieldPostingDiffFlatMaybeChunked(nil, []keyedBatchPostingDelta{delta})
	}

	touchIdx := base.touchChunkIndexFrom(0, delta.key)
	if touchIdx < 0 {
		return newChunkedFieldIndexStorage(base)
	}
	page, off := base.pagePosForChunk(touchIdx)
	if base.pages == nil || page >= base.pages.Len() {
		return newChunkedFieldIndexStorage(base)
	}
	pageRef := base.pages.Get(page)

	ref, ok := base.refAtChunk(touchIdx)
	if !ok || ref.chunk == nil {
		return newChunkedFieldIndexStorage(base)
	}
	entryIdx := lowerBoundFieldIndexChunkKey(ref.chunk, delta.key)
	if delta.delta.remove.IsEmpty() &&
		!delta.delta.add.IsEmpty() &&
		entryIdx < ref.chunk.keyCount() &&
		keycodec.Compare(ref.chunk.keyAt(entryIdx), delta.key) == 0 {
		baseIDs := ref.chunk.postingAt(entryIdx)
		updatedIDs := applyBatchPostingDeltaOwned(baseIDs, &delta.delta)
		if updatedIDs.SharesPayload(baseIDs) {
			return newChunkedFieldIndexStorage(base)
		}
		rowsDelta := int64(updatedIDs.Cardinality()) - int64(baseIDs.Cardinality())
		rows := ref.chunk.rows
		if rowsDelta > 0 {
			rows += uint64(rowsDelta)
		} else if rowsDelta < 0 {
			rows -= uint64(-rowsDelta)
		}
		posts := make([]posting.List, ref.chunk.keyCount())
		for i := range posts {
			posts[i] = ref.chunk.postingAt(i)
		}
		posts[entryIdx] = updatedIDs
		var chunk *fieldIndexChunk
		if ref.chunk.hasNumericKeys() {
			if ref.chunk.hasUniqueNumericOwners() {
				keys := make([]uint64, ref.chunk.keyCount())
				for i := range keys {
					keys[i] = ref.chunk.keyAt(i).U64()
				}
				chunk = newNumericFieldIndexChunk(posts, keys, rows)
			} else {
				chunk = newNumericFieldIndexChunk(posts, ref.chunk.numeric, rows)
			}
		} else {
			chunk = newStringFieldIndexChunk(posts, ref.chunk.stringRefs, ref.chunk.stringData, rows)
		}
		repl := [1]fieldIndexChunkRef{{
			last:  ref.last,
			chunk: chunk,
		}}
		root := rebuildChunkedRootWithOwnedPageRefsReplaced(base, page, newFieldIndexChunkRefBufWithReplacedRef(pageRef, off, repl[:]))
		if root == nil {
			return fieldIndexStorage{}
		}
		return newChunkedFieldIndexStorage(root)
	}
	if delta.delta.remove.IsEmpty() && !delta.delta.add.IsEmpty() {
		replRefs := newFieldIndexChunkRefsWithInsertedEntry(ref, entryIdx, index{
			Key: delta.key,
			IDs: delta.delta.add,
		})
		if len(replRefs) > 0 {
			root := rebuildChunkedRootWithOwnedPageRefsReplaced(base, page, newFieldIndexChunkRefBufWithReplacedRef(pageRef, off, replRefs))
			if root == nil {
				return fieldIndexStorage{}
			}
			if root.keyCount < fieldIndexChunkThreshold {
				return newFlatFieldIndexStorage(flattenChunkedFieldIndexRoot(root))
			}
			return newChunkedFieldIndexStorage(root)
		}
	}

	entries := fieldIndexChunkEntriesBorrowed(ref.chunk)
	updated := applySingleFieldPostingDiffSorted(&entries, delta)
	var replRefs []fieldIndexChunkRef
	if updated != nil {
		replRefs = newFieldIndexChunkRefsFromEntries(*updated)
	}
	root := rebuildChunkedRootWithOwnedPageRefsReplaced(base, page, newFieldIndexChunkRefBufWithReplacedRef(pageRef, off, replRefs))
	if root == nil {
		return fieldIndexStorage{}
	}
	if root.keyCount < fieldIndexChunkThreshold {
		return newFlatFieldIndexStorage(flattenChunkedFieldIndexRoot(root))
	}
	return newChunkedFieldIndexStorage(root)
}

func sortedInsertPostingAddsBufOwned(
	adds map[string]uint32,
	arena *insertPostingAccumArena,
	fixed8 bool,
) *pooled.Slice[keyedBatchPostingDelta] {
	if len(adds) == 0 {
		insertPostingMapPool.Put(adds)
		return nil
	}
	buf := keyedBatchPostingDeltaSlicePool.Get()
	buf.Grow(len(adds))
	for raw, ref := range adds {
		ids := arena.accum(ref).materializeOwned()
		buf.Append(keyedBatchPostingDelta{
			key: keycodec.FromStoredString(raw, fixed8),
			delta: batchPostingDelta{
				add: ids,
			},
		})
	}
	insertPostingMapPool.Put(adds)
	sortKeyedBatchPostingDeltasBuf(buf)
	return buf
}

func sortedFixedInsertPostingAddsBufOwned(
	adds map[uint64]uint32,
	arena *insertPostingAccumArena,
) *pooled.Slice[keyedBatchPostingDelta] {
	if len(adds) == 0 {
		fixedInsertPostingMapPool.Put(adds)
		return nil
	}
	buf := keyedBatchPostingDeltaSlicePool.Get()
	buf.Grow(len(adds))
	for raw, ref := range adds {
		ids := arena.accum(ref).materializeOwned()
		buf.Append(keyedBatchPostingDelta{
			key: keycodec.FromU64(raw),
			delta: batchPostingDelta{
				add: ids,
			},
		})
	}
	fixedInsertPostingMapPool.Put(adds)
	sortKeyedBatchPostingDeltasBuf(buf)
	return buf
}

func mergeInsertOnlyFieldStorageOwned(
	base fieldIndexStorage,
	adds map[string]uint32,
	arena *insertPostingAccumArena,
	fixed8 bool,
	allowChunk bool,
) fieldIndexStorage {
	if len(adds) == 0 {
		insertPostingMapPool.Put(adds)
		return base
	}
	if base.flat == nil && base.chunked == nil {
		if allowChunk {
			return newRegularFieldIndexStorageFromInsertPostingAccumsOwned(adds, arena, fixed8)
		}
		return newFlatFieldIndexStorageFromInsertPostingAccumsOwned(adds, arena, fixed8)
	}
	if allowChunk && base.chunked != nil {
		if len(adds) == 1 {
			for raw, ref := range adds {
				add := keyedBatchPostingDelta{
					key: keycodec.FromStoredString(raw, fixed8),
					delta: batchPostingDelta{
						add: arena.accum(ref).materializeOwned(),
					},
				}
				insertPostingMapPool.Put(adds)
				return applySingleFieldPostingDiffChunked(base.chunked, add)
			}
		}
		buf := sortedInsertPostingAddsBufOwned(adds, arena, fixed8)
		if buf == nil {
			return base
		}
		defer keyedBatchPostingDeltaSlicePool.Put(buf)
		return applyFieldPostingDiffChunkedBuf(base.chunked, buf)
	}
	flat := base.flatSlice()
	if allowChunk && flat != nil && len(*flat)+len(adds) >= fieldIndexChunkThreshold {
		buf := sortedInsertPostingAddsBufOwned(adds, arena, fixed8)
		if buf == nil {
			return base
		}
		defer keyedBatchPostingDeltaSlicePool.Put(buf)
		borrowed := make([]index, len(*flat))
		copyBorrowedIndexEntries(borrowed, *flat)
		return applyFieldPostingDiffChunkedBuf(buildChunkedFieldIndexRoot(borrowed), buf)
	}
	slice := mergeInsertOnlyFieldSliceOwned(base.flatSlice(), adds, arena, fixed8)
	if !allowChunk {
		return newFlatFieldIndexStorage(slice)
	}
	return newRegularFieldIndexStorage(slice)
}

func mergeInsertOnlyFixedFieldStorageOwned(
	base fieldIndexStorage,
	adds map[uint64]uint32,
	arena *insertPostingAccumArena,
	allowChunk bool,
) fieldIndexStorage {
	if len(adds) == 0 {
		fixedInsertPostingMapPool.Put(adds)
		return base
	}
	if base.flat == nil && base.chunked == nil {
		if allowChunk {
			return newRegularFieldIndexStorageFromFixedInsertPostingAccumsOwned(adds, arena)
		}
		return newFlatFieldIndexStorageFromFixedInsertPostingAccumsOwned(adds, arena)
	}
	if allowChunk && base.chunked != nil {
		if len(adds) == 1 {
			for raw, ref := range adds {
				add := keyedBatchPostingDelta{
					key: keycodec.FromU64(raw),
					delta: batchPostingDelta{
						add: arena.accum(ref).materializeOwned(),
					},
				}
				fixedInsertPostingMapPool.Put(adds)
				return applySingleFieldPostingDiffChunked(base.chunked, add)
			}
		}
		buf := sortedFixedInsertPostingAddsBufOwned(adds, arena)
		if buf == nil {
			return base
		}
		defer keyedBatchPostingDeltaSlicePool.Put(buf)
		return applyFieldPostingDiffChunkedBuf(base.chunked, buf)
	}
	flat := base.flatSlice()
	if allowChunk && flat != nil && len(*flat)+len(adds) >= fieldIndexChunkThreshold {
		buf := sortedFixedInsertPostingAddsBufOwned(adds, arena)
		if buf == nil {
			return base
		}
		defer keyedBatchPostingDeltaSlicePool.Put(buf)
		borrowed := make([]index, len(*flat))
		copyBorrowedIndexEntries(borrowed, *flat)
		return applyFieldPostingDiffChunkedBuf(buildChunkedFieldIndexRoot(borrowed), buf)
	}
	slice := mergeInsertOnlyFixedFieldSliceOwned(base.flatSlice(), adds, arena)
	if !allowChunk {
		return newFlatFieldIndexStorage(slice)
	}
	return newRegularFieldIndexStorage(slice)
}

func (qe *queryEngine) collectSnapshotBatchEntryDiffs(
	op snapshotBatchEntry,
	deltas *indexedFieldBatchDeltas,
	lenZeroComplement []bool,
	patchMap map[string]*field,
) {
	if op.patchOnly {
		for i, patchField := range op.patch {
			fieldDef, ok := patchMap[patchField.Name]
			if !ok {
				continue
			}
			acc, ok := qe.indexedFieldMap[fieldDef.DBName]
			if !ok {
				continue
			}
			duplicate := false
			for j := 0; j < i; j++ {
				prev, ok := patchMap[op.patch[j].Name]
				if ok && prev.DBName == fieldDef.DBName {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
			deltas.markTouched(acc.ordinal)
			useZeroComplement := acc.ordinal < len(lenZeroComplement) && lenZeroComplement[acc.ordinal]
			acc.collectSnapshotBatchDiff(op.idx, op.oldVal, op.newVal, useZeroComplement, deltas.fields.GetPtr(acc.ordinal))
		}
		return
	}

	if op.oldVal == nil || op.newVal == nil {
		for _, acc := range qe.indexedFieldAccess {
			deltas.markTouched(acc.ordinal)
			useZeroComplement := acc.ordinal < len(lenZeroComplement) && lenZeroComplement[acc.ordinal]
			acc.collectSnapshotBatchDiff(op.idx, op.oldVal, op.newVal, useZeroComplement, deltas.fields.GetPtr(acc.ordinal))
		}
		return
	}

	for _, acc := range qe.indexedFieldAccess {
		if acc.modified == nil || !acc.modified(op.oldVal, op.newVal) {
			continue
		}
		deltas.markTouched(acc.ordinal)
		useZeroComplement := acc.ordinal < len(lenZeroComplement) && lenZeroComplement[acc.ordinal]
		acc.collectSnapshotBatchDiff(op.idx, op.oldVal, op.newVal, useZeroComplement, deltas.fields.GetPtr(acc.ordinal))
	}
}

func (qe *queryEngine) buildPreparedSnapshotAggregatedNoLock(
	seq uint64,
	prev *indexSnapshot,
	strMap *strMapper,
	patchMap map[string]*field,
	entries []snapshotBatchEntry,
) *indexSnapshot {

	var strmap *strMapSnapshot
	if strMap != nil {
		strmap = strMap.snapshot()
	}
	next := &indexSnapshot{
		seq: seq,

		index:              cloneFieldIndexStorageSlots(prev.index, len(qe.indexedFieldAccess)),
		nilIndex:           cloneFieldIndexStorageSlots(prev.nilIndex, len(qe.indexedFieldAccess)),
		lenIndex:           cloneFieldIndexStorageSlots(prev.lenIndex, len(qe.indexedFieldAccess)),
		lenZeroComplement:  cloneFieldIndexBoolSlots(prev.lenZeroComplement, len(qe.indexedFieldAccess)),
		measure:            cloneMeasureFieldStorageSlots(prev.measure, len(qe.measureFieldAccess)),
		indexedFieldByName: qe.indexedFieldMap,
		universe:           prev.universe,
		universeOwner:      prev.universeOwner,
		strmap:             strmap,
	}
	qe.initSnapshotRuntimeCaches(next)

	normalized := normalizePreparedBatchForSnapshot(entries)
	deltas := indexedFieldBatchDeltas{
		fields:  snapshotFieldBatchStateSlicePool.Get(),
		touched: pools.GetIntSlice(len(qe.indexedFieldAccess)),
		changed: pools.GetBoolSlice(len(qe.indexedFieldAccess))[:len(qe.indexedFieldAccess)],
	}
	deltas.fields.SetLen(len(qe.indexedFieldAccess))
	clear(deltas.changed)
	measureDeltas := newMeasureFieldBatchDeltas(len(qe.measureFieldAccess))

	universeOwned := false

	for i := range normalized {
		op := normalized[i]
		switch {
		case op.oldVal == nil && op.newVal != nil:
			ensureSnapshotUniverseOwned(next, &universeOwned)
			next.universe = next.universe.BuildAdded(op.idx)
		case op.oldVal != nil && op.newVal == nil:
			ensureSnapshotUniverseOwned(next, &universeOwned)
			next.universe = next.universe.BuildRemoved(op.idx)
		}
		qe.collectSnapshotBatchEntryDiffs(op, &deltas, prev.lenZeroComplement, patchMap)
		qe.collectSnapshotMeasureEntryDiffs(op, &measureDeltas, patchMap)
	}

	for i := range deltas.touched {
		deltas.changed[deltas.touched[i]] = false
	}

	changedCount := 0

	for i := range deltas.touched {
		ordinal := deltas.touched[i]
		acc := qe.indexedFieldAccess[ordinal]
		state := deltas.fields.GetPtr(ordinal)
		baseIndex := next.index.Get(ordinal)
		if storage := acc.applySnapshotBatchStorageOwned(baseIndex, state, true); storage.keyCount() == 0 {
			if baseIndex.keyCount() > 0 {
				next.index.Set(ordinal, fieldIndexStorage{})
			}
		} else if storage != baseIndex {
			next.index.Set(ordinal, storage)
		}
		baseNil := next.nilIndex.Get(ordinal)
		if storage := acc.applySnapshotBatchNilStorageOwned(baseNil, state); storage.keyCount() == 0 {
			if baseNil.keyCount() > 0 {
				next.nilIndex.Set(ordinal, fieldIndexStorage{})
			}
		} else if storage != baseNil {
			next.nilIndex.Set(ordinal, storage)
		}
		if state.lengths != nil {
			baseLen := next.lenIndex.Get(ordinal)
			if storage := applyLenFieldPostingDiffStorageOwned(baseLen, state.lengths); storage != baseLen {
				next.lenIndex.Set(ordinal, storage)
			}
			state.lengths = nil
		}
		if state.changed {
			changedCount++
			deltas.changed[ordinal] = true
		}
		state.releaseOwned()
	}
	applyMeasureFieldBatchDeltas(next, &measureDeltas)
	measureDeltas.release()
	inheritNumericRangeBucketCache(next, prev)
	if changedCount > 0 {
		inheritMaterializedPredCache(next, prev, qe.indexedFieldMap, deltas.changed)
	} else {
		inheritMaterializedPredCache(next, prev, qe.indexedFieldMap, nil)
	}
	snapshotFieldBatchStateSlicePool.Put(deltas.fields)
	pools.PutBoolSlice(deltas.changed)
	pools.PutIntSlice(deltas.touched)
	next.retainSharedOwnedStorageFrom(prev)

	return next
}
