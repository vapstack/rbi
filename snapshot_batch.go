package rbi

import (
	"maps"
	"slices"
	"sort"
	"sync"
	"unsafe"

	"github.com/vapstack/rbi/internal/posting"
)

type snapshotBatchEntry[K ~string | ~uint64, V any] struct {
	req    *autoBatchRequest[K, V]
	idx    uint64
	oldVal *V
	newVal *V
}

type batchPostingDelta struct {
	add    posting.List
	remove posting.List
}

type keyedBatchPostingDelta struct {
	key   indexKey
	delta batchPostingDelta
}

type keyedBatchPostingDeltaOrder []keyedBatchPostingDelta

func (s keyedBatchPostingDeltaOrder) Len() int      { return len(s) }
func (s keyedBatchPostingDeltaOrder) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s keyedBatchPostingDeltaOrder) Less(i, j int) bool {
	return compareIndexKeys(s[i].key, s[j].key) < 0
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

var lenFieldPostingDeltaPool sync.Pool

func getLenFieldPostingDelta(capHint int) *lenFieldPostingDelta {
	if v := lenFieldPostingDeltaPool.Get(); v != nil {
		return v.(*lenFieldPostingDelta)
	}
	return &lenFieldPostingDelta{
		lengths: getFixedBatchPostingDeltaMapHint(capHint),
	}
}

func putLenFieldPostingDelta(delta *lenFieldPostingDelta) {
	if delta == nil {
		return
	}
	if delta.lengths != nil {
		releaseFixedBatchPostingDeltaMap(delta.lengths)
		delta.lengths = nil
	}
	delta.nonEmpty = batchPostingDelta{}
	delta.hasNonEmpty = false
	lenFieldPostingDeltaPool.Put(delta)
}

func releaseLenFieldPostingDeltaOwned(delta *lenFieldPostingDelta) {
	if delta == nil {
		return
	}
	if delta.lengths != nil {
		releaseFixedBatchPostingDeltaMapOwned(delta.lengths)
		delta.lengths = nil
	}
	delta.nonEmpty.add.Release()
	delta.nonEmpty.remove.Release()
	delta.nonEmpty = batchPostingDelta{}
	delta.hasNonEmpty = false
	lenFieldPostingDeltaPool.Put(delta)
}

type indexedFieldBatchDeltas struct {
	fields []snapshotFieldBatchState
}

func normalizePreparedBatchForSnapshot[K ~string | ~uint64, V any](prepared []autoBatchPrepared[K, V]) []snapshotBatchEntry[K, V] {
	if len(prepared) == 0 {
		return nil
	}
	if len(prepared) == 1 {
		op := prepared[0]
		if op.oldVal == nil && op.newVal == nil {
			return nil
		}
		return []snapshotBatchEntry[K, V]{
			{
				req:    op.req,
				idx:    op.idx,
				oldVal: op.oldVal,
				newVal: op.newVal,
			},
		}
	}

	out := make([]snapshotBatchEntry[K, V], 0, len(prepared))
	pos := make(map[uint64]int, len(prepared))

	for i := range prepared {
		op := prepared[i]
		if p, ok := pos[op.idx]; ok {
			// Repeated-id entries collapse to first oldVal -> last newVal.
			// Patch metadata from any individual request is no longer sufficient
			// to describe the aggregated diff across the whole chain, so disable
			// the patch-only fast path for this normalized entry.
			out[p].req = nil
			out[p].newVal = op.newVal
			continue
		}
		pos[op.idx] = len(out)
		out = append(out, snapshotBatchEntry[K, V]{
			req:    op.req,
			idx:    op.idx,
			oldVal: op.oldVal,
			newVal: op.newVal,
		})
	}

	n := 0
	for i := range out {
		if out[i].oldVal == nil && out[i].newVal == nil {
			continue
		}
		out[n] = out[i]
		n++
	}
	return out[:n]
}

func addFixedFieldBatchPostingDelta(fieldDelta map[uint64]batchPostingDelta, key uint64, idx uint64, isAdd bool) map[uint64]batchPostingDelta {
	if fieldDelta == nil {
		fieldDelta = getFixedBatchPostingDeltaMap()
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

func collectFieldBatchPostingDiff(
	fieldDelta map[string]uint32,
	arena **batchPostingAccumArena,
	idx uint64,
	oldVals, newVals []string,
) (map[string]uint32, bool) {
	if len(oldVals) == 0 && len(newVals) == 0 {
		return fieldDelta, false
	}

	var changed bool
	i, j := 0, 0
	for i < len(oldVals) && j < len(newVals) {
		switch {
		case oldVals[i] < newVals[j]:
			fieldDelta = addFieldBatchPostingAccum(fieldDelta, arena, oldVals[i], idx, false, 0)
			changed = true
			i++
		case oldVals[i] > newVals[j]:
			fieldDelta = addFieldBatchPostingAccum(fieldDelta, arena, newVals[j], idx, true, 0)
			changed = true
			j++
		default:
			i++
			j++
		}
	}
	for ; i < len(oldVals); i++ {
		fieldDelta = addFieldBatchPostingAccum(fieldDelta, arena, oldVals[i], idx, false, 0)
		changed = true
	}
	for ; j < len(newVals); j++ {
		fieldDelta = addFieldBatchPostingAccum(fieldDelta, arena, newVals[j], idx, true, 0)
		changed = true
	}
	return fieldDelta, changed
}

func collectFixedFieldBatchPostingDiff(
	fieldDelta map[uint64]uint32,
	arena **batchPostingAccumArena,
	idx uint64,
	oldVals, newVals []uint64,
) (map[uint64]uint32, bool) {
	if len(oldVals) == 0 && len(newVals) == 0 {
		return fieldDelta, false
	}

	var changed bool
	i, j := 0, 0
	for i < len(oldVals) && j < len(newVals) {
		switch {
		case oldVals[i] < newVals[j]:
			fieldDelta = addFixedFieldBatchPostingAccum(fieldDelta, arena, oldVals[i], idx, false, 0)
			changed = true
			i++
		case oldVals[i] > newVals[j]:
			fieldDelta = addFixedFieldBatchPostingAccum(fieldDelta, arena, newVals[j], idx, true, 0)
			changed = true
			j++
		default:
			i++
			j++
		}
	}
	for ; i < len(oldVals); i++ {
		fieldDelta = addFixedFieldBatchPostingAccum(fieldDelta, arena, oldVals[i], idx, false, 0)
		changed = true
	}
	for ; j < len(newVals); j++ {
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
	addLenBucketDelta := func(length int, isAdd bool) {
		if fieldDelta == nil {
			fieldDelta = getLenFieldPostingDelta(0)
		}
		fieldDelta.lengths = addFixedFieldBatchPostingDelta(fieldDelta.lengths, uint64(length), idx, isAdd)
		changed = true
	}
	addNonEmptyDelta := func(isAdd bool) {
		if fieldDelta == nil {
			fieldDelta = getLenFieldPostingDelta(0)
		}
		fieldDelta.hasNonEmpty = true
		if isAdd {
			fieldDelta.nonEmpty.add = fieldDelta.nonEmpty.add.BuildAdded(idx)
		} else {
			fieldDelta.nonEmpty.remove = fieldDelta.nonEmpty.remove.BuildAdded(idx)
		}
		changed = true
	}

	if !useZeroComplement {
		if diff.oldExists {
			addLenBucketDelta(diff.oldLen, false)
		}
		if diff.newExists {
			addLenBucketDelta(diff.newLen, true)
		}
		return fieldDelta, changed
	}

	if diff.oldExists {
		if diff.oldLen > 0 {
			addLenBucketDelta(diff.oldLen, false)
		}
		if diff.oldLen > 0 && (!diff.newExists || diff.newLen == 0) {
			addNonEmptyDelta(false)
		}
	}
	if diff.newExists {
		if diff.newLen > 0 {
			addLenBucketDelta(diff.newLen, true)
		}
		if diff.newLen > 0 && (!diff.oldExists || diff.oldLen == 0) {
			addNonEmptyDelta(true)
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
) *keyedBatchPostingDeltaSliceBuf {
	if len(deltas) == 0 {
		releaseBatchPostingAccumMap(deltas)
		return nil
	}

	buf := getKeyedBatchPostingDeltaSliceBuf(len(deltas))

	for raw, ref := range deltas {
		delta := arena.accum(ref).materializeOwned()
		if delta.add.IsEmpty() && delta.remove.IsEmpty() {
			continue
		}
		buf.values = append(buf.values, keyedBatchPostingDelta{
			key:   indexKeyFromStoredString(raw, fixed8),
			delta: delta,
		})
	}
	releaseBatchPostingAccumMap(deltas)
	if len(buf.values) == 0 {
		releaseKeyedBatchPostingDeltaSliceBuf(buf)
		return nil
	}

	sort.Sort(keyedBatchPostingDeltaOrder(buf.values))
	return buf
}

func sortedFixedBatchPostingDeltasBufOwned(
	deltas map[uint64]uint32,
	arena *batchPostingAccumArena,
) *keyedBatchPostingDeltaSliceBuf {
	if len(deltas) == 0 {
		releaseFixedBatchPostingAccumMap(deltas)
		return nil
	}

	buf := getKeyedBatchPostingDeltaSliceBuf(len(deltas))

	for raw, ref := range deltas {
		delta := arena.accum(ref).materializeOwned()
		if delta.add.IsEmpty() && delta.remove.IsEmpty() {
			continue
		}
		buf.values = append(buf.values, keyedBatchPostingDelta{
			key:   indexKeyFromU64(raw),
			delta: delta,
		})
	}
	releaseFixedBatchPostingAccumMap(deltas)
	if len(buf.values) == 0 {
		releaseKeyedBatchPostingDeltaSliceBuf(buf)
		return nil
	}

	sort.Sort(keyedBatchPostingDeltaOrder(buf.values))
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
			out = append(out, src[i:]...)
			i = len(src)
		case i >= len(src):
			ids := applyBatchPostingDeltaOwned(posting.List{}, &deltaKeys[j].delta)
			if !ids.IsEmpty() {
				out = append(out, index{Key: deltaKeys[j].key, IDs: ids})
			}
			j++
		default:
			cmp := compareIndexKeys(src[i].Key, deltaKeys[j].key)
			switch {
			case cmp < 0:
				out = append(out, src[i])
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
	if pos >= len(src) || compareIndexKeys(src[pos].Key, delta.key) > 0 {
		ids := applyBatchPostingDeltaOwned(posting.List{}, &delta.delta)
		if ids.IsEmpty() {
			return base
		}
		out := make([]index, len(src)+1)
		copy(out, src[:pos])
		out[pos] = index{Key: delta.key, IDs: ids}
		copy(out[pos+1:], src[pos:])
		return &out
	}

	ids := applyBatchPostingDeltaOwned(src[pos].IDs, &delta.delta)
	if ids.IsEmpty() {
		if len(src) == 1 {
			return nil
		}
		out := make([]index, len(src)-1)
		copy(out, src[:pos])
		copy(out[pos:], src[pos+1:])
		return &out
	}

	out := make([]index, len(src))
	copy(out, src)
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
		numeric = src[0].Key.isNumeric()
	} else if len(deltaKeys) > 0 {
		numeric = deltaKeys[0].key.isNumeric()
	}
	out := newFieldIndexChunkStreamBuilder(builder, numeric)

	i, j := 0, 0
	for i < len(src) || j < len(deltaKeys) {
		switch {
		case j >= len(deltaKeys):
			out.append(src[i].Key, src[i].IDs)
			i++
		case i >= len(src):
			ids := applyBatchPostingDeltaOwned(posting.List{}, &deltaKeys[j].delta)
			if !ids.IsEmpty() {
				out.append(deltaKeys[j].key, ids)
			}
			j++
		default:
			cmp := compareIndexKeys(src[i].Key, deltaKeys[j].key)
			switch {
			case cmp < 0:
				out.append(src[i].Key, src[i].IDs)
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
	if !shouldUseChunkedFieldIndex(root.keyCount) {
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
		if ref, ok := base.refAtChunk(startChunk); ok && ref.chunk != nil && ref.chunk.numeric != nil {
			numeric = true
		}
	} else if len(deltaKeys) > 0 {
		numeric = deltaKeys[0].key.isNumeric()
	}
	out := newFieldIndexChunkStreamBuilder(builder, numeric)

	chunkIdx := startChunk
	entryIdx := 0
	j := 0

	nextBase := func() (index, bool) {
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
	advanceBase := func() {
		entryIdx++
		for chunkIdx < endChunk {
			ref, _ := base.refAtChunk(chunkIdx)
			if entryIdx < ref.chunk.keyCount() {
				return
			}
			chunkIdx++
			entryIdx = 0
		}
	}

	for {
		baseEnt, hasBase := nextBase()
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
			advanceBase()
		default:
			cmp := compareIndexKeys(baseEnt.Key, deltaKeys[j].key)
			switch {
			case cmp < 0:
				out.append(baseEnt.Key, baseEnt.IDs)
				advanceBase()
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
				advanceBase()
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
	defer releaseKeyedBatchPostingDeltaSliceBuf(buf)
	deltaKeys := buf.values
	if !allowChunk {
		return newFlatFieldIndexStorage(applyFieldPostingDiffSorted(base.flatSlice(), deltaKeys))
	}
	if base.chunked != nil {
		return applyFieldPostingDiffChunked(base.chunked, deltaKeys)
	}
	flat := base.flatSlice()
	baseCount := 0
	if flat != nil {
		baseCount = len(*flat)
	}
	if shouldUseChunkedFieldIndex(len(deltaKeys) + baseCount) {
		return applyFieldPostingDiffFlatMaybeChunked(flat, deltaKeys)
	}
	return newRegularFieldIndexStorage(applyFieldPostingDiffSorted(flat, deltaKeys))
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
	defer releaseKeyedBatchPostingDeltaSliceBuf(buf)
	deltaKeys := buf.values
	if !allowChunk {
		return newFlatFieldIndexStorage(applyFieldPostingDiffSorted(base.flatSlice(), deltaKeys))
	}
	if base.chunked != nil {
		return applyFieldPostingDiffChunked(base.chunked, deltaKeys)
	}
	flat := base.flatSlice()
	baseCount := 0
	if flat != nil {
		baseCount = len(*flat)
	}
	if shouldUseChunkedFieldIndex(len(deltaKeys) + baseCount) {
		return applyFieldPostingDiffFlatMaybeChunked(flat, deltaKeys)
	}
	return newRegularFieldIndexStorage(applyFieldPostingDiffSorted(flat, deltaKeys))
}

func sortedLenFieldPostingDeltasBufOwned(deltas *lenFieldPostingDelta) *keyedBatchPostingDeltaSliceBuf {
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

	buf := getKeyedBatchPostingDeltaSliceBuf(count)
	for raw, delta := range deltas.lengths {
		if delta.add.IsEmpty() && delta.remove.IsEmpty() {
			continue
		}
		buf.values = append(buf.values, keyedBatchPostingDelta{
			key:   indexKeyFromU64(raw),
			delta: delta,
		})
	}
	if deltas.hasNonEmpty && (!deltas.nonEmpty.add.IsEmpty() || !deltas.nonEmpty.remove.IsEmpty()) {
		buf.values = append(buf.values, keyedBatchPostingDelta{
			key:   indexKeyFromString(lenIndexNonEmptyKey),
			delta: deltas.nonEmpty,
		})
	}
	putLenFieldPostingDelta(deltas)
	if len(buf.values) == 0 {
		releaseKeyedBatchPostingDeltaSliceBuf(buf)
		return nil
	}
	sort.Sort(keyedBatchPostingDeltaOrder(buf.values))
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
			key:   indexKeyFromU64(raw),
			delta: delta,
		}
		n++
		if n == len(dst) {
			break
		}
	}
	if n < len(dst) && deltas.hasNonEmpty && (!deltas.nonEmpty.add.IsEmpty() || !deltas.nonEmpty.remove.IsEmpty()) {
		dst[n] = keyedBatchPostingDelta{
			key:   indexKeyFromString(lenIndexNonEmptyKey),
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
	var buf *keyedBatchPostingDeltaSliceBuf
	if count <= len(inline) {
		n := takeLenFieldPostingDeltasOwned(deltas, inline[:count])
		deltaKeys = inline[:n]
		if len(deltaKeys) > 1 && compareIndexKeys(deltaKeys[0].key, deltaKeys[1].key) > 0 {
			deltaKeys[0], deltaKeys[1] = deltaKeys[1], deltaKeys[0]
		}
	} else {
		buf = sortedLenFieldPostingDeltasBufOwned(deltas)
		if buf == nil {
			return base
		}
		defer releaseKeyedBatchPostingDeltaSliceBuf(buf)
		deltaKeys = buf.values
	}

	storage := newFlatFieldIndexStorage(applyFieldPostingDiffSorted(base.flatSlice(), deltaKeys))
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
			key:   indexKeyFromU64(raw),
			delta: delta,
		}, true
	}
	if deltas.hasNonEmpty && (!deltas.nonEmpty.add.IsEmpty() || !deltas.nonEmpty.remove.IsEmpty()) {
		delta := deltas.nonEmpty
		putLenFieldPostingDelta(deltas)
		return keyedBatchPostingDelta{
			key:   indexKeyFromString(lenIndexNonEmptyKey),
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
	if !shouldUseChunkedFieldIndex(root.keyCount) {
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

func applyRowDeltaSuffix(rows []uint64, start int, delta int64) {
	if delta == 0 {
		return
	}
	if delta > 0 {
		add := uint64(delta)
		for i := start; i < len(rows); i++ {
			rows[i] += add
		}
		return
	}
	sub := uint64(-delta)
	for i := start; i < len(rows); i++ {
		rows[i] -= sub
	}
}

func applyIntDeltaSuffix(values []int, start, delta int) {
	if delta == 0 {
		return
	}
	for i := start; i < len(values); i++ {
		values[i] += delta
	}
}

func rebuildChunkedRootWithPageRefsReplaced(
	base *fieldIndexChunkedRoot,
	page int,
	replRefs []fieldIndexChunkRef,
) *fieldIndexChunkedRoot {
	if base == nil || page < 0 || page >= len(base.pages) {
		return nil
	}
	est := base.keyCount - base.pages[page].keyCount()
	for i := range replRefs {
		if replRefs[i].chunk != nil {
			est += replRefs[i].chunk.keyCount()
		}
	}
	if est < 0 {
		est = 0
	}
	builder := newFieldIndexChunkBuilder(est)
	for i := 0; i < page; i++ {
		builder.appendPage(base.pages[i])
	}
	builder.appendRefSlice(replRefs)
	for i := page + 1; i < len(base.pages); i++ {
		builder.appendPage(base.pages[i])
	}
	return builder.root()
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
	if page >= len(base.pages) {
		return newChunkedFieldIndexStorage(base)
	}

	ref, ok := base.refAtChunk(touchIdx)
	if !ok || ref.chunk == nil {
		return newChunkedFieldIndexStorage(base)
	}
	entryIdx := lowerBoundFieldIndexChunkKey(ref.chunk, delta.key)
	if delta.delta.remove.IsEmpty() &&
		!delta.delta.add.IsEmpty() &&
		entryIdx < ref.chunk.keyCount() &&
		compareIndexKeys(ref.chunk.keyAt(entryIdx), delta.key) == 0 {
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
		posts := make([]posting.List, len(ref.chunk.posts))
		copyBorrowedPostingSlice(posts, ref.chunk.posts)
		posts[entryIdx] = updatedIDs
		chunk := &fieldIndexChunk{
			posts: posts,
			rows:  rows,
		}
		if ref.chunk.numeric != nil {
			chunk.numeric = ref.chunk.numeric
		} else {
			chunk.stringRefs = ref.chunk.stringRefs
			chunk.stringData = ref.chunk.stringData
		}

		refs := slices.Clone(base.pages[page].refs)
		refs[off] = fieldIndexChunkRef{
			last:  ref.last,
			chunk: chunk,
		}
		pageRowPrefix := base.pages[page].rowPrefix
		if rowsDelta != 0 {
			pageRowPrefix = slices.Clone(pageRowPrefix)
			applyRowDeltaSuffix(pageRowPrefix, off+1, rowsDelta)
		}
		pages := slices.Clone(base.pages)
		pages[page] = fieldIndexChunkDirPage{
			refs:      refs,
			prefix:    base.pages[page].prefix,
			rowPrefix: pageRowPrefix,
		}
		rootRowPrefix := base.rowPrefix
		if rowsDelta != 0 {
			rootRowPrefix = slices.Clone(rootRowPrefix)
			applyRowDeltaSuffix(rootRowPrefix, page+1, rowsDelta)
		}
		return newChunkedFieldIndexStorage(&fieldIndexChunkedRoot{
			pages:       pages,
			chunkPrefix: base.chunkPrefix,
			prefix:      base.prefix,
			rowPrefix:   rootRowPrefix,
			keyCount:    base.keyCount,
			chunkCount:  base.chunkCount,
		})
	}
	if delta.delta.remove.IsEmpty() && !delta.delta.add.IsEmpty() {
		replRefs := newFieldIndexChunkRefsWithInsertedEntry(ref, entryIdx, index{
			Key: delta.key,
			IDs: delta.delta.add,
		})
		if len(replRefs) > 0 {
			if len(replRefs) == 1 {
				repl := replRefs[0]
				keyDelta := repl.chunk.keyCount() - ref.chunk.keyCount()
				rowsDelta := int64(repl.chunk.rowCount()) - int64(ref.chunk.rowCount())

				refs := slices.Clone(base.pages[page].refs)
				refs[off] = repl

				pagePrefix := base.pages[page].prefix
				if keyDelta != 0 {
					pagePrefix = slices.Clone(pagePrefix)
					applyIntDeltaSuffix(pagePrefix, off+1, keyDelta)
				}

				pageRowPrefix := base.pages[page].rowPrefix
				if rowsDelta != 0 {
					pageRowPrefix = slices.Clone(pageRowPrefix)
					applyRowDeltaSuffix(pageRowPrefix, off+1, rowsDelta)
				}

				pages := slices.Clone(base.pages)
				pages[page] = fieldIndexChunkDirPage{
					refs:      refs,
					prefix:    pagePrefix,
					rowPrefix: pageRowPrefix,
				}

				rootPrefix := base.prefix
				if keyDelta != 0 {
					rootPrefix = slices.Clone(rootPrefix)
					applyIntDeltaSuffix(rootPrefix, page+1, keyDelta)
				}

				rootRowPrefix := base.rowPrefix
				if rowsDelta != 0 {
					rootRowPrefix = slices.Clone(rootRowPrefix)
					applyRowDeltaSuffix(rootRowPrefix, page+1, rowsDelta)
				}

				return newChunkedFieldIndexStorage(&fieldIndexChunkedRoot{
					pages:       pages,
					chunkPrefix: base.chunkPrefix,
					prefix:      rootPrefix,
					rowPrefix:   rootRowPrefix,
					keyCount:    base.keyCount + keyDelta,
					chunkCount:  base.chunkCount,
				})
			}

			oldPage := base.pages[page]
			newPageRefs := make([]fieldIndexChunkRef, 0, len(oldPage.refs)-1+len(replRefs))
			newPageRefs = append(newPageRefs, oldPage.refs[:off]...)
			newPageRefs = append(newPageRefs, replRefs...)
			newPageRefs = append(newPageRefs, oldPage.refs[off+1:]...)

			pagesCap := len(base.pages)
			if len(newPageRefs) == 0 {
				pagesCap--
			}
			if len(newPageRefs) > fieldIndexDirPageTargetRefs {
				root := rebuildChunkedRootWithPageRefsReplaced(base, page, newPageRefs)
				if root == nil {
					return fieldIndexStorage{}
				}
				if !shouldUseChunkedFieldIndex(root.keyCount) {
					return newFlatFieldIndexStorage(flattenChunkedFieldIndexRoot(root))
				}
				return newChunkedFieldIndexStorage(root)
			}
			pages := make([]fieldIndexChunkDirPage, 0, max(pagesCap, 0))
			pages = append(pages, base.pages[:page]...)
			if len(newPageRefs) > 0 {
				pages = append(pages, newFieldIndexChunkDirPage(newPageRefs))
			}
			pages = append(pages, base.pages[page+1:]...)

			root := newFieldIndexChunkedRootFromPages(pages)
			if root == nil {
				return fieldIndexStorage{}
			}
			if !shouldUseChunkedFieldIndex(root.keyCount) {
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

	oldPage := base.pages[page]
	newPageRefs := make([]fieldIndexChunkRef, 0, len(oldPage.refs)-1+len(replRefs))
	newPageRefs = append(newPageRefs, oldPage.refs[:off]...)
	newPageRefs = append(newPageRefs, replRefs...)
	newPageRefs = append(newPageRefs, oldPage.refs[off+1:]...)

	pagesCap := len(base.pages)
	if len(newPageRefs) == 0 {
		pagesCap--
	}
	if len(newPageRefs) > fieldIndexDirPageTargetRefs {
		root := rebuildChunkedRootWithPageRefsReplaced(base, page, newPageRefs)
		if root == nil {
			return fieldIndexStorage{}
		}
		if !shouldUseChunkedFieldIndex(root.keyCount) {
			return newFlatFieldIndexStorage(flattenChunkedFieldIndexRoot(root))
		}
		return newChunkedFieldIndexStorage(root)
	}
	pages := make([]fieldIndexChunkDirPage, 0, max(pagesCap, 0))
	pages = append(pages, base.pages[:page]...)
	if len(newPageRefs) > 0 {
		pages = append(pages, newFieldIndexChunkDirPage(newPageRefs))
	}
	pages = append(pages, base.pages[page+1:]...)

	root := newFieldIndexChunkedRootFromPages(pages)
	if root == nil {
		return fieldIndexStorage{}
	}
	if !shouldUseChunkedFieldIndex(root.keyCount) {
		return newFlatFieldIndexStorage(flattenChunkedFieldIndexRoot(root))
	}
	return newChunkedFieldIndexStorage(root)
}

func sortedInsertPostingAddsBufOwned(
	adds map[string]uint32,
	arena *insertPostingAccumArena,
	fixed8 bool,
) *keyedBatchPostingDeltaSliceBuf {
	if len(adds) == 0 {
		releaseInsertPostingMap(adds)
		return nil
	}
	buf := getKeyedBatchPostingDeltaSliceBuf(len(adds))
	for raw, ref := range adds {
		ids := arena.accum(ref).materializeOwned()
		buf.values = append(buf.values, keyedBatchPostingDelta{
			key: indexKeyFromStoredString(raw, fixed8),
			delta: batchPostingDelta{
				add: ids,
			},
		})
	}
	releaseInsertPostingMap(adds)
	sort.Sort(keyedBatchPostingDeltaOrder(buf.values))
	return buf
}

func sortedFixedInsertPostingAddsBufOwned(
	adds map[uint64]uint32,
	arena *insertPostingAccumArena,
) *keyedBatchPostingDeltaSliceBuf {
	if len(adds) == 0 {
		releaseFixedInsertPostingMap(adds)
		return nil
	}
	buf := getKeyedBatchPostingDeltaSliceBuf(len(adds))
	for raw, ref := range adds {
		ids := arena.accum(ref).materializeOwned()
		buf.values = append(buf.values, keyedBatchPostingDelta{
			key: indexKeyFromU64(raw),
			delta: batchPostingDelta{
				add: ids,
			},
		})
	}
	releaseFixedInsertPostingMap(adds)
	sort.Sort(keyedBatchPostingDeltaOrder(buf.values))
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
		releaseInsertPostingMap(adds)
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
					key: indexKeyFromStoredString(raw, fixed8),
					delta: batchPostingDelta{
						add: arena.accum(ref).materializeOwned(),
					},
				}
				releaseInsertPostingMap(adds)
				return applySingleFieldPostingDiffChunked(base.chunked, add)
			}
		}
		buf := sortedInsertPostingAddsBufOwned(adds, arena, fixed8)
		if buf == nil {
			return base
		}
		defer releaseKeyedBatchPostingDeltaSliceBuf(buf)
		return applyFieldPostingDiffChunked(base.chunked, buf.values)
	}
	if allowChunk && base.flat != nil && shouldUseChunkedFieldIndex(len(*base.flat)+len(adds)) {
		buf := sortedInsertPostingAddsBufOwned(adds, arena, fixed8)
		if buf == nil {
			return base
		}
		defer releaseKeyedBatchPostingDeltaSliceBuf(buf)
		return applyFieldPostingDiffChunked(buildChunkedFieldIndexRoot(*base.flat), buf.values)
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
		releaseFixedInsertPostingMap(adds)
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
					key: indexKeyFromU64(raw),
					delta: batchPostingDelta{
						add: arena.accum(ref).materializeOwned(),
					},
				}
				releaseFixedInsertPostingMap(adds)
				return applySingleFieldPostingDiffChunked(base.chunked, add)
			}
		}
		buf := sortedFixedInsertPostingAddsBufOwned(adds, arena)
		if buf == nil {
			return base
		}
		defer releaseKeyedBatchPostingDeltaSliceBuf(buf)
		return applyFieldPostingDiffChunked(base.chunked, buf.values)
	}
	if allowChunk && base.flat != nil && shouldUseChunkedFieldIndex(len(*base.flat)+len(adds)) {
		buf := sortedFixedInsertPostingAddsBufOwned(adds, arena)
		if buf == nil {
			return base
		}
		defer releaseKeyedBatchPostingDeltaSliceBuf(buf)
		return applyFieldPostingDiffChunked(buildChunkedFieldIndexRoot(*base.flat), buf.values)
	}
	slice := mergeInsertOnlyFixedFieldSliceOwned(base.flatSlice(), adds, arena)
	if !allowChunk {
		return newFlatFieldIndexStorage(slice)
	}
	return newRegularFieldIndexStorage(slice)
}

func (db *DB[K, V]) forEachSnapshotModifiedIndexedField(op snapshotBatchEntry[K, V], fn func(indexedFieldAccessor) bool) {
	if fn == nil {
		return
	}
	req := op.req
	if req != nil &&
		req.op == autoBatchPatch &&
		op.oldVal != nil &&
		op.newVal != nil &&
		len(req.beforeProcess) == 0 &&
		len(req.beforeStore) == 0 {
		for i, patchField := range req.patch {
			f, ok := db.patchMap[patchField.Name]
			if !ok {
				continue
			}
			acc, ok := db.indexedFieldByName[f.DBName]
			if !ok {
				continue
			}
			duplicate := false
			for j := 0; j < i; j++ {
				prev, ok := db.patchMap[req.patch[j].Name]
				if ok && prev.DBName == f.DBName {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
			if !fn(acc) {
				return
			}
		}
		return
	}
	db.forEachModifiedIndexedField(op.oldVal, op.newVal, fn)
}

func (db *DB[K, V]) collectSnapshotBatchEntryDiffs(
	op snapshotBatchEntry[K, V],
	deltas *indexedFieldBatchDeltas,
	lenZeroComplement map[string]bool,
) {
	var ptrOld, ptrNew unsafe.Pointer
	if op.oldVal != nil {
		ptrOld = unsafe.Pointer(op.oldVal)
	}
	if op.newVal != nil {
		ptrNew = unsafe.Pointer(op.newVal)
	}

	db.forEachSnapshotModifiedIndexedField(op, func(acc indexedFieldAccessor) bool {
		acc.collectSnapshotBatchDiff(op.idx, ptrOld, ptrNew, lenZeroComplement[acc.name], &deltas.fields[acc.ordinal])
		return true
	})
}

func (db *DB[K, V]) buildPreparedSnapshotAggregatedNoLock(
	seq uint64,
	prev *indexSnapshot,
	prepared []autoBatchPrepared[K, V],
) *indexSnapshot {
	next := &indexSnapshot{
		seq: seq,

		index:             prev.index,
		nilIndex:          prev.nilIndex,
		lenIndex:          prev.lenIndex,
		lenZeroComplement: prev.lenZeroComplement,
		universe:          prev.universe,
		strmap:            db.strmap.snapshot(),
	}
	db.initSnapshotRuntimeCaches(next)
	indexCloned := false
	nilIndexCloned := false
	lenIndexCloned := false
	ensureIndex := func() map[string]fieldIndexStorage {
		if !indexCloned {
			next.index = maps.Clone(prev.index)
			indexCloned = true
		}
		return next.index
	}
	ensureNilIndex := func() map[string]fieldIndexStorage {
		if !nilIndexCloned {
			next.nilIndex = maps.Clone(prev.nilIndex)
			nilIndexCloned = true
		}
		return next.nilIndex
	}
	ensureLenIndex := func() map[string]fieldIndexStorage {
		if !lenIndexCloned {
			next.lenIndex = maps.Clone(prev.lenIndex)
			lenIndexCloned = true
		}
		return next.lenIndex
	}

	normalized := normalizePreparedBatchForSnapshot(prepared)
	deltas := indexedFieldBatchDeltas{
		fields: make([]snapshotFieldBatchState, len(db.indexedFieldAccess)),
	}

	universeOwned := false
	ensureUniverseOwned := func() {
		if universeOwned {
			return
		}
		next.universe = next.universe.Clone()
		universeOwned = true
	}

	for i := range normalized {
		op := normalized[i]
		switch {
		case op.oldVal == nil && op.newVal != nil:
			ensureUniverseOwned()
			next.universe = next.universe.BuildAdded(op.idx)
		case op.oldVal != nil && op.newVal == nil:
			ensureUniverseOwned()
			next.universe = next.universe.BuildRemoved(op.idx)
		}
		db.collectSnapshotBatchEntryDiffs(op, &deltas, prev.lenZeroComplement)
	}

	changedCount := 0
	for i, acc := range db.indexedFieldAccess {
		f := acc.name
		state := &deltas.fields[i]
		baseIndex := next.index[f]
		if storage := acc.applySnapshotBatchStorageOwned(baseIndex, state, true); storage.keyCount() == 0 {
			if baseIndex.keyCount() > 0 {
				delete(ensureIndex(), f)
			}
		} else if storage != baseIndex {
			ensureIndex()[f] = storage
		}
		baseNil := next.nilIndex[f]
		if storage := acc.applySnapshotBatchNilStorageOwned(baseNil, state); storage.keyCount() == 0 {
			if baseNil.keyCount() > 0 {
				delete(ensureNilIndex(), f)
			}
		} else if storage != baseNil {
			ensureNilIndex()[f] = storage
		}
		if state.lengths != nil {
			baseLen := next.lenIndex[f]
			if storage := applyLenFieldPostingDiffStorageOwned(baseLen, state.lengths); storage != baseLen {
				ensureLenIndex()[f] = storage
			}
			state.lengths = nil
		}
		if state.changed {
			changedCount++
		}
		state.releaseOwned()
	}

	inheritNumericRangeBucketCache(next, prev)
	if changedCount > 0 {
		changed := make([]bool, len(db.indexedFieldAccess))
		for i := range deltas.fields {
			if deltas.fields[i].changed {
				changed[i] = true
			}
		}
		inheritMaterializedPredCache(db, next, prev, changed)
	} else {
		inheritMaterializedPredCache(db, next, prev, nil)
	}

	return next
}
