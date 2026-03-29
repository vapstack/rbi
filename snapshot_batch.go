package rbi

import (
	"maps"
	"slices"
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

type batchLenDiff struct {
	oldExists bool
	oldLen    int
	newExists bool
	newLen    int
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

func addFieldBatchPostingDelta(fieldDelta map[string]batchPostingDelta, key string, idx uint64, isAdd bool) map[string]batchPostingDelta {
	if fieldDelta == nil {
		fieldDelta = getBatchPostingDeltaMap()
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

func collectFieldBatchPostingDiff(fieldDelta map[string]batchPostingDelta, idx uint64, oldVals, newVals []string) (map[string]batchPostingDelta, bool) {
	if len(oldVals) == 0 && len(newVals) == 0 {
		return fieldDelta, false
	}

	var changed bool
	i, j := 0, 0
	for i < len(oldVals) && j < len(newVals) {
		switch {
		case oldVals[i] < newVals[j]:
			fieldDelta = addFieldBatchPostingDelta(fieldDelta, oldVals[i], idx, false)
			changed = true
			i++
		case oldVals[i] > newVals[j]:
			fieldDelta = addFieldBatchPostingDelta(fieldDelta, newVals[j], idx, true)
			changed = true
			j++
		default:
			i++
			j++
		}
	}
	for ; i < len(oldVals); i++ {
		fieldDelta = addFieldBatchPostingDelta(fieldDelta, oldVals[i], idx, false)
		changed = true
	}
	for ; j < len(newVals); j++ {
		fieldDelta = addFieldBatchPostingDelta(fieldDelta, newVals[j], idx, true)
		changed = true
	}
	return fieldDelta, changed
}

func collectFixedFieldBatchPostingDiff(fieldDelta map[uint64]batchPostingDelta, idx uint64, oldVals, newVals []uint64) (map[uint64]batchPostingDelta, bool) {
	if len(oldVals) == 0 && len(newVals) == 0 {
		return fieldDelta, false
	}

	var changed bool
	i, j := 0, 0
	for i < len(oldVals) && j < len(newVals) {
		switch {
		case oldVals[i] < newVals[j]:
			fieldDelta = addFixedFieldBatchPostingDelta(fieldDelta, oldVals[i], idx, false)
			changed = true
			i++
		case oldVals[i] > newVals[j]:
			fieldDelta = addFixedFieldBatchPostingDelta(fieldDelta, newVals[j], idx, true)
			changed = true
			j++
		default:
			i++
			j++
		}
	}
	for ; i < len(oldVals); i++ {
		fieldDelta = addFixedFieldBatchPostingDelta(fieldDelta, oldVals[i], idx, false)
		changed = true
	}
	for ; j < len(newVals); j++ {
		fieldDelta = addFixedFieldBatchPostingDelta(fieldDelta, newVals[j], idx, true)
		changed = true
	}
	return fieldDelta, changed
}

func collectScalarFieldBatchPostingDiff(
	fieldDelta map[string]batchPostingDelta,
	idx uint64,
	oldOK bool,
	oldVal string,
	newOK bool,
	newVal string,
) (map[string]batchPostingDelta, bool) {
	switch {
	case oldOK && newOK:
		if oldVal == newVal {
			return fieldDelta, false
		}
		fieldDelta = addFieldBatchPostingDelta(fieldDelta, oldVal, idx, false)
		fieldDelta = addFieldBatchPostingDelta(fieldDelta, newVal, idx, true)
		return fieldDelta, true
	case oldOK:
		fieldDelta = addFieldBatchPostingDelta(fieldDelta, oldVal, idx, false)
		return fieldDelta, true
	case newOK:
		fieldDelta = addFieldBatchPostingDelta(fieldDelta, newVal, idx, true)
		return fieldDelta, true
	default:
		return fieldDelta, false
	}
}

func collectScalarFixedFieldBatchPostingDiff(
	fieldDelta map[uint64]batchPostingDelta,
	idx uint64,
	oldOK bool,
	oldVal uint64,
	newOK bool,
	newVal uint64,
) (map[uint64]batchPostingDelta, bool) {
	switch {
	case oldOK && newOK:
		if oldVal == newVal {
			return fieldDelta, false
		}
		fieldDelta = addFixedFieldBatchPostingDelta(fieldDelta, oldVal, idx, false)
		fieldDelta = addFixedFieldBatchPostingDelta(fieldDelta, newVal, idx, true)
		return fieldDelta, true
	case oldOK:
		fieldDelta = addFixedFieldBatchPostingDelta(fieldDelta, oldVal, idx, false)
		return fieldDelta, true
	case newOK:
		fieldDelta = addFixedFieldBatchPostingDelta(fieldDelta, newVal, idx, true)
		return fieldDelta, true
	default:
		return fieldDelta, false
	}
}

func collectFieldBatchLenDiff(
	fieldDelta map[string]batchPostingDelta,
	idx uint64,
	diff batchLenDiff,
	useZeroComplement bool,
) (map[string]batchPostingDelta, bool) {
	logicalChange := diff.oldExists != diff.newExists || diff.oldLen != diff.newLen
	if !logicalChange {
		return fieldDelta, false
	}

	var changed bool
	addLenBucketDelta := func(length int, isAdd bool) {
		fieldDelta = addFieldBatchPostingDelta(fieldDelta, uint64ByteStr(uint64(length)), idx, isAdd)
		changed = true
	}
	addNonEmptyDelta := func(isAdd bool) {
		fieldDelta = addFieldBatchPostingDelta(fieldDelta, lenIndexNonEmptyKey, idx, isAdd)
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
		out.AndNotInPlace(deltaValue.remove)
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
			out.OrInPlace(deltaValue.add)
		}
	}

	if changed && shouldOptimizeAfterBatchDelta(base, deltaValue, out) {
		out.Optimize()
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

func sortedBatchPostingDeltasBufOwned(deltas map[string]batchPostingDelta, fixed8 bool) *keyedBatchPostingDeltaSliceBuf {
	if len(deltas) == 0 {
		releaseBatchPostingDeltaMap(deltas)
		return nil
	}

	buf := getKeyedBatchPostingDeltaSliceBuf(len(deltas))

	for raw, delta := range deltas {
		if delta.add.IsEmpty() && delta.remove.IsEmpty() {
			continue
		}
		buf.values = append(buf.values, keyedBatchPostingDelta{
			key:   indexKeyFromStoredString(raw, fixed8),
			delta: delta,
		})
	}
	releaseBatchPostingDeltaMap(deltas)
	if len(buf.values) == 0 {
		releaseKeyedBatchPostingDeltaSliceBuf(buf)
		return nil
	}

	slices.SortFunc(buf.values, func(a, b keyedBatchPostingDelta) int {
		return compareIndexKeys(a.key, b.key)
	})
	return buf
}

func sortedFixedBatchPostingDeltasBufOwned(deltas map[uint64]batchPostingDelta) *keyedBatchPostingDeltaSliceBuf {
	if len(deltas) == 0 {
		releaseFixedBatchPostingDeltaMap(deltas)
		return nil
	}

	buf := getKeyedBatchPostingDeltaSliceBuf(len(deltas))

	for raw, delta := range deltas {
		if delta.add.IsEmpty() && delta.remove.IsEmpty() {
			continue
		}
		buf.values = append(buf.values, keyedBatchPostingDelta{
			key:   indexKeyFromU64(raw),
			delta: delta,
		})
	}
	releaseFixedBatchPostingDeltaMap(deltas)
	if len(buf.values) == 0 {
		releaseKeyedBatchPostingDeltaSliceBuf(buf)
		return nil
	}

	slices.SortFunc(buf.values, func(a, b keyedBatchPostingDelta) int {
		return compareIndexKeys(a.key, b.key)
	})
	return buf
}

func applyFieldPostingDiffSorted(base *[]index, deltaKeys []keyedBatchPostingDelta) *[]index {
	if len(deltaKeys) == 0 {
		return base
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
	deltas map[string]batchPostingDelta,
	fixed8 bool,
	allowChunk bool,
) fieldIndexStorage {
	buf := sortedBatchPostingDeltasBufOwned(deltas, fixed8)
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
	deltas map[uint64]batchPostingDelta,
	allowChunk bool,
) fieldIndexStorage {
	buf := sortedFixedBatchPostingDeltasBufOwned(deltas)
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

func applyLenFieldPostingDiffStorageOwned(base fieldIndexStorage, deltas map[string]batchPostingDelta) fieldIndexStorage {
	storage := applyFieldPostingDiffStorageOwned(base, deltas, false, false)
	if storage.flat != nil || storage.chunked != nil {
		return storage
	}
	empty := make([]index, 0)
	return newFlatFieldIndexStorage(&empty)
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

func sortedPostingAddsBufOwned(adds map[string]posting.List, fixed8 bool) *keyedBatchPostingDeltaSliceBuf {
	if len(adds) == 0 {
		releasePostingMap(adds)
		return nil
	}
	buf := getKeyedBatchPostingDeltaSliceBuf(len(adds))
	for raw, ids := range adds {
		if ids.IsEmpty() {
			continue
		}
		buf.values = append(buf.values, keyedBatchPostingDelta{
			key: indexKeyFromStoredString(raw, fixed8),
			delta: batchPostingDelta{
				add: ids,
			},
		})
	}
	releasePostingMap(adds)
	if len(buf.values) == 0 {
		releaseKeyedBatchPostingDeltaSliceBuf(buf)
		return nil
	}
	slices.SortFunc(buf.values, func(a, b keyedBatchPostingDelta) int {
		return compareIndexKeys(a.key, b.key)
	})
	return buf
}

func sortedFixedPostingAddsBufOwned(adds map[uint64]posting.List) *keyedBatchPostingDeltaSliceBuf {
	if len(adds) == 0 {
		releaseFixedPostingMap(adds)
		return nil
	}
	buf := getKeyedBatchPostingDeltaSliceBuf(len(adds))
	for raw, ids := range adds {
		if ids.IsEmpty() {
			continue
		}
		buf.values = append(buf.values, keyedBatchPostingDelta{
			key: indexKeyFromU64(raw),
			delta: batchPostingDelta{
				add: ids,
			},
		})
	}
	releaseFixedPostingMap(adds)
	if len(buf.values) == 0 {
		releaseKeyedBatchPostingDeltaSliceBuf(buf)
		return nil
	}
	slices.SortFunc(buf.values, func(a, b keyedBatchPostingDelta) int {
		return compareIndexKeys(a.key, b.key)
	})
	return buf
}

func mergeInsertOnlyFieldStorageOwned(
	base fieldIndexStorage,
	adds map[string]posting.List,
	fixed8 bool,
	allowChunk bool,
) fieldIndexStorage {
	if len(adds) == 0 {
		releasePostingMap(adds)
		return base
	}
	if base.flat == nil && base.chunked == nil {
		if allowChunk {
			return newRegularFieldIndexStorageFromPostingMapOwned(adds, fixed8)
		}
		return newFlatFieldIndexStorageFromPostingMapOwned(adds, fixed8)
	}
	if allowChunk && base.chunked != nil {
		buf := sortedPostingAddsBufOwned(adds, fixed8)
		if buf == nil {
			return base
		}
		defer releaseKeyedBatchPostingDeltaSliceBuf(buf)
		return applyFieldPostingDiffChunked(base.chunked, buf.values)
	}
	if allowChunk && base.flat != nil && shouldUseChunkedFieldIndex(len(*base.flat)+len(adds)) {
		buf := sortedPostingAddsBufOwned(adds, fixed8)
		if buf == nil {
			return base
		}
		defer releaseKeyedBatchPostingDeltaSliceBuf(buf)
		return applyFieldPostingDiffChunked(buildChunkedFieldIndexRoot(*base.flat), buf.values)
	}
	slice := mergeInsertOnlyFieldSliceOwned(base.flatSlice(), adds, fixed8)
	if !allowChunk {
		return newFlatFieldIndexStorage(slice)
	}
	return newRegularFieldIndexStorage(slice)
}

func mergeInsertOnlyFixedFieldStorageOwned(
	base fieldIndexStorage,
	adds map[uint64]posting.List,
	allowChunk bool,
) fieldIndexStorage {
	if len(adds) == 0 {
		releaseFixedPostingMap(adds)
		return base
	}
	if base.flat == nil && base.chunked == nil {
		if allowChunk {
			return newRegularFieldIndexStorageFromFixedPostingMapOwned(adds)
		}
		return newFlatFieldIndexStorageFromFixedPostingMapOwned(adds)
	}
	if allowChunk && base.chunked != nil {
		buf := sortedFixedPostingAddsBufOwned(adds)
		if buf == nil {
			return base
		}
		defer releaseKeyedBatchPostingDeltaSliceBuf(buf)
		return applyFieldPostingDiffChunked(base.chunked, buf.values)
	}
	if allowChunk && base.flat != nil && shouldUseChunkedFieldIndex(len(*base.flat)+len(adds)) {
		buf := sortedFixedPostingAddsBufOwned(adds)
		if buf == nil {
			return base
		}
		defer releaseKeyedBatchPostingDeltaSliceBuf(buf)
		return applyFieldPostingDiffChunked(buildChunkedFieldIndexRoot(*base.flat), buf.values)
	}
	slice := mergeInsertOnlyFixedFieldSliceOwned(base.flatSlice(), adds)
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

		index:             maps.Clone(prev.index),
		nilIndex:          maps.Clone(prev.nilIndex),
		lenIndex:          maps.Clone(prev.lenIndex),
		lenZeroComplement: maps.Clone(prev.lenZeroComplement),
		universe:          prev.universe,
		strmap:            db.strmap.snapshot(),
	}
	db.initSnapshotRuntimeCaches(next)

	normalized := normalizePreparedBatchForSnapshot(prepared)
	deltas := indexedFieldBatchDeltas{
		fields: make([]snapshotFieldBatchState, len(db.indexedFieldAccess)),
	}

	universeOwned := false
	ensureUniverse := func() *posting.List {
		if universeOwned {
			return &next.universe
		}
		next.universe = next.universe.Clone()
		universeOwned = true
		return &next.universe
	}

	for i := range normalized {
		op := normalized[i]
		switch {
		case op.oldVal == nil && op.newVal != nil:
			ensureUniverse().Add(op.idx)
		case op.oldVal != nil && op.newVal == nil:
			ensureUniverse().Remove(op.idx)
		}
		db.collectSnapshotBatchEntryDiffs(op, &deltas, prev.lenZeroComplement)
	}

	changedCount := 0
	for i, acc := range db.indexedFieldAccess {
		f := acc.name
		state := &deltas.fields[i]
		if storage := acc.applySnapshotBatchStorageOwned(next.index[f], state, true); storage.keyCount() == 0 {
			delete(next.index, f)
		} else {
			next.index[f] = storage
		}
		if storage := acc.applySnapshotBatchNilStorageOwned(next.nilIndex[f], state); storage.keyCount() == 0 {
			delete(next.nilIndex, f)
		} else {
			next.nilIndex[f] = storage
		}
		if state.lengths != nil {
			next.lenIndex[f] = applyLenFieldPostingDiffStorageOwned(next.lenIndex[f], state.lengths)
			state.lengths = nil
		}
		if state.changed {
			changedCount++
		}
	}

	inheritNumericRangeBucketCache(next, prev)
	if changedCount > 0 {
		changedIndexFields := make(map[string]struct{}, changedCount)
		for i := range deltas.fields {
			if deltas.fields[i].changed {
				changedIndexFields[db.indexedFieldAccess[i].name] = struct{}{}
			}
		}
		inheritMaterializedPredCache(next, prev, changedIndexFields)
	} else {
		inheritMaterializedPredCache(next, prev, nil)
	}

	return next
}
