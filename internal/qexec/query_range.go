package qexec

import (
	"github.com/vapstack/rbi/internal/indexdata"

	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/schema"
)

func (qv *View) tryEvalNumericRangeBuckets(field string, fieldOrdinal int, fm *schema.Field, ov indexdata.FieldIndexView, br indexdata.FieldIndexRange) (postingResult, bool) {
	if !schema.FieldUsesOrderedNumericKeys(fm) {
		return postingResult{}, false
	}

	bucketSize := qv.exec.NumericRangeBucketSize
	minFieldKeys := qv.exec.NumericRangeBucketMinFieldKeys
	minSpan := qv.exec.NumericRangeBucketMinSpanKeys

	if bucketSize <= 0 || minFieldKeys <= 0 || minSpan <= 0 {
		return postingResult{}, false
	}

	span := br.BaseEnd - br.BaseStart
	if span < minSpan {
		return postingResult{}, false
	}

	if br.BaseStart >= br.BaseEnd || ov.KeyCount() == 0 {
		return postingResult{}, false
	}

	desc := qv.exec.fields[fieldOrdinal]
	storage := qv.snap.Index[desc.storageOrdinal]
	entry := qv.snap.NumericRangeBucketCacheEntry(field, desc.storageOrdinal, storage, bucketSize, minFieldKeys)
	if entry == nil {
		return postingResult{}, false
	}

	idx := entry.Index()
	if idx.BucketCount() == 0 {
		return postingResult{}, false
	}
	if idx.KeyCount() != ov.KeyCount() {
		return postingResult{}, false
	}

	startFull, endFull, ok := idx.FullBucketSpan(br)
	if !ok {
		return postingResult{}, false
	}

	fullSpanReuse := newMaterializedPredReadOnlyReuse(
		qv.snap,
		qcache.MaterializedPredKeyForNumericBucketSpan(field, startFull, endFull),
	)

	var res posting.List
	if cached, ok := entry.LoadFullSpan(startFull, endFull); ok {
		res = cached
	}

	if res.IsEmpty() {
		if cached, ok := fullSpanReuse.load(); ok && !cached.IsEmpty() {
			res = cached
		}
	}

	if res.IsEmpty() {
		if cached, cachedStart, cachedEnd, ok := entry.LoadExtendedFullSpan(startFull, endFull); ok {
			res = cached
			switch {

			case cachedStart == startFull && cachedEnd < endFull:
				res = ov.MergeRangePostingsInto(
					res,
					ov.RangeByRanks(idx.BucketStart(cachedEnd+1), idx.BucketEnd(endFull)),
					indexdata.FieldIndexRange{},
				)

			case cachedEnd == endFull && cachedStart > startFull:
				res = ov.MergeRangePostingsInto(
					res,
					ov.RangeByRanks(idx.BucketStart(startFull), idx.BucketStart(cachedStart)),
					indexdata.FieldIndexRange{},
				)
			}

		} else {
			res = ov.MergeRangePostingsInto(res, ov.RangeByRanks(idx.BucketStart(startFull), idx.BucketEnd(endFull)), indexdata.FieldIndexRange{})
		}
		res, _ = entry.TryStoreFullSpan(startFull, endFull, res)
	}

	leftEnd := min(br.BaseEnd, idx.BucketStart(startFull))
	rightStart := max(br.BaseStart, idx.BucketEnd(endFull))

	if (leftEnd > br.BaseStart) || (rightStart < br.BaseEnd) {
		res = ov.MergeRangePostingsInto(
			res,
			ov.RangeByRanks(br.BaseStart, leftEnd),
			ov.RangeByRanks(rightStart, br.BaseEnd),
		)
	}

	return postingResult{ids: res}, true
}

func (qv *View) tryEvalNumericRangeBucketsUntil(field string, fieldOrdinal int, fm *schema.Field, ov indexdata.FieldIndexView, br indexdata.FieldIndexRange, cancel *indexdata.RangePostingsUnionCancel) (postingResult, bool) {
	if !schema.FieldUsesOrderedNumericKeys(fm) {
		return postingResult{}, false
	}

	bucketSize := qv.exec.NumericRangeBucketSize
	minFieldKeys := qv.exec.NumericRangeBucketMinFieldKeys
	minSpan := qv.exec.NumericRangeBucketMinSpanKeys

	if bucketSize <= 0 || minFieldKeys <= 0 || minSpan <= 0 {
		return postingResult{}, false
	}

	span := br.BaseEnd - br.BaseStart
	if span < minSpan {
		return postingResult{}, false
	}

	if br.BaseStart >= br.BaseEnd || ov.KeyCount() == 0 {
		return postingResult{}, false
	}
	if cancel.Canceled() {
		return postingResult{}, false
	}

	desc := qv.exec.fields[fieldOrdinal]
	storage := qv.snap.Index[desc.storageOrdinal]
	entry := qv.snap.NumericRangeBucketCacheEntry(field, desc.storageOrdinal, storage, bucketSize, minFieldKeys)
	if entry == nil {
		return postingResult{}, false
	}
	if cancel.Canceled() {
		return postingResult{}, false
	}

	idx := entry.Index()
	if idx.BucketCount() == 0 {
		return postingResult{}, false
	}
	if idx.KeyCount() != ov.KeyCount() {
		return postingResult{}, false
	}

	startFull, endFull, ok := idx.FullBucketSpan(br)
	if !ok {
		return postingResult{}, false
	}

	fullSpanReuse := newMaterializedPredReadOnlyReuse(
		qv.snap,
		qcache.MaterializedPredKeyForNumericBucketSpan(field, startFull, endFull),
	)

	var res posting.List
	if cached, ok := entry.LoadFullSpan(startFull, endFull); ok {
		res = cached
	}

	if res.IsEmpty() {
		if cached, ok := fullSpanReuse.load(); ok && !cached.IsEmpty() {
			res = cached
		}
	}

	if res.IsEmpty() {
		var pending indexdata.FieldIndexRange
		if cached, cachedStart, cachedEnd, ok := entry.LoadExtendedFullSpan(startFull, endFull); ok {
			res = cached
			switch {

			case cachedStart == startFull && cachedEnd < endFull:
				pending = ov.RangeByRanks(idx.BucketStart(cachedEnd+1), idx.BucketEnd(endFull))

			case cachedEnd == endFull && cachedStart > startFull:
				pending = ov.RangeByRanks(idx.BucketStart(startFull), idx.BucketStart(cachedStart))
			}

		} else {
			pending = ov.RangeByRanks(idx.BucketStart(startFull), idx.BucketEnd(endFull))
		}
		if !pending.Empty() {
			pendingIDs, ok := ov.UnionRangePostingsUntil(pending, indexdata.FieldIndexRange{}, cancel)
			if !ok {
				res.Release()
				return postingResult{}, false
			}
			if res.IsEmpty() {
				res = pendingIDs
			} else {
				res = res.BuildMergedOwned(pendingIDs)
			}
		}
		if cancel.Canceled() {
			res.Release()
			return postingResult{}, false
		}
		res, _ = entry.TryStoreFullSpan(startFull, endFull, res)
	}

	leftEnd := min(br.BaseEnd, idx.BucketStart(startFull))
	rightStart := max(br.BaseStart, idx.BucketEnd(endFull))

	if (leftEnd > br.BaseStart) || (rightStart < br.BaseEnd) {
		edgeIDs, ok := ov.UnionRangePostingsUntil(
			ov.RangeByRanks(br.BaseStart, leftEnd),
			ov.RangeByRanks(rightStart, br.BaseEnd),
			cancel,
		)
		if !ok {
			res.Release()
			return postingResult{}, false
		}
		if res.IsEmpty() {
			res = edgeIDs
		} else {
			res = res.BuildMergedOwned(edgeIDs)
		}
	}
	if cancel.Canceled() {
		res.Release()
		return postingResult{}, false
	}

	return postingResult{ids: res}, true
}

func (qv *View) tryLoadNumericRangeBuckets(field string, fieldOrdinal int, fm *schema.Field, ov indexdata.FieldIndexView, br indexdata.FieldIndexRange) (postingResult, bool) {
	if !schema.FieldUsesOrderedNumericKeys(fm) {
		return postingResult{}, false
	}

	bucketSize := qv.exec.NumericRangeBucketSize
	minFieldKeys := qv.exec.NumericRangeBucketMinFieldKeys
	minSpan := qv.exec.NumericRangeBucketMinSpanKeys

	if bucketSize <= 0 || minFieldKeys <= 0 || minSpan <= 0 {
		return postingResult{}, false
	}

	span := br.BaseEnd - br.BaseStart
	if span < minSpan {
		return postingResult{}, false
	}

	if br.BaseStart >= br.BaseEnd || ov.KeyCount() == 0 {
		return postingResult{}, false
	}

	desc := qv.exec.fields[fieldOrdinal]
	storage := qv.snap.Index[desc.storageOrdinal]
	entry := qv.snap.NumericRangeBucketCacheEntry(field, desc.storageOrdinal, storage, bucketSize, minFieldKeys)
	if entry == nil {
		return postingResult{}, false
	}

	idx := entry.Index()
	if idx.BucketCount() == 0 {
		return postingResult{}, false
	}

	if idx.KeyCount() != ov.KeyCount() {
		return postingResult{}, false
	}

	startFull, endFull, ok := idx.FullBucketSpan(br)
	if !ok {
		return postingResult{}, false
	}

	fullSpanReuse := newMaterializedPredReadOnlyReuse(
		qv.snap,
		qcache.MaterializedPredKeyForNumericBucketSpan(field, startFull, endFull),
	)

	var res posting.List
	if cached, ok := entry.LoadFullSpan(startFull, endFull); ok {
		res = cached
	}
	if res.IsEmpty() {
		if cached, ok := fullSpanReuse.load(); ok && !cached.IsEmpty() {
			res = cached
		}
	}
	if res.IsEmpty() {
		return postingResult{}, false
	}

	leftEnd := min(br.BaseEnd, idx.BucketStart(startFull))
	rightStart := max(br.BaseStart, idx.BucketEnd(endFull))

	if (leftEnd > br.BaseStart) || (rightStart < br.BaseEnd) {
		res = ov.MergeRangePostingsInto(
			res,
			ov.RangeByRanks(br.BaseStart, leftEnd),
			ov.RangeByRanks(rightStart, br.BaseEnd),
		)
	}

	return postingResult{ids: res}, true
}

func (qv *View) trySnapshotNumericRangeCardinality(field string, fieldOrdinal int, fm *schema.Field, ov indexdata.FieldIndexView, start, end int) (uint64, bool) {
	if !schema.FieldUsesOrderedNumericKeys(fm) {
		return 0, false
	}
	if start < 0 || start > end || end > ov.KeyCount() {
		return 0, false
	}

	desc := qv.exec.fields[fieldOrdinal]
	storage := qv.snap.Index[desc.storageOrdinal]
	keyCount := storage.KeyCount()
	if keyCount == 0 || keyCount != ov.KeyCount() {
		return 0, false
	}
	return indexdata.NewFieldIndexViewFromStorage(storage).RangeRows(start, end), true
}
