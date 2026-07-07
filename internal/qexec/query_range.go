package qexec

import (
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
)

type numericRangeBucketSpan struct {
	snap      *snapshot.View
	entry     *qcache.NumericRangeBucketEntry
	index     qcache.NumericRangeBucketIndex
	fieldSlot int
	start     int
	end       int
}

func (qv *View) numericRangeBucketSpan(field string, fieldOrdinal int, fm *schema.Field, ov indexdata.FieldIndexView, br indexdata.FieldIndexRange) (numericRangeBucketSpan, bool) {
	if !schema.FieldUsesOrderedNumericKeys(fm) {
		return numericRangeBucketSpan{}, false
	}

	bucketSize := qv.exec.NumericRangeBucketSize
	minFieldKeys := qv.exec.NumericRangeBucketMinFieldKeys
	minSpan := qv.exec.NumericRangeBucketMinSpanKeys

	if bucketSize <= 0 || minFieldKeys <= 0 || minSpan <= 0 {
		return numericRangeBucketSpan{}, false
	}

	span := br.BaseEnd - br.BaseStart
	if span < minSpan {
		return numericRangeBucketSpan{}, false
	}

	if br.BaseStart >= br.BaseEnd || ov.KeyCount() == 0 {
		return numericRangeBucketSpan{}, false
	}

	desc := qv.exec.fields[fieldOrdinal]
	storage := qv.snap.Index[desc.storageOrdinal]
	entry := qv.snap.NumericRangeBucketCacheEntry(field, desc.storageOrdinal, storage, bucketSize, minFieldKeys)
	if entry == nil {
		return numericRangeBucketSpan{}, false
	}

	idx := entry.Index()
	if idx.BucketCount() == 0 {
		return numericRangeBucketSpan{}, false
	}
	if idx.KeyCount() != ov.KeyCount() {
		return numericRangeBucketSpan{}, false
	}

	startFull, endFull, ok := idx.FullBucketSpan(br)
	if !ok {
		return numericRangeBucketSpan{}, false
	}

	return numericRangeBucketSpan{
		snap:      qv.snap,
		entry:     entry,
		index:     idx,
		fieldSlot: desc.storageOrdinal,
		start:     startFull,
		end:       endFull,
	}, true
}

func (qv *View) tryEvalNumericRangeBuckets(field string, fieldOrdinal int, fm *schema.Field, ov indexdata.FieldIndexView, br indexdata.FieldIndexRange) (postingResult, bool) {
	span, ok := qv.numericRangeBucketSpan(field, fieldOrdinal, fm, ov, br)
	if !ok {
		return postingResult{}, false
	}
	return evalNumericRangeBucketSpan(ov, br, span), true
}

func evalNumericRangeBucketSpan(ov indexdata.FieldIndexView, br indexdata.FieldIndexRange, span numericRangeBucketSpan) postingResult {
	leftEnd := min(br.BaseEnd, span.index.BucketStart(span.start))
	rightStart := max(br.BaseStart, span.index.BucketEnd(span.end))
	hasEdge := leftEnd > br.BaseStart || rightStart < br.BaseEnd
	if hasEdge {
		if cached, ok := span.snap.LoadNumericRangeExactResult(span.fieldSlot, br.BaseStart, br.BaseEnd); ok {
			return postingResult{ids: cached}
		}
	}

	var res posting.List
	if cached, ok := span.snap.LoadNumericRangeFullSpan(span.entry, span.fieldSlot, span.start, span.end); ok {
		res = cached
	}

	if res.IsEmpty() {
		if cached, cachedStart, cachedEnd, ok := span.snap.LoadExtendedNumericRangeFullSpan(span.entry, span.fieldSlot, span.start, span.end); ok {
			res = cached
			switch {

			case cachedStart == span.start && cachedEnd < span.end:
				res = ov.MergeRangePostingsInto(
					res,
					ov.RangeByRanks(span.index.BucketStart(cachedEnd+1), span.index.BucketEnd(span.end)),
					indexdata.FieldIndexRange{},
				)

			case cachedEnd == span.end && cachedStart > span.start:
				res = ov.MergeRangePostingsInto(
					res,
					ov.RangeByRanks(span.index.BucketStart(span.start), span.index.BucketStart(cachedStart)),
					indexdata.FieldIndexRange{},
				)
			}

		} else {
			res = ov.MergeRangePostingsInto(res, ov.RangeByRanks(span.index.BucketStart(span.start), span.index.BucketEnd(span.end)), indexdata.FieldIndexRange{})
		}
		res, _ = span.snap.TryStoreNumericRangeFullSpan(span.entry, span.fieldSlot, span.start, span.end, res)
	}

	if hasEdge {
		res = ov.MergeRangePostingsInto(
			res,
			ov.RangeByRanks(br.BaseStart, leftEnd),
			ov.RangeByRanks(rightStart, br.BaseEnd),
		)
		res, _ = span.snap.TryStoreNumericRangeExactResult(span.fieldSlot, br.BaseStart, br.BaseEnd, res)
	}

	return postingResult{ids: res}
}

func (qv *View) tryEvalNumericRangeBucketsUntil(field string, fieldOrdinal int, fm *schema.Field, ov indexdata.FieldIndexView, br indexdata.FieldIndexRange, cancel *indexdata.RangePostingsUnionCancel) (postingResult, bool) {
	if cancel.Canceled() {
		return postingResult{}, false
	}
	span, ok := qv.numericRangeBucketSpan(field, fieldOrdinal, fm, ov, br)
	if !ok {
		return postingResult{}, false
	}
	if cancel.Canceled() {
		return postingResult{}, false
	}
	return evalNumericRangeBucketSpanUntil(ov, br, span, cancel)
}

func evalNumericRangeBucketSpanUntil(ov indexdata.FieldIndexView, br indexdata.FieldIndexRange, span numericRangeBucketSpan, cancel *indexdata.RangePostingsUnionCancel) (postingResult, bool) {
	leftEnd := min(br.BaseEnd, span.index.BucketStart(span.start))
	rightStart := max(br.BaseStart, span.index.BucketEnd(span.end))
	hasEdge := leftEnd > br.BaseStart || rightStart < br.BaseEnd
	if hasEdge {
		if cached, ok := span.snap.LoadNumericRangeExactResult(span.fieldSlot, br.BaseStart, br.BaseEnd); ok {
			if cancel.Canceled() {
				cached.Release()
				return postingResult{}, false
			}
			return postingResult{ids: cached}, true
		}
	}

	var res posting.List
	if cached, ok := span.snap.LoadNumericRangeFullSpan(span.entry, span.fieldSlot, span.start, span.end); ok {
		res = cached
	}

	if res.IsEmpty() {
		var pending indexdata.FieldIndexRange
		if cached, cachedStart, cachedEnd, ok := span.snap.LoadExtendedNumericRangeFullSpan(span.entry, span.fieldSlot, span.start, span.end); ok {
			res = cached
			switch {

			case cachedStart == span.start && cachedEnd < span.end:
				pending = ov.RangeByRanks(span.index.BucketStart(cachedEnd+1), span.index.BucketEnd(span.end))

			case cachedEnd == span.end && cachedStart > span.start:
				pending = ov.RangeByRanks(span.index.BucketStart(span.start), span.index.BucketStart(cachedStart))
			}

		} else {
			pending = ov.RangeByRanks(span.index.BucketStart(span.start), span.index.BucketEnd(span.end))
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
		res, _ = span.snap.TryStoreNumericRangeFullSpan(span.entry, span.fieldSlot, span.start, span.end, res)
	}

	if hasEdge {
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
	if hasEdge {
		res, _ = span.snap.TryStoreNumericRangeExactResult(span.fieldSlot, br.BaseStart, br.BaseEnd, res)
	}

	return postingResult{ids: res}, true
}

func (qv *View) tryLoadNumericRangeBuckets(field string, fieldOrdinal int, fm *schema.Field, ov indexdata.FieldIndexView, br indexdata.FieldIndexRange) (postingResult, bool) {
	span, ok := qv.numericRangeBucketSpan(field, fieldOrdinal, fm, ov, br)
	if !ok {
		return postingResult{}, false
	}
	return loadNumericRangeBucketSpan(ov, br, span)
}

func loadNumericRangeBucketSpan(ov indexdata.FieldIndexView, br indexdata.FieldIndexRange, span numericRangeBucketSpan) (postingResult, bool) {
	leftEnd := min(br.BaseEnd, span.index.BucketStart(span.start))
	rightStart := max(br.BaseStart, span.index.BucketEnd(span.end))
	hasEdge := leftEnd > br.BaseStart || rightStart < br.BaseEnd
	if hasEdge {
		if cached, ok := span.snap.LoadNumericRangeExactResult(span.fieldSlot, br.BaseStart, br.BaseEnd); ok {
			return postingResult{ids: cached}, true
		}
	}

	var res posting.List
	if cached, ok := span.snap.LoadNumericRangeFullSpan(span.entry, span.fieldSlot, span.start, span.end); ok {
		res = cached
	}
	if res.IsEmpty() {
		return postingResult{}, false
	}

	if hasEdge {
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
