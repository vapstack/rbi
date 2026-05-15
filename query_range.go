package rbi

import (
	"reflect"

	"github.com/vapstack/rbi/internal/indexdata"

	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/schema"
)

func isNumericScalarKind(kind reflect.Kind) bool {
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

func (qv *queryView) numericRangeFieldOrdinal(field string) (int, bool) {
	acc, ok := qv.engine.schema.IndexedByName[field]
	if !ok {
		return 0, false
	}
	return acc.Ordinal, true
}

func (qv *queryView) numericRangeBucketCacheEntry(field string, storage indexdata.FieldStorage, bucketSize, minFieldKeys int) *qcache.NumericRangeBucketEntry {
	ordinal, ok := qv.numericRangeFieldOrdinal(field)
	if !ok {
		return nil
	}
	return qv.snap.NumericRangeBucketCacheEntry(field, ordinal, storage, bucketSize, minFieldKeys)
}

func (qv *queryView) tryEvalNumericRangeBuckets(field string, fm *schema.Field, ov indexdata.FieldOverlay, br indexdata.OverlayRange) (postingResult, bool) {
	if !schema.FieldUsesOrderedNumericKeys(fm) {
		return postingResult{}, false
	}

	bucketSize := qv.engine.numericRangeBucketSize
	minFieldKeys := qv.engine.numericRangeBucketMinFieldKeys
	minSpan := qv.engine.numericRangeBucketMinSpanKeys
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

	storage, ok := qv.snap.FieldIndexStorage(field)
	if !ok || storage.KeyCount() == 0 {
		return postingResult{}, false
	}
	entry := qv.numericRangeBucketCacheEntry(field, storage, bucketSize, minFieldKeys)
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
					indexdata.OverlayRange{},
				)
			case cachedEnd == endFull && cachedStart > startFull:
				res = ov.MergeRangePostingsInto(
					res,
					ov.RangeByRanks(idx.BucketStart(startFull), idx.BucketStart(cachedStart)),
					indexdata.OverlayRange{},
				)
			}
		} else {
			res = ov.MergeRangePostingsInto(res, ov.RangeByRanks(idx.BucketStart(startFull), idx.BucketEnd(endFull)), indexdata.OverlayRange{})
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

func (qv *queryView) tryLoadNumericRangeBuckets(field string, fm *schema.Field, ov indexdata.FieldOverlay, br indexdata.OverlayRange) (postingResult, bool) {
	if !schema.FieldUsesOrderedNumericKeys(fm) {
		return postingResult{}, false
	}

	bucketSize := qv.engine.numericRangeBucketSize
	minFieldKeys := qv.engine.numericRangeBucketMinFieldKeys
	minSpan := qv.engine.numericRangeBucketMinSpanKeys
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

	storage, ok := qv.snap.FieldIndexStorage(field)
	if !ok || storage.KeyCount() == 0 {
		return postingResult{}, false
	}
	entry := qv.numericRangeBucketCacheEntry(field, storage, bucketSize, minFieldKeys)
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

func (qv *queryView) tryCountSnapshotNumericRange(field string, fm *schema.Field, ov indexdata.FieldOverlay, start, end int) (uint64, bool) {
	if !schema.FieldUsesOrderedNumericKeys(fm) {
		return 0, false
	}
	if start < 0 || start > end || end > ov.KeyCount() {
		return 0, false
	}

	storage, ok := qv.snap.FieldIndexStorage(field)
	if !ok || storage.KeyCount() == 0 {
		return 0, false
	}
	if storage.KeyCount() != ov.KeyCount() {
		return 0, false
	}
	return indexdata.NewFieldOverlayStorage(storage).RangeRows(start, end), true
}
