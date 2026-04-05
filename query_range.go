package rbi

import (
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/vapstack/rbi/internal/posting"
)

const numericRangeFullSpanCacheMaxEntries = 4

type numericRangeBucket struct {
	start int
	end   int
}

type numericRangeBucketIndex struct {
	bucketSize int
	keyCount   int
	buckets    []numericRangeBucket
}

type numericRangeBucketCacheEntry struct {
	storage       fieldIndexStorage
	idx           *numericRangeBucketIndex
	maxCard       uint64
	fullSpanCache sync.Map
	fullSpanCount atomic.Int32
}

func numericRangeFullSpanCacheKey(start, end int) uint64 {
	return uint64(uint32(start))<<32 | uint64(uint32(end))
}

func (e *numericRangeBucketCacheEntry) loadFullSpan(start, end int) (posting.List, bool) {
	if e == nil {
		return posting.List{}, false
	}
	v, ok := e.fullSpanCache.Load(numericRangeFullSpanCacheKey(start, end))
	if !ok {
		return posting.List{}, false
	}
	ids, ok := v.(posting.List)
	if !ok || ids.IsEmpty() {
		return posting.List{}, false
	}
	return ids.Borrow(), true
}

func (e *numericRangeBucketCacheEntry) tryStoreFullSpan(start, end int, ids posting.List) (posting.List, bool) {
	if e == nil || ids.IsEmpty() {
		return ids, false
	}
	key := numericRangeFullSpanCacheKey(start, end)
	if cached, ok := e.loadFullSpan(start, end); ok {
		ids.Release()
		return cached, true
	}
	if e.maxCard > 0 && ids.Cardinality() > e.maxCard {
		return ids, false
	}
	stored := ids
	if stored.IsBorrowed() {
		stored = stored.Clone()
	}
	for {
		n := e.fullSpanCount.Load()
		if n >= numericRangeFullSpanCacheMaxEntries {
			var evictKey any
			e.fullSpanCache.Range(func(k, v any) bool {
				evictKey = k
				return false
			})
			if evictKey == nil {
				if !stored.SharesPayload(ids) {
					stored.Release()
				}
				return ids, false
			}
			actual, deleted := e.fullSpanCache.LoadAndDelete(evictKey)
			if !deleted {
				continue
			}
			// loadFullSpan hands out Borrow() views, which are non-owning wrappers
			// over the same posting payload. Eviction must only stop future cache
			// hits; it cannot release the payload eagerly because concurrent readers
			// may still be traversing the evicted posting.
			_ = actual
			e.fullSpanCount.Add(-1)
			continue
		}
		if e.fullSpanCount.CompareAndSwap(n, n+1) {
			actual, loaded := e.fullSpanCache.LoadOrStore(key, stored)
			if loaded {
				e.fullSpanCount.Add(-1)
				if !stored.SharesPayload(ids) {
					stored.Release()
				}
				ids.Release()
				cached, _ := actual.(posting.List)
				if !cached.IsEmpty() {
					return cached.Borrow(), true
				}
				return posting.List{}, false
			}
			return stored.Borrow(), true
		}
	}
}

func (idx *numericRangeBucketIndex) fullBucketSpan(br overlayRange) (start, end int, ok bool) {
	if idx == nil || idx.bucketSize <= 0 || len(idx.buckets) == 0 {
		return 0, 0, false
	}
	if br.baseStart < 0 || br.baseStart >= br.baseEnd {
		return 0, 0, false
	}

	startBucket := br.baseStart / idx.bucketSize
	endBucket := (br.baseEnd - 1) / idx.bucketSize
	if startBucket < 0 {
		startBucket = 0
	}
	if endBucket >= len(idx.buckets) {
		endBucket = len(idx.buckets) - 1
	}
	if startBucket > endBucket {
		return 0, 0, false
	}

	start = startBucket
	if idx.buckets[start].start < br.baseStart {
		start++
	}
	end = endBucket
	if end >= 0 && idx.buckets[end].end > br.baseEnd {
		end--
	}
	if start > end {
		return 0, 0, false
	}
	return start, end, true
}

func satAddUint64(total, add uint64) uint64 {
	if ^uint64(0)-total < add {
		return ^uint64(0)
	}
	return total + add
}

func mergeOverlayRangeInto(dst posting.List, ov fieldOverlay, br overlayRange) posting.List {
	if br.baseStart >= br.baseEnd {
		return dst
	}
	cur := ov.newCursor(br, false)
	for {
		_, ids, ok := cur.next()
		if !ok {
			return dst
		}
		if ids.IsEmpty() {
			continue
		}
		dst = dst.BuildOr(ids)
	}
}

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

func buildNumericRangeBucketIndex(base []index, bucketSize, minFieldKeys int) *numericRangeBucketIndex {
	if bucketSize <= 0 || minFieldKeys <= 0 {
		return nil
	}
	if len(base) < minFieldKeys {
		return nil
	}
	if bucketSize < 16 {
		bucketSize = 16
	}

	bucketCount := (len(base) + bucketSize - 1) / bucketSize
	out := &numericRangeBucketIndex{
		bucketSize: bucketSize,
		keyCount:   len(base),
		buckets:    make([]numericRangeBucket, 0, bucketCount),
	}

	for start := 0; start < len(base); start += bucketSize {
		end := min(start+bucketSize, len(base))

		out.buckets = append(out.buckets, numericRangeBucket{
			start: start,
			end:   end,
		})
	}

	return out
}

func buildNumericRangeBucketIndexOverlay(ov fieldOverlay, bucketSize, minFieldKeys int) *numericRangeBucketIndex {
	if !ov.hasData() {
		return nil
	}
	if ov.chunked == nil {
		return buildNumericRangeBucketIndex(ov.base, bucketSize, minFieldKeys)
	}
	if bucketSize <= 0 || minFieldKeys <= 0 {
		return nil
	}
	if ov.keyCount() < minFieldKeys {
		return nil
	}
	if bucketSize < 16 {
		bucketSize = 16
	}

	keyCount := ov.keyCount()
	bucketCount := (keyCount + bucketSize - 1) / bucketSize
	out := &numericRangeBucketIndex{
		bucketSize: bucketSize,
		keyCount:   keyCount,
		buckets:    make([]numericRangeBucket, 0, bucketCount),
	}
	for start := 0; start < keyCount; start += bucketSize {
		end := min(start+bucketSize, keyCount)

		out.buckets = append(out.buckets, numericRangeBucket{
			start: start,
			end:   end,
		})
	}
	return out
}

func (s *indexSnapshot) getNumericRangeBucketCacheEntry(field string, storage fieldIndexStorage, bucketSize, minFieldKeys int) *numericRangeBucketCacheEntry {
	if s == nil || storage.keyCount() == 0 {
		return nil
	}
	ov := newFieldOverlayStorage(storage)
	cache := s.numericRangeBucketCache
	if cache == nil {
		return &numericRangeBucketCacheEntry{
			storage: storage,
			idx:     buildNumericRangeBucketIndexOverlay(ov, bucketSize, minFieldKeys),
			maxCard: s.matPredCacheMaxCard,
		}
	}

	if cached, ok := cache.Load(field); ok {
		if entry, ok := cached.(*numericRangeBucketCacheEntry); ok && entry != nil && entry.storage == storage {
			return entry
		}
	}

	entry := &numericRangeBucketCacheEntry{
		storage: storage,
		idx:     buildNumericRangeBucketIndexOverlay(ov, bucketSize, minFieldKeys),
		maxCard: s.matPredCacheMaxCard,
	}
	if actual, loaded := cache.LoadOrStore(field, entry); loaded {
		if stored, ok := actual.(*numericRangeBucketCacheEntry); ok && stored != nil && stored.storage == storage {
			return stored
		}
		cache.Store(field, entry)
	}

	return entry
}

func (qv *queryView[K, V]) tryEvalNumericRangeBuckets(field string, fm *field, ov fieldOverlay, br overlayRange) (postingResult, bool) {
	if fm == nil || fm.Slice || !isNumericScalarKind(fm.Kind) {
		return postingResult{}, false
	}

	bucketSize := qv.options.NumericRangeBucketSize
	minFieldKeys := qv.options.NumericRangeBucketMinFieldKeys
	minSpan := qv.options.NumericRangeBucketMinSpanKeys
	if bucketSize <= 0 || minFieldKeys <= 0 {
		return postingResult{}, false
	}
	if minSpan <= 0 {
		return postingResult{}, false
	}
	span := br.baseEnd - br.baseStart
	if span < minSpan {
		return postingResult{}, false
	}
	if br.baseStart >= br.baseEnd || ov.keyCount() == 0 {
		return postingResult{}, false
	}

	storage, ok := qv.snap.fieldIndexStorage(field)
	if !ok || storage.keyCount() == 0 {
		return postingResult{}, false
	}
	entry := qv.snap.getNumericRangeBucketCacheEntry(field, storage, bucketSize, minFieldKeys)
	if entry == nil || entry.idx == nil {
		return postingResult{}, false
	}
	idx := entry.idx
	if idx == nil || idx.bucketSize <= 0 || len(idx.buckets) == 0 {
		return postingResult{}, false
	}

	if idx.keyCount != ov.keyCount() {
		return postingResult{}, false
	}

	startFull, endFull, ok := idx.fullBucketSpan(br)
	if !ok {
		return postingResult{}, false
	}

	fullSpanReuse := newMaterializedPredReadOnlyReuse(
		qv.snap,
		materializedPredCacheKeyForNumericBucketSpan(field, startFull, endFull),
	)

	var res posting.List
	if cached, ok := entry.loadFullSpan(startFull, endFull); ok {
		res = cached
	}
	if res.IsEmpty() {
		if cached, ok := fullSpanReuse.load(); ok && !cached.IsEmpty() {
			res = cached
		}
	}
	if res.IsEmpty() {
		res = mergeOverlayRangeInto(res, ov, ov.rangeByRanks(idx.buckets[startFull].start, idx.buckets[endFull].end))
		res, _ = entry.tryStoreFullSpan(startFull, endFull, res)
	}

	leftEnd := min(br.baseEnd, idx.buckets[startFull].start)
	rightStart := max(br.baseStart, idx.buckets[endFull].end)
	if (leftEnd > br.baseStart) || (rightStart < br.baseEnd) {
		res = mergeOverlayRangeInto(res, ov, ov.rangeByRanks(br.baseStart, leftEnd))
		res = mergeOverlayRangeInto(res, ov, ov.rangeByRanks(rightStart, br.baseEnd))
	}

	return postingResult{ids: res}, true
}

func (qv *queryView[K, V]) tryLoadNumericRangeBuckets(field string, fm *field, ov fieldOverlay, br overlayRange) (postingResult, bool) {
	if fm == nil || fm.Slice || !isNumericScalarKind(fm.Kind) {
		return postingResult{}, false
	}

	bucketSize := qv.options.NumericRangeBucketSize
	minFieldKeys := qv.options.NumericRangeBucketMinFieldKeys
	minSpan := qv.options.NumericRangeBucketMinSpanKeys
	if bucketSize <= 0 || minFieldKeys <= 0 || minSpan <= 0 {
		return postingResult{}, false
	}
	span := br.baseEnd - br.baseStart
	if span < minSpan {
		return postingResult{}, false
	}
	if br.baseStart >= br.baseEnd || ov.keyCount() == 0 {
		return postingResult{}, false
	}

	storage, ok := qv.snap.fieldIndexStorage(field)
	if !ok || storage.keyCount() == 0 {
		return postingResult{}, false
	}
	entry := qv.snap.getNumericRangeBucketCacheEntry(field, storage, bucketSize, minFieldKeys)
	if entry == nil || entry.idx == nil {
		return postingResult{}, false
	}
	idx := entry.idx
	if idx == nil || idx.bucketSize <= 0 || len(idx.buckets) == 0 {
		return postingResult{}, false
	}
	if idx.keyCount != ov.keyCount() {
		return postingResult{}, false
	}

	startFull, endFull, ok := idx.fullBucketSpan(br)
	if !ok {
		return postingResult{}, false
	}

	fullSpanReuse := newMaterializedPredReadOnlyReuse(
		qv.snap,
		materializedPredCacheKeyForNumericBucketSpan(field, startFull, endFull),
	)

	var res posting.List
	if cached, ok := entry.loadFullSpan(startFull, endFull); ok {
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

	leftEnd := min(br.baseEnd, idx.buckets[startFull].start)
	rightStart := max(br.baseStart, idx.buckets[endFull].end)
	if (leftEnd > br.baseStart) || (rightStart < br.baseEnd) {
		res = mergeOverlayRangeInto(res, ov, ov.rangeByRanks(br.baseStart, leftEnd))
		res = mergeOverlayRangeInto(res, ov, ov.rangeByRanks(rightStart, br.baseEnd))
	}

	return postingResult{ids: res}, true
}

func (qv *queryView[K, V]) tryCountSnapshotNumericRange(field string, fm *field, ov fieldOverlay, start, end int) (uint64, bool) {
	if fm == nil || fm.Slice || !isNumericScalarKind(fm.Kind) {
		return 0, false
	}
	if start < 0 || start > end || end > ov.keyCount() {
		return 0, false
	}

	storage, ok := qv.snap.fieldIndexStorage(field)
	if !ok || storage.keyCount() == 0 {
		return 0, false
	}
	if storage.keyCount() != ov.keyCount() {
		return 0, false
	}
	if storage.chunked != nil {
		root := storage.chunked
		return root.rangeRows(root.posForRank(start), root.posForRank(end)), true
	}
	if storage.flat == nil {
		return 0, false
	}
	return countBaseIndexRangeCardinality(*storage.flat, start, end), true
}
