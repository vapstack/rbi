package rbi

import (
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/vapstack/rbi/internal/roaring64"
)

const numericRangeFullSpanCacheMaxEntries = 4

type numericRangeBucket struct {
	start int
	end   int
	ids   *roaring64.Bitmap
	rows  uint64
}

type numericRangeBucketIndex struct {
	bucketSize      int
	keyCount        int
	buckets         []numericRangeBucket
	prefixRows      []uint64
	superBucketSpan int
	superBuckets    []numericRangeBucket
}

type numericRangeBucketCacheEntry struct {
	base          *[]index
	idx           *numericRangeBucketIndex
	fullSpanCache sync.Map
	fullSpanCount atomic.Int32
}

func numericRangeFullSpanCacheKey(start, end int) uint64 {
	return uint64(uint32(start))<<32 | uint64(uint32(end))
}

func (e *numericRangeBucketCacheEntry) loadFullSpan(start, end int) (*roaring64.Bitmap, bool) {
	if e == nil {
		return nil, false
	}
	v, ok := e.fullSpanCache.Load(numericRangeFullSpanCacheKey(start, end))
	if !ok {
		return nil, false
	}
	bm, ok := v.(*roaring64.Bitmap)
	if !ok || bm == nil || bm.IsEmpty() {
		return nil, false
	}
	return bm, true
}

func (e *numericRangeBucketCacheEntry) tryStoreFullSpan(start, end int, bm *roaring64.Bitmap, maxCardinality uint64) (*roaring64.Bitmap, bool) {
	if e == nil || bm == nil || bm.IsEmpty() {
		return nil, false
	}
	if maxCardinality > 0 && bm.GetCardinality() > maxCardinality {
		return nil, false
	}
	key := numericRangeFullSpanCacheKey(start, end)
	if cached, ok := e.loadFullSpan(start, end); ok {
		return cached, true
	}
	for {
		n := e.fullSpanCount.Load()
		if n >= numericRangeFullSpanCacheMaxEntries {
			return nil, false
		}
		if e.fullSpanCount.CompareAndSwap(n, n+1) {
			actual, loaded := e.fullSpanCache.LoadOrStore(key, bm)
			if loaded {
				e.fullSpanCount.Add(-1)
				cached, _ := actual.(*roaring64.Bitmap)
				if cached != nil && !cached.IsEmpty() {
					return cached, true
				}
				return nil, false
			}
			return bm, true
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

func (idx *numericRangeBucketIndex) fullBucketRows(startFull, endFull int) uint64 {
	if idx == nil || startFull < 0 || endFull < startFull || endFull >= len(idx.buckets) {
		return 0
	}
	if len(idx.prefixRows) != len(idx.buckets)+1 {
		return 0
	}
	return idx.prefixRows[endFull+1] - idx.prefixRows[startFull]
}

func (idx *numericRangeBucketIndex) countBaseRange(base []index, start, end int) (uint64, bool) {
	if idx == nil || idx.bucketSize <= 0 || len(idx.buckets) == 0 {
		return 0, false
	}
	if len(base) != idx.keyCount || start < 0 || start > end || end > len(base) {
		return 0, false
	}
	if start >= end {
		return 0, true
	}

	startFull := start / idx.bucketSize
	if startFull < len(idx.buckets) && idx.buckets[startFull].start < start {
		startFull++
	}
	endFull := (end - 1) / idx.bucketSize
	if endFull >= 0 && endFull < len(idx.buckets) && idx.buckets[endFull].end > end {
		endFull--
	}

	if startFull > endFull {
		return countBaseIndexRangeCardinality(base, start, end), true
	}

	total := idx.fullBucketRows(startFull, endFull)
	leftEnd := min(end, idx.buckets[startFull].start)
	rightStart := max(start, idx.buckets[endFull].end)

	total = satAddUint64(total, countBaseIndexRangeCardinality(base, start, leftEnd))
	total = satAddUint64(total, countBaseIndexRangeCardinality(base, rightStart, end))
	return total, true
}

func orBaseIndexRange(dst *roaring64.Bitmap, base []index, start, end int) {
	if dst == nil || start >= end {
		return
	}
	for i := start; i < end; i++ {
		ids := base[i].IDs
		if ids.IsEmpty() {
			continue
		}
		if ids.isSingleton() {
			dst.Add(ids.single)
			continue
		}
		orBitmapAdaptive(dst, ids.bitmap())
	}
}

func (idx *numericRangeBucketIndex) orFullBucketSpan(dst *roaring64.Bitmap, startFull, endFull int) {
	if idx == nil || dst == nil || startFull > endFull {
		return
	}
	if idx.superBucketSpan <= 1 || len(idx.superBuckets) == 0 {
		for b := startFull; b <= endFull; b++ {
			cur := idx.buckets[b]
			if cur.ids != nil && !cur.ids.IsEmpty() {
				orBitmapAdaptive(dst, cur.ids)
			}
		}
		return
	}

	span := idx.superBucketSpan
	b := startFull
	for b <= endFull && b%span != 0 {
		cur := idx.buckets[b]
		if cur.ids != nil && !cur.ids.IsEmpty() {
			orBitmapAdaptive(dst, cur.ids)
		}
		b++
	}
	for b+span-1 <= endFull {
		sb := idx.superBuckets[b/span]
		if sb.ids != nil && !sb.ids.IsEmpty() {
			orBitmapAdaptive(dst, sb.ids)
		}
		b += span
	}
	for ; b <= endFull; b++ {
		cur := idx.buckets[b]
		if cur.ids != nil && !cur.ids.IsEmpty() {
			orBitmapAdaptive(dst, cur.ids)
		}
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
		prefixRows: make([]uint64, 1, bucketCount+1),
	}

	for start := 0; start < len(base); start += bucketSize {
		end := min(start+bucketSize, len(base))

		var ids *roaring64.Bitmap
		rows := uint64(0)
		for i := start; i < end; i++ {
			src := base[i].IDs
			if src.IsEmpty() {
				continue
			}
			rows = satAddUint64(rows, src.Cardinality())
			if ids == nil {
				ids = roaring64.NewBitmap()
			}
			src.OrInto(ids)
		}
		if ids != nil {
			ids.RunOptimize()
		}

		out.buckets = append(out.buckets, numericRangeBucket{
			start: start,
			end:   end,
			ids:   ids,
			rows:  rows,
		})
		out.prefixRows = append(out.prefixRows, satAddUint64(out.prefixRows[len(out.prefixRows)-1], rows))
	}

	out.buildSuperBuckets(16)

	return out
}

func (idx *numericRangeBucketIndex) buildSuperBuckets(span int) {
	if idx == nil || span <= 1 || len(idx.buckets) < span {
		return
	}
	fullGroups := len(idx.buckets) / span
	if fullGroups <= 0 {
		return
	}

	idx.superBucketSpan = span
	idx.superBuckets = make([]numericRangeBucket, 0, fullGroups)
	for g := 0; g < fullGroups; g++ {
		startBucket := g * span
		endBucket := startBucket + span

		var ids *roaring64.Bitmap
		for i := startBucket; i < endBucket; i++ {
			src := idx.buckets[i].ids
			if src == nil || src.IsEmpty() {
				continue
			}
			if ids == nil {
				ids = roaring64.NewBitmap()
			}
			orBitmapAdaptive(ids, src)
		}
		if ids != nil {
			ids.RunOptimize()
		}

		idx.superBuckets = append(idx.superBuckets, numericRangeBucket{
			start: idx.buckets[startBucket].start,
			end:   idx.buckets[endBucket-1].end,
			ids:   ids,
		})
	}
}

func (s *indexSnapshot) getNumericRangeBucketCacheEntry(field string, base *[]index, bucketSize, minFieldKeys int) *numericRangeBucketCacheEntry {
	if s == nil || base == nil || len(*base) == 0 {
		return nil
	}
	cache := s.numericRangeBucketCache
	if cache == nil {
		return &numericRangeBucketCacheEntry{
			base: base,
			idx:  buildNumericRangeBucketIndex(*base, bucketSize, minFieldKeys),
		}
	}

	if cached, ok := cache.Load(field); ok {
		if entry, ok := cached.(*numericRangeBucketCacheEntry); ok && entry != nil && entry.base == base {
			return entry
		}
	}

	entry := &numericRangeBucketCacheEntry{
		base: base,
		idx:  buildNumericRangeBucketIndex(*base, bucketSize, minFieldKeys),
	}
	if actual, loaded := cache.LoadOrStore(field, entry); loaded {
		if stored, ok := actual.(*numericRangeBucketCacheEntry); ok && stored != nil && stored.base == base {
			return stored
		}
		cache.Store(field, entry)
	}

	return entry
}

func (db *DB[K, V]) tryEvalNumericRangeBuckets(field string, fm *field, ov fieldOverlay, br overlayRange) (bitmap, bool) {
	if fm == nil || fm.Slice || !isNumericScalarKind(fm.Kind) {
		return bitmap{}, false
	}

	bucketSize := db.options.NumericRangeBucketSize
	minFieldKeys := db.options.NumericRangeBucketMinFieldKeys
	minSpan := db.options.NumericRangeBucketMinSpanKeys
	if bucketSize <= 0 || minFieldKeys <= 0 {
		return bitmap{}, false
	}
	if minSpan <= 0 {
		return bitmap{}, false
	}
	span := br.baseEnd - br.baseStart
	if span < minSpan {
		return bitmap{}, false
	}
	if br.baseStart >= br.baseEnd || len(ov.base) == 0 {
		return bitmap{}, false
	}

	snap := db.getSnapshot()
	basePtr := snap.fieldIndexSlice(field)
	if basePtr == nil || len(*basePtr) == 0 {
		return bitmap{}, false
	}

	entry := snap.getNumericRangeBucketCacheEntry(field, basePtr, bucketSize, minFieldKeys)
	if entry == nil || entry.idx == nil {
		return bitmap{}, false
	}
	idx := entry.idx
	if idx == nil || idx.bucketSize <= 0 || len(idx.buckets) == 0 {
		return bitmap{}, false
	}

	if idx.keyCount != len(*basePtr) {
		return bitmap{}, false
	}

	startFull, endFull, ok := idx.fullBucketSpan(br)
	if !ok {
		return bitmap{}, false
	}

	base := *basePtr
	fullCacheKey := ""
	if db.materializedPredCacheEnabled() {
		fullCacheKey = materializedPredCacheKeyForNumericBucketSpan(field, startFull, endFull)
	}

	var (
		res      *roaring64.Bitmap
		readonly bool
	)
	if cached, ok := entry.loadFullSpan(startFull, endFull); ok {
		res = cached
		readonly = true
	}
	if fullCacheKey != "" {
		if cached, hit := snap.loadMaterializedPred(fullCacheKey); res == nil && hit && cached != nil && !cached.IsEmpty() {
			res = cached
			readonly = true
		}
	}
	if res == nil {
		res = getRoaringBuf()
		idx.orFullBucketSpan(res, startFull, endFull)
		if cached, ok := entry.tryStoreFullSpan(startFull, endFull, res, snap.matPredCacheMaxBitmapCard); ok {
			if cached != res {
				releaseRoaringBuf(res)
			}
			res = cached
			readonly = true
		}
		if fullCacheKey != "" && db.tryShareMaterializedPred(fullCacheKey, res) {
			readonly = true
		}
	}

	leftEnd := min(br.baseEnd, idx.buckets[startFull].start)
	rightStart := max(br.baseStart, idx.buckets[endFull].end)
	if (leftEnd > br.baseStart) || (rightStart < br.baseEnd) || (ov.delta != nil && br.deltaStart < br.deltaEnd) {
		if readonly {
			out := getRoaringBuf()
			out.Or(res)
			res = out
			readonly = false
		}
		orBaseIndexRange(res, base, br.baseStart, leftEnd)
		orBaseIndexRange(res, base, rightStart, br.baseEnd)
	}

	if ov.delta != nil && br.deltaStart < br.deltaEnd {
		// Apply delta removals before additions so ID moves between in-range keys
		// preserve final membership.
		for i := br.deltaStart; i < br.deltaEnd; i++ {
			_, de, _ := ov.delta.orderedEntryAt(i)
			deltaEntryApplyDelToBitmap(res, de)
		}
		for i := br.deltaStart; i < br.deltaEnd; i++ {
			_, de, _ := ov.delta.orderedEntryAt(i)
			deltaEntryApplyAddToBitmap(res, de)
		}
	}

	return bitmap{bm: res, readonly: readonly}, true
}

func (db *DB[K, V]) tryCountSnapshotNumericRange(field string, fm *field, base *[]index, start, end int) (uint64, bool) {
	if fm == nil || fm.Slice || !isNumericScalarKind(fm.Kind) {
		return 0, false
	}
	if base == nil || start < 0 || start > end || end > len(*base) {
		return 0, false
	}
	snap := db.getSnapshot()
	if snap == nil {
		return 0, false
	}

	entry := snap.getNumericRangeBucketCacheEntry(field, base, db.options.NumericRangeBucketSize, db.options.NumericRangeBucketMinFieldKeys)
	if entry == nil || entry.idx == nil {
		return 0, false
	}
	return entry.idx.countBaseRange(*base, start, end)
}
