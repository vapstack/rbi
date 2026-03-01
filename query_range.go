package rbi

import (
	"reflect"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

type numericRangeBucket struct {
	start int
	end   int
	ids   *roaring64.Bitmap
}

type numericRangeBucketIndex struct {
	bucketSize int
	keyCount   int
	buckets    []numericRangeBucket
}

type numericRangeBucketCacheEntry struct {
	base *[]index
	idx  *numericRangeBucketIndex
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

		var ids *roaring64.Bitmap
		for i := start; i < end; i++ {
			src := base[i].IDs
			if src.IsEmpty() {
				continue
			}
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
		})
	}

	return out
}

func (s *indexSnapshot) getNumericRangeBucketIndex(field string, base *[]index, bucketSize, minFieldKeys int) *numericRangeBucketIndex {
	if s == nil || base == nil || len(*base) == 0 {
		return nil
	}
	cache := s.numericRangeBucketCache
	if cache == nil {
		return buildNumericRangeBucketIndex(*base, bucketSize, minFieldKeys)
	}

	if cached, ok := cache.Load(field); ok {
		if entry, ok := cached.(*numericRangeBucketCacheEntry); ok && entry != nil && entry.base == base {
			return entry.idx
		}
	}

	idx := buildNumericRangeBucketIndex(*base, bucketSize, minFieldKeys)
	entry := &numericRangeBucketCacheEntry{
		base: base,
		idx:  idx,
	}
	if actual, loaded := cache.LoadOrStore(field, entry); loaded {
		if stored, ok := actual.(*numericRangeBucketCacheEntry); ok && stored != nil && stored.base == base {
			return stored.idx
		}
		cache.Store(field, entry)
	}

	return idx
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

	idx := snap.getNumericRangeBucketIndex(field, basePtr, bucketSize, minFieldKeys)
	if idx == nil || idx.bucketSize <= 0 || len(idx.buckets) == 0 {
		return bitmap{}, false
	}

	if idx.keyCount != len(*basePtr) {
		return bitmap{}, false
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
		return bitmap{}, false
	}

	base := *basePtr
	res := getRoaringBuf()
	usedFullBucket := false

	for b := startBucket; b <= endBucket; b++ {
		cur := idx.buckets[b]
		lo := max(br.baseStart, cur.start)
		hi := min(br.baseEnd, cur.end)
		if lo >= hi {
			continue
		}

		if cur.start >= br.baseStart && cur.end <= br.baseEnd {
			usedFullBucket = true
			if cur.ids != nil && !cur.ids.IsEmpty() {
				orBitmapAdaptive(res, cur.ids)
			}
			continue
		}

		for i := lo; i < hi; i++ {
			bm := base[i].IDs
			if bm.IsEmpty() {
				continue
			}
			bm.OrInto(res)
		}
	}

	// If there were no full buckets in the range, regular key-scan path is
	// cheaper and avoids unnecessary bucket-index overhead.
	if !usedFullBucket {
		releaseRoaringBuf(res)
		return bitmap{}, false
	}

	if ov.delta != nil && br.deltaStart < br.deltaEnd {
		// Apply delta removals before additions so ID moves between in-range keys
		// preserve final membership.
		dkeys := ov.delta.sortedKeys()
		for i := br.deltaStart; i < br.deltaEnd; i++ {
			de, _ := ov.delta.get(dkeys[i])
			deltaEntryApplyDelToBitmap(res, de)
		}
		for i := br.deltaStart; i < br.deltaEnd; i++ {
			de, _ := ov.delta.get(dkeys[i])
			deltaEntryApplyAddToBitmap(res, de)
		}
	}

	return bitmap{bm: res}, true
}
