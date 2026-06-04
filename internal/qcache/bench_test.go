package qcache

import (
	"strconv"
	"testing"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/schema"
)

var (
	qcacheBenchKey    MaterializedPredKey
	qcacheBenchString string
	qcacheBenchBool   bool
	qcacheBenchInt    int
)

func qcacheBenchKeys(n int) []MaterializedPredKey {
	keys := make([]MaterializedPredKey, n)
	for i := range keys {
		keys[i] = MaterializedPredKeyForScalar("email", qir.OpPREFIX, "user"+strconv.Itoa(i))
	}
	return keys
}

func qcacheBenchBounds() indexdata.Bounds {
	return indexdata.Bounds{
		HasLo:     true,
		LoInc:     true,
		LoNumeric: true,
		LoIndex:   keycodec.FromU64(keycodec.OrderedInt64Key(100)),
		HasHi:     true,
		HiNumeric: true,
		HiIndex:   keycodec.FromU64(keycodec.OrderedInt64Key(500)),
	}
}

func BenchmarkMaterializedPredKeyStringScalar(b *testing.B) {
	key := MaterializedPredKeyForScalar("email", qir.OpPREFIX, "user-12345")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qcacheBenchString = key.String()
	}
}

func BenchmarkMaterializedPredKeyStringExactNumericRange(b *testing.B) {
	key := MaterializedPredKeyForExactScalarRange("age", qcacheBenchBounds())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qcacheBenchString = key.String()
	}
}

func BenchmarkMaterializedPredKeyConstructScalar(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		qcacheBenchKey = MaterializedPredKeyForScalar("email", qir.OpPREFIX, "user-12345")
	}
}

func BenchmarkMaterializedPredKeyConstructExactNumericRange(b *testing.B) {
	bounds := qcacheBenchBounds()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qcacheBenchKey = MaterializedPredKeyForExactScalarRange("age", bounds)
	}
}

func BenchmarkMaterializedPredKeyConstructDistinctSet(b *testing.B) {
	vals := []string{"alpha", "beta", "gamma", "delta"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qcacheBenchKey = MaterializedPredKeyForDistinctSetTerms("tags", qir.OpHASANY, vals, true)
	}
}

func BenchmarkMaterializedPredKeySlicePoolGetPut(b *testing.B) {
	keys := qcacheBenchKeys(16)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := GetMaterializedPredKeySlice(len(keys))
		buf = append(buf, keys...)
		qcacheBenchInt = len(buf)
		ReleaseMaterializedPredKeySlice(buf)
	}
}

func BenchmarkMaterializedPredCacheLoadHit(b *testing.B) {
	keys := qcacheBenchKeys(64)
	cache := GetMaterializedPredCache(len(keys), 0)
	defer cache.ReleaseRef()
	for i := range keys {
		cache.Store(keys[i], qcacheTestPosting(uint64(i+1), uint64(i+65)))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ids, ok := cache.Load(keys[i&63])
		if !ok || ids.IsEmpty() {
			b.Fatal("load miss")
		}
		qcacheBenchBool = ok
		ids.Release()
	}
}

func BenchmarkMaterializedPredCacheLoadMiss(b *testing.B) {
	keys := qcacheBenchKeys(64)
	cache := GetMaterializedPredCache(len(keys), 0)
	defer cache.ReleaseRef()
	for i := range keys {
		cache.Store(keys[i], posting.List{})
	}
	missing := MaterializedPredKeyForScalar("email", qir.OpPREFIX, "missing")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok := cache.Load(missing)
		qcacheBenchBool = ok
	}
}

func BenchmarkMaterializedPredCacheStoreNegativeFresh(b *testing.B) {
	keys := qcacheBenchKeys(2)
	cache := GetMaterializedPredCache(1, 0)
	defer cache.ReleaseRef()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Store(keys[i&1], posting.List{})
		cache.Clear()
	}
}

func BenchmarkMaterializedPredCacheLoadOrStoreHit(b *testing.B) {
	key := MaterializedPredKeyForScalar("email", qir.OpPREFIX, "user")
	cache := GetMaterializedPredCache(8, 0)
	defer cache.ReleaseRef()
	cache.Store(key, qcacheTestPosting(1, 2, 3))

	caller := qcacheTestPosting(4, 5, 6)
	defer caller.Release()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ids, ok := cache.LoadOrStore(key, caller.Borrow())
		if !ok || ids.IsEmpty() {
			b.Fatal("load-or-store miss")
		}
		ids.Release()
	}
}

func BenchmarkMaterializedPredCacheTryLoadOrStoreOversizedHit(b *testing.B) {
	key := MaterializedPredKeyForNumericBucketSpan("age", 3, 7)
	cache := GetMaterializedPredCache(8, 1)
	defer cache.ReleaseRef()
	caller := qcacheTestPosting(1, 2, 3)
	defer caller.Release()
	seed, ok := cache.TryLoadOrStoreOversized(key, caller.Borrow())
	if !ok || seed.IsEmpty() {
		b.Fatal("oversized seed miss")
	}
	seed.Release()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ids, ok := cache.TryLoadOrStoreOversized(key, caller.Borrow())
		if !ok || ids.IsEmpty() {
			b.Fatal("oversized hit miss")
		}
		ids.Release()
	}
}

func BenchmarkMaterializedPredCacheInheritFrom(b *testing.B) {
	keys := qcacheBenchKeys(32)
	prev := GetMaterializedPredCache(len(keys), 0)
	next := GetMaterializedPredCache(len(keys), 0)
	defer prev.ReleaseRef()
	defer next.ReleaseRef()
	fields := make(schema.IndexedFieldMap, len(keys))
	for i := range keys {
		prev.Store(keys[i], qcacheTestPosting(uint64(i+1)))
		fields[keys[i].Field()] = schema.IndexedFieldAccessor{Ordinal: i}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		next.Clear()
		next.InheritFrom(prev, fields, nil)
		qcacheBenchInt = next.EntryCount()
	}
}

func BenchmarkRecentKeyCacheTouchHot(b *testing.B) {
	keys := qcacheBenchKeys(32)
	var cache RecentKeyCache
	defer cache.Clear()
	for i := range keys {
		cache.TouchOrRemember(keys[i], len(keys))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qcacheBenchBool = cache.TouchOrRemember(keys[i&31], len(keys))
	}
}

func BenchmarkRecentKeyCacheAddWorkHot(b *testing.B) {
	keys := qcacheBenchKeys(32)
	var cache RecentKeyCache
	defer cache.Clear()
	for i := range keys {
		_, _ = cache.AddWorkAndShouldPromote(keys[i], len(keys), 1, ^uint64(0))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qcacheBenchBool, _ = cache.AddWorkAndShouldPromote(keys[i&31], len(keys), 1, ^uint64(0))
	}
}

func BenchmarkNumericRangeBucketIndexBuild(b *testing.B) {
	storage := qcacheTestFieldStorage(4096, 1000)
	defer storage.Release()
	ov := indexdata.NewFieldIndexViewFromStorage(storage)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx, ok := BuildNumericRangeBucketIndex(ov, 32, 1)
		if !ok {
			b.Fatal("index build rejected")
		}
		qcacheBenchInt = idx.BucketCount()
	}
}

func BenchmarkNumericRangeBucketIndexFullBucketSpan(b *testing.B) {
	storage := qcacheTestFieldStorage(4096, 1000)
	defer storage.Release()
	ov := indexdata.NewFieldIndexViewFromStorage(storage)
	idx, ok := BuildNumericRangeBucketIndex(ov, 32, 1)
	if !ok {
		b.Fatal("index build rejected")
	}
	br := ov.RangeByRanks(17, 3077)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start, end, ok := idx.FullBucketSpan(br)
		if !ok {
			b.Fatal("span miss")
		}
		qcacheBenchInt = start + end
	}
}

func BenchmarkNumericRangeBucketCacheLoadSlot(b *testing.B) {
	storage := qcacheTestFieldStorage(256, 1000)
	defer storage.Release()
	cache := GetNumericRangeBucketCache(64, 0)
	defer ReleaseNumericRangeBucketCache(cache)
	idx := NumericRangeBucketIndex{bucketSize: 16, keyCount: storage.KeyCount()}
	for i := 0; i < 64; i++ {
		cache.StoreSlot("field"+strconv.Itoa(i), i, GetNumericRangeBucketEntry(storage, idx, 0))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry, ok := cache.LoadSlot("field63", 63)
		if !ok {
			b.Fatal("slot miss")
		}
		qcacheBenchInt = entry.idx.keyCount
	}
}

func BenchmarkNumericRangeBucketCacheLoadField(b *testing.B) {
	storage := qcacheTestFieldStorage(256, 1000)
	defer storage.Release()
	cache := GetNumericRangeBucketCache(64, 0)
	defer ReleaseNumericRangeBucketCache(cache)
	idx := NumericRangeBucketIndex{bucketSize: 16, keyCount: storage.KeyCount()}
	for i := 0; i < 64; i++ {
		cache.StoreSlot("field"+strconv.Itoa(i), i, GetNumericRangeBucketEntry(storage, idx, 0))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry, ok := cache.LoadField("field63")
		if !ok {
			b.Fatal("field miss")
		}
		qcacheBenchInt = entry.idx.keyCount
	}
}

func BenchmarkNumericRangeBucketCacheStoreSlot(b *testing.B) {
	storage := qcacheTestFieldStorage(256, 1000)
	defer storage.Release()
	cache := GetNumericRangeBucketCache(64, 0)
	defer ReleaseNumericRangeBucketCache(cache)
	entry := GetNumericRangeBucketEntry(storage, NumericRangeBucketIndex{bucketSize: 16, keyCount: storage.KeyCount()}, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.StoreSlot("field63", 63, entry)
	}
}

func BenchmarkNumericRangeBucketCacheEntryCount(b *testing.B) {
	storage := qcacheTestFieldStorage(256, 1000)
	defer storage.Release()
	cache := GetNumericRangeBucketCache(64, 0)
	defer ReleaseNumericRangeBucketCache(cache)
	idx := NumericRangeBucketIndex{bucketSize: 16, keyCount: storage.KeyCount()}
	for i := 0; i < 64; i++ {
		cache.StoreSlot("field"+strconv.Itoa(i), i, GetNumericRangeBucketEntry(storage, idx, 0))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qcacheBenchInt = cache.EntryCount()
	}
}

func BenchmarkNumericRangeBucketCacheInheritFrom(b *testing.B) {
	storage := qcacheTestFieldStorage(256, 1000)
	defer storage.Release()
	prev := GetNumericRangeBucketCache(32, 0)
	next := GetNumericRangeBucketCache(32, 0)
	defer ReleaseNumericRangeBucketCache(prev)
	defer ReleaseNumericRangeBucketCache(next)

	idx := NumericRangeBucketIndex{bucketSize: 16, keyCount: storage.KeyCount()}
	nextIndex := make([]indexdata.FieldStorage, 32)
	fields := make(schema.IndexedFieldMap, 32)
	for i := 0; i < 32; i++ {
		field := "field" + strconv.Itoa(i)
		prev.StoreSlot(field, i, GetNumericRangeBucketEntry(storage, idx, 0))
		nextIndex[i] = storage
		fields[field] = schema.IndexedFieldAccessor{Ordinal: i}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		next.ClearEntries()
		next.InheritFrom(prev, nextIndex, fields)
		qcacheBenchInt = next.EntryCount()
	}
}

func BenchmarkNumericRangeBucketEntryLoadFullSpanHit(b *testing.B) {
	entry := GetNumericRangeBucketEntry(indexdata.FieldStorage{}, NumericRangeBucketIndex{}, 0)
	defer entry.Release()
	seed, ok := entry.TryStoreFullSpan(3, 7, qcacheTestPosting(1, 2, 3, 4))
	if !ok {
		b.Fatal("full-span seed miss")
	}
	seed.Release()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ids, ok := entry.LoadFullSpan(3, 7)
		if !ok || ids.IsEmpty() {
			b.Fatal("full-span load miss")
		}
		ids.Release()
	}
}

func BenchmarkNumericRangeBucketEntryLoadExtendedFullSpan(b *testing.B) {
	entry := GetNumericRangeBucketEntry(indexdata.FieldStorage{}, NumericRangeBucketIndex{}, 0)
	defer entry.Release()
	for i := 0; i < 4; i++ {
		ids, ok := entry.TryStoreFullSpan(i*4, i*4+3, qcacheTestPosting(uint64(i+1), uint64(i+17)))
		if !ok {
			b.Fatal("full-span seed miss")
		}
		ids.Release()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ids, _, _, ok := entry.LoadExtendedFullSpan(0, 19)
		if !ok || ids.IsEmpty() {
			b.Fatal("extended load miss")
		}
		ids.Release()
	}
}

func BenchmarkNumericRangeBucketEntryTryStoreFullSpanHit(b *testing.B) {
	entry := GetNumericRangeBucketEntry(indexdata.FieldStorage{}, NumericRangeBucketIndex{}, 0)
	defer entry.Release()
	caller := qcacheTestPosting(1, 2, 3, 4)
	defer caller.Release()
	seed, ok := entry.TryStoreFullSpan(3, 7, caller.Borrow())
	if !ok {
		b.Fatal("full-span seed miss")
	}
	seed.Release()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ids, ok := entry.TryStoreFullSpan(3, 7, caller.Borrow())
		if !ok || ids.IsEmpty() {
			b.Fatal("full-span store hit miss")
		}
		ids.Release()
	}
}
