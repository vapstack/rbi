package qcache

import (
	"fmt"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/schema"
)

func qcacheTestFieldStorage(n int, base uint64) indexdata.FieldStorage {
	m := make(map[uint64]posting.List, n)
	for i := 0; i < n; i++ {
		m[base+uint64(i)] = qcacheTestPosting(uint64(i + 1))
	}
	return indexdata.NewRegularFieldStorageFromFixedPostingMapOwned(m)
}

func TestNumericRangeBucketCache_SlotLifecycleAndClear(t *testing.T) {
	storage := qcacheTestFieldStorage(32, 100)
	defer storage.Release()

	cache := GetNumericRangeBucketCache(3, 99)
	defer ReleaseNumericRangeBucketCache(cache)
	if got := cache.MaxCardinality(); got != 99 {
		t.Fatalf("MaxCardinality=%d want 99", got)
	}

	entry := GetNumericRangeBucketEntry(storage, NumericRangeBucketIndex{bucketSize: 16, keyCount: storage.KeyCount()}, 0)
	cache.StoreSlot("age", 1, entry)

	if got := cache.EntryCount(); got != 1 {
		t.Fatalf("EntryCount=%d want 1", got)
	}
	if got, ok := cache.LoadSlot("age", 1); !ok || got != entry {
		t.Fatal("expected ordinal load to return stored entry")
	}
	if got, ok := cache.LoadField("age"); !ok || got != entry {
		t.Fatal("expected field load to return stored entry")
	}
	if _, ok := cache.LoadSlot("score", 1); ok {
		t.Fatal("expected mismatched field load to miss")
	}

	cache.ClearEntries()
	if got := cache.EntryCount(); got != 0 {
		t.Fatalf("EntryCount after clear=%d want 0", got)
	}
	if _, ok := cache.LoadField("age"); ok {
		t.Fatal("expected field load to miss after clear")
	}
}

func TestNumericRangeBucketCacheReleaseClearsSlotsIndexAndEntry(t *testing.T) {
	storage := qcacheTestFieldStorage(32, 100)
	defer storage.Release()

	cache := GetNumericRangeBucketCache(2, 99)
	entry := GetNumericRangeBucketEntry(storage, NumericRangeBucketIndex{bucketSize: 16, keyCount: storage.KeyCount()}, 0)
	cached, ok := entry.TryStoreFullSpan(0, 1, qcacheTestPosting(1, 2, 3))
	if !ok {
		ReleaseNumericRangeBucketCache(cache)
		t.Fatal("expected full-span store to succeed")
	}
	cached.Release()

	cache.StoreSlot("age", 0, entry)
	if got, ok := cache.LoadField("age"); !ok || got != entry {
		ReleaseNumericRangeBucketCache(cache)
		t.Fatal("expected field index load to return stored entry")
	}

	slots := cache.slots
	fieldIndex := cache.fieldIndex
	ReleaseNumericRangeBucketCache(cache)

	if len(cache.slots) != 0 ||
		cache.count != 0 ||
		cache.maxCard != 0 ||
		cache.fieldIndex != nil ||
		cache.fieldIndexLen != 0 {
		t.Fatalf("released numeric range cache retained state: %+v", cache)
	}
	if slots[0].field != "" || slots[0].entry != nil {
		t.Fatalf("released numeric range cache slot retained entry: %+v", slots[0])
	}
	if len(fieldIndex) != 0 {
		t.Fatalf("released numeric range cache field index retained entries: %+v", fieldIndex)
	}
	if entry.refs.Load() != 0 ||
		entry.storage.KeyCount() != 0 ||
		entry.idx.keyCount != 0 ||
		entry.maxCard != 0 ||
		entry.fullSpanClock != 0 ||
		entry.retired != nil {
		t.Fatalf("released numeric range entry retained state: %+v", entry)
	}
	for i := range entry.fullSpanCache {
		if entry.fullSpanCache[i].used || !entry.fullSpanCache[i].ids.IsEmpty() {
			t.Fatalf("released numeric full-span slot %d retained state: %+v", i, entry.fullSpanCache[i])
		}
	}
}

func TestNumericRangeBucketCacheInitClearsSlotsFieldIndexAndEntries(t *testing.T) {
	storage := qcacheTestFieldStorage(32, 100)
	defer storage.Release()

	cache := GetNumericRangeBucketCache(2, 99)
	defer ReleaseNumericRangeBucketCache(cache)

	idx := NumericRangeBucketIndex{bucketSize: 16, keyCount: storage.KeyCount()}
	ageEntry := GetNumericRangeBucketEntry(storage, idx, 0)
	scoreEntry := GetNumericRangeBucketEntry(storage, idx, 0)
	cache.StoreSlot("age", 0, ageEntry)
	cache.StoreSlot("score", 1, scoreEntry)
	if _, ok := cache.LoadField("age"); !ok {
		t.Fatal("expected field index to be built")
	}
	fieldIndex := cache.fieldIndex

	cache.Init(2, 7)
	if ageEntry.refs.Load() != 0 || scoreEntry.refs.Load() != 0 {
		t.Fatalf("expected init to release old entries: age=%d score=%d", ageEntry.refs.Load(), scoreEntry.refs.Load())
	}
	if got := cache.EntryCount(); got != 0 {
		t.Fatalf("EntryCount after init=%d want 0", got)
	}
	if got := cache.MaxCardinality(); got != 7 {
		t.Fatalf("MaxCardinality after init=%d want 7", got)
	}
	if cache.fieldIndexLen != 0 {
		t.Fatalf("fieldIndexLen after init=%d want 0", cache.fieldIndexLen)
	}
	if fieldIndex != nil && len(fieldIndex) != 0 {
		t.Fatalf("expected init to clear field index, got %d entries", len(fieldIndex))
	}
	if _, ok := cache.LoadSlot("age", 0); ok {
		t.Fatal("expected old ordinal slot to miss after init")
	}
	if _, ok := cache.LoadField("score"); ok {
		t.Fatal("expected old field index entry to miss after init")
	}

	heightEntry := GetNumericRangeBucketEntry(storage, idx, 0)
	cache.StoreSlot("height", 1, heightEntry)
	if _, ok := cache.LoadField("height"); !ok {
		t.Fatal("expected fresh field index entry")
	}

	cache.Init(4, 8)
	if heightEntry.refs.Load() != 0 {
		t.Fatalf("expected growing init to release old entry, refs=%d", heightEntry.refs.Load())
	}
	if got := cache.EntryCount(); got != 0 {
		t.Fatalf("EntryCount after growing init=%d want 0", got)
	}
	if got := cache.MaxCardinality(); got != 8 {
		t.Fatalf("MaxCardinality after growing init=%d want 8", got)
	}
	if _, ok := cache.LoadSlot("height", 1); ok {
		t.Fatal("expected fresh slot to miss after growing init")
	}
}

func TestNumericRangeBucketCacheStoreSlotReleasesReplacedEntry(t *testing.T) {
	storage := qcacheTestFieldStorage(32, 100)
	defer storage.Release()

	cache := GetNumericRangeBucketCache(1, 0)
	defer ReleaseNumericRangeBucketCache(cache)

	oldEntry := GetNumericRangeBucketEntry(storage, NumericRangeBucketIndex{bucketSize: 16, keyCount: storage.KeyCount()}, 0)
	newEntry := GetNumericRangeBucketEntry(storage, NumericRangeBucketIndex{bucketSize: 16, keyCount: storage.KeyCount()}, 0)

	cache.StoreSlot("age", 0, oldEntry)
	if _, ok := cache.LoadField("age"); !ok {
		t.Fatal("expected field index to be built for old entry")
	}
	cache.StoreSlot("age", 0, newEntry)

	if oldEntry.refs.Load() != 0 {
		t.Fatalf("expected replaced numeric range entry to be released, refs=%d", oldEntry.refs.Load())
	}
	if got := cache.EntryCount(); got != 1 {
		t.Fatalf("EntryCount after replacement=%d want 1", got)
	}
	if got, ok := cache.LoadSlot("age", 0); !ok || got != newEntry {
		t.Fatal("expected ordinal load to return replacement entry")
	}
	if got, ok := cache.LoadField("age"); !ok || got != newEntry {
		t.Fatal("expected field index to return replacement entry")
	}

	cache.StoreSlot("age", 0, nil)
	if newEntry.refs.Load() != 0 {
		t.Fatalf("expected cleared numeric range entry to be released, refs=%d", newEntry.refs.Load())
	}
	if got := cache.EntryCount(); got != 0 {
		t.Fatalf("EntryCount after clear=%d want 0", got)
	}
}

func TestNumericRangeBucketCacheLoadOrStoreSlotConcurrentSameStorage(t *testing.T) {
	storage := qcacheTestFieldStorage(32, 100)
	defer storage.Release()

	cache := GetNumericRangeBucketCache(1, 0)
	defer ReleaseNumericRangeBucketCache(cache)

	n := max(4, runtime.GOMAXPROCS(0)*2)
	idx := NumericRangeBucketIndex{bucketSize: 16, keyCount: storage.KeyCount()}
	candidates := make([]*NumericRangeBucketEntry, n)
	got := make([]*NumericRangeBucketEntry, n)
	for i := range candidates {
		candidates[i] = GetNumericRangeBucketEntry(storage, idx, 0)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range candidates {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			got[i] = cache.LoadOrStoreSlot("age", 0, candidates[i])
		}(i)
	}
	close(start)
	wg.Wait()

	winner := got[0]
	for i := range got {
		if got[i] != winner {
			t.Fatalf("concurrent numeric slot warm returned different entries: got[%d]=%p winner=%p", i, got[i], winner)
		}
	}
	if got, ok := cache.LoadSlot("age", 0); !ok || got != winner {
		t.Fatal("expected published numeric slot to contain the shared winner")
	}
	if winner.refs.Load() != 1 {
		t.Fatalf("expected published numeric entry to keep one cache ref, got %d", winner.refs.Load())
	}
	for i := range candidates {
		if candidates[i] != winner && candidates[i].refs.Load() != 0 {
			t.Fatalf("expected losing candidate %d to be released, refs=%d", i, candidates[i].refs.Load())
		}
	}
}

func TestNumericRangeBucketCacheDrainRetiredSkipsSharedEntries(t *testing.T) {
	cache := GetNumericRangeBucketCache(2, 0)
	defer ReleaseNumericRangeBucketCache(cache)

	unshared := GetNumericRangeBucketEntry(indexdata.FieldStorage{}, NumericRangeBucketIndex{}, 0)
	shared := GetNumericRangeBucketEntry(indexdata.FieldStorage{}, NumericRangeBucketIndex{}, 0)
	shared.Retain()
	defer shared.Release()

	for i := 0; i <= numericRangeFullSpanCacheMaxEntries; i++ {
		ids := qcacheTestPosting(uint64(i+1), 1<<32|uint64(i+1))
		cached, ok := unshared.TryStoreFullSpan(i, i, ids)
		if !ok {
			t.Fatalf("expected unshared full-span store %d to succeed", i)
		}
		cached.Release()

		ids = qcacheTestPosting(uint64(i+11), 1<<32|uint64(i+11))
		cached, ok = shared.TryStoreFullSpan(i, i, ids)
		if !ok {
			t.Fatalf("expected shared full-span store %d to succeed", i)
		}
		cached.Release()
	}
	if len(unshared.retired) == 0 || len(shared.retired) == 0 {
		t.Fatal("expected both numeric entries to have retired full-span postings")
	}

	cache.StoreSlot("age", 0, unshared)
	cache.StoreSlot("score", 1, shared)
	retired := cache.TakeRetired()
	if retired.IsEmpty() {
		t.Fatal("expected unshared numeric retired postings to be detached")
	}
	retired.Release()

	if unshared.retired != nil {
		t.Fatal("expected unshared numeric retired postings to be drained")
	}
	if len(shared.retired) == 0 {
		t.Fatal("expected shared numeric retired postings to remain protected")
	}
}

func TestNumericRangeBucketCache_InheritRetainsMatchingStorage(t *testing.T) {
	shared := qcacheTestFieldStorage(32, 100)
	changed := qcacheTestFieldStorage(32, 200)
	defer shared.Release()
	defer changed.Release()

	prev := GetNumericRangeBucketCache(2, 0)
	next := GetNumericRangeBucketCache(2, 0)
	defer ReleaseNumericRangeBucketCache(next)

	idx := NumericRangeBucketIndex{bucketSize: 16, keyCount: shared.KeyCount()}
	ageEntry := GetNumericRangeBucketEntry(shared, idx, 0)
	stored, ok := ageEntry.TryStoreFullSpan(0, 0, qcacheTestPosting(1, 2, 3))
	if !ok {
		ReleaseNumericRangeBucketCache(prev)
		t.Fatal("expected full-span store to succeed")
	}
	stored.Release()
	prev.StoreSlot("age", 0, ageEntry)

	scoreEntry := GetNumericRangeBucketEntry(changed, idx, 0)
	prev.StoreSlot("score", 1, scoreEntry)

	fields := schema.IndexedFieldMap{
		"age":   {Ordinal: 0},
		"score": {Ordinal: 1},
	}
	next.InheritFrom(prev, []indexdata.FieldStorage{shared, shared}, fields)
	ReleaseNumericRangeBucketCache(prev)

	inherited, ok := next.LoadField("age")
	if !ok || inherited != ageEntry {
		t.Fatal("expected matching storage entry to be inherited")
	}
	if _, ok := next.LoadField("score"); ok {
		t.Fatal("expected changed storage entry to be skipped")
	}
	cached, ok := inherited.LoadFullSpan(0, 0)
	if !ok {
		t.Fatal("expected inherited retained entry to survive previous cache release")
	}
	qcacheTestAssertPostingSet(t, cached, []uint64{1, 2, 3})
	cached.Release()
}

func TestNumericRangeBucketEntryRetainedBorrowedViewSurvivesSourceRelease(t *testing.T) {
	entry := GetNumericRangeBucketEntry(indexdata.FieldStorage{}, NumericRangeBucketIndex{}, 0)

	base := qcacheTestLargePosting()
	want := base.ToArray()
	stored, ok := entry.TryStoreFullSpan(0, 0, base.Borrow())
	if !ok || stored.IsEmpty() {
		base.Release()
		entry.Release()
		t.Fatal("expected full-span cache store to succeed")
	}
	stored.Release()
	base.Release()

	retained := entry
	retained.Retain()
	defer retained.Release()

	held, ok := retained.LoadFullSpan(0, 0)
	if !ok || held.IsEmpty() {
		entry.Release()
		t.Fatal("expected retained full-span posting")
	}
	defer held.Release()

	var failed atomic.Pointer[string]
	setFailed := func(msg string) {
		if failed.Load() != nil {
			return
		}
		copyMsg := msg
		failed.CompareAndSwap(nil, &copyMsg)
	}

	readerN := max(4, runtime.GOMAXPROCS(0))
	start := make(chan struct{})
	ready := make(chan struct{}, readerN)
	var wg sync.WaitGroup
	for i := 0; i < readerN; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ready <- struct{}{}
			for i := 0; i < 1000; i++ {
				if got := held.ToArray(); !slices.Equal(got, want) {
					setFailed(fmt.Sprintf("held full-span posting changed while source entry was released: got=%v want=%v", got, want))
					return
				}
			}
		}()
	}

	close(start)
	for i := 0; i < readerN; i++ {
		<-ready
	}
	entry.Release()
	wg.Wait()

	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}
	qcacheTestAssertPostingSet(t, held, want)
}

func TestNumericRangeBucketIndex_BuildAndFullBucketSpan(t *testing.T) {
	storage := qcacheTestFieldStorage(100, 1000)
	defer storage.Release()
	ov := indexdata.NewFieldIndexViewFromStorage(storage)

	idx, ok := BuildNumericRangeBucketIndex(ov, 20, 1)
	if !ok {
		t.Fatal("expected numeric range bucket index to build")
	}
	if idx.KeyCount() != 100 || idx.BucketSize() != 20 || idx.BucketCount() != 5 {
		t.Fatalf("unexpected index shape: keyCount=%d bucketSize=%d bucketCount=%d", idx.KeyCount(), idx.BucketSize(), idx.BucketCount())
	}
	if idx.BucketStart(3) != 60 || idx.BucketEnd(4) != 100 {
		t.Fatalf("unexpected bucket bounds")
	}

	start, end, ok := idx.FullBucketSpan(ov.RangeByRanks(5, 75))
	if !ok || start != 1 || end != 2 {
		t.Fatalf("FullBucketSpan(5,75) got %d..%d ok=%v, want 1..2 true", start, end, ok)
	}
	start, end, ok = idx.FullBucketSpan(ov.RangeByRanks(0, 100))
	if !ok || start != 0 || end != 4 {
		t.Fatalf("FullBucketSpan(0,100) got %d..%d ok=%v, want 0..4 true", start, end, ok)
	}
	if _, _, ok := idx.FullBucketSpan(ov.RangeByRanks(3, 17)); ok {
		t.Fatal("expected no full bucket inside narrow partial range")
	}
	if _, ok := BuildNumericRangeBucketIndex(ov, 20, 200); ok {
		t.Fatal("expected minFieldKeys guard to reject index")
	}
}

func TestNumericRangeBucketSpanCache_LoadExtendedFullSpan(t *testing.T) {
	entry := GetNumericRangeBucketEntry(indexdata.FieldStorage{}, NumericRangeBucketIndex{}, 0)
	defer entry.Release()

	base := qcacheTestPosting(10, 20, 30, 40)
	if _, ok := entry.TryStoreFullSpan(4, 10, base); !ok {
		base.Release()
		t.Fatal("expected first full span store to succeed")
	}

	cachedSuffix, startSuffix, endSuffix, ok := entry.LoadExtendedFullSpan(2, 10)
	if !ok {
		t.Fatal("expected suffix extension hit")
	}
	if startSuffix != 4 || endSuffix != 10 {
		t.Fatalf("unexpected suffix extension bounds: got=%d..%d want=4..10", startSuffix, endSuffix)
	}
	cachedSuffix.Release()

	cachedPrefix, startPrefix, endPrefix, ok := entry.LoadExtendedFullSpan(4, 12)
	if !ok {
		t.Fatal("expected prefix extension hit")
	}
	if startPrefix != 4 || endPrefix != 10 {
		t.Fatalf("unexpected prefix extension bounds: got=%d..%d want=4..10", startPrefix, endPrefix)
	}
	cachedPrefix.Release()
}

func TestNumericRangeFullSpanStoreBorrowedDetachesFromSourceOwner(t *testing.T) {
	entry := GetNumericRangeBucketEntry(indexdata.FieldStorage{}, NumericRangeBucketIndex{}, 0)
	defer entry.Release()

	base := qcacheTestLargePosting()
	want := base.ToArray()

	stored, ok := entry.TryStoreFullSpan(3, 7, base.Borrow())
	if !ok || stored.IsEmpty() {
		base.Release()
		t.Fatal("expected numeric full-span cache store to succeed")
	}
	if stored.SharesPayload(base) {
		stored.Release()
		base.Release()
		t.Fatal("borrowed full-span store kept source payload")
	}
	stored.Release()

	extra := uint64(1<<32 | 91)
	if base.Contains(extra) {
		base.Release()
		t.Fatalf("test setup chose existing id %d", extra)
	}
	base = base.BuildAdded(extra)

	cached, ok := entry.LoadFullSpan(3, 7)
	if !ok || cached.IsEmpty() {
		base.Release()
		t.Fatal("expected cached numeric full-span posting after source mutation")
	}
	if !slices.Equal(cached.ToArray(), want) {
		cached.Release()
		base.Release()
		t.Fatalf("cached full-span posting changed after source mutation: got=%v want=%v", cached.ToArray(), want)
	}
	if cached.Contains(extra) {
		cached.Release()
		base.Release()
		t.Fatalf("numeric full-span cache leaked source mutation id=%d", extra)
	}

	cached.Release()
	base.Release()
}

func TestNumericRangeFullSpanRepeatedStoreSameKeyReturnsCachedPayloadNotCaller(t *testing.T) {
	entry := GetNumericRangeBucketEntry(indexdata.FieldStorage{}, NumericRangeBucketIndex{}, 0)
	defer entry.Release()

	cachedBase := qcacheTestLargePosting()
	want := cachedBase.ToArray()

	stored, ok := entry.TryStoreFullSpan(3, 7, cachedBase.Borrow())
	if !ok || stored.IsEmpty() {
		cachedBase.Release()
		t.Fatal("expected initial numeric full-span store to succeed")
	}
	stored.Release()

	cached, ok := entry.LoadFullSpan(3, 7)
	if !ok || cached.IsEmpty() {
		cachedBase.Release()
		t.Fatal("expected seeded cached numeric full-span posting")
	}
	defer cached.Release()

	source := qcacheTestLargePosting()
	extraSource := uint64(7<<32 | 21)
	if source.Contains(extraSource) {
		source.Release()
		cachedBase.Release()
		t.Fatalf("test setup chose existing id %d", extraSource)
	}
	source = source.BuildAdded(extraSource)

	got, ok := entry.TryStoreFullSpan(3, 7, source.Borrow())
	if !ok || got.IsEmpty() {
		source.Release()
		cachedBase.Release()
		t.Fatal("expected same-key numeric full-span store to return cached posting")
	}
	defer got.Release()

	if !got.SharesPayload(cached) {
		source.Release()
		cachedBase.Release()
		t.Fatal("same-key numeric full-span store did not reuse cached payload")
	}
	if got.SharesPayload(source) {
		source.Release()
		cachedBase.Release()
		t.Fatal("same-key numeric full-span store reused caller payload instead of cached payload")
	}

	extraMut := uint64(9<<32 | 25)
	if source.Contains(extraMut) {
		source.Release()
		cachedBase.Release()
		t.Fatalf("test setup chose existing id %d", extraMut)
	}
	source = source.BuildAdded(extraMut)

	reloaded, ok := entry.LoadFullSpan(3, 7)
	if !ok || reloaded.IsEmpty() {
		source.Release()
		cachedBase.Release()
		t.Fatal("expected reloaded numeric full-span cache after same-key store")
	}
	defer reloaded.Release()

	if !slices.Equal(got.ToArray(), want) {
		source.Release()
		cachedBase.Release()
		t.Fatalf("same-key numeric full-span result changed after caller mutation: got=%v want=%v", got.ToArray(), want)
	}
	if !slices.Equal(reloaded.ToArray(), want) {
		source.Release()
		cachedBase.Release()
		t.Fatalf("numeric full-span cache changed after caller mutation: got=%v want=%v", reloaded.ToArray(), want)
	}
	if reloaded.Contains(extraSource) || reloaded.Contains(extraMut) {
		source.Release()
		cachedBase.Release()
		t.Fatalf("numeric full-span cache leaked caller ids: source=%d mut=%d", extraSource, extraMut)
	}

	source.Release()
	cachedBase.Release()
}

func TestNumericRangeBucketSpanCacheDetachedLoadsUnderConcurrency(t *testing.T) {
	entry := GetNumericRangeBucketEntry(indexdata.FieldStorage{}, NumericRangeBucketIndex{}, 0)
	defer entry.Release()

	base := qcacheTestPosting()
	for i := uint64(1); i <= 48; i++ {
		base = base.BuildAdded(i * 5)
	}
	want := base.ToArray()

	stored := base.Borrow()
	var ok bool
	stored, ok = entry.TryStoreFullSpan(11, 23, stored)
	if !ok {
		t.Fatal("expected initial full-span store to succeed")
	}
	stored.Release()
	base.Release()

	remove := qcacheTestPosting(want[0], want[1], want[2])
	add := qcacheTestPosting(1<<32|3, 1<<32|7)
	defer remove.Release()
	defer add.Release()

	var failed atomic.Pointer[string]
	setFailed := func(msg string) {
		if failed.Load() != nil {
			return
		}
		copyMsg := msg
		failed.CompareAndSwap(nil, &copyMsg)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup

	readerN := max(6, runtime.GOMAXPROCS(0))
	for g := 0; g < readerN; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 1000; i++ {
				cached, ok := entry.LoadFullSpan(11, 23)
				if !ok {
					setFailed("full-span cache entry unexpectedly missing")
					return
				}
				if !slices.Equal(cached.ToArray(), want) {
					setFailed(fmt.Sprintf("cached full-span mismatch: got=%v want=%v", cached.ToArray(), want))
					cached.Release()
					return
				}
				cached.Release()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 300; i++ {
			cached, ok := entry.LoadFullSpan(11, 23)
			if !ok {
				setFailed("writer unexpectedly missed full-span cache entry")
				return
			}
			cached = cached.BuildAndNot(remove)
			cached = cached.BuildOr(add)
			cached = cached.BuildAdded(6<<32 | uint64(i))
			cached = cached.BuildOptimized()
			cached.Release()
		}
	}()

	close(start)
	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}

	cached, ok := entry.LoadFullSpan(11, 23)
	if !ok {
		t.Fatal("full-span cache entry missing after concurrent mutations")
	}
	defer cached.Release()
	qcacheTestAssertPostingSet(t, cached, want)
}

func TestNumericRangeFullSpanBorrowedViewSurvivesConcurrentEviction(t *testing.T) {
	entry := GetNumericRangeBucketEntry(indexdata.FieldStorage{}, NumericRangeBucketIndex{}, 0)
	defer entry.Release()

	base := qcacheTestPosting()
	for i := uint64(1); i <= 48; i++ {
		base = base.BuildAdded(i * 9)
	}
	want := base.ToArray()

	stored := base.Borrow()
	stored, ok := entry.TryStoreFullSpan(0, 0, stored)
	if !ok {
		t.Fatal("expected initial full-span store to succeed")
	}
	stored.Release()
	base.Release()

	held, ok := entry.LoadFullSpan(0, 0)
	if !ok || held.IsEmpty() {
		t.Fatal("expected held full-span cache entry")
	}
	defer held.Release()

	var failed atomic.Pointer[string]
	setFailed := func(msg string) {
		if failed.Load() != nil {
			return
		}
		copyMsg := msg
		failed.CompareAndSwap(nil, &copyMsg)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup

	readerN := max(6, runtime.GOMAXPROCS(0))
	for g := 0; g < readerN; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 1000; i++ {
				if !slices.Equal(held.ToArray(), want) {
					setFailed(fmt.Sprintf("held full-span posting changed under eviction: got=%v want=%v", held.ToArray(), want))
					return
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 1; i <= 256; i++ {
			ids := qcacheTestPosting(uint64(i), 1<<32|uint64(i))
			var ok bool
			ids, ok = entry.TryStoreFullSpan(i, i, ids)
			if !ok {
				setFailed(fmt.Sprintf("tryStoreFullSpan(%d,%d) failed", i, i))
				return
			}
			ids.Release()
		}
	}()

	close(start)
	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}

	qcacheTestAssertPostingSet(t, held, want)
}
