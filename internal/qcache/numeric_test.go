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

func qcacheTestNumericSpanCache(t testing.TB) (*NumericRangeBucketCache, *NumericRangeBucketEntry) {
	t.Helper()
	cache := GetNumericRangeBucketCache(1)
	entry := GetNumericRangeBucketEntry(indexdata.FieldStorage{}, NumericRangeBucketIndex{})
	cache.StoreSlot("age", 0, entry)
	t.Cleanup(func() {
		ReleaseNumericRangeBucketCache(cache)
	})
	return cache, entry
}

func TestNumericRangeBucketCache_SlotLifecycleAndClear(t *testing.T) {
	storage := qcacheTestFieldStorage(32, 100)
	defer storage.Release()

	cache := GetNumericRangeBucketCache(3)
	defer ReleaseNumericRangeBucketCache(cache)

	entry := GetNumericRangeBucketEntry(storage, NumericRangeBucketIndex{bucketSize: 16, keyCount: storage.KeyCount()})
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

	cache := GetNumericRangeBucketCache(2)
	entry := GetNumericRangeBucketEntry(storage, NumericRangeBucketIndex{bucketSize: 16, keyCount: storage.KeyCount()})
	cache.StoreSlot("age", 0, entry)
	cached, ok := cache.TryStoreFullSpan(entry, 0, 0, 1, qcacheTestPosting(1, 2, 3))
	if !ok {
		ReleaseNumericRangeBucketCache(cache)
		t.Fatal("expected full-span store to succeed")
	}
	cached.Release()

	if got, ok := cache.LoadField("age"); !ok || got != entry {
		ReleaseNumericRangeBucketCache(cache)
		t.Fatal("expected field index load to return stored entry")
	}

	slots := cache.slots
	fieldIndex := cache.fieldIndex
	ReleaseNumericRangeBucketCache(cache)

	if len(cache.slots) != 0 ||
		cache.count != 0 ||
		cache.fieldIndex != nil ||
		cache.fieldIndexLen != 0 ||
		cache.spanCount != 0 ||
		cache.spanLiveBytes != 0 {
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
		entry.fullSpanClock != 0 {
		t.Fatalf("released numeric range entry retained state: %+v", entry)
	}
	for i := range entry.fullSpanRecent {
		if entry.fullSpanRecent[i].used {
			t.Fatalf("released numeric full-span recent slot %d retained state: %+v", i, entry.fullSpanRecent[i])
		}
	}
}

func TestNumericRangeBucketCacheInitClearsSlotsFieldIndexAndEntries(t *testing.T) {
	storage := qcacheTestFieldStorage(32, 100)
	defer storage.Release()

	cache := GetNumericRangeBucketCache(2)
	defer ReleaseNumericRangeBucketCache(cache)

	idx := NumericRangeBucketIndex{bucketSize: 16, keyCount: storage.KeyCount()}
	ageEntry := GetNumericRangeBucketEntry(storage, idx)
	scoreEntry := GetNumericRangeBucketEntry(storage, idx)
	cache.StoreSlot("age", 0, ageEntry)
	cache.StoreSlot("score", 1, scoreEntry)
	if _, ok := cache.LoadField("age"); !ok {
		t.Fatal("expected field index to be built")
	}
	fieldIndex := cache.fieldIndex

	cache.Init(2)
	if ageEntry.refs.Load() != 0 || scoreEntry.refs.Load() != 0 {
		t.Fatalf("expected init to release old entries: age=%d score=%d", ageEntry.refs.Load(), scoreEntry.refs.Load())
	}
	if got := cache.EntryCount(); got != 0 {
		t.Fatalf("EntryCount after init=%d want 0", got)
	}
	if got := cache.spanMaxEntries; got != NumericRangeSpanCacheMaxEntries(0) {
		t.Fatalf("spanMaxEntries after init=%d want %d", got, NumericRangeSpanCacheMaxEntries(0))
	}
	if cache.fieldIndexLen != 0 {
		t.Fatalf("fieldIndexLen after init=%d want 0", cache.fieldIndexLen)
	}
	if len(fieldIndex) != 0 {
		t.Fatalf("expected init to clear field index, got %d entries", len(fieldIndex))
	}
	if _, ok := cache.LoadSlot("age", 0); ok {
		t.Fatal("expected old ordinal slot to miss after init")
	}
	if _, ok := cache.LoadField("score"); ok {
		t.Fatal("expected old field index entry to miss after init")
	}

	heightEntry := GetNumericRangeBucketEntry(storage, idx)
	cache.StoreSlot("height", 1, heightEntry)
	if _, ok := cache.LoadField("height"); !ok {
		t.Fatal("expected fresh field index entry")
	}

	cache.Init(4)
	if heightEntry.refs.Load() != 0 {
		t.Fatalf("expected growing init to release old entry, refs=%d", heightEntry.refs.Load())
	}
	if got := cache.EntryCount(); got != 0 {
		t.Fatalf("EntryCount after growing init=%d want 0", got)
	}
	if got := cache.spanMaxEntries; got != NumericRangeSpanCacheMaxEntries(0) {
		t.Fatalf("spanMaxEntries after growing init=%d want %d", got, NumericRangeSpanCacheMaxEntries(0))
	}
	if _, ok := cache.LoadSlot("height", 1); ok {
		t.Fatal("expected fresh slot to miss after growing init")
	}
}

func TestNumericRangeBucketCacheStoreSlotReleasesReplacedEntry(t *testing.T) {
	storage := qcacheTestFieldStorage(32, 100)
	defer storage.Release()

	cache := GetNumericRangeBucketCache(1)
	defer ReleaseNumericRangeBucketCache(cache)

	oldEntry := GetNumericRangeBucketEntry(storage, NumericRangeBucketIndex{bucketSize: 16, keyCount: storage.KeyCount()})
	newEntry := GetNumericRangeBucketEntry(storage, NumericRangeBucketIndex{bucketSize: 16, keyCount: storage.KeyCount()})

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

	cache := GetNumericRangeBucketCache(1)
	defer ReleaseNumericRangeBucketCache(cache)

	n := max(4, runtime.GOMAXPROCS(0)*2)
	idx := NumericRangeBucketIndex{bucketSize: 16, keyCount: storage.KeyCount()}
	candidates := make([]*NumericRangeBucketEntry, n)
	got := make([]*NumericRangeBucketEntry, n)
	for i := range candidates {
		candidates[i] = GetNumericRangeBucketEntry(storage, idx)
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

func TestNumericRangeBucketCacheDrainRetiredDetachesEvictedSpans(t *testing.T) {
	cache := GetNumericRangeBucketCacheWithRetireContext(1, 4, NumericRangeSpanCacheMaxEntryBytes(0), NumericRangeExactCacheMaxEntries(0), NumericRangeExactCacheMaxEntryBytes(0), nil, nil)
	defer ReleaseNumericRangeBucketCache(cache)

	entry := GetNumericRangeBucketEntry(indexdata.FieldStorage{}, NumericRangeBucketIndex{})
	cache.StoreSlot("age", 0, entry)
	for i := 0; i < 4; i++ {
		cached, ok := cache.TryStoreFullSpan(entry, 0, i, i, qcacheTestPosting(uint64(i+1), 1<<32|uint64(i+1)))
		if !ok {
			t.Fatalf("expected full-span store %d to succeed", i)
		}
		cached.Release()
	}
	if cached, ok := cache.TryStoreFullSpan(entry, 0, 4, 4, qcacheTestPosting(5, 1<<32|5)); ok {
		cached.Release()
		t.Fatal("expected first over-capacity span to be observed, not admitted")
	}
	cached, ok := cache.TryStoreFullSpan(entry, 0, 4, 4, qcacheTestPosting(5, 1<<32|5))
	if !ok {
		t.Fatal("expected second over-capacity span to evict/admit")
	}
	cached.Release()
	if cache.spanRetiredCount != 1 {
		t.Fatalf("retired count=%d want 1", cache.spanRetiredCount)
	}

	retired := cache.TakeRetired()
	if retired.IsEmpty() {
		t.Fatal("expected numeric retired postings to be detached")
	}
	retired.Release()
	if cache.spanRetiredCount != 0 || cache.spanRetiredBytes != 0 {
		t.Fatalf("retired accounting after drain: count=%d bytes=%d", cache.spanRetiredCount, cache.spanRetiredBytes)
	}
}

func TestNumericRangeBucketCacheTakeRetiredBeforeUsesEpochForSharedEntries(t *testing.T) {
	var epoch atomic.Uint64
	epoch.Store(1)

	cache := GetNumericRangeBucketCacheWithRetireContext(1, 4, NumericRangeSpanCacheMaxEntryBytes(0), NumericRangeExactCacheMaxEntries(0), NumericRangeExactCacheMaxEntryBytes(0), nil, &epoch)
	defer ReleaseNumericRangeBucketCache(cache)

	entry := GetNumericRangeBucketEntry(indexdata.FieldStorage{}, NumericRangeBucketIndex{})
	cache.StoreSlot("age", 0, entry)

	for i := 0; i < 4; i++ {
		cached, ok := cache.TryStoreFullSpan(entry, 0, i, i, qcacheTestPosting(uint64(i+1), 1<<32|uint64(i+1)))
		if !ok {
			t.Fatalf("expected full-span store %d to succeed", i)
		}
		cached.Release()
	}
	if cached, ok := cache.TryStoreFullSpan(entry, 0, 4, 4, qcacheTestPosting(5, 1<<32|5)); ok {
		cached.Release()
		t.Fatal("expected first over-capacity span to be observed, not admitted")
	}
	cached, ok := cache.TryStoreFullSpan(entry, 0, 4, 4, qcacheTestPosting(5, 1<<32|5))
	if !ok {
		t.Fatal("expected second over-capacity span to evict/admit")
	}
	cached.Release()
	if cache.spanRetiredCount == 0 {
		t.Fatal("expected numeric cache to have retired full-span postings")
	}

	retained := cache.TakeRetiredBefore(1)
	if !retained.IsEmpty() {
		retained.Release()
		t.Fatal("expected retired postings at safe epoch to remain protected")
	}
	retained.Release()
	if cache.spanRetiredCount == 0 {
		t.Fatal("expected retained retired postings after same-epoch drain")
	}

	retired := cache.TakeRetiredBefore(2)
	if retired.IsEmpty() {
		t.Fatal("expected older retired postings to detach by epoch")
	}
	retired.Release()
	if cache.spanRetiredCount != 0 {
		t.Fatal("expected shared numeric retired postings to be drained by epoch")
	}
}

func TestNumericRangeBucketCacheRetiredBacklogRejectsReplacementUntilDrain(t *testing.T) {
	cache := GetNumericRangeBucketCacheWithRetireContext(1, 4, NumericRangeSpanCacheMaxEntryBytes(0), NumericRangeExactCacheMaxEntries(0), NumericRangeExactCacheMaxEntryBytes(0), nil, nil)
	entry := GetNumericRangeBucketEntry(indexdata.FieldStorage{}, NumericRangeBucketIndex{})
	cache.StoreSlot("age", 0, entry)

	for i := 0; i < 4; i++ {
		cached, ok := cache.TryStoreFullSpan(entry, 0, i, i, qcacheTestPosting(uint64(i+1), 1<<32|uint64(i+1)))
		if !ok {
			t.Fatalf("expected full-span store %d to succeed", i)
		}
		cached.Release()
	}
	for i := 4; i < 8; i++ {
		if cached, ok := cache.TryStoreFullSpan(entry, 0, i, i, qcacheTestPosting(uint64(i+1), 1<<32|uint64(i+1))); ok {
			cached.Release()
			t.Fatalf("expected first observation for span %d to skip admission", i)
		}
		cached, ok := cache.TryStoreFullSpan(entry, 0, i, i, qcacheTestPosting(uint64(i+1), 1<<32|uint64(i+1)))
		if !ok {
			t.Fatalf("expected second observation for span %d to evict/admit", i)
		}
		cached.Release()
	}
	if got := cache.spanRetiredCount; got != 4 {
		t.Fatalf("retired backlog=%d want 4", got)
	}

	caller := qcacheTestPosting(99, 1<<32|99)
	got, ok := cache.TryStoreFullSpan(entry, 0, 99, 99, caller.Borrow())
	if ok {
		got.Release()
		caller.Release()
		t.Fatal("expected replacement to be rejected while retired backlog is full")
	}
	if !got.SharesPayload(caller) {
		caller.Release()
		t.Fatal("expected rejected replacement to return caller payload")
	}
	caller.Release()

	retired := cache.TakeRetired()
	retired.Release()
	cached, ok := cache.TryStoreFullSpan(entry, 0, 99, 99, qcacheTestPosting(99, 1<<32|99))
	ReleaseNumericRangeBucketCache(cache)
	if !ok {
		t.Fatal("expected replacement to resume after retired drain")
	}
	cached.Release()
}

func TestNumericRangeBucketCache_InheritRetainsMatchingStorage(t *testing.T) {
	shared := qcacheTestFieldStorage(32, 100)
	changed := qcacheTestFieldStorage(32, 200)
	defer shared.Release()
	defer changed.Release()

	prev := GetNumericRangeBucketCache(2)
	next := GetNumericRangeBucketCache(2)
	defer ReleaseNumericRangeBucketCache(next)

	idx := NumericRangeBucketIndex{bucketSize: 16, keyCount: shared.KeyCount()}
	ageEntry := GetNumericRangeBucketEntry(shared, idx)
	prev.StoreSlot("age", 0, ageEntry)
	stored, ok := prev.TryStoreFullSpan(ageEntry, 0, 0, 0, qcacheTestPosting(1, 2, 3))
	if !ok {
		ReleaseNumericRangeBucketCache(prev)
		t.Fatal("expected full-span store to succeed")
	}
	stored.Release()

	scoreEntry := GetNumericRangeBucketEntry(changed, idx)
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
	cached, ok := next.LoadFullSpan(inherited, 0, 0, 0)
	if !ok {
		t.Fatal("expected inherited retained entry to survive previous cache release")
	}
	qcacheTestAssertPostingSet(t, cached, []uint64{1, 2, 3})
	cached.Release()
}

func TestNumericRangeBucketCacheInheritReleasesReplacedEntry(t *testing.T) {
	storage := qcacheTestFieldStorage(32, 100)
	defer storage.Release()

	prev := GetNumericRangeBucketCache(1)
	next := GetNumericRangeBucketCache(1)
	defer ReleaseNumericRangeBucketCache(prev)
	defer ReleaseNumericRangeBucketCache(next)

	idx := NumericRangeBucketIndex{bucketSize: 16, keyCount: storage.KeyCount()}
	inherited := GetNumericRangeBucketEntry(storage, idx)
	replaced := GetNumericRangeBucketEntry(storage, idx)
	prev.StoreSlot("age", 0, inherited)
	next.StoreSlot("score", 0, replaced)
	if _, ok := next.LoadField("score"); !ok {
		t.Fatal("expected target field index to contain replaced entry")
	}

	fields := schema.IndexedFieldMap{"age": {Ordinal: 0}}
	next.InheritFrom(prev, []indexdata.FieldStorage{storage}, fields)

	if replaced.refs.Load() != 0 {
		t.Fatalf("expected overwritten numeric range entry to be released, refs=%d", replaced.refs.Load())
	}
	if inherited.refs.Load() != 2 {
		t.Fatalf("expected inherited entry to be retained by both caches, refs=%d", inherited.refs.Load())
	}
	if got, ok := next.LoadField("age"); !ok || got != inherited {
		t.Fatal("expected target field index to return inherited entry")
	}
	if _, ok := next.LoadField("score"); ok {
		t.Fatal("expected replaced field index entry to be removed")
	}
}

func TestNumericRangeBucketCacheInheritSameEntryDoesNotRetainAgain(t *testing.T) {
	storage := qcacheTestFieldStorage(32, 100)
	defer storage.Release()

	prev := GetNumericRangeBucketCache(1)
	next := GetNumericRangeBucketCache(1)
	defer ReleaseNumericRangeBucketCache(prev)
	defer ReleaseNumericRangeBucketCache(next)

	idx := NumericRangeBucketIndex{bucketSize: 16, keyCount: storage.KeyCount()}
	entry := GetNumericRangeBucketEntry(storage, idx)
	prev.StoreSlot("age", 0, entry)

	fields := schema.IndexedFieldMap{"age": {Ordinal: 0}}
	next.InheritFrom(prev, []indexdata.FieldStorage{storage}, fields)
	if entry.refs.Load() != 2 {
		t.Fatalf("expected inherited entry to have two cache refs, got %d", entry.refs.Load())
	}

	next.InheritFrom(prev, []indexdata.FieldStorage{storage}, fields)
	if entry.refs.Load() != 2 {
		t.Fatalf("expected repeated inherit not to add a cache ref, got %d", entry.refs.Load())
	}
}

func TestNumericRangeBucketCacheEvictionMarksDirtyOwner(t *testing.T) {
	storage := qcacheTestFieldStorage(32, 100)
	defer storage.Release()

	var owner atomic.Bool
	var epoch atomic.Uint64
	cache := GetNumericRangeBucketCacheWithRetireContext(1, 4, NumericRangeSpanCacheMaxEntryBytes(0), NumericRangeExactCacheMaxEntries(0), NumericRangeExactCacheMaxEntryBytes(0), &owner, &epoch)
	defer ReleaseNumericRangeBucketCache(cache)

	idx := NumericRangeBucketIndex{bucketSize: 16, keyCount: storage.KeyCount()}
	entry := GetNumericRangeBucketEntry(storage, idx)
	cache.StoreSlot("age", 0, entry)
	for i := 0; i < 4; i++ {
		cached, ok := cache.TryStoreFullSpan(entry, 0, i, i, qcacheTestPosting(uint64(i+1)))
		if !ok {
			t.Fatalf("expected full-span store %d to succeed", i)
		}
		cached.Release()
	}
	if cached, ok := cache.TryStoreFullSpan(entry, 0, 4, 4, qcacheTestPosting(5)); ok {
		cached.Release()
		t.Fatal("expected first over-capacity span observation to skip admission")
	}
	cached, ok := cache.TryStoreFullSpan(entry, 0, 4, 4, qcacheTestPosting(5))
	if !ok {
		t.Fatal("expected second over-capacity span observation to evict/admit")
	}
	cached.Release()
	if !cache.retiredDirty.Load() {
		t.Fatal("expected eviction to mark numeric cache dirty")
	}
	if !owner.Load() {
		t.Fatal("expected eviction to mark dirty owner")
	}
}

func TestNumericRangeBucketCacheInheritsLiveSpansForMatchingStorage(t *testing.T) {
	storage := qcacheTestFieldStorage(32, 100)
	defer storage.Release()

	prev := GetNumericRangeBucketCache(1)
	defer ReleaseNumericRangeBucketCache(prev)

	next := GetNumericRangeBucketCache(1)
	defer ReleaseNumericRangeBucketCache(next)

	entry := GetNumericRangeBucketEntry(storage, NumericRangeBucketIndex{bucketSize: 16, keyCount: storage.KeyCount()})
	prev.StoreSlot("age", 0, entry)
	cached, ok := prev.TryStoreFullSpan(entry, 0, 1, 3, qcacheTestPosting(1, 2, 3))
	if !ok {
		t.Fatal("expected source full-span store")
	}
	cached.Release()
	next.InheritFrom(prev, []indexdata.FieldStorage{storage}, schema.IndexedFieldMap{"age": {Ordinal: 0}})
	nextEntry, ok := next.LoadField("age")
	if !ok {
		t.Fatal("expected inherited numeric entry")
	}
	got, ok := next.LoadFullSpan(nextEntry, 0, 1, 3)
	if !ok {
		t.Fatal("expected inherited full-span hit")
	}
	qcacheTestAssertPostingSet(t, got, []uint64{1, 2, 3})
	got.Release()
}

func TestNumericRangeBucketCacheKeepsRetiredSpansOnSource(t *testing.T) {
	storage := qcacheTestFieldStorage(32, 100)
	defer storage.Release()

	var owner atomic.Bool
	var epoch atomic.Uint64
	epoch.Store(1)

	prev := GetNumericRangeBucketCacheWithRetireContext(1, 4, NumericRangeSpanCacheMaxEntryBytes(0), NumericRangeExactCacheMaxEntries(0), NumericRangeExactCacheMaxEntryBytes(0), &owner, &epoch)
	defer ReleaseNumericRangeBucketCache(prev)

	next := GetNumericRangeBucketCacheWithRetireContext(1, 4, NumericRangeSpanCacheMaxEntryBytes(0), NumericRangeExactCacheMaxEntries(0), NumericRangeExactCacheMaxEntryBytes(0), &owner, &epoch)
	defer ReleaseNumericRangeBucketCache(next)

	entry := GetNumericRangeBucketEntry(storage, NumericRangeBucketIndex{bucketSize: 16, keyCount: storage.KeyCount()})
	prev.StoreSlot("age", 0, entry)
	for i := 0; i < 4; i++ {
		cached, ok := prev.TryStoreFullSpan(entry, 0, i, i, qcacheTestPosting(uint64(i+1), 1<<32|uint64(i+1)))
		if !ok {
			t.Fatalf("expected source full-span store %d", i)
		}
		cached.Release()
	}
	if cached, ok := prev.TryStoreFullSpan(entry, 0, 4, 4, qcacheTestPosting(5, 1<<32|5)); ok {
		cached.Release()
		t.Fatal("expected first over-capacity span to be observed, not admitted")
	}
	cached, ok := prev.TryStoreFullSpan(entry, 0, 4, 4, qcacheTestPosting(5, 1<<32|5))
	if !ok {
		t.Fatal("expected second over-capacity span to evict/admit")
	}
	cached.Release()
	if prev.spanRetiredCount != 1 || !prev.RetiredDirty() {
		t.Fatalf("source retired backlog: count=%d dirty=%v", prev.spanRetiredCount, prev.RetiredDirty())
	}

	owner.Store(false)
	next.InheritFrom(prev, []indexdata.FieldStorage{storage}, schema.IndexedFieldMap{"age": {Ordinal: 0}})
	if prev.spanRetiredCount != 1 || !prev.RetiredDirty() {
		t.Fatalf("source retired backlog after inherit: count=%d dirty=%v", prev.spanRetiredCount, prev.RetiredDirty())
	}
	if next.spanRetiredCount != 0 || next.RetiredDirty() || owner.Load() {
		t.Fatalf("target retired backlog: count=%d dirty=%v owner=%v", next.spanRetiredCount, next.RetiredDirty(), owner.Load())
	}

	retained := prev.TakeRetiredBefore(1)
	if !retained.IsEmpty() {
		retained.Release()
		t.Fatal("expected source retired span to remain protected at same safe epoch")
	}
	retained.Release()
	if prev.spanRetiredCount != 1 {
		t.Fatalf("source retired backlog after protected drain=%d want 1", prev.spanRetiredCount)
	}

	retired := prev.TakeRetiredBefore(2)
	if retired.IsEmpty() {
		t.Fatal("expected source retired span to detach after safe epoch advances")
	}
	retired.Release()
	if prev.spanRetiredCount != 0 || prev.RetiredDirty() {
		t.Fatalf("source retired backlog after drain: count=%d dirty=%v", prev.spanRetiredCount, prev.RetiredDirty())
	}
}

func TestNumericRangeBucketInheritedBorrowedViewSurvivesSourceRelease(t *testing.T) {
	storage := qcacheTestFieldStorage(32, 100)
	defer storage.Release()
	idx := NumericRangeBucketIndex{bucketSize: 16, keyCount: storage.KeyCount()}

	prev := GetNumericRangeBucketCache(1)
	next := GetNumericRangeBucketCache(1)
	defer ReleaseNumericRangeBucketCache(next)

	entry := GetNumericRangeBucketEntry(storage, idx)
	prev.StoreSlot("age", 0, entry)

	base := qcacheTestLargePosting()
	want := base.ToArray()
	stored, ok := prev.TryStoreFullSpan(entry, 0, 0, 0, base.Borrow())
	if !ok || stored.IsEmpty() {
		base.Release()
		ReleaseNumericRangeBucketCache(prev)
		t.Fatal("expected full-span cache store to succeed")
	}
	stored.Release()
	base.Release()

	next.InheritFrom(prev, []indexdata.FieldStorage{storage}, schema.IndexedFieldMap{"age": {Ordinal: 0}})
	nextEntry, ok := next.LoadField("age")
	if !ok {
		ReleaseNumericRangeBucketCache(prev)
		t.Fatal("expected inherited entry")
	}

	held, ok := next.LoadFullSpan(nextEntry, 0, 0, 0)
	if !ok || held.IsEmpty() {
		ReleaseNumericRangeBucketCache(prev)
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
	ReleaseNumericRangeBucketCache(prev)
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
	cache, entry := qcacheTestNumericSpanCache(t)

	base := qcacheTestPosting(10, 20, 30, 40)
	stored, ok := cache.TryStoreFullSpan(entry, 0, 4, 10, base)
	if !ok {
		base.Release()
		t.Fatal("expected first full span store to succeed")
	}
	stored.Release()

	cachedSuffix, startSuffix, endSuffix, ok := cache.LoadExtendedFullSpan(entry, 0, 2, 10)
	if !ok {
		t.Fatal("expected suffix extension hit")
	}
	if startSuffix != 4 || endSuffix != 10 {
		t.Fatalf("unexpected suffix extension bounds: got=%d..%d want=4..10", startSuffix, endSuffix)
	}
	cachedSuffix.Release()

	cachedPrefix, startPrefix, endPrefix, ok := cache.LoadExtendedFullSpan(entry, 0, 4, 12)
	if !ok {
		t.Fatal("expected prefix extension hit")
	}
	if startPrefix != 4 || endPrefix != 10 {
		t.Fatalf("unexpected prefix extension bounds: got=%d..%d want=4..10", startPrefix, endPrefix)
	}
	cachedPrefix.Release()
}

func TestNumericRangeBucketSpanCacheStoresMoreThanRecentRing(t *testing.T) {
	cache := GetNumericRangeBucketCacheWithRetireContext(1, 8, NumericRangeSpanCacheMaxEntryBytes(0), NumericRangeExactCacheMaxEntries(0), NumericRangeExactCacheMaxEntryBytes(0), nil, nil)
	defer ReleaseNumericRangeBucketCache(cache)

	entry := GetNumericRangeBucketEntry(indexdata.FieldStorage{}, NumericRangeBucketIndex{})
	cache.StoreSlot("age", 0, entry)

	for i := 0; i < 8; i++ {
		ids, ok := cache.TryStoreFullSpan(entry, 0, i, i, qcacheTestPosting(uint64(i+1), uint64(i+101)))
		if !ok {
			t.Fatalf("TryStoreFullSpan(%d) rejected before capacity", i)
		}
		ids.Release()
	}
	if got := cache.FullSpanEntryCount(); got != 8 {
		t.Fatalf("FullSpanEntryCount=%d want 8", got)
	}

	recent := 0
	for i := range entry.fullSpanRecent {
		if entry.fullSpanRecent[i].used {
			recent++
		}
	}
	if recent != numericRangeFullSpanRecentMaxEntries {
		t.Fatalf("recent ring entries=%d want %d", recent, numericRangeFullSpanRecentMaxEntries)
	}

	for i := 0; i < 8; i++ {
		ids, ok := cache.LoadFullSpan(entry, 0, i, i)
		if !ok {
			t.Fatalf("LoadFullSpan(%d) missed stored span", i)
		}
		qcacheTestAssertPostingSet(t, ids, []uint64{uint64(i + 1), uint64(i + 101)})
		ids.Release()
	}
}

func TestNumericRangeBucketSpanCacheStatsTrackAdmissionLifecycle(t *testing.T) {
	cache := GetNumericRangeBucketCacheWithRetireContext(1, 1, NumericRangeSpanCacheMaxEntryBytes(0), NumericRangeExactCacheMaxEntries(0), NumericRangeExactCacheMaxEntryBytes(0), nil, nil)
	defer ReleaseNumericRangeBucketCache(cache)

	entry := GetNumericRangeBucketEntry(indexdata.FieldStorage{}, NumericRangeBucketIndex{})
	cache.StoreSlot("age", 0, entry)

	ids, ok := cache.TryStoreFullSpan(entry, 0, 0, 0, qcacheTestPosting(1, 2, 3))
	if !ok {
		t.Fatal("initial full-span store rejected")
	}
	ids.Release()

	hit, ok := cache.LoadFullSpan(entry, 0, 0, 0)
	if !ok {
		t.Fatal("expected cache hit")
	}
	hit.Release()
	if miss, ok := cache.LoadFullSpan(entry, 0, 10, 10); ok {
		miss.Release()
		t.Fatal("expected cache miss")
	}

	rejected, ok := cache.TryStoreFullSpan(entry, 0, 1, 1, qcacheTestPosting(4))
	if ok {
		rejected.Release()
		t.Fatal("first observation admitted while cache was full")
	}
	rejected.Release()

	admitted, ok := cache.TryStoreFullSpan(entry, 0, 1, 1, qcacheTestPosting(4))
	if !ok {
		t.Fatal("second observation did not admit replacement")
	}
	admitted.Release()

	rejected, ok = cache.TryStoreFullSpan(entry, 0, 2, 2, qcacheTestPosting(5))
	if ok {
		rejected.Release()
		t.Fatal("first observation admitted while retired backlog was full")
	}
	rejected.Release()
	rejected, ok = cache.TryStoreFullSpan(entry, 0, 2, 2, qcacheTestPosting(5))
	if ok {
		rejected.Release()
		t.Fatal("retired backlog allowed another eviction")
	}
	rejected.Release()

	stats := cache.NumericSpanStats()
	if stats.EntryCount != 1 || stats.CurrentBytes == 0 || stats.RetiredBytes == 0 {
		t.Fatalf("unexpected span size stats: %+v", stats)
	}
	if stats.MaxEntries != 1 || stats.MaxEntryBytes != NumericRangeSpanCacheMaxEntryBytes(0) {
		t.Fatalf("unexpected span limits: %+v", stats)
	}
	if stats.Stores != 2 || stats.Evictions != 1 {
		t.Fatalf("unexpected span lifecycle counters: %+v", stats)
	}
	if stats.RejectedCapacity != 2 || stats.RejectedRetiredBacklog != 1 {
		t.Fatalf("unexpected span reject counters: %+v", stats)
	}

	tooLargeCache := GetNumericRangeBucketCacheWithRetireContext(1, 1, 1, NumericRangeExactCacheMaxEntries(0), NumericRangeExactCacheMaxEntryBytes(0), nil, nil)
	defer ReleaseNumericRangeBucketCache(tooLargeCache)
	tooLargeEntry := GetNumericRangeBucketEntry(indexdata.FieldStorage{}, NumericRangeBucketIndex{})
	tooLargeCache.StoreSlot("age", 0, tooLargeEntry)
	rejected, ok = tooLargeCache.TryStoreFullSpan(tooLargeEntry, 0, 0, 0, qcacheTestLargePosting())
	if ok {
		rejected.Release()
		t.Fatal("oversized span admitted")
	}
	rejected.Release()
	if stats := tooLargeCache.NumericSpanStats(); stats.RejectedTooLarge != 1 {
		t.Fatalf("RejectedTooLarge=%d want 1: %+v", stats.RejectedTooLarge, stats)
	}
}

func TestNumericRangeFullSpanStoreBorrowedDetachesFromSourceOwner(t *testing.T) {
	cache, entry := qcacheTestNumericSpanCache(t)

	base := qcacheTestLargePosting()
	want := base.ToArray()

	stored, ok := cache.TryStoreFullSpan(entry, 0, 3, 7, base.Borrow())
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

	cached, ok := cache.LoadFullSpan(entry, 0, 3, 7)
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
	cache, entry := qcacheTestNumericSpanCache(t)

	cachedBase := qcacheTestLargePosting()
	want := cachedBase.ToArray()

	stored, ok := cache.TryStoreFullSpan(entry, 0, 3, 7, cachedBase.Borrow())
	if !ok || stored.IsEmpty() {
		cachedBase.Release()
		t.Fatal("expected initial numeric full-span store to succeed")
	}
	stored.Release()

	cached, ok := cache.LoadFullSpan(entry, 0, 3, 7)
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

	got, ok := cache.TryStoreFullSpan(entry, 0, 3, 7, source.Borrow())
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

	reloaded, ok := cache.LoadFullSpan(entry, 0, 3, 7)
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

func TestNumericRangeExactResultCachePromotesOnRepeatAndDetachesBorrowedInput(t *testing.T) {
	cache := GetNumericRangeBucketCacheWithRetireContext(1, 4, NumericRangeSpanCacheMaxEntryBytes(0), 2, NumericRangeExactCacheMaxEntryBytes(0), nil, nil)
	defer ReleaseNumericRangeBucketCache(cache)

	source := qcacheTestLargePosting()
	want := source.ToArray()

	rejected, ok := cache.TryStoreExactResult(0, 100, 900, source.Borrow())
	if ok {
		rejected.Release()
		source.Release()
		t.Fatal("first exact result observation admitted")
	}
	if !rejected.SharesPayload(source) {
		rejected.Release()
		source.Release()
		t.Fatal("first exact result observation did not return caller payload")
	}
	rejected.Release()
	if got := cache.ExactResultEntryCount(); got != 0 {
		source.Release()
		t.Fatalf("exact entries after first observation=%d want 0", got)
	}

	stored, ok := cache.TryStoreExactResult(0, 100, 900, source.Borrow())
	if !ok {
		source.Release()
		t.Fatal("second exact result observation did not admit")
	}
	if stored.SharesPayload(source) {
		stored.Release()
		source.Release()
		t.Fatal("borrowed exact result store kept caller payload")
	}
	stored.Release()

	extra := uint64(1<<32 | 17)
	if source.Contains(extra) {
		source.Release()
		t.Fatalf("test setup chose existing id %d", extra)
	}
	source = source.BuildAdded(extra)

	cached, ok := cache.LoadExactResult(0, 100, 900)
	if !ok {
		source.Release()
		t.Fatal("expected exact result cache hit")
	}
	if !slices.Equal(cached.ToArray(), want) {
		cached.Release()
		source.Release()
		t.Fatalf("cached exact result changed after source mutation: got=%v want=%v", cached.ToArray(), want)
	}
	if cached.Contains(extra) {
		cached.Release()
		source.Release()
		t.Fatalf("cached exact result leaked caller mutation id=%d", extra)
	}
	cached.Release()
	source.Release()

	stats := cache.NumericExactResultStats()
	if stats.EntryCount != 1 || stats.Stores != 1 || stats.RejectedFirstObservation != 1 {
		t.Fatalf("unexpected exact result stats: %+v", stats)
	}
}

func TestNumericRangeExactResultCacheEvictsAndBacklogRejectsUntilDrain(t *testing.T) {
	cache := GetNumericRangeBucketCacheWithRetireContext(1, 4, NumericRangeSpanCacheMaxEntryBytes(0), 1, NumericRangeExactCacheMaxEntryBytes(0), nil, nil)
	defer ReleaseNumericRangeBucketCache(cache)

	for i := 0; i < 2; i++ {
		ids := qcacheTestPosting(uint64(i+1), 1<<32|uint64(i+1))
		if probe, ok := cache.TryStoreExactResult(0, i, i+10, ids.Borrow()); ok {
			probe.Release()
			ids.Release()
			t.Fatalf("first exact observation %d admitted", i)
		} else {
			probe.Release()
		}
		stored, ok := cache.TryStoreExactResult(0, i, i+10, ids)
		if !ok {
			t.Fatalf("second exact observation %d rejected", i)
		}
		stored.Release()
	}
	if cache.exactRetiredCount != 1 || !cache.RetiredDirty() {
		t.Fatalf("exact retired backlog after eviction: count=%d dirty=%v", cache.exactRetiredCount, cache.RetiredDirty())
	}

	ids := qcacheTestPosting(99, 1<<32|99)
	if probe, ok := cache.TryStoreExactResult(0, 99, 199, ids.Borrow()); ok {
		probe.Release()
		ids.Release()
		t.Fatal("first exact observation while retired backlog full admitted")
	} else {
		probe.Release()
	}
	rejected, ok := cache.TryStoreExactResult(0, 99, 199, ids)
	if ok {
		rejected.Release()
		t.Fatal("exact retired backlog allowed another eviction")
	}
	rejected.Release()

	retired := cache.TakeRetired()
	if retired.IsEmpty() {
		t.Fatal("expected exact retired payloads to detach")
	}
	retired.Release()
	if cache.exactRetiredCount != 0 || cache.exactRetiredBytes != 0 || cache.RetiredDirty() {
		t.Fatalf("exact retired accounting after drain: count=%d bytes=%d dirty=%v", cache.exactRetiredCount, cache.exactRetiredBytes, cache.RetiredDirty())
	}
}

func TestNumericRangeExactResultCacheInheritsLiveEntriesAndKeepsRetiredOnSource(t *testing.T) {
	storage := qcacheTestFieldStorage(32, 100)
	defer storage.Release()

	var owner atomic.Bool
	var epoch atomic.Uint64
	epoch.Store(1)

	prev := GetNumericRangeBucketCacheWithRetireContext(1, 4, NumericRangeSpanCacheMaxEntryBytes(0), 1, NumericRangeExactCacheMaxEntryBytes(0), &owner, &epoch)
	defer ReleaseNumericRangeBucketCache(prev)

	next := GetNumericRangeBucketCacheWithRetireContext(1, 4, NumericRangeSpanCacheMaxEntryBytes(0), 1, NumericRangeExactCacheMaxEntryBytes(0), &owner, &epoch)
	defer ReleaseNumericRangeBucketCache(next)

	entry := GetNumericRangeBucketEntry(storage, NumericRangeBucketIndex{bucketSize: 16, keyCount: storage.KeyCount()})
	prev.StoreSlot("age", 0, entry)

	for i := 0; i < 2; i++ {
		ids := qcacheTestPosting(uint64(i+1), 1<<32|uint64(i+1))
		if probe, ok := prev.TryStoreExactResult(0, i, i+10, ids.Borrow()); ok {
			probe.Release()
			ids.Release()
			t.Fatalf("first exact observation %d admitted", i)
		} else {
			probe.Release()
		}
		stored, ok := prev.TryStoreExactResult(0, i, i+10, ids)
		if !ok {
			t.Fatalf("second exact observation %d rejected", i)
		}
		stored.Release()
	}
	if prev.exactRetiredCount != 1 || !prev.RetiredDirty() {
		t.Fatalf("source exact retired backlog: count=%d dirty=%v", prev.exactRetiredCount, prev.RetiredDirty())
	}

	owner.Store(false)
	next.InheritFrom(prev, []indexdata.FieldStorage{storage}, schema.IndexedFieldMap{"age": {Ordinal: 0}})
	if prev.exactRetiredCount != 1 || !prev.RetiredDirty() {
		t.Fatalf("source exact retired backlog after inherit: count=%d dirty=%v", prev.exactRetiredCount, prev.RetiredDirty())
	}
	if next.exactRetiredCount != 0 || next.RetiredDirty() || owner.Load() {
		t.Fatalf("target exact retired backlog: count=%d dirty=%v owner=%v", next.exactRetiredCount, next.RetiredDirty(), owner.Load())
	}
	if got, ok := next.LoadExactResult(0, 1, 11); !ok {
		t.Fatal("expected live exact result to inherit")
	} else {
		qcacheTestAssertPostingSet(t, got, []uint64{2, 1<<32 | 2})
		got.Release()
	}

	retained := prev.TakeRetiredBefore(1)
	if !retained.IsEmpty() {
		retained.Release()
		t.Fatal("expected same-epoch exact retired payload to remain protected")
	}
	retained.Release()
	if prev.exactRetiredCount != 1 {
		t.Fatalf("source exact retired backlog after protected drain=%d want 1", prev.exactRetiredCount)
	}

	retired := prev.TakeRetiredBefore(2)
	if retired.IsEmpty() {
		t.Fatal("expected exact retired payload to detach after safe epoch advances")
	}
	retired.Release()
	if prev.exactRetiredCount != 0 || prev.RetiredDirty() {
		t.Fatalf("source exact retired backlog after drain: count=%d dirty=%v", prev.exactRetiredCount, prev.RetiredDirty())
	}
}

func TestNumericRangeBucketSpanCacheDetachedLoadsUnderConcurrency(t *testing.T) {
	cache, entry := qcacheTestNumericSpanCache(t)

	base := qcacheTestPosting()
	for i := uint64(1); i <= 48; i++ {
		base = base.BuildAdded(i * 5)
	}
	want := base.ToArray()

	stored := base.Borrow()
	var ok bool
	stored, ok = cache.TryStoreFullSpan(entry, 0, 11, 23, stored)
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
				cached, ok := cache.LoadFullSpan(entry, 0, 11, 23)
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
			cached, ok := cache.LoadFullSpan(entry, 0, 11, 23)
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

	cached, ok := cache.LoadFullSpan(entry, 0, 11, 23)
	if !ok {
		t.Fatal("full-span cache entry missing after concurrent mutations")
	}
	defer cached.Release()
	qcacheTestAssertPostingSet(t, cached, want)
}

func TestNumericRangeFullSpanBorrowedViewSurvivesConcurrentEviction(t *testing.T) {
	cache := GetNumericRangeBucketCacheWithRetireContext(1, 4, NumericRangeSpanCacheMaxEntryBytes(0), NumericRangeExactCacheMaxEntries(0), NumericRangeExactCacheMaxEntryBytes(0), nil, nil)
	defer ReleaseNumericRangeBucketCache(cache)

	entry := GetNumericRangeBucketEntry(indexdata.FieldStorage{}, NumericRangeBucketIndex{})
	cache.StoreSlot("age", 0, entry)

	base := qcacheTestPosting()
	for i := uint64(1); i <= 48; i++ {
		base = base.BuildAdded(i * 9)
	}
	want := base.ToArray()

	stored := base.Borrow()
	stored, ok := cache.TryStoreFullSpan(entry, 0, 0, 0, stored)
	if !ok {
		t.Fatal("expected initial full-span store to succeed")
	}
	stored.Release()
	base.Release()

	held, ok := cache.LoadFullSpan(entry, 0, 0, 0)
	if !ok || held.IsEmpty() {
		t.Fatal("expected held full-span cache entry")
	}
	defer held.Release()

	for i := 1; i < 4; i++ {
		cached, ok := cache.TryStoreFullSpan(entry, 0, i, i, qcacheTestPosting(uint64(i), 1<<32|uint64(i)))
		if !ok {
			t.Fatalf("expected prefill span %d to store", i)
		}
		cached.Release()
	}

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
		for i := 4; i < 8; i++ {
			ids := qcacheTestPosting(uint64(i), 1<<32|uint64(i))
			var ok bool
			probe, ok := cache.TryStoreFullSpan(entry, 0, i, i, ids.Borrow())
			if ok {
				probe.Release()
				ids.Release()
				setFailed(fmt.Sprintf("first tryStoreFullSpan(%d,%d) admitted unexpectedly", i, i))
				return
			}
			probe.Release()
			ids, ok = cache.TryStoreFullSpan(entry, 0, i, i, ids)
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
