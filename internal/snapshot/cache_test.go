package snapshot

import (
	"fmt"
	"reflect"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/schema"
)

func testPosting(ids ...uint64) posting.List {
	var out posting.List
	for _, id := range ids {
		out = out.BuildAdded(id)
	}
	return out
}

func testLargePosting() posting.List {
	ids := make([]uint64, 0, posting.MidCap+16)
	for i := uint64(0); i < uint64(posting.MidCap+16); i++ {
		ids = append(ids, i*3+1)
	}
	return posting.BuildFromSorted(ids)
}

func testMatPredView(limit int, maxCardinality uint64) *View {
	v := &View{}
	if limit > 0 {
		v.matPredCache = qcache.GetMaterializedPredCache(limit, maxCardinality)
	}
	return v
}

func testStorage(keys ...string) indexdata.FieldStorage {
	if len(keys) == 0 {
		return indexdata.FieldStorage{}
	}
	m := indexdata.GetPostingMap()
	for i, key := range keys {
		m[key] = (posting.List{}).BuildAdded(uint64(i + 1))
	}
	return indexdata.NewFlatFieldStorageFromPostingMapOwned(m)
}

func testView(index map[string]indexdata.FieldStorage) *View {
	byName := make(map[string]schema.IndexedFieldAccessor, len(index))
	keys := make([]string, 0, len(index))
	for name := range index {
		keys = append(keys, name)
	}
	slices.Sort(keys)
	for i, name := range keys {
		byName[name] = schema.IndexedFieldAccessor{Ordinal: i}
	}
	slots := indexdata.GetFieldStorageSlice(len(byName))[:len(byName)]
	for name, storage := range index {
		slots[byName[name].Ordinal] = storage
	}
	return &View{
		Index:              slots,
		IndexedFieldByName: byName,
	}
}

func testFieldLookupPostingRetained(v *View, field, key string) posting.List {
	storage, ok := v.FieldIndexStorage(field)
	if !ok {
		return posting.List{}
	}
	return indexdata.NewFieldIndexViewFromStorage(storage).LookupPostingRetained(key)
}

func testMatPredEntryCount(v *View) int {
	if v == nil || v.matPredCache == nil {
		return 0
	}
	return v.matPredCache.EntryCount()
}

func testRuntimeMatPredObservedEntryCount(v *View) int {
	return v.runtimeMatPredObserved.EntryCount()
}

func testScalarMatPredKey(field, key string) qcache.MaterializedPredKey {
	return qcache.MaterializedPredKeyForScalar(field, qir.OpPREFIX, key)
}

func testBucketMatPredKey(field string, bucket int) qcache.MaterializedPredKey {
	return qcache.MaterializedPredKeyForNumericBucketSpan(field, bucket, bucket)
}

func testLoadMaterializedPred(v *View, key qcache.MaterializedPredKey) (posting.List, bool) {
	return v.LoadMaterializedPredKey(key)
}

func testStoreMaterializedPred(v *View, key qcache.MaterializedPredKey, ids posting.List) {
	v.StoreMaterializedPredKey(key, ids)
}

func testTryStoreMaterializedPredOversized(v *View, key qcache.MaterializedPredKey, ids posting.List) bool {
	return v.TryStoreMaterializedPredOversizedKey(key, ids)
}

func testShouldPromoteRuntimeMaterializedPred(v *View, key qcache.MaterializedPredKey) bool {
	return v.ShouldPromoteRuntimeMaterializedPredKey(key)
}

func testShouldPromoteObservedMaterializedPred(v *View, key qcache.MaterializedPredKey, observedWork uint64, buildWork uint64) bool {
	return v.ShouldPromoteObservedMaterializedPredKey(key, observedWork, buildWork)
}

func testRunConcurrent(n int, fn func(i int)) {
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			runtime.Gosched()
			fn(i)
		}(i)
	}
	close(start)
	wg.Wait()
}

func testBoolSlots(src map[string]bool, byName map[string]schema.IndexedFieldAccessor) []bool {
	out := pooled.GetBoolSlice(len(byName))[:len(byName)]
	clear(out)
	for name, value := range src {
		if value {
			out[byName[name].Ordinal] = true
		}
	}
	return out
}

func TestViewRuntimeSeenPromotionBounded(t *testing.T) {
	v := testMatPredView(2, 0)
	limit := qcache.RecentKeyLimit(v.MaterializedPredCacheLimit())
	if limit <= 0 {
		t.Fatalf("expected positive recent-key limit")
	}

	for i := 0; i < limit+6; i++ {
		key := testScalarMatPredKey("age", fmt.Sprintf("%d", i))
		if testShouldPromoteRuntimeMaterializedPred(v, key) {
			t.Fatalf("expected first runtime sight for %q to stay local", key.String())
		}
	}
	if got := v.RuntimeMaterializedPredSeenEntryCount(); got > limit {
		t.Fatalf("runtime seen-key cache exceeded limit: got=%d want<=%d", got, limit)
	}

	lastKey := testScalarMatPredKey("age", fmt.Sprintf("%d", limit+5))
	if !testShouldPromoteRuntimeMaterializedPred(v, lastKey) {
		t.Fatalf("expected recent runtime key %q to promote on second sight", lastKey.String())
	}
}

func TestViewOrderedORObservedPromotionAccumulates(t *testing.T) {
	v := testMatPredView(2, 0)
	key := qcache.MaterializedPredComplementKeyForScalar("age", qir.OpGTE, "30")
	if testShouldPromoteObservedMaterializedPred(v, key, 10, 25) {
		t.Fatalf("expected first observed work below threshold to stay local")
	}
	if got := testRuntimeMatPredObservedEntryCount(v); got != 1 {
		t.Fatalf("expected one ordered OR observed-work entry, got=%d", got)
	}
	if !testShouldPromoteObservedMaterializedPred(v, key, 15, 25) {
		t.Fatalf("expected accumulated observed work to trigger promotion")
	}
	if got := testRuntimeMatPredObservedEntryCount(v); got != 1 {
		t.Fatalf("expected promoted ordered OR observed-work entry to stay as evidence, got=%d", got)
	}
	if got := v.ObservedMaterializedPredWork(key); got != 25 {
		t.Fatalf("observed work after promotion=%d want 25", got)
	}
}

func TestViewStoreMaterializedPredConcurrentDistinctKeysRespectsLimit(t *testing.T) {
	n := max(16, runtime.GOMAXPROCS(0)*8)
	for round := 0; round < 16; round++ {
		v := testMatPredView(1, 0)
		testRunConcurrent(n, func(i int) {
			testStoreMaterializedPred(v, testScalarMatPredKey("email", fmt.Sprintf("%d", i)), posting.List{})
		})
		if got := v.matPredCache.EntryCount(); got > 1 {
			t.Fatalf("round=%d materialized cache count exceeded limit: got=%d", round, got)
		}
		if got := testMatPredEntryCount(v); got > 1 {
			t.Fatalf("round=%d materialized cache entries exceeded limit: got=%d", round, got)
		}
	}
}

func TestViewStoreMaterializedPredConcurrentSameKeyStoresSingleEntry(t *testing.T) {
	n := max(16, runtime.GOMAXPROCS(0)*8)
	v := testMatPredView(8, 0)
	key := testScalarMatPredKey("email", "same")

	testRunConcurrent(n, func(i int) {
		testStoreMaterializedPred(v, key, testPosting(uint64(i+1)))
	})

	if got := v.matPredCache.EntryCount(); got != 1 {
		t.Fatalf("expected single cache count for duplicate key, got=%d", got)
	}
	if got := testMatPredEntryCount(v); got != 1 {
		t.Fatalf("expected single cache entry for duplicate key, got=%d", got)
	}
}

func TestViewTryStoreMaterializedPredOversizedConcurrentDistinctKeysRespectsTotalLimit(t *testing.T) {
	n := max(16, runtime.GOMAXPROCS(0)*8)
	bm := testPosting(1, 2)
	for round := 0; round < 16; round++ {
		v := testMatPredView(1, 1)
		testRunConcurrent(n, func(i int) {
			testTryStoreMaterializedPredOversized(v, testBucketMatPredKey("age", i), bm)
		})
		if got := v.matPredCache.EntryCount(); got > 1 {
			t.Fatalf("round=%d oversized cache count exceeded limit: got=%d", round, got)
		}
		if got := testMatPredEntryCount(v); got > 1 {
			t.Fatalf("round=%d oversized cache entries exceeded limit: got=%d", round, got)
		}
		if got := v.matPredCache.OversizedCount(); got > 1 {
			t.Fatalf("round=%d oversized counter exceeded total limit: got=%d", round, got)
		}
	}
}

func TestViewTryStoreMaterializedPredOversizedConcurrentSameKeyDoesNotLeakCounters(t *testing.T) {
	n := max(16, runtime.GOMAXPROCS(0)*8)
	bm := testPosting(1, 2)
	v := testMatPredView(8, 1)
	key := testBucketMatPredKey("age", 0)

	testRunConcurrent(n, func(i int) {
		testTryStoreMaterializedPredOversized(v, key, bm)
	})

	if got := v.matPredCache.EntryCount(); got != 1 {
		t.Fatalf("expected single oversized cache count for duplicate key, got=%d", got)
	}
	if got := testMatPredEntryCount(v); got != 1 {
		t.Fatalf("expected single oversized cache entry for duplicate key, got=%d", got)
	}
	if got := v.matPredCache.OversizedCount(); got != 1 {
		t.Fatalf("expected oversized counter to settle at 1, got=%d", got)
	}
}

func TestViewMaterializedPredCacheOversizedAdmissionTurnsOverEntries(t *testing.T) {
	v := testMatPredView(8, 1)

	small := testPosting(1)
	large := testPosting(1, 2)

	for i := 0; i < 8; i++ {
		testStoreMaterializedPred(v, testScalarMatPredKey("email", fmt.Sprintf("%d", i)), small)
	}
	if got := v.matPredCache.EntryCount(); got != 8 {
		t.Fatalf("expected regular cache to fill total limit, got=%d", got)
	}
	if got := testMatPredEntryCount(v); got != 8 {
		t.Fatalf("expected eight regular cache entries before oversized admission, got=%d", got)
	}

	if !testTryStoreMaterializedPredOversized(v, testBucketMatPredKey("age", 0), large) {
		t.Fatalf("expected first oversized cache entry to replace an older entry")
	}
	if !testTryStoreMaterializedPredOversized(v, testBucketMatPredKey("age", 1), large) {
		t.Fatalf("expected second oversized cache entry to replace an older entry")
	}
	if !testTryStoreMaterializedPredOversized(v, testBucketMatPredKey("age", 2), large) {
		t.Fatalf("expected oversized cache admission to replace an older oversized entry at limit")
	}

	if got := v.matPredCache.EntryCount(); got != 8 {
		t.Fatalf("expected total cache count to remain bounded by limit, got=%d", got)
	}
	if got := testMatPredEntryCount(v); got != 8 {
		t.Fatalf("expected total cache entries to remain bounded by limit, got=%d", got)
	}
	if got := v.matPredCache.OversizedCount(); got != 2 {
		t.Fatalf("expected oversized counter to settle at 2, got=%d", got)
	}
	if _, ok := testLoadMaterializedPred(v, testBucketMatPredKey("age", 2)); !ok {
		t.Fatalf("expected newest oversized entry to remain cached")
	}
	kept := 0
	for _, key := range []qcache.MaterializedPredKey{
		testBucketMatPredKey("age", 0),
		testBucketMatPredKey("age", 1),
		testBucketMatPredKey("age", 2),
	} {
		if _, ok := testLoadMaterializedPred(v, key); ok {
			kept++
		}
	}
	if kept != 2 {
		t.Fatalf("expected oversized cache to keep exactly two entries after replacement, got=%d", kept)
	}
}

func TestViewMaterializedPredCacheRegularAdmissionTurnsOverOlderRegularEntries(t *testing.T) {
	v := testMatPredView(8, 1)

	small := testPosting(1)
	large := testPosting(1, 2)

	for i := 0; i < 8; i++ {
		testStoreMaterializedPred(v, testScalarMatPredKey("email", fmt.Sprintf("%d", i)), small)
	}
	if !testTryStoreMaterializedPredOversized(v, testBucketMatPredKey("age", 0), large) {
		t.Fatalf("expected first oversized cache entry to be admitted")
	}
	if !testTryStoreMaterializedPredOversized(v, testBucketMatPredKey("age", 1), large) {
		t.Fatalf("expected second oversized cache entry to be admitted")
	}

	lateKey := testScalarMatPredKey("email", "late")
	testStoreMaterializedPred(v, lateKey, small)

	if got := v.matPredCache.EntryCount(); got != 8 {
		t.Fatalf("expected total cache count to remain bounded by limit, got=%d", got)
	}
	if got := testMatPredEntryCount(v); got != 8 {
		t.Fatalf("expected total cache entries to remain bounded by limit, got=%d", got)
	}
	if got := v.matPredCache.OversizedCount(); got != 2 {
		t.Fatalf("expected regular turnover to keep oversized entries resident, got=%d", got)
	}
	if _, ok := testLoadMaterializedPred(v, lateKey); !ok {
		t.Fatalf("expected newest regular entry to replace an older regular entry")
	}
}

func TestViewInheritMaterializedPredCacheSkipsChangedFieldsAndKeepsOthers(t *testing.T) {
	prev := testMatPredView(8, 0)
	nameIDs := testPosting(1)
	emailIDs := testPosting(2)
	nameKey := testScalarMatPredKey("name", "a")
	emailKey := testScalarMatPredKey("email", "b")
	testStoreMaterializedPred(prev, nameKey, nameIDs)
	testStoreMaterializedPred(prev, emailKey, emailIDs)

	next := testMatPredView(8, 0)
	fields := map[string]schema.IndexedFieldAccessor{
		"name":  {Ordinal: 0},
		"email": {Ordinal: 1},
	}
	changed := testBoolSlots(map[string]bool{"name": true}, fields)
	defer pooled.ReleaseBoolSlice(changed)

	inheritMaterializedPredCache(next, prev, fields, changed)

	if _, ok := testLoadMaterializedPred(next, nameKey); ok {
		t.Fatalf("expected changed-field cache entry to be skipped")
	}
	cached, ok := testLoadMaterializedPred(next, emailKey)
	if !ok || cached != emailIDs {
		t.Fatalf("expected untouched-field cache entry to be inherited")
	}
}

func TestViewInheritMaterializedPredCacheSkipsFieldlessKeys(t *testing.T) {
	prev := testMatPredView(8, 0)
	validKey := testScalarMatPredKey("name", "b")
	testStoreMaterializedPred(prev, qcache.MaterializedPredKeyFromOpaque("opaque"), testPosting(1))
	testStoreMaterializedPred(prev, validKey, posting.List{})

	next := testMatPredView(8, 0)
	inheritMaterializedPredCache(next, prev, nil, nil)

	if got := next.matPredCache.EntryCount(); got != 1 {
		t.Fatalf("expected exactly one valid inherited entry, got=%d", got)
	}
	cached, ok := testLoadMaterializedPred(next, validKey)
	if !ok || !cached.IsEmpty() {
		t.Fatalf("expected valid negative cache entry to survive malformed-entry filtering")
	}
}

func TestViewInheritMaterializedPredCacheKeepsOversizedCount(t *testing.T) {
	prev := testMatPredView(8, 1)
	oversized := testPosting(1, 2)
	testTryStoreMaterializedPredOversized(prev, testScalarMatPredKey("a", "1"), oversized)
	testTryStoreMaterializedPredOversized(prev, testScalarMatPredKey("b", "2"), oversized)

	next := testMatPredView(8, 1)
	inheritMaterializedPredCache(next, prev, nil, nil)

	if got := next.matPredCache.EntryCount(); got != 2 {
		t.Fatalf("expected oversized entries to be inherited, got=%d", got)
	}
	if got := next.matPredCache.OversizedCount(); got != 2 {
		t.Fatalf("expected oversized counter to be inherited, got=%d", got)
	}
}

func TestViewInheritMaterializedPredCachePreservesRecencyStamps(t *testing.T) {
	prev := testMatPredView(8, 0)
	emailKey := testScalarMatPredKey("email", "a")
	nameKey := testScalarMatPredKey("name", "b")
	testStoreMaterializedPred(prev, emailKey, testPosting(1))
	testStoreMaterializedPred(prev, nameKey, testPosting(2))
	if ids, ok := testLoadMaterializedPred(prev, emailKey); ok {
		ids.Release()
	}

	next := testMatPredView(8, 0)
	inheritMaterializedPredCache(next, prev, nil, nil)

	inheritedClock := next.matPredCache.Clock()
	if _, ok := testLoadMaterializedPred(next, emailKey); !ok {
		t.Fatalf("expected first cache entry to be inherited")
	}
	if _, ok := testLoadMaterializedPred(next, nameKey); !ok {
		t.Fatalf("expected second cache entry to be inherited")
	}
	if want, got := prev.matPredCache.Clock(), inheritedClock; got != want {
		t.Fatalf("expected next snapshot clock to continue from inherited entries, got=%d want=%d", got, want)
	}
}

func TestViewInheritNumericRangeBucketCacheCopiesOnlyMatchingStorage(t *testing.T) {
	shared := testStorage("10", "20")
	prevChanged := testStorage("30")
	nextChanged := testStorage("30")

	ageEntry := qcache.GetNumericRangeBucketEntry(shared, qcache.NumericRangeBucketIndex{}, 0)
	scoreEntry := qcache.GetNumericRangeBucketEntry(prevChanged, qcache.NumericRangeBucketIndex{}, 0)

	prev := testView(map[string]indexdata.FieldStorage{"age": shared, "score": prevChanged})
	prev.numericRangeBucketCache = qcache.GetNumericRangeBucketCache(2, 0)
	prev.numericRangeBucketCache.StoreSlot("age", 0, ageEntry)
	prev.numericRangeBucketCache.StoreSlot("score", 1, scoreEntry)
	defer prev.releaseRuntimeCaches()

	next := testView(map[string]indexdata.FieldStorage{"age": shared, "score": nextChanged})
	next.numericRangeBucketCache = qcache.GetNumericRangeBucketCache(2, 0)
	defer next.releaseRuntimeCaches()
	inheritNumericRangeBucketCache(next, prev)

	if next.numericRangeBucketCache == nil {
		t.Fatalf("expected next snapshot to own a numeric range cache")
	}
	if got := next.numericRangeBucketCache.EntryCount(); got != 1 {
		t.Fatalf("expected exactly one inherited numeric range entry, got=%d", got)
	}
	raw, ok := next.numericRangeBucketCache.LoadField("age")
	if !ok || raw != ageEntry {
		t.Fatalf("expected matching-storage entry to be inherited by pointer")
	}
	if _, ok := next.numericRangeBucketCache.LoadField("score"); ok {
		t.Fatalf("expected changed-storage entry to be skipped")
	}
}

func TestViewInheritNumericRangeBucketCacheSkipsMalformedAndEmptyEntries(t *testing.T) {
	valid := testStorage("10")
	validEntry := qcache.GetNumericRangeBucketEntry(valid, qcache.NumericRangeBucketIndex{}, 0)

	prev := testView(map[string]indexdata.FieldStorage{"age": valid})
	prev.numericRangeBucketCache = qcache.GetNumericRangeBucketCache(3, 0)
	prev.numericRangeBucketCache.StoreSlot("", 0, validEntry)
	prev.numericRangeBucketCache.StoreSlot("score", 1, qcache.GetNumericRangeBucketEntry(indexdata.FieldStorage{}, qcache.NumericRangeBucketIndex{}, 0))
	validEntry.Retain()
	prev.numericRangeBucketCache.StoreSlot("age", 2, validEntry)
	defer prev.releaseRuntimeCaches()

	next := testView(map[string]indexdata.FieldStorage{"age": valid})
	next.numericRangeBucketCache = qcache.GetNumericRangeBucketCache(3, 0)
	defer next.releaseRuntimeCaches()
	inheritNumericRangeBucketCache(next, prev)

	if next.numericRangeBucketCache == nil {
		t.Fatalf("expected next snapshot to own numeric cache map")
	}
	if got := next.numericRangeBucketCache.EntryCount(); got != 1 {
		t.Fatalf("expected malformed/empty entries to be filtered, got=%d", got)
	}
	raw, ok := next.numericRangeBucketCache.LoadField("age")
	if !ok || raw != validEntry {
		t.Fatalf("expected only valid age entry to survive filtering")
	}
}

func TestViewMaterializedPredMixedRegularAndOversizedConcurrentRespectsGlobalLimit(t *testing.T) {
	n := max(16, runtime.GOMAXPROCS(0)*8)
	small := testPosting(1)
	large := testPosting(1, 2)

	for round := 0; round < 32; round++ {
		v := testMatPredView(1, 1)
		testRunConcurrent(n, func(i int) {
			if i%2 == 0 {
				testStoreMaterializedPred(v, testScalarMatPredKey("email", fmt.Sprintf("%d", i)), small)
				return
			}
			testTryStoreMaterializedPredOversized(v, testBucketMatPredKey("age", i), large)
		})

		if got := v.matPredCache.EntryCount(); got > 1 {
			t.Fatalf("round=%d mixed cache count exceeded global limit: got=%d", round, got)
		}
		if got := testMatPredEntryCount(v); got > 1 {
			t.Fatalf("round=%d mixed cache entries exceeded global limit: got=%d", round, got)
		}
	}
}

func TestViewMaterializedPredCacheDetachedLoadsUnderConcurrency(t *testing.T) {
	v := testMatPredView(8, 0)

	base := testPosting()
	for i := uint64(1); i <= 40; i++ {
		base = base.BuildAdded(i * 5)
	}
	want := base.ToArray()
	key := testScalarMatPredKey("email", "al")

	testStoreMaterializedPred(v, key, base.Borrow())
	base.Release()

	remove := testPosting(want[0], want[1], want[2])
	add := testPosting(1<<32|3, 1<<32|7)
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

	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 1000; i++ {
				cached, ok := testLoadMaterializedPred(v, key)
				if !ok {
					setFailed("cache entry unexpectedly missing")
					return
				}
				if !slices.Equal(cached.ToArray(), want) {
					setFailed(fmt.Sprintf("cached view mismatch: got=%v want=%v", cached.ToArray(), want))
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
			cached, ok := testLoadMaterializedPred(v, key)
			if !ok {
				setFailed("writer load unexpectedly missed cache")
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

	cached, ok := testLoadMaterializedPred(v, key)
	if !ok {
		t.Fatalf("cache entry missing after concurrent mutation")
	}
	defer cached.Release()
	if got := cached.ToArray(); !slices.Equal(got, want) {
		t.Fatalf("posting mismatch: got=%v want=%v", got, want)
	}
}

type buildCacheRec struct {
	Name  string  `db:"name" rbi:"index"`
	Email string  `db:"email" rbi:"index"`
	Age   int     `db:"age" rbi:"index"`
	Opt   *string `db:"opt" rbi:"index"`
}

func buildCacheRuntime(t testing.TB) *schema.Schema {
	t.Helper()
	rt, err := schema.Compile(reflect.TypeOf(buildCacheRec{}), schema.Config{})
	if err != nil {
		t.Fatalf("schema.Compile: %v", err)
	}
	return rt
}

func buildCacheSeedView(t testing.TB, s *schema.Schema, rec *buildCacheRec) *View {
	t.Helper()
	snap := BuildPrepared(1, nil, s, CacheConfig{MatPredMaxEntries: 64}, nil, s.Patch.Fields, []BatchEntry{
		{ID: 1, New: unsafe.Pointer(rec)},
	})
	if snap == nil {
		t.Fatal("expected seed snapshot")
	}
	return snap
}

func buildCachePatchView(t testing.TB, s *schema.Schema, prev *View, oldRec, newRec *buildCacheRec, patch []schema.PatchItem) *View {
	t.Helper()
	snap := BuildPrepared(2, prev, s, CacheConfig{MatPredMaxEntries: 64}, nil, s.Patch.Fields, []BatchEntry{{
		ID:        1,
		Old:       unsafe.Pointer(oldRec),
		New:       unsafe.Pointer(newRec),
		Patch:     patch,
		PatchOnly: true,
	}})
	if snap == nil {
		t.Fatal("expected patched snapshot")
	}
	return snap
}

func TestBuildPreparedEmptyBaseDropsTouchedMaterializedPredCache(t *testing.T) {
	rt := buildCacheRuntime(t)
	prev := NewView(0, nil, rt, CacheConfig{MatPredMaxEntries: 64}, Storage{})
	defer prev.releaseRuntimeCaches()
	defer prev.releaseStorage()

	key := qcache.MaterializedPredKeyForScalar("email", qir.OpPREFIX, "user")
	prev.StoreMaterializedPredKey(key, posting.List{})
	if !prev.HasMaterializedPredKey(key) {
		t.Fatal("expected cached negative predicate on empty snapshot")
	}

	rec := buildCacheRec{Name: "alice", Email: "user1@example.com"}
	next := BuildPrepared(1, prev, rt, CacheConfig{MatPredMaxEntries: 64}, nil, rt.Patch.Fields, []BatchEntry{
		{ID: 1, New: unsafe.Pointer(&rec)},
	})
	defer next.releaseRuntimeCaches()
	defer next.releaseStorage()

	if next.HasMaterializedPredKey(key) {
		t.Fatal("expected first write from empty base to drop touched predicate cache")
	}
}

func TestBuildPreparedPatchUnchangedFieldInheritsPositiveMaterializedPredCache(t *testing.T) {
	rt := buildCacheRuntime(t)
	oldRec := buildCacheRec{Name: "alice", Email: "alice@example.com", Age: 30}
	prev := buildCacheSeedView(t, rt, &oldRec)
	defer prev.releaseRuntimeCaches()
	defer prev.releaseStorage()

	key := qcache.MaterializedPredKeyForScalar("email", qir.OpPREFIX, "ali")
	ids := testPosting(1)
	prev.StoreMaterializedPredKey(key, ids)

	newRec := oldRec
	newRec.Age = 31
	next := buildCachePatchView(t, rt, prev, &oldRec, &newRec, []schema.PatchItem{{Name: "age", Value: 31}})
	defer next.releaseRuntimeCaches()
	defer next.releaseStorage()

	cached, ok := next.LoadMaterializedPredKey(key)
	if !ok || cached != ids {
		t.Fatal("expected positive untouched cache entry to be inherited")
	}
}

func TestBuildPreparedPatchUnchangedFieldInheritsNegativeMaterializedPredCache(t *testing.T) {
	rt := buildCacheRuntime(t)
	oldRec := buildCacheRec{Name: "alice", Email: "alice@example.com", Age: 30}
	prev := buildCacheSeedView(t, rt, &oldRec)
	defer prev.releaseRuntimeCaches()
	defer prev.releaseStorage()

	key := qcache.MaterializedPredKeyForScalar("email", qir.OpPREFIX, "zz")
	prev.StoreMaterializedPredKey(key, posting.List{})

	newRec := oldRec
	newRec.Age = 31
	next := buildCachePatchView(t, rt, prev, &oldRec, &newRec, []schema.PatchItem{{Name: "age", Value: 31}})
	defer next.releaseRuntimeCaches()
	defer next.releaseStorage()

	cached, ok := next.LoadMaterializedPredKey(key)
	if !ok || !cached.IsEmpty() {
		t.Fatal("expected negative untouched cache entry to be inherited")
	}
}

func TestBuildPreparedPatchTouchedFieldDropsOnlyTouchedMaterializedPredCache(t *testing.T) {
	rt := buildCacheRuntime(t)
	oldRec := buildCacheRec{Name: "alice", Email: "alice@example.com"}
	prev := buildCacheSeedView(t, rt, &oldRec)
	defer prev.releaseRuntimeCaches()
	defer prev.releaseStorage()

	nameKey := qcache.MaterializedPredKeyForScalar("name", qir.OpPREFIX, "ali")
	emailKey := qcache.MaterializedPredKeyForScalar("email", qir.OpPREFIX, "ali")
	emailIDs := testPosting(1)
	prev.StoreMaterializedPredKey(nameKey, testPosting(1))
	prev.StoreMaterializedPredKey(emailKey, emailIDs)

	newRec := oldRec
	newRec.Name = "alicia"
	next := buildCachePatchView(t, rt, prev, &oldRec, &newRec, []schema.PatchItem{{Name: "name", Value: "alicia"}})
	defer next.releaseRuntimeCaches()
	defer next.releaseStorage()

	if next.HasMaterializedPredKey(nameKey) {
		t.Fatal("expected touched field cache entry to be dropped")
	}
	cached, ok := next.LoadMaterializedPredKey(emailKey)
	if !ok || cached != emailIDs {
		t.Fatal("expected untouched field cache entry to be inherited")
	}
}

func TestBuildPreparedPatchDuplicateTouchedFieldDropsTouchedCacheAndKeepsOthers(t *testing.T) {
	rt := buildCacheRuntime(t)
	oldRec := buildCacheRec{Name: "alice", Email: "alice@example.com"}
	prev := buildCacheSeedView(t, rt, &oldRec)
	defer prev.releaseRuntimeCaches()
	defer prev.releaseStorage()

	nameKey := qcache.MaterializedPredKeyForScalar("name", qir.OpPREFIX, "ali")
	emailKey := qcache.MaterializedPredKeyForScalar("email", qir.OpPREFIX, "ali")
	emailIDs := testPosting(1)
	prev.StoreMaterializedPredKey(nameKey, testPosting(1))
	prev.StoreMaterializedPredKey(emailKey, emailIDs)

	newRec := oldRec
	newRec.Name = "ally"
	next := buildCachePatchView(t, rt, prev, &oldRec, &newRec, []schema.PatchItem{
		{Name: "name", Value: "alicia"},
		{Name: "name", Value: "ally"},
	})
	defer next.releaseRuntimeCaches()
	defer next.releaseStorage()

	if next.HasMaterializedPredKey(nameKey) {
		t.Fatal("expected duplicate touched field cache entry to be dropped")
	}
	cached, ok := next.LoadMaterializedPredKey(emailKey)
	if !ok || cached != emailIDs {
		t.Fatal("expected duplicate touched patch to keep unrelated cache entry")
	}
}

func TestBuildPreparedPatchTouchedNilFieldDropsNilPredicateCache(t *testing.T) {
	rt := buildCacheRuntime(t)
	oldRec := buildCacheRec{Name: "alice", Email: "alice@example.com"}
	prev := buildCacheSeedView(t, rt, &oldRec)
	defer prev.releaseRuntimeCaches()
	defer prev.releaseStorage()

	optKey := qcache.MaterializedPredKeyForScalar("opt", qir.OpPREFIX, "z")
	emailKey := qcache.MaterializedPredKeyForScalar("email", qir.OpPREFIX, "ali")
	emailIDs := testPosting(1)
	prev.StoreMaterializedPredKey(optKey, posting.List{})
	prev.StoreMaterializedPredKey(emailKey, emailIDs)

	opt := "zzz"
	newRec := oldRec
	newRec.Opt = &opt
	next := buildCachePatchView(t, rt, prev, &oldRec, &newRec, []schema.PatchItem{{Name: "opt", Value: &opt}})
	defer next.releaseRuntimeCaches()
	defer next.releaseStorage()

	if next.HasMaterializedPredKey(optKey) {
		t.Fatal("expected touched nil-field cache entry to be dropped")
	}
	cached, ok := next.LoadMaterializedPredKey(emailKey)
	if !ok || cached != emailIDs {
		t.Fatal("expected untouched cache entry to survive opt patch")
	}
}

func TestBuildPreparedPatchUnchangedFieldInheritsObservedMaterializedPredWork(t *testing.T) {
	rt := buildCacheRuntime(t)
	oldRec := buildCacheRec{Name: "alice", Email: "alice@example.com", Age: 30}
	prev := buildCacheSeedView(t, rt, &oldRec)
	defer prev.releaseRuntimeCaches()
	defer prev.releaseStorage()

	key := qcache.MaterializedPredKeyForScalar("email", qir.OpPREFIX, "ali")
	if prev.ShouldPromoteObservedMaterializedPredKey(key, 10, 25) {
		t.Fatal("expected initial observed work to stay below promotion threshold")
	}
	if prev.ShouldPromoteRuntimeMaterializedPredKey(key) {
		t.Fatal("expected initial runtime seen key to stay cold")
	}

	newRec := oldRec
	newRec.Age = 31
	next := buildCachePatchView(t, rt, prev, &oldRec, &newRec, []schema.PatchItem{{Name: "age", Value: 31}})
	defer next.releaseRuntimeCaches()
	defer next.releaseStorage()

	if next.HasRuntimeMaterializedPredSeenKey(key) {
		t.Fatal("expected runtime seen keys not to inherit across snapshots")
	}
	if !next.ShouldPromoteObservedMaterializedPredKey(key, 15, 25) {
		t.Fatal("expected unchanged-field observed work to inherit across snapshots")
	}
}

func TestBuildPreparedPatchTouchedFieldSeparatesDirtyObservedMaterializedPredWork(t *testing.T) {
	rt := buildCacheRuntime(t)
	oldRec := buildCacheRec{Name: "alice", Email: "alice@example.com"}
	prev := buildCacheSeedView(t, rt, &oldRec)
	defer prev.releaseRuntimeCaches()
	defer prev.releaseStorage()

	key := qcache.MaterializedPredKeyForScalar("email", qir.OpPREFIX, "ali")
	secondKey := qcache.MaterializedPredKeyForScalar("email", qir.OpPREFIX, "bob")
	prev.StoreMaterializedPredKey(key, posting.List{})
	prev.StoreMaterializedPredKey(secondKey, posting.List{})
	if prev.ShouldPromoteObservedMaterializedPredKey(key, 10, 25) {
		t.Fatal("expected initial observed work to stay below promotion threshold")
	}
	if prev.ShouldPromoteObservedMaterializedPredKey(secondKey, 10, 25) {
		t.Fatal("expected second initial observed work to stay below promotion threshold")
	}
	if prev.ShouldPromoteRuntimeMaterializedPredKey(key) {
		t.Fatal("expected initial runtime seen key to stay cold")
	}

	newRec := oldRec
	newRec.Email = "ally@example.com"
	next := buildCachePatchView(t, rt, prev, &oldRec, &newRec, []schema.PatchItem{{Name: "email", Value: "ally@example.com"}})
	defer next.releaseRuntimeCaches()
	defer next.releaseStorage()

	if next.HasMaterializedPredKey(key) {
		t.Fatal("expected touched-field materialized predicate cache to be dropped")
	}
	if next.HasRuntimeMaterializedPredSeenKey(key) {
		t.Fatal("expected runtime seen keys not to inherit across snapshots")
	}
	if next.HasMaterializedPredKey(secondKey) {
		t.Fatal("expected second touched-field materialized predicate cache to be dropped")
	}
	if got := next.ObservedMaterializedPredWork(key); got != 0 {
		t.Fatalf("expected touched-field clean observed work to be dropped, got=%d", got)
	}
	if got := next.DirtyObservedMaterializedPredWork(key); got != 10 {
		t.Fatalf("expected touched-field dirty observed work to be retained as evidence, got=%d", got)
	}
	if next.ShouldPromoteObservedMaterializedPredKey(key, 20, 25) {
		t.Fatal("expected inherited dirty work not to participate in promotion")
	}
	if got := testRuntimeMatPredObservedEntryCount(next); got != 1 {
		t.Fatalf("expected only new observed work entry, got=%d", got)
	}
	if next.ShouldPromoteObservedMaterializedPredKey(secondKey, 30, 25) {
		t.Fatal("expected first clean sight for dirty touched field to stay local")
	}
	if !next.ShouldPromoteObservedMaterializedPredKey(secondKey, 1, 25) {
		t.Fatal("expected second clean sight in same snapshot to promote")
	}
}

func TestViewClearRuntimeCachesClearsCacheState(t *testing.T) {
	v := testMatPredView(64, 0)
	defer v.releaseRuntimeCaches()
	v.numericRangeBucketCache = qcache.GetNumericRangeBucketCache(1, 0)

	matKey := qcache.MaterializedPredKeyForScalar("email", qir.OpPREFIX, "a")
	v.StoreMaterializedPredKey(matKey, testPosting(1))
	if v.ShouldPromoteRuntimeMaterializedPredKey(matKey) {
		t.Fatal("expected first runtime promotion sight to stay local")
	}
	orKey := qcache.MaterializedPredKeyForScalar("age", qir.OpGTE, "30")
	if v.ShouldPromoteObservedMaterializedPredKey(orKey, 10, 25) {
		t.Fatal("expected first observed work below threshold to stay local")
	}
	if v.MaterializedPredCacheEntryCount() == 0 {
		t.Fatal("expected materialized cache entry before clear")
	}
	if v.RuntimeMaterializedPredSeenEntryCount() != 1 {
		t.Fatal("expected runtime seen-key entry before clear")
	}
	if testRuntimeMatPredObservedEntryCount(v) != 1 {
		t.Fatal("expected ordered OR observed-work entry before clear")
	}

	v.ClearRuntimeCaches()

	if got := v.MaterializedPredCacheEntryCount(); got != 0 {
		t.Fatalf("expected materialized cache entries to clear, got=%d", got)
	}
	if got := v.RuntimeMaterializedPredSeenEntryCount(); got != 0 {
		t.Fatalf("expected runtime seen-key cache to clear, got=%d", got)
	}
	if got := testRuntimeMatPredObservedEntryCount(v); got != 0 {
		t.Fatalf("expected ordered OR observed-work cache to clear, got=%d", got)
	}
}

func TestMaterializedPredInheritedBorrowedMutationDetaches(t *testing.T) {
	prev := testMatPredView(4, 0)
	defer prev.releaseRuntimeCaches()

	base := testLargePosting()
	want := base.ToArray()
	key := qcache.MaterializedPredKeyForScalar("email", qir.OpPREFIX, "user")
	prev.StoreMaterializedPredKey(key, base.Borrow())
	base.Release()

	next := testMatPredView(4, 0)
	defer next.releaseRuntimeCaches()
	inheritMaterializedPredCache(next, prev, nil, nil)

	fromPrev, ok := prev.LoadMaterializedPredKey(key)
	if !ok || fromPrev.IsEmpty() {
		t.Fatal("expected inherited materialized cache entry in prev snapshot")
	}
	defer fromPrev.Release()

	fromNext, ok := next.LoadMaterializedPredKey(key)
	if !ok || fromNext.IsEmpty() {
		t.Fatal("expected inherited materialized cache entry in next snapshot")
	}
	defer fromNext.Release()

	if !fromPrev.SharesPayload(fromNext) {
		t.Fatal("expected inherited materialized cache to reuse payload before mutation")
	}

	extra := uint64(1<<32 | 123)
	if fromPrev.Contains(extra) {
		t.Fatalf("test setup chose existing id %d", extra)
	}
	mutated := fromNext.BuildAdded(extra)
	defer mutated.Release()

	if mutated.SharesPayload(fromPrev) {
		t.Fatal("mutated inherited materialized posting still shares cache payload")
	}
	if !slices.Equal(fromPrev.ToArray(), want) {
		t.Fatalf("prev borrowed view changed after next mutation: got=%v want=%v", fromPrev.ToArray(), want)
	}
	if !mutated.Contains(extra) {
		t.Fatalf("mutated inherited materialized posting missing id=%d", extra)
	}

	reloadedPrev, ok := prev.LoadMaterializedPredKey(key)
	if !ok || reloadedPrev.IsEmpty() {
		t.Fatal("expected reloaded prev materialized cache entry")
	}
	defer reloadedPrev.Release()

	reloadedNext, ok := next.LoadMaterializedPredKey(key)
	if !ok || reloadedNext.IsEmpty() {
		t.Fatal("expected reloaded next materialized cache entry")
	}
	defer reloadedNext.Release()

	if !slices.Equal(reloadedPrev.ToArray(), want) {
		t.Fatalf("prev materialized cache was corrupted by detached mutation: got=%v want=%v", reloadedPrev.ToArray(), want)
	}
	if !slices.Equal(reloadedNext.ToArray(), want) {
		t.Fatalf("next materialized cache was corrupted by detached mutation: got=%v want=%v", reloadedNext.ToArray(), want)
	}
	if reloadedPrev.Contains(extra) || reloadedNext.Contains(extra) {
		t.Fatalf("inherited materialized cache retained detached mutation id=%d", extra)
	}
}

func TestNumericRangeInheritedBorrowedMutationDetaches(t *testing.T) {
	shared := testStorage("10", "20")
	defer shared.Release()

	entry := qcache.GetNumericRangeBucketEntry(shared, qcache.NumericRangeBucketIndex{}, 0)

	base := testLargePosting()
	want := base.ToArray()
	stored, ok := entry.TryStoreFullSpan(0, 0, base.Borrow())
	if !ok || stored.IsEmpty() {
		base.Release()
		entry.Release()
		t.Fatal("expected full-span cache store to succeed")
	}
	stored.Release()
	base.Release()

	prev := testView(map[string]indexdata.FieldStorage{"age": shared})
	prev.numericRangeBucketCache = qcache.GetNumericRangeBucketCache(1, 0)
	prev.numericRangeBucketCache.StoreSlot("age", 0, entry)
	defer prev.releaseRuntimeCaches()

	next := testView(map[string]indexdata.FieldStorage{"age": shared})
	next.numericRangeBucketCache = qcache.GetNumericRangeBucketCache(1, 0)
	defer next.releaseRuntimeCaches()

	inheritNumericRangeBucketCache(next, prev)

	prevEntry, ok := prev.numericRangeBucketCache.LoadField("age")
	if !ok || prevEntry == nil {
		t.Fatal("expected prev numeric range cache entry")
	}
	nextEntry, ok := next.numericRangeBucketCache.LoadField("age")
	if !ok || nextEntry == nil {
		t.Fatal("expected next numeric range cache entry")
	}
	if prevEntry != nextEntry {
		t.Fatal("expected inherited numeric range cache to reuse entry pointer")
	}

	fromPrev, ok := prevEntry.LoadFullSpan(0, 0)
	if !ok || fromPrev.IsEmpty() {
		t.Fatal("expected prev full-span cached posting")
	}
	defer fromPrev.Release()

	fromNext, ok := nextEntry.LoadFullSpan(0, 0)
	if !ok || fromNext.IsEmpty() {
		t.Fatal("expected next full-span cached posting")
	}
	defer fromNext.Release()

	if !fromPrev.SharesPayload(fromNext) {
		t.Fatal("expected inherited numeric full-span cache to reuse payload before mutation")
	}

	extra := uint64(1<<32 | 211)
	if fromPrev.Contains(extra) {
		t.Fatalf("test setup chose existing id %d", extra)
	}
	mutated := fromNext.BuildAdded(extra)
	defer mutated.Release()

	if mutated.SharesPayload(fromPrev) {
		t.Fatal("mutated inherited numeric full-span posting still shares cache payload")
	}
	if !slices.Equal(fromPrev.ToArray(), want) {
		t.Fatalf("prev numeric full-span view changed after next mutation: got=%v want=%v", fromPrev.ToArray(), want)
	}
	if !mutated.Contains(extra) {
		t.Fatalf("mutated numeric full-span posting missing id=%d", extra)
	}

	reloadedPrev, ok := prevEntry.LoadFullSpan(0, 0)
	if !ok || reloadedPrev.IsEmpty() {
		t.Fatal("expected reloaded prev numeric full-span posting")
	}
	defer reloadedPrev.Release()

	reloadedNext, ok := nextEntry.LoadFullSpan(0, 0)
	if !ok || reloadedNext.IsEmpty() {
		t.Fatal("expected reloaded next numeric full-span posting")
	}
	defer reloadedNext.Release()

	if !slices.Equal(reloadedPrev.ToArray(), want) {
		t.Fatalf("prev numeric full-span cache was corrupted by detached mutation: got=%v want=%v", reloadedPrev.ToArray(), want)
	}
	if !slices.Equal(reloadedNext.ToArray(), want) {
		t.Fatalf("next numeric full-span cache was corrupted by detached mutation: got=%v want=%v", reloadedNext.ToArray(), want)
	}
	if reloadedPrev.Contains(extra) || reloadedNext.Contains(extra) {
		t.Fatalf("inherited numeric full-span cache retained detached mutation id=%d", extra)
	}
}

func TestMaterializedPredInheritedReleaseKeepsSiblingSnapshotEntry(t *testing.T) {
	prev := testMatPredView(4, 0)
	defer prev.releaseRuntimeCaches()

	base := testLargePosting()
	want := base.ToArray()
	key := qcache.MaterializedPredKeyForScalar("email", qir.OpPREFIX, "user")
	prev.StoreMaterializedPredKey(key, base.Borrow())
	base.Release()

	next := testMatPredView(4, 0)
	defer next.releaseRuntimeCaches()
	inheritMaterializedPredCache(next, prev, nil, nil)

	held, ok := next.LoadMaterializedPredKey(key)
	if !ok || held.IsEmpty() {
		t.Fatal("expected inherited materialized cache entry in next snapshot")
	}
	defer held.Release()

	prev.releaseRuntimeCaches()

	reloaded, ok := next.LoadMaterializedPredKey(key)
	if !ok || reloaded.IsEmpty() {
		t.Fatal("expected sibling snapshot materialized cache entry after prev release")
	}
	defer reloaded.Release()

	if !slices.Equal(held.ToArray(), want) {
		t.Fatalf("held inherited materialized posting changed after prev release: got=%v want=%v", held.ToArray(), want)
	}
	if !slices.Equal(reloaded.ToArray(), want) {
		t.Fatalf("reloaded inherited materialized posting changed after prev release: got=%v want=%v", reloaded.ToArray(), want)
	}
}

func TestMaterializedPredInheritedEvictAndDrainKeepsSiblingSnapshotEntry(t *testing.T) {
	prev := testMatPredView(1, 0)
	defer prev.releaseRuntimeCaches()

	base := testLargePosting()
	want := base.ToArray()
	key := qcache.MaterializedPredKeyForScalar("email", qir.OpPREFIX, "user")
	prev.StoreMaterializedPredKey(key, base.Borrow())
	base.Release()

	next := testMatPredView(1, 0)
	defer next.releaseRuntimeCaches()
	inheritMaterializedPredCache(next, prev, nil, nil)

	held, ok := next.LoadMaterializedPredKey(key)
	if !ok || held.IsEmpty() {
		t.Fatal("expected inherited materialized cache entry in next snapshot")
	}
	defer held.Release()

	evictor := testLargePosting()
	extra := uint64(21<<32 | 37)
	if evictor.Contains(extra) {
		evictor.Release()
		t.Fatalf("test setup chose existing id %d", extra)
	}
	evictor = evictor.BuildAdded(extra)
	prev.StoreMaterializedPredKey(qcache.MaterializedPredKeyForScalar("email", qir.OpPREFIX, "other"), evictor.Borrow())
	evictor.Release()

	if prev.HasMaterializedPredKey(key) {
		t.Fatal("expected prev snapshot to evict old materialized entry")
	}
	prev.drainRetiredRuntimeCaches()

	reloaded, ok := next.LoadMaterializedPredKey(key)
	if !ok || reloaded.IsEmpty() {
		t.Fatal("expected sibling snapshot materialized cache entry after prev evict+drain")
	}
	defer reloaded.Release()

	if !slices.Equal(held.ToArray(), want) {
		t.Fatalf("held inherited materialized posting changed after prev evict+drain: got=%v want=%v", held.ToArray(), want)
	}
	if !slices.Equal(reloaded.ToArray(), want) {
		t.Fatalf("reloaded inherited materialized posting changed after prev evict+drain: got=%v want=%v", reloaded.ToArray(), want)
	}
}

func TestNumericRangeInheritedReleaseKeepsSiblingSnapshotEntry(t *testing.T) {
	shared := testStorage("10", "20")
	defer shared.Release()

	entry := qcache.GetNumericRangeBucketEntry(shared, qcache.NumericRangeBucketIndex{}, 0)

	base := testLargePosting()
	want := base.ToArray()
	stored, ok := entry.TryStoreFullSpan(0, 0, base.Borrow())
	if !ok || stored.IsEmpty() {
		base.Release()
		entry.Release()
		t.Fatal("expected full-span cache store to succeed")
	}
	stored.Release()
	base.Release()

	prev := testView(map[string]indexdata.FieldStorage{"age": shared})
	prev.numericRangeBucketCache = qcache.GetNumericRangeBucketCache(1, 0)
	prev.numericRangeBucketCache.StoreSlot("age", 0, entry)
	defer prev.releaseRuntimeCaches()

	next := testView(map[string]indexdata.FieldStorage{"age": shared})
	next.numericRangeBucketCache = qcache.GetNumericRangeBucketCache(1, 0)
	defer next.releaseRuntimeCaches()
	inheritNumericRangeBucketCache(next, prev)

	nextEntry, ok := next.numericRangeBucketCache.LoadField("age")
	if !ok || nextEntry == nil {
		t.Fatal("expected inherited numeric range cache entry in next snapshot")
	}

	held, ok := nextEntry.LoadFullSpan(0, 0)
	if !ok || held.IsEmpty() {
		t.Fatal("expected held inherited numeric full-span posting")
	}
	defer held.Release()

	prev.releaseRuntimeCaches()

	reloaded, ok := nextEntry.LoadFullSpan(0, 0)
	if !ok || reloaded.IsEmpty() {
		t.Fatal("expected sibling numeric full-span cache entry after prev release")
	}
	defer reloaded.Release()

	if !slices.Equal(held.ToArray(), want) {
		t.Fatalf("held inherited numeric full-span posting changed after prev release: got=%v want=%v", held.ToArray(), want)
	}
	if !slices.Equal(reloaded.ToArray(), want) {
		t.Fatalf("reloaded inherited numeric full-span posting changed after prev release: got=%v want=%v", reloaded.ToArray(), want)
	}
}

func TestManagerSameSeqPinnedOldSnapshotReleasesRetiredRuntimeCachesAfterLastUnpin(t *testing.T) {
	m := NewRegistry(true)

	first := testMatPredView(4, 0)
	first.Seq = 7
	first.numericRangeBucketCache = qcache.GetNumericRangeBucketCache(1, 0)
	first.StoreMaterializedPredKey(qcache.MaterializedPredKeyForScalar("email", qir.OpPREFIX, "user"), testPosting(1))
	m.Publish(first)

	pinned, ref, ok := m.PinBySeq(first.Seq)
	if !ok || pinned != first {
		t.Fatal("expected first snapshot to be pinnable")
	}

	second := testMatPredView(4, 0)
	second.Seq = first.Seq
	second.numericRangeBucketCache = qcache.GetNumericRangeBucketCache(1, 0)
	m.Publish(second)

	if first.MaterializedPredCache() == nil || first.NumericRangeBucketCache() == nil {
		t.Fatal("expected pinned retired same-seq snapshot to retain runtime caches before unpin")
	}

	m.Unpin(first.Seq, ref)
	if first.MaterializedPredCache() != nil || first.NumericRangeBucketCache() != nil {
		t.Fatal("expected retired same-seq snapshot runtime caches to be released after last unpin")
	}
	if m.Current() != second {
		t.Fatal("expected same-seq replacement to remain current after old unpin")
	}

	second.releaseRuntimeCaches()
}

func TestManagerReDrainsNumericRetiredAfterSharedSnapshotRelease(t *testing.T) {
	m := NewRegistry(true)

	shared := testStorage("10", "20")
	defer shared.Release()

	first := testView(map[string]indexdata.FieldStorage{"age": shared})
	first.Seq = 17
	first.numericRangeBucketCache = qcache.GetNumericRangeBucketCache(1, 0)
	entry := qcache.GetNumericRangeBucketEntry(shared, qcache.NumericRangeBucketIndex{}, 0)
	first.numericRangeBucketCache.StoreSlot("age", 0, entry)
	m.Publish(first)

	pinnedOld, oldRef, ok := m.PinBySeq(first.Seq)
	if !ok || pinnedOld != first {
		t.Fatal("expected first snapshot to be pinned")
	}

	second := testView(map[string]indexdata.FieldStorage{"age": shared})
	second.Seq = 18
	second.numericRangeBucketCache = qcache.GetNumericRangeBucketCache(1, 0)
	inheritNumericRangeBucketCache(second, first)
	m.Publish(second)

	pinnedCurrent, currentSeq, currentRef := m.PinCurrent()
	if pinnedCurrent != second || currentRef == nil {
		t.Fatal("expected second snapshot to be current")
	}

	evicted := false
	count := entry.FullSpanEntryCount()
	for i := 0; !evicted && i < 64; i++ {
		ids := testPosting(uint64(i+1), 1<<32|uint64(i+1))
		cached, ok := entry.TryStoreFullSpan(i, i, ids)
		if !ok {
			t.Fatalf("expected full-span store %d to succeed", i)
		}
		cached.Release()
		nextCount := entry.FullSpanEntryCount()
		evicted = nextCount == count
		count = nextCount
	}
	if !evicted {
		t.Fatal("expected shared numeric entry to have retired full-span postings")
	}

	m.Unpin(currentSeq, currentRef)
	retained := second.numericRangeBucketCache.TakeRetired()
	if !retained.IsEmpty() {
		retained.Release()
		t.Fatal("expected current unpin to keep retired postings while entry is shared")
	}
	retained.Release()
	if entry.FullSpanEntryCount() == 0 {
		t.Fatal("expected current unpin to keep retired postings while entry is shared")
	}

	m.Unpin(first.Seq, oldRef)
	redrained := second.numericRangeBucketCache.TakeRetired()
	if !redrained.IsEmpty() {
		redrained.Release()
		t.Fatal("expected old snapshot release to re-drain current numeric retired postings")
	}
	redrained.Release()

	second.releaseRuntimeCaches()
}
