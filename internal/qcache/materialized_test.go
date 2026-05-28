package qcache

import (
	"fmt"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qir"
)

func requireZeroAllocsAfterWarmupQCache(t *testing.T, fn func()) {
	t.Helper()
	if testRaceEnabled {
		t.Skip("testing.AllocsPerRun is not stable under -race")
	}
	for i := 0; i < 32; i++ {
		fn()
	}
	if allocs := testing.AllocsPerRun(100, fn); allocs != 0 {
		t.Fatalf("unexpected allocs after pool warmup: got=%v want=0", allocs)
	}
}

func qcacheTestPosting(ids ...uint64) posting.List {
	var out posting.List
	for _, id := range ids {
		out = out.BuildAdded(id)
	}
	return out
}

func qcacheTestLargePosting() posting.List {
	ids := make([]uint64, 0, posting.MidCap+16)
	for i := uint64(0); i < uint64(posting.MidCap+16); i++ {
		ids = append(ids, i*3+1)
	}
	return posting.BuildFromSorted(ids)
}

func qcacheTestAssertPostingSet(t *testing.T, got posting.List, want []uint64) {
	t.Helper()
	gotIDs := got.ToArray()
	if !slices.Equal(gotIDs, want) {
		t.Fatalf("posting mismatch: got=%v want=%v", gotIDs, want)
	}
}

func qcacheTestRunConcurrent(n int, fn func(i int)) {
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn(i)
		}()
	}
	wg.Wait()
}

func TestRecentKeyCache_AllocsPerRunStayZeroAfterWarmup(t *testing.T) {
	var cache RecentKeyCache
	keys := [...]MaterializedPredKey{
		MaterializedPredKeyForScalar("email", qir.OpPREFIX, "user"),
		MaterializedPredKeyForScalar("email", qir.OpPREFIX, "admin"),
		MaterializedPredKeyForScalar("name", qir.OpSUFFIX, "son"),
		MaterializedPredKeyForScalar("name", qir.OpSUFFIX, "ov"),
		MaterializedPredKeyForScalar("bio", qir.OpCONTAINS, "ops"),
		MaterializedPredKeyForScalar("bio", qir.OpCONTAINS, "db"),
	}
	limit := len(keys)

	run := func() {
		cache.Clear()
		for i := range keys {
			if cache.TouchOrRemember(keys[i], limit) {
				t.Fatalf("TouchOrRemember unexpectedly reported warm hit for cold key %v", keys[i])
			}
		}
		for i := range keys {
			if !cache.TouchOrRemember(keys[i], limit) {
				t.Fatalf("TouchOrRemember missed existing key %v", keys[i])
			}
		}
		cache.Clear()
		for i := range keys {
			if promote, _ := cache.AddWorkAndShouldPromote(keys[i], limit, 1, 2); promote {
				t.Fatalf("AddWorkAndShouldPromote promoted too early for key %v", keys[i])
			}
		}
		for i := range keys {
			if promote, _ := cache.AddWorkAndShouldPromote(keys[i], limit, 1, 2); !promote {
				t.Fatalf("AddWorkAndShouldPromote failed to promote key %v", keys[i])
			}
		}
	}

	requireZeroAllocsAfterWarmupQCache(t, run)
}

func TestRecentKeyCache_ShortLivedOwnersReuseIndexMapAfterWarmup(t *testing.T) {
	keys := qcacheBenchKeys(32)
	var seen RecentKeyCache
	var observed RecentKeyCache

	run := func() {
		for i := range keys {
			if seen.TouchOrRemember(keys[i], len(keys)) {
				t.Fatalf("TouchOrRemember unexpectedly reported warm hit for cold key %v", keys[i])
			}
			if promote, _ := observed.AddWorkAndShouldPromote(keys[i], len(keys), 1, ^uint64(0)); promote {
				t.Fatalf("AddWorkAndShouldPromote unexpectedly promoted key %v", keys[i])
			}
		}
		seen.Clear()
		observed.Clear()
		seen = RecentKeyCache{}
		observed = RecentKeyCache{}
	}

	requireZeroAllocsAfterWarmupQCache(t, run)
}

func TestRecentKeyCache_LimitAndEntryCount(t *testing.T) {
	if got := RecentKeyLimit(0); got != 0 {
		t.Fatalf("RecentKeyLimit(0)=%d want 0", got)
	}
	if got := RecentKeyLimit(4); got != 5 {
		t.Fatalf("RecentKeyLimit(4)=%d want 5", got)
	}
	if got := RecentKeyLimit(32); got != 36 {
		t.Fatalf("RecentKeyLimit(32)=%d want 36", got)
	}

	var cache RecentKeyCache
	keyA := MaterializedPredKeyForScalar("email", qir.OpPREFIX, "a")
	keyB := MaterializedPredKeyForScalar("email", qir.OpPREFIX, "b")
	if cache.TouchOrRemember(keyA, 2) {
		t.Fatal("expected cold key to be remembered")
	}
	if cache.TouchOrRemember(keyB, 2) {
		t.Fatal("expected second cold key to be remembered")
	}
	if got := cache.EntryCount(); got != 2 {
		t.Fatalf("EntryCount=%d want 2", got)
	}
	promote, hadWork := cache.AddWorkAndShouldPromote(keyA, 2, 1, 3)
	if promote {
		t.Fatal("expected first observed work increment to stay below threshold")
	}
	if hadWork {
		t.Fatal("expected first observed work increment to have no prior work")
	}
	promote, hadWork = cache.AddWorkAndShouldPromote(keyA, 2, 2, 3)
	if !promote {
		t.Fatal("expected accumulated work to promote key")
	}
	if !hadWork {
		t.Fatal("expected accumulated work increment to report prior work")
	}
	if got := cache.EntryCount(); got != 2 {
		t.Fatalf("EntryCount after promotion=%d want 2", got)
	}
	if got := cache.Work(keyA); got != 3 {
		t.Fatalf("observed work after promotion=%d want 3", got)
	}
	cache.Clear()
}

func TestMaterializedPredCache_StoreBorrowedDetachesFromSourceOwner(t *testing.T) {
	cache := GetMaterializedPredCache(4, 0)
	defer cache.ReleaseRef()

	key := MaterializedPredKeyForScalar("email", qir.OpPREFIX, "user")
	base := qcacheTestLargePosting()
	want := base.ToArray()

	cache.Store(key, base.Borrow())

	cached, ok := cache.Load(key)
	if !ok || cached.IsEmpty() {
		base.Release()
		t.Fatal("expected cached materialized predicate")
	}
	if cached.SharesPayload(base) {
		cached.Release()
		base.Release()
		t.Fatal("borrowed materialized predicate store kept source payload")
	}

	extra := uint64(1<<32 | 77)
	if base.Contains(extra) {
		cached.Release()
		base.Release()
		t.Fatalf("test setup chose existing id %d", extra)
	}
	base = base.BuildAdded(extra)

	reloaded, ok := cache.Load(key)
	if !ok || reloaded.IsEmpty() {
		cached.Release()
		base.Release()
		t.Fatal("expected cached materialized predicate after source mutation")
	}

	if !slices.Equal(cached.ToArray(), want) {
		reloaded.Release()
		cached.Release()
		base.Release()
		t.Fatalf("cached borrowed view changed after source mutation: got=%v want=%v", cached.ToArray(), want)
	}
	if !slices.Equal(reloaded.ToArray(), want) {
		reloaded.Release()
		cached.Release()
		base.Release()
		t.Fatalf("reloaded materialized predicate changed after source mutation: got=%v want=%v", reloaded.ToArray(), want)
	}
	if reloaded.Contains(extra) {
		reloaded.Release()
		cached.Release()
		base.Release()
		t.Fatalf("materialized predicate cache leaked source mutation id=%d", extra)
	}

	reloaded.Release()
	cached.Release()
	base.Release()
}

func TestMaterializedPredCache_LoadOrStoreHitReturnsCachedPayloadNotCaller(t *testing.T) {
	cache := GetMaterializedPredCache(4, 0)
	defer cache.ReleaseRef()

	key := MaterializedPredKeyForScalar("email", qir.OpPREFIX, "user")
	cachedBase := qcacheTestLargePosting()
	want := cachedBase.ToArray()
	cache.Store(key, cachedBase.Borrow())

	cached, ok := cache.Load(key)
	if !ok || cached.IsEmpty() {
		cachedBase.Release()
		t.Fatal("expected seeded cached materialized predicate")
	}
	defer cached.Release()

	source := qcacheTestLargePosting()
	extraSource := uint64(3<<32 | 17)
	if source.Contains(extraSource) {
		source.Release()
		cachedBase.Release()
		t.Fatalf("test setup chose existing id %d", extraSource)
	}
	source = source.BuildAdded(extraSource)

	got, ok := cache.LoadOrStore(key, source.Borrow())
	if !ok || got.IsEmpty() {
		source.Release()
		cachedBase.Release()
		t.Fatal("expected cache hit from LoadOrStore")
	}
	defer got.Release()

	if !got.SharesPayload(cached) {
		source.Release()
		cachedBase.Release()
		t.Fatal("LoadOrStore hit did not return cached payload")
	}
	if got.SharesPayload(source) {
		source.Release()
		cachedBase.Release()
		t.Fatal("LoadOrStore hit reused caller payload instead of cached payload")
	}

	extraMut := uint64(5<<32 | 19)
	if source.Contains(extraMut) {
		source.Release()
		cachedBase.Release()
		t.Fatalf("test setup chose existing id %d", extraMut)
	}
	source = source.BuildAdded(extraMut)

	reloaded, ok := cache.Load(key)
	if !ok || reloaded.IsEmpty() {
		source.Release()
		cachedBase.Release()
		t.Fatal("expected reloaded cached materialized predicate after hit")
	}
	defer reloaded.Release()

	if !slices.Equal(got.ToArray(), want) {
		source.Release()
		cachedBase.Release()
		t.Fatalf("LoadOrStore hit result changed after caller mutation: got=%v want=%v", got.ToArray(), want)
	}
	if !slices.Equal(reloaded.ToArray(), want) {
		source.Release()
		cachedBase.Release()
		t.Fatalf("cached materialized predicate changed after caller mutation: got=%v want=%v", reloaded.ToArray(), want)
	}
	if reloaded.Contains(extraSource) || reloaded.Contains(extraMut) {
		source.Release()
		cachedBase.Release()
		t.Fatalf("cached materialized predicate leaked caller ids: source=%d mut=%d", extraSource, extraMut)
	}

	source.Release()
	cachedBase.Release()
}

func TestMaterializedPredCache_OversizedLoadOrStoreHitReturnsCachedPayloadNotCaller(t *testing.T) {
	cache := GetMaterializedPredCache(4, 1)
	defer cache.ReleaseRef()

	key := MaterializedPredKeyForNumericBucketSpan("age", 3, 7)
	cachedBase := qcacheTestLargePosting()
	want := cachedBase.ToArray()
	got, ok := cache.TryLoadOrStoreOversized(key, cachedBase.Borrow())
	if !ok || got.IsEmpty() {
		cachedBase.Release()
		t.Fatal("expected oversized miss path to store cached posting")
	}
	if got.SharesPayload(cachedBase) {
		got.Release()
		cachedBase.Release()
		t.Fatal("oversized miss path kept caller payload")
	}
	got.Release()

	cached, ok := cache.Load(key)
	if !ok || cached.IsEmpty() {
		cachedBase.Release()
		t.Fatal("expected seeded oversized cached posting")
	}
	defer cached.Release()

	source := qcacheTestLargePosting()
	extraSource := uint64(11<<32 | 29)
	if source.Contains(extraSource) {
		source.Release()
		cachedBase.Release()
		t.Fatalf("test setup chose existing id %d", extraSource)
	}
	source = source.BuildAdded(extraSource)

	hit, ok := cache.TryLoadOrStoreOversized(key, source.Borrow())
	if !ok || hit.IsEmpty() {
		source.Release()
		cachedBase.Release()
		t.Fatal("expected oversized LoadOrStore hit")
	}
	defer hit.Release()

	if !hit.SharesPayload(cached) {
		source.Release()
		cachedBase.Release()
		t.Fatal("oversized LoadOrStore hit did not return cached payload")
	}
	if hit.SharesPayload(source) {
		source.Release()
		cachedBase.Release()
		t.Fatal("oversized LoadOrStore hit reused caller payload instead of cached payload")
	}

	extraMut := uint64(13<<32 | 31)
	if source.Contains(extraMut) {
		source.Release()
		cachedBase.Release()
		t.Fatalf("test setup chose existing id %d", extraMut)
	}
	source = source.BuildAdded(extraMut)

	reloaded, ok := cache.Load(key)
	if !ok || reloaded.IsEmpty() {
		source.Release()
		cachedBase.Release()
		t.Fatal("expected reloaded oversized cached posting after hit")
	}
	defer reloaded.Release()

	if !slices.Equal(hit.ToArray(), want) {
		source.Release()
		cachedBase.Release()
		t.Fatalf("oversized LoadOrStore hit result changed after caller mutation: got=%v want=%v", hit.ToArray(), want)
	}
	if !slices.Equal(reloaded.ToArray(), want) {
		source.Release()
		cachedBase.Release()
		t.Fatalf("oversized cached posting changed after caller mutation: got=%v want=%v", reloaded.ToArray(), want)
	}
	if reloaded.Contains(extraSource) || reloaded.Contains(extraMut) {
		source.Release()
		cachedBase.Release()
		t.Fatalf("oversized cached posting leaked caller ids: source=%d mut=%d", extraSource, extraMut)
	}

	source.Release()
	cachedBase.Release()
}

func TestMaterializedPredCache_ConcurrentDistinctKeysRespectsLimit(t *testing.T) {
	n := max(16, runtime.GOMAXPROCS(0)*8)
	for round := 0; round < 16; round++ {
		cache := GetMaterializedPredCache(1, 0)
		qcacheTestRunConcurrent(n, func(i int) {
			cache.Store(MaterializedPredKeyForScalar("email", qir.OpPREFIX, fmt.Sprintf("%d", i)), posting.List{})
		})
		if got := cache.EntryCount(); got > 1 {
			cache.ReleaseRef()
			t.Fatalf("round=%d materialized cache count exceeded limit: got=%d", round, got)
		}
		cache.ReleaseRef()
	}
}

func TestMaterializedPredCache_ConcurrentSameKeyStoresSingleEntry(t *testing.T) {
	n := max(16, runtime.GOMAXPROCS(0)*8)
	cache := GetMaterializedPredCache(8, 0)
	defer cache.ReleaseRef()

	key := MaterializedPredKeyForScalar("email", qir.OpPREFIX, "same")
	qcacheTestRunConcurrent(n, func(i int) {
		cache.Store(key, posting.List{})
	})

	if got := cache.EntryCount(); got != 1 {
		t.Fatalf("expected single cache count for duplicate key, got=%d", got)
	}
}

func TestMaterializedPredCache_OversizedConcurrentDistinctKeysRespectsTotalLimit(t *testing.T) {
	n := max(16, runtime.GOMAXPROCS(0)*8)
	for round := 0; round < 16; round++ {
		cache := GetMaterializedPredCache(1, 1)
		qcacheTestRunConcurrent(n, func(i int) {
			ids := qcacheTestPosting(1, 2)
			if !cache.TryStoreOversized(MaterializedPredKeyForNumericBucketSpan("age", i, i), ids) {
				ids.Release()
			}
		})
		if got := cache.EntryCount(); got > 1 {
			cache.ReleaseRef()
			t.Fatalf("round=%d oversized cache count exceeded limit: got=%d", round, got)
		}
		if got := cache.OversizedCount(); got > 1 {
			cache.ReleaseRef()
			t.Fatalf("round=%d oversized counter exceeded total limit: got=%d", round, got)
		}
		cache.ReleaseRef()
	}
}

func TestMaterializedPredCache_OversizedConcurrentSameKeyDoesNotLeakCounters(t *testing.T) {
	n := max(16, runtime.GOMAXPROCS(0)*8)
	cache := GetMaterializedPredCache(8, 1)
	defer cache.ReleaseRef()

	key := MaterializedPredKeyForNumericBucketSpan("age", 1, 1)
	qcacheTestRunConcurrent(n, func(i int) {
		ids := qcacheTestPosting(1, 2)
		if !cache.TryStoreOversized(key, ids) {
			ids.Release()
		}
	})

	if got := cache.EntryCount(); got != 1 {
		t.Fatalf("expected single oversized cache count for duplicate key, got=%d", got)
	}
	if got := cache.OversizedCount(); got != 1 {
		t.Fatalf("expected oversized counter to settle at 1, got=%d", got)
	}
}

func TestMaterializedPredCache_OversizedAdmissionTurnsOverEntries(t *testing.T) {
	cache := GetMaterializedPredCache(8, 1)
	defer cache.ReleaseRef()

	small := qcacheTestPosting(1)
	large := qcacheTestPosting(1, 2)
	defer small.Release()
	defer large.Release()

	for i := 0; i < 8; i++ {
		cache.Store(MaterializedPredKeyForScalar("email", qir.OpPREFIX, fmt.Sprintf("%d", i)), small.Borrow())
	}
	if got := cache.EntryCount(); got != 8 {
		t.Fatalf("expected regular cache to fill total limit, got=%d", got)
	}

	if !cache.TryStoreOversized(MaterializedPredKeyForNumericBucketSpan("age", 0, 0), large.Borrow()) {
		t.Fatal("expected first oversized cache entry to replace an older entry")
	}
	if !cache.TryStoreOversized(MaterializedPredKeyForNumericBucketSpan("age", 1, 1), large.Borrow()) {
		t.Fatal("expected second oversized cache entry to replace an older entry")
	}
	if !cache.TryStoreOversized(MaterializedPredKeyForNumericBucketSpan("age", 2, 2), large.Borrow()) {
		t.Fatal("expected oversized cache admission to replace an older oversized entry at limit")
	}

	if got := cache.EntryCount(); got != 8 {
		t.Fatalf("expected total cache count to remain bounded by limit, got=%d", got)
	}
	if got := cache.OversizedCount(); got != 2 {
		t.Fatalf("expected oversized counter to settle at 2, got=%d", got)
	}
	if ids, ok := cache.Load(MaterializedPredKeyForNumericBucketSpan("age", 2, 2)); !ok {
		t.Fatal("expected newest oversized entry to remain cached")
	} else {
		ids.Release()
	}
	kept := 0
	for i := 0; i < 3; i++ {
		if ids, ok := cache.Load(MaterializedPredKeyForNumericBucketSpan("age", i, i)); ok {
			kept++
			ids.Release()
		}
	}
	if kept != 2 {
		t.Fatalf("expected oversized cache to keep exactly two entries after replacement, got=%d", kept)
	}
}

func TestMaterializedPredCache_RegularAdmissionTurnsOverOlderRegularEntries(t *testing.T) {
	cache := GetMaterializedPredCache(8, 1)
	defer cache.ReleaseRef()

	small := qcacheTestPosting(1)
	large := qcacheTestPosting(1, 2)
	defer small.Release()
	defer large.Release()

	for i := 0; i < 8; i++ {
		cache.Store(MaterializedPredKeyForScalar("email", qir.OpPREFIX, fmt.Sprintf("%d", i)), small.Borrow())
	}
	if !cache.TryStoreOversized(MaterializedPredKeyForNumericBucketSpan("age", 0, 0), large.Borrow()) {
		t.Fatal("expected first oversized cache entry to be admitted")
	}
	if !cache.TryStoreOversized(MaterializedPredKeyForNumericBucketSpan("age", 1, 1), large.Borrow()) {
		t.Fatal("expected second oversized cache entry to be admitted")
	}

	lateKey := MaterializedPredKeyForScalar("email", qir.OpPREFIX, "late")
	cache.Store(lateKey, small.Borrow())

	if got := cache.EntryCount(); got != 8 {
		t.Fatalf("expected total cache count to remain bounded by limit, got=%d", got)
	}
	if got := cache.OversizedCount(); got != 2 {
		t.Fatalf("expected regular turnover to keep oversized entries resident, got=%d", got)
	}
	if ids, ok := cache.Load(lateKey); !ok {
		t.Fatal("expected newest regular entry to be cached")
	} else {
		ids.Release()
	}
}

func TestMaterializedPredCache_BorrowedViewSurvivesConcurrentEviction(t *testing.T) {
	cache := GetMaterializedPredCache(1, 0)
	defer cache.ReleaseRef()

	base := qcacheTestPosting()
	for i := uint64(1); i <= 48; i++ {
		base = base.BuildAdded(i * 7)
	}
	want := base.ToArray()

	holdKey := MaterializedPredKeyFromOpaque("hold")
	cache.Store(holdKey, base.Borrow())
	base.Release()

	held, ok := cache.Load(holdKey)
	if !ok || held.IsEmpty() {
		t.Fatal("expected held cache entry")
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
					setFailed(fmt.Sprintf("held materialized posting changed under eviction: got=%v want=%v", held.ToArray(), want))
					return
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 400; i++ {
			ids := qcacheTestPosting(uint64(i+1), 1<<32|uint64(i+1))
			cache.Store(MaterializedPredKeyFromOpaque(fmt.Sprintf("evict-%d", i)), ids)
		}
	}()

	close(start)
	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}

	qcacheTestAssertPostingSet(t, held, want)
}

func TestMaterializedPredCache_RetainDrainRetiredAndMaxCardinality(t *testing.T) {
	cache := GetMaterializedPredCache(1, 7)
	if got := cache.MaxCardinality(); got != 7 {
		cache.ReleaseRef()
		t.Fatalf("MaxCardinality=%d want 7", got)
	}
	cache.Retain()
	cache.ReleaseRef()

	keyA := MaterializedPredKeyForScalar("email", qir.OpPREFIX, "a")
	keyB := MaterializedPredKeyForScalar("email", qir.OpPREFIX, "b")
	idsA := qcacheTestPosting(1)
	idsB := qcacheTestPosting(2)
	defer idsA.Release()
	defer idsB.Release()

	cache.Store(keyA, idsA.Borrow())
	cache.Store(keyB, idsB.Borrow())
	if len(cache.retired) == 0 {
		cache.ReleaseRef()
		t.Fatal("expected evicted entry to be retired before drain")
	}
	cache.DrainRetired()
	if cache.retired != nil {
		cache.ReleaseRef()
		t.Fatal("expected retired entries to be drained")
	}
	cache.ReleaseRef()
}

func TestMaterializedPredCacheEntryPool_AllocsPerRunStayZeroAfterWarmup(t *testing.T) {
	run := func() {
		var entries [4]*materializedPredCacheEntry
		for i := range entries {
			entries[i] = materializedPredCacheEntryPool.Get()
		}
		for i := range entries {
			materializedPredCacheEntryPool.Put(entries[i])
		}
	}

	requireZeroAllocsAfterWarmupQCache(t, run)
}

func TestMaterializedPredCache_InsertNilEntriesAllocsPerRunStayZeroAfterWarmup(t *testing.T) {
	cache := GetMaterializedPredCache(8, 0)
	defer cache.ReleaseRef()

	keys := [...]MaterializedPredKey{
		MaterializedPredKeyForScalar("email", qir.OpPREFIX, "user"),
		MaterializedPredKeyForScalar("email", qir.OpPREFIX, "admin"),
		MaterializedPredKeyForScalar("name", qir.OpSUFFIX, "son"),
		MaterializedPredKeyForScalar("bio", qir.OpCONTAINS, "ops"),
	}

	run := func() {
		cache.Clear()
		cache.mu.Lock()
		for i := range keys {
			if !cache.insertLocked(keys[i], nil) {
				cache.mu.Unlock()
				t.Fatalf("insertLocked(%v) failed", keys[i])
			}
		}
		cache.mu.Unlock()
	}

	requireZeroAllocsAfterWarmupQCache(t, run)
}

func TestMaterializedPredCache_LoadOrStoreAllocsPerRunStayZeroAfterWarmup(t *testing.T) {
	cache := GetMaterializedPredCache(8, ^uint64(0))
	defer cache.ReleaseRef()

	keys := [...]MaterializedPredKey{
		MaterializedPredKeyForScalar("email", qir.OpPREFIX, "user"),
		MaterializedPredKeyForScalar("email", qir.OpPREFIX, "admin"),
		MaterializedPredKeyForScalar("name", qir.OpSUFFIX, "son"),
		MaterializedPredKeyForScalar("bio", qir.OpCONTAINS, "ops"),
	}
	posts := [...]posting.List{
		posting.BuildFromSorted([]uint64{3, 9, 17, 25}),
		posting.BuildFromSorted([]uint64{5, 7, 11, 13, 19, 23, 29, 31, 37, 41, 43, 47}),
		qcacheTestLargePosting(),
		posting.BuildFromSorted([]uint64{1<<32 | 3, 1<<32 | 7, 2<<32 | 9, 2<<32 | 15}),
	}
	defer posts[0].Release()
	defer posts[1].Release()
	defer posts[2].Release()
	defer posts[3].Release()

	run := func() {
		cache.Clear()
		for i := range keys {
			got, ok := cache.LoadOrStore(keys[i], posts[i].Borrow())
			if !ok {
				t.Fatalf("LoadOrStore(%v) did not cache entry", keys[i])
			}
			got.Release()
		}
	}

	requireZeroAllocsAfterWarmupQCache(t, run)
}

func TestMaterializedPredCache_InheritTypedNilEntry(t *testing.T) {
	prev := GetMaterializedPredCache(8, 0)
	next := GetMaterializedPredCache(8, 0)
	defer prev.ReleaseRef()
	defer next.ReleaseRef()

	key := MaterializedPredKeyForScalar("name", qir.OpPREFIX, "a")
	prev.mu.Lock()
	if !prev.insertLocked(key, nil) {
		prev.mu.Unlock()
		t.Fatalf("insertLocked(%v) failed", key)
	}
	prev.count.Add(1)
	prev.mu.Unlock()

	next.InheritFrom(prev, nil, nil)

	if got := next.EntryCount(); got != 1 {
		t.Fatalf("expected typed-nil entry to be inherited as negative cache entry, got=%d", got)
	}
	cached, ok := next.Load(key)
	if !ok || !cached.IsEmpty() {
		t.Fatalf("expected typed-nil entry to survive as empty negative cache entry")
	}
}

func TestMaterializedPredCache_InheritClampsOversizedCount(t *testing.T) {
	prev := GetMaterializedPredCache(8, 0)
	next := GetMaterializedPredCache(8, 1)
	defer prev.ReleaseRef()
	defer next.ReleaseRef()

	for _, key := range [...]MaterializedPredKey{
		MaterializedPredKeyForScalar("a", qir.OpPREFIX, "1"),
		MaterializedPredKeyForScalar("b", qir.OpPREFIX, "2"),
		MaterializedPredKeyForScalar("c", qir.OpPREFIX, "3"),
	} {
		ids := posting.BuildFromSorted([]uint64{1, 2})
		entry := newMaterializedPredCacheEntry(ids, true, &prev.clock)
		prev.mu.Lock()
		if !prev.insertLocked(key, entry) {
			prev.mu.Unlock()
			t.Fatalf("insertLocked(%v) failed", key)
		}
		prev.count.Add(1)
		prev.oversizedCount.Add(1)
		prev.mu.Unlock()
	}

	next.InheritFrom(prev, nil, nil)

	if got := next.EntryCount(); got != 3 {
		t.Fatalf("expected all entries to be inherited before oversized clamp, got=%d", got)
	}
	if want, got := MaterializedPredOversizedLimit(next.Limit()), next.OversizedCount(); got != want {
		t.Fatalf("expected oversized counter clamp to %d, got=%d", want, got)
	}
}

func TestMaterializedPredCache_InheritPreservesRecencyStamps(t *testing.T) {
	prev := GetMaterializedPredCache(8, 0)
	next := GetMaterializedPredCache(8, 0)
	defer prev.ReleaseRef()
	defer next.ReleaseRef()

	keyA := MaterializedPredKeyForScalar("email", qir.OpPREFIX, "a")
	keyB := MaterializedPredKeyForScalar("name", qir.OpPREFIX, "b")
	entryA := newMaterializedPredCacheEntry(posting.BuildFromSorted([]uint64{1}), false, &prev.clock)
	entryA.stamp.Store(7)
	entryB := newMaterializedPredCacheEntry(posting.BuildFromSorted([]uint64{2}), false, &prev.clock)
	entryB.stamp.Store(13)
	prev.clock.Store(13)

	prev.mu.Lock()
	if !prev.insertLocked(keyA, entryA) || !prev.insertLocked(keyB, entryB) {
		prev.mu.Unlock()
		t.Fatal("failed to insert inherited entries")
	}
	prev.count.Add(2)
	prev.mu.Unlock()

	next.InheritFrom(prev, nil, nil)

	next.mu.RLock()
	gotA, okA := next.lookupLocked(&keyA)
	gotB, okB := next.lookupLocked(&keyB)
	next.mu.RUnlock()
	if !okA || gotA == nil || gotA.stamp.Load() != 7 {
		t.Fatalf("expected first inherited stamp to stay at 7")
	}
	if !okB || gotB == nil || gotB.stamp.Load() != 13 {
		t.Fatalf("expected second inherited stamp to stay at 13")
	}
	if got := next.Clock(); got != 13 {
		t.Fatalf("expected inherited clock to continue from max stamp, got=%d", got)
	}
}
