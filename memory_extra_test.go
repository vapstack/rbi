package rbi

import (
	"github.com/vapstack/rbi/internal/indexdata"
	"slices"
	"testing"

	"github.com/vapstack/qx"
)

func TestMemoryExtra_MaterializedPredCacheStoreBorrowedDetachesFromSourceOwner(t *testing.T) {
	snap := &indexSnapshot{
		matPredCacheMaxEntries: 4,
	}
	snapshotExtInitMaterializedPredCache(snap)
	defer snap.releaseRuntimeCaches()

	key := materializedPredKeyForScalar("email", compileScalarOpForTest(qx.OpPREFIX), "user")
	base := buildQueryRuntimeTestLargePosting()
	want := base.ToArray()

	snap.storeMaterializedPredKey(key, base.Borrow())

	cached, ok := snap.loadMaterializedPredKey(key)
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

	reloaded, ok := snap.loadMaterializedPredKey(key)
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

func TestMemoryExtra_NumericRangeFullSpanStoreBorrowedDetachesFromSourceOwner(t *testing.T) {
	entry := numericRangeBucketCacheEntryPool.Get()
	entry.refs.Store(1)
	defer numericRangeBucketCacheEntryPool.Put(entry)

	base := buildQueryRuntimeTestLargePosting()
	want := base.ToArray()

	stored, ok := entry.tryStoreFullSpan(3, 7, base.Borrow())
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

	cached, ok := entry.loadFullSpan(3, 7)
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

func TestMemoryExtra_MaterializedPredLoadOrStoreHitReturnsCachedPayloadNotCaller(t *testing.T) {
	snap := &indexSnapshot{
		matPredCacheMaxEntries: 4,
	}
	snapshotExtInitMaterializedPredCache(snap)
	defer snap.releaseRuntimeCaches()

	key := materializedPredKeyForScalar("email", compileScalarOpForTest(qx.OpPREFIX), "user")
	cachedBase := buildQueryRuntimeTestLargePosting()
	want := cachedBase.ToArray()
	snap.storeMaterializedPredKey(key, cachedBase.Borrow())

	cached, ok := snap.loadMaterializedPredKey(key)
	if !ok || cached.IsEmpty() {
		cachedBase.Release()
		t.Fatal("expected seeded cached materialized predicate")
	}
	defer cached.Release()

	source := buildQueryRuntimeTestLargePosting()
	extraSource := uint64(3<<32 | 17)
	if source.Contains(extraSource) {
		source.Release()
		cachedBase.Release()
		t.Fatalf("test setup chose existing id %d", extraSource)
	}
	source = source.BuildAdded(extraSource)

	got, ok := snap.loadOrStoreMaterializedPredKey(key, source.Borrow())
	if !ok || got.IsEmpty() {
		source.Release()
		cachedBase.Release()
		t.Fatal("expected cache hit from loadOrStoreMaterializedPredKey")
	}
	defer got.Release()

	if !got.SharesPayload(cached) {
		source.Release()
		cachedBase.Release()
		t.Fatal("loadOrStore hit did not return cached payload")
	}
	if got.SharesPayload(source) {
		source.Release()
		cachedBase.Release()
		t.Fatal("loadOrStore hit reused caller payload instead of cached payload")
	}

	extraMut := uint64(5<<32 | 19)
	if source.Contains(extraMut) {
		source.Release()
		cachedBase.Release()
		t.Fatalf("test setup chose existing id %d", extraMut)
	}
	source = source.BuildAdded(extraMut)

	reloaded, ok := snap.loadMaterializedPredKey(key)
	if !ok || reloaded.IsEmpty() {
		source.Release()
		cachedBase.Release()
		t.Fatal("expected reloaded cached materialized predicate after hit")
	}
	defer reloaded.Release()

	if !slices.Equal(got.ToArray(), want) {
		source.Release()
		cachedBase.Release()
		t.Fatalf("loadOrStore hit result changed after caller mutation: got=%v want=%v", got.ToArray(), want)
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

func TestMemoryExtra_NumericRangeFullSpanRepeatedStoreSameKeyReturnsCachedPayloadNotCaller(t *testing.T) {
	entry := numericRangeBucketCacheEntryPool.Get()
	entry.refs.Store(1)
	defer numericRangeBucketCacheEntryPool.Put(entry)

	cachedBase := buildQueryRuntimeTestLargePosting()
	want := cachedBase.ToArray()

	stored, ok := entry.tryStoreFullSpan(3, 7, cachedBase.Borrow())
	if !ok || stored.IsEmpty() {
		cachedBase.Release()
		t.Fatal("expected initial numeric full-span store to succeed")
	}
	stored.Release()

	cached, ok := entry.loadFullSpan(3, 7)
	if !ok || cached.IsEmpty() {
		cachedBase.Release()
		t.Fatal("expected seeded cached numeric full-span posting")
	}
	defer cached.Release()

	source := buildQueryRuntimeTestLargePosting()
	extraSource := uint64(7<<32 | 21)
	if source.Contains(extraSource) {
		source.Release()
		cachedBase.Release()
		t.Fatalf("test setup chose existing id %d", extraSource)
	}
	source = source.BuildAdded(extraSource)

	got, ok := entry.tryStoreFullSpan(3, 7, source.Borrow())
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

	reloaded, ok := entry.loadFullSpan(3, 7)
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

func TestMemoryExtra_MaterializedPredInheritedBorrowedMutationDetaches(t *testing.T) {
	prev := &indexSnapshot{
		matPredCacheMaxEntries: 4,
	}
	snapshotExtInitMaterializedPredCache(prev)
	defer prev.releaseRuntimeCaches()

	base := buildQueryRuntimeTestLargePosting()
	want := base.ToArray()
	key := materializedPredKeyForScalar("email", compileScalarOpForTest(qx.OpPREFIX), "user")
	prev.storeMaterializedPredKey(key, base.Borrow())
	base.Release()

	next := &indexSnapshot{
		matPredCacheMaxEntries: 4,
	}
	snapshotExtInitMaterializedPredCache(next)
	defer next.releaseRuntimeCaches()
	inheritMaterializedPredCache(next, prev, nil, nil)

	fromPrev, ok := prev.loadMaterializedPredKey(key)
	if !ok || fromPrev.IsEmpty() {
		t.Fatal("expected inherited materialized cache entry in prev snapshot")
	}
	defer fromPrev.Release()

	fromNext, ok := next.loadMaterializedPredKey(key)
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

	reloadedPrev, ok := prev.loadMaterializedPredKey(key)
	if !ok || reloadedPrev.IsEmpty() {
		t.Fatal("expected reloaded prev materialized cache entry")
	}
	defer reloadedPrev.Release()

	reloadedNext, ok := next.loadMaterializedPredKey(key)
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

func TestMemoryExtra_NumericRangeInheritedBorrowedMutationDetaches(t *testing.T) {
	shared := snapshotExtStorage("10", "20")
	defer shared.Release()

	entry := numericRangeBucketCacheEntryPool.Get()
	entry.refs.Store(1)
	entry.storage = shared
	entry.idx = numericRangeBucketIndex{
		bucketSize: 1,
		keyCount:   2,
	}

	base := buildQueryRuntimeTestLargePosting()
	want := base.ToArray()
	stored, ok := entry.tryStoreFullSpan(0, 0, base.Borrow())
	if !ok || stored.IsEmpty() {
		base.Release()
		entry.release()
		t.Fatal("expected full-span cache store to succeed")
	}
	stored.Release()
	base.Release()

	prev := snapshotTestNewSnapshot(map[string]indexdata.FieldStorage{"age": shared}, nil, nil, nil)
	prev.numericRangeBucketCache = numericRangeBucketCachePool.Get()
	prev.numericRangeBucketCache.init(1)
	prev.numericRangeBucketCache.storeSlot("age", 0, entry)
	defer prev.releaseRuntimeCaches()

	next := snapshotTestNewSnapshot(map[string]indexdata.FieldStorage{"age": shared}, nil, nil, nil)
	next.numericRangeBucketCache = numericRangeBucketCachePool.Get()
	next.numericRangeBucketCache.init(1)
	defer next.releaseRuntimeCaches()

	inheritNumericRangeBucketCache(next, prev)

	prevEntry, ok := prev.numericRangeBucketCache.loadField("age")
	if !ok || prevEntry == nil {
		t.Fatal("expected prev numeric range cache entry")
	}
	nextEntry, ok := next.numericRangeBucketCache.loadField("age")
	if !ok || nextEntry == nil {
		t.Fatal("expected next numeric range cache entry")
	}
	if prevEntry != nextEntry {
		t.Fatal("expected inherited numeric range cache to reuse entry pointer")
	}

	fromPrev, ok := prevEntry.loadFullSpan(0, 0)
	if !ok || fromPrev.IsEmpty() {
		t.Fatal("expected prev full-span cached posting")
	}
	defer fromPrev.Release()

	fromNext, ok := nextEntry.loadFullSpan(0, 0)
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

	reloadedPrev, ok := prevEntry.loadFullSpan(0, 0)
	if !ok || reloadedPrev.IsEmpty() {
		t.Fatal("expected reloaded prev numeric full-span posting")
	}
	defer reloadedPrev.Release()

	reloadedNext, ok := nextEntry.loadFullSpan(0, 0)
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

func TestMemoryExtra_MaterializedPredOversizedLoadOrStoreHitReturnsCachedPayloadNotCaller(t *testing.T) {
	snap := &indexSnapshot{
		matPredCacheMaxEntries: 4,
		matPredCacheMaxCard:    1,
	}
	snapshotExtInitMaterializedPredCache(snap)
	defer snap.releaseRuntimeCaches()

	key := materializedPredKeyForNumericBucketSpan("age", 3, 7)
	cachedBase := buildQueryRuntimeTestLargePosting()
	want := cachedBase.ToArray()
	got, ok := snap.tryLoadOrStoreMaterializedPredOversizedKey(key, cachedBase.Borrow())
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

	cached, ok := snap.loadMaterializedPredKey(key)
	if !ok || cached.IsEmpty() {
		cachedBase.Release()
		t.Fatal("expected seeded oversized cached posting")
	}
	defer cached.Release()

	source := buildQueryRuntimeTestLargePosting()
	extraSource := uint64(11<<32 | 29)
	if source.Contains(extraSource) {
		source.Release()
		cachedBase.Release()
		t.Fatalf("test setup chose existing id %d", extraSource)
	}
	source = source.BuildAdded(extraSource)

	hit, ok := snap.tryLoadOrStoreMaterializedPredOversizedKey(key, source.Borrow())
	if !ok || hit.IsEmpty() {
		source.Release()
		cachedBase.Release()
		t.Fatal("expected oversized loadOrStore hit")
	}
	defer hit.Release()

	if !hit.SharesPayload(cached) {
		source.Release()
		cachedBase.Release()
		t.Fatal("oversized loadOrStore hit did not return cached payload")
	}
	if hit.SharesPayload(source) {
		source.Release()
		cachedBase.Release()
		t.Fatal("oversized loadOrStore hit reused caller payload instead of cached payload")
	}

	extraMut := uint64(13<<32 | 31)
	if source.Contains(extraMut) {
		source.Release()
		cachedBase.Release()
		t.Fatalf("test setup chose existing id %d", extraMut)
	}
	source = source.BuildAdded(extraMut)

	reloaded, ok := snap.loadMaterializedPredKey(key)
	if !ok || reloaded.IsEmpty() {
		source.Release()
		cachedBase.Release()
		t.Fatal("expected reloaded oversized cached posting after hit")
	}
	defer reloaded.Release()

	if !slices.Equal(hit.ToArray(), want) {
		source.Release()
		cachedBase.Release()
		t.Fatalf("oversized loadOrStore hit result changed after caller mutation: got=%v want=%v", hit.ToArray(), want)
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

func TestMemoryExtra_MaterializedPredInheritedReleaseKeepsSiblingSnapshotEntry(t *testing.T) {
	prev := &indexSnapshot{
		matPredCacheMaxEntries: 4,
	}
	snapshotExtInitMaterializedPredCache(prev)
	defer prev.releaseRuntimeCaches()

	base := buildQueryRuntimeTestLargePosting()
	want := base.ToArray()
	key := materializedPredKeyForScalar("email", compileScalarOpForTest(qx.OpPREFIX), "user")
	prev.storeMaterializedPredKey(key, base.Borrow())
	base.Release()

	next := &indexSnapshot{
		matPredCacheMaxEntries: 4,
	}
	snapshotExtInitMaterializedPredCache(next)
	defer next.releaseRuntimeCaches()
	inheritMaterializedPredCache(next, prev, nil, nil)

	held, ok := next.loadMaterializedPredKey(key)
	if !ok || held.IsEmpty() {
		t.Fatal("expected inherited materialized cache entry in next snapshot")
	}
	defer held.Release()

	prev.releaseRuntimeCaches()

	reloaded, ok := next.loadMaterializedPredKey(key)
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

func TestMemoryExtra_MaterializedPredInheritedEvictAndDrainKeepsSiblingSnapshotEntry(t *testing.T) {
	prev := &indexSnapshot{
		matPredCacheMaxEntries: 1,
	}
	snapshotExtInitMaterializedPredCache(prev)
	defer prev.releaseRuntimeCaches()

	base := buildQueryRuntimeTestLargePosting()
	want := base.ToArray()
	key := materializedPredKeyForScalar("email", compileScalarOpForTest(qx.OpPREFIX), "user")
	prev.storeMaterializedPredKey(key, base.Borrow())
	base.Release()

	next := &indexSnapshot{
		matPredCacheMaxEntries: 1,
	}
	snapshotExtInitMaterializedPredCache(next)
	defer next.releaseRuntimeCaches()
	inheritMaterializedPredCache(next, prev, nil, nil)

	held, ok := next.loadMaterializedPredKey(key)
	if !ok || held.IsEmpty() {
		t.Fatal("expected inherited materialized cache entry in next snapshot")
	}
	defer held.Release()

	evictor := buildQueryRuntimeTestLargePosting()
	extra := uint64(21<<32 | 37)
	if evictor.Contains(extra) {
		evictor.Release()
		t.Fatalf("test setup chose existing id %d", extra)
	}
	evictor = evictor.BuildAdded(extra)
	prev.storeMaterializedPredKey(materializedPredKeyForScalar("email", compileScalarOpForTest(qx.OpPREFIX), "other"), evictor.Borrow())
	evictor.Release()

	if _, ok := prev.loadMaterializedPredKey(key); ok {
		t.Fatal("expected prev snapshot to evict old materialized entry")
	}
	prev.drainRetiredRuntimeCaches()

	reloaded, ok := next.loadMaterializedPredKey(key)
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

func TestMemoryExtra_NumericRangeInheritedReleaseKeepsSiblingSnapshotEntry(t *testing.T) {
	shared := snapshotExtStorage("10", "20")
	defer shared.Release()

	entry := numericRangeBucketCacheEntryPool.Get()
	entry.refs.Store(1)
	entry.storage = shared
	entry.idx = numericRangeBucketIndex{
		bucketSize: 1,
		keyCount:   2,
	}

	base := buildQueryRuntimeTestLargePosting()
	want := base.ToArray()
	stored, ok := entry.tryStoreFullSpan(0, 0, base.Borrow())
	if !ok || stored.IsEmpty() {
		base.Release()
		entry.release()
		t.Fatal("expected full-span cache store to succeed")
	}
	stored.Release()
	base.Release()

	prev := snapshotTestNewSnapshot(map[string]indexdata.FieldStorage{"age": shared}, nil, nil, nil)
	prev.numericRangeBucketCache = numericRangeBucketCachePool.Get()
	prev.numericRangeBucketCache.init(1)
	prev.numericRangeBucketCache.storeSlot("age", 0, entry)
	defer prev.releaseRuntimeCaches()

	next := snapshotTestNewSnapshot(map[string]indexdata.FieldStorage{"age": shared}, nil, nil, nil)
	next.numericRangeBucketCache = numericRangeBucketCachePool.Get()
	next.numericRangeBucketCache.init(1)
	defer next.releaseRuntimeCaches()
	inheritNumericRangeBucketCache(next, prev)

	nextEntry, ok := next.numericRangeBucketCache.loadField("age")
	if !ok || nextEntry == nil {
		t.Fatal("expected inherited numeric range cache entry in next snapshot")
	}

	held, ok := nextEntry.loadFullSpan(0, 0)
	if !ok || held.IsEmpty() {
		t.Fatal("expected held inherited numeric full-span posting")
	}
	defer held.Release()

	prev.releaseRuntimeCaches()

	reloaded, ok := nextEntry.loadFullSpan(0, 0)
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
