package rbi

import (
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/qcache"
	"slices"
	"testing"

	"github.com/vapstack/qx"
)

func TestMemoryExtra_MaterializedPredInheritedBorrowedMutationDetaches(t *testing.T) {
	prev := snapshotExtNewMaterializedPredSnapshot(4, 0)
	defer prev.releaseRuntimeCaches()

	base := buildQueryRuntimeTestLargePosting()
	want := base.ToArray()
	key := qcache.MaterializedPredKeyForScalar("email", compileScalarOpForTest(qx.OpPREFIX), "user")
	prev.storeMaterializedPredKey(key, base.Borrow())
	base.Release()

	next := snapshotExtNewMaterializedPredSnapshot(4, 0)
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

	entry := qcache.GetNumericRangeBucketEntry(shared, qcache.NumericRangeBucketIndex{}, 0)

	base := buildQueryRuntimeTestLargePosting()
	want := base.ToArray()
	stored, ok := entry.TryStoreFullSpan(0, 0, base.Borrow())
	if !ok || stored.IsEmpty() {
		base.Release()
		entry.Release()
		t.Fatal("expected full-span cache store to succeed")
	}
	stored.Release()
	base.Release()

	prev := snapshotTestNewSnapshot(map[string]indexdata.FieldStorage{"age": shared}, nil, nil, nil)
	prev.numericRangeBucketCache = qcache.GetNumericRangeBucketCache(1, 0)
	prev.numericRangeBucketCache.StoreSlot("age", 0, entry)
	defer prev.releaseRuntimeCaches()

	next := snapshotTestNewSnapshot(map[string]indexdata.FieldStorage{"age": shared}, nil, nil, nil)
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

func TestMemoryExtra_MaterializedPredInheritedReleaseKeepsSiblingSnapshotEntry(t *testing.T) {
	prev := snapshotExtNewMaterializedPredSnapshot(4, 0)
	defer prev.releaseRuntimeCaches()

	base := buildQueryRuntimeTestLargePosting()
	want := base.ToArray()
	key := qcache.MaterializedPredKeyForScalar("email", compileScalarOpForTest(qx.OpPREFIX), "user")
	prev.storeMaterializedPredKey(key, base.Borrow())
	base.Release()

	next := snapshotExtNewMaterializedPredSnapshot(4, 0)
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
	prev := snapshotExtNewMaterializedPredSnapshot(1, 0)
	defer prev.releaseRuntimeCaches()

	base := buildQueryRuntimeTestLargePosting()
	want := base.ToArray()
	key := qcache.MaterializedPredKeyForScalar("email", compileScalarOpForTest(qx.OpPREFIX), "user")
	prev.storeMaterializedPredKey(key, base.Borrow())
	base.Release()

	next := snapshotExtNewMaterializedPredSnapshot(1, 0)
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
	prev.storeMaterializedPredKey(qcache.MaterializedPredKeyForScalar("email", compileScalarOpForTest(qx.OpPREFIX), "other"), evictor.Borrow())
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

	entry := qcache.GetNumericRangeBucketEntry(shared, qcache.NumericRangeBucketIndex{}, 0)

	base := buildQueryRuntimeTestLargePosting()
	want := base.ToArray()
	stored, ok := entry.TryStoreFullSpan(0, 0, base.Borrow())
	if !ok || stored.IsEmpty() {
		base.Release()
		entry.Release()
		t.Fatal("expected full-span cache store to succeed")
	}
	stored.Release()
	base.Release()

	prev := snapshotTestNewSnapshot(map[string]indexdata.FieldStorage{"age": shared}, nil, nil, nil)
	prev.numericRangeBucketCache = qcache.GetNumericRangeBucketCache(1, 0)
	prev.numericRangeBucketCache.StoreSlot("age", 0, entry)
	defer prev.releaseRuntimeCaches()

	next := snapshotTestNewSnapshot(map[string]indexdata.FieldStorage{"age": shared}, nil, nil, nil)
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
