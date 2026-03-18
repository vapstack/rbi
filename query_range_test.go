package rbi

import (
	"fmt"
	"slices"
	"testing"

	"github.com/vapstack/qx"
)

func requireNumericRangeBucketCacheEntry(t *testing.T, snap *indexSnapshot, field string) *numericRangeBucketCacheEntry {
	t.Helper()
	if snap == nil || snap.numericRangeBucketCache == nil {
		t.Fatalf("expected non-nil numeric range bucket cache for field %q", field)
	}
	raw, ok := snap.numericRangeBucketCache.Load(field)
	if !ok {
		t.Fatalf("expected numeric range bucket cache entry for field %q", field)
	}
	e, ok := raw.(*numericRangeBucketCacheEntry)
	if !ok || e == nil || e.idx == nil {
		t.Fatalf("expected non-nil numeric range bucket index for field %q", field)
	}
	return e
}

func setNumericBucketKnobs(t *testing.T, db *DB[uint64, Rec], size, minFieldKeys, minSpan int) {
	t.Helper()

	prevSize := db.options.NumericRangeBucketSize
	prevMinField := db.options.NumericRangeBucketMinFieldKeys
	prevMinSpan := db.options.NumericRangeBucketMinSpanKeys

	db.options.NumericRangeBucketSize = size
	db.options.NumericRangeBucketMinFieldKeys = minFieldKeys
	db.options.NumericRangeBucketMinSpanKeys = minSpan

	t.Cleanup(func() {
		db.options.NumericRangeBucketSize = prevSize
		db.options.NumericRangeBucketMinFieldKeys = prevMinField
		db.options.NumericRangeBucketMinSpanKeys = prevMinSpan
	})
}

func bitmapToIDs(t *testing.T, b bitmap) []uint64 {
	t.Helper()
	if b.neg {
		t.Fatalf("unexpected negative bitmap result")
	}
	if b.bm == nil || b.bm.IsEmpty() {
		return nil
	}
	out := make([]uint64, 0, b.bm.GetCardinality())
	it := b.bm.Iterator()
	for it.HasNext() {
		out = append(out, it.Next())
	}
	return out
}

func TestEvalSimple_NumericRangeBuckets_MatchClassicPath(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		SnapshotMaterializedPredCacheMaxEntries:          -1,
		SnapshotMaterializedPredCacheMaxEntriesWithDelta: -1,
	})

	seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}
	if db.getSnapshot().fieldDelta("age") != nil {
		t.Fatalf("expected no age delta after RebuildIndex")
	}

	expr := qx.Expr{
		Op:    qx.OpGTE,
		Field: "age",
		Value: 2_500,
	}

	// Baseline: force classic per-key range scan by requiring an unrealistically
	// large span for bucket routing.
	setNumericBucketKnobs(t, db, 128, 1, 1<<30)
	baseline, err := db.evalSimple(expr)
	if err != nil {
		t.Fatalf("evalSimple baseline: %v", err)
	}
	baselineIDs := bitmapToIDs(t, baseline)
	baseline.release()

	// Bucket path: aggressively enable for this dataset.
	setNumericBucketKnobs(t, db, 128, 1, 1)
	got, err := db.evalSimple(expr)
	if err != nil {
		t.Fatalf("evalSimple bucket: %v", err)
	}
	gotIDs := bitmapToIDs(t, got)
	got.release()

	if !slices.Equal(baselineIDs, gotIDs) {
		t.Fatalf("bucket result mismatch: baseline=%d got=%d", len(baselineIDs), len(gotIDs))
	}

	snap := db.getSnapshot()
	_ = requireNumericRangeBucketCacheEntry(t, snap, "age")
}

func TestEvalSimple_NumericRangeBuckets_WorksWithFieldDelta(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		SnapshotCompactorRequestEveryNWrites:             1 << 30,
		SnapshotCompactorIdleInterval:                    -1,
		SnapshotDeltaLayerMaxDepth:                       1 << 30,
		SnapshotMaterializedPredCacheMaxEntries:          -1,
		SnapshotMaterializedPredCacheMaxEntriesWithDelta: -1,
	})

	seedGeneratedUint64Data(t, db, 8_000, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	// Create delta on numeric field used in the range predicate.
	for i := 1; i <= 128; i++ {
		err := db.Patch(uint64(i), []Field{{Name: "age", Value: 9_000 + i}})
		if err != nil {
			t.Fatalf("Patch(%d): %v", i, err)
		}
	}

	snap := db.getSnapshot()
	if snap.fieldDelta("age") == nil {
		t.Fatalf("expected active delta for age field")
	}

	expr := qx.Expr{
		Op:    qx.OpGTE,
		Field: "age",
		Value: 7_500,
	}

	// Enabled bucket mode should remain exact even when field delta is active.
	setNumericBucketKnobs(t, db, 128, 1, 1)
	withBuckets, err := db.evalSimple(expr)
	if err != nil {
		t.Fatalf("evalSimple with buckets: %v", err)
	}
	withBucketsIDs := bitmapToIDs(t, withBuckets)
	withBuckets.release()

	_ = requireNumericRangeBucketCacheEntry(t, db.getSnapshot(), "age")

	// Baseline: explicit classic path.
	setNumericBucketKnobs(t, db, 128, 1, 1<<30)
	baseline, err := db.evalSimple(expr)
	if err != nil {
		t.Fatalf("evalSimple baseline: %v", err)
	}
	baselineIDs := bitmapToIDs(t, baseline)
	baseline.release()

	if !slices.Equal(withBucketsIDs, baselineIDs) {
		t.Fatalf("delta-path mismatch: bucket-enabled=%d baseline=%d", len(withBucketsIDs), len(baselineIDs))
	}
}

func TestCount_NumericRangeBuckets_MatchClassicPath(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		SnapshotMaterializedPredCacheMaxEntries:          -1,
		SnapshotMaterializedPredCacheMaxEntriesWithDelta: -1,
	})

	seedGeneratedUint64Data(t, db, 25_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Age:    i,
			Score:  float64(i),
			Active: i%2 == 0,
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.GTE("age", 2_000),
		qx.LT("age", 20_000),
		qx.EQ("active", true),
	)

	setNumericBucketKnobs(t, db, 128, 1, 1<<30)
	baseline, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count baseline: %v", err)
	}

	setNumericBucketKnobs(t, db, 128, 1, 1)
	got, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count with buckets: %v", err)
	}
	if got != baseline {
		t.Fatalf("count mismatch: baseline=%d got=%d", baseline, got)
	}
	// Stable AND-count can stay on the lead-bucket count path, so cache
	// population is not guaranteed here.
}

func TestCount_NumericRangeBuckets_WorksWithFieldDelta(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		SnapshotCompactorRequestEveryNWrites:             1 << 30,
		SnapshotCompactorIdleInterval:                    -1,
		SnapshotDeltaLayerMaxDepth:                       1 << 30,
		SnapshotMaterializedPredCacheMaxEntries:          -1,
		SnapshotMaterializedPredCacheMaxEntriesWithDelta: -1,
	})

	seedGeneratedUint64Data(t, db, 12_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Age:    i,
			Score:  float64(i),
			Active: i%2 == 0,
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	for i := 1; i <= 256; i++ {
		err := db.Patch(uint64(i), []Field{
			{Name: "age", Value: 15_000 + i},
			{Name: "active", Value: true},
		})
		if err != nil {
			t.Fatalf("Patch(%d): %v", i, err)
		}
	}

	if db.getSnapshot().fieldDelta("age") == nil {
		t.Fatalf("expected active delta for age field")
	}

	q := qx.Query(
		qx.GTE("age", 9_000),
		qx.LT("age", 16_000),
		qx.EQ("active", true),
	)

	setNumericBucketKnobs(t, db, 128, 1, 1<<30)
	baseline, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count baseline: %v", err)
	}

	setNumericBucketKnobs(t, db, 128, 1, 1)
	got, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count with buckets: %v", err)
	}
	if got != baseline {
		t.Fatalf("count mismatch with delta: baseline=%d got=%d", baseline, got)
	}

	_ = requireNumericRangeBucketCacheEntry(t, db.getSnapshot(), "age")
}

func TestNumericRangeBucketCache_ReusedAcrossDeltaSnapshotsWithSameBase(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		SnapshotCompactorRequestEveryNWrites:             1 << 30,
		SnapshotCompactorIdleInterval:                    -1,
		SnapshotDeltaLayerMaxDepth:                       1 << 30,
		SnapshotMaterializedPredCacheMaxEntries:          -1,
		SnapshotMaterializedPredCacheMaxEntriesWithDelta: -1,
	})

	seedGeneratedUint64Data(t, db, 6_000, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	setNumericBucketKnobs(t, db, 128, 1, 1)
	if _, err := db.Count(qx.Query(qx.GTE("age", 2_000))); err != nil {
		t.Fatalf("initial Count: %v", err)
	}

	snap1 := db.getSnapshot()
	if snap1.numericRangeBucketCache == nil {
		t.Fatalf("expected non-nil numeric range cache in initial snapshot")
	}
	_ = requireNumericRangeBucketCacheEntry(t, snap1, "age")

	if err := db.Patch(1, []Field{{Name: "name", Value: "u_1_x"}}); err != nil {
		t.Fatalf("Patch: %v", err)
	}

	snap2 := db.getSnapshot()
	if snap2 == nil {
		t.Fatalf("expected current snapshot after patch")
	}
	if snap2.numericRangeBucketCache != snap1.numericRangeBucketCache {
		t.Fatalf("expected numeric range cache reuse between snapshots with same base")
	}
	_ = requireNumericRangeBucketCacheEntry(t, snap2, "age")
}

func TestNumericRangeBucketSpanCache_ReusedForNearbyBounds(t *testing.T) {
	db, _ := openTempDBUint64(t)

	seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	setNumericBucketKnobs(t, db, 128, 1, 1)

	snap := db.getSnapshot()
	fm := db.fields["age"]
	if fm == nil {
		t.Fatalf("expected age field metadata")
	}
	ov := db.fieldOverlay("age")
	if !ov.hasData() {
		t.Fatalf("expected age overlay data")
	}

	makeRange := func(v int) overlayRange {
		t.Helper()
		key, isSlice, err := db.exprValueToIdxScalar(qx.Expr{Op: qx.OpGTE, Field: "age", Value: v})
		if err != nil {
			t.Fatalf("exprValueToIdxScalar(%d): %v", v, err)
		}
		if isSlice {
			t.Fatalf("unexpected slice key for age")
		}
		var rb rangeBounds
		rb.applyLo(key, true)
		return ov.rangeForBounds(rb)
	}

	br1 := makeRange(2500)
	out1, ok := db.tryEvalNumericRangeBuckets("age", fm, ov, br1)
	if !ok {
		t.Fatalf("expected numeric range bucket path for first bound")
	}
	out1.release()

	entry := requireNumericRangeBucketCacheEntry(t, snap, "age")
	start1, end1, ok := entry.idx.fullBucketSpan(br1)
	if !ok {
		t.Fatalf("expected full bucket span for first bound")
	}
	cacheKey := materializedPredCacheKeyForNumericBucketSpan("age", start1, end1)
	cached1, ok := snap.loadMaterializedPred(cacheKey)
	if !ok || cached1 == nil || cached1.IsEmpty() {
		t.Fatalf("expected cached full bucket span after first evaluation")
	}
	countAfterFirst := snap.matPredCacheCount.Load()

	br2 := makeRange(2501)
	start2, end2, ok := entry.idx.fullBucketSpan(br2)
	if !ok {
		t.Fatalf("expected full bucket span for second bound")
	}
	if start1 != start2 || end1 != end2 {
		t.Fatalf("expected nearby bounds to share full bucket span: first=%d..%d second=%d..%d", start1, end1, start2, end2)
	}
	out2, ok := db.tryEvalNumericRangeBuckets("age", fm, ov, br2)
	if !ok {
		t.Fatalf("expected numeric range bucket path for second bound")
	}
	out2.release()
	if got := snap.matPredCacheCount.Load(); got != countAfterFirst {
		t.Fatalf("expected cached span reuse without new entries: before=%d after=%d", countAfterFirst, got)
	}
}

func TestNumericRangeBucketSpanCache_PredicateReleaseKeepsSharedBitmap_Base(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		SnapshotMaterializedPredCacheMaxEntries:          -1,
		SnapshotMaterializedPredCacheMaxEntriesWithDelta: -1,
	})

	seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	setNumericBucketKnobs(t, db, 128, 1, 1)

	expr := qx.Expr{Op: qx.OpGTE, Field: "age", Value: 2_500}
	ov := db.fieldOverlay("age")
	if !ov.hasData() {
		t.Fatal("expected age overlay data")
	}

	key, isSlice, err := db.exprValueToIdxScalar(expr)
	if err != nil {
		t.Fatalf("exprValueToIdxScalar: %v", err)
	}
	if isSlice {
		t.Fatal("unexpected slice key for age")
	}
	var rb rangeBounds
	rb.applyLo(key, true)
	br := ov.rangeForBounds(rb)

	pred, ok := db.buildPredicateWithMode(expr, true)
	if !ok {
		t.Fatal("expected range predicate build to succeed")
	}
	released := false
	t.Cleanup(func() {
		if !released {
			releasePredicates([]predicate{pred})
		}
	})

	entry := requireNumericRangeBucketCacheEntry(t, db.getSnapshot(), "age")
	start, end, ok := entry.idx.fullBucketSpan(br)
	if !ok {
		t.Fatal("expected full bucket span")
	}
	cachedBefore, ok := entry.loadFullSpan(start, end)
	if !ok || cachedBefore == nil || cachedBefore.IsEmpty() {
		t.Fatal("expected cached full-span bitmap after predicate build")
	}

	releasePredicates([]predicate{pred})
	released = true

	cachedAfter, ok := entry.loadFullSpan(start, end)
	if !ok || cachedAfter == nil || cachedAfter.IsEmpty() {
		t.Fatal("expected cached full-span bitmap to survive predicate cleanup")
	}
	if cachedAfter != cachedBefore {
		t.Fatal("expected predicate cleanup to preserve cached bitmap instance")
	}
}

func TestNumericRangeBucketSpanCache_PredicateReleaseKeepsSharedBitmap_WithFieldDelta(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		SnapshotCompactorRequestEveryNWrites:             1 << 30,
		SnapshotCompactorIdleInterval:                    -1,
		SnapshotDeltaLayerMaxDepth:                       1 << 30,
		SnapshotMaterializedPredCacheMaxEntries:          -1,
		SnapshotMaterializedPredCacheMaxEntriesWithDelta: -1,
	})

	seedGeneratedUint64Data(t, db, 8_000, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	// Keep age delta outside queried range so bucket fast-path can reuse a
	// shared base full-span bitmap without cloning it.
	for i := 1; i <= 128; i++ {
		if err := db.Patch(uint64(i), []Field{{Name: "age", Value: 1_000 + i}}); err != nil {
			t.Fatalf("Patch(%d): %v", i, err)
		}
	}

	if db.getSnapshot().fieldDelta("age") == nil {
		t.Fatal("expected active delta for age field")
	}

	setNumericBucketKnobs(t, db, 128, 1, 1)

	expr := qx.Expr{Op: qx.OpGTE, Field: "age", Value: 2_500}
	ov := db.fieldOverlay("age")
	if ov.delta == nil {
		t.Fatal("expected overlay delta for age")
	}

	key, isSlice, err := db.exprValueToIdxScalar(expr)
	if err != nil {
		t.Fatalf("exprValueToIdxScalar: %v", err)
	}
	if isSlice {
		t.Fatal("unexpected slice key for age")
	}
	var rb rangeBounds
	rb.applyLo(key, true)
	br := ov.rangeForBounds(rb)
	if br.deltaStart < br.deltaEnd {
		t.Fatal("expected query range to avoid age delta entries")
	}

	pred, ok := db.buildPredicateWithMode(expr, true)
	if !ok {
		t.Fatal("expected range predicate build with field delta to succeed")
	}
	released := false
	t.Cleanup(func() {
		if !released {
			releasePredicates([]predicate{pred})
		}
	})

	entry := requireNumericRangeBucketCacheEntry(t, db.getSnapshot(), "age")
	start, end, ok := entry.idx.fullBucketSpan(br)
	if !ok {
		t.Fatal("expected full bucket span")
	}
	cachedBefore, ok := entry.loadFullSpan(start, end)
	if !ok || cachedBefore == nil || cachedBefore.IsEmpty() {
		t.Fatal("expected cached full-span bitmap after delta predicate build")
	}

	releasePredicates([]predicate{pred})
	released = true

	cachedAfter, ok := entry.loadFullSpan(start, end)
	if !ok || cachedAfter == nil || cachedAfter.IsEmpty() {
		t.Fatal("expected cached full-span bitmap to survive delta predicate cleanup")
	}
	if cachedAfter != cachedBefore {
		t.Fatal("expected predicate cleanup to preserve cached bitmap instance")
	}
}

func TestNumericRangeBucketSpanCache_RespectsBitmapCardinalityGuard(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		SnapshotMaterializedPredCacheMaxEntries:           -1,
		SnapshotMaterializedPredCacheMaxEntriesWithDelta:  -1,
		SnapshotMaterializedPredCacheMaxBitmapCardinality: 32,
	})

	seedGeneratedUint64Data(t, db, 2_000, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	setNumericBucketKnobs(t, db, 128, 1, 1)

	snap := db.getSnapshot()
	fm := db.fields["age"]
	if fm == nil {
		t.Fatalf("expected age field metadata")
	}
	ov := db.fieldOverlay("age")
	if !ov.hasData() {
		t.Fatalf("expected age overlay data")
	}

	key, isSlice, err := db.exprValueToIdxScalar(qx.Expr{Op: qx.OpGTE, Field: "age", Value: 0})
	if err != nil {
		t.Fatalf("exprValueToIdxScalar: %v", err)
	}
	if isSlice {
		t.Fatal("unexpected slice key for age")
	}
	var rb rangeBounds
	rb.applyLo(key, true)
	br := ov.rangeForBounds(rb)

	out, ok := db.tryEvalNumericRangeBuckets("age", fm, ov, br)
	if !ok {
		t.Fatal("expected numeric range bucket path")
	}
	out.release()

	entry := requireNumericRangeBucketCacheEntry(t, snap, "age")
	start, end, ok := entry.idx.fullBucketSpan(br)
	if !ok {
		t.Fatal("expected full bucket span")
	}
	if cached, ok := entry.loadFullSpan(start, end); ok && cached != nil {
		t.Fatal("expected oversized full-span bitmap to be rejected by cache guard")
	}
}

func TestNumericRangeBucketIndex_CountBaseRangeMatchesExact(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		SnapshotMaterializedPredCacheMaxEntries:          -1,
		SnapshotMaterializedPredCacheMaxEntriesWithDelta: -1,
	})

	seedGeneratedUint64Data(t, db, 6_000, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   i / 3,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	setNumericBucketKnobs(t, db, 128, 1, 1)

	fm := db.fields["age"]
	if fm == nil {
		t.Fatalf("expected age field metadata")
	}
	slice := db.snapshotFieldIndexSlice("age")
	if slice == nil {
		t.Fatalf("expected age field index slice")
	}

	cases := []struct {
		start int
		end   int
	}{
		{start: 0, end: 2_000},
		{start: 5, end: 137},
		{start: 127, end: 513},
		{start: 255, end: 1_001},
		{start: 1_777, end: 2_000},
	}
	for _, tc := range cases {
		got, ok := db.tryCountSnapshotNumericRange("age", fm, slice, tc.start, tc.end)
		if !ok {
			t.Fatalf("tryCountSnapshotNumericRange(%d,%d) failed", tc.start, tc.end)
		}
		want := countBaseIndexRangeCardinality(*slice, tc.start, tc.end)
		if got != want {
			t.Fatalf("range count mismatch for [%d:%d): got=%d want=%d", tc.start, tc.end, got, want)
		}
	}

	_ = requireNumericRangeBucketCacheEntry(t, db.getSnapshot(), "age")
}
