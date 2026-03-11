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

	_ = requireNumericRangeBucketCacheEntry(t, db.getSnapshot(), "age")
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
