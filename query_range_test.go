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

func bitmapToIDs(t *testing.T, b postingResult) []uint64 {
	t.Helper()
	if b.neg {
		t.Fatalf("unexpected negative postingResult result")
	}
	return b.ids.ToArray()
}

func warmNumericRangeBucketEntry(t *testing.T, db *DB[uint64, Rec], expr qx.Expr) (*numericRangeBucketCacheEntry, overlayRange) {
	t.Helper()

	fm := db.fields[expr.Field]
	if fm == nil {
		t.Fatalf("expected %s field metadata", expr.Field)
	}
	ov := db.fieldOverlay(expr.Field)
	if !ov.hasData() {
		t.Fatalf("expected %s overlay data", expr.Field)
	}
	key, isSlice, isNil, err := db.exprValueToIdxScalar(expr)
	if err != nil {
		t.Fatalf("exprValueToIdxScalar(%s): %v", expr.Field, err)
	}
	if isSlice || isNil {
		t.Fatalf("unexpected scalar flags for %s: isSlice=%v isNil=%v", expr.Field, isSlice, isNil)
	}
	rb, ok := rangeBoundsForOp(expr.Op, key)
	if !ok {
		t.Fatalf("rangeBoundsForOp(%v) failed", expr.Op)
	}
	br := ov.rangeForBounds(rb)
	out, ok := db.tryEvalNumericRangeBuckets(expr.Field, fm, ov, br)
	if !ok {
		t.Fatalf("expected numeric range bucket path for %s", expr.Field)
	}
	out.release()
	return requireNumericRangeBucketCacheEntry(t, db.getSnapshot(), expr.Field), br
}

func TestEvalSimple_NumericRangeBuckets_MatchClassicPath(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		SnapshotMaterializedPredCacheMaxEntries: 64,
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

}

func TestEvalSimple_NumericRangeBuckets_WorksAfterPatches(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		SnapshotMaterializedPredCacheMaxEntries: 64,
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

	// Mutate the numeric field used in the range predicate before evaluation.
	for i := 1; i <= 128; i++ {
		err := db.Patch(uint64(i), []Field{{Name: "age", Value: 9_000 + i}})
		if err != nil {
			t.Fatalf("Patch(%d): %v", i, err)
		}
	}

	expr := qx.Expr{
		Op:    qx.OpGTE,
		Field: "age",
		Value: 7_500,
	}

	// Enabled bucket mode should remain exact after recent mutations.
	setNumericBucketKnobs(t, db, 128, 1, 1)
	withBuckets, err := db.evalSimple(expr)
	if err != nil {
		t.Fatalf("evalSimple with buckets: %v", err)
	}
	withBucketsIDs := bitmapToIDs(t, withBuckets)
	withBuckets.release()

	// Baseline: explicit classic path.
	setNumericBucketKnobs(t, db, 128, 1, 1<<30)
	baseline, err := db.evalSimple(expr)
	if err != nil {
		t.Fatalf("evalSimple baseline: %v", err)
	}
	baselineIDs := bitmapToIDs(t, baseline)
	baseline.release()

	if !slices.Equal(withBucketsIDs, baselineIDs) {
		t.Fatalf("bucket path mismatch after patches: bucket-enabled=%d baseline=%d", len(withBucketsIDs), len(baselineIDs))
	}
}

func TestCount_NumericRangeBuckets_MatchClassicPath(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		SnapshotMaterializedPredCacheMaxEntries: -1,
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

func TestCount_NumericRangeBuckets_WorksAfterPatches(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		SnapshotMaterializedPredCacheMaxEntries: 64,
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
		t.Fatalf("count mismatch after patches: baseline=%d got=%d", baseline, got)
	}

}

func TestEvalSimple_NumericRangeBuckets_WorkWithoutPredicateCacheWhenReuseEnabled(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		SnapshotMaterializedPredCacheMaxEntries: -1,
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
	snap := db.getSnapshot()
	if snap.numericRangeBucketCache == nil {
		t.Fatalf("expected numeric range bucket cache map when reuse is enabled")
	}

	got1, err := db.evalSimple(expr)
	if err != nil {
		t.Fatalf("evalSimple first: %v", err)
	}
	ids1 := bitmapToIDs(t, got1)
	got1.release()

	entry := requireNumericRangeBucketCacheEntry(t, db.getSnapshot(), "age")
	if entry.storage.keyCount() == 0 {
		t.Fatalf("expected cached numeric range entry to point at live field storage")
	}

	got2, err := db.evalSimple(expr)
	if err != nil {
		t.Fatalf("evalSimple second: %v", err)
	}
	ids2 := bitmapToIDs(t, got2)
	got2.release()

	if !slices.Equal(ids1, ids2) {
		t.Fatalf("bucket result mismatch with predicate cache disabled: first=%d second=%d", len(ids1), len(ids2))
	}
}

func TestNumericRangeBucketCache_InheritsSafeEntriesAcrossSnapshotsWhenFieldIndexUnchanged(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		SnapshotMaterializedPredCacheMaxEntries: 64,
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
	entry1, _ := warmNumericRangeBucketEntry(t, db, qx.Expr{Op: qx.OpGTE, Field: "age", Value: 2_000})

	snap1 := db.getSnapshot()
	if snap1.numericRangeBucketCache == nil {
		t.Fatalf("expected non-nil numeric range cache in initial snapshot")
	}
	storage1, ok := snap1.fieldIndexStorage("age")
	if !ok || entry1.storage != storage1 {
		t.Fatalf("expected cached numeric range entry to point at initial age storage")
	}

	if err := db.Patch(1, []Field{{Name: "name", Value: "u_1_x"}}); err != nil {
		t.Fatalf("Patch: %v", err)
	}

	snap2 := db.getSnapshot()
	if snap2 == nil {
		t.Fatalf("expected current snapshot after patch")
	}
	if snap2.numericRangeBucketCache == snap1.numericRangeBucketCache {
		t.Fatalf("expected each snapshot to own its numeric range cache map")
	}
	entry2 := requireNumericRangeBucketCacheEntry(t, snap2, "age")
	storage2, ok := snap2.fieldIndexStorage("age")
	if !ok || entry2.storage != storage2 {
		t.Fatalf("expected inherited numeric range entry to point at current age storage")
	}
	if storage2 != storage1 {
		t.Fatalf("expected unchanged age field to retain shared storage across snapshots")
	}
}

func TestNumericRangeBucketCache_DropsChangedFieldEntryAcrossSnapshots(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		SnapshotMaterializedPredCacheMaxEntries: 64,
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
	entry1, _ := warmNumericRangeBucketEntry(t, db, qx.Expr{Op: qx.OpGTE, Field: "age", Value: 2_000})

	snap1 := db.getSnapshot()
	storage1, ok := snap1.fieldIndexStorage("age")
	if !ok || entry1.storage != storage1 {
		t.Fatalf("expected cached numeric range entry to point at original age storage")
	}

	if err := db.Patch(1, []Field{{Name: "age", Value: 20_000}}); err != nil {
		t.Fatalf("Patch(age): %v", err)
	}

	snap2 := db.getSnapshot()
	if snap2 == nil || snap2.numericRangeBucketCache == nil {
		t.Fatalf("expected current snapshot with initialized numeric range cache")
	}
	if snap2.numericRangeBucketCache == snap1.numericRangeBucketCache {
		t.Fatalf("expected changed-field snapshot to use a distinct numeric range cache map")
	}
	if _, ok := snap2.numericRangeBucketCache.Load("age"); ok {
		t.Fatalf("expected changed numeric field cache entry to be dropped on inherited snapshot")
	}

	warmNumericRangeBucketEntry(t, db, qx.Expr{Op: qx.OpGTE, Field: "age", Value: 2_000})

	snap3 := db.getSnapshot()
	entry3 := requireNumericRangeBucketCacheEntry(t, snap3, "age")
	storage3, ok := snap3.fieldIndexStorage("age")
	if !ok || entry3.storage != storage3 {
		t.Fatalf("expected rebuilt numeric range entry to point at current age storage")
	}
	if storage3 == storage1 {
		t.Fatalf("expected age storage to change after age patch")
	}
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
		key, isSlice, isNil, err := db.exprValueToIdxScalar(qx.Expr{Op: qx.OpGTE, Field: "age", Value: v})
		if err != nil {
			t.Fatalf("exprValueToIdxScalar(%d): %v", v, err)
		}
		if isSlice || isNil {
			t.Fatalf("unexpected scalar flags for age: isSlice=%v isNil=%v", isSlice, isNil)
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
	cached1, ok := entry.loadFullSpan(start1, end1)
	if !ok || cached1.IsEmpty() {
		t.Fatalf("expected local cached full bucket span after first evaluation")
	}
	cached1.Release()
	countAfterFirst := entry.fullSpanCount.Load()

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
	if got := entry.fullSpanCount.Load(); got != countAfterFirst {
		t.Fatalf("expected cached local full span reuse without new entries: before=%d after=%d", countAfterFirst, got)
	}
}

func TestNumericRangeBucketSpanCache_ReusedFullSpanStillMergesEdgeBuckets(t *testing.T) {
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
		key, isSlice, isNil, err := db.exprValueToIdxScalar(qx.Expr{Op: qx.OpGTE, Field: "age", Value: v})
		if err != nil {
			t.Fatalf("exprValueToIdxScalar(%d): %v", v, err)
		}
		if isSlice || isNil {
			t.Fatalf("unexpected scalar flags for age: isSlice=%v isNil=%v", isSlice, isNil)
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
	got1 := bitmapToIDs(t, out1)
	out1.release()

	entry := requireNumericRangeBucketCacheEntry(t, snap, "age")
	start1, end1, ok := entry.idx.fullBucketSpan(br1)
	if !ok {
		t.Fatalf("expected full bucket span for first bound")
	}
	cached, ok := entry.loadFullSpan(start1, end1)
	if !ok || cached.IsEmpty() {
		t.Fatalf("expected local cached full bucket span after first evaluation")
	}
	cached.Release()

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
	got2 := bitmapToIDs(t, out2)
	out2.release()

	want1, err := db.evalSimple(qx.Expr{Op: qx.OpGTE, Field: "age", Value: 2500})
	if err != nil {
		t.Fatalf("evalSimple(first): %v", err)
	}
	want1IDs := bitmapToIDs(t, want1)
	want1.release()

	want2, err := db.evalSimple(qx.Expr{Op: qx.OpGTE, Field: "age", Value: 2501})
	if err != nil {
		t.Fatalf("evalSimple(second): %v", err)
	}
	want2IDs := bitmapToIDs(t, want2)
	want2.release()

	if !slices.Equal(got1, want1IDs) {
		t.Fatalf("expected first range to match classic path: got=%v want=%v", got1, want1IDs)
	}
	if !slices.Equal(got2, want2IDs) {
		t.Fatalf("expected cached full-span reuse to still merge edge buckets: got=%v want=%v", got2, want2IDs)
	}
}

func TestNumericRangeBucketSpanCache_PredicateReleaseKeepsSharedPosting_Base(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		SnapshotMaterializedPredCacheMaxEntries: 64,
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
	entry, br := warmNumericRangeBucketEntry(t, db, expr)

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

	start, end, ok := entry.idx.fullBucketSpan(br)
	if !ok {
		t.Fatal("expected full bucket span")
	}
	cachedBefore, ok := entry.loadFullSpan(start, end)
	if !ok || cachedBefore.IsEmpty() {
		t.Fatal("expected cached full-span posting after predicate build")
	}

	releasePredicates([]predicate{pred})
	released = true

	cachedAfter, ok := entry.loadFullSpan(start, end)
	if !ok || cachedAfter.IsEmpty() {
		t.Fatal("expected cached full-span posting to survive predicate cleanup")
	}
	if cachedAfter != cachedBefore {
		t.Fatal("expected predicate cleanup to preserve cached posting instance")
	}
}

func TestNumericRangeBucketSpanCache_PredicateReleaseKeepsSharedPosting_AfterPatches(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		SnapshotMaterializedPredCacheMaxEntries: 64,
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

	// Keep mutated ages outside queried range so bucket fast-path can reuse a
	// shared base full-span postingResult without cloning it.
	for i := 1; i <= 128; i++ {
		if err := db.Patch(uint64(i), []Field{{Name: "age", Value: 1_000 + i}}); err != nil {
			t.Fatalf("Patch(%d): %v", i, err)
		}
	}

	setNumericBucketKnobs(t, db, 128, 1, 1)

	expr := qx.Expr{Op: qx.OpGTE, Field: "age", Value: 2_500}
	entry, br := warmNumericRangeBucketEntry(t, db, expr)

	pred, ok := db.buildPredicateWithMode(expr, true)
	if !ok {
		t.Fatal("expected range predicate build to succeed after patches")
	}
	released := false
	t.Cleanup(func() {
		if !released {
			releasePredicates([]predicate{pred})
		}
	})

	start, end, ok := entry.idx.fullBucketSpan(br)
	if !ok {
		t.Fatal("expected full bucket span")
	}
	cachedBefore, ok := entry.loadFullSpan(start, end)
	if !ok || cachedBefore.IsEmpty() {
		t.Fatal("expected cached full-span posting after predicate build")
	}

	releasePredicates([]predicate{pred})
	released = true

	cachedAfter, ok := entry.loadFullSpan(start, end)
	if !ok || cachedAfter.IsEmpty() {
		t.Fatal("expected cached full-span posting to survive predicate cleanup")
	}
	if cachedAfter != cachedBefore {
		t.Fatal("expected predicate cleanup to preserve cached posting instance")
	}
}

func TestNumericRangeBucketSpanCache_RespectsCardinalityGuard(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		SnapshotMaterializedPredCacheMaxEntries:     -1,
		SnapshotMaterializedPredCacheMaxCardinality: 32,
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

	key, isSlice, isNil, err := db.exprValueToIdxScalar(qx.Expr{Op: qx.OpGTE, Field: "age", Value: 0})
	if err != nil {
		t.Fatalf("exprValueToIdxScalar: %v", err)
	}
	if isSlice || isNil {
		t.Fatalf("unexpected scalar flags for age: isSlice=%v isNil=%v", isSlice, isNil)
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
	if cached, ok := entry.loadFullSpan(start, end); ok && !cached.IsEmpty() {
		t.Fatal("expected oversized full-span posting to be rejected by cache guard")
	}
}

func TestNumericRangeBucketIndex_CountBaseRangeMatchesExact(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		SnapshotMaterializedPredCacheMaxEntries: -1,
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
	ov := db.fieldOverlay("age")
	if !ov.hasData() {
		t.Fatalf("expected age field index data")
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
		got, ok := db.tryCountSnapshotNumericRange("age", fm, ov, tc.start, tc.end)
		if !ok {
			t.Fatalf("tryCountSnapshotNumericRange(%d,%d) failed", tc.start, tc.end)
		}
		_, want := overlayRangeStats(ov, ov.rangeByRanks(tc.start, tc.end))
		if got != want {
			t.Fatalf("range count mismatch for [%d:%d): got=%d want=%d", tc.start, tc.end, got, want)
		}
	}
}
