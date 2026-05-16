package qexec

import (
	"fmt"
	"github.com/vapstack/rbi/internal/indexdata"
	"slices"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/snapshot"
)

func requireNumericRangeBucketCacheEntry(t *testing.T, snap *snapshot.View, field string) *qcache.NumericRangeBucketEntry {
	t.Helper()
	if snap == nil || snap.NumericRangeBucketCache() == nil {
		t.Fatalf("expected non-nil numeric range bucket cache for field %q", field)
	}
	e, ok := snap.NumericRangeBucketCache().LoadField(field)
	if !ok {
		t.Fatalf("expected numeric range bucket cache entry for field %q", field)
	}
	if e == nil || e.Index().BucketSize() <= 0 {
		t.Fatalf("expected non-nil numeric range bucket index for field %q", field)
	}
	return e
}

func setNumericBucketKnobs(t *testing.T, db *DB[uint64, Rec], size, minFieldKeys, minSpan int) {
	t.Helper()

	prevSize := db.options.NumericRangeBucketSize
	prevMinField := db.options.NumericRangeBucketMinFieldKeys
	prevMinSpan := db.options.NumericRangeBucketMinSpanKeys
	prevEngineSize := db.engine.exec.NumericRangeBucketSize
	prevEngineMinField := db.engine.exec.NumericRangeBucketMinFieldKeys
	prevEngineMinSpan := db.engine.exec.NumericRangeBucketMinSpanKeys

	db.options.NumericRangeBucketSize = size
	db.options.NumericRangeBucketMinFieldKeys = minFieldKeys
	db.options.NumericRangeBucketMinSpanKeys = minSpan
	db.engine.exec.NumericRangeBucketSize = size
	db.engine.exec.NumericRangeBucketMinFieldKeys = minFieldKeys
	db.engine.exec.NumericRangeBucketMinSpanKeys = minSpan

	t.Cleanup(func() {
		db.options.NumericRangeBucketSize = prevSize
		db.options.NumericRangeBucketMinFieldKeys = prevMinField
		db.options.NumericRangeBucketMinSpanKeys = prevMinSpan
		db.engine.exec.NumericRangeBucketSize = prevEngineSize
		db.engine.exec.NumericRangeBucketMinFieldKeys = prevEngineMinField
		db.engine.exec.NumericRangeBucketMinSpanKeys = prevEngineMinSpan
	})
}

func bitmapToIDs(t *testing.T, b postingResult) []uint64 {
	t.Helper()
	if b.neg {
		t.Fatalf("unexpected negative postingResult result")
	}
	return b.ids.ToArray()
}

func numericRangeFullSpanCacheEntryCount(entry *qcache.NumericRangeBucketEntry) int {
	return entry.FullSpanEntryCount()
}

func warmNumericRangeBucketEntry(t *testing.T, db *DB[uint64, Rec], expr qx.Expr) (*qcache.NumericRangeBucketEntry, indexdata.FieldIndexRange) {
	t.Helper()

	prepared, compiled, err := prepareTestExpr(db.engine, expr)
	if err != nil {
		t.Fatalf("prepareTestExpr(%+v): %v", expr, err)
	}
	defer prepared.Release()
	f := db.engine.fieldNameByOrdinal(compiled.FieldOrdinal)
	fm := db.engine.schema.Fields[f]
	if fm == nil {
		t.Fatalf("expected %s field metadata", f)
	}
	view := db.engine.currentQueryViewForTests()
	ov := view.fieldIndexViewFromSlotsByName(view.snap.Index, f)
	if !ov.HasData() {
		t.Fatalf("expected %s overlay data", f)
	}
	key, isSlice, isNil, err := db.engine.exprValueToIdxScalar(expr)
	if err != nil {
		t.Fatalf("exprValueToIdxScalar(%s): %v", f, err)
	}
	if isSlice || isNil {
		t.Fatalf("unexpected scalar flags for %s: isSlice=%v isNil=%v", f, isSlice, isNil)
	}
	rb, ok := rangeBoundsForOp(compiled.Op, key)
	if !ok {
		t.Fatalf("rangeBoundsForOp(%v) failed", compiled.Op)
	}
	br := ov.RangeForBounds(rb)
	out, ok := db.engine.currentQueryViewForTests().tryEvalNumericRangeBuckets(f, fm, ov, br)
	if !ok {
		t.Fatalf("expected numeric range bucket path for %s", f)
	}
	out.ids.Release()
	return requireNumericRangeBucketCacheEntry(t, db.engine.snapshot.Current(), f), br
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
	expr := qx.GTE("age", 2_500)

	// Baseline: force classic per-key range scan by requiring an unrealistically
	// large span for bucket routing.
	setNumericBucketKnobs(t, db, 128, 1, 1<<30)
	baseline, err := db.engine.evalSimple(expr)
	if err != nil {
		t.Fatalf("evalSimple baseline: %v", err)
	}
	baselineIDs := bitmapToIDs(t, baseline)
	baseline.ids.Release()

	// Bucket path: aggressively enable for this dataset.
	setNumericBucketKnobs(t, db, 128, 1, 1)
	got, err := db.engine.evalSimple(expr)
	if err != nil {
		t.Fatalf("evalSimple bucket: %v", err)
	}
	gotIDs := bitmapToIDs(t, got)
	got.ids.Release()

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

	expr := qx.GTE("age", 7_500)

	// Enabled bucket mode should remain exact after recent mutations.
	setNumericBucketKnobs(t, db, 128, 1, 1)
	withBuckets, err := db.engine.evalSimple(expr)
	if err != nil {
		t.Fatalf("evalSimple with buckets: %v", err)
	}
	withBucketsIDs := bitmapToIDs(t, withBuckets)
	withBuckets.ids.Release()

	// Baseline: explicit classic path.
	setNumericBucketKnobs(t, db, 128, 1, 1<<30)
	baseline, err := db.engine.evalSimple(expr)
	if err != nil {
		t.Fatalf("evalSimple baseline: %v", err)
	}
	baselineIDs := bitmapToIDs(t, baseline)
	baseline.ids.Release()

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
	baseline, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count baseline: %v", err)
	}

	setNumericBucketKnobs(t, db, 128, 1, 1)
	got, err := db.Count(q.Filter)
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
	baseline, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count baseline: %v", err)
	}

	setNumericBucketKnobs(t, db, 128, 1, 1)
	got, err := db.Count(q.Filter)
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

	expr := qx.GTE("age", 2_500)
	snap := db.engine.snapshot.Current()
	if snap.NumericRangeBucketCache() == nil {
		t.Fatalf("expected numeric range bucket cache when reuse is enabled")
	}

	got1, err := db.engine.evalSimple(expr)
	if err != nil {
		t.Fatalf("evalSimple first: %v", err)
	}
	ids1 := bitmapToIDs(t, got1)
	got1.ids.Release()

	entry := requireNumericRangeBucketCacheEntry(t, db.engine.snapshot.Current(), "age")
	if entry.Storage().KeyCount() == 0 {
		t.Fatalf("expected cached numeric range entry to point at live field storage")
	}

	got2, err := db.engine.evalSimple(expr)
	if err != nil {
		t.Fatalf("evalSimple second: %v", err)
	}
	ids2 := bitmapToIDs(t, got2)
	got2.ids.Release()

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
	entry1, _ := warmNumericRangeBucketEntry(t, db, qx.GTE("age", 2_000))

	snap1 := db.engine.snapshot.Current()
	if snap1.NumericRangeBucketCache() == nil {
		t.Fatalf("expected non-nil numeric range cache in initial snapshot")
	}
	storage1, ok := snap1.FieldIndexStorage("age")
	if !ok || entry1.Storage() != storage1 {
		t.Fatalf("expected cached numeric range entry to point at initial age storage")
	}

	if err := db.Patch(1, []Field{{Name: "name", Value: "u_1_x"}}); err != nil {
		t.Fatalf("Patch: %v", err)
	}

	snap2 := db.engine.snapshot.Current()
	if snap2 == nil {
		t.Fatalf("expected current snapshot after patch")
	}
	if snap2.NumericRangeBucketCache() == snap1.NumericRangeBucketCache() {
		t.Fatalf("expected each snapshot to own its numeric range cache")
	}
	entry2 := requireNumericRangeBucketCacheEntry(t, snap2, "age")
	storage2, ok := snap2.FieldIndexStorage("age")
	if !ok || entry2.Storage() != storage2 {
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
	entry1, _ := warmNumericRangeBucketEntry(t, db, qx.GTE("age", 2_000))

	snap1 := db.engine.snapshot.Current()
	storage1, ok := snap1.FieldIndexStorage("age")
	if !ok || entry1.Storage() != storage1 {
		t.Fatalf("expected cached numeric range entry to point at original age storage")
	}

	if err := db.Patch(1, []Field{{Name: "age", Value: 20_000}}); err != nil {
		t.Fatalf("Patch(age): %v", err)
	}

	snap2 := db.engine.snapshot.Current()
	if snap2 == nil || snap2.NumericRangeBucketCache() == nil {
		t.Fatalf("expected current snapshot with initialized numeric range cache")
	}
	if snap2.NumericRangeBucketCache() == snap1.NumericRangeBucketCache() {
		t.Fatalf("expected changed-field snapshot to use a distinct numeric range cache")
	}
	if _, ok := snap2.NumericRangeBucketCache().LoadField("age"); ok {
		t.Fatalf("expected changed numeric field cache entry to be dropped on inherited snapshot")
	}

	warmNumericRangeBucketEntry(t, db, qx.GTE("age", 2_000))

	snap3 := db.engine.snapshot.Current()
	entry3 := requireNumericRangeBucketCacheEntry(t, snap3, "age")
	storage3, ok := snap3.FieldIndexStorage("age")
	if !ok || entry3.Storage() != storage3 {
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

	snap := db.engine.snapshot.Current()
	fm := db.engine.schema.Fields["age"]
	if fm == nil {
		t.Fatalf("expected age field metadata")
	}
	view := db.engine.currentQueryViewForTests()
	ov := view.fieldIndexViewFromSlotsByName(view.snap.Index, "age")
	if !ov.HasData() {
		t.Fatalf("expected age overlay data")
	}

	makeRange := func(v int) indexdata.FieldIndexRange {
		t.Helper()
		key, isSlice, isNil, err := db.engine.exprValueToIdxScalar(qx.GTE("age", v))
		if err != nil {
			t.Fatalf("exprValueToIdxScalar(%d): %v", v, err)
		}
		if isSlice || isNil {
			t.Fatalf("unexpected scalar flags for age: isSlice=%v isNil=%v", isSlice, isNil)
		}
		var rb indexdata.Bounds
		rb.ApplyLo(key, true)
		return ov.RangeForBounds(rb)
	}

	br1 := makeRange(2500)
	out1, ok := db.engine.currentQueryViewForTests().tryEvalNumericRangeBuckets("age", fm, ov, br1)
	if !ok {
		t.Fatalf("expected numeric range bucket path for first bound")
	}
	out1.ids.Release()

	entry := requireNumericRangeBucketCacheEntry(t, snap, "age")
	start1, end1, ok := entry.Index().FullBucketSpan(br1)
	if !ok {
		t.Fatalf("expected full bucket span for first bound")
	}
	cached1, ok := entry.LoadFullSpan(start1, end1)
	if !ok || cached1.IsEmpty() {
		t.Fatalf("expected local cached full bucket span after first evaluation")
	}
	cached1.Release()
	countAfterFirst := numericRangeFullSpanCacheEntryCount(entry)

	br2 := makeRange(2501)
	start2, end2, ok := entry.Index().FullBucketSpan(br2)
	if !ok {
		t.Fatalf("expected full bucket span for second bound")
	}
	if start1 != start2 || end1 != end2 {
		t.Fatalf("expected nearby bounds to share full bucket span: first=%d..%d second=%d..%d", start1, end1, start2, end2)
	}
	out2, ok := db.engine.currentQueryViewForTests().tryEvalNumericRangeBuckets("age", fm, ov, br2)
	if !ok {
		t.Fatalf("expected numeric range bucket path for second bound")
	}
	out2.ids.Release()
	if got := numericRangeFullSpanCacheEntryCount(entry); got != countAfterFirst {
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

	snap := db.engine.snapshot.Current()
	fm := db.engine.schema.Fields["age"]
	if fm == nil {
		t.Fatalf("expected age field metadata")
	}
	view := db.engine.currentQueryViewForTests()
	ov := view.fieldIndexViewFromSlotsByName(view.snap.Index, "age")
	if !ov.HasData() {
		t.Fatalf("expected age overlay data")
	}

	makeRange := func(v int) indexdata.FieldIndexRange {
		t.Helper()
		key, isSlice, isNil, err := db.engine.exprValueToIdxScalar(qx.GTE("age", v))
		if err != nil {
			t.Fatalf("exprValueToIdxScalar(%d): %v", v, err)
		}
		if isSlice || isNil {
			t.Fatalf("unexpected scalar flags for age: isSlice=%v isNil=%v", isSlice, isNil)
		}
		var rb indexdata.Bounds
		rb.ApplyLo(key, true)
		return ov.RangeForBounds(rb)
	}

	br1 := makeRange(2500)
	out1, ok := db.engine.currentQueryViewForTests().tryEvalNumericRangeBuckets("age", fm, ov, br1)
	if !ok {
		t.Fatalf("expected numeric range bucket path for first bound")
	}
	got1 := bitmapToIDs(t, out1)
	out1.ids.Release()

	entry := requireNumericRangeBucketCacheEntry(t, snap, "age")
	start1, end1, ok := entry.Index().FullBucketSpan(br1)
	if !ok {
		t.Fatalf("expected full bucket span for first bound")
	}
	cached, ok := entry.LoadFullSpan(start1, end1)
	if !ok || cached.IsEmpty() {
		t.Fatalf("expected local cached full bucket span after first evaluation")
	}
	cached.Release()

	br2 := makeRange(2501)
	start2, end2, ok := entry.Index().FullBucketSpan(br2)
	if !ok {
		t.Fatalf("expected full bucket span for second bound")
	}
	if start1 != start2 || end1 != end2 {
		t.Fatalf("expected nearby bounds to share full bucket span: first=%d..%d second=%d..%d", start1, end1, start2, end2)
	}
	out2, ok := db.engine.currentQueryViewForTests().tryEvalNumericRangeBuckets("age", fm, ov, br2)
	if !ok {
		t.Fatalf("expected numeric range bucket path for second bound")
	}
	got2 := bitmapToIDs(t, out2)
	out2.ids.Release()

	want1, err := db.engine.evalSimple(qx.GTE("age", 2500))
	if err != nil {
		t.Fatalf("evalSimple(first): %v", err)
	}
	want1IDs := bitmapToIDs(t, want1)
	want1.ids.Release()

	want2, err := db.engine.evalSimple(qx.GTE("age", 2501))
	if err != nil {
		t.Fatalf("evalSimple(second): %v", err)
	}
	want2IDs := bitmapToIDs(t, want2)
	want2.ids.Release()

	if !slices.Equal(got1, want1IDs) {
		t.Fatalf("expected first range to match classic path: got=%v want=%v", got1, want1IDs)
	}
	if !slices.Equal(got2, want2IDs) {
		t.Fatalf("expected cached full-span reuse to still merge edge buckets: got=%v want=%v", got2, want2IDs)
	}
}

func TestNumericRangeBucketSpanCache_ExtendedSuffixSpanStillMatchesRange(t *testing.T) {
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

	snap := db.engine.snapshot.Current()
	fm := db.engine.schema.Fields["age"]
	if fm == nil {
		t.Fatalf("expected age field metadata")
	}
	view := db.engine.currentQueryViewForTests()
	ov := view.fieldIndexViewFromSlotsByName(view.snap.Index, "age")
	if !ov.HasData() {
		t.Fatalf("expected age overlay data")
	}

	makeRange := func(v int) indexdata.FieldIndexRange {
		t.Helper()
		key, isSlice, isNil, err := db.engine.exprValueToIdxScalar(qx.GTE("age", v))
		if err != nil {
			t.Fatalf("exprValueToIdxScalar(%d): %v", v, err)
		}
		if isSlice || isNil {
			t.Fatalf("unexpected scalar flags for age: isSlice=%v isNil=%v", isSlice, isNil)
		}
		var rb indexdata.Bounds
		rb.ApplyLo(key, true)
		return ov.RangeForBounds(rb)
	}

	brNarrow := makeRange(2500)
	outNarrow, ok := db.engine.currentQueryViewForTests().tryEvalNumericRangeBuckets("age", fm, ov, brNarrow)
	if !ok {
		t.Fatalf("expected numeric range bucket path for narrow bound")
	}
	outNarrow.ids.Release()

	entry := requireNumericRangeBucketCacheEntry(t, snap, "age")
	startNarrow, endNarrow, ok := entry.Index().FullBucketSpan(brNarrow)
	if !ok {
		t.Fatalf("expected full bucket span for narrow bound")
	}

	brWide := makeRange(2000)
	startWide, endWide, ok := entry.Index().FullBucketSpan(brWide)
	if !ok {
		t.Fatalf("expected full bucket span for wide bound")
	}
	if endWide != endNarrow || startWide >= startNarrow {
		t.Fatalf("expected wider same-end full span: narrow=%d..%d wide=%d..%d", startNarrow, endNarrow, startWide, endWide)
	}

	outWide, ok := db.engine.currentQueryViewForTests().tryEvalNumericRangeBuckets("age", fm, ov, brWide)
	if !ok {
		t.Fatalf("expected numeric range bucket path for wide bound")
	}
	gotWide := bitmapToIDs(t, outWide)
	outWide.ids.Release()

	wantWide, err := db.engine.evalSimple(qx.GTE("age", 2000))
	if err != nil {
		t.Fatalf("evalSimple(wide): %v", err)
	}
	wantWideIDs := bitmapToIDs(t, wantWide)
	wantWide.ids.Release()

	if !slices.Equal(gotWide, wantWideIDs) {
		t.Fatalf("expected extended suffix reuse to match classic path: got=%v want=%v", gotWide, wantWideIDs)
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

	expr := qx.GTE("age", 2_500)
	entry, br := warmNumericRangeBucketEntry(t, db, expr)

	pred, ok := db.engine.buildPredicateWithMode(expr, true)
	if !ok {
		t.Fatal("expected range predicate build to succeed")
	}
	released := false
	t.Cleanup(func() {
		if !released {
			releasePredicates([]predicate{pred})
		}
	})

	start, end, ok := entry.Index().FullBucketSpan(br)
	if !ok {
		t.Fatal("expected full bucket span")
	}
	cachedBefore, ok := entry.LoadFullSpan(start, end)
	if !ok || cachedBefore.IsEmpty() {
		t.Fatal("expected cached full-span posting after predicate build")
	}

	releasePredicates([]predicate{pred})
	released = true

	cachedAfter, ok := entry.LoadFullSpan(start, end)
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

	expr := qx.GTE("age", 2_500)
	entry, br := warmNumericRangeBucketEntry(t, db, expr)

	pred, ok := db.engine.buildPredicateWithMode(expr, true)
	if !ok {
		t.Fatal("expected range predicate build to succeed after patches")
	}
	released := false
	t.Cleanup(func() {
		if !released {
			releasePredicates([]predicate{pred})
		}
	})

	start, end, ok := entry.Index().FullBucketSpan(br)
	if !ok {
		t.Fatal("expected full bucket span")
	}
	cachedBefore, ok := entry.LoadFullSpan(start, end)
	if !ok || cachedBefore.IsEmpty() {
		t.Fatal("expected cached full-span posting after predicate build")
	}

	releasePredicates([]predicate{pred})
	released = true

	cachedAfter, ok := entry.LoadFullSpan(start, end)
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

	snap := db.engine.snapshot.Current()
	fm := db.engine.schema.Fields["age"]
	if fm == nil {
		t.Fatalf("expected age field metadata")
	}
	view := db.engine.currentQueryViewForTests()
	ov := view.fieldIndexViewFromSlotsByName(view.snap.Index, "age")
	if !ov.HasData() {
		t.Fatalf("expected age overlay data")
	}

	key, isSlice, isNil, err := db.engine.exprValueToIdxScalar(qx.GTE("age", 0))
	if err != nil {
		t.Fatalf("exprValueToIdxScalar: %v", err)
	}
	if isSlice || isNil {
		t.Fatalf("unexpected scalar flags for age: isSlice=%v isNil=%v", isSlice, isNil)
	}
	var rb indexdata.Bounds
	rb.ApplyLo(key, true)
	br := ov.RangeForBounds(rb)

	out, ok := db.engine.currentQueryViewForTests().tryEvalNumericRangeBuckets("age", fm, ov, br)
	if !ok {
		t.Fatal("expected numeric range bucket path")
	}
	out.ids.Release()

	entry := requireNumericRangeBucketCacheEntry(t, snap, "age")
	start, end, ok := entry.Index().FullBucketSpan(br)
	if !ok {
		t.Fatal("expected full bucket span")
	}
	if cached, ok := entry.LoadFullSpan(start, end); ok && !cached.IsEmpty() {
		t.Fatal("expected oversized full-span posting to be rejected by cache guard")
	}
}

func TestNumericRangeBucketSpanCache_LoadHotPathAllocsStayLowAfterWarmup(t *testing.T) {
	if testRaceEnabled {
		t.Skip("testing.AllocsPerRun is not stable under -race")
	}

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

	fm := db.engine.schema.Fields["age"]
	if fm == nil {
		t.Fatalf("expected age field metadata")
	}
	view := db.engine.currentQueryViewForTests()
	ov := view.fieldIndexViewFromSlotsByName(view.snap.Index, "age")
	if !ov.HasData() {
		t.Fatalf("expected age overlay data")
	}
	qv := db.engine.currentQueryViewForTests()

	br := ov.RangeByRanks(2501, 3001)
	warm, ok := db.engine.currentQueryViewForTests().tryEvalNumericRangeBuckets("age", fm, ov, br)
	if !ok {
		t.Fatal("expected numeric range bucket warmup path")
	}
	warm.ids.Release()

	allocs := testing.AllocsPerRun(20, func() {
		out, ok := qv.tryLoadNumericRangeBuckets("age", fm, ov, br)
		if !ok {
			t.Fatal("expected warmed numeric range bucket load path")
		}
		out.ids.Release()
	})
	if allocs > 12 {
		t.Fatalf("unexpected allocs per run on warmed numeric bucket load: got=%v want<=12", allocs)
	}
}

func TestTryEvalNumericRangeBuckets_ColdBuildAllocsStayLowAfterWarmup(t *testing.T) {
	if testRaceEnabled {
		t.Skip("testing.AllocsPerRun is not stable under -race")
	}

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

	fm := db.engine.schema.Fields["age"]
	if fm == nil {
		t.Fatalf("expected age field metadata")
	}
	view := db.engine.currentQueryViewForTests()
	ov := view.fieldIndexViewFromSlotsByName(view.snap.Index, "age")
	if !ov.HasData() {
		t.Fatalf("expected age overlay data")
	}
	qv := db.engine.currentQueryViewForTests()
	br := ov.RangeByRanks(2501, 3001)

	warm, ok := qv.tryEvalNumericRangeBuckets("age", fm, ov, br)
	if !ok {
		t.Fatal("expected numeric range bucket warmup path")
	}
	warm.ids.Release()
	db.clearCurrentSnapshotCachesForTesting()

	allocs := testing.AllocsPerRun(100, func() {
		db.clearCurrentSnapshotCachesForTesting()
		out, ok := qv.tryEvalNumericRangeBuckets("age", fm, ov, br)
		if !ok {
			t.Fatal("expected numeric range bucket cold build path")
		}
		out.ids.Release()
	})
	if allocs != 0 {
		t.Fatalf("unexpected allocs after warmup: got=%v want=0", allocs)
	}
}

func TestMaterializeOrderBasicLimitComplementBaseOp_AllocsPerRunStayZeroAfterWarmup(t *testing.T) {
	if testRaceEnabled {
		t.Skip("testing.AllocsPerRun is not stable under -race")
	}

	db, _ := openTempDBUint64(t, Options{
		SnapshotMaterializedPredCacheMaxEntries: 64,
	})

	seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   i,
			Score: float64(20_000 - i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	setNumericBucketKnobs(t, db, 128, 1, 1)

	qv := db.engine.currentQueryViewForTests()
	op := mustTestQIRExprForDB(t, db, qx.GTE("age", 2_500))
	stats, ok := qv.orderBasicRawBaseOpStats(op, qv.snap.Universe.Cardinality())
	if !ok || stats.cacheKey.IsZero() || !stats.buildComplement {
		t.Fatalf("expected complement materialization stats for %v", op)
	}

	if !qv.materializeOrderBasicLimitComplementBaseOp(op, stats.cacheKey) {
		t.Fatal("expected warmup materialization")
	}
	db.clearCurrentSnapshotCachesForTesting()

	allocs := testing.AllocsPerRun(100, func() {
		db.clearCurrentSnapshotCachesForTesting()
		if !qv.materializeOrderBasicLimitComplementBaseOp(op, stats.cacheKey) {
			t.Fatal("expected cold materialization")
		}
	})
	if allocs != 0 {
		t.Fatalf("unexpected allocs after warmup: got=%v want=0", allocs)
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

	fm := db.engine.schema.Fields["age"]
	if fm == nil {
		t.Fatalf("expected age field metadata")
	}
	view := db.engine.currentQueryViewForTests()
	ov := view.fieldIndexViewFromSlotsByName(view.snap.Index, "age")
	if !ov.HasData() {
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
		got, ok := db.engine.currentQueryViewForTests().trySnapshotNumericRangeCardinality("age", fm, ov, tc.start, tc.end)
		if !ok {
			t.Fatalf("trySnapshotNumericRangeCardinality(%d,%d) failed", tc.start, tc.end)
		}
		_, want := ov.RangeStats(ov.RangeByRanks(tc.start, tc.end))
		if got != want {
			t.Fatalf("range count mismatch for [%d:%d): got=%d want=%d", tc.start, tc.end, got, want)
		}
	}
}
