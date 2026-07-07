package qexec

import (
	"fmt"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/snapshot"
)

type asyncSnapshotPinFunc func()

func (fn asyncSnapshotPinFunc) Unpin() {
	fn()
}

func asyncMaterializedPredPlanForTest(t testing.TB, db *DB[uint64, Rec], expr qx.Expr) (*View, asyncMaterializedPredPlan) {
	t.Helper()
	view := db.engine.currentQueryViewForTests()
	compiled := mustTestQIRExprForDB(t, db, expr)
	bound, isSlice, err := view.normalizedScalarBoundForExpr(compiled)
	if err != nil || isSlice || bound.empty || bound.full {
		t.Fatalf("normalizedScalarBoundForExpr(%+v): bound=%+v isSlice=%v err=%v", expr, bound, isSlice, err)
	}
	key := view.materializedPredKeyForNormalizedScalarBound(view.exec.FieldNameByOrdinal(compiled.FieldOrdinal), bound)
	if key.IsZero() {
		t.Fatalf("expected materialized predicate key for %+v", expr)
	}
	return view, asyncMaterializedPredPlan{
		key:          key,
		bounds:       rangeBoundsForNormalizedScalarBound(bound),
		fieldOrdinal: compiled.FieldOrdinal,
		kind:         asyncMaterializedPredPlanRange,
	}
}

func TestAsyncMaterializedPredWarmupStoresWhileSnapshotCurrent(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		MaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 2_000, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   i,
			Score: float64(i),
		}
	})

	view, plan := asyncMaterializedPredPlanForTest(t, db, qx.GTE("age", 1_000))
	if !view.scheduleAsyncMaterializedPredWarmup(plan) {
		t.Fatal("expected async warmup schedule")
	}
	db.engine.waitAsyncMaterializedPredWarmupForTests(t)

	if !db.engine.snapshot.Current().HasMaterializedPredKey(plan.key) {
		t.Fatal("expected async warmup to store materialized predicate")
	}
	if stats := db.engine.exec.AsyncMaterializedPredStats(); stats.Stored == 0 {
		t.Fatalf("stored stats not updated: %+v", stats)
	}
}

func TestAsyncMaterializedPredFinishHoldsWorkerSlotUntilUnpin(t *testing.T) {
	scheduler := newAsyncMaterializedPredScheduler(1, true)
	taskKey := asyncMaterializedPredTaskKey{seq: 7}
	scheduler.slots <- struct{}{}
	scheduler.inFlight[taskKey] = struct{}{}
	scheduler.inFlightCount.Store(1)

	unpinned := false
	scheduler.finish(asyncSnapshotPinFunc(func() {
		if got := len(scheduler.slots); got != 1 {
			t.Fatalf("worker slot released before Unpin: len=%d", got)
		}
		if got := scheduler.inFlightCount.Load(); got != 1 {
			t.Fatalf("in-flight count released before Unpin: got=%d", got)
		}
		scheduler.mu.Lock()
		_, ok := scheduler.inFlight[taskKey]
		scheduler.mu.Unlock()
		if !ok {
			t.Fatal("in-flight key released before Unpin")
		}
		unpinned = true
	}), taskKey)

	if !unpinned {
		t.Fatal("Unpin was not called")
	}
	if got := len(scheduler.slots); got != 0 {
		t.Fatalf("worker slot not released after Unpin: len=%d", got)
	}
	if got := scheduler.inFlightCount.Load(); got != 0 {
		t.Fatalf("in-flight count not released after Unpin: got=%d", got)
	}
}

func TestBuildAsyncRangeMaterializedPredUsesNumericBuckets(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		MaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
		return &Rec{Age: i}
	})
	setNumericBucketKnobs(t, db, 128, 1, 1)

	view, plan := asyncMaterializedPredPlanForTest(t, db, qx.GTE("age", 2_500))
	cancel := asyncMaterializedPredCancel{
		CurrentSeq: func() uint64 { return view.snap.Seq },
		Seq:        view.snap.Seq,
		NextProbe:  1<<63 - 1,
	}
	ids, status := view.buildAsyncMaterializedPred(plan, &cancel)
	if status != asyncMaterializedPredBuildOK {
		ids.Release()
		t.Fatalf("build status=%d", status)
	}
	defer ids.Release()
	if got := ids.Cardinality(); got != 17_501 {
		t.Fatalf("materialized cardinality=%d want 17501", got)
	}
	requireNumericRangeBucketCacheEntry(t, db.engine.snapshot.Current(), "age")
}

func TestBuildAsyncRangeMaterializedPredCancelsNumericBucketBuild(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		MaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
		return &Rec{Age: i}
	})
	setNumericBucketKnobs(t, db, 128, 1, 1)

	view, plan := asyncMaterializedPredPlanForTest(t, db, qx.GTE("age", 2_500))
	calls := 0
	cancel := asyncMaterializedPredCancel{
		CurrentSeq: func() uint64 {
			calls++
			if calls <= 3 {
				return view.snap.Seq
			}
			return view.snap.Seq + 1
		},
		Seq: view.snap.Seq,
	}
	ids, status := view.buildAsyncMaterializedPred(plan, &cancel)
	if status != asyncMaterializedPredBuildCanceled {
		ids.Release()
		t.Fatalf("build status=%d want canceled", status)
	}
	if !ids.IsEmpty() {
		ids.Release()
		t.Fatal("canceled build returned ids")
	}
	entry := requireNumericRangeBucketCacheEntry(t, db.engine.snapshot.Current(), "age")
	_ = entry
	if got := numericRangeFullSpanCacheEntryCount(db.engine.snapshot.Current()); got != 0 {
		t.Fatalf("stale numeric bucket warmup stored %d full-span entries", got)
	}
}

func TestBuildAsyncRangeComplementMaterializedPredCancelsBeforeNilTailClone(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		MaterializedPredCacheMaxEntries: 16,
	})
	optA := "a"
	optB := "b"
	seedGeneratedUint64Data(t, db, 3, func(i int) *Rec {
		switch i {
		case 1:
			return &Rec{}
		case 2:
			return &Rec{Opt: &optA}
		default:
			return &Rec{Opt: &optB}
		}
	})

	view, plan := asyncMaterializedPredPlanForTest(t, db, qx.GTE("opt", ""))
	plan.kind = asyncMaterializedPredPlanRangeComplement
	ov := view.indexViewByOrdinal(plan.fieldOrdinal)
	br := ov.RangeForBounds(plan.bounds)
	before, after, ok := fieldIndexComplementRangeSpans(ov, br)
	if !ok || !before.Empty() || !after.Empty() {
		t.Fatalf("expected nil-tail-only complement: before=%+v after=%+v ok=%v", before, after, ok)
	}
	nilPosting := view.nilIndexViewByOrdinal(plan.fieldOrdinal).LookupPostingRetained(indexdata.NilIndexEntryKey)
	if nilPosting.IsEmpty() {
		t.Fatal("expected nil-tail posting")
	}
	nilPosting.Release()

	cancel := asyncMaterializedPredCancel{
		CurrentSeq: func() uint64 { return view.snap.Seq + 1 },
		Seq:        view.snap.Seq,
	}
	ids, status := view.buildAsyncMaterializedPred(plan, &cancel)
	if status != asyncMaterializedPredBuildCanceled {
		ids.Release()
		t.Fatalf("build status=%d want canceled", status)
	}
	if !ids.IsEmpty() {
		ids.Release()
		t.Fatal("canceled nil-tail build returned ids")
	}
}

func TestBuildAsyncRangeComplementMaterializedPredUsesNumericBuckets(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		MaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
		return &Rec{Age: i}
	})
	setNumericBucketKnobs(t, db, 128, 1, 1)

	view, plan := asyncMaterializedPredPlanForTest(t, db, qx.GTE("age", 2_500))
	plan.kind = asyncMaterializedPredPlanRangeComplement
	cancel := asyncMaterializedPredCancel{
		CurrentSeq: func() uint64 { return view.snap.Seq },
		Seq:        view.snap.Seq,
		NextProbe:  1<<63 - 1,
	}
	ids, status := view.buildAsyncMaterializedPred(plan, &cancel)
	if status != asyncMaterializedPredBuildOK {
		ids.Release()
		t.Fatalf("build status=%d", status)
	}
	defer ids.Release()
	if got := ids.Cardinality(); got != 2_499 {
		t.Fatalf("materialized complement cardinality=%d want 2499", got)
	}
	requireNumericRangeBucketCacheEntry(t, db.engine.snapshot.Current(), "age")
}

func TestAsyncMaterializedPredPlanForNumericRangeStoresExactResultNotMaterializedKey(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		MaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 8_000, func(i int) *Rec {
		return &Rec{Age: i}
	})
	setNumericBucketKnobs(t, db, 128, 1, 1)

	view := db.engine.currentQueryViewForTests()
	compiled := mustTestQIRExprForDB(t, db, qx.GTE("age", 2_500))
	bound, isSlice, err := view.normalizedScalarBoundForExpr(compiled)
	if err != nil || isSlice || bound.empty || bound.full {
		t.Fatalf("normalizedScalarBoundForExpr: bound=%+v isSlice=%v err=%v", bound, isSlice, err)
	}
	exactKey := view.materializedPredKeyForNormalizedScalarBound("age", bound)
	if exactKey.IsZero() {
		t.Fatal("expected exact range cache key")
	}
	plan, ok := view.asyncMaterializedPredPlanForPredicate(predicate{expr: compiled}, false)
	if !ok {
		t.Fatal("expected async numeric exact plan")
	}
	if plan.kind != asyncMaterializedPredPlanNumericExact {
		t.Fatalf("plan kind=%d want numeric exact", plan.kind)
	}
	if !view.scheduleAsyncMaterializedPredWarmup(plan) {
		t.Fatal("expected async numeric exact warmup schedule")
	}
	db.engine.waitAsyncMaterializedPredWarmupForTests(t)

	snap := db.engine.snapshot.Current()
	if snap.HasMaterializedPredKey(exactKey) {
		t.Fatal("numeric exact warmup populated materialized predicate key")
	}
	if got := snap.NumericRangeExactResultCacheStats().EntryCount; got != 1 {
		t.Fatalf("expected numeric exact warmup to store one result, got %d", got)
	}
	stats := db.engine.exec.AsyncMaterializedPredStats()
	if stats.NumericExactScheduled == 0 || stats.NumericExactStored == 0 {
		t.Fatalf("numeric exact async stats not updated: %+v", stats)
	}
	if stats.Stored != 0 {
		t.Fatalf("materialized async stored counter changed for numeric exact: %+v", stats)
	}
}

func TestAsyncMaterializedPredPlanForCollapsedNumericRangeUsesNumericKey(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		MaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 12_000, func(i int) *Rec {
		return &Rec{
			Age:   i,
			Score: float64(i),
		}
	})
	setNumericBucketKnobs(t, db, 128, 1, 1)

	view := db.engine.currentQueryViewForTests()
	ageOrdinal := db.engine.schema.IndexedByName["age"].Ordinal
	lo, isSlice, isNil, err := db.engine.exprValueToIdxScalar(qx.GTE("age", 2501))
	if err != nil || isSlice || isNil {
		t.Fatalf("age lo key: key=%+v isSlice=%v isNil=%v err=%v", lo, isSlice, isNil, err)
	}
	hi, isSlice, isNil, err := db.engine.exprValueToIdxScalar(qx.LT("age", 3501))
	if err != nil || isSlice || isNil {
		t.Fatalf("age hi key: key=%+v isSlice=%v isNil=%v err=%v", hi, isSlice, isNil, err)
	}
	var bounds indexdata.Bounds
	bounds.ApplyLo(lo, true)
	bounds.ApplyHi(hi, false)

	cacheKey := view.materializedPredKeyForExactScalarRange("age", bounds)
	if !cacheKey.IsZero() {
		t.Fatalf("bucket-eligible collapsed numeric range should not have materialized key: %v", cacheKey)
	}
	plan, ok := view.asyncMaterializedPredPlanForOrderBasicBaseCore(orderBasicBaseCore{
		kind: orderBasicBaseCoreCollapsedRange,
		collapsed: preparedScalarExactRange{
			field:        "age",
			fieldOrdinal: ageOrdinal,
			bounds:       bounds,
			cacheKey:     cacheKey,
		},
	})
	if !ok || plan.kind != asyncMaterializedPredPlanNumericExact {
		t.Fatalf("expected collapsed numeric exact async plan, ok=%v plan=%+v", ok, plan)
	}
}

func TestBuildAndStoreAsyncNumericRangeSpanCancelsBeforeStaleStore(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		MaterializedPredCacheMaxEntries:  16,
		NumericRangeExactCacheMaxEntries: -1,
	})
	seedGeneratedUint64Data(t, db, 8_000, func(i int) *Rec {
		return &Rec{Age: i}
	})
	setNumericBucketKnobs(t, db, 128, 1, 1)

	view := db.engine.currentQueryViewForTests()
	compiled := mustTestQIRExprForDB(t, db, qx.GTE("age", 2_500))
	plan, ok := view.asyncMaterializedPredPlanForPredicate(predicate{expr: compiled}, false)
	if !ok || plan.kind != asyncMaterializedPredPlanNumericSpan {
		t.Fatalf("expected async numeric span plan, ok=%v plan=%+v", ok, plan)
	}
	cancel := asyncMaterializedPredCancel{
		CurrentSeq: func() uint64 { return view.snap.Seq + 1 },
		Seq:        view.snap.Seq,
		NextProbe:  1<<63 - 1,
	}
	status := view.buildAndStoreAsyncNumericRangeSpan(plan, &cancel)
	if status != asyncMaterializedPredBuildCanceled {
		t.Fatalf("build status=%d want canceled", status)
	}
	if got := view.snap.NumericRangeBucketCache().FullSpanEntryCount(); got != 0 {
		t.Fatalf("stale numeric span warmup stored %d full-span entries", got)
	}
}

func TestBuildAndStoreAsyncNumericRangeExactCancelsBeforeStaleStore(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		MaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 8_000, func(i int) *Rec {
		return &Rec{Age: i}
	})
	setNumericBucketKnobs(t, db, 128, 1, 1)

	view := db.engine.currentQueryViewForTests()
	compiled := mustTestQIRExprForDB(t, db, qx.GTE("age", 2_500))
	plan, ok := view.asyncMaterializedPredPlanForPredicate(predicate{expr: compiled}, false)
	if !ok || plan.kind != asyncMaterializedPredPlanNumericExact {
		t.Fatalf("expected async numeric exact plan, ok=%v plan=%+v", ok, plan)
	}
	calls := 0
	cancel := asyncMaterializedPredCancel{
		CurrentSeq: func() uint64 {
			calls++
			if calls <= 2 {
				return view.snap.Seq
			}
			return view.snap.Seq + 1
		},
		Seq:       view.snap.Seq,
		NextProbe: 1<<63 - 1,
	}
	status := view.buildAndStoreAsyncNumericRangeExact(plan, &cancel)
	if status != asyncMaterializedPredBuildCanceled {
		t.Fatalf("build status=%d want canceled", status)
	}
	if calls < 3 {
		t.Fatalf("expected final stale sequence check, calls=%d", calls)
	}
	if stats := view.snap.NumericRangeExactResultCacheStats(); stats.EntryCount != 0 {
		t.Fatalf("stale numeric exact warmup stored exact result: %+v", stats)
	}
}

func TestAsyncMaterializedPredPlanSkipsRejectedTooLargeNumericSpan(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		MaterializedPredCacheMaxEntries:    16,
		NumericRangeSpanCacheMaxEntries:    8,
		NumericRangeSpanCacheMaxEntryBytes: 1,
		NumericRangeExactCacheMaxEntries:   -1,
	})
	seedGeneratedUint64Data(t, db, 8_000, func(i int) *Rec {
		return &Rec{Age: i}
	})
	setNumericBucketKnobs(t, db, 128, 1, 1)

	view := db.engine.currentQueryViewForTests()
	compiled := mustTestQIRExprForDB(t, db, qx.GTE("age", 2_500))
	plan, ok := view.asyncMaterializedPredPlanForPredicate(predicate{expr: compiled}, false)
	if !ok || plan.kind != asyncMaterializedPredPlanNumericSpan {
		t.Fatalf("expected async numeric span plan, plan=%+v ok=%v", plan, ok)
	}
	if !view.scheduleAsyncMaterializedPredWarmup(plan) {
		t.Fatal("expected first oversized numeric span warmup schedule")
	}
	db.engine.waitAsyncMaterializedPredWarmupForTests(t)

	snap := db.engine.snapshot.Current()
	if got := snap.NumericRangeBucketCache().FullSpanEntryCount(); got != 0 {
		t.Fatalf("oversized numeric span stored %d entries", got)
	}
	if !snap.NumericRangeFullSpanRejectedTooLarge(plan.fieldSlot, plan.startBucket, plan.endBucket) {
		t.Fatal("oversized numeric span reject was not remembered")
	}
	stats := db.engine.exec.AsyncMaterializedPredStats()
	if stats.NumericSpanStoreRejected == 0 {
		t.Fatalf("numeric span reject stat not updated: %+v", stats)
	}

	if plan, ok := view.asyncMaterializedPredPlanForPredicate(predicate{expr: compiled}, false); ok {
		t.Fatalf("rejected oversized numeric span planned again: %+v", plan)
	}
}

func TestAsyncMaterializedPredWarmupStoresStringRangeComplement(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		MaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 128, func(i int) *Rec {
		if i <= 2 {
			return &Rec{Name: fmt.Sprintf("a_%03d", i)}
		}
		return &Rec{Name: fmt.Sprintf("n_%03d", i)}
	})

	view := db.engine.currentQueryViewForTests()
	compiled := mustTestQIRExprForDB(t, db, qx.GT("name", "m"))
	stats, ok := view.orderBasicRawBaseOpStats(compiled, view.snap.Universe.Cardinality())
	if !ok || stats.cacheKey.IsZero() || !stats.buildComplement {
		t.Fatalf("expected string range complement stats: ok=%v stats=%+v", ok, stats)
	}
	plan, ok := view.asyncMaterializedPredPlanForOrderBasicBaseCore(orderBasicBaseCore{
		kind: orderBasicBaseCoreRawExpr,
		expr: compiled,
	})
	if !ok {
		t.Fatal("expected async string range complement plan")
	}
	if plan.kind != asyncMaterializedPredPlanRangeComplement || plan.key != stats.cacheKey {
		t.Fatalf("unexpected async plan: plan=%+v stats=%+v", plan, stats)
	}

	if !view.scheduleAsyncMaterializedPredWarmup(plan) {
		t.Fatal("expected async warmup schedule")
	}
	db.engine.waitAsyncMaterializedPredWarmupForTests(t)

	cached, ok := db.engine.snapshot.Current().LoadMaterializedPredKey(stats.cacheKey)
	if !ok {
		t.Fatal("expected string range complement cache entry")
	}
	defer cached.Release()
	if got := cached.Cardinality(); got != 2 {
		t.Fatalf("string range complement cardinality=%d want 2", got)
	}
	if !cached.Contains(1) || !cached.Contains(2) || cached.Contains(3) || cached.Contains(128) {
		t.Fatal("string range complement cached wrong ids")
	}
}

func TestBuildAsyncMaterializedPredCancelsBeforeUnionFinish(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		MaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 128, func(i int) *Rec {
		return &Rec{Age: i}
	})

	view, plan := asyncMaterializedPredPlanForTest(t, db, qx.GTE("age", 1))
	cancel := asyncMaterializedPredCancel{
		CurrentSeq: func() uint64 { return view.snap.Seq + 1 },
		Seq:        view.snap.Seq,
		NextProbe:  1<<63 - 1,
	}
	ids, status := view.buildAsyncMaterializedPred(plan, &cancel)
	if status != asyncMaterializedPredBuildCanceled {
		ids.Release()
		t.Fatalf("build status=%d want canceled", status)
	}
	if !ids.IsEmpty() {
		ids.Release()
		t.Fatal("canceled build returned ids")
	}
}

func TestAsyncMaterializedPredWarmupDropsDuplicateInFlight(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		MaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 128, func(i int) *Rec {
		return &Rec{Age: i}
	})

	view, plan := asyncMaterializedPredPlanForTest(t, db, qx.GTE("age", 64))
	plan.seq = view.snap.Seq
	scheduler := db.engine.exec.asyncMaterializedPredWarm
	taskKey := plan.taskKey()
	origOps := scheduler.ops
	pinCalls := 0
	scheduler.ops.PinCurrentBySeq = func(seq uint64) (*snapshot.View, AsyncSnapshotPin, bool) {
		pinCalls++
		return origOps.PinCurrentBySeq(seq)
	}
	defer func() {
		scheduler.ops = origOps
	}()

	scheduler.mu.Lock()
	scheduler.inFlight[taskKey] = struct{}{}
	scheduler.mu.Unlock()
	defer func() {
		scheduler.mu.Lock()
		delete(scheduler.inFlight, taskKey)
		scheduler.mu.Unlock()
	}()

	before := db.engine.exec.AsyncMaterializedPredStats()
	if view.scheduleAsyncMaterializedPredWarmup(plan) {
		t.Fatal("duplicate in-flight warmup scheduled")
	}
	after := db.engine.exec.AsyncMaterializedPredStats()
	if after.DroppedInFlight != before.DroppedInFlight+1 {
		t.Fatalf("DroppedInFlight=%d want %d", after.DroppedInFlight, before.DroppedInFlight+1)
	}
	if pinCalls != 0 {
		t.Fatalf("duplicate in-flight path pinned snapshot %d times", pinCalls)
	}
}

func TestAsyncMaterializedPredWarmupDropsWhenWorkerCapacityFull(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		MaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 128, func(i int) *Rec {
		return &Rec{Age: i}
	})

	view, plan := asyncMaterializedPredPlanForTest(t, db, qx.GTE("age", 64))
	scheduler := db.engine.exec.asyncMaterializedPredWarm
	origOps := scheduler.ops
	pinCalls := 0
	scheduler.ops.PinCurrentBySeq = func(seq uint64) (*snapshot.View, AsyncSnapshotPin, bool) {
		pinCalls++
		return origOps.PinCurrentBySeq(seq)
	}
	defer func() {
		scheduler.ops = origOps
	}()
	for i := 0; i < cap(scheduler.slots); i++ {
		scheduler.slots <- struct{}{}
	}
	defer func() {
		for i := 0; i < cap(scheduler.slots); i++ {
			<-scheduler.slots
		}
	}()

	before := db.engine.exec.AsyncMaterializedPredStats()
	if view.scheduleAsyncMaterializedPredWarmup(plan) {
		t.Fatal("warmup scheduled while worker capacity was full")
	}
	after := db.engine.exec.AsyncMaterializedPredStats()
	if after.DroppedCapacity != before.DroppedCapacity+1 {
		t.Fatalf("DroppedCapacity=%d want %d", after.DroppedCapacity, before.DroppedCapacity+1)
	}
	if pinCalls != 0 {
		t.Fatalf("capacity drop path pinned snapshot %d times", pinCalls)
	}
}

func TestAsyncMaterializedPredWarmupCancelsStaleSnapshot(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		MaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 128, func(i int) *Rec {
		return &Rec{Age: i}
	})

	view, plan := asyncMaterializedPredPlanForTest(t, db, qx.GTE("age", 64))
	if err := db.Set(10_000, &Rec{Age: 10_000}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	before := db.engine.exec.AsyncMaterializedPredStats()
	if view.scheduleAsyncMaterializedPredWarmup(plan) {
		t.Fatal("stale snapshot warmup scheduled")
	}
	after := db.engine.exec.AsyncMaterializedPredStats()
	if after.Canceled != before.Canceled+1 {
		t.Fatalf("Canceled=%d want %d", after.Canceled, before.Canceled+1)
	}
	if view.snap.HasMaterializedPredKey(plan.key) {
		t.Fatal("stale snapshot stored materialized predicate")
	}
}

func TestAsyncMaterializedPredPlanDetachesMutableScalarBound(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		MaterializedPredCacheMaxEntries: 16,
	})
	if err := db.Set(1, &Rec{Name: "aa-1"}); err != nil {
		t.Fatalf("Set 1: %v", err)
	}
	if err := db.Set(2, &Rec{Name: "aa-2"}); err != nil {
		t.Fatalf("Set 2: %v", err)
	}
	if err := db.Set(3, &Rec{Name: "bb-1"}); err != nil {
		t.Fatalf("Set 3: %v", err)
	}

	prefix := "aa"
	view := db.engine.currentQueryViewForTests()
	compiled := mustTestQIRExprForDB(t, db, qx.PREFIX("name", &prefix))
	plan, ok := view.asyncMaterializedPredPlanForPredicate(predicate{expr: compiled}, false)
	if !ok {
		t.Fatal("expected async materialized predicate plan")
	}

	prefix = "bb"
	cancel := asyncMaterializedPredCancel{
		CurrentSeq: func() uint64 { return view.snap.Seq },
		Seq:        view.snap.Seq,
		NextProbe:  1<<63 - 1,
	}
	ids, status := view.buildAsyncMaterializedPred(plan, &cancel)
	if status != asyncMaterializedPredBuildOK {
		t.Fatalf("build status=%d", status)
	}
	defer ids.Release()

	if got := ids.Cardinality(); got != 2 {
		t.Fatalf("materialized cardinality=%d want 2", got)
	}
	if !ids.Contains(1) || !ids.Contains(2) || ids.Contains(3) {
		t.Fatalf("materialized predicate used mutated query value")
	}
}
