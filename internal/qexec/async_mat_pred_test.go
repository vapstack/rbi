package qexec

import (
	"fmt"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/snapshot"
)

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
		SnapshotMaterializedPredCacheMaxEntries: 16,
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
	scheduler := newAsyncMaterializedPredScheduler(1)
	taskKey := asyncMaterializedPredTaskKey{seq: 7}
	scheduler.slots <- struct{}{}
	scheduler.inFlight[taskKey] = struct{}{}
	scheduler.inFlightCount.Store(1)

	unpinned := false
	scheduler.finish(AsyncMaterializedPredSnapshotOps{
		Unpin: func(seq uint64, ref *snapshot.Ref) {
			if seq != taskKey.seq {
				t.Fatalf("Unpin seq=%d want %d", seq, taskKey.seq)
			}
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
		},
	}, taskKey.seq, nil, taskKey)

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
		SnapshotMaterializedPredCacheMaxEntries: 16,
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
		SnapshotMaterializedPredCacheMaxEntries: 16,
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
	if got := numericRangeFullSpanCacheEntryCount(entry); got != 0 {
		t.Fatalf("stale numeric bucket warmup stored %d full-span entries", got)
	}
}

func TestBuildAsyncRangeComplementMaterializedPredCancelsBeforeNilTailClone(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		SnapshotMaterializedPredCacheMaxEntries: 16,
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
		SnapshotMaterializedPredCacheMaxEntries: 16,
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

func TestAsyncMaterializedPredWarmupStoresStringRangeComplement(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		SnapshotMaterializedPredCacheMaxEntries: 16,
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
		SnapshotMaterializedPredCacheMaxEntries: 16,
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
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 128, func(i int) *Rec {
		return &Rec{Age: i}
	})

	view, plan := asyncMaterializedPredPlanForTest(t, db, qx.GTE("age", 64))
	plan.seq = view.snap.Seq
	scheduler := db.engine.exec.asyncMaterializedPredWarm
	taskKey := asyncMaterializedPredTaskKey{seq: plan.seq, key: plan.key}
	origOps := scheduler.ops
	pinCalls := 0
	scheduler.ops.PinBySeq = func(seq uint64) (*snapshot.View, *snapshot.Ref, bool) {
		pinCalls++
		return origOps.PinBySeq(seq)
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
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 128, func(i int) *Rec {
		return &Rec{Age: i}
	})

	view, plan := asyncMaterializedPredPlanForTest(t, db, qx.GTE("age", 64))
	scheduler := db.engine.exec.asyncMaterializedPredWarm
	origOps := scheduler.ops
	pinCalls := 0
	scheduler.ops.PinBySeq = func(seq uint64) (*snapshot.View, *snapshot.Ref, bool) {
		pinCalls++
		return origOps.PinBySeq(seq)
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
		SnapshotMaterializedPredCacheMaxEntries: 16,
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
		SnapshotMaterializedPredCacheMaxEntries: 16,
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
