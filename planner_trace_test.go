package rbi

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/vapstack/qx"
)

func TestTracer_EmitsAndSamples(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	sink := func(ev TraceEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSink:        sink,
		TraceSampleEvery: 2,
	})
	_ = seedData(t, db, 5_000)

	q := qx.Query(
		qx.EQ("active", true),
		qx.GTE("age", 20),
	).Limit(100)

	for i := 0; i < 5; i++ {
		ids, err := db.QueryKeys(q)
		if err != nil {
			t.Fatalf("QueryKeys: %v", err)
		}
		if len(ids) == 0 {
			t.Fatalf("expected non-empty ids")
		}
	}

	mu.Lock()
	got := append([]TraceEvent(nil), events...)
	mu.Unlock()

	// sampleEvery=2 emits for query #2 and #4.
	if len(got) != 2 {
		t.Fatalf("unexpected trace events count: got=%d want=%d", len(got), 2)
	}

	for i, ev := range got {
		if ev.Plan == "" {
			t.Fatalf("event %d: empty plan", i)
		}
		if ev.Duration <= 0 {
			t.Fatalf("event %d: expected positive duration", i)
		}
		if ev.RowsReturned == 0 {
			t.Fatalf("event %d: expected positive rows returned", i)
		}
	}
}

func TestTracer_ORDecisionEstimates(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	sink := func(ev TraceEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSink:        sink,
		TraceSampleEvery: 1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.OR(
			qx.EQ("active", true),
			qx.EQ("name", "alice"),
		),
	).Limit(120)

	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) == 0 {
		t.Fatalf("expected non-empty ids")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]

	if !strings.HasPrefix(ev.Plan, "plan_or_merge_") {
		t.Fatalf("expected OR planner plan, got %q", ev.Plan)
	}
	if ev.EstimatedRows == 0 {
		t.Fatalf("expected estimated rows to be set")
	}
	if ev.EstimatedCost <= 0 {
		t.Fatalf("expected estimated cost to be positive")
	}
	if ev.RowsExamined == 0 {
		t.Fatalf("expected rows examined to be recorded")
	}
	if ev.RowsReturned != uint64(len(ids)) {
		t.Fatalf("rows returned mismatch: ev=%d ids=%d", ev.RowsReturned, len(ids))
	}
	if len(ev.ORBranches) == 0 {
		t.Fatalf("expected OR branch trace")
	}
	var branchExamined uint64
	var branchEmitted uint64
	for _, b := range ev.ORBranches {
		branchExamined += b.RowsExamined
		branchEmitted += b.RowsEmitted
	}
	if branchExamined == 0 {
		t.Fatalf("expected OR branch examined rows to be recorded")
	}
	if branchEmitted == 0 {
		t.Fatalf("expected OR branch emitted rows to be recorded")
	}
	if ev.EarlyStopReason == "" {
		t.Fatalf("expected early stop reason to be set")
	}
	if ev.OrderIndexScanWidth != 0 {
		t.Fatalf("expected no order-index scan width for unordered OR query, got=%d", ev.OrderIndexScanWidth)
	}
}

func TestTracer_OROrderMetrics(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	sink := func(ev TraceEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSink:        sink,
		TraceSampleEvery: 1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.OR(
			qx.EQ("active", true),
			qx.EQ("name", "alice"),
		),
	).Sort("age", qx.ASC).Limit(80)

	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) == 0 {
		t.Fatalf("expected non-empty ids")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]

	if !strings.HasPrefix(ev.Plan, "plan_or_merge_order_") {
		t.Fatalf("expected ordered OR planner plan, got %q", ev.Plan)
	}
	if len(ev.ORBranches) == 0 {
		t.Fatalf("expected OR branch trace")
	}

	var branchExamined uint64
	var branchEmitted uint64
	for _, b := range ev.ORBranches {
		branchExamined += b.RowsExamined
		branchEmitted += b.RowsEmitted
	}

	if branchExamined == 0 {
		t.Fatalf("expected OR branch examined rows to be recorded")
	}
	if branchEmitted == 0 {
		t.Fatalf("expected OR branch emitted rows to be recorded")
	}
	if ev.OrderIndexScanWidth == 0 {
		t.Fatalf("expected order-index scan width to be recorded")
	}
	if ev.EarlyStopReason == "" {
		t.Fatalf("expected early stop reason to be set")
	}
}

func TestTracer_OROrderPlannerAnalysisMetrics(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 256, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   18 + i,
			Score: float64(i),
		}
	})

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.GTE("age", 30),
				qx.LTE("age", 250),
			),
			qx.EQ("name", "u_1"),
		),
	).Sort("score", qx.DESC).Limit(240)

	db.clearCurrentSnapshotCachesForTesting()
	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)

	warm, ok := db.buildPredicatesOrderedWithMode(
		[]qx.Expr{qx.GTE("age", 30), qx.LTE("age", 250)},
		"score", false, 4096, 0, false, false,
	)
	if !ok {
		t.Fatalf("warm buildPredicatesOrderedWithMode: ok=false")
	}
	if len(warm) != 1 {
		releasePredicates(warm)
		t.Fatalf("unexpected warm predicate count: %d", len(warm))
	}
	if !warm[0].hasEffectiveBounds {
		releasePredicates(warm)
		t.Fatal("expected merged warm predicate with effective bounds")
	}
	if !view.materializeOrderedORPredicate(&warm[0]) {
		releasePredicates(warm)
		t.Fatal("expected warm predicate to materialize")
	}
	releasePredicates(warm)
	if got := db.getSnapshot().matPredCacheCount.Load(); got == 0 {
		t.Fatalf("expected prewarmed materialized predicate cache")
	}

	preparedQ, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer preparedQ.Release()

	window, _ := orderWindowForTest(q)
	branches, alwaysFalse, ok := db.buildORBranchesOrdered(q.Filter.Args, "score", window)
	if !ok {
		t.Fatalf("buildORBranchesOrdered: ok=false")
	}
	if alwaysFalse {
		branches.Release()
		t.Fatalf("unexpected alwaysFalse for ordered OR branches")
	}
	defer branches.Release()

	analysis, ok := view.buildOROrderAnalysis(&viewQ, branches)
	if !ok {
		t.Fatalf("buildOROrderAnalysis: ok=false")
	}
	defer analysis.release()

	for bi := 0; bi < branches.Len(); bi++ {
		branch := branches.Get(bi)
		for pi := 0; pi < branch.predLen(); pi++ {
			_ = view.orderedORPredicateBuildInfoForBranch("score", branch.pred(pi), &analysis, branch, bi, pi)
		}
	}

	trace := &queryTrace{
		sink: func(TraceEvent) {},
	}
	if !view.maybeWarmMaterializeOrderedORPredicates(&viewQ, branches, &analysis, trace) {
		t.Fatalf("expected warm ordered-OR materialization to rewrite predicates")
	}
	ev := trace.ev
	if ev.ORRoute.PlannerAnalysisTime <= 0 {
		t.Fatalf("expected positive planner analysis time, trace=%+v", ev.ORRoute)
	}
	if ev.ORRoute.PlannerPredicates == 0 {
		t.Fatalf("expected planner predicate analysis count, trace=%+v", ev.ORRoute)
	}
	if ev.ORRoute.PlannerExactRanges+ev.ORRoute.PlannerReusedRanges == 0 {
		t.Fatalf("expected planner range analysis counters, trace=%+v", ev.ORRoute)
	}
	if ev.ORRoute.PlannerCacheHits+ev.ORRoute.PlannerBuilds == 0 {
		t.Fatalf("expected planner analysis to observe cache hits or builds, trace=%+v", ev.ORRoute)
	}
}

func TestTracer_OROrderPlannerAnalysisRangeCountersNotDoubleCountAcrossPhases(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 256, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   18 + i,
			Score: float64(i),
		}
	})

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.GTE("age", 30),
				qx.LTE("age", 250),
			),
			qx.EQ("name", "u_1"),
		),
	).Sort("score", qx.DESC).Limit(240)

	db.clearCurrentSnapshotCachesForTesting()
	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)

	warm, ok := db.buildPredicatesOrderedWithMode(
		[]qx.Expr{qx.GTE("age", 30), qx.LTE("age", 250)},
		"score", false, 4096, 0, false, false,
	)
	if !ok {
		t.Fatalf("warm buildPredicatesOrderedWithMode: ok=false")
	}
	if len(warm) != 1 {
		releasePredicates(warm)
		t.Fatalf("unexpected warm predicate count: %d", len(warm))
	}
	if !warm[0].hasEffectiveBounds {
		releasePredicates(warm)
		t.Fatal("expected merged warm predicate with effective bounds")
	}
	if !view.materializeOrderedORPredicate(&warm[0]) {
		releasePredicates(warm)
		t.Fatal("expected warm predicate to materialize")
	}
	releasePredicates(warm)

	preparedQ, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer preparedQ.Release()

	window, _ := orderWindowForTest(q)
	branches, alwaysFalse, ok := db.buildORBranchesOrdered(q.Filter.Args, "score", window)
	if !ok {
		t.Fatalf("buildORBranchesOrdered: ok=false")
	}
	if alwaysFalse {
		branches.Release()
		t.Fatalf("unexpected alwaysFalse for ordered OR branches")
	}
	defer branches.Release()

	analysis, ok := view.buildOROrderAnalysis(&viewQ, branches)
	if !ok {
		t.Fatalf("buildOROrderAnalysis: ok=false")
	}
	defer analysis.release()

	trace := &queryTrace{
		sink: func(TraceEvent) {},
	}
	view.maybeWarmMaterializeOrderedORPredicates(&viewQ, branches, &analysis, trace)
	view.maybeEagerMaterializeOrderedORPredicates(&viewQ, branches, &analysis, false, trace)

	if got, want := trace.ev.ORRoute.PlannerExactRanges, uint64(analysis.exactUniverses); got != want {
		t.Fatalf("PlannerExactRanges = %d, want %d", got, want)
	}
	if got, want := trace.ev.ORRoute.PlannerReusedRanges, uint64(analysis.reusedUniverses); got != want {
		t.Fatalf("PlannerReusedRanges = %d, want %d", got, want)
	}
}

func TestTracer_QueryValuesPathEmitsTrace(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	sink := func(ev TraceEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSink:        sink,
		TraceSampleEvery: 1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.EQ("active", true),
		qx.GTE("age", 22),
		qx.LT("age", 45),
	).Sort("age", qx.ASC).Limit(80)

	items, err := db.Query(q)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(items) == 0 {
		t.Fatalf("expected non-empty items")
	}
	db.ReleaseRecords(items...)

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	if ev.Plan != string(PlanLimitOrderBasic) {
		t.Fatalf("expected %q plan, got %q", PlanLimitOrderBasic, ev.Plan)
	}
	if ev.RowsReturned != uint64(len(items)) {
		t.Fatalf("rows returned mismatch: ev=%d items=%d", ev.RowsReturned, len(items))
	}
	if ev.RowsExamined == 0 {
		t.Fatalf("expected rows examined to be recorded")
	}
}

func TestTracer_CountPathEmitsTrace(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	sink := func(ev TraceEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSink:        sink,
		TraceSampleEvery: 1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.EQ("active", true),
		qx.IN("country", []string{"NL", "DE"}),
		qx.GTE("age", 20),
	)

	cnt, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if cnt == 0 {
		t.Fatalf("expected non-zero count")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	if !strings.HasPrefix(ev.Plan, "plan_count_") {
		t.Fatalf("expected count plan, got %q", ev.Plan)
	}
	if ev.RowsReturned != cnt {
		t.Fatalf("rows returned mismatch: ev=%d count=%d", ev.RowsReturned, cnt)
	}
	if ev.Duration <= 0 {
		t.Fatalf("expected positive duration")
	}
}

func TestTracer_CountPathTracksBroadRangePrepareMetrics(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	sink := func(ev TraceEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSink:        sink,
		TraceSampleEvery: 1,
	})

	countries := []string{"US", "DE", "FR", "GB"}
	seedGeneratedUint64Data(t, db, 160_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%06d@example.com", i),
			Age:    i,
			Score:  float64(i % 1_000),
			Active: i%2 == 0,
			Meta: Meta{
				Country: countries[i%len(countries)],
			},
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.EQ("country", "US"),
		qx.NOTIN("active", []bool{false}),
		qx.GTE("age", 35_000),
	)

	first, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("first Count: %v", err)
	}
	second, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("second Count: %v", err)
	}
	third, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("third Count: %v", err)
	}
	if first == 0 || second != first || third != first {
		t.Fatalf("unexpected counts: first=%d second=%d third=%d", first, second, third)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) < 3 {
		t.Fatalf("expected at least three trace events, got %d", len(events))
	}
	ev1 := events[len(events)-3]
	ev2 := events[len(events)-2]
	ev3 := events[len(events)-1]
	if ev1.Plan != string(PlanCountPredicates) {
		t.Fatalf("expected first plan %q, got %q", PlanCountPredicates, ev1.Plan)
	}
	if ev1.CountPredicatePreparations == 0 {
		t.Fatalf("expected first trace to record count predicate preparation")
	}
	if ev1.CountRangeComplementBuilds != 0 {
		t.Fatalf("expected first trace to keep complement local, got %d builds", ev1.CountRangeComplementBuilds)
	}
	if ev1.CountRangeComplementCacheHits != 0 {
		t.Fatalf("expected first trace to have no complement cache hit, got %d", ev1.CountRangeComplementCacheHits)
	}
	if ev2.CountRangeComplementBuilds == 0 {
		t.Fatalf("expected second trace to record complement build after promotion")
	}
	if ev2.CountRangeComplementCacheHits != 0 {
		t.Fatalf("expected second trace to avoid complement cache hit, got %d", ev2.CountRangeComplementCacheHits)
	}
	if ev3.CountRangeComplementCacheHits == 0 {
		t.Fatalf("expected third trace to record complement cache hit")
	}
	if ev3.CountRangeComplementBuilds != 0 {
		t.Fatalf("expected third trace to skip complement rebuild, got %d builds", ev3.CountRangeComplementBuilds)
	}
}
