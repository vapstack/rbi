package qexec

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/rbistats"
	"github.com/vapstack/rbi/rbitrace"
)

func TestTracer_EmitsAndSamples(t *testing.T) {
	var (
		mu     sync.Mutex
		events []rbitrace.Event
	)

	sink := func(ev rbitrace.Event) {
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
	got := append([]rbitrace.Event(nil), events...)
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

func TestTracer_BeginTraceCollectsWideANDLeafMetadata(t *testing.T) {
	ops := make([]qir.Expr, 9)
	for i := range ops {
		ops[i] = qir.Expr{Op: qir.OpEQ, FieldOrdinal: i}
	}
	ops[1].Not = true
	ops[2].Op = qir.OpPREFIX

	trace := NewRuntime(Config{TraceSink: func(rbitrace.Event) {}, TraceSampleEvery: 1}).BeginTrace(qir.Shape{
		Expr: qir.Expr{
			Op:           qir.OpAND,
			FieldOrdinal: qir.NoFieldOrdinal,
			Operands:     ops,
		},
	}, "")

	ev := trace.Event()
	if ev.LeafCount != len(ops) {
		t.Fatalf("LeafCount=%d want %d", ev.LeafCount, len(ops))
	}
	if !ev.HasNeg {
		t.Fatalf("expected HasNeg")
	}
	if !ev.HasPrefix {
		t.Fatalf("expected HasPrefix")
	}
}

func TestTracer_ORDecisionEstimates(t *testing.T) {
	var (
		mu     sync.Mutex
		events []rbitrace.Event
	)

	sink := func(ev rbitrace.Event) {
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

	if !strings.HasPrefix(string(ev.Plan), "plan_or_merge_") {
		t.Fatalf("expected OR planner plan, got %q", ev.Plan)
	}
	if ev.ORRoute.Selected == "" {
		t.Fatalf("expected OR selector route, trace=%+v", ev.ORRoute)
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
		events []rbitrace.Event
	)

	sink := func(ev rbitrace.Event) {
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

	if !strings.HasPrefix(string(ev.Plan), "plan_or_merge_order_") {
		t.Fatalf("expected ordered OR planner plan, got %q", ev.Plan)
	}
	if ev.ORRoute.Selected == "" {
		t.Fatalf("expected ordered OR selector route, trace=%+v", ev.ORRoute)
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

func TestTracer_OrderedLimitRouteWorkSeparatesExactBucketFilter(t *testing.T) {
	var events []rbitrace.Event
	db := newTestDB(t, testOptions{
		TraceSink: func(ev rbitrace.Event) {
			events = append(events, ev)
		},
		TraceSampleEvery: 1,
	})
	_ = db.seedData(t, 20_000)

	q := qx.Query(
		qx.PREFIX("full_name", "FN-"),
		qx.EQ("active", true),
	).Sort("full_name", qx.ASC).Limit(12)

	prepared, shape, err := db.prepareQuery(q)
	if err != nil {
		t.Fatalf("prepareQuery: %v", err)
	}
	defer prepared.Release()

	ids, err := db.view().Query(&shape, true)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(ids) == 0 {
		t.Fatalf("expected non-empty ids")
	}
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}

	route := events[len(events)-1].OrderedLimitRoute
	if route.Selected != plannerOrderedLimitCandidateOrderScan.String() {
		t.Fatalf("selected=%q want %q route=%+v", route.Selected, plannerOrderedLimitCandidateOrderScan.String(), route)
	}
	if route.SelectedWork.CandidateScan <= 0 || route.SelectedWork.ExactBucketFilter <= 0 {
		t.Fatalf("ordered LIMIT trace work did not preserve scan/exact-filter components: %+v", route.SelectedWork)
	}
	if route.SelectedWork.PostingContains != 0 {
		t.Fatalf("exact bucket filter leaked into posting contains: %+v", route.SelectedWork)
	}
}

func TestTracer_OROrderRouteWorkSeparatesExactBucketPenalty(t *testing.T) {
	db := newTestDB(t, testOptions{})
	_ = db.seedData(t, 8)
	view := db.view()

	shape := qir.Shape{
		HasOrder: true,
		Order:    qir.Order{FieldOrdinal: 0, Kind: qir.OrderKindBasic},
		Limit:    100,
	}
	facts := plannerORFacts{
		ordered:       true,
		universe:      100_000,
		unionCard:     50_000,
		sumCard:       75_000,
		branchCount:   2,
		orderDistinct: 1_000,
		orderStats: rbistats.PlannerField{
			DistinctKeys:    1_000,
			NonEmptyKeys:    1_000,
			TotalBucketCard: 100_000,
			AvgBucketCard:   100,
			P95BucketCard:   180,
			MaxBucketCard:   220,
		},
	}
	facts.branchCards[0] = 45_000
	facts.branchCards[1] = 30_000
	facts.branchesFacts[0] = plannerORBranchFact{
		card:              45_000,
		hasLead:           true,
		leadEst:           45_000,
		hasPrefixTailRisk: true,
	}
	facts.branchesFacts[1] = plannerORBranchFact{
		card:    30_000,
		hasLead: true,
		leadEst: 30_000,
	}
	facts.mergeStats[0] = plannerOROrderMergeBranchStats{streamChecks: 3, mergeChecks: 1}
	facts.mergeStats[1] = plannerOROrderMergeBranchStats{streamChecks: 1, mergeChecks: 1}

	need := 100
	cost := facts.orderMergeCost(uint64(need))
	cost *= plannerOROrderExactBucketApplyPenalty(&facts.mergeStats, facts.branchCount)
	cost *= plannerOROrderPrefixTailRiskPenalty(true, facts.branchCount, 0)

	decision := plannerOROrderDecision{
		selected: plannerOROrderCandidate{
			kind:              plannerOROrderCandidateKWayMerge,
			cost:              cost,
			expectedRows:      estimateRowsForNeed(uint64(need), facts.unionCard, facts.universe),
			unionRows:         facts.unionCard,
			sumRows:           facts.sumCard,
			avgChecks:         facts.avgMergeChecks(),
			overlap:           float64(facts.sumCard) / float64(facts.unionCard),
			hasPrefixTailRisk: true,
		},
		rejected: plannerOROrderCandidate{
			kind:         plannerOROrderCandidateStream,
			cost:         float64(need) * 4,
			expectedRows: uint64(need),
			avgChecks:    facts.orderStreamChecks(),
		},
	}

	route := view.traceOROrderRoute(&shape, &facts, decision)
	work := route.SelectedWork
	if work.CandidateScan <= 0 || work.BranchMerge <= 0 || work.ExactBucketFilter <= 0 || work.TailRiskPenalty <= 0 {
		t.Fatalf("ordered OR trace work did not preserve merge/exact/tail components: %+v", work)
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
	view := db.engine.currentQueryViewForTests()

	warm, ok := db.engine.buildPredicatesOrderedWithMode(
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
	if got := db.engine.snapshot.Current().MaterializedPredCacheEntryCount(); got == 0 {
		t.Fatalf("expected prewarmed materialized predicate cache")
	}

	preparedQ, viewQ, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer preparedQ.Release()

	window, _ := orderWindowForTest(q)
	branches, alwaysFalse, ok := db.engine.buildORBranchesOrdered(q.Filter.Args, "score", window)
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
		branch := branches.owner[bi]
		for pi := 0; pi < branch.preds.Len(); pi++ {
			_ = view.orderedORPredicateBuildInfoForBranch("score", branch.preds.owner[pi], &analysis, branch, bi, pi)
		}
	}

	trace := NewRuntime(Config{TraceSink: func(rbitrace.Event) {}, TraceSampleEvery: 1}).BeginTrace(viewQ, "score")
	if !view.maybeMaterializeOrderedORPredicates(&viewQ, branches, &analysis, false, false, trace) {
		t.Fatalf("expected warm ordered-OR materialization to rewrite predicates")
	}
	ev := trace.Event()
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
	view := db.engine.currentQueryViewForTests()

	warm, ok := db.engine.buildPredicatesOrderedWithMode(
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

	preparedQ, viewQ, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer preparedQ.Release()

	window, _ := orderWindowForTest(q)
	branches, alwaysFalse, ok := db.engine.buildORBranchesOrdered(q.Filter.Args, "score", window)
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

	trace := NewRuntime(Config{TraceSink: func(rbitrace.Event) {}, TraceSampleEvery: 1}).BeginTrace(viewQ, "score")
	view.maybeMaterializeOrderedORPredicates(&viewQ, branches, &analysis, false, false, trace)
	view.maybeEagerMaterializeOrderedORPredicates(&viewQ, branches, &analysis, false, trace)

	ev := trace.Event()
	if got, want := ev.ORRoute.PlannerExactRanges, uint64(analysis.exactUniverses); got != want {
		t.Fatalf("PlannerExactRanges = %d, want %d", got, want)
	}
	if got, want := ev.ORRoute.PlannerReusedRanges, uint64(analysis.reusedUniverses); got != want {
		t.Fatalf("PlannerReusedRanges = %d, want %d", got, want)
	}
}

func TestTracer_ORSelectionRoutePreservesPlannerAnalysis(t *testing.T) {
	trace := NewRuntime(Config{TraceSink: func(rbitrace.Event) {}, TraceSampleEvery: 1}).BeginTrace(qir.Shape{Limit: 1}, "")
	trace.AddOROrderPlannerAnalysis(10*time.Microsecond, 3, 2, 1, 4, 5)
	trace.SetORSelectionRoute(rbitrace.ORRoute{
		Selected:     "stream",
		Rejected:     "materialized_fallback",
		SelectedCost: 12,
		RejectedCost: 34,
		SelectedWork: rbitrace.RouteWork{
			CandidateScan:   8,
			PostingContains: 4,
		},
		RejectedWork: rbitrace.RouteWork{
			MaterializedBuild: 30,
			BranchMerge:       4,
		},
		ExpectedRows:        8,
		UnionRows:           13,
		SumRows:             21,
		Overlap:             1.25,
		AvgChecks:           2.5,
		HasPrefixNonOrder:   true,
		HasSelectiveLead:    true,
		FallbackCollectFast: true,
	})

	ev := trace.Event()
	if ev.ORRoute.PlannerAnalysisTime != 10*time.Microsecond ||
		ev.ORRoute.PlannerPredicates != 3 ||
		ev.ORRoute.PlannerCacheHits != 2 ||
		ev.ORRoute.PlannerBuilds != 1 ||
		ev.ORRoute.PlannerExactRanges != 4 ||
		ev.ORRoute.PlannerReusedRanges != 5 {
		t.Fatalf("planner analysis counters were not preserved: %+v", ev.ORRoute)
	}
	if ev.ORRoute.Selected != "stream" ||
		ev.ORRoute.Rejected != "materialized_fallback" ||
		ev.ORRoute.SelectedCost != 12 ||
		ev.ORRoute.RejectedCost != 34 ||
		ev.ORRoute.SelectedWork.CandidateScan != 8 ||
		ev.ORRoute.SelectedWork.PostingContains != 4 ||
		ev.ORRoute.RejectedWork.MaterializedBuild != 30 ||
		ev.ORRoute.RejectedWork.BranchMerge != 4 ||
		ev.ORRoute.ExpectedRows != 8 ||
		ev.ORRoute.UnionRows != 13 ||
		ev.ORRoute.SumRows != 21 {
		t.Fatalf("selector fields were not preserved: %+v", ev.ORRoute)
	}
	if ev.ORRoute.Overlap != 1.25 ||
		ev.ORRoute.AvgChecks != 2.5 ||
		!ev.ORRoute.HasPrefixNonOrder ||
		!ev.ORRoute.HasSelectiveLead ||
		!ev.ORRoute.FallbackCollectFast {
		t.Fatalf("route decision fields were not recorded: %+v", ev.ORRoute)
	}
}

func TestTracer_QueryValuesPathEmitsTrace(t *testing.T) {
	var (
		mu     sync.Mutex
		events []rbitrace.Event
	)

	sink := func(ev rbitrace.Event) {
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
	if ev.Plan != rbitrace.PlanLimitOrderBasic {
		t.Fatalf("expected %q plan, got %q", rbitrace.PlanLimitOrderBasic, ev.Plan)
	}
	if ev.RowsReturned != uint64(len(items)) {
		t.Fatalf("rows returned mismatch: ev=%d items=%d", ev.RowsReturned, len(items))
	}
	if ev.RowsExamined == 0 {
		t.Fatalf("expected rows examined to be recorded")
	}
}
