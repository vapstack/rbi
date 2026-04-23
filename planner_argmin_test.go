package rbi

import (
	"math"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/qir"
)

type plannerORNoOrderArgminCandidates struct {
	kWayCost     float64
	fallbackCost float64
}

func (c plannerORNoOrderArgminCandidates) expectedUse() bool {
	return c.kWayCost <= c.fallbackCost*plannerORNoOrderAdvantage
}

type plannerOrderedArgminCandidates struct {
	orderedCost  float64
	fallbackCost float64
	gainReq      float64
}

func (c plannerOrderedArgminCandidates) expectedUse() bool {
	return c.orderedCost <= c.fallbackCost*c.gainReq
}

type plannerOROrderArgminCandidates struct {
	fallbackCost float64
	streamCost   float64

	mergeCost       float64
	mergeApplicable bool
}

func (c plannerOROrderArgminCandidates) expectedPlan() (plannerOROrderPlan, float64) {
	bestPlan := plannerOROrderFallback
	bestCost := c.fallbackCost
	if c.mergeApplicable && c.mergeCost <= bestCost*plannerOROrderMergeGain {
		bestPlan = plannerOROrderMerge
		bestCost = c.mergeCost
	}
	if c.streamCost <= bestCost*plannerOROrderStreamGain {
		bestPlan = plannerOROrderStream
		bestCost = c.streamCost
	}
	return bestPlan, bestCost
}

func plannerArgminAssertApproxCost(t *testing.T, label string, got, want float64) {
	t.Helper()
	scale := math.Abs(want)
	if scale < 1 {
		scale = 1
	}
	if math.Abs(got-want) > scale*1e-9 {
		t.Fatalf("%s cost mismatch: got=%0.12f want=%0.12f", label, got, want)
	}
}

func plannerArgminOrderedCandidatesForDecision(
	q *qir.Shape,
	leaves []qir.Expr,
	decision plannerOrderedDecision,
) (plannerOrderedArgminCandidates, bool) {
	if q == nil || !q.HasOrder || decision.orderedCost <= 0 || decision.fallbackCost <= 0 {
		return plannerOrderedArgminCandidates{}, false
	}
	hasNeg := false
	for _, e := range leaves {
		if e.Not {
			hasNeg = true
			break
		}
	}
	gainReq := plannerOrderedCostGainDefault
	if !hasNeg && q.Offset == 0 {
		gainReq = plannerOrderedCostGainNoNeg
	}
	if q.Offset > 0 {
		gainReq = 1.02
	}
	return plannerOrderedArgminCandidates{
		orderedCost:  decision.orderedCost,
		fallbackCost: decision.fallbackCost,
		gainReq:      gainReq,
	}, true
}

func plannerArgminOROrderCandidatesForTest[K ~string | ~uint64, V any](
	qv *queryView[K, V],
	q *qir.Shape,
	branches plannerORBranches,
	analysis *plannerOROrderAnalysis,
) (plannerOROrderArgminCandidates, bool) {
	if q == nil || !q.HasOrder || analysis == nil {
		return plannerOROrderArgminCandidates{}, false
	}
	o := q.Order
	if o.Kind != qir.OrderKindBasic {
		return plannerOROrderArgminCandidates{}, false
	}

	fm := qv.fieldMetaByOrder(o)
	if fm == nil || fm.Slice {
		return plannerOROrderArgminCandidates{}, false
	}

	ov := qv.fieldOverlayForOrder(o)
	if !ov.hasData() {
		return plannerOROrderArgminCandidates{}, false
	}
	orderDistinct := uint64(overlayApproxDistinctTotalCount(ov))
	if orderDistinct == 0 {
		return plannerOROrderArgminCandidates{}, false
	}

	need, ok := orderWindow(q)
	if !ok || need <= 0 {
		return plannerOROrderArgminCandidates{}, false
	}

	snap := qv.planner.stats.Load()
	universe := snap.universeOr(analysis.snapshotUniverse)
	if universe == 0 {
		return plannerOROrderArgminCandidates{}, false
	}

	var branchCards [plannerORBranchLimit]uint64
	unionCard, sumCard, branchCount, hasAlwaysTrue := branches.unionCards(universe, &branchCards)
	if unionCard == 0 {
		return plannerOROrderArgminCandidates{}, false
	}

	orderStats := qv.plannerOrderFieldStats(analysis.orderField, snap, universe, orderDistinct)
	expectedRows := estimateRowsForNeed(uint64(need), unionCard, universe)

	candidates := plannerOROrderArgminCandidates{
		fallbackCost: float64(sumCard) + float64(expectedRows),
	}

	streamChecks := branches.orderStreamChecksByBranch(hasAlwaysTrue, analysis.mergeStats)
	candidates.streamCost = float64(expectedRows) * streamChecks * orderStats.skew()
	candidates.streamCost *= qv.root.plannerCostMultiplier(plannerCalOROrderStream)

	mergeNeedLimit := plannerOROrderMergeNeedLimit(need, branches.Len(), unionCard, sumCard, q.Offset)
	candidates.mergeApplicable = !hasAlwaysTrue && need <= mergeNeedLimit
	if !candidates.mergeApplicable {
		return candidates, true
	}

	candidates.mergeCost = branches.orderMergeCost(
		uint64(need),
		&branchCards,
		branchCount,
		unionCard,
		sumCard,
		universe,
		orderStats,
		analysis.mergeStats,
	)
	routeCost, routeOK := qv.estimateOROrderMergeRouteCost(q, branches, need, analysis.mergeStats)
	if routeOK &&
		routeCost.kWay > 0 &&
		routeCost.kWay < candidates.mergeCost &&
		branches.hasFullSpanOrderBranch(analysis.mergeStats) &&
		!routeCost.hasPrefixTailRisk &&
		!branches.hasKWayExactBucketApplyWork(analysis.mergeStats) {
		candidates.mergeCost = routeCost.kWay
	}
	candidates.mergeCost *= qv.root.plannerCostMultiplier(plannerCalOROrderMerge)
	return candidates, true
}

func plannerArgminOROrderMergeQuery() *qx.QX {
	return qx.Query(
		qx.OR(
			qx.AND(
				qx.EQ("country", "DE"),
				qx.HASANY("tags", []string{"rust", "go"}),
				qx.GTE("score", 40.0),
			),
			qx.PREFIX("email", "user1"),
			qx.GTE("age", 30),
		),
	).Sort("score", qx.DESC).Limit(120)
}

func plannerArgminOROrderStreamQuery() *qx.QX {
	return qx.Query(
		qx.OR(
			qx.AND(
				qx.GTE("age", 25),
				qx.EQ("country", "NL"),
			),
			qx.AND(
				qx.LTE("age", 45),
				qx.EQ("name", "alice"),
			),
		),
	).Sort("age", qx.ASC).Limit(120)
}

func plannerArgminOrderedQuery() *qx.QX {
	return qx.Query(
		qx.EQ("country", "NL"),
		qx.HASANY("tags", []string{"go", "db"}),
		qx.GTE("age", 30),
		qx.NOTIN("name", []string{"alice"}),
	).Sort("full_name", qx.ASC).Offset(500).Limit(80)
}

func TestPlannerArgmin_ORNoOrderDecisionMatchesCandidatePolicy(t *testing.T) {
	openDB := func(t *testing.T, calibrated bool) *DB[uint64, Rec] {
		t.Helper()
		db, _ := openTempDBUint64(t, Options{
			AnalyzeInterval:    -1,
			CalibrationEnabled: calibrated,
		})
		_ = seedData(t, db, 2_000)
		setORPlannerStatsSnapshotForTest(db, 100_000, PlannerFieldStats{
			DistinctKeys:    100_000,
			NonEmptyKeys:    100_000,
			TotalBucketCard: 100_000,
			AvgBucketCard:   1,
			MaxBucketCard:   2,
			P50BucketCard:   1,
			P95BucketCard:   2,
		})
		return db
	}

	tests := []struct {
		name       string
		calibrated bool
		calibrate  func(*testing.T, *DB[uint64, Rec])
	}{
		{
			name:       "BaseWinnerIsKWay",
			calibrated: false,
		},
		{
			name:       "CalibrationFlipsWinnerToFallback",
			calibrated: true,
			calibrate: func(t *testing.T, db *DB[uint64, Rec]) {
				t.Helper()
				err := db.SetCalibrationSnapshot(CalibrationSnapshot{
					UpdatedAt: time.Now(),
					Multipliers: map[string]float64{
						"plan_or_merge_no_order": 3.8,
					},
				})
				if err != nil {
					t.Fatalf("SetCalibrationSnapshot: %v", err)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openDB(t, tc.calibrated)
			if tc.calibrate != nil {
				tc.calibrate(t, db)
			}

			q := qx.Query(
				qx.OR(
					qx.EQ("active", true),
					qx.EQ("name", "alice"),
				),
			).Limit(100)

			branches := newPlannerORBranches(3)
			branches.Append(makeORBranchForCalibrationDecisionTest(4_000, 4))
			branches.Append(makeORBranchForCalibrationDecisionTest(4_000, 4))
			branches.Append(makeORBranchForCalibrationDecisionTest(4_000, 4))
			defer branches.Release()

			decision := db.decidePlanORNoOrder(q, branches)
			if decision.kWayCost <= 0 || decision.fallbackCost <= 0 {
				t.Fatalf("expected positive candidate costs: kway=%v fallback=%v", decision.kWayCost, decision.fallbackCost)
			}

			candidates := plannerORNoOrderArgminCandidates{
				kWayCost:     decision.kWayCost,
				fallbackCost: decision.fallbackCost,
			}
			if decision.use != candidates.expectedUse() {
				t.Fatalf(
					"OR no-order winner mismatch: use=%v kway=%v fallback=%v threshold=%v",
					decision.use, decision.kWayCost, decision.fallbackCost, decision.fallbackCost*plannerORNoOrderAdvantage,
				)
			}
		})
	}
}

func TestPlannerArgmin_OROrderDecisionMatchesCandidatePolicy(t *testing.T) {
	tests := []struct {
		name      string
		calibrate func(*testing.T, *DB[uint64, Rec])
		q         func() *qx.QX
		wantPlan  plannerOROrderPlan
	}{
		{
			name: "CalibrationPrefersMerge",
			calibrate: func(t *testing.T, db *DB[uint64, Rec]) {
				t.Helper()
				err := db.SetCalibrationSnapshot(CalibrationSnapshot{
					UpdatedAt: time.Now(),
					Multipliers: map[string]float64{
						"plan_or_merge_order_merge":  0.45,
						"plan_or_merge_order_stream": 2.00,
					},
				})
				if err != nil {
					t.Fatalf("SetCalibrationSnapshot: %v", err)
				}
			},
			q:        plannerArgminOROrderMergeQuery,
			wantPlan: plannerOROrderMerge,
		},
		{
			name:     "StreamWinner",
			q:        plannerArgminOROrderStreamQuery,
			wantPlan: plannerOROrderStream,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := openTempDBUint64(t, Options{
				AnalyzeInterval:    -1,
				CalibrationEnabled: tc.calibrate != nil,
			})
			_ = seedData(t, db, 20_000)
			if tc.calibrate != nil {
				tc.calibrate(t, db)
			}

			q := tc.q()
			window, ok := orderWindowForTest(q)
			if !ok {
				t.Fatalf("orderWindow: ok=false")
			}
			orderField := q.Order[0].By.Name
			branches, alwaysFalse, ok := db.buildORBranchesOrdered(q.Filter.Args, orderField, window)
			if !ok {
				t.Fatalf("buildORBranchesOrdered: ok=false")
			}
			if alwaysFalse {
				t.Fatalf("unexpected alwaysFalse branches")
			}
			defer branches.Release()

			preparedQ, viewQ, err := prepareTestQuery(db, q)
			if err != nil {
				t.Fatalf("prepareTestQuery: %v", err)
			}
			defer preparedQ.Release()

			view := db.currentQueryViewForTests()
			analysis, ok := view.buildOROrderAnalysis(&viewQ, branches)
			if !ok {
				t.Fatalf("buildOROrderAnalysis: ok=false")
			}
			defer analysis.release()

			candidates, ok := plannerArgminOROrderCandidatesForTest(view, &viewQ, branches, &analysis)
			if !ok {
				t.Fatalf("plannerArgminOROrderCandidatesForTest: ok=false")
			}
			if candidates.fallbackCost <= 0 || candidates.streamCost <= 0 {
				t.Fatalf(
					"expected positive OR-order candidate costs: stream=%v fallback=%v",
					candidates.streamCost, candidates.fallbackCost,
				)
			}
			if candidates.mergeApplicable && candidates.mergeCost <= 0 {
				t.Fatalf("expected positive merge cost, got=%v", candidates.mergeCost)
			}

			wantPlan, wantCost := candidates.expectedPlan()
			if wantPlan != tc.wantPlan {
				t.Fatalf(
					"expected candidate enumeration to choose %v, got %v (merge_applicable=%v merge=%v stream=%v fallback=%v)",
					tc.wantPlan, wantPlan, candidates.mergeApplicable, candidates.mergeCost, candidates.streamCost, candidates.fallbackCost,
				)
			}

			decision := view.decidePlanOROrderWithAnalysis(&viewQ, branches, &analysis)
			if decision.plan != wantPlan {
				t.Fatalf("ordered OR winner mismatch: got=%v want=%v", decision.plan, wantPlan)
			}
			plannerArgminAssertApproxCost(t, "ordered OR best", decision.bestCost, wantCost)
			plannerArgminAssertApproxCost(t, "ordered OR fallback", decision.fallbackCost, candidates.fallbackCost)
		})
	}
}

func TestPlannerArgmin_OROrderDecisionMatchesCandidatePolicy_FallbackSynthetic(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:    -1,
		CalibrationEnabled: true,
	})
	_ = seedData(t, db, 2_000)
	setORPlannerStatsSnapshotForTest(db, 100_000, PlannerFieldStats{
		DistinctKeys:    100_000,
		NonEmptyKeys:    100_000,
		TotalBucketCard: 100_000,
		AvgBucketCard:   1,
		MaxBucketCard:   2,
		P50BucketCard:   1,
		P95BucketCard:   2,
	})

	err := db.SetCalibrationSnapshot(CalibrationSnapshot{
		UpdatedAt: time.Now(),
		Multipliers: map[string]float64{
			"plan_or_merge_order_merge":  4.0,
			"plan_or_merge_order_stream": 4.0,
		},
	})
	if err != nil {
		t.Fatalf("SetCalibrationSnapshot: %v", err)
	}

	q := qx.Query(
		qx.OR(
			qx.EQ("active", true),
			qx.EQ("name", "alice"),
			qx.EQ("country", "NL"),
		),
	).Sort("score", qx.DESC).Limit(100)

	preparedQ, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer preparedQ.Release()

	branches := newPlannerORBranches(3)
	branches.Append(makeORBranchForCalibrationDecisionTest(8_000, 64))
	branches.Append(makeORBranchForCalibrationDecisionTest(8_000, 64))
	branches.Append(makeORBranchForCalibrationDecisionTest(8_000, 64))
	defer branches.Release()

	var analysis plannerOROrderAnalysis
	analysis.orderField = "score"
	analysis.snapshotUniverse = 100_000
	analysis.branchCount = branches.Len()
	for i := 0; i < branches.Len(); i++ {
		checks := branches.Get(i).containsChecks()
		analysis.mergeStats[i].streamChecks = checks
		analysis.mergeStats[i].mergeChecks = checks
	}

	view := db.currentQueryViewForTests()
	candidates, ok := plannerArgminOROrderCandidatesForTest(view, &viewQ, branches, &analysis)
	if !ok {
		t.Fatalf("plannerArgminOROrderCandidatesForTest: ok=false")
	}
	wantPlan, wantCost := candidates.expectedPlan()
	if wantPlan != plannerOROrderFallback {
		t.Fatalf(
			"expected synthetic candidate enumeration to choose fallback, got %v (merge_applicable=%v merge=%v stream=%v fallback=%v)",
			wantPlan, candidates.mergeApplicable, candidates.mergeCost, candidates.streamCost, candidates.fallbackCost,
		)
	}

	decision := view.decidePlanOROrderWithAnalysis(&viewQ, branches, &analysis)
	if decision.plan != wantPlan {
		t.Fatalf("synthetic ordered OR winner mismatch: got=%v want=%v", decision.plan, wantPlan)
	}
	plannerArgminAssertApproxCost(t, "synthetic ordered OR best", decision.bestCost, wantCost)
	plannerArgminAssertApproxCost(t, "synthetic ordered OR fallback", decision.fallbackCost, candidates.fallbackCost)
}

func TestPlannerArgmin_OrderedDecisionMatchesCandidatePolicy(t *testing.T) {
	tests := []struct {
		name      string
		calibrate func(*testing.T, *DB[uint64, Rec])
		wantUse   bool
	}{
		{
			name: "CalibrationPrefersOrdered",
			calibrate: func(t *testing.T, db *DB[uint64, Rec]) {
				t.Helper()
				err := db.SetCalibrationSnapshot(CalibrationSnapshot{
					UpdatedAt: time.Now(),
					Multipliers: map[string]float64{
						"plan_ordered": 0.001,
					},
				})
				if err != nil {
					t.Fatalf("SetCalibrationSnapshot: %v", err)
				}
			},
			wantUse: true,
		},
		{
			name: "CalibrationForcesFallback",
			calibrate: func(t *testing.T, db *DB[uint64, Rec]) {
				t.Helper()
				err := db.SetCalibrationSnapshot(CalibrationSnapshot{
					UpdatedAt: time.Now(),
					Multipliers: map[string]float64{
						"plan_ordered": 4.0,
					},
				})
				if err != nil {
					t.Fatalf("SetCalibrationSnapshot: %v", err)
				}
			},
			wantUse: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := openTempDBUint64(t, Options{
				AnalyzeInterval:    -1,
				CalibrationEnabled: tc.calibrate != nil,
			})
			_ = seedData(t, db, 20_000)
			if tc.calibrate != nil {
				tc.calibrate(t, db)
			}

			q := plannerArgminOrderedQuery()
			preparedQ, viewQ, err := prepareTestQuery(db, q)
			if err != nil {
				t.Fatalf("prepareTestQuery: %v", err)
			}
			defer preparedQ.Release()

			var leavesBuf [plannerPredicateFastPathMaxLeaves]qir.Expr
			leaves, ok := collectAndLeavesScratch(viewQ.Expr, leavesBuf[:0])
			if !ok || len(leaves) == 0 {
				t.Fatalf("collectAndLeavesScratch: ok=%v len=%d", ok, len(leaves))
			}

			view := db.currentQueryViewForTests()
			decision := view.decideOrderedByCost(&viewQ, leaves)
			if decision.orderedCost <= 0 || decision.fallbackCost <= 0 {
				t.Fatalf(
					"expected positive ordered candidate costs: ordered=%v fallback=%v",
					decision.orderedCost, decision.fallbackCost,
				)
			}

			candidates, ok := plannerArgminOrderedCandidatesForDecision(&viewQ, leaves, decision)
			if !ok {
				t.Fatalf("plannerArgminOrderedCandidatesForDecision: ok=false")
			}
			if candidates.expectedUse() != tc.wantUse {
				t.Fatalf(
					"expected candidate policy wantUse=%v, got %v (ordered=%v fallback=%v gain=%v offset=%d)",
					tc.wantUse, candidates.expectedUse(), candidates.orderedCost, candidates.fallbackCost, candidates.gainReq, q.Window.Offset,
				)
			}
			if decision.use != candidates.expectedUse() {
				t.Fatalf(
					"ordered winner mismatch: use=%v ordered=%v fallback=%v gain=%v",
					decision.use, decision.orderedCost, decision.fallbackCost, candidates.gainReq,
				)
			}
		})
	}
}
