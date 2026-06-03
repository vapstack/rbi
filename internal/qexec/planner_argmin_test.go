package qexec

import (
	"math"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
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

func plannerArgminOROrderCandidatesForTest(
	qv *View,
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

	ov := qv.fieldIndexViewFromSlotsForOrder(qv.snap.Index, o)
	if !ov.HasData() {
		return plannerOROrderArgminCandidates{}, false
	}
	orderDistinct := uint64(ov.KeyCount())
	if orderDistinct == 0 {
		return plannerOROrderArgminCandidates{}, false
	}

	need, ok := orderWindow(q)
	if !ok || need <= 0 {
		return plannerOROrderArgminCandidates{}, false
	}

	snap := qv.exec.Stats.Load()
	universe := snap.UniverseOr(analysis.snapshotUniverse)
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
	cacheState, eagerBuildWork, cachePressurePenalty := qv.orderedORCacheCandidateState(branches, analysis, need, q.Offset)
	eagerCost := 0.0
	if q.Offset > 0 && eagerBuildWork > 0 {
		eagerCost = float64(eagerBuildWork)
	}

	candidates := plannerOROrderArgminCandidates{
		fallbackCost: float64(sumCard) + float64(expectedRows),
	}

	streamChecks := branches.orderStreamChecksByBranch(hasAlwaysTrue, analysis.mergeStats)
	candidates.streamCost = float64(expectedRows)*streamChecks*plannerFieldStatsSkew(orderStats) + eagerCost
	cacheLimit := qv.snap.MaterializedPredCacheLimit()
	cacheEntries := 0
	if cache := qv.snap.MaterializedPredCache(); cache != nil {
		cacheEntries = cache.EntryCount()
	}
	cacheColdTiny := cacheLimit <= 0 || (cacheLimit < branchCount && cacheEntries < branchCount)
	cacheCold := cacheEntries == 0
	hasFullSpanOrderBranch := branches.hasFullSpanOrderBranch(analysis.mergeStats)
	hasPrefixTailRisk := false
	hasBroadResidual := false
	highBranchLeads := 0
	for i := 0; i < branchCount; i++ {
		branch := branches.owner[i]
		for pi := 0; pi < branch.preds.Len(); pi++ {
			p := branch.preds.owner[pi]
			if qv.isPositiveNonOrderScalarPrefixLeaf(analysis.orderField, p.expr) && !p.isMaterializedLike() {
				hasPrefixTailRisk = true
			}
			if p.covered ||
				p.expr.Not ||
				qv.exec.FieldNameByOrdinal(p.expr.FieldOrdinal) == analysis.orderField ||
				(p.expr.Op != qir.OpPREFIX && !p.expr.Op.IsNumericRange()) ||
				p.estCard < universe/4 {
				continue
			}
			hasBroadResidual = true
			break
		}
		lead := branchCards[i]
		if branch.hasLead() {
			lead = branch.preds.owner[branch.leadIdx].estCard
		}
		if lead >= uint64(need)*64 && lead >= universe/16 {
			highBranchLeads++
		}
	}
	if cacheColdTiny && branchCount > 1 {
		candidates.fallbackCost *= 1.0 + float64(branchCount)*0.65
		if hasPrefixTailRisk || hasBroadResidual {
			candidates.fallbackCost *= 1.35
		}
	}

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
		hasFullSpanOrderBranch &&
		!routeCost.hasPrefixTailRisk &&
		!branches.hasKWayExactBucketApplyWork(analysis.mergeStats) {
		candidates.mergeCost = routeCost.kWay
	}
	if hasPrefixTailRisk {
		candidates.mergeCost *= plannerOROrderExactBucketApplyPenalty(&analysis.mergeStats, branchCount)
	}
	candidates.mergeCost *= plannerOROrderPrefixTailRiskPenalty(hasPrefixTailRisk, branchCount, q.Offset)
	if routeOK {
		if hasBroadResidual {
			candidates.mergeCost *= 1.25
		}
		if (cacheCold || cacheColdTiny) && hasFullSpanOrderBranch && expectedRows <= uint64(need*8) && highBranchLeads > 0 {
			candidates.mergeCost *= 1.0 + float64(highBranchLeads)*0.12
		}
		if cacheColdTiny && (hasPrefixTailRisk || hasBroadResidual || branches.hasKWayExactBucketApplyWork(analysis.mergeStats)) {
			candidates.mergeCost *= 1.35
			if candidates.streamCost > 0 && candidates.mergeCost >= candidates.streamCost*0.70 && candidates.mergeCost <= candidates.streamCost*1.15 {
				candidates.mergeCost *= 1.35
			}
		}
		if plannerMaterializedCacheStateUnretained(cacheState) {
			candidates.mergeCost *= cachePressurePenalty
		}
	}
	candidates.mergeCost += eagerCost
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
	db := newTestDB(t, testOptions{})
	_ = db.seedData(t, 2_000)
	db.setPlannerStatsSnapshot(100_000, PlannerFieldStats{
		DistinctKeys:    100_000,
		NonEmptyKeys:    100_000,
		TotalBucketCard: 100_000,
		AvgBucketCard:   1,
		MaxBucketCard:   2,
		P50BucketCard:   1,
		P95BucketCard:   2,
	})

	q := qx.Query(
		qx.OR(
			qx.EQ("active", true),
			qx.EQ("name", "alice"),
		),
	).Limit(100)

	branches := newPlannerORBranches(3)
	branches.Append(makeORBranchForPlannerDecisionTest(4_000, 4))
	branches.Append(makeORBranchForPlannerDecisionTest(4_000, 4))
	branches.Append(makeORBranchForPlannerDecisionTest(4_000, 4))
	defer branches.Release()

	decision := db.selectPlanORNoOrder(q, branches)
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
}

func TestPlannerArgmin_OROrderDecisionMatchesCandidatePolicy(t *testing.T) {
	tests := []struct {
		name string
		q    func() *qx.QX
	}{
		{
			name: "MergeQuery",
			q:    plannerArgminOROrderMergeQuery,
		},
		{
			name: "StreamQuery",
			q:    plannerArgminOROrderStreamQuery,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := newTestDB(t, testOptions{})
			_ = db.seedData(t, 20_000)

			q := tc.q()
			window, ok := testOrderWindow(q)
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

			preparedQ, viewQ, err := db.prepareQuery(q)
			if err != nil {
				t.Fatalf("prepareTestQuery: %v", err)
			}
			defer preparedQ.Release()

			view := db.view()
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
			decision := view.selectPlanOROrderWithAnalysis(&viewQ, branches, &analysis)
			if decision.plan != wantPlan {
				t.Fatalf("ordered OR winner mismatch: got=%v want=%v", decision.plan, wantPlan)
			}
			plannerArgminAssertApproxCost(t, "ordered OR best", decision.bestCost, wantCost)
			plannerArgminAssertApproxCost(t, "ordered OR fallback", decision.fallbackCost, candidates.fallbackCost)
		})
	}
}

func TestPlannerArgmin_OROrderDecisionMatchesCandidatePolicy_FallbackSynthetic(t *testing.T) {
	db := newTestDB(t, testOptions{})
	_ = db.seedData(t, 2_000)
	db.setPlannerStatsSnapshot(100_000, PlannerFieldStats{
		DistinctKeys:    100_000,
		NonEmptyKeys:    100_000,
		TotalBucketCard: 100_000,
		AvgBucketCard:   1,
		MaxBucketCard:   2,
		P50BucketCard:   1,
		P95BucketCard:   2,
	})

	q := qx.Query(
		qx.OR(
			qx.EQ("active", true),
			qx.EQ("name", "alice"),
			qx.EQ("country", "NL"),
		),
	).Sort("score", qx.DESC).Limit(20_000)

	preparedQ, viewQ, err := db.prepareQuery(q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer preparedQ.Release()

	branches := newPlannerORBranches(3)
	branches.Append(makeORBranchForPlannerDecisionTest(8_000, 64))
	branches.Append(makeORBranchForPlannerDecisionTest(8_000, 64))
	branches.Append(makeORBranchForPlannerDecisionTest(8_000, 64))
	defer branches.Release()

	var analysis plannerOROrderAnalysis
	analysis.orderField = "score"
	analysis.snapshotUniverse = 100_000
	analysis.branchCount = branches.Len()
	for i := 0; i < branches.Len(); i++ {
		checks := branches.owner[i].containsChecks()
		analysis.mergeStats[i].streamChecks = checks
		analysis.mergeStats[i].mergeChecks = checks
	}

	view := db.view()
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

	decision := view.selectPlanOROrderWithAnalysis(&viewQ, branches, &analysis)
	if decision.plan != wantPlan {
		t.Fatalf("synthetic ordered OR winner mismatch: got=%v want=%v", decision.plan, wantPlan)
	}
	plannerArgminAssertApproxCost(t, "synthetic ordered OR best", decision.bestCost, wantCost)
	plannerArgminAssertApproxCost(t, "synthetic ordered OR fallback", decision.fallbackCost, candidates.fallbackCost)
}

func TestPlannerArgmin_OROrderSmallSideBranchesKeepsBroadCoveredStream(t *testing.T) {
	tests := []struct {
		name         string
		card         uint64
		rangeRows    uint64
		rangeBounded bool
	}{
		{name: "BoundedRange", card: 60_000, rangeRows: 60_000, rangeBounded: true},
		{name: "FullSpan", card: 100_000},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := newTestDB(t, testOptions{})
			_ = db.seedData(t, 2_000)
			db.setPlannerStatsSnapshot(100_000, PlannerFieldStats{
				DistinctKeys:    100_000,
				NonEmptyKeys:    100_000,
				TotalBucketCard: 100_000,
				AvgBucketCard:   1,
				MaxBucketCard:   2,
				P50BucketCard:   1,
				P95BucketCard:   2,
			})

			q := qx.Query(
				qx.OR(
					qx.GTE("score", 10.0),
					qx.EQ("country", "NL"),
					qx.EQ("name", "alice"),
				),
			).Sort("score", qx.DESC).Limit(10)

			preparedQ, viewQ, err := db.prepareQuery(q)
			if err != nil {
				t.Fatalf("prepareTestQuery: %v", err)
			}
			defer preparedQ.Release()

			branches := newPlannerORBranches(3)
			branches.Append(makeORBranchForPlannerDecisionTest(tc.card, 0))
			branches.Append(makeORBranchForPlannerDecisionTest(100, 1))
			branches.Append(makeORBranchForPlannerDecisionTest(100, 1))
			defer branches.Release()

			var analysis plannerOROrderAnalysis
			analysis.orderField = "score"
			analysis.snapshotUniverse = 100_000
			analysis.branchCount = branches.Len()
			analysis.mergeStats[0] = plannerOROrderMergeBranchStats{
				streamChecks: 0,
				mergeChecks:  0,
				rangeRows:    tc.rangeRows,
				rangeBounded: tc.rangeBounded,
			}
			for i := 1; i < branches.Len(); i++ {
				checks := branches.owner[i].containsChecks()
				analysis.mergeStats[i].streamChecks = checks
				analysis.mergeStats[i].mergeChecks = checks
			}

			view := db.view()
			candidates, ok := plannerArgminOROrderCandidatesForTest(view, &viewQ, branches, &analysis)
			if !ok {
				t.Fatalf("plannerArgminOROrderCandidatesForTest: ok=false")
			}
			wantPlan, wantCost := candidates.expectedPlan()
			if wantPlan == plannerOROrderFallback {
				t.Fatalf(
					"expected candidate policy to avoid fallback, got %v (stream=%v fallback=%v merge=%v)",
					wantPlan, candidates.streamCost, candidates.fallbackCost, candidates.mergeCost,
				)
			}

			decision := view.selectPlanOROrderWithAnalysis(&viewQ, branches, &analysis)
			if decision.selected.kind == plannerOROrderCandidateMaterializedFallback {
				t.Fatalf(
					"small-side heuristic forced fallback despite dominant covered order branch: stream=%v fallback=%v",
					candidates.streamCost, candidates.fallbackCost,
				)
			}
			if decision.plan != wantPlan {
				t.Fatalf("ordered OR winner mismatch: got=%v want=%v", decision.plan, wantPlan)
			}
			plannerArgminAssertApproxCost(t, "covered-stream ordered OR best", decision.bestCost, wantCost)
		})
	}
}

func TestPlannerArgmin_OrderedDecisionMatchesCandidatePolicy(t *testing.T) {
	db := newTestDB(t, testOptions{})
	_ = db.seedData(t, 20_000)

	q := plannerArgminOrderedQuery()
	preparedQ, viewQ, err := db.prepareQuery(q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer preparedQ.Release()

	var leavesBuf [plannerPredicateFastPathMaxLeaves]qir.Expr
	leaves, ok := qir.CollectAndLeavesInto(viewQ.Expr, leavesBuf[:0], qir.LeafModeCollect)
	if !ok || len(leaves) == 0 {
		t.Fatalf("collectAndLeavesScratch: ok=%v len=%d", ok, len(leaves))
	}

	view := db.view()
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
	if decision.use != candidates.expectedUse() {
		t.Fatalf(
			"ordered winner mismatch: use=%v ordered=%v fallback=%v gain=%v",
			decision.use, decision.orderedCost, decision.fallbackCost, candidates.gainReq,
		)
	}
}

func TestPlannerArgmin_OrderedLimitCandidatePolicy(t *testing.T) {
	candidates := [...]plannerOrderedLimitCandidate{
		{kind: plannerOrderedLimitCandidateOrderScan, cost: 90},
		{kind: plannerOrderedLimitCandidateColdRetainedBaseCore, cost: 80},
		{kind: plannerOrderedLimitCandidateMaterializedFallback, cost: 120},
	}

	decision := plannerOrderedLimitPick(candidates[:])
	if decision.selected.kind != plannerOrderedLimitCandidateColdRetainedBaseCore {
		t.Fatalf("selected=%v want=%v", decision.selected.kind, plannerOrderedLimitCandidateColdRetainedBaseCore)
	}
	if decision.rejected.kind != plannerOrderedLimitCandidateOrderScan {
		t.Fatalf("nearest rejected=%v want=%v", decision.rejected.kind, plannerOrderedLimitCandidateOrderScan)
	}
	if decision.runtimeFallback.kind != plannerOrderedLimitCandidateOrderScan {
		t.Fatalf("runtime fallback=%v want=%v", decision.runtimeFallback.kind, plannerOrderedLimitCandidateOrderScan)
	}
	if decision.materializedFallback.kind != plannerOrderedLimitCandidateMaterializedFallback {
		t.Fatalf("materialized fallback=%v want=%v", decision.materializedFallback.kind, plannerOrderedLimitCandidateMaterializedFallback)
	}
}

func TestPlannerArgmin_OrderedLimitBasicRequiresOrderedDecisionUse(t *testing.T) {
	orderScan := plannerOrderedLimitCandidate{
		kind:         plannerOrderedLimitCandidateOrderScan,
		cost:         90,
		expectedRows: 100,
	}
	decision := plannerOrderedDecision{
		orderedCost:       10,
		expectedProbeRows: 10,
	}
	if _, ok := orderedLimitBasicCandidate(decision, nil, orderScan); ok {
		t.Fatalf("ordered_basic candidate ignored decision.use=false")
	}

	decision.use = true
	candidate, ok := orderedLimitBasicCandidate(decision, nil, orderScan)
	if !ok {
		t.Fatalf("expected ordered_basic candidate when decision.use=true")
	}
	if candidate.kind != plannerOrderedLimitCandidateOrderedBasic {
		t.Fatalf("candidate=%v want %v", candidate.kind, plannerOrderedLimitCandidateOrderedBasic)
	}
}

func TestPlannerArgmin_OrderedLimitRejectedOrderedBasicDoesNotForceBasic(t *testing.T) {
	db := newTestDB(t, testOptions{MatPredCacheMaxEntries: 16})
	db.seedData(t, 20_000)

	q := qx.Query(
		qx.GTE("score", 30.0),
		qx.EQ("active", true),
		qx.HASANY("tags", []string{"go", "db", "ops"}),
		qx.GTE("age", 22),
	).Sort("score", qx.DESC).Limit(80)

	prepared, shape, err := db.prepareQuery(q)
	if err != nil {
		t.Fatalf("prepareQuery: %v", err)
	}
	defer prepared.Release()

	view := db.view()
	facts := orderedLimitFactsPool.Get()
	defer facts.Release()

	ok, err := view.collectOrderedLimitFacts(&shape, facts)
	if err != nil {
		t.Fatalf("collectOrderedLimitFacts: %v", err)
	}
	if !ok {
		t.Fatalf("expected ordered LIMIT facts")
	}

	orderedDecision := view.decideOrderedByCost(&shape, facts.ops)
	if orderedDecision.use {
		t.Fatalf("fixture no longer tests rejected ordered_basic decision: ordered=%v fallback=%v", orderedDecision.orderedCost, orderedDecision.fallbackCost)
	}

	decision, ok, err := view.selectOrderedLimit(&shape, facts)
	if err != nil {
		t.Fatalf("selectOrderedLimit: %v", err)
	}
	if !ok {
		t.Fatalf("expected ordered LIMIT decision")
	}
	if decision.selected.kind == plannerOrderedLimitCandidateOrderedBasic {
		t.Fatalf("selected ordered_basic despite decision.use=false: %+v", decision.traceRoute())
	}
}

func TestPlannerArgmin_OrderedLimitActiveRegionLikeUsesBoundedRoute(t *testing.T) {
	db := newTestDB(t, testOptions{MatPredCacheMaxEntries: 16})
	db.seedData(t, 20_000)

	q := qx.Query(
		qx.EQ("active", true),
		qx.GTE("score", 60.0),
		qx.IN("country", []string{"NL", "DE"}),
		qx.NOTIN("name", []string{"alice"}),
	).Sort("score", qx.DESC).Limit(40)

	prepared, shape, err := db.prepareQuery(q)
	if err != nil {
		t.Fatalf("prepareQuery: %v", err)
	}
	defer prepared.Release()

	view := db.view()
	facts := orderedLimitFactsPool.Get()
	defer facts.Release()

	ok, err := view.collectOrderedLimitFacts(&shape, facts)
	if err != nil {
		t.Fatalf("collectOrderedLimitFacts: %v", err)
	}
	if !ok {
		t.Fatalf("expected ordered LIMIT facts")
	}

	decision, ok, err := view.selectOrderedLimit(&shape, facts)
	if err != nil {
		t.Fatalf("selectOrderedLimit: %v", err)
	}
	if !ok {
		t.Fatalf("expected ordered LIMIT decision")
	}
	if decision.selected.kind != plannerOrderedLimitCandidateOrderScan &&
		decision.selected.kind != plannerOrderedLimitCandidateOrderedBasic {
		t.Fatalf("selected=%v want bounded ordered route: %+v", decision.selected.kind, decision.traceRoute())
	}
}

func TestPlannerArgmin_OrderedLimitBoundedWindowSelectsOrderedBasic(t *testing.T) {
	db := newTestDB(t, testOptions{MatPredCacheMaxEntries: 16})
	countries := [...]string{"US", "CA", "GB", "DE", "FR", "NL", "PL", "SE"}
	db.seedGeneratedData(t, 100_000, func(id uint64) testRec {
		name := "pro"
		if id%5 == 0 {
			name = "free"
		}
		score := float64(id % (365*24 - 72))
		if id%100 < 60 {
			score = float64(365*24 - 1 - id%72)
		}
		return testRec{
			Meta:   Meta{Country: countries[id&7]},
			Name:   name,
			Score:  score,
			Active: id%20 != 0,
		}
	})

	q := qx.Query(
		qx.EQ("active", true),
		qx.GTE("score", float64(365*24-72)),
		qx.IN("country", []string{"CA", "GB", "DE", "FR", "NL", "PL"}),
		qx.NOTIN("name", []string{"free"}),
	).Sort("score", qx.DESC).Limit(40)

	prepared, shape, err := db.prepareQuery(q)
	if err != nil {
		t.Fatalf("prepareQuery: %v", err)
	}
	defer prepared.Release()

	view := db.view()
	facts := orderedLimitFactsPool.Get()
	defer facts.Release()

	ok, err := view.collectOrderedLimitFacts(&shape, facts)
	if err != nil {
		t.Fatalf("collectOrderedLimitFacts: %v", err)
	}
	if !ok {
		t.Fatalf("expected ordered LIMIT facts")
	}

	orderedDecision := view.decideOrderedByCost(&shape, facts.ops)
	if !orderedDecision.use {
		t.Fatalf("bounded order-window rejected ordered_basic: ordered=%v fallback=%v", orderedDecision.orderedCost, orderedDecision.fallbackCost)
	}

	decision, ok, err := view.selectOrderedLimit(&shape, facts)
	if err != nil {
		t.Fatalf("selectOrderedLimit: %v", err)
	}
	if !ok {
		t.Fatalf("expected ordered LIMIT decision")
	}
	if decision.selected.kind != plannerOrderedLimitCandidateOrderedBasic {
		t.Fatalf("selected=%v want %v route=%+v", decision.selected.kind, plannerOrderedLimitCandidateOrderedBasic, decision.traceRoute())
	}
}

func TestPlannerArgmin_NoOrderLimitCandidatePolicy(t *testing.T) {
	candidates := [...]plannerNoOrderLimitCandidate{
		{kind: plannerNoOrderLimitCandidateDirectRange, cost: 90},
		{kind: plannerNoOrderLimitCandidateLeadScan, cost: 75},
		{kind: plannerNoOrderLimitCandidateMaterializedFallback, cost: 140},
	}

	decision := plannerNoOrderLimitPick(candidates[:])
	if decision.selected.kind != plannerNoOrderLimitCandidateLeadScan {
		t.Fatalf("selected=%v want=%v", decision.selected.kind, plannerNoOrderLimitCandidateLeadScan)
	}
	if decision.rejected.kind != plannerNoOrderLimitCandidateDirectRange {
		t.Fatalf("nearest rejected=%v want=%v", decision.rejected.kind, plannerNoOrderLimitCandidateDirectRange)
	}
	if decision.materializedFallback.kind != plannerNoOrderLimitCandidateMaterializedFallback {
		t.Fatalf("materialized fallback=%v want=%v", decision.materializedFallback.kind, plannerNoOrderLimitCandidateMaterializedFallback)
	}

	candidates[0].cost = 70
	decision = plannerNoOrderLimitPick(candidates[:])
	if decision.selected.kind != plannerNoOrderLimitCandidateDirectRange {
		t.Fatalf("selected=%v want=%v", decision.selected.kind, plannerNoOrderLimitCandidateDirectRange)
	}
	if decision.runtimeFallback.kind != plannerNoOrderLimitCandidateLeadScan {
		t.Fatalf("runtime fallback=%v want=%v", decision.runtimeFallback.kind, plannerNoOrderLimitCandidateLeadScan)
	}
}

func TestPlannerArgmin_ORNoOrderCandidatePolicy(t *testing.T) {
	candidates := [...]plannerORNoOrderCandidate{
		{kind: plannerORNoOrderCandidateAdaptiveMerge, cost: 90},
		{kind: plannerORNoOrderCandidateBaselineMerge, cost: 95},
		{kind: plannerORNoOrderCandidateMaterializedFallback, cost: 120},
	}

	decision := plannerORNoOrderPick(candidates[:], false)
	if decision.selected.kind != plannerORNoOrderCandidateAdaptiveMerge {
		t.Fatalf("selected=%v want=%v", decision.selected.kind, plannerORNoOrderCandidateAdaptiveMerge)
	}
	if decision.rejected.kind != plannerORNoOrderCandidateBaselineMerge {
		t.Fatalf("nearest rejected=%v want=%v", decision.rejected.kind, plannerORNoOrderCandidateBaselineMerge)
	}
	if decision.materializedFallback.kind != plannerORNoOrderCandidateMaterializedFallback {
		t.Fatalf("materialized fallback=%v want=%v", decision.materializedFallback.kind, plannerORNoOrderCandidateMaterializedFallback)
	}

	decision = plannerORNoOrderPick(candidates[:], true)
	if decision.selected.kind != plannerORNoOrderCandidateMaterializedFallback {
		t.Fatalf("forced selected=%v want=%v", decision.selected.kind, plannerORNoOrderCandidateMaterializedFallback)
	}
}

func TestPlannerArgmin_OROrderCandidatePolicy(t *testing.T) {
	candidates := [...]plannerOROrderCandidate{
		{kind: plannerOROrderCandidateMaterializedFallback, cost: 100},
		{kind: plannerOROrderCandidateKWayMerge, cost: 95},
		{kind: plannerOROrderCandidateBranchCollect, cost: 70},
		{kind: plannerOROrderCandidateStream, cost: 80},
	}

	decision := plannerOROrderPick(candidates[:], false)
	if decision.selected.kind != plannerOROrderCandidateBranchCollect {
		t.Fatalf("selected=%v want=%v", decision.selected.kind, plannerOROrderCandidateBranchCollect)
	}
	if decision.rejected.kind != plannerOROrderCandidateStream {
		t.Fatalf("nearest rejected=%v want=%v", decision.rejected.kind, plannerOROrderCandidateStream)
	}
	if decision.materializedFallback.kind != plannerOROrderCandidateMaterializedFallback {
		t.Fatalf("materialized fallback=%v want=%v", decision.materializedFallback.kind, plannerOROrderCandidateMaterializedFallback)
	}

	decision = plannerOROrderPick(candidates[:], true)
	if decision.selected.kind != plannerOROrderCandidateMaterializedFallback {
		t.Fatalf("forced selected=%v want=%v", decision.selected.kind, plannerOROrderCandidateMaterializedFallback)
	}
}

func TestPlannerArgmin_OrderedLimitRuntimeGuard(t *testing.T) {
	q := &qir.Shape{Limit: 10}
	decision := plannerOrderedLimitDecision{
		selected: plannerOrderedLimitCandidate{
			kind:         plannerOrderedLimitCandidateOrderScan,
			expectedRows: 10,
			checks:       1,
		},
		runtimeFallback: plannerOrderedLimitCandidate{
			kind: plannerOrderedLimitCandidateMaterializedFallback,
			cost: 100,
		},
	}

	guard := decision.runtimeGuard(q)
	if !guard.enabled {
		t.Fatalf("expected runtime guard")
	}
	if guard.shouldFallback(guard.minExamined-1, 0) {
		t.Fatalf("guard fired before minExamined")
	}
	if !guard.shouldFallback(guard.minExamined, 0) {
		t.Fatalf("guard did not fire after underestimated scan work")
	}
	if guard.shouldFallback(guard.minExamined, 3) {
		t.Fatalf("guard fired after enough early output")
	}
}

func TestPlannerArgmin_OrderedLimitBaseCoreRuntimeGuard(t *testing.T) {
	q := &qir.Shape{Limit: 10}
	decision := plannerOrderedLimitDecision{
		selected: plannerOrderedLimitCandidate{
			kind:         plannerOrderedLimitCandidateWarmBaseCore,
			expectedRows: 10,
			checks:       1,
		},
		materializedFallback: plannerOrderedLimitCandidate{
			kind: plannerOrderedLimitCandidateMaterializedFallback,
			cost: 100,
		},
		runtimeFallback: plannerOrderedLimitCandidate{
			kind: plannerOrderedLimitCandidateOrderScan,
			cost: 1,
		},
	}

	guard := decision.baseCoreRuntimeGuard(q, 1)
	if !guard.enabled {
		t.Fatalf("expected base-core runtime guard")
	}
	if guard.reason != "base_core_scan_guard" {
		t.Fatalf("reason=%q want base_core_scan_guard", guard.reason)
	}
	if guard.shouldFallback(guard.minExamined-1, 0) {
		t.Fatalf("guard fired before minExamined")
	}
	if !guard.shouldFallback(guard.minExamined, 0) {
		t.Fatalf("guard did not fire after underestimated base-core scan work")
	}
	if guard.shouldFallback(guard.minExamined, 3) {
		t.Fatalf("guard fired after enough early output")
	}

	if guard = decision.baseCoreRuntimeGuard(q, 0); guard.enabled {
		t.Fatalf("base-core guard enabled without base predicates")
	}
	q.Offset = 1
	if guard = decision.baseCoreRuntimeGuard(q, 1); guard.enabled {
		t.Fatalf("base-core guard enabled for offset")
	}
	q.Offset = 0
	decision.materializedFallback = plannerOrderedLimitCandidate{}
	if guard = decision.baseCoreRuntimeGuard(q, 1); guard.enabled {
		t.Fatalf("base-core guard enabled without materialized fallback")
	}
}

func TestPlannerArgmin_NoOrderLimitRuntimeGuard(t *testing.T) {
	q := &qir.Shape{Limit: 10}
	decision := plannerNoOrderLimitDecision{
		selected: plannerNoOrderLimitCandidate{
			kind:         plannerNoOrderLimitCandidateDirectRange,
			cost:         90,
			expectedRows: 10,
			checks:       3,
		},
		runtimeFallback: plannerNoOrderLimitCandidate{
			kind:         plannerNoOrderLimitCandidateLeadScan,
			cost:         100,
			expectedRows: 12,
		},
	}

	guard := decision.runtimeGuard(q)
	if !guard.enabled {
		t.Fatalf("expected runtime guard")
	}
	if guard.shouldFallback(guard.minExamined-1, 0) {
		t.Fatalf("guard fired before minExamined")
	}
	if !guard.shouldFallback(guard.minExamined, 0) {
		t.Fatalf("guard did not fire after underestimated direct range work")
	}
	if guard.shouldFallback(guard.minExamined, 3) {
		t.Fatalf("guard fired after enough early output")
	}

	decision.runtimeFallback.cost = 500
	if guard = decision.runtimeGuard(q); guard.enabled {
		t.Fatalf("runtime guard enabled for non-competitive fallback")
	}

	decision.selected.expectedRows = 100
	decision.runtimeFallback.expectedRows = 70
	if guard = decision.runtimeGuard(q); !guard.enabled {
		t.Fatalf("expected runtime guard for selective fallback")
	}
}

func TestPlannerNoOrderLimitRuntimeGuard_DirectRangeFallback(t *testing.T) {
	db := newTestDB(t, testOptions{
		TraceSink:        func(TraceEvent) {},
		TraceSampleEvery: 1,
	})
	db.seedGeneratedData(t, 3_000, func(id uint64) testRec {
		return testRec{
			Age:    1,
			Active: id > 2_000,
		}
	})

	q := qx.Query(
		qx.GTE("age", 1),
		qx.EQ("active", true),
	).Limit(10)

	prepared, shape, err := db.prepareQuery(q)
	if err != nil {
		t.Fatalf("prepareQuery: %v", err)
	}
	defer prepared.Release()

	var leavesBuf [4]qir.Expr
	leaves, ok := qir.CollectAndLeavesInto(shape.Expr, leavesBuf[:0], qir.LeafModeCollect)
	if !ok {
		t.Fatalf("CollectAndLeavesInto: ok=false")
	}

	view := db.view()
	bounds, ok, err := view.extractBoundsForField("age", leaves)
	if err != nil {
		t.Fatalf("extractBoundsForField: %v", err)
	}
	if !ok {
		t.Fatalf("expected age bounds")
	}

	trace := db.exec.BeginTrace(shape, "")
	guard := plannerNoOrderLimitRuntimeGuard{
		enabled:      true,
		minExamined:  128,
		needWindow:   shape.Limit,
		fallbackCost: 1,
		rowCost:      1,
		reason:       "direct_range_guard",
	}
	_, used, runtimeFallback, err := view.execNoOrderBounds(&shape, "age", view.fieldOrdinalByName("age"), bounds, leaves, guard, trace)
	if err != nil {
		t.Fatalf("execNoOrderBounds: %v", err)
	}
	if used || !runtimeFallback {
		t.Fatalf("expected runtime guard to request fallback, used=%v runtimeFallback=%v", used, runtimeFallback)
	}
	ev := trace.Event()
	if !ev.NoOrderLimitRoute.RuntimeFallbackTriggered {
		t.Fatalf("expected runtime fallback, route=%+v", ev.NoOrderLimitRoute)
	}
	if trace.RowsExamined() < guard.minExamined {
		t.Fatalf("runtime guard fired before minExamined: rowsExamined=%d min=%d", trace.RowsExamined(), guard.minExamined)
	}
	if trace.RowsExamined() > guard.minExamined {
		t.Fatalf("runtime guard scanned beyond bounded window: rowsExamined=%d min=%d", trace.RowsExamined(), guard.minExamined)
	}
}

func TestPlannerOROrderStreamSampleDecision(t *testing.T) {
	selected := plannerOROrderCandidate{
		kind: plannerOROrderCandidateKWayMerge,
		cost: 2_000,
	}
	stream := plannerOROrderCandidate{
		kind:      plannerOROrderCandidateStream,
		avgChecks: 1,
	}
	fallback, reason := plannerOROrderStreamSamplePrefersStream(
		128,
		selected,
		stream,
		plannerOROrderStreamSample{examined: 400, matched: 16, buckets: 8},
	)
	if !fallback || reason == "" {
		t.Fatalf("expected early stream sample to switch, fallback=%v reason=%q", fallback, reason)
	}

	selected.hasBroadResidual = true
	fallback, reason = plannerOROrderStreamSamplePrefersStream(
		128,
		selected,
		stream,
		plannerOROrderStreamSample{examined: 80, matched: 16, buckets: 1},
	)
	if fallback || reason != "sample_narrow" {
		t.Fatalf("expected single-bucket broad sample to keep k-way, fallback=%v reason=%q", fallback, reason)
	}
	selected.hasBroadResidual = false

	fallback, reason = plannerOROrderStreamSamplePrefersStream(
		128,
		selected,
		stream,
		plannerOROrderStreamSample{
			examined: plannerOROrderStreamSampleMaxRows,
			matched:  1,
			buckets:  plannerOROrderStreamSampleMaxBuckets,
		},
	)
	if fallback || reason != "sample_sparse" {
		t.Fatalf("expected sparse sample to keep k-way, fallback=%v reason=%q", fallback, reason)
	}
}

func TestPlannerMaterializedCacheClassifierStates(t *testing.T) {
	key := qcache.MaterializedPredKeyForScalar("name", qir.OpPREFIX, "a")

	db := newTestDB(t, testOptions{
		MatPredCacheMaxEntries: 4,
		MatPredCacheMaxCard:    8,
	})
	_ = db.seedData(t, 64)
	view := db.view()

	if got := view.classifyPlannerMaterializedCacheKey(key, 4, false); got != plannerMaterializedCacheColdRegularAdmissible {
		t.Fatalf("regular state=%v want=%v", got, plannerMaterializedCacheColdRegularAdmissible)
	}
	if got := view.classifyPlannerMaterializedCacheKey(key, 16, false); got != plannerMaterializedCacheColdOversizedAdmissible {
		t.Fatalf("oversized state=%v want=%v", got, plannerMaterializedCacheColdOversizedAdmissible)
	}

	secondHitKey := qcache.MaterializedPredKeyForScalar("email", qir.OpPREFIX, "u")
	if got := view.classifyPlannerMaterializedCacheKey(secondHitKey, 4, true); got != plannerMaterializedCacheColdSecondHitRequired {
		t.Fatalf("second-hit state=%v want=%v", got, plannerMaterializedCacheColdSecondHitRequired)
	}
	view.snap.ShouldPromoteRuntimeMaterializedPredKey(secondHitKey)
	if got := view.classifyPlannerMaterializedCacheKey(secondHitKey, 4, true); got != plannerMaterializedCacheColdRegularAdmissible {
		t.Fatalf("seen second-hit state=%v want=%v", got, plannerMaterializedCacheColdRegularAdmissible)
	}

	view.snap.StoreMaterializedPredKey(key, posting.List{})
	if got := view.classifyPlannerMaterializedCacheKey(key, 4, false); got != plannerMaterializedCacheWarmHit {
		t.Fatalf("warm state=%v want=%v", got, plannerMaterializedCacheWarmHit)
	}

	disabled := newTestDB(t, testOptions{MatPredCacheMaxEntries: -1})
	_ = disabled.seedData(t, 16)
	if got := disabled.view().classifyPlannerMaterializedCacheKey(key, 4, false); got != plannerMaterializedCacheDisabled {
		t.Fatalf("disabled state=%v want=%v", got, plannerMaterializedCacheDisabled)
	}
}
