package qexec

import (
	"math"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/qir"
)

// Cost model margins. Values below 1 require measurable advantage
// before choosing a specialized OR plan over baseline fallback.
const (
	plannerORNoOrderAdvantage = 0.96
	plannerOROrderMergeGain   = 0.93
	plannerOROrderStreamGain  = 0.97

	plannerORBranchFallbackDiv = 32
)

func (qv *View) plannerUniverseCardinality(snap *PlannerStatsSnapshot) uint64 {
	if snap != nil && snap.UniverseCardinality > 0 {
		return snap.UniverseCardinality
	}
	return qv.snap.Universe.Cardinality()
}

type plannerOROrderPlan int

const (
	plannerOROrderFallback plannerOROrderPlan = iota
	plannerOROrderMerge
	plannerOROrderStream
)

type plannerORNoOrderCandidateKind uint8

const (
	plannerORNoOrderCandidateNone plannerORNoOrderCandidateKind = iota
	plannerORNoOrderCandidateUniverse
	plannerORNoOrderCandidateAdaptiveMerge
	plannerORNoOrderCandidateBaselineMerge
	plannerORNoOrderCandidateMaterializedFallback
)

func (k plannerORNoOrderCandidateKind) String() string {
	switch k {
	case plannerORNoOrderCandidateUniverse:
		return "universe"
	case plannerORNoOrderCandidateAdaptiveMerge:
		return "adaptive_merge"
	case plannerORNoOrderCandidateBaselineMerge:
		return "baseline_merge"
	case plannerORNoOrderCandidateMaterializedFallback:
		return "materialized_fallback"
	default:
		return ""
	}
}

type plannerORNoOrderCandidate struct {
	kind         plannerORNoOrderCandidateKind
	cost         float64
	expectedRows uint64
	unionRows    uint64
	sumRows      uint64
	avgChecks    float64
}

type plannerORNoOrderDecision struct {
	selected             plannerORNoOrderCandidate
	materializedFallback plannerORNoOrderCandidate
	rejected             plannerORNoOrderCandidate
	use                  bool
	expectedRows         uint64
	kWayCost             float64
	fallbackCost         float64
}

func (d plannerORNoOrderDecision) selectedKind() plannerORNoOrderCandidateKind {
	return d.selected.kind
}

func (d plannerORNoOrderDecision) traceRoute() TraceORRoute {
	return TraceORRoute{
		Selected:     d.selected.kind.String(),
		Rejected:     d.rejected.kind.String(),
		SelectedCost: d.selected.cost,
		RejectedCost: d.rejected.cost,
		ExpectedRows: d.selected.expectedRows,
		UnionRows:    d.selected.unionRows,
		SumRows:      d.selected.sumRows,
		AvgChecks:    d.selected.avgChecks,
	}
}

type plannerOROrderCandidateKind uint8

const (
	plannerOROrderCandidateNone plannerOROrderCandidateKind = iota
	plannerOROrderCandidateStream
	plannerOROrderCandidateKWayMerge
	plannerOROrderCandidateBranchCollect
	plannerOROrderCandidateMaterializedFallback
)

func (k plannerOROrderCandidateKind) String() string {
	switch k {
	case plannerOROrderCandidateStream:
		return "stream"
	case plannerOROrderCandidateKWayMerge:
		return "kway_merge"
	case plannerOROrderCandidateBranchCollect:
		return "branch_collect"
	case plannerOROrderCandidateMaterializedFallback:
		return "materialized_fallback"
	default:
		return ""
	}
}

type plannerOROrderCandidate struct {
	kind                plannerOROrderCandidateKind
	cost                float64
	expectedRows        uint64
	unionRows           uint64
	sumRows             uint64
	avgChecks           float64
	overlap             float64
	cacheState          plannerMaterializedCacheState
	eagerBuildWork      uint64
	hasPrefixTailRisk   bool
	hasSelectiveLead    bool
	fallbackCollectFast bool
}

func (k plannerOROrderCandidateKind) plan() plannerOROrderPlan {
	switch k {
	case plannerOROrderCandidateStream:
		return plannerOROrderStream
	case plannerOROrderCandidateKWayMerge, plannerOROrderCandidateBranchCollect:
		return plannerOROrderMerge
	default:
		return plannerOROrderFallback
	}
}

type plannerOROrderDecision struct {
	selected             plannerOROrderCandidate
	materializedFallback plannerOROrderCandidate
	rejected             plannerOROrderCandidate
	plan                 plannerOROrderPlan
	expectedRows         uint64
	bestCost             float64
	fallbackCost         float64
}

func (d plannerOROrderDecision) selectedKind() plannerOROrderCandidateKind {
	return d.selected.kind
}

func (d plannerOROrderDecision) traceRoute() TraceORRoute {
	return TraceORRoute{
		Selected:            d.selected.kind.String(),
		Rejected:            d.rejected.kind.String(),
		SelectedCost:        d.selected.cost,
		RejectedCost:        d.rejected.cost,
		ExpectedRows:        d.selected.expectedRows,
		UnionRows:           d.selected.unionRows,
		SumRows:             d.selected.sumRows,
		CacheState:          d.selected.cacheState.String(),
		PostingBuild:        d.selected.eagerBuildWork,
		Overlap:             d.selected.overlap,
		AvgChecks:           d.selected.avgChecks,
		HasPrefixNonOrder:   d.selected.hasPrefixTailRisk,
		HasSelectiveLead:    d.selected.hasSelectiveLead,
		FallbackCollectFast: d.selected.fallbackCollectFast,
	}
}

type plannerOROrderBranchAnalysis struct {
	rangeStart int
	rangeEnd   int
	universe   uint64
	covered    []bool
	predBuild  []orderedORMaterializedPredicateBuildInfo
	buildReady bool
}

type plannerOROrderAnalysis struct {
	orderField       string
	snapshotUniverse uint64
	branchCount      int
	exactUniverses   int
	reusedUniverses  int
	mergeStats       [plannerORBranchLimit]plannerOROrderMergeBranchStats
	branches         [plannerORBranchLimit]plannerOROrderBranchAnalysis
}

func (a *plannerOROrderAnalysis) release() {
	if a == nil {
		return
	}
	n := a.branchCount
	if n > plannerORBranchLimit {
		n = plannerORBranchLimit
	}
	for i := 0; i < n; i++ {
		if a.branches[i].covered != nil {
			pooled.ReleaseBoolSlice(a.branches[i].covered)
		}
		if a.branches[i].predBuild != nil {
			plannerORPredicateBuildInfoSlicePool.Put(a.branches[i].predBuild)
		}
	}
	*a = plannerOROrderAnalysis{}
}

func (a *plannerOROrderAnalysis) applyCovered(branches plannerORBranches) {
	if a == nil {
		return
	}
	n := min(a.branchCount, branches.Len())
	for i := 0; i < n; i++ {
		covered := a.branches[i].covered
		if len(covered) == 0 {
			continue
		}
		branch := &branches.owner[i]
		limit := min(len(covered), branch.preds.Len())
		for pi := 0; pi < limit; pi++ {
			if covered[pi] {
				branch.predPtr(pi).covered = true
			}
		}
	}
}

func (a *plannerOROrderAnalysis) predicateBuildInfo(branchIdx, predIdx int) (orderedORMaterializedPredicateBuildInfo, bool) {
	if a == nil || branchIdx < 0 || branchIdx >= a.branchCount {
		return orderedORMaterializedPredicateBuildInfo{}, false
	}
	buf := a.branches[branchIdx].predBuild
	if buf == nil || predIdx < 0 || predIdx >= len(buf) {
		return orderedORMaterializedPredicateBuildInfo{}, false
	}
	info := buf[predIdx]
	return info, info.ok
}

func (a *plannerOROrderAnalysis) refreshBranch(branches plannerORBranches, branchIdx int) {
	if a == nil || branchIdx < 0 || branchIdx >= a.branchCount || branchIdx >= branches.Len() {
		return
	}
	covered := a.branches[branchIdx].covered
	branch := &branches.owner[branchIdx]
	hasFalse := false
	allTrue := true
	leadIdx := -1
	leadEst := uint64(0)

	for pi := 0; pi < branch.preds.Len(); pi++ {
		p := branch.preds.owner[pi]
		if p.alwaysFalse {
			hasFalse = true
			break
		}
		if p.covered || p.alwaysTrue {
			continue
		}
		if len(covered) != 0 && covered[pi] {
			continue
		}
		allTrue = false
		if p.hasIter() && (leadIdx == -1 || p.estCard < leadEst) {
			leadIdx = pi
			leadEst = p.estCard
		}
	}

	branch.alwaysTrue = false
	branch.leadIdx = -1

	if hasFalse {
		branch.estCard = 0
		branch.estKnown = true
		branch.coveredRangeBounded = true
		branch.coveredRangeStart = 0
		branch.coveredRangeEnd = 0
		a.branches[branchIdx].rangeStart = 0
		a.branches[branchIdx].rangeEnd = 0
		a.branches[branchIdx].universe = 0
		a.mergeStats[branchIdx].rangeBounded = true
		a.mergeStats[branchIdx].rangeRows = 0

	} else if allTrue {

		if a.mergeStats[branchIdx].rangeBounded {
			branch.estCard = a.branches[branchIdx].universe
			branch.estKnown = true
			branch.coveredRangeBounded = true
			branch.coveredRangeStart = a.branches[branchIdx].rangeStart
			branch.coveredRangeEnd = a.branches[branchIdx].rangeEnd
		} else {
			branch.alwaysTrue = true
			branch.estKnown = false
			branch.estCard = 0
			branch.coveredRangeBounded = false
			branch.coveredRangeStart = 0
			branch.coveredRangeEnd = 0
		}

	} else {
		branch.leadIdx = leadIdx
	}

	a.mergeStats[branchIdx].streamChecks, a.mergeStats[branchIdx].mergeChecks = plannerORBranchCheckCounts(*branch, covered)
	if a.branches[branchIdx].predBuild != nil {
		plannerORPredicateBuildInfoSlicePool.Put(a.branches[branchIdx].predBuild)
		a.branches[branchIdx].predBuild = nil
	}
	a.branches[branchIdx].buildReady = false
}

func (qv *View) buildOROrderAnalysis(q *qir.Shape, branches plannerORBranches) (plannerOROrderAnalysis, bool) {
	var analysis plannerOROrderAnalysis
	if q == nil || !q.HasOrder {
		return analysis, false
	}
	order := q.Order
	if order.Kind != qir.OrderKindBasic || order.FieldOrdinal < 0 {
		return analysis, false
	}
	ov := qv.fieldIndexViewFromSlotsForOrder(qv.snap.Index, order)
	if !ov.HasData() {
		return analysis, false
	}
	snapshotUniverse := qv.snap.Universe.Cardinality()
	if snapshotUniverse == 0 {
		return analysis, false
	}
	orderField := qv.exec.FieldNameByOrdinal(order.FieldOrdinal)
	branchCount := branches.Len()
	if branchCount > plannerORBranchLimit {
		branchCount = plannerORBranchLimit
	}
	analysis.orderField = orderField
	analysis.snapshotUniverse = snapshotUniverse
	analysis.branchCount = branchCount
	fullSpanEnd := ov.KeyCount()

	for i := 0; i < branchCount; i++ {
		branch := &branches.owner[i]
		br, covered, ok := qv.extractOrderRangeCoverageIndexViewReader(orderField, branch.preds.owner, ov)
		if !ok {
			analysis.release()
			return plannerOROrderAnalysis{}, false
		}
		analysis.branches[i].covered = covered
		analysis.branches[i].rangeStart = br.BaseStart
		analysis.branches[i].rangeEnd = br.BaseEnd

		if len(covered) == 0 {
			analysis.mergeStats[i].streamChecks, analysis.mergeStats[i].mergeChecks = plannerORBranchCheckCounts(*branch, nil)
		} else {
			analysis.mergeStats[i].streamChecks, analysis.mergeStats[i].mergeChecks = plannerORBranchCheckCounts(*branch, covered)
		}

		if br.BaseStart == 0 && br.BaseEnd == fullSpanEnd {
			analysis.branches[i].universe = snapshotUniverse
			analysis.reusedUniverses++
			continue
		}

		analysis.mergeStats[i].rangeBounded = true
		if branch.coveredRangeBounded &&
			branch.estKnown &&
			branch.coveredRangeStart == br.BaseStart &&
			branch.coveredRangeEnd == br.BaseEnd {
			analysis.mergeStats[i].rangeRows = branch.estCard
			analysis.branches[i].universe = branch.estCard
			analysis.reusedUniverses++
			continue
		}

		_, analysis.mergeStats[i].rangeRows = ov.RangeStats(br)
		analysis.branches[i].universe = analysis.mergeStats[i].rangeRows
		analysis.exactUniverses++

		if branch.coveredRangeBounded &&
			!branch.estKnown &&
			branch.coveredRangeStart == br.BaseStart &&
			branch.coveredRangeEnd == br.BaseEnd {
			branch.estCard = analysis.mergeStats[i].rangeRows
			branch.estKnown = true
		}
	}
	return analysis, true
}

func plannerORNoOrderPick(candidates []plannerORNoOrderCandidate, forceMaterialized bool) plannerORNoOrderDecision {
	var (
		d        plannerORNoOrderDecision
		bestRun  plannerORNoOrderCandidate
		fallback plannerORNoOrderCandidate
	)
	for i := range candidates {
		c := candidates[i]
		if c.kind == plannerORNoOrderCandidateNone {
			continue
		}
		if c.kind == plannerORNoOrderCandidateMaterializedFallback {
			fallback = c
			continue
		}
		if bestRun.kind == plannerORNoOrderCandidateNone || c.cost < bestRun.cost {
			bestRun = c
		}
	}

	d.kWayCost = bestRun.cost
	d.fallbackCost = fallback.cost
	d.materializedFallback = fallback
	if !forceMaterialized &&
		bestRun.kind != plannerORNoOrderCandidateNone &&
		(fallback.kind == plannerORNoOrderCandidateNone || bestRun.cost <= fallback.cost*plannerORNoOrderAdvantage) {
		d.selected = bestRun
		d.use = true
	} else {
		d.selected = fallback
	}
	for i := range candidates {
		c := candidates[i]
		if c.kind == plannerORNoOrderCandidateNone || c.kind == d.selected.kind {
			continue
		}
		if d.rejected.kind == plannerORNoOrderCandidateNone || c.cost < d.rejected.cost {
			d.rejected = c
		}
	}
	d.expectedRows = d.selected.expectedRows
	return d
}

func (qv *View) selectPlanORNoOrder(q *qir.Shape, branches plannerORBranches) plannerORNoOrderDecision {
	need, ok := orderWindow(q)
	if !ok || need <= 0 {
		return plannerORNoOrderDecision{}
	}
	switch plannerORNoOrderClassify(branches) {
	case plannerORNoOrderModeUniverse:
		return plannerORNoOrderDecision{
			selected:     plannerORNoOrderCandidate{kind: plannerORNoOrderCandidateUniverse, cost: 1, expectedRows: uint64(need)},
			use:          true,
			expectedRows: uint64(need),
		}
	case plannerORNoOrderModeUnsupported:
		fallback := plannerORNoOrderCandidate{
			kind:         plannerORNoOrderCandidateMaterializedFallback,
			cost:         float64(need) * float64(max(1, branches.Len())),
			expectedRows: uint64(need),
		}
		return plannerORNoOrderDecision{
			selected:             fallback,
			materializedFallback: fallback,
		}
	}

	snap := qv.exec.Stats.Load()
	universe := qv.plannerUniverseCardinality(snap)
	if universe == 0 {
		return plannerORNoOrderDecision{}
	}

	var branchCards [plannerORBranchLimit]uint64
	unionCard, sumCard, branchCount, hasAlwaysTrue := branches.unionCards(universe, &branchCards)
	if hasAlwaysTrue {
		return plannerORNoOrderDecision{
			selected:     plannerORNoOrderCandidate{kind: plannerORNoOrderCandidateUniverse, cost: 1, expectedRows: uint64(need), unionRows: universe, sumRows: universe},
			use:          true,
			expectedRows: uint64(need),
		}
	}
	if unionCard == 0 {
		return plannerORNoOrderDecision{}
	}

	expectedRows := estimateRowsForNeed(uint64(need), unionCard, universe)
	avgChecks := branches.noOrderChecks(&branchCards, branchCount)
	costKWay := float64(expectedRows) * (1.0 + avgChecks)
	fallbackRows := min(unionCard, uint64(need))
	costFallback := float64(sumCard) + float64(fallbackRows)

	var candidates [3]plannerORNoOrderCandidate
	n := 0
	if q.Offset == 0 && branches.Len() >= 3 {
		candidates[n] = plannerORNoOrderCandidate{
			kind:         plannerORNoOrderCandidateAdaptiveMerge,
			cost:         costKWay,
			expectedRows: expectedRows,
			unionRows:    unionCard,
			sumRows:      sumCard,
			avgChecks:    avgChecks,
		}
		n++
	}
	candidates[n] = plannerORNoOrderCandidate{
		kind:         plannerORNoOrderCandidateBaselineMerge,
		cost:         costKWay,
		expectedRows: expectedRows,
		unionRows:    unionCard,
		sumRows:      sumCard,
		avgChecks:    avgChecks,
	}
	n++
	candidates[n] = plannerORNoOrderCandidate{
		kind:         plannerORNoOrderCandidateMaterializedFallback,
		cost:         costFallback,
		expectedRows: fallbackRows,
		unionRows:    unionCard,
		sumRows:      sumCard,
		avgChecks:    avgChecks,
	}
	n++

	return plannerORNoOrderPick(candidates[:n], q.Limit > plannerORNoOrderLimitMax || q.Offset > plannerORNoOrderOffsetMax)
}

func (qv *View) hasNonOrderPrefixTailRisk(branches plannerORBranches, orderField string) bool {
	for i := 0; i < branches.Len(); i++ {
		branch := branches.owner[i]
		for pi := 0; pi < branch.preds.Len(); pi++ {
			p := branch.preds.owner[pi]
			if qv.isPositiveNonOrderScalarPrefixLeaf(orderField, p.expr) && !p.isMaterializedLike() {
				return true
			}
		}
	}
	return false
}

func (qv *View) selectPlanOROrder(q *qir.Shape, branches plannerORBranches) plannerOROrderDecision {
	analysis, ok := qv.buildOROrderAnalysis(q, branches)
	if !ok {
		return plannerOROrderDecision{plan: plannerOROrderFallback}
	}
	defer analysis.release()

	return qv.selectPlanOROrderWithAnalysis(q, branches, &analysis)
}

func plannerOROrderPick(candidates []plannerOROrderCandidate, forceMaterialized bool) plannerOROrderDecision {
	var (
		d        plannerOROrderDecision
		fallback plannerOROrderCandidate
	)
	for i := range candidates {
		if candidates[i].kind == plannerOROrderCandidateMaterializedFallback {
			fallback = candidates[i]
			break
		}
	}
	if fallback.kind == plannerOROrderCandidateNone {
		return d
	}

	best := fallback
	if !forceMaterialized {
		for i := range candidates {
			c := candidates[i]
			switch c.kind {
			case plannerOROrderCandidateKWayMerge, plannerOROrderCandidateBranchCollect:
				if c.cost <= best.cost*plannerOROrderMergeGain {
					best = c
				}
			}
		}
		for i := range candidates {
			c := candidates[i]
			if c.kind == plannerOROrderCandidateStream && c.cost <= best.cost*plannerOROrderStreamGain {
				best = c
				break
			}
		}
	}

	d.selected = best
	d.materializedFallback = fallback
	for i := range candidates {
		c := candidates[i]
		if c.kind == plannerOROrderCandidateNone || c.kind == best.kind {
			continue
		}
		if d.rejected.kind == plannerOROrderCandidateNone || c.cost < d.rejected.cost {
			d.rejected = c
		}
	}
	d.plan = best.kind.plan()
	d.expectedRows = best.expectedRows
	d.bestCost = best.cost
	d.fallbackCost = fallback.cost
	return d
}

func plannerOROrderPreferMaterializedSmallSideBranches(
	q *qir.Shape,
	branchCards *[plannerORBranchLimit]uint64,
	branchCount int,
	unionCard uint64,
	need int,
	stats *[plannerORBranchLimit]plannerOROrderMergeBranchStats,
) bool {
	if q.Offset != 0 || branchCount < 3 || unionCard == 0 || need <= 0 {
		return false
	}
	smallMax := uint64(need) * 32
	if smallMax < 4096 {
		smallMax = 4096
	}
	wideMin := smallMax * 8
	if unionCard < wideMin {
		return false
	}
	small := 0
	for i := 0; i < branchCount && i < plannerORBranchLimit; i++ {
		card := branchCards[i]
		// A dominant covered order branch can satisfy LIMIT by scanning order buckets;
		// forcing fallback would materialize the broad side branch.
		if card > smallMax && card >= unionCard-card && stats[i].streamChecks == 0 {
			return false
		}
		if card > 0 && card <= smallMax {
			small++
		}
	}
	return small >= 2
}

func plannerOROrderFallbackFirstDecisionFromCost(
	q *qir.Shape,
	need int,
	routeCost plannerOROrderRouteCost,
	fallbackCollectFast bool,
) plannerOROrderFallbackDecision {
	avgChecks := routeCost.avgChecks
	if avgChecks <= 0 {
		avgChecks = 1
	}

	d := plannerOROrderFallbackDecision{
		routeCost:           routeCost,
		avgChecks:           avgChecks,
		fallbackCollectFast: fallbackCollectFast,
	}
	if !fallbackCollectFast {
		d.reason = "order_field_no_index_data"
		return d
	}

	fallbackGain := plannerOROrderFallbackFirstGainForShape(routeCost, q, need)
	offsetFallbackGain := plannerOROrderFallbackFirstOffsetGainForShape(routeCost, q, need)

	if routeCost.fallback <= routeCost.kWay*fallbackGain {
		d.prefer = true
		d.reason = "fallback_cost_better"
		return d
	}

	if q.Offset > 0 {
		offsetRiskMin := uint64(max(64, need/5))
		checksRiskMin := 1.0 + routeCost.overlap
		if checksRiskMin < 2.3 {
			checksRiskMin = 2.3
		}
		if checksRiskMin > 3.4 {
			checksRiskMin = 3.4
		}
		if q.Offset >= offsetRiskMin &&
			avgChecks >= checksRiskMin &&
			(routeCost.hasSelectiveLead || routeCost.hasPrefixTailRisk) {
			d.prefer = true
			d.reason = "deep_offset_risk"
			return d
		}
		if routeCost.hasPrefixTailRisk && routeCost.fallback <= routeCost.kWay*offsetFallbackGain {
			d.prefer = true
			d.reason = "prefix_offset_near_tie"
			return d
		}
		if !routeCost.hasSelectiveLead && routeCost.overlap >= 1.2 &&
			routeCost.fallback <= routeCost.kWay*1.25 {
			d.prefer = true
			d.reason = "overlap_offset_near_tie"
			return d
		}
	}

	d.reason = "kway_preferred"
	return d
}

func (qv *View) orderedORCacheCandidateState(
	branches plannerORBranches,
	analysis *plannerOROrderAnalysis,
	needWindow int,
	orderedOffset uint64,
) (plannerMaterializedCacheState, uint64) {
	var branchUniverses [plannerORBranchLimit]uint64
	n := branches.Len()
	if n > plannerORBranchLimit {
		n = plannerORBranchLimit
	}
	for i := 0; i < n; i++ {
		branchUniverses[i] = analysis.branches[i].universe
	}

	var estimates [plannerORBranchLimit]plannerOROrderedBranchEstimate
	qv.initOrderedORBranchEstimates(branches, &branchUniverses, needWindow, &estimates)

	state := plannerMaterializedCacheDisabled
	buildWork := uint64(0)
	for bi := 0; bi < n; bi++ {
		est := estimates[bi]
		if est.probeRows == 0 || est.universe == 0 {
			continue
		}
		branch := branches.owner[bi]
		rows := est.probeRows
		for pi := 0; pi < branch.preds.Len() && rows > 0; pi++ {
			p := branch.preds.owner[pi]
			if p.alwaysTrue || p.alwaysFalse || p.covered || !p.hasContains() {
				continue
			}
			info := qv.orderedORPredicateBuildInfoForBranch(analysis.orderField, p, analysis, branch, bi, pi)
			if !info.ok || info.buildWork == 0 {
				rows = orderedORNextPredicateRows(rows, p.estCard, est.card, est.universe)
				continue
			}
			nextRows := orderedORNextPredicateRows(rows, p.estCard, est.card, est.universe)
			next := qv.classifyPlannerMaterializedCacheKey(info.cacheKey, p.estCard, false)
			if next == plannerMaterializedCacheWarmHit {
				if info.isPrefix || orderedORMaterializedPredicateWarmHitWorth(rows, info.checkWork, info.cachedCheckWork) {
					state = plannerOrderedLimitCacheGroupState(state, next)
				}
				rows = nextRows
				continue
			}
			if !info.isPrefix && rows > 0 && info.checkWork > info.cachedCheckWork {
				savedWork := satMulUint64(rows, info.checkWork-info.cachedCheckWork)
				if savedWork >= info.buildWork {
					requiresPromotion := qv.orderedORMaterializedPredicateRequiresPromotion(p, est.need, orderedOffset, est.universe)
					requiresSecondHit := requiresPromotion ||
						!orderedORMaterializedPredicateColdBuildWorth(info.buildWork, info.cachedCheckWork, rows, savedWork)
					next = qv.classifyPlannerMaterializedCacheKey(info.cacheKey, p.estCard, requiresSecondHit)
					state = plannerOrderedLimitCacheGroupState(state, next)
					if next != plannerMaterializedCacheColdSecondHitRequired {
						buildWork = satAddUint64(buildWork, info.buildWork)
					}
				}
			}
			rows = nextRows
		}
	}
	return state, buildWork
}

func (qv *View) selectPlanOROrderWithAnalysis(
	q *qir.Shape,
	branches plannerORBranches,
	analysis *plannerOROrderAnalysis,
) plannerOROrderDecision {
	if q == nil || !q.HasOrder || analysis == nil {
		return plannerOROrderDecision{plan: plannerOROrderFallback}
	}

	o := q.Order
	if o.Kind != qir.OrderKindBasic {
		return plannerOROrderDecision{plan: plannerOROrderFallback}
	}
	orderField := analysis.orderField

	fm := qv.fieldMetaByOrder(o)
	if fm == nil || fm.Slice {
		return plannerOROrderDecision{plan: plannerOROrderFallback}
	}

	ov := qv.fieldIndexViewFromSlotsForOrder(qv.snap.Index, o)
	if !ov.HasData() {
		return plannerOROrderDecision{plan: plannerOROrderFallback}
	}
	orderDistinct := uint64(ov.KeyCount())
	if orderDistinct == 0 {
		return plannerOROrderDecision{plan: plannerOROrderFallback}
	}
	need, ok := orderWindow(q)
	if !ok || need <= 0 {
		return plannerOROrderDecision{plan: plannerOROrderFallback}
	}

	snap := qv.exec.Stats.Load()
	universe := snap.UniverseOr(analysis.snapshotUniverse)
	if universe == 0 {
		return plannerOROrderDecision{plan: plannerOROrderFallback}
	}

	var branchCards [plannerORBranchLimit]uint64
	unionCard, sumCard, branchCount, hasAlwaysTrue := branches.unionCards(universe, &branchCards)
	if unionCard == 0 {
		return plannerOROrderDecision{plan: plannerOROrderFallback}
	}

	orderStats := qv.plannerOrderFieldStats(orderField, snap, universe, orderDistinct)
	expectedRows := estimateRowsForNeed(uint64(need), unionCard, universe)
	streamChecks := branches.orderStreamChecksByBranch(hasAlwaysTrue, analysis.mergeStats)
	costFallback := float64(sumCard) + float64(expectedRows)
	cacheState := plannerMaterializedCacheDisabled
	eagerBuildWork := uint64(0)
	if q.Offset > 0 {
		cacheState, eagerBuildWork = qv.orderedORCacheCandidateState(branches, analysis, need, q.Offset)
	}
	eagerCost := 0.0
	if eagerBuildWork > 0 {
		eagerCost = float64(eagerBuildWork)
	}
	costStream := float64(expectedRows)*streamChecks*plannerFieldStatsSkew(orderStats) + eagerCost

	var candidates [4]plannerOROrderCandidate
	n := 0
	candidates[n] = plannerOROrderCandidate{
		kind:         plannerOROrderCandidateMaterializedFallback,
		cost:         costFallback,
		expectedRows: expectedRows,
		unionRows:    unionCard,
		sumRows:      sumCard,
		cacheState:   cacheState,
	}
	n++

	routeCost, routeOK := qv.estimateOROrderMergeRouteCost(q, branches, need, analysis.mergeStats)
	mergeNeedLimit := plannerOROrderMergeNeedLimit(need, branches.Len(), unionCard, sumCard, q.Offset)
	mergeAllowed := !hasAlwaysTrue && need <= mergeNeedLimit
	if mergeAllowed {
		costMerge := branches.orderMergeCost(uint64(need), &branchCards, branchCount, unionCard, sumCard, universe, orderStats, analysis.mergeStats)
		prefixTailRisk := routeOK && routeCost.hasPrefixTailRisk
		if prefixTailRisk {
			costMerge *= plannerOROrderExactBucketApplyPenalty(&analysis.mergeStats, branchCount)
		}
		costMerge *= plannerOROrderPrefixTailRiskPenalty(prefixTailRisk, branchCount, q.Offset)
		if routeOK && routeCost.kWay > 0 && routeCost.kWay < costMerge &&
			branches.hasFullSpanOrderBranch(analysis.mergeStats) &&
			!routeCost.hasPrefixTailRisk &&
			!branches.hasKWayExactBucketApplyWork(analysis.mergeStats) {
			costMerge = routeCost.kWay
		}
		costMerge += eagerCost
		candidates[n] = plannerOROrderCandidate{
			kind:                plannerOROrderCandidateKWayMerge,
			cost:                costMerge,
			expectedRows:        expectedRows,
			unionRows:           unionCard,
			sumRows:             sumCard,
			avgChecks:           routeCost.avgChecks,
			overlap:             routeCost.overlap,
			cacheState:          cacheState,
			eagerBuildWork:      eagerBuildWork,
			hasPrefixTailRisk:   routeCost.hasPrefixTailRisk,
			hasSelectiveLead:    routeCost.hasSelectiveLead,
			fallbackCollectFast: ov.HasData(),
		}
		n++
		if routeOK && routeCost.fallback > 0 {
			fallbackFirst := plannerOROrderFallbackFirstDecisionFromCost(q, need, routeCost, ov.HasData())
			if fallbackFirst.prefer {
				candidates[n] = plannerOROrderCandidate{
					kind:                plannerOROrderCandidateBranchCollect,
					cost:                routeCost.fallback + eagerCost,
					expectedRows:        expectedRows,
					unionRows:           unionCard,
					sumRows:             sumCard,
					avgChecks:           fallbackFirst.avgChecks,
					overlap:             routeCost.overlap,
					cacheState:          cacheState,
					eagerBuildWork:      eagerBuildWork,
					hasPrefixTailRisk:   routeCost.hasPrefixTailRisk,
					hasSelectiveLead:    routeCost.hasSelectiveLead,
					fallbackCollectFast: fallbackFirst.fallbackCollectFast,
				}
				n++
			}
		}
	}

	candidates[n] = plannerOROrderCandidate{
		kind:           plannerOROrderCandidateStream,
		cost:           costStream,
		expectedRows:   expectedRows,
		unionRows:      unionCard,
		sumRows:        sumCard,
		avgChecks:      streamChecks,
		cacheState:     cacheState,
		eagerBuildWork: eagerBuildWork,
	}
	n++

	forceMaterialized := q.Limit > plannerOROrderLimitMax ||
		q.Offset > plannerOROrderOffsetMax ||
		plannerOROrderPreferMaterializedSmallSideBranches(q, &branchCards, branchCount, unionCard, need, &analysis.mergeStats)
	return plannerOROrderPick(candidates[:n], forceMaterialized)
}

// unionCards estimates planner metrics for OR union cardinalities.
func (branches plannerORBranches) unionCards(universe uint64, branchCards *[plannerORBranchLimit]uint64) (uint64, uint64, int, bool) {
	if universe == 0 {
		return 0, 0, 0, false
	}

	n := branches.Len()
	if n > plannerORBranchLimit {
		n = plannerORBranchLimit
	}
	sumCard := uint64(0)
	remainProb := 1.0

	for i := 0; i < n; i++ {
		branch := branches.owner[i]
		if branch.alwaysTrue {
			if branchCards != nil {
				branchCards[i] = universe
			}
			return universe, universe, n, true
		}

		card := branch.estimatedCard(universe)
		if branchCards != nil {
			branchCards[i] = card
		}
		sumCard += card

		p := float64(card) / float64(universe)
		if p < 0 {
			p = 0
		}
		if p > 1 {
			p = 1
		}
		remainProb *= 1.0 - p
	}

	unionCard := uint64((1.0-remainProb)*float64(universe) + 0.5)
	if unionCard > universe {
		unionCard = universe
	}
	if unionCard == 0 && sumCard > 0 {
		unionCard = 1
	}
	return unionCard, sumCard, n, false
}

func (b plannerORBranch) estimatedCard(universe uint64) uint64 {
	if universe == 0 {
		return 0
	}
	if b.alwaysTrue {
		return universe
	}
	if b.estKnown {
		if b.estCard > universe {
			return universe
		}
		return b.estCard
	}

	minEst := uint64(0)
	active := 0
	for i := 0; i < b.preds.Len(); i++ {
		p := b.preds.owner[i]
		if p.alwaysFalse {
			return 0
		}
		if p.alwaysTrue {
			continue
		}
		if p.estCard == 0 {
			continue
		}
		active++
		if minEst == 0 || p.estCard < minEst {
			minEst = p.estCard
		}
	}

	if minEst == 0 {
		if lead := b.leadPtr(); lead != nil && lead.estCard > 0 {
			minEst = lead.estCard
			active = 1
		} else {
			minEst = max(1, universe/plannerORBranchFallbackDiv)
			active = 1
		}
	}

	est := minEst
	if active > 1 {
		decay := 1.0
		for i := 1; i < active; i++ {
			decay *= 0.72
		}
		est = uint64(float64(est) * decay)
		if est == 0 {
			est = 1
		}
	}

	if est > universe {
		est = universe
	}
	return est
}

func (b plannerORBranch) estimatedCardWithinOrderedUniverse(snapshotUniverse, universe uint64) uint64 {
	if snapshotUniverse == 0 || universe == 0 {
		return 0
	}
	if b.alwaysTrue {
		return universe
	}

	hasCovered := false
	for i := 0; i < b.preds.Len(); i++ {
		if b.preds.owner[i].covered {
			hasCovered = true
			break
		}
	}
	if !hasCovered {
		card := b.estimatedCard(snapshotUniverse)
		if card > universe {
			card = universe
		}
		return card
	}

	minEst := uint64(0)
	active := 0
	uncovered := 0

	for i := 0; i < b.preds.Len(); i++ {
		p := b.preds.owner[i]
		if p.alwaysFalse {
			return 0
		}
		if p.alwaysTrue || p.covered {
			continue
		}
		uncovered++
		if p.estCard == 0 {
			continue
		}
		active++
		if minEst == 0 || p.estCard < minEst {
			minEst = p.estCard
		}
	}

	if uncovered == 0 {
		if b.estKnown {
			if b.estCard > universe {
				return universe
			}
			return b.estCard
		}
		return universe
	}

	if minEst == 0 {
		if b.hasLead() {
			lead := b.preds.owner[b.leadIdx]
			if !lead.covered && !lead.alwaysTrue && !lead.alwaysFalse && lead.estCard > 0 {
				minEst = lead.estCard
				active = 1
			}
		}
		if minEst == 0 {
			minEst = max(1, snapshotUniverse/plannerORBranchFallbackDiv)
			active = 1
		}
	}

	est := minEst
	if active > 1 {
		decay := 1.0
		for i := 1; i < active; i++ {
			decay *= 0.72
		}
		est = uint64(float64(est) * decay)
		if est == 0 {
			est = 1
		}
	}
	if est > snapshotUniverse {
		est = snapshotUniverse
	}
	if snapshotUniverse == universe {
		return est
	}

	scaled := uint64(math.Ceil(float64(est) * float64(universe) / float64(snapshotUniverse)))
	if scaled == 0 {
		scaled = 1
	}
	if scaled > universe {
		scaled = universe
	}
	return scaled
}

func estimateRowsForNeed(need, card, universe uint64) uint64 {
	if need == 0 || card == 0 || universe == 0 {
		return 0
	}
	if card >= universe {
		return min(need, universe)
	}

	rows := math.Ceil(float64(need) * float64(universe) / float64(card))
	if rows < float64(need) {
		rows = float64(need)
	}
	if rows > float64(universe) {
		rows = float64(universe)
	}
	return uint64(rows)
}

func (branches plannerORBranches) noOrderChecks(branchCards *[plannerORBranchLimit]uint64, branchCount int) float64 {
	if branches.Len() == 0 {
		return 1.0
	}

	totalWeight := float64(0)
	totalChecks := float64(0)

	n := branches.Len()
	if branchCount > 0 && branchCount < n {
		n = branchCount
	}
	for i := 0; i < n; i++ {
		branch := branches.owner[i]
		weight := float64(1)
		if branchCards != nil && branchCards[i] > 0 {
			weight = float64(branchCards[i])
		}

		checks := 0
		for pi := 0; pi < branch.preds.Len(); pi++ {
			if pi == branch.leadIdx {
				continue
			}
			p := branch.preds.owner[pi]
			if p.alwaysTrue || p.alwaysFalse {
				continue
			}
			if p.contains != nil {
				checks++
			}
		}

		totalWeight += weight
		totalChecks += weight * float64(checks)
	}

	if totalWeight == 0 {
		return 1.0
	}

	avg := totalChecks / totalWeight
	if avg < 1 {
		avg = 1
	}
	return avg
}

func (branches plannerORBranches) orderStreamChecksByBranch(
	hasAlwaysTrue bool,
	stats [plannerORBranchLimit]plannerOROrderMergeBranchStats,
) float64 {
	if hasAlwaysTrue {
		return 1.0
	}

	if branches.Len() == 0 {
		return 1.0
	}

	total := float64(0)
	n := branches.Len()
	if n > plannerORBranchLimit {
		n = plannerORBranchLimit
	}
	for i := 0; i < n; i++ {
		checks := 1 + stats[i].streamChecks
		total += float64(checks)
	}

	avg := total / float64(n)
	if avg < 1 {
		avg = 1
	}

	checks := 1.0 + (avg-1.0)*0.65
	branchFactor := 1.0 + float64(n-1)*0.35
	return checks * branchFactor
}

func (branches plannerORBranches) orderMergeCost(
	need uint64,
	branchCards *[plannerORBranchLimit]uint64,
	branchCount int,
	unionCard, sumCard, universe uint64,
	orderStats PlannerFieldStats,
	branchStats [plannerORBranchLimit]plannerOROrderMergeBranchStats,
) float64 {

	if need == 0 || universe == 0 {
		return math.Inf(1)
	}

	subRows := float64(0)
	candidateUpper := uint64(0)

	n := branches.Len()
	if branchCount > 0 && branchCount < n {
		n = branchCount
	}

	for i := 0; i < n; i++ {
		card := branchCards[i]
		if card == 0 {
			continue
		}

		branchUniverse := universe
		if branchStats[i].rangeRows > 0 && branchStats[i].rangeRows < branchUniverse {
			branchUniverse = branchStats[i].rangeRows
		}
		rows := estimateRowsForNeed(need, card, branchUniverse)
		if branchUniverse == universe && card >= need {
			rowsCap := need + need/2
			if rowsCap < need {
				rowsCap = need
			}
			rows = min(rows, rowsCap)
		}

		checks := 1.0 + float64(branchStats[i].mergeChecks)*0.25
		subRows += float64(rows) * checks
		candidateUpper += min(need, card)
	}

	if candidateUpper == 0 {
		candidateUpper = min(need, unionCard)
	}

	if sumCard > 0 {
		candidateUpper = uint64(float64(candidateUpper) * (float64(unionCard) / float64(sumCard)))
		if candidateUpper == 0 {
			candidateUpper = 1
		}
	}

	rankRows := plannerFieldStatsMergeRankRows(orderStats, candidateUpper, need, universe)
	return subRows + float64(rankRows)
}

func plannerFieldStatsMergeRankRows(stats PlannerFieldStats, candidateUpper, need, universe uint64) uint64 {
	if candidateUpper == 0 || need == 0 || universe == 0 {
		return 0
	}

	avgBucket := stats.AvgBucketCard
	if avgBucket <= 0 {
		if stats.DistinctKeys > 0 {
			avgBucket = float64(universe) / float64(stats.DistinctKeys)
		} else {
			avgBucket = 1
		}
	}
	if avgBucket < 1 {
		avgBucket = 1
	}

	// Use fallback avgBucket when planner stats are incomplete.
	p95 := float64(stats.P95BucketCard)
	if p95 < avgBucket {
		p95 = avgBucket
	}
	skew := p95 / avgBucket
	if skew < 1 {
		skew = 1
	}
	if skew > 8 {
		skew = 8
	}
	headAmplifier := 1.0 + float64(candidateUpper)/float64(max(need, 1))*0.35

	rows := float64(candidateUpper) * skew * headAmplifier
	if rows < float64(need) {
		rows = float64(need)
	}
	if rows > float64(universe) {
		rows = float64(universe)
	}
	return uint64(rows)
}

func (qv *View) plannerOrderFieldStats(field string, snap *PlannerStatsSnapshot, universe uint64, distinctFallback uint64) PlannerFieldStats {
	if snap != nil {
		if s, ok := snap.Fields[field]; ok {
			return s
		}
	}

	if distinctFallback == 0 {
		distinctFallback = 1
	}
	avg := float64(universe) / float64(distinctFallback)
	if avg < 1 {
		avg = 1
	}

	p95 := uint64(avg * 2)
	if p95 < 1 {
		p95 = 1
	}

	return PlannerFieldStats{
		DistinctKeys:    distinctFallback,
		NonEmptyKeys:    distinctFallback,
		TotalBucketCard: universe,
		AvgBucketCard:   avg,
		MaxBucketCard:   p95,
		P50BucketCard:   uint64(avg),
		P95BucketCard:   p95,
	}
}

func plannerFieldStatsSkew(stats PlannerFieldStats) float64 {
	avg := stats.AvgBucketCard
	if avg <= 0 {
		return 1.0
	}

	p95 := float64(stats.P95BucketCard)
	if p95 <= 0 {
		return 1.0
	}

	skew := p95 / avg
	if skew < 1 {
		skew = 1
	}
	if skew > 8 {
		skew = 8
	}
	return skew
}

const (
	plannerOrderedLeafMax              = 24
	plannerOrderedNeedHardMax          = 2_000_000
	plannerOrderedCostGainDefault      = 0.97
	plannerOrderedCostGainNoNeg        = 0.90
	plannerOrderedFallbackProbeBase    = 0.78
	plannerOrderedFallbackProbeSkewMul = 0.14
	plannerExecutionPreferGain         = 0.98

	plannerExecutionOrderBaseFactor     = 0.92
	plannerExecutionOrderCheckFactor    = 0.28
	plannerExecutionOrderRangeFactor    = 0.78
	plannerExecutionOrderPrefixFactor   = 0.62
	plannerExecutionOrderBaseWorkFactor = 0.003

	plannerExecutionNoOrderPrefixLeafMax   = 6
	plannerExecutionNoOrderPrefixLimitMax  = 256
	plannerExecutionNoOrderPrefixBucketMax = 8 << 10
	plannerExecutionNoOrderPrefixProbeMax  = 128 << 10

	plannerExecutionNoOrderPrefixProbeShareMax = 0.40
)

func plannerExecutionOrderFactors(profile plannerOrderedProfile, orderSkew float64, universe uint64) (base, check, rangeMul, prefixMul float64) {
	base = plannerExecutionOrderBaseFactor
	check = plannerExecutionOrderCheckFactor
	rangeMul = plannerExecutionOrderRangeFactor
	prefixMul = plannerExecutionOrderPrefixFactor

	if profile.coverage > 0 && profile.coverage < 0.75 {
		// keep adaptation conservative: only narrow windows should move factors
		narrow := (0.75 - profile.coverage) / 0.75
		base -= 0.04 * narrow
		check -= 0.03 * narrow
		rangeMul -= 0.06 * narrow
		prefixMul -= 0.08 * narrow
	}

	if orderSkew > 3.5 {
		skewAdj := ClampFloat((orderSkew-3.5)*0.015, 0, 0.03)
		check += skewAdj
		rangeMul += skewAdj * 0.40
		prefixMul += skewAdj * 0.50
	}

	base = ClampFloat(base, 0.72, 1.20)
	check = ClampFloat(check, 0.12, 0.45)
	rangeMul = ClampFloat(rangeMul, 0.62, 0.96)
	prefixMul = ClampFloat(prefixMul, 0.50, 0.90)

	return base, check, rangeMul, prefixMul
}

func plannerOrderedFallbackProbeFactor(orderSkew float64, profile plannerOrderedProfile, offset uint64) float64 {
	base := plannerOrderedFallbackProbeBase
	skewMul := plannerOrderedFallbackProbeSkewMul

	if profile.coverage > 0 && profile.coverage < 0.70 {
		narrow := (0.70 - profile.coverage) / 0.70
		base -= 0.06 * narrow
		skewMul -= 0.015 * narrow
	}

	if profile.hasPrefix && profile.coverage < 0.50 {
		base -= 0.02
	}
	if profile.hasNeg {
		base += 0.04
	}
	if offset > 0 {
		base -= 0.02
		skewMul -= 0.005
	}
	if orderSkew >= 5.0 {
		skewMul += 0.02
	}

	base = ClampFloat(base, 0.55, 1.05)
	skewMul = ClampFloat(skewMul, 0.06, 0.30)

	f := base + skewMul*orderSkew
	return ClampFloat(f, 0.65, 2.0)
}

type plannerOrderedProfile struct {
	selectivity      float64
	coverage         float64
	activeChecks     int
	hasNeg           bool
	hasPrefix        bool
	fallbackWorkRows float64
	orderRangeLeaves int
	baseWorkRows     float64
	baseRangeLeaves  int
	basePrefixLeaves int
}

type plannerOrderedDecision struct {
	use bool

	expectedProbeRows uint64
	orderedCost       float64
	fallbackCost      float64
}

type plannerExecutionOrderDecision struct {
	use bool

	plan PlanName

	expectedProbeRows uint64
	executionCost     float64
}

type mergedBaseScalarRangeField struct {
	field  string
	bounds indexdata.Bounds
}

func (qv *View) collectMergedBaseScalarRangeFields(
	orderField string,
	leaves []qir.Expr,
	dst []mergedBaseScalarRangeField,
) ([]mergedBaseScalarRangeField, bool) {

	for _, e := range leaves {
		fieldName := qv.exec.FieldNameByOrdinal(e.FieldOrdinal)
		if e.Not || e.FieldOrdinal < 0 || fieldName == orderField || len(e.Operands) != 0 || !isScalarRangeEqOp(e.Op) {
			continue
		}
		fm := qv.fieldMetaByExpr(e)
		if fm == nil || fm.Slice {
			continue
		}
		nextRB, ok, err := qv.rangeBoundsForScalarExpr(e)
		if err != nil || !ok {
			return nil, false
		}
		merged := false
		for i := range dst {
			if dst[i].field != fieldName {
				continue
			}
			mergeRangeBounds(&dst[i].bounds, nextRB)
			merged = true
			break
		}
		if merged {
			continue
		}
		dst = append(dst, mergedBaseScalarRangeField{
			field:  fieldName,
			bounds: nextRB,
		})
	}
	return dst, true
}

func (qv *View) estimateMergedScalarRangeOrderCost(
	field string,
	bounds indexdata.Bounds,
	universe uint64,
	stats PlannerFieldStats,
) (selectivity float64, fallbackWork float64, ok bool) {

	if field == "" {
		return 0, 0, false
	}
	if qv.exec.Schema.Fields[field] == nil {
		return 0, 0, false
	}
	if !qv.fieldIndexViewFromSlotsByName(qv.snap.Index, field).HasData() {
		return 0, 0, false
	}
	if bounds.Empty {
		return 0, 0, true
	}
	if !bounds.HasLo && !bounds.HasHi && !bounds.HasPrefix {
		return 1, 0, true
	}

	rawSel := 0.0
	ov := qv.fieldIndexViewFromSlotsByName(qv.snap.Index, field)
	br := ov.RangeForBounds(bounds)
	if br.BaseStart >= br.BaseEnd {
		return 0, 0, true
	}

	inBuckets, estCard := ov.RangeStats(br)
	if inBuckets == 0 {
		return 0, 0, true
	}
	if estCard == 0 {
		avg := stats.AvgBucketCard
		if avg <= 0 {
			if stats.DistinctKeys > 0 {
				avg = float64(universe) / float64(stats.DistinctKeys)
			} else {
				avg = float64(universe) / float64(inBuckets)
			}
		}
		estCard = uint64(avg * float64(inBuckets))
	}
	if estCard > universe {
		estCard = universe
	}
	rawSel = float64(estCard) / float64(universe)
	if rawSel < 0 {
		rawSel = 0
	}
	if rawSel > 1 {
		rawSel = 1
	}

	selectivity = rawSel
	fallbackWork = rawSel * float64(universe) * 1.05

	return selectivity, fallbackWork, true
}

func (qv *View) executionOrderShapeInfo(orderField string, leaves []qir.Expr) (hasOrderPrefix bool, compatible bool) {
	for _, e := range leaves {
		if e.Op == qir.OpNOOP {
			if e.Not {
				return false, false
			}
			continue
		}
		if e.Not {
			fieldName := qv.exec.FieldNameByOrdinal(e.FieldOrdinal)
			if e.FieldOrdinal < 0 || fieldName == orderField {
				return false, false
			}
			continue
		}
		if e.FieldOrdinal < 0 {
			return false, false
		}
		switch qv.classifyOrderFieldScalarLeaf(orderField, e) {
		case orderFieldScalarLeafOther:
			continue
		case orderFieldScalarLeafRange:
		case orderFieldScalarLeafPrefix:
			hasOrderPrefix = true
		case orderFieldScalarLeafInvalid:
			return false, false
		}
	}
	return hasOrderPrefix, true
}

func (qv *View) decideExecutionOrderByCost(q *qir.Shape, leaves []qir.Expr) plannerExecutionOrderDecision {
	var d plannerExecutionOrderDecision

	if q.Expr.Not {
		return d
	}
	if !q.HasOrder {
		return d
	}
	if q.Limit == 0 {
		return d
	}
	if len(leaves) == 0 {
		return d
	}

	need, ok := orderWindow(q)
	if !ok || need <= 0 {
		return d
	}

	o := q.Order
	if o.Kind != qir.OrderKindBasic || o.FieldOrdinal < 0 {
		return d
	}
	orderField := qv.exec.FieldNameByOrdinal(o.FieldOrdinal)

	fm := qv.fieldMetaByOrder(o)
	if fm == nil || fm.Slice {
		return d
	}

	ov := qv.fieldIndexViewFromSlotsForOrder(qv.snap.Index, o)
	if !ov.HasData() {
		return d
	}

	hasOrderPrefix, compatible := qv.executionOrderShapeInfo(orderField, leaves)
	if !compatible {
		return d
	}

	snap := qv.exec.Stats.Load()
	universe := qv.plannerUniverseCardinality(snap)
	if universe == 0 {
		return d
	}

	var profile plannerOrderedProfile
	orderDistinct := uint64(0)

	profile, ok = qv.estimateOrderedProfileIndexView(orderField, leaves, ov, snap, universe)
	if !ok {
		return d
	}
	orderDistinct = uint64(ov.KeyCount())
	if orderDistinct == 0 {
		return d
	}

	plan := PlanLimitOrderBasic
	if hasOrderPrefix {
		plan = PlanLimitOrderPrefix
	}

	d.use = true
	d.plan = plan

	if profile.selectivity <= 0 {
		d.expectedProbeRows = 0
		d.executionCost = 1.0
		return d
	}

	orderStats := qv.plannerOrderFieldStats(orderField, snap, universe, orderDistinct)
	orderSkew := plannerFieldStatsSkew(orderStats)
	expectedProbeRows := estimateOrderExpectedProbes(float64(need), float64(universe), profile.selectivity, profile.coverage, orderSkew)

	baseFactor, checkFactor, rangeFactor, prefixFactor := plannerExecutionOrderFactors(profile, orderSkew, universe)
	execRowFactor := baseFactor + float64(profile.activeChecks)*checkFactor

	if profile.orderRangeLeaves > 0 {
		execRowFactor *= rangeFactor
	}
	if hasOrderPrefix {
		execRowFactor *= prefixFactor
	}
	if q.Offset > 0 {
		execRowFactor *= 0.95
	}
	execRowFactor = ClampFloat(execRowFactor, 0.25, 6.0)

	executionCost := expectedProbeRows*execRowFactor + float64(len(leaves))*12.0
	if profile.baseWorkRows > 0 {
		baseWorkCost := profile.baseWorkRows * plannerExecutionOrderBaseWorkFactor
		if profile.activeChecks > 1 {
			baseWorkCost *= 1.0 + float64(profile.activeChecks-1)*0.12
		}
		if profile.baseRangeLeaves > 0 {
			baseWorkCost *= 1.0 + float64(profile.baseRangeLeaves)*0.35
		}
		if hasOrderPrefix {
			baseWorkCost *= 1.10
		}
		executionCost += baseWorkCost
	}
	if profile.baseWorkRows > 0 &&
		(profile.baseRangeLeaves > 0 || profile.basePrefixLeaves > 0) &&
		profile.baseWorkRows <= expectedProbeRows*2.0 {
		probeAmp := expectedProbeRows / profile.baseWorkRows
		if probeAmp > 4.0 {
			probeAmp = 4.0
		}
		executionCost *= 1.0 + probeAmp*1.20
	}

	d.expectedProbeRows = uint64(expectedProbeRows)
	d.executionCost = executionCost
	return d
}

func plannerExecutionNoOrderPrefixLeafLimit(qLimit uint64, universe uint64) int {
	limit := plannerExecutionNoOrderPrefixLeafMax
	if qLimit <= 64 {
		limit++
	}
	if qLimit > 384 && limit > 4 {
		limit--
	}
	if universe > 0 && universe >= 2_000_000 && qLimit <= 128 {
		limit++
	}
	if limit < 4 {
		limit = 4
	}
	if limit > 8 {
		limit = 8
	}
	return limit
}

func plannerExecutionNoOrderPrefixLimitForShape(universe uint64, leafCount int) uint64 {
	limit := uint64(plannerExecutionNoOrderPrefixLimitMax)
	switch {
	case leafCount <= 3:
		limit += 64
	case leafCount >= plannerExecutionNoOrderPrefixLeafMax:
		limit = limit * 3 / 4
	}
	if universe > 0 {
		switch {
		case universe >= 4_000_000:
			limit = limit * 3 / 2
		case universe >= 1_000_000:
			limit = limit * 5 / 4
		case universe < 100_000:
			limit = limit * 3 / 4
		}
	}
	if limit < 96 {
		limit = 96
	}
	if limit > 768 {
		limit = 768
	}
	return limit
}

func plannerExecutionNoOrderPrefixBucketLimit(universe uint64, qLimit uint64, restCount int) int {
	limit := plannerExecutionNoOrderPrefixBucketMax
	if qLimit <= 32 {
		limit *= 2
	}
	if restCount >= 3 {
		limit = limit * 3 / 4
	}
	if universe >= 2_000_000 {
		limit = limit * 5 / 4
	}
	if limit < 2_048 {
		limit = 2_048
	}
	if limit > 32<<10 {
		limit = 32 << 10
	}
	return limit
}

func plannerExecutionNoOrderPrefixProbeLimit(universe uint64, qLimit uint64, restCount int) float64 {
	limit := float64(plannerExecutionNoOrderPrefixProbeMax)
	if qLimit <= 32 {
		limit *= 1.10
	}
	if restCount >= 3 {
		limit *= 0.90
	}
	if universe >= 2_000_000 {
		limit *= 1.15
	}
	if limit < 32<<10 {
		limit = 32 << 10
	}
	if limit > 512<<10 {
		limit = 512 << 10
	}
	return limit
}

func plannerExecutionNoOrderPrefixProbeShare(restCount int, restSelectivity float64) float64 {
	share := plannerExecutionNoOrderPrefixProbeShareMax
	if restCount >= 3 {
		share -= 0.07
	} else if restCount == 1 {
		share += 0.08
	}
	if restSelectivity < 0.05 {
		share += 0.08
	} else if restSelectivity > 0.40 {
		share -= 0.08
	}
	return ClampFloat(share, 0.25, 0.65)
}

func (qv *View) shouldPreferExecutionNoOrderPrefix(q *qir.Shape, leaves []qir.Expr) bool {
	if q.Limit == 0 || q.Offset != 0 {
		return false
	}
	universe := qv.snap.Universe.Cardinality()
	maxLeaves := plannerExecutionNoOrderPrefixLeafLimit(q.Limit, universe)
	if len(leaves) < 2 || len(leaves) > maxLeaves {
		return false
	}
	limitMax := plannerExecutionNoOrderPrefixLimitForShape(universe, len(leaves))
	if q.Limit > limitMax {
		return false
	}

	if universe == 0 {
		return true
	}

	prefixCount := 0
	prefixSpan := 0
	prefixRows := uint64(0)
	restSelectivity := 1.0
	restCount := 0

	for _, e := range leaves {
		if e.Op == qir.OpNOOP || e.Not || e.FieldOrdinal < 0 {
			return false
		}

		if isPositiveScalarPrefixLeaf(e) {
			prefix, ok, err := qv.prepareScalarPrefixRoute(e)
			if err != nil || !ok {
				return false
			}
			if !prefix.hasData {
				return true
			}
			if prefix.br.Empty() {
				return true
			}
			span, rows := prefix.ov.RangeStats(prefix.br)
			if span <= 0 {
				return true
			}
			prefixSpan = span
			if rows == 0 {
				return true
			}
			prefixRows = rows

			prefixCount++
			if prefixCount > 1 {
				return false
			}
			continue
		}

		switch e.Op {
		case qir.OpEQ, qir.OpIN, qir.OpHASALL, qir.OpHASANY:
			fm := qv.fieldMetaByExpr(e)
			if fm == nil {
				return false
			}
			sel, _, _, _, ok := qv.estimateLeafOrderCost(e, nil, universe, "", false)
			if !ok {
				return false
			}
			if sel < 0 {
				sel = 0
			}
			if sel > 1 {
				sel = 1
			}
			restSelectivity *= sel
			restCount++

		default:
			return false
		}
	}

	if prefixCount != 1 || restCount == 0 {
		return false
	}

	if restSelectivity <= 0 {
		// contradictory rest predicates should not force no-order prefix scan:
		// planner/bitmap paths can short-circuit them without walking prefix span
		return false
	}

	bucketLimit := plannerExecutionNoOrderPrefixBucketLimit(universe, q.Limit, restCount)
	if prefixSpan <= bucketLimit {
		return true
	}

	need := float64(q.Limit)
	expectedProbe := need / restSelectivity
	if expectedProbe < need {
		expectedProbe = need
	}
	if expectedProbe > float64(prefixRows) {
		expectedProbe = float64(prefixRows)
	}
	probeLimit := plannerExecutionNoOrderPrefixProbeLimit(universe, q.Limit, restCount)
	if expectedProbe > probeLimit {
		return false
	}
	probeShare := plannerExecutionNoOrderPrefixProbeShare(restCount, restSelectivity)

	return expectedProbe <= float64(prefixRows)*probeShare
}

func (qv *View) decideOrderedByCost(q *qir.Shape, leaves []qir.Expr) plannerOrderedDecision {
	var d plannerOrderedDecision

	if !q.HasOrder {
		return d
	}
	if q.Limit == 0 {
		return d
	}
	if len(leaves) > plannerOrderedLeafMax {
		return d
	}

	need := q.Offset + q.Limit
	if need < q.Offset || need > plannerOrderedNeedHardMax {
		return d
	}

	o := q.Order
	if o.Kind != qir.OrderKindBasic {
		return d
	}
	orderField := qv.exec.FieldNameByOrdinal(o.FieldOrdinal)

	fm := qv.fieldMetaByOrder(o)
	if fm == nil || fm.Slice {
		return d
	}
	ov := qv.fieldIndexViewFromSlotsForOrder(qv.snap.Index, o)
	if !ov.HasData() {
		return d
	}

	snap := qv.exec.Stats.Load()
	universe := qv.plannerUniverseCardinality(snap)
	if universe == 0 {
		return d
	}

	var profile plannerOrderedProfile
	var ok bool

	profile, ok = qv.estimateOrderedProfileIndexView(orderField, leaves, ov, snap, universe)
	if !ok {
		return d
	}

	orderDistinct := uint64(ov.KeyCount())
	if orderDistinct == 0 {
		return d
	}

	prefixNonOrderSel := 1.0
	hasPrefixNonOrder := false

	for i := range leaves {
		e := leaves[i]
		if !qv.isPositiveNonOrderScalarPrefixLeaf(orderField, e) {
			continue
		}
		leafSel, _, _, _, ok := qv.estimateLeafOrderCost(e, snap, universe, orderField, orderDistinct > 0)
		if !ok {
			continue
		}
		hasPrefixNonOrder = true
		if leafSel < prefixNonOrderSel {
			prefixNonOrderSel = leafSel
		}
	}

	// keep baseline fast paths for trivial unpaged positive predicates
	if !profile.hasNeg && q.Offset == 0 && profile.orderRangeLeaves == 0 {
		return d
	}

	// prefix+negation is usually risky for ordered probing when prefix is narrow
	// on a non-order field; broad prefixes can still benefit from ordered scan
	if profile.hasPrefix && profile.hasNeg && q.Offset == 0 {
		if q.Limit > 512 || (hasPrefixNonOrder && prefixNonOrderSel < 0.35) {
			return d
		}
	}

	if profile.selectivity <= 0 {
		// impossible conjunction, ordered strategy can short-circuit after predicate build
		d.use = true
		return d
	}

	orderStats := qv.plannerOrderFieldStats(orderField, snap, universe, orderDistinct)
	orderSkew := plannerFieldStatsSkew(orderStats)

	// probe estimate is the key bridge between logical selectivity and physical
	// order-scan work; all later cost terms are derived from this value
	expectedProbeRows := estimateOrderExpectedProbes(float64(need), float64(universe), profile.selectivity, profile.coverage, orderSkew)

	orderedRowFactor := 1.0 + float64(profile.activeChecks)*0.82
	if profile.hasNeg {
		orderedRowFactor += 0.45
	}
	if profile.hasPrefix {
		orderedRowFactor += 0.25
	}

	// with non-zero offset ordered strategy can skip whole buckets using exact bucket counts
	if q.Offset > 0 {
		orderedRowFactor *= 0.84
	}

	// when order field is tightly bounded, ordered strategy checks fewer buckets
	if profile.orderRangeLeaves > 0 {
		orderedRowFactor *= 0.90
	}

	orderedCost := expectedProbeRows*orderedRowFactor + float64(len(leaves))*32.0
	orderedCost += qv.estimateOrderedAnchorSetupCost(
		orderField,
		leaves,
		snap,
		universe,
		orderDistinct,
		profile.coverage,
		int(need),
	)

	fallbackProbeFactor := plannerOrderedFallbackProbeFactor(orderSkew, profile, q.Offset)
	fallbackCost := profile.fallbackWorkRows + expectedProbeRows*fallbackProbeFactor + float64(len(leaves))*20.0

	// execution-plan fallback is considered here so ordered planner does not
	// preempt a cheaper execution fast-path
	execDecision := qv.decideExecutionOrderByCost(q, leaves)
	if execDecision.use && execDecision.executionCost > 0 && execDecision.executionCost < fallbackCost {
		fallbackCost = execDecision.executionCost
	}

	gainReq := plannerOrderedCostGainDefault
	if !profile.hasNeg && q.Offset == 0 {
		gainReq = plannerOrderedCostGainNoNeg
	}
	if q.Offset > 0 {
		// deep offset is a common baseline-path pain point; accept near-tie
		gainReq = 1.02
	}

	d.expectedProbeRows = uint64(expectedProbeRows)
	d.orderedCost = orderedCost
	d.fallbackCost = fallbackCost
	d.use = orderedCost <= fallbackCost*gainReq
	return d
}

func (qv *View) estimateOrderedAnchorSetupCost(
	orderField string,
	leaves []qir.Expr,
	snap *PlannerStatsSnapshot,
	universe, orderDistinct uint64,
	coverage float64,
	needWindow int,
) float64 {
	if needWindow <= 0 || universe == 0 || orderDistinct == 0 {
		return 0
	}

	orderSpanDistinct := uint64(math.Ceil(coverage * float64(orderDistinct)))
	if orderSpanDistinct < plannerOrderedAnchorSpanMin {
		return 0
	}

	orderHasBuckets := orderDistinct > 0
	activeCount := 0
	activeRangeLikeCount := 0
	leadFound := false
	leadScore := 0.0
	leadEst := uint64(0)
	leadRangeLike := false

	var mergedBaseRangesArr [plannerOrderedLeafMax]mergedBaseScalarRangeField
	mergedBaseRanges, ok := qv.collectMergedBaseScalarRangeFields(orderField, leaves, mergedBaseRangesArr[:0])
	if !ok {
		return 0
	}

	for _, e := range leaves {
		fieldName := qv.exec.FieldNameByOrdinal(e.FieldOrdinal)
		if e.Not || e.FieldOrdinal < 0 || e.Op == qir.OpNOOP {
			continue
		}
		if fieldName == orderField && len(e.Operands) == 0 && isScalarRangeEqOp(e.Op) {
			continue
		}
		if fieldName != orderField && len(e.Operands) == 0 && isScalarRangeEqOp(e.Op) {
			continue
		}

		leafSel, _, _, _, ok := qv.estimateLeafOrderCost(e, snap, universe, orderField, orderHasBuckets)
		if !ok || leafSel <= 0 {
			continue
		}

		activeCount++
		rangeLike := e.Op.IsScalarRangeOrPrefix()
		if rangeLike {
			activeRangeLikeCount++
		}

		estCard := uint64(leafSel * float64(universe))
		if estCard == 0 {
			estCard = 1
		}
		weight := orderedAnchorLeadOpWeight(e.Op)
		if fieldName == orderField {
			weight *= 2.8
		}
		score := float64(estCard) * weight
		if !leadFound || score < leadScore || (score == leadScore && estCard < leadEst) {
			leadFound = true
			leadScore = score
			leadEst = estCard
			leadRangeLike = rangeLike
		}
	}

	for _, merged := range mergedBaseRanges {
		fieldStats := qv.plannerFieldStats(merged.field, snap, universe)
		leafSel, _, ok := qv.estimateMergedScalarRangeOrderCost(merged.field, merged.bounds, universe, fieldStats)
		if !ok || leafSel <= 0 {
			continue
		}

		activeCount++
		rangeLike := true
		weight := orderedAnchorLeadOpWeight(qir.OpGT)
		if merged.bounds.IsPointRange() {
			rangeLike = false
			weight = orderedAnchorLeadOpWeight(qir.OpEQ)
		}
		if rangeLike {
			activeRangeLikeCount++
		}

		estCard := uint64(leafSel * float64(universe))
		if estCard == 0 {
			estCard = 1
		}
		score := float64(estCard) * weight
		if !leadFound || score < leadScore || (score == leadScore && estCard < leadEst) {
			leadFound = true
			leadScore = score
			leadEst = estCard
			leadRangeLike = rangeLike
		}
	}

	if activeCount < plannerOrderedAnchorMinActive || !leadFound || leadEst == 0 {
		return 0
	}

	if leadRangeLike {
		rangeLeadMax := uint64(max(needWindow*64, 65_536))
		if leadEst > rangeLeadMax {
			return 0
		}
	}

	leadBudget, candidateCap := plannerOrderedAnchorBudgets(needWindow, activeCount, leadEst)
	if orderSpanDistinct >= orderDistinct {
		wideLeadMax := candidateCap * 8
		if wideLeadMax < candidateCap {
			wideLeadMax = ^uint64(0)
		}
		if leadEst > wideLeadMax {
			return 0
		}
	}

	setupRows := leadEst
	if setupRows > leadBudget {
		setupRows = leadBudget
	}
	if setupRows == 0 {
		return 0
	}

	nonLeadCount := activeCount - 1
	nonLeadRangeLikeCount := activeRangeLikeCount
	if leadRangeLike && nonLeadRangeLikeCount > 0 {
		nonLeadRangeLikeCount--
	}

	return float64(setupRows) *
		(1.0 + float64(nonLeadCount)*0.35 + float64(nonLeadRangeLikeCount)*0.25)
}

func (qv *View) estimateOrderedProfileIndexView(orderField string, leaves []qir.Expr, ov indexdata.FieldIndexView, snap *PlannerStatsSnapshot, universe uint64) (plannerOrderedProfile, bool) {
	if universe == 0 {
		return plannerOrderedProfile{}, false
	}

	inBuckets, totalBuckets, coveredBuf, ok := qv.extractOrderRangeCoverageLeavesIndexView(orderField, leaves, ov)
	if !ok {
		return plannerOrderedProfile{}, false
	}
	defer pooled.ReleaseBoolSlice(coveredBuf)

	coverage := 1.0
	if totalBuckets > 0 {
		coverage = float64(inBuckets) / float64(totalBuckets)
		if coverage < 0 {
			coverage = 0
		}
		if coverage > 1 {
			coverage = 1
		}
	}

	selProd := 1.0
	selMin := 1.0
	fallbackWork := 0.0
	activeChecks := 0
	hasNeg := false
	hasPrefix := false
	orderRangeLeaves := 0
	baseWork := 0.0
	baseRangeLeaves := 0
	basePrefixLeaves := 0
	orderHasBuckets := totalBuckets > 0

	var mergedBaseRangesArr [plannerOrderedLeafMax]mergedBaseScalarRangeField
	mergedBaseRanges, ok := qv.collectMergedBaseScalarRangeFields(orderField, leaves, mergedBaseRangesArr[:0])
	if !ok {
		return plannerOrderedProfile{}, false
	}

	for i, e := range leaves {
		if !e.Not && e.FieldOrdinal >= 0 && qv.exec.FieldNameByOrdinal(e.FieldOrdinal) != orderField && len(e.Operands) == 0 && isScalarRangeEqOp(e.Op) {
			continue
		}
		leafSel, leafWork, leafOrderRange, leafHasPrefix, ok := qv.estimateLeafOrderCost(e, snap, universe, orderField, orderHasBuckets)
		if !ok {
			return plannerOrderedProfile{}, false
		}

		if leafSel <= 0 {
			return plannerOrderedProfile{
				selectivity:      0,
				coverage:         coverage,
				activeChecks:     0,
				hasNeg:           hasNeg || e.Not,
				hasPrefix:        hasPrefix || leafHasPrefix,
				fallbackWorkRows: fallbackWork + leafWork,
				orderRangeLeaves: orderRangeLeaves + btoi(leafOrderRange),
				baseWorkRows:     baseWork,
				baseRangeLeaves:  baseRangeLeaves,
				basePrefixLeaves: basePrefixLeaves,
			}, true
		}

		selProd *= leafSel
		if leafSel < selMin {
			selMin = leafSel
		}

		fallbackWork += leafWork
		if e.Not {
			hasNeg = true
		}
		if leafHasPrefix {
			hasPrefix = true
		}
		if leafOrderRange {
			orderRangeLeaves++
		}

		if e.Op == qir.OpNOOP && !e.Not {
			continue
		}
		if coveredBuf[i] {
			continue
		}
		activeChecks++
		baseWork += leafWork
		fieldName := qv.exec.FieldNameByOrdinal(e.FieldOrdinal)
		if e.Op == qir.OpPREFIX && fieldName != orderField {
			basePrefixLeaves++
		} else if isBoundOp(e.Op) && fieldName != orderField {
			baseRangeLeaves++
		}
	}

	for _, merged := range mergedBaseRanges {
		fieldStats := qv.plannerFieldStats(merged.field, snap, universe)
		leafSel, leafWork, ok := qv.estimateMergedScalarRangeOrderCost(merged.field, merged.bounds, universe, fieldStats)
		if !ok {
			return plannerOrderedProfile{}, false
		}
		if leafSel <= 0 {
			return plannerOrderedProfile{
				selectivity:      0,
				coverage:         coverage,
				activeChecks:     0,
				hasNeg:           hasNeg,
				hasPrefix:        hasPrefix,
				fallbackWorkRows: fallbackWork + leafWork,
				orderRangeLeaves: orderRangeLeaves,
				baseWorkRows:     baseWork,
				baseRangeLeaves:  baseRangeLeaves,
				basePrefixLeaves: basePrefixLeaves,
			}, true
		}
		selProd *= leafSel
		if leafSel < selMin {
			selMin = leafSel
		}
		fallbackWork += leafWork
		activeChecks++
		baseWork += leafWork
		if merged.bounds.HasPrefix {
			hasPrefix = true
			basePrefixLeaves++
		} else {
			baseRangeLeaves++
		}
	}

	selectivity := selProd
	if selectivity > selMin {
		selectivity = selMin
	}

	minSel := selMin * 0.06
	universeFloor := 1.0 / float64(universe)
	if minSel < universeFloor {
		minSel = universeFloor
	}
	if selectivity < minSel {
		selectivity = minSel
	}
	if selectivity > 1 {
		selectivity = 1
	}

	return plannerOrderedProfile{
		selectivity:      selectivity,
		coverage:         coverage,
		activeChecks:     activeChecks,
		hasNeg:           hasNeg,
		hasPrefix:        hasPrefix,
		fallbackWorkRows: fallbackWork,
		orderRangeLeaves: orderRangeLeaves,
		baseWorkRows:     baseWork,
		baseRangeLeaves:  baseRangeLeaves,
		basePrefixLeaves: basePrefixLeaves,
	}, true
}

func (qv *View) extractOrderRangeCoverageLeavesIndexView(orderField string, leaves []qir.Expr, ov indexdata.FieldIndexView) (int, int, []bool, bool) {
	rb := indexdata.Bounds{}
	coveredBuf := pooled.GetBoolSlice(len(leaves))[:len(leaves)]
	clear(coveredBuf)

	has := false
	for i := range leaves {
		e := leaves[i]
		if e.Not || qv.exec.FieldNameByOrdinal(e.FieldOrdinal) != orderField {
			continue
		}
		if applied, ok := qv.applyScalarExprToRangeBounds(e, &rb); !ok {
			pooled.ReleaseBoolSlice(coveredBuf)
			return 0, 0, nil, false
		} else if applied {
			coveredBuf[i] = true
			has = true
		}
	}

	total := ov.KeyCount()
	if total == 0 {
		return 0, 0, coveredBuf, true
	}

	if !has {
		return total, total, coveredBuf, true
	}

	br := ov.RangeForBounds(rb)
	in := br.BaseEnd - br.BaseStart
	if in < 0 {
		in = 0
	}
	if in > total {
		in = total
	}
	return in, total, coveredBuf, true
}

func (qv *View) estimateLeafOrderCost(
	e qir.Expr,
	snap *PlannerStatsSnapshot,
	universe uint64,
	orderField string,
	orderHasBuckets bool,
) (
	selectivity float64, fallbackWork float64, orderRange bool, hasPrefix bool, ok bool) {

	if e.Op == qir.OpNOOP {
		if e.Not {
			return 0, 0, false, false, true
		}
		return 1, 0, false, false, true
	}

	if e.FieldOrdinal < 0 {
		return 0, 0, false, false, false
	}
	if qv.fieldMetaByExpr(e) == nil {
		return 0, 0, false, false, false
	}
	if !qv.fieldIndexViewFromSlotsForExpr(qv.snap.Index, e).HasData() {
		return 0, 0, false, false, false
	}

	fieldName := qv.exec.FieldNameByOrdinal(e.FieldOrdinal)
	fieldStats := qv.plannerFieldStats(fieldName, snap, universe)
	rawSel := 0.0
	valueCount := 1

	switch e.Op {
	case qir.OpEQ:
		rawSel, ok = qv.estimateEqSelectivity(fieldName, e.Value, universe, fieldStats)
	case qir.OpIN:
		rawSel, valueCount, ok = qv.estimateInLikeSelectivity(fieldName, e.Value, universe, false)
	case qir.OpHASANY:
		rawSel, valueCount, ok = qv.estimateInLikeSelectivity(fieldName, e.Value, universe, false)
	case qir.OpHASALL:
		rawSel, valueCount, ok = qv.estimateInLikeSelectivity(fieldName, e.Value, universe, true)

	default:
		if !isSimpleScalarRangeOrPrefixLeaf(e) {
			return 0, 0, false, false, false
		}
		rawSel, ok = qv.estimateRangeSelectivity(e, universe, fieldStats)
		switch qv.classifyOrderFieldScalarLeaf(orderField, e) {
		case orderFieldScalarLeafRange:
			orderRange = true
		case orderFieldScalarLeafPrefix:
			orderRange = true
			hasPrefix = true
		}
	}

	if !ok {
		return 0, 0, false, false, false
	}

	if rawSel < 0 {
		rawSel = 0
	}
	if rawSel > 1 {
		rawSel = 1
	}

	selectivity = rawSel
	if e.Not {
		selectivity = 1 - rawSel
		if selectivity < 0 {
			selectivity = 0
		}
	}

	fallbackWork = rawSel * float64(universe)
	switch e.Op {
	case qir.OpIN, qir.OpHASANY:
		fallbackWork *= 1.0 + float64(max(valueCount-1, 0))*0.35
	case qir.OpHASALL:
		fallbackWork *= 1.0 + float64(max(valueCount-1, 0))*0.55
	case qir.OpPREFIX:
		fallbackWork *= 1.20
	default:
		if e.Op.IsNumericRange() {
			fallbackWork *= 1.05
		}
	}
	if e.Not {
		fallbackWork += 0.08 * float64(universe)
	}

	// range-only order queries benefit from fast baseline scans when unpaged
	orderLeafKind := qv.classifyOrderFieldScalarLeaf(orderField, e)
	if orderLeafKind == orderFieldScalarLeafRange || orderLeafKind == orderFieldScalarLeafPrefix {
		if orderHasBuckets {
			fallbackWork *= 0.92
		}
	}

	return selectivity, fallbackWork, orderRange, hasPrefix, true
}

func (qv *View) estimateEqSelectivity(field string, value any, universe uint64, stats PlannerFieldStats) (float64, bool) {
	key, isSlice, isNil, err := qv.exprValueToLookupKey(qir.Expr{
		Op:           qir.OpEQ,
		FieldOrdinal: qv.fieldOrdinalByName(field),
		Value:        value,
	})
	if err != nil || isSlice {
		return 0, false
	}

	if isNil {
		card := qv.fieldIndexViewFromSlotsByName(qv.snap.NilIndex, field).LookupCardinality(indexdata.NilIndexEntryKey)
		if card == 0 {
			return 0, true
		}
		return float64(card) / float64(universe), true
	}
	ov := qv.fieldIndexViewFromSlotsByName(qv.snap.Index, field)
	if ov.HasData() {
		card := lookupScalarCardinality(ov, key)
		if card == 0 {
			return 0, true
		}
		return float64(card) / float64(universe), true
	}

	avg := stats.AvgBucketCard
	if avg <= 0 {
		if stats.DistinctKeys == 0 {
			return 1.0 / float64(universe), true
		}
		avg = float64(universe) / float64(stats.DistinctKeys)
	}
	sel := avg / float64(universe)
	if sel < 0 {
		sel = 0
	}
	if sel > 1 {
		sel = 1
	}
	return sel, true
}

func (qv *View) estimateInLikeSelectivity(field string, value any, universe uint64, intersect bool) (float64, int, bool) {
	valsBuf, isSlice, hasNil, err := qv.exprValueToDistinctIdxBuf(qir.Expr{
		Op:           qir.OpIN,
		FieldOrdinal: qv.fieldOrdinalByName(field),
		Value:        value,
	})
	if valsBuf != nil {
		defer pooled.ReleaseStringSlice(valsBuf)
	}

	valueCount := len(valsBuf)
	if err != nil || !isSlice || (valueCount == 0 && !hasNil) {
		return 0, 0, false
	}

	sum := uint64(0)
	minCard := uint64(0)

	ov := qv.fieldIndexViewFromSlotsByName(qv.snap.Index, field)
	if !ov.HasData() {
		if !hasNil {
			return 0, 0, false
		}
	}

	for i := 0; i < valueCount; i++ {
		card := ov.LookupCardinality(valsBuf[i])
		sum += card
		if minCard == 0 || card < minCard {
			minCard = card
		}
		if intersect && card == 0 {
			return 0, valueCount, true
		}
	}

	if fm := qv.exec.Schema.Fields[field]; hasNil && fm != nil && !fm.Slice {
		card := qv.fieldIndexViewFromSlotsByName(qv.snap.NilIndex, field).LookupCardinality(indexdata.NilIndexEntryKey)
		sum += card
		if minCard == 0 || card < minCard {
			minCard = card
		}
		valueCount++
		if intersect && card == 0 {
			return 0, valueCount, true
		}
	}

	if !intersect {
		sel := float64(sum) / float64(universe)
		if sel > 1 {
			sel = 1
		}
		return sel, valueCount, true
	}

	sel := float64(minCard) / float64(universe)
	if valueCount > 1 {
		decay := 1.0
		for i := 1; i < valueCount; i++ {
			decay *= 0.7
		}
		sel *= decay
	}
	if sel < 0 {
		sel = 0
	}
	if sel > 1 {
		sel = 1
	}
	return sel, valueCount, true
}

func (qv *View) estimateRangeSelectivity(e qir.Expr, universe uint64, stats PlannerFieldStats) (float64, bool) {
	rb, ok, err := qv.rangeBoundsForScalarExpr(e)
	if err != nil || !ok {
		return 0, false
	}
	if rb.Empty {
		return 0, true
	}
	if !rb.HasLo && !rb.HasHi && !rb.HasPrefix {
		return 1, true
	}

	ov := qv.fieldIndexViewFromSlotsForExpr(qv.snap.Index, e)
	if ov.HasData() {
		br := ov.RangeForBounds(rb)
		if br.BaseStart >= br.BaseEnd {
			return 0, true
		}

		inBuckets, estCard := ov.RangeStats(br)
		if inBuckets == 0 {
			return 0, true
		}

		if estCard == 0 {
			avg := stats.AvgBucketCard
			if avg <= 0 {
				if stats.DistinctKeys > 0 {
					avg = float64(universe) / float64(stats.DistinctKeys)
				} else {
					avg = float64(universe) / float64(inBuckets)
				}
			}
			estCard = uint64(avg * float64(inBuckets))
		}

		if estCard > universe {
			estCard = universe
		}
		return float64(estCard) / float64(universe), true
	}
	return 0, false
}

func (qv *View) plannerFieldStats(field string, snap *PlannerStatsSnapshot, universe uint64) PlannerFieldStats {
	distinct := uint64(qv.fieldIndexViewFromSlotsByName(qv.snap.Index, field).KeyCount())
	return qv.plannerOrderFieldStats(field, snap, universe, distinct)
}

func estimateOrderExpectedProbes(need, universe, selectivity, coverage, skew float64) float64 {
	if need <= 0 || universe <= 0 {
		return 0
	}
	if selectivity <= 0 {
		return universe
	}

	bySel := need / selectivity
	if bySel < need {
		bySel = need
	}

	coverageRows := universe
	if coverage > 0 && coverage < 1 {
		coverageRows = universe * coverage
	}
	coverageRows *= 1.0 + (skew-1.0)*0.35
	if coverageRows < need {
		coverageRows = need
	}

	probes := bySel
	if coverageRows < probes {
		probes = coverageRows
	}
	if probes > universe {
		probes = universe
	}
	return probes
}

func btoi(v bool) int {
	if v {
		return 1
	}
	return 0
}
