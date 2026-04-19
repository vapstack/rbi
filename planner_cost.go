package rbi

import (
	"math"

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

type plannerOROrderPlan int

const (
	plannerOROrderFallback plannerOROrderPlan = iota
	plannerOROrderMerge
	plannerOROrderStream
)

type plannerORNoOrderDecision struct {
	use          bool
	expectedRows uint64
	kWayCost     float64
	fallbackCost float64
}

type plannerOROrderDecision struct {
	plan         plannerOROrderPlan
	expectedRows uint64
	bestCost     float64
	fallbackCost float64
}

func (qv *queryView[K, V]) decidePlanORNoOrder(q *qir.Shape, branches plannerORBranches) plannerORNoOrderDecision {
	var d plannerORNoOrderDecision

	if q.HasOrder {
		return d
	}
	need, ok := orderWindow(q)
	if !ok || need <= 0 {
		return d
	}

	for i := 0; i < branches.Len(); i++ {
		branch := branches.Get(i)
		if branch.alwaysTrue {
			d.use = true
			d.expectedRows = uint64(need)
			return d
		}
		lead := branch.leadPtr()
		if lead == nil || !lead.hasIter() {
			return d
		}
	}

	snap := qv.planner.stats.Load()
	universe := snap.universeOr(qv.snapshotUniverseCardinality())
	if universe == 0 {
		return d
	}

	var branchCards [plannerORBranchLimit]uint64
	unionCard, sumCard, branchCount, hasAlwaysTrue := branches.unionCards(universe, &branchCards)
	if hasAlwaysTrue {
		d.use = true
		d.expectedRows = uint64(need)
		return d
	}
	if unionCard == 0 {
		return d
	}

	// expectedRows models how many ids we need to inspect before collecting N
	// unique outputs from a union with the given cardinality.
	expectedRows := estimateRowsForNeed(uint64(need), unionCard, universe)
	avgChecks := branches.noOrderChecks(&branchCards, branchCount)
	costKWay := float64(expectedRows) * (1.0 + avgChecks)
	costKWay *= qv.root.plannerCostMultiplier(plannerCalORNoOrder)

	fallbackRows := min(unionCard, uint64(need))
	costFallback := float64(sumCard) + float64(fallbackRows)

	d.expectedRows = expectedRows
	d.kWayCost = costKWay
	d.fallbackCost = costFallback

	// Require advantage margin to avoid route flapping on close/noisy estimates.
	d.use = costKWay <= costFallback*plannerORNoOrderAdvantage

	return d
}

func (qv *queryView[K, V]) hasNonOrderPrefixTailRisk(branches plannerORBranches, orderField string) bool {
	for i := 0; i < branches.Len(); i++ {
		branch := branches.Get(i)
		for pi := 0; pi < branch.predLen(); pi++ {
			p := branch.pred(pi)
			if qv.isPositiveNonOrderScalarPrefixLeaf(orderField, p.expr) && !p.isMaterializedLike() {
				return true
			}
		}
	}
	return false
}

func (qv *queryView[K, V]) decidePlanOROrder(q *qir.Shape, branches plannerORBranches) plannerOROrderDecision {
	d := plannerOROrderDecision{plan: plannerOROrderFallback}

	if !q.HasOrder {
		return d
	}

	o := q.Order
	if o.Kind != qir.OrderKindBasic {
		return d
	}
	orderField := qv.fieldNameByOrder(o)

	fm := qv.fieldMetaByOrder(o)
	if fm == nil || fm.Slice {
		return d
	}

	ov := qv.fieldOverlayForOrder(o)
	if !ov.hasData() {
		return d
	}
	orderDistinct := uint64(overlayApproxDistinctTotalCount(ov))
	if orderDistinct == 0 {
		return d
	}
	need, ok := orderWindow(q)
	if !ok || need <= 0 {
		return d
	}

	snap := qv.planner.stats.Load()
	universe := snap.universeOr(qv.snapshotUniverseCardinality())
	if universe == 0 {
		return d
	}

	mergeStats := qv.orderMergeBranchStats(orderField, branches, ov)
	var branchCards [plannerORBranchLimit]uint64
	unionCard, sumCard, branchCount, hasAlwaysTrue := branches.unionCards(universe, &branchCards)
	if unionCard == 0 {
		return d
	}

	orderStats := qv.plannerOrderFieldStats(orderField, snap, universe, orderDistinct)
	expectedRows := estimateRowsForNeed(uint64(need), unionCard, universe)
	routeCost, routeOK := qv.estimateOROrderMergeRouteCost(q, branches, need, mergeStats)

	// Stream cost tracks row-by-row predicate checks during ordered scan.
	streamChecks := branches.orderStreamChecksByBranch(hasAlwaysTrue, mergeStats)
	streamSkew := orderStats.skew()
	costStream := float64(expectedRows) * streamChecks * streamSkew
	costStream *= qv.root.plannerCostMultiplier(plannerCalOROrderStream)

	costFallback := float64(sumCard) + float64(expectedRows)
	bestPlan := plannerOROrderFallback
	bestCost := costFallback

	// Merge strategy only makes sense when no branch is tautological and the
	// requested window is bounded enough to amortize branch merge overhead.
	mergeNeedLimit := plannerOROrderMergeNeedLimit(need, branches.Len(), unionCard, sumCard, q.Offset)
	mergeAllowed := !hasAlwaysTrue && need <= mergeNeedLimit
	if mergeAllowed {
		costMerge := branches.orderMergeCost(uint64(need), &branchCards, branchCount, unionCard, sumCard, universe, orderStats, mergeStats)
		if routeOK && routeCost.kWay > 0 && routeCost.kWay < costMerge &&
			branches.hasFullSpanOrderBranch(mergeStats) &&
			!routeCost.hasPrefixTailRisk &&
			!branches.hasKWayExactBucketApplyWork(mergeStats) {
			costMerge = routeCost.kWay
		}
		costMerge *= qv.root.plannerCostMultiplier(plannerCalOROrderMerge)
		if costMerge <= bestCost*plannerOROrderMergeGain {
			bestPlan = plannerOROrderMerge
			bestCost = costMerge
		}
	}

	if costStream <= bestCost*plannerOROrderStreamGain {
		bestPlan = plannerOROrderStream
		bestCost = costStream
	}

	d.plan = bestPlan
	d.expectedRows = expectedRows
	d.bestCost = bestCost
	d.fallbackCost = costFallback
	return d
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
		branch := branches.Get(i)
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

func (branch plannerORBranch) estimatedCard(universe uint64) uint64 {
	if universe == 0 {
		return 0
	}
	if branch.alwaysTrue {
		return universe
	}
	if branch.estKnown {
		if branch.estCard > universe {
			return universe
		}
		return branch.estCard
	}

	minEst := uint64(0)
	active := 0
	for i := 0; i < branch.predLen(); i++ {
		p := branch.pred(i)
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
		if lead := branch.leadPtr(); lead != nil && lead.estCard > 0 {
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
		branch := branches.Get(i)
		weight := float64(1)
		if branchCards != nil && branchCards[i] > 0 {
			weight = float64(branchCards[i])
		}

		checks := 0
		for pi := 0; pi < branch.predLen(); pi++ {
			if pi == branch.leadIdx {
				continue
			}
			p := branch.pred(pi)
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
	unionCard uint64,
	sumCard uint64,
	universe uint64,
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

	rankRows := orderStats.mergeRankRows(candidateUpper, need, universe)
	return subRows + float64(rankRows)
}

func (stats PlannerFieldStats) mergeRankRows(candidateUpper, need, universe uint64) uint64 {
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

func (qv *queryView[K, V]) plannerOrderFieldStats(field string, snap *plannerStatsSnapshot, universe uint64, distinctFallback uint64) PlannerFieldStats {
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

func (stats PlannerFieldStats) skew() float64 {
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
		// Keep adaptation conservative: only narrow windows should move factors.
		narrow := (0.75 - profile.coverage) / 0.75
		base -= 0.04 * narrow
		check -= 0.03 * narrow
		rangeMul -= 0.06 * narrow
		prefixMul -= 0.08 * narrow
	}

	if orderSkew > 3.5 {
		skewAdj := plannerClampFloat((orderSkew-3.5)*0.015, 0, 0.03)
		check += skewAdj
		rangeMul += skewAdj * 0.40
		prefixMul += skewAdj * 0.50
	}

	base = plannerClampFloat(base, 0.72, 1.20)
	check = plannerClampFloat(check, 0.12, 0.45)
	rangeMul = plannerClampFloat(rangeMul, 0.62, 0.96)
	prefixMul = plannerClampFloat(prefixMul, 0.50, 0.90)
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

	base = plannerClampFloat(base, 0.55, 1.05)
	skewMul = plannerClampFloat(skewMul, 0.06, 0.30)

	f := base + skewMul*orderSkew
	return plannerClampFloat(f, 0.65, 2.0)
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
	bounds rangeBounds
}

func (qv *queryView[K, V]) collectMergedBaseScalarRangeFields(
	orderField string,
	leaves []qir.Expr,
	dst []mergedBaseScalarRangeField,
) ([]mergedBaseScalarRangeField, bool) {
	for _, e := range leaves {
		fieldName := qv.fieldNameByExpr(e)
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

func (qv *queryView[K, V]) collectMergedBaseScalarRangeFieldsBuf(
	orderField string,
	leaves *pooled.SliceBuf[qir.Expr],
	dst []mergedBaseScalarRangeField,
) ([]mergedBaseScalarRangeField, bool) {
	for i := 0; i < leaves.Len(); i++ {
		e := leaves.Get(i)
		fieldName := qv.fieldNameByExpr(e)
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
		for j := range dst {
			if dst[j].field != fieldName {
				continue
			}
			mergeRangeBounds(&dst[j].bounds, nextRB)
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

func (qv *queryView[K, V]) estimateMergedScalarRangeOrderCost(
	field string,
	bounds rangeBounds,
	universe uint64,
	stats PlannerFieldStats,
) (selectivity float64, fallbackWork float64, ok bool) {
	if field == "" {
		return 0, 0, false
	}
	if qv.fields[field] == nil {
		return 0, 0, false
	}
	if !qv.fieldOverlay(field).hasData() {
		return 0, 0, false
	}
	if bounds.empty {
		return 0, 0, true
	}
	if !bounds.hasLo && !bounds.hasHi && !bounds.hasPrefix {
		return 1, 0, true
	}

	rawSel := 0.0
	ov := qv.fieldOverlay(field)
	br := ov.rangeForBounds(bounds)
	if br.baseStart >= br.baseEnd {
		return 0, 0, true
	}

	inBuckets, estCard := overlayRangeStats(ov, br)
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

func (qv *queryView[K, V]) executionOrderShapeInfo(orderField string, leaves []qir.Expr) (hasOrderPrefix bool, compatible bool) {
	for _, e := range leaves {
		if e.Op == qir.OpNOOP {
			if e.Not {
				return false, false
			}
			continue
		}
		if e.Not {
			fieldName := qv.fieldNameByExpr(e)
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

func (qv *queryView[K, V]) executionOrderShapeInfoBuf(orderField string, leaves *pooled.SliceBuf[qir.Expr]) (hasOrderPrefix bool, compatible bool) {
	for i := 0; i < leaves.Len(); i++ {
		e := leaves.Get(i)
		if e.Op == qir.OpNOOP {
			if e.Not {
				return false, false
			}
			continue
		}
		if e.Not {
			fieldName := qv.fieldNameByExpr(e)
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

func (qv *queryView[K, V]) decideExecutionOrderByCost(q *qir.Shape, leaves []qir.Expr) plannerExecutionOrderDecision {
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
	orderField := qv.fieldNameByOrder(o)

	fm := qv.fieldMetaByOrder(o)
	if fm == nil || fm.Slice {
		return d
	}

	ov := qv.fieldOverlayForOrder(o)
	useOverlayProfile := ov.hasData()

	var orderSlice *[]index
	if !useOverlayProfile {
		orderSlice = qv.snapshotFieldIndexSliceForOrder(o)
		if orderSlice == nil || len(*orderSlice) == 0 {
			return d
		}
	}

	hasOrderPrefix, compatible := qv.executionOrderShapeInfo(orderField, leaves)
	if !compatible {
		return d
	}

	snap := qv.planner.stats.Load()
	universe := snap.universeOr(qv.snapshotUniverseCardinality())
	if universe == 0 {
		return d
	}

	var profile plannerOrderedProfile
	orderDistinct := uint64(0)

	if useOverlayProfile {
		profile, ok = qv.estimateOrderedProfileOverlay(orderField, leaves, ov, snap, universe)
		if !ok {
			return d
		}
		orderDistinct = uint64(overlayApproxDistinctTotalCount(ov))
		if orderDistinct == 0 {
			return d
		}
	} else {
		profile, ok = qv.estimateOrderedProfile(orderField, leaves, *orderSlice, snap, universe)
		if !ok {
			return d
		}
		orderDistinct = uint64(len(*orderSlice))
	}

	plan := PlanLimitOrderBasic
	calPlan := plannerCalLimitOrderBasic
	if hasOrderPrefix {
		plan = PlanLimitOrderPrefix
		calPlan = plannerCalLimitOrderPrefix
	}

	d.use = true
	d.plan = plan

	if profile.selectivity <= 0 {
		d.expectedProbeRows = 0
		d.executionCost = 1.0 * qv.root.plannerCostMultiplier(calPlan)
		return d
	}

	orderStats := qv.plannerOrderFieldStats(orderField, snap, universe, orderDistinct)
	orderSkew := orderStats.skew()
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
	execRowFactor = plannerClampFloat(execRowFactor, 0.25, 6.0)

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
	executionCost *= qv.root.plannerCostMultiplier(calPlan)

	d.expectedProbeRows = uint64(expectedProbeRows)
	d.executionCost = executionCost
	return d
}

func (qv *queryView[K, V]) decideExecutionOrderByCostBuf(q *qir.Shape, leaves *pooled.SliceBuf[qir.Expr]) plannerExecutionOrderDecision {
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
	if leaves == nil || leaves.Len() == 0 {
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
	orderField := qv.fieldNameByOrder(o)

	fm := qv.fieldMetaByOrder(o)
	if fm == nil || fm.Slice {
		return d
	}

	ov := qv.fieldOverlayForOrder(o)
	useOverlayProfile := ov.hasData()

	var orderSlice *[]index
	if !useOverlayProfile {
		orderSlice = qv.snapshotFieldIndexSliceForOrder(o)
		if orderSlice == nil || len(*orderSlice) == 0 {
			return d
		}
	}

	hasOrderPrefix, compatible := qv.executionOrderShapeInfoBuf(orderField, leaves)
	if !compatible {
		return d
	}

	snap := qv.planner.stats.Load()
	universe := snap.universeOr(qv.snapshotUniverseCardinality())
	if universe == 0 {
		return d
	}

	var profile plannerOrderedProfile
	orderDistinct := uint64(0)

	if useOverlayProfile {
		profile, ok = qv.estimateOrderedProfileOverlayBuf(orderField, leaves, ov, snap, universe)
		if !ok {
			return d
		}
		orderDistinct = uint64(overlayApproxDistinctTotalCount(ov))
		if orderDistinct == 0 {
			return d
		}
	} else {
		profile, ok = qv.estimateOrderedProfileBuf(orderField, leaves, *orderSlice, snap, universe)
		if !ok {
			return d
		}
		orderDistinct = uint64(len(*orderSlice))
	}

	plan := PlanLimitOrderBasic
	calPlan := plannerCalLimitOrderBasic
	if hasOrderPrefix {
		plan = PlanLimitOrderPrefix
		calPlan = plannerCalLimitOrderPrefix
	}

	d.use = true
	d.plan = plan

	if profile.selectivity <= 0 {
		d.expectedProbeRows = 0
		d.executionCost = 1.0 * qv.root.plannerCostMultiplier(calPlan)
		return d
	}

	orderStats := qv.plannerOrderFieldStats(orderField, snap, universe, orderDistinct)
	orderSkew := orderStats.skew()
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
	execRowFactor = plannerClampFloat(execRowFactor, 0.25, 6.0)

	executionCost := expectedProbeRows*execRowFactor + float64(leaves.Len())*12.0
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
	executionCost *= qv.root.plannerCostMultiplier(calPlan)

	d.expectedProbeRows = uint64(expectedProbeRows)
	d.executionCost = executionCost
	return d
}

func (qv *queryView[K, V]) shouldPreferExecutionPlan(q *qir.Shape, trace *queryTrace) bool {
	if plannerObviousExecutionFastPath(q) {
		if trace != nil {
			trace.setEstimated(q.Limit, 1.0, 2.0)
		}
		return true
	}

	var leavesBuf [8]qir.Expr
	leaves, ok := collectAndLeavesScratch(q.Expr, leavesBuf[:0])
	if !ok || len(leaves) == 0 {
		return false
	}

	if !q.HasOrder {
		if qv.shouldPreferExecutionNoOrderPrefix(q, leaves) {
			if trace != nil {
				trace.setEstimated(q.Limit, 1.0, 1.1)
			}
			return true
		}
		return false
	}

	if q.Offset == 0 && q.HasOrder && q.Order.Kind == qir.OrderKindBasic && len(leaves) == 1 {
		e := leaves[0]
		orderField := qv.fieldNameByOrder(q.Order)
		if !e.Not && e.FieldOrdinal >= 0 && qv.fieldNameByExpr(e) != orderField && len(e.Operands) == 0 {
			switch e.Op {
			case qir.OpEQ, qir.OpIN, qir.OpHASALL, qir.OpHASANY:
				return false
			}
		}
	}

	execDecision := qv.decideExecutionOrderByCost(q, leaves)
	if !execDecision.use {
		return false
	}

	if q.Offset == 0 && q.HasOrder && q.Order.Kind == qir.OrderKindBasic {
		orderField := qv.fieldNameByOrder(q.Order)
		if _, ok, err := qv.extractBoundsForField(orderField, leaves); err == nil && ok {
			if qv.supportsLimitLeafPredsExcludingBounds(leaves, orderField) &&
				!qv.hasWarmScalarLimitLeafPredsExcludingBounds(leaves, orderField) {
				if trace != nil {
					trace.setEstimated(execDecision.expectedProbeRows, execDecision.executionCost, execDecision.executionCost*plannerExecutionPreferGain)
				}
				return true
			}
		}
	}

	orderedDecision := qv.decideOrderedByCost(q, leaves)
	if orderedDecision.use && orderedDecision.orderedCost > 0 {
		if execDecision.executionCost >= orderedDecision.orderedCost*plannerExecutionPreferGain {
			return false
		}
	}

	if trace != nil {
		fallbackCost := orderedDecision.orderedCost
		if fallbackCost <= 0 {
			fallbackCost = orderedDecision.fallbackCost
		}
		trace.setEstimated(execDecision.expectedProbeRows, execDecision.executionCost, fallbackCost)
	}

	return true
}

func plannerObviousExecutionFastPath(q *qir.Shape) bool {
	if q == nil || q.Limit == 0 || q.Expr.Not {
		return false
	}

	if !q.HasOrder {
		e := q.Expr
		return isPositiveScalarPrefixLeaf(e)
	}

	if !q.HasOrder {
		return false
	}
	ord := q.Order
	if ord.Kind != qir.OrderKindBasic || ord.FieldOrdinal < 0 {
		return false
	}
	orderFieldOrdinal := ord.FieldOrdinal

	expr := q.Expr
	if expr.Op == qir.OpAND {
		hasPrefix := false
		for i := range expr.Operands {
			op := expr.Operands[i]
			if op.Not {
				return false
			}
			if op.FieldOrdinal < 0 {
				return false
			}
			if op.FieldOrdinal != orderFieldOrdinal {
				continue
			}
			if op.Op == qir.OpPREFIX {
				hasPrefix = true
				continue
			}
			if !isScalarRangeEqOp(op.Op) {
				return false
			}
		}
		return hasPrefix
	}

	return expr.FieldOrdinal == orderFieldOrdinal && expr.Op == qir.OpPREFIX && len(expr.Operands) == 0
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
	return plannerClampFloat(share, 0.25, 0.65)
}

func (qv *queryView[K, V]) shouldPreferExecutionNoOrderPrefix(q *qir.Shape, leaves []qir.Expr) bool {
	if q.Limit == 0 || q.Offset != 0 {
		return false
	}
	universe := qv.snapshotUniverseCardinality()
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
			prefix, ok, err := qv.prepareScalarPrefixSpan(e)
			if err != nil || !ok {
				return false
			}
			if !prefix.hasData {
				return true
			}
			if overlayRangeEmpty(prefix.br) {
				return true
			}
			if prefix.span <= 0 {
				return true
			}
			prefixSpan = prefix.span
			if prefix.rows == 0 {
				return true
			}
			prefixRows = prefix.rows

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
		// Contradictory rest predicates should not force no-order prefix scan:
		// planner/bitmap paths can short-circuit them without walking prefix span.
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

func (qv *queryView[K, V]) decideOrderedByCost(q *qir.Shape, leaves []qir.Expr) plannerOrderedDecision {
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
	orderField := qv.fieldNameByOrder(o)

	fm := qv.fieldMetaByOrder(o)
	if fm == nil || fm.Slice {
		return d
	}
	ov := qv.fieldOverlayForOrder(o)
	useOverlayProfile := ov.hasData()
	var orderSlice *[]index
	if !useOverlayProfile {
		orderSlice = qv.snapshotFieldIndexSliceForOrder(o)
		if orderSlice == nil || len(*orderSlice) == 0 {
			return d
		}
	}

	snap := qv.planner.stats.Load()
	universe := snap.universeOr(qv.snapshotUniverseCardinality())
	if universe == 0 {
		return d
	}

	var profile plannerOrderedProfile
	var ok bool
	orderDistinct := uint64(0)
	if useOverlayProfile {
		profile, ok = qv.estimateOrderedProfileOverlay(orderField, leaves, ov, snap, universe)
		if !ok {
			return d
		}
		orderDistinct = uint64(overlayApproxDistinctTotalCount(ov))
		if orderDistinct == 0 {
			return d
		}
	} else {
		profile, ok = qv.estimateOrderedProfile(orderField, leaves, *orderSlice, snap, universe)
		if !ok {
			return d
		}
		orderDistinct = uint64(len(*orderSlice))
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

	// Prefix+negation is usually risky for ordered probing when prefix is narrow
	// on a non-order field. Broad prefixes can still benefit from ordered scan.
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
	orderSkew := orderStats.skew()

	// Probe estimate is the key bridge between logical selectivity and physical
	// order-scan work; all later cost terms are derived from this value.
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
	orderedCost *= qv.root.plannerCostMultiplier(plannerCalOrdered)

	fallbackProbeFactor := plannerOrderedFallbackProbeFactor(orderSkew, profile, q.Offset)
	fallbackCost := profile.fallbackWorkRows + expectedProbeRows*fallbackProbeFactor + float64(len(leaves))*20.0

	// Execution-plan fallback is considered here so ordered planner does not
	// preempt a cheaper execution fast-path.
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

func (qv *queryView[K, V]) estimateOrderedAnchorSetupCost(
	orderField string,
	leaves []qir.Expr,
	snap *plannerStatsSnapshot,
	universe uint64,
	orderDistinct uint64,
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
		fieldName := qv.fieldNameByExpr(e)
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
		rangeLike := isScalarRangeOrPrefixOp(e.Op)
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
		if !merged.bounds.hasPrefix &&
			merged.bounds.hasLo &&
			merged.bounds.hasHi &&
			merged.bounds.loInc &&
			merged.bounds.hiInc &&
			compareRangeBoundKeys(
				merged.bounds.loKey,
				merged.bounds.loIndex,
				merged.bounds.loNumeric,
				merged.bounds.hiKey,
				merged.bounds.hiIndex,
				merged.bounds.hiNumeric,
			) == 0 {
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

func (qv *queryView[K, V]) estimateOrderedProfile(orderField string, leaves []qir.Expr, orderSlice []index, snap *plannerStatsSnapshot, universe uint64) (plannerOrderedProfile, bool) {
	if universe == 0 {
		return plannerOrderedProfile{}, false
	}

	start, end, coveredBuf, ok := qv.extractOrderRangeCoverageLeaves(orderField, leaves, orderSlice)
	if !ok {
		return plannerOrderedProfile{}, false
	}
	defer boolSlicePool.Put(coveredBuf)

	coverage := 1.0
	if len(orderSlice) > 0 {
		coverage = float64(end-start) / float64(len(orderSlice))
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
	orderHasBuckets := len(orderSlice) > 0
	var mergedBaseRangesArr [plannerOrderedLeafMax]mergedBaseScalarRangeField
	mergedBaseRanges, ok := qv.collectMergedBaseScalarRangeFields(orderField, leaves, mergedBaseRangesArr[:0])
	if !ok {
		return plannerOrderedProfile{}, false
	}

	for i, e := range leaves {
		if !e.Not && e.FieldOrdinal >= 0 && qv.fieldNameByExpr(e) != orderField && len(e.Operands) == 0 && isScalarRangeEqOp(e.Op) {
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
		if coveredBuf.Get(i) {
			continue
		}
		activeChecks++
		baseWork += leafWork
		if isBoundOp(e.Op) && qv.fieldNameByExpr(e) != orderField {
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
			}, true
		}
		selProd *= leafSel
		if leafSel < selMin {
			selMin = leafSel
		}
		fallbackWork += leafWork
		activeChecks++
		baseWork += leafWork
		baseRangeLeaves++
	}

	// AND conjunction cannot be broader than the most selective leaf
	selectivity := selProd
	if selectivity > selMin {
		selectivity = selMin
	}

	// clamp over-aggressive multiplicative collapse for correlated predicates
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
	}, true
}

func (qv *queryView[K, V]) estimateOrderedProfileBuf(orderField string, leaves *pooled.SliceBuf[qir.Expr], orderSlice []index, snap *plannerStatsSnapshot, universe uint64) (plannerOrderedProfile, bool) {
	if universe == 0 {
		return plannerOrderedProfile{}, false
	}

	start, end, coveredBuf, ok := qv.extractOrderRangeCoverageLeavesBuf(orderField, leaves, orderSlice)
	if !ok {
		return plannerOrderedProfile{}, false
	}
	defer boolSlicePool.Put(coveredBuf)

	coverage := 1.0
	if len(orderSlice) > 0 {
		coverage = float64(end-start) / float64(len(orderSlice))
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
	orderHasBuckets := len(orderSlice) > 0
	var mergedBaseRangesArr [plannerOrderedLeafMax]mergedBaseScalarRangeField
	mergedBaseRanges, ok := qv.collectMergedBaseScalarRangeFieldsBuf(orderField, leaves, mergedBaseRangesArr[:0])
	if !ok {
		return plannerOrderedProfile{}, false
	}

	for i := 0; i < leaves.Len(); i++ {
		e := leaves.Get(i)
		if !e.Not && e.FieldOrdinal >= 0 && qv.fieldNameByExpr(e) != orderField && len(e.Operands) == 0 && isScalarRangeEqOp(e.Op) {
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
		if coveredBuf.Get(i) {
			continue
		}
		activeChecks++
		baseWork += leafWork
		if isBoundOp(e.Op) && qv.fieldNameByExpr(e) != orderField {
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
			}, true
		}
		selProd *= leafSel
		if leafSel < selMin {
			selMin = leafSel
		}
		fallbackWork += leafWork
		activeChecks++
		baseWork += leafWork
		baseRangeLeaves++
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
	}, true
}

func (qv *queryView[K, V]) estimateOrderedProfileOverlay(orderField string, leaves []qir.Expr, ov fieldOverlay, snap *plannerStatsSnapshot, universe uint64) (plannerOrderedProfile, bool) {
	if universe == 0 {
		return plannerOrderedProfile{}, false
	}

	inBuckets, totalBuckets, coveredBuf, ok := qv.extractOrderRangeCoverageLeavesOverlay(orderField, leaves, ov)
	if !ok {
		return plannerOrderedProfile{}, false
	}
	defer boolSlicePool.Put(coveredBuf)

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
	orderHasBuckets := totalBuckets > 0
	var mergedBaseRangesArr [plannerOrderedLeafMax]mergedBaseScalarRangeField
	mergedBaseRanges, ok := qv.collectMergedBaseScalarRangeFields(orderField, leaves, mergedBaseRangesArr[:0])
	if !ok {
		return plannerOrderedProfile{}, false
	}

	for i, e := range leaves {
		if !e.Not && e.FieldOrdinal >= 0 && qv.fieldNameByExpr(e) != orderField && len(e.Operands) == 0 && isScalarRangeEqOp(e.Op) {
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
		if coveredBuf.Get(i) {
			continue
		}
		activeChecks++
		baseWork += leafWork
		if isBoundOp(e.Op) && qv.fieldNameByExpr(e) != orderField {
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
			}, true
		}
		selProd *= leafSel
		if leafSel < selMin {
			selMin = leafSel
		}
		fallbackWork += leafWork
		activeChecks++
		baseWork += leafWork
		baseRangeLeaves++
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
	}, true
}

func (qv *queryView[K, V]) estimateOrderedProfileOverlayBuf(orderField string, leaves *pooled.SliceBuf[qir.Expr], ov fieldOverlay, snap *plannerStatsSnapshot, universe uint64) (plannerOrderedProfile, bool) {
	if universe == 0 {
		return plannerOrderedProfile{}, false
	}

	inBuckets, totalBuckets, coveredBuf, ok := qv.extractOrderRangeCoverageLeavesOverlayBuf(orderField, leaves, ov)
	if !ok {
		return plannerOrderedProfile{}, false
	}
	defer boolSlicePool.Put(coveredBuf)

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
	orderHasBuckets := totalBuckets > 0
	var mergedBaseRangesArr [plannerOrderedLeafMax]mergedBaseScalarRangeField
	mergedBaseRanges, ok := qv.collectMergedBaseScalarRangeFieldsBuf(orderField, leaves, mergedBaseRangesArr[:0])
	if !ok {
		return plannerOrderedProfile{}, false
	}

	for i := 0; i < leaves.Len(); i++ {
		e := leaves.Get(i)
		if !e.Not && e.FieldOrdinal >= 0 && qv.fieldNameByExpr(e) != orderField && len(e.Operands) == 0 && isScalarRangeEqOp(e.Op) {
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
		if coveredBuf.Get(i) {
			continue
		}
		activeChecks++
		baseWork += leafWork
		if isBoundOp(e.Op) && qv.fieldNameByExpr(e) != orderField {
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
			}, true
		}
		selProd *= leafSel
		if leafSel < selMin {
			selMin = leafSel
		}
		fallbackWork += leafWork
		activeChecks++
		baseWork += leafWork
		baseRangeLeaves++
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
	}, true
}

func (qv *queryView[K, V]) extractOrderRangeCoverageLeaves(orderField string, leaves []qir.Expr, orderSlice []index) (int, int, *pooled.SliceBuf[bool], bool) {
	rb := rangeBounds{}
	coveredBuf := boolSlicePool.Get()
	coveredBuf.SetLen(len(leaves))

	has := false
	for i := range leaves {
		e := leaves[i]
		if e.Not || qv.fieldNameByExpr(e) != orderField {
			continue
		}
		if applied, ok := qv.applyScalarExprToRangeBounds(e, &rb); !ok {
			boolSlicePool.Put(coveredBuf)
			return 0, 0, nil, false
		} else if applied {
			coveredBuf.Set(i, true)
			has = true
		}
	}

	if !has {
		return 0, len(orderSlice), coveredBuf, true
	}

	st, en := applyBoundsToIndexRange(orderSlice, rb)
	return st, en, coveredBuf, true
}

func (qv *queryView[K, V]) extractOrderRangeCoverageLeavesBuf(orderField string, leaves *pooled.SliceBuf[qir.Expr], orderSlice []index) (int, int, *pooled.SliceBuf[bool], bool) {
	rb := rangeBounds{}
	coveredBuf := boolSlicePool.Get()
	coveredBuf.SetLen(leaves.Len())

	has := false
	for i := 0; i < leaves.Len(); i++ {
		e := leaves.Get(i)
		if e.Not || qv.fieldNameByExpr(e) != orderField {
			continue
		}
		if applied, ok := qv.applyScalarExprToRangeBounds(e, &rb); !ok {
			boolSlicePool.Put(coveredBuf)
			return 0, 0, nil, false
		} else if applied {
			coveredBuf.Set(i, true)
			has = true
		}
	}

	if !has {
		return 0, len(orderSlice), coveredBuf, true
	}

	st, en := applyBoundsToIndexRange(orderSlice, rb)
	return st, en, coveredBuf, true
}

func (qv *queryView[K, V]) extractOrderRangeCoverageLeavesOverlay(orderField string, leaves []qir.Expr, ov fieldOverlay) (int, int, *pooled.SliceBuf[bool], bool) {
	rb := rangeBounds{}
	coveredBuf := boolSlicePool.Get()
	coveredBuf.SetLen(len(leaves))

	has := false
	for i := range leaves {
		e := leaves[i]
		if e.Not || qv.fieldNameByExpr(e) != orderField {
			continue
		}
		if applied, ok := qv.applyScalarExprToRangeBounds(e, &rb); !ok {
			boolSlicePool.Put(coveredBuf)
			return 0, 0, nil, false
		} else if applied {
			coveredBuf.Set(i, true)
			has = true
		}
	}

	total := overlayApproxDistinctTotalCount(ov)
	if total == 0 {
		return 0, 0, coveredBuf, true
	}

	if !has {
		return total, total, coveredBuf, true
	}

	br := ov.rangeForBounds(rb)
	in := overlayApproxDistinctRangeCount(ov, br)
	if in < 0 {
		in = 0
	}
	if in > total {
		in = total
	}
	return in, total, coveredBuf, true
}

func (qv *queryView[K, V]) extractOrderRangeCoverageLeavesOverlayBuf(orderField string, leaves *pooled.SliceBuf[qir.Expr], ov fieldOverlay) (int, int, *pooled.SliceBuf[bool], bool) {
	rb := rangeBounds{}
	coveredBuf := boolSlicePool.Get()
	coveredBuf.SetLen(leaves.Len())

	has := false
	for i := 0; i < leaves.Len(); i++ {
		e := leaves.Get(i)
		if e.Not || qv.fieldNameByExpr(e) != orderField {
			continue
		}
		if applied, ok := qv.applyScalarExprToRangeBounds(e, &rb); !ok {
			boolSlicePool.Put(coveredBuf)
			return 0, 0, nil, false
		} else if applied {
			coveredBuf.Set(i, true)
			has = true
		}
	}

	total := overlayApproxDistinctTotalCount(ov)
	if total == 0 {
		return 0, 0, coveredBuf, true
	}

	if !has {
		return total, total, coveredBuf, true
	}

	br := ov.rangeForBounds(rb)
	in := overlayApproxDistinctRangeCount(ov, br)
	if in < 0 {
		in = 0
	}
	if in > total {
		in = total
	}
	return in, total, coveredBuf, true
}

func overlayApproxDistinctTotalCount(ov fieldOverlay) int {
	return ov.keyCount()
}

func overlayApproxDistinctRangeCount(ov fieldOverlay, br overlayRange) int {
	return br.baseEnd - br.baseStart
}

func (qv *queryView[K, V]) estimateLeafOrderCost(
	e qir.Expr,
	snap *plannerStatsSnapshot,
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
	if !qv.fieldOverlayForExpr(e).hasData() {
		return 0, 0, false, false, false
	}

	fieldName := qv.fieldNameByExpr(e)
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
		if isNumericRangeOp(e.Op) {
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

func (qv *queryView[K, V]) estimateEqSelectivity(field string, value any, universe uint64, stats PlannerFieldStats) (float64, bool) {
	key, isSlice, isNil, err := qv.exprValueToIdxScalar(qir.Expr{
		Op:           qir.OpEQ,
		FieldOrdinal: qv.fieldOrdinalByName(field),
		Value:        value,
	})
	if err != nil || isSlice {
		return 0, false
	}

	if isNil {
		card := qv.nilFieldOverlay(field).lookupCardinality(nilIndexEntryKey)
		if card == 0 {
			return 0, true
		}
		return float64(card) / float64(universe), true
	}
	ov := qv.fieldOverlay(field)
	if ov.hasData() {
		card := ov.lookupCardinality(key)
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

func (qv *queryView[K, V]) estimateInLikeSelectivity(field string, value any, universe uint64, intersect bool) (float64, int, bool) {
	valsBuf, isSlice, hasNil, err := qv.exprValueToDistinctIdxBuf(qir.Expr{
		Op:           qir.OpIN,
		FieldOrdinal: qv.fieldOrdinalByName(field),
		Value:        value,
	})
	if valsBuf != nil {
		defer stringSlicePool.Put(valsBuf)
	}
	valueCount := 0
	if valsBuf != nil {
		valueCount = valsBuf.Len()
	}
	if err != nil || !isSlice || (valueCount == 0 && !hasNil) {
		return 0, 0, false
	}

	sum := uint64(0)
	minCard := uint64(0)

	ov := qv.fieldOverlay(field)
	if !ov.hasData() {
		if !hasNil {
			return 0, 0, false
		}
	}

	for i := 0; i < valueCount; i++ {
		card := ov.lookupCardinality(valsBuf.Get(i))
		sum += card
		if minCard == 0 || card < minCard {
			minCard = card
		}
		if intersect && card == 0 {
			return 0, valueCount, true
		}
	}
	if fm := qv.fields[field]; hasNil && fm != nil && !fm.Slice {
		card := qv.nilFieldOverlay(field).lookupCardinality(nilIndexEntryKey)
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

func (qv *queryView[K, V]) estimateRangeSelectivity(e qir.Expr, universe uint64, stats PlannerFieldStats) (float64, bool) {
	rb, ok, err := qv.rangeBoundsForScalarExpr(e)
	if err != nil || !ok {
		return 0, false
	}
	if rb.empty {
		return 0, true
	}
	if !rb.hasLo && !rb.hasHi && !rb.hasPrefix {
		return 1, true
	}

	ov := qv.fieldOverlayForExpr(e)
	if ov.hasData() {
		br := ov.rangeForBounds(rb)
		if br.baseStart >= br.baseEnd {
			return 0, true
		}

		inBuckets, estCard := overlayRangeStats(ov, br)
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

func (qv *queryView[K, V]) plannerFieldStats(field string, snap *plannerStatsSnapshot, universe uint64) PlannerFieldStats {
	distinct := uint64(qv.fieldOverlay(field).keyCount())
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
