package rbi

import (
	"math"

	"github.com/vapstack/qx"
)

const (
	// Cost model margins. Values below 1 require measurable advantage
	// before choosing a specialized OR plan over baseline fallback.
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

func (db *DB[K, V]) shouldUsePlanORNoOrder(q *qx.QX, branches []plannerORBranch) bool {
	return db.decidePlanORNoOrder(q, branches).use
}

func (db *DB[K, V]) decidePlanORNoOrder(q *qx.QX, branches []plannerORBranch) plannerORNoOrderDecision {
	var d plannerORNoOrderDecision

	if len(q.Order) != 0 {
		return d
	}
	need, ok := orderWindow(q)
	if !ok || need <= 0 {
		return d
	}

	for i := range branches {
		if branches[i].alwaysTrue {
			d.use = true
			d.expectedRows = uint64(need)
			return d
		}
		if branches[i].lead == nil || branches[i].lead.iter == nil {
			return d
		}
	}

	snap := db.planner.stats.Load()
	universe := plannerORUniverse(snap, db.universe.GetCardinality())
	if universe == 0 {
		return d
	}

	unionCard, sumCard, branchCards, hasAlwaysTrue := estimateORUnionCards(branches, universe)
	if hasAlwaysTrue {
		d.use = true
		d.expectedRows = uint64(need)
		return d
	}
	if unionCard == 0 {
		return d
	}

	expectedRows := estimateRowsForNeed(uint64(need), unionCard, universe)
	avgChecks := estimateORNoOrderChecks(branches, branchCards)
	costKWay := float64(expectedRows) * (1.0 + avgChecks)
	costKWay *= db.plannerCostMultiplier(plannerCalORNoOrder)

	fallbackRows := minU64(unionCard, uint64(need))
	costFallback := float64(sumCard) + float64(fallbackRows)

	d.expectedRows = expectedRows
	d.kWayCost = costKWay
	d.fallbackCost = costFallback
	d.use = costKWay <= costFallback*plannerORNoOrderAdvantage
	return d
}

func (db *DB[K, V]) choosePlanOROrder(q *qx.QX, branches []plannerORBranch) plannerOROrderPlan {
	return db.decidePlanOROrder(q, branches).plan
}

func hasPrefixOnNonOrderFieldOR(branches []plannerORBranch, orderField string) bool {
	for i := range branches {
		leaves, ok := collectAndLeaves(branches[i].expr)
		if !ok {
			continue
		}
		for _, e := range leaves {
			if e.Not {
				continue
			}
			if e.Op == qx.OpPREFIX && e.Field != orderField {
				return true
			}
		}
	}
	return false
}

func (db *DB[K, V]) decidePlanOROrder(q *qx.QX, branches []plannerORBranch) plannerOROrderDecision {
	d := plannerOROrderDecision{plan: plannerOROrderFallback}

	if len(q.Order) != 1 {
		return d
	}

	o := q.Order[0]
	if o.Type != qx.OrderBasic {
		return d
	}

	fm := db.fields[o.Field]
	if fm == nil || fm.Slice {
		return d
	}

	slice := db.index[o.Field]
	if slice == nil || len(*slice) == 0 {
		return d
	}

	need, ok := orderWindow(q)
	if !ok || need <= 0 {
		return d
	}

	snap := db.planner.stats.Load()
	universe := plannerORUniverse(snap, db.universe.GetCardinality())
	if universe == 0 {
		return d
	}

	unionCard, sumCard, branchCards, hasAlwaysTrue := estimateORUnionCards(branches, universe)
	if unionCard == 0 {
		return d
	}

	orderStats := db.plannerOrderFieldStats(o.Field, snap, universe, uint64(len(*slice)))
	expectedRows := estimateRowsForNeed(uint64(need), unionCard, universe)

	streamChecks := estimateOROrderStreamChecks(branches, hasAlwaysTrue)
	streamSkew := plannerORSkew(orderStats)
	costStream := float64(expectedRows) * streamChecks * streamSkew
	costStream *= db.plannerCostMultiplier(plannerCalOROrderStream)

	costFallback := float64(sumCard) + float64(expectedRows)
	bestPlan := plannerOROrderFallback
	bestCost := costFallback

	mergeAllowed := !hasAlwaysTrue && need <= plannerOROrderMergeNeedMax
	if mergeAllowed && hasPrefixOnNonOrderFieldOR(branches, o.Field) {
		mergeAllowed = false
	}
	if mergeAllowed {
		costMerge := estimateOROrderMergeCost(uint64(need), branches, branchCards, unionCard, sumCard, universe, orderStats)
		costMerge *= db.plannerCostMultiplier(plannerCalOROrderMerge)
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

func plannerORUniverse(snap *PlannerStatsSnapshot, fallback uint64) uint64 {
	if snap != nil && snap.UniverseCardinality > 0 {
		return snap.UniverseCardinality
	}
	return fallback
}

func estimateORUnionCards(branches []plannerORBranch, universe uint64) (uint64, uint64, []uint64, bool) {
	if universe == 0 {
		return 0, 0, nil, false
	}

	branchCards := make([]uint64, 0, len(branches))
	sumCard := uint64(0)
	remainProb := 1.0

	for i := range branches {
		if branches[i].alwaysTrue {
			return universe, universe, append(branchCards, universe), true
		}

		card := estimateORBranchCard(branches[i], universe)
		branchCards = append(branchCards, card)
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
	return unionCard, sumCard, branchCards, false
}

func estimateORBranchCard(branch plannerORBranch, universe uint64) uint64 {
	if universe == 0 {
		return 0
	}
	if branch.alwaysTrue {
		return universe
	}

	minEst := uint64(0)
	active := 0
	for i := range branch.preds {
		p := branch.preds[i]
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
		if branch.lead != nil && branch.lead.estCard > 0 {
			minEst = branch.lead.estCard
			active = 1
		} else {
			minEst = maxU64(1, universe/plannerORBranchFallbackDiv)
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
		return minU64(need, universe)
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

func estimateORNoOrderChecks(branches []plannerORBranch, branchCards []uint64) float64 {
	if len(branches) == 0 {
		return 1.0
	}

	totalWeight := float64(0)
	totalChecks := float64(0)

	for i := range branches {
		weight := float64(1)
		if i < len(branchCards) && branchCards[i] > 0 {
			weight = float64(branchCards[i])
		}

		checks := 0
		for pi := range branches[i].preds {
			if pi == branches[i].leadIdx {
				continue
			}
			p := branches[i].preds[pi]
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

func estimateOROrderStreamChecks(branches []plannerORBranch, hasAlwaysTrue bool) float64 {
	if hasAlwaysTrue {
		return 1.0
	}

	if len(branches) == 0 {
		return 1.0
	}

	total := float64(0)
	for i := range branches {
		checks := 1
		for pi := range branches[i].preds {
			p := branches[i].preds[pi]
			if p.alwaysTrue || p.alwaysFalse {
				continue
			}
			if p.contains != nil {
				checks++
			}
		}
		total += float64(checks)
	}

	avg := total / float64(len(branches))
	if avg < 1 {
		avg = 1
	}
	// OR evaluation short-circuits; discount full-branch check average.
	checks := 1.0 + (avg-1.0)*0.65

	// More OR branches usually means more predicate probes per visited row.
	branchFactor := 1.0 + float64(len(branches)-1)*0.35
	return checks * branchFactor
}

func estimateOROrderMergeCost(
	need uint64,
	branches []plannerORBranch,
	branchCards []uint64,
	unionCard uint64,
	sumCard uint64,
	universe uint64,
	orderStats PlannerFieldStats,
) float64 {
	if need == 0 || universe == 0 {
		return math.Inf(1)
	}

	subRows := float64(0)
	candidateUpper := uint64(0)

	for i := range branches {
		if i >= len(branchCards) {
			break
		}
		card := branchCards[i]
		if card == 0 {
			continue
		}

		containsChecks := 1
		for pi := range branches[i].preds {
			p := branches[i].preds[pi]
			if p.alwaysTrue || p.alwaysFalse {
				continue
			}
			if p.contains != nil {
				containsChecks++
			}
		}

		rows := estimateRowsForNeed(need, card, universe)
		if card >= need {
			rowsCap := need + need/2
			if rowsCap < need {
				rowsCap = need
			}
			rows = minU64(rows, rowsCap)
		}

		checks := 1.0 + float64(containsChecks-1)*0.25
		subRows += float64(rows) * checks
		candidateUpper += minU64(need, card)
	}

	if candidateUpper == 0 {
		candidateUpper = minU64(need, unionCard)
	}

	if sumCard > 0 {
		candidateUpper = uint64(float64(candidateUpper) * (float64(unionCard) / float64(sumCard)))
		if candidateUpper == 0 {
			candidateUpper = 1
		}
	}

	rankRows := estimateOrderMergeRankRows(candidateUpper, need, universe, orderStats)
	return subRows + float64(rankRows)
}

func estimateOrderMergeRankRows(candidateUpper, need, universe uint64, stats PlannerFieldStats) uint64 {
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

	skew := plannerORSkew(stats)
	headAmplifier := 1.0 + float64(candidateUpper)/float64(maxU64(need, 1))*0.35

	rows := float64(candidateUpper) * skew * headAmplifier
	if rows < float64(need) {
		rows = float64(need)
	}
	if rows > float64(universe) {
		rows = float64(universe)
	}
	return uint64(rows)
}

func (db *DB[K, V]) plannerOrderFieldStats(field string, snap *PlannerStatsSnapshot, universe uint64, distinctFallback uint64) PlannerFieldStats {
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

func plannerORSkew(stats PlannerFieldStats) float64 {
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

func minU64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxU64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

const (
	plannerOrderedLeafMax              = 24
	plannerOrderedNeedHardMax          = 2_000_000
	plannerOrderedCostGainDefault      = 0.97
	plannerOrderedCostGainNoNeg        = 0.90
	plannerOrderedFallbackProbeBase    = 0.78
	plannerOrderedFallbackProbeSkewMul = 0.14
	plannerExecutionPreferGain         = 0.98

	plannerExecutionOrderBaseFactor   = 0.92
	plannerExecutionOrderCheckFactor  = 0.28
	plannerExecutionOrderRangeFactor  = 0.78
	plannerExecutionOrderPrefixFactor = 0.62

	plannerExecutionNoOrderPrefixLeafMax   = 6
	plannerExecutionNoOrderPrefixLimitMax  = 256
	plannerExecutionNoOrderPrefixBucketMax = 8 << 10
	plannerExecutionNoOrderPrefixProbeMax  = 128 << 10

	plannerExecutionNoOrderPrefixProbeShareMax = 0.40
)

type plannerOrderedProfile struct {
	selectivity      float64
	coverage         float64
	activeChecks     int
	hasNeg           bool
	hasPrefix        bool
	fallbackWorkRows float64
	orderRangeLeaves int
}

func (db *DB[K, V]) shouldUseOrderedByCost(q *qx.QX, leaves []qx.Expr) bool {
	return db.decideOrderedByCost(q, leaves).use
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

func executionOrderShapeInfo(orderField string, leaves []qx.Expr) (hasPrefix bool, hasOrderBounds bool, compatible bool) {
	for _, e := range leaves {
		if e.Op == qx.OpNOOP {
			if e.Not {
				return false, false, false
			}
			continue
		}
		if e.Not {
			return false, false, false
		}
		if e.Field == "" {
			return false, false, false
		}
		if e.Field != orderField {
			continue
		}

		switch e.Op {
		case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpEQ:
			hasOrderBounds = true
		case qx.OpPREFIX:
			hasOrderBounds = true
			hasPrefix = true
		default:
			return false, false, false
		}
	}
	return hasPrefix, hasOrderBounds, true
}

func (db *DB[K, V]) decideExecutionOrderByCost(q *qx.QX, leaves []qx.Expr) plannerExecutionOrderDecision {
	var d plannerExecutionOrderDecision

	if q.Expr.Not {
		return d
	}
	if len(q.Order) != 1 {
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

	o := q.Order[0]
	if o.Type != qx.OrderBasic || o.Field == "" {
		return d
	}

	fm := db.fields[o.Field]
	if fm == nil || fm.Slice {
		return d
	}
	orderSlice := db.index[o.Field]
	if orderSlice == nil || len(*orderSlice) == 0 {
		return d
	}

	hasPrefix, hasOrderBounds, compatible := executionOrderShapeInfo(o.Field, leaves)
	if !compatible || !hasOrderBounds {
		return d
	}

	snap := db.planner.stats.Load()
	universe := plannerORUniverse(snap, db.universe.GetCardinality())
	if universe == 0 {
		return d
	}

	profile, ok := db.estimateOrderedProfile(o.Field, leaves, *orderSlice, snap, universe)
	if !ok {
		return d
	}
	if profile.hasNeg {
		return d
	}

	plan := PlanLimitOrderBasic
	calPlan := plannerCalLimitOrderBasic
	if hasPrefix {
		plan = PlanLimitOrderPrefix
		calPlan = plannerCalLimitOrderPrefix
	}

	d.use = true
	d.plan = plan

	if profile.selectivity <= 0 {
		d.expectedProbeRows = 0
		d.executionCost = 1.0 * db.plannerCostMultiplier(calPlan)
		return d
	}

	orderStats := db.plannerOrderFieldStats(o.Field, snap, universe, uint64(len(*orderSlice)))
	orderSkew := plannerORSkew(orderStats)
	expectedProbeRows := estimateOrderExpectedProbes(float64(need), float64(universe), profile.selectivity, profile.coverage, orderSkew)

	execRowFactor := plannerExecutionOrderBaseFactor + float64(profile.activeChecks)*plannerExecutionOrderCheckFactor
	if profile.orderRangeLeaves > 0 {
		execRowFactor *= plannerExecutionOrderRangeFactor
	}
	if hasPrefix {
		execRowFactor *= plannerExecutionOrderPrefixFactor
	}
	if q.Offset > 0 {
		execRowFactor *= 0.95
	}
	execRowFactor = plannerClampFloat(execRowFactor, 0.25, 6.0)

	executionCost := expectedProbeRows*execRowFactor + float64(len(leaves))*12.0
	executionCost *= db.plannerCostMultiplier(calPlan)

	d.expectedProbeRows = uint64(expectedProbeRows)
	d.executionCost = executionCost
	return d
}

func (db *DB[K, V]) shouldPreferExecutionPlan(q *qx.QX, trace *queryTrace) bool {
	leaves, ok := collectAndLeaves(q.Expr)
	if !ok || len(leaves) == 0 {
		return false
	}

	if len(q.Order) == 0 {
		if db.shouldPreferExecutionNoOrderPrefix(q, leaves) {
			if trace != nil {
				trace.setEstimated(q.Limit, 1.0, 1.1)
			}
			return true
		}
		return false
	}

	execDecision := db.decideExecutionOrderByCost(q, leaves)
	if !execDecision.use {
		return false
	}

	orderedDecision := db.decideOrderedByCost(q, leaves)
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

func (db *DB[K, V]) shouldPreferExecutionNoOrderPrefix(q *qx.QX, leaves []qx.Expr) bool {
	if q.Limit == 0 || q.Offset != 0 {
		return false
	}
	if q.Limit > plannerExecutionNoOrderPrefixLimitMax {
		return false
	}
	if len(leaves) < 2 || len(leaves) > plannerExecutionNoOrderPrefixLeafMax {
		return false
	}

	universe := db.universe.GetCardinality()
	if universe == 0 {
		return true
	}

	prefixCount := 0
	prefixSpan := 0
	restSelectivity := 1.0
	restCount := 0

	for _, e := range leaves {
		if e.Op == qx.OpNOOP || e.Not || e.Field == "" {
			return false
		}

		if e.Op == qx.OpPREFIX {
			fm := db.fields[e.Field]
			if fm == nil || fm.Slice {
				return false
			}

			p, isSlice, err := db.exprValueToIdxScalar(e)
			if err != nil || isSlice || p == "" {
				return false
			}

			slice := db.index[e.Field]
			if slice == nil {
				return false
			}
			s := *slice
			if len(s) == 0 {
				return true
			}

			start := lowerBoundIndex(s, p)
			end := prefixRangeEndIndex(s, p, start)
			span := end - start
			if span <= 0 {
				return true
			}
			prefixSpan = span

			prefixCount++
			if prefixCount > 1 {
				return false
			}
			continue
		}

		switch e.Op {
		case qx.OpEQ, qx.OpIN, qx.OpHAS, qx.OpHASANY:
			fm := db.fields[e.Field]
			if fm == nil {
				return false
			}
			sel, _, _, _, ok := db.estimateLeafOrderCost(e, nil, universe, "", nil)
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
	if prefixSpan <= plannerExecutionNoOrderPrefixBucketMax {
		return true
	}
	if restSelectivity <= 0 {
		return true
	}

	need := float64(q.Limit)
	expectedProbe := need / restSelectivity
	if expectedProbe < need {
		expectedProbe = need
	}
	if expectedProbe > float64(prefixSpan) {
		expectedProbe = float64(prefixSpan)
	}
	if expectedProbe > plannerExecutionNoOrderPrefixProbeMax {
		return false
	}
	if expectedProbe > float64(prefixSpan)*plannerExecutionNoOrderPrefixProbeShareMax {
		return false
	}

	return true
}

func (db *DB[K, V]) decideOrderedByCost(q *qx.QX, leaves []qx.Expr) plannerOrderedDecision {
	var d plannerOrderedDecision

	if len(q.Order) != 1 {
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

	o := q.Order[0]
	if o.Type != qx.OrderBasic {
		return d
	}

	fm := db.fields[o.Field]
	if fm == nil || fm.Slice {
		return d
	}

	orderSlice := db.index[o.Field]
	if orderSlice == nil || len(*orderSlice) == 0 {
		return d
	}

	snap := db.planner.stats.Load()
	universe := plannerORUniverse(snap, db.universe.GetCardinality())
	if universe == 0 {
		return d
	}

	profile, ok := db.estimateOrderedProfile(o.Field, leaves, *orderSlice, snap, universe)
	if !ok {
		return d
	}

	// keep baseline fast paths for trivial unpaged positive predicates
	if !profile.hasNeg && q.Offset == 0 && profile.orderRangeLeaves == 0 {
		return d
	}
	// prefix + negation is still a high-risk shape for ordered strategy when unpaged
	if profile.hasPrefix && profile.hasNeg && q.Offset == 0 {
		return d
	}

	if profile.selectivity <= 0 {
		// impossible conjunction, ordered strategy can short-circuit after predicate build
		d.use = true
		return d
	}

	orderStats := db.plannerOrderFieldStats(o.Field, snap, universe, uint64(len(*orderSlice)))
	orderSkew := plannerORSkew(orderStats)

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
	orderedCost *= db.plannerCostMultiplier(plannerCalOrdered)

	fallbackProbeFactor := plannerOrderedFallbackProbeBase + plannerOrderedFallbackProbeSkewMul*orderSkew
	if fallbackProbeFactor < 0.75 {
		fallbackProbeFactor = 0.75
	}
	if fallbackProbeFactor > 2.0 {
		fallbackProbeFactor = 2.0
	}
	fallbackCost := profile.fallbackWorkRows + expectedProbeRows*fallbackProbeFactor + float64(len(leaves))*20.0

	execDecision := db.decideExecutionOrderByCost(q, leaves)
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

func (db *DB[K, V]) estimateOrderedProfile(orderField string, leaves []qx.Expr, orderSlice []index, snap *PlannerStatsSnapshot, universe uint64) (plannerOrderedProfile, bool) {
	if universe == 0 {
		return plannerOrderedProfile{}, false
	}

	start, end, covered, ok := db.extractOrderRangeCoverageLeaves(orderField, leaves, orderSlice)
	if !ok {
		return plannerOrderedProfile{}, false
	}

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

	for i, e := range leaves {
		leafSel, leafWork, leafOrderRange, leafHasPrefix, ok := db.estimateLeafOrderCost(e, snap, universe, orderField, orderSlice)
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

		if e.Op == qx.OpNOOP && !e.Not {
			continue
		}
		if i < len(covered) && covered[i] {
			continue
		}
		activeChecks++
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
	}, true
}

func (db *DB[K, V]) extractOrderRangeCoverageLeaves(orderField string, leaves []qx.Expr, orderSlice []index) (int, int, []bool, bool) {
	rb := rangeBounds{}
	covered := make([]bool, len(leaves))

	has := false
	for i := range leaves {
		e := leaves[i]
		if e.Not || e.Field != orderField {
			continue
		}

		switch e.Op {
		case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpEQ:
			k, isSlice, err := db.exprValueToIdxScalar(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
			if err != nil || isSlice {
				return 0, 0, nil, false
			}
			switch e.Op {
			case qx.OpGT:
				rb.applyLo(k, false)
			case qx.OpGTE:
				rb.applyLo(k, true)
			case qx.OpLT:
				rb.applyHi(k, false)
			case qx.OpLTE:
				rb.applyHi(k, true)
			case qx.OpEQ:
				rb.applyLo(k, true)
				rb.applyHi(k, true)
			}
			covered[i] = true
			has = true
		case qx.OpPREFIX:
			p, isSlice, err := db.exprValueToIdxScalar(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
			if err != nil || isSlice {
				return 0, 0, nil, false
			}
			rb.hasPrefix = true
			rb.prefix = p
			covered[i] = true
			has = true
		}
	}

	if !has {
		return 0, len(orderSlice), covered, true
	}

	st, en := applyBoundsToIndexRange(orderSlice, rb)
	return st, en, covered, true
}

func (db *DB[K, V]) estimateLeafOrderCost(
	e qx.Expr,
	snap *PlannerStatsSnapshot,
	universe uint64,
	orderField string,
	orderSlice []index,
) (selectivity float64, fallbackWork float64, orderRange bool, hasPrefix bool, ok bool) {

	if e.Op == qx.OpNOOP {
		if e.Not {
			return 0, 0, false, false, true
		}
		return 1, 0, false, false, true
	}

	if e.Field == "" {
		return 0, 0, false, false, false
	}
	if db.fields[e.Field] == nil {
		return 0, 0, false, false, false
	}
	if db.index[e.Field] == nil {
		return 0, 0, false, false, false
	}

	fieldStats := db.plannerFieldStats(e.Field, snap, universe)
	rawSel := 0.0
	valueCount := 1

	switch e.Op {
	case qx.OpEQ:
		rawSel, ok = db.estimateEqSelectivity(e.Field, e.Value, universe, fieldStats)
	case qx.OpIN:
		rawSel, valueCount, ok = db.estimateInLikeSelectivity(e.Field, e.Value, universe, false)
	case qx.OpHASANY:
		rawSel, valueCount, ok = db.estimateInLikeSelectivity(e.Field, e.Value, universe, false)
	case qx.OpHAS:
		rawSel, valueCount, ok = db.estimateInLikeSelectivity(e.Field, e.Value, universe, true)
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
		rawSel, ok = db.estimateRangeSelectivity(e, universe, fieldStats)
		if e.Field == orderField {
			orderRange = true
		}
		if e.Op == qx.OpPREFIX {
			hasPrefix = true
		}
	default:
		return 0, 0, false, false, false
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
	case qx.OpIN, qx.OpHASANY:
		fallbackWork *= 1.0 + float64(maxInt(valueCount-1, 0))*0.35
	case qx.OpHAS:
		fallbackWork *= 1.0 + float64(maxInt(valueCount-1, 0))*0.55
	case qx.OpPREFIX:
		fallbackWork *= 1.20
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE:
		fallbackWork *= 1.05
	}
	if e.Not {
		fallbackWork += 0.08 * float64(universe)
	}

	// range-only order queries benefit from fast baseline scans when unpaged
	if e.Field == orderField && !e.Not && (e.Op == qx.OpGT || e.Op == qx.OpGTE || e.Op == qx.OpLT || e.Op == qx.OpLTE || e.Op == qx.OpPREFIX) {
		if len(orderSlice) > 0 {
			fallbackWork *= 0.92
		}
	}

	return selectivity, fallbackWork, orderRange, hasPrefix, true
}

func (db *DB[K, V]) estimateEqSelectivity(field string, value any, universe uint64, stats PlannerFieldStats) (float64, bool) {
	key, isSlice, err := db.exprValueToIdxScalar(qx.Expr{Op: qx.OpEQ, Field: field, Value: value})
	if err != nil || isSlice {
		return 0, false
	}

	if slice := db.index[field]; slice != nil {
		if bm := findIndex(slice, key); bm != nil {
			return float64(bm.GetCardinality()) / float64(universe), true
		}
		return 0, true
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

func (db *DB[K, V]) estimateInLikeSelectivity(field string, value any, universe uint64, intersect bool) (float64, int, bool) {
	vals, isSlice, err := db.exprValueToIdx(qx.Expr{Op: qx.OpIN, Field: field, Value: value})
	if err != nil || !isSlice || len(vals) == 0 {
		return 0, 0, false
	}

	slice := db.index[field]
	if slice == nil {
		return 0, 0, false
	}

	sum := uint64(0)
	minCard := uint64(0)
	for _, key := range vals {
		bm := findIndex(slice, key)
		card := uint64(0)
		if bm != nil {
			card = bm.GetCardinality()
		}
		sum += card
		if minCard == 0 || card < minCard {
			minCard = card
		}
		if intersect && card == 0 {
			return 0, len(vals), true
		}
	}

	if !intersect {
		sel := float64(sum) / float64(universe)
		if sel > 1 {
			sel = 1
		}
		return sel, len(vals), true
	}

	sel := float64(minCard) / float64(universe)
	if len(vals) > 1 {
		decay := 1.0
		for i := 1; i < len(vals); i++ {
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
	return sel, len(vals), true
}

func (db *DB[K, V]) estimateRangeSelectivity(e qx.Expr, universe uint64, stats PlannerFieldStats) (float64, bool) {
	key, isSlice, err := db.exprValueToIdxScalar(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil || isSlice {
		return 0, false
	}

	slice := db.index[e.Field]
	if slice == nil {
		return 0, false
	}
	s := *slice
	if len(s) == 0 {
		return 0, true
	}

	start, end, ok := resolveRange(s, e.Op, key)
	if !ok {
		return 0, false
	}
	if start >= end {
		return 0, true
	}
	if start == 0 && end == len(s) {
		return 1, true
	}

	inBuckets := end - start
	estCard := uint64(0)

	if inBuckets == 1 {
		if bm := s[start].IDs; bm != nil {
			estCard = bm.GetCardinality()
		}
	} else {
		ix0 := start
		ix1 := start + inBuckets/2
		ix2 := end - 1

		var sum uint64
		var n uint64

		if bm := s[ix0].IDs; bm != nil {
			sum += bm.GetCardinality()
			n++
		}
		if ix1 != ix0 && ix1 != ix2 {
			if bm := s[ix1].IDs; bm != nil {
				sum += bm.GetCardinality()
				n++
			}
		}
		if ix2 != ix0 {
			if bm := s[ix2].IDs; bm != nil {
				sum += bm.GetCardinality()
				n++
			}
		}

		if n > 0 {
			estCard = (sum / n) * uint64(inBuckets)
		}
	}

	if estCard == 0 {
		avg := stats.AvgBucketCard
		if avg <= 0 {
			if stats.DistinctKeys > 0 {
				avg = float64(universe) / float64(stats.DistinctKeys)
			} else {
				avg = float64(universe) / float64(len(s))
			}
		}
		estCard = uint64(avg * float64(inBuckets))
	}

	if estCard > universe {
		estCard = universe
	}
	return float64(estCard) / float64(universe), true
}

func (db *DB[K, V]) plannerFieldStats(field string, snap *PlannerStatsSnapshot, universe uint64) PlannerFieldStats {
	distinct := uint64(0)
	if slice := db.index[field]; slice != nil {
		distinct = uint64(len(*slice))
	}
	return db.plannerOrderFieldStats(field, snap, universe, distinct)
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

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
