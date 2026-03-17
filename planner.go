package rbi

import (
	"math"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/roaring64"
)

// PlanName is a stable plan identifier used by tracing and calibration.
type PlanName string

const (
	PlanBitmap             PlanName = "plan_bitmap"
	PlanCountBitmap        PlanName = "plan_count_bitmap"
	PlanCountUniqueEq      PlanName = "plan_count_unique_eq"
	PlanCountScalarLookup  PlanName = "plan_count_scalar_lookup"
	PlanCountScalarInSplit PlanName = "plan_count_scalar_in_split"
	PlanCountPredicates    PlanName = "plan_count_predicates"
	PlanCountORPredicates  PlanName = "plan_count_or_predicates"

	PlanCandidateNoOrder PlanName = "plan_candidate_no_order"
	PlanCandidateOrder   PlanName = "plan_candidate_order"

	PlanORMergeNoOrder     PlanName = "plan_or_merge_no_order"
	PlanORMergeOrderMerge  PlanName = "plan_or_merge_order_merge"
	PlanORMergeOrderStream PlanName = "plan_or_merge_order_stream"

	PlanOrdered        PlanName = "plan_ordered"
	PlanOrderedNoOrder PlanName = "plan_ordered_no_order"
	PlanOrderedAnchor  PlanName = "plan_ordered_anchor"
	PlanOrderedLead    PlanName = "plan_ordered_lead"

	PlanLimit              PlanName = "plan_limit"
	PlanLimitOrderBasic    PlanName = "plan_limit_order_basic"
	PlanLimitOrderPrefix   PlanName = "plan_limit_order_prefix"
	PlanLimitPrefixNoOrder PlanName = "plan_limit_prefix_no_order"
	PlanLimitRangeNoOrder  PlanName = "plan_limit_range_no_order"
	PlanUniqueEq           PlanName = "plan_unique_eq"
)

const (
	PlanPrefixORMerge      = "plan_or_merge_"
	PlanPrefixORMergeOrder = "plan_or_merge_order_"
)

const (
	plannerORBranchLimit       = 8
	plannerORNoOrderLimitMax   = 10_000
	plannerOROrderLimitMax     = 10_000
	plannerOROrderMergeNeedMax = 4_096
	plannerOROrderMergeAvgMax  = 8
	plannerORNoOrderOffsetMax  = 1_000_000
	plannerOROrderOffsetMax    = 1_000_000
	plannerORMinOperandCount   = 2

	plannerORNoOrderInitActiveBranches = 2
	plannerORNoOrderMinBranchBudget    = 64
	plannerORNoOrderMaxBranchBudget    = 65_536
	plannerORNoOrderLinearSeenNeedMax  = 8

	plannerOROrderFallbackFirstLeadNeedMul = 6
	plannerOROrderFallbackFirstGain        = 0.95
	plannerOROrderFallbackFirstOffsetGain  = 1.45
	plannerOROrderMergePrecountGain        = 0.92
	plannerOROrderMergePrecountBaseCost    = 64.0

	plannerORKWayRuntimeNeedMin              = 64
	plannerORKWayRuntimeMinPops              = 32
	plannerORKWayRuntimeMinUnique            = 8
	plannerORKWayRuntimeMinExamined          = 16_384
	plannerORKWayRuntimeProjectedExaminedMax = 2_500_000.0
	plannerORKWayRuntimeExaminedPerUniqueMin = 1_024.0
	plannerORKWayRuntimeAvgChecksEnableMin   = 2.0

	plannerORKWayRuntimeLowOverlapNeedMin           = 8_192
	plannerORKWayRuntimeLowOverlapMinPops           = 256
	plannerORKWayRuntimeLowOverlapMinUniquePerPop   = 0.90
	plannerORKWayRuntimeLowOverlapProjectedPopsMax  = 200_000.0
	plannerORKWayRuntimeLowOverlapExaminedPerUnique = 64.0
)

type plannerORBranch struct {
	expr qx.Expr

	preds      []predicate
	alwaysTrue bool
	lead       *predicate
	leadIdx    int
}

type plannerORBranches []plannerORBranch

func plannerORSortPredicates(preds []predicate) {
	if len(preds) <= 1 {
		return
	}

	less := func(a, b predicate) bool {
		if a.alwaysFalse != b.alwaysFalse {
			return a.alwaysFalse
		}
		// Unknown cardinality does not help selectivity ordering.
		ca := a.estCard
		cb := b.estCard
		if ca == 0 {
			ca = math.MaxUint64
		}
		if cb == 0 {
			cb = math.MaxUint64
		}
		if ca != cb {
			return ca < cb
		}
		return false
	}

	// Predicate lists are tiny in practice; insertion sort avoids allocations
	// and keeps hot-path overhead minimal.
	for i := 1; i < len(preds); i++ {
		cur := preds[i]
		j := i
		for j > 0 && less(cur, preds[j-1]) {
			preds[j] = preds[j-1]
			j--
		}
		preds[j] = cur
	}
}

func (b *plannerORBranch) buildMatchChecks(dst []int) []int {
	dst = dst[:0]
	if b.alwaysTrue {
		return dst
	}
	for i := range b.preds {
		p := b.preds[i]
		if p.covered || p.alwaysTrue {
			continue
		}
		dst = append(dst, i)
	}
	sortActivePredicates(dst, b.preds)
	return dst
}

func (b *plannerORBranch) buildBitmapFilterChecks(dst []int, checks []int) []int {
	dst = dst[:0]
	if b.alwaysTrue {
		return dst
	}
	for _, i := range checks {
		if b.preds[i].supportsBitmapFilter() {
			dst = append(dst, i)
		}
	}
	return dst
}

func (b *plannerORBranch) matchesChecks(idx uint64, checks []int) bool {
	if b.alwaysTrue {
		return true
	}
	for _, i := range checks {
		p := b.preds[i]
		if p.alwaysFalse || !p.hasContains() || !p.matches(idx) {
			return false
		}
	}
	return true
}

type plannerPredicateBucketMode uint8

const (
	plannerPredicateBucketFallback plannerPredicateBucketMode = iota
	plannerPredicateBucketEmpty
	plannerPredicateBucketAll
	plannerPredicateBucketExact
)

const plannerPredicateBucketExactMinCard = 32

func plannerFilterBitmapByChecks(preds []predicate, checks []int, src, work *roaring64.Bitmap, allowExact bool) (plannerPredicateBucketMode, *roaring64.Bitmap, uint64) {
	if src == nil || src.IsEmpty() {
		return plannerPredicateBucketEmpty, nil, 0
	}
	card := src.GetCardinality()
	if len(checks) == 0 {
		return plannerPredicateBucketAll, src, card
	}

	skipBucket := false
	fullBucket := true
	for _, pi := range checks {
		cnt, ok := preds[pi].countBucket(src)
		if !ok {
			fullBucket = false
			continue
		}
		if cnt == 0 {
			skipBucket = true
			break
		}
		if cnt != card {
			fullBucket = false
		}
	}
	if skipBucket {
		return plannerPredicateBucketEmpty, nil, card
	}
	if fullBucket {
		return plannerPredicateBucketAll, src, card
	}
	// Tiny buckets are cheaper to scan row-by-row than to clone into a scratch
	// roaring bitmap for exact filtering. This keeps the OR-order hot path from
	// paying large GC/alloc overhead on the common many-small-buckets shape.
	if !allowExact || card <= plannerPredicateBucketExactMinCard {
		return plannerPredicateBucketFallback, nil, card
	}
	if work == nil {
		return plannerPredicateBucketFallback, nil, card
	}

	work.Clear()
	work.Or(src)
	for _, pi := range checks {
		if !preds[pi].applyToBitmap(work) {
			return plannerPredicateBucketFallback, nil, card
		}
		if work.IsEmpty() {
			return plannerPredicateBucketEmpty, nil, card
		}
	}
	return plannerPredicateBucketExact, work, card
}

func (b *plannerORBranch) matchesFromLead(idx uint64) bool {
	if b.alwaysTrue {
		return true
	}
	leadNeedsCheck := b.lead != nil && b.lead.leadIterNeedsContainsCheck()
	for i := range b.preds {
		if i == b.leadIdx && !leadNeedsCheck {
			continue
		}
		p := b.preds[i]
		if p.covered || p.alwaysTrue {
			continue
		}
		if p.alwaysFalse {
			return false
		}
		if !p.hasContains() || !p.matches(idx) {
			return false
		}
	}
	return true
}

func (b plannerORBranch) containsChecks() int {
	if b.alwaysTrue {
		return 0
	}
	checks := 0
	for i := range b.preds {
		p := b.preds[i]
		if p.alwaysTrue || p.alwaysFalse || !p.hasContains() {
			continue
		}
		checks++
	}
	return checks
}

func (b plannerORBranch) evalScore() float64 {
	if b.alwaysTrue {
		return math.MaxFloat64
	}
	checks := b.containsChecks()
	if checks <= 0 {
		checks = 1
	}

	est := uint64(1)
	if b.lead != nil && b.lead.estCard > 0 {
		est = b.lead.estCard
	} else {
		for i := range b.preds {
			if c := b.preds[i].estCard; c > est {
				est = c
			}
		}
	}
	return float64(est) / float64(checks)
}

func (b plannerORBranch) noOrderScore() float64 {
	if b.alwaysTrue {
		return 0
	}
	leadEst := uint64(1)
	if b.lead != nil && b.lead.estCard > 0 {
		leadEst = b.lead.estCard
	}
	extraChecks := 1.0
	for i := range b.preds {
		if i == b.leadIdx {
			continue
		}
		p := b.preds[i]
		if p.alwaysTrue {
			continue
		}
		extraChecks += 1.0
	}
	return float64(leadEst) * extraChecks
}

func (b plannerORBranch) noOrderBudget(need int) uint64 {
	if b.alwaysTrue {
		return plannerORNoOrderMinBranchBudget
	}

	leadEst := uint64(0)
	if b.lead != nil {
		leadEst = b.lead.estCard
	}
	if leadEst == 0 {
		leadEst = plannerORNoOrderMinBranchBudget
	}

	budget := leadEst / 64
	if budget < plannerORNoOrderMinBranchBudget {
		budget = plannerORNoOrderMinBranchBudget
	}
	// For small LIMIT queries, probing too deeply in broad branches tends to
	// waste work before we even establish an initial threshold window.
	if need > 0 {
		perNeedCap := uint64(need) << 6
		if perNeedCap < plannerORNoOrderMinBranchBudget {
			perNeedCap = plannerORNoOrderMinBranchBudget
		}
		if budget > perNeedCap {
			budget = perNeedCap
		}
	}
	if budget > plannerORNoOrderMaxBranchBudget {
		budget = plannerORNoOrderMaxBranchBudget
	}
	return budget
}

func (bs plannerORBranches) avgContainsChecks() float64 {
	totalChecks := 0.0
	activeBranches := 0.0
	for i := range bs {
		if bs[i].alwaysTrue {
			continue
		}
		totalChecks += float64(bs[i].containsChecks())
		activeBranches++
	}
	if activeBranches == 0 {
		return 0
	}
	return totalChecks / activeBranches
}

func (bs plannerORBranches) hasSelectiveLead(need int) bool {
	if need <= 0 {
		return false
	}
	threshold := uint64(need * plannerOROrderFallbackFirstLeadNeedMul)
	if threshold == 0 {
		threshold = 1
	}
	for i := range bs {
		lead := bs[i].lead
		if lead == nil || lead.estCard == 0 {
			continue
		}
		if lead.estCard <= threshold {
			return true
		}
	}
	return false
}

type plannerOROrderRouteCost struct {
	kWay              float64
	fallback          float64
	overlap           float64
	hasPrefixNonOrder bool
	hasSelectiveLead  bool
}

type plannerOROrderFallbackDecision struct {
	prefer              bool
	reason              string
	routeCost           plannerOROrderRouteCost
	avgChecks           float64
	fallbackCollectFast bool
}

type plannerOROrderRuntimeGuardDecision struct {
	enable    bool
	reason    string
	routeCost plannerOROrderRouteCost
	avgChecks float64
}

type plannerORKWayRuntimeShape struct {
	overlap   float64
	avgChecks float64
	offset    uint64
}

type plannerORKWayRuntimeDecision struct {
	fallback             bool
	reason               string
	examinedPerUnique    float64
	projectedExamined    float64
	projectedExaminedMax float64
}

func plannerOROrderMergeNeedLimit(need int, branchCount int, unionCard, sumCard uint64, offset uint64) int {
	limit := plannerOROrderMergeNeedMax
	if branchCount >= 6 {
		limit = limit * 7 / 8
	} else if branchCount >= 4 {
		limit = limit * 15 / 16
	}
	if offset > 0 {
		limit = limit * 17 / 16
	}
	if unionCard > 0 && sumCard > 0 {
		overlap := float64(sumCard) / float64(unionCard)
		switch {
		case overlap >= 2.5:
			limit = limit * 7 / 8
		case overlap >= 1.8:
			limit = limit * 15 / 16
		case overlap <= 1.10:
			limit = limit * 17 / 16
		}
	}
	if limit < 2_048 {
		limit = 2_048
	}
	if limit > 8_192 {
		limit = 8_192
	}
	if need > 0 && limit < need/2 {
		limit = need / 2
	}
	return limit
}

func plannerOROrderFallbackFirstGainForShape(routeCost plannerOROrderRouteCost, q *qx.QX, need int) float64 {
	gain := plannerOROrderFallbackFirstGain
	if routeCost.overlap >= 2.0 {
		gain += 0.02
	} else if routeCost.overlap <= 1.2 {
		gain -= 0.01
	}
	if routeCost.hasSelectiveLead {
		gain -= 0.01
	}
	if q != nil && q.Offset > 0 {
		gain += 0.01
		if need > 0 && q.Offset > uint64(need) {
			gain += 0.01
		}
	}
	if routeCost.hasPrefixNonOrder {
		gain += 0.01
	}
	return plannerClampFloat(gain, 0.90, 1.02)
}

func plannerOROrderFallbackFirstOffsetGainForShape(routeCost plannerOROrderRouteCost, q *qx.QX, need int) float64 {
	gain := plannerOROrderFallbackFirstOffsetGain
	if routeCost.overlap >= 2.0 {
		gain += 0.05
	}
	if routeCost.hasSelectiveLead {
		gain -= 0.05
	}
	if q != nil && need > 0 && q.Offset > uint64(need*2) {
		gain += 0.05
	}
	return plannerClampFloat(gain, 1.25, 1.60)
}

func plannerORKWayRuntimeShapeFromGuard(guard plannerOROrderRuntimeGuardDecision, q *qx.QX) plannerORKWayRuntimeShape {
	shape := plannerORKWayRuntimeShape{
		overlap:   guard.routeCost.overlap,
		avgChecks: guard.avgChecks,
	}
	if q != nil {
		shape.offset = q.Offset
	}
	if shape.overlap <= 0 {
		shape.overlap = 1
	}
	if shape.avgChecks <= 0 {
		shape.avgChecks = 1
	}
	return shape
}

func plannerORKWayRuntimeNeedMinForShape(shape plannerORKWayRuntimeShape) int {
	minNeed := plannerORKWayRuntimeNeedMin
	if shape.offset > 0 {
		minNeed = minNeed * 7 / 8
	}
	if shape.overlap >= 2.0 || shape.avgChecks >= 2.5 {
		minNeed = minNeed * 7 / 8
	}
	if shape.overlap < 1.2 && shape.avgChecks < 1.5 && shape.offset == 0 {
		minNeed = minNeed * 5 / 4
	}
	if minNeed < 32 {
		minNeed = 32
	}
	if minNeed > 128 {
		minNeed = 128
	}
	return minNeed
}

func plannerORKWayRuntimeNearTieGain(shape plannerORKWayRuntimeShape) float64 {
	gain := 0.85
	if shape.overlap >= 2.0 {
		gain += 0.02
	}
	if shape.avgChecks >= 2.5 {
		gain += 0.02
	}
	if shape.offset > 0 {
		gain += 0.01
	}
	return plannerClampFloat(gain, 0.84, 0.90)
}

// estimateOROrderMergeRouteCost estimates relative work for the two merge
// sub-routes: k-way stream merge vs fallback branch-collect+rank.
func (db *DB[K, V]) estimateOROrderMergeRouteCost(q *qx.QX, branches plannerORBranches, need int) (plannerOROrderRouteCost, bool) {
	if need <= 0 || q == nil || len(q.Order) != 1 {
		return plannerOROrderRouteCost{}, false
	}

	orderField := q.Order[0].Field
	if orderField == "" {
		return plannerOROrderRouteCost{}, false
	}

	snap := db.planner.stats.Load()
	universe := snap.universeOr(db.snapshotUniverseCardinality())
	if universe == 0 {
		return plannerOROrderRouteCost{}, false
	}

	unionCard, sumCard, branchCards, _ := branches.unionCards(universe)
	if unionCard == 0 {
		return plannerOROrderRouteCost{}, false
	}

	expectedRows := estimateRowsForNeed(uint64(need), unionCard, universe)
	if expectedRows == 0 {
		return plannerOROrderRouteCost{}, false
	}

	avgChecks := branches.avgContainsChecks()
	if avgChecks <= 0 {
		avgChecks = 1
	}

	overlap := float64(sumCard) / float64(max(unionCard, uint64(1)))
	if overlap < 1 {
		overlap = 1
	}
	if overlap > 16 {
		overlap = 16
	}

	hasPrefixNonOrder := branches.hasPrefixOnNonOrderField(orderField)
	hasSelectiveLead := branches.hasSelectiveLead(need)
	offsetShare := 0.0
	if need > 0 && q.Offset > 0 {
		offsetShare = float64(q.Offset) / float64(need)
		if offsetShare > 1 {
			offsetShare = 1
		}
	}

	kWayRows := float64(expectedRows) * overlap
	if offsetShare > 0 {
		kWayRows *= 1.0 + offsetShare*0.75
	}
	if hasPrefixNonOrder {
		kWayRows *= 1.15
	}
	if hasSelectiveLead {
		kWayRows *= 0.92
	}

	// K-way pays per-pop merge + predicate checks; fallback pays branch collection
	// and order ranking of merged candidates.
	kWayCost := kWayRows * (1.0 + avgChecks*0.55)
	fallbackCollectRows := 0.0
	activeBranches := 0
	for _, card := range branchCards {
		if card == 0 {
			continue
		}
		activeBranches++
		probes := float64(estimateRowsForNeed(uint64(need), card, universe))
		if probes < float64(need) {
			probes = float64(need)
		}
		if offsetShare > 0 {
			probes *= 1.0 + offsetShare*0.35
		}
		fallbackCollectRows += probes
	}
	if activeBranches == 0 {
		activeBranches = len(branches)
	}
	fallbackCandidates := float64(min(sumCard, uint64(need*activeBranches)))
	fallbackCost := fallbackCollectRows*(1.0+avgChecks*0.22) + fallbackCandidates*(1.0+overlap*0.08)

	return plannerOROrderRouteCost{
		kWay:              kWayCost,
		fallback:          fallbackCost,
		overlap:           overlap,
		hasPrefixNonOrder: hasPrefixNonOrder,
		hasSelectiveLead:  hasSelectiveLead,
	}, true
}

func (bs plannerORBranches) evalOrder() ([plannerORBranchLimit]int, int) {
	var order [plannerORBranchLimit]int
	var scores [plannerORBranchLimit]float64
	var checks [plannerORBranchLimit]int

	n := len(bs)
	if n > plannerORBranchLimit {
		n = plannerORBranchLimit
	}
	for i := 0; i < n; i++ {
		order[i] = i
		scores[i] = bs[i].evalScore()
		checks[i] = bs[i].containsChecks()
	}

	// Small fixed-size insertion sort: highest score first, then fewer checks.
	for i := 1; i < n; i++ {
		cur := order[i]
		j := i
		for j > 0 {
			prev := order[j-1]
			cs := scores[cur]
			ps := scores[prev]
			if cs < ps {
				break
			}
			if cs == ps && checks[cur] >= checks[prev] {
				break
			}
			order[j] = prev
			j--
		}
		order[j] = cur
	}
	return order, n
}

type plannerORIter struct {
	it     roaringIter
	branch *plannerORBranch

	examined       *uint64
	branchExamined *uint64
	branchEmitted  *uint64

	has bool
	cur uint64
}

func (it *plannerORIter) Release() {
	releaseRoaringIter(it.it)
	it.it = nil
	it.has = false
	it.cur = 0
}

func (db *DB[K, V]) tryPlan(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	if out, ok, err := db.tryPlanORMerge(q, trace); ok {
		return out, true, err
	}
	if out, ok, err := db.tryPlanOrdered(q, trace); ok {
		return out, true, err
	}
	if out, ok, err := db.tryPlanCandidate(q, trace); ok {
		return out, true, err
	}
	return nil, false, nil
}

func (it *plannerORIter) advance() {
	it.advanceWithBudget(0)
}

func (it *plannerORIter) advanceWithBudget(budget uint64) (budgetHit bool) {
	it.has = false
	var scanned uint64
	for it.it.HasNext() {
		if budget > 0 && scanned >= budget {
			return true
		}
		idx := it.it.Next()
		scanned++
		if it.examined != nil {
			*it.examined = *it.examined + 1
		}
		if it.branchExamined != nil {
			*it.branchExamined = *it.branchExamined + 1
		}
		if it.branch.matchesFromLead(idx) {
			it.cur = idx
			it.has = true
			if it.branchEmitted != nil {
				*it.branchEmitted = *it.branchEmitted + 1
			}
			return false
		}
	}
	return false
}

func (db *DB[K, V]) tryPlanORMerge(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	return db.tryPlanORMergeMode(q, trace)
}

// tryPlanORMergeMode plans top-level OR queries with bounded branch count.
//
// This path exists to avoid full bitmap materialization when branch-aware
// streaming/merge strategies can satisfy LIMIT/OFFSET cheaper.
func (db *DB[K, V]) tryPlanORMergeMode(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	if q.Limit == 0 {
		return nil, false, nil
	}
	if len(q.Order) > 1 {
		return nil, false, nil
	}
	if q.Expr.Op != qx.OpOR || q.Expr.Not {
		return nil, false, nil
	}
	if len(q.Expr.Operands) < plannerORMinOperandCount {
		return nil, false, nil
	}
	if len(q.Expr.Operands) > plannerORBranchLimit {
		return nil, false, nil
	}
	// No-order OR requires branch leads; otherwise the adaptive/baseline runners
	// cannot advance branches independently and we fall back to bitmap eval.
	if len(q.Order) == 0 && !hasNoOrderLeadCandidatesOR(q.Expr.Operands) {
		return nil, false, nil
	}

	branches, alwaysFalse, ok := db.buildORBranches(q.Expr.Operands)
	if !ok {
		return nil, false, nil
	}
	if alwaysFalse {
		return nil, true, nil
	}
	defer releaseORBranches(branches)

	if len(q.Order) == 0 {
		if q.Limit > plannerORNoOrderLimitMax || q.Offset > plannerORNoOrderOffsetMax {
			return nil, false, nil
		}
		// Cost gate is important here: baseline OR merge can regress badly on
		// high-overlap branches, so we only enter when model says it should win.
		noOrderDecision := db.decidePlanORNoOrder(q, branches)
		if trace != nil {
			trace.setEstimated(noOrderDecision.expectedRows, noOrderDecision.kWayCost, noOrderDecision.fallbackCost)
		}
		if !noOrderDecision.use {
			return nil, false, nil
		}
		out, ok := db.execPlanORNoOrder(q, branches, trace)
		if !ok {
			return nil, false, nil
		}
		if trace != nil {
			trace.setPlan(PlanORMergeNoOrder)
		}
		return out, true, nil
	}

	o := q.Order[0]
	if o.Type != qx.OrderBasic {
		return nil, false, nil
	}
	if q.Limit > plannerOROrderLimitMax || q.Offset > plannerOROrderOffsetMax {
		return nil, false, nil
	}

	// Ordered OR has two distinct strategies: k-way merge of branch streams and
	// stream+match fallback. Cost model chooses initial route.
	orderDecision := db.decidePlanOROrder(q, branches)
	if trace != nil {
		trace.setEstimated(orderDecision.expectedRows, orderDecision.bestCost, orderDecision.fallbackCost)
	}

	switch orderDecision.plan {

	case plannerOROrderMerge:
		out, ok, err := db.execPlanOROrderMerge(q, branches, trace)
		if ok {
			if trace != nil {
				trace.setPlan(PlanORMergeOrderMerge)
			}
			return out, true, err
		}
		// merge estimate can be optimistic if branch overlap is high;
		// fall back to streaming OR order plan before giving up.
		out2, ok2 := db.execPlanOROrderBasic(q, branches, trace)
		if ok2 {
			if trace != nil {
				trace.setPlan(PlanORMergeOrderStream)
			}
			return out2, true, nil
		}
		return nil, false, nil

	case plannerOROrderStream:
		out, ok := db.execPlanOROrderBasic(q, branches, trace)
		if !ok {
			return nil, false, nil
		}
		if trace != nil {
			trace.setPlan(PlanORMergeOrderStream)
		}
		return out, true, nil

	default:
		return nil, false, nil
	}
}

func releaseORBranches(branches plannerORBranches) {
	for i := range branches {
		releasePredicates(branches[i].preds)
	}
}

func hasNoOrderLeadCandidatesOR(ops []qx.Expr) bool {
	for _, op := range ops {
		if !branchHasPositiveLeafOR(op) {
			return false
		}
	}
	return true
}

func branchHasPositiveLeafOR(e qx.Expr) bool {
	switch e.Op {
	case qx.OpAND:
		if e.Not || len(e.Operands) == 0 {
			return false
		}
		for _, ch := range e.Operands {
			if branchHasPositiveLeafOR(ch) {
				return true
			}
		}
		return false
	case qx.OpNOOP, qx.OpOR:
		return false
	default:
		if e.Not {
			return false
		}
		return true
	}
}

// buildORBranches compiles each OR branch into planner predicates and optional
// lead iterators used by OR execution strategies.
func (db *DB[K, V]) buildORBranches(ops []qx.Expr) (plannerORBranches, bool, bool) {
	out := make(plannerORBranches, 0, len(ops))

	for _, op := range ops {
		leaves, ok := collectAndLeaves(op)
		if !ok {
			releaseORBranches(out)
			return nil, false, false
		}

		preds, ok := db.buildPredicates(leaves)
		if !ok {
			releaseORBranches(out)
			return nil, false, false
		}
		plannerORSortPredicates(preds)

		branch := plannerORBranch{
			expr:  op,
			preds: preds,
		}

		hasFalse := false
		allTrue := true
		leadIdx := -1

		for i := range preds {
			p := &preds[i]
			if p.alwaysFalse {
				hasFalse = true
				break
			}
			if p.alwaysTrue {
				continue
			}
			allTrue = false
			if !p.hasContains() {
				releasePredicates(preds)
				releaseORBranches(out)
				return nil, false, false
			}
			if p.hasIter() {
				if leadIdx == -1 || p.estCard < preds[leadIdx].estCard {
					leadIdx = i
				}
			}
		}

		if hasFalse {
			releasePredicates(preds)
			continue
		}
		if allTrue {
			branch.alwaysTrue = true
		} else {
			branch.leadIdx = leadIdx
			if leadIdx >= 0 {
				branch.lead = &preds[leadIdx]
			}
		}
		out = append(out, branch)
	}

	if len(out) == 0 {
		return nil, true, true
	}

	return out, false, true
}

// execPlanOROrderBasic evaluates ordered OR by scanning ordered buckets and
// checking branch predicates per candidate.
//
// It keeps deterministic ordering semantics and avoids full OR unions for LIMIT-heavy queries.
func (db *DB[K, V]) execPlanOROrderBasic(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]K, bool) {
	o := q.Order[0]
	f := o.Field
	if f == "" {
		return nil, false
	}

	fm := db.fields[f]
	if fm == nil || fm.Slice {
		return nil, false
	}

	ov := db.fieldOverlay(f)
	if !ov.hasData() {
		return nil, false
	}

	alwaysTrue := false
	alwaysTrueBranch := -1
	for i := range branches {
		if branches[i].alwaysTrue {
			alwaysTrue = true
			alwaysTrueBranch = i
			break
		}
	}

	var (
		branchChecks [plannerORBranchLimit][]int
		checkBufs    [plannerORBranchLimit]*intSliceBuf
	)
	defer func() {
		for i := range branches {
			if checkBufs[i] != nil {
				checkBufs[i].values = branchChecks[i]
				releaseIntSliceBuf(checkBufs[i])
			}
		}
	}()
	for i := range branches {
		checkBufs[i] = getIntSliceBuf(len(branches[i].preds))
		branchChecks[i] = branches[i].buildMatchChecks(checkBufs[i].values)
	}

	skip := q.Offset
	need := int(q.Limit)
	out := make([]K, 0, need)

	var branchEvalOrder [plannerORBranchLimit]int
	branchEvalN := 0
	if !alwaysTrue {
		// Reordering branches is a fail-fast optimization: cheap/selective
		// branches are checked first for the common negative case.
		branchEvalOrder, branchEvalN = branches.evalOrder()
	}

	matches := func(idx uint64) bool {
		if alwaysTrue {
			return true
		}
		for i := 0; i < branchEvalN; i++ {
			bi := branchEvalOrder[i]
			if branches[bi].matchesChecks(idx, branchChecks[bi]) {
				return true
			}
		}
		return false
	}

	// Fast base-only path avoids overlay cursor overhead when no snapshot delta
	// is present and keeps this hot path allocation-light.
	if ov.delta == nil {
		s := ov.base
		if len(s) == 0 {
			return nil, true
		}

		if trace == nil {
			if !o.Desc {
				for i := 0; i < len(s); i++ {
					bm := s[i].IDs
					if bm.IsEmpty() {
						continue
					}
					stop := false
					bm.ForEach(func(idx uint64) bool {
						if !matches(idx) {
							return true
						}
						if skip > 0 {
							skip--
							return true
						}
						out = append(out, db.idFromIdxNoLock(idx))
						need--
						if need == 0 {
							stop = true
							return false
						}
						return true
					})
					if stop {
						return out, true
					}
				}
				return out, true
			}

			for i := len(s) - 1; i >= 0; i-- {
				bm := s[i].IDs
				if bm.IsEmpty() {
					if i == 0 {
						break
					}
					continue
				}
				stop := false
				bm.ForEach(func(idx uint64) bool {
					if !matches(idx) {
						return true
					}
					if skip > 0 {
						skip--
						return true
					}
					out = append(out, db.idFromIdxNoLock(idx))
					need--
					if need == 0 {
						stop = true
						return false
					}
					return true
				})
				if stop {
					return out, true
				}
				if i == 0 {
					break
				}
			}
			return out, true
		}

		branchMetrics := make([]TraceORBranch, len(branches))
		for i := range branchMetrics {
			branchMetrics[i].Index = i
		}

		matchWithMetrics := func(idx uint64) bool {
			if alwaysTrue {
				if alwaysTrueBranch >= 0 {
					branchMetrics[alwaysTrueBranch].RowsExamined++
					branchMetrics[alwaysTrueBranch].RowsEmitted++
				}
				return true
			}

			matchedCount := 0
			for i := range branches {
				branchMetrics[i].RowsExamined++
				if branches[i].matchesChecks(idx, branchChecks[i]) {
					branchMetrics[i].RowsEmitted++
					matchedCount++
				}
			}
			if matchedCount > 1 {
				trace.addDedupe(uint64(matchedCount - 1))
			}
			return matchedCount > 0
		}

		scanWidth := uint64(0)

		if !o.Desc {
			for i := 0; i < len(s); i++ {
				bm := s[i].IDs
				if bm.IsEmpty() {
					continue
				}
				scanWidth++
				trace.addExamined(bm.Cardinality())
				stop := false
				bm.ForEach(func(idx uint64) bool {
					if !matchWithMetrics(idx) {
						return true
					}
					if skip > 0 {
						skip--
						return true
					}
					out = append(out, db.idFromIdxNoLock(idx))
					need--
					if need == 0 {
						trace.addOrderScanWidth(scanWidth)
						trace.setORBranches(branchMetrics)
						trace.setEarlyStopReason("limit_reached")
						stop = true
						return false
					}
					return true
				})
				if stop {
					return out, true
				}
			}
			trace.addOrderScanWidth(scanWidth)
			trace.setORBranches(branchMetrics)
			trace.setEarlyStopReason("input_exhausted")
			return out, true
		}

		for i := len(s) - 1; i >= 0; i-- {
			bm := s[i].IDs
			if bm.IsEmpty() {
				if i == 0 {
					break
				}
				continue
			}
			scanWidth++
			trace.addExamined(bm.Cardinality())
			stop := false
			bm.ForEach(func(idx uint64) bool {
				if !matchWithMetrics(idx) {
					return true
				}
				if skip > 0 {
					skip--
					return true
				}
				out = append(out, db.idFromIdxNoLock(idx))
				need--
				if need == 0 {
					trace.addOrderScanWidth(scanWidth)
					trace.setORBranches(branchMetrics)
					trace.setEarlyStopReason("limit_reached")
					stop = true
					return false
				}
				return true
			})
			if stop {
				return out, true
			}
			if i == 0 {
				break
			}
		}

		trace.addOrderScanWidth(scanWidth)
		trace.setORBranches(branchMetrics)
		trace.setEarlyStopReason("input_exhausted")
		return out, true
	}

	// Overlay path for snapshots with accumulated delta.
	scratch := getRoaringBuf()
	defer releaseRoaringBuf(scratch)
	br := ov.rangeForBounds(rangeBounds{has: true})
	keyCur := ov.newCursor(br, o.Desc)

	if trace == nil {
		for {
			_, baseBM, de, ok := keyCur.next()
			if !ok {
				break
			}
			bm, owned := composePostingOwned(baseBM, de, scratch)
			if bm == nil || bm.IsEmpty() {
				if owned && bm != nil && bm != scratch {
					releaseRoaringBuf(bm)
				}
				continue
			}
			it := bm.Iterator()
			for it.HasNext() {
				idx := it.Next()
				if !matches(idx) {
					continue
				}
				if skip > 0 {
					skip--
					continue
				}
				out = append(out, db.idFromIdxNoLock(idx))
				need--
				if need == 0 {
					if owned && bm != scratch {
						releaseRoaringBuf(bm)
					}
					releaseRoaringBitmapIterator(it)
					return out, true
				}
			}
			releaseRoaringBitmapIterator(it)
			if owned && bm != scratch {
				releaseRoaringBuf(bm)
			}
		}
		return out, true
	}

	branchMetrics := make([]TraceORBranch, len(branches))
	for i := range branchMetrics {
		branchMetrics[i].Index = i
	}

	matchWithMetrics := func(idx uint64) bool {
		if alwaysTrue {
			if alwaysTrueBranch >= 0 {
				branchMetrics[alwaysTrueBranch].RowsExamined++
				branchMetrics[alwaysTrueBranch].RowsEmitted++
			}
			return true
		}

		matchedCount := 0
		for i := range branches {
			branchMetrics[i].RowsExamined++
			if branches[i].matchesChecks(idx, branchChecks[i]) {
				branchMetrics[i].RowsEmitted++
				matchedCount++
			}
		}
		if matchedCount > 1 {
			trace.addDedupe(uint64(matchedCount - 1))
		}
		return matchedCount > 0
	}

	scanWidth := uint64(0)

	for {
		_, baseBM, de, ok := keyCur.next()
		if !ok {
			break
		}
		bm, owned := composePostingOwned(baseBM, de, scratch)
		if bm == nil || bm.IsEmpty() {
			if owned && bm != nil && bm != scratch {
				releaseRoaringBuf(bm)
			}
			continue
		}
		scanWidth++
		trace.addExamined(bm.GetCardinality())
		it := bm.Iterator()
		for it.HasNext() {
			idx := it.Next()
			if !matchWithMetrics(idx) {
				continue
			}
			if skip > 0 {
				skip--
				continue
			}
			out = append(out, db.idFromIdxNoLock(idx))
			need--
			if need == 0 {
				trace.addOrderScanWidth(scanWidth)
				trace.setORBranches(branchMetrics)
				trace.setEarlyStopReason("limit_reached")
				if owned && bm != scratch {
					releaseRoaringBuf(bm)
				}
				releaseRoaringBitmapIterator(it)
				return out, true
			}
		}
		releaseRoaringBitmapIterator(it)
		if owned && bm != scratch {
			releaseRoaringBuf(bm)
		}
	}

	trace.addOrderScanWidth(scanWidth)
	trace.setORBranches(branchMetrics)
	trace.setEarlyStopReason("input_exhausted")
	return out, true
}

func orderWindow(q *qx.QX) (int, bool) {
	if q.Limit == 0 {
		return 0, false
	}

	need := q.Offset + q.Limit
	if need < q.Offset {
		return 0, false
	}
	if need > uint64(math.MaxInt) {
		return 0, false
	}
	return int(need), true
}

func (db *DB[K, V]) shouldPreferOROrderFallbackFirst(q *qx.QX, branches plannerORBranches) bool {
	d, ok := db.decideOROrderFallbackFirst(q, branches)
	return ok && d.prefer
}

func (db *DB[K, V]) decideOROrderFallbackFirst(q *qx.QX, branches plannerORBranches) (plannerOROrderFallbackDecision, bool) {
	need, ok := orderWindow(q)
	if !ok || need <= 0 {
		return plannerOROrderFallbackDecision{}, false
	}
	if len(q.Order) != 1 {
		return plannerOROrderFallbackDecision{}, false
	}

	routeCost, ok := db.estimateOROrderMergeRouteCost(q, branches, need)
	if !ok {
		return plannerOROrderFallbackDecision{}, false
	}
	avgChecks := branches.avgContainsChecks()
	if avgChecks <= 0 {
		avgChecks = 1
	}

	orderField := q.Order[0].Field
	ov := db.fieldOverlay(orderField)
	fallbackCollectFast := ov.hasData() && ov.delta == nil
	d := plannerOROrderFallbackDecision{
		routeCost:           routeCost,
		avgChecks:           avgChecks,
		fallbackCollectFast: fallbackCollectFast,
	}

	if !fallbackCollectFast {
		// Without fast branch collection fallback-first usually adds avoidable
		// allocations (branch subqueries + id->idx roundtrip).
		d.prefer = false
		d.reason = "order_delta_present"
		return d, true
	}

	fallbackGain := plannerOROrderFallbackFirstGainForShape(routeCost, q, need)
	offsetFallbackGain := plannerOROrderFallbackFirstOffsetGainForShape(routeCost, q, need)

	if routeCost.fallback <= routeCost.kWay*fallbackGain {
		d.prefer = true
		d.reason = "fallback_cost_better"
		return d, true
	}

	// Deep offsets are more sensitive to k-way tail behavior; for these windows
	// accept fallback-first even on near ties when shape indicates higher risk.
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
			(routeCost.hasSelectiveLead || routeCost.hasPrefixNonOrder) {
			d.prefer = true
			d.reason = "deep_offset_risk"
			return d, true
		}
		if routeCost.hasPrefixNonOrder && routeCost.fallback <= routeCost.kWay*offsetFallbackGain {
			d.prefer = true
			d.reason = "prefix_offset_near_tie"
			return d, true
		}
		if !routeCost.hasSelectiveLead && routeCost.overlap >= 1.2 &&
			routeCost.fallback <= routeCost.kWay*1.25 {
			d.prefer = true
			d.reason = "overlap_offset_near_tie"
			return d, true
		}
	}

	d.prefer = false
	d.reason = "kway_preferred"
	return d, true
}

func (db *DB[K, V]) shouldUseOROrderKWayRuntimeFallback(q *qx.QX, branches plannerORBranches, needWindow int) bool {
	d, ok := db.decideOROrderKWayRuntimeFallback(q, branches, needWindow)
	return ok && d.enable
}

func (db *DB[K, V]) decideOROrderKWayRuntimeFallback(q *qx.QX, branches plannerORBranches, needWindow int) (plannerOROrderRuntimeGuardDecision, bool) {
	if needWindow <= 0 || len(q.Order) != 1 || len(branches) < 2 {
		return plannerOROrderRuntimeGuardDecision{}, false
	}

	orderField := q.Order[0].Field
	if orderField == "" {
		return plannerOROrderRuntimeGuardDecision{}, false
	}

	routeCost, ok := db.estimateOROrderMergeRouteCost(q, branches, needWindow)
	if !ok {
		return plannerOROrderRuntimeGuardDecision{}, false
	}

	avgChecks := branches.avgContainsChecks()
	if avgChecks <= 0 {
		avgChecks = 1
	}
	shape := plannerORKWayRuntimeShape{
		overlap:   routeCost.overlap,
		avgChecks: avgChecks,
		offset:    q.Offset,
	}
	d := plannerOROrderRuntimeGuardDecision{
		routeCost: routeCost,
		avgChecks: avgChecks,
	}
	minNeed := plannerORKWayRuntimeNeedMinForShape(shape)
	nearTieGain := plannerORKWayRuntimeNearTieGain(shape)

	// Small unpaged windows where k-way is clearly cheaper don't benefit from
	// runtime switching logic.
	if q.Offset == 0 && needWindow < minNeed &&
		routeCost.kWay < routeCost.fallback*0.9 {
		d.enable = false
		d.reason = "small_window_kway_clear"
		return d, true
	}

	hasPrefixNonOrder := branches.hasPrefixOnNonOrderField(orderField)
	if hasPrefixNonOrder && q.Offset > 0 {
		d.enable = true
		d.reason = "prefix_offset_shape"
		return d, true
	}

	if routeCost.kWay >= routeCost.fallback*nearTieGain {
		d.enable = true
		d.reason = "near_tie_cost"
		return d, true
	}
	if q.Offset > 0 && avgChecks >= plannerORKWayRuntimeAvgChecksEnableMin {
		d.enable = true
		d.reason = "offset_with_checks"
		return d, true
	}
	d.enable = false
	d.reason = "guard_not_needed"
	return d, true
}

func plannerORKWayShouldFallbackRuntime(needWindow int, pops int, unique uint64, examined uint64) bool {
	return plannerORKWayShouldFallbackRuntimeDetailed(needWindow, pops, unique, examined).fallback
}

func plannerORKWayShouldFallbackRuntimeDetailed(needWindow int, pops int, unique uint64, examined uint64) plannerORKWayRuntimeDecision {
	return plannerORKWayShouldFallbackRuntimeDetailedWithShape(
		needWindow, pops, unique, examined, plannerORKWayRuntimeShape{
			overlap:   1,
			avgChecks: 1,
		},
	)
}

func plannerORKWayShouldFallbackRuntimeDetailedWithShape(needWindow, pops int, unique, examined uint64, shape plannerORKWayRuntimeShape) plannerORKWayRuntimeDecision {
	d := plannerORKWayRuntimeDecision{
		reason: "not_enough_sample",
	}
	if shape.overlap <= 0 {
		shape.overlap = 1
	}
	if shape.avgChecks <= 0 {
		shape.avgChecks = 1
	}
	minNeed := plannerORKWayRuntimeNeedMinForShape(shape)
	if needWindow < minNeed || pops <= 0 {
		return d
	}
	minPops := max(plannerORKWayRuntimeMinPops, min(plannerORKWayRuntimeLowOverlapMinPops, needWindow/64))
	if shape.overlap >= 2.0 {
		minPops = minPops * 9 / 10
	}
	if shape.avgChecks >= 2.5 {
		minPops = minPops * 9 / 10
	}
	if shape.offset > 0 {
		minPops = minPops * 9 / 10
	}
	if shape.overlap < 1.2 && shape.avgChecks < 1.5 && shape.offset == 0 {
		minPops = minPops * 9 / 8
	}
	if minPops < 24 {
		minPops = 24
	}
	if minPops > 512 {
		minPops = 512
	}
	if pops < minPops {
		d.reason = "too_few_pops"
		return d
	}
	minUnique := uint64(max(plannerORKWayRuntimeMinUnique, minPops/4))
	if unique < minUnique {
		d.reason = "too_few_unique"
		return d
	}
	baseMinExamined := uint64(plannerORKWayRuntimeMinExamined)
	examinedPerPop := 128.0
	if shape.overlap >= 2.0 || shape.avgChecks >= 2.5 {
		examinedPerPop *= 0.88
	} else if shape.overlap < 1.2 && shape.avgChecks < 1.5 {
		examinedPerPop *= 1.12
	}
	if shape.offset > 0 {
		examinedPerPop *= 0.92
	}
	minExamined := max(baseMinExamined, uint64(float64(minPops)*examinedPerPop))
	if examined < minExamined {
		d.reason = "too_few_examined"
		return d
	}

	examinedPerUnique := float64(examined) / float64(unique)
	d.examinedPerUnique = examinedPerUnique
	examinedPerUniqueMin := plannerORKWayRuntimeExaminedPerUniqueMin
	switch {
	case needWindow < plannerOROrderMergeNeedMax/2:
		examinedPerUniqueMin *= 1.5
	case needWindow < plannerORKWayRuntimeLowOverlapNeedMin:
		examinedPerUniqueMin *= 1.2
	default:
		examinedPerUniqueMin *= 0.75
	}
	if shape.overlap >= 2.0 {
		examinedPerUniqueMin *= 0.92
	}
	if shape.avgChecks >= 2.5 {
		examinedPerUniqueMin *= 0.92
	}
	if shape.offset > 0 {
		examinedPerUniqueMin *= 0.95
	}
	if shape.overlap < 1.2 && shape.avgChecks < 1.5 && shape.offset == 0 {
		examinedPerUniqueMin *= 1.1
	}
	if examinedPerUniqueMin < 256 {
		examinedPerUniqueMin = 256
	}

	// Large-window low-overlap streams can still be expensive even when
	// examined/unique is below the generic threshold. Detect this shape
	// separately to avoid walking deep offsets via k-way merge.
	lowOverlapMinPops := max(
		plannerORKWayRuntimeLowOverlapMinPops,
		min(plannerORKWayRuntimeLowOverlapMinPops*8, needWindow/128),
	)
	lowOverlapProjectedPopsMax := max(
		plannerORKWayRuntimeLowOverlapProjectedPopsMax,
		float64(needWindow)*0.8,
	)
	lowOverlapExaminedPerUnique := plannerORKWayRuntimeLowOverlapExaminedPerUnique
	if needWindow >= plannerORKWayRuntimeLowOverlapNeedMin*4 {
		lowOverlapExaminedPerUnique *= 0.75
	}
	if needWindow >= plannerORKWayRuntimeLowOverlapNeedMin &&
		pops >= lowOverlapMinPops &&
		examinedPerUnique >= lowOverlapExaminedPerUnique {
		uniquePerPop := float64(unique) / float64(pops)
		if uniquePerPop >= plannerORKWayRuntimeLowOverlapMinUniquePerPop {
			projectedPops := (float64(pops) / float64(unique)) * float64(needWindow)
			if projectedPops >= lowOverlapProjectedPopsMax {
				d.fallback = true
				d.reason = "low_overlap_projected_pops"
				return d
			}
		}
	}

	if examinedPerUnique < examinedPerUniqueMin {
		d.reason = "examined_per_unique_ok"
		return d
	}

	projectedExamined := examinedPerUnique * float64(needWindow)
	projectedExaminedMax := max(
		plannerORKWayRuntimeProjectedExaminedMax,
		float64(needWindow)*48.0,
	)
	if shape.overlap >= 2.0 || shape.avgChecks >= 2.5 {
		projectedExaminedMax *= 0.92
	}
	if shape.offset > 0 {
		projectedExaminedMax *= 0.95
	}
	if shape.overlap < 1.2 && shape.avgChecks < 1.5 && shape.offset == 0 {
		projectedExaminedMax *= 1.08
	}
	minProjectedExaminedMax := float64(needWindow) * 24.0
	if projectedExaminedMax < minProjectedExaminedMax {
		projectedExaminedMax = minProjectedExaminedMax
	}
	d.projectedExamined = projectedExamined
	d.projectedExaminedMax = projectedExaminedMax
	if projectedExamined >= projectedExaminedMax {
		d.fallback = true
		d.reason = "projected_examined_limit"
		return d
	}
	d.reason = "projected_examined_ok"
	return d
}

func (db *DB[K, V]) execPlanOROrderMerge(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]K, bool, error) {
	fallbackDec, decOK := db.decideOROrderFallbackFirst(q, branches)
	if trace != nil && decOK {
		route := "kway_first"
		if fallbackDec.prefer {
			route = "fallback_first"
		}
		trace.setOROrderRouteDecision(
			route,
			fallbackDec.reason,
			fallbackDec.routeCost,
			fallbackDec.avgChecks,
			fallbackDec.fallbackCollectFast,
		)
	}

	if decOK && fallbackDec.prefer {
		return db.execPlanOROrderMergeFallback(q, branches, trace)
	}
	out, ok, err := db.execPlanOROrderKWay(q, branches, trace)
	if ok || err != nil {
		return out, ok, err
	}
	return db.execPlanOROrderMergeFallback(q, branches, trace)
}

type plannerOROrderMergeItem struct {
	branch int
	bucket int
	idx    uint64
}

type plannerOROrderMergeHeap struct {
	desc  bool
	items []plannerOROrderMergeItem
}

func (h *plannerOROrderMergeHeap) len() int { return len(h.items) }

func (h *plannerOROrderMergeHeap) less(i, j int) bool {
	a := h.items[i]
	b := h.items[j]
	if a.bucket != b.bucket {
		if h.desc {
			return a.bucket > b.bucket
		}
		return a.bucket < b.bucket
	}
	if a.idx != b.idx {
		// Keep deterministic in-bucket ordering aligned with roaring iterator.
		return a.idx < b.idx
	}
	return a.branch < b.branch
}

func (h *plannerOROrderMergeHeap) swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

func (h *plannerOROrderMergeHeap) push(x plannerOROrderMergeItem) {
	h.items = append(h.items, x)
	h.up(len(h.items) - 1)
}

func (h *plannerOROrderMergeHeap) pop() plannerOROrderMergeItem {
	n := len(h.items) - 1
	h.swap(0, n)
	x := h.items[n]
	h.items = h.items[:n]
	if len(h.items) > 0 {
		h.down(0)
	}
	return x
}

func (h *plannerOROrderMergeHeap) up(j int) {
	for {
		i := (j - 1) / 2
		if i == j || !h.less(j, i) {
			break
		}
		h.swap(i, j)
		j = i
	}
}

func (h *plannerOROrderMergeHeap) down(i0 int) {
	n := len(h.items)
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < n && h.less(j2, j1) {
			j = j2
		}
		if !h.less(j, i) {
			break
		}
		h.swap(i, j)
		i = j
	}
}

type plannerOROrderBranchIter struct {
	branch         *plannerORBranch
	checks         []int
	exactChecks    []int
	buckets        []index
	desc           bool
	single         int
	allChecksExact bool

	startBucket int
	endBucket   int

	nextBucket int
	curBucket  int
	curIter    roaringIter
	curExact   bool
	bucketWork *roaring64.Bitmap

	onBucketVisit func(int)

	totalExamined  *uint64
	branchExamined *uint64
	branchEmitted  *uint64

	has bool
	cur uint64
}

type plannerOrderIndexView struct {
	slice   []index
	release func()
}

func (v plannerOrderIndexView) close() {
	if v.release != nil {
		v.release()
	}
}

// plannerOrderIndexSnapshotView returns an order-index slice that reflects the
// current snapshot state (base + delta). For delta-backed entries it keeps
// track of owned composed bitmaps and releases them when view.close() is called.
func (db *DB[K, V]) plannerOrderIndexSnapshotView(field string) (plannerOrderIndexView, bool) {
	ov := db.fieldOverlay(field)
	if !ov.hasData() {
		return plannerOrderIndexView{}, false
	}
	if ov.delta == nil {
		return plannerOrderIndexView{slice: ov.base}, true
	}

	br := ov.rangeForBounds(rangeBounds{has: true})
	if br.baseStart >= br.baseEnd && br.deltaStart >= br.deltaEnd {
		return plannerOrderIndexView{slice: make([]index, 0)}, true
	}

	out := make([]index, 0, (br.baseEnd-br.baseStart)+(br.deltaEnd-br.deltaStart))
	var owned []*roaring64.Bitmap
	cur := ov.newCursor(br, false)
	for {
		key, baseIDs, de, ok := cur.next()
		if !ok {
			break
		}
		if deltaEntryIsEmpty(de) {
			if baseIDs.IsEmpty() {
				continue
			}
			out = append(out, index{Key: key, IDs: baseIDs})
			continue
		}

		bm, isOwned := composePostingOwned(baseIDs, de, nil)
		if bm == nil || bm.IsEmpty() {
			if isOwned && bm != nil {
				releaseRoaringBuf(bm)
			}
			continue
		}

		if isOwned {
			// Keep singleton entries compact and return bitmap buffer to pool.
			if bm.GetCardinality() == 1 {
				out = append(out, index{Key: key, IDs: postingList{bm: postingSingleFlag, single: bm.Minimum()}})
				releaseRoaringBuf(bm)
				continue
			}
			out = append(out, index{Key: key, IDs: postingList{bm: bm}})
			owned = append(owned, bm)
			continue
		}

		out = append(out, index{Key: key, IDs: postingFromBitmapViewAdaptive(bm)})
	}

	return plannerOrderIndexView{
		slice: out,
		release: func() {
			for _, bm := range owned {
				if bm != nil {
					releaseRoaringBuf(bm)
				}
			}
		},
	}, true
}

func (it *plannerOROrderBranchIter) init() {
	if it.startBucket < 0 {
		it.startBucket = 0
	}
	if it.endBucket <= 0 || it.endBucket > len(it.buckets) {
		it.endBucket = len(it.buckets)
	}
	if it.endBucket < it.startBucket {
		it.endBucket = it.startBucket
	}
	if it.desc {
		it.nextBucket = it.endBucket - 1
	} else {
		it.nextBucket = it.startBucket
	}
	it.curBucket = -1
}

func (it *plannerOROrderBranchIter) close() {
	if it.curIter != nil {
		releaseRoaringIter(it.curIter)
		it.curIter = nil
	}
	if it.bucketWork != nil {
		releaseRoaringBuf(it.bucketWork)
		it.bucketWork = nil
	}
}

func (it *plannerOROrderBranchIter) advance() bool {
	it.has = false
	for {
		if it.curIter != nil {
			if it.curExact {
				if it.curIter.HasNext() {
					it.cur = it.curIter.Next()
					it.has = true
					return true
				}
				releaseRoaringIter(it.curIter)
				it.curIter = nil
				it.curExact = false
				continue
			}
			for it.curIter.HasNext() {
				idx := it.curIter.Next()
				if it.totalExamined != nil {
					*it.totalExamined = *it.totalExamined + 1
				}
				if it.branchExamined != nil {
					*it.branchExamined = *it.branchExamined + 1
				}
				if it.branch.matchesChecks(idx, it.checks) {
					it.cur = idx
					it.has = true
					if it.branchEmitted != nil {
						*it.branchEmitted = *it.branchEmitted + 1
					}
					return true
				}
			}
			releaseRoaringIter(it.curIter)
			it.curIter = nil
		}

		if it.desc {
			if it.nextBucket < it.startBucket {
				return false
			}
			b := it.nextBucket
			it.nextBucket--
			bucket := it.buckets[b].IDs
			if bucket.IsEmpty() {
				continue
			}
			if it.onBucketVisit != nil {
				it.onBucketVisit(b)
			}
			it.curBucket = b
			if bucket.isSingleton() {
				idx := bucket.single
				if it.totalExamined != nil {
					*it.totalExamined = *it.totalExamined + 1
				}
				if it.branchExamined != nil {
					*it.branchExamined = *it.branchExamined + 1
				}
				matched := false
				if it.single >= 0 {
					matched = it.branch.preds[it.single].matches(idx)
				} else {
					matched = it.branch.matchesChecks(idx, it.checks)
				}
				if !matched {
					continue
				}
				it.cur = idx
				it.has = true
				if it.branchEmitted != nil {
					*it.branchEmitted = *it.branchEmitted + 1
				}
				return true
			}
			bm := bucket.bitmap()
			if len(it.exactChecks) > 0 && it.bucketWork == nil {
				it.bucketWork = getRoaringBuf()
			}
			if len(it.exactChecks) > 0 {
				mode, exactBM, card := plannerFilterBitmapByChecks(it.branch.preds, it.exactChecks, bm, it.bucketWork, true)
				switch mode {
				case plannerPredicateBucketEmpty:
					if it.totalExamined != nil {
						*it.totalExamined = *it.totalExamined + card
					}
					if it.branchExamined != nil {
						*it.branchExamined = *it.branchExamined + card
					}
					continue
				case plannerPredicateBucketAll:
					if it.allChecksExact {
						if it.totalExamined != nil {
							*it.totalExamined = *it.totalExamined + card
						}
						if it.branchExamined != nil {
							*it.branchExamined = *it.branchExamined + card
						}
						if it.branchEmitted != nil {
							*it.branchEmitted = *it.branchEmitted + card
						}
						it.curIter = exactBM.Iterator()
						it.curExact = true
						continue
					}
					it.curIter = exactBM.Iterator()
					it.curExact = false
					continue
				case plannerPredicateBucketExact:
					if it.allChecksExact {
						if it.totalExamined != nil {
							*it.totalExamined = *it.totalExamined + card
						}
						if it.branchExamined != nil {
							*it.branchExamined = *it.branchExamined + card
						}
						if it.branchEmitted != nil {
							*it.branchEmitted = *it.branchEmitted + exactBM.GetCardinality()
						}
						it.curIter = exactBM.Iterator()
						it.curExact = true
						continue
					}
					it.curIter = exactBM.Iterator()
					it.curExact = false
					continue
				}
			}
			it.curIter = bm.Iterator()
			it.curExact = false
			continue
		}

		if it.nextBucket >= it.endBucket {
			return false
		}
		b := it.nextBucket
		it.nextBucket++
		bucket := it.buckets[b].IDs
		if bucket.IsEmpty() {
			continue
		}
		if it.onBucketVisit != nil {
			it.onBucketVisit(b)
		}
		it.curBucket = b
		if bucket.isSingleton() {
			idx := bucket.single
			if it.totalExamined != nil {
				*it.totalExamined = *it.totalExamined + 1
			}
			if it.branchExamined != nil {
				*it.branchExamined = *it.branchExamined + 1
			}
			matched := false
			if it.single >= 0 {
				matched = it.branch.preds[it.single].matches(idx)
			} else {
				matched = it.branch.matchesChecks(idx, it.checks)
			}
			if !matched {
				continue
			}
			it.cur = idx
			it.has = true
			if it.branchEmitted != nil {
				*it.branchEmitted = *it.branchEmitted + 1
			}
			return true
		}
		bm := bucket.bitmap()
		if len(it.exactChecks) > 0 && it.bucketWork == nil {
			it.bucketWork = getRoaringBuf()
		}
		if len(it.exactChecks) > 0 {
			mode, exactBM, card := plannerFilterBitmapByChecks(it.branch.preds, it.exactChecks, bm, it.bucketWork, true)
			switch mode {
			case plannerPredicateBucketEmpty:
				if it.totalExamined != nil {
					*it.totalExamined = *it.totalExamined + card
				}
				if it.branchExamined != nil {
					*it.branchExamined = *it.branchExamined + card
				}
				continue
			case plannerPredicateBucketAll:
				if it.allChecksExact {
					if it.totalExamined != nil {
						*it.totalExamined = *it.totalExamined + card
					}
					if it.branchExamined != nil {
						*it.branchExamined = *it.branchExamined + card
					}
					if it.branchEmitted != nil {
						*it.branchEmitted = *it.branchEmitted + card
					}
					it.curIter = exactBM.Iterator()
					it.curExact = true
					continue
				}
				it.curIter = exactBM.Iterator()
				it.curExact = false
				continue
			case plannerPredicateBucketExact:
				if it.allChecksExact {
					if it.totalExamined != nil {
						*it.totalExamined = *it.totalExamined + card
					}
					if it.branchExamined != nil {
						*it.branchExamined = *it.branchExamined + card
					}
					if it.branchEmitted != nil {
						*it.branchEmitted = *it.branchEmitted + exactBM.GetCardinality()
					}
					it.curIter = exactBM.Iterator()
					it.curExact = true
					continue
				}
				it.curIter = exactBM.Iterator()
				it.curExact = false
				continue
			}
		}
		it.curIter = bm.Iterator()
		it.curExact = false
	}
}

func (db *DB[K, V]) execPlanOROrderKWay(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]K, bool, error) {
	needWindow, ok := orderWindow(q)
	if !ok || needWindow <= 0 {
		return nil, false, nil
	}

	o := q.Order[0]
	fm := db.fields[o.Field]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}
	orderView, ok := db.plannerOrderIndexSnapshotView(o.Field)
	if !ok {
		return nil, false, nil
	}
	defer orderView.close()
	s := orderView.slice
	if len(s) == 0 {
		return nil, true, nil
	}

	for i := range branches {
		if branches[i].alwaysTrue {
			return nil, false, nil
		}
	}
	allowRuntimeFallback := false
	runtimeShape := plannerORKWayRuntimeShape{
		overlap:   1,
		avgChecks: 1,
	}
	if guardDec, ok := db.decideOROrderKWayRuntimeFallback(q, branches, needWindow); ok {
		allowRuntimeFallback = guardDec.enable
		runtimeShape = plannerORKWayRuntimeShapeFromGuard(guardDec, q)
		if trace != nil {
			trace.setOROrderRuntimeGuard(guardDec.enable, guardDec.reason)
		}
	}

	var (
		branchMetrics []TraceORBranch
		examined      uint64
		scanWidth     uint64
		dedupe        uint64
	)
	var (
		branchChecks      [plannerORBranchLimit][]int
		branchExactChecks [plannerORBranchLimit][]int
		checkBufs         [plannerORBranchLimit]*intSliceBuf
		exactCheckBufs    [plannerORBranchLimit]*intSliceBuf
	)
	defer func() {
		for i := range branches {
			if checkBufs[i] != nil {
				checkBufs[i].values = branchChecks[i]
				releaseIntSliceBuf(checkBufs[i])
			}
			if exactCheckBufs[i] != nil {
				exactCheckBufs[i].values = branchExactChecks[i]
				releaseIntSliceBuf(exactCheckBufs[i])
			}
		}
	}()
	var examinedPtr *uint64
	if trace != nil {
		branchMetrics = make([]TraceORBranch, len(branches))
		for i := range branchMetrics {
			branchMetrics[i].Index = i
		}
		examinedPtr = &examined
	}

	var bucketSeen []bool
	if trace != nil {
		bucketSeen = make([]bool, len(s))
	}
	markBucketVisited := func(bucket int) {
		if trace == nil || bucket < 0 || bucket >= len(bucketSeen) {
			return
		}
		if bucketSeen[bucket] {
			return
		}
		bucketSeen[bucket] = true
		scanWidth++
	}

	var (
		iters         []plannerOROrderBranchIter
		itersBuf      *plannerOROrderIterSliceBuf
		mergeItems    []plannerOROrderMergeItem
		mergeItemsBuf *plannerOROrderMergeItemSliceBuf
	)
	if len(branches) <= 8 {
		var itersInline [8]plannerOROrderBranchIter
		iters = itersInline[:len(branches)]
		var mergeItemsInline [8]plannerOROrderMergeItem
		mergeItems = mergeItemsInline[:0]
	} else {
		itersBuf = getPlannerOROrderIterSliceBuf(len(branches))
		if cap(itersBuf.values) < len(branches) {
			itersBuf.values = make([]plannerOROrderBranchIter, 0, len(branches))
		}
		iters = itersBuf.values[:len(branches)]
		clear(iters)
		defer func() {
			clear(iters)
			itersBuf.values = iters[:0]
			releasePlannerOROrderIterSliceBuf(itersBuf)
		}()

		mergeItemsBuf = getPlannerOROrderMergeItemSliceBuf(len(branches))
		if cap(mergeItemsBuf.values) < len(branches) {
			mergeItemsBuf.values = make([]plannerOROrderMergeItem, 0, len(branches))
		}
		mergeItems = mergeItemsBuf.values[:0]
		defer func() {
			clear(mergeItems)
			mergeItemsBuf.values = mergeItems[:0]
			releasePlannerOROrderMergeItemSliceBuf(mergeItemsBuf)
		}()
	}
	defer func() {
		for i := range iters {
			iters[i].close()
		}
	}()

	h := &plannerOROrderMergeHeap{
		desc:  o.Desc,
		items: mergeItems,
	}

	for i := range branches {
		branchStart, branchEnd, covered, rangeOK := db.extractOrderRangeCoverage(o.Field, branches[i].preds, s)
		if !rangeOK {
			return nil, false, nil
		}
		for pi := range covered {
			if covered[pi] {
				branches[i].preds[pi].covered = true
			}
		}
		if branchStart >= branchEnd {
			if trace != nil {
				branchMetrics[i].Skipped = true
				branchMetrics[i].SkipReason = "order_range_empty"
			}
			continue
		}

		var bePtr *uint64
		var bmPtr *uint64
		if trace != nil {
			bePtr = &branchMetrics[i].RowsExamined
			bmPtr = &branchMetrics[i].RowsEmitted
		}
		checkBufs[i] = getIntSliceBuf(len(branches[i].preds))
		branchChecks[i] = branches[i].buildMatchChecks(checkBufs[i].values)
		exactCheckBufs[i] = getIntSliceBuf(len(branchChecks[i]))
		branchExactChecks[i] = branches[i].buildBitmapFilterChecks(exactCheckBufs[i].values, branchChecks[i])
		singleCheck := -1
		if len(branchChecks[i]) == 1 {
			singleCheck = branchChecks[i][0]
		}
		iters[i] = plannerOROrderBranchIter{
			branch:         &branches[i],
			checks:         branchChecks[i],
			exactChecks:    branchExactChecks[i],
			buckets:        s,
			desc:           o.Desc,
			single:         singleCheck,
			allChecksExact: len(branchChecks[i]) > 0 && len(branchExactChecks[i]) == len(branchChecks[i]),
			startBucket:    branchStart,
			endBucket:      branchEnd,
			onBucketVisit:  markBucketVisited,
			totalExamined:  examinedPtr,
			branchExamined: bePtr,
			branchEmitted:  bmPtr,
		}
		iters[i].init()
		if iters[i].advance() {
			h.push(plannerOROrderMergeItem{
				branch: i,
				bucket: iters[i].curBucket,
				idx:    iters[i].cur,
			})
		} else if trace != nil {
			branchMetrics[i].Skipped = true
			branchMetrics[i].SkipReason = "stream_empty"
		}
	}

	if h.len() == 0 {
		if trace != nil {
			trace.addExamined(examined)
			trace.addOrderScanWidth(scanWidth)
			trace.setORBranches(branchMetrics)
			trace.setEarlyStopReason("no_candidates")
		}
		return nil, true, nil
	}

	outCap := int(q.Limit)
	if outCap <= 0 {
		outCap = needWindow
	}
	out := make([]K, 0, outCap)

	seen := newU64Set(max(64, needWindow*2))
	skip := q.Offset
	needOut := q.Limit
	stopReason := "input_exhausted"
	pops := 0
	unique := uint64(0)

	for h.len() > 0 {
		item := h.pop()
		bi := item.branch
		pops++

		if iters[bi].advance() {
			h.push(plannerOROrderMergeItem{
				branch: bi,
				bucket: iters[bi].curBucket,
				idx:    iters[bi].cur,
			})
		}

		if !seen.Add(item.idx) {
			dedupe++
			continue
		}
		unique++
		if skip > 0 {
			skip--
			if allowRuntimeFallback {
				rt := plannerORKWayShouldFallbackRuntimeDetailedWithShape(needWindow, pops, unique, examined, runtimeShape)
				if rt.fallback {
					if trace != nil {
						trace.setOROrderRuntimeFallback(
							rt.reason,
							rt.examinedPerUnique,
							rt.projectedExamined,
							rt.projectedExaminedMax,
						)
					}
					return nil, false, nil
				}
			}
			continue
		}

		out = append(out, db.idFromIdxNoLock(item.idx))
		needOut--
		if needOut == 0 {
			stopReason = "limit_reached"
			break
		}
		if allowRuntimeFallback {
			rt := plannerORKWayShouldFallbackRuntimeDetailedWithShape(needWindow, pops, unique, examined, runtimeShape)
			if rt.fallback {
				if trace != nil {
					trace.setOROrderRuntimeFallback(
						rt.reason,
						rt.examinedPerUnique,
						rt.projectedExamined,
						rt.projectedExaminedMax,
					)
				}
				return nil, false, nil
			}
		}
	}

	if trace != nil {
		trace.addExamined(examined)
		trace.addDedupe(dedupe)
		trace.addOrderScanWidth(scanWidth)
		trace.setORBranches(branchMetrics)
		trace.setEarlyStopReason(stopReason)
	}
	return out, true, nil
}

func (db *DB[K, V]) execPlanOROrderMergeFallback(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]K, bool, error) {
	need, ok := orderWindow(q)
	if !ok || need <= 0 {
		return nil, false, nil
	}

	candidateBM := getRoaringBuf()
	defer releaseRoaringBuf(candidateBM)

	var branchMetrics []TraceORBranch
	if trace != nil {
		branchMetrics = make([]TraceORBranch, len(branches))
		for i := range branchMetrics {
			branchMetrics[i].Index = i
		}
	}

	orderField := q.Order[0].Field
	directBranchCollect := false
	if orderField != "" {
		// Direct branch collection relies on a stable base order slice.
		// With order-field delta we keep legacy subquery path.
		ov := db.fieldOverlay(orderField)
		directBranchCollect = ov.hasData() && ov.delta == nil
	}

	for i := range branches {
		branch := branches[i]
		if branch.alwaysTrue {
			return nil, false, nil
		}

		branchLimit, branchExhaustive, err := db.planOROrderMergeBranchLimit(branch, need)
		if err != nil {
			return nil, true, err
		}
		if branchLimit <= 0 {
			if trace != nil {
				branchMetrics[i].Skipped = true
				branchMetrics[i].SkipReason = "branch_limit_zero"
			}
			continue
		}

		if directBranchCollect {
			emitted, examined, dedupe, okCollect := db.collectOROrderFallbackBranchCandidates(
				&branches[i], q.Order[0], branchLimit, candidateBM,
			)
			if okCollect {
				if trace != nil {
					trace.addExamined(examined)
					trace.addDedupe(dedupe)
					branchMetrics[i].RowsExamined += examined
					branchMetrics[i].RowsEmitted += emitted
				}
				if emitted == 0 {
					continue
				}
				continue
			}
		}

		subQ := &qx.QX{
			Expr:  branch.expr,
			Limit: uint64(branchLimit),
		}
		if !branchExhaustive {
			subQ.Order = q.Order
		}

		ids, err := db.execPreparedQuery(subQ)
		if err != nil {
			return nil, true, err
		}
		if trace != nil {
			trace.addExamined(uint64(len(ids)))
			branchMetrics[i].RowsExamined += uint64(len(ids))
			branchMetrics[i].RowsEmitted += uint64(len(ids))
		}
		if len(ids) == 0 {
			continue
		}

		db.forEachIdxFromID(ids, func(idx uint64) {
			if candidateBM.CheckedAdd(idx) {
				return
			}
			if trace != nil {
				trace.addDedupe(1)
			}
		})
	}

	if candidateBM.IsEmpty() {
		if trace != nil {
			trace.setORBranches(branchMetrics)
			trace.setEarlyStopReason("no_candidates")
		}
		return nil, true, nil
	}

	o := q.Order[0]
	fm := db.fields[o.Field]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}
	orderView, ok := db.plannerOrderIndexSnapshotView(o.Field)
	if !ok {
		return nil, false, nil
	}
	defer orderView.close()
	s := orderView.slice
	if len(s) == 0 {
		return nil, true, nil
	}

	skip := q.Offset
	needOut := q.Limit
	if needOut == 0 {
		return nil, true, nil
	}

	totalCandidates := candidateBM.GetCardinality()
	out := make([]K, 0, int(min(needOut, totalCandidates)))

	seenCandidates := uint64(0)
	scanWidth := uint64(0)

	emit := func(idx uint64) bool {
		if !candidateBM.Contains(idx) {
			return false
		}
		seenCandidates++
		if skip > 0 {
			skip--
			return false
		}
		out = append(out, db.idFromIdxNoLock(idx))
		needOut--
		return needOut == 0
	}

	if !o.Desc {
		for i := 0; i < len(s); i++ {
			bm := s[i].IDs
			if bm.IsEmpty() || !bm.IntersectsBitmap(candidateBM) {
				continue
			}
			scanWidth++
			if bm.Cardinality() == 1 {
				idx, _ := bm.Minimum()
				if emit(idx) {
					if trace != nil {
						trace.addExamined(seenCandidates)
						trace.addOrderScanWidth(scanWidth)
						trace.setORBranches(branchMetrics)
						trace.setEarlyStopReason("limit_reached")
					}
					return out, true, nil
				}
				if seenCandidates == totalCandidates {
					break
				}
				continue
			}
			stop := false
			bm.ForEach(func(idx uint64) bool {
				if emit(idx) {
					if trace != nil {
						trace.addExamined(seenCandidates)
						trace.addOrderScanWidth(scanWidth)
						trace.setORBranches(branchMetrics)
						trace.setEarlyStopReason("limit_reached")
					}
					stop = true
					return false
				}
				return true
			})
			if stop {
				return out, true, nil
			}
			if seenCandidates == totalCandidates {
				break
			}
		}
	} else {
		for i := len(s) - 1; i >= 0; i-- {
			bm := s[i].IDs
			if bm.IsEmpty() || !bm.IntersectsBitmap(candidateBM) {
				if i == 0 {
					break
				}
				continue
			}
			scanWidth++
			if bm.Cardinality() == 1 {
				idx, _ := bm.Minimum()
				if emit(idx) {
					if trace != nil {
						trace.addExamined(seenCandidates)
						trace.addOrderScanWidth(scanWidth)
						trace.setORBranches(branchMetrics)
						trace.setEarlyStopReason("limit_reached")
					}
					return out, true, nil
				}
				if seenCandidates == totalCandidates {
					break
				}
				if i == 0 {
					break
				}
				continue
			}
			stop := false
			bm.ForEach(func(idx uint64) bool {
				if emit(idx) {
					if trace != nil {
						trace.addExamined(seenCandidates)
						trace.addOrderScanWidth(scanWidth)
						trace.setORBranches(branchMetrics)
						trace.setEarlyStopReason("limit_reached")
					}
					stop = true
					return false
				}
				return true
			})
			if stop {
				return out, true, nil
			}
			if seenCandidates == totalCandidates {
				break
			}
			if i == 0 {
				break
			}
		}
	}

	if trace != nil {
		trace.addExamined(seenCandidates)
		trace.addOrderScanWidth(scanWidth)
		trace.setORBranches(branchMetrics)
		if seenCandidates == totalCandidates {
			trace.setEarlyStopReason("candidates_exhausted")
		} else {
			trace.setEarlyStopReason("order_index_exhausted")
		}
	}
	return out, true, nil
}

// collectOROrderFallbackBranchCandidates collects up to branchLimit matching ids
// from one OR branch directly from the base order index and writes them into dst.
// Returns ok=false when fast-path preconditions are not satisfied.
func (db *DB[K, V]) collectOROrderFallbackBranchCandidates(
	branch *plannerORBranch,
	order qx.Order,
	branchLimit int,
	dst *roaring64.Bitmap,
) (emitted uint64, examined uint64, dedupe uint64, ok bool) {
	if branch == nil || dst == nil || branchLimit <= 0 {
		return 0, 0, 0, false
	}
	if order.Type != qx.OrderBasic || order.Field == "" {
		return 0, 0, 0, false
	}
	fm := db.fields[order.Field]
	if fm == nil || fm.Slice {
		return 0, 0, 0, false
	}
	slice := db.snapshotFieldIndexSlice(order.Field)
	if slice == nil {
		return 0, 0, 0, false
	}
	s := *slice
	if len(s) == 0 {
		return 0, 0, 0, true
	}

	start, end, covered, rangeOK := db.extractOrderRangeCoverage(order.Field, branch.preds, s)
	if !rangeOK {
		return 0, 0, 0, false
	}
	for pi := range covered {
		if covered[pi] {
			branch.preds[pi].covered = true
		}
	}
	if start >= end {
		return 0, 0, 0, true
	}

	checksBuf := getIntSliceBuf(len(branch.preds))
	checks := branch.buildMatchChecks(checksBuf.values)
	defer func() {
		checksBuf.values = checks
		releaseIntSliceBuf(checksBuf)
	}()
	exactChecksBuf := getIntSliceBuf(len(checks))
	exactChecks := branch.buildBitmapFilterChecks(exactChecksBuf.values, checks)
	defer func() {
		exactChecksBuf.values = exactChecks
		releaseIntSliceBuf(exactChecksBuf)
	}()
	singleCheck := -1
	if len(checks) == 1 {
		singleCheck = checks[0]
	}
	var bucketWork *roaring64.Bitmap
	if len(exactChecks) > 0 {
		bucketWork = getRoaringBuf()
		defer releaseRoaringBuf(bucketWork)
	}
	allChecksExact := len(checks) > 0 && len(exactChecks) == len(checks)
	limit := uint64(branchLimit)
	var emit func(uint64) bool

	emitBitmapNoChecks := func(bm *roaring64.Bitmap, card uint64) bool {
		examined += card
		stop := false
		it := bm.Iterator()
		defer releaseRoaringBitmapIterator(it)
		for it.HasNext() {
			idx := it.Next()
			emitted++
			if !dst.CheckedAdd(idx) {
				dedupe++
			}
			if emitted >= limit {
				stop = true
				break
			}
		}
		return stop
	}

	emitBitmapChecked := func(bm *roaring64.Bitmap) bool {
		stop := false
		it := bm.Iterator()
		defer releaseRoaringBitmapIterator(it)
		for it.HasNext() {
			if emit(it.Next()) {
				stop = true
				break
			}
		}
		return stop
	}
	emit = func(idx uint64) bool {
		examined++
		if singleCheck >= 0 {
			if !branch.preds[singleCheck].matches(idx) {
				return false
			}
		} else if !branch.matchesChecks(idx, checks) {
			return false
		}
		emitted++
		if !dst.CheckedAdd(idx) {
			dedupe++
		}
		return emitted >= limit
	}

	if !order.Desc {
		for i := start; i < end; i++ {
			bucket := s[i].IDs
			if bucket.IsEmpty() {
				continue
			}
			if bucket.isSingleton() {
				if emit(bucket.single) {
					return emitted, examined, dedupe, true
				}
				continue
			}
			bm := bucket.bitmap()
			if len(exactChecks) > 0 {
				mode, exactBM, card := plannerFilterBitmapByChecks(branch.preds, exactChecks, bm, bucketWork, true)
				switch mode {
				case plannerPredicateBucketEmpty:
					examined += card
					continue
				case plannerPredicateBucketAll, plannerPredicateBucketExact:
					if allChecksExact {
						if emitBitmapNoChecks(exactBM, card) {
							return emitted, examined, dedupe, true
						}
						continue
					}
					if emitBitmapChecked(exactBM) {
						return emitted, examined, dedupe, true
					}
					continue
				}
			}
			stop := false
			bucket.ForEach(func(idx uint64) bool {
				if emit(idx) {
					stop = true
					return false
				}
				return true
			})
			if stop {
				return emitted, examined, dedupe, true
			}
		}
		return emitted, examined, dedupe, true
	}

	for i := end - 1; i >= start; i-- {
		bucket := s[i].IDs
		if !bucket.IsEmpty() {
			if bucket.isSingleton() {
				if emit(bucket.single) {
					return emitted, examined, dedupe, true
				}
			} else {
				bm := bucket.bitmap()
				if len(exactChecks) > 0 {
					mode, exactBM, card := plannerFilterBitmapByChecks(branch.preds, exactChecks, bm, bucketWork, true)
					switch mode {
					case plannerPredicateBucketEmpty:
						examined += card
						continue
					case plannerPredicateBucketAll, plannerPredicateBucketExact:
						if allChecksExact {
							if emitBitmapNoChecks(exactBM, card) {
								return emitted, examined, dedupe, true
							}
							continue
						}
						if emitBitmapChecked(exactBM) {
							return emitted, examined, dedupe, true
						}
						continue
					}
				}
				stop := false
				bucket.ForEach(func(idx uint64) bool {
					if emit(idx) {
						stop = true
						return false
					}
					return true
				})
				if stop {
					return emitted, examined, dedupe, true
				}
			}
		}
		if i == start {
			break
		}
	}
	return emitted, examined, dedupe, true
}

func (db *DB[K, V]) planOROrderMergeBranchLimit(branch plannerORBranch, need int) (limit int, exhaustive bool, err error) {
	if need <= 0 {
		return 0, true, nil
	}

	// Default safe limit: full window size.
	limit = need
	if !plannerOROrderMergePrecountWorth(branch, need) {
		return limit, false, nil
	}

	if len(branch.preds) == 0 || len(branch.preds) > 4 {
		return limit, false, nil
	}

	// Keep branch pre-counting conservative:
	// for broad/range/string scans it is usually not worth extra Count().
	for i := range branch.preds {
		e := branch.preds[i].expr
		if e.Not {
			return limit, false, nil
		}
		if !db.canPrecountORBranchExpr(e) {
			return limit, false, nil
		}
	}

	if cnt, ok := db.countORBranchByUniqueLead(branch); ok {
		if cnt == 0 {
			return 0, true, nil
		}
		if cnt < uint64(limit) {
			return int(cnt), true, nil
		}
		return limit, false, nil
	}

	var cnt uint64
	cnt, err = db.countPreparedExpr(branch.expr)
	if err != nil {
		return 0, false, err
	}
	if cnt == 0 {
		return 0, true, nil
	}
	if cnt < uint64(limit) {
		return int(cnt), true, nil
	}
	return limit, false, nil
}

func plannerOROrderMergePrecountWorth(branch plannerORBranch, need int) bool {
	if need <= 0 || branch.lead == nil || branch.lead.estCard == 0 {
		return false
	}

	checks := float64(branch.containsChecks())
	needF := float64(need)
	leadEst := float64(branch.lead.estCard)
	if leadEst < 1 {
		leadEst = 1
	}

	// Approximate branch scan work without pre-count by limit-sized lead scan.
	branchScanWork := needF * (1.0 + checks*0.30)
	// Approximate pre-count work by lead-driven branch count + residual scan for
	// the reduced limit (bounded by lead estimate).
	reduced := math.Min(needF, leadEst)
	preCountWork := plannerOROrderMergePrecountBaseCost + reduced*(0.65+checks*0.35)
	postCountScanWork := reduced * (1.0 + checks*0.30)

	return preCountWork+postCountScanWork <= branchScanWork*plannerOROrderMergePrecountGain
}

func (db *DB[K, V]) canPrecountORBranchExpr(e qx.Expr) bool {
	switch e.Op {
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX, qx.OpSUFFIX, qx.OpCONTAINS:
		cacheKey := db.materializedPredCacheKey(e)
		if cacheKey == "" {
			return false
		}
		_, ok := db.getSnapshot().loadMaterializedPred(cacheKey)
		return ok
	default:
		return true
	}
}

// countORBranchByUniqueLead tries to count one OR branch by scanning a unique
// EQ predicate lead and validating the remaining predicates with contains().
func (db *DB[K, V]) countORBranchByUniqueLead(branch plannerORBranch) (uint64, bool) {
	if len(branch.preds) == 0 {
		return 0, false
	}

	leadIdx := -1
	leadEst := uint64(0)
	for i := range branch.preds {
		p := branch.preds[i]
		if p.alwaysTrue || p.covered || !p.hasIter() {
			continue
		}
		if !db.isPositiveUniqueEqExpr(p.expr) {
			continue
		}
		if leadIdx == -1 || p.estCard < leadEst {
			leadIdx = i
			leadEst = p.estCard
		}
	}
	if leadIdx < 0 {
		return 0, false
	}
	if leadEst > 64 {
		return 0, false
	}

	checksBuf := getIntSliceBuf(len(branch.preds))
	checks := checksBuf.values
	defer func() {
		checksBuf.values = checks
		releaseIntSliceBuf(checksBuf)
	}()
	for i := range branch.preds {
		if i == leadIdx {
			continue
		}
		p := branch.preds[i]
		if p.covered || p.alwaysTrue {
			continue
		}
		if p.alwaysFalse || !p.hasContains() {
			return 0, true
		}
		checks = append(checks, i)
	}
	sortActivePredicates(checks, branch.preds)

	it := branch.preds[leadIdx].newIter()
	if it == nil {
		return 0, false
	}
	defer releaseRoaringIter(it)

	var cnt uint64
	for it.HasNext() {
		idx := it.Next()
		pass := true
		for _, ci := range checks {
			if !branch.preds[ci].matches(idx) {
				pass = false
				break
			}
		}
		if pass {
			cnt++
		}
	}

	return cnt, true
}

type plannerORNoOrderBranchState struct {
	index int

	score  float64
	budget uint64

	initialized bool
	capped      bool
	exhausted   bool

	iter plannerORIter
}

func plannerORNoOrderSortBranchOrder(order []int, states []plannerORNoOrderBranchState) {
	for i := 1; i < len(order); i++ {
		cur := order[i]
		curState := states[cur]
		j := i - 1
		for ; j >= 0; j-- {
			prev := states[order[j]]
			if prev.score < curState.score || (prev.score == curState.score && prev.index < curState.index) {
				break
			}
			order[j+1] = order[j]
		}
		order[j+1] = cur
	}
}

func plannerORNoOrderInsertTopN(top []uint64, idx uint64, n int) []uint64 {
	if n <= 0 {
		return top
	}
	lo, hi := 0, len(top)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if top[mid] < idx {
			lo = mid + 1
			continue
		}
		hi = mid
	}
	pos := lo

	if len(top) < n {
		top = append(top, 0)
		copy(top[pos+1:], top[pos:])
		top[pos] = idx
		return top
	}
	if pos >= len(top) {
		return top
	}

	copy(top[pos+1:], top[pos:len(top)-1])
	top[pos] = idx
	return top
}

func (db *DB[K, V]) execPlanORNoOrder(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]K, bool) {
	for i := range branches {
		if branches[i].alwaysTrue {
			return db.execPlanORUniverseNoOrder(q, trace), true
		}
		if branches[i].lead == nil || !branches[i].lead.hasIter() {
			return nil, false
		}
	}

	if q.Offset == 0 && len(branches) >= 3 {
		if out, ok := db.execPlanORNoOrderAdaptive(q, branches, trace); ok {
			return out, true
		}
	}
	return db.execPlanORNoOrderBaseline(q, branches, trace)
}

// execPlanORNoOrderAdaptive collects top-N ids for no-order OR via adaptive per-branch probing.
//
// It starts with cheap branches and increases probe budgets only when needed to prove top-N completeness.
func (db *DB[K, V]) execPlanORNoOrderAdaptive(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]K, bool) {
	if q.Limit == 0 || q.Offset != 0 {
		return nil, false
	}

	need := int(q.Limit)
	if need <= 0 {
		return nil, true
	}

	var (
		examined    uint64
		examinedPtr *uint64
	)
	var branchMetrics []TraceORBranch
	if trace != nil {
		examinedPtr = &examined
		branchMetrics = make([]TraceORBranch, len(branches))
		for i := range branchMetrics {
			branchMetrics[i].Index = i
		}
	}

	var states []plannerORNoOrderBranchState
	if len(branches) <= 8 {
		var statesInline [8]plannerORNoOrderBranchState
		states = statesInline[:len(branches)]
	} else {
		states = make([]plannerORNoOrderBranchState, len(branches))
	}
	for i := range states {
		states[i] = plannerORNoOrderBranchState{}
	}

	var order []int
	if len(branches) <= 8 {
		var orderInline [8]int
		order = orderInline[:len(branches)]
	} else {
		order = make([]int, len(branches))
	}
	for i := range branches {
		state := &states[i]
		state.index = i
		state.score = branches[i].noOrderScore()
		state.budget = branches[i].noOrderBudget(need)
		order[i] = i

		var branchExaminedPtr *uint64
		var branchEmittedPtr *uint64
		if trace != nil {
			branchExaminedPtr = &branchMetrics[i].RowsExamined
			branchEmittedPtr = &branchMetrics[i].RowsEmitted
		}
		state.iter = plannerORIter{
			it:             branches[i].lead.newIter(),
			branch:         &branches[i],
			examined:       examinedPtr,
			branchExamined: branchExaminedPtr,
			branchEmitted:  branchEmittedPtr,
		}
	}
	defer func() {
		for i := range states {
			states[i].iter.Release()
		}
	}()

	plannerORNoOrderSortBranchOrder(order, states)

	nextBudget := func(cur uint64) uint64 {
		if cur < plannerORNoOrderMinBranchBudget {
			return plannerORNoOrderMinBranchBudget
		}
		n := cur * 2
		if n > plannerORNoOrderMaxBranchBudget {
			n = plannerORNoOrderMaxBranchBudget
		}
		return n
	}

	probeBranch := func(st *plannerORNoOrderBranchState) {
		if st.exhausted || st.iter.has {
			return
		}
		st.initialized = true
		budgetHit := st.iter.advanceWithBudget(st.budget)
		if st.iter.has {
			st.capped = false
			return
		}
		if budgetHit {
			st.capped = true
			st.budget = nextBudget(st.budget)
			return
		}
		st.capped = false
		st.exhausted = true
	}

	consumeBranch := func(st *plannerORNoOrderBranchState) {
		if !st.iter.has {
			return
		}
		budgetHit := st.iter.advanceWithBudget(st.budget)
		if st.iter.has {
			st.capped = false
			return
		}
		if budgetHit {
			st.capped = true
			st.budget = nextBudget(st.budget)
			return
		}
		st.capped = false
		st.exhausted = true
	}

	findReadyMin := func(maxInclusive uint64, useMax bool) *plannerORNoOrderBranchState {
		var best *plannerORNoOrderBranchState
		for i := range states {
			st := &states[i]
			if !st.iter.has {
				continue
			}
			if useMax && st.iter.cur > maxInclusive {
				continue
			}
			if best == nil || st.iter.cur < best.iter.cur {
				best = st
			}
		}
		return best
	}

	findNextUninitialized := func() *plannerORNoOrderBranchState {
		for _, idx := range order {
			st := &states[idx]
			if st.initialized || st.exhausted {
				continue
			}
			return st
		}
		return nil
	}

	findNextCapped := func() *plannerORNoOrderBranchState {
		for _, idx := range order {
			st := &states[idx]
			if st.exhausted || !st.capped || st.iter.has {
				continue
			}
			return st
		}
		return nil
	}

	useLinearSeen := need <= plannerORNoOrderLinearSeenNeedMax
	seen := u64set{}
	if !useLinearSeen {
		seen = newU64Set(max(64, need*2))
	}
	topBuf := getUint64SliceBuf(need)
	top := topBuf.values
	defer func() {
		topBuf.values = top
		releaseUint64SliceBuffer(topBuf)
	}()

	addCandidate := func(idx uint64) {
		// Threshold prune prevents growing a candidate set that can no longer
		// improve top-N; it is key for large OR unions with small LIMIT.
		if len(top) == need && idx > top[len(top)-1] {
			return
		}

		if useLinearSeen {
			for _, existing := range top {
				if existing == idx {
					if trace != nil {
						trace.addDedupe(1)
					}
					return
				}
			}
		} else {
			if !seen.Add(idx) {
				if trace != nil {
					trace.addDedupe(1)
				}
				return
			}
		}
		top = plannerORNoOrderInsertTopN(top, idx, need)
	}

	initialActive := plannerORNoOrderInitActiveBranches
	if initialActive > len(order) {
		initialActive = len(order)
	}
	for i := 0; i < initialActive; i++ {
		probeBranch(&states[order[i]])
	}

	// Phase 1: gather enough unique candidates from cheap-first branches.
	for len(top) < need {
		st := findReadyMin(0, false)
		if st != nil {
			addCandidate(st.iter.cur)
			consumeBranch(st)
			continue
		}

		if st = findNextUninitialized(); st != nil {
			probeBranch(st)
			continue
		}
		if st = findNextCapped(); st != nil {
			probeBranch(st)
			continue
		}
		break
	}

	earlyStopReason := "input_exhausted"

	// Phase 2: if LIMIT is already filled, prove all unseen branches cannot
	// contribute ids <= current threshold.
	for len(top) == need {
		threshold := top[len(top)-1]

		st := findReadyMin(threshold, true)
		if st != nil {
			addCandidate(st.iter.cur)
			consumeBranch(st)
			continue
		}

		if st = findNextUninitialized(); st != nil {
			probeBranch(st)
			continue
		}
		if st = findNextCapped(); st != nil {
			probeBranch(st)
			continue
		}

		earlyStopReason = "limit_reached"
		break
	}

	if trace != nil {
		trace.addExamined(examined)

		if len(top) == need {
			threshold := top[len(top)-1]
			for i := range states {
				st := &states[i]
				if st.exhausted {
					continue
				}
				if st.iter.has && st.iter.cur > threshold {
					branchMetrics[i].Skipped = true
					branchMetrics[i].SkipReason = "threshold_pruned"
				}
			}
		}

		trace.setORBranches(branchMetrics)
		trace.setEarlyStopReason(earlyStopReason)
	}

	if len(top) == 0 {
		return nil, true
	}

	out := make([]K, 0, len(top))
	for _, idx := range top {
		out = append(out, db.idFromIdxNoLock(idx))
	}
	return out, true
}

func (db *DB[K, V]) execPlanORNoOrderBaseline(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]K, bool) {
	var (
		examined    uint64
		examinedPtr *uint64
	)
	var branchMetrics []TraceORBranch
	if trace != nil {
		examinedPtr = &examined
		branchMetrics = make([]TraceORBranch, len(branches))
		for i := range branchMetrics {
			branchMetrics[i].Index = i
		}
	}
	var iters []plannerORIter
	if len(branches) <= 8 {
		var itersInline [8]plannerORIter
		iters = itersInline[:0]
	} else {
		iters = make([]plannerORIter, 0, len(branches))
	}
	for i := range branches {
		var branchExaminedPtr *uint64
		var branchEmittedPtr *uint64
		if trace != nil {
			branchExaminedPtr = &branchMetrics[i].RowsExamined
			branchEmittedPtr = &branchMetrics[i].RowsEmitted
		}
		iters = append(iters, plannerORIter{
			it:             branches[i].lead.newIter(),
			branch:         &branches[i],
			examined:       examinedPtr,
			branchExamined: branchExaminedPtr,
			branchEmitted:  branchEmittedPtr,
		})
	}
	defer func() {
		for i := range iters {
			iters[i].Release()
		}
	}()
	for i := range iters {
		iters[i].advance()
	}

	skip := q.Offset
	need := int(q.Limit)
	out := make([]K, 0, need)
	stopReason := "input_exhausted"

	for need > 0 {
		minIdx := uint64(math.MaxUint64)
		minPos := -1

		for i := range iters {
			if !iters[i].has {
				continue
			}
			if minPos == -1 || iters[i].cur < minIdx {
				minIdx = iters[i].cur
				minPos = i
			}
		}

		if minPos == -1 {
			break
		}

		dupHits := uint64(0)
		for i := range iters {
			if iters[i].has && iters[i].cur == minIdx {
				dupHits++
				iters[i].advance()
			}
		}
		if trace != nil && dupHits > 1 {
			trace.addDedupe(dupHits - 1)
		}

		if skip > 0 {
			skip--
			continue
		}

		out = append(out, db.idFromIdxNoLock(minIdx))
		need--
		if need == 0 {
			stopReason = "limit_reached"
			break
		}
	}

	if trace != nil {
		trace.addExamined(examined)
		trace.setORBranches(branchMetrics)
		trace.setEarlyStopReason(stopReason)
	}
	return out, true
}

func (db *DB[K, V]) execPlanORUniverseNoOrder(q *qx.QX, trace *queryTrace) []K {
	skip := q.Offset
	need := int(q.Limit)
	out := make([]K, 0, need)
	stopReason := "input_exhausted"

	if trace != nil {
		trace.addExamined(db.snapshotUniverseCardinality())
	}

	universe, owned := db.snapshotUniverseView()
	if owned {
		defer releaseRoaringBuf(universe)
	}
	it := universe.Iterator()
	defer releaseRoaringBitmapIterator(it)
	for it.HasNext() {
		idx := it.Next()
		if skip > 0 {
			skip--
			continue
		}
		out = append(out, db.idFromIdxNoLock(idx))
		need--
		if need == 0 {
			stopReason = "limit_reached"
			break
		}
	}
	if trace != nil {
		trace.setEarlyStopReason(stopReason)
	}
	return out
}
