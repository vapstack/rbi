package rbi

import (
	"math"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/posting"
)

// PlanName is a stable plan identifier used by tracing and calibration.
type PlanName string

const (
	PlanMaterialized       PlanName = "plan_materialized"
	PlanCountMaterialized  PlanName = "plan_count_materialized"
	PlanCountUniqueEq      PlanName = "plan_count_unique_eq"
	PlanCountScalarLookup  PlanName = "plan_count_scalar_lookup"
	PlanCountScalarInSplit PlanName = "plan_count_scalar_in_split"
	PlanCountPredicates    PlanName = "plan_count_predicates"
	PlanCountORPredicates  PlanName = "plan_count_or_predicates"
	PlanCountORHybrid      PlanName = "plan_count_or_hybrid"

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

	preds               []predicate
	predsBuf            *predicateSliceBuf
	alwaysTrue          bool
	estCard             uint64
	estKnown            bool
	coveredRangeBounded bool
	coveredRangeStart   int
	coveredRangeEnd     int
	lead                *predicate
	leadIdx             int
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

func (qv *queryView[K, V]) buildORBranchesWithBuilder(
	ops []qx.Expr,
	build func([]qx.Expr) ([]predicate, *predicateSliceBuf, bool),
) (plannerORBranches, bool, bool) {
	out := make(plannerORBranches, 0, len(ops))
	leavesBuf := getExprSliceBuf(8)
	defer releaseExprSliceBuf(leavesBuf)

	for _, op := range ops {
		leaves, ok := collectAndLeavesScratch(op, leavesBuf.values[:0])
		if !ok {
			releaseORBranches(out)
			return nil, false, false
		}

		preds, predsBuf, ok := build(leaves)
		if !ok {
			releaseORBranches(out)
			return nil, false, false
		}
		plannerORSortPredicates(preds)

		branch := plannerORBranch{
			expr:     op,
			preds:    preds,
			predsBuf: predsBuf,
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
			if p.alwaysTrue || p.covered {
				continue
			}
			allTrue = false
			if !p.hasContains() {
				releasePredicates(preds, predsBuf)
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
			releasePredicates(preds, predsBuf)
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

func (b *plannerORBranch) buildPostingFilterChecks(dst []int, checks []int) []int {
	dst = dst[:0]
	if b.alwaysTrue {
		return dst
	}
	for _, i := range checks {
		if b.preds[i].supportsExactBucketPostingFilter() {
			dst = append(dst, i)
		}
	}
	return dst
}

func plannerResidualChecks(dst []int, checks []int, exactChecks []int) []int {
	dst = dst[:0]
	if len(checks) == 0 {
		return dst
	}
	if len(exactChecks) == 0 {
		return append(dst, checks...)
	}
	ei := 0
	for _, check := range checks {
		if ei < len(exactChecks) && exactChecks[ei] == check {
			ei++
			continue
		}
		dst = append(dst, check)
	}
	return dst
}

func (b *plannerORBranch) matchesChecks(idx uint64, checks []int) bool {
	if b.alwaysTrue {
		return true
	}
	switch len(checks) {
	case 0:
		return true
	case 1:
		p0 := &b.preds[checks[0]]
		return !p0.alwaysFalse && p0.hasContains() && p0.matches(idx)
	case 2:
		p0 := &b.preds[checks[0]]
		if p0.alwaysFalse || !p0.hasContains() || !p0.matches(idx) {
			return false
		}
		p1 := &b.preds[checks[1]]
		return !p1.alwaysFalse && p1.hasContains() && p1.matches(idx)
	case 3:
		p0 := &b.preds[checks[0]]
		if p0.alwaysFalse || !p0.hasContains() || !p0.matches(idx) {
			return false
		}
		p1 := &b.preds[checks[1]]
		if p1.alwaysFalse || !p1.hasContains() || !p1.matches(idx) {
			return false
		}
		p2 := &b.preds[checks[2]]
		return !p2.alwaysFalse && p2.hasContains() && p2.matches(idx)
	case 4:
		p0 := &b.preds[checks[0]]
		if p0.alwaysFalse || !p0.hasContains() || !p0.matches(idx) {
			return false
		}
		p1 := &b.preds[checks[1]]
		if p1.alwaysFalse || !p1.hasContains() || !p1.matches(idx) {
			return false
		}
		p2 := &b.preds[checks[2]]
		if p2.alwaysFalse || !p2.hasContains() || !p2.matches(idx) {
			return false
		}
		p3 := &b.preds[checks[3]]
		return !p3.alwaysFalse && p3.hasContains() && p3.matches(idx)
	}
	for _, i := range checks {
		p := &b.preds[i]
		if p.alwaysFalse || !p.hasContains() || !p.matches(idx) {
			return false
		}
	}
	return true
}

func (b *plannerORBranch) matchesChecksObserved(idx uint64, checks []int, observed []uint64, candidates []bool) bool {
	if b.alwaysTrue {
		return true
	}
	switch len(checks) {
	case 0:
		return true
	case 1:
		if len(observed) > 0 && len(candidates) > 0 && candidates[0] {
			observed[0]++
		}
		p0 := &b.preds[checks[0]]
		return !p0.alwaysFalse && p0.hasContains() && p0.matches(idx)
	case 2:
		if len(observed) > 0 && len(candidates) > 0 && candidates[0] {
			observed[0]++
		}
		p0 := &b.preds[checks[0]]
		if p0.alwaysFalse || !p0.hasContains() || !p0.matches(idx) {
			return false
		}
		if len(observed) > 1 && len(candidates) > 1 && candidates[1] {
			observed[1]++
		}
		p1 := &b.preds[checks[1]]
		return !p1.alwaysFalse && p1.hasContains() && p1.matches(idx)
	case 3:
		if len(observed) > 0 && len(candidates) > 0 && candidates[0] {
			observed[0]++
		}
		p0 := &b.preds[checks[0]]
		if p0.alwaysFalse || !p0.hasContains() || !p0.matches(idx) {
			return false
		}
		if len(observed) > 1 && len(candidates) > 1 && candidates[1] {
			observed[1]++
		}
		p1 := &b.preds[checks[1]]
		if p1.alwaysFalse || !p1.hasContains() || !p1.matches(idx) {
			return false
		}
		if len(observed) > 2 && len(candidates) > 2 && candidates[2] {
			observed[2]++
		}
		p2 := &b.preds[checks[2]]
		return !p2.alwaysFalse && p2.hasContains() && p2.matches(idx)
	case 4:
		if len(observed) > 0 && len(candidates) > 0 && candidates[0] {
			observed[0]++
		}
		p0 := &b.preds[checks[0]]
		if p0.alwaysFalse || !p0.hasContains() || !p0.matches(idx) {
			return false
		}
		if len(observed) > 1 && len(candidates) > 1 && candidates[1] {
			observed[1]++
		}
		p1 := &b.preds[checks[1]]
		if p1.alwaysFalse || !p1.hasContains() || !p1.matches(idx) {
			return false
		}
		if len(observed) > 2 && len(candidates) > 2 && candidates[2] {
			observed[2]++
		}
		p2 := &b.preds[checks[2]]
		if p2.alwaysFalse || !p2.hasContains() || !p2.matches(idx) {
			return false
		}
		if len(observed) > 3 && len(candidates) > 3 && candidates[3] {
			observed[3]++
		}
		p3 := &b.preds[checks[3]]
		return !p3.alwaysFalse && p3.hasContains() && p3.matches(idx)
	}
	for ci, i := range checks {
		if ci < len(observed) && ci < len(candidates) && candidates[ci] {
			observed[ci]++
		}
		p := &b.preds[i]
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

type plannerBucketPostingFilterPredicate interface {
	countBucket(posting.List) (uint64, bool)
	applyToPosting(posting.List) (posting.List, bool)
}

func plannerAllowExactBucketFilter(skip, need, card uint64, exactOnly bool, exactChecks int) bool {
	if card <= plannerPredicateBucketExactMinCard {
		return false
	}
	if skip > 0 {
		return true
	}
	if exactChecks <= 0 {
		return false
	}
	if exactOnly {
		return exactChecks > 1
	}
	if exactChecks <= 1 || need == 0 {
		return false
	}
	return card > need*uint64(exactChecks)
}

func plannerPredicateBucketExactMinCardForChecks(checks int) uint64 {
	if checks <= 1 {
		return plannerPredicateBucketExactMinCard
	}
	threshold := plannerPredicateBucketExactMinCard
	switch {
	case checks >= 4:
		threshold /= 8
	case checks == 3:
		threshold /= 4
	default:
		threshold /= 2
	}
	if threshold < 4 {
		threshold = 4
	}
	return uint64(threshold)
}

func plannerFilterPostingByChecks[T plannerBucketPostingFilterPredicate](
	preds []T,
	checks []int,
	src posting.List,
	work posting.List,
	allowExact bool,
) (plannerPredicateBucketMode, posting.List, posting.List, uint64) {
	if src.IsEmpty() {
		return plannerPredicateBucketEmpty, posting.List{}, work, 0
	}
	card := src.Cardinality()
	if len(checks) == 0 {
		return plannerPredicateBucketAll, src, work, card
	}

	if !allowExact || card <= plannerPredicateBucketExactMinCardForChecks(len(checks)) {
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
			return plannerPredicateBucketEmpty, posting.List{}, work, card
		}
		if fullBucket {
			return plannerPredicateBucketAll, src, work, card
		}
		return plannerPredicateBucketFallback, posting.List{}, work, card
	}

	work = src.CloneInto(work)
	for _, pi := range checks {
		var ok bool
		work, ok = preds[pi].applyToPosting(work)
		if !ok {
			return plannerPredicateBucketFallback, posting.List{}, work, card
		}
		if work.IsEmpty() {
			return plannerPredicateBucketEmpty, posting.List{}, work, card
		}
	}
	if work.Cardinality() == card {
		return plannerPredicateBucketAll, src, work, card
	}
	return plannerPredicateBucketExact, work, work, card
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
	if b.estKnown {
		if b.estCard == 0 {
			est = 1
		} else {
			est = b.estCard
		}
	} else if b.lead != nil && b.lead.estCard > 0 {
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

func (bs plannerORBranches) avgContainsChecksByBranch(checks [plannerORBranchLimit]int) float64 {
	totalChecks := 0.0
	activeBranches := 0.0
	n := len(bs)
	if n > plannerORBranchLimit {
		n = plannerORBranchLimit
	}
	for i := 0; i < n; i++ {
		if bs[i].alwaysTrue {
			continue
		}
		totalChecks += float64(checks[i])
		activeBranches++
	}
	if activeBranches == 0 {
		return 0
	}
	return totalChecks / activeBranches
}

type plannerOROrderMergeBranchStats struct {
	streamChecks int
	mergeChecks  int
	rangeRows    uint64
	rangeBounded bool
}

func (qv *queryView[K, V]) orderMergeBranchStats(orderField string, branches plannerORBranches, ov fieldOverlay) [plannerORBranchLimit]plannerOROrderMergeBranchStats {
	var stats [plannerORBranchLimit]plannerOROrderMergeBranchStats
	n := len(branches)
	if n > plannerORBranchLimit {
		n = plannerORBranchLimit
	}
	for i := 0; i < n; i++ {
		stats[i].streamChecks = branches[i].containsChecks()
		stats[i].mergeChecks = stats[i].streamChecks
		if branches[i].coveredRangeBounded {
			stats[i].streamChecks = 0
			stats[i].mergeChecks = 0
			stats[i].rangeBounded = true
			if branches[i].estKnown {
				stats[i].rangeRows = branches[i].estCard
			} else {
				br := overlayRange{
					baseStart: branches[i].coveredRangeStart,
					baseEnd:   branches[i].coveredRangeEnd,
				}
				_, stats[i].rangeRows = overlayRangeStats(ov, br)
				branches[i].estCard = stats[i].rangeRows
				branches[i].estKnown = true
			}
			continue
		}
		if orderField == "" || !ov.hasData() {
			continue
		}
		br, covered, ok := qv.extractOrderRangeCoverageOverlay(orderField, branches[i].preds, ov)
		if !ok || len(covered) == 0 {
			if ok && (br.baseStart != 0 || br.baseEnd != ov.keyCount()) {
				stats[i].rangeBounded = true
				_, stats[i].rangeRows = overlayRangeStats(ov, br)
			}
			checksBuf := getIntSliceBuf(len(branches[i].preds))
			checksBuf.values = branches[i].buildMatchChecks(checksBuf.values[:0])
			exactChecksBuf := getIntSliceBuf(len(checksBuf.values))
			exactChecksBuf.values = branches[i].buildPostingFilterChecks(exactChecksBuf.values[:0], checksBuf.values)
			residualChecksBuf := getIntSliceBuf(len(checksBuf.values))
			residualChecksBuf.values = plannerResidualChecks(residualChecksBuf.values[:0], checksBuf.values, exactChecksBuf.values)
			stats[i].streamChecks = len(checksBuf.values)
			stats[i].mergeChecks = len(residualChecksBuf.values)
			releaseIntSliceBuf(exactChecksBuf)
			releaseIntSliceBuf(residualChecksBuf)
			releaseIntSliceBuf(checksBuf)
			continue
		}
		checksBuf := getIntSliceBuf(len(branches[i].preds))
		checksBuf.values = checksBuf.values[:0]
		for pi := range branches[i].preds {
			p := branches[i].preds[pi]
			if p.alwaysTrue || p.alwaysFalse || !p.hasContains() {
				continue
			}
			if pi < len(covered) && covered[pi] {
				continue
			}
			checksBuf.values = append(checksBuf.values, pi)
		}
		exactChecksBuf := getIntSliceBuf(len(checksBuf.values))
		exactChecksBuf.values = branches[i].buildPostingFilterChecks(exactChecksBuf.values[:0], checksBuf.values)
		residualChecksBuf := getIntSliceBuf(len(checksBuf.values))
		residualChecksBuf.values = plannerResidualChecks(residualChecksBuf.values[:0], checksBuf.values, exactChecksBuf.values)
		stats[i].streamChecks = len(checksBuf.values)
		stats[i].mergeChecks = len(residualChecksBuf.values)
		releaseIntSliceBuf(exactChecksBuf)
		releaseIntSliceBuf(residualChecksBuf)
		releaseIntSliceBuf(checksBuf)
		if br.baseStart == 0 && br.baseEnd == ov.keyCount() {
			continue
		}
		stats[i].rangeBounded = true
		_, stats[i].rangeRows = overlayRangeStats(ov, br)
	}
	return stats
}

func (branches plannerORBranches) hasFullSpanOrderBranch(stats [plannerORBranchLimit]plannerOROrderMergeBranchStats) bool {
	n := len(branches)
	if n > plannerORBranchLimit {
		n = plannerORBranchLimit
	}
	for i := 0; i < n; i++ {
		if branches[i].alwaysTrue {
			continue
		}
		if !stats[i].rangeBounded {
			return true
		}
	}
	return false
}

func (branches plannerORBranches) hasKWayExactBucketApplyWork(stats [plannerORBranchLimit]plannerOROrderMergeBranchStats) bool {
	n := len(branches)
	if n > plannerORBranchLimit {
		n = plannerORBranchLimit
	}
	for i := 0; i < n; i++ {
		if stats[i].streamChecks > stats[i].mergeChecks {
			return true
		}
	}
	return false
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
	avgChecks         float64
	hasPrefixTailRisk bool
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
	if routeCost.hasPrefixTailRisk {
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
func (qv *queryView[K, V]) estimateOROrderMergeRouteCost(
	q *qx.QX,
	branches plannerORBranches,
	need int,
	mergeStats [plannerORBranchLimit]plannerOROrderMergeBranchStats,
) (plannerOROrderRouteCost, bool) {
	if need <= 0 || q == nil || len(q.Order) != 1 {
		return plannerOROrderRouteCost{}, false
	}

	orderField := q.Order[0].Field
	if orderField == "" {
		return plannerOROrderRouteCost{}, false
	}

	snap := qv.planner.stats.Load()
	universe := snap.universeOr(qv.snapshotUniverseCardinality())
	if universe == 0 {
		return plannerOROrderRouteCost{}, false
	}
	orderOV := qv.fieldOverlay(orderField)
	orderDistinct := uint64(overlayApproxDistinctTotalCount(orderOV))
	orderStats := qv.plannerOrderFieldStats(orderField, snap, universe, orderDistinct)

	unionCard, sumCard, branchCards, _ := branches.unionCards(universe)
	if unionCard == 0 {
		return plannerOROrderRouteCost{}, false
	}
	headSensitiveOrderShape := orderDistinct >= 64

	expectedRows := estimateRowsForNeed(uint64(need), unionCard, universe)
	if expectedRows == 0 {
		return plannerOROrderRouteCost{}, false
	}

	var mergeChecks [plannerORBranchLimit]int
	for i := range branches {
		mergeChecks[i] = mergeStats[i].mergeChecks
	}
	avgChecks := branches.avgContainsChecksByBranch(mergeChecks)
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

	hasPrefixTailRisk := branches.hasNonOrderPrefixTailRisk(orderField)
	hasSelectiveLead := branches.hasSelectiveLead(need)
	offsetShare := 0.0
	if q.Offset > 0 {
		offsetShare = float64(q.Offset) / float64(need)
		if offsetShare > 1 {
			offsetShare = 1
		}
	}

	kWayRows := float64(expectedRows) * overlap
	if offsetShare > 0 {
		kWayRows *= 1.0 + offsetShare*0.75
	}
	if hasPrefixTailRisk {
		kWayRows *= 1.15
	}
	if hasSelectiveLead {
		kWayRows *= 0.92
	}
	if headSensitiveOrderShape && orderStats.AvgBucketCard > float64(max(need, 1)) {
		avgBucket := orderStats.AvgBucketCard
		headBucketAmp := plannerClampFloat((avgBucket/float64(max(need, 1))-1.0)*0.12, 0, 3.0)
		kWayRows *= 1.0 + headBucketAmp
	}

	// K-way pays per-pop merge + predicate checks; fallback pays branch collection
	// and order ranking of merged candidates.
	kWayCost := kWayRows * (1.0 + avgChecks*0.55)
	fallbackCollectRows := 0.0
	activeBranches := 0
	directCollectFast := orderOV.hasData()
	for i, card := range branchCards {
		if card == 0 {
			continue
		}
		activeBranches++
		branchUniverse := universe
		if i < len(mergeStats) && mergeStats[i].rangeRows > 0 && mergeStats[i].rangeRows < branchUniverse {
			branchUniverse = mergeStats[i].rangeRows
		}
		probes := float64(estimateRowsForNeed(uint64(need), card, branchUniverse))
		if probes < float64(need) {
			probes = float64(need)
		}
		if offsetShare > 0 {
			probes *= 1.0 + offsetShare*0.35
		}
		if directCollectFast && headSensitiveOrderShape {
			if avgBucket := orderStats.AvgBucketCard; avgBucket > float64(max(need, 1)) {
				headCollectFactor := plannerClampFloat((float64(max(need, 1))/avgBucket)*1.5, 0.20, 1.0)
				probes *= headCollectFactor
			}
		}
		fallbackCollectRows += probes
	}
	if activeBranches == 0 {
		activeBranches = len(branches)
	}
	fallbackCandidates := float64(min(sumCard, uint64(need*activeBranches)))
	fallbackCost := fallbackCollectRows*(1.0+avgChecks*0.22) + fallbackCandidates*(1.0+overlap*0.08)
	if directCollectFast && headSensitiveOrderShape {
		fallbackCost *= 0.92
		if avgBucket := orderStats.AvgBucketCard; avgBucket > float64(max(need, 1))*4.0 {
			fallbackCost *= 0.90
		}
	}

	return plannerOROrderRouteCost{
		kWay:              kWayCost,
		fallback:          fallbackCost,
		overlap:           overlap,
		avgChecks:         avgChecks,
		hasPrefixTailRisk: hasPrefixTailRisk,
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
	it     posting.Iterator
	branch *plannerORBranch

	examined       *uint64
	branchExamined *uint64
	branchEmitted  *uint64

	has bool
	cur uint64
}

func (it *plannerORIter) Release() {
	if it.it != nil {
		it.it.Release()
	}
	it.it = nil
	it.has = false
	it.cur = 0
}

func (qv *queryView[K, V]) tryPlan(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	orderedPtr := false
	if len(q.Order) == 1 && q.Order[0].Type == qx.OrderBasic {
		if fm := qv.fields[q.Order[0].Field]; fm != nil && fm.Ptr {
			orderedPtr = true
		}
	}
	if !orderedPtr {
		if out, ok, err := qv.tryPlanORMergeMode(q, trace); ok {
			return out, true, err
		}
	}
	if out, ok, err := qv.tryPlanOrdered(q, trace); ok {
		return out, true, err
	}
	if !orderedPtr {
		if out, ok, err := qv.tryPlanCandidate(q, trace); ok {
			return out, true, err
		}
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

// tryPlanORMergeMode plans top-level OR queries with bounded branch count.
//
// This path exists to avoid full bitmap materialization when branch-aware
// streaming/merge strategies can satisfy LIMIT/OFFSET cheaper.
func (qv *queryView[K, V]) tryPlanORMergeMode(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
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

	if len(q.Order) == 0 {
		branches, alwaysFalse, ok := qv.buildORBranches(q.Expr.Operands)
		if !ok {
			return nil, false, nil
		}
		if alwaysFalse {
			return nil, true, nil
		}
		defer releaseORBranches(branches)

		if q.Limit > plannerORNoOrderLimitMax || q.Offset > plannerORNoOrderOffsetMax {
			return nil, false, nil
		}
		// Cost gate is important here: baseline OR merge can regress badly on
		// high-overlap branches, so we only enter when model says it should win.
		noOrderDecision := qv.decidePlanORNoOrder(q, branches)
		if trace != nil {
			trace.setEstimated(noOrderDecision.expectedRows, noOrderDecision.kWayCost, noOrderDecision.fallbackCost)
		}
		if !noOrderDecision.use {
			return nil, false, nil
		}
		out, ok := qv.execPlanORNoOrder(q, branches, trace)
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
	window, _ := orderWindow(q)
	branches, alwaysFalse, ok := qv.buildORBranchesOrdered(q.Expr.Operands, o.Field, window)
	if !ok {
		return nil, false, nil
	}
	if alwaysFalse {
		return nil, true, nil
	}
	defer releaseORBranches(branches)

	// Ordered OR has two distinct strategies: k-way merge of branch streams and
	// stream+match fallback. Cost model chooses initial route.
	orderDecision := qv.decidePlanOROrder(q, branches)
	if trace != nil {
		trace.setEstimated(orderDecision.expectedRows, orderDecision.bestCost, orderDecision.fallbackCost)
	}

	switch orderDecision.plan {

	case plannerOROrderMerge:
		out, ok, err := qv.execPlanOROrderMerge(q, branches, trace)
		if ok {
			if trace != nil {
				trace.setPlan(PlanORMergeOrderMerge)
			}
			return out, true, err
		}
		// merge estimate can be optimistic if branch overlap is high;
		// fall back to streaming OR order plan before giving up.
		var observed orderedORObservedStats
		var observe *orderedORObservedStats
		if trace == nil {
			observe = &observed
			defer observed.release()
		}
		out2, ok2 := qv.execPlanOROrderBasic(q, branches, trace, observe)
		if ok2 {
			if trace != nil {
				trace.setPlan(PlanORMergeOrderStream)
			}
			return out2, true, nil
		}
		return nil, false, nil

	case plannerOROrderStream:
		var observed orderedORObservedStats
		var observe *orderedORObservedStats
		if trace == nil {
			observe = &observed
			defer observed.release()
		}
		out, ok := qv.execPlanOROrderBasic(q, branches, trace, observe)
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

type orderedORObservedStats struct {
	countsBuf     *uint64SliceBuf
	counts        []uint64
	candidatesBuf *boolSliceBuf
	candidates    []bool
	offsets       [plannerORBranchLimit + 1]int
	active        bool
}

func (s *orderedORObservedStats) init(branches plannerORBranches, branchChecks [plannerORBranchLimit][]int, shouldObserve func(qx.Expr) bool) {
	if s == nil {
		return
	}
	total := 0
	s.offsets[0] = 0
	branchCount := len(branches)
	if branchCount > plannerORBranchLimit {
		branchCount = plannerORBranchLimit
	}
	for i := 0; i < branchCount; i++ {
		total += len(branchChecks[i])
		s.offsets[i+1] = total
	}
	if total == 0 {
		s.counts = nil
		s.countsBuf = nil
		s.candidates = nil
		s.candidatesBuf = nil
		s.active = false
		return
	}
	active := 0
	for bi := 0; bi < branchCount; bi++ {
		for _, pi := range branchChecks[bi] {
			if shouldObserve(branches[bi].preds[pi].expr) {
				active++
			}
		}
	}
	if active == 0 {
		s.counts = nil
		s.countsBuf = nil
		s.candidates = nil
		s.candidatesBuf = nil
		s.active = false
		return
	}
	s.countsBuf = getUint64SliceBuf(total)
	s.counts = s.countsBuf.values[:total]
	clear(s.counts)
	s.candidatesBuf = getBoolSliceBuf(total)
	s.candidates = s.candidatesBuf.values[:total]
	clear(s.candidates)
	s.active = true
	for bi := 0; bi < branchCount; bi++ {
		start := s.offsets[bi]
		for ci, pi := range branchChecks[bi] {
			if shouldObserve(branches[bi].preds[pi].expr) {
				s.candidates[start+ci] = true
			}
		}
	}
}

func (s *orderedORObservedStats) branchObserved(branch int) ([]uint64, []bool) {
	if s == nil || branch < 0 || branch >= plannerORBranchLimit {
		return nil, nil
	}
	start := s.offsets[branch]
	end := s.offsets[branch+1]
	if start >= end || end > len(s.counts) {
		return nil, nil
	}
	return s.counts[start:end], s.candidates[start:end]
}

func (s *orderedORObservedStats) release() {
	if s == nil {
		return
	}
	if s.countsBuf != nil {
		s.countsBuf.values = s.counts
		releaseUint64SliceBuf(s.countsBuf)
	}
	if s.candidatesBuf != nil {
		s.candidatesBuf.values = s.candidates
		releaseBoolSliceBuf(s.candidatesBuf)
	}
	*s = orderedORObservedStats{}
}

func (qv *queryView[K, V]) orderedORMaterializedRangeLeafCosts(
	orderField string,
	leaf qx.Expr,
) (string, uint64, uint64, uint64, bool) {
	candidate, ok := qv.prepareScalarRangeRoutingCandidate(leaf)
	if !ok || !candidate.numeric || leaf.Field == orderField {
		return "", 0, 0, 0, false
	}
	core := candidate.core
	plan := candidate.plan
	if core.bound.full || plan.bucketCount == 0 || plan.est == 0 {
		return "", 0, 0, 0, false
	}
	if !plan.useComplement {
		return core.sharedReuse.cacheKey,
			rangeProbeMaterializeWork(plan.bucketCount, plan.est),
			rangeProbeContainsWork(plan.bucketCount, plan.est),
			postingContainsLookupWork(plan.est),
			true
	}

	compPlan, ok := core.prepareComplementMaterialization()
	if !ok || compPlan.buckets == 0 || compPlan.est == 0 {
		return "", 0, 0, 0, false
	}
	checkBuckets := compPlan.buckets
	checkEst := compPlan.est
	if !compPlan.nilPosting.IsEmpty() {
		nilCard := compPlan.nilPosting.Cardinality()
		if checkBuckets > 0 {
			checkBuckets--
		}
		if checkEst > nilCard {
			checkEst -= nilCard
		} else {
			checkEst = 0
		}
	}
	if checkBuckets == 0 || checkEst == 0 {
		return "", 0, 0, 0, false
	}
	return compPlan.sharedReuse.cacheKey,
		rangeProbeMaterializeWork(compPlan.buckets, compPlan.est),
		rangeProbeContainsWork(checkBuckets, checkEst),
		postingContainsLookupWork(compPlan.est),
		true
}

func (qv *queryView[K, V]) orderedORMaterializedPrefixLeafBuildWork(
	orderField string,
	leaf qx.Expr,
) (string, uint64, bool) {
	if !isPositiveNonOrderScalarPrefixLeaf(orderField, leaf) {
		return "", 0, false
	}
	candidate, ok := qv.prepareScalarRangeRoutingCandidate(leaf)
	if !ok || candidate.core.sharedReuse.cacheKey == "" {
		return "", 0, false
	}
	core := candidate.core
	snap := qv.planner.stats.Load()
	universe := max(qv.snapshotUniverseCardinality(), uint64(1))
	sel, _, _, _, ok := qv.estimateLeafOrderCost(leaf, snap, universe, orderField, qv.fieldOverlay(orderField).hasData())
	if !ok || sel <= 0 {
		return "", 0, false
	}
	estCard := uint64(sel*float64(universe) + 0.5)
	if estCard == 0 {
		estCard = 1
	}
	if qv.snap.matPredCacheMaxCard > 0 && estCard > qv.snap.matPredCacheMaxCard {
		return "", 0, false
	}
	return core.sharedReuse.cacheKey, estCard, true
}

func (qv *queryView[K, V]) promoteOrderedORMaterializedBaseOps(
	q *qx.QX,
	branches plannerORBranches,
	branchChecks [plannerORBranchLimit][]int,
	observed *orderedORObservedStats,
) {
	if q == nil || len(branches) == 0 || qv.snap == nil || len(q.Order) != 1 || observed == nil {
		return
	}
	orderField := q.Order[0].Field
	if orderField == "" {
		return
	}

	baseOpsBuf := getExprSliceBuf(len(branches) * 4)
	baseOpsBuf.values = baseOpsBuf.values[:0]
	defer func() {
		releaseExprSliceBuf(baseOpsBuf)
	}()
	cacheKeysBuf := getStringSliceBuf(len(branches) * 4)
	cacheKeysBuf.values = cacheKeysBuf.values[:0]
	defer func() {
		releaseStringSliceBuf(cacheKeysBuf)
	}()
	buildWorksBuf := getUint64SliceBuf(len(branches) * 4)
	buildWorksBuf.values = buildWorksBuf.values[:0]
	defer func() {
		releaseUint64SliceBuf(buildWorksBuf)
	}()
	checkWorksBuf := getUint64SliceBuf(len(branches) * 4)
	checkWorksBuf.values = checkWorksBuf.values[:0]
	defer func() {
		releaseUint64SliceBuf(checkWorksBuf)
	}()
	cachedCheckWorksBuf := getUint64SliceBuf(len(branches) * 4)
	cachedCheckWorksBuf.values = cachedCheckWorksBuf.values[:0]
	defer func() {
		releaseUint64SliceBuf(cachedCheckWorksBuf)
	}()
	prefixFlagsBuf := getBoolSliceBuf(len(branches) * 4)
	prefixFlagsBuf.values = prefixFlagsBuf.values[:0]
	defer func() {
		releaseBoolSliceBuf(prefixFlagsBuf)
	}()
	expectedChecksBuf := getUint64SliceBuf(len(branches) * 4)
	expectedChecksBuf.values = expectedChecksBuf.values[:0]
	defer func() {
		releaseUint64SliceBuf(expectedChecksBuf)
	}()
	observedWorksBuf := getUint64SliceBuf(len(branches) * 4)
	observedWorksBuf.values = observedWorksBuf.values[:0]
	defer func() {
		releaseUint64SliceBuf(observedWorksBuf)
	}()

	for bi, branch := range branches {
		checks := branchChecks[bi]
		checked, candidates := observed.branchObserved(bi)
		for ci, pi := range checks {
			p := branch.preds[pi]
			if p.alwaysFalse || p.covered || p.alwaysTrue {
				continue
			}
			if ci >= len(checked) || ci >= len(candidates) || !candidates[ci] || checked[ci] == 0 {
				continue
			}
			leafChecks := checked[ci]
			cacheKey, buildWork, checkWork, cachedCheckWork, ok := qv.orderedORMaterializedRangeLeafCosts(orderField, p.expr)
			isPrefix := false
			if !ok || cacheKey == "" || buildWork == 0 || checkWork == 0 {
				cacheKey, buildWork, ok = qv.orderedORMaterializedPrefixLeafBuildWork(orderField, p.expr)
				if !ok || cacheKey == "" || buildWork == 0 {
					goto nextPred
				}
				checkWork = 0
				cachedCheckWork = 0
				isPrefix = true
			}
			for slot := range cacheKeysBuf.values {
				if cacheKeysBuf.values[slot] == cacheKey {
					expectedChecksBuf.values[slot] = satAddUint64(expectedChecksBuf.values[slot], leafChecks)
					if isPrefix {
						observedWorksBuf.values[slot] = satAddUint64(observedWorksBuf.values[slot], leafChecks)
					} else if checkWork > cachedCheckWork {
						observedWorksBuf.values[slot] = satAddUint64(observedWorksBuf.values[slot], satMulUint64(leafChecks, checkWork-cachedCheckWork))
					}
					goto nextPred
				}
			}
			cacheKeysBuf.values = append(cacheKeysBuf.values, cacheKey)
			baseOpsBuf.values = append(baseOpsBuf.values, p.expr)
			buildWorksBuf.values = append(buildWorksBuf.values, buildWork)
			checkWorksBuf.values = append(checkWorksBuf.values, checkWork)
			cachedCheckWorksBuf.values = append(cachedCheckWorksBuf.values, cachedCheckWork)
			prefixFlagsBuf.values = append(prefixFlagsBuf.values, isPrefix)
			expectedChecksBuf.values = append(expectedChecksBuf.values, leafChecks)
			if isPrefix {
				observedWorksBuf.values = append(observedWorksBuf.values, leafChecks)
			} else if checkWork > cachedCheckWork {
				observedWorksBuf.values = append(observedWorksBuf.values, satMulUint64(leafChecks, checkWork-cachedCheckWork))
			} else {
				observedWorksBuf.values = append(observedWorksBuf.values, 0)
			}
		nextPred:
		}
	}
	if len(baseOpsBuf.values) == 0 {
		return
	}
	write := 0
	for i := range baseOpsBuf.values {
		if observedWorksBuf.values[i] == 0 {
			continue
		}
		if _, ok := qv.snap.loadMaterializedPred(cacheKeysBuf.values[i]); ok {
			continue
		}
		if !qv.snap.shouldPromoteObservedOrderedORMaterializedPred(cacheKeysBuf.values[i], observedWorksBuf.values[i], buildWorksBuf.values[i]) {
			continue
		}
		baseOpsBuf.values[write] = baseOpsBuf.values[i]
		write++
	}
	baseOpsBuf.values = baseOpsBuf.values[:write]
	if len(baseOpsBuf.values) == 0 {
		return
	}
	qv.materializeOrderMaterializedBaseOps(orderField, baseOpsBuf.values)
}

func releaseORBranches(branches plannerORBranches) {
	for i := range branches {
		releasePredicates(branches[i].preds, branches[i].predsBuf)
	}
}

func (qv *queryView[K, V]) shouldObserveOrderedORLeaf(orderField string, expr qx.Expr) bool {
	cacheKey, buildWork, checkWork, _, ok := qv.orderedORMaterializedRangeLeafCosts(orderField, expr)
	if ok && cacheKey != "" && buildWork != 0 && checkWork != 0 {
		if _, hit := qv.snap.loadMaterializedPred(cacheKey); hit {
			return false
		}
		return true
	}
	cacheKey, buildWork, ok = qv.orderedORMaterializedPrefixLeafBuildWork(orderField, expr)
	if !ok || cacheKey == "" || buildWork == 0 {
		return false
	}
	if _, hit := qv.snap.loadMaterializedPred(cacheKey); hit {
		return false
	}
	return true
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
func (qv *queryView[K, V]) buildORBranches(ops []qx.Expr) (plannerORBranches, bool, bool) {
	return qv.buildORBranchesWithBuilder(ops, qv.buildPredicates)
}

func (qv *queryView[K, V]) buildORBranchesOrdered(
	ops []qx.Expr,
	orderField string,
	orderedWindow int,
) (plannerORBranches, bool, bool) {
	branches, alwaysFalse, ok := qv.buildORBranchesWithBuilder(ops, func(leaves []qx.Expr) ([]predicate, *predicateSliceBuf, bool) {
		return qv.buildPredicatesOrderedWithMode(leaves, orderField, false, orderedWindow, true, true)
	})
	if !ok || alwaysFalse {
		return branches, alwaysFalse, ok
	}
	ov := qv.fieldOverlay(orderField)
	if !ov.hasData() {
		return branches, false, true
	}
	for i := range branches {
		if !branches[i].alwaysTrue || len(branches[i].preds) == 0 {
			continue
		}
		br, covered, rangeOK := qv.extractOrderRangeCoverageOverlay(orderField, branches[i].preds, ov)
		if !rangeOK {
			releaseORBranches(branches)
			return nil, false, false
		}
		if len(covered) == 0 {
			continue
		}
		if br.baseStart != 0 || br.baseEnd != ov.keyCount() {
			branches[i].alwaysTrue = false
			branches[i].coveredRangeBounded = true
			branches[i].coveredRangeStart = br.baseStart
			branches[i].coveredRangeEnd = br.baseEnd
			if br.baseStart == br.baseEnd {
				branches[i].estCard = 0
				branches[i].estKnown = true
			}
		}
	}
	return branches, false, true
}

// execPlanOROrderBasic evaluates ordered OR by scanning ordered buckets and
// checking branch predicates per candidate.
//
// It keeps deterministic ordering semantics and avoids full OR unions for LIMIT-heavy queries.
func (qv *queryView[K, V]) execPlanOROrderBasic(q *qx.QX, branches plannerORBranches, trace *queryTrace, observed *orderedORObservedStats) ([]K, bool) {
	o := q.Order[0]
	f := o.Field
	if f == "" {
		return nil, false
	}

	fm := qv.fields[f]
	if fm == nil || fm.Slice {
		return nil, false
	}

	ov := qv.fieldOverlay(f)
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
		branchStart  [plannerORBranchLimit]int
		branchEnd    [plannerORBranchLimit]int
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
		br, covered, ok := qv.extractOrderRangeCoverageOverlay(f, branches[i].preds, ov)
		if !ok {
			return nil, false
		}
		branchStart[i], branchEnd[i] = br.baseStart, br.baseEnd
		for pi := range covered {
			if covered[pi] {
				branches[i].preds[pi].covered = true
			}
		}
		checkBufs[i] = getIntSliceBuf(len(branches[i].preds))
		branchChecks[i] = branches[i].buildMatchChecks(checkBufs[i].values)
	}
	var (
		branchObservedCounts     [plannerORBranchLimit][]uint64
		branchObservedCandidates [plannerORBranchLimit][]bool
	)
	if observed != nil {
		observed.init(branches, branchChecks, func(expr qx.Expr) bool {
			return qv.shouldObserveOrderedORLeaf(f, expr)
		})
		if observed.active {
			for i := range branches {
				branchObservedCounts[i], branchObservedCandidates[i] = observed.branchObserved(i)
			}
			defer qv.promoteOrderedORMaterializedBaseOps(q, branches, branchChecks, observed)
		} else {
			observed = nil
		}
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

	matches := func(idx uint64, bucket int) bool {
		if alwaysTrue {
			return true
		}
		for i := 0; i < branchEvalN; i++ {
			bi := branchEvalOrder[i]
			if bucket < branchStart[bi] || bucket >= branchEnd[bi] {
				continue
			}
			if branches[bi].matchesChecks(idx, branchChecks[bi]) {
				return true
			}
		}
		return false
	}

	if !ov.hasData() {
		return nil, true
	}
	br := ov.rangeForBounds(rangeBounds{has: true})

	fullTrace := trace.full()

	if !fullTrace {
		cur := ov.newCursor(br, o.Desc)
		bucket := 0
		if o.Desc {
			bucket = ov.keyCount() - 1
		}
		for {
			_, bm, ok := cur.next()
			if !ok {
				break
			}
			curBucket := bucket
			if o.Desc {
				bucket--
			} else {
				bucket++
			}
			if bm.IsEmpty() {
				continue
			}
			if trace != nil {
				trace.addExamined(bm.Cardinality())
			}
			stop := false
			bm.ForEach(func(idx uint64) bool {
				if observed != nil {
					matched := false
					if alwaysTrue {
						matched = true
					} else {
						for i := 0; i < branchEvalN; i++ {
							bi := branchEvalOrder[i]
							if curBucket < branchStart[bi] || curBucket >= branchEnd[bi] {
								continue
							}
							if branches[bi].matchesChecksObserved(idx, branchChecks[bi], branchObservedCounts[bi], branchObservedCandidates[bi]) {
								matched = true
								break
							}
						}
					}
					if !matched {
						return true
					}
				} else if !matches(idx, curBucket) {
					return true
				}
				if skip > 0 {
					skip--
					return true
				}
				out = append(out, qv.idFromIdxNoLock(idx))
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

	branchMetrics := make([]TraceORBranch, len(branches))
	for i := range branchMetrics {
		branchMetrics[i].Index = i
	}

	matchWithMetrics := func(idx uint64, bucket int) bool {
		if alwaysTrue {
			if fullTrace && alwaysTrueBranch >= 0 {
				branchMetrics[alwaysTrueBranch].RowsExamined++
				branchMetrics[alwaysTrueBranch].RowsEmitted++
			}
			return true
		}

		matchedCount := 0
		for i := range branches {
			if bucket < branchStart[i] || bucket >= branchEnd[i] {
				continue
			}
			if fullTrace {
				branchMetrics[i].RowsExamined++
			}
			if branches[i].matchesChecks(idx, branchChecks[i]) {
				if fullTrace {
					branchMetrics[i].RowsEmitted++
				}
				matchedCount++
			}
		}
		if matchedCount > 1 {
			trace.addDedupe(uint64(matchedCount - 1))
		}
		return matchedCount > 0
	}

	scanWidth := uint64(0)

	cur := ov.newCursor(br, o.Desc)
	bucket := 0
	if o.Desc {
		bucket = ov.keyCount() - 1
	}
	for {
		_, bm, ok := cur.next()
		if !ok {
			break
		}
		curBucket := bucket
		if o.Desc {
			bucket--
		} else {
			bucket++
		}
		if bm.IsEmpty() {
			continue
		}
		scanWidth++
		trace.addExamined(bm.Cardinality())
		stop := false
		bm.ForEach(func(idx uint64) bool {
			if !matchWithMetrics(idx, curBucket) {
				return true
			}
			if skip > 0 {
				skip--
				return true
			}
			out = append(out, qv.idFromIdxNoLock(idx))
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

func (qv *queryView[K, V]) shouldPreferOROrderFallbackFirst(q *qx.QX, branches plannerORBranches) bool {
	d, ok := qv.decideOROrderFallbackFirst(q, branches)
	return ok && d.prefer
}

func (qv *queryView[K, V]) decideOROrderFallbackFirst(q *qx.QX, branches plannerORBranches) (plannerOROrderFallbackDecision, bool) {
	need, ok := orderWindow(q)
	if !ok || need <= 0 {
		return plannerOROrderFallbackDecision{}, false
	}
	if len(q.Order) != 1 {
		return plannerOROrderFallbackDecision{}, false
	}

	orderField := q.Order[0].Field
	ov := qv.fieldOverlay(orderField)
	mergeStats := qv.orderMergeBranchStats(orderField, branches, ov)
	routeCost, ok := qv.estimateOROrderMergeRouteCost(q, branches, need, mergeStats)
	if !ok {
		return plannerOROrderFallbackDecision{}, false
	}
	avgChecks := routeCost.avgChecks
	if avgChecks <= 0 {
		avgChecks = 1
	}
	fallbackCollectFast := ov.hasData()
	d := plannerOROrderFallbackDecision{
		routeCost:           routeCost,
		avgChecks:           avgChecks,
		fallbackCollectFast: fallbackCollectFast,
	}

	if !fallbackCollectFast {
		// Without direct branch collection fallback-first usually adds avoidable
		// allocations (branch subqueries + id->idx roundtrip).
		d.prefer = false
		d.reason = "order_field_no_index_data"
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
			(routeCost.hasSelectiveLead || routeCost.hasPrefixTailRisk) {
			d.prefer = true
			d.reason = "deep_offset_risk"
			return d, true
		}
		if routeCost.hasPrefixTailRisk && routeCost.fallback <= routeCost.kWay*offsetFallbackGain {
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

func (qv *queryView[K, V]) shouldUseOROrderKWayRuntimeFallback(q *qx.QX, branches plannerORBranches, needWindow int) bool {
	d, ok := qv.decideOROrderKWayRuntimeFallback(q, branches, needWindow)
	return ok && d.enable
}

func (qv *queryView[K, V]) decideOROrderKWayRuntimeFallback(q *qx.QX, branches plannerORBranches, needWindow int) (plannerOROrderRuntimeGuardDecision, bool) {
	if needWindow <= 0 || len(q.Order) != 1 || len(branches) < 2 {
		return plannerOROrderRuntimeGuardDecision{}, false
	}

	orderField := q.Order[0].Field
	if orderField == "" {
		return plannerOROrderRuntimeGuardDecision{}, false
	}

	ov := qv.fieldOverlay(orderField)
	mergeStats := qv.orderMergeBranchStats(orderField, branches, ov)
	routeCost, ok := qv.estimateOROrderMergeRouteCost(q, branches, needWindow, mergeStats)
	if !ok {
		return plannerOROrderRuntimeGuardDecision{}, false
	}

	avgChecks := routeCost.avgChecks
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

	hasPrefixTailRisk := branches.hasNonOrderPrefixTailRisk(orderField)
	if hasPrefixTailRisk && q.Offset > 0 {
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

func (qv *queryView[K, V]) execPlanOROrderMerge(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]K, bool, error) {
	fallbackDec, decOK := qv.decideOROrderFallbackFirst(q, branches)
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
		return qv.execPlanOROrderMergeFallback(q, branches, trace)
	}
	out, ok, err := qv.execPlanOROrderKWay(q, branches, trace)
	if ok || err != nil {
		return out, ok, err
	}
	return qv.execPlanOROrderMergeFallback(q, branches, trace)
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
	residualChecks []int
	overlay        fieldOverlay
	desc           bool
	single         int
	residualSingle int
	allChecksExact bool

	startBucket int
	endBucket   int

	nextBucket int
	curBucket  int
	curIter    posting.Iterator
	curExact   bool
	curChecks  []int
	curSingle  int
	bucketWork posting.List

	onBucketVisit func(int)

	totalExamined  *uint64
	branchExamined *uint64
	branchEmitted  *uint64

	has bool
	cur uint64
}

type plannerOrderIndexView struct {
	overlay fieldOverlay
	release func()
}

func (v plannerOrderIndexView) close() {
	if v.release != nil {
		v.release()
	}
}

// plannerOrderIndexSnapshotView returns the current immutable order-index slice.
func (qv *queryView[K, V]) plannerOrderIndexSnapshotView(field string) (plannerOrderIndexView, bool) {
	ov := qv.fieldOverlay(field)
	if !ov.hasData() {
		return plannerOrderIndexView{}, false
	}
	return plannerOrderIndexView{overlay: ov}, true
}

func (it *plannerOROrderBranchIter) init() {
	if it.startBucket < 0 {
		it.startBucket = 0
	}
	if it.endBucket <= 0 || it.endBucket > it.overlay.keyCount() {
		it.endBucket = it.overlay.keyCount()
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
		it.curIter.Release()
		it.curIter = nil
	}
	it.bucketWork.Release()
	it.bucketWork = posting.List{}
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
				it.curIter.Release()
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
				matched := false
				if it.curSingle >= 0 {
					matched = it.branch.preds[it.curSingle].matches(idx)
				} else {
					matched = it.branch.matchesChecks(idx, it.curChecks)
				}
				if matched {
					it.cur = idx
					it.has = true
					if it.branchEmitted != nil {
						*it.branchEmitted = *it.branchEmitted + 1
					}
					return true
				}
			}
			it.curIter.Release()
			it.curIter = nil
		}

		if it.desc {
			if it.nextBucket < it.startBucket {
				return false
			}
			b := it.nextBucket
			it.nextBucket--
			bucket := it.overlay.postingAt(b)
			if bucket.IsEmpty() {
				continue
			}
			if it.onBucketVisit != nil {
				it.onBucketVisit(b)
			}
			it.curBucket = b
			if idx, ok := bucket.TrySingle(); ok {
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
			if len(it.exactChecks) > 0 {
				mode, exactIDs, nextBucketWork, card := plannerFilterPostingByChecks(it.branch.preds, it.exactChecks, bucket, it.bucketWork, true)
				it.bucketWork = nextBucketWork
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
						it.curIter = exactIDs.Iter()
						it.curExact = true
						it.curChecks = nil
						it.curSingle = -1
						continue
					}
					it.curIter = exactIDs.Iter()
					it.curExact = false
					it.curChecks = it.residualChecks
					it.curSingle = it.residualSingle
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
							*it.branchEmitted = *it.branchEmitted + exactIDs.Cardinality()
						}
						it.curIter = exactIDs.Iter()
						it.curExact = true
						it.curChecks = nil
						it.curSingle = -1
						continue
					}
					it.curIter = exactIDs.Iter()
					it.curExact = false
					it.curChecks = it.residualChecks
					it.curSingle = it.residualSingle
					continue
				}
			}
			it.curIter = bucket.Iter()
			it.curExact = false
			it.curChecks = it.checks
			it.curSingle = it.single
			continue
		}

		if it.nextBucket >= it.endBucket {
			return false
		}
		b := it.nextBucket
		it.nextBucket++
		bucket := it.overlay.postingAt(b)
		if bucket.IsEmpty() {
			continue
		}
		if it.onBucketVisit != nil {
			it.onBucketVisit(b)
		}
		it.curBucket = b
		if idx, ok := bucket.TrySingle(); ok {
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
		if len(it.exactChecks) > 0 {
			mode, exactIDs, nextBucketWork, card := plannerFilterPostingByChecks(it.branch.preds, it.exactChecks, bucket, it.bucketWork, true)
			it.bucketWork = nextBucketWork
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
					it.curIter = exactIDs.Iter()
					it.curExact = true
					it.curChecks = nil
					it.curSingle = -1
					continue
				}
				it.curIter = exactIDs.Iter()
				it.curExact = false
				it.curChecks = it.residualChecks
				it.curSingle = it.residualSingle
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
						*it.branchEmitted = *it.branchEmitted + exactIDs.Cardinality()
					}
					it.curIter = exactIDs.Iter()
					it.curExact = true
					it.curChecks = nil
					it.curSingle = -1
					continue
				}
				it.curIter = exactIDs.Iter()
				it.curExact = false
				it.curChecks = it.residualChecks
				it.curSingle = it.residualSingle
				continue
			}
		}
		it.curIter = bucket.Iter()
		it.curExact = false
		it.curChecks = it.checks
		it.curSingle = it.single
	}
}

func (qv *queryView[K, V]) execPlanOROrderKWay(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]K, bool, error) {
	needWindow, ok := orderWindow(q)
	if !ok || needWindow <= 0 {
		return nil, false, nil
	}

	o := q.Order[0]
	fm := qv.fields[o.Field]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}
	orderView, ok := qv.plannerOrderIndexSnapshotView(o.Field)
	if !ok {
		return nil, false, nil
	}
	defer orderView.close()
	ov := orderView.overlay
	if !ov.hasData() {
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
	if guardDec, ok := qv.decideOROrderKWayRuntimeFallback(q, branches, needWindow); ok {
		allowRuntimeFallback = guardDec.enable
		runtimeShape = plannerORKWayRuntimeShapeFromGuard(guardDec, q)
		if trace != nil {
			trace.setOROrderRuntimeGuard(guardDec.enable, guardDec.reason)
		}
	}

	fullTrace := trace.full()
	var (
		branchMetrics []TraceORBranch
		examined      uint64
		scanWidth     uint64
		dedupe        uint64
	)
	var (
		branchChecks      [plannerORBranchLimit][]int
		branchExactChecks [plannerORBranchLimit][]int
		branchResidual    [plannerORBranchLimit][]int
		checkBufs         [plannerORBranchLimit]*intSliceBuf
		exactCheckBufs    [plannerORBranchLimit]*intSliceBuf
		residualCheckBufs [plannerORBranchLimit]*intSliceBuf
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
			if residualCheckBufs[i] != nil {
				residualCheckBufs[i].values = branchResidual[i]
				releaseIntSliceBuf(residualCheckBufs[i])
			}
		}
	}()
	var examinedPtr *uint64
	if trace != nil {
		if fullTrace {
			branchMetrics = make([]TraceORBranch, len(branches))
			for i := range branchMetrics {
				branchMetrics[i].Index = i
			}
		}
		examinedPtr = &examined
	}

	var bucketSeen []bool
	if fullTrace {
		bucketSeen = make([]bool, ov.keyCount())
	}
	markBucketVisited := func(bucket int) {
		if !fullTrace || bucket < 0 || bucket >= len(bucketSeen) {
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
		iters = itersBuf.values[:len(branches)]
		clear(iters)
		defer func() {
			clear(iters)
			itersBuf.values = iters[:0]
			releasePlannerOROrderIterSliceBuf(itersBuf)
		}()

		mergeItemsBuf = getPlannerOROrderMergeItemSliceBuf(len(branches))
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
		br, covered, rangeOK := qv.extractOrderRangeCoverageOverlay(o.Field, branches[i].preds, ov)
		if !rangeOK {
			return nil, false, nil
		}
		for pi := range covered {
			if covered[pi] {
				branches[i].preds[pi].covered = true
			}
		}
		branchStart, branchEnd := br.baseStart, br.baseEnd
		if branchStart >= branchEnd {
			if fullTrace {
				branchMetrics[i].Skipped = true
				branchMetrics[i].SkipReason = "order_range_empty"
			}
			continue
		}

		var bePtr *uint64
		var bmPtr *uint64
		if fullTrace {
			bePtr = &branchMetrics[i].RowsExamined
			bmPtr = &branchMetrics[i].RowsEmitted
		}
		checkBufs[i] = getIntSliceBuf(len(branches[i].preds))
		branchChecks[i] = branches[i].buildMatchChecks(checkBufs[i].values)
		exactCheckBufs[i] = getIntSliceBuf(len(branchChecks[i]))
		branchExactChecks[i] = branches[i].buildPostingFilterChecks(exactCheckBufs[i].values, branchChecks[i])
		residualCheckBufs[i] = getIntSliceBuf(len(branchChecks[i]))
		branchResidual[i] = plannerResidualChecks(residualCheckBufs[i].values, branchChecks[i], branchExactChecks[i])
		singleCheck := -1
		if len(branchChecks[i]) == 1 {
			singleCheck = branchChecks[i][0]
		}
		residualSingle := -1
		if len(branchResidual[i]) == 1 {
			residualSingle = branchResidual[i][0]
		}
		iters[i] = plannerOROrderBranchIter{
			branch:         &branches[i],
			checks:         branchChecks[i],
			exactChecks:    branchExactChecks[i],
			residualChecks: branchResidual[i],
			overlay:        ov,
			desc:           o.Desc,
			single:         singleCheck,
			residualSingle: residualSingle,
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
		} else if fullTrace {
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
	defer releaseU64Set(&seen)
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

		out = append(out, qv.idFromIdxNoLock(item.idx))
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

func (qv *queryView[K, V]) execPlanOROrderMergeFallback(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]K, bool, error) {
	need, ok := orderWindow(q)
	if !ok || need <= 0 {
		return nil, false, nil
	}

	var candidateIDs posting.List
	defer candidateIDs.Release()
	candidateSet := newPostingLazySetBuilder(satMulUint64(uint64(need), uint64(len(branches))))
	defer candidateSet.release()

	fullTrace := trace.full()
	var branchMetrics []TraceORBranch
	if fullTrace {
		branchMetrics = make([]TraceORBranch, len(branches))
		for i := range branchMetrics {
			branchMetrics[i].Index = i
		}
	}

	orderField := q.Order[0].Field
	directBranchCollect := false
	var orderOV fieldOverlay
	if orderField != "" {
		orderOV = qv.fieldOverlay(orderField)
		directBranchCollect = orderOV.hasData()
	}

	for i := range branches {
		branch := branches[i]
		if branch.alwaysTrue {
			return nil, false, nil
		}

		branchLimit, branchExhaustive, err := qv.planOROrderMergeBranchLimit(branch, need)
		if err != nil {
			return nil, true, err
		}
		if branchLimit <= 0 {
			if fullTrace {
				branchMetrics[i].Skipped = true
				branchMetrics[i].SkipReason = "branch_limit_zero"
			}
			continue
		}

		if directBranchCollect {
			emitted, examined, dedupe, okCollect := qv.collectOROrderFallbackBranchCandidates(
				&branches[i], q.Order[0], branchLimit, orderOV, &candidateSet,
			)
			if okCollect {
				if trace != nil {
					trace.addExamined(examined)
					trace.addDedupe(dedupe)
				}
				if fullTrace {
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

		ids, err := qv.execPreparedQuery(subQ)
		if err != nil {
			return nil, true, err
		}
		if trace != nil {
			trace.addExamined(uint64(len(ids)))
		}
		if fullTrace {
			branchMetrics[i].RowsExamined += uint64(len(ids))
			branchMetrics[i].RowsEmitted += uint64(len(ids))
		}
		if len(ids) == 0 {
			continue
		}

		qv.root.forEachIdxFromID(ids, func(idx uint64) {
			if candidateSet.addChecked(idx) {
				return
			}
			if trace != nil {
				trace.addDedupe(1)
			}
		})
	}

	candidateIDs = candidateSet.finish(false)

	if candidateIDs.IsEmpty() {
		if trace != nil {
			trace.setORBranches(branchMetrics)
			trace.setEarlyStopReason("no_candidates")
		}
		return nil, true, nil
	}

	o := q.Order[0]
	fm := qv.fields[o.Field]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}
	orderView, ok := qv.plannerOrderIndexSnapshotView(o.Field)
	if !ok {
		return nil, false, nil
	}
	defer orderView.close()
	ov := orderView.overlay
	if !ov.hasData() {
		return nil, true, nil
	}

	skip := q.Offset
	needOut := q.Limit
	if needOut == 0 {
		return nil, true, nil
	}

	totalCandidates := candidateIDs.Cardinality()
	out := make([]K, 0, int(min(needOut, totalCandidates)))

	seenCandidates := uint64(0)
	scanWidth := uint64(0)

	emit := func(idx uint64) bool {
		if !candidateIDs.Contains(idx) {
			return false
		}
		seenCandidates++
		if skip > 0 {
			skip--
			return false
		}
		out = append(out, qv.idFromIdxNoLock(idx))
		needOut--
		return needOut == 0
	}

	cur := ov.newCursor(ov.rangeForBounds(rangeBounds{has: true}), o.Desc)
	for {
		_, bm, ok := cur.next()
		if !ok {
			break
		}
		if bm.IsEmpty() || !bm.Intersects(candidateIDs) {
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

type plannerOROrderFallbackAccumulator struct {
	branch         *plannerORBranch
	dst            *postingLazySetBuilder
	checks         []int
	residualChecks []int
	singleCheck    int
	residualSingle int
	limit          uint64
	emitted        uint64
	examined       uint64
	dedupe         uint64
}

func (c *plannerOROrderFallbackAccumulator) add(idx uint64) bool {
	c.emitted++
	if !c.dst.addChecked(idx) {
		c.dedupe++
	}
	return c.emitted >= c.limit
}

func (c *plannerOROrderFallbackAccumulator) emitPostingNoChecks(ids posting.List, card uint64) bool {
	c.examined += card
	if idx, ok := ids.TrySingle(); ok {
		return c.add(idx)
	}
	it := ids.Iter()
	for it.HasNext() {
		if c.add(it.Next()) {
			it.Release()
			return true
		}
	}
	it.Release()
	return false
}

func (c *plannerOROrderFallbackAccumulator) emitPostingChecked(ids posting.List) bool {
	if idx, ok := ids.TrySingle(); ok {
		return c.emitExact(idx)
	}
	it := ids.Iter()
	for it.HasNext() {
		if c.emitExact(it.Next()) {
			it.Release()
			return true
		}
	}
	it.Release()
	return false
}

func (c *plannerOROrderFallbackAccumulator) emit(idx uint64) bool {
	c.examined++
	if c.singleCheck >= 0 {
		if !c.branch.preds[c.singleCheck].matches(idx) {
			return false
		}
	} else if !c.branch.matchesChecks(idx, c.checks) {
		return false
	}
	return c.add(idx)
}

func (c *plannerOROrderFallbackAccumulator) emitExact(idx uint64) bool {
	c.examined++
	if c.residualSingle >= 0 {
		if !c.branch.preds[c.residualSingle].matches(idx) {
			return false
		}
	} else if !c.branch.matchesChecks(idx, c.residualChecks) {
		return false
	}
	return c.add(idx)
}

// collectOROrderFallbackBranchCandidates collects up to branchLimit matching ids
// from one OR branch directly from the base order index and writes them into dst.
// Returns ok=false when fast-path preconditions are not satisfied.
func (qv *queryView[K, V]) collectOROrderFallbackBranchCandidates(
	branch *plannerORBranch,
	order qx.Order,
	branchLimit int,
	ov fieldOverlay,
	dst *postingLazySetBuilder,
) (uint64, uint64, uint64, bool) {
	if branchLimit <= 0 {
		return 0, 0, 0, false
	}
	if order.Type != qx.OrderBasic || order.Field == "" {
		return 0, 0, 0, false
	}
	fm := qv.fields[order.Field]
	if fm == nil || fm.Slice {
		return 0, 0, 0, false
	}
	if !ov.hasData() {
		ov = qv.fieldOverlay(order.Field)
	}
	if !ov.hasData() {
		return 0, 0, 0, false
	}
	br, covered, rangeOK := qv.extractOrderRangeCoverageOverlay(order.Field, branch.preds, ov)
	if !rangeOK {
		return 0, 0, 0, false
	}
	for pi := range covered {
		if covered[pi] {
			branch.preds[pi].covered = true
		}
	}
	start, end := br.baseStart, br.baseEnd
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
	exactChecks := branch.buildPostingFilterChecks(exactChecksBuf.values, checks)
	defer func() {
		exactChecksBuf.values = exactChecks
		releaseIntSliceBuf(exactChecksBuf)
	}()
	residualChecksBuf := getIntSliceBuf(len(checks))
	residualChecks := plannerResidualChecks(residualChecksBuf.values, checks, exactChecks)
	defer func() {
		residualChecksBuf.values = residualChecks
		releaseIntSliceBuf(residualChecksBuf)
	}()
	singleCheck := -1
	if len(checks) == 1 {
		singleCheck = checks[0]
	}
	residualSingle := -1
	if len(residualChecks) == 1 {
		residualSingle = residualChecks[0]
	}
	var bucketWork posting.List
	defer bucketWork.Release()
	allChecksExact := len(checks) > 0 && len(exactChecks) == len(checks)
	acc := plannerOROrderFallbackAccumulator{
		branch:         branch,
		dst:            dst,
		checks:         checks,
		residualChecks: residualChecks,
		singleCheck:    singleCheck,
		residualSingle: residualSingle,
		limit:          uint64(branchLimit),
	}

	cur := ov.newCursor(ov.rangeByRanks(start, end), order.Desc)
	for {
		_, bucket, ok := cur.next()
		if !ok {
			break
		}
		if bucket.IsEmpty() {
			continue
		}
		if idx, ok := bucket.TrySingle(); ok {
			if acc.emit(idx) {
				return acc.emitted, acc.examined, acc.dedupe, true
			}
			continue
		}
		if len(exactChecks) > 0 {
			mode, exactIDs, nextBucketWork, card := plannerFilterPostingByChecks(branch.preds, exactChecks, bucket, bucketWork, true)
			bucketWork = nextBucketWork
			switch mode {
			case plannerPredicateBucketEmpty:
				acc.examined += card
				continue
			case plannerPredicateBucketAll, plannerPredicateBucketExact:
				if allChecksExact {
					if acc.emitPostingNoChecks(exactIDs, card) {
						return acc.emitted, acc.examined, acc.dedupe, true
					}
					continue
				}
				if acc.emitPostingChecked(exactIDs) {
					return acc.emitted, acc.examined, acc.dedupe, true
				}
				continue
			}
		}
		it := bucket.Iter()
		for it.HasNext() {
			if acc.emit(it.Next()) {
				it.Release()
				return acc.emitted, acc.examined, acc.dedupe, true
			}
		}
		it.Release()
	}
	return acc.emitted, acc.examined, acc.dedupe, true
}

func (qv *queryView[K, V]) planOROrderMergeBranchLimit(branch plannerORBranch, need int) (limit int, exhaustive bool, err error) {
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
		if !qv.canPrecountORBranchExpr(e) {
			return limit, false, nil
		}
	}

	if cnt, ok := qv.countORBranchByUniqueLead(branch); ok {
		if cnt == 0 {
			return 0, true, nil
		}
		if cnt < uint64(limit) {
			return int(cnt), true, nil
		}
		return limit, false, nil
	}

	var cnt uint64
	cnt, err = qv.countPreparedExpr(branch.expr)
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

func (qv *queryView[K, V]) canPrecountORBranchExpr(e qx.Expr) bool {
	if !isMaterializedScalarCacheOp(e.Op) {
		return true
	}
	cacheKey := qv.materializedPredCacheKey(e)
	if cacheKey == "" {
		return false
	}
	_, ok := qv.snap.loadMaterializedPred(cacheKey)
	return ok
}

// countORBranchByUniqueLead tries to count one OR branch by scanning a unique
// EQ predicate lead and validating the remaining predicates with contains().
func (qv *queryView[K, V]) countORBranchByUniqueLead(branch plannerORBranch) (uint64, bool) {
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
		if !qv.isPositiveUniqueEqExpr(p.expr) {
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
	defer it.Release()

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

type plannerORNoOrderMode uint8

const (
	plannerORNoOrderModeRun plannerORNoOrderMode = iota
	plannerORNoOrderModeUniverse
	plannerORNoOrderModeUnsupported
)

func plannerORNoOrderClassify(branches plannerORBranches) plannerORNoOrderMode {
	for i := range branches {
		if branches[i].alwaysTrue {
			return plannerORNoOrderModeUniverse
		}
		if branches[i].lead == nil || !branches[i].lead.hasIter() {
			return plannerORNoOrderModeUnsupported
		}
	}
	return plannerORNoOrderModeRun
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

func (qv *queryView[K, V]) execPlanORNoOrder(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]K, bool) {
	switch plannerORNoOrderClassify(branches) {
	case plannerORNoOrderModeUniverse:
		return qv.execPlanORUniverseNoOrder(q, trace), true
	case plannerORNoOrderModeUnsupported:
		return nil, false
	}

	if q.Offset == 0 && len(branches) >= 3 {
		if out, ok := qv.execPlanORNoOrderAdaptiveCore(q, branches, trace); ok {
			return out, true
		}
	}
	return qv.execPlanORNoOrderBaselineCore(q, branches, trace)
}

// execPlanORNoOrderAdaptive collects top-N ids for no-order OR via adaptive per-branch probing.
//
// It starts with cheap branches and increases probe budgets only when needed to prove top-N completeness.
func (qv *queryView[K, V]) execPlanORNoOrderAdaptive(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]K, bool) {
	switch plannerORNoOrderClassify(branches) {
	case plannerORNoOrderModeUniverse:
		return qv.execPlanORUniverseNoOrder(q, trace), true
	case plannerORNoOrderModeUnsupported:
		return nil, false
	}
	return qv.execPlanORNoOrderAdaptiveCore(q, branches, trace)
}

func (qv *queryView[K, V]) execPlanORNoOrderAdaptiveCore(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]K, bool) {
	if q.Limit == 0 || q.Offset != 0 {
		return nil, false
	}

	need := int(q.Limit)
	if need <= 0 {
		return nil, true
	}

	fullTrace := trace.full()
	var (
		examined    uint64
		examinedPtr *uint64
	)
	var branchMetrics []TraceORBranch
	if trace != nil {
		examinedPtr = &examined
		if fullTrace {
			branchMetrics = make([]TraceORBranch, len(branches))
			for i := range branchMetrics {
				branchMetrics[i].Index = i
			}
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
		if fullTrace {
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
		defer releaseU64Set(&seen)
	}
	topBuf := getUint64SliceBuf(need)
	top := topBuf.values
	defer func() {
		topBuf.values = top
		releaseUint64SliceBuf(topBuf)
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

		if fullTrace && len(top) == need {
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
		out = append(out, qv.idFromIdxNoLock(idx))
	}
	return out, true
}

func (qv *queryView[K, V]) execPlanORNoOrderBaseline(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]K, bool) {
	switch plannerORNoOrderClassify(branches) {
	case plannerORNoOrderModeUniverse:
		return qv.execPlanORUniverseNoOrder(q, trace), true
	case plannerORNoOrderModeUnsupported:
		return nil, false
	}
	return qv.execPlanORNoOrderBaselineCore(q, branches, trace)
}

func (qv *queryView[K, V]) execPlanORNoOrderBaselineCore(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]K, bool) {
	fullTrace := trace.full()
	var (
		examined    uint64
		examinedPtr *uint64
	)
	var branchMetrics []TraceORBranch
	if trace != nil {
		examinedPtr = &examined
		if fullTrace {
			branchMetrics = make([]TraceORBranch, len(branches))
			for i := range branchMetrics {
				branchMetrics[i].Index = i
			}
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
		if fullTrace {
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
	needWindow := need
	if skip > 0 {
		if skip > uint64(^uint(0)>>1)-uint64(needWindow) {
			needWindow = int(^uint(0) >> 1)
		} else {
			needWindow += int(skip)
		}
	}
	useLinearSeen := needWindow <= plannerORNoOrderLinearSeenNeedMax
	var (
		seen    u64set
		seenBuf *uint64SliceBuf
	)
	if useLinearSeen {
		seenBuf = getUint64SliceBuf(needWindow)
		seenBuf.values = seenBuf.values[:0]
		defer func() {
			releaseUint64SliceBuf(seenBuf)
		}()
	} else {
		seen = newU64Set(max(64, needWindow*2))
		defer releaseU64Set(&seen)
	}

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

		if useLinearSeen {
			dup := false
			for _, existing := range seenBuf.values {
				if existing == minIdx {
					dup = true
					break
				}
			}
			if dup {
				if trace != nil {
					trace.addDedupe(1)
				}
				continue
			}
			seenBuf.values = append(seenBuf.values, minIdx)
		} else {
			// No-order branch leads are only required to be iterable, not globally
			// monotone by idx. Identical ids may therefore surface again long after
			// the first branch emitted them, so baseline merge must dedupe against
			// the full seen set, not only synchronized branch heads.
			if !seen.Add(minIdx) {
				if trace != nil {
					trace.addDedupe(1)
				}
				continue
			}
		}

		if skip > 0 {
			skip--
			continue
		}

		out = append(out, qv.idFromIdxNoLock(minIdx))
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

func (qv *queryView[K, V]) execPlanORUniverseNoOrder(q *qx.QX, trace *queryTrace) []K {
	skip := q.Offset
	need := int(q.Limit)
	out := make([]K, 0, need)
	stopReason := "input_exhausted"

	if trace != nil {
		trace.addExamined(qv.snapshotUniverseCardinality())
	}

	it := qv.snapshotUniverseView().Iter()
	defer it.Release()
	for it.HasNext() {
		idx := it.Next()
		if skip > 0 {
			skip--
			continue
		}
		out = append(out, qv.idFromIdxNoLock(idx))
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
