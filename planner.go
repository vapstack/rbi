package rbi

import (
	"math"
	"time"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/schema"
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
	expr qir.Expr

	preds               predicateSet
	alwaysTrue          bool
	estCard             uint64
	estKnown            bool
	coveredRangeBounded bool
	coveredRangeStart   int
	coveredRangeEnd     int
	leadIdx             int
}

type plannerORBranches struct {
	owner *pooled.Slice[plannerORBranch]
}

func newPlannerORBranches(capHint int) plannerORBranches {
	owner := plannerORBranchSlicePool.Get()
	if capHint > 0 {
		owner.Grow(capHint)
	}
	return plannerORBranches{owner: owner}
}

func (branches plannerORBranches) Len() int {
	if branches.owner == nil {
		return 0
	}
	return branches.owner.Len()
}

func (branches plannerORBranches) Get(i int) plannerORBranch {
	return branches.owner.Get(i)
}

func (branches plannerORBranches) GetPtr(i int) *plannerORBranch {
	return branches.owner.GetPtr(i)
}

func (branches *plannerORBranches) Append(br plannerORBranch) {
	if branches.owner == nil {
		branches.owner = plannerORBranchSlicePool.Get()
	}
	branches.owner.Append(br)
}

func (branches *plannerORBranches) Release() {
	if branches == nil || branches.owner == nil {
		return
	}
	plannerORBranchSlicePool.Put(branches.owner)
	branches.owner = nil
}

func plannerORPredicateLess(a, b predicate) bool {
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

func plannerORSortPredicates(preds *predicateSet) {
	if preds == nil || preds.Len() <= 1 {
		return
	}

	// Predicate lists are tiny in practice; insertion sort avoids allocations
	// and keeps hot-path overhead minimal.
	for i := 1; i < preds.Len(); i++ {
		cur := preds.Get(i)
		j := i
		for j > 0 && plannerORPredicateLess(cur, preds.Get(j-1)) {
			preds.Set(j, preds.Get(j-1))
			j--
		}
		preds.Set(j, cur)
	}
}

func buildPlannerORBranch(op qir.Expr, preds predicateSet) (plannerORBranch, bool, bool) {
	plannerORSortPredicates(&preds)

	branch := newPlannerORBranch(op, preds)

	hasFalse := false
	allTrue := true
	leadIdx := -1

	for i := 0; i < preds.Len(); i++ {
		p := preds.GetPtr(i)
		if p.alwaysFalse {
			hasFalse = true
			break
		}
		if p.alwaysTrue || p.covered {
			continue
		}
		allTrue = false
		if !p.hasContains() {
			preds.Release()
			return plannerORBranch{}, false, false
		}
		if p.hasIter() {
			if leadIdx == -1 || p.estCard < preds.Get(leadIdx).estCard {
				leadIdx = i
			}
		}
	}

	if hasFalse {
		preds.Release()
		return plannerORBranch{}, false, true
	}
	if allTrue {
		branch.alwaysTrue = true
	} else {
		branch.leadIdx = leadIdx
	}

	return branch, true, true
}

func newPlannerORBranch(op qir.Expr, preds predicateSet) plannerORBranch {
	br := plannerORBranch{
		expr:    op,
		preds:   preds,
		leadIdx: -1,
	}
	return br
}

func (b plannerORBranch) predLen() int {
	return b.preds.Len()
}

func (b plannerORBranch) pred(i int) predicate {
	return b.preds.Get(i)
}

func (b *plannerORBranch) predPtr(i int) *predicate {
	if b == nil {
		return nil
	}
	return b.preds.GetPtr(i)
}

func (b plannerORBranch) hasLead() bool {
	return b.leadIdx >= 0 && b.leadIdx < b.predLen()
}

func (b plannerORBranch) leadPred() predicate {
	return b.pred(b.leadIdx)
}

func (b *plannerORBranch) leadPtr() *predicate {
	if b == nil || !b.hasLead() {
		return nil
	}
	return b.predPtr(b.leadIdx)
}

func releasePlannerORBranchPredicates(br plannerORBranch) {
	br.preds.Release()
}

func (b *plannerORBranch) buildMatchChecks(dst []int) []int {
	dst = dst[:0]
	if b.alwaysTrue {
		return dst
	}
	for i := 0; i < b.predLen(); i++ {
		p := b.pred(i)
		if p.covered || p.alwaysTrue {
			continue
		}
		dst = append(dst, i)
	}
	sortActivePredicatesReader(dst, b.preds)
	return dst
}

func (b *plannerORBranch) buildMatchChecksBuf(dst []int) []int {
	dst = dst[:0]
	if b.alwaysTrue {
		return dst
	}
	for i := 0; i < b.predLen(); i++ {
		p := b.pred(i)
		if p.covered || p.alwaysTrue {
			continue
		}
		dst = append(dst, i)
	}
	sortActivePredicatesBufReader(dst, b.preds)
	return dst
}

func (b *plannerORBranch) buildPostingFilterChecks(dst []int, checks []int) []int {
	dst = dst[:0]
	if b.alwaysTrue {
		return dst
	}
	for _, i := range checks {
		if b.pred(i).supportsExactBucketPostingFilter() {
			dst = append(dst, i)
		}
	}
	return dst
}

func (b *plannerORBranch) matchesChecksBuf(idx uint64, checks []int) bool {
	if b.alwaysTrue {
		return true
	}
	switch len(checks) {
	case 0:
		return true
	case 1:
		p0 := b.predPtr(checks[0])
		return !p0.alwaysFalse && p0.hasContains() && p0.matches(idx)
	case 2:
		p0 := b.predPtr(checks[0])
		if p0.alwaysFalse || !p0.hasContains() || !p0.matches(idx) {
			return false
		}
		p1 := b.predPtr(checks[1])
		return !p1.alwaysFalse && p1.hasContains() && p1.matches(idx)
	case 3:
		p0 := b.predPtr(checks[0])
		if p0.alwaysFalse || !p0.hasContains() || !p0.matches(idx) {
			return false
		}
		p1 := b.predPtr(checks[1])
		if p1.alwaysFalse || !p1.hasContains() || !p1.matches(idx) {
			return false
		}
		p2 := b.predPtr(checks[2])
		return !p2.alwaysFalse && p2.hasContains() && p2.matches(idx)
	case 4:
		p0 := b.predPtr(checks[0])
		if p0.alwaysFalse || !p0.hasContains() || !p0.matches(idx) {
			return false
		}
		p1 := b.predPtr(checks[1])
		if p1.alwaysFalse || !p1.hasContains() || !p1.matches(idx) {
			return false
		}
		p2 := b.predPtr(checks[2])
		if p2.alwaysFalse || !p2.hasContains() || !p2.matches(idx) {
			return false
		}
		p3 := b.predPtr(checks[3])
		return !p3.alwaysFalse && p3.hasContains() && p3.matches(idx)
	}
	for _, check := range checks {
		p := b.predPtr(check)
		if p.alwaysFalse || !p.hasContains() || !p.matches(idx) {
			return false
		}
	}
	return true
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

func plannerResidualChecksBuf(dst, checks, exactChecks []int) []int {
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

func plannerORBranchBuildActiveChecksWithCoveredBuf(dst []int, branch *plannerORBranch, covered []bool) []int {
	dst = dst[:0]
	if branch == nil {
		return dst
	}
	if covered == nil {
		return branch.buildMatchChecksBuf(dst)
	}
	for pi := 0; pi < branch.predLen(); pi++ {
		p := branch.pred(pi)
		if p.alwaysFalse {
			return dst[:0]
		}
		if p.alwaysTrue || !p.hasContains() {
			continue
		}
		if len(covered) != 0 && covered[pi] {
			continue
		}
		dst = append(dst, pi)
	}
	sortActivePredicatesBufReader(dst, branch.preds)
	return dst
}

func plannerORBranchCheckCounts(branch plannerORBranch, covered []bool) (int, int) {
	for pi := 0; pi < branch.predLen(); pi++ {
		if branch.pred(pi).alwaysFalse {
			return 0, 0
		}
	}
	if branch.predLen() <= plannerPredicateFastPathMaxLeaves {
		var checksInline [plannerPredicateFastPathMaxLeaves]int
		checks := checksInline[:0]
		if len(covered) == 0 {
			checks = branch.buildMatchChecks(checks)
		} else {
			for pi := 0; pi < branch.predLen(); pi++ {
				p := branch.pred(pi)
				if p.alwaysTrue || p.alwaysFalse || !p.hasContains() {
					continue
				}
				if covered[pi] {
					continue
				}
				checks = append(checks, pi)
			}
		}
		var exactChecksInline [plannerPredicateFastPathMaxLeaves]int
		var residualChecksInline [plannerPredicateFastPathMaxLeaves]int
		exactChecks := branch.buildPostingFilterChecks(exactChecksInline[:0], checks)
		residualChecks := plannerResidualChecks(residualChecksInline[:0], checks, exactChecks)
		return len(checks), len(residualChecks)
	}

	checksBuf := pooled.GetIntSlice(branch.predLen())
	checksBuf = plannerORBranchBuildActiveChecksWithCoveredBuf(checksBuf, &branch, covered)
	exactChecksBuf := pooled.GetIntSlice(len(checksBuf))
	exactChecksBuf = buildExactBucketPostingFilterActiveBufReader(exactChecksBuf, checksBuf, branch.preds)
	residualChecksBuf := pooled.GetIntSlice(len(checksBuf))
	residualChecksBuf = plannerResidualChecksBuf(residualChecksBuf, checksBuf, exactChecksBuf)
	streamChecks := len(checksBuf)
	mergeChecks := len(residualChecksBuf)
	pooled.ReleaseIntSlice(residualChecksBuf)
	pooled.ReleaseIntSlice(exactChecksBuf)
	pooled.ReleaseIntSlice(checksBuf)
	return streamChecks, mergeChecks
}

func (b *plannerORBranch) matchesChecks(idx uint64, checks []int) bool {
	if b.alwaysTrue {
		return true
	}
	switch len(checks) {
	case 0:
		return true
	case 1:
		p0 := b.predPtr(checks[0])
		return !p0.alwaysFalse && p0.hasContains() && p0.matches(idx)
	case 2:
		p0 := b.predPtr(checks[0])
		if p0.alwaysFalse || !p0.hasContains() || !p0.matches(idx) {
			return false
		}
		p1 := b.predPtr(checks[1])
		return !p1.alwaysFalse && p1.hasContains() && p1.matches(idx)
	case 3:
		p0 := b.predPtr(checks[0])
		if p0.alwaysFalse || !p0.hasContains() || !p0.matches(idx) {
			return false
		}
		p1 := b.predPtr(checks[1])
		if p1.alwaysFalse || !p1.hasContains() || !p1.matches(idx) {
			return false
		}
		p2 := b.predPtr(checks[2])
		return !p2.alwaysFalse && p2.hasContains() && p2.matches(idx)
	case 4:
		p0 := b.predPtr(checks[0])
		if p0.alwaysFalse || !p0.hasContains() || !p0.matches(idx) {
			return false
		}
		p1 := b.predPtr(checks[1])
		if p1.alwaysFalse || !p1.hasContains() || !p1.matches(idx) {
			return false
		}
		p2 := b.predPtr(checks[2])
		if p2.alwaysFalse || !p2.hasContains() || !p2.matches(idx) {
			return false
		}
		p3 := b.predPtr(checks[3])
		return !p3.alwaysFalse && p3.hasContains() && p3.matches(idx)
	}
	for _, i := range checks {
		p := b.predPtr(i)
		if p.alwaysFalse || !p.hasContains() || !p.matches(idx) {
			return false
		}
	}
	return true
}

func (b *plannerORBranch) matchesChecksObservedBuf(idx uint64, branch int, checks []int, observed *orderedORObservedStats) bool {
	if b.alwaysTrue {
		return true
	}
	offset := 0
	if observed != nil {
		offset = observed.offsets[branch]
	}
	switch len(checks) {

	case 0:
		return true

	case 1:
		if observed != nil && observed.candidatesBuf[offset] {
			observed.countsBuf[offset]++
		}
		p0 := b.predPtr(checks[0])
		return !p0.alwaysFalse && p0.hasContains() && p0.matches(idx)

	case 2:
		if observed != nil && observed.candidatesBuf[offset] {
			observed.countsBuf[offset]++
		}
		p0 := b.predPtr(checks[0])
		if p0.alwaysFalse || !p0.hasContains() || !p0.matches(idx) {
			return false
		}
		if observed != nil && observed.candidatesBuf[offset+1] {
			observed.countsBuf[offset+1]++
		}
		p1 := b.predPtr(checks[1])
		return !p1.alwaysFalse && p1.hasContains() && p1.matches(idx)

	case 3:
		if observed != nil && observed.candidatesBuf[offset] {
			observed.countsBuf[offset]++
		}
		p0 := b.predPtr(checks[0])
		if p0.alwaysFalse || !p0.hasContains() || !p0.matches(idx) {
			return false
		}
		if observed != nil && observed.candidatesBuf[offset+1] {
			observed.countsBuf[offset+1]++
		}
		p1 := b.predPtr(checks[1])
		if p1.alwaysFalse || !p1.hasContains() || !p1.matches(idx) {
			return false
		}
		if observed != nil && observed.candidatesBuf[offset+2] {
			observed.countsBuf[offset+2]++
		}
		p2 := b.predPtr(checks[2])
		return !p2.alwaysFalse && p2.hasContains() && p2.matches(idx)

	case 4:
		if observed != nil && observed.candidatesBuf[offset] {
			observed.countsBuf[offset]++
		}
		p0 := b.predPtr(checks[0])
		if p0.alwaysFalse || !p0.hasContains() || !p0.matches(idx) {
			return false
		}
		if observed != nil && observed.candidatesBuf[offset+1] {
			observed.countsBuf[offset+1]++
		}
		p1 := b.predPtr(checks[1])
		if p1.alwaysFalse || !p1.hasContains() || !p1.matches(idx) {
			return false
		}
		if observed != nil && observed.candidatesBuf[offset+2] {
			observed.countsBuf[offset+2]++
		}
		p2 := b.predPtr(checks[2])
		if p2.alwaysFalse || !p2.hasContains() || !p2.matches(idx) {
			return false
		}
		if observed != nil && observed.candidatesBuf[offset+3] {
			observed.countsBuf[offset+3]++
		}
		p3 := b.predPtr(checks[3])
		return !p3.alwaysFalse && p3.hasContains() && p3.matches(idx)
	}

	for ci, check := range checks {
		if observed != nil && observed.candidatesBuf[offset+ci] {
			slot := offset + ci
			observed.countsBuf[slot]++
		}
		p := b.predPtr(check)
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

func plannerHasPreferredExactBucketFilterReader(preds predicateReader, checks []int) bool {
	for _, pi := range checks {
		if preds.Get(pi).prefersExactBucketPostingFilter() {
			return true
		}
	}
	return false
}

func plannerHasPreferredExactBucketFilterBufReader(preds predicateReader, checks []int) bool {
	for _, pi := range checks {
		if preds.Get(pi).prefersExactBucketPostingFilter() {
			return true
		}
	}
	return false
}

func plannerFilterCompactPostingByPredicateChecks(
	preds predicateReader,
	checks []int,
	src posting.List,
	work posting.List,
	card uint64,
) (plannerPredicateBucketMode, posting.List, posting.List, bool) {
	if card == 0 || card > posting.MidCap {
		return 0, posting.List{}, work, false
	}
	if idx, ok := src.TrySingle(); ok {
		for _, pi := range checks {
			if !preds.GetPtr(pi).matches(idx) {
				return plannerPredicateBucketEmpty, posting.List{}, work, true
			}
		}
		return plannerPredicateBucketAll, src, work, true
	}
	var matched [posting.MidCap]uint64
	n := 0
	it := src.Iter()
	for it.HasNext() {
		idx := it.Next()
		keep := true
		for _, pi := range checks {
			if !preds.GetPtr(pi).matches(idx) {
				keep = false
				break
			}
		}
		if keep {
			matched[n] = idx
			n++
		}
	}
	it.Release()
	if n == 0 {
		_, nextWork, ok := work.TryResetOwnedCompactLikeFromSorted(src, nil)
		if !ok {
			return 0, posting.List{}, work, false
		}
		return plannerPredicateBucketEmpty, posting.List{}, nextWork, true
	}
	if uint64(n) == card {
		return plannerPredicateBucketAll, src, work, true
	}
	exact, nextWork, ok := work.TryResetOwnedCompactLikeFromSorted(src, matched[:n])
	if !ok {
		return 0, posting.List{}, work, false
	}
	return plannerPredicateBucketExact, exact, nextWork, true
}

func plannerFilterCompactPostingByPredicateChecksBuf(
	preds predicateReader,
	checks []int,
	src posting.List,
	work posting.List,
	card uint64,
) (plannerPredicateBucketMode, posting.List, posting.List, bool) {
	if card == 0 || card > posting.MidCap {
		return 0, posting.List{}, work, false
	}
	if idx, ok := src.TrySingle(); ok {
		for _, pi := range checks {
			if !preds.GetPtr(pi).matches(idx) {
				return plannerPredicateBucketEmpty, posting.List{}, work, true
			}
		}
		return plannerPredicateBucketAll, src, work, true
	}
	var matched [posting.MidCap]uint64
	n := 0
	it := src.Iter()
	for it.HasNext() {
		idx := it.Next()
		keep := true
		for _, pi := range checks {
			if !preds.GetPtr(pi).matches(idx) {
				keep = false
				break
			}
		}
		if keep {
			matched[n] = idx
			n++
		}
	}
	it.Release()
	if n == 0 {
		_, nextWork, ok := work.TryResetOwnedCompactLikeFromSorted(src, nil)
		if !ok {
			return 0, posting.List{}, work, false
		}
		return plannerPredicateBucketEmpty, posting.List{}, nextWork, true
	}
	if uint64(n) == card {
		return plannerPredicateBucketAll, src, work, true
	}
	exact, nextWork, ok := work.TryResetOwnedCompactLikeFromSorted(src, matched[:n])
	if !ok {
		return 0, posting.List{}, work, false
	}
	return plannerPredicateBucketExact, exact, nextWork, true
}

func plannerFilterPostingByChecks(
	preds []predicate,
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

	smallCard := card <= plannerPredicateBucketExactMinCardForChecks(len(checks))
	preferExact := smallCard && plannerHasPreferredExactBucketFilterReader(predicateSliceView(preds), checks)
	if (!allowExact || smallCard) && !preferExact {
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

	if mode, exact, nextWork, ok := plannerFilterCompactPostingByPredicateChecks(predicateSliceView(preds), checks, src, work, card); ok {
		return mode, exact, nextWork, card
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

func plannerFilterPostingByPredicateChecks(
	preds predicateReader,
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

	smallCard := card <= plannerPredicateBucketExactMinCardForChecks(len(checks))
	preferExact := smallCard && plannerHasPreferredExactBucketFilterReader(preds, checks)
	if (!allowExact || smallCard) && !preferExact {
		skipBucket := false
		fullBucket := true
		for _, pi := range checks {
			cnt, ok := preds.Get(pi).countBucket(src)
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

	if mode, exact, nextWork, ok := plannerFilterCompactPostingByPredicateChecks(preds, checks, src, work, card); ok {
		return mode, exact, nextWork, card
	}

	work = src.CloneInto(work)
	for _, pi := range checks {
		var ok bool
		work, ok = preds.Get(pi).applyToPosting(work)
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

func plannerFilterPostingByPredicateSetChecks(
	preds predicateSet,
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

	smallCard := card <= plannerPredicateBucketExactMinCardForChecks(len(checks))
	preferExact := smallCard && plannerHasPreferredExactBucketFilterReader(preds, checks)

	if (!allowExact || smallCard) && !preferExact {
		skipBucket := false
		fullBucket := true
		for _, pi := range checks {
			cnt, ok := preds.Get(pi).countBucket(src)
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

	if mode, exact, nextWork, ok := plannerFilterCompactPostingByPredicateChecks(preds, checks, src, work, card); ok {
		return mode, exact, nextWork, card
	}

	work = src.CloneInto(work)
	for _, pi := range checks {
		var ok bool
		work, ok = preds.Get(pi).applyToPosting(work)
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

func plannerFilterPostingByPredicateChecksBuf(
	preds predicateReader,
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

	smallCard := card <= plannerPredicateBucketExactMinCardForChecks(len(checks))
	preferExact := smallCard && plannerHasPreferredExactBucketFilterBufReader(preds, checks)

	if (!allowExact || smallCard) && !preferExact {
		skipBucket := false
		fullBucket := true
		for _, pi := range checks {
			cnt, ok := preds.Get(pi).countBucket(src)
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

	if mode, exact, nextWork, ok := plannerFilterCompactPostingByPredicateChecksBuf(preds, checks, src, work, card); ok {
		return mode, exact, nextWork, card
	}

	work = src.CloneInto(work)
	for _, pi := range checks {
		var ok bool
		work, ok = preds.Get(pi).applyToPosting(work)
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
	lead := b.leadPtr()
	leadNeedsCheck := lead != nil && lead.leadIterNeedsContainsCheck()
	for i := 0; i < b.predLen(); i++ {
		if i == b.leadIdx && !leadNeedsCheck {
			continue
		}
		p := b.pred(i)
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
	for i := 0; i < b.predLen(); i++ {
		p := b.pred(i)
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
	} else if b.hasLead() && b.leadPred().estCard > 0 {
		est = b.leadPred().estCard
	} else {
		for i := 0; i < b.predLen(); i++ {
			if c := b.pred(i).estCard; c > est {
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
	if b.hasLead() && b.leadPred().estCard > 0 {
		leadEst = b.leadPred().estCard
	}
	extraChecks := 1.0
	for i := 0; i < b.predLen(); i++ {
		if i == b.leadIdx {
			continue
		}
		p := b.pred(i)
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
	if b.hasLead() {
		leadEst = b.leadPred().estCard
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

func (branches plannerORBranches) avgContainsChecksByBranch(checks [plannerORBranchLimit]int) float64 {
	totalChecks := 0.0
	activeBranches := 0.0
	n := branches.Len()
	if n > plannerORBranchLimit {
		n = plannerORBranchLimit
	}
	for i := 0; i < n; i++ {
		if branches.Get(i).alwaysTrue {
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

func (qv *queryView) orderMergeBranchStats(orderField string, branches plannerORBranches, ov indexdata.FieldOverlay) [plannerORBranchLimit]plannerOROrderMergeBranchStats {
	var stats [plannerORBranchLimit]plannerOROrderMergeBranchStats
	n := branches.Len()
	if n > plannerORBranchLimit {
		n = plannerORBranchLimit
	}
	for i := 0; i < n; i++ {
		branch := branches.GetPtr(i)
		stats[i].streamChecks = branch.containsChecks()
		stats[i].mergeChecks = stats[i].streamChecks
		if branch.coveredRangeBounded {
			stats[i].streamChecks = 0
			stats[i].mergeChecks = 0
			stats[i].rangeBounded = true
			if branch.estKnown {
				stats[i].rangeRows = branch.estCard
			} else {
				br := indexdata.OverlayRange{
					BaseStart: branch.coveredRangeStart,
					BaseEnd:   branch.coveredRangeEnd,
				}
				_, stats[i].rangeRows = ov.RangeStats(br)
				branch.estCard = stats[i].rangeRows
				branch.estKnown = true
			}
			continue
		}
		if orderField == "" || !ov.HasData() {
			continue
		}
		br, covered, ok := qv.extractOrderRangeCoverageOverlayReader(orderField, branch.preds, ov)
		if !ok || len(covered) == 0 {
			if ok && (br.BaseStart != 0 || br.BaseEnd != ov.KeyCount()) {
				stats[i].rangeBounded = true
				_, stats[i].rangeRows = ov.RangeStats(br)
			}
			stats[i].streamChecks, stats[i].mergeChecks = plannerORBranchCheckCounts(*branch, nil)
			if ok {
				pooled.ReleaseBoolSlice(covered)
			}
			continue
		}
		stats[i].streamChecks, stats[i].mergeChecks = plannerORBranchCheckCounts(*branch, covered)
		pooled.ReleaseBoolSlice(covered)
		if br.BaseStart == 0 && br.BaseEnd == ov.KeyCount() {
			continue
		}
		stats[i].rangeBounded = true
		_, stats[i].rangeRows = ov.RangeStats(br)
	}
	return stats
}

func (branches plannerORBranches) hasFullSpanOrderBranch(stats [plannerORBranchLimit]plannerOROrderMergeBranchStats) bool {
	n := branches.Len()
	if n > plannerORBranchLimit {
		n = plannerORBranchLimit
	}
	for i := 0; i < n; i++ {
		if branches.Get(i).alwaysTrue {
			continue
		}
		if !stats[i].rangeBounded {
			return true
		}
	}
	return false
}

func (branches plannerORBranches) hasKWayExactBucketApplyWork(stats [plannerORBranchLimit]plannerOROrderMergeBranchStats) bool {
	n := branches.Len()
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

func (branches plannerORBranches) hasSelectiveLead(need int) bool {
	if need <= 0 {
		return false
	}
	threshold := uint64(need * plannerOROrderFallbackFirstLeadNeedMul)
	if threshold == 0 {
		threshold = 1
	}
	for i := 0; i < branches.Len(); i++ {
		branch := branches.Get(i)
		if !branch.hasLead() || branch.leadPred().estCard == 0 {
			continue
		}
		if branch.leadPred().estCard <= threshold {
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

func plannerOROrderFallbackFirstGainForShape(routeCost plannerOROrderRouteCost, q *qir.Shape, need int) float64 {
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

func plannerOROrderFallbackFirstOffsetGainForShape(routeCost plannerOROrderRouteCost, q *qir.Shape, need int) float64 {
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

func plannerORKWayRuntimeShapeFromGuard(guard plannerOROrderRuntimeGuardDecision, q *qir.Shape) plannerORKWayRuntimeShape {
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
func (qv *queryView) estimateOROrderMergeRouteCost(
	q *qir.Shape,
	branches plannerORBranches,
	need int,
	mergeStats [plannerORBranchLimit]plannerOROrderMergeBranchStats,
) (plannerOROrderRouteCost, bool) {
	if need <= 0 || q == nil || !q.HasOrder {
		return plannerOROrderRouteCost{}, false
	}

	order := q.Order
	orderField := qv.engine.fieldNameByOrdinal(order.FieldOrdinal)
	if order.FieldOrdinal < 0 {
		return plannerOROrderRouteCost{}, false
	}

	snap := qv.planner.stats.Load()
	universe := snap.universeOr(qv.snapshotUniverseCardinality())
	if universe == 0 {
		return plannerOROrderRouteCost{}, false
	}
	orderOV := qv.fieldOverlayForOrder(order)
	orderDistinct := uint64(orderOV.KeyCount())
	orderStats := qv.plannerOrderFieldStats(orderField, snap, universe, orderDistinct)

	var branchCards [plannerORBranchLimit]uint64
	unionCard, sumCard, _, _ := branches.unionCards(universe, &branchCards)
	if unionCard == 0 {
		return plannerOROrderRouteCost{}, false
	}
	headSensitiveOrderShape := orderDistinct >= 64

	expectedRows := estimateRowsForNeed(uint64(need), unionCard, universe)
	if expectedRows == 0 {
		return plannerOROrderRouteCost{}, false
	}

	var mergeChecks [plannerORBranchLimit]int
	for i := 0; i < branches.Len(); i++ {
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

	hasPrefixTailRisk := qv.hasNonOrderPrefixTailRisk(branches, orderField)
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
	directCollectFast := orderOV.HasData()
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
		activeBranches = branches.Len()
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

func (branches plannerORBranches) evalOrder() ([plannerORBranchLimit]int, int) {
	var order [plannerORBranchLimit]int
	var scores [plannerORBranchLimit]float64
	var checks [plannerORBranchLimit]int

	n := branches.Len()
	if n > plannerORBranchLimit {
		n = plannerORBranchLimit
	}
	for i := 0; i < n; i++ {
		order[i] = i
		branch := branches.Get(i)
		scores[i] = branch.evalScore()
		checks[i] = branch.containsChecks()
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

func releasePlannerORNoOrderStateIters(states []plannerORNoOrderBranchState) {
	for i := range states {
		states[i].iter.Release()
	}
}

func releasePlannerORIters(iters []plannerORIter) {
	for i := range iters {
		iters[i].Release()
	}
}

func (qv *queryView) tryPlan(q *qir.Shape, trace *queryTrace) ([]uint64, bool, error) {
	orderedPtr := false
	if q.HasOrder && q.Order.Kind == qir.OrderKindBasic {
		if fm := qv.fieldMetaByOrder(q.Order); fm != nil && fm.Ptr {
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

func (it *plannerORIter) advance() (uint64, uint64) {
	examinedDelta, emittedDelta, _ := it.advanceWithBudget(0)
	return examinedDelta, emittedDelta
}

func (it *plannerORIter) advanceWithBudget(budget uint64) (uint64, uint64, bool) {
	it.has = false
	var scanned uint64
	for it.it.HasNext() {
		if budget > 0 && scanned >= budget {
			return scanned, 0, true
		}
		idx := it.it.Next()
		scanned++
		if it.branch.matchesFromLead(idx) {
			it.cur = idx
			it.has = true
			return scanned, 1, false
		}
	}
	return scanned, 0, false
}

// tryPlanORMergeMode plans top-level OR queries with bounded branch count.
//
// This path exists to avoid full bitmap materialization when branch-aware
// streaming/merge strategies can satisfy LIMIT/OFFSET cheaper.
func (qv *queryView) tryPlanORMergeMode(q *qir.Shape, trace *queryTrace) ([]uint64, bool, error) {
	if q.Limit == 0 {
		return nil, false, nil
	}
	if q.Expr.Op != qir.OpOR || q.Expr.Not {
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
	if !q.HasOrder && !hasNoOrderLeadCandidatesOR(q.Expr.Operands) {
		return nil, false, nil
	}

	if !q.HasOrder {
		branches, alwaysFalse, ok := qv.buildORBranches(q.Expr.Operands)
		if !ok {
			return nil, false, nil
		}
		if alwaysFalse {
			return nil, true, nil
		}
		defer branches.Release()

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

	o := q.Order
	if o.Kind != qir.OrderKindBasic {
		return nil, false, nil
	}
	if q.Limit > plannerOROrderLimitMax || q.Offset > plannerOROrderOffsetMax {
		return nil, false, nil
	}
	window, _ := orderWindow(q)
	branches, alwaysFalse, ok := qv.buildORBranchesOrdered(q.Expr.Operands, qv.engine.fieldNameByOrdinal(o.FieldOrdinal), window, q.Offset)
	if !ok {
		return nil, false, nil
	}
	if alwaysFalse {
		return nil, true, nil
	}
	defer branches.Release()

	// Ordered OR has two distinct strategies: k-way merge of branch streams and
	// stream+match fallback. Cost model chooses initial route.
	analysis, ok := qv.buildOROrderAnalysis(q, branches)
	if !ok {
		return nil, false, nil
	}
	defer analysis.release()
	// Keep the extra ordered-OR materialization analysis on offset-shaped
	// queries where it can avoid deep tail work; offset-free shapes are more
	// sensitive to this fixed planner cost.
	if q.Offset > 0 {
		qv.maybeWarmMaterializeOrderedORPredicates(q, branches, &analysis, trace)
	}
	orderDecision := qv.decidePlanOROrderWithAnalysis(q, branches, &analysis)
	if trace != nil {
		trace.setEstimated(orderDecision.expectedRows, orderDecision.bestCost, orderDecision.fallbackCost)
	}

	switch orderDecision.plan {

	case plannerOROrderMerge:
		out, ok, err := qv.execPlanOROrderMerge(q, branches, &analysis, trace)
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
		out2, ok2 := qv.execPlanOROrderBasic(q, branches, &analysis, trace, observe)
		if ok2 {
			if trace != nil {
				trace.setPlan(PlanORMergeOrderStream)
			}
			return out2, true, nil
		}
		return nil, false, nil

	case plannerOROrderStream:
		if q.Offset > 0 {
			qv.maybeEagerMaterializeOrderedORPredicates(q, branches, &analysis, false, trace)
		}
		var observed orderedORObservedStats
		var observe *orderedORObservedStats
		if trace == nil {
			observe = &observed
			defer observed.release()
		}
		out, ok := qv.execPlanOROrderBasic(q, branches, &analysis, trace, observe)
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
	countsBuf     []uint64
	candidatesBuf []bool
	offsets       [plannerORBranchLimit + 1]int
	active        bool
}

func initOrderedORObservedStats(
	qv *queryView,
	field string,
	observed *orderedORObservedStats,
	branches plannerORBranches,
	branchChecks [plannerORBranchLimit][]int,
	analysis *plannerOROrderAnalysis,
) {
	if observed == nil {
		return
	}
	total := 0
	observed.offsets[0] = 0
	branchCount := branches.Len()
	if branchCount > plannerORBranchLimit {
		branchCount = plannerORBranchLimit
	}
	for i := 0; i < branchCount; i++ {
		total += len(branchChecks[i])
		observed.offsets[i+1] = total
	}
	if total == 0 {
		observed.countsBuf = nil
		observed.candidatesBuf = nil
		observed.active = false
		return
	}
	active := 0
	for bi := 0; bi < branchCount; bi++ {
		branch := branches.Get(bi)
		checks := branchChecks[bi]
		for _, pi := range checks {
			p := branch.pred(pi)
			info := qv.orderedORPredicateBuildInfoForBranch(field, p, analysis, branch, bi, pi)
			if qv.shouldObserveOrderedORPredicate(p, info) {
				active++
			}
		}
	}
	if active == 0 {
		observed.countsBuf = nil
		observed.candidatesBuf = nil
		observed.active = false
		return
	}
	observed.countsBuf = pooled.GetUint64Slice(total)[:total]
	clear(observed.countsBuf)

	observed.candidatesBuf = pooled.GetBoolSlice(total)[:total]
	clear(observed.candidatesBuf)

	observed.active = true
	for bi := 0; bi < branchCount; bi++ {
		branch := branches.Get(bi)
		start := observed.offsets[bi]
		checks := branchChecks[bi]
		for ci, pi := range checks {
			p := branch.pred(pi)
			info := qv.orderedORPredicateBuildInfoForBranch(field, p, analysis, branch, bi, pi)
			if qv.shouldObserveOrderedORPredicate(p, info) {
				observed.candidatesBuf[start+ci] = true
			}
		}
	}
}

func (s *orderedORObservedStats) branchRange(branch int) (int, int, bool) {
	if s == nil || branch < 0 || branch >= plannerORBranchLimit {
		return 0, 0, false
	}
	start := s.offsets[branch]
	end := s.offsets[branch+1]
	if start >= end || s.countsBuf == nil || end > len(s.countsBuf) {
		return 0, 0, false
	}
	return start, end, true
}

func (s *orderedORObservedStats) release() {
	if s == nil {
		return
	}
	if s.countsBuf != nil {
		pooled.ReleaseUint64Slice(s.countsBuf)
	}
	if s.candidatesBuf != nil {
		pooled.ReleaseBoolSlice(s.candidatesBuf)
	}
	*s = orderedORObservedStats{}
}

func releasePlannerOROrderBasicCheckBufs(
	branchCount int,
	checkBufs *[plannerORBranchLimit][]int,
) {
	if branchCount > plannerORBranchLimit {
		branchCount = plannerORBranchLimit
	}
	for i := 0; i < branchCount; i++ {
		if checkBufs[i] == nil {
			continue
		}
		pooled.ReleaseIntSlice(checkBufs[i])
		checkBufs[i] = nil
	}
}

func plannerOROrderBasicMatches(
	branches plannerORBranches,
	branchEvalOrder *[plannerORBranchLimit]int,
	branchEvalN int,
	branchStart *[plannerORBranchLimit]int,
	branchEnd *[plannerORBranchLimit]int,
	branchChecks *[plannerORBranchLimit][]int,
	idx uint64,
	bucket int,
) bool {
	for i := 0; i < branchEvalN; i++ {
		bi := branchEvalOrder[i]
		if bucket < branchStart[bi] || bucket >= branchEnd[bi] {
			continue
		}
		if branches.GetPtr(bi).matchesChecksBuf(idx, branchChecks[bi]) {
			return true
		}
	}
	return false
}

func plannerOROrderBasicMatchesObserved(
	branches plannerORBranches,
	branchEvalOrder *[plannerORBranchLimit]int,
	branchEvalN int,
	branchStart *[plannerORBranchLimit]int,
	branchEnd *[plannerORBranchLimit]int,
	branchChecks *[plannerORBranchLimit][]int,
	idx uint64,
	bucket int,
	observed *orderedORObservedStats,
) bool {
	for i := 0; i < branchEvalN; i++ {
		bi := branchEvalOrder[i]
		if bucket < branchStart[bi] || bucket >= branchEnd[bi] {
			continue
		}
		if branches.GetPtr(bi).matchesChecksObservedBuf(idx, bi, branchChecks[bi], observed) {
			return true
		}
	}
	return false
}

func plannerOROrderBasicMatchWithMetrics(
	branches plannerORBranches,
	branchStart *[plannerORBranchLimit]int,
	branchEnd *[plannerORBranchLimit]int,
	branchChecks *[plannerORBranchLimit][]int,
	branchMetrics []TraceORBranch,
	idx uint64,
	bucket int,
	alwaysTrue bool,
	alwaysTrueBranch int,
) int {
	if alwaysTrue {
		if alwaysTrueBranch >= 0 {
			branchMetrics[alwaysTrueBranch].RowsExamined++
			branchMetrics[alwaysTrueBranch].RowsEmitted++
		}
		return 1
	}

	matchedCount := 0
	for i := 0; i < branches.Len(); i++ {
		if bucket < branchStart[i] || bucket >= branchEnd[i] {
			continue
		}
		branchMetrics[i].RowsExamined++
		if branches.GetPtr(i).matchesChecksBuf(idx, branchChecks[i]) {
			branchMetrics[i].RowsEmitted++
			matchedCount++
		}
	}
	return matchedCount
}

func setOROrderMergeFallbackTrace(trace *queryTrace, examined, scanWidth uint64, branchMetrics []TraceORBranch, stopReason string) {
	if trace == nil {
		return
	}
	trace.addExamined(examined)
	trace.addOrderScanWidth(scanWidth)
	trace.setORBranches(branchMetrics)
	trace.setEarlyStopReason(stopReason)
}

func (qv *queryView) orderedORMaterializedRangeLeafCosts(
	orderField string,
	leaf qir.Expr,
) (qcache.MaterializedPredKey, uint64, uint64, uint64, bool) {
	candidate, ok := qv.prepareScalarRangeRoutingCandidate(leaf)
	if !ok || !candidate.numeric || qv.engine.fieldNameByOrdinal(leaf.FieldOrdinal) == orderField {
		return qcache.MaterializedPredKey{}, 0, 0, 0, false
	}
	core := candidate.core
	plan := candidate.plan
	if core.bound.full || plan.bucketCount == 0 || plan.est == 0 {
		return qcache.MaterializedPredKey{}, 0, 0, 0, false
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
		return qcache.MaterializedPredKey{}, 0, 0, 0, false
	}
	if candidate.shouldPreferPositiveMaterializationForNullableComplement(compPlan) {
		return core.sharedReuse.cacheKey,
			rangeProbeMaterializeWork(plan.bucketCount, plan.est),
			rangeProbeContainsWork(plan.bucketCount, plan.est),
			postingContainsLookupWork(plan.est),
			true
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
		return qcache.MaterializedPredKey{}, 0, 0, 0, false
	}
	return compPlan.sharedReuse.cacheKey,
		rangeProbeMaterializeWork(compPlan.buckets, compPlan.est),
		rangeProbeContainsWork(checkBuckets, checkEst),
		postingContainsLookupWork(compPlan.est),
		true
}

func (qv *queryView) orderedORMaterializedExactRangePredicateCosts(
	orderField string,
	p predicate,
) (qcache.MaterializedPredKey, uint64, uint64, uint64, bool) {
	if !p.hasEffectiveBounds || p.expr.FieldOrdinal < 0 {
		return qcache.MaterializedPredKey{}, 0, 0, 0, false
	}
	fieldName := qv.engine.fieldNameByOrdinal(p.expr.FieldOrdinal)
	if fieldName == orderField {
		return qcache.MaterializedPredKey{}, 0, 0, 0, false
	}
	fm := qv.fieldMetaByExpr(p.expr)
	if !schema.FieldUsesOrderedNumericKeys(fm) {
		return qcache.MaterializedPredKey{}, 0, 0, 0, false
	}

	var core preparedScalarRangePredicate
	qv.initPreparedExactScalarRangePredicate(&core, p.expr, fm, p.effectiveBounds)
	ov := qv.fieldOverlayForExpr(p.expr)
	if !ov.HasData() {
		return qcache.MaterializedPredKey{}, 0, 0, 0, false
	}
	plan, _, done := core.planOverlay(ov)
	if done || plan.bucketCount == 0 || plan.est == 0 {
		return qcache.MaterializedPredKey{}, 0, 0, 0, false
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
		return qcache.MaterializedPredKey{}, 0, 0, 0, false
	}
	candidate := preparedScalarRangeRoutingCandidate{
		core:    core,
		plan:    plan,
		numeric: true,
	}
	if candidate.shouldPreferPositiveMaterializationForNullableComplement(compPlan) {
		return core.sharedReuse.cacheKey,
			rangeProbeMaterializeWork(plan.bucketCount, plan.est),
			rangeProbeContainsWork(plan.bucketCount, plan.est),
			postingContainsLookupWork(plan.est),
			true
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
		return qcache.MaterializedPredKey{}, 0, 0, 0, false
	}
	return compPlan.sharedReuse.cacheKey,
		rangeProbeMaterializeWork(compPlan.buckets, compPlan.est),
		rangeProbeContainsWork(checkBuckets, checkEst),
		postingContainsLookupWork(compPlan.est),
		true
}

func (qv *queryView) orderedORMaterializedPrefixLeafBuildWork(
	orderField string,
	leaf qir.Expr,
) (qcache.MaterializedPredKey, uint64, bool) {
	if !qv.isPositiveNonOrderScalarPrefixLeaf(orderField, leaf) {
		return qcache.MaterializedPredKey{}, 0, false
	}
	candidate, ok := qv.prepareScalarRangeRoutingCandidate(leaf)
	if !ok || candidate.plan.useComplement || candidate.core.sharedReuse.cacheKey.IsZero() {
		return qcache.MaterializedPredKey{}, 0, false
	}
	core := candidate.core
	snap := qv.planner.stats.Load()
	universe := max(qv.snapshotUniverseCardinality(), uint64(1))
	sel, _, _, _, ok := qv.estimateLeafOrderCost(leaf, snap, universe, orderField, qv.fieldOverlay(orderField).HasData())
	if !ok || sel <= 0 {
		return qcache.MaterializedPredKey{}, 0, false
	}
	estCard := uint64(sel*float64(universe) + 0.5)
	if estCard == 0 {
		estCard = 1
	}
	if !qv.snap.AllowsMaterializedPredCard(estCard) {
		return qcache.MaterializedPredKey{}, 0, false
	}
	return core.sharedReuse.cacheKey, estCard, true
}

type orderedORMaterializedPredicateBuildInfo struct {
	cacheKey        qcache.MaterializedPredKey
	buildWork       uint64
	checkWork       uint64
	cachedCheckWork uint64
	isPrefix        bool
	ok              bool
}

func (qv *queryView) orderedORMaterializedPredicateBuildInfo(
	orderField string,
	p predicate,
) orderedORMaterializedPredicateBuildInfo {
	cacheKey, buildWork, checkWork, cachedCheckWork, ok := qv.orderedORMaterializedExactRangePredicateCosts(orderField, p)
	if ok && buildWork != 0 && checkWork != 0 {
		return orderedORMaterializedPredicateBuildInfo{
			cacheKey:        cacheKey,
			buildWork:       buildWork,
			checkWork:       checkWork,
			cachedCheckWork: cachedCheckWork,
			ok:              true,
		}
	}
	cacheKey, buildWork, checkWork, cachedCheckWork, ok = qv.orderedORMaterializedRangeLeafCosts(orderField, p.expr)
	if ok && buildWork != 0 && checkWork != 0 {
		return orderedORMaterializedPredicateBuildInfo{
			cacheKey:        cacheKey,
			buildWork:       buildWork,
			checkWork:       checkWork,
			cachedCheckWork: cachedCheckWork,
			ok:              true,
		}
	}
	cacheKey, buildWork, ok = qv.orderedORMaterializedPrefixLeafBuildWork(orderField, p.expr)
	if ok && buildWork != 0 {
		return orderedORMaterializedPredicateBuildInfo{
			cacheKey:  cacheKey,
			buildWork: buildWork,
			isPrefix:  true,
			ok:        true,
		}
	}
	return orderedORMaterializedPredicateBuildInfo{}
}

func (qv *queryView) buildOROrderPredicateBuildInfos(
	orderField string,
	branch plannerORBranch,
	covered []bool,
) *pooled.Slice[orderedORMaterializedPredicateBuildInfo] {
	if branch.predLen() == 0 {
		return nil
	}
	buf := plannerORPredicateBuildInfoSlicePool.Get()
	buf.SetLen(branch.predLen())
	anyInfo := false
	for pi := 0; pi < branch.predLen(); pi++ {
		p := branch.pred(pi)
		if p.alwaysTrue || p.alwaysFalse || !p.hasContains() {
			continue
		}
		if len(covered) != 0 && covered[pi] {
			continue
		}
		info := qv.orderedORMaterializedPredicateBuildInfo(orderField, p)
		if !info.ok {
			continue
		}
		buf.Set(pi, info)
		anyInfo = true
	}
	if !anyInfo {
		plannerORPredicateBuildInfoSlicePool.Put(buf)
		return nil
	}
	return buf
}

func (qv *queryView) orderedORPredicateBuildInfoForBranch(
	orderField string,
	p predicate,
	analysis *plannerOROrderAnalysis,
	branch plannerORBranch,
	branchIdx int,
	predIdx int,
) orderedORMaterializedPredicateBuildInfo {
	if analysis == nil || branchIdx < 0 || branchIdx >= analysis.branchCount {
		return qv.orderedORMaterializedPredicateBuildInfo(orderField, p)
	}
	if !analysis.branches[branchIdx].buildReady {
		analysis.branches[branchIdx].predBuild = qv.buildOROrderPredicateBuildInfos(orderField, branch, analysis.branches[branchIdx].covered)
		analysis.branches[branchIdx].buildReady = true
	}
	if info, ok := analysis.predicateBuildInfo(branchIdx, predIdx); ok {
		return info
	}
	if analysis.branches[branchIdx].buildReady {
		return orderedORMaterializedPredicateBuildInfo{}
	}
	return qv.orderedORMaterializedPredicateBuildInfo(orderField, p)
}

func (qv *queryView) materializeOrderedORPredicate(p *predicate) bool {
	if p == nil {
		return false
	}
	candidate, ok := qv.preparePredicateScalarRangeRoutingCandidate(*p)
	if !ok {
		return false
	}
	if candidate.plan.useComplement {
		plan, cached, cacheHit, empty, ok := candidate.core.loadComplementMaterialization()
		if !ok {
			return false
		}
		if !cacheHit && !empty && candidate.shouldPreferPositiveMaterializationForNullableComplement(plan) {
			return qv.materializePositiveScalarRangePredicateWithCandidate(p, candidate)
		}
		if cacheHit {
			setPredicateMaterializedNot(p, cached)
			return true
		}
		if empty {
			storeEmptyScalarComplementMaterialization(plan)
			setPredicateAlwaysTrue(p)
			return true
		}
		ids := candidate.core.materializeComplement(plan)
		if ids.IsEmpty() {
			storeEmptyScalarComplementMaterialization(plan)
			setPredicateAlwaysTrue(p)
			return true
		}
		ids = plan.sharedReuse.share(ids)
		setPredicateMaterializedNot(p, ids)
		return true
	}

	return qv.materializePositiveScalarRangePredicateWithCandidate(p, candidate)
}

func (qv *queryView) materializePositiveScalarRangePredicate(p *predicate) bool {
	if p == nil {
		return false
	}
	candidate, ok := qv.preparePredicateScalarRangeRoutingCandidate(*p)
	if !ok {
		return false
	}
	return qv.materializePositiveScalarRangePredicateWithCandidate(p, candidate)
}

func (qv *queryView) materializePositiveScalarRangePredicateWithCandidate(p *predicate, candidate preparedScalarRangeRoutingCandidate) bool {
	ov := qv.fieldOverlayForExpr(p.expr)
	if !ov.HasData() {
		hasEffectiveBounds := p.hasEffectiveBounds
		effectiveBounds := p.effectiveBounds
		releasePredicateOwnedState(p)
		*p = materializedRangePredicateWithMode(p.expr, posting.List{})
		p.hasEffectiveBounds = hasEffectiveBounds
		p.effectiveBounds = effectiveBounds
		return true
	}
	ids := candidate.core.evalMaterializedPostingResult(ov).ids
	hasEffectiveBounds := p.hasEffectiveBounds
	effectiveBounds := p.effectiveBounds
	releasePredicateOwnedState(p)
	*p = materializedRangePredicateWithMode(p.expr, ids)
	p.hasEffectiveBounds = hasEffectiveBounds
	p.effectiveBounds = effectiveBounds
	return true
}

type plannerOROrderedBranchEstimate struct {
	need      uint64
	card      uint64
	universe  uint64
	probeRows uint64
}

func (qv *queryView) initOrderedORBranchEstimates(branches plannerORBranches, branchUniverses *[plannerORBranchLimit]uint64, needWindow int, out *[plannerORBranchLimit]plannerOROrderedBranchEstimate) {
	if out == nil {
		return
	}
	*out = [plannerORBranchLimit]plannerOROrderedBranchEstimate{}
	if needWindow <= 0 || branchUniverses == nil {
		return
	}
	snapshotUniverse := qv.snapshotUniverseCardinality()
	if snapshotUniverse == 0 {
		return
	}

	branchCount := branches.Len()
	if branchCount > plannerORBranchLimit {
		branchCount = plannerORBranchLimit
	}

	totalCard := uint64(0)
	for i := 0; i < branchCount; i++ {
		universe := branchUniverses[i]
		if universe == 0 {
			continue
		}
		if universe > snapshotUniverse {
			universe = snapshotUniverse
		}
		card := branches.Get(i).estimatedCardWithinOrderedUniverse(snapshotUniverse, universe)
		out[i] = plannerOROrderedBranchEstimate{
			card:     card,
			universe: universe,
		}
		totalCard = satAddUint64(totalCard, card)
	}
	if totalCard == 0 {
		return
	}

	need := uint64(needWindow)
	for i := 0; i < branchCount; i++ {
		est := out[i]
		if est.card == 0 || est.universe == 0 {
			continue
		}
		branchNeed := uint64(math.Ceil(float64(need) * float64(est.card) / float64(totalCard)))
		if branchNeed == 0 {
			branchNeed = 1
		}
		if branchNeed > need {
			branchNeed = need
		}
		est.need = branchNeed
		est.probeRows = estimateRowsForNeed(branchNeed, est.card, est.universe)
		out[i] = est
	}
}

func (qv *queryView) initNoOrderORBranchEstimates(branches plannerORBranches, needWindow int, out *[plannerORBranchLimit]plannerOROrderedBranchEstimate) bool {
	if out == nil || needWindow <= 0 {
		return false
	}
	*out = [plannerORBranchLimit]plannerOROrderedBranchEstimate{}

	universe := qv.snapshotUniverseCardinality()
	if universe == 0 {
		return false
	}

	var branchCards [plannerORBranchLimit]uint64
	branchCount := branches.Len()
	if branchCount > plannerORBranchLimit {
		branchCount = plannerORBranchLimit
	}
	totalCard := uint64(0)
	for i := 0; i < branchCount; i++ {
		card := branches.Get(i).estimatedCard(universe)
		branchCards[i] = card
		totalCard = satAddUint64(totalCard, card)
	}
	if totalCard == 0 {
		return false
	}

	need := uint64(needWindow)
	for i := 0; i < branchCount; i++ {
		card := branchCards[i]
		if card == 0 {
			continue
		}
		branchNeed := uint64(math.Ceil(float64(need) * float64(card) / float64(totalCard)))
		if branchNeed == 0 {
			branchNeed = 1
		}
		if branchNeed > need {
			branchNeed = need
		}
		leadEst := card
		if branch := branches.Get(i); branch.hasLead() && branch.leadPred().estCard > 0 {
			leadEst = branch.leadPred().estCard
		}
		if leadEst == 0 {
			leadEst = 1
		}
		probeRows := estimateRowsForNeed(branchNeed, card, leadEst)
		budget := branches.Get(i).noOrderBudget(needWindow)
		if probeRows < budget {
			probeRows = budget
		}
		if probeRows > leadEst {
			probeRows = leadEst
		}
		out[i] = plannerOROrderedBranchEstimate{
			need:      branchNeed,
			card:      card,
			universe:  universe,
			probeRows: probeRows,
		}
	}
	return true
}

func orderedORNextPredicateRows(rows, predCard, branchCard, branchUniverse uint64) uint64 {
	if rows == 0 || branchUniverse == 0 {
		return 0
	}
	if predCard == 0 {
		return rows
	}
	if branchCard > 0 && predCard < branchCard {
		predCard = branchCard
	}
	if predCard >= branchUniverse {
		return rows
	}
	if rows > ^uint64(0)/predCard {
		return rows
	}
	product := rows * predCard
	next := product / branchUniverse
	if product%branchUniverse != 0 {
		next++
	}
	if next == 0 {
		next = 1
	}
	if next > rows {
		next = rows
	}
	return next
}

func orderedORAdvanceRowsForChecks(branch *plannerORBranch, checks []int, est plannerOROrderedBranchEstimate, rows uint64) uint64 {
	if branch == nil || len(checks) == 0 || rows == 0 {
		return rows
	}
	for _, check := range checks {
		if rows == 0 {
			break
		}
		rows = orderedORNextPredicateRows(rows, branch.pred(check).estCard, est.card, est.universe)
	}
	return rows
}

func (qv *queryView) orderedORMaterializedPredicateRequiresPromotion(p predicate, branchNeed, orderedOffset, branchUniverse uint64) bool {
	if orderedOffset != 0 || branchNeed == 0 || branchUniverse == 0 {
		return false
	}
	candidate, ok := qv.preparePredicateScalarRangeRoutingCandidate(p)
	if !ok || !candidate.numeric || candidate.plan.est == 0 {
		return false
	}
	expectedRows := orderedPredicateExpectedRows(clampUint64ToInt(branchNeed), candidate.plan.est, branchUniverse)
	return expectedRows > 0 &&
		expectedRows < candidate.plan.est &&
		satMulUint64(expectedRows, 2) < candidate.plan.est
}

func orderedORMaterializedPredicateColdBuildWorth(buildWork, cachedCheckWork, leafChecks, savedWork uint64) bool {
	if savedWork <= buildWork {
		return false
	}
	margin := satMulUint64(leafChecks, cachedCheckWork)
	if margin < buildWork {
		margin = buildWork
	}
	return savedWork >= satAddUint64(buildWork, margin)
}

func orderedORMaterializedPredicateWarmHitWorth(leafChecks, checkWork, cachedCheckWork uint64) bool {
	if leafChecks == 0 || checkWork <= cachedCheckWork {
		return false
	}
	savedWork := satMulUint64(leafChecks, checkWork-cachedCheckWork)
	return savedWork >= cachedCheckWork
}

func (qv *queryView) shouldEagerMaterializeOrderedORPredicate(
	p predicate,
	info orderedORMaterializedPredicateBuildInfo,
	est plannerOROrderedBranchEstimate,
	orderedOffset, leafChecks uint64,
) (bool, bool) {

	if !info.ok || info.buildWork == 0 {
		return false, false
	}
	if !info.cacheKey.IsZero() && qv.snap != nil {
		if _, hit := qv.snap.LoadMaterializedPredKey(info.cacheKey); hit {
			if !info.isPrefix && !orderedORMaterializedPredicateWarmHitWorth(leafChecks, info.checkWork, info.cachedCheckWork) {
				return false, false
			}
			return true, true
		}
	}
	if info.isPrefix || leafChecks == 0 || info.checkWork <= info.cachedCheckWork {
		return false, false
	}
	savedWork := satMulUint64(leafChecks, info.checkWork-info.cachedCheckWork)
	if savedWork < info.buildWork {
		return false, false
	}
	requiresPromotion := qv.orderedORMaterializedPredicateRequiresPromotion(p, est.need, orderedOffset, est.universe)
	if info.cacheKey.IsZero() || qv.snap == nil {
		return true, false
	}
	if !requiresPromotion && orderedORMaterializedPredicateColdBuildWorth(info.buildWork, info.cachedCheckWork, leafChecks, savedWork) {
		return true, false
	}
	if !qv.snap.ShouldPromoteRuntimeMaterializedPredKey(info.cacheKey) {
		return false, false
	}
	return true, false
}

func (qv *queryView) maybeWarmMaterializeOrderedORPredicates(q *qir.Shape, branches plannerORBranches, analysis *plannerOROrderAnalysis, trace *queryTrace) bool {
	if qv.snap == nil || qv.snap.MaterializedPredCacheEntryCount() == 0 {
		return false
	}
	return qv.maybeMaterializeOrderedORPredicates(q, branches, analysis, false, false, trace)
}

func (qv *queryView) maybeEagerMaterializeOrderedORPredicates(
	q *qir.Shape,
	branches plannerORBranches,
	analysis *plannerOROrderAnalysis,
	merge bool,
	trace *queryTrace,
) bool {
	return qv.maybeMaterializeOrderedORPredicates(q, branches, analysis, merge, true, trace)
}

func (qv *queryView) maybeMaterializeOrderedORPredicates(
	q *qir.Shape,
	branches plannerORBranches,
	analysis *plannerOROrderAnalysis,
	merge bool,
	allowBuild bool,
	trace *queryTrace,
) bool {
	if q == nil || !q.HasOrder || branches.Len() == 0 || analysis == nil {
		return false
	}
	needWindow, ok := orderWindow(q)
	if !ok || needWindow <= 0 {
		return false
	}
	if analysis.snapshotUniverse == 0 {
		return false
	}
	orderField := analysis.orderField
	branchCount := branches.Len()
	if branchCount > plannerORBranchLimit {
		branchCount = plannerORBranchLimit
	}
	analysis.applyCovered(branches)

	var analysisStarted time.Time
	recordAnalysis := trace != nil && trace.full()
	if recordAnalysis {
		analysisStarted = time.Now()
	}
	considered := uint64(0)
	cacheHits := uint64(0)
	builds := uint64(0)
	defer func() {
		if recordAnalysis {
			trace.addOROrderPlannerAnalysis(
				time.Since(analysisStarted),
				considered,
				cacheHits,
				builds,
				uint64(analysis.exactUniverses),
				uint64(analysis.reusedUniverses),
			)
		}
	}()
	changed := false
	var dirtyBranches [plannerORBranchLimit]bool

	var branchUniverses [plannerORBranchLimit]uint64
	for i := 0; i < branchCount; i++ {
		branchUniverses[i] = analysis.branches[i].universe
	}

	var estimates [plannerORBranchLimit]plannerOROrderedBranchEstimate
	qv.initOrderedORBranchEstimates(branches, &branchUniverses, needWindow, &estimates)

	var candidateMarks [plannerORBranchLimit][]bool
	defer func() {
		for i := 0; i < branchCount; i++ {
			if candidateMarks[i] != nil {
				pooled.ReleaseBoolSlice(candidateMarks[i])
			}
		}
	}()

	hasBuildCandidates := false
	for bi := 0; bi < branchCount; bi++ {
		branch := branches.GetPtr(bi)
		est := estimates[bi]
		rows := est.probeRows
		if rows == 0 || est.universe == 0 {
			continue
		}
		covered := analysis.branches[bi].covered
		fullChecks := pooled.GetIntSlice(branch.predLen())
		fullChecks = plannerORBranchBuildActiveChecksWithCoveredBuf(fullChecks, branch, covered)
		branchDone := false
		if !merge {
			for _, pi := range fullChecks {
				if rows == 0 {
					break
				}
				p := branch.pred(pi)
				considered++
				info := qv.orderedORPredicateBuildInfoForBranch(orderField, p, analysis, *branch, bi, pi)
				eager, cacheHit := qv.shouldEagerMaterializeOrderedORPredicate(p, info, est, q.Offset, rows)
				if eager {
					if cacheHit {
						if qv.materializeOrderedORPredicate(branch.predPtr(pi)) {
							cacheHits++
							dirtyBranches[bi] = true
							changed = true
						}
						p = branch.pred(pi)
						if p.alwaysTrue || p.alwaysFalse {
							analysis.refreshBranch(branches, bi)
							branch = branches.GetPtr(bi)
							if branch.alwaysTrue ||
								(branch.estKnown && branch.estCard == 0) ||
								analysis.mergeStats[bi].streamChecks == 0 {
								branchDone = true
								break
							}
						}
					} else if allowBuild {
						if candidateMarks[bi] == nil {
							candidateMarks[bi] = pooled.GetBoolSlice(branch.predLen())[:branch.predLen()]
							clear(candidateMarks[bi])
						}
						candidateMarks[bi][pi] = true
						hasBuildCandidates = true
					}
				}
				rows = orderedORNextPredicateRows(rows, p.estCard, est.card, est.universe)
			}
			pooled.ReleaseIntSlice(fullChecks)
			if branchDone {
				continue
			}
			continue
		}

		exactChecks := pooled.GetIntSlice(len(fullChecks))
		exactChecks = buildExactBucketPostingFilterActiveBufReader(exactChecks, fullChecks, branch.preds)
		residualChecks := pooled.GetIntSlice(len(fullChecks))
		residualChecks = plannerResidualChecksBuf(residualChecks, fullChecks, exactChecks)
		pooled.ReleaseIntSlice(fullChecks)

		for _, pi := range exactChecks {
			if rows == 0 {
				break
			}
			p := branch.pred(pi)
			considered++
			info := qv.orderedORPredicateBuildInfoForBranch(orderField, p, analysis, *branch, bi, pi)
			eager, cacheHit := qv.shouldEagerMaterializeOrderedORPredicate(p, info, est, q.Offset, rows)
			if eager {
				if cacheHit {
					if qv.materializeOrderedORPredicate(branch.predPtr(pi)) {
						cacheHits++
						dirtyBranches[bi] = true
						changed = true
					}
					p = branch.pred(pi)
					if p.alwaysTrue || p.alwaysFalse {
						analysis.refreshBranch(branches, bi)
						branch = branches.GetPtr(bi)
						if branch.alwaysTrue ||
							(branch.estKnown && branch.estCard == 0) ||
							analysis.mergeStats[bi].streamChecks == 0 {
							branchDone = true
							break
						}
					}
				} else if allowBuild {
					if candidateMarks[bi] == nil {
						candidateMarks[bi] = pooled.GetBoolSlice(branch.predLen())[:branch.predLen()]
						clear(candidateMarks[bi])
					}
					candidateMarks[bi][pi] = true
					hasBuildCandidates = true
				}
			}
			rows = orderedORNextPredicateRows(rows, p.estCard, est.card, est.universe)
		}
		if !branchDone {
			for _, pi := range residualChecks {
				if rows == 0 {
					break
				}
				p := branch.pred(pi)
				considered++
				info := qv.orderedORPredicateBuildInfoForBranch(orderField, p, analysis, *branch, bi, pi)
				eager, cacheHit := qv.shouldEagerMaterializeOrderedORPredicate(p, info, est, q.Offset, rows)
				if eager {
					if cacheHit {
						if qv.materializeOrderedORPredicate(branch.predPtr(pi)) {
							cacheHits++
							dirtyBranches[bi] = true
							changed = true
						}
						p = branch.pred(pi)
						if p.alwaysTrue || p.alwaysFalse {
							analysis.refreshBranch(branches, bi)
							branch = branches.GetPtr(bi)
							if branch.alwaysTrue ||
								(branch.estKnown && branch.estCard == 0) ||
								analysis.mergeStats[bi].streamChecks == 0 {
								branchDone = true
								break
							}
						}
					} else if allowBuild {
						if candidateMarks[bi] == nil {
							candidateMarks[bi] = pooled.GetBoolSlice(branch.predLen())[:branch.predLen()]
							clear(candidateMarks[bi])
						}
						candidateMarks[bi][pi] = true
						hasBuildCandidates = true
					}
				}
				rows = orderedORNextPredicateRows(rows, p.estCard, est.card, est.universe)
			}
		}
		pooled.ReleaseIntSlice(residualChecks)
		pooled.ReleaseIntSlice(exactChecks)
	}
	if !allowBuild || !hasBuildCandidates {
		if changed {
			for bi := 0; bi < branchCount; bi++ {
				if dirtyBranches[bi] {
					analysis.refreshBranch(branches, bi)
				}
			}
		}
		return changed
	}

	if !merge {
		for bi := 0; bi < branchCount; bi++ {
			marks := candidateMarks[bi]
			if marks == nil {
				continue
			}
			branch := branches.GetPtr(bi)
			for pi := 0; pi < branch.predLen(); pi++ {
				if !marks[pi] {
					continue
				}
				if qv.materializeOrderedORPredicate(branch.predPtr(pi)) {
					builds++
					dirtyBranches[bi] = true
					changed = true
				}
			}
		}
		if changed {
			for bi := 0; bi < branchCount; bi++ {
				if dirtyBranches[bi] {
					analysis.refreshBranch(branches, bi)
				}
			}
		}
		return changed
	}

	var branchChecks [plannerORBranchLimit][]int
	var branchExactChecks [plannerORBranchLimit][]int
	defer releasePlannerOROrderBasicCheckBufs(branchCount, &branchChecks)
	defer releasePlannerOROrderBasicCheckBufs(branchCount, &branchExactChecks)

	for bi := 0; bi < branchCount; bi++ {
		if candidateMarks[bi] == nil {
			continue
		}
		branch := branches.GetPtr(bi)
		fullChecks := pooled.GetIntSlice(branch.predLen())
		fullChecks = plannerORBranchBuildActiveChecksWithCoveredBuf(fullChecks, branch, analysis.branches[bi].covered)
		exactChecks := pooled.GetIntSlice(len(fullChecks))
		exactChecks = buildExactBucketPostingFilterActiveBufReader(exactChecks, fullChecks, branch.preds)
		branchExactChecks[bi] = exactChecks
		residualChecks := pooled.GetIntSlice(len(fullChecks))
		residualChecks = plannerResidualChecksBuf(residualChecks, fullChecks, exactChecks)
		branchChecks[bi] = residualChecks
		pooled.ReleaseIntSlice(fullChecks)
	}

	for bi := 0; bi < branchCount; bi++ {
		checks := branchChecks[bi]
		exactChecks := branchExactChecks[bi]
		marks := candidateMarks[bi]
		if marks == nil || (len(checks) == 0 && len(exactChecks) == 0) {
			continue
		}
		branch := branches.GetPtr(bi)
		est := estimates[bi]
		rows := est.probeRows
		if est.universe == 0 {
			continue
		}
		for _, pi := range exactChecks {
			if rows == 0 {
				break
			}
			p := branch.pred(pi)
			if p.alwaysFalse || p.alwaysTrue || p.covered {
				rows = orderedORNextPredicateRows(rows, p.estCard, est.card, est.universe)
				continue
			}
			if marks[pi] && qv.materializeOrderedORPredicate(branch.predPtr(pi)) {
				builds++
				dirtyBranches[bi] = true
				changed = true
				p = branch.pred(pi)
			}
			rows = orderedORNextPredicateRows(rows, p.estCard, est.card, est.universe)
		}
		for _, pi := range checks {
			if rows == 0 {
				break
			}
			p := branch.pred(pi)
			if p.alwaysFalse || p.alwaysTrue || p.covered {
				rows = orderedORNextPredicateRows(rows, p.estCard, est.card, est.universe)
				continue
			}
			if marks[pi] && qv.materializeOrderedORPredicate(branch.predPtr(pi)) {
				builds++
				dirtyBranches[bi] = true
				changed = true
				p = branch.pred(pi)
			}
			rows = orderedORNextPredicateRows(rows, p.estCard, est.card, est.universe)
		}
	}
	if changed {
		for bi := 0; bi < branchCount; bi++ {
			if dirtyBranches[bi] {
				analysis.refreshBranch(branches, bi)
			}
		}
	}
	return changed
}

func (qv *queryView) maybeEagerMaterializeNoOrderORPredicates(q *qir.Shape, branches plannerORBranches) {
	if q == nil || q.HasOrder || qv.snap == nil || qv.snap.MaterializedPredCacheLimit() <= 0 || branches.Len() == 0 {
		return
	}
	if !noOrderOREagerMaterializeEligible(branches) {
		return
	}
	needWindow, ok := orderWindow(q)
	if !ok || needWindow <= 0 {
		return
	}

	var estimates [plannerORBranchLimit]plannerOROrderedBranchEstimate
	if !qv.initNoOrderORBranchEstimates(branches, needWindow, &estimates) {
		return
	}

	branchCount := branches.Len()
	if branchCount > plannerORBranchLimit {
		branchCount = plannerORBranchLimit
	}
	for bi := 0; bi < branchCount; bi++ {
		branch := branches.GetPtr(bi)
		est := estimates[bi]
		if branch == nil || est.card == 0 || est.probeRows == 0 || est.universe == 0 {
			continue
		}
		leadNeedsCheck := branch.leadPtr() != nil && branch.leadPtr().leadIterNeedsContainsCheck()
		rows := est.probeRows
		for pi := 0; pi < branch.predLen() && rows > 0; pi++ {
			p := branch.pred(pi)
			if p.alwaysTrue || p.alwaysFalse || p.covered || !p.hasContains() {
				continue
			}
			if pi == branch.leadIdx && !leadNeedsCheck {
				continue
			}
			nextRows := orderedORNextPredicateRows(rows, p.estCard, est.card, est.universe)
			if !qv.shouldKeepORBranchNumericRangeLazy(p.expr) {
				rows = nextRows
				continue
			}
			info := qv.orderedORMaterializedPredicateBuildInfo("", p)
			if info.ok {
				if eager, _ := qv.shouldEagerMaterializeOrderedORPredicate(p, info, est, 0, rows); eager {
					qv.materializeOrderedORPredicate(branch.predPtr(pi))
				}
			}
			rows = nextRows
		}
	}
}

func (qv *queryView) promoteOrderedORMaterializedBaseOps(
	q *qir.Shape,
	branches plannerORBranches,
	branchChecks [plannerORBranchLimit][]int,
	observed *orderedORObservedStats,
	analysis *plannerOROrderAnalysis,
) {
	if q == nil || branches.Len() == 0 || qv.snap == nil || !q.HasOrder || observed == nil {
		return
	}
	orderField := qv.engine.fieldNameByOrdinal(q.Order.FieldOrdinal)
	if q.Order.FieldOrdinal < 0 {
		return
	}
	branchCount := branches.Len()
	if branchCount > plannerORBranchLimit {
		branchCount = plannerORBranchLimit
	}
	repCap := 0
	for bi := 0; bi < branchCount; bi++ {
		repCap += len(branchChecks[bi])
	}

	cacheKeysBuf := qcache.GetMaterializedPredKeySlice(repCap)
	defer qcache.ReleaseMaterializedPredKeySlice(cacheKeysBuf)

	repBranchBuf := pooled.GetIntSlice(repCap)
	defer pooled.ReleaseIntSlice(repBranchBuf)

	repPredBuf := pooled.GetIntSlice(repCap)
	defer pooled.ReleaseIntSlice(repPredBuf)

	buildWorksBuf := pooled.GetUint64Slice(repCap)
	defer func() { pooled.ReleaseUint64Slice(buildWorksBuf) }()

	observedWorksBuf := pooled.GetUint64Slice(repCap)
	defer func() { pooled.ReleaseUint64Slice(observedWorksBuf) }()

	for bi := 0; bi < branchCount; bi++ {
		branch := branches.Get(bi)
		checks := branchChecks[bi]
		start, end, ok := observed.branchRange(bi)
		if !ok {
			continue
		}
		for ci, pi := range checks {
			p := branch.pred(pi)
			if p.alwaysFalse || p.covered || p.alwaysTrue {
				continue
			}
			if start+ci >= end || !observed.candidatesBuf[start+ci] {
				continue
			}
			leafChecks := observed.countsBuf[start+ci]
			if leafChecks == 0 {
				continue
			}
			info := qv.orderedORPredicateBuildInfoForBranch(orderField, p, analysis, branch, bi, pi)
			if !info.ok || info.cacheKey.IsZero() || info.buildWork == 0 {
				continue
			}
			found := false
			for slot := 0; slot < len(cacheKeysBuf); slot++ {
				if cacheKeysBuf[slot] == info.cacheKey {
					if info.isPrefix {
						observedWorksBuf[slot] = satAddUint64(observedWorksBuf[slot], leafChecks)
					} else if info.checkWork > info.cachedCheckWork {
						observedWorksBuf[slot] = satAddUint64(observedWorksBuf[slot], satMulUint64(leafChecks, info.checkWork-info.cachedCheckWork))
					}
					found = true
					break
				}
			}
			if found {
				continue
			}
			cacheKeysBuf = append(cacheKeysBuf, info.cacheKey)
			repBranchBuf = append(repBranchBuf, bi)
			repPredBuf = append(repPredBuf, pi)
			buildWorksBuf = append(buildWorksBuf, info.buildWork)
			if info.isPrefix {
				observedWorksBuf = append(observedWorksBuf, leafChecks)
			} else if info.checkWork > info.cachedCheckWork {
				observedWorksBuf = append(observedWorksBuf, satMulUint64(leafChecks, info.checkWork-info.cachedCheckWork))
			} else {
				observedWorksBuf = append(observedWorksBuf, 0)
			}
		}
	}
	if len(cacheKeysBuf) == 0 {
		return
	}
	for i := 0; i < len(cacheKeysBuf); i++ {
		if observedWorksBuf[i] == 0 {
			continue
		}
		if _, ok := qv.snap.LoadMaterializedPredKey(cacheKeysBuf[i]); ok {
			continue
		}
		if !qv.snap.ShouldPromoteObservedOrderedORMaterializedPredKey(cacheKeysBuf[i], observedWorksBuf[i], buildWorksBuf[i]) {
			continue
		}
		branchIdx := repBranchBuf[i]
		predIdx := repPredBuf[i]
		qv.materializeOrderedORPredicate(branches.GetPtr(branchIdx).predPtr(predIdx))
	}
}

func (qv *queryView) promoteObservedOrderedORKWayMaterializedBaseOps(
	q *qir.Shape,
	branches plannerORBranches,
	branchChecks [plannerORBranchLimit][]int,
	branchObservedRows *[plannerORBranchLimit]uint64,
	branchUniverses *[plannerORBranchLimit]uint64,
	analysis *plannerOROrderAnalysis,
) {
	if q == nil || branches.Len() == 0 || qv.snap == nil || !q.HasOrder || branchObservedRows == nil || branchUniverses == nil {
		return
	}
	needWindow, ok := orderWindow(q)
	if !ok || needWindow <= 0 {
		return
	}
	orderField := qv.engine.fieldNameByOrdinal(q.Order.FieldOrdinal)
	if q.Order.FieldOrdinal < 0 {
		return
	}

	var estimates [plannerORBranchLimit]plannerOROrderedBranchEstimate
	qv.initOrderedORBranchEstimates(branches, branchUniverses, needWindow, &estimates)
	branchCount := branches.Len()
	if branchCount > plannerORBranchLimit {
		branchCount = plannerORBranchLimit
	}
	repCap := 0
	for bi := 0; bi < branchCount; bi++ {
		repCap += len(branchChecks[bi])
	}

	cacheKeysBuf := qcache.GetMaterializedPredKeySlice(repCap)
	defer qcache.ReleaseMaterializedPredKeySlice(cacheKeysBuf)

	repBranchBuf := pooled.GetIntSlice(repCap)
	defer pooled.ReleaseIntSlice(repBranchBuf)

	repPredBuf := pooled.GetIntSlice(repCap)
	defer pooled.ReleaseIntSlice(repPredBuf)

	buildWorksBuf := pooled.GetUint64Slice(repCap)
	defer func() { pooled.ReleaseUint64Slice(buildWorksBuf) }()

	observedWorksBuf := pooled.GetUint64Slice(repCap)
	defer func() { pooled.ReleaseUint64Slice(observedWorksBuf) }()

	for bi := 0; bi < branchCount; bi++ {
		checks := branchChecks[bi]
		if len(checks) == 0 {
			continue
		}
		rows := branchObservedRows[bi]
		est := estimates[bi]
		if rows == 0 || est.universe == 0 {
			continue
		}
		branch := branches.Get(bi)
		for _, pi := range checks {
			if rows == 0 {
				break
			}
			p := branch.pred(pi)
			if p.alwaysFalse || p.covered || p.alwaysTrue {
				continue
			}
			info := qv.orderedORPredicateBuildInfoForBranch(orderField, p, analysis, branch, bi, pi)
			if info.ok && !info.cacheKey.IsZero() && info.buildWork != 0 {
				observedWork := uint64(0)
				if info.isPrefix {
					observedWork = rows
				} else if info.checkWork > info.cachedCheckWork {
					observedWork = satMulUint64(rows, info.checkWork-info.cachedCheckWork)
				}
				if observedWork != 0 {
					found := false
					for slot := 0; slot < len(cacheKeysBuf); slot++ {
						if cacheKeysBuf[slot] == info.cacheKey {
							observedWorksBuf[slot] = satAddUint64(observedWorksBuf[slot], observedWork)
							found = true
							break
						}
					}
					if !found {
						cacheKeysBuf = append(cacheKeysBuf, info.cacheKey)
						repBranchBuf = append(repBranchBuf, bi)
						repPredBuf = append(repPredBuf, pi)
						buildWorksBuf = append(buildWorksBuf, info.buildWork)
						observedWorksBuf = append(observedWorksBuf, observedWork)
					}
				}
			}
			rows = orderedORNextPredicateRows(rows, p.estCard, est.card, est.universe)
		}
	}

	for i := 0; i < len(cacheKeysBuf); i++ {
		if observedWorksBuf[i] == 0 {
			continue
		}
		if _, ok := qv.snap.LoadMaterializedPredKey(cacheKeysBuf[i]); ok {
			continue
		}
		if !qv.snap.ShouldPromoteObservedOrderedORMaterializedPredKey(cacheKeysBuf[i], observedWorksBuf[i], buildWorksBuf[i]) {
			continue
		}
		branchIdx := repBranchBuf[i]
		predIdx := repPredBuf[i]
		qv.materializeOrderedORPredicate(branches.GetPtr(branchIdx).predPtr(predIdx))
	}
}

func (qv *queryView) shouldObserveOrderedORPredicate(p predicate, info orderedORMaterializedPredicateBuildInfo) bool {
	if !info.ok || info.cacheKey.IsZero() || info.buildWork == 0 {
		return false
	}
	if !info.isPrefix && info.checkWork == 0 {
		return false
	}
	if _, hit := qv.snap.LoadMaterializedPredKey(info.cacheKey); hit {
		return false
	}
	return true
}

func hasNoOrderLeadCandidatesOR(ops []qir.Expr) bool {
	for _, op := range ops {
		if !branchHasPositiveLeafOR(op) {
			return false
		}
	}
	return true
}

func branchHasPositiveLeafOR(e qir.Expr) bool {
	switch e.Op {
	case qir.OpAND:
		if e.Not || len(e.Operands) == 0 {
			return false
		}
		for _, ch := range e.Operands {
			if branchHasPositiveLeafOR(ch) {
				return true
			}
		}
		return false
	case qir.OpNOOP, qir.OpOR:
		return false
	default:
		if e.Not {
			return false
		}
		return true
	}
}

func (qv *queryView) shouldKeepORBranchNumericRangeLazy(e qir.Expr) bool {
	if qv.snap == nil || e.Not || e.FieldOrdinal < 0 || !e.Op.IsNumericRange() {
		return false
	}
	fm := qv.fieldMetaByExpr(e)
	if !schema.FieldUsesOrderedNumericKeys(fm) {
		return false
	}
	candidate, ok := qv.prepareScalarRangeRoutingCandidate(e)
	if !ok {
		return false
	}
	universe := qv.snapshotUniverseCardinality()
	if universe == 0 {
		return false
	}
	if qv.shouldForceORBranchNumericRangeMaterializeWithCandidate(candidate, universe) {
		return false
	}
	// Keep broad positive ranges on runtime state inside multi-leaf OR branches.
	// Runtime second-hit sharing already rejects this shape; forcing eager
	// materialization here creates a hotter-but-heavier branch state.
	return !allowRuntimePositiveRangeSecondHitShare(candidate.plan.est, universe)
}

func (qv *queryView) shouldForceORBranchNumericRangeMaterializeWithCandidate(
	candidate preparedScalarRangeRoutingCandidate,
	_ uint64,
) bool {
	return candidate.plan.useComplement &&
		candidate.core.qv.nilFieldOverlayForExpr(candidate.core.expr).LookupCardinality(nilIndexEntryKey) > 0
}

func (qv *queryView) shouldForceORBranchNumericRangeMaterialize(e qir.Expr) bool {
	if e.Not || e.FieldOrdinal < 0 || !e.Op.IsNumericRange() {
		return false
	}
	fm := qv.fieldMetaByExpr(e)
	if !schema.FieldUsesOrderedNumericKeys(fm) {
		return false
	}
	candidate, ok := qv.prepareScalarRangeRoutingCandidate(e)
	if !ok {
		return false
	}
	return qv.shouldForceORBranchNumericRangeMaterializeWithCandidate(candidate, qv.snapshotUniverseCardinality())
}

func (qv *queryView) shouldBuildORBranchLeafLazy(e qir.Expr, branchLeafCount int) bool {
	if qv.snap == nil || qv.snap.MaterializedPredCacheLimit() <= 0 || branchLeafCount <= 1 {
		return false
	}
	if e.Not || e.FieldOrdinal < 0 || !e.Op.IsNumericRange() {
		return false
	}
	fm := qv.fieldMetaByExpr(e)
	if !schema.FieldUsesOrderedNumericKeys(fm) {
		return false
	}
	candidate, ok := qv.prepareScalarRangeRoutingCandidate(e)
	if !ok {
		return false
	}
	universe := qv.snapshotUniverseCardinality()
	if universe == 0 {
		return false
	}
	if qv.shouldForceORBranchNumericRangeMaterializeWithCandidate(candidate, universe) {
		return false
	}
	key := qv.materializedPredKey(e)
	if key.IsZero() {
		return false
	}
	hot := qv.snap.ShouldPromoteRuntimeMaterializedPredKey(key)
	if qv.shouldKeepORBranchNumericRangeLazy(e) {
		if candidate, ok := qv.prepareScalarRangeRoutingCandidate(e); ok &&
			candidate.plan.useComplement &&
			!candidate.core.complementCacheKey.IsZero() {
			qv.snap.ShouldPromoteRuntimeMaterializedPredKey(candidate.core.complementCacheKey)
		}
		return true
	}
	return !hot
}

func predicateHasBroadPositiveComplementRuntimeRange(p predicate) bool {
	if p.expr.Not || p.expr.FieldOrdinal < 0 || !p.expr.Op.IsNumericRange() {
		return false
	}
	if p.overlayState != nil {
		return p.overlayState.probe.useComplement && !p.overlayState.keepProbeHits
	}
	return false
}

func (qv *queryView) mayNeedORBranchRangeRewrite(leaves []qir.Expr) bool {
	if len(leaves) <= 1 {
		return false
	}
	for _, e := range leaves {
		if e.Not || e.FieldOrdinal < 0 || !e.Op.IsNumericRange() {
			continue
		}
		fm := qv.fieldMetaByExpr(e)
		if schema.FieldUsesOrderedNumericKeys(fm) {
			if qv.snap != nil && qv.snap.MaterializedPredCacheLimit() > 0 {
				return true
			}
			if qv.shouldForceORBranchNumericRangeMaterialize(e) {
				return true
			}
		}
	}
	return false
}

func noOrderOREagerMaterializeEligible(branches plannerORBranches) bool {
	for bi := 0; bi < branches.Len(); bi++ {
		branch := branches.Get(bi)
		for pi := 0; pi < branch.predLen(); pi++ {
			p := branch.pred(pi)
			if p.alwaysTrue || p.alwaysFalse || p.covered || !p.hasContains() {
				continue
			}
			if predicateHasBroadPositiveComplementRuntimeRange(p) {
				return true
			}
		}
	}
	return false
}

func (qv *queryView) buildORBranchPredicates(leaves []qir.Expr) (predicateSet, bool) {
	if !qv.mayNeedORBranchRangeRewrite(leaves) {
		return qv.buildPredicatesWithColdMode(leaves, true, false)
	}

	preds := newPredicateSet(len(leaves))
	forced := pooled.GetIntSlice(len(leaves))
	defer pooled.ReleaseIntSlice(forced)
	leadIdx := -1
	leadEst := uint64(0)
	for _, e := range leaves {
		forceMaterialize := qv.shouldForceORBranchNumericRangeMaterialize(e)
		p, ok := qv.buildPredicateWithColdMode(e, true, qv.shouldBuildORBranchLeafLazy(e, len(leaves)))
		if !ok {
			preds.Release()
			return predicateSet{}, false
		}
		preds.Append(p)
		pi := preds.Len() - 1
		if forceMaterialize && p.hasRuntimeRangeState() {
			forced = append(forced, pi)
			continue
		}
		if p.alwaysTrue || p.covered || p.alwaysFalse || !p.hasContains() || !p.hasIter() {
			continue
		}
		if leadIdx == -1 || p.estCard < leadEst {
			leadIdx = pi
			leadEst = p.estCard
		}
	}
	for _, pi := range forced {
		p := preds.GetPtr(pi)
		if !qv.materializePositiveScalarRangePredicate(p) {
			preds.Release()
			return predicateSet{}, false
		}
	}
	return preds, true
}

// buildORBranches compiles each OR branch into planner predicates and optional
// lead iterators used by OR execution strategies.
func (qv *queryView) buildORBranches(ops []qir.Expr) (plannerORBranches, bool, bool) {
	out := newPlannerORBranches(len(ops))
	var leavesBuf [8]qir.Expr

	for _, op := range ops {
		leaves, ok := collectAndLeavesModeScratch(op, leavesBuf[:0], andLeafModeCollect)
		if !ok {
			out.Release()
			return plannerORBranches{}, false, false
		}

		preds, ok := qv.buildORBranchPredicates(leaves)
		if !ok {
			out.Release()
			return plannerORBranches{}, false, false
		}
		branch, keep, ok := buildPlannerORBranch(op, preds)
		if !ok {
			out.Release()
			return plannerORBranches{}, false, false
		}
		if keep {
			out.Append(branch)
		}
	}

	if out.Len() == 0 {
		out.Release()
		return plannerORBranches{}, true, true
	}

	return out, false, true
}

func (qv *queryView) buildORBranchesOrdered(
	ops []qir.Expr,
	orderField string,
	orderedWindow int,
	orderedOffset uint64,
) (plannerORBranches, bool, bool) {
	out := newPlannerORBranches(len(ops))
	var leavesBuf [8]qir.Expr

	for _, op := range ops {
		leaves, ok := collectAndLeavesModeScratch(op, leavesBuf[:0], andLeafModeCollect)
		if !ok {
			out.Release()
			return plannerORBranches{}, false, false
		}

		// Ordered OR merge consumes predicates incrementally from order buckets.
		// Eager materializing non-order range predicates here can dominate total
		// query cost before branch streaming even starts.
		preds, ok := qv.buildPredicatesOrderedWithMode(leaves, orderField, false, orderedWindow, orderedOffset, true, false)
		// preds, ok := qv.buildPredicatesOrderedWithMode(leaves, orderField, false, orderedWindow, orderedOffset, true, true)
		if !ok {
			out.Release()
			return plannerORBranches{}, false, false
		}
		branch, keep, ok := buildPlannerORBranch(op, preds)
		if !ok {
			out.Release()
			return plannerORBranches{}, false, false
		}
		if keep {
			out.Append(branch)
		}
	}

	if out.Len() == 0 {
		out.Release()
		return plannerORBranches{}, true, true
	}

	branches := out
	ov := qv.fieldOverlay(orderField)
	if !ov.HasData() {
		return branches, false, true
	}
	for i := 0; i < branches.Len(); i++ {
		branch := branches.GetPtr(i)
		if !branch.alwaysTrue || branch.predLen() == 0 {
			continue
		}
		br, covered, rangeOK := qv.extractOrderRangeCoverageOverlayReader(orderField, branch.preds, ov)
		if !rangeOK {
			branches.Release()
			return plannerORBranches{}, false, false
		}
		if len(covered) == 0 {
			pooled.ReleaseBoolSlice(covered)
			continue
		}
		if br.BaseStart != 0 || br.BaseEnd != ov.KeyCount() {
			branch.alwaysTrue = false
			branch.coveredRangeBounded = true
			branch.coveredRangeStart = br.BaseStart
			branch.coveredRangeEnd = br.BaseEnd
			if br.BaseStart == br.BaseEnd {
				branch.estCard = 0
				branch.estKnown = true
			}
		}
		pooled.ReleaseBoolSlice(covered)
	}
	return branches, false, true
}

// execPlanOROrderBasic evaluates ordered OR by scanning ordered buckets and
// checking branch predicates per candidate.
//
// It keeps deterministic ordering semantics and avoids full OR unions for LIMIT-heavy queries.
func (qv *queryView) execPlanOROrderBasic(q *qir.Shape, branches plannerORBranches, analysis *plannerOROrderAnalysis, trace *queryTrace, observed *orderedORObservedStats) ([]uint64, bool) {
	o := q.Order
	f := qv.engine.fieldNameByOrdinal(o.FieldOrdinal)
	if o.FieldOrdinal < 0 {
		return nil, false
	}

	fm := qv.fieldMetaByOrder(o)
	if fm == nil || fm.Slice {
		return nil, false
	}

	ov := qv.fieldOverlayForOrder(o)
	if !ov.HasData() {
		return nil, false
	}

	alwaysTrue := false
	alwaysTrueBranch := -1
	for i := 0; i < branches.Len(); i++ {
		if branches.Get(i).alwaysTrue {
			alwaysTrue = true
			alwaysTrueBranch = i
			break
		}
	}

	branchCount := branches.Len()
	var (
		branchChecks [plannerORBranchLimit][]int
		branchStart  [plannerORBranchLimit]int
		branchEnd    [plannerORBranchLimit]int
	)
	for i := 0; i < branches.Len(); i++ {
		branch := branches.GetPtr(i)
		var covered []bool
		if analysis != nil && i < analysis.branchCount {
			covered = analysis.branches[i].covered
			branchStart[i] = analysis.branches[i].rangeStart
			branchEnd[i] = analysis.branches[i].rangeEnd
		} else {
			br, rangeCovered, ok := qv.extractOrderRangeCoverageOverlayReader(f, branch.preds, ov)
			if !ok {
				releasePlannerOROrderBasicCheckBufs(branchCount, &branchChecks)
				return nil, false
			}
			covered = rangeCovered
			branchStart[i], branchEnd[i] = br.BaseStart, br.BaseEnd
		}
		for pi := 0; pi < len(covered); pi++ {
			if covered[pi] {
				branch.predPtr(pi).covered = true
			}
		}
		if analysis == nil || i >= analysis.branchCount {
			pooled.ReleaseBoolSlice(covered)
		}
		branchChecks[i] = pooled.GetIntSlice(branch.predLen())
		branchChecks[i] = branch.buildMatchChecksBuf(branchChecks[i])
	}
	promoteObserved := false
	if observed != nil {
		initOrderedORObservedStats(qv, f, observed, branches, branchChecks, analysis)
		if observed.active {
			promoteObserved = true
		} else {
			observed = nil
		}
	}

	skip := q.Offset
	need := int(q.Limit)
	out := make([]uint64, 0, need)

	var branchEvalOrder [plannerORBranchLimit]int
	branchEvalN := 0
	if !alwaysTrue {
		// Reordering branches is a fail-fast optimization: cheap/selective
		// branches are checked first for the common negative case.
		branchEvalOrder, branchEvalN = branches.evalOrder()
	}

	br := ov.RangeForBounds((indexdata.Bounds{Has: true}))
	fullTrace := trace.full()

	if !fullTrace {
		cur := ov.NewCursor(br, o.Desc)
		bucket := 0
		if o.Desc {
			bucket = ov.KeyCount() - 1
		}
		for need > 0 {
			_, bm, ok := cur.Next()
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

			if idx, ok := bm.TrySingle(); ok {
				matched := alwaysTrue
				if !matched {
					if observed != nil {
						matched = plannerOROrderBasicMatchesObserved(
							branches,
							&branchEvalOrder,
							branchEvalN,
							&branchStart,
							&branchEnd,
							&branchChecks,
							idx,
							curBucket,
							observed,
						)
					} else {
						matched = plannerOROrderBasicMatches(
							branches,
							&branchEvalOrder,
							branchEvalN,
							&branchStart,
							&branchEnd,
							&branchChecks,
							idx,
							curBucket,
						)
					}
				}
				if !matched {
					continue
				}
				if skip > 0 {
					skip--
					continue
				}
				out = append(out, idx)
				need--
				continue
			}

			it := bm.Iter()
			for need > 0 && it.HasNext() {
				idx := it.Next()
				matched := alwaysTrue
				if !matched {
					if observed != nil {
						matched = plannerOROrderBasicMatchesObserved(
							branches,
							&branchEvalOrder,
							branchEvalN,
							&branchStart,
							&branchEnd,
							&branchChecks,
							idx,
							curBucket,
							observed,
						)
					} else {
						matched = plannerOROrderBasicMatches(
							branches,
							&branchEvalOrder,
							branchEvalN,
							&branchStart,
							&branchEnd,
							&branchChecks,
							idx,
							curBucket,
						)
					}
				}
				if !matched {
					continue
				}
				if skip > 0 {
					skip--
					continue
				}
				out = append(out, idx)
				need--
			}
			it.Release()
		}
		if promoteObserved {
			qv.promoteOrderedORMaterializedBaseOps(q, branches, branchChecks, observed, analysis)
		}
		releasePlannerOROrderBasicCheckBufs(branchCount, &branchChecks)
		return out, true
	}

	var branchMetricsInline [plannerORBranchLimit]TraceORBranch
	branchMetrics := branchMetricsInline[:branches.Len()]
	for i := range branchMetrics {
		branchMetrics[i].Index = i
	}
	scanWidth := uint64(0)
	stopReason := "input_exhausted"

	cur := ov.NewCursor(br, o.Desc)
	bucket := 0
	if o.Desc {
		bucket = ov.KeyCount() - 1
	}
	for need > 0 {
		_, bm, ok := cur.Next()
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
		if idx, ok := bm.TrySingle(); ok {
			matchedCount := plannerOROrderBasicMatchWithMetrics(
				branches,
				&branchStart,
				&branchEnd,
				&branchChecks,
				branchMetrics,
				idx,
				curBucket,
				alwaysTrue,
				alwaysTrueBranch,
			)
			if matchedCount == 0 {
				continue
			}
			if matchedCount > 1 {
				trace.addDedupe(uint64(matchedCount - 1))
			}
			if skip > 0 {
				skip--
				continue
			}
			out = append(out, idx)
			need--
			if need == 0 {
				stopReason = "limit_reached"
			}
			continue
		}

		it := bm.Iter()
		for need > 0 && it.HasNext() {
			idx := it.Next()
			matchedCount := plannerOROrderBasicMatchWithMetrics(
				branches,
				&branchStart,
				&branchEnd,
				&branchChecks,
				branchMetrics,
				idx,
				curBucket,
				alwaysTrue,
				alwaysTrueBranch,
			)
			if matchedCount == 0 {
				continue
			}
			if matchedCount > 1 {
				trace.addDedupe(uint64(matchedCount - 1))
			}
			if skip > 0 {
				skip--
				continue
			}
			out = append(out, idx)
			need--
			if need == 0 {
				stopReason = "limit_reached"
			}
		}
		it.Release()
	}

	trace.addOrderScanWidth(scanWidth)
	trace.setORBranches(branchMetrics)
	trace.setEarlyStopReason(stopReason)
	if promoteObserved {
		qv.promoteOrderedORMaterializedBaseOps(q, branches, branchChecks, observed, analysis)
	}
	releasePlannerOROrderBasicCheckBufs(branchCount, &branchChecks)
	return out, true
}

func orderWindow(q *qir.Shape) (int, bool) {
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

func (qv *queryView) shouldPreferOROrderFallbackFirst(q *qir.Shape, branches plannerORBranches) bool {
	d, ok := qv.decideOROrderFallbackFirst(q, branches)
	return ok && d.prefer
}

func (qv *queryView) decideOROrderFallbackFirst(q *qir.Shape, branches plannerORBranches) (plannerOROrderFallbackDecision, bool) {
	return qv.decideOROrderFallbackFirstWithAnalysis(q, branches, nil)
}

func (qv *queryView) decideOROrderFallbackFirstWithAnalysis(
	q *qir.Shape,
	branches plannerORBranches,
	analysis *plannerOROrderAnalysis,
) (plannerOROrderFallbackDecision, bool) {
	need, ok := orderWindow(q)
	if !ok || need <= 0 {
		return plannerOROrderFallbackDecision{}, false
	}
	if !q.HasOrder {
		return plannerOROrderFallbackDecision{}, false
	}

	order := q.Order
	ov := qv.fieldOverlayForOrder(order)
	var mergeStats [plannerORBranchLimit]plannerOROrderMergeBranchStats
	if analysis != nil {
		mergeStats = analysis.mergeStats
	} else {
		mergeStats = qv.orderMergeBranchStats(qv.engine.fieldNameByOrdinal(order.FieldOrdinal), branches, ov)
	}
	routeCost, ok := qv.estimateOROrderMergeRouteCost(q, branches, need, mergeStats)
	if !ok {
		return plannerOROrderFallbackDecision{}, false
	}
	avgChecks := routeCost.avgChecks
	if avgChecks <= 0 {
		avgChecks = 1
	}
	fallbackCollectFast := ov.HasData()
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

func (qv *queryView) shouldUseOROrderKWayRuntimeFallback(q *qir.Shape, branches plannerORBranches, needWindow int) bool {
	d, ok := qv.decideOROrderKWayRuntimeFallback(q, branches, needWindow)
	return ok && d.enable
}

func (qv *queryView) decideOROrderKWayRuntimeFallback(q *qir.Shape, branches plannerORBranches, needWindow int) (plannerOROrderRuntimeGuardDecision, bool) {
	return qv.decideOROrderKWayRuntimeFallbackWithAnalysis(q, branches, needWindow, nil)
}

func (qv *queryView) decideOROrderKWayRuntimeFallbackWithAnalysis(
	q *qir.Shape,
	branches plannerORBranches,
	needWindow int,
	analysis *plannerOROrderAnalysis,
) (plannerOROrderRuntimeGuardDecision, bool) {
	if needWindow <= 0 || !q.HasOrder || branches.Len() < 2 {
		return plannerOROrderRuntimeGuardDecision{}, false
	}

	order := q.Order
	orderField := qv.engine.fieldNameByOrdinal(order.FieldOrdinal)
	if order.FieldOrdinal < 0 {
		return plannerOROrderRuntimeGuardDecision{}, false
	}

	ov := qv.fieldOverlayForOrder(order)
	var mergeStats [plannerORBranchLimit]plannerOROrderMergeBranchStats
	if analysis != nil {
		mergeStats = analysis.mergeStats
	} else {
		mergeStats = qv.orderMergeBranchStats(orderField, branches, ov)
	}
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

	hasPrefixTailRisk := qv.hasNonOrderPrefixTailRisk(branches, orderField)
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

func (qv *queryView) execPlanOROrderMerge(
	q *qir.Shape,
	branches plannerORBranches,
	analysis *plannerOROrderAnalysis,
	trace *queryTrace,
) ([]uint64, bool, error) {
	fallbackDec, decOK := qv.decideOROrderFallbackFirstWithAnalysis(q, branches, analysis)
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
		if q.Offset > 0 {
			qv.maybeEagerMaterializeOrderedORPredicates(q, branches, analysis, false, trace)
		}
		return qv.execPlanOROrderMergeFallback(q, branches, trace)
	}
	if q.Offset > 0 {
		qv.maybeEagerMaterializeOrderedORPredicates(q, branches, analysis, true, trace)
	}
	out, ok, err := qv.execPlanOROrderKWay(q, branches, analysis, trace)
	if ok || err != nil {
		return out, ok, err
	}
	if q.Offset > 0 {
		qv.maybeEagerMaterializeOrderedORPredicates(q, branches, analysis, false, trace)
	}
	return qv.execPlanOROrderMergeFallback(q, branches, trace)
}

type plannerOROrderMergeItem struct {
	branch int
	bucket int
	idx    uint64
}

type plannerOROrderMergeHeap struct {
	desc     bool
	items    []plannerOROrderMergeItem
	itemsBuf *pooled.Slice[plannerOROrderMergeItem]
}

func (h *plannerOROrderMergeHeap) len() int {
	if h.itemsBuf != nil {
		return h.itemsBuf.Len()
	}
	return len(h.items)
}

func (h *plannerOROrderMergeHeap) item(i int) plannerOROrderMergeItem {
	if h.itemsBuf != nil {
		return h.itemsBuf.Get(i)
	}
	return h.items[i]
}

func (h *plannerOROrderMergeHeap) setItem(i int, item plannerOROrderMergeItem) {
	if h.itemsBuf != nil {
		h.itemsBuf.Set(i, item)
		return
	}
	h.items[i] = item
}

func (h *plannerOROrderMergeHeap) less(i, j int) bool {
	a := h.item(i)
	b := h.item(j)
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

func (h *plannerOROrderMergeHeap) swap(i, j int) {
	a := h.item(i)
	b := h.item(j)
	h.setItem(i, b)
	h.setItem(j, a)
}

func (h *plannerOROrderMergeHeap) push(x plannerOROrderMergeItem) {
	if h.itemsBuf != nil {
		h.itemsBuf.Append(x)
		h.up(h.itemsBuf.Len() - 1)
		return
	}
	h.items = append(h.items, x)
	h.up(len(h.items) - 1)
}

func (h *plannerOROrderMergeHeap) pop() plannerOROrderMergeItem {
	n := h.len() - 1
	h.swap(0, n)
	x := h.item(n)
	if h.itemsBuf != nil {
		h.itemsBuf.SetLen(n)
	} else {
		h.items = h.items[:n]
	}
	if h.len() > 0 {
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
	n := h.len()
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

type plannerOROrderIterStore struct {
	inline [8]plannerOROrderBranchIter
	buf    *pooled.Slice[plannerOROrderBranchIter]
}

func (s *plannerOROrderIterStore) get(i int) plannerOROrderBranchIter {
	if s.buf != nil {
		return s.buf.Get(i)
	}
	return s.inline[i]
}

func (s *plannerOROrderIterStore) set(i int, iter plannerOROrderBranchIter) {
	if s.buf != nil {
		s.buf.Set(i, iter)
		return
	}
	s.inline[i] = iter
}

func closePlannerOROrderInlineIters(iters *[8]plannerOROrderBranchIter, n int) {
	for i := 0; i < n; i++ {
		iters[i].close()
	}
}

type plannerOROrderBranchIter struct {
	branch         *plannerORBranch
	checks         []int
	exactChecks    []int
	residualChecks []int
	overlay        indexdata.FieldOverlay
	desc           bool
	single         int
	exactSingle    int
	residualSingle int
	allChecksExact bool

	startBucket int
	endBucket   int

	nextBucket    int
	curBucket     int
	curIter       posting.Iterator
	curExact      bool
	curChecks     []int
	curSingle     int
	curResidual   bool
	curSplitExact bool
	bucketWork    posting.List
	bucketSeen    []bool

	has bool
	cur uint64
}

type plannerOrderIndexView struct {
	overlay indexdata.FieldOverlay
	release func()
}

func (v plannerOrderIndexView) close() {
	if v.release != nil {
		v.release()
	}
}

// plannerOrderIndexSnapshotViewByOrdinal returns the current immutable order-index slice.
func (qv *queryView) plannerOrderIndexSnapshotViewByOrdinal(fieldOrdinal int) (plannerOrderIndexView, bool) {
	ov := qv.fieldOverlayByOrdinal(fieldOrdinal)
	if !ov.HasData() {
		return plannerOrderIndexView{}, false
	}
	return plannerOrderIndexView{overlay: ov}, true
}

func (it *plannerOROrderBranchIter) init() {
	if it.startBucket < 0 {
		it.startBucket = 0
	}
	if it.endBucket <= 0 || it.endBucket > it.overlay.KeyCount() {
		it.endBucket = it.overlay.KeyCount()
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
	if it.checks != nil {
		pooled.ReleaseIntSlice(it.checks)
		it.checks = nil
	}
	if it.exactChecks != nil {
		pooled.ReleaseIntSlice(it.exactChecks)
		it.exactChecks = nil
	}
	if it.residualChecks != nil {
		pooled.ReleaseIntSlice(it.residualChecks)
		it.residualChecks = nil
	}
	it.curChecks = nil
	it.curResidual = false
	it.curSplitExact = false
}

func (it *plannerOROrderBranchIter) matchesChecks(idx uint64, checks []int, single int) bool {
	if len(checks) == 0 {
		return true
	}
	if single >= 0 {
		p := it.branch.predPtr(single)
		return !p.alwaysFalse && p.hasContains() && p.matches(idx)
	}
	return it.branch.matchesChecksBuf(idx, checks)
}

func (it *plannerOROrderBranchIter) matchBucketCandidate(idx uint64) (bool, bool) {
	if len(it.exactChecks) > 0 {
		if !it.matchesChecks(idx, it.exactChecks, it.exactSingle) {
			return false, false
		}
		if it.allChecksExact || len(it.residualChecks) == 0 {
			return true, false
		}
		return it.matchesChecks(idx, it.residualChecks, it.residualSingle), true
	}
	if len(it.checks) == 0 {
		return true, false
	}
	return it.matchesChecks(idx, it.checks, it.single), true
}

func (it *plannerOROrderBranchIter) matchCurrentCandidate(idx uint64) (bool, bool) {
	if it.curSplitExact {
		if !it.matchesChecks(idx, it.exactChecks, it.exactSingle) {
			return false, false
		}
		if len(it.residualChecks) == 0 {
			return true, false
		}
		return it.matchesChecks(idx, it.residualChecks, it.residualSingle), true
	}
	if len(it.curChecks) == 0 {
		return true, false
	}
	return it.matchesChecks(idx, it.curChecks, it.curSingle), it.curResidual
}

func (it *plannerOROrderBranchIter) advance() (uint64, uint64, uint64, bool) {
	examinedDelta := uint64(0)
	residualExaminedDelta := uint64(0)
	emittedDelta := uint64(0)
	it.has = false
	for {
		if it.curIter != nil {
			if it.curExact {
				if it.curIter.HasNext() {
					it.cur = it.curIter.Next()
					it.has = true
					return examinedDelta, residualExaminedDelta, emittedDelta, true
				}
				it.curIter.Release()
				it.curIter = nil
				it.curExact = false
				continue
			}
			for it.curIter.HasNext() {
				idx := it.curIter.Next()
				examinedDelta++
				matched, residualChecked := it.matchCurrentCandidate(idx)
				if residualChecked {
					residualExaminedDelta++
				}
				if matched {
					it.cur = idx
					it.has = true
					emittedDelta++
					return examinedDelta, residualExaminedDelta, emittedDelta, true
				}
			}
			it.curIter.Release()
			it.curIter = nil
		}

		if it.desc {
			if it.nextBucket < it.startBucket {
				return examinedDelta, residualExaminedDelta, emittedDelta, false
			}
			b := it.nextBucket
			it.nextBucket--
			bucket := it.overlay.PostingAt(b)
			if bucket.IsEmpty() {
				continue
			}
			if it.bucketSeen != nil && !it.bucketSeen[b] {
				it.bucketSeen[b] = true
			}
			it.curBucket = b
			if idx, ok := bucket.TrySingle(); ok {
				examinedDelta++
				matched, residualChecked := it.matchBucketCandidate(idx)
				if residualChecked {
					residualExaminedDelta++
				}
				if !matched {
					continue
				}
				it.cur = idx
				it.has = true
				emittedDelta++
				return examinedDelta, residualExaminedDelta, emittedDelta, true
			}
			if len(it.exactChecks) > 0 {
				mode, exactIDs, nextBucketWork, card := plannerFilterPostingByPredicateChecksBuf(it.branch.preds, it.exactChecks, bucket, it.bucketWork, true)
				it.bucketWork = nextBucketWork
				switch mode {
				case plannerPredicateBucketEmpty:
					examinedDelta += card
					continue
				case plannerPredicateBucketAll:
					if it.allChecksExact {
						examinedDelta += card
						emittedDelta += card
						it.curIter = exactIDs.Iter()
						it.curExact = true
						it.curChecks = nil
						it.curSingle = -1
						it.curResidual = false
						it.curSplitExact = false
						continue
					}
					it.curIter = exactIDs.Iter()
					it.curExact = false
					it.curChecks = it.residualChecks
					it.curSingle = it.residualSingle
					it.curResidual = len(it.residualChecks) > 0
					it.curSplitExact = false
					continue
				case plannerPredicateBucketExact:
					if it.allChecksExact {
						examinedDelta += card
						emittedDelta += exactIDs.Cardinality()
						it.curIter = exactIDs.Iter()
						it.curExact = true
						it.curChecks = nil
						it.curSingle = -1
						it.curResidual = false
						it.curSplitExact = false
						continue
					}
					it.curIter = exactIDs.Iter()
					it.curExact = false
					it.curChecks = it.residualChecks
					it.curSingle = it.residualSingle
					it.curResidual = len(it.residualChecks) > 0
					it.curSplitExact = false
					continue
				}
			}
			it.curIter = bucket.Iter()
			it.curExact = false
			it.curChecks = it.checks
			it.curSingle = it.single
			it.curResidual = len(it.checks) > 0
			it.curSplitExact = len(it.exactChecks) > 0 && len(it.residualChecks) > 0
			continue
		}

		if it.nextBucket >= it.endBucket {
			return examinedDelta, residualExaminedDelta, emittedDelta, false
		}
		b := it.nextBucket
		it.nextBucket++
		bucket := it.overlay.PostingAt(b)
		if bucket.IsEmpty() {
			continue
		}
		if it.bucketSeen != nil && !it.bucketSeen[b] {
			it.bucketSeen[b] = true
		}
		it.curBucket = b
		if idx, ok := bucket.TrySingle(); ok {
			examinedDelta++
			matched, residualChecked := it.matchBucketCandidate(idx)
			if residualChecked {
				residualExaminedDelta++
			}
			if !matched {
				continue
			}
			it.cur = idx
			it.has = true
			emittedDelta++
			return examinedDelta, residualExaminedDelta, emittedDelta, true
		}
		if len(it.exactChecks) > 0 {
			mode, exactIDs, nextBucketWork, card := plannerFilterPostingByPredicateChecksBuf(it.branch.preds, it.exactChecks, bucket, it.bucketWork, true)
			it.bucketWork = nextBucketWork
			switch mode {
			case plannerPredicateBucketEmpty:
				examinedDelta += card
				continue
			case plannerPredicateBucketAll:
				if it.allChecksExact {
					examinedDelta += card
					emittedDelta += card
					it.curIter = exactIDs.Iter()
					it.curExact = true
					it.curChecks = nil
					it.curSingle = -1
					it.curResidual = false
					it.curSplitExact = false
					continue
				}
				it.curIter = exactIDs.Iter()
				it.curExact = false
				it.curChecks = it.residualChecks
				it.curSingle = it.residualSingle
				it.curResidual = len(it.residualChecks) > 0
				it.curSplitExact = false
				continue
			case plannerPredicateBucketExact:
				if it.allChecksExact {
					examinedDelta += card
					emittedDelta += exactIDs.Cardinality()
					it.curIter = exactIDs.Iter()
					it.curExact = true
					it.curChecks = nil
					it.curSingle = -1
					it.curResidual = false
					it.curSplitExact = false
					continue
				}
				it.curIter = exactIDs.Iter()
				it.curExact = false
				it.curChecks = it.residualChecks
				it.curSingle = it.residualSingle
				it.curResidual = len(it.residualChecks) > 0
				it.curSplitExact = false
				continue
			}
		}
		it.curIter = bucket.Iter()
		it.curExact = false
		it.curChecks = it.checks
		it.curSingle = it.single
		it.curResidual = len(it.checks) > 0
		it.curSplitExact = len(it.exactChecks) > 0 && len(it.residualChecks) > 0
	}
}

func (qv *queryView) execPlanOROrderKWay(
	q *qir.Shape,
	branches plannerORBranches,
	analysis *plannerOROrderAnalysis,
	trace *queryTrace,
) ([]uint64, bool, error) {
	needWindow, ok := orderWindow(q)
	if !ok || needWindow <= 0 {
		return nil, false, nil
	}

	o := q.Order
	fm := qv.fieldMetaByOrder(o)
	if fm == nil || fm.Slice {
		return nil, false, nil
	}
	orderView, ok := qv.plannerOrderIndexSnapshotViewByOrdinal(o.FieldOrdinal)
	if !ok {
		return nil, false, nil
	}
	defer orderView.close()
	ov := orderView.overlay
	if !ov.HasData() {
		return nil, true, nil
	}
	orderField := qv.engine.fieldNameByOrdinal(o.FieldOrdinal)

	for i := 0; i < branches.Len(); i++ {
		if branches.Get(i).alwaysTrue {
			return nil, false, nil
		}
	}
	allowRuntimeFallback := false
	runtimeShape := plannerORKWayRuntimeShape{
		overlap:   1,
		avgChecks: 1,
	}
	if guardDec, ok := qv.decideOROrderKWayRuntimeFallbackWithAnalysis(q, branches, needWindow, analysis); ok {
		allowRuntimeFallback = guardDec.enable
		runtimeShape = plannerORKWayRuntimeShapeFromGuard(guardDec, q)
		if trace != nil {
			trace.setOROrderRuntimeGuard(guardDec.enable, guardDec.reason)
		}
	}

	fullTrace := trace.full()
	var (
		branchMetrics       []TraceORBranch
		branchMetricsInline [plannerORBranchLimit]TraceORBranch
		examined            uint64
		scanWidth           uint64
		dedupe              uint64
	)
	if trace != nil {
		if fullTrace {
			branchMetrics = branchMetricsInline[:branches.Len()]
			for i := range branchMetrics {
				branchMetrics[i].Index = i
			}
		}
	}

	var bucketSeen []bool
	if fullTrace {
		bucketSeen = make([]bool, ov.KeyCount())
	}

	var (
		iters         plannerOROrderIterStore
		mergeItemsBuf *pooled.Slice[plannerOROrderMergeItem]
	)
	var (
		observedCheckRows    [plannerORBranchLimit]uint64
		branchUniverses      [plannerORBranchLimit]uint64
		branchObservedChecks [plannerORBranchLimit][]int
	)
	if branches.Len() <= len(iters.inline) {
		defer closePlannerOROrderInlineIters(&iters.inline, branches.Len())
	} else {
		iters.buf = plannerOROrderIterSlicePool.Get()
		iters.buf.SetLen(branches.Len())
		defer plannerOROrderIterSlicePool.Put(iters.buf)
	}
	mergeItemsBuf = plannerOROrderMergeItemSlicePool.Get()
	mergeItemsBuf.Grow(branches.Len())
	defer plannerOROrderMergeItemSlicePool.Put(mergeItemsBuf)

	h := plannerOROrderMergeHeap{
		desc:     o.Desc,
		itemsBuf: mergeItemsBuf,
	}
	snapshotUniverse := qv.snapshotUniverseCardinality()

	for i := 0; i < branches.Len(); i++ {
		branch := branches.GetPtr(i)
		var covered []bool
		branchStart := 0
		branchEnd := ov.KeyCount()
		branchUniverse := snapshotUniverse
		reuseAnalysis := analysis != nil && i < analysis.branchCount
		if reuseAnalysis {
			covered = analysis.branches[i].covered
			branchStart = analysis.branches[i].rangeStart
			branchEnd = analysis.branches[i].rangeEnd
			branchUniverse = analysis.branches[i].universe
		} else {
			br, rangeCovered, rangeOK := qv.extractOrderRangeCoverageOverlayReader(orderField, branch.preds, ov)
			if !rangeOK {
				return nil, false, nil
			}
			covered = rangeCovered
			branchStart, branchEnd = br.BaseStart, br.BaseEnd
			if branchStart != 0 || branchEnd != ov.KeyCount() {
				_, branchUniverse = ov.RangeStats(br)
			}
		}
		if covered != nil {
			for pi := 0; pi < len(covered); pi++ {
				if covered[pi] {
					branch.predPtr(pi).covered = true
				}
			}
			if !reuseAnalysis {
				pooled.ReleaseBoolSlice(covered)
			}
		}
		if branchStart >= branchEnd {
			if fullTrace {
				branchMetrics[i].Skipped = true
				branchMetrics[i].SkipReason = "order_range_empty"
			}
			continue
		}
		branchUniverses[i] = branchUniverse

		checksBuf := pooled.GetIntSlice(branch.predLen())
		checksBuf = branch.buildMatchChecksBuf(checksBuf)
		exactChecksBuf := pooled.GetIntSlice(len(checksBuf))
		exactChecksBuf = buildExactBucketPostingFilterActiveBufReader(exactChecksBuf, checksBuf, branch.preds)
		residualChecksBuf := pooled.GetIntSlice(len(checksBuf))
		residualChecksBuf = plannerResidualChecksBuf(residualChecksBuf, checksBuf, exactChecksBuf)
		if len(residualChecksBuf) > 0 {
			branchObservedChecks[i] = residualChecksBuf
		} else if len(exactChecksBuf) > 0 {
			branchObservedChecks[i] = exactChecksBuf
		}
		singleCheck := -1
		if len(checksBuf) == 1 {
			singleCheck = checksBuf[0]
		}
		exactSingle := -1
		if len(exactChecksBuf) == 1 {
			exactSingle = exactChecksBuf[0]
		}
		residualSingle := -1
		if len(residualChecksBuf) == 1 {
			residualSingle = residualChecksBuf[0]
		}
		iter := plannerOROrderBranchIter{
			branch:         branch,
			checks:         checksBuf,
			exactChecks:    exactChecksBuf,
			residualChecks: residualChecksBuf,
			overlay:        ov,
			desc:           o.Desc,
			single:         singleCheck,
			exactSingle:    exactSingle,
			residualSingle: residualSingle,
			allChecksExact: len(checksBuf) > 0 && len(exactChecksBuf) == len(checksBuf),
			startBucket:    branchStart,
			endBucket:      branchEnd,
			bucketSeen:     bucketSeen,
		}
		iter.init()
		examinedDelta, residualExaminedDelta, emittedDelta, ok := iter.advance()
		examined += examinedDelta
		observedDelta := residualExaminedDelta
		if len(residualChecksBuf) == 0 && len(exactChecksBuf) > 0 {
			observedDelta = examinedDelta
		}
		observedCheckRows[i] = satAddUint64(observedCheckRows[i], observedDelta)
		if fullTrace {
			branchMetrics[i].RowsExamined += examinedDelta
			branchMetrics[i].RowsEmitted += emittedDelta
		}
		if ok {
			iters.set(i, iter)
			h.push(plannerOROrderMergeItem{
				branch: i,
				bucket: iter.curBucket,
				idx:    iter.cur,
			})
		} else if fullTrace {
			iters.set(i, iter)
			branchMetrics[i].Skipped = true
			branchMetrics[i].SkipReason = "stream_empty"
		} else {
			iters.set(i, iter)
		}
	}

	if h.len() == 0 {
		qv.promoteObservedOrderedORKWayMaterializedBaseOps(q, branches, branchObservedChecks, &observedCheckRows, &branchUniverses, analysis)
		if trace != nil {
			if fullTrace {
				for i := range bucketSeen {
					if bucketSeen[i] {
						scanWidth++
					}
				}
			}
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
	out := make([]uint64, 0, outCap)

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

		iter := iters.get(bi)
		examinedDelta, residualExaminedDelta, emittedDelta, ok := iter.advance()
		examined += examinedDelta
		observedDelta := residualExaminedDelta
		if len(iter.residualChecks) == 0 && len(iter.exactChecks) > 0 {
			observedDelta = examinedDelta
		}
		observedCheckRows[bi] = satAddUint64(observedCheckRows[bi], observedDelta)
		if fullTrace {
			branchMetrics[bi].RowsExamined += examinedDelta
			branchMetrics[bi].RowsEmitted += emittedDelta
		}
		if ok {
			iters.set(bi, iter)
			h.push(plannerOROrderMergeItem{
				branch: bi,
				bucket: iter.curBucket,
				idx:    iter.cur,
			})
		} else {
			iters.set(bi, iter)
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
					qv.promoteObservedOrderedORKWayMaterializedBaseOps(q, branches, branchObservedChecks, &observedCheckRows, &branchUniverses, analysis)
					return nil, false, nil
				}
			}
			continue
		}

		out = append(out, item.idx)
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
				qv.promoteObservedOrderedORKWayMaterializedBaseOps(q, branches, branchObservedChecks, &observedCheckRows, &branchUniverses, analysis)
				return nil, false, nil
			}
		}
	}

	qv.promoteObservedOrderedORKWayMaterializedBaseOps(q, branches, branchObservedChecks, &observedCheckRows, &branchUniverses, analysis)
	if trace != nil {
		if fullTrace {
			for i := range bucketSeen {
				if bucketSeen[i] {
					scanWidth++
				}
			}
		}
		trace.addExamined(examined)
		trace.addDedupe(dedupe)
		trace.addOrderScanWidth(scanWidth)
		trace.setORBranches(branchMetrics)
		trace.setEarlyStopReason(stopReason)
	}
	return out, true, nil
}

func (qv *queryView) execPlanOROrderMergeFallback(q *qir.Shape, branches plannerORBranches, trace *queryTrace) ([]uint64, bool, error) {
	need, ok := orderWindow(q)
	if !ok || need <= 0 {
		return nil, false, nil
	}

	candidateSet := newPostingLazySetBuilder(satMulUint64(uint64(need), uint64(branches.Len())))

	fullTrace := trace.full()
	var branchMetrics []TraceORBranch
	var branchMetricsInline [plannerORBranchLimit]TraceORBranch
	if fullTrace {
		branchMetrics = branchMetricsInline[:branches.Len()]
		for i := range branchMetrics {
			branchMetrics[i].Index = i
		}
	}

	order := q.Order
	directBranchCollect := false
	var orderOV indexdata.FieldOverlay
	if order.FieldOrdinal >= 0 {
		orderOV = qv.fieldOverlayForOrder(order)
		directBranchCollect = orderOV.HasData()
	}

	for i := 0; i < branches.Len(); i++ {
		branch := branches.Get(i)
		if branch.alwaysTrue {
			candidateSet.release()
			return nil, false, nil
		}

		branchLimit, branchExhaustive, err := qv.planOROrderMergeBranchLimit(branch, need)
		if err != nil {
			candidateSet.release()
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
				branches.GetPtr(i), q.Order, branchLimit, orderOV, &candidateSet,
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

		subQ := q.WithExpr(branch.expr).WithWindow(0, uint64(branchLimit))
		if branchExhaustive {
			subQ = subQ.WithoutOrder()
		}

		ids, err := qv.execPreparedQuery(&subQ)
		if err != nil {
			candidateSet.release()
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

		dedupe := uint64(0)
		for _, idx := range ids {
			if !candidateSet.addChecked(idx) {
				dedupe++
			}
		}
		if dedupe > 0 && trace != nil {
			trace.addDedupe(dedupe)
		}
	}

	candidateIDs := candidateSet.finish(false)

	if candidateIDs.IsEmpty() {
		if trace != nil {
			trace.setORBranches(branchMetrics)
			trace.setEarlyStopReason("no_candidates")
		}
		return nil, true, nil
	}

	o := q.Order
	fm := qv.fieldMetaByOrder(o)
	if fm == nil || fm.Slice {
		candidateIDs.Release()
		return nil, false, nil
	}
	orderView, ok := qv.plannerOrderIndexSnapshotViewByOrdinal(o.FieldOrdinal)
	if !ok {
		candidateIDs.Release()
		return nil, false, nil
	}
	ov := orderView.overlay
	if !ov.HasData() {
		orderView.close()
		candidateIDs.Release()
		return nil, true, nil
	}

	skip := q.Offset
	needOut := q.Limit
	if needOut == 0 {
		orderView.close()
		candidateIDs.Release()
		return nil, true, nil
	}

	totalCandidates := candidateIDs.Cardinality()
	out := make([]uint64, 0, int(min(needOut, totalCandidates)))

	seenCandidates := uint64(0)
	scanWidth := uint64(0)

	cur := ov.NewCursor(ov.RangeForBounds((indexdata.Bounds{Has: true})), o.Desc)
	for {
		_, bm, ok := cur.Next()
		if !ok {
			break
		}
		if bm.IsEmpty() || !bm.Intersects(candidateIDs) {
			continue
		}
		scanWidth++
		if bm.Cardinality() == 1 {
			idx, _ := bm.Minimum()
			if !candidateIDs.Contains(idx) {
				continue
			}
			seenCandidates++
			if skip > 0 {
				skip--
			} else {
				out = append(out, idx)
				needOut--
				if needOut == 0 {
					setOROrderMergeFallbackTrace(trace, seenCandidates, scanWidth, branchMetrics, "limit_reached")
					orderView.close()
					candidateIDs.Release()
					return out, true, nil
				}
			}
			if seenCandidates == totalCandidates {
				break
			}
			continue
		}

		it := bm.Iter()
		for needOut > 0 && it.HasNext() {
			idx := it.Next()
			if !candidateIDs.Contains(idx) {
				continue
			}
			seenCandidates++
			if skip > 0 {
				skip--
				continue
			}
			out = append(out, idx)
			needOut--
		}
		it.Release()
		if needOut == 0 {
			setOROrderMergeFallbackTrace(trace, seenCandidates, scanWidth, branchMetrics, "limit_reached")
			orderView.close()
			candidateIDs.Release()
			return out, true, nil
		}
		if seenCandidates == totalCandidates {
			break
		}
	}

	stopReason := "order_index_exhausted"
	if seenCandidates == totalCandidates {
		stopReason = "candidates_exhausted"
	}
	setOROrderMergeFallbackTrace(trace, seenCandidates, scanWidth, branchMetrics, stopReason)
	orderView.close()
	candidateIDs.Release()
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

type plannerOROrderFallbackAccumulatorBuf struct {
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
		if !c.branch.predPtr(c.singleCheck).matches(idx) {
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
		if !c.branch.predPtr(c.residualSingle).matches(idx) {
			return false
		}
	} else if !c.branch.matchesChecks(idx, c.residualChecks) {
		return false
	}
	return c.add(idx)
}

func (c *plannerOROrderFallbackAccumulatorBuf) add(idx uint64) bool {
	c.emitted++
	if !c.dst.addChecked(idx) {
		c.dedupe++
	}
	return c.emitted >= c.limit
}

func (c *plannerOROrderFallbackAccumulatorBuf) emitPostingNoChecks(ids posting.List, card uint64) bool {
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

func (c *plannerOROrderFallbackAccumulatorBuf) emitPostingChecked(ids posting.List) bool {
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

func (c *plannerOROrderFallbackAccumulatorBuf) emit(idx uint64) bool {
	c.examined++
	if c.singleCheck >= 0 {
		if !c.branch.predPtr(c.singleCheck).matches(idx) {
			return false
		}
	} else if !c.branch.matchesChecksBuf(idx, c.checks) {
		return false
	}
	return c.add(idx)
}

func (c *plannerOROrderFallbackAccumulatorBuf) emitExact(idx uint64) bool {
	c.examined++
	if c.residualSingle >= 0 {
		if !c.branch.predPtr(c.residualSingle).matches(idx) {
			return false
		}
	} else if !c.branch.matchesChecksBuf(idx, c.residualChecks) {
		return false
	}
	return c.add(idx)
}

func (qv *queryView) collectOROrderFallbackBranchCandidatesWithChecks(
	branch *plannerORBranch,
	branchLimit int,
	ov indexdata.FieldOverlay,
	desc bool,
	start, end int,
	dst *postingLazySetBuilder,
	checks, exactChecks, residualChecks []int,
) (uint64, uint64, uint64) {
	singleCheck := -1
	if len(checks) == 1 {
		singleCheck = checks[0]
	}
	residualSingle := -1
	if len(residualChecks) == 1 {
		residualSingle = residualChecks[0]
	}
	var bucketWork posting.List
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

	cur := ov.NewCursor(ov.RangeByRanks(start, end), desc)
	for {
		_, bucket, ok := cur.Next()
		if !ok {
			break
		}
		if bucket.IsEmpty() {
			continue
		}
		if idx, ok := bucket.TrySingle(); ok {
			if acc.emit(idx) {
				break
			}
			continue
		}
		if len(exactChecks) > 0 {
			mode, exactIDs, nextBucketWork, card := plannerFilterPostingByPredicateChecks(branch.preds, exactChecks, bucket, bucketWork, true)
			bucketWork = nextBucketWork
			switch mode {
			case plannerPredicateBucketEmpty:
				acc.examined += card
				continue
			case plannerPredicateBucketAll, plannerPredicateBucketExact:
				if allChecksExact {
					if acc.emitPostingNoChecks(exactIDs, card) {
						bucketWork.Release()
						return acc.emitted, acc.examined, acc.dedupe
					}
					continue
				}
				if acc.emitPostingChecked(exactIDs) {
					bucketWork.Release()
					return acc.emitted, acc.examined, acc.dedupe
				}
				continue
			}
		}
		it := bucket.Iter()
		for it.HasNext() {
			if acc.emit(it.Next()) {
				it.Release()
				bucketWork.Release()
				return acc.emitted, acc.examined, acc.dedupe
			}
		}
		it.Release()
	}
	bucketWork.Release()
	return acc.emitted, acc.examined, acc.dedupe
}

func (qv *queryView) collectOROrderFallbackBranchCandidatesWithChecksBuf(
	branch *plannerORBranch,
	branchLimit int,
	ov indexdata.FieldOverlay,
	desc bool,
	start, end int,
	dst *postingLazySetBuilder,
	checks, exactChecks, residualChecks []int,
) (uint64, uint64, uint64) {
	singleCheck := -1
	if len(checks) == 1 {
		singleCheck = checks[0]
	}
	residualSingle := -1
	if len(residualChecks) == 1 {
		residualSingle = residualChecks[0]
	}
	var bucketWork posting.List
	allChecksExact := len(checks) > 0 && len(exactChecks) == len(checks)
	acc := plannerOROrderFallbackAccumulatorBuf{
		branch:         branch,
		dst:            dst,
		checks:         checks,
		residualChecks: residualChecks,
		singleCheck:    singleCheck,
		residualSingle: residualSingle,
		limit:          uint64(branchLimit),
	}

	cur := ov.NewCursor(ov.RangeByRanks(start, end), desc)
	for {
		_, bucket, ok := cur.Next()
		if !ok {
			break
		}
		if bucket.IsEmpty() {
			continue
		}
		if idx, ok := bucket.TrySingle(); ok {
			if acc.emit(idx) {
				break
			}
			continue
		}
		if len(exactChecks) > 0 {
			mode, exactIDs, nextBucketWork, card := plannerFilterPostingByPredicateChecksBuf(branch.preds, exactChecks, bucket, bucketWork, true)
			bucketWork = nextBucketWork
			switch mode {
			case plannerPredicateBucketEmpty:
				acc.examined += card
				continue
			case plannerPredicateBucketAll, plannerPredicateBucketExact:
				if allChecksExact {
					if acc.emitPostingNoChecks(exactIDs, card) {
						bucketWork.Release()
						return acc.emitted, acc.examined, acc.dedupe
					}
					continue
				}
				if acc.emitPostingChecked(exactIDs) {
					bucketWork.Release()
					return acc.emitted, acc.examined, acc.dedupe
				}
				continue
			}
		}
		it := bucket.Iter()
		for it.HasNext() {
			if acc.emit(it.Next()) {
				it.Release()
				bucketWork.Release()
				return acc.emitted, acc.examined, acc.dedupe
			}
		}
		it.Release()
	}
	bucketWork.Release()
	return acc.emitted, acc.examined, acc.dedupe
}

// collectOROrderFallbackBranchCandidates collects up to branchLimit matching ids
// from one OR branch directly from the base order index and writes them into dst.
// Returns ok=false when fast-path preconditions are not satisfied.
func (qv *queryView) collectOROrderFallbackBranchCandidates(
	branch *plannerORBranch,
	order qir.Order,
	branchLimit int,
	ov indexdata.FieldOverlay,
	dst *postingLazySetBuilder,
) (uint64, uint64, uint64, bool) {
	if branchLimit <= 0 {
		return 0, 0, 0, false
	}
	if order.Kind != qir.OrderKindBasic || order.FieldOrdinal < 0 {
		return 0, 0, 0, false
	}
	fieldName := qv.engine.fieldNameByOrdinal(order.FieldOrdinal)
	fm := qv.fieldMetaByOrder(order)
	if fm == nil || fm.Slice {
		return 0, 0, 0, false
	}
	if !ov.HasData() {
		ov = qv.fieldOverlay(fieldName)
	}
	if !ov.HasData() {
		return 0, 0, 0, false
	}
	br, covered, rangeOK := qv.extractOrderRangeCoverageOverlayReader(fieldName, branch.preds, ov)
	if !rangeOK {
		return 0, 0, 0, false
	}
	for pi := 0; pi < len(covered); pi++ {
		if covered[pi] {
			branch.predPtr(pi).covered = true
		}
	}
	pooled.ReleaseBoolSlice(covered)
	start, end := br.BaseStart, br.BaseEnd
	if start >= end {
		return 0, 0, 0, true
	}

	if branch.predLen() <= plannerPredicateFastPathMaxLeaves {
		var checksInline [plannerPredicateFastPathMaxLeaves]int
		var exactChecksInline [plannerPredicateFastPathMaxLeaves]int
		var residualChecksInline [plannerPredicateFastPathMaxLeaves]int
		checks := branch.buildMatchChecks(checksInline[:0])
		exactChecks := branch.buildPostingFilterChecks(exactChecksInline[:0], checks)
		residualChecks := plannerResidualChecks(residualChecksInline[:0], checks, exactChecks)
		emitted, examined, dedupe := qv.collectOROrderFallbackBranchCandidatesWithChecks(
			branch, branchLimit, ov, order.Desc, start, end, dst, checks, exactChecks, residualChecks,
		)
		return emitted, examined, dedupe, true
	}

	checksBuf := pooled.GetIntSlice(branch.predLen())
	checksBuf = branch.buildMatchChecksBuf(checksBuf)
	exactChecksBuf := pooled.GetIntSlice(len(checksBuf))
	exactChecksBuf = buildExactBucketPostingFilterActiveBufReader(exactChecksBuf, checksBuf, branch.preds)
	residualChecksBuf := pooled.GetIntSlice(len(checksBuf))
	residualChecksBuf = plannerResidualChecksBuf(residualChecksBuf, checksBuf, exactChecksBuf)
	emitted, examined, dedupe := qv.collectOROrderFallbackBranchCandidatesWithChecksBuf(
		branch, branchLimit, ov, order.Desc, start, end, dst, checksBuf, exactChecksBuf, residualChecksBuf,
	)
	pooled.ReleaseIntSlice(residualChecksBuf)
	pooled.ReleaseIntSlice(exactChecksBuf)
	pooled.ReleaseIntSlice(checksBuf)
	return emitted, examined, dedupe, true
}

func (qv *queryView) planOROrderMergeBranchLimit(branch plannerORBranch, need int) (limit int, exhaustive bool, err error) {
	if need <= 0 {
		return 0, true, nil
	}

	// Default safe limit: full window size.
	limit = need
	if !plannerOROrderMergePrecountWorth(branch, need) {
		return limit, false, nil
	}

	if branch.predLen() == 0 || branch.predLen() > 4 {
		return limit, false, nil
	}

	// Keep branch pre-counting conservative:
	// for broad/range/string scans it is usually not worth extra Count().
	for i := 0; i < branch.predLen(); i++ {
		e := branch.pred(i).expr
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
	cnt, err = qv.aggregateCountPreparedExpr(branch.expr)
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
	if need <= 0 || !branch.hasLead() || branch.leadPred().estCard == 0 {
		return false
	}

	checks := float64(branch.containsChecks())
	needF := float64(need)
	leadEst := float64(branch.leadPred().estCard)
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

func (qv *queryView) canPrecountORBranchExpr(e qir.Expr) bool {
	if !e.Op.IsMaterializedScalarCache() {
		return true
	}
	cacheKey := qv.materializedPredKey(e)
	if cacheKey.IsZero() {
		return false
	}
	_, ok := qv.snap.LoadMaterializedPredKey(cacheKey)
	return ok
}

func (qv *queryView) countORBranchByUniqueLeadWithChecks(branch plannerORBranch, leadIdx int, checks []int) (uint64, bool) {
	lead := branch.pred(leadIdx)
	it := lead.newIter()
	if it == nil {
		return 0, false
	}
	defer it.Release()

	var cnt uint64
	for it.HasNext() {
		idx := it.Next()
		pass := true
		for _, ci := range checks {
			if !branch.predPtr(ci).matches(idx) {
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

func (qv *queryView) countORBranchByUniqueLeadWithChecksBuf(branch plannerORBranch, leadIdx int, checks []int) (uint64, bool) {
	lead := branch.pred(leadIdx)
	it := lead.newIter()
	if it == nil {
		return 0, false
	}
	defer it.Release()

	var cnt uint64
	for it.HasNext() {
		idx := it.Next()
		if branch.matchesChecksBuf(idx, checks) {
			cnt++
		}
	}
	return cnt, true
}

// countORBranchByUniqueLead tries to count one OR branch by scanning a unique
// EQ predicate lead and validating the remaining predicates with contains().
func (qv *queryView) countORBranchByUniqueLead(branch plannerORBranch) (uint64, bool) {
	if branch.predLen() == 0 {
		return 0, false
	}

	leadIdx := -1
	leadEst := uint64(0)
	for i := 0; i < branch.predLen(); i++ {
		p := branch.pred(i)
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
	if branch.predLen() <= plannerPredicateFastPathMaxLeaves {
		var checksInline [plannerPredicateFastPathMaxLeaves]int
		checks := checksInline[:0]
		for i := 0; i < branch.predLen(); i++ {
			if i == leadIdx {
				continue
			}
			p := branch.pred(i)
			if p.covered || p.alwaysTrue {
				continue
			}
			if p.alwaysFalse || !p.hasContains() {
				return 0, true
			}
			checks = append(checks, i)
		}
		sortActivePredicatesReader(checks, branch.preds)
		return qv.countORBranchByUniqueLeadWithChecks(branch, leadIdx, checks)
	}

	checksBuf := pooled.GetIntSlice(branch.predLen())
	for i := 0; i < branch.predLen(); i++ {
		if i == leadIdx {
			continue
		}
		p := branch.pred(i)
		if p.covered || p.alwaysTrue {
			continue
		}
		if p.alwaysFalse || !p.hasContains() {
			pooled.ReleaseIntSlice(checksBuf)
			return 0, true
		}
		checksBuf = append(checksBuf, i)
	}
	sortActivePredicatesBufReader(checksBuf, branch.preds)
	cnt, ok := qv.countORBranchByUniqueLeadWithChecksBuf(branch, leadIdx, checksBuf)
	pooled.ReleaseIntSlice(checksBuf)
	return cnt, ok
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
	for i := 0; i < branches.Len(); i++ {
		branch := branches.Get(i)
		if branch.alwaysTrue {
			return plannerORNoOrderModeUniverse
		}
		lead := branch.leadPtr()
		if lead == nil || !lead.hasIter() {
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

func plannerORNoOrderNextBudget(cur uint64) uint64 {
	if cur < plannerORNoOrderMinBranchBudget {
		return plannerORNoOrderMinBranchBudget
	}
	n := cur * 2
	if n > plannerORNoOrderMaxBranchBudget {
		n = plannerORNoOrderMaxBranchBudget
	}
	return n
}

func plannerORNoOrderProbeBranch(st *plannerORNoOrderBranchState) (uint64, uint64) {
	if st.exhausted || st.iter.has {
		return 0, 0
	}
	st.initialized = true
	examinedDelta, emittedDelta, budgetHit := st.iter.advanceWithBudget(st.budget)
	if st.iter.has {
		st.capped = false
		return examinedDelta, emittedDelta
	}
	if budgetHit {
		st.capped = true
		st.budget = plannerORNoOrderNextBudget(st.budget)
		return examinedDelta, emittedDelta
	}
	st.capped = false
	st.exhausted = true
	return examinedDelta, emittedDelta
}

func plannerORNoOrderConsumeBranch(st *plannerORNoOrderBranchState) (uint64, uint64) {
	if !st.iter.has {
		return 0, 0
	}
	examinedDelta, emittedDelta, budgetHit := st.iter.advanceWithBudget(st.budget)
	if st.iter.has {
		st.capped = false
		return examinedDelta, emittedDelta
	}
	if budgetHit {
		st.capped = true
		st.budget = plannerORNoOrderNextBudget(st.budget)
		return examinedDelta, emittedDelta
	}
	st.capped = false
	st.exhausted = true
	return examinedDelta, emittedDelta
}

func plannerORNoOrderFindReadyMin(states []plannerORNoOrderBranchState, maxInclusive uint64, useMax bool) *plannerORNoOrderBranchState {
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

func plannerORNoOrderFindNextUninitialized(order []int, states []plannerORNoOrderBranchState) *plannerORNoOrderBranchState {
	for _, idx := range order {
		st := &states[idx]
		if st.initialized || st.exhausted {
			continue
		}
		return st
	}
	return nil
}

func plannerORNoOrderFindNextCapped(order []int, states []plannerORNoOrderBranchState) *plannerORNoOrderBranchState {
	for _, idx := range order {
		st := &states[idx]
		if st.exhausted || !st.capped || st.iter.has {
			continue
		}
		return st
	}
	return nil
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
		copy(top[pos+1:], top[pos:len(top)-1])
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

func plannerORNoOrderAddCandidate(top []uint64, idx uint64, need int, useLinearSeen bool, seen *u64set, trace *queryTrace) []uint64 {
	// Threshold prune prevents growing a candidate set that can no longer
	// improve top-N; it is key for large OR unions with small LIMIT.
	if len(top) == need && idx > top[len(top)-1] {
		return top
	}

	if useLinearSeen {
		for i := 0; i < len(top); i++ {
			if top[i] == idx {
				if trace != nil {
					trace.addDedupe(1)
				}
				return top
			}
		}
	} else if !seen.Add(idx) {
		if trace != nil {
			trace.addDedupe(1)
		}
		return top
	}

	return plannerORNoOrderInsertTopN(top, idx, need)
}

func (qv *queryView) execPlanORNoOrder(q *qir.Shape, branches plannerORBranches, trace *queryTrace) ([]uint64, bool) {
	switch plannerORNoOrderClassify(branches) {
	case plannerORNoOrderModeUniverse:
		return qv.execPlanORUniverseNoOrder(q, trace), true
	case plannerORNoOrderModeUnsupported:
		return nil, false
	}

	if q.Offset == 0 && branches.Len() >= 3 {
		if out, ok := qv.execPlanORNoOrderAdaptiveCore(q, branches, trace); ok {
			return out, true
		}
	}
	return qv.execPlanORNoOrderBaselineCore(q, branches, trace)
}

// execPlanORNoOrderAdaptive collects top-N ids for no-order OR via adaptive per-branch probing.
//
// It starts with cheap branches and increases probe budgets only when needed to prove top-N completeness.
func (qv *queryView) execPlanORNoOrderAdaptive(q *qir.Shape, branches plannerORBranches, trace *queryTrace) ([]uint64, bool) {
	switch plannerORNoOrderClassify(branches) {
	case plannerORNoOrderModeUniverse:
		return qv.execPlanORUniverseNoOrder(q, trace), true
	case plannerORNoOrderModeUnsupported:
		return nil, false
	}
	return qv.execPlanORNoOrderAdaptiveCore(q, branches, trace)
}

func (qv *queryView) execPlanORNoOrderAdaptiveCore(q *qir.Shape, branches plannerORBranches, trace *queryTrace) ([]uint64, bool) {
	if q.Limit == 0 || q.Offset != 0 {
		return nil, false
	}

	need := int(q.Limit)
	if need <= 0 {
		return nil, true
	}

	qv.maybeEagerMaterializeNoOrderORPredicates(q, branches)

	fullTrace := trace.full()
	examined := uint64(0)
	var branchMetrics []TraceORBranch
	var branchMetricsInline [plannerORBranchLimit]TraceORBranch
	if trace != nil {
		if fullTrace {
			branchMetrics = branchMetricsInline[:branches.Len()]
			for i := range branchMetrics {
				branchMetrics[i].Index = i
			}
		}
	}

	var statesInline [plannerORBranchLimit]plannerORNoOrderBranchState
	states := statesInline[:branches.Len()]
	for i := range states {
		states[i] = plannerORNoOrderBranchState{}
	}

	var orderInline [plannerORBranchLimit]int
	order := orderInline[:branches.Len()]
	for i := 0; i < branches.Len(); i++ {
		branch := branches.Get(i)
		state := &states[i]
		state.index = i
		state.score = branch.noOrderScore()
		state.budget = branch.noOrderBudget(need)
		order[i] = i
		lead := branch.leadPtr()
		state.iter = plannerORIter{
			it:     lead.newIter(),
			branch: branches.GetPtr(i),
		}
	}
	defer releasePlannerORNoOrderStateIters(states)

	plannerORNoOrderSortBranchOrder(order, states)

	useLinearSeen := need <= plannerORNoOrderLinearSeenNeedMax
	seen := u64set{}
	if !useLinearSeen {
		seen = newU64Set(max(64, need*2))
		defer releaseU64Set(&seen)
	}
	topBuf := pooled.GetUint64Slice(need)
	defer func() { pooled.ReleaseUint64Slice(topBuf) }()

	initialActive := plannerORNoOrderInitActiveBranches
	if initialActive > len(order) {
		initialActive = len(order)
	}
	for i := 0; i < initialActive; i++ {
		st := &states[order[i]]
		examinedDelta, emittedDelta := plannerORNoOrderProbeBranch(st)
		examined += examinedDelta
		if fullTrace {
			branchMetrics[st.index].RowsExamined += examinedDelta
			branchMetrics[st.index].RowsEmitted += emittedDelta
		}
	}

	// Phase 1: gather enough unique candidates from cheap-first branches.
	for len(topBuf) < need {
		st := plannerORNoOrderFindReadyMin(states, 0, false)
		if st != nil {
			topBuf = plannerORNoOrderAddCandidate(topBuf, st.iter.cur, need, useLinearSeen, &seen, trace)
			examinedDelta, emittedDelta := plannerORNoOrderConsumeBranch(st)
			examined += examinedDelta
			if fullTrace {
				branchMetrics[st.index].RowsExamined += examinedDelta
				branchMetrics[st.index].RowsEmitted += emittedDelta
			}
			continue
		}

		if st = plannerORNoOrderFindNextUninitialized(order, states); st != nil {
			examinedDelta, emittedDelta := plannerORNoOrderProbeBranch(st)
			examined += examinedDelta
			if fullTrace {
				branchMetrics[st.index].RowsExamined += examinedDelta
				branchMetrics[st.index].RowsEmitted += emittedDelta
			}
			continue
		}
		if st = plannerORNoOrderFindNextCapped(order, states); st != nil {
			examinedDelta, emittedDelta := plannerORNoOrderProbeBranch(st)
			examined += examinedDelta
			if fullTrace {
				branchMetrics[st.index].RowsExamined += examinedDelta
				branchMetrics[st.index].RowsEmitted += emittedDelta
			}
			continue
		}
		break
	}

	earlyStopReason := "input_exhausted"

	// Phase 2: if LIMIT is already filled, prove all unseen branches cannot
	// contribute ids <= current threshold.
	for len(topBuf) == need {
		threshold := topBuf[len(topBuf)-1]

		st := plannerORNoOrderFindReadyMin(states, threshold, true)
		if st != nil {
			topBuf = plannerORNoOrderAddCandidate(topBuf, st.iter.cur, need, useLinearSeen, &seen, trace)
			examinedDelta, emittedDelta := plannerORNoOrderConsumeBranch(st)
			examined += examinedDelta
			if fullTrace {
				branchMetrics[st.index].RowsExamined += examinedDelta
				branchMetrics[st.index].RowsEmitted += emittedDelta
			}
			continue
		}

		if st = plannerORNoOrderFindNextUninitialized(order, states); st != nil {
			examinedDelta, emittedDelta := plannerORNoOrderProbeBranch(st)
			examined += examinedDelta
			if fullTrace {
				branchMetrics[st.index].RowsExamined += examinedDelta
				branchMetrics[st.index].RowsEmitted += emittedDelta
			}
			continue
		}
		if st = plannerORNoOrderFindNextCapped(order, states); st != nil {
			examinedDelta, emittedDelta := plannerORNoOrderProbeBranch(st)
			examined += examinedDelta
			if fullTrace {
				branchMetrics[st.index].RowsExamined += examinedDelta
				branchMetrics[st.index].RowsEmitted += emittedDelta
			}
			continue
		}

		earlyStopReason = "limit_reached"
		break
	}

	if trace != nil {
		trace.addExamined(examined)

		if fullTrace && len(topBuf) == need {
			threshold := topBuf[len(topBuf)-1]
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

	if len(topBuf) == 0 {
		return nil, true
	}

	out := make([]uint64, len(topBuf))
	copy(out, topBuf)
	return out, true
}

func (qv *queryView) execPlanORNoOrderBaseline(q *qir.Shape, branches plannerORBranches, trace *queryTrace) ([]uint64, bool) {
	switch plannerORNoOrderClassify(branches) {
	case plannerORNoOrderModeUniverse:
		return qv.execPlanORUniverseNoOrder(q, trace), true
	case plannerORNoOrderModeUnsupported:
		return nil, false
	}
	return qv.execPlanORNoOrderBaselineCore(q, branches, trace)
}

func (qv *queryView) execPlanORNoOrderBaselineCore(q *qir.Shape, branches plannerORBranches, trace *queryTrace) ([]uint64, bool) {
	qv.maybeEagerMaterializeNoOrderORPredicates(q, branches)

	fullTrace := trace.full()
	examined := uint64(0)
	var branchMetrics []TraceORBranch
	var branchMetricsInline [plannerORBranchLimit]TraceORBranch
	if trace != nil {
		if fullTrace {
			branchMetrics = branchMetricsInline[:branches.Len()]
			for i := range branchMetrics {
				branchMetrics[i].Index = i
			}
		}
	}
	var itersInline [plannerORBranchLimit]plannerORIter
	iters := itersInline[:0]
	for i := 0; i < branches.Len(); i++ {
		branch := branches.Get(i)
		lead := branch.leadPtr()
		iters = append(iters, plannerORIter{
			it:     lead.newIter(),
			branch: branches.GetPtr(i),
		})
	}
	defer releasePlannerORIters(iters)
	for i := range iters {
		examinedDelta, emittedDelta := iters[i].advance()
		examined += examinedDelta
		if fullTrace {
			branchMetrics[i].RowsExamined += examinedDelta
			branchMetrics[i].RowsEmitted += emittedDelta
		}
	}

	skip := q.Offset
	need := int(q.Limit)
	out := make([]uint64, 0, need)
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
		seenBuf []uint64
	)
	if useLinearSeen {
		seenBuf = pooled.GetUint64Slice(needWindow)
		defer func() { pooled.ReleaseUint64Slice(seenBuf) }()
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
				examinedDelta, emittedDelta := iters[i].advance()
				examined += examinedDelta
				if fullTrace {
					branchMetrics[i].RowsExamined += examinedDelta
					branchMetrics[i].RowsEmitted += emittedDelta
				}
			}
		}
		if trace != nil && dupHits > 1 {
			trace.addDedupe(dupHits - 1)
		}

		if useLinearSeen {
			dup := false
			for i := 0; i < len(seenBuf); i++ {
				if seenBuf[i] == minIdx {
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
			seenBuf = append(seenBuf, minIdx)
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

		out = append(out, minIdx)
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

func (qv *queryView) execPlanORUniverseNoOrder(q *qir.Shape, trace *queryTrace) []uint64 {
	skip := q.Offset
	need := int(q.Limit)
	out := make([]uint64, 0, need)
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
		out = append(out, idx)
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
