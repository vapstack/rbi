package qexec

import (
	"fmt"
	"math"
	"time"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/schema"
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

	plannerORNoOrderMinBranchBudget   = 64
	plannerORNoOrderMaxBranchBudget   = 65_536
	plannerORNoOrderLinearSeenNeedMax = 8

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
	plannerORKWayRuntimeDuplicateRateMin     = 0.45
	plannerORKWayRuntimeBranchImbalanceMin   = 3.0

	plannerORKWayRuntimeLowOverlapNeedMin           = 8_192
	plannerORKWayRuntimeLowOverlapMinPops           = 256
	plannerORKWayRuntimeLowOverlapMinUniquePerPop   = 0.90
	plannerORKWayRuntimeLowOverlapProjectedPopsMax  = 200_000.0
	plannerORKWayRuntimeLowOverlapExaminedPerUnique = 64.0

	plannerOROrderStreamSampleMaxBuckets = 64
	plannerOROrderStreamSampleMaxRows    = 2048
	plannerOROrderStreamSampleMinRows    = 128
	plannerOROrderStreamSampleMaxMatches = 32
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
	owner []plannerORBranch
}

func newPlannerORBranches(capHint int) plannerORBranches {
	owner := plannerORBranchSlicePool.Get(capHint)
	return plannerORBranches{owner: owner}
}

func (branches plannerORBranches) Len() int {
	if branches.owner == nil {
		return 0
	}
	return len(branches.owner)
}

func (branches *plannerORBranches) Append(br plannerORBranch) {
	if branches.owner == nil {
		branches.owner = plannerORBranchSlicePool.Get(1)
	}
	branches.owner = append(branches.owner, br)
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
	if preds == nil || len(preds.owner) <= 1 {
		return
	}

	// predicate lists are tiny in practice; insertion sort avoids allocations
	// and keeps hot-path overhead minimal

	for i := 1; i < len(preds.owner); i++ {
		cur := preds.owner[i]
		j := i
		for j > 0 && plannerORPredicateLess(cur, preds.owner[j-1]) {
			preds.owner[j] = preds.owner[j-1]
			j--
		}
		preds.owner[j] = cur
	}
}

func buildPlannerORBranch(op qir.Expr, preds predicateSet) (plannerORBranch, bool, bool) {
	plannerORSortPredicates(&preds)

	branch := newPlannerORBranch(op, preds)

	hasFalse := false
	allTrue := true
	leadIdx := -1

	for i := 0; i < len(preds.owner); i++ {
		p := &preds.owner[i]
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
			if leadIdx == -1 || p.estCard < preds.owner[leadIdx].estCard {
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

func (b *plannerORBranch) predPtr(i int) *predicate {
	if b == nil {
		return nil
	}
	return &b.preds.owner[i]
}

func (b plannerORBranch) hasLead() bool {
	return b.leadIdx >= 0 && b.leadIdx < b.preds.Len()
}

func (b *plannerORBranch) leadPtr() *predicate {
	if b == nil || !b.hasLead() {
		return nil
	}
	return b.predPtr(b.leadIdx)
}

func (b *plannerORBranch) buildMatchChecks(dst []int) []int {
	dst = dst[:0]
	if b.alwaysTrue {
		return dst
	}
	for i := 0; i < b.preds.Len(); i++ {
		p := b.preds.owner[i]
		if p.covered || p.alwaysTrue {
			continue
		}
		dst = append(dst, i)
	}
	sortActivePredicatesReader(dst, b.preds.owner)
	return dst
}

func (b *plannerORBranch) buildPostingFilterChecks(dst []int, checks []int) []int {
	dst = dst[:0]
	if b.alwaysTrue {
		return dst
	}
	for _, i := range checks {
		if b.preds.owner[i].postingFilterCapability().supportsExactBucket() {
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

func plannerORBranchBuildActiveChecksWithCovered(dst []int, branch *plannerORBranch, covered []bool) []int {
	dst = dst[:0]
	if branch == nil {
		return dst
	}
	if covered == nil {
		return branch.buildMatchChecks(dst)
	}
	for pi := 0; pi < branch.preds.Len(); pi++ {
		p := branch.preds.owner[pi]
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
	sortActivePredicatesReader(dst, branch.preds.owner)
	return dst
}

func plannerORBranchCheckCounts(branch plannerORBranch, covered []bool) (int, int) {
	for pi := 0; pi < branch.preds.Len(); pi++ {
		if branch.preds.owner[pi].alwaysFalse {
			return 0, 0
		}
	}

	if branch.preds.Len() <= plannerPredicateFastPathMaxLeaves {
		var checksInline [plannerPredicateFastPathMaxLeaves]int
		checks := checksInline[:0]

		if len(covered) == 0 {
			checks = branch.buildMatchChecks(checks)

		} else {
			for pi := 0; pi < branch.preds.Len(); pi++ {
				p := branch.preds.owner[pi]
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

	checksBuf := pooled.GetIntSlice(branch.preds.Len())
	checksBuf = plannerORBranchBuildActiveChecksWithCovered(checksBuf, &branch, covered)
	exactChecksBuf := pooled.GetIntSlice(len(checksBuf))
	exactChecksBuf = buildExactBucketPostingFilterActiveReader(exactChecksBuf, checksBuf, branch.preds.owner)
	residualChecksBuf := pooled.GetIntSlice(len(checksBuf))
	residualChecksBuf = plannerResidualChecks(residualChecksBuf, checksBuf, exactChecksBuf)
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
		p0 := &b.preds.owner[checks[0]]
		return p0.matches(idx)

	case 2:
		p0 := &b.preds.owner[checks[0]]
		if !p0.matches(idx) {
			return false
		}
		p1 := &b.preds.owner[checks[1]]
		return p1.matches(idx)

	case 3:
		p0 := &b.preds.owner[checks[0]]
		if !p0.matches(idx) {
			return false
		}
		p1 := &b.preds.owner[checks[1]]
		if !p1.matches(idx) {
			return false
		}
		p2 := &b.preds.owner[checks[2]]
		return p2.matches(idx)

	case 4:
		p0 := &b.preds.owner[checks[0]]
		if !p0.matches(idx) {
			return false
		}
		p1 := &b.preds.owner[checks[1]]
		if !p1.matches(idx) {
			return false
		}
		p2 := &b.preds.owner[checks[2]]
		if !p2.matches(idx) {
			return false
		}
		p3 := &b.preds.owner[checks[3]]
		return p3.matches(idx)
	}

	for _, i := range checks {
		p := &b.preds.owner[i]
		if !p.matches(idx) {
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
		p0 := &b.preds.owner[checks[0]]
		return p0.matches(idx)

	case 2:
		if observed != nil && observed.candidatesBuf[offset] {
			observed.countsBuf[offset]++
		}
		p0 := &b.preds.owner[checks[0]]
		if !p0.matches(idx) {
			return false
		}
		if observed != nil && observed.candidatesBuf[offset+1] {
			observed.countsBuf[offset+1]++
		}
		p1 := &b.preds.owner[checks[1]]
		return p1.matches(idx)

	case 3:
		if observed != nil && observed.candidatesBuf[offset] {
			observed.countsBuf[offset]++
		}
		p0 := &b.preds.owner[checks[0]]
		if !p0.matches(idx) {
			return false
		}
		if observed != nil && observed.candidatesBuf[offset+1] {
			observed.countsBuf[offset+1]++
		}
		p1 := &b.preds.owner[checks[1]]
		if !p1.matches(idx) {
			return false
		}
		if observed != nil && observed.candidatesBuf[offset+2] {
			observed.countsBuf[offset+2]++
		}
		p2 := &b.preds.owner[checks[2]]
		return p2.matches(idx)

	case 4:
		if observed != nil && observed.candidatesBuf[offset] {
			observed.countsBuf[offset]++
		}
		p0 := &b.preds.owner[checks[0]]
		if !p0.matches(idx) {
			return false
		}
		if observed != nil && observed.candidatesBuf[offset+1] {
			observed.countsBuf[offset+1]++
		}
		p1 := &b.preds.owner[checks[1]]
		if !p1.matches(idx) {
			return false
		}
		if observed != nil && observed.candidatesBuf[offset+2] {
			observed.countsBuf[offset+2]++
		}
		p2 := &b.preds.owner[checks[2]]
		if !p2.matches(idx) {
			return false
		}
		if observed != nil && observed.candidatesBuf[offset+3] {
			observed.countsBuf[offset+3]++
		}
		p3 := &b.preds.owner[checks[3]]
		return p3.matches(idx)
	}

	for ci, check := range checks {
		if observed != nil && observed.candidatesBuf[offset+ci] {
			slot := offset + ci
			observed.countsBuf[slot]++
		}
		p := &b.preds.owner[check]
		if !p.matches(idx) {
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
		if preds[pi].postingFilterCapability().prefersExactBucket() {
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
			if !(&preds[pi]).matches(idx) {
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
			if !(&preds[pi]).matches(idx) {
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

	if mode, exact, nextWork, ok := plannerFilterCompactPostingByPredicateChecks(preds, checks, src, work, card); ok {
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

func (b *plannerORBranch) matchesFromLead(idx uint64) bool {
	if b.alwaysTrue {
		return true
	}
	lead := b.leadPtr()
	leadNeedsCheck := lead != nil && lead.leadIterNeedsContainsCheck()

	for i := 0; i < b.preds.Len(); i++ {
		if i == b.leadIdx && !leadNeedsCheck {
			continue
		}
		p := b.preds.owner[i]
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
	for i := 0; i < b.preds.Len(); i++ {
		p := b.preds.owner[i]
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
	} else if b.hasLead() && b.preds.owner[b.leadIdx].estCard > 0 {
		est = b.preds.owner[b.leadIdx].estCard
	} else {
		for i := 0; i < b.preds.Len(); i++ {
			if c := b.preds.owner[i].estCard; c > est {
				est = c
			}
		}
	}
	return float64(est) / float64(checks)
}

func (b plannerORBranch) noOrderLeadScore(leadIdx, need int) uint64 {
	if b.alwaysTrue {
		return 0
	}
	if leadIdx < 0 || leadIdx >= b.preds.Len() {
		return 1
	}
	leadEst := b.preds.owner[leadIdx].estCard
	if leadEst == 0 {
		leadEst = 1
	}
	outEst := leadEst
	checks := uint64(1)
	var skip uint64
	for i := 0; i < b.preds.Len(); i++ {
		if i == leadIdx {
			continue
		}
		p := b.preds.owner[i]
		if p.alwaysTrue || p.covered {
			continue
		}
		checks++
		if p.estCard > 0 && p.estCard < outEst {
			outEst = p.estCard
		}
		if p.hasEffectiveBounds && p.effectiveBounds.HasLo && !p.effectiveBounds.HasHi && p.fieldIndexRangeState != nil {
			keyCount := p.fieldIndexRangeState.ov.KeyCount()
			if keyCount > 0 && p.fieldIndexRangeState.br.BaseStart > 0 {
				skip = satAddUint64(skip, satMulUint64(leadEst, uint64(p.fieldIndexRangeState.br.BaseStart))/uint64(keyCount))
			}
		}
	}
	scan := uint64(need)
	if scan == 0 {
		scan = 1
	}
	if outEst < leadEst {
		scan = satMulUint64(scan, leadEst)
		scan = satAddUint64(scan, outEst-1) / outEst
	}
	if scan > leadEst {
		scan = leadEst
	}
	return satMulUint64(satAddUint64(scan, skip), checks)
}

func (b plannerORBranch) noOrderLeadIndex(need int) int {
	best := b.leadIdx
	bestScore := b.noOrderLeadScore(best, need)
	for i := 0; i < b.preds.Len(); i++ {
		if i == b.leadIdx {
			continue
		}
		p := b.preds.owner[i]
		if p.alwaysTrue || p.covered || p.alwaysFalse || !p.hasContains() || !p.hasIter() {
			continue
		}
		if best >= 0 && p.expr.Op.IsNumericRange() && !b.preds.owner[best].expr.Op.IsNumericRange() {
			bestEst := b.preds.owner[best].estCard
			if bestEst > 0 && p.estCard < satMulUint64(bestEst, 4) {
				continue
			}
		}
		score := b.noOrderLeadScore(i, need)
		if score < bestScore || (score == bestScore && p.estCard < b.preds.owner[best].estCard) {
			best = i
			bestScore = score
		}
	}
	return best
}

func (b plannerORBranch) noOrderScore(need int) float64 {
	if b.alwaysTrue {
		return 0
	}
	return float64(b.noOrderLeadScore(b.leadIdx, need))
}

func (b plannerORBranch) noOrderBudget(need int) uint64 {
	if b.alwaysTrue {
		return plannerORNoOrderMinBranchBudget
	}

	leadEst := uint64(0)
	if b.hasLead() {
		leadEst = b.preds.owner[b.leadIdx].estCard
	}
	if leadEst == 0 {
		leadEst = plannerORNoOrderMinBranchBudget
	}

	budget := leadEst / 64
	if budget < plannerORNoOrderMinBranchBudget {
		budget = plannerORNoOrderMinBranchBudget
	}

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
		if branches.owner[i].alwaysTrue {
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

func (qv *View) orderMergeBranchStats(orderField string, branches plannerORBranches, ov indexdata.FieldIndexView) [plannerORBranchLimit]plannerOROrderMergeBranchStats {
	var stats [plannerORBranchLimit]plannerOROrderMergeBranchStats
	n := branches.Len()
	if n > plannerORBranchLimit {
		n = plannerORBranchLimit
	}
	for i := 0; i < n; i++ {

		branch := &branches.owner[i]
		stats[i].streamChecks = branch.containsChecks()
		stats[i].mergeChecks = stats[i].streamChecks

		if branch.coveredRangeBounded {
			stats[i].streamChecks = 0
			stats[i].mergeChecks = 0
			stats[i].rangeBounded = true
			if branch.estKnown {
				stats[i].rangeRows = branch.estCard
			} else {
				br := indexdata.FieldIndexRange{
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

		br, covered, ok := qv.extractOrderRangeCoverageIndexViewReader(orderField, branch.preds.owner, ov)
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
		if branches.owner[i].alwaysTrue {
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
		branch := branches.owner[i]
		if !branch.hasLead() || branch.preds.owner[branch.leadIdx].estCard == 0 {
			continue
		}
		if branch.preds.owner[branch.leadIdx].estCard <= threshold {
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

type plannerOROrderStreamSample struct {
	examined uint64
	matched  uint64
	buckets  uint64
	dropped  uint64
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
	return ClampFloat(gain, 0.90, 1.02)
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
	return ClampFloat(gain, 1.25, 1.60)
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
	return ClampFloat(gain, 0.84, 0.90)
}

// estimateOROrderMergeRouteCost estimates relative work for the two merge
// sub-routes: k-way stream merge vs fallback branch-collect+rank.
func (qv *View) estimateOROrderMergeRouteCost(
	q *qir.Shape,
	branches plannerORBranches,
	need int,
	mergeStats [plannerORBranchLimit]plannerOROrderMergeBranchStats,
) (plannerOROrderRouteCost, bool) {

	if need <= 0 || q == nil || !q.HasOrder {
		return plannerOROrderRouteCost{}, false
	}

	order := q.Order
	orderField := qv.exec.FieldNameByOrdinal(order.FieldOrdinal)
	if order.FieldOrdinal < 0 {
		return plannerOROrderRouteCost{}, false
	}

	snap := qv.exec.Stats.Load()
	universe := qv.plannerUniverseCardinality(snap)
	if universe == 0 {
		return plannerOROrderRouteCost{}, false
	}
	orderOV := qv.fieldIndexViewFromSlotsForOrder(qv.snap.Index, order)
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
	if hasSelectiveLead {
		kWayRows *= 0.92
	}
	if offsetShare > 0 {
		kWayRows *= 1.0 + offsetShare*0.75
	}
	if hasPrefixTailRisk {
		kWayRows *= 1.15
	}
	if headSensitiveOrderShape && orderStats.AvgBucketCard > float64(max(need, 1)) {
		avgBucket := orderStats.AvgBucketCard
		headBucketAmp := ClampFloat((avgBucket/float64(max(need, 1))-1.0)*0.12, 0, 3.0)
		kWayRows *= 1.0 + headBucketAmp
	}

	// k-way pays per-pop merge + predicate checks;
	// fallback pays branch collection and order ranking of merged candidates

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
				headCollectFactor := ClampFloat((float64(max(need, 1))/avgBucket)*1.5, 0.20, 1.0)
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
	var (
		order  [plannerORBranchLimit]int
		scores [plannerORBranchLimit]float64
		checks [plannerORBranchLimit]int
	)
	n := branches.Len()
	if n > plannerORBranchLimit {
		n = plannerORBranchLimit
	}
	for i := 0; i < n; i++ {
		order[i] = i
		branch := branches.owner[i]
		scores[i] = branch.evalScore()
		checks[i] = branch.containsChecks()
	}

	// small fixed-size insertion sort: highest score first, then fewer checks
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

func releasePlannerORIters(iters []plannerORIter) {
	for i := range iters {
		iters[i].Release()
	}
}

func releasePlannerORNoOrderStateIters(states []plannerORNoOrderBranchState) {
	for i := range states {
		states[i].iter.Release()
	}
}

type plannerORDecisionKind uint8

const (
	plannerORDecisionNone plannerORDecisionKind = iota
	plannerORDecisionAlwaysFalse
	plannerORDecisionNoOrder
	plannerORDecisionOrder
)

type plannerORBranchFact struct {
	card              uint64
	leadEst           uint64
	noOrderChecks     int
	streamChecks      int
	mergeChecks       int
	rangeRows         uint64
	alwaysTrue        bool
	hasLead           bool
	rangeBounded      bool
	hasPrefixTailRisk bool
	broadResidual     bool
}

type plannerORFacts struct {
	branches      plannerORBranches
	analysis      plannerOROrderAnalysis
	branchesFacts [plannerORBranchLimit]plannerORBranchFact
	branchCards   [plannerORBranchLimit]uint64
	mergeStats    [plannerORBranchLimit]plannerOROrderMergeBranchStats
	branchCount   int
	operandCount  int
	ordered       bool
	unsupported   bool
	allFalse      bool
	hasAlwaysTrue bool
	hasAnalysis   bool
	universe      uint64
	unionCard     uint64
	sumCard       uint64
	orderField    string
	orderDistinct uint64
	orderView     indexdata.FieldIndexView
	orderStats    PlannerFieldStats
}

func (facts *plannerORFacts) Release() {
	if facts.hasAnalysis {
		facts.analysis.release()
	}
	facts.branches.Release()
	*facts = plannerORFacts{}
}

type plannerORDecision struct {
	kind    plannerORDecisionKind
	noOrder plannerORNoOrderDecision
	order   plannerOROrderDecision
}

func (qv *View) executeOR(q *qir.Shape, trace *Trace) ([]uint64, bool, error) {
	var facts plannerORFacts
	ok := qv.collectORFacts(q, &facts)
	defer facts.Release()
	if !ok {
		return nil, false, nil
	}

	decision := qv.selectOR(q, &facts)
	if trace != nil {
		switch decision.kind {
		case plannerORDecisionNoOrder:
			trace.SetORSelectionRoute(decision.noOrder.traceRoute())
			trace.SetEstimated(decision.noOrder.expectedRows, decision.noOrder.kWayCost, decision.noOrder.fallbackCost)
		case plannerORDecisionOrder:
			trace.SetORSelectionRoute(qv.traceOROrderRoute(q, &facts, decision.order))
			trace.SetEstimated(decision.order.expectedRows, decision.order.bestCost, decision.order.fallbackCost)
		}
	}

	return qv.dispatchOR(q, &facts, decision, trace)
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

func (qv *View) collectORFacts(q *qir.Shape, facts *plannerORFacts) bool {
	// Keep OR collection on cheap facts; branch predicates are dispatch-owned.
	if q.Limit == 0 {
		return false
	}
	if q.Expr.Op != qir.OpOR || q.Expr.Not {
		return false
	}
	if len(q.Expr.Operands) < plannerORMinOperandCount {
		return false
	}
	if len(q.Expr.Operands) > plannerORBranchLimit {
		return false
	}
	facts.operandCount = len(q.Expr.Operands)

	snap := qv.exec.Stats.Load()
	facts.universe = qv.plannerUniverseCardinality(snap)

	if !q.HasOrder {
		return qv.collectORBranchFacts(q, facts, snap)
	}

	o := q.Order
	if o.Kind != qir.OrderKindBasic {
		return false
	}
	if fm := qv.fieldMetaByOrder(o); fm != nil && fm.Ptr {
		return false
	}
	facts.ordered = true
	facts.orderField = qv.exec.FieldNameByOrdinal(o.FieldOrdinal)
	facts.orderView = qv.fieldIndexViewFromSlotsForOrder(qv.snap.Index, o)
	facts.orderDistinct = uint64(facts.orderView.KeyCount())
	facts.orderStats = qv.plannerOrderFieldStats(facts.orderField, snap, facts.universe, facts.orderDistinct)
	if fm := qv.fieldMetaByOrder(o); fm == nil || fm.Slice || !facts.orderView.HasData() {
		facts.unsupported = true
		return true
	}
	return qv.collectORBranchFacts(q, facts, snap)
}

func (qv *View) collectORBranchFacts(q *qir.Shape, facts *plannerORFacts, snap *PlannerStatsSnapshot) bool {
	var leavesBuf [plannerPredicateFastPathMaxLeaves]qir.Expr
	for i := 0; i < len(q.Expr.Operands); i++ {
		fact, keep, ok := qv.collectORBranchFact(q.Expr.Operands[i], facts, snap, leavesBuf[:0])
		if !ok {
			facts.unsupported = true
			continue
		}
		if !keep {
			continue
		}
		idx := facts.branchCount
		facts.branchesFacts[idx] = fact
		facts.branchCards[idx] = fact.card
		facts.mergeStats[idx] = plannerOROrderMergeBranchStats{
			streamChecks: fact.streamChecks,
			mergeChecks:  fact.mergeChecks,
			rangeRows:    fact.rangeRows,
			rangeBounded: fact.rangeBounded,
		}
		facts.branchCount++
		if fact.alwaysTrue {
			facts.hasAlwaysTrue = true
		}
		if facts.branchCount == plannerORBranchLimit {
			break
		}
	}
	if facts.branchCount == 0 {
		facts.allFalse = !facts.unsupported
		return true
	}
	facts.computeORUnionCards()
	return true
}

func collectORBranchLeaves(op qir.Expr, leavesBuf []qir.Expr) ([]qir.Expr, []qir.Expr, bool) {
	return qir.CollectAndLeavesPooledFallback(op, leavesBuf[:0], qir.LeafModeCollect)
}

func (qv *View) collectORBranchFact(
	op qir.Expr,
	facts *plannerORFacts,
	snap *PlannerStatsSnapshot,
	leavesBuf []qir.Expr,
) (plannerORBranchFact, bool, bool) {
	leaves, leavesHeap, ok := collectORBranchLeaves(op, leavesBuf)
	if !ok {
		return plannerORBranchFact{}, false, false
	}
	if leavesHeap != nil {
		defer qir.ReleaseExprSlice(leavesHeap)
	}
	if len(leaves) == 0 {
		return plannerORBranchFact{alwaysTrue: true, card: facts.universe}, true, true
	}

	fact := plannerORBranchFact{}
	minEst := uint64(0)
	active := 0
	leadEst := uint64(0)
	leadChecks := 0
	hasRange := false
	var rb indexdata.Bounds

	for i := 0; i < len(leaves); i++ {
		e := leaves[i]
		if qir.IsFalseConst(e) {
			return plannerORBranchFact{}, false, true
		}
		if qir.IsTrueConst(e) {
			continue
		}
		if e.FieldOrdinal < 0 {
			return plannerORBranchFact{}, false, false
		}

		orderCovered := false
		if facts.ordered && !e.Not && qv.exec.FieldNameByOrdinal(e.FieldOrdinal) == facts.orderField {
			if applied, ok := qv.applyScalarExprToRangeBounds(e, &rb); !ok {
				return plannerORBranchFact{}, false, false
			} else if applied {
				orderCovered = true
				hasRange = true
			}
		}

		leafSel, _, _, _, ok := qv.estimateLeafOrderCost(e, snap, facts.universe, facts.orderField, facts.orderDistinct > 0)
		if !ok {
			if e.Op != qir.OpSUFFIX && e.Op != qir.OpCONTAINS {
				return plannerORBranchFact{}, false, false
			}
			_, isSlice, _, err := qv.exprValueToLookupKey(e)
			if err != nil || isSlice {
				return plannerORBranchFact{}, false, false
			}
			leafSel = 1
		}
		if leafSel <= 0 {
			return plannerORBranchFact{}, false, true
		}
		if leafSel > 1 {
			leafSel = 1
		}
		est := uint64(math.Ceil(leafSel * float64(facts.universe)))
		if est == 0 {
			est = 1
		}
		if est > facts.universe {
			est = facts.universe
		}

		active++
		if minEst == 0 || est < minEst {
			minEst = est
		}
		if !orderCovered {
			fact.streamChecks++
			if !plannerORLeafExactBucketFact(e) {
				fact.mergeChecks++
			}
			leadChecks++
		}
		if !e.Not && e.Op != qir.OpSUFFIX && e.Op != qir.OpCONTAINS {
			if leadEst == 0 || est < leadEst {
				leadEst = est
			}
			fact.hasLead = true
		}
		if facts.ordered && qv.isPositiveNonOrderScalarPrefixLeaf(facts.orderField, e) {
			fact.hasPrefixTailRisk = true
		}
		if facts.ordered &&
			!orderCovered &&
			!e.Not &&
			qv.exec.FieldNameByOrdinal(e.FieldOrdinal) != facts.orderField &&
			(e.Op == qir.OpPREFIX || e.Op.IsNumericRange()) &&
			est >= facts.universe/4 {
			fact.broadResidual = true
		}
	}

	if active == 0 {
		return plannerORBranchFact{alwaysTrue: true, card: facts.universe}, true, true
	}
	fact.card = minEst
	if active > 1 {
		decay := 1.0
		for i := 1; i < active; i++ {
			decay *= 0.72
		}
		fact.card = uint64(float64(fact.card) * decay)
		if fact.card == 0 {
			fact.card = 1
		}
	}
	if hasRange {
		br := facts.orderView.RangeForBounds(rb)
		if br.Empty() {
			return plannerORBranchFact{}, false, true
		}
		if br.BaseStart != 0 || br.BaseEnd != facts.orderView.KeyCount() {
			fact.rangeBounded = true
			_, fact.rangeRows = facts.orderView.RangeStats(br)
			if fact.streamChecks == 0 {
				fact.card = fact.rangeRows
			}
		}
	}
	if fact.card > facts.universe {
		fact.card = facts.universe
	}
	fact.leadEst = leadEst
	if fact.hasLead && leadChecks > 0 {
		fact.noOrderChecks = leadChecks - 1
	}
	return fact, true, true
}

func plannerORLeafExactBucketFact(e qir.Expr) bool {
	switch e.Op {
	case qir.OpEQ, qir.OpIN, qir.OpHASALL, qir.OpHASANY:
		return true
	default:
		return false
	}
}

func (facts *plannerORFacts) computeORUnionCards() {
	if facts.universe == 0 {
		return
	}
	if facts.hasAlwaysTrue {
		facts.unionCard = facts.universe
		facts.sumCard = facts.universe
		return
	}
	sumCard := uint64(0)
	remainProb := 1.0
	for i := 0; i < facts.branchCount; i++ {
		card := facts.branchCards[i]
		sumCard += card
		p := float64(card) / float64(facts.universe)
		if p < 0 {
			p = 0
		}
		if p > 1 {
			p = 1
		}
		remainProb *= 1.0 - p
	}
	unionCard := uint64((1.0-remainProb)*float64(facts.universe) + 0.5)
	if unionCard > facts.universe {
		unionCard = facts.universe
	}
	if unionCard == 0 && sumCard > 0 {
		unionCard = 1
	}
	facts.unionCard = unionCard
	facts.sumCard = sumCard
}

func (qv *View) selectOR(q *qir.Shape, facts *plannerORFacts) plannerORDecision {
	if facts.allFalse {
		return plannerORDecision{kind: plannerORDecisionAlwaysFalse}
	}
	if facts.ordered {
		return plannerORDecision{
			kind:  plannerORDecisionOrder,
			order: qv.selectOROrder(q, facts),
		}
	}
	return plannerORDecision{
		kind:    plannerORDecisionNoOrder,
		noOrder: qv.selectORNoOrder(q, facts),
	}
}

func (qv *View) selectORNoOrder(q *qir.Shape, facts *plannerORFacts) plannerORNoOrderDecision {
	need, ok := orderWindow(q)
	if !ok || need <= 0 {
		return plannerORNoOrderDecision{}
	}
	if facts.unsupported || facts.universe == 0 {
		fallback := plannerORNoOrderCandidate{
			kind:         plannerORNoOrderCandidateMaterializedFallback,
			cost:         float64(need) * float64(max(1, facts.operandCount)),
			expectedRows: uint64(need),
		}
		return plannerORNoOrderDecision{
			selected:             fallback,
			materializedFallback: fallback,
			expectedRows:         uint64(need),
		}
	}
	if facts.hasAlwaysTrue {
		return plannerORNoOrderDecision{
			selected:     plannerORNoOrderCandidate{kind: plannerORNoOrderCandidateUniverse, cost: 1, expectedRows: uint64(need), unionRows: facts.universe, sumRows: facts.universe},
			use:          true,
			expectedRows: uint64(need),
		}
	}
	for i := 0; i < facts.branchCount; i++ {
		if !facts.branchesFacts[i].hasLead {
			fallback := plannerORNoOrderCandidate{
				kind:         plannerORNoOrderCandidateMaterializedFallback,
				cost:         float64(need) * float64(max(1, facts.operandCount)),
				expectedRows: uint64(need),
				unionRows:    facts.unionCard,
				sumRows:      facts.sumCard,
			}
			return plannerORNoOrderDecision{
				selected:             fallback,
				materializedFallback: fallback,
				expectedRows:         uint64(need),
			}
		}
	}
	if facts.unionCard == 0 {
		return plannerORNoOrderDecision{}
	}

	expectedRows := estimateRowsForNeed(uint64(need), facts.unionCard, facts.universe)
	avgChecks := facts.noOrderChecks()
	runRows := expectedRows
	leadRows := uint64(0)
	for i := 0; i < facts.branchCount; i++ {
		lead := facts.branchesFacts[i].leadEst
		if lead == 0 {
			lead = facts.branchCards[i]
		}
		leadRows = satAddUint64(leadRows, lead)
	}
	if leadRows > 0 && leadRows < runRows {
		runRows = leadRows
	}
	costKWay := float64(runRows) * (1.0 + avgChecks)
	fallbackRows := min(facts.unionCard, uint64(need))
	costFallback := float64(facts.sumCard) + float64(fallbackRows)

	var candidates [3]plannerORNoOrderCandidate
	n := 0
	if q.Offset == 0 && facts.branchCount >= 3 {
		candidates[n] = plannerORNoOrderCandidate{
			kind:         plannerORNoOrderCandidateAdaptiveMerge,
			cost:         costKWay,
			expectedRows: runRows,
			unionRows:    facts.unionCard,
			sumRows:      facts.sumCard,
			avgChecks:    avgChecks,
		}
		n++
	}
	candidates[n] = plannerORNoOrderCandidate{
		kind:         plannerORNoOrderCandidateBaselineMerge,
		cost:         costKWay,
		expectedRows: runRows,
		unionRows:    facts.unionCard,
		sumRows:      facts.sumCard,
		avgChecks:    avgChecks,
	}
	n++
	candidates[n] = plannerORNoOrderCandidate{
		kind:         plannerORNoOrderCandidateMaterializedFallback,
		cost:         costFallback,
		expectedRows: fallbackRows,
		unionRows:    facts.unionCard,
		sumRows:      facts.sumCard,
		avgChecks:    avgChecks,
	}
	n++

	return plannerORNoOrderPick(candidates[:n], q.Limit > plannerORNoOrderLimitMax || q.Offset > plannerORNoOrderOffsetMax)
}

func (facts *plannerORFacts) noOrderChecks() float64 {
	totalWeight := 0.0
	totalChecks := 0.0
	for i := 0; i < facts.branchCount; i++ {
		weight := float64(1)
		if facts.branchCards[i] > 0 {
			weight = float64(facts.branchCards[i])
		}
		totalWeight += weight
		totalChecks += weight * float64(facts.branchesFacts[i].noOrderChecks)
	}
	if totalWeight == 0 {
		return 1
	}
	avg := totalChecks / totalWeight
	if avg < 1 {
		avg = 1
	}
	return avg
}

func (qv *View) selectOROrder(q *qir.Shape, facts *plannerORFacts) plannerOROrderDecision {
	if facts.unsupported {
		cost := float64(max(1, q.Limit))
		fallback := plannerOROrderCandidate{
			kind:         plannerOROrderCandidateMaterializedFallback,
			cost:         cost,
			expectedRows: q.Limit,
		}
		return plannerOROrderDecision{
			selected:             fallback,
			materializedFallback: fallback,
			plan:                 plannerOROrderFallback,
			expectedRows:         q.Limit,
			bestCost:             cost,
			fallbackCost:         cost,
		}
	}
	need, ok := orderWindow(q)
	if !ok || need <= 0 || facts.universe == 0 || facts.unionCard == 0 {
		return plannerOROrderDecision{plan: plannerOROrderFallback}
	}

	expectedRows := estimateRowsForNeed(uint64(need), facts.unionCard, facts.universe)
	streamChecks := facts.orderStreamChecks()
	costFallback := float64(facts.sumCard) + float64(expectedRows)
	costStream := float64(expectedRows) * streamChecks * plannerFieldStatsSkew(facts.orderStats)
	cacheLimit := qv.snap.MaterializedPredCacheLimit()
	cacheEntries := 0
	if cache := qv.snap.MaterializedPredCache(); cache != nil {
		cacheEntries = cache.EntryCount()
	}
	cacheColdTiny := cacheLimit <= 0 || (cacheLimit < facts.branchCount && cacheEntries < facts.branchCount)
	cacheCold := cacheEntries == 0
	hasFullSpanOrderBranch := facts.hasFullSpanOrderBranch()
	hasBroadResidual := false
	highBranchLeads := 0
	for i := 0; i < facts.branchCount; i++ {
		if facts.branchesFacts[i].broadResidual {
			hasBroadResidual = true
		}
		lead := facts.branchesFacts[i].leadEst
		if lead == 0 {
			lead = facts.branchCards[i]
		}
		if lead >= uint64(need)*64 && lead >= facts.universe/16 {
			highBranchLeads++
		}
	}
	if cacheColdTiny && facts.branchCount > 1 {
		costFallback *= 1.0 + float64(facts.branchCount)*0.65
		if facts.hasPrefixTailRisk() || hasBroadResidual {
			costFallback *= 1.35
		}
	}
	cacheState, cachePressurePenalty := qv.orderedORSelectorCacheRoute(q, facts)

	var candidates [4]plannerOROrderCandidate
	n := 0
	candidates[n] = plannerOROrderCandidate{
		kind:         plannerOROrderCandidateMaterializedFallback,
		cost:         costFallback,
		expectedRows: expectedRows,
		unionRows:    facts.unionCard,
		sumRows:      facts.sumCard,
		cacheState:   cacheState,
	}
	n++

	routeCost, routeOK := qv.estimateOROrderMergeRouteCostFacts(q, facts, need)
	mergeNeedLimit := plannerOROrderMergeNeedLimit(need, facts.branchCount, facts.unionCard, facts.sumCard, q.Offset)
	mergeAllowed := !facts.hasAlwaysTrue && need <= mergeNeedLimit
	if mergeAllowed {
		costMerge := facts.orderMergeCost(uint64(need))
		prefixTailRisk := routeOK && routeCost.hasPrefixTailRisk
		if routeOK && routeCost.kWay > 0 && routeCost.kWay < costMerge &&
			hasFullSpanOrderBranch &&
			!routeCost.hasPrefixTailRisk &&
			!facts.hasKWayExactBucketApplyWork() {
			costMerge = routeCost.kWay
		}
		if prefixTailRisk {
			costMerge *= plannerOROrderExactBucketApplyPenalty(&facts.mergeStats, facts.branchCount)
		}
		costMerge *= plannerOROrderPrefixTailRiskPenalty(prefixTailRisk, facts.branchCount, q.Offset)
		if routeOK {
			if hasBroadResidual {
				costMerge *= 1.25
			}
			if (cacheCold || cacheColdTiny) && hasFullSpanOrderBranch && expectedRows <= uint64(need*8) && highBranchLeads > 0 {
				costMerge *= 1.0 + float64(highBranchLeads)*0.12
			}
			if cacheColdTiny && (prefixTailRisk || hasBroadResidual || facts.hasKWayExactBucketApplyWork()) {
				costMerge *= 1.35
				if costStream > 0 && costMerge >= costStream*0.70 && costMerge <= costStream*1.15 {
					costMerge *= 1.35
				}
			}
			if plannerMaterializedCacheStateUnretained(cacheState) {
				costMerge *= cachePressurePenalty
			}
		}
		candidates[n] = plannerOROrderCandidate{
			kind:                plannerOROrderCandidateKWayMerge,
			cost:                costMerge,
			expectedRows:        expectedRows,
			unionRows:           facts.unionCard,
			sumRows:             facts.sumCard,
			avgChecks:           routeCost.avgChecks,
			overlap:             routeCost.overlap,
			cacheState:          cacheState,
			hasPrefixTailRisk:   routeCost.hasPrefixTailRisk,
			hasBroadResidual:    hasBroadResidual,
			hasSelectiveLead:    routeCost.hasSelectiveLead,
			fallbackCollectFast: facts.orderView.HasData(),
		}
		n++
		if routeOK && routeCost.fallback > 0 {
			fallbackFirst := plannerOROrderFallbackFirstDecisionFromCost(q, need, routeCost, facts.orderView.HasData())
			if fallbackFirst.prefer {
				candidates[n] = plannerOROrderCandidate{
					kind:                plannerOROrderCandidateBranchCollect,
					cost:                routeCost.fallback,
					expectedRows:        expectedRows,
					unionRows:           facts.unionCard,
					sumRows:             facts.sumCard,
					avgChecks:           fallbackFirst.avgChecks,
					overlap:             routeCost.overlap,
					cacheState:          cacheState,
					hasPrefixTailRisk:   routeCost.hasPrefixTailRisk,
					hasBroadResidual:    hasBroadResidual,
					hasSelectiveLead:    routeCost.hasSelectiveLead,
					fallbackCollectFast: fallbackFirst.fallbackCollectFast,
				}
				n++
			}
		}
	}

	candidates[n] = plannerOROrderCandidate{
		kind:         plannerOROrderCandidateStream,
		cost:         costStream,
		expectedRows: expectedRows,
		unionRows:    facts.unionCard,
		sumRows:      facts.sumCard,
		avgChecks:    streamChecks,
		cacheState:   cacheState,
	}
	n++

	forceMaterialized := q.Limit > plannerOROrderLimitMax ||
		q.Offset > plannerOROrderOffsetMax ||
		(!cacheColdTiny &&
			!plannerMaterializedCacheStateUnretained(cacheState) &&
			plannerOROrderPreferMaterializedSmallSideBranches(q, &facts.branchCards, facts.branchCount, facts.unionCard, need, &facts.mergeStats))
	return plannerOROrderPick(candidates[:n], forceMaterialized)
}

func (qv *View) traceOROrderRoute(q *qir.Shape, facts *plannerORFacts, d plannerOROrderDecision) TraceORRoute {
	route := d.traceRoute()
	need, ok := orderWindow(q)
	if !ok || need <= 0 || facts == nil || facts.universe == 0 {
		return route
	}
	route.SelectedWork = qv.orderedORCandidateTraceWork(q, facts, d.selected, need)
	route.RejectedWork = qv.orderedORCandidateTraceWork(q, facts, d.rejected, need)
	return route
}

func plannerOROrderTraceWorkCost(work TraceRouteWork) float64 {
	return work.CandidateScan +
		work.PostingContains +
		work.ExactBucketFilter +
		work.BranchMerge +
		work.MaterializedBuild +
		work.UnretainedRebuildPenalty +
		work.TailRiskPenalty -
		work.RetainedCacheBenefit
}

func (qv *View) orderedORCandidateTraceWork(q *qir.Shape, facts *plannerORFacts, c plannerOROrderCandidate, need int) TraceRouteWork {
	if c.cost <= 0 {
		return TraceRouteWork{}
	}
	switch c.kind {
	case plannerOROrderCandidateMaterializedFallback:
		work := TraceRouteWork{
			CandidateScan:     float64(c.expectedRows),
			MaterializedBuild: float64(c.sumRows),
		}
		base := plannerOROrderTraceWorkCost(work)
		if c.cost > base {
			work.TailRiskPenalty = c.cost - base
		} else if c.cost < base {
			work.RetainedCacheBenefit = base - c.cost
		}
		return work
	case plannerOROrderCandidateStream:
		scan := float64(c.expectedRows) * plannerFieldStatsSkew(facts.orderStats)
		work := TraceRouteWork{CandidateScan: scan}
		if c.avgChecks > 1 {
			work.PostingContains = scan * (c.avgChecks - 1)
		}
		base := plannerOROrderTraceWorkCost(work)
		if c.cost > base {
			work.TailRiskPenalty = c.cost - base
		} else if c.cost < base {
			work.RetainedCacheBenefit = base - c.cost
		}
		return work
	case plannerOROrderCandidateKWayMerge:
		return qv.orderedORKWayCandidateTraceWork(q, facts, c, need)
	case plannerOROrderCandidateBranchCollect:
		_, fallbackWork, ok := qv.estimateOROrderMergeRouteTraceWorkFacts(q, facts, need)
		if !ok {
			return c.traceWork()
		}
		base := plannerOROrderTraceWorkCost(fallbackWork)
		if c.cost > base {
			fallbackWork.TailRiskPenalty += c.cost - base
		} else if c.cost < base {
			fallbackWork.RetainedCacheBenefit += base - c.cost
		}
		return fallbackWork
	default:
		return c.traceWork()
	}
}

func (qv *View) orderedORKWayCandidateTraceWork(q *qir.Shape, facts *plannerORFacts, c plannerOROrderCandidate, need int) TraceRouteWork {
	routeCost, routeOK := qv.estimateOROrderMergeRouteCostFacts(q, facts, need)
	mergeWork := facts.orderMergeTraceWork(uint64(need))
	costMerge := plannerOROrderTraceWorkCost(mergeWork)
	if routeOK && routeCost.kWay > 0 && routeCost.kWay < costMerge &&
		facts.hasFullSpanOrderBranch() &&
		!routeCost.hasPrefixTailRisk &&
		!facts.hasKWayExactBucketApplyWork() {
		kwayWork, _, ok := qv.estimateOROrderMergeRouteTraceWorkFacts(q, facts, need)
		if ok {
			mergeWork = kwayWork
			costMerge = plannerOROrderTraceWorkCost(mergeWork)
		}
	}

	prefixTailRisk := routeOK && routeCost.hasPrefixTailRisk
	if prefixTailRisk {
		nextCost := costMerge * plannerOROrderExactBucketApplyPenalty(&facts.mergeStats, facts.branchCount)
		mergeWork.ExactBucketFilter += nextCost - costMerge
		costMerge = nextCost
	}
	nextCost := costMerge * plannerOROrderPrefixTailRiskPenalty(prefixTailRisk, facts.branchCount, q.Offset)
	mergeWork.TailRiskPenalty += nextCost - costMerge
	costMerge = nextCost

	if routeOK {
		cacheLimit := qv.snap.MaterializedPredCacheLimit()
		cacheEntries := 0
		if cache := qv.snap.MaterializedPredCache(); cache != nil {
			cacheEntries = cache.EntryCount()
		}
		cacheColdTiny := cacheLimit <= 0 || (cacheLimit < facts.branchCount && cacheEntries < facts.branchCount)
		cacheCold := cacheEntries == 0
		hasBroadResidual := false
		highBranchLeads := 0
		for i := 0; i < facts.branchCount; i++ {
			if facts.branchesFacts[i].broadResidual {
				hasBroadResidual = true
			}
			lead := facts.branchesFacts[i].leadEst
			if lead == 0 {
				lead = facts.branchCards[i]
			}
			if lead >= uint64(need)*64 && lead >= facts.universe/16 {
				highBranchLeads++
			}
		}
		if hasBroadResidual {
			nextCost = costMerge * 1.25
			mergeWork.TailRiskPenalty += nextCost - costMerge
			costMerge = nextCost
		}
		if (cacheCold || cacheColdTiny) && facts.hasFullSpanOrderBranch() && c.expectedRows <= uint64(need*8) && highBranchLeads > 0 {
			nextCost = costMerge * (1.0 + float64(highBranchLeads)*0.12)
			mergeWork.TailRiskPenalty += nextCost - costMerge
			costMerge = nextCost
		}
		if cacheColdTiny && (prefixTailRisk || hasBroadResidual || facts.hasKWayExactBucketApplyWork()) {
			nextCost = costMerge * 1.35
			mergeWork.TailRiskPenalty += nextCost - costMerge
			costMerge = nextCost
			streamCost := float64(c.expectedRows) * facts.orderStreamChecks() * plannerFieldStatsSkew(facts.orderStats)
			if streamCost > 0 && costMerge >= streamCost*0.70 && costMerge <= streamCost*1.15 {
				nextCost = costMerge * 1.35
				mergeWork.TailRiskPenalty += nextCost - costMerge
				costMerge = nextCost
			}
		}
		cacheState, cachePressurePenalty := qv.orderedORSelectorCacheRoute(q, facts)
		if plannerMaterializedCacheStateUnretained(cacheState) {
			nextCost = costMerge * cachePressurePenalty
			mergeWork.UnretainedRebuildPenalty += nextCost - costMerge
		}
	}

	base := plannerOROrderTraceWorkCost(mergeWork)
	if c.cost > base {
		mergeWork.TailRiskPenalty += c.cost - base
	} else if c.cost < base {
		mergeWork.RetainedCacheBenefit += base - c.cost
	}
	return mergeWork
}

func (qv *View) orderedORSelectorCacheRoute(q *qir.Shape, facts *plannerORFacts) (plannerMaterializedCacheState, float64) {
	if qv.snap == nil {
		return plannerMaterializedCacheDisabled, 1
	}
	cache := qv.snap.MaterializedPredCache()
	if cache == nil {
		return plannerMaterializedCacheDisabled, 1
	}
	if q == nil || facts == nil || !q.HasOrder || facts.universe == 0 {
		return plannerMaterializedCacheDisabled, 1
	}
	potentialKeys := 0
	for i := 0; i < facts.branchCount; i++ {
		potentialKeys += facts.branchesFacts[i].streamChecks
	}
	if potentialKeys == 0 {
		return plannerMaterializedCacheDisabled, 1
	}
	if plannerMaterializedCacheRouteEnvelopeRetained(cache, potentialKeys, facts.universe) {
		return plannerMaterializedCacheColdRegularAdmissible, 1
	}

	var routeSet plannerMaterializedCacheRouteSet
	routeSet.init(qv)
	snap := qv.exec.Stats.Load()
	var leavesBuf [plannerPredicateFastPathMaxLeaves]qir.Expr
	for i := 0; i < len(q.Expr.Operands); i++ {
		leaves, leavesHeap, ok := collectORBranchLeaves(q.Expr.Operands[i], leavesBuf[:0])
		if !ok {
			if leavesHeap != nil {
				qir.ReleaseExprSlice(leavesHeap)
			}
			continue
		}

		var mergedRangesBuf []orderedMergedScalarRangeField
		if len(leaves) > 1 {
			mergedRangesBuf = orderedMergedScalarRangeFieldSlicePool.Get(len(leaves))
			var mergeOK bool
			mergedRangesBuf, mergeOK = qv.collectOrderedMergedScalarRangeFields(facts.orderField, leaves, mergedRangesBuf)
			if !mergeOK {
				orderedMergedScalarRangeFieldSlicePool.Put(mergedRangesBuf)
				if leavesHeap != nil {
					qir.ReleaseExprSlice(leavesHeap)
				}
				continue
			}
		}

		for li := 0; li < len(leaves); li++ {
			e := leaves[li]
			if e.Not || e.FieldOrdinal < 0 {
				continue
			}
			fieldName := qv.exec.FieldNameByOrdinal(e.FieldOrdinal)
			if fieldName == facts.orderField && qv.isOrderRangeCoveredLeaf(facts.orderField, e) {
				continue
			}

			if qv.isPositiveOrderedMergedScalarRangeLeaf(e, facts.orderField) && mergedRangesBuf != nil {
				idx := findOrderedMergedScalarRangeField(mergedRangesBuf, fieldName)
				if idx >= 0 {
					merged := mergedRangesBuf[idx]
					if merged.count > 1 {
						if merged.first != li {
							continue
						}
						var key qcache.MaterializedPredKey
						if boundsExactStringPrefix(merged.bounds) {
							prefixExpr := merged.expr
							prefixExpr.Op = qir.OpPREFIX
							prefixExpr.Value = merged.bounds.Prefix
							key = qv.materializedPredKey(prefixExpr)
						} else {
							key = qv.materializedPredKeyForExactScalarRange(fieldName, merged.bounds)
						}
						if key.IsZero() {
							continue
						}
						ov := qv.fieldIndexViewFromSlotsByName(qv.snap.Index, fieldName)
						if !ov.HasData() {
							continue
						}
						_, est := ov.RangeStats(ov.RangeForBounds(merged.bounds))
						if est == 0 {
							continue
						}
						routeSet.add(key, est, qv.classifyPlannerMaterializedCacheKey(key, est, false))
						continue
					}
				}
			}

			if !e.Op.IsMaterializedScalarCache() {
				continue
			}
			key := qv.materializedPredKey(e)
			if key.IsZero() {
				continue
			}
			sel, _, _, _, ok := qv.estimateLeafOrderCost(e, snap, facts.universe, facts.orderField, facts.orderDistinct > 0)
			if !ok {
				if e.Op != qir.OpSUFFIX && e.Op != qir.OpCONTAINS {
					continue
				}
				sel = 1
			}
			if sel <= 0 {
				continue
			}
			if sel > 1 {
				sel = 1
			}
			est := uint64(math.Ceil(sel * float64(facts.universe)))
			if est == 0 {
				est = 1
			}
			if est > facts.universe {
				est = facts.universe
			}
			routeSet.add(key, est, qv.classifyPlannerMaterializedCacheKey(key, est, false))
		}

		if mergedRangesBuf != nil {
			orderedMergedScalarRangeFieldSlicePool.Put(mergedRangesBuf)
		}
		if leavesHeap != nil {
			qir.ReleaseExprSlice(leavesHeap)
		}
	}

	return routeSet.finish(), routeSet.pressurePenalty()
}

func (facts *plannerORFacts) orderStreamChecks() float64 {
	if facts.hasAlwaysTrue {
		return 1
	}
	if facts.branchCount == 0 {
		return 1
	}
	total := 0.0
	for i := 0; i < facts.branchCount; i++ {
		total += float64(1 + facts.mergeStats[i].streamChecks)
	}
	avg := total / float64(facts.branchCount)
	if avg < 1 {
		avg = 1
	}
	checks := 1.0 + (avg-1.0)*0.65
	branchFactor := 1.0 + float64(facts.branchCount-1)*0.35
	return checks * branchFactor
}

func (facts *plannerORFacts) hasFullSpanOrderBranch() bool {
	for i := 0; i < facts.branchCount; i++ {
		if facts.branchesFacts[i].alwaysTrue {
			continue
		}
		if !facts.mergeStats[i].rangeBounded {
			return true
		}
	}
	return false
}

func (facts *plannerORFacts) hasKWayExactBucketApplyWork() bool {
	for i := 0; i < facts.branchCount; i++ {
		if facts.mergeStats[i].streamChecks > facts.mergeStats[i].mergeChecks {
			return true
		}
	}
	return false
}

func plannerOROrderExactBucketApplyPenalty(stats *[plannerORBranchLimit]plannerOROrderMergeBranchStats, branchCount int) float64 {
	exactOnly := 0
	active := 0
	for i := 0; i < branchCount && i < plannerORBranchLimit; i++ {
		streamChecks := stats[i].streamChecks
		mergeChecks := stats[i].mergeChecks
		if streamChecks == 0 && mergeChecks == 0 {
			continue
		}
		active++
		if streamChecks > mergeChecks {
			exactOnly += streamChecks - mergeChecks
		}
	}
	if exactOnly == 0 || active == 0 {
		return 1
	}
	return 1 + float64(exactOnly)*2.4/float64(active)
}

func plannerOROrderPrefixTailRiskPenalty(hasPrefixTailRisk bool, branchCount int, offset uint64) float64 {
	if !hasPrefixTailRisk || offset != 0 || branchCount <= 0 {
		return 1
	}
	return 1 + float64(branchCount)*0.09
}

func (facts *plannerORFacts) hasSelectiveLead(need int) bool {
	if need <= 0 {
		return false
	}
	threshold := uint64(need * plannerOROrderFallbackFirstLeadNeedMul)
	if threshold == 0 {
		threshold = 1
	}
	for i := 0; i < facts.branchCount; i++ {
		branch := facts.branchesFacts[i]
		if !branch.hasLead || branch.leadEst == 0 {
			continue
		}
		if branch.leadEst <= threshold {
			return true
		}
	}
	return false
}

func (facts *plannerORFacts) hasPrefixTailRisk() bool {
	for i := 0; i < facts.branchCount; i++ {
		if facts.branchesFacts[i].hasPrefixTailRisk {
			return true
		}
	}
	return false
}

func (facts *plannerORFacts) avgMergeChecks() float64 {
	totalChecks := 0.0
	activeBranches := 0.0
	for i := 0; i < facts.branchCount; i++ {
		if facts.branchesFacts[i].alwaysTrue {
			continue
		}
		totalChecks += float64(facts.mergeStats[i].mergeChecks)
		activeBranches++
	}
	if activeBranches == 0 {
		return 0
	}
	return totalChecks / activeBranches
}

func (facts *plannerORFacts) orderMergeCost(need uint64) float64 {
	if need == 0 || facts.universe == 0 {
		return math.Inf(1)
	}

	subRows := 0.0
	candidateUpper := uint64(0)
	for i := 0; i < facts.branchCount; i++ {
		card := facts.branchCards[i]
		if card == 0 {
			continue
		}
		branchUniverse := facts.universe
		if facts.mergeStats[i].rangeRows > 0 && facts.mergeStats[i].rangeRows < branchUniverse {
			branchUniverse = facts.mergeStats[i].rangeRows
		}
		rows := estimateRowsForNeed(need, card, branchUniverse)
		if branchUniverse == facts.universe && card >= need {
			rowsCap := need + need/2
			if rowsCap < need {
				rowsCap = need
			}
			rows = min(rows, rowsCap)
		}
		checks := 1.0 + float64(facts.mergeStats[i].mergeChecks)*0.25
		subRows += float64(rows) * checks
		candidateUpper += min(need, card)
	}
	if candidateUpper == 0 {
		candidateUpper = min(need, facts.unionCard)
	}
	if facts.sumCard > 0 {
		candidateUpper = uint64(float64(candidateUpper) * (float64(facts.unionCard) / float64(facts.sumCard)))
		if candidateUpper == 0 {
			candidateUpper = 1
		}
	}
	rankRows := plannerFieldStatsMergeRankRows(facts.orderStats, candidateUpper, need, facts.universe)
	return subRows + float64(rankRows)
}

func (facts *plannerORFacts) orderMergeTraceWork(need uint64) TraceRouteWork {
	if need == 0 || facts.universe == 0 {
		return TraceRouteWork{TailRiskPenalty: math.Inf(1)}
	}

	var work TraceRouteWork
	candidateUpper := uint64(0)
	for i := 0; i < facts.branchCount; i++ {
		card := facts.branchCards[i]
		if card == 0 {
			continue
		}
		branchUniverse := facts.universe
		if facts.mergeStats[i].rangeRows > 0 && facts.mergeStats[i].rangeRows < branchUniverse {
			branchUniverse = facts.mergeStats[i].rangeRows
		}
		rows := estimateRowsForNeed(need, card, branchUniverse)
		if branchUniverse == facts.universe && card >= need {
			rowsCap := need + need/2
			if rowsCap < need {
				rowsCap = need
			}
			rows = min(rows, rowsCap)
		}
		checks := float64(facts.mergeStats[i].mergeChecks) * 0.25
		rowsF := float64(rows)
		work.CandidateScan += rowsF
		work.PostingContains += rowsF * checks
		candidateUpper += min(need, card)
	}
	if candidateUpper == 0 {
		candidateUpper = min(need, facts.unionCard)
	}
	if facts.sumCard > 0 {
		candidateUpper = uint64(float64(candidateUpper) * (float64(facts.unionCard) / float64(facts.sumCard)))
		if candidateUpper == 0 {
			candidateUpper = 1
		}
	}
	work.BranchMerge = float64(plannerFieldStatsMergeRankRows(facts.orderStats, candidateUpper, need, facts.universe))
	return work
}

func (qv *View) estimateOROrderMergeRouteCostFacts(q *qir.Shape, facts *plannerORFacts, need int) (plannerOROrderRouteCost, bool) {
	if need <= 0 || q == nil || !q.HasOrder || facts.universe == 0 || facts.unionCard == 0 {
		return plannerOROrderRouteCost{}, false
	}

	headSensitiveOrderShape := facts.orderDistinct >= 64
	expectedRows := estimateRowsForNeed(uint64(need), facts.unionCard, facts.universe)
	if expectedRows == 0 {
		return plannerOROrderRouteCost{}, false
	}

	avgChecks := facts.avgMergeChecks()
	if avgChecks <= 0 {
		avgChecks = 1
	}
	overlap := float64(facts.sumCard) / float64(max(facts.unionCard, uint64(1)))
	if overlap < 1 {
		overlap = 1
	}
	if overlap > 16 {
		overlap = 16
	}

	hasPrefixTailRisk := facts.hasPrefixTailRisk()
	hasSelectiveLead := facts.hasSelectiveLead(need)
	offsetShare := 0.0
	if q.Offset > 0 {
		offsetShare = float64(q.Offset) / float64(need)
		if offsetShare > 1 {
			offsetShare = 1
		}
	}

	kWayRows := float64(expectedRows) * overlap
	if hasSelectiveLead {
		kWayRows *= 0.92
	}
	if offsetShare > 0 {
		kWayRows *= 1.0 + offsetShare*0.75
	}
	if hasPrefixTailRisk {
		kWayRows *= 1.15
	}
	if headSensitiveOrderShape && facts.orderStats.AvgBucketCard > float64(max(need, 1)) {
		avgBucket := facts.orderStats.AvgBucketCard
		headBucketAmp := ClampFloat((avgBucket/float64(max(need, 1))-1.0)*0.12, 0, 3.0)
		kWayRows *= 1.0 + headBucketAmp
	}

	kWayCost := kWayRows * (1.0 + avgChecks*0.55)
	fallbackCollectRows := 0.0
	activeBranches := 0
	directCollectFast := facts.orderView.HasData()
	for i := 0; i < facts.branchCount; i++ {
		card := facts.branchCards[i]
		if card == 0 {
			continue
		}
		activeBranches++
		branchUniverse := facts.universe
		if facts.mergeStats[i].rangeRows > 0 && facts.mergeStats[i].rangeRows < branchUniverse {
			branchUniverse = facts.mergeStats[i].rangeRows
		}
		probes := float64(estimateRowsForNeed(uint64(need), card, branchUniverse))
		if probes < float64(need) {
			probes = float64(need)
		}
		if offsetShare > 0 {
			probes *= 1.0 + offsetShare*0.35
		}
		if directCollectFast && headSensitiveOrderShape {
			if avgBucket := facts.orderStats.AvgBucketCard; avgBucket > float64(max(need, 1)) {
				headCollectFactor := ClampFloat((float64(max(need, 1))/avgBucket)*1.5, 0.20, 1.0)
				probes *= headCollectFactor
			}
		}
		fallbackCollectRows += probes
	}
	if activeBranches == 0 {
		activeBranches = facts.branchCount
	}
	fallbackCandidates := float64(min(facts.sumCard, uint64(need*activeBranches)))
	fallbackCost := fallbackCollectRows*(1.0+avgChecks*0.22) + fallbackCandidates*(1.0+overlap*0.08)
	if directCollectFast && headSensitiveOrderShape {
		fallbackCost *= 0.92
		if avgBucket := facts.orderStats.AvgBucketCard; avgBucket > float64(max(need, 1))*4.0 {
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

func (qv *View) estimateOROrderMergeRouteTraceWorkFacts(q *qir.Shape, facts *plannerORFacts, need int) (TraceRouteWork, TraceRouteWork, bool) {
	routeCost, ok := qv.estimateOROrderMergeRouteCostFacts(q, facts, need)
	if !ok {
		return TraceRouteWork{}, TraceRouteWork{}, false
	}

	expectedRows := estimateRowsForNeed(uint64(need), facts.unionCard, facts.universe)
	kWayRows := float64(expectedRows) * routeCost.overlap
	if routeCost.hasSelectiveLead {
		kWayRows *= 0.92
	}
	kWayBaseRows := kWayRows
	offsetShare := 0.0
	if q.Offset > 0 {
		offsetShare = float64(q.Offset) / float64(need)
		if offsetShare > 1 {
			offsetShare = 1
		}
		kWayRows *= 1.0 + offsetShare*0.75
	}
	if routeCost.hasPrefixTailRisk {
		kWayRows *= 1.15
	}
	if facts.orderDistinct >= 64 && facts.orderStats.AvgBucketCard > float64(max(need, 1)) {
		avgBucket := facts.orderStats.AvgBucketCard
		headBucketAmp := ClampFloat((avgBucket/float64(max(need, 1))-1.0)*0.12, 0, 3.0)
		kWayRows *= 1.0 + headBucketAmp
	}
	kWayWork := TraceRouteWork{
		CandidateScan:   kWayBaseRows,
		PostingContains: kWayBaseRows * routeCost.avgChecks * 0.55,
	}
	kWayBaseCost := plannerOROrderTraceWorkCost(kWayWork)
	kWayCost := kWayRows * (1.0 + routeCost.avgChecks*0.55)
	if kWayCost > kWayBaseCost {
		kWayWork.TailRiskPenalty = kWayCost - kWayBaseCost
	}

	fallbackCollectRows := 0.0
	activeBranches := 0
	directCollectFast := facts.orderView.HasData()
	for i := 0; i < facts.branchCount; i++ {
		card := facts.branchCards[i]
		if card == 0 {
			continue
		}
		activeBranches++
		branchUniverse := facts.universe
		if facts.mergeStats[i].rangeRows > 0 && facts.mergeStats[i].rangeRows < branchUniverse {
			branchUniverse = facts.mergeStats[i].rangeRows
		}
		probes := float64(estimateRowsForNeed(uint64(need), card, branchUniverse))
		if probes < float64(need) {
			probes = float64(need)
		}
		if offsetShare > 0 {
			probes *= 1.0 + offsetShare*0.35
		}
		if directCollectFast && facts.orderDistinct >= 64 {
			if avgBucket := facts.orderStats.AvgBucketCard; avgBucket > float64(max(need, 1)) {
				headCollectFactor := ClampFloat((float64(max(need, 1))/avgBucket)*1.5, 0.20, 1.0)
				probes *= headCollectFactor
			}
		}
		fallbackCollectRows += probes
	}
	if activeBranches == 0 {
		activeBranches = facts.branchCount
	}
	fallbackCandidates := float64(min(facts.sumCard, uint64(need*activeBranches)))
	fallbackFactor := 1.0
	if directCollectFast && facts.orderDistinct >= 64 {
		fallbackFactor *= 0.92
		if avgBucket := facts.orderStats.AvgBucketCard; avgBucket > float64(max(need, 1))*4.0 {
			fallbackFactor *= 0.90
		}
	}
	fallbackWork := TraceRouteWork{
		CandidateScan:   fallbackCollectRows * fallbackFactor,
		PostingContains: fallbackCollectRows * routeCost.avgChecks * 0.22 * fallbackFactor,
		BranchMerge:     fallbackCandidates * (1.0 + routeCost.overlap*0.08) * fallbackFactor,
	}
	return kWayWork, fallbackWork, true
}

func (qv *View) dispatchORMaterialized(q *qir.Shape, trace *Trace) ([]uint64, bool, error) {
	out, err := qv.queryMaterialized(q)
	if trace != nil {
		trace.SetPlan(PlanMaterialized)
	}
	return out, true, err
}

func (qv *View) dispatchORSelectedMaterialized(q *qir.Shape, decision plannerORDecision, trace *Trace) ([]uint64, bool, error) {
	switch decision.kind {
	case plannerORDecisionNoOrder:
		if decision.noOrder.selected.kind == plannerORNoOrderCandidateMaterializedFallback {
			return qv.dispatchORMaterialized(q, trace)
		}
	case plannerORDecisionOrder:
		if decision.order.selected.kind == plannerOROrderCandidateMaterializedFallback {
			return qv.dispatchORMaterialized(q, trace)
		}
	}
	return nil, true, fmt.Errorf("selected OR route is not materialized fallback")
}

func (qv *View) dispatchORFallback(q *qir.Shape, decision plannerORDecision, trace *Trace) ([]uint64, bool, error) {
	switch decision.kind {
	case plannerORDecisionNoOrder:
		if decision.noOrder.materializedFallback.kind == plannerORNoOrderCandidateMaterializedFallback {
			return qv.dispatchORMaterialized(q, trace)
		}
		return nil, true, fmt.Errorf("selected OR no-order route %s was not executable", decision.noOrder.selected.kind.String())
	case plannerORDecisionOrder:
		if decision.order.materializedFallback.kind == plannerOROrderCandidateMaterializedFallback {
			return qv.dispatchORMaterialized(q, trace)
		}
		return nil, true, fmt.Errorf("selected OR ordered route %s was not executable", decision.order.selected.kind.String())
	}
	return nil, false, nil
}

func (qv *View) dispatchOR(
	q *qir.Shape,
	facts *plannerORFacts,
	decision plannerORDecision,
	trace *Trace,
) ([]uint64, bool, error) {
	facts.branches.Release()
	if facts.hasAnalysis {
		facts.analysis.release()
		facts.hasAnalysis = false
	}

	if decision.kind == plannerORDecisionAlwaysFalse {
		return nil, true, nil
	}

	if decision.kind == plannerORDecisionNoOrder {
		selected := decision.noOrder.selectedKind()
		if selected == plannerORNoOrderCandidateNone {
			return nil, false, nil
		}
		if selected == plannerORNoOrderCandidateAdaptiveMerge ||
			selected == plannerORNoOrderCandidateBaselineMerge {
			branches, alwaysFalse, ok := qv.buildORBranches(q.Expr.Operands)
			if !ok {
				return qv.dispatchORFallback(q, decision, trace)
			}
			if alwaysFalse {
				return nil, true, nil
			}
			if plannerORNoOrderClassify(branches) == plannerORNoOrderModeUnsupported {
				branches.Release()
				return qv.dispatchORFallback(q, decision, trace)
			}
			facts.branches = branches
		}
		branches := facts.branches
		switch selected {
		case plannerORNoOrderCandidateUniverse:
			out := qv.execPlanORUniverseNoOrder(q, trace)
			if trace != nil {
				trace.SetPlan(PlanORMergeNoOrder)
			}
			return out, true, nil

		case plannerORNoOrderCandidateAdaptiveMerge:
			out, ok := qv.execPlanORNoOrderAdaptiveCore(q, branches, trace)
			if !ok {
				return qv.dispatchORFallback(q, decision, trace)
			}
			if trace != nil {
				trace.SetPlan(PlanORMergeNoOrder)
			}
			return out, true, nil

		case plannerORNoOrderCandidateBaselineMerge:
			out, ok := qv.execPlanORNoOrderBaselineCore(q, branches, trace)
			if !ok {
				return qv.dispatchORFallback(q, decision, trace)
			}
			if trace != nil {
				trace.SetPlan(PlanORMergeNoOrder)
			}
			return out, true, nil

		case plannerORNoOrderCandidateMaterializedFallback:
			return qv.dispatchORSelectedMaterialized(q, decision, trace)
		}
		return nil, false, nil
	}

	selected := decision.order.selectedKind()
	if selected == plannerOROrderCandidateNone {
		return nil, false, nil
	}
	if selected != plannerOROrderCandidateMaterializedFallback {
		window, _ := orderWindow(q)
		branches, alwaysFalse, ok := qv.buildORBranchesOrdered(q.Expr.Operands, qv.exec.FieldNameByOrdinal(q.Order.FieldOrdinal), window, q.Offset)
		if !ok {
			return qv.dispatchORFallback(q, decision, trace)
		}
		if alwaysFalse {
			return nil, true, nil
		}
		facts.branches = branches
		if selected == plannerOROrderCandidateKWayMerge || q.Offset > 0 {
			analysis, ok := qv.buildOROrderAnalysis(q, branches)
			if !ok {
				return qv.dispatchORFallback(q, decision, trace)
			}
			facts.analysis = analysis
			facts.hasAnalysis = true
		}
	}
	branches := facts.branches
	var analysis *plannerOROrderAnalysis
	if facts.hasAnalysis {
		analysis = &facts.analysis
	}
	if selected == plannerOROrderCandidateKWayMerge &&
		decision.order.rejected.kind == plannerOROrderCandidateStream &&
		(decision.order.selected.hasPrefixTailRisk ||
			(q.Offset > 0 && decision.order.selected.hasBroadResidual)) {
		if needWindow, ok := orderWindow(q); ok {
			sample, sampleOK := qv.sampleOROrderStream(q, branches, analysis, needWindow)
			if sampleOK {
				fallback, reason := plannerOROrderStreamSamplePrefersStream(
					needWindow,
					decision.order.selected,
					decision.order.rejected,
					sample,
				)
				if trace != nil {
					trace.SetOROrderSample(sample.examined, sample.matched, sample.buckets, sample.dropped, fallback, reason)
				}
				if fallback {
					selected = plannerOROrderCandidateStream
				}
			}
		}
	}
	switch selected {

	case plannerOROrderCandidateKWayMerge:
		if q.Offset > 0 {
			qv.maybeEagerMaterializeOrderedORPredicates(q, branches, analysis, true, trace)
		}
		out, ok, err := qv.execPlanOROrderKWayWithFallback(q, branches, analysis, decision.order.rejected, trace)
		if ok || err != nil {
			if ok && trace != nil {
				trace.SetPlan(PlanORMergeOrderMerge)
			}
			return out, ok, err
		}

		switch decision.order.rejected.kind {
		case plannerOROrderCandidateBranchCollect:
			if q.Offset > 0 {
				qv.maybeEagerMaterializeOrderedORPredicates(q, branches, analysis, false, trace)
			}
			out, ok, err = qv.execPlanOROrderMergeFallback(q, branches, trace)
			if ok && trace != nil {
				trace.SetPlan(PlanORMergeOrderMerge)
			}
			if ok || err != nil {
				return out, ok, err
			}

		case plannerOROrderCandidateStream:
			if q.Offset > 0 {
				qv.maybeEagerMaterializeOrderedORPredicates(q, branches, analysis, false, trace)
			}
			var observed orderedORObservedStats
			var observe *orderedORObservedStats
			cacheState := decision.order.selected.cacheState
			if trace == nil && qv.snap.MaterializedPredCacheLimit() > 0 &&
				cacheState != plannerMaterializedCacheDisabled &&
				!plannerMaterializedCacheStateUnretained(cacheState) {
				observe = &observed
			}
			out, ok = qv.execPlanOROrderBasic(q, branches, analysis, trace, observe)
			if observe != nil {
				observed.release()
			}
			if ok && trace != nil {
				trace.SetPlan(PlanORMergeOrderStream)
			}
			if ok {
				return out, true, nil
			}
		}
		return qv.dispatchORFallback(q, decision, trace)

	case plannerOROrderCandidateBranchCollect:
		if q.Offset > 0 {
			qv.maybeEagerMaterializeOrderedORPredicates(q, branches, analysis, false, trace)
		}
		out, ok, err := qv.execPlanOROrderMergeFallback(q, branches, trace)
		if ok {
			if trace != nil {
				trace.SetPlan(PlanORMergeOrderMerge)
			}
			return out, true, err
		}
		if err != nil {
			return nil, false, err
		}
		return qv.dispatchORFallback(q, decision, trace)

	case plannerOROrderCandidateStream:
		if q.Offset > 0 {
			qv.maybeEagerMaterializeOrderedORPredicates(q, branches, analysis, false, trace)
		}
		var observed orderedORObservedStats
		var observe *orderedORObservedStats
		cacheState := decision.order.selected.cacheState
		if trace == nil && qv.snap.MaterializedPredCacheLimit() > 0 &&
			cacheState != plannerMaterializedCacheDisabled &&
			!plannerMaterializedCacheStateUnretained(cacheState) {
			observe = &observed
		}
		out, ok := qv.execPlanOROrderBasic(q, branches, analysis, trace, observe)
		if observe != nil {
			observed.release()
		}
		if !ok {
			return qv.dispatchORFallback(q, decision, trace)
		}
		if trace != nil {
			trace.SetPlan(PlanORMergeOrderStream)
		}
		return out, true, nil

	case plannerOROrderCandidateMaterializedFallback:
		return qv.dispatchORSelectedMaterialized(q, decision, trace)
	}
	return nil, false, nil
}

type orderedORObservedStats struct {
	countsBuf     []uint64
	candidatesBuf []bool
	offsets       [plannerORBranchLimit + 1]int
	active        bool
}

func initOrderedORObservedStats(
	qv *View,
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
	observed.candidatesBuf = pooled.GetBoolSlice(total)[:total]
	clear(observed.candidatesBuf)

	var rememberKeysBuf []qcache.MaterializedPredKey
	if qv.snap != nil && qv.snap.MaterializedPredCacheLimit() > 0 {
		rememberKeysBuf = qcache.GetMaterializedPredKeySlice(total)
		defer qcache.ReleaseMaterializedPredKeySlice(rememberKeysBuf)
	}

	for bi := 0; bi < branchCount; bi++ {
		branch := branches.owner[bi]
		start := observed.offsets[bi]
		checks := branchChecks[bi]
		for ci, pi := range checks {
			p := branch.preds.owner[pi]
			info := qv.orderedORPredicateBuildInfoForBranch(field, p, analysis, branch, bi, pi)
			if qv.shouldObserveOrderedORPredicate(info) {
				observed.candidatesBuf[start+ci] = true
				active++
				continue
			}
			if rememberKeysBuf != nil && qv.shouldRememberColdOrderedORPredicate(info) {
				seen := false
				for i := 0; i < len(rememberKeysBuf); i++ {
					if rememberKeysBuf[i] == info.cacheKey {
						seen = true
						break
					}
				}
				if !seen {
					rememberKeysBuf = append(rememberKeysBuf, info.cacheKey)
				}
			}
		}
	}

	for i := 0; i < len(rememberKeysBuf); i++ {
		qv.snap.ShouldPromoteRuntimeMaterializedPredKey(rememberKeysBuf[i])
	}

	if active == 0 {
		observed.countsBuf = nil
		pooled.ReleaseBoolSlice(observed.candidatesBuf)
		observed.candidatesBuf = nil
		observed.active = false
		return
	}

	observed.countsBuf = pooled.GetUint64Slice(total)[:total]
	clear(observed.countsBuf)

	observed.active = true
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
	branchStart, branchEnd *[plannerORBranchLimit]int,
	branchChecks *[plannerORBranchLimit][]int,
	idx uint64,
	bucket int,
) bool {
	for i := 0; i < branchEvalN; i++ {
		bi := branchEvalOrder[i]
		if bucket < branchStart[bi] || bucket >= branchEnd[bi] {
			continue
		}
		if (&branches.owner[bi]).matchesChecks(idx, branchChecks[bi]) {
			return true
		}
	}
	return false
}

func plannerOROrderBasicMatchesObserved(
	branches plannerORBranches,
	branchEvalOrder *[plannerORBranchLimit]int,
	branchEvalN int,
	branchStart, branchEnd *[plannerORBranchLimit]int,
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
		if (&branches.owner[bi]).matchesChecksObservedBuf(idx, bi, branchChecks[bi], observed) {
			return true
		}
	}
	return false
}

func plannerOROrderBasicMatchWithMetrics(
	branches plannerORBranches,
	branchStart, branchEnd *[plannerORBranchLimit]int,
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
		if (&branches.owner[i]).matchesChecks(idx, branchChecks[i]) {
			branchMetrics[i].RowsEmitted++
			matchedCount++
		}
	}
	return matchedCount
}

func setOROrderMergeFallbackTrace(trace *Trace, examined, scanWidth uint64, branchMetrics []TraceORBranch, stopReason string) {
	if trace == nil {
		return
	}
	trace.AddExamined(examined)
	trace.AddOrderScanWidth(scanWidth)
	trace.SetORBranches(branchMetrics)
	trace.SetEarlyStopReason(stopReason)
}

func plannerOROrderStreamSampleTarget(need int) uint64 {
	target := uint64(need >> 3)
	if target < 4 {
		target = 4
	}
	if target > plannerOROrderStreamSampleMaxMatches {
		target = plannerOROrderStreamSampleMaxMatches
	}
	return target
}

func plannerOROrderStreamSamplePrefersStream(
	need int,
	selected plannerOROrderCandidate,
	stream plannerOROrderCandidate,
	sample plannerOROrderStreamSample,
) (bool, string) {
	if sample.examined == 0 {
		return false, "sample_empty"
	}
	target := plannerOROrderStreamSampleTarget(need)
	if sample.matched < target {
		if sample.examined < plannerOROrderStreamSampleMinRows {
			return false, "sample_short"
		}
		return false, "sample_sparse"
	}
	if selected.hasBroadResidual && !selected.hasPrefixTailRisk && sample.buckets <= 1 {
		return false, "sample_narrow"
	}
	avgChecks := stream.avgChecks
	if avgChecks <= 0 {
		avgChecks = 1
	}
	projectedCost := float64(sample.examined) * float64(need) * avgChecks / float64(sample.matched)
	if selected.cost > 0 && projectedCost <= selected.cost*1.10 {
		return true, "sample_stream_cost"
	}
	earlyRows := uint64(need * 4)
	if earlyRows < plannerOROrderStreamSampleMinRows {
		earlyRows = plannerOROrderStreamSampleMinRows
	}
	if sample.examined <= earlyRows {
		return true, "sample_stream_early"
	}
	return false, "sample_keep"
}

func (qv *View) sampleOROrderStream(
	q *qir.Shape,
	branches plannerORBranches,
	analysis *plannerOROrderAnalysis,
	need int,
) (plannerOROrderStreamSample, bool) {
	if q == nil || !q.HasOrder || analysis == nil || need <= 0 {
		return plannerOROrderStreamSample{}, false
	}
	o := q.Order
	f := qv.exec.FieldNameByOrdinal(o.FieldOrdinal)
	if o.FieldOrdinal < 0 {
		return plannerOROrderStreamSample{}, false
	}
	fm := qv.fieldMetaByOrder(o)
	if fm == nil || fm.Slice {
		return plannerOROrderStreamSample{}, false
	}
	ov := qv.fieldIndexViewFromSlotsForOrder(qv.snap.Index, o)
	if !ov.HasData() {
		return plannerOROrderStreamSample{}, false
	}

	branchCount := branches.Len()
	if branchCount > plannerORBranchLimit {
		branchCount = plannerORBranchLimit
	}

	alwaysTrue := false
	alwaysTrueBranch := -1
	var (
		branchChecks [plannerORBranchLimit][]int
		branchStart  [plannerORBranchLimit]int
		branchEnd    [plannerORBranchLimit]int
	)
	for i := 0; i < branchCount; i++ {
		branch := &branches.owner[i]
		if branch.alwaysTrue {
			alwaysTrue = true
			alwaysTrueBranch = i
		}
		if i < analysis.branchCount {
			covered := analysis.branches[i].covered
			branchStart[i] = analysis.branches[i].rangeStart
			branchEnd[i] = analysis.branches[i].rangeEnd
			for pi := 0; pi < len(covered); pi++ {
				if covered[pi] {
					branch.predPtr(pi).covered = true
				}
			}
		} else {
			br, covered, ok := qv.extractOrderRangeCoverageIndexViewReader(f, branch.preds.owner, ov)
			if !ok {
				releasePlannerOROrderBasicCheckBufs(branchCount, &branchChecks)
				return plannerOROrderStreamSample{}, false
			}
			branchStart[i], branchEnd[i] = br.BaseStart, br.BaseEnd
			for pi := 0; pi < len(covered); pi++ {
				if covered[pi] {
					branch.predPtr(pi).covered = true
				}
			}
			pooled.ReleaseBoolSlice(covered)
		}
		branchChecks[i] = pooled.GetIntSlice(branch.preds.Len())
		branchChecks[i] = branch.buildMatchChecks(branchChecks[i])
	}
	defer releasePlannerOROrderBasicCheckBufs(branchCount, &branchChecks)

	scanStart := ov.KeyCount()
	scanEnd := 0
	for i := 0; i < branchCount; i++ {
		if branchStart[i] < scanStart {
			scanStart = branchStart[i]
		}
		if branchEnd[i] > scanEnd {
			scanEnd = branchEnd[i]
		}
	}
	if scanStart >= scanEnd {
		return plannerOROrderStreamSample{}, true
	}

	var branchMetrics [plannerORBranchLimit]TraceORBranch
	br := ov.RangeByRanks(scanStart, scanEnd)
	cur := ov.NewCursor(br, o.Desc)
	bucket := br.BaseStart
	if o.Desc {
		bucket = br.BaseEnd - 1
	}
	target := plannerOROrderStreamSampleTarget(need)
	var sample plannerOROrderStreamSample
	for sample.buckets < plannerOROrderStreamSampleMaxBuckets && sample.examined < plannerOROrderStreamSampleMaxRows {
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
		sample.buckets++
		if idx, ok := bm.TrySingle(); ok {
			sample.examined++
			matchedCount := plannerOROrderBasicMatchWithMetrics(
				branches,
				&branchStart,
				&branchEnd,
				&branchChecks,
				branchMetrics[:branchCount],
				idx,
				curBucket,
				alwaysTrue,
				alwaysTrueBranch,
			)
			if matchedCount > 0 {
				sample.matched++
				if matchedCount > 1 {
					sample.dropped += uint64(matchedCount - 1)
				}
				if sample.matched >= target {
					break
				}
			}
			continue
		}
		it := bm.Iter()
		for it.HasNext() && sample.examined < plannerOROrderStreamSampleMaxRows {
			sample.examined++
			matchedCount := plannerOROrderBasicMatchWithMetrics(
				branches,
				&branchStart,
				&branchEnd,
				&branchChecks,
				branchMetrics[:branchCount],
				it.Next(),
				curBucket,
				alwaysTrue,
				alwaysTrueBranch,
			)
			if matchedCount == 0 {
				continue
			}
			sample.matched++
			if matchedCount > 1 {
				sample.dropped += uint64(matchedCount - 1)
			}
			if sample.matched >= target {
				break
			}
		}
		it.Release()
		if sample.matched >= target {
			break
		}
	}
	return sample, true
}

func (qv *View) orderedORMaterializedRangeLeafCosts(
	orderField string,
	leaf qir.Expr,
) (qcache.MaterializedPredKey, uint64, uint64, uint64, bool) {
	candidate, ok := qv.prepareScalarRangeRoutingCandidate(leaf)
	if !ok || !candidate.numeric || qv.exec.FieldNameByOrdinal(leaf.FieldOrdinal) == orderField {
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

func (qv *View) orderedORMaterializedExactRangePredicateCosts(orderField string, p predicate) (qcache.MaterializedPredKey, uint64, uint64, uint64, bool) {

	if !p.hasEffectiveBounds || p.expr.FieldOrdinal < 0 {
		return qcache.MaterializedPredKey{}, 0, 0, 0, false
	}
	fieldName := qv.exec.FieldNameByOrdinal(p.expr.FieldOrdinal)
	if fieldName == orderField {
		return qcache.MaterializedPredKey{}, 0, 0, 0, false
	}
	fm := qv.fieldMetaByExpr(p.expr)
	if !schema.FieldUsesOrderedNumericKeys(fm) {
		return qcache.MaterializedPredKey{}, 0, 0, 0, false
	}

	var core preparedScalarRangePredicate
	qv.initPreparedExactScalarRangePredicate(&core, p.expr, fm, p.effectiveBounds)
	ov := qv.fieldIndexViewFromSlotsForExpr(qv.snap.Index, p.expr)
	if !ov.HasData() {
		return qcache.MaterializedPredKey{}, 0, 0, 0, false
	}

	plan, _, done := core.planFieldIndexRange(ov)
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

func (qv *View) orderedORMaterializedPrefixLeafBuildWork(orderField string, leaf qir.Expr) (qcache.MaterializedPredKey, uint64, bool) {
	if !qv.isPositiveNonOrderScalarPrefixLeaf(orderField, leaf) {
		return qcache.MaterializedPredKey{}, 0, false
	}

	candidate, ok := qv.prepareScalarRangeRoutingCandidate(leaf)
	if !ok || candidate.plan.useComplement || candidate.core.sharedReuse.cacheKey.IsZero() {
		return qcache.MaterializedPredKey{}, 0, false
	}

	core := candidate.core
	snap := qv.exec.Stats.Load()
	universe := max(qv.snap.Universe.Cardinality(), uint64(1))

	sel, _, _, _, ok := qv.estimateLeafOrderCost(leaf, snap, universe, orderField, qv.fieldIndexViewFromSlotsByName(qv.snap.Index, orderField).HasData())
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

func (qv *View) orderedORMaterializedPredicateBuildInfo(orderField string, p predicate) orderedORMaterializedPredicateBuildInfo {

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

func (qv *View) buildOROrderPredicateBuildInfos(orderField string, branch plannerORBranch, covered []bool) []orderedORMaterializedPredicateBuildInfo {

	if branch.preds.Len() == 0 {
		return nil
	}
	buf := plannerORPredicateBuildInfoSlicePool.Get(branch.preds.Len())[:branch.preds.Len()]
	anyInfo := false
	for pi := 0; pi < branch.preds.Len(); pi++ {
		p := branch.preds.owner[pi]
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
		buf[pi] = info
		anyInfo = true
	}
	if !anyInfo {
		plannerORPredicateBuildInfoSlicePool.Put(buf)
		return nil
	}
	return buf
}

func (qv *View) orderedORPredicateBuildInfoForBranch(
	orderField string,
	p predicate,
	analysis *plannerOROrderAnalysis,
	branch plannerORBranch,
	branchIdx, predIdx int,
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

func (qv *View) materializeOrderedORPredicate(p *predicate) bool {
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

func (qv *View) materializePositiveScalarRangePredicate(p *predicate) bool {
	if p == nil {
		return false
	}
	candidate, ok := qv.preparePredicateScalarRangeRoutingCandidate(*p)
	if !ok {
		return false
	}
	return qv.materializePositiveScalarRangePredicateWithCandidate(p, candidate)
}

func (qv *View) materializePositiveScalarRangePredicateWithCandidate(p *predicate, candidate preparedScalarRangeRoutingCandidate) bool {
	ov := qv.fieldIndexViewFromSlotsForExpr(qv.snap.Index, p.expr)
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

func (qv *View) initOrderedORBranchEstimates(branches plannerORBranches, branchUniverses *[plannerORBranchLimit]uint64, needWindow int, out *[plannerORBranchLimit]plannerOROrderedBranchEstimate) {
	if out == nil {
		return
	}
	*out = [plannerORBranchLimit]plannerOROrderedBranchEstimate{}
	if needWindow <= 0 || branchUniverses == nil {
		return
	}
	snapshotUniverse := qv.snap.Universe.Cardinality()
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
		card := branches.owner[i].estimatedCardWithinOrderedUniverse(snapshotUniverse, universe)
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

func (qv *View) orderedORMaterializedPredicateRequiresPromotion(p predicate, branchNeed, orderedOffset, branchUniverse uint64) bool {
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

func (qv *View) shouldEagerMaterializeOrderedORPredicate(
	p predicate,
	info orderedORMaterializedPredicateBuildInfo,
	est plannerOROrderedBranchEstimate,
	orderedOffset, leafChecks uint64,
) (bool, bool) {

	if !info.ok || info.buildWork == 0 {
		return false, false
	}
	if !info.cacheKey.IsZero() && qv.snap != nil {
		if qv.snap.HasMaterializedPredKey(info.cacheKey) {
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

func (qv *View) maybeEagerMaterializeOrderedORPredicates(
	q *qir.Shape,
	branches plannerORBranches,
	analysis *plannerOROrderAnalysis,
	merge bool,
	trace *Trace,
) bool {
	return qv.maybeMaterializeOrderedORPredicates(q, branches, analysis, merge, true, trace)
}

func (qv *View) maybeMaterializeOrderedORPredicates(
	q *qir.Shape,
	branches plannerORBranches,
	analysis *plannerOROrderAnalysis,
	merge, allowBuild bool,
	trace *Trace,
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
	recordAnalysis := trace != nil && trace.Full()
	if recordAnalysis {
		analysisStarted = time.Now()
	}
	considered := uint64(0)
	cacheHits := uint64(0)
	builds := uint64(0)
	defer func() {
		if recordAnalysis {
			trace.AddOROrderPlannerAnalysis(
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
		branch := &branches.owner[bi]
		est := estimates[bi]
		rows := est.probeRows
		if rows == 0 || est.universe == 0 {
			continue
		}

		covered := analysis.branches[bi].covered
		fullChecks := pooled.GetIntSlice(branch.preds.Len())
		fullChecks = plannerORBranchBuildActiveChecksWithCovered(fullChecks, branch, covered)
		branchDone := false

		if !merge {
			for _, pi := range fullChecks {
				if rows == 0 {
					break
				}

				p := branch.preds.owner[pi]
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
						p = branch.preds.owner[pi]
						if p.alwaysTrue || p.alwaysFalse {
							analysis.refreshBranch(branches, bi)
							branch = &branches.owner[bi]
							if branch.alwaysTrue ||
								(branch.estKnown && branch.estCard == 0) ||
								analysis.mergeStats[bi].streamChecks == 0 {
								branchDone = true
								break
							}
						}

					} else if allowBuild {
						if candidateMarks[bi] == nil {
							candidateMarks[bi] = pooled.GetBoolSlice(branch.preds.Len())[:branch.preds.Len()]
							clear(candidateMarks[bi])
						}
						candidateMarks[bi][pi] = true
						hasBuildCandidates = true
					}
				}
				rows = orderedORNextPredicateRows(rows, p.estCard, est.card, est.universe)
			}

			pooled.ReleaseIntSlice(fullChecks)

			continue
		}

		exactChecks := pooled.GetIntSlice(len(fullChecks))
		exactChecks = buildExactBucketPostingFilterActiveReader(exactChecks, fullChecks, branch.preds.owner)
		residualChecks := pooled.GetIntSlice(len(fullChecks))
		residualChecks = plannerResidualChecks(residualChecks, fullChecks, exactChecks)
		pooled.ReleaseIntSlice(fullChecks)

		for _, pi := range exactChecks {
			if rows == 0 {
				break
			}
			p := branch.preds.owner[pi]
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
					p = branch.preds.owner[pi]
					if p.alwaysTrue || p.alwaysFalse {
						analysis.refreshBranch(branches, bi)
						branch = &branches.owner[bi]
						if branch.alwaysTrue ||
							(branch.estKnown && branch.estCard == 0) ||
							analysis.mergeStats[bi].streamChecks == 0 {
							branchDone = true
							break
						}
					}

				} else if allowBuild {
					if candidateMarks[bi] == nil {
						candidateMarks[bi] = pooled.GetBoolSlice(branch.preds.Len())[:branch.preds.Len()]
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

				p := branch.preds.owner[pi]
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
						p = branch.preds.owner[pi]
						if p.alwaysTrue || p.alwaysFalse {
							analysis.refreshBranch(branches, bi)
							branch = &branches.owner[bi]
							if branch.alwaysTrue ||
								(branch.estKnown && branch.estCard == 0) ||
								analysis.mergeStats[bi].streamChecks == 0 {
								branchDone = true
								break
							}
						}

					} else if allowBuild {
						if candidateMarks[bi] == nil {
							candidateMarks[bi] = pooled.GetBoolSlice(branch.preds.Len())[:branch.preds.Len()]
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
			branch := &branches.owner[bi]
			for pi := 0; pi < branch.preds.Len(); pi++ {
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
	defer releasePlannerOROrderBasicCheckBufs(branchCount, &branchChecks)

	var branchExactChecks [plannerORBranchLimit][]int
	defer releasePlannerOROrderBasicCheckBufs(branchCount, &branchExactChecks)

	for bi := 0; bi < branchCount; bi++ {
		if candidateMarks[bi] == nil {
			continue
		}
		branch := &branches.owner[bi]
		fullChecks := pooled.GetIntSlice(branch.preds.Len())
		fullChecks = plannerORBranchBuildActiveChecksWithCovered(fullChecks, branch, analysis.branches[bi].covered)
		exactChecks := pooled.GetIntSlice(len(fullChecks))
		exactChecks = buildExactBucketPostingFilterActiveReader(exactChecks, fullChecks, branch.preds.owner)
		branchExactChecks[bi] = exactChecks
		residualChecks := pooled.GetIntSlice(len(fullChecks))
		residualChecks = plannerResidualChecks(residualChecks, fullChecks, exactChecks)
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
		branch := &branches.owner[bi]
		est := estimates[bi]
		rows := est.probeRows
		if est.universe == 0 {
			continue
		}

		for _, pi := range exactChecks {
			if rows == 0 {
				break
			}
			p := branch.preds.owner[pi]
			if p.alwaysFalse || p.alwaysTrue || p.covered {
				rows = orderedORNextPredicateRows(rows, p.estCard, est.card, est.universe)
				continue
			}
			if marks[pi] && qv.materializeOrderedORPredicate(branch.predPtr(pi)) {
				builds++
				dirtyBranches[bi] = true
				changed = true
				p = branch.preds.owner[pi]
			}
			rows = orderedORNextPredicateRows(rows, p.estCard, est.card, est.universe)
		}

		for _, pi := range checks {
			if rows == 0 {
				break
			}
			p := branch.preds.owner[pi]
			if p.alwaysFalse || p.alwaysTrue || p.covered {
				rows = orderedORNextPredicateRows(rows, p.estCard, est.card, est.universe)
				continue
			}
			if marks[pi] && qv.materializeOrderedORPredicate(branch.predPtr(pi)) {
				builds++
				dirtyBranches[bi] = true
				changed = true
				p = branch.preds.owner[pi]
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

func (qv *View) promoteOrderedORMaterializedBaseOps(
	q *qir.Shape,
	branches plannerORBranches,
	branchChecks [plannerORBranchLimit][]int,
	observed *orderedORObservedStats,
	analysis *plannerOROrderAnalysis,
) {
	if q == nil || branches.Len() == 0 || qv.snap == nil || !q.HasOrder || observed == nil {
		return
	}
	if qv.snap.MaterializedPredCacheLimit() <= 0 {
		return
	}
	orderField := qv.exec.FieldNameByOrdinal(q.Order.FieldOrdinal)
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
		branch := branches.owner[bi]
		checks := branchChecks[bi]
		start, end, ok := observed.branchRange(bi)
		if !ok {
			continue
		}
		for ci, pi := range checks {
			p := branch.preds.owner[pi]
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
		if qv.snap.HasMaterializedPredKey(cacheKeysBuf[i]) {
			continue
		}
		if !qv.snap.ShouldPromoteObservedMaterializedPredKey(cacheKeysBuf[i], observedWorksBuf[i], buildWorksBuf[i]) {
			continue
		}
		branchIdx := repBranchBuf[i]
		predIdx := repPredBuf[i]
		qv.materializeOrderedORPredicate((&branches.owner[branchIdx]).predPtr(predIdx))
	}
}

func (qv *View) promoteObservedOrderedORKWayMaterializedBaseOps(
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
	if qv.snap.MaterializedPredCacheLimit() <= 0 {
		return
	}
	needWindow, ok := orderWindow(q)
	if !ok || needWindow <= 0 {
		return
	}
	orderField := qv.exec.FieldNameByOrdinal(q.Order.FieldOrdinal)
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
		branch := branches.owner[bi]
		for _, pi := range checks {
			if rows == 0 {
				break
			}
			p := branch.preds.owner[pi]
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
		if qv.snap.HasMaterializedPredKey(cacheKeysBuf[i]) {
			continue
		}
		if !qv.snap.ShouldPromoteObservedMaterializedPredKey(cacheKeysBuf[i], observedWorksBuf[i], buildWorksBuf[i]) {
			continue
		}
		branchIdx := repBranchBuf[i]
		predIdx := repPredBuf[i]
		qv.materializeOrderedORPredicate((&branches.owner[branchIdx]).predPtr(predIdx))
	}
}

func (qv *View) shouldObserveOrderedORPredicate(info orderedORMaterializedPredicateBuildInfo) bool {
	if !orderedORPredicateObservationEligible(info) {
		return false
	}
	if qv.snap.HasMaterializedPredKey(info.cacheKey) {
		return false
	}
	if qv.snap.HasRuntimeMaterializedPredSeenKey(info.cacheKey) {
		return true
	}
	observedWork := qv.snap.ObservedMaterializedPredWork(info.cacheKey)
	return (observedWork > 0 && observedWork < info.buildWork) ||
		qv.snap.DirtyObservedMaterializedPredWork(info.cacheKey) > 0
}

func (qv *View) shouldRememberColdOrderedORPredicate(info orderedORMaterializedPredicateBuildInfo) bool {
	if !orderedORPredicateObservationEligible(info) {
		return false
	}
	if qv.snap.HasMaterializedPredKey(info.cacheKey) {
		return false
	}
	return !qv.snap.HasRuntimeMaterializedPredSeenKey(info.cacheKey)
}

func orderedORPredicateObservationEligible(info orderedORMaterializedPredicateBuildInfo) bool {
	if !info.ok || info.cacheKey.IsZero() || info.buildWork == 0 {
		return false
	}
	return info.isPrefix || info.checkWork != 0
}

func (qv *View) shouldKeepORBranchNumericRangeLazy(e qir.Expr) bool {
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
	universe := qv.snap.Universe.Cardinality()
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

func (qv *View) shouldForceORBranchNumericRangeMaterializeWithCandidate(
	candidate preparedScalarRangeRoutingCandidate,
	_ uint64,
) bool {
	return candidate.plan.useComplement &&
		candidate.core.qv.fieldIndexViewFromSlotsForExpr(candidate.core.qv.snap.NilIndex, candidate.core.expr).LookupCardinality(indexdata.NilIndexEntryKey) > 0
}

func (qv *View) shouldForceORBranchNumericRangeMaterialize(e qir.Expr) bool {
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
	return qv.shouldForceORBranchNumericRangeMaterializeWithCandidate(candidate, qv.snap.Universe.Cardinality())
}

func (qv *View) shouldBuildORBranchLeafLazy(e qir.Expr, branchLeafCount int) bool {
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
	universe := qv.snap.Universe.Cardinality()
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
		if cand, ok := qv.prepareScalarRangeRoutingCandidate(e); ok &&
			cand.plan.useComplement &&
			!cand.core.complementCacheKey.IsZero() {
			qv.snap.ShouldPromoteRuntimeMaterializedPredKey(cand.core.complementCacheKey)
		}
		return true
	}
	return !hot
}

func (qv *View) mayNeedORBranchRangeRewrite(leaves []qir.Expr) bool {
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

func (qv *View) buildORBranchPredicates(leaves []qir.Expr) (predicateSet, bool) {
	if !qv.mayNeedORBranchRangeRewrite(leaves) {
		return qv.buildPredicatesWithColdMode(leaves, true, false)
	}

	preds := newPredicateSet(len(leaves))

	var mergedRangesBuf []orderedMergedScalarRangeField
	if len(leaves) > 1 {
		mergedRangesBuf = orderedMergedScalarRangeFieldSlicePool.Get(len(leaves))
		var ok bool
		mergedRangesBuf, ok = qv.collectMergedNumericRangeFields(leaves, mergedRangesBuf)
		if !ok {
			preds.Release()
			orderedMergedScalarRangeFieldSlicePool.Put(mergedRangesBuf)
			return predicateSet{}, false
		}
		defer orderedMergedScalarRangeFieldSlicePool.Put(mergedRangesBuf)
	}

	forced := pooled.GetIntSlice(len(leaves))
	defer pooled.ReleaseIntSlice(forced)

	leadIdx := -1
	leadEst := uint64(0)
	for i, e := range leaves {
		merged := false
		if qv.isPositiveMergedNumericRangeLeaf(e) && mergedRangesBuf != nil {
			idx := findOrderedMergedScalarRangeField(mergedRangesBuf, qv.exec.FieldNameByOrdinal(e.FieldOrdinal))
			if idx >= 0 {
				group := mergedRangesBuf[idx]
				if group.count > 1 {
					if group.first != i {
						continue
					}
					e = group.expr
					p, ok := qv.buildMergedScalarRangePredicateWithColdMode(group.expr, group.bounds, true, qv.shouldBuildORBranchLeafLazy(group.expr, len(leaves)))
					if !ok {
						preds.Release()
						return predicateSet{}, false
					}
					preds.Append(p)
					pi := len(preds.owner) - 1
					if qv.shouldForceORBranchNumericRangeMaterialize(group.expr) && p.hasRuntimeRangeState() {
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
					merged = true
				}
			}
		}
		if merged {
			continue
		}

		forceMaterialize := qv.shouldForceORBranchNumericRangeMaterialize(e)
		p, ok := qv.buildPredicateWithColdMode(e, true, qv.shouldBuildORBranchLeafLazy(e, len(leaves)))
		if !ok {
			preds.Release()
			return predicateSet{}, false
		}
		if p.hasRuntimeRangeState() && e.Op.IsNumericRange() {
			bounds, ok, err := qv.rangeBoundsForScalarExpr(e)
			if err != nil || !ok {
				preds.Release()
				return predicateSet{}, false
			}
			p.effectiveBounds = bounds
			p.hasEffectiveBounds = true
		}
		preds.Append(p)
		pi := len(preds.owner) - 1
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
		p := &preds.owner[pi]
		if !qv.materializePositiveScalarRangePredicate(p) {
			preds.Release()
			return predicateSet{}, false
		}
	}
	return preds, true
}

// buildORBranches compiles each OR branch into planner predicates and optional
// lead iterators used by OR execution strategies.
func (qv *View) buildORBranches(ops []qir.Expr) (plannerORBranches, bool, bool) {
	out := newPlannerORBranches(len(ops))
	var leavesBuf [plannerPredicateFastPathMaxLeaves]qir.Expr

	for _, op := range ops {
		leaves, leavesHeap, ok := collectORBranchLeaves(op, leavesBuf[:0])
		if !ok {
			out.Release()
			return plannerORBranches{}, false, false
		}

		preds, ok := qv.buildORBranchPredicates(leaves)
		if leavesHeap != nil {
			qir.ReleaseExprSlice(leavesHeap)
		}
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

func (qv *View) buildORBranchesOrdered(
	ops []qir.Expr,
	orderField string,
	orderedWindow int,
	orderedOffset uint64,
) (plannerORBranches, bool, bool) {

	out := newPlannerORBranches(len(ops))
	var leavesBuf [plannerPredicateFastPathMaxLeaves]qir.Expr

	for _, op := range ops {
		leaves, leavesHeap, ok := collectORBranchLeaves(op, leavesBuf[:0])
		if !ok {
			out.Release()
			return plannerORBranches{}, false, false
		}

		// Ordered OR merge consumes predicates incrementally from order buckets.
		// Eager materializing non-order range predicates here can dominate total
		// query cost before branch streaming even starts.
		preds, ok := qv.buildPredicatesOrderedWithMode(leaves, orderField, false, orderedWindow, orderedOffset, true, false)
		if leavesHeap != nil {
			qir.ReleaseExprSlice(leavesHeap)
		}
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
	ov := qv.fieldIndexViewFromSlotsByName(qv.snap.Index, orderField)
	if !ov.HasData() {
		return branches, false, true
	}
	for i := 0; i < branches.Len(); i++ {
		branch := &branches.owner[i]
		if !branch.alwaysTrue || branch.preds.Len() == 0 {
			continue
		}
		br, covered, rangeOK := qv.extractOrderRangeCoverageIndexViewReader(orderField, branch.preds.owner, ov)
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
func (qv *View) execPlanOROrderBasic(q *qir.Shape, branches plannerORBranches, analysis *plannerOROrderAnalysis, trace *Trace, observed *orderedORObservedStats) ([]uint64, bool) {
	o := q.Order
	f := qv.exec.FieldNameByOrdinal(o.FieldOrdinal)
	if o.FieldOrdinal < 0 {
		return nil, false
	}

	fm := qv.fieldMetaByOrder(o)
	if fm == nil || fm.Slice {
		return nil, false
	}

	ov := qv.fieldIndexViewFromSlotsForOrder(qv.snap.Index, o)
	if !ov.HasData() {
		return nil, false
	}

	alwaysTrue := false
	alwaysTrueBranch := -1
	for i := 0; i < branches.Len(); i++ {
		if branches.owner[i].alwaysTrue {
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
		branch := &branches.owner[i]
		var covered []bool
		if analysis != nil && i < analysis.branchCount {
			covered = analysis.branches[i].covered
			branchStart[i] = analysis.branches[i].rangeStart
			branchEnd[i] = analysis.branches[i].rangeEnd
		} else {
			br, rangeCovered, ok := qv.extractOrderRangeCoverageIndexViewReader(f, branch.preds.owner, ov)
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
		branchChecks[i] = pooled.GetIntSlice(branch.preds.Len())
		branchChecks[i] = branch.buildMatchChecks(branchChecks[i])
	}
	scanStart := ov.KeyCount()
	scanEnd := 0
	for i := 0; i < branches.Len(); i++ {
		if branchStart[i] < scanStart {
			scanStart = branchStart[i]
		}
		if branchEnd[i] > scanEnd {
			scanEnd = branchEnd[i]
		}
	}
	if scanStart >= scanEnd {
		releasePlannerOROrderBasicCheckBufs(branchCount, &branchChecks)
		return nil, true
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
	need := clampUint64ToInt(q.Limit)
	out := make([]uint64, 0, need)

	var branchEvalOrder [plannerORBranchLimit]int
	branchEvalN := 0
	if !alwaysTrue {
		// Reordering branches is a fail-fast optimization: cheap/selective
		// branches are checked first for the common negative case.
		branchEvalOrder, branchEvalN = branches.evalOrder()
	}

	br := ov.RangeByRanks(scanStart, scanEnd)
	fullTrace := trace != nil && trace.Full()

	if !fullTrace {
		cur := ov.NewCursor(br, o.Desc)
		bucket := br.BaseStart
		if o.Desc {
			bucket = br.BaseEnd - 1
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
				trace.AddExamined(bm.Cardinality())
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
	bucket := br.BaseStart
	if o.Desc {
		bucket = br.BaseEnd - 1
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
		trace.AddExamined(bm.Cardinality())
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
				trace.AddDedupe(uint64(matchedCount - 1))
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
				trace.AddDedupe(uint64(matchedCount - 1))
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

	trace.AddOrderScanWidth(scanWidth)
	trace.SetORBranches(branchMetrics)
	trace.SetEarlyStopReason(stopReason)
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

func (qv *View) shouldUseOROrderKWayRuntimeFallback(q *qir.Shape, branches plannerORBranches, needWindow int) bool {
	d, ok := qv.decideOROrderKWayRuntimeFallback(q, branches, needWindow)
	return ok && d.enable
}

func (qv *View) decideOROrderKWayRuntimeFallback(q *qir.Shape, branches plannerORBranches, needWindow int) (plannerOROrderRuntimeGuardDecision, bool) {
	return qv.decideOROrderKWayRuntimeFallbackWithAnalysis(q, branches, needWindow, nil)
}

func (qv *View) decideOROrderKWayRuntimeFallbackWithAnalysis(
	q *qir.Shape,
	branches plannerORBranches,
	needWindow int,
	analysis *plannerOROrderAnalysis,
) (plannerOROrderRuntimeGuardDecision, bool) {
	return qv.decideOROrderKWayRuntimeFallbackWithAnalysisAndFallback(q, branches, needWindow, analysis, plannerOROrderCandidate{})
}

func (qv *View) decideOROrderKWayRuntimeFallbackWithAnalysisAndFallback(
	q *qir.Shape,
	branches plannerORBranches,
	needWindow int,
	analysis *plannerOROrderAnalysis,
	selectorFallback plannerOROrderCandidate,
) (plannerOROrderRuntimeGuardDecision, bool) {
	if needWindow <= 0 || !q.HasOrder || branches.Len() < 2 {
		return plannerOROrderRuntimeGuardDecision{}, false
	}

	order := q.Order
	orderField := qv.exec.FieldNameByOrdinal(order.FieldOrdinal)
	if order.FieldOrdinal < 0 {
		return plannerOROrderRuntimeGuardDecision{}, false
	}

	ov := qv.fieldIndexViewFromSlotsForOrder(qv.snap.Index, order)

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
	fallbackCost := routeCost.fallback
	if selectorFallback.kind != plannerOROrderCandidateNone {
		switch selectorFallback.kind {
		case plannerOROrderCandidateStream, plannerOROrderCandidateBranchCollect:
			if selectorFallback.cost <= 0 {
				return plannerOROrderRuntimeGuardDecision{}, false
			}
			fallbackCost = selectorFallback.cost
		default:
			d := plannerOROrderRuntimeGuardDecision{
				routeCost: routeCost,
				avgChecks: avgChecks,
				enable:    false,
				reason:    "no_selector_runtime_fallback",
			}
			return d, true
		}
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
		routeCost.kWay < fallbackCost*0.9 {
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

	if routeCost.kWay >= fallbackCost*nearTieGain {
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
	return plannerORKWayShouldFallbackRuntimeDetailedWithProgress(needWindow, pops, unique, examined, 0, 0, 0, shape)
}

func plannerORKWayShouldFallbackRuntimeDetailedWithProgress(
	needWindow, pops int,
	unique, examined, dedupe, maxBranchExamined uint64,
	activeBranches int,
	shape plannerORKWayRuntimeShape,
) plannerORKWayRuntimeDecision {
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

	if dedupe > 0 {
		seen := unique + dedupe
		dropRate := float64(dedupe) / float64(seen)
		projectedPops := float64(pops) * float64(needWindow) / float64(unique)
		if dropRate >= plannerORKWayRuntimeDuplicateRateMin &&
			projectedPops >= float64(needWindow)*2.5 &&
			examinedPerUnique >= max(128.0, examinedPerUniqueMin*0.35) {
			d.fallback = true
			d.reason = "duplicate_drop_projected_work"
			return d
		}
	}

	if activeBranches > 1 && maxBranchExamined > 0 && examined > 0 {
		imbalance := float64(maxBranchExamined) * float64(activeBranches) / float64(examined)
		if imbalance >= plannerORKWayRuntimeBranchImbalanceMin &&
			examinedPerUnique >= max(192.0, examinedPerUniqueMin*0.45) &&
			projectedExamined >= projectedExaminedMax*0.60 {
			d.fallback = true
			d.reason = "branch_imbalance_projected_work"
			return d
		}
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

	if projectedExamined >= projectedExaminedMax {
		d.fallback = true
		d.reason = "projected_examined_limit"
		return d
	}
	d.reason = "projected_examined_ok"
	return d
}

type plannerOROrderMergeItem struct {
	branch int
	bucket int
	idx    uint64
}

func plannerORKWayBranchProgress(rows *[plannerORBranchLimit]uint64, n int) (uint64, int) {
	if n > plannerORBranchLimit {
		n = plannerORBranchLimit
	}
	maxRows := uint64(0)
	active := 0
	for i := 0; i < n; i++ {
		r := rows[i]
		if r == 0 {
			continue
		}
		active++
		if r > maxRows {
			maxRows = r
		}
	}
	return maxRows, active
}

type plannerOROrderMergeHeap struct {
	desc     bool
	items    []plannerOROrderMergeItem
	itemsBuf []plannerOROrderMergeItem
}

func (h *plannerOROrderMergeHeap) len() int {
	if h.itemsBuf != nil {
		return len(h.itemsBuf)
	}
	return len(h.items)
}

func (h *plannerOROrderMergeHeap) item(i int) plannerOROrderMergeItem {
	if h.itemsBuf != nil {
		return h.itemsBuf[i]
	}
	return h.items[i]
}

func (h *plannerOROrderMergeHeap) setItem(i int, item plannerOROrderMergeItem) {
	if h.itemsBuf != nil {
		h.itemsBuf[i] = item
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
		// keep deterministic in-bucket ordering aligned with roaring iterator
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
		h.itemsBuf = append(h.itemsBuf, x)
		h.up(len(h.itemsBuf) - 1)
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
		h.itemsBuf = h.itemsBuf[:n]
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
	buf    []plannerOROrderBranchIter
}

func (s *plannerOROrderIterStore) get(i int) plannerOROrderBranchIter {
	if s.buf != nil {
		return s.buf[i]
	}
	return s.inline[i]
}

func (s *plannerOROrderIterStore) set(i int, iter plannerOROrderBranchIter) {
	if s.buf != nil {
		s.buf[i] = iter
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
	indexView      indexdata.FieldIndexView
	desc           bool
	single         int
	exactSingle    int
	residualSingle int
	allChecksExact bool

	startBucket int
	endBucket   int

	nextBucket    int
	curBucket     int
	curCursor     indexdata.FieldIndexCursor
	curIter       posting.Iterator
	curExact      bool
	curChecks     []int
	curSingle     int
	curResidual   bool
	curSplitExact bool
	bucketWork    posting.List
	bucketSeen    *u64set

	has bool
	cur uint64
}

type plannerOrderIndexView struct {
	indexView indexdata.FieldIndexView
	release   func()
}

func (v plannerOrderIndexView) close() {
	if v.release != nil {
		v.release()
	}
}

// plannerOrderIndexSnapshotViewByOrdinal returns the current immutable order-index slice.
func (qv *View) plannerOrderIndexSnapshotViewByOrdinal(fieldOrdinal int) (plannerOrderIndexView, bool) {
	ov := qv.fieldIndexViewFromSlotsByOrdinal(qv.snap.Index, fieldOrdinal)
	if !ov.HasData() {
		return plannerOrderIndexView{}, false
	}
	return plannerOrderIndexView{indexView: ov}, true
}

func (it *plannerOROrderBranchIter) init() {
	if it.startBucket < 0 {
		it.startBucket = 0
	}
	if it.endBucket <= 0 || it.endBucket > it.indexView.KeyCount() {
		it.endBucket = it.indexView.KeyCount()
	}
	if it.endBucket < it.startBucket {
		it.endBucket = it.startBucket
	}
	if it.desc {
		it.nextBucket = it.endBucket - 1
	} else {
		it.nextBucket = it.startBucket
	}
	it.curCursor = it.indexView.NewCursor(it.indexView.RangeByRanks(it.startBucket, it.endBucket), it.desc)
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
		return it.branch.preds.owner[single].matches(idx)
	}
	return it.branch.matchesChecks(idx, checks)
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
			bucket, idx, single, ok := it.curCursor.NextPostingOrSingle()
			if !ok {
				return examinedDelta, residualExaminedDelta, emittedDelta, false
			}

			if it.bucketSeen != nil {
				it.bucketSeen.Add(uint64(b))
			}
			it.curBucket = b
			if single {
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
				mode, exactIDs, nextBucketWork, card := plannerFilterPostingByPredicateChecks(it.branch.preds.owner, it.exactChecks, bucket, it.bucketWork, true)
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

		bucket, idx, single, ok := it.curCursor.NextPostingOrSingle()
		if !ok {
			return examinedDelta, residualExaminedDelta, emittedDelta, false
		}

		if it.bucketSeen != nil {
			it.bucketSeen.Add(uint64(b))
		}

		it.curBucket = b
		if single {
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
			mode, exactIDs, nextBucketWork, card := plannerFilterPostingByPredicateChecks(it.branch.preds.owner, it.exactChecks, bucket, it.bucketWork, true)
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

func (qv *View) execPlanOROrderKWay(
	q *qir.Shape,
	branches plannerORBranches,
	analysis *plannerOROrderAnalysis,
	trace *Trace,
) ([]uint64, bool, error) {
	return qv.execPlanOROrderKWayWithFallback(q, branches, analysis, plannerOROrderCandidate{}, trace)
}

func (qv *View) execPlanOROrderKWayWithFallback(
	q *qir.Shape,
	branches plannerORBranches,
	analysis *plannerOROrderAnalysis,
	selectorFallback plannerOROrderCandidate,
	trace *Trace,
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

	ov := orderView.indexView
	if !ov.HasData() {
		return nil, true, nil
	}
	orderField := qv.exec.FieldNameByOrdinal(o.FieldOrdinal)

	for i := 0; i < branches.Len(); i++ {
		if branches.owner[i].alwaysTrue {
			return nil, false, nil
		}
	}
	allowRuntimeFallback := false
	runtimeShape := plannerORKWayRuntimeShape{
		overlap:   1,
		avgChecks: 1,
	}

	if guardDec, ok := qv.decideOROrderKWayRuntimeFallbackWithAnalysisAndFallback(q, branches, needWindow, analysis, selectorFallback); ok {
		allowRuntimeFallback = guardDec.enable
		runtimeShape = plannerORKWayRuntimeShapeFromGuard(guardDec, q)
		if trace != nil {
			trace.SetOROrderRuntimeGuard(guardDec.enable, guardDec.reason)
		}
	}

	fullTrace := trace != nil && trace.Full()
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

	var (
		bucketSeen    u64set
		bucketSeenPtr *u64set
	)
	if fullTrace {
		bucketSeen = getU64Set(max(64, needWindow*branches.Len()))
		bucketSeenPtr = &bucketSeen
		defer releaseU64Set(&bucketSeen)
	}

	var (
		iters         plannerOROrderIterStore
		mergeItemsBuf []plannerOROrderMergeItem
	)
	var (
		observedCheckRows    [plannerORBranchLimit]uint64
		branchExaminedRows   [plannerORBranchLimit]uint64
		branchUniverses      [plannerORBranchLimit]uint64
		branchObservedChecks [plannerORBranchLimit][]int
	)

	if branches.Len() <= len(iters.inline) {
		defer closePlannerOROrderInlineIters(&iters.inline, branches.Len())

	} else {
		iters.buf = plannerOROrderIterSlicePool.Get(branches.Len())[:branches.Len()]
		defer plannerOROrderIterSlicePool.Put(iters.buf)
	}

	mergeItemsBuf = plannerOROrderMergeItemSlicePool.Get(branches.Len())
	defer plannerOROrderMergeItemSlicePool.Put(mergeItemsBuf)

	h := plannerOROrderMergeHeap{
		desc:     o.Desc,
		itemsBuf: mergeItemsBuf,
	}
	snapshotUniverse := qv.snap.Universe.Cardinality()

	for i := 0; i < branches.Len(); i++ {
		branch := &branches.owner[i]
		var covered []bool
		var branchStart, branchEnd int
		branchUniverse := snapshotUniverse
		reuseAnalysis := analysis != nil && i < analysis.branchCount

		if reuseAnalysis {
			covered = analysis.branches[i].covered
			branchStart = analysis.branches[i].rangeStart
			branchEnd = analysis.branches[i].rangeEnd
			branchUniverse = analysis.branches[i].universe

		} else {
			br, rangeCovered, rangeOK := qv.extractOrderRangeCoverageIndexViewReader(orderField, branch.preds.owner, ov)
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

		checksBuf := pooled.GetIntSlice(branch.preds.Len())
		checksBuf = branch.buildMatchChecks(checksBuf)
		exactChecksBuf := pooled.GetIntSlice(len(checksBuf))
		exactChecksBuf = buildExactBucketPostingFilterActiveReader(exactChecksBuf, checksBuf, branch.preds.owner)
		residualChecksBuf := pooled.GetIntSlice(len(checksBuf))
		residualChecksBuf = plannerResidualChecks(residualChecksBuf, checksBuf, exactChecksBuf)

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
			indexView:      ov,
			desc:           o.Desc,
			single:         singleCheck,
			exactSingle:    exactSingle,
			residualSingle: residualSingle,
			allChecksExact: len(checksBuf) > 0 && len(exactChecksBuf) == len(checksBuf),
			startBucket:    branchStart,
			endBucket:      branchEnd,
			bucketSeen:     bucketSeenPtr,
		}
		iter.init()

		examinedDelta, residualExaminedDelta, emittedDelta, ok := iter.advance()

		examined += examinedDelta
		if allowRuntimeFallback {
			branchExaminedRows[i] = satAddUint64(branchExaminedRows[i], examinedDelta)
		}

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
				scanWidth = uint64(bucketSeen.Len())
			}
			trace.AddExamined(examined)
			trace.AddOrderScanWidth(scanWidth)
			trace.SetORBranches(branchMetrics)
			trace.SetEarlyStopReason("no_candidates")
		}
		return nil, true, nil
	}

	outCap := clampUint64ToInt(q.Limit)
	if outCap <= 0 {
		outCap = needWindow
	}
	out := make([]uint64, 0, outCap)

	seen := getU64Set(max(64, needWindow*2))
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
		if allowRuntimeFallback {
			branchExaminedRows[bi] = satAddUint64(branchExaminedRows[bi], examinedDelta)
		}
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
			if allowRuntimeFallback {
				maxBranchExamined, activeBranches := plannerORKWayBranchProgress(&branchExaminedRows, branches.Len())
				rt := plannerORKWayShouldFallbackRuntimeDetailedWithProgress(
					needWindow, pops, unique, examined, dedupe, maxBranchExamined, activeBranches, runtimeShape,
				)
				if rt.fallback {
					if trace != nil {
						trace.SetOROrderRuntimeFallback(
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
		unique++
		if skip > 0 {
			skip--
			if allowRuntimeFallback {
				maxBranchExamined, activeBranches := plannerORKWayBranchProgress(&branchExaminedRows, branches.Len())
				rt := plannerORKWayShouldFallbackRuntimeDetailedWithProgress(
					needWindow, pops, unique, examined, dedupe, maxBranchExamined, activeBranches, runtimeShape,
				)
				if rt.fallback {
					if trace != nil {
						trace.SetOROrderRuntimeFallback(
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
			maxBranchExamined, activeBranches := plannerORKWayBranchProgress(&branchExaminedRows, branches.Len())
			rt := plannerORKWayShouldFallbackRuntimeDetailedWithProgress(
				needWindow, pops, unique, examined, dedupe, maxBranchExamined, activeBranches, runtimeShape,
			)
			if rt.fallback {
				if trace != nil {
					trace.SetOROrderRuntimeFallback(
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
			scanWidth = uint64(bucketSeen.Len())
		}
		trace.AddExamined(examined)
		trace.AddDedupe(dedupe)
		trace.AddOrderScanWidth(scanWidth)
		trace.SetORBranches(branchMetrics)
		trace.SetEarlyStopReason(stopReason)
	}
	return out, true, nil
}

func (qv *View) execPlanOROrderMergeFallback(q *qir.Shape, branches plannerORBranches, trace *Trace) ([]uint64, bool, error) {
	need, ok := orderWindow(q)
	if !ok || need <= 0 {
		return nil, false, nil
	}

	candidateSet := newPostingLazySetBuilder(satMulUint64(uint64(need), uint64(branches.Len())))

	fullTrace := trace != nil && trace.Full()
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
	var orderOV indexdata.FieldIndexView
	if order.FieldOrdinal >= 0 {
		orderOV = qv.fieldIndexViewFromSlotsForOrder(qv.snap.Index, order)
		directBranchCollect = orderOV.HasData()
	}

	for i := 0; i < branches.Len(); i++ {
		branch := branches.owner[i]
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
				&branches.owner[i], q.Order, branchLimit, orderOV, &candidateSet,
			)
			if okCollect {
				if trace != nil {
					trace.AddExamined(examined)
					trace.AddDedupe(dedupe)
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

		ids, err := qv.PreparedQuery(&subQ)
		if err != nil {
			candidateSet.release()
			return nil, true, err
		}
		if trace != nil {
			trace.AddExamined(uint64(len(ids)))
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
			trace.AddDedupe(dedupe)
		}
	}

	candidateIDs := candidateSet.finish(false)

	if candidateIDs.IsEmpty() {
		if trace != nil {
			trace.SetORBranches(branchMetrics)
			trace.SetEarlyStopReason("no_candidates")
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
	ov := orderView.indexView
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

	cur := ov.NewCursor(ov.RangeForBounds(indexdata.Bounds{Has: true}), o.Desc)
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

func (qv *View) collectOROrderFallbackBranchCandidatesWithChecks(
	branch *plannerORBranch,
	branchLimit int,
	ov indexdata.FieldIndexView,
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
			mode, exactIDs, nextBucketWork, card := plannerFilterPostingByPredicateChecks(branch.preds.owner, exactChecks, bucket, bucketWork, true)
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
func (qv *View) collectOROrderFallbackBranchCandidates(
	branch *plannerORBranch,
	order qir.Order,
	branchLimit int,
	ov indexdata.FieldIndexView,
	dst *postingLazySetBuilder,
) (uint64, uint64, uint64, bool) {

	if branchLimit <= 0 {
		return 0, 0, 0, false
	}
	if order.Kind != qir.OrderKindBasic || order.FieldOrdinal < 0 {
		return 0, 0, 0, false
	}
	fieldName := qv.exec.FieldNameByOrdinal(order.FieldOrdinal)
	fm := qv.fieldMetaByOrder(order)
	if fm == nil || fm.Slice {
		return 0, 0, 0, false
	}
	if !ov.HasData() {
		ov = qv.fieldIndexViewFromSlotsByName(qv.snap.Index, fieldName)
	}
	if !ov.HasData() {
		return 0, 0, 0, false
	}

	br, covered, rangeOK := qv.extractOrderRangeCoverageIndexViewReader(fieldName, branch.preds.owner, ov)
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

	if branch.preds.Len() <= plannerPredicateFastPathMaxLeaves {
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

	checksBuf := pooled.GetIntSlice(branch.preds.Len())
	checksBuf = branch.buildMatchChecks(checksBuf)
	exactChecksBuf := pooled.GetIntSlice(len(checksBuf))
	exactChecksBuf = buildExactBucketPostingFilterActiveReader(exactChecksBuf, checksBuf, branch.preds.owner)
	residualChecksBuf := pooled.GetIntSlice(len(checksBuf))
	residualChecksBuf = plannerResidualChecks(residualChecksBuf, checksBuf, exactChecksBuf)

	emitted, examined, dedupe := qv.collectOROrderFallbackBranchCandidatesWithChecks(
		branch, branchLimit, ov, order.Desc, start, end, dst, checksBuf, exactChecksBuf, residualChecksBuf,
	)
	pooled.ReleaseIntSlice(residualChecksBuf)
	pooled.ReleaseIntSlice(exactChecksBuf)
	pooled.ReleaseIntSlice(checksBuf)
	return emitted, examined, dedupe, true
}

func (qv *View) planOROrderMergeBranchLimit(branch plannerORBranch, need int) (limit int, exhaustive bool, err error) {
	if need <= 0 {
		return 0, true, nil
	}

	// Default safe limit: full window size.
	limit = need
	if !plannerOROrderMergePrecountWorth(branch, need) {
		return limit, false, nil
	}

	if branch.preds.Len() == 0 || branch.preds.Len() > 4 {
		return limit, false, nil
	}

	// Keep branch pre-counting conservative:
	// for broad/range/string scans it is usually not worth extra cardinality work.
	for i := 0; i < branch.preds.Len(); i++ {
		e := branch.preds.owner[i].expr
		if e.Not {
			return limit, false, nil
		}
		if !qv.canPrecountORBranchExpr(e) {
			return limit, false, nil
		}
	}

	if cnt, ok := qv.uniqueLeadBranchCardinality(branch); ok {
		if cnt == 0 {
			return 0, true, nil
		}
		if cnt < uint64(limit) {
			return int(cnt), true, nil
		}
		return limit, false, nil
	}

	var cnt uint64
	cnt, err = qv.exactExprCardinality(branch.expr)
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
	if need <= 0 || !branch.hasLead() || branch.preds.owner[branch.leadIdx].estCard == 0 {
		return false
	}

	checks := float64(branch.containsChecks())
	needF := float64(need)
	leadEst := float64(branch.preds.owner[branch.leadIdx].estCard)
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

func (qv *View) canPrecountORBranchExpr(e qir.Expr) bool {
	if !e.Op.IsMaterializedScalarCache() {
		return true
	}
	cacheKey := qv.materializedPredKey(e)
	if cacheKey.IsZero() {
		return false
	}
	return qv.snap.HasMaterializedPredKey(cacheKey)
}

func (qv *View) uniqueLeadBranchCardinalityWithChecks(branch plannerORBranch, leadIdx int, checks []int) (uint64, bool) {
	lead := branch.preds.owner[leadIdx]
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

// uniqueLeadBranchCardinality tries to count one OR branch by scanning a unique
// EQ predicate lead and validating the remaining predicates with contains().
func (qv *View) uniqueLeadBranchCardinality(branch plannerORBranch) (uint64, bool) {
	if branch.preds.Len() == 0 {
		return 0, false
	}

	leadIdx := -1
	leadEst := uint64(0)
	for i := 0; i < branch.preds.Len(); i++ {
		p := branch.preds.owner[i]
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

	if branch.preds.Len() <= plannerPredicateFastPathMaxLeaves {
		var checksInline [plannerPredicateFastPathMaxLeaves]int
		checks := checksInline[:0]
		for i := 0; i < branch.preds.Len(); i++ {
			if i == leadIdx {
				continue
			}
			p := branch.preds.owner[i]
			if p.covered || p.alwaysTrue {
				continue
			}
			if p.alwaysFalse || !p.hasContains() {
				return 0, true
			}
			checks = append(checks, i)
		}
		sortActivePredicatesReader(checks, branch.preds.owner)
		return qv.uniqueLeadBranchCardinalityWithChecks(branch, leadIdx, checks)
	}

	checksBuf := pooled.GetIntSlice(branch.preds.Len())
	for i := 0; i < branch.preds.Len(); i++ {
		if i == leadIdx {
			continue
		}
		p := branch.preds.owner[i]
		if p.covered || p.alwaysTrue {
			continue
		}
		if p.alwaysFalse || !p.hasContains() {
			pooled.ReleaseIntSlice(checksBuf)
			return 0, true
		}
		checksBuf = append(checksBuf, i)
	}
	sortActivePredicatesReader(checksBuf, branch.preds.owner)
	cnt, ok := qv.uniqueLeadBranchCardinalityWithChecks(branch, leadIdx, checksBuf)
	pooled.ReleaseIntSlice(checksBuf)
	return cnt, ok
}

type plannerORNoOrderMode uint8

const (
	plannerORNoOrderModeRun plannerORNoOrderMode = iota
	plannerORNoOrderModeUniverse
	plannerORNoOrderModeUnsupported
)

func plannerORNoOrderClassify(branches plannerORBranches) plannerORNoOrderMode {
	for i := 0; i < branches.Len(); i++ {
		branch := branches.owner[i]
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

func plannerORPredicateExactLeadPosting(p *predicate) (posting.List, bool) {
	if p == nil || p.expr.Not || p.leadIterNeedsContainsCheck() {
		return posting.List{}, false
	}
	if p.rangeMat {
		return p.ids, true
	}
	switch p.kind {
	case predicateKindPosting:
		return p.posting, true
	case predicateKindMaterialized:
		return p.ids, true
	case predicateKindPostsAny, predicateKindPostsAll:
		if len(p.postsBuf) == 1 {
			return p.postsBuf[0], true
		}
	}
	return posting.List{}, false
}

func plannerORBranchExactLeadPostingEmpty(branch *plannerORBranch) bool {
	if branch == nil || !branch.hasLead() {
		return false
	}
	leadIDs, ok := plannerORPredicateExactLeadPosting(branch.leadPtr())
	if !ok {
		return false
	}
	if leadIDs.IsEmpty() {
		return true
	}
	for i := 0; i < branch.preds.Len(); i++ {
		if i == branch.leadIdx {
			continue
		}
		p := branch.preds.owner[i]
		if p.covered || p.alwaysTrue {
			continue
		}
		if p.alwaysFalse {
			return true
		}
		cnt, ok := p.countBucket(leadIDs)
		if !ok {
			return false
		}
		if cnt == 0 {
			return true
		}
	}
	return false
}

func plannerORNoOrderProbeBranch(st *plannerORNoOrderBranchState) (uint64, uint64) {
	if st.exhausted || st.iter.has {
		return 0, 0
	}
	st.initialized = true
	if plannerORBranchExactLeadPostingEmpty(st.iter.branch) {
		st.capped = false
		st.exhausted = true
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

func (qv *View) execPlanORNoOrderAdaptiveCore(q *qir.Shape, branches plannerORBranches, trace *Trace) ([]uint64, bool) {
	if q.Limit == 0 || q.Offset != 0 {
		return nil, false
	}

	need64 := q.Limit
	universeCard := qv.snap.Universe.Cardinality()
	if need64 > universeCard {
		need64 = universeCard
	}
	need := clampUint64ToInt(need64)
	if need <= 0 {
		return nil, true
	}

	fullTrace := trace != nil && trace.Full()
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
		branch := &branches.owner[i]
		branch.leadIdx = branch.noOrderLeadIndex(need)
		state := &states[i]
		state.index = i
		state.score = branch.noOrderScore(need)
		state.budget = branch.noOrderBudget(need)
		order[i] = i
		lead := branch.leadPtr()
		state.iter = plannerORIter{
			it:     lead.newIter(),
			branch: branch,
		}
	}
	defer releasePlannerORNoOrderStateIters(states)

	plannerORNoOrderSortBranchOrder(order, states)

	useLinearSeen := need <= plannerORNoOrderLinearSeenNeedMax
	seen := u64set{}
	if !useLinearSeen {
		seen = getU64Set(max(64, need*2))
		defer releaseU64Set(&seen)
	}

	out := make([]uint64, 0, need)
	earlyStopReason := "input_exhausted"

	for len(out) < need {
		progress := false
		for _, idx := range order {
			st := &states[idx]
			if st.exhausted {
				continue
			}
			for len(out) < need && !st.exhausted {
				if !st.iter.has {
					examinedDelta, emittedDelta := plannerORNoOrderProbeBranch(st)
					examined += examinedDelta
					if fullTrace {
						branchMetrics[st.index].RowsExamined += examinedDelta
						branchMetrics[st.index].RowsEmitted += emittedDelta
					}
					if examinedDelta != 0 || emittedDelta != 0 || st.exhausted {
						progress = true
					}
					if !st.iter.has {
						break
					}
				}

				idx := st.iter.cur
				if useLinearSeen {
					dup := false
					for i := 0; i < len(out); i++ {
						if out[i] == idx {
							dup = true
							break
						}
					}
					if dup {
						if trace != nil {
							trace.AddDedupe(1)
						}
					} else {
						out = append(out, idx)
					}
				} else if seen.Add(idx) {
					out = append(out, idx)
				} else if trace != nil {
					trace.AddDedupe(1)
				}

				progress = true
				if len(out) == need {
					earlyStopReason = "limit_reached"
					break
				}
				examinedDelta, emittedDelta := plannerORNoOrderConsumeBranch(st)
				examined += examinedDelta
				if fullTrace {
					branchMetrics[st.index].RowsExamined += examinedDelta
					branchMetrics[st.index].RowsEmitted += emittedDelta
				}
				if st.capped {
					break
				}
			}
			if len(out) == need {
				break
			}
		}
		if len(out) == need || !progress {
			break
		}
	}

	if trace != nil {
		trace.AddExamined(examined)
		trace.SetORBranches(branchMetrics)
		trace.SetEarlyStopReason(earlyStopReason)
	}

	if len(out) == 0 {
		return nil, true
	}

	return out, true
}

func (qv *View) execPlanORNoOrderBaselineCore(q *qir.Shape, branches plannerORBranches, trace *Trace) ([]uint64, bool) {
	fullTrace := trace != nil && trace.Full()
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
		branch := branches.owner[i]
		lead := branch.leadPtr()
		iters = append(iters, plannerORIter{
			it:     lead.newIter(),
			branch: &branches.owner[i],
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
	capHint, exhausted := boundedWindowCap(qv.snap.Universe.Cardinality(), skip, q.Limit)
	if exhausted {
		return nil, true
	}
	need := clampUint64ToInt(capHint)
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
		seen = getU64Set(max(64, needWindow*2))
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
			trace.AddDedupe(dupHits - 1)
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
					trace.AddDedupe(1)
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
					trace.AddDedupe(1)
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
		trace.AddExamined(examined)
		trace.SetORBranches(branchMetrics)
		trace.SetEarlyStopReason(stopReason)
	}
	return out, true
}

func (qv *View) execPlanORUniverseNoOrder(q *qir.Shape, trace *Trace) []uint64 {
	skip := q.Offset
	capHint, exhausted := boundedWindowCap(qv.snap.Universe.Cardinality(), skip, q.Limit)
	if exhausted {
		if trace != nil {
			trace.AddExamined(qv.snap.Universe.Cardinality())
			trace.SetEarlyStopReason("input_exhausted")
		}
		return nil
	}
	need := clampUint64ToInt(capHint)
	out := make([]uint64, 0, need)
	stopReason := "input_exhausted"

	if trace != nil {
		trace.AddExamined(qv.snap.Universe.Cardinality())
	}

	it := qv.snap.Universe.Borrow().Iter()
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
		trace.SetEarlyStopReason(stopReason)
	}
	return out
}
