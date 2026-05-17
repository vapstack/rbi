package qexec

import (
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
)

const (
	cardinalityPredicateScanMaxLeaves                  = 16
	cardinalityORPredicateMaxBranchesBase              = 5
	cardinalityORScalarDisjointMaxBranches             = 8 // Matches the max OR predicate branch limit; keeps disjoint-branch probes on stack.
	cardinalityORHybridMaterializedBranchMax           = 3
	cardinalityORSeenUnionThresholdBase                = 64_000
	cardinalityORSeenUniverseDiv                       = 16
	cardinalityScalarInSplitMaxValues                  = 32
	cardinalitySetMaterializeMinTermsBase              = 6
	cardinalityCustomMaterializeMinProbeBase           = 4_096
	cardinalityBroadRangeLazyMinCard                   = 65_536
	cardinalityBroadRangeComplementMaxCard             = 32_768
	cardinalityBroadRangeComplementMaxCardCap          = 131_072
	cardinalityBroadRangeComplementFastProbeMin        = 4_096
	cardinalityBroadRangeComplementFastAvgPerBucketMax = 8
	cardinalityLeadResidualHasAnyExactMaxTerms         = 4
	cardinalityLeadResidualHasAnyExactMinCard          = 65_536
	cardinalityNumericBucketExactMinRows               = 65_536 // Below this, direct lead iteration is cheaper than building bucket postings for exact residual counts.
)

var cardinalityORMaterializedRouteKeys = [4]qcache.MaterializedPredKey{
	2: qcache.MaterializedPredKeyFromOpaque("cardinality_or_materialized_2"),
	3: qcache.MaterializedPredKeyFromOpaque("cardinality_or_materialized_3"),
}

func (qv *View) TraceOrCalibrationSamplingEnabled() bool {
	return qv.exec.TraceOrCalibrationSamplingEnabled()
}

func (qv *View) TryFilterCardinalityPreparedAndReordered(expr qir.Expr) (uint64, bool, error) {
	if !shouldTryMaterializedCardinalityAND(expr) {
		return 0, false, nil
	}
	return qv.tryFilterCardinalityPreparedAndReordered(expr)
}

func (qv *View) FilterCardinalityByMaterializedExpr(expr qir.Expr, trace *Trace) (uint64, error) {
	b, err := qv.evalExpr(expr)
	if err != nil {
		return 0, err
	}
	defer b.ids.Release()

	if trace != nil {
		trace.SetPlan(planFilterCardinalityMaterialized)
		if b.neg {
			trace.AddExamined(qv.snap.Universe.Cardinality())
		} else if !b.ids.IsEmpty() {
			trace.AddExamined(b.ids.Cardinality())
		}
	}

	return qv.postingResultCardinality(b), nil
}

func (qv *View) TryFilterCardinalityByScalarLookup(expr qir.Expr, trace *Trace) (uint64, bool, error) {
	if expr.FieldOrdinal < 0 {
		return 0, false, nil
	}

	f := qv.fieldMetaByExpr(expr)
	if f == nil || f.Slice {
		return 0, false, nil
	}

	ov := qv.fieldIndexViewFromSlotsForExpr(qv.snap.Index, expr)
	if !ov.HasData() && !qv.hasIndexedFieldForExpr(expr) {
		return 0, false, nil
	}

	switch expr.Op {
	case qir.OpEQ:
		key, isSlice, isNil, err := qv.exprValueToLookupKey(expr)
		if err != nil || isSlice {
			return 0, false, err
		}
		hit := uint64(0)
		if isNil {
			hit = qv.fieldIndexViewFromSlotsForExpr(qv.snap.NilIndex, expr).LookupCardinality(indexdata.NilIndexEntryKey)
		} else {
			hit = lookupScalarCardinality(ov, key)
		}
		if trace != nil {
			trace.SetPlan(planFilterCardinalityScalarLookup)
			trace.AddExamined(hit)
		}
		return cardinalityLookupComplement(qv.snap.Universe.Cardinality(), hit, expr.Not), true, nil

	case qir.OpIN:
		valsBuf, isSlice, hasNil, err := qv.exprValueToDistinctIdxBuf(expr)
		if valsBuf != nil {
			defer pooled.ReleaseStringSlice(valsBuf)
		}
		valCount := len(valsBuf)
		if err != nil || !isSlice || (valCount == 0 && !hasNil) {
			return 0, false, err
		}

		var sum uint64
		for i := 0; i < valCount; i++ {
			sum += ov.LookupCardinality(valsBuf[i])
		}
		if hasNil {
			sum += qv.fieldIndexViewFromSlotsForExpr(qv.snap.NilIndex, expr).LookupCardinality(indexdata.NilIndexEntryKey)
		}
		if trace != nil {
			trace.SetPlan(planFilterCardinalityScalarLookup)
			trace.AddExamined(sum)
		}
		return cardinalityLookupComplement(qv.snap.Universe.Cardinality(), sum, expr.Not), true, nil
	}

	return 0, false, nil
}

func cardinalityLookupComplement(universe, hit uint64, invert bool) uint64 {
	if !invert {
		return hit
	}
	if hit >= universe {
		return 0
	}
	return universe - hit
}

func postingUnionCardinality2(a, b posting.List) uint64 {
	if a.IsEmpty() {
		return b.Cardinality()
	}
	if b.IsEmpty() {
		return a.Cardinality()
	}
	return satAddUint64(a.Cardinality(), b.Cardinality()) - a.AndCardinality(b)
}

func postingAllMatchesCardinality(posts []posting.List) uint64 {
	if len(posts) == 0 {
		return 0
	}
	leadIdx := 0
	lead := posts[0]
	leadCard := lead.Cardinality()
	if leadCard == 0 {
		return 0
	}
	for i := 1; i < len(posts); i++ {
		ids := posts[i]
		card := ids.Cardinality()
		if card == 0 {
			return 0
		}
		if card < leadCard {
			leadIdx = i
			lead = ids
			leadCard = card
		}
	}

	if idx, ok := lead.TrySingle(); ok {
		for i := 0; i < len(posts); i++ {
			if i == leadIdx {
				continue
			}
			if !posts[i].Contains(idx) {
				return 0
			}
		}
		return 1
	}

	var cnt uint64

	it := lead.Iter()
	for it.HasNext() {
		idx := it.Next()
		match := true
		for i := 0; i < len(posts); i++ {
			if i == leadIdx {
				continue
			}
			if !posts[i].Contains(idx) {
				match = false
				break
			}
		}
		if match {
			cnt++
		}
	}
	it.Release()

	return cnt
}

func (qv *View) TryFilterCardinalityBySliceLookup(expr qir.Expr, trace *Trace) (uint64, bool, error) {
	if expr.FieldOrdinal < 0 {
		return 0, false, nil
	}

	f := qv.fieldMetaByExpr(expr)
	if f == nil || !f.Slice {
		return 0, false, nil
	}

	ov := qv.fieldIndexViewFromSlotsForExpr(qv.snap.Index, expr)
	if !ov.HasData() && !qv.hasIndexedFieldForExpr(expr) {
		return 0, false, nil
	}

	valsBuf, isSlice, _, err := qv.exprValueToDistinctIdxBuf(expr)
	if valsBuf != nil {
		defer pooled.ReleaseStringSlice(valsBuf)
	}
	valCount := len(valsBuf)
	if err != nil {
		return 0, false, err
	}
	if !isSlice {
		return 0, false, nil
	}
	if valCount == 0 {
		return 0, false, nil
	}

	universe := qv.snap.Universe.Cardinality()

	switch expr.Op {

	case qir.OpHASANY:

		postsBuf, _ := qv.scalarLookupPostings(qv.exec.FieldNameByOrdinal(expr.FieldOrdinal), expr.FieldOrdinal, valsBuf, false)
		defer posting.ReleaseSlice(postsBuf)

		switch len(postsBuf) {
		case 0:
			if trace != nil {
				trace.SetPlan(planFilterCardinalityScalarLookup)
				trace.AddExamined(0)
			}
			return cardinalityLookupComplement(universe, 0, expr.Not), true, nil

		case 1:
			hit := postsBuf[0].Cardinality()
			if trace != nil {
				trace.SetPlan(planFilterCardinalityScalarLookup)
				trace.AddExamined(hit)
			}
			return cardinalityLookupComplement(universe, hit, expr.Not), true, nil

		case 2:
			a := postsBuf[0]
			b := postsBuf[1]
			hit := postingUnionCardinality2(a, b)
			if trace != nil {
				trace.SetPlan(planFilterCardinalityScalarLookup)
				trace.AddExamined(satAddUint64(a.Cardinality(), b.Cardinality()))
			}
			return cardinalityLookupComplement(universe, hit, expr.Not), true, nil

		default:
			return 0, false, nil
		}

	case qir.OpHASALL:

		postsBuf := posting.GetSlice(valCount)
		defer posting.ReleaseSlice(postsBuf)

		var examined uint64
		for i := 0; i < valCount; i++ {
			ids := ov.LookupPostingRetained(valsBuf[i])
			card := ids.Cardinality()
			examined = satAddUint64(examined, card)
			if ids.IsEmpty() {
				if trace != nil {
					trace.SetPlan(planFilterCardinalityScalarLookup)
					trace.AddExamined(examined)
				}
				return cardinalityLookupComplement(universe, 0, expr.Not), true, nil
			}
			postsBuf = append(postsBuf, ids)
		}

		hit := postingAllMatchesCardinality(postsBuf)
		if trace != nil {
			trace.SetPlan(planFilterCardinalityScalarLookup)
			trace.AddExamined(examined)
		}
		return cardinalityLookupComplement(universe, hit, expr.Not), true, nil
	}

	return 0, false, nil
}

func postingAgainstResultCardinality(ids posting.List, filter postingResult) uint64 {
	if ids.IsEmpty() {
		return 0
	}
	card := ids.Cardinality()
	if !filter.neg {
		if filter.ids.IsEmpty() {
			return 0
		}
		return ids.AndCardinality(filter.ids)
	}
	if filter.ids.IsEmpty() {
		return card
	}
	excluded := ids.AndCardinality(filter.ids)
	if excluded >= card {
		return 0
	}
	return card - excluded
}

type cardinalityLeadResidualExactFilter struct {
	idx   int
	ids   posting.List
	state *postsAnyFilterState
}

func shouldUseCardinalityLeadResidualHasAnyExactFilter(p predicate) bool {
	if p.kind != predicateKindPostsAny || p.expr.Not || p.expr.Op != qir.OpHASANY {
		return false
	}
	l := len(p.postsBuf)
	if l < 2 || l > cardinalityLeadResidualHasAnyExactMaxTerms {
		return false
	}
	return p.estCard >= cardinalityLeadResidualHasAnyExactMinCard
}

func cardinalityLeadResidualExactFiltersContain(filters []cardinalityLeadResidualExactFilter, idx int) bool {
	for i := 0; i < len(filters); i++ {
		if filters[i].idx == idx {
			return true
		}
	}
	return false
}

func cardinalityIndexSliceContains(active []int, idx int) bool {
	for _, v := range active {
		if v == idx {
			return true
		}
	}
	return false
}

func (qv *View) buildCardinalityLeadResidualExactFiltersByCandidatesInto(dst []cardinalityLeadResidualExactFilter, preds predicateReader, candidates []int) []cardinalityLeadResidualExactFilter {
	dst = dst[:0]
	for _, pi := range candidates {
		if pi < 0 || pi >= len(preds) {
			continue
		}
		p := preds[pi]
		if p.kind != predicateKindPostsAny || p.expr.Not || p.expr.Op != qir.OpHASANY || len(p.postsBuf) == 0 {
			continue
		}

		if p.postsAnyState != nil {
			dst = append(dst, cardinalityLeadResidualExactFilter{
				idx:   pi,
				state: p.postsAnyState,
			})
			continue
		}

		var ids posting.List
		if isSingletonHeavyPostingBuf(p.postsBuf) {
			ids = materializePostingBufFast(p.postsBuf)
		} else {
			ids = materializePostingUnionBufOwned(p.postsBuf)
		}
		if ids.IsEmpty() {
			continue
		}
		dst = append(dst, cardinalityLeadResidualExactFilter{idx: pi, ids: ids})
	}
	return dst
}

func (qv *View) collectCardinalityLeadResidualExactCandidatesInto(dst []int, preds predicateReader, active []int, exclude []int) []int {
	dst = dst[:0]
	for _, pi := range active {
		if cardinalityIndexSliceContains(exclude, pi) {
			continue
		}
		p := preds[pi]
		if shouldUseCardinalityLeadResidualHasAnyExactFilter(p) {
			dst = append(dst, pi)
		}
	}
	return dst
}

func applyCardinalityLeadResidualExactFilters(src, work posting.List, filters []cardinalityLeadResidualExactFilter) (posting.List, posting.List) {
	if src.IsEmpty() {
		return posting.List{}, work
	}
	if len(filters) == 0 {
		return src, work
	}

	current := src.Borrow()
	for i := 0; i < len(filters); i++ {
		f := filters[i]
		if f.state != nil {
			sharedWork := !current.IsEmpty() && current.SharesPayload(work)
			next, ok := f.state.apply(current)
			if !ok {
				union := f.state.materialize()
				if union.IsEmpty() {
					if sharedWork {
						work = posting.List{}
					}
					current.Release()
					current = posting.List{}
					break
				}
				if current.IsBorrowed() {
					current = current.CloneInto(work)
				} else if !sharedWork {
					work.Release()
				}
				current = current.BuildAnd(union)
				work = current

			} else {
				current = next
				if current.IsEmpty() {
					if sharedWork {
						work = posting.List{}
					}
					break
				}
				if !current.IsBorrowed() {
					if !sharedWork && !current.SharesPayload(work) {
						work.Release()
					}
					work = current
				}
			}
			continue
		}

		if f.ids.IsEmpty() {
			if current.SharesPayload(work) {
				work = posting.List{}
			}
			current.Release()
			current = posting.List{}
			break
		}

		if current.IsBorrowed() {
			current = current.CloneInto(work)
		} else if !current.SharesPayload(work) {
			work.Release()
		}

		current = current.BuildAnd(f.ids)
		work = current
		if current.IsEmpty() {
			break
		}
	}
	return current, work
}

func shouldApplyCardinalityLeadResidualExactFilters(src posting.List, filters []cardinalityLeadResidualExactFilter) bool {
	if src.IsEmpty() || len(filters) == 0 {
		return false
	}
	return shouldBuildCardinalityLeadResidualExactFilters(src, len(filters))
}

func shouldBuildCardinalityLeadResidualExactFilters(src posting.List, filterCount int) bool {
	if src.IsEmpty() || filterCount <= 0 {
		return false
	}
	return src.Cardinality() > plannerPredicateBucketExactMinCardForChecks(filterCount)
}

func postingMatchesMultiSetCardinality(preds predicateSet, checks []int, ids posting.List) uint64 {
	var cnt uint64
	it := ids.Iter()
	for it.HasNext() {
		if cardinalityPredicatesMatchSet(preds, checks, it.Next()) {
			cnt++
		}
	}
	it.Release()
	return cnt
}

func cardinalityORBranchAcceptIdx(
	branches []cardinalityORBranch,
	branchIdx int,
	br *cardinalityORBranch,
	useSeenDedup bool,
	seen *cardinalityORSeen,
	idx uint64,
	checks []int,
) uint64 {

	if useSeenDedup {
		if len(checks) == 0 {
			if seen.Add(idx) {
				return 1
			}
			return 0
		}
		if seen.Has(idx) {
			return 0
		}
		if !cardinalityPredicatesMatchSet(br.preds, checks, idx) {
			return 0
		}
		if seen.Add(idx) {
			return 1
		}
		return 0
	}

	if len(checks) > 0 && !cardinalityPredicatesMatchSet(br.preds, checks, idx) {
		return 0
	}

	if br.prevOrderLen == branchIdx {
		for i := 0; i < br.prevOrderLen; i++ {
			if (&branches[br.prevOrder[i]]).matches(idx) {
				return 0
			}
		}
		return 1
	}

	for j := 0; j < branchIdx; j++ {
		if (&branches[j]).matches(idx) {
			return 0
		}
	}
	return 1
}

func cardinalityORBranchAcceptPosting(
	branches []cardinalityORBranch,
	branchIdx int,
	br *cardinalityORBranch,
	useSeenDedup bool,
	seen *cardinalityORSeen,
	ids posting.List,
	checks []int,
) uint64 {

	if ids.IsEmpty() {
		return 0
	}
	if idx, ok := ids.TrySingle(); ok {
		return cardinalityORBranchAcceptIdx(branches, branchIdx, br, useSeenDedup, seen, idx, checks)
	}
	if useSeenDedup && len(checks) == 0 {
		return seen.addPosting(ids)
	}
	var cnt uint64
	it := ids.Iter()
	for it.HasNext() {
		cnt += cardinalityORBranchAcceptIdx(branches, branchIdx, br, useSeenDedup, seen, it.Next(), checks)
	}
	it.Release()
	return cnt
}

func (qv *View) ensureCardinalityLeadResidualExactFiltersSet(
	preds predicateSet,
	candidates, residualExact, residualBoth []int,
	extraExactBuf []cardinalityLeadResidualExactFilter,
	extraExactBuilt bool,
) ([]cardinalityLeadResidualExactFilter, bool, []int) {

	if extraExactBuilt {
		return extraExactBuf, true, residualBoth
	}

	extraExactBuf = cardinalityLeadResidualExactFilterSlicePool.Get(len(candidates))
	extraExactBuf = qv.buildCardinalityLeadResidualExactFiltersByCandidatesInto(extraExactBuf, preds.owner, candidates)

	if len(extraExactBuf) == 0 {
		return extraExactBuf, true, residualBoth
	}

	residualBoth = residualBoth[:0]
	for _, pi := range residualExact {
		if cardinalityLeadResidualExactFiltersContain(extraExactBuf, pi) {
			continue
		}
		residualBoth = append(residualBoth, pi)
	}

	return extraExactBuf, true, residualBoth
}

func (qv *View) buildCardinalityLeadResidualScratchSet(
	preds predicateSet,
	active, exactActiveDst, extraExactCandidatesDst, residualExactDst, residualBothDst []int,
) ([]int, []int, []int, []int) {

	exactActive := exactActiveDst[:0]
	for _, pi := range active {
		if preds.owner[pi].postingFilterCapability().supportsApply() {
			exactActive = append(exactActive, pi)
		}
	}

	extraExactCandidates := qv.collectCardinalityLeadResidualExactCandidatesInto(
		extraExactCandidatesDst,
		preds.owner,
		active,
		exactActive,
	)

	residualExact := residualExactDst[:0]
	residualBoth := residualBothDst[:0]

	for _, pi := range active {
		if cardinalityIndexSliceContains(exactActive, pi) {
			continue
		}
		residualExact = append(residualExact, pi)
		residualBoth = append(residualBoth, pi)
	}

	return exactActive, extraExactCandidates, residualExact, residualBoth
}

func (qv *View) evalAndOperandsExceptReordered(ops []qir.Expr, skip int) (postingResult, bool, error) {
	if len(ops) == 0 || (skip >= 0 && len(ops) <= 1) {
		return postingResult{}, false, nil
	}

	var filteredBuf [cardinalityPredicateScanMaxLeaves]qir.Expr
	filtered := filteredBuf[:0]
	for i := range ops {
		if i == skip {
			continue
		}
		filtered = append(filtered, ops[i])
	}
	if len(filtered) == 0 {
		return postingResult{}, false, nil
	}

	universe := qv.snap.Universe.Cardinality()

	var plansBuf [cardinalityPredicateScanMaxLeaves]cardinalityLeafPlan
	plans := plansBuf[:0]

	var posBuf [cardinalityPredicateScanMaxLeaves]int
	pos := posBuf[:0]

	var negBuf [cardinalityPredicateScanMaxLeaves]int
	neg := negBuf[:0]

	var mergedRangesBuf []orderedMergedScalarRangeField
	if len(filtered) > 1 {
		buf := orderedMergedScalarRangeFieldSlicePool.Get(len(filtered))
		var ok bool
		buf, ok = qv.collectMergedNumericRangeFields(filtered, buf)
		if ok {
			mergedRangesBuf = buf
			defer orderedMergedScalarRangeFieldSlicePool.Put(mergedRangesBuf)
		} else {
			orderedMergedScalarRangeFieldSlicePool.Put(buf)
		}
	}

	for i := range filtered {
		e := filtered[i]
		planIdx := len(plans)
		plans = append(plans, cardinalityLeafPlan{expr: e})
		if sel, _, _, _, ok := qv.estimateLeafOrderCost(e, nil, universe, "", false); ok {
			if sel < 0 {
				sel = 0
			}
			if sel > 1 {
				sel = 1
			}
			plans[planIdx].selectivity = sel
			plans[planIdx].hasSel = true
		}
		if e.Not {
			neg = append(neg, planIdx)
			continue
		}
		pos = append(pos, planIdx)
	}

	if len(plans) == 0 {
		return postingResult{}, false, nil
	}

	sortCardinalityLeafPlanOrder(plans, pos)
	qv.reorderCardinalityEvalAndPositivePlans(plans, pos)
	sortCardinalityLeafPlanOrder(plans, neg)

	var (
		acc    postingResult
		hasAcc bool
	)

	for _, pi := range pos {
		if mergedRangesBuf != nil {
			e := plans[pi].expr
			if qv.isPositiveMergedNumericRangeLeaf(e) {
				idx := findOrderedMergedScalarRangeField(mergedRangesBuf, qv.exec.FieldNameByOrdinal(e.FieldOrdinal))
				if idx >= 0 {
					merged := mergedRangesBuf[idx]
					if merged.count > 1 {
						if merged.first != pi {
							continue
						}
						done, err := qv.applyAndMergedExactRangePostingResult(&acc, &hasAcc, merged.expr, merged.bounds)
						if err != nil {
							return postingResult{}, true, err
						}
						if done {
							return postingResult{}, true, nil
						}
						continue
					}
				}
			}
		}
		done, err := qv.applyAndPostingResultExpr(&acc, &hasAcc, plans[pi].expr)
		if err != nil {
			return postingResult{}, true, err
		}
		if done {
			return postingResult{}, true, nil
		}
	}

	for _, ni := range neg {
		done, err := qv.applyAndPostingResultExpr(&acc, &hasAcc, plans[ni].expr)
		if err != nil {
			return postingResult{}, true, err
		}
		if done {
			return postingResult{}, true, nil
		}
	}

	if !hasAcc {
		return postingResult{}, false, nil
	}
	return acc, true, nil
}

func (qv *View) applyAndMergedExactRangePostingResult(
	acc *postingResult,
	hasAcc *bool,
	e qir.Expr,
	bounds indexdata.Bounds,
) (bool, error) {

	b, ok := qv.evalMergedExactRangePostingResult(e, bounds)
	if !ok {
		return qv.applyAndPostingResultExpr(acc, hasAcc, e)
	}

	if b.ids.IsEmpty() {
		if *hasAcc {
			acc.ids.Release()
		}
		return true, nil
	}

	if !*hasAcc {
		*acc = b
		*hasAcc = true
		return false, nil
	}

	next, err := qv.andPostingResult(*acc, b)
	if err != nil {
		acc.ids.Release()
		return false, err
	}

	*acc = next
	if !acc.neg && acc.ids.IsEmpty() {
		acc.ids.Release()
		return true, nil
	}

	return false, nil
}

func (qv *View) evalMergedExactRangePostingResult(e qir.Expr, bounds indexdata.Bounds) (postingResult, bool) {
	if e.Not || e.FieldOrdinal < 0 {
		return postingResult{}, false
	}
	fm := qv.fieldMetaByExpr(e)
	if fm == nil || fm.Slice {
		return postingResult{}, false
	}

	ov := qv.fieldIndexViewFromSlotsForExpr(qv.snap.Index, e)
	if !ov.HasData() {
		return postingResult{}, false
	}

	var core preparedScalarRangePredicate

	qv.initPreparedExactScalarRangePredicate(&core, e, fm, bounds)
	if cached, ok := core.loadReuse.load(); ok {
		if cached.IsEmpty() {
			return postingResult{}, true
		}
		return postingResult{ids: cached}, true
	}

	br := ov.RangeForBounds(core.bounds)
	if br.Empty() {
		return postingResult{}, true
	}

	if core.expr.Op != qir.OpPREFIX {
		if out, ok := qv.tryEvalNumericRangeBuckets(qv.exec.FieldNameByOrdinal(core.expr.FieldOrdinal), core.fm, ov, br); ok {
			if out.ids.IsEmpty() {
				return postingResult{}, true
			}
			out.ids = core.secondHitReuse.share(out.ids)
			return out, true
		}
	}

	ids := ov.UnionRangePostings(br, indexdata.FieldIndexRange{})
	if ids.IsEmpty() {
		return postingResult{}, true
	}
	ids = core.secondHitReuse.share(ids)

	return postingResult{ids: ids}, true
}

// TryFilterCardinalityByScalarInSplit accelerates flat AND counts with a positive scalar IN leaf:
// it evaluates all non-IN leaves once into a postingResult filter and then sums
// per-value intersections against that combined filter.
func (qv *View) TryFilterCardinalityByScalarInSplit(expr qir.Expr, trace *Trace) (uint64, bool, error) {
	if expr.Not || expr.Op != qir.OpAND || len(expr.Operands) < 2 {
		return 0, false, nil
	}

	var leavesBuf [cardinalityPredicateScanMaxLeaves]qir.Expr
	leaves, ok := qir.CollectAndLeavesScratch(expr, leavesBuf[:0], qir.LeafModeCollect)

	if !ok || len(leaves) < 2 || len(leaves) > cardinalityPredicateScanMaxLeaves {
		return 0, false, nil
	}

	lead := -1
	leadVals := 0
	for i := range leaves {
		e := leaves[i]
		if e.Not || e.Op != qir.OpIN || e.FieldOrdinal < 0 {
			continue
		}
		fm := qv.fieldMetaByExpr(e)
		if fm == nil || fm.Slice {
			continue
		}
		valsBuf, isSlice, hasNil, err := qv.exprValueToDistinctIdxBuf(qir.Expr{
			Op:           qir.OpIN,
			FieldOrdinal: e.FieldOrdinal,
			Value:        e.Value,
		})
		totalVals := 0
		if valsBuf != nil {
			totalVals = len(valsBuf)
			pooled.ReleaseStringSlice(valsBuf)
		}
		if hasNil {
			totalVals++
		}
		if err != nil || !isSlice || totalVals < 2 || totalVals > cardinalityScalarInSplitMaxValues {
			continue
		}
		if lead == -1 || totalVals < leadVals {
			lead = i
			leadVals = totalVals
		}
	}

	if lead < 0 {
		return 0, false, nil
	}

	inLeaf := leaves[lead]
	ov := qv.fieldIndexViewFromSlotsForExpr(qv.snap.Index, inLeaf)
	if !ov.HasData() {
		return 0, false, nil
	}

	valsBuf, isSlice, hasNil, err := qv.exprValueToDistinctIdxBuf(qir.Expr{
		Op:           qir.OpIN,
		FieldOrdinal: inLeaf.FieldOrdinal,
		Value:        inLeaf.Value,
	})
	if valsBuf != nil {
		defer pooled.ReleaseStringSlice(valsBuf)
	}

	valCount := len(valsBuf)
	if err != nil || !isSlice || (valCount == 0 && !hasNil) {
		return 0, false, nil
	}

	totalVals := valCount
	if hasNil {
		totalVals++
	}

	if totalVals < 2 || totalVals > cardinalityScalarInSplitMaxValues {
		return 0, false, nil
	}

	var (
		filter    postingResult
		useFilter bool
	)
	if len(leaves) > 1 {
		filter, ok, err = qv.evalAndOperandsExceptReordered(leaves, lead)
		if err != nil {
			return 0, true, err
		}
		if !ok {
			return 0, false, nil
		}
		defer filter.ids.Release()

		if filter.ids.IsEmpty() {
			if filter.neg {
				if trace != nil {
					trace.SetPlan(planFilterCardinalityScalarInSplit)
					trace.AddExamined(0)
				}
				return 0, true, nil
			}
			return 0, true, nil
		}
		useFilter = true
	}

	var cnt uint64
	var examined uint64
	for i := 0; i < valCount; i++ {
		ids := ov.LookupPostingRetained(valsBuf[i])
		if ids.IsEmpty() {
			continue
		}
		examined += ids.Cardinality()
		if useFilter {
			cnt += postingAgainstResultCardinality(ids, filter)
			continue
		}
		cnt += ids.Cardinality()
	}

	if hasNil {
		ids := qv.fieldIndexViewFromSlotsForExpr(qv.snap.NilIndex, inLeaf).LookupPostingRetained(indexdata.NilIndexEntryKey)
		if !ids.IsEmpty() {
			examined += ids.Cardinality()
			if useFilter {
				cnt += postingAgainstResultCardinality(ids, filter)
			} else {
				cnt += ids.Cardinality()
			}
		}
	}

	if trace != nil {
		trace.SetPlan(planFilterCardinalityScalarInSplit)
		trace.AddExamined(examined)
	}

	return cnt, true, nil
}

func cardinalityLeavesForUniquePath(expr qir.Expr, dst []qir.Expr) ([]qir.Expr, bool) {
	if expr.Not {
		return nil, false
	}

	if expr.Op == qir.OpAND {
		leaves, ok := qir.CollectAndLeavesScratch(expr, dst, qir.LeafModeCollect)
		if !ok || len(leaves) == 0 {
			return nil, false
		}
		return leaves, true
	}

	if expr.Op == qir.OpOR || expr.Op == qir.OpNOOP || len(expr.Operands) != 0 {
		return nil, false
	}

	if cap(dst) == 0 {
		return []qir.Expr{expr}, true
	}

	dst = dst[:1]
	dst[0] = expr
	return dst, true
}

func (qv *View) uniqueCardinalityPathPrecheck(expr qir.Expr) (hasUnique bool, ok bool) {
	if expr.Not {
		return false, false
	}

	if expr.Op == qir.OpAND {
		if len(expr.Operands) == 0 {
			return false, false
		}
		var found bool
		for i := range expr.Operands {
			childHasUnique, childOK := qv.uniqueCardinalityPathPrecheck(expr.Operands[i])
			if !childOK {
				return false, false
			}
			if childHasUnique {
				found = true
			}
		}
		return found, true
	}

	if expr.Op == qir.OpOR || expr.Op == qir.OpNOOP || len(expr.Operands) != 0 {
		return false, false
	}

	return qv.isPositiveUniqueEqExpr(expr), true
}

// TryFilterCardinalityByUniqueEq counts expressions anchored by a positive EQ predicate on
// a unique scalar field, which bounds the candidate set to at most one row.
func (qv *View) TryFilterCardinalityByUniqueEq(expr qir.Expr, trace *Trace) (uint64, bool, error) {
	hasUnique, ok := qv.uniqueCardinalityPathPrecheck(expr)
	if !ok || !hasUnique {
		return 0, false, nil
	}

	var leavesBuf [cardinalityPredicateScanMaxLeaves]qir.Expr
	leaves, ok := cardinalityLeavesForUniquePath(expr, leavesBuf[:0])
	if !ok {
		return 0, false, nil
	}

	// Count paths defer broad numeric-range materialization until a lead is
	// chosen; otherwise NoCaching runs can rebuild huge range bitmaps per query.
	predSet, ok := qv.buildCardinalityPredicatesWithMode(leaves, false)
	if !ok {
		return 0, false, nil
	}
	defer predSet.Release()

	leadIdx := -1
	leadEst := uint64(0)
	for i := 0; i < predSet.Len(); i++ {
		p := predSet.owner[i]
		if p.alwaysFalse {
			return 0, true, nil
		}
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
		return 0, false, nil
	}
	// Unique predicates should be tiny;
	// keep this path conservative if data is unexpectedly inconsistent.
	if leadEst > 64 {
		return 0, false, nil
	}

	var checksBuf [cardinalityPredicateScanMaxLeaves]int
	checks := checksBuf[:0]

	for i := 0; i < predSet.Len(); i++ {
		if i == leadIdx {
			continue
		}
		p := predSet.owner[i]
		if p.covered || p.alwaysTrue {
			continue
		}
		if p.alwaysFalse {
			return 0, true, nil
		}
		if !p.hasContains() {
			return 0, false, nil
		}
		checks = append(checks, i)
	}
	sortActivePredicatesReader(checks, predSet.owner)

	it := (&predSet.owner[leadIdx]).newIter()
	if it == nil {
		return 0, false, nil
	}
	defer it.Release()

	var cnt uint64
	var examined uint64
	for it.HasNext() {
		idx := it.Next()
		examined++
		pass := true
		for _, ci := range checks {
			if !(&predSet.owner[ci]).matches(idx) {
				pass = false
				break
			}
		}
		if pass {
			cnt++
		}
	}

	if trace != nil {
		trace.SetPlan(planFilterCardinalityUniqueEq)
		trace.AddExamined(examined)
	}

	return cnt, true, nil
}

func shouldTryCardinalityByPredicates(leaves []qir.Expr) bool {
	if len(leaves) < 2 || len(leaves) > cardinalityPredicateScanMaxLeaves {
		return false
	}

	hasNeg := false
	hasComplex := false
	hasRange := false
	hasScalarEQ := false

	for i := range leaves {
		e := leaves[i]
		if e.Not {
			hasNeg = true
		}
		switch e.Op {
		case qir.OpPREFIX, qir.OpSUFFIX, qir.OpCONTAINS, qir.OpHASALL, qir.OpHASANY, qir.OpIN:
			hasComplex = true
		case qir.OpEQ:
			hasScalarEQ = e.FieldOrdinal >= 0 && len(e.Operands) == 0
		default:
			if e.Op.IsNumericRange() {
				hasRange = true
			}
		}
	}

	return hasNeg || hasComplex || (hasRange && (len(leaves) >= 3 || hasScalarEQ))
}

func shouldPreferMaterializedCardinalityEval(preds predicateReader, leadScore, universe uint64) bool {
	if universe == 0 || leadScore <= universe {
		return false
	}

	materializedPositive := 0
	for i := 0; i < len(preds); i++ {
		p := preds[i]
		if p.alwaysTrue || p.covered {
			continue
		}
		if p.isMaterializedLike() {
			if !p.expr.Not {
				materializedPositive++
			}
			continue
		}
		switch p.kind {
		case predicateKindPosting,
			predicateKindPostingNot,
			predicateKindPostsAny,
			predicateKindPostsAnyNot,
			predicateKindPostsAll,
			predicateKindPostsAllNot:
		default:
			return false
		}
	}
	return materializedPositive > 0
}

func shouldTryMaterializedCardinalityAND(expr qir.Expr) bool {
	if expr.Not || expr.Op != qir.OpAND || len(expr.Operands) < 2 {
		return false
	}

	var leavesBuf [cardinalityPredicateScanMaxLeaves]qir.Expr
	leaves, ok := qir.CollectAndLeavesScratch(expr, leavesBuf[:0], qir.LeafModeCollect)

	if !ok || len(leaves) < 2 || len(leaves) > cardinalityPredicateScanMaxLeaves {
		return false
	}

	hasSet := false
	hasRange := false
	for i := range leaves {
		e := leaves[i]
		if e.Op == qir.OpSUFFIX || e.Op == qir.OpCONTAINS {
			return false
		}
		if e.Not {
			continue
		}
		switch e.Op {
		case qir.OpHASANY, qir.OpHASALL:
			hasSet = true
		default:
			if e.Op.IsNumericRange() {
				hasRange = true
			}
		}
	}
	return hasSet && hasRange
}

func cardinalityBroadLeadResidualScoreLimit(leadEst, universe uint64) uint64 {
	if universe == 0 || leadEst <= universe/2 {
		return 0
	}
	// Broad leads must still keep residual check work near a small multiple of
	// the universe scan. As the lead narrows within the broad regime, allow more
	// residual score before falling back to postingResult materialization.
	return satAddUint64(satMulUint64(universe, 2), satMulUint64(universe-leadEst, 2))
}

func (qv *View) shouldRejectBroadLeadPredicateScan(preds predicateReader, active []int, leadEst, universe uint64) bool {
	limit := cardinalityBroadLeadResidualScoreLimit(leadEst, universe)
	if limit == 0 {
		return false
	}
	var residualScore uint64
	for _, pi := range active {
		residualScore = satAddUint64(residualScore, qv.cardinalityLeadResidualScore(preds[pi], leadEst, universe))
		if residualScore > limit {
			return true
		}
	}
	return false
}

func (qv *View) buildCardinalityPredicatesWithMode(leaves []qir.Expr, allowMaterialize bool) (predicateSet, bool) {
	owner := predicateSlicePool.Get(len(leaves))
	preds := predicateSet{owner: owner}

	if len(leaves) == 1 && leaves[0].Op == qir.OpNOOP && leaves[0].Not {
		preds.Append(predicate{alwaysFalse: true})
		return preds, true
	}

	var mergedRangesBuf []orderedMergedScalarRangeField
	if len(leaves) > 1 {
		var ok bool
		mergedRangesBuf = orderedMergedScalarRangeFieldSlicePool.Get(len(leaves))
		mergedRangesBuf, ok = qv.collectMergedNumericRangeFields(leaves, mergedRangesBuf)
		if !ok {
			preds.Release()
			orderedMergedScalarRangeFieldSlicePool.Put(mergedRangesBuf)
			return predicateSet{}, false
		}
		defer orderedMergedScalarRangeFieldSlicePool.Put(mergedRangesBuf)
	}

	for i, e := range leaves {
		if mergedRangesBuf != nil && qv.isPositiveMergedNumericRangeLeaf(e) {
			idx := findOrderedMergedScalarRangeField(mergedRangesBuf, qv.exec.FieldNameByOrdinal(e.FieldOrdinal))
			if idx >= 0 {
				merged := mergedRangesBuf[idx]
				if merged.count > 1 {
					if merged.first != i {
						continue
					}
					p, ok := qv.buildMergedScalarRangePredicate(merged.expr, merged.bounds, allowMaterialize)
					if !ok {
						preds.Release()
						return predicateSet{}, false
					}
					preds.Append(p)
					continue
				}
			}
		}

		p, ok := qv.buildPredicateWithMode(e, allowMaterialize)
		if !ok {
			preds.Release()
			return predicateSet{}, false
		}

		if p.postsAnyState != nil {
			if p.expr.Not {
				postsAnyFilterStatePool.Put(p.postsAnyState)
				p.postsAnyState = nil
			} else {
				p.postsAnyState.reuse = materializedPredReuse{}
			}
		}

		preds.Append(p)
	}

	return preds, true
}

type cardinalityORBranch struct {
	index        int
	preds        predicateSet
	lead         int
	checksLen    int
	leadInChecks bool
	checks       [cardinalityPredicateScanMaxLeaves]int
	matchWeight  uint64
	prevOrderLen int
	prevOrder    [cardinalityORPredicateMaxBranchesBase + 1]int
	est          uint64
}

func newCardinalityORBranch(index int, preds predicateSet, lead int, checks []int, est uint64) cardinalityORBranch {
	br := cardinalityORBranch{
		index:     index,
		preds:     preds,
		lead:      lead,
		checksLen: len(checks),
		est:       est,
	}
	copy(br.checks[:], checks)

	for _, pi := range checks {
		if pi == lead {
			br.leadInChecks = true
			break
		}
	}
	return br
}

func cardinalityORBranchDupOrderLess(branches []cardinalityORBranch, a, b int) bool {
	ba := branches[a]
	bb := branches[b]
	aw := ba.matchWeight
	if aw == 0 {
		aw = 1
	}
	bw := bb.matchWeight
	if bw == 0 {
		bw = 1
	}
	left := satMulUint64(ba.est, bw)
	right := satMulUint64(bb.est, aw)
	if left != right {
		return left > right
	}
	if aw != bw {
		return aw < bw
	}
	if ba.est != bb.est {
		return ba.est > bb.est
	}
	return a < b
}

func cardinalityORPreparePrevOrderBuf(branches []cardinalityORBranch) {
	if len(branches) <= 1 {
		return
	}
	for i := 1; i < len(branches); i++ {
		br := &branches[i]
		br.prevOrderLen = i
		for j := 0; j < i; j++ {
			br.prevOrder[j] = j
		}
		for j := 1; j < br.prevOrderLen; j++ {
			cur := br.prevOrder[j]
			k := j
			for k > 0 && cardinalityORBranchDupOrderLess(branches, cur, br.prevOrder[k-1]) {
				br.prevOrder[k] = br.prevOrder[k-1]
				k--
			}
			br.prevOrder[k] = cur
		}
	}
}

type cardinalityORMaterializedBranch struct {
	index int
	expr  qir.Expr
	est   uint64
}

type cardinalityORSeenMode uint8

const (
	cardinalityORSeenModeNone cardinalityORSeenMode = iota
	cardinalityORSeenModeHash
	cardinalityORSeenModePosting
)

const cardinalityORSeenHashMaxHint = u64SetPoolMaxCap / 2

type cardinalityORSeen struct {
	mode cardinalityORSeenMode
	hash u64set
	ids  posting.List
}

func cardinalityPredicateLeadPostingCount(p predicate) int {
	switch {
	case p.iterKind == predicateIterPosting && !p.posting.IsEmpty():
		return 1
	case p.kind == predicateKindPostsAny && p.iterKind == predicateIterPostsConcat && len(p.postsBuf) >= 2:
		return len(p.postsBuf)
	default:
		return 0
	}
}

func cardinalityPredicateLeadPostingAt(p predicate, i int) posting.List {
	if p.iterKind == predicateIterPosting {
		return p.posting
	}
	return p.postsBuf[i]
}

func cardinalityORBranchesUnionUpperBoundBuf(branches []cardinalityORBranch, universe uint64) uint64 {
	var unionUpper uint64
	for i := 0; i < len(branches); i++ {
		unionUpper += branches[i].est
		if universe > 0 && unionUpper >= universe {
			return universe
		}
	}
	return unionUpper
}

func cardinalityORBranchesUnionEstimateBuf(branches []cardinalityORBranch, universe uint64) uint64 {
	if universe == 0 {
		return cardinalityORBranchesUnionUpperBoundBuf(branches, 0)
	}
	if len(branches) == 0 {
		return 0
	}

	remainProb := 1.0
	for i := 0; i < len(branches); i++ {
		est := branches[i].est
		if est == 0 {
			continue
		}
		p := float64(est) / float64(universe)
		if p < 0 {
			p = 0
		}
		if p > 1 {
			p = 1
		}
		remainProb *= 1.0 - p
	}
	union := uint64((1.0-remainProb)*float64(universe) + 0.5)
	if union > universe {
		union = universe
	}
	if union == 0 {
		union = 1
	}
	return union
}

func (br *cardinalityORBranch) buildPostingFilterChecks(dst []int) []int {
	dst = dst[:0]
	for i := 0; i < br.checksLen; i++ {
		pi := br.checks[i]
		if br.preds.owner[pi].postingFilterCapability().supportsApply() {
			dst = append(dst, pi)
		}
	}
	return dst
}

func cardinalityPredicatesMatchSet(preds predicateSet, checks []int, idx uint64) bool {
	for _, pi := range checks {
		if !(&preds.owner[pi]).matches(idx) {
			return false
		}
	}
	return true
}

func (br *cardinalityORBranch) checksMatch(idx uint64) bool {
	for i := 0; i < br.checksLen; i++ {
		if !(&br.preds.owner[br.checks[i]]).matches(idx) {
			return false
		}
	}
	return true
}

func (br *cardinalityORBranch) matches(idx uint64) bool {
	if !br.leadInChecks && !(&br.preds.owner[br.lead]).matches(idx) {
		return false
	}
	return br.checksMatch(idx)
}

func cardinalityORBranchEstimateSet(preds predicateSet, active []int, leadEst uint64) uint64 {
	est := leadEst
	for _, pi := range active {
		p := preds.owner[pi]
		if p.covered || p.alwaysTrue || p.estCard == 0 {
			continue
		}
		if est == 0 || p.estCard < est {
			est = p.estCard
		}
	}
	if est == 0 {
		return 1
	}
	return est
}

func appendCardinalityORMaterializedBranch(
	branchTrace []TraceORBranch,
	materializedBranches []cardinalityORMaterializedBranch,
	materializedBranchEst uint64,
	branchIdx int,
	reason string,
	op qir.Expr,
	branchEst uint64,
) ([]cardinalityORMaterializedBranch, uint64) {

	if branchTrace != nil {
		branchTrace[branchIdx].Skipped = true
		branchTrace[branchIdx].SkipReason = reason
	}

	materializedBranches = append(materializedBranches, cardinalityORMaterializedBranch{
		index: branchIdx,
		expr:  op,
		est:   branchEst,
	})

	return materializedBranches, satAddUint64(materializedBranchEst, branchEst)
}

func shouldTryCardinalityORHybridMaterializedSpill(totalBranches int, scanBranches int, spillBranches int, expectedProbes uint64, spillEst uint64, universe uint64) bool {
	if totalBranches < 4 || scanBranches == 0 || spillBranches == 0 {
		return false
	}
	if spillBranches > cardinalityORHybridMaterializedBranchMax {
		return false
	}
	if universe == 0 {
		return false
	}
	if spillBranches >= scanBranches && expectedProbes > universe {
		return false
	}
	if spillBranches > 1 && spillEst > universe && expectedProbes > satMulUint64(universe, 3)/2 {
		return false
	}

	budget := satAddUint64(expectedProbes, spillEst)
	limit := satMulUint64(universe, 5) / 2
	if spillBranches == 1 || scanBranches >= spillBranches+1 {
		limit = satMulUint64(universe, 11) / 4
	}
	if limit < universe {
		limit = universe
	}
	return budget <= limit
}

func cardinalityORSeenUnionThreshold(universe uint64, branches int, expectedProbes uint64) uint64 {
	threshold := uint64(cardinalityORSeenUnionThresholdBase)
	if universe > 0 {
		dyn := universe / cardinalityORSeenUniverseDiv
		if dyn < threshold {
			threshold = dyn
		}
		if threshold < 16_384 {
			threshold = 16_384
		}
		if threshold > 256_000 {
			threshold = 256_000
		}
	}

	if branches >= 5 {
		threshold = threshold * 3 / 4
	} else if branches >= 4 {
		threshold = threshold * 7 / 8
	}

	if universe > 0 && expectedProbes > 0 {
		switch {
		case expectedProbes >= universe:
			threshold = threshold * 3 / 4
		case expectedProbes >= universe-universe/4:
			threshold = threshold * 7 / 8
		case expectedProbes <= universe/4:
			threshold = threshold * 9 / 8
		}
	}
	if threshold < 8_192 {
		threshold = 8_192
	}
	if threshold > 320_000 {
		threshold = 320_000
	}
	return threshold
}

func cardinalityORDedupDupShareBounds(branchCount int, universe uint64, expectedProbes uint64) (float64, float64) {
	minShare := 0.20
	forceShare := 0.33

	switch {
	case branchCount >= 6:
		minShare -= 0.03
		forceShare -= 0.05
	case branchCount >= 5:
		minShare -= 0.02
		forceShare -= 0.03
	}

	if universe > 0 {
		switch {
		case expectedProbes >= universe:
			minShare -= 0.02
			forceShare -= 0.03
		case expectedProbes > 0 && expectedProbes <= universe/4:
			minShare += 0.02
			forceShare += 0.03
		}
	}

	minShare = ClampFloat(minShare, 0.12, 0.30)
	forceShare = ClampFloat(forceShare, minShare+0.08, 0.45)
	return minShare, forceShare
}

func cardinalityORBranchesMatchWorkBuf(branches []cardinalityORBranch) uint64 {
	if len(branches) == 0 {
		return 0
	}
	var work uint64
	for i := 0; i < len(branches); i++ {
		br := branches[i]
		weight := br.matchWeight
		if weight == 0 {
			weight = 1
		}
		work = satAddUint64(work, satMulUint64(br.est, weight))
	}
	return work
}

func cardinalityORDedupWorkMultiplier(sumEst uint64, matchWork uint64) float64 {
	if sumEst == 0 || matchWork <= sumEst {
		return 1
	}
	mult := float64(matchWork) / float64(sumEst)
	if mult < 1 {
		return 1
	}
	if mult > 4 {
		return 4
	}
	return mult
}

func cardinalityORBranchesShouldUseSeenDedupBuf(branches []cardinalityORBranch, universe uint64, expectedProbes uint64) bool {
	if len(branches) <= 2 {
		return false
	}
	sumEst := cardinalityORBranchesUnionUpperBoundBuf(branches, 0)
	if sumEst == 0 {
		return false
	}
	matchWork := cardinalityORBranchesMatchWorkBuf(branches)
	if matchWork == 0 {
		return false
	}

	threshold := cardinalityORSeenUnionThreshold(universe, len(branches), expectedProbes)
	if matchWork < threshold {
		return false
	}

	workMult := cardinalityORDedupWorkMultiplier(sumEst, matchWork)
	if universe > 0 {
		unionEst := cardinalityORBranchesUnionEstimateBuf(branches, universe)
		if unionEst > 0 && sumEst > unionEst {
			dupEst := sumEst - unionEst
			dupShare := float64(dupEst) / float64(sumEst)
			dupShare *= workMult
			if dupShare > 1 {
				dupShare = 1
			}
			minDupShare, forceDupShare := cardinalityORDedupDupShareBounds(len(branches), universe, expectedProbes)
			if dupShare < minDupShare {
				return false
			}
			if dupShare >= forceDupShare {
				return true
			}
			if expectedProbes >= unionEst {
				return true
			}
			return false
		}
		if expectedProbes >= universe {
			return true
		}
		return false
	}

	if sumEst >= threshold {
		return true
	}
	return expectedProbes >= sumEst
}

func cardinalityORSeenCapHintBuf(branches []cardinalityORBranch, spillEst uint64, universe uint64) int {
	hint := satAddUint64(cardinalityORBranchesUnionEstimateBuf(branches, universe), spillEst)
	if universe > 0 && hint > universe {
		hint = universe
	}
	if hint < 64 {
		hint = 64
	}
	return clampUint64ToInt(hint)
}

func newCardinalityORSeenBuf(branches []cardinalityORBranch, spillEst uint64, universe uint64) cardinalityORSeen {
	hint := cardinalityORSeenCapHintBuf(branches, spillEst, universe)
	if hint <= cardinalityORSeenHashMaxHint {
		return cardinalityORSeen{
			mode: cardinalityORSeenModeHash,
			hash: getU64Set(hint),
		}
	}
	return cardinalityORSeen{mode: cardinalityORSeenModePosting}
}

func (s *cardinalityORSeen) release() {
	switch s.mode {
	case cardinalityORSeenModeHash:
		releaseU64Set(&s.hash)
	case cardinalityORSeenModePosting:
		s.ids.Release()
	}
	*s = cardinalityORSeen{}
}

func (s *cardinalityORSeen) Has(idx uint64) bool {
	switch s.mode {
	case cardinalityORSeenModeHash:
		return s.hash.Has(idx)
	case cardinalityORSeenModePosting:
		return s.ids.Contains(idx)
	default:
		return false
	}
}

func (s *cardinalityORSeen) Add(idx uint64) bool {
	switch s.mode {
	case cardinalityORSeenModeHash:
		return s.hash.Add(idx)
	case cardinalityORSeenModePosting:
		var added bool
		s.ids, added = s.ids.BuildAddedChecked(idx)
		return added
	default:
		return false
	}
}

func (s *cardinalityORSeen) addPosting(ids posting.List) uint64 {
	if ids.IsEmpty() {
		return 0
	}
	switch s.mode {

	case cardinalityORSeenModeHash:
		var added uint64
		it := ids.Iter()
		defer it.Release()
		for it.HasNext() {
			if s.hash.Add(it.Next()) {
				added++
			}
		}
		return added

	case cardinalityORSeenModePosting:
		before := s.ids.Cardinality()
		s.ids = s.ids.BuildOr(ids)
		after := s.ids.Cardinality()
		if after <= before {
			return 0
		}
		return after - before

	default:
		return 0
	}
}

func (s *cardinalityORSeen) addPostingOwned(ids posting.List) uint64 {
	if ids.IsEmpty() {
		return 0
	}
	switch s.mode {

	case cardinalityORSeenModeHash:
		added := s.addPosting(ids)
		ids.Release()
		return added

	case cardinalityORSeenModePosting:
		if s.ids.IsEmpty() {
			s.ids = ids
			return ids.Cardinality()
		}
		before := s.ids.Cardinality()
		s.ids = s.ids.BuildOr(ids)
		ids.Release()
		after := s.ids.Cardinality()
		if after <= before {
			return 0
		}
		return after - before

	default:
		ids.Release()
		return 0
	}
}

func (qv *View) cardinalityORMaterializedSpillUnion(
	branches []cardinalityORMaterializedBranch,
	seen *cardinalityORSeen,
	trace *Trace,
	branchTrace []TraceORBranch,
) (uint64, uint64, bool, error) {

	if len(branches) == 0 {
		return 0, 0, true, nil
	}

	var cnt uint64
	var examined uint64
	for _, br := range branches {
		if branchTrace != nil {
			branchTrace[br.index].Skipped = true
			branchTrace[br.index].SkipReason = "materialized_spill"
		}

		var (
			res postingResult
			err error
		)
		if !br.expr.Not && br.expr.Op == qir.OpAND && len(br.expr.Operands) > 1 {
			var ok bool
			res, ok, err = qv.evalAndOperandsExceptReordered(br.expr.Operands, -1)
			if err != nil {
				return 0, 0, true, err
			}
			if !ok {
				res, err = qv.evalExpr(br.expr)
				if err != nil {
					return 0, 0, true, err
				}
			}

		} else {
			res, err = qv.evalExpr(br.expr)
			if err != nil {
				return 0, 0, true, err
			}
		}

		if res.neg {
			res.ids.Release()
			return 0, 0, false, nil
		}
		if trace != nil {
			trace.AddPostingMaterialization(1)
		}
		if res.ids.IsEmpty() {
			res.ids.Release()
			continue
		}

		card := res.ids.Cardinality()
		examined += card
		added := seen.addPostingOwned(res.ids)
		res.ids = posting.List{}
		cnt += added
		if branchTrace != nil {
			branchTrace[br.index].RowsExamined += card
			branchTrace[br.index].RowsEmitted += added
		}
		res.ids.Release()
	}

	return cnt, examined, true, nil
}

func (qv *View) tryFilterCardinalityORBranchLeadPostingsBuf(
	branches []cardinalityORBranch,
	branchIdx int,
	useSeenDedup bool,
	seen *cardinalityORSeen,
	trace *TraceORBranch,
) (uint64, uint64, bool) {

	if branchIdx < 0 || branchIdx >= len(branches) {
		return 0, 0, false
	}

	br := &branches[branchIdx]
	if br.lead < 0 || br.lead >= br.preds.Len() {
		return 0, 0, false
	}

	lead := br.preds.owner[br.lead]
	leadPostCount := cardinalityPredicateLeadPostingCount(lead)
	if leadPostCount == 0 {
		return 0, 0, false
	}

	var checksBuf [cardinalityPredicateScanMaxLeaves]int
	checks := checksBuf[:br.checksLen]
	copy(checks, br.checks[:br.checksLen])

	var exactChecksBuf [cardinalityPredicateScanMaxLeaves]int
	var extraExactCandidatesBuf [cardinalityPredicateScanMaxLeaves]int
	var residualExactBuf [cardinalityPredicateScanMaxLeaves]int
	var residualBothBuf [cardinalityPredicateScanMaxLeaves]int

	exactChecks := br.buildPostingFilterChecks(exactChecksBuf[:0])
	extraExactCandidates := qv.collectCardinalityLeadResidualExactCandidatesInto(
		extraExactCandidatesBuf[:0],
		br.preds.owner,
		checks,
		exactChecks,
	)
	residualExact := residualExactBuf[:0]
	residualBoth := residualBothBuf[:0]

	for _, pi := range checks {
		if cardinalityIndexSliceContains(exactChecks, pi) {
			continue
		}
		residualExact = append(residualExact, pi)
		residualBoth = append(residualBoth, pi)
	}

	var extraExactBuf []cardinalityLeadResidualExactFilter
	extraExactBuilt := len(extraExactCandidates) == 0

	var bucketWork posting.List
	var extraWork posting.List

	var cnt uint64
	var examined uint64
	for i := 0; i < leadPostCount; i++ {
		ids := cardinalityPredicateLeadPostingAt(lead, i)
		if ids.IsEmpty() {
			continue
		}
		card := ids.Cardinality()
		examined += card
		if trace != nil {
			trace.RowsExamined += card
		}
		if len(checks) == 0 {
			added := cardinalityORBranchAcceptPosting(branches, branchIdx, br, useSeenDedup, seen, ids, nil)
			cnt += added
			if trace != nil {
				trace.RowsEmitted += added
			}
			continue
		}

		if idx, ok := ids.TrySingle(); ok {
			added := cardinalityORBranchAcceptIdx(branches, branchIdx, br, useSeenDedup, seen, idx, checks)
			cnt += added
			if trace != nil {
				trace.RowsEmitted += added
			}
			continue
		}

		if len(exactChecks) == 0 && len(extraExactBuf) == 0 {
			added := cardinalityORBranchAcceptPosting(branches, branchIdx, br, useSeenDedup, seen, ids, checks)
			cnt += added
			if trace != nil {
				trace.RowsEmitted += added
			}
			continue
		}

		current := ids
		exactApplied := false
		if len(exactChecks) > 0 {
			mode, exactIDs, nextBucketWork, _ := plannerFilterPostingByPredicateChecks(br.preds.owner, exactChecks, ids, bucketWork, true)
			bucketWork = nextBucketWork
			switch mode {
			case plannerPredicateBucketEmpty:
				continue
			case plannerPredicateBucketAll:
				current = exactIDs
				exactApplied = true
			case plannerPredicateBucketExact:
				if exactIDs.IsEmpty() {
					continue
				}
				current = exactIDs
				exactApplied = true
			}
		}

		extraApplied := false
		if extraExactBuilt || shouldBuildCardinalityLeadResidualExactFilters(current, len(extraExactCandidates)) {
			extraExactBuf, extraExactBuilt, residualBoth = qv.ensureCardinalityLeadResidualExactFiltersSet(
				br.preds,
				extraExactCandidates,
				residualExact,
				residualBoth,
				extraExactBuf,
				extraExactBuilt,
			)
			if shouldApplyCardinalityLeadResidualExactFilters(current, extraExactBuf) {
				filtered, nextExtraWork := applyCardinalityLeadResidualExactFilters(current, extraWork, extraExactBuf)
				extraWork = nextExtraWork
				if filtered.IsEmpty() {
					continue
				}
				current = filtered
				extraApplied = true
			}
		}

		checkList := checks
		if extraApplied {
			checkList = residualBoth
		} else if exactApplied {
			checkList = residualExact
		}

		added := cardinalityORBranchAcceptPosting(branches, branchIdx, br, useSeenDedup, seen, current, checkList)
		cnt += added
		if trace != nil {
			trace.RowsEmitted += added
		}
	}

	if extraExactBuf != nil {
		cardinalityLeadResidualExactFilterSlicePool.Put(extraExactBuf)
	}
	bucketWork.Release()
	extraWork.Release()

	return cnt, examined, true
}

func cardinalitySetMaterializeMinTerms(probeEst uint64, universe uint64) int {
	var terms int
	if probeEst >= 2_048 {
		terms = cardinalitySetMaterializeMinTermsBase
	} else if probeEst >= 1_024 {
		terms = 7
	} else {
		terms = 8
	}

	if universe > 0 {
		switch {
		case universe <= 64_000:
			terms++
		case universe >= 4_000_000 && probeEst >= 4_096:
			terms--
		case universe >= 1_000_000 && probeEst >= 2_048:
			terms--
		}
	}
	if terms < 4 {
		terms = 4
	}
	if terms > 10 {
		terms = 10
	}
	return terms
}

func cardinalitySetMaterializeMinProbe(termCount int, probeEst uint64, universe uint64) uint64 {
	var minProbe uint64
	switch {
	case termCount >= 24:
		minProbe = 384
	case termCount >= 12:
		minProbe = 640
	case termCount >= 8:
		minProbe = 896
	default:
		minProbe = 1_280
	}
	if probeEst > 0 {
		switch {
		case probeEst <= 1_024:
			minProbe = minProbe * 5 / 4
		case probeEst >= 16_384:
			minProbe = minProbe * 7 / 8
		}
	}

	if universe > 0 {
		floor := universe / 512
		if floor < 256 {
			floor = 256
		}
		if floor > 4_096 {
			floor = 4_096
		}
		if termCount >= 16 {
			floor = floor * 4 / 5
		} else if termCount <= 6 {
			floor = floor * 5 / 4
		}
		if minProbe < floor {
			minProbe = floor
		}
	}
	if minProbe < 192 {
		minProbe = 192
	}
	if minProbe > 8_192 {
		minProbe = 8_192
	}
	return minProbe
}

func shouldMaterializeCardinalitySetPredicate(p predicate, probeEst uint64, universe uint64) bool {
	switch p.kind {
	case predicateKindPostsAny,
		predicateKindPostsAnyNot,
		predicateKindPostsAll,
		predicateKindPostsAllNot:
	default:
		return false
	}
	l := len(p.postsBuf)
	if l <= 1 {
		return false
	}
	if l < cardinalitySetMaterializeMinTerms(probeEst, universe) {
		return false
	}
	return probeEst >= cardinalitySetMaterializeMinProbe(l, probeEst, universe)
}

func cardinalityCustomMaterializeMinProbe(op qir.Op, est uint64, probeEst uint64, universe uint64) uint64 {
	minProbe := uint64(cardinalityCustomMaterializeMinProbeBase)
	switch op {
	case qir.OpPREFIX:
		minProbe = 3_072
	case qir.OpSUFFIX:
		minProbe = 6_144
	case qir.OpCONTAINS:
		minProbe = 8_192
	}

	if est > 0 {
		if est <= minProbe/6 {
			minProbe = minProbe * 3 / 2
		} else if est >= minProbe*8 {
			minProbe = minProbe * 3 / 4
		}
	}
	if probeEst > 0 {
		switch {
		case probeEst <= minProbe/2:
			minProbe = minProbe * 5 / 4
		case probeEst >= minProbe*2:
			minProbe = minProbe * 7 / 8
		}
	}

	if universe > 0 {
		div := uint64(320)
		switch op {
		case qir.OpPREFIX:
			div = 384
		case qir.OpCONTAINS:
			div = 256
		}
		floor := universe / div
		if floor < 1_024 {
			floor = 1_024
		}
		if floor > 12_288 {
			floor = 12_288
		}
		if minProbe < floor {
			minProbe = floor
		}
	}
	if minProbe < 768 {
		minProbe = 768
	}
	if minProbe > 16_384 {
		minProbe = 16_384
	}
	return minProbe
}

func shouldMaterializeCustomCardinalityPredicate(p predicate, probeEst uint64, universe uint64) bool {
	if !p.isCustomUnmaterialized() {
		return false
	}
	if p.alwaysTrue || p.alwaysFalse || p.covered {
		return false
	}
	switch p.expr.Op {
	case qir.OpPREFIX, qir.OpSUFFIX, qir.OpCONTAINS:
		return probeEst >= cardinalityCustomMaterializeMinProbe(p.expr.Op, p.estCard, probeEst, universe)
	default:
		return false
	}
}

type cardinalityScalarComplementRouting struct {
	coarseMaterialize  bool
	exactMaterialize   bool
	complementCacheKey qcache.MaterializedPredKey
}

func (route cardinalityScalarComplementRouting) wantsComplementMaterialization() bool {
	return route.coarseMaterialize || route.exactMaterialize
}

func (route cardinalityScalarComplementRouting) prefersLazyPostingFilter(p predicate, snap *snapshot.View) bool {
	if !p.isCustomUnmaterialized() || !p.postingFilterCapability().isCheap() {
		return false
	}
	if !route.wantsComplementMaterialization() {
		return false
	}
	if snap == nil || snap.MaterializedPredCacheLimit() <= 0 {
		return true
	}
	if route.complementCacheKey.IsZero() {
		return true
	}
	return !snap.ShouldPromoteRuntimeMaterializedPredKey(route.complementCacheKey)
}

func (qv *View) cardinalityScalarComplementRouting(p predicate, leadProbeEst uint64, universe uint64) cardinalityScalarComplementRouting {
	if universe == 0 || leadProbeEst == 0 || !p.isCustomUnmaterialized() || p.expr.Not || !p.expr.Op.IsNumericRange() {
		return cardinalityScalarComplementRouting{}
	}

	candidate, ok := qv.preparePredicateScalarRangeRoutingCandidate(p)
	if !ok {
		return cardinalityScalarComplementRouting{}
	}

	est := candidate.plan.est
	route := cardinalityScalarComplementRouting{
		exactMaterialize:   candidate.numeric,
		complementCacheKey: candidate.core.complementCacheKey,
	}
	if leadProbeEst >= est || est < cardinalityBroadRangeLazyMinCard || est >= universe {
		return route
	}
	threshold := universe - universe/4
	route.coarseMaterialize = est >= threshold
	return route
}

func cardinalityBroadRangeComplementMaxCardinality(leadProbeEst, universe uint64) uint64 {
	limit := uint64(cardinalityBroadRangeComplementMaxCard)
	if leadProbeEst == 0 || universe == 0 {
		return limit
	}
	// Only widen beyond the base cap when the chosen lead is already narrow
	// relative to the full result universe; otherwise complement setup cost can
	// dominate and broad AND-counts regress toward postingResult-first behavior.
	if leadProbeEst > universe/4 {
		return limit
	}
	dyn := leadProbeEst + leadProbeEst/4
	if dyn > limit {
		limit = dyn
	}
	theoreticalCap := universe / 4
	if limit > theoreticalCap {
		limit = theoreticalCap
	}
	if limit > uint64(cardinalityBroadRangeComplementMaxCardCap) {
		limit = uint64(cardinalityBroadRangeComplementMaxCardCap)
	}
	return limit
}

func shouldUseFastCardinalityBroadRangeComplementMaterializationForShape(probeLen int, est uint64) bool {
	if probeLen < cardinalityBroadRangeComplementFastProbeMin || est == 0 {
		return false
	}
	avgPerBucket := est / uint64(probeLen)
	if avgPerBucket == 0 {
		avgPerBucket = 1
	}
	return avgPerBucket <= cardinalityBroadRangeComplementFastAvgPerBucketMax
}

func (qv *View) tryMaterializeBroadRangeComplementPredicateForCardinality(p *predicate, leadProbeEst uint64, universe uint64, trace *Trace) bool {
	if p == nil {
		return false
	}
	route := qv.cardinalityScalarComplementRouting(*p, leadProbeEst, universe)
	if !route.wantsComplementMaterialization() {
		return false
	}

	fm := qv.fieldMetaByExpr(p.expr)
	if fm == nil || fm.Slice {
		return false
	}
	if !route.coarseMaterialize && !schema.FieldUsesOrderedNumericKeys(fm) {
		return false
	}
	candidate, ok := qv.preparePredicateScalarRangeRoutingCandidate(*p)
	if !ok {
		return false
	}
	plan, cached, cacheHit, empty, ok := candidate.core.loadComplementMaterialization()
	if !ok {
		return false
	}
	if cacheHit {
		if cached.IsEmpty() {
			if trace != nil {
				trace.AddCountRangeComplementCacheHit(1)
			}
			setPredicateAlwaysTrue(p)
			return true
		}

		if trace != nil {
			trace.AddCountRangeComplementCacheHit(1)
		}
		setPredicateMaterializedNot(p, cached)
		return true
	}

	if empty {
		storeEmptyScalarComplementMaterialization(plan)
		setPredicateAlwaysTrue(p)
		return true
	}
	if plan.est > cardinalityBroadRangeComplementMaxCardinality(leadProbeEst, universe) {
		return false
	}

	ids := candidate.core.materializeComplement(plan)
	if ids.IsEmpty() {
		storeEmptyScalarComplementMaterialization(plan)
		setPredicateAlwaysTrue(p)
		return true
	}
	if trace != nil {
		trace.AddCountRangeComplementBuild(ids.Cardinality(), false)
	}

	ids = plan.sharedReuse.share(ids)
	setPredicateMaterializedNot(p, ids)
	return true
}

func (qv *View) materializePostingIntersection(posts []posting.List) posting.List {
	if len(posts) == 0 {
		return posting.List{}
	}

	seed := -1
	var seedCard uint64
	for i := 0; i < len(posts); i++ {
		p := posts[i]
		c := p.Cardinality()
		if c == 0 {
			return posting.List{}
		}
		if seed == -1 || c < seedCard {
			seed = i
			seedCard = c
		}
	}

	out := posts[seed].Clone()
	for i := 0; i < len(posts); i++ {
		if i == seed || out.IsEmpty() {
			continue
		}
		out = out.BuildAnd(posts[i])
	}
	return out.BuildOptimized()
}

func (qv *View) materializeSetPredicateForCardinality(p *predicate) bool {
	if p == nil {
		return false
	}

	origKind := p.kind
	var ids posting.List

	switch origKind {
	case predicateKindPostsAny, predicateKindPostsAnyNot:
		ids = materializePostingUnionBufOwned(p.postsBuf)
	case predicateKindPostsAll, predicateKindPostsAllNot:
		ids = qv.materializePostingIntersection(p.postsBuf)
	default:
		return false
	}

	isNot := origKind == predicateKindPostsAnyNot || origKind == predicateKindPostsAllNot
	if ids.IsEmpty() {
		releasePredicateOwnedState(p)
		p.kind = predicateKindCustom
		p.iterKind = predicateIterNone
		p.posting = posting.List{}
		p.ids = posting.List{}
		p.contains = nil
		p.iter = nil
		p.bucketCount = nil
		p.estCard = 0
		p.alwaysTrue = isNot
		p.alwaysFalse = !isNot
		return true
	}

	releasePredicateOwnedState(p)
	p.posting = posting.List{}
	p.ids = ids
	p.releaseIDs = true
	p.contains = nil
	p.iter = nil
	p.bucketCount = nil
	p.estCard = ids.Cardinality()
	p.alwaysTrue = false
	p.alwaysFalse = false

	switch origKind {

	case predicateKindPostsAny, predicateKindPostsAll:
		p.kind = predicateKindMaterialized
		p.iterKind = predicateIterMaterialized

	case predicateKindPostsAnyNot, predicateKindPostsAllNot:
		p.kind = predicateKindMaterializedNot
		p.iterKind = predicateIterNone
	}
	return true
}

func (qv *View) materializeCustomPredicateForCardinality(p *predicate) error {
	if p == nil {
		return nil
	}

	raw := p.expr
	raw.Not = false
	ids := qv.evalLazyMaterializedPredicateWithKey(raw, qv.materializedPredKey(raw))

	if ids.IsEmpty() {
		releasePredicateOwnedState(p)
		p.kind = predicateKindCustom
		p.iterKind = predicateIterNone
		p.posting = posting.List{}
		p.ids = posting.List{}
		p.contains = nil
		p.iter = nil
		p.bucketCount = nil
		p.estCard = 0
		p.alwaysTrue = p.expr.Not
		p.alwaysFalse = !p.expr.Not
		return nil
	}

	releasePredicateOwnedState(p)

	p.posting = posting.List{}
	p.contains = nil
	p.iter = nil
	p.bucketCount = nil
	p.ids = ids
	p.releaseIDs = true
	p.alwaysTrue = false
	p.alwaysFalse = false

	if p.expr.Not {
		p.kind = predicateKindMaterializedNot
		p.iterKind = predicateIterNone
		p.estCard = 0
	} else {
		p.kind = predicateKindMaterialized
		p.iterKind = predicateIterMaterialized
		p.estCard = ids.Cardinality()
	}
	return nil
}

func (qv *View) prepareCardinalityPredicate(p *predicate, probeEst uint64, universe uint64) error {
	return qv.prepareCardinalityPredicateWithTrace(p, probeEst, universe, nil)
}

func (qv *View) prepareCardinalityPredicateWithTrace(p *predicate, probeEst uint64, universe uint64, trace *Trace) error {
	if p == nil || p.alwaysTrue || p.alwaysFalse || p.covered {
		return nil
	}
	if trace != nil {
		trace.AddCountPredicatePreparation(1)
	}
	if probeEst > 0 {
		p.setExpectedContainsCalls(clampUint64ToInt(probeEst))
	}

	if shouldMaterializeCardinalitySetPredicate(*p, probeEst, universe) {
		qv.materializeSetPredicateForCardinality(p)
	}

	route := qv.cardinalityScalarComplementRouting(*p, probeEst, universe)
	if route.prefersLazyPostingFilter(*p, qv.snap) {
		return nil
	}

	if route.wantsComplementMaterialization() &&
		qv.tryMaterializeBroadRangeComplementPredicateForCardinality(p, probeEst, universe, trace) {
		return nil
	}

	if shouldMaterializeCustomCardinalityPredicate(*p, probeEst, universe) {
		if err := qv.materializeCustomPredicateForCardinality(p); err != nil {
			return err
		}
	}

	return nil
}

// TryFilterCardinalityByPredicates counts AND expressions by scanning a selective lead
// predicate and validating remaining predicates via contains checks.
//
// It exists to avoid materializing full postingResult intermediates when a lead can
// cheaply prune the candidate space.
func (qv *View) TryFilterCardinalityByPredicates(expr qir.Expr, trace *Trace) (uint64, bool, error) {
	if expr.Not || expr.Op != qir.OpAND || len(expr.Operands) < 2 {
		return 0, false, nil
	}

	var leavesBuf [cardinalityPredicateScanMaxLeaves]qir.Expr
	leaves, ok := qir.CollectAndLeavesScratch(expr, leavesBuf[:0], qir.LeafModeCollect)
	if !ok || !shouldTryCardinalityByPredicates(leaves) {
		return 0, false, nil
	}

	// Count paths defer broad numeric-range materialization until a lead is
	// chosen; otherwise NoCaching runs can rebuild huge range bitmaps per query.
	predSet, ok := qv.buildCardinalityPredicatesWithMode(leaves, false)
	if !ok {
		return 0, false, nil
	}

	universe := qv.snap.Universe.Cardinality()
	if universe == 0 {
		predSet.Release()
		return 0, true, nil
	}

	// Prefer leads that stay cheap under repeated predicate scans; broad
	// HASANY unions are better left to postingResult fallback than to posts_union scans.
	for i := 0; i < predSet.Len(); i++ {
		if predSet.owner[i].alwaysFalse {
			predSet.Release()
			return 0, true, nil
		}
	}
	leadIdx, leadEst, leadScore := qv.pickCardinalityLeadPredicate(predSet.owner, universe)
	if leadIdx < 0 {
		predSet.Release()
		return 0, false, nil
	}
	if shouldPreferMaterializedCardinalityEval(predSet.owner, leadScore, universe) {
		predSet.Release()
		out, err := qv.FilterCardinalityByMaterializedExpr(expr, trace)
		return out, true, err
	}
	defer predSet.Release()

	if err := qv.prepareCardinalityPredicateWithTrace((&predSet.owner[leadIdx]), leadEst, universe, trace); err != nil {
		return 0, true, err
	}
	if predSet.owner[leadIdx].alwaysFalse {
		return 0, true, nil
	}
	if !(&predSet.owner[leadIdx]).hasIter() {
		return 0, false, nil
	}
	if predSet.owner[leadIdx].estCard > 0 {
		leadEst = predSet.owner[leadIdx].estCard
	}
	leadNeedsCheck := (&predSet.owner[leadIdx]).leadIterNeedsContainsCheck()

	// remaining predicates are ordered by expected check cost/selectivity so we fail fast on likely negatives.
	var activeBuf [cardinalityPredicateScanMaxLeaves]int
	active := activeBuf[:0]
	for i := 0; i < predSet.Len(); i++ {
		if i == leadIdx && !leadNeedsCheck {
			continue
		}
		p := predSet.owner[i]
		if p.covered || p.alwaysTrue {
			continue
		}
		if p.alwaysFalse {
			return 0, true, nil
		}
		active = append(active, i)
	}

	// Broad leads are only worth predicate scans when the predicted residual
	// check score stays near the universe scan budget. Use the same residual
	// model as lead picking instead of a fixed active-count gate so cheap
	// materialized/exact-filter residuals can still stay on the count fast path.
	if !(&predSet.owner[leadIdx]).isMaterializedLike() && qv.shouldRejectBroadLeadPredicateScan(predSet.owner, active, leadEst, universe) {
		out, err := qv.FilterCardinalityByMaterializedExpr(expr, trace)
		return out, true, err
	}
	for _, pi := range active {
		if err := qv.prepareCardinalityPredicateWithTrace((&predSet.owner[pi]), leadEst, universe, trace); err != nil {
			return 0, true, err
		}
	}

	write := 0
	for read := 0; read < len(active); read++ {
		pi := active[read]
		p := predSet.owner[pi]
		if p.covered || p.alwaysTrue {
			continue
		}
		if p.alwaysFalse {
			return 0, true, nil
		}
		if !p.hasContains() {
			return 0, false, nil
		}
		active[write] = pi
		write++
	}

	active = active[:write]
	sortActivePredicatesReader(active, predSet.owner)

	// For range/prefix leads on stable base slices, count by buckets first:
	// many buckets can be skipped (or fully counted) via bucketCount hooks.
	if cnt, examined, ok := qv.TryFilterCardinalityByPredicatesLeadBuckets(predSet, leadIdx, active); ok {
		if trace != nil {
			trace.SetPlan(planFilterCardinalityPredicates)
			trace.AddExamined(examined)
		}
		return cnt, true, nil
	}

	// Posting-backed leads can count posting-by-posting so exact-capable
	// residual predicates prune whole posting bitmaps before row fallback.
	if cnt, examined, ok := qv.TryFilterCardinalityByPredicatesLeadPostings(predSet, leadIdx, active); ok {
		if trace != nil {
			trace.SetPlan(planFilterCardinalityPredicates)
			trace.AddExamined(examined)
		}
		return cnt, true, nil
	}

	it := (&predSet.owner[leadIdx]).newIter()
	if it == nil {
		return 0, false, nil
	}
	defer it.Release()

	var cnt uint64
	var examined uint64
	for it.HasNext() {
		idx := it.Next()
		examined++
		pass := true
		for _, pi := range active {
			if !(&predSet.owner[pi]).matches(idx) {
				pass = false
				break
			}
		}
		if pass {
			cnt++
		}
	}

	if trace != nil {
		trace.SetPlan(planFilterCardinalityPredicates)
		trace.AddExamined(examined)
	}

	return cnt, true, nil
}

func cardinalityNumericBucketResidualsExact(preds predicateReader, active []int) bool {
	if len(active) == 0 {
		return false
	}
	for _, pi := range active {
		if preds[pi].expr.Op.IsNumericRange() {
			return false
		}
		capability := preds[pi].postingFilterCapability()
		if !capability.supportsExactBucket() || !capability.isCheap() {
			return false
		}
	}
	return true
}

func cardinalityNumericBucketResidualsCountable(preds predicateReader, active []int) bool {
	hasRange := false
	for _, pi := range active {
		if preds[pi].expr.Op.IsNumericRange() {
			if hasRange {
				return false
			}
			hasRange = true
			continue
		}
		if preds[pi].expr.Not {
			return false
		}
		capability := preds[pi].postingFilterCapability()
		if !capability.supportsExactBucket() || !capability.isCheap() {
			return false
		}
	}
	return hasRange
}

func cardinalityCountPredicateBucket(preds predicateSet, checks []int, ids posting.List, work posting.List) (uint64, posting.List) {
	if ids.IsEmpty() {
		return 0, work
	}
	if len(checks) == 0 {
		return ids.Cardinality(), work
	}
	if idx, ok := ids.TrySingle(); ok {
		if cardinalityPredicatesMatchSet(preds, checks, idx) {
			return 1, work
		}
		return 0, work
	}
	if len(checks) == 1 {
		if in, ok := preds.owner[checks[0]].countBucket(ids); ok {
			return in, work
		}
	}

	mode, exactIDs, nextWork, card := plannerFilterPostingByPredicateChecks(preds.owner, checks, ids, work, true)
	work = nextWork
	switch mode {
	case plannerPredicateBucketEmpty:
		return 0, work
	case plannerPredicateBucketAll:
		return card, work
	case plannerPredicateBucketExact:
		return exactIDs.Cardinality(), work
	default:
		return postingMatchesMultiSetCardinality(preds, checks, ids), work
	}
}

func countCardinalityNumericBucketRange(
	preds predicateSet,
	active []int,
	ov indexdata.FieldIndexView,
	start, end int,
	work posting.List,
) (uint64, uint64, posting.List) {

	if start >= end {
		return 0, 0, work
	}
	ids := ov.UnionRangePostings(ov.RangeByRanks(start, end), indexdata.FieldIndexRange{})
	if ids.IsEmpty() {
		return 0, 0, work
	}
	examined := ids.Cardinality()
	cnt, work := cardinalityCountPredicateBucket(preds, active, ids, work)
	ids.Release()
	return cnt, examined, work
}

func (qv *View) TryFilterCardinalityByPredicatesNumericBucketPosting(
	preds predicateSet,
	lead *predicate,
	ov indexdata.FieldIndexView,
	br indexdata.FieldIndexRange,
	field string,
	fm *schema.Field,
	active []int,
) (uint64, uint64, bool) {

	if fm == nil || br.Empty() {
		return 0, 0, false
	}
	if br.BaseEnd-br.BaseStart < cardinalityNumericBucketExactMinRows {
		return 0, 0, false
	}
	if !cardinalityNumericBucketResidualsCountable(preds.owner, active) {
		return 0, 0, false
	}
	rows := ov.RangeRows(br.BaseStart, br.BaseEnd)
	if rows < cardinalityNumericBucketExactMinRows {
		return 0, 0, false
	}
	if lead.fieldIndexRangeState != nil && !lead.expr.Not {
		leadIDs := lead.fieldIndexRangeState.materializeRange()
		examined := leadIDs.Cardinality()
		if len(active) == 1 {
			p := &preds.owner[active[0]]
			if p.fieldIndexRangeState != nil && !p.expr.Not {
				ids := p.fieldIndexRangeState.materializeRange()
				return leadIDs.AndCardinality(ids), examined, true
			}
		}
		var work posting.List
		cnt, work := cardinalityCountPredicateBucket(preds, active, leadIDs, work)
		work.Release()
		return cnt, examined, true
	}
	res, ok := qv.tryEvalNumericRangeBuckets(field, fm, ov, br)
	if !ok {
		return 0, 0, false
	}
	examined := res.ids.Cardinality()

	if len(active) == 1 {
		p := &preds.owner[active[0]]
		if p.rangeMat || p.kind == predicateKindMaterialized {
			cnt := res.ids.AndCardinality(p.ids)
			res.ids.Release()
			return cnt, examined, true
		}
		if p.kind == predicateKindMaterializedNot {
			excluded := res.ids.AndCardinality(p.ids)
			cnt := uint64(0)
			if excluded < examined {
				cnt = examined - excluded
			}
			res.ids.Release()
			return cnt, examined, true
		}
	}

	if len(active) == 0 {
		cnt := examined
		res.ids.Release()
		return cnt, examined, true
	}

	var work posting.List
	cnt, work := cardinalityCountPredicateBucket(preds, active, res.ids, work)
	work.Release()
	res.ids.Release()
	return cnt, examined, true
}

func (qv *View) TryFilterCardinalityByPredicatesNumericBuckets(
	preds predicateSet,
	ov indexdata.FieldIndexView,
	br indexdata.FieldIndexRange,
	field string,
	fm *schema.Field,
	active []int,
) (uint64, uint64, bool) {

	if fm == nil || !cardinalityNumericBucketResidualsExact(preds.owner, active) || br.Empty() {
		return 0, 0, false
	}
	if !schema.FieldUsesOrderedNumericKeys(fm) {
		return 0, 0, false
	}

	bucketSize := qv.exec.NumericRangeBucketSize
	minFieldKeys := qv.exec.NumericRangeBucketMinFieldKeys
	minSpan := qv.exec.NumericRangeBucketMinSpanKeys
	if bucketSize <= 0 || minFieldKeys <= 0 || minSpan <= 0 {
		return 0, 0, false
	}

	span := br.BaseEnd - br.BaseStart
	if span < minSpan {
		return 0, 0, false
	}

	storage, ok := qv.snap.FieldIndexStorage(field)
	if !ok || storage.KeyCount() == 0 {
		return 0, 0, false
	}

	entry := qv.numericRangeBucketCacheEntry(field, storage, bucketSize, minFieldKeys)
	if entry == nil {
		return 0, 0, false
	}

	idx := entry.Index()
	if idx.BucketCount() == 0 || idx.KeyCount() != ov.KeyCount() {
		return 0, 0, false
	}

	startFull, endFull, ok := idx.FullBucketSpan(br)
	if !ok {
		return 0, 0, false
	}

	rows := ov.RangeRows(br.BaseStart, br.BaseEnd)
	if rows < cardinalityNumericBucketExactMinRows {
		return 0, 0, false
	}

	if qv.snap.AllowsMaterializedPredCard(rows) {
		res, ok := qv.tryEvalNumericRangeBuckets(field, fm, ov, br)
		if ok {
			examined := res.ids.Cardinality()
			var work posting.List
			cnt, work := cardinalityCountPredicateBucket(preds, active, res.ids, work)
			work.Release()
			res.ids.Release()
			return cnt, examined, true
		}
	}

	var cnt uint64
	var examined uint64
	var work posting.List

	leftEnd := min(br.BaseEnd, idx.BucketStart(startFull))
	if leftEnd > br.BaseStart {
		var partCnt, partExamined uint64
		partCnt, partExamined, work = countCardinalityNumericBucketRange(preds, active, ov, br.BaseStart, leftEnd, work)
		cnt += partCnt
		examined += partExamined
	}

	for bucket := startFull; bucket <= endFull; bucket++ {
		var bucketCnt, bucketExamined uint64
		bucketCnt, bucketExamined, work = countCardinalityNumericBucketRange(
			preds,
			active,
			ov,
			idx.BucketStart(bucket),
			idx.BucketEnd(bucket),
			work,
		)
		cnt += bucketCnt
		examined += bucketExamined
	}

	rightStart := max(br.BaseStart, idx.BucketEnd(endFull))
	if rightStart < br.BaseEnd {
		var partCnt, partExamined uint64
		partCnt, partExamined, work = countCardinalityNumericBucketRange(preds, active, ov, rightStart, br.BaseEnd, work)
		cnt += partCnt
		examined += partExamined
	}

	work.Release()
	return cnt, examined, true
}

func (qv *View) TryFilterCardinalityByPredicatesLeadBuckets(preds predicateSet, leadIdx int, active []int) (uint64, uint64, bool) {
	if leadIdx < 0 || leadIdx >= len(preds.owner) {
		return 0, 0, false
	}

	lead := &preds.owner[leadIdx]
	e := lead.expr
	field := qv.exec.FieldNameByOrdinal(e.FieldOrdinal)
	fm := qv.fieldMetaByExpr(e)

	span, ok, err := qv.prepareScalarIndexSpan(e)
	if err != nil || !ok {
		return 0, 0, false
	}

	ov := span.ov
	if ov.IsChunked() {
		rb, covered, hasBounds, ok := qv.collectPredicateRangeBoundsReader(field, preds.owner)
		if !ok || !hasBounds {
			if ok {
				pooled.ReleaseBoolSlice(covered)
			}
			return 0, 0, false
		}
		br := ov.RangeForBounds(rb)
		if br.Empty() {
			pooled.ReleaseBoolSlice(covered)
			return 0, 0, true
		}
		if br.BaseEnd-br.BaseStart < 4 {
			pooled.ReleaseBoolSlice(covered)
			return 0, 0, false
		}

		var activeBuf [cardinalityPredicateScanMaxLeaves]int
		activeChecks := activeBuf[:0]
		for _, pi := range active {
			if covered[pi] {
				continue
			}
			activeChecks = append(activeChecks, pi)
		}
		pooled.ReleaseBoolSlice(covered)

		if cnt, examined, ok := qv.TryFilterCardinalityByPredicatesNumericBucketPosting(preds, lead, ov, br, field, fm, activeChecks); ok {
			return cnt, examined, true
		}
		if cnt, examined, ok := qv.TryFilterCardinalityByPredicatesNumericBuckets(preds, ov, br, field, fm, activeChecks); ok {
			return cnt, examined, true
		}

		var (
			exactActiveBuf          [cardinalityPredicateScanMaxLeaves]int
			extraExactCandidatesBuf [cardinalityPredicateScanMaxLeaves]int
			residualExactBuf        [cardinalityPredicateScanMaxLeaves]int
			residualBothBuf         [cardinalityPredicateScanMaxLeaves]int
		)

		exactActive, extraExactCandidates, residualExact, residualBoth := qv.buildCardinalityLeadResidualScratchSet(
			preds,
			activeChecks,
			exactActiveBuf[:0],
			extraExactCandidatesBuf[:0],
			residualExactBuf[:0],
			residualBothBuf[:0],
		)

		var extraExactBuf []cardinalityLeadResidualExactFilter
		extraExactBuilt := len(extraExactCandidates) == 0

		var cnt uint64
		var examined uint64
		var bucketWork posting.List
		var extraWork posting.List

		cur := ov.NewCursor(br, false)
		for {
			_, ids, ok := cur.Next()
			if !ok {
				break
			}
			if ids.IsEmpty() {
				continue
			}
			examined += ids.Cardinality()

			if len(activeChecks) == 0 {
				cnt += ids.Cardinality()
				continue
			}
			if idx, ok := ids.TrySingle(); ok {
				if cardinalityPredicatesMatchSet(preds, activeChecks, idx) {
					cnt++
				}
				continue
			}
			if len(activeChecks) == 1 {
				if in, ok := preds.owner[activeChecks[0]].countBucket(ids); ok {
					cnt += in
					continue
				}
			}

			if len(exactActive) == 0 && len(extraExactBuf) == 0 {
				cnt += postingMatchesMultiSetCardinality(preds, activeChecks, ids)
				continue
			}

			current := ids
			exactApplied := false
			if len(exactActive) > 0 {
				mode, exactIDs, nextBucketWork, _ := plannerFilterPostingByPredicateChecks(preds.owner, exactActive, ids, bucketWork, true)
				bucketWork = nextBucketWork
				switch mode {
				case plannerPredicateBucketEmpty:
					continue
				case plannerPredicateBucketAll:
					current = exactIDs
					exactApplied = true
				case plannerPredicateBucketExact:
					if exactIDs.IsEmpty() {
						continue
					}
					current = exactIDs
					exactApplied = true
				}
			}

			extraApplied := false
			if extraExactBuilt || shouldBuildCardinalityLeadResidualExactFilters(current, len(extraExactCandidates)) {
				extraExactBuf, extraExactBuilt, residualBoth = qv.ensureCardinalityLeadResidualExactFiltersSet(
					preds,
					extraExactCandidates,
					residualExact,
					residualBoth,
					extraExactBuf,
					extraExactBuilt,
				)
				if shouldApplyCardinalityLeadResidualExactFilters(current, extraExactBuf) {
					filtered, nextExtraWork := applyCardinalityLeadResidualExactFilters(current, extraWork, extraExactBuf)
					extraWork = nextExtraWork
					if filtered.IsEmpty() {
						continue
					}
					current = filtered
					extraApplied = true
				}
			}

			checks := activeChecks
			if extraApplied {
				checks = residualBoth
			} else if exactApplied {
				checks = residualExact
			}

			if len(checks) == 0 {
				cnt += current.Cardinality()
				continue
			}

			if idx, ok := current.TrySingle(); ok {
				if cardinalityPredicatesMatchSet(preds, checks, idx) {
					cnt++
				}
				continue
			}
			cnt += postingMatchesMultiSetCardinality(preds, checks, current)
		}

		if extraExactBuf != nil {
			cardinalityLeadResidualExactFilterSlicePool.Put(extraExactBuf)
		}

		bucketWork.Release()
		extraWork.Release()

		return cnt, examined, true
	}

	if !span.hasData {
		return 0, 0, false
	}
	rb, covered, hasBounds, ok := qv.collectPredicateRangeBoundsReader(field, preds.owner)
	if !ok || !hasBounds {
		if ok {
			pooled.ReleaseBoolSlice(covered)
		}
		return 0, 0, false
	}

	br := ov.RangeForBounds(rb)
	start, end := br.BaseStart, br.BaseEnd

	if start >= end {
		pooled.ReleaseBoolSlice(covered)
		return 0, 0, true
	}
	if end-start < 4 {
		pooled.ReleaseBoolSlice(covered)
		return 0, 0, false
	}

	var activeBuf [cardinalityPredicateScanMaxLeaves]int
	activeChecks := activeBuf[:0]
	for _, pi := range active {
		if covered[pi] {
			continue
		}
		activeChecks = append(activeChecks, pi)
	}
	pooled.ReleaseBoolSlice(covered)

	if cnt, examined, ok := qv.TryFilterCardinalityByPredicatesNumericBucketPosting(preds, lead, ov, br, field, fm, activeChecks); ok {
		return cnt, examined, true
	}
	if cnt, examined, ok := qv.TryFilterCardinalityByPredicatesNumericBuckets(preds, ov, br, field, fm, activeChecks); ok {
		return cnt, examined, true
	}

	var (
		exactActiveBuf          [cardinalityPredicateScanMaxLeaves]int
		extraExactCandidatesBuf [cardinalityPredicateScanMaxLeaves]int
		residualExactBuf        [cardinalityPredicateScanMaxLeaves]int
		residualBothBuf         [cardinalityPredicateScanMaxLeaves]int
	)
	exactActive, extraExactCandidates, residualExact, residualBoth := qv.buildCardinalityLeadResidualScratchSet(
		preds,
		activeChecks,
		exactActiveBuf[:0],
		extraExactCandidatesBuf[:0],
		residualExactBuf[:0],
		residualBothBuf[:0],
	)

	var extraExactBuf []cardinalityLeadResidualExactFilter
	extraExactBuilt := len(extraExactCandidates) == 0

	var cnt uint64
	var examined uint64
	var bucketWork posting.List
	var extraWork posting.List

	cur := ov.NewCursor(ov.RangeByRanks(start, end), false)
	for {
		_, ids, ok := cur.Next()
		if !ok {
			break
		}
		if ids.IsEmpty() {
			continue
		}
		examined += ids.Cardinality()

		if len(activeChecks) == 0 {
			cnt += ids.Cardinality()
			continue
		}
		if idx, ok := ids.TrySingle(); ok {
			if cardinalityPredicatesMatchSet(preds, activeChecks, idx) {
				cnt++
			}
			continue
		}
		if len(activeChecks) == 1 {
			if in, ok := preds.owner[activeChecks[0]].countBucket(ids); ok {
				cnt += in
				continue
			}
		}

		if len(exactActive) == 0 && len(extraExactBuf) == 0 {
			cnt += postingMatchesMultiSetCardinality(preds, activeChecks, ids)
			continue
		}

		current := ids
		exactApplied := false
		if len(exactActive) > 0 {
			mode, exactIDs, nextBucketWork, _ := plannerFilterPostingByPredicateChecks(preds.owner, exactActive, ids, bucketWork, true)
			bucketWork = nextBucketWork
			switch mode {
			case plannerPredicateBucketEmpty:
				continue
			case plannerPredicateBucketAll:
				current = exactIDs
				exactApplied = true
			case plannerPredicateBucketExact:
				if exactIDs.IsEmpty() {
					continue
				}
				current = exactIDs
				exactApplied = true
			}
		}

		extraApplied := false
		if extraExactBuilt || shouldBuildCardinalityLeadResidualExactFilters(current, len(extraExactCandidates)) {
			extraExactBuf, extraExactBuilt, residualBoth = qv.ensureCardinalityLeadResidualExactFiltersSet(
				preds,
				extraExactCandidates,
				residualExact,
				residualBoth,
				extraExactBuf,
				extraExactBuilt,
			)
			if shouldApplyCardinalityLeadResidualExactFilters(current, extraExactBuf) {
				filtered, nextExtraWork := applyCardinalityLeadResidualExactFilters(current, extraWork, extraExactBuf)
				extraWork = nextExtraWork
				if filtered.IsEmpty() {
					continue
				}
				current = filtered
				extraApplied = true
			}
		}

		checks := activeChecks
		if extraApplied {
			checks = residualBoth
		} else if exactApplied {
			checks = residualExact
		}
		if len(checks) == 0 {
			cnt += current.Cardinality()
			continue
		}
		if idx, ok := current.TrySingle(); ok {
			if cardinalityPredicatesMatchSet(preds, checks, idx) {
				cnt++
			}
			continue
		}
		cnt += postingMatchesMultiSetCardinality(preds, checks, current)
	}

	if extraExactBuf != nil {
		cardinalityLeadResidualExactFilterSlicePool.Put(extraExactBuf)
	}
	bucketWork.Release()
	extraWork.Release()
	return cnt, examined, true
}

func (qv *View) TryFilterCardinalityByPredicatesLeadPostings(preds predicateSet, leadIdx int, active []int) (uint64, uint64, bool) {
	if leadIdx < 0 || leadIdx >= len(preds.owner) {
		return 0, 0, false
	}
	lead := preds.owner[leadIdx]
	if lead.expr.Not {
		return 0, 0, false
	}
	leadPostCount := cardinalityPredicateLeadPostingCount(lead)
	if leadPostCount == 0 {
		return 0, 0, false
	}

	var activeBuf [cardinalityPredicateScanMaxLeaves]int
	activeChecks := append(activeBuf[:0], active...)

	var (
		exactActiveBuf          [cardinalityPredicateScanMaxLeaves]int
		extraExactCandidatesBuf [cardinalityPredicateScanMaxLeaves]int
		residualExactBuf        [cardinalityPredicateScanMaxLeaves]int
		residualBothBuf         [cardinalityPredicateScanMaxLeaves]int
	)
	exactActive, extraExactCandidates, residualExact, residualBoth := qv.buildCardinalityLeadResidualScratchSet(
		preds,
		activeChecks,
		exactActiveBuf[:0],
		extraExactCandidatesBuf[:0],
		residualExactBuf[:0],
		residualBothBuf[:0],
	)

	var extraExactBuf []cardinalityLeadResidualExactFilter
	extraExactBuilt := len(extraExactCandidates) == 0

	var (
		cnt        uint64
		examined   uint64
		bucketWork posting.List
		extraWork  posting.List
	)
	for i := 0; i < leadPostCount; i++ {
		ids := cardinalityPredicateLeadPostingAt(lead, i)
		if ids.IsEmpty() {
			continue
		}
		examined += ids.Cardinality()

		if len(activeChecks) == 0 {
			cnt += ids.Cardinality()
			continue
		}
		if idx, ok := ids.TrySingle(); ok {
			if cardinalityPredicatesMatchSet(preds, activeChecks, idx) {
				cnt++
			}
			continue
		}
		if len(activeChecks) == 1 {
			if in, ok := preds.owner[activeChecks[0]].countBucket(ids); ok {
				cnt += in
				continue
			}
		}

		if len(exactActive) == 0 && len(extraExactBuf) == 0 {
			cnt += postingMatchesMultiSetCardinality(preds, activeChecks, ids)
			continue
		}

		current := ids
		exactApplied := false
		if len(exactActive) > 0 {
			mode, exactIDs, nextBucketWork, _ := plannerFilterPostingByPredicateChecks(preds.owner, exactActive, ids, bucketWork, true)
			bucketWork = nextBucketWork
			switch mode {
			case plannerPredicateBucketEmpty:
				continue
			case plannerPredicateBucketAll:
				current = exactIDs
				exactApplied = true
			case plannerPredicateBucketExact:
				if exactIDs.IsEmpty() {
					continue
				}
				current = exactIDs
				exactApplied = true
			}
		}

		extraApplied := false
		if extraExactBuilt || shouldBuildCardinalityLeadResidualExactFilters(current, len(extraExactCandidates)) {
			extraExactBuf, extraExactBuilt, residualBoth = qv.ensureCardinalityLeadResidualExactFiltersSet(
				preds,
				extraExactCandidates,
				residualExact,
				residualBoth,
				extraExactBuf,
				extraExactBuilt,
			)
			if shouldApplyCardinalityLeadResidualExactFilters(current, extraExactBuf) {
				filtered, nextExtraWork := applyCardinalityLeadResidualExactFilters(current, extraWork, extraExactBuf)
				extraWork = nextExtraWork
				if filtered.IsEmpty() {
					continue
				}
				current = filtered
				extraApplied = true
			}
		}

		checks := activeChecks
		if extraApplied {
			checks = residualBoth
		} else if exactApplied {
			checks = residualExact
		}
		if len(checks) == 0 {
			cnt += current.Cardinality()
			continue
		}

		if idx, ok := current.TrySingle(); ok {
			if cardinalityPredicatesMatchSet(preds, checks, idx) {
				cnt++
			}
			continue
		}

		cnt += postingMatchesMultiSetCardinality(preds, checks, current)
	}

	if extraExactBuf != nil {
		cardinalityLeadResidualExactFilterSlicePool.Put(extraExactBuf)
	}

	bucketWork.Release()
	extraWork.Release()

	return cnt, examined, true
}

func shouldTryCardinalityORByPredicates(expr qir.Expr) bool {
	if expr.Not || expr.Op != qir.OpOR {
		return false
	}
	n := len(expr.Operands)
	if n < 2 {
		return false
	}
	if !cardinalityORHasPrefixLikeBranch(expr) {
		return false
	}
	return true
}

func (qv *View) shouldPreferMaterializedCardinalityOR(expr qir.Expr) bool {
	n := len(expr.Operands)
	if n < 2 || n > 3 {
		return false
	}
	for _, op := range expr.Operands {
		if cardinalityORBranchHasExpensiveMaterialization(op) {
			return false
		}
	}
	return qv.snap.ShouldPromoteRuntimeMaterializedPredKey(cardinalityORMaterializedRouteKeys[n])
}

func cardinalityORBranchHasExpensiveMaterialization(expr qir.Expr) bool {
	switch expr.Op {

	case qir.OpAND, qir.OpOR:
		if expr.Not {
			return true
		}
		if len(expr.Operands) == 0 {
			return true
		}
		for _, op := range expr.Operands {
			if cardinalityORBranchHasExpensiveMaterialization(op) {
				return true
			}
		}
		return false

	case qir.OpSUFFIX, qir.OpCONTAINS:
		return true

	default:
		return len(expr.Operands) != 0
	}
}

func cardinalityORPredicateBranchLimit(universe uint64) int {
	limit := cardinalityORPredicateMaxBranchesBase
	switch {
	case universe >= 4_000_000:
		limit = 6
	case universe > 0 && universe <= 128_000:
		limit = 4
	}
	/*
		if limit < 3 {
			limit = 3
		}
		if limit > 8 {
			limit = 8
		}
	*/
	return min(max(limit, 3), 8)
}

func cardinalityORHasPrefixLikeBranch(expr qir.Expr) bool {
	for _, op := range expr.Operands {
		found, ok := cardinalityHasPositivePrefixLikeAndLeaf(op)
		if !ok {
			return false
		}
		if found {
			return true
		}
	}
	return false
}

func cardinalityHasPositivePrefixLikeAndLeaf(e qir.Expr) (bool, bool) {
	switch e.Op {
	case qir.OpNOOP:
		return false, false

	case qir.OpAND:
		if e.Not || len(e.Operands) == 0 {
			return false, false
		}
		for _, ch := range e.Operands {
			found, ok := cardinalityHasPositivePrefixLikeAndLeaf(ch)
			if !ok {
				return false, false
			}
			if found {
				return true, true
			}
		}
		return false, true

	default:
		if e.Op == qir.OpOR || len(e.Operands) != 0 {
			return false, false
		}
		if e.Not {
			return false, true
		}
		switch e.Op {
		case qir.OpPREFIX, qir.OpSUFFIX, qir.OpCONTAINS:
			return true, true
		default:
			return false, true
		}
	}
}

func (qv *View) cardinalityORBranchesDisjointByScalarEQ(expr qir.Expr) bool {
	branchCount := len(expr.Operands)
	if branchCount < 2 || branchCount > cardinalityORScalarDisjointMaxBranches {
		return false
	}

	var leavesBuf [cardinalityPredicateScanMaxLeaves]qir.Expr
	leaves, ok := qir.CollectAndLeavesFixed(expr.Operands[0], leavesBuf[:0])
	if !ok {
		return false
	}

	fieldOrdinal := -1
	for i := range leaves {
		e := leaves[i]
		if e.Not || e.Op != qir.OpEQ || e.FieldOrdinal < 0 || len(e.Operands) != 0 {
			continue
		}
		fm := qv.fieldMetaByExpr(e)
		if fm == nil || fm.Slice {
			continue
		}
		fieldOrdinal = e.FieldOrdinal
		break
	}
	if fieldOrdinal < 0 {
		return false
	}

	var keys [cardinalityORScalarDisjointMaxBranches]string
	var nils [cardinalityORScalarDisjointMaxBranches]bool
	for branchIdx, op := range expr.Operands {
		leaves, ok = qir.CollectAndLeavesFixed(op, leavesBuf[:0])
		if !ok {
			return false
		}

		found := false
		var key string
		var isNil bool
		for i := range leaves {
			e := leaves[i]
			if e.Not || e.Op != qir.OpEQ || e.FieldOrdinal != fieldOrdinal || len(e.Operands) != 0 {
				continue
			}
			nextKey, isSlice, nextNil, err := qv.exprValueToIdxScalar(e)
			if err != nil || isSlice {
				return false
			}
			key = nextKey
			isNil = nextNil
			found = true
			break
		}
		if !found {
			return false
		}
		for i := 0; i < branchIdx; i++ {
			if nils[i] == isNil && keys[i] == key {
				return false
			}
		}
		keys[branchIdx] = key
		nils[branchIdx] = isNil
	}

	return true
}

func cardinalityLeadOpWeight(op qir.Op) uint64 {
	switch op {
	case qir.OpEQ:
		return 1
	case qir.OpIN:
		return 3
	case qir.OpHASALL:
		return 4
	case qir.OpHASANY:
		return 12
	default:
		if op.IsScalarRangeOrPrefix() {
			return 2
		}
		return 8
	}
}

func cardinalityPredicateLeadWeight(p predicate) uint64 {
	if p.isMaterializedLike() {
		return 1
	}
	switch p.expr.Op {
	case qir.OpEQ:
		return 1
	case qir.OpIN:
		return 2
	case qir.OpHASALL:
		return 5
	case qir.OpHASANY:
		return 12
	default:
		if p.expr.Op.IsScalarRangeOrPrefix() {
			return 4
		}
		return cardinalityLeadOpWeight(p.expr.Op)
	}
}

func cardinalityPredicateLeadScanWeight(p predicate) uint64 {
	weight := cardinalityPredicateLeadWeight(p)
	if !p.isCustomUnmaterialized() {
		return weight
	}
	if p.expr.Op.IsScalarRangeOrPrefix() {
		return satMulUint64(weight, 3)
	}
	return weight
}

func cardinalityLeadTooRisky(op qir.Op, est, universe uint64) bool {
	if universe == 0 || est == 0 {
		return false
	}
	switch op {
	case qir.OpHASANY:
		return est > universe/16 && est > 16_384
	case qir.OpHASALL:
		return est > universe/8 && est > 32_768
	default:
		return false
	}
}

func (qv *View) prepareCardinalityORPredicateWithTrace(p *predicate, probeEst uint64, universe uint64, trace *Trace) error {
	if p == nil || p.alwaysTrue || p.alwaysFalse || p.covered {
		return nil
	}
	if trace != nil {
		trace.AddCountPredicatePreparation(1)
	}
	if probeEst > 0 {
		p.setExpectedContainsCalls(clampUint64ToInt(probeEst))
	}

	route := qv.cardinalityScalarComplementRouting(*p, probeEst, universe)
	if route.prefersLazyPostingFilter(*p, qv.snap) {
		return nil
	}

	if route.wantsComplementMaterialization() &&
		qv.tryMaterializeBroadRangeComplementPredicateForCardinality(p, probeEst, universe, trace) {
		return nil
	}

	return nil
}

func (qv *View) cardinalityPredicateResidualCheckWeight(p predicate, probeEst, universe uint64) uint64 {
	if p.alwaysTrue || p.covered {
		return 0
	}
	if p.alwaysFalse {
		return ^uint64(0) / 4
	}
	if p.isMaterializedLike() {
		return 1
	}
	if shouldUseCardinalityLeadResidualHasAnyExactFilter(p) {
		return 1
	}
	if shouldMaterializeCardinalitySetPredicate(p, probeEst, universe) {
		return 1
	}
	if qv.cardinalityScalarComplementRouting(p, probeEst, universe).wantsComplementMaterialization() {
		return 1
	}
	if shouldMaterializeCustomCardinalityPredicate(p, probeEst, universe) {
		return 1
	}

	weight := cardinalityPredicateResidualOpWeight(p)
	if p.expr.Not {
		weight++
	}
	return weight
}

func (qv *View) cardinalityORPredicateResidualCheckWeight(p predicate, probeEst, universe uint64) uint64 {
	if p.alwaysTrue || p.covered {
		return 0
	}
	if p.alwaysFalse {
		return ^uint64(0) / 4
	}
	if p.isMaterializedLike() {
		return 1
	}
	if p.postingFilterCapability().isCheap() {
		return 1
	}
	if shouldUseCardinalityLeadResidualHasAnyExactFilter(p) {
		return 1
	}
	if qv.cardinalityScalarComplementRouting(p, probeEst, universe).wantsComplementMaterialization() {
		return 1
	}

	weight := cardinalityPredicateResidualOpWeight(p)
	if p.expr.Not {
		weight++
	}
	return weight
}

func (qv *View) cardinalityLeadResidualScore(p predicate, leadEst, universe uint64) uint64 {
	if leadEst == 0 || p.alwaysTrue || p.covered {
		return 0
	}
	weight := qv.cardinalityPredicateResidualCheckWeight(p, leadEst, universe)
	if weight == 0 {
		return 0
	}
	effectiveRows := leadEst
	if p.estCard > 0 && p.estCard < effectiveRows {
		effectiveRows = p.estCard
	}
	return satMulUint64(effectiveRows, weight)
}

func (qv *View) cardinalityORLeadResidualScore(p predicate, leadEst, universe uint64) uint64 {
	if leadEst == 0 || p.alwaysTrue || p.covered {
		return 0
	}
	weight := qv.cardinalityORPredicateResidualCheckWeight(p, leadEst, universe)
	if weight == 0 {
		return 0
	}
	return satMulUint64(leadEst, weight)
}

func (qv *View) pickCardinalityLeadPredicate(preds predicateReader, universe uint64) (int, uint64, uint64) {
	leadIdx := -1
	leadEst := uint64(0)
	leadScore := uint64(0)

	for i := 0; i < len(preds); i++ {
		p := preds[i]
		if p.alwaysTrue || p.covered || !p.hasIter() || p.estCard == 0 {
			continue
		}
		if cardinalityLeadTooRisky(p.expr.Op, p.estCard, universe) {
			continue
		}

		score := satMulUint64(p.estCard, cardinalityPredicateLeadScanWeight(p))
		if p.leadIterNeedsContainsCheck() {
			score = satAddUint64(score, qv.cardinalityLeadResidualScore(p, p.estCard, universe))
		}
		for j := 0; j < len(preds); j++ {
			if j == i {
				continue
			}
			score = satAddUint64(score, qv.cardinalityLeadResidualScore(preds[j], p.estCard, universe))
		}

		if leadIdx == -1 || score < leadScore {
			leadIdx = i
			leadEst = p.estCard
			leadScore = score
		}
	}
	return leadIdx, leadEst, leadScore
}

func (qv *View) pickCardinalityORLeadPredicate(preds predicateSet, universe uint64) (int, uint64, uint64) {
	leadIdx := -1
	leadEst := uint64(0)
	leadScore := uint64(0)

	for i := 0; i < len(preds.owner); i++ {
		p := preds.owner[i]
		if p.alwaysTrue || p.covered || !p.hasIter() || p.estCard == 0 {
			continue
		}
		if cardinalityLeadTooRisky(p.expr.Op, p.estCard, universe) {
			continue
		}

		score := satMulUint64(p.estCard, cardinalityPredicateLeadScanWeight(p))
		if p.leadIterNeedsContainsCheck() {
			score = satAddUint64(score, qv.cardinalityORLeadResidualScore(p, p.estCard, universe))
		}
		for j := 0; j < len(preds.owner); j++ {
			if j == i {
				continue
			}
			score = satAddUint64(score, qv.cardinalityORLeadResidualScore(preds.owner[j], p.estCard, universe))
		}

		if leadIdx == -1 || score < leadScore {
			leadIdx = i
			leadEst = p.estCard
			leadScore = score
		}
	}
	return leadIdx, leadEst, leadScore
}

// TryFilterCardinalityORByPredicates counts top-level OR expressions via branch lead scans.
//
// The path is intentionally conservative: it is enabled only for bounded OR
// shapes where lead-driven probing is usually cheaper than full postingResult union.
func (qv *View) TryFilterCardinalityORByPredicates(expr qir.Expr, trace *Trace) (uint64, bool, error) {
	if !shouldTryCardinalityORByPredicates(expr) {
		return 0, false, nil
	}

	uc := qv.snap.Universe.Cardinality()
	if uc == 0 {
		return 0, true, nil
	}
	branchCount := len(expr.Operands)
	if branchCount > cardinalityORPredicateBranchLimit(uc) {
		out, err := qv.FilterCardinalityByMaterializedExpr(expr, trace)
		return out, true, err
	}
	if qv.cardinalityORBranchesDisjointByScalarEQ(expr) {
		var cnt uint64
		for i := range expr.Operands {
			branchCnt, err := qv.exactExprCardinality(expr.Operands[i])
			if err != nil {
				return 0, true, err
			}
			cnt = satAddUint64(cnt, branchCnt)
		}
		if trace != nil {
			trace.SetPlan(planFilterCardinalityORPredicates)
			trace.AddExamined(cnt)
		}
		return cnt, true, nil
	}
	if qv.shouldPreferMaterializedCardinalityOR(expr) {
		out, err := qv.FilterCardinalityByMaterializedExpr(expr, trace)
		return out, true, err
	}
	strictWide := branchCount > 3
	fullTrace := trace.Full()

	var branchTrace []TraceORBranch
	if fullTrace {
		var branchTraceInline [cardinalityORPredicateMaxBranchesBase + 1]TraceORBranch
		branchTrace = branchTraceInline[:branchCount]
		for i := range branchTrace {
			branchTrace[i].Index = i
		}
	}

	branchesBuf := cardinalityORBranchSlicePool.Get(len(expr.Operands))

	var (
		expectedProbes        uint64
		materializedBranchEst uint64
	)

	var materializedBranchesInline [cardinalityORPredicateMaxBranchesBase + 1]cardinalityORMaterializedBranch
	materializedBranches := materializedBranchesInline[:0]

	// Build branch plans first and estimate total probe budget before scanning.
	// This prevents expensive partial work when the union is too broad.
	var leafBuf [cardinalityPredicateScanMaxLeaves]qir.Expr
LOOP:
	for branchIdx, op := range expr.Operands {
		leaves, ok := qir.CollectAndLeavesFixed(op, leafBuf[:0])
		if !ok {
			cardinalityORBranchSlicePool.Put(branchesBuf)
			return 0, false, nil
		}

		// Count paths defer broad numeric-range materialization until a lead is
		// chosen; otherwise NoCaching runs can rebuild huge range bitmaps per query.
		predSet, ok := qv.buildCardinalityPredicatesWithMode(leaves, false)
		if !ok {
			cardinalityORBranchSlicePool.Put(branchesBuf)
			return 0, false, nil
		}

		var activeBuf [cardinalityPredicateScanMaxLeaves]int
		active := activeBuf[:0]
		branchFalse := false

		for i := 0; i < predSet.Len(); i++ {
			p := predSet.owner[i]
			if p.alwaysFalse {
				branchFalse = true
				break
			}
			if p.covered || p.alwaysTrue {
				continue
			}
			active = append(active, i)
		}

		if branchFalse {
			predSet.Release()
			continue
		}

		// Branch is tautology: OR result equals universe.
		if len(active) == 0 {
			predSet.Release()
			cardinalityORBranchSlicePool.Put(branchesBuf)
			return uc, true, nil
		}

		leadIdx, leadEst, leadScore := qv.pickCardinalityORLeadPredicate(predSet, uc)
		branchEst := cardinalityORBranchEstimateSet(predSet, active, leadEst)
		leadWeight := uint64(0)
		if leadIdx >= 0 {
			leadWeight = cardinalityPredicateLeadScanWeight(predSet.owner[leadIdx])
		}

		// Cannot drive branch without iterable lead.
		if leadIdx < 0 {
			if strictWide {
				materializedBranches, materializedBranchEst = appendCardinalityORMaterializedBranch(
					branchTrace,
					materializedBranches,
					materializedBranchEst,
					branchIdx,
					"materialized_spill_no_lead",
					op,
					branchEst,
				)
				predSet.Release()
				continue
			}
			predSet.Release()
			cardinalityORBranchSlicePool.Put(branchesBuf)
			return 0, false, nil
		}

		// Keep OR path setup minimal: avoid eager predicate materialization here.
		// Broad OR branches are sensitive to one-shot setup allocations.
		if cardinalityLeadTooRisky(predSet.owner[leadIdx].expr.Op, leadEst, uc) {
			if strictWide {
				materializedBranches, materializedBranchEst = appendCardinalityORMaterializedBranch(
					branchTrace,
					materializedBranches,
					materializedBranchEst,
					branchIdx,
					"materialized_spill_risky_lead",
					op,
					branchEst,
				)
				predSet.Release()
				continue
			}
			predSet.Release()
			cardinalityORBranchSlicePool.Put(branchesBuf)
			return 0, false, nil
		}

		if strictWide {
			if leadWeight > 3 && leadEst > 2_048 && leadEst > uc/64 {
				materializedBranches, materializedBranchEst = appendCardinalityORMaterializedBranch(
					branchTrace,
					materializedBranches,
					materializedBranchEst,
					branchIdx,
					"materialized_spill_expensive_lead",
					op,
					branchEst,
				)
				predSet.Release()
				continue LOOP
			}
			if leadScore > uc && leadEst > 2_048 && leadEst > uc/32 {
				materializedBranches, materializedBranchEst = appendCardinalityORMaterializedBranch(
					branchTrace,
					materializedBranches,
					materializedBranchEst,
					branchIdx,
					"materialized_spill_expensive_branch_score",
					op,
					branchEst,
				)
				predSet.Release()
				continue LOOP
			}
			if leadEst > uc/3 {
				materializedBranches, materializedBranchEst = appendCardinalityORMaterializedBranch(
					branchTrace,
					materializedBranches,
					materializedBranchEst,
					branchIdx,
					"materialized_spill_broad_lead",
					op,
					branchEst,
				)
				predSet.Release()
				continue LOOP
			}
		}

		var checksBuf [cardinalityPredicateScanMaxLeaves]int
		checks := checksBuf[:0]

		leadNeedsCheck := (&predSet.owner[leadIdx]).leadIterNeedsContainsCheck()

		for _, pi := range active {
			if pi == leadIdx && !leadNeedsCheck {
				continue
			}
			p := predSet.owner[pi]
			if p.covered || p.alwaysTrue {
				continue
			}
			if p.alwaysFalse {
				branchFalse = true
				break
			}
			if !p.hasContains() {
				if strictWide {
					materializedBranches, materializedBranchEst = appendCardinalityORMaterializedBranch(
						branchTrace,
						materializedBranches,
						materializedBranchEst,
						branchIdx,
						"materialized_spill_no_contains",
						op,
						branchEst,
					)
					predSet.Release()
					continue LOOP
				}
				predSet.Release()
				cardinalityORBranchSlicePool.Put(branchesBuf)
				return 0, false, nil
			}
			checks = append(checks, pi)
		}

		if branchFalse {
			predSet.Release()
			continue
		}

		for _, pi := range checks {
			if err := qv.prepareCardinalityORPredicateWithTrace((&predSet.owner[pi]), leadEst, uc, trace); err != nil {
				predSet.Release()
				cardinalityORBranchSlicePool.Put(branchesBuf)
				return 0, true, err
			}
		}

		write := 0
		for read := 0; read < len(checks); read++ {
			pi := checks[read]
			p := predSet.owner[pi]
			if p.covered || p.alwaysTrue {
				continue
			}
			if p.alwaysFalse {
				branchFalse = true
				break
			}
			if !p.hasContains() {
				if strictWide {
					materializedBranches, materializedBranchEst = appendCardinalityORMaterializedBranch(
						branchTrace,
						materializedBranches,
						materializedBranchEst,
						branchIdx,
						"materialized_spill_post_prepare",
						op,
						branchEst,
					)
					predSet.Release()
					continue LOOP
				}
				predSet.Release()
				cardinalityORBranchSlicePool.Put(branchesBuf)
				return 0, false, nil
			}
			checks[write] = pi
			write++
		}

		if branchFalse {
			predSet.Release()
			continue
		}

		checks = checks[:write]
		sortActivePredicatesReader(checks, predSet.owner)

		matchWeight := uint64(0)

		if !cardinalityIndexSliceContains(checks, leadIdx) {
			matchWeight = qv.cardinalityORPredicateResidualCheckWeight(predSet.owner[leadIdx], leadEst, uc)
		}
		for _, pi := range checks {
			matchWeight = satAddUint64(matchWeight, qv.cardinalityORPredicateResidualCheckWeight(predSet.owner[pi], leadEst, uc))
		}
		if matchWeight == 0 {
			matchWeight = 1
		}

		if leadEst == 0 || leadWeight == 0 {
			leadEst = 1
			leadWeight = 1
		}
		expectedProbes += leadEst * leadWeight
		br := newCardinalityORBranch(branchIdx, predSet, leadIdx, checks, leadEst)
		br.matchWeight = matchWeight
		branchesBuf = append(branchesBuf, br)
	}
	defer cardinalityORBranchSlicePool.Put(branchesBuf)

	if len(branchesBuf) == 0 {
		if len(materializedBranches) > 0 {
			return 0, false, nil
		}
		return 0, true, nil
	}

	useMaterializedSpill := len(materializedBranches) > 0
	if useMaterializedSpill && !shouldTryCardinalityORHybridMaterializedSpill(branchCount, len(branchesBuf), len(materializedBranches), expectedProbes, materializedBranchEst, uc) {
		out, err := qv.FilterCardinalityByMaterializedExpr(expr, trace)
		return out, true, err
	}

	// Guardrail: skip this path when projected probe volume approaches a broad
	// union, where postingResult materialization is typically cheaper overall.
	limit := uc * 2
	if strictWide {
		limit = uc
	}
	if !useMaterializedSpill && expectedProbes > limit {
		out, err := qv.FilterCardinalityByMaterializedExpr(expr, trace)
		return out, true, err
	}

	useSeenDedup := useMaterializedSpill || cardinalityORBranchesShouldUseSeenDedupBuf(branchesBuf, uc, expectedProbes)
	var seen cardinalityORSeen
	if useSeenDedup {
		seen = newCardinalityORSeenBuf(branchesBuf, materializedBranchEst, uc)
		defer seen.release()
	} else {
		cardinalityORPreparePrevOrderBuf(branchesBuf)
	}

	var cnt uint64
	var examined uint64
	if trace != nil {
		if useMaterializedSpill {
			trace.SetPlan(planFilterCardinalityORHybrid)
		} else {
			trace.SetPlan(planFilterCardinalityORPredicates)
		}
	}

	if useMaterializedSpill {
		var (
			spillCnt      uint64
			spillExamined uint64
			ok            bool
			err           error
		)
		spillCnt, spillExamined, ok, err = qv.cardinalityORMaterializedSpillUnion(materializedBranches, &seen, trace, branchTrace)
		if err != nil {
			return 0, true, err
		}
		if !ok {
			return 0, false, nil
		}
		cnt += spillCnt
		examined += spillExamined
	}

	for i := 0; i < len(branchesBuf); i++ {
		br := branchesBuf[i]
		var brTrace *TraceORBranch
		if branchTrace != nil {
			brTrace = &branchTrace[br.index]
		}
		var (
			cntBranch      uint64
			examinedBranch uint64
			ok             bool
		)
		if cntBranch, examinedBranch, ok = qv.tryFilterCardinalityORBranchLeadPostingsBuf(branchesBuf, i, useSeenDedup, &seen, brTrace); ok {
			cnt += cntBranch
			examined += examinedBranch
			continue
		}

		lead := br.preds.owner[br.lead]
		it := lead.newIter()
		if it == nil {
			return 0, false, nil
		}

		for it.HasNext() {
			idx := it.Next()
			examined++
			if brTrace != nil {
				brTrace.RowsExamined++
			}

			if useSeenDedup {
				// OR union dedupe first to avoid repeated expensive predicate checks
				// for ids that were already accepted by previous branches.
				if seen.Has(idx) {
					continue
				}
			}

			if !br.checksMatch(idx) {
				continue
			}

			if useSeenDedup {
				if seen.Add(idx) {
					cnt++
					if brTrace != nil {
						brTrace.RowsEmitted++
					}
				}
				continue
			}

			dup := false
			for j := 0; j < i; j++ {
				if (&branchesBuf[j]).matches(idx) {
					dup = true
					break
				}
			}
			if !dup {
				cnt++
				if brTrace != nil {
					brTrace.RowsEmitted++
				}
			}
		}
		it.Release()
	}

	if trace != nil {
		trace.AddExamined(examined)
		trace.SetORBranches(branchTrace)
	}

	return cnt, true, nil
}

func (qv *View) exactExprCardinality(expr qir.Expr) (uint64, error) {
	if expr.Op == qir.OpNOOP {
		if expr.Not {
			return 0, nil
		}
		return qv.snap.Universe.Cardinality(), nil
	}
	if expr.FieldOrdinal >= 0 && len(expr.Operands) == 0 {
		if out, ok, err := qv.TryFilterCardinalityByScalarLookup(expr, nil); ok || err != nil {
			return out, err
		}
		if out, ok, err := qv.TryFilterCardinalityBySliceLookup(expr, nil); ok || err != nil {
			return out, err
		}
	}
	if out, ok, err := qv.TryFilterCardinalityByUniqueEq(expr, nil); ok || err != nil {
		return out, err
	}
	if out, ok, err := qv.TryFilterCardinalityByScalarInSplit(expr, nil); ok || err != nil {
		return out, err
	}
	if out, ok, err := qv.TryFilterCardinalityPreparedAndReordered(expr); ok || err != nil {
		return out, err
	}
	if out, ok, err := qv.TryFilterCardinalityByPredicates(expr, nil); ok || err != nil {
		return out, err
	}
	if out, ok, err := qv.TryFilterCardinalityORByPredicates(expr, nil); ok || err != nil {
		return out, err
	}

	b, err := qv.evalExpr(expr)
	if err != nil {
		return 0, err
	}
	defer b.ids.Release()

	return qv.postingResultCardinality(b), nil
}

type cardinalityLeafPlan struct {
	expr        qir.Expr
	selectivity float64
	hasSel      bool
}

func cardinalityLeafOpCost(e qir.Expr) int {
	cost := 5
	switch e.Op {
	case qir.OpEQ, qir.OpNOOP:
		cost = 0
	case qir.OpIN, qir.OpHASALL, qir.OpHASANY:
		cost = 1
	case qir.OpPREFIX:
		cost = 3
	case qir.OpSUFFIX, qir.OpCONTAINS:
		cost = 4
	default:
		if e.Op.IsNumericRange() {
			cost = 2
		}
	}
	if e.Not {
		cost++
	}
	return cost
}

func cardinalityPredicateResidualOpWeight(p predicate) uint64 {
	switch p.expr.Op {
	case qir.OpEQ, qir.OpNOOP:
		return 1
	case qir.OpIN, qir.OpHASALL:
		return 2
	case qir.OpHASANY:
		return 3
	case qir.OpPREFIX:
		return 6
	case qir.OpSUFFIX:
		return 8
	case qir.OpCONTAINS:
		return 10
	default:
		if p.expr.Op.IsNumericRange() {
			return 4
		}
		return max(2, p.checkCost()+2)
	}
}

func sortCardinalityLeafPlanOrder(plans []cardinalityLeafPlan, ids []int) {
	if len(ids) <= 1 {
		return
	}
	for i := 1; i < len(ids); i++ {
		cur := ids[i]
		j := i
		for j > 0 && cardinalityLeafPlanLess(plans, cur, ids[j-1]) {
			ids[j] = ids[j-1]
			j--
		}
		ids[j] = cur
	}
}

func cardinalityLeafPlanLess(plans []cardinalityLeafPlan, a, b int) bool {
	pa := plans[a]
	pb := plans[b]
	if pa.hasSel != pb.hasSel {
		return pa.hasSel
	}
	if pa.hasSel && pb.hasSel && pa.selectivity != pb.selectivity {
		return pa.selectivity < pb.selectivity
	}
	ca := cardinalityLeafOpCost(pa.expr)
	cb := cardinalityLeafOpCost(pb.expr)
	if ca != cb {
		return ca < cb
	}
	return a < b
}

func cardinalityLeafPlanStartsBroadScalarMaterialization(plan cardinalityLeafPlan) bool {
	if plan.expr.Not || !plan.hasSel || !plan.expr.Op.IsScalarRangeOrPrefix() {
		return false
	}
	return plan.selectivity > 0.5
}

func cardinalityLeafPlanSeedsCustomMaterialization(plan cardinalityLeafPlan) bool {
	if plan.expr.Not {
		return false
	}
	return plan.expr.Op == qir.OpSUFFIX || plan.expr.Op == qir.OpCONTAINS
}

func (qv *View) reorderCardinalityEvalAndPositivePlans(plans []cardinalityLeafPlan, pos []int) {
	if len(pos) <= 1 {
		return
	}
	if !cardinalityLeafPlanStartsBroadScalarMaterialization(plans[pos[0]]) {
		return
	}
	pick := -1
	for i := 1; i < len(pos); i++ {
		if cardinalityLeafPlanSeedsCustomMaterialization(plans[pos[i]]) {
			cacheKey := qv.materializedPredKey(plans[pos[i]].expr)
			if !cacheKey.IsZero() {
				if cached, ok := qv.snap.LoadMaterializedPredKey(cacheKey); ok && !cached.IsEmpty() {
					continue
				}
			}
			pick = i
			break
		}
	}
	if pick < 0 {
		return
	}
	cur := pos[pick]
	for i := pick; i > 0; i-- {
		pos[i] = pos[i-1]
	}
	pos[0] = cur
}

// Complement range apply also mutates the accumulated posting, so it needs a
// clear build-work win over materializing the positive range and intersecting it.
const evalAndComplementRangeApplyMinWorkGain = 2

func (qv *View) applyAndPostingResultExpr(acc *postingResult, hasAcc *bool, expr qir.Expr) (bool, error) {
	if *hasAcc {
		if !acc.neg && !acc.ids.IsEmpty() && expr.FieldOrdinal >= 0 {

			usePostingApply := false

			if isScalarEqOrInOp(expr.Op) || expr.Op == qir.OpHASALL || expr.Op == qir.OpHASANY {
				usePostingApply = true

			} else if expr.Op.IsScalarRangeOrPrefix() {
				usePostingApply = true
				if qv.snap != nil {
					cacheKey := qv.materializedPredKey(expr)
					if !cacheKey.IsZero() {
						if _, ok := qv.snap.LoadMaterializedPredKey(cacheKey); ok {
							usePostingApply = false
						} else if qv.snap.ShouldPromoteRuntimeMaterializedPredKey(cacheKey) {
							usePostingApply = false
						}
					}
				}
			}

			if usePostingApply {
				p, ok := qv.buildPredicateWithMode(expr, false)
				if ok {
					defer releasePredicateOwnedState(&p)

					if p.alwaysFalse {
						acc.ids.Release()
						*acc = postingResult{}
						return true, nil
					}

					if !p.alwaysTrue && !p.covered {
						if p.fieldIndexRangeState != nil {
							dstCard := acc.ids.Cardinality()
							p.setExpectedContainsCalls(clampUint64ToInt(dstCard))
							state := p.fieldIndexRangeState
							if state.probePostingFilter && state.keepProbeHits {
								maxRowsForContains := rangePostingFilterKeepRowsMaxCard
								if state.probe.probeLen > 0 {
									maxRowsForContains = maxRowsForContains / state.probe.probeLen
									if maxRowsForContains < 1 {
										maxRowsForContains = 1
									}
								}
								if dstCard > uint64(maxRowsForContains) {
									usePostingApply = false
								}
							} else if state.probePostingFilter && !state.keepProbeHits {
								applyWork := postingUnionLinearWork(state.probe.probeLen, state.probe.probeEst)
								materializeWork := postingUnionLinearWork(state.bucketCount, p.estCard)
								if satMulUint64(applyWork, evalAndComplementRangeApplyMinWorkGain) >= materializeWork {
									usePostingApply = false
								}
							}
						}
						if usePostingApply {
							if next, ok := p.applyToPosting(acc.ids); ok {
								acc.ids = next
								acc.neg = false
								if acc.ids.IsEmpty() {
									acc.ids.Release()
									*acc = postingResult{}
									return true, nil
								}
								return false, nil
							}
						}
					}
				}
			}
		}
	}

	b, err := qv.evalExpr(expr)
	if err != nil {
		return false, err
	}

	if !b.neg && b.ids.IsEmpty() {
		b.ids.Release()
		if *hasAcc {
			acc.ids.Release()
		}
		return true, nil
	}

	if !*hasAcc {
		*acc = b
		*hasAcc = true
		return false, nil
	}

	next, err := qv.andPostingResult(*acc, b)
	if err != nil {
		acc.ids.Release()
		return false, err
	}

	*acc = next
	if !acc.neg && acc.ids.IsEmpty() {
		acc.ids.Release()
		return true, nil
	}

	return false, nil
}

func (qv *View) tryFilterCardinalityPreparedAndReordered(expr qir.Expr) (uint64, bool, error) {
	if expr.Not || expr.Op != qir.OpAND || len(expr.Operands) < 2 {
		return 0, false, nil
	}

	var leavesBuf [cardinalityPredicateScanMaxLeaves]qir.Expr
	leaves, ok := qir.CollectAndLeavesScratch(expr, leavesBuf[:0], qir.LeafModeCollect)
	if !ok || len(leaves) < 2 || len(leaves) > cardinalityPredicateScanMaxLeaves {
		return 0, false, nil
	}

	universe := qv.snap.Universe.Cardinality()
	if universe == 0 {
		return 0, true, nil
	}

	if !qv.snap.AllowsMaterializedPredCard(universe) && !qv.snap.CanShareModeratelyOversizedMaterializedPred(universe) {
		mergedRanges := orderedMergedScalarRangeFieldSlicePool.Get(len(leaves))
		mergedRanges, ok = qv.collectMergedNumericRangeFields(leaves, mergedRanges)
		if ok {
			for i := range mergedRanges {
				merged := mergedRanges[i]
				if merged.count < 2 {
					continue
				}
				fm := qv.exec.Schema.Fields[merged.field]
				if fm == nil || fm.Slice {
					continue
				}
				ov := qv.fieldIndexViewFromSlotsForExpr(qv.snap.Index, merged.expr)
				if !ov.HasData() {
					continue
				}
				br := ov.RangeForBounds(merged.bounds)
				if br.Empty() {
					orderedMergedScalarRangeFieldSlicePool.Put(mergedRanges)
					return 0, true, nil
				}
				_, est := ov.RangeStats(br)
				if est == 0 || qv.snap.AllowsMaterializedPredCard(est) || qv.snap.CanShareModeratelyOversizedMaterializedPred(est) {
					continue
				}

				// Count-only callers discard the merged range posting immediately; for
				// unshareable broad ranges, count against the residual accumulator instead.
				var filteredBuf [cardinalityPredicateScanMaxLeaves]qir.Expr
				filtered := filteredBuf[:0]
				for j := range leaves {
					e := leaves[j]
					if qv.isPositiveMergedNumericRangeLeaf(e) && qv.exec.FieldNameByOrdinal(e.FieldOrdinal) == merged.field {
						continue
					}
					filtered = append(filtered, e)
				}

				if len(filtered) == 0 {
					cnt, ok := qv.trySnapshotNumericRangeCardinality(merged.field, fm, ov, br.BaseStart, br.BaseEnd)
					orderedMergedScalarRangeFieldSlicePool.Put(mergedRanges)
					if ok {
						return cnt, true, nil
					}
					return est, true, nil
				}

				acc, ok, err := qv.evalAndOperandsExceptReordered(filtered, -1)
				if err != nil {
					orderedMergedScalarRangeFieldSlicePool.Put(mergedRanges)
					return 0, true, err
				}
				if !ok {
					orderedMergedScalarRangeFieldSlicePool.Put(mergedRanges)
					return 0, false, nil
				}
				if acc.neg {
					acc.ids.Release()
					break
				}
				var core preparedScalarRangePredicate
				qv.initPreparedExactScalarRangePredicate(&core, merged.expr, fm, merged.bounds)
				if plan, ok := core.prepareComplementMaterialization(); ok && plan.est > 0 && plan.est < est {
					ids := core.materializeComplement(plan)
					accCard := acc.ids.Cardinality()
					if ids.IsEmpty() {
						acc.ids.Release()
						orderedMergedScalarRangeFieldSlicePool.Put(mergedRanges)
						return accCard, true, nil
					}
					excluded := acc.ids.AndCardinality(ids)
					ids.Release()
					acc.ids.Release()
					orderedMergedScalarRangeFieldSlicePool.Put(mergedRanges)
					if excluded >= accCard {
						return 0, true, nil
					}
					return accCard - excluded, true, nil
				}
				p, ok := qv.buildMergedScalarRangePredicate(merged.expr, merged.bounds, false)
				if ok {
					cnt, counted := p.countBucket(acc.ids)
					releasePredicateOwnedState(&p)
					if counted {
						acc.ids.Release()
						orderedMergedScalarRangeFieldSlicePool.Put(mergedRanges)
						return cnt, true, nil
					}
				}
				acc.ids.Release()
				break
			}
		}
		orderedMergedScalarRangeFieldSlicePool.Put(mergedRanges)
	}

	acc, ok, err := qv.evalAndOperandsExceptReordered(leaves, -1)
	if err != nil {
		return 0, true, err
	}
	if !ok {
		return 0, false, nil
	}
	cnt := qv.postingResultCardinality(acc)
	acc.ids.Release()
	return cnt, true, nil
}
