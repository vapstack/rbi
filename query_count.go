package rbi

import (
	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/normalize"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

const countPredicateScanMaxLeaves = 16
const countORPredicateMaxBranchesBase = 5
const countORHybridMaterializedBranchMax = 3
const countORSeenUnionThresholdBase = 64_000
const countORSeenUniverseDiv = 16
const countScalarInSplitMaxValues = 32
const countPredSetMaterializeMinTermsBase = 6
const countPredCustomMaterializeMinProbeBase = 4_096
const countPredBroadRangeLazyMinCard = 65_536
const countPredBroadRangeComplementMaxCard = 32_768
const countPredBroadRangeComplementMaxCardCap = 131_072
const countPredBroadRangeComplementFastProbeMin = 4_096
const countPredBroadRangeComplementFastAvgPerBucketMax = 8
const countPredLeadResidualHasAnyExactMaxTerms = 4
const countPredLeadResidualHasAnyExactMinCard = 65_536

// Count evaluates the expression from the given query and returns the number of matching records.
// It ignores Order, Offset and Limit fields.
// If q is nil, Count returns the total number of keys currently present in the database.
func (db *DB[K, V]) Count(q *qx.QX) (uint64, error) {
	if err := db.beginOp(); err != nil {
		return 0, err
	}
	defer db.endOp()

	if db.transparent {
		return 0, ErrNoIndex
	}

	snap, seq, ref, pinned := db.pinCurrentSnapshot()
	defer db.unpinCurrentSnapshot(seq, ref, pinned)

	view := db.makeQueryView(snap)
	defer db.releaseQueryView(view)
	return view.countInternal(q, true)
}

func countInternalFinishTrace(trace *queryTrace, out uint64, err error) (uint64, error) {
	if trace != nil {
		trace.finish(out, err)
	}
	return out, err
}

func (qv *queryView[K, V]) countInternal(q *qx.QX, emitTrace bool) (uint64, error) {
	if q == nil {
		return qv.snapshotUniverseCardinality(), nil
	}

	if err := qv.checkUsedExpr(q.Expr); err != nil {
		return 0, err
	}

	expr, _ := normalize.Expr(q.Expr)
	var trace *queryTrace
	if emitTrace && qv.root.traceOrCalibrationSamplingEnabled() {
		trace = qv.root.beginTrace(&qx.QX{Expr: expr})
	}

	if out, ok, err := qv.tryCountByUniqueEq(expr, trace); ok || err != nil {
		return countInternalFinishTrace(trace, out, err)
	}
	if out, ok, err := qv.tryCountByScalarLookup(expr, trace); ok || err != nil {
		return countInternalFinishTrace(trace, out, err)
	}
	if out, ok, err := qv.tryCountByScalarInSplit(expr, trace); ok || err != nil {
		return countInternalFinishTrace(trace, out, err)
	}
	if out, ok, err := qv.tryCountByPredicates(expr, trace); ok || err != nil {
		return countInternalFinishTrace(trace, out, err)
	}
	if out, ok, err := qv.tryCountORByPredicates(expr, trace); ok || err != nil {
		return countInternalFinishTrace(trace, out, err)
	}

	b, err := qv.evalExpr(expr)
	if err != nil {
		return countInternalFinishTrace(trace, 0, err)
	}
	defer b.release()

	if trace != nil {
		trace.setPlan(PlanCountMaterialized)
		if b.neg {
			trace.addExamined(qv.snapshotUniverseCardinality())
		} else if !b.ids.IsEmpty() {
			trace.addExamined(b.ids.Cardinality())
		}
	}

	return countInternalFinishTrace(trace, qv.countPostingResult(b), nil)
}

func (qv *queryView[K, V]) tryCountByScalarLookup(expr qx.Expr, trace *queryTrace) (uint64, bool, error) {
	if expr.Field == "" {
		return 0, false, nil
	}

	f := qv.fields[expr.Field]
	if f == nil || f.Slice {
		return 0, false, nil
	}

	ov := qv.fieldOverlay(expr.Field)
	if !ov.hasData() && !qv.hasIndexedField(expr.Field) {
		return 0, false, nil
	}

	switch expr.Op {
	case qx.OpEQ:
		key, isSlice, isNil, err := qv.exprValueToIdxScalar(expr)
		if err != nil || isSlice {
			return 0, false, err
		}
		hit := uint64(0)
		if isNil {
			hit = qv.nilFieldOverlay(expr.Field).lookupCardinality(nilIndexEntryKey)
		} else {
			hit = ov.lookupCardinality(key)
		}
		if trace != nil {
			trace.setPlan(PlanCountScalarLookup)
			trace.addExamined(hit)
		}
		return countScalarLookupComplement(qv.snapshotUniverseCardinality(), hit, expr.Not), true, nil

	case qx.OpIN:
		valsBuf, isSlice, hasNil, err := qv.exprValueToDistinctIdxBuf(expr)
		if valsBuf != nil {
			defer stringSlicePool.Put(valsBuf)
		}
		valCount := 0
		if valsBuf != nil {
			valCount = valsBuf.Len()
		}
		if err != nil || !isSlice || (valCount == 0 && !hasNil) {
			return 0, false, err
		}

		var sum uint64
		for i := 0; i < valCount; i++ {
			sum += ov.lookupCardinality(valsBuf.Get(i))
		}
		if hasNil {
			sum += qv.nilFieldOverlay(expr.Field).lookupCardinality(nilIndexEntryKey)
		}
		if trace != nil {
			trace.setPlan(PlanCountScalarLookup)
			trace.addExamined(sum)
		}
		return countScalarLookupComplement(qv.snapshotUniverseCardinality(), sum, expr.Not), true, nil
	}

	return 0, false, nil
}

func countScalarLookupComplement(universe, hit uint64, invert bool) uint64 {
	if !invert {
		return hit
	}
	if hit >= universe {
		return 0
	}
	return universe - hit
}

func countPostingAgainstResult(ids posting.List, filter postingResult) uint64 {
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

type countLeadResidualExactFilter struct {
	idx   int
	ids   posting.List
	state *postsAnyFilterState
}

func shouldUseCountLeadResidualHasAnyExactFilter(p predicate) bool {
	if p.kind != predicateKindPostsAny || p.expr.Not || p.expr.Op != qx.OpHASANY {
		return false
	}
	if p.postCount() < 2 || p.postCount() > countPredLeadResidualHasAnyExactMaxTerms {
		return false
	}
	return p.estCard >= countPredLeadResidualHasAnyExactMinCard
}

func countLeadResidualExactFiltersContain(filters *pooled.SliceBuf[countLeadResidualExactFilter], idx int) bool {
	if filters == nil {
		return false
	}
	for i := 0; i < filters.Len(); i++ {
		if filters.Get(i).idx == idx {
			return true
		}
	}
	return false
}

func countIndexSliceContains(active []int, idx int) bool {
	for _, v := range active {
		if v == idx {
			return true
		}
	}
	return false
}

func (qv *queryView[K, V]) buildCountLeadResidualExactFiltersByCandidatesInto(dst *pooled.SliceBuf[countLeadResidualExactFilter], preds predicateReader, candidates []int) {
	dst.Truncate()
	for _, pi := range candidates {
		if pi < 0 || pi >= preds.Len() {
			continue
		}
		p := preds.Get(pi)
		if p.kind != predicateKindPostsAny || p.expr.Not || p.expr.Op != qx.OpHASANY || p.postCount() == 0 {
			continue
		}
		if p.postsAnyState != nil {
			dst.Append(countLeadResidualExactFilter{
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
		dst.Append(countLeadResidualExactFilter{idx: pi, ids: ids})
	}
}

func (qv *queryView[K, V]) buildCountLeadResidualExactFiltersByCandidatesIntoSet(dst *pooled.SliceBuf[countLeadResidualExactFilter], preds predicateSet, candidates []int) {
	dst.Truncate()
	for _, pi := range candidates {
		if pi < 0 || pi >= preds.Len() {
			continue
		}
		p := preds.Get(pi)
		if p.kind != predicateKindPostsAny || p.expr.Not || p.expr.Op != qx.OpHASANY || p.postCount() == 0 {
			continue
		}
		if p.postsAnyState != nil {
			dst.Append(countLeadResidualExactFilter{
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
		dst.Append(countLeadResidualExactFilter{idx: pi, ids: ids})
	}
}

func (qv *queryView[K, V]) collectCountLeadResidualExactCandidatesInto(dst []int, preds predicateReader, active []int, exclude []int) []int {
	dst = dst[:0]
	for _, pi := range active {
		if countIndexSliceContains(exclude, pi) {
			continue
		}
		p := preds.Get(pi)
		if shouldUseCountLeadResidualHasAnyExactFilter(p) {
			dst = append(dst, pi)
		}
	}
	return dst
}

func (qv *queryView[K, V]) collectCountLeadResidualExactCandidatesIntoSet(dst []int, preds predicateSet, active []int, exclude []int) []int {
	dst = dst[:0]
	for _, pi := range active {
		if countIndexSliceContains(exclude, pi) {
			continue
		}
		p := preds.Get(pi)
		if shouldUseCountLeadResidualHasAnyExactFilter(p) {
			dst = append(dst, pi)
		}
	}
	return dst
}

func countApplyLeadResidualExactFilters(src, work posting.List, filters *pooled.SliceBuf[countLeadResidualExactFilter]) (posting.List, posting.List) {
	if src.IsEmpty() {
		return posting.List{}, work
	}
	if filters == nil || filters.Len() == 0 {
		return src, work
	}

	current := src.Borrow()
	for i := 0; i < filters.Len(); i++ {
		f := filters.Get(i)
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
					if !current.SharesPayload(work) {
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

func shouldApplyCountLeadResidualExactFilters(src posting.List, filters *pooled.SliceBuf[countLeadResidualExactFilter]) bool {
	if src.IsEmpty() || filters == nil || filters.Len() == 0 {
		return false
	}
	return shouldBuildCountLeadResidualExactFilters(src, filters.Len())
}

func shouldBuildCountLeadResidualExactFilters(src posting.List, filterCount int) bool {
	if src.IsEmpty() || filterCount <= 0 {
		return false
	}
	return src.Cardinality() > plannerPredicateBucketExactMinCardForChecks(filterCount)
}

func countPostingMatchesMultiSet(preds predicateSet, checks []int, ids posting.List) uint64 {
	var cnt uint64
	it := ids.Iter()
	for it.HasNext() {
		if countPredicatesMatchSet(preds, checks, it.Next()) {
			cnt++
		}
	}
	it.Release()
	return cnt
}

func countORBranchAcceptIdx(
	branches *pooled.SliceBuf[countORBranch],
	branchIdx int,
	br *countORBranch,
	useSeenDedup bool,
	seen *countORSeen,
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
		if !countPredicatesMatchSet(br.preds, checks, idx) {
			return 0
		}
		if seen.Add(idx) {
			return 1
		}
		return 0
	}
	if len(checks) > 0 && !countPredicatesMatchSet(br.preds, checks, idx) {
		return 0
	}
	if br.prevOrderLen == branchIdx {
		for i := 0; i < br.prevOrderLen; i++ {
			if branches.GetPtr(br.prevOrder[i]).matches(idx) {
				return 0
			}
		}
		return 1
	}
	for j := 0; j < branchIdx; j++ {
		if branches.GetPtr(j).matches(idx) {
			return 0
		}
	}
	return 1
}

func countORBranchAcceptPosting(
	branches *pooled.SliceBuf[countORBranch],
	branchIdx int,
	br *countORBranch,
	useSeenDedup bool,
	seen *countORSeen,
	ids posting.List,
	checks []int,
) uint64 {
	if ids.IsEmpty() {
		return 0
	}
	if idx, ok := ids.TrySingle(); ok {
		return countORBranchAcceptIdx(branches, branchIdx, br, useSeenDedup, seen, idx, checks)
	}
	if useSeenDedup && len(checks) == 0 {
		return seen.addPosting(ids)
	}
	var cnt uint64
	it := ids.Iter()
	for it.HasNext() {
		cnt += countORBranchAcceptIdx(branches, branchIdx, br, useSeenDedup, seen, it.Next(), checks)
	}
	it.Release()
	return cnt
}

func (qv *queryView[K, V]) ensureCountLeadResidualExactFiltersSet(
	preds predicateSet,
	candidates []int,
	residualExact []int,
	residualBoth []int,
	extraExactBuf *pooled.SliceBuf[countLeadResidualExactFilter],
	extraExactBuilt bool,
) (*pooled.SliceBuf[countLeadResidualExactFilter], bool, []int) {
	if extraExactBuilt {
		return extraExactBuf, true, residualBoth
	}
	extraExactBuf = countLeadResidualExactFilterSlicePool.Get()
	extraExactBuf.Grow(len(candidates))
	qv.buildCountLeadResidualExactFiltersByCandidatesIntoSet(extraExactBuf, preds, candidates)
	if extraExactBuf.Len() == 0 {
		return extraExactBuf, true, residualBoth
	}
	residualBoth = residualBoth[:0]
	for _, pi := range residualExact {
		if countLeadResidualExactFiltersContain(extraExactBuf, pi) {
			continue
		}
		residualBoth = append(residualBoth, pi)
	}
	return extraExactBuf, true, residualBoth
}

func (qv *queryView[K, V]) buildCountLeadResidualScratchSet(
	preds predicateSet,
	active []int,
	exactActiveDst []int,
	extraExactCandidatesDst []int,
	residualExactDst []int,
	residualBothDst []int,
) ([]int, []int, []int, []int) {
	exactActive := exactActiveDst[:0]
	for _, pi := range active {
		if preds.Get(pi).supportsPostingApply() {
			exactActive = append(exactActive, pi)
		}
	}
	extraExactCandidates := qv.collectCountLeadResidualExactCandidatesIntoSet(
		extraExactCandidatesDst,
		preds,
		active,
		exactActive,
	)
	residualExact := residualExactDst[:0]
	residualBoth := residualBothDst[:0]
	for _, pi := range active {
		if countIndexSliceContains(exactActive, pi) {
			continue
		}
		residualExact = append(residualExact, pi)
		residualBoth = append(residualBoth, pi)
	}
	return exactActive, extraExactCandidates, residualExact, residualBoth
}

func (qv *queryView[K, V]) evalAndOperandsExceptReordered(ops []qx.Expr, skip int) (postingResult, bool, error) {
	if len(ops) == 0 || (skip >= 0 && len(ops) <= 1) {
		return postingResult{}, false, nil
	}

	var filteredBuf [countPredicateScanMaxLeaves]qx.Expr
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

	universe := qv.snapshotUniverseCardinality()
	var plansBuf [countPredicateScanMaxLeaves]countLeafPlan
	plans := plansBuf[:0]
	var posBuf [countPredicateScanMaxLeaves]int
	pos := posBuf[:0]
	var negBuf [countPredicateScanMaxLeaves]int
	neg := negBuf[:0]

	var mergedRangesBuf *pooled.SliceBuf[orderedMergedScalarRangeField]
	if len(filtered) > 1 {
		buf := orderedMergedScalarRangeFieldSlicePool.Get()
		buf.Grow(len(filtered))
		if qv.collectMergedNumericRangeFields(filtered, buf) {
			mergedRangesBuf = buf
			defer orderedMergedScalarRangeFieldSlicePool.Put(mergedRangesBuf)
		} else {
			orderedMergedScalarRangeFieldSlicePool.Put(buf)
		}
	}

	for i := range filtered {
		e := filtered[i]
		planIdx := len(plans)
		plans = append(plans, countLeafPlan{expr: e})
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

	sortCountLeafPlanOrder(plans, pos)
	sortCountLeafPlanOrder(plans, neg)

	var (
		acc    postingResult
		hasAcc bool
	)

	for _, pi := range pos {
		if mergedRangesBuf != nil {
			e := plans[pi].expr
			if qv.isPositiveMergedNumericRangeLeaf(e) {
				idx := findOrderedMergedScalarRangeField(mergedRangesBuf, e.Field)
				if idx >= 0 {
					merged := mergedRangesBuf.Get(idx)
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

func (qv *queryView[K, V]) applyAndMergedExactRangePostingResult(
	acc *postingResult,
	hasAcc *bool,
	e qx.Expr,
	bounds rangeBounds,
) (bool, error) {
	b, ok := qv.evalMergedExactRangePostingResult(e, bounds)
	if !ok {
		return qv.applyAndPostingResultExpr(acc, hasAcc, e)
	}
	if b.ids.IsEmpty() {
		if *hasAcc {
			acc.release()
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
		acc.release()
		return false, err
	}
	*acc = next
	if !acc.neg && acc.ids.IsEmpty() {
		acc.release()
		return true, nil
	}
	return false, nil
}

func (qv *queryView[K, V]) evalMergedExactRangePostingResult(e qx.Expr, bounds rangeBounds) (postingResult, bool) {
	if e.Not || e.Field == "" {
		return postingResult{}, false
	}
	fm := qv.fields[e.Field]
	if fm == nil || fm.Slice {
		return postingResult{}, false
	}
	ov := qv.fieldOverlay(e.Field)
	if !ov.hasData() {
		return postingResult{}, false
	}
	var core preparedScalarRangePredicate[K, V]
	qv.initPreparedExactScalarRangePredicate(&core, e, fm, bounds)
	if cached, ok := core.loadReuse.load(); ok {
		if cached.IsEmpty() {
			return postingResult{}, true
		}
		return postingResult{ids: cached}, true
	}

	br := ov.rangeForBounds(core.bounds)
	if overlayRangeEmpty(br) {
		return postingResult{}, true
	}
	if core.expr.Op != qx.OpPREFIX {
		if out, ok := qv.tryEvalNumericRangeBuckets(core.expr.Field, core.fm, ov, br); ok {
			if out.ids.IsEmpty() {
				return postingResult{}, true
			}
			out.ids = core.secondHitReuse.share(out.ids)
			return out, true
		}
	}

	ids := overlayUnionRange(ov, br)
	if ids.IsEmpty() {
		return postingResult{}, true
	}
	ids = core.secondHitReuse.share(ids)
	return postingResult{ids: ids}, true
}

// tryCountByScalarInSplit accelerates flat AND counts with a positive scalar IN leaf:
// it evaluates all non-IN leaves once into a postingResult filter and then sums
// per-value intersections against that combined filter.
func (qv *queryView[K, V]) tryCountByScalarInSplit(expr qx.Expr, trace *queryTrace) (uint64, bool, error) {
	if expr.Not || expr.Op != qx.OpAND || len(expr.Operands) < 2 {
		return 0, false, nil
	}

	var leavesBuf [countPredicateScanMaxLeaves]qx.Expr
	leaves, ok := collectAndLeavesScratch(expr, leavesBuf[:0])
	if !ok || len(leaves) < 2 || len(leaves) > countPredicateScanMaxLeaves {
		return 0, false, nil
	}

	lead := -1
	leadVals := 0
	for i := range leaves {
		e := leaves[i]
		if e.Not || e.Op != qx.OpIN || e.Field == "" {
			continue
		}
		fm := qv.fields[e.Field]
		if fm == nil || fm.Slice {
			continue
		}
		valsBuf, isSlice, hasNil, err := qv.exprValueToDistinctIdxBuf(qx.Expr{Op: qx.OpIN, Field: e.Field, Value: e.Value})
		totalVals := 0
		if valsBuf != nil {
			totalVals = valsBuf.Len()
			stringSlicePool.Put(valsBuf)
		}
		if hasNil {
			totalVals++
		}
		if err != nil || !isSlice || totalVals < 2 || totalVals > countScalarInSplitMaxValues {
			continue
		}
		if totalVals < 2 {
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
	ov := qv.fieldOverlay(inLeaf.Field)
	if !ov.hasData() {
		return 0, false, nil
	}

	valsBuf, isSlice, hasNil, err := qv.exprValueToDistinctIdxBuf(qx.Expr{Op: qx.OpIN, Field: inLeaf.Field, Value: inLeaf.Value})
	if valsBuf != nil {
		defer stringSlicePool.Put(valsBuf)
	}
	valCount := 0
	if valsBuf != nil {
		valCount = valsBuf.Len()
	}
	if err != nil || !isSlice || (valCount == 0 && !hasNil) {
		return 0, false, nil
	}
	totalVals := valCount
	if hasNil {
		totalVals++
	}
	if totalVals < 2 || totalVals > countScalarInSplitMaxValues {
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
		defer filter.release()
		if filter.ids.IsEmpty() {
			if filter.neg {
				if trace != nil {
					trace.setPlan(PlanCountScalarInSplit)
					trace.addExamined(0)
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
		ids := ov.lookupPostingRetained(valsBuf.Get(i))
		if ids.IsEmpty() {
			continue
		}
		examined += ids.Cardinality()
		if useFilter {
			cnt += countPostingAgainstResult(ids, filter)
			continue
		}
		cnt += ids.Cardinality()
	}
	if hasNil {
		ids := qv.nilFieldOverlay(inLeaf.Field).lookupPostingRetained(nilIndexEntryKey)
		if !ids.IsEmpty() {
			examined += ids.Cardinality()
			if useFilter {
				cnt += countPostingAgainstResult(ids, filter)
			} else {
				cnt += ids.Cardinality()
			}
		}
	}
	if trace != nil {
		trace.setPlan(PlanCountScalarInSplit)
		trace.addExamined(examined)
	}

	return cnt, true, nil
}

func (qv *queryView[K, V]) isPositiveUniqueEqExpr(e qx.Expr) bool {
	if !isPositiveScalarEqLeaf(e) {
		return false
	}
	fm := qv.fields[e.Field]
	return fm != nil && !fm.Slice && fm.Unique
}

func countLeavesForUniquePath(expr qx.Expr, dst []qx.Expr) ([]qx.Expr, bool) {
	if expr.Not {
		return nil, false
	}
	if expr.Op == qx.OpAND {
		leaves, ok := collectAndLeavesScratch(expr, dst)
		if !ok || len(leaves) == 0 {
			return nil, false
		}
		return leaves, true
	}
	if expr.Op == qx.OpOR || expr.Op == qx.OpNOOP || len(expr.Operands) != 0 {
		return nil, false
	}
	if cap(dst) == 0 {
		return []qx.Expr{expr}, true
	}
	dst = dst[:1]
	dst[0] = expr
	return dst, true
}

func (qv *queryView[K, V]) uniqueCountPathPrecheck(expr qx.Expr) (hasUnique bool, ok bool) {
	if expr.Not {
		return false, false
	}

	if expr.Op == qx.OpAND {
		if len(expr.Operands) == 0 {
			return false, false
		}
		var found bool
		for i := range expr.Operands {
			childHasUnique, childOK := qv.uniqueCountPathPrecheck(expr.Operands[i])
			if !childOK {
				return false, false
			}
			if childHasUnique {
				found = true
			}
		}
		return found, true
	}

	if expr.Op == qx.OpOR || expr.Op == qx.OpNOOP || len(expr.Operands) != 0 {
		return false, false
	}
	return qv.isPositiveUniqueEqExpr(expr), true
}

// tryCountByUniqueEq counts expressions anchored by a positive EQ predicate on
// a unique scalar field, which bounds the candidate set to at most one row.
func (qv *queryView[K, V]) tryCountByUniqueEq(expr qx.Expr, trace *queryTrace) (uint64, bool, error) {
	hasUnique, ok := qv.uniqueCountPathPrecheck(expr)
	if !ok || !hasUnique {
		return 0, false, nil
	}

	var leavesBuf [countPredicateScanMaxLeaves]qx.Expr
	leaves, ok := countLeavesForUniquePath(expr, leavesBuf[:0])
	if !ok {
		return 0, false, nil
	}

	// Count paths defer broad numeric-range materialization until a lead is
	// chosen; otherwise NoCaching runs can rebuild huge range bitmaps per query.
	predSet, ok := qv.buildCountPredicatesWithMode(leaves, false)
	if !ok {
		return 0, false, nil
	}
	defer predSet.Release()

	leadIdx := -1
	leadEst := uint64(0)
	for i := 0; i < predSet.Len(); i++ {
		p := predSet.Get(i)
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

	var checksBuf [countPredicateScanMaxLeaves]int
	checks := checksBuf[:0]
	for i := 0; i < predSet.Len(); i++ {
		if i == leadIdx {
			continue
		}
		p := predSet.Get(i)
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
	sortActivePredicatesSet(checks, predSet)

	it := predSet.GetPtr(leadIdx).newIter()
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
			if !predSet.GetPtr(ci).matches(idx) {
				pass = false
				break
			}
		}
		if pass {
			cnt++
		}
	}
	if trace != nil {
		trace.setPlan(PlanCountUniqueEq)
		trace.addExamined(examined)
	}
	return cnt, true, nil
}

func shouldTryCountByPredicates(leaves []qx.Expr) bool {
	if len(leaves) < 2 || len(leaves) > countPredicateScanMaxLeaves {
		return false
	}

	hasNeg := false
	hasComplex := false
	hasRange := false
	for i := range leaves {
		e := leaves[i]
		if e.Not {
			hasNeg = true
		}
		switch e.Op {
		case qx.OpPREFIX, qx.OpSUFFIX, qx.OpCONTAINS, qx.OpHAS, qx.OpHASANY, qx.OpIN:
			hasComplex = true
		default:
			if isNumericRangeOp(e.Op) {
				hasRange = true
			}
		}
	}
	return hasNeg || hasComplex || (hasRange && len(leaves) >= 3)
}

func shouldPreferMaterializedCountEval(preds predicateReader, leadScore, universe uint64) bool {
	if universe == 0 || leadScore <= universe {
		return false
	}

	materializedPositive := 0
	for i := 0; i < preds.Len(); i++ {
		p := preds.Get(i)
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

func countBroadLeadResidualScoreLimit(leadEst, universe uint64) uint64 {
	if universe == 0 || leadEst <= universe/2 {
		return 0
	}
	// Broad leads must still keep residual check work near a small multiple of
	// the universe scan. As the lead narrows within the broad regime, allow more
	// residual score before falling back to postingResult materialization.
	return satAddUint64(satMulUint64(universe, 2), satMulUint64(universe-leadEst, 2))
}

func (qv *queryView[K, V]) shouldRejectBroadLeadPredicateScan(preds predicateReader, active []int, leadEst, universe uint64) bool {
	limit := countBroadLeadResidualScoreLimit(leadEst, universe)
	if limit == 0 {
		return false
	}
	var residualScore uint64
	for _, pi := range active {
		residualScore = satAddUint64(residualScore, qv.countLeadResidualScore(preds.Get(pi), leadEst, universe))
		if residualScore > limit {
			return true
		}
	}
	return false
}

func (qv *queryView[K, V]) buildCountPredicatesWithMode(leaves []qx.Expr, allowMaterialize bool) (predicateSet, bool) {
	owner := predicateSlicePool.Get()
	owner.Grow(len(leaves))
	preds := predicateSet{owner: owner}
	if len(leaves) == 1 && leaves[0].Op == qx.OpNOOP && leaves[0].Not {
		preds.Append(predicate{alwaysFalse: true})
		return preds, true
	}

	var mergedRangesBuf *pooled.SliceBuf[orderedMergedScalarRangeField]
	if len(leaves) > 1 {
		mergedRangesBuf = orderedMergedScalarRangeFieldSlicePool.Get()
		mergedRangesBuf.Grow(len(leaves))
		if !qv.collectMergedNumericRangeFields(leaves, mergedRangesBuf) {
			preds.Release()
			orderedMergedScalarRangeFieldSlicePool.Put(mergedRangesBuf)
			return predicateSet{}, false
		}
		defer orderedMergedScalarRangeFieldSlicePool.Put(mergedRangesBuf)
	}

	for i, e := range leaves {
		if mergedRangesBuf != nil && qv.isPositiveMergedNumericRangeLeaf(e) {
			idx := findOrderedMergedScalarRangeField(mergedRangesBuf, e.Field)
			if idx >= 0 {
				merged := mergedRangesBuf.Get(idx)
				if merged.count > 1 {
					if merged.first != i {
						continue
					}
					p, ok := qv.buildMergedNumericRangePredicate(merged.expr, merged.bounds, allowMaterialize)
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
		preds.Append(p)
	}
	return preds, true
}

type countORBranch struct {
	index        int
	preds        predicateSet
	lead         int
	checksLen    int
	leadInChecks bool
	checks       [countPredicateScanMaxLeaves]int
	matchWeight  uint64
	prevOrderLen int
	prevOrder    [countORPredicateMaxBranchesBase + 1]int
	est          uint64
}

func newCountORBranch(index int, preds predicateSet, lead int, checks []int, est uint64) countORBranch {
	br := countORBranch{
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

func countORBranchDupOrderLess(branches *pooled.SliceBuf[countORBranch], a, b int) bool {
	ba := branches.Get(a)
	bb := branches.Get(b)
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

func countORPreparePrevOrderBuf(branches *pooled.SliceBuf[countORBranch]) {
	if branches == nil || branches.Len() <= 1 {
		return
	}
	for i := 1; i < branches.Len(); i++ {
		br := branches.GetPtr(i)
		br.prevOrderLen = i
		for j := 0; j < i; j++ {
			br.prevOrder[j] = j
		}
		for j := 1; j < br.prevOrderLen; j++ {
			cur := br.prevOrder[j]
			k := j
			for k > 0 && countORBranchDupOrderLess(branches, cur, br.prevOrder[k-1]) {
				br.prevOrder[k] = br.prevOrder[k-1]
				k--
			}
			br.prevOrder[k] = cur
		}
	}
}

func (br *countORBranch) predLen() int {
	return br.preds.Len()
}

func (br *countORBranch) pred(i int) predicate {
	return br.preds.Get(i)
}

func releaseCountORBranchPredicates(br countORBranch) {
	br.preds.Release()
}

type countORMaterializedBranch struct {
	index int
	expr  qx.Expr
	est   uint64
}

type countORSeenMode uint8

const (
	countORSeenModeNone countORSeenMode = iota
	countORSeenModeHash
	countORSeenModePosting
)

const countORSeenHashMaxHint = u64SetPoolMaxCap / 2

type countORSeen struct {
	mode countORSeenMode
	hash u64set
	ids  posting.List
}

func countPredicateLeadPostingCount(p predicate) int {
	switch {
	case p.iterKind == predicateIterPosting && !p.posting.IsEmpty():
		return 1
	case p.kind == predicateKindPostsAny && p.iterKind == predicateIterPostsConcat && p.postCount() >= 2:
		return p.postCount()
	default:
		return 0
	}
}

func countPredicateLeadPostingAt(p predicate, i int) posting.List {
	if p.iterKind == predicateIterPosting {
		return p.posting
	}
	return p.postAt(i)
}

func countORBranchesUnionUpperBoundBuf(branches *pooled.SliceBuf[countORBranch], universe uint64) uint64 {
	var unionUpper uint64
	for i := 0; i < branches.Len(); i++ {
		unionUpper += branches.Get(i).est
		if universe > 0 && unionUpper >= universe {
			return universe
		}
	}
	return unionUpper
}

func countORBranchesUnionEstimateBuf(branches *pooled.SliceBuf[countORBranch], universe uint64) uint64 {
	if universe == 0 {
		return countORBranchesUnionUpperBoundBuf(branches, 0)
	}
	if branches == nil || branches.Len() == 0 {
		return 0
	}

	remainProb := 1.0
	for i := 0; i < branches.Len(); i++ {
		est := branches.Get(i).est
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

func (br *countORBranch) buildPostingFilterChecks(dst []int) []int {
	dst = dst[:0]
	for i := 0; i < br.checksLen; i++ {
		pi := br.checks[i]
		if br.preds.Get(pi).supportsPostingApply() {
			dst = append(dst, pi)
		}
	}
	return dst
}

func countPredicatesMatchSet(preds predicateSet, checks []int, idx uint64) bool {
	for _, pi := range checks {
		if !preds.GetPtr(pi).matches(idx) {
			return false
		}
	}
	return true
}

func (br *countORBranch) checksMatch(idx uint64) bool {
	for i := 0; i < br.checksLen; i++ {
		if !br.preds.GetPtr(br.checks[i]).matches(idx) {
			return false
		}
	}
	return true
}

func (br *countORBranch) matches(idx uint64) bool {
	if !br.leadInChecks && !br.preds.GetPtr(br.lead).matches(idx) {
		return false
	}
	return br.checksMatch(idx)
}

func countORBranchEstimateSet(preds predicateSet, active []int, leadEst uint64) uint64 {
	est := leadEst
	for _, pi := range active {
		p := preds.Get(pi)
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

func appendCountORMaterializedBranch(
	branchTrace []TraceORBranch,
	materializedBranches []countORMaterializedBranch,
	materializedBranchEst uint64,
	branchIdx int,
	reason string,
	op qx.Expr,
	branchEst uint64,
) ([]countORMaterializedBranch, uint64) {
	if branchTrace != nil {
		branchTrace[branchIdx].Skipped = true
		branchTrace[branchIdx].SkipReason = reason
	}
	materializedBranches = append(materializedBranches, countORMaterializedBranch{
		index: branchIdx,
		expr:  op,
		est:   branchEst,
	})
	return materializedBranches, satAddUint64(materializedBranchEst, branchEst)
}

func shouldTryCountORHybridMaterializedSpill(totalBranches int, scanBranches int, spillBranches int, expectedProbes uint64, spillEst uint64, universe uint64) bool {
	if totalBranches < 4 || scanBranches == 0 || spillBranches == 0 {
		return false
	}
	if spillBranches > countORHybridMaterializedBranchMax {
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

func countORSeenUnionThreshold(universe uint64, branches int, expectedProbes uint64) uint64 {
	threshold := uint64(countORSeenUnionThresholdBase)
	if universe > 0 {
		dyn := universe / countORSeenUniverseDiv
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

func countORDedupDupShareBounds(branchCount int, universe uint64, expectedProbes uint64) (float64, float64) {
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

	minShare = plannerClampFloat(minShare, 0.12, 0.30)
	forceShare = plannerClampFloat(forceShare, minShare+0.08, 0.45)
	return minShare, forceShare
}

func countORBranchesMatchWorkBuf(branches *pooled.SliceBuf[countORBranch]) uint64 {
	if branches == nil || branches.Len() == 0 {
		return 0
	}
	var work uint64
	for i := 0; i < branches.Len(); i++ {
		br := branches.Get(i)
		weight := br.matchWeight
		if weight == 0 {
			weight = 1
		}
		work = satAddUint64(work, satMulUint64(br.est, weight))
	}
	return work
}

func countORDedupWorkMultiplier(sumEst uint64, matchWork uint64) float64 {
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

func countORBranchesShouldUseSeenDedupBuf(branches *pooled.SliceBuf[countORBranch], universe uint64, expectedProbes uint64) bool {
	if branches == nil || branches.Len() <= 2 {
		return false
	}
	sumEst := countORBranchesUnionUpperBoundBuf(branches, 0)
	if sumEst == 0 {
		return false
	}
	matchWork := countORBranchesMatchWorkBuf(branches)
	if matchWork == 0 {
		return false
	}

	threshold := countORSeenUnionThreshold(universe, branches.Len(), expectedProbes)
	if matchWork < threshold {
		return false
	}

	workMult := countORDedupWorkMultiplier(sumEst, matchWork)
	if universe > 0 {
		unionEst := countORBranchesUnionEstimateBuf(branches, universe)
		if unionEst > 0 && sumEst > unionEst {
			dupEst := sumEst - unionEst
			dupShare := float64(dupEst) / float64(sumEst)
			dupShare *= workMult
			if dupShare > 1 {
				dupShare = 1
			}
			minDupShare, forceDupShare := countORDedupDupShareBounds(branches.Len(), universe, expectedProbes)
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

func countORSeenCapHintBuf(branches *pooled.SliceBuf[countORBranch], spillEst uint64, universe uint64) int {
	hint := satAddUint64(countORBranchesUnionEstimateBuf(branches, universe), spillEst)
	if universe > 0 && hint > universe {
		hint = universe
	}
	if hint < 64 {
		hint = 64
	}
	return clampUint64ToInt(hint)
}

func newCountORSeenBuf(branches *pooled.SliceBuf[countORBranch], spillEst uint64, universe uint64) countORSeen {
	hint := countORSeenCapHintBuf(branches, spillEst, universe)
	if hint <= countORSeenHashMaxHint {
		return countORSeen{
			mode: countORSeenModeHash,
			hash: newU64Set(hint),
		}
	}
	return countORSeen{mode: countORSeenModePosting}
}

func (s *countORSeen) release() {
	switch s.mode {
	case countORSeenModeHash:
		releaseU64Set(&s.hash)
	case countORSeenModePosting:
		s.ids.Release()
	}
	*s = countORSeen{}
}

func (s *countORSeen) Has(idx uint64) bool {
	switch s.mode {
	case countORSeenModeHash:
		return s.hash.Has(idx)
	case countORSeenModePosting:
		return s.ids.Contains(idx)
	default:
		return false
	}
}

func (s *countORSeen) Add(idx uint64) bool {
	switch s.mode {
	case countORSeenModeHash:
		return s.hash.Add(idx)
	case countORSeenModePosting:
		var added bool
		s.ids, added = s.ids.BuildAddedChecked(idx)
		return added
	default:
		return false
	}
}

func (s *countORSeen) addPosting(ids posting.List) uint64 {
	if ids.IsEmpty() {
		return 0
	}
	switch s.mode {
	case countORSeenModeHash:
		var added uint64
		it := ids.Iter()
		defer it.Release()
		for it.HasNext() {
			if s.hash.Add(it.Next()) {
				added++
			}
		}
		return added
	case countORSeenModePosting:
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

func (qv *queryView[K, V]) countORMaterializedSpillUnion(
	branches []countORMaterializedBranch,
	seen *countORSeen,
	trace *queryTrace,
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
		if !br.expr.Not && br.expr.Op == qx.OpAND && len(br.expr.Operands) > 1 {
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
			res.release()
			return 0, 0, false, nil
		}
		if trace != nil {
			trace.addPostingMaterialization(1)
		}
		if res.ids.IsEmpty() {
			res.release()
			continue
		}

		card := res.ids.Cardinality()
		examined += card
		added := seen.addPosting(res.ids)
		cnt += added
		if branchTrace != nil {
			branchTrace[br.index].RowsExamined += card
			branchTrace[br.index].RowsEmitted += added
		}
		res.release()
	}

	return cnt, examined, true, nil
}

func (qv *queryView[K, V]) tryCountORBranchLeadPostingsBuf(
	branches *pooled.SliceBuf[countORBranch],
	branchIdx int,
	useSeenDedup bool,
	seen *countORSeen,
	trace *TraceORBranch,
) (uint64, uint64, bool) {
	if branchIdx < 0 || branchIdx >= branches.Len() {
		return 0, 0, false
	}

	br := branches.GetPtr(branchIdx)
	if br.lead < 0 || br.lead >= br.predLen() {
		return 0, 0, false
	}

	lead := br.pred(br.lead)
	leadPostCount := countPredicateLeadPostingCount(lead)
	if leadPostCount == 0 {
		return 0, 0, false
	}

	var checksBuf [countPredicateScanMaxLeaves]int
	checks := checksBuf[:br.checksLen]
	copy(checks, br.checks[:br.checksLen])
	var exactChecksBuf [countPredicateScanMaxLeaves]int
	var extraExactCandidatesBuf [countPredicateScanMaxLeaves]int
	var residualExactBuf [countPredicateScanMaxLeaves]int
	var residualBothBuf [countPredicateScanMaxLeaves]int
	exactChecks := br.buildPostingFilterChecks(exactChecksBuf[:0])
	extraExactCandidates := qv.collectCountLeadResidualExactCandidatesIntoSet(
		extraExactCandidatesBuf[:0],
		br.preds,
		checks,
		exactChecks,
	)
	residualExact := residualExactBuf[:0]
	residualBoth := residualBothBuf[:0]
	for _, pi := range checks {
		if countIndexSliceContains(exactChecks, pi) {
			continue
		}
		residualExact = append(residualExact, pi)
		residualBoth = append(residualBoth, pi)
	}

	var extraExactBuf *pooled.SliceBuf[countLeadResidualExactFilter]
	extraExactBuilt := len(extraExactCandidates) == 0

	var bucketWork posting.List
	var extraWork posting.List

	var cnt uint64
	var examined uint64
	for i := 0; i < leadPostCount; i++ {
		ids := countPredicateLeadPostingAt(lead, i)
		if ids.IsEmpty() {
			continue
		}
		card := ids.Cardinality()
		examined += card
		if trace != nil {
			trace.RowsExamined += card
		}
		if len(checks) == 0 {
			added := countORBranchAcceptPosting(branches, branchIdx, br, useSeenDedup, seen, ids, nil)
			cnt += added
			if trace != nil {
				trace.RowsEmitted += added
			}
			continue
		}

		if idx, ok := ids.TrySingle(); ok {
			added := countORBranchAcceptIdx(branches, branchIdx, br, useSeenDedup, seen, idx, checks)
			cnt += added
			if trace != nil {
				trace.RowsEmitted += added
			}
			continue
		}

		if len(exactChecks) == 0 && (extraExactBuf == nil || extraExactBuf.Len() == 0) {
			added := countORBranchAcceptPosting(branches, branchIdx, br, useSeenDedup, seen, ids, checks)
			cnt += added
			if trace != nil {
				trace.RowsEmitted += added
			}
			continue
		}

		current := ids
		exactApplied := false
		if len(exactChecks) > 0 {
			mode, exactIDs, nextBucketWork, _ := plannerFilterPostingByPredicateSetChecks(br.preds, exactChecks, ids, bucketWork, true)
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
		if extraExactBuilt || shouldBuildCountLeadResidualExactFilters(current, len(extraExactCandidates)) {
			extraExactBuf, extraExactBuilt, residualBoth = qv.ensureCountLeadResidualExactFiltersSet(
				br.preds,
				extraExactCandidates,
				residualExact,
				residualBoth,
				extraExactBuf,
				extraExactBuilt,
			)
			if shouldApplyCountLeadResidualExactFilters(current, extraExactBuf) {
				filtered, nextExtraWork := countApplyLeadResidualExactFilters(current, extraWork, extraExactBuf)
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
		added := countORBranchAcceptPosting(branches, branchIdx, br, useSeenDedup, seen, current, checkList)
		cnt += added
		if trace != nil {
			trace.RowsEmitted += added
		}
	}

	if extraExactBuf != nil {
		countLeadResidualExactFilterSlicePool.Put(extraExactBuf)
	}
	bucketWork.Release()
	extraWork.Release()
	return cnt, examined, true
}

func countSetMaterializeMinTerms(probeEst uint64, universe uint64) int {
	var terms int
	if probeEst >= 2_048 {
		terms = countPredSetMaterializeMinTermsBase
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

func countSetMaterializeMinProbe(termCount int, probeEst uint64, universe uint64) uint64 {
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

func shouldMaterializeCountSetPredicate(p predicate, probeEst uint64, universe uint64) bool {
	switch p.kind {
	case predicateKindPostsAny,
		predicateKindPostsAnyNot,
		predicateKindPostsAll,
		predicateKindPostsAllNot:
	default:
		return false
	}
	if p.postCount() <= 1 {
		return false
	}
	if p.postCount() < countSetMaterializeMinTerms(probeEst, universe) {
		return false
	}
	return probeEst >= countSetMaterializeMinProbe(p.postCount(), probeEst, universe)
}

func countCustomMaterializeMinProbe(op qx.Op, est uint64, probeEst uint64, universe uint64) uint64 {
	minProbe := uint64(countPredCustomMaterializeMinProbeBase)
	switch op {
	case qx.OpPREFIX:
		minProbe = 3_072
	case qx.OpSUFFIX:
		minProbe = 6_144
	case qx.OpCONTAINS:
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
		case qx.OpPREFIX:
			div = 384
		case qx.OpCONTAINS:
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

func shouldMaterializeCustomCountPredicate(p predicate, probeEst uint64, universe uint64) bool {
	if !p.isCustomUnmaterialized() {
		return false
	}
	if p.alwaysTrue || p.alwaysFalse || p.covered {
		return false
	}
	switch p.expr.Op {
	case qx.OpPREFIX, qx.OpSUFFIX, qx.OpCONTAINS:
		return probeEst >= countCustomMaterializeMinProbe(p.expr.Op, p.estCard, probeEst, universe)
	default:
		return false
	}
}

type countScalarComplementRouting struct {
	coarseMaterialize  bool
	exactMaterialize   bool
	complementCacheKey materializedPredKey
}

func (route countScalarComplementRouting) wantsComplementMaterialization() bool {
	return route.coarseMaterialize || route.exactMaterialize
}

func (route countScalarComplementRouting) prefersLazyPostingFilter(p predicate, snap *indexSnapshot) bool {
	if !p.isCustomUnmaterialized() || !p.supportsCheapPostingApply() {
		return false
	}
	if !route.wantsComplementMaterialization() {
		return false
	}
	if snap == nil || snap.matPredCacheMaxEntries <= 0 {
		return true
	}
	if route.complementCacheKey.isZero() {
		return true
	}
	return !snap.shouldPromoteRuntimeMaterializedPredKey(route.complementCacheKey)
}

func (qv *queryView[K, V]) countScalarComplementRouting(p predicate, leadProbeEst uint64, universe uint64) countScalarComplementRouting {
	if universe == 0 || leadProbeEst == 0 || !p.isCustomUnmaterialized() || p.expr.Not || !isNumericRangeOp(p.expr.Op) {
		return countScalarComplementRouting{}
	}

	candidate, ok := qv.preparePredicateScalarRangeRoutingCandidate(p)
	if !ok {
		return countScalarComplementRouting{}
	}

	est := candidate.plan.est
	route := countScalarComplementRouting{
		exactMaterialize:   candidate.numeric,
		complementCacheKey: candidate.core.complementCacheKey,
	}
	if leadProbeEst >= est || est < countPredBroadRangeLazyMinCard || est >= universe {
		return route
	}
	threshold := universe - universe/4
	route.coarseMaterialize = est >= threshold
	return route
}

func countBroadRangeComplementMaxCardinality(leadProbeEst, universe uint64) uint64 {
	limit := uint64(countPredBroadRangeComplementMaxCard)
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
	if limit > uint64(countPredBroadRangeComplementMaxCardCap) {
		limit = uint64(countPredBroadRangeComplementMaxCardCap)
	}
	return limit
}

func shouldUseFastCountBroadRangeComplementMaterializationForShape(probeLen int, est uint64) bool {
	if probeLen < countPredBroadRangeComplementFastProbeMin || est == 0 {
		return false
	}
	avgPerBucket := est / uint64(probeLen)
	if avgPerBucket == 0 {
		avgPerBucket = 1
	}
	return avgPerBucket <= countPredBroadRangeComplementFastAvgPerBucketMax
}

func countBaseIndexRangeCardinality(s []index, start, end int) uint64 {
	if start >= end {
		return 0
	}
	var total uint64
	for i := start; i < end; i++ {
		ids := s[i].IDs
		if ids.IsEmpty() {
			continue
		}
		card := ids.Cardinality()
		if ^uint64(0)-total < card {
			return ^uint64(0)
		}
		total += card
	}
	return total
}

func (qv *queryView[K, V]) tryMaterializeBroadRangeComplementPredicateForCount(p *predicate, leadProbeEst uint64, universe uint64, trace *queryTrace) bool {
	if p == nil {
		return false
	}
	route := qv.countScalarComplementRouting(*p, leadProbeEst, universe)
	if !route.wantsComplementMaterialization() {
		return false
	}

	fm := qv.fields[p.expr.Field]
	if fm == nil || fm.Slice {
		return false
	}
	if !route.coarseMaterialize && !isNumericScalarKind(fm.Kind) {
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
				trace.addCountRangeComplementCacheHit(1)
			}
			setPredicateAlwaysTrue(p)
			return true
		}

		if trace != nil {
			trace.addCountRangeComplementCacheHit(1)
		}
		setPredicateMaterializedNot(p, cached)
		return true
	}

	if empty {
		storeEmptyScalarComplementMaterialization(plan)
		setPredicateAlwaysTrue(p)
		return true
	}
	if plan.est > countBroadRangeComplementMaxCardinality(leadProbeEst, universe) {
		return false
	}

	ids := candidate.core.materializeComplement(plan)
	if ids.IsEmpty() {
		storeEmptyScalarComplementMaterialization(plan)
		setPredicateAlwaysTrue(p)
		return true
	}
	if trace != nil {
		trace.addCountRangeComplementBuild(ids.Cardinality(), false)
	}

	ids = plan.sharedReuse.share(ids)
	setPredicateMaterializedNot(p, ids)
	return true
}

func (qv *queryView[K, V]) materializePostingIntersection(posts *pooled.SliceBuf[posting.List]) posting.List {
	if posts == nil || posts.Len() == 0 {
		return posting.List{}
	}

	seed := -1
	var seedCard uint64
	for i := 0; i < posts.Len(); i++ {
		p := posts.Get(i)
		c := p.Cardinality()
		if c == 0 {
			return posting.List{}
		}
		if seed == -1 || c < seedCard {
			seed = i
			seedCard = c
		}
	}

	out := posts.Get(seed).Clone()
	for i := 0; i < posts.Len(); i++ {
		if i == seed || out.IsEmpty() {
			continue
		}
		out = out.BuildAnd(posts.Get(i))
	}
	return out.BuildOptimized()
}

func (qv *queryView[K, V]) materializeSetPredicateForCount(p *predicate) bool {
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

func (qv *queryView[K, V]) materializeCustomPredicateForCount(p *predicate) error {
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

func (qv *queryView[K, V]) prepareCountPredicate(p *predicate, probeEst uint64, universe uint64) error {
	return qv.prepareCountPredicateWithTrace(p, probeEst, universe, nil)
}

func (qv *queryView[K, V]) prepareCountPredicateWithTrace(p *predicate, probeEst uint64, universe uint64, trace *queryTrace) error {
	if p == nil || p.alwaysTrue || p.alwaysFalse || p.covered {
		return nil
	}
	if trace != nil {
		trace.addCountPredicatePreparation(1)
	}
	if probeEst > 0 {
		p.setExpectedContainsCalls(clampUint64ToInt(probeEst))
	}

	if shouldMaterializeCountSetPredicate(*p, probeEst, universe) {
		qv.materializeSetPredicateForCount(p)
	}

	route := qv.countScalarComplementRouting(*p, probeEst, universe)
	if route.prefersLazyPostingFilter(*p, qv.snap) {
		return nil
	}

	if route.wantsComplementMaterialization() &&
		qv.tryMaterializeBroadRangeComplementPredicateForCount(p, probeEst, universe, trace) {
		return nil
	}

	if shouldMaterializeCustomCountPredicate(*p, probeEst, universe) {
		if err := qv.materializeCustomPredicateForCount(p); err != nil {
			return err
		}
	}

	return nil
}

// tryCountByPredicates counts AND expressions by scanning a selective lead
// predicate and validating remaining predicates via contains checks.
//
// It exists to avoid materializing full postingResult intermediates when a lead can
// cheaply prune the candidate space.
func (qv *queryView[K, V]) tryCountByPredicates(expr qx.Expr, trace *queryTrace) (uint64, bool, error) {
	if expr.Not || expr.Op != qx.OpAND || len(expr.Operands) < 2 {
		return 0, false, nil
	}

	var leavesBuf [countPredicateScanMaxLeaves]qx.Expr
	leaves, ok := collectAndLeavesScratch(expr, leavesBuf[:0])
	if !ok || !shouldTryCountByPredicates(leaves) {
		return 0, false, nil
	}

	// Count paths defer broad numeric-range materialization until a lead is
	// chosen; otherwise NoCaching runs can rebuild huge range bitmaps per query.
	predSet, ok := qv.buildCountPredicatesWithMode(leaves, false)
	if !ok {
		return 0, false, nil
	}
	defer predSet.Release()

	universe := qv.snapshotUniverseCardinality()
	if universe == 0 {
		return 0, true, nil
	}

	// Prefer leads that stay cheap under repeated predicate scans; broad
	// HASANY unions are better left to postingResult fallback than to posts_union scans.
	for i := 0; i < predSet.Len(); i++ {
		if predSet.Get(i).alwaysFalse {
			return 0, true, nil
		}
	}
	leadIdx, leadEst, leadScore := qv.pickCountLeadPredicate(predSet, universe)
	if leadIdx < 0 {
		return 0, false, nil
	}
	if shouldPreferMaterializedCountEval(predSet, leadScore, universe) {
		return 0, false, nil
	}
	if err := qv.prepareCountPredicateWithTrace(predSet.GetPtr(leadIdx), leadEst, universe, trace); err != nil {
		return 0, true, err
	}
	if predSet.Get(leadIdx).alwaysFalse {
		return 0, true, nil
	}
	if !predSet.GetPtr(leadIdx).hasIter() {
		return 0, false, nil
	}
	if predSet.Get(leadIdx).estCard > 0 {
		leadEst = predSet.Get(leadIdx).estCard
	}
	leadNeedsCheck := predSet.GetPtr(leadIdx).leadIterNeedsContainsCheck()

	// remaining predicates are ordered by expected check cost/selectivity so we fail fast on likely negatives.
	var activeBuf [countPredicateScanMaxLeaves]int
	active := activeBuf[:0]
	for i := 0; i < predSet.Len(); i++ {
		if i == leadIdx && !leadNeedsCheck {
			continue
		}
		p := predSet.Get(i)
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
	if !predSet.GetPtr(leadIdx).isMaterializedLike() && qv.shouldRejectBroadLeadPredicateScan(predSet, active, leadEst, universe) {
		return 0, false, nil
	}
	for _, pi := range active {
		if err := qv.prepareCountPredicateWithTrace(predSet.GetPtr(pi), leadEst, universe, trace); err != nil {
			return 0, true, err
		}
	}
	write := 0
	for read := 0; read < len(active); read++ {
		pi := active[read]
		p := predSet.Get(pi)
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
	sortActivePredicatesSet(active, predSet)

	// For range/prefix leads on stable base slices, count by buckets first:
	// many buckets can be skipped (or fully counted) via bucketCount hooks.
	if cnt, examined, ok := qv.tryCountByPredicatesLeadBuckets(predSet, leadIdx, active); ok {
		if trace != nil {
			trace.setPlan(PlanCountPredicates)
			trace.addExamined(examined)
		}
		return cnt, true, nil
	}
	// Posting-backed leads can count posting-by-posting so exact-capable
	// residual predicates prune whole posting bitmaps before row fallback.
	if cnt, examined, ok := qv.tryCountByPredicatesLeadPostings(predSet, leadIdx, active); ok {
		if trace != nil {
			trace.setPlan(PlanCountPredicates)
			trace.addExamined(examined)
		}
		return cnt, true, nil
	}

	it := predSet.GetPtr(leadIdx).newIter()
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
			if !predSet.GetPtr(pi).matches(idx) {
				pass = false
				break
			}
		}
		if pass {
			cnt++
		}
	}
	if trace != nil {
		trace.setPlan(PlanCountPredicates)
		trace.addExamined(examined)
	}
	return cnt, true, nil
}

func (qv *queryView[K, V]) tryCountByPredicatesLeadBuckets(preds predicateSet, leadIdx int, active []int) (uint64, uint64, bool) {
	if leadIdx < 0 || leadIdx >= preds.Len() {
		return 0, 0, false
	}
	lead := preds.Get(leadIdx)
	e := lead.expr
	span, ok, err := qv.prepareScalarOverlaySpan(e)
	if err != nil || !ok {
		return 0, 0, false
	}
	ov := span.ov
	if ov.chunked != nil {
		rb, covered, hasBounds, ok := qv.collectPredicateRangeBoundsSet(e.Field, preds)
		if !ok || !hasBounds {
			if ok {
				boolSlicePool.Put(covered)
			}
			return 0, 0, false
		}
		br := ov.rangeForBounds(rb)
		if overlayRangeEmpty(br) {
			boolSlicePool.Put(covered)
			return 0, 0, true
		}
		if br.baseEnd-br.baseStart < 4 {
			boolSlicePool.Put(covered)
			return 0, 0, false
		}

		var activeBuf [countPredicateScanMaxLeaves]int
		activeChecks := activeBuf[:0]
		for _, pi := range active {
			if pi >= 0 && pi < covered.Len() && covered.Get(pi) {
				continue
			}
			activeChecks = append(activeChecks, pi)
		}
		boolSlicePool.Put(covered)

		var exactActiveBuf [countPredicateScanMaxLeaves]int
		var extraExactCandidatesBuf [countPredicateScanMaxLeaves]int
		var residualExactBuf [countPredicateScanMaxLeaves]int
		var residualBothBuf [countPredicateScanMaxLeaves]int
		exactActive, extraExactCandidates, residualExact, residualBoth := qv.buildCountLeadResidualScratchSet(
			preds,
			activeChecks,
			exactActiveBuf[:0],
			extraExactCandidatesBuf[:0],
			residualExactBuf[:0],
			residualBothBuf[:0],
		)
		var extraExactBuf *pooled.SliceBuf[countLeadResidualExactFilter]
		extraExactBuilt := len(extraExactCandidates) == 0

		var cnt uint64
		var examined uint64
		var bucketWork posting.List
		var extraWork posting.List

		cur := ov.newCursor(br, false)
		for {
			_, ids, ok := cur.next()
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
				if countPredicatesMatchSet(preds, activeChecks, idx) {
					cnt++
				}
				continue
			}
			if len(activeChecks) == 1 {
				if in, ok := preds.Get(activeChecks[0]).countBucket(ids); ok {
					cnt += in
					continue
				}
			}

			if len(exactActive) == 0 && (extraExactBuf == nil || extraExactBuf.Len() == 0) {
				cnt += countPostingMatchesMultiSet(preds, activeChecks, ids)
				continue
			}

			current := ids
			exactApplied := false
			if len(exactActive) > 0 {
				mode, exactIDs, nextBucketWork, _ := plannerFilterPostingByPredicateSetChecks(preds, exactActive, ids, bucketWork, true)
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
			if extraExactBuilt || shouldBuildCountLeadResidualExactFilters(current, len(extraExactCandidates)) {
				extraExactBuf, extraExactBuilt, residualBoth = qv.ensureCountLeadResidualExactFiltersSet(
					preds,
					extraExactCandidates,
					residualExact,
					residualBoth,
					extraExactBuf,
					extraExactBuilt,
				)
				if shouldApplyCountLeadResidualExactFilters(current, extraExactBuf) {
					filtered, nextExtraWork := countApplyLeadResidualExactFilters(current, extraWork, extraExactBuf)
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
				if countPredicatesMatchSet(preds, checks, idx) {
					cnt++
				}
				continue
			}
			cnt += countPostingMatchesMultiSet(preds, checks, current)
		}

		if extraExactBuf != nil {
			countLeadResidualExactFilterSlicePool.Put(extraExactBuf)
		}
		bucketWork.Release()
		extraWork.Release()
		return cnt, examined, true
	}

	if !span.hasData {
		return 0, 0, false
	}
	rb, covered, hasBounds, ok := qv.collectPredicateRangeBoundsSet(e.Field, preds)
	if !ok || !hasBounds {
		if ok {
			boolSlicePool.Put(covered)
		}
		return 0, 0, false
	}
	br := ov.rangeForBounds(rb)
	start, end := br.baseStart, br.baseEnd
	if start >= end {
		boolSlicePool.Put(covered)
		return 0, 0, true
	}
	if end-start < 4 {
		boolSlicePool.Put(covered)
		return 0, 0, false
	}

	var activeBuf [countPredicateScanMaxLeaves]int
	activeChecks := activeBuf[:0]
	for _, pi := range active {
		if pi >= 0 && pi < covered.Len() && covered.Get(pi) {
			continue
		}
		activeChecks = append(activeChecks, pi)
	}
	boolSlicePool.Put(covered)

	var exactActiveBuf [countPredicateScanMaxLeaves]int
	var extraExactCandidatesBuf [countPredicateScanMaxLeaves]int
	var residualExactBuf [countPredicateScanMaxLeaves]int
	var residualBothBuf [countPredicateScanMaxLeaves]int
	exactActive, extraExactCandidates, residualExact, residualBoth := qv.buildCountLeadResidualScratchSet(
		preds,
		activeChecks,
		exactActiveBuf[:0],
		extraExactCandidatesBuf[:0],
		residualExactBuf[:0],
		residualBothBuf[:0],
	)
	var extraExactBuf *pooled.SliceBuf[countLeadResidualExactFilter]
	extraExactBuilt := len(extraExactCandidates) == 0

	var cnt uint64
	var examined uint64
	var bucketWork posting.List
	var extraWork posting.List

	cur := ov.newCursor(ov.rangeByRanks(start, end), false)
	for {
		_, ids, ok := cur.next()
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
			if countPredicatesMatchSet(preds, activeChecks, idx) {
				cnt++
			}
			continue
		}
		if len(activeChecks) == 1 {
			if in, ok := preds.Get(activeChecks[0]).countBucket(ids); ok {
				cnt += in
				continue
			}
		}

		if len(exactActive) == 0 && (extraExactBuf == nil || extraExactBuf.Len() == 0) {
			cnt += countPostingMatchesMultiSet(preds, activeChecks, ids)
			continue
		}

		current := ids
		exactApplied := false
		if len(exactActive) > 0 {
			mode, exactIDs, nextBucketWork, _ := plannerFilterPostingByPredicateSetChecks(preds, exactActive, ids, bucketWork, true)
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
		if extraExactBuilt || shouldBuildCountLeadResidualExactFilters(current, len(extraExactCandidates)) {
			extraExactBuf, extraExactBuilt, residualBoth = qv.ensureCountLeadResidualExactFiltersSet(
				preds,
				extraExactCandidates,
				residualExact,
				residualBoth,
				extraExactBuf,
				extraExactBuilt,
			)
			if shouldApplyCountLeadResidualExactFilters(current, extraExactBuf) {
				filtered, nextExtraWork := countApplyLeadResidualExactFilters(current, extraWork, extraExactBuf)
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
			if countPredicatesMatchSet(preds, checks, idx) {
				cnt++
			}
			continue
		}
		cnt += countPostingMatchesMultiSet(preds, checks, current)
	}

	if extraExactBuf != nil {
		countLeadResidualExactFilterSlicePool.Put(extraExactBuf)
	}
	bucketWork.Release()
	extraWork.Release()
	return cnt, examined, true
}

func (qv *queryView[K, V]) tryCountByPredicatesLeadPostings(preds predicateSet, leadIdx int, active []int) (uint64, uint64, bool) {
	if leadIdx < 0 || leadIdx >= preds.Len() {
		return 0, 0, false
	}
	lead := preds.Get(leadIdx)
	if lead.expr.Not {
		return 0, 0, false
	}
	leadPostCount := countPredicateLeadPostingCount(lead)
	if leadPostCount == 0 {
		return 0, 0, false
	}

	var activeBuf [countPredicateScanMaxLeaves]int
	activeChecks := append(activeBuf[:0], active...)
	var exactActiveBuf [countPredicateScanMaxLeaves]int
	var extraExactCandidatesBuf [countPredicateScanMaxLeaves]int
	var residualExactBuf [countPredicateScanMaxLeaves]int
	var residualBothBuf [countPredicateScanMaxLeaves]int
	exactActive, extraExactCandidates, residualExact, residualBoth := qv.buildCountLeadResidualScratchSet(
		preds,
		activeChecks,
		exactActiveBuf[:0],
		extraExactCandidatesBuf[:0],
		residualExactBuf[:0],
		residualBothBuf[:0],
	)
	var extraExactBuf *pooled.SliceBuf[countLeadResidualExactFilter]
	extraExactBuilt := len(extraExactCandidates) == 0

	var cnt uint64
	var examined uint64
	var bucketWork posting.List
	var extraWork posting.List

	for i := 0; i < leadPostCount; i++ {
		ids := countPredicateLeadPostingAt(lead, i)
		if ids.IsEmpty() {
			continue
		}
		examined += ids.Cardinality()

		if len(activeChecks) == 0 {
			cnt += ids.Cardinality()
			continue
		}
		if idx, ok := ids.TrySingle(); ok {
			if countPredicatesMatchSet(preds, activeChecks, idx) {
				cnt++
			}
			continue
		}
		if len(activeChecks) == 1 {
			if in, ok := preds.Get(activeChecks[0]).countBucket(ids); ok {
				cnt += in
				continue
			}
		}

		if len(exactActive) == 0 && (extraExactBuf == nil || extraExactBuf.Len() == 0) {
			cnt += countPostingMatchesMultiSet(preds, activeChecks, ids)
			continue
		}

		current := ids
		exactApplied := false
		if len(exactActive) > 0 {
			mode, exactIDs, nextBucketWork, _ := plannerFilterPostingByPredicateSetChecks(preds, exactActive, ids, bucketWork, true)
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
		if extraExactBuilt || shouldBuildCountLeadResidualExactFilters(current, len(extraExactCandidates)) {
			extraExactBuf, extraExactBuilt, residualBoth = qv.ensureCountLeadResidualExactFiltersSet(
				preds,
				extraExactCandidates,
				residualExact,
				residualBoth,
				extraExactBuf,
				extraExactBuilt,
			)
			if shouldApplyCountLeadResidualExactFilters(current, extraExactBuf) {
				filtered, nextExtraWork := countApplyLeadResidualExactFilters(current, extraWork, extraExactBuf)
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
			if countPredicatesMatchSet(preds, checks, idx) {
				cnt++
			}
			continue
		}
		cnt += countPostingMatchesMultiSet(preds, checks, current)
	}

	if extraExactBuf != nil {
		countLeadResidualExactFilterSlicePool.Put(extraExactBuf)
	}
	bucketWork.Release()
	extraWork.Release()
	return cnt, examined, true
}

func shouldTryCountORByPredicates(expr qx.Expr) bool {
	if expr.Not || expr.Op != qx.OpOR {
		return false
	}
	n := len(expr.Operands)
	if n < 2 {
		return false
	}
	if !countORHasPrefixLikeBranch(expr) {
		return false
	}
	return true
}

func countORPredicateBranchLimit(universe uint64) int {
	limit := countORPredicateMaxBranchesBase
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

func countORHasPrefixLikeBranch(expr qx.Expr) bool {
	for _, op := range expr.Operands {
		found, ok := countHasPositivePrefixLikeAndLeaf(op)
		if !ok {
			return false
		}
		if found {
			return true
		}
	}
	return false
}

func countHasPositivePrefixLikeAndLeaf(e qx.Expr) (bool, bool) {
	switch e.Op {
	case qx.OpNOOP:
		return false, false
	case qx.OpAND:
		if e.Not || len(e.Operands) == 0 {
			return false, false
		}
		for _, ch := range e.Operands {
			found, ok := countHasPositivePrefixLikeAndLeaf(ch)
			if !ok {
				return false, false
			}
			if found {
				return true, true
			}
		}
		return false, true
	default:
		if e.Op == qx.OpOR || len(e.Operands) != 0 {
			return false, false
		}
		if e.Not {
			return false, true
		}
		switch e.Op {
		case qx.OpPREFIX, qx.OpSUFFIX, qx.OpCONTAINS:
			return true, true
		default:
			return false, true
		}
	}
}

func countLeadOpWeight(op qx.Op) uint64 {
	switch op {
	case qx.OpEQ:
		return 1
	case qx.OpIN:
		return 3
	case qx.OpHAS:
		return 4
	case qx.OpHASANY:
		return 12
	default:
		if isScalarRangeOrPrefixOp(op) {
			return 2
		}
		return 8
	}
}

func countPredicateLeadWeight(p predicate) uint64 {
	if p.isMaterializedLike() {
		return 1
	}
	switch p.expr.Op {
	case qx.OpEQ:
		return 1
	case qx.OpIN:
		return 2
	case qx.OpHAS:
		return 5
	case qx.OpHASANY:
		return 12
	default:
		if isScalarRangeOrPrefixOp(p.expr.Op) {
			return 4
		}
		return countLeadOpWeight(p.expr.Op)
	}
}

func countPredicateLeadScanWeight(p predicate) uint64 {
	weight := countPredicateLeadWeight(p)
	if !p.isCustomUnmaterialized() {
		return weight
	}
	if isScalarRangeOrPrefixOp(p.expr.Op) {
		return satMulUint64(weight, 3)
	}
	return weight
}

func countLeadTooRisky(op qx.Op, est, universe uint64) bool {
	if universe == 0 || est == 0 {
		return false
	}
	switch op {
	case qx.OpHASANY:
		return est > universe/16 && est > 16_384
	case qx.OpHAS:
		return est > universe/8 && est > 32_768
	default:
		return false
	}
}

func satMulUint64(a, b uint64) uint64 {
	if a == 0 || b == 0 {
		return 0
	}
	if ^uint64(0)/a < b {
		return ^uint64(0)
	}
	return a * b
}

func (qv *queryView[K, V]) prepareCountORPredicateWithTrace(p *predicate, probeEst uint64, universe uint64, trace *queryTrace) error {
	if p == nil || p.alwaysTrue || p.alwaysFalse || p.covered {
		return nil
	}
	if trace != nil {
		trace.addCountPredicatePreparation(1)
	}
	if probeEst > 0 {
		p.setExpectedContainsCalls(clampUint64ToInt(probeEst))
	}

	route := qv.countScalarComplementRouting(*p, probeEst, universe)
	if route.prefersLazyPostingFilter(*p, qv.snap) {
		return nil
	}

	if route.wantsComplementMaterialization() &&
		qv.tryMaterializeBroadRangeComplementPredicateForCount(p, probeEst, universe, trace) {
		return nil
	}

	return nil
}

func (qv *queryView[K, V]) countPredicateResidualCheckWeight(p predicate, probeEst, universe uint64) uint64 {
	if p.alwaysTrue || p.covered {
		return 0
	}
	if p.alwaysFalse {
		return ^uint64(0) / 4
	}
	if p.isMaterializedLike() {
		return 1
	}
	if shouldUseCountLeadResidualHasAnyExactFilter(p) {
		return 1
	}
	if shouldMaterializeCountSetPredicate(p, probeEst, universe) {
		return 1
	}
	if qv.countScalarComplementRouting(p, probeEst, universe).wantsComplementMaterialization() {
		return 1
	}
	if shouldMaterializeCustomCountPredicate(p, probeEst, universe) {
		return 1
	}

	weight := countPredicateResidualOpWeight(p)
	if p.expr.Not {
		weight++
	}
	return weight
}

func (qv *queryView[K, V]) countORPredicateResidualCheckWeight(p predicate, probeEst, universe uint64) uint64 {
	if p.alwaysTrue || p.covered {
		return 0
	}
	if p.alwaysFalse {
		return ^uint64(0) / 4
	}
	if p.isMaterializedLike() {
		return 1
	}
	if p.supportsCheapPostingApply() {
		return 1
	}
	if shouldUseCountLeadResidualHasAnyExactFilter(p) {
		return 1
	}
	if qv.countScalarComplementRouting(p, probeEst, universe).wantsComplementMaterialization() {
		return 1
	}

	weight := countPredicateResidualOpWeight(p)
	if p.expr.Not {
		weight++
	}
	return weight
}

func (qv *queryView[K, V]) countLeadResidualScore(p predicate, leadEst, universe uint64) uint64 {
	if leadEst == 0 || p.alwaysTrue || p.covered {
		return 0
	}
	weight := qv.countPredicateResidualCheckWeight(p, leadEst, universe)
	if weight == 0 {
		return 0
	}
	effectiveRows := leadEst
	if p.estCard > 0 && p.estCard < effectiveRows {
		effectiveRows = p.estCard
	}
	return satMulUint64(effectiveRows, weight)
}

func (qv *queryView[K, V]) countORLeadResidualScore(p predicate, leadEst, universe uint64) uint64 {
	if leadEst == 0 || p.alwaysTrue || p.covered {
		return 0
	}
	weight := qv.countORPredicateResidualCheckWeight(p, leadEst, universe)
	if weight == 0 {
		return 0
	}
	return satMulUint64(leadEst, weight)
}

func (qv *queryView[K, V]) pickCountLeadPredicate(preds predicateReader, universe uint64) (int, uint64, uint64) {
	leadIdx := -1
	leadEst := uint64(0)
	leadScore := uint64(0)

	for i := 0; i < preds.Len(); i++ {
		p := preds.Get(i)
		if p.alwaysTrue || p.covered || !p.hasIter() || p.estCard == 0 {
			continue
		}
		if countLeadTooRisky(p.expr.Op, p.estCard, universe) {
			continue
		}

		score := satMulUint64(p.estCard, countPredicateLeadScanWeight(p))
		if p.leadIterNeedsContainsCheck() {
			score = satAddUint64(score, qv.countLeadResidualScore(p, p.estCard, universe))
		}
		for j := 0; j < preds.Len(); j++ {
			if j == i {
				continue
			}
			score = satAddUint64(score, qv.countLeadResidualScore(preds.Get(j), p.estCard, universe))
		}

		if leadIdx == -1 || score < leadScore {
			leadIdx = i
			leadEst = p.estCard
			leadScore = score
		}
	}
	return leadIdx, leadEst, leadScore
}

func (qv *queryView[K, V]) pickCountORLeadPredicate(preds predicateSet, universe uint64) (int, uint64, uint64) {
	leadIdx := -1
	leadEst := uint64(0)
	leadScore := uint64(0)

	for i := 0; i < preds.Len(); i++ {
		p := preds.Get(i)
		if p.alwaysTrue || p.covered || !p.hasIter() || p.estCard == 0 {
			continue
		}
		if countLeadTooRisky(p.expr.Op, p.estCard, universe) {
			continue
		}

		score := satMulUint64(p.estCard, countPredicateLeadScanWeight(p))
		if p.leadIterNeedsContainsCheck() {
			score = satAddUint64(score, qv.countORLeadResidualScore(p, p.estCard, universe))
		}
		for j := 0; j < preds.Len(); j++ {
			if j == i {
				continue
			}
			score = satAddUint64(score, qv.countORLeadResidualScore(preds.Get(j), p.estCard, universe))
		}

		if leadIdx == -1 || score < leadScore {
			leadIdx = i
			leadEst = p.estCard
			leadScore = score
		}
	}
	return leadIdx, leadEst, leadScore
}

// tryCountORByPredicates counts top-level OR expressions via branch lead scans.
//
// The path is intentionally conservative: it is enabled only for bounded OR
// shapes where lead-driven probing is usually cheaper than full postingResult union.
func (qv *queryView[K, V]) tryCountORByPredicates(expr qx.Expr, trace *queryTrace) (uint64, bool, error) {
	if !shouldTryCountORByPredicates(expr) {
		return 0, false, nil
	}

	uc := qv.snapshotUniverseCardinality()
	if uc == 0 {
		return 0, true, nil
	}
	branchCount := len(expr.Operands)
	if branchCount > countORPredicateBranchLimit(uc) {
		return 0, false, nil
	}
	strictWide := branchCount > 3
	fullTrace := trace.full()

	var branchTrace []TraceORBranch
	if fullTrace {
		var branchTraceInline [countORPredicateMaxBranchesBase + 1]TraceORBranch
		branchTrace = branchTraceInline[:branchCount]
		for i := range branchTrace {
			branchTrace[i].Index = i
		}
	}

	branchesBuf := countORBranchSlicePool.Get()
	branchesBuf.Grow(len(expr.Operands))
	defer countORBranchSlicePool.Put(branchesBuf)

	var expectedProbes uint64
	var materializedBranchesInline [countORPredicateMaxBranchesBase + 1]countORMaterializedBranch
	materializedBranches := materializedBranchesInline[:0]
	var materializedBranchEst uint64

	// Build branch plans first and estimate total probe budget before scanning.
	// This prevents expensive partial work when the union is too broad.
	var leafBuf [countPredicateScanMaxLeaves]qx.Expr
branchLoop:
	for branchIdx, op := range expr.Operands {
		leaves, ok := collectAndLeavesFixed(op, leafBuf[:0])
		if !ok {
			return 0, false, nil
		}

		// Count paths defer broad numeric-range materialization until a lead is
		// chosen; otherwise NoCaching runs can rebuild huge range bitmaps per query.
		predSet, ok := qv.buildCountPredicatesWithMode(leaves, false)
		if !ok {
			return 0, false, nil
		}

		var activeBuf [countPredicateScanMaxLeaves]int
		active := activeBuf[:0]
		branchFalse := false

		for i := 0; i < predSet.Len(); i++ {
			p := predSet.Get(i)
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
			return uc, true, nil
		}

		leadIdx, leadEst, leadScore := qv.pickCountORLeadPredicate(predSet, uc)
		branchEst := countORBranchEstimateSet(predSet, active, leadEst)
		leadWeight := uint64(0)
		if leadIdx >= 0 {
			leadWeight = countPredicateLeadScanWeight(predSet.Get(leadIdx))
		}

		// Cannot drive branch without iterable lead.
		if leadIdx < 0 {
			if strictWide {
				materializedBranches, materializedBranchEst = appendCountORMaterializedBranch(
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
			return 0, false, nil
		}
		// Keep OR path setup minimal: avoid eager predicate materialization here.
		// Broad OR branches are sensitive to one-shot setup allocations.
		if countLeadTooRisky(predSet.Get(leadIdx).expr.Op, leadEst, uc) {
			if strictWide {
				materializedBranches, materializedBranchEst = appendCountORMaterializedBranch(
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
			return 0, false, nil
		}
		if strictWide {
			if leadWeight > 3 && leadEst > 2_048 && leadEst > uc/64 {
				materializedBranches, materializedBranchEst = appendCountORMaterializedBranch(
					branchTrace,
					materializedBranches,
					materializedBranchEst,
					branchIdx,
					"materialized_spill_expensive_lead",
					op,
					branchEst,
				)
				predSet.Release()
				continue branchLoop
			}
			if leadScore > uc && leadEst > 2_048 && leadEst > uc/32 {
				materializedBranches, materializedBranchEst = appendCountORMaterializedBranch(
					branchTrace,
					materializedBranches,
					materializedBranchEst,
					branchIdx,
					"materialized_spill_expensive_branch_score",
					op,
					branchEst,
				)
				predSet.Release()
				continue branchLoop
			}
			if leadEst > uc/3 {
				materializedBranches, materializedBranchEst = appendCountORMaterializedBranch(
					branchTrace,
					materializedBranches,
					materializedBranchEst,
					branchIdx,
					"materialized_spill_broad_lead",
					op,
					branchEst,
				)
				predSet.Release()
				continue branchLoop
			}
		}

		var checksBuf [countPredicateScanMaxLeaves]int
		checks := checksBuf[:0]
		leadNeedsCheck := predSet.GetPtr(leadIdx).leadIterNeedsContainsCheck()
		for _, pi := range active {
			if pi == leadIdx && !leadNeedsCheck {
				continue
			}
			p := predSet.Get(pi)
			if p.covered || p.alwaysTrue {
				continue
			}
			if p.alwaysFalse {
				branchFalse = true
				break
			}
			if !p.hasContains() {
				if strictWide {
					materializedBranches, materializedBranchEst = appendCountORMaterializedBranch(
						branchTrace,
						materializedBranches,
						materializedBranchEst,
						branchIdx,
						"materialized_spill_no_contains",
						op,
						branchEst,
					)
					predSet.Release()
					continue branchLoop
				}
				predSet.Release()
				return 0, false, nil
			}
			checks = append(checks, pi)
		}
		if branchFalse {
			predSet.Release()
			continue
		}
		for _, pi := range checks {
			if err := qv.prepareCountORPredicateWithTrace(predSet.GetPtr(pi), leadEst, uc, trace); err != nil {
				predSet.Release()
				return 0, true, err
			}
		}
		write := 0
		for read := 0; read < len(checks); read++ {
			pi := checks[read]
			p := predSet.Get(pi)
			if p.covered || p.alwaysTrue {
				continue
			}
			if p.alwaysFalse {
				branchFalse = true
				break
			}
			if !p.hasContains() {
				if strictWide {
					materializedBranches, materializedBranchEst = appendCountORMaterializedBranch(
						branchTrace,
						materializedBranches,
						materializedBranchEst,
						branchIdx,
						"materialized_spill_post_prepare",
						op,
						branchEst,
					)
					predSet.Release()
					continue branchLoop
				}
				predSet.Release()
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
		sortActivePredicatesSet(checks, predSet)
		matchWeight := uint64(0)
		if !countIndexSliceContains(checks, leadIdx) {
			matchWeight = qv.countORPredicateResidualCheckWeight(predSet.Get(leadIdx), leadEst, uc)
		}
		for _, pi := range checks {
			matchWeight = satAddUint64(matchWeight, qv.countORPredicateResidualCheckWeight(predSet.Get(pi), leadEst, uc))
		}
		if matchWeight == 0 {
			matchWeight = 1
		}

		if leadEst == 0 || leadWeight == 0 {
			leadEst = 1
			leadWeight = 1
		}
		expectedProbes += leadEst * leadWeight
		br := newCountORBranch(branchIdx, predSet, leadIdx, checks, leadEst)
		br.matchWeight = matchWeight
		branchesBuf.Append(br)
	}

	if branchesBuf.Len() == 0 {
		if len(materializedBranches) > 0 {
			return 0, false, nil
		}
		return 0, true, nil
	}
	useMaterializedSpill := len(materializedBranches) > 0
	if useMaterializedSpill && !shouldTryCountORHybridMaterializedSpill(branchCount, branchesBuf.Len(), len(materializedBranches), expectedProbes, materializedBranchEst, uc) {
		return 0, false, nil
	}

	// Guardrail: skip this path when projected probe volume approaches a broad
	// union, where postingResult materialization is typically cheaper overall.
	limit := uc * 2
	if strictWide {
		limit = uc
	}
	if !useMaterializedSpill && expectedProbes > limit {
		return 0, false, nil
	}

	useSeenDedup := useMaterializedSpill || countORBranchesShouldUseSeenDedupBuf(branchesBuf, uc, expectedProbes)
	var seen countORSeen
	if useSeenDedup {
		seen = newCountORSeenBuf(branchesBuf, materializedBranchEst, uc)
		defer seen.release()
	} else {
		countORPreparePrevOrderBuf(branchesBuf)
	}

	var cnt uint64
	var examined uint64
	if trace != nil {
		if useMaterializedSpill {
			trace.setPlan(PlanCountORHybrid)
		} else {
			trace.setPlan(PlanCountORPredicates)
		}
	}
	if useMaterializedSpill {
		var spillCnt uint64
		var spillExamined uint64
		var ok bool
		var err error
		spillCnt, spillExamined, ok, err = qv.countORMaterializedSpillUnion(materializedBranches, &seen, trace, branchTrace)
		if err != nil {
			return 0, true, err
		}
		if !ok {
			return 0, false, nil
		}
		cnt += spillCnt
		examined += spillExamined
	}
	for i := 0; i < branchesBuf.Len(); i++ {
		br := branchesBuf.Get(i)
		var brTrace *TraceORBranch
		if branchTrace != nil {
			brTrace = &branchTrace[br.index]
		}
		var cntBranch uint64
		var examinedBranch uint64
		var ok bool
		if cntBranch, examinedBranch, ok = qv.tryCountORBranchLeadPostingsBuf(branchesBuf, i, useSeenDedup, &seen, brTrace); ok {
			cnt += cntBranch
			examined += examinedBranch
			continue
		}

		lead := br.pred(br.lead)
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
				if branchesBuf.GetPtr(j).matches(idx) {
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
		trace.addExamined(examined)
		trace.setORBranches(branchTrace)
	}

	return cnt, true, nil
}

func (qv *queryView[K, V]) countPreparedExpr(expr qx.Expr) (uint64, error) {
	if cnt, ok, err := qv.tryCountPreparedAndReordered(expr); ok || err != nil {
		return cnt, err
	}

	b, err := qv.evalExpr(expr)
	if err != nil {
		return 0, err
	}
	defer b.release()

	return qv.countPostingResult(b), nil
}

type countLeafPlan struct {
	expr        qx.Expr
	selectivity float64
	hasSel      bool
}

func countLeafOpCost(e qx.Expr) int {
	cost := 5
	switch e.Op {
	case qx.OpEQ, qx.OpNOOP:
		cost = 0
	case qx.OpIN, qx.OpHAS, qx.OpHASANY:
		cost = 1
	case qx.OpPREFIX:
		cost = 3
	case qx.OpSUFFIX, qx.OpCONTAINS:
		cost = 4
	default:
		if isNumericRangeOp(e.Op) {
			cost = 2
		}
	}
	if e.Not {
		cost++
	}
	return cost
}

func countPredicateResidualOpWeight(p predicate) uint64 {
	switch p.expr.Op {
	case qx.OpEQ, qx.OpNOOP:
		return 1
	case qx.OpIN, qx.OpHAS:
		return 2
	case qx.OpHASANY:
		return 3
	case qx.OpPREFIX:
		return 6
	case qx.OpSUFFIX:
		return 8
	case qx.OpCONTAINS:
		return 10
	default:
		if isNumericRangeOp(p.expr.Op) {
			return 4
		}
		return max(2, p.checkCost()+2)
	}
}

func sortCountLeafPlanOrder(plans []countLeafPlan, ids []int) {
	if len(ids) <= 1 {
		return
	}
	for i := 1; i < len(ids); i++ {
		cur := ids[i]
		j := i
		for j > 0 && countLeafPlanLess(plans, cur, ids[j-1]) {
			ids[j] = ids[j-1]
			j--
		}
		ids[j] = cur
	}
}

func countLeafPlanLess(plans []countLeafPlan, a, b int) bool {
	pa := plans[a]
	pb := plans[b]
	if pa.hasSel != pb.hasSel {
		return pa.hasSel
	}
	if pa.hasSel && pb.hasSel && pa.selectivity != pb.selectivity {
		return pa.selectivity < pb.selectivity
	}
	ca := countLeafOpCost(pa.expr)
	cb := countLeafOpCost(pb.expr)
	if ca != cb {
		return ca < cb
	}
	return a < b
}

func (qv *queryView[K, V]) applyAndPostingResultExpr(acc *postingResult, hasAcc *bool, expr qx.Expr) (bool, error) {
	b, err := qv.evalExpr(expr)
	if err != nil {
		return false, err
	}
	if !b.neg && b.ids.IsEmpty() {
		b.release()
		if *hasAcc {
			acc.release()
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
		acc.release()
		return false, err
	}
	*acc = next
	if !acc.neg && acc.ids.IsEmpty() {
		acc.release()
		return true, nil
	}
	return false, nil
}

func (qv *queryView[K, V]) tryCountPreparedAndReordered(expr qx.Expr) (uint64, bool, error) {
	if expr.Not || expr.Op != qx.OpAND || len(expr.Operands) < 2 {
		return 0, false, nil
	}

	var leavesBuf [countPredicateScanMaxLeaves]qx.Expr
	leaves, ok := collectAndLeavesScratch(expr, leavesBuf[:0])
	if !ok || len(leaves) < 2 || len(leaves) > countPredicateScanMaxLeaves {
		return 0, false, nil
	}

	universe := qv.snapshotUniverseCardinality()
	if universe == 0 {
		return 0, true, nil
	}

	var plansBuf [countPredicateScanMaxLeaves]countLeafPlan
	plans := plansBuf[:len(leaves)]
	var posBuf [countPredicateScanMaxLeaves]int
	pos := posBuf[:0]
	var negBuf [countPredicateScanMaxLeaves]int
	neg := negBuf[:0]

	for i := range leaves {
		e := leaves[i]
		plans[i] = countLeafPlan{expr: e}
		if sel, _, _, _, ok := qv.estimateLeafOrderCost(e, nil, universe, "", false); ok {
			if sel < 0 {
				sel = 0
			}
			if sel > 1 {
				sel = 1
			}
			plans[i].selectivity = sel
			plans[i].hasSel = true
		}
		if e.Not {
			neg = append(neg, i)
			continue
		}
		pos = append(pos, i)
	}

	// Pure-NOT conjunctions are rare and don't benefit from this path.
	if len(pos) == 0 {
		return 0, false, nil
	}

	sortCountLeafPlanOrder(plans, pos)
	sortCountLeafPlanOrder(plans, neg)

	var (
		acc    postingResult
		hasAcc bool
	)

	for _, pi := range pos {
		done, err := qv.applyAndPostingResultExpr(&acc, &hasAcc, plans[pi].expr)
		if err != nil {
			return 0, true, err
		}
		if done {
			return 0, true, nil
		}
	}
	for _, ni := range neg {
		done, err := qv.applyAndPostingResultExpr(&acc, &hasAcc, plans[ni].expr)
		if err != nil {
			return 0, true, err
		}
		if done {
			return 0, true, nil
		}
	}

	if !hasAcc {
		return 0, true, nil
	}

	cnt := qv.countPostingResult(acc)
	acc.release()
	return cnt, true, nil
}

func (qv *queryView[K, V]) countPostingResult(b postingResult) uint64 {
	if b.neg {
		if b.ids.IsEmpty() {
			return qv.snapshotUniverseCardinality()
		}
		ex := b.ids.Cardinality()
		uc := qv.snapshotUniverseCardinality()
		if ex >= uc {
			return 0
		}
		return uc - ex
	}
	if b.ids.IsEmpty() {
		return 0
	}
	return b.ids.Cardinality()
}
