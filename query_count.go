package rbi

import (
	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/normalize"
	"github.com/vapstack/rbi/internal/posting"
)

const countPredicateScanMaxLeaves = 16
const countORPredicateMaxBranchesBase = 5
const countORHybridMaterializedBranchMax = 3
const countORSeenUnionThresholdBase = 64_000
const countORSeenUniverseDiv = 16
const countScalarInSplitMaxValues = 32
const countScalarInSplitMaxOtherLeaves = 3
const countPredSetMaterializeMinTermsBase = 6
const countPredCustomMaterializeMinProbeBase = 4_096
const countPredBroadRangeLazyMinCard = 65_536
const countPredBroadRangeComplementMaxCard = 32_768
const countPredBroadRangeComplementMaxCardCap = 131_072
const countPredBroadRangeComplementFastProbeMin = 4_096
const countPredBroadRangeComplementFastAvgPerBucketMax = 8
const countPredLeadResidualHasAnyExactMaxTerms = 3
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

	view := db.makeQueryView(db.getSnapshot())
	defer db.releaseQueryView(view)
	return view.countInternal(q, true)
}

func (qv *queryView[K, V]) countInternal(q *qx.QX, emitTrace bool) (out uint64, err error) {
	if q == nil {
		return qv.snapshotUniverseCardinality(), nil
	}

	if err = qv.checkUsedExpr(q.Expr); err != nil {
		return 0, err
	}

	expr, _ := normalize.Expr(q.Expr)
	var trace *queryTrace
	if emitTrace && qv.root.traceOrCalibrationSamplingEnabled() {
		trace = qv.root.beginTrace(&qx.QX{Expr: expr})
		if trace != nil {
			defer func() {
				trace.finish(out, err)
			}()
		}
	}
	var ok bool

	if out, ok, err = qv.tryCountByUniqueEq(expr, trace); ok || err != nil {
		return out, err
	}
	if out, ok, err = qv.tryCountByScalarLookup(expr, trace); ok || err != nil {
		return out, err
	}
	if out, ok, err = qv.tryCountByScalarInSplit(expr, trace); ok || err != nil {
		return out, err
	}
	if out, ok, err = qv.tryCountByPredicates(expr, trace); ok || err != nil {
		return out, err
	}
	if out, ok, err = qv.tryCountORByPredicates(expr, trace); ok || err != nil {
		return out, err
	}

	b, err := qv.evalExpr(expr)
	if err != nil {
		return 0, err
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

	return qv.countPostingResult(b), nil
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
		vals, isSlice, hasNil, err := qv.exprValueToDistinctIdxOwned(expr)
		if err != nil || !isSlice || (len(vals) == 0 && !hasNil) {
			return 0, false, err
		}

		var sum uint64
		for _, key := range vals {
			sum += ov.lookupCardinality(key)
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

type countLeadResidualExactFilter struct {
	idx int
	ids posting.List
}

func shouldUseCountLeadResidualHasAnyExactFilter(p predicate) bool {
	if p.kind != predicateKindPostsAny || p.expr.Not || p.expr.Op != qx.OpHASANY {
		return false
	}
	if len(p.posts) < 2 || len(p.posts) > countPredLeadResidualHasAnyExactMaxTerms {
		return false
	}
	return p.estCard >= countPredLeadResidualHasAnyExactMinCard
}

func countLeadResidualExactFiltersContain(filters []countLeadResidualExactFilter, idx int) bool {
	for _, f := range filters {
		if f.idx == idx {
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

func releaseCountLeadResidualExactFilters(filters []countLeadResidualExactFilter) {
	for _, f := range filters {
		f.ids.Release()
	}
}

func (qv *queryView[K, V]) buildCountLeadResidualExactFilters(preds []predicate, active []int) []countLeadResidualExactFilter {
	candidates := make([]int, 0, len(active))
	for _, pi := range active {
		p := preds[pi]
		if shouldUseCountLeadResidualHasAnyExactFilter(p) {
			candidates = append(candidates, pi)
		}
	}
	return qv.buildCountLeadResidualExactFiltersByCandidates(preds, candidates)
}

func (qv *queryView[K, V]) buildCountLeadResidualExactFiltersByCandidates(preds []predicate, candidates []int) []countLeadResidualExactFilter {
	if len(candidates) == 0 {
		return nil
	}

	filters := make([]countLeadResidualExactFilter, 0, len(candidates))
	for _, pi := range candidates {
		if pi < 0 || pi >= len(preds) {
			continue
		}
		p := preds[pi]
		if p.kind != predicateKindPostsAny || p.expr.Not || p.expr.Op != qx.OpHASANY || len(p.posts) == 0 {
			continue
		}
		ids := qv.materializeProbeUnion(p.posts)
		if ids.IsEmpty() {
			continue
		}
		filters = append(filters, countLeadResidualExactFilter{idx: pi, ids: ids})
	}
	return filters
}

func (qv *queryView[K, V]) collectCountLeadResidualExactCandidates(preds []predicate, active []int) []int {
	out := make([]int, 0, len(active))
	for _, pi := range active {
		p := preds[pi]
		if shouldUseCountLeadResidualHasAnyExactFilter(p) {
			out = append(out, pi)
		}
	}
	return out
}

func countApplyLeadResidualExactFilters(src posting.List, work *posting.List, filters []countLeadResidualExactFilter) (posting.List, bool) {
	if src.IsEmpty() {
		return posting.List{}, true
	}
	if len(filters) == 0 {
		return src, true
	}
	if work == nil {
		return posting.List{}, false
	}

	work.Clear()
	*work = src.Clone()
	for _, f := range filters {
		if f.ids.IsEmpty() {
			work.Clear()
			break
		}
		work.AndInPlace(f.ids)
		if work.IsEmpty() {
			break
		}
	}
	return *work, true
}

func canUseScalarInSplitSupportLeaf(e qx.Expr, dbFields map[string]*field) bool {
	if e.Not || e.Field == "" {
		return false
	}
	fm := dbFields[e.Field]
	if fm == nil || fm.Slice {
		return false
	}
	switch e.Op {
	case qx.OpEQ, qx.OpIN:
		return true
	default:
		return false
	}
}

// tryCountByScalarInSplit accelerates flat AND counts with a positive scalar IN leaf:
// it evaluates all non-IN leaves once and then sums per-value posting intersections.
func (qv *queryView[K, V]) tryCountByScalarInSplit(expr qx.Expr, trace *queryTrace) (uint64, bool, error) {
	if expr.Not || expr.Op != qx.OpAND || len(expr.Operands) < 2 {
		return 0, false, nil
	}

	var leavesBuf [countPredicateScanMaxLeaves]qx.Expr
	leaves, ok := collectAndLeavesScratch(expr, leavesBuf[:0])
	if !ok || len(leaves) < 2 || len(leaves) > countPredicateScanMaxLeaves {
		return 0, false, nil
	}
	if len(leaves)-1 > countScalarInSplitMaxOtherLeaves {
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
		vals, isSlice, hasNil, err := qv.exprValueToDistinctIdxOwned(qx.Expr{Op: qx.OpIN, Field: e.Field, Value: e.Value})
		totalVals := len(vals)
		if hasNil {
			totalVals++
		}
		if err != nil || !isSlice || totalVals < 2 || totalVals > countScalarInSplitMaxValues {
			continue
		}
		n := len(vals)
		if hasNil {
			n++
		}
		if n < 2 {
			continue
		}
		if lead == -1 || n < leadVals {
			lead = i
			leadVals = n
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

	vals, isSlice, hasNil, err := qv.exprValueToDistinctIdxOwned(qx.Expr{Op: qx.OpIN, Field: inLeaf.Field, Value: inLeaf.Value})
	if err != nil || !isSlice || (len(vals) == 0 && !hasNil) {
		return 0, false, nil
	}
	totalVals := len(vals)
	if hasNil {
		totalVals++
	}
	if totalVals < 2 || totalVals > countScalarInSplitMaxValues {
		return 0, false, nil
	}

	for i := range leaves {
		if i == lead {
			continue
		}
		if !canUseScalarInSplitSupportLeaf(leaves[i], qv.fields) {
			return 0, false, nil
		}
	}

	var (
		filter    postingResult
		useFilter bool
	)
	if len(leaves) > 1 {
		filter, err = qv.evalAndOperandsExcept(leaves, lead)
		if err != nil {
			return 0, true, err
		}
		defer filter.release()

		// Keep this path simple and exact only for positive filter sets.
		if filter.neg {
			return 0, false, nil
		}
		if filter.ids.IsEmpty() {
			return 0, true, nil
		}
		useFilter = true
	}

	var cnt uint64
	var examined uint64
	for _, v := range vals {
		ids := ov.lookupPostingRetained(v)
		if ids.IsEmpty() {
			continue
		}
		examined += ids.Cardinality()
		if useFilter {
			cnt += ids.AndCardinality(filter.ids)
			continue
		}
		cnt += ids.Cardinality()
	}
	if hasNil {
		ids := qv.nilFieldOverlay(inLeaf.Field).lookupPostingRetained(nilIndexEntryKey)
		if !ids.IsEmpty() {
			examined += ids.Cardinality()
			if useFilter {
				cnt += ids.AndCardinality(filter.ids)
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
	if e.Not || e.Op != qx.OpEQ || e.Field == "" {
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
	preds, ok := qv.buildPredicatesWithMode(leaves, false)
	if !ok {
		return 0, false, nil
	}
	defer releasePredicates(preds)

	leadIdx := -1
	leadEst := uint64(0)
	for i := range preds {
		p := preds[i]
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

	checksBuf := getIntSliceBuf(len(preds))
	checks := checksBuf.values
	defer func() {
		checksBuf.values = checks
		releaseIntSliceBuf(checksBuf)
	}()
	for i := range preds {
		if i == leadIdx {
			continue
		}
		p := preds[i]
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
	sortActivePredicates(checks, preds)

	it := preds[leadIdx].newIter()
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
			if !preds[ci].matches(idx) {
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
		case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE:
			hasRange = true
		}
	}
	return hasNeg || hasComplex || (hasRange && len(leaves) >= 3)
}

type countORBranch struct {
	index     int
	preds     []predicate
	lead      int
	checks    []int
	checksBuf *intSliceBuf
	est       uint64
}

type countORMaterializedBranch struct {
	index int
	expr  qx.Expr
	est   uint64
}

type countORBranches []countORBranch

func countPredicateLeadPostings(p predicate) ([]posting.List, bool) {
	switch {
	case p.iterKind == predicateIterPosting && !p.posting.IsEmpty():
		return []posting.List{p.posting}, true
	case p.kind == predicateKindPostsAny && p.iterKind == predicateIterPostsConcat && len(p.posts) >= 2:
		return p.posts, true
	default:
		return nil, false
	}
}

func releaseCountORBranches(branches countORBranches) {
	for i := range branches {
		releasePredicates(branches[i].preds)
		if branches[i].checksBuf != nil {
			branches[i].checksBuf.values = branches[i].checks
			releaseIntSliceBuf(branches[i].checksBuf)
		}
		branches[i] = countORBranch{}
	}
}

func (br countORBranch) buildPostingFilterChecks(dst []int) []int {
	dst = dst[:0]
	for _, pi := range br.checks {
		if br.preds[pi].supportsPostingFilter() {
			dst = append(dst, pi)
		}
	}
	return dst
}

func countPredicatesMatch(preds []predicate, checks []int, idx uint64) bool {
	for _, pi := range checks {
		if !preds[pi].matches(idx) {
			return false
		}
	}
	return true
}

func (br countORBranch) checksMatch(idx uint64) bool {
	return countPredicatesMatch(br.preds, br.checks, idx)
}

func (br countORBranch) matches(idx uint64) bool {
	if !br.preds[br.lead].matches(idx) {
		return false
	}
	return br.checksMatch(idx)
}

func (branches countORBranches) unionUpperBound(universe uint64) uint64 {
	var unionUpper uint64
	for i := range branches {
		unionUpper += branches[i].est
		if universe > 0 && unionUpper >= universe {
			return universe
		}
	}
	return unionUpper
}

func (branches countORBranches) unionEstimate(universe uint64) uint64 {
	if universe == 0 {
		return branches.unionUpperBound(0)
	}
	if len(branches) == 0 {
		return 0
	}

	remainProb := 1.0
	for i := range branches {
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

func countORBranchEstimate(preds []predicate, active []int, leadEst uint64) uint64 {
	est := leadEst
	for _, pi := range active {
		p := preds[pi]
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

func (branches countORBranches) shouldUseSeenDedup(universe uint64, expectedProbes uint64) bool {
	if len(branches) <= 3 {
		return false
	}
	sumEst := branches.unionUpperBound(0)
	if sumEst == 0 {
		return false
	}

	threshold := countORSeenUnionThreshold(universe, len(branches), expectedProbes)
	if sumEst < threshold {
		return false
	}

	// Overlap-aware gate: avoid paying roaring allocation cost when expected OR
	// overlap is low and pairwise duplicate checks are cheaper.
	if universe > 0 {
		unionEst := branches.unionEstimate(universe)
		if unionEst > 0 && sumEst > unionEst {
			dupEst := sumEst - unionEst
			dupShare := float64(dupEst) / float64(sumEst)
			minDupShare, forceDupShare := countORDedupDupShareBounds(len(branches), universe, expectedProbes)
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

func countORSeenAddPosting(seen *posting.List, ids posting.List) uint64 {
	if seen == nil || ids.IsEmpty() {
		return 0
	}
	var added uint64
	it := ids.Iter()
	defer it.Release()
	for it.HasNext() {
		if seen.CheckedAdd(it.Next()) {
			added++
		}
	}
	return added
}

func (qv *queryView[K, V]) countORMaterializedSpillUnion(
	branches []countORMaterializedBranch,
	seen *posting.List,
	trace *queryTrace,
	branchTrace []TraceORBranch,
) (uint64, uint64, bool, error) {
	if len(branches) == 0 {
		return 0, 0, true, nil
	}
	if seen == nil {
		return 0, 0, false, nil
	}

	var cnt uint64
	var examined uint64
	for _, br := range branches {
		if branchTrace != nil {
			branchTrace[br.index].Skipped = true
			branchTrace[br.index].SkipReason = "materialized_spill"
		}

		res, err := qv.evalExpr(br.expr)
		if err != nil {
			return 0, 0, true, err
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
		added := countORSeenAddPosting(seen, res.ids)
		cnt += added
		if branchTrace != nil {
			branchTrace[br.index].RowsExamined += card
			branchTrace[br.index].RowsEmitted += added
		}
		res.release()
	}

	return cnt, examined, true, nil
}

func (qv *queryView[K, V]) tryCountORBranchLeadPostings(
	branches countORBranches,
	branchIdx int,
	useSeenDedup bool,
	seen *posting.List,
	trace *TraceORBranch,
) (uint64, uint64, bool) {
	if branchIdx < 0 || branchIdx >= len(branches) {
		return 0, 0, false
	}

	br := branches[branchIdx]
	if br.lead < 0 || br.lead >= len(br.preds) {
		return 0, 0, false
	}

	leadPosts, ok := countPredicateLeadPostings(br.preds[br.lead])
	if !ok {
		return 0, 0, false
	}

	exactChecksBuf := getIntSliceBuf(len(br.checks))
	exactChecks := br.buildPostingFilterChecks(exactChecksBuf.values)
	defer func() {
		exactChecksBuf.values = exactChecks
		releaseIntSliceBuf(exactChecksBuf)
	}()

	extraExactCandidatesBuf := getIntSliceBuf(len(br.checks))
	extraExactCandidates := extraExactCandidatesBuf.values[:0]
	extraExactCandidates = append(extraExactCandidates, qv.collectCountLeadResidualExactCandidates(br.preds, br.checks)...)
	defer func() {
		extraExactCandidatesBuf.values = extraExactCandidates
		releaseIntSliceBuf(extraExactCandidatesBuf)
	}()

	residualExactBuf := getIntSliceBuf(len(br.checks))
	residualAfterExact := residualExactBuf.values[:0]
	defer func() {
		residualExactBuf.values = residualAfterExact
		releaseIntSliceBuf(residualExactBuf)
	}()

	residualBothBuf := getIntSliceBuf(len(br.checks))
	residualAfterBoth := residualBothBuf.values[:0]
	defer func() {
		residualBothBuf.values = residualAfterBoth
		releaseIntSliceBuf(residualBothBuf)
	}()

	for _, pi := range br.checks {
		if countIndexSliceContains(exactChecks, pi) {
			continue
		}
		residualAfterExact = append(residualAfterExact, pi)
		residualAfterBoth = append(residualAfterBoth, pi)
	}

	var extraExact []countLeadResidualExactFilter
	defer func() {
		releaseCountLeadResidualExactFilters(extraExact)
	}()
	extraExactBuilt := len(extraExactCandidates) == 0

	var bucketWork posting.List
	defer bucketWork.Release()
	var extraWork posting.List
	defer extraWork.Release()

	ensureExtraExact := func() {
		if extraExactBuilt {
			return
		}
		extraExactBuilt = true
		extraExact = qv.buildCountLeadResidualExactFiltersByCandidates(br.preds, extraExactCandidates)
		if len(extraExact) == 0 {
			return
		}
		extraWork.Release()
		residualAfterBoth = residualAfterBoth[:0]
		for _, pi := range residualAfterExact {
			if countLeadResidualExactFiltersContain(extraExact, pi) {
				continue
			}
			residualAfterBoth = append(residualAfterBoth, pi)
		}
	}

	var cnt uint64
	var examined uint64
	addAccepted := func(n uint64) {
		if n == 0 {
			return
		}
		cnt += n
		if trace != nil {
			trace.RowsEmitted += n
		}
	}
	var acceptIdx func(uint64, []int)
	acceptPostingNoChecks := func(ids posting.List) {
		if ids.IsEmpty() {
			return
		}
		ids.ForEach(func(idx uint64) bool {
			acceptIdx(idx, nil)
			return true
		})
	}
	acceptIdx = func(idx uint64, checks []int) {
		if useSeenDedup && seen != nil && seen.Contains(idx) {
			return
		}
		if len(checks) > 0 && !countPredicatesMatch(br.preds, checks, idx) {
			return
		}
		if useSeenDedup {
			if seen != nil && seen.CheckedAdd(idx) {
				addAccepted(1)
			}
			return
		}
		for j := 0; j < branchIdx; j++ {
			if branches[j].matches(idx) {
				return
			}
		}
		addAccepted(1)
	}

	for _, ids := range leadPosts {
		if ids.IsEmpty() {
			continue
		}
		card := ids.Cardinality()
		examined += card
		if trace != nil {
			trace.RowsExamined += card
		}
		if len(br.checks) == 0 {
			if idx, ok := ids.TrySingle(); ok {
				acceptIdx(idx, nil)
				continue
			}
			ids.ForEach(func(idx uint64) bool {
				acceptIdx(idx, nil)
				return true
			})
			continue
		}

		if idx, ok := ids.TrySingle(); ok {
			acceptIdx(idx, br.checks)
			continue
		}

		if len(exactChecks) == 0 && len(extraExact) == 0 {
			ids.ForEach(func(idx uint64) bool {
				acceptIdx(idx, br.checks)
				return true
			})
			continue
		}

		current := ids
		exactApplied := false
		if len(exactChecks) > 0 {
			mode, exactIDs, _ := plannerFilterPostingByChecks(br.preds, exactChecks, ids, &bucketWork, true)
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
		if !extraExactBuilt {
			ensureExtraExact()
		}
		if len(extraExact) > 0 {
			filtered, ok := countApplyLeadResidualExactFilters(current, &extraWork, extraExact)
			if ok {
				if filtered.IsEmpty() {
					continue
				}
				current = filtered
				extraApplied = true
			}
		}

		checks := br.checks
		if extraApplied {
			checks = residualAfterBoth
		} else if exactApplied {
			checks = residualAfterExact
		}
		if len(checks) == 0 {
			acceptPostingNoChecks(current)
			continue
		}

		it := current.Iter()
		for it.HasNext() {
			acceptIdx(it.Next(), checks)
		}
		it.Release()
	}

	return cnt, examined, true
}

func chainPredicateCleanup(prev, next func()) func() {
	if prev == nil {
		return next
	}
	if next == nil {
		return prev
	}
	return func() {
		next()
		prev()
	}
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
	if len(p.posts) <= 1 {
		return false
	}
	if len(p.posts) < countSetMaterializeMinTerms(probeEst, universe) {
		return false
	}
	return probeEst >= countSetMaterializeMinProbe(len(p.posts), probeEst, universe)
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
	if p.kind != predicateKindCustom {
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

func shouldUseCountBroadRangeComplementMaterialization(p predicate, leadProbeEst uint64, universe uint64) bool {
	if universe == 0 || leadProbeEst == 0 || p.kind != predicateKindCustom {
		return false
	}
	if leadProbeEst >= p.estCard {
		return false
	}
	if p.expr.Not || p.estCard < countPredBroadRangeLazyMinCard {
		return false
	}
	switch p.expr.Op {
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE:
	default:
		return false
	}
	threshold := universe - universe/4
	return p.estCard >= threshold
}

func shouldTryExactNumericCountBroadRangeComplement(p predicate, leadProbeEst uint64, universe uint64) bool {
	if universe == 0 || leadProbeEst == 0 || p.kind != predicateKindCustom || p.expr.Not {
		return false
	}
	switch p.expr.Op {
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE:
		return true
	default:
		return false
	}
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
	coarseEligible := shouldUseCountBroadRangeComplementMaterialization(*p, leadProbeEst, universe)
	exactEligible := shouldTryExactNumericCountBroadRangeComplement(*p, leadProbeEst, universe)
	if !coarseEligible && !exactEligible {
		return false
	}

	fm := qv.fields[p.expr.Field]
	if fm == nil || fm.Slice {
		return false
	}
	if !coarseEligible && !isNumericScalarKind(fm.Kind) {
		return false
	}
	bound, isSlice, err := qv.exprValueToNormalizedScalarBound(qx.Expr{Op: p.expr.Op, Field: p.expr.Field, Value: p.expr.Value})
	if err != nil || isSlice || bound.empty {
		return false
	}
	cacheKey := ""
	if !bound.full {
		cacheKey = qv.materializedPredComplementCacheKeyForScalar(p.expr.Field, bound.op, bound.key)
	}
	if cacheKey != "" {
		if cached, ok := qv.snap.loadMaterializedPred(cacheKey); ok {
			if cached.IsEmpty() {
				if trace != nil {
					trace.addCountRangeComplementCacheHit(1)
				}
				p.kind = predicateKindCustom
				p.iterKind = predicateIterNone
				p.posting = posting.List{}
				p.posts = nil
				p.ids = posting.List{}
				p.contains = nil
				p.iter = nil
				p.bucketCount = nil
				p.estCard = 0
				p.alwaysTrue = true
				p.alwaysFalse = false
				return true
			}

			if trace != nil {
				trace.addCountRangeComplementCacheHit(1)
			}
			p.kind = predicateKindMaterializedNot
			p.iterKind = predicateIterNone
			p.posting = posting.List{}
			p.posts = nil
			p.ids = cached
			p.contains = nil
			p.iter = nil
			p.bucketCount = nil
			p.estCard = 0
			p.alwaysTrue = false
			p.alwaysFalse = false
			return true
		}
	}

	ov := qv.fieldOverlay(p.expr.Field)
	if !ov.hasData() {
		return false
	}
	rb := rangeBounds{has: true}
	applyNormalizedScalarBound(&rb, bound)
	br := ov.rangeForBounds(rb)
	before, after := overlayComplementRangeSpans(ov, br)
	nilPosting := qv.nilFieldOverlay(p.expr.Field).lookupPostingRetained(nilIndexEntryKey)

	complementEst := uint64(0)
	addRangeStats := func(span overlayRange) {
		if overlayRangeEmpty(span) {
			return
		}
		_, est := overlayRangeStats(ov, span)
		complementEst = satAddUint64(complementEst, est)
	}
	addRangeStats(before)
	addRangeStats(after)
	complementEst = satAddUint64(complementEst, nilPosting.Cardinality())
	if complementEst == 0 {
		if cacheKey != "" {
			qv.snap.storeMaterializedPred(cacheKey, posting.List{})
		}
		p.kind = predicateKindCustom
		p.iterKind = predicateIterNone
		p.posting = posting.List{}
		p.posts = nil
		p.ids = posting.List{}
		p.contains = nil
		p.iter = nil
		p.bucketCount = nil
		p.estCard = 0
		p.alwaysTrue = true
		p.alwaysFalse = false
		return true
	}
	if complementEst > countBroadRangeComplementMaxCardinality(leadProbeEst, universe) {
		return false
	}

	var ids posting.List
	appendComplement := func(span overlayRange) {
		if overlayRangeEmpty(span) {
			return
		}
		if out, ok := qv.tryEvalNumericRangeBuckets(p.expr.Field, fm, ov, span); ok {
			out.ids.OrInto(&ids)
			out.ids.Release()
			return
		}
		part := overlayUnionRange(ov, span)
		part.OrInto(&ids)
		part.Release()
	}
	appendComplement(before)
	appendComplement(after)
	nilPosting.OrInto(&ids)
	if ids.IsEmpty() {
		if cacheKey != "" {
			qv.snap.storeMaterializedPred(cacheKey, posting.List{})
		}
		p.kind = predicateKindCustom
		p.iterKind = predicateIterNone
		p.posting = posting.List{}
		p.posts = nil
		p.ids = posting.List{}
		p.contains = nil
		p.iter = nil
		p.bucketCount = nil
		p.estCard = 0
		p.alwaysTrue = true
		p.alwaysFalse = false
		return true
	}
	if trace != nil {
		trace.addCountRangeComplementBuild(ids.Cardinality(), false)
	}

	if cacheKey != "" {
		qv.tryShareMaterializedPred(cacheKey, &ids)
	}
	p.cleanup = chainPredicateCleanup(p.cleanup, func() {
		ids.Release()
	})
	p.kind = predicateKindMaterializedNot
	p.iterKind = predicateIterNone
	p.posting = posting.List{}
	p.posts = nil
	p.ids = ids
	p.contains = nil
	p.iter = nil
	p.bucketCount = nil
	p.estCard = 0
	p.alwaysTrue = false
	p.alwaysFalse = false
	return true
}

func (qv *queryView[K, V]) materializePostingIntersection(posts []posting.List) posting.List {
	if len(posts) == 0 {
		return posting.List{}
	}

	seed := -1
	var seedCard uint64
	for i := range posts {
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
	for i := range posts {
		if i == seed || out.IsEmpty() {
			continue
		}
		out.AndInPlace(posts[i])
	}
	out.Optimize()
	return out
}

func (qv *queryView[K, V]) materializeSetPredicateForCount(p *predicate) bool {
	if p == nil {
		return false
	}

	origKind := p.kind
	var ids posting.List

	switch origKind {
	case predicateKindPostsAny, predicateKindPostsAnyNot:
		ids = qv.materializeProbeUnion(p.posts)
	case predicateKindPostsAll, predicateKindPostsAllNot:
		ids = qv.materializePostingIntersection(p.posts)
	default:
		return false
	}

	isNot := origKind == predicateKindPostsAnyNot || origKind == predicateKindPostsAllNot
	if ids.IsEmpty() {
		p.kind = predicateKindCustom
		p.iterKind = predicateIterNone
		p.posting = posting.List{}
		p.posts = nil
		p.ids = posting.List{}
		p.contains = nil
		p.iter = nil
		p.bucketCount = nil
		p.estCard = 0
		p.alwaysTrue = isNot
		p.alwaysFalse = !isNot
		return true
	}

	p.cleanup = chainPredicateCleanup(p.cleanup, func() {
		ids.Release()
	})
	p.posting = posting.List{}
	p.posts = nil
	p.ids = ids
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
	b, err := qv.evalSimple(raw)
	if err != nil {
		return err
	}

	if b.ids.IsEmpty() {
		b.release()
		p.kind = predicateKindCustom
		p.iterKind = predicateIterNone
		p.posting = posting.List{}
		p.posts = nil
		p.ids = posting.List{}
		p.contains = nil
		p.iter = nil
		p.bucketCount = nil
		p.estCard = 0
		p.alwaysTrue = p.expr.Not
		p.alwaysFalse = !p.expr.Not
		return nil
	}

	ids := b.ids
	p.cleanup = chainPredicateCleanup(p.cleanup, func() {
		ids.Release()
	})

	p.posting = posting.List{}
	p.posts = nil
	p.contains = nil
	p.iter = nil
	p.bucketCount = nil
	p.ids = ids
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

	if shouldMaterializeCountSetPredicate(*p, probeEst, universe) {
		qv.materializeSetPredicateForCount(p)
	}

	if qv.shouldPreferLazyCountPostingFilter(*p, probeEst, universe) {
		return nil
	}

	if (shouldUseCountBroadRangeComplementMaterialization(*p, probeEst, universe) ||
		shouldTryExactNumericCountBroadRangeComplement(*p, probeEst, universe)) &&
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
	preds, ok := qv.buildPredicatesWithMode(leaves, false)
	if !ok {
		return 0, false, nil
	}
	defer releasePredicates(preds)

	universe := qv.snapshotUniverseCardinality()
	if universe == 0 {
		return 0, true, nil
	}

	// Prefer leads that stay cheap under repeated predicate scans; broad
	// HASANY unions are better left to postingResult fallback than to posts_union scans.
	for i := range preds {
		if preds[i].alwaysFalse {
			return 0, true, nil
		}
	}
	leadIdx, leadEst, _ := qv.pickCountLeadPredicate(preds, universe)
	if leadIdx < 0 {
		return 0, false, nil
	}
	if err := qv.prepareCountPredicateWithTrace(&preds[leadIdx], leadEst, universe, trace); err != nil {
		return 0, true, err
	}
	if preds[leadIdx].alwaysFalse {
		return 0, true, nil
	}
	if !preds[leadIdx].hasIter() {
		return 0, false, nil
	}
	if preds[leadIdx].estCard > 0 {
		leadEst = preds[leadIdx].estCard
	}
	leadNeedsCheck := preds[leadIdx].leadIterNeedsContainsCheck()

	// remaining predicates are ordered by expected check cost/selectivity so we fail fast on likely negatives.
	activeBuf := getIntSliceBuf(len(preds))
	active := activeBuf.values
	defer func() {
		activeBuf.values = active
		releaseIntSliceBuf(activeBuf)
	}()
	for i := range preds {
		if i == leadIdx && !leadNeedsCheck {
			continue
		}
		p := preds[i]
		if p.covered || p.alwaysTrue {
			continue
		}
		if p.alwaysFalse {
			return 0, true, nil
		}
		active = append(active, i)
	}
	// Broad leads are only worth predicate scans when the remaining checks are
	// few and cheap. Apply the conservative gate before residual preparation so
	// postingResult fallback does not inherit avoidable setup/materialization cost.
	if universe > 0 && leadEst > universe/2 {
		cheapChecks := true
		for _, pi := range active {
			if preds[pi].checkCost() > 1 {
				cheapChecks = false
				break
			}
		}
		if len(active) > 2 || !cheapChecks {
			return 0, false, nil
		}
	}
	for _, pi := range active {
		if err := qv.prepareCountPredicateWithTrace(&preds[pi], leadEst, universe, trace); err != nil {
			return 0, true, err
		}
	}
	filtered := active[:0]
	for _, pi := range active {
		p := preds[pi]
		if p.covered || p.alwaysTrue {
			continue
		}
		if p.alwaysFalse {
			return 0, true, nil
		}
		if !p.hasContains() {
			return 0, false, nil
		}
		filtered = append(filtered, pi)
	}
	active = filtered
	sortActivePredicates(active, preds)

	// For range/prefix leads on stable base slices, count by buckets first:
	// many buckets can be skipped (or fully counted) via bucketCount hooks.
	if cnt, examined, ok := qv.tryCountByPredicatesLeadBuckets(preds, leadIdx, active); ok {
		if trace != nil {
			trace.setPlan(PlanCountPredicates)
			trace.addExamined(examined)
		}
		return cnt, true, nil
	}
	// Posting-backed leads can count posting-by-posting so exact-capable
	// residual predicates prune whole posting bitmaps before row fallback.
	if cnt, examined, ok := qv.tryCountByPredicatesLeadPostings(preds, leadIdx, active); ok {
		if trace != nil {
			trace.setPlan(PlanCountPredicates)
			trace.addExamined(examined)
		}
		return cnt, true, nil
	}

	it := preds[leadIdx].newIter()
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
			if !preds[pi].matches(idx) {
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

func (qv *queryView[K, V]) tryCountByPredicatesLeadBuckets(preds []predicate, leadIdx int, active []int) (uint64, uint64, bool) {
	if leadIdx < 0 || leadIdx >= len(preds) {
		return 0, 0, false
	}
	lead := preds[leadIdx]
	e := lead.expr
	if e.Not {
		return 0, 0, false
	}
	switch e.Op {
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
	default:
		return 0, 0, false
	}

	fm := qv.fields[e.Field]
	if fm == nil || fm.Slice {
		return 0, 0, false
	}

	ov := qv.fieldOverlay(e.Field)
	if ov.chunked != nil {
		rb, covered, hasBounds, ok := qv.collectOrderRangeBounds(e.Field, len(preds), func(i int) qx.Expr {
			return preds[i].expr
		})
		if !ok || !hasBounds {
			return 0, 0, false
		}
		br := ov.rangeForBounds(rb)
		if overlayRangeEmpty(br) {
			return 0, 0, true
		}
		if br.baseEnd-br.baseStart < 4 {
			return 0, 0, false
		}

		activeBuf := getIntSliceBuf(len(active))
		localActive := activeBuf.values[:0]
		defer func() {
			activeBuf.values = localActive
			releaseIntSliceBuf(activeBuf)
		}()
		for _, pi := range active {
			if pi >= 0 && pi < len(covered) && covered[pi] {
				continue
			}
			localActive = append(localActive, pi)
		}

		exactActiveBuf := getIntSliceBuf(len(localActive))
		exactActive := buildPostingFilterActive(exactActiveBuf.values, localActive, preds)
		defer func() {
			exactActiveBuf.values = exactActive
			releaseIntSliceBuf(exactActiveBuf)
		}()
		extraExactCandidatesBuf := getIntSliceBuf(len(localActive))
		extraExactCandidates := extraExactCandidatesBuf.values[:0]
		extraExactCandidates = append(extraExactCandidates, qv.collectCountLeadResidualExactCandidates(preds, localActive)...)
		defer func() {
			extraExactCandidatesBuf.values = extraExactCandidates
			releaseIntSliceBuf(extraExactCandidatesBuf)
		}()
		var extraExact []countLeadResidualExactFilter
		defer func() {
			releaseCountLeadResidualExactFilters(extraExact)
		}()
		extraExactBuilt := len(extraExactCandidates) == 0
		residualExactBuf := getIntSliceBuf(len(localActive))
		residualAfterExact := residualExactBuf.values[:0]
		defer func() {
			residualExactBuf.values = residualAfterExact
			releaseIntSliceBuf(residualExactBuf)
		}()
		residualBothBuf := getIntSliceBuf(len(localActive))
		residualAfterBoth := residualBothBuf.values[:0]
		defer func() {
			residualBothBuf.values = residualAfterBoth
			releaseIntSliceBuf(residualBothBuf)
		}()
		for _, pi := range localActive {
			if countIndexSliceContains(exactActive, pi) {
				continue
			}
			residualAfterExact = append(residualAfterExact, pi)
			residualAfterBoth = append(residualAfterBoth, pi)
		}

		var cnt uint64
		var examined uint64
		var bucketWork posting.List
		defer bucketWork.Release()
		var extraWork posting.List
		defer extraWork.Release()

		ensureExtraExact := func() {
			if extraExactBuilt {
				return
			}
			extraExactBuilt = true
			extraExact = qv.buildCountLeadResidualExactFiltersByCandidates(preds, extraExactCandidates)
			if len(extraExact) == 0 {
				return
			}
			extraWork.Release()
			residualAfterBoth = residualAfterBoth[:0]
			for _, pi := range residualAfterExact {
				if countLeadResidualExactFiltersContain(extraExact, pi) {
					continue
				}
				residualAfterBoth = append(residualAfterBoth, pi)
			}
		}

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

			if len(localActive) == 0 {
				cnt += ids.Cardinality()
				continue
			}
			if idx, ok := ids.TrySingle(); ok {
				pass := true
				for _, pi := range localActive {
					if !preds[pi].matches(idx) {
						pass = false
						break
					}
				}
				if pass {
					cnt++
				}
				continue
			}

			if len(exactActive) == 0 && len(extraExact) == 0 {
				ids.ForEach(func(idx uint64) bool {
					pass := true
					for _, pi := range localActive {
						if !preds[pi].matches(idx) {
							pass = false
							break
						}
					}
					if pass {
						cnt++
					}
					return true
				})
				continue
			}

			current := ids
			exactApplied := false
			if len(exactActive) > 0 {
				mode, exactIDs, _ := plannerFilterPostingByChecks(preds, exactActive, ids, &bucketWork, true)
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
			if !extraExactBuilt {
				ensureExtraExact()
			}
			if len(extraExact) > 0 {
				filtered, ok := countApplyLeadResidualExactFilters(current, &extraWork, extraExact)
				if ok {
					if filtered.IsEmpty() {
						continue
					}
					current = filtered
					extraApplied = true
				}
			}

			checks := localActive
			if extraApplied {
				checks = residualAfterBoth
			} else if exactApplied {
				checks = residualAfterExact
			}
			if len(checks) == 0 {
				cnt += current.Cardinality()
				continue
			}

			it := current.Iter()
			for it.HasNext() {
				idx := it.Next()
				pass := true
				for _, pi := range checks {
					if !preds[pi].matches(idx) {
						pass = false
						break
					}
				}
				if pass {
					cnt++
				}
			}
			it.Release()
		}

		return cnt, examined, true
	}

	if !ov.hasData() {
		return 0, 0, false
	}
	rb, covered, hasBounds, ok := qv.collectOrderRangeBounds(e.Field, len(preds), func(i int) qx.Expr {
		return preds[i].expr
	})
	if !ok || !hasBounds {
		return 0, 0, false
	}
	br := ov.rangeForBounds(rb)
	start, end := br.baseStart, br.baseEnd
	if start >= end {
		return 0, 0, true
	}
	if end-start < 4 {
		return 0, 0, false
	}

	activeBuf := getIntSliceBuf(len(active))
	localActive := activeBuf.values[:0]
	defer func() {
		activeBuf.values = localActive
		releaseIntSliceBuf(activeBuf)
	}()
	for _, pi := range active {
		if pi >= 0 && pi < len(covered) && covered[pi] {
			continue
		}
		localActive = append(localActive, pi)
	}

	exactActiveBuf := getIntSliceBuf(len(localActive))
	exactActive := buildPostingFilterActive(exactActiveBuf.values, localActive, preds)
	defer func() {
		exactActiveBuf.values = exactActive
		releaseIntSliceBuf(exactActiveBuf)
	}()
	extraExactCandidatesBuf := getIntSliceBuf(len(localActive))
	extraExactCandidates := extraExactCandidatesBuf.values[:0]
	extraExactCandidates = append(extraExactCandidates, qv.collectCountLeadResidualExactCandidates(preds, localActive)...)
	defer func() {
		extraExactCandidatesBuf.values = extraExactCandidates
		releaseIntSliceBuf(extraExactCandidatesBuf)
	}()
	var extraExact []countLeadResidualExactFilter
	defer func() {
		releaseCountLeadResidualExactFilters(extraExact)
	}()
	extraExactBuilt := len(extraExactCandidates) == 0
	residualExactBuf := getIntSliceBuf(len(localActive))
	residualAfterExact := residualExactBuf.values[:0]
	defer func() {
		residualExactBuf.values = residualAfterExact
		releaseIntSliceBuf(residualExactBuf)
	}()
	residualBothBuf := getIntSliceBuf(len(localActive))
	residualAfterBoth := residualBothBuf.values[:0]
	defer func() {
		residualBothBuf.values = residualAfterBoth
		releaseIntSliceBuf(residualBothBuf)
	}()
	for _, pi := range localActive {
		if countIndexSliceContains(exactActive, pi) {
			continue
		}
		residualAfterExact = append(residualAfterExact, pi)
		residualAfterBoth = append(residualAfterBoth, pi)
	}

	var cnt uint64
	var examined uint64
	var bucketWork posting.List
	defer bucketWork.Release()
	var extraWork posting.List
	defer extraWork.Release()

	ensureExtraExact := func() {
		if extraExactBuilt {
			return
		}
		extraExactBuilt = true
		extraExact = qv.buildCountLeadResidualExactFiltersByCandidates(preds, extraExactCandidates)
		if len(extraExact) == 0 {
			return
		}
		extraWork.Release()
		residualAfterBoth = residualAfterBoth[:0]
		for _, pi := range residualAfterExact {
			if countLeadResidualExactFiltersContain(extraExact, pi) {
				continue
			}
			residualAfterBoth = append(residualAfterBoth, pi)
		}
	}

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

		if len(localActive) == 0 {
			cnt += ids.Cardinality()
			continue
		}
		if idx, ok := ids.TrySingle(); ok {
			pass := true
			for _, pi := range localActive {
				if !preds[pi].matches(idx) {
					pass = false
					break
				}
			}
			if pass {
				cnt++
			}
			continue
		}

		if len(exactActive) == 0 && len(extraExact) == 0 {
			ids.ForEach(func(idx uint64) bool {
				pass := true
				for _, pi := range localActive {
					if !preds[pi].matches(idx) {
						pass = false
						break
					}
				}
				if pass {
					cnt++
				}
				return true
			})
			continue
		}

		current := ids
		exactApplied := false
		if len(exactActive) > 0 {
			mode, exactIDs, _ := plannerFilterPostingByChecks(preds, exactActive, ids, &bucketWork, true)
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
		if !extraExactBuilt {
			ensureExtraExact()
		}
		if len(extraExact) > 0 {
			filtered, ok := countApplyLeadResidualExactFilters(current, &extraWork, extraExact)
			if ok {
				if filtered.IsEmpty() {
					continue
				}
				current = filtered
				extraApplied = true
			}
		}

		checks := localActive
		if extraApplied {
			checks = residualAfterBoth
		} else if exactApplied {
			checks = residualAfterExact
		}
		if len(checks) == 0 {
			cnt += current.Cardinality()
			continue
		}

		it := current.Iter()
		for it.HasNext() {
			idx := it.Next()
			pass := true
			for _, pi := range checks {
				if !preds[pi].matches(idx) {
					pass = false
					break
				}
			}
			if pass {
				cnt++
			}
		}
		it.Release()
	}

	return cnt, examined, true
}

func (qv *queryView[K, V]) tryCountByPredicatesLeadPostings(preds []predicate, leadIdx int, active []int) (uint64, uint64, bool) {
	if leadIdx < 0 || leadIdx >= len(preds) {
		return 0, 0, false
	}
	lead := preds[leadIdx]
	if lead.expr.Not {
		return 0, 0, false
	}

	leadPosts, ok := countPredicateLeadPostings(lead)
	if !ok {
		return 0, 0, false
	}

	activeBuf := getIntSliceBuf(len(active))
	localActive := activeBuf.values[:0]
	defer func() {
		activeBuf.values = localActive
		releaseIntSliceBuf(activeBuf)
	}()
	localActive = append(localActive, active...)

	exactActiveBuf := getIntSliceBuf(len(localActive))
	exactActive := buildPostingFilterActive(exactActiveBuf.values, localActive, preds)
	defer func() {
		exactActiveBuf.values = exactActive
		releaseIntSliceBuf(exactActiveBuf)
	}()
	extraExactCandidatesBuf := getIntSliceBuf(len(localActive))
	extraExactCandidates := extraExactCandidatesBuf.values[:0]
	extraExactCandidates = append(extraExactCandidates, qv.collectCountLeadResidualExactCandidates(preds, localActive)...)
	defer func() {
		extraExactCandidatesBuf.values = extraExactCandidates
		releaseIntSliceBuf(extraExactCandidatesBuf)
	}()
	var extraExact []countLeadResidualExactFilter
	defer func() {
		releaseCountLeadResidualExactFilters(extraExact)
	}()
	extraExactBuilt := len(extraExactCandidates) == 0
	residualExactBuf := getIntSliceBuf(len(localActive))
	residualAfterExact := residualExactBuf.values[:0]
	defer func() {
		residualExactBuf.values = residualAfterExact
		releaseIntSliceBuf(residualExactBuf)
	}()
	residualBothBuf := getIntSliceBuf(len(localActive))
	residualAfterBoth := residualBothBuf.values[:0]
	defer func() {
		residualBothBuf.values = residualAfterBoth
		releaseIntSliceBuf(residualBothBuf)
	}()
	for _, pi := range localActive {
		if countIndexSliceContains(exactActive, pi) {
			continue
		}
		residualAfterExact = append(residualAfterExact, pi)
		residualAfterBoth = append(residualAfterBoth, pi)
	}

	var cnt uint64
	var examined uint64
	var bucketWork posting.List
	defer bucketWork.Release()
	var extraWork posting.List
	defer extraWork.Release()

	ensureExtraExact := func() {
		if extraExactBuilt {
			return
		}
		extraExactBuilt = true
		extraExact = qv.buildCountLeadResidualExactFiltersByCandidates(preds, extraExactCandidates)
		if len(extraExact) == 0 {
			return
		}
		extraWork.Release()
		residualAfterBoth = residualAfterBoth[:0]
		for _, pi := range residualAfterExact {
			if countLeadResidualExactFiltersContain(extraExact, pi) {
				continue
			}
			residualAfterBoth = append(residualAfterBoth, pi)
		}
	}

	for _, ids := range leadPosts {
		if ids.IsEmpty() {
			continue
		}
		examined += ids.Cardinality()

		if len(localActive) == 0 {
			cnt += ids.Cardinality()
			continue
		}
		if idx, ok := ids.TrySingle(); ok {
			pass := true
			for _, pi := range localActive {
				if !preds[pi].matches(idx) {
					pass = false
					break
				}
			}
			if pass {
				cnt++
			}
			continue
		}

		if len(exactActive) == 0 && len(extraExact) == 0 {
			ids.ForEach(func(idx uint64) bool {
				pass := true
				for _, pi := range localActive {
					if !preds[pi].matches(idx) {
						pass = false
						break
					}
				}
				if pass {
					cnt++
				}
				return true
			})
			continue
		}

		current := ids
		exactApplied := false
		if len(exactActive) > 0 {
			mode, exactIDs, _ := plannerFilterPostingByChecks(preds, exactActive, ids, &bucketWork, true)
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
		if !extraExactBuilt {
			ensureExtraExact()
		}
		if len(extraExact) > 0 {
			filtered, ok := countApplyLeadResidualExactFilters(current, &extraWork, extraExact)
			if ok {
				if filtered.IsEmpty() {
					continue
				}
				current = filtered
				extraApplied = true
			}
		}

		checks := localActive
		if extraApplied {
			checks = residualAfterBoth
		} else if exactApplied {
			checks = residualAfterExact
		}
		if len(checks) == 0 {
			cnt += current.Cardinality()
			continue
		}

		it := current.Iter()
		for it.HasNext() {
			idx := it.Next()
			pass := true
			for _, pi := range checks {
				if !preds[pi].matches(idx) {
					pass = false
					break
				}
			}
			if pass {
				cnt++
			}
		}
		it.Release()
	}

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
		found := false
		ok := forEachAndLeaf(op, func(e qx.Expr) bool {
			if e.Not {
				return true
			}
			switch e.Op {
			case qx.OpPREFIX, qx.OpSUFFIX, qx.OpCONTAINS:
				found = true
				return true
			default:
				return true
			}
		})
		if !ok {
			return false
		}
		if found {
			return true
		}
	}
	return false
}

func countLeadOpWeight(op qx.Op) uint64 {
	switch op {
	case qx.OpEQ:
		return 1
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
		return 2
	case qx.OpIN:
		return 3
	case qx.OpHAS:
		return 4
	case qx.OpHASANY:
		return 12
	default:
		return 8
	}
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

func countPredicateLeadWeight(p predicate) uint64 {
	switch p.expr.Op {
	case qx.OpEQ:
		return 1
	case qx.OpIN:
		return 2
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
		return 4
	case qx.OpHAS:
		return 5
	case qx.OpHASANY:
		return 12
	default:
		return countLeadOpWeight(p.expr.Op)
	}
}

func countPredicateLeadScanWeight(p predicate) uint64 {
	weight := countPredicateLeadWeight(p)
	if p.kind != predicateKindCustom {
		return weight
	}
	switch p.expr.Op {
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
		return satMulUint64(weight, 3)
	default:
		return weight
	}
}

func (qv *queryView[K, V]) countPredicateCanUseExactNumericComplement(p predicate, probeEst, universe uint64) bool {
	if !shouldTryExactNumericCountBroadRangeComplement(p, probeEst, universe) {
		return false
	}
	fm := qv.fields[p.expr.Field]
	if fm == nil || fm.Slice || !isNumericScalarKind(fm.Kind) {
		return false
	}
	if p.postingFilterCheap {
		return true
	}
	return true
}

func (qv *queryView[K, V]) shouldPreferLazyCountPostingFilter(p predicate, probeEst uint64, universe uint64) bool {
	if qv.materializedPredCacheEnabled() {
		return false
	}
	if p.kind != predicateKindCustom || !p.postingFilterCheap {
		return false
	}
	return shouldUseCountBroadRangeComplementMaterialization(p, probeEst, universe) ||
		qv.countPredicateCanUseExactNumericComplement(p, probeEst, universe)
}

func (qv *queryView[K, V]) prepareCountORPredicateWithTrace(p *predicate, probeEst uint64, universe uint64, trace *queryTrace) error {
	if p == nil || p.alwaysTrue || p.alwaysFalse || p.covered {
		return nil
	}
	if trace != nil {
		trace.addCountPredicatePreparation(1)
	}

	if qv.shouldPreferLazyCountPostingFilter(*p, probeEst, universe) {
		return nil
	}

	if (shouldUseCountBroadRangeComplementMaterialization(*p, probeEst, universe) ||
		shouldTryExactNumericCountBroadRangeComplement(*p, probeEst, universe)) &&
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
	if shouldUseCountLeadResidualHasAnyExactFilter(p) {
		return 1
	}
	if shouldMaterializeCountSetPredicate(p, probeEst, universe) {
		return 1
	}
	if shouldUseCountBroadRangeComplementMaterialization(p, probeEst, universe) ||
		qv.countPredicateCanUseExactNumericComplement(p, probeEst, universe) {
		return 1
	}
	if shouldMaterializeCustomCountPredicate(p, probeEst, universe) {
		switch p.expr.Op {
		case qx.OpPREFIX:
			return 1
		case qx.OpSUFFIX:
			return 2
		case qx.OpCONTAINS:
			return 2
		default:
			return 1
		}
	}

	var weight uint64
	switch p.expr.Op {
	case qx.OpEQ, qx.OpNOOP:
		weight = 1
	case qx.OpIN, qx.OpHAS:
		weight = 2
	case qx.OpHASANY:
		weight = 3
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE:
		weight = 4
	case qx.OpPREFIX:
		weight = 6
	case qx.OpSUFFIX:
		weight = 8
	case qx.OpCONTAINS:
		weight = 10
	default:
		weight = uint64(max(2, p.checkCost()+2))
	}
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
	if p.supportsPostingFilter() {
		return 1
	}
	if shouldUseCountLeadResidualHasAnyExactFilter(p) {
		return 1
	}
	if shouldUseCountBroadRangeComplementMaterialization(p, probeEst, universe) ||
		qv.countPredicateCanUseExactNumericComplement(p, probeEst, universe) {
		return 1
	}

	var weight uint64
	switch p.expr.Op {
	case qx.OpEQ, qx.OpNOOP:
		weight = 1
	case qx.OpIN, qx.OpHAS:
		weight = 2
	case qx.OpHASANY:
		weight = 3
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE:
		weight = 4
	case qx.OpPREFIX:
		weight = 6
	case qx.OpSUFFIX:
		weight = 8
	case qx.OpCONTAINS:
		weight = 10
	default:
		weight = uint64(max(2, p.checkCost()+2))
	}
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

func (qv *queryView[K, V]) pickCountLeadPredicate(preds []predicate, universe uint64) (int, uint64, uint64) {
	leadIdx := -1
	leadEst := uint64(0)
	leadScore := uint64(0)

	for i := range preds {
		p := preds[i]
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
		for j := range preds {
			if j == i {
				continue
			}
			score = satAddUint64(score, qv.countLeadResidualScore(preds[j], p.estCard, universe))
		}

		if leadIdx == -1 || score < leadScore {
			leadIdx = i
			leadEst = p.estCard
			leadScore = score
		}
	}
	return leadIdx, leadEst, leadScore
}

func (qv *queryView[K, V]) pickCountORLeadPredicate(preds []predicate, universe uint64) (int, uint64, uint64) {
	leadIdx := -1
	leadEst := uint64(0)
	leadScore := uint64(0)

	for i := range preds {
		p := preds[i]
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
		for j := range preds {
			if j == i {
				continue
			}
			score = satAddUint64(score, qv.countORLeadResidualScore(preds[j], p.estCard, universe))
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
		branchTrace = make([]TraceORBranch, branchCount)
		for i := range branchTrace {
			branchTrace[i].Index = i
		}
	}

	branchesBuf := getCountORBranchSliceBuf(len(expr.Operands))
	branches := branchesBuf.values
	defer func() {
		releaseCountORBranches(branches)
		branchesBuf.values = branches
		releaseCountORBranchSliceBuf(branchesBuf)
	}()

	var expectedProbes uint64
	materializedBranches := make([]countORMaterializedBranch, 0, max(0, branchCount-1))
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
		preds, ok := qv.buildPredicatesWithMode(leaves, false)
		if !ok {
			return 0, false, nil
		}

		activeBuf := getIntSliceBuf(len(preds))
		active := activeBuf.values
		branchFalse := false
		releaseCurrentBranch := func(checksBuf *intSliceBuf, checks []int) {
			if checksBuf != nil {
				checksBuf.values = checks
				releaseIntSliceBuf(checksBuf)
			}
			activeBuf.values = active
			releaseIntSliceBuf(activeBuf)
			releasePredicates(preds)
		}

		for i := range preds {
			p := preds[i]
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
			activeBuf.values = active
			releaseIntSliceBuf(activeBuf)
			releasePredicates(preds)
			continue
		}

		// Branch is tautology: OR result equals universe.
		if len(active) == 0 {
			activeBuf.values = active
			releaseIntSliceBuf(activeBuf)
			releasePredicates(preds)
			return uc, true, nil
		}

		leadIdx, leadEst, leadScore := qv.pickCountORLeadPredicate(preds, uc)
		branchEst := countORBranchEstimate(preds, active, leadEst)
		leadWeight := uint64(0)
		if leadIdx >= 0 {
			leadWeight = countPredicateLeadScanWeight(preds[leadIdx])
		}
		spillBranch := func(reason string, checksBuf *intSliceBuf, checks []int) {
			if branchTrace != nil {
				branchTrace[branchIdx].Skipped = true
				branchTrace[branchIdx].SkipReason = reason
			}
			materializedBranches = append(materializedBranches, countORMaterializedBranch{
				index: branchIdx,
				expr:  op,
				est:   branchEst,
			})
			materializedBranchEst = satAddUint64(materializedBranchEst, branchEst)
			releaseCurrentBranch(checksBuf, checks)
		}

		// Cannot drive branch without iterable lead.
		if leadIdx < 0 {
			if strictWide {
				spillBranch("materialized_spill_no_lead", nil, nil)
				continue
			}
			releaseCurrentBranch(nil, nil)
			return 0, false, nil
		}
		// Keep OR path setup minimal: avoid eager predicate materialization here.
		// Broad OR branches are sensitive to one-shot setup allocations.
		if countLeadTooRisky(preds[leadIdx].expr.Op, leadEst, uc) {
			if strictWide {
				spillBranch("materialized_spill_risky_lead", nil, nil)
				continue
			}
			releaseCurrentBranch(nil, nil)
			return 0, false, nil
		}
		if strictWide {
			if leadWeight > 3 && leadEst > 2_048 && leadEst > uc/64 {
				spillBranch("materialized_spill_expensive_lead", nil, nil)
				continue branchLoop
			}
			if leadScore > uc && leadEst > 2_048 && leadEst > uc/32 {
				spillBranch("materialized_spill_expensive_branch_score", nil, nil)
				continue branchLoop
			}
			if leadEst > uc/3 {
				spillBranch("materialized_spill_broad_lead", nil, nil)
				continue branchLoop
			}
		}

		checksBuf := getIntSliceBuf(len(active))
		checks := checksBuf.values
		leadNeedsCheck := preds[leadIdx].leadIterNeedsContainsCheck()
		for _, pi := range active {
			if pi == leadIdx && !leadNeedsCheck {
				continue
			}
			p := preds[pi]
			if p.covered || p.alwaysTrue {
				continue
			}
			if p.alwaysFalse {
				branchFalse = true
				break
			}
			if !p.hasContains() {
				if strictWide {
					spillBranch("materialized_spill_no_contains", checksBuf, checks)
					continue branchLoop
				}
				releaseCurrentBranch(checksBuf, checks)
				return 0, false, nil
			}
			checks = append(checks, pi)
		}
		if branchFalse {
			releaseCurrentBranch(checksBuf, checks)
			continue
		}
		for _, pi := range checks {
			if err := qv.prepareCountORPredicateWithTrace(&preds[pi], leadEst, uc, trace); err != nil {
				releaseCurrentBranch(checksBuf, checks)
				return 0, true, err
			}
		}
		filteredChecks := checks[:0]
		for _, pi := range checks {
			p := preds[pi]
			if p.covered || p.alwaysTrue {
				continue
			}
			if p.alwaysFalse {
				branchFalse = true
				break
			}
			if !p.hasContains() {
				if strictWide {
					spillBranch("materialized_spill_post_prepare", checksBuf, checks)
					continue branchLoop
				}
				releaseCurrentBranch(checksBuf, checks)
				return 0, false, nil
			}
			filteredChecks = append(filteredChecks, pi)
		}
		if branchFalse {
			releaseCurrentBranch(checksBuf, checks)
			continue
		}
		checks = filteredChecks
		sortActivePredicates(checks, preds)
		activeBuf.values = active
		releaseIntSliceBuf(activeBuf)

		if leadEst == 0 || leadWeight == 0 {
			leadEst = 1
			leadWeight = 1
		}
		expectedProbes += leadEst * leadWeight
		branches = append(branches, countORBranch{
			index:     branchIdx,
			preds:     preds,
			lead:      leadIdx,
			checks:    checks,
			checksBuf: checksBuf,
			est:       leadEst,
		})
	}

	if len(branches) == 0 {
		if len(materializedBranches) > 0 {
			return 0, false, nil
		}
		return 0, true, nil
	}
	useMaterializedSpill := len(materializedBranches) > 0
	if useMaterializedSpill && !shouldTryCountORHybridMaterializedSpill(branchCount, len(branches), len(materializedBranches), expectedProbes, materializedBranchEst, uc) {
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

	useSeenDedup := useMaterializedSpill || branches.shouldUseSeenDedup(uc, expectedProbes)
	var seen posting.List
	var seenRef *posting.List
	if useSeenDedup {
		seenRef = &seen
		defer seen.Release()
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
		spillCnt, spillExamined, ok, err := qv.countORMaterializedSpillUnion(materializedBranches, seenRef, trace, branchTrace)
		if err != nil {
			return 0, true, err
		}
		if !ok {
			return 0, false, nil
		}
		cnt += spillCnt
		examined += spillExamined
	}
	for i := range branches {
		br := branches[i]
		var brTrace *TraceORBranch
		if branchTrace != nil {
			brTrace = &branchTrace[br.index]
		}
		if cntBranch, examinedBranch, ok := qv.tryCountORBranchLeadPostings(branches, i, useSeenDedup, seenRef, brTrace); ok {
			cnt += cntBranch
			examined += examinedBranch
			continue
		}

		it := br.preds[br.lead].newIter()
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
				if seenRef.Contains(idx) {
					continue
				}
			}

			if !br.checksMatch(idx) {
				continue
			}

			if useSeenDedup {
				if seenRef.CheckedAdd(idx) {
					cnt++
					if brTrace != nil {
						brTrace.RowsEmitted++
					}
				}
				continue
			}

			dup := false
			for j := 0; j < i; j++ {
				if branches[j].matches(idx) {
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
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE:
		cost = 2
	case qx.OpPREFIX:
		cost = 3
	case qx.OpSUFFIX, qx.OpCONTAINS:
		cost = 4
	}
	if e.Not {
		cost++
	}
	return cost
}

func sortCountLeafPlanOrder(plans []countLeafPlan, ids []int) {
	if len(ids) <= 1 {
		return
	}
	less := func(a, b int) bool {
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
	for i := 1; i < len(ids); i++ {
		cur := ids[i]
		j := i
		for j > 0 && less(cur, ids[j-1]) {
			ids[j] = ids[j-1]
			j--
		}
		ids[j] = cur
	}
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

	plans := make([]countLeafPlan, len(leaves))

	posBuf := getIntSliceBuf(len(plans))
	pos := posBuf.values[:0]
	defer func() {
		posBuf.values = pos
		releaseIntSliceBuf(posBuf)
	}()

	negBuf := getIntSliceBuf(len(plans))
	neg := negBuf.values[:0]
	defer func() {
		negBuf.values = neg
		releaseIntSliceBuf(negBuf)
	}()

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

	apply := func(idx int) (bool, error) {
		b, err := qv.evalExpr(plans[idx].expr)
		if err != nil {
			return false, err
		}
		if !b.neg && b.ids.IsEmpty() {
			b.release()
			if hasAcc {
				acc.release()
			}
			return true, nil
		}
		if !hasAcc {
			acc = b
			hasAcc = true
			return false, nil
		}
		acc, err = qv.andPostingResult(acc, b)
		if err != nil {
			acc.release()
			return false, err
		}
		if !acc.neg && acc.ids.IsEmpty() {
			acc.release()
			return true, nil
		}
		return false, nil
	}

	for _, pi := range pos {
		done, err := apply(pi)
		if err != nil {
			return 0, true, err
		}
		if done {
			return 0, true, nil
		}
	}
	for _, ni := range neg {
		done, err := apply(ni)
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
