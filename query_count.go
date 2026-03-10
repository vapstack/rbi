package rbi

import (
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/vapstack/qx"
)

const countPredicateScanMaxLeaves = 16
const countORPredicateMaxBranchesBase = 5
const countORSeenUnionThresholdBase = 64_000
const countORSeenUniverseDiv = 16
const countScalarInSplitMaxValues = 32
const countScalarInSplitMaxOtherLeaves = 3
const countPredSetMaterializeMinTermsBase = 6
const countPredCustomMaterializeMinProbeBase = 4_096

// Count evaluates the expression from the given query and returns the number of matching records.
// It ignores Order, Offset and Limit fields.
// If q is nil, Count returns the total number of keys currently present in the database.
func (db *DB[K, V]) Count(q *qx.QX) (uint64, error) {
	if err := db.beginOp(); err != nil {
		return 0, err
	}
	defer db.endOp()
	view := db.makeQueryView(db.getSnapshot())
	defer db.releaseQueryView(view)
	return view.countInternal(q)
}

func (db *DB[K, V]) countInternal(q *qx.QX) (uint64, error) {
	if q == nil {
		return db.snapshotUniverseCardinality(), nil
	}

	if err := db.checkUsedFields(q); err != nil {
		return 0, err
	}

	expr, _ := normalizeExpr(q.Expr)

	if cnt, ok, err := db.tryCountByUniqueEq(expr); ok || err != nil {
		return cnt, err
	}
	if cnt, ok, err := db.tryCountByScalarInSplit(expr); ok || err != nil {
		return cnt, err
	}
	if cnt, ok, err := db.tryCountByPredicates(expr); ok || err != nil {
		return cnt, err
	}
	if cnt, ok, err := db.tryCountORByPredicates(expr); ok || err != nil {
		return cnt, err
	}

	b, err := db.evalExpr(expr)
	if err != nil {
		return 0, err
	}
	defer b.release()

	return db.countBitmapResult(b), nil
}

// tryCountByScalarInSplit accelerates flat AND counts with a positive scalar IN leaf:
// it evaluates all non-IN leaves once and then sums per-value posting intersections.
func (db *DB[K, V]) tryCountByScalarInSplit(expr qx.Expr) (uint64, bool, error) {
	if expr.Not || expr.Op != qx.OpAND || len(expr.Operands) < 2 {
		return 0, false, nil
	}

	leaves, ok := collectAndLeaves(expr)
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
		fm := db.fields[e.Field]
		if fm == nil || fm.Slice {
			continue
		}
		vals, isSlice, err := db.exprValueToIdxOwned(qx.Expr{Op: qx.OpIN, Field: e.Field, Value: e.Value})
		if err != nil || !isSlice || len(vals) < 2 || len(vals) > countScalarInSplitMaxValues {
			continue
		}
		n := len(dedupStringsInplace(vals))
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
	ov := db.fieldOverlay(inLeaf.Field)
	if !ov.hasData() {
		return 0, false, nil
	}

	vals, isSlice, err := db.exprValueToIdxOwned(qx.Expr{Op: qx.OpIN, Field: inLeaf.Field, Value: inLeaf.Value})
	if err != nil || !isSlice || len(vals) < 2 {
		return 0, false, nil
	}
	vals = dedupStringsInplace(vals)
	if len(vals) < 2 || len(vals) > countScalarInSplitMaxValues {
		return 0, false, nil
	}

	var (
		filter    bitmap
		useFilter bool
	)
	if len(leaves) > 1 {
		others := make([]qx.Expr, 0, len(leaves)-1)
		for i := range leaves {
			if i == lead {
				continue
			}
			others = append(others, leaves[i])
		}
		if len(others) == 1 {
			filter, err = db.evalExpr(others[0])
		} else {
			filter, err = db.evalExpr(qx.Expr{Op: qx.OpAND, Operands: others})
		}
		if err != nil {
			return 0, true, err
		}
		defer filter.release()

		// Keep this path simple and exact only for positive filter sets.
		if filter.neg {
			return 0, false, nil
		}
		if filter.bm == nil || filter.bm.IsEmpty() {
			return 0, true, nil
		}
		useFilter = true
	}

	var keepOwned [countScalarInSplitMaxValues]*roaring64.Bitmap
	ownedCount := 0
	defer func() {
		if ownedCount > 0 {
			releaseOwnedBitmapSlice(keepOwned[:ownedCount])
		}
	}()

	var cnt uint64
	for _, v := range vals {
		ids, keep := overlayLookupPostingRetained(ov, v)
		if ids.IsEmpty() {
			continue
		}
		if keep != nil {
			if ownedCount >= len(keepOwned) {
				return 0, false, nil
			}
			keepOwned[ownedCount] = keep
			ownedCount++
		}
		if useFilter {
			cnt += ids.AndCardinalityBitmap(filter.bm)
			continue
		}
		cnt += ids.Cardinality()
	}

	return cnt, true, nil
}

func (db *DB[K, V]) isPositiveUniqueEqExpr(e qx.Expr) bool {
	if e.Not || e.Op != qx.OpEQ || e.Field == "" {
		return false
	}
	fm := db.fields[e.Field]
	return fm != nil && !fm.Slice && fm.Unique
}

func countLeavesForUniquePath(expr qx.Expr) ([]qx.Expr, bool) {
	if expr.Not {
		return nil, false
	}
	if expr.Op == qx.OpAND {
		leaves, ok := collectAndLeaves(expr)
		if !ok || len(leaves) == 0 {
			return nil, false
		}
		return leaves, true
	}
	if expr.Op == qx.OpOR || expr.Op == qx.OpNOOP || len(expr.Operands) != 0 {
		return nil, false
	}
	return []qx.Expr{expr}, true
}

func (db *DB[K, V]) uniqueCountPathPrecheck(expr qx.Expr) (hasUnique bool, ok bool) {
	if expr.Not {
		return false, false
	}

	if expr.Op == qx.OpAND {
		if len(expr.Operands) == 0 {
			return false, false
		}
		var found bool
		for i := range expr.Operands {
			childHasUnique, childOK := db.uniqueCountPathPrecheck(expr.Operands[i])
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
	return db.isPositiveUniqueEqExpr(expr), true
}

// tryCountByUniqueEq counts expressions anchored by a positive EQ predicate on
// a unique scalar field, which bounds the candidate set to at most one row.
func (db *DB[K, V]) tryCountByUniqueEq(expr qx.Expr) (uint64, bool, error) {
	hasUnique, ok := db.uniqueCountPathPrecheck(expr)
	if !ok || !hasUnique {
		return 0, false, nil
	}

	leaves, ok := countLeavesForUniquePath(expr)
	if !ok {
		return 0, false, nil
	}

	preds, ok := db.buildPredicates(leaves)
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
		if !db.isPositiveUniqueEqExpr(p.expr) {
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

	var cnt uint64
	for it.HasNext() {
		idx := it.Next()
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
	preds     []predicate
	lead      int
	checks    []int
	checksBuf *intSliceBuf
	est       uint64
}

type countORBranches []countORBranch

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

func (br countORBranch) checksMatch(idx uint64) bool {
	for _, pi := range br.checks {
		if !br.preds[pi].matches(idx) {
			return false
		}
	}
	return true
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

func (db *DB[K, V]) materializePostingIntersection(posts []postingList) *roaring64.Bitmap {
	if len(posts) == 0 {
		return getRoaringBuf()
	}

	seed := -1
	var seedCard uint64
	for i := range posts {
		p := posts[i]
		c := p.Cardinality()
		if c == 0 {
			return getRoaringBuf()
		}
		if seed == -1 || c < seedCard {
			seed = i
			seedCard = c
		}
	}

	out := getRoaringBuf()
	posts[seed].OrInto(out)
	for i := range posts {
		if i == seed || out.IsEmpty() {
			continue
		}
		p := posts[i]
		if p.isSingleton() {
			if !out.Contains(p.single) {
				out.Clear()
				break
			}
			if out.GetCardinality() != 1 {
				out.Clear()
				out.Add(p.single)
			}
			continue
		}
		if bm := p.bitmap(); bm != nil {
			out.And(bm)
			continue
		}
		out.Clear()
		break
	}
	return out
}

func (db *DB[K, V]) materializeSetPredicateForCount(p *predicate) bool {
	if p == nil {
		return false
	}

	origKind := p.kind
	var bm *roaring64.Bitmap

	switch origKind {
	case predicateKindPostsAny, predicateKindPostsAnyNot:
		bm = db.materializeProbeUnion(p.posts)
	case predicateKindPostsAll, predicateKindPostsAllNot:
		bm = db.materializePostingIntersection(p.posts)
	default:
		return false
	}

	isNot := origKind == predicateKindPostsAnyNot || origKind == predicateKindPostsAllNot
	if bm == nil || bm.IsEmpty() {
		if bm != nil {
			releaseRoaringBuf(bm)
		}
		p.kind = predicateKindCustom
		p.iterKind = predicateIterNone
		p.posting = postingList{}
		p.posts = nil
		p.bm = nil
		p.contains = nil
		p.iter = nil
		p.bucketCount = nil
		p.estCard = 0
		p.alwaysTrue = isNot
		p.alwaysFalse = !isNot
		return true
	}

	p.cleanup = chainPredicateCleanup(p.cleanup, func() {
		releaseRoaringBuf(bm)
	})
	p.posting = postingList{}
	p.posts = nil
	p.bm = bm
	p.contains = nil
	p.iter = nil
	p.bucketCount = nil
	p.estCard = bm.GetCardinality()
	p.alwaysTrue = false
	p.alwaysFalse = false

	switch origKind {
	case predicateKindPostsAny, predicateKindPostsAll:
		p.kind = predicateKindBitmap
		p.iterKind = predicateIterBitmap
	case predicateKindPostsAnyNot, predicateKindPostsAllNot:
		p.kind = predicateKindBitmapNot
		p.iterKind = predicateIterNone
	}
	return true
}

func (db *DB[K, V]) materializeCustomPredicateForCount(p *predicate) error {
	if p == nil {
		return nil
	}

	raw := p.expr
	raw.Not = false
	b, err := db.evalSimple(raw)
	if err != nil {
		return err
	}

	if b.bm == nil || b.bm.IsEmpty() {
		b.release()
		p.kind = predicateKindCustom
		p.iterKind = predicateIterNone
		p.posting = postingList{}
		p.posts = nil
		p.bm = nil
		p.contains = nil
		p.iter = nil
		p.bucketCount = nil
		p.estCard = 0
		p.alwaysTrue = p.expr.Not
		p.alwaysFalse = !p.expr.Not
		return nil
	}

	bm := b.bm
	if !b.readonly {
		p.cleanup = chainPredicateCleanup(p.cleanup, func() {
			releaseRoaringBuf(bm)
		})
	}

	p.posting = postingList{}
	p.posts = nil
	p.contains = nil
	p.iter = nil
	p.bucketCount = nil
	p.bm = bm
	p.alwaysTrue = false
	p.alwaysFalse = false

	if p.expr.Not {
		p.kind = predicateKindBitmapNot
		p.iterKind = predicateIterNone
		p.estCard = 0
	} else {
		p.kind = predicateKindBitmap
		p.iterKind = predicateIterBitmap
		p.estCard = bm.GetCardinality()
	}
	return nil
}

func (db *DB[K, V]) prepareCountPredicate(p *predicate, probeEst uint64, universe uint64) error {
	if p == nil || p.alwaysTrue || p.alwaysFalse || p.covered {
		return nil
	}

	if shouldMaterializeCountSetPredicate(*p, probeEst, universe) {
		db.materializeSetPredicateForCount(p)
	}

	if shouldMaterializeCustomCountPredicate(*p, probeEst, universe) {
		if err := db.materializeCustomPredicateForCount(p); err != nil {
			return err
		}
	}

	return nil
}

// tryCountByPredicates counts AND expressions by scanning a selective lead
// predicate and validating remaining predicates via contains checks.
//
// It exists to avoid materializing full bitmap intermediates when a lead can
// cheaply prune the candidate space.
func (db *DB[K, V]) tryCountByPredicates(expr qx.Expr) (uint64, bool, error) {
	if expr.Not || expr.Op != qx.OpAND || len(expr.Operands) < 2 {
		return 0, false, nil
	}

	leaves, ok := collectAndLeaves(expr)
	if !ok || !shouldTryCountByPredicates(leaves) {
		return 0, false, nil
	}

	preds, ok := db.buildPredicates(leaves)
	if !ok {
		return 0, false, nil
	}
	defer releasePredicates(preds)

	// select the smallest iterable predicate as lead to minimize total probes
	leadIdx := -1
	leadEst := uint64(0)
	for i := range preds {
		p := preds[i]
		if p.alwaysFalse {
			return 0, true, nil
		}
		if p.alwaysTrue || p.covered || !p.hasIter() || p.estCard == 0 {
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
	universe := db.snapshotUniverseCardinality()
	if err := db.prepareCountPredicate(&preds[leadIdx], leadEst, universe); err != nil {
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
	for _, pi := range active {
		if err := db.prepareCountPredicate(&preds[pi], leadEst, universe); err != nil {
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

	// Keep path conservative for very broad leads unless remaining checks are
	// few and cheap. This avoids scanning a large lead when bitmap fallback is
	// expected to win.
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

	// For range/prefix leads on stable base slices, count by buckets first:
	// many buckets can be skipped (or fully counted) via bucketCount hooks.
	if cnt, ok := db.tryCountByPredicatesLeadBuckets(preds, leadIdx, active); ok {
		return cnt, true, nil
	}

	it := preds[leadIdx].newIter()
	if it == nil {
		return 0, false, nil
	}

	var cnt uint64
	for it.HasNext() {
		idx := it.Next()
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
	return cnt, true, nil
}

func (db *DB[K, V]) tryCountByPredicatesLeadBuckets(preds []predicate, leadIdx int, active []int) (uint64, bool) {
	if leadIdx < 0 || leadIdx >= len(preds) {
		return 0, false
	}
	lead := preds[leadIdx]
	e := lead.expr
	if e.Not {
		return 0, false
	}
	switch e.Op {
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
	default:
		return 0, false
	}

	fm := db.fields[e.Field]
	if fm == nil || fm.Slice {
		return 0, false
	}

	snap := db.getSnapshot()
	if snap == nil || snap.fieldDelta(e.Field) != nil {
		return 0, false
	}

	slice := snap.fieldIndexSlice(e.Field)
	if slice == nil || len(*slice) == 0 {
		return 0, false
	}

	key, isSlice, err := db.exprValueToIdxScalar(qx.Expr{
		Op:    e.Op,
		Field: e.Field,
		Value: e.Value,
	})
	if err != nil || isSlice {
		return 0, false
	}

	s := *slice
	start, end, ok := resolveRange(s, e.Op, key)
	if !ok {
		return 0, false
	}
	if start >= end {
		return 0, true
	}
	if end-start < 4 {
		return 0, false
	}

	singleActive := -1
	if len(active) == 1 {
		singleActive = active[0]
	}

	var cnt uint64
	var scratch *roaring64.Bitmap
	if len(active) > 0 {
		scratch = getRoaringBuf()
		defer releaseRoaringBuf(scratch)
	}
	var bucketWork *roaring64.Bitmap
	if len(active) > 1 {
		bucketWork = getRoaringBuf()
		defer releaseRoaringBuf(bucketWork)
	}

	for i := start; i < end; i++ {
		ids := s[i].IDs
		if ids.IsEmpty() {
			continue
		}

		if len(active) == 0 {
			cnt += ids.Cardinality()
			continue
		}
		if ids.isSingleton() {
			idx := ids.single
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
			continue
		}

		bm, owned := ids.ToBitmapOwned(scratch)
		if bm == nil || bm.IsEmpty() {
			if owned && bm != nil && bm != scratch {
				releaseRoaringBuf(bm)
			}
			continue
		}

		if singleActive >= 0 {
			if bucketCnt, ok := preds[singleActive].countBucket(bm); ok {
				cnt += bucketCnt
				if owned && bm != scratch {
					releaseRoaringBuf(bm)
				}
				continue
			}
		}

		skipBucket := false
		for _, pi := range active {
			if bucketCnt, ok := preds[pi].countBucket(bm); ok && bucketCnt == 0 {
				skipBucket = true
				break
			}
		}
		if skipBucket {
			if owned && bm != scratch {
				releaseRoaringBuf(bm)
			}
			continue
		}

		if bucketWork != nil {
			bucketWork.Clear()
			bucketWork.Or(bm)
			exact := true
			for _, pi := range active {
				if !preds[pi].applyToBitmap(bucketWork) {
					exact = false
					break
				}
				if bucketWork.IsEmpty() {
					break
				}
			}
			if exact {
				cnt += bucketWork.GetCardinality()
				if owned && bm != scratch {
					releaseRoaringBuf(bm)
				}
				continue
			}
		}

		ids.ForEach(func(idx uint64) bool {
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
			return true
		})
		if owned && bm != scratch {
			releaseRoaringBuf(bm)
		}
	}

	return cnt, true
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
	if limit < 3 {
		limit = 3
	}
	if limit > 8 {
		limit = 8
	}
	return limit
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

func forEachAndLeaf(e qx.Expr, fn func(qx.Expr) bool) bool {
	switch e.Op {
	case qx.OpNOOP:
		return false
	case qx.OpAND:
		if len(e.Operands) == 0 {
			return false
		}
		for _, ch := range e.Operands {
			if !forEachAndLeaf(ch, fn) {
				return false
			}
		}
		return true
	default:
		if e.Op == qx.OpOR || len(e.Operands) != 0 {
			return false
		}
		return fn(e)
	}
}

func collectAndLeavesFixed(e qx.Expr, dst []qx.Expr) ([]qx.Expr, bool) {
	dst = dst[:0]
	ok := forEachAndLeaf(e, func(leaf qx.Expr) bool {
		if len(dst) >= cap(dst) {
			return false
		}
		dst = append(dst, leaf)
		return true
	})
	if !ok || len(dst) == 0 {
		return nil, false
	}
	return dst, true
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

// tryCountORByPredicates counts top-level OR expressions via branch lead scans.
//
// The path is intentionally conservative: it is enabled only for bounded OR
// shapes where lead-driven probing is usually cheaper than full bitmap union.
func (db *DB[K, V]) tryCountORByPredicates(expr qx.Expr) (uint64, bool, error) {
	if !shouldTryCountORByPredicates(expr) {
		return 0, false, nil
	}

	uc := db.snapshotUniverseCardinality()
	if uc == 0 {
		return 0, true, nil
	}
	branchCount := len(expr.Operands)
	if branchCount > countORPredicateBranchLimit(uc) {
		return 0, false, nil
	}
	strictWide := branchCount > 3

	branchesBuf := getCountORBranchSliceBuf(len(expr.Operands))
	branches := branchesBuf.values
	defer func() {
		releaseCountORBranches(branches)
		branchesBuf.values = branches
		releaseCountORBranchSliceBuf(branchesBuf)
	}()

	var expectedProbes uint64

	// Build branch plans first and estimate total probe budget before scanning.
	// This prevents expensive partial work when the union is too broad.
	var leafBuf [countPredicateScanMaxLeaves]qx.Expr
	for _, op := range expr.Operands {
		leaves, ok := collectAndLeavesFixed(op, leafBuf[:0])
		if !ok {
			return 0, false, nil
		}

		preds, ok := db.buildPredicates(leaves)
		if !ok {
			return 0, false, nil
		}

		activeBuf := getIntSliceBuf(len(preds))
		active := activeBuf.values
		leadIdx := -1
		leadEst := uint64(0)
		leadWeight := uint64(0)
		leadScore := 0.0
		branchFalse := false

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
			if p.hasIter() && p.estCard > 0 {
				w := countLeadOpWeight(p.expr.Op)
				score := float64(p.estCard) * float64(w)
				if leadIdx == -1 || score < leadScore {
					leadIdx = i
					leadEst = p.estCard
					leadWeight = w
					leadScore = score
				}
			}
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

		// Cannot drive branch without iterable lead.
		if leadIdx < 0 {
			activeBuf.values = active
			releaseIntSliceBuf(activeBuf)
			releasePredicates(preds)
			return 0, false, nil
		}
		// Keep OR path setup minimal: avoid eager predicate materialization here.
		// Broad OR branches are sensitive to one-shot setup allocations.
		if countLeadTooRisky(preds[leadIdx].expr.Op, leadEst, uc) {
			activeBuf.values = active
			releaseIntSliceBuf(activeBuf)
			releasePredicates(preds)
			return 0, false, nil
		}
		if strictWide {
			if leadWeight > 3 && leadEst > 2_048 && leadEst > uc/64 {
				activeBuf.values = active
				releaseIntSliceBuf(activeBuf)
				releasePredicates(preds)
				return 0, false, nil
			}
			if leadEst > uc/3 {
				activeBuf.values = active
				releaseIntSliceBuf(activeBuf)
				releasePredicates(preds)
				return 0, false, nil
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
				checksBuf.values = checks
				releaseIntSliceBuf(checksBuf)
				activeBuf.values = active
				releaseIntSliceBuf(activeBuf)
				releasePredicates(preds)
				return 0, false, nil
			}
			checks = append(checks, pi)
		}
		if branchFalse {
			checksBuf.values = checks
			releaseIntSliceBuf(checksBuf)
			activeBuf.values = active
			releaseIntSliceBuf(activeBuf)
			releasePredicates(preds)
			continue
		}
		sortActivePredicates(checks, preds)
		activeBuf.values = active
		releaseIntSliceBuf(activeBuf)

		if leadEst == 0 || leadWeight == 0 {
			leadEst = 1
			leadWeight = 1
		}
		expectedProbes += leadEst * leadWeight
		branches = append(branches, countORBranch{
			preds:     preds,
			lead:      leadIdx,
			checks:    checks,
			checksBuf: checksBuf,
			est:       leadEst,
		})
	}

	if len(branches) == 0 {
		return 0, true, nil
	}

	// Guardrail: skip this path when projected probe volume approaches a broad
	// union, where bitmap materialization is typically cheaper overall.
	limit := uc * 2
	if strictWide {
		limit = uc
	}
	if expectedProbes > limit {
		return 0, false, nil
	}

	useSeenDedup := branches.shouldUseSeenDedup(uc, expectedProbes)
	var seen *roaring64.Bitmap
	if useSeenDedup {
		seen = getRoaringBuf()
		defer releaseRoaringBuf(seen)
	}

	var cnt uint64
	for i := range branches {
		br := branches[i]
		it := br.preds[br.lead].newIter()
		if it == nil {
			return 0, false, nil
		}

		for it.HasNext() {
			idx := it.Next()

			if useSeenDedup {
				// OR union dedupe first to avoid repeated expensive predicate checks
				// for ids that were already accepted by previous branches.
				if seen.Contains(idx) {
					continue
				}
			}

			if !br.checksMatch(idx) {
				continue
			}

			if useSeenDedup {
				if seen.CheckedAdd(idx) {
					cnt++
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
			}
		}
	}

	return cnt, true, nil
}

func (db *DB[K, V]) countPreparedExpr(expr qx.Expr) (uint64, error) {
	if cnt, ok, err := db.tryCountPreparedAndReordered(expr); ok || err != nil {
		return cnt, err
	}

	b, err := db.evalExpr(expr)
	if err != nil {
		return 0, err
	}
	defer b.release()

	return db.countBitmapResult(b), nil
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

func (db *DB[K, V]) tryCountPreparedAndReordered(expr qx.Expr) (uint64, bool, error) {
	if expr.Not || expr.Op != qx.OpAND || len(expr.Operands) < 2 {
		return 0, false, nil
	}

	leaves, ok := collectAndLeaves(expr)
	if !ok || len(leaves) < 2 || len(leaves) > countPredicateScanMaxLeaves {
		return 0, false, nil
	}

	universe := db.snapshotUniverseCardinality()
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
		if sel, _, _, _, ok := db.estimateLeafOrderCost(e, nil, universe, "", false); ok {
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
		acc    bitmap
		hasAcc bool
	)

	apply := func(idx int) (bool, error) {
		b, err := db.evalExpr(plans[idx].expr)
		if err != nil {
			return false, err
		}
		if !b.neg && (b.bm == nil || b.bm.IsEmpty()) {
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
		acc, err = db.andBitmap(acc, b)
		if err != nil {
			acc.release()
			return false, err
		}
		if !acc.neg && (acc.bm == nil || acc.bm.IsEmpty()) {
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

	cnt := db.countBitmapResult(acc)
	acc.release()
	return cnt, true, nil
}

func (db *DB[K, V]) countBitmapResult(b bitmap) uint64 {
	if b.neg {
		if b.bm == nil {
			return db.snapshotUniverseCardinality()
		}
		ex := b.bm.GetCardinality()
		uc := db.snapshotUniverseCardinality()
		if ex >= uc {
			return 0
		}
		return uc - ex
	}
	if b.bm == nil {
		return 0
	}
	return b.bm.GetCardinality()
}
