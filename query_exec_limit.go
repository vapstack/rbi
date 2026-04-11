package rbi

import (
	"math/bits"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

type leafPred struct {
	kind          leafPredKind
	pred          predicate
	baseCore      orderBasicBaseCore
	hasBaseCore   bool
	posting       posting.List
	estCard       uint64
	postingFilter func(posting.List) (posting.List, bool)
	postsBuf      *pooled.SliceBuf[posting.List]
	postsAnyState *postsAnyFilterState
}

type leafPredKind uint8

const (
	leafPredKindEmpty leafPredKind = iota
	leafPredKindPredicate
	leafPredKindPosting
	leafPredKindPostsConcat
	leafPredKindPostsUnion
	leafPredKindPostsAll
)

const limitQueryFastPathMaxLeaves = 8

func (p leafPred) postCount() int {
	return postingBufLen(p.postsBuf)
}

func (p leafPred) postAt(i int) posting.List {
	return p.postsBuf.Get(i)
}

func (p leafPred) hasIter() bool {
	switch p.kind {
	case leafPredKindEmpty:
		return true
	case leafPredKindPredicate:
		return p.pred.hasIter()
	case leafPredKindPosting, leafPredKindPostsConcat, leafPredKindPostsUnion, leafPredKindPostsAll:
		return true
	default:
		return false
	}
}

func (p leafPred) leadIterNeedsContainsCheck() bool {
	if p.kind == leafPredKindPredicate {
		return p.pred.leadIterNeedsContainsCheck()
	}
	// HAS(list) uses one posting as iterator seed and must still validate
	// remaining terms for each candidate.
	return p.kind == leafPredKindPostsAll && p.postCount() > 1
}

func (p leafPred) iterNew() posting.Iterator {
	switch p.kind {
	case leafPredKindEmpty:
		return emptyIter{}
	case leafPredKindPredicate:
		return p.pred.newIter()
	case leafPredKindPosting:
		return p.posting.Iter()
	case leafPredKindPostsConcat:
		return newPostingConcatBufIter(p.postsBuf)
	case leafPredKindPostsUnion:
		return newPostingUnionBufIter(p.postsBuf)
	case leafPredKindPostsAll:
		return p.posting.Iter()
	default:
		return emptyIter{}
	}
}

func (p leafPred) containsIdx(idx uint64) bool {
	switch p.kind {
	case leafPredKindPredicate:
		return p.pred.matches(idx)
	case leafPredKindPosting:
		return p.posting.Contains(idx)
	case leafPredKindPostsConcat, leafPredKindPostsUnion:
		for i := 0; i < p.postCount(); i++ {
			ids := p.postAt(i)
			if ids.Contains(idx) {
				return true
			}
		}
		return false
	case leafPredKindPostsAll:
		for i := 0; i < p.postCount(); i++ {
			ids := p.postAt(i)
			if !ids.Contains(idx) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func (p leafPred) supportsExactBucketPostingFilter() bool {
	switch p.kind {
	case leafPredKindPredicate:
		return p.pred.supportsExactBucketPostingFilter()
	case leafPredKindPosting, leafPredKindPostsAll:
		return true
	case leafPredKindPostsConcat, leafPredKindPostsUnion:
		return p.postsAnyState != nil || p.postingFilter != nil
	default:
		return false
	}
}

func (p leafPred) supportsPostingApply() bool {
	switch p.kind {
	case leafPredKindPredicate:
		return p.pred.supportsPostingApply()
	case leafPredKindPosting, leafPredKindPostsAll:
		return true
	case leafPredKindPostsConcat, leafPredKindPostsUnion:
		return p.postsAnyState != nil || p.postingFilter != nil
	default:
		return false
	}
}

func (p leafPred) countBucket(bucket posting.List) (uint64, bool) {
	switch p.kind {
	case leafPredKindEmpty:
		return 0, true
	case leafPredKindPredicate:
		return p.pred.countBucket(bucket)
	case leafPredKindPosting:
		return p.posting.AndCardinality(bucket), true
	case leafPredKindPostsConcat, leafPredKindPostsUnion:
		return countBucketPostsAnyBuf(p.postsBuf, bucket)
	case leafPredKindPostsAll:
		return countBucketPostsAllBuf(p.postsBuf, bucket)
	default:
		return 0, false
	}
}

func (p leafPred) applyToPosting(dst posting.List) (posting.List, bool) {
	switch p.kind {
	case leafPredKindEmpty:
		dst.Release()
		return posting.List{}, true

	case leafPredKindPredicate:
		return p.pred.applyToPosting(dst)

	case leafPredKindPosting:
		return dst.BuildAnd(p.posting), true

	case leafPredKindPostsConcat, leafPredKindPostsUnion:
		if p.postsAnyState != nil {
			return p.postsAnyState.apply(dst)
		}
		if p.postingFilter == nil {
			return posting.List{}, false
		}
		next, ok := p.postingFilter(dst)
		if !ok {
			return posting.List{}, false
		}
		return next, true

	case leafPredKindPostsAll:
		if p.postCount() == 0 {
			dst.Release()
			return posting.List{}, true
		}
		for i := 0; i < p.postCount(); i++ {
			ids := p.postAt(i)
			if ids.IsEmpty() {
				dst.Release()
				return posting.List{}, true
			}
			dst = dst.BuildAnd(ids)
			if dst.IsEmpty() {
				return dst, true
			}
		}
		return dst, true
	}
	return posting.List{}, false
}

func leafPredsMatchActive(preds *pooled.SliceBuf[leafPred], checks []int, idx uint64) bool {
	for _, pi := range checks {
		if !preds.Get(pi).containsIdx(idx) {
			return false
		}
	}
	return true
}

func leafPredsEmitCandidate[K ~uint64 | ~string, V any](
	cursor *queryCursor[K, V],
	preds *pooled.SliceBuf[leafPred],
	checks []int,
	trace *queryTrace,
	idx uint64,
	examined *uint64,
) bool {
	*examined = *examined + 1
	if !leafPredsMatchActive(preds, checks, idx) {
		return false
	}
	if trace != nil {
		trace.addMatched(1)
	}
	return cursor.emit(idx)
}

func leafPredsEmitMatchedPosting[K ~uint64 | ~string, V any](
	cursor *queryCursor[K, V],
	ids posting.List,
	card uint64,
	trace *queryTrace,
	examined *uint64,
) bool {
	if ids.IsEmpty() {
		return false
	}
	*examined += card
	if trace != nil {
		trace.addMatched(card)
	}
	if idx, ok := ids.TrySingle(); ok {
		return cursor.emit(idx)
	}
	it := ids.Iter()
	for it.HasNext() {
		if cursor.emit(it.Next()) {
			it.Release()
			return true
		}
	}
	it.Release()
	return false
}

func leafPredsTryBucketPosting[K ~uint64 | ~string, V any](
	cursor *queryCursor[K, V],
	preds *pooled.SliceBuf[leafPred],
	active []int,
	exactActive []int,
	residualActive []int,
	exactOnly bool,
	residualApplyOnly bool,
	ids posting.List,
	exactWork posting.List,
	applyWork posting.List,
	trace *queryTrace,
	examined *uint64,
) (current posting.List, exactApplied bool, handled bool, stop bool, nextExactWork posting.List, nextApplyWork posting.List) {
	nextExactWork = exactWork
	nextApplyWork = applyWork
	if ids.IsEmpty() {
		return posting.List{}, false, true, false, nextExactWork, nextApplyWork
	}
	card := ids.Cardinality()
	if len(active) == 0 {
		return ids, false, true, leafPredsEmitMatchedPosting(cursor, ids, card, trace, examined), nextExactWork, nextApplyWork
	}
	if len(exactActive) == 0 {
		return ids, false, false, false, nextExactWork, nextApplyWork
	}

	allowExact := plannerAllowExactBucketFilter(0, cursor.need, card, exactOnly, len(exactActive))
	mode, exactIDs, updatedExactWork, _ := plannerFilterPostingByLeafChecks(preds, exactActive, ids, exactWork, allowExact)
	nextExactWork = updatedExactWork
	current = ids
	currentCard := card

	switch mode {
	case plannerPredicateBucketEmpty:
		*examined += card
		return posting.List{}, false, true, false, nextExactWork, nextApplyWork
	case plannerPredicateBucketAll:
		if exactOnly {
			return exactIDs, true, true, leafPredsEmitMatchedPosting(cursor, exactIDs, card, trace, examined), nextExactWork, nextApplyWork
		}
		current = exactIDs
		exactApplied = true
	case plannerPredicateBucketExact:
		if trace != nil {
			trace.addPostingExactFilter(1)
		}
		if exactIDs.IsEmpty() {
			*examined += card
			return posting.List{}, true, true, false, nextExactWork, nextApplyWork
		}
		current = exactIDs
		currentCard = exactIDs.Cardinality()
		exactApplied = true
		if exactOnly {
			return exactIDs, true, true, leafPredsEmitMatchedPosting(cursor, exactIDs, currentCard, trace, examined), nextExactWork, nextApplyWork
		}
	default:
		return ids, false, false, false, nextExactWork, nextApplyWork
	}

	if residualApplyOnly {
		allowApply := currentCard > plannerPredicateBucketExactMinCardForChecks(len(residualActive))
		mode, applyIDs, updatedApplyWork, _ := plannerFilterPostingByLeafChecks(preds, residualActive, current, applyWork, allowApply)
		nextApplyWork = updatedApplyWork
		switch mode {
		case plannerPredicateBucketEmpty:
			*examined += currentCard
			return posting.List{}, exactApplied, true, false, nextExactWork, nextApplyWork
		case plannerPredicateBucketAll:
			return current, exactApplied, true, leafPredsEmitMatchedPosting(cursor, current, currentCard, trace, examined), nextExactWork, nextApplyWork
		case plannerPredicateBucketExact:
			return applyIDs, true, true, leafPredsEmitMatchedPosting(cursor, applyIDs, applyIDs.Cardinality(), trace, examined), nextExactWork, nextApplyWork
		}
	}

	if exactApplied {
		return current, true, false, false, nextExactWork, nextApplyWork
	}
	return ids, false, false, false, nextExactWork, nextApplyWork
}

func leafPredsEmitPosting[K ~uint64 | ~string, V any](
	cursor *queryCursor[K, V],
	preds *pooled.SliceBuf[leafPred],
	active []int,
	exactActive []int,
	residualActive []int,
	exactOnly bool,
	residualApplyOnly bool,
	ids posting.List,
	exactWork posting.List,
	applyWork posting.List,
	trace *queryTrace,
	examined *uint64,
) (bool, posting.List, posting.List) {
	if ids.IsEmpty() {
		return false, exactWork, applyWork
	}
	if len(active) == 0 {
		return leafPredsEmitMatchedPosting(cursor, ids, ids.Cardinality(), trace, examined), exactWork, applyWork
	}
	if idx, ok := ids.TrySingle(); ok {
		return leafPredsEmitCandidate(cursor, preds, active, trace, idx, examined), exactWork, applyWork
	}

	iterSrc := ids.Iter()
	exactApplied := false
	if current, applied, handled, stop, nextExactWork, nextApplyWork := leafPredsTryBucketPosting(
		cursor,
		preds,
		active,
		exactActive,
		residualActive,
		exactOnly,
		residualApplyOnly,
		ids,
		exactWork,
		applyWork,
		trace,
		examined,
	); handled {
		iterSrc.Release()
		return stop, nextExactWork, nextApplyWork
	} else if applied && !current.IsEmpty() {
		iterSrc.Release()
		iterSrc = current.Iter()
		exactApplied = true
		exactWork = nextExactWork
		applyWork = nextApplyWork
	} else {
		exactWork = nextExactWork
		applyWork = nextApplyWork
	}

	checks := active
	if exactApplied {
		checks = residualActive
	}
	for iterSrc.HasNext() {
		if leafPredsEmitCandidate(cursor, preds, checks, trace, iterSrc.Next(), examined) {
			iterSrc.Release()
			return true, exactWork, applyWork
		}
	}
	iterSrc.Release()
	return false, exactWork, applyWork
}

func (qv *queryView[K, V]) tryLimitQuery(q *qx.QX, trace *queryTrace) ([]K, bool, PlanName, error) {
	if q.Limit == 0 || q.Offset != 0 {
		return nil, false, "", nil
	}
	if q.Expr.Not {
		return nil, false, "", nil
	}

	var leavesBuf [8]qx.Expr
	leaves, ok := extractAndLeavesScratch(q.Expr, leavesBuf[:0])
	if !ok || len(leaves) == 0 {
		return nil, false, "", nil
	}

	if len(q.Order) == 0 && len(leaves) == 1 && isPositiveScalarPrefixLeaf(leaves[0]) {
		out, used, err := qv.tryQueryPrefixNoOrderWithLimit(q, trace)
		if !used {
			return nil, false, "", err
		}
		return out, true, PlanLimitPrefixNoOrder, err
	}

	if len(q.Order) == 1 && q.Order[0].Type == qx.OrderBasic {
		out, used, err := qv.tryLimitQueryOrderBasic(q, leaves, trace)
		if !used {
			return nil, false, "", err
		}
		plan := PlanLimitOrderBasic
		if hasPrefixBoundForField(leaves, q.Order[0].Field) {
			plan = PlanLimitOrderPrefix
		}
		return out, true, plan, err
	}

	f, bounds, ok, err := qv.extractNoOrderBounds(leaves)
	if err != nil {
		return nil, false, "", err
	}
	if ok {
		out, used, err := qv.tryLimitQueryRangeNoOrderByField(q, f, bounds, leaves, trace)
		if !used {
			return nil, false, "", err
		}
		return out, true, PlanLimitRangeNoOrder, err
	}

	out, used, err := qv.tryLimitQueryNoOrder(q, leaves, trace)
	if !used {
		return nil, false, "", err
	}
	return out, true, PlanLimit, err
}

func (qv *queryView[K, V]) extractNoOrderBounds(leaves []qx.Expr) (string, rangeBounds, bool, error) {
	var (
		f      string
		bounds rangeBounds
		found  bool
	)

	for _, e := range leaves {
		if !isBoundOp(e.Op) {
			continue
		}
		if e.Not || e.Field == "" {
			return "", rangeBounds{}, false, nil
		}
		if !found {
			found = true
			f = e.Field
		} else if e.Field != f {
			return "", rangeBounds{}, false, nil
		}
	}
	if !found {
		return "", rangeBounds{}, false, nil
	}

	bounds.has = true
	for _, e := range leaves {
		if !isBoundOp(e.Op) {
			continue
		}
		bound, isSlice, err := qv.exprValueToNormalizedScalarBound(e)
		if err != nil {
			return "", rangeBounds{}, true, err
		}
		if isSlice {
			return "", rangeBounds{}, false, nil
		}
		applyNormalizedScalarBound(&bounds, bound)
	}

	return f, bounds, true, nil
}

// tryUniqueEqNoOrder executes a direct no-order path for conjunctions that
// contain at least one positive EQ predicate on a unique scalar field.
//
// The goal is to keep EQ(unique) queries on the same fast path regardless of
// whether caller sets Max(1) explicitly.
func (qv *queryView[K, V]) tryUniqueEqNoOrder(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	if q == nil || q.Expr.Not || q.Offset != 0 || len(q.Order) != 0 {
		return nil, false, nil
	}

	var leavesBuf [8]qx.Expr
	leaves, ok := extractAndLeavesScratch(q.Expr, leavesBuf[:0])
	if !ok || len(leaves) == 0 {
		return nil, false, nil
	}

	hasUniqueLead := false
	for _, e := range leaves {
		if qv.isPositiveUniqueEqExpr(e) {
			hasUniqueLead = true
			break
		}
	}
	if !hasUniqueLead {
		return nil, false, nil
	}

	predsBuf := leafPredSlicePool.Get()
	predsBuf.Grow(len(leaves))
	defer leafPredSlicePool.Put(predsBuf)

	uniqueLead := -1
	sawEmpty := false

	for _, e := range leaves {
		lp, ok, err := qv.buildLeafPred(e)
		if err != nil {
			return nil, true, err
		}
		if !ok {
			return nil, false, nil
		}
		predsBuf.Append(lp)
		if lp.kind == leafPredKindEmpty {
			sawEmpty = true
		}

		if !qv.isPositiveUniqueEqExpr(e) {
			continue
		}

		idx := predsBuf.Len() - 1
		if uniqueLead == -1 || predsBuf.Get(idx).estCard < predsBuf.Get(uniqueLead).estCard {
			uniqueLead = idx
		}
	}

	if uniqueLead < 0 {
		return nil, false, nil
	}
	if sawEmpty {
		if trace != nil {
			trace.setPlan(PlanUniqueEq)
			trace.setEarlyStopReason("empty_leaf")
		}
		return nil, true, nil
	}

	lead := predsBuf.Get(uniqueLead)
	iter := lead.iterNew()
	defer iter.Release()

	if trace != nil {
		trace.setPlan(PlanUniqueEq)
	}

	needAll := q.Limit == 0
	out := make([]K, 0, 1)
	cursor := qv.newQueryCursor(out, 0, q.Limit, needAll, 0)

	var examined uint64
	for iter.HasNext() {
		idx := iter.Next()
		examined++

		pass := true
		for i := 0; i < predsBuf.Len(); i++ {
			if i == uniqueLead {
				continue
			}
			if !predsBuf.Get(i).containsIdx(idx) {
				pass = false
				break
			}
		}
		if !pass {
			continue
		}

		if cursor.emit(idx) {
			if trace != nil {
				trace.addExamined(examined)
				trace.setEarlyStopReason("limit_reached")
			}
			return cursor.out, true, nil
		}
	}

	if trace != nil {
		trace.addExamined(examined)
		trace.setEarlyStopReason("input_exhausted")
	}
	return cursor.out, true, nil
}

func hasPrefixBoundForField(leaves []qx.Expr, field string) bool {
	for _, e := range leaves {
		if e.Not {
			continue
		}
		if e.Field == field && e.Op == qx.OpPREFIX {
			return true
		}
	}
	return false
}

func (qv *queryView[K, V]) tryLimitQueryNoOrder(q *qx.QX, leaves []qx.Expr, trace *queryTrace) ([]K, bool, error) {

	predsBuf := leafPredSlicePool.Get()
	predsBuf.Grow(len(leaves))
	defer leafPredSlicePool.Put(predsBuf)

	for _, e := range leaves {
		if isBoundOp(e.Op) {
			return nil, false, nil
		}
		lp, ok, err := qv.buildLeafPred(e)
		if err != nil {
			return nil, true, err
		}
		if !ok {
			return nil, false, nil
		}
		if lp.kind == leafPredKindEmpty {
			// AND with an empty leaf is empty; avoid selecting a non-iterable lead.
			return nil, true, nil
		}
		predsBuf.Append(lp)
	}

	leadIdx := pickLeadIndex(predsBuf)
	if leadIdx < 0 {
		return nil, false, nil
	}
	lead := predsBuf.Get(leadIdx)
	leadNeedsCheck := lead.leadIterNeedsContainsCheck()

	limit := int(q.Limit)
	out := make([]K, 0, limit)
	cursor := qv.newQueryCursor(out, 0, q.Limit, false, 0)

	iter := lead.iterNew()
	defer iter.Release()

	var examined uint64
	for iter.HasNext() {
		idx := iter.Next()
		examined++

		pass := true
		for i := 0; i < predsBuf.Len(); i++ {
			if i == leadIdx && !leadNeedsCheck {
				continue
			}
			if !predsBuf.Get(i).containsIdx(idx) {
				pass = false
				break
			}
		}
		if !pass {
			continue
		}

		if cursor.emit(idx) {
			trace.addExamined(examined)
			trace.setEarlyStopReason("limit_reached")
			return cursor.out, true, nil
		}
	}

	trace.addExamined(examined)
	trace.setEarlyStopReason("input_exhausted")
	return cursor.out, true, nil
}

func (qv *queryView[K, V]) tryLimitQueryOrderBasic(q *qx.QX, leaves []qx.Expr, trace *queryTrace) ([]K, bool, error) {
	order := q.Order[0]
	needWindow, _ := orderWindow(q)

	f := order.Field
	if f == "" {
		return nil, false, nil
	}

	fm := qv.fields[f]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}

	bounds, ok, err := qv.extractBoundsForField(f, leaves)
	if err != nil {
		return nil, true, err
	}
	if !ok {
		return nil, false, nil
	}
	nilTailField := orderNilTailField(fm, f, bounds)
	ov := qv.fieldOverlay(f)
	if !ov.hasData() && nilTailField == "" {
		if !qv.hasIndexedField(f) {
			return nil, false, nil
		}
		return nil, true, nil
	}

	predsBuf, ok, err := qv.buildLeafPredsExcludingBounds(leaves, f, needWindow)
	if err != nil {
		return nil, true, err
	}
	br := ov.rangeForBounds(bounds)
	if overlayRangeEmpty(br) && nilTailField == "" {
		return nil, true, nil
	}
	if ok {
		if predsBuf != nil {
			defer leafPredSlicePool.Put(predsBuf)
		}
		if hasEmptyLeafPred(predsBuf) {
			return nil, true, nil
		}
		universe := qv.snapshotUniverseCardinality()
		if predsBuf != nil {
			for i := 0; i < predsBuf.Len(); i++ {
				pred := predsBuf.Get(i)
				if pred.kind != leafPredKindPredicate || pred.pred.baseRangeState == nil {
					continue
				}
				pred.pred.setExpectedContainsCalls(
					orderedBaseRangeExpectedContainsCalls(pred.pred.baseRangeState, needWindow, universe),
				)
				predsBuf.Set(i, pred)
			}
		}
		execTrace := trace
		var observedTrace queryTrace
		observedStart := uint64(0)
		if execTrace == nil {
			execTrace = &observedTrace
		} else {
			observedStart = execTrace.ev.RowsExamined
		}
		out := qv.scanLimitByOverlayBounds(q, ov, br, order.Desc, predsBuf, nilTailField, execTrace)
		qv.promoteObservedLimitLeafPreds(f, predsBuf, execTrace.ev.RowsExamined-observedStart, q.Limit)
		return out, true, nil
	}

	window := needWindow
	if window <= 0 {
		return nil, false, nil
	}
	var residualLeavesBuf [limitQueryFastPathMaxLeaves]qx.Expr
	residualLeaves := residualLeavesBuf[:0]
	for _, e := range leaves {
		if isBoundOp(e.Op) && !e.Not && e.Field == f {
			continue
		}
		residualLeaves = append(residualLeaves, e)
	}
	if len(residualLeaves) == 0 {
		out, _ := qv.scanOrderLimitNoPredicates(q, ov, br, order.Desc, nilTailField, trace)
		return out, true, nil
	}
	fullPredSet, ok := qv.buildPredicatesOrderedWithMode(residualLeaves, f, false, window, false, true)
	if !ok {
		return nil, false, nil
	}
	defer fullPredSet.Release()
	for i := 0; i < fullPredSet.Len(); i++ {
		if fullPredSet.Get(i).alwaysFalse {
			return nil, true, nil
		}
	}
	out, _ := qv.scanOrderLimitWithPredicatesReader(q, ov, br, order.Desc, fullPredSet, nilTailField, trace)
	return out, true, nil
}

func (qv *queryView[K, V]) tryLimitQueryRangeNoOrderByField(q *qx.QX, field string, bounds rangeBounds, leaves []qx.Expr, trace *queryTrace) ([]K, bool, error) {

	fm := qv.fields[field]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}
	ov := qv.fieldOverlay(field)
	if !ov.hasData() {
		if !qv.hasIndexedField(field) {
			return nil, false, nil
		}
		return nil, true, nil
	}

	predsBuf, ok, err := qv.buildLeafPredsExcludingBounds(leaves, field, 0)
	if err != nil {
		return nil, true, err
	}
	if !ok {
		return nil, false, nil
	}
	if predsBuf != nil {
		defer leafPredSlicePool.Put(predsBuf)
	}
	if hasEmptyLeafPred(predsBuf) {
		return nil, true, nil
	}

	br := ov.rangeForBounds(bounds)
	if overlayRangeEmpty(br) {
		return nil, true, nil
	}
	return qv.scanLimitByOverlayBounds(q, ov, br, false, predsBuf, "", trace), true, nil
}

func (qv *queryView[K, V]) buildLeafPredsExcludingBounds(leaves []qx.Expr, field string, orderedWindow int) (*pooled.SliceBuf[leafPred], bool, error) {
	predsBuf := leafPredSlicePool.Get()
	predsBuf.Grow(len(leaves))
	var mergedRangesBuf *pooled.SliceBuf[orderedMergedScalarRangeField]
	if len(leaves) > 1 {
		mergedRangesBuf = orderedMergedScalarRangeFieldSlicePool.Get()
		mergedRangesBuf.Grow(len(leaves))
		if !qv.collectMergedNumericRangeFields(leaves, mergedRangesBuf) {
			leafPredSlicePool.Put(predsBuf)
			orderedMergedScalarRangeFieldSlicePool.Put(mergedRangesBuf)
			return nil, false, nil
		}
		defer orderedMergedScalarRangeFieldSlicePool.Put(mergedRangesBuf)
	}
	for i, e := range leaves {
		if isBoundOp(e.Op) && !e.Not && e.Field == field {
			continue
		}
		if mergedRangesBuf != nil && qv.isPositiveMergedNumericRangeLeaf(e) {
			idx := findOrderedMergedScalarRangeField(mergedRangesBuf, e.Field)
			if idx >= 0 {
				merged := mergedRangesBuf.Get(idx)
				if merged.count > 1 {
					if merged.first != i {
						continue
					}
					p, ok, err := qv.buildMergedLimitLeafPred(merged.expr, merged.bounds, orderedWindow)
					if err != nil {
						leafPredSlicePool.Put(predsBuf)
						return nil, true, err
					}
					if !ok {
						leafPredSlicePool.Put(predsBuf)
						return nil, false, nil
					}
					qv.attachLeafPredPostingFilter(&p)
					predsBuf.Append(p)
					continue
				}
			}
		}
		p, ok, err := qv.buildLimitLeafPred(e, orderedWindow)
		if err != nil {
			leafPredSlicePool.Put(predsBuf)
			return nil, true, err
		}
		if !ok {
			leafPredSlicePool.Put(predsBuf)
			return nil, false, nil
		}
		qv.attachLeafPredPostingFilter(&p)
		predsBuf.Append(p)
	}
	if predsBuf.Len() == 0 {
		leafPredSlicePool.Put(predsBuf)
		return nil, true, nil
	}
	return predsBuf, true, nil
}

func (qv *queryView[K, V]) scanLimitByOverlayBounds(q *qx.QX, ov fieldOverlay, br overlayRange, desc bool, preds *pooled.SliceBuf[leafPred], nilTailField string, trace *queryTrace) []K {
	limit := int(q.Limit)
	out := make([]K, 0, limit)
	cursor := qv.newQueryCursor(out, 0, q.Limit, false, 0)
	trackScanWidth := len(q.Order) == 1
	var (
		examined  uint64
		scanWidth uint64
	)

	predCount := 0
	if preds != nil {
		predCount = preds.Len()
	}
	var activeBuf [limitQueryFastPathMaxLeaves]int
	active := activeBuf[:0]
	for i := 0; i < predCount; i++ {
		active = append(active, i)
	}

	var exactActiveBuf [limitQueryFastPathMaxLeaves]int
	exactActive := buildExactBucketPostingFilterActiveLeaf(exactActiveBuf[:0], active, preds)
	exactOnly := len(active) > 0 && len(active) == len(exactActive)

	var residualActiveBuf [limitQueryFastPathMaxLeaves]int
	residualActive := plannerResidualChecks(residualActiveBuf[:0], active, exactActive)
	residualApplyOnly := len(residualActive) > 0
	if residualApplyOnly {
		for _, pi := range residualActive {
			if !preds.Get(pi).supportsPostingApply() {
				residualApplyOnly = false
				break
			}
		}
	}

	var exactWork posting.List

	var applyWork posting.List

	keyCur := ov.newCursor(br, desc)
	for {
		_, ids, ok := keyCur.next()
		if !ok {
			break
		}
		if ids.IsEmpty() {
			continue
		}
		if trackScanWidth {
			scanWidth++
		}
		stop, nextExactWork, nextApplyWork := leafPredsEmitPosting(
			&cursor,
			preds,
			active,
			exactActive,
			residualActive,
			exactOnly,
			residualApplyOnly,
			ids,
			exactWork,
			applyWork,
			trace,
			&examined,
		)
		exactWork = nextExactWork
		applyWork = nextApplyWork
		if stop {
			trace.addExamined(examined)
			if trackScanWidth {
				trace.addOrderScanWidth(scanWidth)
			}
			trace.setEarlyStopReason("limit_reached")
			exactWork.Release()
			applyWork.Release()
			return cursor.out
		}
	}

	if nilTailField != "" {
		ids := qv.nilFieldOverlay(nilTailField).lookupPostingRetained(nilIndexEntryKey)
		if !ids.IsEmpty() {
			if trackScanWidth {
				scanWidth++
			}
			stop, nextExactWork, nextApplyWork := leafPredsEmitPosting(
				&cursor,
				preds,
				active,
				exactActive,
				residualActive,
				exactOnly,
				residualApplyOnly,
				ids,
				exactWork,
				applyWork,
				trace,
				&examined,
			)
			exactWork = nextExactWork
			applyWork = nextApplyWork
			if stop {
				trace.addExamined(examined)
				if trackScanWidth {
					trace.addOrderScanWidth(scanWidth)
				}
				trace.setEarlyStopReason("limit_reached")
				exactWork.Release()
				applyWork.Release()
				return cursor.out
			}
		}
	}

	trace.addExamined(examined)
	if trackScanWidth {
		trace.addOrderScanWidth(scanWidth)
	}
	trace.setEarlyStopReason("input_exhausted")
	exactWork.Release()
	applyWork.Release()
	return cursor.out
}

func isBoundOp(op qx.Op) bool {
	switch op {
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
		return true
	default:
		return false
	}
}

func (qv *queryView[K, V]) extractBoundsForField(field string, leaves []qx.Expr) (rangeBounds, bool, error) {
	var b rangeBounds
	found := false

	for _, e := range leaves {
		if !isBoundOp(e.Op) {
			continue
		}
		if e.Not || e.Field == "" {
			return b, false, nil
		}
		if e.Field != field {
			continue
		}

		bound, isSlice, err := qv.exprValueToNormalizedScalarBound(e)
		if err != nil {
			return b, true, err
		}
		if isSlice {
			return b, false, nil
		}
		applyNormalizedScalarBound(&b, bound)
		found = true
	}

	return b, found, nil
}

func applyBoundsToIndexRange(s []index, b rangeBounds) (start, end int) {
	start = 0
	end = len(s)

	if b.empty {
		return 0, 0
	}

	if b.hasPrefix {
		p := b.prefix
		start = lowerBoundIndex(s, p)
		end = prefixRangeEndIndex(s, p, start)
	}

	if b.hasLo {
		lo := 0
		if b.loNumeric {
			lo = lowerBoundIndexKey(s, b.loIndex)
		} else {
			lo = lowerBoundIndex(s, b.loKey)
		}
		if !b.loInc {
			if lo < len(s) &&
				((b.loNumeric && compareIndexKeys(s[lo].Key, b.loIndex) == 0) ||
					(!b.loNumeric && indexKeyEqualsString(s[lo].Key, b.loKey))) {
				lo++
			}
		}
		if lo > start {
			start = lo
		}
	}
	if b.hasHi {
		hi := 0
		if b.hiNumeric {
			if b.hiInc {
				hi = upperBoundIndexKey(s, b.hiIndex)
			} else {
				hi = lowerBoundIndexKey(s, b.hiIndex)
			}
		} else {
			if b.hiInc {
				hi = upperBoundIndex(s, b.hiKey)
			} else {
				hi = lowerBoundIndex(s, b.hiKey)
			}
		}
		if hi < end {
			end = hi
		}
	}

	if start < 0 {
		start = 0
	}
	if end > len(s) {
		end = len(s)
	}
	if start >= end {
		return 0, 0
	}
	return start, end
}

func (qv *queryView[K, V]) buildLeafPred(e qx.Expr) (leafPred, bool, error) {
	if e.Not || e.Field == "" {
		return leafPred{}, false, nil
	}

	ov := qv.fieldOverlay(e.Field)
	if !ov.hasData() && !qv.hasIndexedField(e.Field) {
		return leafPred{}, false, nil
	}

	fm := qv.fields[e.Field]
	if fm == nil {
		return leafPred{}, false, nil
	}

	switch e.Op {

	case qx.OpEQ:
		if fm.Slice {
			return leafPred{}, false, nil
		}

		key, isSlice, isNil, err := qv.exprValueToIdxScalar(e)
		if err != nil {
			return leafPred{}, true, err
		}
		if isSlice {
			return leafPred{}, false, nil
		}
		if isNil {
			ids := qv.nilFieldOverlay(e.Field).lookupPostingRetained(nilIndexEntryKey)
			if ids.IsEmpty() {
				return emptyLeaf(), true, nil
			}

			return leafPred{
				kind:    leafPredKindPosting,
				posting: ids,
				estCard: ids.Cardinality(),
			}, true, nil
		}

		ids := ov.lookupPostingRetained(key)
		if ids.IsEmpty() {
			return emptyLeaf(), true, nil
		}

		return leafPred{
			kind:    leafPredKindPosting,
			posting: ids,
			estCard: ids.Cardinality(),
		}, true, nil

	case qx.OpIN:
		if fm.Slice {
			return leafPred{}, false, nil
		}

		keysBuf, isSlice, hasNil, err := qv.exprValueToDistinctIdxBuf(e)
		if err != nil {
			return leafPred{}, true, err
		}
		if keysBuf != nil {
			defer stringSlicePool.Put(keysBuf)
		}
		keyCount := 0
		if keysBuf != nil {
			keyCount = keysBuf.Len()
		}
		if !isSlice || (keyCount == 0 && !hasNil) {
			return leafPred{}, false, nil
		}

		postsBuf, est := qv.scalarLookupPostings(e.Field, keysBuf, hasNil)
		if postsBuf.Len() == 0 {
			postingSlicePool.Put(postsBuf)
			return emptyLeaf(), true, nil
		}
		if postsBuf.Len() == 1 {
			ids := postsBuf.Get(0)
			return leafPred{
				kind:     leafPredKindPosting,
				posting:  ids,
				estCard:  ids.Cardinality(),
				postsBuf: postsBuf,
			}, true, nil
		}

		return leafPred{
			kind:     leafPredKindPostsConcat,
			estCard:  est,
			postsBuf: postsBuf,
		}, true, nil

	case qx.OpHAS:
		if !fm.Slice {
			return leafPred{}, false, nil
		}

		keysBuf, isSlice, _, err := qv.exprValueToDistinctIdxBuf(e)
		if err != nil {
			return leafPred{}, true, err
		}
		if keysBuf != nil {
			defer stringSlicePool.Put(keysBuf)
		}
		keyCount := 0
		if keysBuf != nil {
			keyCount = keysBuf.Len()
		}
		if !isSlice || keyCount == 0 {
			return leafPred{}, false, nil
		}

		postsBuf := postingSlicePool.Get()
		postsBuf.Grow(keyCount)

		var est uint64
		for i := 0; i < keyCount; i++ {
			ids := ov.lookupPostingRetained(keysBuf.Get(i))
			if ids.IsEmpty() {
				postingSlicePool.Put(postsBuf)
				return emptyLeaf(), true, nil
			}
			postsBuf.Append(ids)
			c := ids.Cardinality()
			if est == 0 || c < est {
				est = c
			}
		}

		lead := minCardPostingBuf(postsBuf)
		return leafPred{
			kind:     leafPredKindPostsAll,
			posting:  lead,
			estCard:  est,
			postsBuf: postsBuf,
		}, true, nil

	case qx.OpHASANY:
		if !fm.Slice {
			return leafPred{}, false, nil
		}

		keysBuf, isSlice, _, err := qv.exprValueToDistinctIdxBuf(e)
		if err != nil {
			return leafPred{}, true, err
		}
		if keysBuf != nil {
			defer stringSlicePool.Put(keysBuf)
		}
		if !isSlice || keysBuf == nil || keysBuf.Len() == 0 {
			return leafPred{}, false, nil
		}

		postsBuf, est := ov.lookupPostings(keysBuf)
		if postsBuf.Len() == 0 {
			postingSlicePool.Put(postsBuf)
			return emptyLeaf(), true, nil
		}
		if postsBuf.Len() == 1 {
			ids := postsBuf.Get(0)
			return leafPred{
				kind:     leafPredKindPosting,
				posting:  ids,
				estCard:  ids.Cardinality(),
				postsBuf: postsBuf,
			}, true, nil
		}

		return leafPred{
			kind:     leafPredKindPostsUnion,
			estCard:  est,
			postsBuf: postsBuf,
		}, true, nil

	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
		p, ok := qv.buildPredicateWithMode(e, false)
		if !ok {
			return leafPred{}, false, nil
		}
		if p.alwaysFalse {
			releasePredicateOwnedState(&p)
			return emptyLeaf(), true, nil
		}
		return leafPred{
			kind:    leafPredKindPredicate,
			pred:    p,
			estCard: p.estCard,
		}, true, nil
	}

	return leafPred{}, false, nil
}

func (qv *queryView[K, V]) buildLimitLeafPred(e qx.Expr, orderedWindow int) (leafPred, bool, error) {
	if orderedWindow > 0 && isSimpleScalarRangeOrPrefixLeaf(e) && !e.Not {
		if candidate, ok := qv.prepareScalarRangeRoutingCandidate(e); ok &&
			candidate.plan.orderedEagerMaterializeUseful(orderedWindow, qv.snapshotUniverseCardinality()) {
			p, ok := qv.buildPredicateWithMode(e, true)
			if ok {
				if p.alwaysFalse {
					releasePredicateOwnedState(&p)
					return emptyLeaf(), true, nil
				}
				return leafPred{
					kind:        leafPredKindPredicate,
					pred:        p,
					hasBaseCore: true,
					baseCore: orderBasicBaseCore{
						kind: orderBasicBaseCoreRawExpr,
						expr: e,
					},
					estCard: p.estCard,
				}, true, nil
			}
		}
	}
	return qv.buildLeafPred(e)
}

func (qv *queryView[K, V]) buildMergedLimitLeafPred(e qx.Expr, bounds rangeBounds, orderedWindow int) (leafPred, bool, error) {
	fm := qv.fields[e.Field]
	if fm == nil || fm.Slice {
		return leafPred{}, false, nil
	}
	allowMaterialize := false
	if orderedWindow > 0 {
		var core preparedScalarRangePredicate[K, V]
		qv.initPreparedExactScalarRangePredicate(&core, e, fm, bounds)
		allowMaterialize = core.orderedEagerMaterializeUseful(orderedWindow)
	}
	p, ok := qv.buildMergedNumericRangePredicate(e, bounds, allowMaterialize)
	if !ok {
		return leafPred{}, false, nil
	}
	if p.alwaysFalse {
		releasePredicateOwnedState(&p)
		return emptyLeaf(), true, nil
	}
	return leafPred{
		kind:        leafPredKindPredicate,
		pred:        p,
		hasBaseCore: true,
		baseCore: orderBasicBaseCore{
			kind: orderBasicBaseCoreCollapsedRange,
			collapsed: preparedScalarExactRange{
				field:    e.Field,
				bounds:   bounds,
				cacheKey: qv.materializedPredKeyForExactScalarRange(e.Field, bounds),
			},
		},
		estCard: p.estCard,
	}, true, nil
}

func (qv *queryView[K, V]) supportsLimitLeafPredExpr(e qx.Expr) bool {
	if e.Not || e.Field == "" {
		return false
	}
	fm := qv.fields[e.Field]
	if fm == nil {
		return false
	}
	if !qv.fieldOverlay(e.Field).hasData() && !qv.hasIndexedField(e.Field) {
		return false
	}
	switch e.Op {
	case qx.OpEQ:
		return !fm.Slice
	case qx.OpIN:
		return !fm.Slice
	case qx.OpHAS, qx.OpHASANY:
		return fm.Slice
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
		return true
	default:
		return false
	}
}

func (qv *queryView[K, V]) supportsLimitLeafPredsExcludingBounds(leaves []qx.Expr, field string) bool {
	hasResidual := false
	for _, e := range leaves {
		if isBoundOp(e.Op) && !e.Not && e.Field == field {
			continue
		}
		hasResidual = true
		if !qv.supportsLimitLeafPredExpr(e) {
			return false
		}
	}
	return hasResidual
}

func (qv *queryView[K, V]) hasWarmScalarLimitLeafPredsExcludingBounds(leaves []qx.Expr, field string) bool {
	for _, e := range leaves {
		if isBoundOp(e.Op) && !e.Not && e.Field == field {
			continue
		}
		if !isSimpleScalarRangeOrPrefixLeaf(e) {
			continue
		}
		candidate, ok := qv.prepareScalarRangeRoutingCandidate(e)
		if !ok {
			continue
		}
		if hit, ok := candidate.core.loadWarmScalarPostingResult(); ok {
			hit.release()
			return true
		}
	}
	return false
}

func (qv *queryView[K, V]) attachLeafPredPostingFilter(p *leafPred) {
	if p == nil || p.postCount() <= 1 {
		return
	}
	switch p.kind {
	case leafPredKindPostsConcat, leafPredKindPostsUnion:
	default:
		return
	}
	p.postsAnyState = postsAnyFilterStatePool.Get()
	p.postsAnyState.postsBuf = p.postsBuf
	p.postsAnyState.containsMaterializeAt = postsAnyContainsMaterializeAfterBuf(p.postsBuf)
}

func buildExactBucketPostingFilterActiveLeaf(dst, active []int, preds *pooled.SliceBuf[leafPred]) []int {
	dst = dst[:0]
	if preds == nil {
		return dst
	}
	for _, pi := range active {
		if preds.Get(pi).supportsExactBucketPostingFilter() {
			dst = append(dst, pi)
		}
	}
	return dst
}

func plannerFilterCompactPostingByLeafChecks(
	preds *pooled.SliceBuf[leafPred],
	checks []int,
	src posting.List,
	work posting.List,
	card uint64,
) (plannerPredicateBucketMode, posting.List, posting.List, bool) {
	if preds == nil || card == 0 || card > posting.MidCap {
		return 0, posting.List{}, work, false
	}
	if idx, ok := src.TrySingle(); ok {
		for _, pi := range checks {
			if !preds.Get(pi).containsIdx(idx) {
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
			if !preds.Get(pi).containsIdx(idx) {
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

func plannerFilterPostingByLeafChecks(
	preds *pooled.SliceBuf[leafPred],
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

	if mode, exact, nextWork, ok := plannerFilterCompactPostingByLeafChecks(preds, checks, src, work, card); ok {
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

func pickLeadIndex(ps *pooled.SliceBuf[leafPred]) int {
	if ps == nil || ps.Len() == 0 {
		return -1
	}
	best := -1
	for i := 0; i < ps.Len(); i++ {
		pred := ps.Get(i)
		if !pred.hasIter() {
			continue
		}
		if best < 0 || pred.estCard < ps.Get(best).estCard {
			best = i
		}
	}
	return best
}

func hasEmptyLeafPred(preds *pooled.SliceBuf[leafPred]) bool {
	if preds == nil {
		return false
	}
	for i := 0; i < preds.Len(); i++ {
		if preds.Get(i).kind == leafPredKindEmpty {
			return true
		}
	}
	return false
}

func minCardPosting(posts []posting.List) posting.List {
	if len(posts) == 0 {
		return posting.List{}
	}
	best := posts[0]
	bestC := best.Cardinality()
	for i := 1; i < len(posts); i++ {
		if c := posts[i].Cardinality(); c < bestC {
			best = posts[i]
			bestC = c
		}
	}
	return best
}

func minCardPostingBuf(posts *pooled.SliceBuf[posting.List]) posting.List {
	if posts == nil || posts.Len() == 0 {
		return posting.List{}
	}
	best := posts.Get(0)
	bestC := best.Cardinality()
	for i := 1; i < posts.Len(); i++ {
		if c := posts.Get(i).Cardinality(); c < bestC {
			best = posts.Get(i)
			bestC = c
		}
	}
	return best
}

func emptyLeaf() leafPred {
	return leafPred{
		kind:    leafPredKindEmpty,
		estCard: 0,
	}
}

type emptyIter struct{}

func (emptyIter) HasNext() bool { return false }
func (emptyIter) Next() uint64  { return 0 }
func (emptyIter) Release()      {}

type postingConcatIter struct {
	posts    []posting.List
	postsBuf *pooled.SliceBuf[posting.List]
	i        int
	curIt    posting.Iterator
}

func newPostingConcatIter(posts []posting.List) posting.Iterator {
	return &postingConcatIter{posts: posts}
}

func newPostingConcatBufIter(posts *pooled.SliceBuf[posting.List]) posting.Iterator {
	return &postingConcatIter{postsBuf: posts}
}

func (it *postingConcatIter) postCount() int {
	if it.postsBuf != nil {
		return it.postsBuf.Len()
	}
	return len(it.posts)
}

func (it *postingConcatIter) postAt(i int) posting.List {
	if it.postsBuf != nil {
		return it.postsBuf.Get(i)
	}
	return it.posts[i]
}

func (it *postingConcatIter) HasNext() bool {
	for {
		if it.curIt != nil && it.curIt.HasNext() {
			return true
		}
		if it.curIt != nil {
			it.curIt.Release()
			it.curIt = nil
		}
		if it.i >= it.postCount() {
			return false
		}
		p := it.postAt(it.i)
		it.i++
		if p.IsEmpty() {
			continue
		}
		it.curIt = p.Iter()
	}
}

func (it *postingConcatIter) Next() uint64 {
	if !it.HasNext() {
		return 0
	}
	return it.curIt.Next()
}

func (it *postingConcatIter) Release() {
	if it.curIt != nil {
		it.curIt.Release()
		it.curIt = nil
	}
	it.posts = nil
	it.postsBuf = nil
	it.i = 0
}

type postingUnionIter struct {
	posts    []posting.List
	postsBuf *pooled.SliceBuf[posting.List]
	i        int
	curIt    posting.Iterator
	seen     u64set
	next     uint64
	has      bool
}

type postingSmallUnionIter struct {
	posts    []posting.List
	postsBuf *pooled.SliceBuf[posting.List]
	i        int
	curIt    posting.Iterator
	next     uint64
	has      bool
}

type u64setPoolBuf struct {
	keys []uint64
	used []byte
}

var u64setPools = initU64SetPools()

var postingSmallUnionIterPool = pooled.Pointers[postingSmallUnionIter]{
	Cleanup: func(it *postingSmallUnionIter) {
		if it.curIt != nil {
			it.curIt.Release()
		}
	},
	Clear: true,
}

var postingUnionIterPool = pooled.Pointers[postingUnionIter]{
	Clear: true,
}

func initU64SetPools() []pooled.Pointers[u64setPoolBuf] {
	pools := make([]pooled.Pointers[u64setPoolBuf], u64setPoolClassIndex(u64SetPoolMaxCap)+1)
	for i := range pools {
		size := 1 << i
		pools[i] = pooled.Pointers[u64setPoolBuf]{
			New: func() *u64setPoolBuf {
				return &u64setPoolBuf{
					keys: make([]uint64, size),
					used: make([]byte, size),
				}
			},
		}
	}
	return pools
}

func u64setPoolClassIndex(size int) int {
	return bits.Len(uint(size)) - 1
}

func u64setRequiredSize(capHint int) int {
	size := 1
	for size < capHint*2 {
		size <<= 1
	}
	return size
}

func newPostingUnionIter(posts []posting.List) posting.Iterator {
	if len(posts) > 1 && len(posts) <= 3 {
		it := postingSmallUnionIterPool.Get()
		it.posts = posts
		return it
	}

	/*
		capHint := len(posts) * 16
		if capHint < 64 {
			capHint = 64
		}
		if capHint > 1024 {
			capHint = 1024
		}
	*/
	capHint := min(max(len(posts)*16, 64), 1024)

	it := postingUnionIterPool.Get()
	it.posts = posts
	it.seen = newU64Set(capHint)
	return it
}

func newPostingUnionBufIter(posts *pooled.SliceBuf[posting.List]) posting.Iterator {
	if posts.Len() > 1 && posts.Len() <= 3 {
		it := postingSmallUnionIterPool.Get()
		it.postsBuf = posts
		return it
	}
	capHint := min(max(posts.Len()*16, 64), 1024)
	it := postingUnionIterPool.Get()
	it.postsBuf = posts
	it.seen = newU64Set(capHint)
	return it
}

func (u *postingSmallUnionIter) postCount() int {
	if u.postsBuf != nil {
		return u.postsBuf.Len()
	}
	return len(u.posts)
}

func (u *postingSmallUnionIter) postAt(i int) posting.List {
	if u.postsBuf != nil {
		return u.postsBuf.Get(i)
	}
	return u.posts[i]
}

func (u *postingUnionIter) postCount() int {
	if u.postsBuf != nil {
		return u.postsBuf.Len()
	}
	return len(u.posts)
}

func (u *postingUnionIter) postAt(i int) posting.List {
	if u.postsBuf != nil {
		return u.postsBuf.Get(i)
	}
	return u.posts[i]
}

func (u *postingSmallUnionIter) HasNext() bool {
	if u.has {
		return true
	}
	for {
		if u.curIt != nil {
			for u.curIt.HasNext() {
				v := u.curIt.Next()
				dup := false
				for j := 0; j < u.i-1; j++ {
					if u.postAt(j).Contains(v) {
						dup = true
						break
					}
				}
				if dup {
					continue
				}
				u.next = v
				u.has = true
				return true
			}
			u.curIt.Release()
			u.curIt = nil
		}
		if u.i >= u.postCount() {
			return false
		}
		p := u.postAt(u.i)
		u.i++
		if p.IsEmpty() {
			continue
		}
		u.curIt = p.Iter()
	}
}

func (u *postingSmallUnionIter) Next() uint64 {
	if !u.HasNext() {
		return 0
	}
	u.has = false
	return u.next
}

func (u *postingSmallUnionIter) Release() {
	postingSmallUnionIterPool.Put(u)
}

func (u *postingUnionIter) HasNext() bool {
	if u.has {
		return true
	}
	for {
		if u.curIt != nil {
			for u.curIt.HasNext() {
				v := u.curIt.Next()
				if u.seen.Add(v) {
					u.next = v
					u.has = true
					return true
				}
			}
			u.curIt.Release()
			u.curIt = nil
		}
		if u.i >= u.postCount() {
			return false
		}
		p := u.postAt(u.i)
		u.i++
		if p.IsEmpty() {
			continue
		}
		u.curIt = p.Iter()
	}
}

func (u *postingUnionIter) Next() uint64 {
	if !u.HasNext() {
		return 0
	}
	u.has = false
	return u.next
}

func (u *postingUnionIter) Release() {
	if u.curIt != nil {
		u.curIt.Release()
		u.curIt = nil
	}
	u.posts = nil
	u.postsBuf = nil
	releaseU64Set(&u.seen)
	postingUnionIterPool.Put(u)
}

type u64set struct {
	keys   []uint64
	used   []byte
	mask   uint64
	n      int
	pooled *u64setPoolBuf
}

func newU64Set(capHint int) u64set {
	size := u64setRequiredSize(capHint)
	if size <= u64SetPoolMaxCap {
		buf := u64setPools[u64setPoolClassIndex(size)].Get()
		return u64set{
			keys:   buf.keys[:size],
			used:   buf.used[:size],
			mask:   uint64(size - 1),
			pooled: buf,
		}
	}
	return u64set{
		keys: make([]uint64, size),
		used: make([]byte, size),
		mask: uint64(size - 1),
	}
}

func releaseU64Set(s *u64set) {
	if s == nil {
		return
	}
	buf := s.pooled
	size := cap(s.keys)
	if buf != nil && size > 0 && size == cap(s.used) && size <= u64SetPoolMaxCap {
		clear(s.used[:size])
		u64setPools[u64setPoolClassIndex(size)].Put(buf)
	}
	*s = u64set{}
}

func (s *u64set) Add(x uint64) bool {
	if s.n*2 >= len(s.keys) {
		s.grow()
	}
	i := mix64(x) & s.mask
	for {
		if s.used[i] == 0 {
			s.used[i] = 1
			s.keys[i] = x
			s.n++
			return true
		}
		if s.keys[i] == x {
			return false
		}
		i = (i + 1) & s.mask
	}
}

func (s *u64set) Has(x uint64) bool {
	if len(s.keys) == 0 {
		return false
	}
	i := mix64(x) & s.mask
	for {
		if s.used[i] == 0 {
			return false
		}
		if s.keys[i] == x {
			return true
		}
		i = (i + 1) & s.mask
	}
}

func (s *u64set) Len() int {
	return s.n
}

func (s *u64set) grow() {
	old := u64set{
		keys:   s.keys,
		used:   s.used,
		mask:   s.mask,
		n:      s.n,
		pooled: s.pooled,
	}
	next := newU64Set(len(old.keys))
	s.keys = next.keys
	s.used = next.used
	s.mask = next.mask
	s.n = 0
	s.pooled = next.pooled

	for i := 0; i < len(old.keys); i++ {
		if old.used[i] != 0 {
			_ = s.Add(old.keys[i])
		}
	}
	releaseU64Set(&old)
}

func mix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}
