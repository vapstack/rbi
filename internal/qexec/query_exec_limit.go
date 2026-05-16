package qexec

import (
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qir"
)

type leafPred struct {
	kind          leafPredKind
	pred          predicate
	baseCore      orderBasicBaseCore
	hasBaseCore   bool
	posting       posting.List
	estCard       uint64
	postingFilter func(posting.List) (posting.List, bool)
	postsBuf      []posting.List
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
	return len(p.postsBuf)
}

func (p leafPred) postAt(i int) posting.List {
	return p.postsBuf[i]
}

func (p *leafPred) setExpectedContainsCalls(expected int) {
	if p == nil {
		return
	}
	if p.kind == leafPredKindPredicate {
		p.pred.setExpectedContainsCalls(expected)
	}
	if p.postsAnyState != nil {
		p.postsAnyState.setExpectedContainsCalls(expected)
	}
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
		return newPostingUnionIter(p.postsBuf)
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
		if p.postsAnyState != nil &&
			(!p.postsAnyState.ids.IsEmpty() || p.postsAnyState.containsMaterializeAt == 1) {
			return p.postsAnyState.matches(idx)
		}
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
		return p.pred.postingFilterCapability().supportsExactBucket()

	case leafPredKindPosting, leafPredKindPostsAll:
		return true

	case leafPredKindPostsConcat, leafPredKindPostsUnion:
		return p.postsAnyState != nil || p.postingFilter != nil

	default:
		return false
	}
}

func (p leafPred) prefersExactBucketPostingFilter() bool {
	switch p.kind {

	case leafPredKindPredicate:
		return p.pred.postingFilterCapability().prefersExactBucket()

	case leafPredKindPostsConcat, leafPredKindPostsUnion:
		return p.postsAnyState != nil || p.postingFilter != nil

	default:
		return false
	}
}

func (p leafPred) supportsPostingApply() bool {
	switch p.kind {
	case leafPredKindPredicate:
		return p.pred.postingFilterCapability().supportsApply()

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
		if p.postsAnyState != nil {
			return p.postsAnyState.countBucket(bucket)
		}
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

func leafPredsMatchActive(preds []leafPred, checks []int, idx uint64) bool {
	for _, pi := range checks {
		if !preds[pi].containsIdx(idx) {
			return false
		}
	}
	return true
}

func leafPredsEmitCandidate(
	cursor *queryCursor,
	preds []leafPred,
	checks []int,
	trace *Trace,
	idx uint64,
	examined *uint64,
) bool {
	*examined = *examined + 1
	if !leafPredsMatchActive(preds, checks, idx) {
		return false
	}
	if trace != nil {
		trace.AddMatched(1)
	}
	return cursor.emit(idx)
}

func leafPredsEmitMatchedPosting(
	cursor *queryCursor,
	ids posting.List,
	card uint64,
	trace *Trace,
	examined *uint64,
) bool {
	if ids.IsEmpty() {
		return false
	}
	*examined += card
	if trace != nil {
		trace.AddMatched(card)
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

func leafPredsTryBucketPosting(
	cursor *queryCursor,
	preds []leafPred,
	active, exactActive, residualActive []int,
	exactOnly, residualApplyOnly bool,
	ids, exactWork, applyWork posting.List,
	trace *Trace,
	examined *uint64,
) (current posting.List, exactApplied, handled, stop bool, nextExactWork, nextApplyWork posting.List) {

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
			trace.AddPostingExactFilter(1)
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

	return current, true, false, false, nextExactWork, nextApplyWork
}

func leafPredsEmitPosting(
	cursor *queryCursor,
	preds []leafPred,
	active, exactActive, residualActive []int,
	exactOnly, residualApplyOnly bool,
	ids, exactWork, applyWork posting.List,
	trace *Trace,
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

func (qv *View) tryLimitQuery(q *qir.Shape, trace *Trace) ([]uint64, bool, PlanName, error) {
	if q.Limit == 0 || q.Offset != 0 {
		return nil, false, "", nil
	}
	if q.Expr.Not {
		return nil, false, "", nil
	}

	var leavesBuf [8]qir.Expr
	leaves, ok := qir.CollectAndLeavesScratch(q.Expr, leavesBuf[:0], qir.LeafModeExtract)
	if !ok || len(leaves) == 0 {
		return nil, false, "", nil
	}

	if !q.HasOrder && len(leaves) == 1 && isPositiveScalarPrefixLeaf(leaves[0]) {
		out, used, err := qv.tryQueryPrefixNoOrderWithLimit(q, trace)
		if !used {
			return nil, false, "", err
		}
		return out, true, PlanLimitPrefixNoOrder, err
	}

	if q.HasOrder && q.Order.Kind == qir.OrderKindBasic {
		out, used, err := qv.tryLimitQueryOrderBasic(q, leaves, trace)
		if !used {
			return nil, false, "", err
		}
		plan := PlanLimitOrderBasic
		if qv.hasPrefixBoundForField(leaves, qv.exec.FieldNameByOrdinal(q.Order.FieldOrdinal)) {
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

func (qv *View) extractNoOrderBounds(leaves []qir.Expr) (string, indexdata.Bounds, bool, error) {
	var (
		f      string
		bounds indexdata.Bounds
		found  bool
	)

	for _, e := range leaves {
		if !isBoundOp(e.Op) {
			continue
		}
		if e.Not || e.FieldOrdinal < 0 {
			return "", indexdata.Bounds{}, false, nil
		}
		fieldName := qv.exec.FieldNameByOrdinal(e.FieldOrdinal)
		if !found {
			found = true
			f = fieldName
		} else if fieldName != f {
			return "", indexdata.Bounds{}, false, nil
		}
	}
	if !found {
		return "", indexdata.Bounds{}, false, nil
	}

	bounds.Has = true
	for _, e := range leaves {
		if !isBoundOp(e.Op) {
			continue
		}
		bound, isSlice, err := qv.exprValueToNormalizedScalarBound(e)
		if err != nil {
			return "", indexdata.Bounds{}, true, err
		}
		if isSlice {
			return "", indexdata.Bounds{}, false, nil
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
func (qv *View) tryUniqueEqNoOrder(q *qir.Shape, trace *Trace) ([]uint64, bool, error) {
	if q == nil || q.Expr.Not || q.Offset != 0 || q.HasOrder {
		return nil, false, nil
	}

	if out, ok, err := qv.tryDirectSingleUniqueEqNoOrder(q, trace); ok || err != nil {
		return out, ok, err
	}

	var leavesBuf [8]qir.Expr
	leaves, ok := qir.CollectAndLeavesScratch(q.Expr, leavesBuf[:0], qir.LeafModeExtract)
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

	predsBuf := leafPredSlicePool.Get(len(leaves))

	uniqueLead := -1
	sawEmpty := false

	for _, e := range leaves {
		lp, ok, err := qv.buildLeafPred(e)
		if err != nil {
			leafPredSlicePool.Put(predsBuf)
			return nil, true, err
		}
		if !ok {
			leafPredSlicePool.Put(predsBuf)
			return nil, false, nil
		}
		predsBuf = append(predsBuf, lp)
		if lp.kind == leafPredKindEmpty {
			sawEmpty = true
		}

		if !qv.isPositiveUniqueEqExpr(e) {
			continue
		}

		idx := len(predsBuf) - 1
		if uniqueLead == -1 || predsBuf[idx].estCard < predsBuf[uniqueLead].estCard {
			uniqueLead = idx
		}
	}
	defer leafPredSlicePool.Put(predsBuf)

	if uniqueLead < 0 {
		return nil, false, nil
	}
	if sawEmpty {
		if trace != nil {
			trace.SetPlan(PlanUniqueEq)
			trace.SetEarlyStopReason("empty_leaf")
		}
		return nil, true, nil
	}

	lead := predsBuf[uniqueLead]
	iter := lead.iterNew()
	defer iter.Release()

	if trace != nil {
		trace.SetPlan(PlanUniqueEq)
	}

	needAll := q.Limit == 0
	out := make([]uint64, 0, 1)
	cursor := newQueryCursor(out, 0, q.Limit, needAll, 0)

	var examined uint64
	for iter.HasNext() {
		idx := iter.Next()
		examined++

		pass := true
		for i := 0; i < len(predsBuf); i++ {
			if i == uniqueLead {
				continue
			}
			if !predsBuf[i].containsIdx(idx) {
				pass = false
				break
			}
		}
		if !pass {
			continue
		}

		if cursor.emit(idx) {
			if trace != nil {
				trace.AddExamined(examined)
				trace.SetEarlyStopReason("limit_reached")
			}
			return cursor.out, true, nil
		}
	}

	if trace != nil {
		trace.AddExamined(examined)
		trace.SetEarlyStopReason("input_exhausted")
	}
	return cursor.out, true, nil
}

func (qv *View) tryDirectSingleUniqueEqNoOrder(q *qir.Shape, trace *Trace) ([]uint64, bool, error) {
	if q == nil || q.Offset != 0 || q.HasOrder {
		return nil, false, nil
	}
	e := q.Expr
	if e.Not || e.Op != qir.OpEQ || e.FieldOrdinal < 0 || len(e.Operands) != 0 {
		return nil, false, nil
	}
	if !qv.isPositiveUniqueEqExpr(e) {
		return nil, false, nil
	}

	key, isSlice, isNil, err := qv.exprValueToLookupKey(e)
	if err != nil {
		return nil, true, err
	}
	if isSlice {
		return nil, false, nil
	}

	var ids posting.List
	if isNil {
		ids = qv.fieldIndexViewFromSlotsForExpr(qv.snap.NilIndex, e).LookupPostingRetained(indexdata.NilIndexEntryKey)
	} else {
		ids = lookupScalarPostingRetained(qv.fieldIndexViewFromSlotsForExpr(qv.snap.Index, e), key)
	}
	if ids.IsEmpty() {
		if trace != nil {
			trace.SetPlan(PlanUniqueEq)
			trace.AddExamined(0)
			trace.SetEarlyStopReason("input_exhausted")
		}
		return nil, true, nil
	}
	defer ids.Release()

	if trace != nil {
		trace.SetPlan(PlanUniqueEq)
	}
	if idx, ok := ids.TrySingle(); ok {
		out := make([]uint64, 1)
		out[0] = idx
		if trace != nil {
			trace.AddExamined(1)
			if q.Limit == 1 {
				trace.SetEarlyStopReason("limit_reached")
			} else {
				trace.SetEarlyStopReason("input_exhausted")
			}
		}
		return out, true, nil
	}

	needAll := q.Limit == 0
	capOut := clampUint64ToInt(q.Limit)
	if needAll {
		capOut = clampUint64ToInt(ids.Cardinality())
	}

	out := make([]uint64, 0, capOut)
	cursor := newQueryCursor(out, 0, q.Limit, needAll, 0)

	var examined uint64
	var examinedPtr *uint64

	if trace != nil {
		examinedPtr = &examined
	}

	stopped := emitAcceptedPostingNoOrder(&cursor, ids, examinedPtr)
	if trace != nil {
		trace.AddExamined(examined)
		if stopped {
			trace.SetEarlyStopReason("limit_reached")
		} else {
			trace.SetEarlyStopReason("input_exhausted")
		}
	}
	return cursor.out, true, nil
}

func (qv *View) hasPrefixBoundForField(leaves []qir.Expr, field string) bool {
	for _, e := range leaves {
		if e.Not {
			continue
		}
		if qv.exec.FieldNameByOrdinal(e.FieldOrdinal) == field && e.Op == qir.OpPREFIX {
			return true
		}
	}
	return false
}

func (qv *View) tryLimitQueryNoOrder(q *qir.Shape, leaves []qir.Expr, trace *Trace) ([]uint64, bool, error) {

	predsBuf := leafPredSlicePool.Get(len(leaves))

	for _, e := range leaves {
		if isBoundOp(e.Op) {
			leafPredSlicePool.Put(predsBuf)
			return nil, false, nil
		}
		lp, ok, err := qv.buildLeafPred(e)
		if err != nil {
			leafPredSlicePool.Put(predsBuf)
			return nil, true, err
		}
		if !ok {
			leafPredSlicePool.Put(predsBuf)
			return nil, false, nil
		}
		if lp.kind == leafPredKindEmpty {
			// AND with an empty leaf is empty; avoid selecting a non-iterable lead.
			leafPredSlicePool.Put(predsBuf)
			return nil, true, nil
		}
		predsBuf = append(predsBuf, lp)
	}
	defer leafPredSlicePool.Put(predsBuf)

	leadIdx := pickLeadIndex(predsBuf)
	if leadIdx < 0 {
		return nil, false, nil
	}

	lead := predsBuf[leadIdx]
	leadNeedsCheck := lead.leadIterNeedsContainsCheck()

	limit := int(q.Limit)
	out := make([]uint64, 0, limit)
	cursor := newQueryCursor(out, 0, q.Limit, false, 0)

	iter := lead.iterNew()
	defer iter.Release()

	var examined uint64
	for iter.HasNext() {
		idx := iter.Next()
		examined++

		pass := true
		for i := 0; i < len(predsBuf); i++ {
			if i == leadIdx && !leadNeedsCheck {
				continue
			}
			if !predsBuf[i].containsIdx(idx) {
				pass = false
				break
			}
		}
		if !pass {
			continue
		}

		if cursor.emit(idx) {
			trace.AddExamined(examined)
			trace.SetEarlyStopReason("limit_reached")
			return cursor.out, true, nil
		}
	}

	trace.AddExamined(examined)
	trace.SetEarlyStopReason("input_exhausted")
	return cursor.out, true, nil
}

func (qv *View) tryLimitQueryOrderBasic(q *qir.Shape, leaves []qir.Expr, trace *Trace) ([]uint64, bool, error) {
	order := q.Order
	needWindow, _ := orderWindow(q)

	f := qv.exec.FieldNameByOrdinal(order.FieldOrdinal)
	if order.FieldOrdinal < 0 {
		return nil, false, nil
	}

	fm := qv.fieldMetaByOrder(order)
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
	ov := qv.fieldIndexViewFromSlotsForOrder(qv.snap.Index, order)
	if !ov.HasData() && nilTailField == "" {
		if !qv.hasIndexedFieldForOrder(order) {
			return nil, false, nil
		}
		return nil, true, nil
	}

	predsBuf, ok, err := qv.buildLeafPredsExcludingBounds(leaves, f, needWindow)
	if err != nil {
		return nil, true, err
	}

	br := ov.RangeForBounds(bounds)
	if br.Empty() && nilTailField == "" {
		return nil, true, nil
	}

	if ok {
		if predsBuf != nil {
			defer leafPredSlicePool.Put(predsBuf)
		}
		if hasEmptyLeafPred(predsBuf) {
			return nil, true, nil
		}

		universe := qv.snap.Universe.Cardinality()
		if predsBuf != nil {
			for i := 0; i < len(predsBuf); i++ {
				pred := predsBuf[i]
				if pred.kind != leafPredKindPredicate || pred.pred.fieldIndexRangeState == nil {
					continue
				}
				pred.pred.setExpectedContainsCalls(
					orderedFieldIndexRangeExpectedContainsCalls(pred.pred.fieldIndexRangeState, needWindow, universe),
				)
				predsBuf[i] = pred
			}
		}

		execTrace := trace
		var observedTrace Trace
		observedStart := uint64(0)
		if execTrace == nil {
			execTrace = &observedTrace
		} else {
			observedStart = execTrace.RowsExamined()
		}

		out := qv.scanLimitByFieldIndexBounds(q, ov, br, order.Desc, predsBuf, nilTailField, execTrace)
		qv.promoteObservedLimitLeafPreds(f, predsBuf, execTrace.RowsExamined()-observedStart, q.Limit)
		return out, true, nil
	}

	window := needWindow
	if window <= 0 {
		return nil, false, nil
	}

	var residualLeavesBuf [limitQueryFastPathMaxLeaves]qir.Expr
	residualLeaves := residualLeavesBuf[:0]

	for _, e := range leaves {
		if isBoundOp(e.Op) && !e.Not && e.FieldOrdinal == order.FieldOrdinal {
			continue
		}
		residualLeaves = append(residualLeaves, e)
	}

	if len(residualLeaves) == 0 {
		out, _ := qv.scanOrderLimitNoPredicates(q, ov, br, order.Desc, nilTailField, trace)
		return out, true, nil
	}

	fullPredSet, ok := qv.buildPredicatesOrderedWithMode(residualLeaves, f, false, window, q.Offset, false, true)
	if !ok {
		return nil, false, nil
	}
	defer fullPredSet.Release()

	for i := 0; i < fullPredSet.Len(); i++ {
		if fullPredSet.owner[i].alwaysFalse {
			return nil, true, nil
		}
	}

	out, _ := qv.scanOrderLimitWithPredicatesReader(q, ov, br, order.Desc, fullPredSet.owner, nilTailField, trace)
	return out, true, nil
}

func (qv *View) tryLimitQueryRangeNoOrderByField(q *qir.Shape, field string, bounds indexdata.Bounds, leaves []qir.Expr, trace *Trace) ([]uint64, bool, error) {
	fm := qv.exec.Schema.Fields[field]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}

	ov := qv.fieldIndexViewFromSlotsByName(qv.snap.Index, field)
	if !ov.HasData() {
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

	br := ov.RangeForBounds(bounds)
	if br.Empty() {
		return nil, true, nil
	}
	return qv.scanLimitByFieldIndexBounds(q, ov, br, false, predsBuf, "", trace), true, nil
}

func (qv *View) buildLeafPredsExcludingBounds(leaves []qir.Expr, field string, orderedWindow int) ([]leafPred, bool, error) {
	predsBuf := leafPredSlicePool.Get(len(leaves))

	orderedUniverse := uint64(0)
	if orderedWindow > 0 {
		orderedUniverse = qv.snap.Universe.Cardinality()
	}

	var mergedRangesBuf []orderedMergedScalarRangeField
	if len(leaves) > 1 {
		mergedRangesBuf = orderedMergedScalarRangeFieldSlicePool.Get(len(leaves))
		var ok bool
		mergedRangesBuf, ok = qv.collectMergedNumericRangeFields(leaves, mergedRangesBuf)
		if !ok {
			leafPredSlicePool.Put(predsBuf)
			orderedMergedScalarRangeFieldSlicePool.Put(mergedRangesBuf)
			return nil, false, nil
		}
		defer orderedMergedScalarRangeFieldSlicePool.Put(mergedRangesBuf)
	}

	for i, e := range leaves {
		if isBoundOp(e.Op) && !e.Not && qv.exec.FieldNameByOrdinal(e.FieldOrdinal) == field {
			continue
		}
		if mergedRangesBuf != nil && qv.isPositiveMergedNumericRangeLeaf(e) {
			idx := findOrderedMergedScalarRangeField(mergedRangesBuf, qv.exec.FieldNameByOrdinal(e.FieldOrdinal))
			if idx >= 0 {
				merged := mergedRangesBuf[idx]
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
					if orderedUniverse > 0 && p.postsAnyState != nil {
						p.setExpectedContainsCalls(
							clampUint64ToInt(orderedPredicateExpectedRows(orderedWindow, p.estCard, orderedUniverse)),
						)
					}
					predsBuf = append(predsBuf, p)
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
		if orderedUniverse > 0 && p.postsAnyState != nil {
			p.setExpectedContainsCalls(
				clampUint64ToInt(orderedPredicateExpectedRows(orderedWindow, p.estCard, orderedUniverse)),
			)
		}
		predsBuf = append(predsBuf, p)
	}

	if len(predsBuf) == 0 {
		leafPredSlicePool.Put(predsBuf)
		return nil, true, nil
	}

	if orderedUniverse > 0 {
		for i := 0; i < len(predsBuf); i++ {
			p := predsBuf[i]
			if p.kind != leafPredKindPredicate || !p.pred.hasRuntimeRangeState() {
				continue
			}
			orderedRoute := qv.orderedPredicateScalarRangeRouting(p.pred, orderedWindow, 0, orderedUniverse)
			if !orderedRoute.broadComplement && !orderedRoute.forceComplement {
				continue
			}
			exactSiblingCount := 0
			for j := 0; j < len(predsBuf); j++ {
				if j == i {
					continue
				}
				if predsBuf[j].supportsExactBucketPostingFilter() {
					exactSiblingCount++
				}
			}
			if exactSiblingCount >= 2 && !orderedRoute.forceComplement {
				// Multiple exact bucket siblings already cut broad range work
				// aggressively, so first-hit complement materialization is only
				// worth taking when a warm shared complement already exists.
				if orderedRoute.complementCacheKey.IsZero() || qv.snap == nil {
					continue
				}
				if _, ok := qv.snap.LoadMaterializedPredKey(orderedRoute.complementCacheKey); !ok {
					continue
				}
			}
			qv.tryMaterializeBroadRangeComplementPredicateForOrdered(
				&p.pred,
				orderedRoute.broadComplement,
				orderedUniverse,
				orderedWindow,
				orderedRoute.forceComplement,
			)
			predsBuf[i] = p
		}
	}

	return predsBuf, true, nil
}

func (qv *View) scanLimitByFieldIndexBounds(q *qir.Shape, ov indexdata.FieldIndexView, br indexdata.FieldIndexRange, desc bool, preds []leafPred, nilTailField string, trace *Trace) []uint64 {
	limit := int(q.Limit)
	out := make([]uint64, 0, limit)
	cursor := newQueryCursor(out, 0, q.Limit, false, 0)
	trackScanWidth := q.HasOrder

	var (
		examined  uint64
		scanWidth uint64
	)

	predCount := 0
	if preds != nil {
		predCount = len(preds)
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
			if !preds[pi].supportsPostingApply() {
				residualApplyOnly = false
				break
			}
		}
	}

	var (
		exactWork posting.List
		applyWork posting.List
	)

	keyCur := ov.NewCursor(br, desc)
	for {
		_, ids, ok := keyCur.Next()
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
			trace.AddExamined(examined)
			if trackScanWidth {
				trace.AddOrderScanWidth(scanWidth)
			}
			trace.SetEarlyStopReason("limit_reached")

			exactWork.Release()
			applyWork.Release()

			return cursor.out
		}
	}

	if nilTailField != "" {
		ids := qv.fieldIndexViewFromSlotsByName(qv.snap.NilIndex, nilTailField).LookupPostingRetained(indexdata.NilIndexEntryKey)
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
				trace.AddExamined(examined)
				if trackScanWidth {
					trace.AddOrderScanWidth(scanWidth)
				}
				trace.SetEarlyStopReason("limit_reached")

				exactWork.Release()
				applyWork.Release()

				return cursor.out
			}
		}
	}

	trace.AddExamined(examined)
	if trackScanWidth {
		trace.AddOrderScanWidth(scanWidth)
	}
	trace.SetEarlyStopReason("input_exhausted")

	exactWork.Release()
	applyWork.Release()

	return cursor.out
}

func isBoundOp(op qir.Op) bool {
	switch op {
	case qir.OpGT, qir.OpGTE, qir.OpLT, qir.OpLTE, qir.OpPREFIX:
		return true
	default:
		return false
	}
}

func (qv *View) extractBoundsForField(field string, leaves []qir.Expr) (indexdata.Bounds, bool, error) {
	var b indexdata.Bounds
	found := false

	for _, e := range leaves {
		if !isBoundOp(e.Op) {
			continue
		}
		if e.Not || e.FieldOrdinal < 0 {
			return b, false, nil
		}
		if qv.exec.FieldNameByOrdinal(e.FieldOrdinal) != field {
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

func (qv *View) buildLeafPred(e qir.Expr) (leafPred, bool, error) {
	if e.Not || e.FieldOrdinal < 0 {
		return leafPred{}, false, nil
	}

	fieldName := qv.exec.FieldNameByOrdinal(e.FieldOrdinal)
	ov := qv.fieldIndexViewFromSlotsForExpr(qv.snap.Index, e)
	if !ov.HasData() && !qv.hasIndexedFieldForExpr(e) {
		return leafPred{}, false, nil
	}

	fm := qv.fieldMetaByExpr(e)
	if fm == nil {
		return leafPred{}, false, nil
	}

	switch e.Op {

	case qir.OpEQ:
		if fm.Slice {
			return leafPred{}, false, nil
		}

		key, isSlice, isNil, err := qv.exprValueToLookupKey(e)
		if err != nil {
			return leafPred{}, true, err
		}
		if isSlice {
			return leafPred{}, false, nil
		}
		if isNil {
			ids := qv.fieldIndexViewFromSlotsForExpr(qv.snap.NilIndex, e).LookupPostingRetained(indexdata.NilIndexEntryKey)
			if ids.IsEmpty() {
				return emptyLeaf(), true, nil
			}

			return leafPred{
				kind:    leafPredKindPosting,
				posting: ids,
				estCard: ids.Cardinality(),
			}, true, nil
		}

		ids := lookupScalarPostingRetained(ov, key)
		if ids.IsEmpty() {
			return emptyLeaf(), true, nil
		}

		return leafPred{
			kind:    leafPredKindPosting,
			posting: ids,
			estCard: ids.Cardinality(),
		}, true, nil

	case qir.OpIN:
		if fm.Slice {
			return leafPred{}, false, nil
		}

		keysBuf, isSlice, hasNil, err := qv.exprValueToDistinctIdxBuf(e)
		if err != nil {
			return leafPred{}, true, err
		}
		if keysBuf != nil {
			defer pooled.ReleaseStringSlice(keysBuf)
		}
		keyCount := len(keysBuf)
		if !isSlice || (keyCount == 0 && !hasNil) {
			return leafPred{}, false, nil
		}

		postsBuf, est := qv.scalarLookupPostings(fieldName, e.FieldOrdinal, keysBuf, hasNil)
		if len(postsBuf) == 0 {
			posting.ReleaseSlice(postsBuf)
			return emptyLeaf(), true, nil
		}
		if len(postsBuf) == 1 {
			ids := postsBuf[0]
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

	case qir.OpHASALL:
		if !fm.Slice {
			return leafPred{}, false, nil
		}

		keysBuf, isSlice, _, err := qv.exprValueToDistinctIdxBuf(e)
		if err != nil {
			return leafPred{}, true, err
		}
		if keysBuf != nil {
			defer pooled.ReleaseStringSlice(keysBuf)
		}
		keyCount := len(keysBuf)
		if !isSlice || keyCount == 0 {
			return leafPred{}, false, nil
		}

		postsBuf := posting.GetSlice(keyCount)

		var est uint64
		for i := 0; i < keyCount; i++ {
			ids := ov.LookupPostingRetained(keysBuf[i])
			if ids.IsEmpty() {
				posting.ReleaseSlice(postsBuf)
				return emptyLeaf(), true, nil
			}
			postsBuf = append(postsBuf, ids)
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

	case qir.OpHASANY:
		if !fm.Slice {
			return leafPred{}, false, nil
		}

		keysBuf, isSlice, _, err := qv.exprValueToDistinctIdxBuf(e)
		if err != nil {
			return leafPred{}, true, err
		}
		if keysBuf != nil {
			defer pooled.ReleaseStringSlice(keysBuf)
		}
		if !isSlice || len(keysBuf) == 0 {
			return leafPred{}, false, nil
		}

		postsBuf, est := ov.LookupPostings(keysBuf)
		if len(postsBuf) == 0 {
			posting.ReleaseSlice(postsBuf)
			return emptyLeaf(), true, nil
		}
		if len(postsBuf) == 1 {
			ids := postsBuf[0]
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

	case qir.OpGT, qir.OpGTE, qir.OpLT, qir.OpLTE, qir.OpPREFIX:
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

func (qv *View) buildLimitLeafPred(e qir.Expr, orderedWindow int) (leafPred, bool, error) {

	if orderedWindow > 0 && isSimpleScalarRangeOrPrefixLeaf(e) && !e.Not {

		if candidate, ok := qv.prepareScalarRangeRoutingCandidate(e); ok &&
			candidate.plan.orderedEagerMaterializeUseful(orderedWindow, qv.snap.Universe.Cardinality()) {

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

func (qv *View) buildMergedLimitLeafPred(e qir.Expr, bounds indexdata.Bounds, orderedWindow int) (leafPred, bool, error) {
	fm := qv.fieldMetaByExpr(e)
	if fm == nil || fm.Slice {
		return leafPred{}, false, nil
	}

	fieldName := qv.exec.FieldNameByOrdinal(e.FieldOrdinal)
	allowMaterialize := false

	if orderedWindow > 0 {
		var core preparedScalarRangePredicate
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
				field:    fieldName,
				bounds:   bounds,
				cacheKey: qv.materializedPredKeyForExactScalarRange(fieldName, bounds),
			},
		},
		estCard: p.estCard,
	}, true, nil
}

func (qv *View) supportsLimitLeafPredExpr(e qir.Expr) bool {
	if e.Not || e.FieldOrdinal < 0 {
		return false
	}
	fm := qv.fieldMetaByExpr(e)
	if fm == nil {
		return false
	}

	if !qv.fieldIndexViewFromSlotsForExpr(qv.snap.Index, e).HasData() && !qv.hasIndexedFieldForExpr(e) {
		return false
	}

	switch e.Op {
	case qir.OpEQ:
		return !fm.Slice
	case qir.OpIN:
		return !fm.Slice
	case qir.OpHASALL, qir.OpHASANY:
		return fm.Slice
	case qir.OpGT, qir.OpGTE, qir.OpLT, qir.OpLTE, qir.OpPREFIX:
		return true
	default:
		return false
	}
}

func (qv *View) supportsLimitLeafPredsExcludingBounds(leaves []qir.Expr, field string) bool {
	hasResidual := false
	for _, e := range leaves {
		if isBoundOp(e.Op) && !e.Not && qv.exec.FieldNameByOrdinal(e.FieldOrdinal) == field {
			continue
		}
		hasResidual = true
		if !qv.supportsLimitLeafPredExpr(e) {
			return false
		}
	}
	return hasResidual
}

func (qv *View) hasWarmScalarLimitLeafPredsExcludingBounds(leaves []qir.Expr, field string) bool {
	for _, e := range leaves {
		if isBoundOp(e.Op) && !e.Not && qv.exec.FieldNameByOrdinal(e.FieldOrdinal) == field {
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
			hit.ids.Release()
			return true
		}
	}
	return false
}

func (qv *View) attachLeafPredPostingFilter(p *leafPred) {
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

func buildExactBucketPostingFilterActiveLeaf(dst, active []int, preds []leafPred) []int {
	dst = dst[:0]
	if preds == nil {
		return dst
	}
	for _, pi := range active {
		if preds[pi].supportsExactBucketPostingFilter() {
			dst = append(dst, pi)
		}
	}
	return dst
}

func plannerFilterCompactPostingByLeafChecks(
	preds []leafPred,
	checks []int,
	src, work posting.List,
	card uint64,
) (plannerPredicateBucketMode, posting.List, posting.List, bool) {

	if preds == nil || card == 0 || card > posting.MidCap {
		return 0, posting.List{}, work, false
	}

	if idx, ok := src.TrySingle(); ok {
		for _, pi := range checks {
			if !preds[pi].containsIdx(idx) {
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
			if !preds[pi].containsIdx(idx) {
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
	preds []leafPred,
	checks []int,
	src, work posting.List,
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
	preferExact := false
	if smallCard {
		for _, pi := range checks {
			if preds[pi].prefersExactBucketPostingFilter() {
				preferExact = true
				break
			}
		}
	}

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

	if mode, exact, nextWork, ok := plannerFilterCompactPostingByLeafChecks(preds, checks, src, work, card); ok {
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

func pickLeadIndex(ps []leafPred) int {
	if ps == nil || len(ps) == 0 {
		return -1
	}
	best := -1
	for i := 0; i < len(ps); i++ {
		pred := ps[i]
		if !pred.hasIter() {
			continue
		}
		if best < 0 || pred.estCard < ps[best].estCard {
			best = i
		}
	}
	return best
}

func hasEmptyLeafPred(preds []leafPred) bool {
	if preds == nil {
		return false
	}
	for i := 0; i < len(preds); i++ {
		if preds[i].kind == leafPredKindEmpty {
			return true
		}
	}
	return false
}

func minCardPostingBuf(posts []posting.List) posting.List {
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

func emptyLeaf() leafPred {
	return leafPred{
		kind:    leafPredKindEmpty,
		estCard: 0,
	}
}
