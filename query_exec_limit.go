package rbi

import (
	"sync"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/posting"
)

type leafPred struct {
	kind          leafPredKind
	posting       posting.List
	posts         []posting.List
	estCard       uint64
	postingFilter func(*posting.List) bool
	release       func()
}

type leafPredKind uint8

const (
	leafPredKindEmpty leafPredKind = iota
	leafPredKindPosting
	leafPredKindPostsConcat
	leafPredKindPostsUnion
	leafPredKindPostsAll
)

func (p leafPred) hasIter() bool {
	switch p.kind {
	case leafPredKindEmpty, leafPredKindPosting, leafPredKindPostsConcat, leafPredKindPostsUnion, leafPredKindPostsAll:
		return true
	default:
		return false
	}
}

func (p leafPred) leadIterNeedsContainsCheck() bool {
	// HAS(list) uses one posting as iterator seed and must still validate
	// remaining terms for each candidate.
	return p.kind == leafPredKindPostsAll && len(p.posts) > 1
}

func (p leafPred) iterNew() posting.Iterator {
	switch p.kind {
	case leafPredKindEmpty:
		return emptyIter{}
	case leafPredKindPosting:
		return p.posting.Iter()
	case leafPredKindPostsConcat:
		return newPostingConcatIter(p.posts)
	case leafPredKindPostsUnion:
		return newPostingUnionIter(p.posts)
	case leafPredKindPostsAll:
		return p.posting.Iter()
	default:
		return emptyIter{}
	}
}

func (p leafPred) containsIdx(idx uint64) bool {
	switch p.kind {
	case leafPredKindPosting:
		return p.posting.Contains(idx)
	case leafPredKindPostsConcat, leafPredKindPostsUnion:
		for _, ids := range p.posts {
			if ids.Contains(idx) {
				return true
			}
		}
		return false
	case leafPredKindPostsAll:
		for _, ids := range p.posts {
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
	case leafPredKindPosting, leafPredKindPostsAll:
		return true
	case leafPredKindPostsConcat, leafPredKindPostsUnion:
		return p.postingFilter != nil
	default:
		return false
	}
}

func (p leafPred) countBucket(bucket posting.List) (uint64, bool) {
	switch p.kind {
	case leafPredKindEmpty:
		return 0, true
	case leafPredKindPosting:
		return p.posting.AndCardinality(bucket), true
	case leafPredKindPostsConcat, leafPredKindPostsUnion:
		return countBucketPostsAny(p.posts, bucket)
	case leafPredKindPostsAll:
		return countBucketPostsAll(p.posts, bucket)
	default:
		return 0, false
	}
}

func (p leafPred) applyToPosting(dst *posting.List) bool {
	if dst == nil {
		return false
	}
	switch p.kind {
	case leafPredKindEmpty:
		dst.Clear()
		return true

	case leafPredKindPosting:
		dst.AndInPlace(p.posting)
		return true

	case leafPredKindPostsConcat, leafPredKindPostsUnion:
		if p.postingFilter == nil {
			return false
		}
		return p.postingFilter(dst)

	case leafPredKindPostsAll:
		if len(p.posts) == 0 {
			dst.Clear()
			return true
		}
		for _, ids := range p.posts {
			if ids.IsEmpty() {
				dst.Clear()
				return true
			}
			dst.AndInPlace(ids)
			if dst.IsEmpty() {
				return true
			}
		}
		return true
	}
	return false
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

	preds := make([]leafPred, 0, len(leaves))
	defer func() { releaseLeafPreds(preds) }()

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
		preds = append(preds, lp)
		if lp.kind == leafPredKindEmpty {
			sawEmpty = true
		}

		if !qv.isPositiveUniqueEqExpr(e) {
			continue
		}

		idx := len(preds) - 1
		if uniqueLead == -1 || preds[idx].estCard < preds[uniqueLead].estCard {
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

	lead := preds[uniqueLead]
	iter := lead.iterNew()
	defer iter.Release()

	if trace != nil {
		trace.setPlan(PlanUniqueEq)
	}

	needAll := q.Limit == 0
	out := make([]K, 0, 1)
	cursor := qv.newQueryCursor(out, 0, q.Limit, needAll, nil)

	var examined uint64
	for iter.HasNext() {
		idx := iter.Next()
		examined++

		pass := true
		for i := range preds {
			if i == uniqueLead {
				continue
			}
			if !preds[i].containsIdx(idx) {
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

	preds := make([]leafPred, 0, len(leaves))
	defer func() { releaseLeafPreds(preds) }()

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
		preds = append(preds, lp)
	}

	leadIdx := pickLeadIndex(preds)
	if leadIdx < 0 {
		return nil, false, nil
	}
	lead := preds[leadIdx]
	leadNeedsCheck := lead.leadIterNeedsContainsCheck()

	limit := int(q.Limit)
	out := make([]K, 0, limit)
	cursor := qv.newQueryCursor(out, 0, q.Limit, false, nil)

	iter := lead.iterNew()
	defer iter.Release()

	var examined uint64
	for iter.HasNext() {
		idx := iter.Next()
		examined++

		pass := true
		for i := range preds {
			if i == leadIdx && !leadNeedsCheck {
				continue
			}
			if !preds[i].containsIdx(idx) {
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

	preds, ok, err := qv.buildLeafPredsExcludingBounds(leaves, f)
	if err != nil {
		return nil, true, err
	}
	if !ok {
		return nil, false, nil
	}
	defer func() { releaseLeafPreds(preds) }()
	if hasEmptyLeafPred(preds) {
		return nil, true, nil
	}

	br := ov.rangeForBounds(bounds)
	if overlayRangeEmpty(br) && nilTailField == "" {
		return nil, true, nil
	}
	return qv.scanLimitByOverlayBounds(q, ov, br, order.Desc, preds, nilTailField, trace), true, nil
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

	preds, ok, err := qv.buildLeafPredsExcludingBounds(leaves, field)
	if err != nil {
		return nil, true, err
	}
	if !ok {
		return nil, false, nil
	}
	defer func() { releaseLeafPreds(preds) }()
	if hasEmptyLeafPred(preds) {
		return nil, true, nil
	}

	br := ov.rangeForBounds(bounds)
	if overlayRangeEmpty(br) {
		return nil, true, nil
	}
	return qv.scanLimitByOverlayBounds(q, ov, br, false, preds, "", trace), true, nil
}

func (qv *queryView[K, V]) buildLeafPredsExcludingBounds(leaves []qx.Expr, field string) ([]leafPred, bool, error) {
	preds := make([]leafPred, 0, len(leaves))
	for _, e := range leaves {
		if isBoundOp(e.Op) {
			if e.Not || e.Field == "" || e.Field != field {
				releaseLeafPreds(preds)
				return nil, false, nil
			}
			continue
		}
		p, ok, err := qv.buildLeafPred(e)
		if err != nil {
			releaseLeafPreds(preds)
			return nil, true, err
		}
		if !ok {
			releaseLeafPreds(preds)
			return nil, false, nil
		}
		qv.attachLeafPredPostingFilter(&p)
		preds = append(preds, p)
	}
	return preds, true, nil
}

func (qv *queryView[K, V]) scanLimitByOverlayBounds(q *qx.QX, ov fieldOverlay, br overlayRange, desc bool, preds []leafPred, nilTailField string, trace *queryTrace) []K {
	limit := int(q.Limit)
	out := make([]K, 0, limit)
	cursor := qv.newQueryCursor(out, 0, q.Limit, false, nil)
	trackScanWidth := len(q.Order) == 1
	var (
		examined  uint64
		scanWidth uint64
	)

	activeBuf := getIntSliceBuf(len(preds))
	active := activeBuf.values[:0]
	defer func() {
		activeBuf.values = active
		releaseIntSliceBuf(activeBuf)
	}()
	for i := range preds {
		active = append(active, i)
	}

	exactActiveBuf := getIntSliceBuf(len(active))
	exactActive := buildExactBucketPostingFilterActive(exactActiveBuf.values, active, preds)
	defer func() {
		exactActiveBuf.values = exactActive
		releaseIntSliceBuf(exactActiveBuf)
	}()
	exactOnly := len(active) > 0 && len(active) == len(exactActive)

	residualActiveBuf := getIntSliceBuf(len(active))
	residualActive := plannerResidualChecks(residualActiveBuf.values[:0], active, exactActive)
	defer func() {
		residualActiveBuf.values = residualActive
		releaseIntSliceBuf(residualActiveBuf)
	}()

	var exactWork posting.List
	defer exactWork.Release()

	emitCandidate := func(idx uint64, checks []int) bool {
		examined++
		for _, pi := range checks {
			if !preds[pi].containsIdx(idx) {
				return false
			}
		}
		if trace != nil {
			trace.addMatched(1)
		}
		return cursor.emit(idx)
	}

	emitMatchedPosting := func(ids posting.List, card uint64) bool {
		if ids.IsEmpty() {
			return false
		}
		examined += card
		if trace != nil {
			trace.addMatched(card)
		}
		if idx, ok := ids.TrySingle(); ok {
			return cursor.emit(idx)
		}
		stop := false
		ids.ForEach(func(idx uint64) bool {
			if cursor.emit(idx) {
				stop = true
				return false
			}
			return true
		})
		return stop
	}

	tryBucketPosting := func(ids posting.List) (current posting.List, exactApplied bool, handled bool, stop bool) {
		if ids.IsEmpty() {
			return posting.List{}, false, true, false
		}
		card := ids.Cardinality()
		if len(active) == 0 {
			return ids, false, true, emitMatchedPosting(ids, card)
		}
		if len(exactActive) == 0 {
			return ids, false, false, false
		}

		allowExact := plannerAllowExactBucketFilter(0, cursor.need, card, exactOnly, len(exactActive))
		mode, exactIDs, _ := plannerFilterPostingByChecks(preds, exactActive, ids, &exactWork, allowExact)
		switch mode {
		case plannerPredicateBucketEmpty:
			examined += card
			return posting.List{}, false, true, false
		case plannerPredicateBucketAll:
			if exactOnly {
				return exactIDs, true, true, emitMatchedPosting(exactIDs, card)
			}
			return exactIDs, true, false, false
		case plannerPredicateBucketExact:
			if trace != nil {
				trace.addPostingExactFilter(1)
			}
			if exactIDs.IsEmpty() {
				examined += card
				return posting.List{}, true, true, false
			}
			if exactOnly {
				return exactIDs, true, true, emitMatchedPosting(exactIDs, exactIDs.Cardinality())
			}
			return exactIDs, true, false, false
		default:
			return ids, false, false, false
		}
	}

	emitBucketPosting := func(ids posting.List) bool {
		if ids.IsEmpty() {
			return false
		}
		if len(active) == 0 {
			if idx, ok := ids.TrySingle(); ok {
				if trace != nil {
					trace.addMatched(1)
				}
				examined++
				return cursor.emit(idx)
			}
			card := ids.Cardinality()
			if trace != nil {
				trace.addMatched(card)
			}
			if cursor.skip >= card {
				cursor.skip -= card
				examined += card
				return false
			}
			stop := false
			ids.ForEach(func(idx uint64) bool {
				examined++
				if cursor.emit(idx) {
					stop = true
					return false
				}
				return true
			})
			return stop
		}
		if idx, ok := ids.TrySingle(); ok {
			return emitCandidate(idx, active)
		}

		iterSrc := ids.Iter()
		defer func() {
			if iterSrc != nil {
				iterSrc.Release()
			}
		}()

		exactApplied := false
		if len(exactActive) > 0 {
			if current, applied, handled, stop := tryBucketPosting(ids); handled {
				return stop
			} else if applied && !current.IsEmpty() {
				iterSrc.Release()
				iterSrc = current.Iter()
				exactApplied = true
			}
		}

		checks := active
		if exactApplied {
			checks = residualActive
		}
		for iterSrc.HasNext() {
			if emitCandidate(iterSrc.Next(), checks) {
				return true
			}
		}
		return false
	}

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
		if emitBucketPosting(ids) {
			trace.addExamined(examined)
			if trackScanWidth {
				trace.addOrderScanWidth(scanWidth)
			}
			trace.setEarlyStopReason("limit_reached")
			return cursor.out
		}
	}

	if nilTailField != "" {
		ids := qv.nilFieldOverlay(nilTailField).lookupPostingRetained(nilIndexEntryKey)
		if !ids.IsEmpty() {
			if trackScanWidth {
				scanWidth++
			}
			if emitBucketPosting(ids) {
				trace.addExamined(examined)
				if trackScanWidth {
					trace.addOrderScanWidth(scanWidth)
				}
				trace.setEarlyStopReason("limit_reached")
				return cursor.out
			}
		}
	}

	trace.addExamined(examined)
	if trackScanWidth {
		trace.addOrderScanWidth(scanWidth)
	}
	trace.setEarlyStopReason("input_exhausted")
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

	for _, e := range leaves {
		if !isBoundOp(e.Op) {
			continue
		}
		if e.Not || e.Field == "" {
			return b, false, nil
		}
		if e.Field != field {
			return b, false, nil
		}

		bound, isSlice, err := qv.exprValueToNormalizedScalarBound(e)
		if err != nil {
			return b, true, err
		}
		if isSlice {
			return b, false, nil
		}
		applyNormalizedScalarBound(&b, bound)
	}

	return b, true, nil
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
		lo := lowerBoundIndex(s, b.loKey)
		if !b.loInc {
			if lo < len(s) && indexKeyEqualsString(s[lo].Key, b.loKey) {
				lo++
			}
		}
		if lo > start {
			start = lo
		}
	}
	if b.hasHi {
		hi := 0
		if b.hiInc {
			hi = upperBoundIndex(s, b.hiKey)
		} else {
			hi = lowerBoundIndex(s, b.hiKey)
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

		keys, isSlice, hasNil, err := qv.exprValueToDistinctIdxOwned(e)
		if err != nil {
			return leafPred{}, true, err
		}
		if !isSlice || (len(keys) == 0 && !hasNil) {
			return leafPred{}, false, nil
		}

		posts, est, release := qv.scalarLookupPostings(e.Field, keys, hasNil)
		if len(posts) == 0 {
			if release != nil {
				release()
			}
			return emptyLeaf(), true, nil
		}

		return leafPred{
			kind:    leafPredKindPostsConcat,
			posts:   posts,
			estCard: est,
			release: release,
		}, true, nil

	case qx.OpHAS:
		if !fm.Slice {
			return leafPred{}, false, nil
		}

		keys, isSlice, _, err := qv.exprValueToIdxBorrowed(e)
		if err != nil {
			return leafPred{}, true, err
		}
		if !isSlice || len(keys) == 0 {
			return leafPred{}, false, nil
		}

		postsBuf := getPostingSliceBuf(len(keys))
		posts := postsBuf.values

		var est uint64
		for _, k := range keys {
			ids := ov.lookupPostingRetained(k)
			if ids.IsEmpty() {
				postsBuf.values = posts
				releasePostingSliceBuf(postsBuf)
				return emptyLeaf(), true, nil
			}
			posts = append(posts, ids)
			c := ids.Cardinality()
			if est == 0 || c < est {
				est = c
			}
		}

		lead := minCardPosting(posts)
		var release func()
		release = func() {
			postsBuf.values = posts
			releasePostingSliceBuf(postsBuf)
		}

		return leafPred{
			kind:    leafPredKindPostsAll,
			posting: lead,
			posts:   posts,
			estCard: est,
			release: release,
		}, true, nil

	case qx.OpHASANY:
		if !fm.Slice {
			return leafPred{}, false, nil
		}

		keys, isSlice, _, err := qv.exprValueToIdxBorrowed(e)
		if err != nil {
			return leafPred{}, true, err
		}
		if !isSlice || len(keys) == 0 {
			return leafPred{}, false, nil
		}

		posts, est, release := ov.lookupPostings(keys)
		if len(posts) == 0 {
			if release != nil {
				release()
			}
			return emptyLeaf(), true, nil
		}

		return leafPred{
			kind:    leafPredKindPostsUnion,
			posts:   posts,
			estCard: est,
			release: release,
		}, true, nil
	}

	return leafPred{}, false, nil
}

func (qv *queryView[K, V]) attachLeafPredPostingFilter(p *leafPred) {
	if p == nil {
		return
	}
	switch p.kind {
	case leafPredKindPostsConcat, leafPredKindPostsUnion:
	default:
		return
	}
	filter, cleanup := qv.buildPostsAnyPostingFilter(p.posts)
	p.postingFilter = filter
	if cleanup == nil {
		return
	}
	prev := p.release
	if prev == nil {
		p.release = cleanup
		return
	}
	p.release = func() {
		cleanup()
		prev()
	}
}

func pickLeadIndex(ps []leafPred) int {
	if len(ps) == 0 {
		return -1
	}
	best := -1
	for i := range ps {
		if !ps[i].hasIter() {
			continue
		}
		if best < 0 || ps[i].estCard < ps[best].estCard {
			best = i
		}
	}
	return best
}

func hasEmptyLeafPred(preds []leafPred) bool {
	for i := range preds {
		if preds[i].kind == leafPredKindEmpty {
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

func emptyLeaf() leafPred {
	return leafPred{
		kind:    leafPredKindEmpty,
		estCard: 0,
		release: nil,
	}
}

func releaseLeafPreds(preds []leafPred) {
	for i := range preds {
		if preds[i].release != nil {
			preds[i].release()
		}
	}
}

type emptyIter struct{}

func (emptyIter) HasNext() bool { return false }
func (emptyIter) Next() uint64  { return 0 }
func (emptyIter) Release()      {}

type postingConcatIter struct {
	posts []posting.List
	i     int
	curIt posting.Iterator
}

func newPostingConcatIter(posts []posting.List) posting.Iterator {
	return &postingConcatIter{posts: posts}
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
		if it.i >= len(it.posts) {
			return false
		}
		p := it.posts[it.i]
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
	it.i = 0
}

type postingUnionIter struct {
	posts []posting.List
	i     int
	curIt posting.Iterator
	seen  u64set
	next  uint64
	has   bool
}

type postingSmallUnionIter struct {
	posts []posting.List
	i     int
	curIt posting.Iterator
	next  uint64
	has   bool
}

type u64setPoolBuf struct {
	keys []uint64
	used []byte
}

var u64setPool = sync.Pool{
	New: func() any { return new(u64setPoolBuf) },
}

var postingUnionIterPool = sync.Pool{
	New: func() any { return new(postingUnionIter) },
}

func newPostingUnionIter(posts []posting.List) posting.Iterator {
	if len(posts) > 1 && len(posts) <= 3 {
		return &postingSmallUnionIter{posts: posts}
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

	it := postingUnionIterPool.Get().(*postingUnionIter)
	*it = postingUnionIter{
		posts: posts,
		seen:  newU64Set(capHint),
	}
	return it
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
					if u.posts[j].Contains(v) {
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
		if u.i >= len(u.posts) {
			return false
		}
		p := u.posts[u.i]
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
	if u.curIt != nil {
		u.curIt.Release()
		u.curIt = nil
	}
	u.posts = nil
	u.i = 0
	u.next = 0
	u.has = false
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
		if u.i >= len(u.posts) {
			return false
		}
		p := u.posts[u.i]
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
	releaseU64Set(&u.seen)
	u.posts = nil
	u.i = 0
	u.next = 0
	u.has = false
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
	n := 1
	for n < capHint*2 {
		n <<= 1
	}
	buf := u64setPool.Get().(*u64setPoolBuf)
	size := cap(buf.keys)
	if size < n || size != cap(buf.used) {
		buf.keys = make([]uint64, n)
		buf.used = make([]byte, n)
		size = n
	}
	return u64set{
		keys:   buf.keys[:size],
		used:   buf.used[:size],
		mask:   uint64(size - 1),
		pooled: buf,
	}
}

func releaseU64Set(s *u64set) {
	if s == nil {
		return
	}
	buf := s.pooled
	size := cap(s.keys)
	if buf != nil {
		if size > 0 && size == cap(s.used) && size <= u64SetPoolMaxCap {
			clear(s.used[:size])
			buf.keys = s.keys[:size]
			buf.used = s.used[:size]
			u64setPool.Put(buf)
		} else {
			buf.keys = nil
			buf.used = nil
			u64setPool.Put(buf)
		}
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
	old := &u64set{
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
	releaseU64Set(old)
}

func mix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}
