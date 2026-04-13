package rbi

import (
	"fmt"
	"strings"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/normalize"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

func (qv *queryView[K, V]) tryExecutionPlan(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	// Execution-plan fast paths support only single-column basic order.
	// Non-basic order types must stay on planner/postingResult routes.
	if len(q.Order) > 0 {
		if len(q.Order) != 1 || q.Order[0].Type != qx.OrderBasic {
			return nil, false, nil
		}
	}

	// optimized path for LIMIT without OFFSET
	if out, ok, plan, err := qv.tryLimitQuery(q, trace); ok {
		if trace != nil {
			trace.setPlan(plan)
		}
		return out, ok, err
	}

	// optimization for simple ORDER + LIMIT without complex filters (and OFFSET)
	if out, ok, err := qv.tryQueryOrderBasicWithLimit(q, trace); ok {
		if trace != nil {
			trace.setPlan(PlanLimitOrderBasic)
		}
		return out, ok, err
	}

	// optimization for PREFIX + ORDER + LIMIT without complex filters
	if out, ok, err := qv.tryQueryOrderPrefixWithLimit(q, trace); ok {
		if trace != nil {
			trace.setPlan(PlanLimitOrderPrefix)
		}
		return out, ok, err
	}

	// optimization for simple PREFIX + LIMIT without ORDER
	if out, ok, err := qv.tryQueryPrefixNoOrderWithLimit(q, trace); ok {
		if trace != nil {
			trace.setPlan(PlanLimitPrefixNoOrder)
		}
		return out, ok, err
	}

	// optimization for simple range + LIMIT without ORDER
	if out, ok, err := qv.tryQueryRangeNoOrderWithLimit(q, trace); ok {
		if trace != nil {
			trace.setPlan(PlanLimitRangeNoOrder)
		}
		return out, ok, err
	}

	return nil, false, nil
}

func (qv *queryView[K, V]) tryOrderBasicNoFilterWithLimit(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	if len(q.Order) != 1 || q.Limit == 0 || !normalize.IsTrueConst(q.Expr) {
		return nil, false, nil
	}

	order := q.Order[0]
	if order.Type != qx.OrderBasic {
		return nil, false, nil
	}

	fm := qv.fields[order.Field]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}

	fullBounds := rangeBounds{has: true}
	nilTailField := orderNilTailField(fm, order.Field, fullBounds)
	ov := qv.fieldOverlay(order.Field)
	if !ov.hasData() && nilTailField == "" {
		if !qv.hasIndexedField(order.Field) {
			return nil, false, nil
		}
		return nil, true, nil
	}

	br := ov.rangeForBounds(fullBounds)
	if overlayRangeEmpty(br) && nilTailField == "" {
		return nil, true, nil
	}

	out, _ := qv.scanOrderLimitNoPredicates(q, ov, br, order.Desc, nilTailField, trace)
	return out, true, nil
}

func emitAcceptedPostingNoOrder[K ~uint64 | ~string, V any](cursor *queryCursor[K, V], ids posting.List, examined *uint64) bool {
	if ids.IsEmpty() {
		return false
	}
	card := ids.Cardinality()
	*examined += card
	if cursor.skip >= card {
		cursor.skip -= card
		return false
	}
	if idx, ok := ids.TrySingle(); ok {
		return cursor.emit(idx)
	}
	it := ids.Iter()
	defer it.Release()
	for it.HasNext() {
		if cursor.emit(it.Next()) {
			return true
		}
	}
	return false
}

func predicatesMatchActiveReader(preds predicateReader, active []int, idx uint64) bool {
	switch len(active) {
	case 0:
		return true
	case 1:
		return preds.GetPtr(active[0]).matches(idx)
	case 2:
		return preds.GetPtr(active[0]).matches(idx) &&
			preds.GetPtr(active[1]).matches(idx)
	case 3:
		return preds.GetPtr(active[0]).matches(idx) &&
			preds.GetPtr(active[1]).matches(idx) &&
			preds.GetPtr(active[2]).matches(idx)
	case 4:
		return preds.GetPtr(active[0]).matches(idx) &&
			preds.GetPtr(active[1]).matches(idx) &&
			preds.GetPtr(active[2]).matches(idx) &&
			preds.GetPtr(active[3]).matches(idx)
	}
	for _, pi := range active {
		if !preds.GetPtr(pi).matches(idx) {
			return false
		}
	}
	return true
}

func orderPredicatesEmitCandidateReader[K ~uint64 | ~string, V any](
	cursor *queryCursor[K, V],
	preds predicateReader,
	checks []int,
	trace *queryTrace,
	idx uint64,
	examined *uint64,
) bool {
	*examined = *examined + 1
	if !predicatesMatchActiveReader(preds, checks, idx) {
		return false
	}
	if trace != nil {
		trace.addMatched(1)
	}
	return cursor.emit(idx)
}

func orderPredicatesEmitCandidateBufReader[K ~uint64 | ~string, V any](
	cursor *queryCursor[K, V],
	preds predicateReader,
	checks *pooled.SliceBuf[int],
	trace *queryTrace,
	idx uint64,
	examined *uint64,
) bool {
	*examined = *examined + 1
	if !predicatesMatchActiveBufReader(preds, checks, idx) {
		return false
	}
	if trace != nil {
		trace.addMatched(1)
	}
	return cursor.emit(idx)
}

func orderPredicatesEmitAcceptedPosting[K ~uint64 | ~string, V any](
	cursor *queryCursor[K, V],
	ids posting.List,
	card uint64,
	trace *queryTrace,
	examined *uint64,
) bool {
	*examined += card
	if trace != nil {
		trace.addMatched(card)
	}
	if cursor.skip >= card {
		cursor.skip -= card
		return false
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

func orderPredicatesTryBucketPostingReader[K ~uint64 | ~string, V any](
	cursor *queryCursor[K, V],
	preds predicateReader,
	exactActive []int,
	exactOnly bool,
	ids posting.List,
	exactWork posting.List,
	trace *queryTrace,
	examined *uint64,
) (current posting.List, exactApplied bool, handled bool, stop bool, nextExactWork posting.List) {
	nextExactWork = exactWork
	if ids.IsEmpty() {
		return posting.List{}, false, true, false, nextExactWork
	}
	if len(exactActive) == 0 {
		return ids, false, false, false, nextExactWork
	}
	card := ids.Cardinality()
	allowExact := plannerAllowExactBucketFilter(cursor.skip, cursor.need, card, exactOnly, len(exactActive))
	mode, exactIDs, updatedExactWork, _ := plannerFilterPostingByPredicateChecks(preds, exactActive, ids, exactWork, allowExact)
	nextExactWork = updatedExactWork
	switch mode {
	case plannerPredicateBucketEmpty:
		*examined += card
		return posting.List{}, false, true, false, nextExactWork
	case plannerPredicateBucketAll:
		if exactOnly {
			return exactIDs, true, true, orderPredicatesEmitAcceptedPosting(cursor, exactIDs, card, trace, examined), nextExactWork
		}
		return exactIDs, true, false, false, nextExactWork
	case plannerPredicateBucketExact:
		if trace != nil {
			trace.addPostingExactFilter(1)
		}
		if exactIDs.IsEmpty() {
			*examined += card
			return posting.List{}, true, true, false, nextExactWork
		}
		if exactOnly {
			return exactIDs, true, true, orderPredicatesEmitAcceptedPosting(cursor, exactIDs, exactIDs.Cardinality(), trace, examined), nextExactWork
		}
		return exactIDs, true, false, false, nextExactWork
	default:
		return ids, false, false, false, nextExactWork
	}
}

func orderPredicatesTryBucketPostingBufReader[K ~uint64 | ~string, V any](
	cursor *queryCursor[K, V],
	preds predicateReader,
	exactActive *pooled.SliceBuf[int],
	exactOnly bool,
	ids posting.List,
	exactWork posting.List,
	trace *queryTrace,
	examined *uint64,
) (current posting.List, exactApplied bool, handled bool, stop bool, nextExactWork posting.List) {
	nextExactWork = exactWork
	if ids.IsEmpty() {
		return posting.List{}, false, true, false, nextExactWork
	}
	if exactActive == nil || exactActive.Len() == 0 {
		return ids, false, false, false, nextExactWork
	}
	card := ids.Cardinality()
	allowExact := plannerAllowExactBucketFilter(cursor.skip, cursor.need, card, exactOnly, exactActive.Len())
	mode, exactIDs, updatedExactWork, _ := plannerFilterPostingByPredicateChecksBuf(preds, exactActive, ids, exactWork, allowExact)
	nextExactWork = updatedExactWork
	switch mode {
	case plannerPredicateBucketEmpty:
		*examined += card
		return posting.List{}, false, true, false, nextExactWork
	case plannerPredicateBucketAll:
		if exactOnly {
			return exactIDs, true, true, orderPredicatesEmitAcceptedPosting(cursor, exactIDs, card, trace, examined), nextExactWork
		}
		return exactIDs, true, false, false, nextExactWork
	case plannerPredicateBucketExact:
		if trace != nil {
			trace.addPostingExactFilter(1)
		}
		if exactIDs.IsEmpty() {
			*examined += card
			return posting.List{}, true, true, false, nextExactWork
		}
		if exactOnly {
			return exactIDs, true, true, orderPredicatesEmitAcceptedPosting(cursor, exactIDs, exactIDs.Cardinality(), trace, examined), nextExactWork
		}
		return exactIDs, true, false, false, nextExactWork
	default:
		return ids, false, false, false, nextExactWork
	}
}

func orderPredicatesEmitPostingReader[K ~uint64 | ~string, V any](
	cursor *queryCursor[K, V],
	preds predicateReader,
	active []int,
	exactActive []int,
	residualActive []int,
	exactOnly bool,
	ids posting.List,
	exactWork posting.List,
	trace *queryTrace,
	examined *uint64,
) (bool, posting.List) {
	if ids.IsEmpty() {
		return false, exactWork
	}
	if idx, ok := ids.TrySingle(); ok {
		return orderPredicatesEmitCandidateReader(cursor, preds, active, trace, idx, examined), exactWork
	}

	currentIDs := ids
	iterSrc := ids.Iter()
	exactApplied := false
	if current, applied, handled, stop, nextExactWork := orderPredicatesTryBucketPostingReader(
		cursor,
		preds,
		exactActive,
		exactOnly,
		ids,
		exactWork,
		trace,
		examined,
	); handled {
		iterSrc.Release()
		return stop, nextExactWork
	} else if applied && !current.IsEmpty() {
		currentIDs = current
		iterSrc.Release()
		iterSrc = current.Iter()
		exactApplied = true
		exactWork = nextExactWork
	} else {
		exactWork = nextExactWork
	}

	checks := active
	singleCheck := -1
	if exactApplied {
		checks = residualActive
		if len(residualActive) == 1 {
			singleCheck = residualActive[0]
		}
	} else if len(active) == 1 {
		singleCheck = active[0]
	}
	if orderedSkipSingleCheckBucket(cursor, preds, singleCheck, currentIDs, trace) {
		*examined += currentIDs.Cardinality()
		iterSrc.Release()
		return false, exactWork
	}
	for iterSrc.HasNext() {
		if orderPredicatesEmitCandidateReader(cursor, preds, checks, trace, iterSrc.Next(), examined) {
			iterSrc.Release()
			return true, exactWork
		}
	}
	iterSrc.Release()
	return false, exactWork
}

func orderPredicatesEmitPostingBufReader[K ~uint64 | ~string, V any](
	cursor *queryCursor[K, V],
	preds predicateReader,
	active *pooled.SliceBuf[int],
	exactActive *pooled.SliceBuf[int],
	residualActive *pooled.SliceBuf[int],
	exactOnly bool,
	ids posting.List,
	exactWork posting.List,
	trace *queryTrace,
	examined *uint64,
) (bool, posting.List) {
	if ids.IsEmpty() {
		return false, exactWork
	}
	if idx, ok := ids.TrySingle(); ok {
		return orderPredicatesEmitCandidateBufReader(cursor, preds, active, trace, idx, examined), exactWork
	}

	currentIDs := ids
	iterSrc := ids.Iter()
	exactApplied := false
	if current, applied, handled, stop, nextExactWork := orderPredicatesTryBucketPostingBufReader(
		cursor,
		preds,
		exactActive,
		exactOnly,
		ids,
		exactWork,
		trace,
		examined,
	); handled {
		iterSrc.Release()
		return stop, nextExactWork
	} else if applied && !current.IsEmpty() {
		currentIDs = current
		iterSrc.Release()
		iterSrc = current.Iter()
		exactApplied = true
		exactWork = nextExactWork
	} else {
		exactWork = nextExactWork
	}

	checks := active
	singleCheck := -1
	if exactApplied {
		checks = residualActive
		if residualActive.Len() == 1 {
			singleCheck = residualActive.Get(0)
		}
	} else if active.Len() == 1 {
		singleCheck = active.Get(0)
	}
	if orderedSkipSingleCheckBucket(cursor, preds, singleCheck, currentIDs, trace) {
		*examined += currentIDs.Cardinality()
		iterSrc.Release()
		return false, exactWork
	}
	for iterSrc.HasNext() {
		if orderPredicatesEmitCandidateBufReader(cursor, preds, checks, trace, iterSrc.Next(), examined) {
			iterSrc.Release()
			return true, exactWork
		}
	}
	iterSrc.Release()
	return false, exactWork
}

func releaseOrderBasicResidualState(preds predicateSet, activeBuf *pooled.SliceBuf[int]) {
	if activeBuf != nil {
		predicateCheckSlicePool.Put(activeBuf)
	}
	preds.Release()
}

func emitOrderLimitPosting[K ~uint64 | ~string, V any](
	cursor *queryCursor[K, V],
	ids posting.List,
	examined *uint64,
	trace *queryTrace,
) bool {
	if ids.IsEmpty() {
		return false
	}
	if idx, ok := ids.TrySingle(); ok {
		*examined += 1
		if trace != nil {
			trace.addMatched(1)
		}
		return cursor.emit(idx)
	}
	card := ids.Cardinality()
	if cursor.skip >= card {
		cursor.skip -= card
		*examined += card
		if trace != nil {
			trace.addMatched(card)
		}
		return false
	}
	it := ids.Iter()
	for it.HasNext() {
		idx := it.Next()
		*examined += 1
		if trace != nil {
			trace.addMatched(1)
		}
		if cursor.emit(idx) {
			it.Release()
			return true
		}
	}
	it.Release()
	return false
}

func emitOrderPrefixPostingByBase[K ~uint64 | ~string, V any](
	cursor *queryCursor[K, V],
	ids posting.List,
	base postingResult,
	baseBM posting.List,
	baseNegUniverse bool,
	examined *uint64,
) bool {
	if ids.IsEmpty() {
		return false
	}
	if idx, ok := ids.TrySingle(); ok {
		*examined += 1
		if base.neg {
			if !baseNegUniverse && baseBM.Contains(idx) {
				return false
			}
			return cursor.emit(idx)
		}
		if !baseBM.Contains(idx) {
			return false
		}
		return cursor.emit(idx)
	}
	it := ids.Iter()
	for it.HasNext() {
		idx := it.Next()
		*examined += 1
		if base.neg {
			if !baseNegUniverse && baseBM.Contains(idx) {
				continue
			}
		} else if !baseBM.Contains(idx) {
			continue
		}
		if cursor.emit(idx) {
			it.Release()
			return true
		}
	}
	it.Release()
	return false
}

func predicatesMatchActiveBufReader(preds predicateReader, active *pooled.SliceBuf[int], idx uint64) bool {
	switch active.Len() {
	case 0:
		return true
	case 1:
		return preds.GetPtr(active.Get(0)).matches(idx)
	case 2:
		return preds.GetPtr(active.Get(0)).matches(idx) &&
			preds.GetPtr(active.Get(1)).matches(idx)
	case 3:
		return preds.GetPtr(active.Get(0)).matches(idx) &&
			preds.GetPtr(active.Get(1)).matches(idx) &&
			preds.GetPtr(active.Get(2)).matches(idx)
	case 4:
		return preds.GetPtr(active.Get(0)).matches(idx) &&
			preds.GetPtr(active.Get(1)).matches(idx) &&
			preds.GetPtr(active.Get(2)).matches(idx) &&
			preds.GetPtr(active.Get(3)).matches(idx)
	}
	for i := 0; i < active.Len(); i++ {
		if !preds.GetPtr(active.Get(i)).matches(idx) {
			return false
		}
	}
	return true
}

func orderBasicResidualMatchesReader(preds predicateReader, active []int, idx uint64) bool {
	if len(active) == 0 {
		return true
	}
	return predicatesMatchActiveReader(preds, active, idx)
}

func orderBasicResidualMatchesBufReader(preds predicateReader, active *pooled.SliceBuf[int], idx uint64) bool {
	if active == nil || active.Len() == 0 {
		return true
	}
	return predicatesMatchActiveBufReader(preds, active, idx)
}

func orderBasicContainsReader(preds predicateReader, active []int, baseBM posting.List, baseNeg bool, baseNegUniverse bool, idx uint64) bool {
	if baseNeg {
		if baseNegUniverse {
			return orderBasicResidualMatchesReader(preds, active, idx)
		}
		if baseBM.Contains(idx) {
			return false
		}
	} else if !baseBM.Contains(idx) {
		return false
	}
	return orderBasicResidualMatchesReader(preds, active, idx)
}

func orderBasicContainsBufReader(preds predicateReader, active *pooled.SliceBuf[int], baseBM posting.List, baseNeg bool, baseNegUniverse bool, idx uint64) bool {
	if baseNeg {
		if baseNegUniverse {
			return orderBasicResidualMatchesBufReader(preds, active, idx)
		}
		if baseBM.Contains(idx) {
			return false
		}
	} else if !baseBM.Contains(idx) {
		return false
	}
	return orderBasicResidualMatchesBufReader(preds, active, idx)
}

func orderBasicEmitFilteredPostingReader[K ~uint64 | ~string, V any](
	cursor *queryCursor[K, V],
	preds predicateReader,
	active []int,
	baseBM posting.List,
	baseNeg bool,
	baseNegUniverse bool,
	ids posting.List,
	tmp posting.List,
	examined *uint64,
) (bool, posting.List) {
	if ids.IsEmpty() {
		return false, tmp
	}

	if card := ids.Cardinality(); card <= iteratorThreshold || (0 < cursor.need && cursor.need < 1000) {
		if idx, ok := ids.TrySingle(); ok {
			*examined++
			return orderBasicContainsReader(preds, active, baseBM, baseNeg, baseNegUniverse, idx) && cursor.emit(idx), tmp
		}

		it := ids.Iter()
		for it.HasNext() {
			idx := it.Next()
			*examined++
			if !orderBasicContainsReader(preds, active, baseBM, baseNeg, baseNegUniverse, idx) {
				continue
			}
			if cursor.emit(idx) {
				it.Release()
				return true, tmp
			}
		}
		it.Release()
		return false, tmp
	}

	*examined += ids.Cardinality()
	nextTmp := tmp
	if baseNeg {
		nextTmp.Release()
		nextTmp = ids.Clone()
		if !baseNegUniverse {
			nextTmp = nextTmp.BuildAndNot(baseBM)
		}
	} else {
		nextTmp = tmpIntersectPosting(nextTmp, ids, baseBM)
	}
	if nextTmp.IsEmpty() {
		return false, nextTmp
	}
	if len(active) == 0 {
		return cursor.emitPosting(nextTmp), nextTmp
	}
	it := nextTmp.Iter()
	for it.HasNext() {
		idx := it.Next()
		if orderBasicResidualMatchesReader(preds, active, idx) && cursor.emit(idx) {
			it.Release()
			return true, nextTmp
		}
	}
	it.Release()
	return false, nextTmp
}

func orderBasicEmitFilteredPostingBufReader[K ~uint64 | ~string, V any](
	cursor *queryCursor[K, V],
	preds predicateReader,
	active *pooled.SliceBuf[int],
	baseBM posting.List,
	baseNeg bool,
	baseNegUniverse bool,
	ids posting.List,
	tmp posting.List,
	examined *uint64,
) (bool, posting.List) {
	if ids.IsEmpty() {
		return false, tmp
	}

	if card := ids.Cardinality(); card <= iteratorThreshold || (0 < cursor.need && cursor.need < 1000) {
		if idx, ok := ids.TrySingle(); ok {
			*examined++
			return orderBasicContainsBufReader(preds, active, baseBM, baseNeg, baseNegUniverse, idx) && cursor.emit(idx), tmp
		}

		it := ids.Iter()
		for it.HasNext() {
			idx := it.Next()
			*examined++
			if !orderBasicContainsBufReader(preds, active, baseBM, baseNeg, baseNegUniverse, idx) {
				continue
			}
			if cursor.emit(idx) {
				it.Release()
				return true, tmp
			}
		}
		it.Release()
		return false, tmp
	}

	*examined += ids.Cardinality()
	nextTmp := tmp
	if baseNeg {
		nextTmp.Release()
		nextTmp = ids.Clone()
		if !baseNegUniverse {
			nextTmp = nextTmp.BuildAndNot(baseBM)
		}
	} else {
		nextTmp = tmpIntersectPosting(nextTmp, ids, baseBM)
	}
	if nextTmp.IsEmpty() {
		return false, nextTmp
	}
	if active == nil || active.Len() == 0 {
		return cursor.emitPosting(nextTmp), nextTmp
	}
	it := nextTmp.Iter()
	for it.HasNext() {
		idx := it.Next()
		if orderBasicResidualMatchesBufReader(preds, active, idx) && cursor.emit(idx) {
			it.Release()
			return true, nextTmp
		}
	}
	it.Release()
	return false, nextTmp
}

func (qv *queryView[K, V]) runOrderBasicBaseQuery(
	q *qx.QX,
	orderField string,
	baseOps []qx.Expr,
	needWindow int,
	order qx.Order,
	ov fieldOverlay,
	br overlayRange,
	nilTailField string,
	base postingResult,
	residualPreds predicateReader,
	residualActive []int,
	trace *queryTrace,
) ([]K, bool, error) {
	defer base.release()

	baseBM := base.ids
	baseNegUniverse := base.neg && baseBM.IsEmpty()
	if !base.neg && baseBM.IsEmpty() {
		return nil, true, nil
	}

	skip := q.Offset
	need := q.Limit
	out := make([]K, 0, need)
	cursor := qv.newQueryCursor(out, skip, need, false, 0)

	var tmp posting.List

	keyCur := ov.newCursor(br, order.Desc)
	var (
		examined  uint64
		scanWidth uint64
	)
	nilOV := qv.nilFieldOverlay(nilTailField)
	for {
		_, ids, ok := keyCur.next()
		if !ok {
			break
		}
		if ids.IsEmpty() {
			continue
		}
		scanWidth++
		stop, nextTmp := orderBasicEmitFilteredPostingReader(
			&cursor,
			residualPreds,
			residualActive,
			baseBM,
			base.neg,
			baseNegUniverse,
			ids,
			tmp,
			&examined,
		)
		tmp = nextTmp
		if stop {
			trace.addExamined(examined)
			trace.addOrderScanWidth(scanWidth)
			trace.setEarlyStopReason("limit_reached")
			qv.promoteOrderBasicLimitMaterializedBaseOps(orderField, baseOps, examined, uint64(needWindow))
			tmp.Release()
			return cursor.out, true, nil
		}
	}

	if nilTailField != "" {
		ids := nilOV.lookupPostingRetained(nilIndexEntryKey)
		if !ids.IsEmpty() {
			scanWidth++
			stop, nextTmp := orderBasicEmitFilteredPostingReader(
				&cursor,
				residualPreds,
				residualActive,
				baseBM,
				base.neg,
				baseNegUniverse,
				ids,
				tmp,
				&examined,
			)
			tmp = nextTmp
			if stop {
				trace.addExamined(examined)
				trace.addOrderScanWidth(scanWidth)
				trace.setEarlyStopReason("limit_reached")
				qv.promoteOrderBasicLimitMaterializedBaseOps(orderField, baseOps, examined, uint64(needWindow))
				tmp.Release()
				return cursor.out, true, nil
			}
		}
	}

	trace.addExamined(examined)
	trace.addOrderScanWidth(scanWidth)
	trace.setEarlyStopReason("input_exhausted")
	qv.promoteOrderBasicLimitMaterializedBaseOps(orderField, baseOps, examined, uint64(needWindow))
	tmp.Release()
	return cursor.out, true, nil
}

func (qv *queryView[K, V]) runOrderBasicBaseQueryBuf(
	q *qx.QX,
	orderField string,
	baseOps []qx.Expr,
	needWindow int,
	order qx.Order,
	ov fieldOverlay,
	br overlayRange,
	nilTailField string,
	base postingResult,
	residualPreds predicateReader,
	residualActive *pooled.SliceBuf[int],
	trace *queryTrace,
) ([]K, bool, error) {
	defer base.release()

	baseBM := base.ids
	baseNegUniverse := base.neg && baseBM.IsEmpty()
	if !base.neg && baseBM.IsEmpty() {
		return nil, true, nil
	}

	skip := q.Offset
	need := q.Limit
	out := make([]K, 0, need)
	cursor := qv.newQueryCursor(out, skip, need, false, 0)

	var tmp posting.List

	keyCur := ov.newCursor(br, order.Desc)
	var (
		examined  uint64
		scanWidth uint64
	)
	nilOV := qv.nilFieldOverlay(nilTailField)
	for {
		_, ids, ok := keyCur.next()
		if !ok {
			break
		}
		if ids.IsEmpty() {
			continue
		}
		scanWidth++
		stop, nextTmp := orderBasicEmitFilteredPostingBufReader(
			&cursor,
			residualPreds,
			residualActive,
			baseBM,
			base.neg,
			baseNegUniverse,
			ids,
			tmp,
			&examined,
		)
		tmp = nextTmp
		if stop {
			trace.addExamined(examined)
			trace.addOrderScanWidth(scanWidth)
			trace.setEarlyStopReason("limit_reached")
			qv.promoteOrderBasicLimitMaterializedBaseOps(orderField, baseOps, examined, uint64(needWindow))
			tmp.Release()
			return cursor.out, true, nil
		}
	}

	if nilTailField != "" {
		ids := nilOV.lookupPostingRetained(nilIndexEntryKey)
		if !ids.IsEmpty() {
			scanWidth++
			stop, nextTmp := orderBasicEmitFilteredPostingBufReader(
				&cursor,
				residualPreds,
				residualActive,
				baseBM,
				base.neg,
				baseNegUniverse,
				ids,
				tmp,
				&examined,
			)
			tmp = nextTmp
			if stop {
				trace.addExamined(examined)
				trace.addOrderScanWidth(scanWidth)
				trace.setEarlyStopReason("limit_reached")
				qv.promoteOrderBasicLimitMaterializedBaseOps(orderField, baseOps, examined, uint64(needWindow))
				tmp.Release()
				return cursor.out, true, nil
			}
		}
	}

	trace.addExamined(examined)
	trace.addOrderScanWidth(scanWidth)
	trace.setEarlyStopReason("input_exhausted")
	qv.promoteOrderBasicLimitMaterializedBaseOps(orderField, baseOps, examined, uint64(needWindow))
	tmp.Release()
	return cursor.out, true, nil
}

type rangeBounds struct {
	has bool

	empty bool

	hasLo     bool
	loKey     string
	loIndex   indexKey
	loNumeric bool
	loInc     bool

	hasHi     bool
	hiKey     string
	hiIndex   indexKey
	hiNumeric bool
	hiInc     bool

	hasPrefix bool
	prefix    string
}

func compareRangeBoundKeys(
	aKey string,
	aIndex indexKey,
	aNumeric bool,
	bKey string,
	bIndex indexKey,
	bNumeric bool,
) int {
	if aNumeric != bNumeric {
		panic("rbi: mixed range bound key representations")
	}
	if aNumeric {
		return compareIndexKeys(aIndex, bIndex)
	}
	return strings.Compare(aKey, bKey)
}

func (rb *rangeBounds) setEmpty() {
	rb.has = true
	rb.empty = true
}

func (rb *rangeBounds) normalize() {
	if rb.empty {
		return
	}

	if rb.hasLo && rb.hasHi {
		cmp := compareRangeBoundKeys(rb.loKey, rb.loIndex, rb.loNumeric, rb.hiKey, rb.hiIndex, rb.hiNumeric)
		if cmp > 0 || (cmp == 0 && (!rb.loInc || !rb.hiInc)) {
			rb.setEmpty()
			return
		}
	}

	if !rb.hasPrefix {
		return
	}

	if rb.hasHi {
		cmp := strings.Compare(rb.hiKey, rb.prefix)
		if rb.hiNumeric {
			cmp = compareIndexKeyString(rb.hiIndex, rb.prefix)
		}
		if cmp < 0 || (cmp == 0 && !rb.hiInc) {
			rb.setEmpty()
			return
		}
	}

	if rb.hasLo {
		if upper, ok := newPrefixUpperBound(rb.prefix); ok {
			cmp := compareStringPrefixUpperBound(rb.loKey, upper)
			if rb.loNumeric {
				cmp = compareIndexKeyPrefixUpperBound(rb.loIndex, upper)
			}
			if cmp >= 0 {
				rb.setEmpty()
			}
		}
	}
}

func (rb *rangeBounds) applyLo(key string, inc bool) {
	if rb.empty {
		return
	}
	if !rb.hasLo || rb.loKey < key || (rb.loKey == key && !rb.loInc && inc) {
		rb.hasLo = true
		rb.loKey = key
		rb.loIndex = indexKey{}
		rb.loNumeric = false
		rb.loInc = inc
	}
	rb.normalize()
}

func (rb *rangeBounds) applyLoIndex(key indexKey, inc bool) {
	if rb.empty {
		return
	}
	if !rb.hasLo || compareIndexKeys(rb.loIndex, key) < 0 || (compareIndexKeys(rb.loIndex, key) == 0 && !rb.loInc && inc) {
		rb.hasLo = true
		rb.loKey = ""
		rb.loIndex = key
		rb.loNumeric = true
		rb.loInc = inc
	}
	rb.normalize()
}

func (rb *rangeBounds) applyHi(key string, inc bool) {
	if rb.empty {
		return
	}
	if !rb.hasHi || rb.hiKey > key || (rb.hiKey == key && !rb.hiInc && inc) {
		rb.hasHi = true
		rb.hiKey = key
		rb.hiIndex = indexKey{}
		rb.hiNumeric = false
		rb.hiInc = inc
	}
	rb.normalize()
}

func (rb *rangeBounds) applyHiIndex(key indexKey, inc bool) {
	if rb.empty {
		return
	}
	if !rb.hasHi || compareIndexKeys(rb.hiIndex, key) > 0 || (compareIndexKeys(rb.hiIndex, key) == 0 && !rb.hiInc && inc) {
		rb.hasHi = true
		rb.hiKey = ""
		rb.hiIndex = key
		rb.hiNumeric = true
		rb.hiInc = inc
	}
	rb.normalize()
}

func (rb *rangeBounds) applyPrefix(prefix string) {
	if rb.empty {
		return
	}
	if rb.hasPrefix {
		switch {
		case strings.HasPrefix(rb.prefix, prefix):
		case strings.HasPrefix(prefix, rb.prefix):
			rb.prefix = prefix
		default:
			rb.setEmpty()
			return
		}
	} else {
		rb.hasPrefix = true
		rb.prefix = prefix
	}
	rb.normalize()
}

func (rb *rangeBounds) applyOp(op qx.Op, key string) bool {
	rb.has = true
	switch op {
	case qx.OpGT:
		rb.applyLo(key, false)
	case qx.OpGTE:
		rb.applyLo(key, true)
	case qx.OpLT:
		rb.applyHi(key, false)
	case qx.OpLTE:
		rb.applyHi(key, true)
	case qx.OpEQ:
		rb.applyLo(key, true)
		rb.applyHi(key, true)
	case qx.OpPREFIX:
		rb.applyPrefix(key)
	default:
		return false
	}
	return true
}

func orderNilTailField(fm *field, field string, bounds rangeBounds) string {
	if fm == nil || !fm.Ptr || field == "" {
		return ""
	}
	if bounds.empty || bounds.hasLo || bounds.hasHi || bounds.hasPrefix {
		return ""
	}
	return field
}

func lowerBoundIndex(s []index, key string) int {
	lo, hi := 0, len(s)
	for lo < hi {
		mid := (lo + hi) >> 1
		if compareIndexKeyString(s[mid].Key, key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func lowerBoundIndexKey(s []index, key indexKey) int {
	return lowerBoundIndexEntriesKey(s, key)
}

func upperBoundIndex(s []index, key string) int {
	lo, hi := 0, len(s)
	for lo < hi {
		mid := (lo + hi) >> 1
		if compareIndexKeyString(s[mid].Key, key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func upperBoundIndexKey(s []index, key indexKey) int {
	return upperBoundIndexEntriesKey(s, key)
}

func prefixRangeEndIndex(s []index, prefix string, start int) int {
	if start < 0 || start >= len(s) {
		return start
	}
	if compareIndexKeyString(s[start].Key, prefix) < 0 || !indexKeyHasPrefixString(s[start].Key, prefix) {
		return start
	}
	upper, ok := newPrefixUpperBound(prefix)
	if !ok {
		return len(s)
	}

	lo, hi := start, len(s)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if compareIndexKeyPrefixUpperBound(s[mid].Key, upper) >= 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

const iteratorThreshold = 2048 // 256

type orderBasicBaseCoreKind uint8

const (
	orderBasicBaseCoreRawExpr orderBasicBaseCoreKind = iota
	orderBasicBaseCoreCollapsedRange
)

type orderBasicBaseCore struct {
	kind      orderBasicBaseCoreKind
	expr      qx.Expr
	collapsed preparedScalarExactRange
}

func isOrderBasicCollapsibleNumericRangeExpr(op qx.Expr, fm *field) bool {
	if op.Not || op.Field == "" || fm == nil || fm.Slice || !isNumericScalarKind(fm.Kind) {
		return false
	}
	return isScalarRangeEqOp(op.Op)
}

func (qv *queryView[K, V]) shouldCollapseOrderBasicNumericRange(field string, rb rangeBounds) bool {
	if field == "" || rb.empty {
		return false
	}
	ov := qv.fieldOverlay(field)
	if !ov.hasData() {
		return true
	}
	br := ov.rangeForBounds(rb)
	if overlayRangeEmpty(br) {
		return true
	}
	bucketCount, est := overlayRangeStats(ov, br)
	if bucketCount == 0 || est == 0 {
		return true
	}
	universe := qv.snapshotUniverseCardinality()
	if universe == 0 {
		return false
	}
	positiveWork := rangeProbeMaterializeWork(bucketCount, est)
	if positiveWork == 0 {
		return false
	}
	complementBuckets := ov.keyCount() - bucketCount
	if qv.nilFieldOverlay(field).lookupCardinality(nilIndexEntryKey) > 0 {
		complementBuckets++
	}
	complementEst := uint64(0)
	if universe > est {
		complementEst = universe - est
	}
	complementWork := rangeProbeMaterializeWork(complementBuckets, complementEst)
	if complementWork == 0 {
		return false
	}
	return positiveWork <= complementWork
}

func (qv *queryView[K, V]) prepareOrderBasicBaseCores(baseOps []qx.Expr) (*pooled.SliceBuf[orderBasicBaseCore], *pooled.SliceBuf[int], bool, error) {
	if len(baseOps) == 0 {
		return nil, nil, false, nil
	}
	coresBuf := orderBasicBaseCoreSlicePool.Get()
	coresBuf.Grow(len(baseOps))
	rawCoreIdxBuf := orderBasicBaseCoreIndexSlicePool.Get()
	rawCoreIdxBuf.SetLen(len(baseOps))
	for i := 0; i < rawCoreIdxBuf.Len(); i++ {
		rawCoreIdxBuf.Set(i, -1)
	}

	for i, op := range baseOps {
		if rawCoreIdxBuf.Get(i) >= 0 {
			continue
		}
		fm := qv.fields[op.Field]
		if !isOrderBasicCollapsibleNumericRangeExpr(op, fm) {
			continue
		}

		var rb rangeBounds
		groupCount := 0
		for j := i; j < len(baseOps); j++ {
			if rawCoreIdxBuf.Get(j) >= 0 {
				continue
			}
			other := baseOps[j]
			if other.Field != op.Field || !isOrderBasicCollapsibleNumericRangeExpr(other, fm) {
				continue
			}
			nextRB, ok, err := qv.rangeBoundsForScalarExpr(other)
			if err != nil {
				orderBasicBaseCoreSlicePool.Put(coresBuf)
				orderBasicBaseCoreIndexSlicePool.Put(rawCoreIdxBuf)
				return nil, nil, false, err
			}
			if !ok {
				continue
			}
			mergeRangeBounds(&rb, nextRB)
			groupCount++
		}
		if groupCount <= 1 {
			continue
		}
		if rb.empty {
			orderBasicBaseCoreSlicePool.Put(coresBuf)
			orderBasicBaseCoreIndexSlicePool.Put(rawCoreIdxBuf)
			return nil, nil, true, nil
		}
		if !qv.shouldCollapseOrderBasicNumericRange(op.Field, rb) {
			continue
		}

		coreIdx := coresBuf.Len()
		coresBuf.Append(orderBasicBaseCore{
			kind: orderBasicBaseCoreCollapsedRange,
			collapsed: preparedScalarExactRange{
				field:    op.Field,
				bounds:   rb,
				cacheKey: qv.materializedPredKeyForExactScalarRange(op.Field, rb),
			},
		})
		for j := i; j < len(baseOps); j++ {
			other := baseOps[j]
			if other.Field == op.Field && isOrderBasicCollapsibleNumericRangeExpr(other, fm) {
				rawCoreIdxBuf.Set(j, coreIdx)
			}
		}
	}

	for i, op := range baseOps {
		if rawCoreIdxBuf.Get(i) >= 0 {
			continue
		}
		rawCoreIdxBuf.Set(i, coresBuf.Len())
		coresBuf.Append(orderBasicBaseCore{
			kind: orderBasicBaseCoreRawExpr,
			expr: op,
		})
	}
	return coresBuf, rawCoreIdxBuf, false, nil
}

func (qv *queryView[K, V]) prepareOrderBasicBaseCoresBuf(baseOps *pooled.SliceBuf[qx.Expr]) (*pooled.SliceBuf[orderBasicBaseCore], *pooled.SliceBuf[int], bool, error) {
	if baseOps == nil || baseOps.Len() == 0 {
		return nil, nil, false, nil
	}
	coresBuf := orderBasicBaseCoreSlicePool.Get()
	coresBuf.Grow(baseOps.Len())
	rawCoreIdxBuf := orderBasicBaseCoreIndexSlicePool.Get()
	rawCoreIdxBuf.SetLen(baseOps.Len())
	for i := 0; i < rawCoreIdxBuf.Len(); i++ {
		rawCoreIdxBuf.Set(i, -1)
	}

	for i := 0; i < baseOps.Len(); i++ {
		if rawCoreIdxBuf.Get(i) >= 0 {
			continue
		}
		op := baseOps.Get(i)
		fm := qv.fields[op.Field]
		if !isOrderBasicCollapsibleNumericRangeExpr(op, fm) {
			continue
		}

		var rb rangeBounds
		groupCount := 0
		for j := i; j < baseOps.Len(); j++ {
			if rawCoreIdxBuf.Get(j) >= 0 {
				continue
			}
			other := baseOps.Get(j)
			if other.Field != op.Field || !isOrderBasicCollapsibleNumericRangeExpr(other, fm) {
				continue
			}
			nextRB, ok, err := qv.rangeBoundsForScalarExpr(other)
			if err != nil {
				orderBasicBaseCoreSlicePool.Put(coresBuf)
				orderBasicBaseCoreIndexSlicePool.Put(rawCoreIdxBuf)
				return nil, nil, false, err
			}
			if !ok {
				continue
			}
			mergeRangeBounds(&rb, nextRB)
			groupCount++
		}
		if groupCount <= 1 {
			continue
		}
		if rb.empty {
			orderBasicBaseCoreSlicePool.Put(coresBuf)
			orderBasicBaseCoreIndexSlicePool.Put(rawCoreIdxBuf)
			return nil, nil, true, nil
		}
		if !qv.shouldCollapseOrderBasicNumericRange(op.Field, rb) {
			continue
		}

		coreIdx := coresBuf.Len()
		coresBuf.Append(orderBasicBaseCore{
			kind: orderBasicBaseCoreCollapsedRange,
			collapsed: preparedScalarExactRange{
				field:    op.Field,
				bounds:   rb,
				cacheKey: qv.materializedPredKeyForExactScalarRange(op.Field, rb),
			},
		})
		for j := i; j < baseOps.Len(); j++ {
			other := baseOps.Get(j)
			if other.Field == op.Field && isOrderBasicCollapsibleNumericRangeExpr(other, fm) {
				rawCoreIdxBuf.Set(j, coreIdx)
			}
		}
	}

	for i := 0; i < baseOps.Len(); i++ {
		if rawCoreIdxBuf.Get(i) >= 0 {
			continue
		}
		rawCoreIdxBuf.Set(i, coresBuf.Len())
		coresBuf.Append(orderBasicBaseCore{
			kind: orderBasicBaseCoreRawExpr,
			expr: baseOps.Get(i),
		})
	}
	return coresBuf, rawCoreIdxBuf, false, nil
}

func (qv *queryView[K, V]) loadWarmOrderBasicBaseCore(core orderBasicBaseCore) (postingResult, bool) {
	switch core.kind {
	case orderBasicBaseCoreCollapsedRange:
		return qv.loadWarmPreparedScalarExactRange(core.collapsed)
	case orderBasicBaseCoreRawExpr:
		return qv.loadWarmOrderBasicRawBaseOp(core.expr)
	default:
		return postingResult{}, false
	}
}

func (qv *queryView[K, V]) evalOrderBasicBaseCore(core orderBasicBaseCore) (postingResult, error) {
	switch core.kind {
	case orderBasicBaseCoreCollapsedRange:
		return qv.evalPreparedScalarExactRange(core.collapsed)
	case orderBasicBaseCoreRawExpr:
		return qv.evalOrderBasicRawBaseOp(core.expr)
	default:
		return postingResult{}, nil
	}
}

func (qv *queryView[K, V]) shouldPromoteObservedOrderBasicBaseCore(
	orderField string,
	core orderBasicBaseCore,
	universe uint64,
	observedRows uint64,
	needWindow uint64,
) bool {
	switch core.kind {
	case orderBasicBaseCoreCollapsedRange:
		return qv.shouldPromoteObservedPreparedScalarExactRange(core.collapsed, observedRows, needWindow)
	case orderBasicBaseCoreRawExpr:
		if qv.snap.materializedPredCacheLimit() <= 0 {
			return false
		}
		return qv.shouldPromoteObservedOrderBasicRawBaseOp(
			orderField,
			core.expr,
			universe,
			observedRows,
			needWindow,
		)
	default:
		return false
	}
}

func (qv *queryView[K, V]) promoteObservedOrderBasicBaseCore(core orderBasicBaseCore) {
	switch core.kind {
	case orderBasicBaseCoreCollapsedRange:
		cacheKey := core.collapsed.cacheKey
		if !cacheKey.isZero() {
			if _, ok := qv.snap.loadMaterializedPredKey(cacheKey); ok {
				return
			}
		}
		ids, err := qv.evalPreparedScalarExactRange(core.collapsed)
		if err == nil {
			ids.release()
		}
	case orderBasicBaseCoreRawExpr:
		stats, ok := qv.orderBasicRawBaseOpStats(core.expr, qv.snapshotUniverseCardinality())
		cacheKey := materializedPredKey{}
		if ok {
			cacheKey = stats.cacheKey
		}
		if cacheKey.isZero() {
			return
		}
		if _, ok := qv.snap.loadMaterializedPredKey(cacheKey); ok {
			return
		}
		scalarKey := qv.materializedPredKey(core.expr)
		if cacheKey == scalarKey {
			ids := qv.evalLazyMaterializedPredicateWithKey(core.expr, cacheKey)
			ids.Release()
			return
		}
		qv.materializeOrderBasicLimitComplementBaseOp(core.expr, cacheKey)
	}
}

func (qv *queryView[K, V]) hasWarmOrderBasicBaseCores(cores *pooled.SliceBuf[orderBasicBaseCore]) bool {
	if cores == nil {
		return false
	}
	for i := 0; i < cores.Len(); i++ {
		if hit, ok := qv.loadWarmOrderBasicBaseCore(cores.Get(i)); ok {
			hit.release()
			return true
		}
	}
	return false
}

func (qv *queryView[K, V]) orderBasicRawBaseOpStats(
	op qx.Expr,
	universe uint64,
) (scalarMaterializationStats, bool) {
	if !isSimpleScalarRangeOrPrefixLeaf(op) {
		return scalarMaterializationStats{}, false
	}

	candidate, ok := qv.prepareScalarRangeRoutingCandidate(op)
	if !ok {
		return scalarMaterializationStats{}, false
	}
	return candidate.core.orderBasicMaterializationStats(universe)
}

func (qv *queryView[K, V]) shouldPromoteObservedOrderBasicRawBaseOp(
	orderField string,
	op qx.Expr,
	universe uint64,
	observedRows uint64,
	needWindow uint64,
) bool {
	if universe == 0 || observedRows == 0 || op.Not || op.Field == "" || op.Field == orderField {
		return false
	}
	if needWindow == 0 {
		needWindow = 1
	}
	if observedRows <= needWindow {
		return false
	}
	stats, ok := qv.orderBasicRawBaseOpStats(op, universe)
	if !ok || stats.cacheKey.isZero() || stats.probeBuckets == 0 || stats.probeEst == 0 {
		return false
	}
	buildWork := rangeProbeMaterializeWork(stats.buildBuckets, stats.buildEst)
	if buildWork == 0 {
		return false
	}
	if observedRows > universe {
		observedRows = universe
	}
	excessRows := observedRows - needWindow
	probeWork := rangeProbeTotalWorkForRows(clampUint64ToInt(excessRows), stats.probeBuckets, stats.probeEst)
	return probeWork >= buildWork
}

func (qv *queryView[K, V]) materializeOrderBasicLimitComplementBaseOp(op qx.Expr, cacheKey materializedPredKey) bool {
	if cacheKey.isZero() || op.Field == "" {
		return false
	}
	candidate, ok := qv.prepareScalarRangeRoutingCandidate(op)
	if !ok {
		return false
	}
	plan, ok := candidate.core.prepareComplementMaterialization()
	if !ok {
		return false
	}
	if plan.est == 0 {
		qv.snap.storeMaterializedPredKey(cacheKey, posting.List{})
		return true
	}
	ids := candidate.core.materializeComplement(plan)
	if ids.IsEmpty() {
		qv.snap.storeMaterializedPredKey(cacheKey, posting.List{})
		return true
	}
	ids = tryShareMaterializedPredOnSnapshot(qv.snap, cacheKey, ids)
	ids.Release()
	return true
}

func (qv *queryView[K, V]) materializeOrderMaterializedBaseOpsBuf(orderField string, baseOps *pooled.SliceBuf[qx.Expr]) {
	if qv.snap == nil || qv.snap.materializedPredCacheLimit() <= 0 || baseOps == nil || baseOps.Len() == 0 {
		return
	}
	coresBuf, rawCoreIdxBuf, empty, err := qv.prepareOrderBasicBaseCoresBuf(baseOps)
	if err != nil || empty {
		if coresBuf != nil {
			orderBasicBaseCoreSlicePool.Put(coresBuf)
		}
		if rawCoreIdxBuf != nil {
			orderBasicBaseCoreIndexSlicePool.Put(rawCoreIdxBuf)
		}
		return
	}
	defer orderBasicBaseCoreSlicePool.Put(coresBuf)
	defer orderBasicBaseCoreIndexSlicePool.Put(rawCoreIdxBuf)

	keysBuf := materializedPredKeySlicePool.Get()
	keysBuf.Grow(coresBuf.Len())
	defer materializedPredKeySlicePool.Put(keysBuf)

	for i := 0; i < coresBuf.Len(); i++ {
		core := coresBuf.Get(i)
		key := materializedPredKey{}
		switch core.kind {
		case orderBasicBaseCoreCollapsedRange:
			key = core.collapsed.cacheKey
		case orderBasicBaseCoreRawExpr:
			if core.expr.Not || core.expr.Field == "" || core.expr.Field == orderField {
				continue
			}
			stats, ok := qv.orderBasicRawBaseOpStats(core.expr, qv.snapshotUniverseCardinality())
			if ok {
				key = stats.cacheKey
			}
		}
		if key.isZero() {
			continue
		}
		seen := false
		for j := 0; j < keysBuf.Len(); j++ {
			if keysBuf.Get(j) == key {
				seen = true
				break
			}
		}
		if seen {
			continue
		}
		keysBuf.Append(key)
		qv.promoteObservedOrderBasicBaseCore(core)
	}
}

func (qv *queryView[K, V]) promoteOrderBasicLimitMaterializedBaseOps(orderField string, baseOps []qx.Expr, observedRows uint64, needWindow uint64) {
	if qv.snap == nil || len(baseOps) == 0 || observedRows == 0 {
		return
	}
	universe := qv.snapshotUniverseCardinality()
	if universe == 0 {
		return
	}
	coresBuf, rawCoreIdxBuf, empty, err := qv.prepareOrderBasicBaseCores(baseOps)
	if err != nil || empty {
		if coresBuf != nil {
			orderBasicBaseCoreSlicePool.Put(coresBuf)
		}
		if rawCoreIdxBuf != nil {
			orderBasicBaseCoreIndexSlicePool.Put(rawCoreIdxBuf)
		}
		return
	}
	defer orderBasicBaseCoreSlicePool.Put(coresBuf)
	defer orderBasicBaseCoreIndexSlicePool.Put(rawCoreIdxBuf)

	keysBuf := materializedPredKeySlicePool.Get()
	keysBuf.Grow(coresBuf.Len())
	defer materializedPredKeySlicePool.Put(keysBuf)

	for i := 0; i < coresBuf.Len(); i++ {
		core := coresBuf.Get(i)
		if !qv.shouldPromoteObservedOrderBasicBaseCore(orderField, core, universe, observedRows, needWindow) {
			continue
		}
		key := materializedPredKey{}
		requiresSecondHit := false
		switch core.kind {
		case orderBasicBaseCoreCollapsedRange:
			key = core.collapsed.cacheKey
		case orderBasicBaseCoreRawExpr:
			stats, ok := qv.orderBasicRawBaseOpStats(core.expr, qv.snapshotUniverseCardinality())
			if ok {
				key = stats.cacheKey
				requiresSecondHit = true
			}
		}
		if key.isZero() {
			continue
		}
		if requiresSecondHit && !qv.snap.shouldPromoteRuntimeMaterializedPredKey(key) {
			continue
		}
		seen := false
		for j := 0; j < keysBuf.Len(); j++ {
			if keysBuf.Get(j) == key {
				seen = true
				break
			}
		}
		if seen {
			continue
		}
		keysBuf.Append(key)
		qv.promoteObservedOrderBasicBaseCore(core)
	}
}

func (qv *queryView[K, V]) promoteObservedLimitLeafPreds(orderField string, preds *pooled.SliceBuf[leafPred], observedRows uint64, needWindow uint64) {
	if qv.snap == nil || preds == nil || preds.Len() == 0 || observedRows == 0 {
		return
	}
	universe := qv.snapshotUniverseCardinality()
	if universe == 0 {
		return
	}

	keysBuf := materializedPredKeySlicePool.Get()
	keysBuf.Grow(preds.Len())
	defer materializedPredKeySlicePool.Put(keysBuf)
	for i := 0; i < preds.Len(); i++ {
		pred := preds.Get(i)
		if pred.kind != leafPredKindPredicate {
			continue
		}
		var (
			core orderBasicBaseCore
			ok   bool
		)
		if pred.hasBaseCore {
			core = pred.baseCore
			ok = true
		} else {
			op := pred.pred.expr
			if !isSimpleScalarRangeOrPrefixLeaf(op) || op.Field == "" || op.Field == orderField || op.Not {
				continue
			}
			core = orderBasicBaseCore{
				kind: orderBasicBaseCoreRawExpr,
				expr: op,
			}
			ok = true
		}
		if !ok {
			continue
		}
		if !qv.shouldPromoteObservedOrderBasicBaseCore(orderField, core, universe, observedRows, needWindow) {
			continue
		}

		key := materializedPredKey{}
		requiresSecondHit := false
		switch core.kind {
		case orderBasicBaseCoreCollapsedRange:
			key = core.collapsed.cacheKey
		case orderBasicBaseCoreRawExpr:
			stats, ok := qv.orderBasicRawBaseOpStats(core.expr, universe)
			if ok {
				key = stats.cacheKey
				requiresSecondHit = true
			}
		}
		if key.isZero() {
			continue
		}
		if requiresSecondHit && !qv.snap.shouldPromoteRuntimeMaterializedPredKey(key) {
			continue
		}

		seen := false
		for j := 0; j < keysBuf.Len(); j++ {
			if keysBuf.Get(j) == key {
				seen = true
				break
			}
		}
		if seen {
			continue
		}
		keysBuf.Append(key)
		qv.promoteObservedOrderBasicBaseCore(core)
	}
}

func (qv *queryView[K, V]) evalOrderBasicRawBaseOp(op qx.Expr) (postingResult, error) {
	stats, ok := qv.orderBasicRawBaseOpStats(op, qv.snapshotUniverseCardinality())
	cacheKey := materializedPredKey{}
	if ok {
		cacheKey = stats.cacheKey
	}
	if !cacheKey.isZero() {
		if cached, ok := qv.snap.loadMaterializedPredKey(cacheKey); ok {
			if cacheKey == qv.materializedPredKey(op) {
				return postingResult{ids: cached}, nil
			}
			return postingResult{ids: cached, neg: true}, nil
		}
	}
	return qv.evalExpr(op)
}

func (qv *queryView[K, V]) loadWarmOrderBasicRawBaseOp(op qx.Expr) (postingResult, bool) {
	candidate, ok := qv.prepareScalarRangeRoutingCandidate(op)
	if !ok {
		return postingResult{}, false
	}
	return candidate.core.loadWarmScalarPostingResult()
}

func (qv *queryView[K, V]) shouldPreferOrderBasicBaseCorePathForNonOrderPrefix(q *qx.QX, orderField string, baseOps []qx.Expr) bool {
	if len(baseOps) == 0 {
		return false
	}
	window, _ := orderWindow(q)
	if window <= 0 {
		return false
	}

	universe := qv.snapshotUniverseCardinality()
	if universe == 0 {
		return false
	}

	for _, op := range baseOps {
		if !isPositiveNonOrderScalarPrefixLeaf(orderField, op) {
			continue
		}
		candidate, ok := qv.prepareScalarRangeRoutingCandidate(op)
		if !ok {
			continue
		}
		if candidate.plan.orderedEagerMaterializeUseful(window, universe) {
			return true
		}
	}

	return false
}

func (qv *queryView[K, V]) tryQueryOrderBasicWithLimit(q *qx.QX, trace *queryTrace) ([]K, bool, error) {

	if len(q.Order) != 1 || q.Limit == 0 {
		return nil, false, nil
	}

	if q.Expr.Not {
		return nil, false, nil
	}

	order := q.Order[0]
	if order.Type != qx.OrderBasic {
		return nil, false, nil
	}

	ops := q.Expr.Operands

	if normalize.IsTrueConst(q.Expr) {
		ops = nil
	} else if q.Expr.Op != qx.OpAND {
		var single [1]qx.Expr
		single[0] = q.Expr
		ops = single[:]
	}

	f := order.Field
	needWindow, _ := orderWindow(q)
	fm := qv.fields[f]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}

	var rb rangeBounds

	var baseOps []qx.Expr
	var baseOpsStack [8]qx.Expr
	if len(ops) <= len(baseOpsStack) {
		baseOps = baseOpsStack[:0]
	} else {
		baseOps = make([]qx.Expr, 0, len(ops))
	}

	for _, op := range ops {
		if op.Field == f {
			if op.Not || !isScalarRangeEqOp(op.Op) {
				return nil, false, nil
			}
			nextRB, ok, err := qv.rangeBoundsForScalarExpr(op)
			if err != nil {
				return nil, true, err
			}
			if !ok {
				return nil, false, nil
			}
			mergeRangeBounds(&rb, nextRB)
			continue
		}
		baseOps = append(baseOps, op)
	}
	baseCoresBuf, baseRawCoreIdxBuf, noMatch, err := qv.prepareOrderBasicBaseCores(baseOps)
	if err != nil {
		return nil, true, err
	}
	if baseCoresBuf != nil {
		defer orderBasicBaseCoreSlicePool.Put(baseCoresBuf)
	}
	if baseRawCoreIdxBuf != nil {
		defer orderBasicBaseCoreIndexSlicePool.Put(baseRawCoreIdxBuf)
	}
	if noMatch {
		return nil, true, nil
	}

	nilTailField := orderNilTailField(fm, f, rb)
	ov := qv.fieldOverlay(f)
	if !ov.hasData() && nilTailField == "" {
		if !qv.hasIndexedField(f) {
			return nil, false, nil
		}
		return nil, true, nil
	}

	br := ov.rangeForBounds(rb)
	if overlayRangeEmpty(br) && nilTailField == "" {
		return nil, true, nil
	}

	preferBaseCores := !rb.hasLo && !rb.hasHi && !rb.hasPrefix &&
		q.Offset == 0 &&
		qv.shouldPreferOrderBasicBaseCorePathForNonOrderPrefix(q, f, baseOps)

	hasWarmBaseOps := baseCoresBuf != nil && baseCoresBuf.Len() > 0 && qv.hasWarmOrderBasicBaseCores(baseCoresBuf)
	if !preferBaseCores && !fm.Ptr && len(baseOps) > 0 {
		leavesBuf := exprSlicePool.Get()
		leavesBuf.Grow(len(baseOps) + 2)
		if collectAndLeavesBuf(q.Expr, leavesBuf) {
			execDecision := qv.decideExecutionOrderByCostBuf(q, leavesBuf)
			if execDecision.use {
				window, _ := orderWindow(q)
				predSet, ok := qv.buildPredicatesOrderedWithModeBuf(leavesBuf, f, false, window, q.Offset, true, true)
				if ok {
					defer predSet.Release()
					for i := 0; i < predSet.Len(); i++ {
						if predSet.Get(i).alwaysFalse {
							exprSlicePool.Put(leavesBuf)
							return nil, true, nil
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
					if out, ok := qv.execPlanOrderedBasicReader(q, predSet, execTrace); ok {
						exprSlicePool.Put(leavesBuf)
						qv.promoteOrderBasicLimitMaterializedBaseOps(f, baseOps, execTrace.ev.RowsExamined-observedStart, uint64(needWindow))
						return out, true, nil
					}
				}
			}
		}
		exprSlicePool.Put(leavesBuf)
		if !hasWarmBaseOps {
			return nil, false, nil
		}
	}

	if len(baseOps) == 0 {
		out, _ := qv.scanOrderLimitNoPredicates(q, ov, br, order.Desc, nilTailField, trace)
		return out, true, nil
	}

	var base postingResult
	var residualPredSet predicateSet
	var residualActiveBuf *pooled.SliceBuf[int]
	var residualActive []int

	if hasWarmBaseOps {
		residualOpsBuf := exprSlicePool.Get()
		residualOpsBuf.Grow(len(baseOps))
		defer exprSlicePool.Put(residualOpsBuf)
		var loadedCoreBuf *pooled.SliceBuf[bool]
		if baseCoresBuf != nil && baseCoresBuf.Len() > 0 {
			loadedCoreBuf = boolSlicePool.Get()
			loadedCoreBuf.SetLen(baseCoresBuf.Len())
			defer boolSlicePool.Put(loadedCoreBuf)
		}

		baseBuilt := false
		for i := 0; i < baseCoresBuf.Len(); i++ {
			b, ok := qv.loadWarmOrderBasicBaseCore(baseCoresBuf.Get(i))
			if !ok {
				continue
			}
			loadedCoreBuf.Set(i, true)
			if !baseBuilt {
				base = b
				baseBuilt = true
				continue
			}
			var err error
			base, err = qv.andPostingResult(base, b)
			if err != nil {
				base.release()
				return nil, true, err
			}
		}
		for i, op := range baseOps {
			if loadedCoreBuf.Get(baseRawCoreIdxBuf.Get(i)) {
				continue
			}
			residualOpsBuf.Append(op)
		}
		if !baseBuilt {
			hasWarmBaseOps = false
		} else if residualOpsBuf.Len() > 0 {
			window, _ := orderWindow(q)
			var ok bool
			residualPredSet, ok = qv.buildPredicatesOrderedWithModeBuf(residualOpsBuf, f, false, window, q.Offset, false, true)
			if !ok {
				base.release()
				base = postingResult{}
				hasWarmBaseOps = false
				residualPredSet.Release()
			} else {
				var residualActiveInline [limitQueryFastPathMaxLeaves]int
				if residualPredSet.Len() <= limitQueryFastPathMaxLeaves {
					residualActive = residualActiveInline[:0]
				} else {
					residualActiveBuf = predicateCheckSlicePool.Get()
					residualActiveBuf.Grow(residualPredSet.Len())
				}
				for i := 0; i < residualPredSet.Len(); i++ {
					p := residualPredSet.Get(i)
					if p.alwaysFalse {
						base.release()
						releaseOrderBasicResidualState(residualPredSet, residualActiveBuf)
						return nil, true, nil
					}
					if p.covered || p.alwaysTrue {
						continue
					}
					if residualActiveBuf != nil {
						residualActiveBuf.Append(i)
						continue
					}
					residualActive = append(residualActive, i)
				}
			}
		}
	}

	if !hasWarmBaseOps {
		baseBuilt := false
		for i := 0; i < baseCoresBuf.Len(); i++ {
			b, err := qv.evalOrderBasicBaseCore(baseCoresBuf.Get(i))
			if err != nil {
				base.release()
				return nil, true, err
			}
			if !baseBuilt {
				base = b
				baseBuilt = true
				continue
			}
			base, err = qv.andPostingResult(base, b)
			if err != nil {
				base.release()
				return nil, true, err
			}
		}
	}
	if !base.neg && base.ids.IsEmpty() {
		base.release()
		releaseOrderBasicResidualState(residualPredSet, residualActiveBuf)
		return nil, true, nil
	}
	var (
		out  []K
		used bool
	)
	if residualActiveBuf != nil {
		out, used, err = qv.runOrderBasicBaseQueryBuf(q, f, baseOps, needWindow, order, ov, br, nilTailField, base, residualPredSet, residualActiveBuf, trace)
	} else {
		out, used, err = qv.runOrderBasicBaseQuery(q, f, baseOps, needWindow, order, ov, br, nilTailField, base, residualPredSet, residualActive, trace)
	}
	releaseOrderBasicResidualState(residualPredSet, residualActiveBuf)
	return out, used, err
}

func (qv *queryView[K, V]) scanOrderLimitNoPredicates(q *qx.QX, ov fieldOverlay, br overlayRange, desc bool, nilTailField string, trace *queryTrace) ([]K, bool) {
	out := make([]K, 0, q.Limit)
	cursor := qv.newQueryCursor(out, q.Offset, q.Limit, false, 0)

	var (
		examined  uint64
		scanWidth uint64
	)

	keyCur := ov.newCursor(br, desc)
	for {
		_, ids, ok := keyCur.next()
		if !ok {
			break
		}
		if ids.IsEmpty() {
			continue
		}
		scanWidth++
		if emitOrderLimitPosting(&cursor, ids, &examined, trace) {
			if trace != nil {
				trace.addExamined(examined)
				trace.addOrderScanWidth(scanWidth)
				trace.setEarlyStopReason("limit_reached")
			}
			return cursor.out, true
		}
	}

	if nilTailField != "" {
		ids := qv.nilFieldOverlay(nilTailField).lookupPostingRetained(nilIndexEntryKey)
		if !ids.IsEmpty() {
			scanWidth++
			if emitOrderLimitPosting(&cursor, ids, &examined, trace) {
				if trace != nil {
					trace.addExamined(examined)
					trace.addOrderScanWidth(scanWidth)
					trace.setEarlyStopReason("limit_reached")
				}
				return cursor.out, true
			}
		}
	}

	if trace != nil {
		trace.addExamined(examined)
		trace.addOrderScanWidth(scanWidth)
		trace.setEarlyStopReason("input_exhausted")
	}
	return cursor.out, true
}

func (qv *queryView[K, V]) scanOrderLimitWithPredicatesReader(q *qx.QX, ov fieldOverlay, br overlayRange, desc bool, preds predicateReader, nilTailField string, trace *queryTrace) ([]K, bool) {
	if preds.Len() > limitQueryFastPathMaxLeaves {
		activeBuf := predicateCheckSlicePool.Get()
		activeBuf.Grow(preds.Len())
		defer predicateCheckSlicePool.Put(activeBuf)
		for i := 0; i < preds.Len(); i++ {
			p := preds.Get(i)
			if p.alwaysFalse {
				if trace != nil {
					trace.setEarlyStopReason("empty_predicate")
				}
				return nil, true
			}
			if p.covered || p.alwaysTrue {
				continue
			}
			if !p.hasContains() {
				return nil, false
			}
			activeBuf.Append(i)
		}
		if activeBuf.Len() == 0 {
			return qv.scanOrderLimitNoPredicates(q, ov, br, desc, nilTailField, trace)
		}
		sortActivePredicatesBufReader(activeBuf, preds)

		exactActiveBuf := predicateCheckSlicePool.Get()
		exactActiveBuf.Grow(activeBuf.Len())
		defer predicateCheckSlicePool.Put(exactActiveBuf)
		buildExactBucketPostingFilterActiveBufReader(exactActiveBuf, activeBuf, preds)

		residualActiveBuf := predicateCheckSlicePool.Get()
		residualActiveBuf.Grow(activeBuf.Len())
		defer predicateCheckSlicePool.Put(residualActiveBuf)
		plannerResidualChecksBuf(residualActiveBuf, activeBuf, exactActiveBuf)

		exactOnly := activeBuf.Len() > 0 && activeBuf.Len() == exactActiveBuf.Len()

		out := make([]K, 0, q.Limit)
		cursor := qv.newQueryCursor(out, q.Offset, q.Limit, false, 0)

		var (
			examined  uint64
			scanWidth uint64
		)

		var exactWork posting.List

		keyCur := ov.newCursor(br, desc)
		for {
			_, ids, ok := keyCur.next()
			if !ok {
				break
			}
			if !ids.IsEmpty() {
				scanWidth++
			}
			stop, nextExactWork := orderPredicatesEmitPostingBufReader(
				&cursor,
				preds,
				activeBuf,
				exactActiveBuf,
				residualActiveBuf,
				exactOnly,
				ids,
				exactWork,
				trace,
				&examined,
			)
			exactWork = nextExactWork
			if stop {
				if trace != nil {
					trace.addExamined(examined)
					trace.addOrderScanWidth(scanWidth)
					trace.setEarlyStopReason("limit_reached")
				}
				exactWork.Release()
				return cursor.out, true
			}
		}

		if nilTailField != "" {
			ids := qv.nilFieldOverlay(nilTailField).lookupPostingRetained(nilIndexEntryKey)
			if !ids.IsEmpty() {
				scanWidth++
				stop, nextExactWork := orderPredicatesEmitPostingBufReader(
					&cursor,
					preds,
					activeBuf,
					exactActiveBuf,
					residualActiveBuf,
					exactOnly,
					ids,
					exactWork,
					trace,
					&examined,
				)
				exactWork = nextExactWork
				if stop {
					if trace != nil {
						trace.addExamined(examined)
						trace.addOrderScanWidth(scanWidth)
						trace.setEarlyStopReason("limit_reached")
					}
					exactWork.Release()
					return cursor.out, true
				}
			}
		}

		if trace != nil {
			trace.addExamined(examined)
			trace.addOrderScanWidth(scanWidth)
			trace.setEarlyStopReason("input_exhausted")
		}
		exactWork.Release()
		return cursor.out, true
	}

	var activeInline [limitQueryFastPathMaxLeaves]int
	active := activeInline[:0]
	for i := 0; i < preds.Len(); i++ {
		p := preds.Get(i)
		if p.alwaysFalse {
			if trace != nil {
				trace.setEarlyStopReason("empty_predicate")
			}
			return nil, true
		}
		if p.covered || p.alwaysTrue {
			continue
		}
		if !p.hasContains() {
			return nil, false
		}
		active = append(active, i)
	}
	if len(active) == 0 {
		return qv.scanOrderLimitNoPredicates(q, ov, br, desc, nilTailField, trace)
	}
	sortActivePredicatesReader(active, preds)

	var exactActiveInline [limitQueryFastPathMaxLeaves]int
	var residualActiveInline [limitQueryFastPathMaxLeaves]int
	exactActive := buildExactBucketPostingFilterActiveReader(exactActiveInline[:0], active, preds)
	residualActive := plannerResidualChecks(residualActiveInline[:0], active, exactActive)
	exactOnly := len(active) > 0 && len(active) == len(exactActive)

	out := make([]K, 0, q.Limit)
	cursor := qv.newQueryCursor(out, q.Offset, q.Limit, false, 0)

	var (
		examined  uint64
		scanWidth uint64
	)

	var exactWork posting.List

	keyCur := ov.newCursor(br, desc)
	for {
		_, ids, ok := keyCur.next()
		if !ok {
			break
		}
		if !ids.IsEmpty() {
			scanWidth++
		}
		stop, nextExactWork := orderPredicatesEmitPostingReader(
			&cursor,
			preds,
			active,
			exactActive,
			residualActive,
			exactOnly,
			ids,
			exactWork,
			trace,
			&examined,
		)
		exactWork = nextExactWork
		if stop {
			if trace != nil {
				trace.addExamined(examined)
				trace.addOrderScanWidth(scanWidth)
				trace.setEarlyStopReason("limit_reached")
			}
			exactWork.Release()
			return cursor.out, true
		}
	}

	if nilTailField != "" {
		ids := qv.nilFieldOverlay(nilTailField).lookupPostingRetained(nilIndexEntryKey)
		if !ids.IsEmpty() {
			scanWidth++
			stop, nextExactWork := orderPredicatesEmitPostingReader(
				&cursor,
				preds,
				active,
				exactActive,
				residualActive,
				exactOnly,
				ids,
				exactWork,
				trace,
				&examined,
			)
			exactWork = nextExactWork
			if stop {
				if trace != nil {
					trace.addExamined(examined)
					trace.addOrderScanWidth(scanWidth)
					trace.setEarlyStopReason("limit_reached")
				}
				exactWork.Release()
				return cursor.out, true
			}
		}
	}

	if trace != nil {
		trace.addExamined(examined)
		trace.addOrderScanWidth(scanWidth)
		trace.setEarlyStopReason("input_exhausted")
	}
	exactWork.Release()
	return cursor.out, true
}

func (qv *queryView[K, V]) tryQueryOrderPrefixWithLimit(q *qx.QX, trace *queryTrace) ([]K, bool, error) {

	if len(q.Order) != 1 || q.Limit == 0 {
		return nil, false, nil
	}

	ord := q.Order[0]
	if ord.Type != qx.OrderBasic {
		return nil, false, nil
	}
	if q.Expr.Not {
		return nil, false, nil
	}

	f := ord.Field
	fm := qv.fields[f]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}

	ov := qv.fieldOverlay(f)
	if !ov.hasData() {
		return nil, true, nil
	}

	expr := q.Expr
	ops := expr.Operands
	if expr.Op != qx.OpAND {
		var single [1]qx.Expr
		single[0] = expr
		ops = single[:]
	}

	var (
		hasPrefix bool
		prefix    string
		baseOps   []qx.Expr
	)
	var baseOpsStack [8]qx.Expr
	if len(ops) <= len(baseOpsStack) {
		baseOps = baseOpsStack[:0]
	} else {
		baseOps = make([]qx.Expr, 0, len(ops))
	}

	for _, op := range ops {
		if op.Not {
			return nil, false, nil
		}
		if classifyOrderFieldScalarLeaf(f, op) == orderFieldScalarLeafPrefix {
			prefixState, ok, err := qv.prepareScalarPrefixRoute(op)
			if err != nil {
				return nil, true, err
			}
			if !ok {
				return nil, false, nil
			}
			if !prefixState.hasData || overlayRangeEmpty(prefixState.br) {
				return nil, false, nil
			}
			hasPrefix = true
			prefix = prefixState.prefix
			continue
		}
		baseOps = append(baseOps, op)
	}

	if !hasPrefix {
		return nil, false, nil
	}

	br := ov.rangeForBounds(rangeBounds{
		has:       true,
		hasPrefix: true,
		prefix:    prefix,
	})
	if overlayRangeEmpty(br) {
		return nil, true, nil
	}

	if len(baseOps) == 0 {
		out, _ := qv.scanOrderLimitNoPredicates(q, ov, br, ord.Desc, "", trace)
		return out, true, nil
	}

	var base postingResult
	if len(baseOps) == 1 {
		b, err := qv.evalExpr(baseOps[0])
		if err != nil {
			return nil, true, err
		}
		if b.ids.IsEmpty() {
			b.release()
			return nil, true, nil
		}
		base = b

	} else {
		b, err := qv.evalAndOperands(baseOps, false)
		if err != nil {
			return nil, true, err
		}
		if b.ids.IsEmpty() {
			b.release()
			return nil, true, nil
		}
		base = b
	}
	defer base.release()

	baseBM := base.ids
	baseNegUniverse := base.neg && baseBM.IsEmpty()
	if !base.neg && baseBM.IsEmpty() {
		return nil, true, nil
	}

	skip := q.Offset
	need := q.Limit
	out := make([]K, 0, need)
	cursor := qv.newQueryCursor(out, skip, need, false, 0)

	keyCur := ov.newCursor(br, ord.Desc)
	var (
		examined  uint64
		scanWidth uint64
	)
	for {
		_, ids, ok := keyCur.next()
		if !ok {
			break
		}
		if ids.IsEmpty() {
			continue
		}
		scanWidth++
		if emitOrderPrefixPostingByBase(&cursor, ids, base, baseBM, baseNegUniverse, &examined) {
			trace.addExamined(examined)
			trace.addOrderScanWidth(scanWidth)
			trace.setEarlyStopReason("limit_reached")
			return cursor.out, true, nil
		}
	}

	trace.addExamined(examined)
	trace.addOrderScanWidth(scanWidth)
	trace.setEarlyStopReason("input_exhausted")
	return cursor.out, true, nil
}

func (qv *queryView[K, V]) tryQueryRangeNoOrderWithLimit(q *qx.QX, trace *queryTrace) ([]K, bool, error) {

	if len(q.Order) > 0 || q.Limit == 0 {
		return nil, false, nil
	}

	if q.Expr.Not {
		return nil, false, nil
	}

	e := q.Expr
	if e.Op == qx.OpAND || e.Op == qx.OpOR || len(e.Operands) != 0 {
		return nil, false, nil
	}

	if !isScalarRangeEqOp(e.Op) {
		return nil, false, nil
	}

	f := e.Field
	if f == "" {
		return nil, false, nil
	}

	fm := qv.fields[f]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}

	ov := qv.fieldOverlay(f)
	if !ov.hasData() {
		return nil, true, nil
	}

	isNil := false
	rb := rangeBounds{has: true}
	if e.Op == qx.OpEQ {
		key, isSlice, eqNil, err := qv.exprValueToIdxScalar(e)
		if err != nil {
			return nil, true, err
		}
		if isSlice {
			return nil, false, nil
		}
		isNil = eqNil
		rb.applyLo(key, true)
		rb.applyHi(key, true)
	} else {
		nextRB, ok, err := qv.rangeBoundsForScalarExpr(e)
		if err != nil {
			return nil, true, err
		}
		if !ok {
			return nil, false, nil
		}
		rb = nextRB
	}

	br := ov.rangeForBounds(rb)
	if overlayRangeEmpty(br) {
		return nil, true, nil
	}

	skip := q.Offset
	need := q.Limit

	if isNil {
		if e.Op != qx.OpEQ {
			return nil, true, nil
		}
		ids := qv.nilFieldOverlay(f).lookupPostingRetained(nilIndexEntryKey)
		if ids.IsEmpty() {
			return nil, true, nil
		}
		out := make([]K, 0, need)
		cursor := qv.newQueryCursor(out, skip, need, false, 0)
		var examined uint64
		if emitAcceptedPostingNoOrder(&cursor, ids, &examined) {
			trace.addExamined(examined)
			trace.setEarlyStopReason("limit_reached")
			return cursor.out, true, nil
		}
		trace.addExamined(examined)
		trace.setEarlyStopReason("input_exhausted")
		return cursor.out, true, nil
	}

	out := make([]K, 0, need)
	cursor := qv.newQueryCursor(out, skip, need, false, 0)

	keyCur := ov.newCursor(br, false)
	var examined uint64
	for {
		_, ids, ok := keyCur.next()
		if !ok {
			break
		}
		if ids.IsEmpty() {
			continue
		}
		if emitAcceptedPostingNoOrder(&cursor, ids, &examined) {
			trace.addExamined(examined)
			trace.setEarlyStopReason("limit_reached")
			return cursor.out, true, nil
		}
	}

	trace.addExamined(examined)
	trace.setEarlyStopReason("input_exhausted")
	return cursor.out, true, nil
}

func (qv *queryView[K, V]) tryQueryPrefixNoOrderWithLimit(q *qx.QX, trace *queryTrace) ([]K, bool, error) {

	if len(q.Order) > 0 || q.Limit == 0 {
		return nil, false, nil
	}

	if q.Expr.Not {
		return nil, false, nil
	}

	e := q.Expr
	if !isPositiveScalarPrefixLeaf(e) {
		return nil, false, nil
	}

	fm := qv.fields[e.Field]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}

	prefixState, ok, err := qv.prepareScalarPrefixRoute(e)
	if err != nil {
		return nil, true, err
	}
	if !ok {
		return nil, false, nil
	}
	if !prefixState.hasData {
		if !qv.hasIndexedField(e.Field) {
			return nil, true, fmt.Errorf("no index for field: %v", e.Field)
		}
		return nil, true, nil
	}
	if overlayRangeEmpty(prefixState.br) {
		return nil, true, nil
	}

	skip := q.Offset
	need := q.Limit
	out := make([]K, 0, need)
	cursor := qv.newQueryCursor(out, skip, need, false, 0)

	keyCur := prefixState.ov.newCursor(prefixState.br, false)
	var examined uint64
	for {
		_, ids, ok := keyCur.next()
		if !ok {
			break
		}
		if ids.IsEmpty() {
			continue
		}
		if emitAcceptedPostingNoOrder(&cursor, ids, &examined) {
			trace.addExamined(examined)
			trace.setEarlyStopReason("limit_reached")
			return cursor.out, true, nil
		}
	}

	trace.addExamined(examined)
	trace.setEarlyStopReason("input_exhausted")
	return cursor.out, true, nil
}
