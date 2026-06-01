package qexec

import (
	"fmt"
	"reflect"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/schema"
)

func (qv *View) execSelectedNoOrderNoFilter(q *qir.Shape, trace *Trace) ([]uint64, bool, error) {
	universe := qv.snap.Universe.Borrow()
	if universe.IsEmpty() {
		if trace != nil {
			trace.AddExamined(0)
			trace.SetEarlyStopReason("input_exhausted")
		}
		return nil, true, nil
	}

	card := universe.Cardinality()
	outCap := q.Limit
	if outCap > card {
		outCap = card
	}

	out := make([]uint64, 0, clampUint64ToInt(outCap))
	if trace == nil && q.Offset == 0 {
		out, _ = appendPostingLimitNoTrace(out, universe, clampUint64ToInt(q.Limit))
		return out, true, nil
	}

	cursor := newQueryCursor(out, 0, q.Limit, false, 0)

	var (
		examined    uint64
		examinedPtr *uint64
	)
	if trace != nil {
		examinedPtr = &examined
	}

	stopped := emitAcceptedPostingNoOrder(&cursor, universe, examinedPtr)
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

func (qv *View) execSelectedNoOrderUniverseScan(q *qir.Shape, leaves []qir.Expr, expectedRows uint64, trace *Trace) ([]uint64, bool, error) {
	universe := qv.snap.Universe.Borrow()
	if universe.IsEmpty() {
		if trace != nil {
			trace.AddExamined(0)
			trace.SetEarlyStopReason("input_exhausted")
		}
		return nil, true, nil
	}
	if len(leaves) == 1 {
		if out, ok, err := qv.execSelectedNoOrderSingleNegativeScalarUniverseScan(q, leaves[0], universe, expectedRows, trace); ok || err != nil {
			return out, ok, err
		}
	}

	predsBuf := leafPredSlicePool.Get(len(leaves))
	for i := range leaves {
		p, ok, err := qv.buildLimitLeafPred(leaves[i], 0)
		if err != nil {
			leafPredSlicePool.Put(predsBuf)
			return nil, true, err
		}
		if !ok {
			leafPredSlicePool.Put(predsBuf)
			return nil, false, nil
		}
		if p.kind == leafPredKindEmpty {
			leafPredSlicePool.Put(predsBuf)
			return nil, true, nil
		}
		p.setExpectedContainsCalls(clampUint64ToInt(expectedRows))
		predsBuf = append(predsBuf, p)
	}
	defer leafPredSlicePool.Put(predsBuf)

	var activeBuf [limitQueryFastPathMaxLeaves]int
	active := activeBuf[:0]
	var activeHeap []int
	if len(predsBuf) > limitQueryFastPathMaxLeaves {
		activeHeap = pooled.GetIntSlice(len(predsBuf))
		active = activeHeap
	}
	for i := 0; i < len(predsBuf); i++ {
		active = append(active, i)
	}
	if activeHeap != nil {
		defer pooled.ReleaseIntSlice(activeHeap)
	}

	out := makeOutSlice(expectedRows, q.Limit)
	cursor := newQueryCursor(out, q.Offset, q.Limit, false, 0)
	it := universe.Iter()
	var examined uint64
	for it.HasNext() {
		if leafPredsEmitCandidate(&cursor, predsBuf, active, trace, it.Next(), &examined) {
			it.Release()
			if trace != nil {
				trace.AddExamined(examined)
				trace.SetEarlyStopReason("limit_reached")
			}
			return cursor.out, true, nil
		}
	}
	it.Release()
	if trace != nil {
		trace.AddExamined(examined)
		trace.SetEarlyStopReason("input_exhausted")
	}
	return cursor.out, true, nil
}

func (qv *View) execSelectedNoOrderSingleNegativeScalarUniverseScan(q *qir.Shape, e qir.Expr, universe posting.List, expectedRows uint64, trace *Trace) ([]uint64, bool, error) {
	if !e.Not || e.FieldOrdinal < 0 || len(e.Operands) != 0 {
		return nil, false, nil
	}
	if e.Op != qir.OpEQ && e.Op != qir.OpIN {
		return nil, false, nil
	}
	fm := qv.fieldMetaByExpr(e)
	if fm == nil || fm.Slice {
		return nil, false, nil
	}

	var postsBuf []posting.List
	switch e.Op {
	case qir.OpEQ:
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
		if !ids.IsEmpty() {
			postsBuf = posting.GetSlice(1)
			postsBuf = append(postsBuf, ids)
			defer posting.ReleaseSlice(postsBuf)
		}

	case qir.OpIN:
		keysBuf, isSlice, hasNil, err := qv.exprValueToDistinctIdxBuf(e)
		if err != nil {
			return nil, true, err
		}
		if keysBuf != nil {
			defer pooled.ReleaseStringSlice(keysBuf)
		}
		if !isSlice || (len(keysBuf) == 0 && !hasNil) {
			return nil, false, nil
		}
		postsBuf, _ = qv.scalarLookupPostings(qv.exec.FieldNameByOrdinal(e.FieldOrdinal), e.FieldOrdinal, keysBuf, hasNil)
		if len(postsBuf) > 4 {
			posting.ReleaseSlice(postsBuf)
			return nil, false, nil
		}
		if postsBuf != nil {
			defer posting.ReleaseSlice(postsBuf)
		}
	}

	out := makeOutSlice(expectedRows, q.Limit)
	cursor := newQueryCursor(out, q.Offset, q.Limit, false, 0)
	it := universe.Iter()
	var examined uint64
	var cur0, cur1, cur2, cur3 posting.ContainsCursor
	if len(postsBuf) > 0 {
		cur0.Reset(postsBuf[0])
	}
	if len(postsBuf) > 1 {
		cur1.Reset(postsBuf[1])
	}
	if len(postsBuf) > 2 {
		cur2.Reset(postsBuf[2])
	}
	if len(postsBuf) > 3 {
		cur3.Reset(postsBuf[3])
	}
	for it.HasNext() {
		idx := it.Next()
		examined++
		excluded := false
		switch len(postsBuf) {
		case 0:
		case 1:
			excluded = cur0.Contains(idx)
		case 2:
			excluded = cur0.Contains(idx) || cur1.Contains(idx)
		case 3:
			excluded = cur0.Contains(idx) || cur1.Contains(idx) || cur2.Contains(idx)
		case 4:
			excluded = cur0.Contains(idx) || cur1.Contains(idx) || cur2.Contains(idx) || cur3.Contains(idx)
		default:
			for i := 0; i < len(postsBuf); i++ {
				if postsBuf[i].Contains(idx) {
					excluded = true
					break
				}
			}
		}
		if excluded {
			continue
		}
		if trace != nil {
			trace.AddMatched(1)
		}
		if cursor.emit(idx) {
			it.Release()
			if trace != nil {
				trace.AddExamined(examined)
				trace.SetEarlyStopReason("limit_reached")
			}
			return cursor.out, true, nil
		}
	}
	it.Release()
	if trace != nil {
		trace.AddExamined(examined)
		trace.SetEarlyStopReason("input_exhausted")
	}
	return cursor.out, true, nil
}

func emitAcceptedPostingNoOrder(cursor *queryCursor, ids posting.List, examined *uint64) bool {
	if ids.IsEmpty() {
		return false
	}

	if cursor.skip > 0 {
		card := ids.Cardinality()
		if cursor.skip >= card {
			if examined != nil {
				*examined += card
			}
			cursor.skip -= card
			return false
		}
	}

	if idx, ok := ids.TrySingle(); ok {
		if examined != nil {
			*examined++
		}
		return cursor.emit(idx)
	}

	it := ids.Iter()
	defer it.Release()

	for it.HasNext() {
		if examined != nil {
			*examined++
		}
		if cursor.emit(it.Next()) {
			return true
		}
	}
	return false
}

func appendPostingLimitNoTrace(out []uint64, ids posting.List, limit int) ([]uint64, bool) {
	if ids.IsEmpty() {
		return out, false
	}
	if idx, ok := ids.TrySingle(); ok {
		out = append(out, idx)
		return out, len(out) >= limit
	}

	it := ids.Iter()
	for it.HasNext() {
		out = append(out, it.Next())
		if len(out) >= limit {
			it.Release()
			return out, true
		}
	}
	it.Release()
	return out, false
}

func appendPostingWindowNoTrace(out []uint64, ids posting.List, skip uint64, limit int) ([]uint64, uint64, bool) {
	if ids.IsEmpty() {
		return out, skip, false
	}
	if idx, ok := ids.TrySingle(); ok {
		if skip > 0 {
			return out, skip - 1, false
		}
		out = append(out, idx)
		return out, 0, len(out) >= limit
	}
	if skip > 0 {
		card := ids.Cardinality()
		if skip >= card {
			return out, skip - card, false
		}
	}
	it := ids.Iter()
	for skip > 0 {
		it.Next()
		skip--
	}
	for it.HasNext() {
		out = append(out, it.Next())
		if len(out) >= limit {
			it.Release()
			return out, 0, true
		}
	}
	it.Release()
	return out, 0, false
}

func predicatesMatchActiveReader(preds predicateReader, active []int, idx uint64) bool {
	switch len(active) {
	case 0:
		return true
	case 1:
		return (&preds[active[0]]).matches(idx)
	case 2:
		return (&preds[active[0]]).matches(idx) &&
			(&preds[active[1]]).matches(idx)
	case 3:
		return (&preds[active[0]]).matches(idx) &&
			(&preds[active[1]]).matches(idx) &&
			(&preds[active[2]]).matches(idx)
	case 4:
		return (&preds[active[0]]).matches(idx) &&
			(&preds[active[1]]).matches(idx) &&
			(&preds[active[2]]).matches(idx) &&
			(&preds[active[3]]).matches(idx)
	}
	for _, pi := range active {
		if !(&preds[pi]).matches(idx) {
			return false
		}
	}
	return true
}

func orderPredicatesEmitCandidateReader(
	cursor *queryCursor,
	preds predicateReader,
	checks []int,
	trace *Trace,
	idx uint64,
	examined *uint64,
) bool {
	*examined = *examined + 1
	if !predicatesMatchActiveReader(preds, checks, idx) {
		return false
	}
	if trace != nil {
		trace.AddMatched(1)
	}
	return cursor.emit(idx)
}

func orderPredicatesEmitAcceptedPosting(
	cursor *queryCursor,
	ids posting.List,
	card uint64,
	trace *Trace,
	examined *uint64,
) bool {
	*examined += card
	if trace != nil {
		trace.AddMatched(card)
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

func orderPredicatesTryBucketPostingReader(
	cursor *queryCursor,
	preds predicateReader,
	exactActive []int,
	exactOnly bool,
	ids, exactWork posting.List,
	trace *Trace,
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
			trace.AddPostingExactFilter(1)
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

func orderPredicatesEmitPostingReader(
	cursor *queryCursor,
	preds predicateReader,
	active, exactActive, residualActive []int,
	exactOnly bool,
	ids, exactWork posting.List,
	trace *Trace,
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

func releaseOrderBasicResidualState(preds predicateSet, activeBuf []int) {
	if activeBuf != nil {
		pooled.ReleaseIntSlice(activeBuf)
	}
	preds.Release()
}

func emitOrderLimitPosting(
	cursor *queryCursor,
	ids posting.List,
	examined *uint64,
	trace *Trace,
) bool {
	if ids.IsEmpty() {
		return false
	}
	if idx, ok := ids.TrySingle(); ok {
		*examined += 1
		if trace != nil {
			trace.AddMatched(1)
		}
		return cursor.emit(idx)
	}

	card := ids.Cardinality()
	if cursor.skip >= card {
		cursor.skip -= card
		*examined += card
		if trace != nil {
			trace.AddMatched(card)
		}
		return false
	}

	it := ids.Iter()
	for it.HasNext() {
		idx := it.Next()
		*examined += 1
		if trace != nil {
			trace.AddMatched(1)
		}
		if cursor.emit(idx) {
			it.Release()
			return true
		}
	}
	it.Release()

	return false
}

func orderBasicResidualMatchesReader(preds predicateReader, active []int, idx uint64) bool {
	if len(active) == 0 {
		return true
	}
	return predicatesMatchActiveReader(preds, active, idx)
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

func orderBasicEmitFilteredPostingReader(
	cursor *queryCursor,
	preds predicateReader,
	active []int,
	baseBM posting.List,
	baseNeg bool,
	baseNegUniverse bool,
	ids, tmp posting.List,
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

func (qv *View) runOrderBasicBaseQuery(
	q *qir.Shape,
	orderField string,
	baseOps []qir.Expr,
	needWindow int,
	order qir.Order,
	ov indexdata.FieldIndexView,
	br indexdata.FieldIndexRange,
	nilTailField string,
	base postingResult,
	residualPreds predicateReader,
	residualActive []int,
	guard plannerOrderedLimitRuntimeGuard,
	trace *Trace,
) ([]uint64, bool, error) {

	defer base.ids.Release()

	baseBM := base.ids
	baseNegUniverse := base.neg && baseBM.IsEmpty()

	if !base.neg && baseBM.IsEmpty() {
		return nil, true, nil
	}

	if trace == nil && !base.neg && nilTailField == "" {
		baseCard := baseBM.Cardinality()
		if len(residualActive) == 0 {
			if fm := qv.fieldMetaByOrder(order); fm == nil || !fm.Ptr {
				keyCount := uint64(ov.KeyCount())
				dense := baseCard >= (keyCount+3)/4
				sparseWarm := baseCard > 128 && baseCard <= 512 && keyCount >= baseCard*8
				if dense || sparseWarm {
					if q.Offset >= baseCard {
						return nil, true, nil
					}
					need := q.Limit
					if remaining := baseCard - q.Offset; need > remaining {
						need = remaining
					}
					if guard.enabled {
						out, ok := scanLimitByFieldIndexBoundsPostingFilterNoTrace(q, ov, br, order.Desc, baseBM, need, guard)
						if !ok {
							return nil, false, nil
						}
						return out, true, nil
					}
					out := make([]uint64, 0, need)
					out, _, _ = ov.AppendPostingFilter(out, br, order.Desc, baseBM, q.Offset, need)
					return out, true, nil
				}
			}
		}
		if baseCard > 0 && baseCard <= 4096 {
			out, examined, ok, fallback := scanOrderLimitDenseBaseNoTrace(q, ov, br, order.Desc, baseBM, int(baseCard), residualPreds, residualActive, guard)
			if fallback {
				return nil, false, nil
			}
			if ok {
				qv.promoteOrderBasicLimitMaterializedBaseOps(orderField, baseOps, examined, uint64(needWindow))
				return out, true, nil
			}
		}
		if baseCard > 0 && baseCard <= 128 {
			out, examined, fallback := scanOrderLimitSmallBaseNoTrace(q, ov, br, order.Desc, baseBM, int(baseCard), residualPreds, residualActive, guard)
			if fallback {
				return nil, false, nil
			}
			qv.promoteOrderBasicLimitMaterializedBaseOps(orderField, baseOps, examined, uint64(needWindow))
			return out, true, nil
		}
		if baseCard <= 4096 {
			out, examined, fallback := scanOrderLimitPooledBaseNoTrace(q, ov, br, order.Desc, baseBM, int(baseCard), residualPreds, residualActive, guard)
			if fallback {
				return nil, false, nil
			}
			qv.promoteOrderBasicLimitMaterializedBaseOps(orderField, baseOps, examined, uint64(needWindow))
			return out, true, nil
		}
	}

	skip := q.Offset
	need := q.Limit
	var out []uint64
	if !guard.enabled {
		out = make([]uint64, 0, need)
	}
	cursor := newQueryCursor(out, skip, need, false, 0)
	if guard.enabled {
		cursor.allocCap = need
	}

	var tmp posting.List

	keyCur := ov.NewCursor(br, order.Desc)
	var (
		examined  uint64
		scanWidth uint64
	)

	nilOV := qv.fieldIndexViewFromSlotsByName(qv.snap.NilIndex, nilTailField)

	for {
		ids, idx, single, ok := keyCur.NextPostingOrSingle()
		if !ok {
			break
		}
		if !single && ids.IsEmpty() {
			continue
		}
		scanWidth++
		var stop bool
		if single {
			examined++
			if len(residualActive) == 0 {
				if base.neg {
					if baseNegUniverse || !baseBM.Contains(idx) {
						stop = cursor.emit(idx)
					}
				} else if baseBM.Contains(idx) {
					stop = cursor.emit(idx)
				}
			} else if orderBasicContainsReader(residualPreds, residualActive, baseBM, base.neg, baseNegUniverse, idx) {
				stop = cursor.emit(idx)
			}
		} else if !ids.IsEmpty() {
			if len(residualActive) == 0 {
				stop, tmp = orderBasicEmitFilteredPostingReader(
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
			} else {
				stop, tmp = orderBasicEmitFilteredPostingReader(
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
			}
		}
		if stop {
			if trace != nil {
				trace.AddMatched(uint64(len(cursor.out)))
				trace.AddExamined(examined)
				trace.AddOrderScanWidth(scanWidth)
				trace.SetEarlyStopReason("limit_reached")
			}
			qv.promoteOrderBasicLimitMaterializedBaseOps(orderField, baseOps, examined, uint64(needWindow))
			tmp.Release()
			return cursor.out, true, nil
		}
		if guard.enabled && guard.shouldFallback(examined, len(cursor.out)) {
			if trace != nil {
				trace.AddMatched(uint64(len(cursor.out)))
				trace.AddExamined(examined)
				trace.AddOrderScanWidth(scanWidth)
				trace.SetOrderedLimitRuntimeFallback(guard.reason)
				trace.SetEarlyStopReason(guard.reason)
			}
			tmp.Release()
			return nil, false, nil
		}
	}

	if nilTailField != "" {
		ids := nilOV.LookupPostingRetained(indexdata.NilIndexEntryKey)
		if !ids.IsEmpty() {
			scanWidth++
			var stop bool
			if len(residualActive) == 0 {
				if idx, ok := ids.TrySingle(); ok {
					examined++
					if base.neg {
						if baseNegUniverse || !baseBM.Contains(idx) {
							stop = cursor.emit(idx)
						}
					} else if baseBM.Contains(idx) {
						stop = cursor.emit(idx)
					}
				} else {
					stop, tmp = orderBasicEmitFilteredPostingReader(
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
				}
			} else {
				stop, tmp = orderBasicEmitFilteredPostingReader(
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
			}
			if stop {
				if trace != nil {
					trace.AddMatched(uint64(len(cursor.out)))
					trace.AddExamined(examined)
					trace.AddOrderScanWidth(scanWidth)
					trace.SetEarlyStopReason("limit_reached")
				}
				qv.promoteOrderBasicLimitMaterializedBaseOps(orderField, baseOps, examined, uint64(needWindow))
				tmp.Release()
				return cursor.out, true, nil
			}
			if guard.enabled && guard.shouldFallback(examined, len(cursor.out)) {
				if trace != nil {
					trace.AddMatched(uint64(len(cursor.out)))
					trace.AddExamined(examined)
					trace.AddOrderScanWidth(scanWidth)
					trace.SetOrderedLimitRuntimeFallback(guard.reason)
					trace.SetEarlyStopReason(guard.reason)
				}
				tmp.Release()
				return nil, false, nil
			}
		}
	}

	if trace != nil {
		trace.AddMatched(uint64(len(cursor.out)))
		trace.AddExamined(examined)
		trace.AddOrderScanWidth(scanWidth)
		trace.SetEarlyStopReason("input_exhausted")
	}
	qv.promoteOrderBasicLimitMaterializedBaseOps(orderField, baseOps, examined, uint64(needWindow))
	tmp.Release()
	return cursor.out, true, nil
}

func applyRangeOp(rb *indexdata.Bounds, op qir.Op, key string) bool {
	rb.Has = true
	switch op {
	case qir.OpGT:
		rb.ApplyLo(key, false)
	case qir.OpGTE:
		rb.ApplyLo(key, true)
	case qir.OpLT:
		rb.ApplyHi(key, false)
	case qir.OpLTE:
		rb.ApplyHi(key, true)
	case qir.OpEQ:
		rb.ApplyLo(key, true)
		rb.ApplyHi(key, true)
	case qir.OpPREFIX:
		rb.ApplyPrefix(key)
	default:
		return false
	}
	return true
}

func orderNilTailField(fm *schema.Field, field string, bounds indexdata.Bounds) string {
	if fm == nil || !fm.Ptr || field == "" {
		return ""
	}
	if bounds.Empty || bounds.HasLo || bounds.HasHi || bounds.HasPrefix {
		return ""
	}
	return field
}

func (qv *View) validateOrderBasicBaseOps(baseOps []qir.Expr) error {
	for _, op := range baseOps {
		if err := qv.validateOrderBasicExpr(op); err != nil {
			return err
		}
	}
	return nil
}

// Validate operator/value compatibility without evaluating postings on paths
// that are already proven empty by contradictory order-field predicates.
func (qv *View) validateOrderBasicExpr(e qir.Expr) error {
	switch e.Op {

	case qir.OpNOOP:
		if e.FieldOrdinal != qir.NoFieldOrdinal || e.Value != nil || len(e.Operands) != 0 {
			return fmt.Errorf("%w: invalid expression, op: %v", ErrInvalidQuery, e.Op)
		}
		return nil

	case qir.OpAND:
		if len(e.Operands) == 0 {
			return fmt.Errorf("%w: empty AND expression", ErrInvalidQuery)
		}
		for _, op := range e.Operands {
			if err := qv.validateOrderBasicExpr(op); err != nil {
				return err
			}
		}
		return nil

	case qir.OpOR:
		if len(e.Operands) == 0 {
			return fmt.Errorf("%w: empty OR expression", ErrInvalidQuery)
		}
		for _, op := range e.Operands {
			if err := qv.validateOrderBasicExpr(op); err != nil {
				return err
			}
		}
		return nil

	default:
		return qv.validateOrderBasicSimpleExpr(e)
	}
}

func (qv *View) validateOrderBasicSimpleExpr(e qir.Expr) error {
	if e.FieldOrdinal < 0 {
		return fmt.Errorf("%w: invalid expression, op: %v", ErrInvalidQuery, e.Op)
	}
	fieldName := qv.exec.FieldNameByOrdinal(e.FieldOrdinal)
	ov := qv.fieldIndexViewFromSlotsForExpr(qv.snap.Index, e)

	if !ov.HasData() && !qv.hasIndexedFieldForExpr(e) {
		return fmt.Errorf("no index for field: %v", fieldName)
	}

	fm := qv.fieldMetaByExpr(e)
	if fm == nil {
		return fmt.Errorf("no metadata for field: %v", fieldName)
	}

	switch e.Op {

	case qir.OpEQ:
		if !fm.Slice {
			_, isSlice, _, err := qv.exprValueToLookupKey(e)
			if err != nil {
				return err
			}
			if isSlice {
				return fmt.Errorf("%w: %v expects a single value for scalar field %v", ErrInvalidQuery, e.Op, fieldName)
			}
			return nil
		}
		valsBuf, isSlice, _, err := qv.exprValueToDistinctIdxBuf(e)
		if valsBuf != nil {
			defer pooled.ReleaseStringSlice(valsBuf)
		}
		if err != nil {
			return err
		}
		if !isSlice {
			return fmt.Errorf("%w: %v expects a slice for slice field %v", ErrInvalidQuery, e.Op, fieldName)
		}
		return nil

	case qir.OpIN:
		if fm.Slice {
			return fmt.Errorf("%w: %v not supported on slice field %v", ErrInvalidQuery, e.Op, fieldName)
		}
		valsBuf, isSlice, hasNil, err := qv.exprValueToDistinctIdxBuf(e)
		if valsBuf != nil {
			defer pooled.ReleaseStringSlice(valsBuf)
		}
		if err != nil {
			return err
		}
		if !isSlice && e.Value != nil {
			return fmt.Errorf("%w: %v expects a slice", ErrInvalidQuery, e.Op)
		}
		valCount := len(valsBuf)
		if valCount == 0 && !hasNil {
			return fmt.Errorf("%w: %v: no values provided", ErrInvalidQuery, e.Op)
		}
		return nil

	case qir.OpHASANY, qir.OpHASALL:
		if !fm.Slice {
			return fmt.Errorf("%w: %v not supported on non-slice field %v", ErrInvalidQuery, e.Op, fieldName)
		}
		valsBuf, isSlice, _, err := qv.exprValueToDistinctIdxBuf(e)
		if valsBuf != nil {
			defer pooled.ReleaseStringSlice(valsBuf)
		}
		if err != nil {
			return err
		}
		if !isSlice && e.Value != nil {
			return fmt.Errorf("%w: %v expects a slice", ErrInvalidQuery, e.Op)
		}
		if len(valsBuf) == 0 {
			return fmt.Errorf("%w: %v: no values provided", ErrInvalidQuery, e.Op)
		}
		return nil

	case qir.OpGT, qir.OpGTE, qir.OpLT, qir.OpLTE, qir.OpPREFIX:
		_, isSlice, err := qv.exprValueToNormalizedScalarBound(e)
		if err != nil {
			return err
		}
		if isSlice {
			return fmt.Errorf("%w: %v expects a single value", ErrInvalidQuery, e.Op)
		}
		return nil

	case qir.OpSUFFIX, qir.OpCONTAINS:
		_, isSlice, _, err := qv.exprValueToIdxScalar(e)
		if err != nil {
			return err
		}
		if isSlice {
			return fmt.Errorf("%w: %v expects a single string value", ErrInvalidQuery, e.Op)
		}
		return nil

	default:
		return fmt.Errorf("%w: invalid expression, op: %v", ErrInvalidQuery, e.Op)
	}
}

const iteratorThreshold = 2048 // 256

type orderBasicBaseCoreKind uint8

const (
	orderBasicBaseCoreRawExpr orderBasicBaseCoreKind = iota
	orderBasicBaseCoreCollapsedRange
)

type orderBasicBaseCore struct {
	kind      orderBasicBaseCoreKind
	expr      qir.Expr
	collapsed preparedScalarExactRange
}

func isOrderBasicCollapsibleScalarRangeExpr(op qir.Expr, fm *schema.Field) bool {
	if op.Not || op.FieldOrdinal < 0 || fm == nil || fm.Slice || len(op.Operands) != 0 {
		return false
	}
	return isScalarRangeEqOp(op.Op)
}

func (qv *View) shouldCollapseOrderBasicScalarRange(field string, fieldOrdinal int, rb indexdata.Bounds) bool {
	if field == "" || rb.Empty {
		return false
	}

	ov := qv.fieldIndexViewFromSlotsRef(qv.snap.Index, field, fieldOrdinal)
	if !ov.HasData() {
		return true
	}

	br := ov.RangeForBounds(rb)
	if br.Empty() {
		return true
	}

	bucketCount, est := ov.RangeStats(br)
	if bucketCount == 0 || est == 0 {
		return true
	}

	universe := qv.snap.Universe.Cardinality()
	if universe == 0 {
		return false
	}

	positiveWork := rangeProbeMaterializeWork(bucketCount, est)
	if positiveWork == 0 {
		return false
	}

	complementBuckets := ov.KeyCount() - bucketCount
	if qv.fieldIndexViewFromSlotsRef(qv.snap.NilIndex, field, fieldOrdinal).LookupCardinality(indexdata.NilIndexEntryKey) > 0 {
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

func (qv *View) prepareOrderBasicBaseCores(baseOps []qir.Expr) ([]orderBasicBaseCore, []int, bool, error) {
	if len(baseOps) == 0 {
		return nil, nil, false, nil
	}

	coresBuf := orderBasicBaseCoreSlicePool.Get(len(baseOps))
	rawCoreIdxBuf := pooled.GetIntSlice(len(baseOps))[:len(baseOps)]

	for i := range rawCoreIdxBuf {
		rawCoreIdxBuf[i] = -1
	}

	for i, op := range baseOps {
		if rawCoreIdxBuf[i] >= 0 {
			continue
		}
		fm := qv.fieldMetaByExpr(op)
		if !isOrderBasicCollapsibleScalarRangeExpr(op, fm) {
			continue
		}
		fieldName := qv.exec.FieldNameByOrdinal(op.FieldOrdinal)

		var rb indexdata.Bounds
		groupCount := 0
		for j := i; j < len(baseOps); j++ {
			if rawCoreIdxBuf[j] >= 0 {
				continue
			}
			other := baseOps[j]
			if other.FieldOrdinal != op.FieldOrdinal || !isOrderBasicCollapsibleScalarRangeExpr(other, fm) {
				continue
			}
			nextRB, ok, err := qv.rangeBoundsForScalarExpr(other)
			if err != nil {
				orderBasicBaseCoreSlicePool.Put(coresBuf)
				pooled.ReleaseIntSlice(rawCoreIdxBuf)
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

		if rb.Empty {
			orderBasicBaseCoreSlicePool.Put(coresBuf)
			pooled.ReleaseIntSlice(rawCoreIdxBuf)
			return nil, nil, true, nil
		}
		if !qv.shouldCollapseOrderBasicScalarRange(fieldName, op.FieldOrdinal, rb) {
			continue
		}

		coreIdx := len(coresBuf)
		coresBuf = append(coresBuf, orderBasicBaseCore{
			kind: orderBasicBaseCoreCollapsedRange,
			collapsed: preparedScalarExactRange{
				field:    fieldName,
				bounds:   rb,
				cacheKey: qv.materializedPredKeyForExactScalarRange(fieldName, rb),
			},
		})
		for j := i; j < len(baseOps); j++ {
			other := baseOps[j]
			if other.FieldOrdinal == op.FieldOrdinal && isOrderBasicCollapsibleScalarRangeExpr(other, fm) {
				rawCoreIdxBuf[j] = coreIdx
			}
		}
	}

	for i, op := range baseOps {
		if rawCoreIdxBuf[i] >= 0 {
			continue
		}
		rawCoreIdxBuf[i] = len(coresBuf)
		coresBuf = append(coresBuf, orderBasicBaseCore{
			kind: orderBasicBaseCoreRawExpr,
			expr: op,
		})
	}
	return coresBuf, rawCoreIdxBuf, false, nil
}

func (qv *View) loadWarmOrderBasicBaseCore(core orderBasicBaseCore) (postingResult, bool) {
	switch core.kind {
	case orderBasicBaseCoreCollapsedRange:
		return qv.loadWarmPreparedScalarExactRange(core.collapsed)
	case orderBasicBaseCoreRawExpr:
		return qv.loadWarmOrderBasicRawBaseOp(core.expr)
	default:
		return postingResult{}, false
	}
}

func (qv *View) evalOrderBasicBaseCore(core orderBasicBaseCore) (postingResult, error) {
	switch core.kind {
	case orderBasicBaseCoreCollapsedRange:
		return qv.evalPreparedScalarExactRange(core.collapsed)
	case orderBasicBaseCoreRawExpr:
		return qv.evalOrderBasicRawBaseOp(core.expr)
	default:
		return postingResult{}, nil
	}
}

func (qv *View) shouldPromoteObservedOrderBasicBaseCore(
	orderField string,
	core orderBasicBaseCore,
	universe, observedRows, needWindow uint64,
) bool {
	switch core.kind {
	case orderBasicBaseCoreCollapsedRange:
		return qv.shouldPromoteObservedPreparedScalarExactRange(core.collapsed, observedRows, needWindow)

	case orderBasicBaseCoreRawExpr:
		if qv.snap.MaterializedPredCacheLimit() <= 0 {
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

func (qv *View) materializedPredObservationCandidate(key qcache.MaterializedPredKey, est, buildWork uint64) bool {
	if key.IsZero() || buildWork == 0 {
		return false
	}
	if qv.snap.HasMaterializedPredKey(key) {
		return false
	}
	cache := qv.snap.MaterializedPredCache()
	maxCard := cache.MaxCardinality()
	return maxCard == 0 || est <= maxCard || qcache.MaterializedPredOversizedLimit(cache.Limit()) > 0
}

func (qv *View) orderBasicBaseCoreObservationCandidate(orderField string, core orderBasicBaseCore, universe uint64) bool {
	switch core.kind {

	case orderBasicBaseCoreCollapsedRange:
		stats, ok := qv.orderedLimitCollapsedRangeStats(core.collapsed)
		if !ok {
			return false
		}
		buildWork := rangeProbeMaterializeWork(stats.buildBuckets, stats.buildEst)
		return qv.materializedPredObservationCandidate(stats.cacheKey, stats.buildEst, buildWork)

	case orderBasicBaseCoreRawExpr:
		op := core.expr
		if op.Not || op.FieldOrdinal < 0 || qv.exec.FieldNameByOrdinal(op.FieldOrdinal) == orderField {
			return false
		}
		stats, ok := qv.orderBasicRawBaseOpStats(op, universe)
		if !ok {
			return false
		}
		buildWork := rangeProbeMaterializeWork(stats.buildBuckets, stats.buildEst)
		return qv.materializedPredObservationCandidate(stats.cacheKey, stats.buildEst, buildWork)

	default:
		return false
	}
}

func (qv *View) hasOrderBasicBaseOpObservationCandidate(orderField string, baseOps []qir.Expr) bool {
	if qv.snap.MaterializedPredCacheLimit() <= 0 || len(baseOps) == 0 {
		return false
	}
	universe := qv.snap.Universe.Cardinality()
	if universe == 0 {
		return false
	}
	if len(baseOps) == 1 {
		return qv.orderBasicBaseCoreObservationCandidate(orderField, orderBasicBaseCore{
			kind: orderBasicBaseCoreRawExpr,
			expr: baseOps[0],
		}, universe)
	}

	coresBuf, rawCoreIdxBuf, empty, err := qv.prepareOrderBasicBaseCores(baseOps)
	if err != nil || empty {
		if coresBuf != nil {
			orderBasicBaseCoreSlicePool.Put(coresBuf)
		}
		if rawCoreIdxBuf != nil {
			pooled.ReleaseIntSlice(rawCoreIdxBuf)
		}
		return false
	}

	found := false
	for i := 0; i < len(coresBuf); i++ {
		if qv.orderBasicBaseCoreObservationCandidate(orderField, coresBuf[i], universe) {
			found = true
			break
		}
	}
	orderBasicBaseCoreSlicePool.Put(coresBuf)
	pooled.ReleaseIntSlice(rawCoreIdxBuf)
	return found
}

func (qv *View) hasLimitLeafPredObservationCandidate(orderField string, preds []leafPred) bool {
	if qv.snap.MaterializedPredCacheLimit() <= 0 || len(preds) == 0 {
		return false
	}
	universe := qv.snap.Universe.Cardinality()
	if universe == 0 {
		return false
	}
	for i := 0; i < len(preds); i++ {
		pred := preds[i]
		if pred.kind != leafPredKindPredicate {
			continue
		}
		if pred.hasBaseCore {
			if qv.orderBasicBaseCoreObservationCandidate(orderField, pred.baseCore, universe) {
				return true
			}
			continue
		}
		op := pred.pred.expr
		if !isSimpleScalarRangeOrPrefixLeaf(op) || op.FieldOrdinal < 0 || qv.exec.FieldNameByOrdinal(op.FieldOrdinal) == orderField || op.Not {
			continue
		}
		if qv.orderBasicBaseCoreObservationCandidate(orderField, orderBasicBaseCore{
			kind: orderBasicBaseCoreRawExpr,
			expr: op,
		}, universe) {
			return true
		}
	}
	return false
}

func (qv *View) promoteObservedOrderBasicBaseCore(core orderBasicBaseCore) {
	switch core.kind {
	case orderBasicBaseCoreCollapsedRange:
		cacheKey := core.collapsed.cacheKey
		if !cacheKey.IsZero() {
			if qv.snap.HasMaterializedPredKey(cacheKey) {
				return
			}
		}
		ids, err := qv.evalPreparedScalarExactRange(core.collapsed)
		if err == nil {
			ids.ids.Release()
		}

	case orderBasicBaseCoreRawExpr:
		stats, ok := qv.orderBasicRawBaseOpStats(core.expr, qv.snap.Universe.Cardinality())
		cacheKey := qcache.MaterializedPredKey{}
		if ok {
			cacheKey = stats.cacheKey
		}
		if cacheKey.IsZero() {
			return
		}
		if qv.snap.HasMaterializedPredKey(cacheKey) {
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

func (qv *View) loadFirstWarmOrderBasicBaseCore(cores []orderBasicBaseCore) (int, postingResult, bool) {
	if cores == nil {
		return -1, postingResult{}, false
	}
	for i := 0; i < len(cores); i++ {
		if hit, ok := qv.loadWarmOrderBasicBaseCore(cores[i]); ok {
			return i, hit, true
		}
	}
	return -1, postingResult{}, false
}

func (qv *View) orderBasicRawBaseOpStats(
	op qir.Expr,
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

func (qv *View) shouldPromoteObservedOrderBasicRawBaseOp(
	orderField string,
	op qir.Expr,
	universe, observedRows, needWindow uint64,
) bool {
	if universe == 0 || observedRows == 0 || op.Not || op.FieldOrdinal < 0 || qv.exec.FieldNameByOrdinal(op.FieldOrdinal) == orderField {
		return false
	}
	if needWindow == 0 {
		needWindow = 1
	}
	if observedRows <= needWindow {
		return false
	}
	stats, ok := qv.orderBasicRawBaseOpStats(op, universe)
	emptyComplement := ok && stats.buildComplement && stats.buildEst == 0
	if !ok || stats.cacheKey.IsZero() || (!emptyComplement && (stats.probeBuckets == 0 || stats.probeEst == 0)) {
		return false
	}

	buildWork := rangeProbeMaterializeWork(stats.buildBuckets, stats.buildEst)
	if buildWork == 0 {
		return false
	}
	if !qv.materializedPredObservationCandidate(stats.cacheKey, stats.buildEst, buildWork) {
		return false
	}

	if observedRows > universe {
		observedRows = universe
	}

	probeWork := rangeAdaptiveProbeWorkForRows(observedRows, stats.probeBuckets, stats.probeEst)
	if probeWork == 0 {
		return false
	}
	return qv.snap.ShouldPromoteObservedMaterializedPredKey(stats.cacheKey, probeWork, buildWork)
}

func (qv *View) materializeOrderBasicLimitComplementBaseOp(op qir.Expr, cacheKey qcache.MaterializedPredKey) bool {
	if cacheKey.IsZero() || op.FieldOrdinal < 0 {
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
		qv.snap.StoreMaterializedPredKey(cacheKey, posting.List{})
		return true
	}

	ids := candidate.core.materializeComplement(plan)
	if ids.IsEmpty() {
		qv.snap.StoreMaterializedPredKey(cacheKey, posting.List{})
		return true
	}

	ids = tryShareMaterializedPredOnSnapshot(qv.snap, cacheKey, ids)
	ids.Release()

	return true
}

func (qv *View) promoteOrderBasicLimitMaterializedBaseOps(orderField string, baseOps []qir.Expr, observedRows uint64, needWindow uint64) {
	if qv.snap == nil || qv.snap.MaterializedPredCacheLimit() <= 0 || len(baseOps) == 0 || observedRows == 0 {
		return
	}
	universe := qv.snap.Universe.Cardinality()
	if universe == 0 {
		return
	}

	coresBuf, rawCoreIdxBuf, empty, err := qv.prepareOrderBasicBaseCores(baseOps)
	if err != nil || empty {
		if coresBuf != nil {
			orderBasicBaseCoreSlicePool.Put(coresBuf)
		}
		if rawCoreIdxBuf != nil {
			pooled.ReleaseIntSlice(rawCoreIdxBuf)
		}
		return
	}

	defer orderBasicBaseCoreSlicePool.Put(coresBuf)
	defer pooled.ReleaseIntSlice(rawCoreIdxBuf)

	keysBuf := qcache.GetMaterializedPredKeySlice(len(coresBuf))
	defer qcache.ReleaseMaterializedPredKeySlice(keysBuf)

	for i := 0; i < len(coresBuf); i++ {
		core := coresBuf[i]
		if core.kind == orderBasicBaseCoreRawExpr {
			fm := qv.fieldMetaByExpr(core.expr)
			if isOrderBasicCollapsibleScalarRangeExpr(core.expr, fm) {
				groupCount := 0
				for j := 0; j < len(baseOps); j++ {
					op := baseOps[j]
					if op.FieldOrdinal == core.expr.FieldOrdinal && isOrderBasicCollapsibleScalarRangeExpr(op, fm) {
						groupCount++
					}
				}
				if groupCount > 1 {
					// The ordered predicate builder merges same-field scalar bounds; promoting
					// individual half-ranges adds large cache entries that this route won't use.
					continue
				}
			}
		}
		if !qv.shouldPromoteObservedOrderBasicBaseCore(orderField, core, universe, observedRows, needWindow) {
			continue
		}
		key := qcache.MaterializedPredKey{}
		switch core.kind {
		case orderBasicBaseCoreCollapsedRange:
			key = core.collapsed.cacheKey
		case orderBasicBaseCoreRawExpr:
			stats, ok := qv.orderBasicRawBaseOpStats(core.expr, qv.snap.Universe.Cardinality())
			if ok {
				key = stats.cacheKey
			}
		}
		if key.IsZero() {
			continue
		}
		seen := false
		for j := 0; j < len(keysBuf); j++ {
			if keysBuf[j] == key {
				seen = true
				break
			}
		}
		if seen {
			continue
		}
		keysBuf = append(keysBuf, key)
		qv.promoteObservedOrderBasicBaseCore(core)
	}
}

func (qv *View) promoteObservedLimitLeafPreds(orderField string, preds []leafPred, observedRows uint64, needWindow uint64) {
	if qv.snap == nil || qv.snap.MaterializedPredCacheLimit() <= 0 || len(preds) == 0 || observedRows == 0 {
		return
	}
	universe := qv.snap.Universe.Cardinality()
	if universe == 0 {
		return
	}

	keysBuf := qcache.GetMaterializedPredKeySlice(len(preds))
	defer qcache.ReleaseMaterializedPredKeySlice(keysBuf)

	for i := 0; i < len(preds); i++ {
		pred := preds[i]
		if pred.kind != leafPredKindPredicate {
			continue
		}

		var core orderBasicBaseCore

		if pred.hasBaseCore {
			core = pred.baseCore
		} else {
			op := pred.pred.expr
			if !isSimpleScalarRangeOrPrefixLeaf(op) || op.FieldOrdinal < 0 || qv.exec.FieldNameByOrdinal(op.FieldOrdinal) == orderField || op.Not {
				continue
			}
			core = orderBasicBaseCore{
				kind: orderBasicBaseCoreRawExpr,
				expr: op,
			}
		}

		if !qv.shouldPromoteObservedOrderBasicBaseCore(orderField, core, universe, observedRows, needWindow) {
			continue
		}

		key := qcache.MaterializedPredKey{}

		switch core.kind {
		case orderBasicBaseCoreCollapsedRange:
			key = core.collapsed.cacheKey

		case orderBasicBaseCoreRawExpr:
			stats, ok := qv.orderBasicRawBaseOpStats(core.expr, universe)
			if ok {
				key = stats.cacheKey
			}
		}

		if key.IsZero() {
			continue
		}

		seen := false
		for j := 0; j < len(keysBuf); j++ {
			if keysBuf[j] == key {
				seen = true
				break
			}
		}
		if seen {
			continue
		}
		keysBuf = append(keysBuf, key)

		qv.promoteObservedOrderBasicBaseCore(core)
	}
}

func (qv *View) evalOrderBasicRawBaseOp(op qir.Expr) (postingResult, error) {
	stats, ok := qv.orderBasicRawBaseOpStats(op, qv.snap.Universe.Cardinality())
	cacheKey := qcache.MaterializedPredKey{}
	if ok {
		cacheKey = stats.cacheKey
	}
	if !cacheKey.IsZero() {
		if cached, ok := qv.snap.LoadMaterializedPredKey(cacheKey); ok {
			if cacheKey == qv.materializedPredKey(op) {
				return postingResult{ids: cached}, nil
			}
			return postingResult{ids: cached, neg: true}, nil
		}
	}
	if ok && stats.buildComplement {
		candidate, ok := qv.prepareScalarRangeRoutingCandidate(op)
		if ok {
			plan, ok := candidate.core.prepareComplementMaterialization()
			if ok {
				if plan.est == 0 {
					if !cacheKey.IsZero() {
						qv.snap.StoreMaterializedPredKey(cacheKey, posting.List{})
					}
					return postingResult{neg: true}, nil
				}
				ids := candidate.core.materializeComplement(plan)
				if ids.IsEmpty() {
					if !cacheKey.IsZero() {
						qv.snap.StoreMaterializedPredKey(cacheKey, posting.List{})
					}
					return postingResult{neg: true}, nil
				}
				ids = tryShareMaterializedPredOnSnapshot(qv.snap, cacheKey, ids)
				return postingResult{ids: ids, neg: true}, nil
			}
		}
	}
	return qv.evalExpr(op)
}

func (qv *View) loadWarmOrderBasicRawBaseOp(op qir.Expr) (postingResult, bool) {
	candidate, ok := qv.prepareScalarRangeRoutingCandidate(op)
	if !ok {
		return postingResult{}, false
	}
	return candidate.core.loadWarmScalarPostingResult()
}

// Require a large residual range per requested row before pre-materializing it
// for an order-field-bounded LIMIT scan.
const orderBasicBoundedRangeBaseMinRowsPerNeed = 64

func (qv *View) dispatchLimitMaterialized(q *qir.Shape) ([]uint64, bool, PlanName, error) {
	out, err := qv.queryMaterialized(q)
	return out, true, PlanMaterialized, err
}

func (qv *View) dispatchOrderedLimitFallback(
	q *qir.Shape,
	decision plannerOrderedLimitDecision,
) ([]uint64, bool, PlanName, error) {
	if decision.materializedFallback.kind == plannerOrderedLimitCandidateMaterializedFallback {
		return qv.dispatchLimitMaterialized(q)
	}
	return nil, true, "", fmt.Errorf("selected ordered LIMIT route %s was not executable", decision.selected.kind.String())
}

// executeOrderedLimit is the ordered LIMIT selector-family boundary: it may
// collect cheap facts, but physical routes are chosen only by selectOrderedLimit.
func (qv *View) executeOrderedLimit(q *qir.Shape, trace *Trace) ([]uint64, bool, PlanName, error) {
	facts := orderedLimitFactsPool.Get()
	defer facts.Release()

	ok, err := qv.collectOrderedLimitFacts(q, facts)
	if err != nil {
		return nil, ok, "", err
	}
	if !ok {
		return nil, false, "", nil
	}

	decision, ok, err := qv.selectOrderedLimit(q, facts)
	if err != nil {
		return nil, true, "", err
	}
	if !ok {
		return nil, false, "", nil
	}

	guard := decision.runtimeGuard(q)
	if !guard.enabled {
		guard = decision.baseCoreRuntimeGuard(q, len(facts.baseOps))
	}
	if trace != nil {
		trace.SetOrderedLimitRoute(qv.traceOrderedLimitRoute(q, facts, decision))
		fallbackCost := decision.materializedFallback.cost
		if fallbackCost <= 0 {
			fallbackCost = decision.rejected.cost
		}
		trace.SetEstimated(decision.selected.expectedRows, decision.selected.cost, fallbackCost)
		trace.SetOrderedLimitRuntimeGuard(guard.enabled, guard.reason)
	}

	return qv.dispatchOrderedLimit(q, facts, decision, guard, trace)
}

func (qv *View) dispatchOrderedLimit(
	q *qir.Shape,
	facts *orderedLimitFacts,
	decision plannerOrderedLimitDecision,
	guard plannerOrderedLimitRuntimeGuard,
	trace *Trace,
) ([]uint64, bool, PlanName, error) {
	order := facts.order
	f := facts.orderField
	fm := facts.orderMeta
	ops := facts.ops
	baseOps := facts.baseOps
	needWindow := facts.needWindow
	ov := facts.ov
	br := facts.br
	nilTailField := facts.nilTailField

	if facts.empty {
		if facts.validateBase {
			if err := qv.validateOrderBasicBaseOps(baseOps); err != nil {
				return nil, true, "", err
			}
		}
		return nil, true, PlanLimitOrderBasic, nil
	}

	selected := decision.selectedKind()

dispatch:
	switch selected {
	case plannerOrderedLimitCandidateEmpty:
		return nil, true, PlanLimitOrderBasic, nil

	case plannerOrderedLimitCandidateMaterializedFallback:
		return qv.dispatchLimitMaterialized(q)

	case plannerOrderedLimitCandidateCandidateOrder:
		predSet, ok := qv.buildPredicatesCandidate(ops)
		if !ok {
			return qv.dispatchOrderedLimitFallback(q, decision)
		}
		defer predSet.Release()
		for i := 0; i < predSet.Len(); i++ {
			if predSet.owner[i].alwaysFalse {
				return nil, true, PlanCandidateOrder, nil
			}
		}
		return qv.execPlanCandidateOrderBasic(q, predSet.owner, trace), true, PlanCandidateOrder, nil

	case plannerOrderedLimitCandidateOrderScan:
		if len(baseOps) == 0 {
			out, _ := qv.scanOrderLimitNoPredicates(q, ov, br, order.Desc, nilTailField, trace)
			plan := PlanLimitOrderBasic
			if qv.hasPrefixBoundForField(ops, f) {
				plan = PlanLimitOrderPrefix
			}
			return out, true, plan, nil
		}
		if trace == nil && len(baseOps) == 1 {
			e := baseOps[0]
			if e.Op == qir.OpEQ && e.FieldOrdinal >= 0 && len(e.Operands) == 0 {
				residualMeta := qv.fieldMetaByExpr(e)
				residualView := qv.fieldIndexViewFromSlotsForExpr(qv.snap.Index, e)
				if residualMeta != nil && !residualMeta.Slice && residualView.HasData() && (!e.Not || (!residualMeta.UseVI && !residualMeta.Ptr && residualMeta.Kind == reflect.Bool)) {
					filterExpr := e
					if e.Not {
						v, ok := e.Value.(bool)
						if !ok {
							goto orderScanPostingFilterDone
						}
						filterExpr.Not = false
						filterExpr.Value = !v
					}
					key, isSlice, isNil, err := qv.exprValueToLookupKey(filterExpr)
					if err != nil {
						return nil, true, "", err
					}
					if !isSlice && !isNil {
						filter := lookupScalarPostingRetained(residualView, key)
						plan := PlanLimitOrderBasic
						if facts.bounds.HasPrefix {
							plan = PlanLimitOrderPrefix
						}
						if filter.IsEmpty() {
							return nil, true, plan, nil
						}
						need, exhausted := boundedWindowCap(filter.Cardinality(), q.Offset, q.Limit)
						if exhausted {
							return nil, true, plan, nil
						}
						if guard.enabled {
							out, ok := scanLimitByFieldIndexBoundsPostingFilterNoTrace(q, ov, br, order.Desc, filter, need, guard)
							if !ok {
								if decision.runtimeFallback.kind != plannerOrderedLimitCandidateNone {
									selected = decision.runtimeFallback.kind
									guard = plannerOrderedLimitRuntimeGuard{}
									goto dispatch
								}
								return qv.dispatchOrderedLimitFallback(q, decision)
							}
							if uint64(len(out)) < need {
								if nilTailField != "" {
									goto orderScanPostingFilterDone
								}
							}
							return out, true, plan, nil
						}
						out := make([]uint64, 0, clampUint64ToInt(need))
						out, _, _ = ov.AppendPostingFilter(out, br, order.Desc, filter, q.Offset, need)
						if uint64(len(out)) < need && nilTailField != "" {
							goto orderScanPostingFilterDone
						}
						return out, true, plan, nil
					}
				}
			}
		}
	orderScanPostingFilterDone:
		if q.Offset == 0 || !fm.Ptr || nilTailField != "" {
			predsBuf, ok, err := qv.buildLeafPredsExcludingBounds(ops, f, needWindow)
			if err != nil {
				return nil, true, "", err
			}
			if ok {
				if hasEmptyLeafPred(predsBuf) {
					if predsBuf != nil {
						leafPredSlicePool.Put(predsBuf)
					}
					return nil, true, PlanLimitOrderBasic, nil
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
				observePreds := qv.hasLimitLeafPredObservationCandidate(f, predsBuf)
				var observedTrace Trace
				observedStart := uint64(0)
				if observePreds && execTrace == nil {
					execTrace = &observedTrace
				} else if observePreds {
					observedStart = execTrace.RowsExamined()
				}

				scanGuard := limitScanGuard{}
				if guard.enabled {
					scanGuard = limitScanGuard{
						enabled:     true,
						minExamined: guard.minExamined,
						needWindow:  guard.needWindow,
						maxCost:     guard.fallbackCost * 1.25,
						rowCost:     guard.rowCost,
						reason:      guard.reason,
					}
				}
				out, ok := qv.scanLimitByFieldIndexBounds(q, ov, br, order.Desc, predsBuf, nilTailField, scanGuard, execTrace)
				if !ok {
					if predsBuf != nil {
						leafPredSlicePool.Put(predsBuf)
					}
					if guard.enabled && decision.runtimeFallback.kind != plannerOrderedLimitCandidateNone {
						selected = decision.runtimeFallback.kind
						guard = plannerOrderedLimitRuntimeGuard{}
						goto dispatch
					}
					return qv.dispatchOrderedLimitFallback(q, decision)
				}
				if observePreds {
					qv.promoteObservedLimitLeafPreds(f, predsBuf, execTrace.RowsExamined()-observedStart, uint64(needWindow))
				}
				if predsBuf != nil {
					leafPredSlicePool.Put(predsBuf)
				}
				plan := PlanLimitOrderBasic
				if qv.hasPrefixBoundForField(ops, f) {
					plan = PlanLimitOrderPrefix
				}
				return out, true, plan, nil
			}
		}
		return qv.dispatchOrderedLimitFallback(q, decision)

	case plannerOrderedLimitCandidateOrderedBasic:
		predSet, ok := qv.buildPredicatesOrderedWithMode(ops, f, false, needWindow, q.Offset, true, true)
		if !ok {
			return qv.dispatchOrderedLimitFallback(q, decision)
		}
		for i := 0; i < predSet.Len(); i++ {
			if predSet.owner[i].alwaysFalse {
				predSet.Release()
				return nil, true, PlanOrdered, nil
			}
		}
		execTrace := trace
		observeBaseOps := qv.hasOrderBasicBaseOpObservationCandidate(f, baseOps)
		var observedTrace Trace
		observedStart := uint64(0)
		if observeBaseOps {
			if execTrace == nil {
				execTrace = &observedTrace
			} else {
				observedStart = execTrace.RowsExamined()
			}
		}
		out, ok := qv.execPlanOrderedBasicReaderGuarded(q, predSet.owner, plannerOrderedLimitRuntimeGuard{}, execTrace)
		predSet.Release()
		if !ok {
			return qv.dispatchOrderedLimitFallback(q, decision)
		}
		if observeBaseOps {
			qv.promoteOrderBasicLimitMaterializedBaseOps(f, baseOps, execTrace.RowsExamined()-observedStart, uint64(needWindow))
		}
		if trace != nil && trace.ev.Plan != "" {
			return out, true, "", nil
		}
		return out, true, PlanOrdered, nil
	}

	if !selected.usesBaseCore() {
		return nil, false, "", nil
	}

	baseCoresBuf, baseRawCoreIdxBuf, noMatch, err := qv.prepareOrderBasicBaseCores(baseOps)
	if err != nil {
		return nil, true, "", err
	}
	if noMatch {
		if baseCoresBuf != nil {
			orderBasicBaseCoreSlicePool.Put(baseCoresBuf)
		}
		if baseRawCoreIdxBuf != nil {
			pooled.ReleaseIntSlice(baseRawCoreIdxBuf)
		}
		return nil, true, PlanLimitOrderBasic, nil
	}
	warmBaseIdx, warmBase, hasWarmBaseOps := qv.loadFirstWarmOrderBasicBaseCore(baseCoresBuf)

	var (
		base              postingResult
		residualPredSet   predicateSet
		residualActiveBuf []int
		residualActive    []int
		warmBaseLoaded    bool
	)

	if hasWarmBaseOps {
		if len(baseCoresBuf) == 1 {
			base = warmBase
			warmBaseLoaded = true
		}
	}

	if hasWarmBaseOps && !warmBaseLoaded {
		var loadedCoreInline [limitQueryFastPathMaxLeaves]bool
		var loadedCoreBuf []bool
		var loadedCoreHeap []bool
		if len(baseCoresBuf) <= limitQueryFastPathMaxLeaves {
			loadedCoreBuf = loadedCoreInline[:len(baseCoresBuf)]
		} else {
			loadedCoreHeap = pooled.GetBoolSlice(len(baseCoresBuf))[:len(baseCoresBuf)]
			clear(loadedCoreHeap)
			loadedCoreBuf = loadedCoreHeap
		}

		base = warmBase
		loadedCoreBuf[warmBaseIdx] = true
		loadedCount := 1
		for i := 0; i < len(baseCoresBuf); i++ {
			if i == warmBaseIdx {
				continue
			}
			b, ok := qv.loadWarmOrderBasicBaseCore(baseCoresBuf[i])
			if !ok {
				continue
			}
			loadedCoreBuf[i] = true
			loadedCount++
			var err error
			base, err = qv.andPostingResult(base, b)
			if err != nil {
				if loadedCoreHeap != nil {
					pooled.ReleaseBoolSlice(loadedCoreHeap)
				}
				orderBasicBaseCoreSlicePool.Put(baseCoresBuf)
				pooled.ReleaseIntSlice(baseRawCoreIdxBuf)
				base.ids.Release()
				return nil, true, "", err
			}
		}

		if loadedCount < len(baseCoresBuf) {
			residualOpsBuf := qir.GetExprSlice(len(baseOps))
			for i, op := range baseOps {
				if loadedCoreBuf[baseRawCoreIdxBuf[i]] {
					continue
				}
				residualOpsBuf = append(residualOpsBuf, op)
			}
			window, _ := orderWindow(q)
			var ok bool
			residualPredSet, ok = qv.buildPredicatesOrderedWithMode(residualOpsBuf, f, false, window, q.Offset, false, true)
			qir.ReleaseExprSlice(residualOpsBuf)

			if !ok {
				base.ids.Release()
				base = postingResult{}
				hasWarmBaseOps = false
				residualPredSet.Release()

			} else {
				var residualActiveInline [limitQueryFastPathMaxLeaves]int
				if residualPredSet.Len() <= limitQueryFastPathMaxLeaves {
					residualActive = residualActiveInline[:0]
				} else {
					residualActiveBuf = pooled.GetIntSlice(residualPredSet.Len())
				}
				for i := 0; i < residualPredSet.Len(); i++ {
					p := residualPredSet.owner[i]
					if p.alwaysFalse {
						if loadedCoreHeap != nil {
							pooled.ReleaseBoolSlice(loadedCoreHeap)
						}
						orderBasicBaseCoreSlicePool.Put(baseCoresBuf)
						pooled.ReleaseIntSlice(baseRawCoreIdxBuf)
						base.ids.Release()
						releaseOrderBasicResidualState(residualPredSet, residualActiveBuf)
						return nil, true, PlanLimitOrderBasic, nil
					}
					if p.covered || p.alwaysTrue {
						continue
					}
					if residualActiveBuf != nil {
						residualActiveBuf = append(residualActiveBuf, i)
						continue
					}
					residualActive = append(residualActive, i)
				}
			}
		}
		if loadedCoreHeap != nil {
			pooled.ReleaseBoolSlice(loadedCoreHeap)
		}
	}
	if baseRawCoreIdxBuf != nil {
		pooled.ReleaseIntSlice(baseRawCoreIdxBuf)
	}

	if !hasWarmBaseOps {
		baseBuilt := false
		recordMaterializedWork := qv.snap.MaterializedPredCacheLimit() > 0
		universe := uint64(0)
		hasOrderBounds := false
		if recordMaterializedWork {
			universe = qv.snap.Universe.Cardinality()
			hasOrderBounds = facts.bounds.HasLo || facts.bounds.HasHi || facts.bounds.HasPrefix
		}
		for i := 0; i < len(baseCoresBuf); i++ {
			b, err := qv.evalOrderBasicBaseCore(baseCoresBuf[i])
			if err != nil {
				orderBasicBaseCoreSlicePool.Put(baseCoresBuf)
				base.ids.Release()
				return nil, true, "", err
			}
			if recordMaterializedWork {
				st, ok := qv.orderedLimitBaseCoreStats(baseCoresBuf[i], universe, needWindow, hasOrderBounds)
				if ok {
					qv.snap.ShouldPromoteObservedMaterializedPredKey(st.cacheKey, st.buildWork, st.buildWork)
				}
			}
			if !baseBuilt {
				base = b
				baseBuilt = true
				continue
			}
			base, err = qv.andPostingResult(base, b)
			if err != nil {
				orderBasicBaseCoreSlicePool.Put(baseCoresBuf)
				base.ids.Release()
				return nil, true, "", err
			}
		}
	}
	if baseCoresBuf != nil {
		orderBasicBaseCoreSlicePool.Put(baseCoresBuf)
	}

	if !base.neg && base.ids.IsEmpty() {
		base.ids.Release()
		releaseOrderBasicResidualState(residualPredSet, residualActiveBuf)
		return nil, true, PlanLimitOrderBasic, nil
	}

	var (
		out  []uint64
		used bool
	)
	if residualActiveBuf != nil {
		out, used, err = qv.runOrderBasicBaseQuery(q, f, baseOps, needWindow, order, ov, br, nilTailField, base, residualPredSet.owner, residualActiveBuf, guard, trace)
	} else {
		out, used, err = qv.runOrderBasicBaseQuery(q, f, baseOps, needWindow, order, ov, br, nilTailField, base, residualPredSet.owner, residualActive, guard, trace)
	}

	releaseOrderBasicResidualState(residualPredSet, residualActiveBuf)

	if err != nil {
		return out, used, PlanLimitOrderBasic, err
	}
	if !used {
		return qv.dispatchOrderedLimitFallback(q, decision)
	}

	return out, used, PlanLimitOrderBasic, err
}

const fieldIndexRangeExactCapMinLimit = 4096

func fieldIndexRangeWindowCap(ov indexdata.FieldIndexView, br indexdata.FieldIndexRange, offset, limit, extraRows uint64) (uint64, bool) {
	window := satAddUint64(offset, limit)
	if br.Empty() {
		return boundedWindowCap(extraRows, offset, limit)
	}
	if limit != 0 && limit <= fieldIndexRangeExactCapMinLimit {
		return limit, false
	}
	if uint64(br.Len()) < window {
		_, rows := ov.RangeStats(br)
		if extraRows != 0 {
			rows = satAddUint64(rows, extraRows)
		}
		return boundedWindowCap(rows, offset, limit)
	}
	return limit, limit == 0
}

func (qv *View) scanOrderLimitNoPredicates(q *qir.Shape, ov indexdata.FieldIndexView, br indexdata.FieldIndexRange, desc bool, nilTailField string, trace *Trace) ([]uint64, bool) {
	var extraRows uint64
	if nilTailField != "" {
		extraRows = qv.fieldIndexViewFromSlotsByName(qv.snap.NilIndex, nilTailField).LookupCardinality(indexdata.NilIndexEntryKey)
	}
	capHint, exhausted := fieldIndexRangeWindowCap(ov, br, q.Offset, q.Limit, extraRows)
	if exhausted {
		if trace != nil {
			trace.AddExamined(0)
			trace.SetEarlyStopReason("input_exhausted")
		}
		return nil, true
	}
	out := make([]uint64, 0, clampUint64ToInt(capHint))
	if trace == nil {
		limit := clampUint64ToInt(q.Limit)
		if limit <= 0 {
			return out, true
		}
		skip := q.Offset
		keyCur := ov.NewCursor(br, desc)
		for {
			ids, idx, single, ok := keyCur.NextPostingOrSingle()
			if !ok {
				break
			}
			if single {
				if skip > 0 {
					skip--
					continue
				}
				out = append(out, idx)
				if len(out) >= limit {
					return out, true
				}
				continue
			}
			out, skip, ok = appendPostingWindowNoTrace(out, ids, skip, limit)
			if ok {
				return out, true
			}
		}
		if nilTailField != "" {
			ids := qv.fieldIndexViewFromSlotsByName(qv.snap.NilIndex, nilTailField).LookupPostingRetained(indexdata.NilIndexEntryKey)
			out, _, _ = appendPostingWindowNoTrace(out, ids, skip, limit)
		}
		return out, true
	}

	cursor := newQueryCursor(out, q.Offset, q.Limit, false, 0)

	var (
		examined       uint64
		scanWidth      uint64
		trackScanWidth = q.HasOrder
	)

	keyCur := ov.NewCursor(br, desc)
	for {
		ids, idx, single, ok := keyCur.NextPostingOrSingle()
		if !ok {
			break
		}
		if single {
			if trackScanWidth {
				scanWidth++
			}
			examined++
			trace.AddMatched(1)
			if cursor.emit(idx) {
				trace.AddExamined(examined)
				if trackScanWidth {
					trace.AddOrderScanWidth(scanWidth)
				}
				trace.SetEarlyStopReason("limit_reached")
				return cursor.out, true
			}
			continue
		}
		if ids.IsEmpty() {
			continue
		}
		if trackScanWidth {
			scanWidth++
		}
		if emitOrderLimitPosting(&cursor, ids, &examined, trace) {
			trace.AddExamined(examined)
			if trackScanWidth {
				trace.AddOrderScanWidth(scanWidth)
			}
			trace.SetEarlyStopReason("limit_reached")
			return cursor.out, true
		}
	}

	if nilTailField != "" {
		ids := qv.fieldIndexViewFromSlotsByName(qv.snap.NilIndex, nilTailField).LookupPostingRetained(indexdata.NilIndexEntryKey)
		if !ids.IsEmpty() {
			if trackScanWidth {
				scanWidth++
			}
			if emitOrderLimitPosting(&cursor, ids, &examined, trace) {
				trace.AddExamined(examined)
				if trackScanWidth {
					trace.AddOrderScanWidth(scanWidth)
				}
				trace.SetEarlyStopReason("limit_reached")
				return cursor.out, true
			}
		}
	}

	trace.AddExamined(examined)
	if trackScanWidth {
		trace.AddOrderScanWidth(scanWidth)
	}
	trace.SetEarlyStopReason("input_exhausted")

	return cursor.out, true
}

func (qv *View) scanOrderLimitWithPredicatesReader(q *qir.Shape, ov indexdata.FieldIndexView, br indexdata.FieldIndexRange, desc bool, preds predicateReader, nilTailField string, trace *Trace) ([]uint64, bool) {
	return qv.scanOrderLimitWithPredicatesReaderGuarded(q, ov, br, desc, preds, nilTailField, plannerOrderedLimitRuntimeGuard{}, trace)
}

func (qv *View) scanOrderLimitWithPredicatesReaderGuarded(q *qir.Shape, ov indexdata.FieldIndexView, br indexdata.FieldIndexRange, desc bool, preds predicateReader, nilTailField string, guard plannerOrderedLimitRuntimeGuard, trace *Trace) ([]uint64, bool) {
	if len(preds) > limitQueryFastPathMaxLeaves {

		activeBuf := pooled.GetIntSlice(len(preds))
		defer pooled.ReleaseIntSlice(activeBuf)

		for i := 0; i < len(preds); i++ {
			p := preds[i]
			if p.alwaysFalse {
				if trace != nil {
					trace.SetEarlyStopReason("empty_predicate")
				}
				return nil, true
			}
			if p.covered || p.alwaysTrue {
				continue
			}
			if !p.hasContains() {
				return nil, false
			}
			activeBuf = append(activeBuf, i)
		}

		if len(activeBuf) == 0 {
			return qv.scanOrderLimitNoPredicates(q, ov, br, desc, nilTailField, trace)
		}

		sortActivePredicatesReader(activeBuf, preds)

		exactActiveBuf := pooled.GetIntSlice(len(activeBuf))
		defer pooled.ReleaseIntSlice(exactActiveBuf)

		exactActiveBuf = buildExactBucketPostingFilterActiveReader(exactActiveBuf, activeBuf, preds)
		if q.Offset == 0 && len(activeBuf) == 1 && len(exactActiveBuf) == 1 {
			// A single residual check reaches LIMIT faster by testing emitted
			// candidates directly than by exact-filtering every order bucket.
			exactActiveBuf = exactActiveBuf[:0]
		}

		residualActiveBuf := pooled.GetIntSlice(len(activeBuf))
		defer pooled.ReleaseIntSlice(residualActiveBuf)

		residualActiveBuf = plannerResidualChecks(residualActiveBuf, activeBuf, exactActiveBuf)

		exactOnly := len(activeBuf) > 0 && len(activeBuf) == len(exactActiveBuf)

		var extraRows uint64
		if nilTailField != "" {
			extraRows = qv.fieldIndexViewFromSlotsByName(qv.snap.NilIndex, nilTailField).LookupCardinality(indexdata.NilIndexEntryKey)
		}
		capHint, exhausted := fieldIndexRangeWindowCap(ov, br, q.Offset, q.Limit, extraRows)
		if exhausted {
			if trace != nil {
				trace.AddExamined(0)
				trace.SetEarlyStopReason("input_exhausted")
			}
			return nil, true
		}
		out := make([]uint64, 0, clampUint64ToInt(capHint))
		cursor := newQueryCursor(out, q.Offset, q.Limit, false, 0)

		var (
			examined  uint64
			scanWidth uint64
		)

		var exactWork posting.List

		keyCur := ov.NewCursor(br, desc)
		for {
			ids, idx, single, ok := keyCur.NextPostingOrSingle()
			if !ok {
				break
			}
			if single {
				scanWidth++
				if orderPredicatesEmitCandidateReader(&cursor, preds, activeBuf, trace, idx, &examined) {
					if trace != nil {
						trace.AddExamined(examined)
						trace.AddOrderScanWidth(scanWidth)
						trace.SetEarlyStopReason("limit_reached")
					}
					exactWork.Release()
					return cursor.out, true
				}
				if guard.shouldFallback(examined, len(cursor.out)) {
					if trace != nil {
						trace.AddExamined(examined)
						trace.AddOrderScanWidth(scanWidth)
						trace.SetOrderedLimitRuntimeFallback(guard.reason)
						trace.SetEarlyStopReason(guard.reason)
					}
					exactWork.Release()
					return nil, false
				}
				continue
			}
			if ids.IsEmpty() {
				continue
			}
			scanWidth++
			stop, nextExactWork := orderPredicatesEmitPostingReader(
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
					trace.AddExamined(examined)
					trace.AddOrderScanWidth(scanWidth)
					trace.SetEarlyStopReason("limit_reached")
				}
				exactWork.Release()
				return cursor.out, true
			}
			if guard.shouldFallback(examined, len(cursor.out)) {
				if trace != nil {
					trace.AddExamined(examined)
					trace.AddOrderScanWidth(scanWidth)
					trace.SetOrderedLimitRuntimeFallback(guard.reason)
					trace.SetEarlyStopReason(guard.reason)
				}
				exactWork.Release()
				return nil, false
			}
		}

		if nilTailField != "" {
			ids := qv.fieldIndexViewFromSlotsByName(qv.snap.NilIndex, nilTailField).LookupPostingRetained(indexdata.NilIndexEntryKey)
			if !ids.IsEmpty() {
				scanWidth++
				stop, nextExactWork := orderPredicatesEmitPostingReader(
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
						trace.AddExamined(examined)
						trace.AddOrderScanWidth(scanWidth)
						trace.SetEarlyStopReason("limit_reached")
					}
					exactWork.Release()
					return cursor.out, true
				}
			}
		}

		if trace != nil {
			trace.AddExamined(examined)
			trace.AddOrderScanWidth(scanWidth)
			trace.SetEarlyStopReason("input_exhausted")
		}
		exactWork.Release()
		return cursor.out, true
	}

	var activeInline [limitQueryFastPathMaxLeaves]int
	active := activeInline[:0]
	for i := 0; i < len(preds); i++ {
		p := preds[i]
		if p.alwaysFalse {
			if trace != nil {
				trace.SetEarlyStopReason("empty_predicate")
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

	var (
		exactActiveInline    [limitQueryFastPathMaxLeaves]int
		residualActiveInline [limitQueryFastPathMaxLeaves]int
	)
	exactActive := buildExactBucketPostingFilterActiveReader(exactActiveInline[:0], active, preds)
	if q.Offset == 0 && len(active) == 1 && len(exactActive) == 1 {
		// A single residual check reaches LIMIT faster by testing emitted
		// candidates directly than by exact-filtering every order bucket.
		exactActive = exactActive[:0]
	}
	residualActive := plannerResidualChecks(residualActiveInline[:0], active, exactActive)
	exactOnly := len(active) > 0 && len(active) == len(exactActive)

	var extraRows uint64
	if nilTailField != "" {
		extraRows = qv.fieldIndexViewFromSlotsByName(qv.snap.NilIndex, nilTailField).LookupCardinality(indexdata.NilIndexEntryKey)
	}
	capHint, exhausted := fieldIndexRangeWindowCap(ov, br, q.Offset, q.Limit, extraRows)
	if exhausted {
		if trace != nil {
			trace.AddExamined(0)
			trace.SetEarlyStopReason("input_exhausted")
		}
		return nil, true
	}
	out := make([]uint64, 0, clampUint64ToInt(capHint))
	cursor := newQueryCursor(out, q.Offset, q.Limit, false, 0)

	var (
		examined  uint64
		scanWidth uint64
	)

	var exactWork posting.List

	keyCur := ov.NewCursor(br, desc)
	for {
		ids, idx, single, ok := keyCur.NextPostingOrSingle()
		if !ok {
			break
		}
		if single {
			scanWidth++
			if orderPredicatesEmitCandidateReader(&cursor, preds, active, trace, idx, &examined) {
				if trace != nil {
					trace.AddExamined(examined)
					trace.AddOrderScanWidth(scanWidth)
					trace.SetEarlyStopReason("limit_reached")
				}
				exactWork.Release()
				return cursor.out, true
			}
			if guard.shouldFallback(examined, len(cursor.out)) {
				if trace != nil {
					trace.AddExamined(examined)
					trace.AddOrderScanWidth(scanWidth)
					trace.SetOrderedLimitRuntimeFallback(guard.reason)
					trace.SetEarlyStopReason(guard.reason)
				}
				exactWork.Release()
				return nil, false
			}
			continue
		}
		if ids.IsEmpty() {
			continue
		}
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
				trace.AddExamined(examined)
				trace.AddOrderScanWidth(scanWidth)
				trace.SetEarlyStopReason("limit_reached")
			}
			exactWork.Release()
			return cursor.out, true
		}
		if guard.shouldFallback(examined, len(cursor.out)) {
			if trace != nil {
				trace.AddExamined(examined)
				trace.AddOrderScanWidth(scanWidth)
				trace.SetOrderedLimitRuntimeFallback(guard.reason)
				trace.SetEarlyStopReason(guard.reason)
			}
			exactWork.Release()
			return nil, false
		}
	}

	if nilTailField != "" {
		ids := qv.fieldIndexViewFromSlotsByName(qv.snap.NilIndex, nilTailField).LookupPostingRetained(indexdata.NilIndexEntryKey)
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
					trace.AddExamined(examined)
					trace.AddOrderScanWidth(scanWidth)
					trace.SetEarlyStopReason("limit_reached")
				}
				exactWork.Release()
				return cursor.out, true
			}
		}
	}

	if trace != nil {
		trace.AddExamined(examined)
		trace.AddOrderScanWidth(scanWidth)
		trace.SetEarlyStopReason("input_exhausted")
	}
	exactWork.Release()

	return cursor.out, true
}

func scanOrderLimitDenseBaseNoTrace(q *qir.Shape, ov indexdata.FieldIndexView, br indexdata.FieldIndexRange, desc bool, base posting.List, baseCard int, preds predicateReader, active []int, guard plannerOrderedLimitRuntimeGuard) ([]uint64, uint64, bool, bool) {
	if q.Offset >= uint64(baseCard) {
		return nil, 0, true, false
	}
	it := base.Iter()
	minID := it.Next()
	maxID := minID
	for it.HasNext() {
		idx := it.Next()
		if idx < minID {
			minID = idx
		} else if idx > maxID {
			maxID = idx
		}
	}
	it.Release()

	span := maxID - minID + 1
	if span == 0 || span > 4096 {
		return nil, 0, false, false
	}

	limit := clampUint64ToInt(q.Limit)
	skip := int(q.Offset)
	need := limit
	if remaining := baseCard - skip; need > remaining {
		need = remaining
	}
	out := make([]uint64, 0, need)

	if span == uint64(baseCard) {
		keyCur := ov.NewCursor(br, desc)
		examined := uint64(0)
		for len(out) < need {
			ids, idx, single, ok := keyCur.NextPostingOrSingle()
			if !ok {
				break
			}
			if single {
				examined++
				if idx >= minID && idx-minID < span {
					if len(active) == 0 || orderBasicResidualMatchesReader(preds, active, idx) {
						if skip > 0 {
							skip--
						} else {
							out = append(out, idx)
							if len(out) >= need {
								return out, examined, true, false
							}
						}
					}
				}
				if guard.enabled && guard.shouldFallback(examined, len(out)) {
					return nil, examined, true, true
				}
				continue
			}
			it = ids.Iter()
			for it.HasNext() {
				examined++
				idx = it.Next()
				if idx >= minID && idx-minID < span {
					if len(active) == 0 || orderBasicResidualMatchesReader(preds, active, idx) {
						if skip > 0 {
							skip--
						} else {
							out = append(out, idx)
						}
						if len(out) >= need {
							it.Release()
							return out, examined, true, false
						}
					}
				}
			}
			it.Release()
			if guard.enabled && guard.shouldFallback(examined, len(out)) {
				return nil, examined, true, true
			}
		}
		return out, examined, true, false
	}

	var bits [64]uint64
	it = base.Iter()
	for it.HasNext() {
		off := it.Next() - minID
		bits[off>>6] |= uint64(1) << (off & 63)
	}
	it.Release()

	keyCur := ov.NewCursor(br, desc)
	examined := uint64(0)
	for len(out) < need {
		ids, idx, single, ok := keyCur.NextPostingOrSingle()
		if !ok {
			break
		}
		if single {
			examined++
			if idx >= minID {
				off := idx - minID
				if off < span && bits[off>>6]&(uint64(1)<<(off&63)) != 0 {
					if len(active) == 0 || orderBasicResidualMatchesReader(preds, active, idx) {
						if skip > 0 {
							skip--
						} else {
							out = append(out, idx)
							if len(out) >= need {
								return out, examined, true, false
							}
						}
					}
				}
			}
			if guard.enabled && guard.shouldFallback(examined, len(out)) {
				return nil, examined, true, true
			}
			continue
		}
		it = ids.Iter()
		for it.HasNext() {
			examined++
			idx = it.Next()
			if idx >= minID {
				off := idx - minID
				if off < span && bits[off>>6]&(uint64(1)<<(off&63)) != 0 {
					if len(active) == 0 || orderBasicResidualMatchesReader(preds, active, idx) {
						if skip > 0 {
							skip--
						} else {
							out = append(out, idx)
						}
						if len(out) >= need {
							it.Release()
							return out, examined, true, false
						}
					}
				}
			}
		}
		it.Release()
		if guard.enabled && guard.shouldFallback(examined, len(out)) {
			return nil, examined, true, true
		}
	}
	return out, examined, true, false
}

func scanOrderLimitSmallBaseNoTrace(q *qir.Shape, ov indexdata.FieldIndexView, br indexdata.FieldIndexRange, desc bool, base posting.List, baseCard int, preds predicateReader, active []int, guard plannerOrderedLimitRuntimeGuard) ([]uint64, uint64, bool) {
	if q.Offset >= uint64(baseCard) {
		return nil, 0, false
	}
	size := 1
	for size < baseCard*2 {
		size <<= 1
	}
	var keys [256]uint64
	var used [256]bool
	mask := uint64(size - 1)

	it := base.Iter()
	for it.HasNext() {
		idx := it.Next()
		slot := (idx * 11400714819323198485) & mask
		for used[slot] {
			slot = (slot + 1) & mask
		}
		used[slot] = true
		keys[slot] = idx
	}
	it.Release()

	limit := clampUint64ToInt(q.Limit)
	skip := int(q.Offset)
	need := limit
	if remaining := baseCard - skip; need > remaining {
		need = remaining
	}
	out := make([]uint64, 0, need)
	keyCur := ov.NewCursor(br, desc)
	examined := uint64(0)
	for len(out) < need {
		ids, idx, single, ok := keyCur.NextPostingOrSingle()
		if !ok {
			break
		}
		if single {
			examined++
			slot := (idx * 11400714819323198485) & mask
			for used[slot] {
				if keys[slot] == idx {
					if len(active) == 0 || orderBasicResidualMatchesReader(preds, active, idx) {
						if skip > 0 {
							skip--
						} else {
							out = append(out, idx)
							if len(out) >= need {
								return out, examined, false
							}
						}
					}
					break
				}
				slot = (slot + 1) & mask
			}
			if guard.enabled && guard.shouldFallback(examined, len(out)) {
				return nil, examined, true
			}
			continue
		}
		it := ids.Iter()
		for it.HasNext() {
			examined++
			idx = it.Next()
			slot := (idx * 11400714819323198485) & mask
			for used[slot] {
				if keys[slot] == idx {
					if len(active) == 0 || orderBasicResidualMatchesReader(preds, active, idx) {
						if skip > 0 {
							skip--
						} else {
							out = append(out, idx)
						}
					}
					break
				}
				slot = (slot + 1) & mask
			}
			if len(out) >= need {
				it.Release()
				return out, examined, false
			}
		}
		it.Release()
		if guard.enabled && guard.shouldFallback(examined, len(out)) {
			return nil, examined, true
		}
	}
	return out, examined, false
}

func scanOrderLimitPooledBaseNoTrace(q *qir.Shape, ov indexdata.FieldIndexView, br indexdata.FieldIndexRange, desc bool, base posting.List, baseCard int, preds predicateReader, active []int, guard plannerOrderedLimitRuntimeGuard) ([]uint64, uint64, bool) {
	size := 1
	for size < baseCard*2 {
		size <<= 1
	}
	keys := pooled.GetUint64Slice(size)[:size]
	used := pooled.GetBoolSlice(size)[:size]
	clear(used)
	out, examined, fallback := scanOrderLimitBaseHashNoTrace(q, ov, br, desc, base, baseCard, keys, used, preds, active, guard)
	pooled.ReleaseBoolSlice(used)
	pooled.ReleaseUint64Slice(keys)
	return out, examined, fallback
}

func scanOrderLimitBaseHashNoTrace(q *qir.Shape, ov indexdata.FieldIndexView, br indexdata.FieldIndexRange, desc bool, base posting.List, baseCard int, keys []uint64, used []bool, preds predicateReader, active []int, guard plannerOrderedLimitRuntimeGuard) ([]uint64, uint64, bool) {
	if q.Offset >= uint64(baseCard) {
		return nil, 0, false
	}
	mask := uint64(len(keys) - 1)

	it := base.Iter()
	for it.HasNext() {
		idx := it.Next()
		slot := (idx * 11400714819323198485) & mask
		for used[slot] {
			slot = (slot + 1) & mask
		}
		used[slot] = true
		keys[slot] = idx
	}
	it.Release()

	limit := clampUint64ToInt(q.Limit)
	skip := int(q.Offset)
	need := limit
	if remaining := baseCard - skip; need > remaining {
		need = remaining
	}
	out := make([]uint64, 0, need)
	keyCur := ov.NewCursor(br, desc)
	examined := uint64(0)
	for len(out) < need {
		ids, idx, single, ok := keyCur.NextPostingOrSingle()
		if !ok {
			break
		}
		if single {
			examined++
			slot := (idx * 11400714819323198485) & mask
			for used[slot] {
				if keys[slot] == idx {
					if len(active) == 0 || orderBasicResidualMatchesReader(preds, active, idx) {
						if skip > 0 {
							skip--
						} else {
							out = append(out, idx)
							if len(out) >= need {
								return out, examined, false
							}
						}
					}
					break
				}
				slot = (slot + 1) & mask
			}
			if guard.enabled && guard.shouldFallback(examined, len(out)) {
				return nil, examined, true
			}
			continue
		}
		it := ids.Iter()
		for it.HasNext() {
			examined++
			idx = it.Next()
			slot := (idx * 11400714819323198485) & mask
			for used[slot] {
				if keys[slot] == idx {
					if len(active) == 0 || orderBasicResidualMatchesReader(preds, active, idx) {
						if skip > 0 {
							skip--
						} else {
							out = append(out, idx)
						}
					}
					break
				}
				slot = (slot + 1) & mask
			}
			if len(out) >= need {
				it.Release()
				return out, examined, false
			}
		}
		it.Release()
		if guard.enabled && guard.shouldFallback(examined, len(out)) {
			return nil, examined, true
		}
	}
	return out, examined, false
}

func (qv *View) execSelectedNoOrderDirectRange(q *qir.Shape, trace *Trace) ([]uint64, bool, error) {
	e := q.Expr
	if e.Op == qir.OpAND || e.Op == qir.OpOR || len(e.Operands) != 0 {
		return nil, false, nil
	}

	if !isScalarRangeEqOp(e.Op) {
		return nil, false, nil
	}

	if e.FieldOrdinal < 0 {
		return nil, false, nil
	}

	fm := qv.fieldMetaByExpr(e)
	if fm == nil || fm.Slice {
		return nil, false, nil
	}

	isNil := false
	rb := indexdata.Bounds{Has: true}
	if e.Op == qir.OpEQ {
		key, isSlice, eqNil, err := qv.exprValueToLookupKey(e)
		if err != nil {
			return nil, true, err
		}
		if isSlice {
			return nil, false, nil
		}
		isNil = eqNil
		if isNil {
			ids := qv.fieldIndexViewFromSlotsForExpr(qv.snap.NilIndex, e).LookupPostingRetained(indexdata.NilIndexEntryKey)
			if ids.IsEmpty() {
				return nil, true, nil
			}
			capHint, exhausted := boundedWindowCap(ids.Cardinality(), q.Offset, q.Limit)
			if exhausted {
				ids.Release()
				if trace != nil {
					trace.AddExamined(0)
					trace.SetEarlyStopReason("input_exhausted")
				}
				return nil, true, nil
			}
			skip := q.Offset
			need := q.Limit
			out := make([]uint64, 0, clampUint64ToInt(capHint))
			cursor := newQueryCursor(out, skip, need, false, 0)
			var (
				examined    uint64
				examinedPtr *uint64
			)
			if trace != nil {
				examinedPtr = &examined
			}
			if emitAcceptedPostingNoOrder(&cursor, ids, examinedPtr) {
				if trace != nil {
					trace.AddExamined(examined)
					trace.SetEarlyStopReason("limit_reached")
				}
				return cursor.out, true, nil
			}
			if trace != nil {
				trace.AddExamined(examined)
				trace.SetEarlyStopReason("input_exhausted")
			}
			return cursor.out, true, nil
		}

		if key.IsNumeric() {
			idxKey := key.IndexKey()
			rb.ApplyLoIndex(idxKey, true)
			rb.ApplyHiIndex(idxKey, true)
		} else {
			s := key.StringKey()
			rb.ApplyLo(s, true)
			rb.ApplyHi(s, true)
		}

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

	ov := qv.fieldIndexViewFromSlotsForExpr(qv.snap.Index, e)
	if !ov.HasData() {
		return nil, true, nil
	}

	br := ov.RangeForBounds(rb)
	if br.Empty() {
		return nil, true, nil
	}

	skip := q.Offset
	need := q.Limit
	capHint, exhausted := fieldIndexRangeWindowCap(ov, br, skip, need, 0)
	if exhausted {
		if trace != nil {
			trace.AddExamined(0)
			trace.SetEarlyStopReason("input_exhausted")
		}
		return nil, true, nil
	}

	out := make([]uint64, 0, clampUint64ToInt(capHint))
	cursor := newQueryCursor(out, skip, need, false, 0)

	keyCur := ov.NewCursor(br, false)
	if trace == nil && skip == 0 {
		limit := clampUint64ToInt(need)
		for {
			ids, idx, single, ok := keyCur.NextPostingOrSingle()
			if !ok {
				break
			}
			if single {
				out = append(out, idx)
				if len(out) >= limit {
					return out, true, nil
				}
				continue
			}
			out, ok = appendPostingLimitNoTrace(out, ids, limit)
			if ok {
				return out, true, nil
			}
		}
		return out, true, nil
	}

	var (
		examined    uint64
		examinedPtr *uint64
	)
	if trace != nil {
		examinedPtr = &examined
	}
	for {
		_, ids, ok := keyCur.Next()
		if !ok {
			break
		}
		if ids.IsEmpty() {
			continue
		}
		if emitAcceptedPostingNoOrder(&cursor, ids, examinedPtr) {
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

func (qv *View) execSelectedNoOrderDirectPrefix(q *qir.Shape, trace *Trace) ([]uint64, bool, error) {
	e := q.Expr
	if !isPositiveScalarPrefixLeaf(e) {
		return nil, false, nil
	}

	fm := qv.fieldMetaByExpr(e)
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
		if !qv.hasIndexedFieldForExpr(e) {
			return nil, true, fmt.Errorf("no index for field: %v", qv.exec.FieldNameByOrdinal(e.FieldOrdinal))
		}
		return nil, true, nil
	}
	if prefixState.br.Empty() {
		return nil, true, nil
	}

	skip := q.Offset
	need := q.Limit
	capHint, exhausted := fieldIndexRangeWindowCap(prefixState.ov, prefixState.br, skip, need, 0)
	if exhausted {
		if trace != nil {
			trace.AddExamined(0)
			trace.SetEarlyStopReason("input_exhausted")
		}
		return nil, true, nil
	}
	out := make([]uint64, 0, clampUint64ToInt(capHint))
	cursor := newQueryCursor(out, skip, need, false, 0)

	keyCur := prefixState.ov.NewCursor(prefixState.br, false)
	if trace == nil && skip == 0 {
		limit := clampUint64ToInt(need)
		for {
			ids, idx, single, ok := keyCur.NextPostingOrSingle()
			if !ok {
				break
			}
			if single {
				out = append(out, idx)
				if len(out) >= limit {
					return out, true, nil
				}
				continue
			}
			out, ok = appendPostingLimitNoTrace(out, ids, limit)
			if ok {
				return out, true, nil
			}
		}
		return out, true, nil
	}

	var (
		examined    uint64
		examinedPtr *uint64
	)
	if trace != nil {
		examinedPtr = &examined
	}
	for {
		_, ids, ok := keyCur.Next()
		if !ok {
			break
		}
		if ids.IsEmpty() {
			continue
		}
		if emitAcceptedPostingNoOrder(&cursor, ids, examinedPtr) {
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
