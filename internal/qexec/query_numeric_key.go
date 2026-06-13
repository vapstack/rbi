package qexec

import (
	"fmt"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/rbierrors"
)

func (qv *View) isNumericKeyOrdinal(ordinal int) bool {
	field, ok := qv.fieldByOrdinal(ordinal)
	return ok && field.kind == queryFieldNumericKey
}

func numericKeyRangeLimits(bounds indexdata.Bounds) (uint64, uint64, bool, bool) {
	bounds.Normalize()
	if bounds.Empty || bounds.HasPrefix {
		return 0, 0, false, true
	}
	lo := uint64(0)
	if bounds.HasLo {
		if !bounds.LoNumeric {
			return 0, 0, false, true
		}
		lo = bounds.LoIndex.U64()
		if !bounds.LoInc {
			if lo == ^uint64(0) {
				return 0, 0, false, true
			}
			lo++
		}
	}
	hi := uint64(0)
	hasHi := false
	if bounds.HasHi {
		if !bounds.HiNumeric {
			return 0, 0, false, true
		}
		hi = bounds.HiIndex.U64()
		if !bounds.HiInc {
			if hi == 0 {
				return 0, 0, false, true
			}
			hi--
		}
		hasHi = true
		if lo > hi {
			return 0, 0, false, true
		}
	}
	return lo, hi, hasHi, false
}

func (qv *View) numericKeyPostingForID(id uint64) posting.List {
	if !qv.snap.Universe.Contains(id) {
		return posting.List{}
	}
	return (posting.List{}).BuildAdded(id)
}

func (qv *View) numericKeyPostingForLookupKeys(keys []keycodec.IndexLookupKey) posting.List {
	var ids posting.List
	for i := range keys {
		if !keys[i].IsNumeric() {
			continue
		}
		id := keys[i].U64()
		if qv.snap.Universe.Contains(id) {
			ids = ids.BuildAdded(id)
		}
	}
	return ids
}

func (qv *View) numericKeyRangePosting(bounds indexdata.Bounds) posting.List {
	lo, hi, hasHi, empty := numericKeyRangeLimits(bounds)
	if empty {
		return posting.List{}
	}
	it := qv.snap.Universe.AdvancingIter()
	it.AdvanceIfNeeded(lo)
	var ids posting.List
	for it.HasNext() {
		id := it.Next()
		if hasHi && id > hi {
			break
		}
		ids = ids.BuildAdded(id)
	}
	it.Release()
	return ids
}

func (qv *View) evalNumericKeySimple(e qir.Expr) (postingResult, error) {
	switch e.Op {

	case qir.OpEQ:
		key, isSlice, _, err := qv.exprValueToLookupKey(e)
		if err != nil {
			return postingResult{}, err
		}
		if isSlice {
			return postingResult{}, fmt.Errorf("%w: %v expects a single value for scalar field %v", rbierrors.ErrInvalidQuery, e.Op, qv.exec.FieldNameByOrdinal(e.FieldOrdinal))
		}
		if !key.IsNumeric() {
			return postingResult{}, nil
		}
		return postingResult{ids: qv.numericKeyPostingForID(key.U64())}, nil

	case qir.OpIN:
		keys, isSlice, _, err := qv.exprValueToDistinctLookupKeyBuf(e)
		if err != nil {
			return postingResult{}, err
		}
		if keys != nil {
			defer keycodec.ReleaseIndexLookupKeySlice(keys)
		}
		if !isSlice && e.Value != nil {
			return postingResult{}, fmt.Errorf("%w: %v expects a slice", rbierrors.ErrInvalidQuery, e.Op)
		}
		if len(keys) == 0 {
			return postingResult{}, nil
		}
		return postingResult{ids: qv.numericKeyPostingForLookupKeys(keys)}, nil

	case qir.OpGT, qir.OpGTE, qir.OpLT, qir.OpLTE:
		bounds, ok, err := qv.rangeBoundsForScalarExpr(e)
		if err != nil || !ok {
			return postingResult{}, err
		}
		return postingResult{ids: qv.numericKeyRangePosting(bounds)}, nil

	default:
		return postingResult{}, fmt.Errorf("%w: %v is not supported for %v", rbierrors.ErrInvalidQuery, e.Op, qv.exec.FieldNameByOrdinal(e.FieldOrdinal))
	}
}

func (qv *View) numericKeyBoundsAndResiduals(expr qir.Expr, orderOrdinal int, residuals []qir.Expr) (indexdata.Bounds, []qir.Expr, bool, error) {
	bounds := indexdata.Bounds{Has: true}
	residuals = residuals[:0]

	if expr.Op == qir.OpConst {
		if expr.Not {
			bounds.SetEmpty()
		}
		return bounds, residuals, true, nil
	}

	if expr.Op != qir.OpAND {
		if !expr.Not && len(expr.Operands) == 0 && expr.FieldOrdinal == orderOrdinal && isScalarRangeEqOp(expr.Op) {
			next, ok, err := qv.rangeBoundsForScalarExpr(expr)
			if err != nil || !ok {
				return bounds, residuals, false, err
			}
			mergeRangeBounds(&bounds, next)
			return bounds, residuals, true, nil
		}
		residuals = append(residuals, expr)
		return bounds, residuals, true, nil
	}

	for i := range expr.Operands {
		op := expr.Operands[i]
		if !op.Not && len(op.Operands) == 0 && op.FieldOrdinal == orderOrdinal && isScalarRangeEqOp(op.Op) {
			next, ok, err := qv.rangeBoundsForScalarExpr(op)
			if err != nil || !ok {
				return bounds, residuals, false, err
			}
			mergeRangeBounds(&bounds, next)
			continue
		}
		residuals = append(residuals, op)
	}

	return bounds, residuals, true, nil
}

func (qv *View) scanNumericKeyRange(q *qir.Shape, bounds indexdata.Bounds, desc bool, preds []leafPred, trace *Trace) ([]uint64, bool) {
	lo, hi, hasHi, empty := numericKeyRangeLimits(bounds)
	if empty {
		return nil, true
	}
	out := makeOutSlice(qv.snap.Universe.Cardinality(), q.Limit)
	cursor := newQueryCursor(out, q.Offset, q.Limit, q.Limit == 0, 0)
	var examined uint64
	var activeBuf [limitQueryFastPathMaxLeaves]int
	active := activeBuf[:0]
	for i := 0; i < len(preds); i++ {
		active = append(active, i)
	}

	if desc {
		it := qv.snap.Universe.DescAdvancingIter()
		if hasHi {
			it.AdvanceIfNeeded(hi)
		}
		for it.HasNext() {
			id := it.Next()
			if id < lo {
				break
			}
			if leafPredsEmitCandidate(&cursor, preds, active, trace, id, &examined) {
				it.Release()
				if trace != nil {
					trace.AddExamined(examined)
					trace.SetEarlyStopReason("limit_reached")
				}
				return cursor.out, true
			}
		}
		it.Release()

	} else {
		it := qv.snap.Universe.AdvancingIter()
		it.AdvanceIfNeeded(lo)
		for it.HasNext() {
			id := it.Next()
			if hasHi && id > hi {
				break
			}
			if leafPredsEmitCandidate(&cursor, preds, active, trace, id, &examined) {
				it.Release()
				if trace != nil {
					trace.AddExamined(examined)
					trace.SetEarlyStopReason("limit_reached")
				}
				return cursor.out, true
			}
		}
		it.Release()
	}

	if trace != nil {
		trace.AddExamined(examined)
		trace.SetEarlyStopReason("input_exhausted")
	}

	return cursor.out, true
}

func (qv *View) dispatchNumericKeyOrderedLimit(q *qir.Shape, facts *orderedLimitFacts, trace *Trace) ([]uint64, bool, error) {
	if len(facts.baseOps) == 0 {
		out, _ := qv.scanNumericKeyRange(q, facts.bounds, q.Order.Desc, nil, trace)
		return out, true, nil
	}

	predsBuf := leafPredSlicePool.Get(len(facts.baseOps))

	for i := range facts.baseOps {
		p, ok, err := qv.buildLimitLeafPred(facts.baseOps[i], 0)
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
		predsBuf = append(predsBuf, p)
	}

	out, _ := qv.scanNumericKeyRange(q, facts.bounds, q.Order.Desc, predsBuf, trace)
	leafPredSlicePool.Put(predsBuf)

	return out, true, nil
}

func (qv *View) queryOrderNumericKey(result postingResult, desc bool, skip, need uint64, needAll bool) ([]uint64, error) {
	out := makeOutSlice(qv.postingResultCardinality(result), need)
	cursor := newQueryCursor(out, skip, need, needAll, 0)

	if result.neg {
		var ex posting.ContainsCursor
		ex.Reset(result.ids)
		if desc {
			it := qv.snap.Universe.DescIter()
			for it.HasNext() {
				id := it.Next()
				if !ex.Contains(id) && cursor.emit(id) {
					it.Release()
					return cursor.out, nil
				}
			}
			it.Release()
			return cursor.out, nil
		}
		it := qv.snap.Universe.Iter()
		for it.HasNext() {
			id := it.Next()
			if !ex.Contains(id) && cursor.emit(id) {
				it.Release()
				return cursor.out, nil
			}
		}
		it.Release()
		return cursor.out, nil
	}

	if desc {
		it := result.ids.DescIter()
		for it.HasNext() {
			if cursor.emit(it.Next()) {
				it.Release()
				return cursor.out, nil
			}
		}
		it.Release()
		return cursor.out, nil
	}

	it := result.ids.Iter()
	for it.HasNext() {
		if cursor.emit(it.Next()) {
			it.Release()
			return cursor.out, nil
		}
	}
	it.Release()

	return cursor.out, nil
}
