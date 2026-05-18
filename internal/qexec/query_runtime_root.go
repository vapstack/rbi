package qexec

import (
	"fmt"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qir"
)

func (qv *View) TryQueryEmptyOnSnapshot(q *qir.Shape) (bool, error) {
	if q == nil || q.Offset != 0 || q.Limit != 0 || q.HasOrder {
		return false, nil
	}

	e := q.Expr
	if e.Not || e.FieldOrdinal < 0 || len(e.Operands) != 0 {
		return false, nil
	}

	switch e.Op {
	case qir.OpEQ:
		fm := qv.fieldMetaByExpr(e)
		if fm == nil || fm.Slice {
			return false, nil
		}
		key, isSlice, isNil, err := qv.exprValueToLookupKey(e)
		if err != nil {
			return false, err
		}
		if isSlice {
			return false, nil
		}
		if isNil {
			return qv.fieldIndexViewFromSlotsForExpr(qv.snap.NilIndex, e).LookupCardinality(indexdata.NilIndexEntryKey) == 0, nil
		}
		return lookupScalarCardinality(qv.fieldIndexViewFromSlotsForExpr(qv.snap.Index, e), key) == 0, nil

	case qir.OpGT, qir.OpGTE, qir.OpLT, qir.OpLTE, qir.OpPREFIX:
		fm := qv.fieldMetaByExpr(e)
		if fm == nil || fm.Slice {
			return false, nil
		}
		bound, isSlice, err := qv.exprValueToNormalizedScalarBound(e)
		if err != nil {
			return false, err
		}
		if isSlice {
			return false, nil
		}
		var bounds indexdata.Bounds
		bounds.Has = true
		applyNormalizedScalarBound(&bounds, bound)
		if bounds.Empty {
			return true, nil
		}
		ov := qv.fieldIndexViewFromSlotsForExpr(qv.snap.Index, e)
		if !ov.HasData() {
			return true, nil
		}
		return ov.RangeForBounds(bounds).Empty(), nil
	}

	return false, nil
}

func shouldSkipPlannerForArrayOrderShape(q *qir.Shape) bool {
	if q == nil || !q.HasOrder {
		return false
	}
	switch q.Order.Kind {
	case qir.OrderKindArrayPos, qir.OrderKindArrayCount:
		return true
	default:
		return false
	}
}

func finishQueryTrace(trace *Trace, out *[]uint64, err *error) {
	if trace == nil {
		return
	}
	trace.Finish(uint64(len(*out)), *err)
}

// broad negative full scans are faster when we materialize the complement once
// instead of probing exclusion membership for every universe id.
func shouldMaterializeNegativeAllNumericKeys(universeCard, excludedCard uint64) bool {
	if universeCard == 0 || excludedCard >= universeCard {
		return false
	}
	resultCard := universeCard - excludedCard
	if resultCard < 64_000 {
		return false
	}
	return resultCard >= excludedCard*2
}

func (qv *View) materializeNegativeResultKeys() []uint64 {
	universe := qv.snap.Universe.Borrow()
	return universe.ToArray()
}

func (qv *View) materializeNegativeResultKeysExcluding(exclude posting.List) []uint64 {
	if exclude.IsEmpty() {
		return qv.materializeNegativeResultKeys()
	}
	ids := qv.snap.Universe.Borrow().BuildAndNot(exclude)
	if ids.IsEmpty() {
		return nil
	}
	out := ids.ToArray()
	ids.Release()
	return out
}

func (qv *View) Filter(q *qir.Query) (posting.List, error) {
	if q == nil {
		return qv.snap.Universe.Borrow(), nil
	}
	shape := qir.NewShape(q)
	res, err := qv.evalExpr(shape.Expr)
	if err != nil {
		return posting.List{}, err
	}
	if res.neg {
		ids := qv.snap.Universe.Borrow()
		if !res.ids.IsEmpty() {
			ids = ids.BuildAndNot(res.ids)
		}
		res.ids.Release()
		return ids, nil
	}
	ids := res.ids.Clone()
	res.ids.Release()
	return ids, nil
}

func (qv *View) PreparedQuery(q *qir.Shape) ([]uint64, error) {
	return qv.Query(q, false, true)
}

// Query runs the full query pipeline against one snapshot view.
//
// The method keeps all routing decisions in one place so fast-path/planner
// selection remains consistent across QueryKeys and Query callers.
func (qv *View) Query(q *qir.Shape, emitTrace bool, prepared bool) (out []uint64, err error) {
	traceEnabled := emitTrace && qv.exec.TraceSamplingEnabled()

	if !prepared && !traceEnabled {
		if out, ok, fastErr := qv.tryDirectSingleUniqueEqNoOrder(q, nil); ok {
			return out, fastErr
		}
		if out, ok, fastErr := qv.tryNoFilterNoOrderWithLimit(q, nil); ok {
			return out, fastErr
		}
		if out, ok, fastErr := qv.tryOrderBasicNoFilterWithLimit(q, nil); ok {
			return out, fastErr
		}
		if out, ok, fastErr := qv.tryQueryPrefixNoOrderWithLimit(q, nil); ok {
			return out, fastErr
		}
		if out, ok, fastErr := qv.tryQueryRangeNoOrderWithLimit(q, nil); ok {
			return out, fastErr
		}
	}

	var trace *Trace
	if traceEnabled {
		trace = qv.BeginTrace(*q)
		if trace != nil {
			defer finishQueryTrace(trace, &out, &err)
		}
	}

	var ok bool

	if out, ok, err = qv.tryUniqueEqNoOrder(q, trace); ok {
		return out, err
	}

	if out, ok, err = qv.tryNoFilterNoOrderWithLimit(q, trace); ok {
		if trace != nil {
			trace.SetPlan(PlanLimit)
		}
		return out, err
	}

	if out, ok, err = qv.tryOrderBasicNoFilterWithLimit(q, trace); ok {
		if trace != nil {
			trace.SetPlan(PlanLimitOrderBasic)
		}
		return out, err
	}

	if out, ok, err = qv.tryQueryOrderArrayPosSingleHasAny(q, trace); ok {
		if trace != nil {
			trace.SetPlan(PlanMaterialized)
		}
		return out, err
	}

	// Planner/execution fast-paths are attempted before postingResult fallback because
	// they can short-circuit large scans when query shape matches known patterns.
	if !shouldSkipPlannerForArrayOrderShape(q) {
		if qv.shouldPreferExecutionPlan(q, trace) {
			if out, ok, err = qv.tryExecutionPlan(q, trace); ok {
				return out, err
			}
			if out, ok, err = qv.tryPlan(q, trace); ok {
				return out, err
			}
		} else {
			if out, ok, err = qv.tryPlan(q, trace); ok {
				return out, err
			}
			if out, ok, err = qv.tryExecutionPlan(q, trace); ok {
				return out, err
			}
		}
	}

	if trace != nil {
		trace.SetPlan(PlanMaterialized)
	}

	result, err := qv.evalExpr(q.Expr)
	if err != nil {
		return nil, err
	}
	if !result.neg {
		if result.ids.IsEmpty() {
			result.ids.Release()
			return nil, nil
		}
	} else {
		if qv.snap.Universe.Cardinality() == 0 {
			result.ids.Release()
			return nil, nil
		}
	}
	defer result.ids.Release()

	skip := q.Offset
	needAll := q.Limit == 0
	need := q.Limit

	// case 1: no ordering, negative result: iterate over universe excluding the result set
	if !q.HasOrder && result.neg {
		if !qv.strKey && needAll && skip == 0 &&
			shouldMaterializeNegativeAllNumericKeys(qv.snap.Universe.Cardinality(), result.ids.Cardinality()) {
			ids := qv.materializeNegativeResultKeysExcluding(result.ids)
			if len(ids) == 0 {
				return nil, nil
			}
			return ids, nil
		}
		out = makeOutSlice(qv.postingResultCardinality(result), need)
		cursor := newQueryCursor(out, skip, need, needAll, 0)

		ex := result.ids
		universe := qv.snap.Universe.Borrow()
		it := universe.Iter()
		defer it.Release()
		for it.HasNext() {
			idx := it.Next()
			if ex.Contains(idx) {
				continue
			}
			if cursor.emit(idx) {
				return cursor.out, nil
			}
		}
		return cursor.out, nil
	}

	// case 2: ordering
	if q.HasOrder {
		order := q.Order
		orderField := qv.exec.FieldNameByOrdinal(order.FieldOrdinal)

		switch order.Kind {

		case qir.OrderKindArrayPos:
			ov := qv.fieldIndexViewFromSlotsForOrder(qv.snap.Index, order)
			if !ov.HasData() && !qv.hasIndexedFieldForOrder(order) {
				return nil, fmt.Errorf("cannot sort non-indexed field: %v", orderField)
			}
			return qv.queryOrderArrayPosIndexView(result, ov, order, skip, need, needAll)

		case qir.OrderKindArrayCount:
			lenOV := qv.fieldIndexViewFromSlotsForOrder(qv.snap.LenIndex, order)
			useZeroComplement := qv.isLenZeroComplementOrdinal(order.FieldOrdinal)
			if !lenOV.HasData() && !useZeroComplement {
				return nil, fmt.Errorf("no lenIndex for slice field: %v", orderField)
			}
			return qv.queryOrderArrayCount(result, lenOV, order, skip, need, needAll, useZeroComplement)
		}

		ov := qv.fieldIndexViewFromSlotsForOrder(qv.snap.Index, order)
		if !ov.HasData() && !qv.hasIndexedFieldForOrder(order) {
			return nil, fmt.Errorf("cannot sort non-indexed field: %v", orderField)
		}
		return qv.queryOrderBasic(result, ov, order, skip, need, needAll)
	}

	// Fast-path only when unbounded query also has no offset.
	// Offset requires cursor-based pagination logic.
	if !qv.strKey && needAll && skip == 0 {
		ids := result.ids.ToArray()
		if len(ids) == 0 {
			return nil, nil
		}
		return ids, nil
	}

	out = makeOutSlice(result.ids.Cardinality(), need)
	cursor := newQueryCursor(out, skip, need, needAll, 0)

	iter := result.ids.Iter()
	defer iter.Release()
	for iter.HasNext() {
		if cursor.emit(iter.Next()) {
			return cursor.out, nil
		}
	}
	return cursor.out, nil
}
