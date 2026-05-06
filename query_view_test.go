package rbi

import (
	"fmt"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/pools"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qir"
)

func (qe *queryEngine) currentQueryViewForTests() *queryView {
	if qe == nil || qe.snapshot == nil {
		return &queryView{snap: &indexSnapshot{}}
	}
	if snap := qe.snapshot.current.Load(); snap != nil {
		return &queryView{
			engine:            qe,
			snap:              snap,
			strKey:            snap.strmap != nil,
			strMapView:        snap.strmap,
			fields:            qe.fields,
			planner:           qe.planner,
			lenZeroComplement: snap.lenZeroComplement,
		}
	}

	snap := &indexSnapshot{
		index:             qe.index,
		nilIndex:          qe.nilIndex,
		lenIndex:          qe.lenIndex,
		lenZeroComplement: qe.lenZeroComplement,
		universe:          qe.universe,
	}
	return &queryView{
		engine:            qe,
		snap:              snap,
		strKey:            snap.strmap != nil,
		strMapView:        snap.strmap,
		fields:            qe.fields,
		planner:           qe.planner,
		lenZeroComplement: snap.lenZeroComplement,
	}
}

func TestQueryView_NativeTimeNormalizedBoundCache_WorksAndStaysTypeSensitive(t *testing.T) {
	db := openTempDBUint64Reflect[reflectNamedTimeRec](t, "query_view_time_cache.db")

	when := time.Unix(1_700_001_000, 900_000_000).UTC()
	if err := db.Set(1, &reflectNamedTimeRec{When: reflectNamedTime(when)}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	view := db.engine.currentQueryViewForTests()
	timeExpr := mustTestQIRExprForDB(t, db, qx.GTE("when", when))
	bound, isSlice, err := view.exprValueToNormalizedScalarBound(timeExpr)
	if err != nil || isSlice {
		t.Fatalf("exprValueToNormalizedScalarBound(time): bound=%+v err=%v isSlice=%v", bound, err, isSlice)
	}
	if view.normalizedScalarBoundCacheLen != 1 {
		t.Fatalf("expected one cached native-time bound, got=%d", view.normalizedScalarBoundCacheLen)
	}
	if view.normalizedScalarBoundCache[0].kind != normalizedScalarBoundCacheUnixTime {
		t.Fatalf("expected unix-time cache kind, got=%v", view.normalizedScalarBoundCache[0].kind)
	}

	bound2, isSlice, err := view.exprValueToNormalizedScalarBound(timeExpr)
	if err != nil || isSlice {
		t.Fatalf("exprValueToNormalizedScalarBound(time, second): bound=%+v err=%v isSlice=%v", bound2, err, isSlice)
	}
	if bound2 != bound {
		t.Fatalf("cached native-time bound mismatch: first=%+v second=%+v", bound, bound2)
	}
	if view.normalizedScalarBoundCacheLen != 1 {
		t.Fatalf("expected cache len to stay 1 after hit, got=%d", view.normalizedScalarBoundCacheLen)
	}

	intExpr := mustTestQIRExprForDB(t, db, qx.GTE("when", int64(when.Unix())))
	bound3, isSlice, err := view.exprValueToNormalizedScalarBound(intExpr)
	if err != nil || isSlice {
		t.Fatalf("exprValueToNormalizedScalarBound(int): bound=%+v err=%v isSlice=%v", bound3, err, isSlice)
	}
	if bound3 != bound {
		t.Fatalf("time and int operands must normalize to the same bound on native-time field: time=%+v int=%+v", bound, bound3)
	}
	if view.normalizedScalarBoundCacheLen != 2 {
		t.Fatalf("expected distinct cache entries for time and int operands, got=%d", view.normalizedScalarBoundCacheLen)
	}
}

func testExprFieldName(qe *queryEngine, expr qir.Expr) string {
	if qe == nil {
		return ""
	}
	return qe.fieldNameByOrdinal(expr.FieldOrdinal)
}

func testOrderFieldName(qe *queryEngine, order qir.Order) string {
	if qe == nil {
		return ""
	}
	return qe.fieldNameByOrdinal(order.FieldOrdinal)
}

func prepareTestQuery(qe *queryEngine, q *qx.QX) (*qir.Query, qir.Shape, error) {
	var (
		prepared *qir.Query
		err      error
	)
	if qe == nil {
		prepared, err = qir.PrepareQueryNoResolve(q)
	} else {
		prepared, err = qir.PrepareQuery(q, qe.indexedFieldMap)
	}
	if err != nil {
		return nil, qir.Shape{}, err
	}
	return prepared, qir.NewShape(prepared), nil
}

func prepareTestExpr(qe *queryEngine, expr qx.Expr) (*qir.Query, qir.Expr, error) {
	var (
		prepared *qir.Query
		err      error
	)
	if qe == nil {
		prepared, err = qir.PrepareCountExprsNoResolve(expr)
	} else {
		prepared, err = qir.PrepareCountExprsResolved(qe.indexedFieldMap, expr)
	}
	if err != nil {
		return nil, qir.Expr{}, err
	}
	return prepared, prepared.Expr, nil
}

func prepareTestExprs(qe *queryEngine, exprs []qx.Expr) ([]*qir.Query, []qir.Expr, error) {
	prepared := make([]*qir.Query, 0, len(exprs))
	out := make([]qir.Expr, 0, len(exprs))
	for i := range exprs {
		p, expr, err := prepareTestExpr(qe, exprs[i])
		if err != nil {
			releasePreparedQueriesForTest(prepared)
			return nil, nil, err
		}
		prepared = append(prepared, p)
		out = append(out, detachTestQIRExpr(expr))
	}
	return prepared, out, nil
}

func releasePreparedQueriesForTest(prepared []*qir.Query) {
	for i := range prepared {
		prepared[i].Release()
	}
}

func compileScalarOpForTest(op qx.Op) qir.Op {
	switch op {
	case qx.OpEQ:
		return qir.OpEQ
	case qx.OpGT:
		return qir.OpGT
	case qx.OpGTE:
		return qir.OpGTE
	case qx.OpLT:
		return qir.OpLT
	case qx.OpLTE:
		return qir.OpLTE
	case qx.OpIN:
		return qir.OpIN
	case qx.OpHASANY:
		return qir.OpHASANY
	case qx.OpHASALL:
		return qir.OpHASALL
	case qx.OpPREFIX:
		return qir.OpPREFIX
	case qx.OpSUFFIX:
		return qir.OpSUFFIX
	case qx.OpCONTAINS:
		return qir.OpCONTAINS
	default:
		panic(fmt.Sprintf("unsupported qx op in test helper: %q", op))
	}
}

func (qe *queryEngine) evalExpr(e qx.Expr) (postingResult, error) {
	prepared, expr, err := prepareTestExpr(qe, e)
	if err != nil {
		return postingResult{}, err
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().evalExpr(expr)
}

func (qe *queryEngine) tryExecutionPlan(q *qx.QX, trace *queryTrace) ([]uint64, bool, error) {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil, false, err
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().tryExecutionPlan(&viewQ, trace)
}

func (qe *queryEngine) tryPlan(q *qx.QX, trace *queryTrace) ([]uint64, bool, error) {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil, false, err
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().tryPlan(&viewQ, trace)
}

func (qe *queryEngine) countPreparedExpr(expr qx.Expr) (uint64, error) {
	prepared, compiled, err := prepareTestExpr(qe, expr)
	if err != nil {
		return 0, err
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().countPreparedExpr(compiled)
}

func (qe *queryEngine) tryPlanCandidate(q *qx.QX, trace *queryTrace) ([]uint64, bool, error) {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil, false, err
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().tryPlanCandidate(&viewQ, trace)
}

func (qe *queryEngine) tryLimitQueryOrderBasic(q *qx.QX, leaves []qx.Expr, trace *queryTrace) ([]uint64, bool, error) {
	preparedQ, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil, false, err
	}
	defer preparedQ.Release()

	preparedLeaves, compiledLeaves, err := prepareTestExprs(qe, leaves)
	if err != nil {
		return nil, false, err
	}
	defer releasePreparedQueriesForTest(preparedLeaves)

	return qe.currentQueryViewForTests().tryLimitQueryOrderBasic(&viewQ, compiledLeaves, trace)
}

func (qe *queryEngine) tryLimitQueryRangeNoOrderByField(q *qx.QX, field string, bounds rangeBounds, rest []qx.Expr, trace *queryTrace) ([]uint64, bool, error) {
	preparedQ, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil, false, err
	}
	defer preparedQ.Release()

	preparedRest, compiledRest, err := prepareTestExprs(qe, rest)
	if err != nil {
		return nil, false, err
	}
	defer releasePreparedQueriesForTest(preparedRest)

	return qe.currentQueryViewForTests().tryLimitQueryRangeNoOrderByField(&viewQ, field, bounds, compiledRest, trace)
}

func (qe *queryEngine) tryQueryOrderBasicWithLimit(q *qx.QX, trace *queryTrace) ([]uint64, bool, error) {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil, false, err
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().tryQueryOrderBasicWithLimit(&viewQ, trace)
}

func (qe *queryEngine) tryQueryOrderPrefixWithLimit(q *qx.QX, trace *queryTrace) ([]uint64, bool, error) {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil, false, err
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().tryQueryOrderPrefixWithLimit(&viewQ, trace)
}

func (qe *queryEngine) tryQueryRangeNoOrderWithLimit(q *qx.QX, trace *queryTrace) ([]uint64, bool, error) {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil, false, err
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().tryQueryRangeNoOrderWithLimit(&viewQ, trace)
}

func (qe *queryEngine) tryQueryPrefixNoOrderWithLimit(q *qx.QX, trace *queryTrace) ([]uint64, bool, error) {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil, false, err
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().tryQueryPrefixNoOrderWithLimit(&viewQ, trace)
}

func (qe *queryEngine) buildPredicatesOrderedWithMode(leaves []qx.Expr, orderField string, allowMaterialize bool, orderedWindow int, orderedOffset uint64, coverOrderRange bool, allowOrderedEagerMaterialize bool) ([]predicate, bool) {
	prepared, compiledLeaves, err := prepareTestExprs(qe, leaves)
	if err != nil {
		return nil, false
	}
	defer releasePreparedQueriesForTest(prepared)

	preds, ok := qe.currentQueryViewForTests().buildPredicatesOrderedWithMode(compiledLeaves, orderField, allowMaterialize, orderedWindow, orderedOffset, coverOrderRange, allowOrderedEagerMaterialize)
	return detachPredicateSetForTests(preds), ok
}

func (qe *queryEngine) execPlanOrderedBasic(q *qx.QX, preds []predicate, trace *queryTrace) ([]uint64, bool) {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil, false
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().execPlanOrderedBasicReader(&viewQ, predicateSliceView(preds), trace)
}

func (qe *queryEngine) execPlanOrderedBasicFallback(q *qx.QX, preds []predicate, active []int, start, end int, s []index, trace *queryTrace) []uint64 {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().execPlanOrderedBasicFallback(&viewQ, preds, active, start, end, s, trace)
}

func (qe *queryEngine) buildORBranches(ops []qx.Expr) (plannerORBranches, bool, bool) {
	prepared, compiledOps, err := prepareTestExprs(qe, ops)
	if err != nil {
		return plannerORBranches{}, false, false
	}
	defer releasePreparedQueriesForTest(prepared)
	return qe.currentQueryViewForTests().buildORBranches(compiledOps)
}

func (qe *queryEngine) buildORBranchesOrdered(ops []qx.Expr, orderField string, orderedWindow int) (plannerORBranches, bool, bool) {
	prepared, compiledOps, err := prepareTestExprs(qe, ops)
	if err != nil {
		return plannerORBranches{}, false, false
	}
	defer releasePreparedQueriesForTest(prepared)
	return qe.currentQueryViewForTests().buildORBranchesOrdered(compiledOps, orderField, orderedWindow, 0)
}

func (qe *queryEngine) buildORBranchesOrderedWithOffset(
	ops []qx.Expr,
	orderField string,
	orderedWindow int,
	orderedOffset uint64,
) (plannerORBranches, bool, bool) {
	prepared, compiledOps, err := prepareTestExprs(qe, ops)
	if err != nil {
		return plannerORBranches{}, false, false
	}
	defer releasePreparedQueriesForTest(prepared)
	return qe.currentQueryViewForTests().buildORBranchesOrdered(compiledOps, orderField, orderedWindow, orderedOffset)
}

func (qe *queryEngine) execPlanORNoOrderAdaptive(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]uint64, bool) {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil, false
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().execPlanORNoOrderAdaptive(&viewQ, branches, trace)
}

func (qe *queryEngine) execPlanORNoOrderBaseline(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]uint64, bool) {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil, false
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().execPlanORNoOrderBaseline(&viewQ, branches, trace)
}

func (qe *queryEngine) execPlanOROrderBasic(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]uint64, bool) {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil, false
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().execPlanOROrderBasic(&viewQ, branches, nil, trace, nil)
}

func (qe *queryEngine) execPlanOROrderMergeFallback(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]uint64, bool, error) {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil, false, err
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().execPlanOROrderMergeFallback(&viewQ, branches, trace)
}

func (qe *queryEngine) execPlanOROrderKWay(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]uint64, bool, error) {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return nil, false, err
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().execPlanOROrderKWay(&viewQ, branches, nil, trace)
}

func (qe *queryEngine) materializedPredCacheKey(e qx.Expr) string {
	prepared, expr, err := prepareTestExpr(qe, e)
	if err != nil {
		return ""
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().materializedPredCacheKey(expr)
}

func (qe *queryEngine) buildPredRangeCandidateWithMode(e qx.Expr, fm *field, ov fieldOverlay, allowMaterialize bool) (predicate, bool) {
	prepared, expr, err := prepareTestExpr(qe, e)
	if err != nil {
		return predicate{}, false
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().buildPredRangeCandidateWithMode(expr, fm, ov, allowMaterialize)
}

func (qe *queryEngine) buildPredicateWithMode(e qx.Expr, allowMaterialize bool) (predicate, bool) {
	prepared, expr, err := prepareTestExpr(qe, e)
	if err != nil {
		return predicate{}, false
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().buildPredicateWithMode(expr, allowMaterialize)
}

func (qe *queryEngine) collectOrderRangeBounds(field string, n int, exprAt func(i int) qx.Expr) (rangeBounds, []bool, bool, bool) {
	prepared := make([]*qir.Query, 0, n)
	compiled := make([]qir.Expr, 0, n)
	for i := 0; i < n; i++ {
		p, expr, err := prepareTestExpr(qe, exprAt(i))
		if err != nil {
			releasePreparedQueriesForTest(prepared)
			return rangeBounds{}, nil, false, false
		}
		prepared = append(prepared, p)
		compiled = append(compiled, expr)
	}
	defer releasePreparedQueriesForTest(prepared)

	rb, covered, has, ok := qe.currentQueryViewForTests().collectOrderRangeBounds(field, n, func(i int) qir.Expr {
		return compiled[i]
	})
	return rb, copyBoolBufAndRelease(covered), has, ok
}

func (qe *queryEngine) extractOrderRangeCoverage(field string, preds []predicate, s []index) (int, int, []bool, bool) {
	ov := fieldOverlay{base: s}
	br, covered, ok := qe.currentQueryViewForTests().extractOrderRangeCoverageOverlayReader(field, predicateSliceView(preds), ov)
	if !ok {
		return 0, 0, nil, false
	}
	return br.baseStart, br.baseEnd, copyBoolBufAndRelease(covered), true
}

func (qe *queryEngine) extractOrderRangeCoverageOverlay(field string, preds []predicate, ov fieldOverlay) (overlayRange, []bool, bool) {
	br, covered, ok := qe.currentQueryViewForTests().extractOrderRangeCoverageOverlayReader(field, predicateSliceView(preds), ov)
	return br, copyBoolBufAndRelease(covered), ok
}

func copyBoolBufAndRelease(buf []bool) []bool {
	if buf == nil {
		return nil
	}
	out := append([]bool(nil), buf...)
	pools.PutBoolSlice(buf)
	return out
}

func (qe *queryEngine) decidePlanORNoOrder(q *qx.QX, branches plannerORBranches) plannerORNoOrderDecision {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return plannerORNoOrderDecision{}
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().decidePlanORNoOrder(&viewQ, branches)
}

func (qe *queryEngine) shouldUseOROrderKWayRuntimeFallback(q *qx.QX, branches plannerORBranches, needWindow int) bool {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return false
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().shouldUseOROrderKWayRuntimeFallback(&viewQ, branches, needWindow)
}

func (qe *queryEngine) buildPredicates(leaves []qx.Expr) ([]predicate, bool) {
	prepared, compiledLeaves, err := prepareTestExprs(qe, leaves)
	if err != nil {
		return nil, false
	}
	defer releasePreparedQueriesForTest(prepared)

	preds, ok := qe.currentQueryViewForTests().buildPredicates(compiledLeaves)
	return detachPredicateSetForTests(preds), ok
}

func (qe *queryEngine) buildPredicatesOrdered(leaves []qx.Expr, orderField string) ([]predicate, bool) {
	prepared, compiledLeaves, err := prepareTestExprs(qe, leaves)
	if err != nil {
		return nil, false
	}
	defer releasePreparedQueriesForTest(prepared)

	preds, ok := qe.currentQueryViewForTests().buildPredicatesOrdered(compiledLeaves, orderField)
	return detachPredicateSetForTests(preds), ok
}

func (qe *queryEngine) shouldPreferExecutionNoOrderPrefix(q *qx.QX, leaves []qx.Expr) bool {
	preparedQ, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return false
	}
	defer preparedQ.Release()

	preparedLeaves, compiledLeaves, err := prepareTestExprs(qe, leaves)
	if err != nil {
		return false
	}
	defer releasePreparedQueriesForTest(preparedLeaves)

	return qe.currentQueryViewForTests().shouldPreferExecutionNoOrderPrefix(&viewQ, compiledLeaves)
}

func (qe *queryEngine) shouldPreferExecutionPlan(q *qx.QX) bool {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return false
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().shouldPreferExecutionPlan(&viewQ, nil)
}

func (qe *queryEngine) shouldPreferOROrderFallbackFirst(q *qx.QX, branches plannerORBranches) bool {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return false
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().shouldPreferOROrderFallbackFirst(&viewQ, branches)
}

func (qe *queryEngine) shouldUseCandidateOrder(o qx.Order, leaves []qx.Expr) bool {
	dir := qx.ASC
	if o.Desc {
		dir = qx.DESC
	}
	order, err := qir.PrepareQuery(qx.Query().SortBy(o.By, dir), qe.indexedFieldMap)
	if err != nil {
		return false
	}
	defer order.Release()
	orderView := qir.NewShape(order)
	if !orderView.HasOrder {
		return false
	}

	prepared, compiledLeaves, err := prepareTestExprs(qe, leaves)
	if err != nil {
		return false
	}
	defer releasePreparedQueriesForTest(prepared)

	return qe.currentQueryViewForTests().shouldUseCandidateOrder(orderView.Order, compiledLeaves)
}

func (qe *queryEngine) buildPredicatesWithMode(leaves []qx.Expr, allowMaterialize bool) ([]predicate, bool) {
	prepared, compiledLeaves, err := prepareTestExprs(qe, leaves)
	if err != nil {
		return nil, false
	}
	defer releasePreparedQueriesForTest(prepared)

	preds, ok := qe.currentQueryViewForTests().buildPredicatesWithMode(compiledLeaves, allowMaterialize)
	return detachPredicateSetForTests(preds), ok
}

func (qe *queryEngine) buildCountPredicatesWithMode(leaves []qx.Expr, allowMaterialize bool) ([]predicate, bool) {
	prepared, compiledLeaves, err := prepareTestExprs(qe, leaves)
	if err != nil {
		return nil, false
	}
	defer releasePreparedQueriesForTest(prepared)

	preds, ok := qe.currentQueryViewForTests().buildCountPredicatesWithMode(compiledLeaves, allowMaterialize)
	return detachPredicateSetForTests(preds), ok
}

func detachPredicateSetForTests(preds predicateSet) []predicate {
	out := make([]predicate, preds.Len())
	for i := 0; i < len(out); i++ {
		out[i] = preds.Get(i)
	}
	if preds.owner != nil {
		for i := 0; i < preds.owner.Len(); i++ {
			preds.owner.Set(i, predicate{})
		}
		predicateSlicePool.Put(preds.owner)
	}
	return out
}

func detachCountLeadResidualExactFiltersForTests(owner *pooled.Slice[countLeadResidualExactFilter]) []countLeadResidualExactFilter {
	if owner == nil {
		return nil
	}
	out := make([]countLeadResidualExactFilter, 0, owner.Len())
	for i := 0; i < owner.Len(); i++ {
		out = append(out, owner.Get(i))
	}
	owner.Truncate()
	countLeadResidualExactFilterSlicePool.Put(owner)
	return out
}

func (qe *queryEngine) buildCountLeadResidualExactFilters(t *testing.T, preds []predicate, active []int) []countLeadResidualExactFilter {
	t.Helper()
	qv := qe.currentQueryViewForTests()
	if len(active) > countPredicateScanMaxLeaves {
		t.Fatalf("unexpected active predicate count: got=%d max=%d", len(active), countPredicateScanMaxLeaves)
	}
	var candidatesInline [countPredicateScanMaxLeaves]int
	candidates := qv.collectCountLeadResidualExactCandidatesInto(candidatesInline[:0], predicateSliceView(preds), active, nil)
	if len(candidates) == 0 {
		return nil
	}
	filtersBuf := countLeadResidualExactFilterSlicePool.Get()
	filtersBuf.Grow(len(candidates))
	qv.buildCountLeadResidualExactFiltersByCandidatesInto(filtersBuf, predicateSliceView(preds), candidates)
	if filtersBuf.Len() == 0 {
		countLeadResidualExactFilterSlicePool.Put(filtersBuf)
		return nil
	}
	return detachCountLeadResidualExactFiltersForTests(filtersBuf)
}

func (qe *queryEngine) tryCountByPredicatesLeadBuckets(preds []predicate, leadIdx int, active []int) (uint64, uint64, bool) {
	predSet := newPredicateSet(len(preds))
	for i := range preds {
		predSet.Append(preds[i])
	}
	defer predSet.Release()
	return qe.currentQueryViewForTests().tryCountByPredicatesLeadBuckets(predSet, leadIdx, active)
}

func (qe *queryEngine) tryCountByPredicatesLeadPostings(preds []predicate, leadIdx int, active []int) (uint64, uint64, bool) {
	predSet := newPredicateSet(len(preds))
	for i := range preds {
		predSet.Append(preds[i])
	}
	defer predSet.Release()
	return qe.currentQueryViewForTests().tryCountByPredicatesLeadPostings(predSet, leadIdx, active)
}

func (qe *queryEngine) tryCountORByPredicates(expr qx.Expr, trace *queryTrace) (uint64, bool, error) {
	prepared, compiled, err := prepareTestExpr(qe, expr)
	if err != nil {
		return 0, false, err
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().tryCountORByPredicates(compiled, trace)
}

func (qe *queryEngine) pickCountORLeadPredicate(preds []predicate, universe uint64) (int, uint64, uint64) {
	predSet := newPredicateSet(len(preds))
	for i := range preds {
		predSet.Append(preds[i])
	}
	defer predSet.Release()
	return qe.currentQueryViewForTests().pickCountORLeadPredicate(predSet, universe)
}

func (qe *queryEngine) exprValueToIdxScalar(expr qx.Expr) (string, bool, bool, error) {
	prepared, compiled, err := prepareTestExpr(qe, expr)
	if err != nil {
		return "", false, false, err
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().exprValueToIdxScalar(compiled)
}

func (qe *queryEngine) tryCountByPredicates(expr qx.Expr, trace *queryTrace) (uint64, bool, error) {
	prepared, compiled, err := prepareTestExpr(qe, expr)
	if err != nil {
		return 0, false, err
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().tryCountByPredicates(compiled, trace)
}

func (qe *queryEngine) extractNoOrderBounds(leaves []qx.Expr) (string, rangeBounds, bool, error) {
	prepared, compiledLeaves, err := prepareTestExprs(qe, leaves)
	if err != nil {
		return "", rangeBounds{}, false, err
	}
	defer releasePreparedQueriesForTest(prepared)
	return qe.currentQueryViewForTests().extractNoOrderBounds(compiledLeaves)
}

func (qe *queryEngine) checkUsedQuery(q *qx.QX) error {
	prepared, viewQ, err := prepareTestQuery(qe, q)
	if err != nil {
		return err
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().checkUsedQuery(&viewQ)
}

func (qe *queryEngine) evalSimple(e qx.Expr) (postingResult, error) {
	prepared, expr, err := prepareTestExpr(qe, e)
	if err != nil {
		return postingResult{}, err
	}
	defer prepared.Release()
	return qe.currentQueryViewForTests().evalSimple(expr)
}

func (qe *queryEngine) exprValueToIdxOwned(expr qx.Expr) ([]string, bool, bool, error) {
	qv := qe.currentQueryViewForTests()
	var (
		prepared *qir.Query
		compiled qir.Expr
		fm       *field
		err      error
	)
	if qv.engine == nil || len(qv.engine.indexedFieldAccess) == 0 {
		prepared, err = qir.PrepareCountExprResolved(testExprFieldResolver{}, expr)
		if err != nil {
			return nil, false, false, err
		}
		compiled = prepared.Expr
	} else {
		prepared, compiled, err = prepareTestExpr(qe, expr)
		if err != nil {
			return nil, false, false, err
		}
		fm = qv.fields[qv.engine.fieldNameByOrdinal(compiled.FieldOrdinal)]
	}
	defer prepared.Release()

	if compiled.Value == nil {
		if compiled.Op == qir.OpIN {
			return nil, true, false, nil
		}
		return nil, false, true, nil
	}
	switch v := compiled.Value.(type) {
	case []string:
		if fm != nil && (fm.UseVI || fm.Kind == reflect.String) {
			return slices.Clone(v), true, false, nil
		}
	case string:
		if fm != nil && (fm.UseVI || fm.Kind == reflect.String) {
			return []string{v}, false, false, nil
		}
	}

	v := reflect.ValueOf(compiled.Value)
	v, isNil := unwrapExprValue(v)
	if isNil {
		if compiled.Op == qir.OpIN {
			return nil, true, false, nil
		}
		return nil, false, true, nil
	}

	if queryValueIsCollectionForField(v, fm) {
		valsBuf, hasNil, err := sliceValueToIdxStringBuf(v, fm)
		if err != nil {
			return nil, true, false, err
		}
		if valsBuf == nil {
			return nil, true, hasNil, nil
		}
		defer stringSlicePool.Put(valsBuf)

		out := make([]string, valsBuf.Len())
		for i := 0; i < valsBuf.Len(); i++ {
			out[i] = valsBuf.Get(i)
		}
		return out, true, hasNil, nil
	}

	key, err := scalarValueToIdxField(compiled.Value, v, fm)
	if err != nil {
		return nil, false, false, err
	}
	return []string{key}, false, false, nil
}

func (qe *queryEngine) exprValueToDistinctIdxOwned(expr qx.Expr) ([]string, bool, bool, error) {
	qv := qe.currentQueryViewForTests()
	var (
		prepared *qir.Query
		compiled qir.Expr
		err      error
	)
	if qv.engine == nil || len(qv.engine.indexedFieldAccess) == 0 {
		prepared, err = qir.PrepareCountExprResolved(testExprFieldResolver{}, expr)
		if err != nil {
			return nil, false, false, err
		}
		compiled = prepared.Expr
	} else {
		prepared, compiled, err = prepareTestExpr(qe, expr)
		if err != nil {
			return nil, false, false, err
		}
	}
	defer prepared.Release()
	if qv.engine == nil || len(qv.engine.indexedFieldAccess) == 0 {
		if compiled.Value == nil {
			if compiled.Op == qir.OpIN {
				return nil, true, false, nil
			}
			return nil, false, true, nil
		}
		v := reflect.ValueOf(compiled.Value)
		v, isNil := unwrapExprValue(v)
		if isNil {
			if compiled.Op == qir.OpIN {
				return nil, true, false, nil
			}
			return nil, false, true, nil
		}
		if queryValueIsCollectionForField(v, nil) {
			valsBuf, hasNil, err := sliceValueToIdxStringBuf(v, nil)
			if err != nil || valsBuf == nil {
				return nil, true, hasNil, err
			}
			defer stringSlicePool.Put(valsBuf)
			dedupStringBufInPlace(valsBuf)
			out := make([]string, valsBuf.Len())
			for i := 0; i < valsBuf.Len(); i++ {
				out[i] = valsBuf.Get(i)
			}
			return out, true, hasNil, nil
		}
		return nil, false, false, nil
	}
	valsBuf, isSlice, hasNil, err := qv.exprValueToDistinctIdxBuf(compiled)
	if err != nil || valsBuf == nil {
		return nil, isSlice, hasNil, err
	}
	defer stringSlicePool.Put(valsBuf)

	out := make([]string, valsBuf.Len())
	for i := 0; i < valsBuf.Len(); i++ {
		out[i] = valsBuf.Get(i)
	}
	return out, isSlice, hasNil, nil
}

func unionPostingConsumerSets(src []posting.List) []uint64 {
	seen := make(map[uint64]struct{}, 1024)
	out := make([]uint64, 0, 1024)
	for i := range src {
		for _, id := range src[i].ToArray() {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out
}

func TestQueryViewParallelBatchedPostingUnionKeepsInputsStable(t *testing.T) {
	sources := make([]posting.List, 0, 320)
	sourceWants := make([][]uint64, 0, 320)
	for i := 0; i < 320; i++ {
		var ids posting.List
		base := uint64(i * 16)
		ids = ids.BuildAdded(base + 1)
		ids = ids.BuildAdded(base + 3)
		ids = ids.BuildAdded(base + 5)
		if i%3 == 0 {
			ids = ids.BuildAdded(1 << 32)
		}
		if i%7 == 0 {
			ids = ids.BuildAdded(2<<32 | uint64(i))
		}
		sourceWants = append(sourceWants, ids.ToArray())
		sources = append(sources, ids)
	}
	defer posting.ReleaseAll(sources)

	posts := make([]posting.List, len(sources))
	for i := range sources {
		posts[i] = sources[i].Borrow()
	}
	want := unionPostingConsumerSets(sources)

	var failed atomic.Pointer[string]
	setFailed := func(msg string) {
		if failed.Load() != nil {
			return
		}
		copyMsg := msg
		failed.CompareAndSwap(nil, &copyMsg)
	}

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			out := parallelBatchedPostingUnionOwned(posts)
			if !slices.Equal(out.ToArray(), want) {
				setFailed(fmt.Sprintf("union mismatch: got=%v want=%v", out.ToArray(), want))
				out.Release()
				return
			}
			out = out.BuildAdded(9<<32 | id)
			out.Release()
		}(uint64(g))
	}

	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}

	for i := range sources {
		if !slices.Equal(sources[i].ToArray(), sourceWants[i]) {
			t.Fatalf("source posting #%d changed after parallel union", i)
		}
	}
}
