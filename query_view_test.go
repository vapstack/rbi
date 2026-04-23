package rbi

import (
	"fmt"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qir"
)

var (
	_ preparedRouteEqUint64 = (*DB[uint64, Rec])(nil)
	_ preparedRouteEqString = (*DB[string, Rec])(nil)
	_ preparedRouteEqUint64 = (*queryView[uint64, Rec])(nil)
	_ preparedRouteEqString = (*queryView[string, Rec])(nil)
)

func (db *DB[K, V]) rootDB() *DB[K, V] { return db }

func (db *DB[K, V]) currentQueryViewForTests() *queryView[K, V] {
	if db == nil {
		snap := &indexSnapshot{strmap: &strMapSnapshot{}}
		return &queryView[K, V]{snap: snap, strmapView: snap.strmap}
	}
	if snap := db.snapshot.current.Load(); snap != nil {
		return &queryView[K, V]{
			root:              db,
			snap:              snap,
			strkey:            db.strkey,
			strmapView:        snap.strmap,
			fields:            db.fields,
			planner:           &db.planner,
			options:           db.options,
			lenZeroComplement: snap.lenZeroComplement,
		}
	}

	snap := &indexSnapshot{
		index:             db.index,
		nilIndex:          db.nilIndex,
		lenIndex:          db.lenIndex,
		lenZeroComplement: db.lenZeroComplement,
		universe:          db.universe,
		strmap:            &strMapSnapshot{},
	}
	if db.strmap != nil {
		snap.strmap = db.strmap.snapshot()
	}
	return &queryView[K, V]{
		root:              db,
		snap:              snap,
		strkey:            db.strkey,
		strmapView:        snap.strmap,
		fields:            db.fields,
		planner:           &db.planner,
		options:           db.options,
		lenZeroComplement: snap.lenZeroComplement,
	}
}

func testExprFieldName[K ~string | ~uint64, V any](db *DB[K, V], expr qir.Expr) string {
	if db == nil {
		return ""
	}
	return db.fieldNameByOrdinal(expr.FieldOrdinal)
}

func testOrderFieldName[K ~string | ~uint64, V any](db *DB[K, V], order qir.Order) string {
	if db == nil {
		return ""
	}
	return db.fieldNameByOrdinal(order.FieldOrdinal)
}

func (db *DB[K, V]) snapshotFieldIndexSlice(field string) *[]index {
	return db.currentQueryViewForTests().snapshotFieldIndexSlice(field)
}

func (db *DB[K, V]) snapshotLenFieldIndexSlice(field string) *[]index {
	return db.currentQueryViewForTests().snapshotLenFieldIndexSlice(field)
}

func (db *DB[K, V]) snapshotUniverseCardinality() uint64 {
	return db.currentQueryViewForTests().snapshotUniverseCardinality()
}

func (db *DB[K, V]) snapshotUniverseView() posting.List {
	return db.currentQueryViewForTests().snapshotUniverseView()
}

func (db *DB[K, V]) fieldOverlay(field string) fieldOverlay {
	return db.currentQueryViewForTests().fieldOverlay(field)
}

func (db *DB[K, V]) fieldLookupPostingRetained(field, key string) posting.List {
	return db.currentQueryViewForTests().fieldLookupPostingRetained(field, key)
}

func (db *DB[K, V]) nilFieldOverlay(field string) fieldOverlay {
	return db.currentQueryViewForTests().nilFieldOverlay(field)
}

func (db *DB[K, V]) lenFieldOverlay(field string) fieldOverlay {
	return db.currentQueryViewForTests().lenFieldOverlay(field)
}

func (qv *queryView[K, V]) rootDB() *DB[K, V] { return qv.root }

func (qv *queryView[K, V]) fieldLookupPostingRetained(field, key string) posting.List {
	return qv.snap.fieldLookupPostingRetained(field, key)
}

func (qv *queryView[K, V]) nilFieldLookupPostingRetained(field string) posting.List {
	return newFieldOverlay(qv.snap.nilFieldIndexSlice(field)).lookupPostingRetained(nilIndexEntryKey)
}

func prepareTestQuery[K ~string | ~uint64, V any](db *DB[K, V], q *qx.QX) (*qir.Query, qir.Shape, error) {
	var (
		prepared *qir.Query
		err      error
	)
	if db == nil {
		prepared, err = qir.PrepareQueryNoResolve(q)
	} else {
		prepared, err = db.prepareQuery(q)
	}
	if err != nil {
		return nil, qir.Shape{}, err
	}
	return prepared, qir.NewShape(prepared), nil
}

func prepareTestExpr[K ~string | ~uint64, V any](db *DB[K, V], expr qx.Expr) (*qir.Query, qir.Expr, error) {
	var (
		prepared *qir.Query
		err      error
	)
	if db == nil {
		prepared, err = qir.PrepareCountExprsNoResolve(expr)
	} else {
		prepared, err = db.prepareCountExprs(expr)
	}
	if err != nil {
		return nil, qir.Expr{}, err
	}
	return prepared, prepared.Expr, nil
}

func prepareTestExprs[K ~string | ~uint64, V any](db *DB[K, V], exprs []qx.Expr) ([]*qir.Query, []qir.Expr, error) {
	prepared := make([]*qir.Query, 0, len(exprs))
	out := make([]qir.Expr, 0, len(exprs))
	for i := range exprs {
		p, expr, err := prepareTestExpr(db, exprs[i])
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

func (db *DB[K, V]) evalExpr(e qx.Expr) (postingResult, error) {
	prepared, expr, err := prepareTestExpr(db, e)
	if err != nil {
		return postingResult{}, err
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().evalExpr(expr)
}

func (db *DB[K, V]) tryExecutionPlan(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	prepared, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		return nil, false, err
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().tryExecutionPlan(&viewQ, trace)
}

func (db *DB[K, V]) tryPlan(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	prepared, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		return nil, false, err
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().tryPlan(&viewQ, trace)
}

func (db *DB[K, V]) countPreparedExpr(expr qx.Expr) (uint64, error) {
	prepared, compiled, err := prepareTestExpr(db, expr)
	if err != nil {
		return 0, err
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().countPreparedExpr(compiled)
}

func (db *DB[K, V]) tryPlanCandidate(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	prepared, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		return nil, false, err
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().tryPlanCandidate(&viewQ, trace)
}

func (db *DB[K, V]) tryLimitQueryOrderBasic(q *qx.QX, leaves []qx.Expr, trace *queryTrace) ([]K, bool, error) {
	preparedQ, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		return nil, false, err
	}
	defer preparedQ.Release()

	preparedLeaves, compiledLeaves, err := prepareTestExprs(db, leaves)
	if err != nil {
		return nil, false, err
	}
	defer releasePreparedQueriesForTest(preparedLeaves)

	return db.currentQueryViewForTests().tryLimitQueryOrderBasic(&viewQ, compiledLeaves, trace)
}

func (db *DB[K, V]) tryLimitQueryRangeNoOrderByField(q *qx.QX, field string, bounds rangeBounds, rest []qx.Expr, trace *queryTrace) ([]K, bool, error) {
	preparedQ, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		return nil, false, err
	}
	defer preparedQ.Release()

	preparedRest, compiledRest, err := prepareTestExprs(db, rest)
	if err != nil {
		return nil, false, err
	}
	defer releasePreparedQueriesForTest(preparedRest)

	return db.currentQueryViewForTests().tryLimitQueryRangeNoOrderByField(&viewQ, field, bounds, compiledRest, trace)
}

func (db *DB[K, V]) tryQueryOrderBasicWithLimit(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	prepared, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		return nil, false, err
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().tryQueryOrderBasicWithLimit(&viewQ, trace)
}

func (db *DB[K, V]) tryQueryOrderPrefixWithLimit(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	prepared, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		return nil, false, err
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().tryQueryOrderPrefixWithLimit(&viewQ, trace)
}

func (db *DB[K, V]) tryQueryRangeNoOrderWithLimit(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	prepared, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		return nil, false, err
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().tryQueryRangeNoOrderWithLimit(&viewQ, trace)
}

func (db *DB[K, V]) tryQueryPrefixNoOrderWithLimit(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	prepared, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		return nil, false, err
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().tryQueryPrefixNoOrderWithLimit(&viewQ, trace)
}

func (db *DB[K, V]) buildPredicatesOrderedWithMode(leaves []qx.Expr, orderField string, allowMaterialize bool, orderedWindow int, orderedOffset uint64, coverOrderRange bool, allowOrderedEagerMaterialize bool) ([]predicate, bool) {
	prepared, compiledLeaves, err := prepareTestExprs(db, leaves)
	if err != nil {
		return nil, false
	}
	defer releasePreparedQueriesForTest(prepared)

	preds, ok := db.currentQueryViewForTests().buildPredicatesOrderedWithMode(compiledLeaves, orderField, allowMaterialize, orderedWindow, orderedOffset, coverOrderRange, allowOrderedEagerMaterialize)
	return detachPredicateSetForTests(preds), ok
}

func (db *DB[K, V]) execPlanOrderedBasic(q *qx.QX, preds []predicate, trace *queryTrace) ([]K, bool) {
	prepared, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		return nil, false
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().execPlanOrderedBasicReader(&viewQ, predicateSliceView(preds), trace)
}

func (db *DB[K, V]) execPlanOrderedBasicFallback(q *qx.QX, preds []predicate, active []int, start, end int, s []index, trace *queryTrace) []K {
	prepared, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		return nil
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().execPlanOrderedBasicFallback(&viewQ, preds, active, start, end, s, trace)
}

func (db *DB[K, V]) buildORBranches(ops []qx.Expr) (plannerORBranches, bool, bool) {
	prepared, compiledOps, err := prepareTestExprs(db, ops)
	if err != nil {
		return plannerORBranches{}, false, false
	}
	defer releasePreparedQueriesForTest(prepared)
	return db.currentQueryViewForTests().buildORBranches(compiledOps)
}

func (db *DB[K, V]) buildORBranchesOrdered(ops []qx.Expr, orderField string, orderedWindow int) (plannerORBranches, bool, bool) {
	prepared, compiledOps, err := prepareTestExprs(db, ops)
	if err != nil {
		return plannerORBranches{}, false, false
	}
	defer releasePreparedQueriesForTest(prepared)
	return db.currentQueryViewForTests().buildORBranchesOrdered(compiledOps, orderField, orderedWindow, 0)
}

func (db *DB[K, V]) buildORBranchesOrderedWithOffset(
	ops []qx.Expr,
	orderField string,
	orderedWindow int,
	orderedOffset uint64,
) (plannerORBranches, bool, bool) {
	prepared, compiledOps, err := prepareTestExprs(db, ops)
	if err != nil {
		return plannerORBranches{}, false, false
	}
	defer releasePreparedQueriesForTest(prepared)
	return db.currentQueryViewForTests().buildORBranchesOrdered(compiledOps, orderField, orderedWindow, orderedOffset)
}

func (db *DB[K, V]) execPlanORNoOrderAdaptive(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]K, bool) {
	prepared, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		return nil, false
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().execPlanORNoOrderAdaptive(&viewQ, branches, trace)
}

func (db *DB[K, V]) execPlanORNoOrderBaseline(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]K, bool) {
	prepared, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		return nil, false
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().execPlanORNoOrderBaseline(&viewQ, branches, trace)
}

func (db *DB[K, V]) execPlanOROrderBasic(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]K, bool) {
	prepared, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		return nil, false
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().execPlanOROrderBasic(&viewQ, branches, nil, trace, nil)
}

func (db *DB[K, V]) execPlanOROrderMergeFallback(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]K, bool, error) {
	prepared, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		return nil, false, err
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().execPlanOROrderMergeFallback(&viewQ, branches, trace)
}

func (db *DB[K, V]) execPlanOROrderKWay(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]K, bool, error) {
	prepared, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		return nil, false, err
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().execPlanOROrderKWay(&viewQ, branches, nil, trace)
}

func (db *DB[K, V]) materializedPredCacheKey(e qx.Expr) string {
	prepared, expr, err := prepareTestExpr(db, e)
	if err != nil {
		return ""
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().materializedPredCacheKey(expr)
}

func (db *DB[K, V]) buildPredRangeCandidateWithMode(e qx.Expr, fm *field, ov fieldOverlay, allowMaterialize bool) (predicate, bool) {
	prepared, expr, err := prepareTestExpr(db, e)
	if err != nil {
		return predicate{}, false
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().buildPredRangeCandidateWithMode(expr, fm, ov, allowMaterialize)
}

func (db *DB[K, V]) buildPredicateWithMode(e qx.Expr, allowMaterialize bool) (predicate, bool) {
	prepared, expr, err := prepareTestExpr(db, e)
	if err != nil {
		return predicate{}, false
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().buildPredicateWithMode(expr, allowMaterialize)
}

func (db *DB[K, V]) collectOrderRangeBounds(field string, n int, exprAt func(i int) qx.Expr) (rangeBounds, []bool, bool, bool) {
	prepared := make([]*qir.Query, 0, n)
	compiled := make([]qir.Expr, 0, n)
	for i := 0; i < n; i++ {
		p, expr, err := prepareTestExpr(db, exprAt(i))
		if err != nil {
			releasePreparedQueriesForTest(prepared)
			return rangeBounds{}, nil, false, false
		}
		prepared = append(prepared, p)
		compiled = append(compiled, expr)
	}
	defer releasePreparedQueriesForTest(prepared)

	rb, covered, has, ok := db.currentQueryViewForTests().collectOrderRangeBounds(field, n, func(i int) qir.Expr {
		return compiled[i]
	})
	return rb, copyBoolBufAndRelease(covered), has, ok
}

func (db *DB[K, V]) extractOrderRangeCoverage(field string, preds []predicate, s []index) (int, int, []bool, bool) {
	ov := fieldOverlay{base: s}
	br, covered, ok := db.currentQueryViewForTests().extractOrderRangeCoverageOverlayReader(field, predicateSliceView(preds), ov)
	if !ok {
		return 0, 0, nil, false
	}
	return br.baseStart, br.baseEnd, copyBoolBufAndRelease(covered), true
}

func (db *DB[K, V]) extractOrderRangeCoverageOverlay(field string, preds []predicate, ov fieldOverlay) (overlayRange, []bool, bool) {
	br, covered, ok := db.currentQueryViewForTests().extractOrderRangeCoverageOverlayReader(field, predicateSliceView(preds), ov)
	return br, copyBoolBufAndRelease(covered), ok
}

func copyBoolBufAndRelease(buf *pooled.SliceBuf[bool]) []bool {
	if buf == nil {
		return nil
	}
	out := make([]bool, buf.Len())
	for i := 0; i < buf.Len(); i++ {
		out[i] = buf.Get(i)
	}
	boolSlicePool.Put(buf)
	return out
}

func (db *DB[K, V]) decidePlanORNoOrder(q *qx.QX, branches plannerORBranches) plannerORNoOrderDecision {
	prepared, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		return plannerORNoOrderDecision{}
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().decidePlanORNoOrder(&viewQ, branches)
}

func (db *DB[K, V]) shouldUseOROrderKWayRuntimeFallback(q *qx.QX, branches plannerORBranches, needWindow int) bool {
	prepared, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		return false
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().shouldUseOROrderKWayRuntimeFallback(&viewQ, branches, needWindow)
}

func (db *DB[K, V]) buildPredicates(leaves []qx.Expr) ([]predicate, bool) {
	prepared, compiledLeaves, err := prepareTestExprs(db, leaves)
	if err != nil {
		return nil, false
	}
	defer releasePreparedQueriesForTest(prepared)

	preds, ok := db.currentQueryViewForTests().buildPredicates(compiledLeaves)
	return detachPredicateSetForTests(preds), ok
}

func (db *DB[K, V]) buildPredicatesOrdered(leaves []qx.Expr, orderField string) ([]predicate, bool) {
	prepared, compiledLeaves, err := prepareTestExprs(db, leaves)
	if err != nil {
		return nil, false
	}
	defer releasePreparedQueriesForTest(prepared)

	preds, ok := db.currentQueryViewForTests().buildPredicatesOrdered(compiledLeaves, orderField)
	return detachPredicateSetForTests(preds), ok
}

func (db *DB[K, V]) shouldPreferExecutionNoOrderPrefix(q *qx.QX, leaves []qx.Expr) bool {
	preparedQ, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		return false
	}
	defer preparedQ.Release()

	preparedLeaves, compiledLeaves, err := prepareTestExprs(db, leaves)
	if err != nil {
		return false
	}
	defer releasePreparedQueriesForTest(preparedLeaves)

	return db.currentQueryViewForTests().shouldPreferExecutionNoOrderPrefix(&viewQ, compiledLeaves)
}

func (db *DB[K, V]) shouldPreferExecutionPlan(q *qx.QX) bool {
	prepared, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		return false
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().shouldPreferExecutionPlan(&viewQ, nil)
}

func (db *DB[K, V]) shouldPreferOROrderFallbackFirst(q *qx.QX, branches plannerORBranches) bool {
	prepared, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		return false
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().shouldPreferOROrderFallbackFirst(&viewQ, branches)
}

func (db *DB[K, V]) shouldUseCandidateOrder(o qx.Order, leaves []qx.Expr) bool {
	dir := qx.ASC
	if o.Desc {
		dir = qx.DESC
	}
	order, err := qir.PrepareQueryResolved(qx.Query().SortBy(o.By, dir), preparedFieldResolver[K, V]{db: db})
	if err != nil {
		return false
	}
	defer order.Release()
	orderView := qir.NewShape(order)
	if !orderView.HasOrder {
		return false
	}

	prepared, compiledLeaves, err := prepareTestExprs(db, leaves)
	if err != nil {
		return false
	}
	defer releasePreparedQueriesForTest(prepared)

	return db.currentQueryViewForTests().shouldUseCandidateOrder(orderView.Order, compiledLeaves)
}

func (db *DB[K, V]) countPostingResult(b postingResult) uint64 {
	return db.currentQueryViewForTests().countPostingResult(b)
}

func (db *DB[K, V]) buildPredicatesWithMode(leaves []qx.Expr, allowMaterialize bool) ([]predicate, bool) {
	prepared, compiledLeaves, err := prepareTestExprs(db, leaves)
	if err != nil {
		return nil, false
	}
	defer releasePreparedQueriesForTest(prepared)

	preds, ok := db.currentQueryViewForTests().buildPredicatesWithMode(compiledLeaves, allowMaterialize)
	return detachPredicateSetForTests(preds), ok
}

func (db *DB[K, V]) buildCountPredicatesWithMode(leaves []qx.Expr, allowMaterialize bool) ([]predicate, bool) {
	prepared, compiledLeaves, err := prepareTestExprs(db, leaves)
	if err != nil {
		return nil, false
	}
	defer releasePreparedQueriesForTest(prepared)

	preds, ok := db.currentQueryViewForTests().buildCountPredicatesWithMode(compiledLeaves, allowMaterialize)
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

func detachCountLeadResidualExactFiltersForTests(owner *pooled.SliceBuf[countLeadResidualExactFilter]) []countLeadResidualExactFilter {
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

func (db *DB[K, V]) prepareCountPredicate(p *predicate, probeEst uint64, universe uint64) error {
	return db.currentQueryViewForTests().prepareCountPredicate(p, probeEst, universe)
}

func (db *DB[K, V]) buildCountLeadResidualExactFilters(t *testing.T, preds []predicate, active []int) []countLeadResidualExactFilter {
	t.Helper()
	qv := db.currentQueryViewForTests()
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

func (db *DB[K, V]) tryCountByPredicatesLeadBuckets(preds []predicate, leadIdx int, active []int) (uint64, uint64, bool) {
	predSet := newPredicateSet(len(preds))
	for i := range preds {
		predSet.Append(preds[i])
	}
	defer predSet.Release()
	return db.currentQueryViewForTests().tryCountByPredicatesLeadBuckets(predSet, leadIdx, active)
}

func (db *DB[K, V]) tryCountByPredicatesLeadPostings(preds []predicate, leadIdx int, active []int) (uint64, uint64, bool) {
	predSet := newPredicateSet(len(preds))
	for i := range preds {
		predSet.Append(preds[i])
	}
	defer predSet.Release()
	return db.currentQueryViewForTests().tryCountByPredicatesLeadPostings(predSet, leadIdx, active)
}

func (db *DB[K, V]) tryCountORByPredicates(expr qx.Expr, trace *queryTrace) (uint64, bool, error) {
	prepared, compiled, err := prepareTestExpr(db, expr)
	if err != nil {
		return 0, false, err
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().tryCountORByPredicates(compiled, trace)
}

func (db *DB[K, V]) pickCountORLeadPredicate(preds []predicate, universe uint64) (int, uint64, uint64) {
	predSet := newPredicateSet(len(preds))
	for i := range preds {
		predSet.Append(preds[i])
	}
	defer predSet.Release()
	return db.currentQueryViewForTests().pickCountORLeadPredicate(predSet, universe)
}

func (db *DB[K, V]) exprValueToIdxScalar(expr qx.Expr) (string, bool, bool, error) {
	prepared, compiled, err := prepareTestExpr(db, expr)
	if err != nil {
		return "", false, false, err
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().exprValueToIdxScalar(compiled)
}

func (db *DB[K, V]) materializedPredCacheKeyForScalar(field string, op qx.Op, key string) string {
	return db.currentQueryViewForTests().materializedPredCacheKeyForScalar(field, compileScalarOpForTest(op), key)
}

func (db *DB[K, V]) materializedPredComplementCacheKeyForScalar(field string, op qx.Op, key string) string {
	return db.currentQueryViewForTests().materializedPredComplementCacheKeyForScalar(field, compileScalarOpForTest(op), key)
}

func (db *DB[K, V]) pickCountLeadPredicate(preds []predicate, universe uint64) (int, uint64, uint64) {
	return db.currentQueryViewForTests().pickCountLeadPredicate(predicateSliceView(preds), universe)
}

func (db *DB[K, V]) tryCountByPredicates(expr qx.Expr, trace *queryTrace) (uint64, bool, error) {
	prepared, compiled, err := prepareTestExpr(db, expr)
	if err != nil {
		return 0, false, err
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().tryCountByPredicates(compiled, trace)
}

func (db *DB[K, V]) extractNoOrderBounds(leaves []qx.Expr) (string, rangeBounds, bool, error) {
	prepared, compiledLeaves, err := prepareTestExprs(db, leaves)
	if err != nil {
		return "", rangeBounds{}, false, err
	}
	defer releasePreparedQueriesForTest(prepared)
	return db.currentQueryViewForTests().extractNoOrderBounds(compiledLeaves)
}

func (db *DB[K, V]) checkUsedQuery(q *qx.QX) error {
	prepared, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		return err
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().checkUsedQuery(&viewQ)
}

func (db *DB[K, V]) tryEvalNumericRangeBuckets(field string, fm *field, ov fieldOverlay, br overlayRange) (postingResult, bool) {
	return db.currentQueryViewForTests().tryEvalNumericRangeBuckets(field, fm, ov, br)
}

func (db *DB[K, V]) evalSimple(e qx.Expr) (postingResult, error) {
	prepared, expr, err := prepareTestExpr(db, e)
	if err != nil {
		return postingResult{}, err
	}
	defer prepared.Release()
	return db.currentQueryViewForTests().evalSimple(expr)
}

func (db *DB[K, V]) tryCountSnapshotNumericRange(field string, fm *field, ov fieldOverlay, start, end int) (uint64, bool) {
	return db.currentQueryViewForTests().tryCountSnapshotNumericRange(field, fm, ov, start, end)
}

func (db *DB[K, V]) exprValueToIdxOwned(expr qx.Expr) ([]string, bool, bool, error) {
	qv := db.currentQueryViewForTests()
	var (
		prepared *qir.Query
		compiled qir.Expr
		fm       *field
		err      error
	)
	if qv.root == nil || len(qv.root.indexedFieldAccess) == 0 {
		prepared, err = qir.PrepareCountExprResolved(testExprFieldResolver{}, expr)
		if err != nil {
			return nil, false, false, err
		}
		compiled = prepared.Expr
	} else {
		prepared, compiled, err = prepareTestExpr(db, expr)
		if err != nil {
			return nil, false, false, err
		}
		fm = qv.fields[qv.fieldNameByExpr(compiled)]
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

func (db *DB[K, V]) exprValueToDistinctIdxOwned(expr qx.Expr) ([]string, bool, bool, error) {
	qv := db.currentQueryViewForTests()
	var (
		prepared *qir.Query
		compiled qir.Expr
		err      error
	)
	if qv.root == nil || len(qv.root.indexedFieldAccess) == 0 {
		prepared, err = qir.PrepareCountExprResolved(testExprFieldResolver{}, expr)
		if err != nil {
			return nil, false, false, err
		}
		compiled = prepared.Expr
	} else {
		prepared, compiled, err = prepareTestExpr(db, expr)
		if err != nil {
			return nil, false, false, err
		}
	}
	defer prepared.Release()
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
	defer posting.ReleaseSliceOwned(sources)

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
