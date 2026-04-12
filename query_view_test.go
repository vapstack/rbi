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

func (db *DB[K, V]) evalExpr(e qx.Expr) (postingResult, error) {
	return db.currentQueryViewForTests().evalExpr(e)
}

func (db *DB[K, V]) tryExecutionPlan(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	return db.currentQueryViewForTests().tryExecutionPlan(q, trace)
}

func (db *DB[K, V]) tryPlan(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	return db.currentQueryViewForTests().tryPlan(q, trace)
}

func (db *DB[K, V]) countPreparedExpr(expr qx.Expr) (uint64, error) {
	return db.currentQueryViewForTests().countPreparedExpr(expr)
}

func (db *DB[K, V]) tryPlanCandidate(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	return db.currentQueryViewForTests().tryPlanCandidate(q, trace)
}

func (db *DB[K, V]) tryLimitQueryOrderBasic(q *qx.QX, leaves []qx.Expr, trace *queryTrace) ([]K, bool, error) {
	return db.currentQueryViewForTests().tryLimitQueryOrderBasic(q, leaves, trace)
}

func (db *DB[K, V]) tryLimitQueryRangeNoOrderByField(q *qx.QX, field string, bounds rangeBounds, rest []qx.Expr, trace *queryTrace) ([]K, bool, error) {
	return db.currentQueryViewForTests().tryLimitQueryRangeNoOrderByField(q, field, bounds, rest, trace)
}

func (db *DB[K, V]) tryQueryOrderBasicWithLimit(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	return db.currentQueryViewForTests().tryQueryOrderBasicWithLimit(q, trace)
}

func (db *DB[K, V]) tryQueryOrderPrefixWithLimit(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	return db.currentQueryViewForTests().tryQueryOrderPrefixWithLimit(q, trace)
}

func (db *DB[K, V]) tryQueryRangeNoOrderWithLimit(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	return db.currentQueryViewForTests().tryQueryRangeNoOrderWithLimit(q, trace)
}

func (db *DB[K, V]) tryQueryPrefixNoOrderWithLimit(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	return db.currentQueryViewForTests().tryQueryPrefixNoOrderWithLimit(q, trace)
}

func (db *DB[K, V]) buildPredicatesOrderedWithMode(leaves []qx.Expr, orderField string, allowMaterialize bool, orderedWindow int, orderedOffset uint64, coverOrderRange bool, allowOrderedEagerMaterialize bool) ([]predicate, bool) {
	preds, ok := db.currentQueryViewForTests().buildPredicatesOrderedWithMode(leaves, orderField, allowMaterialize, orderedWindow, orderedOffset, coverOrderRange, allowOrderedEagerMaterialize)
	return detachPredicateSetForTests(preds), ok
}

func (db *DB[K, V]) execPlanOrderedBasic(q *qx.QX, preds []predicate, trace *queryTrace) ([]K, bool) {
	return db.currentQueryViewForTests().execPlanOrderedBasicReader(q, predicateSliceView(preds), trace)
}

func (db *DB[K, V]) execPlanOrderedBasicFallback(q *qx.QX, preds []predicate, active []int, start, end int, s []index, trace *queryTrace) []K {
	return db.currentQueryViewForTests().execPlanOrderedBasicFallback(q, preds, active, start, end, s, trace)
}

func (db *DB[K, V]) buildORBranches(ops []qx.Expr) (plannerORBranches, bool, bool) {
	return db.currentQueryViewForTests().buildORBranches(ops)
}

func (db *DB[K, V]) buildORBranchesOrdered(ops []qx.Expr, orderField string, orderedWindow int) (plannerORBranches, bool, bool) {
	return db.currentQueryViewForTests().buildORBranchesOrdered(ops, orderField, orderedWindow, 0)
}

func (db *DB[K, V]) execPlanORNoOrderAdaptive(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]K, bool) {
	return db.currentQueryViewForTests().execPlanORNoOrderAdaptive(q, branches, trace)
}

func (db *DB[K, V]) execPlanORNoOrderBaseline(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]K, bool) {
	return db.currentQueryViewForTests().execPlanORNoOrderBaseline(q, branches, trace)
}

func (db *DB[K, V]) execPlanOROrderBasic(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]K, bool) {
	return db.currentQueryViewForTests().execPlanOROrderBasic(q, branches, trace, nil)
}

func (db *DB[K, V]) execPlanOROrderMergeFallback(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]K, bool, error) {
	return db.currentQueryViewForTests().execPlanOROrderMergeFallback(q, branches, trace)
}

func (db *DB[K, V]) execPlanOROrderKWay(q *qx.QX, branches plannerORBranches, trace *queryTrace) ([]K, bool, error) {
	return db.currentQueryViewForTests().execPlanOROrderKWay(q, branches, trace)
}

func (db *DB[K, V]) materializedPredCacheKey(e qx.Expr) string {
	return db.currentQueryViewForTests().materializedPredCacheKey(e)
}

func (db *DB[K, V]) buildPredRangeCandidateWithMode(e qx.Expr, fm *field, ov fieldOverlay, allowMaterialize bool) (predicate, bool) {
	return db.currentQueryViewForTests().buildPredRangeCandidateWithMode(e, fm, ov, allowMaterialize)
}

func (db *DB[K, V]) buildPredicateWithMode(e qx.Expr, allowMaterialize bool) (predicate, bool) {
	return db.currentQueryViewForTests().buildPredicateWithMode(e, allowMaterialize)
}

func (db *DB[K, V]) collectOrderRangeBounds(field string, n int, exprAt func(i int) qx.Expr) (rangeBounds, []bool, bool, bool) {
	rb, covered, has, ok := db.currentQueryViewForTests().collectOrderRangeBounds(field, n, exprAt)
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
	return db.currentQueryViewForTests().decidePlanORNoOrder(q, branches)
}

func (db *DB[K, V]) shouldUseOROrderKWayRuntimeFallback(q *qx.QX, branches plannerORBranches, needWindow int) bool {
	return db.currentQueryViewForTests().shouldUseOROrderKWayRuntimeFallback(q, branches, needWindow)
}

func (db *DB[K, V]) buildPredicates(leaves []qx.Expr) ([]predicate, bool) {
	preds, ok := db.currentQueryViewForTests().buildPredicates(leaves)
	return detachPredicateSetForTests(preds), ok
}

func (db *DB[K, V]) buildPredicatesOrdered(leaves []qx.Expr, orderField string) ([]predicate, bool) {
	preds, ok := db.currentQueryViewForTests().buildPredicatesOrdered(leaves, orderField)
	return detachPredicateSetForTests(preds), ok
}

func (db *DB[K, V]) shouldPreferExecutionNoOrderPrefix(q *qx.QX, leaves []qx.Expr) bool {
	return db.currentQueryViewForTests().shouldPreferExecutionNoOrderPrefix(q, leaves)
}

func (db *DB[K, V]) shouldPreferOROrderFallbackFirst(q *qx.QX, branches plannerORBranches) bool {
	return db.currentQueryViewForTests().shouldPreferOROrderFallbackFirst(q, branches)
}

func (db *DB[K, V]) shouldUseCandidateOrder(o qx.Order, leaves []qx.Expr) bool {
	return db.currentQueryViewForTests().shouldUseCandidateOrder(o, leaves)
}

func (db *DB[K, V]) countPostingResult(b postingResult) uint64 {
	return db.currentQueryViewForTests().countPostingResult(b)
}

func (db *DB[K, V]) buildPredicatesWithMode(leaves []qx.Expr, allowMaterialize bool) ([]predicate, bool) {
	preds, ok := db.currentQueryViewForTests().buildPredicatesWithMode(leaves, allowMaterialize)
	return detachPredicateSetForTests(preds), ok
}

func (db *DB[K, V]) buildCountPredicatesWithMode(leaves []qx.Expr, allowMaterialize bool) ([]predicate, bool) {
	preds, ok := db.currentQueryViewForTests().buildCountPredicatesWithMode(leaves, allowMaterialize)
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
	return db.currentQueryViewForTests().tryCountORByPredicates(expr, trace)
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
	return db.currentQueryViewForTests().exprValueToIdxScalar(expr)
}

func (db *DB[K, V]) materializedPredCacheKeyForScalar(field string, op qx.Op, key string) string {
	return db.currentQueryViewForTests().materializedPredCacheKeyForScalar(field, op, key)
}

func (db *DB[K, V]) materializedPredComplementCacheKeyForScalar(field string, op qx.Op, key string) string {
	return db.currentQueryViewForTests().materializedPredComplementCacheKeyForScalar(field, op, key)
}

func (db *DB[K, V]) pickCountLeadPredicate(preds []predicate, universe uint64) (int, uint64, uint64) {
	return db.currentQueryViewForTests().pickCountLeadPredicate(predicateSliceView(preds), universe)
}

func (db *DB[K, V]) tryCountByPredicates(expr qx.Expr, trace *queryTrace) (uint64, bool, error) {
	return db.currentQueryViewForTests().tryCountByPredicates(expr, trace)
}

func (db *DB[K, V]) extractNoOrderBounds(leaves []qx.Expr) (string, rangeBounds, bool, error) {
	return db.currentQueryViewForTests().extractNoOrderBounds(leaves)
}

func (db *DB[K, V]) checkUsedQuery(q *qx.QX) error {
	return db.currentQueryViewForTests().checkUsedQuery(q)
}

func (db *DB[K, V]) tryEvalNumericRangeBuckets(field string, fm *field, ov fieldOverlay, br overlayRange) (postingResult, bool) {
	return db.currentQueryViewForTests().tryEvalNumericRangeBuckets(field, fm, ov, br)
}

func (db *DB[K, V]) evalSimple(e qx.Expr) (postingResult, error) {
	return db.currentQueryViewForTests().evalSimple(e)
}

func (db *DB[K, V]) tryCountSnapshotNumericRange(field string, fm *field, ov fieldOverlay, start, end int) (uint64, bool) {
	return db.currentQueryViewForTests().tryCountSnapshotNumericRange(field, fm, ov, start, end)
}

func (db *DB[K, V]) exprValueToIdxOwned(expr qx.Expr) ([]string, bool, bool, error) {
	qv := db.currentQueryViewForTests()
	fm := qv.fields[expr.Field]

	if expr.Value == nil {
		if expr.Op == qx.OpIN {
			return nil, true, false, nil
		}
		return nil, false, true, nil
	}
	switch v := expr.Value.(type) {
	case []string:
		if fm != nil && (fm.UseVI || fm.Kind == reflect.String) {
			return slices.Clone(v), true, false, nil
		}
	case string:
		if fm != nil && (fm.UseVI || fm.Kind == reflect.String) {
			return []string{v}, false, false, nil
		}
	}

	v := reflect.ValueOf(expr.Value)
	v, isNil := unwrapExprValue(v)
	if isNil {
		if expr.Op == qx.OpIN {
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

	key, err := scalarValueToIdxField(expr.Value, v, fm)
	if err != nil {
		return nil, false, false, err
	}
	return []string{key}, false, false, nil
}

func (db *DB[K, V]) exprValueToDistinctIdxOwned(expr qx.Expr) ([]string, bool, bool, error) {
	qv := db.currentQueryViewForTests()
	valsBuf, isSlice, hasNil, err := qv.exprValueToDistinctIdxBuf(expr)
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
