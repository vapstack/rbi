package rbi

import (
	"fmt"
	"unsafe"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qir"
	"go.etcd.io/bbolt"
)

// Query evaluates the given query against the index and returns all matching values.
func (db *DB[K, V]) Query(q *qx.QX) ([]*V, error) {
	if err := db.beginOp(); err != nil {
		return nil, err
	}
	defer db.endOp()

	if db.transparent {
		return nil, ErrNoIndex
	}
	if q == nil {
		return nil, fmt.Errorf("QX is nil")
	}
	prepared, err := db.prepareQuery(q)
	if err != nil {
		return nil, err
	}
	defer prepared.Release()
	viewQ := qir.NewShape(prepared)

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		return nil, err
	}
	defer db.unpinSnapshotRef(seq, ref)
	defer rollback(tx)

	return db.queryRecords(tx, snap, &viewQ)
}

func (qv *queryView[K, V]) tryQueryEmptyOnSnapshot(q *qir.Shape) (bool, error) {
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
		key, isSlice, isNil, err := qv.exprValueToIdxScalar(e)
		if err != nil {
			return false, err
		}
		if isSlice {
			return false, nil
		}
		if isNil {
			return qv.nilFieldOverlayForExpr(e).lookupCardinality(nilIndexEntryKey) == 0, nil
		}
		return qv.fieldOverlayForExpr(e).lookupCardinality(key) == 0, nil

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
		var bounds rangeBounds
		bounds.has = true
		applyNormalizedScalarBound(&bounds, bound)
		if bounds.empty {
			return true, nil
		}
		ov := qv.fieldOverlayForExpr(e)
		if !ov.hasData() {
			return true, nil
		}
		return overlayRangeEmpty(ov.rangeForBounds(bounds)), nil
	}

	return false, nil
}

func (db *DB[K, V]) beginQueryTxSnapshot() (*bbolt.Tx, *indexSnapshot, uint64, *snapshotRef, error) {
	// Hold the registry read lock across Begin(false) -> Sequence() -> pin so a
	// writer cannot publish/retire away the exact snapshot needed by this read tx
	// in the gap between opening the tx and pinning its sequence-aligned snapshot.
	db.snapshot.mu.RLock()
	tx, err := db.bolt.Begin(false)
	if err != nil {
		db.snapshot.mu.RUnlock()
		return nil, nil, 0, nil, fmt.Errorf("tx error: %w", err)
	}

	bucket := tx.Bucket(db.bucket)
	if bucket == nil {
		db.snapshot.mu.RUnlock()
		_ = tx.Rollback()
		return nil, nil, 0, nil, fmt.Errorf("bucket does not exist")
	}

	seq := bucket.Sequence()
	ref := db.snapshot.bySeq[seq]
	if ref == nil || ref.snap == nil {
		db.snapshot.mu.RUnlock()
		_ = tx.Rollback()
		if err = db.unavailableErr(); err != nil {
			return nil, nil, 0, nil, err
		}
		return nil, nil, 0, nil, fmt.Errorf("snapshot sequence %d is not available", seq)
	}
	ref.refs.Add(1)
	snap := ref.snap
	db.snapshot.mu.RUnlock()
	return tx, snap, seq, ref, nil
}

func (db *DB[K, V]) queryRecords(tx *bbolt.Tx, snap *indexSnapshot, q *qir.Shape) ([]*V, error) {
	view := db.makeQueryView(snap)
	defer db.releaseQueryView(view)

	if !db.traceOrCalibrationSamplingEnabled() {
		if empty, err := view.tryQueryEmptyOnSnapshot(q); empty || err != nil {
			return nil, err
		}
	}

	ids, err := view.execQuery(q, true, false)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	values, err := db.batchGetTxCompact(tx, ids)
	if err != nil {
		return nil, err
	}
	return values, nil
}

func (db *DB[K, V]) pinCurrentSnapshot() (*indexSnapshot, uint64, *snapshotRef, bool) {
	for {
		db.snapshot.mu.RLock()
		ref := db.snapshot.currentRef.Load()
		if ref == nil {
			db.snapshot.mu.RUnlock()
			return db.buildPublishedSnapshotNoLock(0), 0, nil, false
		}
		ref.refs.Add(1)
		if db.snapshot.currentRef.Load() != ref {
			db.snapshot.mu.RUnlock()
			ref.refs.Add(-1)
			continue
		}
		snap := ref.snap
		db.snapshot.mu.RUnlock()
		if snap == nil {
			ref.refs.Add(-1)
			continue
		}
		return snap, snap.seq, ref, true
	}
}

func (db *DB[K, V]) unpinCurrentSnapshot(seq uint64, ref *snapshotRef, pinned bool) {
	if !pinned || ref == nil {
		return
	}
	db.unpinSnapshotRef(seq, ref)
}

// QueryKeys evaluates the given query against the index and returns all matching ids.
func (db *DB[K, V]) QueryKeys(q *qx.QX) ([]K, error) {
	if err := db.beginOp(); err != nil {
		return nil, err
	}
	defer db.endOp()

	if db.transparent {
		return nil, ErrNoIndex
	}
	if q == nil {
		return nil, fmt.Errorf("QX is nil")
	}
	prepared, err := db.prepareQuery(q)
	if err != nil {
		return nil, err
	}
	defer prepared.Release()
	viewQ := qir.NewShape(prepared)

	snap, seq, ref, pinned := db.pinCurrentSnapshot()
	defer db.unpinCurrentSnapshot(seq, ref, pinned)

	view := db.makeQueryView(snap)
	defer db.releaseQueryView(view)

	return view.execQuery(&viewQ, true, false)
}

// execPreparedQuery skips normalize/field-validation and tracing for internal
// callers that already operate on validated/normalized QX.
func (db *DB[K, V]) execPreparedQuery(q *qir.Shape) ([]K, error) {
	snap, seq, ref, pinned := db.pinCurrentSnapshot()
	defer db.unpinCurrentSnapshot(seq, ref, pinned)

	view := db.makeQueryView(snap)
	defer db.releaseQueryView(view)

	return view.execPreparedQuery(q)
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

func (db *DB[K, V]) makeQueryView(snap *indexSnapshot) *queryView[K, V] {
	root := db
	if db.traceRoot != nil {
		root = db.traceRoot
	}
	view := root.viewPool.Get()
	*view = queryView[K, V]{
		root:              root,
		snap:              snap,
		strkey:            root.strkey,
		strmapView:        snap.strmap,
		fields:            root.fields,
		planner:           &root.planner,
		options:           root.options,
		lenZeroComplement: snap.lenZeroComplement,
	}
	return view
}

func (db *DB[K, V]) releaseQueryView(view *queryView[K, V]) {
	if view == nil {
		return
	}
	root := db
	if db.traceRoot != nil {
		root = db.traceRoot
	}
	*view = queryView[K, V]{}
	root.viewPool.Put(view)
}

func finishQueryTrace[K ~uint64 | ~string, V any](trace *queryTrace, out *[]K, err *error) {
	if trace == nil {
		return
	}
	trace.finish(uint64(len(*out)), *err)
}

// Broad negative full scans are faster when we materialize the complement once
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

func (qv *queryView[K, V]) materializeNegativeResultKeys() []uint64 {
	universe := qv.snapshotUniverseView()
	return universe.ToArray()
}

func (qv *queryView[K, V]) materializeNegativeResultKeysExcluding(exclude posting.List) []uint64 {
	if exclude.IsEmpty() {
		return qv.materializeNegativeResultKeys()
	}
	ids := qv.snapshotUniverseView().BuildAndNot(exclude)
	if ids.IsEmpty() {
		return nil
	}
	out := ids.ToArray()
	ids.Release()
	return out
}

func (qv *queryView[K, V]) execPreparedQuery(q *qir.Shape) ([]K, error) {
	return qv.execQuery(q, false, true)
}

// execQuery runs the full query pipeline against one snapshot view.
//
// The method keeps all routing decisions in one place so fast-path/planner
// selection remains consistent across QueryKeys and Query callers.
func (qv *queryView[K, V]) execQuery(q *qir.Shape, emitTrace bool, prepared bool) (out []K, err error) {
	traceEnabled := emitTrace && qv.root.traceOrCalibrationSamplingEnabled()

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

	var trace *queryTrace
	if traceEnabled {
		trace = qv.root.beginTrace(*q)
		if trace != nil {
			defer finishQueryTrace[K, V](trace, &out, &err)
		}
	}

	var ok bool

	if out, ok, err = qv.tryUniqueEqNoOrder(q, trace); ok {
		return out, err
	}

	if out, ok, err = qv.tryNoFilterNoOrderWithLimit(q, trace); ok {
		if trace != nil {
			trace.setPlan(PlanLimit)
		}
		return out, err
	}

	if out, ok, err = qv.tryOrderBasicNoFilterWithLimit(q, trace); ok {
		if trace != nil {
			trace.setPlan(PlanLimitOrderBasic)
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
		trace.setPlan(PlanMaterialized)
	}

	result, err := qv.evalExpr(q.Expr)
	if err != nil {
		return nil, err
	}
	if !result.neg {
		if result.ids.IsEmpty() {
			result.release()
			return nil, nil
		}
	} else {
		if qv.snapshotUniverseCardinality() == 0 {
			result.release()
			return nil, nil
		}
	}
	defer result.release()

	skip := q.Offset
	needAll := q.Limit == 0
	need := q.Limit

	// case 1: no ordering, negative result: iterate over universe excluding the result set
	if !q.HasOrder && result.neg {
		if !qv.strkey && needAll && skip == 0 &&
			shouldMaterializeNegativeAllNumericKeys(qv.snapshotUniverseCardinality(), result.ids.Cardinality()) {
			ids := qv.materializeNegativeResultKeysExcluding(result.ids)
			if len(ids) == 0 {
				return nil, nil
			}
			return unsafe.Slice((*K)(unsafe.Pointer(&ids[0])), len(ids)), nil
		}
		out = makeOutSlice[K](qv.postingResultCardinality(result), need)
		cursor := qv.newQueryCursor(out, skip, need, needAll, 0)

		ex := result.ids
		universe := qv.snapshotUniverseView()
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
		orderField := qv.fieldNameByOrder(order)

		switch order.Kind {

		case qir.OrderKindArrayPos:
			ov := qv.fieldOverlayForOrder(order)
			if !ov.hasData() && !qv.hasIndexedFieldForOrder(order) {
				return nil, fmt.Errorf("cannot sort non-indexed field: %v", orderField)
			}
			return qv.queryOrderArrayPosOverlay(result, ov, order, skip, need, needAll)

		case qir.OrderKindArrayCount:
			lenOV := qv.lenFieldOverlayForOrder(order)
			useZeroComplement := qv.isLenZeroComplementForOrder(order)
			if !lenOV.hasData() && !qv.hasIndexedLenField(orderField) {
				return nil, fmt.Errorf("cannot sort non-indexed field: %v", orderField)
			}
			slice := qv.snapshotLenFieldIndexSliceForOrder(order)
			if slice == nil {
				return nil, fmt.Errorf("cannot sort non-indexed field: %v", orderField)
			}
			return qv.queryOrderArrayCount(result, *slice, order, skip, need, needAll, useZeroComplement)
		}

		ov := qv.fieldOverlayForOrder(order)
		if !ov.hasData() && !qv.hasIndexedFieldForOrder(order) {
			return nil, fmt.Errorf("cannot sort non-indexed field: %v", orderField)
		}
		return qv.queryOrderBasic(result, ov, order, skip, need, needAll)
	}

	// case 3: no ordering, positive result:
	// for numeric keys and unbounded result, return a zero-copy reinterpretation
	// to avoid an extra allocation/copy in the hottest read path.

	// Fast-path only when unbounded query also has no offset.
	// Offset requires cursor-based pagination logic.
	if !qv.strkey && needAll && skip == 0 {
		ids := result.ids.ToArray()
		if len(ids) == 0 {
			return nil, nil
		}
		return unsafe.Slice((*K)(unsafe.Pointer(&ids[0])), len(ids)), nil
	}

	out = makeOutSlice[K](result.ids.Cardinality(), need)
	cursor := qv.newQueryCursor(out, skip, need, needAll, 0)

	iter := result.ids.Iter()
	defer iter.Release()
	for iter.HasNext() {
		if cursor.emit(iter.Next()) {
			return cursor.out, nil
		}
	}
	return cursor.out, nil
}
