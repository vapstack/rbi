package rbi

import (
	"fmt"
	"runtime"
	"time"
	"unsafe"

	"github.com/vapstack/qx"
	"go.etcd.io/bbolt"
)

const snapshotRetryBudgetMult = 30

func snapshotPinWaitTimeout(v time.Duration) time.Duration {
	if v == 0 {
		return defaultOptionsSnapshotPinWaitTimeout
	}
	if v < 0 {
		return 0
	}
	return v
}

// Query evaluates the given query against the index and returns all matching values.
func (db *DB[K, V]) Query(q *qx.QX) ([]*V, error) {

	if q == nil {
		return nil, fmt.Errorf("QX is nil")
	}
	if len(q.Order) > 1 {
		return nil, fmt.Errorf("rbi does not support multi-column ordering")
	}
	if err := db.beginOp(); err != nil {
		return nil, err
	}
	defer db.endOp()

	if db.noIndex.Load() {
		return nil, ErrIndexDisabled
	}

	retryBudget := db.snapshot.pinWait * snapshotRetryBudgetMult
	deadline := time.Now().Add(retryBudget)

	lastTxID := uint64(0)
	for {
		tx, txErr := db.bolt.Begin(false)
		if txErr != nil {
			return nil, fmt.Errorf("tx error: %w", txErr)
		}

		txID := uint64(tx.ID())
		lastTxID = txID

		if snap, ok := db.pinByTxID(txID); ok && snap != nil {
			values, err := db.queryRecords(tx, snap, q)
			db.unpinByTxID(txID)
			_ = tx.Rollback()
			if err != nil {
				return nil, err
			}
			return values, nil
		}

		if db.isPending(txID) {
			if snap, ok := db.pinByTxIDWait(txID, db.snapshot.pinWait); ok && snap != nil {
				values, err := db.queryRecords(tx, snap, q)
				db.unpinByTxID(txID)
				_ = tx.Rollback()
				if err != nil {
					return nil, err
				}
				return values, nil
			}
		}

		latest := db.getSnapshot()
		if latest != nil && latest.txID <= txID {
			values, err := db.queryRecords(tx, latest, q)
			_ = tx.Rollback()
			if err != nil {
				return nil, err
			}
			return values, nil
		}

		if snap, floorTx, ok := db.pinFloorByTxID(txID); ok && snap != nil {
			values, err := db.queryRecords(tx, snap, q)
			db.unpinByTxID(floorTx)
			_ = tx.Rollback()
			if err != nil {
				return nil, err
			}
			return values, nil
		}

		_ = tx.Rollback()

		if db.closed.Load() {
			return nil, ErrClosed
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("index snapshot is not available for txID=%v", lastTxID)
		}
		runtime.Gosched()
	}
}

func (db *DB[K, V]) queryRecords(tx *bbolt.Tx, snap *indexSnapshot, q *qx.QX) ([]*V, error) {
	ids, err := db.queryNoTraceOnSnapshot(q, snap)
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

// QueryKeys evaluates the given query against the index and returns all matching ids.
func (db *DB[K, V]) QueryKeys(q *qx.QX) ([]K, error) {
	if q == nil {
		return nil, fmt.Errorf("QX is nil")
	}
	if len(q.Order) > 1 {
		return nil, fmt.Errorf("rbi does not support multi-column ordering")
	}
	if err := db.beginOp(); err != nil {
		return nil, err
	}
	defer db.endOp()

	if db.noIndex.Load() {
		return nil, ErrIndexDisabled
	}
	return db.queryOnSnapshot(q, db.getSnapshot())
}

func (db *DB[K, V]) query(q *qx.QX) ([]K, error) {
	return db.execQuery(q, true, false)
}

func (db *DB[K, V]) queryNoTrace(q *qx.QX) ([]K, error) {
	return db.execQuery(q, false, false)
}

// queryNoTracePrepared skips normalize/field-validation phase and is intended
// only for internal callers that already operate on validated/normalized QX.
func (db *DB[K, V]) queryNoTracePrepared(q *qx.QX) ([]K, error) {
	return db.execQuery(q, false, true)
}

func (db *DB[K, V]) queryNoTraceOnSnapshot(q *qx.QX, snap *indexSnapshot) ([]K, error) {
	view := db.makeQueryView(snap)
	defer db.releaseQueryView(view)
	return view.queryNoTrace(q)
}

func (db *DB[K, V]) queryOnSnapshot(q *qx.QX, snap *indexSnapshot) ([]K, error) {
	view := db.makeQueryView(snap)
	defer db.releaseQueryView(view)
	return view.query(q)
}

func shouldSkipPlannerInDeltaMode(q *qx.QX) bool {
	if q == nil || len(q.Order) != 1 {
		return false
	}
	switch q.Order[0].Type {
	case qx.OrderByArrayPos, qx.OrderByArrayCount:
		return true
	default:
		return false
	}
}

func (db *DB[K, V]) makeQueryView(snap *indexSnapshot) *DB[K, V] {
	root := db
	if db.traceRoot != nil {
		root = db.traceRoot
	}
	view := root.viewPool.Get().(*DB[K, V])
	*view = DB[K, V]{
		strkey:     root.strkey,
		strmap:     root.strmap,
		strmapView: snap.strmap,
		traceRoot:  root,
		fields:     root.fields,
		getters:    root.getters,
		options:    root.options,
	}
	view.snapshot.current.Store(snap)
	return view
}

func (db *DB[K, V]) releaseQueryView(view *DB[K, V]) {
	if view == nil {
		return
	}
	root := db
	if db.traceRoot != nil {
		root = db.traceRoot
	}
	*view = DB[K, V]{}
	root.viewPool.Put(view)
}

// execQuery runs the full query pipeline against one snapshot view.
//
// The method keeps all routing decisions in one place so fast-path/planner
// selection remains consistent across QueryKeys and Query callers.
func (db *DB[K, V]) execQuery(q *qx.QX, emitTrace bool, prepared bool) (out []K, err error) {

	// Normalization is intentionally skipped for pre-normalized internal calls
	// to avoid duplicate AST rewrites in hot paths.
	if !prepared {
		q = normalizeQuery(q)
	}

	var trace *queryTrace
	if emitTrace && db.traceOrCalibrationSamplingEnabled() {
		trace = db.beginTrace(q)
		if trace != nil {
			defer func() {
				trace.finish(uint64(len(out)), err)
			}()
		}
	}

	if !prepared {
		if err = db.checkUsedFields(q); err != nil {
			return nil, err
		}
	}

	var ok bool

	if out, ok, err = db.tryUniqueEqNoOrder(q, trace); ok {
		return out, err
	}

	// Planner/execution fast-paths are attempted before bitmap fallback because
	// they can short-circuit large scans when query shape matches known patterns.
	if !shouldSkipPlannerInDeltaMode(q) {
		if db.shouldPreferExecutionPlan(q, trace) {
			if out, ok, err = db.tryExecutionPlan(q, trace); ok {
				return out, err
			}
			if out, ok, err = db.tryPlan(q, trace); ok {
				return out, err
			}
		} else {
			if out, ok, err = db.tryPlan(q, trace); ok {
				return out, err
			}
			if out, ok, err = db.tryExecutionPlan(q, trace); ok {
				return out, err
			}
		}
	}

	if trace != nil {
		trace.setPlan(PlanBitmap)
	}

	result, err := db.evalExpr(q.Expr)
	if err != nil {
		return nil, err
	}
	if !result.neg {
		if result.bm == nil || result.bm.IsEmpty() {
			result.release()
			return nil, nil
		}
	} else {
		if db.snapshotUniverseCardinality() == 0 {
			result.release()
			return nil, nil
		}
	}
	defer func() { result.release() }()

	skip := q.Offset
	needAll := q.Limit == 0
	need := q.Limit

	// case 1: no ordering, negative result: iterate over universe excluding the result set
	if len(q.Order) == 0 && result.neg {
		out = make([]K, 0, func() uint64 {
			if needAll {
				return db.snapshotUniverseCardinality()
			}
			return need
		}())
		cursor := db.newQueryCursor(out, skip, need, needAll, nil)

		ex := result.bm
		universe, owned := db.snapshotUniverseView()
		if owned {
			defer releaseRoaringBuf(universe)
		}
		it := universe.Iterator()
		for it.HasNext() {
			idx := it.Next()
			if ex != nil && ex.Contains(idx) {
				continue
			}
			if cursor.emit(idx) {
				return cursor.out, nil
			}
		}
		return cursor.out, nil
	}

	// case 2: ordering with negative result:
	// materialize into a positive bitmap once, because order executors work on
	// concrete candidate sets and cannot stream complement sets directly.
	if len(q.Order) > 0 && result.neg {
		prev := result
		mat := getRoaringBuf()
		universe, owned := db.snapshotUniverseView()
		mat.Or(universe)
		if owned {
			releaseRoaringBuf(universe)
		}
		if prev.bm != nil {
			mat.AndNot(prev.bm)
		}
		result = bitmap{bm: mat}
		prev.release()
	}

	// case 3: ordering with positive result
	if len(q.Order) > 0 {
		order := q.Order[0]

		switch order.Type {

		case qx.OrderByArrayPos:
			ov := db.fieldOverlay(order.Field)
			if !ov.hasData() && !db.hasFieldIndex(order.Field) {
				return nil, fmt.Errorf("cannot sort non-indexed field: %v", order.Field)
			}
			if ov.delta != nil {
				return db.queryOrderArrayPosOverlay(result, ov, order, skip, need, needAll)
			}
			slice := db.snapshotFieldIndexSlice(order.Field)
			if slice == nil {
				return nil, fmt.Errorf("cannot sort non-indexed field: %v", order.Field)
			}
			return db.queryOrderArrayPos(result, slice, order, skip, need, needAll)

		case qx.OrderByArrayCount:
			lenOV := db.lenFieldOverlay(order.Field)
			useZeroComplement := db.isLenZeroComplementField(order.Field)
			if !lenOV.hasData() && !db.hasLenFieldIndex(order.Field) {
				return nil, fmt.Errorf("cannot sort non-indexed field: %v", order.Field)
			}
			if lenOV.delta != nil {
				return db.queryOrderArrayCountOverlay(result, lenOV, order, skip, need, needAll, useZeroComplement)
			}
			slice := db.snapshotLenFieldIndexSlice(order.Field)
			if slice == nil {
				return nil, fmt.Errorf("cannot sort non-indexed field: %v", order.Field)
			}
			return db.queryOrderArrayCount(result, *slice, order, skip, need, needAll, useZeroComplement)
		}

		ov := db.fieldOverlay(order.Field)
		if !ov.hasData() && !db.hasFieldIndex(order.Field) {
			return nil, fmt.Errorf("cannot sort non-indexed field: %v", order.Field)
		}
		if ov.delta == nil && need > 0 && need <= 256 {
			slice := db.snapshotFieldIndexSlice(order.Field)
			if slice == nil {
				return nil, fmt.Errorf("cannot sort non-indexed field: %v", order.Field)
			}
			return db.queryOrderBasicSlice(result, slice, order, skip, need, needAll)
		}
		return db.queryOrderBasic(result, ov, order, skip, need, needAll)
	}

	// case 4: no ordering, positive result:
	// for numeric keys and unbounded result, return a zero-copy reinterpretation
	// to avoid an extra allocation/copy in the hottest read path.

	// Fast-path only when unbounded query also has no offset.
	// Offset requires cursor-based pagination logic.
	if !db.strkey && needAll && skip == 0 {
		ids := result.bm.ToArray()
		return unsafe.Slice((*K)(unsafe.Pointer(&ids[0])), len(ids)), nil
	}

	out = makeOutSlice[K](result.bm, need)
	cursor := db.newQueryCursor(out, skip, need, needAll, nil)

	iter := result.bm.Iterator()
	for iter.HasNext() {
		if cursor.emit(iter.Next()) {
			return cursor.out, nil
		}
	}
	return cursor.out, nil
}
