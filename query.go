package rbi

import (
	"fmt"
	"sync"
	"unsafe"

	"github.com/vapstack/qx"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

// QueryItems evaluates the given query against the index and returns all matching values.
func (db *DB[K, V]) QueryItems(q *qx.QX) ([]*V, error) {

	if q == nil {
		return nil, fmt.Errorf("QL is nil")
	}
	if len(q.Order) > 1 {
		return nil, fmt.Errorf("rbi does not support multi-column ordering")
	}

	db.mu.RLock()

	if db.closed.Load() {
		db.mu.RUnlock()
		return nil, ErrClosed
	}
	if db.noIndex.Load() {
		db.mu.RUnlock()
		return nil, ErrIndexDisabled
	}

	ids, err := db.query(q)
	if err != nil {
		db.mu.RUnlock()
		return nil, err
	}
	if len(ids) == 0 {
		db.mu.RUnlock()
		return nil, nil
	}

	// start a read tx while index read-lock is still held so ids and values
	// are read from the same logical snapshot, then release db.mu early
	tx, txErr := db.bolt.Begin(false)
	db.mu.RUnlock()
	if txErr != nil {
		return nil, fmt.Errorf("tx error: %w", txErr)
	}
	defer rollback(tx)

	values, err := db.getManyTx(tx, ids...)
	if err != nil {
		return nil, err
	}

	return values, nil
}

// QueryKeys evaluates the given query against the index and returns all matching ids.
func (db *DB[K, V]) QueryKeys(q *qx.QX) ([]K, error) {
	if q == nil {
		return nil, fmt.Errorf("QL is nil")
	}
	if len(q.Order) > 1 {
		return nil, fmt.Errorf("rbi does not support multi-column ordering")
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed.Load() {
		return nil, ErrClosed
	}
	if db.noIndex.Load() {
		return nil, ErrIndexDisabled
	}

	return db.query(q)
}

func (db *DB[K, V]) query(q *qx.QX) ([]K, error) {
	return db.queryInternal(q, true)
}

func (db *DB[K, V]) queryNoTrace(q *qx.QX) ([]K, error) {
	return db.queryInternal(q, false)
}

func (db *DB[K, V]) queryInternal(q *qx.QX, emitTrace bool) (out []K, err error) {

	q = normalizeQuery(q)

	var trace *queryTrace
	if emitTrace {
		trace = db.beginTrace(q)
		if trace != nil {
			defer func() {
				trace.finish(uint64(len(out)), err)
			}()
		}
	}

	if err = db.checkUsedFields(q); err != nil {
		return nil, err
	}

	var ok bool

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
		if db.universe.IsEmpty() {
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
		if db.strkey {
			db.strmap.RLock()
			defer db.strmap.RUnlock()
		}

		out = make([]K, 0, func() uint64 {
			if needAll {
				return db.universe.GetCardinality()
			}
			return need
		}())
		cursor := db.newQueryCursor(out, skip, need, needAll, nil)

		ex := result.bm
		it := db.universe.Iterator()
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
	// materialize the negative set into a positive one (universe AND NOT result)
	if len(q.Order) > 0 && result.neg {
		prev := result
		mat := getRoaringBuf()
		mat.Or(db.universe)
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
			slice := db.index[order.Field]
			if slice == nil {
				return nil, fmt.Errorf("cannot sort non-indexed field: %v", order.Field)
			}
			return db.queryOrderArrayPos(result, slice, order, skip, need, needAll)

		case qx.OrderByArrayCount:
			slice := db.lenIndex[order.Field]
			if slice == nil {
				return nil, fmt.Errorf("cannot sort non-indexed field: %v", order.Field)
			}
			return db.queryOrderArrayCount(result, *slice, order, skip, need, needAll)

		default:
		}

		slice := db.index[order.Field]
		if slice == nil {
			return nil, fmt.Errorf("cannot sort non-indexed field: %v", order.Field)
		}

		return db.queryOrderBasic(result, slice, order, skip, need, needAll)
	}

	// case 4: no ordering, positive result:
	// simply return the IDs from the result bitmap

	if !db.strkey && needAll {
		ids := result.bm.ToArray()
		return unsafe.Slice((*K)(unsafe.Pointer(&ids[0])), len(ids)), nil
	}

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
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

// Count evaluates the expression from the given query and returns the number of matching records.
// It ignores Order, Offset and Limit fields.
// If q is nil, Count returns the total number of keys currently present in the database.
func (db *DB[K, V]) Count(q *qx.QX) (uint64, error) {
	if q == nil {
		db.mu.RLock()
		defer db.mu.RUnlock()
		if db.closed.Load() {
			return 0, ErrClosed
		}
		if db.noIndex.Load() {
			return 0, ErrIndexDisabled
		}
		return db.universe.GetCardinality(), nil
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed.Load() {
		return 0, ErrClosed
	}
	if db.noIndex.Load() {
		return 0, ErrIndexDisabled
	}

	if err := db.checkUsedFields(q); err != nil {
		return 0, err
	}

	expr, _ := normalizeExpr(q.Expr)

	b, err := db.evalExpr(expr)
	if err != nil {
		return 0, err
	}
	defer b.release()

	if b.neg {
		if b.bm == nil {
			return db.universe.GetCardinality(), nil
		}
		ex := b.bm.GetCardinality()
		uc := db.universe.GetCardinality()
		if ex >= uc {
			return 0, nil
		}
		return uc - ex, nil
	}
	if b.bm == nil {
		return 0, nil
	}
	return b.bm.GetCardinality(), nil
}

var roaringPool = sync.Pool{
	New: func() any { return roaring64.New() },
}

func getRoaringBuf() *roaring64.Bitmap {
	return roaringPool.Get().(*roaring64.Bitmap)
}

func releaseRoaringBuf(bm *roaring64.Bitmap) {
	bm.Clear()
	roaringPool.Put(bm)
}
