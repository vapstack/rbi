package rbi

import (
	"fmt"
	"strings"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/vapstack/qx"
)

// we definitely need a normal query planner...

func (db *DB[K, V]) tryFastPath(q *qx.QX) ([]K, bool, error) {

	// optimized path for LIMIT without OFFSET
	if out, ok, err := db.tryLimitQuery(q); ok {
		return out, ok, err
	}

	// optimization for simple ORDER + LIMIT without complex filters (and OFFSET)
	if out, ok, err := db.tryQueryOrderBasicWithLimit(q); ok {
		return out, ok, err
	}

	//
	// these old paths can be removed, but more benchmarks is needed (with OFFSET)
	//

	// optimization for PREFIX + ORDER + LIMIT without complex filters
	if out, ok, err := db.tryQueryOrderPrefixWithLimit(q); ok {
		return out, ok, err
	}

	// optimization for simple PREFIX + LIMIT without ORDER
	if out, ok, err := db.tryQueryPrefixNoOrderWithLimit(q); ok {
		return out, ok, err
	}

	// optimization for simple range + LIMIT without ORDER
	if out, ok, err := db.tryQueryRangeNoOrderWithLimit(q); ok {
		return out, ok, err
	}

	return nil, false, nil
}

type rangeBounds struct {
	has bool

	hasLo bool
	loKey string
	loInc bool

	hasHi bool
	hiKey string
	hiInc bool

	hasPrefix bool
	prefix    string
}

func (rb *rangeBounds) applyLo(key string, inc bool) {
	if !rb.hasLo || rb.loKey < key || (rb.loKey == key && rb.loInc == false && inc == true) {
		rb.hasLo = true
		rb.loKey = key
		rb.loInc = inc
	}
}

func (rb *rangeBounds) applyHi(key string, inc bool) {
	if !rb.hasHi || rb.hiKey > key || (rb.hiKey == key && rb.hiInc == false && inc == true) {
		rb.hasHi = true
		rb.hiKey = key
		rb.hiInc = inc
	}
}

func lowerBoundIndex(s []index, key string) int {
	lo, hi := 0, len(s)
	for lo < hi {
		mid := (lo + hi) >> 1
		if s[mid].Key < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func upperBoundIndex(s []index, key string) int {
	lo, hi := 0, len(s)
	for lo < hi {
		mid := (lo + hi) >> 1
		if s[mid].Key <= key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func (db *DB[K, V]) tryQueryOrderBasicWithLimit(q *qx.QX) ([]K, bool, error) {

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

	if q.Expr.Op != qx.OpAND {
		ops = []qx.Expr{q.Expr}
	}

	f := order.Field
	fm := db.fields[f]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}

	slice := db.index[f]
	if slice == nil {
		return nil, false, nil
	}
	s := *slice
	if len(s) == 0 {
		return nil, true, nil
	}

	var rb rangeBounds

	baseOps := make([]qx.Expr, 0, len(ops))

	for _, op := range ops {
		if op.Not {
			return nil, false, nil
		}
		if op.Field == f {
			switch op.Op {
			case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpEQ:
				vals, isSlice, err := db.exprValueToIdx(op)
				if err != nil {
					return nil, true, err
				}
				if isSlice || len(vals) != 1 {
					return nil, false, nil
				}
				k := vals[0]
				switch op.Op {
				case qx.OpGT:
					rb.applyLo(k, false)
				case qx.OpGTE:
					rb.applyLo(k, true)
				case qx.OpLT:
					rb.applyHi(k, false)
				case qx.OpLTE:
					rb.applyHi(k, true)
				case qx.OpEQ:
					rb.applyLo(k, true)
					rb.applyHi(k, true)
				}
				continue
			default:
				return nil, false, nil
			}
		}
		baseOps = append(baseOps, op)
	}

	var base bitmap

	if len(baseOps) == 0 {
		bm := getRoaringBuf()
		bm.Or(db.universe)
		base = bitmap{bm: bm}

	} else if len(baseOps) == 1 {

		b, err := db.evalExpr(baseOps[0])
		if err != nil {
			return nil, true, err
		}
		if b.bm == nil || b.bm.IsEmpty() {
			b.release()
			return nil, true, nil
		}
		base = b

	} else {

		b, err := db.evalExpr(qx.Expr{Op: qx.OpAND, Operands: baseOps})
		if err != nil {
			return nil, true, err
		}
		if b.bm == nil || b.bm.IsEmpty() {
			b.release()
			return nil, true, nil
		}
		base = b

	}
	defer base.release()

	start := 0
	end := len(s)

	if rb.hasLo {
		start = lowerBoundIndex(s, rb.loKey)
		if !rb.loInc {
			if start < len(s) && s[start].Key == rb.loKey {
				start++
			}
		}
	}
	if rb.hasHi {
		if rb.hiInc {
			end = upperBoundIndex(s, rb.hiKey)
		} else {
			end = lowerBoundIndex(s, rb.hiKey)
		}
	}
	if start < 0 {
		start = 0
	}
	if end > len(s) {
		end = len(s)
	}
	if start >= end {
		return nil, true, nil
	}

	skip := q.Offset
	need := q.Limit

	out := make([]K, 0, need)

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	tmp := getRoaringBuf()
	defer releaseRoaringBuf(tmp)

	emit := func(bm *roaring64.Bitmap) bool {
		iter := bm.Iterator()
		for iter.HasNext() {
			idx := iter.Next()
			if skip > 0 {
				skip--
				continue
			}
			out = append(out, db.idFromIdxNoLock(idx))
			need--
			if need == 0 {
				return true
			}
		}
		return false
	}

	contains := func(idx uint64) bool {
		if base.neg {
			if base.bm == nil || base.bm.IsEmpty() {
				return true // NOT empty == universe
			}
			return !base.bm.Contains(idx)
		}
		if base.bm == nil || base.bm.IsEmpty() {
			return false
		}
		return base.bm.Contains(idx)
	}

	if !order.Desc {
		for i := start; i < end; i++ {
			// fast path check
			if card := s[i].IDs.GetCardinality(); card <= iteratorThreshold || (0 < need && need < 1000) {
				if card == 0 {
					continue
				}
				// manual iteration
				it := s[i].IDs.Iterator()
				for it.HasNext() {
					idx := it.Next()
					if !contains(idx) {
						continue
					}
					if skip > 0 {
						skip--
						continue
					}
					out = append(out, db.idFromIdxNoLock(idx))
					need--
					if need == 0 {
						return out, true, nil
					}
				}
				continue
			}

			// slow path
			// tmp.Clear()
			// tmp.Or(s[i].IDs)
			// tmp.And(base.bm)
			// if tmp.IsEmpty() {
			// 	continue
			// }

			// tmp.Clear()
			// tmp.Xor(tmp)
			// tmp.Or(s[i].IDs)

			if base.neg {
				tmp.Xor(tmp)
				tmp.Or(s[i].IDs)
				if base.bm != nil && !base.bm.IsEmpty() {
					tmp.AndNot(base.bm)
				}
			} else {
				if base.bm == nil || base.bm.IsEmpty() {
					continue
				}
				tmpORSmallestAND(tmp, s[i].IDs, base.bm)
				// tmp.And(base.bm)
			}
			if tmp.IsEmpty() {
				continue
			}
			if emit(tmp) {
				return out, true, nil
			}
		}
	} else {
		for i := end - 1; i >= start; i-- {
			if card := s[i].IDs.GetCardinality(); card <= iteratorThreshold || (0 < need && need < 1000) {
				if card == 0 {
					continue
				}
				iter := s[i].IDs.Iterator()
				for iter.HasNext() {
					idx := iter.Next()
					if !contains(idx) {
						continue
					}
					if skip > 0 {
						skip--
						continue
					}
					out = append(out, db.idFromIdxNoLock(idx))
					need--
					if need == 0 {
						return out, true, nil
					}
				}
				continue
			}

			// tmp.Clear()
			// tmp.Or(s[i].IDs)
			// tmp.And(base.bm)
			// if tmp.IsEmpty() {
			// 	continue
			// }

			// tmp.Clear()
			// tmp.Xor(tmp)
			// tmp.Or(s[i].IDs)

			if base.neg {
				tmp.Xor(tmp)
				tmp.Or(s[i].IDs)
				if base.bm != nil && !base.bm.IsEmpty() {
					tmp.AndNot(base.bm)
				}
			} else {
				if base.bm == nil || base.bm.IsEmpty() {
					continue
				}
				tmpORSmallestAND(tmp, s[i].IDs, base.bm)
				// tmp.And(base.bm)
			}
			if tmp.IsEmpty() {
				continue
			}
			if emit(tmp) {
				return out, true, nil
			}
			if i == start {
				break
			}
		}
	}

	return out, true, nil
}

func (db *DB[K, V]) tryQueryOrderPrefixWithLimit(q *qx.QX) ([]K, bool, error) {

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
	fm := db.fields[f]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}

	slice := db.index[f]
	if slice == nil {
		return nil, false, nil
	}
	s := *slice
	if len(s) == 0 {
		return nil, true, nil
	}

	expr := q.Expr
	ops := expr.Operands
	if expr.Op != qx.OpAND {
		ops = []qx.Expr{expr}
	}

	var (
		hasPrefix bool
		prefix    string
		baseOps   = make([]qx.Expr, 0, len(ops))
	)

	for _, op := range ops {
		if op.Not {
			return nil, false, nil
		}
		if op.Field == f && op.Op == qx.OpPREFIX {
			vals, isSlice, err := db.exprValueToIdx(op)
			if err != nil {
				return nil, true, err
			}
			if isSlice || len(vals) != 1 {
				return nil, false, nil
			}
			hasPrefix = true
			prefix = vals[0]
			continue
		}
		baseOps = append(baseOps, op)
	}

	if !hasPrefix {
		return nil, false, nil
	}

	var base bitmap

	if len(baseOps) == 0 {
		bm := getRoaringBuf()
		bm.Or(db.universe)
		base = bitmap{bm: bm}

	} else if len(baseOps) == 1 {
		b, err := db.evalExpr(baseOps[0])
		if err != nil {
			return nil, true, err
		}
		if b.bm == nil || b.bm.IsEmpty() {
			b.release()
			return nil, true, nil
		}
		base = b

	} else {
		b, err := db.evalExpr(qx.Expr{Op: qx.OpAND, Operands: baseOps})
		if err != nil {
			return nil, true, err
		}
		if b.bm == nil || b.bm.IsEmpty() {
			b.release()
			return nil, true, nil
		}
		base = b
	}
	defer base.release()

	start := lowerBoundIndex(s, prefix)
	end := start
	for end < len(s) {
		if !strings.HasPrefix(s[end].Key, prefix) {
			break
		}
		end++
	}
	if start >= end {
		return nil, true, nil
	}

	skip := q.Offset
	need := q.Limit
	out := make([]K, 0, need)

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	contains := func(idx uint64) bool {
		if base.neg {
			if base.bm == nil || base.bm.IsEmpty() {
				return true // NOT empty == universe
			}
			return !base.bm.Contains(idx)
		}
		if base.bm == nil || base.bm.IsEmpty() {
			return false
		}
		return base.bm.Contains(idx)
	}

	if !ord.Desc {

		for i := start; i < end; i++ {
			iter := s[i].IDs.Iterator()
			for iter.HasNext() {
				idx := iter.Next()
				if !contains(idx) {
					continue
				}
				if skip > 0 {
					skip--
					continue
				}
				out = append(out, db.idFromIdxNoLock(idx))
				need--
				if need == 0 {
					return out, true, nil
				}
			}
		}

	} else {

		for i := end - 1; i >= start; i-- {
			iter := s[i].IDs.Iterator()
			for iter.HasNext() {
				idx := iter.Next()
				if !contains(idx) {
					continue
				}
				if skip > 0 {
					skip--
					continue
				}
				out = append(out, db.idFromIdxNoLock(idx))
				need--
				if need == 0 {
					return out, true, nil
				}
			}
			if i == start {
				break
			}
		}
	}

	return out, true, nil
}

func (db *DB[K, V]) tryQueryRangeNoOrderWithLimit(q *qx.QX) ([]K, bool, error) {

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

	switch e.Op {
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpEQ:
	default:
		return nil, false, nil
	}

	f := e.Field
	if f == "" {
		return nil, false, nil
	}

	fm := db.fields[f]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}

	slice := db.index[f]
	if slice == nil {
		return nil, false, nil
	}
	s := *slice
	if len(s) == 0 {
		return nil, true, nil
	}

	vals, isSlice, err := db.exprValueToIdx(e)
	if err != nil {
		return nil, true, err
	}
	if isSlice || len(vals) != 1 {
		return nil, false, nil
	}
	key := vals[0]

	start := 0
	end := len(s)

	lo := lowerBoundIndex(s, key)

	switch e.Op {
	case qx.OpGT:
		start = lo
		if start < len(s) && s[start].Key == key {
			start++
		}
	case qx.OpGTE:
		start = lo
	case qx.OpLT:
		end = lo
	case qx.OpLTE:
		end = upperBoundIndex(s, key)
	case qx.OpEQ:
		start = lo
		if start >= len(s) || s[start].Key != key {
			return nil, true, nil
		}
		end = start + 1
	}

	if start < 0 {
		start = 0
	}
	if end > len(s) {
		end = len(s)
	}
	if start >= end {
		return nil, true, nil
	}

	skip := q.Offset
	need := q.Limit

	out := make([]K, 0, need)

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	for i := start; i < end; i++ {
		it := s[i].IDs.Iterator()
		for it.HasNext() {
			idx := it.Next()
			if skip > 0 {
				skip--
				continue
			}
			out = append(out, db.idFromIdxNoLock(idx))
			need--
			if need == 0 {
				return out, true, nil
			}
		}
	}

	return out, true, nil
}

func (db *DB[K, V]) tryQueryPrefixNoOrderWithLimit(q *qx.QX) ([]K, bool, error) {

	if len(q.Order) > 0 || q.Limit == 0 {
		return nil, false, nil
	}

	if q.Expr.Not {
		return nil, false, nil
	}

	e := q.Expr
	if e.Op != qx.OpPREFIX || e.Field == "" {
		return nil, false, nil
	}
	if len(e.Operands) != 0 {
		return nil, false, nil
	}

	fm := db.fields[e.Field]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}

	vals, isSlice, err := db.exprValueToIdx(e)
	if err != nil {
		return nil, true, err
	}
	if isSlice || len(vals) != 1 {
		return nil, false, nil
	}
	prefix := vals[0]

	slice := db.index[e.Field]
	if slice == nil {
		return nil, true, fmt.Errorf("no index for field: %v", e.Field)
	}
	s := *slice
	if len(s) == 0 {
		return nil, true, nil
	}

	start := lowerBoundIndex(s, prefix)
	end := start
	for end < len(s) {
		if !strings.HasPrefix(s[end].Key, prefix) {
			break
		}
		end++
	}
	if start >= end {
		return nil, true, nil
	}

	skip := q.Offset
	need := q.Limit
	out := make([]K, 0, need)

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	for i := start; i < end; i++ {
		it := s[i].IDs.Iterator()
		for it.HasNext() {
			idx := it.Next()
			if skip > 0 {
				skip--
				continue
			}
			out = append(out, db.idFromIdxNoLock(idx))
			need--
			if need == 0 {
				return out, true, nil
			}
		}
	}

	return out, true, nil
}
