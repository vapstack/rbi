package rbi

import (
	"fmt"

	"github.com/vapstack/qx"
)

func (db *DB[K, V]) tryExecutionPlan(q *qx.QX, trace *queryTrace) ([]K, bool, error) {

	// optimized path for LIMIT without OFFSET
	if out, ok, plan, err := db.tryLimitQuery(q); ok {
		if trace != nil {
			trace.setPlan(plan)
		}
		return out, ok, err
	}

	// optimization for simple ORDER + LIMIT without complex filters (and OFFSET)
	if out, ok, err := db.tryQueryOrderBasicWithLimit(q); ok {
		if trace != nil {
			trace.setPlan(PlanLimitOrderBasic)
		}
		return out, ok, err
	}

	// optimization for PREFIX + ORDER + LIMIT without complex filters
	if out, ok, err := db.tryQueryOrderPrefixWithLimit(q); ok {
		if trace != nil {
			trace.setPlan(PlanLimitOrderPrefix)
		}
		return out, ok, err
	}

	// optimization for simple PREFIX + LIMIT without ORDER
	if out, ok, err := db.tryQueryPrefixNoOrderWithLimit(q); ok {
		if trace != nil {
			trace.setPlan(PlanLimitPrefixNoOrder)
		}
		return out, ok, err
	}

	// optimization for simple range + LIMIT without ORDER
	if out, ok, err := db.tryQueryRangeNoOrderWithLimit(q); ok {
		if trace != nil {
			trace.setPlan(PlanLimitRangeNoOrder)
		}
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

func nextPrefixUpperBound(prefix string) (string, bool) {
	if prefix == "" {
		return "", false
	}
	buf := []byte(prefix)
	for i := len(buf) - 1; i >= 0; i-- {
		if buf[i] == 0xFF {
			continue
		}
		buf[i]++
		return string(buf[:i+1]), true
	}
	return "", false
}

func prefixRangeEndIndex(s []index, prefix string, start int) int {
	if start < 0 || start >= len(s) {
		return start
	}
	if s[start].Key < prefix || len(s[start].Key) < len(prefix) || s[start].Key[:len(prefix)] != prefix {
		return start
	}
	if upper, ok := nextPrefixUpperBound(prefix); ok {
		end := lowerBoundIndex(s, upper)
		if end < start {
			end = start
		}
		return end
	}

	// Extremely rare fallback when prefix consists entirely of 0xFF bytes.
	end := start
	for end < len(s) {
		key := s[end].Key
		if len(key) < len(prefix) || key[:len(prefix)] != prefix {
			break
		}
		end++
	}
	return end
}

const iteratorThreshold = 2048 // 256

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
		// Prefix on a non-ordered field is typically expensive with this plan:
		// it forces broad ordered-field traversal plus per-id contains checks.
		// Let planner paths handle this shape instead.
		if op.Op == qx.OpPREFIX && op.Field != f {
			return nil, false, nil
		}
		if op.Field == f {
			switch op.Op {
			case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpEQ:
				k, isSlice, err := db.exprValueToIdxScalar(op)
				if err != nil {
					return nil, true, err
				}
				if isSlice {
					return nil, false, nil
				}
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
	cursor := db.newQueryCursor(out, skip, need, false, nil)

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	tmp := getRoaringBuf()
	defer releaseRoaringBuf(tmp)

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
			if card := s[i].IDs.GetCardinality(); card <= iteratorThreshold || (0 < cursor.need && cursor.need < 1000) {
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
					if cursor.emit(idx) {
						return cursor.out, true, nil
					}
				}
				continue
			}

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
			}
			if tmp.IsEmpty() {
				continue
			}
			if cursor.emitBitmap(tmp) {
				return cursor.out, true, nil
			}
		}
	} else {
		for i := end - 1; i >= start; i-- {
			if card := s[i].IDs.GetCardinality(); card <= iteratorThreshold || (0 < cursor.need && cursor.need < 1000) {
				if card == 0 {
					continue
				}
				iter := s[i].IDs.Iterator()
				for iter.HasNext() {
					idx := iter.Next()
					if !contains(idx) {
						continue
					}
					if cursor.emit(idx) {
						return cursor.out, true, nil
					}
				}
				continue
			}

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
			}
			if tmp.IsEmpty() {
				continue
			}
			if cursor.emitBitmap(tmp) {
				return cursor.out, true, nil
			}
			if i == start {
				break
			}
		}
	}

	return cursor.out, true, nil
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
			p, isSlice, err := db.exprValueToIdxScalar(op)
			if err != nil {
				return nil, true, err
			}
			if isSlice {
				return nil, false, nil
			}
			hasPrefix = true
			prefix = p
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
	end := prefixRangeEndIndex(s, prefix, start)
	if start >= end {
		return nil, true, nil
	}

	skip := q.Offset
	need := q.Limit
	out := make([]K, 0, need)
	cursor := db.newQueryCursor(out, skip, need, false, nil)

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
				if cursor.emit(idx) {
					return cursor.out, true, nil
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
				if cursor.emit(idx) {
					return cursor.out, true, nil
				}
			}
			if i == start {
				break
			}
		}
	}

	return cursor.out, true, nil
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

	key, isSlice, err := db.exprValueToIdxScalar(e)
	if err != nil {
		return nil, true, err
	}
	if isSlice {
		return nil, false, nil
	}

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
	cursor := db.newQueryCursor(out, skip, need, false, nil)

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	for i := start; i < end; i++ {
		it := s[i].IDs.Iterator()
		for it.HasNext() {
			if cursor.emit(it.Next()) {
				return cursor.out, true, nil
			}
		}
	}

	return cursor.out, true, nil
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

	prefix, isSlice, err := db.exprValueToIdxScalar(e)
	if err != nil {
		return nil, true, err
	}
	if isSlice {
		return nil, false, nil
	}

	slice := db.index[e.Field]
	if slice == nil {
		return nil, true, fmt.Errorf("no index for field: %v", e.Field)
	}
	s := *slice
	if len(s) == 0 {
		return nil, true, nil
	}

	start := lowerBoundIndex(s, prefix)
	end := prefixRangeEndIndex(s, prefix, start)
	if start >= end {
		return nil, true, nil
	}

	skip := q.Offset
	need := q.Limit
	out := make([]K, 0, need)
	cursor := db.newQueryCursor(out, skip, need, false, nil)

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	for i := start; i < end; i++ {
		it := s[i].IDs.Iterator()
		for it.HasNext() {
			if cursor.emit(it.Next()) {
				return cursor.out, true, nil
			}
		}
	}

	return cursor.out, true, nil
}
