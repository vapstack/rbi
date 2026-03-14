package rbi

import (
	"fmt"
	"unsafe"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/vapstack/qx"
)

func (db *DB[K, V]) tryExecutionPlan(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	// Execution-plan fast paths support only single-column basic order.
	// Non-basic order types must stay on planner/bitmap routes.
	if len(q.Order) > 0 {
		if len(q.Order) != 1 || q.Order[0].Type != qx.OrderBasic {
			return nil, false, nil
		}
	}

	// optimized path for LIMIT without OFFSET
	if out, ok, plan, err := db.tryLimitQuery(q, trace); ok {
		if trace != nil {
			trace.setPlan(plan)
		}
		return out, ok, err
	}

	// optimization for simple ORDER + LIMIT without complex filters (and OFFSET)
	if out, ok, err := db.tryQueryOrderBasicWithLimit(q, trace); ok {
		if trace != nil {
			trace.setPlan(PlanLimitOrderBasic)
		}
		return out, ok, err
	}

	// optimization for PREFIX + ORDER + LIMIT without complex filters
	if out, ok, err := db.tryQueryOrderPrefixWithLimit(q, trace); ok {
		if trace != nil {
			trace.setPlan(PlanLimitOrderPrefix)
		}
		return out, ok, err
	}

	// optimization for simple PREFIX + LIMIT without ORDER
	if out, ok, err := db.tryQueryPrefixNoOrderWithLimit(q, trace); ok {
		if trace != nil {
			trace.setPlan(PlanLimitPrefixNoOrder)
		}
		return out, ok, err
	}

	// optimization for simple range + LIMIT without ORDER
	if out, ok, err := db.tryQueryRangeNoOrderWithLimit(q, trace); ok {
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
		if compareIndexKeyString(s[mid].Key, key) < 0 {
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
		if compareIndexKeyString(s[mid].Key, key) <= 0 {
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
		result := buf[:i+1]
		return unsafe.String(unsafe.SliceData(result), len(result)), true
	}
	return "", false
}

func prefixRangeEndIndex(s []index, prefix string, start int) int {
	if start < 0 || start >= len(s) {
		return start
	}
	if compareIndexKeyString(s[start].Key, prefix) < 0 || !indexKeyHasPrefixString(s[start].Key, prefix) {
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
		if !indexKeyHasPrefixString(s[end].Key, prefix) {
			break
		}
		end++
	}
	return end
}

const iteratorThreshold = 2048 // 256

func (db *DB[K, V]) tryQueryOrderBasicWithLimit(q *qx.QX, trace *queryTrace) ([]K, bool, error) {

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
		var single [1]qx.Expr
		single[0] = q.Expr
		ops = single[:]
	}

	f := order.Field
	fm := db.fields[f]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}

	ov := db.fieldOverlay(f)
	if !ov.hasData() {
		if !db.hasFieldIndex(f) {
			return nil, false, nil
		}
		return nil, true, nil
	}

	var rb rangeBounds

	var baseOps []qx.Expr
	var baseOpsStack [8]qx.Expr
	if len(ops) <= len(baseOpsStack) {
		baseOps = baseOpsStack[:0]
	} else {
		baseOps = make([]qx.Expr, 0, len(ops))
	}

	for _, op := range ops {
		if op.Not {
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
		universe, owned := db.snapshotUniverseView()
		bm.Or(universe)
		if owned {
			releaseRoaringBuf(universe)
		}
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

	baseBM := base.bm
	baseNegUniverse := base.neg && (baseBM == nil || baseBM.IsEmpty())
	if !base.neg && (baseBM == nil || baseBM.IsEmpty()) {
		return nil, true, nil
	}

	br := ov.rangeForBounds(rb)
	if br.baseStart >= br.baseEnd && br.deltaStart >= br.deltaEnd {
		return nil, true, nil
	}

	skip := q.Offset
	need := q.Limit

	out := make([]K, 0, need)
	cursor := db.newQueryCursor(out, skip, need, false, nil)

	tmp := getRoaringBuf()
	defer releaseRoaringBuf(tmp)
	var scratch *roaring64.Bitmap
	if ov.delta != nil {
		scratch = getRoaringBuf()
		defer releaseRoaringBuf(scratch)
	}

	contains := func(idx uint64) bool {
		if base.neg {
			if baseNegUniverse {
				return true // NOT empty == universe
			}
			return !baseBM.Contains(idx)
		}
		return baseBM.Contains(idx)
	}

	keyCur := ov.newCursor(br, order.Desc)
	var (
		examined  uint64
		scanWidth uint64
	)
	if ov.delta == nil {
		for {
			_, ids, _, ok := keyCur.next()
			if !ok {
				break
			}
			if ids.IsEmpty() {
				continue
			}
			scanWidth++

			if card := ids.Cardinality(); card <= iteratorThreshold || (0 < cursor.need && cursor.need < 1000) {
				if ids.isSingleton() {
					idx := ids.single
					examined++
					if contains(idx) && cursor.emit(idx) {
						trace.addExamined(examined)
						trace.addOrderScanWidth(scanWidth)
						trace.setEarlyStopReason("limit_reached")
						return cursor.out, true, nil
					}
				} else {
					it := ids.bitmap().Iterator()
					for it.HasNext() {
						idx := it.Next()
						examined++
						if !contains(idx) {
							continue
						}
						if cursor.emit(idx) {
							trace.addExamined(examined)
							trace.addOrderScanWidth(scanWidth)
							trace.setEarlyStopReason("limit_reached")
							return cursor.out, true, nil
						}
					}
				}
				continue
			}

			examined += ids.Cardinality()
			if base.neg {
				tmp.Xor(tmp)
				ids.OrInto(tmp)
				if !baseNegUniverse {
					tmp.AndNot(baseBM)
				}
			} else {
				tmpORSmallestANDPosting(tmp, ids, baseBM)
			}
			if tmp.IsEmpty() {
				continue
			}
			if cursor.emitBitmap(tmp) {
				trace.addExamined(examined)
				trace.addOrderScanWidth(scanWidth)
				trace.setEarlyStopReason("limit_reached")
				return cursor.out, true, nil
			}
		}
		trace.addExamined(examined)
		trace.addOrderScanWidth(scanWidth)
		trace.setEarlyStopReason("input_exhausted")
		return cursor.out, true, nil
	}

	for {
		_, bucketIDs, de, ok := keyCur.next()
		if !ok {
			break
		}
		bm, owned := composePostingOwned(bucketIDs, de, scratch)
		if bm == nil || bm.IsEmpty() {
			if owned && bm != nil && bm != scratch {
				releaseRoaringBuf(bm)
			}
			continue
		}
		scanWidth++

		if card := bm.GetCardinality(); card <= iteratorThreshold || (0 < cursor.need && cursor.need < 1000) {
			if card == 1 {
				idx := bm.Minimum()
				examined++
				if contains(idx) && cursor.emit(idx) {
					if owned && bm != scratch {
						releaseRoaringBuf(bm)
					}
					trace.addExamined(examined)
					trace.addOrderScanWidth(scanWidth)
					trace.setEarlyStopReason("limit_reached")
					return cursor.out, true, nil
				}
			} else {
				it := bm.Iterator()
				for it.HasNext() {
					idx := it.Next()
					examined++
					if !contains(idx) {
						continue
					}
					if cursor.emit(idx) {
						if owned && bm != scratch {
							releaseRoaringBuf(bm)
						}
						trace.addExamined(examined)
						trace.addOrderScanWidth(scanWidth)
						trace.setEarlyStopReason("limit_reached")
						return cursor.out, true, nil
					}
				}
			}
			if owned && bm != scratch {
				releaseRoaringBuf(bm)
			}
			continue
		}

		examined += bm.GetCardinality()
		if base.neg {
			tmp.Xor(tmp)
			tmp.Or(bm)
			if !baseNegUniverse {
				tmp.AndNot(baseBM)
			}
		} else {
			tmpORSmallestAND(tmp, bm, baseBM)
		}
		if owned && bm != scratch {
			releaseRoaringBuf(bm)
		}
		if tmp.IsEmpty() {
			continue
		}
		if cursor.emitBitmap(tmp) {
			trace.addExamined(examined)
			trace.addOrderScanWidth(scanWidth)
			trace.setEarlyStopReason("limit_reached")
			return cursor.out, true, nil
		}
	}

	trace.addExamined(examined)
	trace.addOrderScanWidth(scanWidth)
	trace.setEarlyStopReason("input_exhausted")
	return cursor.out, true, nil
}

func (db *DB[K, V]) tryQueryOrderPrefixWithLimit(q *qx.QX, trace *queryTrace) ([]K, bool, error) {

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

	ov := db.fieldOverlay(f)
	if !ov.hasData() {
		return nil, true, nil
	}

	expr := q.Expr
	ops := expr.Operands
	if expr.Op != qx.OpAND {
		var single [1]qx.Expr
		single[0] = expr
		ops = single[:]
	}

	var (
		hasPrefix bool
		prefix    string
		baseOps   []qx.Expr
	)
	var baseOpsStack [8]qx.Expr
	if len(ops) <= len(baseOpsStack) {
		baseOps = baseOpsStack[:0]
	} else {
		baseOps = make([]qx.Expr, 0, len(ops))
	}

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
		universe, owned := db.snapshotUniverseView()
		bm.Or(universe)
		if owned {
			releaseRoaringBuf(universe)
		}
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

	baseBM := base.bm
	baseNegUniverse := base.neg && (baseBM == nil || baseBM.IsEmpty())
	if !base.neg && (baseBM == nil || baseBM.IsEmpty()) {
		return nil, true, nil
	}

	br := ov.rangeForBounds(rangeBounds{
		has:       true,
		hasPrefix: true,
		prefix:    prefix,
	})
	if br.baseStart >= br.baseEnd && br.deltaStart >= br.deltaEnd {
		return nil, true, nil
	}

	skip := q.Offset
	need := q.Limit
	out := make([]K, 0, need)
	cursor := db.newQueryCursor(out, skip, need, false, nil)

	contains := func(idx uint64) bool {
		if base.neg {
			if baseNegUniverse {
				return true // NOT empty == universe
			}
			return !baseBM.Contains(idx)
		}
		return baseBM.Contains(idx)
	}

	var scratch *roaring64.Bitmap
	if ov.delta != nil {
		scratch = getRoaringBuf()
		defer releaseRoaringBuf(scratch)
	}

	keyCur := ov.newCursor(br, ord.Desc)
	var (
		examined  uint64
		scanWidth uint64
	)
	if ov.delta == nil {
		for {
			_, ids, _, ok := keyCur.next()
			if !ok {
				break
			}
			if ids.IsEmpty() {
				continue
			}
			scanWidth++
			if ids.isSingleton() {
				examined++
				if contains(ids.single) && cursor.emit(ids.single) {
					trace.addExamined(examined)
					trace.addOrderScanWidth(scanWidth)
					trace.setEarlyStopReason("limit_reached")
					return cursor.out, true, nil
				}
				continue
			}
			iter := ids.bitmap().Iterator()
			for iter.HasNext() {
				idx := iter.Next()
				examined++
				if !contains(idx) {
					continue
				}
				if cursor.emit(idx) {
					trace.addExamined(examined)
					trace.addOrderScanWidth(scanWidth)
					trace.setEarlyStopReason("limit_reached")
					return cursor.out, true, nil
				}
			}
		}
		trace.addExamined(examined)
		trace.addOrderScanWidth(scanWidth)
		trace.setEarlyStopReason("input_exhausted")
		return cursor.out, true, nil
	}

	for {
		_, baseIDs, de, ok := keyCur.next()
		if !ok {
			break
		}
		bm, owned := composePostingOwned(baseIDs, de, scratch)
		if bm == nil || bm.IsEmpty() {
			if owned && bm != nil && bm != scratch {
				releaseRoaringBuf(bm)
			}
			continue
		}
		scanWidth++
		iter := bm.Iterator()
		for iter.HasNext() {
			idx := iter.Next()
			examined++
			if !contains(idx) {
				continue
			}
			if cursor.emit(idx) {
				if owned && bm != scratch {
					releaseRoaringBuf(bm)
				}
				trace.addExamined(examined)
				trace.addOrderScanWidth(scanWidth)
				trace.setEarlyStopReason("limit_reached")
				return cursor.out, true, nil
			}
		}
		if owned && bm != scratch {
			releaseRoaringBuf(bm)
		}
	}

	trace.addExamined(examined)
	trace.addOrderScanWidth(scanWidth)
	trace.setEarlyStopReason("input_exhausted")
	return cursor.out, true, nil
}

func (db *DB[K, V]) tryQueryRangeNoOrderWithLimit(q *qx.QX, trace *queryTrace) ([]K, bool, error) {

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

	ov := db.fieldOverlay(f)
	if !ov.hasData() {
		return nil, true, nil
	}

	key, isSlice, err := db.exprValueToIdxScalar(e)
	if err != nil {
		return nil, true, err
	}
	if isSlice {
		return nil, false, nil
	}

	rb := rangeBounds{has: true}
	switch e.Op {
	case qx.OpGT:
		rb.applyLo(key, false)
	case qx.OpGTE:
		rb.applyLo(key, true)
	case qx.OpLT:
		rb.applyHi(key, false)
	case qx.OpLTE:
		rb.applyHi(key, true)
	case qx.OpEQ:
		rb.applyLo(key, true)
		rb.applyHi(key, true)
	}

	br := ov.rangeForBounds(rb)
	if br.baseStart >= br.baseEnd && br.deltaStart >= br.deltaEnd {
		return nil, true, nil
	}

	skip := q.Offset
	need := q.Limit

	out := make([]K, 0, need)
	cursor := db.newQueryCursor(out, skip, need, false, nil)

	var scratch *roaring64.Bitmap
	if ov.delta != nil {
		scratch = getRoaringBuf()
		defer releaseRoaringBuf(scratch)
	}

	keyCur := ov.newCursor(br, false)
	var examined uint64
	if ov.delta == nil {
		for {
			_, ids, _, ok := keyCur.next()
			if !ok {
				break
			}
			if ids.IsEmpty() {
				continue
			}
			if ids.isSingleton() {
				examined++
				if cursor.emit(ids.single) {
					trace.addExamined(examined)
					trace.setEarlyStopReason("limit_reached")
					return cursor.out, true, nil
				}
				continue
			}
			it := ids.bitmap().Iterator()
			for it.HasNext() {
				examined++
				if cursor.emit(it.Next()) {
					trace.addExamined(examined)
					trace.setEarlyStopReason("limit_reached")
					return cursor.out, true, nil
				}
			}
		}
		trace.addExamined(examined)
		trace.setEarlyStopReason("input_exhausted")
		return cursor.out, true, nil
	}

	for {
		_, baseIDs, de, ok := keyCur.next()
		if !ok {
			break
		}
		bm, owned := composePostingOwned(baseIDs, de, scratch)
		if bm == nil || bm.IsEmpty() {
			if owned && bm != nil && bm != scratch {
				releaseRoaringBuf(bm)
			}
			continue
		}
		it := bm.Iterator()
		for it.HasNext() {
			examined++
			if cursor.emit(it.Next()) {
				if owned && bm != scratch {
					releaseRoaringBuf(bm)
				}
				trace.addExamined(examined)
				trace.setEarlyStopReason("limit_reached")
				return cursor.out, true, nil
			}
		}
		if owned && bm != scratch {
			releaseRoaringBuf(bm)
		}
	}

	trace.addExamined(examined)
	trace.setEarlyStopReason("input_exhausted")
	return cursor.out, true, nil
}

func (db *DB[K, V]) tryQueryPrefixNoOrderWithLimit(q *qx.QX, trace *queryTrace) ([]K, bool, error) {

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

	ov := db.fieldOverlay(e.Field)
	if !ov.hasData() {
		if !db.hasFieldIndex(e.Field) {
			return nil, true, fmt.Errorf("no index for field: %v", e.Field)
		}
		return nil, true, nil
	}

	br := ov.rangeForBounds(rangeBounds{
		has:       true,
		hasPrefix: true,
		prefix:    prefix,
	})
	if br.baseStart >= br.baseEnd && br.deltaStart >= br.deltaEnd {
		return nil, true, nil
	}

	skip := q.Offset
	need := q.Limit
	out := make([]K, 0, need)
	cursor := db.newQueryCursor(out, skip, need, false, nil)

	var scratch *roaring64.Bitmap
	if ov.delta != nil {
		scratch = getRoaringBuf()
		defer releaseRoaringBuf(scratch)
	}

	keyCur := ov.newCursor(br, false)
	var examined uint64
	if ov.delta == nil {
		for {
			_, ids, _, ok := keyCur.next()
			if !ok {
				break
			}
			if ids.IsEmpty() {
				continue
			}
			if ids.isSingleton() {
				examined++
				if cursor.emit(ids.single) {
					trace.addExamined(examined)
					trace.setEarlyStopReason("limit_reached")
					return cursor.out, true, nil
				}
				continue
			}
			it := ids.bitmap().Iterator()
			for it.HasNext() {
				examined++
				if cursor.emit(it.Next()) {
					trace.addExamined(examined)
					trace.setEarlyStopReason("limit_reached")
					return cursor.out, true, nil
				}
			}
		}
		trace.addExamined(examined)
		trace.setEarlyStopReason("input_exhausted")
		return cursor.out, true, nil
	}

	for {
		_, baseIDs, de, ok := keyCur.next()
		if !ok {
			break
		}
		bm, owned := composePostingOwned(baseIDs, de, scratch)
		if bm == nil || bm.IsEmpty() {
			if owned && bm != nil && bm != scratch {
				releaseRoaringBuf(bm)
			}
			continue
		}
		it := bm.Iterator()
		for it.HasNext() {
			examined++
			if cursor.emit(it.Next()) {
				if owned && bm != scratch {
					releaseRoaringBuf(bm)
				}
				trace.addExamined(examined)
				trace.setEarlyStopReason("limit_reached")
				return cursor.out, true, nil
			}
		}
		if owned && bm != scratch {
			releaseRoaringBuf(bm)
		}
	}

	trace.addExamined(examined)
	trace.setEarlyStopReason("input_exhausted")
	return cursor.out, true, nil
}
