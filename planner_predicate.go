package rbi

import (
	"strings"
	"sync"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/vapstack/qx"
)

type predCandidate struct {
	expr qx.Expr

	contains func(uint64) bool
	iter     func() roaringIter
	estCard  uint64

	alwaysTrue  bool
	alwaysFalse bool
	covered     bool

	cleanup func()
}

func (db *DB[K, V]) tryPlanCandidate(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	// candidate planner is focused on the latency-critical paged path.
	if q.Limit == 0 {
		return nil, false, nil
	}
	if len(q.Order) > 1 {
		return nil, false, nil
	}

	leaves, ok := collectAndLeaves(q.Expr)
	if !ok {
		return nil, false, nil
	}
	if len(q.Order) == 1 {
		o := q.Order[0]
		if o.Type != qx.OrderBasic {
			return nil, false, nil
		}
		if q.Offset != 0 {
			return nil, false, nil
		}
		if !db.shouldUseCandidateOrder(o, leaves) {
			return nil, false, nil
		}

		preds, ok := db.buildPredicatesCandidate(leaves)
		if !ok {
			return nil, false, nil
		}
		defer releasePredicatesCandidate(preds)

		for i := range preds {
			if preds[i].alwaysFalse {
				return nil, true, nil
			}
		}

		if trace != nil {
			trace.setPlan(PlanCandidateOrder)
		}
		return db.execPlanCandidateOrderBasic(q, preds), true, nil
	}

	// baseline fast-paths are better for trivial single-field range/prefix scans with limit;
	// candidate strategy is intended for mixed/complex predicates
	if len(leaves) == 1 && !leaves[0].Not {
		switch leaves[0].Op {
		case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
			return nil, false, nil
		}
	}
	// pure EQ conjunctions are already handled very well by baseline paths
	allEq := len(leaves) > 0
	for _, e := range leaves {
		if e.Not || e.Op != qx.OpEQ {
			allEq = false
			break
		}
	}
	if allEq {
		return nil, false, nil
	}

	preds, ok := db.buildPredicatesCandidate(leaves)
	if !ok {
		return nil, false, nil
	}
	defer releasePredicatesCandidate(preds)

	for i := range preds {
		if preds[i].alwaysFalse {
			return nil, true, nil
		}
	}

	if trace != nil {
		trace.setPlan(PlanCandidateNoOrder)
	}
	return db.execPlanCandidateNoOrder(q, preds), true, nil
}

func (db *DB[K, V]) shouldUseCandidateOrder(o qx.Order, leaves []qx.Expr) bool {
	fm := db.fields[o.Field]
	if fm == nil || fm.Slice {
		return false
	}
	if db.index[o.Field] == nil {
		return false
	}
	if len(leaves) < 2 {
		return false
	}

	hasNeg := false
	orderOnly := true

	for _, e := range leaves {
		// keep candidate ORDER conservative: only scalar equality/set predicates
		if e.Op != qx.OpEQ && e.Op != qx.OpIN {
			return false
		}
		if ef := db.fields[e.Field]; ef == nil || ef.Slice {
			return false
		}
		if e.Not {
			hasNeg = true
		}
		if e.Field != o.Field {
			orderOnly = false
		}
	}

	if !hasNeg {
		return false
	}
	if orderOnly {
		return false
	}

	return true
}

func collectAndLeaves(e qx.Expr) ([]qx.Expr, bool) {
	n, ok := countCollectAndLeaves(e)
	if !ok {
		return nil, false
	}
	if n == 0 {
		return nil, true
	}
	out := make([]qx.Expr, 0, n)
	out, ok = appendCollectAndLeaves(out, e)
	if !ok {
		return nil, false
	}
	return out, true
}

func countCollectAndLeaves(e qx.Expr) (int, bool) {
	switch e.Op {
	case qx.OpNOOP:
		if e.Not {
			return 1, true
		}
		return 0, true
	case qx.OpAND:
		if e.Not {
			return 0, false
		}
		if len(e.Operands) == 0 {
			return 0, false
		}
		total := 0
		for _, ch := range e.Operands {
			n, ok := countCollectAndLeaves(ch)
			if !ok {
				return 0, false
			}
			total += n
		}
		return total, true
	case qx.OpOR:
		return 0, false
	default:
		if len(e.Operands) != 0 {
			return 0, false
		}
		return 1, true
	}
}

func appendCollectAndLeaves(dst []qx.Expr, e qx.Expr) ([]qx.Expr, bool) {
	switch e.Op {
	case qx.OpNOOP:
		if e.Not {
			return append(dst, qx.Expr{Op: qx.OpNOOP, Not: true}), true
		}
		return dst, true
	case qx.OpAND:
		if e.Not || len(e.Operands) == 0 {
			return nil, false
		}
		for _, ch := range e.Operands {
			var ok bool
			dst, ok = appendCollectAndLeaves(dst, ch)
			if !ok {
				return nil, false
			}
		}
		return dst, true
	case qx.OpOR:
		return nil, false
	default:
		if len(e.Operands) != 0 {
			return nil, false
		}
		return append(dst, e), true
	}
}

func releasePredicatesCandidate(preds []predCandidate) {
	for i := range preds {
		if preds[i].cleanup != nil {
			preds[i].cleanup()
		}
	}
}

func (db *DB[K, V]) buildPredicatesCandidate(leaves []qx.Expr) ([]predCandidate, bool) {
	if len(leaves) == 1 && leaves[0].Op == qx.OpNOOP && leaves[0].Not {
		return []predCandidate{{alwaysFalse: true}}, true
	}

	preds := make([]predCandidate, 0, len(leaves))
	for _, e := range leaves {
		p, ok := db.buildPredicateCandidate(e)
		if !ok {
			releasePredicatesCandidate(preds)
			return nil, false
		}
		preds = append(preds, p)
	}
	return preds, true
}

func (db *DB[K, V]) buildPredicateCandidate(e qx.Expr) (predCandidate, bool) {
	if e.Op == qx.OpNOOP {
		if e.Not {
			return predCandidate{expr: e, alwaysFalse: true}, true
		}
		return predCandidate{expr: e, alwaysTrue: true}, true
	}
	if e.Field == "" {
		return predCandidate{}, false
	}

	slice := db.index[e.Field]
	if slice == nil {
		return predCandidate{}, false
	}
	fm := db.fields[e.Field]
	if fm == nil {
		return predCandidate{}, false
	}

	switch e.Op {
	case qx.OpEQ:
		return db.buildPredEqCandidate(e, fm, slice)
	case qx.OpIN:
		return db.buildPredInCandidate(e, fm, slice)
	case qx.OpHAS:
		return db.buildPredHasCandidate(e, fm, slice)
	case qx.OpHASANY:
		return db.buildPredHasAnyCandidate(e, fm, slice)
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
		return db.buildPredRangeCandidate(e, fm, slice)
	case qx.OpSUFFIX, qx.OpCONTAINS:
		return db.buildPredMaterializedCandidate(e)
	default:
		return predCandidate{}, false
	}
}

func (db *DB[K, V]) buildPredEqCandidate(e qx.Expr, fm *field, slice *[]index) (predCandidate, bool) {
	key, isSlice, err := db.exprValueToIdxScalar(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil {
		return predCandidate{}, false
	}

	if !fm.Slice {
		if isSlice {
			return predCandidate{}, false
		}
		bm := findIndex(slice, key)
		if e.Not {
			if bm == nil || bm.IsEmpty() {
				return predCandidate{expr: e, alwaysTrue: true}, true
			}
			return predCandidate{
				expr: e,
				contains: func(idx uint64) bool {
					return !bm.Contains(idx)
				},
			}, true
		}
		if bm == nil || bm.IsEmpty() {
			return predCandidate{expr: e, alwaysFalse: true}, true
		}
		return predCandidate{
			expr: e,
			iter: func() roaringIter { return bm.Iterator() },
			contains: func(idx uint64) bool {
				return bm.Contains(idx)
			},
			estCard: bm.GetCardinality(),
		}, true
	}

	if !isSlice {
		return predCandidate{}, false
	}

	vals, _, err := db.exprValueToIdx(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil {
		return predCandidate{}, false
	}

	b, err := db.evalSliceEQ(e.Field, vals)
	if err != nil {
		return predCandidate{}, false
	}

	if e.Not {
		if b.bm == nil || b.bm.IsEmpty() {
			b.release()
			return predCandidate{expr: e, alwaysTrue: true}, true
		}
		bm := b.bm
		readonly := b.readonly
		return predCandidate{
			expr: e,
			contains: func(idx uint64) bool {
				return !bm.Contains(idx)
			},
			cleanup: func() {
				if !readonly {
					releaseRoaringBuf(bm)
				}
			},
		}, true
	}

	if b.bm == nil || b.bm.IsEmpty() {
		b.release()
		return predCandidate{expr: e, alwaysFalse: true}, true
	}

	bm := b.bm
	readonly := b.readonly
	return predCandidate{
		expr: e,
		iter: func() roaringIter { return bm.Iterator() },
		contains: func(idx uint64) bool {
			return bm.Contains(idx)
		},
		estCard: bm.GetCardinality(),
		cleanup: func() {
			if !readonly {
				releaseRoaringBuf(bm)
			}
		},
	}, true
}

func (db *DB[K, V]) buildPredInCandidate(e qx.Expr, fm *field, slice *[]index) (predCandidate, bool) {
	if fm.Slice {
		return predCandidate{}, false
	}

	vals, isSlice, err := db.exprValueToIdx(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil {
		return predCandidate{}, false
	}
	if !isSlice || len(vals) == 0 {
		return predCandidate{}, false
	}
	vals = dedupStringsInplace(vals)

	bms := make([]*roaring64.Bitmap, 0, len(vals))
	var est uint64
	for _, v := range vals {
		bm := findIndex(slice, v)
		if bm != nil && !bm.IsEmpty() {
			bms = append(bms, bm)
			est += bm.GetCardinality()
		}
	}

	if e.Not {
		if len(bms) == 0 {
			return predCandidate{expr: e, alwaysTrue: true}, true
		}
		return predCandidate{
			expr: e,
			contains: func(idx uint64) bool {
				for _, bm := range bms {
					if bm.Contains(idx) {
						return false
					}
				}
				return true
			},
		}, true
	}

	if len(bms) == 0 {
		return predCandidate{expr: e, alwaysFalse: true}, true
	}
	if len(bms) == 1 {
		bm := bms[0]
		return predCandidate{
			expr: e,
			iter: func() roaringIter { return bm.Iterator() },
			contains: func(idx uint64) bool {
				return bm.Contains(idx)
			},
			estCard: bm.GetCardinality(),
		}, true
	}

	return predCandidate{
		expr: e,
		iter: func() roaringIter { return newConcatIter(bms) },
		contains: func(idx uint64) bool {
			for _, bm := range bms {
				if bm.Contains(idx) {
					return true
				}
			}
			return false
		},
		estCard: est,
	}, true
}

func (db *DB[K, V]) buildPredHasCandidate(e qx.Expr, fm *field, slice *[]index) (predCandidate, bool) {
	if !fm.Slice {
		return predCandidate{}, false
	}

	vals, isSlice, err := db.exprValueToIdx(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil {
		return predCandidate{}, false
	}
	if !isSlice || len(vals) == 0 {
		return predCandidate{}, false
	}
	vals = dedupStringsInplace(vals)

	bms := make([]*roaring64.Bitmap, 0, len(vals))
	var minCard uint64

	for _, v := range vals {
		bm := findIndex(slice, v)
		if bm == nil || bm.IsEmpty() {
			if e.Not {
				return predCandidate{expr: e, alwaysTrue: true}, true
			}
			return predCandidate{expr: e, alwaysFalse: true}, true
		}
		bms = append(bms, bm)
		c := bm.GetCardinality()
		if minCard == 0 || c < minCard {
			minCard = c
		}
	}

	if e.Not {
		return predCandidate{
			expr: e,
			contains: func(idx uint64) bool {
				for _, bm := range bms {
					if !bm.Contains(idx) {
						return true
					}
				}
				return false
			},
		}, true
	}

	lead := minCardBM(bms)
	return predCandidate{
		expr: e,
		iter: func() roaringIter { return lead.Iterator() },
		contains: func(idx uint64) bool {
			for _, bm := range bms {
				if !bm.Contains(idx) {
					return false
				}
			}
			return true
		},
		estCard: minCard,
	}, true
}

func (db *DB[K, V]) buildPredHasAnyCandidate(e qx.Expr, fm *field, slice *[]index) (predCandidate, bool) {
	if !fm.Slice {
		return predCandidate{}, false
	}

	vals, isSlice, err := db.exprValueToIdx(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil {
		return predCandidate{}, false
	}
	if !isSlice || len(vals) == 0 {
		return predCandidate{}, false
	}
	vals = dedupStringsInplace(vals)

	bms := make([]*roaring64.Bitmap, 0, len(vals))
	var est uint64
	for _, v := range vals {
		bm := findIndex(slice, v)
		if bm != nil && !bm.IsEmpty() {
			bms = append(bms, bm)
			est += bm.GetCardinality()
		}
	}

	if e.Not {
		if len(bms) == 0 {
			return predCandidate{expr: e, alwaysTrue: true}, true
		}
		return predCandidate{
			expr: e,
			contains: func(idx uint64) bool {
				for _, bm := range bms {
					if bm.Contains(idx) {
						return false
					}
				}
				return true
			},
		}, true
	}

	if len(bms) == 0 {
		return predCandidate{expr: e, alwaysFalse: true}, true
	}
	if len(bms) == 1 {
		bm := bms[0]
		return predCandidate{
			expr: e,
			iter: func() roaringIter { return bm.Iterator() },
			contains: func(idx uint64) bool {
				return bm.Contains(idx)
			},
			estCard: bm.GetCardinality(),
		}, true
	}

	return predCandidate{
		expr: e,
		iter: func() roaringIter { return newUnionIter(bms) },
		contains: func(idx uint64) bool {
			for _, bm := range bms {
				if bm.Contains(idx) {
					return true
				}
			}
			return false
		},
		estCard: est,
	}, true
}

func (db *DB[K, V]) buildPredRangeCandidate(e qx.Expr, fm *field, slice *[]index) (predCandidate, bool) {
	if fm.Slice {
		return predCandidate{}, false
	}

	key, isSlice, err := db.exprValueToIdxScalar(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil {
		return predCandidate{}, false
	}
	if isSlice {
		return predCandidate{}, false
	}

	s := *slice
	start, end, ok := resolveRange(s, e.Op, key)
	if !ok {
		return predCandidate{}, false
	}

	if start >= end {
		if e.Not {
			return predCandidate{expr: e, alwaysTrue: true}, true
		}
		return predCandidate{expr: e, alwaysFalse: true}, true
	}

	var est uint64
	for i := start; i < end; i++ {
		est += s[i].IDs.GetCardinality()
	}

	var (
		once sync.Once
		bm   *roaring64.Bitmap
	)
	contains := func(idx uint64) bool {
		if end-start <= 16 {
			for i := start; i < end; i++ {
				if s[i].IDs.Contains(idx) {
					return true
				}
			}
			return false
		}

		once.Do(func() {
			bm = db.unionIndexRange(e.Field, start, end)
		})
		return bm.Contains(idx)
	}

	cleanup := func() {
		if bm != nil {
			releaseRoaringBuf(bm)
		}
	}

	if e.Not {
		return predCandidate{
			expr: e,
			contains: func(idx uint64) bool {
				return !contains(idx)
			},
			cleanup: cleanup,
		}, true
	}

	return predCandidate{
		expr: e,
		iter: func() roaringIter {
			return newRangeIter(s, start, end)
		},
		contains: contains,
		estCard:  est,
		cleanup:  cleanup,
	}, true
}

func (db *DB[K, V]) buildPredMaterializedCandidate(e qx.Expr) (predCandidate, bool) {
	raw := e
	raw.Not = false

	b, err := db.evalSimple(raw)
	if err != nil {
		return predCandidate{}, false
	}

	if e.Not {
		if b.bm == nil || b.bm.IsEmpty() {
			b.release()
			return predCandidate{expr: e, alwaysTrue: true}, true
		}
		bm := b.bm
		readonly := b.readonly
		return predCandidate{
			expr: e,
			contains: func(idx uint64) bool {
				return !bm.Contains(idx)
			},
			cleanup: func() {
				if !readonly {
					releaseRoaringBuf(bm)
				}
			},
		}, true
	}

	if b.bm == nil || b.bm.IsEmpty() {
		b.release()
		return predCandidate{expr: e, alwaysFalse: true}, true
	}

	bm := b.bm
	readonly := b.readonly
	return predCandidate{
		expr: e,
		iter: func() roaringIter { return bm.Iterator() },
		contains: func(idx uint64) bool {
			return bm.Contains(idx)
		},
		estCard: bm.GetCardinality(),
		cleanup: func() {
			if !readonly {
				releaseRoaringBuf(bm)
			}
		},
	}, true
}

func resolveRange(s []index, op qx.Op, key string) (start, end int, ok bool) {
	lo := lowerBoundIndex(s, key)

	start = 0
	end = len(s)

	switch op {
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
	case qx.OpPREFIX:
		start = lo
		end = start
		for end < len(s) && strings.HasPrefix(s[end].Key, key) {
			end++
		}
	default:
		return 0, 0, false
	}

	if start < 0 {
		start = 0
	}
	if end > len(s) {
		end = len(s)
	}
	if start > end {
		start = end
	}
	return start, end, true
}

type rangeIter struct {
	s     []index
	i     int
	end   int
	curIt roaringIter
}

func newRangeIter(s []index, start, end int) roaringIter {
	return &rangeIter{s: s, i: start, end: end}
}

func (it *rangeIter) HasNext() bool {
	for {
		if it.curIt != nil && it.curIt.HasNext() {
			return true
		}
		if it.i >= it.end {
			return false
		}
		bm := it.s[it.i].IDs
		it.i++
		if bm == nil || bm.IsEmpty() {
			continue
		}
		it.curIt = bm.Iterator()
	}
}

func (it *rangeIter) Next() uint64 {
	if !it.HasNext() {
		return 0
	}
	return it.curIt.Next()
}

func (db *DB[K, V]) execPlanCandidateNoOrder(q *qx.QX, preds []predCandidate) []K {
	skip := q.Offset
	need := q.Limit

	var leadIdx = -1
	var best uint64
	for i := range preds {
		p := preds[i]
		if p.alwaysTrue || p.alwaysFalse || p.iter == nil {
			continue
		}
		if leadIdx == -1 || p.estCard < best {
			leadIdx = i
			best = p.estCard
		}
	}

	var it roaringIter
	if leadIdx >= 0 {
		it = preds[leadIdx].iter()
	} else {
		it = db.universe.Iterator()
	}

	out := make([]K, 0, need)

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	for it.HasNext() {
		idx := it.Next()

		pass := true
		for i := range preds {
			if i == leadIdx {
				continue
			}
			p := preds[i]
			if p.covered || p.alwaysTrue {
				continue
			}
			if p.alwaysFalse {
				pass = false
				break
			}
			if p.contains == nil || !p.contains(idx) {
				pass = false
				break
			}
		}
		if !pass {
			continue
		}

		if skip > 0 {
			skip--
			continue
		}

		out = append(out, db.idFromIdxNoLock(idx))
		need--
		if need == 0 {
			break
		}
	}

	return out
}

func (db *DB[K, V]) execPlanCandidateOrderBasic(q *qx.QX, preds []predCandidate) []K {
	o := q.Order[0]

	fm := db.fields[o.Field]
	if fm == nil || fm.Slice {
		return nil
	}
	slice := db.index[o.Field]
	if slice == nil {
		return nil
	}
	s := *slice
	if len(s) == 0 {
		return nil
	}

	start, end := 0, len(s)
	rangeCovered := make([]bool, len(preds))
	if st, en, cov, ok := db.extractOrderRangeCoverage(o.Field, preds, s); ok {
		start, end = st, en
		rangeCovered = cov
	}
	if start >= end {
		return nil
	}
	for i := range rangeCovered {
		if rangeCovered[i] {
			preds[i].covered = true
		}
	}

	skip := q.Offset
	need := q.Limit
	out := make([]K, 0, need)

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	emit := func(bm *roaring64.Bitmap) bool {
		it := bm.Iterator()
		for it.HasNext() {
			idx := it.Next()

			pass := true
			for i := range preds {
				p := preds[i]
				if p.covered || p.alwaysTrue {
					continue
				}
				if p.alwaysFalse {
					pass = false
					break
				}
				if p.contains == nil || !p.contains(idx) {
					pass = false
					break
				}
			}
			if !pass {
				continue
			}

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

	if !o.Desc {
		for i := start; i < end; i++ {
			if s[i].IDs == nil || s[i].IDs.IsEmpty() {
				continue
			}
			if emit(s[i].IDs) {
				break
			}
		}
	} else {
		for i := end - 1; i >= start; i-- {
			if s[i].IDs != nil && !s[i].IDs.IsEmpty() {
				if emit(s[i].IDs) {
					break
				}
			}
			if i == start {
				break
			}
		}
	}

	return out
}

func (db *DB[K, V]) extractOrderRangeCoverage(field string, preds []predCandidate, s []index) (int, int, []bool, bool) {
	rb := rangeBounds{}
	covered := make([]bool, len(preds))

	has := false
	for i := range preds {
		e := preds[i].expr
		if e.Not || e.Field != field {
			continue
		}
		switch e.Op {
		case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpEQ:
			k, isSlice, err := db.exprValueToIdxScalar(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
			if err != nil || isSlice {
				return 0, 0, nil, false
			}
			switch e.Op {
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
			covered[i] = true
			has = true
		case qx.OpPREFIX:
			p, isSlice, err := db.exprValueToIdxScalar(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
			if err != nil || isSlice {
				return 0, 0, nil, false
			}
			rb.hasPrefix = true
			rb.prefix = p
			covered[i] = true
			has = true
		}
	}

	if !has {
		return 0, len(s), covered, true
	}

	st, en := applyBoundsToIndexRange(s, rb)
	return st, en, covered, true
}

const (
	rangeLinearContainsMax   = 16
	rangeMaterializeAfter    = 64
	rangeMaterializeAfterMed = 8
	rangeMaterializeAfterBig = 1
	rangeHashSetBucketsMin   = 1024
	rangeHashSetMaxCard      = 200_000
	rangeBucketCountMaxProbe = 64
)

type plannerPred struct {
	expr qx.Expr

	contains func(uint64) bool
	iter     func() roaringIter
	estCard  uint64

	alwaysTrue  bool
	alwaysFalse bool
	covered     bool

	// Exact match count for a candidate order bucket.
	bucketCount func(*roaring64.Bitmap) (uint64, bool)

	cleanup func()
}

func (db *DB[K, V]) tryPlanOrdered(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	if q.Limit == 0 {
		return nil, false, nil
	}
	if len(q.Order) > 1 {
		return nil, false, nil
	}

	leaves, ok := collectAndLeaves(q.Expr)
	if !ok {
		return nil, false, nil
	}

	if len(q.Order) == 1 {
		o := q.Order[0]
		if o.Type != qx.OrderBasic {
			return nil, false, nil
		}
		decision := db.decideOrderedByCost(q, leaves)
		if trace != nil {
			trace.setEstimated(decision.expectedProbeRows, decision.orderedCost, decision.fallbackCost)
		}
		if !decision.use {
			return nil, false, nil
		}

		preds, ok := db.buildPredicates(leaves)
		if !ok {
			return nil, false, nil
		}
		defer releasePredicates(preds)

		for i := range preds {
			if preds[i].alwaysFalse {
				return nil, true, nil
			}
		}

		if trace != nil {
			trace.setPlan(PlanOrdered)
		}
		out, ok := db.execPlanOrderedBasic(q, preds, trace)
		if !ok {
			return nil, false, nil
		}
		return out, true, nil
	}

	// keep baseline paths for trivial patterns where ordered strategy usually has no advantage
	if len(leaves) == 1 && !leaves[0].Not {
		switch leaves[0].Op {
		case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
			return nil, false, nil
		}
	}
	allEq := len(leaves) > 0
	for _, e := range leaves {
		if e.Not || e.Op != qx.OpEQ {
			allEq = false
			break
		}
	}
	if allEq {
		return nil, false, nil
	}

	preds, ok := db.buildPredicates(leaves)
	if !ok {
		return nil, false, nil
	}
	defer releasePredicates(preds)

	for i := range preds {
		if preds[i].alwaysFalse {
			return nil, true, nil
		}
	}

	if trace != nil {
		trace.setPlan(PlanOrderedNoOrder)
	}
	return db.execPlanOrderedNoOrder(q, preds, trace), true, nil
}

func (db *DB[K, V]) shouldUseOrdered(q *qx.QX, leaves []qx.Expr) bool {
	return db.shouldUseOrderedByCost(q, leaves)
}

func plannerPredFromCandidate(p predCandidate) plannerPred {
	return plannerPred{
		expr:        p.expr,
		contains:    p.contains,
		iter:        p.iter,
		estCard:     p.estCard,
		alwaysTrue:  p.alwaysTrue,
		alwaysFalse: p.alwaysFalse,
		covered:     p.covered,
		cleanup:     p.cleanup,
	}
}

func releasePredicates(preds []plannerPred) {
	for i := range preds {
		if preds[i].cleanup != nil {
			preds[i].cleanup()
		}
	}
}

func (db *DB[K, V]) buildPredicates(leaves []qx.Expr) ([]plannerPred, bool) {
	if len(leaves) == 1 && leaves[0].Op == qx.OpNOOP && leaves[0].Not {
		return []plannerPred{{alwaysFalse: true}}, true
	}

	preds := make([]plannerPred, 0, len(leaves))
	for _, e := range leaves {
		p, ok := db.buildPredicate(e)
		if !ok {
			releasePredicates(preds)
			return nil, false
		}
		preds = append(preds, p)
	}
	return preds, true
}

func (db *DB[K, V]) buildPredicate(e qx.Expr) (plannerPred, bool) {
	if e.Op == qx.OpNOOP {
		if e.Not {
			return plannerPred{expr: e, alwaysFalse: true}, true
		}
		return plannerPred{expr: e, alwaysTrue: true}, true
	}

	if e.Field == "" {
		return plannerPred{}, false
	}

	slice := db.index[e.Field]
	if slice == nil {
		return plannerPred{}, false
	}
	fm := db.fields[e.Field]
	if fm == nil {
		return plannerPred{}, false
	}

	switch e.Op {
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
		return db.buildPredRange(e, fm, slice)
	default:
		p, ok := db.buildPredicateCandidate(e)
		if !ok {
			return plannerPred{}, false
		}
		return plannerPredFromCandidate(p), true
	}
}

func (db *DB[K, V]) buildPredRange(e qx.Expr, fm *field, slice *[]index) (plannerPred, bool) {
	if fm.Slice {
		return plannerPred{}, false
	}

	key, isSlice, err := db.exprValueToIdxScalar(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil {
		return plannerPred{}, false
	}
	if isSlice {
		return plannerPred{}, false
	}

	s := *slice
	start, end, ok := resolveRange(s, e.Op, key)
	if !ok {
		return plannerPred{}, false
	}

	if start >= end {
		if e.Not {
			return plannerPred{expr: e, alwaysTrue: true}, true
		}
		return plannerPred{expr: e, alwaysFalse: true}, true
	}

	if start == 0 && end == len(s) {
		if e.Not {
			return plannerPred{expr: e, alwaysFalse: true}, true
		}
		return plannerPred{expr: e, alwaysTrue: true}, true
	}

	inBuckets := end - start
	outBuckets := len(s) - inBuckets
	useComplement := outBuckets < inBuckets

	// estimation is only for lead selection;
	// keep it cheap and avoid full-range cardinality scans
	var est uint64
	if inBuckets == 1 {
		if ids := s[start].IDs; ids != nil {
			est = ids.GetCardinality()
		}
	} else {
		ix0 := start
		ix1 := start + inBuckets/2
		ix2 := end - 1
		var sum uint64
		var n uint64

		if ids := s[ix0].IDs; ids != nil {
			sum += ids.GetCardinality()
			n++
		}
		if ix1 != ix0 && ix1 != ix2 {
			if ids := s[ix1].IDs; ids != nil {
				sum += ids.GetCardinality()
				n++
			}
		}
		if ix2 != ix0 {
			if ids := s[ix2].IDs; ids != nil {
				sum += ids.GetCardinality()
				n++
			}
		}

		if n > 0 {
			est = (sum / n) * uint64(inBuckets)
		}
	}
	if est == 0 {
		est = uint64(inBuckets)
	}

	probe := make([]*roaring64.Bitmap, 0, func() int {
		if useComplement {
			return outBuckets
		}
		return inBuckets
	}())

	if useComplement {
		for i := 0; i < start; i++ {
			if s[i].IDs != nil && !s[i].IDs.IsEmpty() {
				probe = append(probe, s[i].IDs)
			}
		}
		for i := end; i < len(s); i++ {
			if s[i].IDs != nil && !s[i].IDs.IsEmpty() {
				probe = append(probe, s[i].IDs)
			}
		}
	} else {
		for i := start; i < end; i++ {
			if s[i].IDs != nil && !s[i].IDs.IsEmpty() {
				probe = append(probe, s[i].IDs)
			}
		}
	}

	containsInRange, cleanup := db.buildRangeContains(probe, useComplement, est)

	countInRange := func(bucket *roaring64.Bitmap) (uint64, bool) {
		if bucket == nil || bucket.IsEmpty() {
			return 0, true
		}
		if len(probe) > rangeBucketCountMaxProbe {
			return 0, false
		}

		var hit uint64
		for _, bm := range probe {
			hit += bucket.AndCardinality(bm)
		}

		if !useComplement {
			return hit, true
		}

		bc := bucket.GetCardinality()
		if hit >= bc {
			return 0, true
		}
		return bc - hit, true
	}

	if e.Not {
		return plannerPred{
			expr: e,
			contains: func(idx uint64) bool {
				return !containsInRange(idx)
			},
			bucketCount: func(bucket *roaring64.Bitmap) (uint64, bool) {
				in, ok := countInRange(bucket)
				if !ok {
					return 0, false
				}
				bc := bucket.GetCardinality()
				if in >= bc {
					return 0, true
				}
				return bc - in, true
			},
			cleanup: cleanup,
		}, true
	}

	return plannerPred{
		expr: e,
		iter: func() roaringIter {
			return newRangeIter(s, start, end)
		},
		contains: containsInRange,
		estCard:  est,
		bucketCount: func(bucket *roaring64.Bitmap) (uint64, bool) {
			return countInRange(bucket)
		},
		cleanup: cleanup,
	}, true
}

func (db *DB[K, V]) buildRangeContains(probe []*roaring64.Bitmap, invert bool, est uint64) (func(uint64) bool, func()) {
	if len(probe) == 0 {
		if invert {
			return func(uint64) bool { return true }, nil
		}
		return func(uint64) bool { return false }, nil
	}

	linearContains := func(idx uint64) bool {
		for _, bm := range probe {
			if bm.Contains(idx) {
				return true
			}
		}
		return false
	}

	if invert || len(probe) <= rangeLinearContainsMax {
		return func(idx uint64) bool {
			hit := linearContains(idx)
			if invert {
				return !hit
			}
			return hit
		}, nil
	}

	var (
		calls int
		set   *u64set
		bm    *roaring64.Bitmap
	)

	contains := func(idx uint64) bool {
		if set != nil {
			return set.Has(idx)
		}
		if bm != nil {
			return bm.Contains(idx)
		}

		calls++
		materializeAfter := rangeMaterializeAfter
		if len(probe) >= rangeHashSetBucketsMin {
			materializeAfter = rangeMaterializeAfterBig
		} else if len(probe) >= 256 {
			materializeAfter = rangeMaterializeAfterMed
		}
		if calls >= materializeAfter {
			if len(probe) >= rangeHashSetBucketsMin && est > 0 && est <= rangeHashSetMaxCard {
				capHint := int(est)
				if capHint < 64 {
					capHint = 64
				}
				hs := newU64Set(capHint)
				for _, b := range probe {
					it := b.Iterator()
					for it.HasNext() {
						hs.Add(it.Next())
					}
				}
				set = &hs
				return set.Has(idx)
			}
			if len(probe) > rangeLinearContainsMax {
				bm = db.unionBitmaps(probe)
				return bm.Contains(idx)
			}
		}

		return linearContains(idx)
	}

	cleanup := func() {
		if bm != nil {
			releaseRoaringBuf(bm)
		}
	}

	return contains, cleanup
}

func (db *DB[K, V]) execPlanOrderedNoOrder(q *qx.QX, preds []plannerPred, trace *queryTrace) []K {
	skip := q.Offset
	need := q.Limit

	var leadIdx = -1
	var best uint64
	for i := range preds {
		p := preds[i]
		if p.alwaysTrue || p.alwaysFalse || p.iter == nil {
			continue
		}
		if leadIdx == -1 || p.estCard < best {
			leadIdx = i
			best = p.estCard
		}
	}

	var it roaringIter
	if leadIdx >= 0 {
		it = preds[leadIdx].iter()
	} else {
		it = db.universe.Iterator()
	}
	if trace != nil {
		if leadIdx >= 0 && best > 0 {
			trace.addExamined(best)
		} else {
			trace.addExamined(db.universe.GetCardinality())
		}
	}

	out := make([]K, 0, need)

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	for it.HasNext() {
		idx := it.Next()

		pass := true
		for i := range preds {
			if i == leadIdx {
				continue
			}
			p := preds[i]
			if p.covered || p.alwaysTrue {
				continue
			}
			if p.alwaysFalse {
				pass = false
				break
			}
			if p.contains == nil || !p.contains(idx) {
				pass = false
				break
			}
		}
		if !pass {
			continue
		}

		if skip > 0 {
			skip--
			continue
		}

		out = append(out, db.idFromIdxNoLock(idx))
		need--
		if need == 0 {
			break
		}
	}

	return out
}

const (
	plannerOrderedAnchorMinActive        = 2
	plannerOrderedAnchorSpanMin          = 256
	plannerOrderedAnchorLeadBudgetMin    = 4_096
	plannerOrderedAnchorLeadBudgetMax    = 350_000
	plannerOrderedAnchorLeadNeedMul      = 32
	plannerOrderedAnchorCandidateCapMin  = 8_192
	plannerOrderedAnchorCandidateCapMax  = 300_000
	plannerOrderedAnchorCandidateNeedMul = 64
)

func orderedAnchorLeadOpRank(op qx.Op) int {
	switch op {
	case qx.OpEQ:
		return 0
	case qx.OpIN, qx.OpHAS, qx.OpHASANY:
		return 1
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
		return 2
	default:
		return 3
	}
}

func chooseOrderedAnchorLead(orderField string, preds []plannerPred, active []int) (int, bool) {
	lead := -1
	bestRank := 0
	bestCard := uint64(0)

	for _, pi := range active {
		p := preds[pi]
		if p.iter == nil || p.estCard == 0 {
			continue
		}
		rank := orderedAnchorLeadOpRank(p.expr.Op)
		if p.expr.Field == orderField {
			// prefer non-order predicates for anchoring
			rank += 3
		}
		if lead == -1 || rank < bestRank || (rank == bestRank && p.estCard < bestCard) {
			lead = pi
			bestRank = rank
			bestCard = p.estCard
		}
	}

	return lead, lead >= 0
}

func (db *DB[K, V]) execPlanOrderedBasicAnchored(q *qx.QX, preds []plannerPred, active []int, start, end int, s []index, trace *queryTrace) ([]K, bool) {
	if len(active) < plannerOrderedAnchorMinActive {
		return nil, false
	}
	if end-start < plannerOrderedAnchorSpanMin {
		return nil, false
	}

	leadIdx, ok := chooseOrderedAnchorLead(q.Order[0].Field, preds, active)
	if !ok {
		return nil, false
	}

	needWindow, ok := orderWindow(q)
	if !ok || needWindow <= 0 {
		return nil, false
	}

	leadBudget := uint64(max(
		plannerOrderedAnchorLeadBudgetMin,
		min(plannerOrderedAnchorLeadBudgetMax, needWindow*plannerOrderedAnchorLeadNeedMul),
	))
	candidateCap := uint64(max(
		plannerOrderedAnchorCandidateCapMin,
		min(plannerOrderedAnchorCandidateCapMax, needWindow*plannerOrderedAnchorCandidateNeedMul),
	))

	leadIt := preds[leadIdx].iter()
	if leadIt == nil {
		return nil, false
	}

	candidates := getRoaringBuf()
	defer releaseRoaringBuf(candidates)

	leadExamined := uint64(0)
	for leadIt.HasNext() {
		leadExamined++
		if leadExamined > leadBudget {
			return nil, false
		}
		if trace != nil {
			trace.addExamined(1)
		}

		idx := leadIt.Next()

		pass := true
		for _, pi := range active {
			if pi == leadIdx {
				continue
			}
			p := preds[pi]
			if p.contains == nil || !p.contains(idx) {
				pass = false
				break
			}
		}
		if !pass {
			continue
		}

		candidates.Add(idx)
		if candidates.GetCardinality() > candidateCap {
			return nil, false
		}
	}

	if candidates.IsEmpty() {
		if trace != nil {
			trace.setEarlyStopReason("no_candidates")
		}
		return nil, true
	}

	o := q.Order[0]
	skip := q.Offset
	need := q.Limit
	out := make([]K, 0, need)
	totalCandidates := candidates.GetCardinality()
	seenCandidates := uint64(0)
	scanWidth := uint64(0)

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	emitBucket := func(bm *roaring64.Bitmap) bool {
		if bm == nil || bm.IsEmpty() {
			return false
		}
		scanWidth++
		if trace != nil {
			trace.addExamined(bm.GetCardinality())
		}

		it := bm.Iterator()
		for it.HasNext() {
			idx := it.Next()
			if !candidates.Contains(idx) {
				continue
			}
			seenCandidates++

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

	if !o.Desc {
		for i := start; i < end; i++ {
			if emitBucket(s[i].IDs) {
				break
			}
			if seenCandidates == totalCandidates {
				break
			}
		}
	} else {
		for i := end - 1; i >= start; i-- {
			if emitBucket(s[i].IDs) {
				break
			}
			if seenCandidates == totalCandidates {
				break
			}
			if i == start {
				break
			}
		}
	}

	if trace != nil {
		trace.addOrderScanWidth(scanWidth)
		if need == 0 {
			trace.setEarlyStopReason("limit_reached")
		} else if seenCandidates == totalCandidates {
			trace.setEarlyStopReason("candidates_exhausted")
		} else {
			trace.setEarlyStopReason("order_index_exhausted")
		}
	}

	return out, true
}

func (db *DB[K, V]) execPlanOrderedBasic(q *qx.QX, preds []plannerPred, trace *queryTrace) ([]K, bool) {
	o := q.Order[0]

	fm := db.fields[o.Field]
	if fm == nil || fm.Slice {
		return nil, false
	}
	slice := db.index[o.Field]
	if slice == nil {
		return nil, false
	}
	s := *slice
	if len(s) == 0 {
		return nil, true
	}

	start, end := 0, len(s)
	rangeCovered := make([]bool, len(preds))
	if st, en, cov, ok := db.extractOrderRangeCoveragePreds(o.Field, preds, s); ok {
		start, end = st, en
		rangeCovered = cov
	}
	if start >= end {
		return nil, true
	}
	for i := range rangeCovered {
		if rangeCovered[i] {
			preds[i].covered = true
		}
	}

	active := make([]int, 0, len(preds))
	for i := range preds {
		p := preds[i]
		if p.covered || p.alwaysTrue {
			continue
		}
		if p.alwaysFalse {
			return nil, true
		}
		active = append(active, i)
	}

	if out, ok := db.execPlanOrderedBasicAnchored(q, preds, active, start, end, s, trace); ok {
		if trace != nil {
			trace.setPlan(PlanOrderedAnchor)
		}
		return out, true
	}

	return db.execPlanOrderedBasicFallback(q, preds, active, start, end, s, trace), true
}

func (db *DB[K, V]) execPlanOrderedBasicFallback(q *qx.QX, preds []plannerPred, active []int, start, end int, s []index, trace *queryTrace) []K {
	skip := q.Offset
	need := q.Limit
	out := make([]K, 0, need)

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	singleActive := -1
	if len(active) == 1 {
		singleActive = active[0]
	}

	maybeSkipBucket := func(bucket *roaring64.Bitmap) bool {
		if bucket == nil || bucket.IsEmpty() {
			return true
		}

		if skip == 0 {
			return false
		}

		if len(active) == 0 {
			bc := bucket.GetCardinality()
			if bc <= skip {
				skip -= bc
				return true
			}
			return false
		}

		if singleActive >= 0 {
			if counter := preds[singleActive].bucketCount; counter != nil {
				cnt, ok := counter(bucket)
				if ok {
					if cnt == 0 {
						return true
					}
					if cnt <= skip {
						skip -= cnt
						return true
					}
				}
			}
		}

		return false
	}

	emit := func(bm *roaring64.Bitmap) bool {
		if bm == nil || bm.IsEmpty() {
			return false
		}
		if trace != nil {
			trace.addExamined(bm.GetCardinality())
		}

		if len(active) == 0 {
			it := bm.Iterator()
			for it.HasNext() {
				idx := it.Next()
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

		if singleActive >= 0 {
			pred := preds[singleActive]
			it := bm.Iterator()
			for it.HasNext() {
				idx := it.Next()
				if pred.contains == nil || !pred.contains(idx) {
					continue
				}
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

		it := bm.Iterator()
		for it.HasNext() {
			idx := it.Next()

			pass := true
			for _, pi := range active {
				p := preds[pi]
				if p.contains == nil || !p.contains(idx) {
					pass = false
					break
				}
			}
			if !pass {
				continue
			}

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

	o := q.Order[0]
	if !o.Desc {
		for i := start; i < end; i++ {
			bucket := s[i].IDs
			if maybeSkipBucket(bucket) {
				continue
			}
			if emit(bucket) {
				break
			}
		}
	} else {
		for i := end - 1; i >= start; i-- {
			bucket := s[i].IDs
			if maybeSkipBucket(bucket) {
				if i == start {
					break
				}
				continue
			}
			if emit(bucket) {
				break
			}
			if i == start {
				break
			}
		}
	}

	return out
}

func (db *DB[K, V]) extractOrderRangeCoveragePreds(field string, preds []plannerPred, s []index) (int, int, []bool, bool) {
	var rb rangeBounds

	covered := make([]bool, len(preds))

	has := false
	for i := range preds {
		e := preds[i].expr
		if e.Not || e.Field != field {
			continue
		}

		switch e.Op {

		case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpEQ:
			k, isSlice, err := db.exprValueToIdxScalar(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
			if err != nil || isSlice {
				return 0, 0, nil, false
			}
			switch e.Op {
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
			covered[i] = true
			has = true

		case qx.OpPREFIX:
			p, isSlice, err := db.exprValueToIdxScalar(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
			if err != nil || isSlice {
				return 0, 0, nil, false
			}
			rb.hasPrefix = true
			rb.prefix = p
			covered[i] = true
			has = true
		}
	}

	if !has {
		return 0, len(s), covered, true
	}

	st, en := applyBoundsToIndexRange(s, rb)
	return st, en, covered, true
}
