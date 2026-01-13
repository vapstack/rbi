package rbi

import (
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"unsafe"

	"github.com/vapstack/qx"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

const iteratorThreshold = 2048 // 256

// QueryItems evaluates the given query against the index and returns all matching values.
func (db *DB[K, V]) QueryItems(q *qx.QX) ([]*V, error) {

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

	ids, err := db.query(q)
	if err != nil {
		return nil, err
	}
	return db.GetMany(ids...)
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

	q = normalizeQuery(q)

	if err := db.checkUsedFields(q); err != nil {
		return nil, err
	}

	if out, ok, err := db.tryFastPath(q); ok {
		return out, err
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
	defer result.release()

	skip := q.Offset
	needAll := q.Limit == 0
	need := q.Limit

	// case 1: no ordering, negative result: iterate over universe excluding the result set
	if len(q.Order) == 0 && result.neg {
		if db.strkey {
			db.strmap.RLock()
			defer db.strmap.RUnlock()
		}

		out := make([]K, 0, func() uint64 {
			if needAll {
				return db.universe.GetCardinality()
			}
			return need
		}())

		ex := result.bm
		it := db.universe.Iterator()
		for it.HasNext() {
			idx := it.Next()
			if ex != nil && ex.Contains(idx) {
				continue
			}
			if skip > 0 {
				skip--
				continue
			}
			out = append(out, db.idFromIdxNoLock(idx))
			if !needAll {
				need--
				if need == 0 {
					break
				}
			}
		}
		return out, nil
	}

	// case 2: ordering with negative result:
	// materialize the negative set into a positive one (universe AND NOT result)
	if len(q.Order) > 0 && result.neg {
		mat := getRoaringBuf()
		mat.Or(db.universe)
		if result.bm != nil {
			mat.AndNot(result.bm)
		}
		result.release()
		result = bitmap{bm: mat}
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

		isSliceOrderField := false
		if fm := db.fields[order.Field]; fm != nil && fm.Slice {
			isSliceOrderField = true
		}

		var seen *roaring64.Bitmap
		if isSliceOrderField {
			seen = getRoaringBuf()
			defer releaseRoaringBuf(seen)
		}

		tmp := getRoaringBuf()
		defer releaseRoaringBuf(tmp)

		out := makeOutSlice[K](result.bm, need)

		if db.strkey {
			db.strmap.RLock()
			defer db.strmap.RUnlock()
		}

		if !order.Desc {

		LOOP_ASC:
			for _, ix := range *slice {
				// fast path: if the bucket is small, avoid expensive bitmap operations
				// (iterating and checking Contains() is faster than Or/And for small sets)
				if card := ix.IDs.GetCardinality(); card <= iteratorThreshold || (0 < need && need < 1000) {
					if card == 0 {
						continue
					}

					// fast path for unique keys
					if card == 1 {
						idx := ix.IDs.Minimum()
						if !result.bm.Contains(idx) {
							continue
						}
						if seen != nil {
							if seen.Contains(idx) {
								continue
							}
							seen.Add(idx)
						}
						if skip > 0 {
							skip--
							continue
						}
						out = append(out, db.idFromIdxNoLock(idx))
						if !needAll {
							need--
							if need == 0 {
								break LOOP_ASC
							}
						}
						continue
					}

					iter := ix.IDs.Iterator()
					for iter.HasNext() {
						idx := iter.Next()
						if !result.bm.Contains(idx) {
							continue
						}
						if seen != nil {
							if seen.Contains(idx) {
								continue
							}
							seen.Add(idx)
						}
						if skip > 0 {
							skip--
							continue
						}
						out = append(out, db.idFromIdxNoLock(idx))
						if !needAll {
							need--
							if need == 0 {
								break LOOP_ASC
							}
						}
					}
					continue
				}

				// slow path: large buckets require full bitmap intersection
				tmpORSmallestAND(tmp, ix.IDs, result.bm)
				// tmp.Clear() // tmp.Xor(tmp)
				// tmp.Or(ix.IDs)
				// tmp.And(result.bm)
				if tmp.IsEmpty() {
					continue
				}
				iter := tmp.Iterator()
				for iter.HasNext() {
					idx := iter.Next()
					if seen != nil {
						if seen.Contains(idx) {
							continue
						}
						seen.Add(idx)
					}
					if skip > 0 {
						skip--
						continue
					}
					out = append(out, db.idFromIdxNoLock(idx))
					if !needAll {
						need--
						if need == 0 {
							break LOOP_ASC
						}
					}
				}
			}

		} else { // DESC

			s := *slice
			l := len(*slice)
		LOOP_DESC:
			for i := l - 1; i >= 0; i-- {
				ix := s[i]

				// fast path (same as ASC)
				if card := ix.IDs.GetCardinality(); card <= iteratorThreshold || (0 < need && need < 1000) {
					if card == 0 {
						continue
					}

					if card == 1 {
						idx := ix.IDs.Minimum()
						if !result.bm.Contains(idx) {
							continue
						}
						if seen != nil {
							if seen.Contains(idx) {
								continue
							}
							seen.Add(idx)
						}
						if skip > 0 {
							skip--
							continue
						}
						out = append(out, db.idFromIdxNoLock(idx))
						if !needAll {
							need--
							if need == 0 {
								break LOOP_DESC
							}
						}
						continue
					}

					iter := ix.IDs.Iterator()
					for iter.HasNext() {
						idx := iter.Next()
						if !result.bm.Contains(idx) {
							continue
						}
						if seen != nil {
							if seen.Contains(idx) {
								continue
							}
							seen.Add(idx)
						}
						if skip > 0 {
							skip--
							continue
						}
						out = append(out, db.idFromIdxNoLock(idx))
						if !needAll {
							need--
							if need == 0 {
								break LOOP_DESC
							}
						}
					}
					continue
				}

				// slow path
				tmpORSmallestAND(tmp, ix.IDs, result.bm)
				// tmp.Clear() // tmp.Xor(tmp)
				// tmp.Or(ix.IDs)
				// tmp.And(result.bm)
				if tmp.IsEmpty() {
					continue
				}
				iter := tmp.Iterator()
				for iter.HasNext() {
					idx := iter.Next()
					if seen != nil {
						if seen.Contains(idx) {
							continue
						}
						seen.Add(idx)
					}
					if skip > 0 {
						skip--
						continue
					}
					out = append(out, db.idFromIdxNoLock(idx))
					if !needAll {
						need--
						if need == 0 {
							break LOOP_DESC
						}
					}
				}
			}
		}

		return out, nil
	}

	// case 4: no ordering, positive result:
	// simply return the IDs from the result bitmap.

	if !db.strkey && needAll {
		ids := result.bm.ToArray()
		return unsafe.Slice((*K)(unsafe.Pointer(&ids[0])), len(ids)), nil
	}

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	out := makeOutSlice[K](result.bm, need)

	iter := result.bm.Iterator()
	for iter.HasNext() {
		idx := iter.Next()
		if skip > 0 {
			skip--
			continue
		}
		out = append(out, db.idFromIdxNoLock(idx))
		if !needAll {
			need--
			if need == 0 {
				return out, nil
			}
		}
	}
	return out, nil
}

func tmpORSmallestAND(tmp, a, b *roaring64.Bitmap) {
	tmp.Xor(tmp)
	if a.GetCardinality() < b.GetCardinality() {
		tmp.Or(a)
		tmp.And(b)
	} else {
		tmp.Or(b)
		tmp.And(a)
	}
}

func makeOutSlice[K ~int64 | ~uint64 | ~string](result *roaring64.Bitmap, limit uint64) []K {
	var out []K
	if limit > 0 {
		out = make([]K, 0, min(limit, result.GetCardinality()))
	} else {
		out = make([]K, 0, result.GetCardinality())
	}
	return out
}

func (db *DB[K, V]) queryOrderArrayPos(result bitmap, s *[]index, o qx.Order, skip, need uint64, all bool) ([]K, error) {

	vals, _, err := db.exprValueToIdx(qx.Expr{Op: qx.OpIN, Value: o.Data})
	if err != nil {
		return nil, err
	}

	if result.readonly {
		result = result.clone()
	}

	out := makeOutSlice[K](result.bm, need)

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	tmp := getRoaringBuf()
	defer releaseRoaringBuf(tmp)

	doValue := func(key string) error {
		bm := findIndex(s, key)
		if bm == nil {
			return nil
		}

		// tmp.Clear()
		tmp.Xor(tmp)
		tmp.Or(bm)
		tmp.And(result.bm)
		if tmp.IsEmpty() {
			return nil
		}

		iter := tmp.Iterator()
		for iter.HasNext() {
			idx := iter.Next()

			result.bm.Remove(idx)

			if skip > 0 {
				skip--
				continue
			}

			out = append(out, db.idFromIdxNoLock(idx))

			if !all {
				need--
				if need == 0 {
					return io.EOF // stop signal
				}
			}
		}
		return nil
	}

	if !o.Desc {
		for _, v := range vals {
			if err = doValue(v); err == io.EOF {
				return out, nil
			} else if err != nil {
				return nil, err
			}
		}
	} else {
		for i := len(vals) - 1; i >= 0; i-- {
			if err = doValue(vals[i]); err == io.EOF {
				return out, nil
			} else if err != nil {
				return nil, err
			}
		}
	}

	// process remaining items (those not in the priority list)
	iter := result.bm.Iterator()
	for iter.HasNext() {
		idx := iter.Next()

		if skip > 0 {
			skip--
			continue
		}

		out = append(out, db.idFromIdxNoLock(idx))

		if !all {
			need--
			if need == 0 {
				break
			}
		}
	}

	return out, nil
}

func (db *DB[K, V]) queryOrderArrayCount(result bitmap, s []index, o qx.Order, skip, need uint64, all bool) ([]K, error) {

	out := makeOutSlice[K](result.bm, need)

	if result.readonly {
		result = result.clone()
	}

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	tmp := getRoaringBuf()
	defer releaseRoaringBuf(tmp)

	if !o.Desc {

		for _, ix := range s {
			// tmp.Clear() // tmp.Xor(tmp)
			// tmp.Or(ix.IDs)
			// tmp.And(result.bm)
			tmpORSmallestAND(tmp, ix.IDs, result.bm)

			if tmp.IsEmpty() {
				continue
			}

			iter := tmp.Iterator()
			for iter.HasNext() {
				idx := iter.Next()

				if skip > 0 {
					skip--
					continue
				}
				out = append(out, db.idFromIdxNoLock(idx))

				if !all {
					need--
					if need == 0 {
						return out, nil
					}
				}
			}
		}

	} else {

		for i := len(s) - 1; i >= 0; i-- {
			ix := s[i]

			// tmp.Clear() // tmp.Xor(tmp)
			// tmp.Or(ix.IDs)
			// tmp.And(result.bm)
			tmpORSmallestAND(tmp, ix.IDs, result.bm)

			if tmp.IsEmpty() {
				continue
			}

			iter := tmp.Iterator()
			for iter.HasNext() {
				idx := iter.Next()

				if skip > 0 {
					skip--
					continue
				}

				out = append(out, db.idFromIdxNoLock(idx))

				if !all {
					need--
					if need == 0 {
						return out, nil
					}
				}
			}
		}
	}

	return out, nil
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

// QueryBitmap evaluates expression against the index and returns a bitmap of matching record IDs.
// The caller is free to modify the returned bitmap.
/*func (db *DB[K, V]) QueryBitmap(expr qx.Expr) (*roaring64.Bitmap, error) {

	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed.Load() {
		return nil, ErrClosed
	}
	if db.noIndex.Load() {
		return nil, ErrIndexDisabled
	}

	if err := db.checkUsed(expr); err != nil {
		return nil, err
	}

	b, err := db.evalExpr(expr)
	if err != nil {
		return nil, err
	}
	if b.bm == nil {
		return roaring64.NewBitmap(), nil
	}
	defer b.release()

	return b.bm.Clone(), nil
}*/

func (db *DB[K, V]) checkUsedFields(q *qx.QX) error {
	for _, o := range q.Order {
		if _, ok := db.fields[o.Field]; !ok {
			return fmt.Errorf("no index for field: %v", o.Field)
		}
	}
	return db.checkUsed(q.Expr)
}

func (db *DB[K, V]) checkUsed(exp qx.Expr) error {
	if exp.Field != "" {
		if _, ok := db.fields[exp.Field]; !ok {
			return fmt.Errorf("no index for field: %v", exp.Field)
		}
	}
	for _, op := range exp.Operands {
		if err := db.checkUsed(op); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB[K, V]) evalExpr(e qx.Expr) (bitmap, error) {
	switch e.Op {

	case qx.OpNOOP:
		if e.Field != "" || e.Value != nil || len(e.Operands) != 0 {
			return bitmap{}, fmt.Errorf("%w: invalid expression, op: %v", ErrInvalidQuery, e.Op)
		}
		res := bitmap{neg: true}
		if e.Not {
			res.neg = !res.neg
		}
		return res, nil

	case qx.OpAND:
		if len(e.Operands) == 0 {
			return bitmap{}, fmt.Errorf("%w: empty AND expression", ErrInvalidQuery)
		}

		var (
			first    bitmap
			hasFirst bool
		)

		for _, op := range e.Operands {
			b, err := db.evalExpr(op)
			if err != nil {
				if hasFirst {
					first.release()
				}
				return bitmap{}, err
			}

			// short-circuit: if empty non-neg set is found, the result is empty
			if !b.neg && (b.bm == nil || b.bm.IsEmpty()) {
				b.release()
				if hasFirst {
					first.release()
				}
				return bitmap{}, nil
			}

			if !hasFirst {
				first = b
				hasFirst = true
				continue
			}

			first, err = db.andBitmap(first, b)
			if err != nil {
				first.release()
				return bitmap{}, err
			}

			if !first.neg && (first.bm == nil || first.bm.IsEmpty()) {
				first.release()
				return bitmap{}, nil
			}
		}

		if e.Not {
			first.neg = !first.neg
		}
		return first, nil

	case qx.OpOR:
		if len(e.Operands) == 0 {
			return bitmap{}, fmt.Errorf("%w: empty OR expression", ErrInvalidQuery)
		}

		var (
			positives []*roaring64.Bitmap
			negatives []bitmap
		)

		for _, op := range e.Operands {
			b, err := db.evalExpr(op)
			if err != nil {
				for _, n := range negatives {
					n.release()
				}
				return bitmap{}, err
			}

			// short-circuit: universe OR ... == universe
			if b.neg && (b.bm == nil || b.bm.IsEmpty()) {
				b.release()
				for _, n := range negatives {
					n.release()
				}
				res := bitmap{neg: true}
				if e.Not {
					res.neg = !res.neg
				}
				return res, nil
			}

			if b.neg {
				negatives = append(negatives, b)
				continue
			}

			if b.bm == nil || b.bm.IsEmpty() {
				b.release()
				continue
			}

			positives = append(positives, b.bm)
		}

		// 1. merge all positives using batched parallel union
		var resultBm *roaring64.Bitmap
		if len(positives) > 0 {
			resultBm = db.unionBitmaps(positives)
		} else {
			resultBm = getRoaringBuf()
		}
		res := bitmap{bm: resultBm}

		// 2. merge negatives (slow path: A OR NOT B)
		for _, neg := range negatives {
			var err error
			res, err = db.orBitmap(res, neg)
			if err != nil {
				res.release()
				return bitmap{}, err
			}
		}

		if e.Not {
			res.neg = !res.neg
		}
		return res, nil

	default:
		res, err := db.evalSimple(e)
		if err != nil {
			return bitmap{}, err
		}
		if e.Not {
			res.neg = !res.neg
		}
		return res, nil
	}
}

var FlagRangeAlternativeOR = true

func (db *DB[K, V]) evalSimple(e qx.Expr) (bitmap, error) {
	slice := db.index[e.Field]
	if slice == nil {
		return bitmap{}, fmt.Errorf("no index for field: %v", e.Field)
	}

	f := db.fields[e.Field]
	if f == nil {
		return bitmap{}, fmt.Errorf("no metadata for field: %v", e.Field)
	}

	vals, isSlice, err := db.exprValueToIdx(e)
	if err != nil {
		return bitmap{}, err
	}

	var bitmaps []*roaring64.Bitmap

	switch e.Op {
	case qx.OpEQ:
		if !f.Slice {
			if isSlice {
				return bitmap{}, fmt.Errorf("%w: %v expects a single value for scalar field %v", ErrInvalidQuery, e.Op, e.Field)
			}
		} else {
			if !isSlice {
				return bitmap{}, fmt.Errorf("%w: %v expects a slice for slice field %v", ErrInvalidQuery, e.Op, e.Field)
			}
			return db.evalSliceEQ(e.Field, vals)
		}
		if bm := findIndex(slice, vals[0]); bm != nil {
			return bitmap{bm: bm, readonly: true}, nil
		}
		return bitmap{bm: getRoaringBuf()}, nil

	case qx.OpIN:
		if f.Slice {
			return bitmap{}, fmt.Errorf("%w: %v not supported on slice field %v", ErrInvalidQuery, e.Op, e.Field)
		}
		if !isSlice && e.Value != nil {
			return bitmap{}, fmt.Errorf("%w: %v expects a slice", ErrInvalidQuery, e.Op)
		}
		if len(vals) == 0 {
			return bitmap{}, fmt.Errorf("%v: %v: no values provided", ErrInvalidQuery, e.Op)
		}
		for _, v := range vals {
			if bm := findIndex(slice, v); bm != nil {
				bitmaps = append(bitmaps, bm)
			}
		}

	case qx.OpHASANY, qx.OpHAS:
		if !f.Slice {
			return bitmap{}, fmt.Errorf("%w: %v not supported on non-slice field %v", ErrInvalidQuery, e.Op, e.Field)
		}
		if !isSlice && e.Value != nil {
			return bitmap{}, fmt.Errorf("%w: %v expects a slice", ErrInvalidQuery, e.Op)
		}
		if len(vals) == 0 {
			return bitmap{}, fmt.Errorf("%v: %v: no values provided", ErrInvalidQuery, e.Op)
		}

		if e.Op == qx.OpHAS {
			// HAS - AND logic
			var andBitmaps []*roaring64.Bitmap
			for _, v := range vals {
				bm := findIndex(slice, v)
				if bm == nil {
					// if any value is missing, result is empty
					return bitmap{bm: getRoaringBuf()}, nil
				}
				andBitmaps = append(andBitmaps, bm)
			}
			return db.mergeAnd(andBitmaps)
		}

		// HASANY - OR logic
		for _, v := range vals {
			if bm := findIndex(slice, v); bm != nil {
				bitmaps = append(bitmaps, bm)
			}
		}

	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
		if len(vals) != 1 {
			return bitmap{}, fmt.Errorf("%w: %v expects a single value", ErrInvalidQuery, e.Op)
		}

		s := *slice
		key := vals[0]

		lo, hi := 0, len(s)
		for lo < hi {
			mid := (lo + hi) >> 1
			if s[mid].Key < key {
				lo = mid + 1
			} else {
				hi = mid
			}
		}

		if FlagRangeAlternativeOR { // && e.Op != qx.OpPREFIX {
			start := 0
			end := len(s)

			switch e.Op {
			case qx.OpGT:
				start = lo
				if start < len(s) && s[start].Key == key {
					start++
				}
				end = len(s)
			case qx.OpGTE:
				start = lo
				end = len(s)
			case qx.OpLT:
				start = 0
				end = lo
			case qx.OpLTE:
				start = 0
				end = lo
				if lo < len(s) && s[lo].Key == key {
					end = lo + 1
				}
			case qx.OpPREFIX:
				start = lo
				end = lo
				for end < len(s) {
					if !strings.HasPrefix(s[end].Key, key) {
						break
					}
					end++
				}
			}
			res := db.unionIndexRange(e.Field, start, end)
			return bitmap{bm: res}, nil
		}

		switch e.Op {
		case qx.OpGT:
			start := lo
			if start < len(s) && s[start].Key == key {
				start++
			}
			for i := start; i < len(s); i++ {
				bitmaps = append(bitmaps, s[i].IDs)
			}
		case qx.OpGTE:
			for i := lo; i < len(s); i++ {
				bitmaps = append(bitmaps, s[i].IDs)
			}
		case qx.OpLT:
			for i := 0; i < lo; i++ {
				bitmaps = append(bitmaps, s[i].IDs)
			}
		case qx.OpLTE:
			for i := 0; i < lo; i++ {
				bitmaps = append(bitmaps, s[i].IDs)
			}
			if lo < len(s) && s[lo].Key == key {
				bitmaps = append(bitmaps, s[lo].IDs)
			}
		case qx.OpPREFIX:
			for i := lo; i < len(s); i++ {
				if !strings.HasPrefix(s[i].Key, key) {
					break
				}
				bitmaps = append(bitmaps, s[i].IDs)
			}
		}

	case qx.OpSUFFIX, qx.OpCONTAINS:
		if len(vals) != 1 {
			return bitmap{}, fmt.Errorf("%w: %v expects a single string value", ErrInvalidQuery, e.Op)
		}
		s := *slice
		v := vals[0]
		for i := range s {
			match := false
			if e.Op == qx.OpSUFFIX {
				match = strings.HasSuffix(s[i].Key, v)
			} else {
				match = strings.Contains(s[i].Key, v)
			}
			if match {
				bitmaps = append(bitmaps, s[i].IDs)
			}
		}

	default:
		return bitmap{}, fmt.Errorf("unsupported op: %v", e.Op)
	}

	if len(bitmaps) == 0 {
		return bitmap{bm: getRoaringBuf()}, nil
	}
	if len(bitmaps) == 1 {
		return bitmap{bm: bitmaps[0], readonly: true}, nil
	}

	res := db.unionBitmaps(bitmaps)
	return bitmap{bm: res}, nil
}

// unionIndexRange merges bitmaps between start and end for a single field
func (db *DB[K, V]) unionIndexRange(field string, start, end int) *roaring64.Bitmap {
	slice := db.index[field]
	if slice == nil || start >= end {
		return getRoaringBuf()
	}
	s := *slice

	if start < 0 {
		start = 0
	}
	if end > len(s) {
		end = len(s)
	}
	if start >= end {
		return getRoaringBuf()
	}

	n := end - start

	if n >= 256 {
		return db.parallelUnionIndexRange(s, start, end)
	}

	best := -1
	var bestCard uint64
	for i := start; i < end; i++ {
		c := s[i].IDs.GetCardinality()
		if c > bestCard {
			bestCard = c
			best = i
		}
	}

	res := getRoaringBuf()
	if best == -1 || bestCard == 0 {
		return res
	}
	res.Or(s[best].IDs)
	for i := start; i < end; i++ {
		if i == best {
			continue
		}
		res.Or(s[i].IDs)
	}
	return res
}

func (db *DB[K, V]) parallelUnionIndexRange(s []index, start, end int) *roaring64.Bitmap {
	n := end - start

	workers := 8
	if n < workers*2 {
		workers = n / 2
	}
	if workers < 2 {
		// fallback
		res := getRoaringBuf()
		for i := start; i < end; i++ {
			res.Or(s[i].IDs)
		}
		return res
	}

	chunk := (n + workers - 1) / workers
	results := make([]*roaring64.Bitmap, workers)
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		a := start + w*chunk
		if a >= end {
			break
		}
		b := a + chunk
		if b > end {
			b = end
		}

		wg.Add(1)
		go func(idx, lo, hi int) {
			defer wg.Done()

			best := -1
			var bestCard uint64
			for i := lo; i < hi; i++ {
				c := s[i].IDs.GetCardinality()
				if c > bestCard {
					bestCard = c
					best = i
				}
			}

			r := getRoaringBuf()
			if best != -1 && bestCard != 0 {
				r.Or(s[best].IDs)
				for i := lo; i < hi; i++ {
					if i == best {
						continue
					}
					r.Or(s[i].IDs)
				}
			}
			results[idx] = r
		}(w, a, b)
	}

	wg.Wait()

	final := results[0]
	if final == nil {
		final = getRoaringBuf()
	}
	for i := 1; i < len(results); i++ {
		if results[i] == nil {
			continue
		}
		final.Or(results[i])
		releaseRoaringBuf(results[i])
	}
	return final
}

// unionBitmaps merges multiple bitmaps using an adaptive strategy:
// linear merge for small counts, batched parallel merge for large counts.
func (db *DB[K, V]) unionBitmaps(bitmaps []*roaring64.Bitmap) *roaring64.Bitmap {
	n := len(bitmaps)
	switch n {
	case 0:
		return getRoaringBuf()
	case 1:
		res := getRoaringBuf()
		res.Or(bitmaps[0])
		return res
	}

	// if few bitmaps, linear is faster/cheaper than goroutine overhead
	if n < 256 {
		return db.linearOr(bitmaps)
	}

	return db.parallelBatchedOr(bitmaps)
}

func (db *DB[K, V]) linearOr(bitmaps []*roaring64.Bitmap) *roaring64.Bitmap {

	bestIdx := 0
	maxCard := bitmaps[0].GetCardinality()

	for i := 1; i < len(bitmaps); i++ {
		c := bitmaps[i].GetCardinality()
		if c > maxCard {
			maxCard = c
			bestIdx = i
		}
	}

	res := getRoaringBuf()
	res.Or(bitmaps[bestIdx])

	for i, bm := range bitmaps {
		if i == bestIdx {
			continue
		}
		res.Or(bm)
	}
	return res
}

func (db *DB[K, V]) parallelBatchedOr(bitmaps []*roaring64.Bitmap) *roaring64.Bitmap {

	n := len(bitmaps)

	workers := 8
	if n < workers*2 {
		workers = n / 2
	}
	if workers < 2 {
		return db.linearOr(bitmaps)
	}

	chunkSize := (n + workers - 1) / workers
	results := make([]*roaring64.Bitmap, workers)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		start := i * chunkSize
		if start >= n {
			break
		}
		end := start + chunkSize
		if end > n {
			end = n
		}

		wg.Add(1)
		go func(idx int, part []*roaring64.Bitmap) {
			defer wg.Done()
			results[idx] = db.linearOr(part)
		}(i, bitmaps[start:end])
	}

	wg.Wait()

	final := results[0]
	if final == nil {
		final = getRoaringBuf()
	}

	for i := 1; i < len(results); i++ {
		if results[i] != nil {
			final.Or(results[i])
			releaseRoaringBuf(results[i])
		}
	}

	return final
}

func (db *DB[K, V]) mergeAnd(bitmaps []*roaring64.Bitmap) (bitmap, error) {

	if len(bitmaps) == 0 {
		return bitmap{bm: getRoaringBuf()}, nil
	}

	smallest := bitmaps[0]
	cardinality := smallest.GetCardinality()

	for _, b := range bitmaps[1:] {
		if c := b.GetCardinality(); c < cardinality {
			smallest = b
			cardinality = c
		}
	}

	res := getRoaringBuf()
	res.Or(smallest)

	for _, b := range bitmaps {
		if b != smallest {
			res.And(b)
			if res.IsEmpty() {
				break
			}
		}
	}
	return bitmap{bm: res}, nil
}

func (db *DB[K, V]) evalSliceEQ(field string, vals []string) (bitmap, error) {

	vals = dedupStringsInplace(vals)

	lenSlice := db.lenIndex[field]
	if lenSlice == nil {
		return bitmap{}, fmt.Errorf("no lenIndex for slice field: %v", field)
	}

	lenBM := findIndex(lenSlice, uint64ByteStr(uint64(len(vals))))

	if lenBM == nil || lenBM.IsEmpty() {
		return bitmap{bm: getRoaringBuf()}, nil
	}

	slice := db.index[field]

	var bitmaps []*roaring64.Bitmap
	bitmaps = append(bitmaps, lenBM)

	for _, v := range vals {
		bm := findIndex(slice, v)
		if bm == nil {
			return bitmap{bm: getRoaringBuf()}, nil
		}
		bitmaps = append(bitmaps, bm)
	}

	return db.mergeAnd(bitmaps)
}

func (db *DB[K, V]) diffBitmap(acc, sub bitmap) (bitmap, error) {
	if !acc.readonly {
		acc.bm.AndNot(sub.bm)
		sub.release()
		return acc, nil
	}

	res := acc.clone()
	res.bm.AndNot(sub.bm)

	acc.release()
	sub.release()
	return res, nil
}

func (db *DB[K, V]) exprValueToIdx(expr qx.Expr) ([]string, bool, error) {

	if expr.Value == nil {
		switch expr.Op {
		case qx.OpIN, qx.OpHAS, qx.OpHASANY:
			return nil, false, nil
		default:
			return []string{nilValue}, false, nil
		}
	}

	v := reflect.ValueOf(expr.Value)

	for {
		switch v.Kind() {
		case reflect.Interface:
			if v.IsNil() {
				return []string{nilValue}, false, nil
			}
			v = v.Elem()
			continue
		case reflect.Pointer:
			if v.IsNil() {
				return []string{nilValue}, false, nil
			}
			v = v.Elem()
			continue
		}
		break
	}

	if v.Kind() == reflect.Slice {

		ixs := make([]string, v.Len())
		etype := v.Type().Elem()

		switch etype.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			for i := 0; i < len(ixs); i++ {
				ixs[i] = int64ByteStr(v.Index(i).Int())
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			for i := 0; i < len(ixs); i++ {
				ixs[i] = uint64ByteStr(v.Index(i).Uint())
			}
		case reflect.Float32, reflect.Float64:
			for i := 0; i < len(ixs); i++ {
				ixs[i] = float64ByteStr(v.Index(i).Float())
			}
		case reflect.String:
			for i := 0; i < len(ixs); i++ {
				ixs[i] = v.Index(i).String()
			}
		case reflect.Bool:
			for i := 0; i < len(ixs); i++ {
				if v.Index(i).Bool() {
					ixs[i] = "1"
				} else {
					ixs[i] = "0"
				}
			}
		default:
			if etype.Implements(viType) {
				for i := 0; i < len(ixs); i++ {
					ixs[i] = v.Index(i).Interface().(ValueIndexer).IndexingValue()
				}
			} else {
				return nil, true, fmt.Errorf("unsupported slice element type: %v", etype)
			}
		}
		return ixs, true, nil
	}

	ixs := make([]string, 1)

	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		ixs[0] = int64ByteStr(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		ixs[0] = uint64ByteStr(v.Uint())
	case reflect.Float32, reflect.Float64:
		ixs[0] = float64ByteStr(v.Float())
	case reflect.String:
		ixs[0] = v.String()
	case reflect.Bool:
		if v.Bool() {
			ixs[0] = "1"
		} else {
			ixs[0] = "0"
		}
	default:
		if vi, ok := expr.Value.(ValueIndexer); ok {
			ixs[0] = vi.IndexingValue()
		} else {
			return nil, false, fmt.Errorf("unsupported value type: %v", v.Type())
		}
	}

	return ixs, false, nil
}

type bitmap struct {
	bm       *roaring64.Bitmap
	readonly bool
	neg      bool
}

func (b bitmap) release() {
	if !b.readonly && b.bm != nil {
		releaseRoaringBuf(b.bm)
	}
}

func (b bitmap) clone() bitmap {
	if b.bm == nil {
		return b
	}
	c := getRoaringBuf()
	c.Or(b.bm)
	return bitmap{bm: c, neg: b.neg, readonly: false}
}

func (db *DB[K, V]) cardOf(b bitmap) uint64 {
	if !b.neg {
		if b.bm == nil {
			return 0
		}
		return b.bm.GetCardinality()
	}
	uc := db.universe.GetCardinality()
	if b.bm == nil {
		return uc
	}
	ex := b.bm.GetCardinality()
	if ex >= uc {
		return 0
	}
	return uc - ex
}

func diffOwned(a, b *roaring64.Bitmap) *roaring64.Bitmap {
	res := getRoaringBuf()
	if a != nil {
		res.Or(a)
	}
	if b != nil && !b.IsEmpty() {
		res.AndNot(b)
	}
	return res
}

func (db *DB[K, V]) andBitmap(a, b bitmap) (bitmap, error) {
	// handle empty sets
	if !a.neg && (a.bm == nil || a.bm.IsEmpty()) {
		a.release()
		b.release()
		return bitmap{}, nil
	}
	if !b.neg && (b.bm == nil || b.bm.IsEmpty()) {
		a.release()
		b.release()
		return bitmap{}, nil
	}

	switch {

	case !a.neg && !b.neg:

		// A AND B (intersection)
		// Reuse mutable bitmap if possible.
		// If both readonly: clone the smallest one to minimize allocations

		var res bitmap

		if !a.readonly {
			a.bm.And(b.bm)
			b.release()
			return a, nil
		}
		if !b.readonly {
			b.bm.And(a.bm)
			a.release()
			return b, nil
		}

		// both are readonly, clone the smallest
		cardA := a.bm.GetCardinality()
		cardB := b.bm.GetCardinality()

		if cardA < cardB {
			res = a.clone()
			res.bm.And(b.bm)
		} else {
			res = b.clone()
			res.bm.And(a.bm)
		}

		a.release()
		b.release()
		return res, nil

	case a.neg && b.neg:

		// NOT A AND NOT B == NOT (A OR B)
		// merge A and B, return as negative

		if !a.readonly {
			if b.bm != nil {
				a.bm.Or(b.bm)
			}
			b.release()
			a.neg = true
			return a, nil
		}
		if !b.readonly {
			if a.bm != nil {
				b.bm.Or(a.bm)
			}
			a.release()
			b.neg = true
			return b, nil
		}

		u := getRoaringBuf()
		if a.bm != nil {
			u.Or(a.bm)
		}
		if b.bm != nil {
			u.Or(b.bm)
		}

		a.release()
		b.release()
		return bitmap{bm: u, neg: true}, nil

	case a.neg && !b.neg:
		// NOT A AND B ==  B \ A
		return db.diffBitmap(b, a)

	case !a.neg && b.neg:
		// A AND NOT B  ->  A \ B
		return db.diffBitmap(a, b)
	}

	return bitmap{}, nil
}

func (db *DB[K, V]) orBitmap(a, b bitmap) (bitmap, error) {
	// universe (negative empty)
	if a.neg && (a.bm == nil || a.bm.IsEmpty()) {
		a.release()
		b.release()
		return bitmap{neg: true}, nil
	}
	if b.neg && (b.bm == nil || b.bm.IsEmpty()) {
		a.release()
		b.release()
		return bitmap{neg: true}, nil
	}

	// standard empty
	if !a.neg && (a.bm == nil || a.bm.IsEmpty()) {
		a.release()
		return b, nil
	}
	if !b.neg && (b.bm == nil || b.bm.IsEmpty()) {
		b.release()
		return a, nil
	}

	switch {

	case !a.neg && !b.neg:

		// A OR B (union)
		// Reuse mutable. If both readonly: clone largest to minimize resizing

		if !a.readonly {
			a.bm.Or(b.bm)
			b.release()
			return a, nil
		}
		if !b.readonly {
			b.bm.Or(a.bm)
			a.release()
			return b, nil
		}

		cardA := a.bm.GetCardinality()
		cardB := b.bm.GetCardinality()

		var res bitmap
		if cardA >= cardB {
			res = a.clone()
			res.bm.Or(b.bm)
		} else {
			res = b.clone()
			res.bm.Or(a.bm)
		}
		a.release()
		b.release()
		return res, nil

	case a.neg && b.neg:

		// NOT A OR NOT B == NOT (A AND B)
		// Intersect internals

		var res *roaring64.Bitmap

		if !a.readonly {
			a.bm.And(b.bm)
			b.release()
			a.neg = true
			return a, nil
		}
		if !b.readonly {
			b.bm.And(a.bm)
			a.release()
			b.neg = true
			return b, nil
		}

		if a.bm.GetCardinality() < b.bm.GetCardinality() {
			res = getRoaringBuf()
			res.Or(a.bm)
			res.And(b.bm)
		} else {
			res = getRoaringBuf()
			res.Or(b.bm)
			res.And(a.bm)
		}
		a.release()
		b.release()
		return bitmap{bm: res, neg: true}, nil

	case a.neg && !b.neg:
		// NOT A OR B  ->  NOT (A \ B)
		payload := diffOwned(a.bm, b.bm)
		a.release()
		b.release()
		return bitmap{bm: payload, neg: true}, nil

	case !a.neg && b.neg:
		// A OR NOT B  ->  NOT (B \ A)
		payload := diffOwned(b.bm, a.bm)
		a.release()
		b.release()
		return bitmap{bm: payload, neg: true}, nil
	}

	return bitmap{}, nil
}

/**/

var roaringPool = sync.Pool{
	New: func() any { return roaring64.New() },
}

func getRoaringBuf() *roaring64.Bitmap {
	return roaringPool.Get().(*roaring64.Bitmap)
}

func releaseRoaringBuf(bm *roaring64.Bitmap) {
	// if releaseQue.enqueue(bm) {
	// 	return
	// }
	bm.Clear()
	// bm.Xor(bm)
	roaringPool.Put(bm)
}

/*
type cell struct {
	seq atomic.Uint64
	val atomic.Pointer[roaring64.Bitmap]
}

type ringQue struct {
	mask  uint64
	head  atomic.Uint64
	tail  atomic.Uint64
	cells []cell
}

const ringSizePow2 = 1 << 14

func newQue() *ringQue {
	r := &ringQue{
		mask:  uint64(ringSizePow2 - 1),
		cells: make([]cell, ringSizePow2),
	}
	for i := 0; i < ringSizePow2; i++ {
		r.cells[i].seq.Store(uint64(i))
	}
	return r
}

func (q *ringQue) enqueue(bm *roaring64.Bitmap) bool {
	for {
		pos := q.tail.Load()
		c := &q.cells[pos&q.mask]
		seq := c.seq.Load()
		dif := int64(seq) - int64(pos)
		if dif == 0 {
			if q.tail.CompareAndSwap(pos, pos+1) {
				c.val.Store(bm)
				c.seq.Store(pos + 1)
				return true
			}
			continue
		}
		if dif < 0 {
			return false
		}
	}
}

func (q *ringQue) dequeue() (*roaring64.Bitmap, bool) {
	for {
		pos := q.head.Load()
		c := &q.cells[pos&q.mask]
		seq := c.seq.Load()
		dif := int64(seq) - int64(pos+1)
		if dif == 0 {
			if q.head.CompareAndSwap(pos, pos+1) {
				bm := c.val.Swap(nil)
				c.seq.Store(pos + q.mask + 1)
				return bm, true
			}
			continue
		}
		if dif < 0 {
			return nil, false
		}
	}
}

var releaseQue = newQue()

func init() {
	go func() {
		t := time.NewTicker(50 * time.Millisecond)
		defer t.Stop()

		for range t.C {
			for {
				bm, ok := releaseQue.dequeue()
				if !ok {
					break
				}
				if bm != nil {
					bm.Xor(bm)
					// bm.Clear()
					roaringPool.Put(bm)
				}
			}
		}
	}()
}
*/
