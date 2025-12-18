package rbi

import (
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
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

	for _, f := range q.UsedFields() {
		if _, ok := db.fields[f]; !ok {
			return nil, fmt.Errorf("no index for field: %v", f)
		}
	}
	result, err := db.evalExpr(q.Expr)
	if err != nil {
		return nil, err
	}
	if result == nil || result.IsEmpty() {
		if result != nil {
			releaseRoaringBuf(result)
		}
		return nil, nil
	}
	defer releaseRoaringBuf(result)

	skip := q.Offset
	needAll := q.Limit == 0
	need := q.Limit

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

		// what is the expected result of sorting by the slice field?...
		/*
			if f := db.fields[order.Field]; f != nil && f.Slice {
				return nil, fmt.Errorf("%w: cannot use By(\"%v\") on slice field; use ByArrayPos/ByArrayCount", ErrInvalidQuery, order.Field)
			}
		*/

		slice := db.index[order.Field]
		if slice == nil {
			return nil, fmt.Errorf("cannot sort non-indexed field: %v", order.Field)
		}

		seen := getRoaringBuf()
		defer releaseRoaringBuf(seen)

		tmp := getRoaringBuf()
		defer releaseRoaringBuf(tmp)

		out := makeOutSlice[K](result, need)

		if db.strkey {
			db.strmap.RLock()
			defer db.strmap.RUnlock()
		}

		if !order.Desc {

		LOOP_ASC:
			for _, ix := range *slice {
				tmp.Clear()
				tmp.Or(ix.IDs)
				tmp.And(result)
				if tmp.IsEmpty() {
					continue
				}
				iter := tmp.Iterator()
				for iter.HasNext() {
					idx := iter.Next()
					if seen.Contains(idx) {
						continue
					}
					seen.Add(idx)
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

		} else {

			s := *slice
			l := len(*slice)
		LOOP_DESC:
			for i := l - 1; i >= 0; i-- {
				ix := s[i]
				tmp.Clear()
				tmp.Or(ix.IDs)
				tmp.And(result)
				if tmp.IsEmpty() {
					continue
				}
				iter := tmp.Iterator()
				for iter.HasNext() {
					idx := iter.Next()
					if seen.Contains(idx) {
						continue
					}
					seen.Add(idx)
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

	if !db.strkey && needAll {
		ids := result.ToArray()
		return unsafe.Slice((*K)(unsafe.Pointer(&ids[0])), len(ids)), nil
	}

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	out := makeOutSlice[K](result, need)

	iter := result.Iterator()
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

func makeOutSlice[K ~int64 | ~uint64 | ~string](result *roaring64.Bitmap, limit uint64) []K {
	var out []K
	if limit > 0 {
		out = make([]K, 0, min(limit, result.GetCardinality()))
	} else {
		out = make([]K, 0, result.GetCardinality())
	}
	return out
}

func (db *DB[K, V]) queryOrderArrayPos(result *roaring64.Bitmap, s *[]index, o qx.Order, skip, need uint64, all bool) ([]K, error) {

	vals, _, err := db.exprValueToIdx(qx.Expr{Op: qx.OpIN, Value: o.Data})
	if err != nil {
		return nil, err
	}

	out := makeOutSlice[K](result, need)

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

		tmp.Clear()
		tmp.Or(bm)
		tmp.And(result)
		if tmp.IsEmpty() {
			return nil
		}

		iter := tmp.Iterator()
		for iter.HasNext() {
			idx := iter.Next()

			result.Remove(idx)

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

	iter := result.Iterator()
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

func (db *DB[K, V]) queryOrderArrayCount(result *roaring64.Bitmap, s []index, o qx.Order, skip, need uint64, all bool) ([]K, error) {

	out := makeOutSlice[K](result, need)

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	tmp := getRoaringBuf()
	defer releaseRoaringBuf(tmp)

	if !o.Desc {

		for _, ix := range s {
			tmp.Clear()
			tmp.Or(ix.IDs)
			tmp.And(result)

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

			tmp.Clear()
			tmp.Or(ix.IDs)
			tmp.And(result)

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

// Count evaluates the given query and returns the number of matching records.
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

	for _, f := range q.UsedFields() {
		if _, ok := db.fields[f]; !ok {
			return 0, fmt.Errorf("no index for field: %v", f)
		}
	}

	bm, err := db.evalExpr(q.Expr)
	if err != nil {
		return 0, err
	}
	if bm == nil {
		return 0, nil
	}
	defer releaseRoaringBuf(bm)

	return bm.GetCardinality(), nil
}

// QueryBitmap evaluates expression against the index and returns a bitmap of matching record IDs.
func (db *DB[K, V]) QueryBitmap(expr qx.Expr) (*roaring64.Bitmap, error) {

	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed.Load() {
		return nil, ErrClosed
	}
	if db.noIndex.Load() {
		return nil, ErrIndexDisabled
	}

	for _, f := range expr.UsedFields() {
		if _, ok := db.fields[f]; !ok {
			return nil, fmt.Errorf("no index for field: %v", f)
		}
	}

	bm, err := db.evalExpr(expr)
	if err != nil {
		return nil, err
	}
	if bm == nil {
		return roaring64.NewBitmap(), nil
	}
	defer releaseRoaringBuf(bm)

	result := bm.Clone()

	return result, nil
}

func (db *DB[K, V]) evalExpr(e qx.Expr) (*roaring64.Bitmap, error) {
	switch e.Op {

	case qx.OpNOOP: // weird but anyway
		if e.Field != "" || e.Value != nil || len(e.Operands) != 0 {
			return nil, fmt.Errorf("%w: invalid expression, op: %v", ErrInvalidQuery, e.Op)
		}
		if e.Not {
			return getRoaringBuf(), nil
		}
		res := getRoaringBuf()
		res.Or(db.universe)
		return res, nil

	case qx.OpAND:
		n := len(e.Operands)
		if n == 0 {
			return nil, fmt.Errorf("%w: empty AND expression", ErrInvalidQuery)
		}

		if n == 1 {
			bm, err := db.evalExpr(e.Operands[0])
			if err != nil {
				return nil, err
			}
			if e.Not {
				negated := getRoaringBuf()
				negated.Or(db.universe)
				negated.AndNot(bm)
				releaseRoaringBuf(bm)
				return negated, nil
			}
			return bm, nil
		}

		type bmCardinality struct {
			bm   *roaring64.Bitmap
			card uint64
		}
		bms := make([]bmCardinality, 0, n)

		for _, op := range e.Operands {
			bm, err := db.evalExpr(op)
			if err != nil {
				for _, bb := range bms {
					releaseRoaringBuf(bb.bm)
				}
				return nil, err
			}
			if bm == nil || bm.IsEmpty() {
				for _, bb := range bms {
					releaseRoaringBuf(bb.bm)
				}
				return getRoaringBuf(), nil
			}
			bms = append(bms, bmCardinality{bm: bm, card: bm.GetCardinality()})
		}

		sort.Slice(bms, func(i, j int) bool { return bms[i].card < bms[j].card })

		res := bms[0].bm
		for i := 1; i < len(bms); i++ {
			res.And(bms[i].bm)
			releaseRoaringBuf(bms[i].bm)
		}
		if e.Not {
			negated := getRoaringBuf()
			negated.Or(db.universe)
			negated.AndNot(res)
			releaseRoaringBuf(res)
			return negated, nil
		}
		return res, nil

	case qx.OpOR:
		if len(e.Operands) == 0 {
			return nil, fmt.Errorf("%w: empty OR expression", ErrInvalidQuery)
		}
		res := getRoaringBuf()

		for _, op := range e.Operands {
			bm, err := db.evalExpr(op)
			if err != nil {
				releaseRoaringBuf(res)
				return nil, err
			}
			if bm == nil {
				continue
			}
			res.Or(bm)
			releaseRoaringBuf(bm)
		}
		if e.Not {
			negated := getRoaringBuf()
			negated.Or(db.universe)
			negated.AndNot(res)
			releaseRoaringBuf(res)
			return negated, nil
		}
		return res, nil

	default:
		bm, err := db.evalSimple(e)
		if err != nil {
			return nil, err
		}
		if e.Not {
			negated := getRoaringBuf()
			negated.Or(db.universe)
			negated.AndNot(bm)
			releaseRoaringBuf(bm)
			return negated, nil
		}
		return bm, nil
	}
}

func (db *DB[K, V]) evalSimple(e qx.Expr) (*roaring64.Bitmap, error) {
	slice := db.index[e.Field]
	if slice == nil {
		return nil, fmt.Errorf("no index for field: %v", e.Field)
	}

	f := db.fields[e.Field]
	if f == nil {
		return nil, fmt.Errorf("no metadata for field: %v", e.Field)
	}

	vals, isSlice, err := db.exprValueToIdx(e)
	if err != nil {
		return nil, err
	}

	switch e.Op {

	case qx.OpEQ:
		if !f.Slice {
			if isSlice {
				return nil, fmt.Errorf("%w: %v expects a single value for scalar field %v", ErrInvalidQuery, e.Op, e.Field)
			}
		} else {
			if !isSlice {
				return nil, fmt.Errorf("%w: %v expects a slice for slice field %v", ErrInvalidQuery, e.Op, e.Field)
			}
			return db.evalSliceEQ(e.Field, vals)
		}
		result := getRoaringBuf()
		for _, v := range vals {
			if bm := findIndex(slice, v); bm != nil {
				result.Or(bm)
			}
		}
		return result, nil

	case qx.OpIN:
		if f.Slice {
			return nil, fmt.Errorf("%w: %v not supported on slice field %v", ErrInvalidQuery, e.Op, e.Field)
		}
		if !isSlice && e.Value != nil {
			return nil, fmt.Errorf("%w: %v expects a slice", ErrInvalidQuery, e.Op)
		}
		if len(vals) == 0 {
			return nil, fmt.Errorf("%v: %v: no values provided", ErrInvalidQuery, e.Op)
		}
		result := getRoaringBuf()
		roaring64.FastOr()
		for _, v := range vals {
			if bm := findIndex(slice, v); bm != nil {
				result.Or(bm)
			}
		}
		return result, nil

	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
		if len(vals) != 1 {
			return nil, fmt.Errorf("%w: %v expects a single value", ErrInvalidQuery, e.Op)
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

		result := getRoaringBuf()
		switch e.Op {
		case qx.OpGT:
			start := lo
			if start < len(s) && s[start].Key == key {
				start++
			}
			for i := start; i < len(s); i++ {
				result.Or(s[i].IDs)
			}

		case qx.OpGTE:
			for i := lo; i < len(s); i++ {
				result.Or(s[i].IDs)
			}

		case qx.OpLT:
			for i := 0; i < lo; i++ {
				result.Or(s[i].IDs)
			}

		case qx.OpLTE:
			for i := 0; i < lo; i++ {
				result.Or(s[i].IDs)
			}
			if lo < len(s) && s[lo].Key == key {
				result.Or(s[lo].IDs)
			}

		case qx.OpPREFIX:
			for i := lo; i < len(s); i++ {
				k := s[i].Key
				if !strings.HasPrefix(k, key) {
					break
				}
				result.Or(s[i].IDs)
			}
		}
		return result, nil

	case qx.OpHAS:
		if !f.Slice {
			return nil, fmt.Errorf("%w: %v not supported on non-slice field %v", ErrInvalidQuery, e.Op, e.Field)
		}
		if !isSlice && e.Value != nil {
			return nil, fmt.Errorf("%w: %v expects a slice", ErrInvalidQuery, e.Op)
		}
		if len(vals) == 0 {
			return nil, fmt.Errorf("%v: %v: no values provided", ErrInvalidQuery, e.Op)
		}
		result := getRoaringBuf()
		first := true
		for _, v := range vals {
			if bm := findIndex(slice, v); bm != nil {
				if first {
					result.Or(bm)
					first = false
				} else {
					result.And(bm)
				}
			} else {
				result.Clear()
				return result, nil
			}
		}
		return result, nil

	case qx.OpHASANY:
		if !f.Slice {
			return nil, fmt.Errorf("%w: %v not supported on non-slice field %v, use IN instead", ErrInvalidQuery, e.Op, e.Field)
		}
		if !isSlice && e.Value != nil {
			return nil, fmt.Errorf("%w: %v expects a slice", ErrInvalidQuery, e.Op)
		}
		if len(vals) == 0 {
			return nil, fmt.Errorf("%v: %v: no values provided", ErrInvalidQuery, e.Op)
		}
		result := getRoaringBuf()
		for _, v := range vals {
			if bm := findIndex(slice, v); bm != nil {
				result.Or(bm)
			}
		}
		return result, nil

	case qx.OpHASNONE:
		if !f.Slice {
			return nil, fmt.Errorf("%w: %v not supported on non-slice field %v, use NOTIN instead", ErrInvalidQuery, e.Op, e.Field)
		}
		if !isSlice && e.Value != nil {
			return nil, fmt.Errorf("%w: %v expects a slice", ErrInvalidQuery, e.Op)
		}
		result := getRoaringBuf()
		if len(vals) == 0 {
			result.Or(db.universe)
			return result, nil
		}
		if len(vals) == 1 {
			result.Or(db.universe)
			if bm := findIndex(slice, vals[0]); bm != nil {
				result.AndNot(bm)
			}
			return result, nil
		}

		tmp := getRoaringBuf()
		defer releaseRoaringBuf(tmp)

		for _, v := range vals {
			if bm := findIndex(slice, v); bm != nil {
				tmp.Or(bm)
			}
		}
		result.Or(db.universe)
		result.AndNot(tmp)
		return result, nil

	case qx.OpSUFFIX:
		if len(vals) != 1 {
			return nil, fmt.Errorf("%w: %v expects a single string value", ErrInvalidQuery, e.Op)
		}
		result := getRoaringBuf()
		suf := vals[0]
		for _, ix := range *slice { // very slow
			if strings.HasSuffix(ix.Key, suf) {
				result.Or(ix.IDs)
			}
		}
		return result, nil

	case qx.OpCONTAINS:
		if len(vals) != 1 {
			return nil, fmt.Errorf("%w: %v expects a single string value", ErrInvalidQuery, e.Op)
		}
		result := getRoaringBuf()
		sub := vals[0]
		for _, ix := range *slice { // very slow
			if strings.Contains(ix.Key, sub) {
				result.Or(ix.IDs)
			}
		}
		return result, nil

	default:
		return nil, fmt.Errorf("%w: unsupported op: %v", ErrInvalidQuery, e.Op)
	}
}

func (db *DB[K, V]) evalSliceEQ(field string, vals []string) (*roaring64.Bitmap, error) {

	vals = dedupStringsInplace(vals)

	slice := db.index[field]
	if slice == nil {
		return nil, fmt.Errorf("no index for field: %v", field)
	}

	lenSlice := db.lenIndex[field]
	if lenSlice == nil {
		return nil, fmt.Errorf("no lenIndex for slice field: %v", field)
	}

	lenBM := findIndex(lenSlice, uint64ByteStr(uint64(len(vals))))

	result := getRoaringBuf()

	if len(vals) == 0 {
		if lenBM != nil {
			result.Or(lenBM)
		}
		return result, nil
	}

	first := true
	for _, v := range vals {
		bm := findIndex(slice, v)
		if bm == nil {
			result.Clear()
			return result, nil
		}
		if first {
			result.Or(bm)
			first = false
		} else {
			result.And(bm)
			if result.IsEmpty() {
				return result, nil
			}
		}
	}

	if lenBM == nil {
		result.Clear()
		return result, nil
	}
	result.And(lenBM)
	return result, nil
}

var roaringPool = sync.Pool{
	New: func() any { return roaring64.New() },
}

func getRoaringBuf() *roaring64.Bitmap {
	return roaringPool.Get().(*roaring64.Bitmap)
}
func releaseRoaringBuf(rb *roaring64.Bitmap) {
	rb.Clear()
	roaringPool.Put(rb)
}

func (db *DB[K, V]) exprValueToIdx(expr qx.Expr) ([]string, bool, error) {

	if expr.Value == nil {
		switch expr.Op {
		case qx.OpIN, qx.OpHAS, qx.OpHASANY, qx.OpHASNONE:
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
