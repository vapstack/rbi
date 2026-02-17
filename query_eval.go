package rbi

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/vapstack/qx"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

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

	var bitmaps []*roaring64.Bitmap

	switch e.Op {
	case qx.OpEQ:
		if !f.Slice {
			key, isSlice, err := db.exprValueToIdxScalar(e)
			if err != nil {
				return bitmap{}, err
			}
			if isSlice {
				return bitmap{}, fmt.Errorf("%w: %v expects a single value for scalar field %v", ErrInvalidQuery, e.Op, e.Field)
			}

			if bm := findIndex(slice, key); bm != nil {
				return bitmap{bm: bm, readonly: true}, nil
			}
			return bitmap{bm: getRoaringBuf()}, nil
		} else {
			vals, isSlice, err := db.exprValueToIdx(e)
			if err != nil {
				return bitmap{}, err
			}
			if !isSlice {
				return bitmap{}, fmt.Errorf("%w: %v expects a slice for slice field %v", ErrInvalidQuery, e.Op, e.Field)
			}
			return db.evalSliceEQ(e.Field, vals)
		}

	case qx.OpIN:
		if f.Slice {
			return bitmap{}, fmt.Errorf("%w: %v not supported on slice field %v", ErrInvalidQuery, e.Op, e.Field)
		}
		vals, isSlice, err := db.exprValueToIdx(e)
		if err != nil {
			return bitmap{}, err
		}
		if !isSlice && e.Value != nil {
			return bitmap{}, fmt.Errorf("%w: %v expects a slice", ErrInvalidQuery, e.Op)
		}
		if len(vals) == 0 {
			return bitmap{}, fmt.Errorf("%v: %v: no values provided", ErrInvalidQuery, e.Op)
		}
		bitmaps = make([]*roaring64.Bitmap, 0, len(vals))
		for _, v := range vals {
			if bm := findIndex(slice, v); bm != nil {
				bitmaps = append(bitmaps, bm)
			}
		}

	case qx.OpHASANY, qx.OpHAS:
		if !f.Slice {
			return bitmap{}, fmt.Errorf("%w: %v not supported on non-slice field %v", ErrInvalidQuery, e.Op, e.Field)
		}
		vals, isSlice, err := db.exprValueToIdx(e)
		if err != nil {
			return bitmap{}, err
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
		bitmaps = make([]*roaring64.Bitmap, 0, len(vals))
		for _, v := range vals {
			if bm := findIndex(slice, v); bm != nil {
				bitmaps = append(bitmaps, bm)
			}
		}

	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
		key, isSlice, err := db.exprValueToIdxScalar(e)
		if err != nil {
			return bitmap{}, err
		}
		if isSlice {
			return bitmap{}, fmt.Errorf("%w: %v expects a single value", ErrInvalidQuery, e.Op)
		}

		s := *slice

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
		v, isSlice, err := db.exprValueToIdxScalar(e)
		if err != nil {
			return bitmap{}, err
		}
		if isSlice {
			return bitmap{}, fmt.Errorf("%w: %v expects a single string value", ErrInvalidQuery, e.Op)
		}
		s := *slice
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

func unwrapExprValue(v reflect.Value) (reflect.Value, bool) {
	for {
		switch v.Kind() {
		case reflect.Interface, reflect.Pointer:
			if v.IsNil() {
				return v, true
			}
			v = v.Elem()
			continue
		}
		return v, false
	}
}

func scalarValueToIdx(raw any, v reflect.Value) (string, error) {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return int64ByteStr(v.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return uint64ByteStr(v.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return float64ByteStr(v.Float()), nil
	case reflect.String:
		return v.String(), nil
	case reflect.Bool:
		if v.Bool() {
			return "1", nil
		}
		return "0", nil
	default:
		if vi, ok := raw.(ValueIndexer); ok {
			return vi.IndexingValue(), nil
		}
		if v.IsValid() && v.CanInterface() {
			if vi, ok := v.Interface().(ValueIndexer); ok {
				return vi.IndexingValue(), nil
			}
		}
		return "", fmt.Errorf("unsupported value type: %v", v.Type())
	}
}

func (db *DB[K, V]) exprValueToIdxScalar(expr qx.Expr) (string, bool, error) {
	if expr.Value == nil {
		switch expr.Op {
		case qx.OpIN, qx.OpHAS, qx.OpHASANY:
			return "", false, nil
		default:
			return nilValue, false, nil
		}
	}

	v := reflect.ValueOf(expr.Value)
	v, isNil := unwrapExprValue(v)
	if isNil {
		return nilValue, false, nil
	}
	if v.Kind() == reflect.Slice {
		return "", true, nil
	}

	key, err := scalarValueToIdx(expr.Value, v)
	if err != nil {
		return "", false, err
	}
	return key, false, nil
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
	v, isNil := unwrapExprValue(v)
	if isNil {
		return []string{nilValue}, false, nil
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

	key, err := scalarValueToIdx(expr.Value, v)
	if err != nil {
		return nil, false, err
	}
	ixs := make([]string, 1)
	ixs[0] = key

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
