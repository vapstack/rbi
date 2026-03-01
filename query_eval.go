package rbi

import (
	"fmt"
	"reflect"
	"slices"
	"sync"

	"github.com/vapstack/qx"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

const (
	singleChunkCap       = 32768
	singleAdaptiveMaxLen = 200_000
)

var singleIDsPool = sync.Pool{
	New: func() any {
		return &singleIDsBuffer{
			values: make([]uint64, 0, singleChunkCap),
		}
	},
}

type singleIDsBuffer struct {
	values []uint64
}

func getSingleIDsBuf() *singleIDsBuffer {
	buf := singleIDsPool.Get().(*singleIDsBuffer)
	buf.values = buf.values[:0]
	return buf
}

func releaseSingleIDs(buf *singleIDsBuffer) {
	if buf == nil || cap(buf.values) != singleChunkCap {
		return
	}
	buf.values = buf.values[:0]
	singleIDsPool.Put(buf)
}

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

// evalExpr evaluates a boolean expression tree into a bitmap representation.
//
// Results may be positive sets or negative/complement sets to
// postpone expensive universe materialization until it is actually required.
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

			// Keep the accumulator in bitmap form to preserve short-circuiting and
			// avoid temporary key arrays in deep boolean trees.
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

		positivesBuf := getBitmapResultSliceBuf(len(e.Operands))
		positives := positivesBuf.values
		defer func() {
			positivesBuf.values = positives
			releaseBitmapResultSliceBuf(positivesBuf)
		}()

		negativesBuf := getBitmapResultSliceBuf(len(e.Operands))
		negatives := negativesBuf.values
		defer func() {
			negativesBuf.values = negatives
			releaseBitmapResultSliceBuf(negativesBuf)
		}()

		releaseAll := func(xs []bitmap) {
			for _, x := range xs {
				x.release()
			}
		}

		for _, op := range e.Operands {
			b, err := db.evalExpr(op)
			if err != nil {
				releaseAll(positives)
				releaseAll(negatives)
				return bitmap{}, err
			}

			// short-circuit: universe OR ... == universe
			if b.neg && (b.bm == nil || b.bm.IsEmpty()) {
				b.release()
				releaseAll(positives)
				releaseAll(negatives)
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

			positives = append(positives, b)
		}

		// 1. Merge positive branches first; this captures the common OR pattern
		// and keeps negative-branch handling as a rare slow path.
		var resultBm *roaring64.Bitmap
		if len(positives) > 0 {
			unionInputsBuf := getRoaringSliceBuf(len(positives))
			unionInputs := unionInputsBuf.values
			for _, p := range positives {
				unionInputs = append(unionInputs, p.bm)
			}
			resultBm = db.unionBitmaps(unionInputs)
			unionInputsBuf.values = unionInputs
			releaseRoaringSliceBuf(unionInputsBuf)
			releaseAll(positives)
		} else {
			resultBm = getRoaringBuf()
		}
		res := bitmap{bm: resultBm}

		// 2. Merge negatives (A OR NOT B) via bitmap algebra.
		for i, neg := range negatives {
			var err error
			res, err = db.orBitmap(res, neg)
			if err != nil {
				res.release()
				releaseAll(negatives[i+1:])
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

// evalSimple evaluates a single non-boolean predicate against index snapshots.
//
// It routes every operator through overlay-aware lookups so query semantics
// stay correct during snapshot-delta compaction.
func (db *DB[K, V]) evalSimple(e qx.Expr) (bitmap, error) {
	ov := db.fieldOverlay(e.Field)
	if !ov.hasData() && !db.hasFieldIndex(e.Field) {
		return bitmap{}, fmt.Errorf("no index for field: %v", e.Field)
	}

	f := db.fields[e.Field]
	if f == nil {
		return bitmap{}, fmt.Errorf("no metadata for field: %v", e.Field)
	}

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

			bm, owned := ov.lookupWithState(key, nil)
			if bm != nil && !bm.IsEmpty() {
				return bitmap{bm: bm, readonly: !owned}, nil
			}
			return bitmap{bm: getRoaringBuf()}, nil

		} else {
			vals, isSlice, err := db.exprValueToIdxOwned(e)
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

		res := getRoaringBuf()
		var scratch *roaring64.Bitmap
		if ov.delta != nil {
			scratch = getRoaringBuf()
			defer releaseRoaringBuf(scratch)
		}

		for _, v := range vals {
			bm, owned := ov.lookupWithState(v, scratch)
			if bm == nil || bm.IsEmpty() {
				continue
			}
			orBitmapAdaptive(res, bm)
			if owned && bm != scratch {
				releaseRoaringBuf(bm)
			}
		}
		return bitmap{bm: res}, nil

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
			var acc *roaring64.Bitmap
			var scratch *roaring64.Bitmap
			if ov.delta != nil {
				scratch = getRoaringBuf()
				defer releaseRoaringBuf(scratch)
			}
			for _, v := range vals {
				bm, owned := ov.lookupWithState(v, scratch)
				if bm == nil || bm.IsEmpty() {
					// if any value is missing, result is empty
					if acc != nil {
						releaseRoaringBuf(acc)
					}
					return bitmap{bm: getRoaringBuf()}, nil
				}
				if acc == nil {
					acc = getRoaringBuf()
					acc.Or(bm)
				} else {
					acc.And(bm)
				}
				if owned && bm != scratch {
					releaseRoaringBuf(bm)
				}
				if acc.IsEmpty() {
					return bitmap{bm: acc}, nil
				}
			}
			if acc == nil {
				return bitmap{bm: getRoaringBuf()}, nil
			}
			return bitmap{bm: acc}, nil
		}

		// HASANY - OR logic
		res := getRoaringBuf()
		var scratch *roaring64.Bitmap
		if ov.delta != nil {
			scratch = getRoaringBuf()
			defer releaseRoaringBuf(scratch)
		}

		for _, v := range vals {
			bm, owned := ov.lookupWithState(v, scratch)
			if bm == nil || bm.IsEmpty() {
				continue
			}
			orBitmapAdaptive(res, bm)
			if owned && bm != scratch {
				releaseRoaringBuf(bm)
			}
		}
		return bitmap{bm: res}, nil

	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
		key, isSlice, err := db.exprValueToIdxScalar(e)
		if err != nil {
			return bitmap{}, err
		}
		if isSlice {
			return bitmap{}, fmt.Errorf("%w: %v expects a single value", ErrInvalidQuery, e.Op)
		}
		cacheKey := db.materializedPredCacheKeyForScalar(e.Field, e.Op, key)
		if cacheKey != "" {
			if cached, ok := db.getSnapshot().loadMaterializedPred(cacheKey); ok {
				if cached == nil || cached.IsEmpty() {
					return bitmap{bm: getRoaringBuf()}, nil
				}
				return bitmap{bm: cached, readonly: true}, nil
			}
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
		case qx.OpPREFIX:
			rb.hasPrefix = true
			rb.prefix = key
		}

		br := ov.rangeForBounds(rb)
		if br.baseStart >= br.baseEnd && br.deltaStart >= br.deltaEnd {
			if cacheKey != "" {
				db.getSnapshot().storeMaterializedPred(cacheKey, nil)
			}
			return bitmap{bm: getRoaringBuf()}, nil
		}

		if e.Op != qx.OpPREFIX {
			if out, ok := db.tryEvalNumericRangeBuckets(e.Field, f, ov, br); ok {
				if db.tryStoreMaterializedPredShared(cacheKey, out.bm) {
					out.readonly = true
				}
				return out, nil
			}
		}

		res := getRoaringBuf()
		var scratch *roaring64.Bitmap
		if ov.delta != nil {
			scratch = getRoaringBuf()
			defer releaseRoaringBuf(scratch)
		}

		spanLen := (br.baseEnd - br.baseStart) + (br.deltaEnd - br.deltaStart)
		bulkSingles := spanLen > 0 && spanLen <= singleAdaptiveMaxLen

		var singles *singleIDsBuffer
		if bulkSingles {
			singles = getSingleIDsBuf()
			defer releaseSingleIDs(singles)
		}

		if ov.delta == nil {
			for i := br.baseStart; i < br.baseEnd; i++ {
				ids := ov.base[i].IDs
				if ids.IsEmpty() {
					continue
				}

				// Singleton-heavy ranges are accumulated via batched AddMany
				// because repeated OR of tiny bitmaps is much more expensive.
				if ids.isSingleton() {
					if bulkSingles {
						singles.values = append(singles.values, ids.single)
						if len(singles.values) == cap(singles.values) {
							res.AddMany(singles.values)
							singles.values = singles.values[:0]
						}
					} else {
						res.Add(ids.single)
					}
					continue
				}

				if singles != nil && len(singles.values) > 0 {
					res.AddMany(singles.values)
					singles.values = singles.values[:0]
				}
				res.Or(ids.bitmap())
			}
		} else {
			cur := ov.newCursor(br, false)
			for {
				_, baseBM, de, ok := cur.next()
				if !ok {
					break
				}

				bm := composePosting(baseBM, de, scratch)
				if bm == nil || bm.IsEmpty() {
					continue
				}

				// Singleton-heavy ranges are accumulated via batched AddMany because
				// repeated OR of tiny bitmaps is much more expensive.
				if bm.GetCardinality() == 1 {
					if bulkSingles {
						singles.values = append(singles.values, bm.Minimum())
						if len(singles.values) == cap(singles.values) {
							res.AddMany(singles.values)
							singles.values = singles.values[:0]
						}
					} else {
						res.Add(bm.Minimum())
					}
					continue
				}

				if singles != nil && len(singles.values) > 0 {
					res.AddMany(singles.values)
					singles.values = singles.values[:0]
				}
				res.Or(bm)
			}
		}

		if singles != nil && len(singles.values) > 0 {
			res.AddMany(singles.values)
		}
		if db.tryStoreMaterializedPredShared(cacheKey, res) {
			return bitmap{bm: res, readonly: true}, nil
		}
		return bitmap{bm: res}, nil

	case qx.OpSUFFIX, qx.OpCONTAINS:
		v, isSlice, err := db.exprValueToIdxScalar(e)
		if err != nil {
			return bitmap{}, err
		}
		if isSlice {
			return bitmap{}, fmt.Errorf("%w: %v expects a single string value", ErrInvalidQuery, e.Op)
		}
		cacheKey := db.materializedPredCacheKeyForScalar(e.Field, e.Op, v)
		if cacheKey != "" {
			if cached, ok := db.getSnapshot().loadMaterializedPred(cacheKey); ok {
				if cached == nil || cached.IsEmpty() {
					return bitmap{bm: getRoaringBuf()}, nil
				}
				return bitmap{bm: cached, readonly: true}, nil
			}
		}

		res := getRoaringBuf()
		var scratch *roaring64.Bitmap
		if ov.delta != nil {
			scratch = getRoaringBuf()
			defer releaseRoaringBuf(scratch)
		}

		full := ov.rangeForBounds(rangeBounds{has: true})
		spanLen := (full.baseEnd - full.baseStart) + (full.deltaEnd - full.deltaStart)
		bulkSingles := spanLen > 0 && spanLen <= singleAdaptiveMaxLen

		var singles *singleIDsBuffer
		if bulkSingles {
			singles = getSingleIDsBuf()
			defer releaseSingleIDs(singles)
		}

		if ov.delta == nil {
			for i := full.baseStart; i < full.baseEnd; i++ {
				kv := ov.base[i]
				match := e.Op == qx.OpSUFFIX && indexKeyHasSuffixString(kv.Key, v) ||
					e.Op == qx.OpCONTAINS && indexKeyContainsString(kv.Key, v)
				if !match {
					continue
				}

				ids := kv.IDs
				if ids.IsEmpty() {
					continue
				}

				// Same singleton optimization for suffix/contains scans over wide
				// dictionary spans.
				if ids.isSingleton() {
					if bulkSingles {
						singles.values = append(singles.values, ids.single)
						if len(singles.values) == cap(singles.values) {
							res.AddMany(singles.values)
							singles.values = singles.values[:0]
						}
					} else {
						res.Add(ids.single)
					}
					continue
				}
				if singles != nil && len(singles.values) > 0 {
					res.AddMany(singles.values)
					singles.values = singles.values[:0]
				}
				res.Or(ids.bitmap())
			}
		} else {
			cur := ov.newCursor(full, false)
			for {
				k, baseBM, de, ok := cur.next()
				if !ok {
					break
				}
				match := e.Op == qx.OpSUFFIX && indexKeyHasSuffixString(k, v) ||
					e.Op == qx.OpCONTAINS && indexKeyContainsString(k, v)
				if !match {
					continue
				}
				bm := composePosting(baseBM, de, scratch)
				if bm == nil || bm.IsEmpty() {
					continue
				}

				// Same singleton optimization for suffix/contains scans over wide
				// dictionary spans.
				if bm.GetCardinality() == 1 {
					if bulkSingles {
						singles.values = append(singles.values, bm.Minimum())
						if len(singles.values) == cap(singles.values) {
							res.AddMany(singles.values)
							singles.values = singles.values[:0]
						}
					} else {
						res.Add(bm.Minimum())
					}
					continue
				}
				if singles != nil && len(singles.values) > 0 {
					res.AddMany(singles.values)
					singles.values = singles.values[:0]
				}
				res.Or(bm)
			}
		}
		if singles != nil && len(singles.values) > 0 {
			res.AddMany(singles.values)
		}
		if db.tryStoreMaterializedPredShared(cacheKey, res) {
			return bitmap{bm: res, readonly: true}, nil
		}
		return bitmap{bm: res}, nil

	default:
		return bitmap{}, fmt.Errorf("unsupported op: %v", e.Op)
	}
}

func orBitmapAdaptive(dst, src *roaring64.Bitmap) {
	if dst == nil || src == nil || src.IsEmpty() {
		return
	}
	if src.GetCardinality() == 1 {
		dst.Add(src.Minimum())
		return
	}
	dst.Or(src)
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
		orBitmapAdaptive(res, bm)
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

// evalSliceEQ evaluates equality for slice fields by intersecting member
// bitmaps, because slice-EQ semantics require full set match.
func (db *DB[K, V]) evalSliceEQ(field string, vals []string) (bitmap, error) {

	vals = dedupStringsInplace(vals)

	lenOV := db.lenFieldOverlay(field)
	useZeroComplement := len(vals) == 0 && db.isLenZeroComplementField(field)
	if !lenOV.hasData() && db.snapshotLenFieldIndexSlice(field) == nil && !useZeroComplement {
		return bitmap{}, fmt.Errorf("no lenIndex for slice field: %v", field)
	}

	var (
		lenBM    *roaring64.Bitmap
		lenOwned bool
	)
	if useZeroComplement {
		nonEmpty, nonEmptyOwned := lenOV.lookupWithState(lenIndexNonEmptyKey, nil)
		universe, universeOwned := db.snapshotUniverseView()
		zero := getRoaringBuf()
		if universe != nil && !universe.IsEmpty() {
			zero.Or(universe)
		}
		if nonEmpty != nil && !nonEmpty.IsEmpty() {
			zero.AndNot(nonEmpty)
		}
		if universeOwned && universe != nil {
			releaseRoaringBuf(universe)
		}
		if nonEmptyOwned && nonEmpty != nil {
			releaseRoaringBuf(nonEmpty)
		}
		lenBM = zero
		lenOwned = true
	} else {
		lenKey := uint64ByteStr(uint64(len(vals)))
		lenBM, lenOwned = lenOV.lookupWithState(lenKey, nil)
	}
	if lenBM == nil || lenBM.IsEmpty() {
		if lenOwned && lenBM != nil {
			releaseRoaringBuf(lenBM)
		}
		return bitmap{bm: getRoaringBuf()}, nil
	}

	ov := db.fieldOverlay(field)
	acc := getRoaringBuf()
	acc.Or(lenBM)
	if lenOwned {
		releaseRoaringBuf(lenBM)
	}

	var scratch *roaring64.Bitmap
	if ov.delta != nil {
		scratch = getRoaringBuf()
		defer releaseRoaringBuf(scratch)
	}
	for _, v := range vals {
		bm, owned := ov.lookupWithState(v, scratch)
		if bm == nil || bm.IsEmpty() {
			releaseRoaringBuf(acc)
			return bitmap{bm: getRoaringBuf()}, nil
		}
		acc.And(bm)
		if owned && bm != scratch {
			releaseRoaringBuf(bm)
		}
		if acc.IsEmpty() {
			return bitmap{bm: acc}, nil
		}
	}

	return bitmap{bm: acc}, nil
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
	return db.exprValueToIdxWithMode(expr, false)
}

// exprValueToIdxOwned returns a slice safe for in-place mutation by callers.
func (db *DB[K, V]) exprValueToIdxOwned(expr qx.Expr) ([]string, bool, error) {
	return db.exprValueToIdxWithMode(expr, true)
}

func (db *DB[K, V]) exprValueToIdxWithMode(expr qx.Expr, cloneStringSlice bool) ([]string, bool, error) {

	if expr.Value == nil {
		switch expr.Op {
		case qx.OpIN, qx.OpHAS, qx.OpHASANY:
			return nil, false, nil
		default:
			return []string{nilValue}, false, nil
		}
	}
	switch v := expr.Value.(type) {
	case []string:
		if cloneStringSlice && len(v) > 1 {
			return slices.Clone(v), true, nil
		}
		return v, true, nil
	case string:
		return []string{v}, false, nil
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

		// Both operands are readonly: clone the smaller payload to reduce copy
		// and reduce subsequent intersection work.
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
