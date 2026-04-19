package rbi

import (
	"fmt"
	"math"
	"reflect"
	"sync"

	"github.com/vapstack/rbi/internal/qir"

	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

type stringKeyReader interface {
	Len() int
	Get(i int) string
}

func stringKeyReaderLen(keys stringKeyReader) int {
	switch v := keys.(type) {
	case nil:
		return 0
	case *pooled.SliceBuf[string]:
		if v == nil {
			return 0
		}
	}
	return keys.Len()
}

func releasePostingResults(buf *pooled.SliceBuf[postingResult]) {
	for i := 0; i < buf.Len(); i++ {
		buf.Get(i).release()
	}
}

func (qv *queryView[K, V]) checkUsedQuery(q *qir.Shape) error {
	if q.HasOrder && qv.fieldMetaByOrder(q.Order) == nil {
		return fmt.Errorf("no index for field: %v", qv.fieldNameByOrder(q.Order))
	}
	return qv.checkUsedExpr(q.Expr)
}

func (qv *queryView[K, V]) checkUsedExpr(exp qir.Expr) error {
	if exp.FieldOrdinal >= 0 {
		if qv.fieldMetaByExpr(exp) == nil {
			return fmt.Errorf("no index for field: %v", qv.fieldNameByExpr(exp))
		}
	}
	for _, op := range exp.Operands {
		if err := qv.checkUsedExpr(op); err != nil {
			return err
		}
	}
	return nil
}

// evalExpr evaluates a boolean expression tree into a postingResult representation.
//
// Results may be positive sets or negative/complement sets to
// postpone expensive universe materialization until it is actually required.
func (qv *queryView[K, V]) evalExpr(e qir.Expr) (postingResult, error) {
	switch e.Op {

	case qir.OpNOOP:
		if e.FieldOrdinal != qir.NoFieldOrdinal || e.Value != nil || len(e.Operands) != 0 {
			return postingResult{}, fmt.Errorf("%w: invalid expression, op: %v", ErrInvalidQuery, e.Op)
		}
		res := postingResult{neg: true}
		if e.Not {
			res.neg = !res.neg
		}
		return res, nil

	case qir.OpAND:
		return qv.evalAndOperands(e.Operands, e.Not)

	case qir.OpOR:
		if len(e.Operands) == 0 {
			return postingResult{}, fmt.Errorf("%w: empty OR expression", ErrInvalidQuery)
		}

		positive := postingResultSlicePool.Get()
		positive.Grow(len(e.Operands))
		defer postingResultSlicePool.Put(positive)

		negative := postingResultSlicePool.Get()
		negative.Grow(len(e.Operands))
		defer postingResultSlicePool.Put(negative)

		for _, op := range e.Operands {
			b, err := qv.evalExpr(op)
			if err != nil {
				releasePostingResults(positive)
				releasePostingResults(negative)
				return postingResult{}, err
			}

			// short-circuit: universe OR ... == universe
			if b.neg && b.ids.IsEmpty() {
				b.release()
				releasePostingResults(positive)
				releasePostingResults(negative)
				res := postingResult{neg: true}
				if e.Not {
					res.neg = !res.neg
				}
				return res, nil
			}

			if b.neg {
				negative.Append(b)
				continue
			}

			if b.ids.IsEmpty() {
				b.release()
				continue
			}

			positive.Append(b)
		}

		// 1. Merge positive branches first; this captures the common OR pattern
		// and keeps negative-branch handling as a rare slow path.
		var res postingResult
		if positive.Len() > 0 {
			res = positive.Get(0)
			for i := 1; i < positive.Len(); i++ {
				res = qv.orPostingResult(res, positive.Get(i))
			}
		}

		// 2. Merge negatives (A OR NOT B) via postingResult algebra.
		for i := 0; i < negative.Len(); i++ {
			res = qv.orPostingResult(res, negative.Get(i))
		}

		if e.Not {
			res.neg = !res.neg
		}
		return res, nil

	default:
		res, err := qv.evalSimple(e)
		if err != nil {
			return postingResult{}, err
		}
		if e.Not {
			res.neg = !res.neg
		}
		return res, nil
	}
}

func (qv *queryView[K, V]) evalAndOperands(ops []qir.Expr, negate bool) (postingResult, error) {
	if len(ops) == 0 {
		return postingResult{}, fmt.Errorf("%w: empty AND expression", ErrInvalidQuery)
	}

	var (
		first    postingResult
		hasFirst bool
	)

	for _, op := range ops {
		b, err := qv.evalExpr(op)
		if err != nil {
			if hasFirst {
				first.release()
			}
			return postingResult{}, err
		}

		// short-circuit: if empty non-neg set is found, the result is empty
		if !b.neg && b.ids.IsEmpty() {
			b.release()
			if hasFirst {
				first.release()
			}
			return postingResult{}, nil
		}

		if !hasFirst {
			first = b
			hasFirst = true
			continue
		}

		// Keep the accumulator in postingResult form to preserve short-circuiting and
		// avoid temporary key arrays in deep boolean trees.
		first, err = qv.andPostingResult(first, b)
		if err != nil {
			first.release()
			return postingResult{}, err
		}

		if !first.neg && first.ids.IsEmpty() {
			first.release()
			return postingResult{}, nil
		}
	}

	if negate {
		first.neg = !first.neg
	}
	return first, nil
}

// evalSimple evaluates a single non-boolean predicate against the current
// immutable snapshot indexes.
func (qv *queryView[K, V]) evalSimple(e qir.Expr) (postingResult, error) {
	fieldName := qv.fieldNameByExpr(e)
	ov := qv.fieldOverlayForExpr(e)
	if !ov.hasData() && !qv.hasIndexedFieldForExpr(e) {
		return postingResult{}, fmt.Errorf("no index for field: %v", fieldName)
	}

	f := qv.fieldMetaByExpr(e)
	if f == nil {
		return postingResult{}, fmt.Errorf("no metadata for field: %v", fieldName)
	}

	switch e.Op {
	case qir.OpEQ:
		if !f.Slice {
			key, isSlice, isNil, err := qv.exprValueToIdxScalar(e)
			if err != nil {
				return postingResult{}, err
			}
			if isSlice {
				return postingResult{}, fmt.Errorf("%w: %v expects a single value for scalar field %v", ErrInvalidQuery, e.Op, fieldName)
			}

			var ids posting.List
			if isNil {
				ids = qv.nilFieldOverlayForExpr(e).lookupPostingRetained(nilIndexEntryKey)
			} else {
				ids = ov.lookupPostingRetained(key)
			}
			if !ids.IsEmpty() {
				return postingResult{ids: ids}, nil
			}
			return postingResult{}, nil

		} else {
			valsBuf, isSlice, _, err := qv.exprValueToDistinctIdxBuf(e)
			if err != nil {
				return postingResult{}, err
			}
			if valsBuf != nil {
				defer stringSlicePool.Put(valsBuf)
			}
			if !isSlice {
				return postingResult{}, fmt.Errorf("%w: %v expects a slice for slice field %v", ErrInvalidQuery, e.Op, fieldName)
			}
			return qv.evalSliceEQ(fieldName, e.FieldOrdinal, valsBuf)
		}

	case qir.OpIN:
		if f.Slice {
			return postingResult{}, fmt.Errorf("%w: %v not supported on slice field %v", ErrInvalidQuery, e.Op, fieldName)
		}
		valsBuf, isSlice, hasNil, err := qv.exprValueToDistinctIdxBuf(e)
		if err != nil {
			return postingResult{}, err
		}
		if valsBuf != nil {
			defer stringSlicePool.Put(valsBuf)
		}
		if !isSlice && e.Value != nil {
			return postingResult{}, fmt.Errorf("%w: %v expects a slice", ErrInvalidQuery, e.Op)
		}
		valCount := 0
		if valsBuf != nil {
			valCount = valsBuf.Len()
		}
		if valCount == 0 && !hasNil {
			return postingResult{}, fmt.Errorf("%v: %v: no values provided", ErrInvalidQuery, e.Op)
		}

		capHint := valCount
		if hasNil {
			capHint++
		}
		builder := newPostingUnionBuilder(postingBatchSinglesEnabled(uint64(capHint)))
		defer builder.release()
		for i := 0; i < valCount; i++ {
			ids := ov.lookupPostingRetained(valsBuf.Get(i))
			if ids.IsEmpty() {
				continue
			}
			builder.addPosting(ids)
		}
		if hasNil {
			ids := qv.nilFieldOverlayForExpr(e).lookupPostingRetained(nilIndexEntryKey)
			if !ids.IsEmpty() {
				builder.addPosting(ids)
			}
		}
		return postingResult{ids: builder.finish(false)}, nil

	case qir.OpHASANY, qir.OpHASALL:
		if !f.Slice {
			return postingResult{}, fmt.Errorf("%w: %v not supported on non-slice field %v", ErrInvalidQuery, e.Op, fieldName)
		}
		valsBuf, isSlice, _, err := qv.exprValueToDistinctIdxBuf(e)
		if err != nil {
			return postingResult{}, err
		}
		if valsBuf != nil {
			defer stringSlicePool.Put(valsBuf)
		}
		if !isSlice && e.Value != nil {
			return postingResult{}, fmt.Errorf("%w: %v expects a slice", ErrInvalidQuery, e.Op)
		}
		valCount := 0
		if valsBuf != nil {
			valCount = valsBuf.Len()
		}
		if valCount == 0 {
			return postingResult{}, fmt.Errorf("%v: %v: no values provided", ErrInvalidQuery, e.Op)
		}

		if e.Op == qir.OpHASALL {
			var acc posting.List
			for i := 0; i < valCount; i++ {
				ids := ov.lookupPostingRetained(valsBuf.Get(i))
				if ids.IsEmpty() {
					// if any value is missing, result is empty
					acc.Release()
					return postingResult{}, nil
				}
				if acc.IsEmpty() {
					acc = ids.Clone()
				} else {
					acc = acc.BuildAnd(ids)
				}
				if acc.IsEmpty() {
					return postingResult{ids: acc}, nil
				}
			}
			return postingResult{ids: acc}, nil
		}

		// HASANY - OR logic
		builder := newPostingUnionBuilder(postingBatchSinglesEnabled(uint64(valCount)))
		defer builder.release()
		for i := 0; i < valCount; i++ {
			ids := ov.lookupPostingRetained(valsBuf.Get(i))
			if ids.IsEmpty() {
				continue
			}
			builder.addPosting(ids)
		}
		return postingResult{ids: builder.finish(false)}, nil

	case qir.OpGT, qir.OpGTE, qir.OpLT, qir.OpLTE, qir.OpPREFIX:
		bound, isSlice, err := qv.exprValueToNormalizedScalarBound(e)
		if err != nil {
			return postingResult{}, err
		}
		if isSlice {
			return postingResult{}, fmt.Errorf("%w: %v expects a single value", ErrInvalidQuery, e.Op)
		}
		if bound.empty {
			return postingResult{}, nil
		}
		var core preparedScalarRangePredicate[K, V]
		qv.initPreparedScalarRangePredicateFromBound(&core, e, f, bound)
		return core.evalMaterializedPostingResult(ov), nil

	case qir.OpSUFFIX, qir.OpCONTAINS:
		v, isSlice, isNil, err := qv.exprValueToIdxScalar(e)
		if err != nil {
			return postingResult{}, err
		}
		if isSlice {
			return postingResult{}, fmt.Errorf("%w: %v expects a single string value", ErrInvalidQuery, e.Op)
		}
		if isNil {
			return postingResult{}, nil
		}
		cacheKey := qv.materializedPredKeyForScalar(fieldName, e.Op, v)
		if !cacheKey.isZero() {
			if cached, ok := qv.snap.loadMaterializedPredKey(cacheKey); ok {
				if cached.IsEmpty() {
					return postingResult{}, nil
				}
				return postingResult{ids: cached}, nil
			}
		}

		full := ov.rangeForBounds(rangeBounds{has: true})
		spanLen := full.baseEnd - full.baseStart
		builder := newPostingUnionBuilder(spanLen > 0 && spanLen <= singleAdaptiveMaxLen)
		defer builder.release()

		cur := ov.newCursor(full, false)
		for {
			key, ids, ok := cur.next()
			if !ok {
				break
			}
			match := e.Op == qir.OpSUFFIX && indexKeyHasSuffixString(key, v) ||
				e.Op == qir.OpCONTAINS && indexKeyContainsString(key, v)
			if !match {
				continue
			}
			if ids.IsEmpty() {
				continue
			}

			// Same singleton optimization for suffix/contains scans over wide
			// dictionary spans.
			builder.addPosting(ids)
		}
		res := builder.finish(false)
		res = qv.tryShareMaterializedPred(cacheKey, res)
		return postingResult{ids: res}, nil

	default:
		return postingResult{}, fmt.Errorf("unsupported op: %v", e.Op)
	}
}

func linearPostingUnionOwned(posts []posting.List) posting.List {
	bestIdx := 0
	maxCard := posts[0].Cardinality()
	for i := 1; i < len(posts); i++ {
		c := posts[i].Cardinality()
		if c > maxCard {
			maxCard = c
			bestIdx = i
		}
	}

	res := posts[bestIdx].Clone()
	for i, ids := range posts {
		if i == bestIdx {
			continue
		}
		res = res.BuildOr(ids)
	}
	return res.BuildOptimized()
}

func parallelBatchedPostingUnionOwned(posts []posting.List) posting.List {
	n := len(posts)

	workers := 8
	if n < workers*2 {
		workers = n / 2
	}
	if workers < 2 {
		return linearPostingUnionOwned(posts)
	}

	chunkSize := (n + workers - 1) / workers
	resultsBuf := postingSlicePool.Get()
	resultsBuf.SetLen(workers)
	defer postingSlicePool.Put(resultsBuf)

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
		go func(idx int, part []posting.List) {
			defer wg.Done()
			resultsBuf.Set(idx, linearPostingUnionOwned(part))
		}(i, posts[start:end])
	}

	wg.Wait()

	final := resultsBuf.Get(0)
	for i := 1; i < resultsBuf.Len(); i++ {
		final = final.BuildMergedOwned(resultsBuf.Get(i))
	}
	return final.BuildOptimized()
}

// evalSliceEQ evaluates equality for slice fields by intersecting member
// bitmaps, because slice-EQ semantics require full set match.
//
// Callers pass distinct values; duplicates are removed before this point so
// this path can stay read-only.
func (qv *queryView[K, V]) evalSliceEQ(field string, fieldOrdinal int, vals stringKeyReader) (postingResult, error) {
	valCount := stringKeyReaderLen(vals)
	lenOV := qv.lenFieldOverlayRef(field, fieldOrdinal)
	useZeroComplement := valCount == 0 && qv.isLenZeroComplementRef(field, fieldOrdinal)
	if !lenOV.hasData() && qv.snapshotLenFieldIndexSliceRef(field, fieldOrdinal) == nil && !useZeroComplement {
		return postingResult{}, fmt.Errorf("no lenIndex for slice field: %v", field)
	}

	var (
		lenBM posting.List
	)
	if useZeroComplement {
		nonEmpty := lenOV.lookupPostingRetained(lenIndexNonEmptyKey)
		lenBM = qv.snapshotUniverseView().Clone()
		lenBM = lenBM.BuildAndNot(nonEmpty)
	} else {
		lenKey := uint64ByteStr(uint64(valCount))
		lenBM = lenOV.lookupPostingRetained(lenKey)
	}
	if lenBM.IsEmpty() {
		return postingResult{}, nil
	}

	ov := qv.fieldOverlayRef(field, fieldOrdinal)
	acc := lenBM.Clone()
	for i := 0; i < valCount; i++ {
		ids := ov.lookupPostingRetained(vals.Get(i))
		if ids.IsEmpty() {
			acc.Release()
			return postingResult{}, nil
		}
		acc = acc.BuildAnd(ids)
		if acc.IsEmpty() {
			return postingResult{ids: acc}, nil
		}
	}

	return postingResult{ids: acc}, nil
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

func queryValueIsCollectionForField(v reflect.Value, fm *field) bool {
	if v.Kind() != reflect.Slice {
		return false
	}
	if fm != nil && fm.UseVI && !fm.Slice {
		return false
	}
	return true
}

const impossibleLookupKey = "\x00"

type normalizedScalarBound struct {
	op          qir.Op
	key         string
	keyIndex    indexKey
	hasIndexKey bool
	full        bool
	empty       bool
}

func normalizedScalarBoundFromString(op qir.Op, key string) normalizedScalarBound {
	return normalizedScalarBound{op: op, key: key}
}

func normalizedScalarBoundFromIndexKey(op qir.Op, key indexKey) normalizedScalarBound {
	return normalizedScalarBound{
		op:          op,
		keyIndex:    key,
		hasIndexKey: true,
	}
}

func scalarValueToIdxRaw(raw any, v reflect.Value) (string, error) {
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

func signedIntFieldKind(kind reflect.Kind) bool {
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return true
	default:
		return false
	}
}

func unsignedIntFieldKind(kind reflect.Kind) bool {
	switch kind {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	default:
		return false
	}
}

func numericQueryValueToFloat64(v reflect.Value) (float64, bool) {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(v.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(v.Uint()), true
	case reflect.Float32, reflect.Float64:
		return canonicalizeFloat64ForIndex(v.Float()), true
	default:
		return 0, false
	}
}

func numericQueryValueToInt64Exact(v reflect.Value) (int64, bool) {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int(), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u := v.Uint()
		if u > math.MaxInt64 {
			return 0, false
		}
		return int64(u), true
	case reflect.Float32, reflect.Float64:
		f := canonicalizeFloat64ForIndex(v.Float())
		if math.IsNaN(f) || math.IsInf(f, 0) || f != math.Trunc(f) || f < math.MinInt64 || f > math.MaxInt64 {
			return 0, false
		}
		i := int64(f)
		return i, float64(i) == f
	default:
		return 0, false
	}
}

func numericQueryValueToUint64Exact(v reflect.Value) (uint64, bool) {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i := v.Int()
		if i < 0 {
			return 0, false
		}
		return uint64(i), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint(), true
	case reflect.Float32, reflect.Float64:
		f := canonicalizeFloat64ForIndex(v.Float())
		if math.IsNaN(f) || math.IsInf(f, 0) || f != math.Trunc(f) || f < 0 || f > float64(^uint64(0)) {
			return 0, false
		}
		u := uint64(f)
		return u, float64(u) == f
	default:
		return 0, false
	}
}

func scalarValueToIdxField(raw any, v reflect.Value, fm *field) (string, error) {
	if fm != nil && fm.UseVI {
		if vi, ok := raw.(ValueIndexer); ok {
			return vi.IndexingValue(), nil
		}
		if v.IsValid() && v.CanInterface() {
			if vi, ok := v.Interface().(ValueIndexer); ok {
				return vi.IndexingValue(), nil
			}
		}
		if v.Kind() == reflect.String {
			return v.String(), nil
		}
		return "", fmt.Errorf("unsupported value type: %v", v.Type())
	}

	if fm == nil {
		return scalarValueToIdxRaw(raw, v)
	}

	switch {
	case signedIntFieldKind(fm.Kind):
		if i, ok := numericQueryValueToInt64Exact(v); ok {
			return int64ByteStr(i), nil
		}
		return impossibleLookupKey, nil
	case unsignedIntFieldKind(fm.Kind):
		if u, ok := numericQueryValueToUint64Exact(v); ok {
			return uint64ByteStr(u), nil
		}
		return impossibleLookupKey, nil
	case fm.Kind == reflect.Float32 || fm.Kind == reflect.Float64:
		if f, ok := numericQueryValueToFloat64(v); ok {
			return float64ByteStr(f), nil
		}
		return impossibleLookupKey, nil
	default:
		return scalarValueToIdxRaw(raw, v)
	}
}

func normalizeSignedIntRangeBound(op qir.Op, v reflect.Value) normalizedScalarBound {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return normalizedScalarBoundFromIndexKey(op, indexKeyFromU64(orderedInt64Key(v.Int())))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u := v.Uint()
		if u > math.MaxInt64 {
			switch op {
			case qir.OpGT, qir.OpGTE:
				return normalizedScalarBound{empty: true}
			default:
				return normalizedScalarBound{full: true}
			}
		}
		return normalizedScalarBoundFromIndexKey(op, indexKeyFromU64(orderedInt64Key(int64(u))))
	case reflect.Float32, reflect.Float64:
		f := canonicalizeFloat64ForIndex(v.Float())
		switch {
		case math.IsNaN(f), math.IsInf(f, 1):
			switch op {
			case qir.OpGT, qir.OpGTE:
				return normalizedScalarBound{empty: true}
			default:
				return normalizedScalarBound{full: true}
			}
		case math.IsInf(f, -1):
			switch op {
			case qir.OpGT, qir.OpGTE:
				return normalizedScalarBound{full: true}
			default:
				return normalizedScalarBound{empty: true}
			}
		case f == math.Trunc(f):
			if f < math.MinInt64 || f > math.MaxInt64 {
				switch op {
				case qir.OpGT, qir.OpGTE:
					if f < math.MinInt64 {
						return normalizedScalarBound{full: true}
					}
					return normalizedScalarBound{empty: true}
				default:
					if f < math.MinInt64 {
						return normalizedScalarBound{empty: true}
					}
					return normalizedScalarBound{full: true}
				}
			}
			return normalizedScalarBoundFromIndexKey(op, indexKeyFromU64(orderedInt64Key(int64(f))))
		case op == qir.OpEQ:
			return normalizedScalarBound{empty: true}
		case op == qir.OpGT || op == qir.OpGTE:
			floor := math.Floor(f)
			if floor < math.MinInt64 {
				return normalizedScalarBound{full: true}
			}
			if floor > math.MaxInt64 {
				return normalizedScalarBound{empty: true}
			}
			return normalizedScalarBoundFromIndexKey(qir.OpGT, indexKeyFromU64(orderedInt64Key(int64(floor))))
		default:
			ceil := math.Ceil(f)
			if ceil < math.MinInt64 {
				return normalizedScalarBound{empty: true}
			}
			if ceil > math.MaxInt64 {
				return normalizedScalarBound{full: true}
			}
			return normalizedScalarBoundFromIndexKey(qir.OpLT, indexKeyFromU64(orderedInt64Key(int64(ceil))))
		}
	default:
		return normalizedScalarBound{empty: true}
	}
}

func normalizeUnsignedIntRangeBound(op qir.Op, v reflect.Value) normalizedScalarBound {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i := v.Int()
		if i < 0 {
			switch op {
			case qir.OpGT, qir.OpGTE:
				return normalizedScalarBound{full: true}
			default:
				return normalizedScalarBound{empty: true}
			}
		}
		return normalizedScalarBoundFromIndexKey(op, indexKeyFromU64(uint64(i)))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return normalizedScalarBoundFromIndexKey(op, indexKeyFromU64(v.Uint()))
	case reflect.Float32, reflect.Float64:
		f := canonicalizeFloat64ForIndex(v.Float())
		switch {
		case math.IsNaN(f), math.IsInf(f, 1):
			switch op {
			case qir.OpGT, qir.OpGTE:
				return normalizedScalarBound{empty: true}
			default:
				return normalizedScalarBound{full: true}
			}
		case math.IsInf(f, -1):
			switch op {
			case qir.OpGT, qir.OpGTE:
				return normalizedScalarBound{full: true}
			default:
				return normalizedScalarBound{empty: true}
			}
		case f < 0:
			switch op {
			case qir.OpGT, qir.OpGTE:
				return normalizedScalarBound{full: true}
			default:
				return normalizedScalarBound{empty: true}
			}
		case f == math.Trunc(f):
			if f > float64(^uint64(0)) {
				switch op {
				case qir.OpGT, qir.OpGTE:
					return normalizedScalarBound{empty: true}
				default:
					return normalizedScalarBound{full: true}
				}
			}
			return normalizedScalarBoundFromIndexKey(op, indexKeyFromU64(uint64(f)))
		case op == qir.OpEQ:
			return normalizedScalarBound{empty: true}
		case op == qir.OpGT || op == qir.OpGTE:
			floor := math.Floor(f)
			if floor > float64(^uint64(0)) {
				return normalizedScalarBound{empty: true}
			}
			return normalizedScalarBoundFromIndexKey(qir.OpGT, indexKeyFromU64(uint64(floor)))
		default:
			ceil := math.Ceil(f)
			if ceil > float64(^uint64(0)) {
				return normalizedScalarBound{full: true}
			}
			return normalizedScalarBoundFromIndexKey(qir.OpLT, indexKeyFromU64(uint64(ceil)))
		}
	default:
		return normalizedScalarBound{empty: true}
	}
}

func normalizeFloatRangeBound(op qir.Op, v reflect.Value) normalizedScalarBound {
	f, ok := numericQueryValueToFloat64(v)
	if !ok {
		return normalizedScalarBound{empty: true}
	}
	return normalizedScalarBoundFromIndexKey(op, indexKeyFromU64(orderedFloat64Key(f)))
}

func (qv *queryView[K, V]) exprValueToNormalizedScalarBound(expr qir.Expr) (normalizedScalarBound, bool, error) {
	if expr.Value == nil {
		return normalizedScalarBound{empty: true}, false, nil
	}

	fm := qv.fieldMetaByExpr(expr)
	v := reflect.ValueOf(expr.Value)
	v, isNil := unwrapExprValue(v)
	if isNil {
		return normalizedScalarBound{empty: true}, false, nil
	}
	if queryValueIsCollectionForField(v, fm) {
		return normalizedScalarBound{}, true, nil
	}
	if bound, ok := qv.loadNormalizedScalarBound(expr, v); ok {
		return bound, false, nil
	}
	if expr.Op == qir.OpPREFIX {
		if fm != nil && fm.KeyKind == fieldWriteKeysOrderedU64 {
			bound := normalizedScalarBound{empty: true}
			qv.storeNormalizedScalarBound(expr, v, bound)
			return bound, false, nil
		}
		key, err := scalarValueToIdxField(expr.Value, v, fm)
		if err != nil {
			return normalizedScalarBound{}, false, err
		}
		bound := normalizedScalarBoundFromString(qir.OpPREFIX, key)
		qv.storeNormalizedScalarBound(expr, v, bound)
		return bound, false, nil
	}

	if fm != nil && !fm.UseVI {
		switch {
		case signedIntFieldKind(fm.Kind):
			bound := normalizeSignedIntRangeBound(expr.Op, v)
			qv.storeNormalizedScalarBound(expr, v, bound)
			return bound, false, nil
		case unsignedIntFieldKind(fm.Kind):
			bound := normalizeUnsignedIntRangeBound(expr.Op, v)
			qv.storeNormalizedScalarBound(expr, v, bound)
			return bound, false, nil
		case fm.Kind == reflect.Float32 || fm.Kind == reflect.Float64:
			bound := normalizeFloatRangeBound(expr.Op, v)
			qv.storeNormalizedScalarBound(expr, v, bound)
			return bound, false, nil
		}
	}

	key, err := scalarValueToIdxField(expr.Value, v, fm)
	if err != nil {
		return normalizedScalarBound{}, false, err
	}
	bound := normalizedScalarBoundFromString(expr.Op, key)
	qv.storeNormalizedScalarBound(expr, v, bound)
	return bound, false, nil
}

func applyNormalizedScalarBound(rb *rangeBounds, b normalizedScalarBound) {
	if rb == nil {
		return
	}
	rb.has = true
	if b.empty {
		rb.setEmpty()
		return
	}
	if b.full {
		return
	}
	switch b.op {
	case qir.OpGT:
		if b.hasIndexKey {
			rb.applyLoIndex(b.keyIndex, false)
		} else {
			rb.applyLo(b.key, false)
		}
	case qir.OpGTE:
		if b.hasIndexKey {
			rb.applyLoIndex(b.keyIndex, true)
		} else {
			rb.applyLo(b.key, true)
		}
	case qir.OpLT:
		if b.hasIndexKey {
			rb.applyHiIndex(b.keyIndex, false)
		} else {
			rb.applyHi(b.key, false)
		}
	case qir.OpLTE:
		if b.hasIndexKey {
			rb.applyHiIndex(b.keyIndex, true)
		} else {
			rb.applyHi(b.key, true)
		}
	case qir.OpEQ:
		if b.hasIndexKey {
			rb.applyLoIndex(b.keyIndex, true)
			rb.applyHiIndex(b.keyIndex, true)
		} else {
			rb.applyLo(b.key, true)
			rb.applyHi(b.key, true)
		}
	case qir.OpPREFIX:
		rb.applyPrefix(b.key)
	}
}

func dedupStringBufInPlace(buf *pooled.SliceBuf[string]) {
	if buf == nil || buf.Len() < 2 {
		return
	}
	pooled.SortSlice(buf)
	write := 1
	for read := 1; read < buf.Len(); read++ {
		if buf.Get(read) == buf.Get(write-1) {
			continue
		}
		if write != read {
			buf.Set(write, buf.Get(read))
		}
		write++
	}
	buf.SetLen(write)
}

func sliceValueToIdxStringBuf(v reflect.Value, fm *field) (*pooled.SliceBuf[string], bool, error) {
	ixsBuf := stringSlicePool.Get()
	ixsBuf.Grow(v.Len())
	hasNil := false

	for i := 0; i < v.Len(); i++ {
		elem := v.Index(i)
		raw := any(nil)
		if elem.IsValid() && elem.CanInterface() {
			raw = elem.Interface()
		}

		elem, elemNil := unwrapExprValue(elem)
		if elemNil {
			hasNil = true
			continue
		}
		if elem.Kind() == reflect.Slice {
			stringSlicePool.Put(ixsBuf)
			return nil, false, fmt.Errorf("unsupported slice element type: %v", elem.Type())
		}

		key, err := scalarValueToIdxField(raw, elem, fm)
		if err != nil {
			stringSlicePool.Put(ixsBuf)
			return nil, false, err
		}
		ixsBuf.Append(key)
	}

	if ixsBuf.Len() == 0 {
		stringSlicePool.Put(ixsBuf)
		return nil, hasNil, nil
	}
	return ixsBuf, hasNil, nil
}

func (qv *queryView[K, V]) scalarLookupPostings(field string, fieldOrdinal int, keys stringKeyReader, includeNil bool) (*pooled.SliceBuf[posting.List], uint64) {
	postsBuf := postingSlicePool.Get()
	keyCount := stringKeyReaderLen(keys)
	postsBuf.Grow(keyCount + btoi(includeNil))
	var est uint64

	ov := qv.fieldOverlayRef(field, fieldOrdinal)
	for i := 0; i < keyCount; i++ {
		ids := ov.lookupPostingRetained(keys.Get(i))
		if ids.IsEmpty() {
			continue
		}
		postsBuf.Append(ids)
		est += ids.Cardinality()
	}
	if includeNil {
		ids := qv.nilFieldOverlayRef(field, fieldOrdinal).lookupPostingRetained(nilIndexEntryKey)
		if !ids.IsEmpty() {
			postsBuf.Append(ids)
			est += ids.Cardinality()
		}
	}
	return postsBuf, est
}

func (qv *queryView[K, V]) exprValueToIdxScalar(expr qir.Expr) (string, bool, bool, error) {
	if expr.Value == nil {
		return "", false, true, nil
	}

	fm := qv.fieldMetaByExpr(expr)
	v := reflect.ValueOf(expr.Value)
	v, isNil := unwrapExprValue(v)
	if isNil {
		return "", false, true, nil
	}
	if queryValueIsCollectionForField(v, fm) {
		return "", true, false, nil
	}

	key, err := scalarValueToIdxField(expr.Value, v, fm)
	if err != nil {
		return "", false, false, err
	}
	return key, false, false, nil
}

func (qv *queryView[K, V]) diffPostingResult(acc, sub postingResult) (postingResult, error) {
	acc.ids = acc.ids.BuildAndNot(sub.ids)
	sub.release()
	return acc, nil
}

// exprValueToDistinctIdxBuf returns caller-owned indexed values deduplicated
// for set-like operators whose semantics do not depend on input order.
func (qv *queryView[K, V]) exprValueToDistinctIdxBuf(expr qir.Expr) (*pooled.SliceBuf[string], bool, bool, error) {
	fm := qv.fieldMetaByExpr(expr)

	if expr.Value == nil {
		if expr.Op == qir.OpIN {
			return nil, true, false, nil
		}
		return nil, false, true, nil
	}
	switch v := expr.Value.(type) {
	case []string:
		if fm != nil && (fm.UseVI || fm.Kind == reflect.String) {
			if len(v) == 0 {
				return nil, true, false, nil
			}
			valsBuf := stringSlicePool.Get()
			valsBuf.Grow(len(v))
			valsBuf.AppendAll(v)
			dedupStringBufInPlace(valsBuf)
			if valsBuf.Len() == 0 {
				stringSlicePool.Put(valsBuf)
				return nil, true, false, nil
			}
			return valsBuf, true, false, nil
		}
	}

	v := reflect.ValueOf(expr.Value)
	v, isNil := unwrapExprValue(v)
	if isNil {
		if expr.Op == qir.OpIN {
			return nil, true, false, nil
		}
		return nil, false, true, nil
	}

	if queryValueIsCollectionForField(v, fm) {
		valsBuf, hasNil, err := sliceValueToIdxStringBuf(v, fm)
		if err != nil {
			return nil, true, false, err
		}
		if valsBuf == nil {
			return nil, true, hasNil, nil
		}
		dedupStringBufInPlace(valsBuf)
		if valsBuf.Len() == 0 {
			stringSlicePool.Put(valsBuf)
			return nil, true, hasNil, nil
		}
		return valsBuf, true, hasNil, nil
	}

	return nil, false, false, nil
}

type postingResult struct {
	ids posting.List
	neg bool
}

func (b postingResult) release() {
	b.ids.Release()
}

func diffOwned(a, b posting.List) posting.List {
	res := a.Clone()
	if !b.IsEmpty() {
		res = res.BuildAndNot(b)
	}
	return res
}

func (qv *queryView[K, V]) andPostingResult(a, b postingResult) (postingResult, error) {
	// handle empty sets
	if !a.neg && a.ids.IsEmpty() {
		a.release()
		b.release()
		return postingResult{}, nil
	}
	if !b.neg && b.ids.IsEmpty() {
		a.release()
		b.release()
		return postingResult{}, nil
	}

	switch {

	case !a.neg && !b.neg:
		if a.ids.Cardinality() <= b.ids.Cardinality() {
			a.ids = a.ids.BuildAnd(b.ids)
			b.release()
			return a, nil
		}
		b.ids = b.ids.BuildAnd(a.ids)
		a.release()
		return b, nil

	case a.neg && b.neg:
		if a.ids.Cardinality() >= b.ids.Cardinality() {
			a.ids = a.ids.BuildOr(b.ids)
			b.release()
			a.neg = true
			return a, nil
		}
		b.ids = b.ids.BuildOr(a.ids)
		a.release()
		b.neg = true
		return b, nil

	case a.neg && !b.neg:
		// NOT A AND B ==  B \ A
		return qv.diffPostingResult(b, a)

	case !a.neg && b.neg:
		// A AND NOT B  ->  A \ B
		return qv.diffPostingResult(a, b)
	}

	return postingResult{}, nil
}

func (qv *queryView[K, V]) orPostingResult(a, b postingResult) postingResult {
	// universe (negative empty)
	if a.neg && a.ids.IsEmpty() {
		a.release()
		b.release()
		return postingResult{neg: true}
	}
	if b.neg && b.ids.IsEmpty() {
		a.release()
		b.release()
		return postingResult{neg: true}
	}

	// standard empty
	if !a.neg && a.ids.IsEmpty() {
		a.release()
		return b
	}
	if !b.neg && b.ids.IsEmpty() {
		b.release()
		return a
	}

	switch {

	case !a.neg && !b.neg:
		if a.ids.Cardinality() >= b.ids.Cardinality() {
			a.ids = a.ids.BuildOr(b.ids)
			b.release()
			return a
		}
		b.ids = b.ids.BuildOr(a.ids)
		a.release()
		return b

	case a.neg && b.neg:
		if a.ids.Cardinality() <= b.ids.Cardinality() {
			a.ids = a.ids.BuildAnd(b.ids)
			b.release()
			a.neg = true
			return a
		}
		b.ids = b.ids.BuildAnd(a.ids)
		a.release()
		b.neg = true
		return b

	case a.neg && !b.neg:
		// NOT A OR B  ->  NOT (A \ B)
		payload := diffOwned(a.ids, b.ids)
		a.release()
		b.release()
		return postingResult{ids: payload, neg: true}

	case !a.neg && b.neg:
		// A OR NOT B  ->  NOT (B \ A)
		payload := diffOwned(b.ids, a.ids)
		a.release()
		b.release()
		return postingResult{ids: payload, neg: true}
	}

	return postingResult{}
}
