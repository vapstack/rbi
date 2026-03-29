package rbi

import (
	"fmt"
	"math"
	"reflect"
	"slices"
	"sync"

	"github.com/vapstack/qx"

	"github.com/vapstack/rbi/internal/posting"
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

func (qv *queryView[K, V]) checkUsedQuery(q *qx.QX) error {
	for _, o := range q.Order {
		if _, ok := qv.fields[o.Field]; !ok {
			return fmt.Errorf("no index for field: %v", o.Field)
		}
	}
	return qv.checkUsedExpr(q.Expr)
}

func (qv *queryView[K, V]) checkUsedExpr(exp qx.Expr) error {
	if exp.Field != "" {
		if _, ok := qv.fields[exp.Field]; !ok {
			return fmt.Errorf("no index for field: %v", exp.Field)
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
func (qv *queryView[K, V]) evalExpr(e qx.Expr) (postingResult, error) {
	switch e.Op {

	case qx.OpNOOP:
		if e.Field != "" || e.Value != nil || len(e.Operands) != 0 {
			return postingResult{}, fmt.Errorf("%w: invalid expression, op: %v", ErrInvalidQuery, e.Op)
		}
		res := postingResult{neg: true}
		if e.Not {
			res.neg = !res.neg
		}
		return res, nil

	case qx.OpAND:
		return qv.evalAndOperands(e.Operands, e.Not)

	case qx.OpOR:
		if len(e.Operands) == 0 {
			return postingResult{}, fmt.Errorf("%w: empty OR expression", ErrInvalidQuery)
		}

		positive := getPostingResultSliceBuf(len(e.Operands))
		defer releasePostingResultSliceBuf(positive)

		negative := getPostingResultSliceBuf(len(e.Operands))
		defer releasePostingResultSliceBuf(negative)

		releaseAll := func(xs []postingResult) {
			for _, x := range xs {
				x.release()
			}
		}

		for _, op := range e.Operands {
			b, err := qv.evalExpr(op)
			if err != nil {
				releaseAll(positive.values)
				releaseAll(negative.values)
				return postingResult{}, err
			}

			// short-circuit: universe OR ... == universe
			if b.neg && b.ids.IsEmpty() {
				b.release()
				releaseAll(positive.values)
				releaseAll(negative.values)
				res := postingResult{neg: true}
				if e.Not {
					res.neg = !res.neg
				}
				return res, nil
			}

			if b.neg {
				negative.values = append(negative.values, b)
				continue
			}

			if b.ids.IsEmpty() {
				b.release()
				continue
			}

			positive.values = append(positive.values, b)
		}

		// 1. Merge positive branches first; this captures the common OR pattern
		// and keeps negative-branch handling as a rare slow path.
		var res postingResult
		if len(positive.values) > 0 {
			res = positive.values[0]
			for i := 1; i < len(positive.values); i++ {
				res = qv.orPostingResult(res, positive.values[i])
			}
		}

		// 2. Merge negatives (A OR NOT B) via postingResult algebra.
		for _, neg := range negative.values {
			res = qv.orPostingResult(res, neg)
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

func (qv *queryView[K, V]) evalAndOperands(ops []qx.Expr, negate bool) (postingResult, error) {
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

func (qv *queryView[K, V]) evalAndOperandsExcept(ops []qx.Expr, skip int) (postingResult, error) {
	if len(ops) == 0 || (skip >= 0 && len(ops) <= 1) {
		return postingResult{}, fmt.Errorf("%w: empty AND expression", ErrInvalidQuery)
	}

	var (
		first    postingResult
		hasFirst bool
	)

	for i, op := range ops {
		if i == skip {
			continue
		}

		b, err := qv.evalExpr(op)
		if err != nil {
			if hasFirst {
				first.release()
			}
			return postingResult{}, err
		}

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

	if !hasFirst {
		return postingResult{}, fmt.Errorf("%w: empty AND expression", ErrInvalidQuery)
	}
	return first, nil
}

// evalSimple evaluates a single non-boolean predicate against the current
// immutable snapshot indexes.
func (qv *queryView[K, V]) evalSimple(e qx.Expr) (postingResult, error) {
	ov := qv.fieldOverlay(e.Field)
	if !ov.hasData() && !qv.hasIndexedField(e.Field) {
		return postingResult{}, fmt.Errorf("no index for field: %v", e.Field)
	}

	f := qv.fields[e.Field]
	if f == nil {
		return postingResult{}, fmt.Errorf("no metadata for field: %v", e.Field)
	}

	switch e.Op {
	case qx.OpEQ:
		if !f.Slice {
			key, isSlice, isNil, err := qv.exprValueToIdxScalar(e)
			if err != nil {
				return postingResult{}, err
			}
			if isSlice {
				return postingResult{}, fmt.Errorf("%w: %v expects a single value for scalar field %v", ErrInvalidQuery, e.Op, e.Field)
			}

			var ids posting.List
			if isNil {
				ids = qv.nilFieldOverlay(e.Field).lookupPostingRetained(nilIndexEntryKey)
			} else {
				ids = ov.lookupPostingRetained(key)
			}
			if !ids.IsEmpty() {
				return postingResult{ids: ids}, nil
			}
			return postingResult{}, nil

		} else {
			vals, isSlice, _, err := qv.exprValueToDistinctIdxOwned(e)
			if err != nil {
				return postingResult{}, err
			}
			if !isSlice {
				return postingResult{}, fmt.Errorf("%w: %v expects a slice for slice field %v", ErrInvalidQuery, e.Op, e.Field)
			}
			return qv.evalSliceEQ(e.Field, vals)
		}

	case qx.OpIN:
		if f.Slice {
			return postingResult{}, fmt.Errorf("%w: %v not supported on slice field %v", ErrInvalidQuery, e.Op, e.Field)
		}
		vals, isSlice, hasNil, err := qv.exprValueToDistinctIdxOwned(e)
		if err != nil {
			return postingResult{}, err
		}
		if !isSlice && e.Value != nil {
			return postingResult{}, fmt.Errorf("%w: %v expects a slice", ErrInvalidQuery, e.Op)
		}
		if len(vals) == 0 && !hasNil {
			return postingResult{}, fmt.Errorf("%v: %v: no values provided", ErrInvalidQuery, e.Op)
		}

		var res posting.List

		for _, v := range vals {
			ids := ov.lookupPostingRetained(v)
			if ids.IsEmpty() {
				continue
			}
			ids.OrInto(&res)
		}
		if hasNil {
			ids := qv.nilFieldOverlay(e.Field).lookupPostingRetained(nilIndexEntryKey)
			if !ids.IsEmpty() {
				ids.OrInto(&res)
			}
		}
		return postingResult{ids: res}, nil

	case qx.OpHASANY, qx.OpHAS:
		if !f.Slice {
			return postingResult{}, fmt.Errorf("%w: %v not supported on non-slice field %v", ErrInvalidQuery, e.Op, e.Field)
		}
		vals, isSlice, _, err := qv.exprValueToIdxBorrowed(e)
		if err != nil {
			return postingResult{}, err
		}
		if !isSlice && e.Value != nil {
			return postingResult{}, fmt.Errorf("%w: %v expects a slice", ErrInvalidQuery, e.Op)
		}
		if len(vals) == 0 {
			return postingResult{}, fmt.Errorf("%v: %v: no values provided", ErrInvalidQuery, e.Op)
		}

		if e.Op == qx.OpHAS {
			var acc posting.List
			for _, v := range vals {
				ids := ov.lookupPostingRetained(v)
				if ids.IsEmpty() {
					// if any value is missing, result is empty
					acc.Release()
					return postingResult{}, nil
				}
				if acc.IsEmpty() {
					acc = ids.Clone()
				} else {
					acc.AndInPlace(ids)
				}
				if acc.IsEmpty() {
					return postingResult{ids: acc}, nil
				}
			}
			return postingResult{ids: acc}, nil
		}

		// HASANY - OR logic
		var res posting.List

		for _, v := range vals {
			ids := ov.lookupPostingRetained(v)
			if ids.IsEmpty() {
				continue
			}
			ids.OrInto(&res)
		}
		return postingResult{ids: res}, nil

	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
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
		cacheKey := ""
		if !bound.full {
			cacheKey = qv.materializedPredCacheKeyForScalar(e.Field, bound.op, bound.key)
		}
		if cacheKey != "" {
			if cached, ok := qv.snap.loadMaterializedPred(cacheKey); ok {
				if cached.IsEmpty() {
					return postingResult{}, nil
				}
				return postingResult{ids: cached}, nil
			}
		}

		rb := rangeBounds{has: true}
		if bound.full {
			rb = rangeBounds{has: true}
		} else {
			switch bound.op {
			case qx.OpGT:
				rb.applyLo(bound.key, false)
			case qx.OpGTE:
				rb.applyLo(bound.key, true)
			case qx.OpLT:
				rb.applyHi(bound.key, false)
			case qx.OpLTE:
				rb.applyHi(bound.key, true)
			case qx.OpPREFIX:
				rb.hasPrefix = true
				rb.prefix = bound.key
			case qx.OpEQ:
				rb.applyLo(bound.key, true)
				rb.applyHi(bound.key, true)
			}
		}

		br := ov.rangeForBounds(rb)
		if overlayRangeEmpty(br) {
			if cacheKey != "" {
				qv.snap.storeMaterializedPred(cacheKey, posting.List{})
			}
			return postingResult{}, nil
		}

		// Prefix keeps its dedicated path; other numeric ranges can use bucket
		// routing regardless of predicate cache sharing.
		if e.Op != qx.OpPREFIX {
			if out, ok := qv.tryEvalNumericRangeBuckets(e.Field, f, ov, br); ok {
				if cacheKey != "" {
					qv.tryShareMaterializedPred(cacheKey, &out.ids)
				}
				return out, nil
			}
		}

		var res posting.List
		spanLen := br.baseEnd - br.baseStart
		bulkSingles := spanLen > 0 && spanLen <= singleAdaptiveMaxLen

		var singles *singleIDsBuffer
		if bulkSingles {
			singles = getSingleIDsBuf()
			defer releaseSingleIDs(singles)
		}

		flushSingles := func() {
			if singles == nil || len(singles.values) == 0 {
				return
			}
			slices.Sort(singles.values)
			res.AddMany(singles.values)
			singles.values = singles.values[:0]
		}

		cur := ov.newCursor(br, false)
		for {
			_, ids, ok := cur.next()
			if !ok {
				break
			}
			if ids.IsEmpty() {
				continue
			}

			// Singleton-heavy ranges are accumulated via batched AddMany
			// because repeated OR of tiny bitmaps is much more expensive.
			if idx, ok := ids.TrySingle(); ok {
				if bulkSingles {
					singles.values = append(singles.values, idx)
					if len(singles.values) == cap(singles.values) {
						flushSingles()
					}
				} else {
					res.Add(idx)
				}
				continue
			}

			flushSingles()
			ids.OrInto(&res)
		}

		flushSingles()
		qv.tryShareMaterializedPred(cacheKey, &res)
		return postingResult{ids: res}, nil

	case qx.OpSUFFIX, qx.OpCONTAINS:
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
		cacheKey := qv.materializedPredCacheKeyForScalar(e.Field, e.Op, v)
		if cacheKey != "" {
			if cached, ok := qv.snap.loadMaterializedPred(cacheKey); ok {
				if cached.IsEmpty() {
					return postingResult{}, nil
				}
				return postingResult{ids: cached}, nil
			}
		}

		var res posting.List
		full := ov.rangeForBounds(rangeBounds{has: true})
		spanLen := full.baseEnd - full.baseStart
		bulkSingles := spanLen > 0 && spanLen <= singleAdaptiveMaxLen

		var singles *singleIDsBuffer
		if bulkSingles {
			singles = getSingleIDsBuf()
			defer releaseSingleIDs(singles)
		}

		flushSingles := func() {
			if singles == nil || len(singles.values) == 0 {
				return
			}
			slices.Sort(singles.values)
			res.AddMany(singles.values)
			singles.values = singles.values[:0]
		}

		cur := ov.newCursor(full, false)
		for {
			key, ids, ok := cur.next()
			if !ok {
				break
			}
			match := e.Op == qx.OpSUFFIX && indexKeyHasSuffixString(key, v) ||
				e.Op == qx.OpCONTAINS && indexKeyContainsString(key, v)
			if !match {
				continue
			}
			if ids.IsEmpty() {
				continue
			}

			// Same singleton optimization for suffix/contains scans over wide
			// dictionary spans.
			if idx, ok := ids.TrySingle(); ok {
				if bulkSingles {
					singles.values = append(singles.values, idx)
					if len(singles.values) == cap(singles.values) {
						flushSingles()
					}
				} else {
					res.Add(idx)
				}
				continue
			}
			flushSingles()
			ids.OrInto(&res)
		}
		flushSingles()
		qv.tryShareMaterializedPred(cacheKey, &res)
		return postingResult{ids: res}, nil

	default:
		return postingResult{}, fmt.Errorf("unsupported op: %v", e.Op)
	}
}

func (qv *queryView[K, V]) unionPostings(posts []posting.List) posting.List {
	dst := posts[:0]
	for _, ids := range posts {
		if ids.IsEmpty() {
			continue
		}
		dst = append(dst, ids)
	}
	posts = dst
	n := len(posts)
	switch n {
	case 0:
		return posting.List{}
	case 1:
		return posts[0].Clone()
	}

	// Few postings do not justify goroutine overhead.
	if n < 256 {
		return qv.linearPostingUnion(posts)
	}
	return qv.parallelBatchedPostingUnion(posts)
}

func (qv *queryView[K, V]) linearPostingUnion(posts []posting.List) posting.List {
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
		res.OrInPlace(ids)
	}
	res.Optimize()
	return res
}

func (qv *queryView[K, V]) parallelBatchedPostingUnion(posts []posting.List) posting.List {
	n := len(posts)

	workers := 8
	if n < workers*2 {
		workers = n / 2
	}
	if workers < 2 {
		return qv.linearPostingUnion(posts)
	}

	chunkSize := (n + workers - 1) / workers
	results := make([]posting.List, workers)

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
			results[idx] = qv.linearPostingUnion(part)
		}(i, posts[start:end])
	}

	wg.Wait()

	final := results[0]
	for i := 1; i < len(results); i++ {
		final.MergeOwned(results[i])
	}
	final.Optimize()
	return final
}

// evalSliceEQ evaluates equality for slice fields by intersecting member
// bitmaps, because slice-EQ semantics require full set match.
//
// Callers pass distinct values; duplicates are removed before this point so
// this path can stay read-only.
func (qv *queryView[K, V]) evalSliceEQ(field string, vals []string) (postingResult, error) {
	lenOV := qv.lenFieldOverlay(field)
	useZeroComplement := len(vals) == 0 && qv.isLenZeroComplementField(field)
	if !lenOV.hasData() && qv.snapshotLenFieldIndexSlice(field) == nil && !useZeroComplement {
		return postingResult{}, fmt.Errorf("no lenIndex for slice field: %v", field)
	}

	var (
		lenBM posting.List
	)
	if useZeroComplement {
		nonEmpty := lenOV.lookupPostingRetained(lenIndexNonEmptyKey)
		lenBM = qv.snapshotUniverseView().Clone()
		lenBM.AndNotInPlace(nonEmpty)
	} else {
		lenKey := uint64ByteStr(uint64(len(vals)))
		lenBM = lenOV.lookupPostingRetained(lenKey)
	}
	if lenBM.IsEmpty() {
		return postingResult{}, nil
	}

	ov := qv.fieldOverlay(field)
	acc := lenBM.Clone()
	for _, v := range vals {
		ids := ov.lookupPostingRetained(v)
		if ids.IsEmpty() {
			acc.Release()
			return postingResult{}, nil
		}
		acc.AndInPlace(ids)
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
	op    qx.Op
	key   string
	full  bool
	empty bool
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

func normalizeSignedIntRangeBound(op qx.Op, v reflect.Value) normalizedScalarBound {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return normalizedScalarBound{op: op, key: int64ByteStr(v.Int())}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u := v.Uint()
		if u > math.MaxInt64 {
			switch op {
			case qx.OpGT, qx.OpGTE:
				return normalizedScalarBound{empty: true}
			default:
				return normalizedScalarBound{full: true}
			}
		}
		return normalizedScalarBound{op: op, key: int64ByteStr(int64(u))}
	case reflect.Float32, reflect.Float64:
		f := canonicalizeFloat64ForIndex(v.Float())
		switch {
		case math.IsNaN(f), math.IsInf(f, 1):
			switch op {
			case qx.OpGT, qx.OpGTE:
				return normalizedScalarBound{empty: true}
			default:
				return normalizedScalarBound{full: true}
			}
		case math.IsInf(f, -1):
			switch op {
			case qx.OpGT, qx.OpGTE:
				return normalizedScalarBound{full: true}
			default:
				return normalizedScalarBound{empty: true}
			}
		case f == math.Trunc(f):
			if f < math.MinInt64 || f > math.MaxInt64 {
				switch op {
				case qx.OpGT, qx.OpGTE:
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
			return normalizedScalarBound{op: op, key: int64ByteStr(int64(f))}
		case op == qx.OpEQ:
			return normalizedScalarBound{empty: true}
		case op == qx.OpGT || op == qx.OpGTE:
			floor := math.Floor(f)
			if floor < math.MinInt64 {
				return normalizedScalarBound{full: true}
			}
			if floor > math.MaxInt64 {
				return normalizedScalarBound{empty: true}
			}
			return normalizedScalarBound{op: qx.OpGT, key: int64ByteStr(int64(floor))}
		default:
			ceil := math.Ceil(f)
			if ceil < math.MinInt64 {
				return normalizedScalarBound{empty: true}
			}
			if ceil > math.MaxInt64 {
				return normalizedScalarBound{full: true}
			}
			return normalizedScalarBound{op: qx.OpLT, key: int64ByteStr(int64(ceil))}
		}
	default:
		return normalizedScalarBound{empty: true}
	}
}

func normalizeUnsignedIntRangeBound(op qx.Op, v reflect.Value) normalizedScalarBound {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i := v.Int()
		if i < 0 {
			switch op {
			case qx.OpGT, qx.OpGTE:
				return normalizedScalarBound{full: true}
			default:
				return normalizedScalarBound{empty: true}
			}
		}
		return normalizedScalarBound{op: op, key: uint64ByteStr(uint64(i))}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return normalizedScalarBound{op: op, key: uint64ByteStr(v.Uint())}
	case reflect.Float32, reflect.Float64:
		f := canonicalizeFloat64ForIndex(v.Float())
		switch {
		case math.IsNaN(f), math.IsInf(f, 1):
			switch op {
			case qx.OpGT, qx.OpGTE:
				return normalizedScalarBound{empty: true}
			default:
				return normalizedScalarBound{full: true}
			}
		case math.IsInf(f, -1):
			switch op {
			case qx.OpGT, qx.OpGTE:
				return normalizedScalarBound{full: true}
			default:
				return normalizedScalarBound{empty: true}
			}
		case f < 0:
			switch op {
			case qx.OpGT, qx.OpGTE:
				return normalizedScalarBound{full: true}
			default:
				return normalizedScalarBound{empty: true}
			}
		case f == math.Trunc(f):
			if f > float64(^uint64(0)) {
				switch op {
				case qx.OpGT, qx.OpGTE:
					return normalizedScalarBound{empty: true}
				default:
					return normalizedScalarBound{full: true}
				}
			}
			return normalizedScalarBound{op: op, key: uint64ByteStr(uint64(f))}
		case op == qx.OpEQ:
			return normalizedScalarBound{empty: true}
		case op == qx.OpGT || op == qx.OpGTE:
			floor := math.Floor(f)
			if floor > float64(^uint64(0)) {
				return normalizedScalarBound{empty: true}
			}
			return normalizedScalarBound{op: qx.OpGT, key: uint64ByteStr(uint64(floor))}
		default:
			ceil := math.Ceil(f)
			if ceil > float64(^uint64(0)) {
				return normalizedScalarBound{full: true}
			}
			return normalizedScalarBound{op: qx.OpLT, key: uint64ByteStr(uint64(ceil))}
		}
	default:
		return normalizedScalarBound{empty: true}
	}
}

func normalizeFloatRangeBound(op qx.Op, v reflect.Value) normalizedScalarBound {
	f, ok := numericQueryValueToFloat64(v)
	if !ok {
		return normalizedScalarBound{empty: true}
	}
	return normalizedScalarBound{op: op, key: float64ByteStr(f)}
}

func (qv *queryView[K, V]) exprValueToNormalizedScalarBound(expr qx.Expr) (normalizedScalarBound, bool, error) {
	if expr.Value == nil {
		return normalizedScalarBound{empty: true}, false, nil
	}

	fm := qv.fields[expr.Field]
	v := reflect.ValueOf(expr.Value)
	v, isNil := unwrapExprValue(v)
	if isNil {
		return normalizedScalarBound{empty: true}, false, nil
	}
	if queryValueIsCollectionForField(v, fm) {
		return normalizedScalarBound{}, true, nil
	}
	if expr.Op == qx.OpPREFIX {
		if fm != nil && fm.KeyKind == fieldWriteKeysOrderedU64 {
			return normalizedScalarBound{empty: true}, false, nil
		}
		key, err := scalarValueToIdxField(expr.Value, v, fm)
		if err != nil {
			return normalizedScalarBound{}, false, err
		}
		return normalizedScalarBound{op: qx.OpPREFIX, key: key}, false, nil
	}

	if fm != nil && !fm.UseVI {
		switch {
		case signedIntFieldKind(fm.Kind):
			return normalizeSignedIntRangeBound(expr.Op, v), false, nil
		case unsignedIntFieldKind(fm.Kind):
			return normalizeUnsignedIntRangeBound(expr.Op, v), false, nil
		case fm.Kind == reflect.Float32 || fm.Kind == reflect.Float64:
			return normalizeFloatRangeBound(expr.Op, v), false, nil
		}
	}

	key, err := scalarValueToIdxField(expr.Value, v, fm)
	if err != nil {
		return normalizedScalarBound{}, false, err
	}
	return normalizedScalarBound{op: expr.Op, key: key}, false, nil
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
	case qx.OpGT:
		rb.applyLo(b.key, false)
	case qx.OpGTE:
		rb.applyLo(b.key, true)
	case qx.OpLT:
		rb.applyHi(b.key, false)
	case qx.OpLTE:
		rb.applyHi(b.key, true)
	case qx.OpEQ:
		rb.applyLo(b.key, true)
		rb.applyHi(b.key, true)
	case qx.OpPREFIX:
		rb.applyPrefix(b.key)
	}
}

func sliceValueToIdxStrings(v reflect.Value, fm *field) ([]string, bool, error) {
	ixs := make([]string, 0, v.Len())
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
			return nil, false, fmt.Errorf("unsupported slice element type: %v", elem.Type())
		}

		key, err := scalarValueToIdxField(raw, elem, fm)
		if err != nil {
			return nil, false, err
		}
		ixs = append(ixs, key)
	}

	return ixs, hasNil, nil
}

func (qv *queryView[K, V]) scalarLookupPostings(field string, keys []string, includeNil bool) ([]posting.List, uint64, func()) {
	postsBuf := getPostingSliceBuf(len(keys) + btoi(includeNil))
	posts := postsBuf.values
	var est uint64

	ov := qv.fieldOverlay(field)
	for _, key := range keys {
		ids := ov.lookupPostingRetained(key)
		if ids.IsEmpty() {
			continue
		}
		posts = append(posts, ids)
		est += ids.Cardinality()
	}
	if includeNil {
		ids := qv.nilFieldOverlay(field).lookupPostingRetained(nilIndexEntryKey)
		if !ids.IsEmpty() {
			posts = append(posts, ids)
			est += ids.Cardinality()
		}
	}

	cleanup := func() {
		postsBuf.values = posts
		releasePostingSliceBuf(postsBuf)
	}
	return posts, est, cleanup
}

func (qv *queryView[K, V]) exprValueToIdxScalar(expr qx.Expr) (string, bool, bool, error) {
	if expr.Value == nil {
		return "", false, true, nil
	}

	fm := qv.fields[expr.Field]
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
	acc.ids.AndNotInPlace(sub.ids)
	sub.release()
	return acc, nil
}

// exprValueToIdxBorrowed returns indexed values that may alias the original
// query input when expr.Value is already a []string. Callers must not mutate
// the returned slice.
func (qv *queryView[K, V]) exprValueToIdxBorrowed(expr qx.Expr) ([]string, bool, bool, error) {
	return qv.exprValueToIdxWithOwnership(expr, false)
}

// exprValueToIdxOwned returns indexed values backed by caller-owned storage and
// safe for in-place mutation.
func (qv *queryView[K, V]) exprValueToIdxOwned(expr qx.Expr) ([]string, bool, bool, error) {
	return qv.exprValueToIdxWithOwnership(expr, true)
}

// exprValueToDistinctIdxOwned returns caller-owned indexed values deduplicated
// for set-like operators whose semantics do not depend on input order.
func (qv *queryView[K, V]) exprValueToDistinctIdxOwned(expr qx.Expr) ([]string, bool, bool, error) {
	vals, isSlice, hasNil, err := qv.exprValueToIdxOwned(expr)
	if err != nil {
		return nil, isSlice, hasNil, err
	}
	return dedupStringsInplace(vals), isSlice, hasNil, nil
}

func (qv *queryView[K, V]) exprValueToIdxWithOwnership(expr qx.Expr, ownStringSlice bool) ([]string, bool, bool, error) {
	fm := qv.fields[expr.Field]

	if expr.Value == nil {
		if expr.Op == qx.OpIN {
			return nil, true, false, nil
		}
		return nil, false, true, nil
	}
	switch v := expr.Value.(type) {
	case []string:
		if fm != nil && (fm.UseVI || fm.Kind == reflect.String) {
			if ownStringSlice {
				return slices.Clone(v), true, false, nil
			}
			return v, true, false, nil
		}
	case string:
		if fm != nil && (fm.UseVI || fm.Kind == reflect.String) {
			return []string{v}, false, false, nil
		}
	}

	v := reflect.ValueOf(expr.Value)
	v, isNil := unwrapExprValue(v)
	if isNil {
		if expr.Op == qx.OpIN {
			return nil, true, false, nil
		}
		return nil, false, true, nil
	}

	if queryValueIsCollectionForField(v, fm) {
		ixs, hasNil, err := sliceValueToIdxStrings(v, fm)
		if err != nil {
			return nil, true, false, err
		}
		return ixs, true, hasNil, nil
	}

	key, err := scalarValueToIdxField(expr.Value, v, fm)
	if err != nil {
		return nil, false, false, err
	}
	ixs := make([]string, 1)
	ixs[0] = key

	return ixs, false, false, nil
}

type postingResult struct {
	ids posting.List
	neg bool
}

func (b postingResult) release() {
	b.ids.Release()
}

func (b postingResult) clone() postingResult {
	if b.ids.IsEmpty() {
		return b
	}
	return postingResult{ids: b.ids.Clone(), neg: b.neg}
}

func diffOwned(a, b posting.List) posting.List {
	res := a.Clone()
	if !b.IsEmpty() {
		b.AndNotFrom(&res)
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
			a.ids.AndInPlace(b.ids)
			b.release()
			return a, nil
		}
		b.ids.AndInPlace(a.ids)
		a.release()
		return b, nil

	case a.neg && b.neg:
		if a.ids.Cardinality() >= b.ids.Cardinality() {
			a.ids.OrInPlace(b.ids)
			b.release()
			a.neg = true
			return a, nil
		}
		b.ids.OrInPlace(a.ids)
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
			a.ids.OrInPlace(b.ids)
			b.release()
			return a
		}
		b.ids.OrInPlace(a.ids)
		a.release()
		return b

	case a.neg && b.neg:
		if a.ids.Cardinality() <= b.ids.Cardinality() {
			a.ids.AndInPlace(b.ids)
			b.release()
			a.neg = true
			return a
		}
		b.ids.AndInPlace(a.ids)
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
