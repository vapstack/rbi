package qexec

import (
	"fmt"
	"math"
	"reflect"
	"slices"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/qir"

	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/rbierrors"
)

// evalExpr evaluates a boolean expression tree into a postingResult representation.
//
// Results may be positive sets or negative/complement sets to
// postpone expensive universe materialization until it is actually required.
func (qv *View) evalExpr(e qir.Expr) (postingResult, error) {
	switch e.Op {

	case qir.OpConst:
		if e.FieldOrdinal != qir.NoFieldOrdinal || e.Value != nil || len(e.Operands) != 0 {
			return postingResult{}, fmt.Errorf("%w: invalid expression, op: %v", rbierrors.ErrInvalidQuery, e.Op)
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
			return postingResult{}, fmt.Errorf("%w: empty OR expression", rbierrors.ErrInvalidQuery)
		}

		positive := postingResultSlicePool.Get(len(e.Operands))
		negative := postingResultSlicePool.Get(len(e.Operands))

		for _, op := range e.Operands {
			b, err := qv.evalExpr(op)
			if err != nil {
				releasePostingResults(positive)
				releasePostingResults(negative)
				postingResultSlicePool.Put(positive)
				postingResultSlicePool.Put(negative)
				return postingResult{}, err
			}

			// short-circuit: universe OR ... == universe
			if b.neg && b.ids.IsEmpty() {
				b.ids.Release()
				releasePostingResults(positive)
				releasePostingResults(negative)
				res := postingResult{neg: true}
				if e.Not {
					res.neg = !res.neg
				}
				postingResultSlicePool.Put(positive)
				postingResultSlicePool.Put(negative)
				return res, nil
			}

			if b.neg {
				negative = append(negative, b)
				continue
			}

			if b.ids.IsEmpty() {
				b.ids.Release()
				continue
			}

			positive = append(positive, b)
		}

		// 1. Merge positive branches first; this captures the common OR pattern
		// and keeps negative-branch handling as a rare slow path.
		var res postingResult
		if len(positive) > 0 {
			res = positive[0]
			for i := 1; i < len(positive); i++ {
				res = qv.orPostingResult(res, positive[i])
			}
		}

		// 2. Merge negatives (A OR NOT B) via postingResult algebra.
		for i := 0; i < len(negative); i++ {
			res = qv.orPostingResult(res, negative[i])
		}

		if e.Not {
			res.neg = !res.neg
		}
		postingResultSlicePool.Put(positive)
		postingResultSlicePool.Put(negative)
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

func (qv *View) evalAndOperands(ops []qir.Expr, negate bool) (postingResult, error) {
	if len(ops) == 0 {
		return postingResult{}, fmt.Errorf("%w: empty AND expression", rbierrors.ErrInvalidQuery)
	}

	if len(ops) > 1 && qv.hasEvalMergeableNumericRangeOperands(ops) {
		res, ok, err := qv.evalAndOperandsWithMergedNumericRanges(ops, negate)
		if ok || err != nil {
			return res, err
		}
	}

	return qv.evalAndOperandsDefault(ops, negate)
}

func (qv *View) hasEvalMergeableNumericRangeOperands(ops []qir.Expr) bool {
	for i := 0; i < len(ops)-1; i++ {
		e := ops[i]
		if !qv.isPositiveMergedNumericRangeLeaf(e) {
			continue
		}
		for j := i + 1; j < len(ops); j++ {
			next := ops[j]
			if next.FieldOrdinal == e.FieldOrdinal && qv.isPositiveMergedNumericRangeLeaf(next) {
				return true
			}
		}
	}
	return false
}

func (qv *View) evalAndOperandsWithMergedNumericRanges(ops []qir.Expr, negate bool) (postingResult, bool, error) {
	mergedRangesBuf := orderedMergedScalarRangeFieldSlicePool.Get(len(ops))
	var ok bool
	mergedRangesBuf, ok = qv.collectMergedNumericRangeFields(ops, mergedRangesBuf)
	if !ok {
		orderedMergedScalarRangeFieldSlicePool.Put(mergedRangesBuf)
		return postingResult{}, false, nil
	}
	defer orderedMergedScalarRangeFieldSlicePool.Put(mergedRangesBuf)

	var (
		first    postingResult
		hasFirst bool
	)

	for i, op := range ops {
		var (
			b      postingResult
			err    error
			merged bool
		)
		if qv.isPositiveMergedNumericRangeLeaf(op) {
			idx := findOrderedMergedScalarRangeField(mergedRangesBuf, qv.exec.FieldNameByOrdinal(op.FieldOrdinal))
			if idx >= 0 {
				group := mergedRangesBuf[idx]
				if group.count > 1 {
					if group.first != i {
						continue
					}
					merged = true
					b, err = qv.evalPreparedScalarExactRange(preparedScalarExactRange{
						field:        group.field,
						fieldOrdinal: group.expr.FieldOrdinal,
						bounds:       group.bounds,
						cacheKey:     qv.materializedPredKeyForExactScalarRange(group.field, group.bounds),
					})
				}
			}
		}
		if !merged {
			b, err = qv.evalExpr(op)
		}
		if err != nil {
			if hasFirst {
				first.ids.Release()
			}
			return postingResult{}, true, err
		}

		if !b.neg && b.ids.IsEmpty() {
			b.ids.Release()
			if hasFirst {
				first.ids.Release()
			}
			return postingResult{}, true, nil
		}

		if !hasFirst {
			first = b
			hasFirst = true
			continue
		}

		first, err = qv.andPostingResult(first, b)
		if err != nil {
			first.ids.Release()
			return postingResult{}, true, err
		}

		if !first.neg && first.ids.IsEmpty() {
			first.ids.Release()
			return postingResult{}, true, nil
		}
	}

	if negate {
		first.neg = !first.neg
	}
	return first, true, nil
}

func (qv *View) evalAndOperandsDefault(ops []qir.Expr, negate bool) (postingResult, error) {
	var (
		first    postingResult
		hasFirst bool
	)

	for _, op := range ops {
		b, err := qv.evalExpr(op)
		if err != nil {
			if hasFirst {
				first.ids.Release()
			}
			return postingResult{}, err
		}

		// short-circuit: if empty non-neg set is found, the result is empty
		if !b.neg && b.ids.IsEmpty() {
			b.ids.Release()
			if hasFirst {
				first.ids.Release()
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
			first.ids.Release()
			return postingResult{}, err
		}

		if !first.neg && first.ids.IsEmpty() {
			first.ids.Release()
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
func (qv *View) evalSimple(e qir.Expr) (postingResult, error) {
	fieldName := qv.exec.FieldNameByOrdinal(e.FieldOrdinal)

	ov := qv.fieldIndexViewFromSlotsForExpr(qv.snap.Index, e)
	if !ov.HasData() && !qv.hasIndexedFieldForExpr(e) {
		return postingResult{}, fmt.Errorf("no index for field: %v", fieldName)
	}

	f := qv.fieldMetaByExpr(e)
	if f == nil {
		return postingResult{}, fmt.Errorf("no metadata for field: %v", fieldName)
	}

	switch e.Op {
	case qir.OpEQ:
		if !f.Slice {
			key, isSlice, isNil, err := qv.exprValueToLookupKey(e)
			if err != nil {
				return postingResult{}, err
			}
			if isSlice {
				return postingResult{}, fmt.Errorf("%w: %v expects a single value for scalar field %v", rbierrors.ErrInvalidQuery, e.Op, fieldName)
			}

			var ids posting.List
			if isNil {
				ids = qv.fieldIndexViewFromSlotsForExpr(qv.snap.NilIndex, e).LookupPostingRetained(indexdata.NilIndexEntryKey)
			} else {
				ids = lookupScalarPostingRetained(ov, key)
			}
			if !ids.IsEmpty() {
				return postingResult{ids: ids}, nil
			}
			return postingResult{}, nil

		}

		valsBuf, isSlice, _, err := qv.exprValueToDistinctLookupKeyBuf(e)
		if err != nil {
			return postingResult{}, err
		}
		if valsBuf != nil {
			defer keycodec.ReleaseIndexLookupKeySlice(valsBuf)
		}
		if !isSlice {
			return postingResult{}, fmt.Errorf("%w: %v expects a slice for slice field %v", rbierrors.ErrInvalidQuery, e.Op, fieldName)
		}
		return qv.evalSliceEQ(fieldName, e.FieldOrdinal, valsBuf)

	case qir.OpIN:
		if f.Slice {
			return postingResult{}, fmt.Errorf("%w: %v not supported on slice field %v", rbierrors.ErrInvalidQuery, e.Op, fieldName)
		}
		valsBuf, isSlice, hasNil, err := qv.exprValueToDistinctLookupKeyBuf(e)
		if err != nil {
			return postingResult{}, err
		}
		if valsBuf != nil {
			defer keycodec.ReleaseIndexLookupKeySlice(valsBuf)
		}
		if !isSlice && e.Value != nil {
			return postingResult{}, fmt.Errorf("%w: %v expects a slice", rbierrors.ErrInvalidQuery, e.Op)
		}
		valCount := len(valsBuf)
		if valCount == 0 && !hasNil {
			return postingResult{}, fmt.Errorf("%w: %v: no values provided", rbierrors.ErrInvalidQuery, e.Op)
		}

		capHint := valCount
		if hasNil {
			capHint++
		}

		builder := newPostingUnionBuilder(postingBatchSinglesEnabled(uint64(capHint)))
		defer builder.release()

		for i := 0; i < valCount; i++ {
			ids := lookupScalarPostingRetained(ov, valsBuf[i])
			if ids.IsEmpty() {
				continue
			}
			builder.addPosting(ids)
		}
		if hasNil {
			ids := qv.fieldIndexViewFromSlotsForExpr(qv.snap.NilIndex, e).LookupPostingRetained(indexdata.NilIndexEntryKey)
			if !ids.IsEmpty() {
				builder.addPosting(ids)
			}
		}
		return postingResult{ids: builder.finish(false)}, nil

	case qir.OpHASANY, qir.OpHASALL:
		if !f.Slice {
			return postingResult{}, fmt.Errorf("%w: %v not supported on non-slice field %v", rbierrors.ErrInvalidQuery, e.Op, fieldName)
		}
		valsBuf, isSlice, _, err := qv.exprValueToDistinctLookupKeyBuf(e)
		if err != nil {
			return postingResult{}, err
		}
		if valsBuf != nil {
			defer keycodec.ReleaseIndexLookupKeySlice(valsBuf)
		}
		if !isSlice && e.Value != nil {
			return postingResult{}, fmt.Errorf("%w: %v expects a slice", rbierrors.ErrInvalidQuery, e.Op)
		}
		valCount := len(valsBuf)
		if valCount == 0 {
			return postingResult{}, fmt.Errorf("%w: %v: no values provided", rbierrors.ErrInvalidQuery, e.Op)
		}

		if e.Op == qir.OpHASALL {
			var acc posting.List
			for i := 0; i < valCount; i++ {
				ids := lookupScalarPostingRetained(ov, valsBuf[i])
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
			ids := lookupScalarPostingRetained(ov, valsBuf[i])
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
			return postingResult{}, fmt.Errorf("%w: %v expects a single value", rbierrors.ErrInvalidQuery, e.Op)
		}
		if bound.empty {
			return postingResult{}, nil
		}
		var core preparedScalarRangePredicate
		qv.initPreparedScalarRangePredicateFromBound(&core, e, f, bound)
		return core.evalMaterializedPostingResult(ov), nil

	case qir.OpSUFFIX, qir.OpCONTAINS:
		v, isSlice, isNil, err := qv.exprValueToIdxScalar(e)
		if err != nil {
			return postingResult{}, err
		}
		if isSlice {
			return postingResult{}, fmt.Errorf("%w: %v expects a single string value", rbierrors.ErrInvalidQuery, e.Op)
		}
		if isNil {
			return postingResult{}, nil
		}
		cacheKey := qv.materializedPredKeyForScalar(fieldName, e.Op, v)
		if !cacheKey.IsZero() {
			if cached, ok := qv.snap.LoadMaterializedPredKey(cacheKey); ok {
				if cached.IsEmpty() {
					cached.Release()
					return postingResult{}, nil
				}
				return postingResult{ids: cached}, nil
			}
		}

		full := ov.RangeForBounds(indexdata.Bounds{Has: true})
		spanLen := full.BaseEnd - full.BaseStart

		builder := newPostingUnionBuilder(spanLen > 0 && spanLen <= singleAdaptiveMaxLen)
		defer builder.release()

		cur := ov.NewCursor(full, false)
		for {
			key, ids, ok := cur.Next()
			if !ok {
				break
			}
			match := e.Op == qir.OpSUFFIX && keycodec.HasSuffixString(key, v) ||
				e.Op == qir.OpCONTAINS && keycodec.ContainsString(key, v)
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

// evalSliceEQ evaluates equality for slice fields by intersecting member
// bitmaps, because slice-EQ semantics require full set match.
//
// Callers pass distinct values; duplicates are removed before this point so
// this path can stay read-only.
func (qv *View) evalSliceEQ(field string, fieldOrdinal int, vals []keycodec.IndexLookupKey) (postingResult, error) {
	valCount := len(vals)
	lenOV := qv.fieldIndexViewFromSlotsRef(qv.snap.LenIndex, field, fieldOrdinal)

	useZeroComplement := valCount == 0 && qv.isLenZeroComplementRef(field, fieldOrdinal)

	if !lenOV.HasData() && !useZeroComplement {
		if qv.snap.Universe.Cardinality() == 0 {
			return postingResult{}, nil
		}
		return postingResult{}, fmt.Errorf("no lenIndex for slice field: %v", field)
	}

	var lenBM posting.List

	if useZeroComplement {
		nonEmpty := lenOV.LookupPostingRetained(indexdata.LenIndexNonEmptyKey)
		lenBM = qv.snap.Universe.Borrow().Clone()
		lenBM = lenBM.BuildAndNot(nonEmpty)
	} else {
		lenBM = lenOV.LookupPostingRetainedKey(keycodec.FromU64(uint64(valCount)))
	}

	if lenBM.IsEmpty() {
		return postingResult{}, nil
	}

	ov := qv.fieldIndexViewFromSlotsRef(qv.snap.Index, field, fieldOrdinal)
	acc := lenBM.Clone()
	for i := 0; i < valCount; i++ {
		ids := lookupScalarPostingRetained(ov, vals[i])
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

func queryValueIsCollectionForField(v reflect.Value, fm *schema.Field) bool {
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
	keyIndex    keycodec.IndexKey
	hasIndexKey bool
	full        bool
	empty       bool
}

func normalizedScalarBoundFromIndexKey(op qir.Op, key keycodec.IndexKey) normalizedScalarBound {
	return normalizedScalarBound{
		op:          op,
		keyIndex:    key,
		hasIndexKey: true,
	}
}

func lookupScalarPostingRetained(ov indexdata.FieldIndexView, key keycodec.IndexLookupKey) posting.List {
	if key.IsNumeric() {
		return ov.LookupPostingRetainedKey(key.IndexKey())
	}
	return ov.LookupPostingRetained(key.StringKey())
}

func lookupScalarCardinality(ov indexdata.FieldIndexView, key keycodec.IndexLookupKey) uint64 {
	if key.IsNumeric() {
		return ov.LookupCardinalityKey(key.IndexKey())
	}
	return ov.LookupCardinality(key.StringKey())
}

func lookupScalarPostingsInView(ov indexdata.FieldIndexView, keys []keycodec.IndexLookupKey, capExtra int) ([]posting.List, uint64) {
	keyCount := len(keys)
	postsBuf := posting.GetSlice(keyCount + capExtra)
	var est uint64

	for i := 0; i < keyCount; i++ {
		ids := lookupScalarPostingRetained(ov, keys[i])
		if ids.IsEmpty() {
			continue
		}
		postsBuf = append(postsBuf, ids)
		est += ids.Cardinality()
	}

	return postsBuf, est
}

func scalarValueToLookupKeyRaw(raw any, v reflect.Value) (keycodec.IndexLookupKey, error) {
	switch v.Kind() {

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return keycodec.IndexLookupU64(keycodec.OrderedInt64Key(v.Int())), nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return keycodec.IndexLookupU64(v.Uint()), nil

	case reflect.Float32, reflect.Float64:
		return keycodec.IndexLookupU64(keycodec.OrderedFloat64Key(v.Float())), nil

	case reflect.String:
		return keycodec.IndexLookupString(v.String()), nil

	case reflect.Bool:
		if v.Bool() {
			return keycodec.IndexLookupString("1"), nil
		}
		return keycodec.IndexLookupString("0"), nil

	default:
		if vi, ok := raw.(schema.ValueIndexer); ok {
			return keycodec.IndexLookupString(vi.IndexingValue()), nil
		}
		if v.IsValid() && v.CanInterface() {
			if vi, ok := v.Interface().(schema.ValueIndexer); ok {
				return keycodec.IndexLookupString(vi.IndexingValue()), nil
			}
		}
		return keycodec.IndexLookupKey{}, fmt.Errorf("unsupported value type: %v", v.Type())
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
		return keycodec.CanonicalizeFloat64ForIndex(v.Float()), true

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
		f := keycodec.CanonicalizeFloat64ForIndex(v.Float())
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
		f := keycodec.CanonicalizeFloat64ForIndex(v.Float())
		if math.IsNaN(f) || math.IsInf(f, 0) || f != math.Trunc(f) || f < 0 || f > float64(^uint64(0)) {
			return 0, false
		}
		u := uint64(f)
		return u, float64(u) == f

	default:
		return 0, false
	}
}

func timeQueryValueToInt64Exact(v reflect.Value) (int64, bool) {
	if unix, ok := schema.QueryValueToUnixSeconds(v); ok {
		return unix, true
	}
	return numericQueryValueToInt64Exact(v)
}

func scalarValueToLookupKeyField(raw any, v reflect.Value, fm *schema.Field) (keycodec.IndexLookupKey, error) {
	if fm != nil && fm.UseVI {
		if vi, ok := raw.(schema.ValueIndexer); ok {
			return keycodec.IndexLookupString(vi.IndexingValue()), nil
		}
		if v.IsValid() && v.CanInterface() {
			if vi, ok := v.Interface().(schema.ValueIndexer); ok {
				return keycodec.IndexLookupString(vi.IndexingValue()), nil
			}
		}
		if v.Kind() == reflect.String {
			return keycodec.IndexLookupString(v.String()), nil
		}
		return keycodec.IndexLookupKey{}, fmt.Errorf("unsupported value type: %v", v.Type())
	}

	if fm == nil {
		return scalarValueToLookupKeyRaw(raw, v)
	}

	switch {
	case schema.IsNativeTimeField(fm):
		if unix, ok := timeQueryValueToInt64Exact(v); ok {
			return keycodec.IndexLookupU64(keycodec.OrderedInt64Key(unix)), nil
		}
		return keycodec.IndexLookupString(impossibleLookupKey), nil

	case signedIntFieldKind(fm.Kind):
		if i, ok := numericQueryValueToInt64Exact(v); ok {
			return keycodec.IndexLookupU64(keycodec.OrderedInt64Key(i)), nil
		}
		return keycodec.IndexLookupString(impossibleLookupKey), nil

	case unsignedIntFieldKind(fm.Kind):
		if u, ok := numericQueryValueToUint64Exact(v); ok {
			return keycodec.IndexLookupU64(u), nil
		}
		return keycodec.IndexLookupString(impossibleLookupKey), nil

	case fm.Kind == reflect.Float32 || fm.Kind == reflect.Float64:
		if f, ok := numericQueryValueToFloat64(v); ok {
			return keycodec.IndexLookupU64(keycodec.OrderedFloat64Key(f)), nil
		}
		return keycodec.IndexLookupString(impossibleLookupKey), nil

	default:
		return scalarValueToLookupKeyRaw(raw, v)
	}
}

func scalarValueToIdxField(raw any, v reflect.Value, fm *schema.Field) (string, error) {
	key, err := scalarValueToLookupKeyField(raw, v, fm)
	if err != nil {
		return "", err
	}
	if key.IsNumeric() {
		return keycodec.U64ByteString(key.U64()), nil
	}
	return key.StringKey(), nil
}

func normalizeUnixTimeRangeBound(op qir.Op, v reflect.Value) normalizedScalarBound {
	if unix, ok := schema.QueryValueToUnixSeconds(v); ok {
		return normalizedScalarBoundFromIndexKey(op, keycodec.FromU64(keycodec.OrderedInt64Key(unix)))
	}
	return normalizeSignedIntRangeBound(op, v)
}

func normalizeSignedIntRangeBound(op qir.Op, v reflect.Value) normalizedScalarBound {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return normalizedScalarBoundFromIndexKey(op, keycodec.FromU64(keycodec.OrderedInt64Key(v.Int())))

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
		return normalizedScalarBoundFromIndexKey(op, keycodec.FromU64(keycodec.OrderedInt64Key(int64(u))))

	case reflect.Float32, reflect.Float64:
		f := keycodec.CanonicalizeFloat64ForIndex(v.Float())
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
			return normalizedScalarBoundFromIndexKey(op, keycodec.FromU64(keycodec.OrderedInt64Key(int64(f))))

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
			return normalizedScalarBoundFromIndexKey(qir.OpGT, keycodec.FromU64(keycodec.OrderedInt64Key(int64(floor))))

		default:
			ceil := math.Ceil(f)
			if ceil < math.MinInt64 {
				return normalizedScalarBound{empty: true}
			}
			if ceil > math.MaxInt64 {
				return normalizedScalarBound{full: true}
			}
			return normalizedScalarBoundFromIndexKey(qir.OpLT, keycodec.FromU64(keycodec.OrderedInt64Key(int64(ceil))))
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
		return normalizedScalarBoundFromIndexKey(op, keycodec.FromU64(uint64(i)))

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return normalizedScalarBoundFromIndexKey(op, keycodec.FromU64(v.Uint()))

	case reflect.Float32, reflect.Float64:
		f := keycodec.CanonicalizeFloat64ForIndex(v.Float())
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
			return normalizedScalarBoundFromIndexKey(op, keycodec.FromU64(uint64(f)))

		case op == qir.OpEQ:
			return normalizedScalarBound{empty: true}

		case op == qir.OpGT || op == qir.OpGTE:
			floor := math.Floor(f)
			if floor > float64(^uint64(0)) {
				return normalizedScalarBound{empty: true}
			}
			return normalizedScalarBoundFromIndexKey(qir.OpGT, keycodec.FromU64(uint64(floor)))

		default:
			ceil := math.Ceil(f)
			if ceil > float64(^uint64(0)) {
				return normalizedScalarBound{full: true}
			}
			return normalizedScalarBoundFromIndexKey(qir.OpLT, keycodec.FromU64(uint64(ceil)))
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
	return normalizedScalarBoundFromIndexKey(op, keycodec.FromU64(keycodec.OrderedFloat64Key(f)))
}

func (qv *View) exprValueToNormalizedScalarBound(expr qir.Expr) (normalizedScalarBound, bool, error) {
	if expr.Value == nil {
		return normalizedScalarBound{empty: true}, false, nil
	}

	fm := qv.fieldMetaByExpr(expr)
	v := reflect.ValueOf(expr.Value)

	v, isNil := schema.UnwrapQueryValue(v)
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
		if fm != nil && fm.KeyKind == schema.FieldWriteKeysOrderedU64 {
			bound := normalizedScalarBound{empty: true}
			qv.storeNormalizedScalarBound(expr, v, bound)
			return bound, false, nil
		}

		key, err := scalarValueToIdxField(expr.Value, v, fm)
		if err != nil {
			return normalizedScalarBound{}, false, err
		}
		bound := normalizedScalarBound{op: qir.OpPREFIX, key: key}
		qv.storeNormalizedScalarBound(expr, v, bound)

		return bound, false, nil
	}

	if fm != nil && !fm.UseVI {
		switch {

		case schema.IsNativeTimeField(fm):
			bound := normalizeUnixTimeRangeBound(expr.Op, v)
			qv.storeNormalizedScalarBound(expr, v, bound)
			return bound, false, nil

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

	bound := normalizedScalarBound{op: expr.Op, key: key}
	qv.storeNormalizedScalarBound(expr, v, bound)
	return bound, false, nil
}

func applyNormalizedScalarBound(rb *indexdata.Bounds, b normalizedScalarBound) {
	if rb == nil {
		return
	}
	rb.Has = true
	if b.empty {
		rb.SetEmpty()
		return
	}
	if b.full {
		return
	}

	switch b.op {
	case qir.OpGT:
		if b.hasIndexKey {
			rb.ApplyLoIndex(b.keyIndex, false)
		} else {
			rb.ApplyLo(b.key, false)
		}
	case qir.OpGTE:
		if b.hasIndexKey {
			rb.ApplyLoIndex(b.keyIndex, true)
		} else {
			rb.ApplyLo(b.key, true)
		}
	case qir.OpLT:
		if b.hasIndexKey {
			rb.ApplyHiIndex(b.keyIndex, false)
		} else {
			rb.ApplyHi(b.key, false)
		}
	case qir.OpLTE:
		if b.hasIndexKey {
			rb.ApplyHiIndex(b.keyIndex, true)
		} else {
			rb.ApplyHi(b.key, true)
		}
	case qir.OpEQ:
		if b.hasIndexKey {
			rb.ApplyLoIndex(b.keyIndex, true)
			rb.ApplyHiIndex(b.keyIndex, true)
		} else {
			rb.ApplyLo(b.key, true)
			rb.ApplyHi(b.key, true)
		}
	case qir.OpPREFIX:
		rb.ApplyPrefix(b.key)
	}
}

func compareLookupKey(a, b keycodec.IndexLookupKey) int {
	return keycodec.Compare(a.IndexKey(), b.IndexKey())
}

func dedupLookupKeyBufInPlace(buf []keycodec.IndexLookupKey) []keycodec.IndexLookupKey {
	if len(buf) < 2 {
		return buf
	}
	slices.SortFunc(buf, compareLookupKey)
	write := 1
	for read := 1; read < len(buf); read++ {
		if compareLookupKey(buf[read], buf[write-1]) == 0 {
			continue
		}
		if write != read {
			buf[write] = buf[read]
		}
		write++
	}
	return buf[:write]
}

func sliceValueToLookupKeyBuf(v reflect.Value, fm *schema.Field) ([]keycodec.IndexLookupKey, bool, error) {
	if v.Len() == 0 {
		return nil, false, nil
	}

	ixsBuf := keycodec.GetIndexLookupKeySlice(v.Len())
	hasNil := false

	for i := 0; i < v.Len(); i++ {
		elem := v.Index(i)
		raw := any(nil)
		if elem.IsValid() && elem.CanInterface() {
			raw = elem.Interface()
		}

		elem, elemNil := schema.UnwrapQueryValue(elem)
		if elemNil {
			hasNil = true
			continue
		}
		if elem.Kind() == reflect.Slice {
			keycodec.ReleaseIndexLookupKeySlice(ixsBuf)
			return nil, false, fmt.Errorf("unsupported slice element type: %v", elem.Type())
		}

		key, err := scalarValueToLookupKeyField(raw, elem, fm)
		if err != nil {
			keycodec.ReleaseIndexLookupKeySlice(ixsBuf)
			return nil, false, err
		}
		ixsBuf = append(ixsBuf, key)
	}

	if len(ixsBuf) == 0 {
		keycodec.ReleaseIndexLookupKeySlice(ixsBuf)
		return nil, hasNil, nil
	}
	return ixsBuf, hasNil, nil
}

func (qv *View) scalarLookupPostings(field string, fieldOrdinal int, keys []keycodec.IndexLookupKey, includeNil bool) ([]posting.List, uint64) {
	ov := qv.fieldIndexViewFromSlotsRef(qv.snap.Index, field, fieldOrdinal)
	postsBuf, est := lookupScalarPostingsInView(ov, keys, btoi(includeNil))

	if includeNil {
		ids := qv.fieldIndexViewFromSlotsRef(qv.snap.NilIndex, field, fieldOrdinal).LookupPostingRetained(indexdata.NilIndexEntryKey)
		if !ids.IsEmpty() {
			postsBuf = append(postsBuf, ids)
			est += ids.Cardinality()
		}
	}

	return postsBuf, est
}

func (qv *View) exprValueToIdxScalar(expr qir.Expr) (string, bool, bool, error) {
	key, isSlice, isNil, err := qv.exprValueToLookupKey(expr)
	if err != nil || isSlice || isNil {
		return "", isSlice, isNil, err
	}
	if key.IsNumeric() {
		return keycodec.U64ByteString(key.U64()), false, false, nil
	}
	return key.StringKey(), false, false, nil
}

func (qv *View) exprValueToLookupKey(expr qir.Expr) (keycodec.IndexLookupKey, bool, bool, error) {
	if expr.Value == nil {
		return keycodec.IndexLookupKey{}, false, true, nil
	}

	fm := qv.fieldMetaByExpr(expr)
	v := reflect.ValueOf(expr.Value)
	v, isNil := schema.UnwrapQueryValue(v)
	if isNil {
		return keycodec.IndexLookupKey{}, false, true, nil
	}
	if queryValueIsCollectionForField(v, fm) {
		return keycodec.IndexLookupKey{}, true, false, nil
	}

	key, err := scalarValueToLookupKeyField(expr.Value, v, fm)
	if err != nil {
		return keycodec.IndexLookupKey{}, false, false, err
	}
	return key, false, false, nil
}

func (qv *View) diffPostingResult(acc, sub postingResult) (postingResult, error) {
	acc.ids = acc.ids.BuildAndNot(sub.ids)
	sub.ids.Release()
	return acc, nil
}

// exprValueToDistinctLookupKeyBuf returns caller-owned indexed values deduplicated
// for set-like operators whose semantics do not depend on input order.
func (qv *View) exprValueToDistinctLookupKeyBuf(expr qir.Expr) ([]keycodec.IndexLookupKey, bool, bool, error) {
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
			valsBuf := keycodec.GetIndexLookupKeySlice(len(v))
			for i := range v {
				valsBuf = append(valsBuf, keycodec.IndexLookupString(v[i]))
			}
			valsBuf = dedupLookupKeyBufInPlace(valsBuf)
			if len(valsBuf) == 0 {
				keycodec.ReleaseIndexLookupKeySlice(valsBuf)
				return nil, true, false, nil
			}
			return valsBuf, true, false, nil
		}
	}

	v := reflect.ValueOf(expr.Value)
	v, isNil := schema.UnwrapQueryValue(v)
	if isNil {
		if expr.Op == qir.OpIN {
			return nil, true, false, nil
		}
		return nil, false, true, nil
	}

	if queryValueIsCollectionForField(v, fm) {
		valsBuf, hasNil, err := sliceValueToLookupKeyBuf(v, fm)
		if err != nil {
			return nil, true, false, err
		}
		if valsBuf == nil {
			return nil, true, hasNil, nil
		}
		valsBuf = dedupLookupKeyBufInPlace(valsBuf)
		if len(valsBuf) == 0 {
			keycodec.ReleaseIndexLookupKeySlice(valsBuf)
			return nil, true, hasNil, nil
		}
		return valsBuf, true, hasNil, nil
	}

	return nil, false, false, nil
}

func diffOwned(a, b posting.List) posting.List {
	res := a.Clone()
	if !b.IsEmpty() {
		res = res.BuildAndNot(b)
	}
	return res
}

func (qv *View) andPostingResult(a, b postingResult) (postingResult, error) {
	// handle empty sets
	if !a.neg && a.ids.IsEmpty() {
		a.ids.Release()
		b.ids.Release()
		return postingResult{}, nil
	}
	if !b.neg && b.ids.IsEmpty() {
		a.ids.Release()
		b.ids.Release()
		return postingResult{}, nil
	}

	switch {

	case !a.neg && !b.neg:
		if a.ids.Cardinality() <= b.ids.Cardinality() {
			a.ids = a.ids.BuildAnd(b.ids)
			b.ids.Release()
			return a, nil
		}
		b.ids = b.ids.BuildAnd(a.ids)
		a.ids.Release()
		return b, nil

	case a.neg && b.neg:
		if a.ids.Cardinality() >= b.ids.Cardinality() {
			a.ids = a.ids.BuildOr(b.ids)
			b.ids.Release()
			a.neg = true
			return a, nil
		}
		b.ids = b.ids.BuildOr(a.ids)
		a.ids.Release()
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

func (qv *View) orPostingResult(a, b postingResult) postingResult {
	// universe (negative empty)
	if a.neg && a.ids.IsEmpty() {
		a.ids.Release()
		b.ids.Release()
		return postingResult{neg: true}
	}
	if b.neg && b.ids.IsEmpty() {
		a.ids.Release()
		b.ids.Release()
		return postingResult{neg: true}
	}

	// standard empty
	if !a.neg && a.ids.IsEmpty() {
		a.ids.Release()
		return b
	}
	if !b.neg && b.ids.IsEmpty() {
		b.ids.Release()
		return a
	}

	switch {

	case !a.neg && !b.neg:
		if a.ids.Cardinality() >= b.ids.Cardinality() {
			a.ids = a.ids.BuildOr(b.ids)
			b.ids.Release()
			return a
		}
		b.ids = b.ids.BuildOr(a.ids)
		a.ids.Release()
		return b

	case a.neg && b.neg:
		if a.ids.Cardinality() <= b.ids.Cardinality() {
			a.ids = a.ids.BuildAnd(b.ids)
			b.ids.Release()
			a.neg = true
			return a
		}
		b.ids = b.ids.BuildAnd(a.ids)
		a.ids.Release()
		b.neg = true
		return b

	case a.neg && !b.neg:
		// NOT A OR B  ->  NOT (A \ B)
		payload := diffOwned(a.ids, b.ids)
		a.ids.Release()
		b.ids.Release()
		return postingResult{ids: payload, neg: true}

	case !a.neg && b.neg:
		// A OR NOT B  ->  NOT (B \ A)
		payload := diffOwned(b.ids, a.ids)
		a.ids.Release()
		b.ids.Release()
		return postingResult{ids: payload, neg: true}
	}

	return postingResult{}
}
