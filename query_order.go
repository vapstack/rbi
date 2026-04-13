package rbi

import (
	"reflect"
	"unsafe"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/posting"
)

func shouldUseOnePassIntersectionPosting(a posting.List, b posting.List, need uint64, all bool) bool {
	if b.IsEmpty() || a.IsEmpty() {
		return false
	}
	if !all && need > 0 && need <= 1024 {
		return true
	}
	minCard := a.Cardinality()
	if bc := b.Cardinality(); bc < minCard {
		minCard = bc
	}
	return minCard <= iteratorThreshold*4
}

func emitArrayCountZeroBucket[K ~uint64 | ~string, V any](cursor *queryCursor[K, V], resultBM posting.List, nonEmpty posting.List) bool {
	if resultBM.IsEmpty() {
		return false
	}

	card := resultBM.Cardinality()
	if nonEmpty.IsEmpty() {
		if cursor.skip >= card {
			cursor.skip -= card
			return false
		}
		it := resultBM.Iter()
		defer it.Release()
		for it.HasNext() {
			if cursor.emit(it.Next()) {
				return true
			}
		}
		return false
	}

	if cursor.skip > 0 {
		nonEmptyCount := nonEmpty.AndCardinality(resultBM)
		if nonEmptyCount >= card {
			return false
		}
		zeroCount := card - nonEmptyCount
		if cursor.skip >= zeroCount {
			cursor.skip -= zeroCount
			return false
		}
		if nonEmptyCount == 0 {
			it := resultBM.Iter()
			defer it.Release()
			for it.HasNext() {
				if cursor.emit(it.Next()) {
					return true
				}
			}
			return false
		}
	}

	resultIt := resultBM.Iter()
	defer resultIt.Release()
	nonEmptyIt := nonEmpty.Iter()
	defer nonEmptyIt.Release()

	var exclude uint64
	hasExclude := false
	for resultIt.HasNext() {
		idx := resultIt.Next()
		for {
			if !hasExclude {
				if !nonEmptyIt.HasNext() {
					if cursor.emit(idx) {
						return true
					}
					for resultIt.HasNext() {
						if cursor.emit(resultIt.Next()) {
							return true
						}
					}
					return false
				}
				exclude = nonEmptyIt.Next()
				hasExclude = true
			}
			if exclude < idx {
				hasExclude = false
				continue
			}
			break
		}
		if exclude == idx {
			hasExclude = false
			continue
		}
		if cursor.emit(idx) {
			return true
		}
	}
	return false
}

func tmpIntersectPosting(tmp posting.List, a posting.List, b posting.List) posting.List {
	tmp.Release()
	tmp = posting.List{}
	if a.IsEmpty() || b.IsEmpty() {
		return tmp
	}
	upper := min(a.Cardinality(), b.Cardinality())
	builder := newPostingUnionBuilder(upper > posting.SmallCap && upper <= singleAdaptiveMaxLen)
	a.ForEachIntersecting(b, func(idx uint64) bool {
		builder.addSingle(idx)
		return false
	})
	return builder.finish(false)
}

func makeOutSlice[K ~int64 | ~uint64 | ~string](cardinality, limit uint64) []K {
	var out []K
	if limit > 0 {
		out = make([]K, 0, min(limit, cardinality))
	} else {
		out = make([]K, 0, cardinality)
	}
	return out
}

func shouldMaterializeOrderedAllNumericBuckets[K ~uint64 | ~string, V any](
	qv *queryView[K, V],
	skip uint64,
	all bool,
	resultCard uint64,
) bool {
	return !qv.strkey && all && skip == 0 && resultCard >= 64_000
}

func appendMaterializedNumericPostingKeys[K ~uint64 | ~string](out []K, ids posting.List) []K {
	if ids.IsEmpty() {
		return out
	}
	if idx, ok := ids.TrySingle(); ok {
		return append(out, *(*K)(unsafe.Pointer(&idx)))
	}
	card := int(ids.Cardinality())
	if card == 0 {
		return out
	}
	base := len(out)
	out = out[:base+card]
	dst := unsafe.Slice((*uint64)(unsafe.Pointer(&out[base])), card)
	it := ids.Iter()
	for i := 0; i < card; i++ {
		dst[i] = it.Next()
	}
	it.Release()
	return out
}

func appendMaterializedNumericPostingResultKeys[K ~uint64 | ~string](
	out []K,
	ids posting.List,
	result postingResult,
) []K {
	if ids.IsEmpty() {
		return out
	}
	if result.neg {
		matched := ids.Borrow().BuildAndNot(result.ids)
		out = appendMaterializedNumericPostingKeys(out, matched)
		matched.Release()
		return out
	}
	matched := ids.Borrow().BuildAnd(result.ids)
	out = appendMaterializedNumericPostingKeys(out, matched)
	matched.Release()
	return out
}

func emitPostingResultBucketToCursor[K ~uint64 | ~string, V any](
	qv *queryView[K, V],
	cursor *queryCursor[K, V],
	tmp posting.List,
	ids posting.List,
	result postingResult,
) (posting.List, bool) {
	if ids.IsEmpty() {
		return tmp, false
	}
	if idx, ok := ids.TrySingle(); ok {
		if result.neg {
			if !result.ids.Contains(idx) {
				return tmp, cursor.emit(idx)
			}
			return tmp, false
		}
		if result.ids.Contains(idx) {
			return tmp, cursor.emit(idx)
		}
		return tmp, false
	}
	if shouldUseOnePassPostingResultFilter(ids, result, cursor.need, cursor.all) {
		return tmp, qv.forEachPostingResultBucket(ids, result, cursor.emit)
	}
	tmp = tmpIntersectPosting(tmp, ids, result.ids)
	return tmp, cursor.emitPosting(tmp)
}

func shouldUseOnePassPostingResultFilter(ids posting.List, result postingResult, need uint64, all bool) bool {
	if result.neg {
		return true
	}
	return shouldUseOnePassIntersectionPosting(ids, result.ids, need, all)
}

func (qv *queryView[K, V]) postingResultCardinality(result postingResult) uint64 {
	if !result.neg {
		return result.ids.Cardinality()
	}
	return qv.snapshotUniverseCardinality() - result.ids.Cardinality()
}

func (qv *queryView[K, V]) forEachPostingResultBucket(ids posting.List, result postingResult, fn func(uint64) bool) bool {
	if ids.IsEmpty() {
		return false
	}
	if !result.neg {
		return ids.ForEachIntersecting(result.ids, fn)
	}
	exclude := result.ids
	it := ids.Iter()
	defer it.Release()
	for it.HasNext() {
		idx := it.Next()
		if !exclude.IsEmpty() && exclude.Contains(idx) {
			continue
		}
		if fn(idx) {
			return true
		}
	}
	return false
}

func (qv *queryView[K, V]) forEachPostingResultAll(result postingResult, fn func(uint64) bool) bool {
	if !result.neg {
		it := result.ids.Iter()
		defer it.Release()
		for it.HasNext() {
			if fn(it.Next()) {
				return true
			}
		}
		return false
	}
	exclude := result.ids
	it := qv.snapshotUniverseView().Iter()
	defer it.Release()
	for it.HasNext() {
		idx := it.Next()
		if !exclude.IsEmpty() && exclude.Contains(idx) {
			continue
		}
		if fn(idx) {
			return true
		}
	}
	return false
}

func (qv *queryView[K, V]) emitArrayCountZeroBucketResult(cursor *queryCursor[K, V], result postingResult, nonEmpty posting.List) bool {
	if !result.neg {
		return emitArrayCountZeroBucket(cursor, result.ids, nonEmpty)
	}

	universeCard := qv.snapshotUniverseCardinality()
	if universeCard == 0 {
		return false
	}

	zeroCount := universeCard
	if !nonEmpty.IsEmpty() {
		zeroCount -= nonEmpty.Cardinality()
	}
	if zeroCount == 0 {
		return false
	}

	excludedZero := result.ids.Cardinality()
	if !nonEmpty.IsEmpty() {
		excludedZero -= result.ids.AndCardinality(nonEmpty)
	}
	if excludedZero >= zeroCount {
		return false
	}

	zeroCount -= excludedZero
	if cursor.skip >= zeroCount {
		cursor.skip -= zeroCount
		return false
	}

	exclude := result.ids
	it := qv.snapshotUniverseView().Iter()
	defer it.Release()
	for it.HasNext() {
		idx := it.Next()
		if !exclude.IsEmpty() && exclude.Contains(idx) {
			continue
		}
		if !nonEmpty.IsEmpty() && nonEmpty.Contains(idx) {
			continue
		}
		if cursor.emit(idx) {
			return true
		}
	}
	return false
}

func (qv *queryView[K, V]) queryOrderBasic(result postingResult, ov fieldOverlay, o qx.Order, skip, need uint64, all bool) ([]K, error) {
	resultCard := qv.postingResultCardinality(result)
	if resultCard == 0 {
		return nil, nil
	}

	isSliceOrderField := false
	if fm := qv.fields[o.Field]; fm != nil && fm.Slice {
		isSliceOrderField = true
	}

	var tmp posting.List

	out := makeOutSlice[K](resultCard, need)
	if !isSliceOrderField && shouldMaterializeOrderedAllNumericBuckets(qv, skip, all, resultCard) {
		br := ov.rangeForBounds(rangeBounds{has: true})
		cur := ov.newCursor(br, o.Desc)
		for {
			_, ids, ok := cur.next()
			if !ok {
				break
			}
			out = appendMaterializedNumericPostingResultKeys(out, ids, result)
		}
		if fm := qv.fields[o.Field]; fm != nil && fm.Ptr {
			out = appendMaterializedNumericPostingResultKeys(
				out,
				qv.nilFieldOverlay(o.Field).lookupPostingRetained(nilIndexEntryKey),
				result,
			)
		}
		return out, nil
	}
	dedupeCap := uint64(0)
	if isSliceOrderField {
		dedupeCap = queryCursorDedupeCap(resultCard, skip, need, all)
	}
	cursor := qv.newQueryCursor(out, skip, need, all, dedupeCap)
	defer cursor.release()

	br := ov.rangeForBounds(rangeBounds{has: true})
	cur := ov.newCursor(br, o.Desc)
	for {
		_, ids, ok := cur.next()
		if !ok {
			break
		}
		var done bool
		tmp, done = emitPostingResultBucketToCursor(qv, &cursor, tmp, ids, result)
		if done {
			tmp.Release()
			return cursor.out, nil
		}
	}

	if fm := qv.fields[o.Field]; fm != nil && fm.Ptr {
		nilIDs := qv.nilFieldOverlay(o.Field).lookupPostingRetained(nilIndexEntryKey)
		if !nilIDs.IsEmpty() {
			var done bool
			tmp, done = emitPostingResultBucketToCursor(qv, &cursor, tmp, nilIDs, result)
			if done {
				tmp.Release()
				return cursor.out, nil
			}
		}
	}
	tmp.Release()
	return cursor.out, nil
}

func orderedDistinctStrings(vals []string, desc bool) []string {
	if len(vals) < 2 {
		return vals
	}

	seen := stringSetPool.Get(len(vals))
	defer stringSetPool.Put(seen)

	if desc {
		out := make([]string, 0, len(vals))
		for i := len(vals) - 1; i >= 0; i-- {
			v := vals[i]
			if _, ok := seen[v]; ok {
				continue
			}
			seen[v] = struct{}{}
			out = append(out, v)
		}
		return out
	}

	for i, v := range vals {
		if _, ok := seen[v]; ok {
			out := make([]string, 0, len(vals))
			out = append(out, vals[:i]...)
			for j := i + 1; j < len(vals); j++ {
				v = vals[j]
				if _, ok := seen[v]; ok {
					continue
				}
				seen[v] = struct{}{}
				out = append(out, v)
			}
			return out
		}
		seen[v] = struct{}{}
	}
	return vals
}

func scalarArrayPosPriorityCoversAllKeysOverlay(ov fieldOverlay, vals []string) bool {
	if !ov.hasData() {
		return true
	}
	if len(vals) == 0 {
		return false
	}
	set := stringSetPool.Get(len(vals))
	defer stringSetPool.Put(set)
	for _, v := range vals {
		set[v] = struct{}{}
	}
	if len(set) < ov.keyCount() {
		return false
	}
	cur := ov.newCursor(ov.rangeByRanks(0, ov.keyCount()), false)
	for {
		key, _, ok := cur.next()
		if !ok {
			return true
		}
		if _, ok = set[key.asUnsafeString()]; !ok {
			return false
		}
	}
}

func scalarArrayPosPriorityCoversAllResultsOverlay(resultBM posting.List, ov, nilOV fieldOverlay, vals []string) bool {
	if !scalarArrayPosPriorityCoversAllKeysOverlay(ov, vals) {
		return false
	}
	if !nilOV.hasData() {
		return true
	}
	return !nilOV.lookupPostingRetained(nilIndexEntryKey).Intersects(resultBM)
}

func (qv *queryView[K, V]) queryOrderArrayPosOverlay(result postingResult, ov fieldOverlay, o qx.Order, skip, need uint64, all bool) ([]K, error) {
	resultCard := qv.postingResultCardinality(result)
	if resultCard == 0 {
		return nil, nil
	}

	vals, err := qv.orderDataValues(o.Data)
	if err != nil {
		return nil, err
	}
	if fm := qv.fields[o.Field]; fm != nil && !fm.Slice {
		return qv.queryOrderArrayPosScalarOverlay(result, o.Field, ov, vals, o.Desc, skip, need, all)
	}

	out := makeOutSlice[K](resultCard, need)
	var tmp posting.List
	cursor := qv.newQueryCursor(out, skip, need, all, queryCursorDedupeCap(resultCard, skip, need, all))
	defer cursor.release()

	if !o.Desc {
		for _, v := range vals {
			var done bool
			tmp, done = emitPostingResultBucketToCursor(qv, &cursor, tmp, ov.lookupPostingRetained(v), result)
			if done {
				tmp.Release()
				return cursor.out, nil
			}
		}
	} else {
		for i := len(vals) - 1; i >= 0; i-- {
			var done bool
			tmp, done = emitPostingResultBucketToCursor(qv, &cursor, tmp, ov.lookupPostingRetained(vals[i]), result)
			if done {
				tmp.Release()
				return cursor.out, nil
			}
		}
	}

	qv.forEachPostingResultAll(result, cursor.emit)

	tmp.Release()
	return cursor.out, nil
}

func (qv *queryView[K, V]) queryOrderArrayPosScalarOverlay(result postingResult, field string, ov fieldOverlay, vals []string, desc bool, skip, need uint64, all bool) ([]K, error) {
	resultCard := qv.postingResultCardinality(result)
	if resultCard == 0 {
		return nil, nil
	}
	nilOV := fieldOverlay{}
	if fm := qv.fields[field]; fm != nil && fm.Ptr {
		nilOV = qv.nilFieldOverlay(field)
	}
	orderedVals := orderedDistinctStrings(vals, desc)
	coversAll := !result.neg && len(orderedVals) > 0 && scalarArrayPosPriorityCoversAllResultsOverlay(result.ids, ov, nilOV, orderedVals)
	// Empty priorities still require a full fallback pass; they just do not need
	// duplicate tracking against priority buckets because none were emitted.
	needFallback := result.neg || !coversAll
	needSeen := needFallback && len(orderedVals) > 0

	out := makeOutSlice[K](resultCard, need)
	if !needFallback &&
		!result.neg &&
		shouldMaterializeOrderedAllNumericBuckets(qv, skip, all, resultCard) {
		for _, v := range orderedVals {
			ids := ov.lookupPostingRetained(v).Borrow().BuildAnd(result.ids)
			out = appendMaterializedNumericPostingKeys(out, ids)
			ids.Release()
		}
		return out, nil
	}
	dedupeCap := uint64(0)
	if needSeen {
		dedupeCap = queryCursorDedupeCap(resultCard, skip, need, all)
	}
	cursor := qv.newQueryCursor(out, skip, need, all, dedupeCap)
	defer cursor.release()

	var tmp posting.List

	for _, v := range orderedVals {
		var done bool
		tmp, done = emitPostingResultBucketToCursor(qv, &cursor, tmp, ov.lookupPostingRetained(v), result)
		if done {
			tmp.Release()
			return cursor.out, nil
		}
	}

	if !cursor.all && cursor.need == 0 {
		tmp.Release()
		return cursor.out, nil
	}
	if !needFallback {
		tmp.Release()
		return cursor.out, nil
	}
	qv.forEachPostingResultAll(result, cursor.emit)

	tmp.Release()
	return cursor.out, nil
}

func (qv *queryView[K, V]) orderDataValues(v any) ([]string, error) {
	if vals, ok := v.([]string); ok {
		return vals, nil
	}

	if s, ok := v.(string); ok {
		return []string{s}, nil
	}

	rv := reflect.ValueOf(v)
	rv, isNil := unwrapExprValue(rv)
	if isNil {
		return nil, nil
	}
	if queryValueIsCollectionForField(rv, nil) {
		valsBuf, _, err := sliceValueToIdxStringBuf(rv, nil)
		if err != nil {
			return nil, err
		}
		if valsBuf == nil {
			return nil, nil
		}
		defer stringSlicePool.Put(valsBuf)

		out := make([]string, valsBuf.Len())
		for i := 0; i < valsBuf.Len(); i++ {
			out[i] = valsBuf.Get(i)
		}
		return out, nil
	}

	key, err := scalarValueToIdxField(v, rv, nil)
	if err != nil {
		return nil, err
	}
	return []string{key}, nil
}

func (qv *queryView[K, V]) queryOrderArrayCount(result postingResult, s []index, o qx.Order, skip, need uint64, all bool, useZeroComplement bool) ([]K, error) {
	resultCard := qv.postingResultCardinality(result)
	if resultCard == 0 {
		return nil, nil
	}

	out := makeOutSlice[K](resultCard, need)

	var tmp posting.List

	cursor := qv.newQueryCursor(out, skip, need, all, 0)
	defer cursor.release()
	var nonEmpty posting.List
	if useZeroComplement {
		for _, ix := range s {
			if indexKeyEqualsString(ix.Key, lenIndexNonEmptyKey) {
				nonEmpty = ix.IDs
				break
			}
		}
	}
	if !result.neg && shouldMaterializeOrderedAllNumericBuckets(qv, skip, all, resultCard) {
		appendZero := func() {
			if !useZeroComplement {
				return
			}
			ids := result.ids.Borrow().BuildAndNot(nonEmpty)
			out = appendMaterializedNumericPostingKeys(out, ids)
			ids.Release()
		}
		if !o.Desc {
			appendZero()
			for _, ix := range s {
				if indexKeyEqualsString(ix.Key, lenIndexNonEmptyKey) || ix.IDs.IsEmpty() {
					continue
				}
				ids := ix.IDs.Borrow().BuildAnd(result.ids)
				out = appendMaterializedNumericPostingKeys(out, ids)
				ids.Release()
			}
			return out, nil
		}
		for i := len(s) - 1; i >= 0; i-- {
			ix := s[i]
			if indexKeyEqualsString(ix.Key, lenIndexNonEmptyKey) || ix.IDs.IsEmpty() {
				continue
			}
			ids := ix.IDs.Borrow().BuildAnd(result.ids)
			out = appendMaterializedNumericPostingKeys(out, ids)
			ids.Release()
		}
		appendZero()
		return out, nil
	}

	if !o.Desc {
		if useZeroComplement && qv.emitArrayCountZeroBucketResult(&cursor, result, nonEmpty) {
			tmp.Release()
			return cursor.out, nil
		}

		for _, ix := range s {
			if indexKeyEqualsString(ix.Key, lenIndexNonEmptyKey) {
				continue
			}
			if ix.IDs.IsEmpty() {
				continue
			}

			var done bool
			tmp, done = emitPostingResultBucketToCursor(qv, &cursor, tmp, ix.IDs, result)
			if done {
				tmp.Release()
				return cursor.out, nil
			}
		}

	} else {

		for i := len(s) - 1; i >= 0; i-- {
			ix := s[i]
			if indexKeyEqualsString(ix.Key, lenIndexNonEmptyKey) {
				continue
			}
			if ix.IDs.IsEmpty() {
				continue
			}

			var done bool
			tmp, done = emitPostingResultBucketToCursor(qv, &cursor, tmp, ix.IDs, result)
			if done {
				tmp.Release()
				return cursor.out, nil
			}
		}
		if useZeroComplement && qv.emitArrayCountZeroBucketResult(&cursor, result, nonEmpty) {
			tmp.Release()
			return cursor.out, nil
		}
	}

	tmp.Release()
	return cursor.out, nil
}
