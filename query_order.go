package rbi

import (
	"reflect"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qir"
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

func emitArrayCountZeroBucket(cursor *queryCursor, resultBM posting.List, nonEmpty posting.List) bool {
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
	if a.Cardinality() > b.Cardinality() {
		a, b = b, a
	}
	return a.Clone().BuildAnd(b)
}

func makeOutSlice(cardinality, limit uint64) []uint64 {
	var out []uint64
	if limit > 0 {
		out = make([]uint64, 0, min(limit, cardinality))
	} else {
		out = make([]uint64, 0, cardinality)
	}
	return out
}

func shouldMaterializeOrderedAllNumericBuckets(
	qv *queryView,
	skip uint64,
	all bool,
	resultCard uint64,
) bool {
	return !qv.strKey && all && skip == 0 && resultCard >= 64_000
}

func appendMaterializedNumericPostingKeys(out []uint64, ids posting.List) []uint64 {
	if ids.IsEmpty() {
		return out
	}
	if idx, ok := ids.TrySingle(); ok {
		return append(out, idx)
	}
	card := int(ids.Cardinality())
	if card == 0 {
		return out
	}
	base := len(out)
	out = out[:base+card]
	it := ids.Iter()
	for i := 0; i < card; i++ {
		out[base+i] = it.Next()
	}
	it.Release()
	return out
}

func appendMaterializedNumericPostingResultKeys(
	out []uint64,
	ids posting.List,
	result postingResult,
) []uint64 {
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

func emitPostingResultBucketToCursor(
	qv *queryView,
	cursor *queryCursor,
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

func (qv *queryView) postingResultCardinality(result postingResult) uint64 {
	if !result.neg {
		return result.ids.Cardinality()
	}
	return qv.snapshotUniverseCardinality() - result.ids.Cardinality()
}

func (qv *queryView) forEachPostingResultBucket(ids posting.List, result postingResult, fn func(uint64) bool) bool {
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

func (qv *queryView) forEachPostingResultAll(result postingResult, fn func(uint64) bool) bool {
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

func (qv *queryView) emitArrayCountZeroBucketResult(cursor *queryCursor, result postingResult, nonEmpty posting.List) bool {
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

func (qv *queryView) queryOrderBasic(result postingResult, ov indexdata.FieldOverlay, o qir.Order, skip, need uint64, all bool) ([]uint64, error) {
	resultCard := qv.postingResultCardinality(result)
	if resultCard == 0 {
		return nil, nil
	}

	isSliceOrderField := false
	if fm := qv.fieldMetaByOrder(o); fm != nil && fm.Slice {
		isSliceOrderField = true
	}

	var tmp posting.List

	out := makeOutSlice(resultCard, need)
	if !isSliceOrderField && shouldMaterializeOrderedAllNumericBuckets(qv, skip, all, resultCard) {
		br := ov.RangeForBounds((indexdata.Bounds{Has: true}))
		cur := ov.NewCursor(br, o.Desc)
		for {
			_, ids, ok := cur.Next()
			if !ok {
				break
			}
			out = appendMaterializedNumericPostingResultKeys(out, ids, result)
		}
		if fm := qv.fieldMetaByOrder(o); fm != nil && fm.Ptr {
			out = appendMaterializedNumericPostingResultKeys(
				out,
				qv.nilFieldOverlayForOrder(o).LookupPostingRetained(nilIndexEntryKey),
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

	br := ov.RangeForBounds((indexdata.Bounds{Has: true}))
	cur := ov.NewCursor(br, o.Desc)
	for {
		_, ids, ok := cur.Next()
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

	if fm := qv.fieldMetaByOrder(o); fm != nil && fm.Ptr {
		nilIDs := qv.nilFieldOverlayForOrder(o).LookupPostingRetained(nilIndexEntryKey)
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

func scalarArrayPosPriorityCoversAllKeysOverlay(ov indexdata.FieldOverlay, vals []string) bool {
	if !ov.HasData() {
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
	if len(set) < ov.KeyCount() {
		return false
	}
	cur := ov.NewCursor(ov.RangeByRanks(0, ov.KeyCount()), false)
	for {
		key, _, ok := cur.Next()
		if !ok {
			return true
		}
		if _, ok = set[key.UnsafeString()]; !ok {
			return false
		}
	}
}

func scalarArrayPosPriorityCoversAllResultsOverlay(resultBM posting.List, ov, nilOV indexdata.FieldOverlay, vals []string) bool {
	if !scalarArrayPosPriorityCoversAllKeysOverlay(ov, vals) {
		return false
	}
	if !nilOV.HasData() {
		return true
	}
	return !nilOV.LookupPostingRetained(nilIndexEntryKey).Intersects(resultBM)
}

func (qv *queryView) queryOrderArrayPosOverlay(result postingResult, ov indexdata.FieldOverlay, o qir.Order, skip, need uint64, all bool) ([]uint64, error) {
	resultCard := qv.postingResultCardinality(result)
	if resultCard == 0 {
		return nil, nil
	}

	vals, err := qv.orderDataValues(o.Data, qv.fieldMetaByOrder(o))
	if err != nil {
		return nil, err
	}
	if fm := qv.fieldMetaByOrder(o); fm != nil && !fm.Slice {
		return qv.queryOrderArrayPosScalarOverlay(result, qv.engine.fieldNameByOrdinal(o.FieldOrdinal), o.FieldOrdinal, ov, vals, o.Desc, skip, need, all)
	}

	out := makeOutSlice(resultCard, need)
	var tmp posting.List
	cursor := qv.newQueryCursor(out, skip, need, all, queryCursorDedupeCap(resultCard, skip, need, all))
	defer cursor.release()

	if !o.Desc {
		for _, v := range vals {
			var done bool
			tmp, done = emitPostingResultBucketToCursor(qv, &cursor, tmp, ov.LookupPostingRetained(v), result)
			if done {
				tmp.Release()
				return cursor.out, nil
			}
		}
	} else {
		for i := len(vals) - 1; i >= 0; i-- {
			var done bool
			tmp, done = emitPostingResultBucketToCursor(qv, &cursor, tmp, ov.LookupPostingRetained(vals[i]), result)
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

func (qv *queryView) queryOrderArrayPosScalarOverlay(result postingResult, field string, fieldOrdinal int, ov indexdata.FieldOverlay, vals []string, desc bool, skip, need uint64, all bool) ([]uint64, error) {
	resultCard := qv.postingResultCardinality(result)
	if resultCard == 0 {
		return nil, nil
	}
	nilOV := indexdata.FieldOverlay{}
	if fm := qv.fieldMeta(field, fieldOrdinal); fm != nil && fm.Ptr {
		nilOV = qv.nilFieldOverlayRef(field, fieldOrdinal)
	}
	orderedVals := orderedDistinctStrings(vals, desc)
	coversAll := !result.neg && len(orderedVals) > 0 && scalarArrayPosPriorityCoversAllResultsOverlay(result.ids, ov, nilOV, orderedVals)
	// Empty priorities still require a full fallback pass; they just do not need
	// duplicate tracking against priority buckets because none were emitted.
	needFallback := result.neg || !coversAll
	needSeen := needFallback && len(orderedVals) > 0

	out := makeOutSlice(resultCard, need)
	if !needFallback &&
		!result.neg &&
		shouldMaterializeOrderedAllNumericBuckets(qv, skip, all, resultCard) {
		for _, v := range orderedVals {
			ids := ov.LookupPostingRetained(v).Borrow().BuildAnd(result.ids)
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
		tmp, done = emitPostingResultBucketToCursor(qv, &cursor, tmp, ov.LookupPostingRetained(v), result)
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

func (qv *queryView) orderDataValues(v any, fm *field) ([]string, error) {
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
	collection := queryValueIsCollectionForField(rv, fm)
	if !collection && rv.Kind() == reflect.Slice {
		collection = !rv.Type().Implements(viType)
	}
	if collection {
		valsBuf, _, err := sliceValueToIdxStringBuf(rv, fm)
		if err != nil {
			return nil, err
		}
		if valsBuf == nil {
			return nil, nil
		}
		defer pooled.PutStringSlice(valsBuf)

		out := make([]string, len(valsBuf))
		copy(out, valsBuf)
		return out, nil
	}

	key, err := scalarValueToIdxField(v, rv, fm)
	if err != nil {
		return nil, err
	}
	return []string{key}, nil
}

func (qv *queryView) queryOrderArrayCount(result postingResult, ov indexdata.FieldOverlay, o qir.Order, skip, need uint64, all bool, useZeroComplement bool) ([]uint64, error) {
	resultCard := qv.postingResultCardinality(result)
	if resultCard == 0 {
		return nil, nil
	}

	out := makeOutSlice(resultCard, need)

	var tmp posting.List

	cursor := qv.newQueryCursor(out, skip, need, all, 0)
	defer cursor.release()
	var nonEmpty posting.List
	if useZeroComplement {
		nonEmpty = ov.LookupPostingRetained(indexdata.LenIndexNonEmptyKey)
	}
	if !result.neg && shouldMaterializeOrderedAllNumericBuckets(qv, skip, all, resultCard) {
		br := ov.RangeByRanks(0, ov.KeyCount())
		if !o.Desc {
			if useZeroComplement {
				ids := result.ids.Borrow().BuildAndNot(nonEmpty)
				out = appendMaterializedNumericPostingKeys(out, ids)
				ids.Release()
			}
			cur := ov.NewCursor(br, false)
			for {
				key, ids, ok := cur.Next()
				if !ok {
					break
				}
				if keycodec.EqualsString(key, indexdata.LenIndexNonEmptyKey) || ids.IsEmpty() {
					continue
				}
				out = appendMaterializedNumericPostingResultKeys(out, ids, result)
			}
			return out, nil
		}
		cur := ov.NewCursor(br, true)
		for {
			key, ids, ok := cur.Next()
			if !ok {
				break
			}
			if keycodec.EqualsString(key, indexdata.LenIndexNonEmptyKey) || ids.IsEmpty() {
				continue
			}
			out = appendMaterializedNumericPostingResultKeys(out, ids, result)
		}
		if useZeroComplement {
			ids := result.ids.Borrow().BuildAndNot(nonEmpty)
			out = appendMaterializedNumericPostingKeys(out, ids)
			ids.Release()
		}
		return out, nil
	}

	br := ov.RangeByRanks(0, ov.KeyCount())
	if !o.Desc {
		if useZeroComplement && qv.emitArrayCountZeroBucketResult(&cursor, result, nonEmpty) {
			tmp.Release()
			return cursor.out, nil
		}
		cur := ov.NewCursor(br, false)
		for {
			key, ids, ok := cur.Next()
			if !ok {
				break
			}
			if keycodec.EqualsString(key, indexdata.LenIndexNonEmptyKey) {
				continue
			}
			if ids.IsEmpty() {
				continue
			}

			var done bool
			tmp, done = emitPostingResultBucketToCursor(qv, &cursor, tmp, ids, result)
			if done {
				tmp.Release()
				return cursor.out, nil
			}
		}
	} else {
		cur := ov.NewCursor(br, true)
		for {
			key, ids, ok := cur.Next()
			if !ok {
				break
			}
			if keycodec.EqualsString(key, indexdata.LenIndexNonEmptyKey) {
				continue
			}
			if ids.IsEmpty() {
				continue
			}

			var done bool
			tmp, done = emitPostingResultBucketToCursor(qv, &cursor, tmp, ids, result)
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
