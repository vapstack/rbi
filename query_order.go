package rbi

import (
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
		stop := false
		resultBM.ForEach(func(idx uint64) bool {
			if cursor.emit(idx) {
				stop = true
				return false
			}
			return true
		})
		return stop
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
			stop := false
			resultBM.ForEach(func(idx uint64) bool {
				if cursor.emit(idx) {
					stop = true
					return false
				}
				return true
			})
			return stop
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

func tmpIntersectPosting(tmp *posting.List, a posting.List, b posting.List) {
	tmp.Clear()
	if a.IsEmpty() || b.IsEmpty() {
		return
	}
	a.ForEachIntersecting(b, func(idx uint64) bool {
		tmp.Add(idx)
		return false
	})
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

func postingResultContains(result postingResult, idx uint64) bool {
	if result.neg {
		return !result.ids.Contains(idx)
	}
	return result.ids.Contains(idx)
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
	stop := false
	exclude := result.ids
	ids.ForEach(func(idx uint64) bool {
		if !exclude.IsEmpty() && exclude.Contains(idx) {
			return true
		}
		if fn(idx) {
			stop = true
			return false
		}
		return true
	})
	return stop
}

func (qv *queryView[K, V]) forEachPostingResultAll(result postingResult, fn func(uint64) bool) bool {
	if !result.neg {
		stop := false
		result.ids.ForEach(func(idx uint64) bool {
			if fn(idx) {
				stop = true
				return false
			}
			return true
		})
		return stop
	}
	stop := false
	exclude := result.ids
	qv.snapshotUniverseView().ForEach(func(idx uint64) bool {
		if !exclude.IsEmpty() && exclude.Contains(idx) {
			return true
		}
		if fn(idx) {
			stop = true
			return false
		}
		return true
	})
	return stop
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

	stop := false
	exclude := result.ids
	qv.snapshotUniverseView().ForEach(func(idx uint64) bool {
		if !exclude.IsEmpty() && exclude.Contains(idx) {
			return true
		}
		if !nonEmpty.IsEmpty() && nonEmpty.Contains(idx) {
			return true
		}
		if cursor.emit(idx) {
			stop = true
			return false
		}
		return true
	})
	return stop
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

	var seen posting.List
	var seenRef *posting.List
	if isSliceOrderField {
		seenRef = &seen
		defer seen.Release()
	}

	var tmp posting.List
	defer tmp.Release()

	out := makeOutSlice[K](resultCard, need)
	cursor := qv.newQueryCursor(out, skip, need, all, seenRef)

	processBucket := func(ids posting.List) bool {
		if ids.IsEmpty() {
			return false
		}
		if idx, ok := ids.TrySingle(); ok {
			if postingResultContains(result, idx) {
				return cursor.emit(idx)
			}
			return false
		}
		if shouldUseOnePassPostingResultFilter(ids, result, cursor.need, cursor.all) {
			return qv.forEachPostingResultBucket(ids, result, func(idx uint64) bool {
				return cursor.emit(idx)
			})
		}
		tmpIntersectPosting(&tmp, ids, result.ids)
		return cursor.emitPosting(tmp)
	}

	br := ov.rangeForBounds(rangeBounds{has: true})
	cur := ov.newCursor(br, o.Desc)
	for {
		_, ids, ok := cur.next()
		if !ok {
			break
		}
		if processBucket(ids) {
			return cursor.out, nil
		}
	}

	if fm := qv.fields[o.Field]; fm != nil && fm.Ptr {
		nilIDs := qv.nilFieldOverlay(o.Field).lookupPostingRetained(nilIndexEntryKey)
		if !nilIDs.IsEmpty() {
			if processBucket(nilIDs) {
				return cursor.out, nil
			}
		}
	}
	return cursor.out, nil
}

func orderedDistinctStrings(vals []string, desc bool) []string {
	if len(vals) < 2 {
		return vals
	}

	seen := getStringSet(len(vals))
	defer releaseStringSet(seen)

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
	set := getStringSet(len(vals))
	defer releaseStringSet(set)
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
	var seen posting.List
	defer seen.Release()
	var tmp posting.List
	defer tmp.Release()
	cursor := qv.newQueryCursor(out, skip, need, all, nil)

	doValue := func(key string) bool {
		bm := ov.lookupPostingRetained(key)
		if bm.IsEmpty() {
			return false
		}

		if shouldUseOnePassPostingResultFilter(bm, result, cursor.need, cursor.all) {
			return qv.forEachPostingResultBucket(bm, result, func(idx uint64) bool {
				if seen.Contains(idx) {
					return false
				}
				seen.Add(idx)
				return cursor.emit(idx)
			})
		}

		tmpIntersectPosting(&tmp, bm, result.ids)
		it := tmp.Iter()
		defer it.Release()
		for it.HasNext() {
			idx := it.Next()
			if seen.Contains(idx) {
				continue
			}
			seen.Add(idx)
			if cursor.emit(idx) {
				return true
			}
		}
		return false
	}

	if !o.Desc {
		for _, v := range vals {
			if doValue(v) {
				return cursor.out, nil
			}
		}
	} else {
		for i := len(vals) - 1; i >= 0; i-- {
			if doValue(vals[i]) {
				return cursor.out, nil
			}
		}
	}

	qv.forEachPostingResultAll(result, func(idx uint64) bool {
		if seen.Contains(idx) {
			return false
		}
		return cursor.emit(idx)
	})

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
	cursor := qv.newQueryCursor(out, skip, need, all, nil)
	var seen posting.List
	var seenEnabled bool
	defer func() {
		seen.Release()
	}()

	var tmp posting.List
	defer tmp.Release()

	enableSeen := func() {
		if !needSeen || seenEnabled {
			return
		}
		seenEnabled = true
		cursor.seen = &seen
	}

	emitPriority := func(key string) bool {
		bm := ov.lookupPostingRetained(key)
		if bm.IsEmpty() {
			return false
		}

		if shouldUseOnePassPostingResultFilter(bm, result, cursor.need, cursor.all) {
			return qv.forEachPostingResultBucket(bm, result, func(idx uint64) bool {
				enableSeen()
				return cursor.emit(idx)
			})
		}

		tmpIntersectPosting(&tmp, bm, result.ids)
		it := tmp.Iter()
		defer it.Release()
		for it.HasNext() {
			enableSeen()
			if cursor.emit(it.Next()) {
				return true
			}
		}
		return false
	}

	for _, v := range orderedVals {
		if emitPriority(v) {
			return cursor.out, nil
		}
	}

	if !cursor.all && cursor.need == 0 {
		return cursor.out, nil
	}
	if !needFallback {
		return cursor.out, nil
	}
	qv.forEachPostingResultAll(result, func(idx uint64) bool {
		return cursor.emit(idx)
	})

	return cursor.out, nil
}

func (qv *queryView[K, V]) orderDataValues(v any) ([]string, error) {
	if vals, ok := v.([]string); ok {
		return vals, nil
	}
	vals, _, _, err := qv.exprValueToIdxBorrowed(qx.Expr{Op: qx.OpIN, Value: v})
	if err != nil {
		return nil, err
	}
	return vals, nil
}

func (qv *queryView[K, V]) queryOrderArrayCount(result postingResult, s []index, o qx.Order, skip, need uint64, all bool, useZeroComplement bool) ([]K, error) {
	resultCard := qv.postingResultCardinality(result)
	if resultCard == 0 {
		return nil, nil
	}

	out := makeOutSlice[K](resultCard, need)

	var tmp posting.List
	defer tmp.Release()

	cursor := qv.newQueryCursor(out, skip, need, all, nil)
	var nonEmpty posting.List
	if useZeroComplement {
		for _, ix := range s {
			if indexKeyEqualsString(ix.Key, lenIndexNonEmptyKey) {
				nonEmpty = ix.IDs
				break
			}
		}
	}

	processZero := func() bool {
		if !useZeroComplement {
			return false
		}
		return qv.emitArrayCountZeroBucketResult(&cursor, result, nonEmpty)
	}

	if !o.Desc {
		if processZero() {
			return cursor.out, nil
		}

		for _, ix := range s {
			if indexKeyEqualsString(ix.Key, lenIndexNonEmptyKey) {
				continue
			}
			if ix.IDs.IsEmpty() {
				continue
			}

			if shouldUseOnePassPostingResultFilter(ix.IDs, result, cursor.need, cursor.all) {
				stop := qv.forEachPostingResultBucket(ix.IDs, result, func(idx uint64) bool {
					return cursor.emit(idx)
				})
				if stop {
					return cursor.out, nil
				}
				continue
			}

			tmpIntersectPosting(&tmp, ix.IDs, result.ids)
			it := tmp.Iter()
			for it.HasNext() {
				if cursor.emit(it.Next()) {
					it.Release()
					return cursor.out, nil
				}
			}
			it.Release()
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

			if shouldUseOnePassPostingResultFilter(ix.IDs, result, cursor.need, cursor.all) {
				stop := qv.forEachPostingResultBucket(ix.IDs, result, func(idx uint64) bool {
					return cursor.emit(idx)
				})
				if stop {
					return cursor.out, nil
				}
				continue
			}

			tmpIntersectPosting(&tmp, ix.IDs, result.ids)
			it := tmp.Iter()
			for it.HasNext() {
				if cursor.emit(it.Next()) {
					it.Release()
					return cursor.out, nil
				}
			}
			it.Release()
		}
		if processZero() {
			return cursor.out, nil
		}
	}

	return cursor.out, nil
}
