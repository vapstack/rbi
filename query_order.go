package rbi

import (
	"github.com/vapstack/qx"

	"github.com/vapstack/rbi/internal/roaring64"
)

func forEachIntersectionContains(a, b *roaring64.Bitmap, fn func(uint64) bool) bool {
	if a == nil || b == nil || a.IsEmpty() || b.IsEmpty() {
		return false
	}

	small := a
	large := b
	if a.GetCardinality() > b.GetCardinality() {
		small = b
		large = a
	}

	it := small.Iterator()
	defer releaseRoaringBitmapIterator(it)
	for it.HasNext() {
		idx := it.Next()
		if !large.Contains(idx) {
			continue
		}
		if fn(idx) {
			return true
		}
	}
	return false
}

func forEachIntersectionContainsPosting(a postingList, b *roaring64.Bitmap, fn func(uint64) bool) bool {
	if b == nil || b.IsEmpty() || a.IsEmpty() {
		return false
	}
	if a.isSingleton() {
		if !b.Contains(a.single) {
			return false
		}
		return fn(a.single)
	}
	return forEachIntersectionContains(a.bitmap(), b, fn)
}

func shouldUseOnePassIntersection(a, b *roaring64.Bitmap, need uint64, all bool) bool {
	if a == nil || b == nil || a.IsEmpty() || b.IsEmpty() {
		return false
	}
	if !all && need > 0 && need <= 1024 {
		return true
	}
	minCard := a.GetCardinality()
	if bc := b.GetCardinality(); bc < minCard {
		minCard = bc
	}
	return minCard <= iteratorThreshold*4
}

func shouldUseOnePassIntersectionPosting(a postingList, b *roaring64.Bitmap, need uint64, all bool) bool {
	if b == nil || b.IsEmpty() || a.IsEmpty() {
		return false
	}
	if !all && need > 0 && need <= 1024 {
		return true
	}
	minCard := a.Cardinality()
	if bc := b.GetCardinality(); bc < minCard {
		minCard = bc
	}
	return minCard <= iteratorThreshold*4
}

func tmpORSmallestAND(tmp, a, b *roaring64.Bitmap) {
	tmp.Clear()
	if a.GetCardinality() < b.GetCardinality() {
		tmp.Or(a)
		tmp.And(b)
	} else {
		tmp.Or(b)
		tmp.And(a)
	}
}

func tmpORSmallestANDPosting(tmp *roaring64.Bitmap, a postingList, b *roaring64.Bitmap) {
	tmp.Clear()
	if b == nil || b.IsEmpty() || a.IsEmpty() {
		return
	}
	if a.isSingleton() {
		if b.Contains(a.single) {
			tmp.Add(a.single)
		}
		return
	}
	tmpORSmallestAND(tmp, a.bitmap(), b)
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

func (db *DB[K, V]) queryOrderBasic(result bitmap, ov fieldOverlay, o qx.Order, skip, need uint64, all bool) ([]K, error) {
	if result.bm == nil || result.bm.IsEmpty() {
		return nil, nil
	}
	resultBM := result.bm

	isSliceOrderField := false
	if fm := db.fields[o.Field]; fm != nil && fm.Slice {
		isSliceOrderField = true
	}

	var seen *roaring64.Bitmap
	if isSliceOrderField {
		seen = getRoaringBuf()
		defer releaseRoaringBuf(seen)
	}

	tmp := getRoaringBuf()
	defer releaseRoaringBuf(tmp)
	var scratch *roaring64.Bitmap
	if ov.delta != nil {
		scratch = getRoaringBuf()
		defer releaseRoaringBuf(scratch)
	}

	out := makeOutSlice[K](result.bm, need)
	cursor := db.newQueryCursor(out, skip, need, all, seen)

	processBucket := func(ids *roaring64.Bitmap) bool {
		if ids == nil || ids.IsEmpty() {
			return false
		}
		card := ids.GetCardinality()

		if card <= iteratorThreshold || (0 < cursor.need && cursor.need < 1000) {
			if card == 1 {
				idx := ids.Minimum()
				if !resultBM.Contains(idx) {
					return false
				}
				return cursor.emit(idx)
			}

			iter := ids.Iterator()
			defer releaseRoaringBitmapIterator(iter)
			for iter.HasNext() {
				idx := iter.Next()
				if !resultBM.Contains(idx) {
					continue
				}
				if cursor.emit(idx) {
					return true
				}
			}
			return false
		}

		tmpORSmallestAND(tmp, ids, resultBM)
		if tmp.IsEmpty() {
			return false
		}
		iter := tmp.Iterator()
		defer releaseRoaringBitmapIterator(iter)
		for iter.HasNext() {
			if cursor.emit(iter.Next()) {
				return true
			}
		}
		return false
	}

	br := ov.rangeForBounds(rangeBounds{has: true})
	cur := ov.newCursor(br, o.Desc)
	for {
		_, baseBM, de, ok := cur.next()
		if !ok {
			break
		}
		bm, owned := composePostingOwned(baseBM, de, scratch)
		if bm == nil || bm.IsEmpty() {
			if owned && bm != nil && bm != scratch {
				releaseRoaringBuf(bm)
			}
			continue
		}
		if processBucket(bm) {
			if owned && bm != scratch {
				releaseRoaringBuf(bm)
			}
			return cursor.out, nil
		}
		if owned && bm != scratch {
			releaseRoaringBuf(bm)
		}
	}

	if fm := db.fields[o.Field]; fm != nil && fm.Ptr {
		nilBM, owned := db.nilFieldLookupOwned(o.Field, nil)
		if nilBM != nil && !nilBM.IsEmpty() {
			if processBucket(nilBM) {
				if owned {
					releaseRoaringBuf(nilBM)
				}
				return cursor.out, nil
			}
		}
		if owned {
			releaseRoaringBuf(nilBM)
		}
	}
	return cursor.out, nil
}

func (db *DB[K, V]) queryOrderBasicSlice(result bitmap, s *[]index, o qx.Order, skip, need uint64, all bool) ([]K, error) {
	if result.bm == nil || result.bm.IsEmpty() {
		return nil, nil
	}
	resultBM := result.bm
	var base []index
	if s != nil {
		base = *s
	}

	isSliceOrderField := false
	isPtrOrderField := false
	if fm := db.fields[o.Field]; fm != nil {
		isSliceOrderField = fm.Slice
		isPtrOrderField = fm.Ptr
	}

	var seen *roaring64.Bitmap
	if isSliceOrderField {
		seen = getRoaringBuf()
		defer releaseRoaringBuf(seen)
	}

	tmp := getRoaringBuf()
	defer releaseRoaringBuf(tmp)

	out := makeOutSlice[K](result.bm, need)
	cursor := db.newQueryCursor(out, skip, need, all, seen)

	processBucket := func(ids postingList) bool {
		if ids.IsEmpty() {
			return false
		}
		card := ids.Cardinality()

		if card <= iteratorThreshold || (0 < cursor.need && cursor.need < 1000) {
			if card == 1 {
				idx, _ := ids.Minimum()
				if !resultBM.Contains(idx) {
					return false
				}
				return cursor.emit(idx)
			}

			stop := false
			ids.ForEach(func(idx uint64) bool {
				if !resultBM.Contains(idx) {
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

		tmpORSmallestANDPosting(tmp, ids, resultBM)
		if tmp.IsEmpty() {
			return false
		}
		iter := tmp.Iterator()
		defer releaseRoaringBitmapIterator(iter)
		for iter.HasNext() {
			if cursor.emit(iter.Next()) {
				return true
			}
		}
		return false
	}

	if !o.Desc {
		for _, ix := range base {
			if processBucket(ix.IDs) {
				return cursor.out, nil
			}
		}
	} else {
		for i := len(base) - 1; i >= 0; i-- {
			if processBucket(base[i].IDs) {
				return cursor.out, nil
			}
		}
	}

	if isPtrOrderField {
		nilBM, owned := db.nilFieldLookupOwned(o.Field, nil)
		if nilBM != nil && !nilBM.IsEmpty() {
			if processBucket(postingFromBitmapViewAdaptive(nilBM)) {
				if owned {
					releaseRoaringBuf(nilBM)
				}
				return cursor.out, nil
			}
		}
		if owned {
			releaseRoaringBuf(nilBM)
		}
	}

	return cursor.out, nil
}

func (db *DB[K, V]) queryOrderArrayPos(result bitmap, s *[]index, o qx.Order, skip, need uint64, all bool) ([]K, error) {
	if result.bm == nil || result.bm.IsEmpty() {
		return nil, nil
	}
	resultBM := result.bm

	vals, err := db.orderDataValues(o.Data)
	if err != nil {
		return nil, err
	}
	if fm := db.fields[o.Field]; fm != nil && !fm.Slice {
		return db.queryOrderArrayPosScalar(result, s, vals, o.Desc, skip, need, all)
	}

	out := makeOutSlice[K](result.bm, need)

	seen := getRoaringBuf()
	defer releaseRoaringBuf(seen)

	tmp := getRoaringBuf()
	defer releaseRoaringBuf(tmp)

	cursor := db.newQueryCursor(out, skip, need, all, nil)

	doValue := func(key string) bool {
		bm := findIndex(s, key)
		if bm.IsEmpty() {
			return false
		}

		if shouldUseOnePassIntersectionPosting(bm, resultBM, cursor.need, cursor.all) {
			stop := false
			bm.ForEach(func(idx uint64) bool {
				if !resultBM.Contains(idx) || seen.Contains(idx) {
					return true
				}
				seen.Add(idx)
				if cursor.emit(idx) {
					stop = true
					return false
				}
				return true
			})
			return stop
		}

		tmpORSmallestANDPosting(tmp, bm, resultBM)
		it := tmp.Iterator()
		defer releaseRoaringBitmapIterator(it)
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

	// process remaining items (those not in the priority list)
	iter := resultBM.Iterator()
	defer releaseRoaringBitmapIterator(iter)
	for iter.HasNext() {
		idx := iter.Next()
		if seen.Contains(idx) {
			continue
		}
		if cursor.emit(idx) {
			break
		}
	}

	return cursor.out, nil
}

func dedupStringsStable(vals []string) []string {
	if len(vals) < 2 {
		return vals
	}
	for i := 1; i < len(vals); i++ {
		for j := 0; j < i; j++ {
			if vals[i] == vals[j] {
				seen := make(map[string]struct{}, len(vals))
				out := make([]string, 0, len(vals))
				for _, v := range vals {
					if _, ok := seen[v]; ok {
						continue
					}
					seen[v] = struct{}{}
					out = append(out, v)
				}
				return out
			}
		}
	}
	return vals
}

func scalarArrayPosPriorityCoversAllKeys(s []index, vals []string) bool {
	if len(s) == 0 {
		return true
	}
	if len(vals) == 0 {
		return false
	}
	set := make(map[string]struct{}, len(vals))
	for _, v := range vals {
		set[v] = struct{}{}
	}
	if len(set) < len(s) {
		return false
	}
	for i := range s {
		if _, ok := set[s[i].Key.asString()]; !ok {
			return false
		}
	}
	return true
}

func (db *DB[K, V]) queryOrderArrayPosScalar(result bitmap, s *[]index, vals []string, desc bool, skip, need uint64, all bool) ([]K, error) {
	if result.bm == nil || result.bm.IsEmpty() {
		return nil, nil
	}
	resultBM := result.bm
	vals = dedupStringsStable(vals)

	out := makeOutSlice[K](result.bm, need)
	cursor := db.newQueryCursor(out, skip, need, all, nil)

	tmp := getRoaringBuf()
	defer releaseRoaringBuf(tmp)

	emitPriority := func(key string) bool {
		bm := findIndex(s, key)
		if bm.IsEmpty() {
			return false
		}

		if shouldUseOnePassIntersectionPosting(bm, resultBM, cursor.need, cursor.all) {
			stop := false
			bm.ForEach(func(idx uint64) bool {
				if !resultBM.Contains(idx) {
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

		tmpORSmallestANDPosting(tmp, bm, resultBM)
		it := tmp.Iterator()
		defer releaseRoaringBitmapIterator(it)
		for it.HasNext() {
			if cursor.emit(it.Next()) {
				return true
			}
		}
		return false
	}

	if !desc {
		for _, v := range vals {
			if emitPriority(v) {
				return cursor.out, nil
			}
		}
	} else {
		for i := len(vals) - 1; i >= 0; i-- {
			if emitPriority(vals[i]) {
				return cursor.out, nil
			}
		}
	}

	if !cursor.all && cursor.need == 0 {
		return cursor.out, nil
	}

	if scalarArrayPosPriorityCoversAllKeys(*s, vals) {
		return cursor.out, nil
	}

	seen := getRoaringBuf()
	defer releaseRoaringBuf(seen)

	markPriority := func(key string) {
		bm := findIndex(s, key)
		if bm.IsEmpty() {
			return
		}
		if shouldUseOnePassIntersectionPosting(bm, resultBM, 0, true) {
			bm.ForEach(func(idx uint64) bool {
				if resultBM.Contains(idx) {
					seen.Add(idx)
				}
				return true
			})
			return
		}
		tmpORSmallestANDPosting(tmp, bm, resultBM)
		seen.Or(tmp)
	}

	if !desc {
		for _, v := range vals {
			markPriority(v)
		}
	} else {
		for i := len(vals) - 1; i >= 0; i-- {
			markPriority(vals[i])
		}
	}

	iter := resultBM.Iterator()
	defer releaseRoaringBitmapIterator(iter)
	for iter.HasNext() {
		idx := iter.Next()
		if seen.Contains(idx) {
			continue
		}
		if cursor.emit(idx) {
			break
		}
	}

	return cursor.out, nil
}

func (db *DB[K, V]) queryOrderArrayPosOverlay(result bitmap, ov fieldOverlay, o qx.Order, skip, need uint64, all bool) ([]K, error) {
	if result.bm == nil || result.bm.IsEmpty() {
		return nil, nil
	}
	resultBM := result.bm

	vals, err := db.orderDataValues(o.Data)
	if err != nil {
		return nil, err
	}

	out := makeOutSlice[K](result.bm, need)

	seen := getRoaringBuf()
	defer releaseRoaringBuf(seen)

	tmp := getRoaringBuf()
	defer releaseRoaringBuf(tmp)

	var scratch *roaring64.Bitmap
	if ov.delta != nil {
		scratch = getRoaringBuf()
		defer releaseRoaringBuf(scratch)
	}

	cursor := db.newQueryCursor(out, skip, need, all, nil)

	doValue := func(key string) bool {
		bm, owned := ov.lookupOwned(key, scratch)
		if bm == nil || bm.IsEmpty() {
			if owned && bm != nil && bm != scratch {
				releaseRoaringBuf(bm)
			}
			return false
		}

		if shouldUseOnePassIntersection(bm, resultBM, cursor.need, cursor.all) {
			it := bm.Iterator()
			defer releaseRoaringBitmapIterator(it)
			for it.HasNext() {
				idx := it.Next()
				if !resultBM.Contains(idx) {
					continue
				}
				if seen != nil {
					if seen.Contains(idx) {
						continue
					}
					seen.Add(idx)
				}
				if cursor.emit(idx) {
					if owned && bm != scratch {
						releaseRoaringBuf(bm)
					}
					return true
				}
			}
			if owned && bm != scratch {
				releaseRoaringBuf(bm)
			}
			return false
		}

		tmpORSmallestAND(tmp, bm, resultBM)
		if owned && bm != scratch {
			releaseRoaringBuf(bm)
		}
		it := tmp.Iterator()
		defer releaseRoaringBitmapIterator(it)
		for it.HasNext() {
			idx := it.Next()
			if seen != nil {
				if seen.Contains(idx) {
					continue
				}
				seen.Add(idx)
			}
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

	iter := resultBM.Iterator()
	defer releaseRoaringBitmapIterator(iter)
	for iter.HasNext() {
		idx := iter.Next()
		if seen != nil {
			if seen.Contains(idx) {
				continue
			}
		}
		if cursor.emit(idx) {
			break
		}
	}

	return cursor.out, nil
}

func (db *DB[K, V]) orderDataValues(v any) ([]string, error) {
	if vals, ok := v.([]string); ok {
		return vals, nil
	}
	vals, _, _, err := db.exprValueToIdx(qx.Expr{Op: qx.OpIN, Value: v})
	if err != nil {
		return nil, err
	}
	return vals, nil
}

func (db *DB[K, V]) queryOrderArrayCount(result bitmap, s []index, o qx.Order, skip, need uint64, all bool, useZeroComplement bool) ([]K, error) {
	if result.bm == nil || result.bm.IsEmpty() {
		return nil, nil
	}
	resultBM := result.bm

	out := makeOutSlice[K](result.bm, need)

	tmp := getRoaringBuf()
	defer releaseRoaringBuf(tmp)
	cursor := db.newQueryCursor(out, skip, need, all, nil)
	var nonEmpty postingList
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
		zero := getRoaringBuf()
		zero.Or(resultBM)
		nonEmpty.AndNotFrom(zero)
		it := zero.Iterator()
		defer releaseRoaringBitmapIterator(it)
		for it.HasNext() {
			if cursor.emit(it.Next()) {
				releaseRoaringBuf(zero)
				return true
			}
		}
		releaseRoaringBuf(zero)
		return false
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

			if shouldUseOnePassIntersectionPosting(ix.IDs, resultBM, cursor.need, cursor.all) {
				stop := forEachIntersectionContainsPosting(ix.IDs, resultBM, func(idx uint64) bool {
					return cursor.emit(idx)
				})
				if stop {
					return cursor.out, nil
				}
				continue
			}

			tmpORSmallestANDPosting(tmp, ix.IDs, resultBM)
			it := tmp.Iterator()
			defer releaseRoaringBitmapIterator(it)
			for it.HasNext() {
				if cursor.emit(it.Next()) {
					return cursor.out, nil
				}
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

			if shouldUseOnePassIntersectionPosting(ix.IDs, resultBM, cursor.need, cursor.all) {
				stop := forEachIntersectionContainsPosting(ix.IDs, resultBM, func(idx uint64) bool {
					return cursor.emit(idx)
				})
				if stop {
					return cursor.out, nil
				}
				continue
			}

			tmpORSmallestANDPosting(tmp, ix.IDs, resultBM)
			it := tmp.Iterator()
			defer releaseRoaringBitmapIterator(it)
			for it.HasNext() {
				if cursor.emit(it.Next()) {
					return cursor.out, nil
				}
			}
		}
		if processZero() {
			return cursor.out, nil
		}
	}

	return cursor.out, nil
}

func (db *DB[K, V]) queryOrderArrayCountOverlay(result bitmap, ov fieldOverlay, o qx.Order, skip, need uint64, all bool, useZeroComplement bool) ([]K, error) {
	if result.bm == nil || result.bm.IsEmpty() {
		return nil, nil
	}
	resultBM := result.bm

	out := makeOutSlice[K](result.bm, need)

	tmp := getRoaringBuf()
	defer releaseRoaringBuf(tmp)
	var scratch *roaring64.Bitmap
	if ov.delta != nil {
		scratch = getRoaringBuf()
		defer releaseRoaringBuf(scratch)
	}
	cursor := db.newQueryCursor(out, skip, need, all, nil)

	processBucket := func(ids *roaring64.Bitmap) bool {
		if ids == nil || ids.IsEmpty() {
			return false
		}
		if shouldUseOnePassIntersection(ids, resultBM, cursor.need, cursor.all) {
			return forEachIntersectionContains(ids, resultBM, func(idx uint64) bool {
				return cursor.emit(idx)
			})
		}
		tmpORSmallestAND(tmp, ids, resultBM)
		it := tmp.Iterator()
		defer releaseRoaringBitmapIterator(it)
		for it.HasNext() {
			if cursor.emit(it.Next()) {
				return true
			}
		}
		return false
	}

	processZero := func() bool {
		if !useZeroComplement {
			return false
		}
		nonEmpty, nonEmptyOwned := ov.lookupOwned(lenIndexNonEmptyKey, scratch)
		zero := getRoaringBuf()
		zero.Or(resultBM)
		if nonEmpty != nil && !nonEmpty.IsEmpty() {
			zero.AndNot(nonEmpty)
		}
		if nonEmptyOwned && nonEmpty != nil && nonEmpty != scratch {
			releaseRoaringBuf(nonEmpty)
		}
		stop := processBucket(zero)
		releaseRoaringBuf(zero)
		return stop
	}

	br := ov.rangeForBounds(rangeBounds{has: true})
	cur := ov.newCursor(br, o.Desc)
	if !o.Desc && processZero() {
		return cursor.out, nil
	}
	for {
		key, baseBM, de, ok := cur.next()
		if !ok {
			break
		}
		if indexKeyEqualsString(key, lenIndexNonEmptyKey) {
			continue
		}
		bm, owned := composePostingOwned(baseBM, de, scratch)
		if bm == nil || bm.IsEmpty() {
			if owned && bm != nil && bm != scratch {
				releaseRoaringBuf(bm)
			}
			continue
		}
		if processBucket(bm) {
			if owned && bm != scratch {
				releaseRoaringBuf(bm)
			}
			return cursor.out, nil
		}
		if owned && bm != scratch {
			releaseRoaringBuf(bm)
		}
	}
	if o.Desc && processZero() {
		return cursor.out, nil
	}
	return cursor.out, nil
}
