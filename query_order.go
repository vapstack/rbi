package rbi

import (
	"io"

	"github.com/vapstack/qx"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
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

func tmpORSmallestAND(tmp, a, b *roaring64.Bitmap) {
	tmp.Xor(tmp)
	if a.GetCardinality() < b.GetCardinality() {
		tmp.Or(a)
		tmp.And(b)
	} else {
		tmp.Or(b)
		tmp.And(a)
	}
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

func (db *DB[K, V]) queryOrderBasic(result bitmap, s *[]index, o qx.Order, skip, need uint64, all bool) ([]K, error) {
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

	out := makeOutSlice[K](result.bm, need)
	cursor := db.newQueryCursor(out, skip, need, all, seen)

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	processBucket := func(ix index) bool {
		card := ix.IDs.GetCardinality()
		if card == 0 {
			return false
		}

		if card <= iteratorThreshold || (0 < cursor.need && cursor.need < 1000) {
			if card == 1 {
				idx := ix.IDs.Minimum()
				if !result.bm.Contains(idx) {
					return false
				}
				return cursor.emit(idx)
			}

			iter := ix.IDs.Iterator()
			for iter.HasNext() {
				idx := iter.Next()
				if !result.bm.Contains(idx) {
					continue
				}
				if cursor.emit(idx) {
					return true
				}
			}
			return false
		}

		tmpORSmallestAND(tmp, ix.IDs, result.bm)
		if tmp.IsEmpty() {
			return false
		}
		iter := tmp.Iterator()
		for iter.HasNext() {
			if cursor.emit(iter.Next()) {
				return true
			}
		}
		return false
	}

	if !o.Desc {
		for _, ix := range *s {
			if processBucket(ix) {
				return cursor.out, nil
			}
		}
		return cursor.out, nil
	}

	slice := *s
	for i := len(slice) - 1; i >= 0; i-- {
		if processBucket(slice[i]) {
			return cursor.out, nil
		}
	}
	return cursor.out, nil
}

func (db *DB[K, V]) queryOrderArrayPos(result bitmap, s *[]index, o qx.Order, skip, need uint64, all bool) ([]K, error) {

	vals, _, err := db.exprValueToIdx(qx.Expr{Op: qx.OpIN, Value: o.Data})
	if err != nil {
		return nil, err
	}

	if result.readonly {
		result = result.clone()
	}

	out := makeOutSlice[K](result.bm, need)

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	tmp := getRoaringBuf()
	defer releaseRoaringBuf(tmp)
	cursor := db.newQueryCursor(out, skip, need, all, nil)

	doValue := func(key string) error {
		bm := findIndex(s, key)
		if bm == nil {
			return nil
		}

		if shouldUseOnePassIntersection(bm, result.bm, cursor.need, cursor.all) {
			it := bm.Iterator()
			for it.HasNext() {
				idx := it.Next()
				if !result.bm.Contains(idx) {
					continue
				}
				result.bm.Remove(idx)
				if cursor.emit(idx) {
					return io.EOF // stop signal
				}
			}
			return nil
		}

		tmpORSmallestAND(tmp, bm, result.bm)
		it := tmp.Iterator()
		for it.HasNext() {
			idx := it.Next()
			result.bm.Remove(idx)
			if cursor.emit(idx) {
				return io.EOF
			}
		}
		return nil
	}

	if !o.Desc {
		for _, v := range vals {
			if err = doValue(v); err == io.EOF {
				return cursor.out, nil
			} else if err != nil {
				return nil, err
			}
		}
	} else {
		for i := len(vals) - 1; i >= 0; i-- {
			if err = doValue(vals[i]); err == io.EOF {
				return cursor.out, nil
			} else if err != nil {
				return nil, err
			}
		}
	}

	// process remaining items (those not in the priority list)
	iter := result.bm.Iterator()
	for iter.HasNext() {
		if cursor.emit(iter.Next()) {
			break
		}
	}

	return cursor.out, nil
}

func (db *DB[K, V]) queryOrderArrayCount(result bitmap, s []index, o qx.Order, skip, need uint64, all bool) ([]K, error) {

	out := makeOutSlice[K](result.bm, need)

	if result.readonly {
		result = result.clone()
	}

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	tmp := getRoaringBuf()
	defer releaseRoaringBuf(tmp)
	cursor := db.newQueryCursor(out, skip, need, all, nil)

	if !o.Desc {

		for _, ix := range s {
			if ix.IDs == nil || ix.IDs.IsEmpty() || result.bm == nil || result.bm.IsEmpty() {
				continue
			}

			if shouldUseOnePassIntersection(ix.IDs, result.bm, cursor.need, cursor.all) {
				stop := forEachIntersectionContains(ix.IDs, result.bm, func(idx uint64) bool {
					return cursor.emit(idx)
				})
				if stop {
					return cursor.out, nil
				}
				continue
			}

			tmpORSmallestAND(tmp, ix.IDs, result.bm)
			it := tmp.Iterator()
			for it.HasNext() {
				if cursor.emit(it.Next()) {
					return cursor.out, nil
				}
			}
		}

	} else {

		for i := len(s) - 1; i >= 0; i-- {
			ix := s[i]
			if ix.IDs == nil || ix.IDs.IsEmpty() || result.bm == nil || result.bm.IsEmpty() {
				continue
			}

			if shouldUseOnePassIntersection(ix.IDs, result.bm, cursor.need, cursor.all) {
				stop := forEachIntersectionContains(ix.IDs, result.bm, func(idx uint64) bool {
					return cursor.emit(idx)
				})
				if stop {
					return cursor.out, nil
				}
				continue
			}

			tmpORSmallestAND(tmp, ix.IDs, result.bm)
			it := tmp.Iterator()
			for it.HasNext() {
				if cursor.emit(it.Next()) {
					return cursor.out, nil
				}
			}
		}
	}

	return cursor.out, nil
}
