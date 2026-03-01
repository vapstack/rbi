package rbi

import "github.com/RoaringBitmap/roaring/v2/roaring64"

type queryCursor[K ~uint64 | ~string, V any] struct {
	db *DB[K, V]

	out []K

	skip uint64
	need uint64
	all  bool

	seen *roaring64.Bitmap
}

func (db *DB[K, V]) newQueryCursor(out []K, skip, need uint64, all bool, seen *roaring64.Bitmap) queryCursor[K, V] {
	return queryCursor[K, V]{
		db:   db,
		out:  out,
		skip: skip,
		need: need,
		all:  all,
		seen: seen,
	}
}

func (c *queryCursor[K, V]) emit(idx uint64) bool {
	if c.seen != nil {
		if c.seen.Contains(idx) {
			return false
		}
		c.seen.Add(idx)
	}
	if c.skip > 0 {
		c.skip--
		return false
	}
	c.out = append(c.out, c.db.idFromIdxNoLock(idx))
	if !c.all {
		c.need--
		return c.need == 0
	}
	return false
}

func (c *queryCursor[K, V]) emitBitmap(bm *roaring64.Bitmap) bool {
	if bm == nil || bm.IsEmpty() {
		return false
	}
	it := bm.Iterator()
	for it.HasNext() {
		if c.emit(it.Next()) {
			return true
		}
	}
	return false
}
