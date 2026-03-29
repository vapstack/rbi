package rbi

import (
	"unsafe"

	"github.com/vapstack/rbi/internal/posting"
)

type queryCursor[K ~uint64 | ~string, V any] struct {
	out []K

	skip uint64
	need uint64
	all  bool

	seen   *posting.List
	strkey bool
	strmap *strMapSnapshot
}

func (qv *queryView[K, V]) newQueryCursor(out []K, skip, need uint64, all bool, seen *posting.List) queryCursor[K, V] {
	return queryCursor[K, V]{
		out:    out,
		skip:   skip,
		need:   need,
		all:    all,
		seen:   seen,
		strkey: qv.strkey,
		strmap: qv.strmapView,
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
	if c.strkey {
		s, ok := c.strmap.getStringNoLock(idx)
		if !ok {
			panic("rbi: no string key associated with snapshot idx")
		}
		c.out = append(c.out, *(*K)(unsafe.Pointer(&s)))
	} else {
		c.out = append(c.out, *(*K)(unsafe.Pointer(&idx)))
	}
	if !c.all {
		c.need--
		return c.need == 0
	}
	return false
}

func (c *queryCursor[K, V]) emitPosting(ids posting.List) bool {
	if ids.IsEmpty() {
		return false
	}
	if idx, ok := ids.TrySingle(); ok {
		return c.emit(idx)
	}
	it := ids.Iter()
	defer it.Release()
	for it.HasNext() {
		if c.emit(it.Next()) {
			return true
		}
	}
	return false
}
