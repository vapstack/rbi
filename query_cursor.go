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

	seen   u64set
	dedupe bool
	strkey bool
	strmap *strMapSnapshot
}

func (qv *queryView[K, V]) newQueryCursor(out []K, skip, need uint64, all bool, dedupeCap uint64) queryCursor[K, V] {
	c := queryCursor[K, V]{
		out:    out,
		skip:   skip,
		need:   need,
		all:    all,
		strkey: qv.strkey,
		strmap: qv.strmapView,
	}
	if dedupeCap > 0 {
		c.seen = newU64Set(clampUint64ToInt(dedupeCap))
		c.dedupe = true
	}
	return c
}

func queryCursorDedupeCap(cardinality, skip, need uint64, all bool) uint64 {
	if cardinality == 0 {
		return 0
	}
	if all {
		return cardinality
	}
	window := need
	if skip > 0 {
		maxNeed := ^uint64(0) - skip
		if need > maxNeed {
			window = ^uint64(0)
		} else {
			window += skip
		}
	}
	if window > cardinality {
		return cardinality
	}
	return window
}

func clampUint64ToInt(v uint64) int {
	maxInt := int(^uint(0) >> 1)
	if v > uint64(maxInt) {
		return maxInt
	}
	return int(v)
}

func (c *queryCursor[K, V]) release() {
	if c == nil || !c.dedupe {
		return
	}
	releaseU64Set(&c.seen)
	c.dedupe = false
}

func (c *queryCursor[K, V]) emit(idx uint64) bool {
	if c.dedupe && !c.seen.Add(idx) {
		return false
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
