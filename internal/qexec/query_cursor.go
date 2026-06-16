package qexec

import "github.com/vapstack/rbi/internal/posting"

type queryCursor struct {
	out []uint64

	skip     uint64
	need     uint64
	all      bool
	allocCap uint64

	seen   u64set
	dedupe bool
}

func newQueryCursor(out []uint64, skip, need uint64, all bool, dedupeCap uint64) queryCursor {
	c := queryCursor{
		out:  out,
		skip: skip,
		need: need,
		all:  all,
	}
	if dedupeCap > 0 {
		c.seen = getU64Set(clampUint64ToInt(dedupeCap))
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

func boundedWindowCap(cardinality, offset, limit uint64) (uint64, bool) {
	if limit == 0 || offset >= cardinality {
		return 0, true
	}
	remaining := cardinality - offset
	if limit < remaining {
		return limit, false
	}
	return remaining, false
}

func (c *queryCursor) release() {
	if c == nil || !c.dedupe {
		return
	}
	releaseU64Set(&c.seen)
	c.dedupe = false
}

func (c *queryCursor) emit(idx uint64) bool {
	if c.dedupe && !c.seen.Add(idx) {
		return false
	}
	if c.skip > 0 {
		c.skip--
		return false
	}
	if c.allocCap != 0 {
		c.out = make([]uint64, 0, clampUint64ToInt(c.allocCap))
		c.allocCap = 0
	}
	c.out = append(c.out, idx)
	if !c.all {
		c.need--
		return c.need == 0
	}
	return false
}

func (c *queryCursor) emitPosting(ids posting.List) bool {
	if ids.IsEmpty() {
		return false
	}
	if idx, ok := ids.TrySingle(); ok {
		return c.emit(idx)
	}
	if !c.dedupe && c.skip >= uint64(iteratorThreshold) {
		card := ids.Cardinality()
		if c.skip >= card {
			c.skip -= card
			return false
		}
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
