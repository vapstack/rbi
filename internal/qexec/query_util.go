package qexec

import (
	"math/bits"

	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/qir"
)

type u64setPoolBuf struct {
	keys []uint64
	used []byte
}

var u64setPools = initU64SetPools()

func initU64SetPools() []pooled.Pointers[u64setPoolBuf] {
	pools := make([]pooled.Pointers[u64setPoolBuf], u64setPoolClassIndex(u64SetPoolMaxCap)+1)
	for i := range pools {
		size := 1 << i
		pools[i] = pooled.Pointers[u64setPoolBuf]{
			New: func() *u64setPoolBuf {
				return &u64setPoolBuf{
					keys: make([]uint64, size),
					used: make([]byte, size),
				}
			},
		}
	}
	return pools
}

func u64setPoolClassIndex(size int) int {
	return bits.Len(uint(size)) - 1
}

func u64setRequiredSize(capHint int) int {
	size := 1
	for size < capHint*2 {
		size <<= 1
	}
	return size
}

type u64set struct {
	keys   []uint64
	used   []byte
	mask   uint64
	n      int
	pooled *u64setPoolBuf
}

func getU64Set(capHint int) u64set {
	size := u64setRequiredSize(capHint)
	if size <= u64SetPoolMaxCap {
		buf := u64setPools[u64setPoolClassIndex(size)].Get()
		return u64set{
			keys:   buf.keys[:size],
			used:   buf.used[:size],
			mask:   uint64(size - 1),
			pooled: buf,
		}
	}
	return u64set{
		keys: make([]uint64, size),
		used: make([]byte, size),
		mask: uint64(size - 1),
	}
}

func releaseU64Set(s *u64set) {
	buf := s.pooled
	size := cap(s.keys)
	if buf != nil && size > 0 && size == cap(s.used) && size <= u64SetPoolMaxCap {
		clear(s.used[:size])
		u64setPools[u64setPoolClassIndex(size)].Put(buf)
	}
	*s = u64set{}
}

func (s *u64set) Add(x uint64) bool {
	if s.n*2 >= len(s.keys) {
		s.grow()
	}
	i := mix64(x) & s.mask
	for {
		if s.used[i] == 0 {
			s.used[i] = 1
			s.keys[i] = x
			s.n++
			return true
		}
		if s.keys[i] == x {
			return false
		}
		i = (i + 1) & s.mask
	}
}

func (s *u64set) Has(x uint64) bool {
	if len(s.keys) == 0 {
		return false
	}
	i := mix64(x) & s.mask
	for {
		if s.used[i] == 0 {
			return false
		}
		if s.keys[i] == x {
			return true
		}
		i = (i + 1) & s.mask
	}
}

func (s *u64set) Len() int {
	return s.n
}

func (s *u64set) grow() {
	old := u64set{
		keys:   s.keys,
		used:   s.used,
		mask:   s.mask,
		n:      s.n,
		pooled: s.pooled,
	}
	next := getU64Set(len(old.keys))
	s.keys = next.keys
	s.used = next.used
	s.mask = next.mask
	s.n = 0
	s.pooled = next.pooled

	for i := 0; i < len(old.keys); i++ {
		if old.used[i] != 0 {
			_ = s.Add(old.keys[i])
		}
	}
	releaseU64Set(&old)
}

func mix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func satMulUint64(a, b uint64) uint64 {
	if a == 0 || b == 0 {
		return 0
	}
	if ^uint64(0)/a < b {
		return ^uint64(0)
	}
	return a * b
}

func satAddUint64(total, add uint64) uint64 {
	if ^uint64(0)-total < add {
		return ^uint64(0)
	}
	return total + add
}

func (qv *View) isPositiveUniqueEqExpr(e qir.Expr) bool {
	if !isPositiveScalarEqLeaf(e) {
		return false
	}
	fm := qv.fieldMetaByExpr(e)
	return fm != nil && !fm.Slice && fm.Unique
}
