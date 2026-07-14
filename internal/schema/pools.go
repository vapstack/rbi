package schema

import (
	"math/bits"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/mathutil"
)

const (
	stringSetPoolMaxLen = 4 << 10
	u64SetPoolMaxCap    = 16 << 10
	patchItemPoolMaxCap = 1024
	codecScratchMaxCap  = 512 << 10
)

type codecScratch struct {
	first  []byte
	second []byte
}

var codecScratchPool = pooled.Pointers[codecScratch]{
	Cleanup: func(s *codecScratch) {
		if cap(s.first) > codecScratchMaxCap {
			s.first = nil
		} else {
			s.first = s.first[:0]
		}
		if cap(s.second) > codecScratchMaxCap {
			s.second = nil
		} else {
			s.second = s.second[:0]
		}
	},
}

var stringSetPool = pooled.Maps[string, struct{}]{
	NewCap: 64,
	MaxLen: stringSetPoolMaxLen,
}

var patchItemPool = pooled.Slices[PatchItem]{MaxCap: patchItemPoolMaxCap, Clear: pooled.ClearCap}

func GetPatchItemSlice(capHint int) []PatchItem { return patchItemPool.Get(capHint) }
func ReleasePatchItemSlice(buf []PatchItem)     { patchItemPool.Put(buf) }

type u64setPoolBuf struct {
	keys []uint64
	used []byte
}

type u64set struct {
	keys   []uint64
	used   []byte
	mask   uint64
	n      int
	pooled *u64setPoolBuf
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

func newU64Set(capHint int) u64set {
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
	i := mathutil.Mix64(x) & s.mask
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

func (s *u64set) grow() {
	old := u64set{
		keys:   s.keys,
		used:   s.used,
		mask:   s.mask,
		n:      s.n,
		pooled: s.pooled,
	}
	next := newU64Set(len(old.keys))
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
