package pools

import (
	"math/bits"
	"sync"
	"unsafe"
)

var (
	uintPools   [maxNumericShift + 1]sync.Pool
	uint32Pools [maxNumericShift + 1]sync.Pool
	uint64Pools [maxNumericShift + 1]sync.Pool
)

func GetUint64Slice(capHint int) []uint64 {
	if capHint < 0 {
		panic("GetUint64Slice: negative capHint")
	}

	if capHint > MaxNumericPooledCap {
		return make([]uint64, 0, capHint)
	}
	var shift int
	if capHint <= MinNumericPooledCap {
		shift = minNumericShift
	} else {
		shift = bits.Len(uint(capHint - 1)) // ceil(log2(capHint))
	}

	lim := min(shift+3, maxNumericShift)
	for sh := shift; sh <= lim; sh++ {
		if v := uint64Pools[sh].Get(); v != nil {
			ptr := v.(*uint64)
			n := 1 << sh
			s := unsafe.Slice(ptr, n)
			return s[:0:n]
		}
	}

	return make([]uint64, 0, 1<<shift)
}

func PutUint64Slice(s []uint64) {
	c := cap(s)
	if c < MinNumericPooledCap {
		return
	}
	var shift int
	if c >= MaxNumericPooledCap {
		if c <= MaxNumericRetainedCap {
			shift = maxNumericShift
		} else {
			return
		}
	} else {
		shift = bits.Len(uint(c)) - 1 // floor(log2(c))
	}
	n := 1 << shift
	uint64Pools[shift].Put(unsafe.SliceData(s[:n:n]))
}

func GetUint32Slice(capHint int) []uint32 {
	if capHint < 0 {
		panic("GetUint32Slice: negative capHint")
	}

	if capHint > MaxNumericPooledCap {
		return make([]uint32, 0, capHint)
	}
	var shift int
	if capHint <= MinNumericPooledCap {
		shift = minNumericShift
	} else {
		shift = bits.Len(uint(capHint - 1)) // ceil(log2(capHint))
	}

	lim := min(shift+3, maxNumericShift)
	for sh := shift; sh <= lim; sh++ {
		if v := uint32Pools[sh].Get(); v != nil {
			ptr := v.(*uint32)
			n := 1 << sh
			s := unsafe.Slice(ptr, n)
			return s[:0:n]
		}
	}

	return make([]uint32, 0, 1<<shift)
}

func PutUint32Slice(s []uint32) {
	c := cap(s)
	if c < MinNumericPooledCap {
		return
	}
	var shift int
	if c >= MaxNumericPooledCap {
		if c <= MaxNumericRetainedCap {
			shift = maxNumericShift
		} else {
			return
		}
	} else {
		shift = bits.Len(uint(c)) - 1 // floor(log2(c))
	}
	n := 1 << shift
	uint32Pools[shift].Put(unsafe.SliceData(s[:n:n]))
}

func GetUintSlice(capHint int) []uint {
	if capHint < 0 {
		panic("GetUintSlice: negative capHint")
	}

	if capHint > MaxNumericPooledCap {
		return make([]uint, 0, capHint)
	}
	var shift int
	if capHint <= MinNumericPooledCap {
		shift = minNumericShift
	} else {
		shift = bits.Len(uint(capHint - 1)) // ceil(log2(capHint))
	}

	lim := min(shift+3, maxNumericShift)
	for sh := shift; sh <= lim; sh++ {
		if v := uintPools[sh].Get(); v != nil {
			ptr := v.(*uint)
			n := 1 << sh
			s := unsafe.Slice(ptr, n)
			return s[:0:n]
		}
	}

	return make([]uint, 0, 1<<shift)
}

func PutUintSlice(s []uint) {
	c := cap(s)
	if c < MinNumericPooledCap {
		return
	}
	var shift int
	if c >= MaxNumericPooledCap {
		if c <= MaxNumericRetainedCap {
			shift = maxNumericShift
		} else {
			return
		}
	} else {
		shift = bits.Len(uint(c)) - 1 // floor(log2(c))
	}
	n := 1 << shift
	uintPools[shift].Put(unsafe.SliceData(s[:n:n]))
}
