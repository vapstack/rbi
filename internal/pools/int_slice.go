package pools

import (
	"math/bits"
	"sync"
	"unsafe"
)

var (
	intPools   [maxNumericShift + 1]sync.Pool
	int32Pools [maxNumericShift + 1]sync.Pool
	int64Pools [maxNumericShift + 1]sync.Pool
)

func GetInt64Slice(capHint int) []int64 {
	if capHint < 0 {
		panic("GetInt64Slice: negative capHint")
	}

	if capHint > MaxNumericPooledCap {
		return make([]int64, 0, capHint)
	}
	var shift int
	if capHint <= MinNumericPooledCap {
		shift = minNumericShift
	} else {
		shift = bits.Len(uint(capHint - 1)) // ceil(log2(capHint))
	}

	lim := min(shift+maxBucketDistance, maxNumericShift)
	for sh := shift; sh <= lim; sh++ {
		if v := int64Pools[sh].Get(); v != nil {
			ptr := v.(*int64)
			n := 1 << sh
			s := unsafe.Slice(ptr, n)
			return s[:0:n]
		}
	}

	return make([]int64, 0, 1<<shift)
}

func PutInt64Slice(s []int64) {
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
	int64Pools[shift].Put(unsafe.SliceData(s[:n:n]))
}

func GetInt32Slice(capHint int) []int32 {
	if capHint < 0 {
		panic("GetInt32Slice: negative capHint")
	}

	if capHint > MaxNumericPooledCap {
		return make([]int32, 0, capHint)
	}
	var shift int
	if capHint <= MinNumericPooledCap {
		shift = minNumericShift
	} else {
		shift = bits.Len(uint(capHint - 1)) // ceil(log2(capHint))
	}

	lim := min(shift+maxBucketDistance, maxNumericShift)
	for sh := shift; sh <= lim; sh++ {
		if v := int32Pools[sh].Get(); v != nil {
			ptr := v.(*int32)
			n := 1 << sh
			s := unsafe.Slice(ptr, n)
			return s[:0:n]
		}
	}

	return make([]int32, 0, 1<<shift)
}

func PutInt32Slice(s []int32) {
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
	int32Pools[shift].Put(unsafe.SliceData(s[:n:n]))
}

func GetIntSlice(capHint int) []int {
	if capHint < 0 {
		panic("GetIntSlice: negative capHint")
	}

	if capHint > MaxNumericPooledCap {
		return make([]int, 0, capHint)
	}
	var shift int
	if capHint <= MinNumericPooledCap {
		shift = minNumericShift
	} else {
		shift = bits.Len(uint(capHint - 1)) // ceil(log2(capHint))
	}

	lim := min(shift+maxBucketDistance, maxNumericShift)
	for sh := shift; sh <= lim; sh++ {
		if v := intPools[sh].Get(); v != nil {
			ptr := v.(*int)
			n := 1 << sh
			s := unsafe.Slice(ptr, n)
			return s[:0:n]
		}
	}

	return make([]int, 0, 1<<shift)
}

func PutIntSlice(s []int) {
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
	intPools[shift].Put(unsafe.SliceData(s[:n:n]))
}
