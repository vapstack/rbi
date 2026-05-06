package pools

import (
	"math/bits"
	"sync"
	"unsafe"
)

var (
	float32Pools [maxNumericShift + 1]sync.Pool
	float64Pools [maxNumericShift + 1]sync.Pool
)

func GetFloat64Slice(capHint int) []float64 {
	if capHint < 0 {
		panic("GetFloat64Slice: negative capHint")
	}

	if capHint > MaxNumericPooledCap {
		return make([]float64, 0, capHint)
	}
	var shift int
	if capHint <= MinNumericPooledCap {
		shift = minNumericShift
	} else {
		shift = bits.Len(uint(capHint - 1)) // ceil(log2(capHint))
	}

	lim := min(shift+3, maxNumericShift)
	for sh := shift; sh <= lim; sh++ {
		if v := float64Pools[sh].Get(); v != nil {
			ptr := v.(*float64)
			n := 1 << sh
			s := unsafe.Slice(ptr, n)
			return s[:0:n]
		}
	}

	return make([]float64, 0, 1<<shift)
}

func PutFloat64Slice(s []float64) {
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
	float64Pools[shift].Put(unsafe.SliceData(s[:n:n]))
}

func GetFloat32Slice(capHint int) []float32 {
	if capHint < 0 {
		panic("GetFloat32Slice: negative capHint")
	}

	if capHint > MaxNumericPooledCap {
		return make([]float32, 0, capHint)
	}
	var shift int
	if capHint <= MinNumericPooledCap {
		shift = minNumericShift
	} else {
		shift = bits.Len(uint(capHint - 1)) // ceil(log2(capHint))
	}

	lim := min(shift+3, maxNumericShift)
	for sh := shift; sh <= lim; sh++ {
		if v := float32Pools[sh].Get(); v != nil {
			ptr := v.(*float32)
			n := 1 << sh
			s := unsafe.Slice(ptr, n)
			return s[:0:n]
		}
	}

	return make([]float32, 0, 1<<shift)
}

func PutFloat32Slice(s []float32) {
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
	float32Pools[shift].Put(unsafe.SliceData(s[:n:n]))
}
