package pools

import (
	"math/bits"
	"sync"
	"unsafe"
)

var boolPools [maxNumericShift + 1]sync.Pool

func GetBoolSlice(capHint int) []bool {
	if capHint < 0 {
		panic("GetBoolSlice: negative capHint")
	}

	if capHint > MaxNumericPooledCap {
		return make([]bool, 0, capHint)
	}
	var shift int
	if capHint <= MinNumericPooledCap {
		shift = minNumericShift
	} else {
		shift = bits.Len(uint(capHint - 1)) // ceil(log2(capHint))
	}

	lim := min(shift+maxBucketDistance, maxNumericShift)
	for sh := shift; sh <= lim; sh++ {
		if v := boolPools[sh].Get(); v != nil {
			ptr := v.(*bool)
			n := 1 << sh
			s := unsafe.Slice(ptr, n)
			return s[:0:n]
		}
	}

	return make([]bool, 0, 1<<shift)
}

func PutBoolSlice(s []bool) {
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
	boolPools[shift].Put(unsafe.SliceData(s[:n:n]))
}
