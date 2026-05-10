package pools

import (
	"math/bits"
	"sync"
	"unsafe"
)

var bytePools [maxNumericShift + 1]sync.Pool

func GetByteSlice(capHint int) []byte {
	if capHint < 0 {
		panic("GetByteSlice: negative capHint")
	}

	if capHint > MaxNumericPooledCap {
		return make([]byte, 0, capHint)
	}
	var shift int
	if capHint <= MinNumericPooledCap {
		shift = minNumericShift
	} else {
		shift = bits.Len(uint(capHint - 1))
	}

	lim := min(shift+maxBucketDistance, maxNumericShift)
	for sh := shift; sh <= lim; sh++ {
		if v := bytePools[sh].Get(); v != nil {
			ptr := v.(*byte)
			n := 1 << sh
			s := unsafe.Slice(ptr, n)
			return s[:0:n]
		}
	}

	return make([]byte, 0, 1<<shift)
}

func PutByteSlice(s []byte) {
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
		shift = bits.Len(uint(c)) - 1
	}
	n := 1 << shift
	bytePools[shift].Put(unsafe.SliceData(s[:n:n]))
}
