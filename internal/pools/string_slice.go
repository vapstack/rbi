package pools

import (
	"math/bits"
	"sync"
	"unsafe"
)

const (
	minStringShift = 5
	maxStringShift = 20

	MinStringPooledCap = 1 << minStringShift
	MaxStringPooledCap = 1 << maxStringShift

	// MaxStringRetainedCap is the largest external capacity that may be
	// demoted into the largest bucket. Larger slices are dropped to avoid
	// retaining very large backing arrays through a smaller bucket.
	MaxStringRetainedCap = MaxStringPooledCap + MaxStringPooledCap/4
)

var stringPools [maxStringShift + 1]sync.Pool

func GetStringSlice(capHint int) []string {
	if capHint < 0 {
		panic("GetStringSlice: negative capHint")
	}

	if capHint > MaxStringPooledCap {
		return make([]string, 0, capHint)
	}
	var shift int
	if capHint <= MinStringPooledCap {
		shift = minStringShift
	} else {
		shift = bits.Len(uint(capHint - 1)) // ceil(log2(capHint))
	}

	lim := min(shift+3, maxStringShift)
	for sh := shift; sh <= lim; sh++ {
		if v := stringPools[sh].Get(); v != nil {
			ptr := v.(*string)
			n := 1 << sh
			s := unsafe.Slice(ptr, n)
			return s[:0:n]
		}
	}

	return make([]string, 0, 1<<shift)
}

func PutStringSlice(s []string) {
	c := cap(s)
	if c < MinStringPooledCap {
		return
	}
	var shift int
	if c >= MaxStringPooledCap {
		if c <= MaxStringRetainedCap {
			shift = maxStringShift
		} else {
			return
		}
	} else {
		shift = bits.Len(uint(c)) - 1 // floor(log2(c))
	}
	n := 1 << shift
	clear(s[:cap(s)])
	stringPools[shift].Put(unsafe.SliceData(s[:n:n]))
}
