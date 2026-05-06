package pools

import (
	"math/bits"
	"sync"
	"unsafe"

	"github.com/vapstack/rbi/internal/posting"
)

const (
	minPostingShift = 5
	maxPostingShift = 16

	MinPostingPooledCap = 1 << minPostingShift
	MaxPostingPooledCap = 1 << maxPostingShift

	// MaxPostingRetainedCap is the largest external capacity that may be
	// demoted into the largest bucket. Larger slices are dropped to avoid
	// retaining very large backing arrays through a smaller bucket.
	MaxPostingRetainedCap = MaxPostingPooledCap + MaxPostingPooledCap/4
)

var postingPools [maxStringShift + 1]sync.Pool

func GetPostingSlice(capHint int) []posting.List {
	if capHint < 0 {
		panic("GetPostingSlice: negative capHint")
	}

	if capHint > MaxPostingPooledCap {
		return make([]posting.List, 0, capHint)
	}
	var shift int
	if capHint <= MinPostingPooledCap {
		shift = minPostingShift
	} else {
		shift = bits.Len(uint(capHint - 1)) // ceil(log2(capHint))
	}

	lim := min(shift+maxBucketDistance, maxPostingShift)
	for sh := shift; sh <= lim; sh++ {
		if v := postingPools[sh].Get(); v != nil {
			ptr := v.(*posting.List)
			n := 1 << sh
			s := unsafe.Slice(ptr, n)
			return s[:0:n]
		}
	}

	return make([]posting.List, 0, 1<<shift)
}

func PutPostingSliceWithRelease(s []posting.List) {
	for _, p := range s {
		p.Release()
	}
	PutPostingSlice(s)
}

func PutPostingSlice(s []posting.List) {
	c := cap(s)
	if c < MinPostingPooledCap {
		return
	}
	var shift int
	if c >= MaxPostingPooledCap {
		if c <= MaxPostingRetainedCap {
			shift = maxPostingShift
		} else {
			return
		}
	} else {
		shift = bits.Len(uint(c)) - 1 // floor(log2(c))
	}
	clear(s[:c])
	n := 1 << shift
	postingPools[shift].Put(unsafe.SliceData(s[:n:n]))
}
