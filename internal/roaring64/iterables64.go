package roaring64

import (
	"github.com/vapstack/rbi/internal/roaring64/roaring"
)

// IntIterable64 allows you to iterate over the values in a Bitmap
type IntIterable64 interface {
	HasNext() bool
	Next() uint64
}

// IntPeekable64 allows you to look at the next value without advancing and
// advance as long as the next value is smaller than minval
type IntPeekable64 interface {
	IntIterable64
	// PeekNext peeks the next value without advancing the iterator
	PeekNext() uint64
	// AdvanceIfNeeded advances as long as the next value is smaller than minval
	AdvanceIfNeeded(minval uint64)
}

type intIterator struct {
	pos              int
	hs               uint64
	iter             roaring.IntPeekable
	highlowcontainer *roaringArray64
}

// HasNext returns true if there are more integers to iterate over
func (ii *intIterator) HasNext() bool {
	return ii.pos < ii.highlowcontainer.size()
}

func (ii *intIterator) init() {
	if ii.iter != nil {
		roaring.ReleaseIterator(ii.iter)
		ii.iter = nil
	}
	if ii.highlowcontainer.size() > ii.pos {
		ii.iter = ii.highlowcontainer.getContainerAtIndex(ii.pos).Iterator()
		ii.hs = uint64(ii.highlowcontainer.getKeyAtIndex(ii.pos)) << 32
	}
}

// Next returns the next integer
func (ii *intIterator) Next() uint64 {
	lowbits := ii.iter.Next()
	x := uint64(lowbits) | ii.hs
	if !ii.iter.HasNext() {
		ii.pos = ii.pos + 1
		ii.init()
	}
	return x
}

// PeekNext peeks the next value without advancing the iterator
func (ii *intIterator) PeekNext() uint64 {
	return uint64(ii.iter.PeekNext()&maxLowBit) | ii.hs
}

// AdvanceIfNeeded advances as long as the next value is smaller than minval
func (ii *intIterator) AdvanceIfNeeded(minval uint64) {
	to := minval >> 32

	for ii.HasNext() && (ii.hs>>32) < to {
		ii.pos++
		ii.init()
	}

	if ii.HasNext() && (ii.hs>>32) == to {
		ii.iter.AdvanceIfNeeded(lowbits(minval))

		if !ii.iter.HasNext() {
			ii.pos++
			ii.init()
		}
	}
}

func (ii *intIterator) Initialize(a *Bitmap) {
	ii.pos = 0
	ii.hs = 0
	ii.iter = nil
	ii.highlowcontainer = &a.highlowcontainer
	ii.init()
}

func (ii *intIterator) Release() {
	releaseIntIterator64(ii)
}

func newIntIterator(a *Bitmap) *intIterator {
	return acquireIntIterator64(a)
}
