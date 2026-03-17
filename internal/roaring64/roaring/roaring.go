package roaring

import "io"

// Bitmap represents a compressed 32-bit bitmap used internally by the local roaring64 fork.
type Bitmap struct {
	highlowcontainer roaringArray
}

// WriteTo writes a serialized version of this bitmap to stream.
func (rb *Bitmap) WriteTo(stream io.Writer) (int64, error) {
	return rb.highlowcontainer.writeTo(stream)
}

// ReadFrom reads a serialized version of this bitmap from stream.
func (rb *Bitmap) ReadFrom(reader io.Reader, cookieHeader ...byte) (p int64, err error) {
	stream, ok := reader.(ByteInput)
	if !ok {
		byteInputAdapter := ByteInputAdapterPool.Get().(*ByteInputAdapter)
		byteInputAdapter.Reset(reader)
		stream = byteInputAdapter
	}

	p, err = rb.highlowcontainer.readFrom(stream, cookieHeader...)

	if !ok {
		ByteInputAdapterPool.Put(stream.(*ByteInputAdapter))
	}
	return
}

// RunOptimize attempts to further compress the runs of consecutive values found in the bitmap.
func (rb *Bitmap) RunOptimize() {
	rb.highlowcontainer.runOptimize()
}

// NewBitmap creates a new empty Bitmap.
func NewBitmap() *Bitmap {
	return &Bitmap{}
}

// New creates a new empty Bitmap.
func New() *Bitmap {
	return &Bitmap{}
}

// Clear resets the Bitmap to be logically empty.
func (rb *Bitmap) Clear() {
	rb.highlowcontainer.clear()
}

// GetSizeInBytes estimates the memory usage of the Bitmap.
func (rb *Bitmap) GetSizeInBytes() uint64 {
	size := uint64(8)
	for _, c := range rb.highlowcontainer.containers {
		size += uint64(2) + uint64(c.getSizeInBytes())
	}
	return size
}

// GetSerializedSizeInBytes computes the serialized size in bytes of the bitmap.
func (rb *Bitmap) GetSerializedSizeInBytes() uint64 {
	return rb.highlowcontainer.serializedSizeInBytes()
}

// IntIterable allows you to iterate over the values in a Bitmap.
type IntIterable interface {
	HasNext() bool
	Next() uint32
}

// IntPeekable allows you to peek the next value without advancing.
type IntPeekable interface {
	IntIterable
	PeekNext() uint32
	AdvanceIfNeeded(minval uint32)
}

type intIterator struct {
	pos              int
	hs               uint32
	iter             shortPeekable
	highlowcontainer *roaringArray

	shortIter  shortIterator
	runIter    runIterator16
	bitmapIter bitmapContainerShortIterator
}

// HasNext returns true if there are more integers to iterate over.
func (ii *intIterator) HasNext() bool {
	return ii.pos < ii.highlowcontainer.size()
}

func (ii *intIterator) init() {
	if ii.highlowcontainer.size() > ii.pos {
		ii.hs = uint32(ii.highlowcontainer.getKeyAtIndex(ii.pos)) << 16
		c := ii.highlowcontainer.getContainerAtIndex(ii.pos)
		switch t := c.(type) {
		case *arrayContainer:
			ii.shortIter = shortIterator{t.content, 0}
			ii.iter = &ii.shortIter
		case *runContainer16:
			ii.runIter = runIterator16{rc: t, curIndex: 0, curPosInIndex: 0}
			ii.iter = &ii.runIter
		case *bitmapContainer:
			ii.bitmapIter = bitmapContainerShortIterator{t, t.NextSetBit(0)}
			ii.iter = &ii.bitmapIter
		}
		return
	}
	ii.iter = nil
}

// Next returns the next integer.
func (ii *intIterator) Next() uint32 {
	x := uint32(ii.iter.next()) | ii.hs
	if !ii.iter.hasNext() {
		ii.pos++
		ii.init()
	}
	return x
}

// PeekNext peeks the next value without advancing the iterator.
func (ii *intIterator) PeekNext() uint32 {
	return uint32(ii.iter.peekNext()&maxLowBit) | ii.hs
}

// AdvanceIfNeeded advances as long as the next value is smaller than minval.
func (ii *intIterator) AdvanceIfNeeded(minval uint32) {
	to := minval & 0xffff0000

	for ii.HasNext() && ii.hs < to {
		ii.pos++
		ii.init()
	}

	if ii.HasNext() && ii.hs == to {
		ii.iter.advanceIfNeeded(lowbits(minval))

		if !ii.iter.hasNext() {
			ii.pos++
			ii.init()
		}
	}
}

func (ii *intIterator) Initialize(a *Bitmap) {
	ii.pos = 0
	ii.highlowcontainer = &a.highlowcontainer
	ii.init()
}

func (ii *intIterator) Release() {
	releaseIntIterator(ii)
}

// ManyIntIterable allows bulk iteration over values in a Bitmap.
type ManyIntIterable interface {
	NextMany(buf []uint32) int
	NextMany64(hs uint64, buf []uint64) int
}

type manyIntIterator struct {
	pos              int
	hs               uint32
	iter             manyIterable
	highlowcontainer *roaringArray

	shortIter  shortIterator
	runIter    runIterator16
	bitmapIter bitmapContainerManyIterator
}

func (ii *manyIntIterator) init() {
	if ii.highlowcontainer.size() > ii.pos {
		ii.hs = uint32(ii.highlowcontainer.getKeyAtIndex(ii.pos)) << 16
		c := ii.highlowcontainer.getContainerAtIndex(ii.pos)
		switch t := c.(type) {
		case *arrayContainer:
			ii.shortIter = shortIterator{t.content, 0}
			ii.iter = &ii.shortIter
		case *runContainer16:
			ii.runIter = runIterator16{rc: t, curIndex: 0, curPosInIndex: 0}
			ii.iter = &ii.runIter
		case *bitmapContainer:
			ii.bitmapIter = bitmapContainerManyIterator{t, -1, 0}
			ii.iter = &ii.bitmapIter
		}
		return
	}
	ii.iter = nil
}

func (ii *manyIntIterator) NextMany(buf []uint32) int {
	n := 0
	for n < len(buf) {
		if ii.iter == nil {
			break
		}
		moreN := ii.iter.nextMany(ii.hs, buf[n:])
		n += moreN
		if moreN == 0 {
			ii.pos++
			ii.init()
		}
	}
	return n
}

func (ii *manyIntIterator) NextMany64(hs64 uint64, buf []uint64) int {
	n := 0
	for n < len(buf) {
		if ii.iter == nil {
			break
		}

		hs := uint64(ii.hs) | hs64
		moreN := ii.iter.nextMany64(hs, buf[n:])
		n += moreN
		if moreN == 0 {
			ii.pos++
			ii.init()
		}
	}
	return n
}

func (ii *manyIntIterator) Initialize(a *Bitmap) {
	ii.pos = 0
	ii.highlowcontainer = &a.highlowcontainer
	ii.init()
}

func (ii *manyIntIterator) Release() {
	releaseManyIntIterator(ii)
}

// Iterator creates a new IntPeekable to iterate over the integers contained in the bitmap.
func (rb *Bitmap) Iterator() IntPeekable {
	return acquireIntIterator(rb)
}

// ManyIterator creates a new ManyIntIterable for bulk iteration.
func (rb *Bitmap) ManyIterator() ManyIntIterable {
	return acquireManyIntIterator(rb)
}

// Clone creates a copy of the Bitmap.
func (rb *Bitmap) Clone() *Bitmap {
	ptr := new(Bitmap)
	ptr.highlowcontainer = *rb.highlowcontainer.clone()
	return ptr
}

// Minimum gets the smallest value stored in this bitmap.
func (rb *Bitmap) Minimum() uint32 {
	if len(rb.highlowcontainer.containers) == 0 {
		panic("empty bitmap")
	}
	return uint32(rb.highlowcontainer.containers[0].minimum()) | (uint32(rb.highlowcontainer.keys[0]) << 16)
}

// Maximum gets the largest value stored in this bitmap.
func (rb *Bitmap) Maximum() uint32 {
	if len(rb.highlowcontainer.containers) == 0 {
		panic("empty bitmap")
	}
	lastindex := len(rb.highlowcontainer.containers) - 1
	return uint32(rb.highlowcontainer.containers[lastindex].maximum()) | (uint32(rb.highlowcontainer.keys[lastindex]) << 16)
}

// Contains returns true if the integer is contained in the bitmap.
func (rb *Bitmap) Contains(x uint32) bool {
	hb := highbits(x)
	c := rb.highlowcontainer.getContainer(hb)
	return c != nil && c.contains(lowbits(x))
}

// Equals returns true if the two bitmaps contain the same integers.
func (rb *Bitmap) Equals(other *Bitmap) bool {
	return other != nil && other.highlowcontainer.equals(rb.highlowcontainer)
}

// Add the integer x to the bitmap.
func (rb *Bitmap) Add(x uint32) {
	hb := highbits(x)
	ra := &rb.highlowcontainer
	i := ra.getIndex(hb)
	if i >= 0 {
		c := ra.getWritableContainerAtIndex(i).iaddReturnMinimized(lowbits(x))
		rb.highlowcontainer.setContainerAtIndex(i, c)
	} else {
		newac := newArrayContainer()
		rb.highlowcontainer.insertNewKeyValueAt(-i-1, hb, newac.iaddReturnMinimized(lowbits(x)))
	}
}

func (rb *Bitmap) addwithptr(x uint32) (int, container) {
	hb := highbits(x)
	ra := &rb.highlowcontainer
	i := ra.getIndex(hb)
	if i >= 0 {
		c := ra.getWritableContainerAtIndex(i).iaddReturnMinimized(lowbits(x))
		rb.highlowcontainer.setContainerAtIndex(i, c)
		return i, c
	}
	newac := newArrayContainer()
	c := newac.iaddReturnMinimized(lowbits(x))
	rb.highlowcontainer.insertNewKeyValueAt(-i-1, hb, c)
	return -i - 1, c
}

// CheckedAdd adds x and reports whether it was newly inserted.
func (rb *Bitmap) CheckedAdd(x uint32) bool {
	hb := highbits(x)
	i := rb.highlowcontainer.getIndex(hb)
	if i >= 0 {
		c := rb.highlowcontainer.getWritableContainerAtIndex(i)
		oldcard := c.getCardinality()
		c = c.iaddReturnMinimized(lowbits(x))
		rb.highlowcontainer.setContainerAtIndex(i, c)
		return c.getCardinality() > oldcard
	}
	newac := newArrayContainer()
	rb.highlowcontainer.insertNewKeyValueAt(-i-1, hb, newac.iaddReturnMinimized(lowbits(x)))
	return true
}

// Remove the integer x from the bitmap.
func (rb *Bitmap) Remove(x uint32) {
	hb := highbits(x)
	i := rb.highlowcontainer.getIndex(hb)
	if i >= 0 {
		c := rb.highlowcontainer.getWritableContainerAtIndex(i).iremoveReturnMinimized(lowbits(x))
		rb.highlowcontainer.setContainerAtIndex(i, c)
		if rb.highlowcontainer.getContainerAtIndex(i).isEmpty() {
			rb.highlowcontainer.removeAtIndex(i)
		}
	}
}

// AddMany adds all values in dat.
func (rb *Bitmap) AddMany(dat []uint32) {
	if len(dat) == 0 {
		return
	}

	prev := dat[0]
	idx, c := rb.addwithptr(prev)
	for _, v := range dat[1:] {
		if highbits(prev) == highbits(v) {
			c = c.iaddReturnMinimized(lowbits(v))
			rb.highlowcontainer.setContainerAtIndex(idx, c)
		} else {
			idx, c = rb.addwithptr(v)
		}
		prev = v
	}
}

// AddRange adds the integers in [rangeStart, rangeEnd) to the bitmap.
func (rb *Bitmap) AddRange(rangeStart, rangeEnd uint64) {
	if rangeStart >= rangeEnd {
		return
	}
	if rangeEnd-1 > MaxUint32 {
		panic("rangeEnd-1 > MaxUint32")
	}

	hbStart := uint32(highbits(uint32(rangeStart)))
	lbStart := uint32(lowbits(uint32(rangeStart)))
	hbLast := uint32(highbits(uint32(rangeEnd - 1)))
	lbLast := uint32(lowbits(uint32(rangeEnd - 1)))

	var max uint32 = maxLowBit
	for hb := hbStart; hb <= hbLast; hb++ {
		containerStart := uint32(0)
		if hb == hbStart {
			containerStart = lbStart
		}
		containerLast := max
		if hb == hbLast {
			containerLast = lbLast
		}

		i := rb.highlowcontainer.getIndex(uint16(hb))
		if i >= 0 {
			c := rb.highlowcontainer.getWritableContainerAtIndex(i).iaddRange(int(containerStart), int(containerLast)+1)
			rb.highlowcontainer.setContainerAtIndex(i, c)
		} else {
			rb.highlowcontainer.insertNewKeyValueAt(-i-1, uint16(hb), rangeOfOnes(int(containerStart), int(containerLast)))
		}
	}
}

// IsEmpty returns true if the Bitmap is empty.
func (rb *Bitmap) IsEmpty() bool {
	return rb.highlowcontainer.size() == 0
}

// GetCardinality returns the number of integers contained in the bitmap.
func (rb *Bitmap) GetCardinality() uint64 {
	size := uint64(0)
	for _, c := range rb.highlowcontainer.containers {
		size += uint64(c.getCardinality())
	}
	return size
}

// And computes the intersection between two bitmaps and stores the result in the current bitmap.
func (rb *Bitmap) And(x2 *Bitmap) {
	pos1 := 0
	pos2 := 0
	intersectionsize := 0
	length1 := rb.highlowcontainer.size()
	length2 := x2.highlowcontainer.size()

main:
	for {
		if pos1 < length1 && pos2 < length2 {
			s1 := rb.highlowcontainer.getKeyAtIndex(pos1)
			s2 := x2.highlowcontainer.getKeyAtIndex(pos2)
			for {
				if s1 == s2 {
					c1 := rb.highlowcontainer.getWritableContainerAtIndex(pos1)
					c2 := x2.highlowcontainer.getContainerAtIndex(pos2)
					diff := c1.iand(c2)
					if !diff.isEmpty() {
						rb.highlowcontainer.replaceKeyAndContainerAtIndex(intersectionsize, s1, diff)
						if intersectionsize != pos1 && diff == c1 {
							rb.highlowcontainer.clearContainerAtIndex(pos1)
						}
						intersectionsize++
					}
					pos1++
					pos2++
					if pos1 == length1 || pos2 == length2 {
						break main
					}
					s1 = rb.highlowcontainer.getKeyAtIndex(pos1)
					s2 = x2.highlowcontainer.getKeyAtIndex(pos2)
				} else if s1 < s2 {
					pos1 = rb.highlowcontainer.advanceUntil(s2, pos1)
					if pos1 == length1 {
						break main
					}
					s1 = rb.highlowcontainer.getKeyAtIndex(pos1)
				} else {
					pos2 = x2.highlowcontainer.advanceUntil(s1, pos2)
					if pos2 == length2 {
						break main
					}
					s2 = x2.highlowcontainer.getKeyAtIndex(pos2)
				}
			}
		} else {
			break
		}
	}
	rb.highlowcontainer.releaseContainersInRange(intersectionsize)
	rb.highlowcontainer.resize(intersectionsize)
}

// AndCardinality returns the cardinality of the intersection between two bitmaps.
func (rb *Bitmap) AndCardinality(x2 *Bitmap) uint64 {
	pos1 := 0
	pos2 := 0
	answer := uint64(0)
	length1 := rb.highlowcontainer.size()
	length2 := x2.highlowcontainer.size()

main:
	for {
		if pos1 < length1 && pos2 < length2 {
			s1 := rb.highlowcontainer.getKeyAtIndex(pos1)
			s2 := x2.highlowcontainer.getKeyAtIndex(pos2)
			for {
				if s1 == s2 {
					c1 := rb.highlowcontainer.getContainerAtIndex(pos1)
					c2 := x2.highlowcontainer.getContainerAtIndex(pos2)
					answer += uint64(c1.andCardinality(c2))
					pos1++
					pos2++
					if pos1 == length1 || pos2 == length2 {
						break main
					}
					s1 = rb.highlowcontainer.getKeyAtIndex(pos1)
					s2 = x2.highlowcontainer.getKeyAtIndex(pos2)
				} else if s1 < s2 {
					pos1 = rb.highlowcontainer.advanceUntil(s2, pos1)
					if pos1 == length1 {
						break main
					}
					s1 = rb.highlowcontainer.getKeyAtIndex(pos1)
				} else {
					pos2 = x2.highlowcontainer.advanceUntil(s1, pos2)
					if pos2 == length2 {
						break main
					}
					s2 = x2.highlowcontainer.getKeyAtIndex(pos2)
				}
			}
		} else {
			break
		}
	}
	return answer
}

// Intersects checks whether two bitmaps intersect.
func (rb *Bitmap) Intersects(x2 *Bitmap) bool {
	pos1 := 0
	pos2 := 0
	length1 := rb.highlowcontainer.size()
	length2 := x2.highlowcontainer.size()

main:
	for {
		if pos1 < length1 && pos2 < length2 {
			s1 := rb.highlowcontainer.getKeyAtIndex(pos1)
			s2 := x2.highlowcontainer.getKeyAtIndex(pos2)
			for {
				if s1 == s2 {
					c1 := rb.highlowcontainer.getContainerAtIndex(pos1)
					c2 := x2.highlowcontainer.getContainerAtIndex(pos2)
					if c1.intersects(c2) {
						return true
					}
					pos1++
					pos2++
					if pos1 == length1 || pos2 == length2 {
						break main
					}
					s1 = rb.highlowcontainer.getKeyAtIndex(pos1)
					s2 = x2.highlowcontainer.getKeyAtIndex(pos2)
				} else if s1 < s2 {
					pos1 = rb.highlowcontainer.advanceUntil(s2, pos1)
					if pos1 == length1 {
						break main
					}
					s1 = rb.highlowcontainer.getKeyAtIndex(pos1)
				} else {
					pos2 = x2.highlowcontainer.advanceUntil(s1, pos2)
					if pos2 == length2 {
						break main
					}
					s2 = x2.highlowcontainer.getKeyAtIndex(pos2)
				}
			}
		} else {
			break
		}
	}
	return false
}

// Xor computes the symmetric difference between two bitmaps and stores the result in the current bitmap.
func (rb *Bitmap) Xor(x2 *Bitmap) {
	if rb == x2 {
		rb.Clear()
		return
	}
	pos1 := 0
	pos2 := 0
	length1 := rb.highlowcontainer.size()
	length2 := x2.highlowcontainer.size()
	for {
		if pos1 < length1 && pos2 < length2 {
			s1 := rb.highlowcontainer.getKeyAtIndex(pos1)
			s2 := x2.highlowcontainer.getKeyAtIndex(pos2)
			if s1 < s2 {
				pos1 = rb.highlowcontainer.advanceUntil(s2, pos1)
				if pos1 == length1 {
					break
				}
			} else if s1 > s2 {
				rb.highlowcontainer.insertNewKeyValueAt(pos1, x2.highlowcontainer.getKeyAtIndex(pos2), x2.highlowcontainer.getContainerAtIndex(pos2).clone())
				length1++
				pos1++
				pos2++
			} else {
				c := rb.highlowcontainer.getContainerAtIndex(pos1).xor(x2.highlowcontainer.getContainerAtIndex(pos2))
				if !c.isEmpty() {
					rb.highlowcontainer.setContainerAtIndex(pos1, c)
					pos1++
				} else {
					rb.highlowcontainer.removeAtIndex(pos1)
					length1--
				}
				pos2++
			}
		} else {
			break
		}
	}
	if pos1 == length1 {
		rb.highlowcontainer.appendCopyMany(x2.highlowcontainer, pos2, length2)
	}
}

// Or computes the union between two bitmaps and stores the result in the current bitmap.
func (rb *Bitmap) Or(x2 *Bitmap) {
	pos1 := 0
	pos2 := 0
	length1 := rb.highlowcontainer.size()
	length2 := x2.highlowcontainer.size()
main:
	for pos1 < length1 && pos2 < length2 {
		s1 := rb.highlowcontainer.getKeyAtIndex(pos1)
		s2 := x2.highlowcontainer.getKeyAtIndex(pos2)

		for {
			if s1 < s2 {
				pos1++
				if pos1 == length1 {
					break main
				}
				s1 = rb.highlowcontainer.getKeyAtIndex(pos1)
			} else if s1 > s2 {
				rb.highlowcontainer.insertNewKeyValueAt(pos1, s2, x2.highlowcontainer.getContainerAtIndex(pos2).clone())
				pos1++
				length1++
				pos2++
				if pos2 == length2 {
					break main
				}
				s2 = x2.highlowcontainer.getKeyAtIndex(pos2)
			} else {
				newcont := rb.highlowcontainer.getUnionedWritableContainer(pos1, x2.highlowcontainer.getContainerAtIndex(pos2))
				rb.highlowcontainer.replaceKeyAndContainerAtIndex(pos1, s1, newcont)
				pos1++
				pos2++
				if pos1 == length1 || pos2 == length2 {
					break main
				}
				s1 = rb.highlowcontainer.getKeyAtIndex(pos1)
				s2 = x2.highlowcontainer.getKeyAtIndex(pos2)
			}
		}
	}
	if pos1 == length1 {
		rb.highlowcontainer.appendCopyMany(x2.highlowcontainer, pos2, length2)
	}
}

// AndNot computes the difference between two bitmaps and stores the result in the current bitmap.
func (rb *Bitmap) AndNot(x2 *Bitmap) {
	if rb == x2 {
		rb.Clear()
		return
	}
	pos1 := 0
	pos2 := 0
	intersectionsize := 0
	length1 := rb.highlowcontainer.size()
	length2 := x2.highlowcontainer.size()

main:
	for {
		if pos1 < length1 && pos2 < length2 {
			s1 := rb.highlowcontainer.getKeyAtIndex(pos1)
			s2 := x2.highlowcontainer.getKeyAtIndex(pos2)
			for {
				if s1 == s2 {
					c1 := rb.highlowcontainer.getWritableContainerAtIndex(pos1)
					c2 := x2.highlowcontainer.getContainerAtIndex(pos2)
					diff := c1.iandNot(c2)
					if !diff.isEmpty() {
						rb.highlowcontainer.replaceKeyAndContainerAtIndex(intersectionsize, s1, diff)
						if intersectionsize != pos1 && diff == c1 {
							rb.highlowcontainer.clearContainerAtIndex(pos1)
						}
						intersectionsize++
					}
					pos1++
					pos2++
					if pos1 == length1 || pos2 == length2 {
						break main
					}
					s1 = rb.highlowcontainer.getKeyAtIndex(pos1)
					s2 = x2.highlowcontainer.getKeyAtIndex(pos2)
				} else if s1 < s2 {
					c1 := rb.highlowcontainer.getContainerAtIndex(pos1)
					rb.highlowcontainer.replaceKeyAndContainerAtIndex(intersectionsize, s1, c1)
					if intersectionsize != pos1 {
						rb.highlowcontainer.clearContainerAtIndex(pos1)
					}
					intersectionsize++
					pos1++
					if pos1 == length1 {
						break main
					}
					s1 = rb.highlowcontainer.getKeyAtIndex(pos1)
				} else {
					pos2 = x2.highlowcontainer.advanceUntil(s1, pos2)
					if pos2 == length2 {
						break main
					}
					s2 = x2.highlowcontainer.getKeyAtIndex(pos2)
				}
			}
		} else {
			break
		}
	}
	for pos1 < length1 {
		c1 := rb.highlowcontainer.getContainerAtIndex(pos1)
		s1 := rb.highlowcontainer.getKeyAtIndex(pos1)
		rb.highlowcontainer.replaceKeyAndContainerAtIndex(intersectionsize, s1, c1)
		if intersectionsize != pos1 {
			rb.highlowcontainer.clearContainerAtIndex(pos1)
		}
		intersectionsize++
		pos1++
	}
	rb.highlowcontainer.releaseContainersInRange(intersectionsize)
	rb.highlowcontainer.resize(intersectionsize)
}

// Xor computes the symmetric difference between two bitmaps and returns the result.
func Xor(x1, x2 *Bitmap) *Bitmap {
	answer := x1.Clone()
	answer.Xor(x2)
	return answer
}
