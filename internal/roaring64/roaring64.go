package roaring64

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/vapstack/rbi/internal/roaring64/roaring"
)

const (
	serialCookieNoRunContainer = 12346 // only arrays and bitmaps
	serialCookie               = 12347 // runs, arrays, and bitmaps
)

// Bitmap represents a compressed bitmap where you can add integers.
type Bitmap struct {
	highlowcontainer roaringArray64
}

// WriteTo writes a serialized version of this bitmap to stream.
// The format is compatible with other 64-bit RoaringBitmap
// implementations (Java, Go, C++) and it has a specification :
// https://github.com/RoaringBitmap/RoaringFormatSpec#extention-for-64-bit-implementations
func (rb *Bitmap) WriteTo(stream io.Writer) (int64, error) {
	var n int64
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(rb.highlowcontainer.size()))
	written, err := stream.Write(buf)
	if err != nil {
		return n, err
	}
	n += int64(written)
	pos := 0
	keyBuf := buf[:4]
	for pos < rb.highlowcontainer.size() {
		c := rb.highlowcontainer.getContainerAtIndex(pos)
		binary.LittleEndian.PutUint32(keyBuf, rb.highlowcontainer.getKeyAtIndex(pos))
		pos++
		written, err = stream.Write(keyBuf)
		n += int64(written)
		if err != nil {
			return n, err
		}
		written, err := c.WriteTo(stream)
		n += int64(written)
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// ReadFrom reads a serialized version of this bitmap from stream.
// The format is compatible with other 64-bit RoaringBitmap
// implementations (Java, Go, C++) and it has a specification :
// https://github.com/RoaringBitmap/RoaringFormatSpec#extention-for-64-bit-implementations
func (rb *Bitmap) ReadFrom(stream io.Reader) (p int64, err error) {
	sizeBuf := make([]byte, 8)
	var n int
	n, err = io.ReadFull(stream, sizeBuf)
	if err != nil {
		return int64(n), err
	}
	p += int64(n)
	size := binary.LittleEndian.Uint64(sizeBuf)
	rb.highlowcontainer.clear()
	if cap(rb.highlowcontainer.keys) >= int(size) {
		rb.highlowcontainer.keys = rb.highlowcontainer.keys[:size]
	} else {
		rb.highlowcontainer.keys = make([]uint32, size)
	}
	if cap(rb.highlowcontainer.containers) >= int(size) {
		rb.highlowcontainer.containers = rb.highlowcontainer.containers[:size]
	} else {
		rb.highlowcontainer.containers = make([]*roaring.Bitmap, size)
	}
	keyBuf := sizeBuf[:4]
	for i := uint64(0); i < size; i++ {
		n, err = io.ReadFull(stream, keyBuf)
		if err != nil {
			return int64(n), fmt.Errorf("error in bitmap.readFrom: could not read key #%d: %s", i, err)
		}
		p += int64(n)
		rb.highlowcontainer.keys[i] = binary.LittleEndian.Uint32(keyBuf)
		rb.highlowcontainer.containers[i] = roaring.NewBitmap()
		n, err := rb.highlowcontainer.containers[i].ReadFrom(stream)

		if n == 0 || err != nil {
			return int64(n), fmt.Errorf("Could not deserialize bitmap for key #%d: %s", i, err)
		}
		p += int64(n)
	}
	return p, nil
}

// RunOptimize attempts to further compress the runs of consecutive values found in the bitmap
func (rb *Bitmap) RunOptimize() {
	rb.highlowcontainer.runOptimize()
}

// NewBitmap creates a new empty Bitmap (see also New)
func NewBitmap() *Bitmap {
	return &Bitmap{}
}

// New creates a new empty Bitmap (same as NewBitmap)
func New() *Bitmap {
	return &Bitmap{}
}

// Clear resets the Bitmap to be logically empty, but may retain
// some memory allocations that may speed up future operations
func (rb *Bitmap) Clear() {
	rb.highlowcontainer.clear()
}

// ToArray creates a new slice containing all of the integers stored in the Bitmap in sorted order
func (rb *Bitmap) ToArray() []uint64 {
	array := make([]uint64, rb.GetCardinality())
	pos := 0
	pos2 := uint64(0)

	for pos < rb.highlowcontainer.size() {
		hs := uint64(rb.highlowcontainer.getKeyAtIndex(pos)) << 32
		c := rb.highlowcontainer.getContainerAtIndex(pos)
		pos++
		iter := c.ManyIterator()
		copied := iter.NextMany64(hs, array[pos2:])
		roaring.ReleaseManyIterator(iter)
		pos2 += uint64(copied)
	}
	return array
}

// GetSizeInBytes estimates the memory usage of the Bitmap. Note that this
// might differ slightly from the amount of bytes required for persistent storage
func (rb *Bitmap) GetSizeInBytes() uint64 {
	size := uint64(8)
	for _, c := range rb.highlowcontainer.containers {
		size += uint64(4) + c.GetSizeInBytes()
	}
	return size
}

// GetSerializedSizeInBytes computes the serialized size in bytes of the bitmap.
func (rb *Bitmap) GetSerializedSizeInBytes() uint64 {
	return rb.highlowcontainer.serializedSizeInBytes()
}

// Iterator creates a new IntPeekable to iterate over the integers contained in the bitmap, in sorted order;
// the iterator becomes invalid if the bitmap is modified (e.g., with Add or Remove).
func (rb *Bitmap) Iterator() IntPeekable64 {
	return newIntIterator(rb)
}

// Clone creates a copy of the Bitmap
func (rb *Bitmap) Clone() *Bitmap {
	ptr := new(Bitmap)
	ptr.highlowcontainer = *rb.highlowcontainer.clone()
	return ptr
}

// Minimum get the smallest value stored in this roaring bitmap, assumes that it is not empty
func (rb *Bitmap) Minimum() uint64 {
	return uint64(rb.highlowcontainer.containers[0].Minimum()) | (uint64(rb.highlowcontainer.keys[0]) << 32)
}

// Maximum get the largest value stored in this roaring bitmap, assumes that it is not empty
func (rb *Bitmap) Maximum() uint64 {
	lastindex := len(rb.highlowcontainer.containers) - 1
	return uint64(rb.highlowcontainer.containers[lastindex].Maximum()) | (uint64(rb.highlowcontainer.keys[lastindex]) << 32)
}

// Contains returns true if the integer is contained in the bitmap
func (rb *Bitmap) Contains(x uint64) bool {
	hb := highbits(x)
	c := rb.highlowcontainer.getContainer(hb)
	return c != nil && c.Contains(lowbits(x))
}

// Equals returns true if the two bitmaps contain the same integers
func (rb *Bitmap) Equals(srb *Bitmap) bool {
	return srb.highlowcontainer.equals(rb.highlowcontainer)
}

// Add the integer x to the bitmap
func (rb *Bitmap) Add(x uint64) {
	hb := highbits(x)
	ra := &rb.highlowcontainer
	i := ra.getIndex(hb)
	if i >= 0 {
		ra.getWritableContainerAtIndex(i).Add(lowbits(x))
	} else {
		newBitmap := roaring.NewBitmap()
		newBitmap.Add(lowbits(x))
		rb.highlowcontainer.insertNewKeyValueAt(-i-1, hb, newBitmap)
	}
}

// CheckedAdd adds x and reports whether it was newly inserted.
func (rb *Bitmap) CheckedAdd(x uint64) bool {
	hb := highbits(x)
	i := rb.highlowcontainer.getIndex(hb)
	if i >= 0 {
		c := rb.highlowcontainer.getWritableContainerAtIndex(i)
		return c.CheckedAdd(lowbits(x))
	}
	newBitmap := roaring.NewBitmap()
	newBitmap.Add(lowbits(x))
	rb.highlowcontainer.insertNewKeyValueAt(-i-1, hb, newBitmap)
	return true
}

// Remove the integer x from the bitmap
func (rb *Bitmap) Remove(x uint64) {
	hb := highbits(x)
	i := rb.highlowcontainer.getIndex(hb)
	if i >= 0 {
		c := rb.highlowcontainer.getWritableContainerAtIndex(i)
		c.Remove(lowbits(x))
		if c.IsEmpty() {
			rb.highlowcontainer.removeAtIndex(i)
		}
	}
}

// AddMany adds all values in dat.
func (rb *Bitmap) AddMany(dat []uint64) {
	if len(dat) == 0 {
		return
	}

	batchBuf := acquireAddManyBatch(len(dat))
	defer releaseAddManyBatch(batchBuf)

	start, batchHighBits := 0, highbits(dat[0])
	for end := 1; end < len(dat); end++ {
		hi := highbits(dat[end])
		if hi != batchHighBits {
			batchLen := end - start
			if cap(batchBuf.values) < batchLen {
				batchBuf.values = make([]uint32, batchLen)
			}
			batch := batchBuf.values[:batchLen]
			for i := 0; i < end-start; i++ {
				batch[i] = lowbits(dat[start+i])
			}
			rb.getOrCreateContainer(batchHighBits).AddMany(batch)
			batchHighBits = hi
			start = end
		}
	}

	batchLen := len(dat) - start
	if cap(batchBuf.values) < batchLen {
		batchBuf.values = make([]uint32, batchLen)
	}
	batch := batchBuf.values[:batchLen]
	for i := 0; i < batchLen; i++ {
		batch[i] = lowbits(dat[start+i])
	}
	rb.getOrCreateContainer(batchHighBits).AddMany(batch)
}

// BitmapOf generates a new bitmap filled with the specified integers.
func BitmapOf(dat ...uint64) *Bitmap {
	ans := NewBitmap()
	ans.AddMany(dat)
	return ans
}

// AddRange adds the integers in [rangeStart, rangeEnd) to the bitmap.
func (rb *Bitmap) AddRange(rangeStart, rangeEnd uint64) {
	if rangeStart >= rangeEnd {
		return
	}

	hbStart := uint64(highbits(rangeStart))
	lbStart := uint64(lowbits(rangeStart))
	hbLast := uint64(highbits(rangeEnd - 1))
	lbLast := uint64(lowbits(rangeEnd - 1))

	var max uint64 = maxLowBit
	for hb := hbStart; hb <= hbLast; hb++ {
		containerStart := uint64(0)
		if hb == hbStart {
			containerStart = lbStart
		}
		containerLast := max
		if hb == hbLast {
			containerLast = lbLast
		}

		rb.getOrCreateContainer(uint32(hb)).AddRange(containerStart, containerLast+1)
	}
}

func (rb *Bitmap) getOrCreateContainer(hb uint32) *roaring.Bitmap {
	ra := &rb.highlowcontainer
	i := ra.getIndex(hb)
	if i >= 0 {
		return ra.getWritableContainerAtIndex(i)
	}
	newBitmap := roaring.NewBitmap()
	ra.insertNewKeyValueAt(-i-1, hb, newBitmap)
	return newBitmap
}

// IsEmpty returns true if the Bitmap is empty (it is faster than doing (GetCardinality() == 0))
func (rb *Bitmap) IsEmpty() bool {
	return rb.highlowcontainer.size() == 0
}

// GetCardinality returns the number of integers contained in the bitmap
func (rb *Bitmap) GetCardinality() uint64 {
	size := uint64(0)
	for _, c := range rb.highlowcontainer.containers {
		size += c.GetCardinality()
	}
	return size
}

// And computes the intersection between two bitmaps and stores the result in the current bitmap
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
					c1.And(c2)
					if !c1.IsEmpty() {
						rb.highlowcontainer.replaceKeyAndContainerAtIndex(intersectionsize, s1, c1)
						if intersectionsize != pos1 {
							rb.highlowcontainer.clearContainerAtIndex(pos1)
						}
						intersectionsize++
					}
					pos1++
					pos2++
					if (pos1 == length1) || (pos2 == length2) {
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
				} else { // s1 > s2
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
	rb.highlowcontainer.releaseBitmapsInRange(intersectionsize)
	rb.highlowcontainer.resize(intersectionsize)
}

// AndCardinality returns the cardinality of the intersection between two bitmaps, bitmaps are not modified
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
					answer += c1.AndCardinality(c2)
					pos1++
					pos2++
					if (pos1 == length1) || (pos2 == length2) {
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
				} else { // s1 > s2
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

// Intersects checks whether two bitmap intersects, bitmaps are not modified
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
					if c1.Intersects(c2) {
						return true
					}
					pos1++
					pos2++
					if (pos1 == length1) || (pos2 == length2) {
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
				} else { // s1 > s2
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

// Xor computes the symmetric difference between two bitmaps and stores the result in the current bitmap
func (rb *Bitmap) Xor(x2 *Bitmap) {
	pos1 := 0
	pos2 := 0
	length1 := rb.highlowcontainer.size()
	length2 := x2.highlowcontainer.size()
	for {
		if (pos1 < length1) && (pos2 < length2) {
			s1 := rb.highlowcontainer.getKeyAtIndex(pos1)
			s2 := x2.highlowcontainer.getKeyAtIndex(pos2)
			if s1 < s2 {
				pos1 = rb.highlowcontainer.advanceUntil(s2, pos1)
				if pos1 == length1 {
					break
				}
			} else if s1 > s2 {
				c := x2.highlowcontainer.getContainerAtIndex(pos2).Clone()
				rb.highlowcontainer.insertNewKeyValueAt(pos1, x2.highlowcontainer.getKeyAtIndex(pos2), c)
				length1++
				pos1++
				pos2++
			} else {
				// TODO: couple be computed in-place for reduced memory usage
				c := roaring.Xor(rb.highlowcontainer.getContainerAtIndex(pos1), x2.highlowcontainer.getContainerAtIndex(pos2))
				if !c.IsEmpty() {
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

// Or computes the union between two bitmaps and stores the result in the current bitmap
func (rb *Bitmap) Or(x2 *Bitmap) {
	pos1 := 0
	pos2 := 0
	length1 := rb.highlowcontainer.size()
	length2 := x2.highlowcontainer.size()
main:
	for (pos1 < length1) && (pos2 < length2) {
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
				rb.highlowcontainer.insertNewKeyValueAt(pos1, s2, x2.highlowcontainer.getContainerAtIndex(pos2).Clone())
				pos1++
				length1++
				pos2++
				if pos2 == length2 {
					break main
				}
				s2 = x2.highlowcontainer.getKeyAtIndex(pos2)
			} else {
				rb.highlowcontainer.getContainerAtIndex(pos1).Or(x2.highlowcontainer.getContainerAtIndex(pos2))
				pos1++
				pos2++
				if (pos1 == length1) || (pos2 == length2) {
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

// AndNot computes the difference between two bitmaps and stores the result in the current bitmap
func (rb *Bitmap) AndNot(x2 *Bitmap) {
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
					c1.AndNot(c2)
					if !c1.IsEmpty() {
						rb.highlowcontainer.replaceKeyAndContainerAtIndex(intersectionsize, s1, c1)
						if intersectionsize != pos1 {
							rb.highlowcontainer.clearContainerAtIndex(pos1)
						}
						intersectionsize++
					}
					pos1++
					pos2++
					if (pos1 == length1) || (pos2 == length2) {
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
				} else { // s1 > s2
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
	// TODO:implement as a copy
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
	rb.highlowcontainer.releaseBitmapsInRange(intersectionsize)
	rb.highlowcontainer.resize(intersectionsize)
}
