package posting

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"
)

// bitmap32 is the internal 32-bit container index used by large-mode posting.
type bitmap32 struct {
	refs             atomic.Int32
	highlowcontainer containerIndex
}

// runOptimize attempts to further compress the runs of consecutive values found in the bitmap.
func (rb *bitmap32) runOptimize() {
	rb.highlowcontainer.runOptimize()
}

// newBitmap32 creates a new empty bitmap32.
func newBitmap32() *bitmap32 {
	return acquireBitmap()
}

// clear resets the bitmap32 to be logically empty.
func (rb *bitmap32) clear() {
	rb.highlowcontainer.clear()
}

// sizeInBytes estimates the memory usage of the bitmap32.
func (rb *bitmap32) sizeInBytes() uint64 {
	size := uint64(8)
	for _, c := range rb.highlowcontainer.containers {
		size += uint64(2) + uint64(c.getSizeInBytes())
	}
	return size
}

// intIterable iterates over the values in a bitmap32.
type intIterable interface {
	hasNext() bool
	next() uint32
}

// intPeekable allows peeking without advancing.
type intPeekable interface {
	intIterable
	peekNext() uint32
	advanceIfNeeded(minval uint32)
}

type intIterator struct {
	pos              int
	hs               uint32
	iter             shortPeekable
	highlowcontainer *containerIndex

	shortIter  shortIterator
	runIter    runIterator16
	bitmapIter bitmapContainerShortIterator
}

// hasNext returns true if there are more integers to iterate over.
func (ii *intIterator) hasNext() bool {
	return ii.pos < ii.highlowcontainer.size()
}

func (ii *intIterator) init() {
	if ii.highlowcontainer.size() > ii.pos {
		ii.hs = uint32(ii.highlowcontainer.getKeyAtIndex(ii.pos)) << 16
		c := ii.highlowcontainer.getContainerAtIndex(ii.pos)
		switch t := c.(type) {
		case *containerArray:
			ii.shortIter = shortIterator{t.content, 0}
			ii.iter = &ii.shortIter
		case *containerRun:
			ii.runIter = runIterator16{rc: t, curIndex: 0, curPosInIndex: 0}
			ii.iter = &ii.runIter
		case *containerBitmap:
			ii.bitmapIter = bitmapContainerShortIterator{t, t.nextSetBit(0)}
			ii.iter = &ii.bitmapIter
		}
		return
	}
	ii.iter = nil
}

// next returns the next integer.
func (ii *intIterator) next() uint32 {
	x := uint32(ii.iter.next()) | ii.hs
	if !ii.iter.hasNext() {
		ii.pos++
		ii.init()
	}
	return x
}

// peekNext returns the next value without advancing the iterator.
func (ii *intIterator) peekNext() uint32 {
	return uint32(ii.iter.peekNext()&maxLowBit) | ii.hs
}

// advanceIfNeeded skips values smaller than minval.
func (ii *intIterator) advanceIfNeeded(minval uint32) {
	to := minval & 0xffff0000

	for ii.hasNext() && ii.hs < to {
		ii.pos++
		ii.init()
	}

	if ii.hasNext() && ii.hs == to {
		ii.iter.advanceIfNeeded(lowbits(minval))

		if !ii.iter.hasNext() {
			ii.pos++
			ii.init()
		}
	}
}

func (ii *intIterator) initialize(a *bitmap32) {
	ii.pos = 0
	ii.highlowcontainer = &a.highlowcontainer
	ii.init()
}

func (ii *intIterator) release() {
	releaseIntIterator(ii)
}

// manyIntIterable supports bulk iteration over values in a bitmap32.
type manyIntIterable interface {
	nextMany(buf []uint32) int
	nextMany64(hs uint64, buf []uint64) int
}

type manyIntIterator struct {
	pos              int
	hs               uint32
	iter             manyIterable
	highlowcontainer *containerIndex

	shortIter  shortIterator
	runIter    runIterator16
	bitmapIter bitmapContainerManyIterator
}

func (ii *manyIntIterator) init() {
	if ii.highlowcontainer.size() > ii.pos {
		ii.hs = uint32(ii.highlowcontainer.getKeyAtIndex(ii.pos)) << 16
		c := ii.highlowcontainer.getContainerAtIndex(ii.pos)
		switch t := c.(type) {
		case *containerArray:
			ii.shortIter = shortIterator{t.content, 0}
			ii.iter = &ii.shortIter
		case *containerRun:
			ii.runIter = runIterator16{rc: t, curIndex: 0, curPosInIndex: 0}
			ii.iter = &ii.runIter
		case *containerBitmap:
			ii.bitmapIter = bitmapContainerManyIterator{t, -1, 0}
			ii.iter = &ii.bitmapIter
		}
		return
	}
	ii.iter = nil
}

func (ii *manyIntIterator) nextMany(buf []uint32) int {
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

func (ii *manyIntIterator) nextMany64(hs64 uint64, buf []uint64) int {
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

func (ii *manyIntIterator) initialize(a *bitmap32) {
	ii.pos = 0
	ii.highlowcontainer = &a.highlowcontainer
	ii.init()
}

func (ii *manyIntIterator) release() {
	releaseManyIntIterator(ii)
}

// iterator creates a new intPeekable to iterate over the integers contained in the bitmap.
func (rb *bitmap32) iterator() intPeekable {
	return acquireIntIterator(rb)
}

// manyIterator creates a new manyIntIterable for bulk iteration.
func (rb *bitmap32) manyIterator() manyIntIterable {
	return acquireManyIntIterator(rb)
}

// clone creates a copy of the bitmap32.
func (rb *bitmap32) clone() *bitmap32 {
	ptr := acquireBitmap()
	ptr.highlowcontainer.copyFrom(&rb.highlowcontainer)
	return ptr
}

// cloneSharedInto overwrites dst with a copy-on-write clone of rb and returns dst.
func (rb *bitmap32) cloneSharedInto(dst *bitmap32) *bitmap32 {
	if dst == nil {
		dst = newBitmap32()
	}
	if dst == rb {
		return dst
	}
	dst.highlowcontainer.copySharedFrom(&rb.highlowcontainer)
	return dst
}

// retainBitmap32 increments the shared reference count for rb and returns it.
func retainBitmap32(rb *bitmap32) *bitmap32 {
	if rb != nil {
		rb.refs.Add(1)
	}
	return rb
}

func (rb *bitmap32) uniquelyOwned() bool {
	return rb == nil || rb.refs.Load() == 1
}

// isUniquelyOwned reports whether rb can be mutated without copy-on-write cloning.
func (rb *bitmap32) isUniquelyOwned() bool {
	return rb.uniquelyOwned()
}

// releaseBitmap32 returns a bitmap allocated by this package back to the pool.
func releaseBitmap32(rb *bitmap32) {
	releaseBitmap(rb)
}

// minimum gets the smallest value stored in this bitmap.
func (rb *bitmap32) minimum() uint32 {
	if len(rb.highlowcontainer.containers) == 0 {
		panic("empty bitmap")
	}
	return uint32(rb.highlowcontainer.containers[0].minimum()) | (uint32(rb.highlowcontainer.keys[0]) << 16)
}

// maximum gets the largest value stored in this bitmap.
func (rb *bitmap32) maximum() uint32 {
	if len(rb.highlowcontainer.containers) == 0 {
		panic("empty bitmap")
	}
	lastindex := len(rb.highlowcontainer.containers) - 1
	return uint32(rb.highlowcontainer.containers[lastindex].maximum()) | (uint32(rb.highlowcontainer.keys[lastindex]) << 16)
}

func (rb *bitmap32) trySingle() (uint32, bool) {
	if len(rb.highlowcontainer.containers) != 1 {
		return 0, false
	}

	key := rb.highlowcontainer.keys[0]
	switch c := rb.highlowcontainer.containers[0].(type) {
	case *containerArray:
		if len(c.content) != 1 {
			return 0, false
		}
		return uint32(c.content[0]) | (uint32(key) << 16), true
	case *containerBitmap:
		if c.cardinality != 1 {
			return 0, false
		}
		return uint32(c.minimum()) | (uint32(key) << 16), true
	case *containerRun:
		if len(c.iv) != 1 || c.iv[0].length != 0 {
			return 0, false
		}
		return uint32(c.iv[0].start) | (uint32(key) << 16), true
	default:
		panic("unsupported container16 type")
	}
}

// contains reports whether the bitmap holds x.
func (rb *bitmap32) contains(x uint32) bool {
	hb := highbits(x)
	c := rb.highlowcontainer.getContainer(hb)
	return c != nil && c.contains(lowbits(x))
}

// equals returns true if the two bitmaps contain the same integers.
func (rb *bitmap32) equals(other *bitmap32) bool {
	return other != nil && other.highlowcontainer.equals(rb.highlowcontainer)
}

// add inserts x into the bitmap.
func (rb *bitmap32) add(x uint32) {
	hb := highbits(x)
	ra := &rb.highlowcontainer
	i := ra.getIndex(hb)
	if i >= 0 {
		c := ra.getWritableContainerAtIndex(i).iaddReturnMinimized(lowbits(x))
		rb.highlowcontainer.setContainerAtIndex(i, c)
	} else {
		newac := newContainerArray()
		rb.highlowcontainer.insertNewKeyValueAt(-i-1, hb, newac.iaddReturnMinimized(lowbits(x)))
	}
}

func (rb *bitmap32) addwithptr(x uint32) (int, container16) {
	hb := highbits(x)
	ra := &rb.highlowcontainer
	i := ra.getIndex(hb)
	if i >= 0 {
		c := ra.getWritableContainerAtIndex(i).iaddReturnMinimized(lowbits(x))
		rb.highlowcontainer.setContainerAtIndex(i, c)
		return i, c
	}
	newac := newContainerArray()
	c := newac.iaddReturnMinimized(lowbits(x))
	rb.highlowcontainer.insertNewKeyValueAt(-i-1, hb, c)
	return -i - 1, c
}

// checkedAdd adds x and reports whether it was newly inserted.
func (rb *bitmap32) checkedAdd(x uint32) bool {
	hb := highbits(x)
	i := rb.highlowcontainer.getIndex(hb)
	if i >= 0 {
		c := rb.highlowcontainer.getWritableContainerAtIndex(i)
		oldcard := c.getCardinality()
		c = c.iaddReturnMinimized(lowbits(x))
		rb.highlowcontainer.setContainerAtIndex(i, c)
		return c.getCardinality() > oldcard
	}
	newac := newContainerArray()
	rb.highlowcontainer.insertNewKeyValueAt(-i-1, hb, newac.iaddReturnMinimized(lowbits(x)))
	return true
}

// remove deletes x from the bitmap.
func (rb *bitmap32) remove(x uint32) {
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

// addMany adds all values in dat.
func (rb *bitmap32) addMany(dat []uint32) {
	if len(dat) == 0 {
		return
	}
	if isNonDecreasingU32(dat) {
		rb.addManySorted(dat)
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

func (rb *bitmap32) addManySorted(dat []uint32) {
	start := 0
	batchHighBits := highbits(dat[0])
	for end := 1; end < len(dat); end++ {
		hi := highbits(dat[end])
		if hi != batchHighBits {
			rb.addSortedSameHighbits(batchHighBits, dat[start:end])
			batchHighBits = hi
			start = end
		}
	}
	rb.addSortedSameHighbits(batchHighBits, dat[start:])
}

func (rb *bitmap32) addSortedSameHighbits(hb uint16, dat []uint32) {
	i := rb.highlowcontainer.getIndex(hb)
	if i < 0 {
		rb.highlowcontainer.insertNewKeyValueAt(-i-1, hb, buildContainerFromSorted32(dat))
		return
	}

	c := rb.highlowcontainer.getWritableContainerAtIndex(i)
	switch x := c.(type) {
	case *containerArray:
		c = mergeArrayWithSorted32(x, dat)
	case *containerBitmap:
		addSorted32ToBitmap(x, dat)
		c = minimizeWritableSortedContainer(x)
	case *containerRun:
		c = addSorted32Ranges(x, dat)
	default:
		panic("unsupported container16 type")
	}
	rb.highlowcontainer.setContainerAtIndex(i, c)
}

func (rb *bitmap32) loadManySorted64(dat []uint64) {
	if len(dat) == 0 {
		return
	}

	start := 0
	batchHighBits := highbits(lowbits64(dat[0]))
	for end := 1; end < len(dat); end++ {
		hi := highbits(lowbits64(dat[end]))
		if hi != batchHighBits {
			rb.highlowcontainer.appendContainer(batchHighBits, buildContainerFromSorted64Low(dat[start:end]))
			batchHighBits = hi
			start = end
		}
	}
	rb.highlowcontainer.appendContainer(batchHighBits, buildContainerFromSorted64Low(dat[start:]))
}

func buildContainerFromSorted32(dat []uint32) container16 {
	unique := countUniqueSorted32(dat, arrayDefaultMaxSize+1)
	if unique <= arrayDefaultMaxSize {
		ac := newContainerArraySize(unique)
		fillArrayFromSorted32(ac.content, dat)
		return ac
	}
	bc := newContainerBitmap()
	addSorted32ToBitmap(bc, dat)
	return minimizeFreshSortedBitmapContainer(bc)
}

func buildContainerFromSorted64Low(dat []uint64) container16 {
	unique := countUniqueSorted64Low(dat, arrayDefaultMaxSize+1)
	if unique <= arrayDefaultMaxSize {
		ac := newContainerArraySize(unique)
		fillArrayFromSorted64Low(ac.content, dat)
		return ac
	}
	bc := newContainerBitmap()
	addSorted64LowToBitmap(bc, dat)
	return minimizeFreshSortedBitmapContainer(bc)
}

func countUniqueSorted32(dat []uint32, limit int) int {
	if len(dat) == 0 {
		return 0
	}
	count := 1
	prev := dat[0]
	for _, v := range dat[1:] {
		if v == prev {
			continue
		}
		count++
		if count >= limit {
			return count
		}
		prev = v
	}
	return count
}

func countUniqueSorted64Low(dat []uint64, limit int) int {
	if len(dat) == 0 {
		return 0
	}
	count := 1
	prev := lowbits64(dat[0])
	for _, v := range dat[1:] {
		low := lowbits64(v)
		if low == prev {
			continue
		}
		count++
		if count >= limit {
			return count
		}
		prev = low
	}
	return count
}

func fillArrayFromSorted32(dst []uint16, dat []uint32) {
	if len(dst) == 0 {
		return
	}
	pos := 0
	prev := dat[0]
	dst[pos] = lowbits(prev)
	pos++
	for _, v := range dat[1:] {
		if v == prev {
			continue
		}
		dst[pos] = lowbits(v)
		pos++
		prev = v
	}
}

func fillArrayFromSorted64Low(dst []uint16, dat []uint64) {
	if len(dst) == 0 {
		return
	}
	pos := 0
	prev := lowbits64(dat[0])
	dst[pos] = lowbits(prev)
	pos++
	for _, v := range dat[1:] {
		low := lowbits64(v)
		if low == prev {
			continue
		}
		dst[pos] = lowbits(low)
		pos++
		prev = low
	}
}

func addSorted32ToBitmap(bc *containerBitmap, dat []uint32) {
	if len(dat) == 0 {
		return
	}
	prev := dat[0]
	bc.iadd(lowbits(prev))
	for _, v := range dat[1:] {
		if v == prev {
			continue
		}
		bc.iadd(lowbits(v))
		prev = v
	}
}

func addSorted64LowToBitmap(bc *containerBitmap, dat []uint64) {
	if len(dat) == 0 {
		return
	}
	prev := lowbits64(dat[0])
	bc.iadd(lowbits(prev))
	for _, v := range dat[1:] {
		low := lowbits64(v)
		if low == prev {
			continue
		}
		bc.iadd(lowbits(low))
		prev = low
	}
}

func minimizeFreshSortedBitmapContainer(bc *containerBitmap) container16 {
	if !bc.isFull() {
		return bc
	}
	result := newContainerRunRange(0, MaxUint16)
	releaseContainerBitmap(bc)
	return result
}

func minimizeWritableSortedContainer(c container16) container16 {
	bc, ok := c.(*containerBitmap)
	if !ok || !bc.isFull() {
		return c
	}
	return newContainerRunRange(0, MaxUint16)
}

func mergeArrayWithSorted32(ac *containerArray, dat []uint32) container16 {
	if len(ac.content) == 0 {
		return buildContainerFromSorted32(dat)
	}

	first := lowbits(dat[0])
	unique := countUniqueSorted32(dat, arrayDefaultMaxSize+1)
	if ac.content[len(ac.content)-1] < first {
		newCardinality := len(ac.content) + unique
		if newCardinality > arrayDefaultMaxSize {
			bc := ac.toBitmapContainer()
			addSorted32ToBitmap(bc, dat)
			return minimizeFreshSortedBitmapContainer(bc)
		}

		oldLen := len(ac.content)
		if cap(ac.content) < newCardinality {
			donor := newContainerArraySize(newCardinality)
			copy(donor.content[:oldLen], ac.content)
			fillArrayFromSorted32(donor.content[oldLen:], dat)
			replaceContainerArrayStorage(ac, donor)
			return ac
		}

		ac.content = ac.content[:newCardinality]
		fillArrayFromSorted32(ac.content[oldLen:], dat)
		return ac
	}

	newCardinality := mergedCardinalityArrayAndSorted32(ac.content, dat, arrayDefaultMaxSize+1)
	if newCardinality > arrayDefaultMaxSize {
		bc := ac.toBitmapContainer()
		addSorted32ToBitmap(bc, dat)
		return minimizeFreshSortedBitmapContainer(bc)
	}

	oldLen := len(ac.content)
	if cap(ac.content) < newCardinality {
		donor := newContainerArraySize(newCardinality)
		fillMergedArrayAndSorted32(donor.content, ac.content, dat)
		replaceContainerArrayStorage(ac, donor)
		return ac
	}

	ac.content = ac.content[:newCardinality]
	fillMergedArrayAndSorted32InPlace(ac.content, oldLen, dat)
	return ac
}

func mergedCardinalityArrayAndSorted32(left []uint16, right []uint32, limit int) int {
	count := 0
	i := 0
	prev := right[0] - 1
	for _, raw := range right {
		if raw == prev {
			continue
		}
		v := lowbits(raw)
		for i < len(left) && left[i] < v {
			count++
			if count >= limit {
				return count
			}
			i++
		}
		count++
		if count >= limit {
			return count
		}
		if i < len(left) && left[i] == v {
			i++
		}
		prev = raw
	}
	return count + len(left) - i
}

func fillMergedArrayAndSorted32(dst, left []uint16, right []uint32) {
	pos := 0
	i := 0
	prev := right[0] - 1
	for _, raw := range right {
		if raw == prev {
			continue
		}
		v := lowbits(raw)
		for i < len(left) && left[i] < v {
			dst[pos] = left[i]
			pos++
			i++
		}
		dst[pos] = v
		pos++
		if i < len(left) && left[i] == v {
			i++
		}
		prev = raw
	}
	copy(dst[pos:], left[i:])
}

func fillMergedArrayAndSorted32InPlace(dst []uint16, oldLen int, right []uint32) {
	write := len(dst) - 1
	left := oldLen - 1

	for j := len(right) - 1; j >= 0; {
		raw := right[j]
		for j > 0 && right[j-1] == raw {
			j--
		}

		v := lowbits(raw)
		for left >= 0 && dst[left] > v {
			dst[write] = dst[left]
			write--
			left--
		}

		equal := left >= 0 && dst[left] == v
		dst[write] = v
		write--
		if equal {
			left--
		}
		j--
	}

	if left >= 0 {
		copy(dst[:write+1], dst[:left+1])
	}
}

func addSorted32Ranges(c container16, dat []uint32) container16 {
	start := -1
	prev := -1
	lastSeen := dat[0] - 1
	for _, raw := range dat {
		if raw == lastSeen {
			continue
		}
		v := int(lowbits(raw))
		if start < 0 {
			start = v
			prev = v
			lastSeen = raw
			continue
		}
		if v == prev+1 {
			prev = v
			lastSeen = raw
			continue
		}
		c = c.iaddRange(start, prev+1)
		start = v
		prev = v
		lastSeen = raw
	}
	return c.iaddRange(start, prev+1)
}

func isNonDecreasingU32(dat []uint32) bool {
	for i := 1; i < len(dat); i++ {
		if dat[i] < dat[i-1] {
			return false
		}
	}
	return true
}

// addRange adds the integers in [rangeStart, rangeEnd) to the bitmap.
func (rb *bitmap32) addRange(rangeStart, rangeEnd uint64) {
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

	for hb := hbStart; hb <= hbLast; hb++ {
		containerStart := uint32(0)
		if hb == hbStart {
			containerStart = lbStart
		}
		containerLast := uint32(maxLowBit)
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

// isEmpty returns true if the bitmap32 is empty.
func (rb *bitmap32) isEmpty() bool {
	return rb.highlowcontainer.size() == 0
}

// cardinality returns the number of integers contained in the bitmap.
func (rb *bitmap32) cardinality() uint64 {
	size := uint64(0)
	for _, c := range rb.highlowcontainer.containers {
		size += uint64(c.getCardinality())
	}
	return size
}

// and computes the intersection between two bitmaps and stores the result in the current bitmap.
func (rb *bitmap32) and(x2 *bitmap32) {
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

// andCardinality returns the cardinality of the intersection between two bitmaps.
func (rb *bitmap32) andCardinality(x2 *bitmap32) uint64 {
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

// intersects checks whether two bitmaps intersect.
func (rb *bitmap32) intersects(x2 *bitmap32) bool {
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

// xor computes the symmetric difference between two bitmaps and stores the result in the current bitmap.
func (rb *bitmap32) xor(x2 *bitmap32) {
	if rb == x2 {
		rb.clear()
		return
	}
	length2 := x2.highlowcontainer.size()
	if length2 == 0 {
		return
	}
	length1 := rb.highlowcontainer.size()
	if length1 == 0 {
		rb.highlowcontainer.appendCopyMany(x2.highlowcontainer, 0, length2)
		return
	}

	outSize := rb.xorKeyCount(x2)
	if outSize == 0 {
		rb.clear()
		return
	}

	maxSize := length1 + length2
	rb.highlowcontainer.grow(maxSize)

	pos1 := length1 - 1
	pos2 := length2 - 1
	out := maxSize - 1
	for pos1 >= 0 && pos2 >= 0 {
		s1 := rb.highlowcontainer.keys[pos1]
		s2 := x2.highlowcontainer.keys[pos2]
		if s1 > s2 {
			rb.highlowcontainer.moveKeyValueAt(pos1, out)
			pos1--
			out--
			continue
		}
		if s1 < s2 {
			rb.highlowcontainer.keys[out] = s2
			rb.highlowcontainer.containers[out] = x2.highlowcontainer.containers[pos2].clone()
			pos2--
			out--
			continue
		}

		left := rb.highlowcontainer.containers[pos1]
		right := x2.highlowcontainer.containers[pos2]
		if left.equals(right) {
			releaseContainer(left)
			rb.highlowcontainer.keys[pos1] = 0
			rb.highlowcontainer.containers[pos1] = nil
			pos1--
			pos2--
			continue
		}

		c := left.xor(right)
		if c.isEmpty() {
			if c != left {
				releaseContainer(c)
			}
			releaseContainer(left)
			rb.highlowcontainer.keys[pos1] = 0
			rb.highlowcontainer.containers[pos1] = nil
			pos1--
			pos2--
			continue
		}

		rb.highlowcontainer.setContainerAtIndex(pos1, c)
		rb.highlowcontainer.moveKeyValueAt(pos1, out)
		pos1--
		pos2--
		out--
	}
	for pos2 >= 0 {
		rb.highlowcontainer.keys[out] = x2.highlowcontainer.keys[pos2]
		rb.highlowcontainer.containers[out] = x2.highlowcontainer.containers[pos2].clone()
		pos2--
		out--
	}
	for pos1 >= 0 {
		rb.highlowcontainer.moveKeyValueAt(pos1, out)
		pos1--
		out--
	}

	start := maxSize - outSize
	if start != 0 {
		copy(rb.highlowcontainer.keys[:outSize], rb.highlowcontainer.keys[start:maxSize])
		copy(rb.highlowcontainer.containers[:outSize], rb.highlowcontainer.containers[start:maxSize])
		clear(rb.highlowcontainer.keys[outSize:maxSize])
		clear(rb.highlowcontainer.containers[outSize:maxSize])
	}
	rb.highlowcontainer.resize(outSize)
}

// or computes the union between two bitmaps and stores the result in the current bitmap.
func (rb *bitmap32) or(x2 *bitmap32) {
	if rb == x2 {
		return
	}
	length2 := x2.highlowcontainer.size()
	if length2 == 0 {
		return
	}
	length1 := rb.highlowcontainer.size()
	if length1 == 0 {
		rb.highlowcontainer.appendCopyMany(x2.highlowcontainer, 0, length2)
		return
	}

	outSize := countUnionKeys(rb.highlowcontainer.keys[:length1], x2.highlowcontainer.keys[:length2])
	rb.highlowcontainer.grow(outSize)

	pos1 := length1 - 1
	pos2 := length2 - 1
	out := outSize - 1
	for pos1 >= 0 && pos2 >= 0 {
		s1 := rb.highlowcontainer.keys[pos1]
		s2 := x2.highlowcontainer.keys[pos2]
		if s1 > s2 {
			rb.highlowcontainer.moveKeyValueAt(pos1, out)
			pos1--
			out--
			continue
		}
		if s1 < s2 {
			rb.highlowcontainer.keys[out] = s2
			rb.highlowcontainer.containers[out] = x2.highlowcontainer.containers[pos2].clone()
			pos2--
			out--
			continue
		}

		merged := rb.highlowcontainer.getUnionedWritableContainer(pos1, x2.highlowcontainer.containers[pos2])
		rb.highlowcontainer.setContainerAtIndex(pos1, merged)
		rb.highlowcontainer.moveKeyValueAt(pos1, out)
		pos1--
		pos2--
		out--
	}
	for pos2 >= 0 {
		rb.highlowcontainer.keys[out] = x2.highlowcontainer.keys[pos2]
		rb.highlowcontainer.containers[out] = x2.highlowcontainer.containers[pos2].clone()
		pos2--
		out--
	}
	for pos1 >= 0 {
		rb.highlowcontainer.moveKeyValueAt(pos1, out)
		pos1--
		out--
	}
}

func (rb *bitmap32) xorKeyCount(x2 *bitmap32) int {
	pos1 := 0
	pos2 := 0
	count := 0
	length1 := rb.highlowcontainer.size()
	length2 := x2.highlowcontainer.size()
	for pos1 < length1 && pos2 < length2 {
		s1 := rb.highlowcontainer.keys[pos1]
		s2 := x2.highlowcontainer.keys[pos2]
		if s1 < s2 {
			pos1++
			count++
			continue
		}
		if s1 > s2 {
			pos2++
			count++
			continue
		}
		if !rb.highlowcontainer.containers[pos1].equals(x2.highlowcontainer.containers[pos2]) {
			count++
		}
		pos1++
		pos2++
	}
	return count + length1 - pos1 + length2 - pos2
}

// andNot computes the difference between two bitmaps and stores the result in the current bitmap.
func (rb *bitmap32) andNot(x2 *bitmap32) {
	if rb == x2 {
		rb.clear()
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

// xorBitmap32 computes the symmetric difference between two bitmaps and returns the result.
func xorBitmap32(x1, x2 *bitmap32) *bitmap32 {
	answer := x1.clone()
	answer.xor(x2)
	return answer
}

const (
	bitmap32WireContainerArray  byte = 1
	bitmap32WireContainerBitmap byte = 2
	bitmap32WireContainerRun    byte = 3

	bitmap32WireHeaderSize          = 4
	bitmap32WireContainerHeaderSize = 5
)

// WriteTo writes a posting-owned serialized version of this bitmap to stream.
func (rb *bitmap32) WriteTo(stream io.Writer) (int64, error) {
	var (
		n      int64
		header [bitmap32WireContainerHeaderSize]byte
	)

	containerCount := rb.highlowcontainer.size()
	binary.LittleEndian.PutUint32(header[:bitmap32WireHeaderSize], uint32(containerCount))
	written, err := stream.Write(header[:bitmap32WireHeaderSize])
	n += int64(written)
	if err != nil {
		return n, err
	}

	for i := 0; i < containerCount; i++ {
		key := rb.highlowcontainer.getKeyAtIndex(i)
		container := rb.highlowcontainer.getContainerAtIndex(i)

		kind, meta, payload, err := bitmap32WireEncoding(container)
		if err != nil {
			return n, err
		}

		binary.LittleEndian.PutUint16(header[0:2], key)
		header[2] = kind
		binary.LittleEndian.PutUint16(header[3:5], meta)

		written, err = stream.Write(header[:])
		n += int64(written)
		if err != nil {
			return n, err
		}

		written, err = stream.Write(payload)
		n += int64(written)
		if err != nil {
			return n, err
		}
	}

	return n, nil
}

// ReadFrom reads a posting-owned serialized version of this bitmap from stream.
func (rb *bitmap32) ReadFrom(reader io.Reader) (p int64, err error) {
	var header [bitmap32WireContainerHeaderSize]byte
	var prevKey uint16

	n, err := io.ReadFull(reader, header[:bitmap32WireHeaderSize])
	p += int64(n)
	if err != nil {
		return p, err
	}

	containerCount := int(binary.LittleEndian.Uint32(header[:bitmap32WireHeaderSize]))
	if containerCount > maxCapacity {
		return p, fmt.Errorf("bitmap32 container count exceeds %d: %d", maxCapacity, containerCount)
	}

	rb.highlowcontainer.clear()
	defer func() {
		if err != nil {
			rb.highlowcontainer.clear()
		}
	}()

	if cap(rb.highlowcontainer.keys) >= containerCount {
		rb.highlowcontainer.keys = rb.highlowcontainer.keys[:containerCount]
	} else {
		rb.highlowcontainer.keys = make([]uint16, containerCount)
	}
	if cap(rb.highlowcontainer.containers) >= containerCount {
		rb.highlowcontainer.containers = rb.highlowcontainer.containers[:containerCount]
	} else {
		rb.highlowcontainer.containers = make([]container16, containerCount)
	}

	for i := 0; i < containerCount; i++ {
		n, err = io.ReadFull(reader, header[:])
		p += int64(n)
		if err != nil {
			return p, fmt.Errorf("bitmap32 read container header #%d: %w", i, err)
		}

		key := binary.LittleEndian.Uint16(header[0:2])
		kind := header[2]
		meta := binary.LittleEndian.Uint16(header[3:5])
		if i != 0 && key <= prevKey {
			err = fmt.Errorf("bitmap32 keys are not strictly increasing")
			return p, err
		}
		prevKey = key

		container, readBytes, readErr := readBitmap32WireContainer(reader, kind, meta)
		p += readBytes
		if readErr != nil {
			err = fmt.Errorf("bitmap32 read container #%d: %w", i, readErr)
			return p, err
		}

		rb.highlowcontainer.keys[i] = key
		rb.highlowcontainer.containers[i] = container
	}

	return p, nil
}

// serializedSizeInBytes computes the serialized size in bytes of the bitmap.
func (rb *bitmap32) serializedSizeInBytes() uint64 {
	size := uint64(bitmap32WireHeaderSize)
	for _, c := range rb.highlowcontainer.containers {
		size += bitmap32WireContainerHeaderSize
		size += uint64(bitmap32WirePayloadSize(c))
	}
	return size
}

func bitmap32WireEncoding(container container16) (kind byte, meta uint16, payload []byte, err error) {
	switch c := container.(type) {
	case *containerArray:
		if len(c.content) == 0 {
			return 0, 0, nil, fmt.Errorf("cannot serialize empty containerArray")
		}
		return bitmap32WireContainerArray, uint16(len(c.content) - 1), uint16SliceAsByteSlice(c.content), nil
	case *containerBitmap:
		if c.cardinality <= arrayDefaultMaxSize {
			return 0, 0, nil, fmt.Errorf("cannot serialize sparse containerBitmap with cardinality %d", c.cardinality)
		}
		return bitmap32WireContainerBitmap, uint16(c.cardinality - 1), uint64SliceAsByteSlice(c.bitmap), nil
	case *containerRun:
		if len(c.iv) == 0 {
			return 0, 0, nil, fmt.Errorf("cannot serialize empty containerRun")
		}
		return bitmap32WireContainerRun, uint16(len(c.iv) - 1), interval16SliceAsByteSlice(c.iv), nil
	default:
		return 0, 0, nil, fmt.Errorf("unsupported bitmap32 container type %T", container)
	}
}

func bitmap32WirePayloadSize(container container16) int {
	switch c := container.(type) {
	case *containerArray:
		return len(c.content) * 2
	case *containerBitmap:
		return len(c.bitmap) * 8
	case *containerRun:
		return len(c.iv) * 4
	default:
		panic(fmt.Sprintf("unsupported bitmap32 container type %T", container))
	}
}

func validateBitmap32ArrayContainer(container *containerArray) error {
	for i := 1; i < len(container.content); i++ {
		if container.content[i] <= container.content[i-1] {
			return fmt.Errorf("containerArray values are not strictly increasing")
		}
	}
	return nil
}

func validateBitmap32BitmapContainer(container *containerBitmap) error {
	actual := int(popcntSlice(container.bitmap))
	if actual != container.cardinality {
		return fmt.Errorf("containerBitmap cardinality mismatch: header=%d actual=%d", container.cardinality, actual)
	}
	return nil
}

func validateBitmap32RunContainer(container *containerRun) error {
	var prevLast uint32
	for i, iv := range container.iv {
		last := uint32(iv.start) + uint32(iv.length)
		if last > MaxUint16 {
			return fmt.Errorf("containerRun interval %d overflows uint16", i)
		}
		if i != 0 && uint32(iv.start) <= prevLast+1 {
			return fmt.Errorf("containerRun intervals are not strictly increasing and non-adjacent")
		}
		prevLast = last
	}
	return nil
}

func readBitmap32WireContainer(reader io.Reader, kind byte, meta uint16) (container16, int64, error) {
	switch kind {
	case bitmap32WireContainerArray:
		cardinality := int(meta) + 1
		container := newContainerArraySize(cardinality)
		readBytes, err := readBitmap32Array(reader, container)
		if err != nil {
			releaseContainerArray(container)
			return nil, readBytes, err
		}
		if err := validateBitmap32ArrayContainer(container); err != nil {
			releaseContainerArray(container)
			return nil, readBytes, err
		}
		return container, readBytes, nil
	case bitmap32WireContainerBitmap:
		cardinality := int(meta) + 1
		if cardinality <= arrayDefaultMaxSize {
			return nil, 0, fmt.Errorf("invalid containerBitmap cardinality %d", cardinality)
		}
		container := newContainerBitmap()
		container.cardinality = cardinality
		readBytes, err := readBitmap32Bitmap(reader, container)
		if err != nil {
			releaseContainerBitmap(container)
			return nil, readBytes, err
		}
		if err := validateBitmap32BitmapContainer(container); err != nil {
			releaseContainerBitmap(container)
			return nil, readBytes, err
		}
		return container, readBytes, nil
	case bitmap32WireContainerRun:
		intervalCount := int(meta) + 1
		container := acquireContainerRun(intervalCount, intervalCount)
		readBytes, err := readBitmap32Run(reader, container)
		if err != nil {
			releaseContainerRun(container)
			return nil, readBytes, err
		}
		if err := validateBitmap32RunContainer(container); err != nil {
			releaseContainerRun(container)
			return nil, readBytes, err
		}
		return container, readBytes, nil
	default:
		return nil, 0, fmt.Errorf("invalid bitmap32 container kind %d", kind)
	}
}

func readBitmap32Array(reader io.Reader, container *containerArray) (int64, error) {
	n, err := io.ReadFull(reader, uint16SliceAsByteSlice(container.content))
	return int64(n), err
}

func readBitmap32Bitmap(reader io.Reader, container *containerBitmap) (int64, error) {
	n, err := io.ReadFull(reader, uint64SliceAsByteSlice(container.bitmap))
	return int64(n), err
}

func readBitmap32Run(reader io.Reader, container *containerRun) (int64, error) {
	n, err := io.ReadFull(reader, interval16SliceAsByteSlice(container.iv))
	return int64(n), err
}
