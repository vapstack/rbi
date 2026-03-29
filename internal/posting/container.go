package posting

import (
	"math"
	"math/bits"
	"slices"
	"sync"
	"unsafe"
)

type container16 interface {
	clone() container16
	and(container16) container16
	andCardinality(container16) int
	iand(container16) container16
	andNot(container16) container16
	iandNot(container16) container16
	isEmpty() bool
	getCardinality() int

	iadd(x uint16) bool
	iaddReturnMinimized(uint16) container16
	iaddRange(start, endx int) container16

	iremove(x uint16) bool
	iremoveReturnMinimized(uint16) container16

	not(start, final int) container16
	inot(firstOfRange, endx int) container16
	xor(r container16) container16
	iterate(cb func(x uint16) bool) bool
	contains(i uint16) bool
	maximum() uint16
	minimum() uint16

	equals(r container16) bool

	fillLeastSignificant16bits(array []uint32, i int, mask uint32) int
	or(r container16) container16
	isFull() bool
	ior(r container16) container16
	intersects(r container16) bool
	getSizeInBytes() int
	iremoveRange(start, final int) container16

	numberOfRuns() int
	toEfficientContainer() container16
}

func equalArrayBitmap(ac *containerArray, bc *containerBitmap) bool {
	if len(ac.content) != bc.cardinality {
		return false
	}
	ait := shortIterator{slice: ac.content}
	bit := bitmapContainerShortIterator{ptr: bc, i: bc.nextSetBit(0)}
	for ait.hasNext() {
		if ait.next() != bit.next() {
			return false
		}
	}
	return true
}

func equalArrayRun(ac *containerArray, rc *containerRun) bool {
	if len(ac.content) != rc.getCardinality() {
		return false
	}
	ait := shortIterator{slice: ac.content}
	rit := runIterator16{rc: rc}
	for ait.hasNext() {
		if ait.next() != rit.next() {
			return false
		}
	}
	return true
}

type containerIndex struct {
	keys       []uint16
	containers []container16 `msg:"-"`
}

func (ra *containerIndex) aliases(other *containerIndex) bool {
	if ra == nil || other == nil {
		return false
	}
	return slicesShareBacking(ra.keys, other.keys) || slicesShareBacking(ra.containers, other.containers)
}

func slicesShareBacking[T any](a, b []T) bool {
	if cap(a) == 0 || cap(b) == 0 {
		return false
	}
	return unsafe.SliceData(a) == unsafe.SliceData(b)
}

func rangeOfOnes(start, last int) container16 {
	if start > MaxUint16 {
		panic("rangeOfOnes called with start > MaxUint16")
	}
	if last > MaxUint16 {
		panic("rangeOfOnes called with last > MaxUint16")
	}
	if start < 0 {
		panic("rangeOfOnes called with start < 0")
	}
	if last < 0 {
		panic("rangeOfOnes called with last < 0")
	}
	return newContainerRunRange(uint16(start), uint16(last)).toEfficientContainer()
}

func (ra *containerIndex) runOptimize() {
	for i := range ra.containers {
		old := ra.getWritableContainerAtIndex(i)
		if old == nil {
			continue
		}
		next := old.toEfficientContainer()
		ra.containers[i] = next
		if next != old {
			releaseContainer(old)
		}
	}
}

func (ra *containerIndex) appendContainer(key uint16, value container16) {
	ra.keys = append(ra.keys, key)
	ra.containers = append(ra.containers, value)
}

func (ra *containerIndex) appendCopy(sa containerIndex, startingindex int) {
	ra.appendContainer(sa.keys[startingindex], sa.containers[startingindex].clone())
}

func (ra *containerIndex) appendCopyMany(sa containerIndex, startingindex, end int) {
	for i := startingindex; i < end; i++ {
		ra.appendCopy(sa, i)
	}
}

func (ra *containerIndex) grow(newsize int) {
	if newsize <= len(ra.keys) {
		return
	}
	oldSize := len(ra.keys)
	if cap(ra.keys) >= newsize {
		ra.keys = ra.keys[:newsize]
		clear(ra.keys[oldSize:])
	} else {
		keys := make([]uint16, newsize)
		copy(keys, ra.keys)
		ra.keys = keys
	}
	if cap(ra.containers) >= newsize {
		ra.containers = ra.containers[:newsize]
		clear(ra.containers[oldSize:])
	} else {
		containers := make([]container16, newsize)
		copy(containers, ra.containers)
		ra.containers = containers
	}
}

func (ra *containerIndex) moveKeyValueAt(src, dst int) {
	if src == dst {
		return
	}
	ra.keys[dst] = ra.keys[src]
	ra.containers[dst] = ra.containers[src]
	ra.keys[src] = 0
	ra.containers[src] = nil
}

func countUnionKeys[T ~uint16 | ~uint32](left, right []T) int {
	count := 0
	pos1 := 0
	pos2 := 0
	for pos1 < len(left) && pos2 < len(right) {
		s1 := left[pos1]
		s2 := right[pos2]
		if s1 < s2 {
			pos1++
		} else if s1 > s2 {
			pos2++
		} else {
			pos1++
			pos2++
		}
		count++
	}
	return count + len(left) - pos1 + len(right) - pos2
}

func (ra *containerIndex) resize(newsize int) {
	for k := newsize; k < len(ra.containers); k++ {
		ra.keys[k] = 0
		ra.containers[k] = nil
	}
	ra.keys = ra.keys[:newsize]
	ra.containers = ra.containers[:newsize]
}

func (ra *containerIndex) clear() {
	ra.releaseContainersInRange(0)
	ra.resize(0)
}

func (ra *containerIndex) clearContainerAtIndex(i int) {
	ra.keys[i] = 0
	ra.containers[i] = nil
}

func (ra *containerIndex) releaseContainersInRange(start int) {
	if start < 0 {
		start = 0
	}
	for i := start; i < len(ra.containers); i++ {
		if c := ra.containers[i]; c != nil {
			releaseContainer(c)
			ra.clearContainerAtIndex(i)
		}
	}
}

func (ra *containerIndex) copyFrom(src *containerIndex) {
	if ra == src {
		return
	}

	ra.releaseContainersInRange(0)

	if len(src.keys) == 0 {
		ra.keys = ra.keys[:0]
		ra.containers = ra.containers[:0]
		return
	}

	if cap(ra.keys) >= len(src.keys) {
		ra.keys = ra.keys[:len(src.keys)]
	} else {
		ra.keys = make([]uint16, len(src.keys))
	}
	copy(ra.keys, src.keys)

	if cap(ra.containers) >= len(src.containers) {
		ra.containers = ra.containers[:len(src.containers)]
	} else {
		ra.containers = make([]container16, len(src.containers))
	}
	for i := range src.containers {
		ra.containers[i] = src.containers[i].clone()
	}
}

func (ra *containerIndex) copySharedFrom(src *containerIndex) {
	if ra == src {
		return
	}
	if ra.aliases(src) {
		tmp := new(containerIndex)
		tmp.copySharedFrom(src)
		ra.releaseContainersInRange(0)
		ra.keys = tmp.keys
		ra.containers = tmp.containers
		return
	}

	ra.releaseContainersInRange(0)

	if len(src.keys) == 0 {
		ra.keys = ra.keys[:0]
		ra.containers = ra.containers[:0]
		return
	}

	if cap(ra.keys) >= len(src.keys) {
		ra.keys = ra.keys[:len(src.keys)]
	} else {
		ra.keys = make([]uint16, len(src.keys))
	}
	copy(ra.keys, src.keys)

	if cap(ra.containers) >= len(src.containers) {
		ra.containers = ra.containers[:len(src.containers)]
	} else {
		ra.containers = make([]container16, len(src.containers))
	}
	for i := range src.containers {
		ra.containers[i] = retainContainer(src.containers[i])
	}
}

func (ra *containerIndex) getContainer(x uint16) container16 {
	i := ra.binarySearch(0, int64(len(ra.keys)), x)
	if i < 0 {
		return nil
	}
	return ra.containers[i]
}

func (ra *containerIndex) getContainerAtIndex(i int) container16 {
	return ra.containers[i]
}

func (ra *containerIndex) getUnionedWritableContainer(pos int, other container16) container16 {
	return ra.getWritableContainerAtIndex(pos).ior(other)
}

func (ra *containerIndex) getWritableContainerAtIndex(i int) container16 {
	current := ra.containers[i]
	if containerUniquelyOwned(current) {
		return current
	}
	cloned := current.clone()
	releaseContainer(current)
	ra.containers[i] = cloned
	return cloned
}

func (ra *containerIndex) getIndex(x uint16) int {
	size := len(ra.keys)
	if size == 0 {
		return -1
	}
	last := ra.keys[size-1]
	if last == x {
		return size - 1
	}
	if last < x {
		return -(size + 1)
	}
	return ra.binarySearch(0, int64(size), x)
}

func (ra *containerIndex) getKeyAtIndex(i int) uint16 {
	return ra.keys[i]
}

func (ra *containerIndex) insertNewKeyValueAt(i int, key uint16, value container16) {
	if i == len(ra.keys) {
		ra.keys = append(ra.keys, key)
		ra.containers = append(ra.containers, value)
		return
	}

	ra.keys = append(ra.keys, 0)
	ra.containers = append(ra.containers, nil)

	copy(ra.keys[i+1:], ra.keys[i:])
	copy(ra.containers[i+1:], ra.containers[i:])

	ra.keys[i] = key
	ra.containers[i] = value
}

func (ra *containerIndex) removeAtIndex(i int) {
	removed := ra.containers[i]
	last := len(ra.keys) - 1
	copy(ra.keys[i:], ra.keys[i+1:])
	copy(ra.containers[i:], ra.containers[i+1:])
	ra.keys[last] = 0
	ra.containers[last] = nil
	ra.keys = ra.keys[:last]
	ra.containers = ra.containers[:last]
	releaseContainer(removed)
}

func (ra *containerIndex) setContainerAtIndex(i int, c container16) {
	if old := ra.containers[i]; old != nil && old != c {
		releaseContainer(old)
	}
	ra.containers[i] = c
}

func (ra *containerIndex) replaceKeyAndContainerAtIndex(i int, key uint16, c container16) {
	ra.keys[i] = key
	if old := ra.containers[i]; old != nil && old != c {
		releaseContainer(old)
	}
	ra.containers[i] = c
}

func (ra *containerIndex) size() int {
	return len(ra.keys)
}

func (ra *containerIndex) binarySearch(begin, end int64, ikey uint16) int {
	low := begin
	high := end - 1
	for low+16 <= high {
		middleIndex := low + (high-low)/2
		middleValue := ra.keys[middleIndex]

		if middleValue < ikey {
			low = middleIndex + 1
		} else if middleValue > ikey {
			high = middleIndex - 1
		} else {
			return int(middleIndex)
		}
	}
	for ; low <= high; low++ {
		val := ra.keys[low]
		if val >= ikey {
			if val == ikey {
				return int(low)
			}
			break
		}
	}
	return -int(low + 1)
}

func (ra *containerIndex) equals(o interface{}) bool {
	srb, ok := o.(containerIndex)
	if !ok {
		return false
	}

	if srb.size() != ra.size() {
		return false
	}
	for i, k := range ra.keys {
		if k != srb.keys[i] {
			return false
		}
	}

	for i, c := range ra.containers {
		if !c.equals(srb.containers[i]) {
			return false
		}
	}
	return true
}

// Find the smallest integer index larger than pos such that array[index].key >= min.
func (ra *containerIndex) advanceUntil(min uint16, pos int) int {
	lower := pos + 1

	if lower >= len(ra.keys) || ra.keys[lower] >= min {
		return lower
	}

	spansize := 1
	for lower+spansize < len(ra.keys) && ra.keys[lower+spansize] < min {
		spansize *= 2
	}

	var upper int
	if lower+spansize < len(ra.keys) {
		upper = lower + spansize
	} else {
		upper = len(ra.keys) - 1
	}

	if ra.keys[upper] == min {
		return upper
	}
	if ra.keys[upper] < min {
		return len(ra.keys)
	}

	lower += spansize >> 1

	mid := 0
	for lower+1 != upper {
		mid = (lower + upper) >> 1
		if ra.keys[mid] == min {
			return mid
		} else if ra.keys[mid] < min {
			lower = mid
		} else {
			upper = mid
		}
	}
	return upper
}

const (
	arrayDefaultMaxSize = 4096 // containers with 4096 or fewer integers should be array containers.
	arrayLazyLowerBound = 1024
	maxCapacity         = 1 << 16
	invalidCardinality  = -1

	// MaxUint32 is the largest uint32 value.
	MaxUint32 = math.MaxUint32

	// MaxRange is One more than the maximum allowed bitmap bit index. For use as an upper
	// bound for ranges.
	MaxRange uint64 = MaxUint32 + 1

	// MaxUint16 is the largest 16 bit unsigned int.
	// This is the largest value an interval16 can store.
	MaxUint16 = math.MaxUint16

	// Compute wordSizeInBytes, the size of a word in bytes.
	_m              = ^uint64(0)
	_logS           = _m>>8&1 + _m>>16&1 + _m>>32&1
	wordSizeInBytes = 1 << _logS

	// other constants used in ctz_generic.go
	wordSizeInBits = wordSizeInBytes << 3 // word size in bits
)

const maxWord = 1<<wordSizeInBits - 1

func fill(arr []uint64, val uint64) {
	for i := range arr {
		arr[i] = val
	}
}

func fillRange(arr []uint64, start, end int, val uint64) {
	for i := start; i < end; i++ {
		arr[i] = val
	}
}

func highbits(x uint32) uint16 {
	return uint16(x >> 16)
}

func lowbits(x uint32) uint16 {
	return uint16(x & maxLowBit)
}

const maxLowBit = 0xFFFF

func minOfInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type shortIterable interface {
	hasNext() bool
	next() uint16
}

type shortPeekable interface {
	shortIterable
	peekNext() uint16
	advanceIfNeeded(minval uint16)
}

type shortIterator struct {
	slice []uint16
	loc   int
}

func (si *shortIterator) hasNext() bool {
	return si.loc < len(si.slice)
}

func (si *shortIterator) next() uint16 {
	a := si.slice[si.loc]
	si.loc++
	return a
}

func (si *shortIterator) peekNext() uint16 {
	return si.slice[si.loc]
}

func (si *shortIterator) advanceIfNeeded(minval uint16) {
	if si.hasNext() && si.peekNext() < minval {
		si.loc = advanceUntil(si.slice, si.loc, len(si.slice), minval)
	}
}

type manyIterable interface {
	nextMany(hs uint32, buf []uint32) int
	nextMany64(hs uint64, buf []uint64) int
}

func (si *shortIterator) nextMany(hs uint32, buf []uint32) int {
	n := 0
	l := si.loc
	s := si.slice
	for n < len(buf) && l < len(s) {
		buf[n] = uint32(s[l]) | hs
		l++
		n++
	}
	si.loc = l
	return n
}

func (si *shortIterator) nextMany64(hs uint64, buf []uint64) int {
	n := 0
	l := si.loc
	s := si.slice
	for n < len(buf) && l < len(s) {
		buf[n] = uint64(s[l]) | hs
		l++
		n++
	}
	si.loc = l
	return n
}

func ownContainerArray(ac *containerArray) *containerArray {
	if ac != nil {
		ac.refs.Store(1)
	}
	return ac
}

func ownContainerBitmap(bc *containerBitmap) *containerBitmap {
	if bc != nil {
		bc.refs.Store(1)
	}
	return bc
}

func ownContainerRun(rc *containerRun) *containerRun {
	if rc != nil {
		rc.refs.Store(1)
	}
	return rc
}

func retainContainer(c container16) container16 {
	switch x := c.(type) {
	case *containerRun:
		x.refs.Add(1)
	case *containerArray:
		x.refs.Add(1)
	case *containerBitmap:
		x.refs.Add(1)
	}
	return c
}

func containerUniquelyOwned(c container16) bool {
	switch x := c.(type) {
	case *containerRun:
		return x.refs.Load() == 1
	case *containerArray:
		return x.refs.Load() == 1
	case *containerBitmap:
		return x.refs.Load() == 1
	default:
		return true
	}
}

func difference(set1 []uint16, set2 []uint16, buffer []uint16) int {
	if len(set2) == 0 {
		buffer = buffer[:len(set1)]
		copy(buffer, set1)
		return len(set1)
	}
	if len(set1) == 0 {
		return 0
	}
	pos := 0
	k1 := 0
	k2 := 0
	buffer = buffer[:cap(buffer)]
	s1 := set1[k1]
	s2 := set2[k2]
	for {
		if s1 < s2 {
			buffer[pos] = s1
			pos++
			k1++
			if k1 >= len(set1) {
				break
			}
			s1 = set1[k1]
		} else if s1 == s2 {
			k1++
			k2++
			if k1 >= len(set1) {
				break
			}
			s1 = set1[k1]
			if k2 >= len(set2) {
				for ; k1 < len(set1); k1++ {
					buffer[pos] = set1[k1]
					pos++
				}
				break
			}
			s2 = set2[k2]
		} else { // if (val1>val2)
			k2++
			if k2 >= len(set2) {
				for ; k1 < len(set1); k1++ {
					buffer[pos] = set1[k1]
					pos++
				}
				break
			}
			s2 = set2[k2]
		}
	}
	return pos
}

func exclusiveUnion2by2(set1 []uint16, set2 []uint16, buffer []uint16) int {
	if len(set2) == 0 {
		buffer = buffer[:len(set1)]
		copy(buffer, set1[:])
		return len(set1)
	}
	if len(set1) == 0 {
		buffer = buffer[:len(set2)]
		copy(buffer, set2[:])
		return len(set2)
	}
	pos := 0
	k1 := 0
	k2 := 0
	s1 := set1[k1]
	s2 := set2[k2]
	buffer = buffer[:cap(buffer)]
	for {
		if s1 < s2 {
			buffer[pos] = s1
			pos++
			k1++
			if k1 >= len(set1) {
				for ; k2 < len(set2); k2++ {
					buffer[pos] = set2[k2]
					pos++
				}
				break
			}
			s1 = set1[k1]
		} else if s1 == s2 {
			k1++
			k2++
			if k1 >= len(set1) {
				for ; k2 < len(set2); k2++ {
					buffer[pos] = set2[k2]
					pos++
				}
				break
			}
			if k2 >= len(set2) {
				for ; k1 < len(set1); k1++ {
					buffer[pos] = set1[k1]
					pos++
				}
				break
			}
			s1 = set1[k1]
			s2 = set2[k2]
		} else { // if (val1>val2)
			buffer[pos] = s2
			pos++
			k2++
			if k2 >= len(set2) {
				for ; k1 < len(set1); k1++ {
					buffer[pos] = set1[k1]
					pos++
				}
				break
			}
			s2 = set2[k2]
		}
	}
	return pos
}

func intersection2by2(
	set1 []uint16,
	set2 []uint16,
	buffer []uint16,
) int {
	if len(set1)*64 < len(set2) {
		return onesidedgallopingintersect2by2(set1, set2, buffer)
	} else if len(set2)*64 < len(set1) {
		return onesidedgallopingintersect2by2(set2, set1, buffer)
	} else {
		return localintersect2by2(set1, set2, buffer)
	}
}

// intersection2by2Cardinality computes the cardinality of the intersection
func intersection2by2Cardinality(
	set1 []uint16,
	set2 []uint16,
) int {
	if len(set1)*64 < len(set2) {
		return onesidedgallopingintersect2by2Cardinality(set1, set2)
	} else if len(set2)*64 < len(set1) {
		return onesidedgallopingintersect2by2Cardinality(set2, set1)
	} else {
		return localintersect2by2Cardinality(set1, set2)
	}
}

// intersects2by2 computes whether the two sets intersect
func intersects2by2(
	set1 []uint16,
	set2 []uint16,
) bool {
	// could be optimized if one set is much larger than the other one
	if (len(set1) == 0) || (len(set2) == 0) {
		return false
	}
	index1 := 0
	index2 := 0
	value1 := set1[index1]
	value2 := set2[index2]
mainwhile:
	for {

		if value2 < value1 {
			for {
				index2++
				if index2 == len(set2) {
					break mainwhile
				}
				value2 = set2[index2]
				if value2 >= value1 {
					break
				}
			}
		}
		if value1 < value2 {
			for {
				index1++
				if index1 == len(set1) {
					break mainwhile
				}
				value1 = set1[index1]
				if value1 >= value2 {
					break
				}
			}
		} else {
			// (set2[k2] == set1[k1])
			return true
		}
	}
	return false
}

func localintersect2by2(
	set1 []uint16,
	set2 []uint16,
	buffer []uint16,
) int {
	if (len(set1) == 0) || (len(set2) == 0) {
		return 0
	}
	k1 := 0
	k2 := 0
	pos := 0
	buffer = buffer[:cap(buffer)]
	s1 := set1[k1]
	s2 := set2[k2]
mainwhile:
	for {
		if s2 < s1 {
			for {
				k2++
				if k2 == len(set2) {
					break mainwhile
				}
				s2 = set2[k2]
				if s2 >= s1 {
					break
				}
			}
		}
		if s1 < s2 {
			for {
				k1++
				if k1 == len(set1) {
					break mainwhile
				}
				s1 = set1[k1]
				if s1 >= s2 {
					break
				}
			}
		} else {
			// (set2[k2] == set1[k1])
			buffer[pos] = s1
			pos++
			k1++
			if k1 == len(set1) {
				break
			}
			s1 = set1[k1]
			k2++
			if k2 == len(set2) {
				break
			}
			s2 = set2[k2]
		}
	}
	return pos
}

// / localintersect2by2Cardinality computes the cardinality of the intersection
func localintersect2by2Cardinality(
	set1 []uint16,
	set2 []uint16,
) int {
	if (len(set1) == 0) || (len(set2) == 0) {
		return 0
	}
	index1 := 0
	index2 := 0
	pos := 0
	value1 := set1[index1]
	value2 := set2[index2]
mainwhile:
	for {
		if value2 < value1 {
			for {
				index2++
				if index2 == len(set2) {
					break mainwhile
				}
				value2 = set2[index2]
				if value2 >= value1 {
					break
				}
			}
		}
		if value1 < value2 {
			for {
				index1++
				if index1 == len(set1) {
					break mainwhile
				}
				value1 = set1[index1]
				if value1 >= value2 {
					break
				}
			}
		} else {
			// (set2[k2] == set1[k1])
			pos++
			index1++
			if index1 == len(set1) {
				break
			}
			value1 = set1[index1]
			index2++
			if index2 == len(set2) {
				break
			}
			value2 = set2[index2]
		}
	}
	return pos
}

func advanceUntil(
	array []uint16,
	pos int,
	length int,
	min uint16,
) int {
	lower := pos + 1

	if lower >= length || array[lower] >= min {
		return lower
	}

	spansize := 1

	for lower+spansize < length && array[lower+spansize] < min {
		spansize *= 2
	}
	var upper int
	if lower+spansize < length {
		upper = lower + spansize
	} else {
		upper = length - 1
	}

	if array[upper] == min {
		return upper
	}

	if array[upper] < min {
		// means
		// array
		// has no
		// item
		// >= min
		// pos = array.length;
		return length
	}

	// we know that the next-smallest span was too small
	lower += spansize >> 1

	mid := 0
	for lower+1 != upper {
		mid = (lower + upper) >> 1
		if array[mid] == min {
			return mid
		} else if array[mid] < min {
			lower = mid
		} else {
			upper = mid
		}
	}
	return upper
}

func onesidedgallopingintersect2by2(
	smallset []uint16,
	largeset []uint16,
	buffer []uint16,
) int {
	if len(smallset) == 0 {
		return 0
	}
	buffer = buffer[:cap(buffer)]
	k1 := 0
	k2 := 0
	pos := 0
	s1 := largeset[k1]
	s2 := smallset[k2]
mainwhile:

	for {
		if s1 < s2 {
			k1 = advanceUntil(largeset, k1, len(largeset), s2)
			if k1 == len(largeset) {
				break mainwhile
			}
			s1 = largeset[k1]
		}
		if s2 < s1 {
			k2++
			if k2 == len(smallset) {
				break mainwhile
			}
			s2 = smallset[k2]
		} else {

			buffer[pos] = s2
			pos++
			k2++
			if k2 == len(smallset) {
				break
			}
			s2 = smallset[k2]
			k1 = advanceUntil(largeset, k1, len(largeset), s2)
			if k1 == len(largeset) {
				break mainwhile
			}
			s1 = largeset[k1]
		}

	}
	return pos
}

func onesidedgallopingintersect2by2Cardinality(
	smallset []uint16,
	largeset []uint16,
) int {
	if len(smallset) == 0 {
		return 0
	}
	k1 := 0
	k2 := 0
	pos := 0
	s1 := largeset[k1]
	s2 := smallset[k2]
mainwhile:

	for {
		if s1 < s2 {
			k1 = advanceUntil(largeset, k1, len(largeset), s2)
			if k1 == len(largeset) {
				break mainwhile
			}
			s1 = largeset[k1]
		}
		if s2 < s1 {
			k2++
			if k2 == len(smallset) {
				break mainwhile
			}
			s2 = smallset[k2]
		} else {

			pos++
			k2++
			if k2 == len(smallset) {
				break
			}
			s2 = smallset[k2]
			k1 = advanceUntil(largeset, k1, len(largeset), s2)
			if k1 == len(largeset) {
				break mainwhile
			}
			s1 = largeset[k1]
		}

	}
	return pos
}

func binarySearch(array []uint16, ikey uint16) int {
	low := 0
	high := len(array) - 1
	for low+16 <= high {
		middleIndex := int(uint32(low+high) >> 1)
		middleValue := array[middleIndex]
		if middleValue < ikey {
			low = middleIndex + 1
		} else if middleValue > ikey {
			high = middleIndex - 1
		} else {
			return middleIndex
		}
	}
	for ; low <= high; low++ {
		val := array[low]
		if val >= ikey {
			if val == ikey {
				return low
			}
			break
		}
	}
	return -(low + 1)
}

var bitmapPool sync.Pool

var runContainerPool sync.Pool

var containerArrayPoolCapacities = [...]int{
	32,
	64,
	128,
	256,
	512,
	1024,
	1536,
	2048,
	3072,
	4096,
	6144,
	8192,
	12288,
	16384,
}

// maxContainerArrayPoolCapacity limits which containerArray capacities are
// returned to the pool.
var maxContainerArrayPoolCapacity = 4 * arrayDefaultMaxSize

const maxPooledRunContainerCapacity = 4 * arrayDefaultMaxSize

var containerArrayClassPools [len(containerArrayPoolCapacities)]sync.Pool

func containerArrayPoolIndex(size int) int {
	if size <= 0 {
		size = containerArrayPoolCapacities[0]
	}
	if size > maxContainerArrayPoolCapacity {
		return -1
	}
	for i, class := range containerArrayPoolCapacities {
		if size <= class {
			return i
		}
	}
	return -1
}

func containerArrayPoolPutIndex(size int) int {
	if size <= 0 || size > maxContainerArrayPoolCapacity {
		return -1
	}
	idx := -1
	for i, class := range containerArrayPoolCapacities {
		if size < class {
			break
		}
		idx = i
	}
	return idx
}

func acquireBitmap() *bitmap32 {
	if v := bitmapPool.Get(); v != nil {
		rb := v.(*bitmap32)
		rb.refs.Store(1)
		return rb
	}
	rb := new(bitmap32)
	rb.refs.Store(1)
	return rb
}

func releaseBitmap(rb *bitmap32) {
	if rb == nil {
		return
	}
	if rb.refs.Add(-1) != 0 {
		return
	}
	rb.highlowcontainer.clear()
	bitmapPool.Put(rb)
}

func acquireContainerRun(capHint, length int) *containerRun {
	if capHint < length {
		capHint = length
	}

	if v := runContainerPool.Get(); v != nil {
		rc := v.(*containerRun)
		rc.refs.Store(1)
		oldLen := len(rc.iv)
		if cap(rc.iv) < capHint {
			rc.iv = slices.Grow(rc.iv, capHint)
		}
		rc.iv = rc.iv[:length]
		if length > oldLen {
			clear(rc.iv[oldLen:length])
		}
		return rc
	}

	rc := &containerRun{iv: make([]interval16, length, capHint)}
	rc.refs.Store(1)
	return rc
}

func releaseContainerRun(rc *containerRun) {
	if rc == nil {
		return
	}
	if rc.refs.Add(-1) != 0 {
		return
	}
	if cap(rc.iv) > maxPooledRunContainerCapacity {
		return
	}
	clear(rc.iv)
	rc.iv = rc.iv[:0]
	runContainerPool.Put(rc)
}

func newContainerArray() *containerArray {
	return acquireContainerArray(containerArrayPoolCapacities[0], 0)
}

func newContainerArrayCapacity(size int) *containerArray {
	if size <= 0 {
		size = containerArrayPoolCapacities[0]
	}
	return acquireContainerArray(size, 0)
}

func newContainerArraySize(size int) *containerArray {
	if size <= 0 {
		return newContainerArray()
	}
	return acquireContainerArray(size, size)
}

func newContainerArrayFromSlice(src []uint16) *containerArray {
	if len(src) == 0 {
		return newContainerArray()
	}
	ac := newContainerArraySize(len(src))
	copy(ac.content, src)
	return ac
}

func acquireContainerArray(capacity, length int) *containerArray {
	if capacity < length {
		capacity = length
	}

	idx := containerArrayPoolIndex(capacity)
	if idx < 0 {
		ac := &containerArray{content: make([]uint16, length, capacity)}
		ac.refs.Store(1)
		return ac
	}

	if v := containerArrayClassPools[idx].Get(); v != nil {
		ac := v.(*containerArray)
		ac.refs.Store(1)
		oldLen := len(ac.content)
		ac.content = ac.content[:length]
		if length > oldLen {
			clear(ac.content[oldLen:length])
		}
		return ac
	}

	ac := &containerArray{
		content: make([]uint16, containerArrayPoolCapacities[idx])[:length],
	}
	ac.refs.Store(1)
	return ac
}

func releaseContainerArray(ac *containerArray) {
	if ac == nil {
		return
	}
	if ac.refs.Add(-1) != 0 {
		return
	}

	capacity := cap(ac.content)
	if capacity > maxContainerArrayPoolCapacity {
		return
	}
	clear(ac.content)

	idx := containerArrayPoolPutIndex(capacity)
	if idx < 0 {
		return
	}

	ac.content = ac.content[:0]
	containerArrayClassPools[idx].Put(ac)
}

func replaceContainerArrayStorage(ac, donor *containerArray) {
	oldContent := ac.content
	ac.content = donor.content
	donor.content = oldContent
	releaseContainerArray(donor)
}

func replaceContainerRunStorage(rc, donor *containerRun) {
	oldIV := rc.iv
	rc.iv = donor.iv
	donor.iv = oldIV
	releaseContainerRun(donor)
}

func releaseContainer(c container16) {
	switch x := c.(type) {
	case *containerRun:
		releaseContainerRun(x)
	case *containerArray:
		releaseContainerArray(x)
	case *containerBitmap:
		releaseContainerBitmap(x)
	}
}

var (
	intIteratorPool     sync.Pool
	manyIntIteratorPool sync.Pool
)

func acquireIntIterator(a *bitmap32) *intIterator {
	if v := intIteratorPool.Get(); v != nil {
		it := v.(*intIterator)
		it.initialize(a)
		return it
	}
	it := &intIterator{}
	it.initialize(a)
	return it
}

func releaseIntIterator(it *intIterator) {
	if it == nil {
		return
	}
	it.pos = 0
	it.hs = 0
	it.iter = nil
	it.highlowcontainer = nil
	it.shortIter = shortIterator{}
	it.runIter = runIterator16{}
	it.bitmapIter = bitmapContainerShortIterator{}
	intIteratorPool.Put(it)
}

func acquireManyIntIterator(a *bitmap32) *manyIntIterator {
	if v := manyIntIteratorPool.Get(); v != nil {
		it := v.(*manyIntIterator)
		it.initialize(a)
		return it
	}
	it := &manyIntIterator{}
	it.initialize(a)
	return it
}

func releaseManyIntIterator(it *manyIntIterator) {
	if it == nil {
		return
	}
	it.pos = 0
	it.hs = 0
	it.iter = nil
	it.highlowcontainer = nil
	it.shortIter = shortIterator{}
	it.runIter = runIterator16{}
	it.bitmapIter = bitmapContainerManyIterator{}
	manyIntIteratorPool.Put(it)
}

func releaseIterator(it intPeekable) {
	if releasable, ok := it.(interface{ release() }); ok {
		releasable.release()
	}
}

func releaseManyIterator(it manyIntIterable) {
	if releasable, ok := it.(interface{ release() }); ok {
		releasable.release()
	}
}

func popcount(x uint64) uint64 {
	return uint64(bits.OnesCount64(x))
}

func popcntSlice(s []uint64) uint64 {
	cnt := uint64(0)
	for _, x := range s {
		cnt += popcount(x)
	}
	return cnt
}

func popcntMaskSlice(s, m []uint64) uint64 {
	cnt := uint64(0)
	for i := range s {
		cnt += popcount(s[i] &^ m[i])
	}
	return cnt
}

func popcntAndSlice(s, m []uint64) uint64 {
	cnt := uint64(0)
	for i := range s {
		cnt += popcount(s[i] & m[i])
	}
	return cnt
}

func popcntXorSlice(s, m []uint64) uint64 {
	cnt := uint64(0)
	for i := range s {
		cnt += popcount(s[i] ^ m[i])
	}
	return cnt
}

// countLeadingZeros returns the number of leading zeros bits in x; the result is 64 for x == 0.
func countLeadingZeros(x uint64) int {
	return bits.LeadingZeros64(x)
}

// countTrailingZeros returns the number of trailing zero bits in x; the result is 64 for x == 0.
func countTrailingZeros(x uint64) int {
	return bits.TrailingZeros64(x)
}

func union2by2(set1 []uint16, set2 []uint16, buffer []uint16) int {
	pos := 0
	k1 := 0
	k2 := 0
	if len(set2) == 0 {
		buffer = buffer[:len(set1)]
		copy(buffer, set1[:])
		return len(set1)
	}
	if len(set1) == 0 {
		buffer = buffer[:len(set2)]
		copy(buffer, set2[:])
		return len(set2)
	}
	s1 := set1[k1]
	s2 := set2[k2]
	buffer = buffer[:cap(buffer)]
	for {
		if s1 < s2 {
			buffer[pos] = s1
			pos++
			k1++
			if k1 >= len(set1) {
				copy(buffer[pos:], set2[k2:])
				pos += len(set2) - k2
				break
			}
			s1 = set1[k1]
		} else if s1 == s2 {
			buffer[pos] = s1
			pos++
			k1++
			k2++
			if k1 >= len(set1) {
				copy(buffer[pos:], set2[k2:])
				pos += len(set2) - k2
				break
			}
			if k2 >= len(set2) {
				copy(buffer[pos:], set1[k1:])
				pos += len(set1) - k1
				break
			}
			s1 = set1[k1]
			s2 = set2[k2]
		} else { // if (set1[k1]>set2[k2])
			buffer[pos] = s2
			pos++
			k2++
			if k2 >= len(set2) {
				copy(buffer[pos:], set1[k1:])
				pos += len(set1) - k1
				break
			}
			s2 = set2[k2]
		}
	}
	return pos
}

func uint64SliceAsByteSlice(slice []uint64) []byte {
	if len(slice) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(slice))), len(slice)*8)
}

func uint16SliceAsByteSlice(slice []uint16) []byte {
	if len(slice) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(slice))), len(slice)*2)
}

func interval16SliceAsByteSlice(slice []interval16) []byte {
	if len(slice) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(slice))), len(slice)*4)
}
