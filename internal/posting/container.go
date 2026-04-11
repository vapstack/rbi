package posting

import (
	"math"
	"math/bits"
	"unsafe"

	"github.com/vapstack/rbi/internal/pooled"
)

type container16 interface {
	clone() container16
	retain() container16
	release()
	uniquelyOwned() bool
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
	storage    *containerIndexStorage
	inlineKeys [4]uint16
	inlineVals [4]container16
}

type containerIndexStorage struct {
	keys       []uint16
	containers []container16
}

var containerIndexPoolCapacities = [...]int{
	8,
	16,
	32,
	64,
	128,
	256,
	512,
	1024,
	2048,
	4096,
	8192,
	16384,
	32768,
	65536,
}

const maxPooledContainerIndexCapacity = 1 << 16

var containerIndexStoragePools [len(containerIndexPoolCapacities)]pooled.Pointers[containerIndexStorage]

func (ra *containerIndex) ensureInline() {
	if ra == nil || ra.keys != nil || ra.containers != nil {
		return
	}
	ra.keys = ra.inlineKeys[:0]
	ra.containers = ra.inlineVals[:0]
}

func containerIndexPoolIndex(size int) int {
	if size <= containerIndexPoolCapacities[0] {
		return 0
	}
	if size > maxPooledContainerIndexCapacity {
		return -1
	}
	return bits.Len(uint(size-1)) - 3
}

func getContainerIndexStorageWithLen(l int) *containerIndexStorage {
	if l <= 0 {
		l = containerIndexPoolCapacities[0]
	}
	idx := containerIndexPoolIndex(l)
	if idx < 0 {
		panic("containerIndex size exceeds pooled capacity")
	}
	out := containerIndexStoragePools[idx].Get()
	out.keys = out.keys[:l]
	out.containers = out.containers[:l]
	return out
}

func putContainerIndexStorage(storage *containerIndexStorage) {
	if storage == nil {
		return
	}
	idx := containerIndexPoolIndex(cap(storage.keys))
	if idx < 0 {
		panic("containerIndex storage capacity exceeds pooled capacity")
	}
	containerIndexStoragePools[idx].Put(storage)
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
			old.release()
		}
	}
}

func (ra *containerIndex) appendContainer(key uint16, value container16) {
	i := len(ra.keys)
	ra.setSize(i + 1)
	ra.keys[i] = key
	ra.containers[i] = value
}

func (ra *containerIndex) appendCopy(sa containerIndex, startingindex int) {
	ra.appendContainer(sa.keys[startingindex], sa.containers[startingindex].clone())
}

func (ra *containerIndex) appendCopyMany(sa containerIndex, startingindex, end int) {
	for i := startingindex; i < end; i++ {
		ra.appendCopy(sa, i)
	}
}

func (ra *containerIndex) releaseBacking() {
	if ra.storage != nil {
		putContainerIndexStorage(ra.storage)
		ra.storage = nil
	}
	ra.keys = nil
	ra.containers = nil
}

func (ra *containerIndex) setSize(newsize int) {
	ra.ensureInline()
	oldLen := len(ra.keys)
	if cap(ra.keys) >= newsize {
		if newsize > oldLen {
			ra.keys = ra.keys[:newsize]
			clear(ra.keys[oldLen:newsize])
			ra.containers = ra.containers[:newsize]
			clear(ra.containers[oldLen:newsize])
		} else {
			clear(ra.keys[newsize:oldLen])
			clear(ra.containers[newsize:oldLen])
			ra.keys = ra.keys[:newsize]
			ra.containers = ra.containers[:newsize]
		}
		return
	}

	oldKeys := ra.keys
	oldContainers := ra.containers
	oldStorage := ra.storage

	if newsize <= len(ra.inlineKeys) {
		ra.storage = nil
		ra.keys = ra.inlineKeys[:newsize]
		ra.containers = ra.inlineVals[:newsize]
	} else {
		next := getContainerIndexStorageWithLen(newsize)
		ra.storage = next
		ra.keys = next.keys[:newsize]
		ra.containers = next.containers[:newsize]
	}

	copied := min(len(oldKeys), newsize)
	copy(ra.keys, oldKeys[:copied])
	copy(ra.containers, oldContainers[:copied])
	clear(ra.keys[copied:newsize])
	clear(ra.containers[copied:newsize])

	if oldStorage != nil {
		putContainerIndexStorage(oldStorage)
	}
}

func (ra *containerIndex) copyAliasedFrom(src *containerIndex, retain bool) {
	n := len(src.keys)
	if n == 0 {
		ra.storage = nil
		ra.keys = ra.inlineKeys[:0]
		ra.containers = ra.inlineVals[:0]
		return
	}

	if n <= len(ra.inlineKeys) {
		var keys [4]uint16
		var containers [4]container16
		copy(keys[:n], src.keys)
		for i := 0; i < n; i++ {
			if retain {
				containers[i] = src.containers[i].retain()
			} else {
				containers[i] = src.containers[i].clone()
			}
		}

		ra.storage = nil
		ra.keys = ra.inlineKeys[:n]
		ra.containers = ra.inlineVals[:n]
		copy(ra.keys, keys[:n])
		copy(ra.containers, containers[:n])
		return
	}

	next := getContainerIndexStorageWithLen(n)
	copy(next.keys, src.keys)
	for i := 0; i < n; i++ {
		if retain {
			next.containers[i] = src.containers[i].retain()
		} else {
			next.containers[i] = src.containers[i].clone()
		}
	}

	ra.storage = next
	ra.keys = next.keys[:n]
	ra.containers = next.containers[:n]
}

func (ra *containerIndex) grow(newsize int) {
	if newsize <= len(ra.keys) {
		return
	}
	ra.setSize(newsize)
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
	clear(ra.keys[newsize:])
	clear(ra.containers[newsize:])
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
			c.release()
			ra.clearContainerAtIndex(i)
		}
	}
}

func (ra *containerIndex) copyFrom(src *containerIndex) {
	if ra == src {
		return
	}
	if ra.aliases(src) {
		ra.copyAliasedFrom(src, false)
		return
	}

	ra.releaseContainersInRange(0)

	if len(src.keys) == 0 {
		ra.setSize(0)
		return
	}

	ra.setSize(len(src.keys))
	copy(ra.keys, src.keys)
	for i := range src.containers {
		ra.containers[i] = src.containers[i].clone()
	}
}

func (ra *containerIndex) copySharedFrom(src *containerIndex) {
	if ra == src {
		return
	}
	if ra.aliases(src) {
		ra.copyAliasedFrom(src, true)
		return
	}

	ra.releaseContainersInRange(0)

	if len(src.keys) == 0 {
		ra.setSize(0)
		return
	}

	ra.setSize(len(src.keys))
	copy(ra.keys, src.keys)
	for i := range src.containers {
		ra.containers[i] = src.containers[i].retain()
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
	if current.uniquelyOwned() {
		return current
	}
	cloned := current.clone()
	current.release()
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
		ra.appendContainer(key, value)
		return
	}

	oldLen := len(ra.keys)
	ra.setSize(oldLen + 1)
	copy(ra.keys[i+1:], ra.keys[i:oldLen])
	copy(ra.containers[i+1:], ra.containers[i:oldLen])

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
	removed.release()
}

func (ra *containerIndex) setContainerAtIndex(i int, c container16) {
	if old := ra.containers[i]; old != nil && old != c {
		old.release()
	}
	ra.containers[i] = c
}

func (ra *containerIndex) replaceKeyAndContainerAtIndex(i int, key uint16, c container16) {
	ra.keys[i] = key
	if old := ra.containers[i]; old != nil && old != c {
		old.release()
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

type shortIterable interface {
	hasNext() bool
	next() uint16
}

type shortPeekable interface {
	shortIterable
	peekNext() uint16
	advanceIfNeeded(minval uint16)
	release()
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

func (*shortIterator) release() {}

type manyIterable interface {
	nextMany(hs uint32, buf []uint32) int
	nextMany64(hs uint64, buf []uint64) int
	release()
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

var bitmapPool = pooled.Pointers[bitmap32]{
	New: func() *bitmap32 {
		return new(bitmap32)
	},
	Init: func(rb *bitmap32) {
		rb.refs.Store(1)
	},
	Cleanup: func(rb *bitmap32) {
		rb.highlowcontainer.clear()
	},
}

var runContainerPool = pooled.Pointers[containerRun]{
	Init: func(rc *containerRun) {
		rc.refs.Store(1)
	},
	Cleanup: func(rc *containerRun) {
		clear(rc.iv)
		rc.iv = rc.iv[:0]
	},
}

// containerArrayPoolCapacities uses power-of-two classes so both acquire and
// release can classify storage in O(1) via bits.Len.
//
// The release path intentionally accepts containerArray instances whose backing
// capacity drifted at runtime after growth or storage swaps. Because of that,
// pooled classes must support bucketing by floor(capacity) without secondary
// validation on Get.
var containerArrayPoolCapacities = [...]int{
	32,
	64,
	128,
	256,
	512,
	1024,
	2048,
	4096,
	8192,
	16384,
}

// maxContainerArrayPoolCapacity limits which containerArray capacities are
// returned to the pool. Larger arrays are dropped instead of being kept in the
// common power-of-two classes.
var maxContainerArrayPoolCapacity = 4 * arrayDefaultMaxSize

const maxPooledRunContainerCapacity = 4 * arrayDefaultMaxSize

var containerArrayClassPools [len(containerArrayPoolCapacities)]pooled.Pointers[containerArray]

func init() {
	for i, maxcap := range containerIndexPoolCapacities {
		c := maxcap
		containerIndexStoragePools[i] = pooled.Pointers[containerIndexStorage]{
			New: func() *containerIndexStorage {
				return &containerIndexStorage{
					keys:       make([]uint16, 0, c),
					containers: make([]container16, 0, c),
				}
			},
			Cleanup: func(storage *containerIndexStorage) {
				clear(storage.keys[:cap(storage.keys)])
				storage.keys = storage.keys[:0]
				clear(storage.containers[:cap(storage.containers)])
				storage.containers = storage.containers[:0]
			},
		}
	}
	for i, maxcap := range containerArrayPoolCapacities {
		c := maxcap
		containerArrayClassPools[i] = pooled.Pointers[containerArray]{
			New: func() *containerArray {
				return &containerArray{
					content: make([]uint16, 0, c),
				}
			},
			Init: func(ac *containerArray) {
				ac.refs.Store(1)
			},
			Cleanup: func(ac *containerArray) {
				clear(ac.content)
				ac.content = ac.content[:0]
			},
		}
	}
}

// containerArrayPoolIndex returns the smallest pooled class that can satisfy
// size. Requests round up so objects taken from a class always have enough
// capacity without an extra check on the Get path.
func containerArrayPoolIndex(size int) int {
	if size <= containerArrayPoolCapacities[0] {
		return 0
	}
	if size > maxContainerArrayPoolCapacity {
		return -1
	}
	return bits.Len(uint(size-1)) - 5
}

// containerArrayPoolPutIndex returns the largest pooled class not exceeding
// capacity. Release rounds down because a containerArray may come back with a
// reshaped backing array, and the pool must never advertise more capacity than
// the stored slice still has.
func containerArrayPoolPutIndex(capacity int) int {
	if capacity < containerArrayPoolCapacities[0] || capacity > maxContainerArrayPoolCapacity {
		return -1
	}
	return bits.Len(uint(capacity)) - 6
}

func getContainerArray() *containerArray {
	return containerArrayClassPools[0].Get()
}

func getContainerArrayWithCap(c int) *containerArray {
	if c <= 0 {
		return getContainerArray()
	}
	if idx := containerArrayPoolIndex(c); idx >= 0 {
		return containerArrayClassPools[idx].Get()
	}
	ac := &containerArray{
		content: make([]uint16, 0, c),
	}
	ac.refs.Store(1)
	return ac
}

func getContainerArrayWithLen(l int) *containerArray {
	if l <= 0 {
		return getContainerArray()
	}
	ac := getContainerArrayWithCap(l)
	ac.content = ac.content[:l]
	return ac
}

func getContainerArrayFromSlice(src []uint16) *containerArray {
	if len(src) == 0 {
		return getContainerArray()
	}
	ac := getContainerArrayWithLen(len(src))
	copy(ac.content, src)
	return ac
}

func replaceContainerArrayStorage(ac, donor *containerArray) {
	oldContent := ac.content
	ac.content = donor.content
	donor.content = oldContent
	donor.release()
}

func replaceContainerRunStorage(rc, donor *containerRun) {
	oldIV := rc.iv
	rc.iv = donor.iv
	donor.iv = oldIV
	donor.release()
}

var (
	intIteratorPool = pooled.Pointers[intIterator]{
		Clear: true,
	}
	manyIntIteratorPool = pooled.Pointers[manyIntIterator]{
		Clear: true,
	}
)

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
