package roaring

import (
	"encoding/binary"
	"fmt"
	"io"
)

type container interface {
	clone() container
	and(container) container
	andCardinality(container) int
	iand(container) container
	andNot(container) container
	iandNot(container) container
	isEmpty() bool
	getCardinality() int

	iadd(x uint16) bool
	iaddReturnMinimized(uint16) container
	iaddRange(start, endx int) container

	iremove(x uint16) bool
	iremoveReturnMinimized(uint16) container

	not(start, final int) container
	inot(firstOfRange, endx int) container
	xor(r container) container
	getShortIterator() shortPeekable
	iterate(cb func(x uint16) bool) bool
	getManyIterator() manyIterable
	contains(i uint16) bool
	maximum() uint16
	minimum() uint16

	equals(r container) bool

	fillLeastSignificant16bits(array []uint32, i int, mask uint32) int
	or(r container) container
	isFull() bool
	ior(r container) container
	intersects(r container) bool
	getSizeInBytes() int
	iremoveRange(start, final int) container
	writeTo(io.Writer) (int, error)

	numberOfRuns() int
	toEfficientContainer() container
}

type roaringArray struct {
	keys       []uint16
	containers []container `msg:"-"`
}

func rangeOfOnes(start, last int) container {
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
	return newRunContainer16Range(uint16(start), uint16(last)).toEfficientContainer()
}

func (ra *roaringArray) runOptimize() {
	for i := range ra.containers {
		ra.containers[i] = ra.containers[i].toEfficientContainer()
	}
}

func (ra *roaringArray) appendContainer(key uint16, value container) {
	ra.keys = append(ra.keys, key)
	ra.containers = append(ra.containers, value)
}

func (ra *roaringArray) appendCopy(sa roaringArray, startingindex int) {
	ra.appendContainer(sa.keys[startingindex], sa.containers[startingindex].clone())
}

func (ra *roaringArray) appendCopyMany(sa roaringArray, startingindex, end int) {
	for i := startingindex; i < end; i++ {
		ra.appendCopy(sa, i)
	}
}

func (ra *roaringArray) resize(newsize int) {
	for k := newsize; k < len(ra.containers); k++ {
		ra.keys[k] = 0
		ra.containers[k] = nil
	}
	ra.keys = ra.keys[:newsize]
	ra.containers = ra.containers[:newsize]
}

func (ra *roaringArray) clear() {
	ra.releaseContainersInRange(0)
	ra.resize(0)
}

func (ra *roaringArray) clearContainerAtIndex(i int) {
	ra.keys[i] = 0
	ra.containers[i] = nil
}

func (ra *roaringArray) releaseContainersInRange(start int) {
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

func (ra *roaringArray) clone() *roaringArray {
	sa := roaringArray{}
	sa.keys = make([]uint16, len(ra.keys))
	copy(sa.keys, ra.keys)

	sa.containers = make([]container, len(ra.containers))
	for i := range sa.containers {
		sa.containers[i] = ra.containers[i].clone()
	}
	return &sa
}

func (ra *roaringArray) getContainer(x uint16) container {
	i := ra.binarySearch(0, int64(len(ra.keys)), x)
	if i < 0 {
		return nil
	}
	return ra.containers[i]
}

func (ra *roaringArray) getContainerAtIndex(i int) container {
	return ra.containers[i]
}

func (ra *roaringArray) getUnionedWritableContainer(pos int, other container) container {
	return ra.containers[pos].ior(other)
}

func (ra *roaringArray) getWritableContainerAtIndex(i int) container {
	return ra.containers[i]
}

func (ra *roaringArray) getIndex(x uint16) int {
	size := len(ra.keys)
	if size == 0 || ra.keys[size-1] == x {
		return size - 1
	}
	return ra.binarySearch(0, int64(size), x)
}

func (ra *roaringArray) getKeyAtIndex(i int) uint16 {
	return ra.keys[i]
}

func (ra *roaringArray) insertNewKeyValueAt(i int, key uint16, value container) {
	ra.keys = append(ra.keys, 0)
	ra.containers = append(ra.containers, nil)

	copy(ra.keys[i+1:], ra.keys[i:])
	copy(ra.containers[i+1:], ra.containers[i:])

	ra.keys[i] = key
	ra.containers[i] = value
}

func (ra *roaringArray) removeAtIndex(i int) {
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

func (ra *roaringArray) setContainerAtIndex(i int, c container) {
	if old := ra.containers[i]; old != nil && old != c {
		releaseContainer(old)
	}
	ra.containers[i] = c
}

func (ra *roaringArray) replaceKeyAndContainerAtIndex(i int, key uint16, c container) {
	ra.keys[i] = key
	if old := ra.containers[i]; old != nil && old != c {
		releaseContainer(old)
	}
	ra.containers[i] = c
}

func (ra *roaringArray) size() int {
	return len(ra.keys)
}

func (ra *roaringArray) binarySearch(begin, end int64, ikey uint16) int {
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

func (ra *roaringArray) equals(o interface{}) bool {
	srb, ok := o.(roaringArray)
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

func (ra *roaringArray) writeTo(w io.Writer) (n int64, err error) {
	hasRun := ra.hasRunCompression()
	isRunSizeInBytes := 0
	cookieSize := 8
	if hasRun {
		cookieSize = 4
		isRunSizeInBytes = (len(ra.keys) + 7) / 8
	}
	descriptiveHeaderSize := 4 * len(ra.keys)
	preambleSize := cookieSize + isRunSizeInBytes + descriptiveHeaderSize

	buf := make([]byte, preambleSize+4*len(ra.keys))
	nw := 0

	if hasRun {
		binary.LittleEndian.PutUint16(buf[0:], uint16(serialCookie))
		nw += 2
		binary.LittleEndian.PutUint16(buf[2:], uint16(len(ra.keys)-1))
		nw += 2
		runbitmapslice := buf[nw : nw+isRunSizeInBytes]
		for i, c := range ra.containers {
			switch c.(type) {
			case *runContainer16:
				runbitmapslice[i/8] |= 1 << (uint(i) % 8)
			}
		}
		nw += isRunSizeInBytes
	} else {
		binary.LittleEndian.PutUint32(buf[0:], uint32(serialCookieNoRunContainer))
		nw += 4
		binary.LittleEndian.PutUint32(buf[4:], uint32(len(ra.keys)))
		nw += 4
	}

	for i, key := range ra.keys {
		binary.LittleEndian.PutUint16(buf[nw:], key)
		nw += 2
		c := ra.containers[i]
		binary.LittleEndian.PutUint16(buf[nw:], uint16(c.getCardinality()-1))
		nw += 2
	}

	startOffset := int64(preambleSize + 4*len(ra.keys))
	if !hasRun || len(ra.keys) >= noOffsetThreshold {
		for _, c := range ra.containers {
			binary.LittleEndian.PutUint32(buf[nw:], uint32(startOffset))
			nw += 4
			switch rc := c.(type) {
			case *runContainer16:
				startOffset += 2 + int64(len(rc.iv))*4
			default:
				startOffset += int64(getSizeInBytesFromCardinality(c.getCardinality()))
			}
		}
	}

	written, err := w.Write(buf[:nw])
	if err != nil {
		return n, err
	}
	n += int64(written)

	for _, c := range ra.containers {
		written, err := c.writeTo(w)
		if err != nil {
			return n, err
		}
		n += int64(written)
	}
	return n, nil
}

func (ra *roaringArray) readFrom(stream ByteInput, cookieHeader ...byte) (int64, error) {
	var cookie uint32
	var err error
	if len(cookieHeader) > 0 && len(cookieHeader) != 4 {
		return int64(len(cookieHeader)), fmt.Errorf("error in roaringArray.readFrom: could not read initial cookie: incorrect size of cookie header")
	}
	if len(cookieHeader) == 4 {
		cookie = binary.LittleEndian.Uint32(cookieHeader)
	} else {
		cookie, err = stream.ReadUInt32()
		if err != nil {
			return stream.GetReadBytes(), fmt.Errorf("error in roaringArray.readFrom: could not read initial cookie: %s", err)
		}
	}

	var size uint32
	var isRunBitmap []byte

	if cookie&0x0000FFFF == serialCookie {
		size = uint32(cookie>>16 + 1)
		isRunBitmapSize := (int(size) + 7) / 8
		isRunBitmap, err = stream.Next(isRunBitmapSize)
		if err != nil {
			return stream.GetReadBytes(), fmt.Errorf("malformed bitmap, failed to read is-run bitmap, got: %s", err)
		}
	} else if cookie == serialCookieNoRunContainer {
		size, err = stream.ReadUInt32()
		if err != nil {
			return stream.GetReadBytes(), fmt.Errorf("malformed bitmap, failed to read a bitmap size: %s", err)
		}
	} else {
		return stream.GetReadBytes(), fmt.Errorf("error in roaringArray.readFrom: did not find expected serialCookie in header")
	}

	if size > 1<<16 {
		return stream.GetReadBytes(), fmt.Errorf("it is logically impossible to have more than (1<<16) containers")
	}

	ra.clear()

	buf, err := stream.Next(2 * 2 * int(size))
	if err != nil {
		return stream.GetReadBytes(), fmt.Errorf("failed to read descriptive header: %s", err)
	}
	safeSlice := stream.NextReturnsSafeSlice()
	keycard := byteSliceAsUint16Slice(buf)
	if !safeSlice {
		keycardBuf := newArrayContainerFromSlice(keycard)
		defer releaseArrayContainer(keycardBuf)
		keycard = keycardBuf.content
	}

	if isRunBitmap == nil || size >= noOffsetThreshold {
		if err := stream.SkipBytes(int(size) * 4); err != nil {
			return stream.GetReadBytes(), fmt.Errorf("failed to skip bytes: %s", err)
		}
	}

	if cap(ra.containers) >= int(size) {
		ra.containers = ra.containers[:size]
	} else {
		ra.containers = make([]container, size)
	}

	if cap(ra.keys) >= int(size) {
		ra.keys = ra.keys[:size]
	} else {
		ra.keys = make([]uint16, size)
	}

	for i := uint32(0); i < size; i++ {
		key := keycard[2*i]
		card := int(keycard[2*i+1]) + 1
		ra.keys[i] = key

		if isRunBitmap != nil && isRunBitmap[i/8]&(1<<(i%8)) != 0 {
			nr, err := stream.ReadUInt16()
			if err != nil {
				return 0, fmt.Errorf("failed to read runtime container size: %s", err)
			}

			buf, err := stream.Next(int(nr) * 4)
			if err != nil {
				return stream.GetReadBytes(), fmt.Errorf("failed to read runtime container content: %s", err)
			}

			iv := byteSliceAsInterval16Slice(buf)
			if !safeSlice {
				iv = append([]interval16(nil), iv...)
			}
			ra.containers[i] = &runContainer16{iv: iv}
		} else if card > arrayDefaultMaxSize {
			buf, err := stream.Next(arrayDefaultMaxSize * 2)
			if err != nil {
				return stream.GetReadBytes(), fmt.Errorf("failed to read bitmap container: %s", err)
			}

			bitmap := byteSliceAsUint64Slice(buf)
			if !safeSlice {
				bitmap = append([]uint64(nil), bitmap...)
			}
			ra.containers[i] = &bitmapContainer{
				cardinality: card,
				bitmap:      bitmap,
			}
		} else {
			buf, err := stream.Next(card * 2)
			if err != nil {
				return stream.GetReadBytes(), fmt.Errorf("failed to read array container: %s", err)
			}

			content := byteSliceAsUint16Slice(buf)
			if !safeSlice {
				ra.containers[i] = newArrayContainerFromSlice(content)
				continue
			}
			ra.containers[i] = &arrayContainer{content: content}
		}
	}

	return stream.GetReadBytes(), nil
}

func (ra *roaringArray) hasRunCompression() bool {
	for _, c := range ra.containers {
		switch c.(type) {
		case *runContainer16:
			return true
		}
	}
	return false
}

func (ra *roaringArray) headerSize() uint64 {
	size := uint64(len(ra.keys))
	if ra.hasRunCompression() {
		if size < noOffsetThreshold {
			return 4 + (size+7)/8 + 4*size
		}
		return 4 + (size+7)/8 + 8*size
	}
	return 4 + 4 + 8*size
}

func (ra *roaringArray) serializedSizeInBytes() uint64 {
	answer := ra.headerSize()
	for _, c := range ra.containers {
		switch rc := c.(type) {
		case *runContainer16:
			answer += 2 + uint64(len(rc.iv))*4
		default:
			answer += uint64(getSizeInBytesFromCardinality(c.getCardinality()))
		}
	}
	return answer
}

// Find the smallest integer index larger than pos such that array[index].key >= min.
func (ra *roaringArray) advanceUntil(min uint16, pos int) int {
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
