package posting

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

const bitmapContainerWords = maxCapacity / 64

var bitmapContainerPool sync.Pool

type containerBitmap struct {
	refs        atomic.Int32
	cardinality int
	bitmap      []uint64
}

func bitmapOrSlice(dst, left, right []uint64) int {
	cardinality := 0
	for i := range dst {
		word := left[i] | right[i]
		dst[i] = word
		cardinality += int(popcount(word))
	}
	return cardinality
}

func bitmapAndSlice(dst, left, right []uint64) int {
	cardinality := 0
	for i := range dst {
		word := left[i] & right[i]
		dst[i] = word
		cardinality += int(popcount(word))
	}
	return cardinality
}

func bitmapXorSlice(dst, left, right []uint64) int {
	cardinality := 0
	for i := range dst {
		word := left[i] ^ right[i]
		dst[i] = word
		cardinality += int(popcount(word))
	}
	return cardinality
}

func bitmapAndNotSlice(dst, left, right []uint64) int {
	cardinality := 0
	for i := range dst {
		word := left[i] &^ right[i]
		dst[i] = word
		cardinality += int(popcount(word))
	}
	return cardinality
}

func appendBitmapWordValues(dst []uint16, pos int, word uint64, base int) int {
	for word != 0 {
		t := word & -word
		dst[pos] = uint16(base + countTrailingZeros(t))
		pos++
		word ^= t
	}
	return pos
}

func fillArrayBitmapAndRun(dst []uint16, bitmap []uint64, iv []interval16) int {
	const allones = ^uint64(0)

	pos := 0
	for i := range iv {
		start := int(iv[i].start)
		end := int(iv[i].last()) + 1
		firstword := start / 64
		endword := (end - 1) / 64
		if firstword == endword {
			mask := (allones << uint(start%64)) & (allones >> (uint(-end) % 64))
			pos = appendBitmapWordValues(dst, pos, bitmap[firstword]&mask, firstword*64)
			continue
		}
		pos = appendBitmapWordValues(dst, pos, bitmap[firstword]&(allones<<uint(start%64)), firstword*64)
		for word := firstword + 1; word < endword; word++ {
			pos = appendBitmapWordValues(dst, pos, bitmap[word], word*64)
		}
		pos = appendBitmapWordValues(dst, pos, bitmap[endword]&(allones>>(uint(-end)%64)), endword*64)
	}
	return pos
}

func fillBitmapAndRun(dst, src []uint64, iv []interval16) {
	const allones = ^uint64(0)

	for i := range iv {
		start := int(iv[i].start)
		end := int(iv[i].last()) + 1
		firstword := start / 64
		endword := (end - 1) / 64
		if firstword == endword {
			mask := (allones << uint(start%64)) & (allones >> (uint(-end) % 64))
			dst[firstword] |= src[firstword] & mask
			continue
		}
		dst[firstword] |= src[firstword] & (allones << uint(start%64))
		for word := firstword + 1; word < endword; word++ {
			dst[word] = src[word]
		}
		dst[endword] |= src[endword] & (allones >> (uint(-end) % 64))
	}
}

func flipBitmapRange(bitmap []uint64, start int, end int) {
	if start >= end {
		return
	}
	firstword := start / 64
	endword := (end - 1) / 64
	bitmap[firstword] ^= ^(^uint64(0) << uint(start%64))
	for i := firstword; i < endword; i++ {
		bitmap[i] = ^bitmap[i]
	}
	bitmap[endword] ^= ^uint64(0) >> (uint(-end) % 64)
}

func resetBitmapRange(bitmap []uint64, start int, end int) {
	if start >= end {
		return
	}
	firstword := start / 64
	endword := (end - 1) / 64
	if firstword == endword {
		bitmap[firstword] &= ^((^uint64(0) << uint(start%64)) & (^uint64(0) >> (uint(-end) % 64)))
		return
	}
	bitmap[firstword] &= ^(^uint64(0) << uint(start%64))
	clear(bitmap[firstword+1 : endword])
	bitmap[endword] &= ^(^uint64(0) >> (uint(-end) % 64))
}

func setBitmapRange(bitmap []uint64, start int, end int) {
	if start >= end {
		return
	}
	firstword := start / 64
	endword := (end - 1) / 64
	if firstword == endword {
		bitmap[firstword] |= (^uint64(0) << uint(start%64)) & (^uint64(0) >> (uint(-end) % 64))
		return
	}
	bitmap[firstword] |= ^uint64(0) << uint(start%64)
	for i := firstword + 1; i < endword; i++ {
		bitmap[i] = ^uint64(0)
	}
	bitmap[endword] |= ^uint64(0) >> (uint(-end) % 64)
}

func flipBitmapRangeAndCardinalityChange(bitmap []uint64, start int, end int) int {
	if start >= end {
		return 0
	}
	firstword := start / 64
	endword := (end - 1) / 64
	if firstword == endword {
		mask := (^uint64(0) << uint(start%64)) & (^uint64(0) >> (uint(-end) % 64))
		word := bitmap[firstword]
		bitmap[firstword] = word ^ mask
		return int(popcount(mask)) - 2*int(popcount(word&mask))
	}

	delta := 0
	firstMask := ^uint64(0) << uint(start%64)
	firstWord := bitmap[firstword]
	bitmap[firstword] = firstWord ^ firstMask
	delta += int(popcount(firstMask)) - 2*int(popcount(firstWord&firstMask))

	for i := firstword + 1; i < endword; i++ {
		word := bitmap[i]
		bitmap[i] = ^word
		delta += 64 - 2*int(popcount(word))
	}

	lastMask := ^uint64(0) >> (uint(-end) % 64)
	lastWord := bitmap[endword]
	bitmap[endword] = lastWord ^ lastMask
	delta += int(popcount(lastMask)) - 2*int(popcount(lastWord&lastMask))
	return delta
}

func resetBitmapRangeAndCardinalityChange(bitmap []uint64, start int, end int) int {
	if start >= end {
		return 0
	}
	firstword := start / 64
	endword := (end - 1) / 64
	if firstword == endword {
		mask := (^uint64(0) << uint(start%64)) & (^uint64(0) >> (uint(-end) % 64))
		word := bitmap[firstword]
		bitmap[firstword] = word &^ mask
		return -int(popcount(word & mask))
	}

	delta := 0
	firstMask := ^uint64(0) << uint(start%64)
	firstWord := bitmap[firstword]
	bitmap[firstword] = firstWord &^ firstMask
	delta -= int(popcount(firstWord & firstMask))

	for i := firstword + 1; i < endword; i++ {
		word := bitmap[i]
		bitmap[i] = 0
		delta -= int(popcount(word))
	}

	lastMask := ^uint64(0) >> (uint(-end) % 64)
	lastWord := bitmap[endword]
	bitmap[endword] = lastWord &^ lastMask
	delta -= int(popcount(lastWord & lastMask))
	return delta
}

func setBitmapRangeAndCardinalityChange(bitmap []uint64, start int, end int) int {
	if start >= end {
		return 0
	}
	firstword := start / 64
	endword := (end - 1) / 64
	if firstword == endword {
		mask := (^uint64(0) << uint(start%64)) & (^uint64(0) >> (uint(-end) % 64))
		word := bitmap[firstword]
		bitmap[firstword] = word | mask
		return int(popcount(mask &^ word))
	}

	delta := 0
	firstMask := ^uint64(0) << uint(start%64)
	firstWord := bitmap[firstword]
	bitmap[firstword] = firstWord | firstMask
	delta += int(popcount(firstMask &^ firstWord))

	for i := firstword + 1; i < endword; i++ {
		word := bitmap[i]
		bitmap[i] = ^uint64(0)
		delta += 64 - int(popcount(word))
	}

	lastMask := ^uint64(0) >> (uint(-end) % 64)
	lastWord := bitmap[endword]
	bitmap[endword] = lastWord | lastMask
	delta += int(popcount(lastMask &^ lastWord))
	return delta
}

func newContainerBitmap() *containerBitmap {
	if v := bitmapContainerPool.Get(); v != nil {
		bc := v.(*containerBitmap)
		bc.refs.Store(1)
		bc.cardinality = 0
		return bc
	}
	bc := &containerBitmap{
		bitmap: make([]uint64, bitmapContainerWords),
	}
	bc.refs.Store(1)
	return bc
}

func releaseContainerBitmap(bc *containerBitmap) {
	if bc == nil {
		return
	}
	if bc.refs.Add(-1) != 0 {
		return
	}
	if len(bc.bitmap) != bitmapContainerWords {
		return
	}
	bc.cardinality = 0
	clear(bc.bitmap)
	bitmapContainerPool.Put(bc)
}

func releaseTempBitmapUnlessReturned(temp *containerBitmap, result container16) {
	if temp == nil {
		return
	}
	if returned, ok := result.(*containerBitmap); ok && returned == temp {
		return
	}
	releaseContainerBitmap(temp)
}

func efficientContainerFromTempBitmap(temp *containerBitmap) container16 {
	if temp == nil {
		return nil
	}
	result := temp.toEfficientContainer()
	releaseTempBitmapUnlessReturned(temp, result)
	return result
}

func newContainerBitmapWithRange(firstOfRun, lastOfRun int) *containerBitmap {
	bc := newContainerBitmap()
	bc.cardinality = lastOfRun - firstOfRun + 1
	if bc.cardinality == maxCapacity {
		fill(bc.bitmap, uint64(0xffffffffffffffff))
	} else {
		firstWord := firstOfRun / 64
		lastWord := lastOfRun / 64
		zeroPrefixLength := uint64(firstOfRun & 63)
		zeroSuffixLength := uint64(63 - (lastOfRun & 63))

		fillRange(bc.bitmap, firstWord, lastWord+1, uint64(0xffffffffffffffff))
		bc.bitmap[firstWord] ^= (uint64(1) << zeroPrefixLength) - 1
		blockOfOnes := (uint64(1) << zeroSuffixLength) - 1
		maskOnLeft := blockOfOnes << (uint64(64) - zeroSuffixLength)
		bc.bitmap[lastWord] ^= maskOnLeft
	}
	return bc
}

func (bc *containerBitmap) minimum() uint16 {
	for i := 0; i < len(bc.bitmap); i++ {
		w := bc.bitmap[i]
		if w != 0 {
			r := countTrailingZeros(w)
			return uint16(r + i*64)
		}
	}
	return MaxUint16
}

// i should be non-zero
func clz(i uint64) int {
	n := 1
	x := uint32(i >> 32)
	if x == 0 {
		n += 32
		x = uint32(i)
	}
	if x>>16 == 0 {
		n += 16
		x = x << 16
	}
	if x>>24 == 0 {
		n += 8
		x = x << 8
	}
	if x>>28 == 0 {
		n += 4
		x = x << 4
	}
	if x>>30 == 0 {
		n += 2
		x = x << 2
	}
	return n - int(x>>31)
}

func (bc *containerBitmap) maximum() uint16 {
	for i := len(bc.bitmap); i > 0; i-- {
		w := bc.bitmap[i-1]
		if w != 0 {
			r := clz(w)
			return uint16((i-1)*64 + 63 - r)
		}
	}
	return uint16(0)
}

func (bc *containerBitmap) iterate(cb func(x uint16) bool) bool {
	iterator := bitmapContainerShortIterator{bc, bc.nextSetBit(0)}

	for iterator.hasNext() {
		if !cb(iterator.next()) {
			return false
		}
	}

	return true
}

type bitmapContainerShortIterator struct {
	ptr *containerBitmap
	i   int
}

func (bcsi *bitmapContainerShortIterator) next() uint16 {
	j := bcsi.i
	bcsi.i = bcsi.ptr.nextSetBit(uint(bcsi.i) + 1)
	return uint16(j)
}

func (bcsi *bitmapContainerShortIterator) hasNext() bool {
	return bcsi.i >= 0
}

func (bcsi *bitmapContainerShortIterator) peekNext() uint16 {
	return uint16(bcsi.i)
}

func (bcsi *bitmapContainerShortIterator) advanceIfNeeded(minval uint16) {
	if bcsi.hasNext() && bcsi.peekNext() < minval {
		bcsi.i = bcsi.ptr.nextSetBit(uint(minval))
	}
}

func newContainerBitmapShortIterator(a *containerBitmap) *bitmapContainerShortIterator {
	return &bitmapContainerShortIterator{a, a.nextSetBit(0)}
}

func (bc *containerBitmap) getShortIterator() shortPeekable {
	return newContainerBitmapShortIterator(bc)
}

type bitmapContainerManyIterator struct {
	ptr    *containerBitmap
	base   int
	bitset uint64
}

func (bcmi *bitmapContainerManyIterator) nextMany(hs uint32, buf []uint32) int {
	n := 0
	base := bcmi.base
	bitset := bcmi.bitset

	for n < len(buf) {
		if bitset == 0 {
			base++
			if base >= len(bcmi.ptr.bitmap) {
				bcmi.base = base
				bcmi.bitset = bitset
				return n
			}
			bitset = bcmi.ptr.bitmap[base]
			continue
		}
		t := bitset & -bitset
		buf[n] = uint32((base*64)+int(popcount(t-1))) | hs
		n = n + 1
		bitset ^= t
	}

	bcmi.base = base
	bcmi.bitset = bitset
	return n
}

// nextMany64 returns the number of values added to the buffer
func (bcmi *bitmapContainerManyIterator) nextMany64(hs uint64, buf []uint64) int {
	n := 0
	base := bcmi.base
	bitset := bcmi.bitset

	for n < len(buf) {
		if bitset == 0 {
			base++
			if base >= len(bcmi.ptr.bitmap) {
				bcmi.base = base
				bcmi.bitset = bitset
				return n
			}
			bitset = bcmi.ptr.bitmap[base]
			continue
		}
		t := bitset & -bitset
		buf[n] = uint64((base*64)+int(popcount(t-1))) | hs
		n = n + 1
		bitset ^= t
	}

	bcmi.base = base
	bcmi.bitset = bitset
	return n
}

func newContainerBitmapManyIterator(a *containerBitmap) *bitmapContainerManyIterator {
	return &bitmapContainerManyIterator{a, -1, 0}
}

func (bc *containerBitmap) getManyIterator() manyIterable {
	return newContainerBitmapManyIterator(bc)
}

func (bc *containerBitmap) getSizeInBytes() int {
	return len(bc.bitmap) * 8
}

const bcBaseBytes = int(unsafe.Sizeof(containerBitmap{}))

// containerBitmap doesn't depend on card, always fully allocated
func containerBitmapSizeInBytes() int {
	return bcBaseBytes + (1<<16)/8
}

func bitmapEquals(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func (bc *containerBitmap) fillLeastSignificant16bits(x []uint32, i int, mask uint32) int {
	pos := i
	base := mask
	for k := 0; k < len(bc.bitmap); k++ {
		bitset := bc.bitmap[k]
		for bitset != 0 {
			t := bitset & -bitset
			x[pos] = base + uint32(popcount(t-1))
			pos++
			bitset ^= t
		}
		base += 64
	}
	return pos
}

func (bc *containerBitmap) equals(o container16) bool {
	switch other := o.(type) {
	case *containerBitmap:
		if other.cardinality != bc.cardinality {
			return false
		}
		return bitmapEquals(bc.bitmap, other.bitmap)
	case *containerArray:
		return equalArrayBitmap(other, bc)
	case *containerRun:
		if bc.cardinality != other.getCardinality() {
			return false
		}
		bit := bitmapContainerShortIterator{ptr: bc, i: bc.nextSetBit(0)}
		rit := runIterator16{rc: other}
		for rit.hasNext() {
			if bit.next() != rit.next() {
				return false
			}
		}
		return true
	}
	panic("unsupported container16 type")
}

func (bc *containerBitmap) iaddReturnMinimized(i uint16) container16 {
	bc.iadd(i)
	if bc.isFull() {
		return newContainerRunRange(0, MaxUint16)
	}
	return bc
}

// iadd adds the arg i, returning true if not already present
func (bc *containerBitmap) iadd(i uint16) bool {
	x := int(i)
	previous := bc.bitmap[x/64]
	mask := uint64(1) << (uint(x) % 64)
	newb := previous | mask
	bc.bitmap[x/64] = newb
	bc.cardinality += int((previous ^ newb) >> (uint(x) % 64))
	return newb != previous
}

func (bc *containerBitmap) iremoveReturnMinimized(i uint16) container16 {
	if bc.iremove(i) {
		if bc.cardinality == arrayDefaultMaxSize {
			return bc.toArrayContainer()
		}
	}
	return bc
}

// iremove returns true if i was found.
func (bc *containerBitmap) iremove(i uint16) bool {
	if bc.contains(i) {
		bc.cardinality--
		bc.bitmap[i/64] &^= uint64(1) << (i % 64)
		return true
	}
	return false
}

func (bc *containerBitmap) isFull() bool {
	return bc.cardinality == int(MaxUint16)+1
}

func (bc *containerBitmap) getCardinality() int {
	return bc.cardinality
}

func (bc *containerBitmap) isEmpty() bool {
	return bc.cardinality == 0
}

func (bc *containerBitmap) clone() container16 {
	ptr := newContainerBitmap()
	ptr.cardinality = bc.cardinality
	copy(ptr.bitmap, bc.bitmap)
	return ptr
}

// add all values in range [firstOfRange,lastOfRange)
func (bc *containerBitmap) iaddRange(firstOfRange, lastOfRange int) container16 {
	bc.cardinality += setBitmapRangeAndCardinalityChange(bc.bitmap, firstOfRange, lastOfRange)
	return bc
}

// remove all values in range [firstOfRange,lastOfRange)
func (bc *containerBitmap) iremoveRange(firstOfRange, lastOfRange int) container16 {
	bc.cardinality += resetBitmapRangeAndCardinalityChange(bc.bitmap, firstOfRange, lastOfRange)
	if bc.getCardinality() <= arrayDefaultMaxSize {
		return bc.toArrayContainer()
	}
	return bc
}

// flip all values in range [firstOfRange,endx)
func (bc *containerBitmap) inot(firstOfRange, endx int) container16 {
	if endx-firstOfRange == maxCapacity {
		flipBitmapRange(bc.bitmap, firstOfRange, endx)
		bc.cardinality = maxCapacity - bc.cardinality
	} else {
		bc.cardinality += flipBitmapRangeAndCardinalityChange(bc.bitmap, firstOfRange, endx)
	}
	if bc.getCardinality() <= arrayDefaultMaxSize {
		return bc.toArrayContainer()
	}
	return bc
}

// flip all values in range [firstOfRange,endx)
func (bc *containerBitmap) not(firstOfRange, endx int) container16 {
	answer := bc.clone()
	return answer.inot(firstOfRange, endx)
}

func (bc *containerBitmap) or(a container16) container16 {
	switch x := a.(type) {
	case *containerArray:
		return bc.orArray(x)
	case *containerBitmap:
		return bc.orBitmap(x)
	case *containerRun:
		if x.isFull() {
			return x.clone()
		}
		return x.orBitmap(bc)
	}
	panic("unsupported container16 type")
}

func (bc *containerBitmap) ior(a container16) container16 {
	switch x := a.(type) {
	case *containerArray:
		return bc.iorArray(x)
	case *containerBitmap:
		return bc.iorBitmap(x)
	case *containerRun:
		if x.isFull() {
			return x.clone()
		}
		for i := range x.iv {
			bc.iaddRange(int(x.iv[i].start), int(x.iv[i].last())+1)
		}
		if bc.isFull() {
			return newContainerRunRange(0, MaxUint16)
		}
		// bc.computeCardinality()
		return bc
	}
	panic("unsupported container16 type")
}

func (bc *containerBitmap) orArray(value2 *containerArray) container16 {
	answer := bc.clone().(*containerBitmap)
	c := value2.getCardinality()
	for k := 0; k < c; k++ {
		v := value2.content[k]
		i := uint(v) >> 6
		bef := answer.bitmap[i]
		aft := bef | (uint64(1) << (v % 64))
		answer.bitmap[i] = aft
		answer.cardinality += int((bef - aft) >> 63)
	}
	return answer
}

func (bc *containerBitmap) orBitmap(value2 *containerBitmap) container16 {
	answer := newContainerBitmap()
	answer.cardinality = bitmapOrSlice(answer.bitmap, bc.bitmap, value2.bitmap)
	if answer.isFull() {
		return newContainerRunRange(0, MaxUint16)
	}
	return answer
}

func (bc *containerBitmap) andBitmapCardinality(value2 *containerBitmap) int {
	return int(popcntAndSlice(bc.bitmap, value2.bitmap))
}

func (bc *containerBitmap) computeCardinality() {
	bc.cardinality = int(popcntSlice(bc.bitmap))
}

func (bc *containerBitmap) iorArray(ac *containerArray) container16 {
	for k := range ac.content {
		vc := ac.content[k]
		i := uint(vc) >> 6
		bef := bc.bitmap[i]
		aft := bef | (uint64(1) << (vc % 64))
		bc.bitmap[i] = aft
		bc.cardinality += int((bef - aft) >> 63)
	}
	if bc.isFull() {
		return newContainerRunRange(0, MaxUint16)
	}
	return bc
}

func (bc *containerBitmap) iorBitmap(value2 *containerBitmap) container16 {
	bc.cardinality = bitmapOrSlice(bc.bitmap, bc.bitmap, value2.bitmap)
	if bc.isFull() {
		return newContainerRunRange(0, MaxUint16)
	}
	return bc
}

func (bc *containerBitmap) xor(a container16) container16 {
	switch x := a.(type) {
	case *containerArray:
		return bc.xorArray(x)
	case *containerBitmap:
		return bc.xorBitmap(x)
	case *containerRun:
		return x.xorBitmap(bc)
	}
	panic("unsupported container16 type")
}

func (bc *containerBitmap) xorArray(value2 *containerArray) container16 {
	answer := bc.clone().(*containerBitmap)
	c := value2.getCardinality()
	for k := 0; k < c; k++ {
		vc := value2.content[k]
		index := uint(vc) >> 6
		abi := answer.bitmap[index]
		mask := uint64(1) << (vc % 64)
		answer.cardinality += 1 - 2*int((abi&mask)>>(vc%64))
		answer.bitmap[index] = abi ^ mask
	}
	if answer.cardinality <= arrayDefaultMaxSize {
		return answer.toArrayContainer()
	}
	return answer
}

func (bc *containerBitmap) xorBitmap(value2 *containerBitmap) container16 {
	answer := newContainerBitmap()
	answer.cardinality = bitmapXorSlice(answer.bitmap, bc.bitmap, value2.bitmap)
	if answer.cardinality > arrayDefaultMaxSize {
		if answer.isFull() {
			return newContainerRunRange(0, MaxUint16)
		}
		return answer
	}
	ac := newContainerArrayFromBitmap(answer)
	releaseContainerBitmap(answer)
	return ac
}

func (bc *containerBitmap) and(a container16) container16 {
	switch x := a.(type) {
	case *containerArray:
		return bc.andArray(x)
	case *containerBitmap:
		return bc.andBitmap(x)
	case *containerRun:
		return bc.andRun(x)
	}
	panic("unsupported container16 type")
}

func (bc *containerBitmap) andCardinality(a container16) int {
	switch x := a.(type) {
	case *containerArray:
		return bc.andArrayCardinality(x)
	case *containerBitmap:
		return bc.andBitmapCardinality(x)
	case *containerRun:
		return x.andBitmapCardinality(bc)
	}
	panic("unsupported container16 type")
}

func (bc *containerBitmap) intersects(a container16) bool {
	switch x := a.(type) {
	case *containerArray:
		return bc.intersectsArray(x)
	case *containerBitmap:
		return bc.intersectsBitmap(x)
	case *containerRun:
		return x.intersects(bc)

	}
	panic("unsupported container16 type")
}

func (bc *containerBitmap) iand(a container16) container16 {
	switch x := a.(type) {
	case *containerArray:
		return bc.iandArray(x)
	case *containerBitmap:
		return bc.iandBitmap(x)
	case *containerRun:
		if x.isFull() {
			return bc.clone()
		}
		return bc.iandRun(x)
	}
	panic("unsupported container16 type")
}

func (bc *containerBitmap) iandRun(rc *containerRun) container16 {
	return bc.andRun(rc)
}

func (bc *containerBitmap) iandArray(ac *containerArray) container16 {
	return bc.andArray(ac)
}

func (bc *containerBitmap) andRun(rc *containerRun) container16 {
	if bc.isEmpty() || rc.isEmpty() {
		return newContainerArray()
	}
	if rc.isFull() {
		return bc.clone()
	}

	cardinality := rc.andBitmapCardinality(bc)
	if cardinality <= arrayDefaultMaxSize {
		answer := newContainerArraySize(cardinality)
		answer.content = answer.content[:fillArrayBitmapAndRun(answer.content, bc.bitmap, rc.iv)]
		return answer
	}

	answer := newContainerBitmap()
	fillBitmapAndRun(answer.bitmap, bc.bitmap, rc.iv)
	answer.cardinality = cardinality
	return answer
}

func (bc *containerBitmap) andArray(value2 *containerArray) *containerArray {
	answer := newContainerArrayCapacity(len(value2.content))
	answer.content = answer.content[:cap(answer.content)]
	c := value2.getCardinality()
	pos := 0
	for k := 0; k < c; k++ {
		v := value2.content[k]
		answer.content[pos] = v
		pos += int(bc.bitValue(v))
	}
	answer.content = answer.content[:pos]
	return answer
}

func (bc *containerBitmap) andArrayCardinality(value2 *containerArray) int {
	c := value2.getCardinality()
	pos := 0
	for k := 0; k < c; k++ {
		v := value2.content[k]
		pos += int(bc.bitValue(v))
	}
	return pos
}

func (bc *containerBitmap) getCardinalityInRange(start, end uint) int {
	if start >= end {
		return 0
	}
	firstword := start / 64
	endword := (end - 1) / 64
	const allones = ^uint64(0)
	if firstword == endword {
		return int(popcount(bc.bitmap[firstword] & ((allones << (start % 64)) & (allones >> ((64 - end) & 63)))))
	}
	answer := popcount(bc.bitmap[firstword] & (allones << (start % 64)))
	answer += popcntSlice(bc.bitmap[firstword+1 : endword])
	answer += popcount(bc.bitmap[endword] & (allones >> ((64 - end) & 63)))
	return int(answer)
}

func (bc *containerBitmap) andBitmap(value2 *containerBitmap) container16 {
	answer := newContainerBitmap()
	answer.cardinality = bitmapAndSlice(answer.bitmap, bc.bitmap, value2.bitmap)
	if answer.cardinality > arrayDefaultMaxSize {
		return answer
	}
	ac := newContainerArrayFromBitmap(answer)
	releaseContainerBitmap(answer)
	return ac
}

func (bc *containerBitmap) intersectsArray(value2 *containerArray) bool {
	c := value2.getCardinality()
	for k := 0; k < c; k++ {
		v := value2.content[k]
		if bc.contains(v) {
			return true
		}
	}
	return false
}

func (bc *containerBitmap) intersectsBitmap(value2 *containerBitmap) bool {
	for k := 0; k < len(bc.bitmap); k++ {
		if (bc.bitmap[k] & value2.bitmap[k]) != 0 {
			return true
		}
	}
	return false
}

func (bc *containerBitmap) iandBitmap(value2 *containerBitmap) container16 {
	bc.cardinality = bitmapAndSlice(bc.bitmap, bc.bitmap, value2.bitmap)
	if bc.cardinality <= arrayDefaultMaxSize {
		return newContainerArrayFromBitmap(bc)
	}
	return bc
}

func (bc *containerBitmap) andNot(a container16) container16 {
	switch x := a.(type) {
	case *containerArray:
		return bc.andNotArray(x)
	case *containerBitmap:
		return bc.andNotBitmap(x)
	case *containerRun:
		return bc.andNotRun(x)
	}
	panic("unsupported container16 type")
}

func (bc *containerBitmap) andNotRun(rc *containerRun) container16 {
	answer := bc.clone().(*containerBitmap)
	return answer.iandNotRun(rc)
}

func (bc *containerBitmap) iandNot(a container16) container16 {
	switch x := a.(type) {
	case *containerArray:
		return bc.iandNotArray(x)
	case *containerBitmap:
		return bc.iandNotBitmapSurely(x)
	case *containerRun:
		return bc.iandNotRun(x)
	}
	panic("unsupported container16 type")
}

func (bc *containerBitmap) iandNotArray(ac *containerArray) container16 {
	if ac.isEmpty() || bc.isEmpty() {
		// Nothing to do.
		return bc
	}

	// Word by word, we remove the elements in ac from bc. The approach is to build
	// a mask of the elements to remove, and then apply it to the bitmap.
	wordIdx := uint16(0)
	mask := uint64(0)
	for i, v := range ac.content {
		if v/64 != wordIdx {
			// Flush the current word.
			if i != 0 {
				// We're removing bits that are set in the mask and in the current word.
				// To figure out the cardinality change, we count the number of bits that
				// are set in the mask and in the current word.
				mask &= bc.bitmap[wordIdx]
				bc.bitmap[wordIdx] &= ^mask
				bc.cardinality -= int(popcount(mask))
			}

			wordIdx = v / 64
			mask = 0
		}
		mask |= 1 << (v % 64)
	}

	// Flush the last word.
	mask &= bc.bitmap[wordIdx]
	bc.bitmap[wordIdx] &= ^mask
	bc.cardinality -= int(popcount(mask))

	if bc.getCardinality() <= arrayDefaultMaxSize {
		return bc.toArrayContainer()
	}
	return bc
}

func (bc *containerBitmap) iandNotRun(rc *containerRun) container16 {
	if rc.isEmpty() || bc.isEmpty() {
		// Nothing to do.
		return bc
	}

	for i := range rc.iv {
		bc.cardinality += resetBitmapRangeAndCardinalityChange(bc.bitmap, int(rc.iv[i].start), int(rc.iv[i].last())+1)
	}

	if bc.getCardinality() <= arrayDefaultMaxSize {
		return bc.toArrayContainer()
	}
	return bc
}

func (bc *containerBitmap) andNotArray(value2 *containerArray) container16 {
	answer := bc.clone().(*containerBitmap)
	c := value2.getCardinality()
	for k := 0; k < c; k++ {
		vc := value2.content[k]
		i := uint(vc) >> 6
		oldv := answer.bitmap[i]
		newv := oldv &^ (uint64(1) << (vc % 64))
		answer.bitmap[i] = newv
		answer.cardinality -= int((oldv ^ newv) >> (vc % 64))
	}
	if answer.cardinality <= arrayDefaultMaxSize {
		return answer.toArrayContainer()
	}
	return answer
}

func (bc *containerBitmap) andNotBitmap(value2 *containerBitmap) container16 {
	answer := newContainerBitmap()
	answer.cardinality = bitmapAndNotSlice(answer.bitmap, bc.bitmap, value2.bitmap)
	if answer.cardinality > arrayDefaultMaxSize {
		return answer
	}
	ac := newContainerArrayFromBitmap(answer)
	releaseContainerBitmap(answer)
	return ac
}

func (bc *containerBitmap) iandNotBitmapSurely(value2 *containerBitmap) container16 {
	bc.cardinality = bitmapAndNotSlice(bc.bitmap, bc.bitmap, value2.bitmap)
	if bc.getCardinality() <= arrayDefaultMaxSize {
		return bc.toArrayContainer()
	}
	return bc
}

func (bc *containerBitmap) contains(i uint16) bool { // testbit
	x := uint(i)
	w := bc.bitmap[x>>6]
	mask := uint64(1) << (x & 63)
	return (w & mask) != 0
}

func (bc *containerBitmap) bitValue(i uint16) uint64 {
	x := uint(i)
	w := bc.bitmap[x>>6]
	return (w >> (x & 63)) & 1
}

func (bc *containerBitmap) loadData(containerArray *containerArray) {
	bc.cardinality = containerArray.getCardinality()
	c := containerArray.getCardinality()
	for k := 0; k < c; k++ {
		x := containerArray.content[k]
		i := int(x) / 64
		bc.bitmap[i] |= uint64(1) << uint(x%64)
	}
}

func (bc *containerBitmap) toArrayContainer() *containerArray {
	ac := newContainerArray()
	ac.loadData(bc)
	return ac
}

func (bc *containerBitmap) fillArray(container16 []uint16) {
	pos := 0
	base := 0
	for k := 0; k < len(bc.bitmap); k++ {
		bitset := bc.bitmap[k]
		for bitset != 0 {
			t := bitset & -bitset
			container16[pos] = uint16(base + int(popcount(t-1)))
			pos = pos + 1
			bitset ^= t
		}
		base += 64
	}
}

// nextSetBit returns the next set bit e.g the next int packed into the bitmaparray
func (bc *containerBitmap) nextSetBit(i uint) int {
	var (
		x      = i / 64
		length = uint(len(bc.bitmap))
	)
	if x >= length {
		return -1
	}
	w := bc.bitmap[x]
	w = w >> (i % 64)
	if w != 0 {
		return int(i) + countTrailingZeros(w)
	}
	x++
	for ; x < length; x++ {
		if bc.bitmap[x] != 0 {
			return int(x*64) + countTrailingZeros(bc.bitmap[x])
		}
	}
	return -1
}

func (bc *containerBitmap) nextUnsetBit(i uint) int {
	var (
		x      = i / 64
		length = uint(len(bc.bitmap))
	)
	if x >= length {
		return int(i)
	}
	w := ^bc.bitmap[x]
	w &= ^uint64(0) << (i & 63)
	if w != 0 {
		return int(x*64) + countTrailingZeros(w)
	}
	x++
	for ; x < length; x++ {
		w = ^bc.bitmap[x]
		if w != 0 {
			return int(x*64) + countTrailingZeros(w)
		}
	}
	return int(length * 64)
}

// prevSetBit returns the previous set bit e.g the previous int packed into the bitmaparray
func (bc *containerBitmap) prevSetBit(i int) int {
	if i < 0 {
		return -1
	}

	return bc.uPrevSetBit(uint(i))
}

func (bc *containerBitmap) uPrevSetBit(i uint) int {
	var (
		x      = i >> 6
		length = uint(len(bc.bitmap))
	)

	if x >= length {
		return -1
	}

	w := bc.bitmap[x]

	b := i % 64

	w = w << (63 - b)
	if w != 0 {
		return int(i) - countLeadingZeros(w)
	}
	orig := x
	x--
	if x > orig {
		return -1
	}
	for ; x < orig; x-- {
		if bc.bitmap[x] != 0 {
			return int((x*64)+63) - countLeadingZeros(bc.bitmap[x])
		}
	}
	return -1
}

// reference the java implementation
// https://github.com/RoaringBitmap/RoaringBitmap/blob/master/src/main/java/org/roaringbitmap/BitmapContainer.java#L875-L892
func (bc *containerBitmap) numberOfRuns() int {
	if bc.cardinality == 0 {
		return 0
	}

	var numRuns uint64
	nextWord := bc.bitmap[0]

	for i := 0; i < len(bc.bitmap)-1; i++ {
		word := nextWord
		nextWord = bc.bitmap[i+1]
		numRuns += popcount((^word)&(word<<1)) + ((word >> 63) &^ nextWord)
	}

	word := nextWord
	numRuns += popcount((^word) & (word << 1))
	if (word & 0x8000000000000000) != 0 {
		numRuns++
	}

	return int(numRuns)
}

// convert to run or array *if needed*
func (bc *containerBitmap) toEfficientContainer() container16 {
	numRuns := bc.numberOfRuns()

	sizeAsRunContainer := containerRunSerializedSizeInBytes(numRuns)
	sizeAsBitmapContainer := containerBitmapSizeInBytes()
	card := bc.getCardinality()
	sizeAsArrayContainer := containerArraySizeInBytes(card)

	if sizeAsRunContainer < min(sizeAsBitmapContainer, sizeAsArrayContainer) {
		return newContainerRunFromBitmapWithRuns(bc, numRuns)
	}
	if card <= arrayDefaultMaxSize {
		return bc.toArrayContainer()
	}
	return bc
}

func newContainerBitmapFromRun(rc *containerRun) *containerBitmap {
	if len(rc.iv) == 1 {
		return newContainerBitmapWithRange(int(rc.iv[0].start), int(rc.iv[0].last()))
	}

	bc := newContainerBitmap()
	for i := range rc.iv {
		setBitmapRange(bc.bitmap, int(rc.iv[i].start), int(rc.iv[i].last())+1)
		bc.cardinality += int(rc.iv[i].last()) + 1 - int(rc.iv[i].start)
	}
	// bc.computeCardinality()
	return bc
}
