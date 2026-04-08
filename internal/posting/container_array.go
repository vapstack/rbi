package posting

import "sync/atomic"

type containerArray struct {
	refs    atomic.Int32
	content []uint16
}

func (ac *containerArray) retain() container16 {
	ac.refs.Add(1)
	return ac
}

func (ac *containerArray) uniquelyOwned() bool {
	return ac.refs.Load() == 1
}

func (ac *containerArray) release() {
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
	containerArrayClassPools[idx].Put(ac)
}

func (ac *containerArray) Release() { ac.release() }

func (ac *containerArray) fillLeastSignificant16bits(x []uint32, i int, mask uint32) int {
	if i < 0 {
		panic("negative index")
	}
	if len(ac.content) == 0 {
		return i
	}
	_ = x[len(ac.content)-1+i]
	_ = ac.content[len(ac.content)-1]
	for k := 0; k < len(ac.content); k++ {
		x[k+i] = uint32(ac.content[k]) | mask
	}
	return i + len(ac.content)
}

func (ac *containerArray) iterate(cb func(x uint16) bool) bool {
	iterator := shortIterator{ac.content, 0}

	for iterator.hasNext() {
		if !cb(iterator.next()) {
			return false
		}
	}

	return true
}

func (ac *containerArray) getShortIterator() shortPeekable {
	return &shortIterator{ac.content, 0}
}

func (ac *containerArray) getManyIterator() manyIterable {
	return &shortIterator{ac.content, 0}
}

func (ac *containerArray) minimum() uint16 {
	return ac.content[0] // assume not empty
}

func (ac *containerArray) maximum() uint16 {
	return ac.content[len(ac.content)-1] // assume not empty
}

func (ac *containerArray) getSizeInBytes() int {
	return ac.getCardinality() * 2
}

func containerArraySizeInBytes(card int) int {
	return card * 2
}

// add the values in the range [firstOfRange,endx)
func (ac *containerArray) iaddRange(firstOfRange, endx int) container16 {
	if firstOfRange >= endx {
		return ac
	}
	indexstart := binarySearch(ac.content, uint16(firstOfRange))
	if indexstart < 0 {
		indexstart = -indexstart - 1
	}
	indexend := binarySearch(ac.content, uint16(endx-1))
	if indexend < 0 {
		indexend = -indexend - 1
	} else {
		indexend++
	}
	rangelength := endx - firstOfRange
	newcardinality := indexstart + (ac.getCardinality() - indexend) + rangelength
	if newcardinality > arrayDefaultMaxSize {
		a := ac.toBitmapContainer()
		return a.iaddRange(firstOfRange, endx)
	}
	if cap(ac.content) < newcardinality {
		donor := getContainerArrayWithLen(newcardinality)
		copy(donor.content[:indexstart], ac.content[:indexstart])
		copy(donor.content[indexstart+rangelength:], ac.content[indexend:])
		replaceContainerArrayStorage(ac, donor)
	} else {
		ac.content = ac.content[:newcardinality]
		copy(ac.content[indexstart+rangelength:], ac.content[indexend:])

	}
	for k := 0; k < rangelength; k++ {
		ac.content[k+indexstart] = uint16(firstOfRange + k)
	}
	return ac
}

// remove the values in the range [firstOfRange,endx)
func (ac *containerArray) iremoveRange(firstOfRange, endx int) container16 {
	if firstOfRange >= endx {
		return ac
	}
	indexstart := binarySearch(ac.content, uint16(firstOfRange))
	if indexstart < 0 {
		indexstart = -indexstart - 1
	}
	indexend := binarySearch(ac.content, uint16(endx-1))
	if indexend < 0 {
		indexend = -indexend - 1
	} else {
		indexend++
	}
	rangelength := indexend - indexstart
	answer := ac
	copy(answer.content[indexstart:], ac.content[indexstart+rangelength:])
	answer.content = answer.content[:ac.getCardinality()-rangelength]
	return answer
}

// flip the values in the range [firstOfRange,endx)
func (ac *containerArray) not(firstOfRange, endx int) container16 {
	if firstOfRange >= endx {
		return ac.clone()
	}
	return ac.notClose(firstOfRange, endx-1) // remove everything in [firstOfRange,endx-1]
}

// flip the values in the range [firstOfRange,lastOfRange]
func (ac *containerArray) notClose(firstOfRange, lastOfRange int) container16 {
	if firstOfRange > lastOfRange { // unlike add and remove, not uses an inclusive range [firstOfRange,lastOfRange]
		return ac.clone()
	}

	// determine the span of array indices to be affected^M
	startIndex := binarySearch(ac.content, uint16(firstOfRange))
	if startIndex < 0 {
		startIndex = -startIndex - 1
	}
	lastIndex := binarySearch(ac.content, uint16(lastOfRange))
	if lastIndex < 0 {
		lastIndex = -lastIndex - 2
	}
	currentValuesInRange := lastIndex - startIndex + 1
	spanToBeFlipped := lastOfRange - firstOfRange + 1
	newValuesInRange := spanToBeFlipped - currentValuesInRange
	cardinalityChange := newValuesInRange - currentValuesInRange
	newCardinality := len(ac.content) + cardinalityChange
	if newCardinality > arrayDefaultMaxSize {
		bc := ac.toBitmapContainer()
		result := bc.inot(firstOfRange, lastOfRange+1)
		if returned, ok := result.(*containerBitmap); !ok || returned != bc {
			bc.release()
		}
		return result
	}
	answer := getContainerArrayWithLen(newCardinality)

	copy(answer.content, ac.content[:startIndex])
	outPos := startIndex
	inPos := startIndex
	valInRange := firstOfRange
	for ; valInRange <= lastOfRange && inPos <= lastIndex; valInRange++ {
		if uint16(valInRange) != ac.content[inPos] {
			answer.content[outPos] = uint16(valInRange)
			outPos++
		} else {
			inPos++
		}
	}

	for ; valInRange <= lastOfRange; valInRange++ {
		answer.content[outPos] = uint16(valInRange)
		outPos++
	}

	for i := lastIndex + 1; i < len(ac.content); i++ {
		answer.content[outPos] = ac.content[i]
		outPos++
	}
	answer.content = answer.content[:newCardinality]
	return answer
}

func (ac *containerArray) equals(o container16) bool {
	switch other := o.(type) {
	case *containerArray:
		// Check if the containers are the same object.
		if ac == other {
			return true
		}

		if len(other.content) != len(ac.content) {
			return false
		}

		for i, v := range ac.content {
			if v != other.content[i] {
				return false
			}
		}
		return true
	case *containerBitmap:
		return equalArrayBitmap(ac, other)
	case *containerRun:
		return equalArrayRun(ac, other)
	}
	panic("unsupported container16 type")
}

func (ac *containerArray) toBitmapContainer() *containerBitmap {
	bc := newContainerBitmap()
	bc.loadData(ac)
	return bc
}

func (ac *containerArray) appendValue(v uint16) {
	oldLen := len(ac.content)
	if cap(ac.content) < oldLen+1 {
		donor := getContainerArrayWithLen(oldLen + 1)
		copy(donor.content[:oldLen], ac.content)
		donor.content[oldLen] = v
		replaceContainerArrayStorage(ac, donor)
		return
	}
	ac.content = ac.content[:oldLen+1]
	ac.content[oldLen] = v
}

func (ac *containerArray) insertValue(idx int, v uint16) {
	oldLen := len(ac.content)
	if cap(ac.content) < oldLen+1 {
		donor := getContainerArrayWithLen(oldLen + 1)
		copy(donor.content[:idx], ac.content[:idx])
		copy(donor.content[idx+1:], ac.content[idx:])
		donor.content[idx] = v
		replaceContainerArrayStorage(ac, donor)
		return
	}
	ac.content = ac.content[:oldLen+1]
	copy(ac.content[idx+1:], ac.content[idx:oldLen])
	ac.content[idx] = v
}

func (ac *containerArray) iadd(x uint16) (wasNew bool) {
	// Special case adding to the end of the container16.
	l := len(ac.content)
	if l > 0 && l < arrayDefaultMaxSize && ac.content[l-1] < x {
		ac.appendValue(x)
		return true
	}

	loc := binarySearch(ac.content, x)

	if loc < 0 {
		i := -loc - 1
		ac.insertValue(i, x)
		return true
	}
	return false
}

func (ac *containerArray) iaddReturnMinimized(x uint16) container16 {
	// Special case adding to the end of the container16.
	l := len(ac.content)
	if l > 0 && l < arrayDefaultMaxSize && ac.content[l-1] < x {
		ac.appendValue(x)
		return ac
	}

	loc := binarySearch(ac.content, x)

	if loc < 0 {
		if len(ac.content) >= arrayDefaultMaxSize {
			a := ac.toBitmapContainer()
			a.iadd(x)
			return a
		}
		i := -loc - 1
		ac.insertValue(i, x)
	}
	return ac
}

// iremoveReturnMinimized is allowed to change the return type to minimize storage.
func (ac *containerArray) iremoveReturnMinimized(x uint16) container16 {
	ac.iremove(x)
	return ac
}

func (ac *containerArray) iremove(x uint16) bool {
	loc := binarySearch(ac.content, x)
	if loc >= 0 {
		s := ac.content
		s = append(s[:loc], s[loc+1:]...)
		ac.content = s
		return true
	}
	return false
}

func (ac *containerArray) or(a container16) container16 {
	switch x := a.(type) {
	case *containerArray:
		return ac.orArray(x)
	case *containerBitmap:
		return x.orArray(ac)
	case *containerRun:
		if x.isFull() {
			return x.clone()
		}
		return x.orArray(ac)
	}
	panic("unsupported container16 type")
}

func (ac *containerArray) ior(a container16) container16 {
	switch x := a.(type) {
	case *containerArray:
		return ac.iorArray(x)
	case *containerBitmap:
		return a.(*containerBitmap).orArray(ac)
	case *containerRun:
		if x.isFull() {
			return x.clone()
		}
		return ac.iorRun(x)
	}
	panic("unsupported container16 type")
}

func (ac *containerArray) iorArray(value2 *containerArray) container16 {
	value1 := ac
	len1 := value1.getCardinality()
	len2 := value2.getCardinality()
	maxPossibleCardinality := len1 + len2
	if maxPossibleCardinality > cap(value1.content) {
		// doubling the capacity reduces new slice allocations in the case of
		// repeated calls to iorArray().
		newSize := 2 * maxPossibleCardinality
		// the second check is to handle overly large array containers
		// and should not occur in normal usage,
		// as all array containers should be at most arrayDefaultMaxSize
		if newSize > 2*arrayDefaultMaxSize && maxPossibleCardinality <= 2*arrayDefaultMaxSize {
			newSize = 2 * arrayDefaultMaxSize
		}
		donor := getContainerArrayWithLen(newSize)
		copy(donor.content[len2:maxPossibleCardinality], ac.content[0:len1])
		nl := union2by2(donor.content[len2:maxPossibleCardinality], value2.content, donor.content)
		if nl > arrayDefaultMaxSize {
			donor.content = donor.content[:nl]
			bc := donor.toBitmapContainer()
			donor.release()
			return bc
		}
		donor.content = donor.content[:nl]
		replaceContainerArrayStorage(ac, donor)
		return ac
	} else {
		copy(ac.content[len2:maxPossibleCardinality], ac.content[0:len1])
	}
	nl := union2by2(value1.content[len2:maxPossibleCardinality], value2.content, ac.content)
	ac.content = ac.content[:nl] // reslice to match actual used capacity

	if nl > arrayDefaultMaxSize {
		// Only converting to a bitmap when arrayDefaultMaxSize
		// is actually exceeded minimizes conversions in the case of repeated
		// calls to iorArray().
		return ac.toBitmapContainer()
	}
	return ac
}

// Note: such code does not make practical sense, except for lazy evaluations
func (ac *containerArray) iorBitmap(bc2 *containerBitmap) container16 {
	answer := bc2.clone().(*containerBitmap)
	return answer.iorArray(ac)
}

func (ac *containerArray) iorRun(rc *containerRun) container16 {
	runCardinality := rc.getCardinality()
	// heuristic for if the container16 should maybe be an
	// containerArray.
	if runCardinality < ac.getCardinality() &&
		runCardinality+ac.getCardinality() < arrayDefaultMaxSize {
		var result container16
		result = ac
		for _, run := range rc.iv {
			result = result.iaddRange(int(run.start), int(run.start)+int(run.length)+1)
		}
		return result
	}
	return rc.orArray(ac)
}

func (ac *containerArray) orArray(value2 *containerArray) container16 {
	value1 := ac
	maxPossibleCardinality := value1.getCardinality() + value2.getCardinality()
	if maxPossibleCardinality > arrayDefaultMaxSize { // it could be a bitmap!
		bc := newContainerBitmap()
		for k := 0; k < len(value2.content); k++ {
			v := value2.content[k]
			i := uint(v) >> 6
			mask := uint64(1) << (v % 64)
			bc.bitmap[i] |= mask
		}
		for k := 0; k < len(ac.content); k++ {
			v := ac.content[k]
			i := uint(v) >> 6
			mask := uint64(1) << (v % 64)
			bc.bitmap[i] |= mask
		}
		bc.cardinality = int(popcntSlice(bc.bitmap))
		if bc.cardinality <= arrayDefaultMaxSize {
			return bc.toArrayContainer()
		}
		return bc
	}
	answer := getContainerArrayWithCap(maxPossibleCardinality)
	nl := union2by2(value1.content, value2.content, answer.content)
	answer.content = answer.content[:nl] // reslice to match actual used capacity
	return answer
}

func (ac *containerArray) and(a container16) container16 {
	switch x := a.(type) {
	case *containerArray:
		return ac.andArray(x)
	case *containerBitmap:
		return x.and(ac)
	case *containerRun:
		if x.isFull() {
			return ac.clone()
		}
		return x.andArray(ac)
	}
	panic("unsupported container16 type")
}

func (ac *containerArray) andCardinality(a container16) int {
	switch x := a.(type) {
	case *containerArray:
		return ac.andArrayCardinality(x)
	case *containerBitmap:
		return x.andCardinality(ac)
	case *containerRun:
		return x.andArrayCardinality(ac)
	}
	panic("unsupported container16 type")
}

func (ac *containerArray) intersects(a container16) bool {
	switch x := a.(type) {
	case *containerArray:
		return ac.intersectsArray(x)
	case *containerBitmap:
		return x.intersects(ac)
	case *containerRun:
		return x.intersects(ac)
	}
	panic("unsupported container16 type")
}

func (ac *containerArray) iand(a container16) container16 {
	switch x := a.(type) {
	case *containerArray:
		return ac.iandArray(x)
	case *containerBitmap:
		return ac.iandBitmap(x)
	case *containerRun:
		if x.isFull() {
			return ac
		}
		return x.andArray(ac)
	}
	panic("unsupported container16 type")
}

func (ac *containerArray) iandBitmap(bc *containerBitmap) container16 {
	pos := 0
	c := ac.getCardinality()
	for k := 0; k < c; k++ {
		// branchless
		v := ac.content[k]
		ac.content[pos] = v
		pos += int(bc.bitValue(v))
	}
	ac.content = ac.content[:pos]
	return ac
}

func (ac *containerArray) xor(a container16) container16 {
	switch x := a.(type) {
	case *containerArray:
		return ac.xorArray(x)
	case *containerBitmap:
		return a.xor(ac)
	case *containerRun:
		return x.xorArray(ac)
	}
	panic("unsupported container16 type")
}

func (ac *containerArray) xorArray(value2 *containerArray) container16 {
	value1 := ac
	totalCardinality := value1.getCardinality() + value2.getCardinality()
	if totalCardinality > arrayDefaultMaxSize { // it could be a bitmap!
		bc := newContainerBitmap()
		for k := 0; k < len(value2.content); k++ {
			v := value2.content[k]
			i := uint(v) >> 6
			bc.bitmap[i] ^= uint64(1) << (v % 64)
		}
		for k := 0; k < len(ac.content); k++ {
			v := ac.content[k]
			i := uint(v) >> 6
			bc.bitmap[i] ^= uint64(1) << (v % 64)
		}
		bc.computeCardinality()
		if bc.cardinality <= arrayDefaultMaxSize {
			return bc.toArrayContainer()
		}
		return bc
	}
	desiredCapacity := totalCardinality
	answer := getContainerArrayWithCap(desiredCapacity)
	length := exclusiveUnion2by2(value1.content, value2.content, answer.content)
	answer.content = answer.content[:length]
	return answer
}

func (ac *containerArray) andNot(a container16) container16 {
	switch x := a.(type) {
	case *containerArray:
		return ac.andNotArray(x)
	case *containerBitmap:
		return ac.andNotBitmap(x)
	case *containerRun:
		return ac.andNotRun(x)
	}
	panic("unsupported container16 type")
}

func (ac *containerArray) andNotRun(rc *containerRun) container16 {
	answer := ac.clone().(*containerArray)
	return answer.iandNotRun(rc)
}

func (ac *containerArray) iandNot(a container16) container16 {
	switch x := a.(type) {
	case *containerArray:
		return ac.iandNotArray(x)
	case *containerBitmap:
		return ac.iandNotBitmap(x)
	case *containerRun:
		return ac.iandNotRun(x)
	}
	panic("unsupported container16 type")
}

func (ac *containerArray) iandNotRun(rc *containerRun) container16 {
	// Fast path: if either the containerArray or the containerRun is empty, the result is the array.
	if ac.isEmpty() || rc.isEmpty() {
		// Empty
		return ac
	}
	// Fast path: if the containerRun is full, the result is empty.
	if rc.isFull() {
		ac.content = ac.content[:0]
		return ac
	}
	current_run := 0
	// All values in [start_run, end_end] are part of the run
	start_run := rc.iv[current_run].start
	end_end := start_run + rc.iv[current_run].length
	// We are going to read values in the array at index i, and we are
	// going to write them at index pos. So we do in-place processing.
	// We always have that pos <= i by construction. So we can either
	// overwrite a value just read, or a value that was previous read.
	pos := 0
	i := 0
	for ; i < len(ac.content); i++ {
		if ac.content[i] < start_run {
			// the value in the array appears before the run [start_run, end_end]
			ac.content[pos] = ac.content[i]
			pos++
		} else if ac.content[i] <= end_end {
			// nothing to do, the value is in the array but also in the run.
		} else {
			// We have the value in the array after the run. We cannot tell
			// whether we need to keep it or not. So let us move to another run.
			if current_run+1 < len(rc.iv) {
				current_run++
				start_run = rc.iv[current_run].start
				end_end = start_run + rc.iv[current_run].length
				i-- // retry with the same i
			} else {
				// We have exhausted the number of runs. We can keep the rest of the values
				// from i to len(ac.content) - 1 inclusively.
				break // We are done, the rest of the array will be kept
			}
		}
	}
	for ; i < len(ac.content); i++ {
		ac.content[pos] = ac.content[i]
		pos++
	}
	// We 'shink' the slice.
	ac.content = ac.content[:pos]
	return ac
}

func (ac *containerArray) andNotArray(value2 *containerArray) container16 {
	value1 := ac
	desiredcapacity := value1.getCardinality()
	answer := getContainerArrayWithCap(desiredcapacity)
	length := difference(value1.content, value2.content, answer.content)
	answer.content = answer.content[:length]
	return answer
}

func (ac *containerArray) iandNotArray(value2 *containerArray) container16 {
	length := difference(ac.content, value2.content, ac.content)
	ac.content = ac.content[:length]
	return ac
}

func (ac *containerArray) andNotBitmap(value2 *containerBitmap) container16 {
	desiredcapacity := ac.getCardinality()
	answer := getContainerArrayWithCap(desiredcapacity)
	answer.content = answer.content[:desiredcapacity]
	pos := 0
	for _, v := range ac.content {
		answer.content[pos] = v
		pos += 1 - int(value2.bitValue(v))
	}
	answer.content = answer.content[:pos]
	return answer
}

func (ac *containerArray) iandNotBitmap(value2 *containerBitmap) container16 {
	pos := 0
	for _, v := range ac.content {
		ac.content[pos] = v
		pos += 1 - int(value2.bitValue(v))
	}
	ac.content = ac.content[:pos]
	return ac
}

// flip the values in the range [firstOfRange,endx)
func (ac *containerArray) inot(firstOfRange, endx int) container16 {
	if firstOfRange >= endx {
		return ac
	}
	return ac.inotClose(firstOfRange, endx-1) // remove everything in [firstOfRange,endx-1]
}

// flip the values in the range [firstOfRange,lastOfRange]
func (ac *containerArray) inotClose(firstOfRange, lastOfRange int) container16 {
	if firstOfRange > lastOfRange { // unlike add and remove, not uses an inclusive range [firstOfRange,lastOfRange]
		return ac
	}
	// determine the span of array indices to be affected
	startIndex := binarySearch(ac.content, uint16(firstOfRange))
	if startIndex < 0 {
		startIndex = -startIndex - 1
	}
	lastIndex := binarySearch(ac.content, uint16(lastOfRange))
	if lastIndex < 0 {
		lastIndex = -lastIndex - 1 - 1
	}
	currentValuesInRange := lastIndex - startIndex + 1
	spanToBeFlipped := lastOfRange - firstOfRange + 1

	newValuesInRange := spanToBeFlipped - currentValuesInRange
	buffer := getContainerArrayWithLen(newValuesInRange)
	defer buffer.release()
	cardinalityChange := newValuesInRange - currentValuesInRange
	newCardinality := len(ac.content) + cardinalityChange
	if cardinalityChange > 0 {
		if newCardinality > len(ac.content) {
			if newCardinality > arrayDefaultMaxSize {
				bcRet := ac.toBitmapContainer()
				bcRet.inot(firstOfRange, lastOfRange+1)
				return bcRet
			}
			donor := getContainerArrayWithLen(newCardinality)
			copy(donor.content, ac.content)
			replaceContainerArrayStorage(ac, donor)
		}
		base := lastIndex + 1
		copy(ac.content[lastIndex+1+cardinalityChange:], ac.content[base:base+len(ac.content)-1-lastIndex])
		ac.negateRange(buffer.content, startIndex, lastIndex, firstOfRange, lastOfRange+1)
	} else { // no expansion needed
		ac.negateRange(buffer.content, startIndex, lastIndex, firstOfRange, lastOfRange+1)
		if cardinalityChange < 0 {
			for i := startIndex + newValuesInRange; i < newCardinality; i++ {
				ac.content[i] = ac.content[i-cardinalityChange]
			}
		}
	}
	ac.content = ac.content[:newCardinality]
	return ac
}

func (ac *containerArray) negateRange(buffer []uint16, startIndex, lastIndex, startRange, lastRange int) {
	// compute the negation into buffer
	outPos := 0
	inPos := startIndex // value here always >= valInRange,
	// until it is exhausted
	// n.b., we can start initially exhausted.

	valInRange := startRange
	for ; valInRange < lastRange && inPos <= lastIndex; valInRange++ {
		if uint16(valInRange) != ac.content[inPos] {
			buffer[outPos] = uint16(valInRange)
			outPos++
		} else {
			inPos++
		}
	}

	// if there are extra items (greater than the biggest
	// pre-existing one in range), buffer them
	for ; valInRange < lastRange; valInRange++ {
		buffer[outPos] = uint16(valInRange)
		outPos++
	}

	if outPos != len(buffer) {
		panic("negateRange: internal bug")
	}

	for i, item := range buffer {
		ac.content[i+startIndex] = item
	}
}

func (ac *containerArray) isFull() bool {
	return false
}

func (ac *containerArray) andArray(value2 *containerArray) container16 {
	desiredcapacity := min(ac.getCardinality(), value2.getCardinality())
	answer := getContainerArrayWithCap(desiredcapacity)
	length := intersection2by2(
		ac.content,
		value2.content,
		answer.content)
	answer.content = answer.content[:length]
	return answer
}

func (ac *containerArray) andArrayCardinality(value2 *containerArray) int {
	return intersection2by2Cardinality(
		ac.content,
		value2.content)
}

func (ac *containerArray) intersectsArray(value2 *containerArray) bool {
	return intersects2by2(
		ac.content,
		value2.content)
}

func (ac *containerArray) iandArray(value2 *containerArray) container16 {
	length := intersection2by2(
		ac.content,
		value2.content,
		ac.content)
	ac.content = ac.content[:length]
	return ac
}

func (ac *containerArray) getCardinality() int {
	return len(ac.content)
}

func (ac *containerArray) isEmpty() bool {
	return len(ac.content) == 0
}

func (ac *containerArray) clone() container16 {
	ptr := getContainerArrayWithLen(len(ac.content))
	copy(ptr.content, ac.content)
	return ptr
}

func (ac *containerArray) contains(x uint16) bool {
	return binarySearch(ac.content, x) >= 0
}

func (ac *containerArray) loadData(containerBitmap *containerBitmap) {
	ac.realloc(containerBitmap.cardinality)
	containerBitmap.fillArray(ac.content)
}

func (ac *containerArray) realloc(size int) {
	if size <= 0 {
		ac.content = ac.content[:0]
		return
	}
	if cap(ac.content) < size {
		donor := getContainerArrayWithLen(size)
		copy(donor.content, ac.content)
		replaceContainerArrayStorage(ac, donor)
	} else {
		ac.content = ac.content[:size]
	}
}

func newContainerArrayFromBitmap(bc *containerBitmap) *containerArray {
	ac := getContainerArray()
	ac.loadData(bc)
	return ac
}

func (ac *containerArray) numberOfRuns() (nr int) {
	n := len(ac.content)
	var runlen uint16
	var cur, prev uint16

	switch n {
	case 0:
		return 0
	case 1:
		return 1
	default:
		for i := 1; i < n; i++ {
			prev = ac.content[i-1]
			cur = ac.content[i]

			if cur == prev+1 {
				runlen++
			} else {
				if cur < prev {
					panic("the fundamental containerArray assumption of sorted ac.content was broken")
				}
				if cur == prev {
					panic("the fundamental containerArray assumption of deduplicated content was broken")
				} else {
					nr++
					runlen = 0
				}
			}
		}
		nr++
	}
	return
}

// convert to run or array *if needed*
func (ac *containerArray) toEfficientContainer() container16 {
	numRuns := ac.numberOfRuns()
	sizeAsRunContainer := containerRunSerializedSizeInBytes(numRuns)
	sizeAsBitmapContainer := containerBitmapSizeInBytes()
	card := ac.getCardinality()
	sizeAsArrayContainer := containerArraySizeInBytes(card)
	if sizeAsRunContainer < min(sizeAsBitmapContainer, sizeAsArrayContainer) {
		return newContainerRunFromArrayWithRuns(ac, numRuns)
	}
	if card <= arrayDefaultMaxSize {
		return ac
	}
	return ac.toBitmapContainer()
}
