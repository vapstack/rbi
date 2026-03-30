package posting

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
	"sync"
	"unsafe"
)

type largePosting struct {
	highlowcontainer largeArray
}

func newLargePosting() *largePosting {
	return &largePosting{}
}

func largePostingOf(dat ...uint64) *largePosting {
	lp := newLargePosting()
	lp.addMany(dat)
	return lp
}

func (lp *largePosting) WriteTo(stream io.Writer) (int64, error) {
	var (
		n   int64
		buf [8]byte
	)

	binary.LittleEndian.PutUint64(buf[:], uint64(lp.highlowcontainer.size()))
	written, err := stream.Write(buf[:])
	n += int64(written)
	if err != nil {
		return n, err
	}

	keyBuf := buf[:4]
	for pos := 0; pos < lp.highlowcontainer.size(); pos++ {
		c := lp.highlowcontainer.getContainerAtIndex(pos)
		binary.LittleEndian.PutUint32(keyBuf, lp.highlowcontainer.getKeyAtIndex(pos))
		written, err = stream.Write(keyBuf)
		n += int64(written)
		if err != nil {
			return n, err
		}
		n64, writeErr := c.WriteTo(stream)
		n += n64
		err = writeErr
		if err != nil {
			return n, err
		}
	}

	return n, nil
}

func (lp *largePosting) ReadFrom(stream io.Reader) (p int64, err error) {
	var sizeBuf [8]byte
	var prevKey uint32

	n, err := io.ReadFull(stream, sizeBuf[:])
	if err != nil {
		return int64(n), err
	}
	p += int64(n)

	size := binary.LittleEndian.Uint64(sizeBuf[:])
	lp.highlowcontainer.clear()

	if cap(lp.highlowcontainer.keys) >= int(size) {
		lp.highlowcontainer.keys = lp.highlowcontainer.keys[:size]
	} else {
		lp.highlowcontainer.keys = make([]uint32, size)
	}
	if cap(lp.highlowcontainer.containers) >= int(size) {
		lp.highlowcontainer.containers = lp.highlowcontainer.containers[:size]
	} else {
		lp.highlowcontainer.containers = make([]*bitmap32, size)
	}

	keyBuf := sizeBuf[:4]
	for i := uint64(0); i < size; i++ {
		n, err = io.ReadFull(stream, keyBuf)
		if err != nil {
			return int64(n), fmt.Errorf("error in largePosting.ReadFrom: could not read key #%d: %w", i, err)
		}
		p += int64(n)
		key := binary.LittleEndian.Uint32(keyBuf)
		if i != 0 && key <= prevKey {
			return p, fmt.Errorf("large posting keys are not strictly increasing")
		}
		prevKey = key
		lp.highlowcontainer.keys[i] = key
		lp.highlowcontainer.containers[i] = newBitmap()
		n64, readErr := lp.highlowcontainer.containers[i].ReadFrom(stream)
		p += n64
		if n64 == 0 || readErr != nil {
			return p, fmt.Errorf("could not deserialize container for key #%d: %w", i, readErr)
		}
		if lp.highlowcontainer.containers[i].isEmpty() {
			return p, fmt.Errorf("large posting container #%d is empty", i)
		}
	}

	return p, nil
}

func (lp *largePosting) runOptimize() {
	lp.highlowcontainer.runOptimize()
}

func (lp *largePosting) clear() {
	lp.highlowcontainer.clear()
}

func (lp *largePosting) toArray() []uint64 {
	out := make([]uint64, 0, lp.cardinality())
	for i := 0; i < lp.highlowcontainer.size(); i++ {
		hs := uint64(lp.highlowcontainer.getKeyAtIndex(i)) << 32
		out = lp.highlowcontainer.getContainerAtIndex(i).appendToArray64(out, hs)
	}
	return out
}

func (rb *bitmap32) appendToArray64(dst []uint64, hs64 uint64) []uint64 {
	for i := 0; i < rb.highlowcontainer.size(); i++ {
		hs := hs64 | uint64(rb.highlowcontainer.getKeyAtIndex(i))<<16
		dst = appendContainerToArray64(dst, hs, rb.highlowcontainer.getContainerAtIndex(i))
	}
	return dst
}

func appendContainerToArray64(dst []uint64, hs uint64, c container16) []uint64 {
	switch x := c.(type) {
	case *containerArray:
		return appendArrayContainerToArray64(dst, hs, x)
	case *containerBitmap:
		return appendBitmapContainerToArray64(dst, hs, x)
	case *containerRun:
		return appendRunContainerToArray64(dst, hs, x)
	default:
		panic("unsupported container16 type")
	}
}

func appendArrayContainerToArray64(dst []uint64, hs uint64, ac *containerArray) []uint64 {
	start := len(dst)
	dst = slices.Grow(dst, len(ac.content))
	dst = dst[:start+len(ac.content)]
	for i, v := range ac.content {
		dst[start+i] = hs | uint64(v)
	}
	return dst
}

func appendBitmapContainerToArray64(dst []uint64, hs uint64, bc *containerBitmap) []uint64 {
	start := len(dst)
	dst = slices.Grow(dst, bc.cardinality)
	dst = dst[:start+bc.cardinality]
	it := bitmapContainerManyIterator{ptr: bc, base: -1}
	n := it.nextMany64(hs, dst[start:])
	return dst[:start+n]
}

func appendRunContainerToArray64(dst []uint64, hs uint64, rc *containerRun) []uint64 {
	for i := range rc.iv {
		runLen := rc.iv[i].runlen()
		start := len(dst)
		dst = slices.Grow(dst, runLen)
		dst = dst[:start+runLen]
		base := hs | uint64(rc.iv[i].start)
		buf := dst[start:]
		for j := range buf {
			buf[j] = base + uint64(j)
		}
	}
	return dst
}

func (lp *largePosting) sizeInBytes() uint64 {
	size := uint64(8)
	for _, c := range lp.highlowcontainer.containers {
		size += 4 + c.sizeInBytes()
	}
	return size
}

func (lp *largePosting) serializedSizeInBytes() uint64 {
	return lp.highlowcontainer.serializedSizeInBytes()
}

func (lp *largePosting) iterator() *largeIterator {
	return newLargeIterator(lp)
}

func (lp *largePosting) clone() *largePosting {
	return lp.cloneInto(newLargePosting())
}

func (lp *largePosting) cloneInto(dst *largePosting) *largePosting {
	if dst == lp {
		return dst
	}
	if dst == nil {
		dst = newLargePosting()
	}
	dst.highlowcontainer.copyFrom(&lp.highlowcontainer)
	return dst
}

func (lp *largePosting) cloneSharedInto(dst *largePosting) *largePosting {
	if dst == lp {
		return dst
	}
	if dst == nil {
		dst = newLargePosting()
	}
	dst.highlowcontainer.copySharedFrom(&lp.highlowcontainer)
	return dst
}

func (lp *largePosting) minimum() uint64 {
	return uint64(lp.highlowcontainer.containers[0].minimum()) | (uint64(lp.highlowcontainer.keys[0]) << 32)
}

func (lp *largePosting) maximum() uint64 {
	last := len(lp.highlowcontainer.containers) - 1
	return uint64(lp.highlowcontainer.containers[last].maximum()) | (uint64(lp.highlowcontainer.keys[last]) << 32)
}

func (lp *largePosting) trySingle() (uint64, bool) {
	if len(lp.highlowcontainer.containers) != 1 {
		return 0, false
	}

	lo, ok := lp.highlowcontainer.containers[0].trySingle()
	if !ok {
		return 0, false
	}
	return uint64(lo) | (uint64(lp.highlowcontainer.keys[0]) << 32), true
}

func (lp *largePosting) contains(x uint64) bool {
	hb := highbits64(x)
	c := lp.highlowcontainer.getContainer(hb)
	return c != nil && c.contains(lowbits64(x))
}

func (lp *largePosting) equals(other *largePosting) bool {
	return other != nil && other.highlowcontainer.equals(&lp.highlowcontainer)
}

func (lp *largePosting) add(x uint64) {
	hb := highbits64(x)
	la := &lp.highlowcontainer
	i := la.getIndex(hb)
	if i >= 0 {
		la.getWritableContainerAtIndex(i).add(lowbits64(x))
		return
	}
	next := newBitmap()
	next.add(lowbits64(x))
	la.insertNewKeyValueAt(-i-1, hb, next)
}

func (lp *largePosting) checkedAdd(x uint64) bool {
	hb := highbits64(x)
	i := lp.highlowcontainer.getIndex(hb)
	if i >= 0 {
		return lp.highlowcontainer.getWritableContainerAtIndex(i).checkedAdd(lowbits64(x))
	}
	next := newBitmap()
	next.add(lowbits64(x))
	lp.highlowcontainer.insertNewKeyValueAt(-i-1, hb, next)
	return true
}

func (lp *largePosting) remove(x uint64) {
	hb := highbits64(x)
	i := lp.highlowcontainer.getIndex(hb)
	if i >= 0 {
		c := lp.highlowcontainer.getWritableContainerAtIndex(i)
		c.remove(lowbits64(x))
		if c.isEmpty() {
			lp.highlowcontainer.removeAtIndex(i)
		}
	}
}

func (lp *largePosting) addMany(dat []uint64) {
	if len(dat) == 0 {
		return
	}
	if isNonDecreasingU64(dat) {
		lp.addManySorted(dat)
		return
	}

	batchBuf := acquireAddManyBatch(len(dat))
	defer releaseAddManyBatch(batchBuf)

	start := 0
	batchHighBits := highbits64(dat[0])
	for end := 1; end < len(dat); end++ {
		hi := highbits64(dat[end])
		if hi != batchHighBits {
			batchLen := end - start
			batchBuf.values = slices.Grow(batchBuf.values[:0], batchLen)
			batch := batchBuf.values[:batchLen]
			for i := 0; i < batchLen; i++ {
				batch[i] = lowbits64(dat[start+i])
			}
			lp.getOrCreateContainer(batchHighBits).addMany(batch)
			batchHighBits = hi
			start = end
		}
	}

	batchLen := len(dat) - start
	batchBuf.values = slices.Grow(batchBuf.values[:0], batchLen)
	batch := batchBuf.values[:batchLen]
	for i := 0; i < batchLen; i++ {
		batch[i] = lowbits64(dat[start+i])
	}
	lp.getOrCreateContainer(batchHighBits).addMany(batch)
}

func (lp *largePosting) addManySorted(dat []uint64) {
	start := 0
	batchHighBits := highbits64(dat[0])
	for end := 1; end < len(dat); end++ {
		hi := highbits64(dat[end])
		if hi != batchHighBits {
			lp.addSortedHighBitsBatch(batchHighBits, dat[start:end])
			batchHighBits = hi
			start = end
		}
	}
	lp.addSortedHighBitsBatch(batchHighBits, dat[start:])
}

func (lp *largePosting) addSortedHighBitsBatch(hb uint32, dat []uint64) {
	la := &lp.highlowcontainer
	i := la.getIndex(hb)
	if i < 0 {
		next := newBitmap()
		next.loadManySorted64(dat)
		la.insertNewKeyValueAt(-i-1, hb, next)
		return
	}

	batchBuf := acquireAddManyBatch(len(dat))
	batchBuf.values = slices.Grow(batchBuf.values[:0], len(dat))
	batch := batchBuf.values[:len(dat)]
	for i := range dat {
		batch[i] = lowbits64(dat[i])
	}
	la.getWritableContainerAtIndex(i).addMany(batch)
	releaseAddManyBatch(batchBuf)
}

func (lp *largePosting) addRange(rangeStart, rangeEnd uint64) {
	if rangeStart >= rangeEnd {
		return
	}

	hbStart := uint64(highbits64(rangeStart))
	lbStart := uint64(lowbits64(rangeStart))
	hbLast := uint64(highbits64(rangeEnd - 1))
	lbLast := uint64(lowbits64(rangeEnd - 1))

	for hb := hbStart; hb <= hbLast; hb++ {
		containerStart := uint64(0)
		if hb == hbStart {
			containerStart = lbStart
		}
		containerLast := uint64(maxLargeLowBit)
		if hb == hbLast {
			containerLast = lbLast
		}
		lp.getOrCreateContainer(uint32(hb)).addRange(containerStart, containerLast+1)
	}
}

func (lp *largePosting) getOrCreateContainer(hb uint32) *bitmap32 {
	la := &lp.highlowcontainer
	i := la.getIndex(hb)
	if i >= 0 {
		return la.getWritableContainerAtIndex(i)
	}
	next := newBitmap()
	la.insertNewKeyValueAt(-i-1, hb, next)
	return next
}

func (lp *largePosting) isEmpty() bool {
	return lp.highlowcontainer.size() == 0
}

func (lp *largePosting) cardinality() uint64 {
	size := uint64(0)
	for _, c := range lp.highlowcontainer.containers {
		size += c.cardinality()
	}
	return size
}

func (lp *largePosting) and(other *largePosting) {
	pos1 := 0
	pos2 := 0
	outSize := 0
	length1 := lp.highlowcontainer.size()
	length2 := other.highlowcontainer.size()

main:
	for {
		if pos1 < length1 && pos2 < length2 {
			s1 := lp.highlowcontainer.getKeyAtIndex(pos1)
			s2 := other.highlowcontainer.getKeyAtIndex(pos2)
			for {
				if s1 == s2 {
					c1 := lp.highlowcontainer.getWritableContainerAtIndex(pos1)
					c2 := other.highlowcontainer.getContainerAtIndex(pos2)
					c1.and(c2)
					if !c1.isEmpty() {
						lp.highlowcontainer.replaceKeyAndContainerAtIndex(outSize, s1, c1)
						if outSize != pos1 {
							lp.highlowcontainer.clearContainerAtIndex(pos1)
						}
						outSize++
					}
					pos1++
					pos2++
					if pos1 == length1 || pos2 == length2 {
						break main
					}
					s1 = lp.highlowcontainer.getKeyAtIndex(pos1)
					s2 = other.highlowcontainer.getKeyAtIndex(pos2)
				} else if s1 < s2 {
					pos1 = lp.highlowcontainer.advanceUntil(s2, pos1)
					if pos1 == length1 {
						break main
					}
					s1 = lp.highlowcontainer.getKeyAtIndex(pos1)
				} else {
					pos2 = other.highlowcontainer.advanceUntil(s1, pos2)
					if pos2 == length2 {
						break main
					}
					s2 = other.highlowcontainer.getKeyAtIndex(pos2)
				}
			}
		} else {
			break
		}
	}
	lp.highlowcontainer.releaseContainersInRange(outSize)
	lp.highlowcontainer.resize(outSize)
}

func (lp *largePosting) andCardinality(other *largePosting) uint64 {
	pos1 := 0
	pos2 := 0
	answer := uint64(0)
	length1 := lp.highlowcontainer.size()
	length2 := other.highlowcontainer.size()

main:
	for {
		if pos1 < length1 && pos2 < length2 {
			s1 := lp.highlowcontainer.getKeyAtIndex(pos1)
			s2 := other.highlowcontainer.getKeyAtIndex(pos2)
			for {
				if s1 == s2 {
					c1 := lp.highlowcontainer.getContainerAtIndex(pos1)
					c2 := other.highlowcontainer.getContainerAtIndex(pos2)
					answer += c1.andCardinality(c2)
					pos1++
					pos2++
					if pos1 == length1 || pos2 == length2 {
						break main
					}
					s1 = lp.highlowcontainer.getKeyAtIndex(pos1)
					s2 = other.highlowcontainer.getKeyAtIndex(pos2)
				} else if s1 < s2 {
					pos1 = lp.highlowcontainer.advanceUntil(s2, pos1)
					if pos1 == length1 {
						break main
					}
					s1 = lp.highlowcontainer.getKeyAtIndex(pos1)
				} else {
					pos2 = other.highlowcontainer.advanceUntil(s1, pos2)
					if pos2 == length2 {
						break main
					}
					s2 = other.highlowcontainer.getKeyAtIndex(pos2)
				}
			}
		} else {
			break
		}
	}
	return answer
}

func (lp *largePosting) intersects(other *largePosting) bool {
	pos1 := 0
	pos2 := 0
	length1 := lp.highlowcontainer.size()
	length2 := other.highlowcontainer.size()

main:
	for {
		if pos1 < length1 && pos2 < length2 {
			s1 := lp.highlowcontainer.getKeyAtIndex(pos1)
			s2 := other.highlowcontainer.getKeyAtIndex(pos2)
			for {
				if s1 == s2 {
					c1 := lp.highlowcontainer.getContainerAtIndex(pos1)
					c2 := other.highlowcontainer.getContainerAtIndex(pos2)
					if c1.intersects(c2) {
						return true
					}
					pos1++
					pos2++
					if pos1 == length1 || pos2 == length2 {
						break main
					}
					s1 = lp.highlowcontainer.getKeyAtIndex(pos1)
					s2 = other.highlowcontainer.getKeyAtIndex(pos2)
				} else if s1 < s2 {
					pos1 = lp.highlowcontainer.advanceUntil(s2, pos1)
					if pos1 == length1 {
						break main
					}
					s1 = lp.highlowcontainer.getKeyAtIndex(pos1)
				} else {
					pos2 = other.highlowcontainer.advanceUntil(s1, pos2)
					if pos2 == length2 {
						break main
					}
					s2 = other.highlowcontainer.getKeyAtIndex(pos2)
				}
			}
		} else {
			break
		}
	}
	return false
}

func (lp *largePosting) forEachIntersecting(other *largePosting, fn func(uint64) bool) bool {
	it := lp.iterator()
	defer releaseLargeIterator(it)

	otherIt := other.iterator()
	defer releaseLargeIterator(otherIt)

	for it.HasNext() && otherIt.HasNext() {
		v := it.peekNext()
		otherV := otherIt.peekNext()

		if v < otherV {
			it.advanceIfNeeded(otherV)
			continue
		}
		if otherV < v {
			otherIt.advanceIfNeeded(v)
			continue
		}
		if fn(v) {
			return true
		}
		it.Next()
		otherIt.Next()
	}
	return false
}

func (lp *largePosting) or(other *largePosting) {
	if lp == other {
		return
	}
	length2 := other.highlowcontainer.size()
	if length2 == 0 {
		return
	}
	length1 := lp.highlowcontainer.size()
	if length1 == 0 {
		lp.highlowcontainer.appendCopyMany(other.highlowcontainer, 0, length2)
		return
	}

	outSize := countUnionKeys(lp.highlowcontainer.keys[:length1], other.highlowcontainer.keys[:length2])
	lp.highlowcontainer.grow(outSize)

	pos1 := length1 - 1
	pos2 := length2 - 1
	out := outSize - 1
	for pos1 >= 0 && pos2 >= 0 {
		s1 := lp.highlowcontainer.keys[pos1]
		s2 := other.highlowcontainer.keys[pos2]
		if s1 > s2 {
			lp.highlowcontainer.moveKeyValueAt(pos1, out)
			pos1--
			out--
			continue
		}
		if s1 < s2 {
			lp.highlowcontainer.keys[out] = s2
			lp.highlowcontainer.containers[out] = other.highlowcontainer.containers[pos2].clone()
			pos2--
			out--
			continue
		}

		lp.highlowcontainer.getWritableContainerAtIndex(pos1).or(other.highlowcontainer.getContainerAtIndex(pos2))
		lp.highlowcontainer.moveKeyValueAt(pos1, out)
		pos1--
		pos2--
		out--
	}
	for pos2 >= 0 {
		lp.highlowcontainer.keys[out] = other.highlowcontainer.keys[pos2]
		lp.highlowcontainer.containers[out] = other.highlowcontainer.containers[pos2].clone()
		pos2--
		out--
	}
	for pos1 >= 0 {
		lp.highlowcontainer.moveKeyValueAt(pos1, out)
		pos1--
		out--
	}
}

func (lp *largePosting) andNot(other *largePosting) {
	pos1 := 0
	pos2 := 0
	outSize := 0
	length1 := lp.highlowcontainer.size()
	length2 := other.highlowcontainer.size()

main:
	for {
		if pos1 < length1 && pos2 < length2 {
			s1 := lp.highlowcontainer.getKeyAtIndex(pos1)
			s2 := other.highlowcontainer.getKeyAtIndex(pos2)
			for {
				if s1 == s2 {
					c1 := lp.highlowcontainer.getWritableContainerAtIndex(pos1)
					c2 := other.highlowcontainer.getContainerAtIndex(pos2)
					c1.andNot(c2)
					if !c1.isEmpty() {
						lp.highlowcontainer.replaceKeyAndContainerAtIndex(outSize, s1, c1)
						if outSize != pos1 {
							lp.highlowcontainer.clearContainerAtIndex(pos1)
						}
						outSize++
					}
					pos1++
					pos2++
					if pos1 == length1 || pos2 == length2 {
						break main
					}
					s1 = lp.highlowcontainer.getKeyAtIndex(pos1)
					s2 = other.highlowcontainer.getKeyAtIndex(pos2)
				} else if s1 < s2 {
					c1 := lp.highlowcontainer.getContainerAtIndex(pos1)
					lp.highlowcontainer.replaceKeyAndContainerAtIndex(outSize, s1, c1)
					if outSize != pos1 {
						lp.highlowcontainer.clearContainerAtIndex(pos1)
					}
					outSize++
					pos1++
					if pos1 == length1 {
						break main
					}
					s1 = lp.highlowcontainer.getKeyAtIndex(pos1)
				} else {
					pos2 = other.highlowcontainer.advanceUntil(s1, pos2)
					if pos2 == length2 {
						break main
					}
					s2 = other.highlowcontainer.getKeyAtIndex(pos2)
				}
			}
		} else {
			break
		}
	}

	for pos1 < length1 {
		c1 := lp.highlowcontainer.getContainerAtIndex(pos1)
		s1 := lp.highlowcontainer.getKeyAtIndex(pos1)
		lp.highlowcontainer.replaceKeyAndContainerAtIndex(outSize, s1, c1)
		if outSize != pos1 {
			lp.highlowcontainer.clearContainerAtIndex(pos1)
		}
		outSize++
		pos1++
	}

	lp.highlowcontainer.releaseContainersInRange(outSize)
	lp.highlowcontainer.resize(outSize)
}

type largeIterator struct {
	pos              int
	hs               uint64
	iter             intPeekable
	highlowcontainer *largeArray
}

func (it *largeIterator) HasNext() bool {
	return it.pos < it.highlowcontainer.size()
}

func (it *largeIterator) init() {
	if it.iter != nil {
		releaseIterator(it.iter)
		it.iter = nil
	}
	if it.highlowcontainer.size() > it.pos {
		it.iter = it.highlowcontainer.getContainerAtIndex(it.pos).iterator()
		it.hs = uint64(it.highlowcontainer.getKeyAtIndex(it.pos)) << 32
	}
}

func (it *largeIterator) Next() uint64 {
	lo := it.iter.next()
	x := uint64(lo) | it.hs
	if !it.iter.hasNext() {
		it.pos++
		it.init()
	}
	return x
}

func (it *largeIterator) peekNext() uint64 {
	return uint64(it.iter.peekNext()&maxLargeLowBit) | it.hs
}

func (it *largeIterator) advanceIfNeeded(minval uint64) {
	to := minval >> 32

	for it.HasNext() && (it.hs>>32) < to {
		it.pos++
		it.init()
	}

	if it.HasNext() && (it.hs>>32) == to {
		it.iter.advanceIfNeeded(lowbits64(minval))
		if !it.iter.hasNext() {
			it.pos++
			it.init()
		}
	}
}

func (it *largeIterator) initialize(lp *largePosting) {
	it.pos = 0
	it.hs = 0
	it.iter = nil
	it.highlowcontainer = &lp.highlowcontainer
	it.init()
}

func (it *largeIterator) Release() {
	releaseLargeIterator(it)
}

func newLargeIterator(lp *largePosting) *largeIterator {
	return acquireLargeIterator(lp)
}

func writeLarge(writer *bufio.Writer, lp *largePosting) error {
	if lp == nil {
		return writeUvarint(writer, 0)
	}
	size := lp.serializedSizeInBytes()
	if err := writeUvarint(writer, size); err != nil {
		return err
	}
	if size == 0 {
		return nil
	}
	n, err := lp.WriteTo(writer)
	if err != nil {
		return err
	}
	if uint64(n) != size {
		return fmt.Errorf("large posting write size mismatch: wrote %v expected %v", n, size)
	}
	return nil
}

func readLarge(reader *bufio.Reader) (lp *largePosting, err error) {
	var size uint64
	size, err = binary.ReadUvarint(reader)
	if err != nil {
		return nil, err
	}
	if size == 0 {
		lp = getLargePosting()
		return lp, nil
	}
	if size > (^uint64(0) >> 1) {
		return nil, fmt.Errorf("large posting size overflows int64: %v", size)
	}
	lp = getLargePosting()
	lp.clear()
	defer func() {
		if r := recover(); r != nil {
			releaseLargePosting(lp)
			lp = nil
			err = fmt.Errorf("corrupted large posting payload: %v", r)
		}
	}()
	var n int64
	n, err = lp.ReadFrom(reader)
	if err != nil {
		releaseLargePosting(lp)
		return nil, err
	}
	if uint64(n) != size {
		releaseLargePosting(lp)
		return nil, fmt.Errorf("large posting read size mismatch: read %v, expected %v", n, size)
	}
	return lp, nil
}

func skipLarge(reader *bufio.Reader) error {
	size, err := binary.ReadUvarint(reader)
	if err != nil {
		return err
	}
	if size == 0 {
		return nil
	}
	if size > (^uint64(0) >> 1) {
		return fmt.Errorf("large posting size overflows int64: %v", size)
	}
	_, err = io.CopyN(io.Discard, reader, int64(size))
	return err
}

var (
	largePostingPool  sync.Pool
	largeIteratorPool sync.Pool
	addManyBatchPool  sync.Pool
)

type addManyBatch struct {
	values []uint32
}

const maxPooledAddManyBatchCapacity = 128 << 10

func getLargePosting() *largePosting {
	if v := largePostingPool.Get(); v != nil {
		return v.(*largePosting)
	}
	return newLargePosting()
}

func releaseLargePosting(lp *largePosting) {
	if lp == nil {
		return
	}
	lp.clear()
	largePostingPool.Put(lp)
}

func cloneLargeShared(src *largePosting) *largePosting {
	dst := getLargePosting()
	if src == nil {
		return dst
	}
	return src.cloneSharedInto(dst)
}

func acquireLargeIterator(lp *largePosting) *largeIterator {
	if v := largeIteratorPool.Get(); v != nil {
		it := v.(*largeIterator)
		it.initialize(lp)
		return it
	}
	it := &largeIterator{}
	it.initialize(lp)
	return it
}

func releaseLargeIterator(it *largeIterator) {
	if it == nil {
		return
	}
	if it.iter != nil {
		releaseIterator(it.iter)
	}
	it.pos = 0
	it.hs = 0
	it.iter = nil
	it.highlowcontainer = nil
	largeIteratorPool.Put(it)
}

func acquireAddManyBatch(capHint int) *addManyBatch {
	if capHint < 32 {
		capHint = 32
	}

	if v := addManyBatchPool.Get(); v != nil {
		buf := v.(*addManyBatch)
		if cap(buf.values) < capHint {
			buf.values = slices.Grow(buf.values, capHint)
		}
		return buf
	}

	return &addManyBatch{values: make([]uint32, 0, capHint)}
}

func releaseAddManyBatch(buf *addManyBatch) {
	if buf == nil {
		return
	}
	if cap(buf.values) > maxPooledAddManyBatchCapacity {
		return
	}
	buf.values = buf.values[:0]
	addManyBatchPool.Put(buf)
}

func highbits64(x uint64) uint32 {
	return uint32(x >> 32)
}

func lowbits64(x uint64) uint32 {
	return uint32(x & uint64(maxLargeLowBit))
}

const maxLargeLowBit = uint32(0xFFFFFFFF)

type largeArray struct {
	keys       []uint32
	containers []*bitmap32
}

func (la *largeArray) aliases(other *largeArray) bool {
	if la == nil || other == nil {
		return false
	}
	return slicesShareBacking64(la.keys, other.keys) || slicesShareBacking64(la.containers, other.containers)
}

func slicesShareBacking64[T any](a, b []T) bool {
	if cap(a) == 0 || cap(b) == 0 {
		return false
	}
	return unsafe.SliceData(a) == unsafe.SliceData(b)
}

func (la *largeArray) runOptimize() {
	for i := range la.containers {
		la.getWritableContainerAtIndex(i).runOptimize()
	}
}

func (la *largeArray) appendContainer(key uint32, value *bitmap32) {
	la.keys = append(la.keys, key)
	la.containers = append(la.containers, value)
}

func (la *largeArray) appendCopy(src largeArray, idx int) {
	la.appendContainer(src.keys[idx], src.containers[idx].clone())
}

func (la *largeArray) appendCopyMany(src largeArray, start, end int) {
	for i := start; i < end; i++ {
		la.appendCopy(src, i)
	}
}

func (la *largeArray) grow(newsize int) {
	if newsize <= len(la.keys) {
		return
	}
	oldSize := len(la.keys)
	if cap(la.keys) >= newsize {
		la.keys = la.keys[:newsize]
		clear(la.keys[oldSize:])
	} else {
		keys := make([]uint32, newsize)
		copy(keys, la.keys)
		la.keys = keys
	}
	if cap(la.containers) >= newsize {
		la.containers = la.containers[:newsize]
		clear(la.containers[oldSize:])
	} else {
		containers := make([]*bitmap32, newsize)
		copy(containers, la.containers)
		la.containers = containers
	}
}

func (la *largeArray) moveKeyValueAt(src, dst int) {
	if src == dst {
		return
	}
	la.keys[dst] = la.keys[src]
	la.containers[dst] = la.containers[src]
	la.keys[src] = 0
	la.containers[src] = nil
}

func (la *largeArray) resize(newsize int) {
	clear(la.keys[newsize:])
	clear(la.containers[newsize:])

	la.keys = la.keys[:newsize]
	la.containers = la.containers[:newsize]
}

func (la *largeArray) clear() {
	la.releaseContainersInRange(0)
	la.resize(0)
}

func (la *largeArray) clearContainerAtIndex(i int) {
	la.keys[i] = 0
	la.containers[i] = nil
}

func (la *largeArray) releaseContainersInRange(start int) {
	if start < 0 {
		start = 0
	}
	for i := start; i < len(la.containers); i++ {
		if c := la.containers[i]; c != nil {
			releaseBitmap(c)
			la.clearContainerAtIndex(i)
		}
	}
}

func (la *largeArray) serializedSizeInBytes() uint64 {
	size := uint64(8)
	for _, c := range la.containers {
		size += 4
		size += c.serializedSizeInBytes()
	}
	return size
}

func (la *largeArray) copyFrom(src *largeArray) {
	if la == src {
		return
	}
	if la.aliases(src) {
		tmp := new(largeArray)
		tmp.copyFrom(src)
		la.keys = tmp.keys
		la.containers = tmp.containers
		return
	}

	la.releaseContainersInRange(0)

	if len(src.keys) == 0 {
		la.keys = la.keys[:0]
		la.containers = la.containers[:0]
		return
	}

	if cap(la.keys) >= len(src.keys) {
		la.keys = la.keys[:len(src.keys)]
	} else {
		la.keys = make([]uint32, len(src.keys))
	}
	copy(la.keys, src.keys)

	if cap(la.containers) >= len(src.containers) {
		la.containers = la.containers[:len(src.containers)]
	} else {
		la.containers = make([]*bitmap32, len(src.containers))
	}
	for i := range src.containers {
		la.containers[i] = src.containers[i].clone()
	}
}

func (la *largeArray) copySharedFrom(src *largeArray) {
	if la == src {
		return
	}
	if la.aliases(src) {
		tmp := new(largeArray)
		tmp.copySharedFrom(src)
		la.keys = tmp.keys
		la.containers = tmp.containers
		return
	}

	la.releaseContainersInRange(0)

	if len(src.keys) == 0 {
		la.keys = la.keys[:0]
		la.containers = la.containers[:0]
		return
	}

	if cap(la.keys) >= len(src.keys) {
		la.keys = la.keys[:len(src.keys)]
	} else {
		la.keys = make([]uint32, len(src.keys))
	}
	copy(la.keys, src.keys)

	if cap(la.containers) >= len(src.containers) {
		la.containers = la.containers[:len(src.containers)]
	} else {
		la.containers = make([]*bitmap32, len(src.containers))
	}
	for i := range src.containers {
		la.containers[i] = retainBitmap32(src.containers[i])
	}
}

func (la *largeArray) getContainer(x uint32) *bitmap32 {
	i := la.binarySearch(0, int64(len(la.keys)), x)
	if i < 0 {
		return nil
	}
	return la.containers[i]
}

func (la *largeArray) getContainerAtIndex(i int) *bitmap32 {
	return la.containers[i]
}

func (la *largeArray) getWritableContainerAtIndex(i int) *bitmap32 {
	current := la.containers[i]
	if current.isUniquelyOwned() {
		return current
	}
	cloned := current.cloneSharedInto(newBitmap())
	releaseBitmap(current)
	la.containers[i] = cloned
	return cloned
}

func (la *largeArray) getIndex(x uint32) int {
	size := len(la.keys)
	if size == 0 {
		return -1
	}
	last := la.keys[size-1]
	if last == x {
		return size - 1
	}
	if last < x {
		return -(size + 1)
	}
	return la.binarySearch(0, int64(size), x)
}

func (la *largeArray) getKeyAtIndex(i int) uint32 {
	return la.keys[i]
}

func (la *largeArray) insertNewKeyValueAt(i int, key uint32, value *bitmap32) {
	if i == len(la.keys) {
		la.keys = append(la.keys, key)
		la.containers = append(la.containers, value)
		return
	}

	la.keys = append(la.keys, 0)
	la.containers = append(la.containers, nil)

	copy(la.keys[i+1:], la.keys[i:])
	copy(la.containers[i+1:], la.containers[i:])

	la.keys[i] = key
	la.containers[i] = value
}

func (la *largeArray) removeAtIndex(i int) {
	removed := la.containers[i]
	last := len(la.keys) - 1
	copy(la.keys[i:], la.keys[i+1:])
	copy(la.containers[i:], la.containers[i+1:])
	la.keys[last] = 0
	la.containers[last] = nil
	la.keys = la.keys[:last]
	la.containers = la.containers[:last]
	if removed != nil {
		releaseBitmap(removed)
	}
}

func (la *largeArray) setContainerAtIndex(i int, c *bitmap32) {
	if old := la.containers[i]; old != nil && old != c {
		releaseBitmap(old)
	}
	la.containers[i] = c
}

func (la *largeArray) replaceKeyAndContainerAtIndex(i int, key uint32, c *bitmap32) {
	la.keys[i] = key
	if old := la.containers[i]; old != nil && old != c {
		releaseBitmap(old)
	}
	la.containers[i] = c
}

func (la *largeArray) size() int {
	return len(la.keys)
}

func (la *largeArray) binarySearch(begin, end int64, key uint32) int {
	low := begin
	high := end - 1
	for low+16 <= high {
		middleIndex := low + (high-low)/2
		middleValue := la.keys[middleIndex]

		if middleValue < key {
			low = middleIndex + 1
		} else if middleValue > key {
			high = middleIndex - 1
		} else {
			return int(middleIndex)
		}
	}
	for ; low <= high; low++ {
		val := la.keys[low]
		if val >= key {
			if val == key {
				return int(low)
			}
			break
		}
	}
	return -int(low + 1)
}

func (la *largeArray) equals(other *largeArray) bool {
	if other == nil {
		return false
	}
	if other.size() != la.size() {
		return false
	}
	for i, key := range la.keys {
		if key != other.keys[i] {
			return false
		}
	}
	for i, c := range la.containers {
		if !c.equals(other.containers[i]) {
			return false
		}
	}
	return true
}

func (la *largeArray) advanceUntil(min uint32, pos int) int {
	lower := pos + 1

	if lower >= len(la.keys) || la.keys[lower] >= min {
		return lower
	}

	spansize := 1
	for lower+spansize < len(la.keys) && la.keys[lower+spansize] < min {
		spansize *= 2
	}

	var upper int
	if lower+spansize < len(la.keys) {
		upper = lower + spansize
	} else {
		upper = len(la.keys) - 1
	}

	if la.keys[upper] == min {
		return upper
	}
	if la.keys[upper] < min {
		return len(la.keys)
	}

	lower += spansize >> 1
	for lower+1 != upper {
		mid := (lower + upper) >> 1
		if la.keys[mid] == min {
			return mid
		}
		if la.keys[mid] < min {
			lower = mid
		} else {
			upper = mid
		}
	}
	return upper
}
