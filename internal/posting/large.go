package posting

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
	"slices"
	"unsafe"

	"github.com/vapstack/rbi/internal/pooled"
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
	lp.highlowcontainer.setSize(int(size))

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
		lp.highlowcontainer.containers[i] = bitmapPool.Get()
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
	it := largeIteratorPool.Get()
	it.initialize(lp)
	return it
}

func (lp *largePosting) release() {
	largePostingPool.Put(lp)
}

func (lp *largePosting) Release() { lp.release() }

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
	next := bitmapPool.Get()
	next.add(lowbits64(x))
	la.insertNewKeyValueAt(-i-1, hb, next)
}

func (lp *largePosting) checkedAdd(x uint64) bool {
	hb := highbits64(x)
	i := lp.highlowcontainer.getIndex(hb)
	if i >= 0 {
		return lp.highlowcontainer.getWritableContainerAtIndex(i).checkedAdd(lowbits64(x))
	}
	next := bitmapPool.Get()
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

	start := 0
	batchHighBits := highbits64(dat[0])
	for end := 1; end < len(dat); end++ {
		hi := highbits64(dat[end])
		if hi != batchHighBits {
			lp.getOrCreateContainer(batchHighBits).addMany64Low(dat[start:end])
			batchHighBits = hi
			start = end
		}
	}
	lp.getOrCreateContainer(batchHighBits).addMany64Low(dat[start:])
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
		next := bitmapPool.Get()
		next.loadManySorted64(dat)
		la.insertNewKeyValueAt(-i-1, hb, next)
		return
	}
	la.getWritableContainerAtIndex(i).addManySorted64Low(dat)
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
	next := bitmapPool.Get()
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
	defer it.Release()

	otherIt := other.iterator()
	defer otherIt.Release()

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
		it.iter.release()
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
	largeIteratorPool.Put(it)
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
		lp = largePostingPool.Get()
		return lp, nil
	}
	if size > (^uint64(0) >> 1) {
		return nil, fmt.Errorf("large posting size overflows int64: %v", size)
	}
	lp = largePostingPool.Get()
	lp.clear()
	defer recoverLargeRead(&lp, &err)
	var n int64
	n, err = lp.ReadFrom(reader)
	if err != nil {
		lp.release()
		return nil, err
	}
	if uint64(n) != size {
		lp.release()
		return nil, fmt.Errorf("large posting read size mismatch: read %v, expected %v", n, size)
	}
	return lp, nil
}

func recoverLargeRead(lp **largePosting, err *error) {
	if r := recover(); r != nil {
		(*lp).release()
		*lp = nil
		*err = fmt.Errorf("corrupted large posting payload: %v", r)
	}
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
	largePostingPool = pooled.Pointers[largePosting]{
		New: func() *largePosting {
			return new(largePosting)
		},
		Cleanup: func(lp *largePosting) {
			lp.clear()
		},
	}
	largeIteratorPool = pooled.Pointers[largeIterator]{
		Cleanup: func(it *largeIterator) {
			if it.iter != nil {
				it.iter.release()
			}
			it.pos = 0
			it.hs = 0
			it.iter = nil
			it.highlowcontainer = nil
		},
	}
)

func init() {
	for i, maxcap := range largeArrayPoolCapacities {
		c := maxcap
		largeArrayStoragePools[i] = pooled.Pointers[largeArrayStorage]{
			New: func() *largeArrayStorage {
				return &largeArrayStorage{
					keys:       make([]uint32, 0, c),
					containers: make([]*bitmap32, 0, c),
				}
			},
			Cleanup: func(storage *largeArrayStorage) {
				clear(storage.keys[:cap(storage.keys)])
				storage.keys = storage.keys[:0]
				clear(storage.containers[:cap(storage.containers)])
				storage.containers = storage.containers[:0]
			},
		}
	}
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
	storage    *largeArrayStorage
	inlineKeys [4]uint32
	inlineVals [4]*bitmap32
}

type largeArrayStorage struct {
	keys       []uint32
	containers []*bitmap32
}

var largeArrayPoolCapacities = [...]int{
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
	131072,
	262144,
	524288,
	1048576,
}

const maxPooledLargeArrayCapacity = 1 << 20

var largeArrayStoragePools [len(largeArrayPoolCapacities)]pooled.Pointers[largeArrayStorage]

func (la *largeArray) ensureInline() {
	if la == nil || la.keys != nil || la.containers != nil {
		return
	}
	la.keys = la.inlineKeys[:0]
	la.containers = la.inlineVals[:0]
}

func largeArrayPoolIndex(size int) int {
	if size <= largeArrayPoolCapacities[0] {
		return 0
	}
	if size > maxPooledLargeArrayCapacity {
		return -1
	}
	return bits.Len(uint(size-1)) - 3
}

func getLargeArrayStorageWithLen(l int) *largeArrayStorage {
	if l <= 0 {
		l = largeArrayPoolCapacities[0]
	}
	idx := largeArrayPoolIndex(l)
	if idx < 0 {
		return &largeArrayStorage{
			keys:       make([]uint32, l),
			containers: make([]*bitmap32, l),
		}
	}
	out := largeArrayStoragePools[idx].Get()
	out.keys = out.keys[:l]
	out.containers = out.containers[:l]
	return out
}

func putLargeArrayStorage(storage *largeArrayStorage) {
	if storage == nil {
		return
	}
	idx := largeArrayPoolIndex(cap(storage.keys))
	if idx < 0 {
		return
	}
	largeArrayStoragePools[idx].Put(storage)
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
	i := len(la.keys)
	la.setSize(i + 1)
	la.keys[i] = key
	la.containers[i] = value
}

func (la *largeArray) appendCopy(src largeArray, idx int) {
	la.appendContainer(src.keys[idx], src.containers[idx].clone())
}

func (la *largeArray) appendCopyMany(src largeArray, start, end int) {
	for i := start; i < end; i++ {
		la.appendCopy(src, i)
	}
}

func (la *largeArray) releaseBacking() {
	if la.storage != nil {
		putLargeArrayStorage(la.storage)
		la.storage = nil
	}
	la.keys = nil
	la.containers = nil
}

func (la *largeArray) setSize(newsize int) {
	la.ensureInline()
	oldLen := len(la.keys)
	if cap(la.keys) >= newsize {
		if newsize > oldLen {
			la.keys = la.keys[:newsize]
			clear(la.keys[oldLen:newsize])
			la.containers = la.containers[:newsize]
			clear(la.containers[oldLen:newsize])
		} else {
			clear(la.keys[newsize:oldLen])
			clear(la.containers[newsize:oldLen])
			la.keys = la.keys[:newsize]
			la.containers = la.containers[:newsize]
		}
		return
	}

	oldKeys := la.keys
	oldContainers := la.containers
	oldStorage := la.storage

	if newsize <= len(la.inlineKeys) {
		la.storage = nil
		la.keys = la.inlineKeys[:newsize]
		la.containers = la.inlineVals[:newsize]
	} else {
		next := getLargeArrayStorageWithLen(newsize)
		la.storage = next
		la.keys = next.keys[:newsize]
		la.containers = next.containers[:newsize]
	}

	copied := min(len(oldKeys), newsize)
	copy(la.keys, oldKeys[:copied])
	copy(la.containers, oldContainers[:copied])
	clear(la.keys[copied:newsize])
	clear(la.containers[copied:newsize])

	if oldStorage != nil {
		putLargeArrayStorage(oldStorage)
	}
}

func (la *largeArray) copyAliasedFrom(src *largeArray, retain bool) {
	n := len(src.keys)
	if n == 0 {
		la.storage = nil
		la.keys = la.inlineKeys[:0]
		la.containers = la.inlineVals[:0]
		return
	}

	if n <= len(la.inlineKeys) {
		var keys [4]uint32
		var containers [4]*bitmap32
		copy(keys[:n], src.keys)
		for i := 0; i < n; i++ {
			if retain {
				containers[i] = src.containers[i].retain()
			} else {
				containers[i] = src.containers[i].clone()
			}
		}

		la.storage = nil
		la.keys = la.inlineKeys[:n]
		la.containers = la.inlineVals[:n]
		copy(la.keys, keys[:n])
		copy(la.containers, containers[:n])
		return
	}

	next := getLargeArrayStorageWithLen(n)
	copy(next.keys, src.keys)
	for i := 0; i < n; i++ {
		if retain {
			next.containers[i] = src.containers[i].retain()
		} else {
			next.containers[i] = src.containers[i].clone()
		}
	}

	la.storage = next
	la.keys = next.keys[:n]
	la.containers = next.containers[:n]
}

func (la *largeArray) grow(newsize int) {
	if newsize <= len(la.keys) {
		return
	}
	la.setSize(newsize)
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
			c.release()
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
		la.copyAliasedFrom(src, false)
		return
	}

	la.releaseContainersInRange(0)

	if len(src.keys) == 0 {
		la.setSize(0)
		return
	}

	la.setSize(len(src.keys))
	copy(la.keys, src.keys)
	for i := range src.containers {
		la.containers[i] = src.containers[i].clone()
	}
}

func (la *largeArray) copySharedFrom(src *largeArray) {
	if la == src {
		return
	}
	if la.aliases(src) {
		la.copyAliasedFrom(src, true)
		return
	}

	la.releaseContainersInRange(0)

	if len(src.keys) == 0 {
		la.setSize(0)
		return
	}

	la.setSize(len(src.keys))
	copy(la.keys, src.keys)
	for i := range src.containers {
		la.containers[i] = src.containers[i].retain()
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
	cloned := current.cloneSharedInto(bitmapPool.Get())
	current.release()
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
		la.appendContainer(key, value)
		return
	}

	oldLen := len(la.keys)
	la.setSize(oldLen + 1)
	copy(la.keys[i+1:], la.keys[i:oldLen])
	copy(la.containers[i+1:], la.containers[i:oldLen])

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
		removed.release()
	}
}

func (la *largeArray) setContainerAtIndex(i int, c *bitmap32) {
	if old := la.containers[i]; old != nil && old != c {
		old.release()
	}
	la.containers[i] = c
}

func (la *largeArray) replaceKeyAndContainerAtIndex(i int, key uint32, c *bitmap32) {
	la.keys[i] = key
	if old := la.containers[i]; old != nil && old != c {
		old.release()
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
