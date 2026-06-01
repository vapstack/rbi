package posting

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
	"unsafe"
)

func writeUvarint(writer *bufio.Writer, v uint64) error {
	for v >= 0x80 {
		if err := writer.WriteByte(byte(v) | 0x80); err != nil {
			return err
		}
		v >>= 7
	}
	return writer.WriteByte(byte(v))
}

func writeLEUint16(writer *bufio.Writer, v uint16) (int, error) {
	if writer.Available() >= 2 {
		buf := writer.AvailableBuffer()
		buf = binary.LittleEndian.AppendUint16(buf, v)
		return writer.Write(buf)
	}
	if err := writer.WriteByte(byte(v)); err != nil {
		return 0, err
	}
	if err := writer.WriteByte(byte(v >> 8)); err != nil {
		return 1, err
	}
	return 2, nil
}

func writeLEUint32(writer *bufio.Writer, v uint32) (int, error) {
	if writer.Available() >= 4 {
		buf := writer.AvailableBuffer()
		buf = binary.LittleEndian.AppendUint32(buf, v)
		return writer.Write(buf)
	}
	if err := writer.WriteByte(byte(v)); err != nil {
		return 0, err
	}
	if err := writer.WriteByte(byte(v >> 8)); err != nil {
		return 1, err
	}
	if err := writer.WriteByte(byte(v >> 16)); err != nil {
		return 2, err
	}
	if err := writer.WriteByte(byte(v >> 24)); err != nil {
		return 3, err
	}
	return 4, nil
}

func writeLEUint64(writer *bufio.Writer, v uint64) (int, error) {
	if writer.Available() >= 8 {
		buf := writer.AvailableBuffer()
		buf = binary.LittleEndian.AppendUint64(buf, v)
		return writer.Write(buf)
	}
	if err := writer.WriteByte(byte(v)); err != nil {
		return 0, err
	}
	if err := writer.WriteByte(byte(v >> 8)); err != nil {
		return 1, err
	}
	if err := writer.WriteByte(byte(v >> 16)); err != nil {
		return 2, err
	}
	if err := writer.WriteByte(byte(v >> 24)); err != nil {
		return 3, err
	}
	if err := writer.WriteByte(byte(v >> 32)); err != nil {
		return 4, err
	}
	if err := writer.WriteByte(byte(v >> 40)); err != nil {
		return 5, err
	}
	if err := writer.WriteByte(byte(v >> 48)); err != nil {
		return 6, err
	}
	if err := writer.WriteByte(byte(v >> 56)); err != nil {
		return 7, err
	}
	return 8, nil
}

func appendUvarint(dst []byte, v uint64) []byte {
	for v >= 0x80 {
		dst = append(dst, byte(v)|0x80)
		v >>= 7
	}
	return append(dst, byte(v))
}

func writeCompactPosting(writer *bufio.Writer, tag byte, ids []uint64) error {
	if writer.Available() >= 11+len(ids)*10 {
		buf := writer.AvailableBuffer()
		buf = append(buf, tag)
		buf = appendUvarint(buf, uint64(len(ids)))
		var prev uint64
		for i := 0; i < len(ids); i++ {
			v := ids[i]
			delta := v
			if i > 0 {
				delta -= prev
			}
			buf = appendUvarint(buf, delta)
			prev = v
		}
		_, err := writer.Write(buf)
		return err
	}

	if err := writer.WriteByte(tag); err != nil {
		return err
	}
	if err := writeUvarint(writer, uint64(len(ids))); err != nil {
		return err
	}
	var prev uint64
	for i := 0; i < len(ids); i++ {
		v := ids[i]
		delta := v
		if i > 0 {
			delta -= prev
		}
		if err := writeUvarint(writer, delta); err != nil {
			return err
		}
		prev = v
	}
	return nil
}

func readUvarint(reader *bufio.Reader) (uint64, error) {
	var x uint64
	var s uint
	for i := 0; i < 10; i++ {
		b, err := reader.ReadByte()
		if err != nil {
			if i > 0 && err == io.EOF {
				return x, io.ErrUnexpectedEOF
			}
			return x, err
		}
		if b < 0x80 {
			if i == 9 && b > 1 {
				return 0, fmt.Errorf("binary: varint overflows a 64-bit integer")
			}
			return x | uint64(b)<<s, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, fmt.Errorf("binary: varint overflows a 64-bit integer")
}

func readFullBufio(reader *bufio.Reader, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		read, err := reader.Read(buf[n:])
		n += read
		if n == len(buf) {
			return n, nil
		}
		if err != nil {
			if err == io.EOF && n > 0 {
				return n, io.ErrUnexpectedEOF
			}
			return n, err
		}
		if read == 0 {
			return n, io.ErrNoProgress
		}
	}
	return n, nil
}

func skipUvarint(reader *bufio.Reader) error {
	for i := 0; i < 10; i++ {
		b, err := reader.ReadByte()
		if err != nil {
			if i > 0 && err == io.EOF {
				return io.ErrUnexpectedEOF
			}
			return err
		}
		if b < 0x80 {
			if i == 9 && b > 1 {
				return fmt.Errorf("binary: varint overflows a 64-bit integer")
			}
			return nil
		}
	}
	return fmt.Errorf("binary: varint overflows a 64-bit integer")
}

// List is a 16-byte value handle over adaptive posting payloads.
// Production code must keep it value-shaped: pass and return List by value,
// never as *List.
//
// It keeps posting ids in an adaptive representation:
// singleton id for cardinality=1, inline small posting for tiny sets,
// large posting for larger sets.
//
// Encoding:
//   - ptr == nil: empty
//   - ptr == singleValue: single value (in single)
//   - ptr != nil: kind is encoded in single
type List struct {
	ptr    unsafe.Pointer
	single uint64
}

type ContainsCursor struct {
	kind   uint64
	single uint64
	small  *smallPosting
	mid    *midPosting
	large  *largePosting

	largeKey uint32
	largeSet bool
	rb       *bitmap32

	bitmapKey uint16
	bitmapSet bool
	container container16
}

var singleValue = unsafe.Pointer(new(byte))

const (
	postingMetaBorrowed uint64 = 1 << 63
	postingMetaKindMask uint64 = 3 << 61
	postingKindLarge    uint64 = 0
	postingKindSmall    uint64 = 1 << 61
	postingKindMid      uint64 = 2 << 61
	SmallCap                   = 8
	MidCap                     = 32
)

const (
	encodingEmpty     byte = 0
	encodingSingleton byte = 1
	encodingLarge     byte = 2
	encodingSmall     byte = 3
	encodingMid       byte = 4
)

type smallPosting struct {
	n   uint8
	ids [SmallCap]uint64
}

type midPosting struct {
	n   uint8
	ids [MidCap]uint64
}

type arrayIter struct {
	ids []uint64
	i   int
}

func cloneSmallPosting(sp *smallPosting) *smallPosting {
	clone := getSmallPosting()
	*clone = *sp
	return clone
}

func cloneMidPosting(mp *midPosting) *midPosting {
	clone := getMidPosting()
	*clone = *mp
	return clone
}

func singleton(id uint64) List {
	return List{
		ptr:    singleValue,
		single: id,
	}
}

func smallValue(sp *smallPosting) List {
	return List{
		ptr:    unsafe.Pointer(sp),
		single: postingKindSmall,
	}
}

func midValue(mp *midPosting) List {
	return List{
		ptr:    unsafe.Pointer(mp),
		single: postingKindMid,
	}
}

func largeValue(lp *largePosting) List {
	return List{
		ptr:    unsafe.Pointer(lp),
		single: postingKindLarge,
	}
}

func newSmall(ids ...uint64) List {
	switch len(ids) {
	case 0:
		return List{}
	case 1:
		return singleton(ids[0])
	}
	sp := getSmallPosting()
	sp.n = uint8(len(ids))
	copy(sp.ids[:], ids)
	return smallValue(sp)
}

func compactListFromSorted(ids []uint64) List {
	switch len(ids) {
	case 0:
		return List{}
	case 1:
		return singleton(ids[0])
	}
	if len(ids) <= SmallCap {
		sp := getSmallPosting()
		sp.n = uint8(len(ids))
		copy(sp.ids[:len(ids)], ids)
		return smallValue(sp)
	}
	mp := getMidPosting()
	mp.n = uint8(len(ids))
	copy(mp.ids[:len(ids)], ids)
	return midValue(mp)
}

func BuildFromSorted(ids []uint64) List {
	switch len(ids) {
	case 0:
		return List{}
	case 1:
		return singleton(ids[0])
	case 2, 3, 4, 5, 6, 7, 8:
		sp := getSmallPosting()
		sp.n = uint8(len(ids))
		copy(sp.ids[:len(ids)], ids)
		return smallValue(sp)
	case 9, 10, 11, 12, 13, 14, 15, 16,
		17, 18, 19, 20, 21, 22, 23, 24,
		25, 26, 27, 28, 29, 30, 31, 32:
		mp := getMidPosting()
		mp.n = uint8(len(ids))
		copy(mp.ids[:len(ids)], ids)
		return midValue(mp)
	default:
		lp := getLargePosting()
		first := ids[0]
		last := ids[len(ids)-1]
		if last != ^uint64(0) && last-first == uint64(len(ids)-1) {
			lp.addRange(first, last+1)
			return largeValue(lp)
		}
		lp.loadSortedUnique(ids)
		return largeValue(lp)
	}
}

func (p List) isSmall() bool {
	return p.ptr != nil && p.ptr != singleValue && p.kind() == postingKindSmall
}

func (p List) isMid() bool {
	return p.ptr != nil && p.ptr != singleValue && p.kind() == postingKindMid
}

func (p List) small() *smallPosting {
	if p.ptr == nil || p.ptr == singleValue || p.kind() != postingKindSmall {
		return nil
	}
	return (*smallPosting)(p.ptr)
}

func (p List) mid() *midPosting {
	if p.ptr == nil || p.ptr == singleValue || p.kind() != postingKindMid {
		return nil
	}
	return (*midPosting)(p.ptr)
}

func (it *arrayIter) HasNext() bool {
	return it.i < len(it.ids)
}

func (it *arrayIter) Next() uint64 {
	if !it.HasNext() {
		return 0
	}
	v := it.ids[it.i]
	it.i++
	return v
}

func (it *arrayIter) AdvanceIfNeeded(minval uint64) {
	lo := it.i
	hi := len(it.ids)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if it.ids[mid] < minval {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	it.i = lo
}

func (p List) kind() uint64 {
	return p.single & postingMetaKindMask
}

func (p List) IsBorrowed() bool {
	return p.ptr != nil && p.ptr != singleValue && p.single&postingMetaBorrowed != 0
}

type PayloadKey struct {
	Ptr  uintptr
	Kind uint64
}

func (p List) PayloadKey() (PayloadKey, bool) {
	if p.ptr == nil || p.ptr == singleValue {
		return PayloadKey{}, false
	}
	return PayloadKey{
		Ptr:  uintptr(p.ptr),
		Kind: p.kind(),
	}, true
}

func (p List) IsOwnedLarge() bool {
	return p.largeRef() != nil && !p.IsBorrowed()
}

func (p List) Borrow() List {
	if p.ptr == nil || p.ptr == singleValue {
		return p
	}
	p.single |= postingMetaBorrowed
	return p
}

func (p List) SharesPayload(other List) bool {
	switch {
	case p.IsEmpty() || other.IsEmpty():
		return p.IsEmpty() && other.IsEmpty()
	case p.ptr == singleValue || other.ptr == singleValue:
		return p.ptr == singleValue && other.ptr == singleValue && p.single == other.single
	default:
		return p.ptr == other.ptr && p.kind() == other.kind()
	}
}

func compactFilterByMembership(ids List, other List, keepMatches bool) List {
	if sp := ids.small(); sp != nil {
		if otherSp := other.small(); otherSp != nil {
			return compactFilterBySorted(ids, sp.ids[:sp.n], otherSp.ids[:otherSp.n], keepMatches)
		}
		if otherMp := other.mid(); otherMp != nil {
			return compactFilterBySorted(ids, sp.ids[:sp.n], otherMp.ids[:otherMp.n], keepMatches)
		}
		if ids.IsBorrowed() {
			var kept [SmallCap]uint64
			n := 0
			for i := 0; i < int(sp.n); i++ {
				id := sp.ids[i]
				if other.Contains(id) == keepMatches {
					kept[n] = id
					n++
				}
			}
			return compactListFromSorted(kept[:n])
		}
		n := 0
		for i := 0; i < int(sp.n); i++ {
			id := sp.ids[i]
			if other.Contains(id) != keepMatches {
				continue
			}
			sp.ids[n] = id
			n++
		}
		switch n {
		case 0:
			sp.release()
			return List{}
		case 1:
			keep := sp.ids[0]
			sp.release()
			return singleton(keep)
		default:
			sp.n = uint8(n)
			return ids
		}
	}
	if mp := ids.mid(); mp != nil {
		if otherSp := other.small(); otherSp != nil {
			return compactFilterBySorted(ids, mp.ids[:mp.n], otherSp.ids[:otherSp.n], keepMatches)
		}
		if otherMp := other.mid(); otherMp != nil {
			return compactFilterBySorted(ids, mp.ids[:mp.n], otherMp.ids[:otherMp.n], keepMatches)
		}
		if ids.IsBorrowed() {
			var kept [MidCap]uint64
			n := 0
			for i := 0; i < int(mp.n); i++ {
				id := mp.ids[i]
				if other.Contains(id) == keepMatches {
					kept[n] = id
					n++
				}
			}
			return compactListFromSorted(kept[:n])
		}
		n := 0
		for i := 0; i < int(mp.n); i++ {
			id := mp.ids[i]
			if other.Contains(id) != keepMatches {
				continue
			}
			mp.ids[n] = id
			n++
		}
		switch {
		case n == 0:
			mp.release()
			return List{}
		case n == 1:
			keep := mp.ids[0]
			mp.release()
			return singleton(keep)
		case n <= SmallCap:
			sp := getSmallPosting()
			sp.n = uint8(n)
			copy(sp.ids[:n], mp.ids[:n])
			mp.release()
			return smallValue(sp)
		default:
			mp.n = uint8(n)
			return ids
		}
	}
	return ids
}

func compactFilterBySorted(ids List, values, other []uint64, keepMatches bool) List {
	var kept [MidCap]uint64
	n := 0
	j := 0
	if keepMatches {
		i := 0
		for j < len(other) && i < len(values) {
			id := other[j]
			for i < len(values) && values[i] < id {
				i++
			}
			if i == len(values) {
				break
			}
			if values[i] == id {
				kept[n] = id
				n++
				i++
			}
			j++
		}
	} else {
		for i := 0; i < len(values); i++ {
			id := values[i]
			for j < len(other) && other[j] < id {
				j++
			}
			if j == len(other) {
				copy(kept[n:], values[i:])
				n += len(values) - i
				break
			}
			if other[j] != id {
				kept[n] = id
				n++
			}
		}
	}
	return compactFilterResult(ids, kept[:], n)
}

func compactFilterResult(ids List, kept []uint64, n int) List {
	if ids.IsBorrowed() {
		return compactListFromSorted(kept[:n])
	}
	if sp := ids.small(); sp != nil {
		switch n {
		case 0:
			sp.release()
			return List{}
		case 1:
			keep := kept[0]
			sp.release()
			return singleton(keep)
		default:
			sp.n = uint8(n)
			copy(sp.ids[:n], kept[:n])
			return ids
		}
	}
	mp := ids.mid()
	switch {
	case n == 0:
		mp.release()
		return List{}
	case n == 1:
		keep := kept[0]
		mp.release()
		return singleton(keep)
	case n <= SmallCap:
		sp := getSmallPosting()
		sp.n = uint8(n)
		copy(sp.ids[:n], kept[:n])
		mp.release()
		return smallValue(sp)
	default:
		mp.n = uint8(n)
		copy(mp.ids[:n], kept[:n])
		return ids
	}
}

func buildCompactUnion(ids List, left, right []uint64) List {
	var merged [MidCap * 2]uint64
	i := 0
	j := 0
	n := 0
	for i < len(left) && j < len(right) {
		lv := left[i]
		rv := right[j]
		if lv < rv {
			merged[n] = lv
			n++
			i++
			continue
		}
		if rv < lv {
			merged[n] = rv
			n++
			j++
			continue
		}
		merged[n] = lv
		n++
		i++
		j++
	}
	for i < len(left) {
		merged[n] = left[i]
		n++
		i++
	}
	for j < len(right) {
		merged[n] = right[j]
		n++
		j++
	}

	if n <= MidCap {
		return compactUnionResult(ids, merged[:], n)
	}

	lp := getLargePosting()
	lp.loadSortedUnique(merged[:n])
	if !ids.IsBorrowed() {
		switch ids.single & postingMetaKindMask {
		case postingKindSmall:
			(*smallPosting)(ids.ptr).release()
		case postingKindMid:
			(*midPosting)(ids.ptr).release()
		}
	}
	return largeValue(lp)
}

func compactUnionResult(ids List, merged []uint64, n int) List {
	if ids.IsBorrowed() {
		return compactListFromSorted(merged[:n])
	}
	if n == 1 {
		keep := merged[0]
		switch ids.single & postingMetaKindMask {
		case postingKindSmall:
			(*smallPosting)(ids.ptr).release()
		case postingKindMid:
			(*midPosting)(ids.ptr).release()
		}
		return singleton(keep)
	}
	if n <= SmallCap {
		if sp := ids.small(); sp != nil {
			sp.n = uint8(n)
			copy(sp.ids[:n], merged[:n])
			return ids
		}
		mp := ids.mid()
		sp := getSmallPosting()
		sp.n = uint8(n)
		copy(sp.ids[:n], merged[:n])
		mp.release()
		return smallValue(sp)
	}
	if mp := ids.mid(); mp != nil {
		mp.n = uint8(n)
		copy(mp.ids[:n], merged[:n])
		return ids
	}
	sp := ids.small()
	mp := getMidPosting()
	mp.n = uint8(n)
	copy(mp.ids[:n], merged[:n])
	sp.release()
	return midValue(mp)
}

func postingBufContainsAny(posts []List, id uint64) bool {
	for i := 0; i < len(posts); i++ {
		if posts[i].Contains(id) {
			return true
		}
	}
	return false
}

func compactFilterByAnyMembership(ids List, other []List) List {
	if sp := ids.small(); sp != nil {
		if len(other) == 2 {
			if left, ok := compactPostingValues(other[0]); ok {
				if right, ok := compactPostingValues(other[1]); ok {
					return compactFilterByAny2Sorted(ids, sp.ids[:sp.n], left, right)
				}
			}
		}
		if ids.IsBorrowed() {
			var kept [SmallCap]uint64
			n := 0
			for i := 0; i < int(sp.n); i++ {
				id := sp.ids[i]
				if postingBufContainsAny(other, id) {
					kept[n] = id
					n++
				}
			}
			return compactListFromSorted(kept[:n])
		}
		n := 0
		for i := 0; i < int(sp.n); i++ {
			id := sp.ids[i]
			if !postingBufContainsAny(other, id) {
				continue
			}
			sp.ids[n] = id
			n++
		}
		switch n {
		case 0:
			sp.release()
			return List{}
		case 1:
			keep := sp.ids[0]
			sp.release()
			return singleton(keep)
		default:
			sp.n = uint8(n)
			return ids
		}
	}
	if mp := ids.mid(); mp != nil {
		if len(other) == 2 {
			if left, ok := compactPostingValues(other[0]); ok {
				if right, ok := compactPostingValues(other[1]); ok {
					return compactFilterByAny2Sorted(ids, mp.ids[:mp.n], left, right)
				}
			}
		}
		if ids.IsBorrowed() {
			var kept [MidCap]uint64
			n := 0
			for i := 0; i < int(mp.n); i++ {
				id := mp.ids[i]
				if postingBufContainsAny(other, id) {
					kept[n] = id
					n++
				}
			}
			return compactListFromSorted(kept[:n])
		}
		n := 0
		for i := 0; i < int(mp.n); i++ {
			id := mp.ids[i]
			if !postingBufContainsAny(other, id) {
				continue
			}
			mp.ids[n] = id
			n++
		}
		switch {
		case n == 0:
			mp.release()
			return List{}
		case n == 1:
			keep := mp.ids[0]
			mp.release()
			return singleton(keep)
		case n <= SmallCap:
			sp := getSmallPosting()
			sp.n = uint8(n)
			copy(sp.ids[:n], mp.ids[:n])
			mp.release()
			return smallValue(sp)
		default:
			mp.n = uint8(n)
			return ids
		}
	}
	return ids
}

func compactPostingValues(ids List) ([]uint64, bool) {
	if sp := ids.small(); sp != nil {
		return sp.ids[:sp.n], true
	}
	if mp := ids.mid(); mp != nil {
		return mp.ids[:mp.n], true
	}
	return nil, false
}

func compactFilterByAny2Sorted(ids List, values, left, right []uint64) List {
	var kept [MidCap]uint64
	n := 0
	j := 0
	k := 0
	for i := 0; i < len(values); i++ {
		id := values[i]
		for j < len(left) && left[j] < id {
			j++
		}
		matched := j < len(left) && left[j] == id
		if !matched {
			for k < len(right) && right[k] < id {
				k++
			}
			matched = k < len(right) && right[k] == id
		}
		if matched {
			kept[n] = id
			n++
		}
	}
	return compactFilterResult(ids, kept[:], n)
}

func (p List) largeRef() *largePosting {
	if p.ptr == nil || p.ptr == singleValue || p.kind() != postingKindLarge {
		return nil
	}
	return (*largePosting)(p.ptr)
}

func fromLargeOwned(lp *largePosting) List {
	if lp == nil {
		return List{}
	}
	if lp.isEmpty() {
		lp.release()
		return List{}
	}
	return fromLargeOwnedWithCardinality(lp, lp.cardinality())
}

func fromLargeOwnedWithCardinality(lp *largePosting, card uint64) List {
	if card == 1 {
		id := lp.minimum()
		lp.release()
		return singleton(id)
	}
	if card <= MidCap {
		n := int(card)
		var sp *smallPosting
		var mp *midPosting
		var dst []uint64
		if n <= SmallCap {
			sp = getSmallPosting()
			sp.n = uint8(n)
			dst = sp.ids[:n]
		} else {
			mp = getMidPosting()
			mp.n = uint8(n)
			dst = mp.ids[:n]
		}
		i := 0
		for j := 0; j < len(lp.highlowcontainer.keys); j++ {
			base32 := uint64(lp.highlowcontainer.keys[j]) << 32
			rb := lp.highlowcontainer.containers[j]
			for k := 0; k < len(rb.highlowcontainer.keys); k++ {
				base := base32 | uint64(rb.highlowcontainer.keys[k])<<16
				switch c := rb.highlowcontainer.containers[k].(type) {
				case *containerArray:
					for l := 0; l < len(c.content); l++ {
						dst[i] = base | uint64(c.content[l])
						i++
					}
				case *containerBitmap:
					for l := 0; l < len(c.bitmap); l++ {
						word := c.bitmap[l]
						for word != 0 {
							dst[i] = base | uint64(l*64+bits.TrailingZeros64(word))
							i++
							word &= word - 1
						}
					}
				case *containerRun:
					for l := 0; l < len(c.iv); l++ {
						last := int(c.iv[l].last())
						for v := int(c.iv[l].start); v <= last; v++ {
							dst[i] = base | uint64(v)
							i++
						}
					}
				}
			}
		}
		lp.release()
		if n <= SmallCap {
			return smallValue(sp)
		}
		return midValue(mp)
	}
	return largeValue(lp)
}

func fromLargeBorrowedWithCardinality(lp *largePosting, card uint64) List {
	if card == 1 {
		return singleton(lp.minimum())
	}
	n := int(card)
	var sp *smallPosting
	var mp *midPosting
	var dst []uint64
	if n <= SmallCap {
		sp = getSmallPosting()
		sp.n = uint8(n)
		dst = sp.ids[:n]
	} else {
		mp = getMidPosting()
		mp.n = uint8(n)
		dst = mp.ids[:n]
	}
	i := 0
	for j := 0; j < len(lp.highlowcontainer.keys); j++ {
		base32 := uint64(lp.highlowcontainer.keys[j]) << 32
		rb := lp.highlowcontainer.containers[j]
		for k := 0; k < len(rb.highlowcontainer.keys); k++ {
			base := base32 | uint64(rb.highlowcontainer.keys[k])<<16
			switch c := rb.highlowcontainer.containers[k].(type) {
			case *containerArray:
				for l := 0; l < len(c.content); l++ {
					dst[i] = base | uint64(c.content[l])
					i++
				}
			case *containerBitmap:
				for l := 0; l < len(c.bitmap); l++ {
					word := c.bitmap[l]
					for word != 0 {
						dst[i] = base | uint64(l*64+bits.TrailingZeros64(word))
						i++
						word &= word - 1
					}
				}
			case *containerRun:
				for l := 0; l < len(c.iv); l++ {
					last := int(c.iv[l].last())
					for v := int(c.iv[l].start); v <= last; v++ {
						dst[i] = base | uint64(v)
						i++
					}
				}
			}
		}
	}
	if n <= SmallCap {
		return smallValue(sp)
	}
	return midValue(mp)
}

func (p List) IsEmpty() bool {
	return p.ptr == nil
}

func (p List) TrySingle() (uint64, bool) {
	if p.ptr == nil {
		return 0, false
	}
	if p.ptr == singleValue {
		return p.single, true
	}
	switch p.single & postingMetaKindMask {
	case postingKindSmall:
		sp := (*smallPosting)(p.ptr)
		if sp.n == 1 {
			return sp.ids[0], true
		}
		return 0, false
	case postingKindMid:
		mp := (*midPosting)(p.ptr)
		if mp.n == 1 {
			return mp.ids[0], true
		}
		return 0, false
	default:
		lp := (*largePosting)(p.ptr)
		if id, ok := lp.trySingle(); ok {
			return id, true
		}
	}
	return 0, false
}

func (p List) TryAppendCompactTo(dst []uint64) ([]uint64, bool) {
	if p.ptr == nil {
		return dst, true
	}
	if p.ptr == singleValue {
		return append(dst, p.single), true
	}
	switch p.single & postingMetaKindMask {
	case postingKindSmall:
		sp := (*smallPosting)(p.ptr)
		return append(dst, sp.ids[:sp.n]...), true
	case postingKindMid:
		mp := (*midPosting)(p.ptr)
		return append(dst, mp.ids[:mp.n]...), true
	}
	return dst, false
}

func (p List) Cardinality() uint64 {
	if p.ptr == nil {
		return 0
	}
	if p.ptr == singleValue {
		return 1
	}
	switch p.single & postingMetaKindMask {
	case postingKindSmall:
		sp := (*smallPosting)(p.ptr)
		return uint64(sp.n)
	case postingKindMid:
		mp := (*midPosting)(p.ptr)
		return uint64(mp.n)
	default:
		return (*largePosting)(p.ptr).cardinality()
	}
}

func (p List) Contains(id uint64) bool {
	if p.ptr == nil {
		return false
	}
	if p.ptr == singleValue {
		return p.single == id
	}
	switch p.single & postingMetaKindMask {
	case postingKindSmall:
		sp := (*smallPosting)(p.ptr)
		for i := 0; i < int(sp.n); i++ {
			if sp.ids[i] == id {
				return true
			}
			if sp.ids[i] > id {
				return false
			}
		}
		return false
	case postingKindMid:
		mp := (*midPosting)(p.ptr)
		lo := 0
		hi := int(mp.n) - 1
		for lo <= hi {
			mid := int(uint(lo+hi) >> 1)
			v := mp.ids[mid]
			if v < id {
				lo = mid + 1
				continue
			}
			if v == id {
				return true
			}
			hi = mid - 1
		}
		return false
	default:
		return (*largePosting)(p.ptr).contains(id)
	}
}

func (c *ContainsCursor) Reset(p List) {
	*c = ContainsCursor{}
	if p.ptr == nil {
		return
	}
	if p.ptr == singleValue {
		c.kind = ^uint64(0)
		c.single = p.single
		return
	}
	c.kind = p.single & postingMetaKindMask
	switch c.kind {
	case postingKindSmall:
		c.small = (*smallPosting)(p.ptr)
	case postingKindMid:
		c.mid = (*midPosting)(p.ptr)
	default:
		c.large = (*largePosting)(p.ptr)
	}
}

func (c *ContainsCursor) Contains(id uint64) bool {
	switch c.kind {
	case ^uint64(0):
		return c.single == id
	case postingKindSmall:
		sp := c.small
		if sp == nil {
			return false
		}
		for i := 0; i < int(sp.n); i++ {
			if sp.ids[i] == id {
				return true
			}
			if sp.ids[i] > id {
				return false
			}
		}
		return false
	case postingKindMid:
		mp := c.mid
		if mp == nil {
			return false
		}
		lo := 0
		hi := int(mp.n) - 1
		for lo <= hi {
			mid := int(uint(lo+hi) >> 1)
			v := mp.ids[mid]
			if v < id {
				lo = mid + 1
				continue
			}
			if v == id {
				return true
			}
			hi = mid - 1
		}
		return false
	case postingKindLarge:
		lp := c.large
		if lp == nil {
			return false
		}
		hb64 := highbits64(id)
		if !c.largeSet || c.largeKey != hb64 {
			c.largeKey = hb64
			c.largeSet = true
			c.rb = lp.highlowcontainer.getContainer(hb64)
			c.bitmapSet = false
		}
		if c.rb == nil {
			return false
		}
		x := lowbits64(id)
		hb := highbits(x)
		if !c.bitmapSet || c.bitmapKey != hb {
			c.bitmapKey = hb
			c.bitmapSet = true
			c.container = c.rb.highlowcontainer.getContainer(hb)
		}
		return c.container != nil && c.container.contains(lowbits(x))
	default:
		return false
	}
}

func isNonDecreasingU64(dat []uint64) bool {
	for i := 1; i < len(dat); i++ {
		if dat[i] < dat[i-1] {
			return false
		}
	}
	return true
}

func (p List) Minimum() (uint64, bool) {
	if p.ptr == nil {
		return 0, false
	}
	if p.ptr == singleValue {
		return p.single, true
	}
	switch p.single & postingMetaKindMask {
	case postingKindSmall:
		return (*smallPosting)(p.ptr).ids[0], true
	case postingKindMid:
		return (*midPosting)(p.ptr).ids[0], true
	default:
		return (*largePosting)(p.ptr).minimum(), true
	}
}

func (p List) Maximum() (uint64, bool) {
	if p.ptr == nil {
		return 0, false
	}
	if p.ptr == singleValue {
		return p.single, true
	}
	switch p.single & postingMetaKindMask {
	case postingKindSmall:
		sp := (*smallPosting)(p.ptr)
		return sp.ids[sp.n-1], true
	case postingKindMid:
		mp := (*midPosting)(p.ptr)
		return mp.ids[mp.n-1], true
	default:
		return (*largePosting)(p.ptr).maximum(), true
	}
}

func (p List) SizeInBytes() uint64 {
	if p.ptr == nil {
		return 0
	}
	if p.ptr == singleValue {
		return 8
	}
	switch p.single & postingMetaKindMask {
	case postingKindSmall:
		sp := (*smallPosting)(p.ptr)
		return uint64(sp.n) * 8
	case postingKindMid:
		mp := (*midPosting)(p.ptr)
		return uint64(mp.n) * 8
	default:
		return (*largePosting)(p.ptr).sizeInBytes()
	}
}

func (p List) ToArray() []uint64 {
	if p.IsEmpty() {
		return nil
	}
	if p.ptr == singleValue {
		return []uint64{p.single}
	}
	if sp := p.small(); sp != nil {
		out := make([]uint64, int(sp.n))
		copy(out, sp.ids[:sp.n])
		return out
	}
	if mp := p.mid(); mp != nil {
		out := make([]uint64, int(mp.n))
		copy(out, mp.ids[:mp.n])
		return out
	}
	return p.largeRef().toArray()
}

func (p List) ForEach(fn func(uint64) bool) bool {
	if p.ptr == nil {
		return true
	}
	if p.ptr == singleValue {
		return fn(p.single)
	}
	switch p.single & postingMetaKindMask {
	case postingKindSmall:
		sp := (*smallPosting)(p.ptr)
		for i := 0; i < int(sp.n); i++ {
			if !fn(sp.ids[i]) {
				return false
			}
		}
		return true
	case postingKindMid:
		mp := (*midPosting)(p.ptr)
		for i := 0; i < int(mp.n); i++ {
			if !fn(mp.ids[i]) {
				return false
			}
		}
		return true
	default:
		return (*largePosting)(p.ptr).forEach(fn)
	}
}

func intersectionSides(p, other List) (List, List) {
	// In mixed compact-vs-large cases, compact cardinality is bounded by MidCap,
	// while large cardinality requires a full tree walk.
	if p.largeRef() != nil {
		if other.largeRef() == nil {
			return other, p
		}
		return p, other
	}
	if other.largeRef() != nil {
		return p, other
	}
	if p.Cardinality() > other.Cardinality() {
		return other, p
	}
	return p, other
}

func (p List) Intersects(other List) bool {
	if p.IsEmpty() || other.IsEmpty() {
		return false
	}
	if id, ok := p.TrySingle(); ok {
		return other.Contains(id)
	}
	if id, ok := other.TrySingle(); ok {
		return p.Contains(id)
	}
	if plarge := p.largeRef(); plarge != nil {
		if olarge := other.largeRef(); olarge != nil {
			return plarge.intersects(olarge)
		}
	}
	small, large := intersectionSides(p, other)
	it := small.Iter()
	defer it.Release()
	for it.HasNext() {
		if large.Contains(it.Next()) {
			return true
		}
	}
	return false
}

func (p List) AndCardinality(other List) uint64 {
	if p.IsEmpty() || other.IsEmpty() {
		return 0
	}
	if id, ok := p.TrySingle(); ok {
		if other.Contains(id) {
			return 1
		}
		return 0
	}
	if id, ok := other.TrySingle(); ok {
		if p.Contains(id) {
			return 1
		}
		return 0
	}
	if plarge := p.largeRef(); plarge != nil {
		if olarge := other.largeRef(); olarge != nil {
			return plarge.andCardinality(olarge)
		}
	}
	small, large := intersectionSides(p, other)
	var out uint64
	it := small.Iter()
	defer it.Release()
	for it.HasNext() {
		if large.Contains(it.Next()) {
			out++
		}
	}
	return out
}

func (p List) ForEachIntersecting(other List, fn func(uint64) bool) bool {
	if p.IsEmpty() || other.IsEmpty() {
		return false
	}
	if id, ok := p.TrySingle(); ok {
		if !other.Contains(id) {
			return false
		}
		return fn(id)
	}
	if id, ok := other.TrySingle(); ok {
		if !p.Contains(id) {
			return false
		}
		return fn(id)
	}
	if plarge := p.largeRef(); plarge != nil {
		if olarge := other.largeRef(); olarge != nil {
			return plarge.forEachIntersecting(olarge, fn)
		}
	}
	small, large := intersectionSides(p, other)
	it := small.Iter()
	defer it.Release()
	for it.HasNext() {
		idx := it.Next()
		if !large.Contains(idx) {
			continue
		}
		if fn(idx) {
			return true
		}
	}
	return false
}

func (p List) Iter() Iterator {
	if p.IsEmpty() {
		return emptyIter{}
	}
	if p.ptr == singleValue {
		return getSingletonIter(p.single)
	}
	switch p.single & postingMetaKindMask {
	case postingKindSmall:
		sp := (*smallPosting)(p.ptr)
		return getArrayIter(sp.ids[:sp.n])
	case postingKindMid:
		mp := (*midPosting)(p.ptr)
		return getArrayIter(mp.ids[:mp.n])
	default:
		return (*largePosting)(p.ptr).iterator()
	}
}

func (p List) andIntoLarge(dst *largePosting) {
	if p.ptr == nil {
		dst.clear()
		return
	}
	if p.ptr == singleValue {
		if !dst.contains(p.single) {
			dst.clear()
			return
		}
		dst.clear()
		dst.add(p.single)
		return
	}
	if sp := p.small(); sp != nil {
		var matched [SmallCap]uint64
		n := 0
		for i := 0; i < int(sp.n); i++ {
			if dst.contains(sp.ids[i]) {
				matched[n] = sp.ids[i]
				n++
			}
		}
		dst.clear()
		if n > 0 {
			dst.addMany(matched[:n])
		}
		return
	}
	if mp := p.mid(); mp != nil {
		var matched [MidCap]uint64
		n := 0
		for i := 0; i < int(mp.n); i++ {
			if dst.contains(mp.ids[i]) {
				matched[n] = mp.ids[i]
				n++
			}
		}
		dst.clear()
		if n > 0 {
			dst.addMany(matched[:n])
		}
		return
	}
	dst.and(p.largeRef())
}

func (p List) Clone() List {
	if p.IsEmpty() || p.ptr == singleValue {
		return p
	}
	if sp := p.small(); sp != nil {
		return smallValue(cloneSmallPosting(sp))
	}
	if mp := p.mid(); mp != nil {
		return midValue(cloneMidPosting(mp))
	}
	return largeValue(p.largeRef().cloneSharedInto(getLargePosting()))
}

func (p List) CloneInto(dst List) List {
	if dst.SharesPayload(p) {
		return p.Clone()
	}
	if p.IsEmpty() || p.ptr == singleValue {
		dst.Release()
		return p
	}
	if sp := p.small(); sp != nil {
		if cur := dst.small(); cur != nil && !dst.IsBorrowed() {
			*cur = *sp
			return smallValue(cur)
		}
		dst.Release()
		return smallValue(cloneSmallPosting(sp))
	}
	if mp := p.mid(); mp != nil {
		if cur := dst.mid(); cur != nil && !dst.IsBorrowed() {
			*cur = *mp
			return midValue(cur)
		}
		dst.Release()
		return midValue(cloneMidPosting(mp))
	}
	src := p.largeRef()
	if src == nil {
		dst.Release()
		return List{}
	}
	if cur := dst.largeRef(); cur != nil && !dst.IsBorrowed() {
		src.cloneSharedInto(cur)
		return largeValue(cur)
	}
	dst.Release()
	return largeValue(src.cloneSharedInto(getLargePosting()))
}

func (p List) TryBuildAndAnyBuf(other []List) (List, bool) {
	if p.IsEmpty() {
		return p, true
	}
	if len(other) == 0 {
		p.Release()
		return List{}, true
	}
	if len(other) == 1 {
		return p.BuildAnd(other[0]), true
	}
	if id, ok := p.TrySingle(); ok {
		if postingBufContainsAny(other, id) {
			return p, true
		}
		p.Release()
		return List{}, true
	}
	if p.isSmall() || p.isMid() {
		return compactFilterByAnyMembership(p, other), true
	}
	return p, false
}

func (p List) TryResetOwnedCompactLikeFromSorted(src List, ids []uint64) (List, List, bool) {
	if src.small() != nil {
		if len(ids) > SmallCap {
			return List{}, p, false
		}
		cur := p.small()
		if cur == nil || p.IsBorrowed() {
			p.Release()
			cur = getSmallPosting()
		}
		copy(cur.ids[:len(ids)], ids)
		cur.n = uint8(len(ids))
		next := smallValue(cur)
		switch len(ids) {
		case 0:
			return List{}, next, true
		case 1:
			return singleton(ids[0]), next, true
		default:
			return next, next, true
		}
	}
	if src.mid() != nil {
		if len(ids) > MidCap {
			return List{}, p, false
		}
		cur := p.mid()
		if cur == nil || p.IsBorrowed() {
			p.Release()
			cur = getMidPosting()
		}
		copy(cur.ids[:len(ids)], ids)
		cur.n = uint8(len(ids))
		next := midValue(cur)
		switch len(ids) {
		case 0:
			return List{}, next, true
		case 1:
			return singleton(ids[0]), next, true
		default:
			return next, next, true
		}
	}
	return List{}, p, false
}

func (p List) BuildRemoved(idx uint64) List {
	return buildRemoved(p, idx)
}

func buildRemoved(ids List, idx uint64) List {
	borrowed := ids.IsBorrowed()
	switch {
	case ids.IsEmpty():
		return ids
	case ids.ptr == singleValue:
		if ids.single == idx {
			return List{}
		}
		return ids
	case ids.isSmall():
		sp := ids.small()
		n := int(sp.n)
		pos := -1
		for i := 0; i < n; i++ {
			if sp.ids[i] == idx {
				pos = i
				break
			}
			if sp.ids[i] > idx {
				return ids
			}
		}
		if pos < 0 {
			return ids
		}
		switch n - 1 {
		case 0:
			if !borrowed {
				sp.release()
			}
			return List{}
		case 1:
			keep := sp.ids[1-pos]
			if !borrowed {
				sp.release()
			}
			return singleton(keep)
		default:
			if !borrowed {
				copy(sp.ids[pos:n-1], sp.ids[pos+1:n])
				sp.n--
				return ids
			}
			next := getSmallPosting()
			next.n = uint8(n - 1)
			copy(next.ids[:pos], sp.ids[:pos])
			copy(next.ids[pos:], sp.ids[pos+1:n])
			return smallValue(next)
		}
	case ids.isMid():
		mp := ids.mid()
		n := int(mp.n)
		pos := -1
		for i := 0; i < n; i++ {
			if mp.ids[i] == idx {
				pos = i
				break
			}
			if mp.ids[i] > idx {
				return ids
			}
		}
		if pos < 0 {
			return ids
		}
		switch n - 1 {
		case 0:
			if !borrowed {
				mp.release()
			}
			return List{}
		case 1:
			keep := mp.ids[1-pos]
			if !borrowed {
				mp.release()
			}
			return singleton(keep)
		default:
			if n-1 <= SmallCap {
				next := getSmallPosting()
				next.n = uint8(n - 1)
				copy(next.ids[:pos], mp.ids[:pos])
				copy(next.ids[pos:], mp.ids[pos+1:n])
				if !borrowed {
					mp.release()
				}
				return smallValue(next)
			}
			if !borrowed {
				copy(mp.ids[pos:n-1], mp.ids[pos+1:n])
				mp.n--
				return ids
			}
			next := getMidPosting()
			next.n = uint8(n - 1)
			copy(next.ids[:pos], mp.ids[:pos])
			copy(next.ids[pos:], mp.ids[pos+1:n])
			return midValue(next)
		}
	default:
		lp := ids.largeRef()
		if borrowed {
			lp = lp.cloneSharedInto(getLargePosting())
		}
		lp.remove(idx)
		if lp.isEmpty() {
			lp.release()
			return List{}
		}
		if borrowed {
			return largeValue(lp)
		}
		return ids
	}
}

func (p List) BuildAdded(idx uint64) List {
	return buildAdded(p, idx)
}

func (p List) BuildAddedChecked(idx uint64) (List, bool) {
	return buildAddedChecked(p, idx)
}

func (p List) BuildAddedMany(ids []uint64) List {
	if len(ids) == 0 {
		return p
	}

	if lp := p.largeRef(); lp != nil {
		if highbits64(ids[0]) > lp.highlowcontainer.keys[len(lp.highlowcontainer.keys)-1] {
			prev := ids[0]
			strict := true
			for i := 1; i < len(ids); i++ {
				id := ids[i]
				if id <= prev {
					strict = false
					break
				}
				prev = id
			}
			if strict {
				if p.IsBorrowed() {
					p = p.Clone()
					lp = p.largeRef()
				}
				first := ids[0]
				last := ids[len(ids)-1]
				if last != ^uint64(0) && last-first == uint64(len(ids)-1) {
					lp.addRange(first, last+1)
				} else {
					lp.loadSortedUnique(ids)
				}
				return p
			}
		}
		if p.IsBorrowed() {
			p = p.Clone()
			lp = p.largeRef()
		}
		lp.addMany(ids)
		return p
	}

	if p.IsEmpty() && len(ids) <= MidCap {
		prev := ids[0]
		strict := true
		for i := 1; i < len(ids); i++ {
			id := ids[i]
			if id <= prev {
				strict = false
				break
			}
			prev = id
		}
		if strict {
			if len(ids) == 1 {
				return singleton(ids[0])
			}
			if len(ids) <= SmallCap {
				sp := getSmallPosting()
				sp.n = uint8(len(ids))
				copy(sp.ids[:], ids)
				return smallValue(sp)
			}
			mp := getMidPosting()
			mp.n = uint8(len(ids))
			copy(mp.ids[:], ids)
			return midValue(mp)
		}
	}

	if len(ids) <= MidCap {
		for _, id := range ids {
			p = buildAdded(p, id)
		}
		return p
	}

	current := p
	lp := getLargePosting()
	switch {
	case current.IsEmpty():
		prev := ids[0]
		strict := true
		for i := 1; i < len(ids); i++ {
			id := ids[i]
			if id <= prev {
				strict = false
				break
			}
			prev = id
		}
		if strict {
			first := ids[0]
			last := ids[len(ids)-1]
			if last != ^uint64(0) && last-first == uint64(len(ids)-1) {
				lp.addRange(first, last+1)
			} else {
				lp.loadSortedUnique(ids)
			}
			return largeValue(lp)
		}
	case current.ptr == singleValue:
		lp.add(current.single)
	case current.isSmall():
		sp := current.small()
		for i := 0; i < int(sp.n); i++ {
			lp.add(sp.ids[i])
		}
		if !current.IsBorrowed() {
			sp.release()
		}
	case current.isMid():
		mp := current.mid()
		for i := 0; i < int(mp.n); i++ {
			lp.add(mp.ids[i])
		}
		if !current.IsBorrowed() {
			mp.release()
		}
	default:
		panic("unsupported posting representation")
	}
	lp.addMany(ids)
	return fromLargeOwned(lp)
}

func buildAddedChecked(ids List, idx uint64) (List, bool) {
	switch {
	case ids.IsEmpty():
		return singleton(idx), true
	case ids.ptr == singleValue:
		if ids.single == idx {
			return ids, false
		}
		if idx < ids.single {
			return newSmall(idx, ids.single), true
		}
		return newSmall(ids.single, idx), true
	case ids.isSmall():
		sp := ids.small()
		n := int(sp.n)
		insert := n
		for i := 0; i < n; i++ {
			if sp.ids[i] == idx {
				return ids, false
			}
			if sp.ids[i] > idx {
				insert = i
				break
			}
		}
		if ids.IsBorrowed() {
			if n < SmallCap {
				next := getSmallPosting()
				next.n = uint8(n + 1)
				copy(next.ids[:insert], sp.ids[:insert])
				next.ids[insert] = idx
				copy(next.ids[insert+1:n+1], sp.ids[insert:n])
				return smallValue(next), true
			}
			mp := getMidPosting()
			mp.n = uint8(n + 1)
			copy(mp.ids[:insert], sp.ids[:insert])
			mp.ids[insert] = idx
			copy(mp.ids[insert+1:n+1], sp.ids[insert:n])
			return midValue(mp), true
		}
		if n < SmallCap {
			copy(sp.ids[insert+1:n+1], sp.ids[insert:n])
			sp.ids[insert] = idx
			sp.n++
			return ids, true
		}
		mp := getMidPosting()
		mp.n = uint8(n + 1)
		copy(mp.ids[:insert], sp.ids[:insert])
		mp.ids[insert] = idx
		copy(mp.ids[insert+1:n+1], sp.ids[insert:n])
		sp.release()
		return midValue(mp), true
	case ids.isMid():
		mp := ids.mid()
		n := int(mp.n)
		insert := n
		for i := 0; i < n; i++ {
			if mp.ids[i] == idx {
				return ids, false
			}
			if mp.ids[i] > idx {
				insert = i
				break
			}
		}
		if ids.IsBorrowed() {
			if n < MidCap {
				next := getMidPosting()
				next.n = uint8(n + 1)
				copy(next.ids[:insert], mp.ids[:insert])
				next.ids[insert] = idx
				copy(next.ids[insert+1:n+1], mp.ids[insert:n])
				return midValue(next), true
			}
			lp := getLargePosting()
			for i := 0; i < n; i++ {
				lp.add(mp.ids[i])
			}
			lp.add(idx)
			return largeValue(lp), true
		}
		if n < MidCap {
			copy(mp.ids[insert+1:n+1], mp.ids[insert:n])
			mp.ids[insert] = idx
			mp.n++
			return ids, true
		}
		lp := getLargePosting()
		for i := 0; i < n; i++ {
			lp.add(mp.ids[i])
		}
		lp.add(idx)
		mp.release()
		return largeValue(lp), true
	default:
		lp := ids.largeRef()
		if ids.IsBorrowed() {
			if lp.contains(idx) {
				return ids, false
			}
			lp = lp.cloneSharedInto(getLargePosting())
			lp.checkedAdd(idx)
			return largeValue(lp), true
		}
		return ids, lp.checkedAdd(idx)
	}
}

func buildAdded(ids List, idx uint64) List {
	if ids.IsBorrowed() {
		ids = ids.Clone()
	}
	switch {
	case ids.IsEmpty():
		return singleton(idx)
	case ids.ptr == singleValue:
		if ids.single == idx {
			return ids
		}
		if idx < ids.single {
			return newSmall(idx, ids.single)
		}
		return newSmall(ids.single, idx)
	case ids.isSmall():
		sp := ids.small()
		n := int(sp.n)
		insert := n
		for i := 0; i < n; i++ {
			if sp.ids[i] == idx {
				return ids
			}
			if sp.ids[i] > idx {
				insert = i
				break
			}
		}
		if n < SmallCap {
			copy(sp.ids[insert+1:n+1], sp.ids[insert:n])
			sp.ids[insert] = idx
			sp.n++
			return ids
		}
		mp := getMidPosting()
		mp.n = uint8(n + 1)
		copy(mp.ids[:insert], sp.ids[:insert])
		mp.ids[insert] = idx
		copy(mp.ids[insert+1:], sp.ids[insert:n])
		sp.release()
		return midValue(mp)
	case ids.isMid():
		mp := ids.mid()
		n := int(mp.n)
		insert := n
		for i := 0; i < n; i++ {
			if mp.ids[i] == idx {
				return ids
			}
			if mp.ids[i] > idx {
				insert = i
				break
			}
		}
		if n < MidCap {
			copy(mp.ids[insert+1:n+1], mp.ids[insert:n])
			mp.ids[insert] = idx
			mp.n++
			return ids
		}
		lp := getLargePosting()
		for i := 0; i < n; i++ {
			lp.add(mp.ids[i])
		}
		lp.add(idx)
		mp.release()
		return largeValue(lp)
	default:
		ids.largeRef().add(idx)
		return ids
	}
}

func (p List) BuildAnd(other List) List {
	if p.SharesPayload(other) {
		return p
	}
	if p.IsEmpty() || other.IsEmpty() {
		p.Release()
		return List{}
	}
	if id, ok := p.TrySingle(); ok {
		if !other.Contains(id) {
			p.Release()
			return List{}
		}
		return p
	}
	if lp := p.largeRef(); lp != nil {
		if id, ok := other.TrySingle(); ok {
			if lp.contains(id) {
				p.Release()
				return singleton(id)
			}
			p.Release()
			return List{}
		}
		if sp := other.small(); sp != nil {
			var matched [SmallCap]uint64
			var cursor ContainsCursor
			cursor.Reset(p)
			n := 0
			for i := 0; i < int(sp.n); i++ {
				id := sp.ids[i]
				if !cursor.Contains(id) {
					continue
				}
				matched[n] = id
				n++
			}
			p.Release()
			return compactListFromSorted(matched[:n])
		}
		if mp := other.mid(); mp != nil {
			var matched [MidCap]uint64
			var cursor ContainsCursor
			cursor.Reset(p)
			n := 0
			for i := 0; i < int(mp.n); i++ {
				id := mp.ids[i]
				if !cursor.Contains(id) {
					continue
				}
				matched[n] = id
				n++
			}
			p.Release()
			return compactListFromSorted(matched[:n])
		}
		if p.IsBorrowed() {
			p = p.Clone()
			lp = p.largeRef()
		}
		other.andIntoLarge(lp)
		if lp.isEmpty() {
			lp.release()
			return List{}
		}
		return p
	}
	return compactFilterByMembership(p, other, true)
}

func (p List) BuildOr(other List) List {
	if other.IsEmpty() {
		return p
	}
	if p.IsEmpty() {
		return other.Clone()
	}
	if p.SharesPayload(other) {
		return p
	}
	if other.ptr == singleValue {
		return buildAdded(p, other.single)
	}
	if sp := other.small(); sp != nil {
		if psp := p.small(); psp != nil {
			return buildCompactUnion(p, psp.ids[:psp.n], sp.ids[:sp.n])
		}
		if pmp := p.mid(); pmp != nil {
			return buildCompactUnion(p, pmp.ids[:pmp.n], sp.ids[:sp.n])
		}
		for i := 0; i < int(sp.n); i++ {
			p = buildAdded(p, sp.ids[i])
		}
		return p
	}
	if mp := other.mid(); mp != nil {
		if psp := p.small(); psp != nil {
			return buildCompactUnion(p, psp.ids[:psp.n], mp.ids[:mp.n])
		}
		if pmp := p.mid(); pmp != nil {
			return buildCompactUnion(p, pmp.ids[:pmp.n], mp.ids[:mp.n])
		}
		for i := 0; i < int(mp.n); i++ {
			p = buildAdded(p, mp.ids[i])
		}
		return p
	}
	if p.ptr == singleValue {
		lp := getLargePosting()
		lp.add(p.single)
		lp.or(other.largeRef())
		return largeValue(lp)
	}
	if sp := p.small(); sp != nil {
		lp := getLargePosting()
		for i := 0; i < int(sp.n); i++ {
			lp.add(sp.ids[i])
		}
		lp.or(other.largeRef())
		if !p.IsBorrowed() {
			sp.release()
		}
		return largeValue(lp)
	}
	if mp := p.mid(); mp != nil {
		lp := getLargePosting()
		for i := 0; i < int(mp.n); i++ {
			lp.add(mp.ids[i])
		}
		lp.or(other.largeRef())
		if !p.IsBorrowed() {
			mp.release()
		}
		return largeValue(lp)
	}
	if p.IsBorrowed() {
		p = p.Clone()
	}
	p.largeRef().or(other.largeRef())
	return p
}

func (p List) BuildAndNot(other List) List {
	if p.IsEmpty() || other.IsEmpty() {
		return p
	}
	if p.SharesPayload(other) {
		p.Release()
		return List{}
	}
	if id, ok := p.TrySingle(); ok {
		if other.Contains(id) {
			p.Release()
			return List{}
		}
		return p
	}
	if p.isSmall() || p.isMid() {
		return compactFilterByMembership(p, other, false)
	}
	if sp := other.small(); sp != nil {
		for i := 0; i < int(sp.n) && !p.IsEmpty(); i++ {
			p = buildRemoved(p, sp.ids[i])
		}
		return p
	}
	if mp := other.mid(); mp != nil {
		for i := 0; i < int(mp.n) && !p.IsEmpty(); i++ {
			p = buildRemoved(p, mp.ids[i])
		}
		return p
	}
	if id, ok := other.TrySingle(); ok {
		return buildRemoved(p, id)
	}
	if p.IsBorrowed() {
		p = p.Clone()
	}
	p.largeRef().andNot(other.largeRef())
	if p.largeRef().isEmpty() {
		p.largeRef().release()
		return List{}
	}
	return p
}

func (p List) BuildOptimized() List {
	if p.ptr == nil || p.ptr == singleValue || p.kind() != postingKindLarge {
		return p
	}
	lp := p.largeRef()
	card := lp.cardinality()
	if card <= MidCap {
		if p.IsBorrowed() {
			return fromLargeBorrowedWithCardinality(lp, card)
		}
		return fromLargeOwnedWithCardinality(lp, card)
	}
	if p.IsBorrowed() {
		p = p.Clone()
		lp = p.largeRef()
	}
	lp.runOptimize()
	return p
}

func (p List) TryResetOwnedLargeFromSorted(ids []uint64) (List, bool) {
	lp := p.largeRef()
	if lp == nil || p.IsBorrowed() {
		return List{}, false
	}
	lp.clear()
	if len(ids) == 0 {
		lp.release()
		return List{}, true
	}
	first := ids[0]
	last := ids[len(ids)-1]
	if last != ^uint64(0) && last-first == uint64(len(ids)-1) {
		lp.addRange(first, last+1)
		return p, true
	}
	lp.loadSortedUnique(ids)
	return p, true
}

func (p List) WriteTo(writer *bufio.Writer) error {
	if p.IsEmpty() {
		return writer.WriteByte(encodingEmpty)
	}
	if id, ok := p.TrySingle(); ok {
		return WriteSingleton(writer, id)
	}
	if sp := p.small(); sp != nil {
		return writeCompactPosting(writer, encodingSmall, sp.ids[:sp.n])
	}
	if mp := p.mid(); mp != nil {
		return writeCompactPosting(writer, encodingMid, mp.ids[:mp.n])
	}
	if err := writer.WriteByte(encodingLarge); err != nil {
		return err
	}
	return writeLarge(writer, p.largeRef())
}

func WriteSingleton(writer *bufio.Writer, id uint64) error {
	if err := writer.WriteByte(encodingSingleton); err != nil {
		return err
	}
	return writeUvarint(writer, id)
}

func readCompactPostingValues(reader *bufio.Reader, ids []uint64, kind string) error {
	if len(ids) != 0 {
		if buf, err := reader.Peek(len(ids)); err == nil {
			var prev uint64
			ok := true
			for i := 0; i < len(buf); i++ {
				b := buf[i]
				if b >= 0x80 {
					ok = false
					break
				}
				delta := uint64(b)
				if i == 0 {
					ids[i] = delta
					prev = delta
					continue
				}
				next := prev + delta
				if delta == 0 || next <= prev {
					return fmt.Errorf("invalid %s posting data: ids must be strictly increasing", kind)
				}
				ids[i] = next
				prev = next
			}
			if ok {
				_, err = reader.Discard(len(ids))
				return err
			}
		}
	}

	var prev uint64
	for i := range ids {
		delta, err := readUvarint(reader)
		if err != nil {
			return err
		}
		if i == 0 {
			ids[i] = delta
			prev = delta
			continue
		}

		next := prev + delta
		if delta == 0 || next <= prev {
			return fmt.Errorf("invalid %s posting data: ids must be strictly increasing", kind)
		}

		ids[i] = next
		prev = next
	}
	return nil
}

func ReadFrom(reader *bufio.Reader) (List, error) {
	tag, err := reader.ReadByte()
	if err != nil {
		return List{}, err
	}
	switch tag {
	case encodingEmpty:
		return List{}, nil
	case encodingSingleton:
		id, err := readUvarint(reader)
		if err != nil {
			return List{}, err
		}
		return singleton(id), nil
	case encodingSmall:
		n, err := readUvarint(reader)
		if err != nil {
			return List{}, err
		}
		if n == 0 || n > SmallCap {
			return List{}, fmt.Errorf("invalid small posting len %v", n)
		}
		sp := getSmallPosting()
		sp.n = uint8(n)
		if err := readCompactPostingValues(reader, sp.ids[:n], "small"); err != nil {
			sp.release()
			return List{}, err
		}
		if sp.n == 1 {
			id := sp.ids[0]
			sp.release()
			return singleton(id), nil
		}
		return smallValue(sp), nil
	case encodingMid:
		n, err := readUvarint(reader)
		if err != nil {
			return List{}, err
		}
		if n == 0 || n <= SmallCap || n > MidCap {
			return List{}, fmt.Errorf("invalid mid posting len %v", n)
		}
		mp := getMidPosting()
		mp.n = uint8(n)
		if err := readCompactPostingValues(reader, mp.ids[:n], "mid"); err != nil {
			mp.release()
			return List{}, err
		}
		return midValue(mp), nil
	case encodingLarge:
		lp, err := readLarge(reader)
		if err != nil {
			return List{}, err
		}
		if lp == nil || lp.isEmpty() {
			if lp != nil {
				lp.release()
			}
			return List{}, nil
		}
		return fromLargeOwned(lp), nil
	default:
		return List{}, fmt.Errorf("invalid posting encoding tag %v", tag)
	}
}

func (p List) Release() {
	if p.ptr == nil || p.ptr == singleValue || p.single&postingMetaBorrowed != 0 {
		return
	}
	if p.single&postingMetaBorrowed != 0 {
		return
	}
	switch p.single & postingMetaKindMask {
	case postingKindSmall:
		(*smallPosting)(p.ptr).release()
	case postingKindMid:
		(*midPosting)(p.ptr).release()
	case postingKindLarge:
		(*largePosting)(p.ptr).release()
	}
}

func ReleaseAll(ids []List) {
	for i := range ids {
		ids[i].Release()
		ids[i] = List{}
	}
}

func ReleaseMapU32(m map[uint32]List) {
	for _, ids := range m {
		ids.Release()
	}
}

func ReleaseMapU64(m map[uint64]List) {
	for _, ids := range m {
		ids.Release()
	}
}

func ReleaseMapString(m map[string]List) {
	for _, ids := range m {
		ids.Release()
	}
}

func (p List) BuildMergedOwned(add List) List {
	if add.IsEmpty() {
		return p
	}
	if p.IsEmpty() {
		return add
	}
	if p.SharesPayload(add) {
		return p
	}
	out := p.BuildOr(add)
	if !out.SharesPayload(add) {
		add.Release()
	}
	return out
}

func Skip(reader *bufio.Reader) error {
	tag, err := reader.ReadByte()
	if err != nil {
		return err
	}
	switch tag {
	case encodingEmpty:
		return nil
	case encodingSingleton:
		return skipUvarint(reader)
	case encodingSmall, encodingMid:
		n, err := readUvarint(reader)
		if err != nil {
			return err
		}
		return skipCompactUvarints(reader, n)
	case encodingLarge:
		return skipLarge(reader)
	default:
		return fmt.Errorf("invalid posting encoding tag %v", tag)
	}
}

func skipCompactUvarints(reader *bufio.Reader, n uint64) error {
	if n <= MidCap {
		if buf, err := reader.Peek(int(n)); err == nil {
			oneByte := true
			for i := 0; i < len(buf); i++ {
				if buf[i] >= 0x80 {
					oneByte = false
					break
				}
			}
			if oneByte {
				_, err = reader.Discard(int(n))
				return err
			}
		}
	}

	for i := uint64(0); i < n; i++ {
		if err := skipUvarint(reader); err != nil {
			return err
		}
	}
	return nil
}

type Iterator interface {
	HasNext() bool
	Next() uint64
	Release()
}

type emptyIter struct{}

func (emptyIter) HasNext() bool { return false }

func (emptyIter) Next() uint64 { return 0 }

func (emptyIter) AdvanceIfNeeded(uint64) {}

func (emptyIter) Release() {}

type singletonIter struct {
	v   uint64
	has bool
}

func (it *singletonIter) HasNext() bool { return it.has }

func (it *singletonIter) Next() uint64 {
	if !it.has {
		return 0
	}
	it.has = false
	return it.v
}

func (it *singletonIter) AdvanceIfNeeded(minval uint64) {
	if it.has && it.v < minval {
		it.has = false
	}
}

func (it *singletonIter) Release() {
	singletonIterPool.Put(it)
}

func (sp *smallPosting) release() {
	sp.n = 0
	smallPostingPool.Put(sp)
}

func (sp *smallPosting) Release() { sp.release() }

func (mp *midPosting) release() {
	mp.n = 0
	midPostingPool.Put(mp)
}

func (mp *midPosting) Release() { mp.release() }

func (it *arrayIter) Release() {
	it.ids = nil
	arrayIterPool.Put(it)
}
