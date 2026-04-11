package posting

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/vapstack/rbi/internal/pooled"
)

func writeUvarint(writer *bufio.Writer, v uint64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	_, err := writer.Write(buf[:n])
	return err
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
//   - ptr != nil && single metadata says small: small
//   - ptr != nil && single metadata says mid: mid
//   - ptr != nil && single metadata says large: large posting
type List struct {
	ptr    unsafe.Pointer
	single uint64
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
	if sp == nil {
		return nil
	}
	clone := smallSetPool.Get()
	*clone = *sp
	return clone
}

func cloneMidPosting(mp *midPosting) *midPosting {
	if mp == nil {
		return nil
	}
	clone := midSetPool.Get()
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
	sp := smallSetPool.Get()
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
		sp := smallSetPool.Get()
		sp.n = uint8(len(ids))
		copy(sp.ids[:len(ids)], ids)
		return smallValue(sp)
	}
	mp := midSetPool.Get()
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
		sp := smallSetPool.Get()
		sp.n = uint8(len(ids))
		copy(sp.ids[:len(ids)], ids)
		return smallValue(sp)
	case 9, 10, 11, 12, 13, 14, 15, 16,
		17, 18, 19, 20, 21, 22, 23, 24,
		25, 26, 27, 28, 29, 30, 31, 32:
		mp := midSetPool.Get()
		mp.n = uint8(len(ids))
		copy(mp.ids[:len(ids)], ids)
		return midValue(mp)
	default:
		lp := largePostingPool.Get()
		lp.addMany(ids)
		out := largeValue(lp)
		return out.BuildOptimized()
	}
}

func (p List) isSmall() bool {
	return p.ptr != nil && p.ptr != singleValue && p.kind() == postingKindSmall
}

func (p List) isMid() bool {
	return p.ptr != nil && p.ptr != singleValue && p.kind() == postingKindMid
}

func (p List) small() *smallPosting {
	if !p.isSmall() {
		return nil
	}
	return (*smallPosting)(p.ptr)
}

func (p List) mid() *midPosting {
	if !p.isMid() {
		return nil
	}
	return (*midPosting)(p.ptr)
}

func (p List) smallLen() int {
	sp := p.small()
	if sp == nil {
		return 0
	}
	return int(sp.n)
}

func (it *arrayIter) HasNext() bool {
	return it != nil && it.i < len(it.ids)
}

func (it *arrayIter) Next() uint64 {
	if !it.HasNext() {
		return 0
	}
	v := it.ids[it.i]
	it.i++
	return v
}

func (p List) isSingleton() bool { return p.ptr == singleValue }

func (p List) kind() uint64 {
	return p.single & postingMetaKindMask
}

func (p List) IsBorrowed() bool {
	return p.ptr != nil && !p.isSingleton() && p.single&postingMetaBorrowed != 0
}

type PayloadKey struct {
	Ptr  uintptr
	Kind uint64
}

func (p List) PayloadKey() (PayloadKey, bool) {
	if p.ptr == nil || p.isSingleton() {
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
	if p.ptr == nil || p.isSingleton() {
		return p
	}
	p.single |= postingMetaBorrowed
	return p
}

func (p List) SharesPayload(other List) bool {
	switch {
	case p.IsEmpty() || other.IsEmpty():
		return p.IsEmpty() && other.IsEmpty()
	case p.isSingleton() || other.isSingleton():
		return p.isSingleton() && other.isSingleton() && p.single == other.single
	default:
		return p.ptr == other.ptr && p.kind() == other.kind()
	}
}

func compactFilterByMembership(ids List, other List, keepMatches bool) List {
	if sp := ids.small(); sp != nil {
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
			sp := smallSetPool.Get()
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

func postingBufContainsAny(posts *pooled.SliceBuf[List], id uint64) bool {
	if posts == nil {
		return false
	}
	for i := 0; i < posts.Len(); i++ {
		if posts.Get(i).Contains(id) {
			return true
		}
	}
	return false
}

func compactFilterByAnyMembership(ids List, other *pooled.SliceBuf[List]) List {
	if sp := ids.small(); sp != nil {
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
			sp := smallSetPool.Get()
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

func (p List) largeRef() *largePosting {
	if p.ptr == nil || p.isSingleton() || p.kind() != postingKindLarge {
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
	if card <= SmallCap {
		sp := smallSetPool.Get()
		sp.n = uint8(card)
		it := lp.iterator()
		defer it.Release()
		i := 0
		for it.HasNext() {
			sp.ids[i] = it.Next()
			i++
		}
		lp.release()
		return smallValue(sp)
	}
	if card <= MidCap {
		mp := midSetPool.Get()
		mp.n = uint8(card)
		it := lp.iterator()
		defer it.Release()
		i := 0
		for it.HasNext() {
			mp.ids[i] = it.Next()
			i++
		}
		lp.release()
		return midValue(mp)
	}
	return largeValue(lp)
}

func (p List) IsEmpty() bool {
	return p.ptr == nil
}

func (p List) TrySingle() (uint64, bool) {
	if p.ptr == nil {
		return 0, false
	}
	if p.isSingleton() {
		return p.single, true
	}
	if sp := p.small(); sp != nil {
		if sp.n == 1 {
			return sp.ids[0], true
		}
		return 0, false
	}
	if mp := p.mid(); mp != nil {
		if mp.n == 1 {
			return mp.ids[0], true
		}
		return 0, false
	}
	if lp := p.largeRef(); lp != nil {
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
	if p.isSingleton() {
		return append(dst, p.single), true
	}
	if sp := p.small(); sp != nil {
		return append(dst, sp.ids[:sp.n]...), true
	}
	if mp := p.mid(); mp != nil {
		return append(dst, mp.ids[:mp.n]...), true
	}
	return dst, false
}

func (p List) Cardinality() uint64 {
	if p.ptr == nil {
		return 0
	}
	if p.isSingleton() {
		return 1
	}
	if p.isSmall() {
		return uint64(p.smallLen())
	}
	if mp := p.mid(); mp != nil {
		return uint64(mp.n)
	}
	return p.largeRef().cardinality()
}

func (p List) Contains(id uint64) bool {
	if p.ptr == nil {
		return false
	}
	if p.isSingleton() {
		return p.single == id
	}
	if sp := p.small(); sp != nil {
		for i := 0; i < int(sp.n); i++ {
			if sp.ids[i] == id {
				return true
			}
			if sp.ids[i] > id {
				return false
			}
		}
		return false
	}
	if mp := p.mid(); mp != nil {
		for i := 0; i < int(mp.n); i++ {
			if mp.ids[i] == id {
				return true
			}
			if mp.ids[i] > id {
				return false
			}
		}
		return false
	}
	return p.largeRef().contains(id)
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
	if p.isSingleton() {
		return p.single, true
	}
	if sp := p.small(); sp != nil {
		return sp.ids[0], true
	}
	if mp := p.mid(); mp != nil {
		return mp.ids[0], true
	}
	return p.largeRef().minimum(), true
}

func (p List) Maximum() (uint64, bool) {
	if p.ptr == nil {
		return 0, false
	}
	if p.isSingleton() {
		return p.single, true
	}
	if sp := p.small(); sp != nil {
		return sp.ids[sp.n-1], true
	}
	if mp := p.mid(); mp != nil {
		return mp.ids[mp.n-1], true
	}
	return p.largeRef().maximum(), true
}

func (p List) SizeInBytes() uint64 {
	if p.ptr == nil {
		return 0
	}
	if p.isSingleton() {
		return 8
	}
	if sp := p.small(); sp != nil {
		return uint64(sp.n) * 8
	}
	if mp := p.mid(); mp != nil {
		return uint64(mp.n) * 8
	}
	return p.largeRef().sizeInBytes()
}

func (p List) ToArray() []uint64 {
	if p.IsEmpty() {
		return nil
	}
	if p.isSingleton() {
		return []uint64{p.single}
	}
	if sp := p.small(); sp != nil {
		out := make([]uint64, int(sp.n))
		for i := 0; i < int(sp.n); i++ {
			out[i] = sp.ids[i]
		}
		return out
	}
	if mp := p.mid(); mp != nil {
		out := make([]uint64, int(mp.n))
		for i := 0; i < int(mp.n); i++ {
			out[i] = mp.ids[i]
		}
		return out
	}
	return p.largeRef().toArray()
}

func (p List) ForEach(fn func(uint64) bool) bool {
	if p.ptr == nil {
		return true
	}
	if p.isSingleton() {
		return fn(p.single)
	}
	if sp := p.small(); sp != nil {
		for i := 0; i < int(sp.n); i++ {
			if !fn(sp.ids[i]) {
				return false
			}
		}
		return true
	}
	if mp := p.mid(); mp != nil {
		for i := 0; i < int(mp.n); i++ {
			if !fn(mp.ids[i]) {
				return false
			}
		}
		return true
	}
	it := p.largeRef().iterator()
	defer it.Release()
	for it.HasNext() {
		if !fn(it.Next()) {
			return false
		}
	}
	return true
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
		return emptyIterator()
	}
	if p.isSingleton() {
		return getSingletonIter(p.single)
	}
	if sp := p.small(); sp != nil {
		it := compactPostingIterPool.Get()
		it.ids = sp.ids[:sp.n]
		it.i = 0
		return it
	}
	if mp := p.mid(); mp != nil {
		it := compactPostingIterPool.Get()
		it.ids = mp.ids[:mp.n]
		it.i = 0
		return it
	}
	return p.largeRef().iterator()
}

func (p List) andIntoLarge(dst *largePosting) {
	if dst == nil {
		return
	}
	if p.ptr == nil {
		dst.clear()
		return
	}
	if p.isSingleton() {
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
	if p.IsEmpty() || p.isSingleton() {
		return p
	}
	if sp := p.small(); sp != nil {
		return smallValue(cloneSmallPosting(sp))
	}
	if mp := p.mid(); mp != nil {
		return midValue(cloneMidPosting(mp))
	}
	return largeValue(p.largeRef().cloneSharedInto(largePostingPool.Get()))
}

func (p List) CloneInto(dst List) List {
	if dst.SharesPayload(p) {
		return p.Clone()
	}
	if p.IsEmpty() || p.isSingleton() {
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
	return largeValue(src.cloneSharedInto(largePostingPool.Get()))
}

func (p List) TryBuildAndAnyBuf(other *pooled.SliceBuf[List]) (List, bool) {
	if p.IsEmpty() {
		return p, true
	}
	if other == nil || other.Len() == 0 {
		p.Release()
		return List{}, true
	}
	if other.Len() == 1 {
		return p.BuildAnd(other.Get(0)), true
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
	return List{}, false
}

func (p List) TryResetOwnedCompactLikeFromSorted(src List, ids []uint64) (List, List, bool) {
	if src.small() != nil {
		if len(ids) > SmallCap {
			return List{}, p, false
		}
		cur := p.small()
		if cur == nil || p.IsBorrowed() {
			p.Release()
			cur = smallSetPool.Get()
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
			cur = midSetPool.Get()
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
	case ids.isSingleton():
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
			next := smallSetPool.Get()
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
				next := smallSetPool.Get()
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
			next := midSetPool.Get()
			next.n = uint8(n - 1)
			copy(next.ids[:pos], mp.ids[:pos])
			copy(next.ids[pos:], mp.ids[pos+1:n])
			return midValue(next)
		}
	default:
		lp := ids.largeRef()
		if borrowed {
			lp = lp.cloneSharedInto(largePostingPool.Get())
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
		if p.IsBorrowed() {
			p = p.Clone()
			lp = p.largeRef()
		}
		lp.addMany(ids)
		return p
	}

	if len(ids) <= MidCap {
		for _, id := range ids {
			p = buildAdded(p, id)
		}
		return p
	}

	current := p
	lp := largePostingPool.Get()
	switch {
	case current.IsEmpty():
	case current.isSingleton():
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
	case ids.isSingleton():
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
				next := smallSetPool.Get()
				next.n = uint8(n + 1)
				copy(next.ids[:insert], sp.ids[:insert])
				next.ids[insert] = idx
				copy(next.ids[insert+1:n+1], sp.ids[insert:n])
				return smallValue(next), true
			}
			mp := midSetPool.Get()
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
		mp := midSetPool.Get()
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
				next := midSetPool.Get()
				next.n = uint8(n + 1)
				copy(next.ids[:insert], mp.ids[:insert])
				next.ids[insert] = idx
				copy(next.ids[insert+1:n+1], mp.ids[insert:n])
				return midValue(next), true
			}
			lp := largePostingPool.Get()
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
		lp := largePostingPool.Get()
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
			lp = lp.cloneSharedInto(largePostingPool.Get())
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
	case ids.isSingleton():
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
		mp := midSetPool.Get()
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
		lp := largePostingPool.Get()
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
			n := 0
			for i := 0; i < int(sp.n); i++ {
				id := sp.ids[i]
				if !lp.contains(id) {
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
			n := 0
			for i := 0; i < int(mp.n); i++ {
				id := mp.ids[i]
				if !lp.contains(id) {
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
	if other.isSingleton() {
		return buildAdded(p, other.single)
	}
	if sp := other.small(); sp != nil {
		for i := 0; i < int(sp.n); i++ {
			p = buildAdded(p, sp.ids[i])
		}
		return p
	}
	if mp := other.mid(); mp != nil {
		for i := 0; i < int(mp.n); i++ {
			p = buildAdded(p, mp.ids[i])
		}
		return p
	}
	if p.isSingleton() {
		lp := largePostingPool.Get()
		lp.add(p.single)
		lp.or(other.largeRef())
		return largeValue(lp)
	}
	if sp := p.small(); sp != nil {
		lp := largePostingPool.Get()
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
		lp := largePostingPool.Get()
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
	if p.ptr == nil || p.isSingleton() || p.isSmall() || p.isMid() {
		return p
	}
	if p.IsBorrowed() {
		p = p.Clone()
	}
	lp := p.largeRef()
	card := lp.cardinality()
	if card <= MidCap {
		return fromLargeOwnedWithCardinality(lp, card)
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
	lp.addManySorted(ids)
	return p, true
}

func (p List) WriteTo(writer *bufio.Writer) error {
	if p.IsEmpty() {
		return writer.WriteByte(encodingEmpty)
	}
	if id, ok := p.TrySingle(); ok {
		if err := writer.WriteByte(encodingSingleton); err != nil {
			return err
		}
		return writeUvarint(writer, id)
	}
	if sp := p.small(); sp != nil {
		if err := writer.WriteByte(encodingSmall); err != nil {
			return err
		}
		if err := writeUvarint(writer, uint64(sp.n)); err != nil {
			return err
		}
		var prev uint64
		for i := 0; i < int(sp.n); i++ {
			v := sp.ids[i]
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
	if mp := p.mid(); mp != nil {
		if err := writer.WriteByte(encodingMid); err != nil {
			return err
		}
		if err := writeUvarint(writer, uint64(mp.n)); err != nil {
			return err
		}
		var prev uint64
		for i := 0; i < int(mp.n); i++ {
			v := mp.ids[i]
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
	if err := writer.WriteByte(encodingLarge); err != nil {
		return err
	}
	return writeLarge(writer, p.largeRef())
}

func readCompactPostingValues(reader *bufio.Reader, ids []uint64, kind string) error {
	var prev uint64
	for i := range ids {
		delta, err := binary.ReadUvarint(reader)
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
		id, err := binary.ReadUvarint(reader)
		if err != nil {
			return List{}, err
		}
		return singleton(id), nil
	case encodingSmall:
		n, err := binary.ReadUvarint(reader)
		if err != nil {
			return List{}, err
		}
		if n == 0 || n > SmallCap {
			return List{}, fmt.Errorf("invalid small posting len %v", n)
		}
		sp := smallSetPool.Get()
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
		n, err := binary.ReadUvarint(reader)
		if err != nil {
			return List{}, err
		}
		if n == 0 || n <= SmallCap || n > MidCap {
			return List{}, fmt.Errorf("invalid mid posting len %v", n)
		}
		mp := midSetPool.Get()
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
	if p.IsBorrowed() {
		return
	}
	p.ReleasePayload()
}

func (p List) ReleasePayload() {
	if p.IsBorrowed() {
		return
	}
	if sp := p.small(); sp != nil {
		sp.release()
		return
	}
	if mp := p.mid(); mp != nil {
		mp.release()
		return
	}
	if lp := p.largeRef(); lp != nil {
		lp.release()
	}
}

func ReleaseSliceOwned(ids []List) {
	for i := range ids {
		ids[i].Release()
		ids[i] = List{}
	}
}

func ClearMapOwned[K comparable](m map[K]List) {
	if m == nil {
		return
	}
	for _, ids := range m {
		ids.Release()
	}
	clear(m)
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
		_, err = binary.ReadUvarint(reader)
		return err
	case encodingSmall, encodingMid:
		n, err := binary.ReadUvarint(reader)
		if err != nil {
			return err
		}
		for i := uint64(0); i < n; i++ {
			if _, err = binary.ReadUvarint(reader); err != nil {
				return err
			}
		}
		return nil
	case encodingLarge:
		return skipLarge(reader)
	default:
		return fmt.Errorf("invalid posting encoding tag %v", tag)
	}
}

type Iterator interface {
	HasNext() bool
	Next() uint64
	Release()
}

type emptyIter struct{}

func emptyIterator() Iterator { return emptyIter{} }

func (emptyIter) HasNext() bool { return false }

func (emptyIter) Next() uint64 { return 0 }

func (emptyIter) Release() {}

type singletonIter struct {
	v   uint64
	has bool
}

func getSingletonIter(v uint64) Iterator {
	it := singletonIterPool.Get()
	it.v = v
	it.has = true
	return it
}

func (it *singletonIter) HasNext() bool { return it.has }

func (it *singletonIter) Next() uint64 {
	if !it.has {
		return 0
	}
	it.has = false
	return it.v
}

func (it *singletonIter) Release() {
	singletonIterPool.Put(it)
}

var (
	smallSetPool = pooled.Pointers[smallPosting]{
		Cleanup: func(sp *smallPosting) {
			sp.n = 0
		},
	}
	midSetPool = pooled.Pointers[midPosting]{
		Cleanup: func(mp *midPosting) {
			mp.n = 0
		},
	}
	compactPostingIterPool = pooled.Pointers[arrayIter]{
		Cleanup: func(it *arrayIter) {
			it.ids = nil
			it.i = 0
		},
	}
	singletonIterPool = pooled.Pointers[singletonIter]{
		Cleanup: func(it *singletonIter) {
			it.v = 0
			it.has = false
		},
	}
)

func (sp *smallPosting) release() { smallSetPool.Put(sp) }

func (sp *smallPosting) Release() { sp.release() }

func (mp *midPosting) release() { midSetPool.Put(mp) }

func (mp *midPosting) Release() { mp.release() }

func (it *arrayIter) Release() {
	compactPostingIterPool.Put(it)
}
