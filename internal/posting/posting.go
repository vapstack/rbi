package posting

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"sync"
	"unsafe"
)

func writeUvarint(writer *bufio.Writer, v uint64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	_, err := writer.Write(buf[:n])
	return err
}

// List keeps posting ids in an adaptive representation:
// singleton id for cardinality=1, inline small posting for tiny sets,
// large posting for larger sets.
//
// Encoding:
//   - ptr == nil: empty
//   - ptr == singleSentinelPtr: single value (in single)
//   - ptr != nil && single metadata says small: small
//   - ptr != nil && single metadata says mid: mid
//   - ptr != nil && single metadata says large: large posting
type List struct {
	ptr    unsafe.Pointer
	single uint64
}

var singleSentinel byte

var singleSentinelPtr = unsafe.Pointer(&singleSentinel)

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
	clone := acquireSmallPosting()
	*clone = *sp
	return clone
}

func cloneMidPosting(mp *midPosting) *midPosting {
	if mp == nil {
		return nil
	}
	clone := acquireMidPosting()
	*clone = *mp
	return clone
}

func singleton(id uint64) List {
	return List{
		ptr:    singleSentinelPtr,
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
	sp := acquireSmallPosting()
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
		sp := acquireSmallPosting()
		sp.n = uint8(len(ids))
		copy(sp.ids[:len(ids)], ids)
		return smallValue(sp)
	}
	mp := acquireMidPosting()
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
		sp := acquireSmallPosting()
		sp.n = uint8(len(ids))
		copy(sp.ids[:len(ids)], ids)
		return smallValue(sp)
	case 9, 10, 11, 12, 13, 14, 15, 16,
		17, 18, 19, 20, 21, 22, 23, 24,
		25, 26, 27, 28, 29, 30, 31, 32:
		mp := acquireMidPosting()
		mp.n = uint8(len(ids))
		copy(mp.ids[:len(ids)], ids)
		return midValue(mp)
	default:
		lp := getLargePosting()
		lp.addMany(ids)
		out := largeValue(lp)
		out.Optimize()
		return out
	}
}

func (p List) isSmall() bool {
	return p.ptr != nil && p.ptr != singleSentinelPtr && p.kind() == postingKindSmall
}

func (p List) isMid() bool {
	return p.ptr != nil && p.ptr != singleSentinelPtr && p.kind() == postingKindMid
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

func (p List) isSingleton() bool { return p.ptr == singleSentinelPtr }

func (p List) kind() uint64 {
	return p.single & postingMetaKindMask
}

func (p List) IsBorrowed() bool {
	return p.ptr != nil && !p.isSingleton() && p.single&postingMetaBorrowed != 0
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

func (p *List) ensureOwned() {
	if p == nil || !p.IsBorrowed() {
		return
	}
	*p = p.Clone()
}

func (p *List) compactFilterByMembership(other List, keepMatches bool) {
	if p == nil {
		return
	}
	if sp := p.small(); sp != nil {
		if p.IsBorrowed() {
			var kept [SmallCap]uint64
			n := 0
			for i := 0; i < int(sp.n); i++ {
				id := sp.ids[i]
				if other.Contains(id) == keepMatches {
					kept[n] = id
					n++
				}
			}
			*p = compactListFromSorted(kept[:n])
			return
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
			releaseSmallPosting(sp)
			*p = List{}
		case 1:
			keep := sp.ids[0]
			releaseSmallPosting(sp)
			*p = singleton(keep)
		default:
			sp.n = uint8(n)
		}
		return
	}
	if mp := p.mid(); mp != nil {
		if p.IsBorrowed() {
			var kept [MidCap]uint64
			n := 0
			for i := 0; i < int(mp.n); i++ {
				id := mp.ids[i]
				if other.Contains(id) == keepMatches {
					kept[n] = id
					n++
				}
			}
			*p = compactListFromSorted(kept[:n])
			return
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
			releaseMidPosting(mp)
			*p = List{}
		case n == 1:
			keep := mp.ids[0]
			releaseMidPosting(mp)
			*p = singleton(keep)
		case n <= SmallCap:
			sp := acquireSmallPosting()
			sp.n = uint8(n)
			copy(sp.ids[:n], mp.ids[:n])
			releaseMidPosting(mp)
			*p = smallValue(sp)
		default:
			mp.n = uint8(n)
		}
	}
}

func (p List) largeRef() *largePosting {
	if p.ptr == nil || p.isSingleton() || p.kind() != postingKindLarge {
		return nil
	}
	return (*largePosting)(p.ptr)
}

func fromLargeOwned(lp *largePosting) List {
	if lp == nil || lp.isEmpty() {
		return List{}
	}
	return fromLargeOwnedWithCardinality(lp, lp.cardinality())
}

func fromLargeOwnedWithCardinality(lp *largePosting, card uint64) List {
	if card == 1 {
		id := lp.minimum()
		releaseLargePosting(lp)
		return singleton(id)
	}
	if card <= SmallCap {
		sp := acquireSmallPosting()
		sp.n = uint8(card)
		it := lp.iterator()
		defer releaseLargeIterator(it)
		i := 0
		for it.HasNext() {
			sp.ids[i] = it.Next()
			i++
		}
		releaseLargePosting(lp)
		return smallValue(sp)
	}
	if card <= MidCap {
		mp := acquireMidPosting()
		mp.n = uint8(card)
		it := lp.iterator()
		defer releaseLargeIterator(it)
		i := 0
		for it.HasNext() {
			mp.ids[i] = it.Next()
			i++
		}
		releaseLargePosting(lp)
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

func (p *List) Clear() {
	if p == nil || p.IsEmpty() {
		return
	}
	p.Release()
	*p = List{}
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

func (p *List) CheckedAdd(id uint64) bool {
	if p == nil {
		return false
	}
	next, added := buildAddedChecked(*p, id)
	if added {
		*p = next
	}
	return added
}

func (p *List) AddMany(ids []uint64) {
	if p == nil || len(ids) == 0 {
		return
	}

	if lp := p.largeRef(); lp != nil {
		if p.IsBorrowed() {
			*p = p.Clone()
			lp = p.largeRef()
		}
		lp.addMany(ids)
		return
	}

	if len(ids) <= MidCap {
		for _, id := range ids {
			p.Add(id)
		}
		return
	}

	current := *p
	lp := getLargePosting()
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
			releaseSmallPosting(sp)
		}
	case current.isMid():
		mp := current.mid()
		for i := 0; i < int(mp.n); i++ {
			lp.add(mp.ids[i])
		}
		if !current.IsBorrowed() {
			releaseMidPosting(mp)
		}
	default:
		panic("unsupported posting representation")
	}
	lp.addMany(ids)
	*p = fromLargeOwned(lp)
}

func isNonDecreasingU64(dat []uint64) bool {
	for i := 1; i < len(dat); i++ {
		if dat[i] < dat[i-1] {
			return false
		}
	}
	return true
}

func (p *List) Remove(id uint64) {
	if p == nil || p.IsEmpty() {
		return
	}
	*p = buildRemoved(*p, id)
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
	defer releaseLargeIterator(it)
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
	hit := false
	small.ForEach(func(idx uint64) bool {
		if !large.Contains(idx) {
			return true
		}
		hit = true
		return false
	})
	return hit
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
	small.ForEach(func(idx uint64) bool {
		if large.Contains(idx) {
			out++
		}
		return true
	})
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
	stop := false
	small.ForEach(func(idx uint64) bool {
		if !large.Contains(idx) {
			return true
		}
		if fn(idx) {
			stop = true
			return false
		}
		return true
	})
	return stop
}

func (p List) OrInto(dst *List) {
	dst.OrInPlace(p)
}

func (p List) AndNotFrom(dst *List) {
	if dst == nil || dst.IsEmpty() || p.IsEmpty() {
		return
	}
	dst.AndNotInPlace(p)
}

func (p List) Iter() Iterator {
	if p.IsEmpty() {
		return emptyIterator()
	}
	if p.isSingleton() {
		return newSingletonIter(p.single)
	}
	if sp := p.small(); sp != nil {
		it := getArrayIter()
		it.ids = sp.ids[:sp.n]
		it.i = 0
		return it
	}
	if mp := p.mid(); mp != nil {
		it := getArrayIter()
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

func (p *List) AndInPlace(other List) {
	if p == nil {
		return
	}
	if p.SharesPayload(other) {
		return
	}
	if p.IsEmpty() || other.IsEmpty() {
		p.Release()
		*p = List{}
		return
	}
	if id, ok := p.TrySingle(); ok {
		if !other.Contains(id) {
			*p = List{}
		}
		return
	}
	if lp := p.largeRef(); lp != nil {
		if p.IsBorrowed() {
			*p = p.Clone()
			lp = p.largeRef()
		}
		other.andIntoLarge(lp)
		if lp.isEmpty() {
			releaseLargePosting(lp)
			*p = List{}
			return
		}
		return
	}
	p.compactFilterByMembership(other, true)
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
	return largeValue(cloneLargeShared(p.largeRef()))
}

func (p *List) Add(id uint64) {
	if p == nil {
		return
	}
	*p = buildAdded(*p, id)
}

func (p *List) OrInPlace(other List) {
	if p == nil || other.IsEmpty() {
		return
	}
	if p.IsEmpty() {
		*p = other.Clone()
		return
	}
	if p.SharesPayload(other) {
		return
	}
	if other.isSingleton() {
		p.Add(other.single)
		return
	}
	if sp := other.small(); sp != nil {
		for i := 0; i < int(sp.n); i++ {
			p.Add(sp.ids[i])
		}
		return
	}
	if mp := other.mid(); mp != nil {
		for i := 0; i < int(mp.n); i++ {
			p.Add(mp.ids[i])
		}
		return
	}
	if p.isSingleton() {
		lp := getLargePosting()
		lp.add(p.single)
		lp.or(other.largeRef())
		*p = largeValue(lp)
		return
	}
	if sp := p.small(); sp != nil {
		lp := getLargePosting()
		for i := 0; i < int(sp.n); i++ {
			lp.add(sp.ids[i])
		}
		lp.or(other.largeRef())
		if !p.IsBorrowed() {
			releaseSmallPosting(sp)
		}
		*p = largeValue(lp)
		return
	}
	if mp := p.mid(); mp != nil {
		lp := getLargePosting()
		for i := 0; i < int(mp.n); i++ {
			lp.add(mp.ids[i])
		}
		lp.or(other.largeRef())
		if !p.IsBorrowed() {
			releaseMidPosting(mp)
		}
		*p = largeValue(lp)
		return
	}
	if p.IsBorrowed() {
		*p = p.Clone()
	}
	p.largeRef().or(other.largeRef())
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
				releaseSmallPosting(sp)
			}
			return List{}
		case 1:
			keep := sp.ids[1-pos]
			if !borrowed {
				releaseSmallPosting(sp)
			}
			return singleton(keep)
		default:
			if !borrowed {
				copy(sp.ids[pos:n-1], sp.ids[pos+1:n])
				sp.n--
				return ids
			}
			next := acquireSmallPosting()
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
				releaseMidPosting(mp)
			}
			return List{}
		case 1:
			keep := mp.ids[1-pos]
			if !borrowed {
				releaseMidPosting(mp)
			}
			return singleton(keep)
		default:
			if n-1 <= SmallCap {
				next := acquireSmallPosting()
				next.n = uint8(n - 1)
				copy(next.ids[:pos], mp.ids[:pos])
				copy(next.ids[pos:], mp.ids[pos+1:n])
				if !borrowed {
					releaseMidPosting(mp)
				}
				return smallValue(next)
			}
			if !borrowed {
				copy(mp.ids[pos:n-1], mp.ids[pos+1:n])
				mp.n--
				return ids
			}
			next := acquireMidPosting()
			next.n = uint8(n - 1)
			copy(next.ids[:pos], mp.ids[:pos])
			copy(next.ids[pos:], mp.ids[pos+1:n])
			return midValue(next)
		}
	default:
		lp := ids.largeRef()
		if borrowed {
			lp = cloneLargeShared(lp)
		}
		lp.remove(idx)
		if lp.isEmpty() {
			releaseLargePosting(lp)
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
				next := acquireSmallPosting()
				next.n = uint8(n + 1)
				copy(next.ids[:insert], sp.ids[:insert])
				next.ids[insert] = idx
				copy(next.ids[insert+1:n+1], sp.ids[insert:n])
				return smallValue(next), true
			}
			mp := acquireMidPosting()
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
		mp := acquireMidPosting()
		mp.n = uint8(n + 1)
		copy(mp.ids[:insert], sp.ids[:insert])
		mp.ids[insert] = idx
		copy(mp.ids[insert+1:n+1], sp.ids[insert:n])
		releaseSmallPosting(sp)
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
				next := acquireMidPosting()
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
		releaseMidPosting(mp)
		return largeValue(lp), true
	default:
		lp := ids.largeRef()
		if ids.IsBorrowed() {
			if lp.contains(idx) {
				return ids, false
			}
			lp = cloneLargeShared(lp)
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
		mp := acquireMidPosting()
		mp.n = uint8(n + 1)
		copy(mp.ids[:insert], sp.ids[:insert])
		mp.ids[insert] = idx
		copy(mp.ids[insert+1:], sp.ids[insert:n])
		releaseSmallPosting(sp)
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
		releaseMidPosting(mp)
		return largeValue(lp)
	default:
		ids.largeRef().add(idx)
		return ids
	}
}

func (p *List) AndNotInPlace(other List) {
	if p == nil || p.IsEmpty() || other.IsEmpty() {
		return
	}
	if p.SharesPayload(other) {
		p.Release()
		*p = List{}
		return
	}
	if id, ok := p.TrySingle(); ok {
		if other.Contains(id) {
			*p = List{}
		}
		return
	}
	if p.isSmall() || p.isMid() {
		p.compactFilterByMembership(other, false)
		return
	}
	if sp := other.small(); sp != nil {
		for i := 0; i < int(sp.n) && !p.IsEmpty(); i++ {
			*p = buildRemoved(*p, sp.ids[i])
		}
		return
	}
	if mp := other.mid(); mp != nil {
		for i := 0; i < int(mp.n) && !p.IsEmpty(); i++ {
			*p = buildRemoved(*p, mp.ids[i])
		}
		return
	}
	if p.isSmall() {
		out := *p
		other.ForEach(func(idx uint64) bool {
			out = buildRemoved(out, idx)
			return !out.IsEmpty()
		})
		*p = out
		return
	}
	if p.isMid() {
		out := *p
		other.ForEach(func(idx uint64) bool {
			out = buildRemoved(out, idx)
			return !out.IsEmpty()
		})
		*p = out
		return
	}
	if id, ok := other.TrySingle(); ok {
		*p = buildRemoved(*p, id)
		return
	}
	if p.IsBorrowed() {
		*p = p.Clone()
	}
	p.largeRef().andNot(other.largeRef())
	if p.largeRef().isEmpty() {
		releaseLargePosting(p.largeRef())
		*p = List{}
		return
	}
}

func (p *List) Optimize() {
	if p == nil || p.ptr == nil || p.isSingleton() || p.isSmall() || p.isMid() {
		return
	}
	if p.IsBorrowed() {
		*p = p.Clone()
	}
	lp := p.largeRef()
	card := lp.cardinality()
	if card <= MidCap {
		*p = fromLargeOwnedWithCardinality(lp, card)
		return
	}
	lp.runOptimize()
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

func readFrom(reader *bufio.Reader) (List, error) {
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
		sp := acquireSmallPosting()
		sp.n = uint8(n)
		if err := readCompactPostingValues(reader, sp.ids[:n], "small"); err != nil {
			releaseSmallPosting(sp)
			return List{}, err
		}
		if sp.n == 1 {
			id := sp.ids[0]
			releaseSmallPosting(sp)
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
		mp := acquireMidPosting()
		mp.n = uint8(n)
		if err := readCompactPostingValues(reader, mp.ids[:n], "mid"); err != nil {
			releaseMidPosting(mp)
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
				releaseLargePosting(lp)
			}
			return List{}, nil
		}
		return fromLargeOwned(lp), nil
	default:
		return List{}, fmt.Errorf("invalid posting encoding tag %v", tag)
	}
}

func (p *List) ReadFrom(reader *bufio.Reader) error {
	if p == nil {
		return fmt.Errorf("nil List receiver")
	}
	next, err := readFrom(reader)
	if err != nil {
		return err
	}
	prev := *p
	*p = next
	releaseOwned(prev)
	return nil
}

func (p List) Release() {
	releaseOwned(p)
}

func releaseOwned(ids List) {
	if ids.IsBorrowed() {
		return
	}
	if sp := ids.small(); sp != nil {
		releaseSmallPosting(sp)
		return
	}
	if mp := ids.mid(); mp != nil {
		releaseMidPosting(mp)
		return
	}
	if lp := ids.largeRef(); lp != nil {
		releaseLargePosting(lp)
	}
}

func ReleaseSliceOwned(ids []List) {
	for i := range ids {
		releaseOwned(ids[i])
		ids[i] = List{}
	}
}

func ClearMapOwned[K comparable](m map[K]List) {
	if m == nil {
		return
	}
	for _, ids := range m {
		releaseOwned(ids)
	}
	clear(m)
}

func (p *List) MergeOwned(add List) {
	mergeOwned(p, add)
}

func mergeOwned(dst *List, add List) {
	if dst == nil || add.IsEmpty() {
		return
	}
	if dst.IsEmpty() {
		*dst = add
		return
	}
	dst.OrInPlace(add)
	releaseOwned(add)
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
		_, err := binary.ReadUvarint(reader)
		return err
	case encodingSmall, encodingMid:
		n, err := binary.ReadUvarint(reader)
		if err != nil {
			return err
		}
		for i := uint64(0); i < n; i++ {
			if _, err := binary.ReadUvarint(reader); err != nil {
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

func newSingletonIter(v uint64) Iterator {
	return &singletonIter{v: v, has: true}
}

func (it *singletonIter) HasNext() bool { return it.has }

func (it *singletonIter) Next() uint64 {
	if !it.has {
		return 0
	}
	it.has = false
	return it.v
}

func (*singletonIter) Release() {}

var (
	smallSetPool           sync.Pool
	midSetPool             sync.Pool
	compactPostingIterPool sync.Pool
)

func acquireSmallPosting() *smallPosting {
	if v := smallSetPool.Get(); v != nil {
		return v.(*smallPosting)
	}
	return new(smallPosting)
}

func releaseSmallPosting(sp *smallPosting) {
	if sp == nil {
		return
	}
	sp.n = 0
	smallSetPool.Put(sp)
}

func acquireMidPosting() *midPosting {
	if v := midSetPool.Get(); v != nil {
		return v.(*midPosting)
	}
	return new(midPosting)
}

func releaseMidPosting(mp *midPosting) {
	if mp == nil {
		return
	}
	mp.n = 0
	midSetPool.Put(mp)
}

func getArrayIter() *arrayIter {
	if v := compactPostingIterPool.Get(); v != nil {
		return v.(*arrayIter)
	}
	return new(arrayIter)
}

func (it *arrayIter) release() {
	if it == nil {
		return
	}
	it.ids = nil
	it.i = 0
	compactPostingIterPool.Put(it)
}

func (it *arrayIter) Release() {
	it.release()
}
