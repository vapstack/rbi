package rbi

import "github.com/vapstack/rbi/internal/roaring64"

// postingList keeps posting ids in an adaptive representation:
// singleton id for cardinality=1, roaring bitmap for larger sets.
//
// Encoding:
//   - bm == nil: empty
//   - bm == postingSingleFlag: single value (in single)
//   - bm != nil: roaring
type postingList struct {
	bm     *roaring64.Bitmap
	single uint64
}

var postingSingleFlag = roaring64.New()

func (p postingList) isSingleton() bool { return p.bm == postingSingleFlag }

func (p postingList) bitmap() *roaring64.Bitmap {
	if p.bm == nil || p.bm == postingSingleFlag {
		return nil
	}
	return p.bm
}

func postingFromBitmapOwned(bm *roaring64.Bitmap) postingList {
	if bm == nil || bm.IsEmpty() {
		return postingList{}
	}
	if bm.GetCardinality() == 1 {
		id := bm.Minimum()
		releaseRoaringBuf(bm)
		return postingList{bm: postingSingleFlag, single: id}
	}
	return postingList{bm: bm}
}

func postingFromBitmapViewAdaptive(bm *roaring64.Bitmap) postingList {
	if bm == nil || bm.IsEmpty() {
		return postingList{}
	}
	if bm.GetCardinality() == 1 {
		return postingList{bm: postingSingleFlag, single: bm.Minimum()}
	}
	return postingList{bm: bm}
}

func (p postingList) IsEmpty() bool {
	return p.bm == nil
}

func (p postingList) Cardinality() uint64 {
	if p.bm == nil {
		return 0
	}
	if p.isSingleton() {
		return 1
	}
	return p.bm.GetCardinality()
}

func (p postingList) Contains(id uint64) bool {
	if p.bm == nil {
		return false
	}
	if p.isSingleton() {
		return p.single == id
	}
	return p.bm.Contains(id)
}

func (p postingList) Minimum() (uint64, bool) {
	if p.bm == nil {
		return 0, false
	}
	if p.isSingleton() {
		return p.single, true
	}
	return p.bm.Minimum(), true
}

func (p postingList) Maximum() (uint64, bool) {
	if p.bm == nil {
		return 0, false
	}
	if p.isSingleton() {
		return p.single, true
	}
	return p.bm.Maximum(), true
}

func (p postingList) SizeInBytes() uint64 {
	if p.bm == nil {
		return 0
	}
	if p.isSingleton() {
		return 8
	}
	return p.bm.GetSizeInBytes()
}

func (p postingList) ToBitmapOwned(scratch *roaring64.Bitmap) (*roaring64.Bitmap, bool) {
	if p.bm == nil {
		return nil, false
	}
	if p.isSingleton() {
		if scratch != nil {
			scratch.Clear()
			scratch.Add(p.single)
			return scratch, true
		}
		bm := getRoaringBuf()
		bm.Add(p.single)
		return bm, true
	}
	return p.bm, false
}

func (p postingList) ForEach(fn func(uint64) bool) bool {
	if p.bm == nil {
		return true
	}
	if p.isSingleton() {
		return fn(p.single)
	}
	it := p.bm.Iterator()
	defer releaseRoaringBitmapIterator(it)
	for it.HasNext() {
		if !fn(it.Next()) {
			return false
		}
	}
	return true
}

func (p postingList) Iter() roaringIter {
	if p.IsEmpty() {
		return emptyIter{}
	}
	if p.isSingleton() {
		return newSingletonIter(p.single)
	}
	return p.bitmap().Iterator()
}

func (p postingList) IntersectsBitmap(other *roaring64.Bitmap) bool {
	if other == nil || other.IsEmpty() {
		return false
	}
	if p.bm == nil {
		return false
	}
	if p.isSingleton() {
		return other.Contains(p.single)
	}
	return p.bm.Intersects(other)
}

func (p postingList) AndCardinalityBitmap(other *roaring64.Bitmap) uint64 {
	if other == nil || other.IsEmpty() {
		return 0
	}
	if p.bm == nil {
		return 0
	}
	if p.isSingleton() {
		if other.Contains(p.single) {
			return 1
		}
		return 0
	}
	return p.bm.AndCardinality(other)
}

func (p postingList) OrInto(dst *roaring64.Bitmap) {
	if dst == nil {
		return
	}
	if p.bm == nil {
		return
	}
	if p.isSingleton() {
		dst.Add(p.single)
		return
	}
	dst.Or(p.bm)
}

func (p postingList) AndNotFrom(dst *roaring64.Bitmap) {
	if dst == nil {
		return
	}
	if p.bm == nil {
		return
	}
	if p.isSingleton() {
		dst.Remove(p.single)
		return
	}
	dst.AndNot(p.bm)
}

func (p *postingList) Add(id uint64) {
	if p == nil {
		return
	}
	if p.bm == nil {
		p.bm = postingSingleFlag
		p.single = id
		return
	}
	if p.isSingleton() {
		if p.single == id {
			return
		}
		bm := roaring64.NewBitmap()
		bm.Add(p.single)
		bm.Add(id)
		p.bm = bm
		p.single = 0
		return
	}
	p.bm.Add(id)
}

func (p *postingList) Or(other postingList) {
	if p == nil || other.IsEmpty() {
		return
	}
	if other.isSingleton() {
		p.Add(other.single)
		return
	}

	// other is bitmap-backed.
	if p.bm == nil {
		p.bm = other.bm
		return
	}
	if p.isSingleton() {
		bm := roaring64.NewBitmap()
		bm.Add(p.single)
		bm.Or(other.bm)
		p.bm = bm
		p.single = 0
		return
	}
	p.bm.Or(other.bm)
}

func (p *postingList) OptimizeAdaptive() {
	if p == nil || p.bm == nil || p.isSingleton() {
		return
	}
	p.bm.RunOptimize()
	if p.bm.GetCardinality() == 1 {
		p.single = p.bm.Minimum()
		releaseRoaringBuf(p.bm)
		p.bm = postingSingleFlag
	}
}
