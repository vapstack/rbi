package rbi

import (
	"sync"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

var roaringPool = sync.Pool{
	New: func() any { return roaring64.New() },
}

func getRoaringBuf() *roaring64.Bitmap {
	return roaringPool.Get().(*roaring64.Bitmap)
}

func releaseRoaringBuf(bm *roaring64.Bitmap) {
	if bm == nil || bm == postingSingleFlag {
		return
	}
	bm.Clear()
	roaringPool.Put(bm)
}

func cloneBitmap(src *roaring64.Bitmap) *roaring64.Bitmap {
	if src == nil {
		return getRoaringBuf()
	}
	return src.Clone()
}

/**/

type intSliceBuf struct{ values []int }

var intSlicePool sync.Pool

func getIntSliceBuf(capHint int) *intSliceBuf {
	if v := intSlicePool.Get(); v != nil {
		return v.(*intSliceBuf)
	}
	return &intSliceBuf{values: make([]int, 0, max(capHint, 64))}
}

func releaseIntSliceBuf(buf *intSliceBuf) {
	if buf == nil {
		return
	}
	const maxCap = 4 << 10
	if cap(buf.values) > maxCap {
		return
	}
	buf.values = buf.values[:0]
	intSlicePool.Put(buf)
}

/**/

type uint64SliceBuf struct{ values []uint64 }

var uint64SlicePool sync.Pool

func getUint64SliceBuf(capHint int) *uint64SliceBuf {
	if v := uint64SlicePool.Get(); v != nil {
		return v.(*uint64SliceBuf)
	}
	return &uint64SliceBuf{values: make([]uint64, 0, max(capHint, 64))}
}

func releaseUint64SliceBuffer(buf *uint64SliceBuf) {
	if buf == nil {
		return
	}
	const maxCap = 4 << 10
	if cap(buf.values) > maxCap {
		return
	}
	buf.values = buf.values[:0]
	uint64SlicePool.Put(buf)
}

/**/

type roaringSliceBuf struct{ values []*roaring64.Bitmap }

var roaringSlicePool sync.Pool

func getRoaringSliceBuf(capHint int) *roaringSliceBuf {
	if v := roaringSlicePool.Get(); v != nil {
		return v.(*roaringSliceBuf)
	}
	return &roaringSliceBuf{values: make([]*roaring64.Bitmap, 0, max(capHint, 32))}
}

func releaseRoaringSliceBuf(buf *roaringSliceBuf) {
	if buf == nil {
		return
	}
	const maxCap = 2 << 10
	if cap(buf.values) > maxCap {
		return
	}
	clear(buf.values)
	buf.values = buf.values[:0]
	roaringSlicePool.Put(buf)
}

/**/

type postingListSliceBuf struct{ values []postingList }

var postingListSlicePool sync.Pool

func getPostingListSliceBuf(capHint int) *postingListSliceBuf {
	if v := postingListSlicePool.Get(); v != nil {
		return v.(*postingListSliceBuf)
	}
	return &postingListSliceBuf{values: make([]postingList, 0, max(capHint, 16))}
}

func releasePostingListSliceBuf(buf *postingListSliceBuf) {
	if buf == nil {
		return
	}
	const maxCap = 4 << 10
	if cap(buf.values) > maxCap {
		return
	}
	clear(buf.values)
	buf.values = buf.values[:0]
	postingListSlicePool.Put(buf)
}

/**/

type bitmapResultSliceBuf struct{ values []bitmap }

var bitmapResultSlicePool sync.Pool

func getBitmapResultSliceBuf(capHint int) *bitmapResultSliceBuf {
	if v := bitmapResultSlicePool.Get(); v != nil {
		return v.(*bitmapResultSliceBuf)
	}
	return &bitmapResultSliceBuf{values: make([]bitmap, 0, max(capHint, 16))}
}

func releaseBitmapResultSliceBuf(buf *bitmapResultSliceBuf) {
	if buf == nil {
		return
	}
	const maxCap = 2 << 10
	if cap(buf.values) > maxCap {
		return
	}
	clear(buf.values)
	buf.values = buf.values[:0]
	bitmapResultSlicePool.Put(buf)
}

/**/

type countORBranchSliceBuf struct{ values countORBranches }

var countORBranchSlicePool sync.Pool

func getCountORBranchSliceBuf(capHint int) *countORBranchSliceBuf {
	if v := countORBranchSlicePool.Get(); v != nil {
		return v.(*countORBranchSliceBuf)
	}
	return &countORBranchSliceBuf{values: make(countORBranches, 0, max(capHint, 8))}
}

func releaseCountORBranchSliceBuf(buf *countORBranchSliceBuf) {
	if buf == nil {
		return
	}
	const maxCap = 512
	if cap(buf.values) > maxCap {
		return
	}
	clear(buf.values)
	buf.values = buf.values[:0]
	countORBranchSlicePool.Put(buf)
}

/**/

type plannerOROrderIterSliceBuf struct{ values []plannerOROrderBranchIter }

var plannerOROrderIterSlicePool sync.Pool

func getPlannerOROrderIterSliceBuf(capHint int) *plannerOROrderIterSliceBuf {
	if v := plannerOROrderIterSlicePool.Get(); v != nil {
		return v.(*plannerOROrderIterSliceBuf)
	}
	return &plannerOROrderIterSliceBuf{values: make([]plannerOROrderBranchIter, 0, max(capHint, 16))}
}

func releasePlannerOROrderIterSliceBuf(buf *plannerOROrderIterSliceBuf) {
	if buf == nil {
		return
	}
	const maxCap = 512
	if cap(buf.values) > maxCap {
		return
	}
	clear(buf.values)
	buf.values = buf.values[:0]
	plannerOROrderIterSlicePool.Put(buf)
}

/**/

type plannerOROrderMergeItemSliceBuf struct{ values []plannerOROrderMergeItem }

var plannerOROrderMergeItemSlicePool sync.Pool

func getPlannerOROrderMergeItemSliceBuf(capHint int) *plannerOROrderMergeItemSliceBuf {
	if v := plannerOROrderMergeItemSlicePool.Get(); v != nil {
		return v.(*plannerOROrderMergeItemSliceBuf)
	}
	return &plannerOROrderMergeItemSliceBuf{values: make([]plannerOROrderMergeItem, 0, max(capHint, 16))}
}

func releasePlannerOROrderMergeItemSliceBuf(buf *plannerOROrderMergeItemSliceBuf) {
	if buf == nil {
		return
	}
	const maxCap = 512
	if cap(buf.values) > maxCap {
		return
	}
	clear(buf.values)
	buf.values = buf.values[:0]
	plannerOROrderMergeItemSlicePool.Put(buf)
}
