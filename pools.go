package rbi

import (
	"sync"

	"github.com/vapstack/rbi/internal/roaring64"
)

var roaringPool sync.Pool

func getRoaringBuf() *roaring64.Bitmap {
	if v := roaringPool.Get(); v != nil {
		roaringBufPoolStats.onGetHit()
		return v.(*roaring64.Bitmap)
	}
	roaringBufPoolStats.onGetMiss()
	return roaring64.New()
}

func releaseRoaringBuf(bm *roaring64.Bitmap) {
	if bm == nil {
		roaringBufPoolStats.onDropNil()
		return
	}
	if bm == postingSingleFlag {
		roaringBufPoolStats.onDropRejected()
		return
	}
	bm.Clear()
	roaringPool.Put(bm)
	roaringBufPoolStats.onPut()
}

func cloneBitmap(src *roaring64.Bitmap) *roaring64.Bitmap {
	if src == nil {
		return getRoaringBuf()
	}
	return src.Clone()
}

func releaseRoaringBitmapIterator(it roaring64.IntPeekable64) {
	roaring64.ReleaseIterator(it)
}

func releaseRoaringIter(it any) {
	if releasable, ok := it.(interface{ Release() }); ok {
		releasable.Release()
	}
}

/**/

type intSliceBuf struct{ values []int }

var intSlicePool sync.Pool

func getIntSliceBuf(capHint int) *intSliceBuf {
	intSlicePoolStats.noteRequested(capHint)
	if v := intSlicePool.Get(); v != nil {
		intSlicePoolStats.base.onGetHit()
		return v.(*intSliceBuf)
	}
	intSlicePoolStats.base.onGetMiss()
	return &intSliceBuf{values: make([]int, 0, max(capHint, 64))}
}

func releaseIntSliceBuf(buf *intSliceBuf) {
	if buf == nil {
		intSlicePoolStats.base.onDropNil()
		return
	}
	intSlicePoolStats.noteReturned(cap(buf.values))
	if cap(buf.values) > intSlicePoolMaxCap {
		intSlicePoolStats.base.onDropRejected()
		return
	}
	buf.values = buf.values[:0]
	intSlicePool.Put(buf)
	intSlicePoolStats.base.onPut()
}

/**/

type uint64SliceBuf struct{ values []uint64 }

var uint64SlicePool sync.Pool

func getUint64SliceBuf(capHint int) *uint64SliceBuf {
	uint64SlicePoolStats.noteRequested(capHint)
	if v := uint64SlicePool.Get(); v != nil {
		uint64SlicePoolStats.base.onGetHit()
		return v.(*uint64SliceBuf)
	}
	uint64SlicePoolStats.base.onGetMiss()
	return &uint64SliceBuf{values: make([]uint64, 0, max(capHint, 64))}
}

func releaseUint64SliceBuffer(buf *uint64SliceBuf) {
	if buf == nil {
		uint64SlicePoolStats.base.onDropNil()
		return
	}
	uint64SlicePoolStats.noteReturned(cap(buf.values))
	if cap(buf.values) > uint64SlicePoolMaxCap {
		uint64SlicePoolStats.base.onDropRejected()
		return
	}
	buf.values = buf.values[:0]
	uint64SlicePool.Put(buf)
	uint64SlicePoolStats.base.onPut()
}

/**/

type roaringSliceBuf struct{ values []*roaring64.Bitmap }

var roaringSlicePool sync.Pool

func getRoaringSliceBuf(capHint int) *roaringSliceBuf {
	roaringSlicePoolStats.noteRequested(capHint)
	if v := roaringSlicePool.Get(); v != nil {
		roaringSlicePoolStats.base.onGetHit()
		return v.(*roaringSliceBuf)
	}
	roaringSlicePoolStats.base.onGetMiss()
	return &roaringSliceBuf{values: make([]*roaring64.Bitmap, 0, max(capHint, 32))}
}

func releaseRoaringSliceBuf(buf *roaringSliceBuf) {
	if buf == nil {
		roaringSlicePoolStats.base.onDropNil()
		return
	}
	roaringSlicePoolStats.noteReturned(cap(buf.values))
	if cap(buf.values) > roaringSlicePoolMaxCap {
		roaringSlicePoolStats.base.onDropRejected()
		return
	}
	clear(buf.values)
	buf.values = buf.values[:0]
	roaringSlicePool.Put(buf)
	roaringSlicePoolStats.base.onPut()
}

/**/

type postingListSliceBuf struct{ values []postingList }

var postingListSlicePool sync.Pool

func getPostingListSliceBuf(capHint int) *postingListSliceBuf {
	postingListSlicePoolStats.noteRequested(capHint)
	if v := postingListSlicePool.Get(); v != nil {
		postingListSlicePoolStats.base.onGetHit()
		return v.(*postingListSliceBuf)
	}
	postingListSlicePoolStats.base.onGetMiss()
	return &postingListSliceBuf{values: make([]postingList, 0, max(capHint, 16))}
}

func releasePostingListSliceBuf(buf *postingListSliceBuf) {
	if buf == nil {
		postingListSlicePoolStats.base.onDropNil()
		return
	}
	postingListSlicePoolStats.noteReturned(cap(buf.values))
	if cap(buf.values) > postingListSlicePoolMaxCap {
		postingListSlicePoolStats.base.onDropRejected()
		return
	}
	clear(buf.values)
	buf.values = buf.values[:0]
	postingListSlicePool.Put(buf)
	postingListSlicePoolStats.base.onPut()
}

/**/

type bitmapResultSliceBuf struct{ values []bitmap }

var bitmapResultSlicePool sync.Pool

func getBitmapResultSliceBuf(capHint int) *bitmapResultSliceBuf {
	bitmapResultSlicePoolStats.noteRequested(capHint)
	if v := bitmapResultSlicePool.Get(); v != nil {
		bitmapResultSlicePoolStats.base.onGetHit()
		return v.(*bitmapResultSliceBuf)
	}
	bitmapResultSlicePoolStats.base.onGetMiss()
	return &bitmapResultSliceBuf{values: make([]bitmap, 0, max(capHint, 16))}
}

func releaseBitmapResultSliceBuf(buf *bitmapResultSliceBuf) {
	if buf == nil {
		bitmapResultSlicePoolStats.base.onDropNil()
		return
	}
	bitmapResultSlicePoolStats.noteReturned(cap(buf.values))
	if cap(buf.values) > bitmapResultSlicePoolMaxCap {
		bitmapResultSlicePoolStats.base.onDropRejected()
		return
	}
	clear(buf.values)
	buf.values = buf.values[:0]
	bitmapResultSlicePool.Put(buf)
	bitmapResultSlicePoolStats.base.onPut()
}

/**/

type countORBranchSliceBuf struct{ values countORBranches }

var countORBranchSlicePool sync.Pool

func getCountORBranchSliceBuf(capHint int) *countORBranchSliceBuf {
	countORBranchSlicePoolStats.noteRequested(capHint)
	if v := countORBranchSlicePool.Get(); v != nil {
		countORBranchSlicePoolStats.base.onGetHit()
		return v.(*countORBranchSliceBuf)
	}
	countORBranchSlicePoolStats.base.onGetMiss()
	return &countORBranchSliceBuf{values: make(countORBranches, 0, max(capHint, 8))}
}

func releaseCountORBranchSliceBuf(buf *countORBranchSliceBuf) {
	if buf == nil {
		countORBranchSlicePoolStats.base.onDropNil()
		return
	}
	countORBranchSlicePoolStats.noteReturned(cap(buf.values))
	if cap(buf.values) > countORBranchSlicePoolMaxCap {
		countORBranchSlicePoolStats.base.onDropRejected()
		return
	}
	clear(buf.values)
	buf.values = buf.values[:0]
	countORBranchSlicePool.Put(buf)
	countORBranchSlicePoolStats.base.onPut()
}

/**/

type plannerOROrderIterSliceBuf struct{ values []plannerOROrderBranchIter }

var plannerOROrderIterSlicePool sync.Pool

func getPlannerOROrderIterSliceBuf(capHint int) *plannerOROrderIterSliceBuf {
	plannerOROrderIterSlicePoolStats.noteRequested(capHint)
	if v := plannerOROrderIterSlicePool.Get(); v != nil {
		plannerOROrderIterSlicePoolStats.base.onGetHit()
		return v.(*plannerOROrderIterSliceBuf)
	}
	plannerOROrderIterSlicePoolStats.base.onGetMiss()
	return &plannerOROrderIterSliceBuf{values: make([]plannerOROrderBranchIter, 0, max(capHint, 16))}
}

func releasePlannerOROrderIterSliceBuf(buf *plannerOROrderIterSliceBuf) {
	if buf == nil {
		plannerOROrderIterSlicePoolStats.base.onDropNil()
		return
	}
	plannerOROrderIterSlicePoolStats.noteReturned(cap(buf.values))
	if cap(buf.values) > plannerOROrderIterSlicePoolMaxCap {
		plannerOROrderIterSlicePoolStats.base.onDropRejected()
		return
	}
	clear(buf.values)
	buf.values = buf.values[:0]
	plannerOROrderIterSlicePool.Put(buf)
	plannerOROrderIterSlicePoolStats.base.onPut()
}

/**/

type plannerOROrderMergeItemSliceBuf struct{ values []plannerOROrderMergeItem }

var plannerOROrderMergeItemSlicePool sync.Pool

func getPlannerOROrderMergeItemSliceBuf(capHint int) *plannerOROrderMergeItemSliceBuf {
	plannerOROrderMergeItemPoolStats.noteRequested(capHint)
	if v := plannerOROrderMergeItemSlicePool.Get(); v != nil {
		plannerOROrderMergeItemPoolStats.base.onGetHit()
		return v.(*plannerOROrderMergeItemSliceBuf)
	}
	plannerOROrderMergeItemPoolStats.base.onGetMiss()
	return &plannerOROrderMergeItemSliceBuf{values: make([]plannerOROrderMergeItem, 0, max(capHint, 16))}
}

func releasePlannerOROrderMergeItemSliceBuf(buf *plannerOROrderMergeItemSliceBuf) {
	if buf == nil {
		plannerOROrderMergeItemPoolStats.base.onDropNil()
		return
	}
	plannerOROrderMergeItemPoolStats.noteReturned(cap(buf.values))
	if cap(buf.values) > plannerOROrderMergeItemSliceMaxCap {
		plannerOROrderMergeItemPoolStats.base.onDropRejected()
		return
	}
	clear(buf.values)
	buf.values = buf.values[:0]
	plannerOROrderMergeItemSlicePool.Put(buf)
	plannerOROrderMergeItemPoolStats.base.onPut()
}
