//go:build !rbidebug

package rbi

import (
	"sync"

	"github.com/vapstack/rbi/internal/roaring64"
)

var roaringPool sync.Pool

func getRoaringBuf() *roaring64.Bitmap {
	if v := roaringPool.Get(); v != nil {
		return v.(*roaring64.Bitmap)
	}
	return roaring64.New()
}

func releaseRoaringBuf(bm *roaring64.Bitmap) {
	if bm == nil {
		return
	}
	if bm == postingSingleFlag {
		return
	}
	bm.Clear()
	roaringPool.Put(bm)
}

func cloneBitmap(src *roaring64.Bitmap) *roaring64.Bitmap {
	if src == nil {
		return getRoaringBuf()
	}
	dst := getRoaringBuf()
	return src.CloneInto(dst)
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
	if v := intSlicePool.Get(); v != nil {
		return v.(*intSliceBuf)
	}
	return &intSliceBuf{values: make([]int, 0, max(capHint, 64))}
}

func releaseIntSliceBuf(buf *intSliceBuf) {
	if buf == nil {
		return
	}
	if cap(buf.values) > intSlicePoolMaxCap {
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
	if cap(buf.values) > uint64SlicePoolMaxCap {
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
	if cap(buf.values) > roaringSlicePoolMaxCap {
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
	if cap(buf.values) > postingListSlicePoolMaxCap {
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
	if cap(buf.values) > bitmapResultSlicePoolMaxCap {
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
	if cap(buf.values) > countORBranchSlicePoolMaxCap {
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
	if cap(buf.values) > plannerOROrderIterSlicePoolMaxCap {
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
	if cap(buf.values) > plannerOROrderMergeItemSliceMaxCap {
		return
	}
	clear(buf.values)
	buf.values = buf.values[:0]
	plannerOROrderMergeItemSlicePool.Put(buf)
}

/**/

var uniqueLeavingOuterPool sync.Pool

var uniqueLeavingInnerPool sync.Pool

var uniqueSeenOuterPool sync.Pool

var uniqueSeenInnerPool sync.Pool

func getUniqueLeavingOuterMap() map[string]map[string]*roaring64.Bitmap {
	if v := uniqueLeavingOuterPool.Get(); v != nil {
		return v.(map[string]map[string]*roaring64.Bitmap)
	}
	return make(map[string]map[string]*roaring64.Bitmap, 8)
}

func releaseUniqueLeavingOuterMap(m map[string]map[string]*roaring64.Bitmap) {
	if m == nil {
		return
	}
	oversized := len(m) > pooledUniqueOuterMaxLen
	for _, inner := range m {
		releaseUniqueLeavingInnerMap(inner)
	}
	clear(m)
	if oversized {
		return
	}
	uniqueLeavingOuterPool.Put(m)
}

func getUniqueLeavingInnerMap() map[string]*roaring64.Bitmap {
	if v := uniqueLeavingInnerPool.Get(); v != nil {
		return v.(map[string]*roaring64.Bitmap)
	}
	return make(map[string]*roaring64.Bitmap, 8)
}

func releaseUniqueLeavingInnerMap(m map[string]*roaring64.Bitmap) {
	if m == nil {
		return
	}
	oversized := len(m) > pooledUniqueInnerMaxLen
	for _, bm := range m {
		releaseRoaringBuf(bm)
	}
	clear(m)
	if oversized {
		return
	}
	uniqueLeavingInnerPool.Put(m)
}

func getUniqueSeenOuterMap() map[string]map[string]uint64 {
	if v := uniqueSeenOuterPool.Get(); v != nil {
		return v.(map[string]map[string]uint64)
	}
	return make(map[string]map[string]uint64, 8)
}

func releaseUniqueSeenOuterMap(m map[string]map[string]uint64) {
	if m == nil {
		return
	}
	oversized := len(m) > pooledUniqueOuterMaxLen
	for _, inner := range m {
		releaseUniqueSeenInnerMap(inner)
	}
	clear(m)
	if oversized {
		return
	}
	uniqueSeenOuterPool.Put(m)
}

func getUniqueSeenInnerMap() map[string]uint64 {
	if v := uniqueSeenInnerPool.Get(); v != nil {
		return v.(map[string]uint64)
	}
	return make(map[string]uint64, 8)
}

func releaseUniqueSeenInnerMap(m map[string]uint64) {
	if m == nil {
		return
	}
	oversized := len(m) > pooledUniqueInnerMaxLen
	clear(m)
	if oversized {
		return
	}
	uniqueSeenInnerPool.Put(m)
}

var writeDeltaOuterPool sync.Pool

var writeDeltaInnerPool sync.Pool

func getWriteDeltaOuterMap() map[string]map[string]indexDeltaEntry {
	if v := writeDeltaOuterPool.Get(); v != nil {
		return v.(map[string]map[string]indexDeltaEntry)
	}
	return make(map[string]map[string]indexDeltaEntry, 8)
}

func releaseWriteDeltaOuterMap(m map[string]map[string]indexDeltaEntry) {
	if m == nil {
		return
	}
	oversized := len(m) > pooledWriteDeltaOuterMaxLen
	for _, inner := range m {
		releaseWriteDeltaInnerMap(inner)
	}
	clear(m)
	if oversized {
		return
	}
	writeDeltaOuterPool.Put(m)
}

func getWriteDeltaInnerMap() map[string]indexDeltaEntry {
	if v := writeDeltaInnerPool.Get(); v != nil {
		return v.(map[string]indexDeltaEntry)
	}
	return make(map[string]indexDeltaEntry, 8)
}

func releaseWriteDeltaInnerMap(m map[string]indexDeltaEntry) {
	if m == nil {
		return
	}
	oversized := len(m) > pooledWriteDeltaInnerMaxLen
	for _, e := range m {
		releaseRoaringBuf(e.add)
		releaseRoaringBuf(e.del)
	}
	clear(m)
	if oversized {
		return
	}
	writeDeltaInnerPool.Put(m)
}
