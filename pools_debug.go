//go:build rbidebug

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

// PoolStats returns a process-wide snapshot of pool activity and retention.
//
// Pool counters are global to the current process and may include activity from
// other DB instances sharing the same package-level pools.
//
// This method is available only in builds compiled with the `rbidebug` build tag.
func (db *DB[K, V]) PoolStats() PoolStats {
	_ = db
	return snapshotPoolStats()
}

// PoolStatsSinceReset returns pool stats relative to the last ResetPoolStats call.
//
// This uses a process-wide baseline over the same global package-level pools as
// PoolStats. Lifetime counters remain available through PoolStats().
func (db *DB[K, V]) PoolStatsSinceReset() PoolStats {
	_ = db
	return snapshotPoolStatsSinceReset()
}

// ResetPoolStats starts a new process-wide pool stats window used by
// PoolStatsSinceReset.
//
// This does not clear lifetime counters returned by PoolStats(); it only moves
// the baseline used for reset-relative snapshots.
func (db *DB[K, V]) ResetPoolStats() {
	_ = db
	resetPoolStats()
}

/**/

var uniqueLeavingOuterPool sync.Pool

var uniqueLeavingInnerPool sync.Pool

var uniqueSeenOuterPool sync.Pool

var uniqueSeenInnerPool sync.Pool

func getUniqueLeavingOuterMap() map[string]map[string]*roaring64.Bitmap {
	if v := uniqueLeavingOuterPool.Get(); v != nil {
		uniqueLeavingOuterMapStats.base.onGetHit()
		return v.(map[string]map[string]*roaring64.Bitmap)
	}
	uniqueLeavingOuterMapStats.base.onGetMiss()
	return make(map[string]map[string]*roaring64.Bitmap, 8)
}

func releaseUniqueLeavingOuterMap(m map[string]map[string]*roaring64.Bitmap) {
	if m == nil {
		uniqueLeavingOuterMapStats.base.onDropNil()
		return
	}
	uniqueLeavingOuterMapStats.noteReturned(len(m))
	oversized := len(m) > pooledUniqueOuterMaxLen
	for _, inner := range m {
		releaseUniqueLeavingInnerMap(inner)
	}
	clear(m)
	if oversized {
		uniqueLeavingOuterMapStats.base.onDropRejected()
		return
	}
	uniqueLeavingOuterPool.Put(m)
	uniqueLeavingOuterMapStats.base.onPut()
}

func getUniqueLeavingInnerMap() map[string]*roaring64.Bitmap {
	if v := uniqueLeavingInnerPool.Get(); v != nil {
		uniqueLeavingInnerMapStats.base.onGetHit()
		return v.(map[string]*roaring64.Bitmap)
	}
	uniqueLeavingInnerMapStats.base.onGetMiss()
	return make(map[string]*roaring64.Bitmap, 8)
}

func releaseUniqueLeavingInnerMap(m map[string]*roaring64.Bitmap) {
	if m == nil {
		uniqueLeavingInnerMapStats.base.onDropNil()
		return
	}
	uniqueLeavingInnerMapStats.noteReturned(len(m))
	oversized := len(m) > pooledUniqueInnerMaxLen
	for _, bm := range m {
		releaseRoaringBuf(bm)
	}
	clear(m)
	if oversized {
		uniqueLeavingInnerMapStats.base.onDropRejected()
		return
	}
	uniqueLeavingInnerPool.Put(m)
	uniqueLeavingInnerMapStats.base.onPut()
}

func getUniqueSeenOuterMap() map[string]map[string]uint64 {
	if v := uniqueSeenOuterPool.Get(); v != nil {
		uniqueSeenOuterMapStats.base.onGetHit()
		return v.(map[string]map[string]uint64)
	}
	uniqueSeenOuterMapStats.base.onGetMiss()
	return make(map[string]map[string]uint64, 8)
}

func releaseUniqueSeenOuterMap(m map[string]map[string]uint64) {
	if m == nil {
		uniqueSeenOuterMapStats.base.onDropNil()
		return
	}
	uniqueSeenOuterMapStats.noteReturned(len(m))
	oversized := len(m) > pooledUniqueOuterMaxLen
	for _, inner := range m {
		releaseUniqueSeenInnerMap(inner)
	}
	clear(m)
	if oversized {
		uniqueSeenOuterMapStats.base.onDropRejected()
		return
	}
	uniqueSeenOuterPool.Put(m)
	uniqueSeenOuterMapStats.base.onPut()
}

func getUniqueSeenInnerMap() map[string]uint64 {
	if v := uniqueSeenInnerPool.Get(); v != nil {
		uniqueSeenInnerMapStats.base.onGetHit()
		return v.(map[string]uint64)
	}
	uniqueSeenInnerMapStats.base.onGetMiss()
	return make(map[string]uint64, 8)
}

func releaseUniqueSeenInnerMap(m map[string]uint64) {
	if m == nil {
		uniqueSeenInnerMapStats.base.onDropNil()
		return
	}
	uniqueSeenInnerMapStats.noteReturned(len(m))
	oversized := len(m) > pooledUniqueInnerMaxLen
	clear(m)
	if oversized {
		uniqueSeenInnerMapStats.base.onDropRejected()
		return
	}
	uniqueSeenInnerPool.Put(m)
	uniqueSeenInnerMapStats.base.onPut()
}

var writeDeltaOuterPool sync.Pool

var writeDeltaInnerPool sync.Pool

func getWriteDeltaOuterMap() map[string]map[string]indexDeltaEntry {
	if v := writeDeltaOuterPool.Get(); v != nil {
		writeDeltaOuterMapStats.base.onGetHit()
		return v.(map[string]map[string]indexDeltaEntry)
	}
	writeDeltaOuterMapStats.base.onGetMiss()
	return make(map[string]map[string]indexDeltaEntry, 8)
}

func releaseWriteDeltaOuterMap(m map[string]map[string]indexDeltaEntry) {
	if m == nil {
		writeDeltaOuterMapStats.base.onDropNil()
		return
	}
	writeDeltaOuterMapStats.noteReturned(len(m))
	oversized := len(m) > pooledWriteDeltaOuterMaxLen
	for _, inner := range m {
		releaseWriteDeltaInnerMap(inner)
	}
	clear(m)
	if oversized {
		writeDeltaOuterMapStats.base.onDropRejected()
		return
	}
	writeDeltaOuterPool.Put(m)
	writeDeltaOuterMapStats.base.onPut()
}

func getWriteDeltaInnerMap() map[string]indexDeltaEntry {
	if v := writeDeltaInnerPool.Get(); v != nil {
		writeDeltaInnerMapStats.base.onGetHit()
		return v.(map[string]indexDeltaEntry)
	}
	writeDeltaInnerMapStats.base.onGetMiss()
	return make(map[string]indexDeltaEntry, 8)
}

func releaseWriteDeltaInnerMap(m map[string]indexDeltaEntry) {
	if m == nil {
		writeDeltaInnerMapStats.base.onDropNil()
		return
	}
	writeDeltaInnerMapStats.noteReturned(len(m))
	oversized := len(m) > pooledWriteDeltaInnerMaxLen
	for _, e := range m {
		releaseRoaringBuf(e.add)
		releaseRoaringBuf(e.del)
	}
	clear(m)
	if oversized {
		writeDeltaInnerMapStats.base.onDropRejected()
		return
	}
	writeDeltaInnerPool.Put(m)
	writeDeltaInnerMapStats.base.onPut()
}
