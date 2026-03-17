package roaring64

import (
	"sync"

	"github.com/vapstack/rbi/internal/roaring64/roaring"
)

var intIterator64Pool sync.Pool

func acquireIntIterator64(a *Bitmap) *intIterator {
	if v := intIterator64Pool.Get(); v != nil {
		bitmapIterator64PoolStats.onGetHit()
		it := v.(*intIterator)
		it.Initialize(a)
		return it
	}
	bitmapIterator64PoolStats.onGetMiss()
	it := &intIterator{}
	it.Initialize(a)
	return it
}

func releaseIntIterator64(it *intIterator) {
	if it == nil {
		bitmapIterator64PoolStats.onDropNil()
		return
	}
	if it.iter != nil {
		roaring.ReleaseIterator(it.iter)
	}
	it.pos = 0
	it.hs = 0
	it.iter = nil
	it.highlowcontainer = nil
	intIterator64Pool.Put(it)
	bitmapIterator64PoolStats.onPut()
}

func ReleaseIterator(it IntPeekable64) {
	if releasable, ok := it.(interface{ Release() }); ok {
		releasable.Release()
	}
}

/**/

type addManyBatch struct {
	values []uint32
}

var addManyBatchPool sync.Pool

func acquireAddManyBatch(capHint int) *addManyBatch {
	if capHint < 32 {
		capHint = 32
	}
	addManyBatchPoolStats.noteRequested(capHint)

	if v := addManyBatchPool.Get(); v != nil {
		buf := v.(*addManyBatch)
		if cap(buf.values) >= capHint {
			addManyBatchPoolStats.base.onGetHit()
			buf.values = buf.values[:0]
			return buf
		}
		addManyBatchPoolStats.base.onGetMiss()
		buf.values = make([]uint32, 0, capHint)
		return buf
	}

	addManyBatchPoolStats.base.onGetMiss()
	return &addManyBatch{values: make([]uint32, 0, capHint)}
}

func releaseAddManyBatch(buf *addManyBatch) {
	if buf == nil {
		addManyBatchPoolStats.base.onDropNil()
		return
	}
	addManyBatchPoolStats.noteReturned(cap(buf.values))
	if cap(buf.values) > maxPooledAddManyBatchCapacity {
		addManyBatchPoolStats.base.onDropRejected()
		return
	}
	buf.values = buf.values[:0]
	addManyBatchPool.Put(buf)
	addManyBatchPoolStats.base.onPut()
}
