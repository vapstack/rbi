//go:build rbidebug

package roaring

import (
	"sync"
)

const bitmapContainerWords = maxCapacity / 64

var bitmapContainerPool sync.Pool

var pooledArrayContainerCapacities = [...]int{
	32,
	64,
	128,
	256,
	512,
	1024,
	1536,
	2048,
	3072,
	4096,
	6144,
	8192,
	12288,
	16384,
}

// maxPooledArrayContainerCapacity limits which arrayContainer capacities are
// returned to the pool.
var maxPooledArrayContainerCapacity = 4 * arrayDefaultMaxSize

var arrayContainerClassPools [len(pooledArrayContainerCapacities)]sync.Pool

func pooledArrayContainerIndex(size int) int {
	if size <= 0 {
		size = pooledArrayContainerCapacities[0]
	}
	if size > maxPooledArrayContainerCapacity {
		return -1
	}
	for i, class := range pooledArrayContainerCapacities {
		if size <= class {
			return i
		}
	}
	return -1
}

func pooledArrayContainerPutIndex(size int) int {
	if size <= 0 || size > maxPooledArrayContainerCapacity {
		return -1
	}
	idx := -1
	for i, class := range pooledArrayContainerCapacities {
		if size < class {
			break
		}
		idx = i
	}
	return idx
}

func newBitmapContainer() *bitmapContainer {
	if v := bitmapContainerPool.Get(); v != nil {
		bitmapContainerPoolStats.onGetHit()
		bc := v.(*bitmapContainer)
		bc.cardinality = 0
		return bc
	}
	bitmapContainerPoolStats.onGetMiss()
	return &bitmapContainer{
		bitmap: make([]uint64, bitmapContainerWords),
	}
}

func releaseBitmapContainer(bc *bitmapContainer) {
	if bc == nil {
		bitmapContainerPoolStats.onDropNil()
		return
	}
	if len(bc.bitmap) != bitmapContainerWords {
		bitmapContainerPoolStats.onDropRejected()
		return
	}
	bc.cardinality = 0
	clear(bc.bitmap)
	bitmapContainerPool.Put(bc)
	bitmapContainerPoolStats.onPut()
}

func newArrayContainer() *arrayContainer {
	return acquireArrayContainer(pooledArrayContainerCapacities[0], 0)
}

func newArrayContainerCapacity(size int) *arrayContainer {
	if size <= 0 {
		size = pooledArrayContainerCapacities[0]
	}
	return acquireArrayContainer(size, 0)
}

func newArrayContainerSize(size int) *arrayContainer {
	if size <= 0 {
		return newArrayContainer()
	}
	return acquireArrayContainer(size, size)
}

func newArrayContainerFromSlice(src []uint16) *arrayContainer {
	if len(src) == 0 {
		return newArrayContainer()
	}
	ac := newArrayContainerSize(len(src))
	copy(ac.content, src)
	return ac
}

func acquireArrayContainer(capacity, length int) *arrayContainer {
	if capacity < length {
		capacity = length
	}
	arrayContainerClassPoolStats.noteRequestedCapacity(capacity)

	idx := pooledArrayContainerIndex(capacity)
	if idx < 0 {
		arrayContainerPoolStats.onGetMiss()
		arrayContainerClassPoolStats.noteDirectAlloc(capacity)
		return &arrayContainer{content: make([]uint16, length, capacity)}
	}

	if v := arrayContainerClassPools[idx].Get(); v != nil {
		ac := v.(*arrayContainer)
		arrayContainerPoolStats.onGetHit()
		arrayContainerClassStats[idx].onGetHit()
		ac.content = ac.content[:length]
		return ac
	}

	arrayContainerPoolStats.onGetMiss()
	arrayContainerClassStats[idx].onGetMiss()
	return &arrayContainer{
		content: make([]uint16, pooledArrayContainerCapacities[idx])[:length],
	}
}

func releaseArrayContainer(ac *arrayContainer) {
	if ac == nil {
		arrayContainerPoolStats.onDropNil()
		return
	}

	capacity := cap(ac.content)
	arrayContainerClassPoolStats.noteReturnedCapacity(capacity)
	if capacity > maxPooledArrayContainerCapacity {
		arrayContainerClassPoolStats.dropOutOfRange.Add(1)
		arrayContainerPoolStats.onDropRejected()
		return
	}

	idx := pooledArrayContainerPutIndex(capacity)
	if idx < 0 {
		arrayContainerClassPoolStats.dropBelowMinCapacity.Add(1)
		arrayContainerPoolStats.onDropRejected()
		return
	}

	ac.content = ac.content[:0]
	arrayContainerClassPools[idx].Put(ac)
	arrayContainerClassStats[idx].onPut()
	arrayContainerPoolStats.onPut()
}

func replaceArrayContainerStorage(ac, donor *arrayContainer) {
	oldContent := ac.content
	ac.content = donor.content
	donor.content = oldContent
	releaseArrayContainer(donor)
}

func releaseContainer(c container) {
	switch x := c.(type) {
	case *arrayContainer:
		releaseArrayContainer(x)
	case *bitmapContainer:
		releaseBitmapContainer(x)
	}
}

/**/

var (
	// ByteInputAdapterPool shared pool
	ByteInputAdapterPool = sync.Pool{
		New: func() interface{} {
			return &ByteInputAdapter{}
		},
	}

	// ByteBufferPool shared pool
	ByteBufferPool = sync.Pool{
		New: func() interface{} {
			return &ByteBuffer{}
		},
	}
)

/**/

var (
	intIteratorPool     sync.Pool
	manyIntIteratorPool sync.Pool
)

func acquireIntIterator(a *Bitmap) *intIterator {
	if v := intIteratorPool.Get(); v != nil {
		bitmapIteratorPoolStats.onGetHit()
		it := v.(*intIterator)
		it.Initialize(a)
		return it
	}
	bitmapIteratorPoolStats.onGetMiss()
	it := &intIterator{}
	it.Initialize(a)
	return it
}

func releaseIntIterator(it *intIterator) {
	if it == nil {
		bitmapIteratorPoolStats.onDropNil()
		return
	}
	it.pos = 0
	it.hs = 0
	it.iter = nil
	it.highlowcontainer = nil
	it.shortIter = shortIterator{}
	it.runIter = runIterator16{}
	it.bitmapIter = bitmapContainerShortIterator{}
	intIteratorPool.Put(it)
	bitmapIteratorPoolStats.onPut()
}

func acquireManyIntIterator(a *Bitmap) *manyIntIterator {
	if v := manyIntIteratorPool.Get(); v != nil {
		manyBitmapIteratorPoolStats.onGetHit()
		it := v.(*manyIntIterator)
		it.Initialize(a)
		return it
	}
	manyBitmapIteratorPoolStats.onGetMiss()
	it := &manyIntIterator{}
	it.Initialize(a)
	return it
}

func releaseManyIntIterator(it *manyIntIterator) {
	if it == nil {
		manyBitmapIteratorPoolStats.onDropNil()
		return
	}
	it.pos = 0
	it.hs = 0
	it.iter = nil
	it.highlowcontainer = nil
	it.shortIter = shortIterator{}
	it.runIter = runIterator16{}
	it.bitmapIter = bitmapContainerManyIterator{}
	manyIntIteratorPool.Put(it)
	manyBitmapIteratorPoolStats.onPut()
}

func ReleaseIterator(it IntPeekable) {
	if releasable, ok := it.(interface{ Release() }); ok {
		releasable.Release()
	}
}

func ReleaseManyIterator(it ManyIntIterable) {
	if releasable, ok := it.(interface{ Release() }); ok {
		releasable.Release()
	}
}
