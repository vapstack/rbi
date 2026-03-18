//go:build !rbidebug

package roaring

import (
	"sync"
)

const bitmapContainerWords = maxCapacity / 64

var bitmapPool sync.Pool
var bitmapContainerPool sync.Pool
var runContainerPool sync.Pool

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

const maxPooledRunContainerCapacity = 4 * arrayDefaultMaxSize

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
		bc := v.(*bitmapContainer)
		bc.cardinality = 0
		return bc
	}
	return &bitmapContainer{
		bitmap: make([]uint64, bitmapContainerWords),
	}
}

func acquireBitmap() *Bitmap {
	if v := bitmapPool.Get(); v != nil {
		return v.(*Bitmap)
	}
	return new(Bitmap)
}

func releaseBitmap(rb *Bitmap) {
	if rb == nil {
		return
	}
	rb.highlowcontainer.clear()
	bitmapPool.Put(rb)
}

func releaseBitmapContainer(bc *bitmapContainer) {
	if bc == nil {
		return
	}
	if len(bc.bitmap) != bitmapContainerWords {
		return
	}
	bc.cardinality = 0
	clear(bc.bitmap)
	bitmapContainerPool.Put(bc)
}

func acquireRunContainer16(capHint, length int) *runContainer16 {
	if capHint < length {
		capHint = length
	}

	if v := runContainerPool.Get(); v != nil {
		rc := v.(*runContainer16)
		if cap(rc.iv) >= capHint {
			rc.iv = rc.iv[:length]
			return rc
		}
		rc.iv = make([]interval16, length, capHint)
		return rc
	}

	return &runContainer16{iv: make([]interval16, length, capHint)}
}

func releaseRunContainer16(rc *runContainer16) {
	if rc == nil {
		return
	}
	if cap(rc.iv) > maxPooledRunContainerCapacity {
		return
	}
	rc.iv = rc.iv[:0]
	runContainerPool.Put(rc)
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

	idx := pooledArrayContainerIndex(capacity)
	if idx < 0 {
		return &arrayContainer{content: make([]uint16, length, capacity)}
	}

	if v := arrayContainerClassPools[idx].Get(); v != nil {
		ac := v.(*arrayContainer)
		ac.content = ac.content[:length]
		return ac
	}

	return &arrayContainer{
		content: make([]uint16, pooledArrayContainerCapacities[idx])[:length],
	}
}

func releaseArrayContainer(ac *arrayContainer) {
	if ac == nil {
		return
	}

	capacity := cap(ac.content)
	if capacity > maxPooledArrayContainerCapacity {
		return
	}

	idx := pooledArrayContainerPutIndex(capacity)
	if idx < 0 {
		return
	}

	ac.content = ac.content[:0]
	arrayContainerClassPools[idx].Put(ac)
}

func replaceArrayContainerStorage(ac, donor *arrayContainer) {
	oldContent := ac.content
	ac.content = donor.content
	donor.content = oldContent
	releaseArrayContainer(donor)
}

func releaseContainer(c container) {
	switch x := c.(type) {
	case *runContainer16:
		releaseRunContainer16(x)
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
		it := v.(*intIterator)
		it.Initialize(a)
		return it
	}
	it := &intIterator{}
	it.Initialize(a)
	return it
}

func releaseIntIterator(it *intIterator) {
	if it == nil {
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
}

func acquireManyIntIterator(a *Bitmap) *manyIntIterator {
	if v := manyIntIteratorPool.Get(); v != nil {
		it := v.(*manyIntIterator)
		it.Initialize(a)
		return it
	}
	it := &manyIntIterator{}
	it.Initialize(a)
	return it
}

func releaseManyIntIterator(it *manyIntIterator) {
	if it == nil {
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
