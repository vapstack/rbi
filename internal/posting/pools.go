package posting

import "sync"

var (
	containerIndexStoragePools [len(containerIndexPoolCapacities)]sync.Pool
	shortIteratorPool          sync.Pool
	bitmap32Pool               sync.Pool
	runContainerPool           sync.Pool
	containerArrayClassPools   [len(containerArrayPoolCapacities)]sync.Pool
	intIteratorPool            sync.Pool
	manyIntIteratorPool        sync.Pool
	bitmapContainerPool        sync.Pool
	bitmapShortIteratorPool    sync.Pool
	bitmapManyIteratorPool     sync.Pool
	runIteratorPool            sync.Pool
	smallPostingPool           sync.Pool
	midPostingPool             sync.Pool
	arrayIterPool              sync.Pool
	singletonIterPool          sync.Pool
	largePostingPool           sync.Pool
	largeIteratorPool          sync.Pool
	largeArrayStoragePools     [len(largeArrayPoolCapacities)]sync.Pool
)

func getContainerIndexStorageWithLen(l int) *containerIndexStorage {
	if l <= 0 {
		l = containerIndexPoolCapacities[0]
	}
	idx := containerIndexPoolIndex(l)
	if idx < 0 {
		panic("containerIndex size exceeds pooled capacity")
	}
	if v := containerIndexStoragePools[idx].Get(); v != nil {
		storage := v.(*containerIndexStorage)
		storage.keys = storage.keys[:l]
		storage.containers = storage.containers[:l]
		return storage
	}
	c := containerIndexPoolCapacities[idx]
	return &containerIndexStorage{
		keys:       make([]uint16, l, c),
		containers: make([]container16, l, c),
	}
}

func putContainerIndexStorage(storage *containerIndexStorage) {
	idx := containerIndexPoolIndex(cap(storage.keys))
	if idx < 0 {
		panic("containerIndex storage capacity exceeds pooled capacity")
	}
	storage.keys = storage.keys[:0]
	clear(storage.containers[:cap(storage.containers)])
	storage.containers = storage.containers[:0]
	containerIndexStoragePools[idx].Put(storage)
}

func getShortIterator(slice []uint16) *shortIterator {
	if v := shortIteratorPool.Get(); v != nil {
		it := v.(*shortIterator)
		it.slice = slice
		it.loc = 0
		return it
	}
	return &shortIterator{slice: slice}
}

func putShortIterator(it *shortIterator) {
	it.slice = nil
	shortIteratorPool.Put(it)
}

func getBitmap32() *bitmap32 {
	var rb *bitmap32
	if v := bitmap32Pool.Get(); v != nil {
		rb = v.(*bitmap32)
	} else {
		rb = new(bitmap32)
	}
	rb.refs.Store(1)
	return rb
}

func putBitmap32(rb *bitmap32) {
	rb.highlowcontainer.clear()
	bitmap32Pool.Put(rb)
}

func getRunContainer() *containerRun {
	var rc *containerRun
	if v := runContainerPool.Get(); v != nil {
		rc = v.(*containerRun)
	} else {
		rc = new(containerRun)
	}
	rc.refs.Store(1)
	return rc
}

func putRunContainer(rc *containerRun) {
	if cap(rc.iv) > maxPooledRunContainerCapacity {
		return
	}
	clear(rc.iv)
	rc.iv = rc.iv[:0]
	runContainerPool.Put(rc)
}

func getContainerArray() *containerArray {
	return getContainerArrayWithCap(containerArrayPoolCapacities[0])
}

func getContainerArrayWithCap(c int) *containerArray {
	if c <= 0 {
		c = containerArrayPoolCapacities[0]
	}
	idx := containerArrayPoolIndex(c)
	if idx < 0 {
		ac := &containerArray{
			content: make([]uint16, 0, c),
		}
		ac.refs.Store(1)
		return ac
	}
	var ac *containerArray
	if v := containerArrayClassPools[idx].Get(); v != nil {
		ac = v.(*containerArray)
	} else {
		ac = &containerArray{
			content: make([]uint16, 0, containerArrayPoolCapacities[idx]),
		}
	}
	ac.refs.Store(1)
	return ac
}

func getContainerArrayWithLen(l int) *containerArray {
	if l <= 0 {
		return getContainerArray()
	}
	ac := getContainerArrayWithCap(l)
	ac.content = ac.content[:l]
	return ac
}

func getContainerArrayFromSlice(src []uint16) *containerArray {
	if len(src) == 0 {
		return getContainerArray()
	}
	ac := getContainerArrayWithLen(len(src))
	copy(ac.content, src)
	return ac
}

func putContainerArray(ac *containerArray) {
	idx := containerArrayPoolPutIndex(cap(ac.content))
	if idx < 0 {
		return
	}
	clear(ac.content)
	ac.content = ac.content[:0]
	containerArrayClassPools[idx].Put(ac)
}

func getIntIterator(rb *bitmap32) *intIterator {
	var it *intIterator
	if v := intIteratorPool.Get(); v != nil {
		it = v.(*intIterator)
	} else {
		it = new(intIterator)
	}
	it.initialize(rb)
	return it
}

func putIntIterator(it *intIterator) {
	*it = intIterator{}
	intIteratorPool.Put(it)
}

func getManyIntIterator(rb *bitmap32) *manyIntIterator {
	var it *manyIntIterator
	if v := manyIntIteratorPool.Get(); v != nil {
		it = v.(*manyIntIterator)
	} else {
		it = new(manyIntIterator)
	}
	it.initialize(rb)
	return it
}

func putManyIntIterator(it *manyIntIterator) {
	*it = manyIntIterator{}
	manyIntIteratorPool.Put(it)
}

func getContainerBitmap() *containerBitmap {
	var bc *containerBitmap
	if v := bitmapContainerPool.Get(); v != nil {
		bc = v.(*containerBitmap)
	} else {
		bc = &containerBitmap{
			bitmap: make([]uint64, bitmapContainerWords),
		}
	}
	bc.refs.Store(1)
	return bc
}

func putContainerBitmap(bc *containerBitmap) {
	if len(bc.bitmap) != bitmapContainerWords {
		return
	}
	bc.cardinality = 0
	clear(bc.bitmap)
	bitmapContainerPool.Put(bc)
}

func getBitmapContainerShortIterator(bc *containerBitmap) *bitmapContainerShortIterator {
	if v := bitmapShortIteratorPool.Get(); v != nil {
		it := v.(*bitmapContainerShortIterator)
		it.ptr = bc
		it.i = bc.nextSetBit(0)
		return it
	}
	return &bitmapContainerShortIterator{ptr: bc, i: bc.nextSetBit(0)}
}

func putBitmapContainerShortIterator(it *bitmapContainerShortIterator) {
	it.ptr = nil
	bitmapShortIteratorPool.Put(it)
}

func getBitmapContainerManyIterator(bc *containerBitmap) *bitmapContainerManyIterator {
	if v := bitmapManyIteratorPool.Get(); v != nil {
		it := v.(*bitmapContainerManyIterator)
		it.ptr = bc
		it.base = 0
		it.bitset = bc.bitmap[0]
		return it
	}
	return &bitmapContainerManyIterator{ptr: bc, bitset: bc.bitmap[0]}
}

func putBitmapContainerManyIterator(it *bitmapContainerManyIterator) {
	it.ptr = nil
	bitmapManyIteratorPool.Put(it)
}

func getRunIterator16(rc *containerRun) *runIterator16 {
	if v := runIteratorPool.Get(); v != nil {
		it := v.(*runIterator16)
		it.rc = rc
		it.curIndex = 0
		it.curPosInIndex = 0
		return it
	}
	return &runIterator16{rc: rc}
}

func putRunIterator16(it *runIterator16) {
	it.rc = nil
	runIteratorPool.Put(it)
}

func getSmallPosting() *smallPosting {
	if v := smallPostingPool.Get(); v != nil {
		return v.(*smallPosting)
	}
	return new(smallPosting)
}

func putSmallPosting(sp *smallPosting) {
	sp.n = 0
	smallPostingPool.Put(sp)
}

func getMidPosting() *midPosting {
	if v := midPostingPool.Get(); v != nil {
		return v.(*midPosting)
	}
	return new(midPosting)
}

func putMidPosting(mp *midPosting) {
	mp.n = 0
	midPostingPool.Put(mp)
}

func getArrayIter(ids []uint64) *arrayIter {
	if v := arrayIterPool.Get(); v != nil {
		it := v.(*arrayIter)
		it.ids = ids
		it.i = 0
		return it
	}
	return &arrayIter{ids: ids}
}

func putArrayIter(it *arrayIter) {
	it.ids = nil
	arrayIterPool.Put(it)
}

func getSingletonIter(v uint64) Iterator {
	var it *singletonIter
	if pv := singletonIterPool.Get(); pv != nil {
		it = pv.(*singletonIter)
	} else {
		it = new(singletonIter)
	}
	it.v = v
	it.has = true
	return it
}

func putSingletonIter(it *singletonIter) {
	singletonIterPool.Put(it)
}

func getLargePosting() *largePosting {
	if v := largePostingPool.Get(); v != nil {
		return v.(*largePosting)
	}
	return new(largePosting)
}

func putLargePosting(lp *largePosting) {
	lp.clear()
	largePostingPool.Put(lp)
}

func getLargeIterator(lp *largePosting) *largeIterator {
	var it *largeIterator
	if v := largeIteratorPool.Get(); v != nil {
		it = v.(*largeIterator)
	} else {
		it = new(largeIterator)
	}
	it.initialize(lp)
	return it
}

func putLargeIterator(it *largeIterator) {
	if it.iter != nil {
		it.iter.release()
	}
	it.iter = nil
	it.highlowcontainer.clear()
	it.highlowcontainer.releaseBacking()
	largeIteratorPool.Put(it)
}

func getLargeArrayStorageWithLen(l int) *largeArrayStorage {
	if l <= 0 {
		l = largeArrayPoolCapacities[0]
	}
	idx := largeArrayPoolIndex(l)
	if idx < 0 {
		return &largeArrayStorage{
			keys:       make([]uint32, l),
			containers: make([]*bitmap32, l),
		}
	}
	if v := largeArrayStoragePools[idx].Get(); v != nil {
		storage := v.(*largeArrayStorage)
		storage.keys = storage.keys[:l]
		storage.containers = storage.containers[:l]
		return storage
	}
	c := largeArrayPoolCapacities[idx]
	return &largeArrayStorage{
		keys:       make([]uint32, l, c),
		containers: make([]*bitmap32, l, c),
	}
}

func putLargeArrayStorage(storage *largeArrayStorage) {
	idx := largeArrayPoolIndex(cap(storage.keys))
	if idx < 0 {
		return
	}
	storage.keys = storage.keys[:0]
	clear(storage.containers[:cap(storage.containers)])
	storage.containers = storage.containers[:0]
	largeArrayStoragePools[idx].Put(storage)
}
