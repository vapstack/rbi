package qcache

import (
	"sync"
	"sync/atomic"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/schema"
)

const (
	// full-span cache is a tiny linearly scanned LRU per numeric bucket entry;
	// four slots keep hit reuse without making every lookup scan a larger table.
	numericRangeFullSpanCacheMaxEntries = 4

	// requests for smaller buckets are normalized to 16 keys because tiny
	// buckets create too many spans to amortize materialization.
	numericRangeBucketMinSize = 16

	// full-span keys pack start and end bucket numbers as two uint32 values.
	numericRangeFullSpanCacheCoordBits = 32
)

type NumericRangeBucketCache struct {
	mu            sync.Mutex
	slots         []numericRangeBucketCacheSlot
	fieldIndex    map[string]*NumericRangeBucketEntry
	fieldIndexLen int
	count         int
	maxCard       uint64
}

type NumericRangeBucketIndex struct {
	bucketSize int
	keyCount   int
}

type NumericRangeBucketEntry struct {
	refs          atomic.Int32
	storage       indexdata.FieldStorage
	idx           NumericRangeBucketIndex
	maxCard       uint64
	mu            sync.Mutex
	fullSpanClock uint64
	fullSpanCache [numericRangeFullSpanCacheMaxEntries]numericRangeFullSpanCacheSlot
	retired       []posting.List
}

type numericRangeBucketCacheSlot struct {
	field string
	entry *NumericRangeBucketEntry
}

type numericRangeFullSpanCacheSlot struct {
	key   uint64
	ids   posting.List
	stamp uint64
	used  bool
}

func GetNumericRangeBucketCache(fieldCount int, maxCard uint64) *NumericRangeBucketCache {
	c := numericRangeBucketCachePool.Get()
	c.Init(fieldCount, maxCard)
	return c
}

func ReleaseNumericRangeBucketCache(c *NumericRangeBucketCache) {
	numericRangeBucketCachePool.Put(c)
}

func GetNumericRangeBucketEntry(storage indexdata.FieldStorage, idx NumericRangeBucketIndex, maxCard uint64) *NumericRangeBucketEntry {
	entry := numericRangeBucketEntryPool.Get()
	entry.refs.Store(1)
	entry.storage = storage
	entry.idx = idx
	entry.maxCard = maxCard
	return entry
}

func (c *NumericRangeBucketCache) Init(fieldCount int, maxCard uint64) {
	c.maxCard = maxCard
	c.count = 0
	c.fieldIndexLen = 0
	if cap(c.slots) < fieldCount {
		c.slots = make([]numericRangeBucketCacheSlot, fieldCount)
		return
	}
	c.slots = c.slots[:fieldCount]
}

func (c *NumericRangeBucketCache) ClearEntries() {
	if len(c.slots) == 0 {
		if c.fieldIndex != nil {
			numericRangeBucketFieldIndexPool.Put(c.fieldIndex)
			c.fieldIndex = nil
		}
		c.fieldIndexLen = 0
		c.count = 0
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := range c.slots {
		slot := c.slots[i]
		if slot.entry != nil {
			slot.entry.Release()
		}
		c.slots[i] = numericRangeBucketCacheSlot{}
	}
	if c.fieldIndex != nil {
		numericRangeBucketFieldIndexPool.Put(c.fieldIndex)
		c.fieldIndex = nil
	}
	c.fieldIndexLen = 0
	c.count = 0
}

func (c *NumericRangeBucketCache) LoadSlot(field string, ordinal int) (*NumericRangeBucketEntry, bool) {
	if ordinal < 0 || ordinal >= len(c.slots) {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	slot := c.slots[ordinal]
	if slot.entry == nil || slot.field != field {
		return nil, false
	}
	return slot.entry, true
}

func (c *NumericRangeBucketCache) StoreSlot(field string, ordinal int, entry *NumericRangeBucketEntry) {
	if ordinal < 0 || ordinal >= len(c.slots) {
		return
	}

	c.mu.Lock()
	slot := c.slots[ordinal]
	if slot.entry == nil {
		if entry != nil {
			c.count++
		}
	} else if entry == nil {
		c.count--
	}
	if c.fieldIndex != nil {
		if slot.field != "" {
			delete(c.fieldIndex, slot.field)
		}
		if field != "" && entry != nil {
			c.fieldIndex[field] = entry
		}
	}
	slot.field = field
	slot.entry = entry
	c.slots[ordinal] = slot
	c.mu.Unlock()
}

func (c *NumericRangeBucketCache) LoadField(field string) (*NumericRangeBucketEntry, bool) {
	if len(c.slots) == 0 || field == "" {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.fieldIndex != nil && c.fieldIndexLen == len(c.slots) {
		entry, ok := c.fieldIndex[field]
		return entry, ok
	}
	if c.fieldIndex == nil {
		c.fieldIndex = numericRangeBucketFieldIndexPool.Get(len(c.slots))
	} else {
		clear(c.fieldIndex)
	}
	for i := range c.slots {
		slot := c.slots[i]
		if slot.field != "" && slot.entry != nil {
			c.fieldIndex[slot.field] = slot.entry
		}
	}
	c.fieldIndexLen = len(c.slots)
	entry, ok := c.fieldIndex[field]
	return entry, ok
}

func (c *NumericRangeBucketCache) EntryCount() int {
	if len(c.slots) == 0 {
		return 0
	}

	c.mu.Lock()
	count := c.count
	c.mu.Unlock()

	return count
}

func (c *NumericRangeBucketCache) MaxCardinality() uint64 {
	return c.maxCard
}

func (c *NumericRangeBucketCache) InheritFrom(prev *NumericRangeBucketCache, nextIndex []indexdata.FieldStorage, fields schema.IndexedFieldMap) {
	if len(c.slots) == 0 || nextIndex == nil || len(fields) == 0 {
		return
	}
	prev.mu.Lock()
	c.mu.Lock()

	for i := range prev.slots {
		slot := prev.slots[i]
		if slot.field == "" || slot.entry == nil || slot.entry.storage.KeyCount() == 0 {
			continue
		}
		acc, ok := fields[slot.field]
		if !ok || acc.Ordinal >= len(nextIndex) || acc.Ordinal >= len(c.slots) {
			continue
		}
		if nextIndex[acc.Ordinal] != slot.entry.storage {
			continue
		}
		slot.entry.Retain()
		prevSlot := c.slots[acc.Ordinal]
		if prevSlot.entry == nil {
			c.count++
		}
		if c.fieldIndex != nil {
			if prevSlot.field != "" {
				delete(c.fieldIndex, prevSlot.field)
			}
			c.fieldIndex[slot.field] = slot.entry
		}
		c.slots[acc.Ordinal] = numericRangeBucketCacheSlot{
			field: slot.field,
			entry: slot.entry,
		}
	}
	c.mu.Unlock()
	prev.mu.Unlock()
}

func (c *NumericRangeBucketCache) release() {
	c.ClearEntries()
	c.slots = c.slots[:0]
	c.maxCard = 0
}

func (e *NumericRangeBucketEntry) Retain() {
	e.refs.Add(1)
}

func (e *NumericRangeBucketEntry) Release() {
	if e.refs.Add(-1) == 0 {
		numericRangeBucketEntryPool.Put(e)
	}
}

func (e *NumericRangeBucketEntry) Storage() indexdata.FieldStorage {
	return e.storage
}

func (e *NumericRangeBucketEntry) Index() NumericRangeBucketIndex {
	return e.idx
}

func (e *NumericRangeBucketEntry) FullSpanEntryCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()

	count := 0
	for i := range e.fullSpanCache {
		if e.fullSpanCache[i].used {
			count++
		}
	}
	return count
}

func (e *NumericRangeBucketEntry) releaseFullSpanCache() {
	e.mu.Lock()
	defer e.mu.Unlock()

	for i := range e.fullSpanCache {
		if e.fullSpanCache[i].used {
			e.fullSpanCache[i].ids.Release()
		}
		e.fullSpanCache[i] = numericRangeFullSpanCacheSlot{}
	}
	if e.retired != nil {
		posting.ReleaseAll(e.retired)
		posting.ReleaseSlice(e.retired)
		e.retired = nil
	}
	e.fullSpanClock = 0
}

func (e *NumericRangeBucketEntry) retirePosting(ids posting.List) {
	if e.retired == nil {
		e.retired = posting.GetSlice(1)
	}
	e.retired = append(e.retired, ids)
}

func (e *NumericRangeBucketEntry) LoadFullSpan(start, end int) (posting.List, bool) {
	key := numericRangeFullSpanCacheKey(start, end)

	e.mu.Lock()
	defer e.mu.Unlock()

	for i := range e.fullSpanCache {
		slot := &e.fullSpanCache[i]
		if !slot.used || slot.key != key {
			continue
		}
		e.fullSpanClock++
		slot.stamp = e.fullSpanClock
		return slot.ids.Borrow(), true
	}
	return posting.List{}, false
}

func (e *NumericRangeBucketEntry) LoadExtendedFullSpan(start, end int) (posting.List, int, int, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	bestIdx := -1
	bestBuckets := 0
	bestStart := 0
	bestEnd := 0

	for i := range e.fullSpanCache {
		slot := &e.fullSpanCache[i]
		if !slot.used {
			continue
		}
		slotStart, slotEnd := numericRangeFullSpanCacheBounds(slot.key)
		switch {

		case slotStart == start && slotEnd < end:
			buckets := slotEnd - slotStart + 1
			if bestIdx < 0 || buckets > bestBuckets {
				bestIdx = i
				bestBuckets = buckets
				bestStart = slotStart
				bestEnd = slotEnd
			}

		case slotEnd == end && slotStart > start:
			buckets := slotEnd - slotStart + 1
			if bestIdx < 0 || buckets > bestBuckets {
				bestIdx = i
				bestBuckets = buckets
				bestStart = slotStart
				bestEnd = slotEnd
			}
		}
	}
	if bestIdx < 0 {
		return posting.List{}, 0, 0, false
	}
	e.fullSpanClock++
	e.fullSpanCache[bestIdx].stamp = e.fullSpanClock
	return e.fullSpanCache[bestIdx].ids.Borrow(), bestStart, bestEnd, true
}

func (e *NumericRangeBucketEntry) TryStoreFullSpan(start, end int, ids posting.List) (posting.List, bool) {
	if ids.IsEmpty() {
		return ids, false
	}
	key := numericRangeFullSpanCacheKey(start, end)
	if e.maxCard > 0 && ids.Cardinality() > e.maxCard {
		return ids, false
	}

	e.mu.Lock()
	for i := range e.fullSpanCache {
		slot := &e.fullSpanCache[i]
		if !slot.used || slot.key != key {
			continue
		}
		e.fullSpanClock++
		slot.stamp = e.fullSpanClock
		cached := slot.ids
		e.mu.Unlock()
		ids.Release()
		return cached.Borrow(), true
	}
	e.mu.Unlock()

	stored := ids
	if stored.IsBorrowed() {
		stored = stored.Clone()
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	for i := range e.fullSpanCache {
		slot := &e.fullSpanCache[i]
		if !slot.used || slot.key != key {
			continue
		}
		e.fullSpanClock++
		slot.stamp = e.fullSpanClock
		cached := slot.ids
		if !stored.SharesPayload(ids) {
			stored.Release()
		}
		ids.Release()
		return cached.Borrow(), true
	}

	slotIdx := -1
	oldestStamp := ^uint64(0)

	for i := range e.fullSpanCache {
		slot := &e.fullSpanCache[i]
		if !slot.used {
			slotIdx = i
			break
		}
		if slot.stamp <= oldestStamp {
			oldestStamp = slot.stamp
			slotIdx = i
		}
	}

	e.fullSpanClock++
	if replaced := e.fullSpanCache[slotIdx]; replaced.used {
		e.retirePosting(replaced.ids)
	}

	e.fullSpanCache[slotIdx] = numericRangeFullSpanCacheSlot{
		key:   key,
		ids:   stored,
		stamp: e.fullSpanClock,
		used:  true,
	}

	return stored.Borrow(), true
}

func (idx NumericRangeBucketIndex) FullBucketSpan(br indexdata.OverlayRange) (start, end int, ok bool) {
	if idx.bucketSize <= 0 || idx.keyCount <= 0 {
		return 0, 0, false
	}
	if br.BaseStart < 0 || br.BaseStart >= br.BaseEnd {
		return 0, 0, false
	}

	startBucket := br.BaseStart / idx.bucketSize
	endBucket := (br.BaseEnd - 1) / idx.bucketSize
	lastBucket := idx.BucketCount() - 1
	if endBucket > lastBucket {
		endBucket = lastBucket
	}
	if startBucket > endBucket {
		return 0, 0, false
	}

	start = startBucket
	if idx.BucketStart(start) < br.BaseStart {
		start++
	}
	end = endBucket
	if idx.BucketEnd(end) > br.BaseEnd {
		end--
	}
	if start > end {
		return 0, 0, false
	}
	return start, end, true
}

func (idx NumericRangeBucketIndex) BucketCount() int {
	if idx.bucketSize <= 0 || idx.keyCount <= 0 {
		return 0
	}
	return (idx.keyCount + idx.bucketSize - 1) / idx.bucketSize
}

func (idx NumericRangeBucketIndex) BucketStart(bucket int) int {
	return bucket * idx.bucketSize
}

func (idx NumericRangeBucketIndex) BucketEnd(bucket int) int {
	end := (bucket + 1) * idx.bucketSize
	if end > idx.keyCount {
		return idx.keyCount
	}
	return end
}

func (idx NumericRangeBucketIndex) KeyCount() int {
	return idx.keyCount
}

func (idx NumericRangeBucketIndex) BucketSize() int {
	return idx.bucketSize
}

func BuildNumericRangeBucketIndex(ov indexdata.FieldOverlay, bucketSize, minFieldKeys int) (NumericRangeBucketIndex, bool) {
	if !ov.HasData() {
		return NumericRangeBucketIndex{}, false
	}
	if bucketSize <= 0 || minFieldKeys <= 0 {
		return NumericRangeBucketIndex{}, false
	}
	if ov.KeyCount() < minFieldKeys {
		return NumericRangeBucketIndex{}, false
	}
	if bucketSize < numericRangeBucketMinSize {
		bucketSize = numericRangeBucketMinSize
	}

	keyCount := ov.KeyCount()
	return NumericRangeBucketIndex{
		bucketSize: bucketSize,
		keyCount:   keyCount,
	}, true
}

func numericRangeFullSpanCacheKey(start, end int) uint64 {
	return uint64(uint32(start))<<numericRangeFullSpanCacheCoordBits | uint64(uint32(end))
}

func numericRangeFullSpanCacheBounds(key uint64) (int, int) {
	return int(uint32(key >> numericRangeFullSpanCacheCoordBits)), int(uint32(key))
}
