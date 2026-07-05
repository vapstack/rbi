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
	retiredDirty  atomic.Bool
	retiredOwner  *atomic.Bool
	retireEpoch   *atomic.Uint64
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
	retired       []numericRangeRetiredPosting
	retiredDirty  atomic.Bool
	rootOwner     atomic.Pointer[atomic.Bool]
	retireEpoch   atomic.Pointer[atomic.Uint64]
}

// NumericRangeBucketRetired carries detached retired full-span postings past
// the caller's reader barrier so payload release can run outside cache locks.
type NumericRangeBucketRetired struct {
	chunks [][]numericRangeRetiredPosting
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

type numericRangeRetiredPosting struct {
	ids   posting.List
	epoch uint64
}

func GetNumericRangeBucketCache(fieldCount int, maxCard uint64) *NumericRangeBucketCache {
	return GetNumericRangeBucketCacheWithRetireContext(fieldCount, maxCard, nil, nil)
}

func GetNumericRangeBucketCacheWithRetireContext(fieldCount int, maxCard uint64, owner *atomic.Bool, epoch *atomic.Uint64) *NumericRangeBucketCache {
	c := numericRangeBucketCachePool.Get()
	c.InitWithRetireContext(fieldCount, maxCard, owner, epoch)
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
	c.InitWithRetireContext(fieldCount, maxCard, nil, nil)
}

func (c *NumericRangeBucketCache) InitWithRetireContext(fieldCount int, maxCard uint64, owner *atomic.Bool, epoch *atomic.Uint64) {
	c.mu.Lock()
	c.maxCard = maxCard
	c.count = 0
	c.fieldIndexLen = 0
	c.retiredDirty.Store(false)
	c.retiredOwner = owner
	c.retireEpoch = epoch
	for i := range c.slots {
		slot := c.slots[i]
		if slot.entry != nil {
			slot.entry.Release()
		}
		c.slots[i] = numericRangeBucketCacheSlot{}
	}
	if c.fieldIndex != nil {
		clear(c.fieldIndex)
	}
	if fieldCount == 0 {
		c.slots = c.slots[:0]
		c.mu.Unlock()
		return
	}
	if cap(c.slots) < fieldCount {
		c.slots = make([]numericRangeBucketCacheSlot, fieldCount)
		c.mu.Unlock()
		return
	}
	c.slots = c.slots[:fieldCount]
	c.mu.Unlock()
}

func (c *NumericRangeBucketCache) ClearEntries() {
	if len(c.slots) == 0 {
		if c.fieldIndex != nil {
			numericRangeBucketFieldIndexPool.Put(c.fieldIndex)
			c.fieldIndex = nil
		}
		c.fieldIndexLen = 0
		c.count = 0
		c.retiredDirty.Store(false)
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
	c.retiredDirty.Store(false)
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
	replaced := slot.entry
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
	if entry != nil {
		c.bindEntryOwner(entry)
	}
	c.slots[ordinal] = slot
	c.mu.Unlock()
	if replaced != nil && replaced != entry {
		replaced.Release()
	}
}

func (c *NumericRangeBucketCache) LoadOrStoreSlot(field string, ordinal int, entry *NumericRangeBucketEntry) *NumericRangeBucketEntry {
	if ordinal < 0 || ordinal >= len(c.slots) {
		return entry
	}

	c.mu.Lock()
	slot := c.slots[ordinal]
	if slot.entry != nil && slot.field == field && slot.entry.storage == entry.storage {
		c.mu.Unlock()
		entry.Release()
		return slot.entry
	}
	replaced := slot.entry
	if slot.entry == nil {
		c.count++
	}
	if c.fieldIndex != nil {
		if slot.field != "" {
			delete(c.fieldIndex, slot.field)
		}
		if field != "" {
			c.fieldIndex[field] = entry
		}
	}
	slot.field = field
	slot.entry = entry
	c.bindEntryOwner(entry)
	c.slots[ordinal] = slot
	c.mu.Unlock()
	if replaced != nil && replaced != entry {
		replaced.Release()
	}
	return entry
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
		c.fieldIndex = numericRangeBucketFieldIndexPool.Get()
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

func (c *NumericRangeBucketCache) DrainRetired() {
	c.TakeRetired().Release()
}

func (c *NumericRangeBucketCache) TakeRetired() NumericRangeBucketRetired {
	return c.TakeRetiredBefore(^uint64(0))
}

func (c *NumericRangeBucketCache) TakeRetiredBefore(safeEpoch uint64) NumericRangeBucketRetired {
	if len(c.slots) == 0 {
		return NumericRangeBucketRetired{}
	}

	c.mu.Lock()
	var chunks [][]numericRangeRetiredPosting
	dirty := false
	for i := range c.slots {
		entry := c.slots[i].entry
		if entry == nil {
			continue
		}
		if safeEpoch == ^uint64(0) && entry.refs.Load() != 1 {
			// A drain without a reader barrier is only safe for entries owned
			// exclusively by this cache; inherited entries may still be visible.
			if entry.hasRetired() {
				dirty = true
			}
			continue
		}
		retired := entry.takeRetiredBefore(safeEpoch)
		if retired == nil {
			if entry.hasRetired() {
				dirty = true
			}
			continue
		}
		if chunks == nil {
			chunks = numericRangeBucketRetiredPool.Get(1)
		}
		chunks = append(chunks, retired)
		if entry.hasRetired() {
			dirty = true
		}
	}
	c.retiredDirty.Store(dirty)
	c.mu.Unlock()

	return NumericRangeBucketRetired{chunks: chunks}
}

func (c *NumericRangeBucketCache) RetiredDirty() bool {
	if c.retiredDirty.Load() {
		return true
	}
	if len(c.slots) == 0 {
		return false
	}

	c.mu.Lock()
	dirty := false
	for i := range c.slots {
		entry := c.slots[i].entry
		if entry != nil && entry.retiredDirty.Load() {
			dirty = true
			break
		}
	}
	c.retiredDirty.Store(dirty)
	owner := c.retiredOwner
	c.mu.Unlock()
	if dirty && owner != nil {
		owner.CompareAndSwap(false, true)
	}
	return dirty
}

func (c *NumericRangeBucketCache) bindEntryOwner(entry *NumericRangeBucketEntry) {
	// Shared entries keep the first root dirty/epoch pointers; CAS initializes
	// standalone entries without rewriting inherited ones.
	if c.retiredOwner != nil {
		entry.rootOwner.CompareAndSwap(nil, c.retiredOwner)
	}
	if c.retireEpoch != nil {
		entry.retireEpoch.CompareAndSwap(nil, c.retireEpoch)
	}
}

func (c *NumericRangeBucketCache) InheritFrom(prev *NumericRangeBucketCache, nextIndex []indexdata.FieldStorage, fields schema.IndexedFieldMap) {
	if len(c.slots) == 0 || nextIndex == nil || len(fields) == 0 {
		return
	}
	// Reuse full-span caches only when the field storage object is unchanged;
	// then bucket boundaries and cached postings still describe the same index.
	var replacedFirst *NumericRangeBucketEntry
	var replacedTail []*NumericRangeBucketEntry
	prev.mu.Lock()
	c.mu.Lock()

	inheritedDirty := false
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
		prevSlot := c.slots[acc.Ordinal]
		if prevSlot.entry == slot.entry {
			c.bindEntryOwner(slot.entry)
			if slot.entry.retiredDirty.Load() {
				inheritedDirty = true
			}
			if prevSlot.field != slot.field {
				if c.fieldIndex != nil {
					if prevSlot.field != "" {
						delete(c.fieldIndex, prevSlot.field)
					}
					c.fieldIndex[slot.field] = slot.entry
				}
				prevSlot.field = slot.field
				c.slots[acc.Ordinal] = prevSlot
			}
			continue
		}
		slot.entry.Retain()
		c.bindEntryOwner(slot.entry)
		if slot.entry.retiredDirty.Load() {
			inheritedDirty = true
		}
		if prevSlot.entry == nil {
			c.count++
		} else {
			if replacedFirst == nil {
				replacedFirst = prevSlot.entry
			} else {
				replacedTail = append(replacedTail, prevSlot.entry)
			}
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
	owner := c.retiredOwner
	if inheritedDirty {
		c.retiredDirty.Store(true)
	}
	c.mu.Unlock()
	prev.mu.Unlock()
	if inheritedDirty && owner != nil {
		owner.CompareAndSwap(false, true)
	}
	if replacedFirst != nil {
		replacedFirst.Release()
		for i := range replacedTail {
			replacedTail[i].Release()
		}
	}
}

func (c *NumericRangeBucketCache) release() {
	c.ClearEntries()
	c.slots = c.slots[:0]
	c.maxCard = 0
	c.retiredOwner = nil
	c.retireEpoch = nil
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
		releaseNumericRangeRetiredPostings(e.retired)
		e.retired = nil
	}
	e.fullSpanClock = 0
	e.retiredDirty.Store(false)
	e.rootOwner.Store(nil)
	e.retireEpoch.Store(nil)
}

func (e *NumericRangeBucketEntry) retirePosting(ids posting.List) {
	if e.retired == nil {
		e.retired = numericRangeRetiredPostingPool.Get(1)
	}
	epoch := uint64(0)
	if source := e.retireEpoch.Load(); source != nil {
		epoch = source.Load()
	}
	e.retired = append(e.retired, numericRangeRetiredPosting{ids: ids, epoch: epoch})
	e.retiredDirty.CompareAndSwap(false, true)
	if owner := e.rootOwner.Load(); owner != nil {
		owner.CompareAndSwap(false, true)
	}
}

func (e *NumericRangeBucketEntry) hasRetired() bool {
	return e.retiredDirty.Load()
}

func (e *NumericRangeBucketEntry) takeRetiredBefore(safeEpoch uint64) []numericRangeRetiredPosting {
	e.mu.Lock()
	retired := e.retired
	if retired != nil {
		eligible := 0
		for i := range retired {
			if retired[i].epoch < safeEpoch {
				eligible++
			}
		}
		switch eligible {
		case 0:
			retired = nil
		case len(retired):
			e.retired = nil
			e.retiredDirty.Store(false)
		default:
			detached := numericRangeRetiredPostingPool.Get(eligible)
			kept := retired[:0]
			for i := range retired {
				if retired[i].epoch < safeEpoch {
					detached = append(detached, retired[i])
				} else {
					kept = append(kept, retired[i])
				}
			}
			e.retired = kept
			retired = detached
		}
	}
	e.mu.Unlock()
	return retired
}

func (r NumericRangeBucketRetired) Release() {
	if r.chunks == nil {
		return
	}
	for i := range r.chunks {
		releaseNumericRangeRetiredPostings(r.chunks[i])
	}
	numericRangeBucketRetiredPool.Put(r.chunks)
}

func (r NumericRangeBucketRetired) IsEmpty() bool {
	return r.chunks == nil
}

func releaseNumericRangeRetiredPostings(retired []numericRangeRetiredPosting) {
	for i := range retired {
		retired[i].ids.Release()
	}
	numericRangeRetiredPostingPool.Put(retired)
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

	free := false
	e.mu.Lock()
	for i := range e.fullSpanCache {
		slot := &e.fullSpanCache[i]
		if !slot.used {
			free = true
			continue
		}
		if slot.key != key {
			continue
		}
		e.fullSpanClock++
		slot.stamp = e.fullSpanClock
		cached := slot.ids
		e.mu.Unlock()
		ids.Release()
		return cached.Borrow(), true
	}
	if !free && len(e.retired) >= numericRangeFullSpanCacheMaxEntries {
		e.mu.Unlock()
		return ids, false
	}
	e.mu.Unlock()

	// Clone outside the entry lock; another goroutine may insert the same span,
	// so the key is checked again after reacquiring the lock.
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
		if len(e.retired) >= numericRangeFullSpanCacheMaxEntries {
			if !stored.SharesPayload(ids) {
				stored.Release()
			}
			return ids, false
		}
		// Retire instead of releasing: readers may hold borrowed full-span
		// postings until the root runtime epoch advances past this eviction.
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

func (idx NumericRangeBucketIndex) FullBucketSpan(br indexdata.FieldIndexRange) (start, end int, ok bool) {
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

func BuildNumericRangeBucketIndex(ov indexdata.FieldIndexView, bucketSize, minFieldKeys int) (NumericRangeBucketIndex, bool) {
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
