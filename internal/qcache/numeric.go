package qcache

import (
	"sync"
	"sync/atomic"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/mathutil"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/schema"
)

const (
	// full-span cache is a tiny linearly scanned LRU per numeric bucket entry;
	// four slots keep extension reuse bounded without making every lookup scan
	// the full cache.
	numericRangeFullSpanRecentMaxEntries = 4

	numericRangeSpanCacheDefaultMaxEntries   = 128
	numericRangeSpanCacheDefaultMaxEntrySize = 16 << 20

	numericRangeExactCacheDefaultMaxEntries   = 32
	numericRangeExactCacheDefaultMaxEntrySize = 16 << 20

	numericRangeSpanCacheLinearMaxEntries = 8

	// requests for smaller buckets are normalized to 16 keys because tiny
	// buckets create too many spans to amortize materialization.
	numericRangeBucketMinSize = 16
)

type NumericRangeBucketCache struct {
	mu                sync.RWMutex
	slots             []numericRangeBucketCacheSlot
	fieldIndex        map[string]*NumericRangeBucketEntry
	fieldIndexLen     int
	count             int
	spanSlots         []numericRangeSpanCacheSlot
	spanIndex         map[numericRangeSpanKey]int
	spanObserved      numericRangeSpanObservedCache
	spanTooLarge      numericRangeSpanObservedCache
	spanCount         int
	spanLiveBytes     uint64
	spanRetired       []numericRangeSpanRetiredEntry
	spanRetiredCount  int
	spanRetiredBytes  uint64
	spanFreeHint      int
	spanEvictHand     int
	spanMaxEntries    int
	spanMaxEntrySize  uint64
	spanStats         NumericRangeSpanCacheStats
	exactSlots        []numericRangeExactCacheSlot
	exactIndex        map[numericRangeSpanKey]int
	exactObserved     numericRangeSpanObservedCache
	exactTooLarge     numericRangeSpanObservedCache
	exactCount        int
	exactLiveBytes    uint64
	exactRetired      []numericRangeExactRetiredEntry
	exactRetiredCount int
	exactRetiredBytes uint64
	exactFreeHint     int
	exactEvictHand    int
	exactMaxEntries   int
	exactMaxEntrySize uint64
	exactStats        NumericRangeExactResultCacheStats
	retiredDirty      atomic.Bool
	retiredOwner      *atomic.Bool
	retireEpoch       *atomic.Uint64
}

type NumericRangeBucketIndex struct {
	bucketSize int
	keyCount   int
}

type NumericRangeBucketEntry struct {
	refs           atomic.Int32
	storage        indexdata.FieldStorage
	idx            NumericRangeBucketIndex
	mu             sync.Mutex
	fullSpanClock  uint64
	fullSpanRecent [numericRangeFullSpanRecentMaxEntries]numericRangeFullSpanRecentSlot
}

// NumericRangeBucketRetired carries detached retired numeric postings past the
// caller's reader barrier so payload release can run outside cache locks.
type NumericRangeBucketRetired struct {
	spanEntries  []numericRangeSpanRetiredEntry
	exactEntries []numericRangeExactRetiredEntry
}

type numericRangeBucketCacheSlot struct {
	field string
	entry *NumericRangeBucketEntry
}

type numericRangeSpanKey struct {
	fieldOrdinal int
	start        int
	end          int
}

type numericRangeSpanCacheSlot struct {
	key   numericRangeSpanKey
	entry *numericRangeCachedPosting
	used  bool
}

type numericRangeExactCacheSlot struct {
	key   numericRangeSpanKey
	entry *numericRangeCachedPosting
	used  bool
}

type numericRangeCachedPosting struct {
	refs       atomic.Int32
	referenced atomic.Bool
	ids        posting.List
	size       uint64
}

type numericRangeSpanRetiredEntry struct {
	entry *numericRangeCachedPosting
	epoch uint64
}

type numericRangeExactRetiredEntry struct {
	entry *numericRangeCachedPosting
	epoch uint64
}

type numericRangeFullSpanRecentSlot struct {
	key   numericRangeSpanKey
	stamp uint64
	used  bool
}

type numericRangeSpanObservedCache struct {
	clock    uint64
	slots    []numericRangeSpanObservedSlot
	index    map[numericRangeSpanKey]int
	indexLen int
}

type numericRangeSpanObservedSlot struct {
	key   numericRangeSpanKey
	stamp uint64
	work  uint64
	used  bool
}

type NumericRangeSpanCacheStats struct {
	CurrentBytes           uint64
	RetiredBytes           uint64
	EntryCount             int
	MaxEntryBytes          uint64
	MaxEntries             int
	Stores                 uint64
	Evictions              uint64
	RejectedTooLarge       uint64
	RejectedCapacity       uint64
	RejectedRetiredBacklog uint64
}

type NumericRangeExactResultCacheStats struct {
	CurrentBytes             uint64
	RetiredBytes             uint64
	EntryCount               int
	MaxEntryBytes            uint64
	MaxEntries               int
	Stores                   uint64
	Evictions                uint64
	RejectedTooLarge         uint64
	RejectedCapacity         uint64
	RejectedRetiredBacklog   uint64
	RejectedFirstObservation uint64
}

func GetNumericRangeBucketCache(fieldCount int) *NumericRangeBucketCache {
	return GetNumericRangeBucketCacheWithRetireContext(
		fieldCount,
		NumericRangeSpanCacheMaxEntries(0),
		NumericRangeSpanCacheMaxEntryBytes(0),
		NumericRangeExactCacheMaxEntries(0),
		NumericRangeExactCacheMaxEntryBytes(0),
		nil,
		nil,
	)
}

func GetNumericRangeBucketCacheWithRetireContext(fieldCount int, spanMaxEntries int, spanMaxEntryBytes uint64, exactMaxEntries int, exactMaxEntryBytes uint64, owner *atomic.Bool, epoch *atomic.Uint64) *NumericRangeBucketCache {
	c := numericRangeBucketCachePool.Get()
	c.InitWithRetireContext(fieldCount, spanMaxEntries, spanMaxEntryBytes, exactMaxEntries, exactMaxEntryBytes, owner, epoch)
	return c
}

func ReleaseNumericRangeBucketCache(c *NumericRangeBucketCache) {
	numericRangeBucketCachePool.Put(c)
}

func GetNumericRangeBucketEntry(storage indexdata.FieldStorage, idx NumericRangeBucketIndex) *NumericRangeBucketEntry {
	entry := numericRangeBucketEntryPool.Get()
	entry.refs.Store(1)
	entry.storage = storage
	entry.idx = idx
	return entry
}

func NumericRangeSpanCacheMaxEntries(v int) int {
	if v < 0 {
		return 0
	}
	if v == 0 {
		return numericRangeSpanCacheDefaultMaxEntries
	}
	return v
}

func NumericRangeSpanCacheMaxEntryBytes(v int64) uint64 {
	if v < 0 {
		return 0
	}
	if v == 0 {
		return numericRangeSpanCacheDefaultMaxEntrySize
	}
	return uint64(v)
}

func NumericRangeExactCacheMaxEntries(v int) int {
	if v < 0 {
		return 0
	}
	if v == 0 {
		return numericRangeExactCacheDefaultMaxEntries
	}
	return v
}

func NumericRangeExactCacheMaxEntryBytes(v int64) uint64 {
	if v < 0 {
		return 0
	}
	if v == 0 {
		return numericRangeExactCacheDefaultMaxEntrySize
	}
	return uint64(v)
}

func (c *NumericRangeBucketCache) Init(fieldCount int) {
	c.InitWithRetireContext(
		fieldCount,
		NumericRangeSpanCacheMaxEntries(0),
		NumericRangeSpanCacheMaxEntryBytes(0),
		NumericRangeExactCacheMaxEntries(0),
		NumericRangeExactCacheMaxEntryBytes(0),
		nil,
		nil,
	)
}

func (c *NumericRangeBucketCache) InitWithRetireContext(fieldCount int, spanMaxEntries int, spanMaxEntryBytes uint64, exactMaxEntries int, exactMaxEntryBytes uint64, owner *atomic.Bool, epoch *atomic.Uint64) {
	c.mu.Lock()
	c.clearResultCachesLocked()
	c.count = 0
	c.fieldIndexLen = 0
	c.retiredDirty.Store(false)
	c.retiredOwner = owner
	c.retireEpoch = epoch
	c.spanMaxEntries = spanMaxEntries
	c.spanMaxEntrySize = spanMaxEntryBytes
	c.exactMaxEntries = exactMaxEntries
	c.exactMaxEntrySize = exactMaxEntryBytes
	if spanMaxEntries <= 0 || spanMaxEntryBytes == 0 {
		c.spanSlots = c.spanSlots[:0]
	} else if cap(c.spanSlots) < spanMaxEntries {
		c.spanSlots = make([]numericRangeSpanCacheSlot, spanMaxEntries)
	} else {
		c.spanSlots = c.spanSlots[:spanMaxEntries]
	}
	if exactMaxEntries <= 0 || exactMaxEntryBytes == 0 {
		c.exactSlots = c.exactSlots[:0]
	} else if cap(c.exactSlots) < exactMaxEntries {
		c.exactSlots = make([]numericRangeExactCacheSlot, exactMaxEntries)
	} else {
		c.exactSlots = c.exactSlots[:exactMaxEntries]
	}
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
	c.clearResultCachesLocked()
}

func (c *NumericRangeBucketCache) LoadSlot(field string, ordinal int) (*NumericRangeBucketEntry, bool) {
	if ordinal < 0 || ordinal >= len(c.slots) {
		return nil, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

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

	c.mu.RLock()
	count := c.count
	c.mu.RUnlock()

	return count
}

func (c *NumericRangeBucketCache) DrainRetired() {
	c.TakeRetired().Release()
}

func (c *NumericRangeBucketCache) TakeRetired() NumericRangeBucketRetired {
	return c.TakeRetiredBefore(^uint64(0))
}

func (c *NumericRangeBucketCache) TakeRetiredBefore(safeEpoch uint64) NumericRangeBucketRetired {
	c.mu.Lock()
	if c.spanRetiredCount == 0 && c.exactRetiredCount == 0 {
		c.mu.Unlock()
		return NumericRangeBucketRetired{}
	}

	spanRetired := c.spanRetired
	if spanRetired != nil {
		eligible := 0
		for i := range spanRetired {
			if spanRetired[i].epoch < safeEpoch {
				eligible++
			}
		}
		switch eligible {
		case 0:
			spanRetired = nil

		case len(spanRetired):
			for i := range spanRetired {
				c.spanRetiredBytes -= spanRetired[i].entry.size
			}
			c.spanRetired = nil
			c.spanRetiredCount = 0

		default:
			detached := numericRangeBucketRetiredPool.Get(eligible)
			kept := spanRetired[:0]
			retainedBytes := uint64(0)
			for i := range spanRetired {
				retiredEntry := spanRetired[i]
				if retiredEntry.epoch < safeEpoch {
					c.spanRetiredBytes -= retiredEntry.entry.size
					detached = append(detached, retiredEntry)
				} else {
					retainedBytes += retiredEntry.entry.size
					kept = append(kept, retiredEntry)
				}
			}
			c.spanRetired = kept
			c.spanRetiredCount = len(kept)
			c.spanRetiredBytes = retainedBytes
			spanRetired = detached
		}
	}

	exactRetired := c.exactRetired
	if exactRetired != nil {
		eligible := 0
		for i := range exactRetired {
			if exactRetired[i].epoch < safeEpoch {
				eligible++
			}
		}
		switch eligible {
		case 0:
			exactRetired = nil

		case len(exactRetired):
			for i := range exactRetired {
				c.exactRetiredBytes -= exactRetired[i].entry.size
			}
			c.exactRetired = nil
			c.exactRetiredCount = 0

		default:
			detached := numericRangeExactRetiredPool.Get(eligible)
			kept := exactRetired[:0]
			retainedBytes := uint64(0)
			for i := range exactRetired {
				retiredEntry := exactRetired[i]
				if retiredEntry.epoch < safeEpoch {
					c.exactRetiredBytes -= retiredEntry.entry.size
					detached = append(detached, retiredEntry)
				} else {
					retainedBytes += retiredEntry.entry.size
					kept = append(kept, retiredEntry)
				}
			}
			c.exactRetired = kept
			c.exactRetiredCount = len(kept)
			c.exactRetiredBytes = retainedBytes
			exactRetired = detached
		}
	}
	if c.spanRetiredCount == 0 && c.exactRetiredCount == 0 {
		c.retiredDirty.Store(false)
	}
	c.mu.Unlock()

	return NumericRangeBucketRetired{spanEntries: spanRetired, exactEntries: exactRetired}
}

func (c *NumericRangeBucketCache) RetiredDirty() bool {
	return c.retiredDirty.Load()
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
	c.inheritSpanEntriesLocked(prev, nextIndex, fields)
	c.inheritExactEntriesLocked(prev, nextIndex, fields)
	c.mu.Unlock()
	prev.mu.Unlock()
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
	if c.spanIndex != nil {
		numericRangeSpanCacheIndexPool.Put(c.spanIndex)
		c.spanIndex = nil
	}
	if c.exactIndex != nil {
		numericRangeSpanCacheIndexPool.Put(c.exactIndex)
		c.exactIndex = nil
	}
	c.spanObserved.Clear()
	c.spanTooLarge.Clear()
	c.exactObserved.Clear()
	c.exactTooLarge.Clear()
	c.spanSlots = c.spanSlots[:0]
	c.exactSlots = c.exactSlots[:0]
	c.spanMaxEntries = 0
	c.spanMaxEntrySize = 0
	c.exactMaxEntries = 0
	c.exactMaxEntrySize = 0
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

func (e *NumericRangeBucketEntry) resetRecentFullSpans() {
	e.mu.Lock()
	defer e.mu.Unlock()

	for i := range e.fullSpanRecent {
		e.fullSpanRecent[i] = numericRangeFullSpanRecentSlot{}
	}
	e.fullSpanClock = 0
}

func (c *NumericRangeBucketCache) FullSpanEntryCount() int {
	c.mu.RLock()
	count := c.spanCount
	c.mu.RUnlock()
	return count
}

func (c *NumericRangeBucketCache) NumericSpanStats() NumericRangeSpanCacheStats {
	c.mu.RLock()
	stats := c.spanStats
	stats.CurrentBytes = c.spanLiveBytes
	stats.RetiredBytes = c.spanRetiredBytes
	stats.EntryCount = c.spanCount
	stats.MaxEntries = c.spanMaxEntries
	stats.MaxEntryBytes = c.spanMaxEntrySize
	c.mu.RUnlock()
	return stats
}

func (c *NumericRangeBucketCache) LoadFullSpan(entry *NumericRangeBucketEntry, fieldOrdinal, start, end int) (posting.List, bool) {
	key := numericRangeSpanKey{fieldOrdinal: fieldOrdinal, start: start, end: end}
	ids, ok := c.loadSpanByKey(key)
	if ok {
		entry.touchRecentFullSpan(key)
	}
	return ids, ok
}

func (c *NumericRangeBucketCache) LoadExtendedFullSpan(entry *NumericRangeBucketEntry, fieldOrdinal, start, end int) (posting.List, int, int, bool) {
	var keys [numericRangeFullSpanRecentMaxEntries]numericRangeSpanKey
	n := entry.recentFullSpanKeys(keys[:])
	bestBuckets := 0
	bestStart := 0
	bestEnd := 0
	var best posting.List
	for i := 0; i < n; i++ {
		key := keys[i]
		if key.fieldOrdinal != fieldOrdinal {
			continue
		}
		switch {
		case key.start == start && key.end < end:
		case key.end == end && key.start > start:
		default:
			continue
		}
		ids, ok := c.loadSpanByKey(key)
		if !ok {
			continue
		}
		buckets := key.end - key.start + 1
		if best.IsEmpty() || buckets > bestBuckets {
			best.Release()
			best = ids
			bestBuckets = buckets
			bestStart = key.start
			bestEnd = key.end
			continue
		}
		ids.Release()
	}
	if best.IsEmpty() {
		return posting.List{}, 0, 0, false
	}
	entry.touchRecentFullSpan(numericRangeSpanKey{fieldOrdinal: fieldOrdinal, start: bestStart, end: bestEnd})
	return best, bestStart, bestEnd, true
}

func (c *NumericRangeBucketCache) FullSpanRejectedTooLarge(fieldOrdinal, start, end int) bool {
	key := numericRangeSpanKey{fieldOrdinal: fieldOrdinal, start: start, end: end}
	c.mu.RLock()
	rejected := c.spanTooLarge.Contains(key)
	c.mu.RUnlock()
	return rejected
}

func (c *NumericRangeBucketCache) TryStoreFullSpan(entry *NumericRangeBucketEntry, fieldOrdinal, start, end int, ids posting.List) (posting.List, bool) {
	if ids.IsEmpty() {
		return ids, false
	}
	limit := c.spanMaxEntries
	if limit <= 0 || c.spanMaxEntrySize == 0 {
		return ids, false
	}
	key := numericRangeSpanKey{fieldOrdinal: fieldOrdinal, start: start, end: end}
	if cached, ok := c.loadSpanByKey(key); ok {
		ids.Release()
		entry.touchRecentFullSpan(key)
		return cached, true
	}
	size := ids.SizeInBytes()
	if size > c.spanMaxEntrySize {
		c.mu.Lock()
		c.spanTooLarge.TouchOrRemember(key, limit)
		c.spanStats.RejectedTooLarge++
		c.mu.Unlock()
		return ids, false
	}
	stored := ids
	cloned := false
	if stored.IsBorrowed() {
		stored = stored.Clone()
		cloned = true
	}

	c.mu.Lock()
	if existing, ok := c.lookupSpanLocked(key); ok {
		existing.markReferenced()
		c.mu.Unlock()
		if cloned {
			stored.Release()
		}
		ids.Release()
		entry.touchRecentFullSpan(key)
		return existing.ids.Borrow(), true
	}
	if c.spanCount >= limit {
		if !c.spanObserved.TouchOrRemember(key, limit) {
			c.spanStats.RejectedCapacity++
			c.mu.Unlock()
			if cloned {
				stored.Release()
			}
			return ids, false
		}
		if c.spanRetiredCount >= limit {
			c.spanStats.RejectedRetiredBacklog++
			c.mu.Unlock()
			if cloned {
				stored.Release()
			}
			return ids, false
		}
		if !c.evictSpanLocked() {
			c.spanStats.RejectedCapacity++
			c.mu.Unlock()
			if cloned {
				stored.Release()
			}
			return ids, false
		}
	}
	spanEntry := newNumericRangeCachedPosting(stored, size)
	if !c.insertSpanLocked(key, spanEntry) {
		c.spanStats.RejectedCapacity++
		c.mu.Unlock()
		if !cloned {
			spanEntry.ids = posting.List{}
		}
		spanEntry.release()
		return ids, false
	}
	c.spanCount++
	c.spanLiveBytes += size
	c.spanStats.Stores++
	c.mu.Unlock()

	if cloned {
		ids.Release()
	}
	entry.touchRecentFullSpan(key)

	return stored.Borrow(), true
}

func (c *NumericRangeBucketCache) loadSpanByKey(key numericRangeSpanKey) (posting.List, bool) {
	if c.spanMaxEntries <= 0 || c.spanMaxEntrySize == 0 {
		return posting.List{}, false
	}
	c.mu.RLock()
	entry, ok := c.lookupSpanLocked(key)
	if ok {
		entry.markReferenced()
		ids := entry.ids.Borrow()
		c.mu.RUnlock()
		return ids, true
	}
	c.mu.RUnlock()

	return posting.List{}, false
}

func (c *NumericRangeBucketCache) inheritSpanEntriesLocked(prev *NumericRangeBucketCache, nextIndex []indexdata.FieldStorage, fields schema.IndexedFieldMap) {
	if c.spanMaxEntries <= 0 || c.spanMaxEntrySize == 0 || len(c.spanSlots) == 0 || prev.spanCount == 0 {
		return
	}
	for i := range prev.spanSlots {
		if c.spanCount >= c.spanMaxEntries {
			break
		}
		slot := prev.spanSlots[i]
		if !slot.used {
			continue
		}

		fieldOrdinal := slot.key.fieldOrdinal
		if fieldOrdinal < 0 || fieldOrdinal >= len(prev.slots) {
			continue
		}

		prevField := prev.slots[fieldOrdinal]
		if prevField.field == "" || prevField.entry == nil {
			continue
		}

		acc, ok := fields[prevField.field]
		if !ok || acc.Ordinal >= len(nextIndex) || acc.Ordinal >= len(c.slots) {
			continue
		}

		if nextIndex[acc.Ordinal] != prevField.entry.storage {
			continue
		}

		key := numericRangeSpanKey{fieldOrdinal: acc.Ordinal, start: slot.key.start, end: slot.key.end}
		if _, exists := c.lookupSpanLocked(key); exists {
			continue
		}
		entry := slot.entry
		if entry.size > c.spanMaxEntrySize {
			continue
		}
		entry.retain()
		if !c.insertSpanLocked(key, entry) {
			entry.release()
			break
		}
		c.spanCount++
		c.spanLiveBytes += entry.size
		c.touchEntryRecentFullSpanLocked(acc.Ordinal, key)
	}
}

func (c *NumericRangeBucketCache) ExactResultEntryCount() int {
	c.mu.RLock()
	count := c.exactCount
	c.mu.RUnlock()
	return count
}

func (c *NumericRangeBucketCache) NumericExactResultStats() NumericRangeExactResultCacheStats {
	c.mu.RLock()
	stats := c.exactStats
	stats.CurrentBytes = c.exactLiveBytes
	stats.RetiredBytes = c.exactRetiredBytes
	stats.EntryCount = c.exactCount
	stats.MaxEntries = c.exactMaxEntries
	stats.MaxEntryBytes = c.exactMaxEntrySize
	c.mu.RUnlock()
	return stats
}

func (c *NumericRangeBucketCache) LoadExactResult(fieldOrdinal, start, end int) (posting.List, bool) {
	return c.loadExactByKey(numericRangeSpanKey{fieldOrdinal: fieldOrdinal, start: start, end: end})
}

func (c *NumericRangeBucketCache) ExactResultRejectedTooLarge(fieldOrdinal, start, end int) bool {
	key := numericRangeSpanKey{fieldOrdinal: fieldOrdinal, start: start, end: end}
	c.mu.RLock()
	rejected := c.exactTooLarge.Contains(key)
	c.mu.RUnlock()
	return rejected
}

func (c *NumericRangeBucketCache) ShouldPromoteObservedExactResult(fieldOrdinal, start, end int, observedWork, buildWork uint64) bool {
	if observedWork == 0 || buildWork == 0 {
		return false
	}
	limit := c.exactMaxEntries
	if limit <= 0 || c.exactMaxEntrySize == 0 {
		return false
	}
	key := numericRangeSpanKey{fieldOrdinal: fieldOrdinal, start: start, end: end}
	c.mu.Lock()
	promote := c.exactObserved.AddWorkAndShouldPromote(key, numericRangeExactObservedLimit(limit), observedWork, buildWork)
	c.mu.Unlock()
	return promote
}

func (c *NumericRangeBucketCache) ShouldPromoteRuntimeExactResult(fieldOrdinal, start, end int) bool {
	limit := c.exactMaxEntries
	if limit <= 0 || c.exactMaxEntrySize == 0 {
		return false
	}
	key := numericRangeSpanKey{fieldOrdinal: fieldOrdinal, start: start, end: end}
	c.mu.Lock()
	promote := c.exactObserved.TouchOrRemember(key, numericRangeExactObservedLimit(limit))
	c.mu.Unlock()
	return promote
}

func (c *NumericRangeBucketCache) HasObservedExactResult(fieldOrdinal, start, end int) bool {
	key := numericRangeSpanKey{fieldOrdinal: fieldOrdinal, start: start, end: end}
	c.mu.RLock()
	ok := c.exactObserved.Contains(key)
	c.mu.RUnlock()
	return ok
}

func (c *NumericRangeBucketCache) ObservedExactResultWork(fieldOrdinal, start, end int) uint64 {
	key := numericRangeSpanKey{fieldOrdinal: fieldOrdinal, start: start, end: end}
	c.mu.RLock()
	work := c.exactObserved.Work(key)
	c.mu.RUnlock()
	return work
}

func (c *NumericRangeBucketCache) TryStoreExactResult(fieldOrdinal, start, end int, ids posting.List) (posting.List, bool) {
	return c.tryStoreExactResult(fieldOrdinal, start, end, ids, true)
}

func (c *NumericRangeBucketCache) TryStorePromotedExactResult(fieldOrdinal, start, end int, ids posting.List) (posting.List, bool) {
	return c.tryStoreExactResult(fieldOrdinal, start, end, ids, false)
}

func (c *NumericRangeBucketCache) tryStoreExactResult(fieldOrdinal, start, end int, ids posting.List, requireObserved bool) (posting.List, bool) {
	if ids.IsEmpty() {
		return ids, false
	}
	limit := c.exactMaxEntries
	if limit <= 0 || c.exactMaxEntrySize == 0 {
		return ids, false
	}
	key := numericRangeSpanKey{fieldOrdinal: fieldOrdinal, start: start, end: end}
	if cached, ok := c.loadExactByKey(key); ok {
		ids.Release()
		return cached, true
	}
	if c.ExactResultRejectedTooLarge(fieldOrdinal, start, end) {
		return ids, false
	}
	size := ids.SizeInBytes()
	if size > c.exactMaxEntrySize {
		c.mu.Lock()
		c.exactTooLarge.TouchOrRemember(key, numericRangeExactObservedLimit(limit))
		c.exactStats.RejectedTooLarge++
		c.mu.Unlock()
		return ids, false
	}

	c.mu.Lock()
	if existing, ok := c.lookupExactLocked(key); ok {
		existing.markReferenced()
		c.mu.Unlock()
		ids.Release()
		return existing.ids.Borrow(), true
	}
	if requireObserved && !c.exactObserved.TouchOrRemember(key, numericRangeExactObservedLimit(limit)) {
		c.exactStats.RejectedFirstObservation++
		c.mu.Unlock()
		return ids, false
	}
	c.mu.Unlock()

	stored := ids
	cloned := false
	if stored.IsBorrowed() {
		stored = stored.Clone()
		cloned = true
	}

	c.mu.Lock()

	if existing, ok := c.lookupExactLocked(key); ok {
		existing.markReferenced()
		c.mu.Unlock()
		if cloned {
			stored.Release()
		}
		ids.Release()
		return existing.ids.Borrow(), true
	}

	if c.exactCount >= limit {
		if c.exactRetiredCount >= limit {
			c.exactStats.RejectedRetiredBacklog++
			c.mu.Unlock()
			if cloned {
				stored.Release()
			}
			return ids, false
		}
		if !c.evictExactLocked() {
			c.exactStats.RejectedCapacity++
			c.mu.Unlock()
			if cloned {
				stored.Release()
			}
			return ids, false
		}
	}

	exactEntry := newNumericRangeCachedPosting(stored, size)
	if !c.insertExactLocked(key, exactEntry) {
		c.exactStats.RejectedCapacity++
		c.mu.Unlock()
		if !cloned {
			exactEntry.ids = posting.List{}
		}
		exactEntry.release()
		return ids, false
	}

	c.exactCount++
	c.exactLiveBytes += size
	c.exactStats.Stores++

	c.mu.Unlock()

	if cloned {
		ids.Release()
	}
	return stored.Borrow(), true
}

func (c *NumericRangeBucketCache) loadExactByKey(key numericRangeSpanKey) (posting.List, bool) {
	if c.exactMaxEntries <= 0 || c.exactMaxEntrySize == 0 {
		return posting.List{}, false
	}
	c.mu.RLock()
	entry, ok := c.lookupExactLocked(key)
	if ok {
		entry.markReferenced()
		ids := entry.ids.Borrow()
		c.mu.RUnlock()
		return ids, true
	}
	c.mu.RUnlock()

	return posting.List{}, false
}

func (c *NumericRangeBucketCache) inheritExactEntriesLocked(prev *NumericRangeBucketCache, nextIndex []indexdata.FieldStorage, fields schema.IndexedFieldMap) {
	if c.exactMaxEntries <= 0 || c.exactMaxEntrySize == 0 || len(c.exactSlots) == 0 || prev.exactCount == 0 {
		return
	}

	for i := range prev.exactSlots {
		if c.exactCount >= c.exactMaxEntries {
			break
		}

		slot := prev.exactSlots[i]
		if !slot.used {
			continue
		}

		fieldOrdinal := slot.key.fieldOrdinal
		if fieldOrdinal < 0 || fieldOrdinal >= len(prev.slots) {
			continue
		}

		prevField := prev.slots[fieldOrdinal]
		if prevField.field == "" || prevField.entry == nil {
			continue
		}

		acc, ok := fields[prevField.field]
		if !ok || acc.Ordinal >= len(nextIndex) || acc.Ordinal >= len(c.slots) {
			continue
		}

		if nextIndex[acc.Ordinal] != prevField.entry.storage {
			continue
		}

		key := numericRangeSpanKey{fieldOrdinal: acc.Ordinal, start: slot.key.start, end: slot.key.end}
		if _, exists := c.lookupExactLocked(key); exists {
			continue
		}

		entry := slot.entry
		if entry.size > c.exactMaxEntrySize {
			continue
		}

		entry.retain()
		if !c.insertExactLocked(key, entry) {
			entry.release()
			break
		}

		c.exactCount++
		c.exactLiveBytes += entry.size
	}
}

func (c *NumericRangeBucketCache) lookupSpanLocked(key numericRangeSpanKey) (*numericRangeCachedPosting, bool) {
	if len(c.spanSlots) == 0 {
		return nil, false
	}
	if len(c.spanSlots) > numericRangeSpanCacheLinearMaxEntries {
		if idx, ok := c.spanIndex[key]; ok {
			slot := c.spanSlots[idx]
			if slot.used && slot.key == key {
				return slot.entry, true
			}
			for i := range c.spanSlots {
				slot = c.spanSlots[i]
				if slot.used && slot.key == key {
					return slot.entry, true
				}
			}
		}
		return nil, false
	}
	for i := range c.spanSlots {
		slot := c.spanSlots[i]
		if slot.used && slot.key == key {
			return slot.entry, true
		}
	}
	return nil, false
}

func (c *NumericRangeBucketCache) firstFreeSpanSlotLocked() int {
	for i := c.spanFreeHint; i < len(c.spanSlots); i++ {
		if !c.spanSlots[i].used {
			c.spanFreeHint = i + 1
			return i
		}
	}
	for i := 0; i < c.spanFreeHint; i++ {
		if !c.spanSlots[i].used {
			c.spanFreeHint = i + 1
			return i
		}
	}
	return -1
}

func (c *NumericRangeBucketCache) insertSpanLocked(key numericRangeSpanKey, entry *numericRangeCachedPosting) bool {
	idx := c.firstFreeSpanSlotLocked()
	if idx < 0 {
		return false
	}
	if len(c.spanSlots) > numericRangeSpanCacheLinearMaxEntries {
		if c.spanIndex == nil {
			c.spanIndex = numericRangeSpanCacheIndexPool.Get()
		}
		c.spanIndex[key] = idx
	}
	c.spanSlots[idx] = numericRangeSpanCacheSlot{
		key:   key,
		entry: entry,
		used:  true,
	}
	return true
}

func (c *NumericRangeBucketCache) evictSpanLocked() bool {
	idx := c.findSpanVictimLocked()
	if idx < 0 {
		return false
	}
	slot := c.spanSlots[idx]
	if !slot.used {
		return false
	}
	c.spanSlots[idx] = numericRangeSpanCacheSlot{}
	if len(c.spanSlots) > numericRangeSpanCacheLinearMaxEntries && c.spanIndex != nil {
		delete(c.spanIndex, slot.key)
	}
	if idx < c.spanFreeHint {
		c.spanFreeHint = idx
	}
	c.spanCount--
	c.spanLiveBytes -= slot.entry.size
	c.retireSpanEntryLocked(slot.entry)
	c.spanStats.Evictions++
	return true
}

func (c *NumericRangeBucketCache) findSpanVictimLocked() int {
	n := len(c.spanSlots)
	start := c.spanEvictHand
	for pass := 0; pass < 2; pass++ {
		for step := 0; step < n; step++ {
			idx := start + step
			if idx >= n {
				idx -= n
			}
			slot := c.spanSlots[idx]
			if !slot.used {
				continue
			}
			if slot.entry.referenced.Load() {
				if pass == 0 {
					slot.entry.referenced.Store(false)
				}
				continue
			}
			c.spanEvictHand = idx + 1
			if c.spanEvictHand == n {
				c.spanEvictHand = 0
			}
			return idx
		}
	}
	return -1
}

func (c *NumericRangeBucketCache) retireSpanEntryLocked(entry *numericRangeCachedPosting) {
	if c.spanRetired == nil {
		c.spanRetired = numericRangeBucketRetiredPool.Get(1)
	}
	epoch := uint64(0)
	if c.retireEpoch != nil {
		epoch = c.retireEpoch.Load()
	}
	c.spanRetired = append(c.spanRetired, numericRangeSpanRetiredEntry{entry: entry, epoch: epoch})
	c.spanRetiredCount++
	c.spanRetiredBytes += entry.size
	c.retiredDirty.CompareAndSwap(false, true)
	if c.retiredOwner != nil {
		c.retiredOwner.CompareAndSwap(false, true)
	}
}

func (c *NumericRangeBucketCache) lookupExactLocked(key numericRangeSpanKey) (*numericRangeCachedPosting, bool) {
	if len(c.exactSlots) == 0 {
		return nil, false
	}
	if len(c.exactSlots) > numericRangeSpanCacheLinearMaxEntries {
		if idx, ok := c.exactIndex[key]; ok {
			slot := c.exactSlots[idx]
			if slot.used && slot.key == key {
				return slot.entry, true
			}
			for i := range c.exactSlots {
				slot = c.exactSlots[i]
				if slot.used && slot.key == key {
					return slot.entry, true
				}
			}
		}
		return nil, false
	}
	for i := range c.exactSlots {
		slot := c.exactSlots[i]
		if slot.used && slot.key == key {
			return slot.entry, true
		}
	}
	return nil, false
}

func (c *NumericRangeBucketCache) firstFreeExactSlotLocked() int {
	for i := c.exactFreeHint; i < len(c.exactSlots); i++ {
		if !c.exactSlots[i].used {
			c.exactFreeHint = i + 1
			return i
		}
	}
	for i := 0; i < c.exactFreeHint; i++ {
		if !c.exactSlots[i].used {
			c.exactFreeHint = i + 1
			return i
		}
	}
	return -1
}

func (c *NumericRangeBucketCache) insertExactLocked(key numericRangeSpanKey, entry *numericRangeCachedPosting) bool {
	idx := c.firstFreeExactSlotLocked()
	if idx < 0 {
		return false
	}
	if len(c.exactSlots) > numericRangeSpanCacheLinearMaxEntries {
		if c.exactIndex == nil {
			c.exactIndex = numericRangeSpanCacheIndexPool.Get()
		}
		c.exactIndex[key] = idx
	}
	c.exactSlots[idx] = numericRangeExactCacheSlot{
		key:   key,
		entry: entry,
		used:  true,
	}
	return true
}

func (c *NumericRangeBucketCache) evictExactLocked() bool {
	idx := c.findExactVictimLocked()
	if idx < 0 {
		return false
	}
	slot := c.exactSlots[idx]
	if !slot.used {
		return false
	}
	c.exactSlots[idx] = numericRangeExactCacheSlot{}
	if len(c.exactSlots) > numericRangeSpanCacheLinearMaxEntries && c.exactIndex != nil {
		delete(c.exactIndex, slot.key)
	}
	if idx < c.exactFreeHint {
		c.exactFreeHint = idx
	}
	c.exactCount--
	c.exactLiveBytes -= slot.entry.size
	c.retireExactEntryLocked(slot.entry)
	c.exactStats.Evictions++
	return true
}

func (c *NumericRangeBucketCache) findExactVictimLocked() int {
	n := len(c.exactSlots)
	start := c.exactEvictHand
	for pass := 0; pass < 2; pass++ {
		for step := 0; step < n; step++ {
			idx := start + step
			if idx >= n {
				idx -= n
			}
			slot := c.exactSlots[idx]
			if !slot.used {
				continue
			}
			if slot.entry.referenced.Load() {
				if pass == 0 {
					slot.entry.referenced.Store(false)
				}
				continue
			}
			c.exactEvictHand = idx + 1
			if c.exactEvictHand == n {
				c.exactEvictHand = 0
			}
			return idx
		}
	}
	return -1
}

func (c *NumericRangeBucketCache) retireExactEntryLocked(entry *numericRangeCachedPosting) {
	if c.exactRetired == nil {
		c.exactRetired = numericRangeExactRetiredPool.Get(1)
	}
	epoch := uint64(0)
	if c.retireEpoch != nil {
		epoch = c.retireEpoch.Load()
	}
	c.exactRetired = append(c.exactRetired, numericRangeExactRetiredEntry{entry: entry, epoch: epoch})
	c.exactRetiredCount++
	c.exactRetiredBytes += entry.size
	c.retiredDirty.CompareAndSwap(false, true)
	if c.retiredOwner != nil {
		c.retiredOwner.CompareAndSwap(false, true)
	}
}

func (c *NumericRangeBucketCache) clearResultCachesLocked() {
	for i := range c.spanSlots {
		slot := c.spanSlots[i]
		if slot.entry != nil {
			slot.entry.release()
		}
		c.spanSlots[i] = numericRangeSpanCacheSlot{}
	}
	if c.spanIndex != nil {
		clear(c.spanIndex)
	}
	if c.spanRetired != nil {
		for i := range c.spanRetired {
			c.spanRetired[i].entry.release()
		}
		numericRangeBucketRetiredPool.Put(c.spanRetired)
		c.spanRetired = nil
	}
	for i := range c.exactSlots {
		slot := c.exactSlots[i]
		if slot.entry != nil {
			slot.entry.release()
		}
		c.exactSlots[i] = numericRangeExactCacheSlot{}
	}
	if c.exactIndex != nil {
		clear(c.exactIndex)
	}
	if c.exactRetired != nil {
		for i := range c.exactRetired {
			c.exactRetired[i].entry.release()
		}
		numericRangeExactRetiredPool.Put(c.exactRetired)
		c.exactRetired = nil
	}
	c.spanObserved.Clear()
	c.spanTooLarge.Clear()
	c.exactObserved.Clear()
	c.exactTooLarge.Clear()
	c.spanCount = 0
	c.spanLiveBytes = 0
	c.spanRetiredCount = 0
	c.spanRetiredBytes = 0
	c.spanFreeHint = 0
	c.spanEvictHand = 0
	c.spanStats = NumericRangeSpanCacheStats{}
	c.exactCount = 0
	c.exactLiveBytes = 0
	c.exactRetiredCount = 0
	c.exactRetiredBytes = 0
	c.exactFreeHint = 0
	c.exactEvictHand = 0
	c.exactStats = NumericRangeExactResultCacheStats{}
	c.retiredDirty.Store(false)
}

func (e *NumericRangeBucketEntry) touchRecentFullSpan(key numericRangeSpanKey) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.touchRecentFullSpanLocked(key)
}

func (e *NumericRangeBucketEntry) touchRecentFullSpanLocked(key numericRangeSpanKey) {
	slotIdx := -1
	oldestStamp := ^uint64(0)
	for i := range e.fullSpanRecent {
		slot := &e.fullSpanRecent[i]
		if !slot.used {
			slotIdx = i
			break
		}
		if slot.key == key {
			e.fullSpanClock++
			slot.stamp = e.fullSpanClock
			return
		}
		if slot.stamp <= oldestStamp {
			oldestStamp = slot.stamp
			slotIdx = i
		}
	}
	e.fullSpanClock++
	e.fullSpanRecent[slotIdx] = numericRangeFullSpanRecentSlot{
		key:   key,
		stamp: e.fullSpanClock,
		used:  true,
	}
}

func (e *NumericRangeBucketEntry) recentFullSpanKeys(out []numericRangeSpanKey) int {
	e.mu.Lock()
	n := 0
	for i := range e.fullSpanRecent {
		slot := e.fullSpanRecent[i]
		if slot.used {
			out[n] = slot.key
			n++
		}
	}
	e.mu.Unlock()
	return n
}

func (c *NumericRangeBucketCache) touchEntryRecentFullSpanLocked(fieldOrdinal int, key numericRangeSpanKey) {
	if fieldOrdinal < 0 || fieldOrdinal >= len(c.slots) {
		return
	}
	entry := c.slots[fieldOrdinal].entry
	if entry == nil {
		return
	}
	entry.touchRecentFullSpan(key)
}

func (r NumericRangeBucketRetired) Release() {
	if r.spanEntries != nil {
		for i := range r.spanEntries {
			r.spanEntries[i].entry.release()
		}
		numericRangeBucketRetiredPool.Put(r.spanEntries)
	}
	if r.exactEntries != nil {
		for i := range r.exactEntries {
			r.exactEntries[i].entry.release()
		}
		numericRangeExactRetiredPool.Put(r.exactEntries)
	}
}

func (r NumericRangeBucketRetired) IsEmpty() bool {
	return r.spanEntries == nil && r.exactEntries == nil
}

func newNumericRangeCachedPosting(ids posting.List, size uint64) *numericRangeCachedPosting {
	entry := numericRangeCachedPostingPool.Get()
	entry.refs.Store(1)
	entry.referenced.Store(false)
	entry.ids = ids
	entry.size = size
	return entry
}

func (e *numericRangeCachedPosting) retain() {
	e.refs.Add(1)
}

func (e *numericRangeCachedPosting) release() {
	if e.refs.Add(-1) != 0 {
		return
	}
	e.ids.Release()
	numericRangeCachedPostingPool.Put(e)
}

func (e *numericRangeCachedPosting) markReferenced() {
	if !e.referenced.Load() {
		e.referenced.Store(true)
	}
}

func numericRangeExactObservedLimit(limit int) int {
	if limit <= 0 {
		return 0
	}
	if limit >= 1024 {
		return 4096
	}
	n := limit * 4
	if n < 64 {
		return 64
	}
	if n > 4096 {
		return 4096
	}
	return n
}

func (c *numericRangeSpanObservedCache) Clear() {
	if c.slots != nil {
		numericRangeSpanObservedSlotPool.Put(c.slots)
		c.slots = nil
	}
	if c.index != nil {
		numericRangeSpanObservedIndexPool.Put(c.index)
		c.index = nil
	}
	c.indexLen = 0
	c.clock = 0
}

func (c *numericRangeSpanObservedCache) TouchOrRemember(key numericRangeSpanKey, limit int) bool {
	if limit <= 0 {
		return false
	}
	c.initSlots(limit)
	if idx, ok := c.findSlot(key); ok {
		slot := c.slots[idx]
		slot.stamp = c.nextStamp()
		c.slots[idx] = slot
		return true
	}
	idx := c.selectVictimSlot()
	if idx < 0 {
		return false
	}

	if len(c.slots) > numericRangeSpanCacheLinearMaxEntries && c.slots[idx].used {
		c.removeIndex(c.slots[idx].key, idx)
	}
	if len(c.slots) > numericRangeSpanCacheLinearMaxEntries {
		c.index[key] = idx
	}

	c.slots[idx] = numericRangeSpanObservedSlot{
		key:   key,
		stamp: c.nextStamp(),
		used:  true,
	}
	return false
}

func (c *numericRangeSpanObservedCache) AddWorkAndShouldPromote(key numericRangeSpanKey, limit int, delta, threshold uint64) bool {
	if limit <= 0 || delta == 0 || threshold == 0 {
		return false
	}

	c.initSlots(limit)

	if idx, ok := c.findSlot(key); ok {
		slot := c.slots[idx]
		slot.stamp = c.nextStamp()
		slot.work = mathutil.SatAddUint64(slot.work, delta)
		c.slots[idx] = slot
		return slot.work >= threshold
	}

	idx := c.selectVictimSlot()
	if idx < 0 {
		return false
	}

	if len(c.slots) > numericRangeSpanCacheLinearMaxEntries && c.slots[idx].used {
		c.removeIndex(c.slots[idx].key, idx)
	}
	if len(c.slots) > numericRangeSpanCacheLinearMaxEntries {
		c.index[key] = idx
	}

	c.slots[idx] = numericRangeSpanObservedSlot{
		key:   key,
		stamp: c.nextStamp(),
		work:  delta,
		used:  true,
	}
	return delta >= threshold
}

func (c *numericRangeSpanObservedCache) initSlots(limit int) {
	if c.slots == nil {
		c.slots = numericRangeSpanObservedSlotPool.Get(limit)

	} else if cap(c.slots) < limit {
		slots := numericRangeSpanObservedSlotPool.Get(limit)
		slots = slots[:len(c.slots)]
		copy(slots, c.slots)
		numericRangeSpanObservedSlotPool.Put(c.slots)
		c.slots = slots

	} else if len(c.slots) > limit {
		clear(c.slots[limit:])
	}

	c.slots = c.slots[:limit]

	if limit > numericRangeSpanCacheLinearMaxEntries {
		if c.index == nil {
			c.index = numericRangeSpanObservedIndexPool.Get()
		}
		if c.indexLen != limit {
			clear(c.index)
			for i := range c.slots {
				if c.slots[i].used {
					c.index[c.slots[i].key] = i
				}
			}
			c.indexLen = limit
		}

	} else if c.index != nil {
		numericRangeSpanObservedIndexPool.Put(c.index)
		c.index = nil
		c.indexLen = 0
	}
}

func (c *numericRangeSpanObservedCache) findSlot(key numericRangeSpanKey) (int, bool) {
	if len(c.slots) > numericRangeSpanCacheLinearMaxEntries {
		if idx, ok := c.index[key]; ok {
			slot := c.slots[idx]
			if slot.used && slot.key == key {
				return idx, true
			}
			for i := range c.slots {
				slot = c.slots[i]
				if slot.used && slot.key == key {
					return i, true
				}
			}
		}
		return 0, false
	}
	for i := range c.slots {
		slot := c.slots[i]
		if slot.used && slot.key == key {
			return i, true
		}
	}
	return 0, false
}

func (c *numericRangeSpanObservedCache) Contains(key numericRangeSpanKey) bool {
	_, ok := c.findSlot(key)
	return ok
}

func (c *numericRangeSpanObservedCache) Work(key numericRangeSpanKey) uint64 {
	if idx, ok := c.findSlot(key); ok {
		return c.slots[idx].work
	}
	return 0
}

func (c *numericRangeSpanObservedCache) removeIndex(key numericRangeSpanKey, idx int) {
	if c.index[key] != idx {
		return
	}
	delete(c.index, key)
	for i := range c.slots {
		if i != idx && c.slots[i].used && c.slots[i].key == key {
			c.index[key] = i
			return
		}
	}
}

func (c *numericRangeSpanObservedCache) nextStamp() uint64 {
	c.clock++
	return c.clock
}

func (c *numericRangeSpanObservedCache) selectVictimSlot() int {
	slotIdx := -1
	oldestStamp := ^uint64(0)
	for i := range c.slots {
		slot := c.slots[i]
		if !slot.used {
			return i
		}
		if slot.stamp <= oldestStamp {
			oldestStamp = slot.stamp
			slotIdx = i
		}
	}
	return slotIdx
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
