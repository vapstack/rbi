package qcache

import (
	"sync"
	"sync/atomic"

	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/schema"
)

const (
	// oversized postings get at most one quarter of the regular materialized
	// cache, with one slot still available for very small cache limits.
	materializedPredCacheOversizedDivisor = 4

	// hard cap on oversized entries keeps high-cardinality postings from
	// dominating memory when the regular cache limit is large.
	materializedPredCacheOversizedMaxEntries = 4

	// linear scan is cheaper for tiny caches; above eight slots the hash index
	// wins on qcache lookup and recent-key hot benchmarks.
	materializedPredCacheLinearMaxEntries = 8
)

type materializedPredCacheEvictMode uint8

const (
	matPredCacheEvictPreferRegular materializedPredCacheEvictMode = iota
	matPredCacheEvictOversizedOnly
)

type MaterializedPredCache struct {
	refs           atomic.Int32
	mu             sync.RWMutex
	slots          []materializedPredCacheSlot
	index          map[uint64]int
	retired        []*materializedPredCacheEntry
	count          atomic.Int32
	oversizedCount atomic.Int32
	clock          atomic.Uint64
	freeHint       int
	maxEntries     int
	maxCard        uint64
}

type materializedPredCacheEntry struct {
	refs      atomic.Int32
	ids       posting.List
	oversized bool
	stamp     atomic.Uint64
}

type materializedPredCacheSlot struct {
	key   MaterializedPredKey
	hash  uint64
	entry *materializedPredCacheEntry
	used  bool
}

type RecentKeyCache struct {
	mu       sync.Mutex
	clock    uint64
	slots    []recentKeyCacheSlot
	index    map[uint64]int
	indexLen int
}

type recentKeyCacheSlot struct {
	key   MaterializedPredKey
	hash  uint64
	stamp uint64
	work  uint64
	used  bool
}

func MaterializedPredMaxCardinality(v int) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

func MaterializedPredOversizedLimit(limit int) int32 {
	if limit <= 0 {
		return 0
	}
	if limit <= materializedPredCacheOversizedDivisor {
		return 1
	}
	oversized := int32(limit / materializedPredCacheOversizedDivisor)
	if oversized > materializedPredCacheOversizedMaxEntries {
		oversized = materializedPredCacheOversizedMaxEntries
	}
	return oversized
}

func RecentKeyLimit(materializedLimit int) int {
	if materializedLimit <= 0 {
		return 0
	}
	oversized := int(MaterializedPredOversizedLimit(materializedLimit))
	if materializedLimit > int(^uint(0)>>1)-oversized {
		return int(^uint(0) >> 1)
	}
	return materializedLimit + oversized
}

func GetMaterializedPredCache(maxEntries int, maxCardinality uint64) *MaterializedPredCache {
	c := materializedPredCachePool.Get()
	c.refs.Store(1)
	c.Init(maxEntries, maxCardinality)
	return c
}

func (c *MaterializedPredCache) Init(maxEntries int, maxCardinality uint64) {
	if maxEntries <= 0 {
		return
	}
	c.maxEntries = maxEntries
	c.maxCard = maxCardinality
	c.freeHint = 0
	if cap(c.slots) < maxEntries {
		c.slots = make([]materializedPredCacheSlot, maxEntries)
		if maxEntries > materializedPredCacheLinearMaxEntries && c.index == nil {
			c.index = materializedPredCacheIndexPool.Get()
		}
		return
	}
	c.slots = c.slots[:maxEntries]
	if maxEntries > materializedPredCacheLinearMaxEntries && c.index == nil {
		c.index = materializedPredCacheIndexPool.Get()
	}
}

func (c *MaterializedPredCache) Retain() {
	c.refs.Add(1)
}

func (c *MaterializedPredCache) ReleaseRef() {
	if c.refs.Add(-1) != 0 {
		return
	}
	materializedPredCachePool.Put(c)
}

func (c *MaterializedPredCache) EntryCount() int {
	if len(c.slots) == 0 {
		return 0
	}
	return int(c.count.Load())
}

func (c *MaterializedPredCache) OversizedCount() int32 {
	return c.oversizedCount.Load()
}

func (c *MaterializedPredCache) Clock() uint64 {
	return c.clock.Load()
}

func (c *MaterializedPredCache) Limit() int {
	return c.maxEntries
}

func (c *MaterializedPredCache) MaxCardinality() uint64 {
	return c.maxCard
}

func (c *MaterializedPredCache) Load(key MaterializedPredKey) (posting.List, bool) {
	if key.IsZero() {
		return posting.List{}, false
	}

	c.mu.RLock()
	entry, ok := c.lookupLocked(&key)
	c.mu.RUnlock()

	if !ok {
		return posting.List{}, false
	}
	if entry == nil {
		return posting.List{}, true
	}

	entry.touch(&c.clock)

	return entry.ids.Borrow(), true
}

func (c *MaterializedPredCache) Has(key MaterializedPredKey) bool {
	if key.IsZero() {
		return false
	}

	c.mu.RLock()
	_, ok := c.lookupLocked(&key)
	c.mu.RUnlock()
	return ok
}

func (c *MaterializedPredCache) Store(key MaterializedPredKey, ids posting.List) {
	if key.IsZero() {
		return
	}
	limit := c.maxEntries
	if limit <= 0 {
		return
	}
	if !ids.IsEmpty() && c.maxCard > 0 && ids.Cardinality() > c.maxCard {
		return
	}

	c.mu.Lock()
	if _, ok := c.lookupLocked(&key); ok {
		c.mu.Unlock()
		return
	}
	if int(c.count.Load()) >= limit && !c.evictLocked(matPredCacheEvictPreferRegular) {
		c.mu.Unlock()
		return
	}
	stored := materializedPredCacheStoredIDs(ids)
	entry := newMaterializedPredCacheEntry(stored, false, &c.clock)
	if !c.insertLocked(key, entry) {
		c.mu.Unlock()
		entry.release()
		return
	}
	c.count.Add(1)
	c.mu.Unlock()
}

func (c *MaterializedPredCache) TryStoreOversized(key MaterializedPredKey, ids posting.List) bool {
	if key.IsZero() || ids.IsEmpty() {
		return false
	}
	if c.maxCard == 0 || ids.Cardinality() <= c.maxCard {
		return false
	}
	limit := c.maxEntries
	if limit <= 0 {
		return false
	}

	c.mu.Lock()
	if _, ok := c.lookupLocked(&key); ok {
		c.mu.Unlock()
		return false
	}
	if c.oversizedCount.Load() >= MaterializedPredOversizedLimit(limit) && !c.evictLocked(matPredCacheEvictOversizedOnly) {
		c.mu.Unlock()
		return false
	}
	if int(c.count.Load()) >= limit && !c.evictLocked(matPredCacheEvictPreferRegular) {
		c.mu.Unlock()
		return false
	}
	stored := materializedPredCacheStoredIDs(ids)
	entry := newMaterializedPredCacheEntry(stored, true, &c.clock)
	if !c.insertLocked(key, entry) {
		c.mu.Unlock()
		entry.release()
		return false
	}
	c.count.Add(1)
	c.oversizedCount.Add(1)
	c.mu.Unlock()

	return true
}

func (c *MaterializedPredCache) LoadOrStore(key MaterializedPredKey, ids posting.List) (posting.List, bool) {
	if key.IsZero() || ids.IsEmpty() {
		return ids, false
	}
	limit := c.maxEntries
	if limit <= 0 {
		return ids, false
	}
	if c.maxCard > 0 && ids.Cardinality() > c.maxCard {
		return ids, false
	}
	if cached, ok := c.Load(key); ok {
		ids.Release()
		return cached, true
	}

	c.mu.Lock()
	if entry, ok := c.lookupLocked(&key); ok {
		c.mu.Unlock()
		ids.Release()
		if entry == nil {
			return posting.List{}, true
		}
		entry.touch(&c.clock)
		return entry.ids.Borrow(), true
	}
	if int(c.count.Load()) >= limit && !c.evictLocked(matPredCacheEvictPreferRegular) {
		c.mu.Unlock()
		return ids, false
	}
	stored := materializedPredCacheStoredIDs(ids)
	entry := newMaterializedPredCacheEntry(stored, false, &c.clock)
	if !c.insertLocked(key, entry) {
		c.mu.Unlock()
		entry.release()
		return ids, false
	}
	c.count.Add(1)
	c.mu.Unlock()

	return stored.Borrow(), true
}

func (c *MaterializedPredCache) TryLoadOrStoreOversized(key MaterializedPredKey, ids posting.List) (posting.List, bool) {
	if key.IsZero() || ids.IsEmpty() {
		return ids, false
	}
	if c.maxCard == 0 || ids.Cardinality() <= c.maxCard {
		return ids, false
	}
	limit := c.maxEntries
	if limit <= 0 {
		return ids, false
	}
	if cached, ok := c.Load(key); ok {
		ids.Release()
		return cached, true
	}

	c.mu.Lock()
	if entry, ok := c.lookupLocked(&key); ok {
		c.mu.Unlock()
		ids.Release()
		if entry == nil {
			return posting.List{}, true
		}
		entry.touch(&c.clock)
		return entry.ids.Borrow(), true
	}
	if c.oversizedCount.Load() >= MaterializedPredOversizedLimit(limit) && !c.evictLocked(matPredCacheEvictOversizedOnly) {
		c.mu.Unlock()
		return ids, false
	}
	if int(c.count.Load()) >= limit && !c.evictLocked(matPredCacheEvictPreferRegular) {
		c.mu.Unlock()
		return ids, false
	}
	stored := materializedPredCacheStoredIDs(ids)
	entry := newMaterializedPredCacheEntry(stored, true, &c.clock)
	if !c.insertLocked(key, entry) {
		c.mu.Unlock()
		entry.release()
		return ids, false
	}
	c.count.Add(1)
	c.oversizedCount.Add(1)
	c.mu.Unlock()

	return stored.Borrow(), true
}

func (c *MaterializedPredCache) InheritFrom(prev *MaterializedPredCache, fields schema.IndexedFieldMap, changedFields []bool) {
	limit := c.maxEntries
	if limit <= 0 || prev.count.Load() == 0 || len(prev.slots) == 0 || len(c.slots) == 0 {
		return
	}

	var oversized int32
	var maxStamp uint64

	prev.mu.RLock()
	c.mu.Lock()
	for i := range prev.slots {
		if int(c.count.Load()) >= limit {
			break
		}
		slot := prev.slots[i]
		if !slot.used {
			continue
		}
		key := slot.key
		f := key.Field()
		if f == "" {
			continue
		}
		if changedFields != nil {
			acc, ok := fields[f]
			if !ok || changedFields[acc.Ordinal] {
				continue
			}
		}
		if _, exists := c.lookupLocked(&key); exists {
			continue
		}
		if slot.entry != nil {
			slot.entry.retain()
			stamp := slot.entry.stamp.Load()
			if stamp > maxStamp {
				maxStamp = stamp
			}
			if slot.entry.oversized {
				oversized++
			}
		}
		if !c.insertLocked(key, slot.entry) {
			if slot.entry != nil {
				slot.entry.release()
			}
			break
		}
		c.count.Add(1)
	}

	c.mu.Unlock()
	prev.mu.RUnlock()
	if oversized > 0 {
		c.oversizedCount.Store(min(oversized, MaterializedPredOversizedLimit(limit)))
	}
	if maxStamp > c.clock.Load() {
		c.clock.Store(maxStamp)
	}
}

func (c *MaterializedPredCache) Clear() {
	c.mu.Lock()
	c.clearLocked()
	c.count.Store(0)
	c.oversizedCount.Store(0)
	c.clock.Store(0)
	c.mu.Unlock()
}

func (c *MaterializedPredCache) DrainRetired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.retired == nil {
		return
	}
	for i := range c.retired {
		c.retired[i].release()
	}
	materializedPredCacheRetiredPool.Put(c.retired)
	c.retired = nil
}

func (c *MaterializedPredCache) release() {
	c.Clear()
	if c.index != nil {
		materializedPredCacheIndexPool.Put(c.index)
		c.index = nil
	}
	c.slots = c.slots[:0]
	c.maxEntries = 0
	c.maxCard = 0
}

func (c *MaterializedPredCache) lookupLocked(key *MaterializedPredKey) (*materializedPredCacheEntry, bool) {
	if len(c.slots) == 0 {
		return nil, false
	}
	if len(c.slots) > materializedPredCacheLinearMaxEntries {
		hash := key.hash()
		if idx, ok := c.index[hash]; ok {
			slot := c.slots[idx]
			if slot.used && slot.hash == hash && slot.key == *key {
				return slot.entry, true
			}
			for i := range c.slots {
				slot = c.slots[i]
				if slot.used && slot.hash == hash && slot.key == *key {
					return slot.entry, true
				}
			}
		}
		return nil, false
	}
	for i := range c.slots {
		slot := c.slots[i]
		if slot.used && slot.key == *key {
			return slot.entry, true
		}
	}
	return nil, false
}

func (c *MaterializedPredCache) firstFreeSlotLocked() int {
	if len(c.slots) == 0 {
		return -1
	}
	for i := c.freeHint; i < len(c.slots); i++ {
		if !c.slots[i].used {
			c.freeHint = i + 1
			return i
		}
	}
	for i := 0; i < c.freeHint; i++ {
		if !c.slots[i].used {
			c.freeHint = i + 1
			return i
		}
	}
	return -1
}

func (c *MaterializedPredCache) retireEntryLocked(entry *materializedPredCacheEntry) {
	if entry == nil {
		return
	}
	if c.retired == nil {
		c.retired = materializedPredCacheRetiredPool.Get(1)
	}
	c.retired = append(c.retired, entry)
}

func (c *MaterializedPredCache) clearLocked() {
	for i := range c.slots {
		slot := c.slots[i]
		if slot.entry != nil {
			slot.entry.release()
		}
		c.slots[i] = materializedPredCacheSlot{}
	}
	if c.index != nil {
		clear(c.index)
	}
	if c.retired != nil {
		for i := range c.retired {
			c.retired[i].release()
		}
		materializedPredCacheRetiredPool.Put(c.retired)
		c.retired = nil
	}
	c.freeHint = 0
}

func (c *MaterializedPredCache) findVictimLocked(mode materializedPredCacheEvictMode) int {
	if len(c.slots) == 0 {
		return -1
	}
	evictIdx := -1
	fallbackIdx := -1
	evictStamp := ^uint64(0)
	fallbackStamp := ^uint64(0)
	for i := range c.slots {
		slot := c.slots[i]
		if !slot.used {
			continue
		}
		stamp := uint64(0)
		if slot.entry != nil {
			stamp = slot.entry.stamp.Load()
		}
		if slot.entry == nil {
			if mode != matPredCacheEvictOversizedOnly && stamp <= fallbackStamp {
				fallbackIdx = i
				fallbackStamp = stamp
			}
			continue
		}
		if slot.entry.oversized {
			if mode != matPredCacheEvictOversizedOnly && stamp <= fallbackStamp {
				fallbackIdx = i
				fallbackStamp = stamp
			}
			if mode == matPredCacheEvictOversizedOnly && stamp <= evictStamp {
				evictIdx = i
				evictStamp = stamp
			}
			continue
		}
		if mode != matPredCacheEvictOversizedOnly && stamp <= evictStamp {
			evictIdx = i
			evictStamp = stamp
		}
	}
	if evictIdx >= 0 {
		return evictIdx
	}
	return fallbackIdx
}

func (c *MaterializedPredCache) evictLocked(mode materializedPredCacheEvictMode) bool {
	idx := c.findVictimLocked(mode)
	if idx < 0 {
		return false
	}
	slot := c.slots[idx]
	c.slots[idx] = materializedPredCacheSlot{}
	if len(c.slots) > materializedPredCacheLinearMaxEntries {
		if c.index[slot.hash] == idx {
			delete(c.index, slot.hash)
			for i := range c.slots {
				if i != idx && c.slots[i].used && c.slots[i].hash == slot.hash {
					c.index[slot.hash] = i
					break
				}
			}
		}
	}
	if idx < c.freeHint {
		c.freeHint = idx
	}
	c.count.Add(-1)
	if slot.entry != nil && slot.entry.oversized {
		c.oversizedCount.Add(-1)
	}
	c.retireEntryLocked(slot.entry)
	return true
}

func (c *MaterializedPredCache) insertLocked(key MaterializedPredKey, entry *materializedPredCacheEntry) bool {
	if len(c.slots) == 0 || key.IsZero() {
		return false
	}
	idx := c.firstFreeSlotLocked()
	if idx < 0 {
		return false
	}
	hash := uint64(0)
	if len(c.slots) > materializedPredCacheLinearMaxEntries {
		if c.index == nil {
			c.index = materializedPredCacheIndexPool.Get()
		}
		hash = key.hash()
		c.index[hash] = idx
	}
	c.slots[idx] = materializedPredCacheSlot{
		key:   key,
		hash:  hash,
		entry: entry,
		used:  true,
	}
	return true
}

func materializedPredCacheStoredIDs(ids posting.List) posting.List {
	if ids.IsBorrowed() {
		return ids.Clone()
	}
	return ids
}

func newMaterializedPredCacheEntry(ids posting.List, oversized bool, clock *atomic.Uint64) *materializedPredCacheEntry {
	entry := materializedPredCacheEntryPool.Get()
	entry.refs.Store(1)
	entry.ids = ids
	entry.oversized = oversized
	entry.touch(clock)
	return entry
}

func (e *materializedPredCacheEntry) retain() {
	e.refs.Add(1)
}

func (e *materializedPredCacheEntry) release() {
	if e == nil || e.refs.Add(-1) != 0 {
		return
	}
	if !e.ids.IsEmpty() {
		e.ids.Release()
	}
	materializedPredCacheEntryPool.Put(e)
}

func (e *materializedPredCacheEntry) touch(clock *atomic.Uint64) {
	e.stamp.Store(clock.Add(1))
}

func (c *RecentKeyCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.slots != nil {
		recentKeyCacheSlotPool.Put(c.slots)
		c.slots = nil
	}
	if c.index != nil {
		recentKeyCacheIndexPool.Put(c.index)
		c.index = nil
	}
	c.indexLen = 0
	c.clock = 0
}

func (c *RecentKeyCache) EntryCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.slots == nil {
		return 0
	}
	count := 0
	for i := range c.slots {
		if c.slots[i].used {
			count++
		}
	}
	return count
}

func (c *RecentKeyCache) Contains(key MaterializedPredKey) bool {
	if key.IsZero() {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.slots == nil {
		return false
	}
	_, ok := c.findSlot(key)
	return ok
}

func (c *RecentKeyCache) Work(key MaterializedPredKey) uint64 {
	if key.IsZero() {
		return 0
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.slots == nil {
		return 0
	}
	idx, ok := c.findSlot(key)
	if !ok {
		return 0
	}
	return c.slots[idx].work
}

func (c *RecentKeyCache) TouchOrRemember(key MaterializedPredKey, limit int) bool {
	if key.IsZero() || limit <= 0 {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

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
	if len(c.slots) > materializedPredCacheLinearMaxEntries && c.slots[idx].used {
		c.removeRecentIndexLocked(c.slots[idx].hash, idx)
	}
	hash := uint64(0)
	if len(c.slots) > materializedPredCacheLinearMaxEntries {
		hash = key.hash()
		c.index[hash] = idx
	}
	c.slots[idx] = recentKeyCacheSlot{
		key:   key,
		hash:  hash,
		stamp: c.nextStamp(),
		used:  true,
	}
	return false
}

func (c *RecentKeyCache) AddWorkAndShouldPromote(key MaterializedPredKey, limit int, delta, threshold uint64) (promote bool, hadWork bool) {
	if key.IsZero() || limit <= 0 || delta == 0 || threshold == 0 {
		return false, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.initSlots(limit)
	if idx, ok := c.findSlot(key); ok {
		slot := c.slots[idx]
		hadWork = slot.work != 0
		slot.stamp = c.nextStamp()
		slot.work = addObservedWork(slot.work, delta)
		c.slots[idx] = slot
		return slot.work >= threshold, hadWork
	}
	idx := c.selectVictimSlot()
	if idx < 0 {
		return false, false
	}
	if len(c.slots) > materializedPredCacheLinearMaxEntries && c.slots[idx].used {
		c.removeRecentIndexLocked(c.slots[idx].hash, idx)
	}
	hash := uint64(0)
	if len(c.slots) > materializedPredCacheLinearMaxEntries {
		hash = key.hash()
		c.index[hash] = idx
	}
	c.slots[idx] = recentKeyCacheSlot{
		key:   key,
		hash:  hash,
		stamp: c.nextStamp(),
		work:  delta,
		used:  true,
	}
	return delta >= threshold, false
}

func (c *RecentKeyCache) InheritObservedWorkFrom(prev *RecentKeyCache, fields schema.IndexedFieldMap, changedFields []bool, limit int) {
	c.inheritObservedWorkFrom(prev, fields, changedFields, false, limit)
}

func (c *RecentKeyCache) InheritChangedObservedWorkFrom(prev *RecentKeyCache, fields schema.IndexedFieldMap, changedFields []bool, limit int) {
	c.inheritObservedWorkFrom(prev, fields, changedFields, true, limit)
}

func (c *RecentKeyCache) inheritObservedWorkFrom(prev *RecentKeyCache, fields schema.IndexedFieldMap, changedFields []bool, changedOnly bool, limit int) {
	if limit <= 0 {
		return
	}
	if changedOnly && len(changedFields) == 0 {
		return
	}

	prev.mu.Lock()
	if prev.slots == nil {
		prev.mu.Unlock()
		return
	}

	c.mu.Lock()
	c.initSlots(limit)

	maxStamp := c.clock
	for i := range prev.slots {
		slot := prev.slots[i]
		if !slot.used || slot.work == 0 {
			continue
		}
		f := slot.key.Field()
		if f == "" {
			continue
		}
		if changedFields != nil {
			acc, ok := fields[f]
			if !ok || changedFields[acc.Ordinal] != changedOnly {
				continue
			}
		} else if changedOnly {
			continue
		}
		idx := c.selectVictimSlot()
		if idx < 0 {
			break
		}
		if len(c.slots) > materializedPredCacheLinearMaxEntries && c.slots[idx].used {
			c.removeRecentIndexLocked(c.slots[idx].hash, idx)
		}
		hash := uint64(0)
		if len(c.slots) > materializedPredCacheLinearMaxEntries {
			hash = slot.key.hash()
			c.index[hash] = idx
		}
		c.slots[idx] = recentKeyCacheSlot{
			key:   slot.key,
			hash:  hash,
			stamp: slot.stamp,
			work:  slot.work,
			used:  true,
		}
		if slot.stamp > maxStamp {
			maxStamp = slot.stamp
		}
	}
	c.clock = maxStamp
	c.mu.Unlock()
	prev.mu.Unlock()
}

func (c *RecentKeyCache) initSlots(limit int) {
	if c.slots == nil {
		c.slots = recentKeyCacheSlotPool.Get(limit)
	} else if cap(c.slots) < limit {
		slots := recentKeyCacheSlotPool.Get(limit)
		slots = slots[:len(c.slots)]
		copy(slots, c.slots)
		recentKeyCacheSlotPool.Put(c.slots)
		c.slots = slots
	}
	c.slots = c.slots[:limit]
	if limit > materializedPredCacheLinearMaxEntries {
		if c.index == nil {
			c.index = recentKeyCacheIndexPool.Get()
		}
		if c.indexLen != limit {
			clear(c.index)
			for i := range c.slots {
				if c.slots[i].used {
					hash := c.slots[i].key.hash()
					c.slots[i].hash = hash
					c.index[hash] = i
				}
			}
			c.indexLen = limit
		}
	} else if c.index != nil {
		recentKeyCacheIndexPool.Put(c.index)
		c.index = nil
		c.indexLen = 0
	}
}

func (c *RecentKeyCache) findSlot(key MaterializedPredKey) (int, bool) {
	if len(c.slots) > materializedPredCacheLinearMaxEntries {
		hash := key.hash()
		if idx, ok := c.index[hash]; ok {
			slot := c.slots[idx]
			if slot.used && slot.hash == hash && slot.key == key {
				return idx, true
			}
			for i := range c.slots {
				slot = c.slots[i]
				if slot.used && slot.hash == hash && slot.key == key {
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

func (c *RecentKeyCache) removeRecentIndexLocked(hash uint64, idx int) {
	if c.index[hash] != idx {
		return
	}
	delete(c.index, hash)
	for i := range c.slots {
		if i != idx && c.slots[i].used && c.slots[i].hash == hash {
			c.index[hash] = i
			return
		}
	}
}

func (c *RecentKeyCache) nextStamp() uint64 {
	c.clock++
	return c.clock
}

func (c *RecentKeyCache) selectVictimSlot() int {
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

func addObservedWork(cur, delta uint64) uint64 {
	if ^uint64(0)-cur < delta {
		return ^uint64(0)
	}
	return cur + delta
}
