package rbi

import (
	"sync"
	"sync/atomic"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

// indexSnapshot is an immutable read-view published atomically for query paths.
type indexSnapshot struct {
	seq uint64

	index              []indexdata.FieldStorage
	nilIndex           []indexdata.FieldStorage
	lenIndex           []indexdata.FieldStorage
	lenZeroComplement  []bool
	measure            []indexdata.MeasureStorage
	indexedFieldByName map[string]indexedFieldAccessor
	universe           posting.List
	universeOwner      *snapshotPostingOwner
	strmap             *strMapSnapshot

	numericRangeBucketCache *numericRangeBucketCache

	matPredCache               *materializedPredCache
	matPredCacheCount          atomic.Int32
	matPredCacheMaxEntries     int
	matPredCacheMaxCard        uint64
	matPredCacheOversizedCount atomic.Int32
	matPredCacheClock          atomic.Uint64
	runtimeMatPredSeen         recentKeyCache
	orderORMatPredObserved     recentKeyCache
}

type materializedPredCacheEntry struct {
	refs      atomic.Int32
	ids       posting.List
	oversized bool
	stamp     atomic.Uint64
}

type materializedPredCache struct {
	refs    atomic.Int32
	mu      sync.RWMutex
	slots   *pooled.Slice[materializedPredCacheSlot]
	retired *pooled.Slice[*materializedPredCacheEntry]
}

type materializedPredCacheSlot struct {
	key   materializedPredKey
	entry *materializedPredCacheEntry
	used  bool
}

type recentKeyCache struct {
	mu    sync.Mutex
	clock uint64
	slots *pooled.Slice[recentKeyCacheSlot]
}

type recentKeyCacheSlot struct {
	key   recentKeyCacheKey
	stamp uint64
	work  uint64
	used  bool
}

type recentKeyCacheKey struct {
	kind uint8
	text string
	pred materializedPredKey
}

const matPredCacheOversizedMaxEntries = 4

type materializedPredCacheEvictMode uint8

const (
	matPredCacheEvictPreferRegular materializedPredCacheEvictMode = iota
	matPredCacheEvictOversizedOnly
)

type snapshotRef struct {
	snap    *indexSnapshot
	retired *pooled.Slice[*indexSnapshot]
	refs    atomic.Int64
}

var snapshotRefPool = pooled.Pointers[snapshotRef]{Clear: true}

var snapshotRetiredListPool = pooled.Slices[*indexSnapshot]{Clear: true}

var recentKeyCacheSlotPool = pooled.Slices[recentKeyCacheSlot]{Clear: true}

var materializedPredCachePool = pooled.Pointers[materializedPredCache]{
	Cleanup: func(c *materializedPredCache) {
		c.release()
	},
}

var materializedPredCacheSlotPool = pooled.Slices[materializedPredCacheSlot]{Clear: true}

var materializedPredCacheRetiredPool = pooled.Slices[*materializedPredCacheEntry]{
	Clear: true,
}

var materializedPredCacheEntryPool = pooled.Pointers[materializedPredCacheEntry]{
	Clear: true,
}

func materializedPredCacheMaxCardinality(v int) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

func materializedPredCacheOversizedLimit(limit int) int32 {
	if limit <= 0 {
		return 0
	}
	if limit <= 4 {
		return 1
	}
	oversized := int32(limit / 4)
	if oversized < 1 {
		oversized = 1
	}
	if oversized > matPredCacheOversizedMaxEntries {
		oversized = matPredCacheOversizedMaxEntries
	}
	return oversized
}

func recentKeyCacheLimit(limit int) int {
	if limit <= 0 {
		return 0
	}
	oversized := int(materializedPredCacheOversizedLimit(limit))
	if oversized <= 0 {
		return limit
	}
	if limit > int(^uint(0)>>1)-oversized {
		return int(^uint(0) >> 1)
	}
	return limit + oversized
}

func (c *recentKeyCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.slots != nil {
		recentKeyCacheSlotPool.Put(c.slots)
		c.slots = nil
	}
	c.clock = 0
}

func recentKeyCacheKeyFromAny(key any) (recentKeyCacheKey, bool) {
	switch key := key.(type) {
	case materializedPredKey:
		if key.isZero() {
			return recentKeyCacheKey{}, false
		}
		return recentKeyCacheKey{kind: 1, pred: key}, true
	case string:
		if key == "" {
			return recentKeyCacheKey{}, false
		}
		return recentKeyCacheKey{kind: 2, text: key}, true
	default:
		return recentKeyCacheKey{}, false
	}
}

func (k recentKeyCacheKey) equal(other recentKeyCacheKey) bool {
	if k.kind != other.kind {
		return false
	}
	if k.kind == 1 {
		return k.pred == other.pred
	}
	return k.text == other.text
}

func (c *recentKeyCache) initSlots(limit int) {
	if c.slots == nil {
		c.slots = recentKeyCacheSlotPool.Get()
	}
	c.slots.SetLen(limit)
}

func (c *recentKeyCache) findSlot(key recentKeyCacheKey) (int, bool) {
	for i := 0; i < c.slots.Len(); i++ {
		slot := c.slots.Get(i)
		if slot.used && slot.key.equal(key) {
			return i, true
		}
	}
	return 0, false
}

func (c *recentKeyCache) entryCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.slots == nil {
		return 0
	}
	count := 0
	for i := 0; i < c.slots.Len(); i++ {
		if c.slots.Get(i).used {
			count++
		}
	}
	return count
}

func (c *recentKeyCache) nextStamp() uint64 {
	c.clock++
	return c.clock
}

func (c *recentKeyCache) selectVictimSlot() int {
	slotIdx := -1
	oldestStamp := ^uint64(0)
	for i := 0; i < c.slots.Len(); i++ {
		slot := c.slots.Get(i)
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

func (c *recentKeyCache) touchOrRemember(key any, limit int) bool {
	cacheKey, ok := recentKeyCacheKeyFromAny(key)
	if !ok || limit <= 0 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.initSlots(limit)
	if idx, ok := c.findSlot(cacheKey); ok {
		slot := c.slots.Get(idx)
		slot.stamp = c.nextStamp()
		c.slots.Set(idx, slot)
		return true
	}
	idx := c.selectVictimSlot()
	if idx < 0 {
		return false
	}
	c.slots.Set(idx, recentKeyCacheSlot{
		key:   cacheKey,
		stamp: c.nextStamp(),
		used:  true,
	})
	return false
}

func addObservedWork(cur, delta uint64) uint64 {
	if ^uint64(0)-cur < delta {
		return ^uint64(0)
	}
	return cur + delta
}

func (c *recentKeyCache) addWorkAndShouldPromote(key any, limit int, delta uint64, threshold uint64) bool {
	cacheKey, ok := recentKeyCacheKeyFromAny(key)
	if !ok || limit <= 0 || delta == 0 || threshold == 0 {
		return false
	}
	if delta >= threshold {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.initSlots(limit)
	if idx, ok := c.findSlot(cacheKey); ok {
		slot := c.slots.Get(idx)
		slot.stamp = c.nextStamp()
		slot.work = addObservedWork(slot.work, delta)
		if slot.work < threshold {
			c.slots.Set(idx, slot)
			return false
		}
		c.slots.Set(idx, recentKeyCacheSlot{})
		return true
	}
	idx := c.selectVictimSlot()
	if idx < 0 {
		return false
	}
	c.slots.Set(idx, recentKeyCacheSlot{
		key:   cacheKey,
		stamp: c.nextStamp(),
		work:  delta,
		used:  true,
	})
	return false
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
	if e == nil || clock == nil {
		return
	}
	e.stamp.Store(clock.Add(1))
}

func (c *materializedPredCache) init(limit int) {
	if limit <= 0 {
		return
	}
	if c.slots == nil {
		c.slots = materializedPredCacheSlotPool.Get()
	}
	c.slots.SetLen(limit)
}

func (c *materializedPredCache) retain() {
	c.refs.Add(1)
}

func (c *materializedPredCache) releaseRef() {
	if c == nil || c.refs.Add(-1) != 0 {
		return
	}
	materializedPredCachePool.Put(c)
}

func (c *materializedPredCache) entryCount() int {
	if c == nil || c.slots == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	count := 0
	for i := 0; i < c.slots.Len(); i++ {
		if c.slots.Get(i).used {
			count++
		}
	}
	return count
}

func (c *materializedPredCache) lookupLocked(key materializedPredKey) (*materializedPredCacheEntry, bool) {
	if c == nil || c.slots == nil || key.isZero() {
		return nil, false
	}
	for i := 0; i < c.slots.Len(); i++ {
		slot := c.slots.Get(i)
		if slot.used && slot.key == key {
			return slot.entry, true
		}
	}
	return nil, false
}

func (c *materializedPredCache) load(key materializedPredKey, clock *atomic.Uint64) (posting.List, bool) {
	if c == nil || key.isZero() {
		return posting.List{}, false
	}
	c.mu.RLock()
	entry, ok := c.lookupLocked(key)
	c.mu.RUnlock()
	if !ok {
		return posting.List{}, false
	}
	if entry == nil {
		return posting.List{}, true
	}
	entry.touch(clock)
	return entry.ids.Borrow(), true
}

func (c *materializedPredCache) firstFreeSlotLocked() int {
	if c == nil || c.slots == nil {
		return -1
	}
	for i := 0; i < c.slots.Len(); i++ {
		if !c.slots.Get(i).used {
			return i
		}
	}
	return -1
}

func (c *materializedPredCache) retireEntryLocked(entry *materializedPredCacheEntry) {
	if c == nil || entry == nil {
		return
	}
	if c.retired == nil {
		c.retired = materializedPredCacheRetiredPool.Get()
	}
	c.retired.Append(entry)
}

func (c *materializedPredCache) drainRetired() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.retired == nil {
		return
	}
	for i := 0; i < c.retired.Len(); i++ {
		c.retired.Get(i).release()
	}
	materializedPredCacheRetiredPool.Put(c.retired)
	c.retired = nil
}

func (c *materializedPredCache) clearLocked() {
	if c == nil {
		return
	}
	if c.slots != nil {
		for i := 0; i < c.slots.Len(); i++ {
			slot := c.slots.Get(i)
			if slot.entry != nil {
				slot.entry.release()
			}
			c.slots.Set(i, materializedPredCacheSlot{})
		}
	}
	if c.retired != nil {
		for i := 0; i < c.retired.Len(); i++ {
			c.retired.Get(i).release()
		}
		materializedPredCacheRetiredPool.Put(c.retired)
		c.retired = nil
	}
}

func (c *materializedPredCache) clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.clearLocked()
	c.mu.Unlock()
}

func (c *materializedPredCache) release() {
	if c == nil {
		return
	}
	c.clear()
	if c.slots != nil {
		materializedPredCacheSlotPool.Put(c.slots)
		c.slots = nil
	}
}

func (c *materializedPredCache) findVictimLocked(mode materializedPredCacheEvictMode) int {
	if c == nil || c.slots == nil {
		return -1
	}
	evictIdx := -1
	fallbackIdx := -1
	evictStamp := ^uint64(0)
	fallbackStamp := ^uint64(0)
	for i := 0; i < c.slots.Len(); i++ {
		slot := c.slots.Get(i)
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

func (c *materializedPredCache) evictLocked(mode materializedPredCacheEvictMode, count *atomic.Int32, oversizedCount *atomic.Int32) bool {
	idx := c.findVictimLocked(mode)
	if idx < 0 {
		return false
	}
	slot := c.slots.Get(idx)
	c.slots.Set(idx, materializedPredCacheSlot{})
	if count != nil {
		count.Add(-1)
	}
	if slot.entry != nil && slot.entry.oversized && oversizedCount != nil {
		oversizedCount.Add(-1)
	}
	c.retireEntryLocked(slot.entry)
	return true
}

func (c *materializedPredCache) insertLocked(key materializedPredKey, entry *materializedPredCacheEntry) bool {
	if c == nil || c.slots == nil || key.isZero() {
		return false
	}
	idx := c.firstFreeSlotLocked()
	if idx < 0 {
		return false
	}
	c.slots.Set(idx, materializedPredCacheSlot{
		key:   key,
		entry: entry,
		used:  true,
	})
	return true
}

func appendRetiredSnapshot(buf *pooled.Slice[*indexSnapshot], snap *indexSnapshot) *pooled.Slice[*indexSnapshot] {
	if snap == nil {
		return buf
	}
	if buf == nil {
		buf = snapshotRetiredListPool.Get()
	}
	buf.Append(snap)
	return buf
}

func releaseRetiredSnapshots(buf *pooled.Slice[*indexSnapshot]) {
	if buf == nil {
		return
	}
	for i := 0; i < buf.Len(); i++ {
		retired := buf.Get(i)
		if retired == nil {
			continue
		}
		retired.releaseStorage()
		retired.releaseRuntimeCaches()
	}
	snapshotRetiredListPool.Put(buf)
}

func inheritNumericRangeBucketCache(next, prev *indexSnapshot) {
	if next == nil {
		return
	}
	if prev == nil || prev.numericRangeBucketCache == nil {
		return
	}
	if next.numericRangeBucketCache == nil || next.numericRangeBucketCache.slots == nil {
		return
	}
	prev.numericRangeBucketCache.mu.Lock()
	defer prev.numericRangeBucketCache.mu.Unlock()
	for i := 0; i < prev.numericRangeBucketCache.slots.Len(); i++ {
		slot := prev.numericRangeBucketCache.slots.Get(i)
		if slot.field == "" || slot.entry == nil || slot.entry.storage.KeyCount() == 0 {
			continue
		}
		acc, ok := next.indexedFieldByName[slot.field]
		if !ok || next.index == nil || acc.ordinal >= len(next.index) {
			continue
		}
		nextStorage := next.index[acc.ordinal]
		if nextStorage != slot.entry.storage {
			continue
		}
		slot.entry.retain()
		next.numericRangeBucketCache.storeSlot(slot.field, acc.ordinal, slot.entry)
	}
}

func inheritMaterializedPredCache(next, prev *indexSnapshot, indexedFieldMap indexedFieldMap, changedFields []bool) {
	if next == nil || prev == nil {
		return
	}
	limit := next.materializedPredCacheLimit()
	if limit <= 0 || prev.matPredCacheCount.Load() == 0 || prev.matPredCache == nil || next.matPredCache == nil {
		return
	}

	var oversized int32
	var maxStamp uint64
	prev.matPredCache.mu.RLock()
	next.matPredCache.mu.Lock()
	for i := 0; i < prev.matPredCache.slots.Len(); i++ {
		if int(next.matPredCacheCount.Load()) >= limit {
			break
		}
		slot := prev.matPredCache.slots.Get(i)
		if !slot.used {
			continue
		}
		key := slot.key
		f := key.field
		if f == "" {
			continue
		}
		if changedFields != nil {
			acc, ok := indexedFieldMap[f]
			if !ok || changedFields[acc.ordinal] {
				continue
			}
		}
		if _, exists := next.matPredCache.lookupLocked(key); exists {
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
		if !next.matPredCache.insertLocked(key, slot.entry) {
			if slot.entry != nil {
				slot.entry.release()
			}
			break
		}
		next.matPredCacheCount.Add(1)
	}
	next.matPredCache.mu.Unlock()
	prev.matPredCache.mu.RUnlock()
	if oversized > 0 {
		next.matPredCacheOversizedCount.Store(min(oversized, materializedPredCacheOversizedLimit(limit)))
	}
	if maxStamp > next.matPredCacheClock.Load() {
		next.matPredCacheClock.Store(maxStamp)
	}
}

func materializedPredCacheKeyFromString(key string) (materializedPredKey, bool) {
	if key == "" {
		return materializedPredKey{}, false
	}
	if parsed, ok := materializedPredKeyFromEncoded(key); ok {
		return parsed, true
	}
	return materializedPredKey{
		kind: materializedPredKeyOpaque,
		raw:  key,
	}, true
}

func (s *indexSnapshot) loadMaterializedPredKey(key materializedPredKey) (posting.List, bool) {
	if s == nil || key.isZero() || s.matPredCache == nil {
		return posting.List{}, false
	}
	if s.materializedPredCacheLimit() <= 0 {
		return posting.List{}, false
	}
	return s.matPredCache.load(key, &s.matPredCacheClock)
}

func (s *indexSnapshot) loadMaterializedPred(key string) (posting.List, bool) {
	parsed, ok := materializedPredCacheKeyFromString(key)
	if !ok {
		return posting.List{}, false
	}
	return s.loadMaterializedPredKey(parsed)
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

func (s *indexSnapshot) storeMaterializedPredKey(key materializedPredKey, ids posting.List) {
	if key.isZero() || s == nil || s.matPredCache == nil {
		return
	}
	limit := s.materializedPredCacheLimit()
	if limit <= 0 {
		return
	}
	if !ids.IsEmpty() && s.matPredCacheMaxCard > 0 &&
		ids.Cardinality() > s.matPredCacheMaxCard {
		return
	}
	s.matPredCache.mu.Lock()
	if _, ok := s.matPredCache.lookupLocked(key); ok {
		s.matPredCache.mu.Unlock()
		return
	}
	if int(s.matPredCacheCount.Load()) >= limit &&
		!s.matPredCache.evictLocked(matPredCacheEvictPreferRegular, &s.matPredCacheCount, &s.matPredCacheOversizedCount) {
		s.matPredCache.mu.Unlock()
		return
	}
	stored := materializedPredCacheStoredIDs(ids)
	entry := newMaterializedPredCacheEntry(stored, false, &s.matPredCacheClock)
	if !s.matPredCache.insertLocked(key, entry) {
		s.matPredCache.mu.Unlock()
		entry.release()
		return
	}
	s.matPredCacheCount.Add(1)
	s.matPredCache.mu.Unlock()
}

func (s *indexSnapshot) storeMaterializedPred(key string, ids posting.List) {
	parsed, ok := materializedPredCacheKeyFromString(key)
	if !ok {
		return
	}
	s.storeMaterializedPredKey(parsed, ids)
}

// tryStoreMaterializedPredOversized stores a small bounded number of oversized
// materialized postings per snapshot as a hot-cache fallback.
func (s *indexSnapshot) tryStoreMaterializedPredOversizedKey(key materializedPredKey, ids posting.List) bool {
	if key.isZero() || ids.IsEmpty() || s == nil || s.matPredCache == nil {
		return false
	}
	if s.matPredCacheMaxCard == 0 || ids.Cardinality() <= s.matPredCacheMaxCard {
		return false
	}
	limit := s.materializedPredCacheLimit()
	if limit <= 0 {
		return false
	}
	s.matPredCache.mu.Lock()
	if _, ok := s.matPredCache.lookupLocked(key); ok {
		s.matPredCache.mu.Unlock()
		return false
	}
	if s.matPredCacheOversizedCount.Load() >= materializedPredCacheOversizedLimit(limit) &&
		!s.matPredCache.evictLocked(matPredCacheEvictOversizedOnly, &s.matPredCacheCount, &s.matPredCacheOversizedCount) {
		s.matPredCache.mu.Unlock()
		return false
	}
	if int(s.matPredCacheCount.Load()) >= limit &&
		!s.matPredCache.evictLocked(matPredCacheEvictPreferRegular, &s.matPredCacheCount, &s.matPredCacheOversizedCount) {
		s.matPredCache.mu.Unlock()
		return false
	}
	stored := materializedPredCacheStoredIDs(ids)
	entry := newMaterializedPredCacheEntry(stored, true, &s.matPredCacheClock)
	if !s.matPredCache.insertLocked(key, entry) {
		s.matPredCache.mu.Unlock()
		entry.release()
		return false
	}
	s.matPredCacheCount.Add(1)
	s.matPredCacheOversizedCount.Add(1)
	s.matPredCache.mu.Unlock()
	return true
}

func (s *indexSnapshot) tryStoreMaterializedPredOversized(key string, ids posting.List) bool {
	parsed, ok := materializedPredCacheKeyFromString(key)
	if !ok {
		return false
	}
	return s.tryStoreMaterializedPredOversizedKey(parsed, ids)
}

func (s *indexSnapshot) loadOrStoreMaterializedPredKey(key materializedPredKey, ids posting.List) (posting.List, bool) {
	if key.isZero() || ids.IsEmpty() || s == nil || s.matPredCache == nil {
		return ids, false
	}

	limit := s.materializedPredCacheLimit()
	if limit <= 0 {
		return ids, false
	}

	if s.matPredCacheMaxCard > 0 &&
		ids.Cardinality() > s.matPredCacheMaxCard {
		return ids, false
	}

	if cached, ok := s.loadMaterializedPredKey(key); ok {
		ids.Release()
		return cached, true
	}

	s.matPredCache.mu.Lock()

	if entry, ok := s.matPredCache.lookupLocked(key); ok {
		s.matPredCache.mu.Unlock()
		ids.Release()
		if entry == nil {
			return posting.List{}, true
		}
		entry.touch(&s.matPredCacheClock)
		return entry.ids.Borrow(), true
	}

	if int(s.matPredCacheCount.Load()) >= limit &&
		!s.matPredCache.evictLocked(matPredCacheEvictPreferRegular, &s.matPredCacheCount, &s.matPredCacheOversizedCount) {
		s.matPredCache.mu.Unlock()
		return ids, false
	}
	stored := materializedPredCacheStoredIDs(ids)
	entry := newMaterializedPredCacheEntry(stored, false, &s.matPredCacheClock)

	if !s.matPredCache.insertLocked(key, entry) {
		s.matPredCache.mu.Unlock()
		entry.release()
		return ids, false
	}

	s.matPredCacheCount.Add(1)
	s.matPredCache.mu.Unlock()

	return stored.Borrow(), true
}

func (s *indexSnapshot) tryLoadOrStoreMaterializedPredOversizedKey(key materializedPredKey, ids posting.List) (posting.List, bool) {
	if key.isZero() || ids.IsEmpty() || s == nil || s.matPredCache == nil {
		return ids, false
	}

	if s.matPredCacheMaxCard == 0 || ids.Cardinality() <= s.matPredCacheMaxCard {
		return ids, false
	}

	limit := s.materializedPredCacheLimit()
	if limit <= 0 {
		return ids, false
	}

	if cached, ok := s.loadMaterializedPredKey(key); ok {
		ids.Release()
		return cached, true
	}

	s.matPredCache.mu.Lock()

	if entry, ok := s.matPredCache.lookupLocked(key); ok {
		s.matPredCache.mu.Unlock()
		ids.Release()
		if entry == nil {
			return posting.List{}, true
		}
		entry.touch(&s.matPredCacheClock)
		return entry.ids.Borrow(), true
	}

	if s.matPredCacheOversizedCount.Load() >= materializedPredCacheOversizedLimit(limit) &&
		!s.matPredCache.evictLocked(matPredCacheEvictOversizedOnly, &s.matPredCacheCount, &s.matPredCacheOversizedCount) {
		s.matPredCache.mu.Unlock()
		return ids, false
	}

	if int(s.matPredCacheCount.Load()) >= limit &&
		!s.matPredCache.evictLocked(matPredCacheEvictPreferRegular, &s.matPredCacheCount, &s.matPredCacheOversizedCount) {
		s.matPredCache.mu.Unlock()
		return ids, false
	}

	stored := materializedPredCacheStoredIDs(ids)
	entry := newMaterializedPredCacheEntry(stored, true, &s.matPredCacheClock)
	if !s.matPredCache.insertLocked(key, entry) {
		s.matPredCache.mu.Unlock()
		entry.release()
		return ids, false
	}

	s.matPredCacheCount.Add(1)
	s.matPredCacheOversizedCount.Add(1)

	s.matPredCache.mu.Unlock()

	return stored.Borrow(), true
}

func (s *indexSnapshot) materializedPredCacheLimit() int {
	if s == nil {
		return 0
	}
	return s.matPredCacheMaxEntries
}

func (s *indexSnapshot) clearRuntimeCachesForTesting() {
	if s == nil {
		return
	}
	if s.numericRangeBucketCache != nil {
		s.numericRangeBucketCache.clearEntries()
	}
	if s.matPredCache != nil {
		s.matPredCache.clear()
	}
	s.matPredCacheCount.Store(0)
	s.matPredCacheOversizedCount.Store(0)
	s.matPredCacheClock.Store(0)
	s.runtimeMatPredSeen.clear()
	s.orderORMatPredObserved.clear()
}

func (s *indexSnapshot) drainRetiredRuntimeCaches() {
	if s == nil {
		return
	}
	if s.matPredCache != nil {
		s.matPredCache.drainRetired()
	}
}

func (s *indexSnapshot) releaseRuntimeCaches() {
	if s == nil {
		return
	}
	if s.numericRangeBucketCache != nil {
		numericRangeBucketCachePool.Put(s.numericRangeBucketCache)
		s.numericRangeBucketCache = nil
	}
	if s.matPredCache != nil {
		s.matPredCache.releaseRef()
		s.matPredCache = nil
	}
	s.runtimeMatPredSeen.clear()
	s.orderORMatPredObserved.clear()
	s.matPredCacheCount.Store(0)
	s.matPredCacheOversizedCount.Store(0)
	s.matPredCacheClock.Store(0)
}

func (s *indexSnapshot) shouldPromoteRuntimeMaterializedPredKey(key materializedPredKey) bool {
	if key.isZero() {
		return false
	}
	return s.runtimeMatPredSeen.touchOrRemember(key, recentKeyCacheLimit(s.matPredCacheMaxEntries))
}

func (s *indexSnapshot) shouldPromoteRuntimeMaterializedPred(key string) bool {
	if parsed, ok := materializedPredKeyFromEncoded(key); ok {
		return s.shouldPromoteRuntimeMaterializedPredKey(parsed)
	}
	if s == nil || key == "" {
		return false
	}
	return s.runtimeMatPredSeen.touchOrRemember(key, recentKeyCacheLimit(s.matPredCacheMaxEntries))
}

func (s *indexSnapshot) shouldPromoteObservedOrderedORMaterializedPredKey(key materializedPredKey, observedWork uint64, buildWork uint64) bool {
	if key.isZero() || observedWork == 0 || buildWork == 0 {
		return false
	}
	return s.orderORMatPredObserved.addWorkAndShouldPromote(
		key,
		recentKeyCacheLimit(s.matPredCacheMaxEntries),
		observedWork,
		buildWork,
	)
}

func (s *indexSnapshot) shouldPromoteObservedOrderedORMaterializedPred(key string, observedWork uint64, buildWork uint64) bool {
	if parsed, ok := materializedPredKeyFromEncoded(key); ok {
		return s.shouldPromoteObservedOrderedORMaterializedPredKey(parsed, observedWork, buildWork)
	}
	if s == nil || key == "" || observedWork == 0 || buildWork == 0 {
		return false
	}
	return s.orderORMatPredObserved.addWorkAndShouldPromote(
		key,
		recentKeyCacheLimit(s.matPredCacheMaxEntries),
		observedWork,
		buildWork,
	)
}

// clearCurrentSnapshotCachesForTesting drops runtime caches in-place on the
// currently published snapshot. It is intended for serial tests/benchmarks
// that want cold-cache query behavior without publishing a new snapshot.
func (db *DB[K, V]) clearCurrentSnapshotCachesForTesting() {
	if db == nil {
		return
	}
	db.engine.getSnapshot().clearRuntimeCachesForTesting()
}

func (s *indexSnapshot) fieldIndexStorage(field string) (indexdata.FieldStorage, bool) {
	if s == nil || s.index == nil {
		return indexdata.FieldStorage{}, false
	}
	acc, ok := s.indexedFieldByName[field]
	if !ok || acc.ordinal >= len(s.index) {
		return indexdata.FieldStorage{}, false
	}
	storage := s.index[acc.ordinal]
	return storage, storage.KeyCount() > 0
}

func (s *indexSnapshot) nilFieldNameSet() map[string]struct{} {
	if s == nil || s.nilIndex == nil {
		return nil
	}
	fields := make(map[string]struct{}, len(s.indexedFieldByName))
	for f, acc := range s.indexedFieldByName {
		if acc.ordinal < len(s.nilIndex) && s.nilIndex[acc.ordinal].KeyCount() > 0 {
			fields[f] = struct{}{}
		}
	}
	return fields
}

func (s *indexSnapshot) fieldNameSet() map[string]struct{} {
	if s == nil || s.index == nil {
		return nil
	}
	fields := make(map[string]struct{}, len(s.indexedFieldByName))
	for f, acc := range s.indexedFieldByName {
		if acc.ordinal < len(s.index) && s.index[acc.ordinal].KeyCount() > 0 {
			fields[f] = struct{}{}
		}
	}
	return fields
}

func (s *indexSnapshot) lenFieldNameSet() map[string]struct{} {
	if s == nil || s.lenIndex == nil {
		return nil
	}
	fields := make(map[string]struct{}, len(s.indexedFieldByName))
	for f, acc := range s.indexedFieldByName {
		if !acc.field.Slice || acc.ordinal >= len(s.lenIndex) || s.lenIndex[acc.ordinal].KeyCount() == 0 {
			continue
		}
		fields[f] = struct{}{}
	}
	return fields
}

func (s *indexSnapshot) fieldLookupPostingRetained(field, key string) posting.List {
	if s == nil || s.index == nil {
		return posting.List{}
	}
	acc, ok := s.indexedFieldByName[field]
	if !ok || acc.ordinal >= len(s.index) {
		return posting.List{}
	}
	return indexdata.NewFieldOverlayStorage(s.index[acc.ordinal]).LookupPostingRetained(key)
}

func (s *indexSnapshot) fieldLookupPostingRetainedKey(field string, key keycodec.IndexLookupKey) posting.List {
	if s == nil || s.index == nil {
		return posting.List{}
	}
	acc, ok := s.indexedFieldByName[field]
	if !ok || acc.ordinal >= len(s.index) {
		return posting.List{}
	}
	ov := indexdata.NewFieldOverlayStorage(s.index[acc.ordinal])
	if key.IsNumeric() {
		return ov.LookupPostingRetainedKey(key.IndexKey())
	}
	return ov.LookupPostingRetained(key.StringKey())
}

func (sm *snapshotManager) publishRef(s *indexSnapshot) *pooled.Slice[*indexSnapshot] {
	if s == nil {
		return nil
	}
	sm.mu.Lock()

	prev := sm.current.Load()
	ref := sm.bySeq[s.seq]
	if ref == nil {
		ref = snapshotRefPool.Get()
		sm.bySeq[s.seq] = ref
	}
	var retired *pooled.Slice[*indexSnapshot]
	if prev != nil && prev.seq == s.seq && ref.snap != nil && ref.snap != s {
		if ref.refs.Load() != 0 {
			ref.retired = appendRetiredSnapshot(ref.retired, ref.snap)
		} else {
			retired = appendRetiredSnapshot(retired, ref.snap)
		}
	}
	ref.snap = s
	sm.current.Store(s)
	sm.currentRef.Store(ref)
	if prev != nil && prev.seq != s.seq {
		retired = sm.retireSnapshotLocked(prev.seq)
	}
	sm.mu.Unlock()
	return retired
}

func (sm *snapshotManager) stage(s *indexSnapshot) {
	if s == nil {
		return
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()

	ref := sm.bySeq[s.seq]
	if ref == nil {
		ref = snapshotRefPool.Get()
		sm.bySeq[s.seq] = ref
	}
	ref.snap = s
}

func (sm *snapshotManager) dropStaged(seq uint64) {
	sm.mu.Lock()

	ref := sm.bySeq[seq]
	if ref == nil {
		sm.mu.Unlock()
		return
	}
	if current := sm.current.Load(); current != nil && current.seq == seq {
		sm.mu.Unlock()
		return
	}
	var retired *pooled.Slice[*indexSnapshot]
	if ref.refs.Load() <= 0 {
		retired = sm.releaseRetiredSnapshotRefLocked(seq, ref)
		sm.mu.Unlock()
		releaseRetiredSnapshots(retired)
		return
	}
	ref.retired = appendRetiredSnapshot(ref.retired, ref.snap)
	ref.snap = nil
	sm.mu.Unlock()
}

func (sm *snapshotManager) pinRefBySeq(seq uint64) (*indexSnapshot, *snapshotRef, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	ref := sm.bySeq[seq]
	if ref == nil || ref.snap == nil {
		return nil, nil, false
	}
	ref.refs.Add(1)
	return ref.snap, ref, true
}

func (sm *snapshotManager) unpinRef(seq uint64, ref *snapshotRef) {
	if ref == nil {
		return
	}
	if ref.refs.Add(-1) != 0 {
		return
	}
	var (
		drainCache *materializedPredCache
		retired    *pooled.Slice[*indexSnapshot]
	)

	sm.mu.Lock()
	held := sm.bySeq[seq]
	if held != ref || held.refs.Load() != 0 || held.snap != nil {
		if held == ref && held != nil && held.refs.Load() == 0 && held.snap != nil &&
			held == sm.currentRef.Load() && held.retired != nil {
			retired = held.retired
			held.retired = nil
		}
		if held == ref && held != nil && held.snap != nil &&
			held == sm.currentRef.Load() {
			drainCache = held.snap.matPredCache
			if drainCache != nil {
				drainCache.retain()
			}
		}
		sm.mu.Unlock()

		releaseRetiredSnapshots(retired)
		if drainCache != nil {
			drainCache.drainRetired()
			drainCache.releaseRef()
		}
		return
	}

	retired = sm.releaseRetiredSnapshotRefLocked(seq, held)

	sm.mu.Unlock()

	releaseRetiredSnapshots(retired)
}

func (sm *snapshotManager) retireSnapshotLocked(seq uint64) *pooled.Slice[*indexSnapshot] {
	ref := sm.bySeq[seq]
	if ref == nil {
		return nil
	}
	if current := sm.current.Load(); current != nil && current.seq == seq {
		return nil
	}
	if ref.refs.Load() != 0 {
		ref.retired = appendRetiredSnapshot(ref.retired, ref.snap)
		ref.snap = nil
		return nil
	}
	return sm.releaseRetiredSnapshotRefLocked(seq, ref)
}

func (sm *snapshotManager) releaseRetiredSnapshotRefLocked(seq uint64, ref *snapshotRef) *pooled.Slice[*indexSnapshot] {
	if ref == nil {
		return nil
	}
	if current := sm.current.Load(); current != nil && current.seq == seq {
		return nil
	}
	if ref.refs.Load() != 0 {
		return nil
	}
	retired := ref.retired
	if ref.snap != nil {
		retired = appendRetiredSnapshot(retired, ref.snap)
	}
	ref.snap = nil
	ref.retired = nil
	delete(sm.bySeq, seq)
	if sm.currentRef.Load() == ref {
		sm.currentRef.Store(nil)
	}
	snapshotRefPool.Put(ref)
	return retired
}

// SnapshotStats returns diagnostics for published index snapshots.
//
// Runtime snapshot diagnostics are collected only when
// Options.EnableSnapshotStats was enabled for this DB instance; otherwise the
// method returns a zero value.
//
// In indexed mode it reports the current published snapshot sequence,
// universe cardinality, registry size, and pin counts.
//
// In transparent mode no published index snapshots are maintained, so the
// returned diagnostics remain zero-valued even when snapshot stats collection
// is enabled.
func (db *DB[K, V]) SnapshotStats() SnapshotStats {
	if db.engine == nil {
		return SnapshotStats{}
	}
	if !db.engine.snapshot.statsEnabled {
		return SnapshotStats{}
	}
	if !db.beginOpWait() {
		return SnapshotStats{}
	}
	defer db.endOp()

	s, seq, ref, pinned := db.engine.pinCurrentSnapshot()
	defer db.engine.unpinCurrentSnapshot(seq, ref, pinned)

	diag := SnapshotStats{
		Sequence: s.seq,
	}
	if !s.universe.IsEmpty() {
		diag.UniverseCard = s.universe.Cardinality()
	}

	db.engine.snapshot.mu.RLock()
	diag.RegistrySize = len(db.engine.snapshot.bySeq)
	for _, held := range db.engine.snapshot.bySeq {
		refs := held.refs.Load()
		if pinned && held == ref {
			refs--
		}
		if refs > 0 {
			diag.PinnedRefs++
		}
	}
	db.engine.snapshot.mu.RUnlock()

	return diag
}

func unionPostingListsOwned(base, add posting.List) posting.List {
	merged := base.Clone()
	merged = merged.BuildMergedOwned(add)
	return merged
}

func addFieldPostingList(fieldMap map[string]posting.List, key string, idx uint64) map[string]posting.List {
	if fieldMap == nil {
		fieldMap = indexdata.GetPostingMap()
	}
	p := fieldMap[key]
	p = p.BuildAdded(idx)
	fieldMap[key] = p
	return fieldMap
}

func addFixedFieldPostingList(fieldMap map[uint64]posting.List, key uint64, idx uint64) map[uint64]posting.List {
	if fieldMap == nil {
		fieldMap = indexdata.GetFixedPostingMap()
	}
	p := fieldMap[key]
	p = p.BuildAdded(idx)
	fieldMap[key] = p
	return fieldMap
}

func (qe *queryEngine) buildPreparedSnapshotNoLock(seq uint64, strMap *strMapper, patchMap map[string]*field, entries []snapshotBatchEntry) *indexSnapshot {
	prev := qe.getSnapshot()
	if snap, ok := qe.buildPreparedSnapshotFromEmptyBaseNoLock(seq, prev, strMap, entries); ok {
		return snap
	}
	if snap, ok := qe.buildPreparedSnapshotInsertOnlyNoLock(seq, prev, strMap, entries); ok {
		return snap
	}
	return qe.buildPreparedSnapshotAggregatedNoLock(seq, prev, strMap, patchMap, entries)
}

func (qe *queryEngine) buildPreparedSnapshotFromEmptyBaseNoLock(seq uint64, prev *indexSnapshot, strMap *strMapper, entries []snapshotBatchEntry) (*indexSnapshot, bool) {
	if prev != nil && !prev.universe.IsEmpty() {
		return nil, false
	}
	if len(entries) == 0 {
		return nil, false
	}
	for i := range entries {
		if entries[i].oldVal != nil || entries[i].newVal == nil {
			return nil, false
		}
	}

	var universe posting.List
	hasRepeated := false
	for i := range entries {
		var added bool
		universe, added = universe.BuildAddedChecked(entries[i].idx)
		if !added {
			hasRepeated = true
		}
	}

	var lastByIdx map[uint64]int
	if hasRepeated {
		lastByIdx = uint64IntMapPool.Get(len(entries))
		for i := range entries {
			lastByIdx[entries[i].idx] = i
		}
	}

	fieldStates := make([]snapshotFieldOverlayState, len(qe.indexedFieldAccess))
	measureStates := make([][]indexdata.MeasureEntry, len(qe.measureFieldAccess))

	for i := range entries {
		if hasRepeated && lastByIdx[entries[i].idx] != i {
			continue
		}
		op := entries[i]
		ptr := op.newVal

		for _, acc := range qe.indexedFieldAccess {
			acc.collectSnapshotOverlayValue(ptr, op.idx, &fieldStates[acc.ordinal])
		}
		for _, acc := range qe.measureFieldAccess {
			if value, ok := acc.read(ptr); ok {
				buf := measureStates[acc.ordinal]
				if buf == nil {
					buf = indexdata.GetMeasureEntrySlice(0)
					measureStates[acc.ordinal] = buf
				}
				buf = append(buf, indexdata.MeasureEntry{ID: op.idx, Value: value})
				measureStates[acc.ordinal] = buf
			}
		}
	}
	if hasRepeated {
		uint64IntMapPool.Put(lastByIdx)
	}

	slotCount := len(qe.indexedFieldAccess)
	nextIndex := indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	for i, acc := range qe.indexedFieldAccess {
		state := &fieldStates[i]
		var storage indexdata.FieldStorage
		if acc.field != nil && acc.field.KeyKind == fieldWriteKeysOrderedU64 {
			fixed := state.fixed
			state.fixed = nil
			storage = indexdata.NewRegularFieldStorageFromFixedPostingMapOwned(fixed)
		} else {
			idx := state.index
			state.index = nil
			storage = indexdata.NewRegularFieldStorageFromPostingMapOwned(idx, false)
		}
		if storage.KeyCount() > 0 {
			nextIndex[i] = storage
		}
	}

	nextNilIndex := indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	for i := range qe.indexedFieldAccess {
		state := &fieldStates[i]
		nils := state.nils
		state.nils = nil
		if storage := indexdata.NewFlatFieldStorageFromPostingMapOwned(nils, false); storage.KeyCount() > 0 {
			nextNilIndex[i] = storage
		}
	}

	nextLenIndex := indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	nextLenZeroComplement := pooled.GetBoolSlice(slotCount)[:slotCount]
	clear(nextLenZeroComplement)
	for i, acc := range qe.indexedFieldAccess {
		if !acc.field.Slice {
			continue
		}
		lengths := fieldStates[i].lengths
		fieldStates[i].lengths = nil
		storage, useZeroComplement := indexdata.NewLenFieldStorageFromMapOwned(universe, lengths)
		nextLenIndex[i] = storage
		if useZeroComplement {
			nextLenZeroComplement[i] = true
		}
	}
	measureSlotCount := len(qe.measureFieldAccess)
	nextMeasure := indexdata.GetMeasureStorageSlice(measureSlotCount)[:measureSlotCount]
	for i := range qe.measureFieldAccess {
		storage := indexdata.NewMeasureStorageFromEntriesOwned(measureStates[i])
		nextMeasure[i] = storage
		measureStates[i] = nil
	}

	var strmap *strMapSnapshot
	if strMap != nil {
		strmap = strMap.snapshot()
	}
	snap := &indexSnapshot{
		seq:                seq,
		index:              nextIndex,
		nilIndex:           nextNilIndex,
		lenIndex:           nextLenIndex,
		lenZeroComplement:  nextLenZeroComplement,
		measure:            nextMeasure,
		indexedFieldByName: qe.indexedFieldMap,
		universe:           universe,
		strmap:             strmap,
	}
	qe.initSnapshotRuntimeCaches(snap)
	inheritNumericRangeBucketCache(snap, prev)
	changedCount := 0
	for i := range fieldStates {
		if fieldStates[i].changed {
			changedCount++
		}
	}
	if changedCount > 0 {
		changed := pooled.GetBoolSlice(len(qe.indexedFieldAccess))[:len(qe.indexedFieldAccess)]
		clear(changed)
		for i := range fieldStates {
			if fieldStates[i].changed {
				changed[i] = true
			}
		}
		inheritMaterializedPredCache(snap, prev, qe.indexedFieldMap, changed)
		pooled.ReleaseBoolSlice(changed)
	} else {
		inheritMaterializedPredCache(snap, prev, qe.indexedFieldMap, nil)
	}
	snap.ensureUniverseOwner()
	return snap, true
}

func (qe *queryEngine) buildPreparedSnapshotInsertOnlyNoLock(seq uint64, prev *indexSnapshot, strMap *strMapper, entries []snapshotBatchEntry) (*indexSnapshot, bool) {
	if len(entries) == 0 {
		return nil, false
	}
	for i := range entries {
		if entries[i].oldVal != nil || entries[i].newVal == nil {
			return nil, false
		}
	}
	var addedUniverse posting.List
	for i := range entries {
		var added bool
		addedUniverse, added = addedUniverse.BuildAddedChecked(entries[i].idx)
		if !added {
			addedUniverse.Release()
			return nil, false
		}
	}

	var strmap *strMapSnapshot
	if strMap != nil {
		strmap = strMap.snapshot()
	}
	next := &indexSnapshot{
		seq: seq,

		index:              indexdata.CloneFieldStorageSlots(prev.index, len(qe.indexedFieldAccess)),
		nilIndex:           indexdata.CloneFieldStorageSlots(prev.nilIndex, len(qe.indexedFieldAccess)),
		lenIndex:           indexdata.CloneFieldStorageSlots(prev.lenIndex, len(qe.indexedFieldAccess)),
		lenZeroComplement:  cloneFieldIndexBoolSlots(prev.lenZeroComplement, len(qe.indexedFieldAccess)),
		measure:            indexdata.CloneMeasureStorageSlots(prev.measure, len(qe.measureFieldAccess)),
		indexedFieldByName: qe.indexedFieldMap,
		universe:           prev.universe.Clone(),
		strmap:             strmap,
	}
	next.universe = next.universe.BuildMergedOwned(addedUniverse)
	qe.initSnapshotRuntimeCaches(next)

	fieldStates := snapshotFieldInsertStateSlicePool.Get(len(qe.indexedFieldAccess))
	fieldStates = fieldStates[:len(qe.indexedFieldAccess)]
	initSnapshotFieldInsertStateHints(fieldStates, qe.indexedFieldAccess, prev, len(entries))
	measureDeltas := indexdata.NewMeasureDeltaBatch(len(qe.measureFieldAccess))

	for i := range entries {
		op := entries[i]
		ptr := op.newVal

		for _, acc := range qe.indexedFieldAccess {
			useZeroComplement := acc.ordinal < len(prev.lenZeroComplement) && prev.lenZeroComplement[acc.ordinal]
			acc.collectSnapshotInsertValue(ptr, op.idx, useZeroComplement, &fieldStates[acc.ordinal])
		}
		for _, acc := range qe.measureFieldAccess {
			if value, ok := acc.read(ptr); ok {
				measureDeltas.Append(acc.ordinal, op.idx, true, value)
			}
		}
	}

	changedCount := 0
	var changed []bool
	for i, acc := range qe.indexedFieldAccess {
		state := &fieldStates[i]
		baseIndex := next.index[i]
		if storage := acc.mergeSnapshotInsertStorageOwned(baseIndex, state, true); storage.KeyCount() > 0 {
			if storage != baseIndex {
				next.index[i] = storage
			}
		} else if baseIndex.KeyCount() > 0 {
			next.index[i] = indexdata.FieldStorage{}
		}
		baseNil := next.nilIndex[i]
		if storage := acc.mergeSnapshotInsertNilStorageOwned(baseNil, state); storage.KeyCount() > 0 {
			if storage != baseNil {
				next.nilIndex[i] = storage
			}
		} else if baseNil.KeyCount() > 0 {
			next.nilIndex[i] = indexdata.FieldStorage{}
		}
		if state.lengths != nil {
			baseLen := next.lenIndex[i]
			if storage := baseLen.ApplyLenPostingDiffRetainOwned(state.lengths); storage != baseLen {
				next.lenIndex[i] = storage
			}
		}
		if state.changed {
			changedCount++
			if changed == nil {
				changed = pooled.GetBoolSlice(len(qe.indexedFieldAccess))[:len(qe.indexedFieldAccess)]
				clear(changed)
			}
			changed[i] = true
		}
		state.reset()
	}
	measureDeltas.ApplyToMeasureStorageSlotsOwned(next.measure)
	measureDeltas.Release()
	inheritNumericRangeBucketCache(next, prev)

	if changedCount > 0 {
		inheritMaterializedPredCache(next, prev, qe.indexedFieldMap, changed)
		pooled.ReleaseBoolSlice(changed)
	} else {
		inheritMaterializedPredCache(next, prev, qe.indexedFieldMap, nil)
	}
	next.retainSharedOwnedStorageFrom(prev)
	snapshotFieldInsertStateSlicePool.Put(fieldStates)

	return next, true
}

/**/

type snapshotPostingOwner struct {
	refs atomic.Int32
	ids  posting.List
}

func newSnapshotPostingOwner(ids posting.List) *snapshotPostingOwner {
	owner := &snapshotPostingOwner{
		ids: ids,
	}
	if owner.ids.IsBorrowed() {
		owner.ids = owner.ids.Clone()
	}
	owner.refs.Store(1)
	return owner
}

func (o *snapshotPostingOwner) retain() {
	o.refs.Add(1)
}

func (o *snapshotPostingOwner) release() {
	if o == nil || o.refs.Add(-1) != 0 {
		return
	}
	o.ids.Release()
}

func (s *indexSnapshot) ensureUniverseOwner() {
	if s == nil || s.universeOwner != nil {
		return
	}
	s.universeOwner = newSnapshotPostingOwner(s.universe)
	s.universe = s.universeOwner.ids
}

func (s *indexSnapshot) retainSharedOwnedStorageFrom(prev *indexSnapshot) {
	if s == nil {
		return
	}
	if prev != nil && s.universeOwner == nil && prev.universeOwner != nil && s.universe == prev.universe {
		s.universeOwner = prev.universeOwner
	}
	if s.universeOwner != nil {
		if prev != nil && s.universeOwner == prev.universeOwner {
			s.universeOwner.retain()
		}
		s.universe = s.universeOwner.ids
	} else {
		s.ensureUniverseOwner()
	}
	if prev != nil {
		indexdata.RetainSharedFieldStorageSlots(s.index, prev.index)
		indexdata.RetainSharedFieldStorageSlots(s.nilIndex, prev.nilIndex)
		indexdata.RetainSharedFieldStorageSlots(s.lenIndex, prev.lenIndex)
		indexdata.RetainSharedMeasureStorageSlots(s.measure, prev.measure)
	}
}

func (s *indexSnapshot) releaseStorage() {
	if s == nil {
		return
	}
	if s.universeOwner != nil {
		s.universeOwner.release()
	}
	indexdata.ReleaseFieldStorageSlots(s.index)
	indexdata.ReleaseFieldStorageSlots(s.nilIndex)
	indexdata.ReleaseFieldStorageSlots(s.lenIndex)
	indexdata.ReleaseMeasureStorageSlots(s.measure)
	if s.lenZeroComplement != nil {
		pooled.ReleaseBoolSlice(s.lenZeroComplement)
	}
	s.index = nil
	s.nilIndex = nil
	s.lenIndex = nil
	s.measure = nil
	s.lenZeroComplement = nil
}
