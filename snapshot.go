package rbi

import (
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

// indexSnapshot is an immutable read-view published atomically for query paths.
type indexSnapshot struct {
	seq uint64

	index              *pooled.SliceBuf[fieldIndexStorage]
	nilIndex           *pooled.SliceBuf[fieldIndexStorage]
	lenIndex           *pooled.SliceBuf[fieldIndexStorage]
	lenZeroComplement  *pooled.SliceBuf[bool]
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
	slots   *pooled.SliceBuf[materializedPredCacheSlot]
	retired *pooled.SliceBuf[*materializedPredCacheEntry]
}

type materializedPredCacheSlot struct {
	key   materializedPredKey
	entry *materializedPredCacheEntry
	used  bool
}

type recentKeyCache struct {
	mu    sync.Mutex
	clock uint64
	slots *pooled.SliceBuf[recentKeyCacheSlot]
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
	retired *pooled.SliceBuf[*indexSnapshot]
	refs    atomic.Int64
}

type indexKeyOrder []index

func (s indexKeyOrder) Len() int      { return len(s) }
func (s indexKeyOrder) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s indexKeyOrder) Less(i, j int) bool {
	return compareIndexKeys(s[i].Key, s[j].Key) < 0
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
	if e != nil {
		e.refs.Add(1)
	}
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
	if c != nil {
		c.refs.Add(1)
	}
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

func (c *materializedPredCache) evictLocked(
	mode materializedPredCacheEvictMode,
	count *atomic.Int32,
	oversizedCount *atomic.Int32,
) bool {
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

func appendRetiredSnapshot(buf *pooled.SliceBuf[*indexSnapshot], snap *indexSnapshot) *pooled.SliceBuf[*indexSnapshot] {
	if snap == nil {
		return buf
	}
	if buf == nil {
		buf = snapshotRetiredListPool.Get()
	}
	buf.Append(snap)
	return buf
}

func releaseRetiredSnapshots(buf *pooled.SliceBuf[*indexSnapshot]) {
	if buf == nil {
		return
	}
	for i := 0; i < buf.Len(); i++ {
		retired := buf.Get(i)
		if retired == nil {
			continue
		}
		retired.releaseOwnedStorage()
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
		if slot.field == "" || slot.entry == nil || slot.entry.storage.keyCount() == 0 {
			continue
		}
		acc, ok := next.indexedFieldByName[slot.field]
		if !ok || next.index == nil || acc.ordinal >= next.index.Len() {
			continue
		}
		nextStorage := next.index.Get(acc.ordinal)
		if nextStorage != slot.entry.storage {
			continue
		}
		slot.entry.retain()
		next.numericRangeBucketCache.storeSlot(slot.field, acc.ordinal, slot.entry)
	}
}

func inheritMaterializedPredCache[K ~string | ~uint64, V any](db *DB[K, V], next, prev *indexSnapshot, changedFields []bool) {
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
			acc, ok := db.indexedFieldByName[f]
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

func (db *DB[K, V]) buildPublishedSnapshotNoLock(seq uint64) *indexSnapshot {
	var strmap *strMapSnapshot
	if db.strkey && db.strmap != nil {
		strmap = db.strmap.snapshot()
	}
	snap := &indexSnapshot{
		seq:                seq,
		index:              db.index,
		nilIndex:           db.nilIndex,
		lenIndex:           db.lenIndex,
		lenZeroComplement:  db.lenZeroComplement,
		indexedFieldByName: db.indexedFieldByName,
		universe:           db.universe,
		strmap:             strmap,
	}
	db.initSnapshotRuntimeCaches(snap)
	return snap
}

func (db *DB[K, V]) publishSnapshotNoLock(seq uint64) {
	prev := db.snapshot.current.Load()
	snap := db.buildPublishedSnapshotNoLock(seq)
	if !db.transparent && prev != nil {
		snap.index = cloneFieldIndexStorageSlots(db.index, len(db.indexedFieldAccess))
		snap.nilIndex = cloneFieldIndexStorageSlots(db.nilIndex, len(db.indexedFieldAccess))
		snap.lenIndex = cloneFieldIndexStorageSlots(db.lenIndex, len(db.indexedFieldAccess))
		snap.lenZeroComplement = cloneFieldIndexBoolSlots(db.lenZeroComplement, len(db.indexedFieldAccess))
	}
	snap.retainSharedOwnedStorageFrom(prev)
	db.finishSnapshotPublishNoLock(snap)
}

func (db *DB[K, V]) finishSnapshotPublishNoLock(s *indexSnapshot) {
	if s == nil {
		return
	}
	db.index = s.index
	db.nilIndex = s.nilIndex
	db.lenIndex = s.lenIndex
	db.lenZeroComplement = s.lenZeroComplement
	db.universe = s.universe
	retired := db.publishSnapshotRef(s)
	if db.strkey && db.strmap != nil {
		db.strmap.markCommittedPublished(s.strmap)
	}
	releaseRetiredSnapshots(retired)
}

func (db *DB[K, V]) getSnapshot() *indexSnapshot {
	if s := db.snapshot.current.Load(); s != nil {
		return s
	}
	return db.buildPublishedSnapshotNoLock(0)
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

func newMaterializedPredCacheEntry(
	ids posting.List,
	oversized bool,
	clock *atomic.Uint64,
) *materializedPredCacheEntry {
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
	db.getSnapshot().clearRuntimeCachesForTesting()
}

func (s *indexSnapshot) fieldIndexStorage(field string) (fieldIndexStorage, bool) {
	if s == nil || s.index == nil {
		return fieldIndexStorage{}, false
	}
	acc, ok := s.indexedFieldByName[field]
	if !ok || acc.ordinal >= s.index.Len() {
		return fieldIndexStorage{}, false
	}
	storage := s.index.Get(acc.ordinal)
	return storage, storage.keyCount() > 0
}

func (s *indexSnapshot) nilFieldIndexSlice(field string) *[]index {
	if s == nil || s.nilIndex == nil {
		return nil
	}
	acc, ok := s.indexedFieldByName[field]
	if !ok || acc.ordinal >= s.nilIndex.Len() {
		return nil
	}
	return s.nilIndex.Get(acc.ordinal).flatSlice()
}

func (s *indexSnapshot) nilFieldNameSet() map[string]struct{} {
	if s == nil || s.nilIndex == nil {
		return nil
	}
	fields := make(map[string]struct{}, len(s.indexedFieldByName))
	for f, acc := range s.indexedFieldByName {
		if acc.ordinal < s.nilIndex.Len() && s.nilIndex.Get(acc.ordinal).keyCount() > 0 {
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
		if acc.ordinal < s.index.Len() && s.index.Get(acc.ordinal).keyCount() > 0 {
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
		if !acc.field.Slice || acc.ordinal >= s.lenIndex.Len() || s.lenIndex.Get(acc.ordinal).keyCount() == 0 {
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
	if !ok || acc.ordinal >= s.index.Len() {
		return posting.List{}
	}
	return newFieldOverlayStorage(s.index.Get(acc.ordinal)).lookupPostingRetained(key)
}

func (db *DB[K, V]) publishSnapshotRef(s *indexSnapshot) *pooled.SliceBuf[*indexSnapshot] {
	if s == nil {
		return nil
	}
	db.snapshot.mu.Lock()

	prev := db.snapshot.current.Load()
	ref := db.snapshot.bySeq[s.seq]
	if ref == nil {
		ref = snapshotRefPool.Get()
		db.snapshot.bySeq[s.seq] = ref
	}
	var retired *pooled.SliceBuf[*indexSnapshot]
	if prev != nil && prev.seq == s.seq && ref.snap != nil && ref.snap != s {
		if ref.refs.Load() != 0 {
			ref.retired = appendRetiredSnapshot(ref.retired, ref.snap)
		} else {
			retired = appendRetiredSnapshot(retired, ref.snap)
		}
	}
	ref.snap = s
	db.snapshot.current.Store(s)
	db.snapshot.currentRef.Store(ref)
	if prev != nil && prev.seq != s.seq {
		retired = db.retireSnapshotLocked(prev.seq)
	}
	db.snapshot.mu.Unlock()
	return retired
}

func (db *DB[K, V]) stageSnapshot(s *indexSnapshot) {
	if s == nil {
		return
	}
	db.snapshot.mu.Lock()
	defer db.snapshot.mu.Unlock()

	ref := db.snapshot.bySeq[s.seq]
	if ref == nil {
		ref = snapshotRefPool.Get()
		db.snapshot.bySeq[s.seq] = ref
	}
	ref.snap = s
}

func (db *DB[K, V]) dropStagedSnapshot(seq uint64) {
	db.snapshot.mu.Lock()

	ref := db.snapshot.bySeq[seq]
	if ref == nil {
		db.snapshot.mu.Unlock()
		return
	}
	if current := db.snapshot.current.Load(); current != nil && current.seq == seq {
		db.snapshot.mu.Unlock()
		return
	}
	var retired *pooled.SliceBuf[*indexSnapshot]
	if ref.refs.Load() <= 0 {
		retired = db.releaseRetiredSnapshotRefLocked(seq, ref)
		db.snapshot.mu.Unlock()
		releaseRetiredSnapshots(retired)
		return
	}
	ref.retired = appendRetiredSnapshot(ref.retired, ref.snap)
	ref.snap = nil
	db.snapshot.mu.Unlock()
}

func (db *DB[K, V]) pinSnapshotRefBySeq(seq uint64) (*indexSnapshot, *snapshotRef, bool) {
	db.snapshot.mu.RLock()
	defer db.snapshot.mu.RUnlock()

	ref := db.snapshot.bySeq[seq]
	if ref == nil || ref.snap == nil {
		return nil, nil, false
	}
	ref.refs.Add(1)
	return ref.snap, ref, true
}

func (db *DB[K, V]) unpinSnapshotRef(seq uint64, ref *snapshotRef) {
	if ref == nil {
		return
	}
	if ref.refs.Add(-1) != 0 {
		return
	}
	var drainCache *materializedPredCache
	var retired *pooled.SliceBuf[*indexSnapshot]
	db.snapshot.mu.Lock()
	held := db.snapshot.bySeq[seq]
	if held != ref || held.refs.Load() != 0 || held.snap != nil {
		if held == ref && held != nil && held.refs.Load() == 0 && held.snap != nil &&
			held == db.snapshot.currentRef.Load() && held.retired != nil {
			retired = held.retired
			held.retired = nil
		}
		if held == ref && held != nil && held.snap != nil &&
			held == db.snapshot.currentRef.Load() {
			drainCache = held.snap.matPredCache
			if drainCache != nil {
				drainCache.retain()
			}
		}
		db.snapshot.mu.Unlock()
		releaseRetiredSnapshots(retired)
		if drainCache != nil {
			drainCache.drainRetired()
			drainCache.releaseRef()
		}
		return
	}
	retired = db.releaseRetiredSnapshotRefLocked(seq, held)
	db.snapshot.mu.Unlock()
	releaseRetiredSnapshots(retired)
}

func (db *DB[K, V]) retireSnapshotLocked(seq uint64) *pooled.SliceBuf[*indexSnapshot] {
	ref := db.snapshot.bySeq[seq]
	if ref == nil {
		return nil
	}
	if current := db.snapshot.current.Load(); current != nil && current.seq == seq {
		return nil
	}
	if ref.refs.Load() != 0 {
		ref.retired = appendRetiredSnapshot(ref.retired, ref.snap)
		ref.snap = nil
		return nil
	}
	return db.releaseRetiredSnapshotRefLocked(seq, ref)
}

func (db *DB[K, V]) releaseRetiredSnapshotRefLocked(seq uint64, ref *snapshotRef) *pooled.SliceBuf[*indexSnapshot] {
	if ref == nil {
		return nil
	}
	if current := db.snapshot.current.Load(); current != nil && current.seq == seq {
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
	delete(db.snapshot.bySeq, seq)
	if db.snapshot.currentRef.Load() == ref {
		db.snapshot.currentRef.Store(nil)
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
	if !db.snapshot.statsEnabled {
		return SnapshotStats{}
	}
	if !db.beginOpWait() {
		return SnapshotStats{}
	}
	defer db.endOp()

	s, seq, ref, pinned := db.pinCurrentSnapshot()
	defer db.unpinCurrentSnapshot(seq, ref, pinned)
	diag := SnapshotStats{
		Sequence: s.seq,
	}
	if !s.universe.IsEmpty() {
		diag.UniverseCard = s.universe.Cardinality()
	}

	db.snapshot.mu.RLock()
	diag.RegistrySize = len(db.snapshot.bySeq)
	for _, held := range db.snapshot.bySeq {
		refs := held.refs.Load()
		if pinned && held == ref {
			refs--
		}
		if refs > 0 {
			diag.PinnedRefs++
		}
	}
	db.snapshot.mu.RUnlock()

	return diag
}

func unionPostingListsOwned(base, add posting.List) posting.List {
	merged := base.Clone()
	merged = merged.BuildMergedOwned(add)
	return merged
}

func rebuildLenIndexField(universe posting.List, fieldOV fieldOverlay) (*[]index, bool) {
	result := make([]index, 0)
	if universe.IsEmpty() {
		return &result, false
	}

	var nonEmpty posting.List

	counts := make(map[uint64]uint32, 1024)
	br := fieldOV.rangeForBounds(rangeBounds{has: true})
	if !overlayRangeEmpty(br) {
		cur := fieldOV.newCursor(br, false)
		for {
			_, ids, ok := cur.next()
			if !ok {
				break
			}
			if ids.IsEmpty() {
				continue
			}
			if nonEmpty.IsEmpty() {
				nonEmpty = ids.Clone()
			} else {
				nonEmpty = nonEmpty.BuildOr(ids)
			}
			ids.ForEach(func(idx uint64) bool {
				counts[idx]++
				return true
			})
		}
	}

	lenMap := make(map[uint32]posting.List, len(counts)+1)
	for idx, ln := range counts {
		if ln == 0 {
			continue
		}
		p := lenMap[ln]
		p = p.BuildAdded(idx)
		lenMap[ln] = p
	}

	empty := universe.Clone()
	empty = empty.BuildAndNot(nonEmpty)

	useZeroComplement := false
	var nonEmptyPosting posting.List
	if !empty.IsEmpty() {
		emptyCard := empty.Cardinality()
		nonEmptyCard := nonEmpty.Cardinality()
		if nonEmptyCard > 0 && nonEmptyCard < emptyCard {
			nonEmptyPosting = nonEmpty.Clone()
			useZeroComplement = !nonEmptyPosting.IsEmpty()
		}
	}
	if useZeroComplement {
		empty.Release()
	} else if !empty.IsEmpty() {
		zeroPosting := lenMap[0]
		zeroPosting = zeroPosting.BuildMergedOwned(empty)
		lenMap[0] = zeroPosting
	} else {
		empty.Release()
	}

	resultCap := len(lenMap)
	if useZeroComplement {
		resultCap++
	}
	result = make([]index, 0, resultCap)
	for ln, ids := range lenMap {
		ids = ids.BuildOptimized()
		if ids.IsEmpty() {
			continue
		}
		result = append(result, index{
			Key: indexKeyFromU64(uint64(ln)),
			IDs: ids,
		})
	}
	if useZeroComplement {
		nonEmptyPosting = nonEmptyPosting.BuildOptimized()
		if !nonEmptyPosting.IsEmpty() {
			result = append(result, index{
				Key: indexKeyFromString(lenIndexNonEmptyKey),
				IDs: nonEmptyPosting,
			})
		}
	}

	slices.SortFunc(result, func(a, b index) int {
		return compareIndexKeys(a.Key, b.Key)
	})
	nonEmpty.Release()
	return &result, useZeroComplement
}

func addFieldPostingListHint(fieldMap map[string]posting.List, key string, idx uint64, capHint int) map[string]posting.List {
	if fieldMap == nil {
		if capHint >= 64 {
			fieldMap = make(map[string]posting.List, min(capHint, postingMapPoolMaxLen))
		} else {
			fieldMap = postingMapPool.Get()
		}
	}
	p := fieldMap[key]
	p = p.BuildAdded(idx)
	fieldMap[key] = p
	return fieldMap
}

func addFixedFieldPostingListHint(fieldMap map[uint64]posting.List, key uint64, idx uint64, capHint int) map[uint64]posting.List {
	if fieldMap == nil {
		if capHint >= 64 {
			fieldMap = make(map[uint64]posting.List, min(capHint, postingMapPoolMaxLen))
		} else {
			fieldMap = fixedPostingMapPool.Get()
		}
	}
	p := fieldMap[key]
	p = p.BuildAdded(idx)
	fieldMap[key] = p
	return fieldMap
}

func (db *DB[K, V]) buildPreparedSnapshotNoLock(seq uint64, prepared []autoBatchPrepared[K, V]) *indexSnapshot {
	prev := db.getSnapshot()
	if snap, ok := db.buildPreparedSnapshotFromEmptyBaseNoLock(seq, prev, prepared); ok {
		return snap
	}
	if snap, ok := db.buildPreparedSnapshotInsertOnlyNoLock(seq, prev, prepared); ok {
		return snap
	}
	return db.buildPreparedSnapshotAggregatedNoLock(seq, prev, prepared)
}

func (db *DB[K, V]) buildPreparedSnapshotFromEmptyBaseNoLock(seq uint64, prev *indexSnapshot, prepared []autoBatchPrepared[K, V]) (*indexSnapshot, bool) {
	if prev != nil && !prev.universe.IsEmpty() {
		return nil, false
	}
	if len(prepared) == 0 {
		return nil, false
	}
	for i := range prepared {
		if prepared[i].oldVal != nil || prepared[i].newVal == nil {
			return nil, false
		}
	}

	fieldStates := make([]snapshotFieldOverlayState, len(db.indexedFieldAccess))
	var universe posting.List

	for i := range prepared {
		op := prepared[i]
		universe = universe.BuildAdded(op.idx)
		ptr := unsafe.Pointer(op.newVal)

		for _, acc := range db.indexedFieldAccess {
			acc.collectSnapshotOverlayValue(ptr, op.idx, &fieldStates[acc.ordinal])
		}
	}

	nextIndex := fieldIndexStorageSlicePool.Get()
	nextIndex.SetLen(len(db.indexedFieldAccess))
	for i, acc := range db.indexedFieldAccess {
		if storage := acc.materializeSnapshotOverlayStorageOwned(&fieldStates[i]); storage.keyCount() > 0 {
			nextIndex.Set(i, storage)
		}
	}

	nextNilIndex := fieldIndexStorageSlicePool.Get()
	nextNilIndex.SetLen(len(db.indexedFieldAccess))
	for i, acc := range db.indexedFieldAccess {
		if storage := acc.materializeSnapshotOverlayNilStorageOwned(&fieldStates[i]); storage.keyCount() > 0 {
			nextNilIndex.Set(i, storage)
		}
	}

	nextLenIndex := fieldIndexStorageSlicePool.Get()
	nextLenIndex.SetLen(len(db.indexedFieldAccess))
	nextLenZeroComplement := fieldIndexBoolSlicePool.Get()
	nextLenZeroComplement.SetLen(len(db.indexedFieldAccess))
	for i, acc := range db.indexedFieldAccess {
		if !acc.field.Slice {
			continue
		}
		lengths := fieldStates[i].lengths
		fieldStates[i].lengths = nil
		storage, useZeroComplement := materializeLenFieldStorageOwned(universe, lengths)
		nextLenIndex.Set(i, storage)
		if useZeroComplement {
			nextLenZeroComplement.Set(i, true)
		}
	}

	snap := &indexSnapshot{
		seq:                seq,
		index:              nextIndex,
		nilIndex:           nextNilIndex,
		lenIndex:           nextLenIndex,
		lenZeroComplement:  nextLenZeroComplement,
		indexedFieldByName: db.indexedFieldByName,
		universe:           universe,
		strmap:             db.strmap.snapshot(),
	}
	db.initSnapshotRuntimeCaches(snap)
	inheritNumericRangeBucketCache(snap, prev)
	changedCount := 0
	for i := range fieldStates {
		if fieldStates[i].changed {
			changedCount++
		}
	}
	if changedCount > 0 {
		changed := make([]bool, len(db.indexedFieldAccess))
		for i := range fieldStates {
			if fieldStates[i].changed {
				changed[i] = true
			}
		}
		inheritMaterializedPredCache(db, snap, prev, changed)
	} else {
		inheritMaterializedPredCache(db, snap, prev, nil)
	}
	snap.ensureUniverseOwner()
	return snap, true
}

func mergeInsertOnlyFieldSliceOwned(
	base *[]index,
	adds map[string]uint32,
	arena *insertPostingAccumArena,
	fixed8 bool,
) *[]index {
	if len(adds) == 0 {
		insertPostingMapPool.Put(adds)
		return base
	}
	if len(adds) == 1 {
		var add index
		for key, ref := range adds {
			add = index{
				Key: indexKeyFromStoredString(key, fixed8),
				IDs: arena.accum(ref).materializeOwned(),
			}
		}
		insertPostingMapPool.Put(adds)
		return mergeInsertOnlySingleFieldEntry(base, add)
	}

	addSlice := make([]index, 0, len(adds))
	for key, ref := range adds {
		ids := arena.accum(ref).materializeOwned()
		addSlice = append(addSlice, index{
			Key: indexKeyFromStoredString(key, fixed8),
			IDs: ids,
		})
	}
	insertPostingMapPool.Put(adds)

	return mergeInsertOnlyFieldEntries(base, addSlice)
}

func mergeInsertOnlyFixedFieldSliceOwned(
	base *[]index,
	adds map[uint64]uint32,
	arena *insertPostingAccumArena,
) *[]index {
	if len(adds) == 0 {
		fixedInsertPostingMapPool.Put(adds)
		return base
	}
	if len(adds) == 1 {
		var add index
		for key, ref := range adds {
			add = index{
				Key: indexKeyFromU64(key),
				IDs: arena.accum(ref).materializeOwned(),
			}
		}
		fixedInsertPostingMapPool.Put(adds)
		return mergeInsertOnlySingleFieldEntry(base, add)
	}

	addSlice := make([]index, 0, len(adds))
	for key, ref := range adds {
		ids := arena.accum(ref).materializeOwned()
		addSlice = append(addSlice, index{
			Key: indexKeyFromU64(key),
			IDs: ids,
		})
	}
	fixedInsertPostingMapPool.Put(adds)

	return mergeInsertOnlyFieldEntries(base, addSlice)
}

func mergeInsertOnlySingleFieldEntry(base *[]index, add index) *[]index {
	if add.IDs.IsEmpty() {
		return base
	}
	if base == nil || len(*base) == 0 {
		out := []index{add}
		return &out
	}

	src := *base
	pos := lowerBoundIndexEntriesKey(src, add.Key)
	if pos < len(src) && compareIndexKeys(src[pos].Key, add.Key) == 0 {
		out := make([]index, len(src))
		copyBorrowedIndexEntries(out, src)
		out[pos].IDs = unionPostingListsOwned(src[pos].IDs, add.IDs)
		return &out
	}

	out := make([]index, len(src)+1)
	copyBorrowedIndexEntries(out[:pos], src[:pos])
	out[pos] = add
	copyBorrowedIndexEntries(out[pos+1:], src[pos:])
	return &out
}

func mergeInsertOnlyFieldEntries(base *[]index, addSlice []index) *[]index {
	sort.Sort(indexKeyOrder(addSlice))

	if base == nil || len(*base) == 0 {
		return &addSlice
	}

	src := *base
	out := make([]index, 0, len(src)+len(addSlice))
	i, j := 0, 0
	for i < len(src) && j < len(addSlice) {
		cmp := compareIndexKeys(src[i].Key, addSlice[j].Key)
		switch {
		case cmp < 0:
			out = append(out, borrowedFieldIndexEntry(src[i]))
			i++
		case cmp > 0:
			out = append(out, addSlice[j])
			j++
		default:
			merged := src[i]
			merged.IDs = unionPostingListsOwned(src[i].IDs, addSlice[j].IDs)
			out = append(out, merged)
			i++
			j++
		}
	}
	if i < len(src) {
		out = appendBorrowedIndexEntries(out, src[i:])
	}
	if j < len(addSlice) {
		out = append(out, addSlice[j:]...)
	}
	return &out
}

func (db *DB[K, V]) buildPreparedSnapshotInsertOnlyNoLock(seq uint64, prev *indexSnapshot, prepared []autoBatchPrepared[K, V]) (*indexSnapshot, bool) {
	if len(prepared) == 0 {
		return nil, false
	}
	for i := range prepared {
		if prepared[i].oldVal != nil || prepared[i].newVal == nil {
			return nil, false
		}
	}

	next := &indexSnapshot{
		seq: seq,

		index:              cloneFieldIndexStorageSlots(prev.index, len(db.indexedFieldAccess)),
		nilIndex:           cloneFieldIndexStorageSlots(prev.nilIndex, len(db.indexedFieldAccess)),
		lenIndex:           cloneFieldIndexStorageSlots(prev.lenIndex, len(db.indexedFieldAccess)),
		lenZeroComplement:  cloneFieldIndexBoolSlots(prev.lenZeroComplement, len(db.indexedFieldAccess)),
		indexedFieldByName: db.indexedFieldByName,
		universe:           prev.universe.Clone(),
		strmap:             db.strmap.snapshot(),
	}
	db.initSnapshotRuntimeCaches(next)

	fieldStates := snapshotFieldInsertStateSlicePool.Get()
	fieldStates.SetLen(len(db.indexedFieldAccess))
	initSnapshotFieldInsertStateHints(fieldStates, db.indexedFieldAccess, prev, len(prepared))

	for i := range prepared {
		op := prepared[i]
		next.universe = next.universe.BuildAdded(op.idx)
		ptr := unsafe.Pointer(op.newVal)

		for _, acc := range db.indexedFieldAccess {
			useZeroComplement := prev.lenZeroComplement != nil && acc.ordinal < prev.lenZeroComplement.Len() && prev.lenZeroComplement.Get(acc.ordinal)
			acc.collectSnapshotInsertValue(ptr, op.idx, useZeroComplement, fieldStates.GetPtr(acc.ordinal))
		}
	}

	changedCount := 0
	for i, acc := range db.indexedFieldAccess {
		state := fieldStates.GetPtr(i)
		baseIndex := next.index.Get(i)
		if storage := acc.mergeSnapshotInsertStorageOwned(baseIndex, state, true); storage.keyCount() > 0 {
			if storage != baseIndex {
				next.index.Set(i, storage)
			}
		} else if baseIndex.keyCount() > 0 {
			next.index.Set(i, fieldIndexStorage{})
		}
		baseNil := next.nilIndex.Get(i)
		if storage := acc.mergeSnapshotInsertNilStorageOwned(baseNil, state); storage.keyCount() > 0 {
			if storage != baseNil {
				next.nilIndex.Set(i, storage)
			}
		} else if baseNil.keyCount() > 0 {
			next.nilIndex.Set(i, fieldIndexStorage{})
		}
		if state.lengths != nil {
			baseLen := next.lenIndex.Get(i)
			if storage := applyLenFieldPostingDiffStorageOwned(baseLen, state.lengths); storage != baseLen {
				next.lenIndex.Set(i, storage)
			}
			state.lengths = nil
		}
		if state.changed {
			changedCount++
		}
		state.releaseOwned()
	}
	inheritNumericRangeBucketCache(next, prev)

	if changedCount > 0 {
		changed := make([]bool, len(db.indexedFieldAccess))
		for i := 0; i < fieldStates.Len(); i++ {
			if fieldStates.Get(i).changed {
				changed[i] = true
			}
		}
		inheritMaterializedPredCache(db, next, prev, changed)
	} else {
		inheritMaterializedPredCache(db, next, prev, nil)
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
	if o != nil {
		o.refs.Add(1)
	}
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
		retainSharedFieldIndexStorageSlots(s.index, prev.index)
		retainSharedFieldIndexStorageSlots(s.nilIndex, prev.nilIndex)
		retainSharedFieldIndexStorageSlots(s.lenIndex, prev.lenIndex)
	}
}

func (s *indexSnapshot) releaseOwnedStorage() {
	if s == nil {
		return
	}
	if s.universeOwner != nil {
		s.universeOwner.release()
	}
	releaseFieldIndexStorageSlotsOwned(s.index)
	releaseFieldIndexStorageSlotsOwned(s.nilIndex)
	releaseFieldIndexStorageSlotsOwned(s.lenIndex)
	if s.lenZeroComplement != nil {
		fieldIndexBoolSlicePool.Put(s.lenZeroComplement)
	}
	s.index = nil
	s.nilIndex = nil
	s.lenIndex = nil
	s.lenZeroComplement = nil
}
