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

	index             map[string]fieldIndexStorage
	nilIndex          map[string]fieldIndexStorage
	lenIndex          map[string]fieldIndexStorage
	lenZeroComplement map[string]bool
	universe          posting.List
	strmap            *strMapSnapshot

	numericRangeBucketCache *sync.Map

	matPredCache               sync.Map
	matPredCacheCount          atomic.Int32
	matPredCacheMaxEntries     int
	matPredCacheMaxCard        uint64
	matPredCacheOversizedCount atomic.Int32
	matPredCacheClock          atomic.Uint64
	runtimeMatPredSeen         recentKeyCache
	orderORMatPredObserved     recentKeyCache
}

type materializedPredCacheEntry struct {
	ids       posting.List
	oversized bool
	stamp     atomic.Uint64
}

type recentKeyCache struct {
	keys  sync.Map
	count atomic.Int32
	clock atomic.Uint64
}

type recentKeyCacheEntry struct {
	stamp atomic.Uint64
	work  atomic.Uint64
}

const matPredCacheOversizedMaxEntries = 4

type materializedPredCacheEvictMode uint8

const (
	matPredCacheEvictPreferRegular materializedPredCacheEvictMode = iota
	matPredCacheEvictOversizedOnly
)

type snapshotRef struct {
	snap *indexSnapshot
	refs atomic.Int64
}

type indexKeyOrder []index

func (s indexKeyOrder) Len() int      { return len(s) }
func (s indexKeyOrder) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s indexKeyOrder) Less(i, j int) bool {
	return compareIndexKeys(s[i].Key, s[j].Key) < 0
}

var snapshotRefPool = pooled.Pointers[snapshotRef]{Clear: true}

var recentKeyCacheEntryPool = pooled.Pointers[recentKeyCacheEntry]{Clear: true}

var materializedPredCacheEntryPool = pooled.Pointers[materializedPredCacheEntry]{Clear: true}

func materializedPredCacheMaxCardinality(v int) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

func tryReserveCacheSlot(counter *atomic.Int32, limit int32) bool {
	for {
		n := counter.Load()
		if n >= limit {
			return false
		}
		if counter.CompareAndSwap(n, n+1) {
			return true
		}
	}
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

func (e *materializedPredCacheEntry) touch(clock *atomic.Uint64) {
	if e == nil || clock == nil {
		return
	}
	e.stamp.Store(clock.Add(1))
}

func (e *recentKeyCacheEntry) touch(clock *atomic.Uint64) {
	if e == nil || clock == nil {
		return
	}
	e.stamp.Store(clock.Add(1))
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
	c.keys.Range(func(_, v any) bool {
		entry, _ := v.(*recentKeyCacheEntry)
		if entry != nil {
			recentKeyCacheEntryPool.Put(entry)
		}
		return true
	})
	c.keys.Clear()
	c.count.Store(0)
	c.clock.Store(0)
}

func (c *recentKeyCache) evictOldest() bool {
	var evictKey any
	evictStamp := ^uint64(0)
	c.keys.Range(func(k, v any) bool {
		entry, _ := v.(*recentKeyCacheEntry)
		stamp := uint64(0)
		if entry != nil {
			stamp = entry.stamp.Load()
		}
		if stamp <= evictStamp {
			evictKey = k
			evictStamp = stamp
		}
		return true
	})
	if evictKey == nil {
		return false
	}
	if _, deleted := c.keys.LoadAndDelete(evictKey); !deleted {
		return false
	}
	c.count.Add(-1)
	return true
}

func recentKeyCacheKeyOK(key any) bool {
	switch key := key.(type) {
	case materializedPredKey:
		return !key.isZero()
	case string:
		return key != ""
	default:
		return false
	}
}

func (c *recentKeyCache) touchOrRemember(key any, limit int) bool {
	if !recentKeyCacheKeyOK(key) || limit <= 0 {
		return false
	}
	if v, ok := c.keys.Load(key); ok {
		entry, _ := v.(*recentKeyCacheEntry)
		if entry != nil {
			entry.touch(&c.clock)
		}
		return true
	}

	for !tryReserveCacheSlot(&c.count, int32(limit)) {
		if !c.evictOldest() {
			return false
		}
	}

	entry := recentKeyCacheEntryPool.Get()
	entry.touch(&c.clock)
	actual, loaded := c.keys.LoadOrStore(key, entry)
	if !loaded {
		return false
	}
	recentKeyCacheEntryPool.Put(entry)
	c.count.Add(-1)
	existing, _ := actual.(*recentKeyCacheEntry)
	if existing != nil {
		existing.touch(&c.clock)
	}
	return true
}

func addObservedWork(cur, delta uint64) uint64 {
	if ^uint64(0)-cur < delta {
		return ^uint64(0)
	}
	return cur + delta
}

func (c *recentKeyCache) addWorkAndShouldPromote(key any, limit int, delta uint64, threshold uint64) bool {
	if !recentKeyCacheKeyOK(key) || limit <= 0 || delta == 0 || threshold == 0 {
		return false
	}
	if delta >= threshold {
		return true
	}
	if v, ok := c.keys.Load(key); ok {
		entry, _ := v.(*recentKeyCacheEntry)
		if entry == nil {
			return false
		}
		entry.touch(&c.clock)
		for {
			cur := entry.work.Load()
			next := addObservedWork(cur, delta)
			if entry.work.CompareAndSwap(cur, next) {
				if next < threshold {
					return false
				}
				if actual, deleted := c.keys.LoadAndDelete(key); deleted {
					if kept, _ := actual.(*recentKeyCacheEntry); kept == entry {
						c.count.Add(-1)
					}
				}
				return true
			}
		}
	}

	for !tryReserveCacheSlot(&c.count, int32(limit)) {
		if !c.evictOldest() {
			return false
		}
	}

	entry := recentKeyCacheEntryPool.Get()
	entry.touch(&c.clock)
	entry.work.Store(delta)
	actual, loaded := c.keys.LoadOrStore(key, entry)
	if !loaded {
		return false
	}
	recentKeyCacheEntryPool.Put(entry)
	c.count.Add(-1)
	existing, _ := actual.(*recentKeyCacheEntry)
	if existing == nil {
		return false
	}
	existing.touch(&c.clock)
	for {
		cur := existing.work.Load()
		next := addObservedWork(cur, delta)
		if existing.work.CompareAndSwap(cur, next) {
			if next < threshold {
				return false
			}
			if actual, deleted := c.keys.LoadAndDelete(key); deleted {
				if kept, _ := actual.(*recentKeyCacheEntry); kept == existing {
					c.count.Add(-1)
				}
			}
			return true
		}
	}
}

func (s *indexSnapshot) evictMaterializedPred(mode materializedPredCacheEvictMode) bool {
	if s == nil {
		return false
	}
	for {
		var evictKey any
		var fallbackKey any
		evictStamp := ^uint64(0)
		fallbackStamp := ^uint64(0)
		s.matPredCache.Range(func(k, v any) bool {
			entry, _ := v.(*materializedPredCacheEntry)
			stamp := uint64(0)
			if entry != nil {
				stamp = entry.stamp.Load()
			}
			if entry == nil {
				if mode != matPredCacheEvictOversizedOnly && stamp <= fallbackStamp {
					fallbackKey = k
					fallbackStamp = stamp
				}
				return true
			}
			if entry.oversized {
				if mode != matPredCacheEvictOversizedOnly && stamp <= fallbackStamp {
					fallbackKey = k
					fallbackStamp = stamp
				}
				if mode == matPredCacheEvictOversizedOnly {
					if stamp <= evictStamp {
						evictKey = k
						evictStamp = stamp
					}
				}
				return true
			}
			if mode != matPredCacheEvictOversizedOnly {
				if stamp <= evictStamp {
					evictKey = k
					evictStamp = stamp
				}
			}
			return true
		})
		if evictKey == nil {
			evictKey = fallbackKey
		}
		if evictKey == nil {
			return false
		}
		actual, deleted := s.matPredCache.LoadAndDelete(evictKey)
		if !deleted {
			continue
		}
		// Cached postings may already be borrowed by concurrent readers.
		// Eviction must only stop future hits; it cannot Release() payloads eagerly.
		s.matPredCacheCount.Add(-1)
		entry, _ := actual.(*materializedPredCacheEntry)
		if entry != nil && entry.oversized {
			s.matPredCacheOversizedCount.Add(-1)
		}
		return true
	}
}

func (s *indexSnapshot) reserveMaterializedPredSlot(limit int) bool {
	if s == nil || limit <= 0 {
		return false
	}
	for {
		if tryReserveCacheSlot(&s.matPredCacheCount, int32(limit)) {
			return true
		}
		if !s.evictMaterializedPred(matPredCacheEvictPreferRegular) {
			return false
		}
	}
}

func (s *indexSnapshot) reserveMaterializedPredOversizedSlot(limit int) bool {
	if s == nil || limit <= 0 {
		return false
	}
	oversizedLimit := materializedPredCacheOversizedLimit(limit)
	if oversizedLimit <= 0 {
		return false
	}
	for {
		if tryReserveCacheSlot(&s.matPredCacheOversizedCount, oversizedLimit) {
			return true
		}
		if !s.evictMaterializedPred(matPredCacheEvictOversizedOnly) {
			return false
		}
	}
}

func inheritNumericRangeBucketCache(next, prev *indexSnapshot) {
	if next == nil {
		return
	}
	if next.numericRangeBucketCache == nil {
		next.numericRangeBucketCache = &sync.Map{}
	}
	if prev == nil || prev.numericRangeBucketCache == nil {
		return
	}
	prev.numericRangeBucketCache.Range(func(k, v any) bool {
		field, ok := k.(string)
		if !ok || field == "" {
			return true
		}
		entry, ok := v.(*numericRangeBucketCacheEntry)
		if !ok || entry == nil || entry.storage.keyCount() == 0 {
			return true
		}
		nextStorage, ok := next.fieldIndexStorage(field)
		if !ok || nextStorage != entry.storage {
			return true
		}
		next.numericRangeBucketCache.Store(field, entry)
		return true
	})
}

func materializedPredCacheFieldName(key any) string {
	switch key := key.(type) {
	case materializedPredKey:
		return key.field
	case string:
		parsed, ok := materializedPredKeyFromEncoded(key)
		if !ok {
			return ""
		}
		return parsed.field
	default:
		return ""
	}
}

func materializedPredCacheNormalizedMapKey(key any) (materializedPredKey, bool) {
	switch key := key.(type) {
	case materializedPredKey:
		if key.isZero() {
			return materializedPredKey{}, false
		}
		return key, true
	case string:
		return materializedPredKeyFromEncoded(key)
	default:
		return materializedPredKey{}, false
	}
}

func inheritMaterializedPredCache[K ~string | ~uint64, V any](db *DB[K, V], next, prev *indexSnapshot, changedFields []bool) {
	if next == nil || prev == nil {
		return
	}
	limit := next.materializedPredCacheLimit()
	if limit <= 0 || prev.matPredCacheCount.Load() == 0 {
		return
	}

	var oversized int32
	var maxStamp uint64
	prev.matPredCache.Range(func(k, v any) bool {
		if int(next.matPredCacheCount.Load()) >= limit {
			return false
		}
		key, ok := materializedPredCacheNormalizedMapKey(k)
		if !ok {
			return true
		}
		f := key.field
		if f == "" {
			return true
		}
		if changedFields != nil {
			acc, ok := db.indexedFieldByName[f]
			if !ok || changedFields[acc.ordinal] {
				return true
			}
		}
		entry, ok := v.(*materializedPredCacheEntry)
		if !ok {
			return true
		}
		var (
			cachedIDs      posting.List
			oversizedEntry bool
		)
		if entry != nil {
			cachedIDs = entry.ids
			oversizedEntry = !entry.ids.IsEmpty() && next.matPredCacheMaxCard > 0 &&
				entry.ids.Cardinality() > next.matPredCacheMaxCard
		}
		copied := materializedPredCacheEntryPool.Get()
		copied.ids = cachedIDs
		copied.oversized = oversizedEntry
		if entry != nil {
			stamp := entry.stamp.Load()
			copied.stamp.Store(stamp)
			if stamp > maxStamp {
				maxStamp = stamp
			}
		}
		if _, loaded := next.matPredCache.LoadOrStore(key, copied); loaded {
			copied.ids = posting.List{}
			copied.oversized = false
			materializedPredCacheEntryPool.Put(copied)
			return true
		}
		next.matPredCacheCount.Add(1)
		if oversizedEntry {
			oversized++
		}
		return true
	})
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
		seq:               seq,
		index:             db.index,
		nilIndex:          db.nilIndex,
		lenIndex:          db.lenIndex,
		lenZeroComplement: db.lenZeroComplement,
		universe:          db.universe,
		strmap:            strmap,
	}
	db.initSnapshotRuntimeCaches(snap)
	return snap
}

func (db *DB[K, V]) publishSnapshotNoLock(seq uint64) {
	db.finishSnapshotPublishNoLock(db.buildPublishedSnapshotNoLock(seq))
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
	db.publishSnapshotRef(s)
	if db.strkey && db.strmap != nil {
		db.strmap.markCommittedPublished(s.strmap)
	}
}

func (db *DB[K, V]) getSnapshot() *indexSnapshot {
	if s := db.snapshot.current.Load(); s != nil {
		return s
	}
	return db.buildPublishedSnapshotNoLock(0)
}

func (s *indexSnapshot) loadMaterializedPredStringKey(key string) (posting.List, bool) {
	if s == nil || key == "" {
		return posting.List{}, false
	}
	if s.materializedPredCacheLimit() <= 0 {
		return posting.List{}, false
	}
	v, ok := s.matPredCache.Load(key)
	if !ok {
		return posting.List{}, false
	}
	e, _ := v.(*materializedPredCacheEntry)
	if e == nil {
		return posting.List{}, true
	}
	e.touch(&s.matPredCacheClock)
	return e.ids.Borrow(), true
}

func (s *indexSnapshot) loadMaterializedPredKey(key materializedPredKey) (posting.List, bool) {
	if s == nil || key.isZero() {
		return posting.List{}, false
	}
	if s.materializedPredCacheLimit() <= 0 {
		return posting.List{}, false
	}
	v, ok := s.matPredCache.Load(key)
	if !ok {
		return posting.List{}, false
	}
	e, _ := v.(*materializedPredCacheEntry)
	if e == nil {
		return posting.List{}, true
	}
	e.touch(&s.matPredCacheClock)
	return e.ids.Borrow(), true
}

func (s *indexSnapshot) loadMaterializedPred(key string) (posting.List, bool) {
	if s == nil || key == "" {
		return posting.List{}, false
	}
	if parsed, ok := materializedPredKeyFromEncoded(key); ok {
		return s.loadMaterializedPredKey(parsed)
	}
	return s.loadMaterializedPredStringKey(key)
}

func (s *indexSnapshot) storeMaterializedPredKey(key materializedPredKey, ids posting.List) {
	if key.isZero() {
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
	if !s.reserveMaterializedPredSlot(limit) {
		return
	}
	stored := ids
	if stored.IsBorrowed() {
		stored = stored.Clone()
	}
	e := materializedPredCacheEntryPool.Get()
	e.ids = stored
	e.touch(&s.matPredCacheClock)
	if _, loaded := s.matPredCache.LoadOrStore(key, e); loaded {
		s.matPredCacheCount.Add(-1)
		e.ids = posting.List{}
		materializedPredCacheEntryPool.Put(e)
		if !stored.SharesPayload(ids) {
			stored.Release()
		}
	}
}

func (s *indexSnapshot) storeMaterializedPred(key string, ids posting.List) {
	if parsed, ok := materializedPredKeyFromEncoded(key); ok {
		s.storeMaterializedPredKey(parsed, ids)
		return
	}
	if s == nil || key == "" {
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
	if !s.reserveMaterializedPredSlot(limit) {
		return
	}
	stored := ids
	if stored.IsBorrowed() {
		stored = stored.Clone()
	}
	e := materializedPredCacheEntryPool.Get()
	e.ids = stored
	e.touch(&s.matPredCacheClock)
	if _, loaded := s.matPredCache.LoadOrStore(key, e); loaded {
		s.matPredCacheCount.Add(-1)
		e.ids = posting.List{}
		materializedPredCacheEntryPool.Put(e)
		if !stored.SharesPayload(ids) {
			stored.Release()
		}
	}
}

// tryStoreMaterializedPredOversized stores a small bounded number of oversized
// materialized postings per snapshot as a hot-cache fallback.
func (s *indexSnapshot) tryStoreMaterializedPredOversizedKey(key materializedPredKey, ids posting.List) bool {
	if key.isZero() || ids.IsEmpty() {
		return false
	}
	if s.matPredCacheMaxCard == 0 || ids.Cardinality() <= s.matPredCacheMaxCard {
		return false
	}
	limit := s.materializedPredCacheLimit()
	if limit <= 0 {
		return false
	}
	if !s.reserveMaterializedPredOversizedSlot(limit) {
		return false
	}
	if !s.reserveMaterializedPredSlot(limit) {
		s.matPredCacheOversizedCount.Add(-1)
		return false
	}
	stored := ids
	if stored.IsBorrowed() {
		stored = stored.Clone()
	}
	e := materializedPredCacheEntryPool.Get()
	e.ids = stored
	e.oversized = true
	e.touch(&s.matPredCacheClock)
	if _, loaded := s.matPredCache.LoadOrStore(key, e); loaded {
		s.matPredCacheOversizedCount.Add(-1)
		s.matPredCacheCount.Add(-1)
		e.ids = posting.List{}
		e.oversized = false
		materializedPredCacheEntryPool.Put(e)
		if !stored.SharesPayload(ids) {
			stored.Release()
		}
		return false
	}
	return true
}

func (s *indexSnapshot) tryStoreMaterializedPredOversized(key string, ids posting.List) bool {
	if parsed, ok := materializedPredKeyFromEncoded(key); ok {
		return s.tryStoreMaterializedPredOversizedKey(parsed, ids)
	}
	if s == nil || key == "" || ids.IsEmpty() {
		return false
	}
	if s.matPredCacheMaxCard == 0 || ids.Cardinality() <= s.matPredCacheMaxCard {
		return false
	}
	limit := s.materializedPredCacheLimit()
	if limit <= 0 {
		return false
	}
	if !s.reserveMaterializedPredOversizedSlot(limit) {
		return false
	}
	if !s.reserveMaterializedPredSlot(limit) {
		s.matPredCacheOversizedCount.Add(-1)
		return false
	}
	stored := ids
	if stored.IsBorrowed() {
		stored = stored.Clone()
	}
	e := materializedPredCacheEntryPool.Get()
	e.ids = stored
	e.oversized = true
	e.touch(&s.matPredCacheClock)
	if _, loaded := s.matPredCache.LoadOrStore(key, e); loaded {
		s.matPredCacheOversizedCount.Add(-1)
		s.matPredCacheCount.Add(-1)
		e.ids = posting.List{}
		e.oversized = false
		materializedPredCacheEntryPool.Put(e)
		if !stored.SharesPayload(ids) {
			stored.Release()
		}
		return false
	}
	return true
}

func (s *indexSnapshot) loadOrStoreMaterializedPredKey(key materializedPredKey, ids posting.List) (posting.List, bool) {
	if key.isZero() || ids.IsEmpty() {
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
	if !s.reserveMaterializedPredSlot(limit) {
		if cached, ok := s.loadMaterializedPredKey(key); ok {
			ids.Release()
			return cached, true
		}
		return ids, false
	}

	stored := ids
	if stored.IsBorrowed() {
		stored = stored.Clone()
	}
	e := materializedPredCacheEntryPool.Get()
	e.ids = stored
	actual, loaded := s.matPredCache.LoadOrStore(key, e)
	if loaded {
		s.matPredCacheCount.Add(-1)
		e.ids = posting.List{}
		materializedPredCacheEntryPool.Put(e)
		if !stored.SharesPayload(ids) {
			stored.Release()
		}
		ids.Release()
		entry, _ := actual.(*materializedPredCacheEntry)
		if entry == nil {
			return posting.List{}, true
		} else {
			entry.touch(&s.matPredCacheClock)
			return entry.ids.Borrow(), true
		}
	}
	e.touch(&s.matPredCacheClock)
	return stored.Borrow(), true
}

func (s *indexSnapshot) loadOrStoreMaterializedPred(key string, ids posting.List) (posting.List, bool) {
	if parsed, ok := materializedPredKeyFromEncoded(key); ok {
		return s.loadOrStoreMaterializedPredKey(parsed, ids)
	}
	if s == nil || key == "" || ids.IsEmpty() {
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
	if cached, ok := s.loadMaterializedPred(key); ok {
		ids.Release()
		return cached, true
	}
	if !s.reserveMaterializedPredSlot(limit) {
		if cached, ok := s.loadMaterializedPred(key); ok {
			ids.Release()
			return cached, true
		}
		return ids, false
	}

	stored := ids
	if stored.IsBorrowed() {
		stored = stored.Clone()
	}
	e := materializedPredCacheEntryPool.Get()
	e.ids = stored
	actual, loaded := s.matPredCache.LoadOrStore(key, e)
	if loaded {
		s.matPredCacheCount.Add(-1)
		e.ids = posting.List{}
		materializedPredCacheEntryPool.Put(e)
		if !stored.SharesPayload(ids) {
			stored.Release()
		}
		ids.Release()
		entry, _ := actual.(*materializedPredCacheEntry)
		if entry == nil {
			return posting.List{}, true
		}
		entry.touch(&s.matPredCacheClock)
		return entry.ids.Borrow(), true
	}
	e.touch(&s.matPredCacheClock)
	return stored.Borrow(), true
}

func (s *indexSnapshot) tryLoadOrStoreMaterializedPredOversizedKey(key materializedPredKey, ids posting.List) (posting.List, bool) {
	if key.isZero() || ids.IsEmpty() {
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
	if !s.reserveMaterializedPredOversizedSlot(limit) {
		return ids, false
	}
	if !s.reserveMaterializedPredSlot(limit) {
		s.matPredCacheOversizedCount.Add(-1)
		if cached, ok := s.loadMaterializedPredKey(key); ok {
			ids.Release()
			return cached, true
		}
		return ids, false
	}

	stored := ids
	if stored.IsBorrowed() {
		stored = stored.Clone()
	}
	e := materializedPredCacheEntryPool.Get()
	e.ids = stored
	e.oversized = true
	actual, loaded := s.matPredCache.LoadOrStore(key, e)
	if loaded {
		s.matPredCacheOversizedCount.Add(-1)
		s.matPredCacheCount.Add(-1)
		e.ids = posting.List{}
		e.oversized = false
		materializedPredCacheEntryPool.Put(e)
		if !stored.SharesPayload(ids) {
			stored.Release()
		}
		ids.Release()
		entry, _ := actual.(*materializedPredCacheEntry)
		if entry == nil {
			return posting.List{}, true
		} else {
			entry.touch(&s.matPredCacheClock)
			return entry.ids.Borrow(), true
		}
	}
	e.touch(&s.matPredCacheClock)
	return stored.Borrow(), true
}

func (s *indexSnapshot) tryLoadOrStoreMaterializedPredOversized(key string, ids posting.List) (posting.List, bool) {
	if parsed, ok := materializedPredKeyFromEncoded(key); ok {
		return s.tryLoadOrStoreMaterializedPredOversizedKey(parsed, ids)
	}
	if s == nil || key == "" || ids.IsEmpty() {
		return ids, false
	}
	if s.matPredCacheMaxCard == 0 || ids.Cardinality() <= s.matPredCacheMaxCard {
		return ids, false
	}
	limit := s.materializedPredCacheLimit()
	if limit <= 0 {
		return ids, false
	}
	if cached, ok := s.loadMaterializedPred(key); ok {
		ids.Release()
		return cached, true
	}
	if !s.reserveMaterializedPredOversizedSlot(limit) {
		return ids, false
	}
	if !s.reserveMaterializedPredSlot(limit) {
		s.matPredCacheOversizedCount.Add(-1)
		if cached, ok := s.loadMaterializedPred(key); ok {
			ids.Release()
			return cached, true
		}
		return ids, false
	}

	stored := ids
	if stored.IsBorrowed() {
		stored = stored.Clone()
	}
	e := materializedPredCacheEntryPool.Get()
	e.ids = stored
	e.oversized = true
	actual, loaded := s.matPredCache.LoadOrStore(key, e)
	if loaded {
		s.matPredCacheOversizedCount.Add(-1)
		s.matPredCacheCount.Add(-1)
		e.ids = posting.List{}
		e.oversized = false
		materializedPredCacheEntryPool.Put(e)
		if !stored.SharesPayload(ids) {
			stored.Release()
		}
		ids.Release()
		entry, _ := actual.(*materializedPredCacheEntry)
		if entry == nil {
			return posting.List{}, true
		}
		entry.touch(&s.matPredCacheClock)
		return entry.ids.Borrow(), true
	}
	e.touch(&s.matPredCacheClock)
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
		s.numericRangeBucketCache.Clear()
	}
	s.matPredCache.Range(func(_, v any) bool {
		entry, _ := v.(*materializedPredCacheEntry)
		if entry != nil {
			entry.ids = posting.List{}
			entry.oversized = false
			materializedPredCacheEntryPool.Put(entry)
		}
		return true
	})
	s.matPredCache.Clear()
	s.matPredCacheCount.Store(0)
	s.matPredCacheOversizedCount.Store(0)
	s.matPredCacheClock.Store(0)
	s.runtimeMatPredSeen.clear()
	s.orderORMatPredObserved.clear()
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
	if s == nil {
		return fieldIndexStorage{}, false
	}
	storage, ok := s.index[field]
	return storage, ok
}

func (s *indexSnapshot) nilFieldIndexSlice(field string) *[]index {
	if s == nil {
		return nil
	}
	return s.nilIndex[field].flatSlice()
}

func (s *indexSnapshot) nilFieldNameSet() map[string]struct{} {
	if s == nil {
		return nil
	}
	fields := make(map[string]struct{}, len(s.nilIndex))
	for f := range s.nilIndex {
		fields[f] = struct{}{}
	}
	return fields
}

func (s *indexSnapshot) fieldNameSet() map[string]struct{} {
	if s == nil {
		return nil
	}
	fields := make(map[string]struct{}, len(s.index))
	for f := range s.index {
		fields[f] = struct{}{}
	}
	return fields
}

func (s *indexSnapshot) indexedFieldNameSet() map[string]struct{} {
	if s == nil {
		return nil
	}
	fields := s.fieldNameSet()
	for f := range s.nilFieldNameSet() {
		fields[f] = struct{}{}
	}
	return fields
}

func (s *indexSnapshot) lenFieldNameSet() map[string]struct{} {
	if s == nil {
		return nil
	}
	fields := make(map[string]struct{}, len(s.lenIndex))
	for f := range s.lenIndex {
		fields[f] = struct{}{}
	}
	return fields
}

func (s *indexSnapshot) fieldLookupPostingRetained(field, key string) posting.List {
	if s == nil {
		return posting.List{}
	}
	return newFieldOverlayStorage(s.index[field]).lookupPostingRetained(key)
}

func (db *DB[K, V]) publishSnapshotRef(s *indexSnapshot) {
	if s == nil {
		return
	}
	db.snapshot.mu.Lock()
	defer db.snapshot.mu.Unlock()

	prev := db.snapshot.current.Load()
	ref := db.snapshot.bySeq[s.seq]
	if ref == nil {
		ref = snapshotRefPool.Get()
		db.snapshot.bySeq[s.seq] = ref
	}
	ref.snap = s
	db.snapshot.current.Store(s)
	if prev != nil && prev.seq != s.seq {
		db.retireSnapshotLocked(prev.seq)
	}
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
	defer db.snapshot.mu.Unlock()

	ref := db.snapshot.bySeq[seq]
	if ref == nil {
		return
	}
	if current := db.snapshot.current.Load(); current != nil && current.seq == seq {
		return
	}
	if ref.refs.Load() <= 0 {
		delete(db.snapshot.bySeq, seq)
		snapshotRefPool.Put(ref)
		return
	}
	ref.snap = nil
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
	db.snapshot.mu.Lock()
	defer db.snapshot.mu.Unlock()
	held := db.snapshot.bySeq[seq]
	if held != ref || held.refs.Load() != 0 || held.snap != nil {
		return
	}
	delete(db.snapshot.bySeq, seq)
	snapshotRefPool.Put(held)
}

func (db *DB[K, V]) retireSnapshotLocked(seq uint64) {
	ref := db.snapshot.bySeq[seq]
	if ref == nil {
		return
	}
	if current := db.snapshot.current.Load(); current != nil && current.seq == seq {
		return
	}
	if ref.refs.Load() != 0 {
		ref.snap = nil
		return
	}
	delete(db.snapshot.bySeq, seq)
	snapshotRefPool.Put(ref)
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

	s := db.getSnapshot()
	diag := SnapshotStats{
		Sequence: s.seq,
	}
	if !s.universe.IsEmpty() {
		diag.UniverseCard = s.universe.Cardinality()
	}

	db.snapshot.mu.RLock()
	diag.RegistrySize = len(db.snapshot.bySeq)
	for _, ref := range db.snapshot.bySeq {
		if ref.refs.Load() > 0 {
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
	defer nonEmpty.Release()

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

	nextIndex := make(map[string]fieldIndexStorage, len(db.indexedFieldAccess))
	for i, acc := range db.indexedFieldAccess {
		if storage := acc.materializeSnapshotOverlayStorageOwned(&fieldStates[i]); storage.keyCount() > 0 {
			nextIndex[acc.name] = storage
		}
	}

	nextNilIndex := make(map[string]fieldIndexStorage, len(db.indexedFieldAccess))
	for i, acc := range db.indexedFieldAccess {
		if storage := acc.materializeSnapshotOverlayNilStorageOwned(&fieldStates[i]); storage.keyCount() > 0 {
			nextNilIndex[acc.name] = storage
		}
	}

	nextLenIndex := make(map[string]fieldIndexStorage, len(db.fields))
	nextLenZeroComplement := make(map[string]bool, len(db.fields))
	for i, acc := range db.indexedFieldAccess {
		if !acc.field.Slice {
			continue
		}
		lengths := fieldStates[i].lengths
		fieldStates[i].lengths = nil
		storage, useZeroComplement := materializeLenFieldStorageOwned(universe, lengths)
		nextLenIndex[acc.name] = storage
		if useZeroComplement {
			nextLenZeroComplement[acc.name] = true
		}
	}

	snap := &indexSnapshot{
		seq:               seq,
		index:             nextIndex,
		nilIndex:          nextNilIndex,
		lenIndex:          nextLenIndex,
		lenZeroComplement: nextLenZeroComplement,
		universe:          universe,
		strmap:            db.strmap.snapshot(),
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
		copy(out, src)
		out[pos].IDs = unionPostingListsOwned(src[pos].IDs, add.IDs)
		return &out
	}

	out := make([]index, len(src)+1)
	copy(out, src[:pos])
	out[pos] = add
	copy(out[pos+1:], src[pos:])
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
			out = append(out, src[i])
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
		out = append(out, src[i:]...)
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

		index:             prev.index,
		nilIndex:          prev.nilIndex,
		lenIndex:          prev.lenIndex,
		lenZeroComplement: prev.lenZeroComplement,
		universe:          prev.universe.Clone(),
		strmap:            db.strmap.snapshot(),
	}
	db.initSnapshotRuntimeCaches(next)
	indexCloned := false
	nilIndexCloned := false
	lenIndexCloned := false

	fieldStates := make([]snapshotFieldInsertState, len(db.indexedFieldAccess))
	initSnapshotFieldInsertStateHints(fieldStates, db.indexedFieldAccess, prev, len(prepared))

	for i := range prepared {
		op := prepared[i]
		next.universe = next.universe.BuildAdded(op.idx)
		ptr := unsafe.Pointer(op.newVal)

		for _, acc := range db.indexedFieldAccess {
			acc.collectSnapshotInsertValue(ptr, op.idx, prev.lenZeroComplement[acc.name], &fieldStates[acc.ordinal])
		}
	}

	changedCount := 0
	for i, acc := range db.indexedFieldAccess {
		f := acc.name
		baseIndex := next.index[f]
		if storage := acc.mergeSnapshotInsertStorageOwned(baseIndex, &fieldStates[i], true); storage.keyCount() > 0 {
			if storage != baseIndex {
				ensureSnapshotFieldIndex(&next.index, prev.index, &indexCloned)[f] = storage
			}
		} else if baseIndex.keyCount() > 0 {
			delete(ensureSnapshotFieldIndex(&next.index, prev.index, &indexCloned), f)
		}
		baseNil := next.nilIndex[f]
		if storage := acc.mergeSnapshotInsertNilStorageOwned(baseNil, &fieldStates[i]); storage.keyCount() > 0 {
			if storage != baseNil {
				ensureSnapshotFieldIndex(&next.nilIndex, prev.nilIndex, &nilIndexCloned)[f] = storage
			}
		} else if baseNil.keyCount() > 0 {
			delete(ensureSnapshotFieldIndex(&next.nilIndex, prev.nilIndex, &nilIndexCloned), f)
		}
		if fieldStates[i].lengths != nil {
			baseLen := next.lenIndex[f]
			if storage := applyLenFieldPostingDiffStorageOwned(baseLen, fieldStates[i].lengths); storage != baseLen {
				ensureSnapshotFieldIndex(&next.lenIndex, prev.lenIndex, &lenIndexCloned)[f] = storage
			}
			fieldStates[i].lengths = nil
		}
		if fieldStates[i].changed {
			changedCount++
		}
		fieldStates[i].releaseOwned()
	}
	inheritNumericRangeBucketCache(next, prev)

	if changedCount > 0 {
		changed := make([]bool, len(db.indexedFieldAccess))
		for i := range fieldStates {
			if fieldStates[i].changed {
				changed[i] = true
			}
		}
		inheritMaterializedPredCache(db, next, prev, changed)
	} else {
		inheritMaterializedPredCache(db, next, prev, nil)
	}

	return next, true
}
