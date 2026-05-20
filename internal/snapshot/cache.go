package snapshot

import (
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/schema"
)

func inheritNumericRangeBucketCache(next, prev *View) {
	if prev == nil || next.numericRangeBucketCache == nil || prev.numericRangeBucketCache == nil {
		return
	}
	next.numericRangeBucketCache.InheritFrom(prev.numericRangeBucketCache, next.Index, next.IndexedFieldByName)
}

func (s *View) initRuntimeCaches(rt *schema.Runtime, cfg CacheConfig) {
	s.numericRangeBucketCache = qcache.GetNumericRangeBucketCache(len(rt.Indexed), cfg.MatPredMaxCard)
	if cfg.MatPredMaxEntries > 0 {
		s.matPredCache = qcache.GetMaterializedPredCache(cfg.MatPredMaxEntries, cfg.MatPredMaxCard)
	}
}

func (s *View) NumericRangeBucketCache() *qcache.NumericRangeBucketCache {
	return s.numericRangeBucketCache
}

func (s *View) MaterializedPredCache() *qcache.MaterializedPredCache {
	return s.matPredCache
}

func (s *View) NumericRangeBucketCacheEntry(field string, ordinal int, storage indexdata.FieldStorage, bucketSize, minFieldKeys int) *qcache.NumericRangeBucketEntry {
	if storage.KeyCount() == 0 {
		return nil
	}
	cache := s.numericRangeBucketCache
	if cache == nil {
		return nil
	}
	if entry, ok := cache.LoadSlot(field, ordinal); ok && entry.Storage() == storage {
		return entry
	}
	ov := indexdata.NewFieldIndexViewFromStorage(storage)
	idx, _ := qcache.BuildNumericRangeBucketIndex(ov, bucketSize, minFieldKeys)
	entry := qcache.GetNumericRangeBucketEntry(storage, idx, cache.MaxCardinality())
	if cached, ok := cache.LoadSlot(field, ordinal); ok && cached.Storage() == storage {
		entry.Release()
		return cached
	}
	cache.StoreSlot(field, ordinal, entry)
	return entry
}

func (s *View) MaterializedPredCacheEntryCount() int {
	if s.matPredCache == nil {
		return 0
	}
	return s.matPredCache.EntryCount()
}

func (s *View) AllowsMaterializedPredCard(card uint64) bool {
	if s.matPredCache == nil {
		return false
	}
	maxCard := s.matPredCache.MaxCardinality()
	return maxCard == 0 || card <= maxCard
}

func (s *View) CanShareModeratelyOversizedMaterializedPred(est uint64) bool {
	if s.matPredCache == nil || est == 0 {
		return false
	}
	cacheLimit := s.matPredCache.Limit()
	if cacheLimit <= 0 {
		return false
	}
	maxCard := s.matPredCache.MaxCardinality()
	if maxCard == 0 {
		return false
	}
	if est <= maxCard {
		return true
	}
	oversizedLimit := qcache.MaterializedPredOversizedLimit(cacheLimit)
	if oversizedLimit <= 0 {
		return false
	}
	mul := uint64(oversizedLimit)
	if ^uint64(0)/maxCard < mul {
		return false
	}
	return est <= maxCard*mul
}

func inheritMaterializedPredCache(next, prev *View, fields schema.IndexedFieldMap, changedFields []bool) {
	if prev == nil || next.matPredCache == nil || prev.matPredCache == nil {
		return
	}
	next.matPredCache.InheritFrom(prev.matPredCache, fields, changedFields)
}

func (s *View) LoadMaterializedPredKey(key qcache.MaterializedPredKey) (posting.List, bool) {
	if key.IsZero() || s.matPredCache == nil {
		return posting.List{}, false
	}
	return s.matPredCache.Load(key)
}

func (s *View) HasMaterializedPredKey(key qcache.MaterializedPredKey) bool {
	if key.IsZero() || s.matPredCache == nil {
		return false
	}
	return s.matPredCache.Has(key)
}

func (s *View) StoreMaterializedPredKey(key qcache.MaterializedPredKey, ids posting.List) {
	if key.IsZero() || s.matPredCache == nil {
		return
	}
	s.matPredCache.Store(key, ids)
}

// TryStoreMaterializedPredOversizedKey stores a small bounded number of oversized
// materialized postings per snapshot as a hot-cache fallback.
func (s *View) TryStoreMaterializedPredOversizedKey(key qcache.MaterializedPredKey, ids posting.List) bool {
	if key.IsZero() || ids.IsEmpty() || s.matPredCache == nil {
		return false
	}
	return s.matPredCache.TryStoreOversized(key, ids)
}

func (s *View) LoadOrStoreMaterializedPredKey(key qcache.MaterializedPredKey, ids posting.List) (posting.List, bool) {
	if key.IsZero() || ids.IsEmpty() || s.matPredCache == nil {
		return ids, false
	}
	return s.matPredCache.LoadOrStore(key, ids)
}

func (s *View) TryLoadOrStoreMaterializedPredOversizedKey(key qcache.MaterializedPredKey, ids posting.List) (posting.List, bool) {
	if key.IsZero() || ids.IsEmpty() || s.matPredCache == nil {
		return ids, false
	}
	return s.matPredCache.TryLoadOrStoreOversized(key, ids)
}

func (s *View) MaterializedPredCacheLimit() int {
	if s.matPredCache == nil {
		return 0
	}
	return s.matPredCache.Limit()
}

func (s *View) ClearRuntimeCaches() {
	if s.numericRangeBucketCache != nil {
		s.numericRangeBucketCache.ClearEntries()
	}
	if s.matPredCache != nil {
		s.matPredCache.Clear()
	}
	s.runtimeMatPredSeen.Clear()
	s.orderORMatPredObserved.Clear()
}

func (s *View) RuntimeMaterializedPredSeenEntryCount() int {
	return s.runtimeMatPredSeen.EntryCount()
}

func (s *View) OrderedORMaterializedPredObservedEntryCount() int {
	return s.orderORMatPredObserved.EntryCount()
}

func (s *View) drainRetiredRuntimeCaches() {
	if s.matPredCache != nil {
		s.matPredCache.DrainRetired()
	}
}

func (s *View) releaseRuntimeCaches() {
	if s.numericRangeBucketCache != nil {
		qcache.ReleaseNumericRangeBucketCache(s.numericRangeBucketCache)
		s.numericRangeBucketCache = nil
	}
	if s.matPredCache != nil {
		s.matPredCache.ReleaseRef()
		s.matPredCache = nil
	}
	s.runtimeMatPredSeen.Clear()
	s.orderORMatPredObserved.Clear()
}

func (s *View) ShouldPromoteRuntimeMaterializedPredKey(key qcache.MaterializedPredKey) bool {
	if key.IsZero() {
		return false
	}
	return s.runtimeMatPredSeen.TouchOrRemember(key, qcache.RecentKeyLimit(s.MaterializedPredCacheLimit()))
}

func (s *View) HasRuntimeMaterializedPredSeenKey(key qcache.MaterializedPredKey) bool {
	if key.IsZero() {
		return false
	}
	return s.runtimeMatPredSeen.Contains(key)
}

func (s *View) ShouldPromoteObservedOrderedORMaterializedPredKey(key qcache.MaterializedPredKey, observedWork uint64, buildWork uint64) bool {
	if key.IsZero() || observedWork == 0 || buildWork == 0 {
		return false
	}
	return s.orderORMatPredObserved.AddWorkAndShouldPromote(
		key,
		qcache.RecentKeyLimit(s.MaterializedPredCacheLimit()),
		observedWork,
		buildWork,
	)
}
