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

func (v *View) initRuntimeCaches(s *schema.Schema, cfg CacheConfig) {
	v.numericRangeBucketCache = qcache.GetNumericRangeBucketCache(len(s.Indexed), cfg.MatPredMaxCard)
	if cfg.MatPredMaxEntries > 0 {
		v.matPredCache = qcache.GetMaterializedPredCache(cfg.MatPredMaxEntries, cfg.MatPredMaxCard)
	}
}

func (v *View) NumericRangeBucketCache() *qcache.NumericRangeBucketCache {
	return v.numericRangeBucketCache
}

func (v *View) MaterializedPredCache() *qcache.MaterializedPredCache {
	return v.matPredCache
}

func (v *View) NumericRangeBucketCacheEntry(field string, ordinal int, storage indexdata.FieldStorage, bucketSize, minFieldKeys int) *qcache.NumericRangeBucketEntry {
	if storage.KeyCount() == 0 {
		return nil
	}
	cache := v.numericRangeBucketCache
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

func (v *View) MaterializedPredCacheEntryCount() int {
	if v.matPredCache == nil {
		return 0
	}
	return v.matPredCache.EntryCount()
}

func (v *View) AllowsMaterializedPredCard(card uint64) bool {
	if v.matPredCache == nil {
		return false
	}
	maxCard := v.matPredCache.MaxCardinality()
	return maxCard == 0 || card <= maxCard
}

func (v *View) CanShareModeratelyOversizedMaterializedPred(est uint64) bool {
	if v.matPredCache == nil || est == 0 {
		return false
	}
	cacheLimit := v.matPredCache.Limit()
	if cacheLimit <= 0 {
		return false
	}
	maxCard := v.matPredCache.MaxCardinality()
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
	next.runtimeMatPredObserved.InheritObservedWorkFrom(
		&prev.runtimeMatPredObserved,
		fields,
		changedFields,
		qcache.RecentKeyLimit(next.MaterializedPredCacheLimit()),
	)
	next.runtimeMatPredDirty.InheritChangedObservedWorkFrom(
		&prev.runtimeMatPredObserved,
		fields,
		changedFields,
		qcache.RecentKeyLimit(next.MaterializedPredCacheLimit()),
	)
}

func (v *View) LoadMaterializedPredKey(key qcache.MaterializedPredKey) (posting.List, bool) {
	if key.IsZero() || v.matPredCache == nil {
		return posting.List{}, false
	}
	return v.matPredCache.Load(key)
}

func (v *View) HasMaterializedPredKey(key qcache.MaterializedPredKey) bool {
	if key.IsZero() || v.matPredCache == nil {
		return false
	}
	return v.matPredCache.Has(key)
}

func (v *View) StoreMaterializedPredKey(key qcache.MaterializedPredKey, ids posting.List) {
	if key.IsZero() || v.matPredCache == nil {
		return
	}
	v.matPredCache.Store(key, ids)
}

// TryStoreMaterializedPredOversizedKey stores a small bounded number of oversized
// materialized postings per snapshot as a hot-cache fallback.
func (v *View) TryStoreMaterializedPredOversizedKey(key qcache.MaterializedPredKey, ids posting.List) bool {
	if key.IsZero() || ids.IsEmpty() || v.matPredCache == nil {
		return false
	}
	return v.matPredCache.TryStoreOversized(key, ids)
}

func (v *View) LoadOrStoreMaterializedPredKey(key qcache.MaterializedPredKey, ids posting.List) (posting.List, bool) {
	if key.IsZero() || ids.IsEmpty() || v.matPredCache == nil {
		return ids, false
	}
	return v.matPredCache.LoadOrStore(key, ids)
}

func (v *View) TryLoadOrStoreMaterializedPredOversizedKey(key qcache.MaterializedPredKey, ids posting.List) (posting.List, bool) {
	if key.IsZero() || ids.IsEmpty() || v.matPredCache == nil {
		return ids, false
	}
	return v.matPredCache.TryLoadOrStoreOversized(key, ids)
}

func (v *View) MaterializedPredCacheLimit() int {
	if v.matPredCache == nil {
		return 0
	}
	return v.matPredCache.Limit()
}

func (v *View) ClearRuntimeCaches() {
	if v.numericRangeBucketCache != nil {
		v.numericRangeBucketCache.ClearEntries()
	}
	if v.matPredCache != nil {
		v.matPredCache.Clear()
	}
	v.runtimeMatPredSeen.Clear()
	v.runtimeMatPredObserved.Clear()
	v.runtimeMatPredDirty.Clear()
}

func (v *View) RuntimeMaterializedPredSeenEntryCount() int {
	return v.runtimeMatPredSeen.EntryCount()
}

func (v *View) drainRetiredRuntimeCaches() {
	if v.numericRangeBucketCache != nil {
		v.numericRangeBucketCache.DrainRetired()
	}
	if v.matPredCache != nil {
		v.matPredCache.DrainRetired()
	}
}

func (v *View) releaseRuntimeCaches() {
	if v.numericRangeBucketCache != nil {
		qcache.ReleaseNumericRangeBucketCache(v.numericRangeBucketCache)
		v.numericRangeBucketCache = nil
	}
	if v.matPredCache != nil {
		v.matPredCache.ReleaseRef()
		v.matPredCache = nil
	}
	v.runtimeMatPredSeen.Clear()
	v.runtimeMatPredObserved.Clear()
	v.runtimeMatPredDirty.Clear()
}

func (v *View) ShouldPromoteRuntimeMaterializedPredKey(key qcache.MaterializedPredKey) bool {
	if key.IsZero() {
		return false
	}
	return v.runtimeMatPredSeen.TouchOrRemember(key, qcache.RecentKeyLimit(v.MaterializedPredCacheLimit()))
}

func (v *View) HasRuntimeMaterializedPredSeenKey(key qcache.MaterializedPredKey) bool {
	if key.IsZero() {
		return false
	}
	return v.runtimeMatPredSeen.Contains(key)
}

func (v *View) ShouldPromoteObservedMaterializedPredKey(key qcache.MaterializedPredKey, observedWork uint64, buildWork uint64) bool {
	if key.IsZero() || observedWork == 0 || buildWork == 0 {
		return false
	}
	promote, hadWork := v.runtimeMatPredObserved.AddWorkAndShouldPromote(
		key,
		qcache.RecentKeyLimit(v.MaterializedPredCacheLimit()),
		observedWork,
		buildWork,
	)
	return promote && (hadWork || v.runtimeMatPredDirty.Work(key) == 0)
}

func (v *View) ObservedMaterializedPredWork(key qcache.MaterializedPredKey) uint64 {
	if key.IsZero() {
		return 0
	}
	return v.runtimeMatPredObserved.Work(key)
}

func (v *View) DirtyObservedMaterializedPredWork(key qcache.MaterializedPredKey) uint64 {
	if key.IsZero() {
		return 0
	}
	return v.runtimeMatPredDirty.Work(key)
}
