package snapshot

import (
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/schema"
)

type RuntimeCachesRetired struct {
	matPred      qcache.MaterializedPredRetired
	numericRange qcache.NumericRangeBucketRetired
}

func (r RuntimeCachesRetired) Release() {
	r.matPred.Release()
	r.numericRange.Release()
}

func (r RuntimeCachesRetired) Empty() bool {
	return r.matPred.IsEmpty() && r.numericRange.IsEmpty()
}

func inheritNumericRangeBucketCache(next, prev *View) {
	if prev == nil || next.numericRangeBucketCache == nil || prev.numericRangeBucketCache == nil {
		return
	}
	next.numericRangeBucketCache.InheritFrom(prev.numericRangeBucketCache, next.Index, next.IndexedFieldByName)
}

func (v *View) initRuntimeCaches(s *schema.Schema, cfg CacheConfig) {
	v.numericRangeBucketCache = qcache.GetNumericRangeBucketCacheWithRetireContext(
		len(s.Indexed),
		cfg.MatPredMaxCard,
		cfg.RuntimeCachesDirtyOwner,
		cfg.RuntimeCachesRetireEpoch,
	)
	if cfg.MatPredMaxEntries > 0 {
		v.matPredCache = qcache.GetMaterializedPredCacheWithRetireContext(
			cfg.MatPredMaxEntries,
			cfg.MatPredMaxCard,
			cfg.RuntimeCachesDirtyOwner,
			cfg.RuntimeCachesRetireEpoch,
		)
	}
}

func (v *View) NumericRangeBucketCache() *qcache.NumericRangeBucketCache {
	return v.numericRangeBucketCache
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
	return cache.LoadOrStoreSlot(field, ordinal, entry)
}

func (v *View) MaterializedPredCacheEntryCount() int {
	if v.matPredCache == nil {
		return 0
	}
	return v.matPredCache.EntryCount()
}

func (v *View) MaterializedPredCacheOversizedEntryCount() int32 {
	if v.matPredCache == nil {
		return 0
	}
	return v.matPredCache.OversizedCount()
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

func (v *View) MaterializedPredCacheMaxCardinality() uint64 {
	if v.matPredCache == nil {
		return 0
	}
	return v.matPredCache.MaxCardinality()
}

func (v *View) MaterializedPredCacheOversizedLimit() int32 {
	if v.matPredCache == nil {
		return 0
	}
	return qcache.MaterializedPredOversizedLimit(v.matPredCache.Limit())
}

func (v *View) CanRetainMaterializedPredRoute(potentialKeys int, universe uint64) bool {
	if v.matPredCache == nil || potentialKeys <= 0 {
		return false
	}
	limit := v.matPredCache.Limit()
	if potentialKeys > limit {
		return false
	}
	maxCard := v.matPredCache.MaxCardinality()
	if maxCard == 0 || universe <= maxCard {
		return true
	}
	return int(qcache.MaterializedPredOversizedLimit(limit)) >= potentialKeys
}

func inheritMaterializedPredCache(next, prev *View, changes qcache.FieldChangeSet) {
	if prev == nil || next.matPredCache == nil || prev.matPredCache == nil {
		return
	}
	next.matPredCache.InheritFrom(prev.matPredCache, changes)
	next.runtimeMatPredObserved.InheritObservedWorkFrom(
		&prev.runtimeMatPredObserved,
		changes,
		qcache.RecentKeyLimit(next.MaterializedPredCacheLimit()),
	)
	next.runtimeMatPredDirty.InheritChangedObservedWorkFrom(
		&prev.runtimeMatPredObserved,
		changes,
		qcache.RecentKeyLimit(next.MaterializedPredCacheLimit()),
	)
}

func (v *View) evictRuntimeMaterializedPredField(field string) {
	if v.matPredCache != nil {
		v.matPredCache.EvictField(field)
	}
	v.runtimeMatPredSeen.EvictField(field)
	v.runtimeMatPredObserved.EvictField(field)
	v.runtimeMatPredDirty.EvictField(field)
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
		ids.Release()
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
	v.TakeRetiredRuntimeCaches().Release()
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

func (v *View) RuntimeCachesDirty() bool {
	return (v.matPredCache != nil && v.matPredCache.RetiredDirty()) ||
		(v.numericRangeBucketCache != nil && v.numericRangeBucketCache.RetiredDirty())
}

func (v *View) TakeRetiredRuntimeCaches() RuntimeCachesRetired {
	return v.TakeRetiredRuntimeCachesBefore(^uint64(0))
}

func (v *View) TakeRetiredRuntimeCachesBefore(safeEpoch uint64) RuntimeCachesRetired {
	var retired RuntimeCachesRetired
	// The root registry supplies safeEpoch after retaining current read states;
	// caches detach only payloads older than that reader barrier.
	if v.matPredCache != nil && v.matPredCache.RetiredDirty() {
		retired.matPred = v.matPredCache.TakeRetiredBefore(safeEpoch)
	}
	if v.numericRangeBucketCache != nil && v.numericRangeBucketCache.RetiredDirty() {
		retired.numericRange = v.numericRangeBucketCache.TakeRetiredBefore(safeEpoch)
	}
	return retired
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
