package rbi

import (
	"sync/atomic"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/strmap"
)

// indexSnapshot is an immutable read-view published atomically for query paths.
type indexSnapshot struct {
	seq uint64

	index              []indexdata.FieldStorage
	nilIndex           []indexdata.FieldStorage
	lenIndex           []indexdata.FieldStorage
	lenZeroComplement  []bool
	measure            []indexdata.MeasureStorage
	indexedFieldByName schema.IndexedFieldMap
	universe           posting.List
	universeOwner      *snapshotPostingOwner
	strmap             *strmap.Snapshot

	numericRangeBucketCache *qcache.NumericRangeBucketCache

	matPredCache           *qcache.MaterializedPredCache
	runtimeMatPredSeen     qcache.RecentKeyCache
	orderORMatPredObserved qcache.RecentKeyCache
}

type snapshotRef struct {
	snap    *indexSnapshot
	retired *pooled.Slice[*indexSnapshot]
	refs    atomic.Int64
}

var snapshotRefPool = pooled.Pointers[snapshotRef]{Clear: true}

var snapshotRetiredListPool = pooled.Slices[*indexSnapshot]{Clear: true}

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
	if next == nil || prev == nil || next.numericRangeBucketCache == nil || prev.numericRangeBucketCache == nil {
		return
	}
	next.numericRangeBucketCache.InheritFrom(prev.numericRangeBucketCache, next.index, next.indexedFieldByName)
}

func inheritMaterializedPredCache(next, prev *indexSnapshot, fields schema.IndexedFieldMap, changedFields []bool) {
	if next == nil || prev == nil || next.matPredCache == nil || prev.matPredCache == nil {
		return
	}
	next.matPredCache.InheritFrom(prev.matPredCache, fields, changedFields)
}

func materializedPredCacheKeyFromString(key string) (qcache.MaterializedPredKey, bool) {
	if key == "" {
		return qcache.MaterializedPredKey{}, false
	}
	if parsed, ok := qcache.MaterializedPredKeyFromEncoded(key); ok {
		return parsed, true
	}
	return qcache.MaterializedPredKeyFromOpaque(key), true
}

func (s *indexSnapshot) loadMaterializedPredKey(key qcache.MaterializedPredKey) (posting.List, bool) {
	if s == nil || key.IsZero() || s.matPredCache == nil {
		return posting.List{}, false
	}
	return s.matPredCache.Load(key)
}

func (s *indexSnapshot) loadMaterializedPred(key string) (posting.List, bool) {
	parsed, ok := materializedPredCacheKeyFromString(key)
	if !ok {
		return posting.List{}, false
	}
	return s.loadMaterializedPredKey(parsed)
}

func (s *indexSnapshot) storeMaterializedPredKey(key qcache.MaterializedPredKey, ids posting.List) {
	if key.IsZero() || s == nil || s.matPredCache == nil {
		return
	}
	s.matPredCache.Store(key, ids)
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
func (s *indexSnapshot) tryStoreMaterializedPredOversizedKey(key qcache.MaterializedPredKey, ids posting.List) bool {
	if key.IsZero() || ids.IsEmpty() || s == nil || s.matPredCache == nil {
		return false
	}
	return s.matPredCache.TryStoreOversized(key, ids)
}

func (s *indexSnapshot) tryStoreMaterializedPredOversized(key string, ids posting.List) bool {
	parsed, ok := materializedPredCacheKeyFromString(key)
	if !ok {
		return false
	}
	return s.tryStoreMaterializedPredOversizedKey(parsed, ids)
}

func (s *indexSnapshot) loadOrStoreMaterializedPredKey(key qcache.MaterializedPredKey, ids posting.List) (posting.List, bool) {
	if key.IsZero() || ids.IsEmpty() || s == nil || s.matPredCache == nil {
		return ids, false
	}
	return s.matPredCache.LoadOrStore(key, ids)
}

func (s *indexSnapshot) tryLoadOrStoreMaterializedPredOversizedKey(key qcache.MaterializedPredKey, ids posting.List) (posting.List, bool) {
	if key.IsZero() || ids.IsEmpty() || s == nil || s.matPredCache == nil {
		return ids, false
	}
	return s.matPredCache.TryLoadOrStoreOversized(key, ids)
}

func (s *indexSnapshot) materializedPredCacheLimit() int {
	if s == nil || s.matPredCache == nil {
		return 0
	}
	return s.matPredCache.Limit()
}

func (s *indexSnapshot) clearRuntimeCachesForTesting() {
	if s == nil {
		return
	}
	if s.numericRangeBucketCache != nil {
		s.numericRangeBucketCache.ClearEntries()
	}
	if s.matPredCache != nil {
		s.matPredCache.Clear()
	}
	s.runtimeMatPredSeen.Clear()
	s.orderORMatPredObserved.Clear()
}

func (s *indexSnapshot) drainRetiredRuntimeCaches() {
	if s == nil {
		return
	}
	if s.matPredCache != nil {
		s.matPredCache.DrainRetired()
	}
}

func (s *indexSnapshot) releaseRuntimeCaches() {
	if s == nil {
		return
	}
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

func (s *indexSnapshot) shouldPromoteRuntimeMaterializedPredKey(key qcache.MaterializedPredKey) bool {
	if key.IsZero() {
		return false
	}
	return s.runtimeMatPredSeen.TouchOrRemember(key, qcache.RecentKeyLimit(s.materializedPredCacheLimit()))
}

func (s *indexSnapshot) shouldPromoteRuntimeMaterializedPred(key string) bool {
	if parsed, ok := qcache.MaterializedPredKeyFromEncoded(key); ok {
		return s.shouldPromoteRuntimeMaterializedPredKey(parsed)
	}
	if s == nil || key == "" {
		return false
	}
	return s.runtimeMatPredSeen.TouchOrRemember(
		qcache.MaterializedPredKeyFromOpaque(key),
		qcache.RecentKeyLimit(s.materializedPredCacheLimit()),
	)
}

func (s *indexSnapshot) shouldPromoteObservedOrderedORMaterializedPredKey(key qcache.MaterializedPredKey, observedWork uint64, buildWork uint64) bool {
	if key.IsZero() || observedWork == 0 || buildWork == 0 {
		return false
	}
	return s.orderORMatPredObserved.AddWorkAndShouldPromote(
		key,
		qcache.RecentKeyLimit(s.materializedPredCacheLimit()),
		observedWork,
		buildWork,
	)
}

func (s *indexSnapshot) shouldPromoteObservedOrderedORMaterializedPred(key string, observedWork uint64, buildWork uint64) bool {
	if parsed, ok := qcache.MaterializedPredKeyFromEncoded(key); ok {
		return s.shouldPromoteObservedOrderedORMaterializedPredKey(parsed, observedWork, buildWork)
	}
	if s == nil || key == "" || observedWork == 0 || buildWork == 0 {
		return false
	}
	return s.orderORMatPredObserved.AddWorkAndShouldPromote(
		qcache.MaterializedPredKeyFromOpaque(key),
		qcache.RecentKeyLimit(s.materializedPredCacheLimit()),
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
	if !ok || acc.Ordinal >= len(s.index) {
		return indexdata.FieldStorage{}, false
	}
	storage := s.index[acc.Ordinal]
	return storage, storage.KeyCount() > 0
}

func (s *indexSnapshot) nilFieldNameSet() map[string]struct{} {
	if s == nil || s.nilIndex == nil {
		return nil
	}
	fields := make(map[string]struct{}, len(s.indexedFieldByName))
	for f, acc := range s.indexedFieldByName {
		if acc.Ordinal < len(s.nilIndex) && s.nilIndex[acc.Ordinal].KeyCount() > 0 {
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
		if acc.Ordinal < len(s.index) && s.index[acc.Ordinal].KeyCount() > 0 {
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
		if !acc.Field.Slice || acc.Ordinal >= len(s.lenIndex) || s.lenIndex[acc.Ordinal].KeyCount() == 0 {
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
	if !ok || acc.Ordinal >= len(s.index) {
		return posting.List{}
	}
	return indexdata.NewFieldOverlayStorage(s.index[acc.Ordinal]).LookupPostingRetained(key)
}

func (s *indexSnapshot) fieldLookupPostingRetainedKey(field string, key keycodec.IndexLookupKey) posting.List {
	if s == nil || s.index == nil {
		return posting.List{}
	}
	acc, ok := s.indexedFieldByName[field]
	if !ok || acc.Ordinal >= len(s.index) {
		return posting.List{}
	}
	ov := indexdata.NewFieldOverlayStorage(s.index[acc.Ordinal])
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
		drainCache *qcache.MaterializedPredCache
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
				drainCache.Retain()
			}
		}
		sm.mu.Unlock()

		releaseRetiredSnapshots(retired)
		if drainCache != nil {
			drainCache.DrainRetired()
			drainCache.ReleaseRef()
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

func (qe *queryEngine) buildPreparedSnapshotNoLock(seq uint64, strMap *strmap.Mapper, patchFields map[string]*schema.Field, entries []snapshotBatchEntry) *indexSnapshot {
	prev := qe.getSnapshot()
	if snap, ok := qe.buildPreparedSnapshotFromEmptyBaseNoLock(seq, prev, strMap, entries); ok {
		return snap
	}
	if snap, ok := qe.buildPreparedSnapshotInsertOnlyNoLock(seq, prev, strMap, entries); ok {
		return snap
	}
	return qe.buildPreparedSnapshotAggregatedNoLock(seq, prev, strMap, patchFields, entries)
}

func (qe *queryEngine) buildPreparedSnapshotFromEmptyBaseNoLock(seq uint64, prev *indexSnapshot, strMap *strmap.Mapper, entries []snapshotBatchEntry) (*indexSnapshot, bool) {
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

	fieldStates := make([]schema.OverlayState, len(qe.schema.Indexed))
	measureStates := make([][]indexdata.MeasureEntry, len(qe.schema.Measures))

	for i := range entries {
		if hasRepeated && lastByIdx[entries[i].idx] != i {
			continue
		}
		op := entries[i]
		ptr := op.newVal

		for _, acc := range qe.schema.Indexed {
			acc.CollectOverlayValue(ptr, op.idx, &fieldStates[acc.Ordinal])
		}
		for _, acc := range qe.schema.Measures {
			if value, ok := acc.Read(ptr); ok {
				buf := measureStates[acc.Ordinal]
				if buf == nil {
					buf = indexdata.GetMeasureEntrySlice(0)
					measureStates[acc.Ordinal] = buf
				}
				buf = append(buf, indexdata.MeasureEntry{ID: op.idx, Value: value})
				measureStates[acc.Ordinal] = buf
			}
		}
	}
	if hasRepeated {
		uint64IntMapPool.Put(lastByIdx)
	}

	slotCount := len(qe.schema.Indexed)
	nextIndex := indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	for i, acc := range qe.schema.Indexed {
		state := &fieldStates[i]
		storage := state.MaterializeStorage(acc.Field.KeyKind == schema.FieldWriteKeysOrderedU64)
		if storage.KeyCount() > 0 {
			nextIndex[i] = storage
		}
	}

	nextNilIndex := indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	for i := range qe.schema.Indexed {
		if storage := fieldStates[i].MaterializeNilStorage(); storage.KeyCount() > 0 {
			nextNilIndex[i] = storage
		}
	}

	nextLenIndex := indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	nextLenZeroComplement := pooled.GetBoolSlice(slotCount)[:slotCount]
	clear(nextLenZeroComplement)
	for i, acc := range qe.schema.Indexed {
		if !acc.Field.Slice {
			continue
		}
		storage, useZeroComplement := fieldStates[i].MaterializeLenStorage(universe)
		nextLenIndex[i] = storage
		if useZeroComplement {
			nextLenZeroComplement[i] = true
		}
	}
	measureSlotCount := len(qe.schema.Measures)
	nextMeasure := indexdata.GetMeasureStorageSlice(measureSlotCount)[:measureSlotCount]
	for i := range qe.schema.Measures {
		storage := indexdata.NewMeasureStorageFromEntriesOwned(measureStates[i])
		nextMeasure[i] = storage
		measureStates[i] = nil
	}

	var sm *strmap.Snapshot
	if strMap != nil {
		sm = strMap.Snapshot()
	}
	snap := &indexSnapshot{
		seq:                seq,
		index:              nextIndex,
		nilIndex:           nextNilIndex,
		lenIndex:           nextLenIndex,
		lenZeroComplement:  nextLenZeroComplement,
		measure:            nextMeasure,
		indexedFieldByName: qe.schema.IndexedByName,
		universe:           universe,
		strmap:             sm,
	}
	qe.initSnapshotRuntimeCaches(snap)
	inheritNumericRangeBucketCache(snap, prev)
	changedCount := 0
	for i := range fieldStates {
		if fieldStates[i].Changed() {
			changedCount++
		}
	}
	if changedCount > 0 {
		changed := pooled.GetBoolSlice(len(qe.schema.Indexed))[:len(qe.schema.Indexed)]
		clear(changed)
		for i := range fieldStates {
			if fieldStates[i].Changed() {
				changed[i] = true
			}
		}
		inheritMaterializedPredCache(snap, prev, qe.schema.IndexedByName, changed)
		pooled.ReleaseBoolSlice(changed)
	} else {
		inheritMaterializedPredCache(snap, prev, qe.schema.IndexedByName, nil)
	}
	snap.ensureUniverseOwner()
	return snap, true
}

func (qe *queryEngine) buildPreparedSnapshotInsertOnlyNoLock(seq uint64, prev *indexSnapshot, strMap *strmap.Mapper, entries []snapshotBatchEntry) (*indexSnapshot, bool) {
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

	var sm *strmap.Snapshot
	if strMap != nil {
		sm = strMap.Snapshot()
	}
	next := &indexSnapshot{
		seq: seq,

		index:              indexdata.CloneFieldStorageSlots(prev.index, len(qe.schema.Indexed)),
		nilIndex:           indexdata.CloneFieldStorageSlots(prev.nilIndex, len(qe.schema.Indexed)),
		lenIndex:           indexdata.CloneFieldStorageSlots(prev.lenIndex, len(qe.schema.Indexed)),
		lenZeroComplement:  cloneFieldIndexBoolSlots(prev.lenZeroComplement, len(qe.schema.Indexed)),
		measure:            indexdata.CloneMeasureStorageSlots(prev.measure, len(qe.schema.Measures)),
		indexedFieldByName: qe.schema.IndexedByName,
		universe:           prev.universe.Clone(),
		strmap:             sm,
	}
	next.universe = next.universe.BuildMergedOwned(addedUniverse)
	qe.initSnapshotRuntimeCaches(next)

	fieldStates := schema.GetInsertStates(len(qe.schema.Indexed))
	schema.InitInsertStateHints(fieldStates, qe.schema.Indexed, prev.index, prev.nilIndex, prev.lenIndex, len(entries))
	measureDeltas := indexdata.NewMeasureDeltaBatch(len(qe.schema.Measures))

	for i := range entries {
		op := entries[i]
		ptr := op.newVal

		for _, acc := range qe.schema.Indexed {
			useZeroComplement := acc.Ordinal < len(prev.lenZeroComplement) && prev.lenZeroComplement[acc.Ordinal]
			acc.CollectInsertValue(ptr, op.idx, useZeroComplement, &fieldStates[acc.Ordinal])
		}
		for _, acc := range qe.schema.Measures {
			if value, ok := acc.Read(ptr); ok {
				measureDeltas.Append(acc.Ordinal, op.idx, true, value)
			}
		}
	}

	changedCount := 0
	var changed []bool
	for i, acc := range qe.schema.Indexed {
		state := &fieldStates[i]
		baseIndex := next.index[i]
		if storage := acc.MergeInsertStorageOwned(baseIndex, state, true); storage.KeyCount() > 0 {
			if storage != baseIndex {
				next.index[i] = storage
			}
		} else if baseIndex.KeyCount() > 0 {
			next.index[i] = indexdata.FieldStorage{}
		}
		baseNil := next.nilIndex[i]
		if storage := acc.MergeInsertNilStorageOwned(baseNil, state); storage.KeyCount() > 0 {
			if storage != baseNil {
				next.nilIndex[i] = storage
			}
		} else if baseNil.KeyCount() > 0 {
			next.nilIndex[i] = indexdata.FieldStorage{}
		}
		if lenDiff := state.LenDiff(); lenDiff != nil {
			baseLen := next.lenIndex[i]
			if storage := baseLen.ApplyLenPostingDiffRetainOwned(lenDiff); storage != baseLen {
				next.lenIndex[i] = storage
			}
		}
		if state.Changed() {
			changedCount++
			if changed == nil {
				changed = pooled.GetBoolSlice(len(qe.schema.Indexed))[:len(qe.schema.Indexed)]
				clear(changed)
			}
			changed[i] = true
		}
		state.Reset()
	}
	measureDeltas.ApplyToMeasureStorageSlotsOwned(next.measure)
	measureDeltas.Release()
	inheritNumericRangeBucketCache(next, prev)

	if changedCount > 0 {
		inheritMaterializedPredCache(next, prev, qe.schema.IndexedByName, changed)
		pooled.ReleaseBoolSlice(changed)
	} else {
		inheritMaterializedPredCache(next, prev, qe.schema.IndexedByName, nil)
	}
	next.retainSharedOwnedStorageFrom(prev)
	schema.ReleaseInsertStates(fieldStates)

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
