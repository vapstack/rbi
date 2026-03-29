package rbi

import (
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

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
}

type materializedPredCacheEntry struct {
	ids       posting.List
	oversized bool
	stamp     atomic.Uint64
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

var snapshotRefPool = sync.Pool{
	New: func() any { return new(snapshotRef) },
}

func getSnapshotRef() *snapshotRef {
	return snapshotRefPool.Get().(*snapshotRef)
}

func releaseSnapshotRef(ref *snapshotRef) {
	ref.snap = nil
	ref.refs.Store(0)
	snapshotRefPool.Put(ref)
}

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

func newNumericRangeBucketCache() *sync.Map {
	return &sync.Map{}
}

func inheritNumericRangeBucketCache(next, prev *indexSnapshot) {
	if next == nil {
		return
	}
	if next.numericRangeBucketCache == nil {
		next.numericRangeBucketCache = newNumericRangeBucketCache()
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

func materializedPredCacheFieldName(key string) string {
	if key == "" {
		return ""
	}
	if i := strings.IndexByte(key, '\x1f'); i >= 0 {
		return key[:i]
	}
	return key
}

func inheritMaterializedPredCache(next, prev *indexSnapshot, changedFields map[string]struct{}) {
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
		key, ok := k.(string)
		if !ok || key == "" {
			return true
		}
		f := materializedPredCacheFieldName(key)
		if f == "" {
			return true
		}
		if changedFields != nil {
			if _, touched := changedFields[f]; touched {
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
		copied := &materializedPredCacheEntry{
			ids:       cachedIDs,
			oversized: oversizedEntry,
		}
		if entry != nil {
			stamp := entry.stamp.Load()
			copied.stamp.Store(stamp)
			if stamp > maxStamp {
				maxStamp = stamp
			}
		}
		if _, loaded := next.matPredCache.LoadOrStore(key, copied); loaded {
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

func (s *indexSnapshot) loadMaterializedPred(key string) (posting.List, bool) {
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

func (s *indexSnapshot) storeMaterializedPred(key string, ids posting.List) {
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
	e := &materializedPredCacheEntry{ids: stored}
	e.touch(&s.matPredCacheClock)
	if _, loaded := s.matPredCache.LoadOrStore(key, e); loaded {
		s.matPredCacheCount.Add(-1)
		if !stored.SharesPayload(ids) {
			stored.Release()
		}
	}
}

// tryStoreMaterializedPredOversized stores a small bounded number of oversized
// materialized postings per snapshot as a hot-cache fallback.
func (s *indexSnapshot) tryStoreMaterializedPredOversized(key string, ids posting.List) bool {
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
	e := &materializedPredCacheEntry{ids: stored, oversized: true}
	e.touch(&s.matPredCacheClock)
	if _, loaded := s.matPredCache.LoadOrStore(key, e); loaded {
		s.matPredCacheOversizedCount.Add(-1)
		s.matPredCacheCount.Add(-1)
		if !stored.SharesPayload(ids) {
			stored.Release()
		}
		return false
	}
	return true
}

func (s *indexSnapshot) loadOrStoreMaterializedPred(key string, ids *posting.List) bool {
	if s == nil || key == "" || ids == nil || ids.IsEmpty() {
		return false
	}
	limit := s.materializedPredCacheLimit()
	if limit <= 0 {
		return false
	}
	if !ids.IsEmpty() && s.matPredCacheMaxCard > 0 &&
		ids.Cardinality() > s.matPredCacheMaxCard {
		return false
	}
	if cached, ok := s.loadMaterializedPred(key); ok {
		ids.Release()
		*ids = cached
		return true
	}
	if !s.reserveMaterializedPredSlot(limit) {
		if cached, ok := s.loadMaterializedPred(key); ok {
			ids.Release()
			*ids = cached
			return true
		}
		return false
	}

	stored := *ids
	if stored.IsBorrowed() {
		stored = stored.Clone()
	}
	e := &materializedPredCacheEntry{ids: stored}
	actual, loaded := s.matPredCache.LoadOrStore(key, e)
	if loaded {
		s.matPredCacheCount.Add(-1)
		if !stored.SharesPayload(*ids) {
			stored.Release()
		}
		ids.Release()
		entry, _ := actual.(*materializedPredCacheEntry)
		if entry == nil {
			*ids = posting.List{}
		} else {
			entry.touch(&s.matPredCacheClock)
			*ids = entry.ids.Borrow()
		}
		return true
	}
	e.touch(&s.matPredCacheClock)
	*ids = stored.Borrow()
	return true
}

func (s *indexSnapshot) tryLoadOrStoreMaterializedPredOversized(key string, ids *posting.List) bool {
	if s == nil || key == "" || ids == nil || ids.IsEmpty() {
		return false
	}
	if s.matPredCacheMaxCard == 0 || ids.Cardinality() <= s.matPredCacheMaxCard {
		return false
	}
	limit := s.materializedPredCacheLimit()
	if limit <= 0 {
		return false
	}
	if cached, ok := s.loadMaterializedPred(key); ok {
		ids.Release()
		*ids = cached
		return true
	}
	if !s.reserveMaterializedPredOversizedSlot(limit) {
		return false
	}
	if !s.reserveMaterializedPredSlot(limit) {
		s.matPredCacheOversizedCount.Add(-1)
		if cached, ok := s.loadMaterializedPred(key); ok {
			ids.Release()
			*ids = cached
			return true
		}
		return false
	}

	stored := *ids
	if stored.IsBorrowed() {
		stored = stored.Clone()
	}
	e := &materializedPredCacheEntry{ids: stored, oversized: true}
	actual, loaded := s.matPredCache.LoadOrStore(key, e)
	if loaded {
		s.matPredCacheOversizedCount.Add(-1)
		s.matPredCacheCount.Add(-1)
		if !stored.SharesPayload(*ids) {
			stored.Release()
		}
		ids.Release()
		entry, _ := actual.(*materializedPredCacheEntry)
		if entry == nil {
			*ids = posting.List{}
		} else {
			entry.touch(&s.matPredCacheClock)
			*ids = entry.ids.Borrow()
		}
		return true
	}
	e.touch(&s.matPredCacheClock)
	*ids = stored.Borrow()
	return true
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
	s.matPredCache.Clear()
	s.matPredCacheCount.Store(0)
	s.matPredCacheOversizedCount.Store(0)
	s.matPredCacheClock.Store(0)
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

func (s *indexSnapshot) fieldIndexSlice(field string) *[]index {
	if s == nil {
		return nil
	}
	return s.index[field].flatSlice()
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

func (s *indexSnapshot) lenFieldIndexSlice(field string) *[]index {
	if s == nil {
		return nil
	}
	return s.lenIndex[field].flatSlice()
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
		ref = getSnapshotRef()
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
		ref = getSnapshotRef()
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
		releaseSnapshotRef(ref)
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
	releaseSnapshotRef(held)
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
	releaseSnapshotRef(ref)
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
	merged.MergeOwned(add)
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
				nonEmpty.OrInPlace(ids)
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
		p.Add(idx)
		lenMap[ln] = p
	}

	empty := universe.Clone()
	empty.AndNotInPlace(nonEmpty)

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
		zeroPosting.MergeOwned(empty)
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
		ids.Optimize()
		if ids.IsEmpty() {
			continue
		}
		result = append(result, index{
			Key: indexKeyFromU64(uint64(ln)),
			IDs: ids,
		})
	}
	if useZeroComplement {
		nonEmptyPosting.Optimize()
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

func addFieldPostingList(fieldMap map[string]posting.List, key string, idx uint64) map[string]posting.List {
	if fieldMap == nil {
		fieldMap = getPostingMap()
	}
	p := fieldMap[key]
	p.Add(idx)
	fieldMap[key] = p
	return fieldMap
}

func addFixedFieldPostingList(fieldMap map[uint64]posting.List, key uint64, idx uint64) map[uint64]posting.List {
	if fieldMap == nil {
		fieldMap = getFixedPostingMap()
	}
	p := fieldMap[key]
	p.Add(idx)
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
		universe.Add(op.idx)
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
		changed := make(map[string]struct{}, changedCount)
		for i := range fieldStates {
			if fieldStates[i].changed {
				changed[db.indexedFieldAccess[i].name] = struct{}{}
			}
		}
		inheritMaterializedPredCache(snap, prev, changed)
	} else {
		inheritMaterializedPredCache(snap, prev, nil)
	}
	return snap, true
}

func mergeInsertOnlyFieldSliceOwned(base *[]index, adds map[string]posting.List, fixed8 bool) *[]index {
	if len(adds) == 0 {
		releasePostingMap(adds)
		return base
	}

	addSlice := make([]index, 0, len(adds))
	for key, ids := range adds {
		ids.Optimize()
		if ids.IsEmpty() {
			continue
		}
		addSlice = append(addSlice, index{
			Key: indexKeyFromStoredString(key, fixed8),
			IDs: ids,
		})
	}
	releasePostingMap(adds)
	if len(addSlice) == 0 {
		return base
	}

	return mergeInsertOnlyFieldEntries(base, addSlice)
}

func mergeInsertOnlyFixedFieldSliceOwned(base *[]index, adds map[uint64]posting.List) *[]index {
	if len(adds) == 0 {
		releaseFixedPostingMap(adds)
		return base
	}

	addSlice := make([]index, 0, len(adds))
	for key, ids := range adds {
		ids.Optimize()
		if ids.IsEmpty() {
			continue
		}
		addSlice = append(addSlice, index{
			Key: indexKeyFromU64(key),
			IDs: ids,
		})
	}
	releaseFixedPostingMap(adds)
	if len(addSlice) == 0 {
		return base
	}

	return mergeInsertOnlyFieldEntries(base, addSlice)
}

func mergeInsertOnlyFieldEntries(base *[]index, addSlice []index) *[]index {
	slices.SortFunc(addSlice, func(a, b index) int {
		return compareIndexKeys(a.Key, b.Key)
	})

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

		index:             maps.Clone(prev.index),
		nilIndex:          maps.Clone(prev.nilIndex),
		lenIndex:          maps.Clone(prev.lenIndex),
		lenZeroComplement: maps.Clone(prev.lenZeroComplement),
		universe:          prev.universe.Clone(),
		strmap:            db.strmap.snapshot(),
	}
	db.initSnapshotRuntimeCaches(next)

	fieldStates := make([]snapshotFieldInsertState, len(db.indexedFieldAccess))

	for i := range prepared {
		op := prepared[i]
		next.universe.Add(op.idx)
		ptr := unsafe.Pointer(op.newVal)

		for _, acc := range db.indexedFieldAccess {
			acc.collectSnapshotInsertValue(ptr, op.idx, prev.lenZeroComplement[acc.name], &fieldStates[acc.ordinal])
		}
	}

	changedCount := 0
	for i, acc := range db.indexedFieldAccess {
		f := acc.name
		if storage := acc.mergeSnapshotInsertStorageOwned(next.index[f], &fieldStates[i], true); storage.keyCount() > 0 {
			next.index[f] = storage
		} else {
			delete(next.index, f)
		}
		if storage := acc.mergeSnapshotInsertNilStorageOwned(next.nilIndex[f], &fieldStates[i]); storage.keyCount() > 0 {
			next.nilIndex[f] = storage
		} else {
			delete(next.nilIndex, f)
		}
		if fieldStates[i].lengths != nil {
			next.lenIndex[f] = applyLenFieldPostingDiffStorageOwned(next.lenIndex[f], fieldStates[i].lengths)
			fieldStates[i].lengths = nil
		}
		if fieldStates[i].changed {
			changedCount++
		}
	}
	inheritNumericRangeBucketCache(next, prev)

	if changedCount > 0 {
		changed := make(map[string]struct{}, changedCount)
		for i := range fieldStates {
			if fieldStates[i].changed {
				changed[db.indexedFieldAccess[i].name] = struct{}{}
			}
		}
		inheritMaterializedPredCache(next, prev, changed)
	} else {
		inheritMaterializedPredCache(next, prev, nil)
	}

	return next, true
}
