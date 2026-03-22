package rbi

import (
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/roaring64"
)

// indexSnapshot is an immutable read-view published atomically for query paths.
type indexSnapshot struct {
	seq uint64

	index        map[string]*[]index
	nilIndex     map[string]*[]index
	lenIndex     map[string]*[]index
	indexView    map[string]*[]index
	nilIndexView map[string]*[]index
	lenIndexView map[string]*[]index
	indexDelta   map[string]*fieldIndexDelta
	nilDelta     map[string]*fieldIndexDelta
	lenIdxDelta  map[string]*fieldIndexDelta
	indexLayer   *fieldDeltaLayer
	nilLayer     *fieldDeltaLayer
	lenLayer     *fieldDeltaLayer
	indexDCount  int
	nilDCount    int
	lenDCount    int
	universe     *roaring64.Bitmap
	universeAdd  *roaring64.Bitmap
	universeRem  *roaring64.Bitmap
	strmap       *strMapSnapshot

	indexDeltaCache sync.Map
	nilDeltaCache   sync.Map
	lenDeltaCache   sync.Map
	// numericRangeBucketCache stores lazy-built per-field base bucket unions for
	// numeric range acceleration; field deltas are applied on top at query time.
	numericRangeBucketCache *sync.Map

	matPredCache                    sync.Map
	matPredCacheCount               atomic.Int32
	matPredCacheMaxEntries          int
	matPredCacheMaxEntriesWithDelta int
	matPredCacheMaxBitmapCard       uint64
	matPredCacheOversizedCount      atomic.Int32
}

type materializedPredCacheEntry struct {
	bm *roaring64.Bitmap
}

const matPredCacheOversizedMaxEntries = 2

type fieldDeltaLayer struct {
	parent *fieldDeltaLayer
	fields map[string]*fieldIndexDelta
	depth  int

	fieldUpperStatsMu sync.RWMutex
	fieldUpperStats   map[string]layerUpperStats
}

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

func materializedPredCacheMaxBitmapCardinality(v int) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

func newNumericRangeBucketCache() *sync.Map {
	return &sync.Map{}
}

func inheritNumericRangeBucketCache(prev *indexSnapshot, reuse bool) *sync.Map {
	if reuse && prev != nil && prev.numericRangeBucketCache != nil {
		return prev.numericRangeBucketCache
	}
	return newNumericRangeBucketCache()
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

func parseMaterializedPredScalarRangeCacheKey(key string) (field string, op qx.Op, scalar string, ok bool) {
	field, rest, ok := strings.Cut(key, "\x1f")
	if !ok || field == "" {
		return "", 0, "", false
	}
	opRaw, scalar, ok := strings.Cut(rest, "\x1f")
	if !ok || scalar == "" || strings.Contains(scalar, "\x1f") {
		return "", 0, "", false
	}
	opInt, err := strconv.Atoi(opRaw)
	if err != nil {
		return "", 0, "", false
	}
	op = qx.Op(opInt)
	switch op {
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE:
		return field, op, scalar, true
	default:
		return "", 0, "", false
	}
}

func rangeBoundsMatchKey(rb rangeBounds, key string) bool {
	if rb.hasPrefix && !strings.HasPrefix(key, rb.prefix) {
		return false
	}
	if rb.hasLo {
		cmp := strings.Compare(key, rb.loKey)
		if cmp < 0 || (cmp == 0 && !rb.loInc) {
			return false
		}
	}
	if rb.hasHi {
		cmp := strings.Compare(key, rb.hiKey)
		if cmp > 0 || (cmp == 0 && !rb.hiInc) {
			return false
		}
	}
	return true
}

func updateMaterializedPredCacheEntryWithDelta(prev *materializedPredCacheEntry, delta *fieldIndexDelta, op qx.Op, scalar string) (*materializedPredCacheEntry, bool) {
	if prev == nil || prev.bm == nil || delta == nil || !delta.hasEntries() {
		return nil, false
	}
	rb, ok := rangeBoundsForOp(op, scalar)
	if !ok {
		return nil, false
	}

	bm := getRoaringBuf()
	bm.Or(prev.bm)

	delta.forEach(func(key string, e indexDeltaEntry) {
		if !rangeBoundsMatchKey(rb, key) {
			return
		}
		deltaEntryApplyDelToBitmap(bm, e)
	})
	delta.forEach(func(key string, e indexDeltaEntry) {
		if !rangeBoundsMatchKey(rb, key) {
			return
		}
		deltaEntryApplyAddToBitmap(bm, e)
	})

	return &materializedPredCacheEntry{bm: bm}, true
}

func inheritMaterializedPredCache(next, prev *indexSnapshot, changedFields map[string]*fieldIndexDelta) {
	if next == nil || prev == nil {
		return
	}
	limit := next.materializedPredCacheLimit()
	if limit <= 0 || prev.matPredCacheCount.Load() == 0 {
		return
	}

	var oversized int32
	prev.matPredCache.Range(func(k, v any) bool {
		if int(next.matPredCacheCount.Load()) >= limit {
			return false
		}
		key, ok := k.(string)
		if !ok || key == "" {
			return true
		}
		field := materializedPredCacheFieldName(key)
		if field == "" {
			return true
		}
		entry, ok := v.(*materializedPredCacheEntry)
		if !ok {
			return true
		}
		if changedFields != nil {
			if delta, touched := changedFields[field]; touched {
				rangeField, op, scalar, ok := parseMaterializedPredScalarRangeCacheKey(key)
				if !ok || rangeField != field {
					return true
				}
				updated, ok := updateMaterializedPredCacheEntryWithDelta(entry, delta, op, scalar)
				if !ok || updated == nil || updated.bm == nil {
					return true
				}
				if next.matPredCacheMaxBitmapCard > 0 &&
					updated.bm.GetCardinality() > next.matPredCacheMaxBitmapCard {
					releaseRoaringBuf(updated.bm)
					return true
				}
				if _, loaded := next.matPredCache.LoadOrStore(key, updated); loaded {
					releaseRoaringBuf(updated.bm)
					return true
				}
				next.matPredCacheCount.Add(1)
				if next.matPredCacheMaxBitmapCard > 0 &&
					updated.bm.GetCardinality() > next.matPredCacheMaxBitmapCard {
					oversized++
				}
				return true
			}
		}
		if _, loaded := next.matPredCache.LoadOrStore(key, entry); loaded {
			return true
		}
		next.matPredCacheCount.Add(1)
		if entry != nil && entry.bm != nil && next.matPredCacheMaxBitmapCard > 0 &&
			entry.bm.GetCardinality() > next.matPredCacheMaxBitmapCard {
			oversized++
		}
		return true
	})
	if oversized > 0 {
		next.matPredCacheOversizedCount.Store(min(oversized, int32(matPredCacheOversizedMaxEntries)))
	}
}

func (db *DB[K, V]) publishSnapshotNoLock(seq uint64) {
	s, indexDelta, nilDelta, lenDelta := db.buildSnapshotWithTxDeltaNoLock(seq, nil, nil, nil, nil, nil)
	db.finishSnapshotPublishNoLock(s, indexDelta, nilDelta, lenDelta, nil, nil)
}

func (db *DB[K, V]) finishSnapshotPublishNoLock(s *indexSnapshot, indexDelta, nilDelta, lenDelta map[string]*fieldIndexDelta, add, rem *roaring64.Bitmap) {
	if s == nil {
		return
	}
	db.snapshot.current.Store(s)
	db.registerSnapshot(s)
	if s.hasAnyDeltaState() {
		db.maybeRequestSnapshotCompaction(s, indexDelta, nilDelta, lenDelta, add, rem)
	}
	db.noteSnapshotActivity()
}

func (db *DB[K, V]) buildSnapshotWithTxDeltaNoLock(seq uint64, indexChanges, nilChanges, lenChanges map[string]map[string]indexDeltaEntry, add, rem *roaring64.Bitmap) (*indexSnapshot, map[string]*fieldIndexDelta, map[string]*fieldIndexDelta, map[string]*fieldIndexDelta) {
	indexDelta := buildSnapshotDeltaMap(indexChanges, func(field string) bool {
		return fieldUsesFixed8Keys(db.fields[field])
	})
	nilDelta := buildSnapshotDeltaMap(nilChanges, func(string) bool { return false })
	lenDelta := buildSnapshotDeltaMap(lenChanges, func(string) bool { return true })
	prev := db.snapshot.current.Load()
	indexLayer := layerFromFlatDeltaMap(indexDelta)
	nilLayer := layerFromFlatDeltaMap(nilDelta)
	lenLayer := layerFromFlatDeltaMap(lenDelta)
	var snapStrMap *strMapSnapshot
	if db.strkey {
		snapStrMap = db.strmap.snapshot()
	}

	s := &indexSnapshot{
		seq:         seq,
		index:       db.index,
		nilIndex:    db.nilIndex,
		lenIndex:    db.lenIndex,
		indexDelta:  indexDelta,
		nilDelta:    nilDelta,
		lenIdxDelta: lenDelta,
		indexLayer:  indexLayer,
		nilLayer:    nilLayer,
		lenLayer:    lenLayer,
		indexDCount: indexLayer.effectiveFieldCount(),
		nilDCount:   nilLayer.effectiveFieldCount(),
		lenDCount:   lenLayer.effectiveFieldCount(),
		universe:    db.universe,
		universeAdd: cloneIfNotEmpty(add),
		universeRem: cloneIfNotEmpty(rem),
		strmap:      snapStrMap,

		numericRangeBucketCache: inheritNumericRangeBucketCache(prev, false),

		matPredCacheMaxEntries:          max(0, db.options.SnapshotMaterializedPredCacheMaxEntries),
		matPredCacheMaxEntriesWithDelta: max(0, db.options.SnapshotMaterializedPredCacheMaxEntriesWithDelta),
		matPredCacheMaxBitmapCard:       materializedPredCacheMaxBitmapCardinality(db.options.SnapshotMaterializedPredCacheMaxBitmapCardinality),
	}
	inheritMaterializedPredCache(s, prev, indexDelta)
	return s, indexDelta, nilDelta, lenDelta
}

func (db *DB[K, V]) publishSnapshotWithTxDeltaNoLock(seq uint64, indexChanges, nilChanges, lenChanges map[string]map[string]indexDeltaEntry, add, rem *roaring64.Bitmap) {
	s, indexDelta, nilDelta, lenDelta := db.buildSnapshotWithTxDeltaNoLock(seq, indexChanges, nilChanges, lenChanges, add, rem)
	db.finishSnapshotPublishNoLock(s, indexDelta, nilDelta, lenDelta, add, rem)
}

func (db *DB[K, V]) buildPreparedSnapshotWithAccumDeltaNoLock(seq uint64, delta *preparedSnapshotDelta) *indexSnapshot {
	if delta == nil {
		return db.buildSnapshotWithAccumDeltaNoLock(seq, nil, nil, nil, nil, nil)
	}

	prev := db.getSnapshot()

	baseIndex := db.index
	baseNilIndex := db.nilIndex
	baseLenIndex := db.lenIndex
	baseUniverse := db.universe
	var baseStrMap *strMapSnapshot
	if db.strkey {
		baseStrMap = db.strmap.snapshot()
	}
	if prev != nil {
		baseIndex = prev.index
		baseNilIndex = prev.nilIndex
		baseLenIndex = prev.lenIndex
		baseUniverse = prev.universe
		if !db.strkey {
			baseStrMap = prev.strmap
		}
	}
	prevIndexLayer := prev.getIndexLayer()
	prevNilLayer := prev.getNilLayer()
	prevLenLayer := prev.getLenLayer()

	nextIndexLayer := appendDeltaLayer(prevIndexLayer, delta.indexDelta)
	nextNilLayer := appendDeltaLayer(prevNilLayer, delta.nilDelta)
	nextLenLayer := appendDeltaLayer(prevLenLayer, delta.lenDelta)
	nextIndexCount := appendDeltaLayerFieldCount(prev.indexDeltaCount(), delta.indexDelta, len(baseIndex))
	nextNilCount := appendDeltaLayerFieldCount(prev.nilDeltaCount(), delta.nilDelta, len(baseNilIndex))
	nextLenCount := appendDeltaLayerFieldCount(prev.lenDeltaCount(), delta.lenDelta, len(baseLenIndex))
	nextIndexLayer, nextIndexCount = capPublishedDeltaLayerDepth(nextIndexLayer, nextIndexCount, db.options.SnapshotDeltaLayerMaxDepth)
	nextNilLayer, nextNilCount = capPublishedDeltaLayerDepth(nextNilLayer, nextNilCount, db.options.SnapshotDeltaLayerMaxDepth)
	nextLenLayer, nextLenCount = capPublishedDeltaLayerDepth(nextLenLayer, nextLenCount, db.options.SnapshotDeltaLayerMaxDepth)

	nextUniverseAdd, nextUniverseRem := mergeUniverseDelta(nil, nil, delta.universeAdd, delta.universeRem)
	if prev != nil {
		nextUniverseAdd, nextUniverseRem = mergeUniverseDelta(prev.universeAdd, prev.universeRem, delta.universeAdd, delta.universeRem)
	}

	nextIndexDelta := maybeExposeFlatLayerMap(nextIndexLayer)
	nextNilDelta := maybeExposeFlatLayerMap(nextNilLayer)
	nextLenDelta := maybeExposeFlatLayerMap(nextLenLayer)

	s := &indexSnapshot{
		seq:         seq,
		index:       baseIndex,
		nilIndex:    baseNilIndex,
		lenIndex:    baseLenIndex,
		indexDelta:  nextIndexDelta,
		nilDelta:    nextNilDelta,
		lenIdxDelta: nextLenDelta,
		indexLayer:  nextIndexLayer,
		nilLayer:    nextNilLayer,
		lenLayer:    nextLenLayer,
		indexDCount: nextIndexCount,
		nilDCount:   nextNilCount,
		lenDCount:   nextLenCount,
		universe:    baseUniverse,
		universeAdd: nextUniverseAdd,
		universeRem: nextUniverseRem,
		strmap:      baseStrMap,

		numericRangeBucketCache: inheritNumericRangeBucketCache(prev, prev != nil),

		matPredCacheMaxEntries:          max(0, db.options.SnapshotMaterializedPredCacheMaxEntries),
		matPredCacheMaxEntriesWithDelta: max(0, db.options.SnapshotMaterializedPredCacheMaxEntriesWithDelta),
		matPredCacheMaxBitmapCard:       materializedPredCacheMaxBitmapCardinality(db.options.SnapshotMaterializedPredCacheMaxBitmapCardinality),
	}
	inheritMaterializedPredCache(s, prev, delta.indexDelta)
	return s
}

func (db *DB[K, V]) publishPreparedSnapshotWithAccumDeltaNoLock(seq uint64, delta *preparedSnapshotDelta) {
	s := db.buildPreparedSnapshotWithAccumDeltaNoLock(seq, delta)
	if delta == nil {
		db.finishSnapshotPublishNoLock(s, nil, nil, nil, nil, nil)
		return
	}
	db.finishSnapshotPublishNoLock(s, delta.indexDelta, delta.nilDelta, delta.lenDelta, delta.universeAdd, delta.universeRem)
	delta.releaseTransientUniverse()
}

func (db *DB[K, V]) buildSnapshotWithAccumDeltaNoLock(seq uint64, indexChanges, nilChanges, lenChanges map[string]map[string]indexDeltaEntry, add, rem *roaring64.Bitmap) *indexSnapshot {
	indexDelta := buildSnapshotDeltaMap(indexChanges, func(field string) bool {
		return fieldUsesFixed8Keys(db.fields[field])
	})
	nilDelta := buildSnapshotDeltaMap(nilChanges, func(string) bool { return false })
	lenDelta := buildSnapshotDeltaMap(lenChanges, func(string) bool { return true })
	prev := db.getSnapshot()

	baseIndex := db.index
	baseNilIndex := db.nilIndex
	baseLenIndex := db.lenIndex
	baseUniverse := db.universe
	var baseStrMap *strMapSnapshot
	if db.strkey {
		baseStrMap = db.strmap.snapshot()
	}
	if prev != nil {
		baseIndex = prev.index
		baseNilIndex = prev.nilIndex
		baseLenIndex = prev.lenIndex
		baseUniverse = prev.universe
		if !db.strkey {
			baseStrMap = prev.strmap
		}
	}
	prevIndexLayer := prev.getIndexLayer()
	prevNilLayer := prev.getNilLayer()
	prevLenLayer := prev.getLenLayer()

	nextIndexLayer := appendDeltaLayer(prevIndexLayer, indexDelta)
	nextNilLayer := appendDeltaLayer(prevNilLayer, nilDelta)
	nextLenLayer := appendDeltaLayer(prevLenLayer, lenDelta)
	nextIndexCount := appendDeltaLayerFieldCount(prev.indexDeltaCount(), indexDelta, len(baseIndex))
	nextNilCount := appendDeltaLayerFieldCount(prev.nilDeltaCount(), nilDelta, len(baseNilIndex))
	nextLenCount := appendDeltaLayerFieldCount(prev.lenDeltaCount(), lenDelta, len(baseLenIndex))
	nextIndexLayer, nextIndexCount = capPublishedDeltaLayerDepth(nextIndexLayer, nextIndexCount, db.options.SnapshotDeltaLayerMaxDepth)
	nextNilLayer, nextNilCount = capPublishedDeltaLayerDepth(nextNilLayer, nextNilCount, db.options.SnapshotDeltaLayerMaxDepth)
	nextLenLayer, nextLenCount = capPublishedDeltaLayerDepth(nextLenLayer, nextLenCount, db.options.SnapshotDeltaLayerMaxDepth)

	nextUniverseAdd, nextUniverseRem := mergeUniverseDelta(nil, nil, add, rem)
	if prev != nil {
		nextUniverseAdd, nextUniverseRem = mergeUniverseDelta(prev.universeAdd, prev.universeRem, add, rem)
	}

	nextIndexDelta := maybeExposeFlatLayerMap(nextIndexLayer)
	nextNilDelta := maybeExposeFlatLayerMap(nextNilLayer)
	nextLenDelta := maybeExposeFlatLayerMap(nextLenLayer)

	s := &indexSnapshot{
		seq:         seq,
		index:       baseIndex,
		nilIndex:    baseNilIndex,
		lenIndex:    baseLenIndex,
		indexDelta:  nextIndexDelta,
		nilDelta:    nextNilDelta,
		lenIdxDelta: nextLenDelta,
		indexLayer:  nextIndexLayer,
		nilLayer:    nextNilLayer,
		lenLayer:    nextLenLayer,
		indexDCount: nextIndexCount,
		nilDCount:   nextNilCount,
		lenDCount:   nextLenCount,
		universe:    baseUniverse,
		universeAdd: nextUniverseAdd,
		universeRem: nextUniverseRem,
		strmap:      baseStrMap,

		numericRangeBucketCache: inheritNumericRangeBucketCache(prev, prev != nil),

		matPredCacheMaxEntries:          max(0, db.options.SnapshotMaterializedPredCacheMaxEntries),
		matPredCacheMaxEntriesWithDelta: max(0, db.options.SnapshotMaterializedPredCacheMaxEntriesWithDelta),
		matPredCacheMaxBitmapCard:       materializedPredCacheMaxBitmapCardinality(db.options.SnapshotMaterializedPredCacheMaxBitmapCardinality),
	}
	inheritMaterializedPredCache(s, prev, indexDelta)
	return s
}

func (db *DB[K, V]) publishSnapshotWithAccumDeltaNoLock(seq uint64, indexChanges, nilChanges, lenChanges map[string]map[string]indexDeltaEntry, add, rem *roaring64.Bitmap) {
	indexDelta := buildSnapshotDeltaMap(indexChanges, func(field string) bool {
		return fieldUsesFixed8Keys(db.fields[field])
	})
	nilDelta := buildSnapshotDeltaMap(nilChanges, func(string) bool { return false })
	lenDelta := buildSnapshotDeltaMap(lenChanges, func(string) bool { return true })

	prev := db.getSnapshot()

	baseIndex := db.index
	baseNilIndex := db.nilIndex
	baseLenIndex := db.lenIndex
	baseUniverse := db.universe
	var baseStrMap *strMapSnapshot
	if db.strkey {
		baseStrMap = db.strmap.snapshot()
	}
	if prev != nil {
		baseIndex = prev.index
		baseNilIndex = prev.nilIndex
		baseLenIndex = prev.lenIndex
		baseUniverse = prev.universe
		if !db.strkey {
			baseStrMap = prev.strmap
		}
	}
	prevIndexLayer := prev.getIndexLayer()
	prevNilLayer := prev.getNilLayer()
	prevLenLayer := prev.getLenLayer()

	nextIndexLayer := appendDeltaLayer(prevIndexLayer, indexDelta)
	nextNilLayer := appendDeltaLayer(prevNilLayer, nilDelta)
	nextLenLayer := appendDeltaLayer(prevLenLayer, lenDelta)
	nextIndexCount := appendDeltaLayerFieldCount(prev.indexDeltaCount(), indexDelta, len(baseIndex))
	nextNilCount := appendDeltaLayerFieldCount(prev.nilDeltaCount(), nilDelta, len(baseNilIndex))
	nextLenCount := appendDeltaLayerFieldCount(prev.lenDeltaCount(), lenDelta, len(baseLenIndex))
	nextIndexLayer, nextIndexCount = capPublishedDeltaLayerDepth(nextIndexLayer, nextIndexCount, db.options.SnapshotDeltaLayerMaxDepth)
	nextNilLayer, nextNilCount = capPublishedDeltaLayerDepth(nextNilLayer, nextNilCount, db.options.SnapshotDeltaLayerMaxDepth)
	nextLenLayer, nextLenCount = capPublishedDeltaLayerDepth(nextLenLayer, nextLenCount, db.options.SnapshotDeltaLayerMaxDepth)

	nextUniverseAdd, nextUniverseRem := mergeUniverseDelta(nil, nil, add, rem)
	if prev != nil {
		nextUniverseAdd, nextUniverseRem = mergeUniverseDelta(prev.universeAdd, prev.universeRem, add, rem)
	}

	nextIndexDelta := maybeExposeFlatLayerMap(nextIndexLayer)
	nextNilDelta := maybeExposeFlatLayerMap(nextNilLayer)
	nextLenDelta := maybeExposeFlatLayerMap(nextLenLayer)

	s := &indexSnapshot{
		seq:         seq,
		index:       baseIndex,
		nilIndex:    baseNilIndex,
		lenIndex:    baseLenIndex,
		indexDelta:  nextIndexDelta,
		nilDelta:    nextNilDelta,
		lenIdxDelta: nextLenDelta,
		indexLayer:  nextIndexLayer,
		nilLayer:    nextNilLayer,
		lenLayer:    nextLenLayer,
		indexDCount: nextIndexCount,
		nilDCount:   nextNilCount,
		lenDCount:   nextLenCount,
		universe:    baseUniverse,
		universeAdd: nextUniverseAdd,
		universeRem: nextUniverseRem,
		strmap:      baseStrMap,

		numericRangeBucketCache: inheritNumericRangeBucketCache(prev, prev != nil),

		matPredCacheMaxEntries:          max(0, db.options.SnapshotMaterializedPredCacheMaxEntries),
		matPredCacheMaxEntriesWithDelta: max(0, db.options.SnapshotMaterializedPredCacheMaxEntriesWithDelta),
		matPredCacheMaxBitmapCard:       materializedPredCacheMaxBitmapCardinality(db.options.SnapshotMaterializedPredCacheMaxBitmapCardinality),
	}
	inheritMaterializedPredCache(s, prev, indexDelta)
	db.finishSnapshotPublishNoLock(s, indexDelta, nilDelta, lenDelta, add, rem)
}

func appendDeltaLayer(prev *fieldDeltaLayer, delta map[string]*fieldIndexDelta) *fieldDeltaLayer {
	if len(delta) == 0 {
		return prev
	}
	layer := &fieldDeltaLayer{
		parent: prev,
		fields: delta,
		depth:  1,
	}
	if prev != nil {
		layer.depth = prev.depth + 1
	}
	return layer
}

func appendDeltaLayerFieldCount(prevCount int, delta map[string]*fieldIndexDelta, capFields int) int {
	if len(delta) == 0 {
		return prevCount
	}
	count := prevCount + len(delta)
	if capFields > 0 && count > capFields {
		count = capFields
	}
	return count
}

func capPublishedDeltaLayerDepth(layer *fieldDeltaLayer, fieldCount int, maxDepth int) (*fieldDeltaLayer, int) {
	if layer == nil || maxDepth <= 0 || layer.depth <= maxDepth {
		return layer, fieldCount
	}
	return maybeFlattenDeltaLayerByDepth(layer, fieldCount, maxDepth)
}

func (db *DB[K, V]) startSnapshotCompactor() {
	if db.snapshot.compactReq != nil {
		return
	}
	db.snapshot.compactReq = make(chan struct{}, 1)
	db.snapshot.compactIdle = make(chan struct{}, 1)
	db.snapshot.compactStop = make(chan struct{})
	db.snapshot.compactDone = make(chan struct{})
	go db.snapshotCompactorLoop()
}

func (db *DB[K, V]) stopSnapshotCompactor() {
	if db.snapshot.compactStop == nil || db.snapshot.compactDone == nil {
		return
	}
	select {
	case <-db.snapshot.compactStop:
	default:
		close(db.snapshot.compactStop)
	}
	<-db.snapshot.compactDone
}

func (db *DB[K, V]) requestSnapshotCompaction() {
	if db.snapshot.compactReq == nil {
		return
	}
	if db.snapshot.statsEnabled {
		db.snapshot.compactRequested.Add(1)
	}
	select {
	case db.snapshot.compactReq <- struct{}{}:
	default:
	}
}

func (db *DB[K, V]) requestSnapshotCompactionForce() {
	if db.snapshot.compactReq == nil {
		return
	}
	db.snapshot.compactSkipUntil.Store(0)
	db.snapshot.compactForcePending.Store(true)
	db.requestSnapshotCompaction()
}

func (db *DB[K, V]) noteSnapshotActivity() {
	if db.snapshot.compactIdle == nil || db.options.SnapshotCompactorIdleInterval <= 0 {
		return
	}
	db.snapshot.compactLastActivity.Store(time.Now().UnixNano())
	select {
	case db.snapshot.compactIdle <- struct{}{}:
	default:
	}
}

func (db *DB[K, V]) maybeRequestSnapshotCompaction(s *indexSnapshot, indexDelta, nilDelta, lenDelta map[string]*fieldIndexDelta, add, rem *roaring64.Bitmap) {
	stats := db.snapshot.statsEnabled
	if db.snapshot.compactReq == nil || !s.hasAnyDeltaState() {
		return
	}
	if snapshotCompactionUrgent(s, db.options) || txDeltaNeedsCompaction(indexDelta, nilDelta, lenDelta, add, rem, db.options) {
		db.snapshot.compactSkipUntil.Store(0)
		db.requestSnapshotCompaction()
		return
	}

	seq := db.snapshot.compactWriteSeq.Add(1)
	if until := db.snapshot.compactSkipUntil.Load(); until > 0 && seq < until {
		if stats {
			db.snapshot.compactSkippedWake.Add(1)
		}
		return
	}
	if len(db.snapshot.compactReq) > 0 {
		if stats {
			db.snapshot.compactSkippedWake.Add(1)
		}
		return
	}

	every := db.compactorRequestEveryForSnapshot(s)
	if every == 0 {
		return
	}
	if every == 1 || seq%every == 0 {
		db.requestSnapshotCompaction()
	}
}

func (db *DB[K, V]) compactorRequestEveryForSnapshot(s *indexSnapshot) uint64 {
	everyOpt := db.options.SnapshotCompactorRequestEveryNWrites
	if everyOpt < 0 {
		return 0
	}
	every := uint64(everyOpt)
	if every <= 1 || s == nil {
		return every
	}

	maxDepth := 0
	if l := s.getIndexLayer(); l != nil {
		maxDepth = l.depth
	}
	if l := s.getNilLayer(); l != nil && l.depth > maxDepth {
		maxDepth = l.depth
	}
	if l := s.getLenLayer(); l != nil && l.depth > maxDepth {
		maxDepth = l.depth
	}

	if limit := db.options.SnapshotDeltaLayerMaxDepth; limit > 0 {
		// Under shallow overlays we can request compaction less often to avoid
		// compactor/write contention; urgent depth checks still preempt this path.
		soft := limit / 2
		if soft > 0 && maxDepth <= soft {
			every *= 2
		}
	}
	return every
}

func snapshotCompactionUrgent(s *indexSnapshot, opt *Options) bool {
	if s == nil {
		return false
	}
	if opt.SnapshotDeltaLayerMaxDepth > 0 {
		urgentDepth := opt.SnapshotDeltaLayerMaxDepth
		if opt.SnapshotDeltaLayerMaxDepth >= 8 && defaultSnapshotCompactorUrgentDepthSlack > 0 {
			urgentDepth += defaultSnapshotCompactorUrgentDepthSlack
		}
		if l := s.getIndexLayer(); l != nil && l.depth > urgentDepth {
			return true
		}
		if l := s.getNilLayer(); l != nil && l.depth > urgentDepth {
			return true
		}
		if l := s.getLenLayer(); l != nil && l.depth > urgentDepth {
			return true
		}
	}
	if opt.SnapshotDeltaCompactUniverseOps > 0 &&
		snapshotUniverseDeltaOps(s.universeAdd, s.universeRem) >= uint64(opt.SnapshotDeltaCompactUniverseOps) {
		return true
	}
	return false
}

func txDeltaNeedsCompaction(indexDelta, nilDelta, lenDelta map[string]*fieldIndexDelta, add, rem *roaring64.Bitmap, opt *Options) bool {
	opsThr := uint64(0)
	if opt.SnapshotDeltaCompactFieldOps > 0 {
		opsThr = uint64(opt.SnapshotDeltaCompactFieldOps)
	}
	if opt.SnapshotDeltaCompactUniverseOps > 0 &&
		snapshotUniverseDeltaOps(add, rem) >= uint64(opt.SnapshotDeltaCompactUniverseOps) {
		return true
	}
	return fieldDeltaPatchNeedsCompaction(indexDelta, opt.SnapshotDeltaCompactFieldKeys, opsThr) ||
		fieldDeltaPatchNeedsCompaction(nilDelta, opt.SnapshotDeltaCompactFieldKeys, opsThr) ||
		fieldDeltaPatchNeedsCompaction(lenDelta, opt.SnapshotDeltaCompactFieldKeys, opsThr)
}

func fieldDeltaPatchNeedsCompaction(delta map[string]*fieldIndexDelta, keysThr int, opsThr uint64) bool {
	if len(delta) == 0 {
		return false
	}
	if keysThr <= 0 && opsThr == 0 {
		return false
	}
	for _, d := range delta {
		if d == nil {
			continue
		}
		if keysThr > 0 && d.keyCount() >= keysThr {
			return true
		}
		if opsThr > 0 && d.ops >= opsThr {
			return true
		}
	}
	return false
}

func snapshotUniverseDeltaOps(add, drop *roaring64.Bitmap) uint64 {
	var ops uint64
	if add != nil {
		ops += add.GetCardinality()
	}
	if drop != nil {
		ops += drop.GetCardinality()
	}
	return ops
}

func (db *DB[K, V]) snapshotCompactorLoop() {
	stats := db.snapshot.statsEnabled

	defer close(db.snapshot.compactDone)

	idleInterval := db.options.SnapshotCompactorIdleInterval
	var (
		idleTimer *time.Timer
		idleCh    <-chan time.Time
	)

	resetIdleTimer := func() {
		if idleInterval <= 0 {
			return
		}
		if idleTimer == nil {
			idleTimer = time.NewTimer(idleInterval)
			idleCh = idleTimer.C
			return
		}
		if !idleTimer.Stop() {
			select {
			case <-idleTimer.C:
			default:
			}
		}
		idleTimer.Reset(idleInterval)
		idleCh = idleTimer.C
	}

	stopIdleTimer := func() {
		if idleTimer == nil {
			return
		}
		if !idleTimer.Stop() {
			select {
			case <-idleTimer.C:
			default:
			}
		}
		idleCh = nil
	}

	for {
		if db.broken.Load() {
			stopIdleTimer()
			return
		}
		select {

		case <-db.snapshot.compactStop:
			stopIdleTimer()
			return

		case <-db.snapshot.compactIdle:
			resetIdleTimer()
			continue

		case <-idleCh:
			idleCh = nil
			db.requestSnapshotCompactionForce()
			continue

		case <-db.snapshot.compactReq:
		}
		if db.broken.Load() {
			stopIdleTimer()
			return
		}

		force := db.snapshot.compactForcePending.Swap(false)
		deferForceUntilIdle := func() {
			if force && !db.snapshotCompactorIdleReady(idleInterval) {
				if stats {
					db.snapshot.compactIdleDefers.Add(1)
				}
				force = false
				resetIdleTimer()
			}
		}
		deferForceUntilIdle()
		if stats {
			db.snapshot.compactRuns.Add(1)
		}

	DRAIN:
		for {
			select {
			case <-db.snapshot.compactReq:
				if db.snapshot.compactForcePending.Swap(false) {
					force = true
				}
			default:
				break DRAIN
			}
		}

		deferForceUntilIdle()

		if db.runSnapshotCompaction(force) {
			resetIdleTimer()
		}
	}
}

func (db *DB[K, V]) snapshotCompactorLockMissSkip(streak uint64) uint64 {
	skip := uint64(max(0, db.options.SnapshotCompactorRequestEveryNWrites))
	if skip < 4 {
		skip = 4
	}
	if streak <= 1 {
		return skip
	}
	maxSkip := skip * 32
	for i := uint64(1); i < streak && skip < maxSkip; i++ {
		skip *= 2
		if skip < 4 {
			skip = 4
		}
		if skip >= maxSkip {
			return maxSkip
		}
	}
	if skip > maxSkip {
		return maxSkip
	}
	return skip
}

func (db *DB[K, V]) noteSnapshotCompactorBusySkip(preclaim bool) {
	stats := db.snapshot.statsEnabled
	if stats {
		if preclaim {
			db.snapshot.compactPreclaimBusy.Add(1)
		} else {
			db.snapshot.compactLockMiss.Add(1)
		}
	}
	streak := db.snapshot.compactMissStreak.Add(1)
	seq := db.snapshot.compactWriteSeq.Load()
	skip := db.snapshotCompactorLockMissSkip(streak)
	db.snapshot.compactSkipUntil.Store(seq + skip)
}

func snapshotCompactionWorthwhile(s *indexSnapshot, opt *Options) bool {
	if s == nil || !s.hasAnyDeltaState() {
		return false
	}
	if snapshotCompactionUrgent(s, opt) {
		return true
	}
	if l := s.getIndexLayer(); l != nil && l.depth > 1 {
		return true
	}
	if l := s.getNilLayer(); l != nil && l.depth > 1 {
		return true
	}
	if l := s.getLenLayer(); l != nil && l.depth > 1 {
		return true
	}
	if opt.SnapshotDeltaCompactUniverseOps > 0 {
		universeOps := snapshotUniverseDeltaOps(s.universeAdd, s.universeRem)
		if universeOps >= uint64(max(1, opt.SnapshotDeltaCompactUniverseOps/4)) {
			return true
		}
	}
	fieldLimit := max(1, opt.SnapshotDeltaCompactMaxFieldsPerPublish)
	if s.indexDeltaCount() > fieldLimit || s.nilDeltaCount() > fieldLimit || s.lenDeltaCount() > fieldLimit {
		return true
	}
	return false
}

func (db *DB[K, V]) runSnapshotCompaction(force bool) bool {
	if db.broken.Load() {
		return false
	}
	if !force && !snapshotCompactionWorthwhile(db.snapshot.current.Load(), db.options) {
		if db.snapshot.statsEnabled {
			db.snapshot.compactSoftSkip.Add(1)
		}
		return false
	}
	maxIters := int(db.options.SnapshotCompactorMaxIterationsPerRun)
	if maxIters <= 0 {
		return false
	}
	worked := false
	lockMiss := false
	for i := 0; i < maxIters; i++ {
		if !db.beginOpWait() {
			return false
		}
		applied, missed := db.compactLatestSnapshotOnce(force)
		db.endOp()
		if missed {
			lockMiss = true
			break
		}
		if !applied {
			break
		}
		worked = true
	}
	if force {
		_, blocked := db.compactSnapshotRegistryIdleOnce()
		db.snapshot.compactPinsBlocked.Store(blocked)

		s := db.snapshot.current.Load()
		if s.hasAnyDeltaState() && db.forceCompactionHasWork(s) {
			return true
		}
		if lockMiss {
			return true
		}
		return false
	}
	db.snapshot.compactPinsBlocked.Store(false)
	if !worked {
		return false
	}
	// Keep draining compaction only while pressure stays above urgent thresholds.
	if snapshotCompactionUrgent(db.snapshot.current.Load(), db.options) {
		db.requestSnapshotCompaction()
	}
	return false
}

func (db *DB[K, V]) snapshotCompactorIdleReady(interval time.Duration) bool {
	if interval <= 0 {
		return false
	}
	last := db.snapshot.compactLastActivity.Load()
	if last <= 0 {
		return true
	}
	lastTs := time.Unix(0, last)
	return time.Since(lastTs) >= interval
}

func (db *DB[K, V]) forceCompactionHasWork(s *indexSnapshot) bool {
	if s == nil {
		return false
	}
	if (s.universeAdd != nil && !s.universeAdd.IsEmpty()) ||
		(s.universeRem != nil && !s.universeRem.IsEmpty()) {
		return true
	}
	if db.options.SnapshotDeltaCompactMaxFieldsPerPublish <= 0 {
		return false
	}
	return s.getIndexLayer() != nil || s.getLenLayer() != nil
}

// ForceCompact synchronously compacts the currently published snapshot into a
// clean base view without layered snapshot deltas.
//
// It serializes with writers while compacting. Concurrent writes that start
// after ForceCompact returns may publish new deltas again.
func (db *DB[K, V]) ForceCompact() error {
	if err := db.beginOp(); err != nil {
		return err
	}
	defer db.endOp()

	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.unavailableErr(); err != nil {
		return err
	}

	cur := db.snapshot.current.Load()
	if cur == nil {
		return nil
	}

	nextStrMap := cur.strmap
	if db.strkey {
		nextStrMap = db.strmap.forceSnapshotPublished(cur.strmap)
	}

	next, changed := forceCompactSnapshot(cur, nextStrMap)
	if changed {
		db.snapshot.current.Store(next)
		db.registerSnapshot(next)
		db.noteSnapshotActivity()
	}

	_, blocked := db.compactSnapshotRegistryIdleOnce()
	db.snapshot.compactPinsBlocked.Store(blocked)
	db.snapshot.compactForcePending.Store(false)
	db.snapshot.compactSkipUntil.Store(0)

	return nil
}

func (db *DB[K, V]) compactSnapshotRegistryIdleOnce() (changed bool, blocked bool) {
	db.snapshot.mu.Lock()
	defer db.snapshot.mu.Unlock()

	if len(db.snapshot.bySeq) == 0 {
		if len(db.snapshot.order) > 0 || db.snapshot.head != 0 {
			db.snapshot.order = db.snapshot.order[:0]
			db.snapshot.head = 0
			return true, false
		}
		return false, false
	}

	latestSeq := uint64(0)
	if latest := db.snapshot.current.Load(); latest != nil {
		latestSeq = latest.seq
	}

	for seq, ref := range db.snapshot.bySeq {
		if seq == latestSeq {
			continue
		}
		if ref.refs.Load() > 0 {
			blocked = true
			continue
		}
		delete(db.snapshot.bySeq, seq)
		releaseSnapshotRef(ref)
		changed = true
	}

	ids := make([]uint64, 0, len(db.snapshot.bySeq))
	for seq := range db.snapshot.bySeq {
		ids = append(ids, seq)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	if len(db.snapshot.order) != len(ids) || db.snapshot.head != 0 {
		changed = true
	} else {
		for i, seq := range ids {
			if db.snapshot.order[i] != seq {
				changed = true
				break
			}
		}
	}
	db.snapshot.order = ids
	db.snapshot.head = 0

	return changed, blocked
}

func (db *DB[K, V]) compactLatestSnapshotOnce(force bool) (bool, bool) {
	stats := db.snapshot.statsEnabled
	if stats {
		db.snapshot.compactAttempts.Add(1)
	}
	const maxStaleRetries = 2
	for attempt := 0; attempt < maxStaleRetries; attempt++ {
		cur := db.snapshot.current.Load()
		if cur == nil {
			if stats {
				db.snapshot.compactNoChange.Add(1)
			}
			db.snapshot.compactMissStreak.Store(0)
			return false, false
		}

		if !db.mu.TryRLock() {
			db.noteSnapshotCompactorBusySkip(true)
			return false, true
		}
		latest := db.snapshot.current.Load()
		db.mu.RUnlock()
		if latest != cur {
			if stats {
				db.snapshot.compactStaleRetry.Add(1)
			}
			continue
		}

		next, ok := compactSnapshot(cur, db.options, force)
		if !ok || next == nil {
			if stats {
				db.snapshot.compactNoChange.Add(1)
			}
			db.snapshot.compactMissStreak.Store(0)
			return false, false
		}

		if !db.mu.TryLock() {
			db.noteSnapshotCompactorBusySkip(false)
			return false, true
		}

		latest = db.snapshot.current.Load()
		if latest != cur {
			db.mu.Unlock()
			if stats {
				db.snapshot.compactStaleRetry.Add(1)
			}
			continue
		}

		db.snapshot.current.Store(next)
		db.mu.Unlock()
		db.registerSnapshot(next)
		if stats {
			db.snapshot.compactSucceeded.Add(1)
		}
		db.snapshot.compactMissStreak.Store(0)
		db.snapshot.compactSkipUntil.Store(0)
		return true, false
	}
	return false, false
}

func compactSnapshot(cur *indexSnapshot, opt *Options, force bool) (*indexSnapshot, bool) {
	if cur == nil {
		return nil, false
	}

	baseIndex := cur.index
	baseNilIndex := cur.nilIndex
	baseLenIndex := cur.lenIndex
	indexLayer := cur.getIndexLayer()
	nilLayer := cur.getNilLayer()
	lenLayer := cur.getLenLayer()
	indexCount := cur.indexDCount
	nilCount := cur.nilDCount
	lenCount := cur.lenDCount
	changed := false

	var compacted int
	indexBaseUnchanged := true
	baseIndex, indexLayer, compacted = compactDeltaLayerIntoBase(baseIndex, indexLayer, opt, force)
	if compacted > 0 {
		indexBaseUnchanged = false
		changed = true
		indexCount -= compacted
		if indexCount < 0 {
			indexCount = 0
		}
	}
	nilBaseUnchanged := true
	baseNilIndex, nilLayer, compacted = compactDeltaLayerIntoBase(baseNilIndex, nilLayer, opt, force)
	if compacted > 0 {
		nilBaseUnchanged = false
		changed = true
		nilCount -= compacted
		if nilCount < 0 {
			nilCount = 0
		}
	}
	baseLenIndex, lenLayer, compacted = compactDeltaLayerIntoBase(baseLenIndex, lenLayer, opt, force)
	if compacted > 0 {
		changed = true
		lenCount -= compacted
		if lenCount < 0 {
			lenCount = 0
		}
	}
	if indexLayer == nil {
		indexCount = 0
	} else if indexCount == 0 {
		indexCount = indexLayer.effectiveFieldCount()
	}
	if nilLayer == nil {
		nilCount = 0
	} else if nilCount == 0 {
		nilCount = nilLayer.effectiveFieldCount()
	}
	if lenLayer == nil {
		lenCount = 0
	} else if lenCount == 0 {
		lenCount = lenLayer.effectiveFieldCount()
	}

	nextIndexLayer, nextIndexCount := maybeFlattenDeltaLayerByDepth(indexLayer, indexCount, opt.SnapshotDeltaLayerMaxDepth)
	if nextIndexLayer != indexLayer || nextIndexCount != indexCount {
		changed = true
	}
	nextNilLayer, nextNilCount := maybeFlattenDeltaLayerByDepth(nilLayer, nilCount, opt.SnapshotDeltaLayerMaxDepth)
	if nextNilLayer != nilLayer || nextNilCount != nilCount {
		changed = true
	}
	nextLenLayer, nextLenCount := maybeFlattenDeltaLayerByDepth(lenLayer, lenCount, opt.SnapshotDeltaLayerMaxDepth)
	if nextLenLayer != lenLayer || nextLenCount != lenCount {
		changed = true
	}

	nextUniverse, nextUniverseAdd, nextUniverseRem := maybeCompactUniverseDelta(
		cur.universe,
		cur.universeAdd,
		cur.universeRem,
		uint64(max(0, opt.SnapshotDeltaCompactUniverseOps)),
		force,
	)
	if nextUniverse != cur.universe || nextUniverseAdd != cur.universeAdd || nextUniverseRem != cur.universeRem {
		changed = true
	}

	if !changed {
		return nil, false
	}

	return &indexSnapshot{
		seq:                             cur.seq,
		index:                           baseIndex,
		nilIndex:                        baseNilIndex,
		lenIndex:                        baseLenIndex,
		indexDelta:                      maybeExposeFlatLayerMap(nextIndexLayer),
		nilDelta:                        maybeExposeFlatLayerMap(nextNilLayer),
		lenIdxDelta:                     maybeExposeFlatLayerMap(nextLenLayer),
		indexLayer:                      nextIndexLayer,
		nilLayer:                        nextNilLayer,
		lenLayer:                        nextLenLayer,
		indexDCount:                     nextIndexCount,
		nilDCount:                       nextNilCount,
		lenDCount:                       nextLenCount,
		universe:                        nextUniverse,
		universeAdd:                     nextUniverseAdd,
		universeRem:                     nextUniverseRem,
		strmap:                          cur.strmap,
		numericRangeBucketCache:         inheritNumericRangeBucketCache(cur, indexBaseUnchanged && nilBaseUnchanged),
		matPredCacheMaxEntries:          cur.matPredCacheMaxEntries,
		matPredCacheMaxEntriesWithDelta: cur.matPredCacheMaxEntriesWithDelta,
		matPredCacheMaxBitmapCard:       cur.matPredCacheMaxBitmapCard,
	}, true
}

func forceCompactSnapshotFieldBase(base map[string]*[]index, layer *fieldDeltaLayer) (map[string]*[]index, bool) {
	if layer == nil {
		return base, false
	}

	updates := make(map[string]*[]index, layer.effectiveFieldCount())
	forEachEffectiveLayerField(layer, func(field string, delta *fieldIndexDelta) {
		if delta == nil || !delta.hasEntries() {
			return
		}
		updates[field] = materializeFieldOverlay(newFieldOverlay(base[field], delta))
	})
	if len(updates) == 0 {
		return base, false
	}
	return copyIndexMapWithOverrides(base, updates), true
}

func forceCompactSnapshot(cur *indexSnapshot, nextStrMap *strMapSnapshot) (*indexSnapshot, bool) {
	if cur == nil {
		return nil, false
	}

	indexLayer := cur.getIndexLayer()
	nilLayer := cur.getNilLayer()
	lenLayer := cur.getLenLayer()

	nextIndex, indexBaseChanged := forceCompactSnapshotFieldBase(cur.index, indexLayer)
	nextNilIndex, nilBaseChanged := forceCompactSnapshotFieldBase(cur.nilIndex, nilLayer)
	nextLenIndex, _ := forceCompactSnapshotFieldBase(cur.lenIndex, lenLayer)

	nextUniverse := cur.universe
	universeDeltaCleared := cur.universeAdd != nil || cur.universeRem != nil
	if universeDeltaCleared {
		nextUniverse, _, _ = maybeCompactUniverseDelta(cur.universe, cur.universeAdd, cur.universeRem, 1, true)
	}

	indexDeltaCleared := indexLayer != nil || len(cur.indexDelta) > 0 || cur.indexDCount > 0
	nilDeltaCleared := nilLayer != nil || len(cur.nilDelta) > 0 || cur.nilDCount > 0
	lenDeltaCleared := lenLayer != nil || len(cur.lenIdxDelta) > 0 || cur.lenDCount > 0
	strMapCompacted := nextStrMap != cur.strmap

	if !indexDeltaCleared && !nilDeltaCleared && !lenDeltaCleared && !universeDeltaCleared && !strMapCompacted {
		return cur, false
	}

	return &indexSnapshot{
		seq:                             cur.seq,
		index:                           nextIndex,
		nilIndex:                        nextNilIndex,
		lenIndex:                        nextLenIndex,
		universe:                        nextUniverse,
		strmap:                          nextStrMap,
		numericRangeBucketCache:         inheritNumericRangeBucketCache(cur, !indexBaseChanged && !nilBaseChanged),
		matPredCacheMaxEntries:          cur.matPredCacheMaxEntries,
		matPredCacheMaxEntriesWithDelta: cur.matPredCacheMaxEntriesWithDelta,
		matPredCacheMaxBitmapCard:       cur.matPredCacheMaxBitmapCard,
	}, true
}

func (db *DB[K, V]) getSnapshot() *indexSnapshot {
	if s := db.snapshot.current.Load(); s != nil {
		return s
	}
	return &indexSnapshot{
		index:                           db.index,
		nilIndex:                        db.nilIndex,
		lenIndex:                        db.lenIndex,
		universe:                        db.universe,
		strmap:                          db.strmap.snapshot(),
		numericRangeBucketCache:         newNumericRangeBucketCache(),
		matPredCacheMaxEntries:          max(0, db.options.SnapshotMaterializedPredCacheMaxEntries),
		matPredCacheMaxEntriesWithDelta: max(0, db.options.SnapshotMaterializedPredCacheMaxEntriesWithDelta),
		matPredCacheMaxBitmapCard:       materializedPredCacheMaxBitmapCardinality(db.options.SnapshotMaterializedPredCacheMaxBitmapCardinality),
	}
}

func (s *indexSnapshot) loadMaterializedPred(key string) (*roaring64.Bitmap, bool) {
	if s == nil || key == "" {
		return nil, false
	}
	if s.materializedPredCacheLimit() <= 0 {
		return nil, false
	}
	v, ok := s.matPredCache.Load(key)
	if !ok {
		return nil, false
	}
	e, _ := v.(*materializedPredCacheEntry)
	if e == nil {
		return nil, true
	}
	return e.bm, true
}

func (s *indexSnapshot) storeMaterializedPred(key string, bm *roaring64.Bitmap) {
	if s == nil || key == "" {
		return
	}
	limit := s.materializedPredCacheLimit()
	if limit <= 0 {
		return
	}
	if bm != nil && s.matPredCacheMaxBitmapCard > 0 &&
		bm.GetCardinality() > s.matPredCacheMaxBitmapCard {
		return
	}
	if _, ok := s.matPredCache.Load(key); ok {
		return
	}
	if int(s.matPredCacheCount.Load()) >= limit {
		return
	}
	e := &materializedPredCacheEntry{bm: bm}
	if _, loaded := s.matPredCache.LoadOrStore(key, e); loaded {
		return
	}
	s.matPredCacheCount.Add(1)
}

// tryStoreMaterializedPredOversized stores a small bounded number of
// oversized materialized bitmaps per stable snapshot as a hot-cache fallback.
func (s *indexSnapshot) tryStoreMaterializedPredOversized(key string, bm *roaring64.Bitmap) bool {
	if s == nil || key == "" || bm == nil || bm.IsEmpty() {
		return false
	}
	if s.hasAnyDeltaState() {
		return false
	}
	if s.matPredCacheMaxBitmapCard == 0 || bm.GetCardinality() <= s.matPredCacheMaxBitmapCard {
		return false
	}
	limit := s.materializedPredCacheLimit()
	if limit <= 0 {
		return false
	}
	if _, ok := s.matPredCache.Load(key); ok {
		return false
	}
	for {
		c := s.matPredCacheOversizedCount.Load()
		if c >= matPredCacheOversizedMaxEntries {
			return false
		}
		if s.matPredCacheOversizedCount.CompareAndSwap(c, c+1) {
			break
		}
	}
	stored := false
	defer func() {
		if !stored {
			s.matPredCacheOversizedCount.Add(-1)
		}
	}()
	if int(s.matPredCacheCount.Load()) >= limit {
		return false
	}
	if _, loaded := s.matPredCache.LoadOrStore(key, &materializedPredCacheEntry{bm: bm}); loaded {
		return false
	}
	s.matPredCacheCount.Add(1)
	stored = true
	return true
}

func (s *indexSnapshot) materializedPredCacheLimit() int {
	if s == nil {
		return 0
	}
	if s.hasAnyDeltaState() {
		return s.matPredCacheMaxEntriesWithDelta
	}
	return s.matPredCacheMaxEntries
}

func (s *indexSnapshot) getIndexLayer() *fieldDeltaLayer {
	if s == nil {
		return nil
	}
	if s.indexLayer != nil {
		return s.indexLayer
	}
	return layerFromFlatDeltaMap(s.indexDelta)
}

func (s *indexSnapshot) getNilLayer() *fieldDeltaLayer {
	if s == nil {
		return nil
	}
	if s.nilLayer != nil {
		return s.nilLayer
	}
	return layerFromFlatDeltaMap(s.nilDelta)
}

func (s *indexSnapshot) getLenLayer() *fieldDeltaLayer {
	if s == nil {
		return nil
	}
	if s.lenLayer != nil {
		return s.lenLayer
	}
	return layerFromFlatDeltaMap(s.lenIdxDelta)
}

func (s *indexSnapshot) indexDeltaCount() int {
	if s == nil {
		return 0
	}
	if len(s.indexDelta) > 0 {
		return len(s.indexDelta)
	}
	return s.indexDCount
}

func (s *indexSnapshot) nilDeltaCount() int {
	if s == nil {
		return 0
	}
	if len(s.nilDelta) > 0 {
		return len(s.nilDelta)
	}
	return s.nilDCount
}

func (s *indexSnapshot) lenDeltaCount() int {
	if s == nil {
		return 0
	}
	if len(s.lenIdxDelta) > 0 {
		return len(s.lenIdxDelta)
	}
	return s.lenDCount
}

func (s *indexSnapshot) fieldIndexSlice(field string) *[]index {
	if s == nil {
		return nil
	}
	if s.indexView != nil {
		if v, ok := s.indexView[field]; ok {
			return v
		}
	}
	return s.index[field]
}

func (s *indexSnapshot) nilFieldIndexSlice(field string) *[]index {
	if s == nil {
		return nil
	}
	if s.nilIndexView != nil {
		if v, ok := s.nilIndexView[field]; ok {
			return v
		}
	}
	return s.nilIndex[field]
}

func (s *indexSnapshot) lenFieldIndexSlice(field string) *[]index {
	if s == nil {
		return nil
	}
	if s.lenIndexView != nil {
		if v, ok := s.lenIndexView[field]; ok {
			return v
		}
	}
	return s.lenIndex[field]
}

func (s *indexSnapshot) fieldDelta(field string) *fieldIndexDelta {
	if s == nil {
		return nil
	}
	if len(s.indexDelta) > 0 {
		d := s.indexDelta[field]
		if d == nil || !d.hasEntries() {
			return nil
		}
		return d
	}
	if s.indexLayer == nil {
		return nil
	}
	if cached, ok := s.indexDeltaCache.Load(field); ok {
		return cached.(*fieldIndexDelta)
	}
	d := lookupLayerFieldDelta(s.indexLayer, field)
	s.indexDeltaCache.Store(field, d)
	return d
}

func (s *indexSnapshot) nilFieldDelta(field string) *fieldIndexDelta {
	if s == nil {
		return nil
	}
	if len(s.nilDelta) > 0 {
		d := s.nilDelta[field]
		if d == nil || !d.hasEntries() {
			return nil
		}
		return d
	}
	if s.nilLayer == nil {
		return nil
	}
	if cached, ok := s.nilDeltaCache.Load(field); ok {
		return cached.(*fieldIndexDelta)
	}
	d := lookupLayerFieldDelta(s.nilLayer, field)
	s.nilDeltaCache.Store(field, d)
	return d
}

func (s *indexSnapshot) fieldDeltaEntry(field, key string) (indexDeltaEntry, bool) {
	return s.familyDeltaEntry(s.indexDelta, s.indexLayer, field, key)
}

func (s *indexSnapshot) familyDeltaEntry(flat map[string]*fieldIndexDelta, layer *fieldDeltaLayer, field, key string) (indexDeltaEntry, bool) {
	if s == nil {
		return indexDeltaEntry{}, false
	}
	if len(flat) > 0 {
		d := flat[field]
		if d == nil {
			return indexDeltaEntry{}, false
		}
		e, ok := d.get(key)
		if !ok || deltaEntryIsEmpty(e) {
			return indexDeltaEntry{}, false
		}
		return e, true
	}
	if layer == nil {
		return indexDeltaEntry{}, false
	}
	return lookupLayerFieldDeltaEntry(layer, field, key)
}

func (s *indexSnapshot) nilFieldDeltaEntry(field string) (indexDeltaEntry, bool) {
	return s.familyDeltaEntry(s.nilDelta, s.nilLayer, field, nilIndexEntryKey)
}

func (s *indexSnapshot) lenFieldDelta(field string) *fieldIndexDelta {
	if s == nil {
		return nil
	}
	if len(s.lenIdxDelta) > 0 {
		d := s.lenIdxDelta[field]
		if d == nil || !d.hasEntries() {
			return nil
		}
		return d
	}
	if s.lenLayer == nil {
		return nil
	}
	if cached, ok := s.lenDeltaCache.Load(field); ok {
		return cached.(*fieldIndexDelta)
	}
	d := lookupLayerFieldDelta(s.lenLayer, field)
	s.lenDeltaCache.Store(field, d)
	return d
}

func (s *indexSnapshot) nilFieldNameSet() map[string]struct{} {
	if s == nil {
		return nil
	}
	fields := make(map[string]struct{}, len(s.nilIndex)+s.nilDeltaCount())
	for f := range s.nilIndex {
		fields[f] = struct{}{}
	}
	if len(s.nilDelta) > 0 {
		for f := range s.nilDelta {
			fields[f] = struct{}{}
		}
		return fields
	}
	forEachEffectiveLayerField(s.nilLayer, func(field string, _ *fieldIndexDelta) {
		fields[field] = struct{}{}
	})
	return fields
}

func (s *indexSnapshot) fieldNameSet() map[string]struct{} {
	if s == nil {
		return nil
	}
	fields := make(map[string]struct{}, len(s.index)+s.indexDeltaCount())
	for f := range s.index {
		fields[f] = struct{}{}
	}
	if len(s.indexDelta) > 0 {
		for f := range s.indexDelta {
			fields[f] = struct{}{}
		}
		return fields
	}
	forEachEffectiveLayerField(s.indexLayer, func(field string, _ *fieldIndexDelta) {
		fields[field] = struct{}{}
	})
	return fields
}

func (s *indexSnapshot) indexedFieldNameSet() map[string]struct{} {
	if s == nil {
		return nil
	}
	fields := s.fieldNameSet()
	nilFields := s.nilFieldNameSet()
	if len(fields) == 0 {
		if len(nilFields) == 0 {
			return nil
		}
		return nilFields
	}
	for field := range nilFields {
		fields[field] = struct{}{}
	}
	return fields
}

func (s *indexSnapshot) lenFieldNameSet() map[string]struct{} {
	if s == nil {
		return nil
	}
	fields := make(map[string]struct{}, len(s.lenIndex)+s.lenDeltaCount())
	for f := range s.lenIndex {
		fields[f] = struct{}{}
	}
	if len(s.lenIdxDelta) > 0 {
		for f := range s.lenIdxDelta {
			fields[f] = struct{}{}
		}
		return fields
	}
	forEachEffectiveLayerField(s.lenLayer, func(field string, _ *fieldIndexDelta) {
		fields[field] = struct{}{}
	})
	return fields
}

func lookupLayerFieldDelta(layer *fieldDeltaLayer, field string) *fieldIndexDelta {
	if layer == nil {
		return nil
	}
	stack := make([]*fieldIndexDelta, 0, 4)
	for cur := layer; cur != nil; cur = cur.parent {
		d, ok := cur.fields[field]
		if !ok {
			continue
		}
		if d == nil {
			break
		}
		if !d.hasEntries() {
			continue
		}
		stack = append(stack, d)
	}
	if len(stack) == 0 {
		return nil
	}
	if len(stack) == 1 {
		return stack[0]
	}
	var out *fieldIndexDelta
	for i := len(stack) - 1; i >= 0; i-- {
		if out == nil {
			out = cloneFieldIndexDeltaShallow(stack[i])
			continue
		}
		applyFieldDeltaInPlaceFromDelta(out, stack[i])
	}
	if out == nil || !out.hasEntries() {
		return nil
	}
	return out
}

func lookupLayerFieldDeltaEntry(layer *fieldDeltaLayer, field, key string) (indexDeltaEntry, bool) {
	if layer == nil {
		return indexDeltaEntry{}, false
	}
	stack := make([]indexDeltaEntry, 0, 4)
	for cur := layer; cur != nil; cur = cur.parent {
		d, ok := cur.fields[field]
		if !ok {
			continue
		}
		if d == nil {
			// Field compacted into base in this (newer) layer.
			break
		}
		e, ok := d.get(key)
		if !ok || deltaEntryIsEmpty(e) {
			continue
		}
		stack = append(stack, e)
	}
	if len(stack) == 0 {
		return indexDeltaEntry{}, false
	}
	out := stack[len(stack)-1] // oldest entry
	for i := len(stack) - 2; i >= 0; i-- {
		out = deltaEntryMerge(out, stack[i])
	}
	if deltaEntryIsEmpty(out) {
		return indexDeltaEntry{}, false
	}
	return out, true
}

func (s *indexSnapshot) fieldLookupOwned(field, key string, scratch *roaring64.Bitmap) (*roaring64.Bitmap, bool) {
	if s == nil {
		return nil, false
	}

	var base postingList
	if baseSlice := s.fieldIndexSlice(field); baseSlice != nil && len(*baseSlice) > 0 {
		if i := lowerBoundIndex(*baseSlice, key); i < len(*baseSlice) && indexKeyEqualsString((*baseSlice)[i].Key, key) {
			base = (*baseSlice)[i].IDs
		}
	}

	de, ok := s.fieldDeltaEntry(field, key)
	if !ok {
		return base.ToBitmapOwned(scratch)
	}
	return composePostingOwned(base, de, scratch)
}

func (s *indexSnapshot) nilFieldLookupOwned(field string, scratch *roaring64.Bitmap) (*roaring64.Bitmap, bool) {
	if s == nil {
		return nil, false
	}

	var base postingList
	if baseSlice := s.nilFieldIndexSlice(field); baseSlice != nil && len(*baseSlice) > 0 {
		base = (*baseSlice)[0].IDs
	}

	de, ok := s.nilFieldDeltaEntry(field)
	if !ok {
		return base.ToBitmapOwned(scratch)
	}
	return composePostingOwned(base, de, scratch)
}

type layerUpperStats struct {
	// keys is an upper-bound estimate of effective delta keys.
	keys int
	// ops is an upper-bound estimate of effective delta operations.
	ops uint64
	// has reports whether any effective delta state exists for field.
	has bool
}

// lookupLayerFieldUpperStats returns a cheap upper-bound estimate for effective
// field delta size in a layer chain. It intentionally overestimates for
// overlapping keys to avoid expensive full materialization on hot write paths.
func lookupLayerFieldUpperStats(layer *fieldDeltaLayer, field string) (keys int, ops uint64, has bool) {
	if layer == nil {
		return 0, 0, false
	}
	layer.fieldUpperStatsMu.RLock()
	if s, ok := layer.fieldUpperStats[field]; ok {
		layer.fieldUpperStatsMu.RUnlock()
		return s.keys, s.ops, s.has
	}
	layer.fieldUpperStatsMu.RUnlock()

	var out layerUpperStats
	if d, exists := layer.fields[field]; exists {
		if d != nil && d.hasEntries() {
			out.has = true
			out.keys = d.keyCount()
			out.ops = d.ops
			if pk, po, pok := lookupLayerFieldUpperStats(layer.parent, field); pok {
				out.keys += pk
				out.ops += po
			}
		}
	} else if pk, po, pok := lookupLayerFieldUpperStats(layer.parent, field); pok {
		out = layerUpperStats{
			keys: pk,
			ops:  po,
			has:  true,
		}
	}

	layer.fieldUpperStatsMu.Lock()
	if layer.fieldUpperStats == nil {
		layer.fieldUpperStats = make(map[string]layerUpperStats, 8)
	}
	layer.fieldUpperStats[field] = out
	layer.fieldUpperStatsMu.Unlock()
	return out.keys, out.ops, out.has
}

func (db *DB[K, V]) snapshotFieldIndexSlice(field string) *[]index {
	return db.getSnapshot().fieldIndexSlice(field)
}

func (db *DB[K, V]) snapshotNilFieldIndexSlice(field string) *[]index {
	return db.getSnapshot().nilFieldIndexSlice(field)
}

func (db *DB[K, V]) snapshotLenFieldIndexSlice(field string) *[]index {
	return db.getSnapshot().lenFieldIndexSlice(field)
}

func (db *DB[K, V]) strMapSnapshot() *strMapSnapshot {
	if db.strmapView != nil {
		return db.strmapView
	}
	return db.getSnapshot().strmap
}

func buildSnapshotDeltaMap(changes map[string]map[string]indexDeltaEntry, fixed8ForField func(string) bool) map[string]*fieldIndexDelta {
	if len(changes) == 0 {
		return nil
	}
	var out map[string]*fieldIndexDelta
	for f, ch := range changes {
		if len(ch) == 0 {
			continue
		}
		fixed8 := false
		if fixed8ForField != nil {
			fixed8 = fixed8ForField(f)
		}
		d := buildFieldDeltaPatch(ch, fixed8)
		if d == nil || !d.hasEntries() {
			continue
		}
		if out == nil {
			out = make(map[string]*fieldIndexDelta, len(changes))
		}
		out[f] = d
		changes[f] = nil
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func buildFieldDeltaPatch(changes map[string]indexDeltaEntry, fixed8 bool) *fieldIndexDelta {
	if len(changes) == 0 {
		return nil
	}
	var ops uint64
	for key, e := range changes {
		// entries in changes are produced by write-delta merge and already
		// canonicalized; keep final pass allocation-free.
		e = deltaEntryNormalize(e)
		if deltaEntryIsEmpty(e) {
			delete(changes, key)
			continue
		}
		changes[key] = e
		ops += deltaEntryOps(e)
	}
	if len(changes) == 0 {
		return nil
	}
	if len(changes) == 1 {
		for key, e := range changes {
			return &fieldIndexDelta{
				singleKey:   key,
				singleEntry: e,
				singleSet:   true,
				fixed8:      fixed8,
				ops:         ops,
			}
		}
	}
	out := &fieldIndexDelta{
		byKey:  changes,
		fixed8: fixed8,
		ops:    ops,
	}
	out.invalidateSortedKeys()
	return out
}

func layerFromFlatDeltaMap(delta map[string]*fieldIndexDelta) *fieldDeltaLayer {
	if len(delta) == 0 {
		return nil
	}
	return &fieldDeltaLayer{
		fields: delta,
		depth:  1,
	}
}

func maybeExposeFlatLayerMap(layer *fieldDeltaLayer) map[string]*fieldIndexDelta {
	if layer == nil || layer.parent != nil || len(layer.fields) == 0 {
		return nil
	}
	return layer.fields
}

func (layer *fieldDeltaLayer) effectiveFieldNames() []string {
	if layer == nil {
		return nil
	}
	seen := make(map[string]struct{}, 16)
	fields := make([]string, 0, 16)
	for cur := layer; cur != nil; cur = cur.parent {
		for f, d := range cur.fields {
			if _, exists := seen[f]; exists {
				continue
			}
			if d == nil {
				// Newer tombstone masks all older entries for this field.
				seen[f] = struct{}{}
				continue
			}
			if !d.hasEntries() {
				// Empty patch does not mask older entries.
				continue
			}
			seen[f] = struct{}{}
			fields = append(fields, f)
		}
	}
	return fields
}

func forEachEffectiveLayerField(layer *fieldDeltaLayer, fn func(field string, delta *fieldIndexDelta)) {
	if layer == nil {
		return
	}
	seen := make(map[string]struct{}, 16)
	for cur := layer; cur != nil; cur = cur.parent {
		for f := range cur.fields {
			if _, exists := seen[f]; exists {
				continue
			}
			seen[f] = struct{}{}
		}
	}
	for f := range seen {
		d := lookupLayerFieldDelta(layer, f)
		if d == nil || !d.hasEntries() {
			continue
		}
		fn(f, d)
	}
}

func (layer *fieldDeltaLayer) effectiveFieldCount() int {
	if layer == nil {
		return 0
	}
	count := 0
	forEachEffectiveLayerField(layer, func(_ string, _ *fieldIndexDelta) {
		count++
	})
	return count
}

func maybeFlattenDeltaLayerByDepth(layer *fieldDeltaLayer, fieldCount int, maxDepth int) (*fieldDeltaLayer, int) {
	if layer == nil || maxDepth <= 0 || layer.depth <= maxDepth {
		return layer, fieldCount
	}
	flat := flattenEffectiveDeltaLayerMap(layer, fieldCount)
	if len(flat) == 0 {
		return nil, 0
	}
	return &fieldDeltaLayer{
		fields: flat,
		depth:  1,
	}, len(flat)
}

func flattenEffectiveDeltaLayerMap(layer *fieldDeltaLayer, capHint int) map[string]*fieldIndexDelta {
	if layer == nil {
		return nil
	}

	fields := layer.effectiveFieldNames()
	if len(fields) == 0 {
		return nil
	}
	if capHint < len(fields) {
		capHint = len(fields)
	}
	if capHint < 0 {
		capHint = 0
	}

	out := make(map[string]*fieldIndexDelta, capHint)
	scratch := getLayerFieldDeltaMergeScratch()
	defer releaseLayerFieldDeltaMergeScratch(scratch)

	for _, f := range fields {
		d := lookupLayerFieldDeltaWithScratch(layer, f, scratch)
		if d == nil || !d.hasEntries() {
			continue
		}
		out[f] = freezeFieldIndexDelta(d)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

type deltaCompactFieldPreCandidate struct {
	field string
	keys  int
	ops   uint64
}

type deltaCompactFieldCandidate struct {
	field   string
	keys    int
	ops     uint64
	prevHad bool
	delta   *fieldIndexDelta
}

func sortDeltaCompactPreCandidates(candidates []deltaCompactFieldPreCandidate) {
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].ops != candidates[j].ops {
			return candidates[i].ops > candidates[j].ops
		}
		if candidates[i].keys != candidates[j].keys {
			return candidates[i].keys > candidates[j].keys
		}
		return candidates[i].field < candidates[j].field
	})
}

func sortDeltaCompactCandidates(candidates []deltaCompactFieldCandidate) {
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].ops != candidates[j].ops {
			return candidates[i].ops > candidates[j].ops
		}
		if candidates[i].keys != candidates[j].keys {
			return candidates[i].keys > candidates[j].keys
		}
		return candidates[i].field < candidates[j].field
	})
}

type layerFieldDeltaMergeScratch struct {
	byKey        map[string]indexDeltaEntry
	owned        map[string]struct{}
	maxByKeyUsed int
	stack        []*fieldIndexDelta
	fixed8       bool
	ops          uint64
	deltaView    fieldIndexDelta
}

const (
	deltaCompactProbeMultiplier          = 4
	layerFieldDeltaMergeScratchInitCap   = 64
	layerFieldDeltaMergeScratchMaxMapLen = 16 << 10
	layerFieldDeltaMergeScratchMaxStack  = 128
)

var layerFieldDeltaMergeScratchPool = sync.Pool{
	New: func() any {
		return &layerFieldDeltaMergeScratch{
			byKey: make(map[string]indexDeltaEntry, layerFieldDeltaMergeScratchInitCap),
			owned: make(map[string]struct{}, 8),
			stack: make([]*fieldIndexDelta, 0, 8),
		}
	},
}

func getLayerFieldDeltaMergeScratch() *layerFieldDeltaMergeScratch {
	return layerFieldDeltaMergeScratchPool.Get().(*layerFieldDeltaMergeScratch)
}

func (s *layerFieldDeltaMergeScratch) resetMergedState(releaseOwned bool) {
	if s == nil {
		return
	}
	if releaseOwned && len(s.owned) > 0 {
		for key := range s.owned {
			if e, ok := s.byKey[key]; ok {
				releaseDeltaEntryBitmaps(e)
			}
		}
	}
	if len(s.byKey) > 0 {
		if s.maxByKeyUsed > layerFieldDeltaMergeScratchMaxMapLen {
			s.byKey = make(map[string]indexDeltaEntry, layerFieldDeltaMergeScratchInitCap)
		} else {
			clear(s.byKey)
		}
	}
	if len(s.owned) > 0 {
		if len(s.owned) > layerFieldDeltaMergeScratchMaxMapLen {
			s.owned = make(map[string]struct{}, 8)
		} else {
			clear(s.owned)
		}
	}
	s.maxByKeyUsed = 0
	s.fixed8 = false
	s.ops = 0
	s.deltaView = fieldIndexDelta{}
}

func releaseLayerFieldDeltaMergeScratch(s *layerFieldDeltaMergeScratch) {
	if s == nil {
		return
	}
	s.resetMergedState(true)
	if cap(s.stack) > layerFieldDeltaMergeScratchMaxStack {
		s.stack = make([]*fieldIndexDelta, 0, 8)
	} else {
		s.stack = s.stack[:0]
	}
	layerFieldDeltaMergeScratchPool.Put(s)
}

func detachMergedFieldDeltaFromScratch(scratch *layerFieldDeltaMergeScratch) *fieldIndexDelta {
	if scratch == nil || len(scratch.byKey) == 0 {
		return nil
	}
	fixed8 := scratch.fixed8
	ops := scratch.ops
	if len(scratch.byKey) == 1 {
		for key, e := range scratch.byKey {
			out := &fieldIndexDelta{
				singleKey:   key,
				singleEntry: e,
				singleSet:   true,
				fixed8:      fixed8,
				ops:         ops,
			}
			scratch.resetMergedState(false)
			return out
		}
	}
	capHint := max(len(scratch.byKey), layerFieldDeltaMergeScratchInitCap)
	out := freezeOwnedFieldIndexDeltaMap(scratch.byKey, fixed8, ops)
	scratch.byKey = make(map[string]indexDeltaEntry, capHint)
	scratch.resetMergedState(false)
	return out
}

func collectLayerFieldDeltaStack(layer *fieldDeltaLayer, field string, scratch *layerFieldDeltaMergeScratch) []*fieldIndexDelta {
	if scratch == nil {
		return nil
	}
	scratch.stack = scratch.stack[:0]
	for cur := layer; cur != nil; cur = cur.parent {
		d, ok := cur.fields[field]
		if !ok {
			continue
		}
		if d == nil {
			// Field compacted into base in this (newer) layer.
			break
		}
		if !d.hasEntries() {
			continue
		}
		scratch.stack = append(scratch.stack, d)
	}
	return scratch.stack
}

func mergeLayerFieldDeltaStackIntoScratch(stack []*fieldIndexDelta, scratch *layerFieldDeltaMergeScratch) {
	if scratch == nil {
		return
	}
	scratch.resetMergedState(true)
	for i := len(stack) - 1; i >= 0; i-- {
		d := stack[i]
		if d == nil || !d.hasEntries() {
			continue
		}
		if d.fixed8 {
			scratch.fixed8 = true
		}
		d.forEach(func(key string, e indexDeltaEntry) {
			mergeLayerFieldDeltaEntryIntoScratch(scratch, key, e)
		})
	}
}

func mergeLayerFieldDeltaEntryIntoScratch(scratch *layerFieldDeltaMergeScratch, key string, e indexDeltaEntry) {
	if scratch == nil || deltaEntryIsEmpty(e) {
		return
	}
	if prev, ok := scratch.byKey[key]; ok {
		oldOps := deltaEntryOps(prev)
		if scratch.ops >= oldOps {
			scratch.ops -= oldOps
		} else {
			scratch.ops = 0
		}
		_, owned := scratch.owned[key]
		var merged indexDeltaEntry
		if owned {
			merged = deltaEntryMergeOwned(prev, e)
		} else {
			merged = deltaEntryMergeOwned(cloneDeltaEntryBitmaps(prev), e)
		}
		if deltaEntryIsEmpty(merged) {
			delete(scratch.byKey, key)
			if owned {
				delete(scratch.owned, key)
			}
		} else {
			scratch.byKey[key] = merged
			scratch.owned[key] = struct{}{}
			scratch.ops += deltaEntryOps(merged)
		}
		return
	}
	scratch.byKey[key] = e
	scratch.ops += deltaEntryOps(e)
	if n := len(scratch.byKey); n > scratch.maxByKeyUsed {
		scratch.maxByKeyUsed = n
	}
}

func lookupLayerFieldDeltaStatsWithScratch(layer *fieldDeltaLayer, field string, scratch *layerFieldDeltaMergeScratch) (keys int, ops uint64, has bool) {
	if layer == nil {
		return 0, 0, false
	}
	if scratch == nil {
		d := lookupLayerFieldDelta(layer, field)
		if d == nil || !d.hasEntries() {
			return 0, 0, false
		}
		return d.keyCount(), d.ops, true
	}
	stack := collectLayerFieldDeltaStack(layer, field, scratch)
	if len(stack) == 0 {
		return 0, 0, false
	}
	if len(stack) == 1 {
		d := stack[0]
		return d.keyCount(), d.ops, true
	}
	mergeLayerFieldDeltaStackIntoScratch(stack, scratch)
	if len(scratch.byKey) == 0 {
		return 0, 0, false
	}
	return len(scratch.byKey), scratch.ops, true
}

func lookupLayerFieldDeltaWithScratch(layer *fieldDeltaLayer, field string, scratch *layerFieldDeltaMergeScratch) *fieldIndexDelta {
	if layer == nil {
		return nil
	}
	if scratch == nil {
		return lookupLayerFieldDelta(layer, field)
	}
	d, borrowed := lookupLayerFieldDeltaBorrowedWithScratch(layer, field, scratch)
	if d == nil || !d.hasEntries() || !borrowed {
		return d
	}
	return detachMergedFieldDeltaFromScratch(scratch)
}

func lookupLayerFieldDeltaBorrowedWithScratch(layer *fieldDeltaLayer, field string, scratch *layerFieldDeltaMergeScratch) (*fieldIndexDelta, bool) {
	if layer == nil {
		return nil, false
	}
	if scratch == nil {
		return lookupLayerFieldDelta(layer, field), false
	}
	stack := collectLayerFieldDeltaStack(layer, field, scratch)
	if len(stack) == 0 {
		return nil, false
	}
	if len(stack) == 1 {
		return stack[0], false
	}
	mergeLayerFieldDeltaStackIntoScratch(stack, scratch)
	if len(scratch.byKey) == 0 {
		return nil, false
	}
	if len(scratch.byKey) == 1 {
		for key, e := range scratch.byKey {
			scratch.deltaView = fieldIndexDelta{
				singleKey:   key,
				singleEntry: e,
				singleSet:   true,
				fixed8:      scratch.fixed8,
				ops:         scratch.ops,
			}
			return &scratch.deltaView, true
		}
	}
	scratch.deltaView = fieldIndexDelta{
		byKey:  scratch.byKey,
		fixed8: scratch.fixed8,
		ops:    scratch.ops,
	}
	return &scratch.deltaView, true
}

func compactDeltaLayerIntoBase(base map[string]*[]index, layer *fieldDeltaLayer, opt *Options, force bool) (map[string]*[]index, *fieldDeltaLayer, int) {
	if layer == nil || opt.SnapshotDeltaCompactMaxFieldsPerPublish <= 0 {
		return base, layer, 0
	}

	candidates := collectLayerCompactCandidates(base, layer, opt, force)
	if len(candidates) == 0 {
		return base, layer, 0
	}
	sortDeltaCompactCandidates(candidates)

	limit := len(candidates)
	if opt.SnapshotDeltaCompactMaxFieldsPerPublish > 0 && opt.SnapshotDeltaCompactMaxFieldsPerPublish < limit {
		limit = opt.SnapshotDeltaCompactMaxFieldsPerPublish
	}

	updates := make(map[string]*[]index, limit)
	for i := 0; i < limit; i++ {
		c := candidates[i]
		eff := c.delta
		if eff == nil {
			eff = lookupLayerFieldDelta(layer, c.field)
		}
		if eff == nil || !eff.hasEntries() {
			continue
		}
		ov := newFieldOverlay(base[c.field], eff)
		updates[c.field] = materializeFieldOverlay(ov)
	}

	// Rebase over parent instead of stacking over current layer: this keeps
	// depth from growing on every compaction pass and reduces flatten churn.
	parentMask := make(map[string]bool, limit)
	parentScratch := getLayerFieldDeltaMergeScratch()
	for i := 0; i < limit; i++ {
		f := candidates[i].field
		if _, exists := parentMask[f]; exists {
			continue
		}
		_, _, hasParent := lookupLayerFieldDeltaStatsWithScratch(layer.parent, f, parentScratch)
		parentMask[f] = hasParent
	}
	releaseLayerFieldDeltaMergeScratch(parentScratch)

	nextFields := make(map[string]*fieldIndexDelta, len(layer.fields)+limit)
	for f, d := range layer.fields {
		hadInParent, compacted := parentMask[f]
		if compacted {
			if hadInParent {
				nextFields[f] = nil
			}
			continue
		}
		nextFields[f] = d
	}
	for f, hadInParent := range parentMask {
		if !hadInParent {
			continue
		}
		if _, exists := layer.fields[f]; exists {
			continue
		}
		nextFields[f] = nil
	}

	nextLayer := layer.parent
	if len(nextFields) > 0 {
		nextLayer = &fieldDeltaLayer{
			parent: layer.parent,
			fields: nextFields,
			depth:  1,
		}
		if layer.parent != nil {
			nextLayer.depth = layer.parent.depth + 1
		}
	}

	return copyIndexMapWithOverrides(base, updates), nextLayer, limit
}

func collectLayerCompactCandidates(base map[string]*[]index, layer *fieldDeltaLayer, opt *Options, force bool) []deltaCompactFieldCandidate {
	if layer == nil {
		return nil
	}

	// Phase 1: cheap shortlist by upper-bound stats only.
	fields := layer.effectiveFieldNames()
	if len(fields) == 0 {
		return nil
	}
	pre := make([]deltaCompactFieldPreCandidate, 0, len(fields))
	for _, f := range fields {
		upperKeys, upperOps, has := lookupLayerFieldUpperStats(layer, f)
		if !has {
			continue
		}
		if !force {
			byKeysUpper := opt.SnapshotDeltaCompactFieldKeys > 0 && upperKeys >= opt.SnapshotDeltaCompactFieldKeys
			byOpsUpper := opt.SnapshotDeltaCompactFieldOps > 0 && upperOps >= uint64(opt.SnapshotDeltaCompactFieldOps)
			if !byKeysUpper && !byOpsUpper {
				continue
			}

			baseKeys := 0
			if bs := base[f]; bs != nil {
				baseKeys = len(*bs)
			}
			if baseKeys > defaultSnapshotDeltaCompactLargeBaseFieldKeys &&
				defaultSnapshotDeltaCompactLargeBaseMinDeltaDiv > 0 {
				minDeltaKeys := baseKeys / defaultSnapshotDeltaCompactLargeBaseMinDeltaDiv
				if minDeltaKeys < 1 {
					minDeltaKeys = 1
				}
				forcedByOps := defaultSnapshotDeltaCompactForceFieldOps > 0 && upperOps >= defaultSnapshotDeltaCompactForceFieldOps
				if upperKeys < minDeltaKeys && !forcedByOps {
					continue
				}
			}
		}

		pre = append(pre, deltaCompactFieldPreCandidate{
			field: f,
			keys:  upperKeys,
			ops:   upperOps,
		})
	}
	if len(pre) == 0 {
		return nil
	}
	sortDeltaCompactPreCandidates(pre)

	probeLimit := len(pre)
	if opt.SnapshotDeltaCompactMaxFieldsPerPublish > 0 {
		maxProbe := max(opt.SnapshotDeltaCompactMaxFieldsPerPublish*deltaCompactProbeMultiplier, opt.SnapshotDeltaCompactMaxFieldsPerPublish)
		if maxProbe < probeLimit {
			probeLimit = maxProbe
		}
	}

	// Phase 2: exact stats for shortlisted fields without full map materialization.
	candidates := make([]deltaCompactFieldCandidate, 0, min(probeLimit, 8))
	scratch := getLayerFieldDeltaMergeScratch()
	defer releaseLayerFieldDeltaMergeScratch(scratch)
	for i := 0; i < probeLimit; i++ {
		f := pre[i].field
		eff, borrowed := lookupLayerFieldDeltaBorrowedWithScratch(layer, f, scratch)
		if eff == nil || !eff.hasEntries() {
			continue
		}
		keys, ops := eff.keyCount(), eff.ops
		if !force {
			byKeys := opt.SnapshotDeltaCompactFieldKeys > 0 && keys >= opt.SnapshotDeltaCompactFieldKeys
			byOps := opt.SnapshotDeltaCompactFieldOps > 0 && ops >= uint64(opt.SnapshotDeltaCompactFieldOps)
			if !byKeys && !byOps {
				continue
			}

			baseKeys := 0
			if bs := base[f]; bs != nil {
				baseKeys = len(*bs)
			}
			if baseKeys > defaultSnapshotDeltaCompactLargeBaseFieldKeys &&
				defaultSnapshotDeltaCompactLargeBaseMinDeltaDiv > 0 {
				minDeltaKeys := baseKeys / defaultSnapshotDeltaCompactLargeBaseMinDeltaDiv
				if minDeltaKeys < 1 {
					minDeltaKeys = 1
				}
				forcedByOps := defaultSnapshotDeltaCompactForceFieldOps > 0 && ops >= defaultSnapshotDeltaCompactForceFieldOps
				if keys < minDeltaKeys && !forcedByOps {
					continue
				}
			}
		}

		if borrowed {
			eff = detachMergedFieldDeltaFromScratch(scratch)
		}
		candidates = append(candidates, deltaCompactFieldCandidate{
			field: f,
			keys:  keys,
			ops:   ops,
			delta: eff,
		})
	}
	return candidates
}

func accumulateDeltaLayerState(base map[string]*[]index, prevLayer *fieldDeltaLayer, prevCount int, changes map[string]map[string]indexDeltaEntry, opt *Options) (map[string]*[]index, *fieldDeltaLayer, int) {

	if len(changes) == 0 {
		return base, prevLayer, prevCount
	}

	nextFields := make(map[string]*fieldIndexDelta, len(changes))
	probeLayer := &fieldDeltaLayer{
		parent: prevLayer,
		fields: nextFields,
		depth:  1,
	}
	if prevLayer != nil {
		probeLayer.depth = prevLayer.depth + 1
	}

	for f, ch := range changes {
		if len(ch) == 0 {
			continue
		}
		nextDelta := buildFieldDeltaPatch(ch, false)
		if nextDelta == nil || !nextDelta.hasEntries() {
			continue
		}
		nextFields[f] = nextDelta
		changes[f] = nil
	}

	if len(nextFields) == 0 {
		return base, prevLayer, prevCount
	}

	if opt.SnapshotDeltaCompactMaxFieldsPerPublish > 0 {
		candidates := make([]deltaCompactFieldCandidate, 0, len(nextFields))
		prevHadCache := make(map[string]bool, 4)
		prevHad := func(field string) bool {
			if v, ok := prevHadCache[field]; ok {
				return v
			}
			_, _, had := lookupLayerFieldUpperStats(prevLayer, field)
			prevHadCache[field] = had
			return had
		}
		scratch := getLayerFieldDeltaMergeScratch()
		defer releaseLayerFieldDeltaMergeScratch(scratch)

		for f := range nextFields {
			nextDelta := nextFields[f]
			if nextDelta == nil || !nextDelta.hasEntries() {
				if prevHad(f) {
					nextFields[f] = nil
				} else {
					delete(nextFields, f)
				}
				continue
			}

			upperKeys := nextDelta.keyCount()
			upperOps := nextDelta.ops
			if pk, po, pok := lookupLayerFieldUpperStats(prevLayer, f); pok {
				upperKeys += pk
				upperOps += po
			}

			byKeysUpper := opt.SnapshotDeltaCompactFieldKeys > 0 && upperKeys >= opt.SnapshotDeltaCompactFieldKeys
			byOpsUpper := opt.SnapshotDeltaCompactFieldOps > 0 && upperOps >= uint64(opt.SnapshotDeltaCompactFieldOps)
			if !byKeysUpper && !byOpsUpper {
				continue
			}

			baseKeys := 0
			if bs := base[f]; bs != nil {
				baseKeys = len(*bs)
			}
			if baseKeys > defaultSnapshotDeltaCompactLargeBaseFieldKeys &&
				defaultSnapshotDeltaCompactLargeBaseMinDeltaDiv > 0 {
				minDeltaKeys := baseKeys / defaultSnapshotDeltaCompactLargeBaseMinDeltaDiv
				if minDeltaKeys < 1 {
					minDeltaKeys = 1
				}
				forcedByOps := defaultSnapshotDeltaCompactForceFieldOps > 0 && upperOps >= defaultSnapshotDeltaCompactForceFieldOps
				if upperKeys < minDeltaKeys && !forcedByOps {
					continue
				}
			}

			keys, ops, has := lookupLayerFieldDeltaStatsWithScratch(probeLayer, f, scratch)
			if !has || keys == 0 {
				if prevHad(f) {
					nextFields[f] = nil
				} else {
					delete(nextFields, f)
				}
				continue
			}

			byKeys := opt.SnapshotDeltaCompactFieldKeys > 0 && keys >= opt.SnapshotDeltaCompactFieldKeys
			byOps := opt.SnapshotDeltaCompactFieldOps > 0 && ops >= uint64(opt.SnapshotDeltaCompactFieldOps)
			if !byKeys && !byOps {
				continue
			}

			if baseKeys > defaultSnapshotDeltaCompactLargeBaseFieldKeys &&
				defaultSnapshotDeltaCompactLargeBaseMinDeltaDiv > 0 {
				minDeltaKeys := baseKeys / defaultSnapshotDeltaCompactLargeBaseMinDeltaDiv
				if minDeltaKeys < 1 {
					minDeltaKeys = 1
				}
				forcedByOps := defaultSnapshotDeltaCompactForceFieldOps > 0 && ops >= defaultSnapshotDeltaCompactForceFieldOps
				if keys < minDeltaKeys && !forcedByOps {
					continue
				}
			}

			candidates = append(candidates, deltaCompactFieldCandidate{
				field:   f,
				keys:    keys,
				ops:     ops,
				prevHad: prevHad(f),
			})
		}

		if len(candidates) > 0 {
			sortDeltaCompactCandidates(candidates)

			limit := len(candidates)
			if opt.SnapshotDeltaCompactMaxFieldsPerPublish > 0 && opt.SnapshotDeltaCompactMaxFieldsPerPublish < limit {
				limit = opt.SnapshotDeltaCompactMaxFieldsPerPublish
			}

			updates := make(map[string]*[]index, limit)
			updateScratch := getLayerFieldDeltaMergeScratch()
			defer releaseLayerFieldDeltaMergeScratch(updateScratch)
			for i := 0; i < limit; i++ {
				c := candidates[i]
				eff, _ := lookupLayerFieldDeltaBorrowedWithScratch(probeLayer, c.field, updateScratch)
				if eff == nil || !eff.hasEntries() {
					continue
				}
				ov := newFieldOverlay(base[c.field], eff)
				updates[c.field] = materializeFieldOverlay(ov)
				if c.prevHad {
					nextFields[c.field] = nil
				} else {
					delete(nextFields, c.field)
				}
			}
			base = copyIndexMapWithOverrides(base, updates)
		}
	}

	nextLayer := &fieldDeltaLayer{
		parent: prevLayer,
		fields: nextFields,
		depth:  1,
	}
	if prevLayer != nil {
		nextLayer.depth = prevLayer.depth + 1
	}
	if len(nextFields) == 0 {
		return base, prevLayer, prevCount
	}
	return base, nextLayer, prevCount + len(nextFields)
}

func mergeUniverseDelta(prevAdd, prevDrop, add, drop *roaring64.Bitmap) (*roaring64.Bitmap, *roaring64.Bitmap) {
	if (add == nil || add.IsEmpty()) && (drop == nil || drop.IsEmpty()) {
		return prevAdd, prevDrop
	}

	nextAdd := cloneIfNotEmpty(prevAdd)
	nextDrop := cloneIfNotEmpty(prevDrop)

	if add != nil && !add.IsEmpty() {
		if nextAdd == nil {
			nextAdd = cloneBitmap(add)
		} else {
			nextAdd.Or(add)
		}
	}
	if drop != nil && !drop.IsEmpty() {
		if nextDrop == nil {
			nextDrop = cloneBitmap(drop)
		} else {
			nextDrop.Or(drop)
		}
	}

	if nextAdd != nil && !nextAdd.IsEmpty() && nextDrop != nil && !nextDrop.IsEmpty() {
		common := getRoaringBuf()
		common.Or(nextAdd)
		common.And(nextDrop)
		if common != nil && !common.IsEmpty() {
			nextAdd.AndNot(common)
			nextDrop.AndNot(common)
		}
		releaseRoaringBuf(common)
	}

	return nonEmptyOrNil(nextAdd), nonEmptyOrNil(nextDrop)
}

func maybeCompactUniverseDelta(base, add, drop *roaring64.Bitmap, universeCompactOps uint64, force bool) (*roaring64.Bitmap, *roaring64.Bitmap, *roaring64.Bitmap) {
	var ops uint64
	if add != nil {
		ops += add.GetCardinality()
	}
	if drop != nil {
		ops += drop.GetCardinality()
	}
	if ops == 0 {
		return base, add, drop
	}
	if !force && (universeCompactOps == 0 || ops < universeCompactOps) {
		return base, add, drop
	}

	nextBase := roaring64.NewBitmap()
	if base != nil && !base.IsEmpty() {
		nextBase.Or(base)
	}
	if add != nil && !add.IsEmpty() {
		nextBase.Or(add)
	}
	if drop != nil && !drop.IsEmpty() {
		nextBase.AndNot(drop)
	}
	return nextBase, nil, nil
}

func materializeFieldOverlay(ov fieldOverlay) *[]index {
	br := ov.rangeForBounds(rangeBounds{has: true})
	if br.baseStart >= br.baseEnd && br.deltaStart >= br.deltaEnd {
		s := make([]index, 0)
		return &s
	}

	out := make([]index, 0, (br.baseEnd-br.baseStart)+(br.deltaEnd-br.deltaStart))
	cur := ov.newCursor(br, false)

	for {
		k, baseIDs, de, ok := cur.next()
		if !ok {
			break
		}
		if deltaEntryIsEmpty(de) {
			if baseIDs.IsEmpty() {
				continue
			}
			out = append(out, index{Key: k, IDs: baseIDs})
			continue
		}
		bm, owned := composePostingOwned(baseIDs, de, nil)
		if bm == nil || bm.IsEmpty() {
			if owned && bm != nil {
				releaseRoaringBuf(bm)
			}
			continue
		}
		if owned {
			out = append(out, index{Key: k, IDs: postingFromBitmapOwned(bm)})
			continue
		}
		out = append(out, index{Key: k, IDs: postingFromBitmapViewAdaptive(bm)})
	}
	return &out
}

func copyIndexMapWithOverrides(base map[string]*[]index, overrides map[string]*[]index) map[string]*[]index {
	if len(overrides) == 0 {
		return base
	}
	out := make(map[string]*[]index, len(base)+len(overrides))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range overrides {
		out[k] = v
	}
	return out
}

func (db *DB[K, V]) snapshotUniverseCardinality() uint64 {
	return db.getSnapshot().universeCardinality()
}

func (db *DB[K, V]) snapshotUniverseView() (*roaring64.Bitmap, bool) {
	s := db.getSnapshot()
	if (s.universeAdd == nil || s.universeAdd.IsEmpty()) && (s.universeRem == nil || s.universeRem.IsEmpty()) {
		if s.universe != nil {
			return s.universe, false
		}
		return getRoaringBuf(), true
	}

	out := getRoaringBuf()
	if s.universe != nil && !s.universe.IsEmpty() {
		out.Or(s.universe)
	}
	if s.universeAdd != nil && !s.universeAdd.IsEmpty() {
		out.Or(s.universeAdd)
	}
	if s.universeRem != nil && !s.universeRem.IsEmpty() {
		out.AndNot(s.universeRem)
	}
	return out, true
}

func cloneIfNotEmpty(bm *roaring64.Bitmap) *roaring64.Bitmap {
	if bm == nil || bm.IsEmpty() {
		return nil
	}
	return cloneBitmap(bm)
}

func (db *DB[K, V]) fieldOverlay(field string) fieldOverlay {
	s := db.getSnapshot()
	return newFieldOverlay(s.fieldIndexSlice(field), s.fieldDelta(field))
}

func (db *DB[K, V]) fieldLookupOwned(field, key string, scratch *roaring64.Bitmap) (*roaring64.Bitmap, bool) {
	return db.getSnapshot().fieldLookupOwned(field, key, scratch)
}

func (db *DB[K, V]) nilFieldOverlay(field string) fieldOverlay {
	s := db.getSnapshot()
	return newFieldOverlay(s.nilFieldIndexSlice(field), s.nilFieldDelta(field))
}

func (db *DB[K, V]) nilFieldLookupOwned(field string, scratch *roaring64.Bitmap) (*roaring64.Bitmap, bool) {
	return db.getSnapshot().nilFieldLookupOwned(field, scratch)
}

func (db *DB[K, V]) lenFieldOverlay(field string) fieldOverlay {
	s := db.getSnapshot()
	return newFieldOverlay(s.lenFieldIndexSlice(field), s.lenFieldDelta(field))
}

func (db *DB[K, V]) hasFieldIndex(field string) bool {
	s := db.getSnapshot()
	if s.fieldIndexSlice(field) != nil {
		return true
	}
	if s.fieldDelta(field) != nil {
		return true
	}
	if s.nilFieldIndexSlice(field) != nil {
		return true
	}
	if s.nilFieldDelta(field) != nil {
		return true
	}
	return false
}

func (db *DB[K, V]) hasLenFieldIndex(field string) bool {
	s := db.getSnapshot()
	if s.lenFieldIndexSlice(field) != nil {
		return true
	}
	if s.lenFieldDelta(field) != nil {
		return true
	}
	return false
}

func (db *DB[K, V]) snapshotHasAnyDelta() bool {
	return db.getSnapshot().hasAnyDeltaState()
}

func (s *indexSnapshot) hasAnyDeltaState() bool {
	if s == nil {
		return false
	}
	if s.indexDCount > 0 || s.nilDCount > 0 || s.lenDCount > 0 {
		return true
	}
	if len(s.indexDelta) > 0 || len(s.nilDelta) > 0 || len(s.lenIdxDelta) > 0 {
		return true
	}
	if s.universeAdd != nil && !s.universeAdd.IsEmpty() {
		return true
	}
	if s.universeRem != nil && !s.universeRem.IsEmpty() {
		return true
	}
	return false
}

func (db *DB[K, V]) registerSnapshot(s *indexSnapshot) {
	if s == nil {
		return
	}
	db.snapshot.mu.Lock()
	defer db.snapshot.mu.Unlock()

	ref, exists := db.snapshot.bySeq[s.seq]
	if !exists {
		db.snapshot.order = append(db.snapshot.order, s.seq)
		ref = getSnapshotRef()
		db.snapshot.bySeq[s.seq] = ref
	}
	ref.snap = s

	db.pruneSnapshotsLocked()
}

func (db *DB[K, V]) stageSnapshot(s *indexSnapshot) {
	if s == nil {
		return
	}
	db.snapshot.mu.Lock()
	defer db.snapshot.mu.Unlock()

	ref, exists := db.snapshot.bySeq[s.seq]
	if !exists {
		db.snapshot.order = append(db.snapshot.order, s.seq)
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
	ref.snap = nil
	if ref.snap == nil && ref.refs.Load() <= 0 {
		delete(db.snapshot.bySeq, seq)
		releaseSnapshotRef(ref)
	}
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

func (db *DB[K, V]) unpinSnapshotRef(ref *snapshotRef) {
	refs := ref.refs.Add(-1)
	// Pruning is intentionally not triggered from read-path unpin to avoid
	// O(snapshot-registry) work and lock contention on every Query call.
	if refs <= 0 && db.snapshot.compactPinsBlocked.Load() {
		db.noteSnapshotActivity()
	}
}

func (db *DB[K, V]) pruneSnapshotsLocked() {
	latest := db.snapshot.current.Load()
	latestSeq := uint64(0)
	if latest != nil {
		latestSeq = latest.seq
	}

	// If the order log grows much larger than the active registry, rebuild it
	// from active snapshot sequences. This prevents O(total-ever-seen-seq) scans
	// when an old pinned snapshot keeps snapHead at the front under mixed load.
	maxRegistry := int(db.options.SnapshotRegistryMax)
	if len(db.snapshot.order) > maxRegistry*4 {
		active := len(db.snapshot.bySeq)
		if active < 1 {
			active = 1
		}
		if len(db.snapshot.order) > active*4 {
			ids := make([]uint64, 0, len(db.snapshot.bySeq))
			for seq := range db.snapshot.bySeq {
				ids = append(ids, seq)
			}
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			db.snapshot.order = ids
			db.snapshot.head = 0
		}
	}

	if db.snapshot.head < 0 {
		db.snapshot.head = 0
	}
	if db.snapshot.head > len(db.snapshot.order) {
		db.snapshot.head = len(db.snapshot.order)
	}

	for len(db.snapshot.bySeq) > maxRegistry && db.snapshot.head < len(db.snapshot.order) {
		removedAny := false
		for i := db.snapshot.head; i < len(db.snapshot.order) && len(db.snapshot.bySeq) > maxRegistry; i++ {
			seq := db.snapshot.order[i]
			ref, ok := db.snapshot.bySeq[seq]
			if !ok {
				if i == db.snapshot.head {
					db.snapshot.head++
				}
				continue
			}
			if seq == latestSeq || ref.refs.Load() > 0 {
				// Do not stop pruning on first pinned snapshot: continue scanning
				// and reclaim later unpinned snapshots to keep map size bounded.
				continue
			}

			delete(db.snapshot.bySeq, seq)
			releaseSnapshotRef(ref)
			removedAny = true
			if i == db.snapshot.head {
				db.snapshot.head++
			}
		}
		if !removedAny {
			break
		}
	}

	for db.snapshot.head < len(db.snapshot.order) {
		seq := db.snapshot.order[db.snapshot.head]
		if _, ok := db.snapshot.bySeq[seq]; ok {
			break
		}
		db.snapshot.head++
	}

	// Compact consumed prefix occasionally to keep memory bounded.
	if db.snapshot.head > 0 && (db.snapshot.head >= 1024 || db.snapshot.head*2 >= len(db.snapshot.order)) {
		db.snapshot.order = append([]uint64(nil), db.snapshot.order[db.snapshot.head:]...)
		db.snapshot.head = 0
	}
}

// SnapshotStats returns current snapshot and compactor diagnostics.
//
// Runtime snapshot/compactor counters are collected only when
// Options.EnableSnapshotStats was enabled for this DB instance.
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
		Sequence:              s.seq,
		HasDelta:              s.hasAnyDeltaState(),
		IndexDeltaFields:      s.indexDeltaCount(),
		LenDeltaFields:        s.lenDeltaCount(),
		CompactorRequested:    db.snapshot.compactRequested.Load(),
		CompactorRuns:         db.snapshot.compactRuns.Load(),
		CompactorAttempts:     db.snapshot.compactAttempts.Load(),
		CompactorSucceeded:    db.snapshot.compactSucceeded.Load(),
		CompactorPreclaimBusy: db.snapshot.compactPreclaimBusy.Load(),
		CompactorLockMiss:     db.snapshot.compactLockMiss.Load(),
		CompactorNoChange:     db.snapshot.compactNoChange.Load(),
		CompactorSoftSkip:     db.snapshot.compactSoftSkip.Load(),
		CompactorSkippedWake:  db.snapshot.compactSkippedWake.Load(),
		CompactorIdleDefers:   db.snapshot.compactIdleDefers.Load(),
		CompactorStaleRetry:   db.snapshot.compactStaleRetry.Load(),
	}
	diag.IndexDeltaKeys, diag.IndexDeltaOps = s.getIndexLayer().effectiveDeltaTotals()
	diag.LenDeltaKeys, diag.LenDeltaOps = s.getLenLayer().effectiveDeltaTotals()
	if s.universe != nil {
		diag.UniverseBaseCard = s.universe.GetCardinality()
	}
	if s.indexLayer != nil {
		diag.IndexLayerDepth = s.indexLayer.depth
	}
	if s.lenLayer != nil {
		diag.LenLayerDepth = s.lenLayer.depth
	}
	if s.universeAdd != nil {
		diag.UniverseAddCard = s.universeAdd.GetCardinality()
	}
	if s.universeRem != nil {
		diag.UniverseRemCard = s.universeRem.GetCardinality()
	}
	if db.snapshot.compactReq != nil {
		diag.CompactorQueueLen = len(db.snapshot.compactReq)
	}

	db.snapshot.mu.RLock()
	diag.RegistrySize = len(db.snapshot.bySeq)
	diag.RegistryOrderLen = len(db.snapshot.order)
	diag.RegistryHead = db.snapshot.head
	for _, ref := range db.snapshot.bySeq {
		if ref.refs.Load() > 0 {
			diag.PinnedRefs++
		}
	}
	db.snapshot.mu.RUnlock()

	return diag
}

func (layer *fieldDeltaLayer) effectiveDeltaTotals() (keys int, ops uint64) {
	if layer == nil {
		return 0, 0
	}
	fields := layer.effectiveFieldNames()
	if len(fields) == 0 {
		return 0, 0
	}
	scratch := getLayerFieldDeltaMergeScratch()
	defer releaseLayerFieldDeltaMergeScratch(scratch)
	for _, field := range fields {
		k, o, has := lookupLayerFieldDeltaStatsWithScratch(layer, field, scratch)
		if !has || k <= 0 {
			continue
		}
		keys += k
		ops += o
	}
	return keys, ops
}
