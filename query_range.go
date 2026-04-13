package rbi

import (
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

const numericRangeFullSpanCacheMaxEntries = 4

type numericRangeBucketCache struct {
	mu    sync.Mutex
	slots *pooled.SliceBuf[numericRangeBucketCacheSlot]
}

type numericRangeBucketIndex struct {
	bucketSize int
	keyCount   int
}

type numericRangeBucketCacheSlot struct {
	field string
	entry *numericRangeBucketCacheEntry
}

type numericRangeFullSpanCacheSlot struct {
	key   uint64
	ids   posting.List
	stamp uint64
	used  bool
}

type numericRangeBucketCacheEntry struct {
	refs          atomic.Int32
	storage       fieldIndexStorage
	idx           numericRangeBucketIndex
	maxCard       uint64
	mu            sync.Mutex
	fullSpanClock uint64
	fullSpanCache [numericRangeFullSpanCacheMaxEntries]numericRangeFullSpanCacheSlot
	retired       *pooled.SliceBuf[posting.List]
}

var numericRangeBucketCachePool = pooled.Pointers[numericRangeBucketCache]{
	Cleanup: func(c *numericRangeBucketCache) {
		c.release()
	},
}

var numericRangeBucketCacheSlotPool = pooled.Slices[numericRangeBucketCacheSlot]{
	Clear: true,
}

var numericRangeBucketCacheEntryPool = pooled.Pointers[numericRangeBucketCacheEntry]{
	Cleanup: func(e *numericRangeBucketCacheEntry) {
		e.releaseFullSpanCache()
	},
	Clear: true,
}

var numericRangeRetiredPostingPool = pooled.Slices[posting.List]{
	Cleanup: func(buf *pooled.SliceBuf[posting.List]) {
		for i := 0; i < buf.Len(); i++ {
			ids := buf.Get(i)
			if !ids.IsEmpty() {
				ids.Release()
			}
		}
	},
	Clear: true,
}

func numericRangeFullSpanCacheKey(start, end int) uint64 {
	return uint64(uint32(start))<<32 | uint64(uint32(end))
}

func numericRangeFullSpanCacheBounds(key uint64) (int, int) {
	return int(uint32(key >> 32)), int(uint32(key))
}

func (c *numericRangeBucketCache) init(fieldCount int) {
	if c.slots == nil {
		c.slots = numericRangeBucketCacheSlotPool.Get()
	}
	c.slots.SetLen(fieldCount)
}

func (c *numericRangeBucketCache) clearEntries() {
	if c == nil || c.slots == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < c.slots.Len(); i++ {
		slot := c.slots.Get(i)
		if slot.entry != nil {
			slot.entry.release()
			slot.entry = nil
		}
		slot.field = ""
		c.slots.Set(i, slot)
	}
}

func (c *numericRangeBucketCache) release() {
	if c == nil || c.slots == nil {
		return
	}
	c.clearEntries()
	numericRangeBucketCacheSlotPool.Put(c.slots)
	c.slots = nil
}

func (c *numericRangeBucketCache) loadSlot(field string, ordinal int) (*numericRangeBucketCacheEntry, bool) {
	if c == nil || c.slots == nil || ordinal < 0 || ordinal >= c.slots.Len() {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	slot := c.slots.Get(ordinal)
	if slot.entry == nil || slot.field != field {
		return nil, false
	}
	return slot.entry, true
}

func (c *numericRangeBucketCache) storeSlot(field string, ordinal int, entry *numericRangeBucketCacheEntry) {
	if c == nil || c.slots == nil || ordinal < 0 || ordinal >= c.slots.Len() {
		return
	}
	c.mu.Lock()
	slot := c.slots.Get(ordinal)
	slot.field = field
	slot.entry = entry
	c.slots.Set(ordinal, slot)
	c.mu.Unlock()
}

func (c *numericRangeBucketCache) loadField(field string) (*numericRangeBucketCacheEntry, bool) {
	if c == nil || c.slots == nil || field == "" {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < c.slots.Len(); i++ {
		slot := c.slots.Get(i)
		if slot.field == field && slot.entry != nil {
			return slot.entry, true
		}
	}
	return nil, false
}

func (c *numericRangeBucketCache) entryCount() int {
	if c == nil || c.slots == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for i := 0; i < c.slots.Len(); i++ {
		if c.slots.Get(i).entry != nil {
			count++
		}
	}
	return count
}

func (e *numericRangeBucketCacheEntry) retain() {
	e.refs.Add(1)
}

func (e *numericRangeBucketCacheEntry) release() {
	if e != nil && e.refs.Add(-1) == 0 {
		numericRangeBucketCacheEntryPool.Put(e)
	}
}

func (e *numericRangeBucketCacheEntry) releaseFullSpanCache() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i := range e.fullSpanCache {
		if e.fullSpanCache[i].used && !e.fullSpanCache[i].ids.IsEmpty() {
			e.fullSpanCache[i].ids.Release()
		}
		e.fullSpanCache[i] = numericRangeFullSpanCacheSlot{}
	}
	if e.retired != nil {
		numericRangeRetiredPostingPool.Put(e.retired)
		e.retired = nil
	}
	e.fullSpanClock = 0
}

func (e *numericRangeBucketCacheEntry) retirePosting(ids posting.List) {
	if ids.IsEmpty() {
		return
	}
	if e.retired == nil {
		e.retired = numericRangeRetiredPostingPool.Get()
	}
	e.retired.Append(ids)
}

func (e *numericRangeBucketCacheEntry) loadFullSpan(start, end int) (posting.List, bool) {
	if e == nil {
		return posting.List{}, false
	}
	key := numericRangeFullSpanCacheKey(start, end)
	e.mu.Lock()
	defer e.mu.Unlock()
	for i := range e.fullSpanCache {
		slot := &e.fullSpanCache[i]
		if !slot.used || slot.key != key {
			continue
		}
		if slot.ids.IsEmpty() {
			return posting.List{}, false
		}
		e.fullSpanClock++
		slot.stamp = e.fullSpanClock
		return slot.ids.Borrow(), true
	}
	return posting.List{}, false
}

func (e *numericRangeBucketCacheEntry) loadExtendedFullSpan(start, end int) (posting.List, int, int, bool) {
	if e == nil {
		return posting.List{}, 0, 0, false
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	bestIdx := -1
	bestBuckets := 0
	bestStart := 0
	bestEnd := 0
	for i := range e.fullSpanCache {
		slot := &e.fullSpanCache[i]
		if !slot.used || slot.ids.IsEmpty() {
			continue
		}
		slotStart, slotEnd := numericRangeFullSpanCacheBounds(slot.key)
		switch {
		case slotStart == start && slotEnd < end:
			buckets := slotEnd - slotStart + 1
			if bestIdx < 0 || buckets > bestBuckets {
				bestIdx = i
				bestBuckets = buckets
				bestStart = slotStart
				bestEnd = slotEnd
			}
		case slotEnd == end && slotStart > start:
			buckets := slotEnd - slotStart + 1
			if bestIdx < 0 || buckets > bestBuckets {
				bestIdx = i
				bestBuckets = buckets
				bestStart = slotStart
				bestEnd = slotEnd
			}
		}
	}
	if bestIdx < 0 {
		return posting.List{}, 0, 0, false
	}
	e.fullSpanClock++
	e.fullSpanCache[bestIdx].stamp = e.fullSpanClock
	return e.fullSpanCache[bestIdx].ids.Borrow(), bestStart, bestEnd, true
}

func (e *numericRangeBucketCacheEntry) tryStoreFullSpan(start, end int, ids posting.List) (posting.List, bool) {
	if e == nil || ids.IsEmpty() {
		return ids, false
	}
	key := numericRangeFullSpanCacheKey(start, end)
	if e.maxCard > 0 && ids.Cardinality() > e.maxCard {
		return ids, false
	}
	e.mu.Lock()
	for i := range e.fullSpanCache {
		slot := &e.fullSpanCache[i]
		if !slot.used || slot.key != key {
			continue
		}
		e.fullSpanClock++
		slot.stamp = e.fullSpanClock
		cached := slot.ids
		e.mu.Unlock()
		ids.Release()
		return cached.Borrow(), true
	}
	e.mu.Unlock()
	stored := ids
	if stored.IsBorrowed() {
		stored = stored.Clone()
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for i := range e.fullSpanCache {
		slot := &e.fullSpanCache[i]
		if !slot.used || slot.key != key {
			continue
		}
		e.fullSpanClock++
		slot.stamp = e.fullSpanClock
		cached := slot.ids
		if !stored.SharesPayload(ids) {
			stored.Release()
		}
		ids.Release()
		return cached.Borrow(), true
	}
	slotIdx := -1
	oldestStamp := ^uint64(0)
	for i := range e.fullSpanCache {
		slot := &e.fullSpanCache[i]
		if !slot.used {
			slotIdx = i
			break
		}
		if slot.stamp <= oldestStamp {
			oldestStamp = slot.stamp
			slotIdx = i
		}
	}
	e.fullSpanClock++
	if replaced := e.fullSpanCache[slotIdx]; replaced.used && !replaced.ids.IsEmpty() {
		e.retirePosting(replaced.ids)
	}
	e.fullSpanCache[slotIdx] = numericRangeFullSpanCacheSlot{
		key:   key,
		ids:   stored,
		stamp: e.fullSpanClock,
		used:  true,
	}
	return stored.Borrow(), true
}

func (idx *numericRangeBucketIndex) fullBucketSpan(br overlayRange) (start, end int, ok bool) {
	if idx == nil || idx.bucketSize <= 0 || idx.keyCount <= 0 {
		return 0, 0, false
	}
	if br.baseStart < 0 || br.baseStart >= br.baseEnd {
		return 0, 0, false
	}

	startBucket := br.baseStart / idx.bucketSize
	endBucket := (br.baseEnd - 1) / idx.bucketSize
	if startBucket < 0 {
		startBucket = 0
	}
	lastBucket := idx.bucketCount() - 1
	if lastBucket < 0 {
		return 0, 0, false
	}
	if endBucket > lastBucket {
		endBucket = lastBucket
	}
	if startBucket > endBucket {
		return 0, 0, false
	}

	start = startBucket
	if idx.bucketStart(start) < br.baseStart {
		start++
	}
	end = endBucket
	if end >= 0 && idx.bucketEnd(end) > br.baseEnd {
		end--
	}
	if start > end {
		return 0, 0, false
	}
	return start, end, true
}

func (idx *numericRangeBucketIndex) bucketCount() int {
	if idx == nil || idx.bucketSize <= 0 || idx.keyCount <= 0 {
		return 0
	}
	return (idx.keyCount + idx.bucketSize - 1) / idx.bucketSize
}

func (idx *numericRangeBucketIndex) bucketStart(bucket int) int {
	return bucket * idx.bucketSize
}

func (idx *numericRangeBucketIndex) bucketEnd(bucket int) int {
	end := (bucket + 1) * idx.bucketSize
	if end > idx.keyCount {
		return idx.keyCount
	}
	return end
}

func satAddUint64(total, add uint64) uint64 {
	if ^uint64(0)-total < add {
		return ^uint64(0)
	}
	return total + add
}

func overlayRangeSpanLen(br overlayRange) int {
	if br.baseStart >= br.baseEnd {
		return 0
	}
	return br.baseEnd - br.baseStart
}

func appendOverlayRangeUnion(builder *postingUnionBuilder, ov fieldOverlay, br overlayRange) {
	if builder == nil || overlayRangeEmpty(br) {
		return
	}
	cur := ov.newCursor(br, false)
	for {
		_, ids, ok := cur.next()
		if !ok {
			break
		}
		if ids.IsEmpty() {
			continue
		}
		builder.addPosting(ids)
	}
}

func overlayUnionBatchSinglesEnabled(ov fieldOverlay, first, second overlayRange) bool {
	totalSpan := overlayRangeSpanLen(first) + overlayRangeSpanLen(second)
	if totalSpan == 0 {
		return false
	}
	if totalSpan <= singleAdaptiveMaxLen {
		return true
	}
	if ov.chunked == nil {
		return false
	}
	_, estFirst := overlayRangeStats(ov, first)
	_, estSecond := overlayRangeStats(ov, second)
	return postingBatchSinglesEnabled(satAddUint64(estFirst, estSecond))
}

func overlayUnionRanges(ov fieldOverlay, first, second overlayRange) posting.List {
	if overlayRangeEmpty(first) && overlayRangeEmpty(second) {
		return posting.List{}
	}
	builder := newPostingUnionBuilder(overlayUnionBatchSinglesEnabled(ov, first, second))
	defer builder.release()
	appendOverlayRangeUnion(&builder, ov, first)
	appendOverlayRangeUnion(&builder, ov, second)
	return builder.finish(true)
}

func mergeOverlayRangesInto(dst posting.List, ov fieldOverlay, first, second overlayRange) posting.List {
	totalSpan := overlayRangeSpanLen(first) + overlayRangeSpanLen(second)
	if totalSpan == 0 {
		return dst
	}
	const mergeOverlayRangeDirectMaxBuckets = 32
	builder := newPostingUnionBuilder(totalSpan <= singleAdaptiveMaxLen)
	if !dst.IsEmpty() && totalSpan > mergeOverlayRangeDirectMaxBuckets {
		appendOverlayRangeUnion(&builder, ov, first)
		appendOverlayRangeUnion(&builder, ov, second)
		return dst.BuildMergedOwned(builder.finish(false))
	}
	builder.ids = dst
	appendOverlayRangeUnion(&builder, ov, first)
	appendOverlayRangeUnion(&builder, ov, second)
	return builder.finish(false)
}

func mergeOverlayRangeInto(dst posting.List, ov fieldOverlay, br overlayRange) posting.List {
	return mergeOverlayRangesInto(dst, ov, br, overlayRange{})
}

func isNumericScalarKind(kind reflect.Kind) bool {
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

func buildNumericRangeBucketIndex(base []index, bucketSize, minFieldKeys int) (numericRangeBucketIndex, bool) {
	if bucketSize <= 0 || minFieldKeys <= 0 {
		return numericRangeBucketIndex{}, false
	}
	if len(base) < minFieldKeys {
		return numericRangeBucketIndex{}, false
	}
	if bucketSize < 16 {
		bucketSize = 16
	}

	return numericRangeBucketIndex{
		bucketSize: bucketSize,
		keyCount:   len(base),
	}, true
}

func buildNumericRangeBucketIndexOverlay(ov fieldOverlay, bucketSize, minFieldKeys int) (numericRangeBucketIndex, bool) {
	if !ov.hasData() {
		return numericRangeBucketIndex{}, false
	}
	if ov.chunked == nil {
		return buildNumericRangeBucketIndex(ov.base, bucketSize, minFieldKeys)
	}
	if bucketSize <= 0 || minFieldKeys <= 0 {
		return numericRangeBucketIndex{}, false
	}
	if ov.keyCount() < minFieldKeys {
		return numericRangeBucketIndex{}, false
	}
	if bucketSize < 16 {
		bucketSize = 16
	}

	keyCount := ov.keyCount()
	return numericRangeBucketIndex{
		bucketSize: bucketSize,
		keyCount:   keyCount,
	}, true
}

func (s *indexSnapshot) getNumericRangeBucketCacheEntry(field string, ordinal int, storage fieldIndexStorage, bucketSize, minFieldKeys int) *numericRangeBucketCacheEntry {
	if s == nil || storage.keyCount() == 0 {
		return nil
	}
	ov := newFieldOverlayStorage(storage)
	cache := s.numericRangeBucketCache
	if cache == nil {
		return nil
	}

	if entry, ok := cache.loadSlot(field, ordinal); ok && entry.storage == storage {
		return entry
	}

	idx, _ := buildNumericRangeBucketIndexOverlay(ov, bucketSize, minFieldKeys)
	entry := numericRangeBucketCacheEntryPool.Get()
	entry.refs.Store(1)
	entry.storage = storage
	entry.idx = idx
	entry.maxCard = s.matPredCacheMaxCard
	if cached, ok := cache.loadSlot(field, ordinal); ok && cached.storage == storage {
		entry.release()
		return cached
	}
	cache.storeSlot(field, ordinal, entry)
	return entry
}

func (qv *queryView[K, V]) numericRangeFieldOrdinal(field string) (int, bool) {
	acc, ok := qv.root.indexedFieldByName[field]
	if !ok {
		return 0, false
	}
	return acc.ordinal, true
}

func (qv *queryView[K, V]) numericRangeBucketCacheEntry(field string, storage fieldIndexStorage, bucketSize, minFieldKeys int) *numericRangeBucketCacheEntry {
	ordinal, ok := qv.numericRangeFieldOrdinal(field)
	if !ok {
		return nil
	}
	return qv.snap.getNumericRangeBucketCacheEntry(field, ordinal, storage, bucketSize, minFieldKeys)
}

func (qv *queryView[K, V]) tryEvalNumericRangeBuckets(field string, fm *field, ov fieldOverlay, br overlayRange) (postingResult, bool) {
	if fm == nil || fm.Slice || !isNumericScalarKind(fm.Kind) {
		return postingResult{}, false
	}

	bucketSize := qv.options.NumericRangeBucketSize
	minFieldKeys := qv.options.NumericRangeBucketMinFieldKeys
	minSpan := qv.options.NumericRangeBucketMinSpanKeys
	if bucketSize <= 0 || minFieldKeys <= 0 || minSpan <= 0 {
		return postingResult{}, false
	}
	span := br.baseEnd - br.baseStart
	if span < minSpan {
		return postingResult{}, false
	}
	if br.baseStart >= br.baseEnd || ov.keyCount() == 0 {
		return postingResult{}, false
	}

	storage, ok := qv.snap.fieldIndexStorage(field)
	if !ok || storage.keyCount() == 0 {
		return postingResult{}, false
	}
	entry := qv.numericRangeBucketCacheEntry(field, storage, bucketSize, minFieldKeys)
	if entry == nil {
		return postingResult{}, false
	}
	idx := &entry.idx
	if idx.bucketSize <= 0 || idx.bucketCount() == 0 {
		return postingResult{}, false
	}
	if idx.keyCount != ov.keyCount() {
		return postingResult{}, false
	}

	startFull, endFull, ok := idx.fullBucketSpan(br)
	if !ok {
		return postingResult{}, false
	}

	fullSpanReuse := newMaterializedPredReadOnlyReuse(
		qv.snap,
		materializedPredKeyForNumericBucketSpan(field, startFull, endFull),
	)

	var res posting.List
	if cached, ok := entry.loadFullSpan(startFull, endFull); ok {
		res = cached
	}
	if res.IsEmpty() {
		if cached, ok := fullSpanReuse.load(); ok && !cached.IsEmpty() {
			res = cached
		}
	}
	if res.IsEmpty() {
		if cached, cachedStart, cachedEnd, ok := entry.loadExtendedFullSpan(startFull, endFull); ok {
			res = cached
			switch {
			case cachedStart == startFull && cachedEnd < endFull:
				res = mergeOverlayRangeInto(
					res,
					ov,
					ov.rangeByRanks(idx.bucketStart(cachedEnd+1), idx.bucketEnd(endFull)),
				)
			case cachedEnd == endFull && cachedStart > startFull:
				res = mergeOverlayRangeInto(
					res,
					ov,
					ov.rangeByRanks(idx.bucketStart(startFull), idx.bucketStart(cachedStart)),
				)
			}
		} else {
			res = mergeOverlayRangeInto(res, ov, ov.rangeByRanks(idx.bucketStart(startFull), idx.bucketEnd(endFull)))
		}
		res, _ = entry.tryStoreFullSpan(startFull, endFull, res)
	}

	leftEnd := min(br.baseEnd, idx.bucketStart(startFull))
	rightStart := max(br.baseStart, idx.bucketEnd(endFull))
	if (leftEnd > br.baseStart) || (rightStart < br.baseEnd) {
		res = mergeOverlayRangesInto(
			res,
			ov,
			ov.rangeByRanks(br.baseStart, leftEnd),
			ov.rangeByRanks(rightStart, br.baseEnd),
		)
	}

	return postingResult{ids: res}, true
}

func (qv *queryView[K, V]) tryLoadNumericRangeBuckets(field string, fm *field, ov fieldOverlay, br overlayRange) (postingResult, bool) {
	if fm == nil || fm.Slice || !isNumericScalarKind(fm.Kind) {
		return postingResult{}, false
	}

	bucketSize := qv.options.NumericRangeBucketSize
	minFieldKeys := qv.options.NumericRangeBucketMinFieldKeys
	minSpan := qv.options.NumericRangeBucketMinSpanKeys
	if bucketSize <= 0 || minFieldKeys <= 0 || minSpan <= 0 {
		return postingResult{}, false
	}
	span := br.baseEnd - br.baseStart
	if span < minSpan {
		return postingResult{}, false
	}
	if br.baseStart >= br.baseEnd || ov.keyCount() == 0 {
		return postingResult{}, false
	}

	storage, ok := qv.snap.fieldIndexStorage(field)
	if !ok || storage.keyCount() == 0 {
		return postingResult{}, false
	}
	entry := qv.numericRangeBucketCacheEntry(field, storage, bucketSize, minFieldKeys)
	if entry == nil {
		return postingResult{}, false
	}
	idx := &entry.idx
	if idx.bucketSize <= 0 || idx.bucketCount() == 0 {
		return postingResult{}, false
	}
	if idx.keyCount != ov.keyCount() {
		return postingResult{}, false
	}

	startFull, endFull, ok := idx.fullBucketSpan(br)
	if !ok {
		return postingResult{}, false
	}

	fullSpanReuse := newMaterializedPredReadOnlyReuse(
		qv.snap,
		materializedPredKeyForNumericBucketSpan(field, startFull, endFull),
	)

	var res posting.List
	if cached, ok := entry.loadFullSpan(startFull, endFull); ok {
		res = cached
	}
	if res.IsEmpty() {
		if cached, ok := fullSpanReuse.load(); ok && !cached.IsEmpty() {
			res = cached
		}
	}
	if res.IsEmpty() {
		return postingResult{}, false
	}

	leftEnd := min(br.baseEnd, idx.bucketStart(startFull))
	rightStart := max(br.baseStart, idx.bucketEnd(endFull))
	if (leftEnd > br.baseStart) || (rightStart < br.baseEnd) {
		res = mergeOverlayRangesInto(
			res,
			ov,
			ov.rangeByRanks(br.baseStart, leftEnd),
			ov.rangeByRanks(rightStart, br.baseEnd),
		)
	}

	return postingResult{ids: res}, true
}

func (qv *queryView[K, V]) tryCountSnapshotNumericRange(field string, fm *field, ov fieldOverlay, start, end int) (uint64, bool) {
	if fm == nil || fm.Slice || !isNumericScalarKind(fm.Kind) {
		return 0, false
	}
	if start < 0 || start > end || end > ov.keyCount() {
		return 0, false
	}

	storage, ok := qv.snap.fieldIndexStorage(field)
	if !ok || storage.keyCount() == 0 {
		return 0, false
	}
	if storage.keyCount() != ov.keyCount() {
		return 0, false
	}
	if storage.chunked != nil {
		root := storage.chunked
		return root.rangeRows(root.posForRank(start), root.posForRank(end)), true
	}
	flat := storage.flatSlice()
	if flat == nil {
		return 0, false
	}
	return countBaseIndexRangeCardinality(*flat, start, end), true
}
