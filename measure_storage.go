package rbi

import (
	"sort"
	"sync/atomic"

	"github.com/vapstack/rbi/internal/pooled"
)

const (
	measureChunkTargetRows = 256
	measureChunkThreshold  = measureChunkTargetRows * 2
)

type measureEntry struct {
	id    uint64
	value uint64
}

type measureBatchDelta struct {
	id    uint64
	newOK bool
	new   uint64
}

type measureEntryBufOrder struct {
	buf *pooled.SliceBuf[measureEntry]
}

func (s measureEntryBufOrder) Len() int { return s.buf.Len() }

func (s measureEntryBufOrder) Swap(i, j int) {
	a := s.buf.Get(i)
	s.buf.Set(i, s.buf.Get(j))
	s.buf.Set(j, a)
}

func (s measureEntryBufOrder) Less(i, j int) bool {
	return s.buf.Get(i).id < s.buf.Get(j).id
}

type measureBatchDeltaBufOrder struct {
	buf *pooled.SliceBuf[measureBatchDelta]
}

func (s measureBatchDeltaBufOrder) Len() int { return s.buf.Len() }

func (s measureBatchDeltaBufOrder) Swap(i, j int) {
	a := s.buf.Get(i)
	s.buf.Set(i, s.buf.Get(j))
	s.buf.Set(j, a)
}

func (s measureBatchDeltaBufOrder) Less(i, j int) bool {
	return s.buf.Get(i).id < s.buf.Get(j).id
}

type measureFlatRoot struct {
	refs   atomic.Int32
	ids    *pooled.SliceBuf[uint64]
	values *pooled.SliceBuf[uint64]
}

type measureChunk struct {
	refs   atomic.Int32
	ids    *pooled.SliceBuf[uint64]
	values *pooled.SliceBuf[uint64]
}

type measureChunkRef struct {
	lastID uint64
	chunk  *measureChunk
}

type measureChunkedRoot struct {
	refs     atomic.Int32
	refsByID *pooled.SliceBuf[measureChunkRef]
	rows     int
}

type measureFieldStorage struct {
	flat    *measureFlatRoot
	chunked *measureChunkedRoot
}

var measureEntrySlicePool = pooled.Slices[measureEntry]{Clear: true}
var measureBatchDeltaSlicePool = pooled.Slices[measureBatchDelta]{Clear: true}

var measureUint64SlicePool = pooled.Slices[uint64]{
	MinCap: measureChunkTargetRows,
	Clear:  true,
}

var measureChunkRefSlicePool = pooled.Slices[measureChunkRef]{
	MinCap: 8,
	Clear:  true,
}

var measureFieldStorageSlicePool = pooled.Slices[measureFieldStorage]{
	Clear: true,
}

var measureFlatRootPool = pooled.Pointers[measureFlatRoot]{Clear: true}
var measureChunkPool = pooled.Pointers[measureChunk]{Clear: true}
var measureChunkedRootPool = pooled.Pointers[measureChunkedRoot]{Clear: true}

func sortMeasureEntryBuf(buf *pooled.SliceBuf[measureEntry]) {
	if buf == nil || buf.Len() <= 1 {
		return
	}
	sort.Sort(measureEntryBufOrder{buf: buf})
}

func sortMeasureBatchDeltaBuf(buf *pooled.SliceBuf[measureBatchDelta]) {
	if buf == nil || buf.Len() <= 1 {
		return
	}
	sort.Sort(measureBatchDeltaBufOrder{buf: buf})
}

func releaseMeasureEntryBufs(bufs []*pooled.SliceBuf[measureEntry]) {
	for i := range bufs {
		if bufs[i] != nil {
			measureEntrySlicePool.Put(bufs[i])
			bufs[i] = nil
		}
	}
}

func cleanupBuildMeasureStates(buildOK *bool, states [][]*pooled.SliceBuf[measureEntry]) {
	if *buildOK {
		return
	}
	for i := range states {
		releaseMeasureEntryBufs(states[i])
	}
}

func newMeasureStorageFromEntriesOwned(entries *pooled.SliceBuf[measureEntry]) measureFieldStorage {
	if entries == nil {
		return measureFieldStorage{}
	}
	if entries.Len() == 0 {
		measureEntrySlicePool.Put(entries)
		return measureFieldStorage{}
	}
	sortMeasureEntryBuf(entries)
	if entries.Len() <= measureChunkThreshold {
		return newMeasureFlatStorageFromEntriesOwned(entries)
	}
	return newMeasureChunkedStorageFromEntriesOwned(entries)
}

func newMeasureFlatStorageFromEntriesOwned(entries *pooled.SliceBuf[measureEntry]) measureFieldStorage {
	root := measureFlatRootPool.Get()
	root.ids = measureUint64SlicePool.Get()
	root.values = measureUint64SlicePool.Get()
	root.ids.SetLen(entries.Len())
	root.values.SetLen(entries.Len())
	for i := 0; i < entries.Len(); i++ {
		entry := entries.Get(i)
		root.ids.Set(i, entry.id)
		root.values.Set(i, entry.value)
	}
	root.refs.Store(1)
	measureEntrySlicePool.Put(entries)
	return measureFieldStorage{flat: root}
}

func newMeasureChunkedStorageFromEntriesOwned(entries *pooled.SliceBuf[measureEntry]) measureFieldStorage {
	root := measureChunkedRootPool.Get()
	root.refsByID = measureChunkRefSlicePool.Get()

	for start := 0; start < entries.Len(); {
		size := min(measureChunkTargetRows, entries.Len()-start)
		appendMeasureEntryRangeAsChunk(root, entries, start, start+size)
		start += size
	}

	root.refs.Store(1)
	measureEntrySlicePool.Put(entries)
	return measureFieldStorage{chunked: root}
}

func appendMeasureEntryRangeAsChunk(root *measureChunkedRoot, entries *pooled.SliceBuf[measureEntry], start int, end int) {
	chunk := newMeasureChunkFromEntryRange(entries, start, end)
	appendMeasureChunkRef(root, chunk)
}

func newMeasureChunkFromEntryRange(entries *pooled.SliceBuf[measureEntry], start int, end int) *measureChunk {
	size := end - start
	chunk := measureChunkPool.Get()
	chunk.ids = measureUint64SlicePool.Get()
	chunk.values = measureUint64SlicePool.Get()
	chunk.ids.SetLen(size)
	chunk.values.SetLen(size)
	for i := 0; i < size; i++ {
		entry := entries.Get(start + i)
		chunk.ids.Set(i, entry.id)
		chunk.values.Set(i, entry.value)
	}
	chunk.refs.Store(1)
	return chunk
}

func appendMeasureChunkRef(root *measureChunkedRoot, chunk *measureChunk) {
	rows := chunk.ids.Len()
	root.refsByID.Append(measureChunkRef{
		lastID: chunk.ids.Get(rows - 1),
		chunk:  chunk,
	})
	root.rows += rows
}

func applyMeasureDeltasOwned(base measureFieldStorage, deltas *pooled.SliceBuf[measureBatchDelta]) measureFieldStorage {
	if deltas == nil {
		return base
	}
	if deltas.Len() == 0 {
		measureBatchDeltaSlicePool.Put(deltas)
		return base
	}
	sortMeasureBatchDeltaBuf(deltas)
	if base.flat != nil {
		return applyMeasureDeltasFlatOwned(base.flat, deltas)
	}
	if base.chunked != nil {
		return applyMeasureDeltasChunkedOwned(base.chunked, deltas)
	}
	entries := measureEntrySlicePool.Get()
	for i := 0; i < deltas.Len(); i++ {
		delta := deltas.Get(i)
		if delta.newOK {
			entries.Append(measureEntry{id: delta.id, value: delta.new})
		}
	}
	measureBatchDeltaSlicePool.Put(deltas)
	return newMeasureStorageFromEntriesOwned(entries)
}

func applyMeasureDeltasFlatOwned(base *measureFlatRoot, deltas *pooled.SliceBuf[measureBatchDelta]) measureFieldStorage {
	entries := measureEntrySlicePool.Get()
	i := 0
	j := 0
	for i < base.ids.Len() && j < deltas.Len() {
		id := base.ids.Get(i)
		delta := deltas.Get(j)
		switch {
		case id < delta.id:
			entries.Append(measureEntry{id: id, value: base.values.Get(i)})
			i++
		case id > delta.id:
			if delta.newOK {
				entries.Append(measureEntry{id: delta.id, value: delta.new})
			}
			j++
		default:
			if delta.newOK {
				entries.Append(measureEntry{id: id, value: delta.new})
			}
			i++
			j++
		}
	}
	for ; i < base.ids.Len(); i++ {
		entries.Append(measureEntry{id: base.ids.Get(i), value: base.values.Get(i)})
	}
	for ; j < deltas.Len(); j++ {
		delta := deltas.Get(j)
		if delta.newOK {
			entries.Append(measureEntry{id: delta.id, value: delta.new})
		}
	}
	measureBatchDeltaSlicePool.Put(deltas)
	return newMeasureStorageFromEntriesOwned(entries)
}

func applyMeasureDeltasChunkedOwned(base *measureChunkedRoot, deltas *pooled.SliceBuf[measureBatchDelta]) measureFieldStorage {
	root := measureChunkedRootPool.Get()
	root.refsByID = measureChunkRefSlicePool.Get()

	deltaPos := 0
	for chunkPos := 0; chunkPos < base.refsByID.Len(); chunkPos++ {
		ref := base.refsByID.Get(chunkPos)
		startDelta := deltaPos
		for deltaPos < deltas.Len() && deltas.Get(deltaPos).id <= ref.lastID {
			deltaPos++
		}
		if startDelta == deltaPos {
			ref.chunk.retain()
			appendMeasureChunkRef(root, ref.chunk)
			continue
		}
		entries := measureEntrySlicePool.Get()
		appendMeasureChunkMergedWithDeltas(entries, ref.chunk, deltas, startDelta, deltaPos)
		appendMeasureEntriesAsChunks(root, entries)
	}
	if deltaPos < deltas.Len() {
		fillMeasureTailChunkWithDeltas(root, deltas, &deltaPos)
	}
	if deltaPos < deltas.Len() {
		entries := measureEntrySlicePool.Get()
		for ; deltaPos < deltas.Len(); deltaPos++ {
			delta := deltas.Get(deltaPos)
			if delta.newOK {
				entries.Append(measureEntry{id: delta.id, value: delta.new})
			}
		}
		appendMeasureEntriesAsChunks(root, entries)
	}

	measureBatchDeltaSlicePool.Put(deltas)
	if root.rows == 0 {
		measureChunkRefSlicePool.Put(root.refsByID)
		root.refsByID = nil
		measureChunkedRootPool.Put(root)
		return measureFieldStorage{}
	}
	root.refs.Store(1)
	return measureFieldStorage{chunked: root}
}

func appendMeasureChunkMergedWithDeltas(
	entries *pooled.SliceBuf[measureEntry],
	chunk *measureChunk,
	deltas *pooled.SliceBuf[measureBatchDelta],
	startDelta int,
	endDelta int,
) {
	idPos := 0
	deltaPos := startDelta
	for idPos < chunk.ids.Len() && deltaPos < endDelta {
		id := chunk.ids.Get(idPos)
		delta := deltas.Get(deltaPos)
		switch {
		case id < delta.id:
			entries.Append(measureEntry{id: id, value: chunk.values.Get(idPos)})
			idPos++
		case id > delta.id:
			if delta.newOK {
				entries.Append(measureEntry{id: delta.id, value: delta.new})
			}
			deltaPos++
		default:
			if delta.newOK {
				entries.Append(measureEntry{id: id, value: delta.new})
			}
			idPos++
			deltaPos++
		}
	}
	for ; idPos < chunk.ids.Len(); idPos++ {
		entries.Append(measureEntry{id: chunk.ids.Get(idPos), value: chunk.values.Get(idPos)})
	}
	for ; deltaPos < endDelta; deltaPos++ {
		delta := deltas.Get(deltaPos)
		if delta.newOK {
			entries.Append(measureEntry{id: delta.id, value: delta.new})
		}
	}
}

func fillMeasureTailChunkWithDeltas(root *measureChunkedRoot, deltas *pooled.SliceBuf[measureBatchDelta], deltaPos *int) {
	if root.refsByID.Len() == 0 {
		return
	}
	tailPos := root.refsByID.Len() - 1
	tail := root.refsByID.Get(tailPos).chunk
	tailLen := tail.ids.Len()
	if tailLen >= measureChunkTargetRows {
		return
	}

	entries := measureEntrySlicePool.Get()
	for i := 0; i < tailLen; i++ {
		entries.Append(measureEntry{id: tail.ids.Get(i), value: tail.values.Get(i)})
	}
	pos := *deltaPos
	for pos < deltas.Len() && entries.Len() < measureChunkTargetRows {
		delta := deltas.Get(pos)
		if delta.newOK {
			entries.Append(measureEntry{id: delta.id, value: delta.new})
		}
		pos++
	}
	*deltaPos = pos
	if entries.Len() == tailLen {
		measureEntrySlicePool.Put(entries)
		return
	}

	chunk := newMeasureChunkFromEntryRange(entries, 0, entries.Len())
	root.refsByID.Set(tailPos, measureChunkRef{
		lastID: chunk.ids.Get(chunk.ids.Len() - 1),
		chunk:  chunk,
	})
	root.rows += chunk.ids.Len() - tailLen
	tail.release()
	measureEntrySlicePool.Put(entries)
}

func appendMeasureEntriesAsChunks(root *measureChunkedRoot, entries *pooled.SliceBuf[measureEntry]) {
	for start := 0; start < entries.Len(); {
		size := min(measureChunkTargetRows, entries.Len()-start)
		appendMeasureEntryRangeAsChunk(root, entries, start, start+size)
		start += size
	}
	measureEntrySlicePool.Put(entries)
}

func (s measureFieldStorage) retain() {
	if s.flat != nil {
		s.flat.retain()
		return
	}
	if s.chunked != nil {
		s.chunked.retain()
	}
}

func (r *measureFlatRoot) retain() {
	r.refs.Add(1)
}

func (r *measureChunkedRoot) retain() {
	r.refs.Add(1)
}

func releaseMeasureFieldStorageOwned(s measureFieldStorage) {
	if s.flat != nil {
		s.flat.release()
		return
	}
	if s.chunked != nil {
		s.chunked.release()
	}
}

func (r *measureFlatRoot) release() {
	if r == nil || r.refs.Add(-1) != 0 {
		return
	}
	measureUint64SlicePool.Put(r.ids)
	measureUint64SlicePool.Put(r.values)
	r.ids = nil
	r.values = nil
	measureFlatRootPool.Put(r)
}

func (c *measureChunk) retain() {
	c.refs.Add(1)
}

func (c *measureChunk) release() {
	if c == nil || c.refs.Add(-1) != 0 {
		return
	}
	measureUint64SlicePool.Put(c.ids)
	measureUint64SlicePool.Put(c.values)
	c.ids = nil
	c.values = nil
	measureChunkPool.Put(c)
}

func (r *measureChunkedRoot) release() {
	if r == nil || r.refs.Add(-1) != 0 {
		return
	}
	if r.refsByID != nil {
		for i := 0; i < r.refsByID.Len(); i++ {
			r.refsByID.Get(i).chunk.release()
		}
		measureChunkRefSlicePool.Put(r.refsByID)
		r.refsByID = nil
	}
	r.rows = 0
	measureChunkedRootPool.Put(r)
}

func cloneMeasureFieldStorageSlots(src *pooled.SliceBuf[measureFieldStorage], size int) *pooled.SliceBuf[measureFieldStorage] {
	out := measureFieldStorageSlicePool.Get()
	out.SetLen(size)
	if src == nil {
		return out
	}
	limit := min(size, src.Len())
	for i := 0; i < limit; i++ {
		out.Set(i, src.Get(i))
	}
	return out
}

func retainSharedMeasureFieldStorageSlots(next, prev *pooled.SliceBuf[measureFieldStorage]) {
	if next == nil || prev == nil {
		return
	}
	limit := min(next.Len(), prev.Len())
	for i := 0; i < limit; i++ {
		storage := next.Get(i)
		if storage == prev.Get(i) {
			storage.retain()
		}
	}
}

func releaseMeasureFieldStorageSlotsOwned(slots *pooled.SliceBuf[measureFieldStorage]) {
	if slots == nil {
		return
	}
	for i := 0; i < slots.Len(); i++ {
		releaseMeasureFieldStorageOwned(slots.Get(i))
	}
	measureFieldStorageSlicePool.Put(slots)
}

func releaseMeasureFieldStorageMapOwned(m map[string]measureFieldStorage) {
	for _, storage := range m {
		releaseMeasureFieldStorageOwned(storage)
	}
}

func (s measureFieldStorage) lookup(id uint64) (uint64, bool) {
	if s.flat != nil {
		return s.flat.lookup(id)
	}
	if s.chunked != nil {
		return s.chunked.lookup(id)
	}
	return 0, false
}

func (s measureFieldStorage) rows() int {
	if s.flat != nil {
		return s.flat.ids.Len()
	}
	if s.chunked != nil {
		return s.chunked.rows
	}
	return 0
}

func (r *measureFlatRoot) lookup(id uint64) (uint64, bool) {
	pos := sort.Search(r.ids.Len(), func(i int) bool {
		return r.ids.Get(i) >= id
	})
	if pos < r.ids.Len() && r.ids.Get(pos) == id {
		return r.values.Get(pos), true
	}
	return 0, false
}

func (r *measureChunkedRoot) lookup(id uint64) (uint64, bool) {
	chunkPos := sort.Search(r.refsByID.Len(), func(i int) bool {
		return r.refsByID.Get(i).lastID >= id
	})
	if chunkPos >= r.refsByID.Len() {
		return 0, false
	}
	chunk := r.refsByID.Get(chunkPos).chunk
	pos := sort.Search(chunk.ids.Len(), func(i int) bool {
		return chunk.ids.Get(i) >= id
	})
	if pos < chunk.ids.Len() && chunk.ids.Get(pos) == id {
		return chunk.values.Get(pos), true
	}
	return 0, false
}
