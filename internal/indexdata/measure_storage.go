package indexdata

import (
	"cmp"
	"math/bits"
	"slices"
	"sync/atomic"

	"github.com/vapstack/rbi/internal/binsearch"
	"github.com/vapstack/rbi/internal/pooled"
)

const (
	MeasureChunkTargetRows = 256
	MeasureChunkThreshold  = MeasureChunkTargetRows * 2
)

type MeasureEntry struct {
	ID    uint64
	Value uint64
}

type MeasureDelta struct {
	ID    uint64
	NewOK bool
	New   uint64
}

type MeasureDeltaBatch struct {
	fields  [][]MeasureDelta
	touched []int
}

type measureFlatRoot struct {
	refs   atomic.Int32
	ids    []uint64
	values []uint64
}

type measureChunk struct {
	refs   atomic.Int32
	ids    []uint64
	values []uint64
}

type measureChunkRef struct {
	LastID uint64
	chunk  *measureChunk
}

type measureChunkedRoot struct {
	refs     atomic.Int32
	refsByID []measureChunkRef
	rows     int
}

type MeasureStorage struct {
	flat    *measureFlatRoot
	chunked *measureChunkedRoot
}

func sortMeasureEntryBuf(buf []MeasureEntry) {
	if len(buf) <= 1 {
		return
	}
	slices.SortFunc(buf, func(a, b MeasureEntry) int {
		return cmp.Compare(a.ID, b.ID)
	})
}

func sortMeasureBatchDeltaBuf(buf []MeasureDelta) {
	if len(buf) <= 1 {
		return
	}
	slices.SortFunc(buf, func(a, b MeasureDelta) int {
		return cmp.Compare(a.ID, b.ID)
	})
}

func NewMeasureDeltaBatch(fieldCount int) MeasureDeltaBatch {
	return MeasureDeltaBatch{
		fields:  measureDeltaSlotSlicePool.Get(fieldCount)[:fieldCount],
		touched: pooled.GetIntSlice(fieldCount),
	}
}

func (deltas *MeasureDeltaBatch) Append(ordinal int, ID uint64, newOK bool, value uint64) {
	buf := deltas.fields[ordinal]
	if buf == nil {
		buf = GetMeasureDeltaSlice(0)
		deltas.touched = append(deltas.touched, ordinal)
	}
	buf = append(buf, MeasureDelta{
		ID:    ID,
		NewOK: newOK,
		New:   value,
	})
	deltas.fields[ordinal] = buf
}

func (deltas *MeasureDeltaBatch) ApplyToMeasureStorageSlotsOwned(slots []MeasureStorage) {
	for i := range deltas.touched {
		ordinal := deltas.touched[i]
		base := slots[ordinal]
		storage := base.ApplyDeltasOwned(deltas.fields[ordinal])
		deltas.fields[ordinal] = nil
		if storage != base {
			slots[ordinal] = storage
		}
	}
}

func (deltas *MeasureDeltaBatch) Release() {
	for i := range deltas.touched {
		ordinal := deltas.touched[i]
		buf := deltas.fields[ordinal]
		if buf != nil {
			ReleaseMeasureDeltaSlice(buf)
			deltas.fields[ordinal] = nil
		}
	}
	measureDeltaSlotSlicePool.Put(deltas.fields)
	deltas.fields = nil
	pooled.ReleaseIntSlice(deltas.touched)
	deltas.touched = nil
}

func ReleaseMeasureEntryBufs(bufs [][]MeasureEntry) {
	for i := range bufs {
		if bufs[i] != nil {
			ReleaseMeasureEntrySlice(bufs[i])
			bufs[i] = nil
		}
	}
}

func CleanupBuildMeasureStates(buildOK *bool, states [][][]MeasureEntry) {
	if *buildOK {
		return
	}
	for i := range states {
		ReleaseMeasureEntryBufs(states[i])
	}
}

func NewMeasureStorageFromEntriesOwned(entries []MeasureEntry) MeasureStorage {
	if entries == nil {
		return MeasureStorage{}
	}

	if len(entries) == 0 {
		ReleaseMeasureEntrySlice(entries)
		return MeasureStorage{}
	}
	sortMeasureEntryBuf(entries)

	if len(entries) <= MeasureChunkThreshold {
		return newMeasureFlatStorageFromEntriesOwned(entries)
	}

	return newMeasureChunkedStorageFromEntriesOwned(entries)
}

func newMeasureFlatStorageFromEntriesOwned(entries []MeasureEntry) MeasureStorage {
	root := measureFlatRootPool.Get()
	root.ids = pooled.GetUint64Slice(len(entries))[:len(entries)]
	root.values = pooled.GetUint64Slice(len(entries))[:len(entries)]

	for i := 0; i < len(entries); i++ {
		entry := entries[i]
		root.ids[i] = entry.ID
		root.values[i] = entry.Value
	}
	root.refs.Store(1)

	ReleaseMeasureEntrySlice(entries)

	return MeasureStorage{flat: root}
}

func newMeasureChunkedStorageFromEntriesOwned(entries []MeasureEntry) MeasureStorage {
	root := measureChunkedRootPool.Get()
	root.refsByID = measureChunkRefSlicePool.Get(0)

	for start := 0; start < len(entries); {
		size := min(MeasureChunkTargetRows, len(entries)-start)
		root.appendEntryRangeAsChunk(entries, start, start+size)
		start += size
	}
	root.refs.Store(1)

	ReleaseMeasureEntrySlice(entries)

	return MeasureStorage{chunked: root}
}

func (r *measureChunkedRoot) appendEntryRangeAsChunk(entries []MeasureEntry, start int, end int) {
	chunk := newMeasureChunkFromEntryRange(entries, start, end)
	r.appendChunkRef(chunk)
}

func newMeasureChunkFromEntryRange(entries []MeasureEntry, start int, end int) *measureChunk {
	size := end - start
	chunk := measureChunkPool.Get()
	chunk.ids = pooled.GetUint64Slice(size)[:size]
	chunk.values = pooled.GetUint64Slice(size)[:size]
	for i := 0; i < size; i++ {
		entry := entries[start+i]
		chunk.ids[i] = entry.ID
		chunk.values[i] = entry.Value
	}
	chunk.refs.Store(1)
	return chunk
}

func (r *measureChunkedRoot) appendChunkRef(chunk *measureChunk) {
	rows := len(chunk.ids)
	r.refsByID = append(r.refsByID, measureChunkRef{
		LastID: chunk.ids[rows-1],
		chunk:  chunk,
	})
	r.rows += rows
}

func (s MeasureStorage) ApplyDeltasOwned(deltas []MeasureDelta) MeasureStorage {
	if deltas == nil {
		return s
	}

	if len(deltas) == 0 {
		ReleaseMeasureDeltaSlice(deltas)
		return s
	}
	sortMeasureBatchDeltaBuf(deltas)

	if s.flat != nil {
		return s.flat.applyDeltasOwned(deltas)
	}
	if s.chunked != nil {
		return s.chunked.applyDeltasOwned(deltas)
	}

	entries := GetMeasureEntrySlice(0)
	for i := 0; i < len(deltas); i++ {
		delta := deltas[i]
		if delta.NewOK {
			entries = append(entries, MeasureEntry{ID: delta.ID, Value: delta.New})
		}
	}

	ReleaseMeasureDeltaSlice(deltas)

	return NewMeasureStorageFromEntriesOwned(entries)
}

func (r *measureFlatRoot) applyDeltasOwned(deltas []MeasureDelta) MeasureStorage {
	entries := GetMeasureEntrySlice(0)
	i := 0
	j := 0

	for i < len(r.ids) && j < len(deltas) {
		ID := r.ids[i]
		delta := deltas[j]

		switch {
		case ID < delta.ID:
			entries = append(entries, MeasureEntry{ID: ID, Value: r.values[i]})
			i++

		case ID > delta.ID:
			if delta.NewOK {
				entries = append(entries, MeasureEntry{ID: delta.ID, Value: delta.New})
			}
			j++

		default:
			if delta.NewOK {
				entries = append(entries, MeasureEntry{ID: ID, Value: delta.New})
			}
			i++
			j++
		}
	}

	for ; i < len(r.ids); i++ {
		entries = append(entries, MeasureEntry{ID: r.ids[i], Value: r.values[i]})
	}

	for ; j < len(deltas); j++ {
		delta := deltas[j]
		if delta.NewOK {
			entries = append(entries, MeasureEntry{ID: delta.ID, Value: delta.New})
		}
	}

	ReleaseMeasureDeltaSlice(deltas)

	return NewMeasureStorageFromEntriesOwned(entries)
}

func (r *measureChunkedRoot) applyDeltasOwned(deltas []MeasureDelta) MeasureStorage {
	root := measureChunkedRootPool.Get()
	root.refsByID = measureChunkRefSlicePool.Get(0)

	deltaPos := 0
	for chunkPos := 0; chunkPos < len(r.refsByID); chunkPos++ {
		ref := r.refsByID[chunkPos]
		startDelta := deltaPos
		for deltaPos < len(deltas) && deltas[deltaPos].ID <= ref.LastID {
			deltaPos++
		}
		if startDelta == deltaPos {
			ref.chunk.retain()
			root.appendChunkRef(ref.chunk)
			continue
		}
		entries := GetMeasureEntrySlice(0)
		entries = ref.chunk.appendMergedWithDeltas(entries, deltas, startDelta, deltaPos)
		root.appendEntriesAsChunks(entries)
	}

	if deltaPos < len(deltas) {
		root.fillTailChunkWithDeltas(deltas, &deltaPos)
	}

	if deltaPos < len(deltas) {
		entries := GetMeasureEntrySlice(0)
		for ; deltaPos < len(deltas); deltaPos++ {
			delta := deltas[deltaPos]
			if delta.NewOK {
				entries = append(entries, MeasureEntry{ID: delta.ID, Value: delta.New})
			}
		}
		root.appendEntriesAsChunks(entries)
	}

	ReleaseMeasureDeltaSlice(deltas)
	if root.rows == 0 {
		measureChunkedRootPool.Put(root)
		return MeasureStorage{}
	}
	root.refs.Store(1)

	return MeasureStorage{chunked: root}
}

func (chunk *measureChunk) appendMergedWithDeltas(entries []MeasureEntry, deltas []MeasureDelta, startDelta, endDelta int) []MeasureEntry {
	idPos := 0
	deltaPos := startDelta

	for idPos < len(chunk.ids) && deltaPos < endDelta {
		ID := chunk.ids[idPos]
		delta := deltas[deltaPos]

		switch {
		case ID < delta.ID:
			entries = append(entries, MeasureEntry{ID: ID, Value: chunk.values[idPos]})
			idPos++

		case ID > delta.ID:
			if delta.NewOK {
				entries = append(entries, MeasureEntry{ID: delta.ID, Value: delta.New})
			}
			deltaPos++

		default:
			if delta.NewOK {
				entries = append(entries, MeasureEntry{ID: ID, Value: delta.New})
			}
			idPos++
			deltaPos++
		}
	}

	for ; idPos < len(chunk.ids); idPos++ {
		entries = append(entries, MeasureEntry{ID: chunk.ids[idPos], Value: chunk.values[idPos]})
	}

	for ; deltaPos < endDelta; deltaPos++ {
		delta := deltas[deltaPos]
		if delta.NewOK {
			entries = append(entries, MeasureEntry{ID: delta.ID, Value: delta.New})
		}
	}

	return entries
}

func (r *measureChunkedRoot) fillTailChunkWithDeltas(deltas []MeasureDelta, deltaPos *int) {
	if len(r.refsByID) == 0 {
		return
	}
	tailPos := len(r.refsByID) - 1
	tail := r.refsByID[tailPos].chunk
	tailLen := len(tail.ids)

	if tailLen >= MeasureChunkTargetRows {
		return
	}

	entries := GetMeasureEntrySlice(0)
	for i := 0; i < tailLen; i++ {
		entries = append(entries, MeasureEntry{ID: tail.ids[i], Value: tail.values[i]})
	}

	pos := *deltaPos
	for pos < len(deltas) && len(entries) < MeasureChunkTargetRows {
		delta := deltas[pos]
		if delta.NewOK {
			entries = append(entries, MeasureEntry{ID: delta.ID, Value: delta.New})
		}
		pos++
	}
	*deltaPos = pos

	if len(entries) == tailLen {
		ReleaseMeasureEntrySlice(entries)
		return
	}

	chunk := newMeasureChunkFromEntryRange(entries, 0, len(entries))
	r.refsByID[tailPos] = measureChunkRef{
		LastID: chunk.ids[len(chunk.ids)-1],
		chunk:  chunk,
	}

	r.rows += len(chunk.ids) - tailLen
	tail.release()

	ReleaseMeasureEntrySlice(entries)
}

func (r *measureChunkedRoot) appendEntriesAsChunks(entries []MeasureEntry) {
	for start := 0; start < len(entries); {
		size := min(MeasureChunkTargetRows, len(entries)-start)
		r.appendEntryRangeAsChunk(entries, start, start+size)
		start += size
	}
	ReleaseMeasureEntrySlice(entries)
}

func (s MeasureStorage) Retain() {
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

func (s MeasureStorage) Release() {
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
	measureFlatRootPool.Put(r)
}

func (chunk *measureChunk) retain() {
	chunk.refs.Add(1)
}

func (chunk *measureChunk) release() {
	if chunk == nil || chunk.refs.Add(-1) != 0 {
		return
	}
	measureChunkPool.Put(chunk)
}

func (r *measureChunkedRoot) release() {
	if r == nil || r.refs.Add(-1) != 0 {
		return
	}
	measureChunkedRootPool.Put(r)
}

func CloneMeasureStorageSlots(src []MeasureStorage, size int) []MeasureStorage {
	out := GetMeasureStorageSlice(size)[:size]
	if src == nil {
		return out
	}
	limit := min(size, len(src))
	for i := 0; i < limit; i++ {
		out[i] = src[i]
	}
	return out
}

func RetainSharedMeasureStorageSlots(next, prev []MeasureStorage) {
	if next == nil || prev == nil {
		return
	}
	limit := min(len(next), len(prev))
	for i := 0; i < limit; i++ {
		storage := next[i]
		if storage == prev[i] {
			storage.Retain()
		}
	}
}

func ReleaseMeasureStorageSlots(slots []MeasureStorage) {
	if slots == nil {
		return
	}
	for i := 0; i < len(slots); i++ {
		slots[i].Release()
	}
	ReleaseMeasureStorageSlice(slots)
}

func ReleaseMeasureStorageMap(m map[string]MeasureStorage) {
	for _, storage := range m {
		storage.Release()
	}
}

func (s MeasureStorage) Lookup(ID uint64) (uint64, bool) {
	if s.flat != nil {
		return s.flat.lookup(ID)
	}
	if s.chunked != nil {
		return s.chunked.lookup(ID)
	}
	return 0, false
}

func (s MeasureStorage) Rows() int {
	if s.flat != nil {
		return len(s.flat.ids)
	}
	if s.chunked != nil {
		return s.chunked.rows
	}
	return 0
}

func (s MeasureStorage) IsChunked() bool {
	return s.chunked != nil
}

func (s MeasureStorage) ChunkCount() int {
	if s.chunked == nil {
		return 0
	}
	return len(s.chunked.refsByID)
}

func (s MeasureStorage) FlatSlices() ([]uint64, []uint64, bool) {
	if s.flat == nil {
		return nil, nil, false
	}
	return s.flat.ids, s.flat.values, true
}

func (s MeasureStorage) ChunkSlices(pos int) ([]uint64, []uint64) {
	chunk := s.chunked.refsByID[pos].chunk
	return chunk.ids, chunk.values
}

func (s MeasureStorage) TailChunkRows() int {
	if s.chunked == nil || len(s.chunked.refsByID) == 0 {
		return 0
	}
	return len(s.chunked.refsByID[len(s.chunked.refsByID)-1].chunk.ids)
}

func (s MeasureStorage) LookupSteps() uint64 {
	if s.chunked != nil {
		return uint64(bits.Len(uint(len(s.chunked.refsByID))) + bits.Len(uint(MeasureChunkTargetRows)))
	}
	if s.flat != nil {
		return uint64(bits.Len(uint(len(s.flat.ids))))
	}
	return 0
}

func (r *measureFlatRoot) lookup(ID uint64) (uint64, bool) {
	if pos, ok := binsearch.Uint64(r.ids, ID); ok {
		return r.values[pos], true
	}
	return 0, false
}

func (r *measureChunkedRoot) lookup(ID uint64) (uint64, bool) {
	lo, hi := 0, len(r.refsByID)
	for lo < hi {
		mid := lo + (hi-lo)>>1
		if r.refsByID[mid].LastID >= ID {
			hi = mid
		} else {
			lo = mid + 1
		}
	}

	chunkPos := lo
	if chunkPos >= len(r.refsByID) {
		return 0, false
	}

	chunk := r.refsByID[chunkPos].chunk
	if pos, ok := binsearch.Uint64(chunk.ids, ID); ok {
		return chunk.values[pos], true
	}
	return 0, false
}
