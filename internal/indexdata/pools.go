package indexdata

import (
	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

var (
	postingMapPool = pooled.Maps[string, posting.List]{
		NewCap: 64,
		MaxLen: 4 << 10,
	}
	fixedPostingMapPool = pooled.Maps[uint64, posting.List]{
		NewCap: 64,
		MaxLen: 4 << 10,
	}
	lenPostingMapPool = pooled.Maps[uint32, posting.List]{
		NewCap: 128,
		MaxLen: 4 << 10,
	}
	lenCountMapPool = pooled.Maps[uint64, uint32]{
		NewCap: 1024,
		MaxLen: 64 << 10,
	}
	batchPostingDeltaMapPool = pooled.Maps[uint64, BatchPostingDelta]{
		NewCap: 64,
		MaxLen: 4 << 10,
	}
	fieldIndexFlatRootPool = pooled.Pointers[fieldIndexFlatRoot]{
		Cleanup: func(r *fieldIndexFlatRoot) {
			for i := range r.entries {
				r.entries[i].IDs.Release()
			}
			ReleaseFieldEntrySlice(r.entries)
			pooled.ReleaseByteSlice(r.stringData)
			r.entries = nil
			r.stringData = nil
			r.refs.Store(0)
		},
	}
	fieldIndexChunkPool = pooled.Pointers[fieldIndexChunk]{
		Cleanup: func(c *fieldIndexChunk) {
			if c.posts != nil {
				posting.ReleaseAll(c.posts)
				posting.ReleaseSlice(c.posts)
				c.posts = nil
			}
			if c.numeric != nil {
				pooled.ReleaseUint64Slice(c.numeric)
				c.numeric = nil
			}
			if c.stringRefs != nil {
				pooled.ReleaseUint32Slice(c.stringRefs)
				c.stringRefs = nil
			}
			if c.stringData != nil {
				pooled.ReleaseByteSlice(c.stringData)
				c.stringData = nil
			}
			c.rows = 0
			c.refs.Store(0)
		},
	}
	fieldIndexChunkedRootPool = pooled.Pointers[fieldIndexChunkedRoot]{
		Cleanup: func(r *fieldIndexChunkedRoot) {
			releaseFieldIndexChunkDirPages(r.pages)
			pooled.ReleaseIntSlice(r.chunkPrefix)
			pooled.ReleaseIntSlice(r.prefix)
			pooled.ReleaseUint64Slice(r.rowPrefix)
			r.pages = nil
			r.chunkPrefix = nil
			r.prefix = nil
			r.rowPrefix = nil
			r.keyCount = 0
			r.chunkCount = 0
			r.refs.Store(0)
		},
	}
	fieldIndexChunkDirPagePool = pooled.Pointers[fieldIndexChunkDirPage]{
		Cleanup: func(p *fieldIndexChunkDirPage) {
			for i := range p.refs {
				p.refs[i].chunk.release()
				p.refs[i] = fieldIndexChunkRef{}
			}
			if cap(p.refs) > fieldIndexDirPageTargetRefs {
				p.refs = nil
			} else {
				p.refs = p.refs[:0]
			}
			if cap(p.prefix) > fieldIndexDirPageTargetRefs+1 {
				p.prefix = nil
			} else {
				p.prefix = p.prefix[:0]
			}
			if cap(p.rowPrefix) > fieldIndexDirPageTargetRefs+1 {
				p.rowPrefix = nil
			} else {
				p.rowPrefix = p.rowPrefix[:0]
			}
			p.refsCount.Store(0)
		},
	}

	measureFlatRootPool = pooled.Pointers[measureFlatRoot]{
		Cleanup: func(r *measureFlatRoot) {
			pooled.ReleaseUint64Slice(r.ids)
			pooled.ReleaseUint64Slice(r.values)
			r.ids = nil
			r.values = nil
			r.refs.Store(0)
		},
	}
	measureChunkPool = pooled.Pointers[measureChunk]{
		Cleanup: func(c *measureChunk) {
			if cap(c.ids) > MeasureChunkTargetRows {
				c.ids = nil
			} else {
				c.ids = c.ids[:0]
			}
			if cap(c.values) > MeasureChunkTargetRows {
				c.values = nil
			} else {
				c.values = c.values[:0]
			}
			c.refs.Store(0)
		},
	}
	measureChunkedRootPool = pooled.Pointers[measureChunkedRoot]{
		Cleanup: func(r *measureChunkedRoot) {
			for i := range r.refsByID {
				r.refsByID[i].chunk.release()
			}
			measureChunkRefSlicePool.Put(r.refsByID)
			r.refsByID = nil
			r.rows = 0
			r.refs.Store(0)
		},
	}

	postingDeltaSlicePool = pooled.Slices[PostingDelta]{
		MaxCap: 4 << 10,
		Clear:  pooled.ClearCap,
		Cleanup: func(buf []PostingDelta) {
			for i := range buf {
				buf[i].Delta.Add.Release()
				buf[i].Delta.Remove.Release()
			}
		},
	}
	fieldEntrySlicePool = pooled.Slices[Entry]{
		MaxCap: 4 << 10,
		Clear:  pooled.ClearCap,
	}
	fieldStorageSlicePool = pooled.Slices[FieldStorage]{
		MaxCap: 4 << 10,
		Clear:  pooled.ClearCap,
	}
	fieldStorageRunCursorSlicePool = pooled.Slices[fieldStorageRunCursor]{
		MaxCap: 4 << 10,
		Clear:  pooled.ClearCap,
	}
	fieldIndexChunkRefSlicePool = pooled.Slices[fieldIndexChunkRef]{
		MaxCap: 1 << 8,
		Clear:  pooled.ClearCap,
	}
	fieldIndexChunkDirPageSlicePool = pooled.Slices[*fieldIndexChunkDirPage]{
		MaxCap: 16 << 10,
		Clear:  pooled.ClearCap,
	}
	measureEntrySlicePool = pooled.Slices[MeasureEntry]{
		MaxCap: 16 << 10,
		Clear:  pooled.NoClear,
	}
	measureEntrySlotSlicePool = pooled.Slices[[]MeasureEntry]{
		MaxCap:  4 << 10,
		Clear:   pooled.ClearCap,
		Cleanup: ReleaseMeasureEntryBufs,
	}
	measureBatchDeltaSlicePool = pooled.Slices[MeasureDelta]{
		MaxCap: 8 << 10,
		Clear:  pooled.NoClear,
	}
	measureDeltaSlotSlicePool = pooled.Slices[[]MeasureDelta]{
		MaxCap: 4 << 10,
		Clear:  pooled.ClearCap,
	}
	measureStorageSlicePool = pooled.Slices[MeasureStorage]{
		MaxCap: 4 << 10,
		Clear:  pooled.ClearCap,
	}
	measureChunkRefSlicePool = pooled.Slices[measureChunkRef]{
		MaxCap: 64 << 10,
		Clear:  pooled.ClearLen,
	}
)

// ---

func GetPostingMap() map[string]posting.List      { return postingMapPool.Get() }
func ReleasePostingMap(m map[string]posting.List) { postingMapPool.Put(m) }

func GetFixedPostingMap() map[uint64]posting.List      { return fixedPostingMapPool.Get() }
func ReleaseFixedPostingMap(m map[uint64]posting.List) { fixedPostingMapPool.Put(m) }

func GetPostingDeltaSlice(capHint int) []PostingDelta { return postingDeltaSlicePool.Get(capHint) }
func ReleasePostingDeltaSlice(buf []PostingDelta)     { postingDeltaSlicePool.Put(buf) }

func GetFieldEntrySlice(capHint int) []Entry { return fieldEntrySlicePool.Get(capHint) }
func ReleaseFieldEntrySlice(buf []Entry)     { fieldEntrySlicePool.Put(buf) }

func GetBatchPostingDeltaMap() map[uint64]BatchPostingDelta      { return batchPostingDeltaMapPool.Get() }
func ReleaseBatchPostingDeltaMap(m map[uint64]BatchPostingDelta) { batchPostingDeltaMapPool.Put(m) }

func GetLenPostingMap() map[uint32]posting.List      { return lenPostingMapPool.Get() }
func ReleaseLenPostingMap(m map[uint32]posting.List) { lenPostingMapPool.Put(m) }

func GetFieldStorageSlice(capHint int) []FieldStorage { return fieldStorageSlicePool.Get(capHint) }
func ReleaseFieldStorageSlice(slots []FieldStorage)   { fieldStorageSlicePool.Put(slots) }

func releaseOwnedFieldIndexChunkRefSlice(buf []fieldIndexChunkRef) {
	for i := range buf {
		if buf[i].chunk != nil {
			buf[i].chunk.release()
			buf[i] = fieldIndexChunkRef{}
		}
	}
	fieldIndexChunkRefSlicePool.Put(buf)
}

func GetMeasureEntrySlice(hint int) []MeasureEntry { return measureEntrySlicePool.Get(hint) }
func ReleaseMeasureEntrySlice(buf []MeasureEntry)  { measureEntrySlicePool.Put(buf) }

func GetMeasureEntrySlots(hint int) [][]MeasureEntry {
	return measureEntrySlotSlicePool.Get(hint)[:hint]
}
func ReleaseMeasureEntrySlots(slots [][]MeasureEntry) { measureEntrySlotSlicePool.Put(slots) }

func GetMeasureDeltaSlice(hint int) []MeasureDelta { return measureBatchDeltaSlicePool.Get(hint) }
func ReleaseMeasureDeltaSlice(buf []MeasureDelta)  { measureBatchDeltaSlicePool.Put(buf) }

func GetMeasureStorageSlice(hint int) []MeasureStorage  { return measureStorageSlicePool.Get(hint) }
func ReleaseMeasureStorageSlice(slots []MeasureStorage) { measureStorageSlicePool.Put(slots) }
