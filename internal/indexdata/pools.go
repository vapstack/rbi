package indexdata

import (
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

const postingAccumMapMaxLen = 4 << 10

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
		NewCap: 8,
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
	postingDiffArenaPool = pooled.Pointers[PostingDiffArena]{
		New: func() *PostingDiffArena {
			return &PostingDiffArena{values: make([]postingDiffAccum, 0, 8)}
		},
	}
	postingAddArenaPool = pooled.Pointers[PostingAddArena]{
		New: func() *PostingAddArena {
			return &PostingAddArena{values: make([]postingAddAccum, 0, 8)}
		},
	}
	lenPostingDiffPool = pooled.Pointers[LenPostingDiff]{
		Clear: true,
	}
	stringPostingDiffMapPool = pooled.Maps[string, uint32]{
		NewCap: 8,
		MaxLen: postingAccumMapMaxLen,
	}
	fixedPostingDiffMapPool = pooled.Maps[uint64, uint32]{
		NewCap: 8,
		MaxLen: postingAccumMapMaxLen,
	}
	stringPostingAddMapPool = pooled.Maps[string, uint32]{
		NewCap: 8,
		MaxLen: postingAccumMapMaxLen,
	}
	fixedPostingAddMapPool = pooled.Maps[uint64, uint32]{
		NewCap: 8,
		MaxLen: postingAccumMapMaxLen,
	}

	fieldIndexFlatRootPool = pooled.Pointers[fieldIndexFlatRoot]{
		Cleanup: func(r *fieldIndexFlatRoot) {
			for i := range r.entries {
				r.entries[i].IDs.Release()
			}
			PutFieldEntrySlice(r.entries)
			r.entries = nil
			r.refs.Store(0)
		},
	}
	fieldIndexChunkPool = pooled.Pointers[fieldIndexChunk]{
		Cleanup: func(c *fieldIndexChunk) {
			if c.posts != nil {
				posting.ReleaseAll(c.posts)
				posting.PutSlice(c.posts)
				c.posts = nil
			}
			if c.numeric != nil {
				pooled.PutUint64Slice(c.numeric)
				c.numeric = nil
			}
			if c.stringRefs != nil {
				pooled.PutUint32Slice(c.stringRefs)
				c.stringRefs = nil
			}
			if c.stringData != nil {
				pooled.PutByteSlice(c.stringData)
				c.stringData = nil
			}
			c.rows = 0
			c.refs.Store(0)
		},
	}
	fieldIndexChunkedRootPool = pooled.Pointers[fieldIndexChunkedRoot]{
		Cleanup: func(r *fieldIndexChunkedRoot) {
			releaseFieldIndexChunkDirPages(r.pages)
			pooled.PutIntSlice(r.chunkPrefix)
			pooled.PutIntSlice(r.prefix)
			pooled.PutUint64Slice(r.rowPrefix)
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
			}
			fieldIndexChunkRefSlicePool.Put(p.refs)
			pooled.PutIntSlice(p.prefix)
			pooled.PutUint64Slice(p.rowPrefix)
			p.refs = nil
			p.prefix = nil
			p.rowPrefix = nil
			p.refsCount.Store(0)
		},
	}

	measureFlatRootPool = pooled.Pointers[measureFlatRoot]{
		Cleanup: func(r *measureFlatRoot) {
			pooled.PutUint64Slice(r.ids)
			pooled.PutUint64Slice(r.values)
			r.ids = nil
			r.values = nil
			r.refs.Store(0)
		},
	}
	measureChunkPool = pooled.Pointers[measureChunk]{
		Cleanup: func(c *measureChunk) {
			pooled.PutUint64Slice(c.ids)
			pooled.PutUint64Slice(c.values)
			c.ids = nil
			c.values = nil
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

	postingDeltaSlicePool = pooled.NewSlicePool[PostingDelta](4<<10, pooled.ClearCap, func(buf []PostingDelta) {
		for i := range buf {
			buf[i].Delta.Add.Release()
			buf[i].Delta.Remove.Release()
		}
	})
	fieldEntrySlicePool             = pooled.NewSlicePool[Entry](4<<10, pooled.ClearCap)
	fieldStorageSlicePool           = pooled.NewSlicePool[FieldStorage](4<<10, pooled.ClearCap)
	fieldStorageRunCursorSlicePool  = pooled.NewSlicePool[fieldStorageRunCursor](4<<10, pooled.ClearCap)
	fieldIndexChunkRefSlicePool     = pooled.NewSlicePool[fieldIndexChunkRef](1<<8, pooled.ClearCap)
	fieldIndexChunkDirPageSlicePool = pooled.NewSlicePool[*fieldIndexChunkDirPage](16<<10, pooled.ClearCap)
	fieldIndexKeySlicePool          = pooled.NewSlicePool[keycodec.IndexKey](4<<10, pooled.ClearCap)
	measureEntrySlicePool           = pooled.NewSlicePool[MeasureEntry](16<<10, pooled.NoClear)
	measureBatchDeltaSlicePool      = pooled.NewSlicePool[MeasureDelta](8<<10, pooled.NoClear)
	measureDeltaSlotSlicePool       = pooled.NewSlicePool[[]MeasureDelta](4<<10, pooled.ClearCap)
	measureStorageSlicePool         = pooled.NewSlicePool[MeasureStorage](4<<10, pooled.ClearCap)
	measureChunkRefSlicePool        = pooled.NewSlicePool[measureChunkRef](64<<10, pooled.ClearLen)
)

// ---

func GetPostingMap() map[string]posting.List  { return postingMapPool.Get() }
func PutPostingMap(m map[string]posting.List) { postingMapPool.Put(m) }

func GetFixedPostingMap() map[uint64]posting.List  { return fixedPostingMapPool.Get() }
func PutFixedPostingMap(m map[uint64]posting.List) { fixedPostingMapPool.Put(m) }

func GetPostingDeltaSlice(capHint int) []PostingDelta { return postingDeltaSlicePool.Get(capHint) }
func PutPostingDeltaSlice(buf []PostingDelta)         { postingDeltaSlicePool.Put(buf) }

func GetFieldEntrySlice(capHint int) []Entry { return fieldEntrySlicePool.Get(capHint) }
func PutFieldEntrySlice(buf []Entry)         { fieldEntrySlicePool.Put(buf) }

func getFieldIndexKeySlice(capHint int) []keycodec.IndexKey {
	return fieldIndexKeySlicePool.Get(capHint)
}

func putFieldIndexKeySlice(buf []keycodec.IndexKey) {
	fieldIndexKeySlicePool.Put(buf)
}

func GetBatchPostingDeltaMap() map[uint64]BatchPostingDelta  { return batchPostingDeltaMapPool.Get() }
func PutBatchPostingDeltaMap(m map[uint64]BatchPostingDelta) { batchPostingDeltaMapPool.Put(m) }

func PutStringPostingDiffMap(m map[string]uint32) { stringPostingDiffMapPool.Put(m) }
func PutFixedPostingDiffMap(m map[uint64]uint32)  { fixedPostingDiffMapPool.Put(m) }
func PutStringPostingAddMap(m map[string]uint32)  { stringPostingAddMapPool.Put(m) }
func PutFixedPostingAddMap(m map[uint64]uint32)   { fixedPostingAddMapPool.Put(m) }

func GetFieldStorageSlice(capHint int) []FieldStorage { return fieldStorageSlicePool.Get(capHint) }
func PutFieldStorageSlice(slots []FieldStorage)       { fieldStorageSlicePool.Put(slots) }

func putOwnedFieldIndexChunkRefSlice(buf []fieldIndexChunkRef) {
	for i := range buf {
		if buf[i].chunk != nil {
			buf[i].chunk.release()
			buf[i] = fieldIndexChunkRef{}
		}
	}
	fieldIndexChunkRefSlicePool.Put(buf)
}

func GetMeasureEntrySlice(hint int) []MeasureEntry { return measureEntrySlicePool.Get(hint) }
func PutMeasureEntrySlice(buf []MeasureEntry)      { measureEntrySlicePool.Put(buf) }

func GetMeasureDeltaSlice(hint int) []MeasureDelta { return measureBatchDeltaSlicePool.Get(hint) }
func PutMeasureDeltaSlice(buf []MeasureDelta)      { measureBatchDeltaSlicePool.Put(buf) }

func GetMeasureStorageSlice(hint int) []MeasureStorage { return measureStorageSlicePool.Get(hint) }
func PutMeasureStorageSlice(slots []MeasureStorage)    { measureStorageSlicePool.Put(slots) }
