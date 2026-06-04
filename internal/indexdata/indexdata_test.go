package indexdata

import (
	"fmt"
	"testing"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
)

func fieldStorageSingleton(id uint64) posting.List {
	return (posting.List{}).BuildAdded(id)
}

func fieldStoragePosting(ids ...uint64) posting.List {
	var p posting.List
	for i := range ids {
		p = p.BuildAdded(ids[i])
	}
	return p
}

func fieldStorageOwnedTestPosting(id uint64) posting.List {
	return fieldStorageSingleton(id).BuildAdded(id + 1000000)
}

func NewFieldIndexView(base *[]Entry) FieldIndexView {
	if base == nil {
		return FieldIndexView{}
	}
	return FieldIndexView{base: *base}
}

func (o FieldIndexView) PostingAt(rank int) posting.List {
	if rank < 0 || rank >= o.KeyCount() {
		return posting.List{}
	}
	if o.chunked != nil {
		pos := o.chunked.posForRank(rank)
		ref, ok := o.chunked.refAtChunk(pos.chunk)
		if !ok {
			return posting.List{}
		}
		return ref.chunk.postingAt(pos.entry)
	}
	return o.base[rank].IDs.Borrow()
}

func (s MeasureStorage) TailChunkRows() int {
	if s.chunked == nil || len(s.chunked.refsByID) == 0 {
		return 0
	}
	return len(s.chunked.refsByID[len(s.chunked.refsByID)-1].chunk.ids)
}

func newFieldIndexChunkedRootFromPages(pages []*fieldIndexChunkDirPage) *fieldIndexChunkedRoot {
	if len(pages) == 0 {
		return nil
	}
	pageBuf := fieldIndexChunkDirPageSlicePool.Get(len(pages))
	pageBuf = append(pageBuf, pages...)
	return newFieldIndexChunkedRootFromOwnedPages(pageBuf)
}

func (r *fieldIndexChunkedRoot) entryPrefixForChunk(limit int) int {
	if r == nil || limit <= 0 {
		return 0
	}
	if limit >= r.chunkCount {
		return r.keyCount
	}
	page := searchIntLower(r.chunkPrefix[1:], limit)
	if r.pages == nil || page >= len(r.pages) {
		return r.keyCount
	}
	off := limit - r.chunkPrefix[page]
	return r.prefix[page] + r.pages[page].prefix[off]
}

func fieldStorageEntriesForTest(n int, numeric bool) []Entry {
	entries := make([]Entry, n)
	for i := 0; i < n; i++ {
		key := keycodec.FromStoredString(fmt.Sprintf("k/%06d", i), false)
		if numeric {
			key = keycodec.FromU64(uint64(i * 2))
		}
		entries[i] = Entry{
			Key: key,
			IDs: fieldStorageSingleton(uint64(i + 1)),
		}
	}
	return entries
}

func fieldStorageOwnedChunkRef(size int, fixed8 bool) fieldIndexChunkRef {
	posts := make([]posting.List, size)
	rows := uint64(size * 2)
	if fixed8 {
		keys := make([]uint64, size)
		for i := 0; i < size; i++ {
			keys[i] = uint64(i * 2)
			posts[i] = fieldStorageOwnedTestPosting(uint64(i + 1))
		}
		chunk := &fieldIndexChunk{
			posts:   posts,
			numeric: keys,
			rows:    rows,
		}
		chunk.refs.Store(1)
		return fieldIndexChunkRef{last: chunk.keyAt(size - 1), chunk: chunk}
	}
	keys := make([]keycodec.IndexKey, size)
	for i := 0; i < size; i++ {
		keys[i] = keycodec.FromStoredString(fmt.Sprintf("k/%04d", i*2), false)
		posts[i] = fieldStorageOwnedTestPosting(uint64(i + 1))
	}
	chunk := newFieldIndexChunkFromKeys(posts, keys, rows)
	return fieldIndexChunkRef{last: chunk.keyAt(size - 1), chunk: chunk}
}

func fieldStorageInsertedTestKey(pos int, fixed8 bool) keycodec.IndexKey {
	if fixed8 {
		return keycodec.FromU64(uint64(pos*2 + 1))
	}
	return keycodec.FromStoredString(fmt.Sprintf("k/%04d", pos*2+1), false)
}

func fieldStorageSingleChunkRoot(ref fieldIndexChunkRef) *fieldIndexChunkedRoot {
	return newFieldIndexChunkedRootFromPages([]*fieldIndexChunkDirPage{newFieldIndexChunkDirPage([]fieldIndexChunkRef{ref})})
}

func fieldStorageTwoChunkNumericRoot() *fieldIndexChunkedRoot {
	left := fieldStorageOwnedChunkRef(fieldIndexChunkTargetEntries, true)
	right := fieldStorageOwnedChunkRef(fieldIndexChunkTargetEntries, true)

	const offset = uint64(1 << 20)
	for i := range right.chunk.numeric {
		right.chunk.numeric[i] += offset
	}
	right.last = right.chunk.keyAt(right.chunk.keyCount() - 1)

	return newFieldIndexChunkedRootFromPages([]*fieldIndexChunkDirPage{
		newFieldIndexChunkDirPage([]fieldIndexChunkRef{left, right}),
	})
}

func fieldStorageFullPageSplitNumericRoot() *fieldIndexChunkedRoot {
	refs := make([]fieldIndexChunkRef, 0, fieldIndexDirPageTargetRefs)
	for i := 0; i < fieldIndexDirPageTargetRefs-1; i++ {
		ref := fieldStorageOwnedChunkRef(1, true)
		ref.chunk.numeric[0] = uint64(i * 4)
		ref.last = ref.chunk.keyAt(0)
		refs = append(refs, ref)
	}

	ref := fieldStorageOwnedChunkRef(fieldIndexChunkMaxEntries, true)
	const splitChunkBase = uint64(1 << 32)
	for i := range ref.chunk.numeric {
		ref.chunk.numeric[i] = splitChunkBase + uint64(i*4)
	}
	ref.last = ref.chunk.keyAt(ref.chunk.keyCount() - 1)
	refs = append(refs, ref)

	return newFieldIndexChunkedRootFromPages([]*fieldIndexChunkDirPage{
		newFieldIndexChunkDirPage(refs),
	})
}

func fieldStorageAssertPostingContains(t *testing.T, storage FieldStorage, key string, want ...uint64) {
	t.Helper()
	ids := NewFieldIndexViewFromStorage(storage).LookupPostingRetained(key)
	if ids.Cardinality() != uint64(len(want)) {
		t.Fatalf("posting %q cardinality: got %d want %d", key, ids.Cardinality(), len(want))
	}
	for _, id := range want {
		if !ids.Contains(id) {
			t.Fatalf("posting %q missing id %d", key, id)
		}
	}
}

func measureStorageForTest(rows int) MeasureStorage {
	entries := GetMeasureEntrySlice(rows)
	for i := 0; i < rows; i++ {
		entries = append(entries, MeasureEntry{
			ID:    uint64(i*2 + 1),
			Value: uint64(i * 10),
		})
	}
	return NewMeasureStorageFromEntriesOwned(entries)
}

func measureStorageAssertValue(t *testing.T, storage MeasureStorage, id uint64, want uint64) {
	t.Helper()
	got, ok := storage.Lookup(id)
	if !ok {
		t.Fatalf("measure id %d missing", id)
	}
	if got != want {
		t.Fatalf("measure id %d: got %d want %d", id, got, want)
	}
}

func measureStorageAssertMissing(t *testing.T, storage MeasureStorage, id uint64) {
	t.Helper()
	if got, ok := storage.Lookup(id); ok {
		t.Fatalf("measure id %d unexpectedly present with value %d", id, got)
	}
}

func fieldStorageAssertStorageMatchesEntries(t *testing.T, storage FieldStorage, entries []Entry) {
	t.Helper()
	ov := NewFieldIndexViewFromStorage(storage)
	if ov.KeyCount() != len(entries) {
		t.Fatalf("unexpected storage size: got %d want %d", ov.KeyCount(), len(entries))
	}
	cur := ov.NewCursor(ov.RangeByRanks(0, ov.KeyCount()), false)
	for i := range entries {
		key, ids, ok := cur.Next()
		if !ok {
			t.Fatalf("missing entry at %d", i)
		}
		if keycodec.Compare(key, entries[i].Key) != 0 {
			t.Fatalf("key[%d]: got %q want %q", i, key.UnsafeString(), entries[i].Key.UnsafeString())
		}
		if ids.Cardinality() != entries[i].IDs.Cardinality() {
			t.Fatalf("posting[%d] cardinality: got %d want %d", i, ids.Cardinality(), entries[i].IDs.Cardinality())
		}
		want := entries[i].IDs.ToArray()
		for _, id := range want {
			if !ids.Contains(id) {
				t.Fatalf("posting[%d] missing id %d", i, id)
			}
		}
	}
	if _, _, ok := cur.Next(); ok {
		t.Fatalf("unexpected extra storage entry")
	}
}
