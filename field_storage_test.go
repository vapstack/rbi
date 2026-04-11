package rbi

import (
	"fmt"
	"testing"

	"github.com/vapstack/rbi/internal/posting"
)

func fieldStorageSingleton(id uint64) posting.List {
	return (posting.List{}).BuildAdded(id)
}

func fieldStorageOwnedTestPosting(id uint64) posting.List {
	return fieldStorageSingleton(id).BuildAdded(id + 1000000)
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
		return fieldIndexChunkRef{last: chunk.keyAt(size - 1), chunk: chunk}
	}
	keys := make([]indexKey, size)
	for i := 0; i < size; i++ {
		keys[i] = indexKeyFromStoredString(fmt.Sprintf("k/%04d", i*2), false)
		posts[i] = fieldStorageOwnedTestPosting(uint64(i + 1))
	}
	chunk := newFieldIndexChunkFromKeys(posts, keys, rows)
	return fieldIndexChunkRef{last: chunk.keyAt(size - 1), chunk: chunk}
}

func fieldStorageInsertedTestKey(pos int, fixed8 bool) indexKey {
	if fixed8 {
		return indexKeyFromU64(uint64(pos*2 + 1))
	}
	return indexKeyFromStoredString(fmt.Sprintf("k/%04d", pos*2+1), false)
}

func fieldStorageSingleChunkRoot(ref fieldIndexChunkRef) *fieldIndexChunkedRoot {
	return newFieldIndexChunkedRootFromPages([]*fieldIndexChunkDirPage{newFieldIndexChunkDirPage([]fieldIndexChunkRef{ref})})
}

func TestFlattenChunkedFieldIndexRoot_RoundTrip(t *testing.T) {
	entries := make([]index, fieldIndexChunkThreshold)
	for i := range entries {
		entries[i] = index{
			Key: indexKeyFromStoredString(fmt.Sprintf("k%04d", i), false),
			IDs: fieldStorageSingleton(uint64(i + 1)),
		}
	}

	root := buildChunkedFieldIndexRoot(entries)
	if root == nil {
		t.Fatalf("expected chunked root")
	}

	flat := flattenChunkedFieldIndexRoot(root)
	if flat == nil {
		t.Fatalf("expected flattened slice")
	}
	if len(*flat) != len(entries) {
		t.Fatalf("unexpected materialized len: got %d want %d", len(*flat), len(entries))
	}
	for i := range entries {
		if compareIndexKeys((*flat)[i].Key, entries[i].Key) != 0 {
			t.Fatalf("unexpected key at %d", i)
		}
		if (*flat)[i].IDs.Cardinality() != entries[i].IDs.Cardinality() || !(*flat)[i].IDs.Contains(uint64(i+1)) {
			t.Fatalf("unexpected posting at %d", i)
		}
	}
}

func TestApplyBatchPostingDeltaOwned_ReusesAddWhenRemoveEmptiesBase(t *testing.T) {
	delta := batchPostingDelta{
		remove: fieldStorageSingleton(1),
		add:    fieldStorageSingleton(2).BuildAdded(3),
	}

	out := applyBatchPostingDeltaOwned(fieldStorageSingleton(1), &delta)
	defer out.Release()

	if out.IsEmpty() {
		t.Fatalf("expected rewritten posting to remain non-empty")
	}
	if out.Cardinality() != 2 {
		t.Fatalf("unexpected cardinality: got %d want 2", out.Cardinality())
	}
	if !out.Contains(2) || !out.Contains(3) {
		t.Fatalf("unexpected rewritten posting contents")
	}
	if !delta.add.IsEmpty() || !delta.remove.IsEmpty() {
		t.Fatalf("expected delta buffers to be consumed")
	}
}

func TestOverlayRangeStats_ChunkedMatchesPostingCardinality(t *testing.T) {
	entries := make([]index, 0, fieldIndexChunkThreshold+37)
	var expected uint64
	start := 17
	end := fieldIndexChunkThreshold + 29
	for i := 0; i < fieldIndexChunkThreshold+37; i++ {
		card := uint64(i%5 + 1)
		var ids posting.List
		for j := uint64(0); j < card; j++ {
			ids = ids.BuildAdded(uint64(i*10) + j + 1)
		}
		if i >= start && i < end {
			expected += ids.Cardinality()
		}
		entries = append(entries, index{
			Key: indexKeyFromStoredString(fmt.Sprintf("k%04d", i), false),
			IDs: ids,
		})
	}

	root := buildChunkedFieldIndexRoot(entries)
	if root == nil {
		t.Fatalf("expected chunked root")
	}
	ov := fieldOverlay{chunked: root}
	br := ov.rangeByRanks(start, end)
	buckets, rows := overlayRangeStats(ov, br)
	if buckets != end-start {
		t.Fatalf("unexpected bucket count: got %d want %d", buckets, end-start)
	}
	if rows != expected {
		t.Fatalf("unexpected row estimate: got %d want %d", rows, expected)
	}
}

func TestFieldIndexChunkStreamBuilder_RoundTripAfterFlushes(t *testing.T) {
	tests := []struct {
		name    string
		numeric bool
	}{
		{name: "String", numeric: false},
		{name: "Numeric", numeric: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			const total = fieldIndexChunkThreshold + fieldIndexChunkTargetEntries + 17

			builder := newFieldIndexChunkBuilder(total)
			stream := newFieldIndexChunkStreamBuilder(&builder, tc.numeric)

			wantKeys := make([]string, total)
			wantIDs := make([]uint64, total)
			for i := 0; i < total; i++ {
				id := uint64(i + 1)
				wantIDs[i] = id

				var key indexKey
				if tc.numeric {
					raw := uint64(i*3 + 7)
					wantKeys[i] = uint64ByteStr(raw)
					key = indexKeyFromStoredString(wantKeys[i], true)
				} else {
					wantKeys[i] = fmt.Sprintf("k/%02d/%05d", i%17, i)
					key = indexKeyFromStoredString(wantKeys[i], false)
				}

				stream.append(key, fieldStorageSingleton(id))
			}
			stream.finish()

			root := builder.root()
			if root == nil {
				t.Fatalf("expected chunked root")
			}
			if root.chunkCount < 2 {
				t.Fatalf("expected multiple chunks, got %d", root.chunkCount)
			}

			flat := flattenChunkedFieldIndexRoot(root)
			if flat == nil {
				t.Fatalf("expected flattened slice")
			}
			if len(*flat) != total {
				t.Fatalf("unexpected flattened len: got %d want %d", len(*flat), total)
			}
			for i := range *flat {
				if got := (*flat)[i].Key.asUnsafeString(); got != wantKeys[i] {
					t.Fatalf("key[%d]: got %q want %q", i, got, wantKeys[i])
				}
				if !(*flat)[i].IDs.Contains(wantIDs[i]) || (*flat)[i].IDs.Cardinality() != 1 {
					t.Fatalf("posting[%d]: got=%v want singleton(%d)", i, (*flat)[i].IDs, wantIDs[i])
				}
			}
		})
	}
}

func TestNewFieldIndexChunkRefsWithInsertedEntry_OwnsUntouchedPostings(t *testing.T) {
	tests := []struct {
		name   string
		fixed8 bool
		size   int
	}{
		{name: "StringNoSplit", fixed8: false, size: fieldIndexChunkTargetEntries},
		{name: "StringSplit", fixed8: false, size: fieldIndexChunkMaxEntries},
		{name: "NumericNoSplit", fixed8: true, size: fieldIndexChunkTargetEntries},
		{name: "NumericSplit", fixed8: true, size: fieldIndexChunkMaxEntries},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ref := fieldStorageOwnedChunkRef(tc.size, tc.fixed8)
			pos := tc.size / 2
			add := index{
				Key: fieldStorageInsertedTestKey(pos, tc.fixed8),
				IDs: fieldStorageSingleton(uint64(tc.size + 1000)),
			}

			replRefs := newFieldIndexChunkRefsWithInsertedEntry(ref, pos, add)
			if len(replRefs) == 0 {
				t.Fatalf("expected replacement refs")
			}
			if tc.size+1 <= fieldIndexChunkMaxEntries {
				if len(replRefs) != 1 {
					t.Fatalf("expected no split, got %d refs", len(replRefs))
				}
			} else if len(replRefs) != 2 {
				t.Fatalf("expected split, got %d refs", len(replRefs))
			}

			replRoot := fieldStorageSingleChunkRoot(replRefs[0])
			if len(replRefs) > 1 {
				replRoot = newFieldIndexChunkedRootFromPages([]*fieldIndexChunkDirPage{newFieldIndexChunkDirPage(replRefs)})
			}
			replStorage := newChunkedFieldIndexStorage(replRoot)

			src := 0
			for _, repl := range replRefs {
				for i := 0; i < repl.chunk.keyCount(); i++ {
					key := repl.chunk.keyAt(i)
					ids := repl.chunk.posts[i]
					if compareIndexKeys(key, add.Key) == 0 {
						if ids.IsBorrowed() {
							t.Fatalf("inserted posting unexpectedly borrowed")
						}
						if !ids.SharesPayload(add.IDs) {
							t.Fatalf("inserted posting lost ownership")
						}
						continue
					}
					if src >= len(ref.chunk.posts) {
						t.Fatalf("unexpected extra untouched posting at %d", i)
					}
					if ids.IsBorrowed() {
						t.Fatalf("untouched posting at src=%d unexpectedly borrowed", src)
					}
					if ids.Cardinality() != ref.chunk.posts[src].Cardinality() {
						t.Fatalf("untouched posting at src=%d changed cardinality", src)
					}
					src++
				}
			}
			if src != len(ref.chunk.posts) {
				t.Fatalf("untouched postings consumed: got %d want %d", src, len(ref.chunk.posts))
			}

			releaseFieldIndexStorageOwned(replStorage)
			posting.ReleaseSliceOwned(ref.chunk.posts)
		})
	}
}

func TestApplySingleFieldPostingDiffChunked_OwnsUntouchedPostingsOnExistingKeyFastPath(t *testing.T) {
	tests := []struct {
		name   string
		fixed8 bool
	}{
		{name: "String", fixed8: false},
		{name: "Numeric", fixed8: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ref := fieldStorageOwnedChunkRef(fieldIndexChunkTargetEntries, tc.fixed8)
			root := fieldStorageSingleChunkRoot(ref)
			pos := fieldIndexChunkTargetEntries / 2
			key := ref.chunk.keyAt(pos)
			baseIDs := ref.chunk.posts[pos]
			baseValues := baseIDs.ToArray()
			addedID := uint64(fieldIndexChunkTargetEntries + 1000)

			storage := applySingleFieldPostingDiffChunked(root, keyedBatchPostingDelta{
				key: key,
				delta: batchPostingDelta{
					add: fieldStorageSingleton(addedID),
				},
			})
			if storage.chunked == nil {
				t.Fatalf("expected chunked storage")
			}

			replRef, ok := storage.chunked.refAtChunk(0)
			if !ok || replRef.chunk == nil {
				t.Fatalf("expected replacement chunk")
			}
			if replRef.chunk.keyCount() != ref.chunk.keyCount() {
				t.Fatalf("unexpected key count: got %d want %d", replRef.chunk.keyCount(), ref.chunk.keyCount())
			}

			for i := 0; i < replRef.chunk.keyCount(); i++ {
				ids := replRef.chunk.posts[i]
				if i == pos {
					if ids.IsBorrowed() {
						t.Fatalf("updated posting unexpectedly borrowed")
					}
					if ids.SharesPayload(baseIDs) {
						t.Fatalf("updated posting still shares base payload")
					}
					if !ids.Contains(addedID) {
						t.Fatalf("updated posting missing added id")
					}
					for _, id := range baseValues {
						if !ids.Contains(id) {
							t.Fatalf("updated posting missing base id %d", id)
						}
					}
					continue
				}
				if ids.IsBorrowed() {
					t.Fatalf("untouched posting %d unexpectedly borrowed", i)
				}
				if ids.Cardinality() != ref.chunk.posts[i].Cardinality() {
					t.Fatalf("untouched posting %d changed cardinality", i)
				}
			}

			releaseFieldIndexStorageOwned(storage)
			posting.ReleaseSliceOwned(ref.chunk.posts)
		})
	}
}

func TestApplySingleFieldPostingDiffChunked_RebalancesOversizedDirectoryPagesAfterSplit(t *testing.T) {
	refs := make([]fieldIndexChunkRef, 0, fieldIndexDirPageTargetRefs)
	for i := 0; i < fieldIndexDirPageTargetRefs-1; i++ {
		key := uint64(i * 4)
		ids := fieldStorageOwnedTestPosting(uint64(i + 1))
		chunk := &fieldIndexChunk{
			posts:   []posting.List{ids},
			numeric: []uint64{key},
			rows:    ids.Cardinality(),
		}
		refs = append(refs, fieldIndexChunkRef{last: chunk.keyAt(0), chunk: chunk})
	}

	const splitChunkBase = uint64(1 << 32)
	splitKeys := make([]uint64, fieldIndexChunkMaxEntries)
	splitPosts := make([]posting.List, fieldIndexChunkMaxEntries)
	for i := range splitKeys {
		splitKeys[i] = splitChunkBase + uint64(i*4)
		splitPosts[i] = fieldStorageOwnedTestPosting(uint64(10_000 + i))
	}
	splitChunk := &fieldIndexChunk{
		posts:   splitPosts,
		numeric: splitKeys,
		rows:    postingRows(splitPosts),
	}
	refs = append(refs, fieldIndexChunkRef{
		last:  splitChunk.keyAt(splitChunk.keyCount() - 1),
		chunk: splitChunk,
	})

	root := newFieldIndexChunkedRootFromPages([]*fieldIndexChunkDirPage{newFieldIndexChunkDirPage(refs)})
	if root == nil {
		t.Fatalf("expected chunked root")
	}
	if root.pages.Len() != 1 || root.pages.Get(0).refsLen() != fieldIndexDirPageTargetRefs {
		t.Fatalf("expected exactly one full directory page, got pages=%d refs=%d", root.pages.Len(), root.pages.Get(0).refsLen())
	}

	insertKey := indexKeyFromU64(splitChunkBase + 2)
	got := applySingleFieldPostingDiffChunked(root, keyedBatchPostingDelta{
		key: insertKey,
		delta: batchPostingDelta{
			add: fieldStorageSingleton(999_999),
		},
	})
	if !got.isChunked() {
		t.Fatalf("expected chunked storage after split")
	}
	if got.chunked.chunkCount != fieldIndexDirPageTargetRefs+1 {
		t.Fatalf("unexpected chunk count after split: got=%d want=%d", got.chunked.chunkCount, fieldIndexDirPageTargetRefs+1)
	}
	if got.chunked.pages.Len() < 2 {
		t.Fatalf("expected split to rebalance into multiple pages, got %d", got.chunked.pages.Len())
	}
	for i := 0; i < got.chunked.pages.Len(); i++ {
		if got.chunked.pages.Get(i).refsLen() > fieldIndexDirPageTargetRefs {
			t.Fatalf("page %d exceeds ref bound: got=%d want<=%d", i, got.chunked.pages.Get(i).refsLen(), fieldIndexDirPageTargetRefs)
		}
	}

	baseEntries := flattenChunkedFieldIndexRoot(root)
	if baseEntries == nil {
		t.Fatalf("expected flattened base entries")
	}
	want := applySingleFieldPostingDiffSorted(baseEntries, keyedBatchPostingDelta{
		key: insertKey,
		delta: batchPostingDelta{
			add: fieldStorageSingleton(999_999),
		},
	})
	if want == nil {
		t.Fatalf("expected non-empty expected entries")
	}
	indexExtAssertStorageMatchesEntries(t, got, *want)
}
