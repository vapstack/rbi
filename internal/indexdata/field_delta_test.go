package indexdata

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

func newPostingDelta(key keycodec.IndexKey, add, remove posting.List) PostingDelta {
	return PostingDelta{Key: key, Delta: BatchPostingDelta{Add: add, Remove: remove}}
}

func TestApplyFieldPostingDiffChunked_StringOwnerChunkKeepsStringLayout(t *testing.T) {
	entries := make([]Entry, fieldIndexChunkThreshold)
	for i := range entries {
		entries[i] = Entry{
			Key: keycodec.FromStoredString(fmt.Sprintf("k%04d", i), false),
			IDs: fieldStorageSingleton(uint64(i + 1)),
		}
	}

	root := buildChunkedFieldIndexRoot(entries)
	if root == nil {
		t.Fatalf("expected chunked root")
	}
	defer root.release()

	ref, ok := root.refAtChunk(0)
	if !ok || ref.chunk == nil || !ref.chunk.hasUniqueStringOwners() {
		t.Fatalf("expected owner layout in first chunk")
	}

	oldKey := "k0095"
	newKey := "k0095a"
	deltas := []PostingDelta{
		{
			Key: keycodec.FromStoredString(oldKey, false),
			Delta: BatchPostingDelta{
				Remove: fieldStorageSingleton(96),
			},
		},
		{
			Key: keycodec.FromStoredString(newKey, false),
			Delta: BatchPostingDelta{
				Add: fieldStorageSingleton(900_001),
			},
		},
	}

	storage := root.applyPostingDiff(deltas)
	defer storage.Release()

	if storage.chunked == nil {
		t.Fatalf("expected chunked storage")
	}
	repl, ok := storage.chunked.refAtChunk(0)
	if !ok || repl.chunk == nil {
		t.Fatalf("expected replacement first chunk")
	}
	if !repl.chunk.hasUniqueStringOwners() {
		t.Fatalf("expected owner layout to be preserved for singleton result chunk")
	}
	if ids := storage.chunked.lookupPostingRetained(oldKey); !ids.IsEmpty() {
		t.Fatalf("expected old key to be removed, got %v", ids)
	}
	ids := storage.chunked.lookupPostingRetained(newKey)
	if ids.Cardinality() != 1 || !ids.Contains(900_001) {
		t.Fatalf("unexpected new key posting: %v", ids)
	}
	if ids := storage.chunked.lookupPostingRetained("k0096"); ids.Cardinality() != 1 || !ids.Contains(97) {
		t.Fatalf("neighbor key lookup broken: %v", ids)
	}
}

func TestApplyFieldPostingDiffChunked_NumericOwnerChunkKeepsNumericLayout(t *testing.T) {
	entries := make([]Entry, fieldIndexChunkThreshold)
	for i := range entries {
		entries[i] = Entry{
			Key: keycodec.FromU64(uint64(i * 2)),
			IDs: fieldStorageSingleton(uint64(i + 1)),
		}
	}

	root := buildChunkedFieldIndexRoot(entries)
	if root == nil {
		t.Fatalf("expected chunked root")
	}
	defer root.release()

	ref, ok := root.refAtChunk(0)
	if !ok || ref.chunk == nil || !ref.chunk.hasUniqueNumericOwners() {
		t.Fatalf("expected owner layout in first numeric chunk")
	}

	oldKey := uint64(95 * 2)
	newKey := oldKey + 1
	deltas := []PostingDelta{
		{
			Key: keycodec.FromU64(oldKey),
			Delta: BatchPostingDelta{
				Remove: fieldStorageSingleton(96),
			},
		},
		{
			Key: keycodec.FromU64(newKey),
			Delta: BatchPostingDelta{
				Add: fieldStorageSingleton(900_001),
			},
		},
	}

	storage := root.applyPostingDiff(deltas)
	defer storage.Release()

	if storage.chunked == nil {
		t.Fatalf("expected chunked storage")
	}
	repl, ok := storage.chunked.refAtChunk(0)
	if !ok || repl.chunk == nil {
		t.Fatalf("expected replacement first chunk")
	}
	if !repl.chunk.hasUniqueNumericOwners() {
		t.Fatalf("expected owner layout to be preserved for singleton result numeric chunk")
	}
	if ids := storage.chunked.lookupPostingRetained(keycodec.U64ByteString(oldKey)); !ids.IsEmpty() {
		t.Fatalf("expected old key to be removed, got %v", ids)
	}
	ids := storage.chunked.lookupPostingRetained(keycodec.U64ByteString(newKey))
	if ids.Cardinality() != 1 || !ids.Contains(900_001) {
		t.Fatalf("unexpected new key posting: %v", ids)
	}
	if ids := storage.chunked.lookupPostingRetained(keycodec.U64ByteString(oldKey + 2)); ids.Cardinality() != 1 || !ids.Contains(97) {
		t.Fatalf("neighbor key lookup broken: %v", ids)
	}
}

func TestApplySingleFieldPostingDiffChunked_StringOwnerChunkDemotesOnMultiPosting(t *testing.T) {
	entries := make([]Entry, fieldIndexChunkThreshold)
	for i := range entries {
		entries[i] = Entry{
			Key: keycodec.FromStoredString(fmt.Sprintf("k%04d", i), false),
			IDs: fieldStorageSingleton(uint64(i + 1)),
		}
	}

	root := buildChunkedFieldIndexRoot(entries)
	if root == nil {
		t.Fatalf("expected chunked root")
	}
	defer root.release()

	storage := root.applySinglePostingDiff(PostingDelta{
		Key: keycodec.FromStoredString("k0095", false),
		Delta: BatchPostingDelta{
			Add: fieldStorageSingleton(900_002),
		},
	})
	defer storage.Release()

	if storage.chunked == nil {
		t.Fatalf("expected chunked storage")
	}
	repl, ok := storage.chunked.refAtChunk(0)
	if !ok || repl.chunk == nil {
		t.Fatalf("expected replacement first chunk")
	}
	if !repl.chunk.hasStringKeys() {
		t.Fatalf("expected string chunk after update")
	}
	if repl.chunk.hasUniqueStringOwners() {
		t.Fatalf("expected owner layout to be dropped after multi-posting update")
	}
	ids := storage.chunked.lookupPostingRetained("k0095")
	if ids.Cardinality() != 2 || !ids.Contains(96) || !ids.Contains(900_002) {
		t.Fatalf("unexpected expanded posting: %v", ids)
	}
	if got := repl.chunk.keyAt(95).UnsafeString(); got != "k0095" {
		t.Fatalf("unexpected updated Key: got %q want %q", got, "k0095")
	}
}

func TestApplySingleFieldPostingDiffChunked_NumericOwnerChunkDemotesOnMultiPosting(t *testing.T) {
	entries := make([]Entry, fieldIndexChunkThreshold)
	for i := range entries {
		entries[i] = Entry{
			Key: keycodec.FromU64(uint64(i * 2)),
			IDs: fieldStorageSingleton(uint64(i + 1)),
		}
	}

	root := buildChunkedFieldIndexRoot(entries)
	if root == nil {
		t.Fatalf("expected chunked root")
	}
	defer root.release()

	storage := root.applySinglePostingDiff(PostingDelta{
		Key: keycodec.FromU64(uint64(95 * 2)),
		Delta: BatchPostingDelta{
			Add: fieldStorageSingleton(900_002),
		},
	})
	defer storage.Release()

	if storage.chunked == nil {
		t.Fatalf("expected chunked storage")
	}
	repl, ok := storage.chunked.refAtChunk(0)
	if !ok || repl.chunk == nil {
		t.Fatalf("expected replacement first chunk")
	}
	if !repl.chunk.hasNumericKeys() {
		t.Fatalf("expected numeric chunk after update")
	}
	if repl.chunk.hasUniqueNumericOwners() {
		t.Fatalf("expected owner layout to be dropped after multi-posting numeric update")
	}
	ids := storage.chunked.lookupPostingRetained(keycodec.U64ByteString(uint64(95 * 2)))
	if ids.Cardinality() != 2 || !ids.Contains(96) || !ids.Contains(900_002) {
		t.Fatalf("unexpected expanded posting: %v", ids)
	}
	if got := repl.chunk.keyAt(95).U64(); got != uint64(95*2) {
		t.Fatalf("unexpected updated Key: got %d want %d", got, uint64(95*2))
	}
}

func TestApplyBatchPostingDeltaOwned_ReusesAddWhenRemoveEmptiesBase(t *testing.T) {
	delta := BatchPostingDelta{
		Remove: fieldStorageSingleton(1),
		Add:    fieldStorageSingleton(2).BuildAdded(3),
	}

	out := delta.applyOwned(fieldStorageSingleton(1))
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
	if !delta.Add.IsEmpty() || !delta.Remove.IsEmpty() {
		t.Fatalf("expected delta buffers to be consumed")
	}
}

func TestApplyBatchPostingDeltaOwned_DetachesBorrowedBaseConcurrent(t *testing.T) {
	var base posting.List
	for i := uint64(1); i <= 48; i++ {
		base = base.BuildAdded(i * 2)
	}
	defer base.Release()
	wantBase := base.ToArray()

	removeIDs := []uint64{wantBase[0], wantBase[1], wantBase[2], wantBase[3]}
	addIDs := []uint64{1<<32 | 5, 1<<32 | 9, 2<<32 | 11}

	expected := make([]uint64, 0, len(wantBase))
	for _, id := range wantBase {
		drop := false
		for _, removeID := range removeIDs {
			if id == removeID {
				drop = true
				break
			}
		}
		if !drop {
			expected = append(expected, id)
		}
	}
	expected = append(expected, addIDs...)
	slices.Sort(expected)

	var failed atomic.Pointer[string]
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				delta := BatchPostingDelta{
					Remove: fieldStoragePosting(removeIDs...),
					Add:    fieldStoragePosting(addIDs...),
				}
				out := delta.applyOwned(base.Borrow())
				got := out.ToArray()
				if !slices.Equal(got, expected) {
					msg := fmt.Sprintf("delta result mismatch: got=%v want=%v", got, expected)
					failed.CompareAndSwap(nil, &msg)
					out.Release()
					return
				}
				if !delta.Add.IsEmpty() || !delta.Remove.IsEmpty() {
					msg := "ApplyBatchPostingDeltaOwned must consume delta buffers"
					failed.CompareAndSwap(nil, &msg)
					out.Release()
					return
				}
				out.Release()
			}
		}()
	}
	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}
	if !slices.Equal(base.ToArray(), wantBase) {
		t.Fatalf("borrowed base was mutated")
	}
}

func TestSortPostingDeltasBufOrdersByKey(t *testing.T) {
	buf := GetPostingDeltaSlice(3)
	buf = append(buf, newPostingDelta(keycodec.FromU64(6), fieldStorageSingleton(3), posting.List{}))
	buf = append(buf, newPostingDelta(keycodec.FromU64(2), fieldStorageSingleton(1), posting.List{}))
	buf = append(buf, newPostingDelta(keycodec.FromU64(4), fieldStorageSingleton(2), posting.List{}))
	defer PutPostingDeltaSlice(buf)

	sortPostingDeltasBuf(buf)
	for i, want := range []uint64{2, 4, 6} {
		if got := buf[i].Key.U64(); got != want {
			t.Fatalf("delta %d key: got %d want %d", i, got, want)
		}
	}
	for i := range buf {
		buf[i].Delta.Add.Release()
	}
}

func TestApplyFieldPostingDiffStorage_NoOpFlatRemoveReturnsBase(t *testing.T) {
	entries := []Entry{{
		Key: keycodec.FromStoredString("a", false),
		IDs: fieldStorageOwnedTestPosting(1),
	}}
	storage := newFlatFieldStorage(&entries)
	defer storage.Release()

	missing := keycodec.FromStoredString("b", false)
	delta := PostingDelta{
		Key: missing,
		Delta: BatchPostingDelta{
			Remove: fieldStorageSingleton(99),
		},
	}
	if got := storage.applyPostingDiff([]PostingDelta{delta}, true); got != storage {
		got.Release()
		t.Fatalf("chunk-allowed no-op remove returned a new storage")
	}

	delta = PostingDelta{
		Key: missing,
		Delta: BatchPostingDelta{
			Remove: fieldStorageSingleton(100),
		},
	}
	if got := storage.applyPostingDiff([]PostingDelta{delta}, false); got != storage {
		got.Release()
		t.Fatalf("flat-only no-op remove returned a new storage")
	}

	buf := GetPostingDeltaSlice(0)
	buf = append(buf, PostingDelta{
		Key: missing,
		Delta: BatchPostingDelta{
			Remove: fieldStorageSingleton(101),
		},
	})
	if got := storage.applyPostingDiffBufOwned(buf, true); got != storage {
		got.Release()
		t.Fatalf("buffered no-op remove returned a new storage")
	}
}

func TestFieldStorageApplyPostingDiff_EmptyExistingDeltaDoesNotSharePayload(t *testing.T) {
	entries := []Entry{{
		Key: keycodec.FromStoredString("a", false),
		IDs: fieldStorageOwnedTestPosting(1),
	}}
	base := newFlatFieldStorage(&entries)
	defer base.Release()

	deltas := []PostingDelta{
		{Key: keycodec.FromStoredString("a", false)},
		{
			Key: keycodec.FromStoredString("b", false),
			Delta: BatchPostingDelta{
				Add: fieldStorageOwnedTestPosting(2),
			},
		},
	}
	got := base.applyPostingDiff(deltas, false)
	defer got.Release()
	if got == base {
		t.Fatalf("expected inserted key to create new storage")
	}
	baseOV := NewFieldOverlayStorage(base)
	gotOV := NewFieldOverlayStorage(got)
	if baseOV.KeyCount() != 1 || gotOV.KeyCount() != 2 {
		t.Fatalf("unexpected flat storage state")
	}
	if gotOV.PostingAt(0).SharesPayload(baseOV.PostingAt(0)) {
		t.Fatalf("unchanged existing posting payload is shared by two flat storages")
	}
}

func TestFieldStorageApplyPostingDiffBufOwned_ChunkedMatchesUnbuffered(t *testing.T) {
	entries := fieldStorageEntriesForTest(fieldIndexChunkThreshold+37, true)
	base := newRegularFieldStorage(&entries)
	defer base.Release()
	if !base.IsChunked() {
		t.Fatalf("expected chunked base")
	}

	unbuffered := base.applyPostingDiff([]PostingDelta{
		{
			Key: keycodec.FromU64(20),
			Delta: BatchPostingDelta{
				Remove: fieldStorageSingleton(11),
			},
		},
		{
			Key: keycodec.FromU64(40),
			Delta: BatchPostingDelta{
				Add: fieldStorageSingleton(900_040),
			},
		},
		{
			Key: keycodec.FromU64(61),
			Delta: BatchPostingDelta{
				Add: fieldStorageSingleton(900_061),
			},
		},
	}, true)
	defer unbuffered.Release()

	buf := GetPostingDeltaSlice(3)
	buf = append(buf, PostingDelta{
		Key: keycodec.FromU64(20),
		Delta: BatchPostingDelta{
			Remove: fieldStorageSingleton(11),
		},
	})
	buf = append(buf, PostingDelta{
		Key: keycodec.FromU64(40),
		Delta: BatchPostingDelta{
			Add: fieldStorageSingleton(900_040),
		},
	})
	buf = append(buf, PostingDelta{
		Key: keycodec.FromU64(61),
		Delta: BatchPostingDelta{
			Add: fieldStorageSingleton(900_061),
		},
	})
	buffered := base.applyPostingDiffBufOwned(buf, true)
	defer buffered.Release()

	want := unbuffered.chunked.flatten()
	if want == nil {
		t.Fatalf("expected flattened unbuffered storage")
	}
	fieldStorageAssertStorageMatchesEntries(t, buffered, *want)
}

func TestFieldStorageMergeStringPostingAddsOwned_SortsAndDeduplicates(t *testing.T) {
	var arena *PostingAddArena
	adds := AddStringPostingAdd(nil, &arena, "b", 3, 8)
	adds = AddStringPostingAdd(adds, &arena, "a", 2, 8)
	adds = AddStringPostingAdd(adds, &arena, "a", 2, 8)
	defer arena.Release()

	storage := FieldStorage{}.MergeStringPostingAddsOwned(adds, arena, false, false)
	defer storage.Release()

	ov := NewFieldOverlayStorage(storage)
	if ov.KeyCount() != 2 {
		t.Fatalf("unexpected key count: got %d want 2", ov.KeyCount())
	}
	if got := ov.KeyAt(0).UnsafeString(); got != "a" {
		t.Fatalf("key 0: got %q want a", got)
	}
	if got := ov.KeyAt(1).UnsafeString(); got != "b" {
		t.Fatalf("key 1: got %q want b", got)
	}
	fieldStorageAssertPostingContains(t, storage, "a", 2)
	fieldStorageAssertPostingContains(t, storage, "b", 3)
}

func TestFieldStorageMergeFixedPostingAddsOwned_SortsAndDeduplicates(t *testing.T) {
	var arena *PostingAddArena
	adds := AddFixedPostingAdd(nil, &arena, 6, 3, 8)
	adds = AddFixedPostingAdd(adds, &arena, 2, 1, 8)
	adds = AddFixedPostingAdd(adds, &arena, 2, 1, 8)
	defer arena.Release()

	storage := FieldStorage{}.MergeFixedPostingAddsOwned(adds, arena, false)
	defer storage.Release()

	ov := NewFieldOverlayStorage(storage)
	if ov.KeyCount() != 2 {
		t.Fatalf("unexpected key count: got %d want 2", ov.KeyCount())
	}
	if got := ov.KeyAt(0).U64(); got != 2 {
		t.Fatalf("key 0: got %d want 2", got)
	}
	if got := ov.KeyAt(1).U64(); got != 6 {
		t.Fatalf("key 1: got %d want 6", got)
	}
	fieldStorageAssertPostingContains(t, storage, keycodec.U64ByteString(2), 1)
	fieldStorageAssertPostingContains(t, storage, keycodec.U64ByteString(6), 3)
}

func TestFieldStorageApplyStringPostingDiffOwned_UpdatesAndRemoves(t *testing.T) {
	entries := []Entry{
		{Key: keycodec.FromStoredString("a", false), IDs: fieldStorageSingleton(1)},
		{Key: keycodec.FromStoredString("b", false), IDs: fieldStorageSingleton(2)},
	}
	base := newRegularFieldStorage(&entries)
	defer base.Release()

	var arena *PostingDiffArena
	deltas := AddStringPostingDiff(nil, &arena, "a", 1, false, 8)
	deltas = AddStringPostingDiff(deltas, &arena, "b", 9, true, 8)
	deltas = AddStringPostingDiff(deltas, &arena, "c", 3, true, 8)
	defer arena.Release()

	storage := base.ApplyStringPostingDiffOwned(deltas, arena, false, false)
	defer storage.Release()

	ov := NewFieldOverlayStorage(storage)
	if ov.LookupCardinality("a") != 0 {
		t.Fatalf("removed string key still has posting")
	}
	fieldStorageAssertPostingContains(t, storage, "b", 2, 9)
	fieldStorageAssertPostingContains(t, storage, "c", 3)
}

func TestFieldStorageApplyFixedPostingDiffOwned_UpdatesAndRemoves(t *testing.T) {
	entries := []Entry{
		{Key: keycodec.FromU64(2), IDs: fieldStorageSingleton(1)},
		{Key: keycodec.FromU64(4), IDs: fieldStorageSingleton(2)},
	}
	base := newRegularFieldStorage(&entries)
	defer base.Release()

	var arena *PostingDiffArena
	deltas := AddFixedPostingDiff(nil, &arena, 2, 1, false, 8)
	deltas = AddFixedPostingDiff(deltas, &arena, 4, 9, true, 8)
	deltas = AddFixedPostingDiff(deltas, &arena, 6, 3, true, 8)
	defer arena.Release()

	storage := base.ApplyFixedPostingDiffOwned(deltas, arena, false)
	defer storage.Release()

	ov := NewFieldOverlayStorage(storage)
	if ov.LookupCardinality(keycodec.U64ByteString(2)) != 0 {
		t.Fatalf("removed fixed key still has posting")
	}
	fieldStorageAssertPostingContains(t, storage, keycodec.U64ByteString(4), 2, 9)
	fieldStorageAssertPostingContains(t, storage, keycodec.U64ByteString(6), 3)
}

func TestFieldStorageApplyLenPostingDiffOwned_UpdatesNonEmptyMarker(t *testing.T) {
	entries := []Entry{
		{Key: keycodec.FromU64(1), IDs: fieldStorageSingleton(1)},
		{Key: keycodec.FromString(LenIndexNonEmptyKey), IDs: fieldStorageSingleton(1)},
	}
	base := newRegularFieldStorage(&entries)
	defer base.Release()

	var deltas *LenPostingDiff
	AddLenPostingBucketDiff(&deltas, 1, 1, false)
	AddLenPostingBucketDiff(&deltas, 1, 2, true)
	AddLenPostingNonEmptyDiff(&deltas, 1, false)
	AddLenPostingNonEmptyDiff(&deltas, 1, true)

	storage := base.ApplyLenPostingDiffOwned(deltas)
	defer storage.Release()

	ov := NewFieldOverlayStorage(storage)
	if ov.LookupCardinality(keycodec.U64ByteString(1)) != 0 {
		t.Fatalf("old len bucket still has posting")
	}
	fieldStorageAssertPostingContains(t, storage, keycodec.U64ByteString(2), 1)
	fieldStorageAssertPostingContains(t, storage, LenIndexNonEmptyKey, 1)
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

			storage := root.applySinglePostingDiff(PostingDelta{
				Key: key,
				Delta: BatchPostingDelta{
					Add: fieldStorageSingleton(addedID),
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

			storage.Release()
			posting.ReleaseAll(ref.chunk.posts)
		})
	}
}

func TestApplySingleFieldPostingDiffChunked_StringFastPathRefreshesRefLastOwner(t *testing.T) {
	ref := fieldStorageOwnedChunkRef(fieldIndexChunkTargetEntries, false)
	root := fieldStorageSingleChunkRoot(ref)
	pos := fieldIndexChunkTargetEntries / 2
	oldBytes := len(ref.chunk.stringData)

	storage := root.applySinglePostingDiff(PostingDelta{
		Key: ref.chunk.keyAt(pos),
		Delta: BatchPostingDelta{
			Add: fieldStorageSingleton(1_000_000),
		},
	})
	if storage.chunked == nil {
		t.Fatalf("expected chunked storage")
	}

	root.release()
	reuse := fieldByteSlice(oldBytes)
	for i := range reuse {
		reuse[i] = '!'
	}
	defer pooled.PutByteSlice(reuse)
	defer storage.Release()

	replRef, ok := storage.chunked.refAtChunk(0)
	if !ok || replRef.chunk == nil {
		t.Fatalf("expected replacement chunk")
	}
	last := replRef.chunk.keyAt(replRef.chunk.keyCount() - 1)
	if keycodec.Compare(replRef.last, last) != 0 {
		t.Fatalf("stale chunk last ref: got %q want %q", replRef.last.UnsafeString(), last.UnsafeString())
	}
}

func TestApplySingleFieldPostingDiffChunked_DetachesEntryPrefixMetadata(t *testing.T) {
	root := fieldStorageTwoChunkNumericRoot()
	if root == nil {
		t.Fatal("expected chunked root")
	}
	defer root.release()

	baseFirstChunkKeys := root.entryPrefixForChunk(1)
	baseTotalKeys := root.keyCount

	out := root.applySinglePostingDiff(PostingDelta{
		Key: fieldStorageInsertedTestKey(17, true),
		Delta: BatchPostingDelta{
			Add: fieldStorageSingleton(1<<32 | 17),
		},
	})
	defer out.Release()

	if out.chunked == nil {
		t.Fatal("expected chunked storage after insert")
	}
	if got := root.entryPrefixForChunk(1); got != baseFirstChunkKeys {
		t.Fatalf("base root first-chunk prefix changed after insert: got=%d want=%d", got, baseFirstChunkKeys)
	}
	if got := root.keyCount; got != baseTotalKeys {
		t.Fatalf("base root keyCount changed after insert: got=%d want=%d", got, baseTotalKeys)
	}
	if got := out.chunked.entryPrefixForChunk(1); got != baseFirstChunkKeys+1 {
		t.Fatalf("new root first-chunk prefix mismatch after insert: got=%d want=%d", got, baseFirstChunkKeys+1)
	}
	if got := out.chunked.keyCount; got != baseTotalKeys+1 {
		t.Fatalf("new root keyCount mismatch after insert: got=%d want=%d", got, baseTotalKeys+1)
	}
}

func TestApplySingleFieldPostingDiffChunked_DetachesRowPrefixMetadata(t *testing.T) {
	root := fieldStorageTwoChunkNumericRoot()
	if root == nil {
		t.Fatal("expected chunked root")
	}
	defer root.release()

	ref, ok := root.refAtChunk(0)
	if !ok || ref.chunk == nil {
		t.Fatal("expected first chunk ref")
	}

	baseFirstChunkRows := root.chunkRowsRange(0, 1)
	baseTotalRows := root.chunkRowsRange(0, root.chunkCount)
	key := ref.chunk.keyAt(fieldIndexChunkTargetEntries / 2)

	out := root.applySinglePostingDiff(PostingDelta{
		Key: key,
		Delta: BatchPostingDelta{
			Add: fieldStorageSingleton(1<<32 | 33),
		},
	})
	defer out.Release()

	if out.chunked == nil {
		t.Fatal("expected chunked storage after posting update")
	}
	if got := root.chunkRowsRange(0, 1); got != baseFirstChunkRows {
		t.Fatalf("base root first-chunk rows changed after update: got=%d want=%d", got, baseFirstChunkRows)
	}
	if got := root.chunkRowsRange(0, root.chunkCount); got != baseTotalRows {
		t.Fatalf("base root total rows changed after update: got=%d want=%d", got, baseTotalRows)
	}
	if got := out.chunked.chunkRowsRange(0, 1); got != baseFirstChunkRows+1 {
		t.Fatalf("new root first-chunk rows mismatch after update: got=%d want=%d", got, baseFirstChunkRows+1)
	}
	if got := out.chunked.chunkRowsRange(0, out.chunked.chunkCount); got != baseTotalRows+1 {
		t.Fatalf("new root total rows mismatch after update: got=%d want=%d", got, baseTotalRows+1)
	}
}

func TestApplySingleFieldPostingDiffChunked_FullPageSplitDoesNotMutateBaseMetadata(t *testing.T) {
	root := fieldStorageFullPageSplitNumericRoot()
	if root == nil {
		t.Fatal("expected chunked root")
	}
	defer root.release()

	basePages := len(root.pages)
	baseRefs := len(root.pages[0].refs)
	baseChunks := root.chunkCount
	baseKeys := root.keyCount
	baseRows := root.chunkRowsRange(0, root.chunkCount)
	baseLastChunkStart := root.entryPrefixForChunk(root.chunkCount - 1)

	out := root.applySinglePostingDiff(PostingDelta{
		Key: keycodec.FromU64((1 << 32) + 2),
		Delta: BatchPostingDelta{
			Add: fieldStorageSingleton(17 << 32),
		},
	})
	defer out.Release()

	if out.chunked == nil {
		t.Fatal("expected chunked storage after full-page split insert")
	}
	if len(root.pages) != basePages {
		t.Fatalf("base root page count changed after split rebuild: got=%d want=%d", len(root.pages), basePages)
	}
	if len(root.pages[0].refs) != baseRefs {
		t.Fatalf("base root ref count changed after split rebuild: got=%d want=%d", len(root.pages[0].refs), baseRefs)
	}
	if got := root.chunkCount; got != baseChunks {
		t.Fatalf("base root chunkCount changed after split rebuild: got=%d want=%d", got, baseChunks)
	}
	if got := root.keyCount; got != baseKeys {
		t.Fatalf("base root keyCount changed after split rebuild: got=%d want=%d", got, baseKeys)
	}
	if got := root.chunkRowsRange(0, root.chunkCount); got != baseRows {
		t.Fatalf("base root total rows changed after split rebuild: got=%d want=%d", got, baseRows)
	}
	if got := root.entryPrefixForChunk(root.chunkCount - 1); got != baseLastChunkStart {
		t.Fatalf("base root last chunk prefix changed after split rebuild: got=%d want=%d", got, baseLastChunkStart)
	}
	if got := out.chunked.chunkCount; got != baseChunks+1 {
		t.Fatalf("new root chunkCount mismatch after split rebuild: got=%d want=%d", got, baseChunks+1)
	}
	if got := out.chunked.keyCount; got != baseKeys+1 {
		t.Fatalf("new root keyCount mismatch after split rebuild: got=%d want=%d", got, baseKeys+1)
	}
	if got := out.chunked.chunkRowsRange(0, out.chunked.chunkCount); got != baseRows+1 {
		t.Fatalf("new root total rows mismatch after split rebuild: got=%d want=%d", got, baseRows+1)
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
		chunk.refs.Store(1)
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
	splitChunk.refs.Store(1)
	refs = append(refs, fieldIndexChunkRef{
		last:  splitChunk.keyAt(splitChunk.keyCount() - 1),
		chunk: splitChunk,
	})

	root := newFieldIndexChunkedRootFromPages([]*fieldIndexChunkDirPage{newFieldIndexChunkDirPage(refs)})
	if root == nil {
		t.Fatalf("expected chunked root")
	}
	defer root.release()
	if len(root.pages) != 1 || len(root.pages[0].refs) != fieldIndexDirPageTargetRefs {
		t.Fatalf("expected exactly one full directory page, got pages=%d refs=%d", len(root.pages), len(root.pages[0].refs))
	}

	insertKey := keycodec.FromU64(splitChunkBase + 2)
	got := root.applySinglePostingDiff(PostingDelta{
		Key: insertKey,
		Delta: BatchPostingDelta{
			Add: fieldStorageSingleton(999_999),
		},
	})
	if !got.IsChunked() {
		t.Fatalf("expected chunked storage after split")
	}
	defer got.Release()
	if got.chunked.chunkCount != fieldIndexDirPageTargetRefs+1 {
		t.Fatalf("unexpected chunk count after split: got=%d want=%d", got.chunked.chunkCount, fieldIndexDirPageTargetRefs+1)
	}
	if len(got.chunked.pages) < 2 {
		t.Fatalf("expected split to rebalance into multiple pages, got %d", len(got.chunked.pages))
	}
	for i := 0; i < len(got.chunked.pages); i++ {
		if len(got.chunked.pages[i].refs) > fieldIndexDirPageTargetRefs {
			t.Fatalf("page %d exceeds ref bound: got=%d want<=%d", i, len(got.chunked.pages[i].refs), fieldIndexDirPageTargetRefs)
		}
	}

	baseEntries := root.flatten()
	if baseEntries == nil {
		t.Fatalf("expected flattened base entries")
	}
	want := applySingleFieldPostingDiffSorted(baseEntries, PostingDelta{
		Key: insertKey,
		Delta: BatchPostingDelta{
			Add: fieldStorageSingleton(999_999),
		},
	})
	if want == nil {
		t.Fatalf("expected non-empty expected entries")
	}
	fieldStorageAssertStorageMatchesEntries(t, got, *want)
}
