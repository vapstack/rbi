package indexdata

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
)

func newPostingDelta(key keycodec.IndexKey, add, remove posting.List) PostingDelta {
	return PostingDelta{Key: key, Delta: BatchPostingDelta{Add: add, Remove: remove}}
}

func TestApplyFieldPostingDiffChunked_StringOwnerChunkKeepsStringLayout(t *testing.T) {
	entries := make([]Entry, fieldIndexChunkThreshold)
	for i := range entries {
		entries[i] = Entry{
			Key: keycodec.FromString(fmt.Sprintf("k%04d", i)),
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
			Key: keycodec.FromString(oldKey),
			Delta: BatchPostingDelta{
				Remove: fieldStorageSingleton(96),
			},
		},
		{
			Key: keycodec.FromString(newKey),
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
	if ids := storage.chunked.lookupPostingRetainedKey(keycodec.FromU64(oldKey)); !ids.IsEmpty() {
		t.Fatalf("expected old key to be removed, got %v", ids)
	}
	ids := storage.chunked.lookupPostingRetainedKey(keycodec.FromU64(newKey))
	if ids.Cardinality() != 1 || !ids.Contains(900_001) {
		t.Fatalf("unexpected new key posting: %v", ids)
	}
	if ids := storage.chunked.lookupPostingRetainedKey(keycodec.FromU64(oldKey + 2)); ids.Cardinality() != 1 || !ids.Contains(97) {
		t.Fatalf("neighbor key lookup broken: %v", ids)
	}
}

func TestApplySingleFieldPostingDiffChunked_StringOwnerChunkDemotesOnMultiPosting(t *testing.T) {
	entries := make([]Entry, fieldIndexChunkThreshold)
	for i := range entries {
		entries[i] = Entry{
			Key: keycodec.FromString(fmt.Sprintf("k%04d", i)),
			IDs: fieldStorageSingleton(uint64(i + 1)),
		}
	}

	root := buildChunkedFieldIndexRoot(entries)
	if root == nil {
		t.Fatalf("expected chunked root")
	}
	defer root.release()

	storage := root.applySinglePostingDiff(PostingDelta{
		Key: keycodec.FromString("k0095"),
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
	ids := storage.chunked.lookupPostingRetainedKey(keycodec.FromU64(uint64(95 * 2)))
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
	defer ReleasePostingDeltaSlice(buf)

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
		Key: keycodec.FromString("a"),
		IDs: fieldStorageOwnedTestPosting(1),
	}}
	storage := newFlatFieldStorage(entries, nil)
	defer storage.Release()

	missing := keycodec.FromString("b")
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
		Key: keycodec.FromString("a"),
		IDs: fieldStorageOwnedTestPosting(1),
	}}
	base := newFlatFieldStorage(entries, nil)
	defer base.Release()

	deltas := []PostingDelta{
		{Key: keycodec.FromString("a")},
		{
			Key: keycodec.FromString("b"),
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
	baseOV := NewFieldIndexViewFromStorage(base)
	gotOV := NewFieldIndexViewFromStorage(got)
	if baseOV.KeyCount() != 1 || gotOV.KeyCount() != 2 {
		t.Fatalf("unexpected flat storage state")
	}
	if gotOV.PostingAt(0).SharesPayload(baseOV.PostingAt(0)) {
		t.Fatalf("unchanged existing posting payload is shared by two flat storages")
	}
}

func TestFieldStorageApplyPostingDiff_FlatStringKeysSurviveBaseRelease(t *testing.T) {
	entries := []Entry{
		{Key: keycodec.FromString("hot"), IDs: fieldStorageSingleton(1)},
		{Key: keycodec.FromString("shared"), IDs: fieldStorageSingleton(2)},
		{Key: keycodec.FromString("tag-076"), IDs: fieldStorageSingleton(3)},
	}
	base := newRegularFieldStorage(entries)
	if base.flat == nil || base.flat.stringData == nil {
		t.Fatalf("expected flat string storage with owned key data")
	}

	next := base.applyPostingDiff([]PostingDelta{{
		Key: keycodec.FromString("tag-076"),
		Delta: BatchPostingDelta{
			Remove: fieldStorageSingleton(3),
		},
	}}, true)
	if next == base {
		next.Release()
		t.Fatalf("expected remove to create new flat storage")
	}

	oldData := base.flat.stringData
	base.Release()
	for i := range oldData {
		oldData[i] = 'x'
	}
	defer next.Release()

	fieldStorageAssertPostingContains(t, next, "hot", 1)
	fieldStorageAssertPostingContains(t, next, "shared", 2)
	if got := NewFieldIndexViewFromStorage(next).LookupCardinality("tag-076"); got != 0 {
		t.Fatalf("removed key still has cardinality %d", got)
	}
}

func TestFieldStorageApplyPostingDiffCopiesBorrowedDeltaKeyBytes(t *testing.T) {
	entries := []Entry{
		{Key: keycodec.FromString("diff-direct/a"), IDs: fieldStorageSingleton(1)},
		{Key: keycodec.FromString("diff-direct/c"), IDs: fieldStorageSingleton(3)},
	}
	base := newRegularFieldStorage(entries)

	key := []byte("diff-direct/b")
	next := base.applyPostingDiff([]PostingDelta{{
		Key: keycodec.FromBytes(key),
		Delta: BatchPostingDelta{
			Add: fieldStorageSingleton(2),
		},
	}}, false)
	if next == base {
		next.Release()
		t.Fatalf("expected insert to create new storage")
	}

	base.Release()
	poisonBytes(key)
	defer next.Release()

	fieldStorageAssertPostingContains(t, next, "diff-direct/a", 1)
	fieldStorageAssertPostingContains(t, next, "diff-direct/b", 2)
	fieldStorageAssertPostingContains(t, next, "diff-direct/c", 3)
}

func TestFieldStorageApplyPostingDiffBufOwnedCopiesBorrowedDeltaKeyBytes(t *testing.T) {
	entries := []Entry{
		{Key: keycodec.FromString("diff-buf/a"), IDs: fieldStorageSingleton(1)},
		{Key: keycodec.FromString("diff-buf/c"), IDs: fieldStorageSingleton(3)},
	}
	base := newRegularFieldStorage(entries)

	key := []byte("diff-buf/b")
	buf := GetPostingDeltaSlice(1)
	buf = append(buf, PostingDelta{
		Key: keycodec.FromBytes(key),
		Delta: BatchPostingDelta{
			Add: fieldStorageSingleton(2),
		},
	})
	next := base.applyPostingDiffBufOwned(buf, false)
	if next == base {
		next.Release()
		t.Fatalf("expected insert to create new storage")
	}

	base.Release()
	poisonBytes(key)
	defer next.Release()

	fieldStorageAssertPostingContains(t, next, "diff-buf/a", 1)
	fieldStorageAssertPostingContains(t, next, "diff-buf/b", 2)
	fieldStorageAssertPostingContains(t, next, "diff-buf/c", 3)
}

func TestFieldStorageApplyPostingDiffChunkedToFlatCopiesStringKeys(t *testing.T) {
	const total = fieldIndexChunkThreshold
	entries := make([]Entry, total)
	for i := range entries {
		entries[i] = Entry{
			Key: keycodec.FromString(fmt.Sprintf("down/%04d", i)),
			IDs: fieldStorageSingleton(uint64(i + 1)),
		}
	}
	base := newRegularFieldStorage(entries)
	if !base.IsChunked() {
		base.Release()
		t.Fatalf("expected chunked base")
	}

	oldData := make([][]byte, 0, base.chunked.chunkCount)
	for i := range base.chunked.pages {
		page := base.chunked.pages[i]
		for j := range page.refs {
			oldData = append(oldData, page.refs[j].chunk.stringData)
		}
	}

	deltas := make([]PostingDelta, 0, total-3)
	for i := 0; i < total; i++ {
		if i == 0 || i == fieldIndexChunkTargetEntries || i == total-1 {
			continue
		}
		deltas = append(deltas, PostingDelta{
			Key: keycodec.FromString(fmt.Sprintf("down/%04d", i)),
			Delta: BatchPostingDelta{
				Remove: fieldStorageSingleton(uint64(i + 1)),
			},
		})
	}
	next := base.applyPostingDiff(deltas, true)
	if next.IsChunked() {
		next.Release()
		base.Release()
		t.Fatalf("expected chunked base to flatten after removals")
	}

	base.Release()
	for i := range oldData {
		for j := range oldData[i] {
			oldData[i][j] = '!'
		}
	}
	defer next.Release()

	fieldStorageAssertPostingContains(t, next, "down/0000", 1)
	fieldStorageAssertPostingContains(t, next, "down/0192", 193)
	fieldStorageAssertPostingContains(t, next, "down/0383", 384)
	if got := NewFieldIndexViewFromStorage(next).LookupCardinality("down/0001"); got != 0 {
		t.Fatalf("removed key still has cardinality %d", got)
	}
}

func TestFieldStorageApplyPostingDiffChunkedChainSurvivesIntermediateReleaseAndPoison(t *testing.T) {
	const total = fieldIndexChunkThreshold + 17
	entries := make([]Entry, total)
	for i := range entries {
		entries[i] = Entry{
			Key: keycodec.FromString(fmt.Sprintf("chain/%04d", i)),
			IDs: fieldStorageSingleton(uint64(i + 1)),
		}
	}

	base := newRegularFieldStorage(entries)
	if base.chunked == nil {
		base.Release()
		t.Fatalf("expected chunked base")
	}

	key := keycodec.FromString("chain/0095")
	baseChunk := base.chunked.touchChunkIndexFrom(0, key)
	if baseChunk < 0 {
		base.Release()
		t.Fatalf("missing base chunk for test key")
	}
	baseRef, ok := base.chunked.refAtChunk(baseChunk)
	if !ok || baseRef.chunk == nil {
		base.Release()
		t.Fatalf("missing base ref for test key")
	}
	baseData := baseRef.chunk.stringData

	mid := base.applyPostingDiff([]PostingDelta{{
		Key: key,
		Delta: BatchPostingDelta{
			Add: fieldStorageSingleton(900_001),
		},
	}}, true)
	if mid.chunked == nil {
		mid.Release()
		base.Release()
		t.Fatalf("expected chunked middle storage")
	}

	base.Release()
	poisonBytes(baseData)

	fieldStorageAssertPostingContains(t, mid, "chain/0095", 96, 900_001)

	midChunk := mid.chunked.touchChunkIndexFrom(0, key)
	if midChunk < 0 {
		mid.Release()
		t.Fatalf("missing middle chunk for test key")
	}
	midRef, ok := mid.chunked.refAtChunk(midChunk)
	if !ok || midRef.chunk == nil {
		mid.Release()
		t.Fatalf("missing middle ref for test key")
	}
	midData := midRef.chunk.stringData

	next := mid.applyPostingDiff([]PostingDelta{{
		Key: key,
		Delta: BatchPostingDelta{
			Add: fieldStorageSingleton(900_002),
		},
	}}, true)
	if next.chunked == nil {
		next.Release()
		mid.Release()
		t.Fatalf("expected chunked final storage")
	}

	mid.Release()
	poisonBytes(midData)
	defer next.Release()

	fieldStorageAssertPostingContains(t, next, "chain/0095", 96, 900_001, 900_002)
	fieldStorageAssertPostingContains(t, next, "chain/0250", 251)
	fieldStorageAssertPostingContains(t, next, "chain/0400", 401)
}

func TestFieldStorageApplyPostingDiffBufOwned_ChunkedMatchesUnbuffered(t *testing.T) {
	entries := fieldStorageEntriesForTest(fieldIndexChunkThreshold+37, true)
	base := newRegularFieldStorage(entries)
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

	want, wantData := unbuffered.chunked.flatten()
	if want == nil {
		t.Fatalf("expected flattened unbuffered storage")
	}
	defer ReleaseFieldEntrySlice(want)
	defer pooled.ReleaseByteSlice(wantData)
	fieldStorageAssertStorageMatchesEntries(t, buffered, want)
}

func TestFieldStorageMergeStringPostingAdds_SortsAndDeduplicates(t *testing.T) {
	var arena PostingAddArena
	adds := AddStringPostingAdd(nil, &arena, "b", 3, 8)
	adds = AddStringPostingAdd(adds, &arena, "a", 2, 8)
	adds = AddStringPostingAdd(adds, &arena, "a", 2, 8)
	defer arena.Reset()

	storage := FieldStorage{}.MergeStringPostingAdds(adds, &arena, false)
	defer storage.Release()

	ov := NewFieldIndexViewFromStorage(storage)
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

func TestFieldStorageMergeStringPostingAddsCopiesCallerKeyBytes(t *testing.T) {
	keyA := []byte("owned-add/a")
	keyB := []byte("owned-add/b")
	var arena PostingAddArena
	adds := AddStringPostingAdd(nil, &arena, fieldStorageMutableString(keyB), 20, 8)
	adds = AddStringPostingAdd(adds, &arena, fieldStorageMutableString(keyA), 10, 8)

	storage := FieldStorage{}.MergeStringPostingAdds(adds, &arena, false)
	arena.Reset()
	poisonBytes(keyA, keyB)
	defer storage.Release()

	fieldStorageAssertPostingContains(t, storage, "owned-add/a", 10)
	fieldStorageAssertPostingContains(t, storage, "owned-add/b", 20)
}

func TestFieldStorageMergeFixedPostingAdds_SortsAndDeduplicates(t *testing.T) {
	var arena PostingAddArena
	adds := AddFixedPostingAdd(nil, &arena, 6, 3, 8)
	adds = AddFixedPostingAdd(adds, &arena, 2, 1, 8)
	adds = AddFixedPostingAdd(adds, &arena, 2, 1, 8)
	defer arena.Reset()

	storage := FieldStorage{}.MergeFixedPostingAdds(adds, &arena, false)
	defer storage.Release()

	ov := NewFieldIndexViewFromStorage(storage)
	if ov.KeyCount() != 2 {
		t.Fatalf("unexpected key count: got %d want 2", ov.KeyCount())
	}
	if got := ov.KeyAt(0).U64(); got != 2 {
		t.Fatalf("key 0: got %d want 2", got)
	}
	if got := ov.KeyAt(1).U64(); got != 6 {
		t.Fatalf("key 1: got %d want 6", got)
	}
	fieldStorageAssertPostingContainsKey(t, storage, keycodec.FromU64(2), 1)
	fieldStorageAssertPostingContainsKey(t, storage, keycodec.FromU64(6), 3)
}

func TestFieldStorageApplyStringPostingDiff_UpdatesAndRemoves(t *testing.T) {
	entries := []Entry{
		{Key: keycodec.FromString("a"), IDs: fieldStorageSingleton(1)},
		{Key: keycodec.FromString("b"), IDs: fieldStorageSingleton(2)},
	}
	base := newRegularFieldStorage(entries)
	defer base.Release()

	var arena PostingDiffArena
	deltas := AddStringPostingDiff(nil, &arena, "a", 1, false)
	deltas = AddStringPostingDiff(deltas, &arena, "b", 9, true)
	deltas = AddStringPostingDiff(deltas, &arena, "c", 3, true)
	defer arena.Reset()

	storage := base.ApplyStringPostingDiff(deltas, &arena, false)
	defer storage.Release()

	ov := NewFieldIndexViewFromStorage(storage)
	if ov.LookupCardinality("a") != 0 {
		t.Fatalf("removed string key still has posting")
	}
	fieldStorageAssertPostingContains(t, storage, "b", 2, 9)
	fieldStorageAssertPostingContains(t, storage, "c", 3)
}

func TestFieldStorageApplyStringPostingDiffCopiesCallerKeyBytes(t *testing.T) {
	entries := []Entry{
		{Key: keycodec.FromString("owned-diff/a"), IDs: fieldStorageSingleton(1)},
		{Key: keycodec.FromString("owned-diff/c"), IDs: fieldStorageSingleton(3)},
	}
	base := newRegularFieldStorage(entries)

	key := []byte("owned-diff/b")
	var arena PostingDiffArena
	deltas := AddStringPostingDiff(nil, &arena, fieldStorageMutableString(key), 2, true)
	deltas = AddStringPostingDiff(deltas, &arena, "owned-diff/c", 30, true)

	storage := base.ApplyStringPostingDiff(deltas, &arena, false)
	arena.Reset()
	if storage == base {
		storage.Release()
		t.Fatalf("expected diff to create new storage")
	}

	base.Release()
	poisonBytes(key)
	defer storage.Release()

	fieldStorageAssertPostingContains(t, storage, "owned-diff/a", 1)
	fieldStorageAssertPostingContains(t, storage, "owned-diff/b", 2)
	fieldStorageAssertPostingContains(t, storage, "owned-diff/c", 3, 30)
}

func TestFieldStorageApplyStringPostingDiffModelReplayReleasePoison(t *testing.T) {
	type replayOp struct {
		key    string
		id     uint64
		add    bool
		borrow bool
	}

	model := make(map[string]map[uint64]struct{}, fieldIndexChunkThreshold+64)
	baseMap := GetPostingMap()
	for i := 0; i < 32; i++ {
		key := fmt.Sprintf("model/%04d", i)
		ids := []uint64{uint64(i + 1)}
		if i%5 == 0 {
			ids = append(ids, uint64(1_000_000+i))
		}
		baseMap[key] = fieldStoragePosting(ids...)
		set := make(map[uint64]struct{}, len(ids))
		for _, id := range ids {
			set[id] = struct{}{}
		}
		model[key] = set
	}

	storage := NewRegularFieldStorageFromPostingMapOwned(baseMap)
	if storage.IsChunked() {
		storage.Release()
		t.Fatalf("expected initial flat storage")
	}

	assertMatchesModel := func(label string, storage FieldStorage) {
		t.Helper()
		ov := NewFieldIndexViewFromStorage(storage)
		if ov.KeyCount() != len(model) {
			t.Fatalf("%s: key count got %d want %d", label, ov.KeyCount(), len(model))
		}

		keys := make([]string, 0, len(model))
		for key := range model {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		for _, key := range keys {
			want := model[key]
			ids := ov.LookupPostingRetained(key)
			if ids.Cardinality() != uint64(len(want)) {
				got := ids.ToArray()
				ids.Release()
				t.Fatalf("%s: posting %q cardinality got %d ids=%v want %d", label, key, len(got), got, len(want))
			}
			for id := range want {
				if !ids.Contains(id) {
					got := ids.ToArray()
					ids.Release()
					t.Fatalf("%s: posting %q missing id %d in %v", label, key, id, got)
				}
			}
			ids.Release()
		}
	}

	assertMatchesModel("base", storage)

	applyStep := func(label string, ops []replayOp, allowChunk bool, wantChunked bool) {
		t.Helper()

		var oldData [][]byte
		if storage.flat != nil {
			oldData = append(oldData, storage.flat.stringData)
		} else if !wantChunked {
			oldData = make([][]byte, 0, storage.chunked.chunkCount)
			for i := range storage.chunked.pages {
				page := storage.chunked.pages[i]
				for j := range page.refs {
					oldData = append(oldData, page.refs[j].chunk.stringData)
				}
			}
		} else {
			touched := make(map[int]struct{}, len(ops))
			for _, op := range ops {
				pos := storage.chunked.touchChunkIndexFrom(0, keycodec.FromString(op.key))
				if pos < 0 {
					t.Fatalf("%s: missing touched chunk for %q", label, op.key)
				}
				touched[pos] = struct{}{}
			}
			oldData = make([][]byte, 0, len(touched))
			for pos := range touched {
				ref, ok := storage.chunked.refAtChunk(pos)
				if !ok || ref.chunk == nil {
					t.Fatalf("%s: missing chunk ref %d", label, pos)
				}
				oldData = append(oldData, ref.chunk.stringData)
			}
		}

		var arena PostingDiffArena
		var deltas map[string]uint32
		borrowed := make([][]byte, 0, len(ops))
		for _, op := range ops {
			key := op.key
			if op.borrow {
				buf := []byte(key)
				borrowed = append(borrowed, buf)
				key = fieldStorageMutableString(buf)
			}
			deltas = AddStringPostingDiff(deltas, &arena, key, op.id, op.add)
			set := model[op.key]
			if op.add {
				if set == nil {
					set = make(map[uint64]struct{}, 1)
					model[op.key] = set
				}
				if _, ok := set[op.id]; ok {
					t.Fatalf("%s: duplicate model add %q/%d", label, op.key, op.id)
				}
				set[op.id] = struct{}{}
				continue
			}
			if _, ok := set[op.id]; !ok {
				t.Fatalf("%s: missing model remove %q/%d", label, op.key, op.id)
			}
			delete(set, op.id)
			if len(set) == 0 {
				delete(model, op.key)
			}
		}

		old := storage
		next := storage.ApplyStringPostingDiff(deltas, &arena, allowChunk)
		arena.Reset()
		if next == old {
			t.Fatalf("%s: diff returned unchanged storage", label)
		}

		old.Release()
		poisonBytes(oldData...)
		poisonBytes(borrowed...)

		storage = next
		gotChunked := storage.IsChunked()
		if gotChunked != wantChunked {
			storage.Release()
			t.Fatalf("%s: chunked got %v want %v", label, gotChunked, wantChunked)
		}
		assertMatchesModel(label, storage)
	}

	grow := make([]replayOp, 0, 384)
	for i := 32; i <= 410; i++ {
		grow = append(grow, replayOp{
			key:    fmt.Sprintf("model/%04d", i),
			id:     uint64(10_000 + i),
			add:    true,
			borrow: i%37 == 0,
		})
	}
	grow = append(grow, replayOp{key: "model/0007", id: 700_007, add: true})
	applyStep("flat-to-chunked", grow, true, true)

	update := []replayOp{
		{key: "model/0005", id: 6},
		{key: "model/0005", id: 1_000_005},
		{key: "model/0032", id: 10_032},
		{key: "model/0192", id: 700_192, add: true},
		{key: "model/0200", id: 700_200, add: true},
		{key: "model/0200a", id: 720_000, add: true, borrow: true},
		{key: "model/0409", id: 10_409},
	}
	applyStep("chunked-update", update, true, true)

	keep := map[string]struct{}{
		"model/0000":  {},
		"model/0192":  {},
		"model/0200":  {},
		"model/0200a": {},
		"model/0410":  {},
	}
	keys := make([]string, 0, len(model))
	for key := range model {
		if _, ok := keep[key]; !ok {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	shrink := make([]replayOp, 0, len(keys))
	for _, key := range keys {
		for id := range model[key] {
			shrink = append(shrink, replayOp{key: key, id: id})
		}
	}
	applyStep("chunked-to-flat", shrink, true, false)

	flatUpdate := []replayOp{
		{key: "model/0000", id: 880_000, add: true},
		{key: "model/0001a", id: 880_001, add: true, borrow: true},
		{key: "model/0410", id: 10_410},
	}
	applyStep("flat-update", flatUpdate, true, false)
	storage.Release()
}

func TestFieldStorageApplyFixedPostingDiff_UpdatesAndRemoves(t *testing.T) {
	entries := []Entry{
		{Key: keycodec.FromU64(2), IDs: fieldStorageSingleton(1)},
		{Key: keycodec.FromU64(4), IDs: fieldStorageSingleton(2)},
	}
	base := newRegularFieldStorage(entries)
	defer base.Release()

	var arena PostingDiffArena
	deltas := AddFixedPostingDiff(nil, &arena, 2, 1, false)
	deltas = AddFixedPostingDiff(deltas, &arena, 4, 9, true)
	deltas = AddFixedPostingDiff(deltas, &arena, 6, 3, true)
	defer arena.Reset()

	storage := base.ApplyFixedPostingDiff(deltas, &arena, false)
	defer storage.Release()

	ov := NewFieldIndexViewFromStorage(storage)
	if ov.LookupCardinalityKey(keycodec.FromU64(2)) != 0 {
		t.Fatalf("removed fixed key still has posting")
	}
	fieldStorageAssertPostingContainsKey(t, storage, keycodec.FromU64(4), 2, 9)
	fieldStorageAssertPostingContainsKey(t, storage, keycodec.FromU64(6), 3)
}

func TestFieldStorageApplyFixedPostingDiffModelReplayReleasePoison(t *testing.T) {
	type replayOp struct {
		key uint64
		id  uint64
		add bool
	}

	model := make(map[uint64]map[uint64]struct{}, fieldIndexChunkThreshold+64)
	baseMap := GetFixedPostingMap()
	for i := 0; i < 32; i++ {
		key := uint64(i * 2)
		ids := []uint64{uint64(i + 1)}
		if i%5 == 0 {
			ids = append(ids, uint64(1_000_000+i))
		}
		baseMap[key] = fieldStoragePosting(ids...)
		set := make(map[uint64]struct{}, len(ids))
		for _, id := range ids {
			set[id] = struct{}{}
		}
		model[key] = set
	}

	storage := NewRegularFieldStorageFromFixedPostingMapOwned(baseMap)
	if storage.IsChunked() {
		storage.Release()
		t.Fatalf("expected initial fixed storage to be flat")
	}

	assertMatchesModel := func(label string, storage FieldStorage) {
		t.Helper()
		ov := NewFieldIndexViewFromStorage(storage)
		if ov.KeyCount() != len(model) {
			t.Fatalf("%s: key count got %d want %d", label, ov.KeyCount(), len(model))
		}

		keys := make([]uint64, 0, len(model))
		for key := range model {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		for _, key := range keys {
			want := model[key]
			ids := ov.LookupPostingRetainedKey(keycodec.FromU64(key))
			if ids.Cardinality() != uint64(len(want)) {
				got := ids.ToArray()
				ids.Release()
				t.Fatalf("%s: posting %d cardinality got %d ids=%v want %d", label, key, len(got), got, len(want))
			}
			for id := range want {
				if !ids.Contains(id) {
					got := ids.ToArray()
					ids.Release()
					t.Fatalf("%s: posting %d missing id %d in %v", label, key, id, got)
				}
			}
			ids.Release()
		}
	}

	assertMatchesModel("base", storage)

	applyStep := func(label string, ops []replayOp, allowChunk bool, wantChunked bool) {
		t.Helper()

		var oldEntries []Entry
		var oldNumeric [][]uint64
		if storage.flat != nil {
			oldEntries = storage.flat.entries
		} else if !wantChunked {
			oldNumeric = make([][]uint64, 0, storage.chunked.chunkCount)
			for i := range storage.chunked.pages {
				page := storage.chunked.pages[i]
				for j := range page.refs {
					oldNumeric = append(oldNumeric, page.refs[j].chunk.numeric)
				}
			}
		} else {
			touched := make(map[int]struct{}, len(ops))
			for _, op := range ops {
				pos := storage.chunked.touchChunkIndexFrom(0, keycodec.FromU64(op.key))
				if pos < 0 {
					t.Fatalf("%s: missing touched chunk for %d", label, op.key)
				}
				touched[pos] = struct{}{}
			}
			oldNumeric = make([][]uint64, 0, len(touched))
			for pos := range touched {
				ref, ok := storage.chunked.refAtChunk(pos)
				if !ok || ref.chunk == nil {
					t.Fatalf("%s: missing chunk ref %d", label, pos)
				}
				oldNumeric = append(oldNumeric, ref.chunk.numeric)
			}
		}

		var arena PostingDiffArena
		var deltas map[uint64]uint32
		for _, op := range ops {
			deltas = AddFixedPostingDiff(deltas, &arena, op.key, op.id, op.add)
			set := model[op.key]
			if op.add {
				if set == nil {
					set = make(map[uint64]struct{}, 1)
					model[op.key] = set
				}
				if _, ok := set[op.id]; ok {
					t.Fatalf("%s: duplicate model add %d/%d", label, op.key, op.id)
				}
				set[op.id] = struct{}{}
				continue
			}
			if _, ok := set[op.id]; !ok {
				t.Fatalf("%s: missing model remove %d/%d", label, op.key, op.id)
			}
			delete(set, op.id)
			if len(set) == 0 {
				delete(model, op.key)
			}
		}

		old := storage
		next := storage.ApplyFixedPostingDiff(deltas, &arena, allowChunk)
		arena.Reset()
		if next == old {
			t.Fatalf("%s: diff returned unchanged storage", label)
		}

		old.Release()
		for i := range oldEntries {
			oldEntries[i].Key = keycodec.FromU64(^uint64(0) - uint64(i))
		}
		poisonUint64s(oldNumeric...)

		storage = next
		gotChunked := storage.IsChunked()
		if gotChunked != wantChunked {
			storage.Release()
			t.Fatalf("%s: chunked got %v want %v", label, gotChunked, wantChunked)
		}
		assertMatchesModel(label, storage)
	}

	grow := make([]replayOp, 0, 384)
	for i := 32; i <= 410; i++ {
		grow = append(grow, replayOp{
			key: uint64(i * 2),
			id:  uint64(10_000 + i),
			add: true,
		})
	}
	grow = append(grow, replayOp{key: 14, id: 700_007, add: true})
	applyStep("fixed-flat-to-chunked", grow, true, true)

	update := []replayOp{
		{key: 10, id: 6},
		{key: 10, id: 1_000_005},
		{key: 64, id: 10_032},
		{key: 384, id: 700_192, add: true},
		{key: 400, id: 700_200, add: true},
		{key: 401, id: 720_000, add: true},
		{key: 818, id: 10_409},
	}
	applyStep("fixed-chunked-update", update, true, true)

	keep := map[uint64]struct{}{
		0:   {},
		384: {},
		400: {},
		401: {},
		820: {},
	}
	keys := make([]uint64, 0, len(model))
	for key := range model {
		if _, ok := keep[key]; !ok {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	shrink := make([]replayOp, 0, len(keys))
	for _, key := range keys {
		for id := range model[key] {
			shrink = append(shrink, replayOp{key: key, id: id})
		}
	}
	applyStep("fixed-chunked-to-flat", shrink, true, false)

	flatUpdate := []replayOp{
		{key: 0, id: 880_000, add: true},
		{key: 3, id: 880_001, add: true},
		{key: 820, id: 10_410},
	}
	applyStep("fixed-flat-update", flatUpdate, true, false)
	storage.Release()
}

func TestFieldStorageApplyLenPostingDiff_UpdatesNonEmptyMarker(t *testing.T) {
	entries := []Entry{
		{Key: keycodec.FromU64(1), IDs: fieldStorageSingleton(1)},
		{Key: keycodec.FromString(LenIndexNonEmptyKey), IDs: fieldStorageSingleton(1)},
	}
	base := newRegularFieldStorage(entries)
	defer base.Release()

	var deltas *LenPostingDiff
	deltas = AddLenPostingBucket(deltas, 1, 1, false)
	deltas = AddLenPostingBucket(deltas, 1, 2, true)
	deltas = AddLenPostingNonEmpty(deltas, 1, false)
	deltas = AddLenPostingNonEmpty(deltas, 1, true)

	storage := base.ApplyLenPostingDiff(deltas)
	defer storage.Release()

	ov := NewFieldIndexViewFromStorage(storage)
	if ov.LookupCardinalityKey(keycodec.FromU64(1)) != 0 {
		t.Fatalf("old len bucket still has posting")
	}
	fieldStorageAssertPostingContainsKey(t, storage, keycodec.FromU64(2), 1)
	fieldStorageAssertPostingContains(t, storage, LenIndexNonEmptyKey, 1)
}

func TestApplySingleFieldPostingDiffChunked_OwnsUntouchedPostingsOnExistingKeyFastPath(t *testing.T) {
	tests := []struct {
		name    string
		numeric bool
	}{
		{name: "String", numeric: false},
		{name: "Numeric", numeric: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ref := fieldStorageOwnedChunkRef(fieldIndexChunkTargetEntries, tc.numeric)
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
	defer pooled.ReleaseByteSlice(reuse)
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
		rows:    uint64(len(splitPosts)),
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

	baseEntries, baseData := root.flatten()
	if baseEntries == nil {
		t.Fatalf("expected flattened base entries")
	}
	defer ReleaseFieldEntrySlice(baseEntries)
	defer pooled.ReleaseByteSlice(baseData)
	want, same := applySingleFieldPostingDiffSorted(baseEntries, PostingDelta{
		Key: insertKey,
		Delta: BatchPostingDelta{
			Add: fieldStorageSingleton(999_999),
		},
	})
	if want == nil {
		t.Fatalf("expected non-empty expected entries")
	}
	if !same {
		defer ReleaseFieldEntrySlice(want)
	}
	fieldStorageAssertStorageMatchesEntries(t, got, want)
}
