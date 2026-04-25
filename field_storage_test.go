package rbi

import (
	"bufio"
	"bytes"
	"fmt"
	"strings"
	"testing"
	"unsafe"

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

func TestFieldIndexStringRefSize(t *testing.T) {
	if got := unsafe.Sizeof(fieldIndexStringRef{}); got != 4 {
		t.Fatalf("unexpected fieldIndexStringRef size: got %d want 4", got)
	}
}

func TestNewFieldIndexChunkFromKeys_StringRefsFitUint16(t *testing.T) {
	keys := []indexKey{
		indexKeyFromStoredString(strings.Repeat("a", 40000), false),
		indexKeyFromStoredString(strings.Repeat("b", 40000), false),
	}
	posts := []posting.List{
		fieldStorageSingleton(1),
		fieldStorageSingleton(2),
	}
	chunk := newFieldIndexChunkFromKeys(posts, keys, 2)
	if chunk == nil {
		t.Fatalf("expected string chunk")
	}
	if chunk.keyCount() != len(keys) {
		t.Fatalf("unexpected key count: got %d want %d", chunk.keyCount(), len(keys))
	}
	if got := chunk.keyAt(0).byteLen(); got != 40000 {
		t.Fatalf("unexpected first key len: got %d want 40000", got)
	}
	if got := chunk.keyAt(1).byteLen(); got != 40000 {
		t.Fatalf("unexpected second key len: got %d want 40000", got)
	}
	if got := int(chunk.stringRefs[1].off); got != 40000 {
		t.Fatalf("unexpected second ref offset: got %d want 40000", got)
	}
}

func TestNewFieldIndexChunkFromKeys_StringRefOffsetOverflowPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("expected ref overflow panic")
		}
	}()

	keys := []indexKey{
		indexKeyFromStoredString(strings.Repeat("a", fieldIndexStringRefMax), false),
		indexKeyFromStoredString("b", false),
		indexKeyFromStoredString("c", false),
	}
	posts := []posting.List{
		fieldStorageSingleton(1),
		fieldStorageSingleton(2),
		fieldStorageSingleton(3),
	}
	_ = newFieldIndexChunkFromKeys(posts, keys, 3)
}

func TestNewFieldIndexChunkFromKeys_StringSingletonsUseOwnerLayout(t *testing.T) {
	keys := []indexKey{
		indexKeyFromStoredString("alpha", false),
		indexKeyFromStoredString("beta", false),
	}
	posts := []posting.List{
		fieldStorageSingleton(101),
		fieldStorageSingleton(202),
	}

	chunk := newFieldIndexChunkFromKeys(posts, keys, 2)
	if chunk == nil {
		t.Fatalf("expected string chunk")
	}
	if !chunk.hasUniqueStringOwners() {
		t.Fatalf("expected owner layout for singleton string chunk")
	}
	if len(chunk.posts) != 0 {
		t.Fatalf("expected no stored posting handles, got %d", len(chunk.posts))
	}
	if got := chunk.rowCount(); got != 2 {
		t.Fatalf("unexpected row count: got %d want 2", got)
	}
	if got := chunk.rowsInRange(0, 2); got != 2 {
		t.Fatalf("unexpected range rows: got %d want 2", got)
	}
	ids := chunk.postingAt(1)
	if ids.Cardinality() != 1 || !ids.Contains(202) {
		t.Fatalf("unexpected posting: %v", ids)
	}
}

func TestNewNumericFieldIndexChunk_SingletonsUseOwnerLayout(t *testing.T) {
	keys := []uint64{11, 22}
	posts := []posting.List{
		fieldStorageSingleton(101),
		fieldStorageSingleton(202),
	}

	chunk := newNumericFieldIndexChunk(posts, keys, 2)
	if chunk == nil {
		t.Fatalf("expected numeric chunk")
	}
	if !chunk.hasUniqueNumericOwners() {
		t.Fatalf("expected owner layout for singleton numeric chunk")
	}
	if len(chunk.posts) != 0 {
		t.Fatalf("expected no stored posting handles, got %d", len(chunk.posts))
	}
	if got := chunk.keyCount(); got != len(keys) {
		t.Fatalf("unexpected key count: got %d want %d", got, len(keys))
	}
	if got := chunk.keyAt(0).meta; got != 11 {
		t.Fatalf("unexpected first key: got %d want 11", got)
	}
	if got := chunk.rowCount(); got != 2 {
		t.Fatalf("unexpected row count: got %d want 2", got)
	}
	if got := chunk.rowsInRange(0, 2); got != 2 {
		t.Fatalf("unexpected range rows: got %d want 2", got)
	}
	ids := chunk.postingAt(1)
	if ids.Cardinality() != 1 || !ids.Contains(202) {
		t.Fatalf("unexpected posting: %v", ids)
	}
}

func TestFieldIndexChunk_StringSingletonRoundTripUsesOwnerLayout(t *testing.T) {
	keys := []indexKey{
		indexKeyFromStoredString("alpha", false),
		indexKeyFromStoredString("beta", false),
	}
	posts := []posting.List{
		fieldStorageSingleton(11),
		fieldStorageSingleton(22),
	}
	chunk := newFieldIndexChunkFromKeys(posts, keys, 2)
	if chunk == nil {
		t.Fatalf("expected string chunk")
	}
	if !chunk.hasUniqueStringOwners() {
		t.Fatalf("expected owner layout before serialization")
	}

	var raw bytes.Buffer
	writer := bufio.NewWriter(&raw)
	if err := writeFieldIndexChunk(writer, chunk); err != nil {
		t.Fatalf("write chunk: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush chunk: %v", err)
	}

	roundTrip, err := readFieldIndexChunk(bufio.NewReader(bytes.NewReader(raw.Bytes())))
	if err != nil {
		t.Fatalf("read chunk: %v", err)
	}
	if roundTrip == nil {
		t.Fatalf("expected round-trip chunk")
	}
	if !roundTrip.hasUniqueStringOwners() {
		t.Fatalf("expected owner layout after serialization round-trip")
	}
	if got := roundTrip.keyAt(0).asUnsafeString(); got != "alpha" {
		t.Fatalf("unexpected first key: got %q want %q", got, "alpha")
	}
	if ids := roundTrip.postingAt(1); ids.Cardinality() != 1 || !ids.Contains(22) {
		t.Fatalf("unexpected round-trip posting: %v", ids)
	}
}

func TestFieldIndexChunk_NumericSingletonRoundTripUsesOwnerLayout(t *testing.T) {
	chunk := newNumericFieldIndexChunk([]posting.List{
		fieldStorageSingleton(11),
		fieldStorageSingleton(22),
	}, []uint64{111, 222}, 2)
	if chunk == nil {
		t.Fatalf("expected numeric chunk")
	}
	if !chunk.hasUniqueNumericOwners() {
		t.Fatalf("expected owner layout before serialization")
	}

	var raw bytes.Buffer
	writer := bufio.NewWriter(&raw)
	if err := writeFieldIndexChunk(writer, chunk); err != nil {
		t.Fatalf("write chunk: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush chunk: %v", err)
	}

	roundTrip, err := readFieldIndexChunk(bufio.NewReader(bytes.NewReader(raw.Bytes())))
	if err != nil {
		t.Fatalf("read chunk: %v", err)
	}
	if roundTrip == nil {
		t.Fatalf("expected round-trip chunk")
	}
	if !roundTrip.hasUniqueNumericOwners() {
		t.Fatalf("expected owner layout after serialization round-trip")
	}
	if got := roundTrip.keyAt(0).meta; got != 111 {
		t.Fatalf("unexpected first key: got %d want 111", got)
	}
	if ids := roundTrip.postingAt(1); ids.Cardinality() != 1 || !ids.Contains(22) {
		t.Fatalf("unexpected round-trip posting: %v", ids)
	}
}

func TestApplyFieldPostingDiffChunked_StringOwnerChunkKeepsStringLayout(t *testing.T) {
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
	defer root.release()

	ref, ok := root.refAtChunk(0)
	if !ok || ref.chunk == nil || !ref.chunk.hasUniqueStringOwners() {
		t.Fatalf("expected owner layout in first chunk")
	}

	oldKey := "k0095"
	newKey := "k0095a"
	deltas := []keyedBatchPostingDelta{
		{
			key: indexKeyFromStoredString(oldKey, false),
			delta: batchPostingDelta{
				remove: fieldStorageSingleton(96),
			},
		},
		{
			key: indexKeyFromStoredString(newKey, false),
			delta: batchPostingDelta{
				add: fieldStorageSingleton(900_001),
			},
		},
	}

	storage := applyFieldPostingDiffChunked(root, deltas)
	defer releaseFieldIndexStorageOwned(storage)

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
	entries := make([]index, fieldIndexChunkThreshold)
	for i := range entries {
		entries[i] = index{
			Key: indexKeyFromU64(uint64(i * 2)),
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
	deltas := []keyedBatchPostingDelta{
		{
			key: indexKeyFromU64(oldKey),
			delta: batchPostingDelta{
				remove: fieldStorageSingleton(96),
			},
		},
		{
			key: indexKeyFromU64(newKey),
			delta: batchPostingDelta{
				add: fieldStorageSingleton(900_001),
			},
		},
	}

	storage := applyFieldPostingDiffChunked(root, deltas)
	defer releaseFieldIndexStorageOwned(storage)

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
	if ids := storage.chunked.lookupPostingRetained(uint64ByteStr(oldKey)); !ids.IsEmpty() {
		t.Fatalf("expected old key to be removed, got %v", ids)
	}
	ids := storage.chunked.lookupPostingRetained(uint64ByteStr(newKey))
	if ids.Cardinality() != 1 || !ids.Contains(900_001) {
		t.Fatalf("unexpected new key posting: %v", ids)
	}
	if ids := storage.chunked.lookupPostingRetained(uint64ByteStr(oldKey + 2)); ids.Cardinality() != 1 || !ids.Contains(97) {
		t.Fatalf("neighbor key lookup broken: %v", ids)
	}
}

func TestApplySingleFieldPostingDiffChunked_StringOwnerChunkDemotesOnMultiPosting(t *testing.T) {
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
	defer root.release()

	storage := applySingleFieldPostingDiffChunked(root, keyedBatchPostingDelta{
		key: indexKeyFromStoredString("k0095", false),
		delta: batchPostingDelta{
			add: fieldStorageSingleton(900_002),
		},
	})
	defer releaseFieldIndexStorageOwned(storage)

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
	if got := repl.chunk.keyAt(95).asUnsafeString(); got != "k0095" {
		t.Fatalf("unexpected updated key: got %q want %q", got, "k0095")
	}
}

func TestApplySingleFieldPostingDiffChunked_NumericOwnerChunkDemotesOnMultiPosting(t *testing.T) {
	entries := make([]index, fieldIndexChunkThreshold)
	for i := range entries {
		entries[i] = index{
			Key: indexKeyFromU64(uint64(i * 2)),
			IDs: fieldStorageSingleton(uint64(i + 1)),
		}
	}

	root := buildChunkedFieldIndexRoot(entries)
	if root == nil {
		t.Fatalf("expected chunked root")
	}
	defer root.release()

	storage := applySingleFieldPostingDiffChunked(root, keyedBatchPostingDelta{
		key: indexKeyFromU64(uint64(95 * 2)),
		delta: batchPostingDelta{
			add: fieldStorageSingleton(900_002),
		},
	})
	defer releaseFieldIndexStorageOwned(storage)

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
	ids := storage.chunked.lookupPostingRetained(uint64ByteStr(uint64(95 * 2)))
	if ids.Cardinality() != 2 || !ids.Contains(96) || !ids.Contains(900_002) {
		t.Fatalf("unexpected expanded posting: %v", ids)
	}
	if got := repl.chunk.keyAt(95).meta; got != uint64(95*2) {
		t.Fatalf("unexpected updated key: got %d want %d", got, uint64(95*2))
	}
}

func TestCollectChunkedFieldIndexStats_NumericOwnerChunkCountsOwnerArrayAsStructural(t *testing.T) {
	genericChunk := &fieldIndexChunk{
		posts: []posting.List{
			fieldStorageSingleton(1),
			fieldStorageSingleton(2),
		},
		numeric: []uint64{11, 22},
		rows:    2,
	}
	genericRoot := newFieldIndexChunkedRootFromPages([]*fieldIndexChunkDirPage{newFieldIndexChunkDirPage([]fieldIndexChunkRef{{
		last:  genericChunk.keyAt(1),
		chunk: genericChunk,
	}})})
	if genericRoot == nil {
		t.Fatalf("expected generic root")
	}
	defer genericRoot.release()

	ownerChunk := newNumericFieldIndexChunk([]posting.List{
		fieldStorageSingleton(1),
		fieldStorageSingleton(2),
	}, []uint64{11, 22}, 2)
	if ownerChunk == nil || !ownerChunk.hasUniqueNumericOwners() {
		t.Fatalf("expected numeric owner chunk")
	}
	ownerRoot := newFieldIndexChunkedRootFromPages([]*fieldIndexChunkDirPage{newFieldIndexChunkDirPage([]fieldIndexChunkRef{{
		last:  ownerChunk.keyAt(1),
		chunk: ownerChunk,
	}})})
	if ownerRoot == nil {
		t.Fatalf("expected owner root")
	}
	defer ownerRoot.release()

	genericStats := collectChunkedFieldIndexStats(genericRoot, true)
	ownerStats := collectChunkedFieldIndexStats(ownerRoot, true)

	wantDelta := uint64(len(genericChunk.posts)) *
		(uint64(unsafe.Sizeof(posting.List{})) - uint64(unsafe.Sizeof(uint64(0))))
	if got := genericStats.approxStructBytes - ownerStats.approxStructBytes; got != wantDelta {
		t.Fatalf("unexpected struct overhead delta: got %d want %d", got, wantDelta)
	}
}

func TestNewFieldIndexChunkRefsFromEntries_StringChunksRespectOffsetLimit(t *testing.T) {
	const total = fieldIndexChunkTargetEntries
	entries := make([]index, total)
	wantKeys := make([]string, total)

	for i := 0; i < total; i++ {
		key := fmt.Sprintf("%04d/%s", i, strings.Repeat("x", 395))
		wantKeys[i] = key
		entries[i] = index{
			Key: indexKeyFromStoredString(key, false),
			IDs: fieldStorageSingleton(uint64(i + 1)),
		}
	}

	refs := newFieldIndexChunkRefsFromEntries(entries)
	if len(refs) < 2 {
		t.Fatalf("expected long string entries to split into multiple chunks, got %d", len(refs))
	}
	for i := range refs {
		chunk := refs[i].chunk
		if chunk == nil {
			t.Fatalf("chunk %d is nil", i)
		}
		for j := range chunk.stringRefs {
			ref := chunk.stringRefs[j]
			if int(ref.off) > fieldIndexStringRefMax {
				t.Fatalf("chunk %d ref %d offset exceeds limit: %d", i, j, ref.off)
			}
			if int(ref.len) > fieldIndexStringRefMax {
				t.Fatalf("chunk %d ref %d len exceeds limit: %d", i, j, ref.len)
			}
		}
	}

	root := newFieldIndexChunkedRootFromPages([]*fieldIndexChunkDirPage{newFieldIndexChunkDirPage(refs)})
	flat := flattenChunkedFieldIndexRoot(root)
	if flat == nil || len(*flat) != total {
		t.Fatalf("unexpected flattened len: got %d want %d", len(*flat), total)
	}
	for i := range *flat {
		if got := (*flat)[i].Key.asUnsafeString(); got != wantKeys[i] {
			t.Fatalf("key[%d]: got %q want %q", i, got, wantKeys[i])
		}
	}
}

func TestNewFieldIndexChunkRefsFromEntries_StringChunksPreserveBalancedTail(t *testing.T) {
	const total = fieldIndexChunkThreshold + 1
	entries := make([]index, total)
	for i := range entries {
		entries[i] = index{
			Key: indexKeyFromStoredString(fmt.Sprintf("k%04d", i), false),
			IDs: fieldStorageSingleton(uint64(i + 1)),
		}
	}

	refs := newFieldIndexChunkRefsFromEntries(entries)
	if len(refs) != 3 {
		t.Fatalf("unexpected chunk count: got %d want 3", len(refs))
	}
	if got := refs[0].chunk.keyCount(); got != 192 {
		t.Fatalf("chunk 0 size: got %d want 192", got)
	}
	if got := refs[1].chunk.keyCount(); got != 96 {
		t.Fatalf("chunk 1 size: got %d want 96", got)
	}
	if got := refs[2].chunk.keyCount(); got != 97 {
		t.Fatalf("chunk 2 size: got %d want 97", got)
	}
}

func TestBuildFieldWriteSinkAddStringRejectsTooLongValue(t *testing.T) {
	var err error
	sink := buildFieldWriteSink{
		field: "email",
		err:   &err,
	}
	sink.addString(strings.Repeat("x", fieldIndexStringRefMax+1))
	if err == nil {
		t.Fatalf("expected indexed string validation error")
	}
	if !strings.Contains(err.Error(), "exceeds limit") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReadIndexKeyRejectsTooLongString(t *testing.T) {
	var raw bytes.Buffer
	writer := bufio.NewWriter(&raw)
	if err := writer.WriteByte(indexKeyEncodingString); err != nil {
		t.Fatalf("write tag: %v", err)
	}
	if err := writeString(writer, strings.Repeat("x", fieldIndexStringRefMax+1)); err != nil {
		t.Fatalf("write string: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	_, err := readIndexKey(bufio.NewReader(bytes.NewReader(raw.Bytes())))
	if err == nil {
		t.Fatalf("expected indexed string validation error")
	}
	if !strings.Contains(err.Error(), "exceeds limit") {
		t.Fatalf("unexpected error: %v", err)
	}
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
