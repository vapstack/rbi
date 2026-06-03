package indexdata

import (
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
)

// String-key storage ownership matrix:
// - keycodec.IndexKey built from []byte/string is borrowed input.
// - fieldIndexFlatRoot owns copied stringData for persisted string keys.
// - fieldIndexChunk owns copied stringData for persisted string keys.
// - fieldIndexChunkStreamBuilder may borrow keys while building, but finish publishes owned chunks.
// - posting payloads are owned or retained through posting.List Clone/Borrow contracts.
func fieldStorageMutableString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func poisonBytes(bufs ...[]byte) {
	for i := range bufs {
		buf := bufs[i]
		for j := range buf {
			buf[j] = 0x7f
		}
	}
}

func fieldStorageLargePosting(base uint64) posting.List {
	ids := make([]uint64, 128)
	for i := range ids {
		ids[i] = base + uint64(i)*3
	}
	return posting.BuildFromSorted(ids)
}

func assertRetainedFieldStorageBorrowedViewSurvivesSourceRelease(t *testing.T, storage FieldStorage, key string, want []uint64) {
	t.Helper()

	retained := storage
	retained.retain()
	defer retained.Release()

	held := NewFieldIndexViewFromStorage(retained).LookupPostingRetained(key)
	if held.IsEmpty() {
		storage.Release()
		t.Fatalf("expected retained posting for key %q", key)
	}
	defer held.Release()
	if got := held.ToArray(); !slices.Equal(got, want) {
		storage.Release()
		t.Fatalf("posting %q before release: got=%v want=%v", key, got, want)
	}

	var failed atomic.Pointer[string]
	setFailed := func(msg string) {
		if failed.Load() != nil {
			return
		}
		copyMsg := msg
		failed.CompareAndSwap(nil, &copyMsg)
	}

	readerN := max(4, runtime.GOMAXPROCS(0))
	start := make(chan struct{})
	ready := make(chan struct{}, readerN)
	var wg sync.WaitGroup
	for i := 0; i < readerN; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ready <- struct{}{}
			for i := 0; i < 1000; i++ {
				if got := held.ToArray(); !slices.Equal(got, want) {
					setFailed(fmt.Sprintf("posting %q changed while source storage was released: got=%v want=%v", key, got, want))
					return
				}
			}
		}()
	}

	close(start)
	for i := 0; i < readerN; i++ {
		<-ready
	}
	storage.Release()
	wg.Wait()

	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}
	if got := held.ToArray(); !slices.Equal(got, want) {
		t.Fatalf("posting %q after release: got=%v want=%v", key, got, want)
	}
}

func TestFieldIndexStringRefSize(t *testing.T) {
	if got := unsafe.Sizeof(fieldIndexStringRef(0)); got != 4 {
		t.Fatalf("unexpected fieldIndexStringRef size: got %d want 4", got)
	}
}

func TestNewFieldIndexChunkFromKeys_StringRefsFitUint16(t *testing.T) {
	keys := []keycodec.IndexKey{
		keycodec.FromStoredString(strings.Repeat("a", 40000), false),
		keycodec.FromStoredString(strings.Repeat("b", 40000), false),
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
	if got := chunk.keyAt(0).ByteLen(); got != 40000 {
		t.Fatalf("unexpected first key len: got %d want 40000", got)
	}
	if got := chunk.keyAt(1).ByteLen(); got != 40000 {
		t.Fatalf("unexpected second key len: got %d want 40000", got)
	}
	if got := fieldIndexStringRefOff(chunk.stringRefs[1]); got != 40000 {
		t.Fatalf("unexpected second ref offset: got %d want 40000", got)
	}
}

func TestNewFieldIndexChunkFromKeys_StringRefOffsetOverflowPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("expected ref overflow panic")
		}
	}()

	keys := []keycodec.IndexKey{
		keycodec.FromStoredString(strings.Repeat("a", fieldIndexStringRefMax), false),
		keycodec.FromStoredString("b", false),
		keycodec.FromStoredString("c", false),
	}
	posts := []posting.List{
		fieldStorageSingleton(1),
		fieldStorageSingleton(2),
		fieldStorageSingleton(3),
	}
	_ = newFieldIndexChunkFromKeys(posts, keys, 3)
}

func TestNewFieldIndexChunkFromKeys_StringSingletonsUseOwnerLayout(t *testing.T) {
	keys := []keycodec.IndexKey{
		keycodec.FromStoredString("alpha", false),
		keycodec.FromStoredString("beta", false),
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
	if got := chunk.keyAt(0).U64(); got != 11 {
		t.Fatalf("unexpected first Key: got %d want 11", got)
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

func TestNewFieldIndexChunkRefsFromEntries_StringChunksRespectOffsetLimit(t *testing.T) {
	const total = fieldIndexChunkTargetEntries
	entries := make([]Entry, total)
	wantKeys := make([]string, total)

	for i := 0; i < total; i++ {
		key := fmt.Sprintf("%04d/%s", i, strings.Repeat("x", 395))
		wantKeys[i] = key
		entries[i] = Entry{
			Key: keycodec.FromStoredString(key, false),
			IDs: fieldStorageSingleton(uint64(i + 1)),
		}
	}

	refs := newFieldIndexChunkRefsFromEntries(entries)
	if len(refs) < 2 {
		t.Fatalf("expected long string entries to split into multiple chunks")
	}
	for i := 0; i < len(refs); i++ {
		chunk := refs[i].chunk
		if chunk == nil {
			t.Fatalf("chunk %d is nil", i)
		}
		for j := range chunk.stringRefs {
			ref := chunk.stringRefs[j]
			if fieldIndexStringRefOff(ref) > fieldIndexStringRefMax {
				t.Fatalf("chunk %d ref %d offset exceeds limit: %d", i, j, fieldIndexStringRefOff(ref))
			}
			if fieldIndexStringRefLen(ref) > fieldIndexStringRefMax {
				t.Fatalf("chunk %d ref %d len exceeds limit: %d", i, j, fieldIndexStringRefLen(ref))
			}
		}
	}

	root := newFieldIndexChunkedRootFromPages([]*fieldIndexChunkDirPage{newFieldIndexChunkDirPageOwned(refs)})
	defer root.release()
	flat, data := root.flatten()
	if len(flat) != total {
		t.Fatalf("unexpected flattened len: got %d want %d", len(flat), total)
	}
	defer ReleaseFieldEntrySlice(flat)
	defer pooled.ReleaseByteSlice(data)
	for i := range flat {
		if got := flat[i].Key.UnsafeString(); got != wantKeys[i] {
			t.Fatalf("key[%d]: got %q want %q", i, got, wantKeys[i])
		}
	}
}

func TestNewFieldIndexChunkRefsFromEntries_StringChunksPreserveBalancedTail(t *testing.T) {
	const total = fieldIndexChunkThreshold + 1
	entries := make([]Entry, total)
	for i := range entries {
		entries[i] = Entry{
			Key: keycodec.FromStoredString(fmt.Sprintf("k%04d", i), false),
			IDs: fieldStorageSingleton(uint64(i + 1)),
		}
	}

	refs := newFieldIndexChunkRefsFromEntries(entries)
	if refs == nil {
		t.Fatalf("expected chunk refs")
	}
	defer releaseOwnedFieldIndexChunkRefSlice(refs)
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
	err := ValidateIndexedStringKeyLen(fieldIndexStringRefMax + 1)
	if err == nil {
		t.Fatalf("expected indexed string validation error")
	}
	if !strings.Contains(err.Error(), "exceeds limit") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRegularFieldStorageFlatCopiesBorrowedStringKeyBytes(t *testing.T) {
	keys := [][]byte{
		[]byte("flat/alpha"),
		[]byte("flat/bravo"),
		[]byte("flat/charlie"),
	}
	entries := GetFieldEntrySlice(len(keys))[:len(keys)]
	for i := range keys {
		id := uint64(i + 1)
		entries[i] = Entry{
			Key: keycodec.FromBytes(keys[i]),
			IDs: fieldStoragePosting(id, id+1000),
		}
	}

	storage := newRegularFieldStorage(entries)
	defer storage.Release()
	if storage.IsChunked() {
		t.Fatalf("expected flat storage")
	}

	poisonBytes(keys...)

	fieldStorageAssertPostingContains(t, storage, "flat/alpha", 1, 1001)
	fieldStorageAssertPostingContains(t, storage, "flat/bravo", 2, 1002)
	fieldStorageAssertPostingContains(t, storage, "flat/charlie", 3, 1003)
}

func TestFlatFieldStorageFromPostingMapOwnedCopiesCallerStringKeyBytes(t *testing.T) {
	keys := [][]byte{
		[]byte("map-flat/alpha"),
		[]byte("map-flat/bravo"),
		[]byte("map-flat/charlie"),
	}
	m := GetPostingMap()
	for i := range keys {
		id := uint64(i + 11)
		m[fieldStorageMutableString(keys[i])] = fieldStoragePosting(id, id+1000)
	}

	storage := NewFlatFieldStorageFromPostingMapOwned(m, false)
	defer storage.Release()
	if storage.IsChunked() {
		t.Fatalf("expected flat storage")
	}

	poisonBytes(keys...)

	fieldStorageAssertPostingContains(t, storage, "map-flat/alpha", 11, 1011)
	fieldStorageAssertPostingContains(t, storage, "map-flat/bravo", 12, 1012)
	fieldStorageAssertPostingContains(t, storage, "map-flat/charlie", 13, 1013)
}

func TestRegularFieldStorageChunkedCopiesBorrowedStringKeyBytes(t *testing.T) {
	const total = fieldIndexChunkThreshold
	keys := make([][]byte, total)
	entries := make([]Entry, total)
	for i := range entries {
		key := fmt.Sprintf("chunk/%04d", i)
		keys[i] = []byte(key)
		entries[i] = Entry{
			Key: keycodec.FromBytes(keys[i]),
			IDs: fieldStorageSingleton(uint64(i + 1)),
		}
	}

	storage := newRegularFieldStorage(entries)
	defer storage.Release()
	if !storage.IsChunked() {
		t.Fatalf("expected chunked storage")
	}

	poisonBytes(keys...)

	fieldStorageAssertPostingContains(t, storage, "chunk/0000", 1)
	fieldStorageAssertPostingContains(t, storage, "chunk/0192", 193)
	fieldStorageAssertPostingContains(t, storage, "chunk/0383", 384)
}

func TestRegularFieldStorageFromPostingMapOwnedChunkedCopiesCallerStringKeyBytes(t *testing.T) {
	const total = fieldIndexChunkThreshold
	keys := make([][]byte, total)
	m := GetPostingMap()
	for i := 0; i < total; i++ {
		keys[i] = []byte(fmt.Sprintf("map-chunk/%04d", i))
		m[fieldStorageMutableString(keys[i])] = fieldStorageSingleton(uint64(i + 1))
	}

	storage := NewRegularFieldStorageFromPostingMapOwned(m, false)
	defer storage.Release()
	if !storage.IsChunked() {
		t.Fatalf("expected chunked storage")
	}

	poisonBytes(keys...)

	fieldStorageAssertPostingContains(t, storage, "map-chunk/0000", 1)
	fieldStorageAssertPostingContains(t, storage, "map-chunk/0192", 193)
	fieldStorageAssertPostingContains(t, storage, "map-chunk/0383", 384)
}

func TestFieldStorageBuilderCopiesBorrowedStringKeyBytes(t *testing.T) {
	const total = fieldIndexChunkThreshold + 17
	keys := make([][]byte, total)
	var builder fieldStorageBuilder
	builder.init(total, false)
	for i := 0; i < total; i++ {
		keys[i] = []byte(fmt.Sprintf("builder/%04d", i))
		builder.append(keycodec.FromBytes(keys[i]), fieldStorageSingleton(uint64(i+1)))
	}

	storage := builder.finish()
	defer storage.Release()
	if !storage.IsChunked() {
		t.Fatalf("expected chunked storage")
	}

	poisonBytes(keys...)

	fieldStorageAssertPostingContains(t, storage, "builder/0000", 1)
	fieldStorageAssertPostingContains(t, storage, "builder/0192", 193)
	fieldStorageAssertPostingContains(t, storage, "builder/0400", 401)
}

func TestFlattenChunkedFieldIndexRoot_RoundTrip(t *testing.T) {
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

	flat, data := root.flatten()
	if flat == nil {
		t.Fatalf("expected flattened slice")
	}
	defer ReleaseFieldEntrySlice(flat)
	defer pooled.ReleaseByteSlice(data)
	if len(flat) != len(entries) {
		t.Fatalf("unexpected materialized len: got %d want %d", len(flat), len(entries))
	}
	for i := range entries {
		if keycodec.Compare(flat[i].Key, entries[i].Key) != 0 {
			t.Fatalf("unexpected key at %d", i)
		}
		if flat[i].IDs.Cardinality() != entries[i].IDs.Cardinality() || !flat[i].IDs.Contains(uint64(i+1)) {
			t.Fatalf("unexpected posting at %d", i)
		}
	}
}

func TestFieldIndexChunkRefsWithInsertedEntry_OwnsUntouchedPostings(t *testing.T) {
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
			add := Entry{
				Key: fieldStorageInsertedTestKey(pos, tc.fixed8),
				IDs: fieldStorageSingleton(uint64(tc.size + 1000)),
			}

			replRefs := ref.chunk.refsWithInsertedEntry(pos, add)
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

			replRoot := newFieldIndexChunkedRootFromPages([]*fieldIndexChunkDirPage{newFieldIndexChunkDirPageOwned(replRefs)})
			replStorage := newChunkedFieldStorage(replRoot)

			src := 0
			for replIdx := 0; replIdx < replRoot.chunkCount; replIdx++ {
				repl, ok := replRoot.refAtChunk(replIdx)
				if !ok {
					t.Fatalf("missing replacement ref %d", replIdx)
				}
				for i := 0; i < repl.chunk.keyCount(); i++ {
					key := repl.chunk.keyAt(i)
					ids := repl.chunk.posts[i]
					if keycodec.Compare(key, add.Key) == 0 {
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

			replStorage.Release()
			posting.ReleaseAll(ref.chunk.posts)
		})
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
			stream := newFieldIndexChunkStreamBuilder(tc.numeric)

			wantKeys := make([]string, total)
			wantIDs := make([]uint64, total)
			for i := 0; i < total; i++ {
				id := uint64(i + 1)
				wantIDs[i] = id

				var key keycodec.IndexKey
				if tc.numeric {
					raw := uint64(i*3 + 7)
					wantKeys[i] = keycodec.U64ByteString(raw)
					key = keycodec.FromStoredString(wantKeys[i], true)
				} else {
					wantKeys[i] = fmt.Sprintf("k/%02d/%05d", i%17, i)
					key = keycodec.FromStoredString(wantKeys[i], false)
				}

				stream.append(&builder, key, fieldStorageSingleton(id))
			}
			stream.finish(&builder)

			root := builder.root()
			if root == nil {
				t.Fatalf("expected chunked root")
			}
			if root.chunkCount < 2 {
				t.Fatalf("expected multiple chunks, got %d", root.chunkCount)
			}

			flat, data := root.flatten()
			if flat == nil {
				t.Fatalf("expected flattened slice")
			}
			defer ReleaseFieldEntrySlice(flat)
			defer pooled.ReleaseByteSlice(data)
			if len(flat) != total {
				t.Fatalf("unexpected flattened len: got %d want %d", len(flat), total)
			}
			for i := range flat {
				if got := flat[i].Key.UnsafeString(); got != wantKeys[i] {
					t.Fatalf("key[%d]: got %q want %q", i, got, wantKeys[i])
				}
				if !flat[i].IDs.Contains(wantIDs[i]) || flat[i].IDs.Cardinality() != 1 {
					t.Fatalf("posting[%d]: got=%v want singleton(%d)", i, flat[i].IDs, wantIDs[i])
				}
			}
		})
	}
}

func TestFieldStorageBuilder_RoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		numeric bool
	}{
		{name: "String"},
		{name: "Numeric", numeric: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entries := fieldStorageEntriesForTest(fieldIndexChunkThreshold+17, tc.numeric)
			var builder fieldStorageBuilder
			builder.init(len(entries), tc.numeric)
			for i := range entries {
				builder.append(entries[i].Key, entries[i].IDs)
			}
			storage := builder.finish()
			defer storage.Release()
			if !storage.IsChunked() {
				t.Fatalf("expected builder output to be chunked")
			}
			fieldStorageAssertStorageMatchesEntries(t, storage, entries)
		})
	}
}

func TestFieldStorageFlatRetainedBorrowedPostingSurvivesSourceRelease(t *testing.T) {
	const key = "k/0002"
	entries := GetFieldEntrySlice(4)[:4]
	var want []uint64
	for i := range entries {
		ids := fieldStorageLargePosting(uint64(i+1) << 20)
		if i == 2 {
			want = ids.ToArray()
		}
		entries[i] = Entry{
			Key: keycodec.FromStoredString(fmt.Sprintf("k/%04d", i), false),
			IDs: ids,
		}
	}
	storage := newRegularFieldStorage(entries)
	if storage.IsChunked() {
		storage.Release()
		t.Fatal("expected flat storage")
	}

	assertRetainedFieldStorageBorrowedViewSurvivesSourceRelease(t, storage, key, want)
}

func TestFieldStorageChunkedRetainedBorrowedPostingSurvivesSourceRelease(t *testing.T) {
	const total = fieldIndexChunkThreshold + 11
	const keyIdx = fieldIndexChunkThreshold + 3
	entries := GetFieldEntrySlice(total)[:total]
	var want []uint64
	for i := range entries {
		ids := fieldStorageLargePosting(uint64(i+1) << 20)
		if i == keyIdx {
			want = ids.ToArray()
		}
		entries[i] = Entry{
			Key: keycodec.FromStoredString(fmt.Sprintf("k/%04d", i), false),
			IDs: ids,
		}
	}
	storage := newRegularFieldStorage(entries)
	if !storage.IsChunked() {
		storage.Release()
		t.Fatal("expected chunked storage")
	}

	assertRetainedFieldStorageBorrowedViewSurvivesSourceRelease(t, storage, fmt.Sprintf("k/%04d", keyIdx), want)
}

func TestRegularFieldStorageFromPostingMapOwned_ThresholdShape(t *testing.T) {
	stringFlatMap := GetPostingMap()
	for i := 0; i < FieldChunkThreshold-1; i++ {
		stringFlatMap[fmt.Sprintf("s/%04d", i)] = fieldStorageOwnedTestPosting(uint64(i + 1))
	}
	stringFlat := NewRegularFieldStorageFromPostingMapOwned(stringFlatMap, false)
	if stringFlat.IsChunked() {
		stringFlat.Release()
		t.Fatalf("small string map storage is chunked")
	}
	fieldStorageAssertPostingContains(t, stringFlat, "s/0017", 18, 1_000_018)
	stringFlat.Release()

	stringChunkedMap := GetPostingMap()
	for i := 0; i < FieldChunkThreshold; i++ {
		stringChunkedMap[fmt.Sprintf("s/%04d", i)] = fieldStorageOwnedTestPosting(uint64(i + 1))
	}
	stringChunked := NewRegularFieldStorageFromPostingMapOwned(stringChunkedMap, false)
	if !stringChunked.IsChunked() {
		stringChunked.Release()
		t.Fatalf("threshold string map storage is flat")
	}
	stringChunked.Release()

	fixed8FlatMap := GetPostingMap()
	for i := 0; i < FieldChunkThreshold-1; i++ {
		fixed8FlatMap[keycodec.U64ByteString(uint64(i*2))] = fieldStorageOwnedTestPosting(uint64(i + 10_000))
	}
	fixed8Flat := NewRegularFieldStorageFromPostingMapOwned(fixed8FlatMap, true)
	if fixed8Flat.IsChunked() {
		fixed8Flat.Release()
		t.Fatalf("small fixed8 string map storage is chunked")
	}
	fieldStorageAssertPostingContains(t, fixed8Flat, keycodec.U64ByteString(34), 10_017, 1_010_017)
	fixed8Flat.Release()

	fixed8ChunkedMap := GetPostingMap()
	for i := 0; i < FieldChunkThreshold; i++ {
		fixed8ChunkedMap[keycodec.U64ByteString(uint64(i*2))] = fieldStorageOwnedTestPosting(uint64(i + 10_000))
	}
	fixed8Chunked := NewRegularFieldStorageFromPostingMapOwned(fixed8ChunkedMap, true)
	if !fixed8Chunked.IsChunked() {
		fixed8Chunked.Release()
		t.Fatalf("threshold fixed8 string map storage is flat")
	}
	fixed8Chunked.Release()

	fixedFlatMap := GetFixedPostingMap()
	for i := 0; i < FieldChunkThreshold-1; i++ {
		fixedFlatMap[uint64(i*2)] = fieldStorageOwnedTestPosting(uint64(i + 20_000))
	}
	fixedFlat := NewRegularFieldStorageFromFixedPostingMapOwned(fixedFlatMap)
	if fixedFlat.IsChunked() {
		fixedFlat.Release()
		t.Fatalf("small fixed posting map storage is chunked")
	}
	fieldStorageAssertPostingContains(t, fixedFlat, keycodec.U64ByteString(34), 20_017, 1_020_017)
	fixedFlat.Release()

	fixedChunkedMap := GetFixedPostingMap()
	for i := 0; i < FieldChunkThreshold; i++ {
		fixedChunkedMap[uint64(i*2)] = fieldStorageOwnedTestPosting(uint64(i + 20_000))
	}
	fixedChunked := NewRegularFieldStorageFromFixedPostingMapOwned(fixedChunkedMap)
	if !fixedChunked.IsChunked() {
		fixedChunked.Release()
		t.Fatalf("threshold fixed posting map storage is flat")
	}
	fixedChunked.Release()
}

func TestFieldStorageFromRunsOwned_MergesStringRuns(t *testing.T) {
	left := GetPostingMap()
	left["name"] = fieldStoragePosting(777)
	runLeft := NewStringFieldStorageRunFromPostingMap(left)
	ReleasePostingMap(left)

	right := GetPostingMap()
	right["name"] = fieldStoragePosting(1, 2)
	right["zip"] = fieldStoragePosting(9)
	runRight := NewStringFieldStorageRunFromPostingMap(right)
	ReleasePostingMap(right)

	storage := NewRegularFieldStorageFromRunsOwned([]FieldStorageRun{runLeft, runRight})
	defer storage.Release()

	if storage.KeyCount() != 2 {
		t.Fatalf("unexpected key count: got %d want 2", storage.KeyCount())
	}
	fieldStorageAssertPostingContains(t, storage, "name", 1, 2, 777)
	fieldStorageAssertPostingContains(t, storage, "zip", 9)
}

func TestFieldStorageRunFromPostingMapOwned_DrainsMaps(t *testing.T) {
	strs := GetPostingMap()
	strs["b"] = fieldStoragePosting(2)
	strs["a"] = fieldStoragePosting(1)
	strs["empty"] = posting.List{}
	stringRun := NewStringFieldStorageRunFromPostingMap(strs)
	defer stringRun.ReleaseOwned()

	if len(strs) != 0 {
		t.Fatalf("string map retained entries after run construction")
	}
	ReleasePostingMap(strs)
	if stringRun.KeyCount() != 2 {
		t.Fatalf("string run key count: got %d want 2", stringRun.KeyCount())
	}
	if !keycodec.EqualsString(stringRun.keyAt(0), "a") || !keycodec.EqualsString(stringRun.keyAt(1), "b") {
		t.Fatalf("string run keys are not sorted")
	}

	fixed := GetFixedPostingMap()
	fixed[4] = fieldStoragePosting(4)
	fixed[2] = fieldStoragePosting(2)
	fixed[8] = posting.List{}
	fixedRun := NewFixedFieldStorageRunFromPostingMap(fixed)
	defer fixedRun.ReleaseOwned()

	if len(fixed) != 0 {
		t.Fatalf("fixed map retained entries after run construction")
	}
	ReleaseFixedPostingMap(fixed)
	if fixedRun.KeyCount() != 2 {
		t.Fatalf("fixed run key count: got %d want 2", fixedRun.KeyCount())
	}
	if fixedRun.keyAt(0).U64() != 2 || fixedRun.keyAt(1).U64() != 4 {
		t.Fatalf("fixed run keys are not sorted")
	}
}

func TestFieldStorageFromRunsOwned_MergesFixedRuns(t *testing.T) {
	left := GetFixedPostingMap()
	left[4] = fieldStoragePosting(40)
	runLeft := NewFixedFieldStorageRunFromPostingMap(left)
	ReleaseFixedPostingMap(left)

	right := GetFixedPostingMap()
	right[2] = fieldStoragePosting(20)
	right[4] = fieldStoragePosting(41, 42)
	runRight := NewFixedFieldStorageRunFromPostingMap(right)
	ReleaseFixedPostingMap(right)

	runs := []FieldStorageRun{runLeft, runRight}
	storage := NewRegularFieldStorageFromRunsOwned(runs)
	defer storage.Release()

	if runs[0].u64Buf != nil || runs[0].postBuf != nil || runs[1].u64Buf != nil || runs[1].postBuf != nil {
		t.Fatalf("owned runs were not released after materialization")
	}
	if storage.KeyCount() != 2 {
		t.Fatalf("unexpected fixed key count: got %d want 2", storage.KeyCount())
	}
	fieldStorageAssertPostingContains(t, storage, keycodec.U64ByteString(2), 20)
	fieldStorageAssertPostingContains(t, storage, keycodec.U64ByteString(4), 40, 41, 42)
}

func TestFieldStorageFromRunsOwned_FixedRunsChunkAtThreshold(t *testing.T) {
	left := GetFixedPostingMap()
	for i := 0; i < FieldChunkThreshold/2; i++ {
		left[uint64(i*2)] = fieldStorageOwnedTestPosting(uint64(i + 1))
	}
	runLeft := NewFixedFieldStorageRunFromPostingMap(left)
	ReleaseFixedPostingMap(left)

	right := GetFixedPostingMap()
	for i := FieldChunkThreshold / 2; i < FieldChunkThreshold; i++ {
		right[uint64(i*2)] = fieldStorageOwnedTestPosting(uint64(i + 1))
	}
	runRight := NewFixedFieldStorageRunFromPostingMap(right)
	ReleaseFixedPostingMap(right)

	storage := NewRegularFieldStorageFromRunsOwned([]FieldStorageRun{runLeft, runRight})
	defer storage.Release()

	if !storage.IsChunked() {
		t.Fatalf("threshold fixed run storage is flat")
	}
	fieldStorageAssertPostingContains(t, storage, keycodec.U64ByteString(34), 18, 1_000_018)
}

func TestNilFieldStorageOwned(t *testing.T) {
	empty := NewNilFieldStorageOwned(posting.List{})
	if empty.KeyCount() != 0 {
		empty.Release()
		t.Fatalf("empty nil storage key count: got %d want 0", empty.KeyCount())
	}

	storage := NewNilFieldStorageOwned(fieldStoragePosting(1, 3, 5))
	defer storage.Release()

	if storage.KeyCount() != 1 {
		t.Fatalf("nil storage key count: got %d want 1", storage.KeyCount())
	}
	fieldStorageAssertPostingContains(t, storage, NilIndexEntryKey, 1, 3, 5)
}

func TestLenFieldStorageFromMapOwned_ZeroComplement(t *testing.T) {
	universe := fieldStoragePosting(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	lengths := map[uint32]posting.List{
		0: fieldStoragePosting(1, 2, 3, 4, 5, 6),
		2: fieldStoragePosting(7, 8, 9),
		3: fieldStoragePosting(10),
	}

	storage, useZeroComplement := NewLenFieldStorageFromMapOwned(universe, lengths)
	defer storage.Release()
	universe.Release()

	if !useZeroComplement {
		t.Fatalf("expected zero complement")
	}
	ov := NewFieldIndexViewFromStorage(storage)
	if ov.LookupCardinality(keycodec.U64ByteString(0)) != 0 {
		t.Fatalf("zero posting must be omitted when zero complement is used")
	}
	fieldStorageAssertPostingContains(t, storage, keycodec.U64ByteString(2), 7, 8, 9)
	fieldStorageAssertPostingContains(t, storage, keycodec.U64ByteString(3), 10)
	fieldStorageAssertPostingContains(t, storage, LenIndexNonEmptyKey, 7, 8, 9, 10)

	cur := ov.NewCursor(ov.RangeByRanks(0, ov.KeyCount()), false)
	key, _, ok := cur.Next()
	if !ok || keycodec.Compare(key, keycodec.FromU64(2)) != 0 {
		t.Fatalf("entry 0 key: got %q want len=2", key.UnsafeString())
	}
	key, _, ok = cur.Next()
	if !ok || keycodec.Compare(key, keycodec.FromU64(3)) != 0 {
		t.Fatalf("entry 1 key: got %q want len=3", key.UnsafeString())
	}
	key, _, ok = cur.Next()
	if !ok || !keycodec.EqualsString(key, LenIndexNonEmptyKey) {
		t.Fatalf("entry 2 key: got %q want non-empty marker", key.UnsafeString())
	}
	if _, _, ok = cur.Next(); ok {
		t.Fatalf("unexpected extra len-index entry")
	}
}

func TestLenFieldStorageFromMapOwned_StoresZeroWhenComplementIsNotUsed(t *testing.T) {
	universe := fieldStoragePosting(1, 2, 3, 4, 5, 6)
	lengths := map[uint32]posting.List{
		0: fieldStoragePosting(1, 2),
		1: fieldStoragePosting(3, 4),
		2: fieldStoragePosting(5, 6),
	}

	storage, useZeroComplement := NewLenFieldStorageFromMapOwned(universe, lengths)
	defer storage.Release()
	universe.Release()

	if useZeroComplement {
		t.Fatalf("zero complement is not expected when non-empty cardinality is not smaller")
	}
	fieldStorageAssertPostingContains(t, storage, keycodec.U64ByteString(0), 1, 2)
	fieldStorageAssertPostingContains(t, storage, keycodec.U64ByteString(1), 3, 4)
	fieldStorageAssertPostingContains(t, storage, keycodec.U64ByteString(2), 5, 6)
	if NewFieldIndexViewFromStorage(storage).LookupCardinality(LenIndexNonEmptyKey) != 0 {
		t.Fatalf("unexpected non-empty marker")
	}
}

func TestRebuildLenFieldStorageFromIndexView_CountsValuesAndStoresZero(t *testing.T) {
	universe := fieldStoragePosting(1, 2, 3, 4, 5, 6)
	entries := []Entry{
		{Key: keycodec.FromStoredString("a", false), IDs: fieldStoragePosting(1, 3)},
		{Key: keycodec.FromStoredString("b", false), IDs: fieldStoragePosting(1, 2, 3)},
		{Key: keycodec.FromStoredString("c", false), IDs: fieldStoragePosting(3)},
	}
	base := newRegularFieldStorage(entries)
	defer base.Release()

	storage, useZeroComplement := RebuildLenFieldStorageFromIndexView(universe, NewFieldIndexViewFromStorage(base))
	defer storage.Release()
	universe.Release()

	if useZeroComplement {
		t.Fatalf("zero complement is not expected when empty and non-empty cardinalities match")
	}
	fieldStorageAssertPostingContains(t, storage, keycodec.U64ByteString(0), 4, 5, 6)
	fieldStorageAssertPostingContains(t, storage, keycodec.U64ByteString(1), 2)
	fieldStorageAssertPostingContains(t, storage, keycodec.U64ByteString(2), 1)
	fieldStorageAssertPostingContains(t, storage, keycodec.U64ByteString(3), 3)
	if NewFieldIndexViewFromStorage(storage).LookupCardinality(LenIndexNonEmptyKey) != 0 {
		t.Fatalf("unexpected non-empty complement posting")
	}
}

func TestRebuildLenFieldStorageFromIndexView_UsesZeroComplement(t *testing.T) {
	universe := fieldStoragePosting(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	entries := []Entry{
		{Key: keycodec.FromStoredString("a", false), IDs: fieldStoragePosting(7, 8)},
		{Key: keycodec.FromStoredString("b", false), IDs: fieldStoragePosting(8, 9)},
		{Key: keycodec.FromStoredString("c", false), IDs: fieldStoragePosting(10)},
	}
	base := newRegularFieldStorage(entries)
	defer base.Release()

	storage, useZeroComplement := RebuildLenFieldStorageFromIndexView(universe, NewFieldIndexViewFromStorage(base))
	defer storage.Release()
	universe.Release()

	if !useZeroComplement {
		t.Fatalf("expected zero complement")
	}
	ov := NewFieldIndexViewFromStorage(storage)
	if ov.LookupCardinality(keycodec.U64ByteString(0)) != 0 {
		t.Fatalf("zero posting must be omitted when zero complement is used")
	}
	fieldStorageAssertPostingContains(t, storage, keycodec.U64ByteString(1), 7, 9, 10)
	fieldStorageAssertPostingContains(t, storage, keycodec.U64ByteString(2), 8)
	fieldStorageAssertPostingContains(t, storage, LenIndexNonEmptyKey, 7, 8, 9, 10)
}

func TestRebuildLenFieldStorageFromIndexViewSurvivesSourceRelease(t *testing.T) {
	universe := fieldStoragePosting(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	entries := []Entry{
		{Key: keycodec.FromStoredString("a", false), IDs: fieldStoragePosting(7, 8)},
		{Key: keycodec.FromStoredString("b", false), IDs: fieldStoragePosting(8, 9)},
		{Key: keycodec.FromStoredString("c", false), IDs: fieldStoragePosting(10)},
	}
	base := newRegularFieldStorage(entries)
	if base.flat == nil || base.flat.stringData == nil {
		base.Release()
		t.Fatalf("expected flat string source storage")
	}

	storage, useZeroComplement := RebuildLenFieldStorageFromIndexView(universe, NewFieldIndexViewFromStorage(base))
	if !useZeroComplement {
		storage.Release()
		base.Release()
		universe.Release()
		t.Fatalf("expected zero complement")
	}

	oldData := base.flat.stringData
	base.Release()
	poisonBytes(oldData)
	universe.Release()
	defer storage.Release()

	ov := NewFieldIndexViewFromStorage(storage)
	if ov.LookupCardinality(keycodec.U64ByteString(0)) != 0 {
		t.Fatalf("zero posting must be omitted when zero complement is used")
	}
	fieldStorageAssertPostingContains(t, storage, keycodec.U64ByteString(1), 7, 9, 10)
	fieldStorageAssertPostingContains(t, storage, keycodec.U64ByteString(2), 8)
	fieldStorageAssertPostingContains(t, storage, LenIndexNonEmptyKey, 7, 8, 9, 10)
}

func TestRebuildLenFieldStorageFromChunkedIndexViewSurvivesSourceRelease(t *testing.T) {
	const total = fieldIndexChunkThreshold + 7
	entries := make([]Entry, total)
	var universe posting.List
	for i := 0; i < total; i++ {
		id := uint64(i + 1)
		entries[i] = Entry{
			Key: keycodec.FromStoredString(fmt.Sprintf("len-chunk/%04d", i), false),
			IDs: fieldStorageSingleton(id),
		}
		universe = universe.BuildAdded(id)
	}
	universe = universe.BuildAdded(total + 1)
	universe = universe.BuildAdded(total + 2)
	universe = universe.BuildAdded(total + 3)

	base := newRegularFieldStorage(entries)
	if !base.IsChunked() {
		base.Release()
		universe.Release()
		t.Fatalf("expected chunked source storage")
	}

	oldData := make([][]byte, 0, base.chunked.chunkCount)
	for i := range base.chunked.pages {
		page := base.chunked.pages[i]
		for j := range page.refs {
			oldData = append(oldData, page.refs[j].chunk.stringData)
		}
	}

	storage, useZeroComplement := RebuildLenFieldStorageFromIndexView(universe, NewFieldIndexViewFromStorage(base))
	if useZeroComplement {
		storage.Release()
		base.Release()
		universe.Release()
		t.Fatalf("zero complement is not expected")
	}

	base.Release()
	for i := range oldData {
		poisonBytes(oldData[i])
	}
	universe.Release()
	defer storage.Release()

	ids := NewFieldIndexViewFromStorage(storage).LookupPostingRetained(keycodec.U64ByteString(1))
	defer ids.Release()
	if ids.Cardinality() != total || !ids.Contains(1) || !ids.Contains(fieldIndexChunkThreshold) || !ids.Contains(total) {
		t.Fatalf("len=1 bucket mismatch: cardinality=%d ids=%v", ids.Cardinality(), ids)
	}
	fieldStorageAssertPostingContains(t, storage, keycodec.U64ByteString(0), total+1, total+2, total+3)
	if NewFieldIndexViewFromStorage(storage).LookupCardinality(LenIndexNonEmptyKey) != 0 {
		t.Fatalf("unexpected non-empty marker")
	}
}

func TestFieldStorageSlotsRetainSharedStorage(t *testing.T) {
	tests := []struct {
		name string
		rows int
	}{
		{name: "Flat", rows: 8},
		{name: "Chunked", rows: fieldIndexChunkThreshold + 11},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entries := fieldStorageEntriesForTest(tc.rows, true)
			storage := newRegularFieldStorage(entries)
			prev := GetFieldStorageSlice(1)
			prev = append(prev, storage)
			if FieldStorageSlotsApproxBytes(prev) == 0 {
				t.Fatalf("expected non-zero field storage slot size")
			}
			next := CloneFieldStorageSlots(prev, 1)
			RetainSharedFieldStorageSlots(next, prev)
			ReleaseFieldStorageSlots(prev)

			fieldStorageAssertPostingContains(t, next[0], keycodec.U64ByteString(0), 1)
			ReleaseFieldStorageSlots(next)
		})
	}
}
