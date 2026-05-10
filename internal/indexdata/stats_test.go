package indexdata

import (
	"testing"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
)

func TestCollectChunkedFieldIndexStats_NumericOwnerChunkCountsOnlyOwnersAsStructural(t *testing.T) {
	const size = 64
	ownerPairs := make([]uint64, size*2)
	for i := 0; i < size; i++ {
		base := i << 1
		ownerPairs[base] = uint64(i + 1)
		ownerPairs[base+1] = uint64(i + 1)
	}
	ownerChunk := newUniqueNumericFieldIndexChunk(ownerPairs)
	if ownerChunk == nil || !ownerChunk.hasUniqueNumericOwners() {
		t.Fatalf("expected numeric owner chunk")
	}
	ownerRoot := newFieldIndexChunkedRootFromPages([]*fieldIndexChunkDirPage{newFieldIndexChunkDirPage([]fieldIndexChunkRef{{
		last:  ownerChunk.keyAt(size - 1),
		chunk: ownerChunk,
	}})})
	if ownerRoot == nil {
		t.Fatalf("expected owner root")
	}
	defer ownerRoot.release()

	ownerStats := ownerRoot.stats(true)

	baseCap := cap(ownerChunk.numeric)
	grown := make([]uint64, len(ownerChunk.numeric), baseCap+64)
	copy(grown, ownerChunk.numeric)
	ownerChunk.numeric = grown
	grownStats := ownerRoot.stats(true)
	ownerChunk.numeric = ownerPairs

	gotDelta := grownStats.ApproxStructBytes - ownerStats.ApproxStructBytes
	wantDelta := uint64((cap(grown)-baseCap)>>1) * uint64(unsafe.Sizeof(uint64(0)))
	if gotDelta != wantDelta {
		t.Fatalf("owner chunk structural delta mismatch: got=%d want=%d", gotDelta, wantDelta)
	}
}

func TestFieldStorageStats_FlatAndChunked(t *testing.T) {
	flatEntries := []Entry{
		{Key: keycodec.FromStoredString("a", false), IDs: fieldStorageSingleton(1)},
		{Key: keycodec.FromStoredString("b", false), IDs: fieldStorageSingleton(2).BuildAdded(22)},
		{Key: keycodec.FromStoredString("c", false), IDs: fieldStorageSingleton(3)},
	}
	flat := newRegularFieldStorage(flatEntries)
	flatStats := flat.Stats(true)
	if flatStats.EntryCount != 3 || flatStats.Unique != 3 || flatStats.PostingCardinality != 4 || flatStats.KeyBytes != 3 {
		flat.Release()
		t.Fatalf("flat stats mismatch: %+v", flatStats)
	}
	noDistinct := flat.Stats(false)
	if noDistinct.Unique != 0 || noDistinct.KeyBytes != 0 || noDistinct.EntryCount != 3 || noDistinct.PostingCardinality != 4 {
		flat.Release()
		t.Fatalf("flat no-distinct stats mismatch: %+v", noDistinct)
	}
	flat.Release()

	chunkedEntries := fieldStorageEntriesForTest(fieldIndexChunkThreshold+17, true)
	chunked := newRegularFieldStorage(chunkedEntries)
	if !chunked.IsChunked() {
		chunked.Release()
		t.Fatalf("expected chunked storage")
	}
	chunkedStats := chunked.Stats(true)
	if chunkedStats.EntryCount != uint64(len(chunkedEntries)) ||
		chunkedStats.Unique != uint64(len(chunkedEntries)) ||
		chunkedStats.PostingCardinality != uint64(len(chunkedEntries)) ||
		chunkedStats.KeyBytes != uint64(len(chunkedEntries)*8) {
		chunked.Release()
		t.Fatalf("chunked stats mismatch: %+v", chunkedStats)
	}
	chunked.Release()
}
