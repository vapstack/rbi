package rbi

import (
	"fmt"
	"testing"

	"github.com/vapstack/rbi/internal/posting"
)

func fieldStorageSingleton(id uint64) posting.List {
	return (posting.List{}).BuildAdded(id)
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
