package indexdata

import (
	"testing"

	"github.com/vapstack/rbi/internal/keycodec"
)

func TestFieldIndexViewRangePostingsUnionAndMerge(t *testing.T) {
	tests := []struct {
		name string
		rows int
	}{
		{name: "Flat", rows: 96},
		{name: "Chunked", rows: fieldIndexChunkThreshold + 64},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entries := fieldStorageEntriesForTest(tc.rows, true)
			storage := newRegularFieldStorage(entries)
			defer storage.Release()
			ov := NewFieldIndexViewFromStorage(storage)

			first := ov.RangeByRanks(10, 40)
			second := ov.RangeByRanks(30, 60)
			union := ov.UnionRangePostings(first, second)
			if got := union.Cardinality(); got != 50 {
				union.Release()
				t.Fatalf("union cardinality: got %d want 50", got)
			}
			for id := uint64(11); id <= 60; id++ {
				if !union.Contains(id) {
					union.Release()
					t.Fatalf("union missing id %d", id)
				}
			}
			if union.Contains(10) || union.Contains(61) {
				union.Release()
				t.Fatalf("union contains row outside requested ranges")
			}
			union.Release()

			direct := ov.MergeRangePostingsInto(
				fieldStorageSingleton(900_000),
				ov.RangeByRanks(1, 6),
				ov.RangeByRanks(6, 10),
			)
			if direct.Cardinality() != 10 || !direct.Contains(900_000) {
				got := direct.ToArray()
				direct.Release()
				t.Fatalf("direct merge result mismatch: %v", got)
			}
			for id := uint64(2); id <= 10; id++ {
				if !direct.Contains(id) {
					direct.Release()
					t.Fatalf("direct merge missing id %d", id)
				}
			}
			direct.Release()

			merged := ov.MergeRangePostingsInto(fieldStorageSingleton(900_001), first, second)
			if merged.Cardinality() != 51 || !merged.Contains(900_001) {
				got := merged.ToArray()
				merged.Release()
				t.Fatalf("large merge result mismatch: %v", got)
			}
			for id := uint64(11); id <= 60; id++ {
				if !merged.Contains(id) {
					merged.Release()
					t.Fatalf("large merge missing id %d", id)
				}
			}
			merged.Release()
		})
	}
}

func TestFieldIndexViewRangePostingsUnionBatchesUnorderedSingletons(t *testing.T) {
	const rows = 4097
	entries := make([]Entry, rows)
	for i := 0; i < rows; i++ {
		entries[i] = Entry{
			Key: keycodec.FromU64(uint64(i)),
			IDs: fieldStorageSingleton(uint64(rows-i) * 17),
		}
	}
	storage := newRegularFieldStorage(entries)
	defer storage.Release()

	ov := NewFieldIndexViewFromStorage(storage)
	first := ov.RangeByRanks(0, rows/2)
	second := ov.RangeByRanks(rows/2, rows)

	union := ov.UnionRangePostings(first, second)
	if got := union.Cardinality(); got != rows {
		gotIDs := union.ToArray()
		union.Release()
		t.Fatalf("union cardinality: got %d want %d ids=%v", got, rows, gotIDs)
	}
	for i := 0; i < rows; i++ {
		id := uint64(rows-i) * 17
		if !union.Contains(id) {
			union.Release()
			t.Fatalf("union missing id %d", id)
		}
	}
	union.Release()

	const dstID = 1 << 40
	merged := ov.MergeRangePostingsInto(fieldStorageSingleton(dstID), first, second)
	if got := merged.Cardinality(); got != rows+1 || !merged.Contains(dstID) {
		gotIDs := merged.ToArray()
		merged.Release()
		t.Fatalf("merge result mismatch: card=%d ids=%v", got, gotIDs)
	}
	for i := 0; i < rows; i++ {
		id := uint64(rows-i) * 17
		if !merged.Contains(id) {
			merged.Release()
			t.Fatalf("merge missing id %d", id)
		}
	}
	merged.Release()
}

func TestFieldIndexPostingUnionBuilderMergesFlushedSingletonChunks(t *testing.T) {
	var builder fieldIndexPostingUnionBuilder
	for i := 0; i < fieldIndexSingleChunkCap+257; i++ {
		builder.addSingle(uint64(fieldIndexSingleChunkCap + 257 - i))
	}

	got := builder.finish(false)
	defer got.Release()

	want := uint64(fieldIndexSingleChunkCap + 257)
	if card := got.Cardinality(); card != want {
		t.Fatalf("cardinality got %d want %d", card, want)
	}
	for _, id := range []uint64{1, 17, uint64(fieldIndexSingleChunkCap), want} {
		if !got.Contains(id) {
			t.Fatalf("missing id %d", id)
		}
	}
}
