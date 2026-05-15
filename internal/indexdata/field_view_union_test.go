package indexdata

import "testing"

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
