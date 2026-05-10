package rbi

import (
	"fmt"
	"path/filepath"
	"testing"
)

type indexStatsTestRec struct {
	Name string `db:"name" rbi:"index"`
	Rank *int   `db:"rank" rbi:"index"`
}

func openTempDBUint64IndexStats(t *testing.T, options ...Options) *DB[uint64, indexStatsTestRec] {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "index_stats.db")
	db, raw := openBoltAndNew[uint64, indexStatsTestRec](t, path, options...)

	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	return db
}

func TestIndexStats_ReportsFieldsAndTotals(t *testing.T) {
	db := openTempDBUint64IndexStats(t)

	const total = 96
	rankNil := 0
	var rankSeen [7]bool
	for i := 0; i < total; i++ {
		var rank *int
		if i%5 != 0 {
			rank = new(int)
			*rank = i % 7
			rankSeen[i%7] = true
		} else {
			rankNil++
		}
		if err := db.Set(uint64(i+1), &indexStatsTestRec{
			Name: fmt.Sprintf("user_%04d", i),
			Rank: rank,
		}); err != nil {
			t.Fatalf("Set(%d): %v", i+1, err)
		}
	}
	rankUnique := 0
	for i := range rankSeen {
		if rankSeen[i] {
			rankUnique++
		}
	}
	rankEntries := rankUnique + 1

	got := db.IndexStats()
	if got.UniqueFieldKeys["name"] != total {
		t.Fatalf("name unique keys=%d, want %d", got.UniqueFieldKeys["name"], total)
	}
	if got.UniqueFieldKeys["rank"] != uint64(rankUnique) {
		t.Fatalf("rank unique keys=%d, want %d", got.UniqueFieldKeys["rank"], rankUnique)
	}
	if got.FieldTotalCardinality["name"] != total {
		t.Fatalf("name cardinality=%d, want %d", got.FieldTotalCardinality["name"], total)
	}
	if got.FieldTotalCardinality["rank"] != total {
		t.Fatalf("rank cardinality=%d, want %d", got.FieldTotalCardinality["rank"], total)
	}
	if got.EntryCount != uint64(total+rankEntries) {
		t.Fatalf("entry count=%d, want %d", got.EntryCount, total+rankEntries)
	}
	if got.PostingCardinality != total*2 {
		t.Fatalf("posting cardinality=%d, want %d", got.PostingCardinality, total*2)
	}
	if rankNil == 0 {
		t.Fatalf("test setup did not generate nil rank rows")
	}

	var structSum uint64
	var sizeSum uint64
	var keySum uint64
	var cardSum uint64
	var heapSum uint64
	for _, name := range []string{"name", "rank"} {
		if got.FieldSize[name] == 0 {
			t.Fatalf("expected FieldSize[%q] to be populated", name)
		}
		if got.FieldKeyBytes[name] == 0 {
			t.Fatalf("expected FieldKeyBytes[%q] to be populated", name)
		}
		fieldStruct := got.FieldApproxStructBytes[name]
		fieldHeap := got.FieldApproxHeapBytes[name]
		if fieldHeap != got.FieldSize[name]+got.FieldKeyBytes[name]+fieldStruct {
			t.Fatalf("field heap mismatch for %q: got=%d want=%d", name, fieldHeap, got.FieldSize[name]+got.FieldKeyBytes[name]+fieldStruct)
		}
		sizeSum += got.FieldSize[name]
		keySum += got.FieldKeyBytes[name]
		cardSum += got.FieldTotalCardinality[name]
		structSum += fieldStruct
		heapSum += fieldHeap
	}
	if sizeSum != got.Size {
		t.Fatalf("FieldSize sum mismatch: got=%d want=%d", sizeSum, got.Size)
	}
	if keySum != got.KeyBytes {
		t.Fatalf("FieldKeyBytes sum mismatch: got=%d want=%d", keySum, got.KeyBytes)
	}
	if cardSum != got.PostingCardinality {
		t.Fatalf("FieldTotalCardinality sum mismatch: got=%d want=%d", cardSum, got.PostingCardinality)
	}
	if structSum != got.ApproxStructBytes {
		t.Fatalf("FieldApproxStructBytes sum mismatch: got=%d want=%d", structSum, got.ApproxStructBytes)
	}
	if heapSum != got.ApproxHeapBytes {
		t.Fatalf("FieldApproxHeapBytes sum mismatch: got=%d want=%d", heapSum, got.ApproxHeapBytes)
	}
}
