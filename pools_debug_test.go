//go:build rbidebug

package rbi

import (
	"fmt"
	"testing"
)

func TestBuildLenIndex_ReleasesEmptyScratchBitmap(t *testing.T) {
	db, _ := openTempDBUint64(t)

	for i := 1; i <= 64; i++ {
		rec := &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Email: fmt.Sprintf("u_%d@example.test", i),
			Tags:  []string{"go"},
		}
		if err := db.Set(uint64(i), rec); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	db.ResetPoolStats()
	db.buildLenIndex()

	stats := db.PoolStatsSinceReset()
	if stats.Scratch.RoaringBitmap.Gets != 1 {
		t.Fatalf("expected one pooled roaring bitmap get, got %+v", stats.Scratch.RoaringBitmap)
	}
	if stats.Scratch.RoaringBitmap.Puts != 1 {
		t.Fatalf("expected empty len-index scratch bitmap to be returned to pool, got %+v", stats.Scratch.RoaringBitmap)
	}
}

func TestBuildLenIndex_ReleasesZeroComplementScratchBitmap(t *testing.T) {
	db, _ := openTempDBUint64(t)

	for i := 1; i <= 200; i++ {
		rec := &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Email: fmt.Sprintf("u_%d@example.test", i),
		}
		switch {
		case i%5 == 0:
			rec.Tags = []string{"go", "db"}
		case i%7 == 0:
			rec.Tags = []string{"rust"}
		default:
			rec.Tags = nil
		}
		if err := db.Set(uint64(i), rec); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}
	if !db.isLenZeroComplementField("tags") {
		t.Fatalf("expected zero-complement mode for tags")
	}

	db.ResetPoolStats()
	db.buildLenIndex()

	stats := db.PoolStatsSinceReset()
	if stats.Scratch.RoaringBitmap.Gets != 2 {
		t.Fatalf("expected two pooled roaring bitmap gets (empty + nonEmpty clone), got %+v", stats.Scratch.RoaringBitmap)
	}
	if stats.Scratch.RoaringBitmap.Puts != 1 {
		t.Fatalf("expected only discarded empty scratch bitmap to be returned to pool, got %+v", stats.Scratch.RoaringBitmap)
	}
}
