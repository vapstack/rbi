package rbi

import (
	"math"
	"path/filepath"
	"strconv"
	"testing"
)

func buildWriteBenchDB(b *testing.B) (*DB[uint64, UserBench], uint64) {
	b.Helper()

	dir := b.TempDir()
	path := filepath.Join(dir, "bench_write_seeded.db")

	opts := DefaultOptions()
	opts.DisableIndexRebuild = true
	db, raw := openBoltAndNew[uint64, UserBench](b, path, opts)
	b.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	db.DisableSync()

	seedCount := 200_000

	db.DisableIndexing()

	r := newRand(42)
	countries := []string{"US", "NL", "DE", "PL", "SE", "FR", "GB", "ES"}
	plans := []string{"free", "basic", "pro", "enterprise"}

	ids := make([]uint64, 0, 1000)
	vals := make([]*UserBench, 0, 1000)

	for i := 1; i <= seedCount; i++ {
		rec := &UserBench{
			Country: countries[r.IntN(len(countries))],
			Plan:    plans[r.IntN(len(plans))],
			Age:     18 + r.IntN(60),
			Score:   math.Round(r.Float64()*100000) / 100,
			Name:    "user-" + strconv.Itoa(i),
			Tags:    []string{"go", "seed"},
		}
		ids = append(ids, uint64(i))
		vals = append(vals, rec)

		if len(ids) >= 1000 {
			if err := db.BatchSet(ids, vals); err != nil {
				b.Fatalf("seed error: %v", err)
			}
			ids = ids[:0]
			vals = vals[:0]
		}
	}
	if len(ids) > 0 {
		if err := db.BatchSet(ids, vals); err != nil {
			b.Fatal(err)
		}
	}

	if err := db.RebuildIndex(); err != nil {
		b.Fatalf("rebuild: %v", err)
	}

	db.EnableSync()
	db.EnableIndexing()

	return db, uint64(seedCount)
}

func Benchmark_Write_Set_New_Indexed(b *testing.B) {
	db, startOffset := buildWriteBenchDB(b)

	rec := &UserBench{Name: "new", Age: 20, Country: "DE"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		id := startOffset + uint64(i) + 1
		if err := db.Set(id, rec); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Write_Set_New_NoIndex(b *testing.B) {
	db, startOffset := buildWriteBenchDB(b)
	db.DisableIndexing()

	rec := &UserBench{Name: "new", Age: 20, Country: "DE"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		id := startOffset + uint64(i) + 1
		if err := db.Set(id, rec); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Write_Update_Indexed(b *testing.B) {
	db, _ := buildWriteBenchDB(b)
	targetID := uint64(1000)

	recA := &UserBench{Name: "A", Age: 20, Country: "US", Tags: []string{"a"}}
	recB := &UserBench{Name: "B", Age: 30, Country: "DE", Tags: []string{"b"}}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			if err := db.Set(targetID, recA); err != nil {
				b.Fatal(err)
			}
		} else {
			if err := db.Set(targetID, recB); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func Benchmark_Write_Update_NoIndex(b *testing.B) {
	db, _ := buildWriteBenchDB(b)
	db.DisableIndexing()

	targetID := uint64(1000)
	recA := &UserBench{Name: "A", Age: 20, Country: "US", Tags: []string{"a"}}
	recB := &UserBench{Name: "B", Age: 30, Country: "DE", Tags: []string{"b"}}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			if err := db.Set(targetID, recA); err != nil {
				b.Fatal(err)
			}
		} else {
			if err := db.Set(targetID, recB); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func Benchmark_Write_Patch_Indexed(b *testing.B) {
	db, _ := buildWriteBenchDB(b)
	targetID := uint64(2000)

	patchA := []Field{{Name: "age", Value: 100}}
	patchB := []Field{{Name: "age", Value: 200}}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			if err := db.Patch(targetID, patchA); err != nil {
				b.Fatal(err)
			}
		} else {
			if err := db.Patch(targetID, patchB); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func Benchmark_Write_Patch_NoIndex(b *testing.B) {
	db, _ := buildWriteBenchDB(b)
	db.DisableIndexing()

	targetID := uint64(2000)
	patchA := []Field{{Name: "age", Value: 100}}
	patchB := []Field{{Name: "age", Value: 200}}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			if err := db.Patch(targetID, patchA); err != nil {
				b.Fatal(err)
			}
		} else {
			if err := db.Patch(targetID, patchB); err != nil {
				b.Fatal(err)
			}
		}
	}
}
