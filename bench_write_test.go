package rbi

import (
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"testing"

	"go.etcd.io/bbolt"
)

const writeBenchSeedBatch = 20_000

func buildWriteBenchDB(b *testing.B) (*DB[uint64, UserBench], *bbolt.DB, uint64) {
	b.Helper()

	dir := b.TempDir()
	path := filepath.Join(dir, "bench_write_seeded.db")

	db, raw := openBoltAndNew[uint64, UserBench](b, path, Options{
		DisableIndexStore: true,
	})
	b.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	db.DisableSync()

	seedCount := 200_000

	r := newRand(42)
	countries := []string{"US", "NL", "DE", "PL", "SE", "FR", "GB", "ES"}
	plans := []string{"free", "basic", "pro", "enterprise"}

	ids := make([]uint64, 0, writeBenchSeedBatch)
	vals := make([]*UserBench, 0, writeBenchSeedBatch)

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

		if len(ids) >= writeBenchSeedBatch {
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

	db.EnableSync()

	return db, raw, uint64(seedCount)
}

func rawSetBench(db *DB[uint64, UserBench], raw *bbolt.DB, id uint64, rec *UserBench) error {
	b := getEncodeBuf()
	defer releaseEncodeBuf(b)

	if err := db.encode(rec, b); err != nil {
		return fmt.Errorf("encode: %w", err)
	}

	return raw.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(db.bucket)
		if bucket == nil {
			return fmt.Errorf("bucket does not exist")
		}
		bucket.FillPercent = db.options.BucketFillPercent
		if err := bucket.Put(db.keyFromID(id), b.Bytes()); err != nil {
			return fmt.Errorf("put: %w", err)
		}
		return nil
	})
}

func rawPatchBench(db *DB[uint64, UserBench], raw *bbolt.DB, id uint64, patch []Field) error {
	return raw.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(db.bucket)
		if bucket == nil {
			return fmt.Errorf("bucket does not exist")
		}

		key := db.keyFromID(id)
		oldBytes := bucket.Get(key)
		if oldBytes == nil {
			return ErrRecordNotFound
		}

		oldVal, err := db.decode(oldBytes)
		if err != nil {
			return fmt.Errorf("decode old: %w", err)
		}
		newVal, err := db.decode(oldBytes)
		if err != nil {
			db.ReleaseRecords(oldVal)
			return fmt.Errorf("decode new: %w", err)
		}
		defer db.ReleaseRecords(oldVal, newVal)

		if err = db.applyPatch(newVal, patch, true); err != nil {
			return fmt.Errorf("apply patch: %w", err)
		}

		b := getEncodeBuf()
		defer releaseEncodeBuf(b)
		if err = db.encode(newVal, b); err != nil {
			return fmt.Errorf("encode: %w", err)
		}

		bucket.FillPercent = db.options.BucketFillPercent
		if err = bucket.Put(key, b.Bytes()); err != nil {
			return fmt.Errorf("put: %w", err)
		}
		return nil
	})
}

func Benchmark_Write_Set_New_Indexed(b *testing.B) {
	db, _, startOffset := buildWriteBenchDB(b)

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
	db, raw, startOffset := buildWriteBenchDB(b)

	rec := &UserBench{Name: "new", Age: 20, Country: "DE"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		id := startOffset + uint64(i) + 1
		if err := rawSetBench(db, raw, id, rec); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Write_Update_Indexed(b *testing.B) {
	db, _, _ := buildWriteBenchDB(b)
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
	db, raw, _ := buildWriteBenchDB(b)
	targetID := uint64(1000)

	recA := &UserBench{Name: "A", Age: 20, Country: "US", Tags: []string{"a"}}
	recB := &UserBench{Name: "B", Age: 30, Country: "DE", Tags: []string{"b"}}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var rec *UserBench
		if i%2 == 0 {
			rec = recA
		} else {
			rec = recB
		}
		if err := rawSetBench(db, raw, targetID, rec); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Write_Patch_Indexed(b *testing.B) {
	db, _, _ := buildWriteBenchDB(b)
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
	db, raw, _ := buildWriteBenchDB(b)
	targetID := uint64(2000)

	patchA := []Field{{Name: "age", Value: 100}}
	patchB := []Field{{Name: "age", Value: 200}}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var patch []Field
		if i%2 == 0 {
			patch = patchA
		} else {
			patch = patchB
		}
		if err := rawPatchBench(db, raw, targetID, patch); err != nil {
			b.Fatal(err)
		}
	}
}
