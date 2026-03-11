package rbi

import (
	"encoding/binary"
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	"go.etcd.io/bbolt"
)

const writeBenchSeedBatch = 50_000
const writeBenchUserBatchSize = 1000

func buildWriteBenchDB(b *testing.B) (*DB[uint64, UserBench], *bbolt.DB, uint64) {
	return buildWriteBenchDBWithOptions(b, Options{
		DisableIndexStore: true,
	})
}

func buildWriteBenchDBWithOptions(b *testing.B, opts Options) (*DB[uint64, UserBench], *bbolt.DB, uint64) {
	b.Helper()

	dir := b.TempDir()
	path := filepath.Join(dir, "bench_write_seeded.db")

	db, raw := openBoltAndNew[uint64, UserBench](b, path, opts)
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

func ensureBenchSideBucket(b *testing.B, raw *bbolt.DB, name string) []byte {
	b.Helper()

	bucketName := []byte(name)
	if err := raw.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	}); err != nil {
		b.Fatalf("ensure side bucket %q: %v", name, err)
	}

	return bucketName
}

func appendBenchSideBucket(tx *bbolt.Tx, bucketName, payload []byte) error {
	bucket := tx.Bucket(bucketName)
	if bucket == nil {
		return fmt.Errorf("side bucket %q does not exist", string(bucketName))
	}

	seq, err := bucket.NextSequence()
	if err != nil {
		return fmt.Errorf("side bucket next sequence: %w", err)
	}

	var key [8]byte
	binary.BigEndian.PutUint64(key[:], seq)
	if err = bucket.Put(key[:], payload); err != nil {
		return fmt.Errorf("side bucket put: %w", err)
	}

	return nil
}

func appendBenchAuditLog(tx *bbolt.Tx, bucketName []byte, key uint64, rec *UserBench) error {
	var payload [24]byte
	binary.BigEndian.PutUint64(payload[0:8], key)
	binary.BigEndian.PutUint64(payload[8:16], uint64(rec.Age))
	binary.BigEndian.PutUint64(payload[16:24], math.Float64bits(rec.Score))
	return appendBenchSideBucket(tx, bucketName, payload[:])
}

func writeBenchUpdateRecords() (*UserBench, *UserBench) {
	recA := &UserBench{
		Name:    "A",
		Age:     20,
		Country: "US",
		Plan:    "basic",
		Email:   "a@example.com",
		Tags:    []string{"a"},
		Roles:   []string{"user"},
	}
	recB := &UserBench{
		Name:    "B",
		Age:     30,
		Country: "DE",
		Plan:    "pro",
		Email:   "b@example.com",
		Tags:    []string{"b"},
		Roles:   []string{"user", "admin"},
	}
	return recA, recB
}

func benchWriteRecordForIteration(i int, recA, recB *UserBench) *UserBench {
	if i%2 == 0 {
		return recA
	}
	return recB
}

func buildWriteBenchBatchUpdateInput(batchSize int) ([]uint64, []*UserBench, []*UserBench) {
	ids := make([]uint64, batchSize)
	valsA := make([]*UserBench, batchSize)
	valsB := make([]*UserBench, batchSize)

	countriesA := []string{"US", "DE", "NL", "FR"}
	countriesB := []string{"SE", "PL", "GB", "ES"}
	plansA := []string{"basic", "pro"}
	plansB := []string{"pro", "enterprise"}

	for i := 0; i < batchSize; i++ {
		ids[i] = uint64(i + 1)
		valsA[i] = &UserBench{
			Name:    "batch-a-" + strconv.Itoa(i),
			Age:     20 + (i % 5),
			Country: countriesA[i%len(countriesA)],
			Plan:    plansA[i%len(plansA)],
			Email:   "batch-a-" + strconv.Itoa(i) + "@example.com",
			Tags:    []string{"batch", "a", strconv.Itoa(i % 7)},
			Roles:   []string{"user"},
		}
		valsB[i] = &UserBench{
			Name:    "batch-b-" + strconv.Itoa(i),
			Age:     30 + (i % 5),
			Country: countriesB[i%len(countriesB)],
			Plan:    plansB[i%len(plansB)],
			Email:   "batch-b-" + strconv.Itoa(i) + "@example.com",
			Tags:    []string{"batch", "b", strconv.Itoa(i % 7)},
			Roles:   []string{"user", "admin"},
		}
	}

	return ids, valsA, valsB
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
			return nil
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

	recA, recB := writeBenchUpdateRecords()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := db.Set(targetID, benchWriteRecordForIteration(i, recA, recB)); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Write_Update_NoIndex(b *testing.B) {
	db, raw, _ := buildWriteBenchDB(b)
	targetID := uint64(1000)

	recA, recB := writeBenchUpdateRecords()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := rawSetBench(db, raw, targetID, benchWriteRecordForIteration(i, recA, recB)); err != nil {
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

func Benchmark_Write_Update_BeforeStore(b *testing.B) {
	db, _, _ := buildWriteBenchDB(b)
	targetID := uint64(1000)
	recA, recB := writeBenchUpdateRecords()

	var modifiedTS uint64
	beforeStore := BeforeStore(func(_ uint64, _ *UserBench, newValue *UserBench) error {
		newValue.Score = float64(modifiedTS + 1)
		modifiedTS++
		return nil
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := db.Set(targetID, benchWriteRecordForIteration(i, recA, recB), beforeStore); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Write_Update_BeforeCommit(b *testing.B) {
	db, raw, _ := buildWriteBenchDB(b)
	targetID := uint64(1000)
	recA, recB := writeBenchUpdateRecords()
	auditBucket := ensureBenchSideBucket(b, raw, "bench_audit")

	beforeCommit := BeforeCommit(func(tx *bbolt.Tx, key uint64, _ *UserBench, newValue *UserBench) error {
		return appendBenchAuditLog(tx, auditBucket, key, newValue)
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := db.Set(targetID, benchWriteRecordForIteration(i, recA, recB), beforeCommit); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Write_Update_BeforeStore_BeforeCommit_MakePatch(b *testing.B) {
	db, raw, _ := buildWriteBenchDB(b)
	targetID := uint64(1000)
	recA, recB := writeBenchUpdateRecords()
	patchBucket := ensureBenchSideBucket(b, raw, "bench_patch_log")

	var modifiedTS uint64
	beforeStore := BeforeStore(func(_ uint64, _ *UserBench, newValue *UserBench) error {
		newValue.Score = float64(modifiedTS + 1)
		modifiedTS++
		return nil
	})
	beforeCommit := BeforeCommit(func(tx *bbolt.Tx, _ uint64, oldValue, newValue *UserBench) error {
		patch := db.MakePatch(oldValue, newValue)
		payload, err := msgpack.Marshal(patch)
		if err != nil {
			return fmt.Errorf("marshal patch: %w", err)
		}
		return appendBenchSideBucket(tx, patchBucket, payload)
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := db.Set(targetID, benchWriteRecordForIteration(i, recA, recB), beforeStore, beforeCommit); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Write_Update_BeforeStore_BeforeCommit_MakePatch_BatchSet(b *testing.B) {
	db, raw, _ := buildWriteBenchDB(b)
	ids, valsA, valsB := buildWriteBenchBatchUpdateInput(writeBenchUserBatchSize)
	patchBucket := ensureBenchSideBucket(b, raw, "bench_patch_log")

	var modifiedTS uint64
	beforeStore := BeforeStore(func(_ uint64, _ *UserBench, newValue *UserBench) error {
		newValue.Score = float64(modifiedTS + 1)
		modifiedTS++
		return nil
	})
	beforeCommit := BeforeCommit(func(tx *bbolt.Tx, _ uint64, oldValue, newValue *UserBench) error {
		patch := db.MakePatch(oldValue, newValue)
		payload, err := msgpack.Marshal(patch)
		if err != nil {
			return fmt.Errorf("marshal patch: %w", err)
		}
		return appendBenchSideBucket(tx, patchBucket, payload)
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		vals := valsA
		if i%2 != 0 {
			vals = valsB
		}
		if err := db.BatchSet(ids, vals, beforeStore, beforeCommit); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(writeBenchUserBatchSize, "rows/op")
}

func Benchmark_Write_Update_BeforeCommit_BatchSet(b *testing.B) {
	db, raw, _ := buildWriteBenchDB(b)
	ids, valsA, valsB := buildWriteBenchBatchUpdateInput(writeBenchUserBatchSize)
	auditBucket := ensureBenchSideBucket(b, raw, "bench_audit")

	beforeCommit := BeforeCommit(func(tx *bbolt.Tx, key uint64, _ *UserBench, newValue *UserBench) error {
		return appendBenchAuditLog(tx, auditBucket, key, newValue)
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		vals := valsA
		if i%2 != 0 {
			vals = valsB
		}
		if err := db.BatchSet(ids, vals, beforeCommit); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(writeBenchUserBatchSize, "rows/op")
}

func Benchmark_Write_Update_BeforeStore_BeforeCommit_MakePatch_Parallel(b *testing.B) {
	for _, tc := range []struct {
		name string
		opts Options
	}{
		{
			name: "NoMicroBatching",
			opts: Options{
				DisableIndexStore: true,
				BatchWindow:       -1,
				BatchMax:          1,
			},
		},
		{
			name: "WithMicroBatching",
			opts: Options{
				DisableIndexStore:   true,
				BatchWindow:         200 * time.Microsecond,
				BatchMax:            16,
				BatchMaxQueue:       1024,
				BatchAllowCallbacks: true,
			},
		},
	} {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			db, raw, _ := buildWriteBenchDBWithOptions(b, tc.opts)
			recA, recB := writeBenchUpdateRecords()
			patchBucket := ensureBenchSideBucket(b, raw, "bench_patch_log")

			var modifiedTS atomic.Uint64
			beforeStore := BeforeStore(func(_ uint64, _ *UserBench, newValue *UserBench) error {
				newValue.Score = float64(modifiedTS.Add(1))
				return nil
			})
			beforeCommit := BeforeCommit(func(tx *bbolt.Tx, _ uint64, oldValue, newValue *UserBench) error {
				patch := db.MakePatch(oldValue, newValue)
				payload, err := msgpack.Marshal(patch)
				if err != nil {
					return fmt.Errorf("marshal patch: %w", err)
				}
				return appendBenchSideBucket(tx, patchBucket, payload)
			})

			var (
				opSeq    atomic.Uint64
				errMu    sync.Mutex
				firstErr error
			)

			b.SetParallelism(4)
			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					n := opSeq.Add(1)
					id := uint64(1 + (n % 4096))
					if err := db.Set(id, benchWriteRecordForIteration(int(n), recA, recB), beforeStore, beforeCommit); err != nil {
						errMu.Lock()
						if firstErr == nil {
							firstErr = err
						}
						errMu.Unlock()
						return
					}
				}
			})

			b.StopTimer()

			errMu.Lock()
			defer errMu.Unlock()
			if firstErr != nil {
				b.Fatal(firstErr)
			}

			st := db.BatchStats()
			b.ReportMetric(float64(st.CombinedBatches), "combined_batches")
			b.ReportMetric(st.AvgBatchSize, "avg_batch")
		})
	}
}
