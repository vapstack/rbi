package rbi

import (
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/vapstack/pooled"
	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/rbistats"
	"go.etcd.io/bbolt"
)

func Benchmark_Read_Index_Keys_Scan_All_Uint64(b *testing.B) {
	c := buildBenchCollection(b, benchN)

	var count int
	prepareReadBenchSnapshot(b, c)
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		count = 0
		if err := readScanKeys(c, 0, func(_ uint64) (bool, error) {
			count++
			return true, nil
		}); err != nil {
			b.Fatalf("ScanKeys: %v", err)
		}
	}
	b.ReportMetric(float64(count), "keys/op")
}

func Benchmark_Read_Query_Items_SimpleFetch(b *testing.B) {
	c := buildBenchCollection(b, benchN)
	q := qx.Query(qx.EQ("country", "US")).Sort("age", qx.DESC).Limit(20)
	runReadQueryBench(b, c, q)
}

func Benchmark_Read_Query_Items_HeavyFetch(b *testing.B) {
	runReadQueryBenchCacheModes(b, func() *qx.QX {
		return qx.Query(qx.GTE("age", 20)).Sort("score", qx.DESC).Limit(100)
	})
}

func Benchmark_Read_Query_Items_GT_NoMatch(b *testing.B) {
	// This no-match read microbenchmark mostly measures fixed query overhead;
	// Cold cache rotation stretches runtime without adding much signal.
	c := buildBenchCollection(b, benchN)
	q := qx.Query(qx.GT("age", 100))
	runReadQueryBench(b, c, q)
}

const (
	writeBenchSeedBatch     = 100_000
	writeBenchSeedCount     = 200_000
	writeBenchUserBatchSize = 1000
	writeBenchHighChurnOps  = 2048
)

var writeBenchBufferPool = pooled.Slices[byte]{MaxCap: 64 << 10}
var writeBenchHookSink uint64

func buildWriteBenchCollection(b *testing.B) (*Collection[uint64, UserBench], *bbolt.DB, uint64) {
	return buildWriteBenchCollectionWithOptions(b, Options{
		DisableIndexStore: true,
	})
}

func buildWriteBenchTransparentCollection(b *testing.B) (*Collection[uint64, UserBench], *bbolt.DB, uint64) {
	return buildWriteBenchCollectionWithOptions(b, Options{
		DisableIndexStore: true,
		Index:             map[string]IndexKind{},
	})
}

func openWriteBenchCollection(b *testing.B, opts Options) (*Collection[uint64, UserBench], *bbolt.DB) {
	b.Helper()

	dir := b.TempDir()
	path := filepath.Join(dir, "bench_write_seeded.db")

	c, bolt := openBoltAndCollection[uint64, UserBench](b, path, opts)
	b.Cleanup(func() {
		_ = c.Close()
		_ = bolt.Close()
	})
	return c, bolt
}

func buildWriteBenchCollectionWithOptions(b *testing.B, opts Options) (*Collection[uint64, UserBench], *bbolt.DB, uint64) {
	b.Helper()

	c, bolt := openWriteBenchCollection(b, opts)
	c.disableSync()

	r := newRand(42)
	countries := []string{"US", "NL", "DE", "PL", "SE", "FR", "GB", "ES"}
	plans := []string{"free", "basic", "pro", "enterprise"}

	ids := make([]uint64, 0, writeBenchSeedBatch)
	vals := make([]*UserBench, 0, writeBenchSeedBatch)

	for i := 1; i <= writeBenchSeedCount; i++ {
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
			if err := writeSets(c, ids, vals); err != nil {
				b.Fatalf("seed error: %v", err)
			}
			ids = ids[:0]
			vals = vals[:0]
		}
	}
	if len(ids) > 0 {
		if err := writeSets(c, ids, vals); err != nil {
			b.Fatal(err)
		}
	}

	c.enableSync()

	return c, bolt, uint64(writeBenchSeedCount)
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

func rawSetBench(c *Collection[uint64, UserBench], raw *bbolt.DB, id uint64, rec *UserBench) error {
	buf := writeBenchBufferPool.Get(8 << 10)
	var err error
	buf, err = c.encode(rec, buf)
	if err != nil {
		writeBenchBufferPool.Put(buf)
		return err
	}
	defer writeBenchBufferPool.Put(buf)

	return raw.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(c.dataBucket)
		if bucket == nil {
			return fmt.Errorf("bucket does not exist")
		}
		bucket.FillPercent = c.options.BucketFillPercent
		var keyBuf [8]byte
		if err := bucket.Put(keycodec.UserKeyBytesWithBuf(id, c.strKey, &keyBuf), buf); err != nil {
			return fmt.Errorf("put: %w", err)
		}
		return nil
	})
}

func rawPatchBench(c *Collection[uint64, UserBench], raw *bbolt.DB, id uint64, patch []schema.PatchItem) error {
	return raw.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(c.dataBucket)
		if bucket == nil {
			return fmt.Errorf("bucket does not exist")
		}

		var keyBuf [8]byte
		key := keycodec.UserKeyBytesWithBuf(id, c.strKey, &keyBuf)
		oldBytes := bucket.Get(key)
		if oldBytes == nil {
			return nil
		}

		oldVal, err := c.decode(oldBytes)
		if err != nil {
			return fmt.Errorf("decode old: %w", err)
		}
		newVal, err := c.decode(oldBytes)
		if err != nil {
			c.ReleaseRecords(oldVal)
			return fmt.Errorf("decode new: %w", err)
		}
		defer c.ReleaseRecords(oldVal, newVal)

		if err = c.schema.Patch.Apply(unsafe.Pointer(newVal), patch, true); err != nil {
			return fmt.Errorf("apply patch: %w", err)
		}

		buf := writeBenchBufferPool.Get(8 << 10)
		buf, err = c.encode(newVal, buf)
		if err != nil {
			writeBenchBufferPool.Put(buf)
			return err
		}
		defer writeBenchBufferPool.Put(buf)

		bucket.FillPercent = c.options.BucketFillPercent
		if err = bucket.Put(key, buf); err != nil {
			return fmt.Errorf("put: %w", err)
		}
		return nil
	})
}

func churnWriteBenchSetNew(b *testing.B, c *Collection[uint64, UserBench], startOffset uint64) {
	b.Helper()

	ids := make([]uint64, 0, writeBenchUserBatchSize)
	vals := make([]*UserBench, 0, writeBenchUserBatchSize)
	recs := []*UserBench{
		{Name: "new-a", Age: 20, Country: "DE"},
		{Name: "new-b", Age: 21, Country: "US"},
	}

	flush := func() {
		if len(ids) == 0 {
			return
		}
		if err := writeSets(c, ids, vals); err != nil {
			b.Fatalf("MultiSet(high churn new): %v", err)
		}
		ids = ids[:0]
		vals = vals[:0]
	}

	for i := 0; i < writeBenchHighChurnOps; i++ {
		ids = append(ids, startOffset+uint64(i)+1)
		vals = append(vals, recs[i%len(recs)])
		if len(ids) == writeBenchUserBatchSize {
			flush()
		}
	}
	flush()
}

func churnWriteBenchUpdate(b *testing.B, c *Collection[uint64, UserBench], recA, recB *UserBench) {
	b.Helper()

	ids := make([]uint64, 0, writeBenchUserBatchSize)
	vals := make([]*UserBench, 0, writeBenchUserBatchSize)

	flush := func() {
		if len(ids) == 0 {
			return
		}
		if err := writeSets(c, ids, vals); err != nil {
			b.Fatalf("MultiSet(high churn update): %v", err)
		}
		ids = ids[:0]
		vals = vals[:0]
	}

	for i := 0; i < writeBenchHighChurnOps; i++ {
		ids = append(ids, uint64(i+1))
		vals = append(vals, benchWriteRecordForIteration(i, recA, recB))
		if len(ids) == writeBenchUserBatchSize {
			flush()
		}
	}
	flush()
}

func churnWriteBenchPatch(b *testing.B, c *Collection[uint64, UserBench], patch []Field) {
	b.Helper()

	ids := make([]uint64, 0, writeBenchHighChurnOps)
	for i := 0; i < writeBenchHighChurnOps; i++ {
		ids = append(ids, uint64(i+1))
	}
	if err := writePatches(c, ids, patch); err != nil {
		b.Fatalf("MultiPatch(high churn patch): %v", err)
	}
}

func seedWriteBenchDeleteKeys(b *testing.B, c *Collection[uint64, UserBench], start uint64, count int) {
	b.Helper()

	c.disableSync()
	defer c.enableSync()

	rec := &UserBench{Name: "delete", Age: 20, Country: "US"}
	ids := make([]uint64, 0, writeBenchSeedBatch)
	vals := make([]*UserBench, 0, writeBenchSeedBatch)
	for i := 0; i < count; i++ {
		ids = append(ids, start+uint64(i))
		vals = append(vals, rec)
		if len(ids) == writeBenchSeedBatch {
			if err := writeSets(c, ids, vals); err != nil {
				b.Fatalf("seed delete: %v", err)
			}
			ids = ids[:0]
			vals = vals[:0]
		}
	}
	if len(ids) != 0 {
		if err := writeSets(c, ids, vals); err != nil {
			b.Fatalf("seed delete: %v", err)
		}
	}
}

func Benchmark_Write_Set_New_Indexed(b *testing.B) {
	rec := &UserBench{Name: "new", Age: 20, Country: "DE"}

	b.Run("StableBase", func(b *testing.B) {
		c, _, startOffset := buildWriteBenchCollection(b)
		prepareWriteBenchStableBase(b, c)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			id := startOffset + uint64(i) + 1
			if err := writeSet(c, id, rec); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("HighChurn", func(b *testing.B) {
		c, _, startOffset := buildWriteBenchCollection(b)
		prepareWriteBenchHighChurn(b, c, func(b *testing.B, c *Collection[uint64, UserBench]) {
			churnWriteBenchSetNew(b, c, startOffset)
		})

		startOffset += writeBenchHighChurnOps
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			id := startOffset + uint64(i) + 1
			if err := writeSet(c, id, rec); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func Benchmark_Write_Set_New_RawBolt(b *testing.B) {
	c, bolt, startOffset := buildWriteBenchCollection(b)
	prepareWriteBenchStableBase(b, c)

	rec := &UserBench{Name: "new", Age: 20, Country: "DE"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		id := startOffset + uint64(i) + 1
		if err := rawSetBench(c, bolt, id, rec); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Write_Set_New_Transparent(b *testing.B) {
	rec := &UserBench{Name: "new", Age: 20, Country: "DE"}

	b.Run("StableBase", func(b *testing.B) {
		c, _, startOffset := buildWriteBenchTransparentCollection(b)
		prepareWriteBenchTransparentBase(b, c)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			id := startOffset + uint64(i) + 1
			if err := writeSet(c, id, rec); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("HighChurn", func(b *testing.B) {
		c, _, startOffset := buildWriteBenchTransparentCollection(b)
		prepareWriteBenchTransparentHighChurn(b, c, func(b *testing.B, c *Collection[uint64, UserBench]) {
			churnWriteBenchSetNew(b, c, startOffset)
		})

		startOffset += writeBenchHighChurnOps
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			id := startOffset + uint64(i) + 1
			if err := writeSet(c, id, rec); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func Benchmark_Write_Update_Indexed(b *testing.B) {
	recA, recB := writeBenchUpdateRecords()

	b.Run("StableBase", func(b *testing.B) {
		c, _, _ := buildWriteBenchCollection(b)
		targetID := uint64(1000)
		prepareWriteBenchStableBase(b, c)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if err := writeSet(c, targetID, benchWriteRecordForIteration(i, recA, recB)); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("HighChurn", func(b *testing.B) {
		c, _, _ := buildWriteBenchCollection(b)
		targetID := uint64(1000)
		prepareWriteBenchHighChurn(b, c, func(b *testing.B, c *Collection[uint64, UserBench]) {
			churnWriteBenchUpdate(b, c, recA, recB)
		})

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if err := writeSet(c, targetID, benchWriteRecordForIteration(i, recA, recB)); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func Benchmark_Write_Update_RawBolt(b *testing.B) {
	c, bolt, _ := buildWriteBenchCollection(b)
	targetID := uint64(1000)
	prepareWriteBenchStableBase(b, c)

	recA, recB := writeBenchUpdateRecords()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := rawSetBench(c, bolt, targetID, benchWriteRecordForIteration(i, recA, recB)); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Write_Update_Transparent(b *testing.B) {
	recA, recB := writeBenchUpdateRecords()

	b.Run("StableBase", func(b *testing.B) {
		c, _, _ := buildWriteBenchTransparentCollection(b)
		targetID := uint64(1000)
		prepareWriteBenchTransparentBase(b, c)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if err := writeSet(c, targetID, benchWriteRecordForIteration(i, recA, recB)); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("HighChurn", func(b *testing.B) {
		c, _, _ := buildWriteBenchTransparentCollection(b)
		targetID := uint64(1000)
		prepareWriteBenchTransparentHighChurn(b, c, func(b *testing.B, c *Collection[uint64, UserBench]) {
			churnWriteBenchUpdate(b, c, recA, recB)
		})

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if err := writeSet(c, targetID, benchWriteRecordForIteration(i, recA, recB)); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func Benchmark_Write_Patch_Indexed(b *testing.B) {
	patchA := []Field{{Name: "age", Value: 100}}
	patchB := []Field{{Name: "age", Value: 200}}

	b.Run("StableBase", func(b *testing.B) {
		c, _, _ := buildWriteBenchCollection(b)
		targetID := uint64(2000)
		prepareWriteBenchStableBase(b, c)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if i%2 == 0 {
				if err := writePatch(c, targetID, patchA); err != nil {
					b.Fatal(err)
				}
			} else {
				if err := writePatch(c, targetID, patchB); err != nil {
					b.Fatal(err)
				}
			}
		}
	})

	b.Run("HighChurn", func(b *testing.B) {
		c, _, _ := buildWriteBenchCollection(b)
		targetID := uint64(2000)
		prepareWriteBenchHighChurn(b, c, func(b *testing.B, c *Collection[uint64, UserBench]) {
			churnWriteBenchPatch(b, c, patchA)
		})

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if i%2 == 0 {
				if err := writePatch(c, targetID, patchA); err != nil {
					b.Fatal(err)
				}
			} else {
				if err := writePatch(c, targetID, patchB); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

func Benchmark_Write_Patch_RawBolt(b *testing.B) {
	c, bolt, _ := buildWriteBenchCollection(b)
	targetID := uint64(2000)
	prepareWriteBenchStableBase(b, c)

	patchA := []schema.PatchItem{{Name: "age", Value: 100}}
	patchB := []schema.PatchItem{{Name: "age", Value: 200}}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var patch []schema.PatchItem
		if i%2 == 0 {
			patch = patchA
		} else {
			patch = patchB
		}
		if err := rawPatchBench(c, bolt, targetID, patch); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Write_Patch_Transparent(b *testing.B) {
	patchA := []Field{{Name: "age", Value: 100}}
	patchB := []Field{{Name: "age", Value: 200}}

	b.Run("StableBase", func(b *testing.B) {
		c, _, _ := buildWriteBenchTransparentCollection(b)
		targetID := uint64(2000)
		prepareWriteBenchTransparentBase(b, c)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if i%2 == 0 {
				if err := writePatch(c, targetID, patchA); err != nil {
					b.Fatal(err)
				}
			} else {
				if err := writePatch(c, targetID, patchB); err != nil {
					b.Fatal(err)
				}
			}
		}
	})

	b.Run("HighChurn", func(b *testing.B) {
		c, _, _ := buildWriteBenchTransparentCollection(b)
		targetID := uint64(2000)
		prepareWriteBenchTransparentHighChurn(b, c, func(b *testing.B, c *Collection[uint64, UserBench]) {
			churnWriteBenchPatch(b, c, patchA)
		})

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if i%2 == 0 {
				if err := writePatch(c, targetID, patchA); err != nil {
					b.Fatal(err)
				}
			} else {
				if err := writePatch(c, targetID, patchB); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

func Benchmark_Write_Delete_Indexed(b *testing.B) {
	b.Run("StableBase", func(b *testing.B) {
		c, _, startOffset := buildWriteBenchCollection(b)
		requireBenchSnapshotPublished(b, currentBenchSnapshot(b, c))

		start := startOffset + 1
		seedWriteBenchDeleteKeys(b, c, start, b.N)
		requireBenchSnapshotPublished(b, c.SnapshotStats())

		b.ReportAllocs()
		b.ResetTimer()
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			if err := writeDelete(c, start+uint64(i)); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("HighChurn", func(b *testing.B) {
		c, _, startOffset := buildWriteBenchCollection(b)
		requireBenchSnapshotPublished(b, currentBenchSnapshot(b, c))

		churnWriteBenchSetNew(b, c, startOffset)
		start := startOffset + writeBenchHighChurnOps + 1
		seedWriteBenchDeleteKeys(b, c, start, b.N)
		requireBenchSnapshotPublished(b, c.SnapshotStats())

		b.ReportAllocs()
		b.ResetTimer()
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			if err := writeDelete(c, start+uint64(i)); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func Benchmark_Write_Delete_Transparent(b *testing.B) {
	c, _ := openWriteBenchCollection(b, Options{
		DisableIndexStore: true,
		Index:             map[string]IndexKind{},
	})
	prepareWriteBenchTransparentBase(b, c)

	b.StopTimer()
	seedWriteBenchDeleteKeys(b, c, 1, b.N)
	b.StartTimer()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := writeDelete(c, uint64(i+1)); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Write_Update_OnChangeMutate(b *testing.B) {
	c, _, _ := buildWriteBenchCollection(b)
	targetID := uint64(1000)
	recA, recB := writeBenchUpdateRecords()
	prepareWriteBenchStableBase(b, c)

	var modifiedTS uint64
	onChange := OnChange(func(_ *Tx, _ uint64, _ *UserBench, newValue *UserBench) error {
		newValue.Score = float64(modifiedTS + 1)
		modifiedTS++
		return nil
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := writeSet(c, targetID, benchWriteRecordForIteration(i, recA, recB), onChange); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Write_Update_OnChangeObserve(b *testing.B) {
	c, _, _ := buildWriteBenchCollection(b)
	targetID := uint64(1000)
	recA, recB := writeBenchUpdateRecords()
	prepareWriteBenchStableBase(b, c)

	var sink uint64
	onChange := OnChange(func(_ *Tx, key uint64, _ *UserBench, newValue *UserBench) error {
		sink += key + uint64(newValue.Age) + math.Float64bits(newValue.Score)
		return nil
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := writeSet(c, targetID, benchWriteRecordForIteration(i, recA, recB), onChange); err != nil {
			b.Fatal(err)
		}
	}
	writeBenchHookSink = sink
}

func Benchmark_Write_Update_OnChangeMutateMakePatch(b *testing.B) {
	c, _, _ := buildWriteBenchCollection(b)
	targetID := uint64(1000)
	recA, recB := writeBenchUpdateRecords()
	prepareWriteBenchStableBase(b, c)

	var modifiedTS uint64
	var patchCount uint64
	onChange := OnChange(func(_ *Tx, _ uint64, oldValue, newValue *UserBench) error {
		newValue.Score = float64(modifiedTS + 1)
		modifiedTS++
		patch, err := c.MakePatch(oldValue, newValue)
		if err != nil {
			return err
		}
		patchCount += uint64(len(patch))
		return nil
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := writeSet(c, targetID, benchWriteRecordForIteration(i, recA, recB), onChange); err != nil {
			b.Fatal(err)
		}
	}
	writeBenchHookSink = patchCount
}

func Benchmark_Write_Update_OnChangeMutateMakePatch_MultiSet(b *testing.B) {
	c, _, _ := buildWriteBenchCollection(b)
	ids, valsA, valsB := buildWriteBenchBatchUpdateInput(writeBenchUserBatchSize)
	prepareWriteBenchStableBase(b, c)

	var modifiedTS uint64
	var patchCount uint64
	onChange := OnChange(func(_ *Tx, _ uint64, oldValue, newValue *UserBench) error {
		newValue.Score = float64(modifiedTS + 1)
		modifiedTS++
		patch, err := c.MakePatch(oldValue, newValue)
		if err != nil {
			return err
		}
		patchCount += uint64(len(patch))
		return nil
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		vals := valsA
		if i%2 != 0 {
			vals = valsB
		}
		if err := writeSets(c, ids, vals, onChange); err != nil {
			b.Fatal(err)
		}
	}
	writeBenchHookSink = patchCount
	b.ReportMetric(writeBenchUserBatchSize, "rows/op")
}

func Benchmark_Write_Update_OnChangeObserve_MultiSet(b *testing.B) {
	c, _, _ := buildWriteBenchCollection(b)
	ids, valsA, valsB := buildWriteBenchBatchUpdateInput(writeBenchUserBatchSize)
	prepareWriteBenchStableBase(b, c)

	var sink uint64
	onChange := OnChange(func(_ *Tx, key uint64, _ *UserBench, newValue *UserBench) error {
		sink += key + uint64(newValue.Age) + math.Float64bits(newValue.Score)
		return nil
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		vals := valsA
		if i%2 != 0 {
			vals = valsB
		}
		if err := writeSets(c, ids, vals, onChange); err != nil {
			b.Fatal(err)
		}
	}
	writeBenchHookSink = sink
	b.ReportMetric(writeBenchUserBatchSize, "rows/op")
}

func Benchmark_Write_Update_OnChangeMutateMakePatch_Parallel(b *testing.B) {
	for _, tc := range []struct {
		name string
		opts Options
	}{
		{
			name: "SingleRequestBatches",
			opts: Options{
				DisableIndexStore: true,
				BatchSoftLimit:    1,
			},
		},
		{
			name: "WithAutoBatching",
			opts: Options{
				DisableIndexStore: true,
				BatchSoftLimit:    16,
			},
		},
	} {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			enableStoreStatsForTest(b)
			c, _, _ := buildWriteBenchCollectionWithOptions(b, tc.opts)
			recA, recB := writeBenchUpdateRecords()
			prepareWriteBenchStableBase(b, c)

			var modifiedTS atomic.Uint64
			var patchCount atomic.Uint64
			onChange := OnChange(func(_ *Tx, _ uint64, oldValue, newValue *UserBench) error {
				newValue.Score = float64(modifiedTS.Add(1))
				patch, err := c.MakePatch(oldValue, newValue)
				if err != nil {
					return err
				}
				patchCount.Add(uint64(len(patch)))
				return nil
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
					if err := writeSet(c, id, benchWriteRecordForIteration(int(n), recA, recB), onChange); err != nil {
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

			writeBenchHookSink = patchCount.Load()
			st := c.StoreStats()
			b.ReportMetric(float64(st.MultiUnitBatches), "multi_unit_batches")
			b.ReportMetric(st.AvgBatchSize, "avg_batch")
		})
	}
}

/**/

func currentBenchSnapshot[K ~string | ~uint64, V any](b *testing.B, c *Collection[K, V]) rbistats.Snapshot {
	b.Helper()
	b.StopTimer()
	return c.SnapshotStats()
}

func requireBenchSnapshotPublished(b *testing.B, st rbistats.Snapshot) {
	b.Helper()
	if st.Sequence == 0 {
		b.Fatalf("expected published snapshot, got %+v", st)
	}
}

func prepareReadBenchSnapshot[K ~string | ~uint64, V any](b *testing.B, c *Collection[K, V]) {
	b.Helper()
	requireBenchSnapshotPublished(b, currentBenchSnapshot(b, c))
	b.StartTimer()
}

func prepareWriteBenchStableBase[K ~string | ~uint64, V any](b *testing.B, c *Collection[K, V]) {
	b.Helper()
	requireBenchSnapshotPublished(b, currentBenchSnapshot(b, c))
	b.StartTimer()
}

func prepareWriteBenchTransparentBase[K ~string | ~uint64, V any](b *testing.B, c *Collection[K, V]) {
	b.Helper()
	b.StopTimer()
	st, err := c.Stats()
	if err != nil {
		b.Fatalf("Stats: %v", err)
	}
	if st.Indexed {
		b.Fatalf("expected transparent collection")
	}
	b.StartTimer()
}

func prepareWriteBenchTransparentHighChurn[K ~string | ~uint64, V any](b *testing.B, c *Collection[K, V], churn func(*testing.B, *Collection[K, V])) {
	b.Helper()

	prepareWriteBenchTransparentBase(b, c)
	b.StopTimer()
	churn(b, c)
	st, err := c.Stats()
	if err != nil {
		b.Fatalf("Stats: %v", err)
	}
	if st.Indexed {
		b.Fatalf("expected transparent collection")
	}
	b.StartTimer()
}

func prepareWriteBenchHighChurn[K ~string | ~uint64, V any](b *testing.B, c *Collection[K, V], churn func(*testing.B, *Collection[K, V])) {
	b.Helper()

	requireBenchSnapshotPublished(b, currentBenchSnapshot(b, c))
	churn(b, c)
	requireBenchSnapshotPublished(b, c.SnapshotStats())
	b.StartTimer()
}

func Benchmark_Write_Helper_CloneUserBench(b *testing.B) {
	c := buildBenchCollection(b, benchN)
	src := &UserBench{
		Country: "ES",
		Plan:    "basic",
		Status:  "active",
		Age:     20,
		Score:   0.8,
		Name:    "Test",
		Email:   "test@example.com",
		Tags:    []string{"go", "java"},
		Roles:   []string{"user", "admin"},
	}

	b.Run("CloneInto", func(b *testing.B) {
		var dst UserBench
		var sink uint64
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			c.schema.Clone.CloneInto(unsafe.Pointer(src), unsafe.Pointer(&dst))
			sink += uint64(len(dst.Tags) + len(dst.Roles))
		}
		writeBenchHookSink = sink
	})

	b.Run("AcquireCloneRelease", func(b *testing.B) {
		var sink uint64
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			dst := c.recPool.Get()
			c.schema.Clone.CloneInto(unsafe.Pointer(src), unsafe.Pointer(dst))
			sink += uint64(len(dst.Tags) + len(dst.Roles))
			c.ReleaseRecords(dst)
		}
		writeBenchHookSink = sink
	})
}

func Benchmark_Write_Helper_MakePatch(b *testing.B) {
	c := buildBenchCollection(b, benchN)
	b.ReportAllocs()

	v1 := &UserBench{
		Country: "ES",
		Plan:    "free",
		Status:  "trial",
		Age:     20,
		Score:   0.5,
		Name:    "Test",
		Email:   "test@example.com",
		Tags:    []string{"go"},
		Roles:   []string{"user"},
	}
	v2 := &UserBench{
		Country: "ES",
		Plan:    "basic",
		Status:  "active",
		Age:     20,
		Score:   0.8,
		Name:    "Test",
		Email:   "test@example.com",
		Tags:    []string{"go", "java"},
		Roles:   []string{"user", "admin"},
	}

	buf := make([]Field, 0, 8)
	b.ResetTimer()
	for b.Loop() {
		var err error
		buf, err = c.MakePatchInto(v1, v2, buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}
