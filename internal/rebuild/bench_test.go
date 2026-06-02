package rebuild

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"unsafe"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/internal/strmap"
	"github.com/vmihailenco/msgpack/v5"
	"go.etcd.io/bbolt"
)

func rebuildBenchRows() int {
	if testing.Short() {
		return 512
	}
	return 8192
}

func rebuildBenchWideRows() int {
	if testing.Short() {
		return 1024
	}
	return 32768
}

func rebuildBenchMeasureRows() int {
	if testing.Short() {
		return 2048
	}
	return 65536
}

type rebuildBenchWideShape uint8

const (
	rebuildBenchWideLowCard rebuildBenchWideShape = iota
	rebuildBenchWideHighCard
	rebuildBenchWideSliceFanout
)

type rebuildBenchScalarShape uint8

const (
	rebuildBenchScalarLowCard rebuildBenchScalarShape = iota
	rebuildBenchScalarHighCard
)

type rebuildBenchScalarRec struct {
	Country string  `db:"country" rbi:"index"`
	Plan    string  `db:"plan"    rbi:"index"`
	Status  string  `db:"status"  rbi:"index"`
	Name    string  `db:"name"    rbi:"index"`
	Email   string  `db:"email"   rbi:"index"`
	Age     int     `db:"age"     rbi:"index"`
	Score   float64 `db:"score"   rbi:"index"`
	Login   uint64  `db:"login"   rbi:"index"`
	Rank    int64   `db:"rank"    rbi:"index"`
	Active  bool    `db:"active"  rbi:"index"`
}

type rebuildBenchWideRec struct {
	Country string   `db:"country" rbi:"index"`
	Plan    string   `db:"plan"    rbi:"index"`
	Status  string   `db:"status"  rbi:"index"`
	Name    string   `db:"name"    rbi:"index"`
	Email   string   `db:"email"   rbi:"index"`
	Age     int      `db:"age"     rbi:"index"`
	Score   uint64   `db:"score"   rbi:"index"`
	Active  bool     `db:"active"  rbi:"index"`
	Tags    []string `db:"tags"    rbi:"index"`
	Roles   []string `db:"roles"   rbi:"index"`
	Opt     *string  `db:"opt"     rbi:"index"`

	Revenue uint64 `db:"revenue" rbi:"measure"`
	Latency uint64 `db:"latency" rbi:"measure"`
}

type rebuildBenchMeasureRec struct {
	Group string `db:"group" rbi:"index"`

	M0 uint64 `db:"m0" rbi:"measure"`
	M1 uint64 `db:"m1" rbi:"measure"`
	M2 uint64 `db:"m2" rbi:"measure"`
	M3 uint64 `db:"m3" rbi:"measure"`
	M4 uint64 `db:"m4" rbi:"measure"`
	M5 uint64 `db:"m5" rbi:"measure"`
	M6 uint64 `db:"m6" rbi:"measure"`
	M7 uint64 `db:"m7" rbi:"measure"`
}

type rebuildBenchStorageMetrics struct {
	entries     uint64
	postings    uint64
	keyBytes    uint64
	structBytes uint64
	measureRows uint64
}

var (
	rebuildBenchScalarPool = pooled.Pointers[rebuildBenchScalarRec]{
		Clear: true,
	}
	rebuildBenchWidePool = pooled.Pointers[rebuildBenchWideRec]{
		Clear: true,
	}
	rebuildBenchMeasurePool = pooled.Pointers[rebuildBenchMeasureRec]{
		Clear: true,
	}
)

var rebuildBenchCountries = [...]string{"US", "NL", "DE", "PL", "SE", "FR", "GB", "ES"}
var rebuildBenchPlans = [...]string{"free", "basic", "pro", "enterprise"}
var rebuildBenchStatuses = [...]string{"active", "trial", "paused", "banned", "deleted"}
var rebuildBenchRoles = [...]string{"owner", "admin", "editor", "viewer", "billing", "support"}

func seedRebuildBenchBolt(b *testing.B, bucket []byte, rows int, strKey bool) *bbolt.DB {
	b.Helper()

	db, err := bbolt.Open(filepath.Join(b.TempDir(), "rebuild_bench.db"), 0o600, nil)
	if err != nil {
		b.Fatalf("bbolt.Open: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })

	err = db.Update(func(tx *bbolt.Tx) error {
		bkt, e := tx.CreateBucketIfNotExists(bucket)
		if e != nil {
			return e
		}
		var key [8]byte
		for i := 1; i <= rows; i++ {
			rec := rebuildTestRec{
				Name:  fmt.Sprintf("user_%06d", i%1024),
				Score: uint64(i * 3),
			}
			switch i & 3 {
			case 0:
				rec.Tags = []string{"go", "db", fmt.Sprintf("tag_%d", i%128)}
			case 1:
				rec.Tags = []string{"go"}
			}
			data, e := msgpack.Marshal(&rec)
			if e != nil {
				return e
			}
			if strKey {
				if e = bkt.Put(keycodec.StringBytes(fmt.Sprintf("key_%08d", i)), data); e != nil {
					return e
				}
			} else if e = bkt.Put(keycodec.U64BytesWithBuf(uint64(i), &key), data); e != nil {
				return e
			}
		}
		return nil
	})
	if err != nil {
		b.Fatalf("seed bolt: %v", err)
	}
	return db
}

func seedRebuildBenchScalarBolt(b *testing.B, bucket []byte, rows int, shape rebuildBenchScalarShape) *bbolt.DB {
	b.Helper()

	db, err := bbolt.Open(filepath.Join(b.TempDir(), "rebuild_bench.db"), 0o600, nil)
	if err != nil {
		b.Fatalf("bbolt.Open: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })

	err = db.Update(func(tx *bbolt.Tx) error {
		bkt, e := tx.CreateBucketIfNotExists(bucket)
		if e != nil {
			return e
		}
		var key [8]byte
		for i := 1; i <= rows; i++ {
			rec := rebuildBenchScalarRecord(i, shape)
			data, e := msgpack.Marshal(&rec)
			if e != nil {
				return e
			}
			if e = bkt.Put(keycodec.U64BytesWithBuf(uint64(i), &key), data); e != nil {
				return e
			}
		}
		return nil
	})
	if err != nil {
		b.Fatalf("seed bolt: %v", err)
	}
	return db
}

func seedRebuildBenchWideBolt(b *testing.B, bucket []byte, rows int, shape rebuildBenchWideShape, strKey bool) *bbolt.DB {
	b.Helper()

	db, err := bbolt.Open(filepath.Join(b.TempDir(), "rebuild_bench.db"), 0o600, nil)
	if err != nil {
		b.Fatalf("bbolt.Open: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })

	err = db.Update(func(tx *bbolt.Tx) error {
		bkt, e := tx.CreateBucketIfNotExists(bucket)
		if e != nil {
			return e
		}
		var key [8]byte
		for i := 1; i <= rows; i++ {
			rec := rebuildBenchWideRecord(i, shape)
			data, e := msgpack.Marshal(&rec)
			if e != nil {
				return e
			}
			if strKey {
				if e = bkt.Put(keycodec.StringBytes(fmt.Sprintf("key_%08d", i)), data); e != nil {
					return e
				}
			} else if e = bkt.Put(keycodec.U64BytesWithBuf(uint64(i), &key), data); e != nil {
				return e
			}
		}
		return nil
	})
	if err != nil {
		b.Fatalf("seed bolt: %v", err)
	}
	return db
}

func seedRebuildBenchMeasureBolt(b *testing.B, bucket []byte, rows int) *bbolt.DB {
	b.Helper()

	db, err := bbolt.Open(filepath.Join(b.TempDir(), "rebuild_bench.db"), 0o600, nil)
	if err != nil {
		b.Fatalf("bbolt.Open: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })

	err = db.Update(func(tx *bbolt.Tx) error {
		bkt, e := tx.CreateBucketIfNotExists(bucket)
		if e != nil {
			return e
		}
		var key [8]byte
		for i := 1; i <= rows; i++ {
			rec := rebuildBenchMeasureRec{
				Group: fmt.Sprintf("group_%03d", i%256),
				M0:    uint64(i * 3),
				M1:    uint64(i * 5),
				M2:    uint64(i * 7),
				M3:    uint64(i * 11),
				M4:    uint64(i * 13),
				M5:    uint64(i * 17),
				M6:    uint64(i * 19),
				M7:    uint64(i * 23),
			}
			data, e := msgpack.Marshal(&rec)
			if e != nil {
				return e
			}
			if e = bkt.Put(keycodec.U64BytesWithBuf(uint64(i), &key), data); e != nil {
				return e
			}
		}
		return nil
	})
	if err != nil {
		b.Fatalf("seed bolt: %v", err)
	}
	return db
}

func compileRebuildBenchSchema(b *testing.B) *schema.Schema {
	b.Helper()

	rt, err := schema.Compile(reflect.TypeOf(rebuildTestRec{}), schema.Config{})
	if err != nil {
		b.Fatalf("schema.Compile: %v", err)
	}
	return rt
}

func compileRebuildBenchSchemaFor[T any](b *testing.B) *schema.Schema {
	b.Helper()

	rt, err := schema.Compile(reflect.TypeFor[T](), schema.Config{})
	if err != nil {
		b.Fatalf("schema.Compile: %v", err)
	}
	return rt
}

func baseRebuildBenchConfig(db *bbolt.DB, bucket []byte, s *schema.Schema, decode DecodeFunc, release ReleaseFunc) Config {
	return Config{
		Bolt:    db,
		Bucket:  bucket,
		Schema:  s,
		Decode:  decode,
		Release: release,
	}
}

func rebuildBenchScalarRecord(i int, shape rebuildBenchScalarShape) rebuildBenchScalarRec {
	country := rebuildBenchCountries[i%len(rebuildBenchCountries)]
	plan := rebuildBenchPlans[(i/3)%len(rebuildBenchPlans)]
	status := rebuildBenchStatuses[(i/5)%len(rebuildBenchStatuses)]
	rec := rebuildBenchScalarRec{
		Country: country,
		Plan:    plan,
		Status:  status,
		Age:     18 + i%77,
		Score:   float64((i*17)%100000) / 100,
		Login:   uint64(i * 13),
		Rank:    int64(i%8192 - 4096),
		Active:  i&3 != 0,
	}
	if shape == rebuildBenchScalarHighCard {
		rec.Name = fmt.Sprintf("user_%08d", i)
		rec.Email = fmt.Sprintf("user_%08d@example.test", i)
		rec.Age = i
		rec.Score = float64(i*7919) / 1000
		rec.Rank = int64(i) - 16384
		return rec
	}
	rec.Name = fmt.Sprintf("user_%04d", i%1024)
	rec.Email = fmt.Sprintf("group_%04d@example.test", i%2048)
	return rec
}

func rebuildBenchWideRecord(i int, shape rebuildBenchWideShape) rebuildBenchWideRec {
	country := rebuildBenchCountries[i%len(rebuildBenchCountries)]
	plan := rebuildBenchPlans[(i/3)%len(rebuildBenchPlans)]
	status := rebuildBenchStatuses[(i/5)%len(rebuildBenchStatuses)]
	rec := rebuildBenchWideRec{
		Country: country,
		Plan:    plan,
		Status:  status,
		Age:     18 + i%77,
		Score:   uint64((i * 17) % 100000),
		Active:  i&3 != 0,
		Revenue: uint64(i * 101),
		Latency: uint64(10 + i%2000),
	}

	switch shape {
	case rebuildBenchWideHighCard:
		rec.Name = fmt.Sprintf("user_%08d", i)
		rec.Email = fmt.Sprintf("user_%08d@example.test", i)
		rec.Tags = []string{"common", fmt.Sprintf("tag_%06d", i), fmt.Sprintf("cohort_%03d", i%512)}
		rec.Roles = []string{rebuildBenchRoles[i%len(rebuildBenchRoles)]}
		rec.Score = uint64(i * 7919)
		rec.Age = i

	case rebuildBenchWideSliceFanout:
		rec.Name = fmt.Sprintf("member_%06d", i%4096)
		rec.Email = fmt.Sprintf("member_%06d_%02d@example.test", i%4096, i%97)
		rec.Tags = []string{
			"shared",
			fmt.Sprintf("country_%s", country),
			fmt.Sprintf("plan_%s", plan),
			fmt.Sprintf("tag_%04d", i%4096),
			fmt.Sprintf("tag_%04d", (i*7)%4096),
			fmt.Sprintf("tag_%04d", (i*31)%4096),
		}
		rec.Roles = []string{
			rebuildBenchRoles[i%len(rebuildBenchRoles)],
			rebuildBenchRoles[(i+2)%len(rebuildBenchRoles)],
			rebuildBenchRoles[(i+4)%len(rebuildBenchRoles)],
		}

	default:
		rec.Name = fmt.Sprintf("user_%04d", i%1024)
		rec.Email = fmt.Sprintf("group_%04d@example.test", i%2048)
		rec.Tags = []string{"common", fmt.Sprintf("country_%s", country)}
		if i&1 == 0 {
			rec.Tags = append(rec.Tags, fmt.Sprintf("plan_%s", plan))
		}
		if i&3 == 0 {
			rec.Roles = []string{rebuildBenchRoles[i%len(rebuildBenchRoles)], rebuildBenchRoles[(i+1)%len(rebuildBenchRoles)]}
		} else {
			rec.Roles = []string{rebuildBenchRoles[i%len(rebuildBenchRoles)]}
		}
	}

	if i&7 != 0 {
		opt := fmt.Sprintf("opt_%04d", i%2048)
		rec.Opt = &opt
	}
	return rec
}

func decodeRebuildBenchScalarRec(data []byte) (unsafe.Pointer, error) {
	rec := rebuildBenchScalarPool.Get()
	if err := msgpack.Unmarshal(data, rec); err != nil {
		rebuildBenchScalarPool.Put(rec)
		return nil, err
	}
	return unsafe.Pointer(rec), nil
}

func releaseRebuildBenchScalarRec(ptr unsafe.Pointer) {
	rebuildBenchScalarPool.Put((*rebuildBenchScalarRec)(ptr))
}

func decodeRebuildBenchWideRec(data []byte) (unsafe.Pointer, error) {
	rec := rebuildBenchWidePool.Get()
	if err := msgpack.Unmarshal(data, rec); err != nil {
		rebuildBenchWidePool.Put(rec)
		return nil, err
	}
	return unsafe.Pointer(rec), nil
}

func releaseRebuildBenchWideRec(ptr unsafe.Pointer) {
	rebuildBenchWidePool.Put((*rebuildBenchWideRec)(ptr))
}

func decodeRebuildBenchMeasureRec(data []byte) (unsafe.Pointer, error) {
	rec := rebuildBenchMeasurePool.Get()
	if err := msgpack.Unmarshal(data, rec); err != nil {
		rebuildBenchMeasurePool.Put(rec)
		return nil, err
	}
	return unsafe.Pointer(rec), nil
}

func releaseRebuildBenchMeasureRec(ptr unsafe.Pointer) {
	rebuildBenchMeasurePool.Put((*rebuildBenchMeasureRec)(ptr))
}

func runRebuildFullBench(b *testing.B, rows int, rt *schema.Schema, cfg Config) {
	b.Helper()

	var metrics rebuildBenchStorageMetrics
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := Build(cfg, newRebuildTestState(rt))
		if err != nil {
			b.Fatalf("Build: %v", err)
		}
		if i == 0 {
			metrics = collectRebuildBenchStorageMetrics(rt, result.Storage)
		}
		result.Storage.Release()
	}
	b.StopTimer()
	reportRebuildBenchMetrics(b, rows, metrics)
}

func collectRebuildBenchStorageMetrics(rt *schema.Schema, storage snapshot.Storage) rebuildBenchStorageMetrics {
	var metrics rebuildBenchStorageMetrics
	for _, acc := range rt.Indexed {
		regular := storage.Index[acc.Ordinal].Stats(true)
		nilStorage := storage.NilIndex[acc.Ordinal].Stats(false)
		lenStorage := storage.LenIndex[acc.Ordinal].Stats(true)
		metrics.entries += regular.EntryCount + nilStorage.EntryCount + lenStorage.EntryCount
		metrics.postings += regular.PostingCardinality + nilStorage.PostingCardinality + lenStorage.PostingCardinality
		metrics.keyBytes += regular.KeyBytes + nilStorage.KeyBytes + lenStorage.KeyBytes
		metrics.structBytes += regular.ApproxStructBytes + nilStorage.ApproxStructBytes + lenStorage.ApproxStructBytes
	}
	metrics.structBytes += indexdata.FieldStorageSlotsApproxBytes(storage.Index)
	metrics.structBytes += indexdata.FieldStorageSlotsApproxBytes(storage.NilIndex)
	metrics.structBytes += indexdata.FieldStorageSlotsApproxBytes(storage.LenIndex)
	for i := range storage.Measure {
		metrics.measureRows += uint64(storage.Measure[i].Rows())
	}
	return metrics
}

func reportRebuildBenchMetrics(b *testing.B, rows int, metrics rebuildBenchStorageMetrics) {
	b.Helper()

	b.ReportMetric(float64(rows), "rows/op")
	b.ReportMetric(float64(metrics.entries), "entries/op")
	b.ReportMetric(float64(metrics.postings), "postings/op")
	b.ReportMetric(float64(metrics.keyBytes+metrics.structBytes), "index_heap_B/op")
	if metrics.measureRows > 0 {
		b.ReportMetric(float64(metrics.measureRows), "measure_rows/op")
	}
}

func BenchmarkBuildUint64(b *testing.B) {
	bucket := []byte("bench_uint64")
	rows := rebuildBenchRows()
	db := seedRebuildBenchBolt(b, bucket, rows, false)
	rt := compileRebuildBenchSchema(b)
	cfg := baseRebuildTestConfig(db, bucket, rt)

	var metrics rebuildBenchStorageMetrics
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := Build(cfg, newRebuildTestState(rt))
		if err != nil {
			b.Fatalf("Build: %v", err)
		}
		if i == 0 {
			metrics = collectRebuildBenchStorageMetrics(rt, result.Storage)
		}
		result.Storage.Release()
	}
	b.StopTimer()
	reportRebuildBenchMetrics(b, rows, metrics)
}

func BenchmarkBuildStringKeys(b *testing.B) {
	bucket := []byte("bench_string")
	rows := rebuildBenchRows()
	db := seedRebuildBenchBolt(b, bucket, rows, true)
	rt := compileRebuildBenchSchema(b)
	cfg := baseRebuildTestConfig(db, bucket, rt)
	cfg.StrKey = true

	var metrics rebuildBenchStorageMetrics
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cfg.StrMap = strmap.New(0, 256)
		result, err := Build(cfg, newRebuildTestState(rt))
		if err != nil {
			b.Fatalf("Build: %v", err)
		}
		if i == 0 {
			metrics = collectRebuildBenchStorageMetrics(rt, result.Storage)
		}
		result.Storage.Release()
	}
	b.StopTimer()
	reportRebuildBenchMetrics(b, rows, metrics)
}

func BenchmarkBuildFullCoverage(b *testing.B) {
	rows := rebuildBenchWideRows()

	b.Run("LowCardinalityScalarOnly", func(b *testing.B) {
		bucket := []byte("bench_scalar_low")
		db := seedRebuildBenchScalarBolt(b, bucket, rows, rebuildBenchScalarLowCard)
		rt := compileRebuildBenchSchemaFor[rebuildBenchScalarRec](b)
		cfg := baseRebuildBenchConfig(db, bucket, rt, decodeRebuildBenchScalarRec, releaseRebuildBenchScalarRec)

		runRebuildFullBench(b, rows, rt, cfg)
	})

	b.Run("LowCardinalityWide", func(b *testing.B) {
		bucket := []byte("bench_wide_low")
		db := seedRebuildBenchWideBolt(b, bucket, rows, rebuildBenchWideLowCard, false)
		rt := compileRebuildBenchSchemaFor[rebuildBenchWideRec](b)
		cfg := baseRebuildBenchConfig(db, bucket, rt, decodeRebuildBenchWideRec, releaseRebuildBenchWideRec)

		runRebuildFullBench(b, rows, rt, cfg)
	})

	b.Run("HighCardinalityScalarOnly", func(b *testing.B) {
		bucket := []byte("bench_scalar_high")
		db := seedRebuildBenchScalarBolt(b, bucket, rows, rebuildBenchScalarHighCard)
		rt := compileRebuildBenchSchemaFor[rebuildBenchScalarRec](b)
		cfg := baseRebuildBenchConfig(db, bucket, rt, decodeRebuildBenchScalarRec, releaseRebuildBenchScalarRec)

		runRebuildFullBench(b, rows, rt, cfg)
	})

	b.Run("HighCardinalityWide", func(b *testing.B) {
		bucket := []byte("bench_wide_high")
		db := seedRebuildBenchWideBolt(b, bucket, rows, rebuildBenchWideHighCard, false)
		rt := compileRebuildBenchSchemaFor[rebuildBenchWideRec](b)
		cfg := baseRebuildBenchConfig(db, bucket, rt, decodeRebuildBenchWideRec, releaseRebuildBenchWideRec)

		runRebuildFullBench(b, rows, rt, cfg)
	})

	b.Run("SliceFanoutWide", func(b *testing.B) {
		bucket := []byte("bench_wide_fanout")
		db := seedRebuildBenchWideBolt(b, bucket, rows, rebuildBenchWideSliceFanout, false)
		rt := compileRebuildBenchSchemaFor[rebuildBenchWideRec](b)
		cfg := baseRebuildBenchConfig(db, bucket, rt, decodeRebuildBenchWideRec, releaseRebuildBenchWideRec)

		runRebuildFullBench(b, rows, rt, cfg)
	})
}

func BenchmarkBuildMeasureHeavy(b *testing.B) {
	bucket := []byte("bench_measure_heavy")
	rows := rebuildBenchMeasureRows()
	db := seedRebuildBenchMeasureBolt(b, bucket, rows)
	rt := compileRebuildBenchSchemaFor[rebuildBenchMeasureRec](b)
	cfg := baseRebuildBenchConfig(db, bucket, rt, decodeRebuildBenchMeasureRec, releaseRebuildBenchMeasureRec)

	runRebuildFullBench(b, rows, rt, cfg)
}

func BenchmarkBuildPartialWideSkippedFields(b *testing.B) {
	bucket := []byte("bench_partial_wide")
	rows := rebuildBenchWideRows()
	db := seedRebuildBenchWideBolt(b, bucket, rows, rebuildBenchWideHighCard, false)
	rt := compileRebuildBenchSchemaFor[rebuildBenchWideRec](b)
	cfg := baseRebuildBenchConfig(db, bucket, rt, decodeRebuildBenchWideRec, releaseRebuildBenchWideRec)

	base, err := Build(cfg, newRebuildTestState(rt))
	if err != nil {
		b.Fatalf("base Build: %v", err)
	}

	manager := snapshot.NewRegistry(false)
	prev := snapshot.NewView(1, nil, rt, snapshot.CacheConfig{}, base.Storage)
	manager.Publish(prev)

	cfg.Current = prev
	cfg.SkipFields = map[string]struct{}{
		"name":  {},
		"email": {},
		"tags":  {},
		"roles": {},
		"opt":   {},
	}
	cfg.SkipMeasureFields = map[string]struct{}{
		"latency": {},
	}

	var metrics rebuildBenchStorageMetrics
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := Build(cfg, State{
			Index:             prev.Index,
			NilIndex:          prev.NilIndex,
			LenIndex:          prev.LenIndex,
			LenZeroComplement: prev.LenZeroComplement,
			Measure:           prev.Measure,
			Universe:          prev.Universe,
		})
		if err != nil {
			b.Fatalf("partial Build: %v", err)
		}
		if i == 0 {
			metrics = collectRebuildBenchStorageMetrics(rt, result.Storage)
		}
		next := snapshot.NewView(uint64(i+2), prev, rt, snapshot.CacheConfig{}, result.Storage)
		manager.Publish(next)
		prev = next
		cfg.Current = prev
	}
	b.StopTimer()
	reportRebuildBenchMetrics(b, rows, metrics)
}

func BenchmarkBuildNoActiveLenRebuild(b *testing.B) {
	bucket := []byte("bench_no_active_len")
	rows := rebuildBenchWideRows()
	db := seedRebuildBenchWideBolt(b, bucket, rows, rebuildBenchWideSliceFanout, false)
	rt := compileRebuildBenchSchemaFor[rebuildBenchWideRec](b)
	cfg := baseRebuildBenchConfig(db, bucket, rt, decodeRebuildBenchWideRec, releaseRebuildBenchWideRec)

	base, err := Build(cfg, newRebuildTestState(rt))
	if err != nil {
		b.Fatalf("base Build: %v", err)
	}

	manager := snapshot.NewRegistry(false)
	prev := snapshot.NewView(1, nil, rt, snapshot.CacheConfig{}, base.Storage)
	manager.Publish(prev)

	cfg = Config{
		Schema: rt,
		SkipFields: map[string]struct{}{
			"country": {},
			"plan":    {},
			"status":  {},
			"name":    {},
			"email":   {},
			"age":     {},
			"score":   {},
			"active":  {},
			"tags":    {},
			"roles":   {},
			"opt":     {},
		},
		SkipMeasureFields: map[string]struct{}{
			"revenue": {},
			"latency": {},
		},
	}

	var metrics rebuildBenchStorageMetrics
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := Build(cfg, State{
			Index:     prev.Index,
			NilIndex:  prev.NilIndex,
			Measure:   prev.Measure,
			Universe:  prev.Universe,
			LenLoaded: false,
		})
		if err != nil {
			b.Fatalf("Build no-active len: %v", err)
		}
		if i == 0 {
			metrics = collectRebuildBenchStorageMetrics(rt, result.Storage)
		}
		next := snapshot.NewView(uint64(i+2), prev, rt, snapshot.CacheConfig{}, result.Storage)
		manager.Publish(next)
		prev = next
	}
	b.StopTimer()
	reportRebuildBenchMetrics(b, rows, metrics)
}

func BenchmarkBuildPartialSkippedFields(b *testing.B) {
	bucket := []byte("bench_partial")
	rows := rebuildBenchRows()
	db := seedRebuildBenchBolt(b, bucket, rows, false)
	rt := compileRebuildBenchSchema(b)
	cfg := baseRebuildTestConfig(db, bucket, rt)

	base, err := Build(cfg, newRebuildTestState(rt))
	if err != nil {
		b.Fatalf("base Build: %v", err)
	}

	manager := snapshot.NewRegistry(false)
	prev := snapshot.NewView(1, nil, rt, snapshot.CacheConfig{}, base.Storage)
	manager.Publish(prev)

	cfg.Current = prev
	cfg.SkipFields = map[string]struct{}{"tags": {}}
	cfg.SkipMeasureFields = map[string]struct{}{"score": {}}

	var metrics rebuildBenchStorageMetrics
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := Build(cfg, State{
			Index:             prev.Index,
			NilIndex:          prev.NilIndex,
			LenIndex:          prev.LenIndex,
			LenZeroComplement: prev.LenZeroComplement,
			Measure:           prev.Measure,
			Universe:          prev.Universe,
		})
		if err != nil {
			b.Fatalf("partial Build: %v", err)
		}
		if i == 0 {
			metrics = collectRebuildBenchStorageMetrics(rt, result.Storage)
		}
		next := snapshot.NewView(uint64(i+2), prev, rt, snapshot.CacheConfig{}, result.Storage)
		manager.Publish(next)
		prev = next
		cfg.Current = prev
	}
	b.StopTimer()
	reportRebuildBenchMetrics(b, rows, metrics)
}
