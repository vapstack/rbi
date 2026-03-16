package rbi

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/vapstack/qx"
	"go.etcd.io/bbolt"
)

const (
	benchStringQueryN     = 300_000
	benchStringQueryBatch = 20_000
	benchStringWriteSeed  = 120_000
)

var (
	oneStringDBs  = make(map[bool]*DB[string, UserBench])
	oneStringRaws = make(map[bool]*bbolt.DB)
	oneStringDirs = make(map[bool]string)
	oneStringMu   sync.Mutex
)

func openBenchDBStringWithCaching(b *testing.B, withCaching bool) (*DB[string, UserBench], *bbolt.DB, string) {
	b.Helper()
	dir, err := os.MkdirTemp("", "rbi-bench-string-*")
	if err != nil {
		b.Fatalf("os.MkdirTemp: %v", err)
	}
	path := filepath.Join(dir, "bench_string.db")

	db, raw := openBoltAndNew[string, UserBench](b, path, benchOptions(withCaching))
	return db, raw, dir
}

func seedBenchDataString(b *testing.B, db *DB[string, UserBench], n int) {
	b.Helper()

	db.DisableSync()
	defer db.EnableSync()

	r := newRand(7)
	countries := []string{"US", "NL", "DE", "PL", "SE", "FR", "GB", "ES"}
	plans := []string{"free", "basic", "pro", "enterprise"}
	statuses := []string{"active", "trial", "paused", "banned"}
	tagsPool := [][]string{
		{"go", "db"},
		{"java"},
		{"rust", "perf"},
		{"ops"},
		{"ml", "python"},
		{"frontend", "js"},
		{"security"},
		{"go", "go", "db"},
		{},
	}
	rolesPool := [][]string{
		{"user"},
		{"user", "admin"},
		{"user", "moderator"},
		{"user", "billing"},
		{"user", "support"},
	}

	ids := make([]string, 0, benchStringQueryBatch)
	vals := make([]*UserBench, 0, benchStringQueryBatch)

	flush := func() {
		if len(ids) == 0 {
			return
		}
		if err := db.BatchSet(ids, vals); err != nil {
			b.Fatalf("BatchSet(seed string): %v", err)
		}
		ids = ids[:0]
		vals = vals[:0]
	}

	for i := 1; i <= n; i++ {
		key := "id-" + strconv.Itoa(i)
		age := 18 + r.IntN(60)
		score := math.Round((r.Float64()*1000.0)*100) / 100

		name := "user-" + strconv.Itoa(i)
		email := fmt.Sprintf("user%06d@example.com", i)

		rec := &UserBench{
			Country: countries[r.IntN(len(countries))],
			Plan:    plans[r.IntN(len(plans))],
			Status:  statuses[r.IntN(len(statuses))],
			Age:     age,
			Score:   score,
			Name:    name,
			Email:   email,
			Tags:    append([]string(nil), tagsPool[r.IntN(len(tagsPool))]...),
			Roles:   append([]string(nil), rolesPool[r.IntN(len(rolesPool))]...),
		}

		ids = append(ids, key)
		vals = append(vals, rec)
		if len(ids) == benchStringQueryBatch {
			flush()
		}
	}
	flush()
}

func buildBenchDBString(b *testing.B, n int) *DB[string, UserBench] {
	return buildBenchDBStringWithCaching(b, n, true)
}

func buildBenchDBStringWithCaching(b *testing.B, n int, withCaching bool) *DB[string, UserBench] {
	b.Helper()
	oneStringMu.Lock()
	defer oneStringMu.Unlock()

	if db := oneStringDBs[withCaching]; db != nil && !db.closed.Load() {
		return db
	}

	db, raw, dir := openBenchDBStringWithCaching(b, withCaching)
	b.StopTimer()
	seedBenchDataString(b, db, n)
	b.StartTimer()

	oneStringDBs[withCaching] = db
	oneStringRaws[withCaching] = raw
	oneStringDirs[withCaching] = dir
	return db
}

func runStringCountBenchCacheModes(b *testing.B, qf func() *qx.QX) {
	b.Helper()
	runBenchCacheModes(b, func(b *testing.B, withCaching bool) {
		db := buildBenchDBStringWithCaching(b, benchStringQueryN, withCaching)
		runStringCountBench(b, db, qf())
	})
}

func buildWriteBenchDBString(b *testing.B) *DB[string, UserBench] {
	b.Helper()
	dir := b.TempDir()

	db, raw := openBoltAndNew[string, UserBench](b, filepath.Join(dir, "bench_write_string_seeded.db"), Options{
		DisableIndexStore: true,
	})
	b.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	db.DisableSync()

	r := newRand(42)
	countries := []string{"US", "NL", "DE", "PL", "SE", "FR", "GB", "ES"}
	plans := []string{"free", "basic", "pro", "enterprise"}

	ids := make([]string, 0, writeBenchSeedBatch)
	vals := make([]*UserBench, 0, writeBenchSeedBatch)
	for i := 1; i <= benchStringWriteSeed; i++ {
		key := fmt.Sprintf("seed-%06d", i)
		rec := &UserBench{
			Country: countries[r.IntN(len(countries))],
			Plan:    plans[r.IntN(len(plans))],
			Age:     18 + r.IntN(60),
			Score:   math.Round(r.Float64()*100000) / 100,
			Name:    "user-" + strconv.Itoa(i),
			Tags:    []string{"go", "seed"},
		}
		ids = append(ids, key)
		vals = append(vals, rec)
		if len(ids) == writeBenchSeedBatch {
			if err := db.BatchSet(ids, vals); err != nil {
				b.Fatalf("seed BatchSet string: %v", err)
			}
			ids = ids[:0]
			vals = vals[:0]
		}
	}
	if len(ids) > 0 {
		if err := db.BatchSet(ids, vals); err != nil {
			b.Fatalf("seed final BatchSet string: %v", err)
		}
	}

	db.EnableSync()
	return db
}

func Benchmark_Query_Index_Count_Simple_EQ_Count_StringKeyDB(b *testing.B) {
	db := buildBenchDBString(b, benchStringQueryN)
	q := qx.Query(qx.EQ("country", "NL"))
	runStringCountBench(b, db, q)
}

func warmBenchDBStateString(b *testing.B, db *DB[string, UserBench]) {
	b.Helper()

	countWarmups := []*qx.QX{
		qx.Query(qx.EQ("country", "NL")),
		qx.Query(qx.EQ("status", "active")),
		qx.Query(qx.PREFIX("email", "user1")),
		qx.Query(qx.HASANY("roles", []string{"admin", "moderator"})),
	}
	for _, q := range countWarmups {
		if _, err := db.Count(q); err != nil {
			b.Fatalf("warmup Count(%+v): %v", q, err)
		}
	}

	keyWarmups := []*qx.QX{
		qx.Query().Max(100),
		qx.Query(qx.IN("country", []string{"NL", "DE"})).Max(100),
		qx.Query(qx.EQ("status", "active")).By("age", qx.ASC).Max(50),
		qx.Query(
			qx.OR(
				qx.HAS("roles", []string{"admin"}),
				qx.EQ("plan", "enterprise"),
			),
		).Max(100),
	}
	for _, q := range keyWarmups {
		if _, err := db.QueryKeys(q); err != nil {
			b.Fatalf("warmup QueryKeys(%+v): %v", q, err)
		}
	}

	readWarmups := []*qx.QX{
		qx.Query(qx.EQ("country", "US")).By("age", qx.DESC).Max(20),
	}
	for _, q := range readWarmups {
		items, err := db.Query(q)
		if err != nil {
			b.Fatalf("warmup Query(%+v): %v", q, err)
		}
		db.ReleaseRecords(items...)
	}

	scanned := 0
	if err := db.ScanKeys("", func(_ string) (bool, error) {
		scanned++
		return scanned < 128, nil
	}); err != nil {
		b.Fatalf("warmup ScanKeys: %v", err)
	}

	runtime.GC()
}

func warmBenchCountOnceString(b *testing.B, db *DB[string, UserBench], q *qx.QX) {
	b.Helper()
	b.StopTimer()
	defer b.StartTimer()
	if _, err := db.Count(q); err != nil {
		b.Fatal(err)
	}
}

func warmBenchQueryKeysOnceString(b *testing.B, db *DB[string, UserBench], q *qx.QX) {
	b.Helper()
	b.StopTimer()
	defer b.StartTimer()
	if _, err := db.QueryKeys(q); err != nil {
		b.Fatal(err)
	}
}

func warmBenchReadQueryOnceString(b *testing.B, db *DB[string, UserBench], q *qx.QX) {
	b.Helper()
	b.StopTimer()
	defer b.StartTimer()
	items, err := db.Query(q)
	if err != nil {
		b.Fatal(err)
	}
	db.ReleaseRecords(items...)
}

func runStringCountBench(b *testing.B, db *DB[string, UserBench], q *qx.QX) {
	b.Helper()
	b.ReportAllocs()
	prepareReadBenchSnapshot(b, db)
	warmBenchCountOnceString(b, db, q)
	b.ResetTimer()
	for b.Loop() {
		_, err := db.Count(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func runStringQueryKeysBench(b *testing.B, db *DB[string, UserBench], q *qx.QX) {
	b.Helper()
	b.ReportAllocs()
	prepareReadBenchSnapshot(b, db)
	warmBenchQueryKeysOnceString(b, db, q)
	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func runStringReadQueryBench(b *testing.B, db *DB[string, UserBench], q *qx.QX) {
	b.Helper()
	b.ReportAllocs()
	prepareReadBenchSnapshot(b, db)
	warmBenchReadQueryOnceString(b, db, q)
	b.ResetTimer()
	for b.Loop() {
		items, err := db.Query(q)
		if err != nil {
			b.Fatal(err)
		}
		db.ReleaseRecords(items...)
	}
}

func Benchmark_Query_Index_Count_Realistic_Cohort_StringKeyDB(b *testing.B) {
	runStringCountBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.AND(
				qx.NOTIN("status", []string{"banned"}),
				qx.IN("country", []string{"NL", "DE", "PL", "SE", "FR", "ES", "GB"}),
				qx.GTE("age", 25),
				qx.LTE("age", 45),
				qx.HASANY("tags", []string{"go", "db", "security"}),
				qx.GTE("score", 80.0),
			),
		)
	})
}

func Benchmark_Query_Index_Count_Realistic_HeavyOR_StringKeyDB(b *testing.B) {
	runStringCountBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.OR(
				qx.AND(
					qx.EQ("country", "DE"),
					qx.HASANY("tags", []string{"rust", "go"}),
					qx.GTE("score", 40.0),
				),
				qx.AND(
					qx.PREFIX("email", "user1"),
					qx.EQ("status", "active"),
				),
				qx.AND(
					qx.EQ("plan", "enterprise"),
					qx.GTE("age", 30),
				),
				qx.AND(
					qx.HASANY("roles", []string{"admin"}),
					qx.NOTIN("status", []string{"banned"}),
				),
			),
		)
	})
}

func Benchmark_Query_Index_Keys_Medium_IN_Limit_StringKeyDB(b *testing.B) {
	db := buildBenchDBString(b, benchStringQueryN)
	q := qx.Query(qx.IN("country", []string{"NL", "DE"})).Max(100)
	runStringQueryKeysBench(b, db, q)
}

func Benchmark_Read_Query_Items_SingleByEmail_StringKeyDB(b *testing.B) {
	db := buildBenchDBString(b, benchStringQueryN)
	target := fmt.Sprintf("user%06d@example.com", benchStringQueryN/2)
	q := qx.Query(qx.EQ("email", target)).Max(1)
	runStringReadQueryBench(b, db, q)
}

func Benchmark_Read_Index_Keys_Scan_All_StringKeyDB(b *testing.B) {
	db := buildBenchDBString(b, benchStringQueryN)

	var count int
	prepareReadBenchSnapshot(b, db)
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		count = 0
		if err := db.ScanKeys("", func(_ string) (bool, error) {
			count++
			return true, nil
		}); err != nil {
			b.Fatalf("ScanKeys(string): %v", err)
		}
	}
	b.ReportMetric(float64(count), "keys/op")
}

func Benchmark_Write_Set_New_Indexed_StringKeyDB(b *testing.B) {
	db := buildWriteBenchDBString(b)
	prepareWriteBenchStableBase(b, db)
	rec := &UserBench{Name: "new", Age: 20, Country: "DE"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("new-%08d", i+1)
		if err := db.Set(key, rec); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Write_Patch_Indexed_StringKeyDB(b *testing.B) {
	db := buildWriteBenchDBString(b)
	target := "seed-001000"
	patchA := []Field{{Name: "age", Value: 111}}
	patchB := []Field{{Name: "age", Value: 222}}
	prepareWriteBenchStableBase(b, db)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			if err := db.Patch(target, patchA); err != nil {
				b.Fatal(err)
			}
		} else {
			if err := db.Patch(target, patchB); err != nil {
				b.Fatal(err)
			}
		}
	}
}
