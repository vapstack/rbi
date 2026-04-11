package rbi

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/vapstack/qx"
	"go.etcd.io/bbolt"
)

func openBenchDBString(b *testing.B) (*DB[string, UserBench], *bbolt.DB, string) {
	b.Helper()
	dir, err := os.MkdirTemp("", "rbi-bench-string-*")
	if err != nil {
		b.Fatalf("os.MkdirTemp: %v", err)
	}
	path := filepath.Join(dir, "bench_string.db")

	db, raw := openBoltAndNew[string, UserBench](b, path, benchOptions())
	return db, raw, dir
}

func seedBenchDataString(tb testing.TB, db *DB[string, UserBench], n int) {
	tb.Helper()

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

	ids := make([]string, 0, benchBatch)
	vals := make([]*UserBench, 0, benchBatch)

	flush := func() {
		if len(ids) == 0 {
			return
		}
		if err := db.BatchSet(ids, vals); err != nil {
			tb.Fatalf("BatchSet(seed string): %v", err)
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
		if len(ids) == benchBatch {
			flush()
		}
	}
	flush()
}

type cachedBenchStringDB struct {
	db  *DB[string, UserBench]
	raw *bbolt.DB
	dir string
}

var (
	oneStringDBs = make(map[string]*cachedBenchStringDB)
	oneStringMu  sync.Mutex
)

func benchStringDBFamilyKey(n int) string {
	return "user_string/" + strconv.Itoa(n)
}

func buildBenchDBString(b *testing.B, n int) *DB[string, UserBench] {
	return buildBenchDBStringWithMode(b, n, benchCacheModes[0])
}

func buildBenchDBStringWithMode(b *testing.B, n int, mode benchCacheMode) *DB[string, UserBench] {
	b.Helper()
	oneStringMu.Lock()
	defer oneStringMu.Unlock()

	key := benchStringDBFamilyKey(n)
	if cached := oneStringDBs[key]; cached != nil && cached.db != nil && !cached.db.closed.Load() {
		return cached.db
	}

	db, raw, dir := openBenchDBString(b)
	b.StopTimer()
	seedBenchDataString(b, db, n)
	b.StartTimer()
	oneStringDBs[key] = &cachedBenchStringDB{db: db, raw: raw, dir: dir}
	registerBenchSuiteCleanup(func() {
		_ = db.Close()
		_ = raw.Close()
		_ = os.RemoveAll(dir)
	})
	return db
}

func runStringCountBenchCacheModes(b *testing.B, qf func() *qx.QX) {
	b.Helper()
	runBenchCacheModes(b, func(b *testing.B, mode benchCacheMode) {
		db := buildBenchDBStringWithMode(b, benchN, mode)
		runStringCountBenchWithMode(b, db, qf(), mode)
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
	for i := 1; i <= writeBenchSeedCount; i++ {
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
	db := buildBenchDBString(b, benchN)
	q := qx.Query(qx.EQ("country", "NL"))
	runStringCountBench(b, db, q)
}

func warmBenchCountOnceString(b *testing.B, db *DB[string, UserBench], q *qx.QX) {
	b.Helper()
	b.StopTimer()
	defer b.StartTimer()
	runBenchCountOnceString(b, db, q)
}

func runBenchCountOnceString(b *testing.B, db *DB[string, UserBench], q *qx.QX) {
	b.Helper()
	if _, err := db.Count(q); err != nil {
		b.Fatal(err)
	}
}

func warmBenchQueryKeysOnceString(b *testing.B, db *DB[string, UserBench], q *qx.QX) {
	b.Helper()
	b.StopTimer()
	defer b.StartTimer()
	runBenchQueryKeysOnceString(b, db, q)
}

func runBenchQueryKeysOnceString(b *testing.B, db *DB[string, UserBench], q *qx.QX) {
	b.Helper()
	if _, err := db.QueryKeys(q); err != nil {
		b.Fatal(err)
	}
}

func warmBenchReadQueryOnceString(b *testing.B, db *DB[string, UserBench], q *qx.QX) {
	b.Helper()
	b.StopTimer()
	defer b.StartTimer()
	runBenchReadQueryOnceString(b, db, q)
}

func runBenchReadQueryOnceString(b *testing.B, db *DB[string, UserBench], q *qx.QX) {
	b.Helper()
	items, err := db.Query(q)
	if err != nil {
		b.Fatal(err)
	}
	db.ReleaseRecords(items...)
}

func runStringCountBench(b *testing.B, db *DB[string, UserBench], q *qx.QX) {
	runStringCountBenchWithMode(b, db, q, benchCacheModes[0])
}

func runStringCountBenchWithMode(b *testing.B, db *DB[string, UserBench], q *qx.QX, mode benchCacheMode) {
	b.Helper()
	b.ReportAllocs()
	state := prepareReadBenchWithMode(
		b,
		db,
		q,
		mode,
		warmBenchCountOnceString,
		runBenchCountOnceString,
		buildUserBenchTurnoverRingString,
	)
	for b.Loop() {
		state.beforeQuery(b, db)
		runBenchCountOnceString(b, db, q)
	}
}

func runStringQueryKeysBench(b *testing.B, db *DB[string, UserBench], q *qx.QX) {
	runStringQueryKeysBenchWithMode(b, db, q, benchCacheModes[0])
}

func runStringQueryKeysBenchWithMode(b *testing.B, db *DB[string, UserBench], q *qx.QX, mode benchCacheMode) {
	b.Helper()
	b.ReportAllocs()
	state := prepareReadBenchWithMode(
		b,
		db,
		q,
		mode,
		warmBenchQueryKeysOnceString,
		runBenchQueryKeysOnceString,
		buildUserBenchTurnoverRingString,
	)
	for b.Loop() {
		state.beforeQuery(b, db)
		runBenchQueryKeysOnceString(b, db, q)
	}
}

func runStringReadQueryBench(b *testing.B, db *DB[string, UserBench], q *qx.QX) {
	runStringReadQueryBenchWithMode(b, db, q, benchCacheModes[0])
}

func runStringReadQueryBenchWithMode(b *testing.B, db *DB[string, UserBench], q *qx.QX, mode benchCacheMode) {
	b.Helper()
	b.ReportAllocs()
	state := prepareReadBenchWithMode(
		b,
		db,
		q,
		mode,
		warmBenchReadQueryOnceString,
		runBenchReadQueryOnceString,
		buildUserBenchTurnoverRingString,
	)
	for b.Loop() {
		state.beforeQuery(b, db)
		runBenchReadQueryOnceString(b, db, q)
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
	db := buildBenchDBString(b, benchN)
	q := qx.Query(qx.IN("country", []string{"NL", "DE"})).Max(100)
	runStringQueryKeysBench(b, db, q)
}

func Benchmark_Read_Query_Items_SingleByEmail_StringKeyDB(b *testing.B) {
	db := buildBenchDBString(b, benchN)
	target := fmt.Sprintf("user%06d@example.com", benchN/2)
	q := qx.Query(qx.EQ("email", target)).Max(1)
	runStringReadQueryBench(b, db, q)
}

func Benchmark_Read_Index_Keys_Scan_All_StringKeyDB(b *testing.B) {
	db := buildBenchDBString(b, benchN)

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
