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

const (
	benchStringQueryN     = 300_000
	benchStringQueryBatch = 100_000
	benchStringWriteSeed  = 120_000
)

var (
	oneStringDB  *DB[string, UserBench]
	oneStringRaw *bbolt.DB
	oneStringDir string
	oneStringMu  sync.Mutex
)

func openBenchDBString(b *testing.B) (*DB[string, UserBench], *bbolt.DB, string) {
	b.Helper()
	dir, err := os.MkdirTemp("", "rbi-bench-string-*")
	if err != nil {
		b.Fatalf("os.MkdirTemp: %v", err)
	}
	path := filepath.Join(dir, "bench_string.db")

	db, raw := openBoltAndNew[string, UserBench](b, path, Options{
		DisableIndexLoad:    true,
		DisableIndexStore:   true,
		DisableIndexRebuild: true,
	})
	return db, raw, dir
}

func seedBenchDataString(b *testing.B, db *DB[string, UserBench], n int) {
	b.Helper()

	db.DisableIndexing()
	db.DisableSync()

	defer func() {
		db.EnableIndexing()
		db.EnableSync()
		if err := db.RebuildIndex(); err != nil {
			b.Fatalf("rebuilding index: %v", err)
		}
	}()

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
	b.Helper()
	oneStringMu.Lock()
	defer oneStringMu.Unlock()

	if oneStringDB != nil && !oneStringDB.closed.Load() {
		return oneStringDB
	}

	closeBenchDBStringLocked()

	db, raw, dir := openBenchDBString(b)
	b.StopTimer()
	seedBenchDataString(b, db, n)
	_, _ = db.QueryKeys(qx.Query(qx.EQ("country", "NL"))) // warmup
	b.StartTimer()

	oneStringDB = db
	oneStringRaw = raw
	oneStringDir = dir
	return db
}

func closeBenchDBStringLocked() {
	db := oneStringDB
	raw := oneStringRaw
	dir := oneStringDir

	oneStringDB = nil
	oneStringRaw = nil
	oneStringDir = ""

	if db != nil {
		_ = db.Close()
	}
	if raw != nil {
		_ = raw.Close()
	}
	if dir != "" {
		_ = os.RemoveAll(dir)
	}
}

func buildWriteBenchDBString(b *testing.B) *DB[string, UserBench] {
	b.Helper()
	dir := b.TempDir()

	db, raw := openBoltAndNew[string, UserBench](b, filepath.Join(dir, "bench_write_string_seeded.db"), Options{
		DisableIndexRebuild: true,
	})
	b.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	db.DisableSync()

	db.DisableIndexing()

	r := newRand(42)
	countries := []string{"US", "NL", "DE", "PL", "SE", "FR", "GB", "ES"}
	plans := []string{"free", "basic", "pro", "enterprise"}

	ids := make([]string, 0, 1000)
	vals := make([]*UserBench, 0, 1000)
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
		if len(ids) == 1000 {
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

	if err := db.RebuildIndex(); err != nil {
		b.Fatalf("rebuild string index: %v", err)
	}
	db.EnableSync()
	db.EnableIndexing()
	return db
}

func Benchmark_Query_Index_Count_Simple_EQ_Count_StringKeyDB(b *testing.B) {
	db := buildBenchDBString(b, benchStringQueryN)
	q := qx.Query(qx.EQ("country", "NL"))

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, err := db.Count(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Query_Index_Count_Realistic_Cohort_StringKeyDB(b *testing.B) {
	db := buildBenchDBString(b, benchStringQueryN)
	q := qx.Query(
		qx.AND(
			qx.NOTIN("status", []string{"banned"}),
			qx.IN("country", []string{"NL", "DE", "PL", "SE", "FR", "ES", "GB"}),
			qx.GTE("age", 25),
			qx.LTE("age", 45),
			qx.HASANY("tags", []string{"go", "db", "security"}),
			qx.GTE("score", 80.0),
		),
	)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, err := db.Count(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Query_Index_Count_Realistic_HeavyOR_StringKeyDB(b *testing.B) {
	db := buildBenchDBString(b, benchStringQueryN)
	q := qx.Query(
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

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, err := db.Count(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Query_Index_Keys_Medium_IN_Limit_StringKeyDB(b *testing.B) {
	db := buildBenchDBString(b, benchStringQueryN)
	q := qx.Query(qx.IN("country", []string{"NL", "DE"})).Max(100)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Read_Query_Items_SingleByEmail_StringKeyDB(b *testing.B) {
	db := buildBenchDBString(b, benchStringQueryN)
	target := fmt.Sprintf("user%06d@example.com", benchStringQueryN/2)
	q := qx.Query(qx.EQ("email", target)).Max(1)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, err := db.Query(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Read_Index_Keys_Scan_All_StringKeyDB(b *testing.B) {
	db := buildBenchDBString(b, benchStringQueryN)

	var count int
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
