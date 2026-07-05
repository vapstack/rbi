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

func openBenchStringCollection(b *testing.B) (*Collection[string, UserBench], *bbolt.DB, string) {
	b.Helper()
	dir, err := os.MkdirTemp("", "rbi-bench-string-*")
	if err != nil {
		b.Fatalf("os.MkdirTemp: %v", err)
	}
	path := filepath.Join(dir, "bench_string.db")

	c, bolt := openBoltAndCollection[string, UserBench](b, path, benchOptions())
	return c, bolt, dir
}

func seedBenchDataString(tb testing.TB, c *Collection[string, UserBench], n int) {
	tb.Helper()

	c.disableSync()
	defer c.enableSync()

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
		if err := writeSets(c, ids, vals); err != nil {
			tb.Fatalf("MultiSet(seed string): %v", err)
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
	clc *Collection[string, UserBench]
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

func buildBenchStringCollection(b *testing.B, n int) *Collection[string, UserBench] {
	return buildBenchStringCollectionWithMode(b, n, benchCacheModes[0])
}

func buildBenchStringCollectionWithMode(b *testing.B, n int, mode benchCacheMode) *Collection[string, UserBench] {
	b.Helper()
	oneStringMu.Lock()
	defer oneStringMu.Unlock()

	key := benchStringDBFamilyKey(n)
	if cached := oneStringDBs[key]; cached != nil && cached.clc != nil && cached.clc.state.Load()&collectionClosed == 0 {
		return cached.clc
	}

	c, bolt, dir := openBenchStringCollection(b)
	b.StopTimer()
	seedBenchDataString(b, c, n)
	b.StartTimer()
	oneStringDBs[key] = &cachedBenchStringDB{clc: c, raw: bolt, dir: dir}
	registerBenchSuiteCleanup(func() {
		_ = c.Close()
		_ = bolt.Close()
		_ = os.RemoveAll(dir)
	})
	return c
}

func runStringCountBenchCacheModes(b *testing.B, qf func() *qx.QX) {
	b.Helper()
	runBenchCacheModes(b, func(b *testing.B, mode benchCacheMode) {
		c := buildBenchStringCollectionWithMode(b, benchN, mode)
		runStringCountBenchWithMode(b, c, qf(), mode)
	})
}

func buildWriteBenchStringCollection(b *testing.B) *Collection[string, UserBench] {
	b.Helper()
	dir := b.TempDir()

	c, bolt := openBoltAndCollection[string, UserBench](b, filepath.Join(dir, "bench_write_string_seeded.db"), Options{
		DisableIndexStore: true,
	})
	b.Cleanup(func() {
		_ = c.Close()
		_ = bolt.Close()
	})
	c.disableSync()

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
			if err := writeSets(c, ids, vals); err != nil {
				b.Fatalf("seed MultiSet string: %v", err)
			}
			ids = ids[:0]
			vals = vals[:0]
		}
	}
	if len(ids) > 0 {
		if err := writeSets(c, ids, vals); err != nil {
			b.Fatalf("seed final MultiSet string: %v", err)
		}
	}

	c.enableSync()
	return c
}

func Benchmark_Query_Index_Count_Simple_EQ_Count_StringKeyDB(b *testing.B) {
	c := buildBenchStringCollection(b, benchN)
	q := qx.Query(qx.EQ("country", "NL"))
	runStringCountBench(b, c, q)
}

func warmBenchCountOnceString(b *testing.B, c *Collection[string, UserBench], q *qx.QX) {
	b.Helper()
	b.StopTimer()
	defer b.StartTimer()
	runBenchCountOnceString(b, c, q)
}

func runBenchCountOnceString(b *testing.B, c *Collection[string, UserBench], q *qx.QX) {
	b.Helper()
	if _, err := readCount(c, q.Filter); err != nil {
		b.Fatal(err)
	}
}

func warmBenchQueryKeysOnceString(b *testing.B, c *Collection[string, UserBench], q *qx.QX) {
	b.Helper()
	b.StopTimer()
	defer b.StartTimer()
	runBenchQueryKeysOnceString(b, c, q)
}

func runBenchQueryKeysOnceString(b *testing.B, c *Collection[string, UserBench], q *qx.QX) {
	b.Helper()
	if _, err := readQueryKeys(c, q); err != nil {
		b.Fatal(err)
	}
}

func warmBenchReadQueryOnceString(b *testing.B, c *Collection[string, UserBench], q *qx.QX) {
	b.Helper()
	b.StopTimer()
	defer b.StartTimer()
	runBenchReadQueryOnceString(b, c, q)
}

func runBenchReadQueryOnceString(b *testing.B, c *Collection[string, UserBench], q *qx.QX) {
	b.Helper()
	items, err := readQuery(c, q)
	if err != nil {
		b.Fatal(err)
	}
	c.ReleaseRecords(items...)
}

func runStringCountBench(b *testing.B, c *Collection[string, UserBench], q *qx.QX) {
	runStringCountBenchWithMode(b, c, q, benchCacheModes[0])
}

func runStringCountBenchWithMode(b *testing.B, c *Collection[string, UserBench], q *qx.QX, mode benchCacheMode) {
	b.Helper()
	b.ReportAllocs()
	state := prepareReadBenchWithMode(
		b,
		c,
		q,
		mode,
		warmBenchCountOnceString,
		runBenchCountOnceString,
		buildUserBenchTurnoverRingString,
	)
	for b.Loop() {
		state.beforeQuery(b, c)
		runBenchCountOnceString(b, c, q)
	}
}

func runStringQueryKeysBench(b *testing.B, c *Collection[string, UserBench], q *qx.QX) {
	runStringQueryKeysBenchWithMode(b, c, q, benchCacheModes[0])
}

func runStringQueryKeysBenchWithMode(b *testing.B, c *Collection[string, UserBench], q *qx.QX, mode benchCacheMode) {
	b.Helper()
	b.ReportAllocs()
	state := prepareReadBenchWithMode(
		b,
		c,
		q,
		mode,
		warmBenchQueryKeysOnceString,
		runBenchQueryKeysOnceString,
		buildUserBenchTurnoverRingString,
	)
	for b.Loop() {
		state.beforeQuery(b, c)
		runBenchQueryKeysOnceString(b, c, q)
	}
}

func runStringReadQueryBench(b *testing.B, c *Collection[string, UserBench], q *qx.QX) {
	runStringReadQueryBenchWithMode(b, c, q, benchCacheModes[0])
}

func runStringReadQueryBenchWithMode(b *testing.B, c *Collection[string, UserBench], q *qx.QX, mode benchCacheMode) {
	b.Helper()
	b.ReportAllocs()
	state := prepareReadBenchWithMode(
		b,
		c,
		q,
		mode,
		warmBenchReadQueryOnceString,
		runBenchReadQueryOnceString,
		buildUserBenchTurnoverRingString,
	)
	for b.Loop() {
		state.beforeQuery(b, c)
		runBenchReadQueryOnceString(b, c, q)
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
	c := buildBenchStringCollection(b, benchN)
	q := qx.Query(qx.IN("country", []string{"NL", "DE"})).Limit(100)
	runStringQueryKeysBench(b, c, q)
}

func Benchmark_Read_Query_Items_SingleByEmail_StringKeyDB(b *testing.B) {
	c := buildBenchStringCollection(b, benchN)
	target := fmt.Sprintf("user%06d@example.com", benchN/2)
	q := qx.Query(qx.EQ("email", target)).Limit(1)
	runStringReadQueryBench(b, c, q)
}

func Benchmark_Read_Index_Keys_Query_All_StringKeyDB(b *testing.B) {
	c := buildBenchStringCollection(b, benchN)

	var count int
	prepareReadBenchSnapshot(b, c)
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		keys, err := readQueryKeys(c, qx.Query())
		if err != nil {
			b.Fatalf("QueryKeys(string): %v", err)
		}
		count = len(keys)
	}
	b.ReportMetric(float64(count), "keys/op")
}

func Benchmark_Write_Set_New_Indexed_StringKeyDB(b *testing.B) {
	c := buildWriteBenchStringCollection(b)
	prepareWriteBenchStableBase(b, c)
	rec := &UserBench{Name: "new", Age: 20, Country: "DE"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("new-%08d", i+1)
		if err := writeSet(c, key, rec); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Write_Patch_Indexed_StringKeyDB(b *testing.B) {
	c := buildWriteBenchStringCollection(b)
	target := "seed-001000"
	patchA := []Field{{Name: "age", Value: 111}}
	patchB := []Field{{Name: "age", Value: 222}}
	prepareWriteBenchStableBase(b, c)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			if err := writePatch(c, target, patchA); err != nil {
				b.Fatal(err)
			}
		} else {
			if err := writePatch(c, target, patchB); err != nil {
				b.Fatal(err)
			}
		}
	}
}
