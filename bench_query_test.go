package rbi

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/vapstack/qx"
	"go.etcd.io/bbolt"
)

// Benchmark naming tags for grouped runs:
// - _Query_: index/query execution without value decoding.
// - _Read_: read-path benchmarks (Query/ScanKeys-style flows).
// - _Write_: write-path benchmarks.
// - _Index_: index-centric workloads (QueryKeys/Count/ScanKeys).
// - _Keys_: key-only result paths.
// - _Gap_, _Realistic_: workload families.

type UserBench struct {
	ID      uint64   `db:"id"      dbi:"default"`
	Country string   `db:"country" dbi:"default"`
	Plan    string   `db:"plan"    dbi:"default"`
	Status  string   `db:"status"  dbi:"default"`
	Age     int      `db:"age"     dbi:"default"`
	Score   float64  `db:"score"   dbi:"default"`
	Name    string   `db:"name"    dbi:"default"`
	Email   string   `db:"email"   dbi:"default"`
	Tags    []string `db:"tags"    dbi:"default"`
	Roles   []string `db:"roles"   dbi:"default"`
	Blob    []byte   `db:"-"       dbi:"-"`
}

const (
	benchN     = 500_000
	benchBatch = 20_000
)

func benchOptions() Options {
	return testOptions(Options{
		DisableIndexLoad:  true,
		DisableIndexStore: true,
	})
}

func openBenchDB(b *testing.B) (*DB[uint64, UserBench], *bbolt.DB, string) {
	b.Helper()
	dir, err := os.MkdirTemp("", "rbi-bench-*")
	if err != nil {
		b.Fatalf("os.MkdirTemp: %v", err)
	}

	db, raw := openBoltAndNew[uint64, UserBench](b, filepath.Join(dir, "bench.db"), benchOptions())
	return db, raw, dir
}

func seedBenchData(tb testing.TB, db *DB[uint64, UserBench], n int) {
	tb.Helper()

	db.DisableSync()
	defer db.EnableSync()

	r := newRand(1)

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

	ids := make([]uint64, 0, benchBatch)
	vals := make([]*UserBench, 0, benchBatch)

	flush := func() {
		if len(ids) == 0 {
			return
		}
		if err := db.BatchSet(ids, vals); err != nil {
			tb.Fatalf("BatchSet(seed): %v", err)
		}
		ids = ids[:0]
		vals = vals[:0]
	}

	for i := 1; i <= n; i++ {
		id := uint64(i)

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

		ids = append(ids, id)
		vals = append(vals, rec)
		if len(ids) == benchBatch {
			flush()
		}
	}
	flush()
}

type cachedBenchUserDB struct {
	db  *DB[uint64, UserBench]
	raw *bbolt.DB
	dir string
}

var (
	benchDBs = make(map[string]*cachedBenchUserDB)
	oneMu    sync.Mutex
)

func benchDBFamilyKey(n int) string {
	return "user_uint64/" + strconv.Itoa(n)
}

func buildBenchDB(b *testing.B, n int) *DB[uint64, UserBench] {
	return buildBenchDBWithMode(b, n, benchCacheModes[0])
}

func buildBenchDBWithMode(b *testing.B, n int, mode benchCacheMode) *DB[uint64, UserBench] {
	b.Helper()
	oneMu.Lock()
	defer oneMu.Unlock()

	key := benchDBFamilyKey(n)
	if cached := benchDBs[key]; cached != nil && cached.db != nil && !cached.db.closed.Load() {
		return cached.db
	}

	db, raw, dir := openBenchDB(b)

	b.StopTimer()
	seedBenchData(b, db, n)

	// s := db.Stats()
	// b.Logf("total size: %v", s.IndexSize)
	// b.Logf("index size: %v", s.IndexFieldSize)
	// b.Logf("index build rps: %v", s.IndexBuildRPS)
	// b.Logf("index build time: %v", s.IndexBuildTime)
	// b.Logf("key count: %v", s.KeyCount)
	// b.Logf("unique field keys: %v", s.UniqueFieldKeys)

	b.StartTimer()
	benchDBs[key] = &cachedBenchUserDB{db: db, raw: raw, dir: dir}
	registerBenchSuiteCleanup(func() {
		_ = db.Close()
		_ = raw.Close()
		_ = os.RemoveAll(dir)
	})
	return db
}

func warmBenchCountOnceUint64(b *testing.B, db *DB[uint64, UserBench], q *qx.QX) {
	b.Helper()
	b.StopTimer()
	defer b.StartTimer()
	runBenchCountOnceUint64(b, db, q)
}

func runBenchCountOnceUint64(b *testing.B, db *DB[uint64, UserBench], q *qx.QX) {
	b.Helper()
	if _, err := db.Count(q.Filter); err != nil {
		b.Fatal(err)
	}
}

func warmBenchQueryKeysOnceUint64(b *testing.B, db *DB[uint64, UserBench], q *qx.QX) {
	b.Helper()
	b.StopTimer()
	defer b.StartTimer()
	runBenchQueryKeysOnceUint64(b, db, q)
}

func runBenchQueryKeysOnceUint64(b *testing.B, db *DB[uint64, UserBench], q *qx.QX) {
	b.Helper()
	if _, err := db.QueryKeys(q); err != nil {
		b.Fatal(err)
	}
}

func warmBenchReadQueryOnceUint64(b *testing.B, db *DB[uint64, UserBench], q *qx.QX) {
	b.Helper()
	b.StopTimer()
	defer b.StartTimer()
	runBenchReadQueryOnceUint64(b, db, q)
}

func runBenchReadQueryOnceUint64(b *testing.B, db *DB[uint64, UserBench], q *qx.QX) {
	b.Helper()
	items, err := db.Query(q)
	if err != nil {
		b.Fatal(err)
	}
	db.ReleaseRecords(items...)
}

func runBenchCacheModes(b *testing.B, fn func(*testing.B, benchCacheMode)) {
	b.Helper()
	for _, mode := range activeBenchCacheModes() {
		mode := mode
		b.Run(mode.suffix, func(b *testing.B) {
			fn(b, mode)
		})
	}
}

func runCountBenchCacheModes(b *testing.B, qf func() *qx.QX) {
	b.Helper()
	runBenchCacheModes(b, func(b *testing.B, mode benchCacheMode) {
		db := buildBenchDBWithMode(b, benchN, mode)
		runCountBenchWithMode(b, db, qf(), mode)
	})
}

func runQueryKeysBenchCacheModes(b *testing.B, qf func() *qx.QX) {
	b.Helper()
	runBenchCacheModes(b, func(b *testing.B, mode benchCacheMode) {
		db := buildBenchDBWithMode(b, benchN, mode)
		runQueryKeysBenchWithMode(b, db, qf(), mode)
	})
}

func runReadQueryBenchCacheModes(b *testing.B, qf func() *qx.QX) {
	b.Helper()
	runBenchCacheModes(b, func(b *testing.B, mode benchCacheMode) {
		db := buildBenchDBWithMode(b, benchN, mode)
		runReadQueryBenchWithMode(b, db, qf(), mode)
	})
}

func Benchmark_Query_Index_Count_Simple_EQ_Count(b *testing.B) {
	db := buildBenchDB(b, benchN)
	q := qx.Query(qx.EQ("country", "NL"))
	runCountBench(b, db, q)
}

func Benchmark_Query_Index_Count_Simple_IN_Count(b *testing.B) {
	db := buildBenchDB(b, benchN)
	q := qx.Query(qx.IN("country", []string{"NL", "DE", "PL"}))
	runCountBench(b, db, q)
}

func Benchmark_Query_Index_Count_Simple_HASANY_Count(b *testing.B) {
	db := buildBenchDB(b, benchN)
	q := qx.Query(qx.HASANY("roles", []string{"admin", "moderator"}))
	runCountBench(b, db, q)
}

func Benchmark_Query_Index_Count_Simple_NOTIN_Count(b *testing.B) {
	db := buildBenchDB(b, benchN)
	q := qx.Query(qx.NOTIN("status", []string{"banned"}))
	runCountBench(b, db, q)
}

func Benchmark_Query_Index_Count_Realistic_FeedEligible(b *testing.B) {
	runCountBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.EQ("status", "active"),
			qx.NOTIN("plan", []string{"free"}),
			qx.GTE("score", 120.0),
			qx.HASANY("tags", []string{"go", "security", "ops"}),
		)
	})
}

func Benchmark_Query_Index_Count_Realistic_ModerationQueue(b *testing.B) {
	runCountBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.OR(
				qx.AND(
					qx.EQ("status", "trial"),
					qx.HASANY("roles", []string{"moderator", "admin"}),
				),
				qx.AND(
					qx.EQ("status", "paused"),
					qx.GTE("age", 25),
				),
				qx.AND(
					qx.EQ("plan", "enterprise"),
					qx.HASANY("tags", []string{"security", "ops"}),
				),
			),
		)
	})
}

func Benchmark_Query_Index_Count_Realistic_Discovery_OR(b *testing.B) {
	runCountBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.OR(
				qx.AND(
					qx.PREFIX("email", "user1"),
					qx.EQ("status", "active"),
					qx.GTE("score", 60.0),
				),
				qx.AND(
					qx.EQ("country", "DE"),
					qx.HASANY("tags", []string{"rust", "go"}),
					qx.GTE("age", 24),
				),
				qx.AND(
					qx.EQ("plan", "enterprise"),
					qx.HASANY("roles", []string{"admin", "support"}),
					qx.NOTIN("status", []string{"banned"}),
				),
			),
		)
	})
}

func Benchmark_Query_Index_Count_Realistic_DraftReview(b *testing.B) {
	runCountBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.AND(
				qx.EQ("status", "trial"),
				qx.IN("country", []string{"US", "DE", "FR", "GB"}),
				qx.NOTIN("plan", []string{"free"}),
				qx.GTE("age", 21),
				qx.HASANY("roles", []string{"admin", "moderator", "support"}),
			),
		)
	})
}

func Benchmark_Query_Index_Count_Realistic_SecurityAudit(b *testing.B) {
	runCountBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.AND(
				qx.HASANY("roles", []string{"admin", "support"}),
				qx.NOTIN("status", []string{"banned"}),
				qx.GTE("score", 50.0),
				qx.IN("country", []string{"US", "DE", "GB", "FR"}),
			),
		)
	})
}

func Benchmark_Query_Index_Count_Realistic_Cohort_Retention(b *testing.B) {
	runCountBenchCacheModes(b, func() *qx.QX {
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

func Benchmark_Query_Index_Count_Gap_BroadPrefix_Mixed(b *testing.B) {
	runCountBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.PREFIX("email", "user"),
			qx.EQ("status", "active"),
			qx.NOTIN("plan", []string{"free"}),
		)
	})
}

func Benchmark_Query_Index_Count_Gap_HeavyOR_MultiBranch(b *testing.B) {
	runCountBenchCacheModes(b, func() *qx.QX {
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
				qx.AND(
					qx.CONTAINS("name", "user-1"),
					qx.GTE("score", 20.0),
				),
			),
		)
	})
}

func Benchmark_Query_Index_Keys_Simple_First100(b *testing.B) {
	db := buildBenchDB(b, benchN)
	q := qx.Query().Limit(100)
	runQueryKeysBench(b, db, q)
}

func Benchmark_Read_Index_Keys_Scan_All_Uint64(b *testing.B) {
	db := buildBenchDB(b, benchN)

	var count int
	prepareReadBenchSnapshot(b, db)
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		count = 0
		if err := db.ScanKeys(0, func(_ uint64) (bool, error) {
			count++
			return true, nil
		}); err != nil {
			b.Fatalf("ScanKeys: %v", err)
		}
	}
	b.ReportMetric(float64(count), "keys/op")
}

func Benchmark_Query_Index_Keys_Medium_IN_Limit(b *testing.B) {
	db := buildBenchDB(b, benchN)
	q := qx.Query(qx.IN("country", []string{"NL", "DE"})).Limit(100)
	runQueryKeysBench(b, db, q)
}

func Benchmark_Query_Index_Keys_Heavy_Range_Order_Limit(b *testing.B) {
	runQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.EQ("status", "active"),
			qx.GTE("age", 30),
			qx.LT("age", 50),
		).Sort("age", qx.ASC).Limit(100)
	})
}

func Benchmark_Query_Index_Keys_Heavy_Limit(b *testing.B) {
	runQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.OR(
				qx.AND(
					qx.EQ("country", "DE"),
					qx.EQ("plan", "enterprise"),
					qx.HASANY("tags", []string{"go", "security", "ops"}),
				),
				qx.AND(
					qx.PREFIX("email", "user1"),
					qx.LT("age", 25),
				),
				qx.HASNONE("roles", []string{"admin"}),
			),
		).Limit(10)
	})
}

func Benchmark_Query_Index_Keys_Heavy_All(b *testing.B) {
	runQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.OR(
				qx.AND(
					qx.EQ("country", "DE"),
					qx.EQ("plan", "enterprise"),
					qx.HASANY("tags", []string{"go", "security", "ops"}),
				),
				qx.AND(
					qx.PREFIX("email", "user1"),
					qx.LT("age", 25),
				),
				qx.HASNONE("roles", []string{"admin"}),
			),
		)
	})
}

func Benchmark_Query_Index_Keys_Realistic_DashboardFilter_Limit(b *testing.B) {
	db := buildBenchDB(b, benchN)
	// SELECT * FROM users WHERE status='active' AND plan='enterprise' AND country='US' LIMIT 100
	q := qx.Query(
		qx.EQ("status", "active"),
		qx.EQ("plan", "enterprise"),
		qx.EQ("country", "US"),
	).Limit(100)
	runQueryKeysBench(b, db, q)
}

func Benchmark_Query_Index_Keys_Realistic_Analytics_Range_Order_Limit(b *testing.B) {
	runQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.GTE("age", 25),
			qx.LTE("age", 40),
			qx.GT("score", 0.5),
		).Sort("score", qx.DESC).Limit(100)
	})
}

func Benchmark_Query_Index_Keys_Realistic_LeaderBoard(b *testing.B) {
	db := buildBenchDB(b, benchN)
	q := qx.Query().Sort("score", qx.DESC).Limit(10)
	runQueryKeysBench(b, db, q)
}

func Benchmark_Query_Index_Keys_Realistic_Permissions_HasAny_Limit(b *testing.B) {
	db := buildBenchDB(b, benchN)
	// SELECT * FROM users WHERE roles && ['admin', 'moderator']
	q := qx.Query(
		qx.HASANY("roles", []string{"admin", "moderator"}),
	).Limit(100)
	runQueryKeysBench(b, db, q)
}

func Benchmark_Query_Index_Keys_Realistic_Permissions_HasAny_All(b *testing.B) {
	db := buildBenchDB(b, benchN)
	// SELECT * FROM users WHERE roles && ['admin', 'moderator']
	q := qx.Query(
		qx.HASANY("roles", []string{"admin", "moderator"}),
	)
	runQueryKeysBench(b, db, q)
}

func Benchmark_Query_Index_Keys_Realistic_Skills_HasAll_Limit(b *testing.B) {
	db := buildBenchDB(b, benchN)
	// SELECT * FROM users WHERE tags @> ['go', 'db']
	q := qx.Query(
		qx.HASALL("tags", []string{"go", "db"}),
	).Limit(100)
	runQueryKeysBench(b, db, q)
}

func Benchmark_Query_Index_Keys_Realistic_Skills_HasAll_All(b *testing.B) {
	db := buildBenchDB(b, benchN)
	// SELECT * FROM users WHERE tags @> ['go', 'db']
	q := qx.Query(
		qx.HASALL("tags", []string{"go", "db"}),
	)
	runQueryKeysBench(b, db, q)
}

func Benchmark_Query_Index_Keys_Realistic_Exclusion_Limit(b *testing.B) {
	runQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.EQ("status", "active"),
			qx.NE("plan", "free"),
			qx.NOTIN("country", []string{"US", "GB"}),
		).Limit(100)
	})
}

func Benchmark_Query_Index_Keys_Realistic_Exclusion_All(b *testing.B) {
	runQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.EQ("status", "active"),
			qx.NE("plan", "free"),
			qx.NOTIN("country", []string{"US", "GB"}),
		)
	})
}

func Benchmark_Query_Index_Keys_Realistic_Autocomplete_Prefix_Limit(b *testing.B) {
	// This prefix-only microbenchmark is sensitive to cache-mode turnover
	// overhead; keep it single-mode so Cold rotation does not dominate runtime.
	db := buildBenchDB(b, benchN)
	q := qx.Query(qx.PREFIX("email", "user10")).Limit(10)
	runQueryKeysBench(b, db, q)
}

func Benchmark_Query_Index_Keys_Realistic_Autocomplete_Order_Limit(b *testing.B) {
	runQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.PREFIX("email", "user10"),
			qx.EQ("status", "active"),
		).Sort("email", qx.ASC).Limit(10)
	})
}

func Benchmark_Query_Index_Keys_Realistic_Autocomplete_Complex_Limit(b *testing.B) {
	runQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.PREFIX("email", "user10"),
			qx.EQ("status", "active"),
			qx.NOTIN("plan", []string{"free"}),
		).Sort("score", qx.DESC).Limit(10)
	})
}

func Benchmark_Query_Index_Keys_Realistic_ComplexSegment_Limit(b *testing.B) {
	europe := []string{"NL", "DE", "PL", "SE", "FR", "ES", "GB"}
	runQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.EQ("status", "active"),
			qx.IN("country", europe),
			qx.NE("plan", "free"),
			qx.GTE("age", 20),
			qx.HASANY("tags", []string{"security", "ops"}),
		).Limit(100)
	})
}

func Benchmark_Query_Index_Keys_Realistic_ComplexSegment_All(b *testing.B) {
	europe := []string{"NL", "DE", "PL", "SE", "FR", "ES", "GB"}
	runQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.EQ("status", "active"),
			qx.IN("country", europe),
			qx.NE("plan", "free"),
			qx.GTE("age", 20),
			qx.HASANY("tags", []string{"security", "ops"}),
		)
	})
}

func Benchmark_Query_Index_Keys_Realistic_TopLevel_OR_Limit(b *testing.B) {
	runQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.OR(
				qx.HASALL("roles", []string{"admin"}),
				qx.EQ("plan", "enterprise"),
			),
		).Limit(100)
	})
}

func Benchmark_Query_Index_Keys_Realistic_TopLevel_OR_All(b *testing.B) {
	runQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.OR(
				qx.HASALL("roles", []string{"admin"}),
				qx.EQ("plan", "enterprise"),
			),
		)
	})
}

func Benchmark_Query_Index_Keys_Sort_EarlyExit(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	q := qx.Query(
		qx.EQ("status", "active"),
	).Sort("age", qx.ASC).Limit(20)

	prepareReadBenchSnapshot(b, db)
	warmBenchQueryKeysOnceUint64(b, db, q)
	b.ResetTimer()
	for b.Loop() {
		ids, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
		if len(ids) != 20 {
			b.Fatalf("expected 20, got %d", len(ids))
		}
	}
}

func Benchmark_Query_Index_Keys_Sort_DeepOffset_Limit(b *testing.B) {
	runQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.GTE("age", 18),
		).Sort("score", qx.DESC).Offset(5000).Limit(50)
	})
}

func Benchmark_Query_Index_Keys_Sort_Complex_Order_Limit(b *testing.B) {
	db := buildBenchDB(b, benchN)
	q := qx.Query(
		qx.EQ("country", "DE"),
		qx.HASANY("tags", []string{"rust", "go"}),
	).Sort("age", qx.DESC).Limit(50)
	runQueryKeysBench(b, db, q)
}

func Benchmark_Query_Index_Keys_Sort_ArrayPos_Limit(b *testing.B) {
	db := buildBenchDB(b, benchN)
	priority := []string{"enterprise", "pro", "basic", "free"}
	q := qx.Query(qx.EQ("status", "active")).SortBy(qx.POS("plan", priority), qx.ASC).Limit(50)
	runQueryKeysBench(b, db, q)
}

func Benchmark_Query_Index_Keys_Sort_ArrayPos_All(b *testing.B) {
	db := buildBenchDB(b, benchN)
	priority := []string{"enterprise", "pro", "basic", "free"}
	q := qx.Query(qx.EQ("status", "active")).SortBy(qx.POS("plan", priority), qx.ASC)
	runQueryKeysBench(b, db, q)
}

func Benchmark_Query_Index_Keys_Sort_ArrayCount_Limit(b *testing.B) {
	db := buildBenchDB(b, benchN)
	q := qx.Query(qx.EQ("status", "active")).SortBy(qx.LEN("roles"), qx.DESC).Limit(50)
	runQueryKeysBench(b, db, q)
}

func Benchmark_Query_Index_Keys_Sort_ArrayCount_All(b *testing.B) {
	db := buildBenchDB(b, benchN)
	q := qx.Query(qx.EQ("status", "active")).SortBy(qx.LEN("roles"), qx.DESC)
	runQueryKeysBench(b, db, q)
}

func Benchmark_Read_Query_Items_SimpleFetch(b *testing.B) {
	db := buildBenchDB(b, benchN)
	q := qx.Query(qx.EQ("country", "US")).Sort("age", qx.DESC).Limit(20)
	runReadQueryBench(b, db, q)
}

func Benchmark_Read_Query_Items_HeavyFetch(b *testing.B) {
	runReadQueryBenchCacheModes(b, func() *qx.QX {
		return qx.Query(qx.GTE("age", 20)).Sort("score", qx.DESC).Limit(100)
	})
}

func Benchmark_Read_Query_Items_GT_NoMatch(b *testing.B) {
	// This no-match read microbenchmark mostly measures fixed query overhead;
	// Cold cache rotation stretches runtime without adding much signal.
	db := buildBenchDB(b, benchN)
	q := qx.Query(qx.GT("age", 100))
	runReadQueryBench(b, db, q)
}

func Benchmark_Write_Helper_MakePatch(b *testing.B) {
	db := buildBenchDB(b, benchN)
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
		buf = db.MakePatchInto(v1, v2, buf)
	}
}

func Benchmark_Query_Index_Keys_Gap_FacetedSearch_OR_Order_Offset_Limit(b *testing.B) {
	runQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.OR(
				qx.AND(
					qx.EQ("status", "active"),
					qx.IN("country", []string{"US", "DE", "NL", "PL"}),
					qx.HASANY("tags", []string{"go", "ops"}),
					qx.GTE("score", 60.0),
				),
				qx.AND(
					qx.EQ("status", "trial"),
					qx.NOTIN("plan", []string{"free"}),
					qx.GTE("age", 25),
					qx.LTE("age", 40),
				),
				qx.AND(
					qx.HASANY("roles", []string{"admin", "moderator"}),
					qx.GTE("score", 70.0),
				),
			),
		).Sort("score", qx.DESC).Offset(500).Limit(100)
	})
}

func Benchmark_Query_Index_Keys_Gap_CRM_MultiBranch_OR_Limit(b *testing.B) {
	runQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.OR(
				qx.AND(
					qx.PREFIX("email", "user1"),
					qx.EQ("status", "active"),
				),
				qx.AND(
					qx.SUFFIX("email", "@example.com"),
					qx.NOTIN("country", []string{"US", "GB"}),
					qx.GTE("score", 50.0),
				),
				qx.AND(
					qx.EQ("plan", "enterprise"),
					qx.HASANY("tags", []string{"security", "ops"}),
				),
			),
		).Limit(150)
	})
}

func Benchmark_Query_Index_Keys_Gap_OR_NoOrder_AdaptiveLateBranch_Limit(b *testing.B) {
	runQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.OR(
				qx.EQ("email", "user000010@example.com"),
				qx.EQ("email", "user000020@example.com"),
				qx.GTE("email", "user490000@example.com"),
			),
		).Limit(2)
	})
}

func Benchmark_Query_Index_Keys_Gap_HeavyOR_Order_Limit(b *testing.B) {
	runQueryKeysBenchCacheModes(b, func() *qx.QX {
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
			),
		).Sort("score", qx.DESC).Limit(120)
	})
}

func Benchmark_Query_Index_Keys_Gap_Mixed_EQ_HASANY_GTE_Order_Limit(b *testing.B) {
	runQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.EQ("status", "active"),
			qx.HASANY("tags", []string{"go", "security", "ops"}),
			qx.GTE("age", 25),
		).Sort("score", qx.DESC).Limit(100)
	})
}

func Benchmark_Query_Index_Keys_Gap_BroadPrefix_OtherOrder_Limit(b *testing.B) {
	runQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.PREFIX("email", "user"),
			qx.EQ("status", "active"),
			qx.NOTIN("plan", []string{"free"}),
		).Sort("score", qx.DESC).Limit(100)
	})
}

func Benchmark_Query_Index_Keys_Gap_ArrayCountSort_MixedFilters_Offset_Limit(b *testing.B) {
	runQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.EQ("status", "active"),
			qx.NOTIN("country", []string{"US", "GB"}),
			qx.HASANY("tags", []string{"go", "security", "ops"}),
			qx.GTE("age", 25),
		).SortBy(qx.LEN("roles"), qx.DESC).Offset(2000).Limit(100)
	})
}

func runQueryKeysBench(b *testing.B, db *DB[uint64, UserBench], q *qx.QX) {
	runQueryKeysBenchWithMode(b, db, q, benchCacheModes[0])
}

func runQueryKeysBenchWithMode(b *testing.B, db *DB[uint64, UserBench], q *qx.QX, mode benchCacheMode) {
	b.Helper()
	b.ReportAllocs()
	state := prepareReadBenchWithMode(
		b,
		db,
		q,
		mode,
		warmBenchQueryKeysOnceUint64,
		runBenchQueryKeysOnceUint64,
		buildUserBenchTurnoverRingUint64,
	)
	for b.Loop() {
		state.beforeQuery(b, db)
		runBenchQueryKeysOnceUint64(b, db, q)
	}
}

func runCountBench(b *testing.B, db *DB[uint64, UserBench], q *qx.QX) {
	runCountBenchWithMode(b, db, q, benchCacheModes[0])
}

func runCountBenchWithMode(b *testing.B, db *DB[uint64, UserBench], q *qx.QX, mode benchCacheMode) {
	b.Helper()
	b.ReportAllocs()
	state := prepareReadBenchWithMode(
		b,
		db,
		q,
		mode,
		warmBenchCountOnceUint64,
		runBenchCountOnceUint64,
		buildUserBenchTurnoverRingUint64,
	)
	for b.Loop() {
		state.beforeQuery(b, db)
		runBenchCountOnceUint64(b, db, q)
	}
}

func runReadQueryBench(b *testing.B, db *DB[uint64, UserBench], q *qx.QX) {
	runReadQueryBenchWithMode(b, db, q, benchCacheModes[0])
}

func runReadQueryBenchWithMode(b *testing.B, db *DB[uint64, UserBench], q *qx.QX, mode benchCacheMode) {
	b.Helper()
	b.ReportAllocs()
	state := prepareReadBenchWithMode(
		b,
		db,
		q,
		mode,
		warmBenchReadQueryOnceUint64,
		runBenchReadQueryOnceUint64,
		buildUserBenchTurnoverRingUint64,
	)
	for b.Loop() {
		state.beforeQuery(b, db)
		runBenchReadQueryOnceUint64(b, db, q)
	}
}

func sortedIDs(in []uint64) []uint64 {
	out := append([]uint64(nil), in...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
