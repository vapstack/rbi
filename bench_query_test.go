package rbi

import (
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/qir"
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
	ID      uint64   `db:"id"      rbi:"index"`
	Country string   `db:"country" rbi:"index"`
	Plan    string   `db:"plan"    rbi:"index"`
	Status  string   `db:"status"  rbi:"index"`
	Age     int      `db:"age"     rbi:"index"`
	Score   float64  `db:"score"   rbi:"index"`
	Name    string   `db:"name"    rbi:"index"`
	Email   string   `db:"email"   rbi:"index"`
	Tags    []string `db:"tags"    rbi:"index"`
	Roles   []string `db:"roles"   rbi:"index"`
	Blob    []byte   `db:"-"       rbi:"-"`
}

type queryIRBenchResolver struct{}

func (queryIRBenchResolver) ResolveField(name string) (qir.FieldInfo, bool) {
	switch name {
	case "id":
		return qir.FieldInfo{Ordinal: 0, Caps: qir.FieldCapAll}, true
	case "country":
		return qir.FieldInfo{Ordinal: 1, Caps: qir.FieldCapAll}, true
	case "plan":
		return qir.FieldInfo{Ordinal: 2, Caps: qir.FieldCapAll}, true
	case "status":
		return qir.FieldInfo{Ordinal: 3, Caps: qir.FieldCapAll}, true
	case "age":
		return qir.FieldInfo{Ordinal: 4, Caps: qir.FieldCapAll}, true
	case "score":
		return qir.FieldInfo{Ordinal: 5, Caps: qir.FieldCapAll}, true
	case "name":
		return qir.FieldInfo{Ordinal: 6, Caps: qir.FieldCapAll}, true
	case "email":
		return qir.FieldInfo{Ordinal: 7, Caps: qir.FieldCapAll}, true
	case "tags":
		return qir.FieldInfo{Ordinal: 8, Caps: qir.FieldCapAll}, true
	case "roles":
		return qir.FieldInfo{Ordinal: 9, Caps: qir.FieldCapAll}, true
	}
	return qir.FieldInfo{Ordinal: qir.NoFieldOrdinal}, false
}

const (
	benchN     = 500_000
	benchBatch = 100_000
)

var benchQueryIRSink int

func Benchmark_Query_IR_PrepareAverageQuery(b *testing.B) {
	q := qx.Query(
		qx.EQ("status", "active"),
		qx.IN("country", []string{"US", "DE", "NL"}),
		qx.GTE("age", 25),
		qx.LT("age", 55),
		qx.HASANY("tags", []string{"go", "ops"}),
	).Sort("score", qx.DESC).Limit(100)

	resolver := queryIRBenchResolver{}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		prepared, err := qir.PrepareQuery(q, resolver)
		if err != nil {
			b.Fatal(err)
		}
		benchQueryIRSink += int(prepared.Expr.Op) + int(prepared.Limit)
		prepared.Release()
	}
}

func benchOptions() Options {
	return testOptions(Options{
		DisableIndexLoad:  true,
		DisableIndexStore: true,
	})
}

func openBenchDB(b *testing.B) (*Collection[uint64, UserBench], *bbolt.DB, string) {
	b.Helper()
	dir, err := os.MkdirTemp("", "rbi-bench-*")
	if err != nil {
		b.Fatalf("os.MkdirTemp: %v", err)
	}

	c, bolt := openBoltAndCollection[uint64, UserBench](b, filepath.Join(dir, "bench.db"), benchOptions())
	return c, bolt, dir
}

func seedBenchData(tb testing.TB, c *Collection[uint64, UserBench], n int) {
	tb.Helper()

	c.disableSync()
	defer c.enableSync()

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
		if err := writeSets(c, ids, vals); err != nil {
			tb.Fatalf("MultiSet(seed): %v", err)
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
	clc *Collection[uint64, UserBench]
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

func buildBenchCollection(b *testing.B, n int) *Collection[uint64, UserBench] {
	return buildBenchCollectionWithMode(b, n, benchCacheModes[0])
}

func buildBenchCollectionWithMode(b *testing.B, n int, mode benchCacheMode) *Collection[uint64, UserBench] {
	b.Helper()
	oneMu.Lock()
	defer oneMu.Unlock()

	key := benchDBFamilyKey(n)
	if cached := benchDBs[key]; cached != nil && cached.clc != nil && cached.clc.state.Load()&collectionClosed == 0 {
		return cached.clc
	}

	c, bolt, dir := openBenchDB(b)

	b.StopTimer()
	seedBenchData(b, c, n)

	// s := c.Stats()
	// b.Logf("total size: %v", s.IndexSize)
	// b.Logf("index size: %v", s.IndexFieldSize)
	// b.Logf("index build rps: %v", s.IndexBuildRPS)
	// b.Logf("index build time: %v", s.IndexBuildTime)
	// b.Logf("key count: %v", s.KeyCount)
	// b.Logf("unique field keys: %v", s.UniqueFieldKeys)

	b.StartTimer()
	benchDBs[key] = &cachedBenchUserDB{clc: c, raw: bolt, dir: dir}
	registerBenchSuiteCleanup(func() {
		_ = c.Close()
		_ = bolt.Close()
		_ = os.RemoveAll(dir)
	})
	return c
}

func warmBenchCountOnceUint64(b *testing.B, c *Collection[uint64, UserBench], q *qx.QX) {
	b.Helper()
	b.StopTimer()
	defer b.StartTimer()
	runBenchCountOnceUint64(b, c, q)
}

func runBenchCountOnceUint64(b *testing.B, c *Collection[uint64, UserBench], q *qx.QX) {
	b.Helper()
	if _, err := readCount(c, q.Filter); err != nil {
		b.Fatal(err)
	}
}

func warmBenchQueryKeysOnceUint64(b *testing.B, c *Collection[uint64, UserBench], q *qx.QX) {
	b.Helper()
	b.StopTimer()
	defer b.StartTimer()
	runBenchQueryKeysOnceUint64(b, c, q)
}

func runBenchQueryKeysOnceUint64(b *testing.B, c *Collection[uint64, UserBench], q *qx.QX) {
	b.Helper()
	if _, err := readIndexQueryKeys(c, q); err != nil {
		b.Fatal(err)
	}
}

func warmBenchReadQueryOnceUint64(b *testing.B, c *Collection[uint64, UserBench], q *qx.QX) {
	b.Helper()
	b.StopTimer()
	defer b.StartTimer()
	runBenchReadQueryOnceUint64(b, c, q)
}

func runBenchReadQueryOnceUint64(b *testing.B, c *Collection[uint64, UserBench], q *qx.QX) {
	b.Helper()
	items, err := readQuery(c, q)
	if err != nil {
		b.Fatal(err)
	}
	c.ReleaseRecords(items...)
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
		c := buildBenchCollectionWithMode(b, benchN, mode)
		runCountBenchWithMode(b, c, qf(), mode)
	})
}

func runQueryKeysBenchCacheModes(b *testing.B, qf func() *qx.QX) {
	b.Helper()
	runBenchCacheModes(b, func(b *testing.B, mode benchCacheMode) {
		c := buildBenchCollectionWithMode(b, benchN, mode)
		runQueryKeysBenchWithMode(b, c, qf(), mode)
	})
}

func runReadQueryBenchCacheModes(b *testing.B, qf func() *qx.QX) {
	b.Helper()
	runBenchCacheModes(b, func(b *testing.B, mode benchCacheMode) {
		c := buildBenchCollectionWithMode(b, benchN, mode)
		runReadQueryBenchWithMode(b, c, qf(), mode)
	})
}

func Benchmark_Query_Index_Count_Simple_EQ_Count(b *testing.B) {
	c := buildBenchCollection(b, benchN)
	q := qx.Query(qx.EQ("country", "NL"))
	runCountBench(b, c, q)
}

func Benchmark_Query_Index_Count_Simple_IN_Count(b *testing.B) {
	c := buildBenchCollection(b, benchN)
	q := qx.Query(qx.IN("country", []string{"NL", "DE", "PL"}))
	runCountBench(b, c, q)
}

func Benchmark_Query_Index_Count_Simple_HASANY_Count(b *testing.B) {
	c := buildBenchCollection(b, benchN)
	q := qx.Query(qx.HASANY("roles", []string{"admin", "moderator"}))
	runCountBench(b, c, q)
}

func Benchmark_Query_Index_Count_Simple_HASALL_Count(b *testing.B) {
	c := buildBenchCollection(b, benchN)
	q := qx.Query(qx.HASALL("tags", []string{"go", "db"}))
	runCountBench(b, c, q)
}

func Benchmark_Query_Index_Count_Simple_NOTIN_Count(b *testing.B) {
	c := buildBenchCollection(b, benchN)
	q := qx.Query(qx.NOTIN("status", []string{"banned"}))
	runCountBench(b, c, q)
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
	c := buildBenchCollection(b, benchN)
	q := qx.Query().Limit(100)
	runQueryKeysBench(b, c, q)
}

func Benchmark_Query_Index_Keys_Medium_IN_Limit(b *testing.B) {
	c := buildBenchCollection(b, benchN)
	q := qx.Query(qx.IN("country", []string{"NL", "DE"})).Limit(100)
	runQueryKeysBench(b, c, q)
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
	c := buildBenchCollection(b, benchN)
	// SELECT * FROM users WHERE status='active' AND plan='enterprise' AND country='US' LIMIT 100
	q := qx.Query(
		qx.EQ("status", "active"),
		qx.EQ("plan", "enterprise"),
		qx.EQ("country", "US"),
	).Limit(100)
	runQueryKeysBench(b, c, q)
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
	c := buildBenchCollection(b, benchN)
	q := qx.Query().Sort("score", qx.DESC).Limit(10)
	runQueryKeysBench(b, c, q)
}

func Benchmark_Query_Index_Keys_Realistic_Permissions_HasAny_Limit(b *testing.B) {
	c := buildBenchCollection(b, benchN)
	// SELECT * FROM users WHERE roles && ['admin', 'moderator']
	q := qx.Query(
		qx.HASANY("roles", []string{"admin", "moderator"}),
	).Limit(100)
	runQueryKeysBench(b, c, q)
}

func Benchmark_Query_Index_Keys_Realistic_Permissions_HasAny_All(b *testing.B) {
	c := buildBenchCollection(b, benchN)
	// SELECT * FROM users WHERE roles && ['admin', 'moderator']
	q := qx.Query(
		qx.HASANY("roles", []string{"admin", "moderator"}),
	)
	runQueryKeysBench(b, c, q)
}

func Benchmark_Query_Index_Keys_Realistic_Skills_HasAll_Limit(b *testing.B) {
	c := buildBenchCollection(b, benchN)
	// SELECT * FROM users WHERE tags @> ['go', 'db']
	q := qx.Query(
		qx.HASALL("tags", []string{"go", "db"}),
	).Limit(100)
	runQueryKeysBench(b, c, q)
}

func Benchmark_Query_Index_Keys_Realistic_Skills_HasAll_All(b *testing.B) {
	c := buildBenchCollection(b, benchN)
	// SELECT * FROM users WHERE tags @> ['go', 'db']
	q := qx.Query(
		qx.HASALL("tags", []string{"go", "db"}),
	)
	runQueryKeysBench(b, c, q)
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
	c := buildBenchCollection(b, benchN)
	q := qx.Query(qx.PREFIX("email", "user10")).Limit(10)
	runQueryKeysBench(b, c, q)
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
	c := buildBenchCollection(b, benchN)
	b.ReportAllocs()

	q := qx.Query(
		qx.EQ("status", "active"),
	).Sort("age", qx.ASC).Limit(20)

	prepareReadBenchSnapshot(b, c)
	warmBenchQueryKeysOnceUint64(b, c, q)
	b.ResetTimer()
	for b.Loop() {
		ids, err := readIndexQueryKeys(c, q)
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
	c := buildBenchCollection(b, benchN)
	q := qx.Query(
		qx.EQ("country", "DE"),
		qx.HASANY("tags", []string{"rust", "go"}),
	).Sort("age", qx.DESC).Limit(50)
	runQueryKeysBench(b, c, q)
}

func Benchmark_Query_Index_Keys_Sort_ArrayPos_Limit(b *testing.B) {
	c := buildBenchCollection(b, benchN)
	priority := []string{"enterprise", "pro", "basic", "free"}
	q := qx.Query(qx.EQ("status", "active")).SortBy(qx.POS("plan", priority), qx.ASC).Limit(50)
	runQueryKeysBench(b, c, q)
}

func Benchmark_Query_Index_Keys_Sort_ArrayPos_All(b *testing.B) {
	c := buildBenchCollection(b, benchN)
	priority := []string{"enterprise", "pro", "basic", "free"}
	q := qx.Query(qx.EQ("status", "active")).SortBy(qx.POS("plan", priority), qx.ASC)
	runQueryKeysBench(b, c, q)
}

func Benchmark_Query_Index_Keys_Sort_ArrayCount_Limit(b *testing.B) {
	c := buildBenchCollection(b, benchN)
	q := qx.Query(qx.EQ("status", "active")).SortBy(qx.LEN("roles"), qx.DESC).Limit(50)
	runQueryKeysBench(b, c, q)
}

func Benchmark_Query_Index_Keys_Sort_ArrayCount_All(b *testing.B) {
	c := buildBenchCollection(b, benchN)
	q := qx.Query(qx.EQ("status", "active")).SortBy(qx.LEN("roles"), qx.DESC)
	runQueryKeysBench(b, c, q)
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

func runQueryKeysBench(b *testing.B, c *Collection[uint64, UserBench], q *qx.QX) {
	runQueryKeysBenchWithMode(b, c, q, benchCacheModes[0])
}

func runQueryKeysBenchWithMode(b *testing.B, c *Collection[uint64, UserBench], q *qx.QX, mode benchCacheMode) {
	b.Helper()
	b.ReportAllocs()
	state := prepareReadBenchWithMode(
		b,
		c,
		q,
		mode,
		warmBenchQueryKeysOnceUint64,
		runBenchQueryKeysOnceUint64,
		buildUserBenchTurnoverRingUint64,
	)
	for b.Loop() {
		state.beforeQuery(b, c)
		runBenchQueryKeysOnceUint64(b, c, q)
	}
}

func runCountBench(b *testing.B, c *Collection[uint64, UserBench], q *qx.QX) {
	runCountBenchWithMode(b, c, q, benchCacheModes[0])
}

func runCountBenchWithMode(b *testing.B, c *Collection[uint64, UserBench], q *qx.QX, mode benchCacheMode) {
	b.Helper()
	b.ReportAllocs()
	state := prepareReadBenchWithMode(
		b,
		c,
		q,
		mode,
		warmBenchCountOnceUint64,
		runBenchCountOnceUint64,
		buildUserBenchTurnoverRingUint64,
	)
	for b.Loop() {
		state.beforeQuery(b, c)
		runBenchCountOnceUint64(b, c, q)
	}
}

func runReadQueryBench(b *testing.B, c *Collection[uint64, UserBench], q *qx.QX) {
	runReadQueryBenchWithMode(b, c, q, benchCacheModes[0])
}

func runReadQueryBenchWithMode(b *testing.B, c *Collection[uint64, UserBench], q *qx.QX, mode benchCacheMode) {
	b.Helper()
	b.ReportAllocs()
	state := prepareReadBenchWithMode(
		b,
		c,
		q,
		mode,
		warmBenchReadQueryOnceUint64,
		runBenchReadQueryOnceUint64,
		buildUserBenchTurnoverRingUint64,
	)
	for b.Loop() {
		state.beforeQuery(b, c)
		runBenchReadQueryOnceUint64(b, c, q)
	}
}

func sortedIDs(in []uint64) []uint64 {
	out := append([]uint64(nil), in...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func Benchmark_Query_Index_Keys_UniqueEQ(b *testing.B) {
	c := openBenchUint64CollectionUnique(b, 20_000)
	q := qx.Query(qx.EQ("email", "u010000@example.com"))

	b.ReportAllocs()
	if _, err := readIndexQueryKeys(c, q); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for b.Loop() {
		ids, err := readIndexQueryKeys(c, q)
		if err != nil {
			b.Fatal(err)
		}
		if len(ids) != 1 {
			b.Fatalf("expected 1 id, got %d", len(ids))
		}
	}
}

func openBenchUint64CollectionUnique(b *testing.B, n int) *Collection[uint64, UniqueTestRec] {
	b.Helper()
	dir := b.TempDir()
	path := filepath.Join(dir, "bench_unique.db")

	c, bolt := openBoltAndCollection[uint64, UniqueTestRec](b, path)
	c.disableSync()
	b.Cleanup(func() {
		_ = c.Close()
		_ = bolt.Close()
	})

	ids := make([]uint64, 0, 1000)
	vals := make([]*UniqueTestRec, 0, 1000)
	flush := func() {
		if len(ids) == 0 {
			return
		}
		if err := writeSets(c, ids, vals); err != nil {
			b.Fatalf("MultiSet: %v", err)
		}
		ids = ids[:0]
		vals = vals[:0]
	}

	for i := 1; i <= n; i++ {
		ids = append(ids, uint64(i))
		vals = append(vals, &UniqueTestRec{
			Email: fmt.Sprintf("u%06d@example.com", i),
			Code:  i,
		})
		if len(ids) == cap(ids) {
			flush()
		}
	}
	flush()
	return c
}

type dynamicBenchDistribution int

const (
	dynamicBenchUniform dynamicBenchDistribution = iota
	dynamicBenchZipfLike
)

type dynamicBenchScoreMode int

const (
	dynamicBenchScoreUniformBuckets dynamicBenchScoreMode = iota
	dynamicBenchScoreZipfBuckets
	dynamicBenchScoreHighCard
	dynamicBenchScoreLowCard
)

type dynamicBenchProfile struct {
	name         string
	n            int
	seed         int64
	distribution dynamicBenchDistribution
	scoreMode    dynamicBenchScoreMode
	scoreBuckets uint64
}

type dynamicBenchQueryCase struct {
	name  string
	query func() *qx.QX
}

var dynamicBenchProfiles = []dynamicBenchProfile{
	{
		name:         "Uniform",
		n:            benchN,
		seed:         11,
		distribution: dynamicBenchUniform,
		scoreMode:    dynamicBenchScoreUniformBuckets,
		scoreBuckets: 16_384,
	},
	{
		name:         "Zipf",
		n:            benchN,
		seed:         37,
		distribution: dynamicBenchZipfLike,
		scoreMode:    dynamicBenchScoreZipfBuckets,
		scoreBuckets: 4_096,
	},
	{
		name:         "HighCardOrder",
		n:            benchN,
		seed:         73,
		distribution: dynamicBenchUniform,
		scoreMode:    dynamicBenchScoreHighCard,
		scoreBuckets: 0,
	},
	{
		name:         "LowCardOrder",
		n:            benchN,
		seed:         101,
		distribution: dynamicBenchUniform,
		scoreMode:    dynamicBenchScoreLowCard,
		scoreBuckets: 16,
	},
}

var dynamicBenchQueries = []dynamicBenchQueryCase{
	{
		name: "Analytics_Range_Order_Limit",
		query: func() *qx.QX {
			return qx.Query(
				qx.GTE("age", 24),
				qx.LTE("age", 46),
				qx.GT("score", 4.0),
			).Sort("score", qx.DESC).Limit(100)
		},
	},
	{
		name: "Autocomplete_Complex_Limit",
		query: func() *qx.QX {
			return qx.Query(
				qx.PREFIX("email", "user10"),
				qx.EQ("status", "active"),
				qx.NOTIN("plan", []string{"free"}),
			).Sort("score", qx.DESC).Limit(80)
		},
	},
	{
		name: "ComplexSegment_Limit",
		query: func() *qx.QX {
			return qx.Query(
				qx.EQ("status", "active"),
				qx.IN("country", []string{"US", "DE", "NL", "PL"}),
				qx.NE("plan", "free"),
				qx.HASANY("tags", []string{"go", "ops"}),
				qx.GTE("age", 22),
			).Limit(120)
		},
	},
	{
		name: "Sort_DeepOffset_Limit",
		query: func() *qx.QX {
			return qx.Query(
				qx.GTE("age", 18),
			).Sort("score", qx.DESC).Offset(5_000).Limit(100)
		},
	},
	{
		name: "OR_Faceted_Order_Offset_Limit",
		query: func() *qx.QX {
			return qx.Query(
				qx.OR(
					qx.AND(
						qx.EQ("status", "active"),
						qx.IN("country", []string{"US", "DE", "NL", "PL"}),
						qx.HASANY("tags", []string{"go", "ops"}),
						qx.GTE("score", 6.0),
					),
					qx.AND(
						qx.EQ("status", "trial"),
						qx.NOTIN("plan", []string{"free"}),
						qx.GTE("age", 25),
						qx.LTE("age", 40),
					),
					qx.AND(
						qx.HASANY("roles", []string{"admin", "moderator"}),
						qx.GTE("score", 8.0),
					),
				),
			).Sort("score", qx.DESC).Offset(500).Limit(100)
		},
	},
}

var (
	dynamicBenchMu          sync.Mutex
	dynamicBenchCollections = make(map[string]*cachedDynamicBenchDB)
)

func openDynamicBenchCollection(b *testing.B) (*Collection[uint64, UserBench], *bbolt.DB, string) {
	b.Helper()
	dir, err := os.MkdirTemp("", "rbi-bench-dynamic-*")
	if err != nil {
		b.Fatalf("os.MkdirTemp: %v", err)
	}
	path := filepath.Join(dir, "dynamic_bench.db")

	opts := benchOptions()
	opts.AnalyzeInterval = -1
	c, bolt := openBoltAndCollection[uint64, UserBench](b, path, opts)
	return c, bolt, path
}

type cachedDynamicBenchDB struct {
	clc  *Collection[uint64, UserBench]
	raw  *bbolt.DB
	path string
}

func dynamicBenchProfileCacheKey(profile dynamicBenchProfile) string {
	return fmt.Sprintf(
		"dynamic/%s/%d/%d/%d/%d/%d",
		profile.name,
		profile.n,
		profile.seed,
		profile.distribution,
		profile.scoreMode,
		profile.scoreBuckets,
	)
}

func buildBenchCollectionDynamicProfileWithMode(b *testing.B, profile dynamicBenchProfile, mode benchCacheMode) *Collection[uint64, UserBench] {
	b.Helper()
	dynamicBenchMu.Lock()
	defer dynamicBenchMu.Unlock()

	key := dynamicBenchProfileCacheKey(profile)
	if cached := dynamicBenchCollections[key]; cached != nil && cached.clc != nil && cached.clc.state.Load()&collectionClosed == 0 {
		return cached.clc
	}

	c, bolt, path := openDynamicBenchCollection(b)

	b.StopTimer()
	seedBenchDataDynamicProfile(b, c, profile)
	b.StartTimer()
	dynamicBenchCollections[key] = &cachedDynamicBenchDB{clc: c, raw: bolt, path: path}
	registerBenchSuiteCleanup(func() {
		_ = c.Close()
		_ = bolt.Close()
		_ = os.RemoveAll(filepath.Dir(path))
	})
	return c
}

func pickIndexByDistribution(r *rand.Rand, n int, z *rand.Zipf) int {
	if n <= 1 {
		return 0
	}
	if z == nil {
		return r.IntN(n)
	}
	return int(z.Uint64() % uint64(n))
}

func scoreForDynamicProfile(r *rand.Rand, i int, p dynamicBenchProfile, scoreZipf *rand.Zipf) float64 {
	switch p.scoreMode {
	case dynamicBenchScoreHighCard:
		return float64(i) + r.Float64()*1e-6
	case dynamicBenchScoreLowCard:
		buckets := p.scoreBuckets
		if buckets == 0 {
			buckets = 16
		}
		return float64(r.IntN(int(buckets)))
	case dynamicBenchScoreZipfBuckets:
		if scoreZipf == nil {
			if p.scoreBuckets == 0 {
				return float64(r.IntN(2048))
			}
			return float64(r.IntN(int(p.scoreBuckets)))
		}
		return float64(scoreZipf.Uint64() % max(1, p.scoreBuckets))
	default:
		buckets := p.scoreBuckets
		if buckets == 0 {
			buckets = 4096
		}
		return float64(r.IntN(int(buckets)))
	}
}

func seedBenchDataDynamicProfile(tb testing.TB, c *Collection[uint64, UserBench], profile dynamicBenchProfile) {
	tb.Helper()

	c.disableSync()
	defer c.enableSync()

	r := newRand(profile.seed)

	countries := []string{"US", "NL", "DE", "PL", "SE", "FR", "GB", "ES", "JP", "BR"}
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
		{"go", "ops", "db"},
		{"analytics"},
		{},
	}
	rolesPool := [][]string{
		{"user"},
		{"user", "admin"},
		{"user", "moderator"},
		{"user", "billing"},
		{"user", "support"},
		{"user", "auditor"},
	}

	var (
		countryZipf *rand.Zipf
		planZipf    *rand.Zipf
		statusZipf  *rand.Zipf
		tagZipf     *rand.Zipf
		roleZipf    *rand.Zipf
		scoreZipf   *rand.Zipf
	)
	if profile.distribution == dynamicBenchZipfLike {
		countryZipf = rand.NewZipf(r, 1.28, 1, uint64(len(countries)-1))
		planZipf = rand.NewZipf(r, 1.24, 1, uint64(len(plans)-1))
		statusZipf = rand.NewZipf(r, 1.22, 1, uint64(len(statuses)-1))
		tagZipf = rand.NewZipf(r, 1.24, 1, uint64(len(tagsPool)-1))
		roleZipf = rand.NewZipf(r, 1.21, 1, uint64(len(rolesPool)-1))
		if profile.scoreBuckets > 0 {
			scoreZipf = rand.NewZipf(r, 1.14, 1, profile.scoreBuckets-1)
		}
	}

	ids := make([]uint64, 0, benchBatch)
	vals := make([]*UserBench, 0, benchBatch)

	flush := func() {
		if len(ids) == 0 {
			return
		}
		if err := writeSets(c, ids, vals); err != nil {
			tb.Fatalf("MultiSet(seed dynamic): %v", err)
		}
		ids = ids[:0]
		vals = vals[:0]
	}

	for i := 1; i <= profile.n; i++ {
		id := uint64(i)

		country := countries[pickIndexByDistribution(r, len(countries), countryZipf)]
		plan := plans[pickIndexByDistribution(r, len(plans), planZipf)]
		status := statuses[pickIndexByDistribution(r, len(statuses), statusZipf)]
		tags := append([]string(nil), tagsPool[pickIndexByDistribution(r, len(tagsPool), tagZipf)]...)
		roles := append([]string(nil), rolesPool[pickIndexByDistribution(r, len(rolesPool), roleZipf)]...)

		age := 18 + r.IntN(60)
		score := scoreForDynamicProfile(r, i, profile, scoreZipf)

		name := fmt.Sprintf("user-%d", i)
		email := fmt.Sprintf("user%06d@example.com", i)

		rec := &UserBench{
			Country: country,
			Plan:    plan,
			Status:  status,
			Age:     age,
			Score:   score,
			Name:    name,
			Email:   email,
			Tags:    tags,
			Roles:   roles,
		}

		ids = append(ids, id)
		vals = append(vals, rec)
		if len(ids) == benchBatch {
			flush()
		}
	}
	flush()
}

func Benchmark_Query_Index_Keys_DynamicProfiles_Perf(b *testing.B) {
	runBenchCacheModes(b, func(b *testing.B, mode benchCacheMode) {
		for _, profile := range dynamicBenchProfiles {
			p := profile
			b.Run(p.name, func(b *testing.B) {
				c := buildBenchCollectionDynamicProfileWithMode(b, p, mode)
				for _, qc := range dynamicBenchQueries {
					q := qc
					b.Run(q.name, func(b *testing.B) {
						runQueryKeysBenchWithMode(b, c, q.query(), mode)
					})
				}
			})
		}
	})
}

/**/

type StressBenchUser struct {
	ID         uint64   `db:"id"          rbi:"index"`
	Name       string   `db:"name"        rbi:"index"`
	Email      string   `db:"email"       rbi:"unique"`
	Country    string   `db:"country"     rbi:"index"`
	Plan       string   `db:"plan"        rbi:"index"`
	Status     string   `db:"status"      rbi:"index"`
	Age        int      `db:"age"         rbi:"index"`
	Score      float64  `db:"score"       rbi:"index"`
	IsVerified bool     `db:"is_verified" rbi:"index"`
	CreatedAt  int64    `db:"created_at"  rbi:"index"`
	LastLogin  int64    `db:"last_login"  rbi:"index"`
	Tags       []string `db:"tags"        rbi:"index"`
	Roles      []string `db:"roles"       rbi:"index"`
	Blob       []byte   `db:"-"           rbi:"-"`
}

const (
	benchStressN        = 500_000
	benchStressBaseUnix = int64(1_700_000_000)
)

var (
	benchStressCountries = []string{"US", "CA", "GB", "DE", "FR", "NL", "PL", "SE", "JP", "IN", "BR", "AU"}
	benchStressAllTags   = []string{
		"technology", "programming", "golang", "rust", "linux", "webdev", "frontend", "backend",
		"databases", "ai", "machine-learning", "datascience", "security", "devops", "cloud", "kubernetes",
		"startups", "careeradvice", "productivity", "design", "gaming", "android", "ios", "photography",
		"music", "movies", "books", "history", "science", "space", "economics", "politics",
		"sports", "soccer", "basketball", "fitness", "travel", "food", "finance", "cryptocurrency",
	}
	benchStressAllRoles = []string{"member", "trusted", "moderator", "admin", "staff", "bot"}
)

func openBenchStressCollection(b *testing.B) (*Collection[uint64, StressBenchUser], *bbolt.DB, string) {
	b.Helper()
	dir, err := os.MkdirTemp("", "rbi-bench-stress-*")
	if err != nil {
		b.Fatalf("os.MkdirTemp: %v", err)
	}
	c, bolt := openBoltAndCollection[uint64, StressBenchUser](b, filepath.Join(dir, "bench_stress.db"), benchOptions())
	return c, bolt, dir
}

type cachedBenchStressDB struct {
	clc *Collection[uint64, StressBenchUser]
	raw *bbolt.DB
	dir string
}

var (
	benchStressDBs = make(map[string]*cachedBenchStressDB)
	benchStressMu  sync.Mutex
)

func benchStressDBFamilyKey(n int) string {
	return "stress_uint64/" + strconv.Itoa(n)
}

func buildBenchStressCollectionWithMode(b *testing.B, n int, mode benchCacheMode) *Collection[uint64, StressBenchUser] {
	b.Helper()
	benchStressMu.Lock()
	defer benchStressMu.Unlock()

	key := benchStressDBFamilyKey(n)
	if cached := benchStressDBs[key]; cached != nil && cached.clc != nil && cached.clc.state.Load()&collectionClosed == 0 {
		return cached.clc
	}

	c, bolt, dir := openBenchStressCollection(b)
	b.StopTimer()
	seedBenchStressData(b, c, n)
	b.StartTimer()
	benchStressDBs[key] = &cachedBenchStressDB{clc: c, raw: bolt, dir: dir}
	registerBenchSuiteCleanup(func() {
		_ = c.Close()
		_ = bolt.Close()
		_ = os.RemoveAll(dir)
	})
	return c
}

func seedBenchStressData(tb testing.TB, c *Collection[uint64, StressBenchUser], n int) {
	tb.Helper()

	c.disableSync()
	defer c.enableSync()

	rng := newRand(17)
	namePrefixes := []string{"u_", "user_", "dev_", "mod_", "news_", "anon_"}

	ids := make([]uint64, 0, benchBatch)
	vals := make([]*StressBenchUser, 0, benchBatch)

	flush := func() {
		if len(ids) == 0 {
			return
		}
		if err := writeSets(c, ids, vals); err != nil {
			tb.Fatalf("MultiSet(seed stress): %v", err)
		}
		ids = ids[:0]
		vals = vals[:0]
	}

	for i := 1; i <= n; i++ {
		id := uint64(i)
		createdAt := benchStressSampleCreatedAt(rng)
		lastLogin := benchStressSampleLastLogin(rng)
		rec := &StressBenchUser{
			ID:         id,
			Name:       fmt.Sprintf("%s%x_%d", namePrefixes[rng.IntN(len(namePrefixes))], id, rng.IntN(10_000)),
			Email:      fmt.Sprintf("user%d_%d@example.com", id, rng.IntN(1_000_000)),
			Country:    benchStressCountries[rng.IntN(len(benchStressCountries))],
			Plan:       benchStressWeightedPlan(rng),
			Status:     benchStressWeightedStatus(rng),
			Age:        13 + rng.IntN(58),
			Score:      benchStressSampleScore(rng),
			IsVerified: rng.Float64() < 0.58,
			CreatedAt:  createdAt,
			LastLogin:  lastLogin,
			Tags:       benchStressPickTags(rng),
			Roles:      benchStressPickRoles(rng),
			Blob:       make([]byte, 96+rng.IntN(96)),
		}
		ids = append(ids, id)
		vals = append(vals, rec)
		if len(ids) == benchBatch {
			flush()
		}
	}
	flush()
}

func benchStressSampleCreatedAt(rng *rand.Rand) int64 {
	switch r := rng.Float64(); {
	case r < 0.08:
		return benchStressBaseUnix - int64(rng.IntN(24))*3600
	case r < 0.35:
		return benchStressBaseUnix - int64(rng.IntN(30*24))*3600
	case r < 0.80:
		return benchStressBaseUnix - int64(rng.IntN(365*24))*3600
	default:
		return benchStressBaseUnix - int64(rng.IntN(4*365*24))*3600
	}
}

func benchStressSampleLastLogin(rng *rand.Rand) int64 {
	switch r := rng.Float64(); {
	case r < 0.52:
		return benchStressBaseUnix - int64(rng.IntN(24))*3600
	case r < 0.82:
		return benchStressBaseUnix - int64(rng.IntN(14*24))*3600
	case r < 0.95:
		return benchStressBaseUnix - int64(rng.IntN(90*24))*3600
	default:
		return benchStressBaseUnix - int64(rng.IntN(3*365*24))*3600
	}
}

func benchStressWeightedPlan(rng *rand.Rand) string {
	switch r := rng.Float64(); {
	case r < 0.82:
		return "free"
	case r < 0.93:
		return "starter"
	case r < 0.99:
		return "pro"
	default:
		return "enterprise"
	}
}

func benchStressWeightedStatus(rng *rand.Rand) string {
	switch r := rng.Float64(); {
	case r < 0.72:
		return "active"
	case r < 0.90:
		return "inactive"
	case r < 0.96:
		return "pending"
	case r < 0.99:
		return "suspended"
	default:
		return "banned"
	}
}

func benchStressSampleScore(rng *rand.Rand) float64 {
	base := rng.Float64() * 180
	switch r := rng.Float64(); {
	case r < 0.55:
		base += rng.Float64() * 220
	case r < 0.85:
		base += rng.Float64() * 900
	case r < 0.97:
		base += rng.Float64() * 4500
	default:
		base += rng.Float64() * 22000
	}
	return base
}

func benchStressPickTags(rng *rand.Rand) []string {
	n := 1 + rng.IntN(4)
	if rng.Float64() < 0.08 {
		n = 5
	}
	if n > len(benchStressAllTags) {
		n = len(benchStressAllTags)
	}
	hotSpan := len(benchStressAllTags) / 3
	if hotSpan < 1 {
		hotSpan = 1
	}
	seen := make(map[string]struct{}, n)
	out := make([]string, 0, n)
	for len(out) < n {
		var tag string
		if rng.Float64() < 0.72 {
			tag = benchStressAllTags[rng.IntN(hotSpan)]
		} else {
			tag = benchStressAllTags[rng.IntN(len(benchStressAllTags))]
		}
		if _, ok := seen[tag]; ok {
			continue
		}
		seen[tag] = struct{}{}
		out = append(out, tag)
	}
	return out
}

func benchStressPickRoles(rng *rand.Rand) []string {
	n := 1
	if rng.Float64() < 0.14 {
		n = 2
	}
	if rng.Float64() < 0.02 {
		n = 3
	}
	if n > len(benchStressAllRoles) {
		n = len(benchStressAllRoles)
	}
	seen := make(map[string]struct{}, n)
	out := make([]string, 0, n)
	for len(out) < n {
		role := benchStressAllRoles[rng.IntN(len(benchStressAllRoles))]
		if _, ok := seen[role]; ok {
			continue
		}
		seen[role] = struct{}{}
		out = append(out, role)
	}
	return out
}

func warmBenchCountOnceStress(b *testing.B, c *Collection[uint64, StressBenchUser], q *qx.QX) {
	b.Helper()
	b.StopTimer()
	defer b.StartTimer()
	runBenchCountOnceStress(b, c, q)
}

func runBenchCountOnceStress(b *testing.B, c *Collection[uint64, StressBenchUser], q *qx.QX) {
	b.Helper()
	if _, err := readCount(c, q.Filter); err != nil {
		b.Fatal(err)
	}
}

func warmBenchQueryKeysOnceStress(b *testing.B, c *Collection[uint64, StressBenchUser], q *qx.QX) {
	b.Helper()
	b.StopTimer()
	defer b.StartTimer()
	runBenchQueryKeysOnceStress(b, c, q)
}

func runBenchQueryKeysOnceStress(b *testing.B, c *Collection[uint64, StressBenchUser], q *qx.QX) {
	b.Helper()
	if _, err := readIndexQueryKeys(c, q); err != nil {
		b.Fatal(err)
	}
}

func runStressCountBenchWithMode(b *testing.B, c *Collection[uint64, StressBenchUser], q *qx.QX, mode benchCacheMode) {
	b.Helper()
	b.ReportAllocs()
	state := prepareReadBenchWithMode(
		b,
		c,
		q,
		mode,
		warmBenchCountOnceStress,
		runBenchCountOnceStress,
		buildStressBenchTurnoverRing,
	)
	for b.Loop() {
		state.beforeQuery(b, c)
		runBenchCountOnceStress(b, c, q)
	}
}

func runStressQueryKeysBenchWithMode(b *testing.B, c *Collection[uint64, StressBenchUser], q *qx.QX, mode benchCacheMode) {
	b.Helper()
	b.ReportAllocs()
	state := prepareReadBenchWithMode(
		b,
		c,
		q,
		mode,
		warmBenchQueryKeysOnceStress,
		runBenchQueryKeysOnceStress,
		buildStressBenchTurnoverRing,
	)
	for b.Loop() {
		state.beforeQuery(b, c)
		runBenchQueryKeysOnceStress(b, c, q)
	}
}

func runStressCountBenchCacheModes(b *testing.B, qf func() *qx.QX) {
	b.Helper()
	runBenchCacheModes(b, func(b *testing.B, mode benchCacheMode) {
		c := buildBenchStressCollectionWithMode(b, benchStressN, mode)
		runStressCountBenchWithMode(b, c, qf(), mode)
	})
}

func runStressQueryKeysBenchCacheModes(b *testing.B, qf func() *qx.QX) {
	b.Helper()
	runBenchCacheModes(b, func(b *testing.B, mode benchCacheMode) {
		c := buildBenchStressCollectionWithMode(b, benchStressN, mode)
		runStressQueryKeysBenchWithMode(b, c, qf(), mode)
	})
}

func Benchmark_Query_Index_Keys_Stress_MemberDirectoryPrefix(b *testing.B) {
	runStressQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.PREFIX("name", "user_"),
			qx.EQ("status", "active"),
		).Sort("name", qx.ASC).Limit(12)
	})
}

func Benchmark_Query_Index_Keys_Stress_RecentCountryActive(b *testing.B) {
	runStressQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.EQ("country", "DE"),
			qx.EQ("status", "active"),
		).Sort("last_login", qx.DESC).Limit(20)
	})
}

func Benchmark_Query_Index_Keys_Stress_ActiveRegionPro(b *testing.B) {
	runStressQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.EQ("status", "active"),
			qx.GTE("last_login", benchStressBaseUnix-72*3600),
			qx.IN("country", []string{"DE", "FR"}),
			qx.NOTIN("plan", []string{"free"}),
		).Sort("last_login", qx.DESC).Limit(40)
	})
}

func Benchmark_Query_Index_Keys_Stress_LeaderboardTop(b *testing.B) {
	runStressQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.EQ("status", "active"),
			qx.GTE("score", 250.0),
			qx.GTE("last_login", benchStressBaseUnix-180*24*3600),
		).Sort("score", qx.DESC).Limit(50)
	})
}

func Benchmark_Query_Index_Count_Stress_SignupDashboard(b *testing.B) {
	runStressCountBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.GTE("created_at", benchStressBaseUnix-21*24*3600),
			qx.EQ("is_verified", true),
			qx.IN("country", []string{"DE", "FR"}),
			qx.NOTIN("status", []string{"banned"}),
		)
	})
}

func Benchmark_Query_Index_Keys_Stress_FrontpageCandidate(b *testing.B) {
	runStressQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.EQ("status", "active"),
			qx.HASANY("tags", []string{"technology", "programming", "golang"}),
			qx.GTE("score", 120.0),
			qx.GTE("last_login", benchStressBaseUnix-45*24*3600),
		).Sort("score", qx.DESC).Limit(100)
	})
}

func Benchmark_Query_Index_Keys_Stress_ModerationQueue(b *testing.B) {
	runStressQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.OR(
				qx.EQ("status", "pending"),
				qx.EQ("status", "suspended"),
				qx.AND(
					qx.EQ("status", "active"),
					qx.HASANY("tags", []string{"politics", "cryptocurrency"}),
					qx.GTE("score", 4000.0),
					qx.GTE("created_at", benchStressBaseUnix-180*24*3600),
				),
			),
		).Sort("created_at", qx.DESC).Limit(100)
	})
}

func Benchmark_Query_Index_Keys_Stress_InactiveCleanup(b *testing.B) {
	runStressQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.LT("last_login", benchStressBaseUnix-365*24*3600),
			qx.NOTIN("status", []string{"banned"}),
			qx.NOTIN("plan", []string{"pro", "enterprise"}),
			qx.LT("score", 120.0),
		).Limit(250)
	})
}

func Benchmark_Query_Index_Keys_Stress_StaffAuditFeed(b *testing.B) {
	runStressQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.HASANY("roles", []string{"moderator", "admin", "staff", "bot"}),
			qx.GTE("last_login", benchStressBaseUnix-120*24*3600),
			qx.NOTIN("status", []string{"banned"}),
			qx.EQ("country", "DE"),
		).Sort("last_login", qx.DESC).Limit(120)
	})
}

func Benchmark_Query_Index_Keys_Stress_DiscoveryExplore(b *testing.B) {
	runStressQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.OR(
				qx.AND(
					qx.EQ("status", "active"),
					qx.HASANY("tags", []string{"technology", "gaming"}),
					qx.GTE("score", 320.0),
					qx.GTE("last_login", benchStressBaseUnix-30*24*3600),
				),
				qx.AND(
					qx.EQ("country", "DE"),
					qx.EQ("is_verified", true),
					qx.GTE("created_at", benchStressBaseUnix-180*24*3600),
					qx.GTE("score", 180.0),
				),
				qx.AND(
					qx.IN("plan", []string{"pro", "enterprise"}),
					qx.HASANY("roles", []string{"moderator", "admin", "staff"}),
					qx.GTE("last_login", benchStressBaseUnix-14*24*3600),
				),
			),
		).Sort("created_at", qx.DESC).Limit(150)
	})
}

func Benchmark_Query_Index_Keys_Stress_DormantArchivePage(b *testing.B) {
	runStressQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.NOTIN("status", []string{"banned"}),
			qx.LT("last_login", benchStressBaseUnix-180*24*3600),
			qx.NOTIN("plan", []string{"enterprise"}),
			qx.LT("score", 180.0),
		).Sort("last_login", qx.ASC).Offset(2500).Limit(100)
	})
}
