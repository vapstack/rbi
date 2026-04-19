package rbi

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/vapstack/qx"
	"go.etcd.io/bbolt"
)

const dynamicBenchN = 180_000

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
		n:            dynamicBenchN,
		seed:         11,
		distribution: dynamicBenchUniform,
		scoreMode:    dynamicBenchScoreUniformBuckets,
		scoreBuckets: 16_384,
	},
	{
		name:         "Zipf",
		n:            dynamicBenchN,
		seed:         37,
		distribution: dynamicBenchZipfLike,
		scoreMode:    dynamicBenchScoreZipfBuckets,
		scoreBuckets: 4_096,
	},
	{
		name:         "HighCardOrder",
		n:            dynamicBenchN,
		seed:         73,
		distribution: dynamicBenchUniform,
		scoreMode:    dynamicBenchScoreHighCard,
		scoreBuckets: 0,
	},
	{
		name:         "LowCardOrder",
		n:            dynamicBenchN,
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
	dynamicBenchMu  sync.Mutex
	dynamicBenchDBs = make(map[string]*cachedDynamicBenchDB)
)

func openDynamicBenchDB(b *testing.B) (*DB[uint64, UserBench], *bbolt.DB, string) {
	b.Helper()
	dir, err := os.MkdirTemp("", "rbi-bench-dynamic-*")
	if err != nil {
		b.Fatalf("os.MkdirTemp: %v", err)
	}
	path := filepath.Join(dir, "dynamic_bench.db")

	opts := benchOptions()
	opts.AnalyzeInterval = -1
	db, raw := openBoltAndNew[uint64, UserBench](b, path, opts)
	return db, raw, path
}

type cachedDynamicBenchDB struct {
	db   *DB[uint64, UserBench]
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

func buildBenchDBDynamicProfileWithMode(b *testing.B, profile dynamicBenchProfile, mode benchCacheMode) *DB[uint64, UserBench] {
	b.Helper()
	dynamicBenchMu.Lock()
	defer dynamicBenchMu.Unlock()

	key := dynamicBenchProfileCacheKey(profile)
	if cached := dynamicBenchDBs[key]; cached != nil && cached.db != nil && !cached.db.closed.Load() {
		return cached.db
	}

	db, raw, path := openDynamicBenchDB(b)

	b.StopTimer()
	seedBenchDataDynamicProfile(b, db, profile)
	b.StartTimer()
	dynamicBenchDBs[key] = &cachedDynamicBenchDB{db: db, raw: raw, path: path}
	registerBenchSuiteCleanup(func() {
		_ = db.Close()
		_ = raw.Close()
		_ = os.RemoveAll(filepath.Dir(path))
	})
	return db
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

func seedBenchDataDynamicProfile(tb testing.TB, db *DB[uint64, UserBench], profile dynamicBenchProfile) {
	tb.Helper()

	db.DisableSync()
	defer db.EnableSync()

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
		if err := db.BatchSet(ids, vals); err != nil {
			tb.Fatalf("BatchSet(seed dynamic): %v", err)
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
			profile := profile
			b.Run(profile.name, func(b *testing.B) {
				db := buildBenchDBDynamicProfileWithMode(b, profile, mode)
				for _, qc := range dynamicBenchQueries {
					qc := qc
					b.Run(qc.name, func(b *testing.B) {
						runQueryKeysBenchWithMode(b, db, qc.query(), mode)
					})
				}
			})
		}
	})
}
