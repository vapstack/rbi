package rbi

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/vapstack/qx"
	"go.etcd.io/bbolt"
)

type StressBenchUser struct {
	ID         uint64   `db:"id"          dbi:"default"`
	Name       string   `db:"name"        dbi:"default"`
	Email      string   `db:"email"       dbi:"unique"`
	Country    string   `db:"country"     dbi:"default"`
	Plan       string   `db:"plan"        dbi:"default"`
	Status     string   `db:"status"      dbi:"default"`
	Age        int      `db:"age"         dbi:"default"`
	Score      float64  `db:"score"       dbi:"default"`
	IsVerified bool     `db:"is_verified" dbi:"default"`
	CreatedAt  int64    `db:"created_at"  dbi:"default"`
	LastLogin  int64    `db:"last_login"  dbi:"default"`
	Tags       []string `db:"tags"        dbi:"default"`
	Roles      []string `db:"roles"       dbi:"default"`
	Blob       []byte   `db:"-"           dbi:"-"`
}

const (
	benchStressN        = 500_000
	benchStressBatch    = 20_000
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

func openBenchStressDB(b *testing.B) (*DB[uint64, StressBenchUser], *bbolt.DB, string) {
	b.Helper()
	dir, err := os.MkdirTemp("", "rbi-bench-stress-*")
	if err != nil {
		b.Fatalf("os.MkdirTemp: %v", err)
	}
	db, raw := openBoltAndNew[uint64, StressBenchUser](b, filepath.Join(dir, "bench_stress.db"), benchOptions())
	return db, raw, dir
}

type cachedBenchStressDB struct {
	db  *DB[uint64, StressBenchUser]
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

func buildBenchStressDBWithMode(b *testing.B, n int, mode benchCacheMode) *DB[uint64, StressBenchUser] {
	b.Helper()
	benchStressMu.Lock()
	defer benchStressMu.Unlock()

	key := benchStressDBFamilyKey(n)
	if cached := benchStressDBs[key]; cached != nil && cached.db != nil && !cached.db.closed.Load() {
		return cached.db
	}

	db, raw, dir := openBenchStressDB(b)
	b.StopTimer()
	seedBenchStressData(b, db, n)
	b.StartTimer()
	benchStressDBs[key] = &cachedBenchStressDB{db: db, raw: raw, dir: dir}
	registerBenchSuiteCleanup(func() {
		_ = db.Close()
		_ = raw.Close()
		_ = os.RemoveAll(dir)
	})
	return db
}

func seedBenchStressData(tb testing.TB, db *DB[uint64, StressBenchUser], n int) {
	tb.Helper()

	db.DisableSync()
	defer db.EnableSync()

	rng := newRand(17)
	namePrefixes := []string{"u_", "user_", "dev_", "mod_", "news_", "anon_"}

	ids := make([]uint64, 0, benchStressBatch)
	vals := make([]*StressBenchUser, 0, benchStressBatch)

	flush := func() {
		if len(ids) == 0 {
			return
		}
		if err := db.BatchSet(ids, vals); err != nil {
			tb.Fatalf("BatchSet(seed stress): %v", err)
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
		if len(ids) == benchStressBatch {
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

func warmBenchCountOnceStress(b *testing.B, db *DB[uint64, StressBenchUser], q *qx.QX) {
	b.Helper()
	b.StopTimer()
	defer b.StartTimer()
	runBenchCountOnceStress(b, db, q)
}

func runBenchCountOnceStress(b *testing.B, db *DB[uint64, StressBenchUser], q *qx.QX) {
	b.Helper()
	if _, err := db.Count(q); err != nil {
		b.Fatal(err)
	}
}

func warmBenchQueryKeysOnceStress(b *testing.B, db *DB[uint64, StressBenchUser], q *qx.QX) {
	b.Helper()
	b.StopTimer()
	defer b.StartTimer()
	runBenchQueryKeysOnceStress(b, db, q)
}

func runBenchQueryKeysOnceStress(b *testing.B, db *DB[uint64, StressBenchUser], q *qx.QX) {
	b.Helper()
	if _, err := db.QueryKeys(q); err != nil {
		b.Fatal(err)
	}
}

func runStressCountBenchWithMode(b *testing.B, db *DB[uint64, StressBenchUser], q *qx.QX, mode benchCacheMode) {
	b.Helper()
	b.ReportAllocs()
	state := prepareReadBenchWithMode(
		b,
		db,
		q,
		mode,
		warmBenchCountOnceStress,
		runBenchCountOnceStress,
		buildStressBenchTurnoverRing,
	)
	for b.Loop() {
		state.beforeQuery(b, db)
		runBenchCountOnceStress(b, db, q)
	}
}

func runStressQueryKeysBenchWithMode(b *testing.B, db *DB[uint64, StressBenchUser], q *qx.QX, mode benchCacheMode) {
	b.Helper()
	b.ReportAllocs()
	state := prepareReadBenchWithMode(
		b,
		db,
		q,
		mode,
		warmBenchQueryKeysOnceStress,
		runBenchQueryKeysOnceStress,
		buildStressBenchTurnoverRing,
	)
	for b.Loop() {
		state.beforeQuery(b, db)
		runBenchQueryKeysOnceStress(b, db, q)
	}
}

func runStressCountBenchCacheModes(b *testing.B, qf func() *qx.QX) {
	b.Helper()
	runBenchCacheModes(b, func(b *testing.B, mode benchCacheMode) {
		db := buildBenchStressDBWithMode(b, benchStressN, mode)
		runStressCountBenchWithMode(b, db, qf(), mode)
	})
}

func runStressQueryKeysBenchCacheModes(b *testing.B, qf func() *qx.QX) {
	b.Helper()
	runBenchCacheModes(b, func(b *testing.B, mode benchCacheMode) {
		db := buildBenchStressDBWithMode(b, benchStressN, mode)
		runStressQueryKeysBenchWithMode(b, db, qf(), mode)
	})
}

func Benchmark_Query_Index_Keys_Stress_MemberDirectoryPrefix(b *testing.B) {
	runStressQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.PREFIX("name", "user_"),
			qx.EQ("status", "active"),
		).By("name", qx.ASC).Max(12)
	})
}

func Benchmark_Query_Index_Keys_Stress_RecentCountryActive(b *testing.B) {
	runStressQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.EQ("country", "DE"),
			qx.EQ("status", "active"),
		).By("last_login", qx.DESC).Max(20)
	})
}

func Benchmark_Query_Index_Keys_Stress_ActiveRegionPro(b *testing.B) {
	runStressQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.EQ("status", "active"),
			qx.GTE("last_login", benchStressBaseUnix-72*3600),
			qx.IN("country", []string{"DE", "FR"}),
			qx.NOTIN("plan", []string{"free"}),
		).By("last_login", qx.DESC).Max(40)
	})
}

func Benchmark_Query_Index_Keys_Stress_LeaderboardTop(b *testing.B) {
	runStressQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.EQ("status", "active"),
			qx.GTE("score", 250.0),
			qx.GTE("last_login", benchStressBaseUnix-180*24*3600),
		).By("score", qx.DESC).Max(50)
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
		).By("score", qx.DESC).Max(100)
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
		).By("created_at", qx.DESC).Max(100)
	})
}

func Benchmark_Query_Index_Keys_Stress_InactiveCleanup(b *testing.B) {
	runStressQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.LT("last_login", benchStressBaseUnix-365*24*3600),
			qx.NOTIN("status", []string{"banned"}),
			qx.NOTIN("plan", []string{"pro", "enterprise"}),
			qx.LT("score", 120.0),
		).Max(250)
	})
}

func Benchmark_Query_Index_Keys_Stress_StaffAuditFeed(b *testing.B) {
	runStressQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.HASANY("roles", []string{"moderator", "admin", "staff", "bot"}),
			qx.GTE("last_login", benchStressBaseUnix-120*24*3600),
			qx.NOTIN("status", []string{"banned"}),
			qx.EQ("country", "DE"),
		).By("last_login", qx.DESC).Max(120)
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
		).By("created_at", qx.DESC).Max(150)
	})
}

func Benchmark_Query_Index_Keys_Stress_DormantArchivePage(b *testing.B) {
	runStressQueryKeysBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.NOTIN("status", []string{"banned"}),
			qx.LT("last_login", benchStressBaseUnix-180*24*3600),
			qx.NOTIN("plan", []string{"enterprise"}),
			qx.LT("score", 180.0),
		).By("last_login", qx.ASC).Skip(2500).Max(100)
	})
}
