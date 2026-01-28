package rbi

import (
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/vapstack/qx"
)

type UserBench struct {
	Country string   `db:"country"`
	Plan    string   `db:"plan"`
	Status  string   `db:"status"`
	Age     int      `db:"age"`
	Score   float64  `db:"score"`
	Name    string   `db:"name"`
	Email   string   `db:"email"`
	Tags    []string `db:"tags"`
	Roles   []string `db:"roles"`
}

const (
	benchN     = 500_000
	benchBatch = 100_000
)

func openBenchDB(b *testing.B) (*DB[uint64, UserBench], string) {
	b.Helper()
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.db")

	opts := &Options[uint64, UserBench]{
		DisableIndexLoad:    true,
		DisableIndexStore:   true,
		DisableIndexRebuild: true,
	}

	db, err := Open[uint64, UserBench](path, 0o600, opts)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}

	return db, path
}

func seedBenchData(b *testing.B, db *DB[uint64, UserBench], n int) {
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

	r := rand.New(rand.NewSource(1))

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
		if err := db.SetMany(ids, vals); err != nil {
			b.Fatalf("SetMany(seed): %v", err)
		}
		ids = ids[:0]
		vals = vals[:0]
	}

	for i := 1; i <= n; i++ {
		id := uint64(i)

		age := 18 + r.Intn(60)
		score := math.Round((r.Float64()*1000.0)*100) / 100

		name := "user-" + strconv.Itoa(i)
		email := fmt.Sprintf("user%06d@example.com", i)

		rec := &UserBench{
			Country: countries[r.Intn(len(countries))],
			Plan:    plans[r.Intn(len(plans))],
			Status:  statuses[r.Intn(len(statuses))],
			Age:     age,
			Score:   score,
			Name:    name,
			Email:   email,
			Tags:    append([]string(nil), tagsPool[r.Intn(len(tagsPool))]...),
			Roles:   append([]string(nil), rolesPool[r.Intn(len(rolesPool))]...),
		}

		ids = append(ids, id)
		vals = append(vals, rec)
		if len(ids) == benchBatch {
			flush()
		}
	}
	flush()
}

var (
	oneDB *DB[uint64, UserBench]
	oneMu sync.Mutex
)

func buildBenchDB(b *testing.B, n int) *DB[uint64, UserBench] {
	b.Helper()
	oneMu.Lock()
	defer oneMu.Unlock()
	if oneDB != nil {
		return oneDB
	}
	db, _ := openBenchDB(b)

	b.StopTimer()
	seedBenchData(b, db, n)
	_, _ = db.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "country", Value: "NL"}}) // warmup

	// s := db.Stats()
	// b.Logf("total size: %v", s.IndexSize)
	// b.Logf("index size: %v", s.IndexFieldSize)
	// b.Logf("index build rps: %v", s.IndexBuildRPS)
	// b.Logf("index build time: %v", s.IndexBuildTime)
	// b.Logf("key count: %v", s.KeyCount)
	// b.Logf("unique field keys: %v", s.UniqueFieldKeys)

	b.StartTimer()
	oneDB = db
	return db
}

/*
func BenchmarkStats(b *testing.B) {
	db := buildBenchDB(b, benchN)

	var s Stats[uint64]

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		s = db.Stats()
	}
	b.StopTimer()
	b.Log(s.KeyCount)
	b.Log(s.IndexSize)
	b.Log(s.IndexFieldSize)
}
*/

func BenchmarkCount_Simple_EQ_Count(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	q := qx.Query(qx.EQ("country", "NL"))
	b.ResetTimer()
	for b.Loop() {
		_, err := db.Count(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_Simple_First100(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	q := qx.Query().Max(100)
	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkScanKeys_All_Uint64(b *testing.B) {
	db := buildBenchDB(b, benchN)

	var count int
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

func BenchmarkQueryKeys_Medium_IN_Limit(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	q := qx.Query(qx.IN("country", []string{"NL", "DE"})).Max(100)

	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_Heavy_Range_Order_Limit(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	q := qx.Query(
		qx.EQ("status", "active"),
		qx.GTE("age", 30),
		qx.LT("age", 50),
	).By("age", qx.ASC).Max(100)

	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_Heavy_All(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	q := qx.Query(
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
		))

	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_Realistic_DashboardFilter_Limit(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	// SELECT * FROM users WHERE status='active' AND plan='enterprise' AND country='US' LIMIT 100
	q := qx.Query(
		qx.EQ("status", "active"),
		qx.EQ("plan", "enterprise"),
		qx.EQ("country", "US"),
	).Max(100)

	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_Realistic_Analytics_Range_Order_Limit(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	// SELECT * FROM users WHERE age >= 25 AND age <= 40 AND score > 0.5 ORDER BY score DESC LIMIT 100
	q := qx.Query(
		qx.GTE("age", 25),
		qx.LTE("age", 40),
		qx.GT("score", 0.5),
	).By("score", qx.DESC).Max(100)

	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_Realistic_LeaderBoard(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	q := qx.Query().By("score", qx.DESC).Max(10)

	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_Realistic_Permissions_HasAny_Limit(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	// SELECT * FROM users WHERE roles && ['admin', 'moderator']
	q := qx.Query(
		qx.HASANY("roles", []string{"admin", "moderator"}),
	).Max(100)

	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_Realistic_Permissions_HasAny_All(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	// SELECT * FROM users WHERE roles && ['admin', 'moderator']
	q := qx.Query(
		qx.HASANY("roles", []string{"admin", "moderator"}),
	)

	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_Realistic_Skills_HasAll_Limit(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	// SELECT * FROM users WHERE tags @> ['go', 'db']
	q := qx.Query(
		qx.HAS("tags", []string{"go", "db"}),
	).Max(100)

	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_Realistic_Skills_HasAll_All(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	// SELECT * FROM users WHERE tags @> ['go', 'db']
	q := qx.Query(
		qx.HAS("tags", []string{"go", "db"}),
	)

	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_Realistic_Exclusion_Limit(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	// SELECT * FROM users WHERE status = 'active' AND plan != 'free' AND country NOT IN ('US', 'GB')
	q := qx.Query(
		qx.EQ("status", "active"),
		qx.NE("plan", "free"),
		qx.NOTIN("country", []string{"US", "GB"}),
	).Max(100)

	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_Realistic_Exclusion_All(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	// SELECT * FROM users WHERE status = 'active' AND plan != 'free' AND country NOT IN ('US', 'GB')
	q := qx.Query(
		qx.EQ("status", "active"),
		qx.NE("plan", "free"),
		qx.NOTIN("country", []string{"US", "GB"}),
	)

	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_Realistic_Autocomplete_Prefix_Limit(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	q := qx.Query(qx.PREFIX("email", "user10")).Max(10)

	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_Realistic_Autocomplete_Order_Limit(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	q := qx.Query(
		qx.PREFIX("email", "user10"),
		qx.EQ("status", "active"),
	).By("email", qx.ASC).Max(10)

	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_Realistic_Autocomplete_Complex_Limit(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	q := qx.Query(
		qx.PREFIX("email", "user10"),
		qx.EQ("status", "active"),
		qx.NOTIN("plan", []string{"free"}),
	).By("score", qx.DESC).Max(10)

	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_Realistic_ComplexSegment_Limit(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	europe := []string{"NL", "DE", "PL", "SE", "FR", "ES", "GB"}

	q := qx.Query(
		qx.EQ("status", "active"),
		qx.IN("country", europe),
		qx.NE("plan", "free"),
		qx.GTE("age", 20),
		qx.HASANY("tags", []string{"security", "ops"}),
	).Max(100)

	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_Realistic_ComplexSegment_All(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	europe := []string{"NL", "DE", "PL", "SE", "FR", "ES", "GB"}

	q := qx.Query(
		qx.EQ("status", "active"),
		qx.IN("country", europe),
		qx.NE("plan", "free"),
		qx.GTE("age", 20),
		qx.HASANY("tags", []string{"security", "ops"}),
	)

	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_Realistic_TopLevel_OR_Limit(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	q := qx.Query(
		qx.OR(
			qx.HAS("roles", []string{"admin"}),
			qx.EQ("plan", "enterprise"),
		),
	).Max(100)

	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_Realistic_TopLevel_OR_All(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	q := qx.Query(
		qx.OR(
			qx.HAS("roles", []string{"admin"}),
			qx.EQ("plan", "enterprise"),
		),
	)

	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

/**/

func BenchmarkQueryKeys_Sort_EarlyExit(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	q := qx.Query(
		qx.EQ("status", "active"),
	).By("age", qx.ASC).Max(20)

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

func BenchmarkQueryKeys_Sort_DeepOffset_Limit(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	q := qx.Query(
		qx.GTE("age", 18),
	).By("score", qx.DESC).Skip(5000).Max(50)

	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_Sort_Complex_Order_Limit(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	q := qx.Query(
		qx.EQ("country", "DE"),
		qx.HASANY("tags", []string{"rust", "go"}),
	).By("age", qx.DESC).Max(50)

	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_Sort_ArrayPos_Limit(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	priority := []string{"enterprise", "pro", "basic", "free"}

	q := qx.Query(qx.EQ("status", "active")).ByArrayPos("plan", priority, qx.ASC).Max(50)

	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_Sort_ArrayPos_All(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	priority := []string{"enterprise", "pro", "basic", "free"}

	q := qx.Query(qx.EQ("status", "active")).ByArrayPos("plan", priority, qx.ASC)

	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_Sort_ArrayCount_Limit(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	q := qx.Query(qx.EQ("status", "active")).ByArrayCount("roles", qx.DESC).Max(50)

	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_Sort_ArrayCount_All(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	q := qx.Query(qx.EQ("status", "active")).ByArrayCount("roles", qx.DESC)

	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryItems_SimpleFetch(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	q := qx.Query(qx.EQ("country", "US")).By("age", qx.DESC).Max(20)

	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryItems(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryItems_HeavyFetch(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	q := qx.Query(qx.GTE("age", 20)).By("score", qx.DESC).Max(100)

	b.ResetTimer()
	for b.Loop() {
		_, err := db.QueryItems(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryItems_GT_NoMatch(b *testing.B) {
	db := buildBenchDB(b, benchN)

	q := qx.Query(qx.GT("age", 100))

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		if _, err := db.QueryItems(q); err != nil {
			b.Fatalf("QueryItems: %v", err)
		}
	}
}

func BenchmarkMakePatch(b *testing.B) {
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

/**/

func buildWriteBenchDB(b *testing.B) (*DB[uint64, UserBench], uint64) {
	b.Helper()

	dir := b.TempDir()
	path := filepath.Join(dir, "bench_write_seeded.db")

	opts := &Options[uint64, UserBench]{DisableIndexRebuild: true}
	db, err := Open[uint64, UserBench](path, 0o600, opts)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	db.DisableSync()

	seedCount := 200_000

	db.DisableIndexing()

	r := rand.New(rand.NewSource(42))
	countries := []string{"US", "NL", "DE", "PL", "SE", "FR", "GB", "ES"}
	plans := []string{"free", "basic", "pro", "enterprise"}

	ids := make([]uint64, 0, 1000)
	vals := make([]*UserBench, 0, 1000)

	for i := 1; i <= seedCount; i++ {
		rec := &UserBench{
			Country: countries[r.Intn(len(countries))],
			Plan:    plans[r.Intn(len(plans))],
			Age:     18 + r.Intn(60),
			Score:   math.Round(r.Float64()*100000) / 100,
			Name:    "user-" + strconv.Itoa(i),
			Tags:    []string{"go", "seed"},
		}
		ids = append(ids, uint64(i))
		vals = append(vals, rec)

		if len(ids) >= 1000 {
			if err := db.SetMany(ids, vals); err != nil {
				b.Fatalf("seed error: %v", err)
			}
			ids = ids[:0]
			vals = vals[:0]
		}
	}
	if len(ids) > 0 {
		if err = db.SetMany(ids, vals); err != nil {
			b.Fatal(err)
		}
	}

	if err = db.RebuildIndex(); err != nil {
		b.Fatalf("rebuild: %v", err)
	}

	db.EnableIndexing()

	b.Cleanup(func() { _ = db.Close() })

	return db, uint64(seedCount)
}

func BenchmarkWrite_Set_New_Indexed(b *testing.B) {
	db, startOffset := buildWriteBenchDB(b)

	rec := &UserBench{Name: "new", Age: 20, Country: "DE"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		id := startOffset + uint64(i) + 1
		if err := db.Set(id, rec); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWrite_Set_New_NoIndex(b *testing.B) {
	db, startOffset := buildWriteBenchDB(b)
	db.DisableIndexing()

	rec := &UserBench{Name: "new", Age: 20, Country: "DE"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		id := startOffset + uint64(i) + 1
		if err := db.Set(id, rec); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWrite_Update_Indexed(b *testing.B) {
	db, _ := buildWriteBenchDB(b)
	targetID := uint64(1000)

	recA := &UserBench{Name: "A", Age: 20, Country: "US", Tags: []string{"a"}}
	recB := &UserBench{Name: "B", Age: 30, Country: "DE", Tags: []string{"b"}}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			if err := db.Set(targetID, recA); err != nil {
				b.Fatal(err)
			}
		} else {
			if err := db.Set(targetID, recB); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkWrite_Update_NoIndex(b *testing.B) {
	db, _ := buildWriteBenchDB(b)
	db.DisableIndexing()

	targetID := uint64(1000)
	recA := &UserBench{Name: "A", Age: 20, Country: "US", Tags: []string{"a"}}
	recB := &UserBench{Name: "B", Age: 30, Country: "DE", Tags: []string{"b"}}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			if err := db.Set(targetID, recA); err != nil {
				b.Fatal(err)
			}
		} else {
			if err := db.Set(targetID, recB); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkWrite_Patch_Indexed(b *testing.B) {
	db, _ := buildWriteBenchDB(b)
	targetID := uint64(2000)

	patchA := []Field{{Name: "age", Value: 100}}
	patchB := []Field{{Name: "age", Value: 200}}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			if err := db.Patch(targetID, patchA); err != nil {
				b.Fatal(err)
			}
		} else {
			if err := db.Patch(targetID, patchB); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkWrite_Patch_NoIndex(b *testing.B) {
	db, _ := buildWriteBenchDB(b)
	db.DisableIndexing()

	targetID := uint64(2000)
	patchA := []Field{{Name: "age", Value: 100}}
	patchB := []Field{{Name: "age", Value: 200}}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			if err := db.Patch(targetID, patchA); err != nil {
				b.Fatal(err)
			}
		} else {
			if err := db.Patch(targetID, patchB); err != nil {
				b.Fatal(err)
			}
		}
	}
}
