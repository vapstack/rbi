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
	benchN          = 200_000
	benchBatchSmall = 128
	benchBatchBig   = 1024
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
	// b.Cleanup(func() { _ = db.Close() })

	// db.DisableSync()

	return db, path
}

func seedBenchData(b *testing.B, db *DB[uint64, UserBench], n int) {
	b.Helper()

	db.DisableIndexing()
	db.DisableSync()

	defer func() {
		db.EnableIndexing()
		db.EnableSync()
		_ = db.RebuildIndex()
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

	ids := make([]uint64, 0, benchBatchBig)
	vals := make([]*UserBench, 0, benchBatchBig)

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
		if len(ids) == benchBatchBig {
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
	b.StartTimer()
	oneDB = db
	return db
}

func BenchmarkQueryKeys_SimpleEQ(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	q := &qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "country", Value: "NL"}}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Count(q) // index performance only
		if err != nil {
			b.Fatal(err)
		}
	}
}

// func BenchmarkQueryItems_SimpleEQ(b *testing.B) {
// 	db := buildBenchDB(b, benchN)
// 	b.ReportAllocs()
//
// 	q := &qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "plan", Value: "pro"}}
//
// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		_, err := db.QueryItems(q)
// 		if err != nil {
// 			b.Fatal(err)
// 		}
// 	}
// }

func BenchmarkQueryKeys_MediumAND(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	// q := &qx.QX{
	// 	Expr: qx.Expr{
	// 		Op: qx.OpAND,
	// 		Operands: []qx.Expr{
	// 			{Op: qx.OpEQ, Field: "status", Value: "active"},
	// 			{Op: qx.OpGTE, Field: "age", Value: 30},
	// 			{Op: qx.OpLT, Field: "age", Value: 50},
	// 		},
	// 	},
	// }
	q := qx.Query(
		qx.EQ("status", "active"),
		qx.GTE("age", 30),
		qx.LT("age", 50),
	).By("age", qx.ASC).Max(100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// _, err := db.QueryKeys(q)
		_, err := db.Count(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_ComplexExpr(b *testing.B) {
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
	// q := &qx.QX{
	// 	Expr: qx.Expr{
	// 		Op: qx.OpOR,
	// 		Operands: []qx.Expr{
	// 			{
	// 				Op: qx.OpAND,
	// 				Operands: []qx.Expr{
	// 					{Op: qx.OpEQ, Field: "country", Value: "DE"},
	// 					{Op: qx.OpEQ, Field: "plan", Value: "enterprise"},
	// 					{Op: qx.OpHASANY, Field: "tags", Value: []string{"go", "security", "ops"}},
	// 				},
	// 			},
	// 			{
	// 				Op: qx.OpAND,
	// 				Operands: []qx.Expr{
	// 					{Op: qx.OpPREFIX, Field: "email", Value: "user1"},
	// 					{Op: qx.OpLT, Field: "age", Value: 25},
	// 				},
	// 			},
	// 			{Op: qx.OpHASNONE, Field: "roles", Value: []string{"admin"}},
	// 		},
	// 	},
	// }

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// _, err := db.QueryKeys(q)
		_, err := db.Count(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_OffsetLimit(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	q := &qx.QX{
		Expr:   qx.Expr{Op: qx.OpEQ, Field: "status", Value: "active"},
		Offset: 500,
		Limit:  50,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// _, err := db.QueryKeys(q)
		_, err := db.Count(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_OrderBasic(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	q := &qx.QX{
		Expr:  qx.Expr{Op: qx.OpEQ, Field: "country", Value: "NL"},
		Order: []qx.Order{{Type: qx.OrderBasic, Field: "age", Desc: true}},
		Limit: 200,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// _, err := db.QueryKeys(q)
		_, err := db.Count(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_OrderArrayCount(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	q := &qx.QX{
		Expr:  qx.Expr{Op: qx.OpNOOP},
		Order: []qx.Order{{Type: qx.OrderByArrayCount, Field: "tags", Desc: true}},
		Limit: 500,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// _, err := db.QueryKeys(q)
		_, err := db.Count(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryKeys_OrderArrayPos(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	q := &qx.QX{
		Expr:  qx.Expr{Op: qx.OpNOOP},
		Order: []qx.Order{{Type: qx.OrderByArrayPos, Field: "tags", Data: []string{"go", "ops", "security"}, Desc: false}},
		Limit: 500,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// _, err := db.QueryKeys(q)
		_, err := db.Count(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}

/*
func BenchmarkSet_UpdateSingle(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	r := rand.New(rand.NewSource(2))
	now := time.Now().UnixNano()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := uint64(1 + r.Intn(benchN))
		rec := &UserBench{
			Country: []string{"US", "NL", "DE", "PL"}[r.Intn(4)],
			Plan:    []string{"free", "basic", "pro"}[r.Intn(3)],
			Status:  []string{"active", "trial", "paused"}[r.Intn(3)],
			Age:     18 + r.Intn(60),
			Score:   float64((now + int64(i)) % 10_000),
			Name:    fmt.Sprintf("user-%d", id),
			Email:   fmt.Sprintf("user%06d@example.com", id),
			Tags:    []string{"go", "db"}[:1+r.Intn(2)],
			Roles:   []string{"user"},
		}
		if err := db.Set(id, rec); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSetMany_UpdateBatch128(b *testing.B) {
	db := buildBenchDB(b, benchN)
	b.ReportAllocs()

	r := rand.New(rand.NewSource(3))

	ids := make([]uint64, benchBatchSmall)
	vals := make([]*UserBench, benchBatchSmall)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for k := 0; k < benchBatchSmall; k++ {
			id := uint64(1 + r.Intn(benchN))
			ids[k] = id
			vals[k] = &UserBench{
				Country: []string{"US", "NL", "DE", "PL"}[r.Intn(4)],
				Plan:    []string{"free", "basic", "pro"}[r.Intn(3)],
				Status:  []string{"active", "trial", "paused"}[r.Intn(3)],
				Age:     18 + r.Intn(60),
				Score:   r.Float64() * 1000,
				Name:    fmt.Sprintf("user-%d", id),
				Email:   fmt.Sprintf("user%06d@example.com", id),
				Tags:    []string{"go", "ops", "security"}[:1+r.Intn(3)],
				Roles:   []string{"user", "admin"}[:1+r.Intn(2)],
			}
		}
		if err := db.SetMany(ids, vals); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSetMany_InsertBatch1024(b *testing.B) {
	b.ReportAllocs()

	b.StopTimer()
	db, _ := openBenchDB(b)
	nextID := uint64(1)
	r := rand.New(rand.NewSource(4))
	b.StartTimer()

	ids := make([]uint64, benchBatchBig)
	vals := make([]*UserBench, benchBatchBig)

	for i := 0; i < b.N; i++ {
		for k := 0; k < benchBatchBig; k++ {
			id := nextID
			nextID++

			ids[k] = id
			vals[k] = &UserBench{
				Country: []string{"US", "NL", "DE", "PL"}[r.Intn(4)],
				Plan:    []string{"free", "basic", "pro", "enterprise"}[r.Intn(4)],
				Status:  []string{"active", "trial", "paused", "banned"}[r.Intn(4)],
				Age:     18 + r.Intn(60),
				Score:   math.Round((r.Float64()*1000.0)*100) / 100,
				Name:    fmt.Sprintf("user-%d", id),
				Email:   fmt.Sprintf("user%06d@example.com", id),
				Tags:    []string{"go", "db"}[:1+r.Intn(2)],
				Roles:   []string{"user"},
			}
		}
		if err := db.SetMany(ids, vals); err != nil {
			b.Fatal(err)
		}
	}
}
*/
