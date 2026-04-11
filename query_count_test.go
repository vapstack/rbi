package rbi

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sync"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/normalize"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

func copySeededDBWithSidecars(t *testing.T, src, name string) string {
	t.Helper()

	dst := filepath.Join(t.TempDir(), name)
	copySeededFile(t, src, dst)

	sidecars, err := filepath.Glob(src + ".*.rbi")
	if err != nil {
		t.Fatalf("glob seeded sidecars: %v", err)
	}
	for _, sidecar := range sidecars {
		copySeededFile(t, sidecar, dst+sidecar[len(src):])
	}

	return dst
}

func copySeededFile(t *testing.T, src, dst string) {
	t.Helper()

	in, err := os.Open(src)
	if err != nil {
		t.Fatalf("open seeded file %q: %v", src, err)
	}
	defer func() { _ = in.Close() }()

	out, err := os.Create(dst)
	if err != nil {
		t.Fatalf("create copied file %q: %v", dst, err)
	}
	defer func() { _ = out.Close() }()

	if _, err = io.Copy(out, in); err != nil {
		t.Fatalf("copy seeded file %q: %v", src, err)
	}
	if err = out.Sync(); err != nil {
		t.Fatalf("sync copied file %q: %v", dst, err)
	}
}

func queryCountSingleton(id uint64) posting.List {
	return (posting.List{}).BuildAdded(id)
}

func countByExprBitmap(t *testing.T, db *DB[uint64, Rec], expr qx.Expr) uint64 {
	t.Helper()
	b, err := db.evalExpr(expr)
	if err != nil {
		t.Fatalf("evalExpr: %v", err)
	}
	defer b.release()
	return db.countPostingResult(b)
}

func countByExprBitmapUserBench(t *testing.T, db *DB[uint64, UserBench], expr qx.Expr) uint64 {
	t.Helper()
	b, err := db.evalExpr(expr)
	if err != nil {
		t.Fatalf("evalExpr: %v", err)
	}
	defer b.release()
	return db.countPostingResult(b)
}

func countByExprBitmapCountORBench(t *testing.T, db *DB[uint64, countORBenchRec], expr qx.Expr) uint64 {
	t.Helper()
	b, err := db.evalExpr(expr)
	if err != nil {
		t.Fatalf("evalExpr: %v", err)
	}
	defer b.release()
	return db.countPostingResult(b)
}

func expectPredicateExactPostingFilterCount(t *testing.T, p predicate, src posting.List, want uint64) {
	t.Helper()

	work := src.Clone()
	mode, exact, work, _ := plannerFilterPostingByChecks([]predicate{p}, []int{0}, src, work, true)
	defer work.Release()
	if mode != plannerPredicateBucketExact {
		t.Fatalf("expected exact posting filtering, got mode=%v", mode)
	}
	if exact.IsEmpty() {
		t.Fatalf("expected exact filtered posting")
	}
	if got := exact.Cardinality(); got != want {
		t.Fatalf("unexpected exact filtered posting cardinality: got=%d want=%d", got, want)
	}
}

func cloneSnapshotUniverse(t *testing.T, db *DB[uint64, Rec]) posting.List {
	t.Helper()
	return db.snapshotUniverseView().Clone()
}

type countORBenchRec struct {
	Country string   `db:"country"`
	Plan    string   `db:"plan"`
	Status  string   `db:"status"`
	Age     int      `db:"age"`
	Score   float64  `db:"score"`
	Email   string   `db:"email"`
	Tags    []string `db:"tags"`
	Roles   []string `db:"roles"`
}

type seededDBFixture struct {
	once sync.Once
	path string
	err  error
}

var countPreparePredicateBroadRangeSeeded seededDBFixture
var countORBenchSharedSeeded seededDBFixture
var countBroadRangeComplementWithNilSeeded seededDBFixture
var countBroadRangeComplementNoNilSeeded seededDBFixture

func seedCountORBenchData(t *testing.T, db *DB[uint64, countORBenchRec], n int) {
	t.Helper()

	countries := []string{"US", "DE", "NL", "FR"}
	plans := []string{"free", "pro", "enterprise", "basic"}
	statuses := []string{"active", "trial", "paused", "banned"}
	tagsPool := [][]string{
		{"go", "db"},
		{"security", "ops"},
		{"go", "security"},
		{"rust", "ops"},
	}
	rolesPool := [][]string{
		{"admin", "support"},
		{"admin", "moderator"},
		{"support"},
		{"moderator"},
	}

	ids := make([]uint64, n)
	vals := make([]*countORBenchRec, n)
	for i := 0; i < n; i++ {
		ids[i] = uint64(i + 1)
		vals[i] = &countORBenchRec{
			Country: countries[i%len(countries)],
			Plan:    plans[(i/2)%len(plans)],
			Status:  statuses[(i/3)%len(statuses)],
			Age:     18 + (i % 55),
			Score:   float64(i % 200),
			Email:   fmt.Sprintf("user%06d@example.com", i),
			Tags:    append([]string(nil), tagsPool[i%len(tagsPool)]...),
			Roles:   append([]string(nil), rolesPool[(i/5)%len(rolesPool)]...),
		}
	}
	if err := db.BatchSet(ids, vals); err != nil {
		t.Fatalf("BatchSet: %v", err)
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}
}

func countPreparePredicateBroadRangeSeedPath(t *testing.T) string {
	t.Helper()

	countPreparePredicateBroadRangeSeeded.once.Do(func() {
		dir, err := os.MkdirTemp("", "rbi-count-broad-range-")
		if err != nil {
			countPreparePredicateBroadRangeSeeded.err = err
			return
		}

		path := filepath.Join(dir, "seed.db")
		db, raw := openBoltAndNew[uint64, Rec](t, path, Options{AnalyzeInterval: -1})
		seedGeneratedUint64Data(t, db, 160_000, func(i int) *Rec {
			return &Rec{
				Name:   fmt.Sprintf("u_%d", i),
				Email:  fmt.Sprintf("user%05d@example.com", i),
				Age:    i,
				Score:  float64(i % 1_000),
				Active: i%2 == 0,
				Meta: Meta{
					Country: "US",
				},
			}
		})
		if err = db.RebuildIndex(); err != nil {
			countPreparePredicateBroadRangeSeeded.err = err
			_ = db.Close()
			_ = raw.Close()
			return
		}
		if err = db.Close(); err != nil {
			countPreparePredicateBroadRangeSeeded.err = err
			_ = raw.Close()
			return
		}
		if err = raw.Close(); err != nil {
			countPreparePredicateBroadRangeSeeded.err = err
			return
		}
		countPreparePredicateBroadRangeSeeded.path = path
	})

	if countPreparePredicateBroadRangeSeeded.err != nil {
		t.Fatalf("count prepare predicate seeded fixture: %v", countPreparePredicateBroadRangeSeeded.err)
	}
	return countPreparePredicateBroadRangeSeeded.path
}

func countOpenPreparePredicateBroadRangeDB(t *testing.T, opts Options) *DB[uint64, Rec] {
	t.Helper()

	path := copySeededDBWithSidecars(t, countPreparePredicateBroadRangeSeedPath(t), "count_prepare_predicate_broad_range.db")
	db, raw := openBoltAndNew[uint64, Rec](t, path, opts)
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	return db
}

func countORBenchSharedSeedPath(t *testing.T) string {
	t.Helper()

	countORBenchSharedSeeded.once.Do(func() {
		dir, err := os.MkdirTemp("", "rbi-count-or-shared-")
		if err != nil {
			countORBenchSharedSeeded.err = err
			return
		}

		path := filepath.Join(dir, "seed.db")
		db, raw := openBoltAndNew[uint64, countORBenchRec](t, path, Options{AnalyzeInterval: -1})
		seedCountORBenchData(t, db, 128_000)
		if err = db.Close(); err != nil {
			countORBenchSharedSeeded.err = err
			_ = raw.Close()
			return
		}
		if err = raw.Close(); err != nil {
			countORBenchSharedSeeded.err = err
			return
		}
		countORBenchSharedSeeded.path = path
	})

	if countORBenchSharedSeeded.err != nil {
		t.Fatalf("count or shared seeded fixture: %v", countORBenchSharedSeeded.err)
	}
	return countORBenchSharedSeeded.path
}

func countOpenORBenchSharedDB(t *testing.T, name string, opts Options) *DB[uint64, countORBenchRec] {
	t.Helper()

	path := copySeededDBWithSidecars(t, countORBenchSharedSeedPath(t), name)
	db, raw := openBoltAndNew[uint64, countORBenchRec](t, path, opts)
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	return db
}

func countBroadRangeComplementWithNilSeedPath(t *testing.T) string {
	t.Helper()

	countBroadRangeComplementWithNilSeeded.once.Do(func() {
		dir, err := os.MkdirTemp("", "rbi-count-broad-range-opt-with-nil-")
		if err != nil {
			countBroadRangeComplementWithNilSeeded.err = err
			return
		}

		path := filepath.Join(dir, "seed.db")
		db, raw := openBoltAndNew[uint64, Rec](t, path, Options{AnalyzeInterval: -1})
		seedBroadRangeComplementOptData(t, db, 4_000, 72_000, 4_000)
		if err = db.Close(); err != nil {
			countBroadRangeComplementWithNilSeeded.err = err
			_ = raw.Close()
			return
		}
		if err = raw.Close(); err != nil {
			countBroadRangeComplementWithNilSeeded.err = err
			return
		}
		countBroadRangeComplementWithNilSeeded.path = path
	})

	if countBroadRangeComplementWithNilSeeded.err != nil {
		t.Fatalf("count broad-range complement fixture with nil: %v", countBroadRangeComplementWithNilSeeded.err)
	}
	return countBroadRangeComplementWithNilSeeded.path
}

func countBroadRangeComplementNoNilSeedPath(t *testing.T) string {
	t.Helper()

	countBroadRangeComplementNoNilSeeded.once.Do(func() {
		dir, err := os.MkdirTemp("", "rbi-count-broad-range-opt-no-nil-")
		if err != nil {
			countBroadRangeComplementNoNilSeeded.err = err
			return
		}

		path := filepath.Join(dir, "seed.db")
		db, raw := openBoltAndNew[uint64, Rec](t, path, Options{AnalyzeInterval: -1})
		seedBroadRangeComplementOptData(t, db, 4_000, 76_000, 0)
		if err = db.Close(); err != nil {
			countBroadRangeComplementNoNilSeeded.err = err
			_ = raw.Close()
			return
		}
		if err = raw.Close(); err != nil {
			countBroadRangeComplementNoNilSeeded.err = err
			return
		}
		countBroadRangeComplementNoNilSeeded.path = path
	})

	if countBroadRangeComplementNoNilSeeded.err != nil {
		t.Fatalf("count broad-range complement fixture without nil: %v", countBroadRangeComplementNoNilSeeded.err)
	}
	return countBroadRangeComplementNoNilSeeded.path
}

func countOpenBroadRangeComplementOptDB(t *testing.T, name string, withNil bool) *DB[uint64, Rec] {
	t.Helper()

	src := countBroadRangeComplementNoNilSeedPath(t)
	if withNil {
		src = countBroadRangeComplementWithNilSeedPath(t)
	}

	path := copySeededDBWithSidecars(t, src, name)
	db, raw := openBoltAndNew[uint64, Rec](t, path, Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	return db
}

func TestCount_ScalarInSplit_MixedResiduals_UsesPlan(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	sink := func(ev TraceEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	db := countOpenORBenchSharedDB(t, "test_count_scalar_in_split_mixed.db", Options{
		AnalyzeInterval:  -1,
		TraceSink:        sink,
		TraceSampleEvery: 1,
	})

	q := qx.Query(
		qx.IN("country", []string{"US", "DE", "NL"}),
		qx.NOTIN("status", []string{"banned"}),
		qx.HASANY("tags", []string{"security", "ops"}),
		qx.GTE("age", 24),
		qx.GTE("score", 60.0),
	)

	got, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	want := countByExprBitmapCountORBench(t, db, q.Expr)
	if got != want {
		t.Fatalf("count mismatch: got=%d want=%d", got, want)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	if ev.Plan != string(PlanCountScalarInSplit) {
		t.Fatalf("expected %q, got %q", PlanCountScalarInSplit, ev.Plan)
	}
}

func TestCount_ScalarInSplit_CohortShape_MatchesBitmap(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	sink := func(ev TraceEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	dir := t.TempDir()
	db, raw := openBoltAndNew[uint64, countORBenchRec](t, filepath.Join(dir, "test_count_scalar_in_split_cohort.db"), Options{
		AnalyzeInterval:  -1,
		TraceSink:        sink,
		TraceSampleEvery: 1,
	})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	db.DisableSync()
	defer db.EnableSync()

	countries := []string{"NL", "DE", "PL", "SE", "FR", "ES", "GB", "US"}
	statuses := []string{"active", "trial", "paused", "banned"}
	tagsPool := [][]string{
		{"go", "db"},
		{"security", "ops"},
		{"go", "security"},
		{"rust", "ops"},
		{"db", "security"},
	}

	const n = 128_000
	ids := make([]uint64, n)
	vals := make([]*countORBenchRec, n)
	for i := 0; i < n; i++ {
		ids[i] = uint64(i + 1)
		vals[i] = &countORBenchRec{
			Country: countries[i%len(countries)],
			Status:  statuses[(i/4)%len(statuses)],
			Age:     18 + (i % 55),
			Score:   float64(i % 120),
			Email:   fmt.Sprintf("user%06d@example.com", i),
			Tags:    append([]string(nil), tagsPool[i%len(tagsPool)]...),
		}
	}
	if err := db.BatchSet(ids, vals); err != nil {
		t.Fatalf("BatchSet: %v", err)
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.NOTIN("status", []string{"banned"}),
		qx.IN("country", []string{"NL", "DE", "PL", "SE", "FR", "ES", "GB"}),
		qx.GTE("age", 25),
		qx.LTE("age", 45),
		qx.HASANY("tags", []string{"go", "db", "security"}),
		qx.GTE("score", 80.0),
	)

	got, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	want := countByExprBitmapCountORBench(t, db, q.Expr)
	if got != want {
		t.Fatalf("count mismatch: got=%d want=%d", got, want)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	if ev.Plan != string(PlanCountScalarInSplit) {
		t.Fatalf("expected %q, got %q", PlanCountScalarInSplit, ev.Plan)
	}
}

func TestCount_ByPredicates_BucketLead_MatchesBitmap(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%05d@example.com", i),
			Age:    i,
			Score:  float64(i % 1_000),
			Active: i%2 == 0,
			Meta: Meta{Country: func() string {
				if i%3 == 0 {
					return "NL"
				}
				return "US"
			}()},
		}
	})

	q := qx.Query(
		qx.GTE("age", 2_500),
		qx.LT("age", 15_000),
		qx.NOT(qx.EQ("active", false)),
	)

	got, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	want := countByExprBitmap(t, db, q.Expr)
	if got != want {
		t.Fatalf("count mismatch: got=%d want=%d", got, want)
	}
}

func TestCount_ByPredicates_BucketLeadHasAnyResidual_MatchesBitmap(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: -1,
	})

	tagSets := [][]string{
		{"go"},
		{"db"},
		{"go", "db"},
	}
	seedGeneratedUint64Data(t, db, 66_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%05d@example.com", i),
			Age:    i,
			Score:  float64(i % 1_000),
			Active: i%2 == 0,
			Tags:   append([]string(nil), tagSets[i%len(tagSets)]...),
			Meta: Meta{
				Country: "US",
			},
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	expr := qx.AND(
		qx.GTE("age", 5_000),
		qx.LT("age", 60_000),
		qx.HASANY("tags", []string{"go", "db"}),
	)
	leaves, ok := collectAndLeaves(expr)
	if !ok {
		t.Fatalf("collectAndLeaves failed")
	}
	preds, ok := db.buildPredicatesWithMode(leaves, false)
	if !ok {
		t.Fatalf("buildPredicatesWithMode failed")
	}
	defer releasePredicates(preds)

	leadIdx := -1
	for i := range preds {
		if preds[i].expr.Field == "age" && preds[i].expr.Op == qx.OpGTE {
			leadIdx = i
			break
		}
	}
	if leadIdx < 0 {
		t.Fatalf("expected age range lead predicate")
	}

	universe := db.snapshotUniverseCardinality()
	leadEst := preds[leadIdx].estCard
	if err := db.prepareCountPredicate(&preds[leadIdx], leadEst, universe); err != nil {
		t.Fatalf("prepareCountPredicate(lead): %v", err)
	}

	if len(preds) > countPredicateScanMaxLeaves {
		t.Fatalf("unexpected predicate count: got=%d max=%d", len(preds), countPredicateScanMaxLeaves)
	}
	var activeInline [countPredicateScanMaxLeaves]int
	active := activeInline[:0]
	for i := range preds {
		if i == leadIdx {
			continue
		}
		p := preds[i]
		if p.covered || p.alwaysTrue {
			continue
		}
		if p.alwaysFalse || !p.hasContains() {
			t.Fatalf("unexpected residual predicate state: idx=%d kind=%v", i, p.kind)
		}
		active = append(active, i)
	}
	for _, pi := range active {
		if err := db.prepareCountPredicate(&preds[pi], leadEst, universe); err != nil {
			t.Fatalf("prepareCountPredicate(residual): %v", err)
		}
	}
	sortActivePredicates(active, preds)

	hasAnyIdx := -1
	for _, pi := range active {
		if preds[pi].expr.Field == "tags" && preds[pi].expr.Op == qx.OpHASANY {
			hasAnyIdx = pi
			break
		}
	}
	if hasAnyIdx < 0 {
		t.Fatalf("expected HASANY residual")
	}

	var exactActiveInline [countPredicateScanMaxLeaves]int
	exactActive := buildPostingApplyActive(exactActiveInline[:0], active, preds)
	if countIndexSliceContains(exactActive, hasAnyIdx) {
		t.Fatalf("expected HASANY residual to stay out of postingResult-filter subset, got=%v", exactActive)
	}

	extraExact := db.buildCountLeadResidualExactFilters(t, preds, active)
	defer func() {
		for _, f := range extraExact {
			f.ids.Release()
		}
	}()
	if len(extraExact) != 1 {
		t.Fatalf("expected one local HASANY exact residual filter, got=%d", len(extraExact))
	}

	got, examined, ok := db.tryCountByPredicatesLeadBuckets(preds, leadIdx, active)
	if !ok {
		t.Fatalf("expected bucket-lead count fast path")
	}
	if examined == 0 {
		t.Fatalf("expected examined postings")
	}
	want := countByExprBitmap(t, db, expr)
	if got != want {
		t.Fatalf("count mismatch: got=%d want=%d", got, want)
	}
}

func TestCount_ByPredicates_PostingsLead_MatchesBitmap(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	countries := []string{"US", "DE", "FR", "GB", "NL"}
	tagSets := [][]string{
		{"go"},
		{"db"},
		{"ops"},
		{"go", "db"},
	}
	seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%05d@example.com", i),
			Age:    i,
			Score:  float64(i % 1_000),
			Active: i%2 == 0,
			Tags:   append([]string(nil), tagSets[i%len(tagSets)]...),
			Meta: Meta{
				Country: countries[i%len(countries)],
			},
		}
	})

	expr := qx.AND(
		qx.IN("country", []string{"US", "DE", "FR", "GB"}),
		qx.NOTIN("active", []bool{false}),
		qx.HASANY("tags", []string{"go", "db"}),
	)
	leaves, ok := collectAndLeaves(expr)
	if !ok {
		t.Fatalf("collectAndLeaves failed")
	}
	preds, ok := db.buildPredicatesWithMode(leaves, false)
	if !ok {
		t.Fatalf("buildPredicatesWithMode failed")
	}
	defer releasePredicates(preds)

	universe := db.snapshotUniverseCardinality()
	leadIdx := -1
	leadScore := 0.0
	leadEst := uint64(0)
	for i := range preds {
		p := preds[i]
		if p.alwaysFalse {
			t.Fatalf("unexpected alwaysFalse predicate")
		}
		if p.alwaysTrue || p.covered || !p.hasIter() || p.estCard == 0 {
			continue
		}
		if countLeadTooRisky(p.expr.Op, p.estCard, universe) {
			continue
		}
		score := float64(p.estCard) * float64(countPredicateLeadWeight(p))
		if leadIdx == -1 || score < leadScore {
			leadIdx = i
			leadEst = p.estCard
			leadScore = score
		}
	}
	if leadIdx < 0 {
		t.Fatalf("expected iterable lead")
	}
	if preds[leadIdx].expr.Op != qx.OpIN {
		t.Fatalf("expected IN lead, got %v", preds[leadIdx].expr.Op)
	}
	if err := db.prepareCountPredicate(&preds[leadIdx], leadEst, universe); err != nil {
		t.Fatalf("prepareCountPredicate(lead): %v", err)
	}

	if len(preds) > countPredicateScanMaxLeaves {
		t.Fatalf("unexpected predicate count: got=%d max=%d", len(preds), countPredicateScanMaxLeaves)
	}
	var activeInline [countPredicateScanMaxLeaves]int
	active := activeInline[:0]
	for i := range preds {
		if i == leadIdx {
			continue
		}
		p := preds[i]
		if p.covered || p.alwaysTrue {
			continue
		}
		if p.alwaysFalse || !p.hasContains() {
			t.Fatalf("unexpected residual predicate state: idx=%d kind=%v", i, p.kind)
		}
		active = append(active, i)
	}
	for _, pi := range active {
		if err := db.prepareCountPredicate(&preds[pi], leadEst, universe); err != nil {
			t.Fatalf("prepareCountPredicate(residual): %v", err)
		}
	}
	sortActivePredicates(active, preds)

	got, examined, ok := db.tryCountByPredicatesLeadPostings(preds, leadIdx, active)
	if !ok {
		t.Fatalf("expected postings-lead count fast path")
	}
	if examined == 0 {
		t.Fatalf("expected examined postings")
	}
	want := countByExprBitmap(t, db, expr)
	if got != want {
		t.Fatalf("count mismatch: got=%d want=%d", got, want)
	}
}

func TestCount_ByPredicates_BucketLead_ContradictoryPrefixesReturnZero(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%05d@example.com", i),
			Age:    i,
			Score:  float64(i % 1_000),
			Active: i%2 == 0,
			Meta: Meta{
				Country: "US",
			},
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	expr := qx.AND(
		qx.PREFIX("email", "user01"),
		qx.PREFIX("email", "user02"),
	)
	leaves, ok := collectAndLeaves(expr)
	if !ok {
		t.Fatalf("collectAndLeaves failed")
	}
	preds, ok := db.buildPredicatesWithMode(leaves, false)
	if !ok {
		t.Fatalf("buildPredicatesWithMode failed")
	}
	defer releasePredicates(preds)

	active := []int{0, 1}
	got, examined, ok := db.tryCountByPredicatesLeadBuckets(preds, 0, active)
	if !ok {
		t.Fatalf("expected bucket-lead count fast path")
	}
	if examined != 0 {
		t.Fatalf("expected zero examined rows for empty prefix intersection, got=%d", examined)
	}

	want := countByExprBitmap(t, db, expr)
	if got != want {
		t.Fatalf("count mismatch: got=%d want=%d", got, want)
	}
}

func TestCount_BroadLeadWithSmallIN_PrefersScalarInSplit(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	sink := func(ev TraceEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSink:        sink,
		TraceSampleEvery: 1,
	})

	countries := []string{"US", "DE", "FR", "GB"}
	seedGeneratedUint64Data(t, db, 96_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%06d@example.com", i),
			Age:    i,
			Score:  float64(i % 1_000),
			Active: i%2 == 0,
			Meta: Meta{
				Country: countries[i%len(countries)],
			},
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.IN("country", []string{"US", "DE", "FR"}),
		qx.GTE("age", 1_000),
		qx.LTE("age", 90_000),
		qx.GTE("score", 10.0),
	)

	got, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	want := countByExprBitmap(t, db, q.Expr)
	if got != want {
		t.Fatalf("count mismatch: got=%d want=%d", got, want)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	if ev.Plan != string(PlanCountScalarInSplit) {
		t.Fatalf("expected small-IN broad-lead query to use %q, got %q", PlanCountScalarInSplit, ev.Plan)
	}
	if ev.CountPredicatePreparations != 0 {
		t.Fatalf("expected scalar-in-split path to avoid predicate preparation, got %d total preparations", ev.CountPredicatePreparations)
	}
}

func TestCount_ORByPredicates_PostingLeadResidualExactFilters_MatchesBitmap(t *testing.T) {
	db := countOpenORBenchSharedDB(t, "test_count_or.db", Options{
		AnalyzeInterval: -1,
	})

	expr := qx.OR(
		qx.AND(
			qx.PREFIX("email", "user09"),
			qx.EQ("status", "active"),
			qx.GTE("score", 60.0),
		),
		qx.AND(
			qx.EQ("plan", "enterprise"),
			qx.HASANY("roles", []string{"admin", "support"}),
			qx.NOTIN("status", []string{"banned"}),
		),
		qx.AND(
			qx.EQ("status", "active"),
			qx.HASANY("tags", []string{"security", "ops"}),
			qx.IN("country", []string{"US", "DE", "NL"}),
		),
	)

	got, ok, err := db.tryCountORByPredicates(expr, nil)
	if err != nil {
		t.Fatalf("tryCountORByPredicates: %v", err)
	}
	if !ok {
		t.Fatalf("expected count OR predicate fast path")
	}

	b, err := db.evalExpr(expr)
	if err != nil {
		t.Fatalf("evalExpr: %v", err)
	}
	defer b.release()
	want := db.countPostingResult(b)
	if got != want {
		t.Fatalf("count mismatch: got=%d want=%d", got, want)
	}
}

func TestCount_ORByPredicates_StrictWidePrefixLeadBudgetUsesScanWeight(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	sink := func(ev TraceEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSink:        sink,
		TraceSampleEvery: 1,
	})

	seedGeneratedUint64Data(t, db, 100_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%06d@example.com", i),
			Age:    i,
			Score:  float64(i % 1_000),
			Active: i%2 == 0,
			Meta: Meta{
				Country: "US",
			},
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	expr := qx.OR(
		qx.AND(qx.PREFIX("email", "user00"), qx.EQ("active", true)),
		qx.AND(qx.PREFIX("email", "user01"), qx.EQ("active", true)),
		qx.AND(qx.PREFIX("email", "user02"), qx.EQ("active", true)),
		qx.AND(qx.PREFIX("email", "user03"), qx.EQ("active", true)),
	)
	q := qx.Query(expr)

	uc := db.snapshotUniverseCardinality()
	var oldExpectedProbes uint64
	var scanExpectedProbes uint64
	for _, op := range expr.Operands {
		leaves, ok := collectAndLeaves(op)
		if !ok {
			t.Fatalf("collectAndLeaves failed for %v", op)
		}
		preds, ok := db.buildPredicatesWithMode(leaves, false)
		if !ok {
			t.Fatalf("buildPredicatesWithMode failed for %v", op)
		}
		leadIdx, leadEst, _ := db.pickCountORLeadPredicate(preds, uc)
		if leadIdx < 0 {
			releasePredicates(preds)
			t.Fatalf("expected OR lead for branch %v", op)
		}
		lead := preds[leadIdx]
		if lead.expr.Op != qx.OpPREFIX {
			releasePredicates(preds)
			t.Fatalf("expected prefix lead, got field=%s op=%v", lead.expr.Field, lead.expr.Op)
		}
		oldExpectedProbes += leadEst * countLeadOpWeight(lead.expr.Op)
		scanExpectedProbes += leadEst * countPredicateLeadScanWeight(lead)
		releasePredicates(preds)
	}
	if oldExpectedProbes > uc {
		t.Fatalf("expected old op-weight budget to admit OR path, got oldExpectedProbes=%d universe=%d", oldExpectedProbes, uc)
	}
	if scanExpectedProbes <= uc {
		t.Fatalf("expected scan-weight budget to reject OR path, got scanExpectedProbes=%d universe=%d", scanExpectedProbes, uc)
	}

	got, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	want := countByExprBitmap(t, db, expr)
	if got != want {
		t.Fatalf("count mismatch: got=%d want=%d", got, want)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	if ev.Plan != string(PlanCountMaterialized) {
		t.Fatalf("expected strict-wide OR fallback to use %q, got %q", PlanCountMaterialized, ev.Plan)
	}
}

func TestCount_ByPredicates_SinglePostingLead_MatchesBitmap(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	countries := []string{"US", "DE", "FR", "GB"}
	tagSets := [][]string{
		{"go"},
		{"db"},
		{"ops"},
		{"go", "db"},
		{"go", "ops"},
	}
	seedGeneratedUint64Data(t, db, 120_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%05d@example.com", i),
			Age:    18 + (i % 60),
			Score:  float64(i % 1_000),
			Active: i%2 == 0,
			Tags:   append([]string(nil), tagSets[i%len(tagSets)]...),
			Meta: Meta{
				Country: countries[i%len(countries)],
			},
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	expr := qx.AND(
		qx.EQ("country", "US"),
		qx.NOTIN("active", []bool{false}),
		qx.GTE("score", 120.0),
		qx.HASANY("tags", []string{"go", "db"}),
	)
	leaves, ok := collectAndLeaves(expr)
	if !ok {
		t.Fatalf("collectAndLeaves failed")
	}
	preds, ok := db.buildPredicatesWithMode(leaves, false)
	if !ok {
		t.Fatalf("buildPredicatesWithMode failed")
	}
	defer releasePredicates(preds)

	universe := db.snapshotUniverseCardinality()
	leadIdx := -1
	leadScore := 0.0
	leadEst := uint64(0)
	for i := range preds {
		p := preds[i]
		if p.alwaysFalse {
			t.Fatalf("unexpected alwaysFalse predicate")
		}
		if p.alwaysTrue || p.covered || !p.hasIter() || p.estCard == 0 {
			continue
		}
		if countLeadTooRisky(p.expr.Op, p.estCard, universe) {
			continue
		}
		score := float64(p.estCard) * float64(countPredicateLeadWeight(p))
		if leadIdx == -1 || score < leadScore {
			leadIdx = i
			leadEst = p.estCard
			leadScore = score
		}
	}
	if leadIdx < 0 {
		t.Fatalf("expected iterable lead")
	}
	if preds[leadIdx].expr.Op != qx.OpEQ {
		t.Fatalf("expected EQ lead, got %v", preds[leadIdx].expr.Op)
	}
	if err := db.prepareCountPredicate(&preds[leadIdx], leadEst, universe); err != nil {
		t.Fatalf("prepareCountPredicate(lead): %v", err)
	}

	if len(preds) > countPredicateScanMaxLeaves {
		t.Fatalf("unexpected predicate count: got=%d max=%d", len(preds), countPredicateScanMaxLeaves)
	}
	var activeInline [countPredicateScanMaxLeaves]int
	active := activeInline[:0]
	for i := range preds {
		if i == leadIdx {
			continue
		}
		p := preds[i]
		if p.covered || p.alwaysTrue {
			continue
		}
		if p.alwaysFalse || !p.hasContains() {
			t.Fatalf("unexpected residual predicate state: idx=%d kind=%v", i, p.kind)
		}
		active = append(active, i)
	}
	for _, pi := range active {
		if err := db.prepareCountPredicate(&preds[pi], leadEst, universe); err != nil {
			t.Fatalf("prepareCountPredicate(residual): %v", err)
		}
	}
	sortActivePredicates(active, preds)

	got, examined, ok := db.tryCountByPredicatesLeadPostings(preds, leadIdx, active)
	if !ok {
		t.Fatalf("expected single-posting lead count fast path")
	}
	if examined == 0 {
		t.Fatalf("expected examined postings")
	}
	want := countByExprBitmap(t, db, expr)
	if got != want {
		t.Fatalf("count mismatch: got=%d want=%d", got, want)
	}
}

func TestCount_ORPredicates_FiveBranches_MatchesBitmap(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	seedGeneratedUint64Data(t, db, 25_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%05d@example.com", i),
			Age:    i,
			Score:  float64(i % 10_000),
			Active: i%2 == 0,
			Meta: Meta{Country: func() string {
				if i%3 == 0 {
					return "NL"
				}
				return "US"
			}()},
		}
	})

	expr := qx.OR(
		qx.AND(qx.PREFIX("email", "user10"), qx.EQ("active", true)),
		qx.AND(qx.PREFIX("email", "user11"), qx.EQ("country", "NL")),
		qx.AND(qx.PREFIX("email", "user12"), qx.EQ("active", false)),
		qx.AND(qx.PREFIX("email", "user13"), qx.EQ("country", "US")),
		qx.AND(qx.PREFIX("email", "user14"), qx.GTE("age", 14_000)),
	)
	q := qx.Query(expr)

	got, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	want := countByExprBitmap(t, db, expr)
	if got != want {
		t.Fatalf("count mismatch: got=%d want=%d", got, want)
	}
}

func TestCount_ORByPredicates_HybridMaterializedSpill_MatchesBitmap(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	sink := func(ev TraceEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	dir := t.TempDir()
	db, raw := openBoltAndNew[uint64, UserBench](t, filepath.Join(dir, "test_count_or_hybrid.db"), Options{
		AnalyzeInterval:  -1,
		TraceSink:        sink,
		TraceSampleEvery: 1,
	})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
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

	const n = 150_000
	ids := make([]uint64, n)
	vals := make([]*UserBench, n)
	for i := 0; i < n; i++ {
		id := uint64(i + 1)
		ids[i] = id
		vals[i] = &UserBench{
			ID:      id,
			Country: countries[r.IntN(len(countries))],
			Plan:    plans[r.IntN(len(plans))],
			Status:  statuses[r.IntN(len(statuses))],
			Age:     18 + r.IntN(60),
			Score:   float64(r.IntN(1000)),
			Name:    fmt.Sprintf("user-%d", i+1),
			Email:   fmt.Sprintf("user%06d@example.com", i+1),
			Tags:    append([]string(nil), tagsPool[r.IntN(len(tagsPool))]...),
			Roles:   append([]string(nil), rolesPool[r.IntN(len(rolesPool))]...),
		}
	}
	if err := db.BatchSet(ids, vals); err != nil {
		t.Fatalf("BatchSet: %v", err)
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	expr := qx.OR(
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
	)
	q := qx.Query(expr)

	got, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	want := countByExprBitmapUserBench(t, db, expr)
	if got != want {
		t.Fatalf("count mismatch: got=%d want=%d", got, want)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	if ev.Plan != string(PlanCountORHybrid) {
		t.Fatalf("expected hybrid OR count plan %q, got %q", PlanCountORHybrid, ev.Plan)
	}
	if len(ev.ORBranches) != len(expr.Operands) {
		t.Fatalf("expected %d OR branch traces, got %d", len(expr.Operands), len(ev.ORBranches))
	}
	spilled := false
	for _, br := range ev.ORBranches {
		if br.SkipReason == "materialized_spill" {
			spilled = true
			break
		}
	}
	if !spilled {
		t.Fatalf("expected at least one hybrid postingResult spill branch, got trace=%+v", ev.ORBranches)
	}
}

func TestCount_ORByPredicates_HybridTraceEmitsUseOriginalBranchIndex(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	sink := func(ev TraceEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSink:        sink,
		TraceSampleEvery: 1,
	})
	_ = seedData(t, db, 20_000)

	expr := qx.OR(
		qx.AND(
			qx.CONTAINS("full_name", "ZZZ"),
			qx.GTE("score", 10.0),
		),
		qx.AND(
			qx.PREFIX("full_name", "FN-1"),
			qx.EQ("active", true),
		),
		qx.AND(
			qx.EQ("name", "alice"),
			qx.EQ("active", true),
		),
		qx.AND(
			qx.EQ("country", "NL"),
			qx.HASANY("tags", []string{"go"}),
		),
	)
	q := qx.Query(expr)

	got, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	want := countByExprBitmap(t, db, expr)
	if got != want {
		t.Fatalf("count mismatch: got=%d want=%d", got, want)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	if ev.Plan != string(PlanCountORHybrid) {
		t.Fatalf("expected hybrid OR count plan %q, got %q", PlanCountORHybrid, ev.Plan)
	}
	if len(ev.ORBranches) != len(expr.Operands) {
		t.Fatalf("expected %d OR branch traces, got %d", len(expr.Operands), len(ev.ORBranches))
	}
	if ev.ORBranches[0].SkipReason != "materialized_spill" {
		t.Fatalf("expected branch 0 to spill, got trace=%+v", ev.ORBranches)
	}
	if ev.ORBranches[0].RowsEmitted != 0 {
		t.Fatalf("expected spilled empty branch 0 to emit 0 rows, got trace=%+v", ev.ORBranches)
	}
	if ev.ORBranches[1].RowsEmitted == 0 {
		t.Fatalf("expected branch 1 PREFIX scan to emit rows on its original index, got trace=%+v", ev.ORBranches)
	}
}

func TestCountORBranches_ShouldUseSeenDedup_Adaptive(t *testing.T) {
	newBranchesBuf := func(ests ...uint64) *pooled.SliceBuf[countORBranch] {
		buf := countORBranchSlicePool.Get()
		buf.Grow(len(ests))
		for _, est := range ests {
			buf.Append(countORBranch{est: est})
		}
		return buf
	}
	highOverlap := newBranchesBuf(90_000, 85_000, 80_000, 75_000, 70_000)
	defer countORBranchSlicePool.Put(highOverlap)
	if !countORBranchesShouldUseSeenDedupBuf(highOverlap, 100_000, 250_000) {
		t.Fatalf("expected seen dedup for high-overlap wide OR")
	}

	threeWayOverlap := newBranchesBuf(90_000, 85_000, 80_000)
	defer countORBranchSlicePool.Put(threeWayOverlap)
	if !countORBranchesShouldUseSeenDedupBuf(threeWayOverlap, 100_000, 250_000) {
		t.Fatalf("expected seen dedup for high-overlap three-way OR")
	}

	lowOverlap := newBranchesBuf(25_000, 25_000, 25_000, 25_000)
	defer countORBranchSlicePool.Put(lowOverlap)
	if countORBranchesShouldUseSeenDedupBuf(lowOverlap, 2_000_000, 120_000) {
		t.Fatalf("unexpected seen dedup for low-overlap OR")
	}
}

func TestCountORDedupThresholds_Adaptive(t *testing.T) {
	loProbe := countORSeenUnionThreshold(2_000_000, 4, 200_000)
	hiProbe := countORSeenUnionThreshold(2_000_000, 4, 2_000_000)
	if hiProbe >= loProbe {
		t.Fatalf("expected lower union threshold for high probe share: low_probe=%d high_probe=%d", loProbe, hiProbe)
	}

	minShareLo, forceShareLo := countORDedupDupShareBounds(4, 2_000_000, 200_000)
	minShareHi, forceShareHi := countORDedupDupShareBounds(6, 2_000_000, 2_000_000)
	if minShareHi >= minShareLo {
		t.Fatalf("expected looser min dup share for wider/high-probe OR: low=%.3f high=%.3f", minShareLo, minShareHi)
	}
	if forceShareHi >= forceShareLo {
		t.Fatalf("expected looser force dup share for wider/high-probe OR: low=%.3f high=%.3f", forceShareLo, forceShareHi)
	}
}

func TestCountORSeenStrategy_Adaptive(t *testing.T) {
	newBranchesBuf := func(ests ...uint64) *pooled.SliceBuf[countORBranch] {
		buf := countORBranchSlicePool.Get()
		buf.Grow(len(ests))
		for _, est := range ests {
			buf.Append(countORBranch{est: est})
		}
		return buf
	}

	small := newBranchesBuf(2_000, 1_800, 1_600, 1_400)
	defer countORBranchSlicePool.Put(small)
	seenSmall := newCountORSeenBuf(small, 0, 10_000)
	defer seenSmall.release()
	if seenSmall.mode != countORSeenModeHash {
		t.Fatalf("expected hash seen set for small adaptive cap, got mode=%d", seenSmall.mode)
	}

	large := newBranchesBuf(150_000, 140_000, 130_000, 120_000, 110_000)
	defer countORBranchSlicePool.Put(large)
	seenLarge := newCountORSeenBuf(large, 60_000, 500_000)
	defer seenLarge.release()
	if seenLarge.mode != countORSeenModePosting {
		t.Fatalf("expected posting seen set for large adaptive cap, got mode=%d", seenLarge.mode)
	}
}

func TestCountORPredicateBranchLimit_Adaptive(t *testing.T) {
	if got := countORPredicateBranchLimit(0); got != countORPredicateMaxBranchesBase {
		t.Fatalf("expected base branch limit for unknown universe, got=%d", got)
	}
	if got := countORPredicateBranchLimit(120_000); got >= countORPredicateMaxBranchesBase {
		t.Fatalf("expected stricter branch limit for small universe, got=%d", got)
	}
	if got := countORPredicateBranchLimit(5_000_000); got <= countORPredicateMaxBranchesBase {
		t.Fatalf("expected wider branch limit for large universe, got=%d", got)
	}
}

func TestCountPredicateMaterializationThresholds_Adaptive(t *testing.T) {
	newPostsBuf := func(n int) *pooled.SliceBuf[posting.List] {
		buf := postingSlicePool.Get()
		buf.SetLen(n)
		return buf
	}

	setPred := predicate{
		kind:     predicateKindPostsAny,
		postsBuf: newPostsBuf(12),
	}
	defer postingSlicePool.Put(setPred.postsBuf)
	if shouldMaterializeCountSetPredicate(setPred, 500, 500_000) {
		t.Fatalf("set predicate should not materialize on low probe estimate")
	}
	if !shouldMaterializeCountSetPredicate(setPred, 5_000, 500_000) {
		t.Fatalf("set predicate should materialize on high probe estimate")
	}

	customPred := predicate{
		kind:    predicateKindCustom,
		expr:    qx.Expr{Op: qx.OpPREFIX, Field: "email"},
		estCard: 200_000,
	}
	if shouldMaterializeCustomCountPredicate(customPred, 2_000, 500_000) {
		t.Fatalf("custom predicate should not materialize below adaptive threshold")
	}
	if !shouldMaterializeCustomCountPredicate(customPred, 3_000, 500_000) {
		t.Fatalf("custom predicate should materialize above adaptive threshold")
	}

	smallHasAnyResidual := predicate{
		kind:     predicateKindPostsAny,
		expr:     qx.Expr{Op: qx.OpHASANY, Field: "roles"},
		postsBuf: newPostsBuf(3),
		estCard:  80_000,
	}
	defer postingSlicePool.Put(smallHasAnyResidual.postsBuf)
	if !shouldUseCountLeadResidualHasAnyExactFilter(smallHasAnyResidual) {
		t.Fatalf("expected small HASANY residual exact filter above threshold")
	}
	fourTermHasAnyResidual := predicate{
		kind:     predicateKindPostsAny,
		expr:     qx.Expr{Op: qx.OpHASANY, Field: "roles"},
		postsBuf: newPostsBuf(4),
		estCard:  80_000,
	}
	defer postingSlicePool.Put(fourTermHasAnyResidual.postsBuf)
	if !shouldUseCountLeadResidualHasAnyExactFilter(fourTermHasAnyResidual) {
		t.Fatalf("expected four-term HASANY residual exact filter above threshold")
	}
	tooWide := predicate{
		kind:     predicateKindPostsAny,
		expr:     qx.Expr{Op: qx.OpHASANY, Field: "roles"},
		postsBuf: newPostsBuf(5),
		estCard:  80_000,
	}
	defer postingSlicePool.Put(tooWide.postsBuf)
	if shouldUseCountLeadResidualHasAnyExactFilter(tooWide) {
		t.Fatalf("unexpected residual exact filter beyond max term threshold")
	}

	if got := countSetMaterializeMinTerms(3_000, 32_000); got <= countPredSetMaterializeMinTermsBase {
		t.Fatalf("expected stricter set-term threshold on small universe, got=%d", got)
	}
	if got := countSetMaterializeMinTerms(8_000, 5_000_000); got >= countPredSetMaterializeMinTermsBase {
		t.Fatalf("expected looser set-term threshold on large universe, got=%d", got)
	}
}

func TestCount_ScalarINSplit_MatchesBitmap(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	countries := []string{"US", "DE", "FR", "IN", "NL"}
	seedGeneratedUint64Data(t, db, 30_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%05d@example.com", i),
			Age:    i % 20_000,
			Score:  float64(i % 2_000),
			Active: i%3 != 0,
			Meta: Meta{
				Country: countries[i%len(countries)],
			},
		}
	})

	expr := qx.AND(
		qx.GTE("age", 4_000),
		qx.NOTIN("active", []bool{false}),
		qx.IN("country", []string{"US", "DE", "FR", "IN"}),
	)
	q := qx.Query(expr)

	got, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	want := countByExprBitmap(t, db, expr)
	if got != want {
		t.Fatalf("count mismatch: got=%d want=%d", got, want)
	}
}

func TestCount_ScalarINSplit_WorksAfterPatches(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	countries := []string{"US", "DE", "FR", "IN", "NL"}
	seedGeneratedUint64Data(t, db, 12_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%05d@example.com", i),
			Age:    i % 10_000,
			Score:  float64(i % 2_000),
			Active: i%2 == 0,
			Meta: Meta{
				Country: countries[i%len(countries)],
			},
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	for i := 1; i <= 512; i++ {
		c := countries[(i+2)%len(countries)]
		err := db.Patch(uint64(i), []Field{
			{Name: "country", Value: c},
			{Name: "active", Value: i%3 != 0},
			{Name: "age", Value: 8_000 + i},
		})
		if err != nil {
			t.Fatalf("Patch(%d): %v", i, err)
		}
	}

	expr := qx.AND(
		qx.GTE("age", 7_500),
		qx.NOTIN("active", []bool{false}),
		qx.IN("country", []string{"US", "DE", "FR", "IN"}),
	)
	q := qx.Query(expr)

	got, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	want := countByExprBitmap(t, db, expr)
	if got != want {
		t.Fatalf("count mismatch after patches: got=%d want=%d", got, want)
	}
}

func TestCount_BuildPredicates_DeferBroadRangeMaterialization(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
		countries := []string{"US", "DE", "FR", "GB", "NL", "PL", "SE", "ES"}
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%05d@example.com", i),
			Age:    i,
			Score:  float64(i % 100),
			Active: i%4 != 0,
			Meta: Meta{
				Country: countries[i%len(countries)],
			},
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}
	setNumericBucketKnobs(t, db, 32, 128, 16)

	expr := qx.AND(
		qx.GTE("age", 1_000),
		qx.IN("country", []string{"US", "DE", "FR", "GB"}),
		qx.NOTIN("active", []bool{false}),
	)
	leaves, ok := collectAndLeaves(expr)
	if !ok {
		t.Fatalf("collectAndLeaves failed")
	}
	rangeKey, isSlice, isNil, err := db.exprValueToIdxScalar(qx.Expr{Op: qx.OpGTE, Field: "age", Value: 1_000})
	if err != nil || isSlice || isNil {
		t.Fatalf("exprValueToIdxScalar failed: err=%v isSlice=%v isNil=%v", err, isSlice, isNil)
	}
	cacheKey := db.materializedPredCacheKeyForScalar("age", qx.OpGTE, rangeKey)
	if cacheKey == "" {
		t.Fatalf("expected non-empty materialized cache key for numeric range")
	}
	if _, ok := db.getSnapshot().loadMaterializedPred(cacheKey); ok {
		t.Fatalf("expected empty materialized cache before building predicates")
	}

	predsCount, ok := db.buildCountPredicatesWithMode(leaves, false)
	if !ok {
		t.Fatalf("buildCountPredicatesWithMode failed")
	}
	releasePredicates(predsCount)
	if _, ok := db.getSnapshot().loadMaterializedPred(cacheKey); ok {
		t.Fatalf("count-path predicate build should not eagerly populate numeric range cache")
	}

	predsDefault, ok := db.buildPredicates(leaves)
	if !ok {
		t.Fatalf("buildPredicates failed")
	}
	releasePredicates(predsDefault)
	cached, ok := db.getSnapshot().loadMaterializedPred(cacheKey)
	if !ok || cached.IsEmpty() {
		t.Fatalf("expected default predicate build to populate numeric range cache")
	}
}

func TestCount_BuildPredicates_MergesPositiveNumericRangesSameField(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%05d@example.com", i),
			Age:    i,
			Score:  float64(i % 1_000),
			Active: i%2 == 0,
			Meta: Meta{
				Country: "US",
			},
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	expr := qx.AND(
		qx.GTE("age", 1_000),
		qx.LTE("age", 15_000),
		qx.IN("country", []string{"US"}),
		qx.NOTIN("active", []bool{false}),
	)
	leaves, ok := collectAndLeaves(expr)
	if !ok {
		t.Fatalf("collectAndLeaves failed")
	}
	preds, ok := db.buildCountPredicatesWithMode(leaves, false)
	if !ok {
		t.Fatalf("buildCountPredicatesWithMode failed")
	}
	defer releasePredicates(preds)

	agePreds := 0
	for i := range preds {
		if preds[i].expr.Field == "age" {
			agePreds++
		}
	}
	if agePreds != 1 {
		t.Fatalf("expected merged age predicate, got %d age predicates in %+v", agePreds, preds)
	}
}

func TestCount_PreparePredicate_UsesBroadRangeComplementWithoutCache(t *testing.T) {
	expr := qx.Expr{Op: qx.OpGTE, Field: "age", Value: 1_000}

	t.Run("NoCacheUsesLazyExactPostingFilter", func(t *testing.T) {
		db := countOpenPreparePredicateBroadRangeDB(t, Options{
			AnalyzeInterval:                         -1,
			SnapshotMaterializedPredCacheMaxEntries: -1,
		})
		p, ok := db.buildPredicateWithMode(expr, false)
		if !ok {
			t.Fatalf("buildPredicateWithMode failed")
		}
		defer releasePredicates([]predicate{p})
		if p.kind != predicateKindCustom {
			t.Fatalf("expected initial custom predicate, got kind=%v", p.kind)
		}
		universe := db.snapshotUniverseCardinality()
		if universe < 70_000 {
			t.Fatalf("unexpected small universe=%d", universe)
		}
		if err := db.prepareCountPredicate(&p, universe/2, universe); err != nil {
			t.Fatalf("prepareCountPredicate: %v", err)
		}
		if p.kind != predicateKindCustom {
			t.Fatalf("expected broad no-cache range to stay custom and lazy, got kind=%v", p.kind)
		}
		if !p.supportsPostingApply() || !p.supportsCheapPostingApply() {
			t.Fatalf("expected broad no-cache range to expose cheap exact posting filter")
		}

		src := cloneSnapshotUniverse(t, db)
		defer src.Release()
		want := countByExprBitmap(t, db, expr)
		expectPredicateExactPostingFilterCount(t, p, src, want)
	})
}

func TestCount_PreparePredicate_UsesTinyPostingBroadRangeComplement(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: -1,
	})
	seedGeneratedUint64Data(t, db, 120_000, func(i int) *Rec {
		age := 7
		switch {
		case i < 2_000:
			age = 0
		case i < 4_000:
			age = 1
		case i < 23_000:
			age = 2
		case i < 42_000:
			age = 3
		case i < 61_000:
			age = 4
		case i < 80_000:
			age = 5
		case i < 100_000:
			age = 6
		}
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%05d@example.com", i),
			Age:    age,
			Score:  float64(i % 1_000),
			Active: i%2 == 0,
			Meta: Meta{
				Country: "US",
			},
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	universe := db.snapshotUniverseCardinality()
	if universe < 100_000 {
		t.Fatalf("unexpected small universe=%d", universe)
	}

	for _, tc := range []struct {
		name string
		expr qx.Expr
	}{
		{
			name: "SingleBucket",
			expr: qx.Expr{Op: qx.OpGTE, Field: "age", Value: 1},
		},
		{
			name: "TwoBuckets",
			expr: qx.Expr{Op: qx.OpGTE, Field: "age", Value: 2},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, ok := db.buildPredicateWithMode(tc.expr, false)
			if !ok {
				t.Fatalf("buildPredicateWithMode failed")
			}
			defer releasePredicates([]predicate{p})

			if err := db.prepareCountPredicate(&p, 8_000, universe); err != nil {
				t.Fatalf("prepareCountPredicate: %v", err)
			}
			if p.kind != predicateKindCustom {
				t.Fatalf("expected tiny complement without cache to stay custom, got kind=%v", p.kind)
			}
			if !p.ids.IsEmpty() {
				t.Fatalf("expected tiny complement to avoid postingResult materialization")
			}
			if !p.supportsPostingApply() || !p.supportsCheapPostingApply() {
				t.Fatalf("expected tiny complement predicate to support cheap exact posting filtering")
			}

			src := cloneSnapshotUniverse(t, db)
			defer src.Release()
			want := countByExprBitmap(t, db, tc.expr)
			expectPredicateExactPostingFilterCount(t, p, src, want)
		})
	}
}

func TestCount_PreparePredicate_UsesProbeBoundedBroadRangeComplement(t *testing.T) {
	makeDB := func(t *testing.T, withCache bool) *DB[uint64, Rec] {
		t.Helper()
		opts := Options{AnalyzeInterval: -1}
		if !withCache {
			opts.SnapshotMaterializedPredCacheMaxEntries = -1
		}
		return countOpenPreparePredicateBroadRangeDB(t, opts)
	}

	expr := qx.Expr{Op: qx.OpGTE, Field: "age", Value: 35_000}

	for _, tc := range []struct {
		name      string
		withCache bool
	}{
		{name: "NoCache", withCache: false},
		{name: "WithCache", withCache: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := makeDB(t, tc.withCache)
			buildPrepared := func() predicate {
				t.Helper()
				p, ok := db.buildPredicateWithMode(expr, false)
				if !ok {
					t.Fatalf("buildPredicateWithMode failed")
				}
				universe := db.snapshotUniverseCardinality()
				leadProbeEst := uint64(40_000)
				if err := db.prepareCountPredicate(&p, leadProbeEst, universe); err != nil {
					releasePredicates([]predicate{p})
					t.Fatalf("prepareCountPredicate: %v", err)
				}
				return p
			}

			p := buildPrepared()
			defer releasePredicates([]predicate{p})
			if p.kind != predicateKindCustom {
				t.Fatalf("expected initial custom predicate, got kind=%v", p.kind)
			}

			if tc.withCache {
				if !p.supportsPostingApply() || !p.supportsCheapPostingApply() {
					t.Fatalf("expected first cached broad range sight to stay lazy and exact-filter capable")
				}

				p2 := buildPrepared()
				defer releasePredicates([]predicate{p2})
				if p2.kind != predicateKindMaterializedNot {
					t.Fatalf("expected second cached probe-bounded broad range sight to switch to bitmapNot complement, got kind=%v", p2.kind)
				}
				if p2.ids.IsEmpty() {
					t.Fatalf("expected complement postingResult")
				}
				if got := p2.ids.Cardinality(); got <= countPredBroadRangeComplementMaxCard {
					t.Fatalf("expected complement wider than base cap, got=%d", got)
				}
				key, isSlice, isNil, err := db.exprValueToIdxScalar(expr)
				if err != nil || isSlice || isNil {
					t.Fatalf("exprValueToIdxScalar: err=%v isSlice=%v isNil=%v", err, isSlice, isNil)
				}
				cacheKey := db.materializedPredComplementCacheKeyForScalar(expr.Field, expr.Op, key)
				cached, ok := db.getSnapshot().loadMaterializedPred(cacheKey)
				if !ok || cached.IsEmpty() {
					t.Fatalf("expected complement postingResult to be cached after promotion")
				}
				return
			}

			if p.kind != predicateKindCustom {
				t.Fatalf("expected no-cache broad range to stay custom and lazy, got kind=%v", p.kind)
			}
			if !p.supportsPostingApply() || !p.supportsCheapPostingApply() {
				t.Fatalf("expected no-cache broad range to expose cheap exact posting filter")
			}

			src := cloneSnapshotUniverse(t, db)
			defer src.Release()
			want := countByExprBitmap(t, db, expr)
			expectPredicateExactPostingFilterCount(t, p, src, want)
		})
	}
}

func seedBroadRangeComplementOptData(t *testing.T, db *DB[uint64, Rec], lowCount, highCount, nilCount int) {
	t.Helper()

	total := lowCount + highCount + nilCount
	ids := make([]uint64, 0, 1000)
	vals := make([]*Rec, 0, 1000)
	flush := func() {
		t.Helper()
		if len(ids) == 0 {
			return
		}
		if err := db.BatchSet(ids, vals); err != nil {
			t.Fatalf("BatchSet(seed broad range opt): %v", err)
		}
		ids = ids[:0]
		vals = vals[:0]
	}

	for i := 0; i < total; i++ {
		var opt *string
		switch {
		case i < lowCount:
			v := fmt.Sprintf("a_%05d", i)
			opt = &v
		case i < lowCount+highCount:
			v := fmt.Sprintf("z_%05d", i)
			opt = &v
		}

		rec := &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%05d@example.com", i),
			Age:    i,
			Score:  float64(i % 1000),
			Active: i%2 == 0,
			Meta: Meta{
				Country: "US",
			},
			Opt: opt,
		}
		ids = append(ids, uint64(i+1))
		vals = append(vals, rec)
		if len(ids) == cap(ids) {
			flush()
		}
	}
	flush()
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}
}

func prepareBroadRangeComplementPredicate(t *testing.T, db *DB[uint64, Rec], expr qx.Expr, leadProbeEst uint64) (predicate, string) {
	t.Helper()

	universe := db.snapshotUniverseCardinality()
	p, ok := db.buildPredicateWithMode(expr, false)
	if !ok {
		t.Fatalf("buildPredicateWithMode failed")
	}
	if err := db.prepareCountPredicate(&p, leadProbeEst, universe); err != nil {
		releasePredicates([]predicate{p})
		t.Fatalf("prepareCountPredicate: %v", err)
	}
	if p.kind != predicateKindMaterializedNot {
		releasePredicates([]predicate{p})
		t.Fatalf("expected broad range predicate to materialize complement postingResult, got kind=%v", p.kind)
	}

	key, isSlice, isNil, err := db.exprValueToIdxScalar(expr)
	if err != nil || isSlice || isNil {
		releasePredicates([]predicate{p})
		t.Fatalf("exprValueToIdxScalar: err=%v isSlice=%v isNil=%v", err, isSlice, isNil)
	}
	cacheKey := db.materializedPredComplementCacheKeyForScalar(expr.Field, expr.Op, key)
	if cacheKey == "" {
		releasePredicates([]predicate{p})
		t.Fatalf("expected non-empty complement cache key")
	}
	return p, cacheKey
}

func preparePromotedBroadRangeComplementPredicate(t *testing.T, db *DB[uint64, Rec], expr qx.Expr, leadProbeEst uint64) (predicate, string) {
	t.Helper()

	first, ok := db.buildPredicateWithMode(expr, false)
	if !ok {
		t.Fatalf("buildPredicateWithMode failed")
	}
	universe := db.snapshotUniverseCardinality()
	if err := db.prepareCountPredicate(&first, leadProbeEst, universe); err != nil {
		releasePredicates([]predicate{first})
		t.Fatalf("prepareCountPredicate(first sight): %v", err)
	}
	releasePredicates([]predicate{first})

	return prepareBroadRangeComplementPredicate(t, db, expr, leadProbeEst)
}

func TestCount_PreparePredicate_BroadRangeComplementIncludesNilRows(t *testing.T) {
	db := countOpenBroadRangeComplementOptDB(t, "count_prepare_predicate_broad_range_opt_with_nil.db", true)

	expr := qx.Expr{Op: qx.OpGTE, Field: "opt", Value: "m"}
	p, _ := prepareBroadRangeComplementPredicate(t, db, expr, 3_000)
	defer releasePredicates([]predicate{p})

	src := cloneSnapshotUniverse(t, db)
	defer src.Release()
	want := countByExprBitmap(t, db, expr)
	expectPredicateExactPostingFilterCount(t, p, src, want)
}

func TestCount_PreparePredicate_BroadRangeComplementCacheInvalidatedByNilFieldWrites(t *testing.T) {
	expr := qx.Expr{Op: qx.OpGTE, Field: "opt", Value: "m"}

	t.Run("InsertOnly", func(t *testing.T) {
		db := countOpenBroadRangeComplementOptDB(t, "count_prepare_predicate_broad_range_opt_no_nil.db", false)

		p, cacheKey := prepareBroadRangeComplementPredicate(t, db, expr, 3_000)
		prevSnap := db.getSnapshot()
		prevCached, ok := prevSnap.loadMaterializedPred(cacheKey)
		if !ok || prevCached.IsEmpty() {
			releasePredicates([]predicate{p})
			t.Fatalf("expected cached complement postingResult before nil insert")
		}
		heldSnap, heldRef, ok := db.pinSnapshotRefBySeq(prevSnap.seq)
		if !ok || heldSnap != prevSnap || heldRef == nil {
			releasePredicates([]predicate{p})
			t.Fatalf("pinSnapshotRefBySeq(seq=%d) failed", prevSnap.seq)
		}
		defer db.unpinSnapshotRef(prevSnap.seq, heldRef)
		releasePredicates([]predicate{p})

		if err := db.Set(90_001, &Rec{
			Name:   "nil_insert",
			Email:  "nil_insert@example.test",
			Age:    90_001,
			Score:  1,
			Active: true,
			Meta: Meta{
				Country: "US",
			},
			Opt: nil,
		}); err != nil {
			t.Fatalf("Set(nil insert): %v", err)
		}

		if _, ok := db.getSnapshot().loadMaterializedPred(cacheKey); ok {
			t.Fatalf("expected nil-only insert to invalidate inherited complement cache")
		}

		p, _ = prepareBroadRangeComplementPredicate(t, db, expr, 3_000)
		defer releasePredicates([]predicate{p})
		nextCached, ok := db.getSnapshot().loadMaterializedPred(cacheKey)
		if !ok || nextCached.IsEmpty() {
			t.Fatalf("expected complement cache to be rebuilt after nil insert")
		}
		if nextCached == prevCached {
			t.Fatalf("expected nil-only insert to rebuild complement cache instead of reusing previous postingResult")
		}

		src := cloneSnapshotUniverse(t, db)
		defer src.Release()
		want := countByExprBitmap(t, db, expr)
		expectPredicateExactPostingFilterCount(t, p, src, want)
	})

	t.Run("Delete", func(t *testing.T) {
		db := countOpenBroadRangeComplementOptDB(t, "count_prepare_predicate_broad_range_opt_delete.db", true)

		p, cacheKey := prepareBroadRangeComplementPredicate(t, db, expr, 3_000)
		prevSnap := db.getSnapshot()
		prevCached, ok := prevSnap.loadMaterializedPred(cacheKey)
		if !ok || prevCached.IsEmpty() {
			releasePredicates([]predicate{p})
			t.Fatalf("expected cached complement postingResult before nil delete")
		}
		heldSnap, heldRef, ok := db.pinSnapshotRefBySeq(prevSnap.seq)
		if !ok || heldSnap != prevSnap || heldRef == nil {
			releasePredicates([]predicate{p})
			t.Fatalf("pinSnapshotRefBySeq(seq=%d) failed", prevSnap.seq)
		}
		defer db.unpinSnapshotRef(prevSnap.seq, heldRef)
		releasePredicates([]predicate{p})

		nilID := uint64(80_000)
		if err := db.Delete(nilID); err != nil {
			t.Fatalf("Delete(nil row): %v", err)
		}

		if _, ok := db.getSnapshot().loadMaterializedPred(cacheKey); ok {
			t.Fatalf("expected nil-only delete to invalidate inherited complement cache")
		}

		p, _ = prepareBroadRangeComplementPredicate(t, db, expr, 3_000)
		defer releasePredicates([]predicate{p})
		nextCached, ok := db.getSnapshot().loadMaterializedPred(cacheKey)
		if !ok || nextCached.IsEmpty() {
			t.Fatalf("expected complement cache to be rebuilt after nil delete")
		}
		if nextCached == prevCached {
			t.Fatalf("expected nil-only delete to rebuild complement cache instead of reusing previous postingResult")
		}

		src := cloneSnapshotUniverse(t, db)
		defer src.Release()
		want := countByExprBitmap(t, db, expr)
		expectPredicateExactPostingFilterCount(t, p, src, want)
	})
}

func TestCount_PreparePredicate_BroadRangeComplementMatchesChunkedRangeAfterChurn(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	indexExtBatchSetGenerated(t, db, 1, 500, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("cnt/%03d", i),
			Age:    1000 + i,
			Active: i%3 == 0,
		}
	})

	for _, id := range []uint64{110, 111, 112, 113, 114, 115} {
		if err := db.Delete(id); err != nil {
			t.Fatalf("Delete(%d): %v", id, err)
		}
	}
	for _, tc := range []struct {
		id     uint64
		age    int
		active bool
	}{
		{id: 30, age: 1100, active: true},
		{id: 31, age: 1099, active: true},
		{id: 32, age: 1260, active: true},
		{id: 33, age: 1259, active: false},
		{id: 34, age: 1180, active: true},
	} {
		if err := db.Patch(tc.id, []Field{
			{Name: "age", Value: tc.age},
			{Name: "active", Value: tc.active},
		}); err != nil {
			t.Fatalf("Patch(%d): %v", tc.id, err)
		}
	}

	if db.fieldOverlay("age").chunked == nil {
		t.Fatalf("expected age field to use chunked storage")
	}

	src := cloneSnapshotUniverse(t, db)
	defer src.Release()

	for _, tc := range []struct {
		name string
		expr qx.Expr
	}{
		{
			name: "GTE",
			expr: qx.Expr{Op: qx.OpGTE, Field: "age", Value: 1100},
		},
		{
			name: "LT",
			expr: qx.Expr{Op: qx.OpLT, Field: "age", Value: 1260},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := preparePromotedBroadRangeComplementPredicate(t, db, tc.expr, 166)
			defer releasePredicates([]predicate{p})

			want := countByExprBitmap(t, db, tc.expr)
			expectPredicateExactPostingFilterCount(t, p, src, want)
		})
	}
}

func TestCount_PreparePredicate_KeepsLegacyCapForComparableBroadLead(t *testing.T) {
	db := countOpenPreparePredicateBroadRangeDB(t, Options{AnalyzeInterval: -1})

	expr := qx.Expr{Op: qx.OpGTE, Field: "age", Value: 35_000}
	p, ok := db.buildPredicateWithMode(expr, false)
	if !ok {
		t.Fatalf("buildPredicateWithMode failed")
	}
	defer releasePredicates([]predicate{p})
	if p.kind != predicateKindCustom {
		t.Fatalf("expected initial custom predicate, got kind=%v", p.kind)
	}

	universe := db.snapshotUniverseCardinality()
	leadProbeEst := uint64(80_000)
	if err := db.prepareCountPredicate(&p, leadProbeEst, universe); err != nil {
		t.Fatalf("prepareCountPredicate: %v", err)
	}
	if p.kind != predicateKindCustom {
		t.Fatalf("expected comparable lead to keep base complement cap, got kind=%v", p.kind)
	}
}

func TestCount_BroadRangeComplementFastMaterializationGate(t *testing.T) {
	probe := make([]posting.List, 0, 5_000)
	for i := 0; i < 5_000; i++ {
		probe = append(probe, queryCountSingleton(uint64(i)))
	}
	if !shouldUseFastCountBroadRangeComplementMaterializationForShape(len(probe), 5_000) {
		t.Fatalf("expected many tiny buckets to use fast complement materialization")
	}
	if shouldUseFastCountBroadRangeComplementMaterializationForShape(len(probe[:2_000]), 2_000) {
		t.Fatalf("expected short probe to keep regular complement materialization")
	}
	if shouldUseFastCountBroadRangeComplementMaterializationForShape(len(probe), 90_000) {
		t.Fatalf("expected dense buckets to keep regular complement materialization")
	}
}

func TestCount_PreparePredicate_UsesExactNumericEstimateForSkewedBroadRange(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: -1,
	})

	db.DisableSync()
	defer db.EnableSync()

	var (
		id        uint64
		wantIn    uint64
		wantOut   uint64
		sampleA   = 1_000
		sampleB   = 5_500
		sampleC   = 9_999
		rangeFrom = 1_000
	)
	const batchSize = 32 << 10
	batchIDs := make([]uint64, 0, batchSize)
	batchVals := make([]*Rec, 0, batchSize)
	flush := func() {
		if len(batchIDs) == 0 {
			return
		}
		if err := db.BatchSet(batchIDs, batchVals); err != nil {
			t.Fatalf("BatchSet(seed batch=%d): %v", len(batchIDs), err)
		}
		batchIDs = batchIDs[:0]
		batchVals = batchVals[:0]
	}

	for age := 0; age < 10_000; age++ {
		reps := 10
		if age < rangeFrom {
			reps = 3
		}
		if age == sampleA || age == sampleB || age == sampleC {
			reps = 1
		}
		for rep := 0; rep < reps; rep++ {
			id++
			batchIDs = append(batchIDs, id)
			batchVals = append(batchVals, &Rec{
				Name:   fmt.Sprintf("u_%d_%d", age, rep),
				Email:  fmt.Sprintf("user_%d_%d@example.com", age, rep),
				Age:    age,
				Score:  float64(rep),
				Active: rep%2 == 0,
				Meta: Meta{
					Country: "US",
				},
			})
			if len(batchIDs) == cap(batchIDs) {
				flush()
			}
		}
		if age >= rangeFrom {
			wantIn += uint64(reps)
		} else {
			wantOut += uint64(reps)
		}
	}
	flush()
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	setNumericBucketKnobs(t, db, 128, 1, 1)

	expr := qx.Expr{Op: qx.OpGTE, Field: "age", Value: rangeFrom}
	p, ok := db.buildPredicateWithMode(expr, false)
	if !ok {
		t.Fatalf("buildPredicateWithMode failed")
	}
	defer releasePredicates([]predicate{p})

	if p.kind != predicateKindCustom {
		t.Fatalf("expected initial custom predicate, got kind=%v", p.kind)
	}
	if p.estCard != wantIn {
		t.Fatalf("expected exact numeric range estimate, got=%d want=%d", p.estCard, wantIn)
	}

	universe := db.snapshotUniverseCardinality()
	if universe != wantIn+wantOut {
		t.Fatalf("unexpected universe: got=%d want=%d", universe, wantIn+wantOut)
	}
	if err := db.prepareCountPredicate(&p, 3_000, universe); err != nil {
		t.Fatalf("prepareCountPredicate: %v", err)
	}
	if p.kind != predicateKindCustom {
		t.Fatalf("expected skewed broad numeric range without cache to stay custom, got kind=%v", p.kind)
	}
	if !p.supportsPostingApply() || !p.supportsCheapPostingApply() {
		t.Fatalf("expected skewed broad numeric range to expose cheap exact posting filter")
	}

	src := cloneSnapshotUniverse(t, db)
	defer src.Release()
	expectPredicateExactPostingFilterCount(t, p, src, wantIn)
}

func TestCount_PreparePredicate_UsesBroadRangeComplementOnFullSnapshot(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: -1,
	})

	db.DisableSync()
	defer db.EnableSync()

	var (
		id        uint64
		wantIn    uint64
		wantOut   uint64
		rangeFrom = 1_000
	)
	const batchSize = 32 << 10
	batchIDs := make([]uint64, 0, batchSize)
	batchVals := make([]*Rec, 0, batchSize)
	flush := func() {
		if len(batchIDs) == 0 {
			return
		}
		if err := db.BatchSet(batchIDs, batchVals); err != nil {
			t.Fatalf("BatchSet(seed batch=%d): %v", len(batchIDs), err)
		}
		batchIDs = batchIDs[:0]
		batchVals = batchVals[:0]
	}

	for age := 0; age < 10_000; age++ {
		reps := 10
		if age < rangeFrom {
			reps = 3
		}
		for rep := 0; rep < reps; rep++ {
			id++
			batchIDs = append(batchIDs, id)
			batchVals = append(batchVals, &Rec{
				Name:   fmt.Sprintf("overlay_%d_%d", age, rep),
				Email:  fmt.Sprintf("overlay_%d_%d@example.com", age, rep),
				Age:    age,
				Score:  float64(rep),
				Active: rep%2 == 0,
				Meta: Meta{
					Country: "US",
				},
			})
			if len(batchIDs) == cap(batchIDs) {
				flush()
			}
		}
		if age >= rangeFrom {
			wantIn += uint64(reps)
		} else {
			wantOut += uint64(reps)
		}
	}
	flush()

	setNumericBucketKnobs(t, db, 128, 1, 1)

	expr := qx.Expr{Op: qx.OpGTE, Field: "age", Value: rangeFrom}
	p, ok := db.buildPredicateWithMode(expr, false)
	if !ok {
		t.Fatalf("buildPredicateWithMode failed")
	}
	defer releasePredicates([]predicate{p})

	if p.kind != predicateKindCustom {
		t.Fatalf("expected initial custom predicate, got kind=%v", p.kind)
	}
	if p.estCard != wantIn {
		t.Fatalf("expected exact overlay range estimate, got=%d want=%d", p.estCard, wantIn)
	}

	universe := db.snapshotUniverseCardinality()
	if universe != wantIn+wantOut {
		t.Fatalf("unexpected universe: got=%d want=%d", universe, wantIn+wantOut)
	}
	if err := db.prepareCountPredicate(&p, 3_000, universe); err != nil {
		t.Fatalf("prepareCountPredicate: %v", err)
	}
	if p.kind != predicateKindCustom {
		t.Fatalf("expected broad numeric range without cache to stay custom, got kind=%v", p.kind)
	}
	if !p.supportsPostingApply() || !p.supportsCheapPostingApply() {
		t.Fatalf("expected broad numeric range to expose cheap exact posting filter")
	}

	src := cloneSnapshotUniverse(t, db)
	defer src.Release()
	expectPredicateExactPostingFilterCount(t, p, src, wantIn)
}

func TestCount_PickLeadPredicate_PrefersLeadThatUnlocksCustomResidualMaterialization(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: -1,
	})

	seedGeneratedUint64Data(t, db, 40_000, func(i int) *Rec {
		active := i <= 7_000

		country := "FR"
		switch {
		case i <= 4_500 || (i > 20_000 && i <= 24_500):
			country = "US"
		case (i > 4_500 && i <= 9_000) || (i > 24_500 && i <= 29_000):
			country = "DE"
		}

		name := fmt.Sprintf("user-%05d", i)
		if i <= 30_000 {
			name = "needle-" + name
		}

		return &Rec{
			Name:   name,
			Email:  fmt.Sprintf("user%05d@example.com", i),
			Age:    20 + (i % 40),
			Score:  float64(i % 1_000),
			Active: active,
			Meta: Meta{
				Country: country,
			},
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	expr := qx.AND(
		qx.EQ("active", true),
		qx.IN("country", []string{"US", "DE"}),
		qx.CONTAINS("name", "needle"),
	)
	leaves, ok := collectAndLeaves(expr)
	if !ok {
		t.Fatalf("collectAndLeaves failed")
	}
	preds, ok := db.buildPredicatesWithMode(leaves, false)
	if !ok {
		t.Fatalf("buildPredicatesWithMode failed")
	}
	defer releasePredicates(preds)

	activeIdx := -1
	countryIdx := -1
	containsIdx := -1
	oldLeadIdx := -1
	oldLeadEst := uint64(0)
	oldLeadScore := 0.0
	for i := range preds {
		p := preds[i]
		if p.expr.Field == "active" && p.expr.Op == qx.OpEQ {
			activeIdx = i
		}
		if p.expr.Field == "country" && p.expr.Op == qx.OpIN {
			countryIdx = i
		}
		if p.expr.Field == "name" && p.expr.Op == qx.OpCONTAINS {
			containsIdx = i
		}
		if p.alwaysTrue || p.covered || !p.hasIter() || p.estCard == 0 {
			continue
		}
		score := float64(p.estCard) * float64(countPredicateLeadWeight(p))
		if oldLeadIdx == -1 || score < oldLeadScore {
			oldLeadIdx = i
			oldLeadEst = p.estCard
			oldLeadScore = score
		}
	}
	if activeIdx < 0 || countryIdx < 0 || containsIdx < 0 {
		t.Fatalf("expected active/country/contains predicates, got active=%d country=%d contains=%d", activeIdx, countryIdx, containsIdx)
	}
	if oldLeadIdx != activeIdx {
		t.Fatalf("expected baseline score to prefer active EQ lead, got idx=%d field=%s op=%v", oldLeadIdx, preds[oldLeadIdx].expr.Field, preds[oldLeadIdx].expr.Op)
	}

	universe := db.snapshotUniverseCardinality()
	if universe == 0 {
		t.Fatalf("expected non-empty universe")
	}

	containsWithOldLead := preds[containsIdx]
	defer releasePredicates([]predicate{containsWithOldLead})
	if err := db.prepareCountPredicate(&containsWithOldLead, oldLeadEst, universe); err != nil {
		t.Fatalf("prepareCountPredicate(old lead): %v", err)
	}
	if containsWithOldLead.kind != predicateKindCustom {
		t.Fatalf("expected old lead probe to keep CONTAINS residual custom, got kind=%v", containsWithOldLead.kind)
	}

	containsWithCountryLead := preds[containsIdx]
	defer releasePredicates([]predicate{containsWithCountryLead})
	if err := db.prepareCountPredicate(&containsWithCountryLead, preds[countryIdx].estCard, universe); err != nil {
		t.Fatalf("prepareCountPredicate(country lead): %v", err)
	}
	if containsWithCountryLead.kind != predicateKindMaterialized {
		t.Fatalf("expected broader IN lead probe to materialize CONTAINS residual, got kind=%v", containsWithCountryLead.kind)
	}

	leadIdx, leadEst, _ := db.pickCountLeadPredicate(preds, universe)
	if leadIdx != countryIdx {
		t.Fatalf("expected residual-aware lead pick to prefer country IN, got idx=%d field=%s op=%v", leadIdx, preds[leadIdx].expr.Field, preds[leadIdx].expr.Op)
	}
	if leadEst != preds[countryIdx].estCard {
		t.Fatalf("unexpected chosen lead est: got=%d want=%d", leadEst, preds[countryIdx].estCard)
	}

	got, ok, err := db.tryCountByPredicates(expr, nil)
	if err != nil {
		t.Fatalf("tryCountByPredicates: %v", err)
	}
	if !ok {
		t.Fatalf("expected tryCountByPredicates fast path")
	}
	want := countByExprBitmap(t, db, expr)
	if got != want {
		t.Fatalf("count mismatch: got=%d want=%d", got, want)
	}
}

func TestCount_BroadMaterializedLeadCheapResiduals_UsesPredicateScan(t *testing.T) {
	opts := benchOptions()
	opts.AnalyzeInterval = -1

	path := filepath.Join(t.TempDir(), "bench.db")
	db, raw := openBoltAndNew[uint64, UserBench](t, path, opts)
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	seedBenchData(t, db, 50_000)
	db.clearCurrentSnapshotCachesForTesting()

	expr := qx.AND(
		qx.NOTIN("status", []string{"banned"}),
		qx.IN("country", []string{"NL", "DE", "PL", "SE", "FR", "ES", "GB"}),
		qx.GTE("age", 25),
		qx.LTE("age", 45),
		qx.HASANY("tags", []string{"go", "db", "security"}),
		qx.GTE("score", 80.0),
	)
	q := qx.Query(expr)

	got, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	want := countByExprBitmapUserBench(t, db, expr)
	if got != want {
		t.Fatalf("count mismatch: got=%d want=%d", got, want)
	}

	gotFast, ok, err := db.tryCountByPredicates(expr, nil)
	if err != nil {
		t.Fatalf("tryCountByPredicates: %v", err)
	}
	if !ok {
		t.Fatalf("expected broad-lead count predicate fast path")
	}
	if gotFast != want {
		t.Fatalf("fast-path count mismatch: got=%d want=%d", gotFast, want)
	}
}

func TestCount_LeadPostingsSingleResidualBucketCount_AllocsPerRunStayZeroAfterWarmup(t *testing.T) {
	if testRaceEnabled {
		t.Skip("testing.AllocsPerRun is not stable under -race")
	}

	opts := benchOptions()
	opts.AnalyzeInterval = -1

	path := filepath.Join(t.TempDir(), "bench.db")
	db, raw := openBoltAndNew[uint64, UserBench](t, path, opts)
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	seedBenchData(t, db, 100_000)

	q := qx.Query(
		qx.PREFIX("email", "user"),
		qx.EQ("status", "active"),
		qx.NOTIN("plan", []string{"free"}),
	)
	expr, _ := normalize.Expr(q.Expr)

	run := func() {
		qv := db.makeQueryView(db.getSnapshot())
		defer db.releaseQueryView(qv)
		var leavesBuf [countPredicateScanMaxLeaves]qx.Expr
		leaves, ok := collectAndLeavesScratch(expr, leavesBuf[:0])
		if !ok {
			t.Fatalf("collectAndLeavesScratch=false")
		}

		predSet, ok := qv.buildCountPredicatesWithMode(leaves, false)
		if !ok {
			t.Fatalf("buildCountPredicatesWithMode=false")
		}
		defer predSet.Release()

		universe := qv.snapshotUniverseCardinality()
		leadIdx, leadEst, leadScore := qv.pickCountLeadPredicate(predSet, universe)
		if leadIdx < 0 {
			t.Fatalf("leadIdx < 0")
		}
		if shouldPreferMaterializedCountEval(predSet, leadScore, universe) {
			t.Fatalf("expected predicate count path")
		}
		if err := qv.prepareCountPredicateWithTrace(predSet.GetPtr(leadIdx), leadEst, universe, nil); err != nil {
			t.Fatalf("prepareCountPredicate(lead): %v", err)
		}
		if predSet.Get(leadIdx).estCard > 0 {
			leadEst = predSet.Get(leadIdx).estCard
		}
		leadNeedsCheck := predSet.GetPtr(leadIdx).leadIterNeedsContainsCheck()

		var activeBuf [countPredicateScanMaxLeaves]int
		active := activeBuf[:0]
		for i := 0; i < predSet.Len(); i++ {
			if i == leadIdx && !leadNeedsCheck {
				continue
			}
			p := predSet.Get(i)
			if p.covered || p.alwaysTrue {
				continue
			}
			if p.alwaysFalse {
				t.Fatalf("unexpected alwaysFalse predicate")
			}
			active = append(active, i)
		}
		if !predSet.GetPtr(leadIdx).isMaterializedLike() && qv.shouldRejectBroadLeadPredicateScan(predSet, active, leadEst, universe) {
			t.Fatalf("unexpected broad lead predicate scan rejection")
		}
		for _, pi := range active {
			if err := qv.prepareCountPredicateWithTrace(predSet.GetPtr(pi), leadEst, universe, nil); err != nil {
				t.Fatalf("prepareCountPredicate(active=%d): %v", pi, err)
			}
		}

		write := 0
		for _, pi := range active {
			p := predSet.Get(pi)
			if p.covered || p.alwaysTrue {
				continue
			}
			if p.alwaysFalse {
				t.Fatalf("unexpected alwaysFalse active predicate")
			}
			if !p.hasContains() {
				t.Fatalf("expected count predicate path to keep contains checks")
			}
			active[write] = pi
			write++
		}
		active = active[:write]
		sortActivePredicatesSet(active, predSet)

		cnt, examined, ok := qv.tryCountByPredicatesLeadPostings(predSet, leadIdx, active)
		if !ok {
			t.Fatalf("tryCountByPredicatesLeadPostings=false")
		}
		if cnt == 0 || examined == 0 {
			t.Fatalf("unexpected cnt=%d examined=%d", cnt, examined)
		}
	}

	run()
	allocs := testing.AllocsPerRun(50, run)
	if allocs != 0 {
		t.Fatalf("unexpected allocs after warmup: got=%v want=0", allocs)
	}
}

func TestCountApplyLeadResidualExactFilters_StateOnlyAllocsPerRunStayZeroAfterWarmup(t *testing.T) {
	if testRaceEnabled {
		t.Skip("testing.AllocsPerRun is not stable under -race")
	}

	src := posting.BuildFromSorted([]uint64{
		3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33,
		35, 37, 39, 41, 43, 45, 47, 49, 51, 53, 55, 57, 59, 61, 63, 65,
	})
	defer src.Release()

	postA := posting.BuildFromSorted([]uint64{7, 11, 19, 31, 43, 55, 63})
	postB := posting.BuildFromSorted([]uint64{5, 11, 17, 23, 31, 41, 47, 59})
	postC := posting.BuildFromSorted([]uint64{3, 9, 15, 21, 27, 33, 39, 45, 51, 57, 63})
	postD := posting.BuildFromSorted([]uint64{11, 21, 31, 41, 51, 61})
	defer postA.Release()
	defer postB.Release()
	defer postC.Release()
	defer postD.Release()

	postsBuf1 := postingSlicePool.Get()
	postsBuf1.Append(postA)
	postsBuf1.Append(postB)
	state1 := postsAnyFilterStatePool.Get()
	state1.postsBuf = postsBuf1

	postsBuf2 := postingSlicePool.Get()
	postsBuf2.Append(postC)
	postsBuf2.Append(postD)
	state2 := postsAnyFilterStatePool.Get()
	state2.postsBuf = postsBuf2

	filters := countLeadResidualExactFilterSlicePool.Get()
	filters.Append(countLeadResidualExactFilter{idx: 0, state: state1})
	filters.Append(countLeadResidualExactFilter{idx: 1, state: state2})

	defer func() {
		countLeadResidualExactFilterSlicePool.Put(filters)
		postsAnyFilterStatePool.Put(state1)
		postsAnyFilterStatePool.Put(state2)
		postingSlicePool.Put(postsBuf1)
		postingSlicePool.Put(postsBuf2)
	}()

	var work posting.List
	defer work.Release()

	run := func() {
		out, nextWork := countApplyLeadResidualExactFilters(src.Borrow(), work, filters)
		work = nextWork
		if out.IsEmpty() {
			t.Fatalf("expected non-empty filtered posting")
		}
		if got := out.Cardinality(); got != 4 {
			t.Fatalf("unexpected filtered cardinality: got=%d want=4", got)
		}
	}

	run()
	allocs := testing.AllocsPerRun(100, run)
	if allocs != 0 {
		t.Fatalf("unexpected allocs after warmup: got=%v want=0", allocs)
	}
}

func TestCountApplyLeadResidualExactFilters_MixedStateAndIDsMatchesExpected(t *testing.T) {
	src := posting.BuildFromSorted([]uint64{
		3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33,
		35, 37, 39, 41, 43, 45, 47, 49, 51, 53, 55, 57, 59, 61, 63, 65,
	})
	defer src.Release()

	postA := posting.BuildFromSorted([]uint64{7, 11, 19, 31, 43, 55, 63})
	postB := posting.BuildFromSorted([]uint64{5, 11, 17, 23, 31, 41, 47, 59})
	exact := posting.BuildFromSorted([]uint64{7, 11, 19, 23, 31, 37, 43, 59})
	defer postA.Release()
	defer postB.Release()
	defer exact.Release()

	postsBuf := postingSlicePool.Get()
	postsBuf.Append(postA)
	postsBuf.Append(postB)
	state := postsAnyFilterStatePool.Get()
	state.postsBuf = postsBuf

	filters := countLeadResidualExactFilterSlicePool.Get()
	filters.Append(countLeadResidualExactFilter{idx: 0, state: state})
	filters.Append(countLeadResidualExactFilter{idx: 1, ids: exact})

	defer func() {
		countLeadResidualExactFilterSlicePool.Put(filters)
		postsAnyFilterStatePool.Put(state)
		postingSlicePool.Put(postsBuf)
	}()

	out, nextWork := countApplyLeadResidualExactFilters(src.Borrow(), posting.List{}, filters)
	defer nextWork.Release()

	if out.IsEmpty() {
		t.Fatalf("expected non-empty filtered posting")
	}
	if got := out.Cardinality(); got != 7 {
		t.Fatalf("unexpected filtered cardinality: got=%d want=7", got)
	}

	want := []uint64{7, 11, 19, 23, 31, 43, 59}
	it := out.Iter()
	defer it.Release()
	for i := 0; i < len(want); i++ {
		if !it.HasNext() {
			t.Fatalf("missing filtered id at pos=%d want=%d", i, want[i])
		}
		if got := it.Next(); got != want[i] {
			t.Fatalf("unexpected filtered id at pos=%d: got=%d want=%d", i, got, want[i])
		}
	}
	if it.HasNext() {
		t.Fatalf("unexpected extra filtered id=%d", it.Next())
	}
	if !out.SharesPayload(nextWork) {
		t.Fatalf("expected reusable work to keep filtered posting payload")
	}
}

func TestCount_PickORLeadPredicate_PrefersPrefixLeadOverCheapEqLead(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: -1,
	})

	seedGeneratedUint64Data(t, db, 40_000, func(i int) *Rec {
		email := fmt.Sprintf("mail-%05d@example.com", i)
		if i < 10_000 {
			email = fmt.Sprintf("user1-%05d@example.com", i)
		}

		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  email,
			Age:    20 + (i % 30),
			Score:  float64(i % 100),
			Active: i < 18_000,
			Meta: Meta{
				Country: "US",
			},
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	expr := qx.AND(
		qx.PREFIX("email", "user1-"),
		qx.EQ("active", true),
		qx.GTE("score", 60.0),
	)
	leaves, ok := collectAndLeaves(expr)
	if !ok {
		t.Fatalf("collectAndLeaves failed")
	}
	preds, ok := db.buildPredicatesWithMode(leaves, false)
	if !ok {
		t.Fatalf("buildPredicatesWithMode failed")
	}
	defer releasePredicates(preds)

	prefixIdx := -1
	activeIdx := -1
	oldLeadIdx := -1
	oldLeadScore := 0.0
	for i := range preds {
		p := preds[i]
		if p.expr.Field == "email" && p.expr.Op == qx.OpPREFIX {
			prefixIdx = i
		}
		if p.expr.Field == "active" && p.expr.Op == qx.OpEQ {
			activeIdx = i
		}
		if p.alwaysTrue || p.covered || !p.hasIter() || p.estCard == 0 {
			continue
		}
		score := float64(p.estCard) * float64(countLeadOpWeight(p.expr.Op))
		if oldLeadIdx == -1 || score < oldLeadScore {
			oldLeadIdx = i
			oldLeadScore = score
		}
	}
	if prefixIdx < 0 || activeIdx < 0 {
		t.Fatalf("expected prefix and active predicates, got prefix=%d active=%d", prefixIdx, activeIdx)
	}
	if oldLeadIdx != activeIdx {
		t.Fatalf("expected old OR lead score to prefer active EQ lead, got idx=%d field=%s op=%v", oldLeadIdx, preds[oldLeadIdx].expr.Field, preds[oldLeadIdx].expr.Op)
	}

	universe := db.snapshotUniverseCardinality()
	leadIdx, leadEst, _ := db.pickCountORLeadPredicate(preds, universe)
	if leadIdx != prefixIdx {
		t.Fatalf("expected OR-specific lead score to prefer prefix lead, got idx=%d field=%s op=%v", leadIdx, preds[leadIdx].expr.Field, preds[leadIdx].expr.Op)
	}
	if leadEst != preds[prefixIdx].estCard {
		t.Fatalf("unexpected chosen OR lead est: got=%d want=%d", leadEst, preds[prefixIdx].estCard)
	}
}

func TestCountLeavesForUniquePath_ReusesScratchForSingleLeaf(t *testing.T) {
	expr := qx.EQ("email", "a@x")
	var buf [4]qx.Expr

	leaves, ok := countLeavesForUniquePath(expr, buf[:0])
	if !ok {
		t.Fatalf("countLeavesForUniquePath: ok=false")
	}
	if len(leaves) != 1 {
		t.Fatalf("unexpected leaves len: %d", len(leaves))
	}
	if cap(leaves) != len(buf) {
		t.Fatalf("expected scratch-backed slice cap=%d, got %d", len(buf), cap(leaves))
	}
	if leaves[0].Op != expr.Op || leaves[0].Field != expr.Field || leaves[0].Value != expr.Value || leaves[0].Not != expr.Not {
		t.Fatalf("unexpected leaf: got=%v want=%v", leaves[0], expr)
	}
	leaves[0].Field = "other"
	if buf[0].Field != "other" {
		t.Fatalf("expected returned slice to reuse caller scratch")
	}
}

func TestDumpCountSecurityAuditColdTurnoverCPUProfile(t *testing.T) {
	profilePath := os.Getenv("RBI_SECURITY_AUDIT_CPU_PROFILE")
	if profilePath == "" {
		t.Skip("set RBI_SECURITY_AUDIT_CPU_PROFILE to dump a cold-turnover CPU profile")
	}

	path := filepath.Join(t.TempDir(), "security_audit_profile.db")
	db, raw := openBoltAndNew[uint64, UserBench](t, path, benchOptions())
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	seedBenchData(t, db, benchN)
	q := qx.Query(
		qx.HASANY("roles", []string{"admin", "moderator"}),
		qx.NOTIN("status", []string{"banned"}),
		qx.GTE("score", 200.0),
		qx.IN("country", []string{"US", "DE", "PL"}),
	)

	ring := buildUserBenchTurnoverRingUint64(t, db)
	state := newBenchReadModeState[uint64, UserBench](
		t,
		benchCacheMode{suffix: "ColdTurnover", kind: benchCacheModeColdTurnover},
		ring,
	)
	t.Cleanup(func() { state.close(t, db) })

	for i := 0; i < 12; i++ {
		pprof.Do(context.Background(), pprof.Labels("phase", "turnover"), func(context.Context) {
			state.applyTurnover(t, db)
		})
		pprof.Do(context.Background(), pprof.Labels("phase", "query"), func(context.Context) {
			if _, err := db.Count(q); err != nil {
				t.Fatalf("warm Count: %v", err)
			}
		})
	}

	f, err := os.Create(profilePath)
	if err != nil {
		t.Fatalf("Create(cpu profile): %v", err)
	}
	defer func() {
		_ = f.Close()
	}()
	if err := pprof.StartCPUProfile(f); err != nil {
		t.Fatalf("StartCPUProfile: %v", err)
	}
	defer pprof.StopCPUProfile()

	for i := 0; i < 128; i++ {
		pprof.Do(context.Background(), pprof.Labels("phase", "turnover"), func(context.Context) {
			state.applyTurnover(t, db)
		})
		pprof.Do(context.Background(), pprof.Labels("phase", "query"), func(context.Context) {
			if _, err := db.Count(q); err != nil {
				t.Fatalf("profiled Count: %v", err)
			}
		})
	}
}
