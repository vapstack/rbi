package rbi

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/roaring64"
)

func countByExprBitmap(t *testing.T, db *DB[uint64, Rec], expr qx.Expr) uint64 {
	t.Helper()
	b, err := db.evalExpr(expr)
	if err != nil {
		t.Fatalf("evalExpr: %v", err)
	}
	defer b.release()
	return db.countBitmapResult(b)
}

func countByExprBitmapBenchRec(t *testing.T, db *DB[uint64, countORBenchRec], expr qx.Expr) uint64 {
	t.Helper()
	b, err := db.evalExpr(expr)
	if err != nil {
		t.Fatalf("evalExpr: %v", err)
	}
	defer b.release()
	return db.countBitmapResult(b)
}

func countByExprBitmapUserBench(t *testing.T, db *DB[uint64, UserBench], expr qx.Expr) uint64 {
	t.Helper()
	b, err := db.evalExpr(expr)
	if err != nil {
		t.Fatalf("evalExpr: %v", err)
	}
	defer b.release()
	return db.countBitmapResult(b)
}

func expectPredicateExactBitmapFilterCount(t *testing.T, p predicate, src *roaring64.Bitmap, want uint64) {
	t.Helper()

	work := getRoaringBuf()
	defer releaseRoaringBuf(work)

	mode, exactBM, _ := plannerFilterBitmapByChecks([]predicate{p}, []int{0}, src, work, true)
	if mode != plannerPredicateBucketExact {
		t.Fatalf("expected exact bitmap filtering, got mode=%v", mode)
	}
	if exactBM == nil {
		t.Fatalf("expected exact filtered bitmap")
	}
	if got := exactBM.GetCardinality(); got != want {
		t.Fatalf("unexpected exact filtered cardinality: got=%d want=%d", got, want)
	}
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
		AnalyzeInterval:                                  -1,
		SnapshotMaterializedPredCacheMaxEntries:          -1,
		SnapshotMaterializedPredCacheMaxEntriesWithDelta: -1,
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

	activeBuf := getIntSliceBuf(len(preds))
	active := activeBuf.values[:0]
	defer func() {
		activeBuf.values = active
		releaseIntSliceBuf(activeBuf)
	}()
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

	exactActiveBuf := getIntSliceBuf(len(active))
	exactActive := buildBitmapFilterActive(exactActiveBuf.values, active, preds)
	defer func() {
		exactActiveBuf.values = exactActive
		releaseIntSliceBuf(exactActiveBuf)
	}()
	if countIndexSliceContains(exactActive, hasAnyIdx) {
		t.Fatalf("expected HASANY residual to stay out of bitmap-filter subset, got=%v", exactActive)
	}

	extraExact := db.buildCountLeadResidualExactFilters(preds, active)
	defer releaseCountLeadResidualExactFilters(extraExact)
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

	activeBuf := getIntSliceBuf(len(preds))
	active := activeBuf.values[:0]
	defer func() {
		activeBuf.values = active
		releaseIntSliceBuf(activeBuf)
	}()
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

func TestCount_ByPredicates_BroadLeadFallbackSkipsResidualPreparation(t *testing.T) {
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
	seedGeneratedUint64Data(t, db, 160_000, func(i int) *Rec {
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
		qx.LTE("age", 150_000),
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
	if ev.Plan != string(PlanCountBitmap) {
		t.Fatalf("expected broad-lead fallback to use %q, got %q", PlanCountBitmap, ev.Plan)
	}
	if ev.CountPredicatePreparations > 1 {
		t.Fatalf("expected broad-lead fallback to avoid residual preparation, got %d total preparations", ev.CountPredicatePreparations)
	}
}

func TestCount_ORByPredicates_PostingLeadResidualExactFilters_MatchesBitmap(t *testing.T) {
	dir := t.TempDir()
	db, raw := openBoltAndNew[uint64, countORBenchRec](t, filepath.Join(dir, "test_count_or.db"), Options{
		AnalyzeInterval:                      -1,
		SnapshotCompactorRequestEveryNWrites: 1 << 30,
		SnapshotCompactorIdleInterval:        -1,
		SnapshotDeltaLayerMaxDepth:           1 << 30,
	})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	db.DisableSync()
	defer db.EnableSync()

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

	const n = 200_000
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
	want := db.countBitmapResult(b)
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
	if ev.Plan != string(PlanCountBitmap) {
		t.Fatalf("expected strict-wide OR fallback to use %q, got %q", PlanCountBitmap, ev.Plan)
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

	activeBuf := getIntSliceBuf(len(preds))
	active := activeBuf.values[:0]
	defer func() {
		activeBuf.values = active
		releaseIntSliceBuf(activeBuf)
	}()
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

func TestCount_ORByPredicates_HybridBitmapSpill_MatchesBitmap(t *testing.T) {
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
		if br.SkipReason == "bitmap_spill" {
			spilled = true
			break
		}
	}
	if !spilled {
		t.Fatalf("expected at least one hybrid bitmap spill branch, got trace=%+v", ev.ORBranches)
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
	if ev.ORBranches[0].SkipReason != "bitmap_spill" {
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
	highOverlap := countORBranches{
		{est: 90_000},
		{est: 85_000},
		{est: 80_000},
		{est: 75_000},
		{est: 70_000},
	}
	if !highOverlap.shouldUseSeenDedup(100_000, 250_000) {
		t.Fatalf("expected seen dedup for high-overlap wide OR")
	}

	lowOverlap := countORBranches{
		{est: 25_000},
		{est: 25_000},
		{est: 25_000},
		{est: 25_000},
	}
	if lowOverlap.shouldUseSeenDedup(2_000_000, 120_000) {
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
	setPred := predicate{
		kind:  predicateKindPostsAny,
		posts: make([]postingList, 12),
	}
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
		kind:    predicateKindPostsAny,
		expr:    qx.Expr{Op: qx.OpHASANY, Field: "roles"},
		posts:   make([]postingList, 3),
		estCard: 80_000,
	}
	if !shouldUseCountLeadResidualHasAnyExactFilter(smallHasAnyResidual) {
		t.Fatalf("expected small HASANY residual exact filter above threshold")
	}
	if shouldUseCountLeadResidualHasAnyExactFilter(predicate{
		kind:    predicateKindPostsAny,
		expr:    qx.Expr{Op: qx.OpHASANY, Field: "roles"},
		posts:   make([]postingList, 4),
		estCard: 80_000,
	}) {
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

func TestCount_ScalarINSplit_WorksWithFieldDelta(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		SnapshotCompactorRequestEveryNWrites: 1 << 30,
		SnapshotCompactorIdleInterval:        -1,
		SnapshotDeltaLayerMaxDepth:           1 << 30,
		AnalyzeInterval:                      -1,
	})

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

	if db.getSnapshot().fieldDelta("country") == nil {
		t.Fatalf("expected active country delta")
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
		t.Fatalf("count mismatch with delta: got=%d want=%d", got, want)
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
	rangeKey, isSlice, err := db.exprValueToIdxScalar(qx.Expr{Op: qx.OpGTE, Field: "age", Value: 1_000})
	if err != nil || isSlice {
		t.Fatalf("exprValueToIdxScalar failed: err=%v isSlice=%v", err, isSlice)
	}
	cacheKey := db.materializedPredCacheKeyForScalar("age", qx.OpGTE, rangeKey)
	if cacheKey == "" {
		t.Fatalf("expected non-empty materialized cache key for numeric range")
	}
	if _, ok := db.getSnapshot().loadMaterializedPred(cacheKey); ok {
		t.Fatalf("expected empty materialized cache before building predicates")
	}

	predsCount, ok := db.buildPredicatesWithMode(leaves, false)
	if !ok {
		t.Fatalf("buildPredicatesWithMode failed")
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
	if !ok || cached == nil || cached.IsEmpty() {
		t.Fatalf("expected default predicate build to populate numeric range cache")
	}
}

func TestCount_PreparePredicate_UsesBroadRangeComplementWithoutCache(t *testing.T) {
	makeDB := func(t *testing.T, withCache bool) *DB[uint64, Rec] {
		t.Helper()
		opts := Options{AnalyzeInterval: -1}
		if !withCache {
			opts.SnapshotMaterializedPredCacheMaxEntries = -1
			opts.SnapshotMaterializedPredCacheMaxEntriesWithDelta = -1
		}
		db, _ := openTempDBUint64(t, opts)
		seedGeneratedUint64Data(t, db, 80_000, func(i int) *Rec {
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
		return db
	}

	expr := qx.Expr{Op: qx.OpGTE, Field: "age", Value: 1_000}

	t.Run("NoCacheUsesLazyExactBitmapFilter", func(t *testing.T) {
		db := makeDB(t, false)
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
		if !p.supportsBitmapFilter() || !p.bitmapFilterCheap {
			t.Fatalf("expected broad no-cache range to expose cheap exact bitmap filter")
		}

		src := roaring64.NewBitmap()
		src.AddRange(1, universe+1)
		want := countByExprBitmap(t, db, expr)
		expectPredicateExactBitmapFilterCount(t, p, src, want)
	})
}

func TestCount_PreparePredicate_UsesTinyPostingBroadRangeComplement(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                                  -1,
		SnapshotMaterializedPredCacheMaxEntries:          -1,
		SnapshotMaterializedPredCacheMaxEntriesWithDelta: -1,
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
			if p.bm != nil {
				t.Fatalf("expected tiny complement to avoid bitmap materialization")
			}
			if !p.supportsBitmapFilter() || !p.bitmapFilterCheap {
				t.Fatalf("expected tiny complement predicate to support cheap exact bitmap filtering")
			}

			src := roaring64.NewBitmap()
			src.AddRange(1, universe+1)
			want := countByExprBitmap(t, db, tc.expr)
			expectPredicateExactBitmapFilterCount(t, p, src, want)
		})
	}
}

func TestCount_PreparePredicate_UsesProbeBoundedBroadRangeComplement(t *testing.T) {
	makeDB := func(t *testing.T, withCache bool) *DB[uint64, Rec] {
		t.Helper()
		opts := Options{AnalyzeInterval: -1}
		if !withCache {
			opts.SnapshotMaterializedPredCacheMaxEntries = -1
			opts.SnapshotMaterializedPredCacheMaxEntriesWithDelta = -1
		}
		db, _ := openTempDBUint64(t, opts)
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
		if err := db.RebuildIndex(); err != nil {
			t.Fatalf("RebuildIndex: %v", err)
		}
		return db
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
			p, ok := db.buildPredicateWithMode(expr, false)
			if !ok {
				t.Fatalf("buildPredicateWithMode failed")
			}
			defer releasePredicates([]predicate{p})
			if p.kind != predicateKindCustom {
				t.Fatalf("expected initial custom predicate, got kind=%v", p.kind)
			}

			universe := db.snapshotUniverseCardinality()
			leadProbeEst := uint64(40_000)
			if err := db.prepareCountPredicate(&p, leadProbeEst, universe); err != nil {
				t.Fatalf("prepareCountPredicate: %v", err)
			}
			if tc.withCache {
				if p.kind != predicateKindBitmapNot {
					t.Fatalf("expected cached probe-bounded broad range to switch to bitmapNot complement, got kind=%v", p.kind)
				}
				if p.bm == nil {
					t.Fatalf("expected complement bitmap")
				}
				if got := p.bm.GetCardinality(); got <= countPredBroadRangeComplementMaxCard {
					t.Fatalf("expected complement wider than legacy cap, got=%d", got)
				}
				key, isSlice, err := db.exprValueToIdxScalar(expr)
				if err != nil || isSlice {
					t.Fatalf("exprValueToIdxScalar: err=%v isSlice=%v", err, isSlice)
				}
				cacheKey := db.materializedPredComplementCacheKeyForScalar(expr.Field, expr.Op, key)
				cached, ok := db.getSnapshot().loadMaterializedPred(cacheKey)
				if !ok || cached == nil || cached != p.bm {
					t.Fatalf("expected complement bitmap to be cached and shared")
				}
				return
			}

			if p.kind != predicateKindCustom {
				t.Fatalf("expected no-cache broad range to stay custom and lazy, got kind=%v", p.kind)
			}
			if !p.supportsBitmapFilter() || !p.bitmapFilterCheap {
				t.Fatalf("expected no-cache broad range to expose cheap exact bitmap filter")
			}

			src := roaring64.NewBitmap()
			src.AddRange(1, universe+1)
			want := countByExprBitmap(t, db, expr)
			expectPredicateExactBitmapFilterCount(t, p, src, want)
		})
	}
}

func TestCount_PreparePredicate_KeepsLegacyCapForComparableBroadLead(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
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
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

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
		t.Fatalf("expected comparable lead to keep legacy complement cap, got kind=%v", p.kind)
	}
}

func TestCount_BroadRangeComplementFastMaterializationGate(t *testing.T) {
	probe := make([]postingList, 0, 5_000)
	for i := 0; i < 5_000; i++ {
		probe = append(probe, postingList{bm: postingSingleFlag, single: uint64(i)})
	}
	if !shouldUseFastCountBroadRangeComplementMaterialization(probe, 5_000) {
		t.Fatalf("expected many tiny buckets to use fast complement materialization")
	}
	if shouldUseFastCountBroadRangeComplementMaterialization(probe[:2_000], 2_000) {
		t.Fatalf("expected short probe to keep regular complement materialization")
	}
	if shouldUseFastCountBroadRangeComplementMaterialization(probe, 90_000) {
		t.Fatalf("expected dense buckets to keep regular complement materialization")
	}
}

func TestCount_PreparePredicate_UsesExactNumericEstimateForSkewedBroadRange(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                                  -1,
		SnapshotCompactorRequestEveryNWrites:             1 << 30,
		SnapshotCompactorIdleInterval:                    -1,
		SnapshotDeltaLayerMaxDepth:                       1 << 30,
		SnapshotMaterializedPredCacheMaxEntries:          -1,
		SnapshotMaterializedPredCacheMaxEntriesWithDelta: -1,
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
	const batchSize = 20_000
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
	if !p.supportsBitmapFilter() || !p.bitmapFilterCheap {
		t.Fatalf("expected skewed broad numeric range to expose cheap exact bitmap filter")
	}

	src := roaring64.NewBitmap()
	src.AddRange(1, universe+1)
	expectPredicateExactBitmapFilterCount(t, p, src, wantIn)
}

func TestCount_PreparePredicate_UsesBroadRangeComplementWithDeltaOverlay(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                                  -1,
		SnapshotCompactorRequestEveryNWrites:             1 << 30,
		SnapshotCompactorIdleInterval:                    -1,
		SnapshotDeltaLayerMaxDepth:                       1 << 30,
		SnapshotMaterializedPredCacheMaxEntries:          -1,
		SnapshotMaterializedPredCacheMaxEntriesWithDelta: -1,
	})

	db.DisableSync()
	defer db.EnableSync()

	var (
		id        uint64
		wantIn    uint64
		wantOut   uint64
		rangeFrom = 1_000
	)
	const batchSize = 20_000
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

	snap := db.getSnapshot()
	if snap == nil || snap.fieldDelta("age") == nil {
		t.Fatalf("expected age field delta to remain in snapshot")
	}

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
		t.Fatalf("expected delta-backed broad numeric range without cache to stay custom, got kind=%v", p.kind)
	}
	if !p.supportsBitmapFilter() || !p.bitmapFilterCheap {
		t.Fatalf("expected delta-backed broad numeric range to expose cheap exact bitmap filter")
	}

	src := roaring64.NewBitmap()
	src.AddRange(1, universe+1)
	expectPredicateExactBitmapFilterCount(t, p, src, wantIn)
}

func TestCount_PickLeadPredicate_PrefersLeadThatUnlocksCustomResidualMaterialization(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                                  -1,
		SnapshotMaterializedPredCacheMaxEntries:          -1,
		SnapshotMaterializedPredCacheMaxEntriesWithDelta: -1,
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
	if containsWithCountryLead.kind != predicateKindBitmap {
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

func TestCount_PickORLeadPredicate_PrefersPrefixLeadOverCheapEqLead(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                                  -1,
		SnapshotMaterializedPredCacheMaxEntries:          -1,
		SnapshotMaterializedPredCacheMaxEntriesWithDelta: -1,
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
