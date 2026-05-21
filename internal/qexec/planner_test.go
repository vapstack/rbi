package qexec

import (
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

type plannerPrecountRec struct {
	Email   string `db:"email"   rbi:"unique"`
	Country string `db:"country" rbi:"index"`
	Active  bool   `db:"active"  rbi:"index"`
	Score   int    `db:"score"   rbi:"index"`
}

func newPlannerPrecountDB(t *testing.T) *DB[uint64, plannerPrecountRec] {
	t.Helper()
	path := filepath.Join(t.TempDir(), "planner_precount.db")
	db, raw := openBoltAndNew[uint64, plannerPrecountRec](t, path, Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	rows := []plannerPrecountRec{
		{Email: "a@example.com", Country: "NL", Active: true, Score: 1},
		{Email: "b@example.com", Country: "NL", Active: true, Score: 2},
		{Email: "c@example.com", Country: "NL", Active: false, Score: 3},
		{Email: "d@example.com", Country: "US", Active: true, Score: 4},
		{Email: "e@example.com", Country: "DE", Active: true, Score: 5},
		{Email: "f@example.com", Country: "US", Active: false, Score: 6},
	}
	for i := range rows {
		if err := db.Set(uint64(i+1), &rows[i]); err != nil {
			t.Fatalf("Set(%d): %v", i+1, err)
		}
	}
	return db
}

func plannerPrecountBranch(t *testing.T, db *DB[uint64, plannerPrecountRec], q *qx.QX, field string, value any) plannerORBranch {
	t.Helper()
	window, ok := orderWindowForTest(q)
	if !ok {
		t.Fatalf("orderWindowForTest: ok=false")
	}
	branches, alwaysFalse, ok := db.engine.buildORBranchesOrdered(q.Filter.Args, "score", window)
	if !ok {
		t.Fatalf("buildORBranchesOrdered: ok=false")
	}
	if alwaysFalse {
		branches.Release()
		t.Fatalf("unexpected alwaysFalse")
	}
	t.Cleanup(branches.Release)

	for bi := 0; bi < branches.Len(); bi++ {
		branch := branches.owner[bi]
		for pi := 0; pi < branch.preds.Len(); pi++ {
			expr := branch.preds.owner[pi].expr
			if db.engine.fieldNameByOrdinal(expr.FieldOrdinal) == field && expr.Value == value {
				return branch
			}
		}
	}
	t.Fatalf("branch with %s=%v was not built", field, value)
	return plannerORBranch{}
}

func TestPlannerORBranches_AllocsPerRunStayZeroAfterWarmup(t *testing.T) {
	if testRaceEnabled {
		t.Skip("testing.AllocsPerRun is not stable under -race")
	}

	run := func() {
		branches := newPlannerORBranches(4)
		branches.Append(plannerORBranch{alwaysTrue: true, leadIdx: -1})
		branches.Append(plannerORBranch{estCard: 7, estKnown: true, leadIdx: -1})
		branches.Append(plannerORBranch{coveredRangeBounded: true, coveredRangeStart: 1, coveredRangeEnd: 3, leadIdx: -1})
		branches.Release()
	}

	for i := 0; i < 32; i++ {
		run()
	}
	allocs := testing.AllocsPerRun(100, run)
	if allocs != 0 {
		t.Fatalf("unexpected allocs after warmup: got=%v want=0", allocs)
	}
}

func TestPlanOROrderMergeBranchLimitPrecountsExactBranch(t *testing.T) {
	db := newPlannerPrecountDB(t)
	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.EQ("country", "NL"),
				qx.EQ("active", true),
			),
			qx.EQ("country", "US"),
		),
	).Sort("score", qx.ASC).Limit(200)

	branch := plannerPrecountBranch(t, db, q, "country", "NL")
	view := db.engine.currentQueryViewForTests()
	limit, exhaustive, err := view.planOROrderMergeBranchLimit(branch, 200)
	if err != nil {
		t.Fatalf("planOROrderMergeBranchLimit: %v", err)
	}
	if limit != 2 || !exhaustive {
		t.Fatalf("branch limit=(%d,%v), want (2,true)", limit, exhaustive)
	}
}

func TestPlanOROrderMergeBranchLimitPrecountsUniqueLeadBranch(t *testing.T) {
	db := newPlannerPrecountDB(t)
	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.EQ("email", "b@example.com"),
				qx.EQ("active", true),
			),
			qx.EQ("country", "US"),
		),
	).Sort("score", qx.ASC).Limit(200)

	branch := plannerPrecountBranch(t, db, q, "email", "b@example.com")
	view := db.engine.currentQueryViewForTests()
	card, ok := view.uniqueLeadBranchCardinality(branch)
	if !ok || card != 1 {
		t.Fatalf("uniqueLeadBranchCardinality=(%d,%v), want (1,true)", card, ok)
	}
	limit, exhaustive, err := view.planOROrderMergeBranchLimit(branch, 200)
	if err != nil {
		t.Fatalf("planOROrderMergeBranchLimit: %v", err)
	}
	if limit != 1 || !exhaustive {
		t.Fatalf("branch limit=(%d,%v), want (1,true)", limit, exhaustive)
	}
}

func TestPlanOROrderMergeBranchLimitPrecountZeroIsExhaustive(t *testing.T) {
	db := newPlannerPrecountDB(t)
	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.EQ("email", "b@example.com"),
				qx.EQ("active", false),
			),
			qx.EQ("country", "US"),
		),
	).Sort("score", qx.ASC).Limit(200)

	branch := plannerPrecountBranch(t, db, q, "email", "b@example.com")
	view := db.engine.currentQueryViewForTests()
	limit, exhaustive, err := view.planOROrderMergeBranchLimit(branch, 200)
	if err != nil {
		t.Fatalf("planOROrderMergeBranchLimit: %v", err)
	}
	if limit != 0 || !exhaustive {
		t.Fatalf("branch limit=(%d,%v), want (0,true)", limit, exhaustive)
	}
}

func TestPlannerFilterPostingByPredicateChecksBuf_PostsAnyOwnedLargeAllocsPerRunStayZeroAfterWarmup(t *testing.T) {
	if testRaceEnabled {
		t.Skip("testing.AllocsPerRun is not stable under -race")
	}

	srcIDs := make([]uint64, 0, 96)
	for i := 0; i < 96; i++ {
		srcIDs = append(srcIDs, uint64(i*3+1))
	}
	src := posting.BuildFromSorted(srcIDs)
	defer src.Release()

	postA := posting.BuildFromSorted([]uint64{
		srcIDs[3], srcIDs[8], srcIDs[14], srcIDs[19], srcIDs[27], srcIDs[36], srcIDs[44], srcIDs[52],
	})
	postB := posting.BuildFromSorted([]uint64{
		srcIDs[8], srcIDs[19], srcIDs[31], srcIDs[36], srcIDs[44], srcIDs[61], srcIDs[74], srcIDs[88],
	})
	defer postA.Release()
	defer postB.Release()

	postsBuf := posting.GetSlice(2)
	postsBuf = append(postsBuf, postA, postB)
	defer posting.ReleaseSlice(postsBuf)

	state := postsAnyFilterStatePool.Get()
	state.postsBuf = postsBuf

	preds := newPredicateSet(1)
	preds.Append(predicate{
		kind:          predicateKindPostsAny,
		postsAnyState: state,
	})
	defer preds.Release()

	checks := pooled.GetIntSlice(1)
	checks = append(checks, 0)
	defer pooled.ReleaseIntSlice(checks)

	var work posting.List
	defer func() {
		work.Release()
	}()

	run := func() {
		mode, exact, nextWork, card := plannerFilterPostingByPredicateChecks(preds.owner, checks, src, work, true)
		work = nextWork
		if mode != plannerPredicateBucketExact {
			t.Fatalf("unexpected mode: got=%v want=%v", mode, plannerPredicateBucketExact)
		}
		if card != src.Cardinality() {
			t.Fatalf("unexpected source cardinality: got=%d want=%d", card, src.Cardinality())
		}
		if got := exact.Cardinality(); got != 12 {
			t.Fatalf("unexpected exact cardinality: got=%d want=12", got)
		}
		for _, idx := range []uint64{srcIDs[8], srcIDs[19], srcIDs[36], srcIDs[44], srcIDs[88]} {
			if !exact.Contains(idx) {
				t.Fatalf("exact posting is missing id %d", idx)
			}
		}
	}

	run()
	allocs := testing.AllocsPerRun(100, run)
	if allocs != 0 {
		t.Fatalf("unexpected allocs after warmup: got=%v want=0", allocs)
	}
}

func TestPlannerFilterPostingByPredicateChecksBuf_CompactBorrowedAllocsPerRunStayZeroAfterWarmup(t *testing.T) {
	if testRaceEnabled {
		t.Skip("testing.AllocsPerRun is not stable under -race")
	}

	src := posting.BuildFromSorted([]uint64{1, 3, 5, 7, 9, 11})
	defer src.Release()

	postA := posting.BuildFromSorted([]uint64{1, 3, 5, 7, 9})
	postB := posting.BuildFromSorted([]uint64{3, 5, 11, 13})
	postC := posting.BuildFromSorted([]uint64{1, 3, 5, 15})
	postD := posting.BuildFromSorted([]uint64{3, 5, 7, 11})
	defer postA.Release()
	defer postB.Release()
	defer postC.Release()
	defer postD.Release()

	preds := newPredicateSet(4)
	preds.Append(predicate{
		kind:    predicateKindPosting,
		posting: postA,
	})
	preds.Append(predicate{
		kind:    predicateKindPosting,
		posting: postB,
	})
	preds.Append(predicate{
		kind:    predicateKindPosting,
		posting: postC,
	})
	preds.Append(predicate{
		kind:    predicateKindPosting,
		posting: postD,
	})
	defer preds.Release()

	checks := pooled.GetIntSlice(4)
	checks = append(checks, 0, 1, 2, 3)
	defer pooled.ReleaseIntSlice(checks)

	var work posting.List
	defer func() {
		work.Release()
	}()

	run := func() {
		mode, exact, nextWork, card := plannerFilterPostingByPredicateChecks(preds.owner, checks, src.Borrow(), work, true)
		work = nextWork
		if mode != plannerPredicateBucketExact {
			t.Fatalf("unexpected mode: got=%v want=%v", mode, plannerPredicateBucketExact)
		}
		if card != src.Cardinality() {
			t.Fatalf("unexpected source cardinality: got=%d want=%d", card, src.Cardinality())
		}
		if got := exact.Cardinality(); got != 2 {
			t.Fatalf("unexpected exact cardinality: got=%d want=2", got)
		}
		if !exact.Contains(3) || !exact.Contains(5) {
			t.Fatalf("unexpected exact posting: want ids 3 and 5")
		}
	}

	run()
	allocs := testing.AllocsPerRun(100, run)
	if allocs != 0 {
		t.Fatalf("unexpected allocs after warmup: got=%v want=0", allocs)
	}
}

func TestPlannerFilterPostingByPredicateChecksBuf_PreferredExactBypassesSmallBucketFallback(t *testing.T) {
	src := posting.BuildFromSorted([]uint64{1, 3, 5, 7, 9, 11, 13, 15})
	defer src.Release()

	postA := posting.BuildFromSorted([]uint64{3, 7, 11})
	postB := posting.BuildFromSorted([]uint64{5, 7, 13})
	defer postA.Release()
	defer postB.Release()

	postsBuf := posting.GetSlice(2)
	postsBuf = append(postsBuf, postA, postB)
	defer posting.ReleaseSlice(postsBuf)

	state := postsAnyFilterStatePool.Get()
	state.postsBuf = postsBuf

	preds := newPredicateSet(1)
	preds.Append(predicate{
		kind:          predicateKindPostsAny,
		postsAnyState: state,
	})
	defer preds.Release()

	checks := pooled.GetIntSlice(1)
	checks = append(checks, 0)
	defer pooled.ReleaseIntSlice(checks)

	mode, exact, work, card := plannerFilterPostingByPredicateChecks(preds.owner, checks, src.Borrow(), posting.List{}, false)
	defer work.Release()
	if mode != plannerPredicateBucketExact {
		t.Fatalf("unexpected mode: got=%v want=%v", mode, plannerPredicateBucketExact)
	}
	if card != src.Cardinality() {
		t.Fatalf("unexpected source cardinality: got=%d want=%d", card, src.Cardinality())
	}
	if got := exact.Cardinality(); got != 5 {
		t.Fatalf("unexpected exact cardinality: got=%d want=5", got)
	}
	for _, idx := range []uint64{3, 5, 7, 11, 13} {
		if !exact.Contains(idx) {
			t.Fatalf("exact posting is missing id %d", idx)
		}
	}
}

func TestTraceDisabled_BeginTraceReturnsNil(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSink:        nil,
		TraceSampleEvery: 1,
	})
	if db.engine.exec.TraceSamplingEnabled() {
		t.Fatalf("expected trace sampling to be disabled")
	}

	q := qx.Query(qx.EQ("age", 10))
	preparedQ, viewQ, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer preparedQ.Release()
	if tr := db.engine.beginTraceForTests(viewQ); tr != nil {
		t.Fatalf("expected nil trace when trace sink is disabled")
	}
}

func TestTryPlanOrdered_AllowsPointerSortField(t *testing.T) {
	db := newTestDB(t, testOptions{})
	db.seedGeneratedData(t, 10_000, func(id uint64) testRec {
		rec := testRec{
			Name:     fmt.Sprintf("user_%05d", id),
			Active:   id%20 != 0,
			FullName: fmt.Sprintf("FN-%05d", id),
		}
		if id%10 != 0 {
			opt := fmt.Sprintf("opt-%05d", id)
			rec.Opt = &opt
		}
		return rec
	})

	q := qx.Query(
		qx.NOT(qx.EQ("active", false)),
	).Sort("opt", qx.DESC).Offset(10).Limit(25)

	prepared, shape, err := db.prepareQuery(q)
	if err != nil {
		t.Fatalf("prepareQuery: %v", err)
	}
	defer prepared.Release()

	view := db.view()
	trace := Trace{sink: func(TraceEvent) {}}
	got, ok, plan, err := view.executeOrderedLimit(&shape, &trace)
	if err != nil {
		t.Fatalf("executeOrderedLimit: %v", err)
	}
	if !ok {
		t.Fatalf("executeOrderedLimit: ok=false estimated=%d ordered=%v fallback=%v", trace.ev.EstimatedRows, trace.ev.EstimatedCost, trace.ev.FallbackCost)
	}
	if plan != "" {
		trace.SetPlan(plan)
	}
	if trace.ev.Plan != string(PlanOrdered) && trace.ev.Plan != string(PlanLimitOrderBasic) {
		t.Fatalf("unexpected plan: got=%q want %q or %q", trace.ev.Plan, PlanOrdered, PlanLimitOrderBasic)
	}

	want := make([]uint64, 0, 25)
	skipped := 0
	for id := uint64(10_000); id >= 1 && len(want) < 25; id-- {
		if id%10 == 0 || id%20 == 0 {
			continue
		}
		if skipped < 10 {
			skipped++
			continue
		}
		want = append(want, id)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("tryPlanOrdered result mismatch: got=%v want=%v", got, want)
	}
}

func TestPlannerResidualChecks_RemovesExactChecksByMembershipOrder(t *testing.T) {
	checks := []int{3, 1, 4, 2}
	exactChecks := []int{1, 2}

	got := plannerResidualChecks(nil, checks, exactChecks)
	want := []int{3, 4}
	if !slices.Equal(got, want) {
		t.Fatalf("unexpected residual checks: got=%v want=%v", got, want)
	}
}

func TestOrderRangeCoverage_ConsistencyBetweenPredicateKinds(t *testing.T) {
	db, _ := openTempDBUint64(t)

	s := []indexdata.Entry{
		{Key: keycodec.FromString("alice"), IDs: postingOf(1)},
		{Key: keycodec.FromString("alina"), IDs: postingOf(2)},
		{Key: keycodec.FromString("bob"), IDs: postingOf(3)},
	}
	ov := indexdata.NewFieldIndexView(&s)

	exprs := []qx.Expr{
		qx.PREFIX("name", "al"),
		qx.GTE("name", "al"),
		qx.EQ("age", 20),
		qx.NOT(qx.EQ("name", "alice")),
	}

	cands := make([]predicate, len(exprs))
	preds := make([]predicate, len(exprs))
	for i := range exprs {
		cands[i] = predicate{expr: mustTestQIRExprForDB(t, db, exprs[i])}
		preds[i] = predicate{expr: mustTestQIRExprForDB(t, db, exprs[i])}
	}

	br1, covOv1, okOv1 := db.engine.extractOrderRangeCoverageIndexView("name", cands, ov)
	if !okOv1 {
		t.Fatalf("extractOrderRangeCoverageOverlay failed")
	}
	br2, covOv2, okOv2 := db.engine.extractOrderRangeCoverageIndexView("name", preds, ov)
	if !okOv2 {
		t.Fatalf("extractOrderRangeCoverageOverlay failed")
	}
	if br1.BaseStart != br2.BaseStart || br1.BaseEnd != br2.BaseEnd {
		t.Fatalf("overlay range mismatch: candidate=%+v planner=%+v", br1, br2)
	}
	if !slices.Equal(covOv1, covOv2) {
		t.Fatalf("overlay covered mismatch: candidate=%v planner=%v", covOv1, covOv2)
	}
}

func TestRangeContainsThresholds_Adaptive(t *testing.T) {
	sparseSmall := rangeLinearContainsLimit(128, 128)
	denseSmall := rangeLinearContainsLimit(128, 16_384)
	if denseSmall <= sparseSmall {
		t.Fatalf("expected denser probe to keep linear contains longer: sparse=%d dense=%d", sparseSmall, denseSmall)
	}

	// Use probe width where density scaling is observable and doesn't collapse to clamp=1.
	afterSparse := rangeMaterializeAfterForProbe(2_048, 2_048)
	afterDense := rangeMaterializeAfterForProbe(2_048, 2_048*256)
	if afterSparse >= afterDense {
		t.Fatalf("expected sparse probe to materialize earlier than dense: sparse=%d dense=%d", afterSparse, afterDense)
	}

	afterSmall := rangeMaterializeAfterForProbe(256, 2_048)
	afterLarge := rangeMaterializeAfterForProbe(8_192, 65_536)
	if afterLarge > afterSmall {
		t.Fatalf("expected wider probe to materialize no later than small: small=%d large=%d", afterSmall, afterLarge)
	}
}

func TestPlannerExecutionOrderFactors_Adaptive(t *testing.T) {
	baseProfile := plannerOrderedProfile{
		coverage:     1.0,
		activeChecks: 2,
	}
	baseBase, baseCheck, baseRange, basePrefix := plannerExecutionOrderFactors(baseProfile, 1.2, 500_000)

	narrowProfile := plannerOrderedProfile{
		coverage:     0.10,
		activeChecks: 2,
	}
	narrowBase, narrowCheck, narrowRange, narrowPrefix := plannerExecutionOrderFactors(narrowProfile, 1.2, 500_000)
	if narrowBase >= baseBase || narrowCheck >= baseCheck || narrowRange >= baseRange || narrowPrefix >= basePrefix {
		t.Fatalf(
			"expected narrower coverage to reduce factors: base=(%.3f %.3f %.3f %.3f) narrow=(%.3f %.3f %.3f %.3f)",
			baseBase, baseCheck, baseRange, basePrefix,
			narrowBase, narrowCheck, narrowRange, narrowPrefix,
		)
	}

	_, lowSkewCheck, _, _ := plannerExecutionOrderFactors(baseProfile, 1.2, 500_000)
	_, highSkewCheck, _, _ := plannerExecutionOrderFactors(baseProfile, 5.0, 500_000)
	if highSkewCheck <= lowSkewCheck {
		t.Fatalf("expected higher skew to increase check factor: low=%.3f high=%.3f", lowSkewCheck, highSkewCheck)
	}
}

func TestPlannerOrderedFallbackProbeFactor_Adaptive(t *testing.T) {
	profile := plannerOrderedProfile{
		coverage:         0.25,
		hasPrefix:        true,
		orderRangeLeaves: 1,
	}
	noOffset := plannerOrderedFallbackProbeFactor(2.0, profile, 0)
	withOffset := plannerOrderedFallbackProbeFactor(2.0, profile, 1_000)
	if withOffset >= noOffset {
		t.Fatalf("expected offset shape to reduce fallback probe factor: no_offset=%.3f with_offset=%.3f", noOffset, withOffset)
	}

	lowSkew := plannerOrderedFallbackProbeFactor(1.2, profile, 0)
	highSkew := plannerOrderedFallbackProbeFactor(5.0, profile, 0)
	if highSkew <= lowSkew {
		t.Fatalf("expected higher skew to increase fallback probe factor: low=%.3f high=%.3f", lowSkew, highSkew)
	}
}

func TestTracer_ORNoOrderAdaptiveStopsAtLimit(t *testing.T) {
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

	q := qx.Query(
		qx.OR(
			qx.EQ("full_name", "FN-10"),
			qx.EQ("full_name", "FN-20"),
			qx.GTE("full_name", "FN-9000"),
		),
	).Limit(2)

	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 ids, got %d", len(ids))
	}

	mu.Lock()
	if len(events) == 0 {
		mu.Unlock()
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	mu.Unlock()

	if ev.Plan != "plan_or_merge_no_order" {
		t.Fatalf("expected no-order OR plan, got %q", ev.Plan)
	}
	if ev.EarlyStopReason != "limit_reached" {
		t.Fatalf("expected limit_reached early stop, got %q", ev.EarlyStopReason)
	}
	if len(ev.ORBranches) != 3 {
		t.Fatalf("unexpected OR branch trace size: got=%d want=3", len(ev.ORBranches))
	}

	if ev.RowsExamined > 4 {
		t.Fatalf("expected adaptive no-order OR to stop after filling limit, examined=%d trace=%+v", ev.RowsExamined, ev)
	}

	fullQ := cloneQuery(q)
	fullQ.Window.Limit = 0
	full, err := db.QueryKeys(fullQ)
	if err != nil {
		t.Fatalf("full QueryKeys: %v", err)
	}
	assertNoOrderPage(t, q, ids, full, "or_no_order_adaptive")
}

func TestPlannerORNoOrderAdaptive_ReturnsNoOrderPage(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.OR(
			qx.EQ("active", true),
			qx.EQ("name", "alice"),
			qx.EQ("country", "NL"),
		),
	).Limit(150)
	branchesAdaptive, alwaysFalse, ok := db.engine.buildORBranches(q.Filter.Args)
	if !ok {
		t.Fatalf("buildORBranches adaptive: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for adaptive branches")
	}
	defer branchesAdaptive.Release()

	gotAdaptive, ok := db.engine.execPlanORNoOrderAdaptiveCore(q, branchesAdaptive, nil)
	if !ok {
		t.Fatalf("execPlanORNoOrderAdaptive: ok=false")
	}

	fullQ := cloneQuery(q)
	fullQ.Window.Limit = 0
	full, err := db.QueryKeys(fullQ)
	if err != nil {
		t.Fatalf("full QueryKeys: %v", err)
	}

	assertNoOrderPage(t, q, gotAdaptive, full, "adaptive")
}

func TestBuildORBranches_BroadNumericRangeStaysRuntimeOnSecondBuild(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
		NumericRangeBucketSize:                  8,
		NumericRangeBucketMinFieldKeys:          16,
		NumericRangeBucketMinSpanKeys:           4,
	})
	seedGeneratedUint64Data(t, db, 12_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%05d@example.com", i),
			Age:    i,
			Score:  float64(i),
			Active: i%2 == 0,
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.SUFFIX("email", "@example.com"),
				qx.GTE("score", 6_000.0),
			),
			qx.AND(
				qx.EQ("active", true),
				qx.EQ("name", "u_42"),
			),
		),
	).Limit(150)

	checkRangePred := func(branches plannerORBranches) {
		t.Helper()
		var found bool
		for i := 0; i < branches.Len(); i++ {
			branch := branches.owner[i]
			for j := 0; j < branch.preds.Len(); j++ {
				p := branch.preds.owner[j]
				if db.engine.fieldNameByOrdinal(p.expr.FieldOrdinal) != "score" || p.expr.Op != compileScalarOpForTest(qx.OpGTE) {
					continue
				}
				found = true
				if p.isMaterializedLike() || p.lazyMatState != nil {
					t.Fatalf("expected broad range leaf to stay on runtime state")
				}
				if p.fieldIndexRangeState == nil {
					t.Fatalf("expected broad range leaf runtime state")
				}
			}
		}
		if !found {
			t.Fatalf("expected score range predicate in OR branches")
		}
	}

	branches, alwaysFalse, ok := db.engine.buildORBranches(q.Filter.Args)
	if !ok {
		t.Fatalf("buildORBranches: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse")
	}
	checkRangePred(branches)
	branches.Release()
	if got := db.engine.snapshot.Current().MaterializedPredCache().EntryCount(); got != 0 {
		t.Fatalf("unexpected shared materialized predicate cache entry after first build: %d", got)
	}

	branches, alwaysFalse, ok = db.engine.buildORBranches(q.Filter.Args)
	if !ok {
		t.Fatalf("buildORBranches second: ok=false")
	}
	defer branches.Release()
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse on second build")
	}
	checkRangePred(branches)
	if got := db.engine.snapshot.Current().MaterializedPredCache().EntryCount(); got != 0 {
		t.Fatalf("unexpected shared materialized predicate cache entry after second build: %d", got)
	}
}

func TestPlannerORNoOrder_BroadResidualRangeStaysLazyDuringAdaptiveLimit(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
		NumericRangeBucketSize:                  8,
		NumericRangeBucketMinFieldKeys:          16,
		NumericRangeBucketMinSpanKeys:           4,
	})
	seedGeneratedUint64Data(t, db, 12_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%05d@example.com", i),
			Age:    i,
			Score:  float64(i),
			Active: i%2 == 0,
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.EQ("active", true),
				qx.GTE("score", 4_000.0),
			),
			qx.AND(
				qx.EQ("name", "u_42"),
				qx.EQ("email", "user00042@example.com"),
			),
		),
	).Limit(150)

	checkScorePred := func(branches plannerORBranches, wantKind predicateKind) {
		t.Helper()
		var found bool
		for i := 0; i < branches.Len(); i++ {
			branch := branches.owner[i]
			for j := 0; j < branch.preds.Len(); j++ {
				p := branch.preds.owner[j]
				if db.engine.fieldNameByOrdinal(p.expr.FieldOrdinal) != "score" || p.expr.Op != compileScalarOpForTest(qx.OpGTE) {
					continue
				}
				found = true
				if j == branch.leadIdx {
					t.Fatalf("expected broad score range to stay residual, got lead in branch %d", i)
				}
				if p.kind != wantKind {
					t.Fatalf("unexpected broad score predicate kind: got=%v want=%v", p.kind, wantKind)
				}
			}
		}
		if !found {
			t.Fatalf("expected score range predicate in OR branches")
		}
	}

	branches, alwaysFalse, ok := db.engine.buildORBranches(q.Filter.Args)
	if !ok {
		t.Fatalf("buildORBranches first: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse on first build")
	}
	checkScorePred(branches, predicateKindCustom)
	branches.Release()

	branches, alwaysFalse, ok = db.engine.buildORBranches(q.Filter.Args)
	if !ok {
		t.Fatalf("buildORBranches second: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse on second build")
	}
	checkScorePred(branches, predicateKindCustom)

	if _, ok := db.engine.execPlanORNoOrderAdaptiveCore(q, branches, nil); !ok {
		branches.Release()
		t.Fatalf("execPlanORNoOrderAdaptive: ok=false")
	}
	checkScorePred(branches, predicateKindCustom)
	branches.Release()

	if got := db.engine.snapshot.Current().MaterializedPredCache().EntryCount(); got != 0 {
		t.Fatalf("unexpected shared materialized predicate cache entry: %d", got)
	}
}

func TestPlannerORNoOrder_ExactLeadWithComplementRangeResidualNotExhausted(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
		NumericRangeBucketSize:                  8,
		NumericRangeBucketMinFieldKeys:          16,
		NumericRangeBucketMinSpanKeys:           4,
	})
	seedGeneratedUint64Data(t, db, 12_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%05d@example.com", i),
			Score:  float64(i),
			Active: i%2 == 0,
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.EQ("name", "u_6000"),
				qx.GTE("score", 4_000.0),
			),
			qx.EQ("name", "missing"),
		),
	).Limit(1)

	branches, alwaysFalse, ok := db.engine.buildORBranches(q.Filter.Args)
	if !ok {
		t.Fatalf("buildORBranches: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse")
	}
	defer branches.Release()

	var found bool
	for i := 0; i < branches.Len(); i++ {
		branch := branches.owner[i]
		for j := 0; j < branch.preds.Len(); j++ {
			p := branch.preds.owner[j]
			if db.engine.fieldNameByOrdinal(p.expr.FieldOrdinal) != "score" || p.expr.Op != compileScalarOpForTest(qx.OpGTE) {
				continue
			}
			found = true
			if p.fieldIndexRangeState == nil || !p.fieldIndexRangeState.probe.useComplement {
				t.Fatalf("expected score residual to stay as complement runtime probe")
			}
			leadIDs, ok := plannerORPredicateExactLeadPosting(branch.leadPtr())
			if !ok {
				t.Fatalf("expected exact lead posting")
			}
			cnt, ok := p.countBucket(leadIDs)
			if !ok {
				t.Fatalf("expected complement-probe residual bucket count")
			}
			if cnt != 1 {
				t.Fatalf("complement-probe residual countBucket = %d, want 1", cnt)
			}
			if plannerORBranchExactLeadPostingEmpty(&branch) {
				t.Fatalf("complement-probe residual must not prove exact-lead branch empty")
			}
		}
	}
	if !found {
		t.Fatalf("expected score range predicate in OR branches")
	}

	got, ok := db.engine.execPlanORNoOrderAdaptiveCore(q, branches, nil)
	if !ok {
		t.Fatalf("execPlanORNoOrderAdaptive: ok=false")
	}
	if len(got) != 1 || got[0] != 6000 {
		t.Fatalf("unexpected adaptive result: got=%v want [6000]", got)
	}
}

func TestPlannerORNoOrder_NullableBroadResidualRangeDoesNotStayLazy(t *testing.T) {
	db, _ := openTempDBUint64PtrInt(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	for i := 0; i < 4; i++ {
		rec := &PtrIntRec{Name: fmt.Sprintf("nil_%02d", i), Rank: nil, Active: i%2 == 0}
		if err := db.Set(uint64(i+1), rec); err != nil {
			t.Fatalf("Set(nil_%02d): %v", i, err)
		}
	}
	for i := 0; i < 24; i++ {
		rec := &PtrIntRec{Name: fmt.Sprintf("rank_%02d", i), Rank: intPtr(i), Active: i%2 == 0}
		if err := db.Set(uint64(i+5), rec); err != nil {
			t.Fatalf("Set(rank_%02d): %v", i, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.EQ("active", true),
				qx.GTE("rank", 4),
			),
			qx.AND(
				qx.EQ("name", "nil_01"),
				qx.EQ("active", false),
			),
		),
	).Limit(50)

	branches, alwaysFalse, ok := db.engine.buildORBranches(q.Filter.Args)
	if !ok {
		t.Fatalf("buildORBranches: ok=false")
	}
	defer branches.Release()
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse")
	}

	found := false
	for bi := 0; bi < branches.Len(); bi++ {
		branch := branches.owner[bi]
		for pi := 0; pi < branch.preds.Len(); pi++ {
			p := branch.preds.owner[pi]
			if db.engine.fieldNameByOrdinal(p.expr.FieldOrdinal) != "rank" || p.expr.Op != compileScalarOpForTest(qx.OpGTE) {
				continue
			}
			found = true
			if p.lazyMatState != nil {
				t.Fatalf("expected nullable broad range to avoid lazy state")
			}
			if p.hasRuntimeRangeState() {
				t.Fatalf("expected nullable broad range to avoid runtime complement probe")
			}
			if p.kind == predicateKindMaterializedNot || !p.rangeMat {
				t.Fatalf("expected nil-heavy nullable broad range to materialize positive side when branch already has lead, got kind=%v rangeMat=%v", p.kind, p.rangeMat)
			}
			if !p.isMaterializedLike() {
				t.Fatalf("expected nullable broad range to materialize for correctness, got kind=%v", p.kind)
			}
			if p.matches(1) || p.matches(2) {
				t.Fatalf("nil rank rows must not match broad positive range")
			}
			if !p.matches(9) {
				t.Fatalf("expected in-range row to match materialized nullable range")
			}
		}
	}
	if !found {
		t.Fatalf("expected rank range predicate in OR branches")
	}
}

func TestPlannerORNoOrder_NullableNonBroadComplementResidualRangeDoesNotStayLazy(t *testing.T) {
	db, _ := openTempDBUint64PtrInt(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	for i := 0; i < 64; i++ {
		rec := &PtrIntRec{Name: fmt.Sprintf("nil_%02d", i), Rank: nil, Active: i%2 == 0}
		if err := db.Set(uint64(i+1), rec); err != nil {
			t.Fatalf("Set(nil_%02d): %v", i, err)
		}
	}
	for i := 0; i < 40; i++ {
		v := 0
		rec := &PtrIntRec{Name: fmt.Sprintf("zero_%02d", i), Rank: &v, Active: i%2 == 0}
		if err := db.Set(uint64(i+65), rec); err != nil {
			t.Fatalf("Set(zero_%02d): %v", i, err)
		}
	}
	for i := 1; i <= 9; i++ {
		v := i
		rec := &PtrIntRec{Name: fmt.Sprintf("rank_%02d", i), Rank: &v, Active: true}
		if err := db.Set(uint64(i+105), rec); err != nil {
			t.Fatalf("Set(rank_%02d): %v", i, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	view := db.engine.currentQueryViewForTests()
	expr := mustTestQIRExprForDB(t, db, qx.GTE("rank", 1))
	candidate, ok := view.prepareScalarRangeRoutingCandidate(expr)
	if !ok {
		t.Fatalf("expected nullable range routing candidate")
	}
	universe := view.snap.Universe.Cardinality()
	if universe == 0 {
		t.Fatalf("expected non-zero snapshot universe")
	}
	if !candidate.plan.useComplement {
		t.Fatalf("expected complement route for sparse nullable range")
	}
	if candidate.broadComplementCardinality(universe) {
		t.Fatalf("expected non-broad complement route for sparse nullable range")
	}

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.EQ("active", true),
				qx.GTE("rank", 1),
			),
			qx.AND(
				qx.EQ("name", "nil_01"),
				qx.EQ("active", false),
			),
		),
	).Limit(50)

	branches, alwaysFalse, ok := db.engine.buildORBranches(q.Filter.Args)
	if !ok {
		t.Fatalf("buildORBranches: ok=false")
	}
	defer branches.Release()
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse")
	}

	found := false
	for bi := 0; bi < branches.Len(); bi++ {
		branch := branches.owner[bi]
		for pi := 0; pi < branch.preds.Len(); pi++ {
			p := branch.preds.owner[pi]
			if db.engine.fieldNameByOrdinal(p.expr.FieldOrdinal) != "rank" || p.expr.Op != compileScalarOpForTest(qx.OpGTE) {
				continue
			}
			found = true
			if p.lazyMatState != nil {
				t.Fatalf("expected nullable non-broad complement range to avoid lazy state")
			}
			if p.hasRuntimeRangeState() {
				t.Fatalf("expected nullable non-broad complement range to avoid runtime complement probe")
			}
			if p.kind == predicateKindMaterializedNot || !p.rangeMat {
				t.Fatalf("expected nil-heavy nullable non-broad range to materialize positive side when branch already has lead, got kind=%v rangeMat=%v", p.kind, p.rangeMat)
			}
			if !p.isMaterializedLike() {
				t.Fatalf("expected nullable non-broad complement range to materialize for correctness, got kind=%v", p.kind)
			}
			if p.matches(1) || p.matches(3) {
				t.Fatalf("nil rank rows must not match nullable non-broad positive range")
			}
			if !p.matches(106) {
				t.Fatalf("expected in-range row to match materialized nullable non-broad range")
			}
		}
	}
	if !found {
		t.Fatalf("expected rank range predicate in OR branches")
	}
}

func TestPlannerORNoOrder_NullableNonBroadComplementResidualRangeDoesNotStayLazyWithoutMatPredCache(t *testing.T) {
	db, _ := openTempDBUint64PtrInt(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 0,
	})
	for i := 0; i < 64; i++ {
		rec := &PtrIntRec{Name: fmt.Sprintf("nil_%02d", i), Rank: nil, Active: i%2 == 0}
		if err := db.Set(uint64(i+1), rec); err != nil {
			t.Fatalf("Set(nil_%02d): %v", i, err)
		}
	}
	for i := 0; i < 40; i++ {
		v := 0
		rec := &PtrIntRec{Name: fmt.Sprintf("zero_%02d", i), Rank: &v, Active: i%2 == 0}
		if err := db.Set(uint64(i+65), rec); err != nil {
			t.Fatalf("Set(zero_%02d): %v", i, err)
		}
	}
	for i := 1; i <= 9; i++ {
		v := i
		rec := &PtrIntRec{Name: fmt.Sprintf("rank_%02d", i), Rank: &v, Active: true}
		if err := db.Set(uint64(i+105), rec); err != nil {
			t.Fatalf("Set(rank_%02d): %v", i, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	view := db.engine.currentQueryViewForTests()
	expr := mustTestQIRExprForDB(t, db, qx.GTE("rank", 1))
	candidate, ok := view.prepareScalarRangeRoutingCandidate(expr)
	if !ok {
		t.Fatalf("expected nullable range routing candidate")
	}
	universe := view.snap.Universe.Cardinality()
	if universe == 0 {
		t.Fatalf("expected non-zero snapshot universe")
	}
	if !candidate.plan.useComplement {
		t.Fatalf("expected complement route for sparse nullable range")
	}
	if candidate.broadComplementCardinality(universe) {
		t.Fatalf("expected non-broad complement route for sparse nullable range")
	}

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.EQ("active", true),
				qx.GTE("rank", 1),
			),
			qx.AND(
				qx.EQ("name", "nil_01"),
				qx.EQ("active", false),
			),
		),
	).Limit(50)

	branches, alwaysFalse, ok := db.engine.buildORBranches(q.Filter.Args)
	if !ok {
		t.Fatalf("buildORBranches: ok=false")
	}
	defer branches.Release()
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse")
	}

	found := false
	for bi := 0; bi < branches.Len(); bi++ {
		branch := branches.owner[bi]
		for pi := 0; pi < branch.preds.Len(); pi++ {
			p := branch.preds.owner[pi]
			if db.engine.fieldNameByOrdinal(p.expr.FieldOrdinal) != "rank" || p.expr.Op != compileScalarOpForTest(qx.OpGTE) {
				continue
			}
			found = true
			if p.lazyMatState != nil {
				t.Fatalf("expected nullable non-broad complement range to avoid lazy state with mat-pred cache disabled")
			}
			if p.hasRuntimeRangeState() {
				t.Fatalf("expected nullable non-broad complement range to avoid runtime complement probe with mat-pred cache disabled")
			}
			if p.kind == predicateKindMaterializedNot || !p.rangeMat {
				t.Fatalf("expected nil-heavy nullable non-broad range to materialize positive side with mat-pred cache disabled, got kind=%v rangeMat=%v", p.kind, p.rangeMat)
			}
			if p.matches(1) || p.matches(3) {
				t.Fatalf("nil rank rows must not match nullable non-broad positive range with mat-pred cache disabled")
			}
			if !p.matches(106) {
				t.Fatalf("expected in-range row to match materialized nullable range with mat-pred cache disabled")
			}
		}
	}
	if !found {
		t.Fatalf("expected rank range predicate in OR branches")
	}

	gotBaseline, ok := db.engine.execPlanORNoOrderBaselineCore(q, branches, nil)
	if !ok {
		t.Fatalf("execPlanORNoOrderBaseline: ok=false")
	}
	for _, id := range gotBaseline {
		if id == 1 || id == 3 || id == 5 {
			t.Fatalf("baseline execution leaked unrelated nil-rank row id=%d with mat-pred cache disabled", id)
		}
	}
}

func TestPlannerORNoOrder_NullableNonBroadComplementOnlyPositiveLeafKeepsLead(t *testing.T) {
	db, _ := openTempDBUint64PtrInt(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	for i := 0; i < 64; i++ {
		rec := &PtrIntRec{Name: fmt.Sprintf("nil_%02d", i), Rank: nil, Active: i%2 == 0}
		if err := db.Set(uint64(i+1), rec); err != nil {
			t.Fatalf("Set(nil_%02d): %v", i, err)
		}
	}
	for i := 0; i < 40; i++ {
		v := 0
		rec := &PtrIntRec{Name: fmt.Sprintf("zero_%02d", i), Rank: &v, Active: i%2 == 0}
		if err := db.Set(uint64(i+65), rec); err != nil {
			t.Fatalf("Set(zero_%02d): %v", i, err)
		}
	}
	for i := 1; i <= 9; i++ {
		v := i
		rec := &PtrIntRec{Name: fmt.Sprintf("rank_%02d", i), Rank: &v, Active: true}
		if err := db.Set(uint64(i+105), rec); err != nil {
			t.Fatalf("Set(rank_%02d): %v", i, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.GTE("rank", 1),
				qx.NOT(qx.EQ("active", false)),
			),
			qx.EQ("name", "nil_01"),
		),
	).Limit(50)

	branches, alwaysFalse, ok := db.engine.buildORBranches(q.Filter.Args)
	if !ok {
		t.Fatalf("buildORBranches: ok=false")
	}
	defer branches.Release()
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse")
	}

	found := false
	for bi := 0; bi < branches.Len(); bi++ {
		branch := branches.owner[bi]
		for pi := 0; pi < branch.preds.Len(); pi++ {
			p := branch.preds.owner[pi]
			if db.engine.fieldNameByOrdinal(p.expr.FieldOrdinal) != "rank" || p.expr.Op != compileScalarOpForTest(qx.OpGTE) {
				continue
			}
			found = true
			if !branch.hasLead() || branch.leadPtr() == nil {
				t.Fatalf("expected forced nullable range branch to keep lead iterator")
			}
			if db.engine.fieldNameByOrdinal(branch.preds.owner[branch.leadIdx].expr.FieldOrdinal) != "rank" {
				t.Fatalf("expected nullable range leaf to remain the lead predicate, got field=%s", db.engine.fieldNameByOrdinal(branch.preds.owner[branch.leadIdx].expr.FieldOrdinal))
			}
			if p.kind == predicateKindMaterializedNot {
				t.Fatalf("expected only positive leaf to keep positive materialized side as branch lead")
			}
			if !p.isMaterializedLike() {
				t.Fatalf("expected forced nullable range branch to materialize")
			}
			if p.hasRuntimeRangeState() {
				t.Fatalf("expected forced nullable range branch to avoid runtime complement state")
			}
			if p.matches(1) || p.matches(3) || p.matches(5) {
				t.Fatalf("nil rank rows must not match branch where range is the only positive leaf")
			}
			if !p.matches(106) {
				t.Fatalf("expected in-range row to match branch where range is the only positive leaf")
			}
		}
	}
	if !found {
		t.Fatalf("expected rank range branch with negative sibling leaf")
	}

	gotBaseline, ok := db.engine.execPlanORNoOrderBaselineCore(q, branches, nil)
	if !ok {
		t.Fatalf("execPlanORNoOrderBaseline: ok=false")
	}
	for _, id := range gotBaseline {
		if id == 1 || id == 3 || id == 5 {
			t.Fatalf("baseline execution leaked unrelated nil-rank row id=%d", id)
		}
	}

	branches2, alwaysFalse, ok := db.engine.buildORBranches(q.Filter.Args)
	if !ok {
		t.Fatalf("buildORBranches second: ok=false")
	}
	defer branches2.Release()
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse on second build")
	}
	gotAdaptive, ok := db.engine.execPlanORNoOrderAdaptiveCore(q, branches2, nil)
	if !ok {
		t.Fatalf("execPlanORNoOrderAdaptive: ok=false")
	}
	for _, id := range gotAdaptive {
		if id == 1 || id == 3 || id == 5 {
			t.Fatalf("adaptive execution leaked unrelated nil-rank row id=%d", id)
		}
	}
}

func TestPlannerOROrderKWay_MatchesFallbackMerge(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.EQ("active", true),
				qx.EQ("country", "NL"),
			),
			qx.AND(
				qx.EQ("name", "alice"),
				qx.GTE("age", 25),
			),
			qx.PREFIX("full_name", "FN-1"),
		),
	).Sort("age", qx.ASC).Offset(30).Limit(120)
	branchesKWay, alwaysFalse, ok := db.engine.buildORBranches(q.Filter.Args)
	if !ok {
		t.Fatalf("buildORBranches kway: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for kway branches")
	}
	defer branchesKWay.Release()

	gotKWay, ok, err := db.engine.execPlanOROrderKWay(q, branchesKWay, nil)
	if err != nil {
		t.Fatalf("execPlanOROrderKWay err: %v", err)
	}
	if !ok {
		t.Fatalf("execPlanOROrderKWay: ok=false")
	}

	branchesBaseline, alwaysFalse, ok := db.engine.buildORBranches(q.Filter.Args)
	if !ok {
		t.Fatalf("buildORBranches fallback merge: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for fallback merge branches")
	}
	defer branchesBaseline.Release()

	gotBaseline, ok, err := db.engine.execPlanOROrderMergeFallback(q, branchesBaseline, nil)
	if err != nil {
		t.Fatalf("execPlanOROrderMergeFallback err: %v", err)
	}
	if !ok {
		t.Fatalf("execPlanOROrderMergeFallback: ok=false")
	}

	if !slices.Equal(gotKWay, gotBaseline) {
		t.Fatalf("kway/fallback mismatch:\nkway=%v\nfallback=%v", gotKWay, gotBaseline)
	}
}

func TestPlannerOROrderBranchIter_ResidualRowsExcludeExactOnlyChecks(t *testing.T) {
	bucket := posting.BuildFromSorted([]uint64{1, 2, 3, 4, 5, 6, 7, 8})
	defer bucket.Release()

	exact := posting.BuildFromSorted([]uint64{1, 2, 3, 4})
	defer exact.Release()

	residualAllowed := posting.BuildFromSorted([]uint64{1, 2})
	defer residualAllowed.Release()

	preds := newPredicateSet(2)
	preds.Append(predicate{
		kind:    predicateKindPosting,
		posting: exact.Borrow(),
		estCard: exact.Cardinality(),
	})
	preds.Append(predicate{
		kind:    predicateKindCustom,
		estCard: residualAllowed.Cardinality(),
		contains: func(idx uint64) bool {
			return residualAllowed.Contains(idx)
		},
	})
	defer preds.Release()

	checks := pooled.GetIntSlice(2)
	checks = append(checks, 0, 1)
	exactChecks := pooled.GetIntSlice(1)
	exactChecks = append(exactChecks, 0)
	residualChecks := pooled.GetIntSlice(1)
	residualChecks = append(residualChecks, 1)

	iter := plannerOROrderBranchIter{
		branch:         &plannerORBranch{preds: preds, leadIdx: -1},
		checks:         checks,
		exactChecks:    exactChecks,
		residualChecks: residualChecks,
		indexView:      indexdata.NewFieldIndexView(&[]indexdata.Entry{{IDs: bucket.Borrow()}}),
		single:         -1,
		exactSingle:    0,
		residualSingle: 1,
		allChecksExact: false,
		startBucket:    0,
		endBucket:      1,
	}
	iter.init()
	defer iter.close()

	var examined uint64
	var residualExamined uint64
	var emitted uint64
	for {
		examinedDelta, residualDelta, emittedDelta, ok := iter.advance()
		examined += examinedDelta
		residualExamined += residualDelta
		emitted += emittedDelta
		if !ok {
			break
		}
	}

	if examined != 8 {
		t.Fatalf("expected examined=8, got=%d", examined)
	}
	if residualExamined != 4 {
		t.Fatalf("expected residualExamined=4, got=%d", residualExamined)
	}
	if emitted != 2 {
		t.Fatalf("expected emitted=2, got=%d", emitted)
	}
}

func TestInitOrderedORBranchEstimates_ExcludesCoveredLeafFromCard(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	seedGeneratedUint64Data(t, db, 100, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   18 + i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	view := db.engine.currentQueryViewForTests()
	if got := view.snap.Universe.Cardinality(); got != 100 {
		t.Fatalf("snapshotUniverse=%d, want 100", got)
	}

	preds := newPredicateSet(2)
	preds.Append(predicate{
		estCard: 32,
		covered: true,
	})
	preds.Append(predicate{
		estCard: 90,
	})

	branches := newPlannerORBranches(1)
	branches.Append(plannerORBranch{
		preds:   preds,
		leadIdx: -1,
	})
	defer branches.Release()

	var branchUniverses [plannerORBranchLimit]uint64
	branchUniverses[0] = 32

	var estimates [plannerORBranchLimit]plannerOROrderedBranchEstimate
	view.initOrderedORBranchEstimates(branches, &branchUniverses, 10, &estimates)

	if got := estimates[0].card; got != 29 {
		t.Fatalf("estimate.card=%d, want 29", got)
	}
	wantProbeRows := estimateRowsForNeed(10, 29, 32)
	if got := estimates[0].probeRows; got != wantProbeRows {
		t.Fatalf("estimate.probeRows=%d, want %d", got, wantProbeRows)
	}
}

func TestPlannerOROrder_RepeatedExecutionPromotesMaterializedRange(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.EQ("country", "DE"),
				qx.HASANY("tags", []string{"rust", "go"}),
				qx.GTE("score", 40.0),
			),
			qx.PREFIX("email", "user1"),
			qx.GTE("age", 30),
		),
	).Sort("score", qx.DESC).Limit(240)

	db.clearCurrentSnapshotCachesForTesting()
	if got := db.engine.snapshot.Current().MaterializedPredCache().EntryCount(); got != 0 {
		t.Fatalf("unexpected materialized predicate cache before ordered OR execution: %d", got)
	}

	for i := 0; i < 2; i++ {
		got, err := db.QueryKeys(q)
		if err != nil {
			t.Fatalf("QueryKeys run %d err: %v", i+1, err)
		}
		if len(got) == 0 {
			t.Fatalf("QueryKeys run %d returned no rows", i+1)
		}
	}

	if got := db.engine.snapshot.Current().MaterializedPredCache().EntryCount(); got == 0 {
		t.Fatalf("expected repeated ordered OR execution to promote materialized predicate")
	}
}

func TestPlannerOROrderKWay_RepeatedExecutionPromotesExactOnlyMaterializedRange(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 256, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   18 + i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.GTE("age", 30),
				qx.LTE("age", 250),
			),
			qx.EQ("name", "u_1"),
		),
	).Sort("score", qx.DESC).Limit(96)

	db.clearCurrentSnapshotCachesForTesting()
	if got := db.engine.snapshot.Current().MaterializedPredCache().EntryCount(); got != 0 {
		t.Fatalf("unexpected materialized predicate cache before ordered OR k-way execution: %d", got)
	}

	window, ok := orderWindowForTest(q)
	if !ok {
		t.Fatalf("orderWindowForTest: ok=false")
	}

	for i := 0; i < 2; i++ {
		branches, alwaysFalse, ok := db.engine.buildORBranchesOrdered(q.Filter.Args, "score", window)
		if !ok {
			t.Fatalf("buildORBranchesOrdered run %d: ok=false", i+1)
		}
		if alwaysFalse {
			branches.Release()
			t.Fatalf("unexpected alwaysFalse on run %d", i+1)
		}

		foundExactOnlyAge := false
		for bi := 0; bi < branches.Len(); bi++ {
			branch := branches.owner[bi]
			checks := pooled.GetIntSlice(branch.preds.Len())
			exactChecks := pooled.GetIntSlice(branch.preds.Len())
			residualChecks := pooled.GetIntSlice(branch.preds.Len())
			checks = branch.buildMatchChecks(checks)
			exactChecks = buildExactBucketPostingFilterActiveReader(exactChecks, checks, branch.preds.owner)
			residualChecks = plannerResidualChecks(residualChecks, checks, exactChecks)
			checkLen := len(checks)
			exactLen := len(exactChecks)
			residualLen := len(residualChecks)
			ageBranch := false
			for pi := 0; pi < branch.preds.Len(); pi++ {
				if db.engine.fieldNameByOrdinal(branch.preds.owner[pi].expr.FieldOrdinal) == "age" {
					ageBranch = true
					break
				}
			}
			pooled.ReleaseIntSlice(residualChecks)
			pooled.ReleaseIntSlice(exactChecks)
			pooled.ReleaseIntSlice(checks)
			if !ageBranch {
				continue
			}
			if checkLen == 1 && exactLen == 1 && residualLen == 0 {
				foundExactOnlyAge = true
				continue
			}
			branches.Release()
			t.Fatalf("expected exact-only merged age branch on run %d, branch %d: checks=%d exact=%d residual=%d", i+1, bi, checkLen, exactLen, residualLen)
		}
		if !foundExactOnlyAge {
			branches.Release()
			t.Fatalf("expected exact-only merged age branch on run %d", i+1)
		}

		got, ok, err := db.engine.execPlanOROrderKWay(q, branches, nil)
		branches.Release()
		if err != nil {
			t.Fatalf("execPlanOROrderKWay run %d err: %v", i+1, err)
		}
		if !ok {
			t.Fatalf("execPlanOROrderKWay run %d: ok=false", i+1)
		}
		if len(got) == 0 {
			t.Fatalf("execPlanOROrderKWay run %d returned no rows", i+1)
		}
	}

	if got := db.engine.snapshot.Current().MaterializedPredCache().EntryCount(); got == 0 {
		t.Fatalf("expected repeated ordered OR k-way execution to promote exact-only materialized predicate")
	}
}

func TestPlannerOROrder_WarmMaterializationRefreshesAnalysisBeforeDecision(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 256, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   18 + i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.GTE("age", 30),
				qx.LTE("age", 250),
			),
			qx.EQ("name", "u_1"),
		),
	).Sort("score", qx.DESC).Offset(32).Limit(120)

	view := db.engine.currentQueryViewForTests()
	warm, ok := db.engine.buildPredicatesOrderedWithMode(
		[]qx.Expr{qx.GTE("age", 30), qx.LTE("age", 250)},
		"score",
		false,
		4096,
		0,
		false,
		false,
	)
	if !ok {
		t.Fatalf("warm buildPredicatesOrderedWithMode: ok=false")
	}
	if len(warm) != 1 {
		releasePredicates(warm)
		t.Fatalf("unexpected warm predicate count: %d", len(warm))
	}
	if !warm[0].hasEffectiveBounds {
		releasePredicates(warm)
		t.Fatal("expected merged warm predicate with effective bounds")
	}
	if !view.materializeOrderedORPredicate(&warm[0]) {
		releasePredicates(warm)
		t.Fatal("expected exact-range warm predicate to materialize")
	}
	releasePredicates(warm)

	window, _ := orderWindowForTest(q)
	branches, alwaysFalse, ok := db.engine.buildORBranchesOrderedWithOffset(q.Filter.Args, "score", window, q.Window.Offset)
	if !ok {
		t.Fatalf("buildORBranchesOrdered: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for ordered OR branches")
	}
	defer branches.Release()

	preparedQ, viewQ, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer preparedQ.Release()

	analysis, ok := view.buildOROrderAnalysis(&viewQ, branches)
	if !ok {
		t.Fatalf("buildOROrderAnalysis: ok=false")
	}
	defer analysis.release()

	for bi := 0; bi < branches.Len(); bi++ {
		branch := branches.owner[bi]
		for pi := 0; pi < branch.preds.Len(); pi++ {
			_ = view.orderedORPredicateBuildInfoForBranch("score", branch.preds.owner[pi], &analysis, branch, bi, pi)
		}
	}

	if !view.maybeMaterializeOrderedORPredicates(&viewQ, branches, &analysis, false, false, nil) {
		t.Fatalf("expected warm ordered-OR materialization to rewrite predicates")
	}

	foundMaterialized := false
	refreshedAnalysis := false
	for bi := 0; bi < branches.Len(); bi++ {
		branch := branches.owner[bi]
		for pi := 0; pi < branch.preds.Len(); pi++ {
			p := branch.preds.owner[pi]
			if p.isMaterializedLike() {
				foundMaterialized = true
				if analysis.branches[bi].buildReady || analysis.branches[bi].predBuild != nil {
					t.Fatalf("expected rewritten branch %d analysis cache to be invalidated", bi)
				}
				refreshedAnalysis = true
				break
			}
		}
	}

	if !foundMaterialized {
		t.Fatalf("expected warm ordered-OR rewrite to materialize at least one branch predicate")
	}
	if !refreshedAnalysis {
		t.Fatalf("expected warm ordered-OR rewrite to refresh cached branch analysis")
	}
}

func TestPlannerOROrder_RefreshBranchCollapsesCoveredTautology(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 256, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   18 + i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.GTE("score", 128.0),
				qx.GTE("age", 30),
			),
			qx.EQ("name", "u_1"),
		),
	).Sort("score", qx.DESC).Limit(64)

	window, _ := orderWindowForTest(q)
	branches, alwaysFalse, ok := db.engine.buildORBranchesOrdered(q.Filter.Args, "score", window)
	if !ok {
		t.Fatalf("buildORBranchesOrdered: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for ordered OR branches")
	}
	defer branches.Release()

	preparedQ, viewQ, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer preparedQ.Release()

	view := db.engine.currentQueryViewForTests()
	analysis, ok := view.buildOROrderAnalysis(&viewQ, branches)
	if !ok {
		t.Fatalf("buildOROrderAnalysis: ok=false")
	}
	defer analysis.release()
	analysis.applyCovered(branches)

	targetBranch := -1
	targetPred := -1
	for bi := 0; bi < branches.Len(); bi++ {
		branch := branches.owner[bi]
		for pi := 0; pi < branch.preds.Len(); pi++ {
			if view.exec.FieldNameByOrdinal(branch.preds.owner[pi].expr.FieldOrdinal) != "age" {
				continue
			}
			targetBranch = bi
			targetPred = pi
			break
		}
		if targetBranch >= 0 {
			break
		}
	}
	if targetBranch < 0 || targetPred < 0 {
		t.Fatalf("expected ordered OR branch with residual age predicate")
	}
	if !analysis.mergeStats[targetBranch].rangeBounded {
		t.Fatalf("expected bounded ordered range for target branch")
	}

	branch := &branches.owner[targetBranch]
	setPredicateAlwaysTrue(branch.predPtr(targetPred))
	analysis.refreshBranch(branches, targetBranch)

	if branch.alwaysTrue {
		t.Fatalf("expected covered tautology branch to stay range-bounded, not global alwaysTrue")
	}
	if branch.leadIdx != -1 {
		t.Fatalf("expected covered tautology branch to drop lead, got leadIdx=%d", branch.leadIdx)
	}
	if !branch.estKnown {
		t.Fatalf("expected covered tautology branch to get exact bounded cardinality")
	}
	if branch.estCard != analysis.branches[targetBranch].universe {
		t.Fatalf(
			"expected covered tautology branch cardinality=%d, got=%d",
			analysis.branches[targetBranch].universe,
			branch.estCard,
		)
	}
	if !branch.coveredRangeBounded {
		t.Fatalf("expected covered tautology branch to retain bounded range metadata")
	}
	if branch.coveredRangeStart != analysis.branches[targetBranch].rangeStart ||
		branch.coveredRangeEnd != analysis.branches[targetBranch].rangeEnd {
		t.Fatalf(
			"expected covered range [%d,%d), got [%d,%d)",
			analysis.branches[targetBranch].rangeStart,
			analysis.branches[targetBranch].rangeEnd,
			branch.coveredRangeStart,
			branch.coveredRangeEnd,
		)
	}
	if analysis.mergeStats[targetBranch].streamChecks != 0 || analysis.mergeStats[targetBranch].mergeChecks != 0 {
		t.Fatalf(
			"expected no remaining checks after covered tautology collapse, got stream=%d merge=%d",
			analysis.mergeStats[targetBranch].streamChecks,
			analysis.mergeStats[targetBranch].mergeChecks,
		)
	}
}

func TestPlannerOROrder_RefreshBranchCollapsesImpossibleBranch(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 256, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   18 + i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.GTE("score", 128.0),
				qx.GTE("age", 30),
			),
			qx.EQ("name", "u_1"),
		),
	).Sort("score", qx.DESC).Limit(64)

	window, _ := orderWindowForTest(q)
	branches, alwaysFalse, ok := db.engine.buildORBranchesOrdered(q.Filter.Args, "score", window)
	if !ok {
		t.Fatalf("buildORBranchesOrdered: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for ordered OR branches")
	}
	defer branches.Release()

	preparedQ, viewQ, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer preparedQ.Release()

	view := db.engine.currentQueryViewForTests()
	analysis, ok := view.buildOROrderAnalysis(&viewQ, branches)
	if !ok {
		t.Fatalf("buildOROrderAnalysis: ok=false")
	}
	defer analysis.release()
	analysis.applyCovered(branches)

	targetBranch := -1
	targetPred := -1
	for bi := 0; bi < branches.Len(); bi++ {
		branch := branches.owner[bi]
		for pi := 0; pi < branch.preds.Len(); pi++ {
			if view.exec.FieldNameByOrdinal(branch.preds.owner[pi].expr.FieldOrdinal) != "age" {
				continue
			}
			targetBranch = bi
			targetPred = pi
			break
		}
		if targetBranch >= 0 {
			break
		}
	}
	if targetBranch < 0 || targetPred < 0 {
		t.Fatalf("expected ordered OR branch with residual age predicate")
	}
	if !analysis.mergeStats[targetBranch].rangeBounded {
		t.Fatalf("expected bounded ordered range for target branch")
	}
	if analysis.branches[targetBranch].rangeStart >= analysis.branches[targetBranch].rangeEnd {
		t.Fatalf("expected target branch to start with non-empty ordered span")
	}

	branch := &branches.owner[targetBranch]
	branch.predPtr(targetPred).alwaysFalse = true
	analysis.refreshBranch(branches, targetBranch)

	if branch.alwaysTrue {
		t.Fatalf("expected impossible branch to stay non-tautological")
	}
	if branch.leadIdx != -1 {
		t.Fatalf("expected impossible branch to drop lead, got leadIdx=%d", branch.leadIdx)
	}
	if !branch.estKnown || branch.estCard != 0 {
		t.Fatalf("expected impossible branch to collapse to estCard=0, got known=%v card=%d", branch.estKnown, branch.estCard)
	}
	if !branch.coveredRangeBounded {
		t.Fatalf("expected impossible branch to collapse to empty covered range")
	}
	if branch.coveredRangeStart != 0 || branch.coveredRangeEnd != 0 {
		t.Fatalf("expected impossible branch covered range [0,0), got [%d,%d)", branch.coveredRangeStart, branch.coveredRangeEnd)
	}
	if analysis.branches[targetBranch].rangeStart != 0 || analysis.branches[targetBranch].rangeEnd != 0 {
		t.Fatalf(
			"expected impossible branch analysis range [0,0), got [%d,%d)",
			analysis.branches[targetBranch].rangeStart,
			analysis.branches[targetBranch].rangeEnd,
		)
	}
	if analysis.branches[targetBranch].universe != 0 {
		t.Fatalf("expected impossible branch analysis universe=0, got=%d", analysis.branches[targetBranch].universe)
	}
	if !analysis.mergeStats[targetBranch].rangeBounded || analysis.mergeStats[targetBranch].rangeRows != 0 {
		t.Fatalf(
			"expected impossible branch merge stats to collapse to empty range, got bounded=%v rows=%d",
			analysis.mergeStats[targetBranch].rangeBounded,
			analysis.mergeStats[targetBranch].rangeRows,
		)
	}
	if analysis.mergeStats[targetBranch].streamChecks != 0 || analysis.mergeStats[targetBranch].mergeChecks != 0 {
		t.Fatalf(
			"expected impossible branch checks to collapse to zero, got stream=%d merge=%d",
			analysis.mergeStats[targetBranch].streamChecks,
			analysis.mergeStats[targetBranch].mergeChecks,
		)
	}
}

func TestPlannerORBranchCheckCounts_LargeImpossibleBranchWithZeroLenCoveredReturnsZero(t *testing.T) {
	preds := newPredicateSet(9)
	for i := 0; i < 9; i++ {
		if i == 4 {
			preds.Append(predicate{alwaysFalse: true})
			continue
		}
		ids := posting.BuildFromSorted([]uint64{uint64(i + 1)})
		preds.Append(predicate{
			kind:       predicateKindMaterialized,
			ids:        ids,
			releaseIDs: true,
			estCard:    1,
		})
	}
	defer preds.Release()

	covered := pooled.GetBoolSlice(0)
	defer pooled.ReleaseBoolSlice(covered)

	streamChecks, mergeChecks := plannerORBranchCheckCounts(
		plannerORBranch{preds: preds, leadIdx: -1},
		covered,
	)
	if streamChecks != 0 || mergeChecks != 0 {
		t.Fatalf("expected impossible large branch to report zero checks, got stream=%d merge=%d", streamChecks, mergeChecks)
	}
}

func TestPlannerOROrder_MergeWarmupMaterializesExactPredicate(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 256, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   18 + i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	view := db.engine.currentQueryViewForTests()
	warm, ok := db.engine.buildPredicatesOrderedWithMode(
		[]qx.Expr{qx.GTE("age", 30), qx.LTE("age", 250)},
		"score",
		false,
		4096,
		0,
		false,
		false,
	)
	if !ok {
		t.Fatalf("warm buildPredicatesOrderedWithMode: ok=false")
	}
	if len(warm) != 1 {
		releasePredicates(warm)
		t.Fatalf("unexpected warm predicate count: %d", len(warm))
	}
	if !warm[0].hasEffectiveBounds || !warm[0].postingFilterCapability().supportsExactBucket() {
		releasePredicates(warm)
		t.Fatalf("expected exact-range warm predicate")
	}
	if !view.materializeOrderedORPredicate(&warm[0]) {
		releasePredicates(warm)
		t.Fatalf("expected exact-range warm predicate to materialize")
	}
	releasePredicates(warm)

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.GTE("age", 30),
				qx.LTE("age", 250),
			),
			qx.EQ("name", "u_1"),
		),
	).Sort("score", qx.DESC).Offset(32).Limit(120)

	window, _ := orderWindowForTest(q)
	branches, alwaysFalse, ok := db.engine.buildORBranchesOrderedWithOffset(q.Filter.Args, "score", window, q.Window.Offset)
	if !ok {
		t.Fatalf("buildORBranchesOrdered: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for ordered OR branches")
	}
	defer branches.Release()

	preparedQ, viewQ, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer preparedQ.Release()

	analysis, ok := view.buildOROrderAnalysis(&viewQ, branches)
	if !ok {
		t.Fatalf("buildOROrderAnalysis: ok=false")
	}
	defer analysis.release()

	targetBranch := -1
	targetPred := -1
	for bi := 0; bi < branches.Len(); bi++ {
		branch := branches.owner[bi]
		for pi := 0; pi < branch.preds.Len(); pi++ {
			p := branch.preds.owner[pi]
			if view.exec.FieldNameByOrdinal(p.expr.FieldOrdinal) != "age" {
				continue
			}
			if !p.hasEffectiveBounds || !p.postingFilterCapability().supportsExactBucket() {
				continue
			}
			targetBranch = bi
			targetPred = pi
			break
		}
		if targetBranch >= 0 {
			break
		}
	}
	if targetBranch < 0 || targetPred < 0 {
		t.Fatalf("expected ordered OR branch with exact age predicate")
	}
	if (&branches.owner[targetBranch]).predPtr(targetPred).isMaterializedLike() {
		t.Fatalf("expected target exact predicate to start non-materialized")
	}

	if !view.maybeMaterializeOrderedORPredicates(&viewQ, branches, &analysis, true, false, nil) {
		t.Fatalf("expected merge warmup to materialize exact predicate from warm cache")
	}
	if !(&branches.owner[targetBranch]).predPtr(targetPred).isMaterializedLike() {
		t.Fatalf("expected merge warmup to rewrite exact predicate into materialized form")
	}
}

func TestOrderedORMaterializedPredicateWarmHitWorth(t *testing.T) {
	tests := []struct {
		name            string
		leafChecks      uint64
		checkWork       uint64
		cachedCheckWork uint64
		want            bool
	}{
		{
			name:            "zero_checks",
			leafChecks:      0,
			checkWork:       32,
			cachedCheckWork: 8,
			want:            false,
		},
		{
			name:            "no_per_query_gain",
			leafChecks:      4,
			checkWork:       8,
			cachedCheckWork: 8,
			want:            false,
		},
		{
			name:            "single_check_near_tie",
			leafChecks:      1,
			checkWork:       12,
			cachedCheckWork: 8,
			want:            false,
		},
		{
			name:            "single_check_clear_gain",
			leafChecks:      1,
			checkWork:       24,
			cachedCheckWork: 8,
			want:            true,
		},
		{
			name:            "repeated_checks_amortize",
			leafChecks:      3,
			checkWork:       12,
			cachedCheckWork: 8,
			want:            true,
		},
	}

	for _, tt := range tests {
		if got := orderedORMaterializedPredicateWarmHitWorth(tt.leafChecks, tt.checkWork, tt.cachedCheckWork); got != tt.want {
			t.Fatalf("%s: got=%v want=%v", tt.name, got, tt.want)
		}
	}
}

func TestPlannerORBranchesOrdered_CoversOrderRangeLeaves(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.GTE("age", 25),
				qx.EQ("active", true),
			),
			qx.AND(
				qx.LTE("age", 45),
				qx.EQ("country", "NL"),
			),
		),
	).Sort("age", qx.ASC).Limit(120)

	window, _ := orderWindowForTest(q)
	branches, alwaysFalse, ok := db.engine.buildORBranchesOrdered(q.Filter.Args, "age", window)
	if !ok {
		t.Fatalf("buildORBranchesOrdered: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for ordered OR branches")
	}
	defer branches.Release()

	covered := 0
	for i := 0; i < branches.Len(); i++ {
		branch := branches.owner[i]
		for i := 0; i < branch.preds.Len(); i++ {
			p := branch.preds.owner[i]
			if p.covered && db.engine.fieldNameByOrdinal(p.expr.FieldOrdinal) == "age" {
				covered++
			}
		}
	}
	if covered == 0 {
		t.Fatalf("expected ordered OR branches to cover order-field range leaves")
	}

	got, ok := db.engine.execPlanOROrderBasic(q, branches, nil)
	if !ok {
		t.Fatalf("execPlanOROrderBasic: ok=false")
	}
	want, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys(%+v): %v", q, err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("ordered OR basic mismatch:\ngot=%v\nwant=%v", got, want)
	}
}

func TestBuildOROrderAnalysis_NonRangeOrderPredicateKeepsOrderedPath(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.CONTAINS("full_name", "FN-1"),
				qx.EQ("active", true),
			),
			qx.AND(
				qx.EQ("country", "NL"),
				qx.GTE("age", 25),
			),
		),
	).Sort("full_name", qx.ASC).Limit(120)

	window, _ := orderWindowForTest(q)
	branchesBasic, alwaysFalse, ok := db.engine.buildORBranchesOrdered(q.Filter.Args, "full_name", window)
	if !ok {
		t.Fatalf("buildORBranchesOrdered basic: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for ordered OR branches")
	}
	defer branchesBasic.Release()

	preparedQ, viewQ, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer preparedQ.Release()

	view := db.engine.currentQueryViewForTests()

	analysis, ok := view.buildOROrderAnalysis(&viewQ, branchesBasic)
	if !ok {
		t.Fatalf("buildOROrderAnalysis: ok=false for non-range order predicate")
	}
	defer analysis.release()

	orderOV := view.fieldIndexViewFromSlotsForOrder(view.snap.Index, viewQ.Order)
	if analysis.branches[0].rangeStart != 0 || analysis.branches[0].rangeEnd != orderOV.KeyCount() {
		t.Fatalf(
			"expected non-range order predicate branch to keep full-span order coverage, got start=%d end=%d want_end=%d",
			analysis.branches[0].rangeStart,
			analysis.branches[0].rangeEnd,
			orderOV.KeyCount(),
		)
	}
	if covered := analysis.branches[0].covered; covered != nil && len(covered) != 0 {
		t.Fatalf("expected non-range order predicate branch to keep zero covered leaves, got=%d", len(covered))
	}

	gotBasic, ok := view.execPlanOROrderBasic(&viewQ, branchesBasic, &analysis, nil, nil)
	if !ok {
		t.Fatalf("execPlanOROrderBasic: ok=false")
	}

	branchesFallback, alwaysFalse, ok := db.engine.buildORBranchesOrdered(q.Filter.Args, "full_name", window)
	if !ok {
		t.Fatalf("buildORBranchesOrdered fallback: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for ordered OR fallback branches")
	}
	defer branchesFallback.Release()

	gotFallback, ok, err := view.execPlanOROrderMergeFallback(&viewQ, branchesFallback, nil)
	if err != nil {
		t.Fatalf("execPlanOROrderMergeFallback err: %v", err)
	}
	if !ok {
		t.Fatalf("execPlanOROrderMergeFallback: ok=false")
	}

	want, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys(%+v): %v", q, err)
	}
	if !slices.Equal(gotBasic, want) {
		t.Fatalf("ordered OR basic with non-range order predicate mismatch:\ngot=%v\nwant=%v", gotBasic, want)
	}
	if !slices.Equal(gotFallback, want) {
		t.Fatalf("ordered OR fallback with non-range order predicate mismatch:\ngot=%v\nwant=%v", gotFallback, want)
	}
}

func TestPlannerOROrderDecision_PrefersStreamWhenAllBranchesAreOrderBounded(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.GTE("age", 25),
				qx.EQ("country", "NL"),
			),
			qx.AND(
				qx.LTE("age", 45),
				qx.EQ("name", "alice"),
			),
		),
	).Sort("age", qx.ASC).Limit(120)

	view := db.engine.currentQueryViewForTests()
	window, _ := orderWindowForTest(q)
	branches, alwaysFalse, ok := db.engine.buildORBranchesOrdered(q.Filter.Args, "age", window)
	if !ok {
		t.Fatalf("buildORBranchesOrdered: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for ordered OR branches")
	}
	defer branches.Release()

	preparedQ, viewQ, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer preparedQ.Release()

	decision := view.selectPlanOROrder(&viewQ, branches)
	if decision.plan != plannerOROrderStream && decision.plan != plannerOROrderMerge {
		t.Fatalf("expected ordered OR stream/merge plan for fully order-bounded branches, got=%v", decision.plan)
	}
}

func TestPlannerORBranchesOrdered_BoundedCoveredOnlyBranchNotAlwaysTrue(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.NOTIN("country", []string{"Finland", "Switzerland", "Iceland"}),
				qx.LT("score", 50.0),
			),
			qx.AND(
				qx.HASANY("tags", []string{"ops"}),
				qx.EQ("country", "Switzerland"),
				qx.NOTIN("country", []string{"Switzerland"}),
			),
			qx.EQ("name", "carol"),
			qx.GTE("age", 20),
		),
	).Sort("age", qx.ASC).Limit(9)

	view := db.engine.currentQueryViewForTests()
	window, _ := orderWindowForTest(q)
	branches, alwaysFalse, ok := db.engine.buildORBranchesOrdered(q.Filter.Args, "age", window)
	if !ok {
		t.Fatalf("buildORBranchesOrdered: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for ordered OR branches")
	}
	defer branches.Release()

	foundAgeRange := false
	snap := view.exec.Stats.Load()
	universe := snap.UniverseOr(view.snap.Universe.Cardinality())
	ov := view.fieldIndexViewFromSlotsByName(view.snap.Index, "age")
	for i := 0; i < branches.Len(); i++ {
		branch := &branches.owner[i]
		if branch.expr.Op == compileScalarOpForTest(qx.OpGTE) && db.engine.fieldNameByOrdinal(branch.expr.FieldOrdinal) == "age" {
			foundAgeRange = true
			if branch.alwaysTrue {
				t.Fatalf("bounded covered-only age branch must not become alwaysTrue")
			}
			if !branch.coveredRangeBounded {
				t.Fatalf("bounded covered-only age branch must retain covered range metadata")
			}
			br, _, ok := view.extractOrderRangeCoverageIndexViewReader("age", branch.preds.owner, ov)
			if !ok {
				t.Fatalf("extractOrderRangeCoverageOverlay: ok=false")
			}
			_, wantCard := ov.RangeStats(br)
			mergeStats := view.orderMergeBranchStats("age", branches, ov)
			if mergeStats[i].rangeRows != wantCard {
				t.Fatalf("rangeRows=%d, want %d", mergeStats[i].rangeRows, wantCard)
			}
			if branch.estimatedCard(universe) != wantCard {
				t.Fatalf("estimatedCard=%d, want rangeRows=%d", branch.estimatedCard(universe), wantCard)
			}
		}
	}
	if !foundAgeRange {
		t.Fatalf("expected bounded age branch in ordered OR branches")
	}

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(%+v): %v", q, err)
	}
	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys(%+v): %v", q, err)
	}
	if !queryIDsEqual(q, got, want) {
		t.Fatalf("ordered OR public planner mismatch:\ngot=%v\nwant=%v", got, want)
	}
}

func TestPlannerORBranchesOrdered_EmptyCoveredOnlyBranchKeepsZeroEstimatedCard(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.OR(
			qx.LT("age", -1),
			qx.EQ("name", "alice"),
		),
	).Sort("age", qx.ASC).Limit(9)

	view := db.engine.currentQueryViewForTests()
	window, _ := orderWindowForTest(q)
	branches, alwaysFalse, ok := db.engine.buildORBranchesOrdered(q.Filter.Args, "age", window)
	if !ok {
		t.Fatalf("buildORBranchesOrdered: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for ordered OR branches")
	}
	defer branches.Release()

	snap := view.exec.Stats.Load()
	universe := snap.UniverseOr(view.snap.Universe.Cardinality())
	foundAgeRange := false
	for i := 0; i < branches.Len(); i++ {
		branch := branches.owner[i]
		if branch.expr.Op != compileScalarOpForTest(qx.OpLT) || db.engine.fieldNameByOrdinal(branch.expr.FieldOrdinal) != "age" {
			continue
		}
		foundAgeRange = true
		if branch.alwaysTrue {
			t.Fatalf("empty covered-only age branch must not become alwaysTrue")
		}
		if !branch.estKnown {
			t.Fatalf("empty covered-only age branch must keep known zero cardinality")
		}
		if got := branch.estimatedCard(universe); got != 0 {
			t.Fatalf("estimatedCard=%d, want 0", got)
		}
	}
	if !foundAgeRange {
		t.Fatalf("expected empty age branch in ordered OR branches")
	}
}

func TestPlannerOROrderDecision_PrefersMergeWhenRouteEstimatorBeatsStream(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.EQ("country", "DE"),
				qx.HASANY("tags", []string{"rust", "go"}),
				qx.GTE("score", 40.0),
			),
			qx.PREFIX("email", "user1"),
			qx.GTE("age", 30),
		),
	).Sort("score", qx.DESC).Limit(120)

	view := db.engine.currentQueryViewForTests()
	window, _ := orderWindowForTest(q)
	branches, alwaysFalse, ok := db.engine.buildORBranchesOrdered(q.Filter.Args, "score", window)
	if !ok {
		t.Fatalf("buildORBranchesOrdered: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for ordered OR branches")
	}
	defer branches.Release()

	snap := view.exec.Stats.Load()
	universe := snap.UniverseOr(view.snap.Universe.Cardinality())
	if universe == 0 {
		t.Fatalf("unexpected zero universe")
	}
	ov := view.fieldIndexViewFromSlotsByName(view.snap.Index, "score")
	orderDistinct := uint64(ov.KeyCount())
	if orderDistinct == 0 {
		t.Fatalf("unexpected zero order distinct")
	}
	var branchCards [plannerORBranchLimit]uint64
	unionCard, _, _, hasAlwaysTrue := branches.unionCards(universe, &branchCards)
	if hasAlwaysTrue || unionCard == 0 {
		t.Fatalf("unexpected OR shape metrics: alwaysTrue=%v unionCard=%d", hasAlwaysTrue, unionCard)
	}
	expectedRows := estimateRowsForNeed(uint64(window), unionCard, universe)
	mergeStats := view.orderMergeBranchStats("score", branches, ov)
	streamChecks := branches.orderStreamChecksByBranch(false, mergeStats)
	orderStats := view.plannerOrderFieldStats("score", snap, universe, orderDistinct)
	streamCost := float64(expectedRows) * streamChecks * plannerFieldStatsSkew(orderStats)

	preparedQ, viewQ, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer preparedQ.Release()

	routeCost, ok := view.estimateOROrderMergeRouteCost(&viewQ, branches, window, mergeStats)
	if !ok {
		t.Fatalf("estimateOROrderMergeRouteCost: ok=false")
	}
	if routeCost.kWay >= streamCost {
		t.Fatalf("expected merge route estimate to beat stream: kway=%v stream=%v", routeCost.kWay, streamCost)
	}

	decision := view.selectPlanOROrder(&viewQ, branches)
	if decision.plan != plannerOROrderMerge {
		if decision.plan != plannerOROrderStream || !branches.hasKWayExactBucketApplyWork(mergeStats) {
			t.Fatalf("expected ordered OR merge or stream vetoed by exact bucket work, got=%v", decision.plan)
		}
	}
}

func TestPlannerORBranchesOrdered_AvoidsMaterializingDeferredOrderRangeLeaves(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
		return &Rec{
			Name:  "u",
			Age:   i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	scoreExpr := qx.GTE("score", 500.0)
	ageExpr := qx.GTE("age", 500)
	leaves := []qx.Expr{
		scoreExpr,
		ageExpr,
	}

	preds, ok := db.engine.buildPredicatesOrderedWithMode(leaves, "score", false, 4096, 0, false, true)
	if !ok {
		t.Fatalf("buildPredicatesOrderedWithMode: ok=false")
	}
	defer releasePredicates(preds)

	if len(preds) != 2 {
		t.Fatalf("unexpected preds len: %d", len(preds))
	}
	if db.engine.fieldNameByOrdinal(preds[0].expr.FieldOrdinal) != "score" || db.engine.fieldNameByOrdinal(preds[1].expr.FieldOrdinal) != "age" {
		t.Fatalf("unexpected predicate order: %+v", preds)
	}
	if preds[0].kind == predicateKindMaterialized || preds[0].kind == predicateKindMaterializedNot {
		t.Fatalf("expected deferred order-field range to avoid materialization, got kind=%v", preds[0].kind)
	}
	if preds[1].covered {
		t.Fatalf("unexpected covered residual predicate")
	}
	if !preds[1].hasContains() {
		t.Fatalf("expected residual non-order range to remain active predicate")
	}

	ageKey := db.engine.materializedPredCacheKey(ageExpr)
	_ = ageKey
	scoreKey := db.engine.materializedPredCacheKey(scoreExpr)
	if scoreKey != "" {
		if _, ok := snapshotExtLoadMaterializedPred(db.engine.snapshot.Current(), scoreKey); ok {
			t.Fatalf("unexpected materialized cache entry for deferred order-field range")
		}
	}
}

func TestBuildPredicatesOrdered_MergesPositiveNumericRangeLeavesOnSameField(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})

	seedGeneratedUint64Data(t, db, 1_000, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	leaves := []qx.Expr{
		qx.GTE("score", 500.0),
		qx.GTE("age", 250),
		qx.LTE("age", 400),
	}

	preds, ok := db.engine.buildPredicatesOrderedWithMode(leaves, "score", false, 4096, 0, false, true)
	if !ok {
		t.Fatalf("buildPredicatesOrderedWithMode: ok=false")
	}
	defer releasePredicates(preds)

	if len(preds) != 2 {
		t.Fatalf("unexpected preds len: %d", len(preds))
	}
	if db.engine.fieldNameByOrdinal(preds[0].expr.FieldOrdinal) != "score" || db.engine.fieldNameByOrdinal(preds[1].expr.FieldOrdinal) != "age" {
		t.Fatalf("unexpected predicate order: %+v", preds)
	}
	probeLen := 0
	switch {
	case preds[1].fieldIndexRangeState != nil:
		probeLen = preds[1].fieldIndexRangeState.probe.probeLen
	default:
		t.Fatalf("expected merged age predicate to remain runtime range state")
	}
	if probeLen != 151 {
		t.Fatalf("unexpected merged probe len: got %d want %d", probeLen, 151)
	}
}

func TestBuildPredicates_MergesPositiveNumericRangeLeavesOnSameField(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	seedGeneratedUint64Data(t, db, 1_000, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	leaves := []qx.Expr{
		qx.EQ("active", true),
		qx.GTE("age", 250),
		qx.LTE("age", 400),
	}

	preds, ok := db.engine.buildPredicates(leaves)
	if !ok {
		t.Fatalf("buildPredicates: ok=false")
	}
	defer releasePredicates(preds)

	if len(preds) != 2 {
		t.Fatalf("unexpected preds len: %d", len(preds))
	}
	ageIdx := -1
	for i := range preds {
		if db.engine.fieldNameByOrdinal(preds[i].expr.FieldOrdinal) == "age" {
			ageIdx = i
			break
		}
	}
	if ageIdx < 0 {
		t.Fatalf("expected merged age predicate")
	}
	if !preds[ageIdx].hasEffectiveBounds {
		t.Fatalf("expected merged range predicate to keep exact bounds")
	}

	lo, ok := db.engine.buildPredicateWithMode(qx.GTE("age", 250), false)
	if !ok {
		t.Fatalf("build lower predicate: ok=false")
	}
	defer releasePredicates([]predicate{lo})

	hi, ok := db.engine.buildPredicateWithMode(qx.LTE("age", 400), false)
	if !ok {
		t.Fatalf("build upper predicate: ok=false")
	}
	defer releasePredicates([]predicate{hi})

	universe := db.engine.snapshot.Current().Universe
	var want []uint64
	universe.ForEach(func(idx uint64) bool {
		if lo.matches(idx) && hi.matches(idx) {
			want = append(want, idx)
		}
		return true
	})
	got := predicateMatchIDs(preds[ageIdx], universe)
	if !slices.Equal(got, want) {
		t.Fatalf("merged predicate mismatch: got=%v want=%v", got, want)
	}
}

func TestPlannerOROrderMergeBranchStats_SkipFullSpanRowCountingWithoutOrderBounds(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.OR(
			qx.EQ("country", "NL"),
			qx.EQ("name", "alice"),
		),
	).Sort("age", qx.ASC).Limit(120)

	view := db.engine.currentQueryViewForTests()
	window, _ := orderWindowForTest(q)
	branches, alwaysFalse, ok := db.engine.buildORBranchesOrdered(q.Filter.Args, "age", window)
	if !ok {
		t.Fatalf("buildORBranchesOrdered: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for ordered OR branches")
	}
	defer branches.Release()

	stats := view.orderMergeBranchStats("age", branches, view.fieldIndexViewFromSlotsByName(view.snap.Index, "age"))
	for i := 0; i < branches.Len(); i++ {
		if stats[i].rangeRows != 0 {
			t.Fatalf("expected no full-span row counting for branch %d without order bounds, got rangeRows=%d", i, stats[i].rangeRows)
		}
	}
}

func TestBuildPredRangeCandidateWithColdMode_NullableComplementRouteKeepsPositiveRuntimeProbe(t *testing.T) {
	db, _ := openTempDBUint64PtrInt(t, Options{
		AnalyzeInterval: -1,
	})
	for i := 0; i < 100; i++ {
		rec := &PtrIntRec{Name: fmt.Sprintf("nil_%02d", i), Rank: nil, Active: i%2 == 0}
		if err := db.Set(uint64(i+1), rec); err != nil {
			t.Fatalf("Set(nil_%02d): %v", i, err)
		}
	}
	for i := 0; i < 40; i++ {
		v := 0
		rec := &PtrIntRec{Name: fmt.Sprintf("zero_%02d", i), Rank: &v, Active: i%2 == 0}
		if err := db.Set(uint64(i+101), rec); err != nil {
			t.Fatalf("Set(zero_%02d): %v", i, err)
		}
	}
	for i := 1; i <= 9; i++ {
		v := i
		rec := &PtrIntRec{Name: fmt.Sprintf("rank_%02d", i), Rank: &v, Active: true}
		if err := db.Set(uint64(i+141), rec); err != nil {
			t.Fatalf("Set(rank_%02d): %v", i, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	view := db.engine.currentQueryViewForTests()
	expr := mustTestQIRExprForDB(t, db, qx.GTE("rank", 1))
	fm := view.fieldMetaByExpr(expr)
	if fm == nil {
		t.Fatal("expected field metadata")
	}
	ov := view.fieldIndexViewFromSlotsForExpr(view.snap.Index, expr)
	if !ov.HasData() {
		t.Fatal("expected index view data")
	}

	candidate, ok := view.prepareScalarRangeRoutingCandidate(expr)
	if !ok {
		t.Fatal("prepareScalarRangeRoutingCandidate: ok=false")
	}
	if !candidate.plan.useComplement {
		t.Fatal("expected complement probe path for broad range")
	}
	if candidate.broadComplementCardinality(view.snap.Universe.Cardinality()) {
		t.Fatal("expected sparse nullable complement path to stay non-broad by rows")
	}

	p, ok := view.buildPredRangeCandidateWithColdMode(expr, fm, ov, false, false)
	if !ok {
		t.Fatal("buildPredRangeCandidateWithColdMode: ok=false")
	}
	defer releasePredicateOwnedState(&p)
	if p.fieldIndexRangeState == nil {
		t.Fatal("expected overlay range predicate state")
	}
	if p.fieldIndexRangeState.probe.useComplement {
		t.Fatal("expected nullable complement route to keep positive runtime probe")
	}
	if p.matches(1) || p.matches(50) {
		t.Fatal("nil rank rows must not match positive nullable range")
	}
	if p.matches(101) {
		t.Fatal("out-of-range zero bucket must not match positive nullable range")
	}
	if !p.matches(142) {
		t.Fatal("expected in-range row to match positive nullable range")
	}
}

func TestPlannerOROrderMergePaths_MixedExactAndNonExactChecks_MatchSeqScan(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.EQ("active", true),
				qx.HASANY("tags", []string{"go", "ops"}),
				qx.GTE("score", 40.0),
				qx.GTE("age", 25),
			),
			qx.AND(
				qx.IN("country", []string{"NL", "DE", "PL"}),
				qx.HASANY("tags", []string{"db", "rust"}),
				qx.GTE("score", 30.0),
			),
			qx.AND(
				qx.EQ("name", "alice"),
				qx.GTE("age", 20),
				qx.LTE("age", 45),
			),
		),
	).Sort("score", qx.DESC).Limit(120)

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	if len(want) == 0 {
		t.Fatalf("expected non-empty result set")
	}

	branchesKWay, alwaysFalse, ok := db.engine.buildORBranches(q.Filter.Args)
	if !ok {
		t.Fatalf("buildORBranches kway: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for kway branches")
	}
	defer branchesKWay.Release()

	gotKWay, ok, err := db.engine.execPlanOROrderKWay(q, branchesKWay, nil)
	if err != nil {
		t.Fatalf("execPlanOROrderKWay err: %v", err)
	}
	if !ok {
		t.Fatalf("execPlanOROrderKWay: ok=false")
	}

	branchesFallback, alwaysFalse, ok := db.engine.buildORBranches(q.Filter.Args)
	if !ok {
		t.Fatalf("buildORBranches fallback: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for fallback branches")
	}
	defer branchesFallback.Release()

	gotFallback, ok, err := db.engine.execPlanOROrderMergeFallback(q, branchesFallback, nil)
	if err != nil {
		t.Fatalf("execPlanOROrderMergeFallback err: %v", err)
	}
	if !ok {
		t.Fatalf("execPlanOROrderMergeFallback: ok=false")
	}

	if !slices.Equal(gotKWay, want) {
		t.Fatalf("kway/seqscan mismatch:\nkway=%v\nwant=%v", gotKWay, want)
	}
	if !slices.Equal(gotFallback, want) {
		t.Fatalf("fallback/seqscan mismatch:\nfallback=%v\nwant=%v", gotFallback, want)
	}
}

func TestPlannerORKWayShouldFallbackRuntime(t *testing.T) {
	if plannerORKWayShouldFallbackRuntimeDetailed(120, 16, 20, 300_000).fallback {
		t.Fatalf("unexpected fallback for too-small pop sample")
	}
	if plannerORKWayShouldFallbackRuntimeDetailed(120, 64, 4, 300_000).fallback {
		t.Fatalf("unexpected fallback for too-small unique sample")
	}
	if plannerORKWayShouldFallbackRuntimeDetailed(120, 64, 20, 8_000).fallback {
		t.Fatalf("unexpected fallback for too-small examined sample")
	}
	if plannerORKWayShouldFallbackRuntimeDetailed(120, 64, 50, 40_000).fallback {
		t.Fatalf("unexpected fallback for efficient stream")
	}
	if !plannerORKWayShouldFallbackRuntimeDetailed(120, 64, 12, 500_000).fallback {
		t.Fatalf("expected fallback for poor projected k-way efficiency")
	}
	if !plannerORKWayShouldFallbackRuntimeDetailed(250_000, 3_000, 2_900, 300_000).fallback {
		t.Fatalf("expected fallback for large-window low-overlap stream")
	}
}

func TestPlannerOROrderKWayRuntimeFallbackEnable(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	qPrefix := qx.Query(
		qx.OR(
			qx.AND(
				qx.EQ("active", true),
				qx.EQ("country", "NL"),
				qx.GTE("score", 30.0),
			),
			qx.AND(
				qx.EQ("name", "alice"),
				qx.GTE("age", 25),
			),
			qx.PREFIX("full_name", "FN-1"),
		),
	).Sort("age", qx.ASC).Offset(140).Limit(120)

	branchesPrefix, alwaysFalse, ok := db.engine.buildORBranches(qPrefix.Filter.Args)
	if !ok {
		t.Fatalf("buildORBranches prefix: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for prefix branches")
	}
	defer branchesPrefix.Release()

	needPrefix, ok := orderWindowForTest(qPrefix)
	if !ok {
		t.Fatalf("orderWindow prefix: ok=false")
	}
	if !db.engine.shouldUseOROrderKWayRuntimeFallback(qPrefix, branchesPrefix, needPrefix) {
		t.Fatalf("expected runtime fallback guard to be enabled for prefix/non-order shape")
	}

	qSimple := qx.Query(
		qx.OR(
			qx.EQ("country", "NL"),
			qx.EQ("name", "alice"),
		),
	).Sort("age", qx.ASC).Limit(120)

	branchesSimple, alwaysFalse, ok := db.engine.buildORBranches(qSimple.Filter.Args)
	if !ok {
		t.Fatalf("buildORBranches simple: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for simple branches")
	}
	defer branchesSimple.Release()

	needSimple, ok := orderWindowForTest(qSimple)
	if !ok {
		t.Fatalf("orderWindow simple: ok=false")
	}
	if db.engine.shouldUseOROrderKWayRuntimeFallback(qSimple, branchesSimple, needSimple) {
		t.Fatalf("expected runtime fallback guard to stay disabled for simple OR shape")
	}
}

func TestPlannerOrderedAnchor_MatchesBaseline(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.EQ("country", "NL"),
		qx.HASANY("tags", []string{"go", "db"}),
		qx.GTE("age", 30),
		qx.NOTIN("name", []string{"alice"}),
	).Sort("full_name", qx.ASC).Offset(20).Limit(80)
	leaves, ok := collectAndLeavesForTest(q.Filter)
	if !ok {
		t.Fatalf("collectAndLeaves: ok=false")
	}

	predsA, ok := db.engine.buildPredicates(leaves)
	if !ok {
		t.Fatalf("buildPredicates A: ok=false")
	}
	defer releasePredicates(predsA)

	gotAnchor, ok := db.engine.execPlanOrderedBasic(q, predsA, nil)
	if !ok {
		t.Fatalf("execPlanOrderedBasic A: ok=false")
	}

	predsB, ok := db.engine.buildPredicates(leaves)
	if !ok {
		t.Fatalf("buildPredicates B: ok=false")
	}
	defer releasePredicates(predsB)

	orderField := q.Order[0].By.Name
	view := db.engine.currentQueryViewForTests()
	ov := view.fieldIndexViewFromSlotsByName(view.snap.Index, orderField)
	br := ov.RangeByRanks(0, ov.KeyCount())
	if coveredRange, cov, ok := db.engine.extractOrderRangeCoverageIndexView(orderField, predsB, ov); ok {
		br = coveredRange
		for i := range cov {
			if cov[i] {
				predsB[i].covered = true
			}
		}
	}

	active := make([]int, 0, len(predsB))
	for i := range predsB {
		p := predsB[i]
		if p.covered || p.alwaysTrue {
			continue
		}
		if p.alwaysFalse {
			t.Fatalf("unexpected alwaysFalse predicate")
		}
		active = append(active, i)
	}

	gotBaseline := db.engine.execPlanOrderedBasicFallback(q, predsB, active, ov, br, nil)
	if !slices.Equal(gotAnchor, gotBaseline) {
		t.Fatalf("ordered anchor/fallback mismatch:\nanchor=%v\nfallback=%v", gotAnchor, gotBaseline)
	}
}

func TestPlannerRouting_PrefersOrderedAnchorForMixedPredicates(t *testing.T) {
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

	q := qx.Query(
		qx.EQ("country", "NL"),
		qx.HASANY("tags", []string{"go", "db"}),
		qx.GTE("age", 30),
		qx.NOTIN("name", []string{"alice"}),
	).Sort("full_name", qx.ASC).Offset(20).Limit(80)

	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) == 0 {
		t.Fatalf("expected non-empty ids")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]

	if ev.Plan != "plan_ordered_anchor" && ev.Plan != "plan_ordered" && ev.Plan != "plan_limit_order_basic" {
		t.Fatalf("expected ordered or order-basic plan, got %q", ev.Plan)
	}
	if ev.OrderIndexScanWidth == 0 {
		t.Fatalf("expected non-zero order index scan width")
	}
	if ev.EarlyStopReason == "" {
		t.Fatalf("expected early stop reason to be set")
	}
}

func TestOrderedFallback_TracksMatchedRowsAndExactBitmapFilters(t *testing.T) {
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

	q := qx.Query(
		qx.EQ("active", true),
		qx.HASALL("tags", []string{"go"}),
	).Sort("country", qx.ASC).Offset(200).Limit(40)

	leaves, ok := collectAndLeavesForTest(q.Filter)
	if !ok {
		t.Fatalf("collectAndLeaves: ok=false")
	}
	preds, ok := db.engine.buildPredicatesOrdered(leaves, "country")
	if !ok {
		t.Fatalf("buildPredicatesOrdered: ok=false")
	}
	defer releasePredicates(preds)

	view := db.engine.currentQueryViewForTests()
	ov := view.fieldIndexViewFromSlotsByName(view.snap.Index, "country")
	br := ov.RangeByRanks(0, ov.KeyCount())
	if br.Empty() {
		t.Fatalf("country overlay must be present")
	}

	active := make([]int, 0, len(preds))
	for i := range preds {
		p := preds[i]
		if p.covered || p.alwaysTrue {
			continue
		}
		if p.alwaysFalse {
			t.Fatalf("unexpected alwaysFalse predicate")
		}
		active = append(active, i)
	}

	preparedQ, viewQ, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer preparedQ.Release()

	tr := db.engine.beginTraceForTests(viewQ)
	if tr == nil {
		t.Fatalf("expected trace to be enabled")
	}
	tr.SetPlan(PlanOrdered)
	got := db.engine.execPlanOrderedBasicFallback(q, preds, active, ov, br, tr)
	tr.Finish(uint64(len(got)), nil)

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("ordered fallback mismatch:\ngot=%v\nwant=%v", got, want)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	if ev.PostingExactFilters == 0 {
		t.Fatalf("expected exact bitmap bucket filtering to be used, trace=%+v", ev)
	}
	if ev.RowsMatched <= ev.RowsReturned {
		t.Fatalf("expected matched rows to exceed returned rows under OFFSET, trace=%+v", ev)
	}
	if ev.OrderIndexScanWidth == 0 {
		t.Fatalf("expected non-zero order scan width")
	}
}

func TestPlannerRouting_PrefersExecutionForPrefixOrderLimit(t *testing.T) {
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

	q := qx.Query(
		qx.PREFIX("full_name", "FN-1"),
		qx.EQ("active", true),
	).Sort("full_name", qx.ASC).Limit(20)

	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) == 0 {
		t.Fatalf("expected non-empty ids")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]

	if ev.Plan != "plan_limit_order_prefix" {
		t.Fatalf("expected prefix limit route, got %q", ev.Plan)
	}
	if ev.EstimatedRows == 0 {
		t.Fatalf("expected estimated rows to be set")
	}
	if ev.EstimatedCost <= 0 {
		t.Fatalf("expected estimated cost to be set")
	}
	if ev.FallbackCost <= 0 {
		t.Fatalf("expected fallback cost to be set")
	}
	if ev.RowsExamined == 0 {
		t.Fatalf("expected rows examined to be set")
	}
}

func TestPlannerRouting_PrefersExecutionForPrefixNoOrderLimit(t *testing.T) {
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

	q := qx.Query(
		qx.PREFIX("full_name", "FN-10"),
		qx.EQ("active", true),
	).Limit(15)

	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) == 0 {
		t.Fatalf("expected non-empty ids")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]

	if ev.Plan != "plan_limit_range_no_order" {
		t.Fatalf("expected no-order prefix limit route, got %q", ev.Plan)
	}
}

func TestPlannerRouting_PrefersExecutionForWidePrefixNoOrderLimit(t *testing.T) {
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

	q := qx.Query(
		qx.PREFIX("full_name", "FN-"),
		qx.EQ("active", true),
	).Limit(15)

	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) == 0 {
		t.Fatalf("expected non-empty ids")
	}

	nq := q
	leaves, ok := collectAndLeavesForTest(nq.Filter)
	if !ok || len(leaves) == 0 {
		t.Fatalf("extractAndLeaves: ok=%v len=%d", ok, len(leaves))
	}
	if !db.engine.shouldPreferExecutionNoOrderPrefix(nq, leaves) {
		t.Fatalf("expected execution preference for wide prefix with selective filter")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	if ev.Plan != "plan_limit_range_no_order" {
		t.Fatalf("expected no-order prefix limit route, got %q", ev.Plan)
	}
}

func TestPlannerRouting_AvoidsExecutionForWidePrefixLowHitRate(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.PREFIX("full_name", "FN-"),
		qx.EQ("full_name", "FN-10000"),
	).Limit(15)

	leaves, ok := collectAndLeavesForTest(q.Filter)
	if !ok || len(leaves) == 0 {
		t.Fatalf("extractAndLeaves: ok=%v len=%d", ok, len(leaves))
	}
	if db.engine.shouldPreferExecutionNoOrderPrefix(q, leaves) {
		t.Fatalf("expected fallback/planner preference for low-hit wide prefix shape")
	}
}

func TestPlannerRouting_AvoidsExecutionForWidePrefixZeroHitRate(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.PREFIX("full_name", "FN-"),
		qx.EQ("full_name", "FN-999999999"),
	).Limit(15)

	leaves, ok := collectAndLeavesForTest(q.Filter)
	if !ok || len(leaves) == 0 {
		t.Fatalf("extractAndLeaves: ok=%v len=%d", ok, len(leaves))
	}
	if db.engine.shouldPreferExecutionNoOrderPrefix(q, leaves) {
		t.Fatalf("expected fallback/planner preference for zero-hit wide prefix shape")
	}
}

func TestPlannerOROrderMergeFallbackFirst_ComplexOffset(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	qComplex := qx.Query(
		qx.OR(
			qx.AND(
				qx.EQ("active", true),
				qx.IN("country", []string{"NL", "DE", "PL"}),
				qx.HASANY("tags", []string{"go", "ops"}),
				qx.GTE("score", 40.0),
			),
			qx.AND(
				qx.EQ("name", "alice"),
				qx.GTE("age", 20),
				qx.LTE("age", 45),
			),
			qx.AND(
				qx.HASANY("tags", []string{"rust", "db"}),
				qx.GTE("score", 30.0),
			),
		),
	).Sort("score", qx.DESC).Offset(500).Limit(100)

	needComplex, ok := orderWindowForTest(qComplex)
	if !ok {
		t.Fatalf("orderWindow complex: ok=false")
	}
	branchesComplex, alwaysFalse, ok := db.engine.buildORBranchesOrderedWithOffset(qComplex.Filter.Args, "score", needComplex, qComplex.Window.Offset)
	if !ok {
		t.Fatalf("buildORBranches complex: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for complex branches")
	}
	defer branchesComplex.Release()

	preparedComplex, viewComplex, err := prepareTestQuery(db.engine, qComplex)
	if err != nil {
		t.Fatalf("prepareTestQuery complex: %v", err)
	}
	defer preparedComplex.Release()
	decisionComplex := db.engine.currentQueryViewForTests().selectPlanOROrder(&viewComplex, branchesComplex)
	if decisionComplex.selected.kind != plannerOROrderCandidateBranchCollect {
		if !db.engine.shouldUseOROrderKWayRuntimeFallback(qComplex, branchesComplex, needComplex) {
			if decisionComplex.plan != plannerOROrderStream {
				t.Fatalf("expected fallback-first, runtime fallback guard, or stream plan for complex offset ordered OR")
			}
		}
	}

	qLight := qx.Query(
		qx.OR(
			qx.AND(
				qx.EQ("active", true),
				qx.EQ("country", "NL"),
			),
			qx.AND(
				qx.EQ("name", "alice"),
				qx.GTE("age", 25),
			),
			qx.PREFIX("full_name", "FN-1"),
		),
	).Sort("age", qx.ASC).Limit(80)

	needLight, ok := orderWindowForTest(qLight)
	if !ok {
		t.Fatalf("orderWindow light: ok=false")
	}
	branchesLight, alwaysFalse, ok := db.engine.buildORBranchesOrdered(qLight.Filter.Args, "age", needLight)
	if !ok {
		t.Fatalf("buildORBranches light: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for light branches")
	}
	defer branchesLight.Release()

	preparedLight, viewLight, err := prepareTestQuery(db.engine, qLight)
	if err != nil {
		t.Fatalf("prepareTestQuery light: %v", err)
	}
	defer preparedLight.Release()
	decisionLight := db.engine.currentQueryViewForTests().selectPlanOROrder(&viewLight, branchesLight)
	if decisionLight.selected.kind == plannerOROrderCandidateBranchCollect {
		t.Fatalf("unexpected branch-collect preference for light ordered OR")
	}
}

func TestPlannerRouting_PrefersExecutionForRangeOrderLimit(t *testing.T) {
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

	q := qx.Query(
		qx.EQ("active", true),
		qx.GTE("age", 22),
		qx.LT("age", 45),
	).Sort("age", qx.ASC).Limit(120)

	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) == 0 {
		t.Fatalf("expected non-empty ids")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]

	if ev.Plan != "plan_limit_order_basic" {
		t.Fatalf("expected basic limit route, got %q", ev.Plan)
	}
	if ev.EstimatedRows == 0 {
		t.Fatalf("expected estimated rows to be set")
	}
	if ev.EstimatedCost <= 0 {
		t.Fatalf("expected estimated cost to be set")
	}
	if ev.FallbackCost <= 0 {
		t.Fatalf("expected fallback cost to be set")
	}
}

func TestPlannerRouting_OrderLimitWithSecondaryRange_MatchesSeqScan(t *testing.T) {
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

	q := qx.Query(
		qx.EQ("active", true),
		qx.GTE("score", 30.0),
		qx.GTE("age", 22),
	).Sort("score", qx.DESC).Limit(80)

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertQueryIDsEqual(t, q, got, want)

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	if ev.Plan == "" {
		t.Fatal("expected plan name in trace event")
	}
	if ev.RowsReturned < uint64(len(got)) {
		t.Fatalf("rows returned below page size: ev=%d got=%d", ev.RowsReturned, len(got))
	}
	if ev.RowsExamined == 0 {
		t.Fatalf("expected rows examined to be recorded")
	}
	if ev.RowsMatched == 0 {
		t.Fatalf("expected rows matched to be recorded")
	}
}

func TestPlannerRouting_OrderLimitWithSecondaryRangeAndHasAny_MatchesSeqScan(t *testing.T) {
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

	q := qx.Query(
		qx.EQ("active", true),
		qx.HASANY("tags", []string{"go", "db", "ops"}),
		qx.GTE("score", 30.0),
		qx.GTE("age", 22),
	).Sort("score", qx.DESC).Limit(80)

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertQueryIDsEqual(t, q, got, want)

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	if ev.Plan == "" {
		t.Fatal("expected plan name in trace event")
	}
	if ev.RowsReturned < uint64(len(got)) {
		t.Fatalf("rows returned below page size: ev=%d got=%d", ev.RowsReturned, len(got))
	}
	if ev.RowsExamined == 0 {
		t.Fatalf("expected rows examined to be recorded")
	}
}

func TestLimitRoutes_OrderLimitWithNegativeResidual_MatchesSeqScan(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.PREFIX("full_name", "FN-1"),
		qx.EQ("active", true),
		qx.NOTIN("country", []string{"NL", "DE"}),
	).Sort("score", qx.DESC).Limit(100)

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertQueryIDsEqual(t, q, got, want)

	execOut, ok, err := db.engine.tryLimitRoutes(q, nil)
	if err != nil {
		t.Fatalf("tryLimitRoutes: %v", err)
	}
	if !ok {
		t.Fatalf("expected limit route to be applicable")
	}
	assertQueryIDsEqual(t, q, execOut, want)
}

func TestPlannerRouting_OrderedNoOrderWithNegativeIN_MatchesSeqScan(t *testing.T) {
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

	q := qx.Query(
		qx.LT("age", 35),
		qx.NOTIN("country", []string{"NL", "DE"}),
		qx.NOTIN("name", []string{"alice", "bob"}),
		qx.LT("score", 50.0),
	).Limit(120)

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	mu.Lock()
	if len(events) == 0 {
		mu.Unlock()
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	mu.Unlock()

	fullQ := cloneQuery(q)
	fullQ.Window.Offset = 0
	fullQ.Window.Limit = 0
	full, err := expectedKeysUint64(t, db, fullQ)
	if err != nil {
		t.Fatalf("expectedKeysUint64(full): %v", err)
	}
	assertNoOrderPage(t, q, got, full, "ordered_no_order")

	if ev.Plan == "" {
		t.Fatal("expected plan name in trace event")
	}
	if ev.RowsReturned < uint64(len(got)) {
		t.Fatalf("rows returned below page size: ev=%d got=%d", ev.RowsReturned, len(got))
	}
	if ev.RowsExamined == 0 {
		t.Fatalf("expected rows examined to be recorded")
	}
}

func TestPlannerCandidateOrder_MatchesSeqScan(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.EQ("active", true),
		qx.NOTIN("country", []string{"NL", "DE"}),
		qx.IN("name", []string{"alice", "bob", "carol"}),
	).Sort("age", qx.ASC).Limit(120)

	leaves, ok := collectAndLeavesForTest(q.Filter)
	if !ok || len(leaves) == 0 {
		t.Fatalf("collectAndLeaves: ok=%v len=%d", ok, len(leaves))
	}
	if !db.engine.shouldUseCandidateOrder(q.Order[0], leaves) {
		t.Fatalf("expected candidate-order precheck to pass for query shape")
	}

	got, used, err := db.engine.tryCandidateLimitRoute(q, nil)
	if err != nil {
		t.Fatalf("tryCandidateLimitRoute: %v", err)
	}
	if !used {
		t.Fatalf("expected candidate-order route to be used")
	}

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertSameSlice(t, got, want)
}

func TestPlannerCandidateNoOrder_RespectsPageAndPredicateSet(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.EQ("active", true),
		qx.NOTIN("country", []string{"NL", "DE"}),
		qx.IN("name", []string{"alice", "bob", "carol"}),
	).Limit(90)

	got, used, err := db.engine.tryCandidateLimitRoute(q, nil)
	if err != nil {
		t.Fatalf("tryCandidateLimitRoute: %v", err)
	}
	if !used {
		t.Fatalf("expected candidate no-order route to be used")
	}

	fullQ := cloneQuery(q)
	fullQ.Window.Offset = 0
	fullQ.Window.Limit = 0
	full, err := expectedKeysUint64(t, db, fullQ)
	if err != nil {
		t.Fatalf("expectedKeysUint64(full): %v", err)
	}
	assertNoOrderPage(t, q, got, full, "candidate_no_order")
}
