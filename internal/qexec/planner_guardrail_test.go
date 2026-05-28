package qexec

import (
	"testing"
	"unsafe"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
)

type plannerGuardrailTotals struct {
	rowsExamined   uint64
	orderScanWidth uint64
}

type plannerGuardrailCase struct {
	name           string
	q              *qx.QX
	wantChosenPlan PlanName
}

type plannerGuardrailFamily struct {
	name                    string
	open                    func(*testing.T, *traceContractRecorder) *testDB
	cases                   []plannerGuardrailCase
	runChosen               func(*View, *qir.Shape, *Trace) ([]uint64, bool, error)
	runAlternative          func(*View, *qir.Shape, *Trace) ([]uint64, bool, error)
	skipRowsExaminedCompare bool
	compareOrderScanWidth   bool
}

func plannerGuardrailOpenSeededDB(
	t *testing.T,
	recorder *traceContractRecorder,
) *testDB {
	t.Helper()

	db := newTestDB(t, testOptions{
		TraceSink:        recorder.sink,
		TraceSampleEvery: 1,
	})
	_ = db.seedData(t, 20_000)
	return db
}

func plannerGuardrailOpenOrderedORMergeDB(
	t *testing.T,
	recorder *traceContractRecorder,
) *testDB {
	t.Helper()

	db := newTestDB(t, testOptions{
		TraceSink:        recorder.sink,
		TraceSampleEvery: 1,
	})
	_ = db.seedData(t, 20_000)
	return db
}

func plannerGuardrailOpenNoOrderORSkewedDB(
	t *testing.T,
	recorder *traceContractRecorder,
) *testDB {
	t.Helper()

	db := newTestDB(t, testOptions{
		TraceSink:        recorder.sink,
		TraceSampleEvery: 1,
	})
	db.seedGeneratedData(t, 4_000, func(id uint64) testRec {
		rec := testRec{
			Name:   "user",
			Active: true,
			Meta:   Meta{Country: "US"},
		}
		switch {
		case id <= 20:
			rec.Name = "alice"
			rec.Active = false
			rec.Country = "PL"
		case id <= 40:
			rec.Active = false
			rec.Country = "DE"
		}
		return rec
	})
	return db
}

func plannerGuardrailRunTryPlanORMergeMode(
	view *View,
	viewQ *qir.Shape,
	trace *Trace,
) ([]uint64, bool, error) {
	return view.executeOR(viewQ, trace)
}

func plannerGuardrailRunForcedORNoOrderBaseline(
	view *View,
	viewQ *qir.Shape,
	trace *Trace,
) ([]uint64, bool, error) {
	branches, alwaysFalse, ok := view.buildORBranches(viewQ.Expr.Operands)
	if !ok {
		return nil, false, nil
	}
	if alwaysFalse {
		branches.Release()
		return nil, true, nil
	}
	defer branches.Release()
	out, ok := view.execPlanORNoOrderBaselineCore(viewQ, branches, trace)
	return out, ok, nil
}

func plannerGuardrailRunForcedOROrderFallback(
	view *View,
	viewQ *qir.Shape,
	trace *Trace,
) ([]uint64, bool, error) {
	window, ok := orderWindow(viewQ)
	if !ok {
		return nil, false, nil
	}
	branches, alwaysFalse, ok := view.buildORBranchesOrdered(
		viewQ.Expr.Operands,
		view.exec.FieldNameByOrdinal(viewQ.Order.FieldOrdinal),
		window,
		viewQ.Offset,
	)
	if !ok {
		return nil, false, nil
	}
	if alwaysFalse {
		branches.Release()
		return nil, true, nil
	}
	defer branches.Release()
	return view.execPlanOROrderMergeFallback(viewQ, branches, trace)
}

func plannerGuardrailRunLimitRoutes(
	view *View,
	viewQ *qir.Shape,
	trace *Trace,
) ([]uint64, bool, error) {
	return tryLimitRoutesForTest(view, viewQ, trace)
}

func plannerGuardrailRunForcedOrderedPlanner(
	view *View,
	viewQ *qir.Shape,
	trace *Trace,
) ([]uint64, bool, error) {
	var leavesBuf [plannerPredicateFastPathMaxLeaves]qir.Expr
	leaves, ok := qir.CollectAndLeavesScratch(viewQ.Expr, leavesBuf[:0], qir.LeafModeCollect)
	if !ok {
		return nil, false, nil
	}
	window, _ := orderWindow(viewQ)
	orderField := view.exec.FieldNameByOrdinal(viewQ.Order.FieldOrdinal)
	predSet, ok := view.buildPredicatesOrderedWithMode(leaves, orderField, false, window, viewQ.Offset, true, true)
	if !ok {
		return nil, false, nil
	}
	defer predSet.Release()
	for i := 0; i < predSet.Len(); i++ {
		if predSet.owner[i].alwaysFalse {
			if trace != nil {
				trace.SetPlan(PlanOrdered)
			}
			return nil, true, nil
		}
	}
	if trace != nil {
		trace.SetPlan(PlanOrdered)
	}
	out, ok := view.execPlanOrderedBasicReader(viewQ, predSet.owner, trace)
	return out, ok, nil
}

func plannerGuardrailRunTryPlanCandidate(
	view *View,
	viewQ *qir.Shape,
	trace *Trace,
) ([]uint64, bool, error) {
	if viewQ.HasOrder {
		out, ok, plan, err := view.executeOrderedLimit(viewQ, trace)
		if ok && plan == PlanCandidateOrder {
			if trace != nil {
				trace.SetPlan(plan)
			}
			return out, true, err
		}
		return nil, false, err
	}
	out, ok, plan, err := view.executeNoOrderLimit(viewQ, trace)
	if ok && plan == PlanCandidateNoOrder {
		if trace != nil {
			trace.SetPlan(plan)
		}
		return out, true, err
	}
	return nil, false, err
}

func plannerGuardrailRunForcedOrderedNoOrderPlanner(
	view *View,
	viewQ *qir.Shape,
	trace *Trace,
) ([]uint64, bool, error) {
	var leavesBuf [plannerPredicateFastPathMaxLeaves]qir.Expr
	leaves, ok := qir.CollectAndLeavesScratch(viewQ.Expr, leavesBuf[:0], qir.LeafModeCollect)
	if !ok {
		return nil, false, nil
	}
	predSet, ok := view.buildPredicates(leaves)
	if !ok {
		return nil, false, nil
	}
	defer predSet.Release()
	for i := 0; i < predSet.Len(); i++ {
		if predSet.owner[i].alwaysFalse {
			if trace != nil {
				trace.SetPlan(PlanOrderedNoOrder)
			}
			return nil, true, nil
		}
	}
	if trace != nil {
		trace.SetPlan(PlanOrderedNoOrder)
	}
	return view.execPlanLeadScanNoOrder(viewQ, predSet.owner, -1, trace), true, nil
}

func plannerGuardrailRunNoOrderLimitSelector(
	view *View,
	viewQ *qir.Shape,
	trace *Trace,
) ([]uint64, bool, error) {
	out, ok, plan, err := view.executeNoOrderLimit(viewQ, trace)
	if ok && trace != nil {
		trace.SetPlan(plan)
	}
	return out, ok, err
}

func plannerGuardrailRunForcedNoOrderRange(
	view *View,
	viewQ *qir.Shape,
	trace *Trace,
) ([]uint64, bool, error) {
	var leavesBuf [plannerPredicateFastPathMaxLeaves]qir.Expr
	leaves, ok := qir.CollectAndLeavesScratch(viewQ.Expr, leavesBuf[:0], qir.LeafModeCollect)
	if !ok {
		return nil, false, nil
	}
	f, bounds, ok, err := view.extractNoOrderBounds(leaves)
	if err != nil || !ok {
		if err != nil {
			return nil, false, err
		}
		for _, e := range leaves {
			if e.Not || !isBoundOp(e.Op) || e.FieldOrdinal < 0 {
				continue
			}
			f = view.exec.FieldNameByOrdinal(e.FieldOrdinal)
			bounds, ok, err = view.extractBoundsForField(f, leaves)
			break
		}
		if err != nil || !ok {
			return nil, false, err
		}
	}
	if trace != nil {
		trace.SetPlan(PlanLimitRangeNoOrder)
	}
	return view.execSelectedNoOrderBounds(viewQ, f, bounds, leaves, trace)
}

func runPlannerGuardrailFamily(t *testing.T, family plannerGuardrailFamily) {
	t.Helper()

	recorder := &traceContractRecorder{}
	db := family.open(t, recorder)

	var chosenTotals plannerGuardrailTotals
	var alternativeTotals plannerGuardrailTotals

	for i := range family.cases {
		tc := family.cases[i]
		t.Run(tc.name, func(t *testing.T) {
			h := newPlannerShadowHarness(t, db, recorder, tc.q)

			chosen := h.run("guardrail_chosen", family.runChosen)
			if tc.wantChosenPlan != "" && chosen.trace.Plan != string(tc.wantChosenPlan) {
				t.Fatalf("expected chosen plan %q, got %q", tc.wantChosenPlan, chosen.trace.Plan)
			}
			alternative := h.run("guardrail_alternative", family.runAlternative)
			if !testQueryNoOrderPage(tc.q) {
				h.assertEquivalent(chosen, alternative)
			}

			chosenTotals.rowsExamined += chosen.trace.RowsExamined
			chosenTotals.orderScanWidth += chosen.trace.OrderIndexScanWidth
			alternativeTotals.rowsExamined += alternative.trace.RowsExamined
			alternativeTotals.orderScanWidth += alternative.trace.OrderIndexScanWidth
		})
	}

	if !family.skipRowsExaminedCompare {
		if chosenTotals.rowsExamined == 0 || alternativeTotals.rowsExamined == 0 {
			t.Fatalf(
				"%s: expected positive aggregate RowsExamined: chosen=%d alternative=%d",
				family.name,
				chosenTotals.rowsExamined,
				alternativeTotals.rowsExamined,
			)
		}
		if chosenTotals.rowsExamined > alternativeTotals.rowsExamined {
			t.Fatalf(
				"%s: expected chosen family to examine no more rows: chosen=%d alternative=%d",
				family.name,
				chosenTotals.rowsExamined,
				alternativeTotals.rowsExamined,
			)
		}
	}
	if family.compareOrderScanWidth && chosenTotals.orderScanWidth > alternativeTotals.orderScanWidth {
		t.Fatalf(
			"%s: expected chosen family to scan no more order buckets: chosen=%d alternative=%d",
			family.name,
			chosenTotals.orderScanWidth,
			alternativeTotals.orderScanWidth,
		)
	}
}

func TestPlannerGuardrails_NoOrderORAdaptiveFamily(t *testing.T) {
	runPlannerGuardrailFamily(t, plannerGuardrailFamily{
		name: "NoOrderORAdaptiveFamily",
		open: plannerGuardrailOpenNoOrderORSkewedDB,
		cases: []plannerGuardrailCase{
			{
				name: "Limit10",
				q: qx.Query(
					qx.OR(
						qx.EQ("active", true),
						qx.EQ("name", "alice"),
						qx.EQ("country", "NL"),
					),
				).Limit(10),
				wantChosenPlan: PlanORMergeNoOrder,
			},
			{
				name: "Limit20",
				q: qx.Query(
					qx.OR(
						qx.EQ("active", true),
						qx.EQ("name", "alice"),
						qx.EQ("country", "NL"),
					),
				).Limit(20),
				wantChosenPlan: PlanORMergeNoOrder,
			},
			{
				name: "Limit40",
				q: qx.Query(
					qx.OR(
						qx.EQ("active", true),
						qx.EQ("name", "alice"),
						qx.EQ("country", "NL"),
					),
				).Limit(40),
				wantChosenPlan: PlanORMergeNoOrder,
			},
		},
		runChosen:      plannerGuardrailRunTryPlanORMergeMode,
		runAlternative: plannerGuardrailRunForcedORNoOrderBaseline,
	})
}

func TestPlannerGuardrails_OrderedORMergeNaturalFamily(t *testing.T) {
	base := plannerArgminOROrderStreamQuery()
	q40 := cloneQuery(base)
	q40.Window.Limit = 40
	q80 := cloneQuery(base)
	q80.Window.Limit = 80

	runPlannerGuardrailFamily(t, plannerGuardrailFamily{
		name: "OrderedORMergeNaturalFamily",
		open: plannerGuardrailOpenSeededDB,
		cases: []plannerGuardrailCase{
			{name: "Limit40", q: q40, wantChosenPlan: PlanORMergeOrderMerge},
			{name: "Limit80", q: q80, wantChosenPlan: PlanORMergeOrderMerge},
			{name: "Limit120", q: base, wantChosenPlan: PlanORMergeOrderMerge},
		},
		runChosen:      plannerGuardrailRunTryPlanORMergeMode,
		runAlternative: plannerGuardrailRunForcedOROrderFallback,
	})
}

func TestPlannerGuardrails_OrderedORStreamNaturalFamily(t *testing.T) {
	base := plannerArgminOROrderMergeQuery()
	q60 := cloneQuery(base)
	q60.Window.Limit = 60
	q90 := cloneQuery(base)
	q90.Window.Limit = 90

	runPlannerGuardrailFamily(t, plannerGuardrailFamily{
		name: "OrderedORStreamNaturalFamily",
		open: plannerGuardrailOpenOrderedORMergeDB,
		cases: []plannerGuardrailCase{
			{name: "Limit60", q: q60, wantChosenPlan: PlanORMergeOrderStream},
			{name: "Limit90", q: q90, wantChosenPlan: PlanORMergeOrderStream},
			{name: "Limit120", q: base, wantChosenPlan: PlanORMergeOrderStream},
		},
		runChosen:      plannerGuardrailRunTryPlanORMergeMode,
		runAlternative: plannerGuardrailRunForcedOROrderFallback,
	})
}

func TestPlannerGuardrails_OrderedLimitExecutionFamily(t *testing.T) {
	base := qx.Query(
		qx.EQ("active", true),
		qx.GTE("age", 22),
		qx.LT("age", 45),
	).Sort("age", qx.ASC).Limit(120)
	q60 := cloneQuery(base)
	q60.Window.Limit = 60
	q30 := cloneQuery(base)
	q30.Window.Limit = 30

	runPlannerGuardrailFamily(t, plannerGuardrailFamily{
		name: "OrderedLimitExecutionFamily",
		open: plannerGuardrailOpenSeededDB,
		cases: []plannerGuardrailCase{
			{name: "Limit30", q: q30, wantChosenPlan: PlanLimitOrderBasic},
			{name: "Limit60", q: q60, wantChosenPlan: PlanLimitOrderBasic},
			{name: "Limit120", q: base, wantChosenPlan: PlanLimitOrderBasic},
		},
		runChosen:             plannerGuardrailRunLimitRoutes,
		runAlternative:        plannerGuardrailRunForcedOrderedPlanner,
		compareOrderScanWidth: true,
	})
}

func TestPlannerGuardrails_CandidateOrderFamily(t *testing.T) {
	base := qx.Query(
		qx.EQ("active", true),
		qx.NOTIN("country", []string{"NL", "DE"}),
		qx.IN("name", []string{"alice", "bob", "carol"}),
	).Sort("age", qx.ASC).Limit(120)
	q80 := cloneQuery(base)
	q80.Window.Limit = 80
	q40 := cloneQuery(base)
	q40.Window.Limit = 40

	runPlannerGuardrailFamily(t, plannerGuardrailFamily{
		name: "CandidateOrderFamily",
		open: plannerGuardrailOpenSeededDB,
		cases: []plannerGuardrailCase{
			{name: "Limit40", q: q40, wantChosenPlan: PlanCandidateOrder},
			{name: "Limit80", q: q80, wantChosenPlan: PlanCandidateOrder},
			{name: "Limit120", q: base, wantChosenPlan: PlanCandidateOrder},
		},
		runChosen:             plannerGuardrailRunTryPlanCandidate,
		runAlternative:        plannerGuardrailRunForcedOrderedPlanner,
		compareOrderScanWidth: true,
	})
}

func TestPlannerGuardrails_CandidateNoOrderFamily(t *testing.T) {
	base := qx.Query(
		qx.EQ("active", true),
		qx.NOTIN("country", []string{"NL", "DE"}),
		qx.IN("name", []string{"alice", "bob", "carol"}),
	).Limit(90)
	q60 := cloneQuery(base)
	q60.Window.Limit = 60
	q30 := cloneQuery(base)
	q30.Window.Limit = 30

	runPlannerGuardrailFamily(t, plannerGuardrailFamily{
		name: "CandidateNoOrderFamily",
		open: plannerGuardrailOpenSeededDB,
		cases: []plannerGuardrailCase{
			{name: "Limit30", q: q30, wantChosenPlan: PlanCandidateNoOrder},
			{name: "Limit60", q: q60, wantChosenPlan: PlanCandidateNoOrder},
			{name: "Limit90", q: base, wantChosenPlan: PlanCandidateNoOrder},
		},
		runChosen:      plannerGuardrailRunTryPlanCandidate,
		runAlternative: plannerGuardrailRunForcedOrderedNoOrderPlanner,
	})
}

func TestPlannerGuardrails_NoOrderBroadRangeLeadNamedFamily(t *testing.T) {
	broadSelective := qx.Query(
		qx.GTE("age", 18),
		qx.LT("age", 68),
		qx.EQ("country", "NL"),
		qx.EQ("name", "alice"),
		qx.HASANY("tags", []string{"go", "db"}),
	).Limit(64)
	broadSelectiveNone := qx.Query(
		qx.GTE("age", 18),
		qx.LT("age", 68),
		qx.EQ("country", "NL"),
		qx.EQ("name", "mallory"),
		qx.HASANY("tags", []string{"go", "db"}),
	).Limit(64)
	missingEquality := qx.Query(
		qx.GTE("age", 18),
		qx.LT("age", 68),
		qx.EQ("country", "ZZ"),
	).Limit(64)
	weakControl := qx.Query(
		qx.GTE("age", 18),
		qx.LT("age", 68),
		qx.EQ("active", true),
		qx.HASANY("tags", []string{"go", "db"}),
		qx.PREFIX("full_name", "FN-"),
	).Limit(64)
	narrowControl := qx.Query(
		qx.GTE("age", 18),
		qx.LT("age", 22),
		qx.EQ("active", true),
		qx.HASANY("tags", []string{"go", "db"}),
		qx.PREFIX("full_name", "FN-"),
	).Limit(32)

	runPlannerGuardrailFamily(t, plannerGuardrailFamily{
		name: "NoOrderBroadRangeLeadNamedFamily",
		open: plannerGuardrailOpenSeededDB,
		cases: []plannerGuardrailCase{
			{name: "BroadRangeLead_ThreeSelectiveResiduals", q: broadSelective, wantChosenPlan: PlanCandidateNoOrder},
			{name: "BroadRangeLead_ThreeSelectiveResiduals_None", q: broadSelectiveNone},
			{name: "MissingEqualityResidual", q: missingEquality},
			{name: "BroadRangeLead_ThreeWeakResiduals_Control", q: weakControl, wantChosenPlan: PlanLimitRangeNoOrder},
			{name: "NarrowRangeLead_ThreeResiduals_Control", q: narrowControl, wantChosenPlan: PlanLimitRangeNoOrder},
		},
		runChosen:               plannerGuardrailRunNoOrderLimitSelector,
		runAlternative:          plannerGuardrailRunForcedNoOrderRange,
		skipRowsExaminedCompare: true,
	})
}

func plannerGuardrailOrderedLimitBaseCandidate(
	t *testing.T,
	db *testDB,
	q *qx.QX,
	markSecondHit bool,
	warm bool,
) plannerOrderedLimitCandidate {
	t.Helper()

	preparedQ, viewQ, err := db.prepareQuery(q)
	if err != nil {
		t.Fatalf("prepareQuery: %v", err)
	}
	defer preparedQ.Release()

	view := db.view()
	var leavesBuf [limitQueryFastPathMaxLeaves]qir.Expr
	leaves, ok := qir.CollectAndLeavesScratch(viewQ.Expr, leavesBuf[:0], qir.LeafModeCollect)
	if !ok || len(leaves) == 0 {
		t.Fatalf("CollectAndLeavesScratch: ok=%v len=%d", ok, len(leaves))
	}

	orderField := view.exec.FieldNameByOrdinal(viewQ.Order.FieldOrdinal)
	bounds, ok, err := view.extractBoundsForField(orderField, leaves)
	if err != nil {
		t.Fatalf("extractBoundsForField: %v", err)
	}
	if !ok {
		t.Fatalf("expected order bounds for %q", orderField)
	}

	var baseOpsBuf [limitQueryFastPathMaxLeaves]qir.Expr
	baseOps := baseOpsBuf[:0]
	for _, e := range leaves {
		if isBoundOp(e.Op) && !e.Not && e.FieldOrdinal == viewQ.Order.FieldOrdinal {
			continue
		}
		baseOps = append(baseOps, e)
	}
	if len(baseOps) == 0 {
		t.Fatalf("expected ordered LIMIT base ops")
	}

	if markSecondHit {
		universe := view.snap.Universe.Cardinality()
		for _, op := range baseOps {
			stats, ok := view.orderBasicRawBaseOpStats(op, universe)
			if ok && !stats.cacheKey.IsZero() {
				view.snap.ShouldPromoteRuntimeMaterializedPredKey(stats.cacheKey)
			}
		}
	}
	if warm {
		view.promoteOrderBasicLimitMaterializedBaseOps(orderField, baseOps, 5_000, uint64(viewQ.Limit))
	}

	ov := view.fieldIndexViewFromSlotsForOrder(view.snap.Index, viewQ.Order)
	if ov.RangeForBounds(bounds).Empty() {
		t.Fatalf("expected non-empty order range")
	}
	orderCandidate := plannerOrderedLimitCandidate{
		kind:         plannerOrderedLimitCandidateOrderScan,
		cost:         1_000_000,
		expectedRows: 5_000,
		buckets:      uint64(ov.KeyCount()),
		checks:       uint64(len(leaves)),
	}
	candidate, ok, noMatch, err := view.orderedLimitBaseOpsCandidate(baseOps, int(viewQ.Limit), true, viewQ.Offset > 0, orderCandidate)
	if err != nil {
		t.Fatalf("orderedLimitBaseOpsCandidate: %v", err)
	}
	if noMatch {
		t.Fatalf("orderedLimitBaseOpsCandidate: noMatch")
	}
	if !ok {
		t.Fatalf("orderedLimitBaseOpsCandidate: ok=false")
	}
	return candidate
}

func TestPlannerGuardrails_OrderedLimitCacheStates(t *testing.T) {
	ageRangeQ := qx.Query(
		qx.GTE("age", 25),
		qx.LTE("age", 40),
		qx.GT("score", 0.5),
	).Sort("score", qx.DESC).Limit(10)

	twoRangeQ := qx.Query(
		qx.GTE("age", 25),
		qx.LTE("age", 40),
		qx.GTE("score", 10.0),
		qx.LTE("score", 20.0),
		qx.PREFIX("full_name", "FN-"),
	).Sort("full_name", qx.ASC).Limit(10)

	secondHitQ := qx.Query(
		qx.PREFIX("name", "a"),
		qx.PREFIX("full_name", "FN-"),
	).Sort("full_name", qx.ASC).Limit(10)

	t.Run("Warm", func(t *testing.T) {
		db := newTestDB(t, testOptions{MatPredCacheMaxEntries: 16})
		_ = db.seedData(t, 20_000)
		candidate := plannerGuardrailOrderedLimitBaseCandidate(t, db, ageRangeQ, false, true)
		if candidate.kind != plannerOrderedLimitCandidateWarmBaseCore {
			t.Fatalf("kind=%v want=%v", candidate.kind, plannerOrderedLimitCandidateWarmBaseCore)
		}
		if candidate.cacheState != plannerMaterializedCacheWarmHit {
			t.Fatalf("cacheState=%v want=%v", candidate.cacheState, plannerMaterializedCacheWarmHit)
		}
	})

	t.Run("ColdRetained", func(t *testing.T) {
		db := newTestDB(t, testOptions{MatPredCacheMaxEntries: 16})
		_ = db.seedData(t, 20_000)
		candidate := plannerGuardrailOrderedLimitBaseCandidate(t, db, twoRangeQ, false, false)
		if candidate.kind != plannerOrderedLimitCandidateColdRetainedBaseCore {
			t.Fatalf("kind=%v want=%v", candidate.kind, plannerOrderedLimitCandidateColdRetainedBaseCore)
		}
		if candidate.cacheState != plannerMaterializedCacheColdRegularAdmissible {
			t.Fatalf("cacheState=%v want=%v", candidate.cacheState, plannerMaterializedCacheColdRegularAdmissible)
		}
	})

	t.Run("SecondHitRequired", func(t *testing.T) {
		db := newTestDB(t, testOptions{MatPredCacheMaxEntries: 16})
		_ = db.seedData(t, 20_000)
		candidate := plannerGuardrailOrderedLimitBaseCandidate(t, db, secondHitQ, false, false)
		if candidate.kind != plannerOrderedLimitCandidateColdUnretainedBaseCore {
			t.Fatalf("kind=%v want=%v", candidate.kind, plannerOrderedLimitCandidateColdUnretainedBaseCore)
		}
		if candidate.cacheState != plannerMaterializedCacheColdSecondHitRequired {
			t.Fatalf("cacheState=%v want=%v", candidate.cacheState, plannerMaterializedCacheColdSecondHitRequired)
		}
	})

	t.Run("DirtyObserved", func(t *testing.T) {
		db := newTestDB(t, testOptions{MatPredCacheMaxEntries: 16})
		gen := func(id uint64) testRec {
			name := "bob"
			if id&1 == 0 {
				name = "alice"
			}
			return testRec{
				Name:     name,
				Email:    "user@example.com",
				Age:      18 + int(id%50),
				Score:    float64(id % 100),
				Active:   true,
				FullName: "FN-",
			}
		}
		db.seedGeneratedData(t, 20_000, gen)

		preparedQ, viewQ, err := db.prepareQuery(secondHitQ)
		if err != nil {
			t.Fatalf("prepareQuery: %v", err)
		}
		defer preparedQ.Release()

		view := db.view()
		var leavesBuf [limitQueryFastPathMaxLeaves]qir.Expr
		leaves, ok := qir.CollectAndLeavesScratch(viewQ.Expr, leavesBuf[:0], qir.LeafModeCollect)
		if !ok || len(leaves) == 0 {
			t.Fatalf("CollectAndLeavesScratch: ok=%v len=%d", ok, len(leaves))
		}

		var baseOpsBuf [limitQueryFastPathMaxLeaves]qir.Expr
		baseOps := baseOpsBuf[:0]
		for _, e := range leaves {
			if isBoundOp(e.Op) && !e.Not && e.FieldOrdinal == viewQ.Order.FieldOrdinal {
				continue
			}
			baseOps = append(baseOps, e)
		}

		var dirtyStats scalarMaterializationStats
		var dirtyOp qir.Expr
		dirtyBuildWork := uint64(0)
		universe := view.snap.Universe.Cardinality()
		for _, op := range baseOps {
			stats, ok := view.orderBasicRawBaseOpStats(op, universe)
			if !ok || stats.cacheKey.IsZero() {
				continue
			}
			buildWork := rangeProbeMaterializeWork(stats.buildBuckets, stats.buildEst)
			if buildWork == 0 {
				continue
			}
			if !view.snap.ShouldPromoteObservedMaterializedPredKey(stats.cacheKey, buildWork, buildWork) {
				t.Fatalf("expected clean observed work to cross build threshold")
			}
			dirtyStats = stats
			dirtyOp = op
			dirtyBuildWork = buildWork
			break
		}
		if dirtyStats.cacheKey.IsZero() {
			t.Fatal("expected materializable base op")
		}

		oldRec := gen(1)
		newRec := oldRec
		newRec.Name = "zach"
		db.seq++
		db.snap = snapshot.BuildPrepared(db.seq, db.snap, db.rt, db.cfg, nil, db.rt.Patch.Fields, []snapshot.BatchEntry{{
			ID:        1,
			Old:       unsafe.Pointer(&oldRec),
			New:       unsafe.Pointer(&newRec),
			Patch:     []schema.PatchItem{{Name: "name", Value: newRec.Name}},
			PatchOnly: true,
		}})
		if got := db.snap.DirtyObservedMaterializedPredWork(dirtyStats.cacheKey); got < dirtyBuildWork {
			t.Fatalf("dirty observed work=%d want>=%d", got, dirtyBuildWork)
		}

		view = db.view()
		stats, ok := view.orderedLimitBaseCoreStats(
			orderBasicBaseCore{kind: orderBasicBaseCoreRawExpr, expr: dirtyOp},
			view.snap.Universe.Cardinality(),
			int(viewQ.Limit),
			true,
		)
		if !ok {
			t.Fatalf("orderedLimitBaseCoreStats: ok=false")
		}
		if stats.routeWork != 0 {
			t.Fatalf("dirty observed work leaked into routeWork=%d", stats.routeWork)
		}

		candidate := plannerGuardrailOrderedLimitBaseCandidate(t, db, secondHitQ, false, false)
		if candidate.kind != plannerOrderedLimitCandidateColdUnretainedBaseCore {
			t.Fatalf("kind=%v want=%v", candidate.kind, plannerOrderedLimitCandidateColdUnretainedBaseCore)
		}
		if candidate.cacheState != plannerMaterializedCacheColdSecondHitRequired {
			t.Fatalf("cacheState=%v want=%v", candidate.cacheState, plannerMaterializedCacheColdSecondHitRequired)
		}
	})

	t.Run("Disabled", func(t *testing.T) {
		db := newTestDB(t, testOptions{MatPredCacheMaxEntries: -1})
		_ = db.seedData(t, 20_000)
		candidate := plannerGuardrailOrderedLimitBaseCandidate(t, db, ageRangeQ, false, false)
		if candidate.kind != plannerOrderedLimitCandidateColdUnretainedBaseCore {
			t.Fatalf("kind=%v want=%v", candidate.kind, plannerOrderedLimitCandidateColdUnretainedBaseCore)
		}
		if candidate.cacheState != plannerMaterializedCacheDisabled {
			t.Fatalf("cacheState=%v want=%v", candidate.cacheState, plannerMaterializedCacheDisabled)
		}
	})

	t.Run("Tiny", func(t *testing.T) {
		db := newTestDB(t, testOptions{MatPredCacheMaxEntries: 1})
		_ = db.seedData(t, 20_000)
		candidate := plannerGuardrailOrderedLimitBaseCandidate(t, db, twoRangeQ, false, false)
		if candidate.kind != plannerOrderedLimitCandidateColdUnretainedBaseCore {
			t.Fatalf("kind=%v want=%v", candidate.kind, plannerOrderedLimitCandidateColdUnretainedBaseCore)
		}
		if candidate.cacheState != plannerMaterializedCacheColdUnretainedByPolicy {
			t.Fatalf("cacheState=%v want=%v", candidate.cacheState, plannerMaterializedCacheColdUnretainedByPolicy)
		}
	})

	t.Run("TinyPartialWarm", func(t *testing.T) {
		db := newTestDB(t, testOptions{MatPredCacheMaxEntries: 1})
		_ = db.seedData(t, 20_000)
		candidate := plannerGuardrailOrderedLimitBaseCandidate(t, db, twoRangeQ, false, true)
		if candidate.kind != plannerOrderedLimitCandidateColdUnretainedBaseCore {
			t.Fatalf("kind=%v want=%v", candidate.kind, plannerOrderedLimitCandidateColdUnretainedBaseCore)
		}
		if candidate.cacheState != plannerMaterializedCacheColdUnretainedByPolicy {
			t.Fatalf("cacheState=%v want=%v", candidate.cacheState, plannerMaterializedCacheColdUnretainedByPolicy)
		}
	})
}

func TestPlannerGuardrails_OrderedLimitBroadResidualNoOffsetUsesOrderScan(t *testing.T) {
	var events []TraceEvent
	db := newTestDB(t, testOptions{
		TraceSink: func(ev TraceEvent) {
			events = append(events, ev)
		},
		TraceSampleEvery: 1,
	})
	_ = db.seedData(t, 20_000)

	q := qx.Query(qx.GTE("age", 20)).Sort("score", qx.DESC).Limit(100)
	prepared, viewQ, err := db.prepareQuery(q)
	if err != nil {
		t.Fatalf("prepareQuery: %v", err)
	}
	defer prepared.Release()

	ids, err := db.view().Query(&viewQ, true)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(ids) != 100 {
		t.Fatalf("len(ids)=%d want=100", len(ids))
	}
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	route := events[len(events)-1].OrderedLimitRoute
	if route.Selected != plannerOrderedLimitCandidateOrderScan.String() {
		t.Fatalf("selected=%q want=%q route=%+v", route.Selected, plannerOrderedLimitCandidateOrderScan.String(), route)
	}
}

func TestPlannerGuardrails_OrderedLimitBoundedExactResidualsUsesOrderScan(t *testing.T) {
	var events []TraceEvent
	db := newTestDB(t, testOptions{
		TraceSink: func(ev TraceEvent) {
			events = append(events, ev)
		},
		TraceSampleEvery: 1,
	})
	countries := [...]string{"US", "DE", "NL", "PL", "BR", "JP", "IN", "CA"}
	db.seedGeneratedData(t, 20_000, func(id uint64) testRec {
		tags := []string{"other"}
		if id%2048 == 0 {
			tags = []string{"tag_tiny", "go", "db"}
		}
		return testRec{
			Meta: Meta{Country: countries[id&7]},
			Age:  int(id),
			Tags: tags,
		}
	})

	q := qx.Query(
		qx.GTE("age", 8192),
		qx.LT("age", 8256),
		qx.EQ("country", "US"),
		qx.HASANY("tags", []string{"tag_tiny", "missing_tag"}),
	).Sort("age", qx.ASC).Limit(128)
	prepared, viewQ, err := db.prepareQuery(q)
	if err != nil {
		t.Fatalf("prepareQuery: %v", err)
	}
	defer prepared.Release()

	ids, err := db.view().Query(&viewQ, true)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(ids) != 1 || ids[0] != 8192 {
		t.Fatalf("ids=%v want [8192]", ids)
	}
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	route := events[len(events)-1].OrderedLimitRoute
	if route.Selected != plannerOrderedLimitCandidateOrderScan.String() {
		t.Fatalf("selected=%q want=%q route=%+v", route.Selected, plannerOrderedLimitCandidateOrderScan.String(), route)
	}
}

func TestPlannerGuardrails_OrderedLimitMergedResidualRangeKeepsOrderScanCandidate(t *testing.T) {
	db := newTestDB(t, testOptions{
		NumericRangeBucketSize:         256,
		NumericRangeBucketMinFieldKeys: 1024,
		NumericRangeBucketMinSpanKeys:  256,
	})
	db.seedGeneratedData(t, 20_000, func(id uint64) testRec {
		return testRec{
			Age:   int(id),
			Score: float64(id),
		}
	})

	q := qx.Query(
		qx.GTE("score", 1.0),
		qx.LT("score", 701.0),
		qx.GTE("age", 10_000),
		qx.LT("age", 12_000),
	).Sort("score", qx.ASC).Limit(128)
	prepared, viewQ, err := db.prepareQuery(q)
	if err != nil {
		t.Fatalf("prepareQuery: %v", err)
	}
	defer prepared.Release()

	view := db.view()
	facts := orderedLimitFactsPool.Get()
	ok, err := view.collectOrderedLimitFacts(&viewQ, facts)
	if err != nil {
		facts.Release()
		t.Fatalf("collectOrderedLimitFacts: %v", err)
	}
	if !ok {
		facts.Release()
		t.Fatalf("collectOrderedLimitFacts ok=false")
	}
	orderedDecision := view.decideOrderedByCost(&viewQ, facts.ops)
	orderScan := view.orderedLimitOrderScanCost(&viewQ, facts.ops, facts.ov, facts.br, facts.needWindow, orderedDecision)
	_, ok = view.orderedLimitBoundsScanCandidate(&viewQ, facts, orderScan)
	facts.Release()
	if !ok {
		t.Fatalf("merged residual range rejected bounded order scan: orderScan=%+v", orderScan)
	}
}

func TestPlannerGuardrails_OROrderTinyCacheRouteSet(t *testing.T) {
	db := newTestDB(t, testOptions{MatPredCacheMaxEntries: 1})
	_ = db.seedData(t, 20_000)

	preparedQ, viewQ, err := db.prepareQuery(qx.Query(
		qx.OR(
			qx.AND(qx.GTE("age", 25), qx.EQ("country", "NL")),
			qx.AND(qx.PREFIX("full_name", "FN-0"), qx.EQ("active", true)),
		),
	).Sort("score", qx.DESC).Limit(128))
	if err != nil {
		t.Fatalf("prepareQuery: %v", err)
	}
	defer preparedQ.Release()

	view := db.view()
	var facts plannerORFacts
	if !view.collectORFacts(&viewQ, &facts) {
		t.Fatalf("collectORFacts: ok=false")
	}
	defer facts.Release()

	decision := view.selectOR(&viewQ, &facts)
	if decision.kind != plannerORDecisionOrder {
		t.Fatalf("decision.kind=%v want=%v", decision.kind, plannerORDecisionOrder)
	}
	if decision.order.selected.cacheState != plannerMaterializedCacheColdUnretainedByPolicy {
		t.Fatalf("cacheState=%v want=%v route=%+v", decision.order.selected.cacheState, plannerMaterializedCacheColdUnretainedByPolicy, decision.order.traceRoute())
	}
}
