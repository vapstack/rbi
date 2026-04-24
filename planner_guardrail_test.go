package rbi

import (
	"testing"
	"time"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/qir"
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
	name                  string
	open                  func(*testing.T, *traceContractRecorder) *DB[uint64, Rec]
	cases                 []plannerGuardrailCase
	runChosen             func(*queryView[uint64, Rec], *qir.Shape, *queryTrace) ([]uint64, bool, error)
	runAlternative        func(*queryView[uint64, Rec], *qir.Shape, *queryTrace) ([]uint64, bool, error)
	compareOrderScanWidth bool
}

func plannerGuardrailOpenSeededDB(
	t *testing.T,
	recorder *traceContractRecorder,
) *DB[uint64, Rec] {
	t.Helper()

	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSink:        recorder.sink,
		TraceSampleEvery: 1,
	})
	_ = seedData(t, db, 20_000)
	return db
}

func plannerGuardrailOpenOrderedORMergeDB(
	t *testing.T,
	recorder *traceContractRecorder,
) *DB[uint64, Rec] {
	t.Helper()

	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:    -1,
		CalibrationEnabled: true,
		TraceSink:          recorder.sink,
		TraceSampleEvery:   1,
	})
	_ = seedData(t, db, 20_000)
	if err := db.SetCalibrationSnapshot(CalibrationSnapshot{
		UpdatedAt: time.Now(),
		Multipliers: map[string]float64{
			"plan_or_merge_order_merge":  0.45,
			"plan_or_merge_order_stream": 2.00,
		},
	}); err != nil {
		t.Fatalf("SetCalibrationSnapshot: %v", err)
	}
	return db
}

func plannerGuardrailOpenNoOrderORSkewedDB(
	t *testing.T,
	recorder *traceContractRecorder,
) *DB[uint64, Rec] {
	t.Helper()

	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSink:        recorder.sink,
		TraceSampleEvery: 1,
	})
	for id := uint64(1); id <= 4_000; id++ {
		rec := &Rec{
			Name:   "user",
			Active: true,
			Meta:   Meta{Country: "US"},
		}
		switch {
		case id <= 20:
			rec.Name = "alice"
			rec.Active = false
			rec.Meta.Country = "PL"
		case id <= 40:
			rec.Active = false
			rec.Meta.Country = "DE"
		}
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}
	return db
}

func plannerGuardrailRunTryPlanORMergeMode(
	view *queryView[uint64, Rec],
	viewQ *qir.Shape,
	trace *queryTrace,
) ([]uint64, bool, error) {
	return view.tryPlanORMergeMode(viewQ, trace)
}

func plannerGuardrailRunForcedORNoOrderBaseline(
	view *queryView[uint64, Rec],
	viewQ *qir.Shape,
	trace *queryTrace,
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
	out, ok := view.execPlanORNoOrderBaseline(viewQ, branches, trace)
	return out, ok, nil
}

func plannerGuardrailRunForcedOROrderFallback(
	view *queryView[uint64, Rec],
	viewQ *qir.Shape,
	trace *queryTrace,
) ([]uint64, bool, error) {
	window, ok := orderWindow(viewQ)
	if !ok {
		return nil, false, nil
	}
	branches, alwaysFalse, ok := view.buildORBranchesOrdered(
		viewQ.Expr.Operands,
		view.fieldNameByOrder(viewQ.Order),
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

func plannerGuardrailRunTryExecutionPlan(
	view *queryView[uint64, Rec],
	viewQ *qir.Shape,
	trace *queryTrace,
) ([]uint64, bool, error) {
	return view.tryExecutionPlan(viewQ, trace)
}

func plannerGuardrailRunForcedOrderedPlanner(
	view *queryView[uint64, Rec],
	viewQ *qir.Shape,
	trace *queryTrace,
) ([]uint64, bool, error) {
	var leavesBuf [plannerPredicateFastPathMaxLeaves]qir.Expr
	leaves, ok := collectAndLeavesScratch(viewQ.Expr, leavesBuf[:0])
	if !ok {
		return nil, false, nil
	}
	window, _ := orderWindow(viewQ)
	orderField := view.fieldNameByOrder(viewQ.Order)
	predSet, ok := view.buildPredicatesOrderedWithMode(leaves, orderField, false, window, viewQ.Offset, true, true)
	if !ok {
		return nil, false, nil
	}
	defer predSet.Release()
	for i := 0; i < predSet.Len(); i++ {
		if predSet.Get(i).alwaysFalse {
			if trace != nil {
				trace.setPlan(PlanOrdered)
			}
			return nil, true, nil
		}
	}
	if trace != nil {
		trace.setPlan(PlanOrdered)
	}
	out, ok := view.execPlanOrderedBasicReader(viewQ, predSet, trace)
	return out, ok, nil
}

func plannerGuardrailRunTryPlanCandidate(
	view *queryView[uint64, Rec],
	viewQ *qir.Shape,
	trace *queryTrace,
) ([]uint64, bool, error) {
	return view.tryPlanCandidate(viewQ, trace)
}

func plannerGuardrailRunForcedOrderedNoOrderPlanner(
	view *queryView[uint64, Rec],
	viewQ *qir.Shape,
	trace *queryTrace,
) ([]uint64, bool, error) {
	var leavesBuf [plannerPredicateFastPathMaxLeaves]qir.Expr
	leaves, ok := collectAndLeavesScratch(viewQ.Expr, leavesBuf[:0])
	if !ok {
		return nil, false, nil
	}
	predSet, ok := view.buildPredicates(leaves)
	if !ok {
		return nil, false, nil
	}
	defer predSet.Release()
	for i := 0; i < predSet.Len(); i++ {
		if predSet.Get(i).alwaysFalse {
			if trace != nil {
				trace.setPlan(PlanOrderedNoOrder)
			}
			return nil, true, nil
		}
	}
	if trace != nil {
		trace.setPlan(PlanOrderedNoOrder)
	}
	return view.execPlanLeadScanNoOrder(viewQ, predSet, trace), true, nil
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
			if !queryContractNoOrderWindow(tc.q) {
				h.assertEquivalent(chosen, alternative)
			}

			chosenTotals.rowsExamined += chosen.trace.RowsExamined
			chosenTotals.orderScanWidth += chosen.trace.OrderIndexScanWidth
			alternativeTotals.rowsExamined += alternative.trace.RowsExamined
			alternativeTotals.orderScanWidth += alternative.trace.OrderIndexScanWidth
		})
	}

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

func TestPlannerGuardrails_OrderedORStreamFamily(t *testing.T) {
	base := plannerArgminOROrderStreamQuery()
	q40 := cloneQuery(base)
	q40.Window.Limit = 40
	q80 := cloneQuery(base)
	q80.Window.Limit = 80

	runPlannerGuardrailFamily(t, plannerGuardrailFamily{
		name: "OrderedORStreamFamily",
		open: plannerGuardrailOpenSeededDB,
		cases: []plannerGuardrailCase{
			{name: "Limit40", q: q40, wantChosenPlan: PlanORMergeOrderStream},
			{name: "Limit80", q: q80, wantChosenPlan: PlanORMergeOrderStream},
			{name: "Limit120", q: base, wantChosenPlan: PlanORMergeOrderStream},
		},
		runChosen:      plannerGuardrailRunTryPlanORMergeMode,
		runAlternative: plannerGuardrailRunForcedOROrderFallback,
	})
}

func TestPlannerGuardrails_OrderedORMergeFamily(t *testing.T) {
	base := plannerArgminOROrderMergeQuery()
	q60 := cloneQuery(base)
	q60.Window.Limit = 60
	q90 := cloneQuery(base)
	q90.Window.Limit = 90

	runPlannerGuardrailFamily(t, plannerGuardrailFamily{
		name: "OrderedORMergeFamily",
		open: plannerGuardrailOpenOrderedORMergeDB,
		cases: []plannerGuardrailCase{
			{name: "Limit60", q: q60, wantChosenPlan: PlanORMergeOrderMerge},
			{name: "Limit90", q: q90, wantChosenPlan: PlanORMergeOrderMerge},
			{name: "Limit120", q: base, wantChosenPlan: PlanORMergeOrderMerge},
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
		runChosen:             plannerGuardrailRunTryExecutionPlan,
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
