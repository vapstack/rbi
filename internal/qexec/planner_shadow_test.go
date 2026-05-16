package qexec

import (
	"slices"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/qir"
)

type plannerShadowResult struct {
	label string
	keys  []uint64
	trace TraceEvent
}

type plannerShadowHarness struct {
	t        *testing.T
	db       *testDB
	q        *qx.QX
	recorder *traceContractRecorder
	prepared *qir.Query
	view     *View
	viewQ    qir.Shape
}

func newPlannerShadowHarness(
	t *testing.T,
	db *testDB,
	recorder *traceContractRecorder,
	q *qx.QX,
) *plannerShadowHarness {
	t.Helper()

	prepared, viewQ, err := db.prepareQuery(q)
	if err != nil {
		t.Fatalf("prepareTestQuery(%+v): %v", q, err)
	}
	view := db.view()
	h := &plannerShadowHarness{
		t:        t,
		db:       db,
		q:        q,
		recorder: recorder,
		prepared: prepared,
		view:     view,
		viewQ:    viewQ,
	}
	t.Cleanup(func() {
		prepared.Release()
	})
	return h
}

func (h *plannerShadowHarness) run(
	label string,
	exec func(*View, *qir.Shape, *Trace) ([]uint64, bool, error),
) plannerShadowResult {
	h.t.Helper()

	h.db.clearCurrentSnapshotCaches()

	mark := h.recorder.mark()
	trace := h.db.beginTrace(h.viewQ)
	if trace == nil {
		h.t.Fatalf("%s: expected trace to be enabled", label)
	}

	out, used, err := exec(h.view, &h.viewQ, trace)
	if trace.Event().Plan == "" {
		trace.SetPlan(PlanName(label))
	}
	trace.Finish(uint64(len(out)), err)

	if err != nil {
		h.t.Fatalf("%s: %v", label, err)
	}
	if !used {
		h.t.Fatalf("%s: expected route to be used", label)
	}

	ev := h.recorder.lastSince(h.t, mark)
	traceContractAssertQueryResultTrace(h.t, ev, uint64(len(out)))
	h.db.assertKeysMatchReference(h.t, label, h.q, out)

	return plannerShadowResult{
		label: label,
		keys:  append([]uint64(nil), out...),
		trace: ev,
	}
}

func (h *plannerShadowHarness) assertEquivalent(a, b plannerShadowResult) {
	h.t.Helper()

	if testQueryNoOrderWindow(h.q) {
		return
	}
	if !slices.Equal(a.keys, b.keys) {
		h.t.Fatalf(
			"%s/%s exact mismatch:\nq=%+v\n%s=%v\n%s=%v",
			a.label, b.label, h.q, a.label, a.keys, b.label, b.keys,
		)
	}
}

func plannerShadowAssertNoMoreRowsExamined(
	t *testing.T,
	chosen plannerShadowResult,
	alternative plannerShadowResult,
) {
	t.Helper()

	if chosen.trace.RowsExamined == 0 || alternative.trace.RowsExamined == 0 {
		t.Fatalf(
			"expected positive RowsExamined for shadow comparison:\nchosen=%+v\nalternative=%+v",
			chosen.trace,
			alternative.trace,
		)
	}
	if chosen.trace.RowsExamined > alternative.trace.RowsExamined {
		t.Fatalf(
			"expected chosen route to examine no more rows:\nchosen=%s examined=%d plan=%q\nalternative=%s examined=%d plan=%q",
			chosen.label,
			chosen.trace.RowsExamined,
			chosen.trace.Plan,
			alternative.label,
			alternative.trace.RowsExamined,
			alternative.trace.Plan,
		)
	}
}

func TestPlannerShadow_NoOrderOR_ChosenVsBaseline(t *testing.T) {
	recorder := &traceContractRecorder{}
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
			rec.Meta.Country = "PL"
		case id <= 40:
			rec.Active = false
			rec.Meta.Country = "DE"
		}
		return rec
	})

	q := qx.Query(
		qx.OR(
			qx.EQ("active", true),
			qx.EQ("name", "alice"),
			qx.EQ("country", "NL"),
		),
	).Limit(20)

	h := newPlannerShadowHarness(t, db, recorder, q)

	chosen := h.run("shadow_or_no_order_chosen", func(view *View, viewQ *qir.Shape, trace *Trace) ([]uint64, bool, error) {
		return view.tryPlanORMergeMode(viewQ, trace)
	})
	if chosen.trace.Plan != string(PlanORMergeNoOrder) {
		t.Fatalf("expected chosen no-order OR plan %q, got %q", PlanORMergeNoOrder, chosen.trace.Plan)
	}

	baseline := h.run("shadow_or_no_order_baseline", func(view *View, viewQ *qir.Shape, trace *Trace) ([]uint64, bool, error) {
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
	})

	plannerShadowAssertNoMoreRowsExamined(t, chosen, baseline)
}

func TestPlannerShadow_OrderedOR_StreamVsFallback(t *testing.T) {
	recorder := &traceContractRecorder{}
	db := newTestDB(t, testOptions{
		CalibrationEnabled: true,
		TraceSink:          recorder.sink,
		TraceSampleEvery:   1,
	})
	_ = db.seedData(t, 20_000)
	db.setCalibrationSnapshot(t, testCalibrationSnapshot(map[string]float64{
		"plan_or_merge_order_merge":  2.00,
		"plan_or_merge_order_stream": 0.40,
	}))

	q := plannerArgminOROrderStreamQuery()
	h := newPlannerShadowHarness(t, db, recorder, q)

	chosen := h.run("shadow_or_order_stream_chosen", func(view *View, viewQ *qir.Shape, trace *Trace) ([]uint64, bool, error) {
		return view.tryPlanORMergeMode(viewQ, trace)
	})
	if chosen.trace.Plan != string(PlanORMergeOrderStream) {
		t.Fatalf("expected chosen ordered OR plan %q, got %q", PlanORMergeOrderStream, chosen.trace.Plan)
	}

	fallback := h.run("shadow_or_order_fallback", func(view *View, viewQ *qir.Shape, trace *Trace) ([]uint64, bool, error) {
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
	})

	h.assertEquivalent(chosen, fallback)
	plannerShadowAssertNoMoreRowsExamined(t, chosen, fallback)
}

func TestPlannerShadow_OrderedORMergeVsFallback(t *testing.T) {
	recorder := &traceContractRecorder{}
	db := newTestDB(t, testOptions{
		CalibrationEnabled: true,
		TraceSink:          recorder.sink,
		TraceSampleEvery:   1,
	})
	_ = db.seedData(t, 20_000)

	db.setCalibrationSnapshot(t, testCalibrationSnapshot(map[string]float64{
		"plan_or_merge_order_merge":  0.45,
		"plan_or_merge_order_stream": 2.00,
	}))

	q := plannerArgminOROrderMergeQuery()
	h := newPlannerShadowHarness(t, db, recorder, q)

	chosen := h.run("shadow_or_order_merge_chosen", func(view *View, viewQ *qir.Shape, trace *Trace) ([]uint64, bool, error) {
		return view.tryPlanORMergeMode(viewQ, trace)
	})
	if chosen.trace.Plan != string(PlanORMergeOrderMerge) {
		t.Fatalf("expected chosen ordered OR plan %q, got %q", PlanORMergeOrderMerge, chosen.trace.Plan)
	}

	fallback := h.run("shadow_or_order_merge_fallback", func(view *View, viewQ *qir.Shape, trace *Trace) ([]uint64, bool, error) {
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
	})

	h.assertEquivalent(chosen, fallback)
	plannerShadowAssertNoMoreRowsExamined(t, chosen, fallback)
}

func TestPlannerShadow_OrderedLimitExecutionVsPlanner(t *testing.T) {
	recorder := &traceContractRecorder{}
	db := newTestDB(t, testOptions{
		TraceSink:        recorder.sink,
		TraceSampleEvery: 1,
	})
	_ = db.seedData(t, 20_000)

	q := qx.Query(
		qx.EQ("active", true),
		qx.GTE("age", 22),
		qx.LT("age", 45),
	).Sort("age", qx.ASC).Limit(120)
	prepared, viewQ, err := db.prepareQuery(q)
	if err != nil {
		t.Fatalf("prepareQuery: %v", err)
	}
	if !db.view().shouldPreferExecutionPlan(&viewQ, nil) {
		prepared.Release()
		t.Fatalf("expected ordered-limit shape to prefer execution plan")
	}
	prepared.Release()

	h := newPlannerShadowHarness(t, db, recorder, q)

	chosen := h.run("shadow_order_limit_execution", func(view *View, viewQ *qir.Shape, trace *Trace) ([]uint64, bool, error) {
		return view.tryExecutionPlan(viewQ, trace)
	})
	if chosen.trace.Plan != string(PlanLimitOrderBasic) {
		t.Fatalf("expected chosen execution plan %q, got %q", PlanLimitOrderBasic, chosen.trace.Plan)
	}

	ordered := h.run("shadow_order_limit_planner", func(view *View, viewQ *qir.Shape, trace *Trace) ([]uint64, bool, error) {
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
				return nil, true, nil
			}
		}
		out, ok := view.execPlanOrderedBasicReader(viewQ, predSet.owner, trace)
		return out, ok, nil
	})

	h.assertEquivalent(chosen, ordered)
	plannerShadowAssertNoMoreRowsExamined(t, chosen, ordered)
}

func TestPlannerShadow_CandidateOrderVsOrderedPlanner(t *testing.T) {
	recorder := &traceContractRecorder{}
	db := newTestDB(t, testOptions{
		TraceSink:        recorder.sink,
		TraceSampleEvery: 1,
	})
	_ = db.seedData(t, 20_000)

	q := qx.Query(
		qx.EQ("active", true),
		qx.NOTIN("country", []string{"NL", "DE"}),
		qx.IN("name", []string{"alice", "bob", "carol"}),
	).Sort("age", qx.ASC).Limit(120)

	prepared, viewQ, err := db.prepareQuery(q)
	if err != nil {
		t.Fatalf("prepareQuery: %v", err)
	}
	var leavesBuf [plannerPredicateFastPathMaxLeaves]qir.Expr
	leaves, ok := qir.CollectAndLeavesScratch(viewQ.Expr, leavesBuf[:0], qir.LeafModeCollect)
	if !ok || len(leaves) == 0 {
		t.Fatalf("collectAndLeaves: ok=%v len=%d", ok, len(leaves))
	}
	if !db.view().shouldUseCandidateOrder(viewQ.Order, leaves) {
		prepared.Release()
		t.Fatalf("expected candidate-order precheck to pass")
	}
	prepared.Release()

	h := newPlannerShadowHarness(t, db, recorder, q)

	candidate := h.run("shadow_candidate_order", func(view *View, viewQ *qir.Shape, trace *Trace) ([]uint64, bool, error) {
		return view.tryPlanCandidate(viewQ, trace)
	})
	if candidate.trace.Plan != string(PlanCandidateOrder) {
		t.Fatalf("expected candidate-order plan %q, got %q", PlanCandidateOrder, candidate.trace.Plan)
	}

	ordered := h.run("shadow_candidate_order_alt_planner", func(view *View, viewQ *qir.Shape, trace *Trace) ([]uint64, bool, error) {
		var leavesBuf [plannerPredicateFastPathMaxLeaves]qir.Expr
		l, ok := qir.CollectAndLeavesScratch(viewQ.Expr, leavesBuf[:0], qir.LeafModeCollect)
		if !ok {
			return nil, false, nil
		}
		window, _ := orderWindow(viewQ)
		orderField := view.exec.FieldNameByOrdinal(viewQ.Order.FieldOrdinal)
		predSet, ok := view.buildPredicatesOrderedWithMode(l, orderField, false, window, viewQ.Offset, true, true)
		if !ok {
			return nil, false, nil
		}
		defer predSet.Release()
		for i := 0; i < predSet.Len(); i++ {
			if predSet.owner[i].alwaysFalse {
				return nil, true, nil
			}
		}
		out, ok := view.execPlanOrderedBasicReader(viewQ, predSet.owner, trace)
		return out, ok, nil
	})

	h.assertEquivalent(candidate, ordered)
	plannerShadowAssertNoMoreRowsExamined(t, candidate, ordered)
}

func TestPlannerShadow_CandidateNoOrderVsOrderedPlanner(t *testing.T) {
	recorder := &traceContractRecorder{}
	db := newTestDB(t, testOptions{
		TraceSink:        recorder.sink,
		TraceSampleEvery: 1,
	})
	_ = db.seedData(t, 20_000)

	q := qx.Query(
		qx.EQ("active", true),
		qx.NOTIN("country", []string{"NL", "DE"}),
		qx.IN("name", []string{"alice", "bob", "carol"}),
	).Limit(90)

	h := newPlannerShadowHarness(t, db, recorder, q)

	candidate := h.run("shadow_candidate_no_order", func(view *View, viewQ *qir.Shape, trace *Trace) ([]uint64, bool, error) {
		return view.tryPlanCandidate(viewQ, trace)
	})
	if candidate.trace.Plan != string(PlanCandidateNoOrder) {
		t.Fatalf("expected candidate no-order plan %q, got %q", PlanCandidateNoOrder, candidate.trace.Plan)
	}

	ordered := h.run("shadow_candidate_no_order_alt_planner", func(view *View, viewQ *qir.Shape, trace *Trace) ([]uint64, bool, error) {
		return view.tryPlanOrdered(viewQ, trace)
	})
	if ordered.trace.Plan != string(PlanOrderedNoOrder) {
		t.Fatalf("expected no-order planner plan %q, got %q", PlanOrderedNoOrder, ordered.trace.Plan)
	}

	plannerShadowAssertNoMoreRowsExamined(t, candidate, ordered)
}
