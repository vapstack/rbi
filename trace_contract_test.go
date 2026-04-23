package rbi

import (
	"fmt"
	"sync"
	"testing"

	"github.com/vapstack/qx"
)

type traceContractRecorder struct {
	mu     sync.Mutex
	events []TraceEvent
}

func (r *traceContractRecorder) sink(ev TraceEvent) {
	r.mu.Lock()
	r.events = append(r.events, ev)
	r.mu.Unlock()
}

func (r *traceContractRecorder) mark() int {
	r.mu.Lock()
	n := len(r.events)
	r.mu.Unlock()
	return n
}

func (r *traceContractRecorder) lastSince(t testing.TB, mark int) TraceEvent {
	t.Helper()

	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.events) <= mark {
		t.Fatalf("expected trace event after mark=%d, total=%d", mark, len(r.events))
	}
	return r.events[len(r.events)-1]
}

func (r *traceContractRecorder) eventsSince(t testing.TB, mark int) []TraceEvent {
	t.Helper()

	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.events) <= mark {
		t.Fatalf("expected trace events after mark=%d, total=%d", mark, len(r.events))
	}
	out := append([]TraceEvent(nil), r.events[mark:]...)
	return out
}

func traceContractAssertBase(t testing.TB, ev TraceEvent, rowsReturned uint64) {
	t.Helper()

	if ev.Timestamp.IsZero() {
		t.Fatalf("expected trace timestamp, trace=%+v", ev)
	}
	if ev.Duration <= 0 {
		t.Fatalf("expected positive trace duration, trace=%+v", ev)
	}
	if ev.Plan == "" {
		t.Fatalf("expected non-empty trace plan, trace=%+v", ev)
	}
	if ev.Error != "" {
		t.Fatalf("unexpected trace error=%q, trace=%+v", ev.Error, ev)
	}
	if ev.RowsReturned != rowsReturned {
		t.Fatalf("rows returned mismatch: trace=%d want=%d", ev.RowsReturned, rowsReturned)
	}
	if ev.RowsMatched != 0 && ev.RowsMatched < ev.RowsReturned {
		t.Fatalf("expected RowsMatched >= RowsReturned when populated, trace=%+v", ev)
	}
	if ev.HasOrder {
		if ev.OrderField == "" {
			t.Fatalf("expected order field for ordered trace, trace=%+v", ev)
		}
		return
	}
	if ev.OrderField != "" || ev.OrderDesc {
		t.Fatalf("unexpected order metadata on unordered trace, trace=%+v", ev)
	}
}

func traceContractAssertQueryResultTrace(t testing.TB, ev TraceEvent, rowsReturned uint64) {
	t.Helper()

	traceContractAssertBase(t, ev, rowsReturned)
	if ev.RowsExamined < ev.RowsReturned {
		t.Fatalf("expected RowsExamined >= RowsReturned for query trace, trace=%+v", ev)
	}
}

func traceContractAssertORBranches(t testing.TB, branches []TraceORBranch, wantLen int) {
	t.Helper()

	if len(branches) != wantLen {
		t.Fatalf("unexpected OR branch trace size: got=%d want=%d trace=%+v", len(branches), wantLen, branches)
	}

	var seen [plannerORBranchLimit]bool
	var branchExamined uint64
	var branchEmitted uint64

	for _, br := range branches {
		if br.Index < 0 || br.Index >= wantLen {
			t.Fatalf("unexpected OR branch index=%d for len=%d trace=%+v", br.Index, wantLen, branches)
		}
		if seen[br.Index] {
			t.Fatalf("duplicate OR branch index=%d trace=%+v", br.Index, branches)
		}
		seen[br.Index] = true
		if br.RowsEmitted > br.RowsExamined {
			t.Fatalf("expected branch RowsExamined >= RowsEmitted, branch=%+v", br)
		}
		if br.Skipped && br.SkipReason == "" {
			t.Fatalf("expected skip reason for skipped branch, branch=%+v", br)
		}
		branchExamined += br.RowsExamined
		branchEmitted += br.RowsEmitted
	}

	if branchExamined == 0 {
		t.Fatalf("expected OR branch examined rows, trace=%+v", branches)
	}
	if branchEmitted == 0 {
		t.Fatalf("expected OR branch emitted rows, trace=%+v", branches)
	}
}

type traceContractCase struct {
	name   string
	run    func(t *testing.T, db *DB[uint64, Rec]) uint64
	assert func(t *testing.T, ev TraceEvent, rowsReturned uint64)
}

func TestTraceContract_PublicAPIFamilies(t *testing.T) {
	recorder := &traceContractRecorder{}

	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSink:        recorder.sink,
		TraceSampleEvery: 1,
	})
	_ = seedData(t, db, 20_000)

	countORHybridExpr := qx.OR(
		qx.AND(
			qx.HASANY("tags", []string{"go", "db"}),
			qx.EQ("country", "NL"),
		),
		qx.AND(
			qx.HASANY("tags", []string{"rust", "ops"}),
			qx.EQ("active", true),
		),
		qx.AND(
			qx.PREFIX("full_name", "FN-1"),
			qx.EQ("active", true),
		),
		qx.AND(
			qx.EQ("name", "alice"),
			qx.EQ("active", true),
		),
	)

	cases := []traceContractCase{
		{
			name: "query_keys_order_basic_limit",
			run: func(t *testing.T, db *DB[uint64, Rec]) uint64 {
				t.Helper()

				q := qx.Query(
					qx.EQ("active", true),
					qx.GTE("age", 22),
					qx.LT("age", 45),
				).Sort("age", qx.ASC).Limit(120)

				got, err := db.QueryKeys(q)
				if err != nil {
					t.Fatalf("QueryKeys: %v", err)
				}
				if len(got) == 0 {
					t.Fatalf("expected non-empty ids")
				}
				return uint64(len(got))
			},
			assert: func(t *testing.T, ev TraceEvent, rowsReturned uint64) {
				t.Helper()

				traceContractAssertQueryResultTrace(t, ev, rowsReturned)
				if !ev.HasOrder || ev.OrderField != "age" || ev.OrderDesc {
					t.Fatalf("unexpected ordered trace metadata: %+v", ev)
				}
				if ev.Limit != 120 || ev.Offset != 0 {
					t.Fatalf("unexpected window metadata: trace=%+v", ev)
				}
				if ev.EstimatedRows == 0 || ev.EstimatedCost <= 0 || ev.FallbackCost <= 0 {
					t.Fatalf("expected costed order-basic trace, trace=%+v", ev)
				}
				if ev.OrderIndexScanWidth == 0 {
					t.Fatalf("expected order scan width, trace=%+v", ev)
				}
				if ev.EarlyStopReason == "" {
					t.Fatalf("expected early stop reason, trace=%+v", ev)
				}
				if len(ev.ORBranches) != 0 {
					t.Fatalf("unexpected OR branch trace for order-basic query, trace=%+v", ev.ORBranches)
				}
				if ev.ORRoute.Route != "" {
					t.Fatalf("unexpected ordered-OR route trace for order-basic query, trace=%+v", ev.ORRoute)
				}
			},
		},
		{
			name: "query_values_order_basic_limit",
			run: func(t *testing.T, db *DB[uint64, Rec]) uint64 {
				t.Helper()

				q := qx.Query(
					qx.EQ("active", true),
					qx.GTE("age", 22),
					qx.LT("age", 45),
				).Sort("age", qx.ASC).Limit(80)

				items, err := db.Query(q)
				if err != nil {
					t.Fatalf("Query: %v", err)
				}
				if len(items) == 0 {
					t.Fatalf("expected non-empty records")
				}
				n := uint64(len(items))
				db.ReleaseRecords(items...)
				return n
			},
			assert: func(t *testing.T, ev TraceEvent, rowsReturned uint64) {
				t.Helper()

				traceContractAssertQueryResultTrace(t, ev, rowsReturned)
				if !ev.HasOrder || ev.OrderField != "age" || ev.OrderDesc {
					t.Fatalf("unexpected ordered trace metadata: %+v", ev)
				}
				if ev.Limit != 80 || ev.Offset != 0 {
					t.Fatalf("unexpected window metadata: trace=%+v", ev)
				}
				if ev.OrderIndexScanWidth == 0 {
					t.Fatalf("expected order scan width, trace=%+v", ev)
				}
				if ev.EarlyStopReason == "" {
					t.Fatalf("expected early stop reason, trace=%+v", ev)
				}
				if len(ev.ORBranches) != 0 || ev.ORRoute.Route != "" {
					t.Fatalf("unexpected OR trace for Query values path, trace=%+v", ev)
				}
			},
		},
		{
			name: "query_keys_or_no_order_limit",
			run: func(t *testing.T, db *DB[uint64, Rec]) uint64 {
				t.Helper()

				q := qx.Query(
					qx.OR(
						qx.EQ("active", true),
						qx.EQ("name", "alice"),
					),
				).Limit(120)

				got, err := db.QueryKeys(q)
				if err != nil {
					t.Fatalf("QueryKeys: %v", err)
				}
				if len(got) == 0 {
					t.Fatalf("expected non-empty ids")
				}
				return uint64(len(got))
			},
			assert: func(t *testing.T, ev TraceEvent, rowsReturned uint64) {
				t.Helper()

				traceContractAssertQueryResultTrace(t, ev, rowsReturned)
				if ev.HasOrder || ev.OrderIndexScanWidth != 0 {
					t.Fatalf("unexpected order trace for unordered OR query, trace=%+v", ev)
				}
				if ev.EstimatedRows == 0 || ev.EstimatedCost <= 0 || ev.FallbackCost <= 0 {
					t.Fatalf("expected costed unordered OR trace, trace=%+v", ev)
				}
				if ev.EarlyStopReason == "" {
					t.Fatalf("expected early stop reason, trace=%+v", ev)
				}
				if ev.ORRoute.Route != "" {
					t.Fatalf("unexpected ordered-OR route trace for unordered OR query, trace=%+v", ev.ORRoute)
				}
				traceContractAssertORBranches(t, ev.ORBranches, 2)
			},
		},
		{
			name: "query_keys_or_order_limit",
			run: func(t *testing.T, db *DB[uint64, Rec]) uint64 {
				t.Helper()

				q := qx.Query(
					qx.OR(
						qx.EQ("active", true),
						qx.EQ("name", "alice"),
					),
				).Sort("age", qx.ASC).Limit(80)

				got, err := db.QueryKeys(q)
				if err != nil {
					t.Fatalf("QueryKeys: %v", err)
				}
				if len(got) == 0 {
					t.Fatalf("expected non-empty ids")
				}
				return uint64(len(got))
			},
			assert: func(t *testing.T, ev TraceEvent, rowsReturned uint64) {
				t.Helper()

				traceContractAssertQueryResultTrace(t, ev, rowsReturned)
				if !ev.HasOrder || ev.OrderField != "age" || ev.OrderDesc {
					t.Fatalf("unexpected ordered trace metadata: %+v", ev)
				}
				if ev.OrderIndexScanWidth == 0 {
					t.Fatalf("expected order scan width, trace=%+v", ev)
				}
				if ev.EstimatedRows == 0 || ev.EstimatedCost <= 0 || ev.FallbackCost <= 0 {
					t.Fatalf("expected costed ordered OR trace, trace=%+v", ev)
				}
				if ev.EarlyStopReason == "" {
					t.Fatalf("expected early stop reason, trace=%+v", ev)
				}
				if ev.ORRoute.Route != "" {
					if ev.ORRoute.Reason == "" {
						t.Fatalf("expected ordered-OR route reason, trace=%+v", ev.ORRoute)
					}
					if ev.ORRoute.KWayCost <= 0 || ev.ORRoute.FallbackCost <= 0 {
						t.Fatalf("expected ordered-OR route costs, trace=%+v", ev.ORRoute)
					}
				}
				if ev.ORRoute.RuntimeGuardEnabled && ev.ORRoute.RuntimeGuardReason == "" {
					t.Fatalf("expected runtime guard reason, trace=%+v", ev.ORRoute)
				}
				if ev.ORRoute.RuntimeFallbackTriggered && ev.ORRoute.RuntimeFallbackReason == "" {
					t.Fatalf("expected runtime fallback reason, trace=%+v", ev.ORRoute)
				}
				traceContractAssertORBranches(t, ev.ORBranches, 2)
			},
		},
		{
			name: "count_or_hybrid",
			run: func(t *testing.T, db *DB[uint64, Rec]) uint64 {
				t.Helper()

				q := qx.Query(countORHybridExpr)
				cnt, err := db.Count(q.Filter)
				if err != nil {
					t.Fatalf("Count: %v", err)
				}
				if cnt == 0 {
					t.Fatalf("expected non-zero count")
				}
				return cnt
			},
			assert: func(t *testing.T, ev TraceEvent, rowsReturned uint64) {
				t.Helper()

				traceContractAssertBase(t, ev, rowsReturned)
				if ev.HasOrder || ev.OrderIndexScanWidth != 0 {
					t.Fatalf("unexpected order trace for count query, trace=%+v", ev)
				}
				if ev.EarlyStopReason != "" {
					t.Fatalf("unexpected early stop reason for count query, trace=%+v", ev)
				}
				if ev.ORRoute.Route != "" {
					t.Fatalf("unexpected ordered-OR route trace for count query, trace=%+v", ev.ORRoute)
				}
				traceContractAssertORBranches(t, ev.ORBranches, 4)

				spilled := false
				for _, br := range ev.ORBranches {
					if br.SkipReason == "materialized_spill" {
						spilled = true
						break
					}
				}
				if !spilled {
					t.Fatalf("expected hybrid count trace to record a spill branch, trace=%+v", ev.ORBranches)
				}
			},
		},
	}

	for i := range cases {
		tc := cases[i]
		t.Run(tc.name, func(t *testing.T) {
			mark := recorder.mark()
			rowsReturned := tc.run(t, db)
			ev := recorder.lastSince(t, mark)
			tc.assert(t, ev, rowsReturned)
		})
	}
}

func TestTraceContract_OrderedORMergeRouteDecision(t *testing.T) {
	recorder := &traceContractRecorder{}

	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSink:        recorder.sink,
		TraceSampleEvery: 1,
	})
	_ = seedData(t, db, 20_000)

	q := plannerArgminOROrderMergeQuery()
	preparedQ, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer preparedQ.Release()

	window, _ := orderWindowForTest(q)
	branches, alwaysFalse, ok := db.buildORBranchesOrdered(q.Filter.Args, "score", window)
	if !ok {
		t.Fatalf("buildORBranchesOrdered: ok=false")
	}
	if alwaysFalse {
		branches.Release()
		t.Fatalf("unexpected alwaysFalse for ordered OR branches")
	}
	defer branches.Release()

	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)

	analysis, ok := view.buildOROrderAnalysis(&viewQ, branches)
	if !ok {
		t.Fatalf("buildOROrderAnalysis: ok=false")
	}
	defer analysis.release()

	mark := recorder.mark()
	tr := db.beginTrace(viewQ)
	if tr == nil {
		t.Fatalf("expected trace to be enabled")
	}
	out, used, err := view.execPlanOROrderMerge(&viewQ, branches, &analysis, tr)
	tr.setPlan(PlanORMergeOrderMerge)
	tr.finish(uint64(len(out)), err)
	if err != nil {
		t.Fatalf("execPlanOROrderMerge: %v", err)
	}
	if !used {
		t.Fatalf("expected ordered OR merge route to be used")
	}
	if len(out) == 0 {
		t.Fatalf("expected non-empty ordered OR merge result")
	}

	ev := recorder.lastSince(t, mark)
	traceContractAssertQueryResultTrace(t, ev, uint64(len(out)))
	if !ev.HasOrder || ev.OrderField != "score" || !ev.OrderDesc {
		t.Fatalf("unexpected ordered trace metadata: %+v", ev)
	}
	if ev.OrderIndexScanWidth == 0 {
		t.Fatalf("expected order scan width, trace=%+v", ev)
	}
	if ev.EarlyStopReason == "" {
		t.Fatalf("expected early stop reason, trace=%+v", ev)
	}
	if ev.ORRoute.Route == "" || ev.ORRoute.Reason == "" {
		t.Fatalf("expected ordered-OR route decision, trace=%+v", ev.ORRoute)
	}
	if ev.ORRoute.KWayCost <= 0 || ev.ORRoute.FallbackCost <= 0 {
		t.Fatalf("expected ordered-OR route costs, trace=%+v", ev.ORRoute)
	}
	if ev.ORRoute.RuntimeGuardEnabled && ev.ORRoute.RuntimeGuardReason == "" {
		t.Fatalf("expected runtime guard reason, trace=%+v", ev.ORRoute)
	}
	if ev.ORRoute.RuntimeFallbackTriggered && ev.ORRoute.RuntimeFallbackReason == "" {
		t.Fatalf("expected runtime fallback reason, trace=%+v", ev.ORRoute)
	}
	traceContractAssertORBranches(t, ev.ORBranches, 2)
}

func TestTraceContract_ExactFilterWorkCounters(t *testing.T) {
	recorder := &traceContractRecorder{}

	db, _ := openTempDBUint64(t, Options{
		TraceSink:        recorder.sink,
		TraceSampleEvery: 1,
	})

	seedGeneratedUint64Data(t, db, 12_000, func(i int) *Rec {
		countries := [...]string{"NL", "DE", "US", "GB"}
		group := (i - 1) / 64
		return &Rec{
			Name:     fmt.Sprintf("u_%d", i),
			Email:    fmt.Sprintf("u_%d@example.test", i),
			Age:      i % 64,
			Active:   group%2 == 0,
			Score:    float64(i),
			Meta:     Meta{Country: countries[group%len(countries)]},
			FullName: fmt.Sprintf("grp-%02d", i%64),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	cases := []traceContractCase{
		{
			name: "range_no_order_limit_exact_filters",
			run: func(t *testing.T, db *DB[uint64, Rec]) uint64 {
				t.Helper()

				q := qx.Query(
					qx.GTE("age", 0),
					qx.LT("age", 64),
					qx.IN("country", []string{"NL", "DE"}),
					qx.EQ("active", true),
				).Limit(25)

				leaves := mustExtractAndLeaves(t, q.Filter)
				fieldName, bounds, ok, err := db.extractNoOrderBounds(leaves)
				if err != nil {
					t.Fatalf("extractNoOrderBounds: %v", err)
				}
				if !ok {
					t.Fatalf("expected no-order bounds to be recognized")
				}

				preparedQ, viewQ, err := prepareTestQuery(db, q)
				if err != nil {
					t.Fatalf("prepareTestQuery: %v", err)
				}
				defer preparedQ.Release()

				tr := db.beginTrace(viewQ)
				if tr == nil {
					t.Fatalf("expected trace to be enabled")
				}
				tr.setPlan(PlanLimitRangeNoOrder)
				got, used, err := db.tryLimitQueryRangeNoOrderByField(q, fieldName, bounds, leaves, tr)
				tr.finish(uint64(len(got)), err)
				if err != nil {
					t.Fatalf("tryLimitQueryRangeNoOrderByField: %v", err)
				}
				if !used {
					t.Fatalf("expected range-no-order limit fast path to be used")
				}
				if len(got) == 0 {
					t.Fatalf("expected non-empty ids")
				}
				return uint64(len(got))
			},
			assert: func(t *testing.T, ev TraceEvent, rowsReturned uint64) {
				t.Helper()

				traceContractAssertQueryResultTrace(t, ev, rowsReturned)
				if ev.HasOrder || ev.OrderIndexScanWidth != 0 {
					t.Fatalf("unexpected order trace for no-order exact-filter query, trace=%+v", ev)
				}
				if ev.PostingExactFilters == 0 {
					t.Fatalf("expected exact filter work counter, trace=%+v", ev)
				}
				if ev.RowsMatched == 0 {
					t.Fatalf("expected matched rows to be tracked, trace=%+v", ev)
				}
				if ev.EarlyStopReason == "" {
					t.Fatalf("expected early stop reason, trace=%+v", ev)
				}
			},
		},
		{
			name: "order_basic_limit_exact_filters",
			run: func(t *testing.T, db *DB[uint64, Rec]) uint64 {
				t.Helper()

				q := qx.Query(
					qx.GTE("age", 0),
					qx.LT("age", 64),
					qx.EQ("active", true),
					qx.EQ("country", "NL"),
				).Sort("age", qx.ASC).Limit(20)

				leaves := mustExtractAndLeaves(t, q.Filter)
				preparedQ, viewQ, err := prepareTestQuery(db, q)
				if err != nil {
					t.Fatalf("prepareTestQuery: %v", err)
				}
				defer preparedQ.Release()

				tr := db.beginTrace(viewQ)
				if tr == nil {
					t.Fatalf("expected trace to be enabled")
				}
				tr.setPlan(PlanLimitOrderBasic)
				got, used, err := db.tryLimitQueryOrderBasic(q, leaves, tr)
				tr.finish(uint64(len(got)), err)
				if err != nil {
					t.Fatalf("tryLimitQueryOrderBasic: %v", err)
				}
				if !used {
					t.Fatalf("expected order-basic limit fast path to be used")
				}
				if len(got) == 0 {
					t.Fatalf("expected non-empty ids")
				}
				return uint64(len(got))
			},
			assert: func(t *testing.T, ev TraceEvent, rowsReturned uint64) {
				t.Helper()

				traceContractAssertQueryResultTrace(t, ev, rowsReturned)
				if !ev.HasOrder || ev.OrderField != "age" || ev.OrderDesc {
					t.Fatalf("unexpected ordered trace metadata: %+v", ev)
				}
				if ev.PostingExactFilters == 0 {
					t.Fatalf("expected exact filter work counter, trace=%+v", ev)
				}
				if ev.OrderIndexScanWidth == 0 {
					t.Fatalf("expected order scan width, trace=%+v", ev)
				}
				if ev.EarlyStopReason == "" {
					t.Fatalf("expected early stop reason, trace=%+v", ev)
				}
			},
		},
	}

	for i := range cases {
		tc := cases[i]
		t.Run(tc.name, func(t *testing.T) {
			mark := recorder.mark()
			rowsReturned := tc.run(t, db)
			ev := recorder.lastSince(t, mark)
			tc.assert(t, ev, rowsReturned)
		})
	}
}

func TestTraceContract_CountComplementLifecycle(t *testing.T) {
	recorder := &traceContractRecorder{}

	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSink:        recorder.sink,
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
		qx.EQ("country", "US"),
		qx.NOTIN("active", []bool{false}),
		qx.GTE("age", 35_000),
	)

	mark := recorder.mark()

	first, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("first Count: %v", err)
	}
	second, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("second Count: %v", err)
	}
	third, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("third Count: %v", err)
	}
	if first == 0 || second != first || third != first {
		t.Fatalf("unexpected counts: first=%d second=%d third=%d", first, second, third)
	}

	events := recorder.eventsSince(t, mark)
	if len(events) != 3 {
		t.Fatalf("expected exactly three trace events, got=%d", len(events))
	}

	ev1 := events[0]
	ev2 := events[1]
	ev3 := events[2]

	traceContractAssertBase(t, ev1, first)
	traceContractAssertBase(t, ev2, second)
	traceContractAssertBase(t, ev3, third)

	if ev1.Plan != string(PlanCountPredicates) || ev2.Plan != string(PlanCountPredicates) || ev3.Plan != string(PlanCountPredicates) {
		t.Fatalf("expected predicate-count trace plans, got=%q %q %q", ev1.Plan, ev2.Plan, ev3.Plan)
	}
	if ev1.CountPredicatePreparations == 0 {
		t.Fatalf("expected initial predicate preparation, trace=%+v", ev1)
	}
	if ev1.CountRangeComplementBuilds != 0 || ev1.CountRangeComplementCacheHits != 0 {
		t.Fatalf("expected initial run to keep complement local, trace=%+v", ev1)
	}
	if ev2.CountRangeComplementBuilds == 0 {
		t.Fatalf("expected promoted complement build on second run, trace=%+v", ev2)
	}
	if ev2.CountRangeComplementCacheHits != 0 {
		t.Fatalf("expected second run without complement cache hit, trace=%+v", ev2)
	}
	if ev2.CountRangeComplementRows == 0 {
		t.Fatalf("expected complement rows metric on second run, trace=%+v", ev2)
	}
	if ev3.CountRangeComplementCacheHits == 0 {
		t.Fatalf("expected complement cache hit on third run, trace=%+v", ev3)
	}
	if ev3.CountRangeComplementBuilds != 0 {
		t.Fatalf("expected third run to skip complement rebuild, trace=%+v", ev3)
	}
}
