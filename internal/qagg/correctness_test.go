package qagg

import (
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"unsafe"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/qexec"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/rbitrace"
)

type qaggGroupedOrdinaryMapRec struct {
	Group  int   `db:"group"  rbi:"index"`
	Filter int   `db:"filter" rbi:"index"`
	Value  int64 `db:"value"  rbi:"index"`
}

type qaggGroupedOrdinaryOverflowRec struct {
	Group int   `db:"group" rbi:"index"`
	Value int64 `db:"value" rbi:"index"`
}

type qaggCountDistinctUniqueRec struct {
	Keep  bool   `db:"keep"  rbi:"index"`
	Value *int64 `db:"value" rbi:"index"`
}

func TestGroupedRecursiveEqualsGroupedByIDForDenseOrdinaryInput(t *testing.T) {
	db := newQaggTestDB(t, nil)
	prepared, err := Prepare(
		qx.Group("country").Metrics(
			qx.ROWCOUNT().AS("rows"),
			qx.COUNT("age").AS("age_count"),
			qx.SUM("age").AS("age_sum"),
			qx.AVG("age").AS("age_avg"),
			qx.MIN("age").AS("age_min"),
			qx.MAX("age").AS("age_max"),
		),
		db.rt,
		db.rt.IndexedByName,
	)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer prepared.Release()

	view := db.view()
	defer db.exec.ReleaseView(view)
	ids, err := view.Filter(prepared.filter)
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	defer ids.Release()

	exec := aggregateExecutor{snap: db.snap}
	recursive, err := exec.executeGroupedRecursiveAggregate(prepared, ids)
	if err != nil {
		t.Fatalf("recursive: %v", err)
	}
	decision := selectGroupedAggregate(exec.collectGroupedFacts(prepared, ids))
	if decision.route != aggregateRouteGroupedOrdinaryByID || decision.groupLookup != aggregateGroupLookupOrdinalSlice {
		t.Fatalf("decision=%+v", decision)
	}
	byID, err := exec.executeGroupedOrdinaryByID(prepared, ids, decision)
	if err != nil {
		t.Fatalf("byID: %v", err)
	}
	requireQaggResultsEqualUnordered(t, recursive, byID)
}

func TestGroupedSparseHighIDSelectsRecursiveAndMatchesReference(t *testing.T) {
	db := newQaggSparseIDTestDB(t)
	prepared, err := Prepare(qx.Group("country").Metrics(qx.SUM("age").AS("sum")), db.rt, db.rt.IndexedByName)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer prepared.Release()

	view := db.view()
	defer db.exec.ReleaseView(view)
	ids, err := view.Filter(prepared.filter)
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	defer ids.Release()

	exec := aggregateExecutor{snap: db.snap}
	decision := selectGroupedAggregate(exec.collectGroupedFacts(prepared, ids))
	if decision.route != aggregateRouteGroupedRecursive {
		t.Fatalf("route=%s, want grouped_recursive; decision=%+v", decision.route, decision)
	}
	if decision.rejected != aggregateRouteGroupedOrdinaryByID || decision.selectedCost <= 0 || decision.rejectedCost <= 0 {
		t.Fatalf("decision missing rejected by-ID route/cost: %+v", decision)
	}

	selected, err := Execute(view, db.snap, prepared)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	rows, _ := qaggSparseRows()
	requireQaggResultsEqualUnordered(t, selected, qaggReferenceGroupedAgeSum(rows, []string{"country", "sum"}))
}

func TestGroupedOrdinaryMapRouteMatchesRecursive(t *testing.T) {
	rt, err := schema.Compile(reflect.TypeFor[qaggGroupedOrdinaryMapRec](), schema.Config{})
	if err != nil {
		t.Fatalf("schema.Compile: %v", err)
	}
	execRuntime := qexec.NewRuntime(qexec.Config{Schema: rt})
	rows := make([]qaggGroupedOrdinaryMapRec, 640)
	entries := make([]snapshot.BatchEntry, len(rows))
	for i := range rows {
		row := uint64(i + 1)
		rows[i] = qaggGroupedOrdinaryMapRec{
			Group:  int(row & 63),
			Filter: int(row % 5),
			Value:  int64(row),
		}
		entries[i] = snapshot.BatchEntry{ID: row * 1_000_000, New: unsafe.Pointer(&rows[i])}
	}
	snap := snapshot.Build(1, nil, rt, snapshot.CacheConfig{}, nil, entries)
	view := execRuntime.AcquireView(snap)
	defer execRuntime.ReleaseView(view)

	prepared, err := Prepare(
		qx.Query(qx.EQ("filter", 0)).
			Group("group").
			Metrics(qx.ROWCOUNT().AS("rows"), qx.SUM("value").AS("sum")),
		rt,
		rt.IndexedByName,
	)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer prepared.Release()

	ids, err := view.Filter(prepared.filter)
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	defer ids.Release()

	aggExec := aggregateExecutor{snap: snap}
	decision := selectGroupedAggregate(aggExec.collectGroupedFacts(prepared, ids))
	if decision.route != aggregateRouteGroupedOrdinaryByID || decision.groupLookup != aggregateGroupLookupMap {
		t.Fatalf("decision=%+v", decision)
	}
	selected, err := aggExec.dispatchAggregateRoute(prepared, ids, decision)
	if err != nil {
		t.Fatalf("selected: %v", err)
	}
	recursive, err := aggExec.executeGroupedRecursiveAggregate(prepared, ids)
	if err != nil {
		t.Fatalf("recursive: %v", err)
	}
	requireQaggResultsEqualUnordered(t, selected, recursive)
}

func TestGroupedOrdinaryByIDSharedFieldOverflowReturnsError(t *testing.T) {
	rt, err := schema.Compile(reflect.TypeFor[qaggGroupedOrdinaryOverflowRec](), schema.Config{})
	if err != nil {
		t.Fatalf("schema.Compile: %v", err)
	}
	execRuntime := qexec.NewRuntime(qexec.Config{Schema: rt})
	rows := []qaggGroupedOrdinaryOverflowRec{
		{Group: 1, Value: math.MaxInt64},
		{Group: 1, Value: 1},
		{Group: 2, Value: 0},
	}
	entries := make([]snapshot.BatchEntry, len(rows))
	for i := range rows {
		entries[i] = snapshot.BatchEntry{ID: uint64(i + 1), New: unsafe.Pointer(&rows[i])}
	}
	snap := snapshot.Build(1, nil, rt, snapshot.CacheConfig{}, nil, entries)
	view := execRuntime.AcquireView(snap)
	defer execRuntime.ReleaseView(view)

	prepared, err := Prepare(
		qx.Group("group").Metrics(qx.SUM("value").AS("sum"), qx.AVG("value").AS("avg")),
		rt,
		rt.IndexedByName,
	)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer prepared.Release()

	ids, err := view.Filter(prepared.filter)
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	defer ids.Release()

	aggExec := aggregateExecutor{snap: snap}
	decision := selectGroupedAggregate(aggExec.collectGroupedFacts(prepared, ids))
	if decision.route != aggregateRouteGroupedOrdinaryByID || decision.groupLookup != aggregateGroupLookupOrdinalSlice {
		t.Fatalf("decision=%+v", decision)
	}
	_, err = aggExec.dispatchAggregateRoute(prepared, ids, decision)
	if err == nil || !strings.Contains(err.Error(), "integer SUM overflow") {
		t.Fatalf("dispatch err=%v, want integer SUM overflow", err)
	}
}

func TestGroupedOrdinaryByIDMapSharedFieldOverflowReturnsError(t *testing.T) {
	rt, err := schema.Compile(reflect.TypeFor[qaggGroupedOrdinaryOverflowRec](), schema.Config{})
	if err != nil {
		t.Fatalf("schema.Compile: %v", err)
	}
	execRuntime := qexec.NewRuntime(qexec.Config{Schema: rt})
	rows := make([]qaggGroupedOrdinaryOverflowRec, 640)
	entries := make([]snapshot.BatchEntry, len(rows))
	for i := range rows {
		group := i & 63
		value := int64(i + 2)
		switch i {
		case 0:
			group = 0
			value = math.MaxInt64
		case 1:
			group = 0
			value = 1
		}
		rows[i] = qaggGroupedOrdinaryOverflowRec{Group: group, Value: value}
		entries[i] = snapshot.BatchEntry{ID: uint64(i+1) * 1_000_000, New: unsafe.Pointer(&rows[i])}
	}
	snap := snapshot.Build(1, nil, rt, snapshot.CacheConfig{}, nil, entries)
	view := execRuntime.AcquireView(snap)
	defer execRuntime.ReleaseView(view)

	prepared, err := Prepare(
		qx.Group("group").Metrics(qx.SUM("value").AS("sum"), qx.AVG("value").AS("avg")),
		rt,
		rt.IndexedByName,
	)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer prepared.Release()

	ids, err := view.Filter(prepared.filter)
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	defer ids.Release()

	aggExec := aggregateExecutor{snap: snap}
	decision := selectGroupedAggregate(aggExec.collectGroupedFacts(prepared, ids))
	if decision.route != aggregateRouteGroupedOrdinaryByID || decision.groupLookup != aggregateGroupLookupMap {
		t.Fatalf("decision=%+v", decision)
	}
	_, err = aggExec.dispatchAggregateRoute(prepared, ids, decision)
	if err == nil || !strings.Contains(err.Error(), "integer SUM overflow") {
		t.Fatalf("dispatch err=%v, want integer SUM overflow", err)
	}
}

func TestGroupedMeasureLookupRoutesMatchRecursive(t *testing.T) {
	db := newQaggTestDB(t, nil)
	tests := []struct {
		name string
		q    *qx.QX
		want aggregateRouteCandidate
	}{
		{
			name: "measure",
			q:    qx.Group("age").Metrics(qx.SUM("amount").AS("amount_sum")),
			want: aggregateRouteGroupedMeasure,
		},
		{
			name: "hybrid",
			q:    qx.Group("age").Metrics(qx.SUM("age").AS("age_sum"), qx.SUM("amount").AS("amount_sum")),
			want: aggregateRouteGroupedHybrid,
		},
	}

	view := db.view()
	defer db.exec.ReleaseView(view)
	exec := aggregateExecutor{snap: db.snap}
	for i := range tests {
		tc := tests[i]
		prepared, err := Prepare(tc.q, db.rt, db.rt.IndexedByName)
		if err != nil {
			t.Fatalf("%s Prepare: %v", tc.name, err)
		}
		ids, err := view.Filter(prepared.filter)
		if err != nil {
			prepared.Release()
			t.Fatalf("%s Filter: %v", tc.name, err)
		}

		decision := selectGroupedAggregate(exec.collectGroupedFacts(prepared, ids))
		if decision.route != tc.want || decision.groupLookup != aggregateGroupLookupOrdinalSlice {
			ids.Release()
			prepared.Release()
			t.Fatalf("%s decision=%+v", tc.name, decision)
		}
		selected, err := exec.dispatchAggregateRoute(prepared, ids, decision)
		if err != nil {
			ids.Release()
			prepared.Release()
			t.Fatalf("%s selected: %v", tc.name, err)
		}
		recursive, err := exec.executeGroupedRecursiveAggregate(prepared, ids)
		ids.Release()
		prepared.Release()
		if err != nil {
			t.Fatalf("%s recursive: %v", tc.name, err)
		}
		requireQaggResultsEqualUnordered(t, selected, recursive)
	}
}

func TestAggregateExecuteReleaseReuseKeepsResultsOwned(t *testing.T) {
	db := newQaggTestDB(t, nil)
	cases := []struct {
		name string
		q    *qx.QX
		want Result
	}{
		{
			name: "filtered_group_measure",
			q: qx.Query(qx.GTE("age", 30)).
				Group("country").
				Metrics(qx.ROWCOUNT().AS("rows"), qx.SUM("amount").AS("sum")),
			want: Result{Layout: []string{"country", "rows", "sum"}, Rows: []Row{
				{valueFromSafeString("DE"), Value{num: 2, any: ValueKindUint}, Value{num: 35, any: ValueKindInt}},
				{valueFromSafeString("NL"), Value{num: 1, any: ValueKindUint}, Value{num: 0, any: ValueKindInt}},
				{valueFromSafeString("PL"), Value{num: 1, any: ValueKindUint}, Value{num: 50, any: ValueKindInt}},
				{valueFromSafeString("US"), Value{num: 1, any: ValueKindUint}, Value{num: 40, any: ValueKindInt}},
			}},
		},
		{
			name: "hybrid_group",
			q: qx.Group("country").Metrics(
				qx.SUM("age").AS("age_sum"),
				qx.SUM("amount").AS("amount_sum"),
			),
			want: qaggReferenceGroupedAgeAmountSums(qaggDefaultRows(), []string{"country", "age_sum", "amount_sum"}),
		},
		{
			name: "filtered_group_string",
			q: qx.Query(qx.EQ("active", true)).
				Group("segment").
				Metrics(qx.ROWCOUNT().AS("rows")),
			want: Result{Layout: []string{"segment", "rows"}, Rows: []Row{
				{valueFromSafeString("core"), Value{num: 2, any: ValueKindUint}},
				{valueFromSafeString("edge"), Value{num: 2, any: ValueKindUint}},
			}},
		},
		{
			name: "distinct_string",
			q:    qx.Aggregate(qx.DISTINCT("country").AS("country")),
			want: Result{Layout: []string{"country"}, Rows: []Row{
				{valueFromSafeString("DE")},
				{valueFromSafeString("NL")},
				{valueFromSafeString("PL")},
				{valueFromSafeString("US")},
			}},
		},
		{
			name: "ungrouped_metrics",
			q: qx.Query(qx.GTE("age", 30)).Metrics(
				qx.COUNT("age").AS("age_count"),
				qx.SUM("age").AS("age_sum"),
				qx.AVG("age").AS("age_avg"),
				qx.MIN("age").AS("age_min"),
				qx.MAX("age").AS("age_max"),
			),
			want: qaggReferenceUngroupedAge(qaggDefaultRows(), []string{"age_count", "age_sum", "age_avg", "age_min", "age_max"}),
		},
	}
	type retainedResult struct {
		got  Result
		want Result
	}
	retained := make([]retainedResult, 0, len(cases)*8)

	for iter := 0; iter < 8; iter++ {
		for i := range cases {
			tc := cases[i]
			prepared, err := Prepare(tc.q, db.rt, db.rt.IndexedByName)
			if err != nil {
				t.Fatalf("%s Prepare: %v", tc.name, err)
			}
			view := db.view()
			got, err := Execute(view, db.snap, prepared)
			db.exec.ReleaseView(view)
			prepared.Release()
			if err != nil {
				t.Fatalf("%s Execute: %v", tc.name, err)
			}

			requireQaggResultsEqualUnordered(t, got, tc.want)
			retained = append(retained, retainedResult{got: got, want: tc.want})
			for j := range retained {
				requireQaggResultsEqualUnordered(t, retained[j].got, retained[j].want)
			}
		}
	}
}

func TestRowCountAggregateEqualsCountForRepresentativeFilters(t *testing.T) {
	db := newQaggTestDB(t, nil)
	tests := []struct {
		name  string
		exprs []qx.Expr
	}{
		{name: "all"},
		{name: "scalar_eq", exprs: []qx.Expr{qx.EQ("country", "NL")}},
		{name: "range", exprs: []qx.Expr{qx.GTE("age", 30), qx.LT("age", 55)}},
		{name: "or", exprs: []qx.Expr{qx.OR(qx.EQ("country", "NL"), qx.HASANY("tags", []string{"ops"}))}},
	}

	view := db.view()
	defer db.exec.ReleaseView(view)
	for i := range tests {
		tc := tests[i]
		countQuery, err := PrepareCount(db.rt.IndexedByName, tc.exprs...)
		if err != nil {
			t.Fatalf("%s PrepareCount: %v", tc.name, err)
		}
		want, err := Count(view, countQuery, false)
		countQuery.Release()
		if err != nil {
			t.Fatalf("%s Count: %v", tc.name, err)
		}

		prepared, err := Prepare(qx.Query(tc.exprs...).Metrics(qx.ROWCOUNT().AS("rows")), db.rt, db.rt.IndexedByName)
		if err != nil {
			t.Fatalf("%s Prepare aggregate: %v", tc.name, err)
		}
		result, err := Execute(view, db.snap, prepared)
		prepared.Release()
		if err != nil {
			t.Fatalf("%s Execute: %v", tc.name, err)
		}
		if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
			t.Fatalf("%s result shape=%#v", tc.name, result.Rows)
		}
		requireQaggUint(t, result.Rows[0][0], want)
	}
}

func TestDistinctNullHandlingIsPinned(t *testing.T) {
	db := newQaggTestDB(t, nil)

	prepared, err := Prepare(qx.Aggregate(qx.DISTINCT("segment").AS("segment")), db.rt, db.rt.IndexedByName)
	if err != nil {
		t.Fatalf("Prepare distinct: %v", err)
	}
	view := db.view()
	result, err := Execute(view, db.snap, prepared)
	prepared.Release()
	if err != nil {
		db.exec.ReleaseView(view)
		t.Fatalf("Execute distinct: %v", err)
	}

	seen := make(map[string]bool, len(result.Rows))
	seenNull := false
	for i := range result.Rows {
		if result.Rows[i][0].Kind() == ValueKindNone {
			seenNull = true
			continue
		}
		seen[mustQaggString(t, result.Rows[i][0])] = true
	}
	if !seenNull || !seen["core"] || !seen["edge"] || len(seen) != 2 {
		db.exec.ReleaseView(view)
		t.Fatalf("DISTINCT segment rows=%#v", result.Rows)
	}

	prepared, err = Prepare(qx.Aggregate(qx.COUNT(qx.DISTINCT("segment")).AS("segments")), db.rt, db.rt.IndexedByName)
	if err != nil {
		db.exec.ReleaseView(view)
		t.Fatalf("Prepare count distinct: %v", err)
	}
	result, err = Execute(view, db.snap, prepared)
	db.exec.ReleaseView(view)
	prepared.Release()
	if err != nil {
		t.Fatalf("Execute count distinct: %v", err)
	}
	requireQaggUint(t, result.Rows[0][0], 2)
}

func TestCountDistinctUniqueFieldUsesNonNullFilterCardinality(t *testing.T) {
	rt, err := schema.Compile(reflect.TypeFor[qaggCountDistinctUniqueRec](), schema.Config{})
	if err != nil {
		t.Fatalf("schema.Compile: %v", err)
	}
	rows := []qaggCountDistinctUniqueRec{
		{Keep: true, Value: qaggI64(10)},
		{Keep: true, Value: qaggI64(20)},
		{Keep: true},
		{Keep: false, Value: qaggI64(30)},
	}
	entries := make([]snapshot.BatchEntry, len(rows))
	for i := range rows {
		entries[i] = snapshot.BatchEntry{ID: uint64(i + 1), New: unsafe.Pointer(&rows[i])}
	}
	snap := snapshot.Build(1, nil, rt, snapshot.CacheConfig{}, nil, entries)
	execRuntime := qexec.NewRuntime(qexec.Config{Schema: rt})
	view := execRuntime.AcquireView(snap)
	defer execRuntime.ReleaseView(view)

	prepared, err := Prepare(qx.Query(qx.EQ("keep", true)).Metrics(qx.COUNT(qx.DISTINCT("value")).AS("values")), rt, rt.IndexedByName)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer prepared.Release()
	result, err := Execute(view, snap, prepared)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	requireQaggUint(t, result.Rows[0][0], 2)
}

func TestUngroupedOrdinarySharedFieldMetricsMatchReference(t *testing.T) {
	db := newQaggTestDB(t, nil)
	prepared, err := Prepare(
		qx.Query(qx.GTE("age", 30)).Metrics(
			qx.COUNT("age").AS("age_count"),
			qx.SUM("age").AS("age_sum"),
			qx.AVG("age").AS("age_avg"),
			qx.MIN("age").AS("age_min"),
			qx.MAX("age").AS("age_max"),
		),
		db.rt,
		db.rt.IndexedByName,
	)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer prepared.Release()

	view := db.view()
	result, err := Execute(view, db.snap, prepared)
	db.exec.ReleaseView(view)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	requireQaggResultsEqualUnordered(t, result, qaggReferenceUngroupedAge(qaggDefaultRows(), []string{"age_count", "age_sum", "age_avg", "age_min", "age_max"}))
}

func TestUngroupedMeasureMetricsMatchReference(t *testing.T) {
	tests := []struct {
		name string
		q    *qx.QX
		keep int
	}{
		{
			name: "dense",
			q: qx.Aggregate(
				qx.COUNT("amount").AS("amount_count"),
				qx.SUM("amount").AS("amount_sum"),
				qx.AVG("amount").AS("amount_avg"),
				qx.MIN("amount").AS("amount_min"),
				qx.MAX("amount").AS("amount_max"),
			),
		},
		{
			name: "sparse",
			q: qx.Query(qx.EQ("country", "NL")).Metrics(
				qx.COUNT("amount").AS("amount_count"),
				qx.SUM("amount").AS("amount_sum"),
				qx.AVG("amount").AS("amount_avg"),
				qx.MIN("amount").AS("amount_min"),
				qx.MAX("amount").AS("amount_max"),
			),
			keep: 1,
		},
		{
			name: "null_heavy",
			q: qx.Query(qx.EQ("active", false)).Metrics(
				qx.COUNT("amount").AS("amount_count"),
				qx.SUM("amount").AS("amount_sum"),
				qx.AVG("amount").AS("amount_avg"),
				qx.MIN("amount").AS("amount_min"),
				qx.MAX("amount").AS("amount_max"),
			),
			keep: 2,
		},
	}

	for i := range tests {
		tc := tests[i]
		db := newQaggTestDB(t, nil)
		prepared, err := Prepare(tc.q, db.rt, db.rt.IndexedByName)
		if err != nil {
			t.Fatalf("%s Prepare: %v", tc.name, err)
		}
		view := db.view()
		result, err := Execute(view, db.snap, prepared)
		db.exec.ReleaseView(view)
		prepared.Release()
		if err != nil {
			t.Fatalf("%s Execute: %v", tc.name, err)
		}
		requireQaggResultsEqualUnordered(t, result, qaggReferenceUngroupedAmount(qaggDefaultRows(), tc.keep, []string{"amount_count", "amount_sum", "amount_avg", "amount_min", "amount_max"}))
	}
}

func TestGroupedHybridMetricsMatchReference(t *testing.T) {
	db := newQaggTestDB(t, nil)
	prepared, err := Prepare(
		qx.Group("country").Metrics(
			qx.SUM("age").AS("age_sum"),
			qx.SUM("amount").AS("amount_sum"),
		),
		db.rt,
		db.rt.IndexedByName,
	)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer prepared.Release()

	view := db.view()
	result, err := Execute(view, db.snap, prepared)
	db.exec.ReleaseView(view)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	requireQaggResultsEqualUnordered(t, result, qaggReferenceGroupedAgeAmountSums(qaggDefaultRows(), []string{"country", "age_sum", "amount_sum"}))
}

func TestGroupedHavingOrderWindowMatchesReference(t *testing.T) {
	db := newQaggTestDB(t, nil)
	prepared, err := Prepare(
		qx.Query(qx.GTE("age", 30)).
			Group("country").
			Metrics(
				qx.ROWCOUNT().AS("rows"),
				qx.SUM("amount").AS("sum"),
			).
			Having(qx.GTE(qx.OUT("sum"), int64(20))).
			SortOut("sum", qx.DESC).
			Offset(1).
			Limit(2),
		db.rt,
		db.rt.IndexedByName,
	)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer prepared.Release()

	view := db.view()
	result, err := Execute(view, db.snap, prepared)
	db.exec.ReleaseView(view)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	requireQaggResultsEqualOrdered(t, result, qaggReferenceGroupedHavingOrderWindow(qaggDefaultRows()))
}

func TestAggregateMetamorphicCounts(t *testing.T) {
	db := newQaggTestDB(t, nil)
	view := db.view()
	defer db.exec.ReleaseView(view)

	grouped, err := Prepare(qx.Query(qx.GTE("age", 30)).Group("country").Metrics(qx.ROWCOUNT().AS("rows")), db.rt, db.rt.IndexedByName)
	if err != nil {
		t.Fatalf("Prepare grouped: %v", err)
	}
	groupedResult, err := Execute(view, db.snap, grouped)
	grouped.Release()
	if err != nil {
		t.Fatalf("Execute grouped: %v", err)
	}
	var groupedSum uint64
	for i := range groupedResult.Rows {
		v, ok := groupedResult.Rows[i][1].Uint()
		if !ok {
			t.Fatalf("group row count missing: %#v", groupedResult.Rows[i])
		}
		groupedSum += v
	}

	ungrouped, err := Prepare(qx.Query(qx.GTE("age", 30)).Metrics(qx.ROWCOUNT().AS("rows")), db.rt, db.rt.IndexedByName)
	if err != nil {
		t.Fatalf("Prepare ungrouped: %v", err)
	}
	ungroupedResult, err := Execute(view, db.snap, ungrouped)
	ungrouped.Release()
	if err != nil {
		t.Fatalf("Execute ungrouped: %v", err)
	}
	want, ok := ungroupedResult.Rows[0][0].Uint()
	if !ok {
		t.Fatalf("ungrouped row count missing: %#v", ungroupedResult.Rows)
	}
	if groupedSum != want {
		t.Fatalf("sum grouped rows=%d, ungrouped rows=%d", groupedSum, want)
	}

	countSegment, err := Prepare(qx.Aggregate(qx.COUNT("segment").AS("segment_count")), db.rt, db.rt.IndexedByName)
	if err != nil {
		t.Fatalf("Prepare count segment: %v", err)
	}
	countSegmentResult, err := Execute(view, db.snap, countSegment)
	countSegment.Release()
	if err != nil {
		t.Fatalf("Execute count segment: %v", err)
	}
	requireQaggUint(t, countSegmentResult.Rows[0][0], 4)
}

func TestApplyAggregateOrderTopKMatchesFullSort(t *testing.T) {
	const rowCount = 2048
	rows := make([]Row, rowCount)
	for i := range rows {
		rows[i] = Row{
			Value{num: uint64((i * 37) % rowCount), any: ValueKindUint},
			Value{num: uint64(i), any: ValueKindUint},
		}
	}

	order := []aggregateOrder{{index: 0, desc: true}, {index: 1}}
	full := Result{Layout: []string{"score", "id"}, Rows: append([]Row(nil), rows...)}
	sort.Sort(aggregateRowSorter{rows: full.Rows, order: order})
	full = applyAggregateWindow(full, 17, 113)

	top := Result{Layout: []string{"score", "id"}, Rows: append([]Row(nil), rows...)}
	top = applyAggregateOrder(top, order, true, 17, 113)
	top = applyAggregateWindow(top, 17, 113)

	requireQaggLayout(t, top.Layout, full.Layout)
	if len(top.Rows) != len(full.Rows) {
		t.Fatalf("rows len=%d, want %d", len(top.Rows), len(full.Rows))
	}
	for i := range top.Rows {
		topScore, _ := top.Rows[i][0].Uint()
		fullScore, _ := full.Rows[i][0].Uint()
		topID, _ := top.Rows[i][1].Uint()
		fullID, _ := full.Rows[i][1].Uint()
		if topScore != fullScore || topID != fullID {
			t.Fatalf("row[%d]=%v, want %v", i, top.Rows[i], full.Rows[i])
		}
	}
}

func TestApplyAggregateOrderTiedOrderMatchesFullSort(t *testing.T) {
	rows := []Row{
		{
			Value{num: 4, any: ValueKindUint},
			Value{num: 0, any: ValueKindUint},
		},
		{
			Value{num: 4, any: ValueKindUint},
			Value{num: 1, any: ValueKindUint},
		},
		{
			Value{num: 3, any: ValueKindUint},
			Value{num: 2, any: ValueKindUint},
		},
	}
	for i := 3; i < 64; i++ {
		rows = append(rows, Row{
			Value{num: uint64(i + 5), any: ValueKindUint},
			Value{num: uint64(i), any: ValueKindUint},
		})
	}

	order := []aggregateOrder{{index: 0}}
	full := Result{Layout: []string{"score", "id"}, Rows: append([]Row(nil), rows...)}
	sort.Sort(aggregateRowSorter{rows: full.Rows, order: order})
	full = applyAggregateWindow(full, 0, 2)

	got := Result{Layout: []string{"score", "id"}, Rows: append([]Row(nil), rows...)}
	got = applyAggregateOrder(got, order, false, 0, 2)
	got = applyAggregateWindow(got, 0, 2)

	if len(got.Rows) != len(full.Rows) {
		t.Fatalf("rows len=%d, want %d", len(got.Rows), len(full.Rows))
	}
	for i := range got.Rows {
		gotID, _ := got.Rows[i][1].Uint()
		fullID, _ := full.Rows[i][1].Uint()
		if gotID != fullID {
			t.Fatalf("row[%d]=%v, want %v", i, got.Rows[i], full.Rows[i])
		}
	}
}

func TestPrepareAggregateOrderUniqueRequiresGroupCoverage(t *testing.T) {
	db := newQaggTestDB(t, nil)

	metricOrder, err := Prepare(
		qx.Group("country", "segment").
			Metrics(qx.ROWCOUNT().AS("rows")).
			SortOut("rows").
			Limit(2),
		db.rt,
		db.rt.IndexedByName,
	)
	if err != nil {
		t.Fatalf("Prepare metric order: %v", err)
	}
	if metricOrder.orderUnique {
		t.Fatalf("metric order marked unique")
	}
	metricOrder.Release()

	groupOrder, err := Prepare(
		qx.Group("country", "segment").
			Metrics(qx.ROWCOUNT().AS("rows")).
			SortOut("country").
			SortOut("segment").
			Limit(2),
		db.rt,
		db.rt.IndexedByName,
	)
	if err != nil {
		t.Fatalf("Prepare group order: %v", err)
	}
	if !groupOrder.orderUnique {
		t.Fatalf("group order not marked unique")
	}
	groupOrder.Release()
}

func qaggReferenceUngroupedAge(rows []qaggTestRec, layout []string) Result {
	count := uint64(0)
	sum := int64(0)
	minAge := int64(0)
	maxAge := int64(0)
	for i := range rows {
		if rows[i].Age < 30 {
			continue
		}
		age := int64(rows[i].Age)
		if count == 0 {
			minAge = age
			maxAge = age
		} else {
			if age < minAge {
				minAge = age
			}
			if age > maxAge {
				maxAge = age
			}
		}
		count++
		sum += age
	}
	return Result{Layout: layout, Rows: []Row{{
		Value{num: count, any: ValueKindUint},
		Value{num: uint64(sum), any: ValueKindInt},
		Value{num: math.Float64bits(float64(sum) / float64(count)), any: ValueKindFloat},
		Value{num: uint64(minAge), any: ValueKindInt},
		Value{num: uint64(maxAge), any: ValueKindInt},
	}}}
}

func qaggReferenceUngroupedAmount(rows []qaggTestRec, keep int, layout []string) Result {
	count := uint64(0)
	sum := int64(0)
	minAmount := int64(0)
	maxAmount := int64(0)
	for i := range rows {
		if keep == 1 && rows[i].Country != "NL" {
			continue
		}
		if keep == 2 && rows[i].Active {
			continue
		}
		if rows[i].Amount == nil {
			continue
		}
		amount := *rows[i].Amount
		if count == 0 {
			minAmount = amount
			maxAmount = amount
		} else {
			if amount < minAmount {
				minAmount = amount
			}
			if amount > maxAmount {
				maxAmount = amount
			}
		}
		count++
		sum += amount
	}
	return Result{Layout: layout, Rows: []Row{{
		Value{num: count, any: ValueKindUint},
		Value{num: uint64(sum), any: ValueKindInt},
		Value{num: math.Float64bits(float64(sum) / float64(count)), any: ValueKindFloat},
		Value{num: uint64(minAmount), any: ValueKindInt},
		Value{num: uint64(maxAmount), any: ValueKindInt},
	}}}
}

func qaggReferenceGroupedAgeSum(rows []qaggTestRec, layout []string) Result {
	type group struct {
		sum int64
	}
	groups := make(map[string]group, len(rows))
	for i := range rows {
		g := groups[rows[i].Country]
		g.sum += int64(rows[i].Age)
		groups[rows[i].Country] = g
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := Result{Layout: layout, Rows: make([]Row, len(keys))}
	for i := range keys {
		g := groups[keys[i]]
		out.Rows[i] = Row{valueFromSafeString(keys[i]), Value{num: uint64(g.sum), any: ValueKindInt}}
	}
	return out
}

func qaggReferenceGroupedAgeAmountSums(rows []qaggTestRec, layout []string) Result {
	type group struct {
		ageSum    int64
		amountSum int64
	}
	groups := make(map[string]group, len(rows))
	for i := range rows {
		g := groups[rows[i].Country]
		g.ageSum += int64(rows[i].Age)
		if rows[i].Amount != nil {
			g.amountSum += *rows[i].Amount
		}
		groups[rows[i].Country] = g
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := Result{Layout: layout, Rows: make([]Row, len(keys))}
	for i := range keys {
		g := groups[keys[i]]
		out.Rows[i] = Row{
			valueFromSafeString(keys[i]),
			Value{num: uint64(g.ageSum), any: ValueKindInt},
			Value{num: uint64(g.amountSum), any: ValueKindInt},
		}
	}
	return out
}

func qaggReferenceGroupedHavingOrderWindow(rows []qaggTestRec) Result {
	type group struct {
		rows uint64
		sum  int64
	}
	groups := make(map[string]group, len(rows))
	for i := range rows {
		if rows[i].Age < 30 {
			continue
		}
		g := groups[rows[i].Country]
		g.rows++
		if rows[i].Amount != nil {
			g.sum += *rows[i].Amount
		}
		groups[rows[i].Country] = g
	}
	out := Result{Layout: []string{"country", "rows", "sum"}}
	for key, g := range groups {
		if g.sum < 20 {
			continue
		}
		out.Rows = append(out.Rows, Row{
			valueFromSafeString(key),
			Value{num: g.rows, any: ValueKindUint},
			Value{num: uint64(g.sum), any: ValueKindInt},
		})
	}
	sort.Slice(out.Rows, func(i, j int) bool {
		left, _ := out.Rows[i][2].Int()
		right, _ := out.Rows[j][2].Int()
		if left == right {
			leftCountry, _ := out.Rows[i][0].String()
			rightCountry, _ := out.Rows[j][0].String()
			return leftCountry < rightCountry
		}
		return left > right
	})
	return Result{Layout: out.Layout, Rows: out.Rows[1:3]}
}

func qaggSparseRows() ([]qaggTestRec, []uint64) {
	return []qaggTestRec{
		{Country: "NL", Segment: qaggString("core"), Active: true, Age: 25, Big: math.MaxInt64, Tags: []string{"go"}, Amount: qaggI64(10)},
		{Country: "DE", Segment: qaggString("edge"), Active: true, Age: 31, Big: 1, Tags: []string{"ops"}, Amount: qaggI64(20)},
		{Country: "NL", Active: false, Age: 42, Tags: []string{"db"}},
		{Country: "US", Segment: qaggString("core"), Active: true, Age: 50, Tags: []string{"go"}, Amount: qaggI64(40)},
	}, []uint64{1, 20_000_000, 40_000_000, 60_000_000}
}

func newQaggSparseIDTestDB(t testing.TB, traceSink ...func(rbitrace.Event)) *qaggTestDB {
	t.Helper()

	rt, err := schema.Compile(reflect.TypeFor[qaggTestRec](), schema.Config{})
	if err != nil {
		t.Fatalf("schema.Compile: %v", err)
	}
	var sink func(rbitrace.Event)
	if len(traceSink) > 0 {
		sink = traceSink[0]
	}
	exec := qexec.NewRuntime(qexec.Config{
		Schema:           rt,
		TraceSink:        sink,
		TraceSampleEvery: 1,
	})
	rows, ids := qaggSparseRows()
	entries := make([]snapshot.BatchEntry, len(rows))
	for i := range rows {
		entries[i] = snapshot.BatchEntry{ID: ids[i], New: unsafe.Pointer(&rows[i])}
	}
	snap := snapshot.Build(1, nil, rt, snapshot.CacheConfig{}, nil, entries)
	return &qaggTestDB{rt: rt, exec: exec, snap: snap}
}

func requireQaggResultsEqualUnordered(t *testing.T, left Result, right Result) {
	t.Helper()
	requireQaggLayout(t, left.Layout, right.Layout)
	leftRows := qaggCanonicalRows(left.Rows)
	rightRows := qaggCanonicalRows(right.Rows)
	if len(leftRows) != len(rightRows) {
		t.Fatalf("row count=%d, want %d; left=%v right=%v", len(leftRows), len(rightRows), leftRows, rightRows)
	}
	for i := range leftRows {
		if leftRows[i] != rightRows[i] {
			t.Fatalf("row[%d]=%q, want %q; left=%v right=%v", i, leftRows[i], rightRows[i], leftRows, rightRows)
		}
	}
}

func requireQaggResultsEqualOrdered(t *testing.T, left Result, right Result) {
	t.Helper()
	requireQaggLayout(t, left.Layout, right.Layout)
	if len(left.Rows) != len(right.Rows) {
		t.Fatalf("row count=%d, want %d; left=%v right=%v", len(left.Rows), len(right.Rows), left.Rows, right.Rows)
	}
	for i := range left.Rows {
		leftRow := qaggCanonicalRow(left.Rows[i])
		rightRow := qaggCanonicalRow(right.Rows[i])
		if leftRow != rightRow {
			t.Fatalf("row[%d]=%q, want %q; left=%v right=%v", i, leftRow, rightRow, left.Rows, right.Rows)
		}
	}
}

func qaggCanonicalRows(rows []Row) []string {
	out := make([]string, len(rows))
	for i := range rows {
		out[i] = qaggCanonicalRow(rows[i])
	}
	sort.Strings(out)
	return out
}

func qaggCanonicalRow(row Row) string {
	var b strings.Builder
	for j := range row {
		if j > 0 {
			b.WriteByte('|')
		}
		v := row[j]
		b.WriteString(strconvValueKind(v.Kind()))
		b.WriteByte(':')
		if s, ok := v.ToString(); ok {
			b.WriteString(s)
		}
	}
	return b.String()
}

func strconvValueKind(kind ValueKind) string {
	switch kind {
	case ValueKindNone:
		return "none"
	case ValueKindBool:
		return "bool"
	case ValueKindInt:
		return "int"
	case ValueKindUint:
		return "uint"
	case ValueKindFloat:
		return "float"
	case ValueKindString:
		return "string"
	default:
		return "any"
	}
}
