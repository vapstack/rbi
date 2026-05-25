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
)

type qaggGroupedOrdinaryMapRec struct {
	Group  int   `db:"group"  rbi:"index"`
	Filter int   `db:"filter" rbi:"index"`
	Value  int64 `db:"value"  rbi:"index"`
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

func TestGroupedSparseHighIDSelectsRecursiveAndMatchesRecursiveRoute(t *testing.T) {
	db := newQaggSparseIDTestDB(t)
	prepared, err := Prepare(qx.Group("country").Metrics(qx.SUM("age").AS("sum")), db.rt)
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
	recursive, err := exec.executeGroupedRecursiveAggregate(prepared, ids)
	if err != nil {
		t.Fatalf("recursive: %v", err)
	}
	requireQaggResultsEqualUnordered(t, selected, recursive)
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
	snap := snapshot.BuildPrepared(1, nil, rt, snapshot.CacheConfig{}, nil, nil, entries)
	view := execRuntime.AcquireView(snap)
	defer execRuntime.ReleaseView(view)

	prepared, err := Prepare(
		qx.Query(qx.EQ("filter", 0)).
			Group("group").
			Metrics(qx.ROWCOUNT().AS("rows"), qx.SUM("value").AS("sum")),
		rt,
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
		prepared, err := Prepare(tc.q, db.rt)
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
		countQuery, err := PrepareCount(db.rt, tc.exprs...)
		if err != nil {
			t.Fatalf("%s PrepareCount: %v", tc.name, err)
		}
		want, err := Count(view, countQuery, false)
		countQuery.Release()
		if err != nil {
			t.Fatalf("%s Count: %v", tc.name, err)
		}

		prepared, err := Prepare(qx.Query(tc.exprs...).Metrics(qx.ROWCOUNT().AS("rows")), db.rt)
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

	prepared, err := Prepare(qx.Aggregate(qx.DISTINCT("segment").AS("segment")), db.rt)
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

	prepared, err = Prepare(qx.Aggregate(qx.COUNT(qx.DISTINCT("segment")).AS("segments")), db.rt)
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
	snap := snapshot.BuildPrepared(1, nil, rt, snapshot.CacheConfig{}, nil, nil, entries)
	execRuntime := qexec.NewRuntime(qexec.Config{Schema: rt})
	view := execRuntime.AcquireView(snap)
	defer execRuntime.ReleaseView(view)

	prepared, err := Prepare(qx.Query(qx.EQ("keep", true)).Metrics(qx.COUNT(qx.DISTINCT("value")).AS("values")), rt)
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

func TestGroupedHybridMetricsMatchReference(t *testing.T) {
	db := newQaggTestDB(t, nil)
	prepared, err := Prepare(
		qx.Group("country").Metrics(
			qx.SUM("age").AS("age_sum"),
			qx.SUM("amount").AS("amount_sum"),
		),
		db.rt,
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

	want := map[string][2]int64{
		"DE": {68, 35},
		"NL": {67, 10},
		"PL": {60, 50},
		"US": {50, 40},
	}
	if len(result.Rows) != len(want) {
		t.Fatalf("rows=%d, want %d; rows=%#v", len(result.Rows), len(want), result.Rows)
	}
	for i := range result.Rows {
		country := mustQaggString(t, result.Rows[i][0])
		expected, ok := want[country]
		if !ok {
			t.Fatalf("unexpected country row=%#v", result.Rows[i])
		}
		requireQaggInt(t, result.Rows[i][1], expected[0])
		requireQaggInt(t, result.Rows[i][2], expected[1])
	}
}

func TestAggregateMetamorphicCounts(t *testing.T) {
	db := newQaggTestDB(t, nil)
	view := db.view()
	defer db.exec.ReleaseView(view)

	grouped, err := Prepare(qx.Query(qx.GTE("age", 30)).Group("country").Metrics(qx.ROWCOUNT().AS("rows")), db.rt)
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

	ungrouped, err := Prepare(qx.Query(qx.GTE("age", 30)).Metrics(qx.ROWCOUNT().AS("rows")), db.rt)
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

	countSegment, err := Prepare(qx.Aggregate(qx.COUNT("segment").AS("segment_count")), db.rt)
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
	)
	if err != nil {
		t.Fatalf("Prepare group order: %v", err)
	}
	if !groupOrder.orderUnique {
		t.Fatalf("group order not marked unique")
	}
	groupOrder.Release()
}

func newQaggSparseIDTestDB(t testing.TB, traceSink ...func(qexec.TraceEvent)) *qaggTestDB {
	t.Helper()

	rt, err := schema.Compile(reflect.TypeFor[qaggTestRec](), schema.Config{})
	if err != nil {
		t.Fatalf("schema.Compile: %v", err)
	}
	var sink func(qexec.TraceEvent)
	if len(traceSink) > 0 {
		sink = traceSink[0]
	}
	exec := qexec.NewRuntime(qexec.Config{
		Schema:           rt,
		TraceSink:        sink,
		TraceSampleEvery: 1,
	})
	rows := []qaggTestRec{
		{Country: "NL", Segment: qaggString("core"), Active: true, Age: 25, Big: math.MaxInt64, Tags: []string{"go"}, Amount: qaggI64(10)},
		{Country: "DE", Segment: qaggString("edge"), Active: true, Age: 31, Big: 1, Tags: []string{"ops"}, Amount: qaggI64(20)},
		{Country: "NL", Active: false, Age: 42, Tags: []string{"db"}},
		{Country: "US", Segment: qaggString("core"), Active: true, Age: 50, Tags: []string{"go"}, Amount: qaggI64(40)},
	}
	ids := [...]uint64{1, 20_000_000, 40_000_000, 60_000_000}
	entries := make([]snapshot.BatchEntry, len(rows))
	for i := range rows {
		entries[i] = snapshot.BatchEntry{ID: ids[i], New: unsafe.Pointer(&rows[i])}
	}
	snap := snapshot.BuildPrepared(1, nil, rt, snapshot.CacheConfig{}, nil, nil, entries)
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

func qaggCanonicalRows(rows []Row) []string {
	out := make([]string, len(rows))
	for i := range rows {
		var b strings.Builder
		for j := range rows[i] {
			if j > 0 {
				b.WriteByte('|')
			}
			v := rows[i][j]
			b.WriteString(strconvValueKind(v.Kind()))
			b.WriteByte(':')
			if s, ok := v.ToString(); ok {
				b.WriteString(s)
			}
		}
		out[i] = b.String()
	}
	sort.Strings(out)
	return out
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
