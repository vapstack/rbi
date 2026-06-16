package qagg

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/qexec"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/rbitrace"
)

type qaggTestRec struct {
	Country string   `db:"country" rbi:"index"`
	Segment *string  `db:"segment" rbi:"index"`
	Active  bool     `db:"active"  rbi:"index"`
	Age     int      `db:"age"     rbi:"index"`
	Big     int64    `db:"big"     rbi:"index"`
	Tags    []string `db:"tags"    rbi:"index"`
	Amount  *int64   `db:"amount"  rbi:"measure"`
}

type qaggPinnedSnapshotRec struct {
	Amount int64 `db:"amount" rbi:"measure"`
}

type qaggTestDB struct {
	rt   *schema.Schema
	exec *qexec.Runtime
	snap *snapshot.View
}

func newQaggTestDB(t testing.TB, traceSink func(rbitrace.Event)) *qaggTestDB {
	t.Helper()

	rt, err := schema.Compile(reflect.TypeFor[qaggTestRec](), schema.Config{})
	if err != nil {
		t.Fatalf("schema.Compile: %v", err)
	}
	exec := qexec.NewRuntime(qexec.Config{
		Schema:           rt,
		TraceSink:        traceSink,
		TraceSampleEvery: 1,
	})

	rows := qaggDefaultRows()
	entries := make([]snapshot.BatchEntry, len(rows))
	for i := range rows {
		entries[i] = snapshot.BatchEntry{ID: uint64(i + 1), New: unsafe.Pointer(&rows[i])}
	}
	snap := snapshot.Build(1, nil, rt, snapshot.CacheConfig{
		MatPredMaxEntries: 32,
		MatPredMaxCard:    64 << 10,
	}, nil, entries)

	return &qaggTestDB{rt: rt, exec: exec, snap: snap}
}

func newQaggStringKeyTestDB(t testing.TB) *qaggTestDB {
	t.Helper()

	rt, err := schema.Compile(reflect.TypeFor[qaggTestRec](), schema.Config{})
	if err != nil {
		t.Fatalf("schema.Compile: %v", err)
	}
	exec := qexec.NewRuntime(qexec.Config{
		Schema:  rt,
		StrKey:  true,
		KeyMode: qexec.KeyModeString,
	})

	rows := qaggDefaultRows()
	entries := make([]snapshot.BatchEntry, len(rows))
	keyDeltas := make([]snapshot.KeyDelta, len(rows))
	keys := [...]string{"alpha", "beta", "alphabet", "delta", "zeta", "betamax"}
	for i := range rows {
		id := uint64(i + 1)
		entries[i] = snapshot.BatchEntry{ID: id, New: unsafe.Pointer(&rows[i])}
		keyDeltas[i] = snapshot.KeyDelta{ID: id, Key: keys[i], Add: true}
	}
	snap := snapshot.BuildWithKeyDeltas(1, nil, rt, snapshot.CacheConfig{
		MatPredMaxEntries: 32,
		MatPredMaxCard:    64 << 10,
	}, nil, entries, keyDeltas)

	return &qaggTestDB{rt: rt, exec: exec, snap: snap}
}

func qaggDefaultRows() []qaggTestRec {
	return []qaggTestRec{
		{Country: "NL", Segment: qaggString("core"), Active: true, Age: 25, Big: math.MaxInt64, Tags: []string{"go", "db"}, Amount: qaggI64(10)},
		{Country: "DE", Segment: qaggString("edge"), Active: true, Age: 31, Big: 1, Tags: []string{"ops"}, Amount: qaggI64(20)},
		{Country: "NL", Active: false, Age: 42, Tags: []string{"db"}},
		{Country: "US", Segment: qaggString("core"), Active: true, Age: 50, Tags: []string{"go"}, Amount: qaggI64(40)},
		{Country: "PL", Active: false, Age: 60, Amount: qaggI64(50)},
		{Country: "DE", Segment: qaggString("edge"), Active: true, Age: 37, Tags: []string{"go", "ops"}, Amount: qaggI64(15)},
	}
}

func qaggI64(v int64) *int64 {
	return &v
}

func qaggString(v string) *string {
	return &v
}

func (db *qaggTestDB) view() *qexec.View {
	return db.exec.AcquireView(db.snap)
}

func TestCountDirectFastPathsReturnExactCardinality(t *testing.T) {
	db := newQaggTestDB(t, nil)
	tests := []struct {
		name  string
		exprs []qx.Expr
		want  uint64
	}{
		{name: "noop", want: 6},
		{name: "scalar_eq", exprs: []qx.Expr{qx.EQ("country", "NL")}, want: 2},
		{name: "scalar_not_eq", exprs: []qx.Expr{qx.NOT(qx.EQ("country", "NL"))}, want: 4},
		{name: "scalar_in", exprs: []qx.Expr{qx.IN("country", []string{"NL", "DE"})}, want: 4},
		{name: "slice_has_any", exprs: []qx.Expr{qx.HASANY("tags", []string{"go", "ops"})}, want: 4},
		{name: "slice_has_all", exprs: []qx.Expr{qx.HASALL("tags", []string{"go", "ops"})}, want: 1},
		{name: "and", exprs: []qx.Expr{qx.GTE("age", 30), qx.LT("age", 55), qx.EQ("active", true)}, want: 3},
		{name: "or", exprs: []qx.Expr{qx.OR(qx.EQ("country", "NL"), qx.HASANY("tags", []string{"ops"}))}, want: 4},
	}

	view := db.view()
	defer db.exec.ReleaseView(view)
	for i := range tests {
		prepared, err := PrepareCount(db.rt.IndexedByName, tests[i].exprs...)
		if err != nil {
			t.Fatalf("%s PrepareCount: %v", tests[i].name, err)
		}
		got, err := Count(view, prepared, false)
		prepared.Release()
		if err != nil {
			t.Fatalf("%s Count: %v", tests[i].name, err)
		}
		if got != tests[i].want {
			t.Fatalf("%s Count=%d, want %d", tests[i].name, got, tests[i].want)
		}
	}
}

func TestCountDirectTraceFinishesCountEvent(t *testing.T) {
	var events []rbitrace.Event
	db := newQaggTestDB(t, func(ev rbitrace.Event) {
		events = append(events, ev)
	})

	prepared, err := PrepareCount(db.rt.IndexedByName)
	if err != nil {
		t.Fatalf("PrepareCount: %v", err)
	}
	defer prepared.Release()

	view := db.view()
	got, err := Count(view, prepared, true)
	db.exec.ReleaseView(view)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if got != 6 {
		t.Fatalf("Count=%d, want 6", got)
	}
	if len(events) != 1 {
		t.Fatalf("trace events=%d, want 1", len(events))
	}
	if events[0].Plan != rbitrace.PlanCountMaterialized || events[0].RowsReturned != 6 || events[0].RowsExamined != 6 {
		t.Fatalf("trace event=%+v", events[0])
	}
}

func TestExecuteGroupedMetricsHavingOrderAndWindow(t *testing.T) {
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

	requireQaggLayout(t, result.Layout, []string{"country", "rows", "sum"})
	if len(result.Rows) != 2 {
		t.Fatalf("rows len=%d, want 2; rows=%#v", len(result.Rows), result.Rows)
	}
	requireQaggString(t, result.Rows[0][0], "US")
	requireQaggUint(t, result.Rows[0][1], 1)
	requireQaggInt(t, result.Rows[0][2], 40)
	requireQaggString(t, result.Rows[1][0], "DE")
	requireQaggUint(t, result.Rows[1][1], 2)
	requireQaggInt(t, result.Rows[1][2], 35)
}

func TestPrepareCountRejectsDisabledStringKey(t *testing.T) {
	db := newQaggTestDB(t, nil)
	prepared, err := PrepareCount(db.rt.IndexedByName, qx.EQ(schema.ReservedKeyFieldName, "alpha"))
	if prepared != nil {
		prepared.Release()
	}
	if err == nil || !strings.Contains(err.Error(), "no index for field") {
		t.Fatalf("PrepareCount err=%v, want no index rejection", err)
	}
}

func TestCountSupportsStringKeyFilter(t *testing.T) {
	db := newQaggStringKeyTestDB(t)
	prepared, err := PrepareCount(db.exec, qx.PREFIX(schema.ReservedKeyFieldName, "alpha"))
	if err != nil {
		t.Fatalf("PrepareCount: %v", err)
	}
	defer prepared.Release()

	view := db.view()
	got, err := Count(view, prepared, false)
	db.exec.ReleaseView(view)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if got != 2 {
		t.Fatalf("Count=%d, want 2", got)
	}
}

func TestExecuteAggregateSupportsStringKeyFilter(t *testing.T) {
	db := newQaggStringKeyTestDB(t)
	prepared, err := Prepare(
		qx.Query(qx.PREFIX(schema.ReservedKeyFieldName, "beta")).
			Group("country").
			Metrics(
				qx.ROWCOUNT().AS("rows"),
				qx.SUM("amount").AS("sum"),
			),
		db.rt,
		db.exec,
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
	requireQaggLayout(t, result.Layout, []string{"country", "rows", "sum"})
	if len(result.Rows) != 1 {
		t.Fatalf("rows len=%d, want 1; rows=%#v", len(result.Rows), result.Rows)
	}
	requireQaggString(t, result.Rows[0][0], "DE")
	requireQaggUint(t, result.Rows[0][1], 2)
	requireQaggInt(t, result.Rows[0][2], 35)
}

func TestExecuteRowCountUsesCountFilter(t *testing.T) {
	db := newQaggTestDB(t, nil)
	prepared, err := Prepare(
		qx.Query(qx.OR(qx.EQ("country", "NL"), qx.HASANY("tags", []string{"ops"}))).
			Metrics(qx.ROWCOUNT().AS("rows")),
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

	requireQaggLayout(t, result.Layout, []string{"rows"})
	if len(result.Rows) != 1 {
		t.Fatalf("rows len=%d, want 1", len(result.Rows))
	}
	requireQaggUint(t, result.Rows[0][0], 4)
}

func TestExecuteDistinctAndCountDistinct(t *testing.T) {
	db := newQaggTestDB(t, nil)

	prepared, err := Prepare(qx.Aggregate(qx.DISTINCT("country").AS("country")), db.rt, db.rt.IndexedByName)
	if err != nil {
		t.Fatalf("Prepare distinct: %v", err)
	}
	view := db.view()
	result, err := Execute(view, db.snap, prepared)
	db.exec.ReleaseView(view)
	prepared.Release()
	if err != nil {
		t.Fatalf("Execute distinct: %v", err)
	}
	requireQaggLayout(t, result.Layout, []string{"country"})
	gotCountries := make(map[string]bool, len(result.Rows))
	for i := range result.Rows {
		gotCountries[mustQaggString(t, result.Rows[i][0])] = true
	}
	for _, country := range []string{"DE", "NL", "PL", "US"} {
		if !gotCountries[country] {
			t.Fatalf("missing distinct country %q in %#v", country, result.Rows)
		}
	}
	if len(gotCountries) != 4 {
		t.Fatalf("distinct country count=%d, want 4; rows=%#v", len(gotCountries), result.Rows)
	}

	prepared, err = Prepare(
		qx.Query(qx.EQ("active", true)).Metrics(qx.COUNT(qx.DISTINCT("country")).AS("countries")),
		db.rt,
		db.rt.IndexedByName,
	)
	if err != nil {
		t.Fatalf("Prepare count distinct: %v", err)
	}
	view = db.view()
	result, err = Execute(view, db.snap, prepared)
	db.exec.ReleaseView(view)
	prepared.Release()
	if err != nil {
		t.Fatalf("Execute count distinct: %v", err)
	}
	requireQaggLayout(t, result.Layout, []string{"countries"})
	if len(result.Rows) != 1 {
		t.Fatalf("count distinct rows len=%d, want 1", len(result.Rows))
	}
	requireQaggUint(t, result.Rows[0][0], 3)
}

func TestExecuteDistinctWindow(t *testing.T) {
	db := newQaggTestDB(t, nil)

	prepared, err := Prepare(
		qx.Aggregate(qx.DISTINCT("country").AS("country")).
			Offset(1).
			Limit(2),
		db.rt,
		db.rt.IndexedByName,
	)
	if err != nil {
		t.Fatalf("Prepare distinct window: %v", err)
	}
	view := db.view()
	result, err := Execute(view, db.snap, prepared)
	db.exec.ReleaseView(view)
	prepared.Release()
	if err != nil {
		t.Fatalf("Execute distinct window: %v", err)
	}
	requireQaggLayout(t, result.Layout, []string{"country"})
	if len(result.Rows) != 2 {
		t.Fatalf("rows len=%d, want 2; rows=%#v", len(result.Rows), result.Rows)
	}
	requireQaggString(t, result.Rows[0][0], "NL")
	requireQaggString(t, result.Rows[1][0], "PL")
}

func TestExecuteDistinctOrderWindow(t *testing.T) {
	db := newQaggTestDB(t, nil)

	prepared, err := Prepare(
		qx.Aggregate(qx.DISTINCT("country").AS("country")).
			SortOut("country", qx.DESC).
			Offset(1).
			Limit(2),
		db.rt,
		db.rt.IndexedByName,
	)
	if err != nil {
		t.Fatalf("Prepare distinct order window: %v", err)
	}
	view := db.view()
	result, err := Execute(view, db.snap, prepared)
	db.exec.ReleaseView(view)
	prepared.Release()
	if err != nil {
		t.Fatalf("Execute distinct order window: %v", err)
	}
	requireQaggLayout(t, result.Layout, []string{"country"})
	if len(result.Rows) != 2 {
		t.Fatalf("rows len=%d, want 2; rows=%#v", len(result.Rows), result.Rows)
	}
	requireQaggString(t, result.Rows[0][0], "PL")
	requireQaggString(t, result.Rows[1][0], "NL")
}

func TestExecuteDistinctWindowCanReturnNilValue(t *testing.T) {
	db := newQaggTestDB(t, nil)

	prepared, err := Prepare(
		qx.Aggregate(qx.DISTINCT("segment").AS("segment")).
			Offset(2).
			Limit(1),
		db.rt,
		db.rt.IndexedByName,
	)
	if err != nil {
		t.Fatalf("Prepare distinct nil window: %v", err)
	}
	view := db.view()
	result, err := Execute(view, db.snap, prepared)
	db.exec.ReleaseView(view)
	prepared.Release()
	if err != nil {
		t.Fatalf("Execute distinct nil window: %v", err)
	}
	requireQaggLayout(t, result.Layout, []string{"segment"})
	if len(result.Rows) != 1 {
		t.Fatalf("rows len=%d, want 1; rows=%#v", len(result.Rows), result.Rows)
	}
	if result.Rows[0][0].Kind() != ValueKindNone {
		t.Fatalf("segment kind=%v, want nil; rows=%#v", result.Rows[0][0].Kind(), result.Rows)
	}
}

func TestExecuteNumericMetricsOverMeasureAndOrdinaryFields(t *testing.T) {
	db := newQaggTestDB(t, nil)
	prepared, err := Prepare(
		qx.Query(qx.EQ("active", true)).Metrics(
			qx.COUNT("amount").AS("amount_count"),
			qx.SUM("amount").AS("amount_sum"),
			qx.AVG("amount").AS("amount_avg"),
			qx.MIN("amount").AS("amount_min"),
			qx.MAX("amount").AS("amount_max"),
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
	requireQaggLayout(t, result.Layout, []string{
		"amount_count", "amount_sum", "amount_avg", "amount_min", "amount_max",
		"age_sum", "age_avg", "age_min", "age_max",
	})
	if len(result.Rows) != 1 {
		t.Fatalf("rows len=%d, want 1", len(result.Rows))
	}
	row := result.Rows[0]
	requireQaggUint(t, row[0], 4)
	requireQaggInt(t, row[1], 85)
	requireQaggFloat(t, row[2], 21.25)
	requireQaggInt(t, row[3], 10)
	requireQaggInt(t, row[4], 40)
	requireQaggInt(t, row[5], 143)
	requireQaggFloat(t, row[6], 35.75)
	requireQaggInt(t, row[7], 25)
	requireQaggInt(t, row[8], 50)
}

func TestExecuteEmptyMatchesReturnAggregateShapes(t *testing.T) {
	db := newQaggTestDB(t, nil)

	prepared, err := Prepare(
		qx.Query(qx.EQ("country", "missing")).Metrics(
			qx.COUNT("amount").AS("amount_count"),
			qx.SUM("amount").AS("amount_sum"),
			qx.AVG("amount").AS("amount_avg"),
			qx.MIN("amount").AS("amount_min"),
		),
		db.rt,
		db.rt.IndexedByName,
	)
	if err != nil {
		t.Fatalf("Prepare ungrouped: %v", err)
	}
	view := db.view()
	result, err := Execute(view, db.snap, prepared)
	db.exec.ReleaseView(view)
	prepared.Release()
	if err != nil {
		t.Fatalf("Execute ungrouped: %v", err)
	}
	requireQaggLayout(t, result.Layout, []string{"amount_count", "amount_sum", "amount_avg", "amount_min"})
	if len(result.Rows) != 1 {
		t.Fatalf("ungrouped empty rows len=%d, want 1", len(result.Rows))
	}
	requireQaggUint(t, result.Rows[0][0], 0)
	requireQaggInt(t, result.Rows[0][1], 0)
	requireQaggNone(t, result.Rows[0][2])
	requireQaggNone(t, result.Rows[0][3])

	prepared, err = Prepare(
		qx.Query(qx.EQ("country", "missing")).Group("country").Metrics(qx.ROWCOUNT().AS("rows")),
		db.rt,
		db.rt.IndexedByName,
	)
	if err != nil {
		t.Fatalf("Prepare grouped: %v", err)
	}
	view = db.view()
	result, err = Execute(view, db.snap, prepared)
	db.exec.ReleaseView(view)
	prepared.Release()
	if err != nil {
		t.Fatalf("Execute grouped: %v", err)
	}
	requireQaggLayout(t, result.Layout, []string{"country", "rows"})
	if len(result.Rows) != 0 {
		t.Fatalf("grouped empty rows len=%d, want 0; rows=%#v", len(result.Rows), result.Rows)
	}

	prepared, err = Prepare(
		qx.Query(qx.EQ("country", "missing")).Metrics(qx.DISTINCT("country").AS("country")),
		db.rt,
		db.rt.IndexedByName,
	)
	if err != nil {
		t.Fatalf("Prepare distinct: %v", err)
	}
	view = db.view()
	result, err = Execute(view, db.snap, prepared)
	db.exec.ReleaseView(view)
	prepared.Release()
	if err != nil {
		t.Fatalf("Execute distinct: %v", err)
	}
	requireQaggLayout(t, result.Layout, []string{"country"})
	if len(result.Rows) != 0 {
		t.Fatalf("distinct empty rows len=%d, want 0; rows=%#v", len(result.Rows), result.Rows)
	}

	prepared, err = Prepare(
		qx.Query(qx.EQ("country", "missing")).Metrics(qx.COUNT(qx.DISTINCT("country")).AS("countries")),
		db.rt,
		db.rt.IndexedByName,
	)
	if err != nil {
		t.Fatalf("Prepare count distinct: %v", err)
	}
	view = db.view()
	result, err = Execute(view, db.snap, prepared)
	db.exec.ReleaseView(view)
	prepared.Release()
	if err != nil {
		t.Fatalf("Execute count distinct: %v", err)
	}
	requireQaggLayout(t, result.Layout, []string{"countries"})
	if len(result.Rows) != 1 {
		t.Fatalf("count distinct empty rows len=%d, want 1", len(result.Rows))
	}
	requireQaggUint(t, result.Rows[0][0], 0)
}

func TestExecuteNullGroupProducesNoneKey(t *testing.T) {
	db := newQaggTestDB(t, nil)
	prepared, err := Prepare(qx.Group("segment").Metrics(qx.ROWCOUNT().AS("rows")), db.rt, db.rt.IndexedByName)
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
	requireQaggLayout(t, result.Layout, []string{"segment", "rows"})
	got := make(map[string]uint64, len(result.Rows))
	for i := range result.Rows {
		key := "<null>"
		if result.Rows[i][0].Kind() != ValueKindNone {
			key = mustQaggString(t, result.Rows[i][0])
		}
		rows, ok := result.Rows[i][1].Uint()
		if !ok {
			t.Fatalf("row count is not uint: %#v", result.Rows[i][1])
		}
		got[key] = rows
	}
	want := map[string]uint64{"core": 2, "edge": 2, "<null>": 2}
	for key, wantRows := range want {
		if got[key] != wantRows {
			t.Fatalf("group %q rows=%d, want %d; all=%v", key, got[key], wantRows, got)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("group count=%d, want %d; all=%v", len(got), len(want), got)
	}
}

func TestExecuteSignedSumOverflowReturnsError(t *testing.T) {
	db := newQaggTestDB(t, nil)
	prepared, err := Prepare(qx.Aggregate(qx.SUM("big").AS("sum")), db.rt, db.rt.IndexedByName)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer prepared.Release()

	view := db.view()
	_, err = Execute(view, db.snap, prepared)
	db.exec.ReleaseView(view)
	if err == nil || !strings.Contains(err.Error(), "integer SUM overflow") {
		t.Fatalf("Execute err=%v, want integer SUM overflow", err)
	}
}

func TestExecutePinnedSnapshotIsolation(t *testing.T) {
	rt, err := schema.Compile(reflect.TypeFor[qaggPinnedSnapshotRec](), schema.Config{})
	if err != nil {
		t.Fatalf("schema.Compile: %v", err)
	}
	exec := qexec.NewRuntime(qexec.Config{Schema: rt})

	oldRec := qaggPinnedSnapshotRec{Amount: 10}
	oldSnap := snapshot.Build(1, nil, rt, snapshot.CacheConfig{}, nil, []snapshot.BatchEntry{
		{ID: 1, New: unsafe.Pointer(&oldRec)},
	})
	newRec := qaggPinnedSnapshotRec{Amount: 99}
	newSnap := snapshot.Build(2, oldSnap, rt, snapshot.CacheConfig{}, nil, []snapshot.BatchEntry{
		{ID: 1, Old: unsafe.Pointer(&oldRec), New: unsafe.Pointer(&newRec)},
	})

	prepared, err := Prepare(qx.Aggregate(qx.SUM("amount").AS("sum")), rt, rt.IndexedByName)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer prepared.Release()

	view := exec.AcquireView(oldSnap)
	oldResult, err := Execute(view, oldSnap, prepared)
	exec.ReleaseView(view)
	if err != nil {
		t.Fatalf("Execute old snapshot: %v", err)
	}
	requireQaggInt(t, oldResult.Rows[0][0], 10)

	view = exec.AcquireView(newSnap)
	currentResult, err := Execute(view, newSnap, prepared)
	exec.ReleaseView(view)
	if err != nil {
		t.Fatalf("Execute current snapshot: %v", err)
	}
	requireQaggInt(t, currentResult.Rows[0][0], 99)
}

func TestPrepareRejectsUnsupportedAggregateShapes(t *testing.T) {
	db := newQaggTestDB(t, nil)
	tests := []struct {
		name string
		q    *qx.QX
		want string
	}{
		{
			name: "group_by_measure",
			q:    qx.Group("amount").Metrics(qx.ROWCOUNT()),
			want: `GROUP BY measure field "amount" is not supported`,
		},
		{
			name: "group_expression",
			q:    qx.GroupBy(qx.LOWER("country")).Metrics(qx.ROWCOUNT()),
			want: "GROUP BY supports only field references",
		},
		{
			name: "group_ref_args",
			q:    qx.GroupBy(qx.Expr{Kind: qx.KindREF, Name: "country", Args: []qx.Expr{qx.LIT("ignored")}}).Metrics(qx.ROWCOUNT()),
			want: `GROUP BY field reference "country" must not have arguments`,
		},
		{
			name: "metric_op_value",
			q: qx.Aggregate(qx.Expr{
				Kind:  qx.KindOP,
				Name:  qx.OpSUM,
				Value: true,
				Args:  []qx.Expr{qx.REF("amount")},
			}),
			want: `aggregate metric operation "sum" must not carry a value`,
		},
		{
			name: "metric_ref_args",
			q:    qx.Aggregate(qx.SUM(qx.Expr{Kind: qx.KindREF, Name: "amount", Args: []qx.Expr{qx.LIT("ignored")}})),
			want: `aggregate metric "sum" field reference "amount" must not have arguments`,
		},
		{
			name: "distinct_with_other_metric",
			q:    qx.Aggregate(qx.DISTINCT("country"), qx.ROWCOUNT()),
			want: "DISTINCT is supported only as a single ungrouped metric",
		},
		{
			name: "distinct_measure",
			q:    qx.Aggregate(qx.DISTINCT("amount")),
			want: `DISTINCT over measure field "amount" is not supported`,
		},
		{
			name: "sum_string",
			q:    qx.Aggregate(qx.SUM("country")),
			want: `SUM requires numeric field "country"`,
		},
		{
			name: "count_distinct_expression",
			q:    qx.Aggregate(qx.COUNT(qx.DISTINCT(qx.LOWER("country")))),
			want: `COUNT(DISTINCT) supports only direct field reference`,
		},
		{
			name: "having_unknown_output",
			q:    qx.Aggregate(qx.ROWCOUNT().AS("rows")).Having(qx.GT(qx.OUT("missing"), 1)),
			want: `unknown aggregate output "missing" in HAVING`,
		},
		{
			name: "having_empty_and",
			q:    qx.Aggregate(qx.ROWCOUNT().AS("rows")).Having(qx.AND()),
			want: "aggregate HAVING empty AND expression",
		},
		{
			name: "having_empty_or",
			q:    qx.Aggregate(qx.ROWCOUNT().AS("rows")).Having(qx.OR()),
			want: "aggregate HAVING empty OR expression",
		},
		{
			name: "having_op_value",
			q: qx.Aggregate(qx.ROWCOUNT().AS("rows")).Having(qx.Expr{
				Kind:  qx.KindOP,
				Name:  qx.OpGT,
				Value: true,
				Args:  []qx.Expr{qx.OUT("rows"), qx.LIT(1)},
			}),
			want: `aggregate HAVING operation "gt" must not carry a value`,
		},
		{
			name: "having_out_args",
			q:    qx.Aggregate(qx.ROWCOUNT().AS("rows")).Having(qx.GT(qx.Expr{Kind: qx.KindOUT, Name: "rows", Args: []qx.Expr{qx.LIT("ignored")}}, 1)),
			want: `aggregate HAVING output reference "rows" must not have arguments`,
		},
		{
			name: "having_lit_args",
			q:    qx.Aggregate(qx.ROWCOUNT().AS("rows")).Having(qx.GT(qx.OUT("rows"), qx.Expr{Kind: qx.KindLIT, Value: 1, Args: []qx.Expr{qx.LIT("ignored")}})),
			want: "aggregate HAVING literal must not have arguments",
		},
		{
			name: "order_unknown_output",
			q:    qx.Aggregate(qx.ROWCOUNT().AS("rows")).SortOut("missing"),
			want: `unknown aggregate output "missing" in ORDER`,
		},
		{
			name: "order_out_args",
			q:    qx.Aggregate(qx.ROWCOUNT().AS("rows")).SortBy(qx.Expr{Kind: qx.KindOUT, Name: "rows", Args: []qx.Expr{qx.LIT("ignored")}}),
			want: `aggregate ORDER output reference "rows" must not have arguments`,
		},
		{
			name: "projection",
			q:    qx.Aggregate(qx.ROWCOUNT()).Select("country"),
			want: "aggregate projection is not supported",
		},
		{
			name: "filter_measure",
			q:    qx.Query(qx.EQ("amount", int64(10))).Metrics(qx.ROWCOUNT()),
			want: "amount",
		},
		{
			name: "filter_reserved_key",
			q:    qx.Query(qx.EQ(schema.ReservedKeyFieldName, "alpha")).Metrics(qx.ROWCOUNT()),
			want: schema.ReservedKeyFieldName,
		},
		{
			name: "group_reserved_key",
			q:    qx.Group(schema.ReservedKeyFieldName).Metrics(qx.ROWCOUNT()),
			want: schema.ReservedKeyFieldName,
		},
		{
			name: "metric_reserved_key",
			q:    qx.Aggregate(qx.DISTINCT(schema.ReservedKeyFieldName)),
			want: schema.ReservedKeyFieldName,
		},
		{
			name: "having_reserved_key_ref",
			q:    qx.Aggregate(qx.ROWCOUNT().AS("rows")).Having(qx.EQ(qx.REF(schema.ReservedKeyFieldName), "alpha")),
			want: "OUT references",
		},
	}

	for i := range tests {
		prepared, err := Prepare(tests[i].q, db.rt, db.rt.IndexedByName)
		if prepared != nil {
			prepared.Release()
		}
		if err == nil || !strings.Contains(err.Error(), tests[i].want) {
			t.Fatalf("%s err=%v, want %q", tests[i].name, err, tests[i].want)
		}
	}
}

func requireQaggLayout(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("layout len=%d, want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("layout[%d]=%q, want %q; layout=%#v", i, got[i], want[i], got)
		}
	}
}

func requireQaggString(t *testing.T, v Value, want string) {
	t.Helper()
	got := mustQaggString(t, v)
	if got != want {
		t.Fatalf("string value=%q, want %q; kind=%v", got, want, v.Kind())
	}
}

func mustQaggString(t *testing.T, v Value) string {
	t.Helper()
	got, ok := v.String()
	if !ok {
		t.Fatalf("string value missing; kind=%v", v.Kind())
	}
	return got
}

func requireQaggUint(t *testing.T, v Value, want uint64) {
	t.Helper()
	got, ok := v.Uint()
	if !ok || got != want {
		t.Fatalf("uint value=(%d,%v), want %d; kind=%v", got, ok, want, v.Kind())
	}
}

func requireQaggInt(t *testing.T, v Value, want int64) {
	t.Helper()
	got, ok := v.Int()
	if !ok || got != want {
		t.Fatalf("int value=(%d,%v), want %d; kind=%v", got, ok, want, v.Kind())
	}
}

func requireQaggFloat(t *testing.T, v Value, want float64) {
	t.Helper()
	got, ok := v.Float()
	if !ok || got != want {
		t.Fatalf("float value=(%v,%v), want %v; kind=%v", got, ok, want, v.Kind())
	}
}

func requireQaggNone(t *testing.T, v Value) {
	t.Helper()
	if v.Kind() != ValueKindNone {
		t.Fatalf("value kind=%v, want none", v.Kind())
	}
}
