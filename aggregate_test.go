package rbi

import (
	"bytes"
	"log"
	"math"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vapstack/qx"
)

type aggregateTestRec struct {
	Country string `db:"country" rbi:"index"`
	Status  string `db:"status"  rbi:"index"`
	Name    string `db:"name"    rbi:"index"`
	Age     int    `db:"age"     rbi:"index"`
	Score   *int64 `db:"score"   rbi:"measure"`
}

type aggregateSumTypeRec struct {
	Group     string   `db:"group"         rbi:"index"`
	OrdinaryU uint64   `db:"ordinary_u"    rbi:"index"`
	OrdinaryF float64  `db:"ordinary_f"    rbi:"index"`
	MeasureU  *uint64  `db:"measure_u"     rbi:"measure"`
	MeasureF  *float64 `db:"measure_f"     rbi:"measure"`
}

type aggregatePlannerStatsBaseRec struct {
	Category string `db:"category"`
	Amount   int64  `db:"amount" rbi:"measure"`
}

type aggregatePlannerStatsNextRec struct {
	Category string `db:"category" rbi:"index"`
	Amount   int64  `db:"amount"   rbi:"measure"`
}

type aggregateNumericVI int

func (v aggregateNumericVI) IndexingValue() string {
	if v == 10 {
		return "010"
	}
	if v == 2 {
		return "002"
	}
	return "000"
}

type aggregateBoolVI bool

func (v aggregateBoolVI) IndexingValue() string {
	if v {
		return "Y"
	}
	return "N"
}

type aggregateVIRec struct {
	Code aggregateNumericVI `db:"code" rbi:"index"`
	Flag aggregateBoolVI    `db:"flag" rbi:"index"`
}

type aggregateWideMeasureRec struct {
	Status string `db:"status" rbi:"index"`
	Amount int64  `db:"amount" rbi:"measure"`
}

type aggregateTwoMeasureRec struct {
	Left  int64 `db:"left"  rbi:"measure"`
	Right int64 `db:"right" rbi:"measure"`
}

type aggregateNullGroupRec struct {
	Segment *string `db:"segment" rbi:"index"`
	Amount  *int64  `db:"amount"  rbi:"measure"`
}

type aggregateFloatRec struct {
	Group    string  `db:"group"    rbi:"index"`
	Ordinary float64 `db:"ordinary" rbi:"index"`
	Measure  float64 `db:"measure"  rbi:"measure"`
}

type aggregateOverflowRec struct {
	Ordinary int64  `db:"ordinary" rbi:"index"`
	Signed   int64  `db:"signed"   rbi:"measure"`
	Unsigned uint64 `db:"unsigned" rbi:"measure"`
}

type aggregateHavingPrecisionRec struct {
	Group    string `db:"group"    rbi:"index"`
	Signed   int64  `db:"signed"   rbi:"measure"`
	Unsigned uint64 `db:"unsigned" rbi:"measure"`
}

type aggregateSliceRec struct {
	Tags []string `db:"tags" rbi:"index"`
}

type aggregateModelRec struct {
	Country string `db:"country" rbi:"index"`
	Status  string `db:"status"  rbi:"index"`
	Age     int    `db:"age"     rbi:"index"`
	Amount  *int64 `db:"amount"  rbi:"measure"`
}

type aggregateByIDGateRec struct {
	Group string `db:"group" rbi:"index"`
	Score int    `db:"score" rbi:"index"`
}

func openTempAggregateDB(t *testing.T) *DB[uint64, aggregateTestRec] {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "aggregate.db")
	db, raw := openBoltAndNew[uint64, aggregateTestRec](t, path, Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	return db
}

func seedAggregateTestData(t *testing.T, db *DB[uint64, aggregateTestRec]) {
	t.Helper()
	score10 := int64(10)
	score20 := int64(20)
	score30 := int64(30)
	score40 := int64(40)
	rows := []aggregateTestRec{
		{Country: "US", Status: "active", Name: "alice", Age: 30, Score: &score10},
		{Country: "US", Status: "active", Name: "bob", Age: 40, Score: &score20},
		{Country: "US", Status: "inactive", Name: "carol", Age: 50, Score: &score30},
		{Country: "DE", Status: "active", Name: "dora", Age: 60},
		{Country: "DE", Status: "inactive", Name: "eric", Age: 17, Score: &score40},
	}
	for i := range rows {
		if err := db.Set(uint64(i+1), &rows[i]); err != nil {
			t.Fatalf("Set(%d): %v", i+1, err)
		}
	}
}

func TestAggregateOrdinaryMetricsAndRowCount(t *testing.T) {
	db := openTempAggregateDB(t)
	seedAggregateTestData(t, db)

	result, err := db.Aggregate(qx.Aggregate(qx.ROWCOUNT().AS("rows")))
	if err != nil {
		t.Fatalf("Aggregate rowcount: %v", err)
	}
	requireAggregateUint(t, result.Rows[0][0], 5)

	result, err = db.Aggregate(qx.Query(qx.GTE("age", 30)).Metrics(
		qx.ROWCOUNT().AS("rows"),
		qx.COUNT("age").AS("age_count"),
		qx.SUM("age").AS("age_sum"),
		qx.AVG("age").AS("age_avg"),
		qx.MIN("age").AS("age_min"),
		qx.MAX("age").AS("age_max"),
		qx.MIN("name").AS("name_min"),
		qx.MAX("name").AS("name_max"),
	))
	if err != nil {
		t.Fatalf("Aggregate ordinary: %v", err)
	}
	requireAggregateLayout(t, result.Layout, []string{
		"rows",
		"age_count",
		"age_sum",
		"age_avg",
		"age_min",
		"age_max",
		"name_min",
		"name_max",
	})
	row := result.Rows[0]
	requireAggregateUint(t, row[0], 4)
	requireAggregateUint(t, row[1], 4)
	requireAggregateInt(t, row[2], 180)
	requireAggregateFloat(t, row[3], 45)
	requireAggregateInt(t, row[4], 30)
	requireAggregateInt(t, row[5], 60)
	requireAggregateString(t, row[6], "alice")
	requireAggregateString(t, row[7], "dora")

	result, err = db.Aggregate(qx.Query(qx.EQ("name", "missing")).Metrics(
		qx.ROWCOUNT().AS("rows"),
		qx.COUNT("age").AS("age_count"),
		qx.AVG("age").AS("age_avg"),
		qx.MIN("age").AS("age_min"),
		qx.MAX("age").AS("age_max"),
	))
	if err != nil {
		t.Fatalf("Aggregate ordinary empty: %v", err)
	}
	row = result.Rows[0]
	requireAggregateUint(t, row[0], 0)
	requireAggregateUint(t, row[1], 0)
	requireAggregateNone(t, row[2])
	requireAggregateNone(t, row[3])
	requireAggregateNone(t, row[4])
}

func TestAggregateGroupedOrdinaryByIDGateUsesSelectivity(t *testing.T) {
	dir := t.TempDir()
	db, raw := openBoltAndNew[uint64, aggregateByIDGateRec](t, filepath.Join(dir, "aggregate_by_id_gate.db"), Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	ids := make([]uint64, 0, 20)
	vals := make([]*aggregateByIDGateRec, 0, 20)
	for i := 1; i <= 20; i++ {
		group := "a"
		if i&1 == 0 {
			group = "b"
		}
		ids = append(ids, uint64(i))
		vals = append(vals, &aggregateByIDGateRec{Group: group, Score: i})
	}
	if err := db.BatchSet(ids, vals); err != nil {
		t.Fatalf("BatchSet: %v", err)
	}

	full, err := db.prepareAggregate(qx.Group("group").Metrics(
		qx.COUNT("score").AS("score_count"),
		qx.SUM("score").AS("score_sum"),
		qx.MIN("score").AS("score_min"),
	))
	if err != nil {
		t.Fatalf("prepare full aggregate: %v", err)
	}
	defer full.release()

	selective, err := db.prepareAggregate(qx.Query(qx.EQ("score", 1)).Group("group").Metrics(
		qx.COUNT("score").AS("score_count"),
		qx.SUM("score").AS("score_sum"),
		qx.MIN("score").AS("score_min"),
	))
	if err != nil {
		t.Fatalf("prepare selective aggregate: %v", err)
	}
	defer selective.release()

	snap, seq, ref, pinned := db.pinCurrentSnapshot()
	defer db.unpinCurrentSnapshot(seq, ref, pinned)
	view := db.makeQueryView(snap)
	defer db.releaseQueryView(view)

	fullIDs, err := view.aggregateMatchedIDs(full.filter)
	if err != nil {
		t.Fatalf("full aggregate ids: %v", err)
	}
	if !view.canExecuteGroupedOrdinaryByID(full, fullIDs) {
		fullIDs.Release()
		t.Fatal("full high-card grouped ordinary aggregate did not use by-ID gate")
	}
	fullIDs.Release()

	selectiveIDs, err := view.aggregateMatchedIDs(selective.filter)
	if err != nil {
		t.Fatalf("selective aggregate ids: %v", err)
	}
	if view.canExecuteGroupedOrdinaryByID(selective, selectiveIDs) {
		selectiveIDs.Release()
		t.Fatal("selective grouped ordinary aggregate used by-ID gate")
	}
	selectiveIDs.Release()
}

func TestAggregateUngroupedMeasureAndOrdinaryMetrics(t *testing.T) {
	db := openTempAggregateDB(t)
	seedAggregateTestData(t, db)

	result, err := db.Aggregate(qx.Query(qx.GTE("age", 30)).Metrics(
		qx.ROWCOUNT().AS("rows"),
		qx.COUNT("score").AS("score_count"),
		qx.SUM("score").AS("score_sum"),
		qx.AVG("score").AS("score_avg"),
		qx.MIN("score").AS("score_min"),
		qx.MAX("score").AS("score_max"),
		qx.SUM("age").AS("age_sum"),
		qx.MIN("country").AS("country_min"),
	))
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("rows len=%d, want 1", len(result.Rows))
	}
	row := result.Rows[0]
	requireAggregateLayout(t, result.Layout, []string{
		"rows",
		"score_count",
		"score_sum",
		"score_avg",
		"score_min",
		"score_max",
		"age_sum",
		"country_min",
	})
	requireAggregateUint(t, row[0], 4)
	requireAggregateUint(t, row[1], 3)
	requireAggregateInt(t, row[2], 60)
	requireAggregateFloat(t, row[3], 20)
	requireAggregateInt(t, row[4], 10)
	requireAggregateInt(t, row[5], 30)
	requireAggregateInt(t, row[6], 180)
	requireAggregateString(t, row[7], "DE")
}

func TestAggregateGroupByMultipleFieldsUsesInvertedIndexes(t *testing.T) {
	db := openTempAggregateDB(t)
	seedAggregateTestData(t, db)

	result, err := db.Aggregate(qx.Query(qx.GTE("age", 18)).
		Group("country", "status").
		Metrics(
			qx.ROWCOUNT().AS("rows"),
			qx.COUNT("score").AS("score_count"),
			qx.SUM("score").AS("score_sum"),
		))
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	requireAggregateLayout(t, result.Layout, []string{"country", "status", "rows", "score_count", "score_sum"})

	got := make(map[string]Row, len(result.Rows))
	for i := range result.Rows {
		country := mustAggregateString(t, result.Rows[i][0])
		status := mustAggregateString(t, result.Rows[i][1])
		got[country+"|"+status] = result.Rows[i]
	}
	if len(got) != 3 {
		t.Fatalf("group count=%d, want 3; rows=%#v", len(got), result.Rows)
	}
	requireAggregateUint(t, got["DE|active"][2], 1)
	requireAggregateUint(t, got["DE|active"][3], 0)
	requireAggregateInt(t, got["DE|active"][4], 0)
	requireAggregateUint(t, got["US|active"][2], 2)
	requireAggregateUint(t, got["US|active"][3], 2)
	requireAggregateInt(t, got["US|active"][4], 30)
	requireAggregateUint(t, got["US|inactive"][2], 1)
	requireAggregateUint(t, got["US|inactive"][3], 1)
	requireAggregateInt(t, got["US|inactive"][4], 30)
}

func TestAggregateNullGroupAndNullableMeasure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "aggregate_null_group.db")
	db, raw := openBoltAndNew[uint64, aggregateNullGroupRec](t, path, Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	segmentA := "a"
	segmentB := "b"
	amount10 := int64(10)
	amount20 := int64(20)
	rows := []aggregateNullGroupRec{
		{Segment: &segmentA, Amount: &amount10},
		{Amount: &amount20},
		{},
		{Segment: &segmentB},
	}
	for i := range rows {
		if err := db.Set(uint64(i+1), &rows[i]); err != nil {
			t.Fatalf("Set(%d): %v", i+1, err)
		}
	}

	result, err := db.Aggregate(qx.Group("segment").Metrics(
		qx.ROWCOUNT().AS("rows"),
		qx.COUNT("amount").AS("amount_count"),
		qx.SUM("amount").AS("amount_sum"),
		qx.MIN("amount").AS("amount_min"),
		qx.MAX("amount").AS("amount_max"),
	))
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	requireAggregateLayout(t, result.Layout, []string{"segment", "rows", "amount_count", "amount_sum", "amount_min", "amount_max"})

	got := make(map[string]Row, len(result.Rows))
	for i := range result.Rows {
		got[aggregateGroupKey(t, result.Rows[i][0])] = result.Rows[i]
	}
	if len(got) != 3 {
		t.Fatalf("group count=%d, want 3; rows=%#v", len(got), result.Rows)
	}

	requireAggregateUint(t, got["a"][1], 1)
	requireAggregateUint(t, got["a"][2], 1)
	requireAggregateInt(t, got["a"][3], 10)
	requireAggregateInt(t, got["a"][4], 10)
	requireAggregateInt(t, got["a"][5], 10)

	requireAggregateUint(t, got["b"][1], 1)
	requireAggregateUint(t, got["b"][2], 0)
	requireAggregateInt(t, got["b"][3], 0)
	requireAggregateNone(t, got["b"][4])
	requireAggregateNone(t, got["b"][5])

	requireAggregateUint(t, got["<null>"][1], 2)
	requireAggregateUint(t, got["<null>"][2], 1)
	requireAggregateInt(t, got["<null>"][3], 20)
	requireAggregateInt(t, got["<null>"][4], 20)
	requireAggregateInt(t, got["<null>"][5], 20)
}

func TestAggregateGroupByAliasControlsLayoutAndCollisions(t *testing.T) {
	db := openTempAggregateDB(t)
	seedAggregateTestData(t, db)

	result, err := db.Aggregate(qx.GroupBy(qx.REF("country").AS("c")).Metrics(qx.ROWCOUNT().AS("rows")))
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	requireAggregateLayout(t, result.Layout, []string{"c", "rows"})

	_, err = db.Aggregate(qx.GroupBy(qx.REF("country").AS("rows")).Metrics(qx.ROWCOUNT().AS("rows")))
	if err == nil || !strings.Contains(err.Error(), `duplicate aggregate output "rows"`) {
		t.Fatalf("duplicate group/metric alias err=%v", err)
	}
}

func TestAggregateWindowAppliesToResultRows(t *testing.T) {
	db := openTempAggregateDB(t)
	seedAggregateTestData(t, db)

	result, err := db.Aggregate(qx.Group("country").Metrics(qx.ROWCOUNT().AS("rows")).Offset(1).Limit(1))
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	requireAggregateLayout(t, result.Layout, []string{"country", "rows"})
	if len(result.Rows) != 1 {
		t.Fatalf("window rows len=%d, want 1; rows=%#v", len(result.Rows), result.Rows)
	}
	requireAggregateString(t, result.Rows[0][0], "US")
	requireAggregateUint(t, result.Rows[0][1], 3)

	result, err = db.Aggregate(qx.Group("country").Metrics(qx.ROWCOUNT().AS("rows")).Offset(10))
	if err != nil {
		t.Fatalf("Aggregate offset beyond end: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("offset beyond end rows len=%d, want 0", len(result.Rows))
	}
}

func TestAggregateHavingFiltersRows(t *testing.T) {
	db := openTempAggregateDB(t)
	seedAggregateTestData(t, db)

	result, err := db.Aggregate(qx.Query(qx.GTE("age", 18)).
		Group("country", "status").
		Metrics(
			qx.ROWCOUNT().AS("rows"),
			qx.SUM("score").AS("score_sum"),
		).
		Having(
			qx.IN(qx.OUT("country"), []string{"DE", "US"}),
			qx.GT(qx.OUT("score_sum"), 20),
		))
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	requireAggregateLayout(t, result.Layout, []string{"country", "status", "rows", "score_sum"})

	got := make(map[string]Row, len(result.Rows))
	for i := range result.Rows {
		country := mustAggregateString(t, result.Rows[i][0])
		status := mustAggregateString(t, result.Rows[i][1])
		got[country+"|"+status] = result.Rows[i]
	}
	if len(got) != 2 {
		t.Fatalf("having rows len=%d, want 2; rows=%#v", len(result.Rows), result.Rows)
	}
	requireAggregateUint(t, got["US|active"][2], 2)
	requireAggregateInt(t, got["US|active"][3], 30)
	requireAggregateUint(t, got["US|inactive"][2], 1)
	requireAggregateInt(t, got["US|inactive"][3], 30)

	result, err = db.Aggregate(qx.Aggregate(qx.ROWCOUNT().AS("rows")).Having(qx.GT(qx.OUT("rows"), 10)))
	if err != nil {
		t.Fatalf("Aggregate ungrouped having: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("ungrouped having rows len=%d, want 0", len(result.Rows))
	}
}

func TestAggregateHavingPointerLiterals(t *testing.T) {
	db := openTempAggregateDB(t)
	seedAggregateTestData(t, db)

	limit := uint64(1)
	countryUS := "US"
	result, err := db.Aggregate(qx.Query(qx.GTE("age", 18)).
		Group("country", "status").
		Metrics(qx.ROWCOUNT().AS("rows")).
		Having(
			qx.GT(qx.OUT("rows"), &limit),
			qx.IN(qx.OUT("country"), []*string{&countryUS}),
		))
	if err != nil {
		t.Fatalf("Aggregate pointer scalar/IN element: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("pointer having rows len=%d, want 1; rows=%#v", len(result.Rows), result.Rows)
	}
	requireAggregateString(t, result.Rows[0][0], "US")
	requireAggregateString(t, result.Rows[0][1], "active")
	requireAggregateUint(t, result.Rows[0][2], 2)

	countries := []string{"US"}
	result, err = db.Aggregate(qx.Query(qx.GTE("age", 18)).
		Group("country").
		Metrics(qx.ROWCOUNT().AS("rows")).
		Having(qx.OP(qx.OpIN, qx.OUT("country"), qx.LIT(&countries))))
	if err != nil {
		t.Fatalf("Aggregate pointer IN slice: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("pointer slice IN rows len=%d, want 1; rows=%#v", len(result.Rows), result.Rows)
	}
	requireAggregateString(t, result.Rows[0][0], "US")
	requireAggregateUint(t, result.Rows[0][1], 3)
}

func TestAggregateHavingNullPredicates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "aggregate_having_null.db")
	db, raw := openBoltAndNew[uint64, aggregateNullGroupRec](t, path, Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	segmentA := "a"
	segmentB := "b"
	amount10 := int64(10)
	rows := []aggregateNullGroupRec{
		{Segment: &segmentA, Amount: &amount10},
		{Segment: &segmentB},
		{},
	}
	for i := range rows {
		if err := db.Set(uint64(i+1), &rows[i]); err != nil {
			t.Fatalf("Set(%d): %v", i+1, err)
		}
	}

	result, err := db.Aggregate(qx.Group("segment").Metrics(qx.AVG("amount").AS("avg_amount")).
		Having(qx.ISNULL(qx.OUT("avg_amount"))).
		SortOut("segment"))
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	requireAggregateLayout(t, result.Layout, []string{"segment", "avg_amount"})
	if len(result.Rows) != 2 {
		t.Fatalf("null having rows len=%d, want 2; rows=%#v", len(result.Rows), result.Rows)
	}
	requireAggregateNone(t, result.Rows[0][0])
	requireAggregateNone(t, result.Rows[0][1])
	requireAggregateString(t, result.Rows[1][0], "b")
	requireAggregateNone(t, result.Rows[1][1])

	var nilFloat *float64
	result, err = db.Aggregate(qx.Group("segment").Metrics(qx.AVG("amount").AS("avg_amount")).
		Having(qx.EQ(qx.OUT("avg_amount"), nilFloat)).
		SortOut("segment"))
	if err != nil {
		t.Fatalf("Aggregate typed nil EQ: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("typed nil EQ rows len=%d, want 2; rows=%#v", len(result.Rows), result.Rows)
	}
	requireAggregateNone(t, result.Rows[0][0])
	requireAggregateString(t, result.Rows[1][0], "b")

	result, err = db.Aggregate(qx.Group("segment").Metrics(qx.AVG("amount").AS("avg_amount")).
		Having(qx.IN(qx.OUT("avg_amount"), []*float64{nilFloat})).
		SortOut("segment"))
	if err != nil {
		t.Fatalf("Aggregate typed nil IN: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("typed nil IN rows len=%d, want 2; rows=%#v", len(result.Rows), result.Rows)
	}
	requireAggregateNone(t, result.Rows[0][0])
	requireAggregateString(t, result.Rows[1][0], "b")

	result, err = db.Aggregate(qx.Group("segment").Metrics(qx.AVG("amount").AS("avg_amount")).
		Having(qx.GTE(qx.OUT("avg_amount"), nil)))
	if err != nil {
		t.Fatalf("Aggregate GTE nil: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("GTE nil having rows len=%d, want 0; rows=%#v", len(result.Rows), result.Rows)
	}

	result, err = db.Aggregate(qx.Group("segment").Metrics(qx.AVG("amount").AS("avg_amount")).
		Having(qx.LTE(qx.OUT("avg_amount"), nil)))
	if err != nil {
		t.Fatalf("Aggregate LTE nil: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("LTE nil having rows len=%d, want 0; rows=%#v", len(result.Rows), result.Rows)
	}
}

func TestAggregateOrderByMultipleOutputsBeforeWindow(t *testing.T) {
	db := openTempAggregateDB(t)
	seedAggregateTestData(t, db)

	result, err := db.Aggregate(qx.Query(qx.GTE("age", 18)).
		Group("country", "status").
		Metrics(
			qx.ROWCOUNT().AS("rows"),
			qx.SUM("score").AS("score_sum"),
		).
		SortOut("rows", qx.DESC).
		SortOut("score_sum", qx.DESC).
		SortOut("country"))
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	requireAggregateLayout(t, result.Layout, []string{"country", "status", "rows", "score_sum"})
	if len(result.Rows) != 3 {
		t.Fatalf("ordered rows len=%d, want 3; rows=%#v", len(result.Rows), result.Rows)
	}
	requireAggregateString(t, result.Rows[0][0], "US")
	requireAggregateString(t, result.Rows[0][1], "active")
	requireAggregateString(t, result.Rows[1][0], "US")
	requireAggregateString(t, result.Rows[1][1], "inactive")
	requireAggregateString(t, result.Rows[2][0], "DE")
	requireAggregateString(t, result.Rows[2][1], "active")

	result, err = db.Aggregate(qx.Query(qx.GTE("age", 18)).
		Group("country", "status").
		Metrics(
			qx.ROWCOUNT().AS("rows"),
			qx.SUM("score").AS("score_sum"),
		).
		SortOut("rows", qx.DESC).
		SortOut("score_sum", qx.DESC).
		SortOut("country").
		Offset(1).
		Limit(1))
	if err != nil {
		t.Fatalf("Aggregate with window: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("window rows len=%d, want 1; rows=%#v", len(result.Rows), result.Rows)
	}
	requireAggregateString(t, result.Rows[0][0], "US")
	requireAggregateString(t, result.Rows[0][1], "inactive")
}

func TestAggregateHavingComparesLargeIntegersWithFloatLiteralsExactly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "aggregate_having_precision.db")
	db, raw := openBoltAndNew[uint64, aggregateHavingPrecisionRec](t, path, Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	const exactFloatBoundary = uint64(1 << 53)
	rows := []aggregateHavingPrecisionRec{
		{Group: "uint_eq", Unsigned: exactFloatBoundary},
		{Group: "uint_hi", Unsigned: exactFloatBoundary + 1},
		{Group: "int_eq", Signed: -int64(exactFloatBoundary)},
		{Group: "int_low", Signed: -int64(exactFloatBoundary) - 1},
	}
	for i := range rows {
		if err := db.Set(uint64(i+1), &rows[i]); err != nil {
			t.Fatalf("Set(%d): %v", i+1, err)
		}
	}

	result, err := db.Aggregate(qx.Group("group").Metrics(qx.SUM("unsigned").AS("sum")).
		Having(qx.GT(qx.OUT("sum"), float64(exactFloatBoundary))).
		SortOut("group"))
	if err != nil {
		t.Fatalf("Aggregate unsigned GT: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("unsigned GT rows len=%d, want 1; rows=%#v", len(result.Rows), result.Rows)
	}
	requireAggregateString(t, result.Rows[0][0], "uint_hi")
	requireAggregateUint(t, result.Rows[0][1], exactFloatBoundary+1)

	result, err = db.Aggregate(qx.Group("group").Metrics(qx.SUM("unsigned").AS("sum")).
		Having(qx.EQ(qx.OUT("sum"), float64(exactFloatBoundary))).
		SortOut("group"))
	if err != nil {
		t.Fatalf("Aggregate unsigned EQ: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("unsigned EQ rows len=%d, want 1; rows=%#v", len(result.Rows), result.Rows)
	}
	requireAggregateString(t, result.Rows[0][0], "uint_eq")

	result, err = db.Aggregate(qx.Group("group").Metrics(qx.SUM("unsigned").AS("sum")).
		Having(qx.IN(qx.OUT("sum"), []float64{float64(exactFloatBoundary)})).
		SortOut("group"))
	if err != nil {
		t.Fatalf("Aggregate unsigned IN: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("unsigned IN rows len=%d, want 1; rows=%#v", len(result.Rows), result.Rows)
	}
	requireAggregateString(t, result.Rows[0][0], "uint_eq")

	result, err = db.Aggregate(qx.Group("group").Metrics(qx.SUM("signed").AS("sum")).
		Having(qx.LT(qx.OUT("sum"), -float64(exactFloatBoundary))).
		SortOut("group"))
	if err != nil {
		t.Fatalf("Aggregate signed LT: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("signed LT rows len=%d, want 1; rows=%#v", len(result.Rows), result.Rows)
	}
	requireAggregateString(t, result.Rows[0][0], "int_low")
	requireAggregateInt(t, result.Rows[0][1], -int64(exactFloatBoundary)-1)
}

func TestAggregateValueIndexerFieldsUseStringIndexKeys(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "aggregate_vi.db")
	db, raw := openBoltAndNew[uint64, aggregateVIRec](t, path, Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	if err := db.Set(1, &aggregateVIRec{Code: 10, Flag: true}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &aggregateVIRec{Code: 2, Flag: false}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	result, err := db.Aggregate(qx.Query().Metrics(
		qx.MIN("code").AS("min_code"),
		qx.MAX("code").AS("max_code"),
	))
	if err != nil {
		t.Fatalf("Aggregate MIN/MAX: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("rows len=%d, want 1", len(result.Rows))
	}
	requireAggregateString(t, result.Rows[0][0], "002")
	requireAggregateString(t, result.Rows[0][1], "010")

	result, err = db.Aggregate(qx.Query().Metrics(qx.DISTINCT("flag").AS("flag")))
	if err != nil {
		t.Fatalf("Aggregate DISTINCT flag: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("distinct rows len=%d, want 2", len(result.Rows))
	}
	requireAggregateString(t, result.Rows[0][0], "N")
	requireAggregateString(t, result.Rows[1][0], "Y")

	_, err = db.Aggregate(qx.Query().Metrics(qx.SUM("code").AS("sum_code")))
	if err == nil || !strings.Contains(err.Error(), `SUM requires numeric field "code"`) {
		t.Fatalf("SUM over ValueIndexer err=%v", err)
	}
	_, err = db.Aggregate(qx.Query().Metrics(qx.AVG("code").AS("avg_code")))
	if err == nil || !strings.Contains(err.Error(), `AVG requires numeric field "code"`) {
		t.Fatalf("AVG over ValueIndexer err=%v", err)
	}
}

func TestAggregateDistinctField(t *testing.T) {
	db := openTempAggregateDB(t)
	seedAggregateTestData(t, db)

	result, err := db.Aggregate(qx.Query(qx.GTE("age", 18)).Metrics(qx.DISTINCT("country").AS("country")))
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	requireAggregateLayout(t, result.Layout, []string{"country"})
	if len(result.Rows) != 2 {
		t.Fatalf("rows len=%d, want 2", len(result.Rows))
	}
	requireAggregateString(t, result.Rows[0][0], "DE")
	requireAggregateString(t, result.Rows[1][0], "US")
}

func TestAggregateCountDistinctField(t *testing.T) {
	db := openTempAggregateDB(t)
	seedAggregateTestData(t, db)

	result, err := db.Aggregate(qx.Aggregate(
		qx.COUNT(qx.DISTINCT("country")).AS("country_count"),
		qx.ROWCOUNT().AS("rows"),
	))
	if err != nil {
		t.Fatalf("Aggregate ungrouped: %v", err)
	}
	requireAggregateLayout(t, result.Layout, []string{"country_count", "rows"})
	if len(result.Rows) != 1 {
		t.Fatalf("rows len=%d, want 1", len(result.Rows))
	}
	requireAggregateUint(t, result.Rows[0][0], 2)
	requireAggregateUint(t, result.Rows[0][1], 5)

	result, err = db.Aggregate(qx.Group("status").Metrics(
		qx.COUNT(qx.DISTINCT("country")).AS("country_count"),
		qx.ROWCOUNT().AS("rows"),
	).SortOut("status"))
	if err != nil {
		t.Fatalf("Aggregate grouped: %v", err)
	}
	requireAggregateLayout(t, result.Layout, []string{"status", "country_count", "rows"})
	if len(result.Rows) != 2 {
		t.Fatalf("group rows len=%d, want 2; rows=%#v", len(result.Rows), result.Rows)
	}
	requireAggregateString(t, result.Rows[0][0], "active")
	requireAggregateUint(t, result.Rows[0][1], 2)
	requireAggregateUint(t, result.Rows[0][2], 3)
	requireAggregateString(t, result.Rows[1][0], "inactive")
	requireAggregateUint(t, result.Rows[1][1], 2)
	requireAggregateUint(t, result.Rows[1][2], 2)
}

func TestAggregateCountDistinctIgnoresNullValues(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "aggregate_count_distinct_null.db")
	db, raw := openBoltAndNew[uint64, aggregateNullGroupRec](t, path, Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	segmentA := "a"
	segmentB := "b"
	rows := []aggregateNullGroupRec{
		{Segment: &segmentA},
		{},
		{Segment: &segmentB},
		{},
	}
	for i := range rows {
		if err := db.Set(uint64(i+1), &rows[i]); err != nil {
			t.Fatalf("Set(%d): %v", i+1, err)
		}
	}

	result, err := db.Aggregate(qx.Aggregate(
		qx.COUNT(qx.DISTINCT("segment")).AS("segment_count"),
		qx.DISTINCT("segment").AS("segment"),
	))
	if err == nil || !strings.Contains(err.Error(), "DISTINCT is supported only as a single ungrouped metric") {
		t.Fatalf("mixed rowset DISTINCT err=%v", err)
	}

	result, err = db.Aggregate(qx.Aggregate(qx.COUNT(qx.DISTINCT("segment")).AS("segment_count")))
	if err != nil {
		t.Fatalf("Aggregate count distinct: %v", err)
	}
	requireAggregateLayout(t, result.Layout, []string{"segment_count"})
	requireAggregateUint(t, result.Rows[0][0], 2)

	result, err = db.Aggregate(qx.Aggregate(qx.DISTINCT("segment").AS("segment")))
	if err != nil {
		t.Fatalf("Aggregate distinct: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("distinct rows len=%d, want 3; rows=%#v", len(result.Rows), result.Rows)
	}
}

func TestAggregateEmptyMatchesReturnShapeWithoutRowsForGroupedAndDistinct(t *testing.T) {
	db := openTempAggregateDB(t)
	seedAggregateTestData(t, db)

	result, err := db.Aggregate(qx.Query(qx.EQ("name", "missing")).Metrics(qx.DISTINCT("country").AS("country")))
	if err != nil {
		t.Fatalf("Aggregate DISTINCT empty: %v", err)
	}
	requireAggregateLayout(t, result.Layout, []string{"country"})
	if len(result.Rows) != 0 {
		t.Fatalf("distinct empty rows len=%d, want 0", len(result.Rows))
	}

	result, err = db.Aggregate(qx.Query(qx.EQ("name", "missing")).
		Group("country").
		Metrics(qx.ROWCOUNT().AS("rows"), qx.SUM("age").AS("age_sum")))
	if err != nil {
		t.Fatalf("Aggregate GROUP empty: %v", err)
	}
	requireAggregateLayout(t, result.Layout, []string{"country", "rows", "age_sum"})
	if len(result.Rows) != 0 {
		t.Fatalf("grouped empty rows len=%d, want 0", len(result.Rows))
	}
}

func TestAggregateSUMEmptyPreservesFieldKind(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "aggregate_sum_type.db")
	db, raw := openBoltAndNew[uint64, aggregateSumTypeRec](t, path, Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	if err := db.Set(1, &aggregateSumTypeRec{Group: "nil-only"}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	result, err := db.Aggregate(qx.Query(qx.EQ("group", "missing")).Metrics(
		qx.SUM("ordinary_u").AS("ordinary_u"),
		qx.SUM("ordinary_f").AS("ordinary_f"),
		qx.SUM("measure_u").AS("measure_u"),
		qx.SUM("measure_f").AS("measure_f"),
	))
	if err != nil {
		t.Fatalf("Aggregate(empty): %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("empty rows len=%d, want 1", len(result.Rows))
	}
	requireAggregateUint(t, result.Rows[0][0], 0)
	requireAggregateFloat(t, result.Rows[0][1], 0)
	requireAggregateUint(t, result.Rows[0][2], 0)
	requireAggregateFloat(t, result.Rows[0][3], 0)

	result, err = db.Aggregate(qx.Query(qx.EQ("group", "nil-only")).Metrics(
		qx.SUM("measure_u").AS("measure_u"),
		qx.SUM("measure_f").AS("measure_f"),
	))
	if err != nil {
		t.Fatalf("Aggregate(nil measure): %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("nil measure rows len=%d, want 1", len(result.Rows))
	}
	requireAggregateUint(t, result.Rows[0][0], 0)
	requireAggregateFloat(t, result.Rows[0][1], 0)
}

func TestAggregateMeasureOnlyDB(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "measure_only.db")
	db, raw := openBoltAndNew[uint64, measureOnlyRec](t, path, Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	if err := db.Set(1, &measureOnlyRec{Amount: 10}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &measureOnlyRec{Amount: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	result, err := db.Aggregate(qx.Aggregate(
		qx.ROWCOUNT().AS("rows"),
		qx.SUM("amount").AS("sum"),
		qx.AVG("amount").AS("avg"),
	))
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("rows len=%d, want 1", len(result.Rows))
	}
	requireAggregateLayout(t, result.Layout, []string{"rows", "sum", "avg"})
	requireAggregateUint(t, result.Rows[0][0], 2)
	requireAggregateInt(t, result.Rows[0][1], 30)
	requireAggregateFloat(t, result.Rows[0][2], 15)
}

func TestAggregateFloatValuesIncludeNegativeAndZero(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "aggregate_float.db")
	db, raw := openBoltAndNew[uint64, aggregateFloatRec](t, path, Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	rows := []aggregateFloatRec{
		{Group: "all", Ordinary: -2.5, Measure: -2.5},
		{Group: "all", Ordinary: 0, Measure: 0},
		{Group: "all", Ordinary: 4.5, Measure: 4.5},
	}
	for i := range rows {
		if err := db.Set(uint64(i+1), &rows[i]); err != nil {
			t.Fatalf("Set(%d): %v", i+1, err)
		}
	}

	result, err := db.Aggregate(qx.Query(qx.EQ("group", "all")).Metrics(
		qx.SUM("ordinary").AS("ordinary_sum"),
		qx.AVG("ordinary").AS("ordinary_avg"),
		qx.MIN("ordinary").AS("ordinary_min"),
		qx.MAX("ordinary").AS("ordinary_max"),
		qx.SUM("measure").AS("measure_sum"),
		qx.AVG("measure").AS("measure_avg"),
		qx.MIN("measure").AS("measure_min"),
		qx.MAX("measure").AS("measure_max"),
	))
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	row := result.Rows[0]
	requireAggregateFloat(t, row[0], 2)
	requireAggregateFloat(t, row[1], 2.0/3.0)
	requireAggregateFloat(t, row[2], -2.5)
	requireAggregateFloat(t, row[3], 4.5)
	requireAggregateFloat(t, row[4], 2)
	requireAggregateFloat(t, row[5], 2.0/3.0)
	requireAggregateFloat(t, row[6], -2.5)
	requireAggregateFloat(t, row[7], 4.5)
}

func TestAggregateSUMIntegerOverflowReturnsError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "aggregate_overflow.db")
	db, raw := openBoltAndNew[uint64, aggregateOverflowRec](t, path, Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	rows := []aggregateOverflowRec{
		{Ordinary: math.MaxInt64, Signed: math.MaxInt64, Unsigned: math.MaxUint64},
		{Ordinary: 1, Signed: 1, Unsigned: 1},
	}
	for i := range rows {
		if err := db.Set(uint64(i+1), &rows[i]); err != nil {
			t.Fatalf("Set(%d): %v", i+1, err)
		}
	}

	cases := []struct {
		name string
		q    *qx.QX
		want string
	}{
		{name: "ordinary_signed", q: qx.Aggregate(qx.SUM("ordinary")), want: "integer SUM overflow"},
		{name: "measure_signed", q: qx.Aggregate(qx.SUM("signed")), want: "integer SUM overflow"},
		{name: "measure_unsigned", q: qx.Aggregate(qx.SUM("unsigned")), want: "unsigned SUM overflow"},
	}
	for i := range cases {
		_, err := db.Aggregate(cases[i].q)
		if err == nil || !strings.Contains(err.Error(), cases[i].want) {
			t.Fatalf("%s: err=%v, want %q", cases[i].name, err, cases[i].want)
		}
	}
}

func TestAggregateMeasureEmptyBaseBatchSetDuplicateIDsUsesLastValue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "measure_duplicate_batch.db")
	db, raw := openBoltAndNew[uint64, measureOnlyRec](t, path, Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	if err := db.BatchSet(
		[]uint64{1, 1},
		[]*measureOnlyRec{{Amount: 10}, {Amount: 20}},
	); err != nil {
		t.Fatalf("BatchSet duplicate ids: %v", err)
	}

	acc := db.measureFieldByName["amount"]
	storage := db.getSnapshot().measure.Get(acc.ordinal)
	if storage.rows() != 1 {
		t.Fatalf("measure rows=%d, want 1", storage.rows())
	}
	if got, ok := storage.lookup(1); !ok || got != 20 {
		t.Fatalf("measure lookup=(%d,%v), want (20,true)", got, ok)
	}

	result, err := db.Aggregate(qx.Aggregate(
		qx.COUNT("amount").AS("count"),
		qx.SUM("amount").AS("sum"),
		qx.AVG("amount").AS("avg"),
		qx.MIN("amount").AS("min"),
		qx.MAX("amount").AS("max"),
	))
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	requireAggregateUint(t, result.Rows[0][0], 1)
	requireAggregateInt(t, result.Rows[0][1], 20)
	requireAggregateFloat(t, result.Rows[0][2], 20)
	requireAggregateInt(t, result.Rows[0][3], 20)
	requireAggregateInt(t, result.Rows[0][4], 20)
}

func TestAggregateWideMeasureUsesFullAndMergeScans(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wide_measure.db")
	db, raw := openBoltAndNew[uint64, aggregateWideMeasureRec](t, path, Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	const total = 700
	const keep = 600
	for id := 1; id <= total; id++ {
		status := "drop"
		if id <= keep {
			status = "keep"
		}
		if err := db.Set(uint64(id), &aggregateWideMeasureRec{Status: status, Amount: int64(id)}); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}

	acc := db.measureFieldByName["amount"]
	storage := db.getSnapshot().measure.Get(acc.ordinal)
	if !useMeasureMergeScan(keep, storage) {
		t.Fatal("wide filtered measure aggregate must use merge scan")
	}

	result, err := db.Aggregate(qx.Aggregate(
		qx.COUNT("amount").AS("count"),
		qx.SUM("amount").AS("sum"),
		qx.MIN("amount").AS("min"),
		qx.MAX("amount").AS("max"),
	))
	if err != nil {
		t.Fatalf("Aggregate full: %v", err)
	}
	requireAggregateUint(t, result.Rows[0][0], total)
	requireAggregateInt(t, result.Rows[0][1], total*(total+1)/2)
	requireAggregateInt(t, result.Rows[0][2], 1)
	requireAggregateInt(t, result.Rows[0][3], total)

	result, err = db.Aggregate(qx.Query(qx.EQ("status", "keep")).Metrics(
		qx.COUNT("amount").AS("count"),
		qx.SUM("amount").AS("sum"),
		qx.AVG("amount").AS("avg"),
		qx.MIN("amount").AS("min"),
		qx.MAX("amount").AS("max"),
	))
	if err != nil {
		t.Fatalf("Aggregate filtered: %v", err)
	}
	requireAggregateUint(t, result.Rows[0][0], keep)
	requireAggregateInt(t, result.Rows[0][1], keep*(keep+1)/2)
	requireAggregateFloat(t, result.Rows[0][2], float64(keep+1)/2)
	requireAggregateInt(t, result.Rows[0][3], 1)
	requireAggregateInt(t, result.Rows[0][4], keep)
}

func TestAggregateRebuildIndexKeepsMultipleMeasureFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "two_measure_rebuild.db")
	db, raw := openBoltAndNew[uint64, aggregateTwoMeasureRec](t, path, Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	for id := 1; id <= 3; id++ {
		if err := db.Set(uint64(id), &aggregateTwoMeasureRec{Left: int64(id), Right: int64(id * 10)}); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	result, err := db.Aggregate(qx.Aggregate(
		qx.COUNT("left").AS("left_count"),
		qx.SUM("left").AS("left_sum"),
		qx.COUNT("right").AS("right_count"),
		qx.SUM("right").AS("right_sum"),
	))
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	requireAggregateUint(t, result.Rows[0][0], 3)
	requireAggregateInt(t, result.Rows[0][1], 6)
	requireAggregateUint(t, result.Rows[0][2], 3)
	requireAggregateInt(t, result.Rows[0][3], 60)
}

func TestMeasureChunkedAppendFillsTailChunk(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "measure_tail_fill.db")
	db, raw := openBoltAndNew[uint64, aggregateWideMeasureRec](t, path, Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	const seed = 600
	ids := make([]uint64, seed)
	vals := make([]*aggregateWideMeasureRec, seed)
	for i := 0; i < seed; i++ {
		id := uint64(i + 1)
		ids[i] = id
		vals[i] = &aggregateWideMeasureRec{Status: "keep", Amount: int64(id)}
	}
	if err := db.BatchSet(ids, vals); err != nil {
		t.Fatalf("BatchSet seed: %v", err)
	}

	const appendCount = 10
	appendIDs := make([]uint64, appendCount)
	appendVals := make([]*aggregateWideMeasureRec, appendCount)
	for i := 0; i < appendCount; i++ {
		id := uint64(seed + i + 1)
		appendIDs[i] = id
		appendVals[i] = &aggregateWideMeasureRec{Status: "keep", Amount: int64(id)}
	}
	if err := db.BatchSet(appendIDs, appendVals); err != nil {
		t.Fatalf("BatchSet append: %v", err)
	}

	acc := db.measureFieldByName["amount"]
	storage := db.getSnapshot().measure.Get(acc.ordinal)
	if storage.chunked == nil {
		t.Fatal("measure storage must be chunked")
	}
	if got := storage.chunked.refsByID.Len(); got != 3 {
		t.Fatalf("chunk count=%d, want 3", got)
	}
	last := storage.chunked.refsByID.Get(storage.chunked.refsByID.Len() - 1).chunk
	if got, want := last.ids.Len(), seed-2*measureChunkTargetRows+appendCount; got != want {
		t.Fatalf("tail chunk rows=%d, want %d", got, want)
	}
	if storage.rows() != seed+appendCount {
		t.Fatalf("measure rows=%d, want %d", storage.rows(), seed+appendCount)
	}
	result, err := db.Aggregate(qx.Aggregate(qx.COUNT("amount").AS("count"), qx.SUM("amount").AS("sum")))
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	total := seed + appendCount
	requireAggregateUint(t, result.Rows[0][0], uint64(total))
	requireAggregateInt(t, result.Rows[0][1], int64(total*(total+1)/2))
}

func TestAggregateMeasureStorageLoadsFromPersistedIndex(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "measure_persisted.db")
	var logs bytes.Buffer
	opts := Options{AnalyzeInterval: -1, Logger: log.New(&logs, "", 0)}

	db, raw := openBoltAndNew[uint64, measureOnlyRec](t, path, opts)
	if err := db.Set(1, &measureOnlyRec{Amount: 10}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &measureOnlyRec{Amount: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("raw Close: %v", err)
	}

	logs.Reset()
	db2, raw2 := openBoltAndNew[uint64, measureOnlyRec](t, path, opts)
	t.Cleanup(func() {
		_ = db2.Close()
		_ = raw2.Close()
	})
	if strings.Contains(logs.String(), "rebuilding index from bbolt") {
		t.Fatalf("measure storage was rebuilt instead of loaded: %s", logs.String())
	}

	result, err := db2.Aggregate(qx.Aggregate(qx.SUM("amount").AS("sum")))
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("rows len=%d, want 1", len(result.Rows))
	}
	requireAggregateInt(t, result.Rows[0][0], 30)
}

func TestAggregatePinnedSnapshotIsolation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "aggregate_snapshot_isolation.db")
	db, raw := openBoltAndNew[uint64, measureOnlyRec](t, path, Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	if err := db.Set(1, &measureOnlyRec{Amount: 10}); err != nil {
		t.Fatalf("Set initial: %v", err)
	}
	prepared, err := db.prepareAggregate(qx.Aggregate(qx.SUM("amount").AS("sum")))
	if err != nil {
		t.Fatalf("prepareAggregate: %v", err)
	}
	defer prepared.release()

	snap, seq, ref, pinned := db.pinCurrentSnapshot()
	defer db.unpinCurrentSnapshot(seq, ref, pinned)

	if err := db.Set(1, &measureOnlyRec{Amount: 99}); err != nil {
		t.Fatalf("Set update: %v", err)
	}

	view := db.makeQueryView(snap)
	ids, err := view.aggregateMatchedIDs(prepared.filter)
	if err != nil {
		db.releaseQueryView(view)
		t.Fatalf("aggregateMatchedIDs: %v", err)
	}
	oldResult, err := view.executeAggregate(prepared, ids)
	ids.Release()
	db.releaseQueryView(view)
	if err != nil {
		t.Fatalf("executeAggregate old snapshot: %v", err)
	}
	requireAggregateInt(t, oldResult.Rows[0][0], 10)

	currentResult, err := db.Aggregate(qx.Aggregate(qx.SUM("amount").AS("sum")))
	if err != nil {
		t.Fatalf("Aggregate current: %v", err)
	}
	requireAggregateInt(t, currentResult.Rows[0][0], 99)
}

func TestPersistedMeasureLoadDoesNotSatisfyOrdinaryPlannerStats(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "measure_planner_stats.db")
	opts := Options{AnalyzeInterval: -1, BucketName: "aggregate_planner_stats"}

	db, raw := openBoltAndNew[uint64, aggregatePlannerStatsBaseRec](t, path, opts)
	if err := db.Set(1, &aggregatePlannerStatsBaseRec{Category: "a", Amount: 10}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &aggregatePlannerStatsBaseRec{Category: "b", Amount: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("raw Close: %v", err)
	}

	db2, raw2 := openBoltAndNew[uint64, aggregatePlannerStatsNextRec](t, path, opts)
	t.Cleanup(func() {
		_ = db2.Close()
		_ = raw2.Close()
	})

	stats := db2.planner.stats.Load()
	if stats == nil {
		t.Fatalf("planner stats are missing")
	}
	fieldStats, ok := stats.Fields["category"]
	if !ok {
		t.Fatalf("planner stats for rebuilt ordinary field are missing: %#v", stats.Fields)
	}
	if fieldStats.DistinctKeys != 2 {
		t.Fatalf("category DistinctKeys=%d, want 2; stats=%#v", fieldStats.DistinctKeys, fieldStats)
	}
}

func TestAggregateMatchesSlowReferenceModel(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "aggregate_model.db")
	db, raw := openBoltAndNew[uint64, aggregateModelRec](t, path, Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	amount10 := int64(10)
	amountNeg5 := int64(-5)
	amount0 := int64(0)
	amount25 := int64(25)
	rows := []aggregateModelRec{
		{Country: "US", Status: "active", Age: 30, Amount: &amount10},
		{Country: "US", Status: "active", Age: 40},
		{Country: "US", Status: "paused", Age: 20, Amount: &amountNeg5},
		{Country: "DE", Status: "active", Age: 50, Amount: &amount0},
		{Country: "DE", Status: "paused", Age: 60, Amount: &amount25},
		{Country: "FR", Status: "active", Age: 18},
	}
	for i := range rows {
		if err := db.Set(uint64(i+1), &rows[i]); err != nil {
			t.Fatalf("Set(%d): %v", i+1, err)
		}
	}

	result, err := db.Aggregate(qx.Query(qx.OR(qx.EQ("status", "active"), qx.GTE("age", 50))).
		Group("country", "status").
		Metrics(
			qx.ROWCOUNT().AS("rows"),
			qx.COUNT("amount").AS("amount_count"),
			qx.SUM("amount").AS("amount_sum"),
			qx.AVG("amount").AS("amount_avg"),
			qx.MIN("amount").AS("amount_min"),
			qx.MAX("amount").AS("amount_max"),
			qx.SUM("age").AS("age_sum"),
			qx.AVG("age").AS("age_avg"),
			qx.MIN("age").AS("age_min"),
			qx.MAX("age").AS("age_max"),
		))
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	requireAggregateLayout(t, result.Layout, []string{
		"country",
		"status",
		"rows",
		"amount_count",
		"amount_sum",
		"amount_avg",
		"amount_min",
		"amount_max",
		"age_sum",
		"age_avg",
		"age_min",
		"age_max",
	})

	want := buildAggregateModelReference(rows)
	if len(result.Rows) != len(want) {
		t.Fatalf("rows len=%d, want %d; rows=%#v", len(result.Rows), len(want), result.Rows)
	}
	for i := range result.Rows {
		row := result.Rows[i]
		key := mustAggregateString(t, row[0]) + "|" + mustAggregateString(t, row[1])
		stats, ok := want[key]
		if !ok {
			t.Fatalf("unexpected group %q row=%#v", key, row)
		}
		requireAggregateUint(t, row[2], stats.rows)
		requireAggregateUint(t, row[3], stats.amountCount)
		requireAggregateInt(t, row[4], stats.amountSum)
		if stats.amountCount == 0 {
			requireAggregateNone(t, row[5])
			requireAggregateNone(t, row[6])
			requireAggregateNone(t, row[7])
		} else {
			requireAggregateFloat(t, row[5], float64(stats.amountSum)/float64(stats.amountCount))
			requireAggregateInt(t, row[6], stats.amountMin)
			requireAggregateInt(t, row[7], stats.amountMax)
		}
		requireAggregateInt(t, row[8], stats.ageSum)
		requireAggregateFloat(t, row[9], float64(stats.ageSum)/float64(stats.rows))
		requireAggregateInt(t, row[10], stats.ageMin)
		requireAggregateInt(t, row[11], stats.ageMax)
		delete(want, key)
	}
	if len(want) != 0 {
		t.Fatalf("missing groups: %#v", want)
	}
}

func TestAggregateRejectsUnsupportedFirstVersionShapes(t *testing.T) {
	db := openTempAggregateDB(t)
	seedAggregateTestData(t, db)

	cases := []struct {
		name string
		q    *qx.QX
		want string
	}{
		{
			name: "group_by_measure",
			q:    qx.Group("score").Metrics(qx.ROWCOUNT()),
			want: `GROUP BY measure field "score" is not supported`,
		},
		{
			name: "group_expression",
			q:    qx.GroupBy(qx.LOWER("country")).Metrics(qx.ROWCOUNT()),
			want: "GROUP BY supports only field references",
		},
		{
			name: "distinct_with_other_metric",
			q:    qx.Aggregate(qx.DISTINCT("country"), qx.ROWCOUNT()),
			want: "DISTINCT is supported only as a single ungrouped metric",
		},
		{
			name: "distinct_measure",
			q:    qx.Aggregate(qx.DISTINCT("score")),
			want: `DISTINCT over measure field "score" is not supported`,
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
			name: "count_distinct_measure",
			q:    qx.Aggregate(qx.COUNT(qx.DISTINCT("score"))),
			want: `DISTINCT over measure field "score" is not supported`,
		},
		{
			name: "having_left_expression",
			q:    qx.Aggregate(qx.ROWCOUNT().AS("rows")).Having(qx.GT(qx.ADD(qx.OUT("rows"), qx.LIT(1)), 1)),
			want: "aggregate HAVING left side supports only OUT references",
		},
		{
			name: "having_right_expression",
			q:    qx.Aggregate(qx.ROWCOUNT().AS("rows")).Having(qx.GT(qx.OUT("rows"), qx.ADD(qx.LIT(1), qx.LIT(2)))),
			want: "aggregate HAVING right side supports only literals",
		},
		{
			name: "having_unknown_output",
			q:    qx.Aggregate(qx.ROWCOUNT().AS("rows")).Having(qx.GT(qx.OUT("missing"), 1)),
			want: `unknown aggregate output "missing" in HAVING`,
		},
		{
			name: "having_in_empty_values",
			q:    qx.Aggregate(qx.ROWCOUNT().AS("rows")).Having(qx.IN(qx.OUT("rows"), []uint64{})),
			want: "no values provided",
		},
		{
			name: "having_in_nil_values",
			q:    qx.Aggregate(qx.ROWCOUNT().AS("rows")).Having(qx.IN(qx.OUT("rows"), []uint64(nil))),
			want: "no values provided",
		},
		{
			name: "having_unsupported_predicate",
			q:    qx.Aggregate(qx.ROWCOUNT().AS("rows")).Having(qx.CONTAINS(qx.OUT("rows"), "1")),
			want: "aggregate HAVING supports only simple OUT predicates",
		},
		{
			name: "projection",
			q:    qx.Aggregate(qx.ROWCOUNT()).Select("country"),
			want: "aggregate projection is not supported",
		},
		{
			name: "order_ref",
			q:    qx.Aggregate(qx.ROWCOUNT().AS("rows")).Sort("rows"),
			want: "aggregate ORDER supports only OUT references",
		},
		{
			name: "order_expression",
			q:    qx.Aggregate(qx.ROWCOUNT().AS("rows")).SortBy(qx.ADD(qx.OUT("rows"), qx.LIT(1))),
			want: "aggregate ORDER supports only OUT references",
		},
		{
			name: "order_unknown_output",
			q:    qx.Aggregate(qx.ROWCOUNT().AS("rows")).SortOut("missing"),
			want: `unknown aggregate output "missing" in ORDER`,
		},
		{
			name: "filter_measure",
			q:    qx.Query(qx.EQ("score", int64(10))).Metrics(qx.ROWCOUNT()),
			want: "score",
		},
	}

	for i := range cases {
		_, err := db.Aggregate(cases[i].q)
		if err == nil || !strings.Contains(err.Error(), cases[i].want) {
			t.Fatalf("%s: err=%v, want %q", cases[i].name, err, cases[i].want)
		}
	}
}

func TestAggregateRejectsCountDistinctSliceField(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "aggregate_count_distinct_slice.db")
	db, raw := openBoltAndNew[uint64, aggregateSliceRec](t, path, Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	_, err := db.Aggregate(qx.Aggregate(qx.COUNT(qx.DISTINCT("tags")).AS("tag_count")))
	if err == nil || !strings.Contains(err.Error(), `aggregate over slice field "tags" is not supported`) {
		t.Fatalf("COUNT(DISTINCT slice) err=%v", err)
	}
}

type aggregateModelReferenceStats struct {
	rows        uint64
	amountCount uint64
	amountSum   int64
	amountMin   int64
	amountMax   int64
	ageSum      int64
	ageMin      int64
	ageMax      int64
}

func buildAggregateModelReference(rows []aggregateModelRec) map[string]aggregateModelReferenceStats {
	out := make(map[string]aggregateModelReferenceStats)
	for i := range rows {
		rec := rows[i]
		if rec.Status != "active" && rec.Age < 50 {
			continue
		}
		key := rec.Country + "|" + rec.Status
		stats := out[key]
		stats.rows++
		stats.ageSum += int64(rec.Age)
		if stats.rows == 1 || int64(rec.Age) < stats.ageMin {
			stats.ageMin = int64(rec.Age)
		}
		if stats.rows == 1 || int64(rec.Age) > stats.ageMax {
			stats.ageMax = int64(rec.Age)
		}
		if rec.Amount != nil {
			amount := *rec.Amount
			stats.amountCount++
			stats.amountSum += amount
			if stats.amountCount == 1 || amount < stats.amountMin {
				stats.amountMin = amount
			}
			if stats.amountCount == 1 || amount > stats.amountMax {
				stats.amountMax = amount
			}
		}
		out[key] = stats
	}
	return out
}

func requireAggregateLayout(t *testing.T, got []string, want []string) {
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

func requireAggregateUint(t *testing.T, v Value, want uint64) {
	t.Helper()
	got, ok := v.Uint()
	if !ok || got != want {
		t.Fatalf("uint value=(%d,%v), want %d; kind=%v", got, ok, want, v.Kind())
	}
}

func requireAggregateInt(t *testing.T, v Value, want int64) {
	t.Helper()
	got, ok := v.Int()
	if !ok || got != want {
		t.Fatalf("int value=(%d,%v), want %d; kind=%v", got, ok, want, v.Kind())
	}
}

func requireAggregateFloat(t *testing.T, v Value, want float64) {
	t.Helper()
	got, ok := v.Float()
	if !ok || got != want {
		t.Fatalf("float value=(%v,%v), want %v; kind=%v", got, ok, want, v.Kind())
	}
}

func requireAggregateString(t *testing.T, v Value, want string) {
	t.Helper()
	got := mustAggregateString(t, v)
	if got != want {
		t.Fatalf("string value=%q, want %q; kind=%v", got, want, v.Kind())
	}
}

func mustAggregateString(t *testing.T, v Value) string {
	t.Helper()
	got, ok := v.String()
	if !ok {
		t.Fatalf("string value missing; kind=%v", v.Kind())
	}
	return got
}

func requireAggregateNone(t *testing.T, v Value) {
	t.Helper()
	if v.Kind() != ValueKindNone {
		t.Fatalf("value kind=%v, want none", v.Kind())
	}
}

func aggregateGroupKey(t *testing.T, v Value) string {
	t.Helper()
	if v.Kind() == ValueKindNone {
		return "<null>"
	}
	return mustAggregateString(t, v)
}
