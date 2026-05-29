package rbi

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

	full, err := db.Aggregate(qx.Group("group").Metrics(
		qx.COUNT("score").AS("score_count"),
		qx.SUM("score").AS("score_sum"),
		qx.MIN("score").AS("score_min"),
	))
	if err != nil {
		t.Fatalf("full aggregate: %v", err)
	}
	requireAggregateLayout(t, full.Layout, []string{"group", "score_count", "score_sum", "score_min"})
	fullRows := make(map[string]Row, len(full.Rows))
	for i := range full.Rows {
		fullRows[mustAggregateString(t, full.Rows[i][0])] = full.Rows[i]
	}
	if len(fullRows) != 2 {
		t.Fatalf("full group count=%d, want 2; rows=%#v", len(fullRows), full.Rows)
	}
	requireAggregateUint(t, fullRows["a"][1], 10)
	requireAggregateInt(t, fullRows["a"][2], 100)
	requireAggregateInt(t, fullRows["a"][3], 1)
	requireAggregateUint(t, fullRows["b"][1], 10)
	requireAggregateInt(t, fullRows["b"][2], 110)
	requireAggregateInt(t, fullRows["b"][3], 2)

	selective, err := db.Aggregate(qx.Query(qx.EQ("score", 1)).Group("group").Metrics(
		qx.COUNT("score").AS("score_count"),
		qx.SUM("score").AS("score_sum"),
		qx.MIN("score").AS("score_min"),
	))
	if err != nil {
		t.Fatalf("selective aggregate: %v", err)
	}
	requireAggregateLayout(t, selective.Layout, []string{"group", "score_count", "score_sum", "score_min"})
	if len(selective.Rows) != 1 {
		t.Fatalf("selective group count=%d, want 1; rows=%#v", len(selective.Rows), selective.Rows)
	}
	requireAggregateString(t, selective.Rows[0][0], "a")
	requireAggregateUint(t, selective.Rows[0][1], 1)
	requireAggregateInt(t, selective.Rows[0][2], 1)
	requireAggregateInt(t, selective.Rows[0][3], 1)
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

func TestAggregateMeasureAppendAfterLargeSeed(t *testing.T) {
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

	stats := db2.PlannerStats()
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

/**/

func copySeededDBWithSidecars(t *testing.T, src, name string) string {
	t.Helper()

	dst := filepath.Join(t.TempDir(), name)
	copySeededFile(t, src, dst)

	sidecars, err := filepath.Glob(src + ".*.rbi")
	if err != nil {
		t.Fatalf("glob seeded sidecars: %v", err)
	}
	for _, sidecar := range sidecars {
		copySeededFile(t, sidecar, dst+sidecar[len(src):])
	}

	return dst
}

func copySeededFile(t *testing.T, src, dst string) {
	t.Helper()

	in, err := os.Open(src)
	if err != nil {
		t.Fatalf("open seeded file %q: %v", src, err)
	}
	defer func() { _ = in.Close() }()

	out, err := os.Create(dst)
	if err != nil {
		t.Fatalf("create copied file %q: %v", dst, err)
	}
	defer func() { _ = out.Close() }()

	if _, err = io.Copy(out, in); err != nil {
		t.Fatalf("copy seeded file %q: %v", src, err)
	}
}

func countByExprBitmap(t *testing.T, db *DB[uint64, Rec], expr qx.Expr) uint64 {
	t.Helper()
	ids, err := db.QueryKeys(qx.Query(expr))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	return uint64(len(ids))
}

func countByExprBitmapCountORBench(t *testing.T, db *DB[uint64, countORBenchRec], expr qx.Expr) uint64 {
	t.Helper()
	ids, err := db.QueryKeys(qx.Query(expr))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	return uint64(len(ids))
}

func TestCount_SimpleScalarLeaf_TraceUsesScalarLookupPlan(t *testing.T) {
	var events []TraceEvent
	opts := Options{
		TraceSink: func(ev TraceEvent) {
			events = append(events, ev)
		},
		TraceSampleEvery: 1,
	}
	db, _ := openTempDBUint64(t, opts)

	if err := db.Set(1, &Rec{Name: "a", Age: 20, Meta: Meta{Country: "NL"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "b", Age: 30, Meta: Meta{Country: "DE"}}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	got, err := db.Count(qx.Query(qx.EQ("country", "NL")).Filter)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if got != 1 {
		t.Fatalf("expected count=1, got %d", got)
	}
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	last := events[len(events)-1]
	if last.Plan != string(PlanCountScalarLookup) {
		t.Fatalf("expected plan %q, got %q", PlanCountScalarLookup, last.Plan)
	}
}

func TestAggregateRowCount_TraceUsesAggregateCountRoute(t *testing.T) {
	var events []TraceEvent
	opts := Options{
		TraceSink: func(ev TraceEvent) {
			events = append(events, ev)
		},
		TraceSampleEvery: 1,
	}
	db, _ := openTempDBUint64(t, opts)

	if err := db.Set(1, &Rec{Name: "a", Age: 20, Meta: Meta{Country: "NL"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "b", Age: 30, Meta: Meta{Country: "DE"}}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	result, err := db.Aggregate(qx.Query(qx.EQ("country", "NL")).Metrics(qx.ROWCOUNT().AS("rows")))
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	requireAggregateLayout(t, result.Layout, []string{"rows"})
	if len(result.Rows) != 1 {
		t.Fatalf("rows len=%d, want 1", len(result.Rows))
	}
	requireAggregateUint(t, result.Rows[0][0], 1)
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	last := events[len(events)-1]
	if last.Plan != string(PlanAggregate) {
		t.Fatalf("expected plan %q, got %q", PlanAggregate, last.Plan)
	}
	if last.AggregateRoute.Selected != "row_count" || last.AggregateRoute.FilterInput != "cardinality" {
		t.Fatalf("expected aggregate row-count route, got %+v", last.AggregateRoute)
	}
}

func TestAggregateRowCount_MaterializedCountRoute(t *testing.T) {
	var events []TraceEvent
	opts := Options{
		AnalyzeInterval: -1,
		TraceSink: func(ev TraceEvent) {
			events = append(events, ev)
		},
		TraceSampleEvery: 1,
	}
	db, _ := openTempDBUint64(t, opts)

	seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%06d@example.com", i),
			Active: i%2 == 0,
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
		qx.AND(qx.PREFIX("email", "user04"), qx.EQ("active", true)),
	)
	want := countByExprBitmap(t, db, expr)

	result, err := db.Aggregate(qx.Query(expr).Metrics(qx.ROWCOUNT().AS("rows")))
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	requireAggregateLayout(t, result.Layout, []string{"rows"})
	if len(result.Rows) != 1 {
		t.Fatalf("rows len=%d, want 1", len(result.Rows))
	}
	requireAggregateUint(t, result.Rows[0][0], want)
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	last := events[len(events)-1]
	if last.Plan != string(PlanAggregate) {
		t.Fatalf("expected plan %q, got %q", PlanAggregate, last.Plan)
	}
	if last.AggregateRoute.Selected != "row_count" || last.AggregateRoute.FilterInput != "cardinality" {
		t.Fatalf("expected aggregate row-count route, got %+v", last.AggregateRoute)
	}
}

func TestCount_ANDSetRangeUsesMaterializedRoute(t *testing.T) {
	var events []TraceEvent
	db := countOpenORBenchSharedDB(t, "test_count_and_set_range_materialized.db", Options{
		AnalyzeInterval: -1,
		TraceSink: func(ev TraceEvent) {
			events = append(events, ev)
		},
		TraceSampleEvery: 1,
	})

	expr := qx.AND(
		qx.EQ("status", "active"),
		qx.NOTIN("plan", []string{"free"}),
		qx.GTE("score", 120.0),
		qx.HASANY("tags", []string{"go", "security", "ops"}),
	)

	got, err := db.Count(expr)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	last := events[len(events)-1]
	want := countByExprBitmapCountORBench(t, db, expr)
	if got != want {
		t.Fatalf("count mismatch: got=%d want=%d", got, want)
	}
	if last.Plan != string(PlanCountMaterialized) {
		t.Fatalf("expected plan %q, got %q", PlanCountMaterialized, last.Plan)
	}
}

func TestCount_ORMaterializedRoutePromotesAfterRepeat(t *testing.T) {
	var events []TraceEvent
	db := countOpenORBenchSharedDB(t, "test_count_or_materialized_promotes.db", Options{
		AnalyzeInterval: -1,
		TraceSink: func(ev TraceEvent) {
			events = append(events, ev)
		},
		TraceSampleEvery: 1,
	})

	expr := qx.OR(
		qx.AND(
			qx.PREFIX("email", "user1"),
			qx.EQ("status", "active"),
			qx.GTE("score", 60.0),
		),
		qx.AND(
			qx.EQ("country", "DE"),
			qx.HASANY("tags", []string{"rust", "go"}),
			qx.GTE("age", 24),
		),
		qx.AND(
			qx.EQ("plan", "enterprise"),
			qx.HASANY("roles", []string{"admin", "support"}),
			qx.NOTIN("status", []string{"banned"}),
		),
	)

	first, err := db.Count(expr)
	if err != nil {
		t.Fatalf("first Count: %v", err)
	}
	if len(events) == 0 {
		t.Fatalf("expected first trace event")
	}
	if plan := events[len(events)-1].Plan; plan != string(PlanCountORPredicates) {
		t.Fatalf("expected first plan %q, got %q", PlanCountORPredicates, plan)
	}

	second, err := db.Count(expr)
	if err != nil {
		t.Fatalf("second Count: %v", err)
	}
	if second != first {
		t.Fatalf("count mismatch between repeated runs: first=%d second=%d", first, second)
	}
	if plan := events[len(events)-1].Plan; plan != string(PlanCountMaterialized) {
		t.Fatalf("expected second plan %q, got %q", PlanCountMaterialized, plan)
	}

	want := countByExprBitmapCountORBench(t, db, expr)
	if second != want {
		t.Fatalf("count mismatch: got=%d want=%d", second, want)
	}
}

func TestAggregateRowCount_MatchesCountForRepresentativeFilters(t *testing.T) {
	opts := benchOptions()
	opts.AnalyzeInterval = -1

	path := filepath.Join(t.TempDir(), "rowcount_equiv.db")
	db, raw := openBoltAndNew[uint64, UserBench](t, path, opts)
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	seedBenchData(t, db, 150_000)

	cases := []struct {
		name string
		q    *qx.QX
		all  bool
	}{
		{name: "NoFilter", all: true},
		{name: "SimpleEQ", q: qx.Query(qx.EQ("country", "NL"))},
		{name: "SimpleIN", q: qx.Query(qx.IN("country", []string{"NL", "DE", "PL"}))},
		{name: "SimpleHASANY", q: qx.Query(qx.HASANY("roles", []string{"admin", "moderator"}))},
		{name: "SimpleHASALL", q: qx.Query(qx.HASALL("tags", []string{"go", "db"}))},
		{name: "SimpleNOTIN", q: qx.Query(qx.NOTIN("status", []string{"banned"}))},
		{name: "BroadMixedAND", q: qx.Query(
			qx.PREFIX("email", "user"),
			qx.EQ("status", "active"),
			qx.NOTIN("plan", []string{"free"}),
		)},
		{name: "FeedEligible", q: qx.Query(
			qx.EQ("status", "active"),
			qx.NOTIN("plan", []string{"free"}),
			qx.GTE("score", 120.0),
			qx.HASANY("tags", []string{"go", "security", "ops"}),
		)},
		{name: "HeavyOR", q: qx.Query(
			qx.OR(
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
			),
		)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var (
				count uint64
				err   error
				aggQ  *qx.QX
			)
			if tc.all {
				count, err = db.Count()
				aggQ = qx.Aggregate(qx.ROWCOUNT().AS("rows"))
			} else {
				count, err = db.Count(tc.q.Filter)
				aggQ = qx.Query(tc.q.Filter).Metrics(qx.ROWCOUNT().AS("rows"))
			}
			if err != nil {
				t.Fatalf("Count: %v", err)
			}
			result, err := db.Aggregate(aggQ)
			if err != nil {
				t.Fatalf("Aggregate: %v", err)
			}
			requireAggregateLayout(t, result.Layout, []string{"rows"})
			if len(result.Rows) != 1 {
				t.Fatalf("rows len=%d, want 1", len(result.Rows))
			}
			requireAggregateUint(t, result.Rows[0][0], count)
		})
	}
}

func TestAggregateRowCount_MatchesCountForSmallWorldFilters(t *testing.T) {
	exprs := smallWorldExprCases()

	for _, world := range smallWorldCases() {
		t.Run(world.name, func(t *testing.T) {
			db := openSmallWorldDB(t, world)

			for _, exprCase := range exprs {
				t.Run(exprCase.name, func(t *testing.T) {
					var (
						count uint64
						err   error
						aggQ  *qx.QX
					)
					if exprCase.noFilter {
						count, err = db.Count()
						aggQ = qx.Aggregate(qx.ROWCOUNT().AS("rows"))
					} else {
						count, err = db.Count(exprCase.expr)
						aggQ = qx.Query(exprCase.expr).Metrics(qx.ROWCOUNT().AS("rows"))
					}
					if err != nil {
						t.Fatalf("Count: %v", err)
					}
					result, err := db.Aggregate(aggQ)
					if err != nil {
						t.Fatalf("Aggregate: %v", err)
					}
					requireAggregateLayout(t, result.Layout, []string{"rows"})
					if len(result.Rows) != 1 {
						t.Fatalf("rows len=%d, want 1", len(result.Rows))
					}
					requireAggregateUint(t, result.Rows[0][0], count)
				})
			}
		})
	}
}

func TestCount_PublicRoutesAllocsPerRunStayZeroAfterWarmup(t *testing.T) {
	if testRaceEnabled {
		t.Skip("testing.AllocsPerRun is not stable under -race")
	}

	opts := benchOptions()
	opts.AnalyzeInterval = -1

	path := filepath.Join(t.TempDir(), "count_public_allocs.db")
	db, raw := openBoltAndNew[uint64, UserBench](t, path, opts)
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	seedBenchData(t, db, 150_000)

	cases := []struct {
		name string
		q    *qx.QX
	}{
		{name: "SimpleEQ", q: qx.Query(qx.EQ("country", "NL"))},
		{name: "SimpleIN", q: qx.Query(qx.IN("country", []string{"NL", "DE", "PL"}))},
		{name: "SimpleHASANY", q: qx.Query(qx.HASANY("roles", []string{"admin", "moderator"}))},
		{name: "SimpleNOTIN", q: qx.Query(qx.NOTIN("status", []string{"banned"}))},
		{name: "BroadMixedAND", q: qx.Query(
			qx.PREFIX("email", "user"),
			qx.EQ("status", "active"),
			qx.NOTIN("plan", []string{"free"}),
		)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			run := func() {
				if _, err := db.Count(tc.q.Filter); err != nil {
					t.Fatalf("Count: %v", err)
				}
			}
			run()
			allocs := testing.AllocsPerRun(50, run)
			if allocs != 0 {
				t.Fatalf("unexpected allocs after warmup: got=%v want=0", allocs)
			}
		})
	}

	orDB, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, orDB, 20_000)
	orQ := qx.Query(qx.OR(
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
	))
	t.Run("ORHybrid", func(t *testing.T) {
		run := func() {
			if _, err := orDB.Count(orQ.Filter); err != nil {
				t.Fatalf("Count: %v", err)
			}
		}
		run()
		allocs := testing.AllocsPerRun(50, run)
		if allocs != 0 {
			t.Fatalf("unexpected allocs after warmup: got=%v want=0", allocs)
		}
	})
}

type countORBenchRec struct {
	Country string   `db:"country" rbi:"index"`
	Plan    string   `db:"plan"    rbi:"index"`
	Status  string   `db:"status"  rbi:"index"`
	Age     int      `db:"age"     rbi:"index"`
	Score   float64  `db:"score"   rbi:"index"`
	Email   string   `db:"email"   rbi:"index"`
	Tags    []string `db:"tags"    rbi:"index"`
	Roles   []string `db:"roles"   rbi:"index"`
}

type seededDBFixture struct {
	once sync.Once
	path string
	err  error
}

var countORBenchSharedSeeded seededDBFixture

func seedCountORBenchData(t *testing.T, db *DB[uint64, countORBenchRec], n int) {
	t.Helper()

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
}

func countORBenchSharedSeedPath(t *testing.T) string {
	t.Helper()

	countORBenchSharedSeeded.once.Do(func() {
		dir, err := os.MkdirTemp("", "rbi-count-or-shared-")
		if err != nil {
			countORBenchSharedSeeded.err = err
			return
		}

		path := filepath.Join(dir, "seed.db")
		db, raw := openBoltAndNew[uint64, countORBenchRec](t, path, Options{AnalyzeInterval: -1})
		seedCountORBenchData(t, db, 128_000)
		if err = db.Close(); err != nil {
			countORBenchSharedSeeded.err = err
			_ = raw.Close()
			return
		}
		if err = raw.Close(); err != nil {
			countORBenchSharedSeeded.err = err
			return
		}
		countORBenchSharedSeeded.path = path
	})

	if countORBenchSharedSeeded.err != nil {
		t.Fatalf("count or shared seeded fixture: %v", countORBenchSharedSeeded.err)
	}
	return countORBenchSharedSeeded.path
}

func countOpenORBenchSharedDB(t *testing.T, name string, opts Options) *DB[uint64, countORBenchRec] {
	t.Helper()

	path := copySeededDBWithSidecars(t, countORBenchSharedSeedPath(t), name)
	db, raw := openBoltAndNew[uint64, countORBenchRec](t, path, opts)
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	return db
}

func TestCount_ScalarInSplit_MixedResiduals_UsesPlan(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	sink := func(ev TraceEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	db := countOpenORBenchSharedDB(t, "test_count_scalar_in_split_mixed.db", Options{
		AnalyzeInterval:  -1,
		TraceSink:        sink,
		TraceSampleEvery: 1,
	})

	q := qx.Query(
		qx.IN("country", []string{"US", "DE", "NL"}),
		qx.NOTIN("status", []string{"banned"}),
		qx.HASANY("tags", []string{"security", "ops"}),
		qx.GTE("age", 24),
		qx.GTE("score", 60.0),
	)

	got, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	mu.Lock()
	if len(events) == 0 {
		mu.Unlock()
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	mu.Unlock()
	want := countByExprBitmapCountORBench(t, db, q.Filter)
	if got != want {
		t.Fatalf("count mismatch: got=%d want=%d", got, want)
	}
	if ev.Plan != string(PlanCountScalarInSplit) {
		t.Fatalf("expected %q, got %q", PlanCountScalarInSplit, ev.Plan)
	}
}

func TestCount_ScalarInSplit_CohortShape_MatchesBitmap(t *testing.T) {
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
	db, raw := openBoltAndNew[uint64, countORBenchRec](t, filepath.Join(dir, "test_count_scalar_in_split_cohort.db"), Options{
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

	countries := []string{"NL", "DE", "PL", "SE", "FR", "ES", "GB", "US"}
	statuses := []string{"active", "trial", "paused", "banned"}
	tagsPool := [][]string{
		{"go", "db"},
		{"security", "ops"},
		{"go", "security"},
		{"rust", "ops"},
		{"db", "security"},
	}

	const n = 128_000
	ids := make([]uint64, n)
	vals := make([]*countORBenchRec, n)
	for i := 0; i < n; i++ {
		ids[i] = uint64(i + 1)
		vals[i] = &countORBenchRec{
			Country: countries[i%len(countries)],
			Status:  statuses[(i/4)%len(statuses)],
			Age:     18 + (i % 55),
			Score:   float64(i % 120),
			Email:   fmt.Sprintf("user%06d@example.com", i),
			Tags:    append([]string(nil), tagsPool[i%len(tagsPool)]...),
		}
	}
	if err := db.BatchSet(ids, vals); err != nil {
		t.Fatalf("BatchSet: %v", err)
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.NOTIN("status", []string{"banned"}),
		qx.IN("country", []string{"NL", "DE", "PL", "SE", "FR", "ES", "GB"}),
		qx.GTE("age", 25),
		qx.LTE("age", 45),
		qx.HASANY("tags", []string{"go", "db", "security"}),
		qx.GTE("score", 80.0),
	)

	got, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	mu.Lock()
	if len(events) == 0 {
		mu.Unlock()
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	mu.Unlock()
	want := countByExprBitmapCountORBench(t, db, q.Filter)
	if got != want {
		t.Fatalf("count mismatch: got=%d want=%d", got, want)
	}
	if ev.Plan != string(PlanCountScalarInSplit) {
		t.Fatalf("expected %q, got %q", PlanCountScalarInSplit, ev.Plan)
	}
}
