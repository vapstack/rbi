package rbi

import (
	"bytes"
	"log"
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

func TestAggregateRejectsUnsupportedFirstVersionShapes(t *testing.T) {
	db := openTempAggregateDB(t)
	seedAggregateTestData(t, db)

	cases := []struct {
		name string
		q    *qx.QX
	}{
		{
			name: "group_by_measure",
			q:    qx.Group("score").Metrics(qx.ROWCOUNT()),
		},
		{
			name: "distinct_with_other_metric",
			q:    qx.Aggregate(qx.DISTINCT("country"), qx.ROWCOUNT()),
		},
		{
			name: "sum_string",
			q:    qx.Aggregate(qx.SUM("country")),
		},
		{
			name: "count_distinct_nested",
			q:    qx.Aggregate(qx.COUNT(qx.DISTINCT("country"))),
		},
	}

	for i := range cases {
		_, err := db.Aggregate(cases[i].q)
		if err == nil {
			t.Fatalf("%s: expected error", cases[i].name)
		}
	}
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
