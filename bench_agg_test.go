package rbi

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/vapstack/qx"
	"go.etcd.io/bbolt"
)

func openBenchAggMeasureDB(b *testing.B) (*DB[uint64, UserBench], *bbolt.DB, string) {
	b.Helper()
	dir, err := os.MkdirTemp("", "rbi-bench-agg-*")
	if err != nil {
		b.Fatalf("os.MkdirTemp: %v", err)
	}

	opts := benchOptions()
	opts.Index = map[string]IndexKind{
		"country": IndexDefault,
		"plan":    IndexDefault,
		"status":  IndexDefault,
		"score":   IndexMeasure,
	}

	db, raw := openBoltAndNew[uint64, UserBench](b, filepath.Join(dir, "bench_agg.db"), opts)
	return db, raw, dir
}

func openBenchAggHybridDB(b *testing.B) (*DB[uint64, UserBench], *bbolt.DB, string) {
	b.Helper()
	dir, err := os.MkdirTemp("", "rbi-bench-agg-hybrid-*")
	if err != nil {
		b.Fatalf("os.MkdirTemp: %v", err)
	}

	opts := benchOptions()
	opts.Index = map[string]IndexKind{
		"country": IndexDefault,
		"plan":    IndexDefault,
		"status":  IndexDefault,
		"age":     IndexDefault,
		"score":   IndexMeasure,
	}

	db, raw := openBoltAndNew[uint64, UserBench](b, filepath.Join(dir, "bench_agg_hybrid.db"), opts)
	return db, raw, dir
}

func buildBenchAggMeasureDBWithMode(b *testing.B, n int, mode benchCacheMode) *DB[uint64, UserBench] {
	b.Helper()
	oneMu.Lock()
	defer oneMu.Unlock()

	key := "agg_measure/" + benchDBFamilyKey(n)
	if cached := benchDBs[key]; cached != nil && cached.db != nil && !cached.db.closed.Load() {
		return cached.db
	}

	db, raw, dir := openBenchAggMeasureDB(b)

	b.StopTimer()
	seedBenchData(b, db, n)
	b.StartTimer()

	benchDBs[key] = &cachedBenchUserDB{db: db, raw: raw, dir: dir}
	registerBenchSuiteCleanup(func() {
		_ = db.Close()
		_ = raw.Close()
		_ = os.RemoveAll(dir)
	})
	return db
}

func buildBenchAggHybridDBWithMode(b *testing.B, n int, mode benchCacheMode) *DB[uint64, UserBench] {
	b.Helper()
	oneMu.Lock()
	defer oneMu.Unlock()

	key := "agg_hybrid/" + benchDBFamilyKey(n)
	if cached := benchDBs[key]; cached != nil && cached.db != nil && !cached.db.closed.Load() {
		return cached.db
	}

	db, raw, dir := openBenchAggHybridDB(b)

	b.StopTimer()
	seedBenchData(b, db, n)
	b.StartTimer()

	benchDBs[key] = &cachedBenchUserDB{db: db, raw: raw, dir: dir}
	registerBenchSuiteCleanup(func() {
		_ = db.Close()
		_ = raw.Close()
		_ = os.RemoveAll(dir)
	})
	return db
}

func warmBenchAggregateOnceUint64(b *testing.B, db *DB[uint64, UserBench], q *qx.QX) {
	b.Helper()
	b.StopTimer()
	defer b.StartTimer()
	runBenchAggregateOnceUint64(b, db, q)
}

func runBenchAggregateOnceUint64(b *testing.B, db *DB[uint64, UserBench], q *qx.QX) {
	b.Helper()
	result, err := db.Aggregate(q)
	if err != nil {
		b.Fatal(err)
	}
	if len(result.Rows) == 0 {
		b.Fatal("Aggregate returned no rows")
	}
}

func runAggregateBenchWithMode(b *testing.B, db *DB[uint64, UserBench], q *qx.QX, mode benchCacheMode) {
	b.Helper()
	b.ReportAllocs()
	state := prepareReadBenchWithMode(
		b,
		db,
		q,
		mode,
		warmBenchAggregateOnceUint64,
		runBenchAggregateOnceUint64,
		buildUserBenchTurnoverRingUint64,
	)
	for b.Loop() {
		state.beforeQuery(b, db)
		runBenchAggregateOnceUint64(b, db, q)
	}
}

func runAggregateBenchCacheModes(b *testing.B, qf func() *qx.QX) {
	b.Helper()
	runBenchCacheModes(b, func(b *testing.B, mode benchCacheMode) {
		db := buildBenchAggMeasureDBWithMode(b, benchN, mode)
		runAggregateBenchWithMode(b, db, qf(), mode)
	})
}

func runAggregateHybridBenchCacheModes(b *testing.B, qf func() *qx.QX) {
	b.Helper()
	runBenchCacheModes(b, func(b *testing.B, mode benchCacheMode) {
		db := buildBenchAggHybridDBWithMode(b, benchN, mode)
		runAggregateBenchWithMode(b, db, qf(), mode)
	})
}

func runAggregateOrdinaryBenchCacheModes(b *testing.B, qf func() *qx.QX) {
	b.Helper()
	runBenchCacheModes(b, func(b *testing.B, mode benchCacheMode) {
		db := buildBenchDBWithMode(b, benchN, mode)
		runAggregateBenchWithMode(b, db, qf(), mode)
	})
}

func Benchmark_Aggregate_RowCount_Ungrouped(b *testing.B) {
	runAggregateBenchCacheModes(b, func() *qx.QX {
		return qx.Aggregate(qx.ROWCOUNT().AS("rows"))
	})
}

func Benchmark_Aggregate_GroupOnly_Group2(b *testing.B) {
	runAggregateBenchCacheModes(b, func() *qx.QX {
		return qx.Group("country", "status")
	})
}

func Benchmark_Aggregate_Distinct_Country(b *testing.B) {
	runAggregateBenchCacheModes(b, func() *qx.QX {
		return qx.Aggregate(qx.DISTINCT("country").AS("country"))
	})
}

func Benchmark_Aggregate_CountDistinct_Country_Ungrouped(b *testing.B) {
	runAggregateBenchCacheModes(b, func() *qx.QX {
		return qx.Aggregate(qx.COUNT(qx.DISTINCT("country")).AS("country_count"))
	})
}

func Benchmark_Aggregate_CountDistinct_Country_Group1(b *testing.B) {
	runAggregateBenchCacheModes(b, func() *qx.QX {
		return qx.Group("status").Metrics(qx.COUNT(qx.DISTINCT("country")).AS("country_count"))
	})
}

func Benchmark_Aggregate_OrdinaryLowCard_CountSumMin_Ungrouped(b *testing.B) {
	runAggregateOrdinaryBenchCacheModes(b, func() *qx.QX {
		return qx.Aggregate(
			qx.COUNT("age").AS("age_count"),
			qx.SUM("age").AS("age_sum"),
			qx.MIN("age").AS("age_min"),
		)
	})
}

func Benchmark_Aggregate_OrdinaryHighCard_CountSumMin_Ungrouped(b *testing.B) {
	runAggregateOrdinaryBenchCacheModes(b, func() *qx.QX {
		return qx.Aggregate(
			qx.COUNT("score").AS("score_count"),
			qx.SUM("score").AS("score_sum"),
			qx.MIN("score").AS("score_min"),
		)
	})
}

func Benchmark_Aggregate_OrdinaryHighCard_CountSumMin_Group1(b *testing.B) {
	runAggregateOrdinaryBenchCacheModes(b, func() *qx.QX {
		return qx.Group("country").Metrics(
			qx.COUNT("score").AS("score_count"),
			qx.SUM("score").AS("score_sum"),
			qx.MIN("score").AS("score_min"),
		)
	})
}

func Benchmark_Aggregate_OrdinaryHighCard_CountSumMin_Group2(b *testing.B) {
	runAggregateOrdinaryBenchCacheModes(b, func() *qx.QX {
		return qx.Group("country", "status").Metrics(
			qx.COUNT("score").AS("score_count"),
			qx.SUM("score").AS("score_sum"),
			qx.MIN("score").AS("score_min"),
		)
	})
}

func Benchmark_Aggregate_OrdinaryHighCard_DistinctScore(b *testing.B) {
	runAggregateOrdinaryBenchCacheModes(b, func() *qx.QX {
		return qx.Aggregate(qx.DISTINCT("score").AS("score"))
	})
}

func Benchmark_Aggregate_Measure_CountOnly_Ungrouped(b *testing.B) {
	runAggregateBenchCacheModes(b, func() *qx.QX {
		return qx.Aggregate(qx.COUNT("score").AS("score_count"))
	})
}

func Benchmark_Aggregate_Measure_CountSumMin_Ungrouped(b *testing.B) {
	runAggregateBenchCacheModes(b, func() *qx.QX {
		return qx.Aggregate(
			qx.COUNT("score").AS("score_count"),
			qx.SUM("score").AS("score_sum"),
			qx.MIN("score").AS("score_min"),
		)
	})
}

func Benchmark_Aggregate_Measure_CountSumAvgMinMax_Ungrouped(b *testing.B) {
	runAggregateBenchCacheModes(b, func() *qx.QX {
		return qx.Aggregate(
			qx.COUNT("score").AS("score_count"),
			qx.SUM("score").AS("score_sum"),
			qx.AVG("score").AS("score_avg"),
			qx.MIN("score").AS("score_min"),
			qx.MAX("score").AS("score_max"),
		)
	})
}

func Benchmark_Aggregate_Measure_CountSumMin_FilterBroadMerge(b *testing.B) {
	runAggregateBenchCacheModes(b, func() *qx.QX {
		return qx.Query(qx.EQ("status", "active")).Metrics(
			qx.COUNT("score").AS("score_count"),
			qx.SUM("score").AS("score_sum"),
			qx.MIN("score").AS("score_min"),
		)
	})
}

func Benchmark_Aggregate_Measure_CountSumMin_FilterSelectiveLookup(b *testing.B) {
	runAggregateBenchCacheModes(b, func() *qx.QX {
		return qx.Query(qx.EQ("country", "US"), qx.EQ("status", "active")).Metrics(
			qx.COUNT("score").AS("score_count"),
			qx.SUM("score").AS("score_sum"),
			qx.MIN("score").AS("score_min"),
		)
	})
}

func Benchmark_Aggregate_Measure_CountSumMin_Group1(b *testing.B) {
	runAggregateBenchCacheModes(b, func() *qx.QX {
		return qx.Group("country").Metrics(
			qx.COUNT("score").AS("score_count"),
			qx.SUM("score").AS("score_sum"),
			qx.MIN("score").AS("score_min"),
		)
	})
}

func Benchmark_Aggregate_Measure_CountSumMin_Group2(b *testing.B) {
	runAggregateBenchCacheModes(b, func() *qx.QX {
		return qx.Group("country", "status").Metrics(
			qx.COUNT("score").AS("score_count"),
			qx.SUM("score").AS("score_sum"),
			qx.MIN("score").AS("score_min"),
		)
	})
}

func Benchmark_Aggregate_Measure_CountSumMin_Group2_FilterSelective(b *testing.B) {
	runAggregateBenchCacheModes(b, func() *qx.QX {
		return qx.Query(qx.EQ("country", "US"), qx.EQ("status", "active")).
			Group("country", "status").
			Metrics(
				qx.COUNT("score").AS("score_count"),
				qx.SUM("score").AS("score_sum"),
				qx.MIN("score").AS("score_min"),
			)
	})
}

func Benchmark_Aggregate_Hybrid_MeasureOrdinary_CountSumMin_Ungrouped(b *testing.B) {
	runAggregateHybridBenchCacheModes(b, func() *qx.QX {
		return qx.Aggregate(
			qx.COUNT("score").AS("score_count"),
			qx.SUM("score").AS("score_sum"),
			qx.MIN("score").AS("score_min"),
			qx.COUNT("age").AS("age_count"),
			qx.SUM("age").AS("age_sum"),
			qx.MIN("age").AS("age_min"),
		)
	})
}

func Benchmark_Aggregate_Hybrid_MeasureOrdinary_CountSumMin_Group2(b *testing.B) {
	runAggregateHybridBenchCacheModes(b, func() *qx.QX {
		return qx.Group("country", "status").Metrics(
			qx.COUNT("score").AS("score_count"),
			qx.SUM("score").AS("score_sum"),
			qx.MIN("score").AS("score_min"),
			qx.COUNT("age").AS("age_count"),
			qx.SUM("age").AS("age_sum"),
			qx.MIN("age").AS("age_min"),
		)
	})
}

func Benchmark_Aggregate_Hybrid_MeasureOrdinary_CountSumMin_Group2_FilterSelective(b *testing.B) {
	runAggregateHybridBenchCacheModes(b, func() *qx.QX {
		return qx.Query(qx.EQ("country", "US"), qx.EQ("status", "active")).
			Group("country", "status").
			Metrics(
				qx.COUNT("score").AS("score_count"),
				qx.SUM("score").AS("score_sum"),
				qx.MIN("score").AS("score_min"),
				qx.COUNT("age").AS("age_count"),
				qx.SUM("age").AS("age_sum"),
				qx.MIN("age").AS("age_min"),
			)
	})
}

func Benchmark_Aggregate_Measure_CountSumMin_Group2_Having(b *testing.B) {
	runAggregateBenchCacheModes(b, func() *qx.QX {
		return qx.Group("country", "status").Metrics(
			qx.COUNT("score").AS("score_count"),
			qx.SUM("score").AS("score_sum"),
			qx.MIN("score").AS("score_min"),
		).Having(
			qx.GTE(qx.OUT("score_count"), 10_000),
			qx.GT(qx.OUT("score_sum"), 5_000_000.0),
		)
	})
}

func Benchmark_Aggregate_Measure_CountSumMin_Group2_OrderMultiLimit(b *testing.B) {
	runAggregateBenchCacheModes(b, func() *qx.QX {
		return qx.Group("country", "status").Metrics(
			qx.COUNT("score").AS("score_count"),
			qx.SUM("score").AS("score_sum"),
			qx.MIN("score").AS("score_min"),
		).
			SortOut("score_sum", qx.DESC).
			SortOut("score_count", qx.DESC).
			SortOut("country").
			SortOut("status").
			Limit(16)
	})
}
