package rbi

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/vapstack/qx"
	"go.etcd.io/bbolt"
)

func openBenchAggMeasureCollection(b *testing.B) (*Collection[uint64, UserBench], *bbolt.DB, string) {
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

	c, bolt := openBoltAndCollection[uint64, UserBench](b, filepath.Join(dir, "bench_agg.db"), opts)
	return c, bolt, dir
}

func openBenchAggHybridCollection(b *testing.B) (*Collection[uint64, UserBench], *bbolt.DB, string) {
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

	c, bolt := openBoltAndCollection[uint64, UserBench](b, filepath.Join(dir, "bench_agg_hybrid.db"), opts)
	return c, bolt, dir
}

func buildBenchAggMeasureCollectionWithMode(b *testing.B, n int, mode benchCacheMode) *Collection[uint64, UserBench] {
	b.Helper()
	oneMu.Lock()
	defer oneMu.Unlock()

	key := "agg_measure/" + benchDBFamilyKey(n)
	if cached := benchDBs[key]; cached != nil && cached.clc != nil && cached.clc.state.Load()&collectionClosed == 0 {
		return cached.clc
	}

	c, bolt, dir := openBenchAggMeasureCollection(b)

	b.StopTimer()
	seedBenchData(b, c, n)
	b.StartTimer()

	benchDBs[key] = &cachedBenchUserDB{clc: c, raw: bolt, dir: dir}
	registerBenchSuiteCleanup(func() {
		_ = c.Close()
		_ = bolt.Close()
		_ = os.RemoveAll(dir)
	})
	return c
}

func buildBenchAggHybridCollectionWithMode(b *testing.B, n int, mode benchCacheMode) *Collection[uint64, UserBench] {
	b.Helper()
	oneMu.Lock()
	defer oneMu.Unlock()

	key := "agg_hybrid/" + benchDBFamilyKey(n)
	if cached := benchDBs[key]; cached != nil && cached.clc != nil && cached.clc.state.Load()&collectionClosed == 0 {
		return cached.clc
	}

	c, bolt, dir := openBenchAggHybridCollection(b)

	b.StopTimer()
	seedBenchData(b, c, n)
	b.StartTimer()

	benchDBs[key] = &cachedBenchUserDB{clc: c, raw: bolt, dir: dir}
	registerBenchSuiteCleanup(func() {
		_ = c.Close()
		_ = bolt.Close()
		_ = os.RemoveAll(dir)
	})
	return c
}

func warmBenchAggregateOnceUint64(b *testing.B, c *Collection[uint64, UserBench], q *qx.QX) {
	b.Helper()
	b.StopTimer()
	defer b.StartTimer()
	runBenchAggregateOnceUint64(b, c, q)
}

func runBenchAggregateOnceUint64(b *testing.B, c *Collection[uint64, UserBench], q *qx.QX) {
	b.Helper()
	result, err := readAggregate(c, q)
	if err != nil {
		b.Fatal(err)
	}
	if len(result.Rows) == 0 {
		b.Fatal("Aggregate returned no rows")
	}
}

func runAggregateBenchWithMode(b *testing.B, c *Collection[uint64, UserBench], q *qx.QX, mode benchCacheMode) {
	b.Helper()
	b.ReportAllocs()
	state := prepareReadBenchWithMode(
		b,
		c,
		q,
		mode,
		warmBenchAggregateOnceUint64,
		runBenchAggregateOnceUint64,
		buildUserBenchTurnoverRingUint64,
	)
	for b.Loop() {
		state.beforeQuery(b, c)
		runBenchAggregateOnceUint64(b, c, q)
	}
}

func runAggregateBenchCacheModes(b *testing.B, qf func() *qx.QX) {
	b.Helper()
	runBenchCacheModes(b, func(b *testing.B, mode benchCacheMode) {
		c := buildBenchAggMeasureCollectionWithMode(b, benchN, mode)
		runAggregateBenchWithMode(b, c, qf(), mode)
	})
}

func runAggregateHybridBenchCacheModes(b *testing.B, qf func() *qx.QX) {
	b.Helper()
	runBenchCacheModes(b, func(b *testing.B, mode benchCacheMode) {
		c := buildBenchAggHybridCollectionWithMode(b, benchN, mode)
		runAggregateBenchWithMode(b, c, qf(), mode)
	})
}

func runAggregateOrdinaryBenchCacheModes(b *testing.B, qf func() *qx.QX) {
	b.Helper()
	runBenchCacheModes(b, func(b *testing.B, mode benchCacheMode) {
		c := buildBenchCollectionWithMode(b, benchN, mode)
		runAggregateBenchWithMode(b, c, qf(), mode)
	})
}

func Benchmark_Aggregate_RowCount_Ungrouped(b *testing.B) {
	runAggregateBenchCacheModes(b, func() *qx.QX {
		return qx.Aggregate(qx.ROWCOUNT().AS("rows"))
	})
}

func Benchmark_Aggregate_RowCount_Simple_EQ(b *testing.B) {
	runAggregateOrdinaryBenchCacheModes(b, func() *qx.QX {
		return qx.Query(qx.EQ("country", "NL")).Metrics(qx.ROWCOUNT().AS("rows"))
	})
}

func Benchmark_Aggregate_RowCount_Simple_IN(b *testing.B) {
	runAggregateOrdinaryBenchCacheModes(b, func() *qx.QX {
		return qx.Query(qx.IN("country", []string{"NL", "DE", "PL"})).Metrics(qx.ROWCOUNT().AS("rows"))
	})
}

func Benchmark_Aggregate_RowCount_Simple_HASANY(b *testing.B) {
	runAggregateOrdinaryBenchCacheModes(b, func() *qx.QX {
		return qx.Query(qx.HASANY("roles", []string{"admin", "moderator"})).Metrics(qx.ROWCOUNT().AS("rows"))
	})
}

func Benchmark_Aggregate_RowCount_Simple_HASALL(b *testing.B) {
	runAggregateOrdinaryBenchCacheModes(b, func() *qx.QX {
		return qx.Query(qx.HASALL("tags", []string{"go", "db"})).Metrics(qx.ROWCOUNT().AS("rows"))
	})
}

func Benchmark_Aggregate_RowCount_Simple_NOTIN(b *testing.B) {
	runAggregateOrdinaryBenchCacheModes(b, func() *qx.QX {
		return qx.Query(qx.NOTIN("status", []string{"banned"})).Metrics(qx.ROWCOUNT().AS("rows"))
	})
}

func Benchmark_Aggregate_RowCount_Realistic_FeedEligible(b *testing.B) {
	runAggregateOrdinaryBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.EQ("status", "active"),
			qx.NOTIN("plan", []string{"free"}),
			qx.GTE("score", 120.0),
			qx.HASANY("tags", []string{"go", "security", "ops"}),
		).Metrics(qx.ROWCOUNT().AS("rows"))
	})
}

func Benchmark_Aggregate_RowCount_Realistic_ModerationQueue(b *testing.B) {
	runAggregateOrdinaryBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.OR(
				qx.AND(
					qx.EQ("status", "trial"),
					qx.HASANY("roles", []string{"moderator", "admin"}),
				),
				qx.AND(
					qx.EQ("status", "paused"),
					qx.GTE("age", 25),
				),
				qx.AND(
					qx.EQ("plan", "enterprise"),
					qx.HASANY("tags", []string{"security", "ops"}),
				),
			),
		).Metrics(qx.ROWCOUNT().AS("rows"))
	})
}

func Benchmark_Aggregate_RowCount_Realistic_Discovery_OR(b *testing.B) {
	runAggregateOrdinaryBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.OR(
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
			),
		).Metrics(qx.ROWCOUNT().AS("rows"))
	})
}

func Benchmark_Aggregate_RowCount_Realistic_DraftReview(b *testing.B) {
	runAggregateOrdinaryBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.AND(
				qx.EQ("status", "trial"),
				qx.IN("country", []string{"US", "DE", "FR", "GB"}),
				qx.NOTIN("plan", []string{"free"}),
				qx.GTE("age", 21),
				qx.HASANY("roles", []string{"admin", "moderator", "support"}),
			),
		).Metrics(qx.ROWCOUNT().AS("rows"))
	})
}

func Benchmark_Aggregate_RowCount_Realistic_SecurityAudit(b *testing.B) {
	runAggregateOrdinaryBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.AND(
				qx.HASANY("roles", []string{"admin", "support"}),
				qx.NOTIN("status", []string{"banned"}),
				qx.GTE("score", 50.0),
				qx.IN("country", []string{"US", "DE", "GB", "FR"}),
			),
		).Metrics(qx.ROWCOUNT().AS("rows"))
	})
}

func Benchmark_Aggregate_RowCount_Realistic_Cohort_Retention(b *testing.B) {
	runAggregateOrdinaryBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.AND(
				qx.NOTIN("status", []string{"banned"}),
				qx.IN("country", []string{"NL", "DE", "PL", "SE", "FR", "ES", "GB"}),
				qx.GTE("age", 25),
				qx.LTE("age", 45),
				qx.HASANY("tags", []string{"go", "db", "security"}),
				qx.GTE("score", 80.0),
			),
		).Metrics(qx.ROWCOUNT().AS("rows"))
	})
}

func Benchmark_Aggregate_RowCount_Gap_BroadPrefix_Mixed(b *testing.B) {
	runAggregateOrdinaryBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
			qx.PREFIX("email", "user"),
			qx.EQ("status", "active"),
			qx.NOTIN("plan", []string{"free"}),
		).Metrics(qx.ROWCOUNT().AS("rows"))
	})
}

func Benchmark_Aggregate_RowCount_Gap_HeavyOR_MultiBranch(b *testing.B) {
	runAggregateOrdinaryBenchCacheModes(b, func() *qx.QX {
		return qx.Query(
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
		).Metrics(qx.ROWCOUNT().AS("rows"))
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
