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

func Benchmark_Aggregate_Measure_CountSumMin_Ungrouped(b *testing.B) {
	runAggregateBenchCacheModes(b, func() *qx.QX {
		return qx.Aggregate(
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
