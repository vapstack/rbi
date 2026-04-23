package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi"
)

type focusedReadBenchCase struct {
	name       string
	classAlias string
	build      func(now int64) *qx.QX
	run        func(db *rbi.DB[uint64, UserBench], q *qx.QX) error
}

var focusedReadBenchWorkers = []int{1, 8, 32, 100}

var focusedReadBenchCases = []focusedReadBenchCase{
	{
		name:       "read_active_region_pro_items",
		classAlias: "r_med",
		build: func(now int64) *qx.QX {
			return qx.Query(
				qx.AND(
					qx.EQ("status", "active"),
					qx.GTE("last_login", now-72*3600),
					qx.IN("country", []string{"US", "DE"}),
					qx.NOTIN("plan", []string{"free"}),
				),
			).Sort("last_login", qx.DESC).Limit(40)
		},
		run: runQueryItemsBench,
	},
	{
		name:       "read_leaderboard_top_items",
		classAlias: "r_med",
		build: func(now int64) *qx.QX {
			return qx.Query(
				qx.AND(
					qx.EQ("status", "active"),
					qx.GTE("score", 250.0),
					qx.GTE("last_login", now-180*24*3600),
				),
			).Sort("score", qx.DESC).Limit(50)
		},
		run: runQueryItemsBench,
	},
	{
		name:       "read_signup_dashboard_count",
		classAlias: "r_med",
		build: func(now int64) *qx.QX {
			return qx.Query(
				qx.AND(
					qx.GTE("created_at", now-21*24*3600),
					qx.EQ("is_verified", true),
					qx.IN("country", []string{"US", "DE"}),
					qx.NOTIN("status", []string{"banned"}),
				),
			)
		},
		run: runCountBenchQuery,
	},
	{
		name:       "read_frontpage_candidate_keys",
		classAlias: "r_meh",
		build: func(now int64) *qx.QX {
			return qx.Query(
				qx.AND(
					qx.EQ("status", "active"),
					qx.HASANY("tags", []string{"technology", "programming", "databases"}),
					qx.GTE("score", 120.0),
					qx.GTE("last_login", now-45*24*3600),
				),
			).Sort("score", qx.DESC).Limit(100)
		},
		run: runQueryKeysBenchQuery,
	},
	{
		name:       "read_moderation_queue_keys",
		classAlias: "r_meh",
		build: func(now int64) *qx.QX {
			return qx.Query(
				qx.OR(
					qx.EQ("status", "pending"),
					qx.EQ("status", "suspended"),
					qx.AND(
						qx.EQ("status", "active"),
						qx.HASANY("tags", []string{"politics", "cryptocurrency"}),
						qx.GTE("score", 4000.0),
						qx.GTE("created_at", now-180*24*3600),
					),
				),
			).Sort("created_at", qx.DESC).Limit(100)
		},
		run: runQueryKeysBenchQuery,
	},
	{
		name:       "read_staff_audit_feed_keys",
		classAlias: "r_meh",
		build: func(now int64) *qx.QX {
			return qx.Query(
				qx.AND(
					qx.HASANY("roles", []string{"moderator", "admin", "staff", "bot"}),
					qx.GTE("last_login", now-120*24*3600),
					qx.NOTIN("status", []string{"banned"}),
					qx.EQ("country", "US"),
				),
			).Sort("last_login", qx.DESC).Limit(120)
		},
		run: runQueryKeysBenchQuery,
	},
	{
		name:       "read_discovery_explore_keys",
		classAlias: "r_hvy",
		build: func(now int64) *qx.QX {
			return qx.Query(
				qx.OR(
					qx.AND(
						qx.EQ("status", "active"),
						qx.HASANY("tags", []string{"technology", "gaming"}),
						qx.GTE("score", 320.0),
						qx.GTE("last_login", now-30*24*3600),
					),
					qx.AND(
						qx.EQ("country", "US"),
						qx.EQ("is_verified", true),
						qx.GTE("created_at", now-180*24*3600),
						qx.GTE("score", 180.0),
					),
					qx.AND(
						qx.IN("plan", []string{"pro", "enterprise"}),
						qx.HASANY("roles", []string{"moderator", "admin", "staff"}),
						qx.GTE("last_login", now-14*24*3600),
					),
				),
			).Sort("created_at", qx.DESC).Limit(150)
		},
		run: runQueryKeysBenchQuery,
	},
	{
		name:       "read_dormant_archive_page_keys",
		classAlias: "r_hvy",
		build: func(now int64) *qx.QX {
			return qx.Query(
				qx.AND(
					qx.NOTIN("status", []string{"banned"}),
					qx.LT("last_login", now-180*24*3600),
					qx.NOTIN("plan", []string{"enterprise"}),
					qx.LT("score", 180.0),
				),
			).Sort("last_login", qx.ASC).Offset(2500).Limit(100)
		},
		run: runQueryKeysBenchQuery,
	},
	{
		name:       "read_inactive_cleanup_keys",
		classAlias: "r_hvy",
		build: func(now int64) *qx.QX {
			return qx.Query(
				qx.AND(
					qx.LT("last_login", now-365*24*3600),
					qx.NOTIN("status", []string{"banned"}),
					qx.NOTIN("plan", []string{"pro", "enterprise"}),
					qx.LT("score", 120.0),
				),
			).Limit(250)
		},
		run: runQueryKeysBenchQuery,
	},
}

type focusedRoutingMode struct {
	name        string
	multipliers map[string]float64
}

var focusedRoutingModes = []focusedRoutingMode{
	{name: "default"},
	{
		name: "ordered_bias",
		multipliers: map[string]float64{
			string(rbi.PlanOrdered):          0.4,
			string(rbi.PlanLimitOrderBasic):  4.0,
			string(rbi.PlanLimitOrderPrefix): 4.0,
		},
	},
}

var focusedRoutingQueryNames = []string{
	"read_active_region_pro_items",
	"read_leaderboard_top_items",
	"read_frontpage_candidate_keys",
	"read_moderation_queue_keys",
	"read_discovery_explore_keys",
}

func BenchmarkStressFocusedReadPerf(b *testing.B) {
	runFocusedReadBenchmarks(b, false)
}

func BenchmarkStressFocusedReadTrace(b *testing.B) {
	runFocusedReadBenchmarks(b, true)
}

func BenchmarkStressFocusedReadRoutingTrace(b *testing.B) {
	for _, queryName := range focusedRoutingQueryNames {
		benchCase, ok := focusedReadBenchCaseByName(queryName)
		if !ok {
			b.Fatalf("missing focused bench case %q", queryName)
		}
		benchCaseCopy := benchCase
		for _, mode := range focusedRoutingModes {
			mode := mode
			b.Run(queryName+"/"+mode.name, func(b *testing.B) {
				runFocusedReadRoutingCase(b, benchCaseCopy, mode)
			})
		}
	}
}

func runFocusedReadBenchmarks(b *testing.B, withTrace bool) {
	catalog, _, byName, err := loadClassCatalog()
	if err != nil {
		b.Fatalf("load stress catalog: %v", err)
	}
	_ = catalog

	for _, benchCase := range focusedReadBenchCases {
		benchCase := benchCase
		desc := byName[benchCase.classAlias]
		if desc == nil {
			b.Fatalf("missing class descriptor for %q", benchCase.classAlias)
		}
		b.Run(benchCase.name, func(b *testing.B) {
			for _, workers := range focusedReadBenchWorkers {
				workers := workers
				b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
					runFocusedReadBenchmarkCase(b, desc, benchCase, workers, withTrace)
				})
			}
		})
	}
}

func focusedReadBenchCaseByName(name string) (focusedReadBenchCase, bool) {
	for _, benchCase := range focusedReadBenchCases {
		if benchCase.name == name {
			return benchCase, true
		}
	}
	return focusedReadBenchCase{}, false
}

func runFocusedReadRoutingCase(b *testing.B, benchCase focusedReadBenchCase, mode focusedRoutingMode) {
	catalog, _, byName, err := loadClassCatalog()
	if err != nil {
		b.Fatalf("load stress catalog: %v", err)
	}
	_ = catalog
	desc := byName[benchCase.classAlias]
	if desc == nil {
		b.Fatalf("missing class descriptor for %q", benchCase.classAlias)
	}

	handle, collector, epoch := openFocusedBenchHandle(b, true)
	applyFocusedCalibration(b, handle.DB, mode)
	now := time.Now().Unix()
	warmFocusedReadBenchmarkCase(b, handle, desc, benchCase, now, true)

	runtime.GC()
	b.ReportAllocs()
	b.ResetTimer()
	runFocusedReadWorkers(b, handle, collector, desc, benchCase, 1, now, true)
	b.StopTimer()

	if epoch != nil {
		report := epoch.snapshot().queryReport(desc.Info.Name, benchCase.name)
		if report != nil {
			b.Logf("routing_mode=%s plan_counts=%v", mode.name, report.PlanCounts)
		}
		reportFocusedReadTraceMetrics(b, report)
	}
}

func applyFocusedCalibration(b *testing.B, db *rbi.DB[uint64, UserBench], mode focusedRoutingMode) {
	b.Helper()

	snap := rbi.CalibrationSnapshot{
		UpdatedAt:   time.Now(),
		Multipliers: map[string]float64{},
		Samples:     map[string]uint64{},
	}
	for _, name := range []string{
		string(rbi.PlanOrdered),
		string(rbi.PlanLimitOrderBasic),
		string(rbi.PlanLimitOrderPrefix),
	} {
		snap.Multipliers[name] = 1.0
		snap.Samples[name] = 1
	}
	for name, value := range mode.multipliers {
		snap.Multipliers[name] = value
		snap.Samples[name] = 1
	}
	if err := db.SetCalibrationSnapshot(snap); err != nil {
		b.Fatalf("SetCalibrationSnapshot(%s): %v", mode.name, err)
	}
}

func runFocusedReadBenchmarkCase(b *testing.B, desc *classDescriptor, benchCase focusedReadBenchCase, workers int, withTrace bool) {
	handle, collector, epoch := openFocusedBenchHandle(b, withTrace)
	now := time.Now().Unix()
	warmFocusedReadBenchmarkCase(b, handle, desc, benchCase, now, withTrace)

	runtime.GC()
	b.ReportAllocs()
	b.ResetTimer()
	runFocusedReadWorkers(b, handle, collector, desc, benchCase, workers, now, withTrace)
	b.StopTimer()

	if withTrace && epoch != nil {
		reportFocusedReadTraceMetrics(b, epoch.snapshot().queryReport(desc.Info.Name, benchCase.name))
	}
}

func openFocusedBenchHandle(b *testing.B, withTrace bool) (*DBHandle, *plannerTraceCollector, *plannerTraceEpoch) {
	b.Helper()

	var collector *plannerTraceCollector
	var epoch *plannerTraceEpoch
	cfg := DBConfig{
		DBFile:           focusedBenchDBPath(b),
		AnalyzeInterval:  -1,
		CalibrationOn:    true,
		CalibrationEvery: -1,
	}
	if withTrace {
		collector = newPlannerTraceCollector(nil, 1, 8)
		epoch = collector.newEpoch()
		cfg.TraceSink = collector.traceSink()
		cfg.TraceSampleEvery = 1
	}

	handle, err := OpenBenchDB(cfg, 0)
	if err != nil {
		b.Fatalf("open focused bench db: %v", err)
	}
	b.Cleanup(func() {
		if err := handle.Close(); err != nil {
			b.Fatalf("close focused bench db: %v", err)
		}
	})
	return handle, collector, epoch
}

func focusedBenchDBPath(b *testing.B) string {
	b.Helper()

	if path := os.Getenv("RBI_STRESS_BENCH_DB"); path != "" {
		if abs, err := filepath.Abs(path); err == nil {
			path = abs
		}
		if _, err := os.Stat(path); err != nil {
			b.Skipf("focused bench db %q unavailable: %v", path, err)
		}
		return path
	}

	path := "stress.db"
	if abs, err := filepath.Abs(path); err == nil {
		path = abs
	}
	if _, err := os.Stat(path); err != nil {
		b.Skipf("focused bench db %q unavailable: %v", path, err)
	}
	return path
}

func warmFocusedReadBenchmarkCase(b *testing.B, handle *DBHandle, desc *classDescriptor, benchCase focusedReadBenchCase, now int64, withTrace bool) {
	b.Helper()

	q := benchCase.build(now)
	_ = desc
	_ = withTrace
	if err := benchCase.run(handle.DB, q); err != nil {
		b.Fatalf("warm %s: %v", benchCase.name, err)
	}
}

func runFocusedReadWorkers(b *testing.B, handle *DBHandle, collector *plannerTraceCollector, desc *classDescriptor, benchCase focusedReadBenchCase, workers int, now int64, withTrace bool) {
	b.Helper()

	var (
		next  atomic.Int64
		stop  atomic.Bool
		wg    sync.WaitGroup
		errMu sync.Once
		err   error
	)

	if workers < 1 {
		workers = 1
	}

	for workerIndex := 0; workerIndex < workers; workerIndex++ {
		workerIndex := workerIndex
		wg.Add(1)
		go func() {
			defer wg.Done()

			var traceWorker *plannerTraceWorker
			if withTrace && collector != nil {
				traceWorker = collector.newWorker(desc)
				traceWorker.bindCurrentGoroutine()
				defer traceWorker.close()
			}

			labels := pprof.Labels(
				"stress_query", benchCase.name,
				"stress_class", desc.Info.Alias,
				"stress_workers", strconv.Itoa(workers),
				"stress_bench_mode", focusedBenchMode(withTrace),
			)
			pprof.Do(context.Background(), labels, func(context.Context) {
				q := benchCase.build(now)
				for !stop.Load() {
					n := next.Add(1)
					if int(n) > b.N {
						return
					}
					runErr := runFocusedReadTraceQuery(traceWorker, benchCase.name, func() error {
						return benchCase.run(handle.DB, q)
					})
					if runErr != nil {
						stop.Store(true)
						errMu.Do(func() {
							err = fmt.Errorf("worker %d: %w", workerIndex, runErr)
						})
						return
					}
				}
			})
		}()
	}

	wg.Wait()
	if err != nil {
		b.Fatal(err)
	}
}

func focusedBenchMode(withTrace bool) string {
	if withTrace {
		return "trace"
	}
	return "perf"
}

func runFocusedReadTraceQuery(worker *plannerTraceWorker, query string, fn func() error) error {
	if worker == nil {
		return fn()
	}
	done := worker.begin(query)
	defer done()
	return fn()
}

func runQueryItemsBench(db *rbi.DB[uint64, UserBench], q *qx.QX) error {
	items, err := db.Query(q)
	db.ReleaseRecords(items...)
	return err
}

func runQueryKeysBenchQuery(db *rbi.DB[uint64, UserBench], q *qx.QX) error {
	_, err := db.QueryKeys(q)
	return err
}

func runCountBenchQuery(db *rbi.DB[uint64, UserBench], q *qx.QX) error {
	_, err := db.Count(q.Filter)
	return err
}

func reportFocusedReadTraceMetrics(b *testing.B, report *plannerTraceScopeReport) {
	b.Helper()
	if report == nil || report.Sampled == 0 {
		return
	}

	b.ReportMetric(float64(report.Sampled), "trace_samples")
	b.ReportMetric(report.AvgRowsExamined, "rows_examined/op")
	b.ReportMetric(report.AvgRowsReturned, "rows_returned/op")
	b.ReportMetric(report.AvgOrderScanWidth, "order_scan/op")
	b.ReportMetric(report.AvgDedupeCount, "dedupe/op")
	b.ReportMetric(report.AvgDurationUs, "trace_us/op")
	b.ReportMetric(report.AvgPlannerUs, "planner_us/op")
	b.ReportMetric(report.AvgPlannerPreds, "planner_preds/op")
	b.ReportMetric(report.AvgPlannerHits, "planner_hits/op")
	b.ReportMetric(report.AvgPlannerBuilds, "planner_builds/op")
	b.ReportMetric(report.AvgPlannerExact, "planner_exact/op")
	b.ReportMetric(report.AvgPlannerReused, "planner_reused/op")
	b.ReportMetric(float64(report.RuntimeFallbacks), "runtime_fallbacks")
	if len(report.PlanCounts) > 0 {
		var maxPlan uint64
		for _, count := range report.PlanCounts {
			if count > maxPlan {
				maxPlan = count
			}
		}
		b.ReportMetric(float64(len(report.PlanCounts)), "plans")
		b.ReportMetric(float64(maxPlan)/float64(report.Sampled), "dominant_plan_ratio")
	}
}
