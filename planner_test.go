package rbi

import (
	"math"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/vapstack/qx"
)

func TestPlannerCalibration_ObserveUpdatesMultiplier(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options[uint64, Rec]{
		AnalyzeInterval:        -1,
		CalibrationEnabled:     true,
		CalibrationSampleEvery: 1,
	})

	before, ok := db.GetCalibrationSnapshot()
	if !ok {
		t.Fatalf("expected initialized planner calibration snapshot")
	}
	base := before.Multipliers["plan_or_merge_no_order"]
	if base <= 0 {
		t.Fatalf("unexpected base multiplier: %v", base)
	}

	db.observeCalibration(TraceEvent{
		Plan:          "plan_or_merge_no_order",
		EstimatedRows: 100,
		RowsExamined:  300,
	})

	after, ok := db.GetCalibrationSnapshot()
	if !ok {
		t.Fatalf("expected planner calibration snapshot after update")
	}
	if after.Samples["plan_or_merge_no_order"] != before.Samples["plan_or_merge_no_order"]+1 {
		t.Fatalf("unexpected sample count: before=%d after=%d", before.Samples["plan_or_merge_no_order"], after.Samples["plan_or_merge_no_order"])
	}
	if after.Multipliers["plan_or_merge_no_order"] <= base {
		t.Fatalf("expected multiplier to increase: before=%v after=%v", base, after.Multipliers["plan_or_merge_no_order"])
	}
}

func TestPlannerCalibration_QueryPathUpdatesWithoutTracerSink(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options[uint64, Rec]{
		AnalyzeInterval:        -1,
		CalibrationEnabled:     true,
		CalibrationSampleEvery: 1,
	})
	_ = seedData(t, db, 20_000)

	before, ok := db.GetCalibrationSnapshot()
	if !ok {
		t.Fatalf("expected initialized planner calibration snapshot")
	}

	q := qx.Query(
		qx.OR(
			qx.EQ("active", true),
			qx.EQ("name", "alice"),
		),
	).Max(120)

	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) == 0 {
		t.Fatalf("expected non-empty query result")
	}

	after, ok := db.GetCalibrationSnapshot()
	if !ok {
		t.Fatalf("expected planner calibration snapshot after query")
	}
	if after.Samples["plan_or_merge_no_order"] <= before.Samples["plan_or_merge_no_order"] {
		t.Fatalf(
			"expected calibration samples to increase from query path: before=%d after=%d",
			before.Samples["plan_or_merge_no_order"],
			after.Samples["plan_or_merge_no_order"],
		)
	}
}

func TestPlannerCalibration_InfluencesORNoOrderDecision(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options[uint64, Rec]{
		AnalyzeInterval:        -1,
		CalibrationEnabled:     true,
		CalibrationSampleEvery: 1,
	})
	_ = seedData(t, db, 2_000)

	q := qx.Query(
		qx.OR(
			qx.EQ("active", true),
			qx.EQ("name", "alice"),
		),
	).Max(100)

	setORPlannerStatsSnapshotForTest(db, 100_000, PlannerFieldStats{
		DistinctKeys:    100_000,
		NonEmptyKeys:    100_000,
		TotalBucketCard: 100_000,
		AvgBucketCard:   1,
		MaxBucketCard:   2,
		P50BucketCard:   1,
		P95BucketCard:   2,
	})

	branches := []plannerORBranch{
		makeORBranchForCalibrationDecisionTest(4_000, 4),
		makeORBranchForCalibrationDecisionTest(4_000, 4),
		makeORBranchForCalibrationDecisionTest(4_000, 4),
	}
	defer releaseORBranches(branches)

	base := db.decidePlanORNoOrder(q, branches)
	if !base.use {
		t.Fatalf("expected base decision to use OR no-order plan")
	}

	err := db.SetCalibrationSnapshot(CalibrationSnapshot{
		UpdatedAt: time.Now(),
		Multipliers: map[string]float64{
			"plan_or_merge_no_order": 3.8,
		},
	})
	if err != nil {
		t.Fatalf("SetCalibrationSnapshot: %v", err)
	}

	adjusted := db.decidePlanORNoOrder(q, branches)
	if adjusted.use {
		t.Fatalf("expected calibrated decision to reject OR no-order plan")
	}
}

func TestPlannerCalibration_SaveLoadRoundTrip(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options[uint64, Rec]{
		AnalyzeInterval:    -1,
		CalibrationEnabled: true,
	})

	err := db.SetCalibrationSnapshot(CalibrationSnapshot{
		UpdatedAt: time.Now().UTC().Round(time.Second),
		Multipliers: map[string]float64{
			"plan_or_merge_no_order":     1.65,
			"plan_or_merge_order_merge":  1.20,
			"plan_or_merge_order_stream": 1.05,
			"plan_ordered":               0.88,
		},
		Samples: map[string]uint64{
			"plan_or_merge_no_order":     10,
			"plan_or_merge_order_merge":  7,
			"plan_or_merge_order_stream": 5,
			"plan_ordered":               3,
		},
	})
	if err != nil {
		t.Fatalf("SetCalibrationSnapshot: %v", err)
	}

	path := filepath.Join(t.TempDir(), "planner_calibration.json")
	if err := db.SaveCalibration(path); err != nil {
		t.Fatalf("SaveCalibration: %v", err)
	}

	db2, _ := openTempDBUint64(t, &Options[uint64, Rec]{
		AnalyzeInterval:    -1,
		CalibrationEnabled: true,
	})
	if err := db2.LoadCalibration(path); err != nil {
		t.Fatalf("LoadCalibration: %v", err)
	}

	got, ok := db2.GetCalibrationSnapshot()
	if !ok {
		t.Fatalf("expected snapshot after load")
	}

	assertApproxMultiplier(t, got.Multipliers["plan_or_merge_no_order"], 1.65)
	assertApproxMultiplier(t, got.Multipliers["plan_or_merge_order_merge"], 1.20)
	assertApproxMultiplier(t, got.Multipliers["plan_or_merge_order_stream"], 1.05)
	assertApproxMultiplier(t, got.Multipliers["plan_ordered"], 0.88)
	if got.Samples["plan_or_merge_no_order"] != 10 {
		t.Fatalf("unexpected sample count for plan_or_merge_no_order: %d", got.Samples["plan_or_merge_no_order"])
	}
	if got.Samples["plan_ordered"] != 3 {
		t.Fatalf("unexpected sample count for plan_ordered: %d", got.Samples["plan_ordered"])
	}
}

func TestPlannerCalibration_AutoPersist(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "auto_persist.db")
	calPath := filepath.Join(dir, "planner_calibration_auto.json")

	opts := &Options[uint64, Rec]{
		AnalyzeInterval:        -1,
		CalibrationEnabled:     true,
		CalibrationPersistPath: calPath,
	}

	db, err := Open[uint64, Rec](dbPath, 0o600, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := db.SetCalibrationSnapshot(CalibrationSnapshot{
		UpdatedAt: time.Now(),
		Multipliers: map[string]float64{
			"plan_ordered": 1.42,
		},
		Samples: map[string]uint64{
			"plan_ordered": 12,
		},
	}); err != nil {
		t.Fatalf("SetCalibrationSnapshot: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open[uint64, Rec](dbPath, 0o600, opts)
	if err != nil {
		t.Fatalf("Open (reopen): %v", err)
	}
	defer func() { _ = db2.Close() }()

	snap, ok := db2.GetCalibrationSnapshot()
	if !ok {
		t.Fatalf("expected snapshot after reopen")
	}
	assertApproxMultiplier(t, snap.Multipliers["plan_ordered"], 1.42)
	if snap.Samples["plan_ordered"] != 12 {
		t.Fatalf("unexpected sample count after reopen: %d", snap.Samples["plan_ordered"])
	}
}

func TestPlannerCalibration_SupportsExecutionPlanNames(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options[uint64, Rec]{
		AnalyzeInterval:    -1,
		CalibrationEnabled: true,
	})

	err := db.SetCalibrationSnapshot(CalibrationSnapshot{
		UpdatedAt: time.Now(),
		Multipliers: map[string]float64{
			"plan_limit_order_basic":  1.15,
			"plan_limit_order_prefix": 0.92,
		},
		Samples: map[string]uint64{
			"plan_limit_order_basic":  4,
			"plan_limit_order_prefix": 7,
		},
	})
	if err != nil {
		t.Fatalf("SetCalibrationSnapshot: %v", err)
	}

	snap, ok := db.GetCalibrationSnapshot()
	if !ok {
		t.Fatalf("expected snapshot")
	}
	assertApproxMultiplier(t, snap.Multipliers["plan_limit_order_basic"], 1.15)
	assertApproxMultiplier(t, snap.Multipliers["plan_limit_order_prefix"], 0.92)
	if snap.Samples["plan_limit_order_basic"] != 4 {
		t.Fatalf("unexpected sample count: %d", snap.Samples["plan_limit_order_basic"])
	}
	if snap.Samples["plan_limit_order_prefix"] != 7 {
		t.Fatalf("unexpected sample count: %d", snap.Samples["plan_limit_order_prefix"])
	}
}

func assertApproxMultiplier(t *testing.T, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 0.001 {
		t.Fatalf("multiplier mismatch: got=%v want=%v", got, want)
	}
}

func makeORBranchForCalibrationDecisionTest(estCard uint64, extraChecks int) plannerORBranch {
	if extraChecks < 0 {
		extraChecks = 0
	}
	preds := make([]plannerPred, 0, 1+extraChecks)
	preds = append(preds, plannerPred{
		estCard:  estCard,
		iter:     func() roaringIter { return roaring64.NewBitmap().Iterator() },
		contains: func(uint64) bool { return true },
	})
	for i := 0; i < extraChecks; i++ {
		preds = append(preds, plannerPred{
			// estCard=0 keeps branch cardinality estimate driven by lead,
			// but contains!=nil increases per-row check cost.
			contains: func(uint64) bool { return true },
		})
	}
	b := plannerORBranch{
		preds:   preds,
		leadIdx: 0,
	}
	b.lead = &b.preds[0]
	return b
}

func setORPlannerStatsSnapshotForTest(db *DB[uint64, Rec], universe uint64, scoreStats PlannerFieldStats) {
	db.planner.stats.Store(&PlannerStatsSnapshot{
		Version:             1,
		UniverseCardinality: universe,
		Fields: map[string]PlannerFieldStats{
			"score": scoreStats,
		},
	})
}

/**/

func TestPlannerStatsCollector_FullRefreshMatchesLockedSnapshot(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options[uint64, Rec]{AnalyzeInterval: -1})
	_ = seedData(t, db, 3_000)

	db.mu.RLock()
	expected := db.buildPlannerStatsSnapshotLocked(1)
	db.mu.RUnlock()

	if err := db.RefreshPlannerStats(); err != nil {
		t.Fatalf("RefreshPlannerStats: %v", err)
	}

	got, ok := db.GetPlannerStatsSnapshot()
	if !ok {
		t.Fatalf("expected planner stats snapshot")
	}

	if got.UniverseCardinality != expected.UniverseCardinality {
		t.Fatalf("universe mismatch: got=%d want=%d", got.UniverseCardinality, expected.UniverseCardinality)
	}

	if len(got.Fields) != len(expected.Fields) {
		t.Fatalf("fields count mismatch: got=%d want=%d", len(got.Fields), len(expected.Fields))
	}

	for fieldName, expectedStats := range expected.Fields {
		gotStats, ok := got.Fields[fieldName]
		if !ok {
			t.Fatalf("missing field stats for %q", fieldName)
		}
		if gotStats != expectedStats {
			t.Fatalf("field %q stats mismatch: got=%+v want=%+v", fieldName, gotStats, expectedStats)
		}
	}
}

func TestPlannerStatsCollector_PeriodicBudgetAdvancesCursor(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options[uint64, Rec]{AnalyzeInterval: -1})
	_ = seedData(t, db, 3_000)

	db.mu.RLock()
	fieldCount := len(db.index)
	db.mu.RUnlock()
	if fieldCount < 2 {
		t.Skip("not enough indexed fields for cursor progression test")
	}

	db.planner.analyzer.Lock()
	db.planner.analyzer.cursor = 0
	db.planner.analyzer.softBudget = 1 // 1ns, effectively one heavy field per cycle
	db.planner.analyzer.Unlock()

	s0, ok := db.GetPlannerStatsSnapshot()
	if !ok {
		t.Fatalf("expected initial planner stats snapshot")
	}
	prevVersion := s0.Version
	prevCursor := 0

	cycles := fieldCount + 3
	for i := 0; i < cycles; i++ {
		if err := db.refreshPlannerStatsPeriodic(); err != nil {
			t.Fatalf("refreshPlannerStatsPeriodic cycle %d: %v", i, err)
		}

		db.planner.analyzer.Lock()
		curCursor := db.planner.analyzer.cursor
		db.planner.analyzer.Unlock()

		wantCursor := (prevCursor + 1) % fieldCount
		if curCursor != wantCursor {
			t.Fatalf("cursor mismatch at cycle %d: got=%d want=%d", i, curCursor, wantCursor)
		}
		prevCursor = curCursor

		s, ok := db.GetPlannerStatsSnapshot()
		if !ok {
			t.Fatalf("expected planner stats snapshot at cycle %d", i)
		}
		if s.Version <= prevVersion {
			t.Fatalf("version did not advance at cycle %d: got=%d prev=%d", i, s.Version, prevVersion)
		}
		prevVersion = s.Version
	}
}

/**/

func TestResolvePlannerAnalyzeInterval(t *testing.T) {
	if got := resolvePlannerAnalyzeInterval(-1); got != 0 {
		t.Fatalf("negative interval should disable scheduler: got=%v", got)
	}
	if got := resolvePlannerAnalyzeInterval(0); got != defaultPlannerAnalyzeInterval {
		t.Fatalf("zero interval should map to default: got=%v want=%v", got, defaultPlannerAnalyzeInterval)
	}
	custom := 37 * time.Second
	if got := resolvePlannerAnalyzeInterval(custom); got != custom {
		t.Fatalf("custom interval mismatch: got=%v want=%v", got, custom)
	}
}

func TestPlannerAnalyzeScheduler_Disabled(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options[uint64, Rec]{AnalyzeInterval: -1})

	if db.planner.analyzer.stop != nil || db.planner.analyzer.done != nil {
		t.Fatalf("scheduler should be disabled for negative interval")
	}

	s0, ok := db.GetPlannerStatsSnapshot()
	if !ok {
		t.Fatalf("expected initial snapshot")
	}

	time.Sleep(120 * time.Millisecond)

	s1, ok := db.GetPlannerStatsSnapshot()
	if !ok {
		t.Fatalf("expected snapshot after sleep")
	}
	if s1.Version != s0.Version {
		t.Fatalf("snapshot version changed while scheduler disabled: before=%d after=%d", s0.Version, s1.Version)
	}
}

func TestPlannerAnalyzeScheduler_StartAndStop(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options[uint64, Rec]{AnalyzeInterval: 20 * time.Millisecond})

	if db.planner.analyzer.stop == nil || db.planner.analyzer.done == nil {
		t.Fatalf("scheduler should be started")
	}

	s0, ok := db.GetPlannerStatsSnapshot()
	if !ok {
		t.Fatalf("expected initial snapshot")
	}

	if latest, ok := waitPlannerStatsVersionGreater(db, s0.Version, 900*time.Millisecond); !ok {
		t.Fatalf("expected background refresh to advance snapshot version: start=%d latest=%d", s0.Version, latest)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if db.planner.analyzer.stop != nil || db.planner.analyzer.done != nil {
		t.Fatalf("scheduler channels must be cleared on close")
	}

	closedSnapshot, ok := db.GetPlannerStatsSnapshot()
	if !ok {
		t.Fatalf("expected snapshot after close")
	}

	time.Sleep(120 * time.Millisecond)

	afterSnapshot, ok := db.GetPlannerStatsSnapshot()
	if !ok {
		t.Fatalf("expected snapshot after close sleep")
	}
	if afterSnapshot.Version != closedSnapshot.Version {
		t.Fatalf("snapshot version changed after close: before=%d after=%d", closedSnapshot.Version, afterSnapshot.Version)
	}
}

func waitPlannerStatsVersionGreater(db *DB[uint64, Rec], version uint64, timeout time.Duration) (uint64, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		s, ok := db.GetPlannerStatsSnapshot()
		if ok && s.Version > version {
			return s.Version, true
		}
		time.Sleep(5 * time.Millisecond)
	}
	s, ok := db.GetPlannerStatsSnapshot()
	if !ok {
		return 0, false
	}
	return s.Version, false
}

/**/

func TestPlannerStatsSnapshot_RefreshAndVersion(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	_ = seedData(t, db, 1_000)

	s0, ok := db.GetPlannerStatsSnapshot()
	if !ok {
		t.Fatalf("expected initial planner stats snapshot")
	}

	if err := db.RefreshPlannerStats(); err != nil {
		t.Fatalf("RefreshPlannerStats: %v", err)
	}

	s1, ok := db.GetPlannerStatsSnapshot()
	if !ok {
		t.Fatalf("expected planner stats snapshot after refresh")
	}

	if s1.Version <= s0.Version {
		t.Fatalf("version did not advance: before=%d after=%d", s0.Version, s1.Version)
	}
	if s1.UniverseCardinality != 1_000 {
		t.Fatalf("unexpected universe cardinality: got=%d want=%d", s1.UniverseCardinality, 1_000)
	}

	country, ok := s1.Fields["country"]
	if !ok {
		t.Fatalf("expected country field stats")
	}
	if country.DistinctKeys == 0 {
		t.Fatalf("expected non-zero distinct keys for country")
	}
	if country.P95BucketCard < country.P50BucketCard {
		t.Fatalf("expected p95 >= p50, got p95=%d p50=%d", country.P95BucketCard, country.P50BucketCard)
	}

	// Returned snapshot must be safe to mutate by caller.
	s1.Fields["country"] = PlannerFieldStats{}
	s2, ok := db.GetPlannerStatsSnapshot()
	if !ok {
		t.Fatalf("expected planner stats snapshot")
	}
	if s2.Fields["country"].DistinctKeys == 0 {
		t.Fatalf("snapshot was mutated by caller")
	}
}

/**/

func TestTracer_EmitsAndSamples(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	sink := func(ev TraceEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	db, _ := openTempDBUint64(t, &Options[uint64, Rec]{
		AnalyzeInterval:  -1,
		TraceSink:        sink,
		TraceSampleEvery: 2,
	})
	_ = seedData(t, db, 5_000)

	q := qx.Query(
		qx.EQ("active", true),
		qx.GTE("age", 20),
	).Max(100)

	for i := 0; i < 5; i++ {
		ids, err := db.QueryKeys(q)
		if err != nil {
			t.Fatalf("QueryKeys: %v", err)
		}
		if len(ids) == 0 {
			t.Fatalf("expected non-empty ids")
		}
	}

	mu.Lock()
	got := append([]TraceEvent(nil), events...)
	mu.Unlock()

	// sampleEvery=2 emits for query #2 and #4.
	if len(got) != 2 {
		t.Fatalf("unexpected trace events count: got=%d want=%d", len(got), 2)
	}

	for i, ev := range got {
		if ev.Plan == "" {
			t.Fatalf("event %d: empty plan", i)
		}
		if ev.Duration <= 0 {
			t.Fatalf("event %d: expected positive duration", i)
		}
		if ev.RowsReturned == 0 {
			t.Fatalf("event %d: expected positive rows returned", i)
		}
	}
}

func TestTracer_ORDecisionEstimates(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	sink := func(ev TraceEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	db, _ := openTempDBUint64(t, &Options[uint64, Rec]{
		AnalyzeInterval:  -1,
		TraceSink:        sink,
		TraceSampleEvery: 1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.OR(
			qx.EQ("active", true),
			qx.EQ("name", "alice"),
		),
	).Max(120)

	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) == 0 {
		t.Fatalf("expected non-empty ids")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]

	if !strings.HasPrefix(ev.Plan, "plan_or_merge_") {
		t.Fatalf("expected OR planner plan, got %q", ev.Plan)
	}
	if ev.EstimatedRows == 0 {
		t.Fatalf("expected estimated rows to be set")
	}
	if ev.EstimatedCost <= 0 {
		t.Fatalf("expected estimated cost to be positive")
	}
	if ev.RowsExamined == 0 {
		t.Fatalf("expected rows examined to be recorded")
	}
	if ev.RowsReturned != uint64(len(ids)) {
		t.Fatalf("rows returned mismatch: ev=%d ids=%d", ev.RowsReturned, len(ids))
	}
	if len(ev.ORBranches) == 0 {
		t.Fatalf("expected OR branch trace")
	}
	var branchExamined uint64
	var branchEmitted uint64
	for _, b := range ev.ORBranches {
		branchExamined += b.RowsExamined
		branchEmitted += b.RowsEmitted
	}
	if branchExamined == 0 {
		t.Fatalf("expected OR branch examined rows to be recorded")
	}
	if branchEmitted == 0 {
		t.Fatalf("expected OR branch emitted rows to be recorded")
	}
	if ev.EarlyStopReason == "" {
		t.Fatalf("expected early stop reason to be set")
	}
	if ev.OrderIndexScanWidth != 0 {
		t.Fatalf("expected no order-index scan width for unordered OR query, got=%d", ev.OrderIndexScanWidth)
	}
}

func TestTracer_OROrderMetrics(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	sink := func(ev TraceEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	db, _ := openTempDBUint64(t, &Options[uint64, Rec]{
		AnalyzeInterval:  -1,
		TraceSink:        sink,
		TraceSampleEvery: 1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.OR(
			qx.EQ("active", true),
			qx.EQ("name", "alice"),
		),
	).By("age", qx.ASC).Max(80)

	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) == 0 {
		t.Fatalf("expected non-empty ids")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]

	if !strings.HasPrefix(ev.Plan, "plan_or_merge_order_") {
		t.Fatalf("expected ordered OR planner plan, got %q", ev.Plan)
	}
	if len(ev.ORBranches) == 0 {
		t.Fatalf("expected OR branch trace")
	}

	var branchExamined uint64
	var branchEmitted uint64
	for _, b := range ev.ORBranches {
		branchExamined += b.RowsExamined
		branchEmitted += b.RowsEmitted
	}

	if branchExamined == 0 {
		t.Fatalf("expected OR branch examined rows to be recorded")
	}
	if branchEmitted == 0 {
		t.Fatalf("expected OR branch emitted rows to be recorded")
	}
	if ev.OrderIndexScanWidth == 0 {
		t.Fatalf("expected order-index scan width to be recorded")
	}
	if ev.EarlyStopReason == "" {
		t.Fatalf("expected early stop reason to be set")
	}
}

func TestTracer_ORNoOrderAdaptiveSkipsBranchByThreshold(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	sink := func(ev TraceEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	db, _ := openTempDBUint64(t, &Options[uint64, Rec]{
		AnalyzeInterval:  -1,
		TraceSink:        sink,
		TraceSampleEvery: 1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.OR(
			qx.EQ("full_name", "FN-10"),
			qx.EQ("full_name", "FN-20"),
			qx.GTE("full_name", "FN-9000"),
		),
	).Max(2)

	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 ids, got %d", len(ids))
	}
	if ids[0] != 10 || ids[1] != 20 {
		t.Fatalf("unexpected ids: got=%v want=[10 20]", ids)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]

	if ev.Plan != "plan_or_merge_no_order" {
		t.Fatalf("expected no-order OR plan, got %q", ev.Plan)
	}
	if ev.EarlyStopReason != "limit_reached" {
		t.Fatalf("expected limit_reached early stop, got %q", ev.EarlyStopReason)
	}
	if len(ev.ORBranches) != 3 {
		t.Fatalf("unexpected OR branch trace size: got=%d want=3", len(ev.ORBranches))
	}

	skipped := 0
	for _, b := range ev.ORBranches {
		if b.Skipped && b.SkipReason == "threshold_pruned" {
			skipped++
		}
	}
	if skipped == 0 {
		t.Fatalf("expected at least one threshold-pruned branch in trace")
	}
}

func TestPlannerORNoOrderAdaptive_MatchesBaseline(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options[uint64, Rec]{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.OR(
			qx.EQ("active", true),
			qx.EQ("name", "alice"),
			qx.EQ("country", "NL"),
		),
	).Max(150)
	q = normalizeQuery(q)

	branchesAdaptive, alwaysFalse, ok := db.buildORBranches(q.Expr.Operands)
	if !ok {
		t.Fatalf("buildORBranches adaptive: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for adaptive branches")
	}
	defer releaseORBranches(branchesAdaptive)

	gotAdaptive, ok := db.execPlanORNoOrderAdaptive(q, branchesAdaptive, nil)
	if !ok {
		t.Fatalf("execPlanORNoOrderAdaptive: ok=false")
	}

	branchesBaseline, alwaysFalse, ok := db.buildORBranches(q.Expr.Operands)
	if !ok {
		t.Fatalf("buildORBranches baseline: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for baseline branches")
	}
	defer releaseORBranches(branchesBaseline)

	gotBaseline, ok := db.execPlanORNoOrderBaseline(q, branchesBaseline, nil)
	if !ok {
		t.Fatalf("execPlanORNoOrderBaseline: ok=false")
	}

	if !slices.Equal(gotAdaptive, gotBaseline) {
		t.Fatalf("adaptive/baseline mismatch:\nadaptive=%v\nbaseline=%v", gotAdaptive, gotBaseline)
	}
}

func TestPlannerOROrderKWay_MatchesFallbackMerge(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options[uint64, Rec]{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.EQ("active", true),
				qx.EQ("country", "NL"),
			),
			qx.AND(
				qx.EQ("name", "alice"),
				qx.GTE("age", 25),
			),
			qx.PREFIX("full_name", "FN-1"),
		),
	).By("age", qx.ASC).Skip(30).Max(120)
	q = normalizeQuery(q)

	branchesKWay, alwaysFalse, ok := db.buildORBranches(q.Expr.Operands)
	if !ok {
		t.Fatalf("buildORBranches kway: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for kway branches")
	}
	defer releaseORBranches(branchesKWay)

	gotKWay, ok, err := db.execPlanOROrderKWay(q, branchesKWay, nil)
	if err != nil {
		t.Fatalf("execPlanOROrderKWay err: %v", err)
	}
	if !ok {
		t.Fatalf("execPlanOROrderKWay: ok=false")
	}

	branchesBaseline, alwaysFalse, ok := db.buildORBranches(q.Expr.Operands)
	if !ok {
		t.Fatalf("buildORBranches fallback merge: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for fallback merge branches")
	}
	defer releaseORBranches(branchesBaseline)

	gotBaseline, ok, err := db.execPlanOROrderMergeFallback(q, branchesBaseline, nil)
	if err != nil {
		t.Fatalf("execPlanOROrderMergeFallback err: %v", err)
	}
	if !ok {
		t.Fatalf("execPlanOROrderMergeFallback: ok=false")
	}

	if !slices.Equal(gotKWay, gotBaseline) {
		t.Fatalf("kway/fallback mismatch:\nkway=%v\nfallback=%v", gotKWay, gotBaseline)
	}
}

func TestPlannerORKWayShouldFallbackRuntime(t *testing.T) {
	if plannerORKWayShouldFallbackRuntime(120, 16, 20, 300_000) {
		t.Fatalf("unexpected fallback for too-small pop sample")
	}
	if plannerORKWayShouldFallbackRuntime(120, 64, 4, 300_000) {
		t.Fatalf("unexpected fallback for too-small unique sample")
	}
	if plannerORKWayShouldFallbackRuntime(120, 64, 20, 8_000) {
		t.Fatalf("unexpected fallback for too-small examined sample")
	}
	if plannerORKWayShouldFallbackRuntime(120, 64, 50, 40_000) {
		t.Fatalf("unexpected fallback for efficient stream")
	}
	if !plannerORKWayShouldFallbackRuntime(120, 64, 12, 500_000) {
		t.Fatalf("expected fallback for poor projected k-way efficiency")
	}
}

func TestPlannerOROrderKWayRuntimeFallbackEnable(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options[uint64, Rec]{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	qPrefix := normalizeQuery(qx.Query(
		qx.OR(
			qx.AND(
				qx.EQ("active", true),
				qx.EQ("country", "NL"),
				qx.GTE("score", 30.0),
			),
			qx.AND(
				qx.EQ("name", "alice"),
				qx.GTE("age", 25),
			),
			qx.PREFIX("full_name", "FN-1"),
		),
	).By("age", qx.ASC).Skip(140).Max(120))

	branchesPrefix, alwaysFalse, ok := db.buildORBranches(qPrefix.Expr.Operands)
	if !ok {
		t.Fatalf("buildORBranches prefix: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for prefix branches")
	}
	defer releaseORBranches(branchesPrefix)

	needPrefix, ok := orderWindow(qPrefix)
	if !ok {
		t.Fatalf("orderWindow prefix: ok=false")
	}
	if !db.shouldUseOROrderKWayRuntimeFallback(qPrefix, branchesPrefix, needPrefix) {
		t.Fatalf("expected runtime fallback guard to be enabled for prefix/non-order shape")
	}

	qSimple := normalizeQuery(qx.Query(
		qx.OR(
			qx.EQ("country", "NL"),
			qx.EQ("name", "alice"),
		),
	).By("age", qx.ASC).Max(120))

	branchesSimple, alwaysFalse, ok := db.buildORBranches(qSimple.Expr.Operands)
	if !ok {
		t.Fatalf("buildORBranches simple: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for simple branches")
	}
	defer releaseORBranches(branchesSimple)

	needSimple, ok := orderWindow(qSimple)
	if !ok {
		t.Fatalf("orderWindow simple: ok=false")
	}
	if db.shouldUseOROrderKWayRuntimeFallback(qSimple, branchesSimple, needSimple) {
		t.Fatalf("expected runtime fallback guard to stay disabled for simple OR shape")
	}
}

func TestPlannerOrderedAnchor_MatchesBaseline(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options[uint64, Rec]{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.EQ("country", "NL"),
		qx.HASANY("tags", []string{"go", "db"}),
		qx.GTE("age", 30),
		qx.NOTIN("name", []string{"alice"}),
	).By("full_name", qx.ASC).Skip(20).Max(80)
	q = normalizeQuery(q)

	leaves, ok := collectAndLeaves(q.Expr)
	if !ok {
		t.Fatalf("collectAndLeaves: ok=false")
	}

	predsA, ok := db.buildPredicates(leaves)
	if !ok {
		t.Fatalf("buildPredicates A: ok=false")
	}
	defer releasePredicates(predsA)

	gotAnchor, ok := db.execPlanOrderedBasic(q, predsA, nil)
	if !ok {
		t.Fatalf("execPlanOrderedBasic A: ok=false")
	}

	predsB, ok := db.buildPredicates(leaves)
	if !ok {
		t.Fatalf("buildPredicates B: ok=false")
	}
	defer releasePredicates(predsB)

	orderField := q.Order[0].Field
	slice := db.index[orderField]
	if slice == nil {
		t.Fatalf("order slice is nil")
	}
	s := *slice
	start, end := 0, len(s)
	if st, en, cov, ok := db.extractOrderRangeCoveragePreds(orderField, predsB, s); ok {
		start, end = st, en
		for i := range cov {
			if cov[i] {
				predsB[i].covered = true
			}
		}
	}

	active := make([]int, 0, len(predsB))
	for i := range predsB {
		p := predsB[i]
		if p.covered || p.alwaysTrue {
			continue
		}
		if p.alwaysFalse {
			t.Fatalf("unexpected alwaysFalse predicate")
		}
		active = append(active, i)
	}

	gotBaseline := db.execPlanOrderedBasicFallback(q, predsB, active, start, end, s, nil)
	if !slices.Equal(gotAnchor, gotBaseline) {
		t.Fatalf("ordered anchor/fallback mismatch:\nanchor=%v\nfallback=%v", gotAnchor, gotBaseline)
	}
}

func TestPlannerRouting_PrefersOrderedAnchorForMixedPredicates(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	sink := func(ev TraceEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	db, _ := openTempDBUint64(t, &Options[uint64, Rec]{
		AnalyzeInterval:  -1,
		TraceSink:        sink,
		TraceSampleEvery: 1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.EQ("country", "NL"),
		qx.HASANY("tags", []string{"go", "db"}),
		qx.GTE("age", 30),
		qx.NOTIN("name", []string{"alice"}),
	).By("full_name", qx.ASC).Skip(20).Max(80)

	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) == 0 {
		t.Fatalf("expected non-empty ids")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]

	if ev.Plan != "plan_ordered_anchor" {
		t.Fatalf("expected ordered anchor plan, got %q", ev.Plan)
	}
	if ev.OrderIndexScanWidth == 0 {
		t.Fatalf("expected non-zero order index scan width")
	}
	if ev.EarlyStopReason == "" {
		t.Fatalf("expected early stop reason to be set")
	}
}

func TestPlannerRouting_PrefersExecutionForPrefixOrderLimit(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	sink := func(ev TraceEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	db, _ := openTempDBUint64(t, &Options[uint64, Rec]{
		AnalyzeInterval:  -1,
		TraceSink:        sink,
		TraceSampleEvery: 1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.PREFIX("full_name", "FN-1"),
		qx.EQ("active", true),
	).By("full_name", qx.ASC).Max(20)

	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) == 0 {
		t.Fatalf("expected non-empty ids")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]

	if ev.Plan != "plan_limit_order_prefix" {
		t.Fatalf("expected prefix execution plan, got %q", ev.Plan)
	}
	if ev.EstimatedRows == 0 {
		t.Fatalf("expected estimated rows to be set")
	}
	if ev.EstimatedCost <= 0 {
		t.Fatalf("expected estimated cost to be set")
	}
	if ev.FallbackCost <= 0 {
		t.Fatalf("expected fallback cost to be set")
	}
}

func TestPlannerRouting_PrefersExecutionForPrefixNoOrderLimit(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	sink := func(ev TraceEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	db, _ := openTempDBUint64(t, &Options[uint64, Rec]{
		AnalyzeInterval:  -1,
		TraceSink:        sink,
		TraceSampleEvery: 1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.PREFIX("full_name", "FN-10"),
		qx.EQ("active", true),
	).Max(15)

	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) == 0 {
		t.Fatalf("expected non-empty ids")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]

	if ev.Plan != "plan_limit_range_no_order" {
		t.Fatalf("expected no-order prefix execution plan, got %q", ev.Plan)
	}
}

func TestPlannerRouting_PrefersExecutionForWidePrefixNoOrderLimit(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	sink := func(ev TraceEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	db, _ := openTempDBUint64(t, &Options[uint64, Rec]{
		AnalyzeInterval:  -1,
		TraceSink:        sink,
		TraceSampleEvery: 1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.PREFIX("full_name", "FN-"),
		qx.EQ("active", true),
	).Max(15)

	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) == 0 {
		t.Fatalf("expected non-empty ids")
	}

	nq := normalizeQuery(q)
	leaves, ok := extractAndLeaves(nq.Expr)
	if !ok || len(leaves) == 0 {
		t.Fatalf("extractAndLeaves: ok=%v len=%d", ok, len(leaves))
	}
	if !db.shouldPreferExecutionNoOrderPrefix(nq, leaves) {
		t.Fatalf("expected execution preference for wide prefix with selective filter")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	if ev.Plan != "plan_limit_range_no_order" {
		t.Fatalf("expected no-order prefix execution plan, got %q", ev.Plan)
	}
}

func TestPlannerRouting_AvoidsExecutionForWidePrefixLowHitRate(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options[uint64, Rec]{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	q := normalizeQuery(qx.Query(
		qx.PREFIX("full_name", "FN-"),
		qx.EQ("full_name", "FN-10000"),
	).Max(15))

	leaves, ok := extractAndLeaves(q.Expr)
	if !ok || len(leaves) == 0 {
		t.Fatalf("extractAndLeaves: ok=%v len=%d", ok, len(leaves))
	}
	if db.shouldPreferExecutionNoOrderPrefix(q, leaves) {
		t.Fatalf("expected fallback/planner preference for low-hit wide prefix shape")
	}
}

func TestPlannerOROrderMergeFallbackFirst_ComplexOffset(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options[uint64, Rec]{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	qComplex := normalizeQuery(qx.Query(
		qx.OR(
			qx.AND(
				qx.EQ("active", true),
				qx.IN("country", []string{"NL", "DE", "PL"}),
				qx.HASANY("tags", []string{"go", "ops"}),
				qx.GTE("score", 40.0),
			),
			qx.AND(
				qx.EQ("name", "alice"),
				qx.GTE("age", 20),
				qx.LTE("age", 45),
			),
			qx.AND(
				qx.HASANY("tags", []string{"rust", "db"}),
				qx.GTE("score", 30.0),
			),
		),
	).By("score", qx.DESC).Skip(500).Max(100))

	branchesComplex, alwaysFalse, ok := db.buildORBranches(qComplex.Expr.Operands)
	if !ok {
		t.Fatalf("buildORBranches complex: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for complex branches")
	}
	defer releaseORBranches(branchesComplex)

	if !db.shouldPreferOROrderFallbackFirst(qComplex, branchesComplex) {
		t.Fatalf("expected fallback-first preference for complex offset ordered OR")
	}

	qLight := normalizeQuery(qx.Query(
		qx.OR(
			qx.AND(
				qx.EQ("active", true),
				qx.EQ("country", "NL"),
			),
			qx.AND(
				qx.EQ("name", "alice"),
				qx.GTE("age", 25),
			),
			qx.PREFIX("full_name", "FN-1"),
		),
	).By("age", qx.ASC).Max(80))

	branchesLight, alwaysFalse, ok := db.buildORBranches(qLight.Expr.Operands)
	if !ok {
		t.Fatalf("buildORBranches light: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for light branches")
	}
	defer releaseORBranches(branchesLight)

	if db.shouldPreferOROrderFallbackFirst(qLight, branchesLight) {
		t.Fatalf("unexpected fallback-first preference for light ordered OR")
	}
}

func TestPlannerRouting_PrefersExecutionForRangeOrderLimit(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	sink := func(ev TraceEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	db, _ := openTempDBUint64(t, &Options[uint64, Rec]{
		AnalyzeInterval:  -1,
		TraceSink:        sink,
		TraceSampleEvery: 1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.EQ("active", true),
		qx.GTE("age", 22),
		qx.LT("age", 45),
	).By("age", qx.ASC).Max(120)

	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) == 0 {
		t.Fatalf("expected non-empty ids")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]

	if ev.Plan != "plan_limit_order_basic" {
		t.Fatalf("expected basic execution plan, got %q", ev.Plan)
	}
	if ev.EstimatedRows == 0 {
		t.Fatalf("expected estimated rows to be set")
	}
	if ev.EstimatedCost <= 0 {
		t.Fatalf("expected estimated cost to be set")
	}
	if ev.FallbackCost <= 0 {
		t.Fatalf("expected fallback cost to be set")
	}
}
