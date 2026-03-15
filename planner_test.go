package rbi

import (
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/vapstack/qx"
)

func postingOf(ids ...uint64) postingList {
	return postingFromBitmapViewAdaptive(roaring64.BitmapOf(ids...))
}

func TestPlannerCalibration_ObserveUpdatesMultiplier(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
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
	db, _ := openTempDBUint64(t, Options{
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

func TestPlannerCalibration_QueryViewUsesRootSnapshot(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:        -1,
		CalibrationEnabled:     true,
		CalibrationSampleEvery: -1,
	})
	if err := db.SetCalibrationSnapshot(CalibrationSnapshot{
		UpdatedAt: time.Now(),
		Multipliers: map[string]float64{
			string(PlanOrdered):         0.73,
			string(PlanLimitOrderBasic): 1.41,
		},
		Samples: map[string]uint64{
			string(PlanOrdered):         3,
			string(PlanLimitOrderBasic): 5,
		},
	}); err != nil {
		t.Fatalf("SetCalibrationSnapshot: %v", err)
	}

	view := db.makeQueryView(db.getSnapshot())
	defer db.releaseQueryView(view)

	assertApproxMultiplier(t, view.plannerCostMultiplier(plannerCalOrdered), 0.73)
	assertApproxMultiplier(t, view.plannerCostMultiplier(plannerCalLimitOrderBasic), 1.41)
}

func TestPlannerCalibration_SampleEveryNormalization(t *testing.T) {
	dbEnabled, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:        -1,
		CalibrationEnabled:     true,
		CalibrationSampleEvery: 0,
	})
	if got := dbEnabled.planner.calibrator.sampleEvery; got != defaultOptionsCalibrationSampleEvery {
		t.Fatalf("expected default calibration sampleEvery=%d, got=%d", defaultOptionsCalibrationSampleEvery, got)
	}

	dbDisabled, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:        -1,
		CalibrationEnabled:     false,
		CalibrationSampleEvery: 1,
	})
	if got := dbDisabled.planner.calibrator.sampleEvery; got != 0 {
		t.Fatalf("expected calibration sampleEvery=0 when disabled, got=%d", got)
	}
}

func TestTraceAndCalibrationDisabled_BeginTraceReturnsNil(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:        -1,
		CalibrationEnabled:     false,
		CalibrationSampleEvery: 1,
		TraceSink:              nil,
		TraceSampleEvery:       1,
	})
	if db.traceOrCalibrationSamplingEnabled() {
		t.Fatalf("expected trace/calibration sampling to be disabled")
	}

	q := qx.Query(qx.EQ("age", 10))
	if tr := db.beginTrace(q); tr != nil {
		t.Fatalf("expected nil trace when trace sink and calibration are disabled")
	}
}

func TestOrderRangeCoverage_ConsistencyBetweenPredicateKinds(t *testing.T) {
	db, _ := openTempDBUint64(t)

	s := []index{
		{Key: indexKeyFromString("alice"), IDs: postingOf(1)},
		{Key: indexKeyFromString("alina"), IDs: postingOf(2)},
		{Key: indexKeyFromString("bob"), IDs: postingOf(3)},
	}
	delta := &fieldIndexDelta{
		byKey: map[string]indexDeltaEntry{
			"albert": {addSingle: 10, addSingleSet: true},
		},
	}
	ov := newFieldOverlay(&s, delta)

	exprs := []qx.Expr{
		{Op: qx.OpPREFIX, Field: "name", Value: "al"},
		{Op: qx.OpGTE, Field: "name", Value: "al"},
		{Op: qx.OpEQ, Field: "age", Value: 20},
		{Op: qx.OpEQ, Field: "name", Value: "alice", Not: true},
	}

	cands := make([]predicate, len(exprs))
	preds := make([]predicate, len(exprs))
	for i := range exprs {
		cands[i] = predicate{expr: exprs[i]}
		preds[i] = predicate{expr: exprs[i]}
	}

	st1, en1, cov1, ok1 := db.extractOrderRangeCoverage("name", cands, s)
	if !ok1 {
		t.Fatalf("extractOrderRangeCoverage failed")
	}
	st2, en2, cov2, ok2 := db.extractOrderRangeCoverage("name", preds, s)
	if !ok2 {
		t.Fatalf("extractOrderRangeCoverage failed")
	}
	if st1 != st2 || en1 != en2 {
		t.Fatalf("range mismatch: candidate=(%d,%d) planner=(%d,%d)", st1, en1, st2, en2)
	}
	if !slices.Equal(cov1, cov2) {
		t.Fatalf("covered mismatch: candidate=%v planner=%v", cov1, cov2)
	}

	br1, covOv1, okOv1 := db.extractOrderRangeCoverageOverlay("name", cands, ov)
	if !okOv1 {
		t.Fatalf("extractOrderRangeCoverageOverlay failed")
	}
	br2, covOv2, okOv2 := db.extractOrderRangeCoverageOverlay("name", preds, ov)
	if !okOv2 {
		t.Fatalf("extractOrderRangeCoverageOverlay failed")
	}
	if br1.baseStart != br2.baseStart || br1.baseEnd != br2.baseEnd || br1.deltaStart != br2.deltaStart || br1.deltaEnd != br2.deltaEnd {
		t.Fatalf("overlay range mismatch: candidate=%+v planner=%+v", br1, br2)
	}
	if !slices.Equal(covOv1, covOv2) {
		t.Fatalf("overlay covered mismatch: candidate=%v planner=%v", covOv1, covOv2)
	}
}

func TestOverlayApproxDistinctTotalCount_CorrectsSmallDeltaShadowAndDelete(t *testing.T) {
	base := []index{
		{Key: indexKeyFromString("aa"), IDs: postingOf(1)},
		{Key: indexKeyFromString("bb"), IDs: postingOf(2)},
		{Key: indexKeyFromString("cc"), IDs: postingOf(3)},
	}
	ov := fieldOverlay{
		base: base,
		delta: &fieldIndexDelta{
			entries: []fieldDeltaKV{
				{key: "aa", entry: indexDeltaEntry{delSingle: 1, delSingleSet: true}},
				{key: "bb", entry: indexDeltaEntry{addSingle: 22, addSingleSet: true}},
				{key: "bc", entry: indexDeltaEntry{addSingle: 23, addSingleSet: true}},
				{key: "zz", entry: indexDeltaEntry{addSingle: 99, addSingleSet: true, delSingle: 99, delSingleSet: true}},
			},
		},
	}

	rawApprox := len(base) + ov.delta.keyCount()
	if rawApprox == 3 {
		t.Fatalf("test setup must expose approximation drift")
	}
	if got := overlayApproxDistinctTotalCount(ov); got != 3 {
		t.Fatalf("unexpected corrected total distinct count: got=%d want=3", got)
	}
}

func TestOverlayApproxDistinctRangeCount_CorrectsSmallDeltaShadowAndDelete(t *testing.T) {
	base := []index{
		{Key: indexKeyFromString("aa"), IDs: postingOf(1)},
		{Key: indexKeyFromString("bb"), IDs: postingOf(2)},
		{Key: indexKeyFromString("cc"), IDs: postingOf(3)},
	}
	ov := fieldOverlay{
		base: base,
		delta: &fieldIndexDelta{
			entries: []fieldDeltaKV{
				{key: "aa", entry: indexDeltaEntry{delSingle: 1, delSingleSet: true}},
				{key: "bb", entry: indexDeltaEntry{addSingle: 22, addSingleSet: true}},
				{key: "bc", entry: indexDeltaEntry{addSingle: 23, addSingleSet: true}},
				{key: "zz", entry: indexDeltaEntry{addSingle: 99, addSingleSet: true, delSingle: 99, delSingleSet: true}},
			},
		},
	}

	br := ov.rangeForBounds(rangeBounds{
		has:   true,
		hasLo: true,
		loKey: "aa",
		loInc: true,
		hasHi: true,
		hiKey: "bc",
		hiInc: true,
	})
	rawApprox := (br.baseEnd - br.baseStart) + (br.deltaEnd - br.deltaStart)
	if rawApprox == 2 {
		t.Fatalf("test setup must expose approximation drift")
	}
	if got := overlayApproxDistinctRangeCount(ov, br); got != 2 {
		t.Fatalf("unexpected corrected range distinct count: got=%d want=2", got)
	}
}

func TestRangeContainsThresholds_Adaptive(t *testing.T) {
	sparseSmall := rangeLinearContainsLimit(128, 128)
	denseSmall := rangeLinearContainsLimit(128, 16_384)
	if sparseSmall <= denseSmall {
		t.Fatalf("expected wider linear limit for sparse probe: sparse=%d dense=%d", sparseSmall, denseSmall)
	}

	// Use probe width where density scaling is observable and doesn't collapse to clamp=1.
	afterSparse := rangeMaterializeAfterForProbe(2_048, 2_048)
	afterDense := rangeMaterializeAfterForProbe(2_048, 2_048*256)
	if afterSparse >= afterDense {
		t.Fatalf("expected sparse probe to materialize earlier than dense: sparse=%d dense=%d", afterSparse, afterDense)
	}

	afterSmall := rangeMaterializeAfterForProbe(256, 2_048)
	afterLarge := rangeMaterializeAfterForProbe(8_192, 65_536)
	if afterLarge >= afterSmall {
		t.Fatalf("expected wider probe to materialize earlier: small=%d large=%d", afterSmall, afterLarge)
	}
}

func TestPlannerExecutionOrderFactors_Adaptive(t *testing.T) {
	baseProfile := plannerOrderedProfile{
		coverage:     1.0,
		activeChecks: 2,
	}
	baseBase, baseCheck, baseRange, basePrefix := plannerExecutionOrderFactors(baseProfile, 1.2, 500_000)

	narrowProfile := plannerOrderedProfile{
		coverage:     0.10,
		activeChecks: 2,
	}
	narrowBase, narrowCheck, narrowRange, narrowPrefix := plannerExecutionOrderFactors(narrowProfile, 1.2, 500_000)
	if narrowBase >= baseBase || narrowCheck >= baseCheck || narrowRange >= baseRange || narrowPrefix >= basePrefix {
		t.Fatalf(
			"expected narrower coverage to reduce factors: base=(%.3f %.3f %.3f %.3f) narrow=(%.3f %.3f %.3f %.3f)",
			baseBase, baseCheck, baseRange, basePrefix,
			narrowBase, narrowCheck, narrowRange, narrowPrefix,
		)
	}

	_, lowSkewCheck, _, _ := plannerExecutionOrderFactors(baseProfile, 1.2, 500_000)
	_, highSkewCheck, _, _ := plannerExecutionOrderFactors(baseProfile, 5.0, 500_000)
	if highSkewCheck <= lowSkewCheck {
		t.Fatalf("expected higher skew to increase check factor: low=%.3f high=%.3f", lowSkewCheck, highSkewCheck)
	}
}

func TestPlannerOrderedFallbackProbeFactor_Adaptive(t *testing.T) {
	profile := plannerOrderedProfile{
		coverage:         0.25,
		hasPrefix:        true,
		orderRangeLeaves: 1,
	}
	noOffset := plannerOrderedFallbackProbeFactor(2.0, profile, 0)
	withOffset := plannerOrderedFallbackProbeFactor(2.0, profile, 1_000)
	if withOffset >= noOffset {
		t.Fatalf("expected offset shape to reduce fallback probe factor: no_offset=%.3f with_offset=%.3f", noOffset, withOffset)
	}

	lowSkew := plannerOrderedFallbackProbeFactor(1.2, profile, 0)
	highSkew := plannerOrderedFallbackProbeFactor(5.0, profile, 0)
	if highSkew <= lowSkew {
		t.Fatalf("expected higher skew to increase fallback probe factor: low=%.3f high=%.3f", lowSkew, highSkew)
	}
}

func TestPlannerCalibration_InfluencesORNoOrderDecision(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
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

	branches := plannerORBranches{
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
	db, _ := openTempDBUint64(t, Options{
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
	if err = db.SaveCalibration(path); err != nil {
		t.Fatalf("SaveCalibration: %v", err)
	}

	db2, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:    -1,
		CalibrationEnabled: true,
	})
	if err = db2.LoadCalibration(path); err != nil {
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

	opts := Options{
		AnalyzeInterval:    -1,
		CalibrationEnabled: true,
		PersistCalibration: true,
	}

	db, raw := openBoltAndNew[uint64, Rec](t, dbPath, opts)
	calPath := db.planner.calibrator.persistPath

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
	if err := raw.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}
	if _, err := os.Stat(calPath); err != nil {
		t.Fatalf("expected persisted calibration file %q: %v", calPath, err)
	}

	db2, raw2 := openBoltAndNew[uint64, Rec](t, dbPath, opts)
	defer func() { _ = db2.Close() }()
	defer func() { _ = raw2.Close() }()

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
	db, _ := openTempDBUint64(t, Options{
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
	preds := make([]predicate, 0, 1+extraChecks)
	preds = append(preds, predicate{
		estCard:  estCard,
		iter:     func() roaringIter { return roaring64.NewBitmap().Iterator() },
		contains: func(uint64) bool { return true },
	})
	for i := 0; i < extraChecks; i++ {
		preds = append(preds, predicate{
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
	db.planner.stats.Store(&plannerStatsSnapshot{
		Version:             1,
		UniverseCardinality: universe,
		Fields: map[string]PlannerFieldStats{
			"score": scoreStats,
		},
	})
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

	db, _ := openTempDBUint64(t, Options{
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
	db, _ := openTempDBUint64(t, Options{
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
	db, _ := openTempDBUint64(t, Options{
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

func TestPlannerOROrderMergePaths_MatchBasic_WithOrderDelta(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	rec, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	rec.Age++
	if err := db.Set(1, rec); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	ov := db.fieldOverlay("age")
	if ov.delta == nil {
		t.Fatalf("expected non-nil age delta overlay")
	}

	q := normalizeQuery(qx.Query(
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
	).By("age", qx.ASC).Skip(30).Max(120))

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

	branchesFallback, alwaysFalse, ok := db.buildORBranches(q.Expr.Operands)
	if !ok {
		t.Fatalf("buildORBranches fallback: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for fallback branches")
	}
	defer releaseORBranches(branchesFallback)

	gotFallback, ok, err := db.execPlanOROrderMergeFallback(q, branchesFallback, nil)
	if err != nil {
		t.Fatalf("execPlanOROrderMergeFallback err: %v", err)
	}
	if !ok {
		t.Fatalf("execPlanOROrderMergeFallback: ok=false")
	}

	branchesBasic, alwaysFalse, ok := db.buildORBranches(q.Expr.Operands)
	if !ok {
		t.Fatalf("buildORBranches basic: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for basic branches")
	}
	defer releaseORBranches(branchesBasic)

	gotBasic, ok := db.execPlanOROrderBasic(q, branchesBasic, nil)
	if !ok {
		t.Fatalf("execPlanOROrderBasic: ok=false")
	}

	if !slices.Equal(gotKWay, gotBasic) {
		t.Fatalf("kway/basic mismatch with order delta:\nkway=%v\nbasic=%v", gotKWay, gotBasic)
	}
	if !slices.Equal(gotFallback, gotBasic) {
		t.Fatalf("fallback/basic mismatch with order delta:\nfallback=%v\nbasic=%v", gotFallback, gotBasic)
	}
}

func TestPlannerOROrderMergePaths_WithDeltaInsertDelete_MatchSeqScan(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	// Create order-field delta with both insertion and tombstone scenarios.
	idTop := uint64(30_001)
	if err := db.Set(idTop, &Rec{
		Meta:     Meta{Country: "NL"},
		Name:     "alice",
		Email:    "delta-top@example.test",
		Age:      34,
		Score:    9_999.9,
		Active:   true,
		Tags:     []string{"go", "ops"},
		FullName: "FN-DELTA-TOP",
	}); err != nil {
		t.Fatalf("Set(idTop): %v", err)
	}

	idGone := uint64(30_002)
	if err := db.Set(idGone, &Rec{
		Meta:     Meta{Country: "DE"},
		Name:     "alice",
		Email:    "delta-gone@example.test",
		Age:      31,
		Score:    9_995.7,
		Active:   true,
		Tags:     []string{"go", "ops"},
		FullName: "FN-DELTA-GONE",
	}); err != nil {
		t.Fatalf("Set(idGone): %v", err)
	}
	if err := db.Delete(idGone); err != nil {
		t.Fatalf("Delete(idGone): %v", err)
	}

	ov := db.fieldOverlay("score")
	if ov.delta == nil {
		t.Fatalf("expected non-nil score delta overlay")
	}

	q := normalizeQuery(qx.Query(
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
	).By("score", qx.DESC).Max(120))

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	if len(want) == 0 || want[0] != idTop {
		t.Fatalf("expected delta-top id as first result, got=%v", want)
	}
	for _, id := range want {
		if id == idGone {
			t.Fatalf("deleted id leaked into expected set: %d", idGone)
		}
	}

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

	branchesFallback, alwaysFalse, ok := db.buildORBranches(q.Expr.Operands)
	if !ok {
		t.Fatalf("buildORBranches fallback: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for fallback branches")
	}
	defer releaseORBranches(branchesFallback)

	gotFallback, ok, err := db.execPlanOROrderMergeFallback(q, branchesFallback, nil)
	if err != nil {
		t.Fatalf("execPlanOROrderMergeFallback err: %v", err)
	}
	if !ok {
		t.Fatalf("execPlanOROrderMergeFallback: ok=false")
	}

	if !slices.Equal(gotKWay, want) {
		t.Fatalf("kway/seqscan mismatch:\nkway=%v\nwant=%v", gotKWay, want)
	}
	if !slices.Equal(gotFallback, want) {
		t.Fatalf("fallback/seqscan mismatch:\nfallback=%v\nwant=%v", gotFallback, want)
	}
}

func TestPlannerOROrderMergePaths_MixedExactAndNonExactChecks_MatchSeqScan(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	q := normalizeQuery(qx.Query(
		qx.OR(
			qx.AND(
				qx.EQ("active", true),
				qx.HASANY("tags", []string{"go", "ops"}),
				qx.GTE("score", 40.0),
				qx.GTE("age", 25),
			),
			qx.AND(
				qx.IN("country", []string{"NL", "DE", "PL"}),
				qx.HASANY("tags", []string{"db", "rust"}),
				qx.GTE("score", 30.0),
			),
			qx.AND(
				qx.EQ("name", "alice"),
				qx.GTE("age", 20),
				qx.LTE("age", 45),
			),
		),
	).By("score", qx.DESC).Max(120))

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	if len(want) == 0 {
		t.Fatalf("expected non-empty result set")
	}

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

	branchesFallback, alwaysFalse, ok := db.buildORBranches(q.Expr.Operands)
	if !ok {
		t.Fatalf("buildORBranches fallback: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for fallback branches")
	}
	defer releaseORBranches(branchesFallback)

	gotFallback, ok, err := db.execPlanOROrderMergeFallback(q, branchesFallback, nil)
	if err != nil {
		t.Fatalf("execPlanOROrderMergeFallback err: %v", err)
	}
	if !ok {
		t.Fatalf("execPlanOROrderMergeFallback: ok=false")
	}

	if !slices.Equal(gotKWay, want) {
		t.Fatalf("kway/seqscan mismatch:\nkway=%v\nwant=%v", gotKWay, want)
	}
	if !slices.Equal(gotFallback, want) {
		t.Fatalf("fallback/seqscan mismatch:\nfallback=%v\nwant=%v", gotFallback, want)
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
	if !plannerORKWayShouldFallbackRuntime(250_000, 3_000, 2_900, 300_000) {
		t.Fatalf("expected fallback for large-window low-overlap stream")
	}
}

func TestPlannerOROrderKWayRuntimeFallbackEnable(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
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
	db, _ := openTempDBUint64(t, Options{
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
	ov := db.fieldOverlay(orderField)
	slice := materializeFieldOverlay(ov)
	if slice == nil {
		t.Fatalf("materialized order slice is nil")
	}
	s := *slice
	start, end := 0, len(s)
	if st, en, cov, ok := db.extractOrderRangeCoverage(orderField, predsB, s); ok {
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

	db, _ := openTempDBUint64(t, Options{
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

	if ev.Plan != "plan_ordered_anchor" && ev.Plan != "plan_ordered" {
		t.Fatalf("expected ordered plan (anchor/basic), got %q", ev.Plan)
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

	db, _ := openTempDBUint64(t, Options{
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
	if ev.RowsExamined == 0 {
		t.Fatalf("expected rows examined to be set")
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

	db, _ := openTempDBUint64(t, Options{
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

	db, _ := openTempDBUint64(t, Options{
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
	db, _ := openTempDBUint64(t, Options{
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

func TestPlannerRouting_AvoidsExecutionForWidePrefixZeroHitRate(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	q := normalizeQuery(qx.Query(
		qx.PREFIX("full_name", "FN-"),
		qx.EQ("full_name", "FN-999999999"),
	).Max(15))

	leaves, ok := extractAndLeaves(q.Expr)
	if !ok || len(leaves) == 0 {
		t.Fatalf("extractAndLeaves: ok=%v len=%d", ok, len(leaves))
	}
	if db.shouldPreferExecutionNoOrderPrefix(q, leaves) {
		t.Fatalf("expected fallback/planner preference for zero-hit wide prefix shape")
	}
}

func TestPlannerOROrderMergeFallbackFirst_ComplexOffset(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
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
		needComplex, ok := orderWindow(qComplex)
		if !ok {
			t.Fatalf("orderWindow complex: ok=false")
		}
		if !db.shouldUseOROrderKWayRuntimeFallback(qComplex, branchesComplex, needComplex) {
			t.Fatalf("expected either fallback-first or runtime fallback guard for complex offset ordered OR")
		}
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

func TestPlannerOROrderMergeFallbackFirst_DisabledWithOrderDelta(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	rec, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	rec.Score += 0.314159
	if err := db.Set(1, rec); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	ov := db.fieldOverlay("score")
	if ov.delta == nil {
		t.Fatalf("expected non-nil score delta overlay")
	}

	q := normalizeQuery(qx.Query(
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

	branches, alwaysFalse, ok := db.buildORBranches(q.Expr.Operands)
	if !ok {
		t.Fatalf("buildORBranches: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse branches")
	}
	defer releaseORBranches(branches)

	if db.shouldPreferOROrderFallbackFirst(q, branches) {
		t.Fatalf("fallback-first must stay disabled when order field overlay has delta")
	}
}

func TestTracer_OROrderRouteDecision_WithOrderDelta(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	rec, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	rec.Score += 0.271828
	if err := db.Set(1, rec); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	ov := db.fieldOverlay("score")
	if ov.delta == nil {
		t.Fatalf("expected non-nil score delta overlay")
	}

	q := normalizeQuery(qx.Query(
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

	branches, alwaysFalse, ok := db.buildORBranches(q.Expr.Operands)
	if !ok {
		t.Fatalf("buildORBranches: ok=false")
	}
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse branches")
	}
	defer releaseORBranches(branches)

	trace := &queryTrace{}
	_, _, err = db.execPlanOROrderMerge(q, branches, trace)
	if err != nil {
		t.Fatalf("execPlanOROrderMerge: %v", err)
	}

	if trace.ev.ORRoute.Route != "kway_first" {
		t.Fatalf("expected kway_first route, got %q", trace.ev.ORRoute.Route)
	}
	if trace.ev.ORRoute.Reason != "order_delta_present" {
		t.Fatalf("expected route reason order_delta_present, got %q", trace.ev.ORRoute.Reason)
	}
	if trace.ev.ORRoute.FallbackCollectFast {
		t.Fatalf("expected fallbackCollectFast=false with order delta")
	}
	if trace.ev.ORRoute.KWayCost <= 0 || trace.ev.ORRoute.FallbackCost <= 0 {
		t.Fatalf(
			"expected positive route costs: kway=%v fallback=%v",
			trace.ev.ORRoute.KWayCost, trace.ev.ORRoute.FallbackCost,
		)
	}
	if trace.ev.ORRoute.RuntimeGuardReason == "" {
		t.Fatalf("expected runtime guard reason in OR route trace")
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

	db, _ := openTempDBUint64(t, Options{
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

func TestPlannerRouting_OrderLimitWithSecondaryRange_MatchesSeqScan(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	sink := func(ev TraceEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSink:        sink,
		TraceSampleEvery: 1,
	})
	_ = seedData(t, db, 20_000)

	q := normalizeQuery(qx.Query(
		qx.EQ("active", true),
		qx.GTE("score", 30.0),
		qx.GTE("age", 22),
	).By("score", qx.DESC).Max(80))

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertQueryIDsEqual(t, q, got, want)

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	if ev.Plan == "" {
		t.Fatal("expected plan name in trace event")
	}
	if ev.RowsReturned != uint64(len(got)) {
		t.Fatalf("rows returned mismatch: ev=%d got=%d", ev.RowsReturned, len(got))
	}
	if ev.RowsExamined == 0 {
		t.Fatalf("expected rows examined to be recorded")
	}
}

func TestPlannerRouting_OrderLimitWithSecondaryRangeAndHasAny_MatchesSeqScan(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	sink := func(ev TraceEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSink:        sink,
		TraceSampleEvery: 1,
	})
	_ = seedData(t, db, 20_000)

	q := normalizeQuery(qx.Query(
		qx.EQ("active", true),
		qx.HASANY("tags", []string{"go", "db", "ops"}),
		qx.GTE("score", 30.0),
		qx.GTE("age", 22),
	).By("score", qx.DESC).Max(80))

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertQueryIDsEqual(t, q, got, want)

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	if ev.Plan == "" {
		t.Fatal("expected plan name in trace event")
	}
	if ev.RowsReturned != uint64(len(got)) {
		t.Fatalf("rows returned mismatch: ev=%d got=%d", ev.RowsReturned, len(got))
	}
	if ev.RowsExamined == 0 {
		t.Fatalf("expected rows examined to be recorded")
	}
}

func TestPlannerRouting_OrderedNoOrderWithNegativeIN_MatchesSeqScan(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	sink := func(ev TraceEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSink:        sink,
		TraceSampleEvery: 1,
	})
	_ = seedData(t, db, 20_000)

	q := normalizeQuery(qx.Query(
		qx.LT("age", 35),
		qx.NOTIN("country", []string{"NL", "DE"}),
		qx.NOTIN("name", []string{"alice", "bob"}),
		qx.LT("score", 50.0),
	).Max(120))

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	fullQ := cloneQuery(q)
	fullQ.Offset = 0
	fullQ.Limit = 0
	full, err := expectedKeysUint64(t, db, fullQ)
	if err != nil {
		t.Fatalf("expectedKeysUint64(full): %v", err)
	}
	assertNoOrderWindowSubset(t, q, got, full, "ordered_no_order")
	if len(full) >= int(q.Limit) && len(got) != int(q.Limit) {
		t.Fatalf("expected full page for ordered_no_order route: got=%d limit=%d full=%d", len(got), q.Limit, len(full))
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	if ev.Plan == "" {
		t.Fatal("expected plan name in trace event")
	}
	if ev.RowsReturned != uint64(len(got)) {
		t.Fatalf("rows returned mismatch: ev=%d got=%d", ev.RowsReturned, len(got))
	}
	if ev.RowsExamined == 0 {
		t.Fatalf("expected rows examined to be recorded")
	}
}

func TestPlannerCandidateOrder_MatchesSeqScan(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	q := normalizeQuery(qx.Query(
		qx.EQ("active", true),
		qx.NOTIN("country", []string{"NL", "DE"}),
		qx.IN("name", []string{"alice", "bob", "carol"}),
	).By("age", qx.ASC).Max(120))

	leaves, ok := collectAndLeaves(q.Expr)
	if !ok || len(leaves) == 0 {
		t.Fatalf("collectAndLeaves: ok=%v len=%d", ok, len(leaves))
	}
	if !db.shouldUseCandidateOrder(q.Order[0], leaves) {
		t.Fatalf("expected candidate-order precheck to pass for query shape")
	}

	got, used, err := db.tryPlanCandidate(q, nil)
	if err != nil {
		t.Fatalf("tryPlanCandidate: %v", err)
	}
	if !used {
		t.Fatalf("expected candidate-order route to be used")
	}

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertSameSlice(t, got, want)
}

func TestPlannerCandidateNoOrder_RespectsWindowAndPredicateSet(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})
	_ = seedData(t, db, 20_000)

	q := normalizeQuery(qx.Query(
		qx.EQ("active", true),
		qx.NOTIN("country", []string{"NL", "DE"}),
		qx.IN("name", []string{"alice", "bob", "carol"}),
	).Max(90))

	got, used, err := db.tryPlanCandidate(q, nil)
	if err != nil {
		t.Fatalf("tryPlanCandidate: %v", err)
	}
	if !used {
		t.Fatalf("expected candidate no-order route to be used")
	}

	fullQ := cloneQuery(q)
	fullQ.Offset = 0
	fullQ.Limit = 0
	full, err := expectedKeysUint64(t, db, fullQ)
	if err != nil {
		t.Fatalf("expectedKeysUint64(full): %v", err)
	}
	assertNoOrderWindowSubset(t, q, got, full, "candidate_no_order")

	if len(full) >= int(q.Limit) && len(got) != int(q.Limit) {
		t.Fatalf("expected full page for no-order candidate route: got=%d limit=%d full=%d", len(got), q.Limit, len(full))
	}
}
