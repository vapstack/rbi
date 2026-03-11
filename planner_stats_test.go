package rbi

import (
	"testing"
	"time"
)

func TestPlannerStatsCollector_FullRefreshMatchesLockedSnapshot(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 3_000)

	db.mu.RLock()
	expected := db.buildPlannerStatsSnapshotLocked(1)
	db.mu.RUnlock()

	if err := db.RefreshPlannerStats(); err != nil {
		t.Fatalf("RefreshPlannerStats: %v", err)
	}

	got := db.PlannerStats()

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
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
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

	s0 := db.PlannerStats()
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

		s := db.PlannerStats()
		if s.Version <= prevVersion {
			t.Fatalf("version did not advance at cycle %d: got=%d prev=%d", i, s.Version, prevVersion)
		}
		prevVersion = s.Version
	}
}

func TestResolvePlannerAnalyzeInterval(t *testing.T) {
	if got := plannerAnalyzeInterval(-1); got != 0 {
		t.Fatalf("negative interval should disable scheduler: got=%v", got)
	}
	if got := plannerAnalyzeInterval(0); got != defaultOptionsAnalyzeInterval {
		t.Fatalf("zero interval should map to default: got=%v want=%v", got, defaultOptionsAnalyzeInterval)
	}
	custom := 37 * time.Second
	if got := plannerAnalyzeInterval(custom); got != custom {
		t.Fatalf("custom interval mismatch: got=%v want=%v", got, custom)
	}
}

func TestPlannerAnalyzeScheduler_Disabled(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	if db.planner.analyzer.stop != nil || db.planner.analyzer.done != nil {
		t.Fatalf("scheduler should be disabled for negative interval")
	}

	s0 := db.PlannerStats()

	time.Sleep(30 * time.Millisecond)

	s1 := db.PlannerStats()
	if s1.Version != s0.Version {
		t.Fatalf("snapshot version changed while scheduler disabled: before=%d after=%d", s0.Version, s1.Version)
	}
}

func TestPlannerAnalyzeScheduler_StartAndStop(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: 10 * time.Millisecond})

	if db.planner.analyzer.stop == nil || db.planner.analyzer.done == nil {
		t.Fatalf("scheduler should be started")
	}

	s0 := db.PlannerStats()

	if latest, ok := waitPlannerStatsVersionGreater(db, s0.Version, 250*time.Millisecond); !ok {
		t.Fatalf("expected background refresh to advance snapshot version: start=%d latest=%d", s0.Version, latest)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if db.planner.analyzer.stop != nil || db.planner.analyzer.done != nil {
		t.Fatalf("scheduler channels must be cleared on close")
	}

	closedSnapshot := db.PlannerStats()

	time.Sleep(35 * time.Millisecond)

	afterSnapshot := db.PlannerStats()
	if afterSnapshot.Version != closedSnapshot.Version {
		t.Fatalf("snapshot version changed after close: before=%d after=%d", closedSnapshot.Version, afterSnapshot.Version)
	}
}

func waitPlannerStatsVersionGreater(db *DB[uint64, Rec], version uint64, timeout time.Duration) (uint64, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		s := db.PlannerStats()
		if s.Version > version {
			return s.Version, true
		}
		time.Sleep(2 * time.Millisecond)
	}
	s := db.PlannerStats()
	if s.Version == 0 {
		return 0, false
	}
	return s.Version, false
}

func TestPlannerStats_RefreshAndVersion(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 1_000)

	s0 := db.PlannerStats()

	if err := db.RefreshPlannerStats(); err != nil {
		t.Fatalf("RefreshPlannerStats: %v", err)
	}

	s1 := db.PlannerStats()

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
	s2 := db.PlannerStats()
	if s2.Fields["country"].DistinctKeys == 0 {
		t.Fatalf("snapshot was mutated by caller")
	}
}
