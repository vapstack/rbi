package qexec

import "testing"

func TestP2Quantile_WarmupKeepsExactPercentileAtFiveSamples(t *testing.T) {
	q := newP2Quantile(0.95)
	for _, v := range []uint64{1, 2, 3, 4, 100} {
		q.observe(v)
	}
	if got := q.value(); got != 100 {
		t.Fatalf("unexpected p95 during warmup: got=%d want=100", got)
	}
}

func TestPlannerStatsCollector_PeriodicBudgetAdvancesCursor(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 3_000)

	fieldCount := len(db.engine.schema.Indexed)
	if fieldCount < 2 {
		t.Skip("not enough indexed fields for cursor progression test")
	}

	db.engine.exec.Analyzer.Lock()
	db.engine.exec.Analyzer.Cursor = 0
	db.engine.exec.Analyzer.SoftBudget = 1 // 1ns, effectively one heavy field per cycle
	db.engine.exec.Analyzer.Unlock()

	prevVersion := uint64(0)
	prevCursor := 0

	cycles := fieldCount + 3
	for i := 0; i < cycles; i++ {
		db.engine.exec.Analyzer.Lock()
		db.engine.exec.RefreshPlannerStatsOnSnapshot(db.engine.snapshot.Current(), db.engine.exec.Analyzer.SoftBudget, true)
		curCursor := db.engine.exec.Analyzer.Cursor
		db.engine.exec.Analyzer.Unlock()

		wantCursor := (prevCursor + 1) % fieldCount
		if curCursor != wantCursor {
			t.Fatalf("cursor mismatch at cycle %d: got=%d want=%d", i, curCursor, wantCursor)
		}
		prevCursor = curCursor

		s := db.engine.exec.Stats.Load()
		if s == nil || s.Version <= prevVersion {
			t.Fatalf("version did not advance at cycle %d: got=%v prev=%d", i, s, prevVersion)
		}
		prevVersion = s.Version
	}
}

func TestPlannerStatsSnapshot_BuildPersistAndPublish(t *testing.T) {
	db := newCorrectnessDB(t, Options{AnalyzeInterval: -1})
	snap := db.engine.snapshot.Current()

	built := db.engine.exec.BuildPlannerStatsSnapshot(snap, 42)
	if built.Version != 42 {
		t.Fatalf("built version mismatch: got=%d want=42", built.Version)
	}
	if built.UniverseCardinality != uint64(len(db.values)) {
		t.Fatalf("built universe mismatch: got=%d want=%d", built.UniverseCardinality, len(db.values))
	}
	age := built.Fields["age"]
	if age.DistinctKeys == 0 || age.TotalBucketCard == 0 || age.MaxBucketCard == 0 {
		t.Fatalf("age stats were not collected: %+v", age)
	}

	persisted := db.engine.exec.PlannerStatsSnapshotForPersist(snap, 43)
	if persisted.Version != 43 {
		t.Fatalf("persist version mismatch: got=%d want=43", persisted.Version)
	}
	if persisted.UniverseCardinality != uint64(len(db.values)) {
		t.Fatalf("persist universe mismatch: got=%d want=%d", persisted.UniverseCardinality, len(db.values))
	}
	if len(persisted.Fields) != len(built.Fields) {
		t.Fatalf("persist fields mismatch: got=%d want=%d", len(persisted.Fields), len(built.Fields))
	}

	db.engine.exec.PublishLoadedPlannerStats(&PlannerStatsSnapshot{
		Version:     7,
		GeneratedAt: built.GeneratedAt,
		Fields:      built.Fields,
	}, snap)

	loaded := db.engine.exec.Stats.Load()
	if loaded == nil {
		t.Fatal("loaded stats snapshot is nil")
	}
	if loaded.Version != 7 {
		t.Fatalf("loaded version mismatch: got=%d want=7", loaded.Version)
	}
	if loaded.UniverseCardinality != uint64(len(db.values)) {
		t.Fatalf("loaded universe mismatch: got=%d want=%d", loaded.UniverseCardinality, len(db.values))
	}
	if loaded.Fields["age"] != age {
		t.Fatalf("loaded age stats mismatch: got=%+v want=%+v", loaded.Fields["age"], age)
	}

	persisted = db.engine.exec.PlannerStatsSnapshotForPersist(snap, 8)
	if persisted.Version != 8 {
		t.Fatalf("loaded persist version mismatch: got=%d want=8", persisted.Version)
	}
	if persisted.Fields["age"] != age {
		t.Fatalf("loaded persist age stats mismatch: got=%+v want=%+v", persisted.Fields["age"], age)
	}
}
