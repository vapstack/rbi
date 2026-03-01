package rbi

import (
	"testing"
	"time"
)

func TestStats_ExposePlannerCalibrationAndSnapshotDiagnostics(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options{
		CalibrationEnabled:     true,
		CalibrationSampleEvery: 1,
		AnalyzeInterval:        -1,
	})

	if err := db.Set(1, &Rec{Name: "alice", Age: 10, Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "bob", Age: 20, Tags: []string{"db"}}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	db.observeCalibration(TraceEvent{
		Plan:          string(PlanOrdered),
		EstimatedRows: 64,
		RowsExamined:  96,
	})

	st := db.Stats()

	if st.Index.EntryCount == 0 {
		t.Fatalf("expected index diagnostics entry_count > 0")
	}
	if st.Index.ApproxHeapBytes < st.Index.Size {
		t.Fatalf("expected approx heap bytes >= index size, got approx=%d index=%d", st.Index.ApproxHeapBytes, st.Index.Size)
	}
	if st.Runtime.Goroutines == 0 {
		t.Fatalf("expected runtime goroutine count > 0")
	}
	if st.Runtime.HeapAlloc == 0 {
		t.Fatalf("expected runtime heap_alloc > 0")
	}

	if st.Snapshot.TxID == 0 {
		t.Fatalf("expected snapshot txID > 0")
	}
	if st.Snapshot.RegistrySize == 0 {
		t.Fatalf("expected snapshot registry to be non-empty")
	}
	if st.Snapshot.UniverseBaseCard+st.Snapshot.UniverseAddCard == 0 {
		t.Fatalf("expected snapshot universe cardinality (base+add) > 0")
	}

	if st.Planner.Version == 0 {
		t.Fatalf("expected planner stats version > 0")
	}
	if st.Planner.GeneratedAt.IsZero() {
		t.Fatalf("expected planner generated_at to be set")
	}
	if st.Planner.FieldCount == 0 {
		t.Fatalf("expected planner field_count > 0")
	}
	if st.Planner.AnalyzeInterval != 0 {
		t.Fatalf("expected disabled analyze interval (0), got %v", st.Planner.AnalyzeInterval)
	}

	if !st.Calibration.Enabled {
		t.Fatalf("expected calibration to be enabled in stats")
	}
	if st.Calibration.SampleEvery != 1 {
		t.Fatalf("expected calibration sample_every=1, got %d", st.Calibration.SampleEvery)
	}
	if st.Calibration.UpdatedAt.IsZero() {
		t.Fatalf("expected calibration updated_at to be set")
	}
	if st.Calibration.SamplesTotal == 0 {
		t.Fatalf("expected calibration samples_total > 0")
	}
	if st.Calibration.Samples[string(PlanOrdered)] == 0 {
		t.Fatalf("expected calibration samples for %q > 0", PlanOrdered)
	}
	if _, ok := st.Calibration.Multipliers[string(PlanOrdered)]; !ok {
		t.Fatalf("expected calibration multiplier for %q to be present", PlanOrdered)
	}

	if !st.Batch.Enabled {
		t.Fatalf("expected write combine stats to report enabled")
	}
	if st.Batch.Window <= 0 {
		t.Fatalf("expected positive write combine window, got %v", st.Batch.Window)
	}
	if st.Batch.Enqueued == 0 {
		t.Fatalf("expected write combine stats to observe enqueued writes")
	}
}

func TestStats_PreservesIndexTimingFields(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	db.stats.Index.BuildTime = 123 * time.Millisecond
	db.stats.Index.BuildRPS = 456
	db.stats.Index.LoadTime = 789 * time.Millisecond

	st := db.Stats()

	if st.Index.BuildTime != db.stats.Index.BuildTime {
		t.Fatalf("expected BuildTime=%v, got %v", db.stats.Index.BuildTime, st.Index.BuildTime)
	}
	if st.Index.BuildRPS != db.stats.Index.BuildRPS {
		t.Fatalf("expected BuildRPS=%d, got %d", db.stats.Index.BuildRPS, st.Index.BuildRPS)
	}
	if st.Index.LoadTime != db.stats.Index.LoadTime {
		t.Fatalf("expected LoadTime=%v, got %v", db.stats.Index.LoadTime, st.Index.LoadTime)
	}
}

func TestStats_ComponentAccessors(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options{
		CalibrationEnabled:     true,
		CalibrationSampleEvery: 1,
		AnalyzeInterval:        -1,
	})

	if err := db.Set(1, &Rec{Name: "alice", Age: 10, Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "bob", Age: 20, Tags: []string{"db"}}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	db.observeCalibration(TraceEvent{
		Plan:          string(PlanOrdered),
		EstimatedRows: 64,
		RowsExamined:  96,
	})

	idx := db.IndexStats()
	if idx.Size == 0 {
		t.Fatalf("expected IndexStats.Size > 0")
	}
	if len(idx.UniqueFieldKeys) == 0 {
		t.Fatalf("expected IndexStats.UniqueFieldKeys to be populated")
	}

	rt := db.RuntimeStats()
	if rt.Goroutines == 0 {
		t.Fatalf("expected RuntimeStats.Goroutines > 0")
	}
	if rt.HeapAlloc == 0 {
		t.Fatalf("expected RuntimeStats.HeapAlloc > 0")
	}

	pl := db.PlannerStats()
	if pl.Version == 0 {
		t.Fatalf("expected PlannerStats.Version > 0")
	}
	if pl.GeneratedAt.IsZero() {
		t.Fatalf("expected PlannerStats.GeneratedAt to be set")
	}

	cal := db.CalibrationStats()
	if !cal.Enabled {
		t.Fatalf("expected CalibrationStats.Enabled=true")
	}
	if cal.SamplesTotal == 0 {
		t.Fatalf("expected CalibrationStats.SamplesTotal > 0")
	}

	snap := db.SnapshotStats()
	if snap.TxID == 0 {
		t.Fatalf("expected SnapshotStats.TxID > 0")
	}

	bs := db.BatchStats()
	if !bs.Enabled {
		t.Fatalf("expected BatchStats.Enabled=true")
	}
	if bs.Window <= 0 {
		t.Fatalf("expected BatchStats.Window > 0")
	}
}
