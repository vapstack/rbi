package qexec

import (
	"testing"
	"unsafe"

	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/rbistats"
)

func TestP2Quantile_WarmupKeepsExactPercentileAtFiveSamples(t *testing.T) {
	q := newP2Quantile(0.95)
	for _, v := range []uint64{1, 2, 3, 4, 100} {
		q.observe(v)
	}
	if got := q.value(); got != 100 {
		t.Fatalf("unexpected p95 during warmup: got=%d want=100", got)
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

	db.engine.exec.PublishLoadedPlannerStats(&rbistats.PlannerSnapshot{
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

func TestPlannerStatsSnapshotIncludesStringKeyIndex(t *testing.T) {
	db := newTestDB(t, testOptions{})
	db.enableStringKeyCatalog()

	vals := []testRec{
		{Name: "alice", Age: 30},
		{Name: "bob", Age: 31},
		{Name: "carol", Age: 32},
	}
	entries := make([]snapshot.BatchEntry, len(vals))
	keyDeltas := []snapshot.KeyDelta{
		{ID: 1, Key: "sku-001", Add: true},
		{ID: 2, Key: "sku-002", Add: true},
		{ID: 3, Key: "sku-003", Add: true},
	}
	for i := range vals {
		entries[i] = snapshot.BatchEntry{ID: uint64(i + 1), New: unsafe.Pointer(&vals[i])}
	}
	db.seq++
	db.snap = snapshot.BuildWithKeyDeltas(db.seq, db.snap, db.rt, db.cfg, nil, entries, keyDeltas)

	built := db.exec.BuildPlannerStatsSnapshot(db.snap, 21)
	assertStringKeyPlannerStats(t, built, uint64(len(vals)))

	loadedFields := make(map[string]rbistats.PlannerField, len(built.Fields)-1)
	for field, stats := range built.Fields {
		if field != schema.ReservedKeyFieldName {
			loadedFields[field] = stats
		}
	}
	db.exec.Stats.Store(&rbistats.PlannerSnapshot{
		Version: 20,
		Fields:  loadedFields,
	})
	persisted := db.exec.PlannerStatsSnapshotForPersist(db.snap, 22)
	assertStringKeyPlannerStats(t, persisted, uint64(len(vals)))

	db.exec.PublishLoadedPlannerStats(&rbistats.PlannerSnapshot{
		Version: 21,
		Fields:  loadedFields,
	}, db.snap)
	loaded := db.exec.Stats.Load()
	if loaded == nil {
		t.Fatal("loaded planner stats snapshot is nil")
	}
	assertStringKeyPlannerStats(t, loaded, uint64(len(vals)))
}

func assertStringKeyPlannerStats(t testing.TB, snap *rbistats.PlannerSnapshot, rows uint64) {
	t.Helper()
	stats, ok := snap.Fields[schema.ReservedKeyFieldName]
	if !ok {
		t.Fatalf("planner stats missing %q", schema.ReservedKeyFieldName)
	}
	if stats.DistinctKeys != rows || stats.NonEmptyKeys != rows || stats.TotalBucketCard != rows {
		t.Fatalf("unexpected $key cardinalities: %+v rows=%d", stats, rows)
	}
	if stats.AvgBucketCard != 1 || stats.MaxBucketCard != 1 || stats.P50BucketCard != 1 || stats.P95BucketCard != 1 {
		t.Fatalf("unexpected $key bucket distribution: %+v", stats)
	}
}
