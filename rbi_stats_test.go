package rbi

import (
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/vapstack/rbi/rbierrors"
	"github.com/vapstack/rbi/rbistats"

	"github.com/vapstack/qx"
	"go.etcd.io/bbolt"
)

type statsShapeRec struct {
	Name   string `db:"name" rbi:"index"`
	Email  string `db:"email" rbi:"unique"`
	Amount int64  `db:"amount" rbi:"measure"`
}

func TestAPI_Stats_ZeroAfterClose(t *testing.T) {
	db, _ := openTempDBUint64(t)
	mustSetAPIRec(t, db, 1, &Rec{Name: "alice", Age: 30})

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var zero rbistats.DB[uint64]
	got, err := db.Stats()
	if !errors.Is(err, rbierrors.ErrClosed) {
		t.Fatalf("expected rbierrors.ErrClosed after Close, got %v", err)
	}
	if got != zero {
		t.Fatalf("expected zero Stats after Close, got %+v", got)
	}
}

func TestAPI_IndexStats_ZeroAfterClose(t *testing.T) {
	db, _ := openTempDBUint64(t)
	mustSetAPIRec(t, db, 1, &Rec{Name: "alice", Age: 30})

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := db.IndexStats(); !reflect.DeepEqual(got, rbistats.Index{}) {
		t.Fatalf("expected zero IndexStats after Close, got %+v", got)
	}
}

func TestAPI_SnapshotStats_ZeroAfterClose(t *testing.T) {
	db, _ := openTempDBUint64(t)
	mustSetAPIRec(t, db, 1, &Rec{Name: "alice", Age: 30})

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var zero rbistats.Snapshot
	if got := db.SnapshotStats(); got != zero {
		t.Fatalf("expected zero SnapshotStats after Close, got %+v", got)
	}
}

func TestAPI_Stats_ReportsCheapSchemaAndModeFacts(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shape.db")
	db, raw := openBoltAndNew[uint64, statsShapeRec](t, path)
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	st, err := db.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if st.Mode != rbistats.ModeIndexed {
		t.Fatalf("Mode=%d want indexed", st.Mode)
	}
	if st.StringKeys {
		t.Fatalf("StringKeys=true for uint64 DB")
	}
	if st.IndexFieldCount != 3 {
		t.Fatalf("IndexFieldCount=%d want indexed fields 3", st.IndexFieldCount)
	}
	if st.MeasureFieldCount != 1 {
		t.Fatalf("MeasureFieldCount=%d want 1", st.MeasureFieldCount)
	}
	if st.UniqueFieldCount != 1 {
		t.Fatalf("UniqueFieldCount=%d want 1", st.UniqueFieldCount)
	}
}

func TestAPI_IndexStats_ReturnMapsAreCallerOwned(t *testing.T) {
	db, _ := openTempDBUint64(t)
	mustSetAPIRecs(t, db, map[uint64]*Rec{
		1: {Name: "alice", Age: 30, Tags: []string{"go"}},
		2: {Name: "bob", Age: 35, Tags: []string{"db"}},
	})

	s1 := db.IndexStats()
	if s1.FieldSize["age"] == 0 {
		t.Fatalf("expected age field stats to exist: %+v", s1)
	}

	s1.FieldSize["age"] = 0
	delete(s1.UniqueFieldKeys, "age")
	delete(s1.FieldTotalCardinality, "age")
	delete(s1.FieldApproxStructBytes, "age")
	delete(s1.FieldApproxHeapBytes, "age")

	s2 := db.IndexStats()
	if s2.FieldSize["age"] == 0 {
		t.Fatalf("caller mutation leaked into IndexStats.FieldSize")
	}
	if _, ok := s2.UniqueFieldKeys["age"]; !ok {
		t.Fatalf("caller mutation leaked into IndexStats.UniqueFieldKeys")
	}
	if _, ok := s2.FieldTotalCardinality["age"]; !ok {
		t.Fatalf("caller mutation leaked into IndexStats.FieldTotalCardinality")
	}
	if _, ok := s2.FieldApproxStructBytes["age"]; !ok {
		t.Fatalf("caller mutation leaked into IndexStats.FieldApproxStructBytes")
	}
	if _, ok := s2.FieldApproxHeapBytes["age"]; !ok {
		t.Fatalf("caller mutation leaked into IndexStats.FieldApproxHeapBytes")
	}
}

func TestAPI_ConcurrentStatsAccessAndWrites(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})

	for i := 1; i <= 16; i++ {
		mustSetAPIRec(t, db, uint64(i), &Rec{
			Name:   "seed",
			Age:    20 + i,
			Active: i%2 == 0,
			Tags:   []string{"seed"},
		})
	}

	errCh := make(chan error, 16)
	var wg sync.WaitGroup

	for w := 0; w < 2; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 80; i++ {
				id := uint64((w*37+i)%24 + 1)
				rec := &Rec{
					Name:   "writer",
					Age:    18 + ((w + i) % 50),
					Active: (w+i)%2 == 0,
					Tags:   []string{"w", "api"},
				}
				if err := db.Set(id, rec); err != nil {
					errCh <- err
					return
				}
				if err := db.Patch(id, []Field{{Name: "age", Value: 30 + ((w + i) % 20)}}); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}

	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 80; i++ {
				if _, err := db.Stats(); err != nil {
					errCh <- err
					return
				}
				_ = db.IndexStats()
				_ = db.SnapshotStats()
				_ = db.PlannerStats()
				_ = db.AutoBatchStats()
				_ = db.BucketName()
				items, err := db.Query(qx.Query(qx.GTE("age", 18)).Limit(8))
				if err != nil {
					errCh <- err
					return
				}
				releaseUniqueRecords(db, items...)
				if _, err := db.QueryKeys(qx.Query(qx.EQ("active", true)).Limit(8)); err != nil {
					errCh <- err
					return
				}
				if _, err := db.Count(); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent API operation failed: %v", err)
	}
}

func TestAPI_PlannerStats_ReturnMapsAreCallerOwned(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	mustSetAPIRecs(t, db, map[uint64]*Rec{
		1: {Name: "alice", Age: 30, Tags: []string{"go"}},
		2: {Name: "bob", Age: 35, Tags: []string{"db"}},
		3: {Name: "carol", Age: 35, Tags: []string{"ops"}},
	})

	if err := db.RefreshPlannerStats(); err != nil {
		t.Fatalf("RefreshPlannerStats: %v", err)
	}

	s1 := db.PlannerStats()
	age, ok := s1.Fields["age"]
	if !ok || age.DistinctKeys == 0 {
		t.Fatalf("expected age planner stats to exist: %+v", s1)
	}

	s1.Fields["age"] = rbistats.PlannerField{}
	delete(s1.Fields, "name")

	s2 := db.PlannerStats()
	if got := s2.Fields["age"]; got.DistinctKeys == 0 {
		t.Fatalf("caller mutation leaked into PlannerStats.Fields[age]: %+v", got)
	}
	if _, ok := s2.Fields["name"]; !ok {
		t.Fatalf("caller mutation leaked into PlannerStats.Fields[name]")
	}
}

func TestComponentAccessors_ExposePlannerAndSnapshotDiagnostics(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})

	if err := db.Set(1, &Rec{Name: "alice", Age: 10, Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "bob", Age: 20, Tags: []string{"db"}}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	st, err := db.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if st.KeyCount != 2 {
		t.Fatalf("expected Stats.KeyCount=2, got %d", st.KeyCount)
	}
	if st.LastKey != 2 {
		t.Fatalf("expected Stats.LastKey=2, got %d", st.LastKey)
	}
	if st.IndexFieldCount == 0 {
		t.Fatalf("expected Stats.IndexFieldCount > 0")
	}
	if st.AutoBatchCount == 0 {
		t.Fatalf("expected stats auto-batch count > 0")
	}
	if st.SnapshotSequence == 0 {
		t.Fatalf("expected stats snapshot sequence > 0")
	}

	idx := db.IndexStats()
	if idx.EntryCount == 0 {
		t.Fatalf("expected index diagnostics entry_count > 0")
	}
	if idx.ApproxHeapBytes < idx.Size {
		t.Fatalf("expected approx heap bytes >= index size, got approx=%d index=%d", idx.ApproxHeapBytes, idx.Size)
	}

	snap := db.SnapshotStats()
	if snap.Sequence == 0 {
		t.Fatalf("expected snapshot sequence > 0")
	}
	if snap.RegistrySize == 0 {
		t.Fatalf("expected snapshot registry to be non-empty")
	}
	if snap.UniverseCard == 0 {
		t.Fatalf("expected snapshot universe cardinality > 0")
	}

	pl := db.PlannerStats()
	if pl.Version == 0 {
		t.Fatalf("expected planner stats version > 0")
	}
	if pl.GeneratedAt.IsZero() {
		t.Fatalf("expected planner generated_at to be set")
	}
	if pl.FieldCount == 0 {
		t.Fatalf("expected planner field_count > 0")
	}
	if pl.AnalyzeInterval != 0 {
		t.Fatalf("expected disabled analyze interval (0), got %v", pl.AnalyzeInterval)
	}

	bs := db.AutoBatchStats()
	if bs.Window <= 0 {
		t.Fatalf("expected positive auto-batch window, got %v", bs.Window)
	}
	if bs.Enqueued == 0 {
		t.Fatalf("expected auto-batch stats to observe enqueued writes")
	}
}

func TestStats_PreservesIndexTimingFields(t *testing.T) {
	db, _ := openTempDBUint64(t)

	db.stats.BuildTime = 123 * time.Millisecond
	db.stats.BuildRPS = 456
	db.stats.LoadTime = 789 * time.Millisecond

	st, err := db.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}

	if st.BuildTime != db.stats.BuildTime {
		t.Fatalf("expected BuildTime=%v, got %v", db.stats.BuildTime, st.BuildTime)
	}
	if st.BuildRPS != db.stats.BuildRPS {
		t.Fatalf("expected BuildRPS=%d, got %d", db.stats.BuildRPS, st.BuildRPS)
	}
	if st.LoadTime != db.stats.LoadTime {
		t.Fatalf("expected LoadTime=%v, got %v", db.stats.LoadTime, st.LoadTime)
	}
}

func TestStats_IndexedSnapshotFactsNumeric(t *testing.T) {
	db, _ := openTempDBUint64(t)

	for _, id := range []uint64{7, 2, 4} {
		mustSetAPIRec(t, db, id, &Rec{Name: fmt.Sprintf("n%d", id), Age: int(id)})
	}
	if err := db.Delete(7); err != nil {
		t.Fatalf("Delete(7): %v", err)
	}

	st, err := db.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if st.KeyCount != 2 || st.LastKey != 4 {
		t.Fatalf("Stats=%+v want key_count=2 last_key=4", st)
	}
	if st.Mode != rbistats.ModeIndexed || st.StringKeys {
		t.Fatalf("Stats mode/key kind=%+v want indexed uint64", st)
	}
	if want := readBucketSequence(t, db.Bolt(), db.BucketName()); st.SnapshotSequence != want {
		t.Fatalf("SnapshotSequence=%d want %d", st.SnapshotSequence, want)
	}

	if err := db.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	st, err = db.Stats()
	if err != nil {
		t.Fatalf("Stats after truncate: %v", err)
	}
	if st.KeyCount != 0 || st.LastKey != 0 {
		t.Fatalf("Stats after truncate=%+v want empty key facts", st)
	}
	if want := readBucketSequence(t, db.Bolt(), db.BucketName()); st.SnapshotSequence != want {
		t.Fatalf("SnapshotSequence after truncate=%d want %d", st.SnapshotSequence, want)
	}
}

func TestStats_TransparentNumericLeavesKeyCountZero(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{Index: map[string]IndexKind{}, AutoBatchMax: 1})

	for _, id := range []uint64{3, 9, 1} {
		mustSetAPIRec(t, db, id, &Rec{Name: fmt.Sprintf("n%d", id), Age: int(id)})
	}

	st, err := db.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if st.KeyCount != 0 || st.LastKey != 9 || st.IndexFieldCount != 0 {
		t.Fatalf("Stats=%+v want key_count=0 last_key=9 index_field_count=0", st)
	}
	if st.Mode != rbistats.ModeTransparent || st.StringKeys {
		t.Fatalf("Stats mode/key kind=%+v want transparent uint64", st)
	}
	if want := readBucketSequence(t, db.Bolt(), db.BucketName()); st.SnapshotSequence != want {
		t.Fatalf("SnapshotSequence=%d want %d", st.SnapshotSequence, want)
	}
}

func TestStats_IndexedStringReportsCountWithoutLastKey(t *testing.T) {
	db, _ := openTempDBString(t)

	for _, id := range []string{"b", "aa", "c"} {
		if err := db.Set(id, &Rec{Name: id}); err != nil {
			t.Fatalf("Set(%q): %v", id, err)
		}
	}
	if err := db.Delete("c"); err != nil {
		t.Fatalf("Delete(c): %v", err)
	}

	st, err := db.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if st.KeyCount != 2 || st.LastKey != "b" {
		t.Fatalf("Stats=%+v want key_count=2 last_key=b", st)
	}
	if st.Mode != rbistats.ModeIndexed || !st.StringKeys {
		t.Fatalf("Stats mode/key kind=%+v want indexed string", st)
	}
	if want := readBucketSequence(t, db.Bolt(), db.BucketName()); st.SnapshotSequence != want {
		t.Fatalf("SnapshotSequence=%d want %d", st.SnapshotSequence, want)
	}
}

func TestStats_TransparentStringLeavesKeyCountZero(t *testing.T) {
	db, _ := openTempDBString(t, Options{Index: map[string]IndexKind{}, AutoBatchMax: 1})

	for _, id := range []string{"b", "aa", "c"} {
		if err := db.Set(id, &Rec{Name: id}); err != nil {
			t.Fatalf("Set(%q): %v", id, err)
		}
	}

	st, err := db.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if st.KeyCount != 0 || st.LastKey != "c" || st.IndexFieldCount != 0 {
		t.Fatalf("Stats=%+v want key_count=0 last_key=c index_field_count=0", st)
	}
	if st.Mode != rbistats.ModeTransparent || !st.StringKeys {
		t.Fatalf("Stats mode/key kind=%+v want transparent string", st)
	}
	if want := readBucketSequence(t, db.Bolt(), db.BucketName()); st.SnapshotSequence != want {
		t.Fatalf("SnapshotSequence=%d want %d", st.SnapshotSequence, want)
	}
}

func TestStats_ErrorReturnsZeroValue(t *testing.T) {
	db, _ := openTempDBUint64(t)
	mustSetAPIRec(t, db, 1, &Rec{Name: "alice", Age: 30})

	if err := db.Bolt().Update(func(tx *bbolt.Tx) error {
		return tx.DeleteBucket(db.BucketName())
	}); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}

	var zero rbistats.DB[uint64]
	st, err := db.Stats()
	if err == nil {
		t.Fatal("expected Stats error")
	}
	if st != zero {
		t.Fatalf("Stats on missing bucket=%+v want zero", st)
	}
}

func TestStats_BoltReadSetupFailureReturnsZeroValue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stats_bolt_closed.db")
	raw, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}
	db, err := New[uint64, Rec](raw, testOptions(Options{
		BucketName:        "stats_bolt_closed",
		DisableIndexStore: true,
	}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("raw Close: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	var zero rbistats.DB[uint64]
	st, err := db.Stats()
	if err == nil {
		t.Fatal("expected Stats error")
	}
	if st != zero {
		t.Fatalf("Stats on closed bbolt=%+v want zero", st)
	}
}

func TestComponentAccessors(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})

	if err := db.Set(1, &Rec{Name: "alice", Age: 10, Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "bob", Age: 20, Tags: []string{"db"}}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	st, err := db.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if st.KeyCount != 2 {
		t.Fatalf("expected Stats.KeyCount=2, got %d", st.KeyCount)
	}
	if st.SnapshotSequence == 0 {
		t.Fatalf("expected Stats.SnapshotSequence > 0")
	}

	idx := db.IndexStats()
	if idx.Size == 0 {
		t.Fatalf("expected IndexStats.Size > 0")
	}
	if len(idx.UniqueFieldKeys) == 0 {
		t.Fatalf("expected IndexStats.UniqueFieldKeys to be populated")
	}

	pl := db.PlannerStats()
	if pl.Version == 0 {
		t.Fatalf("expected PlannerStats.Version > 0")
	}
	if pl.GeneratedAt.IsZero() {
		t.Fatalf("expected PlannerStats.GeneratedAt to be set")
	}

	snap := db.SnapshotStats()
	if snap.Sequence == 0 {
		t.Fatalf("expected SnapshotStats.Sequence > 0")
	}

	bs := db.AutoBatchStats()
	if bs.Window <= 0 {
		t.Fatalf("expected AutoBatchStats.Window > 0")
	}
}

func TestIndexStats_ReportsFieldsAndTotals(t *testing.T) {
	db := openTempDBUint64IndexStats(t)

	const total = 96
	rankNil := 0
	var rankSeen [7]bool
	for i := 0; i < total; i++ {
		var rank *int
		if i%5 != 0 {
			rank = new(int)
			*rank = i % 7
			rankSeen[i%7] = true
		} else {
			rankNil++
		}
		if err := db.Set(uint64(i+1), &indexStatsTestRec{
			Name: fmt.Sprintf("user_%04d", i),
			Rank: rank,
		}); err != nil {
			t.Fatalf("Set(%d): %v", i+1, err)
		}
	}
	rankUnique := 0
	for i := range rankSeen {
		if rankSeen[i] {
			rankUnique++
		}
	}
	rankEntries := rankUnique + 1

	got := db.IndexStats()
	if got.UniqueFieldKeys["name"] != total {
		t.Fatalf("name unique keys=%d, want %d", got.UniqueFieldKeys["name"], total)
	}
	if got.UniqueFieldKeys["rank"] != uint64(rankUnique) {
		t.Fatalf("rank unique keys=%d, want %d", got.UniqueFieldKeys["rank"], rankUnique)
	}
	if got.FieldTotalCardinality["name"] != total {
		t.Fatalf("name cardinality=%d, want %d", got.FieldTotalCardinality["name"], total)
	}
	if got.FieldTotalCardinality["rank"] != total {
		t.Fatalf("rank cardinality=%d, want %d", got.FieldTotalCardinality["rank"], total)
	}
	if got.EntryCount != uint64(total+rankEntries) {
		t.Fatalf("entry count=%d, want %d", got.EntryCount, total+rankEntries)
	}
	if got.PostingCardinality != total*2 {
		t.Fatalf("posting cardinality=%d, want %d", got.PostingCardinality, total*2)
	}
	if rankNil == 0 {
		t.Fatalf("test setup did not generate nil rank rows")
	}

	var structSum uint64
	var sizeSum uint64
	var keySum uint64
	var cardSum uint64
	var heapSum uint64
	for _, name := range []string{"name", "rank"} {
		if got.FieldSize[name] == 0 {
			t.Fatalf("expected FieldSize[%q] to be populated", name)
		}
		if got.FieldKeyBytes[name] == 0 {
			t.Fatalf("expected FieldKeyBytes[%q] to be populated", name)
		}
		fieldStruct := got.FieldApproxStructBytes[name]
		fieldHeap := got.FieldApproxHeapBytes[name]
		if fieldHeap != got.FieldSize[name]+got.FieldKeyBytes[name]+fieldStruct {
			t.Fatalf("field heap mismatch for %q: got=%d want=%d", name, fieldHeap, got.FieldSize[name]+got.FieldKeyBytes[name]+fieldStruct)
		}
		sizeSum += got.FieldSize[name]
		keySum += got.FieldKeyBytes[name]
		cardSum += got.FieldTotalCardinality[name]
		structSum += fieldStruct
		heapSum += fieldHeap
	}
	if sizeSum != got.Size {
		t.Fatalf("FieldSize sum mismatch: got=%d want=%d", sizeSum, got.Size)
	}
	if keySum != got.KeyBytes {
		t.Fatalf("FieldKeyBytes sum mismatch: got=%d want=%d", keySum, got.KeyBytes)
	}
	if cardSum != got.PostingCardinality {
		t.Fatalf("FieldTotalCardinality sum mismatch: got=%d want=%d", cardSum, got.PostingCardinality)
	}
	if structSum != got.ApproxStructBytes {
		t.Fatalf("FieldApproxStructBytes sum mismatch: got=%d want=%d", structSum, got.ApproxStructBytes)
	}
	if heapSum != got.ApproxHeapBytes {
		t.Fatalf("FieldApproxHeapBytes sum mismatch: got=%d want=%d", heapSum, got.ApproxHeapBytes)
	}
}
