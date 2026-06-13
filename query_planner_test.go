package rbi

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/rbistats"
	"github.com/vapstack/rbi/rbitrace"
)

type traceContractRecorder struct {
	mu     sync.Mutex
	events []rbitrace.Event
}

func (r *traceContractRecorder) sink(ev rbitrace.Event) {
	r.mu.Lock()
	r.events = append(r.events, ev)
	r.mu.Unlock()
}

func (r *traceContractRecorder) mark() int {
	r.mu.Lock()
	n := len(r.events)
	r.mu.Unlock()
	return n
}

func (r *traceContractRecorder) lastSince(t testing.TB, mark int) rbitrace.Event {
	t.Helper()

	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.events) <= mark {
		t.Fatalf("expected trace event after mark=%d, total=%d", mark, len(r.events))
	}
	return r.events[len(r.events)-1]
}

func (r *traceContractRecorder) eventsSince(t testing.TB, mark int) []rbitrace.Event {
	t.Helper()

	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.events) <= mark {
		t.Fatalf("expected trace events after mark=%d, total=%d", mark, len(r.events))
	}
	out := append([]rbitrace.Event(nil), r.events[mark:]...)
	return out
}

func traceContractAssertBase(t testing.TB, ev rbitrace.Event, rowsReturned uint64) {
	t.Helper()

	if ev.Timestamp.IsZero() {
		t.Fatalf("expected trace timestamp, trace=%+v", ev)
	}
	if ev.Duration <= 0 {
		t.Fatalf("expected positive trace duration, trace=%+v", ev)
	}
	if ev.Plan == "" {
		t.Fatalf("expected non-empty trace plan, trace=%+v", ev)
	}
	if ev.Error != "" {
		t.Fatalf("unexpected trace error=%q, trace=%+v", ev.Error, ev)
	}
	if ev.RowsReturned != rowsReturned {
		t.Fatalf("rows returned mismatch: trace=%d want=%d", ev.RowsReturned, rowsReturned)
	}
	if ev.RowsMatched != 0 && ev.RowsMatched < ev.RowsReturned {
		t.Fatalf("expected RowsMatched >= RowsReturned when populated, trace=%+v", ev)
	}
	if ev.HasOrder {
		if ev.OrderField == "" {
			t.Fatalf("expected order field for ordered trace, trace=%+v", ev)
		}
		return
	}
	if ev.OrderField != "" || ev.OrderDesc {
		t.Fatalf("unexpected order metadata on unordered trace, trace=%+v", ev)
	}
}

func traceContractAssertQueryResultTrace(t testing.TB, ev rbitrace.Event, rowsReturned uint64) {
	t.Helper()

	traceContractAssertBase(t, ev, rowsReturned)
	if ev.RowsExamined < ev.RowsReturned {
		t.Fatalf("expected RowsExamined >= RowsReturned for query trace, trace=%+v", ev)
	}
}

func traceContractAssertORBranches(t testing.TB, branches []rbitrace.ORBranch, wantLen int) {
	t.Helper()

	if len(branches) != wantLen {
		t.Fatalf("unexpected OR branch trace size: got=%d want=%d trace=%+v", len(branches), wantLen, branches)
	}

	seen := make([]bool, wantLen)
	var branchExamined uint64
	var branchEmitted uint64

	for _, br := range branches {
		if br.Index < 0 || br.Index >= wantLen {
			t.Fatalf("unexpected OR branch index=%d for len=%d trace=%+v", br.Index, wantLen, branches)
		}
		if seen[br.Index] {
			t.Fatalf("duplicate OR branch index=%d trace=%+v", br.Index, branches)
		}
		seen[br.Index] = true
		if br.RowsEmitted > br.RowsExamined {
			t.Fatalf("expected branch RowsExamined >= RowsEmitted, branch=%+v", br)
		}
		if br.Skipped && br.SkipReason == "" {
			t.Fatalf("expected skip reason for skipped branch, branch=%+v", br)
		}
		branchExamined += br.RowsExamined
		branchEmitted += br.RowsEmitted
	}

	if branchExamined == 0 {
		t.Fatalf("expected OR branch examined rows, trace=%+v", branches)
	}
	if branchEmitted == 0 {
		t.Fatalf("expected OR branch emitted rows, trace=%+v", branches)
	}
}

type traceContractCase struct {
	name   string
	run    func(t *testing.T, db *DB[uint64, Rec]) uint64
	assert func(t *testing.T, ev rbitrace.Event, rowsReturned uint64)
}

func TestTraceContract_PublicAPIFamilies(t *testing.T) {
	recorder := &traceContractRecorder{}

	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSink:        recorder.sink,
		TraceSampleEvery: 1,
	})
	_ = seedData(t, db, 20_000)

	countORHybridExpr := qx.OR(
		qx.AND(
			qx.HASANY("tags", []string{"go", "db"}),
			qx.EQ("country", "NL"),
		),
		qx.AND(
			qx.HASANY("tags", []string{"rust", "ops"}),
			qx.EQ("active", true),
		),
		qx.AND(
			qx.PREFIX("full_name", "FN-1"),
			qx.EQ("active", true),
		),
		qx.AND(
			qx.EQ("name", "alice"),
			qx.EQ("active", true),
		),
	)

	cases := []traceContractCase{
		{
			name: "query_keys_order_basic_limit",
			run: func(t *testing.T, db *DB[uint64, Rec]) uint64 {
				t.Helper()

				q := qx.Query(
					qx.EQ("active", true),
					qx.GTE("age", 22),
					qx.LT("age", 45),
				).Sort("age", qx.ASC).Limit(120)

				got, err := db.QueryKeys(q)
				if err != nil {
					t.Fatalf("QueryKeys: %v", err)
				}
				if len(got) == 0 {
					t.Fatalf("expected non-empty ids")
				}
				return uint64(len(got))
			},
			assert: func(t *testing.T, ev rbitrace.Event, rowsReturned uint64) {
				t.Helper()

				traceContractAssertQueryResultTrace(t, ev, rowsReturned)
				if !ev.HasOrder || ev.OrderField != "age" || ev.OrderDesc {
					t.Fatalf("unexpected ordered trace metadata: %+v", ev)
				}
				if ev.Limit != 120 || ev.Offset != 0 {
					t.Fatalf("unexpected window metadata: trace=%+v", ev)
				}
				if ev.EstimatedRows == 0 || ev.EstimatedCost <= 0 || ev.FallbackCost <= 0 {
					t.Fatalf("expected costed order-basic trace, trace=%+v", ev)
				}
				if ev.OrderIndexScanWidth == 0 {
					t.Fatalf("expected order scan width, trace=%+v", ev)
				}
				if ev.EarlyStopReason == "" {
					t.Fatalf("expected early stop reason, trace=%+v", ev)
				}
				if len(ev.ORBranches) != 0 {
					t.Fatalf("unexpected OR branch trace for order-basic query, trace=%+v", ev.ORBranches)
				}
				if ev.ORRoute.Selected != "" {
					t.Fatalf("unexpected ordered-OR route trace for order-basic query, trace=%+v", ev.ORRoute)
				}
			},
		},
		{
			name: "query_values_order_basic_limit",
			run: func(t *testing.T, db *DB[uint64, Rec]) uint64 {
				t.Helper()

				q := qx.Query(
					qx.EQ("active", true),
					qx.GTE("age", 22),
					qx.LT("age", 45),
				).Sort("age", qx.ASC).Limit(80)

				items, err := db.Query(q)
				if err != nil {
					t.Fatalf("Query: %v", err)
				}
				if len(items) == 0 {
					t.Fatalf("expected non-empty records")
				}
				n := uint64(len(items))
				db.ReleaseRecords(items...)
				return n
			},
			assert: func(t *testing.T, ev rbitrace.Event, rowsReturned uint64) {
				t.Helper()

				traceContractAssertQueryResultTrace(t, ev, rowsReturned)
				if !ev.HasOrder || ev.OrderField != "age" || ev.OrderDesc {
					t.Fatalf("unexpected ordered trace metadata: %+v", ev)
				}
				if ev.Limit != 80 || ev.Offset != 0 {
					t.Fatalf("unexpected window metadata: trace=%+v", ev)
				}
				if ev.OrderIndexScanWidth == 0 {
					t.Fatalf("expected order scan width, trace=%+v", ev)
				}
				if ev.EarlyStopReason == "" {
					t.Fatalf("expected early stop reason, trace=%+v", ev)
				}
				if len(ev.ORBranches) != 0 || ev.ORRoute.Selected != "" {
					t.Fatalf("unexpected OR trace for Query values path, trace=%+v", ev)
				}
			},
		},
		{
			name: "query_keys_or_no_order_limit",
			run: func(t *testing.T, db *DB[uint64, Rec]) uint64 {
				t.Helper()

				q := qx.Query(
					qx.OR(
						qx.EQ("active", true),
						qx.EQ("name", "alice"),
					),
				).Limit(120)

				got, err := db.QueryKeys(q)
				if err != nil {
					t.Fatalf("QueryKeys: %v", err)
				}
				if len(got) == 0 {
					t.Fatalf("expected non-empty ids")
				}
				return uint64(len(got))
			},
			assert: func(t *testing.T, ev rbitrace.Event, rowsReturned uint64) {
				t.Helper()

				traceContractAssertQueryResultTrace(t, ev, rowsReturned)
				if ev.HasOrder || ev.OrderIndexScanWidth != 0 {
					t.Fatalf("unexpected order trace for unordered OR query, trace=%+v", ev)
				}
				if ev.EstimatedRows == 0 || ev.EstimatedCost <= 0 || ev.FallbackCost <= 0 {
					t.Fatalf("expected costed unordered OR trace, trace=%+v", ev)
				}
				if ev.EarlyStopReason == "" {
					t.Fatalf("expected early stop reason, trace=%+v", ev)
				}
				if ev.ORRoute.Selected == "" || ev.ORRoute.SelectedCost <= 0 || ev.ORRoute.RejectedCost <= 0 {
					t.Fatalf("expected unordered OR selector trace, trace=%+v", ev.ORRoute)
				}
				traceContractAssertORBranches(t, ev.ORBranches, 2)
			},
		},
		{
			name: "query_keys_or_order_limit",
			run: func(t *testing.T, db *DB[uint64, Rec]) uint64 {
				t.Helper()

				q := qx.Query(
					qx.OR(
						qx.EQ("active", true),
						qx.EQ("name", "alice"),
					),
				).Sort("age", qx.ASC).Limit(80)

				got, err := db.QueryKeys(q)
				if err != nil {
					t.Fatalf("QueryKeys: %v", err)
				}
				if len(got) == 0 {
					t.Fatalf("expected non-empty ids")
				}
				return uint64(len(got))
			},
			assert: func(t *testing.T, ev rbitrace.Event, rowsReturned uint64) {
				t.Helper()

				traceContractAssertQueryResultTrace(t, ev, rowsReturned)
				if !ev.HasOrder || ev.OrderField != "age" || ev.OrderDesc {
					t.Fatalf("unexpected ordered trace metadata: %+v", ev)
				}
				if ev.OrderIndexScanWidth == 0 {
					t.Fatalf("expected order scan width, trace=%+v", ev)
				}
				if ev.EstimatedRows == 0 || ev.EstimatedCost <= 0 || ev.FallbackCost <= 0 {
					t.Fatalf("expected costed ordered OR trace, trace=%+v", ev)
				}
				if ev.EarlyStopReason == "" {
					t.Fatalf("expected early stop reason, trace=%+v", ev)
				}
				if ev.ORRoute.Selected == "" || ev.ORRoute.SelectedCost <= 0 || ev.ORRoute.RejectedCost <= 0 {
					t.Fatalf("expected ordered OR selector trace, trace=%+v", ev.ORRoute)
				}
				if ev.ORRoute.RuntimeGuardEnabled && ev.ORRoute.RuntimeGuardReason == "" {
					t.Fatalf("expected runtime guard reason, trace=%+v", ev.ORRoute)
				}
				if ev.ORRoute.RuntimeFallbackTriggered && ev.ORRoute.RuntimeFallbackReason == "" {
					t.Fatalf("expected runtime fallback reason, trace=%+v", ev.ORRoute)
				}
				traceContractAssertORBranches(t, ev.ORBranches, 2)
			},
		},
		{
			name: "count_or_hybrid",
			run: func(t *testing.T, db *DB[uint64, Rec]) uint64 {
				t.Helper()

				q := qx.Query(countORHybridExpr)
				cnt, err := db.Count(q.Filter)
				if err != nil {
					t.Fatalf("Count: %v", err)
				}
				if cnt == 0 {
					t.Fatalf("expected non-zero count")
				}
				return cnt
			},
			assert: func(t *testing.T, ev rbitrace.Event, rowsReturned uint64) {
				t.Helper()

				traceContractAssertBase(t, ev, rowsReturned)
				if ev.HasOrder || ev.OrderIndexScanWidth != 0 {
					t.Fatalf("unexpected order trace for count query, trace=%+v", ev)
				}
				if ev.EarlyStopReason != "" {
					t.Fatalf("unexpected early stop reason for count query, trace=%+v", ev)
				}
				if ev.ORRoute.Selected != "" {
					t.Fatalf("unexpected ordered-OR route trace for count query, trace=%+v", ev.ORRoute)
				}
				traceContractAssertORBranches(t, ev.ORBranches, 4)

				spilled := false
				for _, br := range ev.ORBranches {
					if br.SkipReason == "materialized_spill" {
						spilled = true
						break
					}
				}
				if !spilled {
					t.Fatalf("expected hybrid count trace to record a spill branch, trace=%+v", ev.ORBranches)
				}
			},
		},
	}

	for i := range cases {
		tc := cases[i]
		t.Run(tc.name, func(t *testing.T) {
			mark := recorder.mark()
			rowsReturned := tc.run(t, db)
			ev := recorder.lastSince(t, mark)
			tc.assert(t, ev, rowsReturned)
		})
	}
}

func TestTraceContract_CountComplementLifecycle(t *testing.T) {
	recorder := &traceContractRecorder{}

	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSink:        recorder.sink,
		TraceSampleEvery: 1,
	})

	countries := []string{"US", "DE", "FR", "GB"}
	seedGeneratedUint64Data(t, db, 160_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%06d@example.com", i),
			Age:    i,
			Score:  float64(i % 1_000),
			Active: i%2 == 0,
			Meta: Meta{
				Country: countries[i%len(countries)],
			},
		}
	})

	q := qx.Query(
		qx.EQ("country", "US"),
		qx.NOTIN("active", []bool{false}),
		qx.GTE("age", 35_000),
	)

	mark := recorder.mark()

	first, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("first Count: %v", err)
	}
	second, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("second Count: %v", err)
	}
	third, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("third Count: %v", err)
	}
	if first == 0 || second != first || third != first {
		t.Fatalf("unexpected counts: first=%d second=%d third=%d", first, second, third)
	}

	events := recorder.eventsSince(t, mark)
	if len(events) != 3 {
		t.Fatalf("expected exactly three trace events, got=%d", len(events))
	}

	ev1 := events[0]
	ev2 := events[1]
	ev3 := events[2]

	traceContractAssertBase(t, ev1, first)
	traceContractAssertBase(t, ev2, second)
	traceContractAssertBase(t, ev3, third)

	if ev1.Plan != rbitrace.PlanCountPredicates || ev2.Plan != rbitrace.PlanCountPredicates || ev3.Plan != rbitrace.PlanCountPredicates {
		t.Fatalf("expected predicate-count trace plans, got=%q %q %q", ev1.Plan, ev2.Plan, ev3.Plan)
	}
	if ev1.CountPredicatePreparations == 0 {
		t.Fatalf("expected initial predicate preparation, trace=%+v", ev1)
	}
	if ev1.CountRangeComplementBuilds != 0 || ev1.CountRangeComplementCacheHits != 0 {
		t.Fatalf("expected initial run to keep complement local, trace=%+v", ev1)
	}
	if ev2.CountRangeComplementBuilds == 0 {
		t.Fatalf("expected promoted complement build on second run, trace=%+v", ev2)
	}
	if ev2.CountRangeComplementCacheHits != 0 {
		t.Fatalf("expected second run without complement cache hit, trace=%+v", ev2)
	}
	if ev2.CountRangeComplementRows == 0 {
		t.Fatalf("expected complement rows metric on second run, trace=%+v", ev2)
	}
	if ev3.CountRangeComplementCacheHits == 0 {
		t.Fatalf("expected complement cache hit on third run, trace=%+v", ev3)
	}
	if ev3.CountRangeComplementBuilds != 0 {
		t.Fatalf("expected third run to skip complement rebuild, trace=%+v", ev3)
	}
}

/**/

func TestPlannerStatsCollector_FullRefreshReflectsSnapshot(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 3_000)

	if err := db.RefreshPlannerStats(); err != nil {
		t.Fatalf("RefreshPlannerStats: %v", err)
	}

	got := db.PlannerStats()
	if got.UniverseCardinality != 3_000 {
		t.Fatalf("universe mismatch: got=%d want=3000", got.UniverseCardinality)
	}
	if got.FieldCount != len(got.Fields) {
		t.Fatalf("field count mismatch: FieldCount=%d len(Fields)=%d", got.FieldCount, len(got.Fields))
	}
	if got.FieldCount == 0 {
		t.Fatalf("expected planner fields")
	}
	if stats, ok := got.Fields["country"]; !ok || stats.DistinctKeys == 0 {
		t.Fatalf("country stats=%+v, want non-zero distinct keys", stats)
	}
	if stats, ok := got.Fields["age"]; !ok || stats.DistinctKeys == 0 {
		t.Fatalf("age stats=%+v, want non-zero distinct keys", stats)
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

	s0 := db.PlannerStats()

	time.Sleep(30 * time.Millisecond)

	s1 := db.PlannerStats()
	if s1.Version != s0.Version {
		t.Fatalf("snapshot version changed while scheduler disabled: before=%d after=%d", s0.Version, s1.Version)
	}
}

func TestPlannerAnalyzeScheduler_StartAndStop(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: 10 * time.Millisecond})

	s0 := db.PlannerStats()

	if latest, ok := waitPlannerStatsVersionGreater(db, s0.Version, 250*time.Millisecond); !ok {
		t.Fatalf("expected background refresh to advance snapshot version: start=%d latest=%d", s0.Version, latest)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
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
	s1.Fields["country"] = rbistats.PlannerField{}
	s2 := db.PlannerStats()
	if s2.Fields["country"].DistinctKeys == 0 {
		t.Fatalf("snapshot was mutated by caller")
	}
}

func TestPlannerStats_PersistedIndexLoadRestoresSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "planner_stats_persist.db")

	db, raw := openBoltAndNew[uint64, Rec](t, path, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 2_000)
	if err := db.RefreshPlannerStats(); err != nil {
		t.Fatalf("RefreshPlannerStats: %v", err)
	}
	want := db.PlannerStats()

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("raw.Close: %v", err)
	}

	db2, raw2 := openBoltAndNew[uint64, Rec](t, path, Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		_ = db2.Close()
		_ = raw2.Close()
	})

	got := db2.PlannerStats()
	if got.Version == 0 {
		t.Fatalf("expected planner stats version > 0 after persisted load")
	}
	if got.GeneratedAt.IsZero() {
		t.Fatalf("expected planner stats generated_at after persisted load")
	}
	if got.UniverseCardinality != want.UniverseCardinality {
		t.Fatalf("universe mismatch after persisted load: got=%d want=%d", got.UniverseCardinality, want.UniverseCardinality)
	}
	if got.FieldCount != want.FieldCount {
		t.Fatalf("field count mismatch after persisted load: got=%d want=%d", got.FieldCount, want.FieldCount)
	}
	for field, wantStats := range want.Fields {
		gotStats, ok := got.Fields[field]
		if !ok {
			t.Fatalf("missing persisted planner stats for %q", field)
		}
		if gotStats != wantStats {
			t.Fatalf("persisted planner stats mismatch for %q: got=%+v want=%+v", field, gotStats, wantStats)
		}
	}
}

func TestPlannerStats_ClosePersistsPublishedSnapshotWithoutRebuild(t *testing.T) {
	path := filepath.Join(t.TempDir(), "planner_stats_persist_reuse.db")

	db, raw := openBoltAndNew[uint64, Rec](t, path, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 256)

	if err := db.RefreshPlannerStats(); err != nil {
		t.Fatalf("RefreshPlannerStats: %v", err)
	}

	before := db.PlannerStats()

	if err := db.Set(10_001, &Rec{
		Meta:     Meta{Country: "ZZ"},
		Name:     "planner-persist-new",
		Email:    "planner-persist-new@example.com",
		Age:      99,
		Score:    999.99,
		Active:   true,
		Tags:     []string{"planner-persist-new"},
		FullName: "Planner Persist New",
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("raw.Close: %v", err)
	}

	db2, raw2 := openBoltAndNew[uint64, Rec](t, path, Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		_ = db2.Close()
		_ = raw2.Close()
	})

	got := db2.PlannerStats()

	if got.Version <= before.Version {
		t.Fatalf("expected persisted planner stats version to advance: got=%d before=%d", got.Version, before.Version)
	}
	if !got.GeneratedAt.After(before.GeneratedAt) {
		t.Fatalf("expected persisted generated_at to advance: got=%v before=%v", got.GeneratedAt, before.GeneratedAt)
	}
	if got.UniverseCardinality != before.UniverseCardinality+1 {
		t.Fatalf("unexpected persisted universe cardinality: got=%d want=%d", got.UniverseCardinality, before.UniverseCardinality+1)
	}
	if len(got.Fields) != len(before.Fields) {
		t.Fatalf("field count mismatch after persisted reuse: got=%d want=%d", len(got.Fields), len(before.Fields))
	}
	for field, wantStats := range before.Fields {
		if field == "$key" {
			continue
		}
		gotStats, ok := got.Fields[field]
		if !ok {
			t.Fatalf("missing persisted planner stats for %q", field)
		}
		if gotStats != wantStats {
			t.Fatalf("persisted planner stats changed for %q: got=%+v want=%+v", field, gotStats, wantStats)
		}
	}
	keyStats, ok := got.Fields["$key"]
	if !ok {
		t.Fatalf("missing persisted planner stats for $key")
	}
	if keyStats.DistinctKeys != got.UniverseCardinality || keyStats.TotalBucketCard != got.UniverseCardinality || keyStats.MaxBucketCard != 1 {
		t.Fatalf("persisted planner stats for $key=%+v universe=%d", keyStats, got.UniverseCardinality)
	}
}

var plannerExtSeeded struct {
	once sync.Once
	path string
	err  error
}

func plannerExtSeedPath(t *testing.T) string {
	t.Helper()

	plannerExtSeeded.once.Do(func() {
		dir, err := os.MkdirTemp("", "rbi-planner-ext-")
		if err != nil {
			plannerExtSeeded.err = err
			return
		}

		path := filepath.Join(dir, "seed.db")
		db, raw := openBoltAndNew[uint64, Rec](t, path, Options{AnalyzeInterval: -1})
		_ = seedData(t, db, 8_000)

		if err = db.Close(); err != nil {
			plannerExtSeeded.err = err
			_ = raw.Close()
			return
		}
		if err = raw.Close(); err != nil {
			plannerExtSeeded.err = err
			return
		}
		plannerExtSeeded.path = path
	})

	if plannerExtSeeded.err != nil {
		t.Fatalf("planner ext seeded fixture: %v", plannerExtSeeded.err)
	}
	return plannerExtSeeded.path
}

func plannerExtCopyDB(t *testing.T, src string) string {
	t.Helper()

	return copySeededDBWithSidecars(t, src, "planner_ext.db")
}

func plannerExtOpenSeededDB(t *testing.T, opts Options) *DB[uint64, Rec] {
	t.Helper()

	path := plannerExtCopyDB(t, plannerExtSeedPath(t))
	db, raw := openBoltAndNew[uint64, Rec](t, path, opts)
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	return db
}

func plannerExtQuery005() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.EQ("country", "PL"),
			qx.IN("country", []string{"Iceland", "PL"}),
			qx.LT("score", 65.0),
		).Sort("age", qx.ASC).Limit(10),
	)
}

func plannerExtQuery248() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.LT("score", 65.0),
			qx.LT("score", 55.0),
		).Sort("age", qx.DESC).Offset(10).Limit(40),
	)
}

func plannerExtQuery296() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.OR(
				qx.AND(
					qx.IN("country", []string{"Finland", "Thailand"}),
					qx.IN("country", []string{"DE", "Thailand"}),
				),
				qx.AND(
					qx.IN("country", []string{"DE", "Thailand"}),
					qx.LT("score", 65.0),
				),
			),
		).Sort("age", qx.ASC).Limit(25),
	)
}

func plannerExtQuery561() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.OR(
				qx.EQ("country", "NL"),
				qx.AND(
					qx.LT("score", 35.0),
					qx.LT("score", 65.0),
				),
			),
		).Sort("age", qx.DESC).Offset(20).Limit(5),
	)
}

func plannerExtQuery570() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.OR(
				qx.AND(
					qx.IN("country", []string{"Finland", "Thailand"}),
					qx.LT("score", 15.0),
				),
				qx.AND(
					qx.LT("score", 15.0),
					qx.EQ("active", true),
				),
			),
		).Limit(80),
	)
}

func plannerExtQuery584() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.LT("score", 65.0),
			qx.EQ("country", "PL"),
			qx.NOTIN("name", []string{"bob", "dave"}),
		).Sort("full_name", qx.ASC).Limit(40),
	)
}

func plannerExtQuery651() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.OR(
				qx.AND(
					qx.LT("score", 45.0),
					qx.LT("score", 55.0),
				),
				qx.AND(
					qx.LT("score", 65.0),
					qx.EQ("active", false),
				),
			),
		).Sort("age", qx.ASC).Offset(5).Limit(5),
	)
}

func plannerExtQuery652() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.GTE("score", 45.0),
			qx.NOTIN("name", []string{"alice", "eve"}),
			qx.EQ("country", "PL"),
			qx.LT("score", 55.0),
		).Sort("full_name", qx.ASC).Offset(50).Limit(15),
	)
}

func plannerExtQuery717() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.HASANY("tags", []string{"db", "rust"}),
			qx.LT("score", 65.0),
		).Sort("age", qx.DESC).Limit(10),
	)
}

func plannerExtQuery1145() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.HASANY("tags", []string{"db", "ops"}),
			qx.LT("score", 65.0),
		).Sort("age", qx.ASC).Limit(40),
	)
}

func plannerExtQuery1216() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.GTE("age", 45),
			qx.GTE("age", 35),
			qx.LT("score", 55.0),
			qx.GTE("score", 25.0),
		).Offset(5).Limit(5),
	)
}

func plannerExtQuery1269() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.HASANY("tags", []string{"go", "go"}),
			qx.LT("score", 65.0),
			qx.GTE("age", 22),
		).Sort("age", qx.DESC).Offset(3).Limit(40),
	)
}

func plannerExtQuery1283() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.OR(
				qx.AND(
					qx.LT("score", 65.0),
					qx.EQ("name", "alice"),
				),
				qx.AND(
					qx.LT("score", 65.0),
					qx.EQ("name", "alice"),
				),
			),
		).Sort("age", qx.DESC).Limit(80),
	)
}

func plannerExtQuery1363() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.LT("score", 55.0),
			qx.LT("score", 45.0),
		).Sort("full_name", qx.DESC).Limit(15),
	)
}

func plannerExtQuery1383() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.OR(
				qx.AND(
					qx.IN("country", []string{"DE", "Thailand"}),
					qx.IN("country", []string{"Finland", "DE"}),
				),
				qx.AND(
					qx.IN("country", []string{"Finland", "DE"}),
					qx.LT("score", 65.0),
				),
			),
		).Sort("age", qx.ASC).Limit(5),
	)
}

func plannerExtQuery1386() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.OR(
				qx.AND(
					qx.EQ("country", "NL"),
					qx.GTE("age", 22),
				),
				qx.AND(
					qx.GTE("age", 22),
					qx.LT("score", 55.0),
				),
			),
		).Sort("age", qx.ASC).Limit(10),
	)
}

func plannerExtQuery1473() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.OR(
				qx.AND(
					qx.LT("score", 55.0),
					qx.EQ("active", false),
				),
				qx.AND(
					qx.LT("score", 75.0),
					qx.GTE("score", 55.0),
				),
			),
		).Sort("age", qx.ASC).Limit(40),
	)
}

func plannerExtQueryAdversarialOrderedNegativeOnlyBranch() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.OR(
				qx.NOTIN("country", []string{"NL", "DE"}),
				qx.AND(
					qx.EQ("name", "alice"),
					qx.LT("score", 55.0),
				),
			),
		).Sort("age", qx.ASC).Offset(40).Limit(35),
	)
}

func plannerExtQueryAdversarialOrderedNegativeResidualOverlap() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.OR(
				qx.AND(
					qx.HASANY("tags", []string{"go", "db", "go"}),
					qx.LT("score", 65.0),
					qx.NOTIN("country", []string{"NL", "DE"}),
				),
				qx.AND(
					qx.EQ("country", "PL"),
					qx.GTE("age", 22),
					qx.NOTIN("name", []string{"alice", "bob"}),
				),
				qx.AND(
					qx.EQ("country", "PL"),
					qx.GTE("age", 22),
					qx.LT("score", 65.0),
				),
			),
		).Sort("full_name", qx.DESC).Offset(10).Limit(50),
	)
}

func plannerExtQueryAdversarialNoOrderNegativeResidualOverlap() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.OR(
				qx.AND(
					qx.LT("score", 55.0),
					qx.NOTIN("country", []string{"NL", "DE"}),
					qx.NOTIN("name", []string{"alice"}),
				),
				qx.AND(
					qx.HASANY("tags", []string{"go", "ops", "go"}),
					qx.GTE("age", 22),
					qx.LT("score", 65.0),
				),
				qx.AND(
					qx.EQ("active", true),
					qx.GTE("age", 22),
					qx.NOTIN("name", []string{"eve"}),
				),
			),
		).Limit(120),
	)
}

func plannerExtValidateNoOrderWindow(q *qx.QX, got, full []uint64) error {
	return queryContractValidateNoOrderWindow(q, got, full)
}

func plannerExtExprOp(expr qx.Expr) qx.Op {
	if expr.Kind == qx.KindNONE {
		return qx.OpNOOP
	}
	if expr.Is(qx.KindOP, qx.OpNOT) && len(expr.Args) == 1 {
		return plannerExtExprOp(expr.Args[0])
	}
	if expr.Kind == qx.KindOP {
		return qx.Op(expr.Name)
	}
	return qx.OpNONE
}

func plannerExtRequireTrace(events []rbitrace.Event) error {
	if len(events) == 0 {
		return fmt.Errorf("expected trace event")
	}
	if events[len(events)-1].Plan == "" {
		return fmt.Errorf("expected non-empty trace plan")
	}
	return nil
}

func plannerExtAssertQueryContract(t *testing.T, q *qx.QX) {
	t.Helper()

	var (
		mu     sync.Mutex
		events []rbitrace.Event
	)
	db := plannerExtOpenSeededDB(t, Options{
		AnalyzeInterval:  -1,
		TraceSink:        func(ev rbitrace.Event) { mu.Lock(); events = append(events, ev); mu.Unlock() },
		TraceSampleEvery: 1,
	})

	newUint64QueryContract(t, db).AssertQueryKeysMatchReference(q)

	mu.Lock()
	err := plannerExtRequireTrace(events)
	mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

func plannerExtRunConcurrentQueries(
	t *testing.T,
	db *DB[uint64, Rec],
	q *qx.QX,
	workers int,
	rounds int,
	validate func([]uint64) error,
) {
	t.Helper()

	errCh := make(chan error, workers)
	var wg sync.WaitGroup

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for round := 0; round < rounds; round++ {
				got, err := db.QueryKeys(q)
				if err != nil {
					errCh <- fmt.Errorf("worker=%d round=%d QueryKeys: %w", worker, round, err)
					return
				}
				if err = validate(got); err != nil {
					errCh <- fmt.Errorf("worker=%d round=%d: %w", worker, round, err)
					return
				}
			}
		}(worker)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPlannerExt_Query005_OrderedEqAndInSameFieldAgeAsc(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery005())
}

func TestPlannerExt_Query248_OrderedRedundantUpperBoundsAgeDescOffset(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery248())
}

func TestPlannerExt_Query296_OrderedOROverlappingCountryINAgeAsc(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery296())
}

func TestPlannerExt_Query561_OrderedORBroadEqVsRangeAgeDescOffset(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery561())
}

func TestPlannerExt_Query570_NoOrderORDuplicatesOnOverlap(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery570())
}

func TestPlannerExt_Query584_OrderedNotInPlusCountryAndScoreFullNameAsc(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery584())
}

func TestPlannerExt_Query651_OrderedORRangeBandVsActiveFalseAgeAscOffset(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery651())
}

func TestPlannerExt_Query652_OrderedScoreBandWithNotInFullNameAscOffset(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery652())
}

func TestPlannerExt_Query717_OrderedHasAnyAndScoreAgeDesc(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery717())
}

func TestPlannerExt_Query1145_OrderedHasAnyAndScoreAgeAsc(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery1145())
}

func TestPlannerExt_Query1216_NoOrderResultEscapesFullSet(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery1216())
}

func TestPlannerExt_Query1269_OrderedHasAnyGoAndScoreAndAgeAgeDescOffset(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery1269())
}

func TestPlannerExt_Query1283_OrderedDuplicateORBranchesAgeDesc(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery1283())
}

func TestPlannerExt_Query1363_OrderedRedundantUpperBoundsFullNameDesc(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery1363())
}

func TestPlannerExt_Query1383_OrderedOROverlappingCountryDEFinlandAgeAsc(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery1383())
}

func TestPlannerExt_Query1386_OrderedORCountryNLOrScoreBandAgeAsc(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery1386())
}

func TestPlannerExt_Query1473_OrderedORScoreBandOrScoreWindowAgeAsc(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery1473())
}

func TestPlannerExt_AdversarialPlannerCorpus(t *testing.T) {
	tests := []struct {
		name string
		q    *qx.QX
	}{
		{
			name: "ordered_negative_only_branch",
			q:    plannerExtQueryAdversarialOrderedNegativeOnlyBranch(),
		},
		{
			name: "ordered_negative_residual_overlap",
			q:    plannerExtQueryAdversarialOrderedNegativeResidualOverlap(),
		},
		{
			name: "no_order_negative_residual_overlap",
			q:    plannerExtQueryAdversarialNoOrderNegativeResidualOverlap(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			plannerExtAssertQueryContract(t, tc.q)
		})
	}
}

func TestPlannerExt_Race_OrderedEqAndInSameFieldAgeAsc(t *testing.T) {
	db := plannerExtOpenSeededDB(t, Options{AnalyzeInterval: -1})
	q := plannerExtQuery005()
	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	plannerExtRunConcurrentQueries(t, db, q, 8, 20, func(got []uint64) error {
		if !queryIDsEqual(q, got, want) {
			return fmt.Errorf("got=%v want=%v", got, want)
		}
		return nil
	})
}

func TestPlannerExt_Race_OrderedOROverlappingCountryINAgeAsc(t *testing.T) {
	db := plannerExtOpenSeededDB(t, Options{AnalyzeInterval: -1})
	q := plannerExtQuery296()
	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	plannerExtRunConcurrentQueries(t, db, q, 8, 20, func(got []uint64) error {
		if !queryIDsEqual(q, got, want) {
			return fmt.Errorf("got=%v want=%v", got, want)
		}
		return nil
	})
}

func TestPlannerExt_Race_NoOrderORDuplicatesOnOverlap(t *testing.T) {
	db := plannerExtOpenSeededDB(t, Options{AnalyzeInterval: -1})
	q := plannerExtQuery570()
	fullQ := cloneQuery(q)
	fullQ.Window.Offset = 0
	fullQ.Window.Limit = 0
	full, err := expectedKeysUint64(t, db, fullQ)
	if err != nil {
		t.Fatalf("expectedKeysUint64(full): %v", err)
	}
	plannerExtRunConcurrentQueries(t, db, q, 8, 20, func(got []uint64) error {
		return plannerExtValidateNoOrderWindow(q, got, full)
	})
}

func TestPlannerExt_Race_TraceUnderConcurrentQueries(t *testing.T) {
	var (
		mu     sync.Mutex
		events []rbitrace.Event
	)
	db := plannerExtOpenSeededDB(t, Options{
		AnalyzeInterval:  -1,
		TraceSink:        func(ev rbitrace.Event) { mu.Lock(); events = append(events, ev); mu.Unlock() },
		TraceSampleEvery: 1,
	})

	q := plannerExtQuery717()
	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}

	errCh := make(chan error, 16)
	var wg sync.WaitGroup

	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := 0; round < 20; round++ {
				got, err := db.QueryKeys(q)
				if err != nil {
					errCh <- err
					return
				}
				if !queryIDsEqual(q, got, want) {
					errCh <- fmt.Errorf("got=%v want=%v", got, want)
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace events")
	}
	for i, ev := range events {
		if ev.Plan == "" {
			t.Fatalf("event %d has empty plan", i)
		}
		if ev.RowsReturned != uint64(len(want)) {
			t.Fatalf("event %d rows returned mismatch: got=%d want=%d", i, ev.RowsReturned, len(want))
		}
	}
}

func TestPlannerExt_Race_RefreshPlannerStatsDuringQueries(t *testing.T) {
	db := plannerExtOpenSeededDB(t, Options{AnalyzeInterval: -1})
	q := plannerExtQuery1269()
	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}

	errCh := make(chan error, 16)
	var wg sync.WaitGroup

	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := 0; round < 20; round++ {
				got, err := db.QueryKeys(q)
				if err != nil {
					errCh <- err
					return
				}
				if !queryIDsEqual(q, got, want) {
					errCh <- fmt.Errorf("got=%v want=%v", got, want)
					return
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if err := db.RefreshPlannerStats(); err != nil {
				errCh <- err
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			s := db.PlannerStats()
			if s.Fields != nil {
				s.Fields["country"] = rbistats.PlannerField{}
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPlannerExt_Race_PeriodicAnalyzerDuringAdversarialQueries(t *testing.T) {
	const analyzeInterval = 5 * time.Millisecond

	db := plannerExtOpenSeededDB(t, Options{
		AnalyzeInterval:  analyzeInterval,
		TraceSink:        func(rbitrace.Event) {},
		TraceSampleEvery: 1,
	})

	queries := []*qx.QX{
		plannerExtQueryAdversarialOrderedNegativeOnlyBranch(),
		plannerExtQueryAdversarialOrderedNegativeResidualOverlap(),
		plannerExtQueryAdversarialNoOrderNegativeResidualOverlap(),
	}

	type expectation struct {
		q     *qx.QX
		exact []uint64
		full  []uint64
	}

	exps := make([]expectation, 0, len(queries))
	for _, q := range queries {
		if len(q.Order) > 0 || (q.Window.Offset == 0 && q.Window.Limit == 0) {
			want, err := expectedKeysUint64(t, db, q)
			if err != nil {
				t.Fatalf("expectedKeysUint64(%+v): %v", q, err)
			}
			exps = append(exps, expectation{q: q, exact: want})
			continue
		}

		fullQ := cloneQuery(q)
		fullQ.Window.Offset = 0
		fullQ.Window.Limit = 0
		full, err := expectedKeysUint64(t, db, fullQ)
		if err != nil {
			t.Fatalf("expectedKeysUint64(full %+v): %v", fullQ, err)
		}
		exps = append(exps, expectation{q: q, full: full})
	}

	startVersion := db.PlannerStats().Version
	if latest, ok := waitPlannerStatsVersionGreater(db, startVersion, 250*time.Millisecond); !ok {
		t.Fatalf("expected periodic analyzer to advance planner stats: start=%d latest=%d", startVersion, latest)
	}

	errCh := make(chan error, 32)
	var wg sync.WaitGroup

	for worker := 0; worker < 6; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for round := 0; round < 20; round++ {
				for i := range exps {
					exp := exps[(worker+round+i)%len(exps)]
					got, err := db.QueryKeys(exp.q)
					if err != nil {
						errCh <- fmt.Errorf("worker=%d round=%d QueryKeys(%+v): %w", worker, round, exp.q, err)
						return
					}
					if exp.exact != nil {
						if !queryIDsEqual(exp.q, got, exp.exact) {
							errCh <- fmt.Errorf("worker=%d round=%d exact mismatch q=%+v got=%v want=%v", worker, round, exp.q, got, exp.exact)
							return
						}
						continue
					}
					if err = plannerExtValidateNoOrderWindow(exp.q, got, exp.full); err != nil {
						errCh <- fmt.Errorf("worker=%d round=%d no-order mismatch q=%+v err=%v got=%v full=%v", worker, round, exp.q, err, got, exp.full)
						return
					}
				}
			}
		}(worker)
	}

	for reader := 0; reader < 3; reader++ {
		wg.Add(1)
		go func(reader int) {
			defer wg.Done()
			lastVersion := uint64(0)
			for round := 0; round < 200; round++ {
				s := db.PlannerStats()
				if s.AnalyzeInterval != analyzeInterval {
					errCh <- fmt.Errorf("reader=%d round=%d analyze interval mismatch: got=%v want=%v", reader, round, s.AnalyzeInterval, analyzeInterval)
					return
				}
				if s.Version < lastVersion {
					errCh <- fmt.Errorf("reader=%d round=%d planner stats version regressed: got=%d prev=%d", reader, round, s.Version, lastVersion)
					return
				}
				if s.FieldCount != len(s.Fields) {
					errCh <- fmt.Errorf("reader=%d round=%d field count mismatch: count=%d len=%d", reader, round, s.FieldCount, len(s.Fields))
					return
				}
				if s.Version > 0 && s.GeneratedAt.IsZero() {
					errCh <- fmt.Errorf("reader=%d round=%d missing generated_at for version=%d", reader, round, s.Version)
					return
				}

				s.Fields["country"] = rbistats.PlannerField{}
				delete(s.Fields, "age")
				lastVersion = s.Version
			}
		}(reader)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for round := 0; round < 25; round++ {
			if err := db.RefreshPlannerStats(); err != nil {
				errCh <- fmt.Errorf("RefreshPlannerStats round=%d: %w", round, err)
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}
