package rbi

import (
	"strings"
	"sync"
	"testing"

	"github.com/vapstack/qx"
)

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

	db, _ := openTempDBUint64(t, Options{
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

	db, _ := openTempDBUint64(t, Options{
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

	db, _ := openTempDBUint64(t, Options{
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

func TestTracer_QueryValuesPathEmitsTrace(t *testing.T) {
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
	).By("age", qx.ASC).Max(80)

	items, err := db.Query(q)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(items) == 0 {
		t.Fatalf("expected non-empty items")
	}
	db.ReleaseRecords(items...)

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	if ev.Plan != string(PlanLimitOrderBasic) {
		t.Fatalf("expected %q plan, got %q", PlanLimitOrderBasic, ev.Plan)
	}
	if ev.RowsReturned != uint64(len(items)) {
		t.Fatalf("rows returned mismatch: ev=%d items=%d", ev.RowsReturned, len(items))
	}
	if ev.RowsExamined == 0 {
		t.Fatalf("expected rows examined to be recorded")
	}
}

func TestTracer_CountPathEmitsTrace(t *testing.T) {
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
		qx.IN("country", []string{"NL", "DE"}),
		qx.GTE("age", 20),
	)

	cnt, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if cnt == 0 {
		t.Fatalf("expected non-zero count")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	if !strings.HasPrefix(ev.Plan, "plan_count_") {
		t.Fatalf("expected count plan, got %q", ev.Plan)
	}
	if ev.RowsReturned != cnt {
		t.Fatalf("rows returned mismatch: ev=%d count=%d", ev.RowsReturned, cnt)
	}
	if ev.Duration <= 0 {
		t.Fatalf("expected positive duration")
	}
}
