package main

import (
	"testing"
	"time"

	"github.com/vapstack/rbi"
)

func TestPlannerTraceCollectorAggregatesByClassAndQuery(t *testing.T) {
	catalog, _, byName, err := loadClassCatalog()
	if err != nil {
		t.Fatalf("loadClassCatalog: %v", err)
	}

	collector := newPlannerTraceCollector(catalog, 64, 2)
	if collector == nil {
		t.Fatalf("collector is nil")
	}
	epoch := collector.newEpoch()
	class := byName[ClassReadHeavy]
	if class == nil {
		t.Fatalf("missing class %q", ClassReadHeavy)
	}

	worker := collector.newWorker(class)
	worker.bindCurrentGoroutine()
	defer worker.close()

	done := worker.begin("read_discovery_explore_keys")
	collector.observe(rbi.TraceEvent{
		Timestamp:           time.Unix(1700000000, 0),
		Duration:            1250 * time.Millisecond,
		Plan:                "plan_or_order_merge_fallback",
		HasOrder:            true,
		OrderField:          "created_at",
		OrderDesc:           true,
		Limit:               150,
		RowsExamined:        125_000,
		RowsReturned:        150,
		EstimatedRows:       12_000,
		EstimatedCost:       93.5,
		FallbackCost:        118.0,
		OrderIndexScanWidth: 640,
		DedupeCount:         512,
		EarlyStopReason:     "limit_reached",
		ORRoute: rbi.TraceORRoute{
			Route:                    "fallback",
			Reason:                   "cost",
			RuntimeFallbackTriggered: true,
			RuntimeFallbackReason:    "projected_examined",
		},
		ORBranches: []rbi.TraceORBranch{
			{Index: 0, RowsExamined: 40000, RowsEmitted: 70},
			{Index: 1, RowsExamined: 85000, RowsEmitted: 80},
		},
	})
	done()

	done = worker.begin("read_dormant_archive_page_keys")
	collector.observe(rbi.TraceEvent{
		Timestamp:           time.Unix(1700000001, 0),
		Duration:            2200 * time.Millisecond,
		Plan:                "plan_ordered_scan",
		HasOrder:            true,
		OrderField:          "last_login",
		RowsExamined:        240_000,
		RowsReturned:        100,
		EstimatedRows:       8_500,
		EstimatedCost:       141.0,
		OrderIndexScanWidth: 2100,
		DedupeCount:         0,
		EarlyStopReason:     "limit_reached",
	})
	done()

	snapshot := epoch.snapshot()
	root := snapshot.rootReport()
	if root == nil {
		t.Fatalf("root report is nil")
	}
	if root.Total.Sampled != 2 {
		t.Fatalf("sampled = %d, want 2", root.Total.Sampled)
	}
	if got := root.Total.PlanCounts["plan_or_order_merge_fallback"]; got != 1 {
		t.Fatalf("fallback plan count = %d, want 1", got)
	}
	if root.Total.RuntimeFallbacks != 1 {
		t.Fatalf("runtime fallbacks = %d, want 1", root.Total.RuntimeFallbacks)
	}
	if len(root.TopSamples) != 2 {
		t.Fatalf("top samples = %d, want 2", len(root.TopSamples))
	}
	if root.TopSamples[0].Query != "read_dormant_archive_page_keys" {
		t.Fatalf("slowest sample query = %q, want dormant archive", root.TopSamples[0].Query)
	}
	classReport := snapshot.classReport(ClassReadHeavy)
	if classReport == nil || classReport.Sampled != 2 {
		t.Fatalf("class sampled = %#v, want 2", classReport)
	}
	queryReport := snapshot.queryReport(ClassReadHeavy, "read_discovery_explore_keys")
	if queryReport == nil || queryReport.Sampled != 1 {
		t.Fatalf("query sampled = %#v, want 1", queryReport)
	}
	if got := queryReport.ORRouteCounts["fallback:cost"]; got != 1 {
		t.Fatalf("OR route count = %d, want 1", got)
	}
}
