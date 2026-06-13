package qagg

import (
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/rbitrace"
)

func TestAggregateSelectorsChooseRouteContracts(t *testing.T) {
	db := newQaggTestDB(t, nil)

	tests := []struct {
		name         string
		q            *qx.QX
		wantRoute    aggregateRouteCandidate
		wantInput    aggregateFilterInput
		wantRejected aggregateRouteCandidate
	}{
		{
			name:      "row_count",
			q:         qx.Aggregate(qx.ROWCOUNT().AS("rows")),
			wantRoute: aggregateRouteRowCount,
			wantInput: aggregateFilterInputCardinality,
		},
		{
			name:      "distinct_ungrouped",
			q:         qx.Aggregate(qx.DISTINCT("country").AS("country")),
			wantRoute: aggregateRouteDistinctUngrouped,
			wantInput: aggregateFilterInputMaterializedIDs,
		},
		{
			name:      "count_distinct_ungrouped",
			q:         qx.Aggregate(qx.COUNT(qx.DISTINCT("country")).AS("countries")),
			wantRoute: aggregateRouteCountDistinctUngrouped,
			wantInput: aggregateFilterInputMaterializedIDs,
		},
		{
			name:      "ungrouped_ordinary",
			q:         qx.Aggregate(qx.SUM("age").AS("sum")),
			wantRoute: aggregateRouteUngroupedOrdinary,
			wantInput: aggregateFilterInputMaterializedIDs,
		},
		{
			name:      "ungrouped_measure",
			q:         qx.Aggregate(qx.SUM("amount").AS("sum")),
			wantRoute: aggregateRouteUngroupedMeasure,
			wantInput: aggregateFilterInputMaterializedIDs,
		},
		{
			name:      "ungrouped_hybrid",
			q:         qx.Aggregate(qx.SUM("age").AS("age_sum"), qx.SUM("amount").AS("amount_sum")),
			wantRoute: aggregateRouteUngroupedHybrid,
			wantInput: aggregateFilterInputMaterializedIDs,
		},
		{
			name:      "grouped_row_count_recursive",
			q:         qx.Group("country").Metrics(qx.ROWCOUNT().AS("rows")),
			wantRoute: aggregateRouteGroupedRecursive,
			wantInput: aggregateFilterInputMaterializedIDs,
		},
		{
			name:         "grouped_ordinary_by_id",
			q:            qx.Group("country").Metrics(qx.SUM("age").AS("sum")),
			wantRoute:    aggregateRouteGroupedOrdinaryByID,
			wantInput:    aggregateFilterInputMaterializedIDs,
			wantRejected: aggregateRouteGroupedRecursive,
		},
		{
			name:         "grouped_measure",
			q:            qx.Group("country").Metrics(qx.SUM("amount").AS("sum")),
			wantRoute:    aggregateRouteGroupedMeasure,
			wantInput:    aggregateFilterInputMaterializedIDs,
			wantRejected: aggregateRouteGroupedRecursive,
		},
		{
			name:         "grouped_hybrid",
			q:            qx.Group("country").Metrics(qx.SUM("age").AS("age_sum"), qx.SUM("amount").AS("amount_sum")),
			wantRoute:    aggregateRouteGroupedHybrid,
			wantInput:    aggregateFilterInputMaterializedIDs,
			wantRejected: aggregateRouteGroupedRecursive,
		},
	}

	view := db.view()
	defer db.exec.ReleaseView(view)
	exec := aggregateExecutor{snap: db.snap}

	for i := range tests {
		tc := tests[i]
		prepared, err := Prepare(tc.q, db.rt, db.rt.IndexedByName)
		if err != nil {
			t.Fatalf("%s Prepare: %v", tc.name, err)
		}

		var decision aggregateRouteDecision
		family := selectAggregateFamily(prepared)
		if family == aggregateSelectorCount {
			decision = selectCountAggregate(aggregateCountFacts{filterCardinality: db.snap.Universe.Cardinality()})
		} else {
			ids, err := view.Filter(prepared.filter)
			if err != nil {
				prepared.Release()
				t.Fatalf("%s Filter: %v", tc.name, err)
			}
			switch family {
			case aggregateSelectorDistinct:
				decision = selectDistinctAggregate(exec.collectDistinctFacts(prepared, ids))
			case aggregateSelectorUngrouped:
				decision = selectUngroupedAggregate(exec.collectUngroupedFacts(prepared, ids))
			default:
				decision = selectGroupedAggregate(exec.collectGroupedFacts(prepared, ids))
			}
			ids.Release()
		}

		prepared.Release()
		if decision.route != tc.wantRoute || decision.filterInput != tc.wantInput || decision.rejected != tc.wantRejected {
			t.Fatalf("%s decision=%+v, want route=%d input=%d rejected=%d", tc.name, decision, tc.wantRoute, tc.wantInput, tc.wantRejected)
		}
	}
}

func TestSelectGroupedAggregatePreservesByIDFeasibilityGate(t *testing.T) {
	rejected := selectGroupedAggregate(aggregateGroupedFacts{
		hasOrdinary:        true,
		filterCardinality:  aggregateGroupOrdinalMapMaxLen + 1,
		metricRows:         1_000_000,
		groupCountUpperMax: 1_000_000,
		ordinaryKeyCount:   1_000_000,
		byIDCandidate:      true,
		byIDMapLen:         16,
	})
	if rejected.route != aggregateRouteGroupedRecursive || rejected.rejected != aggregateRouteGroupedOrdinaryByID {
		t.Fatalf("rejected by-ID decision=%+v", rejected)
	}
	if rejected.rejectedCost <= 0 {
		t.Fatalf("rejected by-ID cost missing: %+v", rejected)
	}

	selected := selectGroupedAggregate(aggregateGroupedFacts{
		hasOrdinary:        true,
		filterCardinality:  10,
		metricRows:         10,
		groupCountUpperMax: 20,
		ordinaryKeyCount:   1,
		byIDCandidate:      true,
		byIDFeasible:       true,
		byIDMapLen:         1000,
	})
	if selected.route != aggregateRouteGroupedOrdinaryByID || selected.rejected != aggregateRouteGroupedRecursive {
		t.Fatalf("selected by-ID decision=%+v", selected)
	}
	if selected.groupMapLen != 1000 || selected.selectedCost <= 0 || selected.rejectedCost <= 0 {
		t.Fatalf("selected by-ID costs=%+v", selected)
	}

	mapped := selectGroupedAggregate(aggregateGroupedFacts{
		hasOrdinary:        true,
		filterCardinality:  10,
		metricRows:         1_000_000,
		groupCountUpperMax: 1_000_000,
		ordinaryKeyCount:   1_000_000,
		byIDCandidate:      true,
	})
	if mapped.route != aggregateRouteGroupedOrdinaryByID || mapped.groupLookup != aggregateGroupLookupMap {
		t.Fatalf("selected map by-ID decision=%+v", mapped)
	}
	if mapped.groupMapLen != 10 || mapped.selectedCost <= 0 || mapped.rejectedCost <= 0 {
		t.Fatalf("selected map by-ID costs=%+v", mapped)
	}
}

func TestSelectGroupedMeasurePrefersOrdinalLookupForBroadLowCardinality(t *testing.T) {
	measure := selectGroupedAggregate(aggregateGroupedFacts{
		hasMeasure:         true,
		filterCardinality:  1_000_000,
		maxID:              999_999,
		hasMaxID:           true,
		groupCountUpperMax: 4,
		metricFieldCount:   1,
		measureMetricCount: 1,
		measureRows:        1_000_000,
		measureLookupSteps: 16,
		measureMode:        aggregateMeasureAccessMergeScan,
		measureCost:        2_000_000,
	})
	if measure.route != aggregateRouteGroupedMeasure || measure.groupLookup != aggregateGroupLookupOrdinalSlice {
		t.Fatalf("broad grouped measure decision=%+v", measure)
	}
	if measure.rejected != aggregateRouteGroupedRecursive || measure.selectedCost <= 0 || measure.rejectedCost <= 0 || measure.selectedCost >= measure.rejectedCost {
		t.Fatalf("broad grouped measure costs=%+v", measure)
	}

	hybrid := selectGroupedAggregate(aggregateGroupedFacts{
		hasOrdinary:         true,
		hasMeasure:          true,
		filterCardinality:   1_000_000,
		maxID:               999_999,
		hasMaxID:            true,
		metricRows:          1_000_000,
		groupCountUpperMax:  4,
		metricFieldCount:    2,
		ordinaryMetricCount: 1,
		measureMetricCount:  1,
		measureRows:         1_000_000,
		measureLookupSteps:  16,
		measureMode:         aggregateMeasureAccessMergeScan,
		measureCost:         2_000_000,
		ordinaryKeyCount:    1_000,
	})
	if hybrid.route != aggregateRouteGroupedHybrid || hybrid.groupLookup != aggregateGroupLookupOrdinalSlice {
		t.Fatalf("broad grouped hybrid decision=%+v", hybrid)
	}
	if hybrid.rejected != aggregateRouteGroupedRecursive || hybrid.selectedCost <= 0 || hybrid.rejectedCost <= 0 || hybrid.selectedCost >= hybrid.rejectedCost {
		t.Fatalf("broad grouped hybrid costs=%+v", hybrid)
	}
}

func TestSelectGroupedMeasureFallsBackWithoutDenseOrdinalLookup(t *testing.T) {
	selected := selectGroupedAggregate(aggregateGroupedFacts{
		hasMeasure:         true,
		filterCardinality:  10,
		maxID:              10_000_000,
		hasMaxID:           true,
		groupCountUpperMax: 2,
		metricFieldCount:   1,
		measureMetricCount: 1,
		measureRows:        100_000,
		measureLookupSteps: 16,
		measureMode:        aggregateMeasureAccessLookup,
		measureCost:        160,
	})
	if selected.route != aggregateRouteGroupedRecursive || selected.rejected != aggregateRouteGroupedMeasure {
		t.Fatalf("sparse grouped measure decision=%+v", selected)
	}
	if selected.groupLookup != aggregateGroupLookupNone || selected.groupMapLen != 0 {
		t.Fatalf("sparse grouped measure lookup metrics=%+v", selected)
	}
	if selected.selectedCost <= 0 || selected.rejectedCost <= 0 {
		t.Fatalf("sparse grouped measure costs=%+v", selected)
	}
}

func TestSelectGroupedMeasureRejectedCostsAreRecorded(t *testing.T) {
	selected := selectGroupedAggregate(aggregateGroupedFacts{
		hasMeasure:         true,
		filterCardinality:  10,
		maxID:              9,
		hasMaxID:           true,
		groupCountUpperMax: 10,
		metricFieldCount:   1,
		measureMetricCount: 1,
		measureRows:        100_000,
		measureLookupSteps: 16,
		measureMode:        aggregateMeasureAccessLookup,
		measureCost:        160,
	})
	if selected.route != aggregateRouteGroupedRecursive || selected.rejected != aggregateRouteGroupedMeasure {
		t.Fatalf("lookup grouped measure decision=%+v", selected)
	}
	if selected.selectedCost <= 0 || selected.rejectedCost <= 0 || selected.selectedCost >= selected.rejectedCost {
		t.Fatalf("lookup grouped measure costs=%+v", selected)
	}
}

func TestExecuteAggregateEmitsSelectedRouteTrace(t *testing.T) {
	tests := []struct {
		name      string
		q         *qx.QX
		wantRoute string
	}{
		{
			name:      "row_count",
			q:         qx.Aggregate(qx.ROWCOUNT().AS("rows")),
			wantRoute: "row_count",
		},
		{
			name:      "distinct_ungrouped",
			q:         qx.Aggregate(qx.DISTINCT("country").AS("country")),
			wantRoute: "distinct_ungrouped",
		},
		{
			name:      "count_distinct_ungrouped",
			q:         qx.Aggregate(qx.COUNT(qx.DISTINCT("country")).AS("countries")),
			wantRoute: "count_distinct_ungrouped",
		},
		{
			name:      "ungrouped_ordinary",
			q:         qx.Aggregate(qx.SUM("age").AS("sum")),
			wantRoute: "ungrouped_ordinary",
		},
		{
			name:      "ungrouped_measure",
			q:         qx.Aggregate(qx.SUM("amount").AS("sum")),
			wantRoute: "ungrouped_measure",
		},
		{
			name:      "ungrouped_hybrid",
			q:         qx.Aggregate(qx.SUM("age").AS("age_sum"), qx.SUM("amount").AS("amount_sum")),
			wantRoute: "ungrouped_hybrid",
		},
		{
			name:      "grouped_recursive",
			q:         qx.Group("country").Metrics(qx.ROWCOUNT().AS("rows")),
			wantRoute: "grouped_recursive",
		},
		{
			name:      "grouped_ordinary_by_id",
			q:         qx.Group("country").Metrics(qx.SUM("age").AS("sum")),
			wantRoute: "grouped_ordinary_by_id",
		},
		{
			name:      "grouped_measure",
			q:         qx.Group("country").Metrics(qx.SUM("amount").AS("sum")),
			wantRoute: "grouped_measure",
		},
		{
			name:      "grouped_hybrid",
			q:         qx.Group("country").Metrics(qx.SUM("age").AS("age_sum"), qx.SUM("amount").AS("amount_sum")),
			wantRoute: "grouped_hybrid",
		},
	}

	seen := make(map[string]bool, len(tests))
	for i := range tests {
		tc := tests[i]
		var events []rbitrace.Event
		db := newQaggTestDB(t, func(ev rbitrace.Event) {
			events = append(events, ev)
		})

		prepared, err := Prepare(tc.q, db.rt, db.rt.IndexedByName)
		if err != nil {
			t.Fatalf("%s Prepare: %v", tc.name, err)
		}
		view := db.view()
		result, err := Execute(view, db.snap, prepared)
		db.exec.ReleaseView(view)
		prepared.Release()
		if err != nil {
			t.Fatalf("%s Execute: %v", tc.name, err)
		}
		if len(events) != 1 {
			t.Fatalf("%s trace events=%d, want 1", tc.name, len(events))
		}
		ev := events[0]
		if ev.Plan != rbitrace.PlanAggregate {
			t.Fatalf("%s plan=%q, want %q", tc.name, ev.Plan, rbitrace.PlanAggregate)
		}
		if ev.AggregateRoute.Selected != tc.wantRoute {
			t.Fatalf("%s route=%+v, want selected %q", tc.name, ev.AggregateRoute, tc.wantRoute)
		}
		seen[ev.AggregateRoute.Selected] = true
		if ev.AggregateRoute.FilterInput == "" {
			t.Fatalf("%s empty aggregate filter input: %+v", tc.name, ev.AggregateRoute)
		}
		if ev.AggregateRoute.ExpectedFilterRows == 0 {
			t.Fatalf("%s missing expected filter rows: %+v", tc.name, ev.AggregateRoute)
		}
		if ev.RowsReturned != uint64(len(result.Rows)) {
			t.Fatalf("%s rows returned trace=%d result=%d", tc.name, ev.RowsReturned, len(result.Rows))
		}
		switch tc.wantRoute {
		case "grouped_recursive", "grouped_ordinary_by_id", "grouped_measure", "grouped_hybrid":
			if ev.AggregateRoute.ExpectedGroups == 0 {
				t.Fatalf("%s missing expected groups: %+v", tc.name, ev.AggregateRoute)
			}
		}
		switch tc.wantRoute {
		case "grouped_ordinary_by_id", "grouped_measure", "grouped_hybrid":
			if ev.AggregateRoute.Rejected == "" || ev.AggregateRoute.SelectedCost <= 0 || ev.AggregateRoute.RejectedCost <= 0 {
				t.Fatalf("%s missing competing route cost: %+v", tc.name, ev.AggregateRoute)
			}
		}
		switch tc.wantRoute {
		case "grouped_ordinary_by_id", "grouped_measure", "grouped_hybrid":
			if ev.AggregateRoute.GroupMapLen == 0 {
				t.Fatalf("%s missing group map length: %+v", tc.name, ev.AggregateRoute)
			}
		}
		switch tc.wantRoute {
		case "ungrouped_measure", "ungrouped_hybrid", "grouped_measure", "grouped_hybrid":
			if ev.AggregateRoute.MeasureRows == 0 || ev.AggregateRoute.MeasureLookupSteps == 0 || ev.AggregateRoute.MeasureMetricCount == 0 || ev.AggregateRoute.MeasureMode == "" {
				t.Fatalf("%s missing measure diagnostics: %+v", tc.name, ev.AggregateRoute)
			}
		}
	}
	for _, route := range []string{
		"row_count",
		"distinct_ungrouped",
		"count_distinct_ungrouped",
		"ungrouped_ordinary",
		"ungrouped_measure",
		"ungrouped_hybrid",
		"grouped_recursive",
		"grouped_ordinary_by_id",
		"grouped_measure",
		"grouped_hybrid",
	} {
		if !seen[route] {
			t.Fatalf("aggregate trace table does not cover route %q", route)
		}
	}
}

func TestExecuteGroupedRecursiveTraceIncludesRejectedByID(t *testing.T) {
	var events []rbitrace.Event
	db := newQaggSparseIDTestDB(t, func(ev rbitrace.Event) {
		events = append(events, ev)
	})
	prepared, err := Prepare(qx.Group("country").Metrics(qx.SUM("age").AS("sum")), db.rt, db.rt.IndexedByName)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer prepared.Release()

	view := db.view()
	_, err = Execute(view, db.snap, prepared)
	db.exec.ReleaseView(view)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("trace events=%d, want 1", len(events))
	}
	route := events[0].AggregateRoute
	if route.Selected != "grouped_recursive" || route.Rejected != "grouped_ordinary_by_id" {
		t.Fatalf("aggregate trace route=%+v", route)
	}
	if route.SelectedCost <= 0 || route.RejectedCost <= 0 {
		t.Fatalf("aggregate trace missing costs: %+v", route)
	}
}

func TestExecuteAggregateMeasureTraceIncludesAccessMode(t *testing.T) {
	tests := []struct {
		name     string
		q        *qx.QX
		wantMode string
	}{
		{
			name:     "full_scan",
			q:        qx.Aggregate(qx.SUM("amount").AS("sum")),
			wantMode: "full_scan",
		},
		{
			name:     "merge_scan",
			q:        qx.Query(qx.EQ("active", true)).Metrics(qx.SUM("amount").AS("sum")),
			wantMode: "merge_scan",
		},
		{
			name:     "lookup",
			q:        qx.Query(qx.EQ("country", "US")).Metrics(qx.SUM("amount").AS("sum")),
			wantMode: "lookup",
		},
	}

	for i := range tests {
		tc := tests[i]
		var events []rbitrace.Event
		db := newQaggTestDB(t, func(ev rbitrace.Event) {
			events = append(events, ev)
		})
		prepared, err := Prepare(tc.q, db.rt, db.rt.IndexedByName)
		if err != nil {
			t.Fatalf("%s Prepare: %v", tc.name, err)
		}

		view := db.view()
		_, err = Execute(view, db.snap, prepared)
		db.exec.ReleaseView(view)
		prepared.Release()
		if err != nil {
			t.Fatalf("%s Execute: %v", tc.name, err)
		}
		if len(events) != 1 {
			t.Fatalf("%s trace events=%d, want 1", tc.name, len(events))
		}
		route := events[0].AggregateRoute
		if route.MeasureMode != tc.wantMode {
			t.Fatalf("%s measure mode=%q, want %q; route=%+v", tc.name, route.MeasureMode, tc.wantMode, route)
		}
		if route.MeasureMetricCount != 1 || route.OrdinaryMetricCount != 0 {
			t.Fatalf("%s metric counts missing: %+v", tc.name, route)
		}
		if route.MeasureRows == 0 || route.MeasureLookupSteps == 0 {
			t.Fatalf("%s measure stats missing: %+v", tc.name, route)
		}
	}
}
