package qagg

import (
	"fmt"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/rbierrors"
	"github.com/vapstack/rbi/rbitrace"
)

type aggregateSelectorFamily uint8

const (
	aggregateSelectorCount aggregateSelectorFamily = iota + 1
	aggregateSelectorDistinct
	aggregateSelectorUngrouped
	aggregateSelectorGrouped
)

type aggregateFilterInput uint8

const (
	aggregateFilterInputCardinality aggregateFilterInput = iota + 1
	aggregateFilterInputMaterializedIDs
	aggregateFilterInputIntegrated
)

type aggregateRouteCandidate uint8

const (
	aggregateRouteRowCount aggregateRouteCandidate = iota + 1
	aggregateRouteDistinctUngrouped
	aggregateRouteCountDistinctUngrouped
	aggregateRouteUngroupedOrdinary
	aggregateRouteUngroupedMeasure
	aggregateRouteUngroupedHybrid
	aggregateRouteGroupedRecursive
	aggregateRouteGroupedOrdinaryByID
	aggregateRouteGroupedMeasure
	aggregateRouteGroupedHybrid
)

type aggregateMeasureAccess uint8

const (
	aggregateMeasureAccessNone aggregateMeasureAccess = iota
	aggregateMeasureAccessFullScan
	aggregateMeasureAccessMergeScan
	aggregateMeasureAccessLookup
	aggregateMeasureAccessMixed
)

type aggregateGroupLookup uint8

const (
	aggregateGroupLookupNone aggregateGroupLookup = iota
	aggregateGroupLookupOrdinalSlice
	aggregateGroupLookupMap
)

type aggregateRouteDecision struct {
	route       aggregateRouteCandidate
	rejected    aggregateRouteCandidate
	filterInput aggregateFilterInput
	measureMode aggregateMeasureAccess
	groupLookup aggregateGroupLookup

	selectedCost float64
	rejectedCost float64

	expectedFilterRows  uint64
	expectedGroups      uint64
	metricFieldCount    uint64
	ordinaryMetricCount uint64
	measureMetricCount  uint64
	measureRows         uint64
	measureLookupSteps  uint64
	ordinaryKeyCount    uint64
	groupMapLen         uint64
}

type aggregateCountFacts struct {
	filterCardinality uint64
}

type aggregateDistinctFacts struct {
	filterCardinality uint64
	ordinaryKeyCount  uint64
	countResult       bool
}

type aggregateUngroupedFacts struct {
	hasOrdinary bool
	hasMeasure  bool

	filterCardinality   uint64
	metricFieldCount    uint64
	ordinaryMetricCount uint64
	measureMetricCount  uint64
	measureRows         uint64
	measureLookupSteps  uint64
	measureMode         aggregateMeasureAccess
	measureCost         float64
	ordinaryKeyCount    uint64
}

type aggregateGroupedFacts struct {
	hasOrdinary bool
	hasMeasure  bool

	filterCardinality uint64
	maxID             uint64
	hasMaxID          bool

	metricRows         uint64
	minMetricRows      uint64
	groupCountUpperMax uint64

	metricFieldCount    uint64
	ordinaryMetricCount uint64
	measureMetricCount  uint64
	measureRows         uint64
	measureLookupSteps  uint64
	measureMode         aggregateMeasureAccess
	measureCost         float64
	ordinaryKeyCount    uint64

	byIDCandidate bool
	byIDFeasible  bool
	byIDMapLen    uint64
}

func selectCountAggregate(facts aggregateCountFacts) aggregateRouteDecision {
	return aggregateRouteDecision{
		route:              aggregateRouteRowCount,
		filterInput:        aggregateFilterInputCardinality,
		selectedCost:       float64(facts.filterCardinality),
		expectedFilterRows: facts.filterCardinality,
	}
}

func selectAggregateFamily(q *Query) aggregateSelectorFamily {
	if len(q.groups) == 0 {
		if len(q.metrics) == 1 {
			if q.metrics[0].rowCount {
				return aggregateSelectorCount
			}
			if q.metrics[0].op == aggregateMetricDistinct {
				return aggregateSelectorDistinct
			}
			if q.metrics[0].op == aggregateMetricCountDistinct {
				return aggregateSelectorDistinct
			}
		}
		return aggregateSelectorUngrouped
	}
	return aggregateSelectorGrouped
}

func selectDistinctAggregate(facts aggregateDistinctFacts) aggregateRouteDecision {
	route := aggregateRouteDistinctUngrouped
	if facts.countResult {
		route = aggregateRouteCountDistinctUngrouped
	}
	return aggregateRouteDecision{
		route:              route,
		filterInput:        aggregateFilterInputMaterializedIDs,
		selectedCost:       float64(facts.filterCardinality + facts.ordinaryKeyCount),
		expectedFilterRows: facts.filterCardinality,
		metricFieldCount:   1,
		ordinaryKeyCount:   facts.ordinaryKeyCount,
	}
}

func (ae *aggregateExecutor) collectDistinctFacts(q *Query, ids posting.List) aggregateDistinctFacts {
	acc := q.metrics[0].field.ordinary
	return aggregateDistinctFacts{
		filterCardinality: ids.Cardinality(),
		ordinaryKeyCount:  uint64(indexdata.NewFieldIndexViewFromStorage(ae.snap.Index[acc.Ordinal]).KeyCount()),
		countResult:       q.metrics[0].op == aggregateMetricCountDistinct,
	}
}

func (ae *aggregateExecutor) collectUngroupedFacts(q *Query, ids posting.List) aggregateUngroupedFacts {
	var facts aggregateUngroupedFacts
	facts.filterCardinality = ids.Cardinality()
	universe := ae.snap.Universe.Cardinality()
	for i := range q.metrics {
		metric := q.metrics[i]
		if metric.rowCount {
			continue
		}
		facts.metricFieldCount++
		if metric.field.isMeasure {
			facts.hasMeasure = true
			facts.measureMetricCount++
			storage := ae.snap.Measure[metric.field.measure.Ordinal]
			rows := uint64(storage.Rows())
			lookupSteps := storage.LookupSteps()
			access := selectAggregateMeasureAccess(facts.filterCardinality, universe, storage)
			facts.measureRows += rows
			facts.measureLookupSteps += lookupSteps
			facts.measureMode = aggregateMergeMeasureAccess(facts.measureMode, access)
			facts.measureCost += aggregateMeasureAccessCost(access, facts.filterCardinality, rows, lookupSteps)
		} else {
			facts.hasOrdinary = true
			facts.ordinaryMetricCount++
			if !hasPriorOrdinaryAggregateMetric(q.metrics, i) {
				facts.ordinaryKeyCount += uint64(indexdata.NewFieldIndexViewFromStorage(ae.snap.Index[metric.field.ordinary.Ordinal]).KeyCount())
			}
		}
	}
	return facts
}

func selectUngroupedAggregate(facts aggregateUngroupedFacts) aggregateRouteDecision {
	route := aggregateRouteUngroupedOrdinary
	if facts.hasMeasure {
		route = aggregateRouteUngroupedMeasure
		if facts.hasOrdinary {
			route = aggregateRouteUngroupedHybrid
		}
	}
	return aggregateRouteDecision{
		route:               route,
		filterInput:         aggregateFilterInputMaterializedIDs,
		measureMode:         facts.measureMode,
		selectedCost:        float64(facts.filterCardinality+facts.ordinaryKeyCount) + facts.measureCost,
		expectedFilterRows:  facts.filterCardinality,
		expectedGroups:      1,
		metricFieldCount:    facts.metricFieldCount,
		ordinaryMetricCount: facts.ordinaryMetricCount,
		measureMetricCount:  facts.measureMetricCount,
		measureRows:         facts.measureRows,
		measureLookupSteps:  facts.measureLookupSteps,
		ordinaryKeyCount:    facts.ordinaryKeyCount,
	}
}

func (ae *aggregateExecutor) collectGroupedFacts(q *Query, ids posting.List) aggregateGroupedFacts {
	var facts aggregateGroupedFacts
	facts.filterCardinality = ids.Cardinality()
	universe := ae.snap.Universe.Cardinality()
	for i := range q.metrics {
		metric := q.metrics[i]
		if metric.rowCount {
			continue
		}
		facts.metricFieldCount++
		if metric.field.isMeasure {
			facts.hasMeasure = true
			facts.measureMetricCount++
			storage := ae.snap.Measure[metric.field.measure.Ordinal]
			rows := uint64(storage.Rows())
			lookupSteps := storage.LookupSteps()
			access := selectAggregateMeasureAccess(facts.filterCardinality, universe, storage)
			facts.measureRows += rows
			facts.measureLookupSteps += lookupSteps
			facts.measureMode = aggregateMergeMeasureAccess(facts.measureMode, access)
			facts.measureCost += aggregateMeasureAccessCost(access, facts.filterCardinality, rows, lookupSteps)
		} else {
			facts.hasOrdinary = true
			facts.ordinaryMetricCount++
			if !hasPriorOrdinaryAggregateMetric(q.metrics, i) {
				ordinal := metric.field.ordinary.Ordinal
				ov := indexdata.NewFieldIndexViewFromStorage(ae.snap.Index[ordinal])
				fieldRows := universe
				nilOV := indexdata.NewFieldIndexViewFromStorage(ae.snap.NilIndex[ordinal])
				if nilOV.KeyCount() != 0 {
					fieldRows -= nilOV.LookupCardinality(indexdata.NilIndexEntryKey)
				}
				facts.ordinaryKeyCount += uint64(ov.KeyCount())
				facts.metricRows += fieldRows
				if facts.minMetricRows == 0 || fieldRows < facts.minMetricRows {
					facts.minMetricRows = fieldRows
				}
			}
		}
	}

	facts.maxID, facts.hasMaxID = ids.Maximum()
	facts.groupCountUpperMax = ae.aggregateGroupCountUpperBound(q, facts.filterCardinality)
	facts.byIDCandidate = facts.hasOrdinary && !facts.hasMeasure
	if facts.hasMaxID {
		facts.byIDMapLen = facts.maxID
		if facts.maxID != ^uint64(0) {
			facts.byIDMapLen++
		}
	}

	if !facts.byIDCandidate || !facts.hasMaxID || facts.maxID >= aggregateGroupIDOrdinalMaxLen {
		return facts
	}
	if facts.maxID+1 > facts.filterCardinality*16 {
		return facts
	}
	if facts.metricRows == 0 {
		return facts
	}
	if facts.filterCardinality*8 < facts.minMetricRows {
		return facts
	}
	facts.byIDFeasible = aggregateMulGreater(facts.groupCountUpperMax, facts.ordinaryKeyCount, facts.metricRows) ||
		(facts.groupCountUpperMax > facts.ordinaryKeyCount && aggregateMulGreater(facts.filterCardinality, facts.ordinaryKeyCount, facts.metricRows))
	return facts
}

func selectGroupedAggregate(facts aggregateGroupedFacts) aggregateRouteDecision {
	route := aggregateRouteGroupedRecursive
	rejected := aggregateRouteCandidate(0)
	groupLookup := aggregateGroupLookupNone
	groupMapLen := uint64(0)
	if !facts.hasMeasure && facts.byIDCandidate {
		rejected = aggregateRouteGroupedOrdinaryByID
	}
	recursiveCost := 0.0
	byIDCost := 0.0
	mapCost := 0.0
	groupedMeasureCost := 0.0
	mapLookup := aggregateGroupLookupNone
	mapLen := uint64(0)
	if facts.filterCardinality != 0 {
		recursiveCost = float64(facts.filterCardinality) +
			float64(facts.groupCountUpperMax) +
			float64(facts.metricRows) +
			float64(facts.ordinaryKeyCount)*float64(facts.groupCountUpperMax)
		if facts.hasMeasure && facts.measureMetricCount != 0 {
			lookupCost := float64(facts.filterCardinality) * float64(facts.measureLookupSteps)
			mergeCost := float64(facts.filterCardinality)*float64(facts.measureMetricCount) +
				float64(facts.measureRows)*float64(facts.groupCountUpperMax)
			if lookupCost < mergeCost {
				recursiveCost += lookupCost
			} else {
				recursiveCost += mergeCost
			}
		}
		byIDCost = float64(facts.filterCardinality) +
			float64(facts.metricRows) +
			float64(facts.byIDMapLen) +
			float64(facts.groupCountUpperMax)
		if facts.hasMeasure {
			groupLookup, groupMapLen, groupedMeasureCost = selectAggregateMeasureGroupLookup(facts, recursiveCost)
		} else if facts.byIDCandidate && !facts.byIDFeasible {
			mapLookup, mapLen, mapCost = selectAggregateOrdinaryGroupMapLookup(facts)
		}
	} else if facts.hasMeasure {
		groupedMeasureCost = facts.measureCost
	}
	selectedCost := recursiveCost
	rejectedCost := 0.0
	if facts.byIDFeasible {
		route = aggregateRouteGroupedOrdinaryByID
		rejected = aggregateRouteGroupedRecursive
		groupLookup = aggregateGroupLookupOrdinalSlice
		selectedCost = byIDCost
		rejectedCost = recursiveCost
		groupMapLen = facts.byIDMapLen
	} else if mapLookup != aggregateGroupLookupNone && recursiveCost > mapCost {
		route = aggregateRouteGroupedOrdinaryByID
		rejected = aggregateRouteGroupedRecursive
		groupLookup = mapLookup
		selectedCost = mapCost
		rejectedCost = recursiveCost
		groupMapLen = mapLen
	} else if facts.hasMeasure {
		measureRoute := aggregateRouteGroupedMeasure
		if facts.hasOrdinary {
			measureRoute = aggregateRouteGroupedHybrid
		}
		if groupLookup != aggregateGroupLookupNone || facts.filterCardinality == 0 {
			route = measureRoute
			rejected = aggregateRouteGroupedRecursive
			selectedCost = groupedMeasureCost
			rejectedCost = recursiveCost
		} else {
			rejected = measureRoute
			rejectedCost = groupedMeasureCost
		}
	} else if rejected == aggregateRouteGroupedOrdinaryByID {
		rejectedCost = byIDCost
	}
	return aggregateRouteDecision{
		route:               route,
		rejected:            rejected,
		filterInput:         aggregateFilterInputMaterializedIDs,
		measureMode:         facts.measureMode,
		groupLookup:         groupLookup,
		selectedCost:        selectedCost,
		rejectedCost:        rejectedCost,
		expectedFilterRows:  facts.filterCardinality,
		expectedGroups:      facts.groupCountUpperMax,
		metricFieldCount:    facts.metricFieldCount,
		ordinaryMetricCount: facts.ordinaryMetricCount,
		measureMetricCount:  facts.measureMetricCount,
		measureRows:         facts.measureRows,
		measureLookupSteps:  facts.measureLookupSteps,
		ordinaryKeyCount:    facts.ordinaryKeyCount,
		groupMapLen:         groupMapLen,
	}
}

func selectAggregateMeasureGroupLookup(facts aggregateGroupedFacts, recursiveCost float64) (aggregateGroupLookup, uint64, float64) {
	if facts.filterCardinality == 0 {
		return aggregateGroupLookupNone, 0, facts.measureCost
	}
	if facts.measureMode == aggregateMeasureAccessNone {
		return aggregateGroupLookupNone, 0, 0
	}
	if facts.groupCountUpperMax == 0 || facts.measureMetricCount == 0 {
		return aggregateGroupLookupNone, 0, 0
	}
	mapLen := uint64(0)
	if facts.hasMaxID {
		mapLen = facts.maxID
		if facts.maxID != ^uint64(0) {
			mapLen++
		}
	}
	cost := float64(facts.filterCardinality) +
		float64(facts.groupCountUpperMax) +
		float64(mapLen) +
		float64(facts.metricRows) +
		float64(facts.ordinaryKeyCount) +
		facts.measureCost
	if !facts.hasMaxID || facts.maxID >= aggregateGroupIDOrdinalMaxLen || facts.maxID+1 > facts.filterCardinality*16 {
		return aggregateGroupLookupNone, 0, cost
	}
	if cost >= recursiveCost {
		return aggregateGroupLookupNone, 0, cost
	}
	return aggregateGroupLookupOrdinalSlice, mapLen, cost
}

func selectAggregateOrdinaryGroupMapLookup(facts aggregateGroupedFacts) (aggregateGroupLookup, uint64, float64) {
	if facts.filterCardinality == 0 || facts.filterCardinality > aggregateGroupOrdinalMapMaxLen {
		return aggregateGroupLookupNone, 0, 0
	}
	if facts.metricRows < 512 || facts.groupCountUpperMax < 64 {
		return aggregateGroupLookupNone, 0, 0
	}
	if facts.hasMaxID && facts.maxID < aggregateGroupIDOrdinalMaxLen && facts.maxID+1 <= facts.filterCardinality*16 {
		return aggregateGroupLookupNone, 0, 0
	}
	cost := float64(facts.filterCardinality) +
		float64(facts.metricRows) +
		float64(facts.ordinaryKeyCount) +
		float64(facts.groupCountUpperMax)
	return aggregateGroupLookupMap, facts.filterCardinality, cost
}

func aggregateMulGreater(a uint64, b uint64, limit uint64) bool {
	return b != 0 && a > limit/b
}

func dispatchInvalidAggregateRoute(route aggregateRouteCandidate) error {
	return fmt.Errorf("%w: selected aggregate route %d was not executable", rbierrors.ErrInvalidQuery, route)
}

func (route aggregateRouteCandidate) String() string {
	switch route {
	case aggregateRouteRowCount:
		return "row_count"
	case aggregateRouteDistinctUngrouped:
		return "distinct_ungrouped"
	case aggregateRouteCountDistinctUngrouped:
		return "count_distinct_ungrouped"
	case aggregateRouteUngroupedOrdinary:
		return "ungrouped_ordinary"
	case aggregateRouteUngroupedMeasure:
		return "ungrouped_measure"
	case aggregateRouteUngroupedHybrid:
		return "ungrouped_hybrid"
	case aggregateRouteGroupedRecursive:
		return "grouped_recursive"
	case aggregateRouteGroupedOrdinaryByID:
		return "grouped_ordinary_by_id"
	case aggregateRouteGroupedMeasure:
		return "grouped_measure"
	case aggregateRouteGroupedHybrid:
		return "grouped_hybrid"
	default:
		return ""
	}
}

func (input aggregateFilterInput) String() string {
	switch input {
	case aggregateFilterInputCardinality:
		return "cardinality"
	case aggregateFilterInputMaterializedIDs:
		return "materialized_ids"
	case aggregateFilterInputIntegrated:
		return "integrated"
	default:
		return ""
	}
}

func selectAggregateMeasureAccess(filterCardinality uint64, universe uint64, storage indexdata.MeasureStorage) aggregateMeasureAccess {
	if filterCardinality == 0 || storage.Rows() == 0 {
		return aggregateMeasureAccessNone
	}
	if filterCardinality == universe {
		return aggregateMeasureAccessFullScan
	}
	if useMeasureMergeScan(filterCardinality, storage) {
		return aggregateMeasureAccessMergeScan
	}
	return aggregateMeasureAccessLookup
}

func aggregateMergeMeasureAccess(current aggregateMeasureAccess, next aggregateMeasureAccess) aggregateMeasureAccess {
	if next == aggregateMeasureAccessNone {
		return current
	}
	if current == aggregateMeasureAccessNone || current == next {
		return next
	}
	return aggregateMeasureAccessMixed
}

func aggregateMeasureAccessCost(access aggregateMeasureAccess, filterRows uint64, measureRows uint64, lookupSteps uint64) float64 {
	switch access {
	case aggregateMeasureAccessFullScan:
		return float64(measureRows)
	case aggregateMeasureAccessMergeScan:
		return float64(filterRows) + float64(measureRows)
	case aggregateMeasureAccessLookup:
		return float64(filterRows) * float64(lookupSteps)
	default:
		return 0
	}
}

func (access aggregateMeasureAccess) String() string {
	switch access {
	case aggregateMeasureAccessFullScan:
		return "full_scan"
	case aggregateMeasureAccessMergeScan:
		return "merge_scan"
	case aggregateMeasureAccessLookup:
		return "lookup"
	case aggregateMeasureAccessMixed:
		return "mixed"
	default:
		return ""
	}
}

func (decision aggregateRouteDecision) traceRoute() rbitrace.AggregateRoute {
	rejected := ""
	if decision.rejected != 0 {
		rejected = decision.rejected.String()
	}
	return rbitrace.AggregateRoute{
		Selected:            decision.route.String(),
		Rejected:            rejected,
		FilterInput:         decision.filterInput.String(),
		MeasureMode:         decision.measureMode.String(),
		SelectedCost:        decision.selectedCost,
		RejectedCost:        decision.rejectedCost,
		ExpectedFilterRows:  decision.expectedFilterRows,
		ExpectedGroups:      decision.expectedGroups,
		MetricFieldCount:    decision.metricFieldCount,
		OrdinaryMetricCount: decision.ordinaryMetricCount,
		MeasureMetricCount:  decision.measureMetricCount,
		MeasureRows:         decision.measureRows,
		MeasureLookupSteps:  decision.measureLookupSteps,
		OrdinaryKeyCount:    decision.ordinaryKeyCount,
		GroupMapLen:         decision.groupMapLen,
	}
}
