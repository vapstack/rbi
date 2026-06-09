// Package rbitrace defines public query trace event payloads.
package rbitrace

import "time"

// PlanName is a stable planner route identifier emitted in trace events.
type PlanName string

const (
	PlanMaterialized PlanName = "plan_materialized"

	PlanAggregate          PlanName = "plan_aggregate"
	PlanCountMaterialized  PlanName = "plan_count_materialized"
	PlanCountUniqueEq      PlanName = "plan_count_unique_eq"
	PlanCountScalarLookup  PlanName = "plan_count_scalar_lookup"
	PlanCountScalarInSplit PlanName = "plan_count_scalar_in_split"
	PlanCountPredicates    PlanName = "plan_count_predicates"
	PlanCountORPredicates  PlanName = "plan_count_or_predicates"
	PlanCountORHybrid      PlanName = "plan_count_or_hybrid"

	PlanCandidateNoOrder PlanName = "plan_candidate_no_order"
	PlanCandidateOrder   PlanName = "plan_candidate_order"

	PlanORMergeNoOrder     PlanName = "plan_or_merge_no_order"
	PlanORMergeOrderMerge  PlanName = "plan_or_merge_order_merge"
	PlanORMergeOrderStream PlanName = "plan_or_merge_order_stream"

	PlanOrdered        PlanName = "plan_ordered"
	PlanOrderedNoOrder PlanName = "plan_ordered_no_order"
	PlanOrderedAnchor  PlanName = "plan_ordered_anchor"
	PlanOrderedLead    PlanName = "plan_ordered_lead"

	PlanLimit              PlanName = "plan_limit"
	PlanLimitOrderBasic    PlanName = "plan_limit_order_basic"
	PlanLimitOrderPrefix   PlanName = "plan_limit_order_prefix"
	PlanLimitPrefixNoOrder PlanName = "plan_limit_prefix_no_order"
	PlanLimitRangeNoOrder  PlanName = "plan_limit_range_no_order"
	PlanUniqueEq           PlanName = "plan_unique_eq"
)

// Event is an optional per-query planner execution trace.
type Event struct {
	Timestamp time.Time
	Duration  time.Duration

	Plan PlanName

	HasOrder   bool
	OrderField string
	OrderDesc  bool
	Offset     uint64
	Limit      uint64

	LeafCount int
	HasNeg    bool
	HasPrefix bool

	EstimatedRows uint64
	EstimatedCost float64
	FallbackCost  float64

	RowsExamined uint64
	RowsMatched  uint64
	RowsReturned uint64

	// PostingMaterializations counts temporary posting materializations performed
	// during execution (for example bucket composition or singleton expansion).
	PostingMaterializations uint64

	// PostingExactFilters counts per-bucket exact posting filter applications.
	PostingExactFilters uint64

	// CountPredicatePreparations counts count-side predicate preparation steps.
	CountPredicatePreparations uint64

	// CountRangeComplementBuilds counts broad-range complement materializations
	// performed during count predicate preparation.
	CountRangeComplementBuilds uint64

	// CountRangeComplementCacheHits counts count-side broad-range complement
	// cache hits during predicate preparation.
	CountRangeComplementCacheHits uint64

	// CountRangeComplementFastBuilds counts broad-range complement builds that
	// used the small-bucket fast path instead of generic bitmap union.
	CountRangeComplementFastBuilds uint64

	// CountRangeComplementRows is the total cardinality of materialized
	// broad-range complement bitmaps built during count preparation.
	CountRangeComplementRows uint64

	// ORBranches contains per-branch runtime metrics for OR plans.
	ORBranches []ORBranch
	// ORRoute contains route/cost diagnostics for OR selectors and runtime guards.
	ORRoute ORRoute
	// OrderedLimitRoute contains route/cost diagnostics for ordered LIMIT selectors.
	OrderedLimitRoute OrderedLimitRoute
	// NoOrderLimitRoute contains route/cost diagnostics for no-order LIMIT selectors.
	NoOrderLimitRoute NoOrderLimitRoute
	// ArrayPosOrderRoute contains route diagnostics for ArrayPos order selectors.
	ArrayPosOrderRoute ArrayPosOrderRoute
	// AggregateRoute contains route diagnostics for aggregate selectors.
	AggregateRoute AggregateRoute

	// OrderIndexScanWidth is the number of non-empty order-index buckets
	// traversed while producing query output.
	OrderIndexScanWidth uint64

	// DedupeCount is the number of duplicate candidates dropped globally.
	DedupeCount uint64

	// EarlyStopReason explains why execution stopped early.
	// Examples: "limit_reached", "input_exhausted", "candidates_exhausted".
	EarlyStopReason string

	Error string
}

// ORRoute carries route diagnostics for OR selector decisions.
type ORRoute struct {
	Selected     string
	Rejected     string
	SelectedCost float64
	RejectedCost float64
	SelectedWork RouteWork
	RejectedWork RouteWork
	ExpectedRows uint64
	UnionRows    uint64
	SumRows      uint64
	CacheState   string
	PostingBuild uint64

	Overlap   float64
	AvgChecks float64

	HasPrefixNonOrder   bool
	HasSelectiveLead    bool
	FallbackCollectFast bool
	PlannerAnalysisTime time.Duration
	PlannerPredicates   uint64
	PlannerCacheHits    uint64
	PlannerBuilds       uint64
	PlannerExactRanges  uint64
	PlannerReusedRanges uint64

	RuntimeGuardEnabled bool
	RuntimeGuardReason  string

	SampleExamined uint64
	SampleMatched  uint64
	SampleBuckets  uint64
	SampleDropped  uint64
	SampleFallback bool
	SampleReason   string

	RuntimeFallbackTriggered    bool
	RuntimeFallbackReason       string
	RuntimeExaminedPerUnique    float64
	RuntimeProjectedExamined    float64
	RuntimeProjectedExaminedMax float64
}

// RouteWork decomposes a selector's scalar cost into planner-visible work classes.
type RouteWork struct {
	CandidateScan            float64
	PostingContains          float64
	RangeProbe               float64
	ExactBucketFilter        float64
	BranchMerge              float64
	MaterializedBuild        float64
	RetainedCacheBenefit     float64
	UnretainedRebuildPenalty float64
	TailRiskPenalty          float64
}

// OrderedLimitRoute carries route diagnostics for ordered LIMIT selectors.
type OrderedLimitRoute struct {
	Selected   string
	Rejected   string
	CacheState string

	SelectedCost float64
	RejectedCost float64
	SelectedWork RouteWork
	RejectedWork RouteWork

	ExpectedRows    uint64
	OrderBuckets    uint64
	PredicateChecks uint64
	ExactFilters    uint64
	PostingBuild    uint64

	RuntimeGuardEnabled      bool
	RuntimeGuardReason       string
	RuntimeFallbackTriggered bool
	RuntimeFallbackReason    string
}

// NoOrderLimitRoute carries route diagnostics for no-order LIMIT selectors.
type NoOrderLimitRoute struct {
	Selected string
	Rejected string

	SelectedCost float64
	RejectedCost float64
	SelectedWork RouteWork
	RejectedWork RouteWork

	ExpectedRows uint64
	LeadRows     uint64
	Checks       uint64
	PostingBuild uint64

	RuntimeGuardEnabled      bool
	RuntimeGuardReason       string
	RuntimeFallbackTriggered bool
	RuntimeFallbackReason    string
}

// ArrayPosOrderRoute carries route diagnostics for ArrayPos order selectors.
type ArrayPosOrderRoute struct {
	Selected string
	Rejected string

	SelectedCost float64
	RejectedCost float64

	ExpectedRows uint64
}

// AggregateRoute carries route diagnostics for aggregate selectors.
type AggregateRoute struct {
	Selected    string
	Rejected    string
	FilterInput string
	MeasureMode string

	SelectedCost float64
	RejectedCost float64

	ExpectedFilterRows  uint64
	ExpectedGroups      uint64
	MetricFieldCount    uint64
	OrdinaryMetricCount uint64
	MeasureMetricCount  uint64
	MeasureRows         uint64
	MeasureLookupSteps  uint64
	OrdinaryKeyCount    uint64
	GroupMapLen         uint64
}

// ORBranch carries per-branch runtime metrics for OR plans.
type ORBranch struct {
	Index int

	RowsExamined uint64
	RowsEmitted  uint64

	Skipped    bool
	SkipReason string
}
