package qexec

import (
	"errors"
	"time"
)

var ErrInvalidQuery = errors.New("invalid query")

// PlanName is a stable plan identifier used by tracing.
type PlanName string

const (
	PlanMaterialized PlanName = "plan_materialized"

	planFilterCardinalityMaterialized  PlanName = "plan_count_materialized"
	planFilterCardinalityUniqueEq      PlanName = "plan_count_unique_eq"
	planFilterCardinalityScalarLookup  PlanName = "plan_count_scalar_lookup"
	planFilterCardinalityScalarInSplit PlanName = "plan_count_scalar_in_split"
	planFilterCardinalityPredicates    PlanName = "plan_count_predicates"
	planFilterCardinalityORPredicates  PlanName = "plan_count_or_predicates"
	planFilterCardinalityORHybrid      PlanName = "plan_count_or_hybrid"

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

// TraceEvent is an optional per-query planner execution trace.
// It is emitted only when TraceSink is configured.
type TraceEvent struct {
	Timestamp time.Time
	Duration  time.Duration

	Plan string

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
	ORBranches []TraceORBranch
	// ORRoute contains route/cost diagnostics for OR selectors and runtime guards.
	ORRoute TraceORRoute
	// OrderedLimitRoute contains route/cost diagnostics for ordered LIMIT selectors.
	OrderedLimitRoute TraceOrderedLimitRoute
	// NoOrderLimitRoute contains route/cost diagnostics for no-order LIMIT selectors.
	NoOrderLimitRoute TraceNoOrderLimitRoute
	// ArrayPosOrderRoute contains route diagnostics for ArrayPos order selectors.
	ArrayPosOrderRoute TraceArrayPosOrderRoute
	// AggregateRoute contains route diagnostics for aggregate selectors.
	AggregateRoute TraceAggregateRoute

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

// TraceORRoute carries route diagnostics for ordered OR merge decisions.
type TraceORRoute struct {
	Route  string
	Reason string

	Selected     string
	Rejected     string
	SelectedCost float64
	RejectedCost float64
	SelectedWork TraceRouteWork
	RejectedWork TraceRouteWork
	ExpectedRows uint64
	UnionRows    uint64
	SumRows      uint64
	CacheState   string
	PostingBuild uint64

	KWayCost     float64
	FallbackCost float64
	Overlap      float64
	AvgChecks    float64

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

// TraceRouteWork decomposes a selector's scalar cost into planner-visible work classes.
type TraceRouteWork struct {
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

type TraceOrderedLimitRoute struct {
	Selected   string
	Rejected   string
	CacheState string

	SelectedCost float64
	RejectedCost float64
	SelectedWork TraceRouteWork
	RejectedWork TraceRouteWork

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

type TraceNoOrderLimitRoute struct {
	Selected string
	Rejected string

	SelectedCost float64
	RejectedCost float64
	SelectedWork TraceRouteWork
	RejectedWork TraceRouteWork

	ExpectedRows uint64
	LeadRows     uint64
	Checks       uint64
	PostingBuild uint64

	RuntimeGuardEnabled      bool
	RuntimeGuardReason       string
	RuntimeFallbackTriggered bool
	RuntimeFallbackReason    string
}

type TraceArrayPosOrderRoute struct {
	Selected string
	Rejected string

	SelectedCost float64
	RejectedCost float64

	ExpectedRows uint64
}

type TraceAggregateRoute struct {
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

type TraceORBranch struct {
	Index int

	RowsExamined uint64
	RowsEmitted  uint64

	Skipped    bool
	SkipReason string
}

// PlannerFieldStats contains per-field cardinality distribution metrics.
type PlannerFieldStats struct {
	// DistinctKeys is number of distinct keys in field index.
	DistinctKeys uint64
	// NonEmptyKeys is number of keys with non-empty posting bitmap.
	NonEmptyKeys uint64
	// TotalBucketCard is total cardinality summed across all field buckets.
	TotalBucketCard uint64
	// AvgBucketCard is average bucket cardinality.
	AvgBucketCard float64
	// MaxBucketCard is maximum bucket cardinality.
	MaxBucketCard uint64
	// P50BucketCard is median bucket cardinality.
	P50BucketCard uint64
	// P95BucketCard is 95th percentile bucket cardinality.
	P95BucketCard uint64
}

// PlannerStatsSnapshot is an immutable snapshot used by planner heuristics.
type PlannerStatsSnapshot struct {
	Version             uint64
	GeneratedAt         time.Time
	UniverseCardinality uint64
	Fields              map[string]PlannerFieldStats
}

func (s *PlannerStatsSnapshot) UniverseOr(fallback uint64) uint64 {
	if s != nil && s.UniverseCardinality > 0 {
		return s.UniverseCardinality
	}
	return fallback
}
