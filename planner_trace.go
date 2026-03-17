package rbi

import (
	"time"

	"github.com/vapstack/qx"
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

	// BitmapMaterializations counts temporary bitmap materializations performed
	// during execution (for example bucket composition or singleton expansion).
	BitmapMaterializations uint64

	// BitmapExactFilters counts per-bucket exact bitmap filter applications.
	BitmapExactFilters uint64

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
	// ORRoute contains route/cost diagnostics for ordered OR merge path.
	ORRoute TraceORRoute

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

	KWayCost     float64
	FallbackCost float64
	Overlap      float64
	AvgChecks    float64

	HasPrefixNonOrder   bool
	HasSelectiveLead    bool
	FallbackCollectFast bool

	RuntimeGuardEnabled bool
	RuntimeGuardReason  string

	RuntimeFallbackTriggered    bool
	RuntimeFallbackReason       string
	RuntimeExaminedPerUnique    float64
	RuntimeProjectedExamined    float64
	RuntimeProjectedExaminedMax float64
}

type TraceORBranch struct {
	Index int

	RowsExamined uint64
	RowsEmitted  uint64

	Skipped    bool
	SkipReason string
}

type queryTrace struct {
	sink     func(TraceEvent)
	onFinish func(TraceEvent)
	start    time.Time
	ev       TraceEvent
}

func (t *queryTrace) full() bool {
	return t != nil && t.sink != nil
}

func traceSampleEvery(sampleEvery int, sink func(TraceEvent)) uint64 {
	if sink == nil {
		return 0
	}
	if sampleEvery < 0 {
		return 0
	}
	if sampleEvery == 0 {
		return 1
	}
	return uint64(sampleEvery)
}

func (db *DB[K, V]) traceOrCalibrationSamplingEnabled() bool {
	if db.traceRoot != nil {
		return db.traceRoot.traceOrCalibrationSamplingEnabled()
	}
	if db.planner.tracer.sink != nil && db.planner.tracer.sampleEvery > 0 {
		return true
	}
	return db.planner.calibrator.enabled && db.planner.calibrator.sampleEvery > 0
}

// beginTrace handles begin trace.
func (db *DB[K, V]) beginTrace(q *qx.QX) *queryTrace {
	if db.traceRoot != nil {
		return db.traceRoot.beginTrace(q)
	}

	emitTrace := false
	sink := db.planner.tracer.sink
	if sink != nil && db.planner.tracer.sampleEvery > 0 {
		emitTrace = db.shouldSampleTrace()
	}

	emitCalibration := false
	if db.planner.calibrator.enabled && db.planner.calibrator.sampleEvery > 0 {
		emitCalibration = db.shouldSampleCalibration()
	}

	if !emitTrace && !emitCalibration {
		return nil
	}

	tr := new(queryTrace)
	if emitTrace {
		tr.ev = TraceEvent{
			Timestamp: time.Now(),
			Offset:    q.Offset,
			Limit:     q.Limit,
		}
		if len(q.Order) > 0 {
			tr.ev.HasOrder = true
			tr.ev.OrderField = q.Order[0].Field
			tr.ev.OrderDesc = q.Order[0].Desc
		}

		leaves, ok := collectAndLeaves(q.Expr)
		if ok {
			tr.ev.LeafCount = len(leaves)
			for _, e := range leaves {
				if e.Not {
					tr.ev.HasNeg = true
				}
				if e.Op == qx.OpPREFIX {
					tr.ev.HasPrefix = true
				}
			}
		}

		tr.start = tr.ev.Timestamp
		tr.sink = sink
	}
	if emitCalibration {
		tr.onFinish = db.observeCalibration
	}
	return tr
}

func (db *DB[K, V]) shouldSampleTrace() bool {
	every := db.planner.tracer.sampleEvery
	if every <= 1 {
		return true
	}
	seq := db.planner.tracer.seq.Add(1)
	return seq%every == 0
}

func (t *queryTrace) setPlan(plan PlanName) {
	if t == nil {
		return
	}
	t.ev.Plan = string(plan)
}

func (t *queryTrace) addExamined(n uint64) {
	if t == nil || n == 0 {
		return
	}
	t.ev.RowsExamined += n
}

func (t *queryTrace) addMatched(n uint64) {
	if !t.full() || n == 0 {
		return
	}
	t.ev.RowsMatched += n
}

func (t *queryTrace) addOrderScanWidth(n uint64) {
	if !t.full() || n == 0 {
		return
	}
	t.ev.OrderIndexScanWidth += n
}

func (t *queryTrace) addBitmapMaterialized(n uint64) {
	if !t.full() || n == 0 {
		return
	}
	t.ev.BitmapMaterializations += n
}

func (t *queryTrace) addBitmapExactFilter(n uint64) {
	if !t.full() || n == 0 {
		return
	}
	t.ev.BitmapExactFilters += n
}

func (t *queryTrace) addCountPredicatePreparation(n uint64) {
	if !t.full() || n == 0 {
		return
	}
	t.ev.CountPredicatePreparations += n
}

func (t *queryTrace) addCountRangeComplementCacheHit(n uint64) {
	if !t.full() || n == 0 {
		return
	}
	t.ev.CountRangeComplementCacheHits += n
}

func (t *queryTrace) addCountRangeComplementBuild(rows uint64, fast bool) {
	if !t.full() {
		return
	}
	t.ev.CountRangeComplementBuilds++
	if fast {
		t.ev.CountRangeComplementFastBuilds++
	}
	t.ev.CountRangeComplementRows += rows
}

func (t *queryTrace) addDedupe(n uint64) {
	if !t.full() || n == 0 {
		return
	}
	t.ev.DedupeCount += n
}

func (t *queryTrace) setEarlyStopReason(reason string) {
	if !t.full() || reason == "" {
		return
	}
	if t.ev.EarlyStopReason != "" {
		return
	}
	t.ev.EarlyStopReason = reason
}

func (t *queryTrace) setORBranches(branches []TraceORBranch) {
	if !t.full() {
		return
	}
	if len(branches) == 0 {
		t.ev.ORBranches = nil
		return
	}
	t.ev.ORBranches = append(t.ev.ORBranches[:0], branches...)
}

func (t *queryTrace) setEstimated(rows uint64, estCost, fallbackCost float64) {
	if t == nil {
		return
	}
	t.ev.EstimatedRows = rows
	if !t.full() {
		return
	}
	t.ev.EstimatedCost = estCost
	t.ev.FallbackCost = fallbackCost
}

func (t *queryTrace) setOROrderRouteDecision(route, reason string, cost plannerOROrderRouteCost, avgChecks float64, fallbackCollectFast bool) {
	if !t.full() {
		return
	}
	t.ev.ORRoute.Route = route
	t.ev.ORRoute.Reason = reason
	t.ev.ORRoute.KWayCost = cost.kWay
	t.ev.ORRoute.FallbackCost = cost.fallback
	t.ev.ORRoute.Overlap = cost.overlap
	t.ev.ORRoute.AvgChecks = avgChecks
	t.ev.ORRoute.HasPrefixNonOrder = cost.hasPrefixNonOrder
	t.ev.ORRoute.HasSelectiveLead = cost.hasSelectiveLead
	t.ev.ORRoute.FallbackCollectFast = fallbackCollectFast
}

func (t *queryTrace) setOROrderRuntimeGuard(enabled bool, reason string) {
	if !t.full() {
		return
	}
	t.ev.ORRoute.RuntimeGuardEnabled = enabled
	t.ev.ORRoute.RuntimeGuardReason = reason
}

func (t *queryTrace) setOROrderRuntimeFallback(
	reason string,
	examinedPerUnique float64,
	projectedExamined float64,
	projectedExaminedMax float64,
) {
	if !t.full() {
		return
	}
	t.ev.ORRoute.RuntimeFallbackTriggered = true
	t.ev.ORRoute.RuntimeFallbackReason = reason
	t.ev.ORRoute.RuntimeExaminedPerUnique = examinedPerUnique
	t.ev.ORRoute.RuntimeProjectedExamined = projectedExamined
	t.ev.ORRoute.RuntimeProjectedExaminedMax = projectedExaminedMax
}

func (t *queryTrace) finish(rowsReturned uint64, err error) {
	if t == nil {
		return
	}
	t.ev.RowsReturned = rowsReturned
	if t.full() {
		t.ev.Duration = time.Since(t.start)
	}
	if t.full() && err != nil {
		t.ev.Error = err.Error()
	}
	if t.onFinish != nil {
		t.onFinish(t.ev)
	}
	if t.sink != nil {
		t.sink(t.ev)
	}
}
