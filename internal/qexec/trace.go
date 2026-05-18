package qexec

import (
	"time"

	"github.com/vapstack/rbi/internal/qir"
)

type Trace struct {
	sink  func(TraceEvent)
	start time.Time
	ev    TraceEvent
}

func (r *Runtime) TraceSamplingEnabled() bool {
	return r.Tracer.Enabled()
}

func (r *Runtime) BeginTrace(q qir.Shape, orderField string) *Trace {
	sink := r.Tracer.SampleSink()
	if sink == nil {
		return nil
	}

	tr := new(Trace)
	tr.ev = TraceEvent{
		Timestamp: time.Now(),
		Offset:    q.Offset,
		Limit:     q.Limit,
	}
	if q.HasOrder {
		tr.ev.HasOrder = true
		tr.ev.OrderField = orderField
		tr.ev.OrderDesc = q.Order.Desc
	}

	var leavesBuf [8]qir.Expr
	leaves, ok := qir.CollectAndLeavesScratch(q.Expr, leavesBuf[:0], qir.LeafModeCollect)
	if ok {
		tr.ev.LeafCount = len(leaves)
		for _, expr := range leaves {
			if expr.Not {
				tr.ev.HasNeg = true
			}
			if expr.Op == qir.OpPREFIX {
				tr.ev.HasPrefix = true
			}
		}
	}

	tr.start = tr.ev.Timestamp
	tr.sink = sink
	return tr
}

func (t *Trace) Full() bool {
	return t != nil && t.sink != nil
}

func (t *Trace) Event() TraceEvent {
	if t == nil {
		return TraceEvent{}
	}
	return t.ev
}

func (t *Trace) RowsExamined() uint64 {
	if t == nil {
		return 0
	}
	return t.ev.RowsExamined
}

func (t *Trace) SetPlan(plan PlanName) {
	if t == nil {
		return
	}
	t.ev.Plan = string(plan)
}

func (t *Trace) AddExamined(n uint64) {
	if t == nil || n == 0 {
		return
	}
	t.ev.RowsExamined += n
}

func (t *Trace) AddMatched(n uint64) {
	if !t.Full() || n == 0 {
		return
	}
	t.ev.RowsMatched += n
}

func (t *Trace) AddOrderScanWidth(n uint64) {
	if !t.Full() || n == 0 {
		return
	}
	t.ev.OrderIndexScanWidth += n
}

func (t *Trace) AddPostingMaterialization(n uint64) {
	if !t.Full() || n == 0 {
		return
	}
	t.ev.PostingMaterializations += n
}

func (t *Trace) AddPostingExactFilter(n uint64) {
	if !t.Full() || n == 0 {
		return
	}
	t.ev.PostingExactFilters += n
}

func (t *Trace) AddCountPredicatePreparation(n uint64) {
	if !t.Full() || n == 0 {
		return
	}
	t.ev.CountPredicatePreparations += n
}

func (t *Trace) AddCountRangeComplementCacheHit(n uint64) {
	if !t.Full() || n == 0 {
		return
	}
	t.ev.CountRangeComplementCacheHits += n
}

func (t *Trace) AddCountRangeComplementBuild(rows uint64, fast bool) {
	if !t.Full() {
		return
	}
	t.ev.CountRangeComplementBuilds++
	if fast {
		t.ev.CountRangeComplementFastBuilds++
	}
	t.ev.CountRangeComplementRows += rows
}

func (t *Trace) AddDedupe(n uint64) {
	if !t.Full() || n == 0 {
		return
	}
	t.ev.DedupeCount += n
}

func (t *Trace) SetEarlyStopReason(reason string) {
	if !t.Full() || reason == "" {
		return
	}
	if t.ev.EarlyStopReason != "" {
		return
	}
	t.ev.EarlyStopReason = reason
}

func (t *Trace) SetORBranches(branches []TraceORBranch) {
	if !t.Full() {
		return
	}
	if len(branches) == 0 {
		t.ev.ORBranches = nil
		return
	}
	t.ev.ORBranches = append(t.ev.ORBranches[:0], branches...)
}

func (t *Trace) SetEstimated(rows uint64, estCost, fallbackCost float64) {
	if t == nil {
		return
	}
	t.ev.EstimatedRows = rows
	if !t.Full() {
		return
	}
	t.ev.EstimatedCost = estCost
	t.ev.FallbackCost = fallbackCost
}

func (t *Trace) SetORRoute(route TraceORRoute) {
	if !t.Full() {
		return
	}
	t.ev.ORRoute.Route = route.Route
	t.ev.ORRoute.Reason = route.Reason
	t.ev.ORRoute.KWayCost = route.KWayCost
	t.ev.ORRoute.FallbackCost = route.FallbackCost
	t.ev.ORRoute.Overlap = route.Overlap
	t.ev.ORRoute.AvgChecks = route.AvgChecks
	t.ev.ORRoute.HasPrefixNonOrder = route.HasPrefixNonOrder
	t.ev.ORRoute.HasSelectiveLead = route.HasSelectiveLead
	t.ev.ORRoute.FallbackCollectFast = route.FallbackCollectFast
}

func (t *Trace) AddOROrderPlannerAnalysis(
	d time.Duration,
	predicates uint64,
	cacheHits uint64,
	builds uint64,
	exactRanges uint64,
	reusedRanges uint64,
) {
	if !t.Full() {
		return
	}
	t.ev.ORRoute.PlannerAnalysisTime += d
	t.ev.ORRoute.PlannerPredicates += predicates
	t.ev.ORRoute.PlannerCacheHits += cacheHits
	t.ev.ORRoute.PlannerBuilds += builds
	t.ev.ORRoute.PlannerExactRanges = exactRanges
	t.ev.ORRoute.PlannerReusedRanges = reusedRanges
}

func (t *Trace) SetOROrderRuntimeGuard(enabled bool, reason string) {
	if !t.Full() {
		return
	}
	t.ev.ORRoute.RuntimeGuardEnabled = enabled
	t.ev.ORRoute.RuntimeGuardReason = reason
}

func (t *Trace) SetOROrderRuntimeFallback(reason string, examinedPerUnique, projectedExamined, projectedExaminedMax float64) {
	if !t.Full() {
		return
	}
	t.ev.ORRoute.RuntimeFallbackTriggered = true
	t.ev.ORRoute.RuntimeFallbackReason = reason
	t.ev.ORRoute.RuntimeExaminedPerUnique = examinedPerUnique
	t.ev.ORRoute.RuntimeProjectedExamined = projectedExamined
	t.ev.ORRoute.RuntimeProjectedExaminedMax = projectedExaminedMax
}

func (t *Trace) Finish(rowsReturned uint64, err error) {
	if t == nil {
		return
	}
	t.ev.RowsReturned = rowsReturned
	if t.Full() {
		t.ev.Duration = time.Since(t.start)
	}
	if t.Full() && err != nil {
		t.ev.Error = err.Error()
	}
	if t.sink != nil {
		t.sink(t.ev)
	}
}
