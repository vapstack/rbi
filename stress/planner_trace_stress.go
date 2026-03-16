package main

import (
	"bytes"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vapstack/rbi"
)

type plannerTraceCollector struct {
	sampleEvery int
	topN        int

	active  atomic.Pointer[plannerTraceEpoch]
	workers sync.Map
}

type plannerTraceWorker struct {
	collector *plannerTraceCollector
	info      plannerTraceScopeInfo
	gid       uint64

	currentQuery string
}

type plannerTraceScopeInfo struct {
	ClassID    int
	ClassAlias string
	ClassName  string
	Role       string
}

type plannerTraceEpoch struct {
	sampleEvery int
	topN        int

	mu      sync.Mutex
	total   plannerTraceStats
	classes map[string]*plannerTraceStats
	queries map[string]*plannerTraceStats
	top     []plannerTraceSample
}

type plannerTraceStats struct {
	sampled          uint64
	errors           uint64
	ordered          uint64
	orSamples        uint64
	runtimeFallbacks uint64

	totalDurationNS      uint64
	maxDurationNS        uint64
	totalRowsExamined    uint64
	maxRowsExamined      uint64
	totalRowsMatched     uint64
	maxRowsMatched       uint64
	totalRowsReturned    uint64
	maxRowsReturned      uint64
	totalEstimatedRows   uint64
	maxEstimatedRows     uint64
	totalEstimatedCost   float64
	totalFallbackCost    float64
	totalBitmapMats      uint64
	maxBitmapMats        uint64
	totalBitmapExact     uint64
	maxBitmapExact       uint64
	totalOrderScanWidth  uint64
	maxOrderScanWidth    uint64
	totalDedupeCount     uint64
	maxDedupeCount       uint64
	planCounts           map[string]uint64
	earlyStopCounts      map[string]uint64
	orRouteCounts        map[string]uint64
	runtimeFallbackCause map[string]uint64
}

type plannerTraceSnapshot struct {
	enabled     bool
	sampleEvery int
	topN        int
	total       plannerTraceScopeReport
	classes     map[string]plannerTraceScopeReport
	queries     map[string]plannerTraceScopeReport
	top         []plannerTraceSample
}

type plannerTraceReport struct {
	Enabled     bool                    `json:"enabled"`
	SampleEvery int                     `json:"sample_every"`
	TopN        int                     `json:"top_n"`
	Total       plannerTraceScopeReport `json:"total"`
	TopSamples  []plannerTraceSample    `json:"top_samples,omitempty"`
}

type plannerTraceScopeReport struct {
	Sampled           uint64            `json:"sampled"`
	Errors            uint64            `json:"errors"`
	OrderedSamples    uint64            `json:"ordered_samples"`
	ORSamples         uint64            `json:"or_samples"`
	RuntimeFallbacks  uint64            `json:"runtime_fallbacks"`
	AvgDurationUs     float64           `json:"avg_duration_us"`
	MaxDurationUs     float64           `json:"max_duration_us"`
	AvgRowsExamined   float64           `json:"avg_rows_examined"`
	MaxRowsExamined   uint64            `json:"max_rows_examined"`
	AvgRowsMatched    float64           `json:"avg_rows_matched"`
	MaxRowsMatched    uint64            `json:"max_rows_matched"`
	AvgRowsReturned   float64           `json:"avg_rows_returned"`
	MaxRowsReturned   uint64            `json:"max_rows_returned"`
	AvgEstimatedRows  float64           `json:"avg_estimated_rows"`
	MaxEstimatedRows  uint64            `json:"max_estimated_rows"`
	AvgEstimatedCost  float64           `json:"avg_estimated_cost"`
	AvgFallbackCost   float64           `json:"avg_fallback_cost"`
	AvgBitmapMats     float64           `json:"avg_bitmap_materializations"`
	MaxBitmapMats     uint64            `json:"max_bitmap_materializations"`
	AvgBitmapExact    float64           `json:"avg_bitmap_exact_filters"`
	MaxBitmapExact    uint64            `json:"max_bitmap_exact_filters"`
	AvgOrderScanWidth float64           `json:"avg_order_scan_width"`
	MaxOrderScanWidth uint64            `json:"max_order_scan_width"`
	AvgDedupeCount    float64           `json:"avg_dedupe_count"`
	MaxDedupeCount    uint64            `json:"max_dedupe_count"`
	PlanCounts        map[string]uint64 `json:"plan_counts,omitempty"`
	EarlyStopCounts   map[string]uint64 `json:"early_stop_counts,omitempty"`
	ORRouteCounts     map[string]uint64 `json:"or_route_counts,omitempty"`
	RuntimeFallbackBy map[string]uint64 `json:"runtime_fallback_by,omitempty"`
}

type plannerTraceSample struct {
	CapturedAt string  `json:"captured_at"`
	ClassID    int     `json:"class_id"`
	ClassAlias string  `json:"class_alias"`
	ClassName  string  `json:"class_name"`
	Role       string  `json:"role"`
	Query      string  `json:"query"`
	Plan       string  `json:"plan"`
	DurationUs float64 `json:"duration_us"`

	HasOrder   bool   `json:"has_order"`
	OrderField string `json:"order_field,omitempty"`
	OrderDesc  bool   `json:"order_desc,omitempty"`
	Offset     uint64 `json:"offset,omitempty"`
	Limit      uint64 `json:"limit,omitempty"`

	LeafCount int  `json:"leaf_count,omitempty"`
	HasNeg    bool `json:"has_neg,omitempty"`
	HasPrefix bool `json:"has_prefix,omitempty"`

	EstimatedRows uint64  `json:"estimated_rows,omitempty"`
	EstimatedCost float64 `json:"estimated_cost,omitempty"`
	FallbackCost  float64 `json:"fallback_cost,omitempty"`

	RowsExamined        uint64 `json:"rows_examined,omitempty"`
	RowsMatched         uint64 `json:"rows_matched,omitempty"`
	RowsReturned        uint64 `json:"rows_returned,omitempty"`
	BitmapMats          uint64 `json:"bitmap_materializations,omitempty"`
	BitmapExactFilters  uint64 `json:"bitmap_exact_filters,omitempty"`
	OrderIndexScanWidth uint64 `json:"order_index_scan_width,omitempty"`
	DedupeCount         uint64 `json:"dedupe_count,omitempty"`
	EarlyStopReason     string `json:"early_stop_reason,omitempty"`

	ORRoute           string                     `json:"or_route,omitempty"`
	ORRouteReason     string                     `json:"or_route_reason,omitempty"`
	ORRuntimeGuard    bool                       `json:"or_runtime_guard,omitempty"`
	ORRuntimeReason   string                     `json:"or_runtime_reason,omitempty"`
	ORRuntimeFallback bool                       `json:"or_runtime_fallback,omitempty"`
	ORFallbackReason  string                     `json:"or_fallback_reason,omitempty"`
	ORBranches        []plannerTraceBranchSample `json:"or_branches,omitempty"`

	Error string `json:"error,omitempty"`
}

type plannerTraceBranchSample struct {
	Index        int    `json:"index"`
	RowsExamined uint64 `json:"rows_examined,omitempty"`
	RowsEmitted  uint64 `json:"rows_emitted,omitempty"`
	Skipped      bool   `json:"skipped,omitempty"`
	SkipReason   string `json:"skip_reason,omitempty"`
}

func newPlannerTraceCollector(_ []*classDescriptor, sampleEvery, topN int) *plannerTraceCollector {
	if sampleEvery < 0 {
		return nil
	}
	if topN < 0 {
		topN = 0
	}
	return &plannerTraceCollector{
		sampleEvery: sampleEvery,
		topN:        topN,
	}
}

func (c *plannerTraceCollector) traceSink() func(rbi.TraceEvent) {
	if c == nil {
		return nil
	}
	return c.observe
}

func (c *plannerTraceCollector) newEpoch() *plannerTraceEpoch {
	if c == nil {
		return nil
	}
	epoch := &plannerTraceEpoch{
		sampleEvery: c.sampleEvery,
		topN:        c.topN,
		classes:     make(map[string]*plannerTraceStats, 8),
		queries:     make(map[string]*plannerTraceStats, 32),
	}
	c.active.Store(epoch)
	return epoch
}

func (c *plannerTraceCollector) newWorker(desc *classDescriptor) *plannerTraceWorker {
	if c == nil || desc == nil {
		return nil
	}
	return &plannerTraceWorker{
		collector: c,
		info: plannerTraceScopeInfo{
			ClassID:    desc.Info.ID,
			ClassAlias: desc.Info.Alias,
			ClassName:  desc.Info.Name,
			Role:       desc.Info.Role,
		},
	}
}

func (w *plannerTraceWorker) bindCurrentGoroutine() {
	if w == nil || w.collector == nil {
		return
	}
	w.gid = currentGoroutineID()
	if w.gid == 0 {
		return
	}
	w.collector.workers.Store(w.gid, w)
}

func (w *plannerTraceWorker) close() {
	if w == nil || w.collector == nil || w.gid == 0 {
		return
	}
	w.collector.workers.Delete(w.gid)
}

func (w *plannerTraceWorker) begin(query string) func() {
	if w == nil {
		return func() {}
	}
	prev := w.currentQuery
	w.currentQuery = query
	return func() {
		w.currentQuery = prev
	}
}

func traceQuery(ctx *WorkloadContext, query string) func() {
	if ctx == nil || ctx.Trace == nil {
		return func() {}
	}
	return ctx.Trace.begin(query)
}

func (c *plannerTraceCollector) observe(ev rbi.TraceEvent) {
	if c == nil {
		return
	}
	epoch := c.active.Load()
	if epoch == nil {
		return
	}
	gid := currentGoroutineID()
	if gid == 0 {
		return
	}
	workerAny, ok := c.workers.Load(gid)
	if !ok {
		return
	}
	worker, ok := workerAny.(*plannerTraceWorker)
	if !ok || worker == nil {
		return
	}
	query := worker.currentQuery
	if query == "" {
		query = "unknown"
	}
	epoch.observe(worker.info, query, ev)
}

func (e *plannerTraceEpoch) observe(info plannerTraceScopeInfo, query string, ev rbi.TraceEvent) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.total.observe(ev)

	classStats := e.classes[info.ClassName]
	if classStats == nil {
		classStats = &plannerTraceStats{}
		e.classes[info.ClassName] = classStats
	}
	classStats.observe(ev)

	queryKey := plannerTraceQueryKey(info.ClassName, query)
	queryStats := e.queries[queryKey]
	if queryStats == nil {
		queryStats = &plannerTraceStats{}
		e.queries[queryKey] = queryStats
	}
	queryStats.observe(ev)

	if e.topN == 0 {
		return
	}
	e.top = insertPlannerTraceSample(e.top, plannerTraceSampleFromEvent(info, query, ev), e.topN)
}

func (s *plannerTraceStats) observe(ev rbi.TraceEvent) {
	s.sampled++
	if ev.Error != "" {
		s.errors++
	}
	if ev.HasOrder {
		s.ordered++
	}
	if len(ev.ORBranches) > 0 || ev.ORRoute.Route != "" {
		s.orSamples++
	}
	if ev.ORRoute.RuntimeFallbackTriggered {
		s.runtimeFallbacks++
	}

	durationNS := uint64(ev.Duration.Nanoseconds())
	s.totalDurationNS += durationNS
	if durationNS > s.maxDurationNS {
		s.maxDurationNS = durationNS
	}
	s.totalRowsExamined += ev.RowsExamined
	if ev.RowsExamined > s.maxRowsExamined {
		s.maxRowsExamined = ev.RowsExamined
	}
	s.totalRowsMatched += ev.RowsMatched
	if ev.RowsMatched > s.maxRowsMatched {
		s.maxRowsMatched = ev.RowsMatched
	}
	s.totalRowsReturned += ev.RowsReturned
	if ev.RowsReturned > s.maxRowsReturned {
		s.maxRowsReturned = ev.RowsReturned
	}
	s.totalEstimatedRows += ev.EstimatedRows
	if ev.EstimatedRows > s.maxEstimatedRows {
		s.maxEstimatedRows = ev.EstimatedRows
	}
	s.totalEstimatedCost += ev.EstimatedCost
	s.totalFallbackCost += ev.FallbackCost
	s.totalBitmapMats += ev.BitmapMaterializations
	if ev.BitmapMaterializations > s.maxBitmapMats {
		s.maxBitmapMats = ev.BitmapMaterializations
	}
	s.totalBitmapExact += ev.BitmapExactFilters
	if ev.BitmapExactFilters > s.maxBitmapExact {
		s.maxBitmapExact = ev.BitmapExactFilters
	}
	s.totalOrderScanWidth += ev.OrderIndexScanWidth
	if ev.OrderIndexScanWidth > s.maxOrderScanWidth {
		s.maxOrderScanWidth = ev.OrderIndexScanWidth
	}
	s.totalDedupeCount += ev.DedupeCount
	if ev.DedupeCount > s.maxDedupeCount {
		s.maxDedupeCount = ev.DedupeCount
	}

	if ev.Plan != "" {
		if s.planCounts == nil {
			s.planCounts = make(map[string]uint64, 4)
		}
		s.planCounts[ev.Plan]++
	}
	if ev.EarlyStopReason != "" {
		if s.earlyStopCounts == nil {
			s.earlyStopCounts = make(map[string]uint64, 4)
		}
		s.earlyStopCounts[ev.EarlyStopReason]++
	}
	if ev.ORRoute.Route != "" {
		if s.orRouteCounts == nil {
			s.orRouteCounts = make(map[string]uint64, 4)
		}
		key := ev.ORRoute.Route
		if ev.ORRoute.Reason != "" {
			key += ":" + ev.ORRoute.Reason
		}
		s.orRouteCounts[key]++
	}
	if ev.ORRoute.RuntimeFallbackTriggered && ev.ORRoute.RuntimeFallbackReason != "" {
		if s.runtimeFallbackCause == nil {
			s.runtimeFallbackCause = make(map[string]uint64, 4)
		}
		s.runtimeFallbackCause[ev.ORRoute.RuntimeFallbackReason]++
	}
}

func (e *plannerTraceEpoch) snapshot() plannerTraceSnapshot {
	if e == nil {
		return plannerTraceSnapshot{}
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	out := plannerTraceSnapshot{
		enabled:     true,
		sampleEvery: e.sampleEvery,
		topN:        e.topN,
		total:       e.total.report(),
		classes:     make(map[string]plannerTraceScopeReport, len(e.classes)),
		queries:     make(map[string]plannerTraceScopeReport, len(e.queries)),
		top:         append([]plannerTraceSample(nil), e.top...),
	}
	for name, stats := range e.classes {
		out.classes[name] = stats.report()
	}
	for key, stats := range e.queries {
		out.queries[key] = stats.report()
	}
	return out
}

func (s *plannerTraceStats) report() plannerTraceScopeReport {
	if s == nil || s.sampled == 0 {
		return plannerTraceScopeReport{}
	}
	n := float64(s.sampled)
	return plannerTraceScopeReport{
		Sampled:           s.sampled,
		Errors:            s.errors,
		OrderedSamples:    s.ordered,
		ORSamples:         s.orSamples,
		RuntimeFallbacks:  s.runtimeFallbacks,
		AvgDurationUs:     nsToUs(float64(s.totalDurationNS) / n),
		MaxDurationUs:     nsToUs(float64(s.maxDurationNS)),
		AvgRowsExamined:   float64(s.totalRowsExamined) / n,
		MaxRowsExamined:   s.maxRowsExamined,
		AvgRowsMatched:    float64(s.totalRowsMatched) / n,
		MaxRowsMatched:    s.maxRowsMatched,
		AvgRowsReturned:   float64(s.totalRowsReturned) / n,
		MaxRowsReturned:   s.maxRowsReturned,
		AvgEstimatedRows:  float64(s.totalEstimatedRows) / n,
		MaxEstimatedRows:  s.maxEstimatedRows,
		AvgEstimatedCost:  s.totalEstimatedCost / n,
		AvgFallbackCost:   s.totalFallbackCost / n,
		AvgBitmapMats:     float64(s.totalBitmapMats) / n,
		MaxBitmapMats:     s.maxBitmapMats,
		AvgBitmapExact:    float64(s.totalBitmapExact) / n,
		MaxBitmapExact:    s.maxBitmapExact,
		AvgOrderScanWidth: float64(s.totalOrderScanWidth) / n,
		MaxOrderScanWidth: s.maxOrderScanWidth,
		AvgDedupeCount:    float64(s.totalDedupeCount) / n,
		MaxDedupeCount:    s.maxDedupeCount,
		PlanCounts:        cloneStringCounts(s.planCounts),
		EarlyStopCounts:   cloneStringCounts(s.earlyStopCounts),
		ORRouteCounts:     cloneStringCounts(s.orRouteCounts),
		RuntimeFallbackBy: cloneStringCounts(s.runtimeFallbackCause),
	}
}

func (s plannerTraceSnapshot) rootReport() *plannerTraceReport {
	if !s.enabled {
		return nil
	}
	out := &plannerTraceReport{
		Enabled:     true,
		SampleEvery: s.sampleEvery,
		TopN:        s.topN,
		Total:       s.total,
	}
	if len(s.top) > 0 {
		out.TopSamples = append([]plannerTraceSample(nil), s.top...)
	}
	return out
}

func (s plannerTraceSnapshot) classReport(name string) *plannerTraceScopeReport {
	if !s.enabled {
		return nil
	}
	report, ok := s.classes[name]
	if !ok || report.Sampled == 0 {
		return nil
	}
	return &report
}

func (s plannerTraceSnapshot) queryReport(className, query string) *plannerTraceScopeReport {
	if !s.enabled {
		return nil
	}
	report, ok := s.queries[plannerTraceQueryKey(className, query)]
	if !ok || report.Sampled == 0 {
		return nil
	}
	return &report
}

func plannerTraceSampleFromEvent(info plannerTraceScopeInfo, query string, ev rbi.TraceEvent) plannerTraceSample {
	out := plannerTraceSample{
		CapturedAt:          ev.Timestamp.Format(time.RFC3339Nano),
		ClassID:             info.ClassID,
		ClassAlias:          info.ClassAlias,
		ClassName:           info.ClassName,
		Role:                info.Role,
		Query:               query,
		Plan:                ev.Plan,
		DurationUs:          nsToUs(float64(ev.Duration.Nanoseconds())),
		HasOrder:            ev.HasOrder,
		OrderField:          ev.OrderField,
		OrderDesc:           ev.OrderDesc,
		Offset:              ev.Offset,
		Limit:               ev.Limit,
		LeafCount:           ev.LeafCount,
		HasNeg:              ev.HasNeg,
		HasPrefix:           ev.HasPrefix,
		EstimatedRows:       ev.EstimatedRows,
		EstimatedCost:       ev.EstimatedCost,
		FallbackCost:        ev.FallbackCost,
		RowsExamined:        ev.RowsExamined,
		RowsMatched:         ev.RowsMatched,
		RowsReturned:        ev.RowsReturned,
		BitmapMats:          ev.BitmapMaterializations,
		BitmapExactFilters:  ev.BitmapExactFilters,
		OrderIndexScanWidth: ev.OrderIndexScanWidth,
		DedupeCount:         ev.DedupeCount,
		EarlyStopReason:     ev.EarlyStopReason,
		ORRoute:             ev.ORRoute.Route,
		ORRouteReason:       ev.ORRoute.Reason,
		ORRuntimeGuard:      ev.ORRoute.RuntimeGuardEnabled,
		ORRuntimeReason:     ev.ORRoute.RuntimeGuardReason,
		ORRuntimeFallback:   ev.ORRoute.RuntimeFallbackTriggered,
		ORFallbackReason:    ev.ORRoute.RuntimeFallbackReason,
		Error:               ev.Error,
	}
	if len(ev.ORBranches) > 0 {
		out.ORBranches = make([]plannerTraceBranchSample, 0, len(ev.ORBranches))
		for _, branch := range ev.ORBranches {
			out.ORBranches = append(out.ORBranches, plannerTraceBranchSample{
				Index:        branch.Index,
				RowsExamined: branch.RowsExamined,
				RowsEmitted:  branch.RowsEmitted,
				Skipped:      branch.Skipped,
				SkipReason:   branch.SkipReason,
			})
		}
	}
	return out
}

func insertPlannerTraceSample(samples []plannerTraceSample, sample plannerTraceSample, limit int) []plannerTraceSample {
	if limit <= 0 {
		return samples
	}
	idx := slices.IndexFunc(samples, func(existing plannerTraceSample) bool {
		return sample.DurationUs > existing.DurationUs
	})
	if idx < 0 {
		if len(samples) >= limit {
			return samples
		}
		return append(samples, sample)
	}
	samples = append(samples, plannerTraceSample{})
	copy(samples[idx+1:], samples[idx:])
	samples[idx] = sample
	if len(samples) > limit {
		samples = samples[:limit]
	}
	return samples
}

func cloneStringCounts(src map[string]uint64) map[string]uint64 {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]uint64, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func plannerTraceQueryKey(className, query string) string {
	return className + "\x00" + query
}

func nsToUs(ns float64) float64 {
	return ns / 1000.0
}

func currentGoroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	if n <= 0 {
		return 0
	}
	fields := bytes.Fields(buf[:n])
	if len(fields) < 2 {
		return 0
	}
	id, err := strconv.ParseUint(string(fields[1]), 10, 64)
	if err != nil {
		return 0
	}
	return id
}
