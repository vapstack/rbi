package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vapstack/rbi"
)

const (
	ceilingClassReadFast    = "read_fast"
	ceilingClassReadSlow    = "read_slow"
	ceilingClassWriteUpdate = "write_update"
	ceilingClassWriteInsert = "write_insert"
)

type ceilingRunFunc func(
	db *rbi.DB[uint64, UserBench],
	rng *rand.Rand,
	maxIDPtr *uint64,
	emailSamples []string,
) (string, error)

type ceilingClassSpec struct {
	Name    string
	Role    string
	Workers int
	Run     ceilingRunFunc
}

type ceilingWorkItem struct {
	ScheduledAt time.Time
}

type ceilingClassRuntime struct {
	Spec   ceilingClassSpec
	Target float64
	Queue  chan ceilingWorkItem
	M      *ceilingClassMetrics
}

type ceilingStagePlan struct {
	Name        string
	Description string
	Targets     map[string]float64
}

type ceilingSuitePlan struct {
	Name        string
	Description string
	Stages      []ceilingStagePlan
}

type ceilingFastBaseline struct {
	Throughput float64
	P99Us      float64
	Source     string
	Valid      bool
}

type ceilingWorkerMetrics struct {
	opCounts map[string]uint64

	queue   latencyReservoir
	service latencyReservoir
	end     latencyReservoir
}

type ceilingCounterSnapshot struct {
	offered   uint64
	enqueued  uint64
	dropped   uint64
	started   uint64
	completed uint64
	errors    uint64
	inflight  int64
	queueHigh uint64
}

type ceilingClassMetrics struct {
	offered   atomic.Uint64
	enqueued  atomic.Uint64
	dropped   atomic.Uint64
	started   atomic.Uint64
	completed atomic.Uint64
	errors    atomic.Uint64

	inflight     atomic.Int64
	inflightHigh atomic.Uint64
	queueHigh    atomic.Uint64

	workers []ceilingWorkerMetrics
}

func newCeilingClassMetrics(workers int) *ceilingClassMetrics {
	if workers < 1 {
		workers = 1
	}
	perWorkerCap := MaxLatencySampleSize / workers
	if perWorkerCap*workers < MaxLatencySampleSize {
		perWorkerCap++
	}
	if perWorkerCap < 256 {
		perWorkerCap = 256
	}

	m := &ceilingClassMetrics{workers: make([]ceilingWorkerMetrics, workers)}
	for i := 0; i < workers; i++ {
		m.workers[i] = ceilingWorkerMetrics{
			opCounts: make(map[string]uint64, 32),
			queue:    newLatencyReservoir(perWorkerCap, uint64(i+1)*0x9e3779b97f4a7c15),
			service:  newLatencyReservoir(perWorkerCap, uint64(i+1)*0x94d049bb133111eb),
			end:      newLatencyReservoir(perWorkerCap, uint64(i+1)*0xd6e8feb86659fd93),
		}
	}
	return m
}

func setAtomicU64Max(dst *atomic.Uint64, v uint64) {
	for {
		cur := dst.Load()
		if v <= cur {
			return
		}
		if dst.CompareAndSwap(cur, v) {
			return
		}
	}
}

func (m *ceilingClassMetrics) observeQueue(depth int) {
	if depth < 0 {
		depth = 0
	}
	setAtomicU64Max(&m.queueHigh, uint64(depth))
}

func (m *ceilingClassMetrics) onStart() {
	m.started.Add(1)
	inflight := m.inflight.Add(1)
	if inflight < 0 {
		inflight = 0
	}
	setAtomicU64Max(&m.inflightHigh, uint64(inflight))
}

func (m *ceilingClassMetrics) onDone(workerID int, kind string, queueWait time.Duration, service time.Duration, endToEnd time.Duration, err error) {
	m.completed.Add(1)
	if err != nil {
		m.errors.Add(1)
	}
	m.inflight.Add(-1)

	if kind == "" {
		kind = "unknown"
	}
	if workerID < 0 {
		workerID = 0
	}
	if n := len(m.workers); n > 0 {
		workerID %= n
		if workerID < 0 {
			workerID += n
		}
		w := &m.workers[workerID]
		w.opCounts[kind]++
		w.queue.add(float64(maxDuration(queueWait).Nanoseconds()))
		w.service.add(float64(maxDuration(service).Nanoseconds()))
		w.end.add(float64(maxDuration(endToEnd).Nanoseconds()))
	}
}

func maxDuration(v time.Duration) time.Duration {
	if v < 0 {
		return 0
	}
	return v
}

func (m *ceilingClassMetrics) snapshot() ceilingCounterSnapshot {
	return ceilingCounterSnapshot{
		offered:   m.offered.Load(),
		enqueued:  m.enqueued.Load(),
		dropped:   m.dropped.Load(),
		started:   m.started.Load(),
		completed: m.completed.Load(),
		errors:    m.errors.Load(),
		inflight:  m.inflight.Load(),
		queueHigh: m.queueHigh.Load(),
	}
}

func (m *ceilingClassMetrics) finalize(className, role string, durationSec float64, queueEnd int) CeilingClassResult {
	if durationSec <= 0 {
		durationSec = 1
	}
	if queueEnd < 0 {
		queueEnd = 0
	}

	queue := make([]float64, 0, 1024)
	service := make([]float64, 0, 1024)
	end := make([]float64, 0, 1024)
	opBreakdown := make(map[string]uint64, 64)

	for i := range m.workers {
		w := &m.workers[i]
		queue = append(queue, w.queue.values...)
		service = append(service, w.service.values...)
		end = append(end, w.end.values...)
		for k, v := range w.opCounts {
			opBreakdown[k] += v
		}
	}

	offered := m.offered.Load()
	completed := m.completed.Load()
	throughputRatio := 0.0
	if offered > 0 {
		throughputRatio = float64(completed) / float64(offered)
	}

	return CeilingClassResult{
		Class:              className,
		Role:               role,
		OfferedOps:         offered,
		EnqueuedOps:        m.enqueued.Load(),
		DroppedOps:         m.dropped.Load(),
		StartedOps:         m.started.Load(),
		CompletedOps:       completed,
		Errors:             m.errors.Load(),
		OfferedOpsPerSec:   float64(offered) / durationSec,
		CompletedOpsPerSec: float64(completed) / durationSec,
		ThroughputRatio:    throughputRatio,
		QueueDepthHigh:     m.queueHigh.Load(),
		QueueDepthEnd:      uint64(queueEnd),
		InFlightHigh:       m.inflightHigh.Load(),
		QueueLatencyUs:     summarizeLatencies(queue),
		ServiceLatencyUs:   summarizeLatencies(service),
		EndToEndLatencyUs:  summarizeLatencies(end),
		OperationBreakdown: opBreakdown,
	}
}

func runCeilingDispatcher(ctx context.Context, rt *ceilingClassRuntime, quantum time.Duration) {
	if rt == nil || rt.Target <= 0 {
		return
	}
	if quantum <= 0 {
		quantum = time.Millisecond
	}

	ticker := time.NewTicker(quantum)
	defer ticker.Stop()

	carry := 0.0
	lastTick := time.Now()
	basePerTick := rt.Target * quantum.Seconds()
	maxBurst := int(math.Ceil(basePerTick * 2.0))
	if maxBurst < 1 {
		maxBurst = 1
	}
	maxCatchUp := quantum * 4

	for {
		select {
		case <-ctx.Done():
			return
		case ts := <-ticker.C:
			// Ticker can coalesce/drop ticks under scheduler pressure.
			// Accumulate demand by real elapsed time to avoid systematic under-offer.
			elapsed := ts.Sub(lastTick)
			if elapsed <= 0 {
				elapsed = quantum
			}
			if elapsed > maxCatchUp {
				elapsed = maxCatchUp
			}
			lastTick = ts
			carry += rt.Target * elapsed.Seconds()
			n := int(carry)
			if n <= 0 {
				continue
			}
			if n > maxBurst {
				n = maxBurst
			}
			carry -= float64(n)

			var enqueued uint64
			var dropped uint64
			item := ceilingWorkItem{ScheduledAt: ts}
			for i := 0; i < n; i++ {
				select {
				case rt.Queue <- item:
					enqueued++
				default:
					dropped++
				}
			}
			rt.M.offered.Add(uint64(n))
			if enqueued > 0 {
				rt.M.enqueued.Add(enqueued)
				rt.M.observeQueue(len(rt.Queue))
			}
			if dropped > 0 {
				rt.M.dropped.Add(dropped)
			}
		}
	}
}

func runCeilingExecutor(
	ctx context.Context,
	rt *ceilingClassRuntime,
	workerID int,
	db *rbi.DB[uint64, UserBench],
	maxIDPtr *uint64,
	emailSamples []string,
) {
	if rt == nil || rt.Spec.Run == nil {
		return
	}

	seed := time.Now().UnixNano() + int64(workerID+1)*137 + int64(len(rt.Spec.Name))*104729
	rng := newRand(seed)

	for {
		select {
		case <-ctx.Done():
			return
		case item := <-rt.Queue:
			if ctx.Err() != nil {
				return
			}
			start := time.Now()
			queueWait := start.Sub(item.ScheduledAt)
			if queueWait < 0 {
				queueWait = 0
			}

			rt.M.onStart()
			kind, err := rt.Spec.Run(db, rng, maxIDPtr, emailSamples)
			service := time.Since(start)
			endToEnd := time.Since(item.ScheduledAt)
			if endToEnd < service {
				endToEnd = service
			}
			rt.M.onDone(workerID, kind, queueWait, service, endToEnd, err)
		}
	}
}

func detectCeilingClassSaturation(res *CeilingClassResult, target float64, opts benchOptions) (bool, []string) {
	if res == nil || target <= 0 {
		return false, nil
	}

	queuePressureDepth := opts.ceilingQueueCap / 100
	if queuePressureDepth < 4 {
		queuePressureDepth = 4
	}

	reasons := make([]string, 0, 4)
	if res.DroppedOps > 0 {
		reasons = append(reasons, "dropped_ops")
	}
	if res.OfferedOps >= 100 && res.ThroughputRatio < opts.ceilingMinThroughputRatio {
		reasons = append(reasons, "throughput_ratio")
	}
	if res.QueueDepthHigh >= uint64(opts.ceilingQueueCap) {
		reasons = append(reasons, "queue_highwater")
	}
	if res.QueueDepthEnd >= uint64(queuePressureDepth) && res.OfferedOps >= 100 {
		reasons = append(reasons, "queue_not_drained")
	}
	if res.QueueDepthHigh >= uint64(queuePressureDepth) && res.QueueLatencyUs.P95Us > 0 && res.ServiceLatencyUs.P95Us > 0 {
		if res.QueueLatencyUs.P95Us > res.ServiceLatencyUs.P95Us*3 {
			reasons = append(reasons, "queue_latency_pressure")
		}
	}
	return len(reasons) > 0, reasons
}

func evaluateFastReadRegression(fast *CeilingClassResult, base ceilingFastBaseline, opts benchOptions) *CeilingRegression {
	if fast == nil || !base.Valid || base.Throughput <= 0 {
		return nil
	}

	reg := new(CeilingRegression)
	reasons := make([]string, 0, 2)

	if fast.CompletedOpsPerSec < base.Throughput {
		reg.ThroughputDropPct = 100 * (base.Throughput - fast.CompletedOpsPerSec) / base.Throughput
		if reg.ThroughputDropPct >= opts.ceilingRegressThroughputPct {
			reasons = append(reasons, "throughput_drop")
		}
	}

	if base.P99Us > 0 && fast.ServiceLatencyUs.P99Us > base.P99Us {
		reg.P99IncreasePct = 100 * (fast.ServiceLatencyUs.P99Us - base.P99Us) / base.P99Us
		if reg.P99IncreasePct >= opts.ceilingRegressP99Pct {
			reasons = append(reasons, "p99_increase")
		}
	}

	reg.Regressed = len(reasons) > 0
	reg.Reasons = reasons
	if !reg.Regressed && reg.ThroughputDropPct == 0 && reg.P99IncreasePct == 0 {
		return nil
	}
	return reg
}

func parseOpsGrid(raw string) ([]float64, error) {
	parts := strings.Split(raw, ",")
	out := make([]float64, 0, len(parts))
	for _, p := range parts {
		s := strings.TrimSpace(p)
		if s == "" {
			continue
		}
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid number %q: %w", s, err)
		}
		if v < 0 {
			return nil, fmt.Errorf("negative number %q is not allowed", s)
		}
		out = append(out, v)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty grid")
	}
	slices.Sort(out)
	uniq := out[:0]
	for i, v := range out {
		if i == 0 || v != out[i-1] {
			uniq = append(uniq, v)
		}
	}
	return uniq, nil
}

func parseSuiteFilter(raw string) map[string]struct{} {
	s := strings.TrimSpace(strings.ToLower(raw))
	if s == "" || s == "all" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make(map[string]struct{}, len(parts))
	for _, p := range parts {
		v := strings.TrimSpace(strings.ToLower(p))
		if v == "" {
			continue
		}
		out[v] = struct{}{}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func suiteAllowed(filter map[string]struct{}, name string) bool {
	if filter == nil {
		return true
	}
	_, ok := filter[strings.ToLower(name)]
	return ok
}

func mergeOpsGrids(a, b []float64) []float64 {
	out := append([]float64(nil), a...)
	out = append(out, b...)
	if len(out) == 0 {
		return nil
	}
	slices.Sort(out)
	uniq := out[:0]
	for i, v := range out {
		if i == 0 || v != out[i-1] {
			uniq = append(uniq, v)
		}
	}
	return uniq
}

func formatRateTag(v float64) string {
	if math.Abs(v-math.Round(v)) < 1e-9 {
		return strconv.FormatInt(int64(math.Round(v)), 10)
	}
	s := strconv.FormatFloat(v, 'f', 2, 64)
	s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	return strings.ReplaceAll(s, ".", "_")
}

func buildSingleClassStages(prefix, desc, className string, grid []float64) []ceilingStagePlan {
	stages := make([]ceilingStagePlan, 0, len(grid))
	for _, v := range grid {
		stages = append(stages, ceilingStagePlan{
			Name:        fmt.Sprintf("%s_%s", prefix, formatRateTag(v)),
			Description: fmt.Sprintf(desc, v),
			Targets:     map[string]float64{className: v},
		})
	}
	return stages
}

func formatTargetsMapForLog(m map[string]float64) string {
	if len(m) == 0 {
		return "-"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%.0f/s", k, m[k]))
	}
	return strings.Join(parts, ", ")
}

func formatCeilingStageClassSummary(classes map[string]CeilingClassResult) string {
	if len(classes) == 0 {
		return "-"
	}
	keys := make([]string, 0, len(classes))
	for k := range classes {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		cl := classes[k]
		parts = append(parts, fmt.Sprintf(
			"%s offer=%.0f/s done=%.0f/s q_end=%d sat=%t",
			k,
			cl.OfferedOpsPerSec,
			cl.CompletedOpsPerSec,
			cl.QueueDepthEnd,
			cl.Saturated,
		))
	}
	return strings.Join(parts, " | ")
}

func ceilingBaselineInfo(base ceilingFastBaseline) *CeilingBaselineInfo {
	if !base.Valid || base.Throughput <= 0 {
		return nil
	}
	return &CeilingBaselineInfo{
		Throughput: base.Throughput,
		P99Us:      base.P99Us,
		Source:     base.Source,
	}
}

func runCeilingStage(
	ctx context.Context,
	db *rbi.DB[uint64, UserBench],
	maxIDPtr *uint64,
	emailSamples []string,
	plan ceilingStagePlan,
	classSpecs map[string]ceilingClassSpec,
	opts benchOptions,
	baseline ceilingFastBaseline,
	withFastRegression bool,
) (CeilingStageResult, bool) {
	startedAt := time.Now()
	log.Printf(
		"[ceiling stage=%s] start duration=%s targets={%s}",
		plan.Name,
		opts.ceilingStageDuration,
		formatTargetsMapForLog(plan.Targets),
	)
	res := CeilingStageResult{
		Name:             plan.Name,
		Description:      plan.Description,
		StartedAt:        startedAt.Format(time.RFC3339),
		TargetsOpsPerSec: make(map[string]float64, len(plan.Targets)),
		Executors:        make(map[string]int, len(plan.Targets)),
		Classes:          make(map[string]CeilingClassResult, len(plan.Targets)),
	}
	for k, v := range plan.Targets {
		res.TargetsOpsPerSec[k] = v
	}

	stageCtx, cancel := context.WithTimeout(ctx, opts.ceilingStageDuration)
	defer cancel()

	runtimes := make([]*ceilingClassRuntime, 0, len(plan.Targets))
	keys := make([]string, 0, len(plan.Targets))
	for className := range plan.Targets {
		keys = append(keys, className)
	}
	slices.Sort(keys)
	for _, className := range keys {
		target := plan.Targets[className]
		if target <= 0 {
			continue
		}
		spec, ok := classSpecs[className]
		if !ok || spec.Workers < 1 || spec.Run == nil {
			continue
		}
		rt := &ceilingClassRuntime{
			Spec:   spec,
			Target: target,
			Queue:  make(chan ceilingWorkItem, opts.ceilingQueueCap),
			M:      newCeilingClassMetrics(spec.Workers),
		}
		runtimes = append(runtimes, rt)
		res.Executors[className] = spec.Workers
	}

	if len(runtimes) == 0 {
		finishedAt := time.Now()
		res.FinishedAt = finishedAt.Format(time.RFC3339)
		res.DurationSec = finishedAt.Sub(startedAt).Seconds()
		return res, ctx.Err() != nil
	}

	var wg sync.WaitGroup
	for _, rt := range runtimes {
		for wid := 0; wid < rt.Spec.Workers; wid++ {
			wg.Add(1)
			go func(rt *ceilingClassRuntime, wid int) {
				defer wg.Done()
				runCeilingExecutor(stageCtx, rt, wid, db, maxIDPtr, emailSamples)
			}(rt, wid)
		}
		wg.Add(1)
		go func(rt *ceilingClassRuntime) {
			defer wg.Done()
			runCeilingDispatcher(stageCtx, rt, opts.ceilingDispatchQuantum)
		}(rt)
	}

	prevAt := startedAt
	prev := make(map[string]ceilingCounterSnapshot, len(runtimes))
	for _, rt := range runtimes {
		prev[rt.Spec.Name] = rt.M.snapshot()
	}

	var ticker *time.Ticker
	var tick <-chan time.Time
	if opts.reportEvery > 0 {
		ticker = time.NewTicker(opts.reportEvery)
		defer ticker.Stop()
		tick = ticker.C
	}

	for {
		select {
		case <-stageCtx.Done():
			wg.Wait()
			finishedAt := time.Now()
			res.FinishedAt = finishedAt.Format(time.RFC3339)
			res.DurationSec = finishedAt.Sub(startedAt).Seconds()
			if res.DurationSec <= 0 {
				res.DurationSec = opts.ceilingStageDuration.Seconds()
			}

			saturated := false
			stageReasons := make([]string, 0, 8)
			for _, rt := range runtimes {
				classRes := rt.M.finalize(rt.Spec.Name, rt.Spec.Role, res.DurationSec, len(rt.Queue))
				isSat, reasons := detectCeilingClassSaturation(&classRes, rt.Target, opts)
				classRes.Saturated = isSat
				classRes.Reasons = reasons
				res.Classes[rt.Spec.Name] = classRes
				if isSat {
					saturated = true
					stageReasons = append(stageReasons, fmt.Sprintf("%s:%s", rt.Spec.Name, strings.Join(reasons, "+")))
				}
			}
			res.Saturated = saturated
			res.Reasons = stageReasons

			if withFastRegression {
				if fast, ok := res.Classes[ceilingClassReadFast]; ok {
					f := fast
					res.FastReadRegression = evaluateFastReadRegression(&f, baseline, opts)
				}
			}

			log.Printf(
				"[ceiling stage=%s] done duration=%.2fs saturated=%t reasons=%s results={%s}",
				plan.Name,
				res.DurationSec,
				res.Saturated,
				emptyIfBlank(strings.Join(res.Reasons, ","), "-"),
				formatCeilingStageClassSummary(res.Classes),
			)

			return res, ctx.Err() != nil

		case <-tick:
			now := time.Now()
			deltaSec := now.Sub(prevAt).Seconds()
			if deltaSec <= 0 {
				deltaSec = 1
			}
			parts := make([]string, 0, len(runtimes))
			for _, rt := range runtimes {
				cur := rt.M.snapshot()
				last := prev[rt.Spec.Name]
				parts = append(parts, fmt.Sprintf(
					"%s tgt=%.0f offer=%.0f done=%.0f drop=%d q=%d/%d in=%d",
					rt.Spec.Name,
					rt.Target,
					float64(cur.offered-last.offered)/deltaSec,
					float64(cur.completed-last.completed)/deltaSec,
					cur.dropped-last.dropped,
					len(rt.Queue),
					cur.queueHigh,
					cur.inflight,
				))
				prev[rt.Spec.Name] = cur
			}
			log.Printf("[ceiling stage=%s] %s", plan.Name, strings.Join(parts, " | "))
			prevAt = now
		}
	}
}

func deriveFastBaselineFromSuite(suite CeilingSuiteResult, opts benchOptions) ceilingFastBaseline {
	bestThroughput := 0.0
	bestP99 := 0.0
	for _, st := range suite.Stages {
		fast, ok := st.Classes[ceilingClassReadFast]
		if !ok {
			continue
		}
		if fast.Saturated {
			continue
		}
		if fast.OfferedOps >= 100 && fast.ThroughputRatio < opts.ceilingMinThroughputRatio {
			continue
		}
		if fast.DroppedOps > 0 {
			continue
		}
		if fast.CompletedOpsPerSec > bestThroughput {
			bestThroughput = fast.CompletedOpsPerSec
			bestP99 = fast.ServiceLatencyUs.P99Us
		}
	}
	if bestThroughput <= 0 {
		for _, st := range suite.Stages {
			fast, ok := st.Classes[ceilingClassReadFast]
			if !ok {
				continue
			}
			if fast.CompletedOpsPerSec > bestThroughput {
				bestThroughput = fast.CompletedOpsPerSec
				bestP99 = fast.ServiceLatencyUs.P99Us
			}
		}
	}
	if bestThroughput <= 0 {
		return ceilingFastBaseline{}
	}
	return ceilingFastBaseline{
		Throughput: bestThroughput * opts.ceilingBaselineFraction,
		P99Us:      bestP99,
		Source:     "measured_read_fast_ceiling",
		Valid:      true,
	}
}

func runCeilingSuite(
	ctx context.Context,
	db *rbi.DB[uint64, UserBench],
	maxIDPtr *uint64,
	emailSamples []string,
	plan ceilingSuitePlan,
	classSpecs map[string]ceilingClassSpec,
	opts benchOptions,
	baseline ceilingFastBaseline,
	withFastRegression bool,
) (CeilingSuiteResult, bool) {
	out := CeilingSuiteResult{
		Name:        plan.Name,
		Description: plan.Description,
		Stages:      make([]CeilingStageResult, 0, len(plan.Stages)),
	}
	for _, stage := range plan.Stages {
		if ctx.Err() != nil {
			return out, true
		}
		st, interrupted := runCeilingStage(ctx, db, maxIDPtr, emailSamples, stage, classSpecs, opts, baseline, withFastRegression)
		out.Stages = append(out.Stages, st)
		if interrupted {
			return out, true
		}
	}
	return out, ctx.Err() != nil
}

type ceilingSweepPoint struct {
	StageName string
	Target    float64
	Completed float64
	P99       float64
	Saturated bool
}

func suiteAllClasses(suite CeilingSuiteResult) []string {
	seen := make(map[string]struct{}, 8)
	for _, st := range suite.Stages {
		for className := range st.Classes {
			seen[className] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for className := range seen {
		out = append(out, className)
	}
	slices.Sort(out)
	return out
}

func suiteSweepClasses(suite CeilingSuiteResult) []string {
	type span struct {
		min float64
		max float64
		set bool
	}
	spans := make(map[string]span, 8)
	for _, st := range suite.Stages {
		for className, target := range st.TargetsOpsPerSec {
			cur := spans[className]
			if !cur.set {
				cur.min = target
				cur.max = target
				cur.set = true
			} else {
				if target < cur.min {
					cur.min = target
				}
				if target > cur.max {
					cur.max = target
				}
			}
			spans[className] = cur
		}
	}
	out := make([]string, 0, len(spans))
	for className, cur := range spans {
		if !cur.set {
			continue
		}
		if math.Abs(cur.max-cur.min) > 1e-9 {
			out = append(out, className)
		}
	}
	slices.Sort(out)
	return out
}

func suitePrimarySweepClass(sweep []string, suite CeilingSuiteResult) string {
	if len(sweep) == 1 {
		return sweep[0]
	}
	preferred := []string{
		ceilingClassWriteUpdate,
		ceilingClassWriteInsert,
		ceilingClassReadSlow,
		ceilingClassReadFast,
	}
	for _, className := range preferred {
		if slices.Contains(sweep, className) {
			return className
		}
	}
	if len(sweep) > 1 {
		return sweep[0]
	}
	if len(suite.Stages) == 0 {
		return ""
	}
	targets := make([]string, 0, len(suite.Stages[0].TargetsOpsPerSec))
	for className := range suite.Stages[0].TargetsOpsPerSec {
		targets = append(targets, className)
	}
	slices.Sort(targets)
	if len(targets) == 0 {
		return ""
	}
	return targets[0]
}

func detectSweepKnee(points []ceilingSweepPoint) (int, string) {
	if len(points) < 2 {
		return -1, ""
	}

	baseSlope := 0.0
	for i := 1; i < len(points); i++ {
		dx := points[i].Target - points[i-1].Target
		if dx <= 0 {
			continue
		}
		dy := points[i].Completed - points[i-1].Completed
		baseSlope = dy / dx
		if baseSlope > 0 {
			break
		}
	}
	if baseSlope <= 0 {
		baseSlope = 1e-9
	}

	firstSat := -1
	for i := range points {
		if points[i].Saturated {
			firstSat = i
			break
		}
	}

	for i := 1; i < len(points); i++ {
		dx := points[i].Target - points[i-1].Target
		if dx <= 0 {
			continue
		}
		dy := points[i].Completed - points[i-1].Completed
		slope := dy / dx

		prevComp := points[i-1].Completed
		gainRatio := 0.0
		if prevComp > 0 {
			gainRatio = dy / prevComp
		}

		latJump := 1.0
		if points[i-1].P99 > 0 {
			latJump = points[i].P99 / points[i-1].P99
		}

		flat := slope <= baseSlope*0.35 && gainRatio <= 0.12
		if flat {
			return i, "throughput_flatten"
		}
		if latJump >= 1.8 && gainRatio <= 0.05 {
			return i, "latency_jump"
		}
	}

	if firstSat >= 0 {
		if firstSat > 0 {
			return firstSat - 1, "pre_saturation"
		}
		return firstSat, "saturation"
	}
	return -1, ""
}

func buildCeilingSuiteSummary(suite CeilingSuiteResult) CeilingSuiteSummary {
	summary := CeilingSuiteSummary{
		Name:                    suite.Name,
		Description:             suite.Description,
		StageCount:              len(suite.Stages),
		MaxCompletedOpsPerSec:   make(map[string]float64, 8),
		MaxSafeTargetOpsPerSec:  make(map[string]float64, 8),
		MaxSafeOfferedOpsPerSec: make(map[string]float64, 8),
		FirstClassSaturation:    make(map[string]string, 8),
	}

	summary.SweepClasses = suiteSweepClasses(suite)
	summary.PrimarySweepClass = suitePrimarySweepClass(summary.SweepClasses, suite)

	classes := suiteAllClasses(suite)
	if len(classes) == 0 {
		summary.MaxCompletedOpsPerSec = nil
		summary.MaxSafeTargetOpsPerSec = nil
		summary.FirstClassSaturation = nil
		return summary
	}

	for _, st := range suite.Stages {
		if summary.FirstSaturationStage == "" && st.Saturated {
			summary.FirstSaturationStage = st.Name
		}
		if summary.FirstFastReadRegressionStage == "" && st.FastReadRegression != nil && st.FastReadRegression.Regressed {
			summary.FirstFastReadRegressionStage = st.Name
		}

		for className, classRes := range st.Classes {
			if classRes.CompletedOpsPerSec > summary.MaxCompletedOpsPerSec[className] {
				summary.MaxCompletedOpsPerSec[className] = classRes.CompletedOpsPerSec
			}
			if !classRes.Saturated {
				if target := st.TargetsOpsPerSec[className]; target > summary.MaxSafeTargetOpsPerSec[className] {
					summary.MaxSafeTargetOpsPerSec[className] = target
				}
				if classRes.OfferedOpsPerSec > summary.MaxSafeOfferedOpsPerSec[className] {
					summary.MaxSafeOfferedOpsPerSec[className] = classRes.OfferedOpsPerSec
				}
			}
			if classRes.Saturated && summary.FirstClassSaturation[className] == "" {
				summary.FirstClassSaturation[className] = st.Name
			}
		}
	}

	if summary.PrimarySweepClass != "" {
		points := make([]ceilingSweepPoint, 0, len(suite.Stages))
		for _, st := range suite.Stages {
			target, ok := st.TargetsOpsPerSec[summary.PrimarySweepClass]
			if !ok || target <= 0 {
				continue
			}
			classRes, ok := st.Classes[summary.PrimarySweepClass]
			if !ok {
				continue
			}
			points = append(points, ceilingSweepPoint{
				StageName: st.Name,
				Target:    target,
				Completed: classRes.CompletedOpsPerSec,
				P99:       classRes.ServiceLatencyUs.P99Us,
				Saturated: classRes.Saturated,
			})
		}
		if idx, reason := detectSweepKnee(points); idx >= 0 && idx < len(points) {
			summary.KneeStage = points[idx].StageName
			summary.KneeClass = summary.PrimarySweepClass
			summary.KneeReason = reason
		}
	}

	if len(summary.MaxCompletedOpsPerSec) == 0 {
		summary.MaxCompletedOpsPerSec = nil
	}
	if len(summary.MaxSafeTargetOpsPerSec) == 0 {
		summary.MaxSafeTargetOpsPerSec = nil
	}
	if len(summary.MaxSafeOfferedOpsPerSec) == 0 {
		summary.MaxSafeOfferedOpsPerSec = nil
	}
	if len(summary.FirstClassSaturation) == 0 {
		summary.FirstClassSaturation = nil
	}
	return summary
}

func buildCeilingSummary(report *CeilingReport) *CeilingSummary {
	if report == nil || len(report.Suites) == 0 {
		return nil
	}
	s := &CeilingSummary{
		GeneratedAt:      time.Now().Format(time.RFC3339),
		FastReadBaseline: report.FastReadBaseline,
		Suites:           make([]CeilingSuiteSummary, 0, len(report.Suites)),
	}
	for _, suite := range report.Suites {
		s.Suites = append(s.Suites, buildCeilingSuiteSummary(suite))
	}
	return s
}

func formatOpsMapForLog(m map[string]float64) string {
	if len(m) == 0 {
		return "-"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%.0f", k, m[k]))
	}
	return strings.Join(parts, ",")
}

func logCeilingSummary(summary *CeilingSummary) {
	if summary == nil || len(summary.Suites) == 0 {
		return
	}
	if summary.FastReadBaseline != nil {
		log.Printf(
			"[ceiling baseline] fast_read=%.0f/s source=%s p99=%.1fus",
			summary.FastReadBaseline.Throughput,
			emptyIfBlank(summary.FastReadBaseline.Source, "-"),
			summary.FastReadBaseline.P99Us,
		)
	}
	for _, suite := range summary.Suites {
		log.Printf(
			"[ceiling summary=%s] knee=%s class=%s reason=%s first_sat=%s first_fast_reg=%s safe_target={%s} safe_offer={%s}",
			suite.Name,
			emptyIfBlank(suite.KneeStage, "-"),
			emptyIfBlank(suite.KneeClass, "-"),
			emptyIfBlank(suite.KneeReason, "-"),
			emptyIfBlank(suite.FirstSaturationStage, "-"),
			emptyIfBlank(suite.FirstFastReadRegressionStage, "-"),
			formatOpsMapForLog(suite.MaxSafeTargetOpsPerSec),
			formatOpsMapForLog(suite.MaxSafeOfferedOpsPerSec),
		)
	}
}

func emptyIfBlank(v string, alt string) string {
	if strings.TrimSpace(v) == "" {
		return alt
	}
	return v
}

func parseGridOrPanic(name, raw string) []float64 {
	grid, err := parseOpsGrid(raw)
	if err != nil {
		log.Fatalf("invalid %s: %v", name, err)
	}
	return grid
}

func buildCeilingClassSpecs(opts benchOptions) map[string]ceilingClassSpec {
	return map[string]ceilingClassSpec{
		ceilingClassReadFast: {
			Name:    ceilingClassReadFast,
			Role:    "read",
			Workers: opts.ceilingReadFastWorkers,
			Run:     runCeilingReadFast,
		},
		ceilingClassReadSlow: {
			Name:    ceilingClassReadSlow,
			Role:    "read",
			Workers: opts.ceilingReadSlowWorkers,
			Run:     runCeilingReadSlow,
		},
		ceilingClassWriteUpdate: {
			Name:    ceilingClassWriteUpdate,
			Role:    "write",
			Workers: opts.ceilingWriteUpdateWorkers,
			Run:     runCeilingWriteUpdate,
		},
		ceilingClassWriteInsert: {
			Name:    ceilingClassWriteInsert,
			Role:    "write",
			Workers: opts.ceilingWriteInsertWorkers,
			Run:     runCeilingWriteInsert,
		},
	}
}

func runCeiling(
	ctx context.Context,
	db *rbi.DB[uint64, UserBench],
	maxIDPtr *uint64,
	emailSamples []string,
	opts benchOptions,
) CeilingReport {
	readFastGrid := parseGridOrPanic("ceiling-read-fast-grid", opts.ceilingReadFastGrid)
	writeGrid := parseGridOrPanic("ceiling-write-grid", opts.ceilingWriteGrid)
	slowGrid := parseGridOrPanic("ceiling-read-slow-grid", opts.ceilingReadSlowGrid)
	lowWriteGrid := parseGridOrPanic("ceiling-low-write-grid", opts.ceilingLowWriteGrid)
	writeUnderReadGrid := mergeOpsGrids(lowWriteGrid, writeGrid)

	suiteFilter := parseSuiteFilter(opts.ceilingSuites)
	classSpecs := buildCeilingClassSpecs(opts)

	out := CeilingReport{
		StageDurationSec: opts.ceilingStageDuration.Seconds(),
		ReportEverySec:   opts.reportEvery.Seconds(),
		DispatchQuantum:  opts.ceilingDispatchQuantum.Seconds(),
		QueueCapacity:    opts.ceilingQueueCap,
		Suites:           make([]CeilingSuiteResult, 0, 8),
	}
	out.RecordsAtStart, _ = db.Count(nil)

	finalize := func(interrupted bool) CeilingReport {
		out.Interrupted = interrupted || ctx.Err() != nil
		out.RecordsAtEnd, _ = db.Count(nil)
		out.Summary = buildCeilingSummary(&out)
		logCeilingSummary(out.Summary)
		return out
	}

	baseline := ceilingFastBaseline{}

	if suiteAllowed(suiteFilter, "read_fast_ceiling") {
		plan := ceilingSuitePlan{
			Name:        "read_fast_ceiling",
			Description: "Fast read-only ceiling (single-record query path)",
			Stages:      buildSingleClassStages("read_fast", "read_fast target %.0f ops/s", ceilingClassReadFast, readFastGrid),
		}
		res, interrupted := runCeilingSuite(ctx, db, maxIDPtr, emailSamples, plan, classSpecs, opts, ceilingFastBaseline{}, false)
		out.Suites = append(out.Suites, res)
		if interrupted {
			return finalize(true)
		}
		baseline = deriveFastBaselineFromSuite(res, opts)
	}

	if !baseline.Valid {
		fallback := readFastGrid[0] * opts.ceilingBaselineFraction
		source := "configured_read_fast_grid_fallback"
		if suiteAllowed(suiteFilter, "read_fast_ceiling") {
			source = "configured_read_fast_grid_fallback_after_ceiling_miss"
		}
		baseline = ceilingFastBaseline{
			Throughput: fallback,
			Source:     source,
			Valid:      true,
		}
		if source == "configured_read_fast_grid_fallback" {
			log.Printf(
				"ceiling: fast-read baseline=%.0f ops/s source=%s (read_fast_ceiling suite not run)",
				baseline.Throughput,
				source,
			)
		} else {
			log.Printf(
				"ceiling: fast-read baseline=%.0f ops/s source=%s (read_fast_ceiling produced no usable baseline)",
				baseline.Throughput,
				source,
			)
		}
	} else {
		log.Printf(
			"ceiling: fast-read baseline=%.0f ops/s source=%s (p99=%.1fus)",
			baseline.Throughput,
			baseline.Source,
			baseline.P99Us,
		)
	}
	out.FastReadBaseline = ceilingBaselineInfo(baseline)

	if suiteAllowed(suiteFilter, "write_update_ceiling") {
		plan := ceilingSuitePlan{
			Name:        "write_update_ceiling",
			Description: "Write-only ceiling for update/patch-dominant writes",
			Stages:      buildSingleClassStages("write_update", "write_update target %.0f ops/s", ceilingClassWriteUpdate, writeGrid),
		}
		res, interrupted := runCeilingSuite(ctx, db, maxIDPtr, emailSamples, plan, classSpecs, opts, baseline, false)
		out.Suites = append(out.Suites, res)
		if interrupted {
			return finalize(true)
		}
	}

	if suiteAllowed(suiteFilter, "write_insert_ceiling") {
		plan := ceilingSuitePlan{
			Name:        "write_insert_ceiling",
			Description: "Write-only ceiling for insert writes",
			Stages:      buildSingleClassStages("write_insert", "write_insert target %.0f ops/s", ceilingClassWriteInsert, writeGrid),
		}
		res, interrupted := runCeilingSuite(ctx, db, maxIDPtr, emailSamples, plan, classSpecs, opts, baseline, false)
		out.Suites = append(out.Suites, res)
		if interrupted {
			return finalize(true)
		}
	}

	if suiteAllowed(suiteFilter, "read_fast_under_write") {
		stages := make([]ceilingStagePlan, 0, len(writeUnderReadGrid))
		for _, wops := range writeUnderReadGrid {
			stages = append(stages, ceilingStagePlan{
				Name:        fmt.Sprintf("rfuw_w_%s", formatRateTag(wops)),
				Description: fmt.Sprintf("read_fast baseline + write_update %.0f ops/s", wops),
				Targets: map[string]float64{
					ceilingClassReadFast:    baseline.Throughput,
					ceilingClassWriteUpdate: wops,
				},
			})
		}
		plan := ceilingSuitePlan{
			Name:        "read_fast_under_write",
			Description: "Interference sweep: fast reads under write pressure",
			Stages:      stages,
		}
		res, interrupted := runCeilingSuite(ctx, db, maxIDPtr, emailSamples, plan, classSpecs, opts, baseline, true)
		out.Suites = append(out.Suites, res)
		if interrupted {
			return finalize(true)
		}
	}

	if suiteAllowed(suiteFilter, "slow_vs_fast") {
		stages := make([]ceilingStagePlan, 0, len(slowGrid))
		for _, sops := range slowGrid {
			stages = append(stages, ceilingStagePlan{
				Name:        fmt.Sprintf("svf_s_%s", formatRateTag(sops)),
				Description: fmt.Sprintf("read_fast baseline + read_slow %.0f ops/s", sops),
				Targets: map[string]float64{
					ceilingClassReadFast: baseline.Throughput,
					ceilingClassReadSlow: sops,
				},
			})
		}
		plan := ceilingSuitePlan{
			Name:        "slow_vs_fast",
			Description: "Interference sweep: slow queries impact on fast reads",
			Stages:      stages,
		}
		res, interrupted := runCeilingSuite(ctx, db, maxIDPtr, emailSamples, plan, classSpecs, opts, baseline, true)
		out.Suites = append(out.Suites, res)
		if interrupted {
			return finalize(true)
		}
	}

	if suiteAllowed(suiteFilter, "mixed_fast_slow_write") {
		stages := make([]ceilingStagePlan, 0, len(writeGrid))
		for _, wops := range writeGrid {
			stages = append(stages, ceilingStagePlan{
				Name:        fmt.Sprintf("mfsw_w_%s", formatRateTag(wops)),
				Description: fmt.Sprintf("read_fast baseline + read_slow %.0f ops/s + write_update %.0f ops/s", opts.ceilingMixedSlowOps, wops),
				Targets: map[string]float64{
					ceilingClassReadFast:    baseline.Throughput,
					ceilingClassReadSlow:    opts.ceilingMixedSlowOps,
					ceilingClassWriteUpdate: wops,
				},
			})
		}
		plan := ceilingSuitePlan{
			Name:        "mixed_fast_slow_write",
			Description: "Mixed-load sweep: impact on fast reads with slow reads and writes together",
			Stages:      stages,
		}
		res, interrupted := runCeilingSuite(ctx, db, maxIDPtr, emailSamples, plan, classSpecs, opts, baseline, true)
		out.Suites = append(out.Suites, res)
		if interrupted {
			return finalize(true)
		}
	}

	if suiteAllowed(suiteFilter, "low_write_regression") {
		stages := make([]ceilingStagePlan, 0, len(lowWriteGrid))
		for _, wops := range lowWriteGrid {
			stages = append(stages, ceilingStagePlan{
				Name:        fmt.Sprintf("lwr_w_%s", formatRateTag(wops)),
				Description: fmt.Sprintf("read_fast baseline + low write_update %.0f ops/s", wops),
				Targets: map[string]float64{
					ceilingClassReadFast:    baseline.Throughput,
					ceilingClassWriteUpdate: wops,
				},
			})
		}
		plan := ceilingSuitePlan{
			Name:        "low_write_regression",
			Description: "Low-write pressure sweep to detect early read regression",
			Stages:      stages,
		}
		res, interrupted := runCeilingSuite(ctx, db, maxIDPtr, emailSamples, plan, classSpecs, opts, baseline, true)
		out.Suites = append(out.Suites, res)
		if interrupted {
			return finalize(true)
		}
	}

	return finalize(false)
}

func runCeilingReadFast(
	db *rbi.DB[uint64, UserBench],
	rng *rand.Rand,
	maxIDPtr *uint64,
	emailSamples []string,
) (string, error) {
	return runReadUserLookupItems(db, rng, atomic.LoadUint64(maxIDPtr), emailSamples)
}

func runCeilingReadSlow(
	db *rbi.DB[uint64, UserBench],
	rng *rand.Rand,
	maxIDPtr *uint64,
	emailSamples []string,
) (string, error) {
	currentMax := atomic.LoadUint64(maxIDPtr)
	r := rng.Float64()
	switch {
	case r < 0.35:
		return runReadUserCommunityFrontpageKeys(db, rng, currentMax, emailSamples)
	case r < 0.60:
		return runReadUserLeaderboardItems(db, rng, currentMax, emailSamples)
	case r < 0.80:
		return runReadUserDeepPaginationKeys(db, rng, currentMax, emailSamples)
	case r < 0.92:
		return runReadUserByNamePrefixItems(db, rng, currentMax, emailSamples)
	default:
		return runReadUserSafetyAuditKeys(db, rng, currentMax, emailSamples)
	}
}

func runCeilingWriteUpdate(
	db *rbi.DB[uint64, UserBench],
	rng *rand.Rand,
	maxIDPtr *uint64,
	emailSamples []string,
) (string, error) {
	r := rng.Float64()
	switch {
	case r < 0.55:
		return runDedicatedWriteTouchLastSeen(db, rng, maxIDPtr, emailSamples)
	case r < 0.80:
		return runDedicatedWriteVoteKarma(db, rng, maxIDPtr, emailSamples)
	case r < 0.92:
		return runDedicatedWriteProfileEdit(db, rng, maxIDPtr, emailSamples)
	default:
		return runDedicatedWriteModerationAction(db, rng, maxIDPtr, emailSamples)
	}
}

func runCeilingWriteInsert(
	db *rbi.DB[uint64, UserBench],
	rng *rand.Rand,
	maxIDPtr *uint64,
	emailSamples []string,
) (string, error) {
	return runDedicatedWriteInsert(db, rng, maxIDPtr, emailSamples)
}
