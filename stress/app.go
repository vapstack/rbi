package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vapstack/rbi"
)

var (
	setWorkersPattern   = regexp.MustCompile(`^\s*(\d+)\s+(\d+)\s*$`)
	deltaWorkersPattern = regexp.MustCompile(`^\s*(\d+)\s*([+-])\s*(\d+)\s*$`)
	setGroupPattern     = regexp.MustCompile(`^\s*([rRwWaA])\s+(\d+)\s*$`)
)

type app struct {
	handle         *DBHandle
	startMaxID     uint64
	classes        []*classController
	byID           map[int]*classController
	refreshEvery   time.Duration
	telemetryEvery time.Duration
	reportPath     string
	classFilter    []string
	queryFilter    []string
	queryBreakdown bool
	queryLatency   bool
	jitter         bool
	traces         *plannerTraceCollector

	stopCh   chan struct{}
	stopOnce sync.Once
	workerWG sync.WaitGroup

	epoch atomic.Pointer[runEpoch]

	phaseMu        sync.Mutex
	archivedPhases []phaseReport
	nextPhaseIndex int

	statusMu sync.Mutex
	status   string
}

type classController struct {
	app  *app
	desc *classDescriptor
	ctx  *WorkloadContext

	mu      sync.Mutex
	workers []*workerHandle
	nextID  int64
}

type workerHandle struct {
	id        int64
	name      string
	startedAt time.Time
	stop      chan struct{}
	metrics   atomic.Pointer[workerMetrics]
	stoppedAt atomic.Int64
}

type runEpoch struct {
	startedAt time.Time
	phaseIdx  int
	phaseKind string

	startedByCommand string

	memoryBaseline *MemorySnapshot
	snapshotBase   snapshotSample
	batchBase      batchSample
	planner        *plannerTraceEpoch

	mu              sync.Mutex
	memorySamples   []MemorySnapshot
	snapshotSamples []snapshotSample
	batchSamples    []batchSample
	lastMemory      *MemorySnapshot
	lastSnapshot    snapshotSample
	lastBatch       batchSample
	tps             map[string]*tpsState
}

type tpsState struct {
	lastCount uint64
	lastPause uint64
	lastAt    time.Time
	current   float64
	min       float64
	seen      bool
}

type viewSnapshot struct {
	StartedAt  time.Time
	CapturedAt time.Time
	Classes    []classReport
	Totals     totalsReport
	Planner    *plannerTraceReport
	Memory     *MemorySnapshot
	Snapshot   snapshotSample
	Batch      batchSample
}

type classCapture struct {
	report  classReport
	latency []float64
	paused  uint64
	workers int
}

type workerCommand struct {
	ClassID int
	Group   string
	Op      string
	Value   int
}

func newApp(handle *DBHandle, catalog []*classDescriptor, refreshEvery, telemetryEvery time.Duration, reportPath string, classFilter, queryFilter []string, queryBreakdown, queryLatency, jitter bool, traces *plannerTraceCollector) *app {
	workCtx := &WorkloadContext{
		DB:           handle.DB,
		MaxIDPtr:     &handle.MaxID,
		EmailSamples: handle.EmailSamples,
	}
	a := &app{
		handle:         handle,
		startMaxID:     handle.MaxID,
		refreshEvery:   refreshEvery,
		telemetryEvery: telemetryEvery,
		reportPath:     reportPath,
		classFilter:    append([]string(nil), classFilter...),
		queryFilter:    append([]string(nil), queryFilter...),
		queryBreakdown: queryBreakdown,
		queryLatency:   queryLatency,
		jitter:         jitter,
		traces:         traces,
		stopCh:         make(chan struct{}),
		classes:        make([]*classController, 0, len(catalog)),
		byID:           make(map[int]*classController, len(catalog)),
		nextPhaseIndex: 1,
	}
	for _, desc := range catalog {
		ctrl := &classController{
			app:  a,
			desc: desc,
			ctx:  workCtx,
		}
		a.classes = append(a.classes, ctrl)
		a.byID[desc.Info.ID] = ctrl
	}
	a.resetEpochWithMeta("initial", "")
	return a
}

func (a *app) run(ctx context.Context, renderer *renderer, input *lineReader) error {
	refreshTicker := time.NewTicker(a.refreshEvery)
	defer refreshTicker.Stop()

	telemetryTicker := time.NewTicker(a.telemetryEvery)
	defer telemetryTicker.Stop()

	now := time.Now()
	a.captureTelemetry(now)
	lastSnapshot := a.buildSnapshot(now, false)
	if err := renderer.render(lastSnapshot, input.Buffer(), a.statusText()); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case line, ok := <-input.Lines():
			if !ok {
				return nil
			}
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			if err := a.applyCommand(line); err != nil {
				a.setStatus(err.Error())
			}
			now = time.Now()
			lastSnapshot = a.buildSnapshot(now, false)
			if err := renderer.render(lastSnapshot, input.Buffer(), a.statusText()); err != nil {
				return err
			}
		case <-input.Updates():
			if err := renderer.render(lastSnapshot, input.Buffer(), a.statusText()); err != nil {
				return err
			}
		case <-refreshTicker.C:
			now = time.Now()
			lastSnapshot = a.buildSnapshot(now, false)
			if err := renderer.render(lastSnapshot, input.Buffer(), a.statusText()); err != nil {
				return err
			}
		case <-telemetryTicker.C:
			a.captureTelemetry(time.Now())
		}
	}
}

func (a *app) runHeadless(ctx context.Context) error {
	refreshTicker := time.NewTicker(a.refreshEvery)
	defer refreshTicker.Stop()

	telemetryTicker := time.NewTicker(a.telemetryEvery)
	defer telemetryTicker.Stop()

	now := time.Now()
	a.captureTelemetry(now)
	a.sampleCounts(now)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-refreshTicker.C:
			a.sampleCounts(time.Now())
		case <-telemetryTicker.C:
			a.captureTelemetry(time.Now())
		}
	}
}

func (a *app) applyInitialWorkers(initial map[string]int) {
	for _, class := range a.classes {
		if n, ok := initial[class.desc.Info.Name]; ok && n > 0 {
			class.setWorkers(n)
		}
	}
	a.resetEpochWithMeta("initial", "")
}

func (a *app) stopAllWorkers() {
	a.stopOnce.Do(func() {
		close(a.stopCh)
	})
}

func (a *app) waitWorkers() {
	a.workerWG.Wait()
}

func (a *app) buildReport(interrupted bool) stressReport {
	now := time.Now()
	epoch := a.currentEpoch()
	a.captureTelemetryForEpoch(epoch, now)
	a.sampleCountsForEpoch(epoch, now)
	snap := a.buildSnapshotForEpoch(epoch, now, true)
	phases := a.phaseReports(now)
	memoryBaseline, memoryFinal, memorySamples, snapshotBase, snapshotFinal, snapshotSamples, batchBase, batchFinal, batchSamples := epoch.copyTelemetry()
	return stressReport{
		Schema:            reportSchema,
		Timestamp:         now.Format(time.RFC3339Nano),
		DBFile:            a.handle.DBFile,
		ReportFile:        a.reportPath,
		Interrupted:       interrupted,
		StartedAt:         snap.StartedAt.Format(time.RFC3339Nano),
		FinishedAt:        snap.CapturedAt.Format(time.RFC3339Nano),
		DurationSec:       snap.CapturedAt.Sub(snap.StartedAt).Seconds(),
		RefreshEverySec:   a.refreshEvery.Seconds(),
		TelemetryEverySec: a.telemetryEvery.Seconds(),
		ClassFilter:       append([]string(nil), a.classFilter...),
		QueryFilter:       append([]string(nil), a.queryFilter...),
		RecordsAtStart:    a.handle.StartRecords,
		RecordsAtEnd:      currentRecordCount(a.handle.DB),
		MaxIDAtStart:      a.startMaxID,
		MaxIDAtEnd:        a.handle.MaxID,
		Classes:           snap.Classes,
		Totals:            snap.Totals,
		Planner:           snap.Planner,
		Phases:            phases,
		MemoryBaseline:    memoryBaseline,
		MemoryFinal:       memoryFinal,
		MemorySummary:     summarizeMemory(memorySamples, memoryFinal),
		MemorySamples:     memorySamples,
		SnapshotBaseline:  snapshotBase,
		SnapshotFinal:     snapshotFinal,
		SnapshotSamples:   snapshotSamples,
		BatchBaseline:     batchBase,
		BatchFinal:        batchFinal,
		BatchSamples:      batchSamples,
	}
}

func (a *app) sampleCounts(now time.Time) {
	a.sampleCountsForEpoch(a.currentEpoch(), now)
}

func (a *app) sampleCountsForEpoch(epoch *runEpoch, now time.Time) {
	if epoch == nil {
		return
	}
	var readCompleted uint64
	var writeCompleted uint64
	var readPaused uint64
	var writePaused uint64
	var readWorkers int
	var writeWorkers int

	for _, class := range a.classes {
		workers := class.workerCount()
		completed, _, paused := class.currentCounts()
		_, _ = epoch.observeScope("class:"+class.desc.Info.Name, completed, paused, workers, now)
		switch class.desc.Info.Role {
		case RoleRead:
			readCompleted += completed
			readPaused += paused
			readWorkers += workers
		case RoleWrite:
			writeCompleted += completed
			writePaused += paused
			writeWorkers += workers
		}
	}

	_, _ = epoch.observeScope("role:read", readCompleted, readPaused, readWorkers, now)
	_, _ = epoch.observeScope("role:write", writeCompleted, writePaused, writeWorkers, now)
	_, _ = epoch.observeScope("role:total", readCompleted+writeCompleted, readPaused+writePaused, readWorkers+writeWorkers, now)
}

func (a *app) buildSnapshot(now time.Time, includeWorkers bool) viewSnapshot {
	return a.buildSnapshotForEpoch(a.currentEpoch(), now, includeWorkers)
}

func (a *app) buildSnapshotForEpoch(epoch *runEpoch, now time.Time, includeWorkers bool) viewSnapshot {
	if epoch == nil {
		epoch = &runEpoch{startedAt: now}
	}
	planner := epoch.plannerSnapshot()
	out := viewSnapshot{
		StartedAt:  epoch.startedAt,
		CapturedAt: now,
		Classes:    make([]classReport, 0, len(a.classes)),
		Planner:    planner.rootReport(),
	}

	readLatency := make([]float64, 0, 4096)
	writeLatency := make([]float64, 0, 4096)
	totalLatency := make([]float64, 0, 8192)

	var readCompleted uint64
	var readErrors uint64
	var writeCompleted uint64
	var writeErrors uint64
	var readPaused uint64
	var writePaused uint64
	var readWorkers int
	var writeWorkers int

	for _, class := range a.classes {
		capture := class.capture(now, epoch, planner, includeWorkers)
		out.Classes = append(out.Classes, capture.report)
		totalLatency = append(totalLatency, capture.latency...)
		switch class.desc.Info.Role {
		case RoleRead:
			readCompleted += capture.report.Stats.CompletedOps
			readErrors += capture.report.Stats.Errors
			readPaused += capture.paused
			readWorkers += capture.workers
			readLatency = append(readLatency, capture.latency...)
		case RoleWrite:
			writeCompleted += capture.report.Stats.CompletedOps
			writeErrors += capture.report.Stats.Errors
			writePaused += capture.paused
			writeWorkers += capture.workers
			writeLatency = append(writeLatency, capture.latency...)
		}
	}

	out.Totals.Read = scopeReport{
		CompletedOps: readCompleted,
		Errors:       readErrors,
		AverageTPS:   averageTPS(readCompleted, epoch.startedAt, now, readPaused, readWorkers),
		Latency:      summarizeLatencies(readLatency),
	}
	out.Totals.Write = scopeReport{
		CompletedOps: writeCompleted,
		Errors:       writeErrors,
		AverageTPS:   averageTPS(writeCompleted, epoch.startedAt, now, writePaused, writeWorkers),
		Latency:      summarizeLatencies(writeLatency),
	}
	out.Totals.Total = scopeReport{
		CompletedOps: readCompleted + writeCompleted,
		Errors:       readErrors + writeErrors,
		AverageTPS:   averageTPS(readCompleted+writeCompleted, epoch.startedAt, now, readPaused+writePaused, readWorkers+writeWorkers),
		Latency:      summarizeLatencies(totalLatency),
	}
	out.Totals.Read.CurrentTPS, out.Totals.Read.MinTPS = epoch.observeScope("role:read", readCompleted, readPaused, readWorkers, now)
	out.Totals.Write.CurrentTPS, out.Totals.Write.MinTPS = epoch.observeScope("role:write", writeCompleted, writePaused, writeWorkers, now)
	out.Totals.Total.CurrentTPS, out.Totals.Total.MinTPS = epoch.observeScope("role:total", readCompleted+writeCompleted, readPaused+writePaused, readWorkers+writeWorkers, now)

	out.Memory, out.Snapshot, out.Batch = epoch.latestTelemetry()
	return out
}

func (a *app) resetEpochWithMeta(kind, startedByCommand string) {
	for _, class := range a.classes {
		class.resetMetrics()
	}
	a.epoch.Store(newRunEpoch(a.handle, a.traces, a.nextPhaseID(), kind, startedByCommand))
}

func (a *app) currentEpoch() *runEpoch {
	epoch := a.epoch.Load()
	if epoch != nil {
		return epoch
	}
	epoch = newRunEpoch(a.handle, a.traces, a.nextPhaseID(), "initial", "")
	a.epoch.Store(epoch)
	return epoch
}

func (a *app) nextPhaseID() int {
	a.phaseMu.Lock()
	defer a.phaseMu.Unlock()
	id := a.nextPhaseIndex
	a.nextPhaseIndex++
	return id
}

func (a *app) archivePhase(phase phaseReport) {
	a.phaseMu.Lock()
	defer a.phaseMu.Unlock()
	a.archivedPhases = append(a.archivedPhases, phase)
}

func (a *app) buildPhaseReport(epoch *runEpoch, now time.Time, endedByCommand string) phaseReport {
	if epoch == nil {
		epoch = &runEpoch{startedAt: now}
	}
	snap := a.buildSnapshotForEpoch(epoch, now, false)
	memoryBaseline, memoryFinal, memorySamples, snapshotBase, snapshotFinal, snapshotSamples, batchBase, batchFinal, batchSamples := epoch.copyTelemetry()
	return phaseReport{
		Index:            epoch.phaseIdx,
		Kind:             epoch.phaseKind,
		StartedAt:        snap.StartedAt.Format(time.RFC3339Nano),
		FinishedAt:       snap.CapturedAt.Format(time.RFC3339Nano),
		DurationSec:      snap.CapturedAt.Sub(snap.StartedAt).Seconds(),
		StartedByCommand: epoch.startedByCommand,
		EndedByCommand:   endedByCommand,
		Classes:          snap.Classes,
		Totals:           snap.Totals,
		Planner:          snap.Planner,
		MemoryBaseline:   memoryBaseline,
		MemoryFinal:      memoryFinal,
		MemorySummary:    summarizeMemory(memorySamples, memoryFinal),
		MemorySamples:    memorySamples,
		SnapshotBaseline: snapshotBase,
		SnapshotFinal:    snapshotFinal,
		SnapshotSamples:  snapshotSamples,
		BatchBaseline:    batchBase,
		BatchFinal:       batchFinal,
		BatchSamples:     batchSamples,
	}
}

func (a *app) finalizeCurrentPhase(endedByCommand string) {
	epoch := a.currentEpoch()
	now := time.Now()
	a.captureTelemetryForEpoch(epoch, now)
	a.sampleCountsForEpoch(epoch, now)
	a.archivePhase(a.buildPhaseReport(epoch, now, endedByCommand))
}

func (a *app) phaseReports(now time.Time) []phaseReport {
	epoch := a.currentEpoch()
	a.phaseMu.Lock()
	phases := append([]phaseReport(nil), a.archivedPhases...)
	a.phaseMu.Unlock()
	phases = append(phases, a.buildPhaseReport(epoch, now, ""))
	return phases
}

func (a *app) captureTelemetry(now time.Time) {
	a.captureTelemetryForEpoch(a.currentEpoch(), now)
}

func (a *app) captureTelemetryForEpoch(epoch *runEpoch, now time.Time) {
	if epoch == nil {
		return
	}
	memory := CaptureMemorySnapshot(a.handle)
	var snapRaw rbi.SnapshotStats
	var batchRaw rbi.AutoBatchStats
	if a.handle != nil && a.handle.DB != nil {
		snapRaw = a.handle.DB.SnapshotStats()
		batchRaw = a.handle.DB.AutoBatchStats()
	}
	epoch.recordTelemetry(
		memory,
		makeSnapshotSample(now, epoch.snapshotBase.Stats, snapRaw),
		makeBatchSample(now, epoch.batchBase.Stats, batchRaw),
	)
}

func (a *app) applyCommand(line string) error {
	cmd, err := parseWorkerCommand(line)
	if err != nil {
		return err
	}
	if cmd.Group != "" {
		updated := 0
		for _, class := range a.classes {
			if !workerGroupMatchesRole(class.desc.Info.Role, cmd.Group) {
				continue
			}
			updated++
		}
		if updated == 0 {
			return fmt.Errorf("no classes matched group %q", cmd.Group)
		}
		a.finalizeCurrentPhase(line)
		updated = 0
		for _, class := range a.classes {
			if !workerGroupMatchesRole(class.desc.Info.Role, cmd.Group) {
				continue
			}
			class.setWorkers(cmd.Value)
			updated++
		}
		a.resetEpochWithMeta("manual", line)
		a.setStatus(fmt.Sprintf("%s workers=%d for %d classes; stats reset", groupLabel(cmd.Group), cmd.Value, updated))
		return nil
	}
	class := a.byID[cmd.ClassID]
	if class == nil {
		return fmt.Errorf("unknown class %d", cmd.ClassID)
	}
	current := class.workerCount()
	var target int
	switch cmd.Op {
	case "set":
		target = cmd.Value
	case "add":
		target = current + cmd.Value
	case "sub":
		target = current - cmd.Value
		if target < 0 {
			target = 0
		}
	default:
		return fmt.Errorf("unknown op %q", cmd.Op)
	}
	a.finalizeCurrentPhase(line)
	class.setWorkers(target)
	a.resetEpochWithMeta("manual", line)
	a.setStatus(fmt.Sprintf("%s workers=%d; stats reset", class.desc.Info.Alias, target))
	return nil
}

func (a *app) setStatus(msg string) {
	a.statusMu.Lock()
	defer a.statusMu.Unlock()
	a.status = msg
}

func (a *app) statusText() string {
	a.statusMu.Lock()
	defer a.statusMu.Unlock()
	return a.status
}

func (c *classController) workerCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.workers)
}

func (c *classController) setWorkers(target int) {
	if target < 0 {
		target = 0
	}

	c.mu.Lock()
	current := len(c.workers)
	var added []*workerHandle
	var removed []*workerHandle
	switch {
	case target > current:
		added = make([]*workerHandle, 0, target-current)
		for i := current; i < target; i++ {
			c.nextID++
			handle := &workerHandle{
				id:        c.nextID,
				name:      fmt.Sprintf("%s#%d", c.desc.Info.Alias, c.nextID),
				startedAt: time.Now(),
				stop:      make(chan struct{}),
			}
			handle.metrics.Store(newWorkerMetrics(96, 48, uint64(handle.id)*0x9e3779b97f4a7c15, c.app.queryBreakdown, c.app.queryLatency))
			c.workers = append(c.workers, handle)
			added = append(added, handle)
		}
	case target < current:
		removed = append(removed, c.workers[target:]...)
		c.workers = append([]*workerHandle(nil), c.workers[:target]...)
	}
	c.mu.Unlock()

	for _, handle := range added {
		c.app.workerWG.Add(1)
		go c.app.runWorker(c, handle)
	}
	for _, handle := range removed {
		close(handle.stop)
	}
}

func (c *classController) snapshotWorkers() []*workerHandle {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*workerHandle(nil), c.workers...)
}

func (c *classController) resetMetrics() {
	workers := c.snapshotWorkers()
	totalCap := perWorkerLatencyCap(len(workers))
	queryCap := totalCap / 2
	if queryCap < 32 {
		queryCap = 32
	}
	for i, worker := range workers {
		seed := uint64(worker.id) + uint64(i+1)*0x94d049bb133111eb
		worker.metrics.Store(newWorkerMetrics(totalCap, queryCap, seed, c.app.queryBreakdown, c.app.queryLatency))
	}
}

func (c *classController) currentCounts() (uint64, uint64, uint64) {
	workers := c.snapshotWorkers()
	var completed uint64
	var errors uint64
	var paused uint64
	for _, worker := range workers {
		metrics := worker.metrics.Load()
		if metrics == nil {
			continue
		}
		workerCompleted, workerErrors := metrics.counts()
		completed += workerCompleted
		errors += workerErrors
		paused += metrics.jitterNS.Load()
	}
	return completed, errors, paused
}

func (c *classController) capture(now time.Time, epoch *runEpoch, planner plannerTraceSnapshot, includeWorkers bool) classCapture {
	workers := c.snapshotWorkers()
	report := classReport{
		ID:                c.desc.Info.ID,
		Alias:             c.desc.Info.Alias,
		Name:              c.desc.Info.Name,
		Role:              c.desc.Info.Role,
		ConfiguredWorkers: len(workers),
		ActiveWorkers:     len(workers),
		DefaultWorkers:    c.desc.Info.DefaultWorkers,
		Planner:           planner.classReport(c.desc.Info.Name),
	}

	var completed uint64
	var errors uint64
	var paused uint64
	classLatency := make([]float64, 0, len(workers)*128)
	var queryAgg map[string]*queryAggregate
	if c.app.queryBreakdown {
		queryAgg = make(map[string]*queryAggregate, len(c.desc.Info.Queries)+4)
	}

	if includeWorkers {
		report.Workers = make([]workerReport, 0, len(workers))
	}
	for _, worker := range workers {
		metrics := worker.metrics.Load()
		if metrics == nil {
			continue
		}
		metricsSnapshot := metrics.snapshot(true)
		completed += metricsSnapshot.total
		errors += metricsSnapshot.errors
		paused += metricsSnapshot.jitterNS
		classLatency = append(classLatency, metricsSnapshot.latency...)
		if c.app.queryBreakdown {
			mergeQueryAggregates(queryAgg, metricsSnapshot)
		}
		if includeWorkers {
			report.Workers = append(report.Workers, captureWorker(worker, metricsSnapshot))
		}
	}

	report.Stats = scopeReport{
		CompletedOps: completed,
		Errors:       errors,
		AverageTPS:   averageTPS(completed, epoch.startedAt, now, paused, len(workers)),
		Latency:      summarizeLatencies(classLatency),
	}
	report.Stats.CurrentTPS, report.Stats.MinTPS = epoch.observeScope("class:"+c.desc.Info.Name, completed, paused, len(workers), now)
	if c.app.queryBreakdown {
		report.Queries = buildQueryReports(c.desc, queryAgg, planner, epoch, now, paused, len(workers))
	}

	return classCapture{report: report, latency: classLatency, paused: paused, workers: len(workers)}
}

func (a *app) runWorker(class *classController, worker *workerHandle) {
	defer a.workerWG.Done()
	defer worker.stoppedAt.Store(time.Now().UnixNano())

	workerCtx := *class.ctx
	traceWorker := a.traces.newWorker(class.desc)
	if traceWorker != nil {
		traceWorker.bindCurrentGoroutine()
		defer traceWorker.close()
		workerCtx.Trace = traceWorker
	}

	seed := time.Now().UnixNano() + worker.id*811 + int64(class.desc.Info.ID)*104729
	rng := rand.New(rand.NewPCG(uint64(seed), uint64(seed)^0x9e3779b97f4a7c15))

	for {
		select {
		case <-a.stopCh:
			return
		case <-worker.stop:
			return
		default:
		}

		start := time.Now()
		kind, err := class.desc.Def.Run(&workerCtx, rng)
		latencyNS := float64(time.Since(start).Nanoseconds())
		errCount := uint64(0)
		if err != nil {
			errCount = 1
		}
		metrics := worker.metrics.Load()
		if metrics != nil {
			metrics.observe(kind, latencyNS, errCount)
		}
		if a.jitter {
			jitterStarted := time.Now()
			time.Sleep(jitterDelay(rng))
			if metrics != nil {
				metrics.addJitter(uint64(time.Since(jitterStarted)))
			}
		}
	}
}

func jitterDelay(rng *rand.Rand) time.Duration {
	return time.Duration(500+rng.IntN(501)) * time.Microsecond
}

func newRunEpoch(handle *DBHandle, traces *plannerTraceCollector, phaseIdx int, phaseKind, startedByCommand string) *runEpoch {
	now := time.Now()
	memory := CaptureMemorySnapshot(handle)
	var snapshotRaw rbi.SnapshotStats
	var batchRaw rbi.AutoBatchStats
	if handle != nil && handle.DB != nil {
		snapshotRaw = handle.DB.SnapshotStats()
		batchRaw = handle.DB.AutoBatchStats()
	}
	snapshot := snapshotSample{
		CapturedAt: now.Format(time.RFC3339Nano),
		Stats:      snapshotRaw,
	}
	batch := batchSample{
		CapturedAt: now.Format(time.RFC3339Nano),
		Stats:      batchRaw,
	}
	var planner *plannerTraceEpoch
	if traces != nil {
		planner = traces.newEpoch()
	}
	return &runEpoch{
		startedAt:        now,
		phaseIdx:         phaseIdx,
		phaseKind:        phaseKind,
		startedByCommand: startedByCommand,
		memoryBaseline:   memory,
		snapshotBase:     snapshot,
		batchBase:        batch,
		planner:          planner,
		lastMemory:       memory,
		lastSnapshot:     snapshot,
		lastBatch:        batch,
		tps:              make(map[string]*tpsState, 32),
	}
}

func (e *runEpoch) observeScope(key string, completed uint64, paused uint64, workers int, now time.Time) (float64, float64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	state := e.tps[key]
	if state == nil {
		state = &tpsState{lastAt: e.startedAt}
		e.tps[key] = state
	}
	interval := now.Sub(state.lastAt).Seconds()
	if workers > 0 && paused >= state.lastPause {
		interval -= (float64(paused-state.lastPause) / float64(workers)) / float64(time.Second)
	}
	if interval <= 0 {
		interval = 1
	}
	delta := completed
	if completed >= state.lastCount {
		delta = completed - state.lastCount
	}
	current := float64(delta) / interval
	state.current = current
	if !state.seen || current < state.min {
		state.min = current
	}
	state.lastCount = completed
	state.lastPause = paused
	state.lastAt = now
	state.seen = true
	return state.current, state.min
}

func (e *runEpoch) recordTelemetry(memory *MemorySnapshot, snapshot snapshotSample, batch batchSample) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if memory != nil {
		e.lastMemory = memory
		e.memorySamples = append(e.memorySamples, *memory)
	}
	e.lastSnapshot = snapshot
	e.lastBatch = batch
	e.snapshotSamples = append(e.snapshotSamples, snapshot)
	e.batchSamples = append(e.batchSamples, batch)
}

func (e *runEpoch) latestTelemetry() (*MemorySnapshot, snapshotSample, batchSample) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.lastMemory, e.lastSnapshot, e.lastBatch
}

func (e *runEpoch) copyTelemetry() (*MemorySnapshot, *MemorySnapshot, []MemorySnapshot, snapshotSample, snapshotSample, []snapshotSample, batchSample, batchSample, []batchSample) {
	e.mu.Lock()
	defer e.mu.Unlock()
	memorySamples := append([]MemorySnapshot(nil), e.memorySamples...)
	snapshotSamples := append([]snapshotSample(nil), e.snapshotSamples...)
	batchSamples := append([]batchSample(nil), e.batchSamples...)
	return e.memoryBaseline, e.lastMemory, memorySamples, e.snapshotBase, e.lastSnapshot, snapshotSamples, e.batchBase, e.lastBatch, batchSamples
}

func (e *runEpoch) plannerSnapshot() plannerTraceSnapshot {
	if e == nil || e.planner == nil {
		return plannerTraceSnapshot{}
	}
	return e.planner.snapshot()
}

type queryAggregate struct {
	completed uint64
	errors    uint64
	latency   []float64
}

func captureWorker(worker *workerHandle, metrics workerMetricsSnapshot) workerReport {
	report := workerReport{
		ID:        worker.id,
		Name:      worker.name,
		StartedAt: worker.startedAt.Format(time.RFC3339Nano),
	}
	if stoppedAt := worker.stoppedAt.Load(); stoppedAt > 0 {
		report.StoppedAt = time.Unix(0, stoppedAt).Format(time.RFC3339Nano)
	}
	report.Completed = metrics.total
	report.Errors = metrics.errors
	report.LastQuery = metrics.lastQuery
	report.Latency = summarizeLatencies(append([]float64(nil), metrics.latency...))
	if len(metrics.queries) > 0 {
		report.QueryCounts = make(map[string]uint64, len(metrics.queries))
	}
	for name, query := range metrics.queries {
		report.QueryCounts[name] = query.count
	}
	return report
}

func mergeQueryAggregates(dst map[string]*queryAggregate, metrics workerMetricsSnapshot) {
	for name, query := range metrics.queries {
		agg := dst[name]
		if agg == nil {
			agg = &queryAggregate{}
			dst[name] = agg
		}
		agg.completed += query.count
		agg.errors += query.errors
		agg.latency = append(agg.latency, query.latency...)
	}
}

func buildQueryReports(desc *classDescriptor, agg map[string]*queryAggregate, planner plannerTraceSnapshot, epoch *runEpoch, now time.Time, paused uint64, workers int) []queryReport {
	names := make([]string, 0, len(desc.Info.Queries)+len(agg))
	seen := make(map[string]struct{}, len(desc.Info.Queries)+len(agg))
	for _, query := range desc.Info.Queries {
		names = append(names, query.Name)
		seen[query.Name] = struct{}{}
	}
	for name := range agg {
		if _, ok := seen[name]; ok {
			continue
		}
		names = append(names, name)
	}
	slices.Sort(names)
	reordered := names[:0]
	for _, query := range desc.Info.Queries {
		reordered = append(reordered, query.Name)
	}
	for _, name := range names {
		if _, ok := desc.QueryWeight[name]; ok {
			continue
		}
		reordered = append(reordered, name)
	}

	out := make([]queryReport, 0, len(reordered))
	for _, name := range reordered {
		queryAgg := agg[name]
		stats := scopeReport{}
		if queryAgg != nil {
			stats.CompletedOps = queryAgg.completed
			stats.Errors = queryAgg.errors
			stats.AverageTPS = averageTPS(queryAgg.completed, epoch.startedAt, now, paused, workers)
			stats.Latency = summarizeLatencies(queryAgg.latency)
			stats.CurrentTPS, stats.MinTPS = epoch.observeScope("query:"+desc.Info.Name+":"+name, queryAgg.completed, paused, workers, now)
		} else {
			stats.CurrentTPS, stats.MinTPS = epoch.observeScope("query:"+desc.Info.Name+":"+name, 0, paused, workers, now)
		}
		out = append(out, queryReport{
			Name:    name,
			Weight:  desc.QueryWeight[name],
			Stats:   stats,
			Planner: planner.queryReport(desc.Info.Name, name),
		})
	}
	return out
}

func parseWorkerCommand(line string) (workerCommand, error) {
	if matches := setGroupPattern.FindStringSubmatch(line); len(matches) == 3 {
		value, ok := parsePositiveInt(matches[2])
		if !ok {
			return workerCommand{}, fmt.Errorf("invalid worker count")
		}
		group := strings.ToLower(matches[1])
		return workerCommand{Group: group, Op: "set", Value: value}, nil
	}
	if matches := setWorkersPattern.FindStringSubmatch(line); len(matches) == 3 {
		classID, ok := parsePositiveInt(matches[1])
		if !ok || classID < 1 {
			return workerCommand{}, fmt.Errorf("invalid class id")
		}
		value, ok := parsePositiveInt(matches[2])
		if !ok {
			return workerCommand{}, fmt.Errorf("invalid worker count")
		}
		return workerCommand{ClassID: classID, Op: "set", Value: value}, nil
	}
	if matches := deltaWorkersPattern.FindStringSubmatch(line); len(matches) == 4 {
		classID, ok := parsePositiveInt(matches[1])
		if !ok || classID < 1 {
			return workerCommand{}, fmt.Errorf("invalid class id")
		}
		value, ok := parsePositiveInt(matches[3])
		if !ok {
			return workerCommand{}, fmt.Errorf("invalid worker delta")
		}
		op := "add"
		if matches[2] == "-" {
			op = "sub"
		}
		return workerCommand{ClassID: classID, Op: op, Value: value}, nil
	}
	return workerCommand{}, fmt.Errorf("command format: '<#> <count>', '<#>+<n>', '<#>-<n>', 'r <count>', 'w <count>' or 'a <count>'")
}

func parsePositiveInt(value string) (int, bool) {
	var n int
	if value == "" {
		return 0, false
	}
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return 0, false
		}
		n = n*10 + int(ch-'0')
	}
	return n, true
}

func groupLabel(group string) string {
	switch group {
	case "r":
		return "read"
	case "w":
		return "write"
	case "a":
		return "all"
	default:
		return group
	}
}

func workerGroupMatchesRole(role, group string) bool {
	switch group {
	case "r":
		return role == RoleRead
	case "w":
		return role == RoleWrite
	case "a":
		return true
	default:
		return false
	}
}

func averageTPS(completed uint64, startedAt, now time.Time, paused uint64, workers int) float64 {
	seconds := now.Sub(startedAt).Seconds()
	if workers > 0 {
		seconds -= (float64(paused) / float64(workers)) / float64(time.Second)
	}
	if seconds <= 0 {
		return 0
	}
	return float64(completed) / seconds
}

func currentRecordCount(db *rbi.DB[uint64, UserBench]) uint64 {
	count, err := db.Count(nil)
	if err != nil {
		return 0
	}
	return count
}

func makeSnapshotSample(now time.Time, baseline, current rbi.SnapshotStats) snapshotSample {
	return snapshotSample{
		CapturedAt: now.Format(time.RFC3339Nano),
		Stats:      current,
	}
}

func makeBatchSample(now time.Time, baseline, current rbi.AutoBatchStats) batchSample {
	return batchSample{
		CapturedAt: now.Format(time.RFC3339Nano),
		Stats:      current,
		Delta: batchDelta{
			Submitted:           deltaUint64(current.Submitted, baseline.Submitted),
			Enqueued:            deltaUint64(current.Enqueued, baseline.Enqueued),
			Dequeued:            deltaUint64(current.Dequeued, baseline.Dequeued),
			QueueHighWater:      deltaUint64(current.QueueHighWater, baseline.QueueHighWater),
			ExecutedBatches:     deltaUint64(current.ExecutedBatches, baseline.ExecutedBatches),
			MultiRequestBatches: deltaUint64(current.MultiRequestBatches, baseline.MultiRequestBatches),
			MultiRequestOps:     deltaUint64(current.MultiRequestOps, baseline.MultiRequestOps),
			BatchSize1:          deltaUint64(current.BatchSize1, baseline.BatchSize1),
			BatchSize2To4:       deltaUint64(current.BatchSize2To4, baseline.BatchSize2To4),
			BatchSize5To8:       deltaUint64(current.BatchSize5To8, baseline.BatchSize5To8),
			BatchSize9Plus:      deltaUint64(current.BatchSize9Plus, baseline.BatchSize9Plus),
			MaxBatchSeen:        deltaUint64(current.MaxBatchSeen, baseline.MaxBatchSeen),
			CallbackOps:         deltaUint64(current.CallbackOps, baseline.CallbackOps),
			CoalescedSetDelete:  deltaUint64(current.CoalescedSetDelete, baseline.CoalescedSetDelete),
			CoalesceWaits:       deltaUint64(current.CoalesceWaits, baseline.CoalesceWaits),
			CoalesceWaitTime:    current.CoalesceWaitTime - baseline.CoalesceWaitTime,
			QueueWaitTime:       current.QueueWaitTime - baseline.QueueWaitTime,
			ExecuteTime:         current.ExecuteTime - baseline.ExecuteTime,
			FallbackClosed:      deltaUint64(current.FallbackClosed, baseline.FallbackClosed),
			UniqueRejected:      deltaUint64(current.UniqueRejected, baseline.UniqueRejected),
			TxBeginErrors:       deltaUint64(current.TxBeginErrors, baseline.TxBeginErrors),
			TxOpErrors:          deltaUint64(current.TxOpErrors, baseline.TxOpErrors),
			TxCommitErrors:      deltaUint64(current.TxCommitErrors, baseline.TxCommitErrors),
			CallbackErrors:      deltaUint64(current.CallbackErrors, baseline.CallbackErrors),
		},
	}
}

func deltaUint64(current, baseline uint64) uint64 {
	if current < baseline {
		return 0
	}
	return current - baseline
}
