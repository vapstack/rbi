package main

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vapstack/rbi"
	"github.com/vapstack/rbi/bench/extbench"
)

func buildDedicatedWorkerRunners(
	scenarios []readScenario,
	includeRead bool,
	includeWrite bool,
) []dedicatedWorkerRunner {
	workers := make([]dedicatedWorkerRunner, 0, len(scenarios)+12)
	if includeRead {
		for _, sc := range scenarios {
			sc := sc
			weight := sc.Weight
			if weight < 1 {
				weight = 1
			}
			for i := 0; i < weight; i++ {
				workerName := sc.Name
				if weight > 1 {
					workerName = fmt.Sprintf("%s#%d", sc.Name, i+1)
				}
				workers = append(workers, dedicatedWorkerRunner{
					Name: workerName,
					Role: "read",
					Run: func(
						db *rbi.DB[uint64, extbench.UserBench],
						rng *rand.Rand,
						maxIDPtr *uint64,
						emailSamples []string,
					) (string, error) {
						return sc.Run(db, rng, atomic.LoadUint64(maxIDPtr), emailSamples)
					},
				})
			}
		}
	}
	if includeWrite {
		type writeSpec struct {
			name   string
			weight int
			run    func(
				db *rbi.DB[uint64, extbench.UserBench],
				rng *rand.Rand,
				maxIDPtr *uint64,
				emailSamples []string,
			) (string, error)
		}
		writeSpecs := []writeSpec{
			{name: "write_touch_last_seen", weight: 3, run: runDedicatedWriteTouchLastSeen},
			{name: "write_vote_karma", weight: 2, run: runDedicatedWriteVoteKarma},
			{name: "write_profile_edit", weight: 1, run: runDedicatedWriteProfileEdit},
			{name: "write_moderation_action", weight: 1, run: runDedicatedWriteModerationAction},
			{name: "write_insert_signup", weight: 1, run: runDedicatedWriteInsert},
			{name: "write_delete_account", weight: 1, run: runDedicatedWriteDelete},
		}
		for _, spec := range writeSpecs {
			weight := spec.weight
			if weight < 1 {
				weight = 1
			}
			for i := 0; i < weight; i++ {
				workerName := spec.name
				if weight > 1 {
					workerName = fmt.Sprintf("%s#%d", spec.name, i+1)
				}
				workers = append(workers, dedicatedWorkerRunner{
					Name: workerName,
					Role: "write",
					Run:  spec.run,
				})
			}
		}
	}
	return workers
}

func runDedicatedMix(
	ctx context.Context,
	db *rbi.DB[uint64, extbench.UserBench],
	maxIDPtr *uint64,
	emailSamples []string,
	workers []dedicatedWorkerRunner,
	writeGateGrid []int,
	duration time.Duration,
	reportEvery time.Duration,
) DedicatedMixReport {
	specs := make([]DedicatedWorkerSpec, 0, len(workers))
	for i := range workers {
		specs = append(specs, DedicatedWorkerSpec{
			WorkerID: i,
			Name:     workers[i].Name,
			Role:     workers[i].Role,
		})
	}

	out := DedicatedMixReport{
		DurationSec:         duration.Seconds(),
		ReportEvery:         reportEvery.Seconds(),
		IncludeReadWorkers:  hasDedicatedWorkersByRole(workers, "read"),
		IncludeWriteWorkers: hasDedicatedWorkersByRole(workers, "write"),
		WriteGateGrid:       append([]int(nil), writeGateGrid...),
		WorkerSpecs:         specs,
		Results:             make([]DedicatedMixCaseResult, 0, len(writeGateGrid)),
	}
	out.RecordsAtStart, _ = db.Count(nil)

	for _, gate := range writeGateGrid {
		if ctx.Err() != nil {
			out.Interrupted = true
			break
		}
		if gate <= 0 {
			gate = 1
		}
		log.Printf(
			"Dedicated case: workers=%d write_gate_every_n=%d duration=%s",
			len(workers),
			gate,
			duration.String(),
		)
		// log.Println("cooldown (5s)...")
		// <-time.After(5 * time.Second)
		res := runDedicatedMixCase(
			ctx,
			db,
			maxIDPtr,
			emailSamples,
			workers,
			gate,
			duration,
			reportEvery,
		)
		out.Results = append(out.Results, res)
	}

	out.Interrupted = out.Interrupted || ctx.Err() != nil
	out.RecordsAtEnd, _ = db.Count(nil)
	return out
}

func hasDedicatedWorkersByRole(workers []dedicatedWorkerRunner, role string) bool {
	for i := range workers {
		if workers[i].Role == role {
			return true
		}
	}
	return false
}

func runDedicatedMixCase(
	ctx context.Context,
	db *rbi.DB[uint64, extbench.UserBench],
	maxIDPtr *uint64,
	emailSamples []string,
	workers []dedicatedWorkerRunner,
	writeGateEveryN int,
	duration time.Duration,
	reportEvery time.Duration,
) DedicatedMixCaseResult {
	if writeGateEveryN <= 0 {
		writeGateEveryN = 1
	}

	metrics := NewRunMetrics(MaxLatencySampleSize, len(workers))
	startedAt := time.Now()
	caseCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	stop := make(chan struct{})
	var stopOnce sync.Once
	stopWorkers := func() { stopOnce.Do(func() { close(stop) }) }

	hasReadWorkers := hasDedicatedWorkersByRole(workers, "read")
	hasWriteWorkers := hasDedicatedWorkersByRole(workers, "write")
	writeGateEnabled := writeGateEveryN > 1 && hasReadWorkers && hasWriteWorkers
	writeWorkerCount := 0
	for i := range workers {
		if workers[i].Role == "write" {
			writeWorkerCount++
		}
	}

	var readGateTick atomic.Uint64
	var writePermits atomic.Int64
	permitNotify := make(chan struct{}, 1)
	writePermitCap := int64(1)
	if writeWorkerCount > 0 {
		// Keep only a small backlog of write permits so dedicated gate stays
		// close to target read/write ratio throughout the run.
		writePermitCap = int64(writeWorkerCount * 2)
	}

	grantWritePermit := func() {
		for {
			cur := writePermits.Load()
			if cur >= writePermitCap {
				return
			}
			if writePermits.CompareAndSwap(cur, cur+1) {
				select {
				case permitNotify <- struct{}{}:
				default:
				}
				return
			}
		}
	}
	tryAcquireWritePermit := func() bool {
		for {
			cur := writePermits.Load()
			if cur <= 0 {
				return false
			}
			if writePermits.CompareAndSwap(cur, cur-1) {
				return true
			}
		}
	}

	var wg sync.WaitGroup
	for i := range workers {
		workerID := i
		w := workers[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			seed := time.Now().UnixNano() + int64(workerID)*811 + int64(writeGateEveryN)*9973
			rng := newRand(seed)

			for {
				select {
				case <-stop:
					return
				case <-caseCtx.Done():
					return
				default:
				}

				if w.Role == "write" && writeGateEnabled {
					for {
						if tryAcquireWritePermit() {
							if writePermits.Load() > 0 {
								select {
								case permitNotify <- struct{}{}:
								default:
								}
							}
							break
						}
						select {
						case <-stop:
							return
						case <-caseCtx.Done():
							return
						case <-permitNotify:
						}
					}
				}

				start := time.Now()
				kind, err := w.Run(db, rng, maxIDPtr, emailSamples)
				metrics.Record(workerID, kind, w.Role == "write", time.Since(start), err)

				if writeGateEnabled && w.Role == "read" {
					tick := readGateTick.Add(1)
					if tick%uint64(writeGateEveryN) == 0 {
						grantWritePermit()
					}
				}
			}
		}()
	}

	var ticker *time.Ticker
	var tick <-chan time.Time
	if reportEvery > 0 {
		ticker = time.NewTicker(reportEvery)
		defer ticker.Stop()
		tick = ticker.C
	}

	lastAt := startedAt
	lastTotal := uint64(0)

loop:
	for {
		select {
		case <-caseCtx.Done():
			break loop
		case <-tick:
			now := time.Now()
			total := metrics.TotalOps()
			intervalSec := now.Sub(lastAt).Seconds()
			if intervalSec <= 0 {
				intervalSec = 1
			}
			deltaTotal := total - lastTotal
			totalOpsPerSec := float64(deltaTotal) / intervalSec

			log.Printf(
				"[dedicated gate=%d workers=%d] Ops/s: %.0f | Err: %d",
				writeGateEveryN,
				len(workers),
				totalOpsPerSec,
				metrics.Errors(),
			)
			if EnableSnapshotDiagnosticsLogs {
				d := db.SnapshotDiagnostics()
				log.Printf(
					"[snapshot gate=%d] tx=%d depth(idx=%d len=%d) delta(idx=%d len=%d) universe(add=%d drop=%d) registry(size=%d order=%d head=%d) compactor(queue=%d req=%d run=%d try=%d ok=%d cas_fail=%d lock_miss=%d no_change=%d)",
					writeGateEveryN,
					d.TxID,
					d.IndexLayerDepth,
					d.LenLayerDepth,
					d.IndexDeltaFields,
					d.LenDeltaFields,
					d.UniverseAddCard,
					d.UniverseDropCard,
					d.RegistrySize,
					d.RegistryOrderLen,
					d.RegistryHead,
					d.CompactorQueueLen,
					d.CompactorRequested,
					d.CompactorRuns,
					d.CompactorAttempts,
					d.CompactorSucceeded,
					d.CompactorCASFail,
					d.CompactorLockMiss,
					d.CompactorNoChange,
				)
			}

			lastTotal = total
			lastAt = now
		}
	}

	stopWorkers()
	wg.Wait()
	finishedAt := time.Now()

	readSamples, writeSamples, opCounts := metrics.SnapshotFinal()
	caseDurationSec := finishedAt.Sub(startedAt).Seconds()
	if caseDurationSec <= 0 {
		caseDurationSec = 1
	}

	workerResults := make([]DedicatedWorkerResult, 0, len(workers))
	for i := range workers {
		w := &metrics.workers[i]
		var data []float64
		if workers[i].Role == "write" {
			data = append([]float64(nil), w.write.values...)
		} else {
			data = append([]float64(nil), w.read.values...)
		}
		lastOp := ""
		lastOpCount := uint64(0)
		for k, v := range w.opCounts {
			if v > lastOpCount {
				lastOp = k
				lastOpCount = v
			}
		}
		totalOps := w.totalOps.Load()
		workerResults = append(workerResults, DedicatedWorkerResult{
			WorkerID:      i,
			Name:          workers[i].Name,
			Role:          workers[i].Role,
			TotalOps:      totalOps,
			ReadOps:       w.readOps.Load(),
			WriteOps:      w.writeOps.Load(),
			Errors:        w.errors.Load(),
			OpsPerSec:     float64(totalOps) / caseDurationSec,
			LatencyUs:     summarizeLatencies(data),
			LastOperation: lastOp,
		})
	}

	return DedicatedMixCaseResult{
		WriteGateEveryN:    writeGateEveryN,
		StartedAt:          startedAt.Format(time.RFC3339),
		FinishedAt:         finishedAt.Format(time.RFC3339),
		DurationSec:        caseDurationSec,
		Workers:            len(workers),
		TotalOps:           metrics.TotalOps(),
		ReadOps:            metrics.ReadOps(),
		WriteOps:           metrics.WriteOps(),
		Errors:             metrics.Errors(),
		OpsPerSec:          float64(metrics.TotalOps()) / caseDurationSec,
		ReadOpsPerSec:      float64(metrics.ReadOps()) / caseDurationSec,
		WriteOpsPerSec:     float64(metrics.WriteOps()) / caseDurationSec,
		ReadLatencyUs:      summarizeLatencies(readSamples),
		WriteLatencyUs:     summarizeLatencies(writeSamples),
		OperationBreakdown: opCounts,
		WorkerResults:      workerResults,
	}
}
