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
						db *rbi.DB[uint64, UserBench],
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
				db *rbi.DB[uint64, UserBench],
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

func runStress(
	ctx context.Context,
	db *rbi.DB[uint64, UserBench],
	maxIDPtr *uint64,
	emailSamples []string,
	workers []dedicatedWorkerRunner,
	writeGateGrid []int,
	duration time.Duration,
	reportEvery time.Duration,
) StressReport {
	specs := make([]StressWorkerSpec, 0, len(workers))
	for i := range workers {
		specs = append(specs, StressWorkerSpec{
			WorkerID: i,
			Name:     workers[i].Name,
			Role:     workers[i].Role,
		})
	}

	out := StressReport{
		DurationSec:         duration.Seconds(),
		ReportEvery:         reportEvery.Seconds(),
		IncludeReadWorkers:  hasDedicatedWorkersByRole(workers, "read"),
		IncludeWriteWorkers: hasDedicatedWorkersByRole(workers, "write"),
		WriteGateGrid:       append([]int(nil), writeGateGrid...),
		WorkerSpecs:         specs,
		Results:             make([]StressCaseResult, 0, len(writeGateGrid)),
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
			"Stress case: workers=%d write_gate_every_n=%d duration=%s",
			len(workers),
			gate,
			duration.String(),
		)
		res := runStressCase(
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
	return countDedicatedWorkersByRole(workers, role) > 0
}

func countDedicatedWorkersByRole(workers []dedicatedWorkerRunner, role string) int {
	n := 0
	for i := range workers {
		if workers[i].Role == role {
			n++
		}
	}
	return n
}

func runStressCase(
	ctx context.Context,
	db *rbi.DB[uint64, UserBench],
	maxIDPtr *uint64,
	emailSamples []string,
	workers []dedicatedWorkerRunner,
	writeGateEveryN int,
	duration time.Duration,
	reportEvery time.Duration,
) StressCaseResult {
	if writeGateEveryN <= 0 {
		writeGateEveryN = 1
	}

	metrics := NewRunMetrics(MaxLatencySampleSize, len(workers))
	startedAt := time.Now()
	caseCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	bStats := db.AutoBatchStats()

	stop := make(chan struct{})
	var stopOnce sync.Once
	stopWorkers := func() { stopOnce.Do(func() { close(stop) }) }

	hasReadWorkers := hasDedicatedWorkersByRole(workers, "read")
	hasWriteWorkers := hasDedicatedWorkersByRole(workers, "write")
	writeGateEnabled := writeGateEveryN > 1 && hasReadWorkers && hasWriteWorkers
	writeWorkerCount := countDedicatedWorkersByRole(workers, "write")

	var readGateTick atomic.Uint64
	var writePermits atomic.Int64
	permitNotify := make(chan struct{}, 1)
	writePermitCap := int64(1)
	if writeWorkerCount > 0 {
		// Keep permit backlog small so effective ratio stays close to write_gate_every_n.
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

				startService := time.Now()
				kind, err := w.Run(db, rng, maxIDPtr, emailSamples)
				service := time.Since(startService)
				if w.Role == "write" {
					metrics.RecordWriteDetailed(workerID, kind, 0, service, service, err)
				} else {
					metrics.Record(workerID, kind, false, service, err)
				}
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
	lastRead := uint64(0)
	lastWrite := uint64(0)
	lastBatch := bStats

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
			readTotal := metrics.ReadOps()
			writeTotal := metrics.WriteOps()
			readOpsPerSec := float64(readTotal-lastRead) / intervalSec
			writeOpsPerSec := float64(writeTotal-lastWrite) / intervalSec

			wcNow := db.AutoBatchStats()
			txDelta := uint64(0)
			if wcNow.ExecutedBatches >= lastBatch.ExecutedBatches {
				txDelta = wcNow.ExecutedBatches - lastBatch.ExecutedBatches
			}
			txOpsPerSec := float64(txDelta) / intervalSec

			log.Printf(
				"[stress gate=%d workers=%d] Ops/s: %.0f (read=%.0f write=%.0f tx=%.0f) err=%d",
				writeGateEveryN,
				len(workers),
				totalOpsPerSec,
				readOpsPerSec,
				writeOpsPerSec,
				txOpsPerSec,
				metrics.Errors(),
			)
			if EnableSnapshotDiagnosticsLogs {
				d := db.SnapshotStats()
				log.Printf(
					"[snapshot gate=%v] tx=%v depth(idx=%v len=%v) delta(idx=%v len=%v) universe(add=%v drop=%v) registry(size=%v order=%v head=%v) compactor(queue=%v req=%v run=%v try=%v ok=%v lock_miss=%v no_change=%v)",
					writeGateEveryN,
					d.TxID,
					d.IndexLayerDepth,
					d.LenLayerDepth,
					d.IndexDeltaFields,
					d.LenDeltaFields,
					d.UniverseAddCard,
					d.UniverseRemCard,
					d.RegistrySize,
					d.RegistryOrderLen,
					d.RegistryHead,
					d.CompactorQueueLen,
					d.CompactorRequested,
					d.CompactorRuns,
					d.CompactorAttempts,
					d.CompactorSucceeded,
					d.CompactorLockMiss,
					d.CompactorNoChange,
				)
			}

			lastTotal = total
			lastRead = readTotal
			lastWrite = writeTotal
			lastBatch = wcNow
			lastAt = now
		}
	}

	stopWorkers()
	wg.Wait()
	finishedAt := time.Now()
	wcEnd := db.AutoBatchStats()

	readSamples, writeSamples, writeQueueSamples, writeServiceSamples, writeEndToEndSamples, opCounts := metrics.SnapshotFinalDetailed()
	caseDurationSec := finishedAt.Sub(startedAt).Seconds()
	if caseDurationSec <= 0 {
		caseDurationSec = 1
	}

	delta := func(after, before uint64) uint64 {
		if after < before {
			return 0
		}
		return after - before
	}
	writeTxCommits := delta(wcEnd.ExecutedBatches, bStats.ExecutedBatches)
	batchedOps := delta(wcEnd.MultiRequestOps, bStats.MultiRequestOps)
	writeDequeued := delta(wcEnd.Dequeued, bStats.Dequeued)
	writeAvgBatch := 0.0
	if writeTxCommits > 0 {
		writeAvgBatch = float64(writeDequeued) / float64(writeTxCommits)
	}

	workerResults := make([]StressWorkerResult, 0, len(workers))
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
		workerResults = append(workerResults, StressWorkerResult{
			WorkerID:               i,
			Name:                   workers[i].Name,
			Role:                   workers[i].Role,
			TotalOps:               totalOps,
			ReadOps:                w.readOps.Load(),
			WriteOps:               w.writeOps.Load(),
			Errors:                 w.errors.Load(),
			OpsPerSec:              float64(totalOps) / caseDurationSec,
			LatencyUs:              summarizeLatencies(data),
			LastOperation:          lastOp,
			WriteQueueLatencyUs:    summarizeLatencies(append([]float64(nil), w.writeQueue.values...)),
			WriteServiceLatencyUs:  summarizeLatencies(append([]float64(nil), w.writeService.values...)),
			WriteEndToEndLatencyUs: summarizeLatencies(append([]float64(nil), w.writeEndToEnd.values...)),
		})
	}

	return StressCaseResult{
		WriteGateEveryN:        writeGateEveryN,
		StartedAt:              startedAt.Format(time.RFC3339),
		FinishedAt:             finishedAt.Format(time.RFC3339),
		DurationSec:            caseDurationSec,
		Workers:                len(workers),
		TotalOps:               metrics.TotalOps(),
		ReadOps:                metrics.ReadOps(),
		WriteOps:               metrics.WriteOps(),
		Errors:                 metrics.Errors(),
		OpsPerSec:              float64(metrics.TotalOps()) / caseDurationSec,
		ReadOpsPerSec:          float64(metrics.ReadOps()) / caseDurationSec,
		WriteOpsPerSec:         float64(metrics.WriteOps()) / caseDurationSec,
		WriteTxPerSec:          float64(writeTxCommits) / caseDurationSec,
		ReadLatencyUs:          summarizeLatencies(readSamples),
		WriteLatencyUs:         summarizeLatencies(writeSamples),
		WriteQueueLatencyUs:    summarizeLatencies(writeQueueSamples),
		WriteServiceLatencyUs:  summarizeLatencies(writeServiceSamples),
		WriteEndToEndLatencyUs: summarizeLatencies(writeEndToEndSamples),
		OperationBreakdown:     opCounts,
		WorkerResults:          workerResults,
		WriteTxCommits:         writeTxCommits,
		BatchedOps:             batchedOps,
		WriteAvgBatchSize:      writeAvgBatch,
		BatchSubmitted:         delta(wcEnd.Submitted, bStats.Submitted),
		BatchEnqueued:          delta(wcEnd.Enqueued, bStats.Enqueued),
		BatchDequeued:          writeDequeued,
		BatchQueueHigh:         wcEnd.QueueHighWater,
		BatchCoalesceOps:       delta(wcEnd.CoalesceWaits, bStats.CoalesceWaits),
	}
}
