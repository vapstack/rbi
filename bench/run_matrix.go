package main

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vapstack/rbi"
)

func runMatrixCase(
	ctx context.Context,
	db *rbi.DB[uint64, UserBench],
	maxIDPtr *uint64,
	emailSamples []string,
	profile RWProfile,
	workers int,
	duration time.Duration,
	reportEvery time.Duration,
) CaseResult {
	metrics := NewRunMetrics(MaxLatencySampleSize, workers)
	startedAt := time.Now()

	caseCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	stop := make(chan struct{})
	var stopOnce sync.Once
	stopWorkers := func() { stopOnce.Do(func() { close(stop) }) }

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			seed := time.Now().UnixNano() + int64(workerID)*7919 + int64(workers)*131
			rng := newRand(seed)

			for {
				select {
				case <-stop:
					return
				case <-caseCtx.Done():
					return
				default:
				}

				start := time.Now()
				opRoll := rng.Float64()

				if opRoll < profile.ReadRatio {
					kind, err := runRandomReadScenario(db, rng, atomic.LoadUint64(maxIDPtr), emailSamples)
					metrics.Record(workerID, kind, false, time.Since(start), err)
				} else {
					kind, err := runRandomWriteScenario(db, rng, maxIDPtr)
					metrics.Record(workerID, kind, true, time.Since(start), err)
				}
			}
		}(i)
	}

	var ticker *time.Ticker
	var tick <-chan time.Time
	if reportEvery > 0 {
		ticker = time.NewTicker(reportEvery)
		defer ticker.Stop()
		tick = ticker.C
	}

	lastAt := startedAt
	var lastTotal uint64

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
			delta := total - lastTotal
			opsPerSec := float64(delta) / intervalSec
			s := metrics.TakeIntervalSnapshot()
			log.Printf(
				"[profile=%s workers=%d] Ops/s: %.0f | Read(p95): %.0fµs | Write(p95): %.0fµs | Err: %d",
				profile.Name,
				workers,
				opsPerSec,
				s.ReadP95Us,
				s.WriteP95Us,
				metrics.Errors(),
			)
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
	recordsAfter, _ := db.Count(nil)

	return CaseResult{
		Profile:     profile.Name,
		Workers:     workers,
		ReadRatio:   profile.ReadRatio,
		WriteRatio:  profile.WriteRatio,
		StartedAt:   startedAt.Format(time.RFC3339),
		FinishedAt:  finishedAt.Format(time.RFC3339),
		DurationSec: caseDurationSec,

		TotalOps:     metrics.TotalOps(),
		ReadOps:      metrics.ReadOps(),
		WriteOps:     metrics.WriteOps(),
		Errors:       metrics.Errors(),
		RecordsAfter: recordsAfter,

		OpsPerSec:      float64(metrics.TotalOps()) / caseDurationSec,
		ReadOpsPerSec:  float64(metrics.ReadOps()) / caseDurationSec,
		WriteOpsPerSec: float64(metrics.WriteOps()) / caseDurationSec,

		ReadLatencyUs:      summarizeLatencies(readSamples),
		WriteLatencyUs:     summarizeLatencies(writeSamples),
		OperationBreakdown: opCounts,
		Interrupted:        ctx.Err() != nil,
	}
}
