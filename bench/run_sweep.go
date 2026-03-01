package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/vapstack/rbi"
)

func runQuerySweep(
	ctx context.Context,
	db *rbi.DB[uint64, UserBench],
	maxID uint64,
	emailSamples []string,
	scenarios []readScenario,
	workersList []int,
	duration time.Duration,
	reportEvery time.Duration,
) SweepReport {
	out := SweepReport{
		Workers:        append([]int(nil), workersList...),
		DurationSec:    duration.Seconds(),
		QueryOrder:     make([]string, 0, len(scenarios)),
		Results:        make([]SweepCaseResult, 0, len(scenarios)*len(workersList)),
		RecordsAtStart: 0,
		RecordsAtEnd:   0,
	}

	startRecords, _ := db.Count(nil)
	out.RecordsAtStart = startRecords

	for _, sc := range scenarios {
		out.QueryOrder = append(out.QueryOrder, sc.Name)
	}

	for _, sc := range scenarios {
		for _, workers := range workersList {
			if ctx.Err() != nil {
				out.Interrupted = true
				out.RecordsAtEnd, _ = db.Count(nil)
				return out
			}

			log.Printf("Sweep case: query=%s workers=%d duration=%s", sc.Name, workers, duration.String())
			res := runReadScenarioCase(ctx, db, maxID, emailSamples, sc, workers, duration, reportEvery)
			out.Results = append(out.Results, res)

			if res.DurationSec <= 0 {
				continue
			}
		}
	}

	out.Interrupted = ctx.Err() != nil
	out.RecordsAtEnd, _ = db.Count(nil)
	return out
}

func runReadScenarioCase(
	ctx context.Context,
	db *rbi.DB[uint64, UserBench],
	maxID uint64,
	emailSamples []string,
	sc readScenario,
	workers int,
	duration time.Duration,
	reportEvery time.Duration,
) SweepCaseResult {
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
			seed := time.Now().UnixNano() + int64(workerID)*3571 + int64(workers)*113
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
				kind, err := sc.Run(db, rng, maxID, emailSamples)
				metrics.Record(workerID, kind, false, time.Since(start), err)
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
				"[sweep query=%s workers=%d] Ops/s: %.0f | Read(p95): %.0fµs | Err: %d",
				sc.Name,
				workers,
				opsPerSec,
				s.ReadP95Us,
				metrics.Errors(),
			)
			lastTotal = total
			lastAt = now
		}
	}

	stopWorkers()
	wg.Wait()
	finishedAt := time.Now()

	readSamples, _, opCounts := metrics.SnapshotFinal()
	caseDurationSec := finishedAt.Sub(startedAt).Seconds()
	if caseDurationSec <= 0 {
		caseDurationSec = 1
	}

	return SweepCaseResult{
		Query:              sc.Name,
		Workers:            workers,
		StartedAt:          startedAt.Format(time.RFC3339),
		FinishedAt:         finishedAt.Format(time.RFC3339),
		DurationSec:        caseDurationSec,
		TotalOps:           metrics.TotalOps(),
		Errors:             metrics.Errors(),
		OpsPerSec:          float64(metrics.TotalOps()) / caseDurationSec,
		ReadOpsSec:         float64(metrics.ReadOps()) / caseDurationSec,
		ReadLatencyUs:      summarizeLatencies(readSamples),
		OperationBreakdown: opCounts,
	}
}
