package main

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"github.com/vapstack/rbi"
)

func buildSweepModeReport(
	ctx context.Context,
	db *rbi.DB[uint64, UserBench],
	maxID uint64,
	startCount uint64,
	emailSamples []string,
	opts benchOptions,
) BenchmarkReport {
	scenarios := allReadScenarios()
	log.Printf(
		"Starting sweep mode: %d queries, workers=%v, duration=%s",
		len(scenarios),
		SweepWorkers,
		opts.sweepDuration.String(),
	)

	sweep := runQuerySweep(
		ctx,
		db,
		maxID,
		emailSamples,
		scenarios,
		SweepWorkers,
		opts.sweepDuration,
		opts.reportEvery,
	)

	return BenchmarkReport{
		Timestamp:       time.Now().Format(time.RFC3339),
		Mode:            "sweep",
		DBFile:          DBFilename,
		OutFile:         opts.outFile,
		MaxID:           maxID,
		CaseDurationSec: opts.sweepDuration.Seconds(),
		ReportEverySec:  opts.reportEvery.Seconds(),
		StartRecords:    startCount,
		FinalRecords:    sweep.RecordsAtEnd,
		Workers:         SweepWorkers,
		Sweep:           &sweep,
		Interrupted:     sweep.Interrupted,
	}
}

func buildStressModeReport(
	ctx context.Context,
	db *rbi.DB[uint64, UserBench],
	maxIDPtr *uint64,
	startCount uint64,
	emailSamples []string,
	opts benchOptions,
) BenchmarkReport {
	includeRead := !opts.stressNoRead
	includeWrite := !opts.stressNoWrite

	workers := buildDedicatedWorkerRunners(allReadScenarios(), includeRead, includeWrite)
	if len(workers) == 0 {
		log.Fatal("no workers configured for stress")
	}

	log.Printf(
		"Starting stress mode: workers=%d (read=%v write=%v), write_gates=%v, duration=%s",
		len(workers),
		includeRead,
		includeWrite,
		DedicatedWriteGateGrid,
		opts.caseDuration.String(),
	)

	stress := runStress(
		ctx,
		db,
		maxIDPtr,
		emailSamples,
		workers,
		DedicatedWriteGateGrid,
		opts.caseDuration,
		opts.reportEvery,
	)

	return BenchmarkReport{
		Timestamp:       time.Now().Format(time.RFC3339),
		Mode:            "stress",
		DBFile:          DBFilename,
		OutFile:         opts.outFile,
		MaxID:           atomic.LoadUint64(maxIDPtr),
		CaseDurationSec: opts.caseDuration.Seconds(),
		ReportEverySec:  opts.reportEvery.Seconds(),
		StartRecords:    startCount,
		FinalRecords:    stress.RecordsAtEnd,
		Workers:         []int{len(workers)},
		Stress:          &stress,
		Interrupted:     stress.Interrupted,
	}
}

func buildCeilingModeReport(
	ctx context.Context,
	db *rbi.DB[uint64, UserBench],
	maxIDPtr *uint64,
	startCount uint64,
	emailSamples []string,
	opts benchOptions,
) BenchmarkReport {
	log.Printf(
		"Starting ceiling mode: stage_duration=%s report_every=%s dispatch_quantum=%s queue_cap=%d suites=%s",
		opts.ceilingStageDuration.String(),
		opts.reportEvery.String(),
		opts.ceilingDispatchQuantum.String(),
		opts.ceilingQueueCap,
		opts.ceilingSuites,
	)

	ceiling := runCeiling(
		ctx,
		db,
		maxIDPtr,
		emailSamples,
		opts,
	)

	return BenchmarkReport{
		Timestamp:       time.Now().Format(time.RFC3339),
		Mode:            "ceiling",
		DBFile:          DBFilename,
		OutFile:         opts.outFile,
		MaxID:           atomic.LoadUint64(maxIDPtr),
		CaseDurationSec: opts.ceilingStageDuration.Seconds(),
		ReportEverySec:  opts.reportEvery.Seconds(),
		StartRecords:    startCount,
		FinalRecords:    ceiling.RecordsAtEnd,
		Ceiling:         &ceiling,
		Interrupted:     ceiling.Interrupted,
	}
}

func buildMatrixModeReport(
	ctx context.Context,
	db *rbi.DB[uint64, UserBench],
	maxIDPtr *uint64,
	startCount uint64,
	emailSamples []string,
	opts benchOptions,
	interrupted *atomic.Bool,
) BenchmarkReport {
	report := BenchmarkReport{
		Timestamp:       time.Now().Format(time.RFC3339),
		Mode:            "matrix",
		DBFile:          DBFilename,
		OutFile:         opts.outFile,
		MaxID:           atomic.LoadUint64(maxIDPtr),
		CaseDurationSec: opts.caseDuration.Seconds(),
		ReportEverySec:  opts.reportEvery.Seconds(),
		StartRecords:    startCount,
		Profiles:        Profiles,
		Workers:         WorkerMatrix,
		Results:         make([]CaseResult, 0, len(Profiles)*len(WorkerMatrix)),
		ReadMix:         defaultReadMix(),
		WriteMix:        defaultWriteMix(),
		ComplexReadMix:  defaultComplexReadMix(),
	}

runLoop:
	for _, profile := range Profiles {
		for _, workers := range WorkerMatrix {
			if ctx.Err() != nil {
				break runLoop
			}

			log.Printf(
				"Running case: profile=%s (read=%.6f write=%.6f), workers=%d, duration=%s",
				profile.Name,
				profile.ReadRatio,
				profile.WriteRatio,
				workers,
				opts.caseDuration.String(),
			)

			res := runCase(
				ctx,
				db,
				maxIDPtr,
				emailSamples,
				profile,
				workers,
				opts.caseDuration,
				opts.reportEvery,
			)
			report.Results = append(report.Results, res)
			if res.Interrupted {
				break runLoop
			}
		}
	}

	report.Interrupted = interrupted.Load() || ctx.Err() != nil
	report.MaxID = atomic.LoadUint64(maxIDPtr)
	report.FinalRecords, _ = db.Count(nil)

	return report
}

func defaultReadMix() map[string]float64 {
	return map[string]float64{
		"get_by_id":                0.50,
		"single_record_query":      0.25,
		"complex_query_collection": 0.25,
	}
}

func defaultWriteMix() map[string]float64 {
	return map[string]float64{
		"patch_existing": 0.70,
		"insert_new":     0.25,
		"delete_random":  0.05,
	}
}

func defaultComplexReadMix() map[string]float64 {
	return map[string]float64{
		"recent_active_segment_items": 0.18,
		"verified_permissions_items":  0.15,
		"heavy_or_keys":               0.15,
		"leaderboard_items":           0.14,
		"autocomplete_items":          0.12,
		"analytics_count":             0.10,
		"stale_segment_keys":          0.08,
		"deep_offset_keys":            0.08,
	}
}
