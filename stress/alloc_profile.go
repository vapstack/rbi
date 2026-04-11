package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"time"
)

const (
	allocSourcePrepared = "prepared"
	allocSourceWorkload = "workload"
	allocModeHot        = "hot"
	allocModeTurnover   = "turnover"
	allocScopeFull      = "full"
	allocScopeQuery     = "query"
)

type focusedAllocRunner struct {
	before func() error
	run    func() error
}

type focusedAllocTarget struct {
	classInfo StressClassInfo
	queryInfo StressQueryInfo
	prepare   func(*DBHandle, options) (focusedAllocRunner, error)
}

func selectFocusedAllocWorkloadTarget(classFilter, queryFilter []string) (focusedAllocTarget, error) {
	specs, err := filterStressClassSpecs(defaultStressClassSpecs(), classFilter, queryFilter)
	if err != nil {
		return focusedAllocTarget{}, err
	}
	if len(specs) != 1 {
		return focusedAllocTarget{}, fmt.Errorf("alloc-profile requires exactly one filtered class, got %d", len(specs))
	}
	spec := specs[0]
	if len(spec.queries) != 1 {
		return focusedAllocTarget{}, fmt.Errorf("alloc-profile requires exactly one filtered query, got %d for class %q", len(spec.queries), spec.info.Name)
	}
	query := spec.queries[0]
	return focusedAllocTarget{
		classInfo: spec.info,
		queryInfo: query.info,
		prepare: func(handle *DBHandle, _ options) (focusedAllocRunner, error) {
			workCtx := &WorkloadContext{
				DB:           handle.DB,
				MaxIDPtr:     &handle.MaxID,
				EmailSamples: handle.EmailSamples,
			}
			rng := NewRand(1)
			return focusedAllocRunner{
				run: func() error {
					queryName, runErr := query.run(workCtx, rng)
					if runErr != nil {
						return runErr
					}
					if queryName != "" && queryName != query.info.Name {
						return fmt.Errorf("focused alloc query drift: got %q want %q", queryName, query.info.Name)
					}
					return nil
				},
			}, nil
		},
	}, nil
}

func selectFocusedAllocPreparedTarget(classFilter, queryFilter []string) (focusedAllocTarget, error) {
	specs, err := filterResearchAllocQueries(classFilter, queryFilter)
	if err != nil {
		return focusedAllocTarget{}, err
	}
	if len(specs) != 1 {
		return focusedAllocTarget{}, fmt.Errorf("alloc-profile requires exactly one filtered research query, got %d", len(specs))
	}
	spec := specs[0]
	return focusedAllocTarget{
		classInfo: spec.classInfo,
		queryInfo: spec.queryInfo,
		prepare: func(handle *DBHandle, opts options) (focusedAllocRunner, error) {
			q := spec.build(time.Now().Unix())
			runner := focusedAllocRunner{
				run: func() error {
					return runResearchAllocQuery(handle.DB, spec.runKind, q)
				},
			}
			if opts.AllocMode == allocModeTurnover {
				ring, ringErr := buildAllocTurnoverRing(handle, opts.AllocTurnoverRing)
				if ringErr != nil {
					return focusedAllocRunner{}, ringErr
				}
				runner.before = func() error {
					return ring.apply(handle.DB)
				}
			}
			return runner, nil
		},
	}, nil
}

func selectFocusedAllocTarget(opts options) (focusedAllocTarget, error) {
	switch opts.AllocSource {
	case allocSourcePrepared:
		return selectFocusedAllocPreparedTarget(opts.ClassFilter, opts.QueryFilter)
	case allocSourceWorkload:
		return selectFocusedAllocWorkloadTarget(opts.ClassFilter, opts.QueryFilter)
	default:
		return focusedAllocTarget{}, fmt.Errorf("unsupported alloc-source %q", opts.AllocSource)
	}
}

func runFocusedAllocProfile(handle *DBHandle, opts options, collector *plannerTraceCollector) error {
	target, err := selectFocusedAllocTarget(opts)
	if err != nil {
		return err
	}

	log.Printf(
		"running focused alloc profile source=%s mode=%s scope=%s class=%s query=%s duration=%s alloc_ops=%d warmup_ops=%d memrate=%d",
		opts.AllocSource,
		opts.AllocMode,
		opts.AllocScope,
		target.classInfo.Alias,
		target.queryInfo.Name,
		opts.Duration,
		opts.AllocOps,
		opts.AllocWarmupOps,
		opts.AllocMemProfileRate,
	)

	runner, err := target.prepare(handle, opts)
	if err != nil {
		return err
	}

	var (
		traceWorker *plannerTraceWorker
		traceEpoch  *plannerTraceEpoch
	)
	if collector != nil && opts.TraceSampleEvery >= 0 {
		traceEpoch = collector.newEpoch()
		traceWorker = collector.newWorker(&classDescriptor{Info: target.classInfo})
		traceWorker.bindCurrentGoroutine()
		defer traceWorker.close()
	}
	if traceWorker != nil {
		baseRun := runner.run
		runner.run = func() error {
			done := traceWorker.begin(target.queryInfo.Name)
			defer done()
			return baseRun()
		}
	}

	ctx := context.Background()
	labels := pprof.Labels(
		"stress_mode", "alloc",
		"stress_alloc_source", opts.AllocSource,
		"stress_alloc_exec", opts.AllocMode,
		"stress_alloc_scope", opts.AllocScope,
		"stress_class", target.classInfo.Alias,
		"stress_query", target.queryInfo.Name,
	)

	var measuredOps uint64
	pprof.Do(ctx, labels, func(ctx context.Context) {
		if opts.AllocWarmupOps > 0 {
			warmupLabels := pprof.Labels("phase", "warmup")
			pprof.Do(ctx, warmupLabels, func(context.Context) {
				err = runFocusedAllocWarmup(runner, opts.AllocWarmupOps)
			})
			if err != nil {
				return
			}
		}

		runtime.MemProfileRate = opts.AllocMemProfileRate
		defer func() {
			runtime.MemProfileRate = 0
		}()

		measureLabels := pprof.Labels("phase", "measure")
		pprof.Do(ctx, measureLabels, func(context.Context) {
			measuredOps, err = runFocusedAllocMeasure(runner, opts)
		})
	})
	if err != nil {
		return err
	}

	runtime.GC()
	if err := writeAllocProfile(opts.AllocProfile); err != nil {
		return err
	}

	log.Printf(
		"focused alloc profile saved query=%s measured_ops=%d path=%s",
		target.queryInfo.Name,
		measuredOps,
		opts.AllocProfile,
	)
	if traceEpoch != nil {
		report := traceEpoch.snapshot().queryReport(target.classInfo.Name, target.queryInfo.Name)
		if report != nil && report.Sampled > 0 {
			log.Printf(
				"focused alloc trace samples=%d avg_rows_examined=%.1f avg_rows_returned=%.1f avg_duration_us=%.1f",
				report.Sampled,
				report.AvgRowsExamined,
				report.AvgRowsReturned,
				report.AvgDurationUs,
			)
		}
	}
	return nil
}

func runFocusedAllocWarmup(runner focusedAllocRunner, ops int) error {
	for i := 0; i < ops; i++ {
		if err := runFocusedAllocOp(runner); err != nil {
			return fmt.Errorf("warmup op %d: %w", i+1, err)
		}
	}
	return nil
}

func runFocusedAllocMeasure(runner focusedAllocRunner, opts options) (uint64, error) {
	if opts.AllocOps > 0 {
		var ops uint64
		for ops < uint64(opts.AllocOps) {
			if err := runFocusedAllocMeasuredOp(runner, opts); err != nil {
				return ops, fmt.Errorf("measure op %d: %w", ops+1, err)
			}
			ops++
		}
		return ops, nil
	}
	deadline := time.Now().Add(opts.Duration)
	var ops uint64
	for time.Now().Before(deadline) {
		if err := runFocusedAllocMeasuredOp(runner, opts); err != nil {
			return ops, fmt.Errorf("measure op %d: %w", ops+1, err)
		}
		ops++
	}
	return ops, nil
}

func runFocusedAllocMeasuredOp(runner focusedAllocRunner, opts options) error {
	if runner.before == nil || opts.AllocScope != allocScopeQuery || opts.AllocMode != allocModeTurnover {
		return runFocusedAllocOp(runner)
	}
	runtime.MemProfileRate = 0
	if err := runner.before(); err != nil {
		runtime.MemProfileRate = opts.AllocMemProfileRate
		return err
	}
	runtime.MemProfileRate = opts.AllocMemProfileRate
	return runner.run()
}

func runFocusedAllocOp(runner focusedAllocRunner) error {
	if runner.before != nil {
		if err := runner.before(); err != nil {
			return err
		}
	}
	return runner.run()
}

func writeAllocProfile(path string) error {
	profile := pprof.Lookup("allocs")
	if profile == nil {
		return fmt.Errorf("allocs profile unavailable")
	}
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create alloc profile: %w", err)
	}
	if err := profile.WriteTo(f, 0); err != nil {
		_ = f.Close()
		return fmt.Errorf("write alloc profile: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close alloc profile: %w", err)
	}
	return nil
}
