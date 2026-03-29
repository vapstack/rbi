package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime/debug"
	"sync/atomic"
	"syscall"
	"time"
)

func main() {
	log.SetFlags(0)

	catalog, _, _, err := loadClassCatalog()
	if err != nil {
		fatalf("load stress catalog: %v", err)
	}
	opts, err := parseOptions(catalog)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fatalf("parse options: %v", err)
	}
	catalog, _, _, err = loadFilteredClassCatalog(opts.ClassFilter, opts.QueryFilter)
	if err != nil {
		fatalf("apply stress filters: %v", err)
	}
	initialWorkers, err := resolveInitialWorkers(catalog, opts.InitialWorkers, opts.WorkerGroups)
	if err != nil {
		fatalf("invalid worker overrides: %v", err)
	}

	log.Printf(
		"opening DB file=%s report=%s headless=%t duration=%s trace_sample=%d trace_top=%d query_stats=%t jitter=%t class_filter=%v query_filter=%v",
		opts.DBFile,
		opts.ReportPath,
		opts.Headless,
		opts.Duration,
		opts.TraceSampleEvery,
		opts.TraceTopN,
		opts.QueryStats,
		opts.Jitter,
		opts.ClassFilter,
		opts.QueryFilter,
	)
	stopProfiling, err := startStressProfiling(opts)
	if err != nil {
		fatalf("start profiling: %v", err)
	}
	traceCollector := newPlannerTraceCollector(catalog, opts.TraceSampleEvery, opts.TraceTopN)
	handle, err := OpenBenchDB(DBConfig{
		DBFile:           opts.DBFile,
		BoltNoSync:       opts.BoltNoSync,
		AnalyzeInterval:  opts.AnalyzeInterval,
		TraceSink:        traceCollector.traceSink(),
		TraceSampleEvery: opts.TraceSampleEvery,
	}, opts.EmailSampleN)
	if err != nil {
		fatalf("open db: %v", err)
	}
	log.Printf(
		"DB opened file=%s records=%d max_id=%d email_samples=%d",
		handle.DBFile,
		handle.StartRecords,
		handle.MaxID,
		len(handle.EmailSamples),
	)

	queryBreakdown := !opts.Headless || opts.QueryStats
	queryLatency := !opts.Headless || opts.QueryStats
	app := newApp(handle, catalog, opts.RefreshEvery, opts.TelemetryEvery, opts.ReportPath, opts.ClassFilter, opts.QueryFilter, queryBreakdown, queryLatency, opts.Jitter, traceCollector)
	app.applyInitialWorkers(initialWorkers)

	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := baseCtx
	if opts.Duration > 0 {
		var timeoutCancel context.CancelFunc
		ctx, timeoutCancel = context.WithTimeout(baseCtx, opts.Duration)
		defer timeoutCancel()
	}

	var reader *lineReader
	var renderer *renderer
	if !opts.Headless {
		reader, err = newLineReader()
		if err != nil {
			fatalf("line reader: %v", err)
		}
		defer func() {
			_ = reader.Close()
		}()

		renderer = newRenderer(os.Stdout, reader.Interactive() && isTerminal(int(os.Stdout.Fd())))
		defer func() {
			_ = renderer.Close()
		}()
	}

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	var interrupted atomic.Bool
	go func() {
		for count := 0; ; count++ {
			_, ok := <-sigCh
			if !ok {
				return
			}
			if count == 0 {
				interrupted.Store(true)
				app.stopAllWorkers()
				cancel()
				continue
			}
			if renderer != nil {
				_ = renderer.Close()
			}
			if reader != nil {
				_ = reader.Close()
			}
			os.Exit(130)
		}
	}()

	var runErr error
	switch {
	case opts.Headless:
		log.Printf("running headless stress workers until duration=%s or signal", opts.Duration)
		runErr = app.runHeadless(ctx)
	default:
		runErr = app.run(ctx, renderer, reader)
	}
	if interrupted.Load() && renderer != nil && reader != nil {
		renderShutdownStatus(app, renderer, reader, "Interrupt received; waiting for workers to stop...")
	}
	app.stopAllWorkers()
	app.waitWorkers()
	if stopProfiling != nil {
		if interrupted.Load() && renderer != nil && reader != nil {
			renderShutdownStatus(app, renderer, reader, "Workers stopped; saving profiles...")
		}
		if err := stopProfiling(); err != nil {
			fatalf("stop profiling: %v", err)
		}
	}
	log.Printf("workers stopped; building report")

	if interrupted.Load() && renderer != nil && reader != nil {
		renderShutdownStatus(app, renderer, reader, "Workers stopped; collecting final report...")
	}
	report := app.buildReport(interrupted.Load())
	if interrupted.Load() && renderer != nil && reader != nil {
		renderShutdownStatus(app, renderer, reader, fmt.Sprintf("Writing report to %s...", opts.ReportPath))
	}
	if err := saveReportFile(opts.ReportPath, report); err != nil {
		fatalf("save report: %v", err)
	}
	log.Printf("report file written to %s; DB close still pending", opts.ReportPath)
	if interrupted.Load() && renderer != nil && reader != nil {
		renderShutdownStatus(app, renderer, reader, "Report saved; releasing OS memory...")
	}
	debug.FreeOSMemory()
	if interrupted.Load() && renderer != nil && reader != nil {
		renderShutdownStatus(app, renderer, reader, "Closing DB...")
	}
	log.Printf("closing DB %s", handle.DBFile)
	closeErr := handle.Close()
	if closeErr == nil {
		log.Printf("DB closed %s", handle.DBFile)
	}

	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		fatalf("run stress: %v", runErr)
	}
	if closeErr != nil {
		fatalf("close db: %v", closeErr)
	}

	_, _ = fmt.Fprintf(os.Stdout, "\nreport saved to %s\nDB closed %s\n", opts.ReportPath, handle.DBFile)
}

func renderShutdownStatus(app *app, renderer *renderer, reader *lineReader, message string) {
	app.setStatus(message)
	_ = renderer.render(app.buildSnapshot(time.Now(), false), reader.Buffer(), app.statusText())
}

func saveReportFile(path string, report stressReport) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func fatalf(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
