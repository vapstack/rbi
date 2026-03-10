package main

import (
	"flag"
	"log"
	"time"
)

type benchOptions struct {
	caseDuration         time.Duration
	sweepDuration        time.Duration
	reportEvery          time.Duration
	ceilingStageDuration time.Duration
	outFile              string
	emailSampleN         int
	cpuProfile           string
	heapProfile          string
	pprofHTTP            string

	matrix        bool
	sweep         bool
	stress        bool
	ceiling       bool
	stressNoRead  bool
	stressNoWrite bool

	ceilingDispatchQuantum      time.Duration
	ceilingQueueCap             int
	ceilingReadFastWorkers      int
	ceilingReadSlowWorkers      int
	ceilingWriteUpdateWorkers   int
	ceilingWriteInsertWorkers   int
	ceilingReadFastGrid         string
	ceilingWriteGrid            string
	ceilingReadSlowGrid         string
	ceilingLowWriteGrid         string
	ceilingMixedSlowOps         float64
	ceilingBaselineFraction     float64
	ceilingMinThroughputRatio   float64
	ceilingRegressThroughputPct float64
	ceilingRegressP99Pct        float64
	ceilingSuites               string

	boltNoSync          bool
	snapshotDiagnostics bool
	analyzeInterval     time.Duration

	snapshotRegistryMax                  int
	snapshotDeltaCompactFieldKeys        int
	snapshotDeltaCompactFieldOps         int
	snapshotDeltaCompactMaxFieldsPublish int
	snapshotDeltaCompactUniverseOps      int
	snapshotDeltaLayerMaxDepth           int
}

func parseBenchOptions() benchOptions {
	opts := benchOptions{}

	flag.DurationVar(&opts.caseDuration, "case-duration", DefaultCaseDuration, "duration of each profile/workers case")
	flag.DurationVar(&opts.sweepDuration, "sweep-duration", DefaultSweepDuration, "duration of each query/workers case in sweep mode")
	flag.DurationVar(&opts.reportEvery, "report-every", DefaultReportEvery, "interval for per-case progress logs")
	flag.DurationVar(&opts.ceilingStageDuration, "ceiling-stage-duration", DefaultCeilingStageDuration, "duration of one stage in ceiling mode")
	flag.StringVar(&opts.outFile, "out", DefaultReportPath, "path to JSON report file")
	flag.IntVar(&opts.emailSampleN, "email-sample", DefaultEmailSampleN, "how many existing emails to sample for read queries")
	flag.StringVar(&opts.cpuProfile, "cpu-profile", "", "write CPU profile to file")
	flag.StringVar(&opts.heapProfile, "heap-profile", "", "write heap profile to file at process end")
	flag.StringVar(&opts.pprofHTTP, "pprof-http", "", "listen address for net/http/pprof (e.g. :6060)")

	flag.BoolVar(&opts.matrix, "matrix", false, "run mixed read/write matrix mode")
	flag.BoolVar(&opts.sweep, "sweep", false, "run read-only per-query sweep (workers 1/4/16)")
	flag.BoolVar(&opts.stress, "stress", false, "run dedicated read/write worker stress mode")
	flag.BoolVar(&opts.ceiling, "ceiling", false, "run ceiling stress mode with independent dispatchers and class queues")
	flag.BoolVar(&opts.stressNoRead, "stress-no-read", false, "stress: disable read workers")
	flag.BoolVar(&opts.stressNoWrite, "stress-no-write", false, "stress: disable write workers")

	flag.DurationVar(&opts.ceilingDispatchQuantum, "ceiling-dispatch-quantum", DefaultCeilingDispatchQuantum, "ceiling: dispatcher quantum for open-loop arrivals")
	flag.IntVar(&opts.ceilingQueueCap, "ceiling-queue-cap", DefaultCeilingQueueCap, "ceiling: per-class queue capacity")
	flag.IntVar(&opts.ceilingReadFastWorkers, "ceiling-read-fast-workers", DefaultCeilingReadFastWorkers, "ceiling: executors for fast read class")
	flag.IntVar(&opts.ceilingReadSlowWorkers, "ceiling-read-slow-workers", DefaultCeilingReadSlowWorkers, "ceiling: executors for slow read class")
	flag.IntVar(&opts.ceilingWriteUpdateWorkers, "ceiling-write-update-workers", DefaultCeilingWriteUpdateWorkers, "ceiling: executors for update-write class")
	flag.IntVar(&opts.ceilingWriteInsertWorkers, "ceiling-write-insert-workers", DefaultCeilingWriteInsertWorkers, "ceiling: executors for insert-write class")
	flag.StringVar(&opts.ceilingReadFastGrid, "ceiling-read-fast-grid", DefaultCeilingReadFastGrid, "ceiling: read-fast target ops/s grid (comma-separated)")
	flag.StringVar(&opts.ceilingWriteGrid, "ceiling-write-grid", DefaultCeilingWriteGrid, "ceiling: write target ops/s grid (comma-separated)")
	flag.StringVar(&opts.ceilingReadSlowGrid, "ceiling-read-slow-grid", DefaultCeilingReadSlowGrid, "ceiling: slow-read target ops/s grid (comma-separated)")
	flag.StringVar(&opts.ceilingLowWriteGrid, "ceiling-low-write-grid", DefaultCeilingLowWriteGrid, "ceiling: low-write target ops/s grid (comma-separated)")
	flag.Float64Var(&opts.ceilingMixedSlowOps, "ceiling-mixed-slow-ops", DefaultCeilingMixedSlowOps, "ceiling: fixed slow-read ops/s in mixed suite")
	flag.Float64Var(&opts.ceilingBaselineFraction, "ceiling-baseline-fraction", DefaultCeilingBaselineFraction, "ceiling: fraction of discovered fast-read ceiling used as baseline")
	flag.Float64Var(&opts.ceilingMinThroughputRatio, "ceiling-min-throughput-ratio", DefaultCeilingMinThroughputRatio, "ceiling: minimal completed/offered ratio before saturation")
	flag.Float64Var(&opts.ceilingRegressThroughputPct, "ceiling-regress-throughput-drop-pct", DefaultCeilingRegressionThroughputDrop, "ceiling: fast-read throughput drop pct considered regression")
	flag.Float64Var(&opts.ceilingRegressP99Pct, "ceiling-regress-p99-increase-pct", DefaultCeilingRegressionP99Increase, "ceiling: fast-read p99 increase pct considered regression")
	flag.StringVar(&opts.ceilingSuites, "ceiling-suites", DefaultCeilingSuites, "ceiling: suites to run (all or comma-separated names)")

	flag.BoolVar(&opts.boltNoSync, "bolt-no-sync", false, "set bbolt NoSync=true (diagnostic, unsafe)")
	flag.BoolVar(&opts.snapshotDiagnostics, "snapshot-diagnostics", false, "log snapshot/compactor diagnostics in progress ticker")
	flag.DurationVar(&opts.analyzeInterval, "analyze-interval", 0, "rbi analyze interval (0=default, <0 disable)")

	flag.IntVar(&opts.snapshotRegistryMax, "snapshot-registry-max", -1, "override Options.SnapshotRegistryMax")
	flag.IntVar(&opts.snapshotDeltaCompactFieldKeys, "snapshot-delta-compact-field-keys", -1, "override Options.SnapshotDeltaCompactFieldKeys")
	flag.IntVar(&opts.snapshotDeltaCompactFieldOps, "snapshot-delta-compact-field-ops", -1, "override Options.SnapshotDeltaCompactFieldOps")
	flag.IntVar(&opts.snapshotDeltaCompactMaxFieldsPublish, "snapshot-delta-compact-max-fields-per-publish", -1, "override Options.SnapshotDeltaCompactMaxFieldsPerPublish")
	flag.IntVar(&opts.snapshotDeltaCompactUniverseOps, "snapshot-delta-compact-universe-ops", -1, "override Options.SnapshotDeltaCompactUniverseOps")
	flag.IntVar(&opts.snapshotDeltaLayerMaxDepth, "snapshot-delta-layer-max-depth", -1, "override Options.SnapshotDeltaLayerMaxDepth")
	flag.Parse()

	modeN := 0
	if opts.matrix {
		modeN++
	}
	if opts.sweep {
		modeN++
	}
	if opts.stress {
		modeN++
	}
	if opts.ceiling {
		modeN++
	}
	if modeN != 1 {
		log.Fatal("choose exactly one mode flag: -matrix, -sweep, -stress or -ceiling")
	}

	if opts.stress && opts.stressNoRead && opts.stressNoWrite {
		log.Fatal("stress requires at least one worker type enabled (read or write)")
	}

	if opts.ceilingStageDuration <= 0 {
		log.Fatal("ceiling-stage-duration must be > 0")
	}
	if opts.ceilingDispatchQuantum <= 0 {
		log.Fatal("ceiling-dispatch-quantum must be > 0")
	}
	if opts.ceilingQueueCap < 1 {
		log.Fatal("ceiling-queue-cap must be >= 1")
	}
	if opts.ceilingReadFastWorkers < 1 {
		log.Fatal("ceiling-read-fast-workers must be >= 1")
	}
	if opts.ceilingReadSlowWorkers < 1 {
		log.Fatal("ceiling-read-slow-workers must be >= 1")
	}
	if opts.ceilingWriteUpdateWorkers < 1 {
		log.Fatal("ceiling-write-update-workers must be >= 1")
	}
	if opts.ceilingWriteInsertWorkers < 1 {
		log.Fatal("ceiling-write-insert-workers must be >= 1")
	}
	if opts.ceilingMixedSlowOps < 0 {
		log.Fatal("ceiling-mixed-slow-ops must be >= 0")
	}
	if opts.ceilingBaselineFraction <= 0 || opts.ceilingBaselineFraction > 1 {
		log.Fatal("ceiling-baseline-fraction must be in (0,1]")
	}
	if opts.ceilingMinThroughputRatio <= 0 || opts.ceilingMinThroughputRatio > 1 {
		log.Fatal("ceiling-min-throughput-ratio must be in (0,1]")
	}
	if opts.ceilingRegressThroughputPct < 0 {
		log.Fatal("ceiling-regress-throughput-drop-pct must be >= 0")
	}
	if opts.ceilingRegressP99Pct < 0 {
		log.Fatal("ceiling-regress-p99-increase-pct must be >= 0")
	}

	return opts
}

func (o benchOptions) applySnapshotOverrides() {
	EnableSnapshotDiagnosticsLogs = o.snapshotDiagnostics
}
