package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type workerCountOverride struct {
	Value int
	Set   bool
}

type workerGroupOverrides struct {
	All   workerCountOverride
	Read  workerCountOverride
	Write workerCountOverride
}

type options struct {
	DBFile              string
	ReportPath          string
	CPUProfile          string
	HeapProfile         string
	AllocProfile        string
	AllocSource         string
	AllocMode           string
	AllocScope          string
	PprofHTTP           string
	AllocWarmupOps      int
	AllocOps            int
	AllocMemProfileRate int
	AllocTurnoverRing   int
	EmailSampleN        int
	NoCache             bool
	BoltNoSync          bool
	AnalyzeInterval     time.Duration
	RefreshEvery        time.Duration
	TelemetryEvery      time.Duration
	TraceSampleEvery    int
	TraceTopN           int
	QueryStats          bool
	Jitter              bool
	Duration            time.Duration
	Headless            bool
	ClassFilter         []string
	QueryFilter         []string
	WorkerGroups        workerGroupOverrides
	InitialWorkers      map[string]int
}

func parseOptions(catalog []*classDescriptor) (options, error) {
	opts := options{
		DBFile:              DefaultDBFilename,
		ReportPath:          "stress_report.json",
		AllocSource:         allocSourcePrepared,
		AllocMode:           allocModeHot,
		AllocScope:          allocScopeFull,
		AllocWarmupOps:      64,
		AllocOps:            0,
		AllocMemProfileRate: 1,
		AllocTurnoverRing:   defaultAllocTurnoverRingSize,
		EmailSampleN:        DefaultEmailSampleN,
		RefreshEvery:        2 * time.Second,
		TelemetryEvery:      4 * time.Second,
		TraceSampleEvery:    -1,
		TraceTopN:           24,
		InitialWorkers:      make(map[string]int, len(catalog)),
	}

	fs := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	fs.StringVar(&opts.DBFile, "db", opts.DBFile, "path to bolt db file")
	fs.StringVar(&opts.ReportPath, "report", opts.ReportPath, "path to JSON report file")
	fs.StringVar(&opts.ReportPath, "out", opts.ReportPath, "path to JSON report file")
	fs.StringVar(&opts.CPUProfile, "cpu-profile", "", "write CPU profile to file")
	fs.StringVar(&opts.HeapProfile, "heap-profile", "", "write heap profile to file at process end")
	fs.StringVar(&opts.AllocProfile, "alloc-profile", "", "write focused allocs profile to file for a single filtered query")
	fs.StringVar(&opts.AllocSource, "alloc-source", opts.AllocSource, "focused alloc source: prepared or workload")
	fs.StringVar(&opts.AllocMode, "alloc-mode", opts.AllocMode, "focused alloc mode: hot or turnover")
	fs.StringVar(&opts.AllocScope, "alloc-scope", opts.AllocScope, "focused alloc scope: full or query")
	fs.StringVar(&opts.PprofHTTP, "pprof-http", "", "listen address for net/http/pprof (e.g. :6060)")
	fs.IntVar(&opts.AllocWarmupOps, "alloc-warmup-ops", opts.AllocWarmupOps, "warmup query ops before focused alloc profiling")
	fs.IntVar(&opts.AllocOps, "alloc-ops", opts.AllocOps, "fixed measured query ops for focused alloc profiling (overrides duration)")
	fs.IntVar(&opts.AllocMemProfileRate, "alloc-memrate", opts.AllocMemProfileRate, "runtime.MemProfileRate during focused alloc profiling")
	fs.IntVar(&opts.AllocTurnoverRing, "alloc-turnover-ring", opts.AllocTurnoverRing, "turnover ring size for prepared alloc profiling")
	fs.IntVar(&opts.EmailSampleN, "email-sample", opts.EmailSampleN, "how many existing emails to sample for indexed reads")
	fs.BoolVar(&opts.NoCache, "no-cache", false, "disable rbi runtime caches and numeric range bucket acceleration")
	fs.BoolVar(&opts.BoltNoSync, "bolt-no-sync", false, "open bbolt with NoSync=true (unsafe)")
	fs.DurationVar(&opts.AnalyzeInterval, "analyze-interval", 0, "rbi analyze interval (0=default, <0 disable)")
	fs.DurationVar(&opts.RefreshEvery, "refresh", opts.RefreshEvery, "table refresh interval")
	fs.DurationVar(&opts.TelemetryEvery, "telemetry", opts.TelemetryEvery, "memory/snapshot/batch sampling interval")
	fs.IntVar(&opts.TraceSampleEvery, "trace-sample", opts.TraceSampleEvery, "planner trace sampling (-1 disable, 0 every query, N every Nth query)")
	fs.IntVar(&opts.TraceTopN, "trace-top", opts.TraceTopN, "how many slowest sampled planner traces to keep in the report")
	fs.BoolVar(&opts.QueryStats, "query-stats", false, "enable per-query breakdowns/latency in headless runs and reports; interactive mode already collects them by default")
	fs.BoolVar(&opts.Jitter, "jitter", false, "sleep 0.5-1ms with jitter after each completed operation; excluded from metrics")
	fs.DurationVar(&opts.Duration, "duration", 0, "fixed run duration; when >0, stress runs in headless mode")
	fs.BoolVar(&opts.Headless, "headless", false, "run without interactive UI")
	fs.BoolVar(&opts.Headless, "no-ui", false, "run without interactive UI")
	bindWorkerCountFlag(fs, "a", "initial workers for all classes", &opts.WorkerGroups.All)
	bindWorkerCountFlag(fs, "r", "initial workers for all read classes", &opts.WorkerGroups.Read)
	bindWorkerCountFlag(fs, "w", "initial workers for all write classes", &opts.WorkerGroups.Write)
	fs.Func("class", "restrict workload catalog to the given class aliases/names (comma-separated)", func(value string) error {
		opts.ClassFilter = append(opts.ClassFilter, parseCSVFilter(value)...)
		return nil
	})
	fs.Func("query", "restrict workload catalog to the given query names (comma-separated)", func(value string) error {
		opts.QueryFilter = append(opts.QueryFilter, parseCSVFilter(value)...)
		return nil
	})

	workerFlags := make(map[string]int, len(catalog))
	for _, class := range catalog {
		desc := fmt.Sprintf("initial workers for %s (%s, default workers %d)", class.Info.Alias, class.Info.Name, class.Info.DefaultWorkers)
		bindWorkerOverrideFlag(fs, class.Info.Alias, desc, class.Info.Name, workerFlags)
		bindWorkerOverrideFlag(fs, class.Info.Name, desc, class.Info.Name, workerFlags)
	}

	if err := fs.Parse(os.Args[1:]); err != nil {
		return options{}, err
	}
	if opts.EmailSampleN < 0 {
		return options{}, fmt.Errorf("email-sample must be >= 0")
	}
	if opts.AllocWarmupOps < 0 {
		return options{}, fmt.Errorf("alloc-warmup-ops must be >= 0")
	}
	if opts.AllocOps < 0 {
		return options{}, fmt.Errorf("alloc-ops must be >= 0")
	}
	if opts.AllocMemProfileRate < 0 {
		return options{}, fmt.Errorf("alloc-memrate must be >= 0")
	}
	if opts.RefreshEvery <= 0 {
		return options{}, fmt.Errorf("refresh must be > 0")
	}
	if opts.TelemetryEvery <= 0 {
		return options{}, fmt.Errorf("telemetry must be > 0")
	}
	if opts.TraceTopN < 0 {
		return options{}, fmt.Errorf("trace-top must be >= 0")
	}
	if opts.Duration < 0 {
		return options{}, fmt.Errorf("duration must be >= 0")
	}
	if opts.Duration > 0 {
		opts.Headless = true
	}
	switch opts.AllocSource {
	case allocSourcePrepared, allocSourceWorkload:
	default:
		return options{}, fmt.Errorf("alloc-source must be %q or %q", allocSourcePrepared, allocSourceWorkload)
	}
	switch opts.AllocMode {
	case allocModeHot, allocModeTurnover:
	default:
		return options{}, fmt.Errorf("alloc-mode must be %q or %q", allocModeHot, allocModeTurnover)
	}
	switch opts.AllocScope {
	case allocScopeFull, allocScopeQuery:
	default:
		return options{}, fmt.Errorf("alloc-scope must be %q or %q", allocScopeFull, allocScopeQuery)
	}
	if opts.AllocTurnoverRing < 0 {
		return options{}, fmt.Errorf("alloc-turnover-ring must be >= 0")
	}
	if opts.AllocProfile != "" {
		opts.Headless = true
		if opts.Duration <= 0 && opts.AllocOps <= 0 {
			return options{}, fmt.Errorf("alloc-profile requires duration > 0 or alloc-ops > 0")
		}
		if opts.AllocMemProfileRate == 0 {
			return options{}, fmt.Errorf("alloc-memrate must be > 0 when alloc-profile is enabled")
		}
		if opts.AllocMode == allocModeTurnover && opts.AllocTurnoverRing == 0 {
			return options{}, fmt.Errorf("alloc-turnover-ring must be > 0 when alloc-mode=%s", allocModeTurnover)
		}
	}
	opts.ClassFilter = dedupeStrings(opts.ClassFilter)
	opts.QueryFilter = dedupeStrings(opts.QueryFilter)
	for name, value := range workerFlags {
		opts.InitialWorkers[name] = value
	}
	return opts, nil
}

func bindWorkerCountFlag(fs *flag.FlagSet, name, usage string, dst *workerCountOverride) {
	fs.Func(name, usage, func(value string) error {
		n, err := parseWorkerCountFlag(value)
		if err != nil {
			return err
		}
		dst.Value = n
		dst.Set = true
		return nil
	})
}

func bindWorkerOverrideFlag(fs *flag.FlagSet, flagName, usage, className string, dst map[string]int) {
	fs.Func(flagName, usage, func(value string) error {
		n, err := parseWorkerCountFlag(value)
		if err != nil {
			return err
		}
		dst[className] = n
		return nil
	})
}

func parseWorkerCountFlag(value string) (int, error) {
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("worker count must be an integer")
	}
	if n < 0 {
		return 0, fmt.Errorf("worker count must be >= 0")
	}
	return n, nil
}

func parseCSVFilter(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(strings.ToLower(part))
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}

func dedupeStrings(items []string) []string {
	if len(items) == 0 {
		return nil
	}
	out := make([]string, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}
