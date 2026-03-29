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
	DBFile           string
	ReportPath       string
	CPUProfile       string
	HeapProfile      string
	PprofHTTP        string
	EmailSampleN     int
	BoltNoSync       bool
	AnalyzeInterval  time.Duration
	RefreshEvery     time.Duration
	TelemetryEvery   time.Duration
	TraceSampleEvery int
	TraceTopN        int
	QueryStats       bool
	Jitter           bool
	Duration         time.Duration
	Headless         bool
	ClassFilter      []string
	QueryFilter      []string
	WorkerGroups     workerGroupOverrides
	InitialWorkers   map[string]int
}

func parseOptions(catalog []*classDescriptor) (options, error) {
	opts := options{
		DBFile:           DefaultDBFilename,
		ReportPath:       "stress_report.json",
		EmailSampleN:     DefaultEmailSampleN,
		RefreshEvery:     2 * time.Second,
		TelemetryEvery:   4 * time.Second,
		TraceSampleEvery: -1,
		TraceTopN:        24,
		InitialWorkers:   make(map[string]int, len(catalog)),
	}

	fs := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	fs.StringVar(&opts.DBFile, "db", opts.DBFile, "path to bolt db file")
	fs.StringVar(&opts.ReportPath, "report", opts.ReportPath, "path to JSON report file")
	fs.StringVar(&opts.ReportPath, "out", opts.ReportPath, "path to JSON report file")
	fs.StringVar(&opts.CPUProfile, "cpu-profile", "", "write CPU profile to file")
	fs.StringVar(&opts.HeapProfile, "heap-profile", "", "write heap profile to file at process end")
	fs.StringVar(&opts.PprofHTTP, "pprof-http", "", "listen address for net/http/pprof (e.g. :6060)")
	fs.IntVar(&opts.EmailSampleN, "email-sample", opts.EmailSampleN, "how many existing emails to sample for indexed reads")
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
