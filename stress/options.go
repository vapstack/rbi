package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

type options struct {
	DBFile           string
	ReportPath       string
	EmailSampleN     int
	BoltNoSync       bool
	AnalyzeInterval  time.Duration
	RefreshEvery     time.Duration
	TelemetryEvery   time.Duration
	TraceSampleEvery int
	TraceTopN        int
	Duration         time.Duration
	Headless         bool
	InitialWorkers   map[string]int
}

func parseOptions(catalog []*classDescriptor) (options, error) {
	opts := options{
		DBFile:           DefaultDBFilename,
		ReportPath:       "stress_report.json",
		EmailSampleN:     DefaultEmailSampleN,
		RefreshEvery:     2 * time.Second,
		TelemetryEvery:   4 * time.Second,
		TraceSampleEvery: 256,
		TraceTopN:        24,
		InitialWorkers:   make(map[string]int, len(catalog)),
	}

	fs := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	fs.StringVar(&opts.DBFile, "db", opts.DBFile, "path to bolt db file")
	fs.StringVar(&opts.ReportPath, "report", opts.ReportPath, "path to JSON report file")
	fs.StringVar(&opts.ReportPath, "out", opts.ReportPath, "path to JSON report file")
	fs.IntVar(&opts.EmailSampleN, "email-sample", opts.EmailSampleN, "how many existing emails to sample for indexed reads")
	fs.BoolVar(&opts.BoltNoSync, "bolt-no-sync", false, "open bbolt with NoSync=true (unsafe)")
	fs.DurationVar(&opts.AnalyzeInterval, "analyze-interval", 0, "rbi analyze interval (0=default, <0 disable)")
	fs.DurationVar(&opts.RefreshEvery, "refresh", opts.RefreshEvery, "table refresh interval")
	fs.DurationVar(&opts.TelemetryEvery, "telemetry", opts.TelemetryEvery, "memory/snapshot/batch sampling interval")
	fs.IntVar(&opts.TraceSampleEvery, "trace-sample", opts.TraceSampleEvery, "planner trace sampling (-1 disable, 0 every query, N every Nth query)")
	fs.IntVar(&opts.TraceTopN, "trace-top", opts.TraceTopN, "how many slowest sampled planner traces to keep in the report")
	fs.DurationVar(&opts.Duration, "duration", 0, "fixed run duration; when >0, stress runs in headless mode")
	fs.BoolVar(&opts.Headless, "headless", false, "run without interactive UI")
	fs.BoolVar(&opts.Headless, "no-ui", false, "run without interactive UI")

	workerFlags := make(map[string]*int, len(catalog))
	for _, class := range catalog {
		value := new(int)
		desc := fmt.Sprintf("initial workers for %s (%s, default workers %d)", class.Info.Alias, class.Info.Name, class.Info.DefaultWorkers)
		fs.IntVar(value, class.Info.Alias, 0, desc)
		fs.IntVar(value, class.Info.Name, 0, desc)
		workerFlags[class.Info.Name] = value
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

	for _, class := range catalog {
		value := *workerFlags[class.Info.Name]
		if value < 0 {
			return options{}, fmt.Errorf("%s workers must be >= 0", class.Info.Alias)
		}
		opts.InitialWorkers[class.Info.Name] = value
	}
	return opts, nil
}
