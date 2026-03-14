package main

import (
	"os"
	"testing"
	"time"

	"github.com/vapstack/rbi"
)

func TestParseWorkerCommand(t *testing.T) {
	tests := []struct {
		input string
		want  workerCommand
		ok    bool
	}{
		{input: "r 1", want: workerCommand{Group: "r", Op: "set", Value: 1}, ok: true},
		{input: "w 2", want: workerCommand{Group: "w", Op: "set", Value: 2}, ok: true},
		{input: "a 0", want: workerCommand{Group: "a", Op: "set", Value: 0}, ok: true},
		{input: "3 10", want: workerCommand{ClassID: 3, Op: "set", Value: 10}, ok: true},
		{input: "1+5", want: workerCommand{ClassID: 1, Op: "add", Value: 5}, ok: true},
		{input: "9-2", want: workerCommand{ClassID: 9, Op: "sub", Value: 2}, ok: true},
		{input: " 7 - 4 ", want: workerCommand{ClassID: 7, Op: "sub", Value: 4}, ok: true},
		{input: "0 1", ok: false},
		{input: "x 1", ok: false},
		{input: "1+", ok: false},
	}

	for _, tt := range tests {
		got, err := parseWorkerCommand(tt.input)
		if tt.ok {
			if err != nil {
				t.Fatalf("parseWorkerCommand(%q) unexpected error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Fatalf("parseWorkerCommand(%q) = %+v, want %+v", tt.input, got, tt.want)
			}
			continue
		}
		if err == nil {
			t.Fatalf("parseWorkerCommand(%q) expected error", tt.input)
		}
	}
}

func TestMakeBatchSampleDelta(t *testing.T) {
	baseline := rbi.AutoBatchStats{
		Submitted:        10,
		ExecutedBatches:  5,
		CoalesceWaitTime: 2 * time.Millisecond,
	}
	current := rbi.AutoBatchStats{
		Submitted:        19,
		ExecutedBatches:  8,
		CoalesceWaitTime: 7 * time.Millisecond,
	}
	sample := makeBatchSample(time.Unix(0, 0), baseline, current)
	if sample.Delta.Submitted != 9 {
		t.Fatalf("Submitted delta = %d, want 9", sample.Delta.Submitted)
	}
	if sample.Delta.ExecutedBatches != 3 {
		t.Fatalf("ExecutedBatches delta = %d, want 3", sample.Delta.ExecutedBatches)
	}
	if sample.Delta.CoalesceWaitTime != 5*time.Millisecond {
		t.Fatalf("CoalesceWaitTime delta = %s, want 5ms", sample.Delta.CoalesceWaitTime)
	}
}

func TestParseOptionsDurationEnablesHeadless(t *testing.T) {
	catalog, _, _, err := loadClassCatalog()
	if err != nil {
		t.Fatalf("loadClassCatalog: %v", err)
	}

	origArgs := os.Args
	defer func() { os.Args = origArgs }()
	os.Args = []string{
		"stress",
		"-duration", "15s",
		"-out", "/tmp/custom-report.json",
		"-trace-sample", "64",
		"-trace-top", "7",
		"-r_smp", "7",
	}

	opts, err := parseOptions(catalog)
	if err != nil {
		t.Fatalf("parseOptions: %v", err)
	}
	if !opts.Headless {
		t.Fatalf("Headless = false, want true")
	}
	if opts.Duration != 15*time.Second {
		t.Fatalf("Duration = %s, want 15s", opts.Duration)
	}
	if opts.ReportPath != "/tmp/custom-report.json" {
		t.Fatalf("ReportPath = %q, want /tmp/custom-report.json", opts.ReportPath)
	}
	if opts.TraceSampleEvery != 64 {
		t.Fatalf("TraceSampleEvery = %d, want 64", opts.TraceSampleEvery)
	}
	if opts.TraceTopN != 7 {
		t.Fatalf("TraceTopN = %d, want 7", opts.TraceTopN)
	}
	if opts.InitialWorkers["read_simple"] != 7 {
		t.Fatalf("read_simple workers = %d, want 7", opts.InitialWorkers["read_simple"])
	}
}
