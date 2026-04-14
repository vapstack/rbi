package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
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
		"-cpu-profile", "/tmp/stress.cpu.pprof",
		"-heap-profile", "/tmp/stress.heap.pprof",
		"-pprof-http", ":6060",
		"-class", "r_med,r_meh",
		"-query", "read_leaderboard_top_items,read_moderation_queue_keys",
		"-query-stats",
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
	if opts.CPUProfile != "/tmp/stress.cpu.pprof" {
		t.Fatalf("CPUProfile = %q, want /tmp/stress.cpu.pprof", opts.CPUProfile)
	}
	if opts.HeapProfile != "/tmp/stress.heap.pprof" {
		t.Fatalf("HeapProfile = %q, want /tmp/stress.heap.pprof", opts.HeapProfile)
	}
	if opts.PprofHTTP != ":6060" {
		t.Fatalf("PprofHTTP = %q, want :6060", opts.PprofHTTP)
	}
	if opts.TraceSampleEvery != 64 {
		t.Fatalf("TraceSampleEvery = %d, want 64", opts.TraceSampleEvery)
	}
	if opts.TraceTopN != 7 {
		t.Fatalf("TraceTopN = %d, want 7", opts.TraceTopN)
	}
	if !opts.QueryStats {
		t.Fatal("QueryStats = false, want true")
	}
	if opts.InitialWorkers["read_simple"] != 7 {
		t.Fatalf("read_simple workers = %d, want 7", opts.InitialWorkers["read_simple"])
	}
	if len(opts.ClassFilter) != 2 || opts.ClassFilter[0] != "r_med" || opts.ClassFilter[1] != "r_meh" {
		t.Fatalf("ClassFilter = %v, want [r_med r_meh]", opts.ClassFilter)
	}
	if len(opts.QueryFilter) != 2 || opts.QueryFilter[0] != "read_leaderboard_top_items" || opts.QueryFilter[1] != "read_moderation_queue_keys" {
		t.Fatalf("QueryFilter = %v, want leaderboard/moderation", opts.QueryFilter)
	}
}

func TestParseOptionsGroupWorkerFlags(t *testing.T) {
	catalog, _, _, err := loadClassCatalog()
	if err != nil {
		t.Fatalf("loadClassCatalog: %v", err)
	}

	origArgs := os.Args
	defer func() { os.Args = origArgs }()
	os.Args = []string{
		"stress",
		"-a", "2",
		"-r", "10",
		"-w", "5",
		"-r_smp", "0",
		"-w_hvy", "7",
	}

	opts, err := parseOptions(catalog)
	if err != nil {
		t.Fatalf("parseOptions: %v", err)
	}
	if !opts.WorkerGroups.All.Set || opts.WorkerGroups.All.Value != 2 {
		t.Fatalf("all workers = %+v, want set=2", opts.WorkerGroups.All)
	}
	if !opts.WorkerGroups.Read.Set || opts.WorkerGroups.Read.Value != 10 {
		t.Fatalf("read workers = %+v, want set=10", opts.WorkerGroups.Read)
	}
	if !opts.WorkerGroups.Write.Set || opts.WorkerGroups.Write.Value != 5 {
		t.Fatalf("write workers = %+v, want set=5", opts.WorkerGroups.Write)
	}
	value, ok := opts.InitialWorkers[ClassReadSimple]
	if !ok || value != 0 {
		t.Fatalf("read_simple workers = (%d, %t), want (0, true)", value, ok)
	}
	value, ok = opts.InitialWorkers[ClassWriteHeavy]
	if !ok || value != 7 {
		t.Fatalf("write_heavy workers = (%d, %t), want (7, true)", value, ok)
	}
}

func TestPhaseReportsTrackManualCommandBoundaries(t *testing.T) {
	app := newApp(
		&DBHandle{},
		[]*classDescriptor{
			{
				Info: StressClassInfo{
					ID:    1,
					Alias: "r_idx",
					Name:  ClassReadIndexed,
					Role:  RoleRead,
				},
				Def: ClassDef{Name: ClassReadIndexed, Role: RoleRead},
			},
		},
		time.Second,
		time.Second,
		"/tmp/stress-report.json",
		nil,
		nil,
		false,
		false,
		false,
		nil,
	)

	if err := app.applyCommand("1 0"); err != nil {
		t.Fatalf("applyCommand(1 0): %v", err)
	}
	if err := app.applyCommand("r 0"); err != nil {
		t.Fatalf("applyCommand(r 0): %v", err)
	}

	phases := app.phaseReports(time.Now())
	if len(phases) != 3 {
		t.Fatalf("len(phases) = %d, want 3", len(phases))
	}

	if phases[0].Kind != "initial" || phases[0].EndedByCommand != "1 0" {
		t.Fatalf("phase0 = %+v, want initial ended by first command", phases[0])
	}
	if phases[1].Kind != "manual" || phases[1].StartedByCommand != "1 0" || phases[1].EndedByCommand != "r 0" {
		t.Fatalf("phase1 = %+v, want manual phase between first and second command", phases[1])
	}
	if phases[2].Kind != "manual" || phases[2].StartedByCommand != "r 0" || phases[2].EndedByCommand != "" {
		t.Fatalf("phase2 = %+v, want current manual phase started by second command", phases[2])
	}
}

func TestBuildReport_IncludesFinalIndexStatsOnly(t *testing.T) {
	dir := t.TempDir()
	handle, err := OpenBenchDB(DBConfig{
		DBFile:         filepath.Join(dir, "stress.db"),
		SeedRecords:    0,
		SeedRecordsSet: true,
	}, 0)
	if err != nil {
		t.Fatalf("OpenBenchDB: %v", err)
	}
	t.Cleanup(func() {
		if err := handle.Close(); err != nil {
			t.Fatal(err)
		}
	})

	if err := handle.DB.Set(1, generateUser(NewRand(1), 1)); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	app := newApp(
		handle,
		nil,
		time.Second,
		time.Second,
		filepath.Join(dir, "stress_report.json"),
		nil,
		nil,
		false,
		false,
		false,
		nil,
	)

	report := app.buildReport(false)
	want := handle.DB.IndexStats()
	if !reflect.DeepEqual(report.IndexStats, want) {
		t.Fatalf("report.IndexStats = %+v, want %+v", report.IndexStats, want)
	}

	data, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("Marshal(report): %v", err)
	}
	if got := bytes.Count(data, []byte(`"index_stats"`)); got != 1 {
		t.Fatalf("expected final report JSON to contain exactly one index_stats section, got %d", got)
	}
}
