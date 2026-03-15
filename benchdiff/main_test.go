package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseBenchmarkLineIgnoresExtraMetrics(t *testing.T) {
	t.Parallel()

	line := "Benchmark_Read_Index_Keys_Scan_All_Uint64-16 336 7338198 ns/op 500000 keys/op 112 B/op 1 allocs/op"
	name, run, ok := parseBenchmarkLine(line)
	if !ok {
		t.Fatal("expected benchmark line to parse")
	}

	if name != "Benchmark_Read_Index_Keys_Scan_All_Uint64-16" {
		t.Fatalf("unexpected name: %s", name)
	}
	if run.Iterations != 336 {
		t.Fatalf("unexpected iterations: %d", run.Iterations)
	}
	if run.NSPerOp != 7338198 {
		t.Fatalf("unexpected ns/op: %v", run.NSPerOp)
	}
	if run.BytesPerOp != 112 {
		t.Fatalf("unexpected B/op: %v", run.BytesPerOp)
	}
	if run.AllocsPerOp != 1 {
		t.Fatalf("unexpected allocs/op: %v", run.AllocsPerOp)
	}
}

func TestSummarizeBenchmarkDropsWorstWholeRunBeforeAggregation(t *testing.T) {
	t.Parallel()

	bench := &benchmarkData{
		Name:        "Benchmark__Foo-16",
		DisplayName: normalizeBenchmarkName("Benchmark__Foo-16"),
		Runs: []benchRun{
			{Iterations: 100, NSPerOp: 300, BytesPerOp: 10, AllocsPerOp: 1},
			{Iterations: 250, NSPerOp: 3400, BytesPerOp: 40, AllocsPerOp: 8},
			{Iterations: 200, NSPerOp: 300, BytesPerOp: 10, AllocsPerOp: 1},
			{Iterations: 240, NSPerOp: 300, BytesPerOp: 10, AllocsPerOp: 1},
			{Iterations: 230, NSPerOp: 300, BytesPerOp: 10, AllocsPerOp: 1},
		},
	}

	summary := summarizeBenchmark(bench)
	if summary.DisplayName != "Foo-16" {
		t.Fatalf("unexpected display name: %s", summary.DisplayName)
	}
	if summary.Iterations != 240 {
		t.Fatalf("unexpected iterations after dropping outlier run: %d", summary.Iterations)
	}
	if summary.NSPerOp != 300 {
		t.Fatalf("unexpected ns/op after dropping outlier run: %v", summary.NSPerOp)
	}
	if summary.BytesPerOp != 10 {
		t.Fatalf("unexpected B/op after dropping outlier run: %v", summary.BytesPerOp)
	}
	if summary.AllocsPerOp != 1 {
		t.Fatalf("unexpected allocs/op after dropping outlier run: %v", summary.AllocsPerOp)
	}
}

func TestSummarizeBenchmarkKeepsTwoRunsUntouched(t *testing.T) {
	t.Parallel()

	bench := &benchmarkData{
		Name:        "Benchmark__Foo-16",
		DisplayName: normalizeBenchmarkName("Benchmark__Foo-16"),
		Runs: []benchRun{
			{Iterations: 100, NSPerOp: 300, BytesPerOp: 10, AllocsPerOp: 1},
			{Iterations: 250, NSPerOp: 3400, BytesPerOp: 40, AllocsPerOp: 8},
		},
	}

	summary := summarizeBenchmark(bench)
	if summary.Iterations != 250 {
		t.Fatalf("unexpected iterations when keeping both runs: %d", summary.Iterations)
	}
	if summary.NSPerOp != 1850 {
		t.Fatalf("unexpected ns/op when count < 3: %v", summary.NSPerOp)
	}
	if summary.BytesPerOp != 25 {
		t.Fatalf("unexpected B/op when count < 3: %v", summary.BytesPerOp)
	}
	if summary.AllocsPerOp != 4.5 {
		t.Fatalf("unexpected allocs/op when count < 3: %v", summary.AllocsPerOp)
	}
}

func TestCompareMetricUsesUnitStepsForSmallValues(t *testing.T) {
	t.Parallel()

	display := compareMetric(metricAllocs, 4, 3)
	if !display.Significant {
		t.Fatal("expected difference to be significant")
	}
	if display.Color != colorGray {
		t.Fatalf("unexpected color: %q", display.Color)
	}
	if display.DeltaText != "-1" {
		t.Fatalf("unexpected delta text: %q", display.DeltaText)
	}

	display = compareMetric(metricAllocs, 8, 4)
	if display.Color != colorCyan {
		t.Fatalf("unexpected color for large improvement: %q", display.Color)
	}
	if display.DeltaText != "-4" {
		t.Fatalf("unexpected delta text: %q", display.DeltaText)
	}
}

func TestFormatMetricValueFormatsNSWithoutScaling(t *testing.T) {
	t.Parallel()

	if got := formatMetricValue(metricNS, 1200); got != "1_200ns/op" {
		t.Fatalf("unexpected boundary format: %q", got)
	}
	if got := formatMetricValue(metricNS, 12000); got != "12_000ns/op" {
		t.Fatalf("unexpected grouped format: %q", got)
	}
	if got := formatMetricValue(metricNS, 1234567); got != "1_234_567ns/op" {
		t.Fatalf("unexpected large grouped format: %q", got)
	}
}

func TestFormatMetricValueFormatsBytesWithoutScaling(t *testing.T) {
	t.Parallel()

	if got := formatMetricValue(metricBytes, 1200); got != "1_200B/op" {
		t.Fatalf("unexpected byte boundary format: %q", got)
	}
	if got := formatMetricValue(metricBytes, 12000); got != "12_000B/op" {
		t.Fatalf("unexpected grouped byte format: %q", got)
	}
	if got := formatMetricValue(metricBytes, 1234567); got != "1_234_567B/op" {
		t.Fatalf("unexpected large grouped byte format: %q", got)
	}
}

func TestFormatMetricValueFormatsAllocsWithGrouping(t *testing.T) {
	t.Parallel()

	if got := formatMetricValue(metricAllocs, 12); got != "12 allocs/op" {
		t.Fatalf("unexpected alloc format: %q", got)
	}
	if got := formatMetricValue(metricAllocs, 12000); got != "12_000 allocs/op" {
		t.Fatalf("unexpected grouped alloc format: %q", got)
	}
	if got := formatMetricValue(metricAllocs, 1234.5); got != "1_234 allocs/op" {
		t.Fatalf("unexpected grouped fractional alloc format: %q", got)
	}
}

func TestRenderRowsKeepsColumnsAlignedWithGroupedNSValues(t *testing.T) {
	t.Parallel()

	rows := []outputRow{
		{
			Plain:   []string{"LongBench", "100", "1_234_567ns/op", "+10%", "2_345_678B/op", "+2%", "12_345 allocs/op", "+4"},
			Colored: []string{"LongBench", "100", "1_234_567ns/op", "+10%", "2_345_678B/op", "+2%", "12_345 allocs/op", "+4"},
		},
		{
			Plain:   []string{"Short", "10", "900ns/op", "-10%", "2B/op", "-3%", "1 allocs/op", "-5"},
			Colored: []string{"Short", "10", "900ns/op", "-10%", "2B/op", "-3%", "1 allocs/op", "-5"},
		},
	}

	lines := strings.Split(renderRows(rows), "\n")
	if len(lines) != 2 {
		t.Fatalf("unexpected row count: %d", len(lines))
	}

	if got, want := strings.Index(lines[0], "+10%"), strings.Index(lines[1], "-10%"); got != want {
		t.Fatalf("delta column is not aligned:\n%s\n%s", lines[0], lines[1])
	}
	if got, want := strings.Index(lines[0], "+2%"), strings.Index(lines[1], "-3%"); got != want {
		t.Fatalf("byte delta column is not aligned:\n%s\n%s", lines[0], lines[1])
	}
	if got, want := strings.Index(lines[0], "+4"), strings.Index(lines[1], "-5"); got != want {
		t.Fatalf("alloc delta column is not aligned:\n%s\n%s", lines[0], lines[1])
	}
}

func TestRunCLIRendersOnlyMeaningfulRows(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	previous := filepath.Join(dir, "previous.txt")
	current := filepath.Join(dir, "current.txt")

	previousText := strings.Join([]string{
		"Benchmark__Foo-16 100 1000 ns/op 100 B/op 1 allocs/op",
		"Benchmark__Bar-16 100 2000 ns/op 200 B/op 2 allocs/op",
	}, "\n")
	currentText := strings.Join([]string{
		"Benchmark__Foo-16 120 1004 ns/op 100 B/op 1 allocs/op",
		"Benchmark__Bar-16 90 1600 ns/op 204 B/op 1 allocs/op",
		"Benchmark__Bar-16 110 1700 ns/op 206 B/op 1 allocs/op",
		"Benchmark__Bar-16 100 1500 ns/op 205 B/op 1 allocs/op",
	}, "\n")

	if err := os.WriteFile(previous, []byte(previousText), 0o644); err != nil {
		t.Fatalf("write previous: %v", err)
	}
	if err := os.WriteFile(current, []byte(currentText), 0o644); err != nil {
		t.Fatalf("write current: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := runCLI([]string{previous, current}, &stdout, &stderr, false)
	if code != 0 {
		t.Fatalf("unexpected exit code %d, stderr=%q", code, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %q", stderr.String())
	}

	output := stdout.String()
	if strings.Contains(output, "Foo-16") {
		t.Fatalf("expected insignificant benchmark to be filtered out: %q", output)
	}

	expectedFragments := []string{
		"Bar-16",
		"100",
		"1_550ns/op",
		"-22.5%",
		"204B/op",
		"+2.25%",
		"1 allocs/op",
		"-1",
	}
	for _, fragment := range expectedFragments {
		if !strings.Contains(output, fragment) {
			t.Fatalf("expected output to contain %q, got %q", fragment, output)
		}
	}
}
