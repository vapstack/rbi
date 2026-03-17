package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

const (
	colorReset  = "\x1b[0m"
	colorGray   = "\x1b[37m"
	colorYellow = "\x1b[33m"
	colorRed    = "\x1b[31m"
	colorGreen  = "\x1b[32m"
	colorCyan   = "\x1b[36m"
)

type metricKind int

const (
	metricNS metricKind = iota
	metricBytes
	metricAllocs
)

type severity int

const (
	severityGray severity = iota
	severityMild
	severityHigh
	severityExtreme
)

type benchRun struct {
	Iterations  int64
	NSPerOp     float64
	BytesPerOp  float64
	AllocsPerOp float64
}

type benchmarkData struct {
	Name        string
	DisplayName string
	Runs        []benchRun
}

type benchmarkSet struct {
	Order      []string
	Benchmarks map[string]*benchmarkData
}

type benchmarkSummary struct {
	Name        string
	DisplayName string
	Iterations  int64
	NSPerOp     float64
	BytesPerOp  float64
	AllocsPerOp float64
}

type metricDisplay struct {
	Significant bool
	Color       string
	DeltaText   string
}

type outputRow struct {
	Plain   []string
	Colored []string
}

func main() {
	os.Exit(runCLI(os.Args[1:], os.Stdout, os.Stderr, shouldUseColor()))
}

func runCLI(args []string, stdout io.Writer, stderr io.Writer, useColor bool) int {
	if len(args) != 2 {
		_, _ = fmt.Fprintln(stderr, "usage: benchprint <previous.txt> <current.txt>")
		return 2
	}

	previous, err := parseBenchmarkFile(args[0])
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "read %s: %v\n", args[0], err)
		return 1
	}

	current, err := parseBenchmarkFile(args[1])
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "read %s: %v\n", args[1], err)
		return 1
	}

	previousSummaries := summarizeSet(previous)
	currentSummaries := summarizeSet(current)
	rows := buildRows(previousSummaries, current, currentSummaries, useColor)
	if len(rows) == 0 {
		return 0
	}

	_, _ = fmt.Fprintln(stdout, renderRows(rows))
	return 0
}

func shouldUseColor() bool {
	return os.Getenv("NO_COLOR") == ""
}

func parseBenchmarkFile(path string) (benchmarkSet, error) {
	file, err := os.Open(path)
	if err != nil {
		return benchmarkSet{}, err
	}
	defer func() { _ = file.Close() }()

	set := benchmarkSet{
		Order:      make([]string, 0),
		Benchmarks: make(map[string]*benchmarkData),
	}

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "Benchmark") {
			continue
		}

		name, run, ok := parseBenchmarkLine(line)
		if !ok {
			continue
		}

		bench := set.Benchmarks[name]
		if bench == nil {
			bench = &benchmarkData{
				Name:        name,
				DisplayName: normalizeBenchmarkName(name),
				Runs:        make([]benchRun, 0, 1),
			}
			set.Benchmarks[name] = bench
			set.Order = append(set.Order, name)
		}

		bench.Runs = append(bench.Runs, run)
	}

	if err := scanner.Err(); err != nil {
		return benchmarkSet{}, err
	}
	if len(set.Order) == 0 {
		return benchmarkSet{}, errors.New("no benchmark rows found")
	}

	return set, nil
}

func parseBenchmarkLine(line string) (string, benchRun, bool) {
	fields := strings.Fields(line)
	if len(fields) < 4 {
		return "", benchRun{}, false
	}

	iterations, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return "", benchRun{}, false
	}

	metrics := make(map[string]float64, 4)
	for i := 2; i+1 < len(fields); i += 2 {
		value, err := strconv.ParseFloat(fields[i], 64)
		if err != nil {
			continue
		}
		metrics[fields[i+1]] = value
	}

	nsPerOp, okNS := metrics["ns/op"]
	bytesPerOp, okBytes := metrics["B/op"]
	allocsPerOp, okAllocs := metrics["allocs/op"]
	if !okNS || !okBytes || !okAllocs {
		return "", benchRun{}, false
	}

	return fields[0], benchRun{
		Iterations:  iterations,
		NSPerOp:     nsPerOp,
		BytesPerOp:  bytesPerOp,
		AllocsPerOp: allocsPerOp,
	}, true
}

func normalizeBenchmarkName(name string) string {
	name = strings.TrimPrefix(name, "Benchmark")
	return strings.TrimLeft(name, "_")
}

func summarizeSet(set benchmarkSet) map[string]benchmarkSummary {
	summaries := make(map[string]benchmarkSummary, len(set.Order))
	for _, name := range set.Order {
		summaries[name] = summarizeBenchmark(set.Benchmarks[name])
	}
	return summaries
}

func summarizeBenchmark(bench *benchmarkData) benchmarkSummary {
	runs := runsForAggregation(bench.Runs)
	return benchmarkSummary{
		Name:        bench.Name,
		DisplayName: bench.DisplayName,
		Iterations:  maxIterations(runs),
		NSPerOp:     conservativeMetric(runs, func(run benchRun) float64 { return run.NSPerOp }),
		BytesPerOp:  conservativeMetric(runs, func(run benchRun) float64 { return run.BytesPerOp }),
		AllocsPerOp: conservativeMetric(runs, func(run benchRun) float64 { return run.AllocsPerOp }),
	}
}

func runsForAggregation(runs []benchRun) []benchRun {
	filtered := append([]benchRun(nil), runs...)
	if len(filtered) < 3 {
		return filtered
	}

	worst := 0
	for i := 1; i < len(filtered); i++ {
		if isWorseRun(filtered[i], filtered[worst]) {
			worst = i
		}
	}
	return append(filtered[:worst], filtered[worst+1:]...)
}

func isWorseRun(left benchRun, right benchRun) bool {
	if left.NSPerOp != right.NSPerOp {
		return left.NSPerOp > right.NSPerOp
	}
	if left.BytesPerOp != right.BytesPerOp {
		return left.BytesPerOp > right.BytesPerOp
	}
	if left.AllocsPerOp != right.AllocsPerOp {
		return left.AllocsPerOp > right.AllocsPerOp
	}
	return false
}

func maxIterations(runs []benchRun) int64 {
	var m int64
	for _, run := range runs {
		if run.Iterations > m {
			m = run.Iterations
		}
	}
	return m
}

func conservativeMetric(runs []benchRun, pick func(benchRun) float64) float64 {
	values := make([]float64, 0, len(runs))
	var sum float64
	for _, run := range runs {
		value := pick(run)
		values = append(values, value)
		sum += value
	}

	mean := sum / float64(len(values))
	med := median(values)
	if mean > med {
		return mean
	}
	return med
}

func median(values []float64) float64 {
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)

	middle := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[middle]
	}
	return (sorted[middle-1] + sorted[middle]) / 2
}

func buildRows(previousSummaries map[string]benchmarkSummary, current benchmarkSet, currentSummaries map[string]benchmarkSummary, useColor bool) []outputRow {
	rows := make([]outputRow, 0, len(current.Order))

	for _, name := range current.Order {
		currentSummary, ok := currentSummaries[name]
		if !ok {
			continue
		}

		previousSummary, ok := previousSummaries[name]
		if !ok {
			continue
		}

		nsDisplay := compareMetric(metricNS, previousSummary.NSPerOp, currentSummary.NSPerOp)
		bytesDisplay := compareMetric(metricBytes, previousSummary.BytesPerOp, currentSummary.BytesPerOp)
		allocsDisplay := compareMetric(metricAllocs, previousSummary.AllocsPerOp, currentSummary.AllocsPerOp)

		if !nsDisplay.Significant && !bytesDisplay.Significant && !allocsDisplay.Significant {
			continue
		}

		plain := []string{
			currentSummary.DisplayName,
			strconv.FormatInt(currentSummary.Iterations, 10),
			formatMetricValue(metricNS, currentSummary.NSPerOp),
			nsDisplay.DeltaText,
			formatMetricValue(metricBytes, currentSummary.BytesPerOp),
			bytesDisplay.DeltaText,
			formatMetricValue(metricAllocs, currentSummary.AllocsPerOp),
			allocsDisplay.DeltaText,
		}

		colored := []string{
			plain[0],
			plain[1],
			maybeColor(plain[2], nsDisplay.Color, useColor),
			maybeColor(plain[3], nsDisplay.Color, useColor),
			maybeColor(plain[4], bytesDisplay.Color, useColor),
			maybeColor(plain[5], bytesDisplay.Color, useColor),
			maybeColor(plain[6], allocsDisplay.Color, useColor),
			maybeColor(plain[7], allocsDisplay.Color, useColor),
		}

		rows = append(rows, outputRow{Plain: plain, Colored: colored})
	}

	return rows
}

func compareMetric(kind metricKind, previous float64, current float64) metricDisplay {
	if useUnitSteps(kind, previous, current) {
		delta := current - previous
		magnitude := math.Abs(delta)
		return metricDisplay{
			Significant: magnitude >= 0.5,
			Color:       colorFor(stepSeverity(magnitude), current < previous),
			DeltaText:   formatSignedValue(delta),
		}
	}

	change, finite := percentChange(previous, current)
	magnitude := math.Abs(change)
	significant := magnitude >= 0.5
	if !finite {
		significant = previous != current
	}

	return metricDisplay{
		Significant: significant,
		Color:       colorFor(percentSeverity(magnitude), current < previous),
		DeltaText:   formatPercentChange(change, finite, current),
	}
}

func useUnitSteps(kind metricKind, previous float64, current float64) bool {
	if kind == metricNS {
		return false
	}
	return math.Max(previous, current) <= 10
}

func stepSeverity(delta float64) severity {
	switch {
	case delta < 1.5:
		return severityGray
	case delta < 2.5:
		return severityMild
	case delta < 3.5:
		return severityHigh
	default:
		return severityExtreme
	}
}

func percentSeverity(delta float64) severity {
	switch {
	case delta < 1:
		return severityGray
	case delta < 10:
		return severityMild
	case delta <= 20:
		return severityHigh
	default:
		return severityExtreme
	}
}

func colorFor(level severity, improved bool) string {
	switch level {
	case severityGray:
		return colorGray
	case severityMild:
		if improved {
			return colorGreen
		}
		return colorYellow
	case severityHigh:
		if improved {
			return colorGreen
		}
		return colorRed
	default:
		if improved {
			return colorCyan
		}
		return colorRed
	}
}

func percentChange(previous float64, current float64) (float64, bool) {
	if previous == 0 {
		if current == 0 {
			return 0, true
		}
		return math.Inf(1), false
	}
	return ((current - previous) / previous) * 100, true
}

func formatMetricValue(kind metricKind, value float64) string {
	switch kind {
	case metricNS:
		return formatGroupedFloat(value) + "ns/op"
	case metricBytes:
		return formatGroupedFloat(value) + "B/op"
	default:
		return formatGroupedFloat(value) + " allocs/op"
	}
}

func formatFloat(value float64) string {
	abs := math.Abs(value)
	precision := 2
	switch {
	case abs >= 100 || nearlyInteger(value):
		precision = 0
	case abs >= 10:
		precision = 1
	}

	text := strconv.FormatFloat(value, 'f', precision, 64)
	if precision > 0 {
		text = strings.TrimRight(text, "0")
		text = strings.TrimRight(text, ".")
	}
	if text == "-0" {
		return "0"
	}
	return text
}

func formatGroupedFloat(value float64) string {
	text := formatFloat(value)

	sign := ""
	if strings.HasPrefix(text, "-") {
		sign = "-"
		text = text[1:]
	}

	whole, fractional, hasFraction := strings.Cut(text, ".")
	if len(whole) > 3 {
		var builder strings.Builder
		builder.Grow(len(sign) + len(text) + (len(whole)-1)/3)
		builder.WriteString(sign)

		remainder := len(whole) % 3
		if remainder == 0 {
			remainder = 3
		}

		builder.WriteString(whole[:remainder])
		for i := remainder; i < len(whole); i += 3 {
			builder.WriteByte('_')
			builder.WriteString(whole[i : i+3])
		}

		if hasFraction {
			builder.WriteByte('.')
			builder.WriteString(fractional)
		}

		return builder.String()
	}

	if sign == "" {
		return text
	}
	return sign + text
}

func formatSignedValue(value float64) string {
	if value > 0 {
		return "+" + formatFloat(value)
	}
	return formatFloat(value)
}

func formatPercentChange(change float64, finite bool, current float64) string {
	if !finite {
		if current == 0 {
			return "0%"
		}
		return "+inf%"
	}
	if change > 0 {
		return "+" + formatFloat(change) + "%"
	}
	return formatFloat(change) + "%"
}

func nearlyInteger(value float64) bool {
	return math.Abs(value-math.Round(value)) < 1e-9
}

func maybeColor(value string, color string, useColor bool) string {
	if !useColor || color == "" {
		return value
	}
	return color + value + colorReset
}

func renderRows(rows []outputRow) string {
	if len(rows) == 0 {
		return ""
	}

	widths := make([]int, len(rows[0].Plain))
	for _, row := range rows {
		for i, cell := range row.Plain {
			if len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}

	var builder strings.Builder
	for rowIndex, row := range rows {
		for cellIndex, cell := range row.Colored {
			if cellIndex > 0 {
				builder.WriteString("  ")
			}
			if cellIndex == 0 {
				builder.WriteString(padRight(cell, len(row.Plain[cellIndex]), widths[cellIndex]))
				continue
			}
			builder.WriteString(padLeft(cell, len(row.Plain[cellIndex]), widths[cellIndex]))
		}
		if rowIndex < len(rows)-1 {
			builder.WriteByte('\n')
		}
	}

	return builder.String()
}

func padLeft(value string, visibleLen int, width int) string {
	if visibleLen >= width {
		return value
	}
	return strings.Repeat(" ", width-visibleLen) + value
}

func padRight(value string, visibleLen int, width int) string {
	if visibleLen >= width {
		return value
	}
	return value + strings.Repeat(" ", width-visibleLen)
}
