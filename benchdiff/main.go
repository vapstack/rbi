package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	colorReset  = "\x1b[0m"
	colorGray   = "\x1b[90m"
	colorWhite  = "\x1b[97m"
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
	severityMuted severity = iota
	severityNeutral
	severityMild
	severityHigh
	severityExtreme
)

type severityThresholds struct {
	significant float64
	mutedMax    float64
	neutralMax  float64
	mildMax     float64
	highMax     float64
}

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

type cliOptions struct {
	follow bool
	hide   bool
}

type followReader struct {
	path        string
	offset      int64
	partial     string
	pendingName string
}

var (
	errFollowInterrupted = errors.New("follow interrupted")
	errFileTruncated     = errors.New("followed file truncated")
)

func main() {
	os.Exit(runCLI(os.Args[1:], os.Stdout, os.Stderr, shouldUseColor()))
}

func runCLI(args []string, stdout io.Writer, stderr io.Writer, useColor bool) int {
	options, paths, helpShown, code := parseCLIArgs(args, stderr)
	if code != 0 {
		return code
	}
	if helpShown {
		return 0
	}

	previous, err := parseBenchmarkFile(paths[0])
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "read %s: %v\n", paths[0], err)
		return 1
	}

	previousSummaries := summarizeSet(previous)
	if options.follow {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
		defer stop()

		err = runFollowCLI(previousSummaries, paths[1], stdout, options.hide, useColor, time.Second, ctx.Done())
		if err == nil {
			return 0
		}
		if errors.Is(err, errFollowInterrupted) {
			return 130
		}
		_, _ = fmt.Fprintf(stderr, "read %s: %v\n", paths[1], err)
		return 1
	}

	current, err := parseBenchmarkFile(paths[1])
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "read %s: %v\n", paths[1], err)
		return 1
	}

	currentSummaries := summarizeSet(current)
	rows := buildRows(previousSummaries, current.Order, currentSummaries, options.hide, useColor)
	if len(rows) == 0 {
		return 0
	}

	_, _ = fmt.Fprintln(stdout, renderRows(rows))
	return 0
}

func parseCLIArgs(args []string, stderr io.Writer) (cliOptions, []string, bool, int) {
	flags := flag.NewFlagSet("benchdiff", flag.ContinueOnError)
	flags.SetOutput(stderr)

	options := cliOptions{}
	flags.BoolVar(&options.follow, "f", false, "follow the current benchmark file")
	flags.BoolVar(&options.hide, "h", false, "hide insignificant rows")

	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return cliOptions{}, nil, true, 0
		}
		return cliOptions{}, nil, false, 2
	}

	paths := flags.Args()
	if len(paths) != 2 {
		_, _ = fmt.Fprintln(stderr, "usage: benchdiff [-f] [-h] <previous.txt> <current.txt>")
		return cliOptions{}, nil, false, 2
	}

	return options, paths, false, 0
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

func buildRows(previousSummaries map[string]benchmarkSummary, order []string, currentSummaries map[string]benchmarkSummary, hideInsignificant bool, useColor bool) []outputRow {
	rows := make([]outputRow, 0, len(order))

	for _, name := range order {
		currentSummary, ok := currentSummaries[name]
		if !ok {
			continue
		}

		previousSummary, ok := previousSummaries[name]
		if !ok {
			rows = append(rows, buildNewBenchmarkRow(currentSummary, useColor))
			continue
		}

		nsDisplay := compareMetric(metricNS, previousSummary.NSPerOp, currentSummary.NSPerOp)
		bytesDisplay := compareMetric(metricBytes, previousSummary.BytesPerOp, currentSummary.BytesPerOp)
		allocsDisplay := compareMetric(metricAllocs, previousSummary.AllocsPerOp, currentSummary.AllocsPerOp)

		if hideInsignificant && !nsDisplay.Significant && !bytesDisplay.Significant && !allocsDisplay.Significant {
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

func buildNewBenchmarkRow(currentSummary benchmarkSummary, useColor bool) outputRow {
	plain := []string{
		currentSummary.DisplayName,
		strconv.FormatInt(currentSummary.Iterations, 10),
		formatMetricValue(metricNS, currentSummary.NSPerOp),
		"0%",
		formatMetricValue(metricBytes, currentSummary.BytesPerOp),
		"0%",
		formatMetricValue(metricAllocs, currentSummary.AllocsPerOp),
		"0%",
	}

	colored := []string{
		plain[0],
		plain[1],
		maybeColor(plain[2], colorWhite, useColor),
		maybeColor(plain[3], colorGray, useColor),
		maybeColor(plain[4], colorWhite, useColor),
		maybeColor(plain[5], colorGray, useColor),
		maybeColor(plain[6], colorWhite, useColor),
		maybeColor(plain[7], colorGray, useColor),
	}

	return outputRow{Plain: plain, Colored: colored}
}

func runFollowCLI(previousSummaries map[string]benchmarkSummary, currentPath string, stdout io.Writer, hideInsignificant bool, useColor bool, pollInterval time.Duration, interrupted <-chan struct{}) error {
	current := newBenchmarkSet()
	reader := followReader{path: currentPath}
	widths := estimateFollowWidths(previousSummaries)

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		changedNames, stop, err := reader.poll(&current)
		if err != nil {
			if errors.Is(err, errFileTruncated) {
				reader.reset()
				current = newBenchmarkSet()
				continue
			}
			return err
		}

		if len(changedNames) > 0 {
			currentSummaries := summarizeSet(current)
			rows := buildRows(previousSummaries, changedNames, currentSummaries, hideInsignificant, useColor)
			if len(rows) > 0 {
				_, _ = fmt.Fprintln(stdout, renderRowsWithWidths(rows, widths))
			}
		}

		if stop {
			return nil
		}

		select {
		case <-interrupted:
			return errFollowInterrupted
		case <-ticker.C:
		}
	}
}

func newBenchmarkSet() benchmarkSet {
	return benchmarkSet{
		Order:      make([]string, 0),
		Benchmarks: make(map[string]*benchmarkData),
	}
}

func appendBenchmarkRun(set *benchmarkSet, name string, run benchRun) {
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

func (reader *followReader) poll(current *benchmarkSet) ([]string, bool, error) {
	data, err := reader.readNew()
	if err != nil {
		return nil, false, err
	}
	if len(data) == 0 {
		if reader.partial != "" && !strings.HasPrefix(strings.TrimSpace(reader.partial), "Benchmark") {
			return reader.consume(current, "\n")
		}
		return nil, false, nil
	}
	return reader.consume(current, string(data))
}

func (reader *followReader) readNew() ([]byte, error) {
	file, err := os.Open(reader.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer func() { _ = file.Close() }()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() < reader.offset {
		return nil, errFileTruncated
	}
	if _, err = file.Seek(reader.offset, io.SeekStart); err != nil {
		return nil, err
	}

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	reader.offset += int64(len(data))
	return data, nil
}

func (reader *followReader) consume(current *benchmarkSet, chunk string) ([]string, bool, error) {
	text := reader.partial + chunk
	lines := strings.Split(text, "\n")

	if strings.HasSuffix(text, "\n") {
		reader.partial = ""
		lines = lines[:len(lines)-1]
	} else {
		reader.partial = lines[len(lines)-1]
		lines = lines[:len(lines)-1]
	}

	completedNames := make([]string, 0)

	for _, rawLine := range lines {
		line := strings.TrimSpace(rawLine)
		name, run, ok := parseBenchmarkLine(line)
		if !ok {
			if reader.pendingName == "" && len(current.Order) == 0 {
				continue
			}
			if reader.pendingName != "" {
				completedNames = append(completedNames, reader.pendingName)
				reader.pendingName = ""
			}
			return completedNames, true, nil
		}

		appendBenchmarkRun(current, name, run)
		if reader.pendingName == "" {
			reader.pendingName = name
			continue
		}
		if name == reader.pendingName {
			continue
		}

		completedNames = append(completedNames, reader.pendingName)
		reader.pendingName = name
	}

	return completedNames, false, nil
}

func (reader *followReader) reset() {
	reader.offset = 0
	reader.partial = ""
	reader.pendingName = ""
}

func compareMetric(kind metricKind, previous float64, current float64) metricDisplay {
	if useUnitSteps(kind, previous, current) {
		delta := current - previous
		magnitude := math.Abs(delta)
		thresholds := stepThresholds()
		return metricDisplay{
			Significant: magnitude >= thresholds.significant,
			Color:       colorFor(severityFor(magnitude, thresholds), current < previous),
			DeltaText:   formatSignedValue(delta),
		}
	}

	change, finite := percentChange(previous, current)
	magnitude := math.Abs(change)
	thresholds := percentThresholds(kind)
	significant := magnitude >= thresholds.significant
	if !finite {
		significant = previous != current
	}

	return metricDisplay{
		Significant: significant,
		Color:       colorFor(severityFor(magnitude, thresholds), current < previous),
		DeltaText:   formatPercentChange(change, finite, current),
	}
}

func useUnitSteps(kind metricKind, previous float64, current float64) bool {
	if kind == metricNS {
		return false
	}
	return math.Max(previous, current) <= 10
}

func stepThresholds() severityThresholds {
	return severityThresholds{
		significant: 0.5,
		mutedMax:    1.5,
		neutralMax:  1.5,
		mildMax:     2.5,
		highMax:     3.5,
	}
}

func percentThresholds(kind metricKind) severityThresholds {
	switch kind {
	case metricNS, metricBytes:
		return severityThresholds{
			significant: 1,
			mutedMax:    2.5,
			neutralMax:  5,
			mildMax:     15,
			highMax:     25,
		}
	default:
		return severityThresholds{
			significant: 0.5,
			mutedMax:    1,
			neutralMax:  1,
			mildMax:     10,
			highMax:     20,
		}
	}
}

func severityFor(delta float64, thresholds severityThresholds) severity {
	switch {
	case delta < thresholds.mutedMax:
		return severityMuted
	case delta < thresholds.neutralMax:
		return severityNeutral
	case delta < thresholds.mildMax:
		return severityMild
	case delta <= thresholds.highMax:
		return severityHigh
	default:
		return severityExtreme
	}
}

func colorFor(level severity, improved bool) string {
	switch level {
	case severityMuted:
		return colorGray
	case severityNeutral:
		return ""
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
	return renderRowsWithWidths(rows, nil)
}

func renderRowsWithWidths(rows []outputRow, minWidths []int) string {
	if len(rows) == 0 {
		return ""
	}

	widths := measureRowWidths(rows)
	for i, width := range minWidths {
		if i >= len(widths) {
			break
		}
		if width > widths[i] {
			widths[i] = width
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

func measureRowWidths(rows []outputRow) []int {
	widths := make([]int, len(rows[0].Plain))
	for _, row := range rows {
		for i, cell := range row.Plain {
			if len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}
	return widths
}

func estimateFollowWidths(previousSummaries map[string]benchmarkSummary) []int {
	if len(previousSummaries) == 0 {
		return nil
	}

	widths := make([]int, 8)
	for _, summary := range previousSummaries {
		cells := []string{
			summary.DisplayName,
			strconv.FormatInt(summary.Iterations, 10),
			formatMetricValue(metricNS, summary.NSPerOp),
			estimateDeltaText(metricNS, summary.NSPerOp),
			formatMetricValue(metricBytes, summary.BytesPerOp),
			estimateDeltaText(metricBytes, summary.BytesPerOp),
			formatMetricValue(metricAllocs, summary.AllocsPerOp),
			estimateDeltaText(metricAllocs, summary.AllocsPerOp),
		}

		for i, cell := range cells {
			if len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}

	for i := range widths {
		if widths[i] > 0 {
			widths[i]++
		}
	}

	return widths
}

func estimateDeltaText(kind metricKind, previous float64) string {
	if useUnitSteps(kind, previous, previous) {
		return formatSignedValue(previous)
	}
	if previous == 0 {
		return "+inf%"
	}

	widest := ""
	for _, current := range []float64{0, previous * 2} {
		change, finite := percentChange(previous, current)
		candidate := formatPercentChange(change, finite, current)
		if len(candidate) > len(widest) {
			widest = candidate
		}
	}
	return widest
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
