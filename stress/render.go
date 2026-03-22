package main

import (
	"fmt"
	"os"
	"strings"
	"time"
)

type renderer struct {
	out         *os.File
	interactive bool
}

func newRenderer(out *os.File, interactive bool) *renderer {
	r := &renderer{out: out, interactive: interactive}
	if interactive {
		_, _ = fmt.Fprint(out, "\x1b[?1049h\x1b[2J\x1b[H\x1b[?25l")
	}
	return r
}

func (r *renderer) Close() error {
	if r.interactive {
		_, _ = fmt.Fprint(r.out, "\x1b[?25h\x1b[?1049l")
	}
	return nil
}

func (r *renderer) render(snapshot viewSnapshot, inputBuffer, status string) error {
	var b strings.Builder
	if r.interactive {
		b.WriteString("\x1b[H\x1b[2J")
	}

	b.WriteString(fmt.Sprintf("started %s  uptime=%s\n", snapshot.StartedAt.Format(time.RFC3339), time.Since(snapshot.StartedAt).Round(time.Second)))
	b.WriteString("# item                  wrks      cTPS      aTPS      mTPS       p50       p95       p99\n")
	b.WriteString(strings.Repeat("-", 96))
	b.WriteByte('\n')

	for _, class := range snapshot.Classes {
		b.WriteString(formatScopeRow(
			fmt.Sprintf("%d %s", class.ID, class.Alias),
			fmt.Sprintf("[%4d]", class.ActiveWorkers),
			class.Stats,
		))
		b.WriteByte('\n')
		for _, query := range class.Queries {
			label := fmt.Sprintf("  %3s %s", formatWeight(query.Weight), compactQueryName(query.Name))
			b.WriteString(formatScopeRow(label, "", query.Stats))
			b.WriteByte('\n')
		}
	}

	b.WriteString(strings.Repeat("-", 96))
	b.WriteByte('\n')
	b.WriteString(formatTotalRow("read", countWorkers(snapshot.Classes, RoleRead), snapshot.Totals.Read))
	b.WriteByte('\n')
	b.WriteString(formatTotalRow("write", countWorkers(snapshot.Classes, RoleWrite), snapshot.Totals.Write))
	b.WriteByte('\n')
	b.WriteString(formatTotalRow("total", countWorkers(snapshot.Classes, ""), snapshot.Totals.Total))
	b.WriteByte('\n')
	b.WriteString(strings.Repeat("-", 96))
	b.WriteByte('\n')
	b.WriteString(formatMemoryLine(snapshot.Memory))
	b.WriteByte('\n')
	b.WriteString(formatSnapshotLine(snapshot.Snapshot))
	b.WriteByte('\n')
	b.WriteString(formatBatchLine(snapshot.Batch))
	b.WriteByte('\n')
	b.WriteString(formatTraceLine(snapshot.Planner))
	b.WriteByte('\n')
	if topLine := formatTraceTopLine(snapshot.Planner); topLine != "" {
		b.WriteString(topLine)
		b.WriteByte('\n')
	}
	b.WriteString(strings.Repeat("-", 96))
	b.WriteByte('\n')
	if status != "" {
		b.WriteString(status)
		b.WriteByte('\n')
	}
	b.WriteString("> ")
	b.WriteString(inputBuffer)

	_, err := fmt.Fprint(r.out, b.String())
	return err
}

func formatScopeRow(label, workers string, scope scopeReport) string {
	return fmt.Sprintf(
		"%-22s %6s %9s %9s %9s %9s %9s %9s",
		label,
		workers,
		formatTPS(scope.CurrentTPS),
		formatTPS(scope.AverageTPS),
		formatTPS(scope.MinTPS),
		formatLatency(scope.Latency.P50Us),
		formatLatency(scope.Latency.P95Us),
		formatLatency(scope.Latency.P99Us),
	)
}

func formatTotalRow(label string, workers int, scope scopeReport) string {
	return fmt.Sprintf(
		"%-22s %6s %9s %9s %9s %9s %9s %9s",
		label,
		fmt.Sprintf("[%4d]", workers),
		formatTPS(scope.CurrentTPS),
		formatTPS(scope.AverageTPS),
		formatTPS(scope.MinTPS),
		"",
		"",
		"",
	)
}

func formatMemoryLine(memory *MemorySnapshot) string {
	if memory == nil {
		return "mem     n/a"
	}
	return fmt.Sprintf(
		"mem     heap=%s anon=%s dirty=%s dbmap=%s gc=%d next=%s",
		formatBytes(memory.Go.HeapAllocBytes),
		formatBytes(memory.Process.AnonymousBytes),
		formatBytes(memory.Process.PrivateDirtyBytes),
		formatBytes(memory.Process.BenchDBMapRSSBytes),
		memory.Go.NumGC,
		formatBytes(memory.Go.NextGCBytes),
	)
}

func formatSnapshotLine(sample snapshotSample) string {
	stats := sample.Stats
	return fmt.Sprintf(
		"snap    pin=%d reg=%d depth=%d/%d ops=%s/%s cq=%d busy=%s miss=%s soft=%s skip=%s",
		stats.PinnedRefs,
		stats.RegistrySize,
		stats.IndexLayerDepth,
		stats.LenLayerDepth,
		formatOps(float64(stats.IndexDeltaOps)),
		formatOps(float64(stats.LenDeltaOps)),
		stats.CompactorQueueLen,
		formatOps(float64(stats.CompactorPreclaimBusy)),
		formatOps(float64(stats.CompactorLockMiss)),
		formatOps(float64(stats.CompactorSoftSkip)),
		formatOps(float64(stats.CompactorSkippedWake)),
	)
}

func formatBatchLine(sample batchSample) string {
	stats := sample.Stats
	delta := sample.Delta
	return fmt.Sprintf(
		"batch   q=%d/%d exec=%s multi=%s dist=%d/%d/%d/%d avg=%.1f hot=%t err=%d",
		stats.QueueLen,
		stats.MaxQueue,
		formatOps(float64(delta.ExecutedBatches)),
		formatOps(float64(delta.MultiRequestOps)),
		delta.BatchSize1,
		delta.BatchSize2To4,
		delta.BatchSize5To8,
		delta.BatchSize9Plus,
		stats.AvgBatchSize,
		stats.HotWindowActive,
		delta.TxCommitErrors+delta.TxOpErrors+delta.TxBeginErrors+delta.CallbackErrors,
	)
}

func formatTraceLine(report *plannerTraceReport) string {
	if report == nil || !report.Enabled {
		return "trace   off"
	}
	if report.Total.Sampled == 0 {
		return fmt.Sprintf("trace   samp=%s seen=0", formatSampleEvery(report.SampleEvery))
	}
	return fmt.Sprintf(
		"trace   samp=%s seen=%s avg=%s exam=%s match=%s mat=%s exact=%s scan=%s fb=%d",
		formatSampleEvery(report.SampleEvery),
		formatOps(float64(report.Total.Sampled)),
		formatLatency(report.Total.AvgDurationUs),
		formatOps(report.Total.AvgRowsExamined),
		formatOps(report.Total.AvgRowsMatched),
		formatOps(report.Total.AvgBitmapMats),
		formatOps(report.Total.AvgBitmapExact),
		formatOps(report.Total.AvgOrderScanWidth),
		report.Total.RuntimeFallbacks,
	)
}

func formatTraceTopLine(report *plannerTraceReport) string {
	if report == nil || len(report.TopSamples) == 0 {
		return ""
	}
	top := report.TopSamples[0]
	return fmt.Sprintf(
		"toptr   %s %s plan=%s exam=%s match=%s ret=%s",
		formatLatency(top.DurationUs),
		compactQueryName(top.Query),
		top.Plan,
		formatOps(float64(top.RowsExamined)),
		formatOps(float64(top.RowsMatched)),
		formatOps(float64(top.RowsReturned)),
	)
}

func compactQueryName(name string) string {
	label := strings.TrimPrefix(name, "read_user_")
	label = strings.TrimPrefix(label, "read_")
	label = strings.TrimPrefix(label, "write_")
	label = strings.TrimSuffix(label, "_items")
	label = strings.TrimSuffix(label, "_keys")
	label = strings.TrimSuffix(label, "_count")
	label = strings.TrimSuffix(label, "_existing")
	label = strings.TrimSuffix(label, "_signup")
	if len(label) > 16 {
		label = label[:16]
	}
	return label
}

func formatWeight(weight float64) string {
	if weight <= 0 {
		return ""
	}
	return fmt.Sprintf("%2.0f%%", weight*100)
}

func formatSampleEvery(value int) string {
	if value == 0 {
		return "all"
	}
	return fmt.Sprintf("%d", value)
}

func formatTPS(value float64) string {
	switch {
	case value >= 1_000_000:
		return fmt.Sprintf("%.1fM", value/1_000_000)
	case value >= 10_000:
		return fmt.Sprintf("%.0fK", value/1_000)
	case value >= 1_000:
		return fmt.Sprintf("%.1fK", value/1_000)
	case value >= 100:
		return fmt.Sprintf("%.0f", value)
	case value >= 10:
		return fmt.Sprintf("%.1f", value)
	default:
		return fmt.Sprintf("%.2f", value)
	}
}

func formatOps(value float64) string {
	if value >= 1_000 {
		return formatTPS(value)
	}
	return fmt.Sprintf("%.0f", value)
}

func formatLatency(us float64) string {
	switch {
	case us == 0:
		return "0"
	case us < 1000:
		return fmt.Sprintf("%.0fus", us)
	case us < 1_000_000:
		return fmt.Sprintf("%.1fms", us/1000.0)
	default:
		return fmt.Sprintf("%.1fs", us/1_000_000.0)
	}
}

func formatBytes(value uint64) string {
	const unit = 1024.0
	if value == 0 {
		return "0"
	}
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	v := float64(value)
	idx := 0
	for v >= unit && idx < len(units)-1 {
		v /= unit
		idx++
	}
	if idx == 0 {
		return fmt.Sprintf("%.0f%s", v, units[idx])
	}
	return fmt.Sprintf("%.1f%s", v, units[idx])
}

func countWorkers(classes []classReport, role string) int {
	total := 0
	for _, class := range classes {
		if role != "" && class.Role != role {
			continue
		}
		total += class.ActiveWorkers
	}
	return total
}
