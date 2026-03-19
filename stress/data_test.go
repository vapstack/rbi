package main

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vapstack/rbi"
	bolt "go.etcd.io/bbolt"
)

func TestOpenBenchDBFailsFastOnLockedDB(t *testing.T) {
	path := filepath.Join(t.TempDir(), "locked.db")
	raw, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	defer func() {
		if closeErr := raw.Close(); closeErr != nil {
			t.Fatalf("raw close: %v", closeErr)
		}
	}()

	_, err = OpenBenchDB(DBConfig{
		DBFile:      path,
		OpenTimeout: 25 * time.Millisecond,
	}, 0)
	if err == nil {
		t.Fatalf("OpenBenchDB unexpectedly succeeded on locked db")
	}
	if !strings.Contains(err.Error(), "lock timeout") {
		t.Fatalf("OpenBenchDB error = %q, want lock timeout", err)
	}
}

func TestBuildRBIOptionsMinimizeDeltaProfile(t *testing.T) {
	opts := buildRBIOptions(DBConfig{
		MinimizeDelta:    true,
		AnalyzeInterval:  -1,
		CalibrationOn:    true,
		CalibrationEvery: -1,
		TraceSink:        func(rbi.TraceEvent) {},
		TraceSampleEvery: 17,
	})

	if !opts.EnableAutoBatchStats || !opts.EnableSnapshotStats {
		t.Fatalf("stats flags = autobatch:%t snapshot:%t, want both true", opts.EnableAutoBatchStats, opts.EnableSnapshotStats)
	}
	if opts.AnalyzeInterval != -1 {
		t.Fatalf("AnalyzeInterval = %s, want -1", opts.AnalyzeInterval)
	}
	if !opts.CalibrationEnabled || opts.CalibrationSampleEvery != -1 {
		t.Fatalf("calibration = enabled:%t every:%d, want true/-1", opts.CalibrationEnabled, opts.CalibrationSampleEvery)
	}
	if opts.TraceSink == nil || opts.TraceSampleEvery != 17 {
		t.Fatalf("trace = sink:%v every:%d, want non-nil/17", opts.TraceSink == nil, opts.TraceSampleEvery)
	}
	if opts.SnapshotDeltaCompactFieldKeys != minimizeDeltaCompactFieldKeys {
		t.Fatalf("SnapshotDeltaCompactFieldKeys = %d, want %d", opts.SnapshotDeltaCompactFieldKeys, minimizeDeltaCompactFieldKeys)
	}
	if opts.SnapshotDeltaCompactFieldOps != minimizeDeltaCompactFieldOps {
		t.Fatalf("SnapshotDeltaCompactFieldOps = %d, want %d", opts.SnapshotDeltaCompactFieldOps, minimizeDeltaCompactFieldOps)
	}
	if opts.SnapshotDeltaCompactMaxFieldsPerPublish != minimizeDeltaCompactMaxFieldsPublish {
		t.Fatalf("SnapshotDeltaCompactMaxFieldsPerPublish = %d, want %d", opts.SnapshotDeltaCompactMaxFieldsPerPublish, minimizeDeltaCompactMaxFieldsPublish)
	}
	if opts.SnapshotDeltaCompactUniverseOps != minimizeDeltaCompactUniverseOps {
		t.Fatalf("SnapshotDeltaCompactUniverseOps = %d, want %d", opts.SnapshotDeltaCompactUniverseOps, minimizeDeltaCompactUniverseOps)
	}
	if opts.SnapshotDeltaLayerMaxDepth != minimizeDeltaLayerMaxDepth {
		t.Fatalf("SnapshotDeltaLayerMaxDepth = %d, want %d", opts.SnapshotDeltaLayerMaxDepth, minimizeDeltaLayerMaxDepth)
	}
	if opts.SnapshotCompactorMaxIterationsPerRun != minimizeDeltaCompactorMaxIters {
		t.Fatalf("SnapshotCompactorMaxIterationsPerRun = %d, want %d", opts.SnapshotCompactorMaxIterationsPerRun, minimizeDeltaCompactorMaxIters)
	}
	if opts.SnapshotCompactorRequestEveryNWrites != minimizeDeltaCompactorEveryNWrites {
		t.Fatalf("SnapshotCompactorRequestEveryNWrites = %d, want %d", opts.SnapshotCompactorRequestEveryNWrites, minimizeDeltaCompactorEveryNWrites)
	}
	if opts.SnapshotCompactorIdleInterval != minimizeDeltaCompactorIdleInterval {
		t.Fatalf("SnapshotCompactorIdleInterval = %s, want %s", opts.SnapshotCompactorIdleInterval, minimizeDeltaCompactorIdleInterval)
	}
}
