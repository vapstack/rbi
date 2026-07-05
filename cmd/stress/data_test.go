package main

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vapstack/rbi/rbitrace"
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

func TestBuildRBIOptions(t *testing.T) {
	opts := buildRBIOptions(DBConfig{
		AnalyzeInterval:      -1,
		DisableRuntimeCaches: true,
		TraceSink:            func(rbitrace.Event) {},
		TraceSampleEvery:     17,
	})

	if opts.AnalyzeInterval != -1 {
		t.Fatalf("AnalyzeInterval = %s, want -1", opts.AnalyzeInterval)
	}
	if opts.MaterializedPredicateCacheMaxEntries != -1 {
		t.Fatalf("MaterializedPredCacheMaxEntries = %d, want -1", opts.MaterializedPredicateCacheMaxEntries)
	}
	if opts.NumericRangeBucketSize != -1 {
		t.Fatalf("NumericRangeBucketSize = %d, want -1", opts.NumericRangeBucketSize)
	}
	if opts.NumericRangeBucketMinFieldKeys != -1 {
		t.Fatalf("NumericRangeBucketMinFieldKeys = %d, want -1", opts.NumericRangeBucketMinFieldKeys)
	}
	if opts.NumericRangeBucketMinSpanKeys != -1 {
		t.Fatalf("NumericRangeBucketMinSpanKeys = %d, want -1", opts.NumericRangeBucketMinSpanKeys)
	}
	if opts.TraceSink == nil || opts.TraceSampleEvery != 17 {
		t.Fatalf("trace = sink:%v every:%d, want non-nil/17", opts.TraceSink == nil, opts.TraceSampleEvery)
	}
}

func TestOpenBenchDBSeedsEmptyDBToExplicitTarget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seed_target.db")
	handle, err := OpenBenchDB(DBConfig{
		DBFile:         path,
		SeedRecords:    7,
		SeedRecordsSet: true,
	}, 0)
	if err != nil {
		t.Fatalf("OpenBenchDB: %v", err)
	}
	defer func() {
		if closeErr := handle.Close(); closeErr != nil {
			t.Fatalf("handle close: %v", closeErr)
		}
	}()

	if got := handle.StartRecords; got != 7 {
		t.Fatalf("StartRecords = %d, want 7", got)
	}
	if got := handle.MaxID; got != 7 {
		t.Fatalf("MaxID = %d, want 7", got)
	}
	if got, err := stressReadCount(handle.Collection); err != nil {
		t.Fatalf("Count(): %v", err)
	} else if got != 7 {
		t.Fatalf("Count() = %d, want 7", got)
	}
}

func TestOpenBenchDBTopUpToExplicitTarget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seed_top_up.db")

	handle, err := OpenBenchDB(DBConfig{
		DBFile:         path,
		SeedRecords:    5,
		SeedRecordsSet: true,
	}, 0)
	if err != nil {
		t.Fatalf("OpenBenchDB(initial): %v", err)
	}
	if err := handle.Close(); err != nil {
		t.Fatalf("initial close: %v", err)
	}

	handle, err = OpenBenchDB(DBConfig{
		DBFile:         path,
		SeedRecords:    8,
		SeedRecordsSet: true,
	}, 0)
	if err != nil {
		t.Fatalf("OpenBenchDB(top-up): %v", err)
	}
	defer func() {
		if closeErr := handle.Close(); closeErr != nil {
			t.Fatalf("handle close: %v", closeErr)
		}
	}()

	if got := handle.StartRecords; got != 8 {
		t.Fatalf("StartRecords = %d, want 8", got)
	}
	if got := handle.MaxID; got != 8 {
		t.Fatalf("MaxID = %d, want 8", got)
	}
	if got, err := stressReadCount(handle.Collection); err != nil {
		t.Fatalf("Count(): %v", err)
	} else if got != 8 {
		t.Fatalf("Count() = %d, want 8", got)
	}
}
