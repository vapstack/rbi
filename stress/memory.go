package main

import (
	"bufio"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const procKB = 1024

func CaptureMemorySnapshot(db *DBHandle) *MemorySnapshot {
	snap := &MemorySnapshot{
		CapturedAt: time.Now().Format(time.RFC3339Nano),
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	snap.Go = GoMemoryStats{
		HeapAllocBytes:    ms.HeapAlloc,
		HeapInuseBytes:    ms.HeapInuse,
		HeapReleasedBytes: ms.HeapReleased,
		HeapObjects:       ms.HeapObjects,
		StackInuseBytes:   ms.StackInuse,
		SysBytes:          ms.Sys,
		NextGCBytes:       ms.NextGC,
		NumGC:             ms.NumGC,
	}
	if db != nil {
		snap.Process = captureProcessMemory(db.DBFile)
		if db.DB != nil {
			stats := db.DB.SnapshotStats()
			snap.Snapshot = SnapshotMemoryStats{
				HasDelta:          stats.HasDelta,
				RegistrySize:      stats.RegistrySize,
				PinnedRefs:        stats.PinnedRefs,
				IndexLayerDepth:   stats.IndexLayerDepth,
				LenLayerDepth:     stats.LenLayerDepth,
				IndexDeltaFields:  stats.IndexDeltaFields,
				LenDeltaFields:    stats.LenDeltaFields,
				IndexDeltaKeys:    stats.IndexDeltaKeys,
				LenDeltaKeys:      stats.LenDeltaKeys,
				IndexDeltaOps:     stats.IndexDeltaOps,
				LenDeltaOps:       stats.LenDeltaOps,
				UniverseAddCard:   stats.UniverseAddCard,
				UniverseRemCard:   stats.UniverseRemCard,
				CompactorQueueLen: stats.CompactorQueueLen,
			}
		}
	}
	return snap
}

func summarizeMemory(samples []MemorySnapshot, final *MemorySnapshot) *MemorySummary {
	if len(samples) == 0 && final == nil {
		return nil
	}
	var out MemorySummary
	apply := func(s *MemorySnapshot) {
		if s == nil {
			return
		}
		maxUint64(&out.MaxHeapAllocBytes, s.Go.HeapAllocBytes)
		maxUint64(&out.MaxHeapInuseBytes, s.Go.HeapInuseBytes)
		maxUint64(&out.MaxRSSBytes, s.Process.RSSBytes)
		maxUint64(&out.MaxAnonymousBytes, s.Process.AnonymousBytes)
		maxUint64(&out.MaxPrivateDirtyBytes, s.Process.PrivateDirtyBytes)
		maxUint64(&out.MaxBenchDBMapRSSBytes, s.Process.BenchDBMapRSSBytes)
		maxInt(&out.MaxPinnedRefs, s.Snapshot.PinnedRefs)
		maxInt(&out.MaxRegistrySize, s.Snapshot.RegistrySize)
		maxInt(&out.MaxIndexLayerDepth, s.Snapshot.IndexLayerDepth)
		maxInt(&out.MaxLenLayerDepth, s.Snapshot.LenLayerDepth)
		maxUint64(&out.MaxIndexDeltaOps, s.Snapshot.IndexDeltaOps)
		maxUint64(&out.MaxLenDeltaOps, s.Snapshot.LenDeltaOps)
	}
	for i := range samples {
		apply(&samples[i])
	}
	apply(final)
	return &out
}

func captureProcessMemory(dbFile string) ProcessMemoryStats {
	var out ProcessMemoryStats
	if rollup, err := parseProcRollup("/proc/self/smaps_rollup"); err == nil {
		out.RSSBytes = rollup["Rss"]
		out.PSSBytes = rollup["Pss"]
		out.AnonymousBytes = rollup["Anonymous"]
		out.PrivateCleanBytes = rollup["Private_Clean"]
		out.PrivateDirtyBytes = rollup["Private_Dirty"]
		out.SharedCleanBytes = rollup["Shared_Clean"]
		out.SharedDirtyBytes = rollup["Shared_Dirty"]
	}
	if dbFile == "" {
		return out
	}
	dbPath := filepath.Clean(dbFile)
	if !filepath.IsAbs(dbPath) {
		if abs, err := filepath.Abs(dbPath); err == nil {
			dbPath = abs
		}
	}
	if dbMap, err := parseProcMapRollup("/proc/self/smaps", dbPath); err == nil {
		out.BenchDBMapRSSBytes = dbMap["Rss"]
		out.BenchDBMapPSSBytes = dbMap["Pss"]
		out.BenchDBMapPrivateClean = dbMap["Private_Clean"]
		out.BenchDBMapPrivateDirty = dbMap["Private_Dirty"]
		out.BenchDBMapSharedClean = dbMap["Shared_Clean"]
		out.BenchDBMapSharedDirty = dbMap["Shared_Dirty"]
	}
	return out
}

func parseProcRollup(path string) (map[string]uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	out := make(map[string]uint64, 8)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		key, value, ok := parseProcKVLine(line)
		if !ok {
			continue
		}
		out[key] = value
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func parseProcMapRollup(path, matchPath string) (map[string]uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	out := make(map[string]uint64, 8)
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)

	matchPath = filepath.Clean(matchPath)
	inMatch := false
	for scanner.Scan() {
		line := scanner.Text()
		if isProcMapHeader(line) {
			inMatch = procMapMatchesPath(line, matchPath)
			continue
		}
		if !inMatch {
			continue
		}
		key, value, ok := parseProcKVLine(strings.TrimSpace(line))
		if !ok {
			continue
		}
		out[key] += value
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func isProcMapHeader(line string) bool {
	fields := strings.Fields(line)
	if len(fields) < 5 {
		return false
	}
	if strings.HasSuffix(fields[0], ":") {
		return false
	}
	return strings.Contains(fields[0], "-")
}

func procMapMatchesPath(line, matchPath string) bool {
	fields := strings.Fields(line)
	if len(fields) < 6 {
		return false
	}
	path := strings.Join(fields[5:], " ")
	path = strings.TrimSuffix(path, " (deleted)")
	path = filepath.Clean(path)
	return path == matchPath
}

func parseProcKVLine(line string) (string, uint64, bool) {
	if line == "" || !strings.Contains(line, ":") {
		return "", 0, false
	}
	key, rawValue, ok := strings.Cut(line, ":")
	if !ok {
		return "", 0, false
	}
	fields := strings.Fields(rawValue)
	if len(fields) == 0 {
		return "", 0, false
	}
	n, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return "", 0, false
	}
	if len(fields) > 1 && strings.EqualFold(fields[1], "kB") {
		n *= procKB
	}
	return key, n, true
}

func maxUint64(dst *uint64, value uint64) {
	if value > *dst {
		*dst = value
	}
}

func maxInt(dst *int, value int) {
	if value > *dst {
		*dst = value
	}
}
