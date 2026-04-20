package main

import (
	"math/rand/v2"

	"github.com/vapstack/rbi"
)

const (
	DefaultDBFilename    = "stress.db"
	DefaultEmailSampleN  = 10_000
	InitialRecords       = 10_000_000
	MaxLatencySampleSize = 250_000
)

const (
	RoleRead  = "read"
	RoleWrite = "write"
)

const (
	ClassReadIndexed  = "read_indexed"
	ClassReadSimple   = "read_simple"
	ClassReadMedium   = "read_medium"
	ClassReadMedHeavy = "read_medium_heavy"
	ClassReadHeavy    = "read_heavy"
	ClassWriteFast    = "write_fast"
	ClassWriteSimple  = "write_simple"
	ClassWriteMedium  = "write_medium"
	ClassWriteHeavy   = "write_heavy"
)

type UserBench struct {
	ID         uint64   `db:"id"`
	Name       string   `db:"name"        dbi:"default"`
	Email      string   `db:"email"       dbi:"unique"`
	Country    string   `db:"country"     dbi:"default"`
	Plan       string   `db:"plan"        dbi:"default"`
	Status     string   `db:"status"      dbi:"default"`
	Age        int      `db:"age"         dbi:"default"`
	Score      float64  `db:"score"       dbi:"default"`
	IsVerified bool     `db:"is_verified" dbi:"default"`
	CreatedAt  int64    `db:"created_at"  dbi:"default"`
	LastLogin  int64    `db:"last_login"  dbi:"default"`
	Tags       []string `db:"tags"        dbi:"default"`
	Roles      []string `db:"roles"       dbi:"default"`
	Blob       []byte   `db:"-"`
}

type LatencySummary struct {
	AvgUs float64 `json:"avg"`
	P50Us float64 `json:"p50"`
	P95Us float64 `json:"p95"`
	P99Us float64 `json:"p99"`
}

type MemorySnapshot struct {
	CapturedAt string              `json:"captured_at"`
	Go         GoMemoryStats       `json:"go"`
	Process    ProcessMemoryStats  `json:"process"`
	Snapshot   SnapshotMemoryStats `json:"snapshot"`
}

type GoMemoryStats struct {
	HeapAllocBytes    uint64 `json:"heap_alloc_bytes"`
	HeapInuseBytes    uint64 `json:"heap_inuse_bytes"`
	HeapReleasedBytes uint64 `json:"heap_released_bytes"`
	HeapObjects       uint64 `json:"heap_objects"`
	StackInuseBytes   uint64 `json:"stack_inuse_bytes"`
	SysBytes          uint64 `json:"sys_bytes"`
	NextGCBytes       uint64 `json:"next_gc_bytes"`
	NumGC             uint32 `json:"num_gc"`
}

type ProcessMemoryStats struct {
	RSSBytes               uint64 `json:"rss_bytes"`
	PSSBytes               uint64 `json:"pss_bytes"`
	AnonymousBytes         uint64 `json:"anonymous_bytes"`
	PrivateCleanBytes      uint64 `json:"private_clean_bytes"`
	PrivateDirtyBytes      uint64 `json:"private_dirty_bytes"`
	SharedCleanBytes       uint64 `json:"shared_clean_bytes"`
	SharedDirtyBytes       uint64 `json:"shared_dirty_bytes"`
	BenchDBMapRSSBytes     uint64 `json:"bench_db_map_rss_bytes"`
	BenchDBMapPSSBytes     uint64 `json:"bench_db_map_pss_bytes"`
	BenchDBMapPrivateClean uint64 `json:"bench_db_map_private_clean_bytes"`
	BenchDBMapPrivateDirty uint64 `json:"bench_db_map_private_dirty_bytes"`
	BenchDBMapSharedClean  uint64 `json:"bench_db_map_shared_clean_bytes"`
	BenchDBMapSharedDirty  uint64 `json:"bench_db_map_shared_dirty_bytes"`
}

type SnapshotMemoryStats struct {
	RegistrySize int    `json:"registry_size"`
	PinnedRefs   int    `json:"pinned_refs"`
	UniverseCard uint64 `json:"universe_card"`
}

type MemorySummary struct {
	MaxHeapAllocBytes     uint64 `json:"max_heap_alloc_bytes"`
	MaxHeapInuseBytes     uint64 `json:"max_heap_inuse_bytes"`
	MaxRSSBytes           uint64 `json:"max_rss_bytes"`
	MaxAnonymousBytes     uint64 `json:"max_anonymous_bytes"`
	MaxPrivateDirtyBytes  uint64 `json:"max_private_dirty_bytes"`
	MaxBenchDBMapRSSBytes uint64 `json:"max_bench_db_map_rss_bytes"`
	MaxPinnedRefs         int    `json:"max_pinned_refs"`
	MaxRegistrySize       int    `json:"max_registry_size"`
	MaxUniverseCard       uint64 `json:"max_universe_card"`
}

type StressQueryInfo struct {
	Name   string  `json:"name"`
	Weight float64 `json:"weight,omitempty"`
}

type StressClassInfo struct {
	ID             int               `json:"id"`
	Alias          string            `json:"alias"`
	Name           string            `json:"name"`
	Role           string            `json:"role"`
	DefaultWorkers int               `json:"default_workers"`
	Queries        []StressQueryInfo `json:"queries,omitempty"`
}

type WorkloadContext struct {
	DB           *rbi.DB[uint64, UserBench]
	MaxIDPtr     *uint64
	EmailSamples []string
	Trace        *plannerTraceWorker
}

type WorkloadFunc func(ctx *WorkloadContext, rng *rand.Rand) (string, error)

type ClassDef struct {
	Name    string `json:"name"`
	Role    string `json:"role"`
	Workers int    `json:"workers"`
	Run     WorkloadFunc
}
