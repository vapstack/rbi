package main

import (
	"time"

	"github.com/vapstack/rbi"
)

const reportSchema = "rbi.stress_report/v4"

type stressReport struct {
	Schema      string `json:"schema"`
	Timestamp   string `json:"timestamp"`
	DBFile      string `json:"db_file"`
	ReportFile  string `json:"report_file"`
	Interrupted bool   `json:"interrupted"`

	StartedAt   string  `json:"started_at"`
	FinishedAt  string  `json:"finished_at"`
	DurationSec float64 `json:"duration_sec"`

	RefreshEverySec   float64  `json:"refresh_every_sec"`
	TelemetryEverySec float64  `json:"telemetry_every_sec"`
	ClassFilter       []string `json:"class_filter,omitempty"`
	QueryFilter       []string `json:"query_filter,omitempty"`

	RecordsAtStart uint64 `json:"records_at_start"`
	RecordsAtEnd   uint64 `json:"records_at_end"`
	MaxIDAtStart   uint64 `json:"max_id_at_start"`
	MaxIDAtEnd     uint64 `json:"max_id_at_end"`

	Classes []classReport       `json:"classes"`
	Totals  totalsReport        `json:"totals"`
	Planner *plannerTraceReport `json:"planner,omitempty"`

	MemoryBaseline *MemorySnapshot  `json:"memory_baseline,omitempty"`
	MemoryFinal    *MemorySnapshot  `json:"memory_final,omitempty"`
	MemorySummary  *MemorySummary   `json:"memory_summary,omitempty"`
	MemorySamples  []MemorySnapshot `json:"memory_samples,omitempty"`

	PoolBaseline *poolSample  `json:"pool_baseline,omitempty"`
	PoolFinal    *poolSample  `json:"pool_final,omitempty"`
	PoolSummary  *poolSummary `json:"pool_summary,omitempty"`
	PoolSamples  []poolSample `json:"pool_samples,omitempty"`

	SnapshotBaseline snapshotSample   `json:"snapshot_baseline"`
	SnapshotFinal    snapshotSample   `json:"snapshot_final"`
	SnapshotSamples  []snapshotSample `json:"snapshot_samples,omitempty"`

	BatchBaseline batchSample   `json:"batch_baseline"`
	BatchFinal    batchSample   `json:"batch_final"`
	BatchSamples  []batchSample `json:"batch_samples,omitempty"`
}

type classReport struct {
	ID                int                      `json:"id"`
	Alias             string                   `json:"alias"`
	Name              string                   `json:"name"`
	Role              string                   `json:"role"`
	ConfiguredWorkers int                      `json:"configured_workers"`
	ActiveWorkers     int                      `json:"active_workers"`
	DefaultWorkers    int                      `json:"default_workers"`
	Stats             scopeReport              `json:"stats"`
	Planner           *plannerTraceScopeReport `json:"planner,omitempty"`
	Queries           []queryReport            `json:"queries,omitempty"`
	Workers           []workerReport           `json:"workers,omitempty"`
}

type queryReport struct {
	Name    string                   `json:"name"`
	Weight  float64                  `json:"weight,omitempty"`
	Stats   scopeReport              `json:"stats"`
	Planner *plannerTraceScopeReport `json:"planner,omitempty"`
}

type workerReport struct {
	ID          int64             `json:"id"`
	Name        string            `json:"name"`
	StartedAt   string            `json:"started_at"`
	StoppedAt   string            `json:"stopped_at,omitempty"`
	Completed   uint64            `json:"completed"`
	Errors      uint64            `json:"errors"`
	LastQuery   string            `json:"last_query,omitempty"`
	Latency     LatencySummary    `json:"latency"`
	QueryCounts map[string]uint64 `json:"query_counts,omitempty"`
}

type totalsReport struct {
	Read  scopeReport `json:"read"`
	Write scopeReport `json:"write"`
	Total scopeReport `json:"total"`
}

type scopeReport struct {
	CompletedOps uint64         `json:"completed_ops"`
	Errors       uint64         `json:"errors"`
	CurrentTPS   float64        `json:"current_tps"`
	AverageTPS   float64        `json:"average_tps"`
	MinTPS       float64        `json:"min_tps"`
	Latency      LatencySummary `json:"latency"`
}

type snapshotSample struct {
	CapturedAt string            `json:"captured_at"`
	Stats      rbi.SnapshotStats `json:"stats"`
	Delta      snapshotDelta     `json:"delta"`
}

type snapshotDelta struct {
	CompactorRequested    uint64 `json:"compactor_requested"`
	CompactorRuns         uint64 `json:"compactor_runs"`
	CompactorAttempts     uint64 `json:"compactor_attempts"`
	CompactorSucceeded    uint64 `json:"compactor_succeeded"`
	CompactorPreclaimBusy uint64 `json:"compactor_preclaim_busy"`
	CompactorLockMiss     uint64 `json:"compactor_lock_miss"`
	CompactorNoChange     uint64 `json:"compactor_no_change"`
	CompactorSoftSkip     uint64 `json:"compactor_soft_skip"`
	CompactorSkippedWake  uint64 `json:"compactor_skipped_wake"`
	CompactorIdleDefers   uint64 `json:"compactor_idle_defers"`
	CompactorStaleRetry   uint64 `json:"compactor_stale_retry"`
}

type batchSample struct {
	CapturedAt string             `json:"captured_at"`
	Stats      rbi.AutoBatchStats `json:"stats"`
	Delta      batchDelta         `json:"delta"`
}

type batchDelta struct {
	Submitted           uint64        `json:"submitted"`
	Enqueued            uint64        `json:"enqueued"`
	Dequeued            uint64        `json:"dequeued"`
	QueueHighWater      uint64        `json:"queue_high_water"`
	ExecutedBatches     uint64        `json:"executed_batches"`
	MultiRequestBatches uint64        `json:"multi_request_batches"`
	MultiRequestOps     uint64        `json:"multi_request_ops"`
	BatchSize1          uint64        `json:"batch_size_1"`
	BatchSize2To4       uint64        `json:"batch_size_2_4"`
	BatchSize5To8       uint64        `json:"batch_size_5_8"`
	BatchSize9Plus      uint64        `json:"batch_size_9_plus"`
	MaxBatchSeen        uint64        `json:"max_batch_seen"`
	CallbackOps         uint64        `json:"callback_ops"`
	CoalescedSetDelete  uint64        `json:"coalesced_set_delete"`
	CoalesceWaits       uint64        `json:"coalesce_waits"`
	CoalesceWaitTime    time.Duration `json:"coalesce_wait_time"`
	FallbackDisabled    uint64        `json:"fallback_disabled"`
	FallbackQueueFull   uint64        `json:"fallback_queue_full"`
	FallbackPatchUnique uint64        `json:"fallback_patch_unique"`
	FallbackClosed      uint64        `json:"fallback_closed"`
	UniqueRejected      uint64        `json:"unique_rejected"`
	TxBeginErrors       uint64        `json:"tx_begin_errors"`
	TxOpErrors          uint64        `json:"tx_op_errors"`
	TxCommitErrors      uint64        `json:"tx_commit_errors"`
	CallbackErrors      uint64        `json:"callback_errors"`
}
