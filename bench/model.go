package main

import (
	"math/rand/v2"
	"time"

	"github.com/vapstack/rbi"
)

const (
	DBFilename = "bench.db"

	InitialRecords = 5_000_000

	DefaultCaseDuration  = 60 * time.Second
	DefaultReportEvery   = 2 * time.Second
	DefaultReportPath    = "bench_report.json"
	DefaultEmailSampleN  = 10_000
	DefaultSweepDuration = 20 * time.Second
	MaxLatencySampleSize = 250_000

	DefaultCeilingStageDuration            = 60 * time.Second
	DefaultCeilingDispatchQuantum          = 1 * time.Millisecond
	DefaultCeilingQueueCap                 = 8192
	DefaultCeilingReadFastWorkers          = 1024
	DefaultCeilingReadSlowWorkers          = 128
	DefaultCeilingWriteUpdateWorkers       = 256
	DefaultCeilingWriteInsertWorkers       = 16
	DefaultCeilingReadFastGrid             = "50000,75000,100000,125000,150000,175000,200000"
	DefaultCeilingWriteGrid                = "500,1000,1500,2000,2500,3000,4000,5000"
	DefaultCeilingReadSlowGrid             = "32,64,128,256"
	DefaultCeilingLowWriteGrid             = "250,500,1000,1500"
	DefaultCeilingMixedSlowOps             = 8.0
	DefaultCeilingBaselineFraction         = 0.95 // 0.7
	DefaultCeilingMinThroughputRatio       = 0.95
	DefaultCeilingRegressionThroughputDrop = 10.0
	DefaultCeilingRegressionP99Increase    = 100.0
	DefaultCeilingSuites                   = "all"
)

var (
	Countries = []string{"US", "CA", "GB", "DE", "FR", "NL", "PL", "SE", "JP", "IN", "BR", "AU"}
	Plans     = []string{"free", "starter", "pro", "enterprise"}
	Statuses  = []string{"active", "inactive", "pending", "suspended", "banned"}
	AllTags   = []string{
		"technology", "programming", "golang", "rust", "linux", "webdev", "frontend", "backend",
		"databases", "ai", "machine-learning", "datascience", "security", "devops", "cloud", "kubernetes",
		"startups", "careeradvice", "productivity", "design", "gaming", "android", "ios", "photography",
		"music", "movies", "books", "history", "science", "space", "economics", "politics",
		"sports", "soccer", "basketball", "fitness", "travel", "food", "finance", "cryptocurrency",
	}
	AllRoles = []string{"member", "trusted", "moderator", "admin", "staff", "bot"}
)

var (
	Profiles = []RWProfile{
		{Name: "99_1", ReadRatio: 0.99, WriteRatio: 0.01},
		{Name: "97_3", ReadRatio: 0.97, WriteRatio: 0.03},
		{Name: "95_5", ReadRatio: 0.95, WriteRatio: 0.05},
		{Name: "90_10", ReadRatio: 0.90, WriteRatio: 0.10},
		// {Name: "99.9_0.1", ReadRatio: 0.999, WriteRatio: 0.001},
		// {Name: "99.999_0.001", ReadRatio: 0.99999, WriteRatio: 0.00001},
	}
	WorkerMatrix = []int{4, 64} // []int{4, 8, 16, 32, 64}
	// SweepWorkers = []int{1, 4, 16}
	SweepWorkers = []int{4}

	// DedicatedWriteGateGrid = []int{100000000}
	// DedicatedWriteGateGrid = []int{10000, 100, 10, 5, 100, 10000, 100, 10, 100, 10000}
	// DedicatedWriteGateGrid = []int{10000, 100, 10, 100, 10000}
	DedicatedWriteGateGrid = []int{10}
	// DedicatedWriteGateGrid = []int{100, 20, 100}

	EnableSnapshotDiagnosticsLogs bool
)

type RWProfile struct {
	Name       string  `json:"name"`
	ReadRatio  float64 `json:"read_ratio"`
	WriteRatio float64 `json:"write_ratio"`
}

type LatencySummary struct {
	AvgUs float64 `json:"avg"`
	P50Us float64 `json:"p50"`
	P95Us float64 `json:"p95"`
	P99Us float64 `json:"p99"`
}

type CaseResult struct {
	Profile     string  `json:"profile"`
	Workers     int     `json:"workers"`
	ReadRatio   float64 `json:"read_ratio"`
	WriteRatio  float64 `json:"write_ratio"`
	StartedAt   string  `json:"started_at"`
	FinishedAt  string  `json:"finished_at"`
	DurationSec float64 `json:"duration_sec"`

	TotalOps     uint64 `json:"total_ops"`
	ReadOps      uint64 `json:"read_ops"`
	WriteOps     uint64 `json:"write_ops"`
	Errors       uint64 `json:"errors"`
	RecordsAfter uint64 `json:"records_after"`

	OpsPerSec      float64 `json:"ops_per_sec"`
	ReadOpsPerSec  float64 `json:"read_ops_per_sec"`
	WriteOpsPerSec float64 `json:"write_ops_per_sec"`

	ReadLatencyUs  LatencySummary `json:"read_latency_us"`
	WriteLatencyUs LatencySummary `json:"write_latency_us"`

	OperationBreakdown map[string]uint64 `json:"operation_breakdown"`
	Interrupted        bool              `json:"interrupted"`
}

type BenchmarkReport struct {
	Timestamp string `json:"timestamp"`
	Mode      string `json:"mode"`

	DBFile      string `json:"db_file"`
	OutFile     string `json:"out_file"`
	MaxID       uint64 `json:"max_id"`
	Interrupted bool   `json:"interrupted"`

	CaseDurationSec float64 `json:"case_duration_sec"`
	ReportEverySec  float64 `json:"report_every_sec"`

	StartRecords uint64 `json:"start_records"`
	FinalRecords uint64 `json:"final_records"`

	Profiles []RWProfile `json:"profiles"`
	Workers  []int       `json:"workers"`

	ReadMix        map[string]float64 `json:"read_mix"`
	WriteMix       map[string]float64 `json:"write_mix"`
	ComplexReadMix map[string]float64 `json:"complex_read_mix"`

	Results []CaseResult `json:"results"`

	Sweep   *SweepReport   `json:"sweep,omitempty"`
	Stress  *StressReport  `json:"stress,omitempty"`
	Ceiling *CeilingReport `json:"ceiling,omitempty"`
}

type SweepReport struct {
	Workers        []int             `json:"workers"`
	DurationSec    float64           `json:"duration_sec"`
	QueryOrder     []string          `json:"query_order"`
	Results        []SweepCaseResult `json:"results"`
	Interrupted    bool              `json:"interrupted"`
	RecordsAtStart uint64            `json:"records_at_start"`
	RecordsAtEnd   uint64            `json:"records_at_end"`
}

type SweepCaseResult struct {
	Query       string  `json:"query"`
	Workers     int     `json:"workers"`
	StartedAt   string  `json:"started_at"`
	FinishedAt  string  `json:"finished_at"`
	DurationSec float64 `json:"duration_sec"`

	TotalOps   uint64  `json:"total_ops"`
	Errors     uint64  `json:"errors"`
	OpsPerSec  float64 `json:"ops_per_sec"`
	ReadOpsSec float64 `json:"read_ops_per_sec"`

	ReadLatencyUs      LatencySummary    `json:"read_latency_us"`
	OperationBreakdown map[string]uint64 `json:"operation_breakdown"`
}

type StressReport struct {
	DurationSec float64 `json:"duration_sec"`
	ReportEvery float64 `json:"report_every_sec"`

	IncludeReadWorkers  bool `json:"include_read_workers"`
	IncludeWriteWorkers bool `json:"include_write_workers"`

	WriteGateGrid []int              `json:"write_gate_grid"`
	WorkerSpecs   []StressWorkerSpec `json:"worker_specs"`
	Results       []StressCaseResult `json:"results"`

	Interrupted    bool   `json:"interrupted"`
	RecordsAtStart uint64 `json:"records_at_start"`
	RecordsAtEnd   uint64 `json:"records_at_end"`
}

type StressWorkerSpec struct {
	WorkerID int    `json:"worker_id"`
	Name     string `json:"name"`
	Role     string `json:"role"`
}

type StressCaseResult struct {
	WriteGateEveryN int     `json:"write_gate_every_n"`
	StartedAt       string  `json:"started_at"`
	FinishedAt      string  `json:"finished_at"`
	DurationSec     float64 `json:"duration_sec"`

	Workers int `json:"workers"`

	TotalOps uint64 `json:"total_ops"`
	ReadOps  uint64 `json:"read_ops"`
	WriteOps uint64 `json:"write_ops"`
	Errors   uint64 `json:"errors"`

	OpsPerSec      float64 `json:"ops_per_sec"`
	ReadOpsPerSec  float64 `json:"read_ops_per_sec"`
	WriteOpsPerSec float64 `json:"write_ops_per_sec"`
	WriteTxPerSec  float64 `json:"write_tx_per_sec"`

	ReadLatencyUs          LatencySummary       `json:"read_latency_us"`
	WriteLatencyUs         LatencySummary       `json:"write_latency_us"`
	WriteQueueLatencyUs    LatencySummary       `json:"write_queue_latency_us"`
	WriteServiceLatencyUs  LatencySummary       `json:"write_service_latency_us"`
	WriteEndToEndLatencyUs LatencySummary       `json:"write_end_to_end_latency_us"`
	OperationBreakdown     map[string]uint64    `json:"operation_breakdown"`
	WorkerResults          []StressWorkerResult `json:"worker_results"`
	WriteTxCommits         uint64               `json:"write_tx_commits"`
	BatchedOps             uint64               `json:"batched_ops"`
	WriteAvgBatchSize      float64              `json:"write_avg_batch_size"`
	BatchSubmitted         uint64               `json:"batch_submitted"`
	BatchEnqueued          uint64               `json:"batch_enqueued"`
	BatchDequeued          uint64               `json:"batch_dequeued"`
	BatchQueueHigh         uint64               `json:"batch_queue_high"`
	BatchCoalesceOps       uint64               `json:"write_combine_coalesce_waits"`
}

type StressWorkerResult struct {
	WorkerID int    `json:"worker_id"`
	Name     string `json:"name"`
	Role     string `json:"role"`

	TotalOps uint64 `json:"total_ops"`
	ReadOps  uint64 `json:"read_ops"`
	WriteOps uint64 `json:"write_ops"`
	Errors   uint64 `json:"errors"`

	OpsPerSec     float64        `json:"ops_per_sec"`
	LatencyUs     LatencySummary `json:"latency_us"`
	LastOperation string         `json:"last_operation,omitempty"`

	WriteQueueLatencyUs    LatencySummary `json:"write_queue_latency_us"`
	WriteServiceLatencyUs  LatencySummary `json:"write_service_latency_us"`
	WriteEndToEndLatencyUs LatencySummary `json:"write_end_to_end_latency_us"`
}

type CeilingReport struct {
	StageDurationSec float64 `json:"stage_duration_sec"`
	ReportEverySec   float64 `json:"report_every_sec"`
	DispatchQuantum  float64 `json:"dispatch_quantum_sec"`
	QueueCapacity    int     `json:"queue_capacity"`

	Suites  []CeilingSuiteResult `json:"suites"`
	Summary *CeilingSummary      `json:"summary,omitempty"`

	Interrupted    bool   `json:"interrupted"`
	RecordsAtStart uint64 `json:"records_at_start"`
	RecordsAtEnd   uint64 `json:"records_at_end"`
}

type CeilingSuiteResult struct {
	Name        string               `json:"name"`
	Description string               `json:"description"`
	Stages      []CeilingStageResult `json:"stages"`
}

type CeilingStageResult struct {
	Name        string  `json:"name"`
	Description string  `json:"description"`
	StartedAt   string  `json:"started_at"`
	FinishedAt  string  `json:"finished_at"`
	DurationSec float64 `json:"duration_sec"`

	TargetsOpsPerSec map[string]float64 `json:"targets_ops_per_sec"`
	Executors        map[string]int     `json:"executors"`

	Classes map[string]CeilingClassResult `json:"classes"`

	Saturated bool     `json:"saturated"`
	Reasons   []string `json:"reasons,omitempty"`

	FastReadRegression *CeilingRegression `json:"fast_read_regression,omitempty"`
}

type CeilingClassResult struct {
	Class string `json:"class"`
	Role  string `json:"role"`

	OfferedOps   uint64 `json:"offered_ops"`
	EnqueuedOps  uint64 `json:"enqueued_ops"`
	DroppedOps   uint64 `json:"dropped_ops"`
	StartedOps   uint64 `json:"started_ops"`
	CompletedOps uint64 `json:"completed_ops"`
	Errors       uint64 `json:"errors"`

	OfferedOpsPerSec   float64 `json:"offered_ops_per_sec"`
	CompletedOpsPerSec float64 `json:"completed_ops_per_sec"`
	ThroughputRatio    float64 `json:"throughput_ratio"`

	QueueDepthHigh uint64 `json:"queue_depth_high"`
	QueueDepthEnd  uint64 `json:"queue_depth_end"`
	InFlightHigh   uint64 `json:"in_flight_high"`

	QueueLatencyUs    LatencySummary `json:"queue_latency_us"`
	ServiceLatencyUs  LatencySummary `json:"service_latency_us"`
	EndToEndLatencyUs LatencySummary `json:"end_to_end_latency_us"`

	OperationBreakdown map[string]uint64 `json:"operation_breakdown"`

	Saturated bool     `json:"saturated"`
	Reasons   []string `json:"reasons,omitempty"`
}

type CeilingRegression struct {
	ThroughputDropPct float64  `json:"throughput_drop_pct"`
	P99IncreasePct    float64  `json:"p99_increase_pct"`
	Regressed         bool     `json:"regressed"`
	Reasons           []string `json:"reasons,omitempty"`
}

type CeilingSummary struct {
	GeneratedAt string                `json:"generated_at"`
	Suites      []CeilingSuiteSummary `json:"suites"`
}

type CeilingSuiteSummary struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	StageCount  int    `json:"stage_count"`

	SweepClasses      []string `json:"sweep_classes,omitempty"`
	PrimarySweepClass string   `json:"primary_sweep_class,omitempty"`

	KneeStage  string `json:"knee_stage,omitempty"`
	KneeClass  string `json:"knee_class,omitempty"`
	KneeReason string `json:"knee_reason,omitempty"`

	FirstSaturationStage         string            `json:"first_saturation_stage,omitempty"`
	FirstFastReadRegressionStage string            `json:"first_fast_read_regression_stage,omitempty"`
	FirstClassSaturation         map[string]string `json:"first_class_saturation,omitempty"`

	MaxCompletedOpsPerSec  map[string]float64 `json:"max_completed_ops_per_sec,omitempty"`
	MaxSafeTargetOpsPerSec map[string]float64 `json:"max_safe_target_ops_per_sec,omitempty"`
}

type dedicatedWorkerRunner struct {
	Name string
	Role string
	Run  func(
		db *rbi.DB[uint64, UserBench],
		rng *rand.Rand,
		maxIDPtr *uint64,
		emailSamples []string,
	) (string, error)
}

type readScenario struct {
	Name   string
	Weight int
	Run    func(
		db *rbi.DB[uint64, UserBench],
		rng *rand.Rand,
		currentMaxID uint64,
		emailSamples []string,
	) (string, error)
}
