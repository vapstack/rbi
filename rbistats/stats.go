// Package rbistats defines public RBI statistics payloads.
package rbistats

import "time"

// Mode identifies whether a DB instance maintains runtime indexes.
type Mode uint8

const (
	// ModeUnknown is the zero-value mode used by empty stats payloads.
	ModeUnknown Mode = iota
	// ModeTransparent means writes and reads use bbolt without runtime indexes.
	ModeTransparent
	// ModeIndexed means runtime indexes and snapshots are maintained.
	ModeIndexed
)

// DB is a lightweight database status snapshot.
type DB[K ~uint64 | ~string] struct {
	// Mode is the current DB indexing mode.
	Mode Mode
	// StringKeys reports whether DB keys are strings rather than uint64 values.
	StringKeys bool

	// BuildTime is the duration of the startup index build.
	BuildTime time.Duration
	// BuildRPS is an approximate throughput (records per second) of the startup index build.
	BuildRPS int
	// LoadTime is the time spent loading a persisted index from disk on the last successful load.
	LoadTime time.Duration

	// LastKey is the greatest live data bucket key.
	// For string-key DB this is the lexicographically greatest key.
	LastKey K
	// KeyCount is the current indexed universe cardinality.
	// In transparent mode it remains zero-valued.
	KeyCount uint64

	// IndexFieldCount is the total number of indexed fields configured for this DB.
	IndexFieldCount int
	// MeasureFieldCount is the number of aggregate-only measure fields.
	MeasureFieldCount int
	// UniqueFieldCount is the number of unique indexed fields.
	UniqueFieldCount int

	// AutoBatchQueueLen is the current pending request count.
	AutoBatchQueueLen int
	// AutoBatchQueueMax is the maximum observed queue length.
	AutoBatchQueueMax uint64
	// AutoBatchCount is the total executed auto-batcher transaction count.
	AutoBatchCount uint64
	// AutoBatchAvgSize is the average requests per executed batch.
	AutoBatchAvgSize float64

	// SnapshotSequence is the observed data bucket sequence for this stats read.
	SnapshotSequence uint64
}

// Index contains expensive index shape and memory diagnostics.
type Index struct {
	// UniqueFieldKeys contains distinct logical value keys per field in the
	// regular value index.
	//
	// It excludes the synthetic nil-family entry and excludes slice-length
	// helper indexes. On multivalue fields it counts distinct field values,
	// not records.
	UniqueFieldKeys map[string]uint64
	// Size contains posting payload bytes across all covered field index
	// families.
	//
	// It is the sum of FieldSize and includes nil-family postings when a
	// field has indexed nils. It excludes raw key storage and structural
	// layout overhead.
	Size uint64
	// FieldSize contains posting payload bytes per field across the covered
	// field index families.
	//
	// This includes nil-family postings for the field, but excludes raw key
	// storage and structural layout overhead.
	FieldSize map[string]uint64
	// FieldKeyBytes contains raw retained key bytes per field in the regular
	// value index.
	//
	// String keys contribute their byte length. Numeric keys contribute 8
	// bytes each. Synthetic nil-family entries are excluded.
	FieldKeyBytes map[string]uint64
	// FieldTotalCardinality contains the sum of posting-list cardinalities
	// per field across the covered field index families.
	//
	// This includes nil-family postings. Multivalue fields can exceed the
	// number of records because one record may appear in multiple postings.
	FieldTotalCardinality map[string]uint64
	// FieldApproxStructBytes contains approximate structural/layout overhead
	// per field.
	//
	// It includes roots, pages, chunk metadata, owner arrays, and a share of
	// the top-level field-storage slot owners so that the map sums to
	// ApproxStructBytes. It excludes posting payload bytes and raw key bytes.
	FieldApproxStructBytes map[string]uint64
	// FieldApproxHeapBytes contains approximate total heap per field for the
	// same covered storage as the other per-field maps.
	//
	// For each field it is FieldSize + FieldKeyBytes +
	// FieldApproxStructBytes, and the map sums to ApproxHeapBytes.
	FieldApproxHeapBytes map[string]uint64

	// EntryCount is the total number of non-empty index entries across the
	// covered field index families.
	//
	// This includes synthetic nil-family entries.
	EntryCount uint64
	// KeyBytes is the total raw key bytes across regular value-index entries.
	//
	// It is the sum of FieldKeyBytes.
	KeyBytes uint64
	// PostingCardinality is the sum of posting cardinalities across the
	// covered field index families.
	//
	// It is the sum of FieldTotalCardinality.
	PostingCardinality uint64

	// ApproxStructBytes is approximate layout overhead from roots, pages,
	// chunk metadata, and pooled owner arrays.
	//
	// It excludes posting payload bytes and raw key bytes, and is the sum of
	// FieldApproxStructBytes.
	ApproxStructBytes uint64
	// ApproxHeapBytes is the approximate total heap for the covered field
	// index storage.
	//
	// It is Size + KeyBytes + ApproxStructBytes, and also the sum of
	// FieldApproxHeapBytes.
	//
	// Coverage is limited to regular field value indexes plus synthetic
	// nil-family indexes. It excludes slice-length helper indexes, universe
	// bitmap state, string-key mapping state, and runtime query caches.
	ApproxHeapBytes uint64
}

// AutoBatch contains write-batcher queue, batch, and error diagnostics.
type AutoBatch struct {
	// Window is current coalescing window duration.
	Window time.Duration
	// MaxBatch is configured maximum auto-batch size.
	MaxBatch int
	// MaxQueue is configured maximum queue size (0 means unbounded).
	MaxQueue int

	// QueueLen is current pending requests in queue.
	QueueLen int
	// QueueCap is current allocated queue capacity.
	QueueCap int
	// WorkerRunning reports whether auto-batcher worker goroutine is active.
	WorkerRunning bool
	// HotWindowActive reports whether adaptive hot coalescing window is active.
	HotWindowActive bool

	// Submitted is number of submit attempts from eligible write calls.
	Submitted uint64
	// Enqueued is number of requests accepted into auto-batcher queue.
	Enqueued uint64
	// Dequeued is number of requests popped from queue for execution.
	Dequeued uint64
	// QueueHighWater is maximum observed queue length.
	QueueHighWater uint64

	// ExecutedBatches is total executed auto-batcher transactions.
	ExecutedBatches uint64
	// MultiRequestBatches is number of executed batches containing more than one request.
	MultiRequestBatches uint64
	// MultiRequestOps is total requests executed inside multi-request batches.
	MultiRequestOps uint64
	// BatchSize1 is number of executed single-request batches.
	BatchSize1 uint64
	// BatchSize2To4 is number of executed batches sized 2..4.
	BatchSize2To4 uint64
	// BatchSize5To8 is number of executed batches sized 5..8.
	BatchSize5To8 uint64
	// BatchSize9Plus is number of executed batches sized 9+.
	BatchSize9Plus uint64
	// AvgBatchSize is average requests per executed batch.
	AvgBatchSize float64
	// MaxBatchSeen is maximum observed executed batch size.
	MaxBatchSeen uint64

	// CallbackOps is number of requests with BeforeStore or BeforeCommit hooks
	// executed by auto-batcher.
	CallbackOps uint64
	// CoalescedSetDelete is number of Set/Delete requests collapsed into later Set/Delete of same ID.
	CoalescedSetDelete uint64

	// CoalesceWaits is number of coalescing sleeps performed by worker.
	CoalesceWaits uint64
	// CoalesceWaitTime is total time spent sleeping for coalescing.
	CoalesceWaitTime time.Duration
	// QueueWaitTime is aggregate request wait time from enqueue to dequeue.
	QueueWaitTime time.Duration
	// ExecuteTime is aggregate batch execution wall time after dequeue.
	ExecuteTime time.Duration

	// FallbackClosed is number of write calls rejected by auto-batcher because DB is closed.
	FallbackClosed uint64

	// UniqueRejected is number of queued requests rejected by unique checks before commit.
	UniqueRejected uint64
	// TxBeginErrors is number of write tx begin failures inside auto-batcher.
	TxBeginErrors uint64
	// TxOpErrors is number of write tx operation failures before commit.
	TxOpErrors uint64
	// TxCommitErrors is number of write tx commit failures.
	TxCommitErrors uint64
	// CallbackErrors is number of hook failures returned by BeforeStore or
	// BeforeCommit hooks inside auto-batched execution.
	CallbackErrors uint64
}

// Snapshot contains published snapshot diagnostics.
type Snapshot struct {
	// Sequence is the bucket sequence of the published snapshot.
	Sequence uint64

	// UniverseCard is cardinality of the published universe bitmap.
	UniverseCard uint64

	// RegistrySize is number of snapshot entries tracked in registry map.
	RegistrySize int
	// PinnedRefs is number of registry snapshots with active pins.
	PinnedRefs int
}

// Planner contains planner snapshot metadata, per-field stats and sampling settings.
type Planner struct {
	// Version is the current planner statistics version.
	Version uint64
	// GeneratedAt is the timestamp when planner stats were generated.
	GeneratedAt time.Time
	// UniverseCardinality is the universe cardinality used by planner stats.
	UniverseCardinality uint64
	// FieldCount is the number of fields represented in planner stats.
	FieldCount int
	// Fields contains per-field planner cardinality distribution metrics.
	// The map is deep-copied and safe for caller mutation.
	Fields map[string]PlannerField

	// AnalyzeInterval is the configured periodic planner analyze interval.
	AnalyzeInterval time.Duration
	// TraceSampleEvery controls trace sampling frequency (every Nth query).
	TraceSampleEvery uint64
}

// PlannerField contains per-field cardinality distribution metrics.
type PlannerField struct {
	// DistinctKeys is number of distinct keys in field index.
	DistinctKeys uint64
	// NonEmptyKeys is number of keys with non-empty posting bitmap.
	NonEmptyKeys uint64
	// TotalBucketCard is total cardinality summed across all field buckets.
	TotalBucketCard uint64
	// AvgBucketCard is average bucket cardinality.
	AvgBucketCard float64
	// MaxBucketCard is maximum bucket cardinality.
	MaxBucketCard uint64
	// P50BucketCard is median bucket cardinality.
	P50BucketCard uint64
	// P95BucketCard is 95th percentile bucket cardinality.
	P95BucketCard uint64
}

// PlannerSnapshot is an immutable snapshot used by planner heuristics.
type PlannerSnapshot struct {
	Version             uint64
	GeneratedAt         time.Time
	UniverseCardinality uint64
	Fields              map[string]PlannerField
}
