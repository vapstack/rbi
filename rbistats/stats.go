// Package rbistats defines public RBI statistics payloads.
package rbistats

import "time"

// Collection is a lightweight collection status snapshot.
type Collection[K ~uint64 | ~string] struct {
	// Indexed reports whether Collection is running in an indexing mode.
	Indexed bool
	// StringKeys reports whether Collection keys are strings rather than uint64 values.
	StringKeys bool

	// BuildTime is the duration of the startup index build.
	BuildTime time.Duration
	// BuildRPS is an approximate throughput (records per second) of the startup index build.
	BuildRPS int
	// LoadTime is the time spent loading a persisted index from disk on the last successful load.
	LoadTime time.Duration

	// LastKey is the greatest live data bucket key.
	// For string-key Collection this is the lexicographically greatest key.
	LastKey K
	// KeyCount is the current indexed universe cardinality.
	// In transparent mode it remains zero-valued.
	KeyCount uint64

	// IndexFieldCount is the total number of indexed fields configured for this Collection.
	IndexFieldCount int
	// MeasureFieldCount is the number of aggregate-only measure fields.
	MeasureFieldCount int
	// UniqueFieldCount is the number of unique indexed fields.
	UniqueFieldCount int

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

	// StringKeyIndex contains synthetic string primary-key index diagnostics.
	// It is nil when string key indexing is disabled.
	StringKeyIndex *StringKeyIndex

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

// StringKeyIndex contains storage stats for synthetic string primary-key index.
type StringKeyIndex struct {
	// Size contains posting payload bytes retained by index.
	Size uint64
	// KeyBytes contains raw bytes retained by the index.
	KeyBytes uint64
	// Cardinality is the sum of posting-list cardinalities in the index.
	Cardinality uint64
	// ApproxStructBytes contains approximate structural/layout overhead
	// for the synthetic string primary-key index.
	ApproxStructBytes uint64
	// ApproxHeapBytes is Size + KeyBytes + ApproxStructBytes.
	ApproxHeapBytes uint64
}

// Snapshot contains published snapshot diagnostics.
type Snapshot struct {
	// Sequence is the bucket sequence of the published snapshot.
	Sequence uint64

	// UniverseCard is cardinality of the published universe bitmap.
	UniverseCard uint64
}

// Store contains root-scoped generation and write-scheduler diagnostics.
type Store struct {
	// CurrentEpoch is the current published root epoch.
	CurrentEpoch uint64

	// OpenCollections is the number of currently open collections on the root.
	OpenCollections int
	// CollectionHighWater is the highest collection ordinal count reached by this root.
	CollectionHighWater uint32

	// Broken reports whether the root is in broken state.
	Broken bool
	// Reaped reports whether the root registry has no current generation.
	Reaped bool

	// RegistrySize is number of published/retired root refs tracked by epoch.
	RegistrySize int
	// StagedGenerations is number of staged, not yet published root generations.
	StagedGenerations int
	// PinnedRefs is number of root refs with active external pins.
	PinnedRefs int
	// RetiredGenerations is number of same-epoch metadata generations waiting
	// for active pins on their owning root ref to drain.
	RetiredGenerations int
	// OldestPinnedEpoch is the smallest epoch among currently pinned root refs.
	OldestPinnedEpoch uint64

	// QueueLen is current pending write logical units on this root.
	QueueLen int
	// QueueCap is current allocated write queue capacity on this root.
	QueueCap int
	// WorkerRunning reports whether a root write worker is active.
	WorkerRunning bool
	// QueueHighWater is maximum observed write queue length.
	QueueHighWater uint64

	// LogicalUnitsSubmitted is number of write logical unit submit attempts.
	LogicalUnitsSubmitted uint64
	// LogicalUnitsEnqueued is number of write logical units accepted into a queue.
	LogicalUnitsEnqueued uint64
	// LogicalUnitsDequeued is number of write logical units selected for execution.
	LogicalUnitsDequeued uint64

	// ExecutedBatches is total executed physical write batches.
	ExecutedBatches uint64
	// MultiUnitBatches is number of executed batches containing more than one logical unit.
	MultiUnitBatches uint64
	// MultiUnitOps is total logical units executed inside multi-unit batches.
	MultiUnitOps uint64
	// BatchSize1 is number of executed single-unit batches.
	BatchSize1 uint64
	// BatchSize2To4 is number of executed batches sized 2..4.
	BatchSize2To4 uint64
	// BatchSize5To8 is number of executed batches sized 5..8.
	BatchSize5To8 uint64
	// BatchSize9Plus is number of executed batches sized 9+.
	BatchSize9Plus uint64
	// AvgBatchSize is average logical units per executed physical batch.
	AvgBatchSize float64
	// MaxBatchSeen is maximum observed executed physical batch size.
	MaxBatchSeen uint64

	// CallbackOps is number of write logical units that executed callbacks.
	CallbackOps uint64

	// RejectedClosed is number of write logical units rejected because the root or collection was closed.
	RejectedClosed uint64
	// UniqueRejected is number of write logical units rejected by unique checks before commit.
	UniqueRejected uint64
	// TxBeginErrors is number of physical write tx begin failures.
	TxBeginErrors uint64
	// TxOpErrors is number of physical write operation failures before commit.
	TxOpErrors uint64
	// TxCommitErrors is number of physical write commit failures.
	TxCommitErrors uint64
	// CallbackErrors is number of callback failures returned by write hooks.
	CallbackErrors uint64
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
