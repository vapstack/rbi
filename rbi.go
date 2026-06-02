package rbi

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/engine"
	"github.com/vapstack/rbi/internal/qexec"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/wexec"
	"github.com/vmihailenco/msgpack/v5"
	"go.etcd.io/bbolt"
)

// IndexKind declares how a struct field participates in RBI secondary storage.
type IndexKind uint8

const (
	IndexDefault IndexKind = iota
	IndexUnique
	IndexMeasure
)

// ValueIndexer defines how a field value is converted into a canonical string
// representation used as an index key in rbi.
//
// A type that implements ValueIndexer is responsible for ensuring that
// IndexingValue returns a valid and stable string for every value that may
// appear in indexed data. This includes handling nil receivers if the type
// is a pointer or otherwise nillable. The caller does not perform nil
// checks before invoking IndexingValue.
//
// IndexingValue must return a deterministic string: the same value must
// always produce the same indexing key.
//
// The returned string is compared lexicographically when evaluating
// range queries (>, >=, <, <=). Implementation must ensure that the
// produced ordering matches the intent.
type ValueIndexer interface {
	IndexingValue() string
}

// Codec overrides default msgpack encoding/decoding for *V.
//
// EncodeRBI must write the full encoded form of the receiver to w.
// DecodeRBI must populate the receiver from the encoded form read from r.
//
// RBI does not retry decoding with msgpack when DecodeRBI returns an error.
// If fallback decoding is needed, implement it inside the
// custom codec, for example by using github.com/vmihailenco/msgpack/v5.
type Codec interface {
	EncodeRBI(io.Writer) error
	DecodeRBI(io.Reader) error
}

var codecType = reflect.TypeFor[Codec]()

var (
	ErrNotStructType     = errors.New("value is not a struct")
	ErrClosed            = errors.New("database closed")
	ErrBroken            = errors.New("index is broken")
	ErrNoIndex           = errors.New("index is disabled (transparent mode)")
	ErrInvalidQuery      = qexec.ErrInvalidQuery
	ErrInvalidBucketName = errors.New("invalid bucket name")
	ErrUniqueViolation   = errors.New("unique constraint violation")
	ErrNoValidKeyIndex   = errors.New("no valid key for index")
	ErrNilValue          = errors.New("value is nil")

	errPersistedIndexStale   = errors.New("persisted index is stale")
	errPersistedIndexInvalid = errors.New("persisted index is invalid")
)

// Options configures indexer and how it works with a bbolt database.
//
// Zero-valued option fields use defaults.
// DefaultOptions returns options with all defaults pre-filled.
type Options struct {
	// Index overrides struct rbi tags when non-nil.
	//
	// Keys may be Go field names or db tag values. A nil map means indexes are
	// declared by rbi tags. A non-nil empty map disables all indexed fields.
	Index map[string]IndexKind

	// DisableIndexLoad prevents indexer from loading previously persisted index
	// data from the .rbi file on startup. If set, indexer rebuilds the index
	// from the underlying bucket.
	DisableIndexLoad bool

	// DisableIndexStore prevents indexer from saving the in-memory index state
	// to the .rbi file on Close.
	DisableIndexStore bool

	// Logger receives informational indexer messages.
	//
	// Default: standard logger from package log.
	Logger *log.Logger

	// BucketName overrides the default bucket name.
	// By default, bucket name is derived from the name of the value type V.
	//
	// The bucket name must use identifier-safe ASCII characters only:
	// `[A-Za-z_][A-Za-z0-9_]*`.
	BucketName string

	// AnalyzeInterval configures how often planner statistics should be
	// refreshed in the background.
	//
	// Default: 1 hour
	//
	// Negative value disables periodic refresh.
	AnalyzeInterval time.Duration

	// TraceSink receives optional per-query planner tracing events.
	// If nil, planner tracing is disabled.
	//
	// Synchronous or heavy sinks can significantly increase query latency.
	TraceSink func(TraceEvent)

	// TraceSampleEvery controls trace sampling:
	//   - 0: when sink is set, sample every query (equivalent to 1)
	//   - 1: sample every query
	//   - N: sample every Nth query
	//
	// Negative value disables tracing (even when sink is set)
	//
	// Default: 1
	TraceSampleEvery int

	// BucketFillPercent controls bbolt bucket fill factor for write operations.
	//
	// Default: 0.8
	//
	// Valid range: (0, 1]
	//
	// Lower values leave more free space on pages (usually larger file and
	// lower split cost on random updates). Higher values pack pages denser
	// (usually smaller file, potentially higher split/relocation cost).
	//
	// Values too low can cause excessive file growth and write amplification.
	// Values too high on churn-heavy workloads can sharply increase write latency.
	BucketFillPercent float64

	// SnapshotMaterializedPredCacheMaxEntries controls max number of cached
	// materialized predicate bitmaps per published snapshot.
	//
	// Negative value disables cache.
	//
	// Default: 24
	//
	// Typical range: 16..256
	//
	// High values on diverse workloads can cause sharp memory growth.
	SnapshotMaterializedPredCacheMaxEntries int

	// SnapshotMaterializedPredCacheMaxCardinality skips caching very large
	// materialized postings to reduce retained heap and GC pressure.
	//
	// Negative value disables the guard.
	//
	// Default: 64K
	//
	// Negative (disabled) or very large values can significantly increase memory
	// usage for broad predicates.
	SnapshotMaterializedPredCacheMaxCardinality int

	// EnableSnapshotStats enables runtime collection of snapshot diagnostics.
	//
	// Default: false (disabled).
	EnableSnapshotStats bool

	// AutoBatchWindow configures the coalescing window for parallel
	// single-record Set/Patch/Delete operations.
	//
	// Negative value forces the batcher to process one API write request
	// per Bolt transaction.
	//
	// Default: 50us
	//
	// Typical range: 10us..500us
	//
	// AutoBatching is only useful when multiple goroutines issue single-record
	// writes concurrently. Explicit BatchSet/BatchPatch/BatchDelete already
	// control their own transaction boundaries and do not use this mechanism.
	//
	// Higher values can reduce write-path overhead under contention but may
	// increase single-write latency at low load.
	AutoBatchWindow time.Duration

	// AutoBatchMax limits max operations merged into one combined write tx.
	//
	// Negative value forces the batcher to process one API write request
	// per Bolt transaction.
	//
	// Default: 64
	//
	// Typical range: 4..1024
	//
	// Very high values can create commit-size spikes and tail-latency variance.
	AutoBatchMax int

	// AutoBatchMaxQueue limits pending batch write requests.
	//
	// Negative value disables queue cap.
	//
	// Default: 512
	//
	// Typical range: 128..8192
	//
	// Larger values can increase memory usage under sustained overload.
	AutoBatchMaxQueue int

	// EnableAutoBatchStats enables runtime collection of auto-batch stats.
	//
	// Default: false (disabled).
	EnableAutoBatchStats bool

	// NumericRangeBucketSize controls amount of sorted numeric keys grouped into one
	// pre-aggregated bucket for range predicate acceleration.
	//
	// Negative value disables numeric bucket acceleration.
	//
	// Default: 512
	NumericRangeBucketSize int

	// NumericRangeBucketMinFieldKeys is the minimum amount of unique keys in a
	// numeric field required to build range buckets.
	//
	// Negative value disables numeric bucket acceleration.
	//
	// Default: 8192
	NumericRangeBucketMinFieldKeys int

	// NumericRangeBucketMinSpanKeys is the minimum range span (in keys) required to
	// route GT/GTE/LT/LTE through bucket acceleration path.
	//
	// Negative value disables numeric bucket acceleration.
	//
	// Default: 1024
	NumericRangeBucketMinSpanKeys int
}

const (
	defaultOptionsAnalyzeInterval                  = time.Hour
	defaultBucketFillPercent                       = 0.8
	defaultSnapshotMaterializedPredCacheMaxEntries = 24
	defaultSnapshotMatPredCacheMaxCardinality      = 64 << 10
	defaultAutoBatchWindow                         = 50 * time.Microsecond
	defaultAutoBatchMax                            = 64
	defaultAutoBatchMaxQueue                       = 512
	defaultSnapshotStrMapCompactDepth              = 256
	defaultNumericRangeBucketSize                  = 512
	defaultNumericRangeBucketMinFieldKeys          = 8192
	defaultNumericRangeBucketMinSpanKeys           = 1024
)

func (o *Options) setDefaults() {
	if o.Logger == nil {
		o.Logger = log.Default()
	}
	if o.AnalyzeInterval == 0 {
		o.AnalyzeInterval = defaultOptionsAnalyzeInterval
	}
	if o.TraceSampleEvery == 0 {
		o.TraceSampleEvery = 1
	}
	if o.BucketFillPercent == 0 {
		o.BucketFillPercent = defaultBucketFillPercent
	}
	if o.SnapshotMaterializedPredCacheMaxEntries == 0 {
		o.SnapshotMaterializedPredCacheMaxEntries = defaultSnapshotMaterializedPredCacheMaxEntries
	}
	if o.SnapshotMaterializedPredCacheMaxCardinality == 0 {
		o.SnapshotMaterializedPredCacheMaxCardinality = defaultSnapshotMatPredCacheMaxCardinality
	}
	if o.AutoBatchWindow == 0 {
		o.AutoBatchWindow = defaultAutoBatchWindow
	}
	if o.AutoBatchMax == 0 {
		o.AutoBatchMax = defaultAutoBatchMax
	}
	if o.AutoBatchMaxQueue == 0 {
		o.AutoBatchMaxQueue = defaultAutoBatchMaxQueue
	}
	if o.NumericRangeBucketSize == 0 {
		o.NumericRangeBucketSize = defaultNumericRangeBucketSize
	}
	if o.NumericRangeBucketMinFieldKeys == 0 {
		o.NumericRangeBucketMinFieldKeys = defaultNumericRangeBucketMinFieldKeys
	}
	if o.NumericRangeBucketMinSpanKeys == 0 {
		o.NumericRangeBucketMinSpanKeys = defaultNumericRangeBucketMinSpanKeys
	}
}

func (o Options) validate() error {
	if math.IsNaN(o.BucketFillPercent) || o.BucketFillPercent < 0 || o.BucketFillPercent > 1 {
		return fmt.Errorf("invalid BucketFillPercent: %v", o.BucketFillPercent)
	}
	return nil
}

// Field represents a single field assignment used by Patch and BatchPatch.
// Name is matched against struct field name, "db" tag, or "json" tag,
// and Value is assigned to the matched field using reflection and conversion rules.
type Field struct {
	// Name is the logical name of the field to patch.
	// It can be a struct field name, a "db" tag value, or a "json" tag value.
	Name string
	// Value is the new value to assign to the field.
	// Patch* methods will attempt to convert Value to the field's concrete type,
	// including numeric widening and some int/float conversions.
	Value any
}

type (

	// PlannerStats contains planner snapshot metadata, per-field stats and sampling settings.
	PlannerStats struct {
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
		Fields map[string]PlannerFieldStats

		// AnalyzeInterval is the configured periodic planner analyze interval.
		AnalyzeInterval time.Duration
		// TraceSampleEvery controls trace sampling frequency (every Nth query).
		TraceSampleEvery uint64
	}

	// Stats is a lightweight database status snapshot.
	Stats[K ~uint64 | ~string] struct {
		// BuildTime is the duration of the startup index build.
		BuildTime time.Duration
		// BuildRPS is an approximate throughput (records per second) of the startup index build.
		BuildRPS int
		// LoadTime is the time spent loading a persisted index from disk on the last successful load.
		LoadTime time.Duration

		// LastKey is the largest live key according to the current universe bitmap.
		// For string keys this is resolved through the internal string mapping, so
		// it reflects the highest live internal string index rather than the
		// lexicographically greatest key.
		LastKey K
		// KeyCount is the total number of keys currently present in the database.
		KeyCount uint64

		// FieldCount is the number of indexed fields configured for this DB.
		FieldCount int

		// AutoBatchQueueLen is the current pending request count.
		AutoBatchQueueLen int
		// AutoBatchQueueMax is the maximum observed queue length.
		AutoBatchQueueMax uint64
		// AutoBatchCount is the total executed auto-batcher transaction count.
		AutoBatchCount uint64
		// AutoBatchAvgSize is the average requests per executed batch.
		AutoBatchAvgSize float64

		// SnapshotSequence is the bucket sequence of the published snapshot.
		SnapshotSequence uint64
	}

	// IndexStats contains expensive index shape and memory diagnostics.
	IndexStats struct {
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

	// AutoBatchStats contains write-batcher queue, batch, and error diagnostics.
	AutoBatchStats struct {
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

	// SnapshotStats contains published snapshot diagnostics.
	SnapshotStats struct {
		// Sequence is the bucket sequence of the published snapshot.
		Sequence uint64

		// UniverseCard is cardinality of the published universe bitmap.
		UniverseCard uint64

		// RegistrySize is number of snapshot entries tracked in registry map.
		RegistrySize int
		// PinnedRefs is number of registry snapshots with active pins.
		PinnedRefs int
	}
)

// DB wraps a bbolt database and maintains secondary indexes over values of type *V
// stored in a single bucket. It supports efficient equality and range queries,
// as well as array membership and array-length queries for slice fields.
//
// DB is safe for concurrent use.
type DB[K ~string | ~uint64, V any] struct {
	vtype  reflect.Type
	strKey bool

	bolt   *bbolt.DB
	bucket []byte

	options     *Options
	execOptions execOptions[K, V]

	logger  *log.Logger
	schema  *schema.Schema
	index   *engine.Index // nil in transparent mode
	batcher *wexec.Batcher

	closed atomic.Bool
	broken atomic.Bool

	rbiFile string

	encodeFn func(*V, io.Writer) error
	decodeFn func(*V, io.Reader) error

	recPool pooled.Pointers[V]

	publishMu sync.RWMutex

	stats Stats[K]
}

// New creates a new indexed DB that uses the provided bbolt database.
//
// The generic type V must be a struct; otherwise ErrNotStructType is returned.
//
// Zero-valued option fields use defaults.
//
// If options.BucketName is empty,
// the name of the value type V is used as the bucket name.
// New ensures the bucket exists, optionally loads a persisted index from disk,
// builds missing or incompatible startup index data from bbolt, and sets up
// field metadata and accessors.
//
// Any ExecOptions passed to New become defaults applied to subsequent write
// operations on the returned DB.
//
// The resulting DB does not manage the underlying *bbolt.DB lifecycle.
func New[K ~uint64 | ~string, V any](bolt *bbolt.DB, options Options, execOpts ...ExecOption[K, V]) (db *DB[K, V], err error) {
	var v V
	vtype := reflect.TypeOf(v)
	if vtype == nil {
		return nil, ErrNotStructType
	}
	if vtype.Kind() != reflect.Struct {
		return nil, ErrNotStructType
	}
	if err = options.validate(); err != nil {
		return nil, err
	}
	options.setDefaults()

	vname := vtype.Name()
	if options.BucketName != "" {
		vname = options.BucketName
	}
	if vname == "" {
		return nil, fmt.Errorf("cannot resolve value name of %v", vtype)
	}
	if err = validateBucketName(vname); err != nil {
		return nil, err
	}
	if bolt == nil {
		return nil, fmt.Errorf("bolt instance is nil")
	}
	var defaultExecOptions execOptions[K, V]
	applyExecOptions(&defaultExecOptions, execOpts)
	defaultExecOptions = freezeExecOptions(defaultExecOptions)
	encodeFn, decodeFn, err := defaultCodecMethods[V]()
	if err != nil {
		return nil, err
	}

	boltPath := bolt.Path()

	if err = regInstance(boltPath, vname); err != nil {
		return nil, err
	}
	defer unregInstanceOnError(&err, boltPath, vname)

	db = &DB[K, V]{
		vtype:   vtype,
		bolt:    bolt,
		bucket:  []byte(vname),
		options: &options,
		logger:  options.Logger,

		rbiFile: bolt.Path() + "." + vname + ".rbi",

		execOptions: defaultExecOptions,
		encodeFn:    encodeFn,
		decodeFn:    decodeFn,
	}

	var k K
	if reflect.TypeOf(k).Kind() == reflect.String {
		db.strKey = true
	}

	var schemaIndex map[string]schema.IndexKind
	if options.Index != nil {
		schemaIndex = make(map[string]schema.IndexKind, len(options.Index))
		for name, kind := range options.Index {
			schemaIndex[name] = schema.IndexKind(kind)
		}
	}
	db.schema, err = schema.Compile(vtype, schema.Config{Index: schemaIndex})
	if err != nil {
		return nil, err
	}

	db.index, err = engine.NewIndex(engine.Config{
		Schema:                                  db.schema,
		StrKey:                                  db.strKey,
		SnapshotStats:                           db.options.EnableSnapshotStats,
		SnapshotMaterializedPredCacheMaxEntries: db.options.SnapshotMaterializedPredCacheMaxEntries,
		SnapshotMaterializedPredCacheMaxCardinality: db.options.SnapshotMaterializedPredCacheMaxCardinality,
		NumericRangeBucketSize:                      db.options.NumericRangeBucketSize,
		NumericRangeBucketMinFieldKeys:              db.options.NumericRangeBucketMinFieldKeys,
		NumericRangeBucketMinSpanKeys:               db.options.NumericRangeBucketMinSpanKeys,
		AnalyzeInterval:                             plannerAnalyzeInterval(db.options.AnalyzeInterval),
		TraceSink:                                   db.options.TraceSink,
		TraceSampleEvery:                            db.options.TraceSampleEvery,
		StrMapCompactAt:                             defaultSnapshotStrMapCompactDepth,
	})
	if err != nil {
		return nil, err
	}

	err = bolt.Update(func(tx *bbolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists(db.bucket)
		return e
	})
	if err != nil {
		return nil, err
	}

	var (
		loadedPlannerStats       *qexec.PlannerStatsSnapshot
		loadedFieldCount         int
		loadedOrdinaryFieldCount int
		buildMode                string
	)
	if db.index != nil {
		var (
			skipFields        map[string]struct{}
			skipMeasureFields map[string]struct{}
			rebuildReason     string
		)
		if _, err = os.Stat(db.rbiFile); err == nil {
			if options.DisableIndexLoad {
				rebuildReason = "persisted index load disabled"
			} else {
				skipFields, skipMeasureFields, loadedPlannerStats, err = db.loadIndex()
				if err != nil {
					rebuildReason = fmt.Sprintf("persisted index unavailable (%v)", err)
				}
			}
		} else if os.IsNotExist(err) {
			rebuildReason = fmt.Sprintf("persisted index missing (file=%q)", db.rbiFile)
		} else if !os.IsNotExist(err) {
			rebuildReason = fmt.Sprintf("persisted index stat failed (%v)", err)
		}

		loadedOrdinaryFieldCount = len(skipFields)
		loadedFieldCount = loadedOrdinaryFieldCount + len(skipMeasureFields)
		totalFieldCount := len(db.schema.Fields) + len(db.schema.MeasureFields)
		if rebuildReason != "" {
			buildMode = "full"
			db.logger.Printf("rbi: %s", rebuildReason)
			db.logger.Printf(
				"rbi: rebuilding index from bbolt (mode=full loaded_fields=%d/%d)",
				loadedFieldCount,
				totalFieldCount,
			)
		} else if totalFieldCount > 0 {
			if loadedFieldCount == 0 {
				buildMode = "full"
				db.logger.Printf("rbi: persisted index has no compatible field indexes (file=%q)", db.rbiFile)
				db.logger.Printf("rbi: rebuilding index from bbolt (mode=full loaded_fields=0/%d)", totalFieldCount)
			} else if loadedFieldCount < totalFieldCount {
				buildMode = "partial"
				db.logger.Printf(
					"rbi: partially rebuilding index from bbolt (loaded_fields=%d/%d missing_fields=%d)",
					loadedFieldCount,
					totalFieldCount,
					totalFieldCount-loadedFieldCount,
				)
			}
		}

		buildStarted := time.Now()
		if err = db.buildIndex(skipFields, skipMeasureFields); err != nil {
			return nil, fmt.Errorf("error building index: %w", err)
		}
		if buildMode != "" {
			db.logger.Printf("rbi: index build completed (mode=%s duration=%s)", buildMode, time.Since(buildStarted))
		}
	}

	db.initBatcher()

	if db.index != nil {
		db.stats.FieldCount = len(db.schema.Fields) + len(db.schema.MeasureFields)
		if err = db.index.PublishCurrentSequenceSnapshot(db.bolt, db.bucket); err != nil {
			return nil, fmt.Errorf("failed to publish initial snapshot: %w", err)
		}

		if loadedPlannerStats != nil && loadedOrdinaryFieldCount == len(db.schema.Fields) {
			db.index.PublishLoadedPlannerStats(loadedPlannerStats)

		} else {
			if err = db.RefreshPlannerStats(); err != nil {
				return nil, fmt.Errorf("failed to build planner stats snapshot: %w", err)
			}
		}

		db.startPlannerAnalyzeLoop()
	}

	return db, nil
}

func (db *DB[K, V]) initBatcher() {
	ops := wexec.RecordOps{
		Encode: func(ptr unsafe.Pointer, buf *bytes.Buffer) error {
			return db.encode((*V)(ptr), buf)
		},
		Decode: func(data []byte) (unsafe.Pointer, error) {
			val, err := db.decode(data)
			if err != nil {
				return nil, err
			}
			return unsafe.Pointer(val), nil
		},
		Release: func(ptr unsafe.Pointer) {
			db.ReleaseRecords((*V)(ptr))
		},
		ValidateIndex: func(ptr unsafe.Pointer) error {
			return db.validateIndexedStringValues(ptr)
		},
	}
	cfg := wexec.Config{
		MaxOps:       db.options.AutoBatchMax,
		Window:       db.options.AutoBatchWindow,
		MaxQueue:     db.options.AutoBatchMaxQueue,
		StatsEnabled: db.options.EnableAutoBatchStats,
		Unavailable: func() error {
			if db.closed.Load() {
				return ErrClosed
			}
			if db.broken.Load() {
				return ErrBroken
			}
			return nil
		},
		Errors: wexec.ErrorSet{
			UniqueViolation: ErrUniqueViolation,
		},

		Bolt:               db.bolt,
		Bucket:             db.bucket,
		BucketFillPercent:  db.options.BucketFillPercent,
		RejectEmptyPayload: db.decodeFn == nil,
		PublishMu:          &db.publishMu,
		Commit:             commitTx,
		StrKey:             db.strKey,
		Indexed:            db.index != nil,
		Ops:                &ops,
		Schema:             db.schema,
	}

	if db.index != nil {
		db.index.ConfigureWrite(&cfg, &db.broken, db.logger, ErrUniqueViolation, ErrBroken)
	}
	db.batcher = wexec.NewBatcher(cfg)
}

func (db *DB[K, V]) buildIndex(skipFields map[string]struct{}, skipMeasureFields map[string]struct{}) error {
	result, err := db.index.BuildIndex(db.bolt, db.bucket, skipFields, skipMeasureFields, db.decodeBuildIndexRecord, db.releaseBuildIndexRecord)
	if err != nil {
		return err
	}
	if result.Stats {
		db.stats.BuildTime = result.BuildTime
		db.stats.BuildRPS = result.BuildRPS
	}
	return nil
}

func (db *DB[K, V]) decodeBuildIndexRecord(data []byte) (unsafe.Pointer, error) {
	val, err := db.decode(data)
	if err != nil {
		return nil, err
	}
	return unsafe.Pointer(val), nil
}

func (db *DB[K, V]) releaseBuildIndexRecord(ptr unsafe.Pointer) {
	var zero V
	val := (*V)(ptr)
	*val = zero
	db.recPool.Put(val)
}

func (db *DB[K, V]) validateIndexedStringValues(ptr unsafe.Pointer) error {
	if db.index == nil || ptr == nil {
		return nil
	}
	return db.index.ValidateStringValues(ptr)
}

func (db *DB[K, V]) loadIndex() (
	skipFields map[string]struct{},
	skipMeasureFields map[string]struct{},
	plannerStats *qexec.PlannerStatsSnapshot,
	err error,
) {
	currentSeq, err := currentBucketSequence(db.bolt, db.bucket)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("decode: reading current bucket sequence: %w", err)
	}
	start := time.Now()
	result, err := db.index.LoadIndex(db.rbiFile, db.bolt.Path(), db.bucket, currentSeq, defaultSnapshotStrMapCompactDepth, engine.LoadErrors{
		Stale:   errPersistedIndexStale,
		Invalid: errPersistedIndexInvalid,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	db.stats.LoadTime = time.Since(start)

	return result.SkipFields, result.SkipMeasureFields, result.PlannerStats, nil
}

func (db *DB[K, V]) storeIndex() error {
	return db.index.StoreIndex(db.rbiFile, db.bolt, db.bucket)
}

// disableSync disables fsync for bolt writes. Can help with batch inserts.
// It should not be used during normal operation.
//
// 2026-05-30: removed from public API.
func (db *DB[K, V]) disableSync() { db.bolt.NoSync = true }

// enableSync enables fsync for bolt writes. See disableSync.
// By default, fsync is enabled.
//
// 2026-05-30: removed from public API.
func (db *DB[K, V]) enableSync() {
	db.bolt.NoSync = false
	_ = db.bolt.Sync()
}

func (db *DB[K, V]) unavailableErr() error {
	if db.closed.Load() {
		return ErrClosed
	}
	if db.broken.Load() {
		return ErrBroken
	}
	return nil
}

func currentBucketSequence(bolt *bbolt.DB, bucket []byte) (uint64, error) {
	var seq uint64
	if err := bolt.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		seq = b.Sequence()
		return nil
	}); err != nil {
		return 0, err
	}
	return seq, nil
}

func commitTx(tx *bbolt.Tx, _ string) error {
	return tx.Commit()
}

// Stats returns a lightweight database status snapshot.
//
// In indexed mode it reports startup build/load timings together with the
// current published snapshot cardinality and last key.
//
// In transparent mode no runtime index snapshot is maintained, so fields that
// depend on indexed state remain zero-valued: BuildTime, BuildRPS, LoadTime,
// KeyCount, LastKey, and SnapshotSequence. FieldCount also stays zero because
// no indexed fields exist in that mode. Auto-batcher fields remain meaningful
// in both modes.
//
// If DB is closed or broken, Stats returns a zero value.
func (db *DB[K, V]) Stats() Stats[K] {
	if err := db.unavailableErr(); err != nil {
		return Stats[K]{}
	}

	db.publishMu.RLock()
	out := db.stats
	db.publishMu.RUnlock()

	if db.index != nil {
		st := db.index.DBStats()
		out.KeyCount = st.KeyCount
		out.SnapshotSequence = st.Sequence
		if st.HasLast {
			if db.strKey {
				out.LastKey = *(*K)(unsafe.Pointer(&st.LastKey))
			} else {
				out.LastKey = *(*K)(unsafe.Pointer(&st.LastID))
			}
		}
	}

	queueLen, queueMax, executed, dequeued := db.batcher.BasicStats()
	out.AutoBatchQueueLen = queueLen
	out.AutoBatchQueueMax = queueMax

	out.AutoBatchCount = executed
	if out.AutoBatchCount > 0 {
		out.AutoBatchAvgSize = float64(dequeued) / float64(out.AutoBatchCount)
	}

	return out
}

// IndexStats returns current expensive index shape and memory diagnostics.
//
// In indexed mode it walks the published index snapshot and reports per-field
// entry counts, key bytes, posting cardinalities, and approximate memory
// usage for regular field value indexes plus synthetic nil-family indexes.
// On large databases this can be expensive.
//
// In transparent mode it returns a zero-valued IndexStats because no secondary
// indexes, universe bitmap, or string-key runtime mapping are maintained.
//
// If DB is closed or broken, IndexStats returns a zero value.
func (db *DB[K, V]) IndexStats() IndexStats {
	if err := db.unavailableErr(); err != nil {
		return IndexStats{}
	}

	if db.index == nil {
		return IndexStats{}
	}

	st := db.index.IndexStats()
	return IndexStats{
		UniqueFieldKeys:        st.UniqueFieldKeys,
		Size:                   st.Size,
		FieldSize:              st.FieldSize,
		FieldKeyBytes:          st.FieldKeyBytes,
		FieldTotalCardinality:  st.FieldTotalCardinality,
		FieldApproxStructBytes: st.FieldApproxStructBytes,
		FieldApproxHeapBytes:   st.FieldApproxHeapBytes,
		EntryCount:             st.EntryCount,
		KeyBytes:               st.KeyBytes,
		PostingCardinality:     st.PostingCardinality,
		ApproxStructBytes:      st.ApproxStructBytes,
		ApproxHeapBytes:        st.ApproxHeapBytes,
	}
}

func unregInstanceOnError(err *error, boltPath string, vname string) {
	if *err != nil {
		unregInstance(boltPath, vname)
	}
}

// AutoBatchStats returns auto-batcher queue, batch, and availability diagnostics.
//
// Runtime counters are collected only when Options.EnableAutoBatchStats was
// enabled for this DB instance; otherwise the method returns a zero value.
//
// The method is independent of indexed versus transparent mode. Write-path
// batching remains active in both modes, so when stats collection is enabled
// the returned counters and queue state reflect the current write workload.
func (db *DB[K, V]) AutoBatchStats() AutoBatchStats {
	return AutoBatchStats(db.batcher.Stats())
}

// PlannerStats returns the last published planner statistics snapshot together
// with current planner analyze/trace settings.
//
// In indexed mode the returned payload reflects the most recently published
// planner stats snapshot, whether it was loaded from a persisted sidecar or
// rebuilt in memory.
//
// In transparent mode no query engine and no runtime planner exist, so the
// returned planner payload is zero-valued.
func (db *DB[K, V]) PlannerStats() PlannerStats {
	var out PlannerStats
	if db.index == nil {
		return out
	}
	st := db.index.PlannerStats()
	out.Version = st.Version
	out.GeneratedAt = st.GeneratedAt
	out.UniverseCardinality = st.UniverseCardinality
	out.FieldCount = st.FieldCount
	out.Fields = st.Fields
	out.AnalyzeInterval = st.AnalyzeInterval
	out.TraceSampleEvery = st.TraceSampleEvery
	return out
}

// Close closes the indexer and persists the current index state
// to the .rbi file unless index persistence is disabled.
//
// After Close, all other methods return ErrClosed.
// Subsequent calls to Close are no-op.
//
// It does not close the underlying *bbolt.DB.
func (db *DB[K, V]) Close() error {
	if !db.closed.CompareAndSwap(false, true) {
		return nil
	}
	defer unregInstance(db.bolt.Path(), string(db.bucket))

	db.stopAnalyzeLoop()

	db.batcher.WakeWaiters()

	db.publishMu.Lock()
	defer db.publishMu.Unlock()

	var err error

	if db.index != nil && !db.options.DisableIndexStore && !db.broken.Load() {
		err = db.storeIndex()
	}

	if err == nil && db.broken.Load() {
		err = ErrBroken
	}

	return err
}

// Truncate deletes all values stored in the database. This cannot be undone.
//
// Truncate does not reclaim disk space.
func (db *DB[K, V]) Truncate() error {
	if err := db.unavailableErr(); err != nil {
		return err
	}

	// Keep writer lock order consistent with batched/single write paths:
	// open bbolt write tx first, then take db.publishMu.
	tx, err := db.bolt.Begin(true)
	if err != nil {
		return fmt.Errorf("tx error: %w", err)
	}
	defer rollback(tx)

	db.publishMu.Lock()
	defer db.publishMu.Unlock()

	if err = db.unavailableErr(); err != nil {
		return err
	}

	prevSeq := uint64(0)
	if bucket := tx.Bucket(db.bucket); bucket != nil {
		prevSeq = bucket.Sequence()
		if err = tx.DeleteBucket(db.bucket); err != nil {
			return fmt.Errorf("error deleting bucket: %w", err)
		}
	}

	bucket, err := tx.CreateBucketIfNotExists(db.bucket)
	if err != nil {
		return fmt.Errorf("error creating bucket: %w", err)
	}
	if err = bucket.SetSequence(prevSeq); err != nil {
		return fmt.Errorf("restore bucket sequence: %w", err)
	}
	if db.index == nil {
		if _, err = bucket.NextSequence(); err != nil {
			return fmt.Errorf("advance bucket sequence: %w", err)
		}
		if err = commitTx(tx, "truncate"); err != nil {
			return fmt.Errorf("commit error: %w", err)
		}
		return nil
	}
	seq, err := bucket.NextSequence()
	if err != nil {
		return fmt.Errorf("advance bucket sequence: %w", err)
	}

	staged := db.index.StageTruncate(seq)
	if err = commitTx(tx, "truncate"); err != nil {
		db.index.DropStaged(seq)
		return fmt.Errorf("commit error: %w", err)
	}

	if err = db.index.PublishCommittedStaged(&db.broken, db.logger, ErrBroken, seq, "truncate", staged); err != nil {
		return err
	}

	return nil
}

// ReleaseRecords returns records to the record pool.
//
// The caller transfers ownership of each passed pointer back to DB.
// Passed records must not be used after this call.
// There is no internal protection against double-release.
// If unsure about record ownership or lifecycle, do not use this method.
func (db *DB[K, V]) ReleaseRecords(v ...*V) {
	var zero V
	for _, rec := range v {
		if rec != nil {
			*rec = zero
			db.recPool.Put(rec)
		}
	}
}

// Bolt returns the underlying *bbolt.DB instance used by this DB.
// Should be used with caution; callers must not mutate the bucket managed by
// this DB instance, including its sequence counter.
func (db *DB[K, V]) Bolt() *bbolt.DB {
	return db.bolt
}

// BucketName returns a name of the bucket at which the data is stored.
func (db *DB[K, V]) BucketName() []byte {
	return slices.Clone(db.bucket)
}

var msgpackEncPool = pooled.Pointers[msgpack.Encoder]{
	New: func() *msgpack.Encoder { return msgpack.NewEncoder(io.Discard) },
	Cleanup: func(enc *msgpack.Encoder) {
		enc.Reset(io.Discard)
	},
}

var msgpackDecPool = pooled.Pointers[msgpack.Decoder]{
	New: func() *msgpack.Decoder {
		return msgpack.NewDecoder(strings.NewReader(""))
	},
}

var decodeReaderPool = pooled.Pointers[bytes.Reader]{
	Clear: true,
}

func defaultCodecMethods[V any]() (func(*V, io.Writer) error, func(*V, io.Reader) error, error) {
	t := reflect.TypeFor[*V]()
	if !t.Implements(codecType) {
		return nil, nil, nil
	}
	if _, bad := reflect.TypeFor[V]().MethodByName("DecodeRBI"); bad {
		return nil, nil, fmt.Errorf("invalid Codec implementation for %v: DecodeRBI must have pointer receiver", t)
	}

	encodeMethod, ok := t.MethodByName("EncodeRBI")
	if !ok {
		return nil, nil, nil
	}
	encodeFn, ok := encodeMethod.Func.Interface().(func(*V, io.Writer) error)
	if !ok {
		return nil, nil, nil
	}

	decodeMethod, ok := t.MethodByName("DecodeRBI")
	if !ok {
		return nil, nil, nil
	}
	decodeFn, ok := decodeMethod.Func.Interface().(func(*V, io.Reader) error)
	if !ok {
		return nil, nil, nil
	}

	return encodeFn, decodeFn, nil
}

func (db *DB[K, V]) decode(b []byte) (*V, error) {
	v := db.recPool.Get()

	reader := decodeReaderPool.Get()
	defer decodeReaderPool.Put(reader)

	reader.Reset(b)

	if db.decodeFn != nil {
		if err := db.decodeFn(v, reader); err != nil {
			db.ReleaseRecords(v)
			return nil, err
		}
		return v, nil
	}

	dec := msgpackDecPool.Get()
	defer msgpackDecPool.Put(dec)

	dec.Reset(reader)

	if err := dec.Decode(v); err != nil {
		db.ReleaseRecords(v)
		return nil, err
	}
	return v, nil
}

func (db *DB[K, V]) encode(v *V, b *bytes.Buffer) error {
	if db.encodeFn != nil {
		return db.encodeFn(v, b)
	}
	enc := msgpackEncPool.Get()
	enc.Reset(b)
	err := enc.Encode(v)
	msgpackEncPool.Put(enc)
	return err
}

func rollback(tx *bbolt.Tx) { _ = tx.Rollback() }

func validateBucketName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: empty", ErrInvalidBucketName)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if i == 0 {
			if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' {
				continue
			}
			return fmt.Errorf(
				"%w %q: allowed pattern is [A-Za-z_][A-Za-z0-9_]*",
				ErrInvalidBucketName,
				name,
			)
		}
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			continue
		}
		return fmt.Errorf(
			"%w %q: allowed pattern is [A-Za-z_][A-Za-z0-9_]*",
			ErrInvalidBucketName,
			name,
		)
	}
	return nil
}

var (
	registryMu sync.Mutex
	registry   = make(map[string]struct{})
)

func regInstance(dbPath, bucket string) error {
	registryMu.Lock()
	defer registryMu.Unlock()

	abs, err := filepath.Abs(dbPath)
	if err != nil {
		return fmt.Errorf("error getting absolute file path: %w", err)
	}

	key := abs + "::" + bucket
	if _, exists := registry[key]; exists {
		return fmt.Errorf("rbi is already open for \"%v\" at %v", bucket, dbPath)
	}

	registry[key] = struct{}{}
	return nil
}

func unregInstance(dbPath, bucket string) {
	registryMu.Lock()
	defer registryMu.Unlock()

	absPath, _ := filepath.Abs(dbPath)
	key := absPath + "::" + bucket
	delete(registry, key)
}
