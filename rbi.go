package rbi

import (
	"errors"
	"fmt"
	"log"
	"maps"
	"os"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"go.etcd.io/bbolt"
)

var (
	ErrNotStructType     = errors.New("value is not a struct")
	ErrClosed            = errors.New("database closed")
	ErrRebuildInProgress = errors.New("index rebuild in progress")
	ErrInvalidQuery      = errors.New("invalid query")
	ErrIndexDisabled     = errors.New("index is disabled")
	ErrUniqueViolation   = errors.New("unique constraint violation")
	ErrRecordNotFound    = errors.New("record not found")
	ErrNoValidKeyIndex   = errors.New("no valid key for index")
	ErrNilValue          = errors.New("value is nil")
)

const (
	defaultOptionsAnalyzeInterval                   = time.Hour
	defaultOptionsSnapshotPinWaitTimeout            = 1 * time.Second
	defaultOptionsCalibrationSampleEvery            = 16
	defaultBucketFillPercent                        = 0.8
	defaultSnapshotMaterializedPredCacheMaxEntries  = 16
	defaultSnapshotMatPredCacheEntriesWithDelta     = 2
	defaultSnapshotMatPredCacheMaxBitmapCardinality = 32 << 10
	defaultSnapshotRegistryMax                      = 16
	defaultSnapshotDeltaCompactFieldKeys            = 256
	defaultSnapshotDeltaCompactFieldOps             = 4 << 10
	defaultSnapshotDeltaCompactMaxFieldsPerPublish  = 3
	defaultSnapshotDeltaCompactUniverseOps          = 4 << 10
	defaultSnapshotDeltaLayerMaxDepth               = 6
	defaultSnapshotCompactorMaxIterationsPerRun     = 2
	defaultSnapshotCompactorRequestEveryNWrites     = 8
	defaultSnapshotCompactorIdleInterval            = 2 * time.Second
	defaultBatchWindow                              = 200 * time.Microsecond
	defaultBatchMax                                 = 16
	defaultBatchMaxQueue                            = 512
	defaultBatchAllowCallbacks                      = false
	defaultSnapshotDeltaCompactLargeBaseFieldKeys   = 128 << 10
	defaultSnapshotDeltaCompactLargeBaseMinDeltaDiv = 32
	defaultSnapshotDeltaCompactForceFieldOps        = 256 << 10
	defaultSnapshotCompactorUrgentDepthSlack        = 4
	defaultSnapshotStrMapCompactDepth               = 256
	defaultNumericRangeBucketSize                   = 512
	defaultNumericRangeBucketMinFieldKeys           = 8192
	defaultNumericRangeBucketMinSpanKeys            = 2048
)

// Options configures indexer and how it works with a bbolt database.
//
// Zero-valued option fields use defaults.
// DefaultOptions returns options with all defaults pre-filled.
type Options struct {

	// DisableIndexLoad prevents indexer from loading previously persisted index
	// data from the .rbi file on startup. If set, indexer will either rebuild
	// the index from the underlying bucket (unless DisableIndexRebuild is
	// also set) or operate with an empty index until rebuilt.
	DisableIndexLoad bool

	// DisableIndexStore prevents indexer from saving the in-memory index state
	// to the .rbi file on Close.
	DisableIndexStore bool

	// DisableIndexRebuild skips automatic index rebuilding when the index
	// cannot be loaded or is incomplete. When this is true and the index
	// cannot be reused, queries will fail until the caller explicitly
	// rebuilds the index using RebuildIndex.
	DisableIndexRebuild bool

	// BucketName overrides the default bucket name.
	// By default, bucket name is derived from the name of the value type V.
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

	// SnapshotPinWaitTimeout controls how long Query waits for a snapshot
	// with matching Bolt txID to appear after opening a read transaction.
	//
	// Negative value disables waiting.
	//
	// Default: 1s
	//
	// Query retry budget is bounded by 30x this value.
	// Too low can increase "snapshot is not available" errors under write bursts.
	// Too high can increase tail latency when snapshot publication is delayed.
	SnapshotPinWaitTimeout time.Duration

	// CalibrationEnabled enables online self-calibration of planner
	// cost coefficients using sampled query traces.
	//
	// Enable for workloads with evolving predicate selectivity/cost profile.
	// Keep disabled when strict latency determinism is preferred.
	CalibrationEnabled bool

	// CalibrationSampleEvery controls calibration sampling:
	//   - 0: use default sampling interval
	//   - 1: calibrate every query
	//   - N: calibrate every Nth query
	// The value is ignored when CalibrationEnabled is false.
	//
	// Negative value disables sampled calibration.
	//
	// Default: 16
	//
	// Lower values adapt faster but add overhead and sensitivity to noise.
	// Higher values reduce overhead but adapt slower to workload shifts.
	CalibrationSampleEvery int

	// PersistCalibration enables automatic load/save of planner calibration
	// state in a sidecar JSON file next to the Bolt DB.
	//
	// File name is derived from the Bolt path and bucket name, using the
	// same pattern as the index sidecar but with ".cal" extension.
	//
	// Default: false (disabled).
	PersistCalibration bool

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
	// materialized predicate bitmaps for stable snapshots (without deltas).
	//
	// Negative value disables cache for stable snapshots.
	//
	// Default: 16
	//
	// Typical range: 16..256
	//
	// High values on diverse workloads can cause sharp memory growth.
	SnapshotMaterializedPredCacheMaxEntries int

	// SnapshotMaterializedPredCacheMaxEntriesWithDelta controls max cached
	// materialized predicate bitmaps for snapshots with active deltas.
	//
	// Negative value disables cache for delta snapshots.
	//
	// Default: 2
	//
	// Typical range: 2..64
	//
	// High values can increase GC pressure under write-heavy workloads.
	SnapshotMaterializedPredCacheMaxEntriesWithDelta int

	// SnapshotMaterializedPredCacheMaxBitmapCardinality skips caching very large
	// bitmaps to reduce retained heap and GC pressure.
	//
	// Negative value disables the guard.
	//
	// Default: 32K
	//
	// Negative (disabled) or very large values can significantly increase memory
	// usage for broad predicates.
	SnapshotMaterializedPredCacheMaxBitmapCardinality int

	// SnapshotRegistryMax limits amount of snapshots tracked for txID pinning/floor fallback.
	//
	// Negative value disables the cap.
	//
	// Default: 16
	//
	// Typical range: 32..512
	//
	// Higher values retain more snapshots (higher memory).
	// Too low values can increase snapshot misses for long readers.
	SnapshotRegistryMax int

	// SnapshotDeltaCompactFieldKeys is a per-field threshold for accumulated
	// delta keys; above it field delta is compacted into base index.
	//
	// Negative value disables key-count trigger.
	//
	// Default: 256
	//
	// Typical range: 128..2048
	//
	// Too high can increase delta memory/read CPU.
	// Too low can increase compaction frequency and hurt write latency.
	SnapshotDeltaCompactFieldKeys int

	// SnapshotDeltaCompactFieldOps is a per-field threshold for accumulated
	// add/del cardinality across delta entries.
	//
	// Negative value disables ops-count trigger.
	//
	// Default: 4096
	//
	// Typical range: 2K..64K
	//
	// Too high delays compaction (delta growth, read overhead).
	// Too low can force frequent compaction and hurt write throughput.
	SnapshotDeltaCompactFieldOps int

	// SnapshotDeltaCompactMaxFieldsPerPublish limits how many fields can be
	// compacted from delta into base in one publish pass.
	//
	// Negative value disables field compaction in publish path.
	//
	// Default: 3
	//
	// High values can create severe write-latency spikes.
	SnapshotDeltaCompactMaxFieldsPerPublish int

	// SnapshotDeltaCompactUniverseOps is a threshold for universe add/drop
	// cardinality sum; above it universe delta is compacted into base.
	//
	// Negative value disables universe compaction trigger.
	//
	// Default: 4096
	//
	// Typical values: 2K..64K
	//
	// Too high values can increase overlay growth/read cost.
	// Too low values can increase write-path compaction work.
	SnapshotDeltaCompactUniverseOps int

	// SnapshotDeltaLayerMaxDepth limits per-field delta layer depth.
	// Once exceeded, layered delta is flattened into one layer.
	//
	// Negative value disables depth-based flattening.
	//
	// Default: 6
	//
	// Typical range: 4..64
	//
	// Very high/disabled values can increase read-path CPU and memory.
	// Very low values can increase flattening overhead on writes.
	SnapshotDeltaLayerMaxDepth int

	// SnapshotCompactorMaxIterationsPerRun limits background compaction work per wake-up.
	//
	// Negative value disables compaction passes.
	//
	// Default: 2
	//
	// Typical range: 1..8
	//
	// High values increase contention with writers and can degrade throughput.
	SnapshotCompactorMaxIterationsPerRun int

	// SnapshotCompactorRequestEveryNWrites controls best-effort compactor
	// wakeups under steady write load.
	//
	// Negative value disables periodic write-triggered compactor requests.
	//
	// Default: 8
	//
	// Typical range: 4..64
	//
	// Lower values improve delta control but increase write contention.
	// Higher values reduce contention but can increase delta memory/read cost.
	//
	// Value 1 can cause sustained compactor/writer contention and write
	// throughput degradation on heavy write workloads.
	SnapshotCompactorRequestEveryNWrites int

	// SnapshotCompactorIdleInterval configures one-shot idle debounce for
	// force-drain compaction when snapshot activity stops.
	// After this pause without new snapshot publication, compactor performs a
	// bounded force pass to collapse remaining deltas and aggressively prune
	// snapshot registry for best read-path locality.
	//
	// Negative value disables idle force-drain mode.
	//
	// Default: 2s
	//
	// Typical range: 500ms..10s
	//
	// Lower values converge faster after write bursts but can increase
	// compactor/writer contention on bursty workloads.
	// Higher values reduce background churn but keep layered state longer.
	SnapshotCompactorIdleInterval time.Duration

	// BatchWindow enables lightweight write micro-batching window for
	// single-record Set/Patch/Delete operations.
	//
	// Negative value disables write combining.
	//
	// Default: 200us
	//
	// Typical range: 10us..500us
	//
	// Higher values can reduce write-path overhead under contention but may
	// increase single-write latency at low load.
	BatchWindow time.Duration

	// BatchMax limits max operations merged into one combined write tx.
	//
	// Negative value disables effective batching.
	// Value 1 also disables effective batching (single op per batch).
	//
	// Default: 16
	//
	// Typical range: 4..64
	//
	// Very high values can create commit-size spikes and tail-latency variance.
	BatchMax int

	// BatchMaxQueue limits pending combined write requests.
	//
	// Negative value disables queue cap.
	//
	// Default: 512
	//
	// Typical range: 128..8192
	//
	// Larger values can increase memory usage under sustained overload.
	BatchMaxQueue int

	// BatchAllowCallbacks allows combiner batching for requests with one or more
	// PreCommit callbacks.
	//
	// Default: false
	//
	// When false, any Set/Patch/Delete call with callbacks bypasses combiner queue
	// and is executed via direct single-write path.
	//
	// When true, callback-bearing requests may be combined with other writes and
	// callbacks run inside the same shared write transaction.
	//
	// Limitations:
	// - A callback error aborts the current combined-transaction attempt. The
	//   failed request is isolated and remaining requests are retried without it.
	// - On such abort, the whole current write transaction is rolled back.
	//   Stored records and in-memory index state remain consistent; no partial
	//   data/index changes from the failed attempt are published.
	// - Non-callback transaction errors (put/delete/commit) still fail all
	//   requests from the current combined batch.
	// - Callback execution order follows operation order inside combined batch.
	// - Because surviving requests can be retried after isolating a failed one,
	//   their callbacks may run more than once. Callback logic with side effects
	//   outside this DB write transaction should be idempotent.
	BatchAllowCallbacks bool

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
	// Default: 2048
	NumericRangeBucketMinSpanKeys int
}

func (o *Options) setDefaults() {
	if o.AnalyzeInterval == 0 {
		o.AnalyzeInterval = defaultOptionsAnalyzeInterval
	}
	if o.TraceSampleEvery == 0 {
		o.TraceSampleEvery = 1
	}
	if o.SnapshotPinWaitTimeout == 0 {
		o.SnapshotPinWaitTimeout = defaultOptionsSnapshotPinWaitTimeout
	}
	if o.CalibrationSampleEvery == 0 {
		o.CalibrationSampleEvery = defaultOptionsCalibrationSampleEvery
	}
	if o.BucketFillPercent == 0 {
		o.BucketFillPercent = defaultBucketFillPercent
	}
	if o.SnapshotMaterializedPredCacheMaxEntries == 0 {
		o.SnapshotMaterializedPredCacheMaxEntries = defaultSnapshotMaterializedPredCacheMaxEntries
	}
	if o.SnapshotMaterializedPredCacheMaxEntriesWithDelta == 0 {
		o.SnapshotMaterializedPredCacheMaxEntriesWithDelta = defaultSnapshotMatPredCacheEntriesWithDelta
	}
	if o.SnapshotMaterializedPredCacheMaxBitmapCardinality == 0 {
		o.SnapshotMaterializedPredCacheMaxBitmapCardinality = defaultSnapshotMatPredCacheMaxBitmapCardinality
	}
	if o.SnapshotRegistryMax == 0 {
		o.SnapshotRegistryMax = defaultSnapshotRegistryMax
	} else if o.SnapshotRegistryMax < 0 {
		o.SnapshotRegistryMax = int(^uint(0) >> 1)
	}
	if o.SnapshotDeltaCompactFieldKeys == 0 {
		o.SnapshotDeltaCompactFieldKeys = defaultSnapshotDeltaCompactFieldKeys
	}
	if o.SnapshotDeltaCompactFieldOps == 0 {
		o.SnapshotDeltaCompactFieldOps = defaultSnapshotDeltaCompactFieldOps
	}
	if o.SnapshotDeltaCompactMaxFieldsPerPublish == 0 {
		o.SnapshotDeltaCompactMaxFieldsPerPublish = defaultSnapshotDeltaCompactMaxFieldsPerPublish
	}
	if o.SnapshotDeltaCompactUniverseOps == 0 {
		o.SnapshotDeltaCompactUniverseOps = defaultSnapshotDeltaCompactUniverseOps
	}
	if o.SnapshotDeltaLayerMaxDepth == 0 {
		o.SnapshotDeltaLayerMaxDepth = defaultSnapshotDeltaLayerMaxDepth
	}
	if o.SnapshotCompactorMaxIterationsPerRun == 0 {
		o.SnapshotCompactorMaxIterationsPerRun = defaultSnapshotCompactorMaxIterationsPerRun
	}
	if o.SnapshotCompactorRequestEveryNWrites == 0 {
		o.SnapshotCompactorRequestEveryNWrites = defaultSnapshotCompactorRequestEveryNWrites
	}
	if o.SnapshotCompactorIdleInterval == 0 {
		o.SnapshotCompactorIdleInterval = defaultSnapshotCompactorIdleInterval
	}
	if o.BatchWindow == 0 {
		o.BatchWindow = defaultBatchWindow
	}
	if o.BatchMax == 0 {
		o.BatchMax = defaultBatchMax
	}
	if o.BatchMaxQueue == 0 {
		o.BatchMaxQueue = defaultBatchMaxQueue
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

// PreCommitFunc is a callback invoked inside the write transaction just before
// it is committed.
//
// The callback:
//   - Must not modify oldValue or newValue.
//   - Must not commit or roll back the transaction.
//   - Must not modify records in the bucket managed by this DB instance
//     (or by any other DB instance with enabled indexing),
//     because such writes bypass index synchronization.
//   - May perform additional reads or writes within the same transaction.
//   - May return an error to abort the operation; in this case the
//     transaction will be rolled back and index state will not be updated.
//
// PreCommitFunc is invoked only for records that exist or are being written.
// Patch/Delete operations skip missing records and do not invoke callbacks for them.
type PreCommitFunc[K ~string | ~uint64, V any] = func(tx *bbolt.Tx, key K, oldValue, newValue *V) error

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

// New creates a new indexed DB that uses the provided bbolt database.
//
// The generic type V must be a struct; otherwise ErrNotStructType is returned.
//
// Zero-valued option fields use defaults.
//
// If options.BucketName is empty,
// the name of the value type V is used as the bucket name.
// New ensures the bucket exists, optionally loads a persisted index from disk,
// rebuilds missing parts of the index if allowed, and sets up field metadata
// and accessors.
//
// The resulting DB does not manage the underlying *bbolt.DB lifecycle.
func New[K ~uint64 | ~string, V any](bolt *bbolt.DB, options Options) (db *DB[K, V], err error) {
	var v V
	vtype := reflect.TypeOf(v)
	if vtype == nil {
		return nil, ErrNotStructType
	}
	if vtype.Kind() != reflect.Struct {
		return nil, ErrNotStructType
	}
	options.setDefaults()

	vname := vtype.Name()
	if options.BucketName != "" {
		vname = options.BucketName
	}
	if vname == "" {
		return nil, fmt.Errorf("cannot resolve value name of %v", vtype)
	}
	if bolt == nil {
		return nil, fmt.Errorf("bolt instance is nil")
	}

	boltPath := bolt.Path()

	if err = regInstance(boltPath, vname); err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			unregInstance(boltPath, vname)
		}
	}()

	calPath := ""
	if options.PersistCalibration {
		calPath = bolt.Path() + "." + sanitizeSuffix(vname) + ".cal"
	}

	db = &DB[K, V]{
		bolt:   bolt,
		vtype:  vtype,
		strmap: newStrMapper(0, defaultSnapshotStrMapCompactDepth),
		bucket: []byte(vname),

		fields:            make(map[string]*field),
		getters:           make(map[string]getterFn),
		index:             make(map[string]*[]index),
		lenIndex:          make(map[string]*[]index),
		lenZeroComplement: make(map[string]bool),

		universe: roaring64.NewBitmap(),

		rbiFile: bolt.Path() + "." + sanitizeSuffix(vname) + ".rbi",
		opnFile: bolt.Path() + "." + sanitizeSuffix(vname) + ".rbo",

		recPool: sync.Pool{
			New: func() any {
				return new(V)
			},
		},
		viewPool: sync.Pool{
			New: func() any {
				return new(DB[K, V])
			},
		},

		options: &options,

		snapshot: snapshot{
			pinWait: snapshotPinWaitTimeout(options.SnapshotPinWaitTimeout),
		},

		planner: planner{
			analyzer: analyzer{
				interval:   plannerAnalyzeInterval(options.AnalyzeInterval),
				softBudget: defaultAnalyzeSoftBudget,
			},
			tracer: tracer{
				sink:        options.TraceSink,
				sampleEvery: traceSampleEvery(options.TraceSampleEvery, options.TraceSink),
			},
			calibrator: calibrator{
				enabled:     options.CalibrationEnabled,
				sampleEvery: calibrationSampleEvery(options.CalibrationEnabled, options.CalibrationSampleEvery),
				persistPath: calPath,
			},
		},
	}
	db.rebuilder.cond = sync.NewCond(&db.rebuilder.mu)

	var k K
	if reflect.TypeOf(k).Kind() == reflect.String {
		db.strkey = true
	}

	if err = db.populateFields(vtype, nil); err != nil {
		return nil, fmt.Errorf("failed to populate index fields: %w", err)
	}

	for _, f := range db.fields {
		if db.getters[f.DBName], err = db.makeGetter(f); err != nil {
			return nil, fmt.Errorf("failed to create accessor func for %v: %w", f.Name, err)
		}
	}
	db.initIndexedFieldAccessors()
	db.initBatcher()

	err = bolt.Update(func(tx *bbolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists(db.bucket)
		return e
	})
	if err != nil {
		return nil, err
	}

	var skipFields map[string]struct{}

	if _, err = os.Stat(db.opnFile); err == nil {
		log.Println("rbi: unclean shutdown detected")
	} else {
		if _, err = os.Stat(db.rbiFile); err == nil {
			if !options.DisableIndexLoad {
				skipFields, err = db.loadIndex()
				if err != nil {
					log.Println("rbi: failed to load persisted index:", err)
				}
			}
		}
	}

	if !options.DisableIndexRebuild {
		if err = db.buildIndex(skipFields); err != nil {
			return nil, fmt.Errorf("error building index: %w", err)
		}
	}

	db.patchMap = make(map[string]*field)
	if err = db.populatePatcher(vtype, nil); err != nil {
		return nil, fmt.Errorf("failed to populate patch fields: %w", err)
	}

	for name := range db.fields {
		db.fieldSlice = append(db.fieldSlice, name)
	}
	db.publishSnapshotNoLock(db.currentBoltTxID())

	if err = db.RefreshPlannerStats(); err != nil {
		return nil, fmt.Errorf("failed to build planner stats snapshot: %w", err)
	}

	db.initCalibration()

	if err = touch(db.opnFile); err != nil {
		return nil, fmt.Errorf("error creating flag-file: %w", err)
	}

	db.startPlannerAnalyzeLoop()
	db.startSnapshotCompactor()

	return db, nil
}

func (db *DB[K, V]) initIndexedFieldAccessors() {
	if len(db.fields) == 0 {
		db.indexedFieldAccess = nil
		db.indexedFieldByName = nil
		db.uniqueFieldAccessors = nil
		return
	}

	db.indexedFieldAccess = make([]indexedFieldAccessor, 0, len(db.fields))
	db.indexedFieldByName = make(map[string]indexedFieldAccessor, len(db.fields))
	db.uniqueFieldAccessors = make([]indexedFieldAccessor, 0, 4)

	for _, f := range db.fields {
		acc := indexedFieldAccessor{
			name:   f.DBName,
			field:  f,
			getter: db.getters[f.DBName],
		}
		db.indexedFieldAccess = append(db.indexedFieldAccess, acc)
		db.indexedFieldByName[f.DBName] = acc
		if f.Unique && !f.Slice {
			db.uniqueFieldAccessors = append(db.uniqueFieldAccessors, acc)
		}
	}
}

func (db *DB[K, V]) initBatcher() {
	maxOps := db.options.BatchMax
	if maxOps <= 1 || db.options.BatchWindow <= 0 {
		db.combiner = combiner[K, V]{}
		return
	}
	maxQueue := db.options.BatchMaxQueue
	if maxQueue < 0 {
		maxQueue = 0
	}
	capHint := max(64, maxOps*4)
	if maxQueue > 0 && maxQueue < capHint {
		capHint = maxQueue
	}
	db.combiner = combiner[K, V]{
		enabled:        true,
		window:         db.options.BatchWindow,
		maxOps:         maxOps,
		maxQ:           maxQueue,
		allowCallbacks: db.options.BatchAllowCallbacks,
		queue:          make([]*combineRequest[K, V], 0, capHint),
	}
	db.combiner.retCond = sync.NewCond(&db.combiner.mu)
}

type planner struct {
	statsVersion atomic.Uint64
	stats        atomic.Pointer[plannerStatsSnapshot]

	analyzer   analyzer
	tracer     tracer
	calibrator calibrator
}

type analyzer struct {
	interval   time.Duration
	stop       chan struct{}
	done       chan struct{}
	softBudget time.Duration
	cursor     int

	sync.Mutex
}

type tracer struct {
	sink        func(TraceEvent)
	sampleEvery uint64
	seq         atomic.Uint64
}

type calibrator struct {
	enabled     bool
	sampleEvery uint64
	seq         atomic.Uint64
	state       atomic.Pointer[calibration]
	persistPath string

	sync.Mutex
}

type snapshot struct {
	current atomic.Pointer[indexSnapshot]

	byTx  map[uint64]*snapshotRef
	order []uint64
	head  int

	pinWait time.Duration

	compactReq  chan struct{}
	compactIdle chan struct{}
	compactStop chan struct{}
	compactDone chan struct{}

	compactForcePending atomic.Bool
	compactPinsBlocked  atomic.Bool
	compactLastActivity atomic.Int64

	compactRequested atomic.Uint64
	compactRuns      atomic.Uint64
	compactAttempts  atomic.Uint64
	compactSucceeded atomic.Uint64
	compactLockMiss  atomic.Uint64
	compactNoChange  atomic.Uint64
	compactWriteSeq  atomic.Uint64
	compactSkipUntil atomic.Uint64

	mu sync.RWMutex
}

type indexedFieldAccessor struct {
	name   string
	field  *field
	getter getterFn
}

type combiner[K ~string | ~uint64, V any] struct {
	enabled        bool
	window         time.Duration
	maxOps         int
	maxQ           int
	allowCallbacks bool

	mu       sync.Mutex
	retCond  *sync.Cond
	running  bool
	queue    []*combineRequest[K, V]
	emitSeq  uint64
	retSeq   uint64
	hotUntil time.Time

	submitted          atomic.Uint64
	enqueued           atomic.Uint64
	dequeued           atomic.Uint64
	batches            atomic.Uint64
	combinedBatches    atomic.Uint64
	combinedOps        atomic.Uint64
	callbackOps        atomic.Uint64
	coalescedSetDelete atomic.Uint64
	maxBatchSeen       atomic.Uint64
	queueHighWater     atomic.Uint64
	coalesceWaits      atomic.Uint64
	coalesceWaitNanos  atomic.Uint64

	fallbackDisabled    atomic.Uint64
	fallbackQueueFull   atomic.Uint64
	fallbackCallbacks   atomic.Uint64
	fallbackPatchUnique atomic.Uint64
	fallbackClosed      atomic.Uint64

	uniqueRejected atomic.Uint64
	txBeginErrors  atomic.Uint64
	txOpErrors     atomic.Uint64
	txCommitErrors atomic.Uint64
	callbackErrors atomic.Uint64
}

type rebuilder struct {
	active   atomic.Bool
	inflight atomic.Int64
	mu       sync.Mutex
	cond     *sync.Cond
}

type testHooks struct {
	beforeCommit     func(op string) error
	beforeStoreIndex func() error
}

type (
	// DB wraps a bbolt database and maintains secondary indexes over values of type *V
	// stored in a single bucket. It supports efficient equality and range queries,
	// as well as array membership and array-length queries for slice fields.
	//
	// DB is safe for concurrent use.
	DB[K ~string | ~uint64, V any] struct {
		bolt  *bbolt.DB
		vtype reflect.Type

		bucket []byte

		strkey     bool
		strmap     *strMapper
		strmapView *strMapSnapshot

		universe *roaring64.Bitmap

		fields         map[string]*field
		fieldSlice     []string
		hasUnique      bool
		lenIndexLoaded bool

		getters              map[string]getterFn
		indexedFieldAccess   []indexedFieldAccessor
		indexedFieldByName   map[string]indexedFieldAccessor
		uniqueFieldAccessors []indexedFieldAccessor
		index                map[string]*[]index
		lenIndex             map[string]*[]index
		lenZeroComplement    map[string]bool
		patchMap             map[string]*field

		planner   planner
		snapshot  snapshot
		combiner  combiner[K, V]
		rebuilder rebuilder

		options *Options

		rbiFile string
		opnFile string

		recPool  sync.Pool
		viewPool sync.Pool

		mu      sync.RWMutex
		closed  atomic.Bool
		noIndex atomic.Bool

		stats Stats[K]

		traceRoot *DB[K, V]
		testHooks testHooks
	}

	index struct {
		Key indexKey
		IDs postingList
	}

	// Stats is an aggregate diagnostic snapshot of DB state.
	//
	// It combines outputs of IndexStats, SnapshotStats, PlannerStats, CalibrationStats and BatchStats.
	//
	// For scenario-specific telemetry, prefer calling the corresponding
	// component method directly to avoid unnecessary work.
	Stats[K ~uint64 | ~string] struct {
		// Index contains additional index shape diagnostics useful for memory analysis.
		Index IndexStats[K]
		// Snapshot contains copy-on-write snapshot/compactor diagnostics.
		Snapshot SnapshotStats
		// Planner contains current planner statistics snapshot and settings.
		Planner PlannerStats
		// Calibration contains current online planner calibration state.
		Calibration CalibrationStats
		// Batch contains write-combiner queue/batch/fallback diagnostics.
		Batch BatchStats
	}

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

	// CalibrationStats contains online planner calibration diagnostics.
	CalibrationStats struct {
		// Enabled reports whether online calibration is enabled.
		Enabled bool
		// SampleEvery controls calibration sampling frequency (every Nth query).
		SampleEvery uint64

		// UpdatedAt is the timestamp of the last calibration state update.
		UpdatedAt time.Time
		// SamplesTotal is the total number of accumulated calibration samples.
		SamplesTotal uint64

		// Multipliers stores per-plan calibration multipliers.
		Multipliers map[string]float64
		// Samples stores per-plan calibration sample counters.
		Samples map[string]uint64
	}

	// IndexStats contains index build/load timings and current index shape metrics.
	IndexStats[K ~uint64 | ~string] struct {
		// IndexBuildTime is the duration of the last index rebuild.
		BuildTime time.Duration
		// IndexBuildRPS is an approximate throughput (records per second) of the last index rebuild.
		BuildRPS int
		// IndexLoadTime is the time spent loading a persisted index from disk on the last successful load.
		LoadTime time.Duration

		// LastKey is the largest key present in the database according to the
		// current universe bitmap. For string keys this is derived from the
		// internal string mapping.
		LastKey K
		// KeyCount is the total number of keys currently present in the database.
		KeyCount uint64

		// UniqueFieldKeys contains the number of unique index keys per indexed field name.
		UniqueFieldKeys map[string]uint64
		// IndexSize contains the total size of the index, in bytes.
		Size uint64
		// IndexFieldSize contains the size of the index for each indexed field.
		FieldSize map[string]uint64
		// IndexFieldKeyBytes contains total bytes occupied by index keys per field.
		FieldKeyBytes map[string]uint64
		// IndexFieldTotalCardinality contains sum of posting-list cardinalities per field.
		FieldTotalCardinality map[string]uint64

		// FieldCount is the number of indexed fields in the current snapshot.
		FieldCount int

		// EntryCount is the total number of non-empty index entries across fields.
		EntryCount uint64
		// KeyBytes is the total byte length of index keys across entries.
		KeyBytes uint64
		// BitmapCardinality is the sum of posting bitmap cardinalities across entries.
		BitmapCardinality uint64

		// ApproxStructBytes is approximate memory used by index entry structs.
		ApproxStructBytes uint64
		// ApproxHeapBytes is rough index heap estimate from bitmaps, keys and structs.
		ApproxHeapBytes uint64
	}

	// BatchStats contains write-combiner queue/batch/fallback diagnostics.
	BatchStats struct {
		// Enabled reports whether write combining is enabled.
		Enabled bool
		// Window is current coalescing window duration.
		Window time.Duration
		// MaxBatch is configured maximum combined batch size.
		MaxBatch int
		// MaxQueue is configured maximum queue size (0 means unbounded).
		MaxQueue int
		// AllowCallbacks reports whether callback-bearing writes can be combined.
		AllowCallbacks bool

		// QueueLen is current pending requests in queue.
		QueueLen int
		// QueueCap is current allocated queue capacity.
		QueueCap int
		// WorkerRunning reports whether combiner worker goroutine is active.
		WorkerRunning bool
		// HotWindowActive reports whether adaptive hot coalescing window is active.
		HotWindowActive bool

		// Submitted is number of submit attempts from eligible write calls.
		Submitted uint64
		// Enqueued is number of requests accepted into combiner queue.
		Enqueued uint64
		// Dequeued is number of requests popped from queue for execution.
		Dequeued uint64
		// QueueHighWater is maximum observed queue length.
		QueueHighWater uint64

		// Batches is total executed combined-transaction batches.
		Batches uint64
		// CombinedBatches is number of batches containing more than one request.
		CombinedBatches uint64
		// CombinedOps is total requests executed inside multi-request batches.
		CombinedOps uint64
		// AvgBatchSize is average requests per executed batch.
		AvgBatchSize float64
		// MaxBatchSeen is maximum observed executed batch size.
		MaxBatchSeen uint64

		// CallbackOps is number of requests with PreCommit callbacks executed by combiner.
		CallbackOps uint64
		// CoalescedSetDelete is number of Set/Delete requests collapsed into later Set/Delete of same ID.
		CoalescedSetDelete uint64

		// CoalesceWaits is number of coalescing sleeps performed by worker.
		CoalesceWaits uint64
		// CoalesceWaitTime is total time spent sleeping for coalescing.
		CoalesceWaitTime time.Duration

		// FallbackDisabled is number of write calls not queued because combiner is disabled.
		FallbackDisabled uint64
		// FallbackQueueFull is number of write calls not queued because queue is full.
		FallbackQueueFull uint64
		// FallbackCallbacks is number of write calls not queued because callbacks
		// are present and callback batching is disabled.
		FallbackCallbacks uint64
		// FallbackPatchUnique is a legacy counter of patch calls not queued due to
		// potential unique-field touch (kept for compatibility; expected to stay 0).
		FallbackPatchUnique uint64
		// FallbackClosed is number of write calls rejected by combiner because DB is closed.
		FallbackClosed uint64

		// UniqueRejected is number of queued requests rejected by unique checks before commit.
		UniqueRejected uint64
		// TxBeginErrors is number of write tx begin failures inside combiner.
		TxBeginErrors uint64
		// TxOpErrors is number of write tx operation failures before commit.
		TxOpErrors uint64
		// TxCommitErrors is number of write tx commit failures.
		TxCommitErrors uint64
		// CallbackErrors is number of callback failures returned by PreCommit funcs.
		CallbackErrors uint64
	}

	// SnapshotStats contains copy-on-write snapshot and compactor diagnostics.
	SnapshotStats struct {
		// TxID is the transaction ID of the published snapshot.
		TxID uint64

		// HasDelta reports whether snapshot contains any delta state.
		HasDelta bool
		// UniverseBaseCard is cardinality of the base universe bitmap.
		UniverseBaseCard uint64
		// IndexLayerDepth is depth of index delta layer chain.
		IndexLayerDepth int
		// LenLayerDepth is depth of length-index delta layer chain.
		LenLayerDepth int

		// IndexDeltaFields is number of fields with effective index delta.
		IndexDeltaFields int
		// LenDeltaFields is number of fields with effective length delta.
		LenDeltaFields int
		// IndexDeltaKeys is total effective keys in index delta layers.
		IndexDeltaKeys int
		// LenDeltaKeys is total effective keys in length delta layers.
		LenDeltaKeys int
		// IndexDeltaOps is total effective operations in index delta layers.
		IndexDeltaOps uint64
		// LenDeltaOps is total effective operations in length delta layers.
		LenDeltaOps uint64

		// UniverseAddCard is cardinality of pending universe additions.
		UniverseAddCard uint64
		// UniverseRemCard is cardinality of pending universe removals.
		UniverseRemCard uint64

		// RegistrySize is number of snapshot entries tracked in registry map.
		RegistrySize int
		// RegistryOrderLen is length of registry order buffer.
		RegistryOrderLen int
		// RegistryHead is current head offset inside registry order buffer.
		RegistryHead int
		// PinnedRefs is number of registry snapshots with active pins.
		PinnedRefs int
		// PendingRefs is number of registry snapshots marked pending.
		PendingRefs int

		// CompactorQueueLen is current compactor request queue length.
		CompactorQueueLen int
		// CompactorRequested is total number of compaction requests.
		CompactorRequested uint64
		// CompactorRuns is total number of compactor loop runs.
		CompactorRuns uint64
		// CompactorAttempts is total latest-snapshot compaction attempts.
		CompactorAttempts uint64
		// CompactorSucceeded is total successful compactions applied.
		CompactorSucceeded uint64
		// CompactorLockMiss is total attempts skipped due to DB lock contention.
		CompactorLockMiss uint64
		// CompactorNoChange is total attempts that produced no effective changes.
		CompactorNoChange uint64
	}
)

// DisableSync disables fsync for bolt writes. Can help with batch inserts.
// It should not be used during normal operation.
func (db *DB[K, V]) DisableSync() { db.bolt.NoSync = true }

// EnableSync enables fsync for bolt writes. See DisableSync.
// By default, fsync is enabled.
func (db *DB[K, V]) EnableSync() {
	db.bolt.NoSync = false
	_ = db.bolt.Sync()
}

// DisableIndexing disables index updates for subsequent write operations .
//
// When indexing is disabled:
//   - Index structures are no longer kept up to date.
//   - Query, QueryKeys and Count will return an error, because the index is considered invalid.
//   - The caller is responsible for rebuilding the index via RebuildIndex before attempting to run queries again.
//
// This is intended for high-throughput batch writes where the index will be rebuilt later.
func (db *DB[K, V]) DisableIndexing() { db.noIndex.Store(true) }

// EnableIndexing re-enables index updates for subsequent write operations.
// It does not automatically rebuild or validate any existing index state.
//
// If indexing was previously disabled and writes were performed, the index
// may be stale or inconsistent until RebuildIndex is called.
func (db *DB[K, V]) EnableIndexing() { db.noIndex.Store(false) }

func (db *DB[K, V]) beginOp() error {
	if db.closed.Load() {
		return ErrClosed
	}
	if db.rebuilder.active.Load() {
		return ErrRebuildInProgress
	}

	db.rebuilder.inflight.Add(1)
	if db.closed.Load() {
		db.endOp()
		return ErrClosed
	}
	if db.rebuilder.active.Load() {
		db.endOp()
		return ErrRebuildInProgress
	}
	return nil
}

func (db *DB[K, V]) beginOpWait() bool {
	for {
		if db.closed.Load() {
			return false
		}
		if !db.rebuilder.active.Load() {
			db.rebuilder.inflight.Add(1)
			if db.closed.Load() {
				db.endOp()
				return false
			}
			if !db.rebuilder.active.Load() {
				return true
			}
			db.endOp()
		}
		db.rebuilder.mu.Lock()
		for db.rebuilder.active.Load() {
			db.rebuilder.cond.Wait()
		}
		db.rebuilder.mu.Unlock()
	}
}

func (db *DB[K, V]) endOp() {
	if db.rebuilder.inflight.Add(-1) == 0 {
		db.rebuilder.mu.Lock()
		db.rebuilder.cond.Broadcast()
		db.rebuilder.mu.Unlock()
	}
}

func (db *DB[K, V]) beginRebuildSuspend() error {
	if db.closed.Load() {
		return ErrClosed
	}
	if !db.rebuilder.active.CompareAndSwap(false, true) {
		if db.closed.Load() {
			return ErrClosed
		}
		return ErrRebuildInProgress
	}
	return nil
}

func (db *DB[K, V]) endRebuildSuspend() {
	db.rebuilder.active.Store(false)
	db.rebuilder.mu.Lock()
	db.rebuilder.cond.Broadcast()
	db.rebuilder.mu.Unlock()
}

func (db *DB[K, V]) waitForInFlightOps() {
	db.rebuilder.mu.Lock()
	for db.rebuilder.inflight.Load() > 0 {
		db.rebuilder.cond.Wait()
	}
	db.rebuilder.mu.Unlock()
}

func (db *DB[K, V]) commit(tx *bbolt.Tx, op string) error {
	if hook := db.testHooks.beforeCommit; hook != nil {
		if err := hook(op); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// RebuildIndex discards and rebuilds all in-memory index data.
// It acquires an exclusive lock for its duration.
// While rebuild is active, new DB operations fail with ErrRebuildInProgress.
// While it is safe to call at any time, it might be expensive for large datasets.
func (db *DB[K, V]) RebuildIndex() error {
	if err := db.beginRebuildSuspend(); err != nil {
		return err
	}
	defer db.endRebuildSuspend()

	db.waitForInFlightOps()

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed.Load() {
		return ErrClosed
	}

	if err := db.buildIndex(nil); err != nil {
		return err
	}

	db.refreshPlannerStatsLocked()
	return nil
}

// Stats returns an aggregate diagnostic snapshot by combining results of
// IndexStats, SnapshotStats, PlannerStats, CalibrationStats and BatchStats.
//
// On large databases this can be expensive.
//
// For specific use-cases, prefer calling the corresponding component method
// directly to avoid collecting unrelated diagnostic data.
func (db *DB[K, V]) Stats() Stats[K] {
	var s Stats[K]
	s.Index = db.IndexStats()
	s.Snapshot = db.SnapshotStats()
	s.Planner = db.PlannerStats()
	s.Calibration = db.CalibrationStats()
	s.Batch = db.BatchStats()
	return s
}

// IndexStats returns current index stats.
// On large databases this can be expensive.
func (db *DB[K, V]) IndexStats() IndexStats[K] {
	if !db.beginOpWait() {
		return IndexStats[K]{}
	}
	defer db.endOp()

	snap := db.getSnapshot()

	// Preserve persistent index timings captured during build/load, while
	// recalculating shape/size diagnostics from the current snapshot.
	db.mu.RLock()
	idx := db.stats.Index
	db.mu.RUnlock()
	idx.Size = 0
	idx.FieldCount = 0
	idx.EntryCount = 0
	idx.KeyBytes = 0
	idx.BitmapCardinality = 0
	idx.ApproxStructBytes = 0
	idx.ApproxHeapBytes = 0
	idx.UniqueFieldKeys = make(map[string]uint64)
	idx.FieldSize = make(map[string]uint64)
	idx.FieldKeyBytes = make(map[string]uint64)
	idx.FieldTotalCardinality = make(map[string]uint64)

	fields := snap.fieldNameSet()
	idx.FieldCount = len(fields)

	scratch := getRoaringBuf()
	defer releaseRoaringBuf(scratch)

	for name := range fields {
		ov := newFieldOverlay(snap.fieldIndexSlice(name), snap.fieldDelta(name))
		br := ov.rangeForBounds(rangeBounds{has: true})
		if br.baseStart >= br.baseEnd && br.deltaStart >= br.deltaEnd {
			continue
		}
		var (
			unique uint64
			size   uint64
			keyLen uint64
			card   uint64
		)
		cur := ov.newCursor(br, false)
		for {
			key, baseBM, de, ok := cur.next()
			if !ok {
				break
			}
			if deltaEntryIsEmpty(de) {
				if baseBM.IsEmpty() {
					continue
				}
				unique++
				size += baseBM.SizeInBytes()
				keyLen += uint64(key.byteLen())
				curCard := baseBM.Cardinality()
				card += curCard
				idx.EntryCount++
				idx.KeyBytes += uint64(key.byteLen())
				idx.BitmapCardinality += curCard
				continue
			}
			bm, owned := composePostingOwned(baseBM, de, scratch)
			if bm == nil || bm.IsEmpty() {
				if owned && bm != nil && bm != scratch {
					releaseRoaringBuf(bm)
				}
				continue
			}
			unique++
			size += bm.GetSizeInBytes()
			keyLen += uint64(key.byteLen())
			curCard := bm.GetCardinality()
			card += curCard
			idx.EntryCount++
			idx.KeyBytes += uint64(key.byteLen())
			idx.BitmapCardinality += curCard
			if owned && bm != scratch {
				releaseRoaringBuf(bm)
			}
		}
		idx.UniqueFieldKeys[name] = unique
		idx.FieldSize[name] = size
		idx.FieldKeyBytes[name] = keyLen
		idx.FieldTotalCardinality[name] = card
		idx.Size += size
	}
	idx.ApproxStructBytes = idx.EntryCount * uint64(unsafe.Sizeof(index{}))
	idx.ApproxHeapBytes = idx.Size + idx.KeyBytes + idx.ApproxStructBytes

	universe := roaring64.NewBitmap()
	if snap.universe != nil && !snap.universe.IsEmpty() {
		universe.Or(snap.universe)
	}
	if snap.universeAdd != nil && !snap.universeAdd.IsEmpty() {
		universe.Or(snap.universeAdd)
	}
	if snap.universeRem != nil && !snap.universeRem.IsEmpty() {
		universe.AndNot(snap.universeRem)
	}
	idx.KeyCount = universe.GetCardinality()
	if idx.KeyCount > 0 {
		maxIdx := universe.Maximum()
		if db.strkey {
			if key, ok := snap.strmap.getStringNoLock(maxIdx); ok {
				idx.LastKey = *(*K)(unsafe.Pointer(&key))
			}
		} else {
			idx.LastKey = *(*K)(unsafe.Pointer(&maxIdx))
		}
	}
	return idx
}

// BatchStats returns write-combiner queue/batch/fallback diagnostics.
func (db *DB[K, V]) BatchStats() BatchStats {
	out := BatchStats{
		Enabled:             db.combiner.enabled,
		Window:              db.combiner.window,
		MaxBatch:            db.combiner.maxOps,
		MaxQueue:            db.combiner.maxQ,
		AllowCallbacks:      db.combiner.allowCallbacks,
		Submitted:           db.combiner.submitted.Load(),
		Enqueued:            db.combiner.enqueued.Load(),
		Dequeued:            db.combiner.dequeued.Load(),
		QueueHighWater:      db.combiner.queueHighWater.Load(),
		Batches:             db.combiner.batches.Load(),
		CombinedBatches:     db.combiner.combinedBatches.Load(),
		CombinedOps:         db.combiner.combinedOps.Load(),
		MaxBatchSeen:        db.combiner.maxBatchSeen.Load(),
		CallbackOps:         db.combiner.callbackOps.Load(),
		CoalescedSetDelete:  db.combiner.coalescedSetDelete.Load(),
		CoalesceWaits:       db.combiner.coalesceWaits.Load(),
		CoalesceWaitTime:    time.Duration(db.combiner.coalesceWaitNanos.Load()),
		FallbackDisabled:    db.combiner.fallbackDisabled.Load(),
		FallbackQueueFull:   db.combiner.fallbackQueueFull.Load(),
		FallbackCallbacks:   db.combiner.fallbackCallbacks.Load(),
		FallbackPatchUnique: db.combiner.fallbackPatchUnique.Load(),
		FallbackClosed:      db.combiner.fallbackClosed.Load(),
		UniqueRejected:      db.combiner.uniqueRejected.Load(),
		TxBeginErrors:       db.combiner.txBeginErrors.Load(),
		TxOpErrors:          db.combiner.txOpErrors.Load(),
		TxCommitErrors:      db.combiner.txCommitErrors.Load(),
		CallbackErrors:      db.combiner.callbackErrors.Load(),
	}

	db.combiner.mu.Lock()
	out.QueueLen = len(db.combiner.queue)
	out.QueueCap = cap(db.combiner.queue)
	out.WorkerRunning = db.combiner.running
	out.HotWindowActive = time.Now().Before(db.combiner.hotUntil)
	db.combiner.mu.Unlock()

	if out.Batches > 0 {
		out.AvgBatchSize = float64(out.Dequeued) / float64(out.Batches)
	}
	return out
}

// PlannerStats returns current planner stats.
func (db *DB[K, V]) PlannerStats() PlannerStats {
	out := PlannerStats{
		Fields: make(map[string]PlannerFieldStats),
	}
	if ps := db.planner.stats.Load(); ps != nil {
		out.Version = ps.Version
		out.GeneratedAt = ps.GeneratedAt
		out.UniverseCardinality = ps.UniverseCardinality
		out.FieldCount = len(ps.Fields)
		if out.FieldCount > 0 {
			out.Fields = make(map[string]PlannerFieldStats, out.FieldCount)
			for k, v := range ps.Fields {
				out.Fields[k] = v
			}
		}
	}
	out.TraceSampleEvery = db.planner.tracer.sampleEvery
	db.planner.analyzer.Lock()
	out.AnalyzeInterval = db.planner.analyzer.interval
	db.planner.analyzer.Unlock()
	return out
}

// CalibrationStats returns current planner calibration stats.
func (db *DB[K, V]) CalibrationStats() CalibrationStats {
	out := CalibrationStats{
		Enabled:     db.planner.calibrator.enabled,
		SampleEvery: db.planner.calibrator.sampleEvery,
		Multipliers: make(map[string]float64, int(plannerCalPlanCount)),
		Samples:     make(map[string]uint64, int(plannerCalPlanCount)),
	}
	if cs := db.planner.calibrator.state.Load(); cs != nil {
		cal := calibrationSnapshotFromState(cs)
		out.UpdatedAt = cal.UpdatedAt
		out.Multipliers = cal.Multipliers
		out.Samples = cal.Samples
		for _, n := range cal.Samples {
			out.SamplesTotal += n
		}
	}
	return out
}

// Close closes the indexed DB.
//
// On the first call, Close:
//   - Persists the current index state to the .rbi file unless index persistence is disabled.
//   - Removes the .rbo flag file used to detect unsafe shutdowns.
//   - Does not close the underlying *bbolt.DB.
//
// Subsequent calls to Close are no-op.
// After Close, all other methods return ErrClosed.
func (db *DB[K, V]) Close() error {
	if !db.closed.CompareAndSwap(false, true) {
		return nil
	}

	db.stopAnalyzeLoop()
	db.stopSnapshotCompactor()

	db.mu.Lock()
	defer db.mu.Unlock()

	unregInstance(db.bolt.Path(), string(db.bucket))

	var err error

	if !db.options.DisableIndexStore {
		err = db.storeIndex()
	}

	if e := db.persistCalibrationOnClose(); e != nil {
		if err == nil {
			err = e
		} else {
			log.Println("rbi: failed to persist planner calibration:", e)
		}
	}

	if e := os.Remove(db.opnFile); e != nil {
		if err == nil {
			err = e
		} else {
			log.Println("rbi: failed to remove flag-file:", e)
		}
	}

	return err
}

// Truncate deletes all values stored in the database. This cannot be undone.
//
// Truncate does not reclaim disk space.
func (db *DB[K, V]) Truncate() error {
	if err := db.beginOp(); err != nil {
		return err
	}
	defer db.endOp()

	// Keep writer lock order consistent with batched/single write paths:
	// open bbolt write tx first, then take db.mu. This avoids tx<->mu inversion
	// deadlocks against executeCombinedBatch (which already uses that order).
	tx, err := db.bolt.Begin(true)
	if err != nil {
		return fmt.Errorf("tx error: %w", err)
	}
	defer rollback(tx)

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed.Load() {
		return ErrClosed
	}

	if tx.Bucket(db.bucket) != nil {
		if err = tx.DeleteBucket(db.bucket); err != nil {
			return fmt.Errorf("error deleting bucket: %w", err)
		}
	}

	if _, err = tx.CreateBucketIfNotExists(db.bucket); err != nil {
		return fmt.Errorf("error creating bucket: %w", err)
	}

	txID := uint64(tx.ID())
	db.markPending(txID)
	if err = db.commit(tx, "truncate"); err != nil {
		db.clearPending(txID)
		return fmt.Errorf("commit error: %w", err)
	}

	db.strmap.truncate()
	db.index = make(map[string]*[]index)
	db.lenIndex = make(map[string]*[]index)
	db.lenZeroComplement = make(map[string]bool)

	// Keep previously published snapshots immutable for concurrent readers.
	db.universe = roaring64.NewBitmap()
	db.publishSnapshotNoLock(txID)

	return nil
}

// ReleaseRecords returns records to the record pool.
//
// Make sure that the passed records are no longer used or held.
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
// Should be used with caution.
func (db *DB[K, V]) Bolt() *bbolt.DB {
	return db.bolt
}

// BucketName returns a name of the bucket at which the data is stored.
func (db *DB[K, V]) BucketName() []byte {
	return slices.Clone(db.bucket)
}

type strMapSnapshot struct {
	Next uint64
	Keys map[string]uint64
	Strs map[uint64]string

	// DenseStrs/DenseUsed store compact base snapshots.
	// Delta layers keep sparse Strs map to avoid full-slice copies per publish.
	DenseStrs []string
	DenseUsed []bool

	base     *strMapSnapshot
	baseNext uint64
	depth    int
}

func (s *strMapSnapshot) getIdxNoLock(key string) (uint64, bool) {
	for cur := s; cur != nil; cur = cur.base {
		if cur.Keys == nil {
			continue
		}
		if v, ok := cur.Keys[key]; ok {
			return v, true
		}
	}
	return 0, false
}

func (s *strMapSnapshot) getStringNoLock(idx uint64) (string, bool) {
	for cur := s; cur != nil; cur = cur.base {
		if cur.base != nil && idx <= cur.baseNext {
			continue
		}

		if cur.DenseStrs != nil {
			if idx <= uint64(^uint(0)>>1) {
				i := int(idx)
				if i < len(cur.DenseStrs) && i < len(cur.DenseUsed) && cur.DenseUsed[i] {
					return cur.DenseStrs[i], true
				}
			}
			continue
		}

		if cur.Strs != nil {
			if v, ok := cur.Strs[idx]; ok {
				return v, true
			}
		}
	}
	return "", false
}

func (s *strMapSnapshot) mustGetStringNoLock(idx uint64) string {
	if v, ok := s.getStringNoLock(idx); ok {
		return v
	}
	panic(fmt.Errorf("no id associated with idx %v", idx))
}

type strMapper struct {
	Next uint64
	Keys map[string]uint64
	Strs []string

	strsUsed []bool

	deltaKeys map[string]uint64
	deltaStrs map[uint64]string
	snap      *strMapSnapshot
	compactAt int

	sync.Mutex
}

func newStrMapper(size uint64, compactAt int) *strMapper {
	capHint := 1
	if size > 0 && size < uint64(^uint(0)>>1) {
		capHint = int(size) + 1
	}
	sm := &strMapper{
		Keys:      make(map[string]uint64, size),
		Strs:      make([]string, 1, capHint),
		strsUsed:  make([]bool, 1, capHint),
		compactAt: compactAt,
	}
	sm.snap = new(strMapSnapshot)
	return sm
}

func (sm *strMapper) truncate() {
	sm.Lock()
	defer sm.Unlock()

	sm.Next = 0
	sm.Keys = make(map[string]uint64)
	sm.Strs = sm.Strs[:1]
	sm.Strs[0] = ""
	sm.strsUsed = sm.strsUsed[:1]
	sm.strsUsed[0] = false
	sm.deltaKeys = nil
	sm.deltaStrs = nil
	sm.snap = &strMapSnapshot{}
}

func (sm *strMapper) createIdxNoLock(s string) uint64 {
	if v, ok := sm.Keys[s]; ok {
		return v
	}
	sm.Next++
	idx := sm.Next
	sm.Keys[s] = idx
	if idx > uint64(^uint(0)>>1) {
		panic(fmt.Errorf("strmap index overflows int: %v", idx))
	}
	need := int(idx) + 1
	if need > len(sm.Strs) {
		grow := need - len(sm.Strs)
		sm.Strs = append(sm.Strs, make([]string, grow)...)
		sm.strsUsed = append(sm.strsUsed, make([]bool, grow)...)
	}
	sm.Strs[int(idx)] = s
	sm.strsUsed[int(idx)] = true
	if sm.deltaKeys == nil {
		sm.deltaKeys = make(map[string]uint64, 4)
		sm.deltaStrs = make(map[uint64]string, 4)
	}
	sm.deltaKeys[s] = idx
	sm.deltaStrs[idx] = s
	return idx
}

func (sm *strMapper) replaceAllNoLock(keys map[string]uint64, strs []string, used []bool, next uint64) {
	sm.Next = next
	sm.Keys = keys
	sm.Strs = strs
	sm.strsUsed = used
	sm.deltaKeys = nil
	sm.deltaStrs = nil
	keysCopy := maps.Clone(keys)
	sm.snap = &strMapSnapshot{
		Next:      next,
		Keys:      keysCopy,
		DenseStrs: slices.Clone(strs),
		DenseUsed: slices.Clone(used),
		baseNext:  0,
		depth:     1,
	}
}

func (sm *strMapper) snapshot() *strMapSnapshot {
	sm.Lock()
	defer sm.Unlock()
	return sm.snapshotNoLock()
}

func (sm *strMapper) snapshotNoLock() *strMapSnapshot {
	if sm.snap == nil {
		sm.snap = &strMapSnapshot{}
	}
	if len(sm.deltaKeys) == 0 {
		return sm.snap
	}
	keys := sm.deltaKeys
	strs := sm.deltaStrs
	sm.deltaKeys = nil
	sm.deltaStrs = nil
	nextDepth := 1
	if sm.snap != nil {
		nextDepth = sm.snap.depth + 1
	}
	if sm.compactAt > 0 && nextDepth >= sm.compactAt {
		keysCopy := maps.Clone(sm.Keys)
		sm.snap = &strMapSnapshot{
			Next:      sm.Next,
			Keys:      keysCopy,
			DenseStrs: slices.Clone(sm.Strs),
			DenseUsed: slices.Clone(sm.strsUsed),
			baseNext:  0,
			depth:     1,
		}
		return sm.snap
	}
	var baseNext uint64
	if sm.snap != nil {
		baseNext = sm.snap.Next
	}
	sm.snap = &strMapSnapshot{
		Next:     sm.Next,
		Keys:     keys,
		Strs:     strs,
		base:     sm.snap,
		baseNext: baseNext,
		depth:    nextDepth,
	}
	return sm.snap
}
