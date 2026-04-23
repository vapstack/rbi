package rbi

import (
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"math"
	"os"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
	"go.etcd.io/bbolt"
)

var (
	ErrNotStructType     = errors.New("value is not a struct")
	ErrClosed            = errors.New("database closed")
	ErrBroken            = errors.New("index is broken")
	ErrNoIndex           = errors.New("index is disabled (transparent mode)")
	ErrRebuildInProgress = errors.New("index rebuild in progress")
	ErrInvalidQuery      = errors.New("invalid query")
	ErrInvalidBucketName = errors.New("invalid bucket name")
	ErrUniqueViolation   = errors.New("unique constraint violation")
	ErrNoValidKeyIndex   = errors.New("no valid key for index")
	ErrNilValue          = errors.New("value is nil")

	errPersistedIndexStale   = errors.New("persisted index is stale")
	errPersistedIndexInvalid = errors.New("persisted index is invalid")
)

const (
	defaultOptionsAnalyzeInterval                  = time.Hour
	defaultOptionsCalibrationSampleEvery           = 16
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

// Options configures indexer and how it works with a bbolt database.
//
// Zero-valued option fields use defaults.
// DefaultOptions returns options with all defaults pre-filled.
type Options struct {

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
	// File name is derived from the Bolt path and validated bucket name,
	// using the same pattern as the index sidecar but with ".cal" extension.
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
	if o.CalibrationSampleEvery == 0 {
		o.CalibrationSampleEvery = defaultOptionsCalibrationSampleEvery
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

	calPath := ""
	if options.PersistCalibration {
		calPath = bolt.Path() + "." + vname + ".cal"
	}

	db = &DB[K, V]{
		bolt:   bolt,
		vtype:  vtype,
		strmap: newStrMapper(0, defaultSnapshotStrMapCompactDepth),
		bucket: []byte(vname),

		fields: make(map[string]*field),

		universe: posting.List{},

		rbiFile: bolt.Path() + "." + vname + ".rbi",

		options:     &options,
		execOptions: defaultExecOptions,
		logger:      options.Logger,
		encodeFn:    encodeFn,
		decodeFn:    decodeFn,
		snapshot: snapshot{
			bySeq:        make(map[uint64]*snapshotRef, 128),
			statsEnabled: options.EnableSnapshotStats,
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
	if err = db.initIndexedFieldAccessors(); err != nil {
		return nil, fmt.Errorf("failed to initialize field accessors: %w", err)
	}
	db.transparent = len(db.indexedFieldAccess) == 0
	if db.transparent {
		db.strmap = nil
		db.index = nil
		db.nilIndex = nil
		db.lenIndex = nil
		db.lenZeroComplement = nil
	} else {
		db.index = fieldIndexStorageSlicePool.Get()
		db.index.SetLen(len(db.indexedFieldAccess))
		db.nilIndex = fieldIndexStorageSlicePool.Get()
		db.nilIndex.SetLen(len(db.indexedFieldAccess))
		db.lenIndex = fieldIndexStorageSlicePool.Get()
		db.lenIndex.SetLen(len(db.indexedFieldAccess))
		db.lenZeroComplement = fieldIndexBoolSlicePool.Get()
		db.lenZeroComplement.SetLen(len(db.indexedFieldAccess))
	}
	db.initBatcher()

	err = bolt.Update(func(tx *bbolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists(db.bucket)
		return e
	})
	if err != nil {
		return nil, err
	}

	var (
		loadedPlannerStats *plannerStatsSnapshot
		loadedFieldCount   int
		buildMode          string
	)
	if !db.transparent {
		var (
			skipFields    map[string]struct{}
			rebuildReason string
		)

		if _, err = os.Stat(db.rbiFile); err == nil {
			if options.DisableIndexLoad {
				rebuildReason = "persisted index load disabled"
			} else {
				skipFields, loadedPlannerStats, err = db.loadIndex()
				if err != nil {
					rebuildReason = fmt.Sprintf("persisted index unavailable (%v)", err)
				}
			}
		} else if os.IsNotExist(err) {
			rebuildReason = fmt.Sprintf("persisted index missing (file=%q)", db.rbiFile)
		} else if !os.IsNotExist(err) {
			rebuildReason = fmt.Sprintf("persisted index stat failed (%v)", err)
		}

		loadedFieldCount = len(skipFields)
		totalFieldCount := len(db.fields)
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
		if err = db.buildIndex(skipFields); err != nil {
			return nil, fmt.Errorf("error building index: %w", err)
		}
		if buildMode != "" {
			db.logger.Printf("rbi: index build completed (mode=%s duration=%s)", buildMode, time.Since(buildStarted))
		}
	}

	db.patchMap = make(map[string]*field)
	if err = db.populatePatcher(vtype, nil); err != nil {
		return nil, fmt.Errorf("failed to populate patch fields: %w", err)
	}
	db.initPatchFieldAccessors()

	for name := range db.fields {
		db.fieldSlice = append(db.fieldSlice, name)
	}
	db.stats.FieldCount = len(db.fieldSlice)
	if !db.transparent {
		if err = db.publishCurrentSequenceSnapshotNoLock(); err != nil {
			return nil, fmt.Errorf("failed to publish initial snapshot: %w", err)
		}

		if loadedPlannerStats != nil && loadedFieldCount == len(db.fields) {
			db.publishLoadedPlannerStats(loadedPlannerStats)
		} else {
			if err = db.RefreshPlannerStats(); err != nil {
				return nil, fmt.Errorf("failed to build planner stats snapshot: %w", err)
			}
			if buildMode != "" {
				forceMemoryCleanup(true)
			}
		}

		db.initCalibration()
		db.startPlannerAnalyzeLoop()
	}

	return db, nil
}

func (db *DB[K, V]) initIndexedFieldAccessors() error {
	if len(db.fields) == 0 {
		db.indexedFieldAccess = nil
		db.indexedFieldByName = nil
		db.uniqueFieldAccessors = nil
		return nil
	}

	db.indexedFieldAccess = make([]indexedFieldAccessor, 0, len(db.fields))
	db.indexedFieldByName = make(map[string]indexedFieldAccessor, len(db.fields))
	db.uniqueFieldAccessors = make([]indexedFieldAccessor, 0, 4)

	names := make([]string, 0, len(db.fields))
	for name := range db.fields {
		names = append(names, name)
	}
	slices.Sort(names)

	for _, name := range names {
		f := db.fields[name]
		acc, err := db.makeIndexedFieldAccessor(f)
		if err != nil {
			return err
		}
		acc.ordinal = len(db.indexedFieldAccess)
		db.indexedFieldAccess = append(db.indexedFieldAccess, acc)
		db.indexedFieldByName[f.DBName] = acc
		if f.Unique && !f.Slice {
			db.uniqueFieldAccessors = append(db.uniqueFieldAccessors, acc)
		}
	}
	return nil
}

func (db *DB[K, V]) initPatchFieldAccessors() {
	if len(db.patchMap) == 0 {
		db.patchFieldAccess = nil
		db.patchFieldOrdinal = nil
		return
	}

	db.patchFieldAccess = make([]patchFieldAccessor, 0, len(db.patchMap))
	db.patchFieldOrdinal = make(map[string]int, len(db.patchMap))

	for patchKey, f := range db.patchMap {
		if patchKey != f.Name {
			continue
		}

		acc := patchFieldAccessor{field: f}
		fieldType, offset := resolveFieldTypeAndOffset(db.vtype, f.Index)
		acc.valueEqual = buildPatchValueEqualFn(f, fieldType, offset)
		acc.copyValue = buildPatchValueCopyFn(f, fieldType, offset)

		ordinal := len(db.patchFieldAccess)
		db.patchFieldAccess = append(db.patchFieldAccess, acc)
		db.patchFieldOrdinal[f.Name] = ordinal
	}

	for i := range db.indexedFieldAccess {
		ordinal, ok := db.patchFieldOrdinal[db.indexedFieldAccess[i].field.Name]
		if !ok {
			db.indexedFieldAccess[i].patchOrdinal = -1
			continue
		}
		db.indexedFieldAccess[i].patchOrdinal = ordinal
	}
}

func (db *DB[K, V]) initSnapshotRuntimeCaches(s *indexSnapshot) {
	if s == nil {
		return
	}
	s.numericRangeBucketCache = numericRangeBucketCachePool.Get()
	s.numericRangeBucketCache.init(len(db.indexedFieldAccess))
	s.matPredCacheMaxEntries = max(0, db.options.SnapshotMaterializedPredCacheMaxEntries)
	s.matPredCacheMaxCard = materializedPredCacheMaxCardinality(
		db.options.SnapshotMaterializedPredCacheMaxCardinality,
	)
	if s.matPredCacheMaxEntries > 0 {
		s.matPredCache = materializedPredCachePool.Get()
		s.matPredCache.refs.Store(1)
		s.matPredCache.init(s.matPredCacheMaxEntries)
	}
}

func (db *DB[K, V]) initBatcher() {
	maxOps := db.options.AutoBatchMax
	window := db.options.AutoBatchWindow
	if maxOps <= 1 || window <= 0 {
		maxOps = 1
		window = 0
	}
	maxQueue := db.options.AutoBatchMaxQueue
	if maxQueue < 0 {
		maxQueue = 0
	}
	capHint := max(64, maxOps*4)
	if maxQueue > 0 && maxQueue < capHint {
		capHint = maxQueue
	}
	db.autoBatcher = autoBatcher[K, V]{
		statsEnabled: db.options.EnableAutoBatchStats,
		window:       window,
		maxOps:       maxOps,
		maxQ:         maxQueue,
		waitNotify:   make(chan struct{}, 1),
		queue:        make([]*autoBatchJob[K, V], capHint),
		repeatIDPool: pooled.Maps[K, int]{
			NewCap: 8,
		},
		requestScratchPool: pooled.Slices[*autoBatchRequest[K, V]]{
			Clear: true,
		},
		attemptStatePool: pooled.Pointers[autoBatchAttemptState[K, V]]{
			Cleanup: func(st *autoBatchAttemptState[K, V]) {
				for _, buf := range st.ownedPayloads {
					if buf != nil {
						encodePool.Put(buf)
					}
				}
				clear(st.ownedPayloads)
				st.ownedPayloads = st.ownedPayloads[:0]

				clear(st.prepared)
				st.prepared = st.prepared[:0]

				clear(st.accepted)
				st.accepted = st.accepted[:0]

				clear(st.states)
				st.states = st.states[:0]

				st.uniqueIdxs = st.uniqueIdxs[:0]

				clear(st.uniqueOldVals)
				st.uniqueOldVals = st.uniqueOldVals[:0]

				clear(st.uniqueNewVals)
				st.uniqueNewVals = st.uniqueNewVals[:0]

				if st.stateByID != nil {
					clear(st.stateByID)
				}
				st.db = nil
				st.bucket = nil
				st.statsEnabled = false
			},
		},
		requestPool: pooled.Pointers[autoBatchRequest[K, V]]{
			Init: func(req *autoBatchRequest[K, V]) {
				if req.done == nil {
					req.done = make(chan error, 1)
				}
			},
			Cleanup: func(req *autoBatchRequest[K, V]) {
				if req.setPayload != nil {
					encodePool.Put(req.setPayload)
					req.setPayload = nil
				}
				req.setValue = nil
				req.setBaseline = nil
				clear(req.patch)
				req.patch = req.patch[:0]
				req.patchIgnoreUnknown = false
				req.beforeProcess = nil
				req.beforeStore = nil
				req.beforeCommit = nil
				req.cloneValue = nil
				req.policy = 0
				req.replacedBy = nil
				select {
				case <-req.done:
				default:
				}
				req.op = 0
				var zeroID K
				req.id = zeroID
				req.err = nil
			},
		},
		jobPool: pooled.Pointers[autoBatchJob[K, V]]{
			Init: func(job *autoBatchJob[K, V]) {
				if job.done == nil {
					job.done = make(chan error, 1)
				}
			},
			Cleanup: func(job *autoBatchJob[K, V]) {
				select {
				case <-job.done:
				default:
				}
				job.reqs = nil
				job.isolated = false
				job.enqueuedAt = 0
			},
		},
	}
	db.autoBatcher.cond = sync.NewCond(&db.autoBatcher.mu)
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
	current      atomic.Pointer[indexSnapshot]
	currentRef   atomic.Pointer[snapshotRef]
	statsEnabled bool

	bySeq map[uint64]*snapshotRef

	mu sync.RWMutex
}

type indexedFieldAccessor struct {
	ordinal      int
	patchOrdinal int
	name         string
	field        *field
	uniqueGetter uniqueScalarGetterFn
	writeBuild   buildFieldWriteAccessorFn
	writeOverlay overlayFieldWriteAccessorFn
	writeInsert  insertFieldWriteAccessorFn
	writeScratch scratchFieldWriteAccessorFn
	modified     fieldModifiedFn
}

type patchFieldAccessor struct {
	field      *field
	valueEqual patchValueEqualFn
	copyValue  patchValueCopyFn
}

type autoBatcher[K ~string | ~uint64, V any] struct {
	statsEnabled bool
	window       time.Duration
	maxOps       int
	maxQ         int

	mu                 sync.Mutex
	cond               *sync.Cond
	waitNotify         chan struct{}
	waitTimer          *time.Timer
	running            bool
	queue              []*autoBatchJob[K, V]
	queueHead          int
	queueSize          int
	batchScratch       []*autoBatchJob[K, V]
	repeatIDPool       pooled.Maps[K, int]
	requestScratchPool pooled.Slices[*autoBatchRequest[K, V]]
	attemptStatePool   pooled.Pointers[autoBatchAttemptState[K, V]]
	requestPool        pooled.Pointers[autoBatchRequest[K, V]]
	jobPool            pooled.Pointers[autoBatchJob[K, V]]
	hotUntil           time.Time
	hotBatchSize       int

	submitted          atomic.Uint64
	enqueued           atomic.Uint64
	dequeued           atomic.Uint64
	executedBatches    atomic.Uint64
	multiReqBatches    atomic.Uint64
	multiReqOps        atomic.Uint64
	batchSize1         atomic.Uint64
	batchSize2To4      atomic.Uint64
	batchSize5To8      atomic.Uint64
	batchSize9Plus     atomic.Uint64
	callbackOps        atomic.Uint64
	coalescedSetDelete atomic.Uint64
	maxBatchSeen       atomic.Uint64
	queueHighWater     atomic.Uint64
	coalesceWaits      atomic.Uint64
	coalesceWaitNanos  atomic.Uint64
	queueWaitNanos     atomic.Uint64
	executeNanos       atomic.Uint64

	fallbackClosed atomic.Uint64

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
	beforeCommit       func(op string) error
	afterCommitPublish func(op string)
	beforeStoreIndex   func() error
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

		strkey      bool
		transparent bool
		strmap      *strMapper

		universe posting.List

		fields         map[string]*field
		fieldSlice     []string
		hasUnique      bool
		lenIndexLoaded bool

		indexedFieldAccess   []indexedFieldAccessor
		indexedFieldByName   map[string]indexedFieldAccessor
		uniqueFieldAccessors []indexedFieldAccessor
		index                *pooled.SliceBuf[fieldIndexStorage]
		nilIndex             *pooled.SliceBuf[fieldIndexStorage]
		lenIndex             *pooled.SliceBuf[fieldIndexStorage]
		lenZeroComplement    *pooled.SliceBuf[bool]
		patchMap             map[string]*field
		patchFieldAccess     []patchFieldAccessor
		patchFieldOrdinal    map[string]int

		planner     planner
		snapshot    snapshot
		autoBatcher autoBatcher[K, V]
		rebuilder   rebuilder
		logger      *log.Logger

		options     *Options
		execOptions execOptions[K, V]

		rbiFile string

		encodeFn func(*V, io.Writer) error
		decodeFn func(*V, io.Reader) error

		recPool  pooled.Pointers[V]
		viewPool pooled.Pointers[queryView[K, V]]

		mu     sync.RWMutex
		closed atomic.Bool
		broken atomic.Bool

		stats Stats[K]

		traceRoot *DB[K, V]
		testHooks testHooks
	}

	index struct {
		Key indexKey
		IDs posting.List
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

	// Stats is a lightweight database status snapshot.
	Stats[K ~uint64 | ~string] struct {
		// BuildTime is the duration of the last index rebuild.
		BuildTime time.Duration
		// BuildRPS is an approximate throughput (records per second) of the last index rebuild.
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

// DisableSync disables fsync for bolt writes. Can help with batch inserts.
// It should not be used during normal operation.
func (db *DB[K, V]) DisableSync() { db.bolt.NoSync = true }

// EnableSync enables fsync for bolt writes. See DisableSync.
// By default, fsync is enabled.
func (db *DB[K, V]) EnableSync() {
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

func (db *DB[K, V]) beginOp() error {
	if err := db.unavailableErr(); err != nil {
		return err
	}
	if db.rebuilder.active.Load() {
		return ErrRebuildInProgress
	}

	db.rebuilder.inflight.Add(1)
	if err := db.unavailableErr(); err != nil {
		db.endOp()
		return err
	}
	if db.rebuilder.active.Load() {
		db.endOp()
		return ErrRebuildInProgress
	}
	return nil
}

func (db *DB[K, V]) beginOpWait() bool {
	for {
		if db.unavailableErr() != nil {
			return false
		}
		if !db.rebuilder.active.Load() {
			db.rebuilder.inflight.Add(1)
			if db.unavailableErr() != nil {
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
	if err := db.unavailableErr(); err != nil {
		return err
	}
	if !db.rebuilder.active.CompareAndSwap(false, true) {
		if err := db.unavailableErr(); err != nil {
			return err
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

func (db *DB[K, V]) currentBucketSequence() (uint64, error) {
	var seq uint64
	if err := db.bolt.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(db.bucket)
		if bucket == nil {
			return fmt.Errorf("bucket does not exist")
		}
		seq = bucket.Sequence()
		return nil
	}); err != nil {
		return 0, err
	}
	return seq, nil
}

func (db *DB[K, V]) publishCurrentSequenceSnapshotNoLock() error {
	seq, err := db.currentBucketSequence()
	if err != nil {
		return err
	}
	db.publishSnapshotNoLock(seq)
	return nil
}

func (db *DB[K, V]) tripBrokenLocked(op string, cause any) error {
	if db.broken.CompareAndSwap(false, true) {
		db.logger.Printf("rbi: index entered broken state: post-commit snapshot publish failed (%v): %v", op, cause)
		if stop := db.planner.analyzer.stop; stop != nil {
			select {
			case <-stop:
			default:
				close(stop)
			}
		}
	}
	return ErrBroken
}

func (db *DB[K, V]) publishAfterCommitLocked(seq uint64, op string, publish func()) (err error) {
	defer recoverPublishAfterCommit(db, seq, op, &err)
	if hook := db.testHooks.afterCommitPublish; hook != nil {
		hook(op)
	}
	publish()
	return nil
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

	if err := db.unavailableErr(); err != nil {
		return err
	}
	if db.transparent {
		return ErrNoIndex
	}

	if err := db.buildIndex(nil); err != nil {
		return fmt.Errorf("error building index: %w", err)
	}

	db.refreshPlannerStatsLocked()
	forceMemoryCleanup(true)
	return nil
}

// Stats returns a lightweight database status snapshot.
//
// In indexed mode it reports the last rebuild/load timings together with the
// current published snapshot cardinality and last key.
//
// In transparent mode no runtime index snapshot is maintained, so fields that
// depend on indexed state remain zero-valued: BuildTime, BuildRPS, LoadTime,
// KeyCount, LastKey, and SnapshotSequence. FieldCount also stays zero because
// no indexed fields exist in that mode. Auto-batcher fields remain meaningful
// in both modes.
//
// If the DB is unavailable or already closed and the method cannot enter an
// operation window, it returns a zero value.
func (db *DB[K, V]) Stats() Stats[K] {
	if !db.beginOpWait() {
		return Stats[K]{}
	}
	defer db.endOp()

	snap, seq, ref, pinned := db.pinCurrentSnapshot()
	defer db.unpinCurrentSnapshot(seq, ref, pinned)

	db.mu.RLock()
	out := db.stats
	db.mu.RUnlock()

	out.KeyCount = snap.universeCardinality()
	out.LastKey = db.statsLastKeyFromSnapshot(snap, out.KeyCount)

	db.autoBatcher.mu.Lock()
	out.AutoBatchQueueLen = db.autoBatcher.queueSize
	db.autoBatcher.mu.Unlock()
	out.AutoBatchQueueMax = db.autoBatcher.queueHighWater.Load()

	out.AutoBatchCount = db.autoBatcher.executedBatches.Load()
	if out.AutoBatchCount > 0 {
		out.AutoBatchAvgSize = float64(db.autoBatcher.dequeued.Load()) / float64(out.AutoBatchCount)
	}

	out.SnapshotSequence = snap.seq

	return out
}

func (db *DB[K, V]) statsLastKeyFromSnapshot(snap *indexSnapshot, keyCount uint64) K {
	var zero K
	if snap == nil || keyCount == 0 {
		return zero
	}

	var maxIdx uint64
	if snap.universe.IsEmpty() {
		return zero
	}
	maxIdx, _ = snap.universe.Maximum()

	return db.statsKeyFromIdx(snap, maxIdx)
}

func (db *DB[K, V]) statsKeyFromIdx(snap *indexSnapshot, idx uint64) K {
	var zero K
	if db.strkey {
		if snap == nil || snap.strmap == nil {
			return zero
		}
		if key, ok := snap.strmap.getStringNoLock(idx); ok {
			return *(*K)(unsafe.Pointer(&key))
		}
		return zero
	}
	return *(*K)(unsafe.Pointer(&idx))
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
// If the DB is unavailable or already closed and the method cannot enter an
// operation window, it returns a zero value.
func (db *DB[K, V]) IndexStats() IndexStats {
	if !db.beginOpWait() {
		return IndexStats{}
	}
	defer db.endOp()

	snap, seq, ref, pinned := db.pinCurrentSnapshot()
	defer db.unpinCurrentSnapshot(seq, ref, pinned)

	idx := IndexStats{
		UniqueFieldKeys:        make(map[string]uint64),
		FieldSize:              make(map[string]uint64),
		FieldKeyBytes:          make(map[string]uint64),
		FieldTotalCardinality:  make(map[string]uint64),
		FieldApproxStructBytes: make(map[string]uint64),
		FieldApproxHeapBytes:   make(map[string]uint64),
	}
	sharedStructBytes := approxSliceBufBytes(snap.index) + approxSliceBufBytes(snap.nilIndex)
	fieldCount := len(db.indexedFieldAccess)
	sharedStructPerField := uint64(0)
	sharedStructRemainder := uint64(0)
	if fieldCount > 0 {
		sharedStructPerField = sharedStructBytes / uint64(fieldCount)
		sharedStructRemainder = sharedStructBytes % uint64(fieldCount)
	}

	for i, acc := range db.indexedFieldAccess {
		name := acc.name
		fieldStats := collectFieldIndexStats(snap.index.Get(acc.ordinal), true)
		nilStats := collectFieldIndexStats(snap.nilIndex.Get(acc.ordinal), false)

		unique := fieldStats.unique + nilStats.unique
		size := fieldStats.postingBytes + nilStats.postingBytes
		keyLen := fieldStats.keyBytes + nilStats.keyBytes
		card := fieldStats.postingCardinality + nilStats.postingCardinality

		idx.UniqueFieldKeys[name] = unique
		idx.FieldSize[name] = size
		idx.FieldKeyBytes[name] = keyLen
		idx.FieldTotalCardinality[name] = card
		fieldStructBytes := fieldStats.approxStructBytes + nilStats.approxStructBytes + sharedStructPerField
		if uint64(i) < sharedStructRemainder {
			fieldStructBytes++
		}
		idx.FieldApproxStructBytes[name] = fieldStructBytes
		idx.FieldApproxHeapBytes[name] = size + keyLen + fieldStructBytes

		idx.Size += size
		idx.EntryCount += fieldStats.entryCount + nilStats.entryCount
		idx.KeyBytes += keyLen
		idx.PostingCardinality += card
		idx.ApproxStructBytes += fieldStructBytes
	}
	idx.ApproxHeapBytes = idx.Size + idx.KeyBytes + idx.ApproxStructBytes
	return idx
}

func unregInstanceOnError(err *error, boltPath string, vname string) {
	if *err != nil {
		unregInstance(boltPath, vname)
	}
}

func recoverPublishAfterCommit[K ~string | ~uint64, V any](db *DB[K, V], seq uint64, op string, err *error) {
	if r := recover(); r != nil {
		*err = db.tripBrokenLocked(op, r)
		db.dropStagedSnapshot(seq)
	}
}

type fieldIndexStats struct {
	unique             uint64
	postingBytes       uint64
	keyBytes           uint64
	postingCardinality uint64
	entryCount         uint64
	approxStructBytes  uint64
}

func collectFieldIndexStats(storage fieldIndexStorage, countDistinct bool) fieldIndexStats {
	if storage.flat != nil {
		return collectFlatFieldIndexStats(storage.flat, countDistinct)
	}
	if storage.chunked != nil {
		return collectChunkedFieldIndexStats(storage.chunked, countDistinct)
	}
	return fieldIndexStats{}
}

func collectFlatFieldIndexStats(root *fieldIndexFlatRoot, countDistinct bool) fieldIndexStats {
	if root == nil {
		return fieldIndexStats{}
	}
	stats := fieldIndexStats{
		approxStructBytes: uint64(unsafe.Sizeof(fieldIndexFlatRoot{})) +
			uint64(cap(root.entries))*uint64(unsafe.Sizeof(index{})),
	}
	for i := range root.entries {
		entry := root.entries[i]
		if entry.Key.isNumeric() {
			stats.approxStructBytes -= uint64(unsafe.Sizeof(uint64(0)))
		}
		accumulateIndexEntryStats(entry.Key, entry.IDs, countDistinct, &stats)
	}
	return stats
}

func collectChunkedFieldIndexStats(root *fieldIndexChunkedRoot, countDistinct bool) fieldIndexStats {
	if root == nil {
		return fieldIndexStats{}
	}
	stats := fieldIndexStats{
		approxStructBytes: uint64(unsafe.Sizeof(fieldIndexChunkedRoot{})) +
			approxSliceBufBytes(root.pages) +
			approxSliceBufBytes(root.chunkPrefix) +
			approxSliceBufBytes(root.prefix) +
			approxSliceBufBytes(root.rowPrefix),
	}
	for i := 0; i < root.pages.Len(); i++ {
		page := root.pages.Get(i)
		if page == nil {
			continue
		}
		stats.approxStructBytes += uint64(unsafe.Sizeof(fieldIndexChunkDirPage{})) +
			approxSliceBufBytes(page.refs) +
			approxSliceBufBytes(page.prefix) +
			approxSliceBufBytes(page.rowPrefix)

		for j := 0; j < page.refsLen(); j++ {
			chunk := page.refAt(j).chunk
			if chunk == nil {
				continue
			}
			stats.approxStructBytes += uint64(unsafe.Sizeof(fieldIndexChunk{})) +
				uint64(cap(chunk.posts))*uint64(unsafe.Sizeof(posting.List{}))
			if chunk.numeric == nil {
				stats.approxStructBytes += uint64(cap(chunk.stringRefs)) * uint64(unsafe.Sizeof(fieldIndexStringRef{}))
			}
			for k := 0; k < chunk.keyCount(); k++ {
				accumulateIndexEntryStats(chunk.keyAt(k), chunk.posts[k], countDistinct, &stats)
			}
		}
	}
	return stats
}

func accumulateIndexEntryStats(key indexKey, ids posting.List, countDistinct bool, stats *fieldIndexStats) {
	if ids.IsEmpty() {
		return
	}
	if countDistinct {
		stats.unique++
		stats.keyBytes += uint64(key.byteLen())
	}
	stats.postingBytes += ids.SizeInBytes()
	card := ids.Cardinality()
	stats.postingCardinality += card
	stats.entryCount++
}

func approxSliceBufBytes[T any](buf *pooled.SliceBuf[T]) uint64 {
	if buf == nil {
		return 0
	}
	var zero T
	return uint64(unsafe.Sizeof(pooled.SliceBuf[T]{})) +
		uint64(buf.Cap())*uint64(unsafe.Sizeof(zero))
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
	if !db.autoBatcher.statsEnabled {
		return AutoBatchStats{}
	}
	out := AutoBatchStats{
		Window:              db.autoBatcher.window,
		MaxBatch:            db.autoBatcher.maxOps,
		MaxQueue:            db.autoBatcher.maxQ,
		Submitted:           db.autoBatcher.submitted.Load(),
		Enqueued:            db.autoBatcher.enqueued.Load(),
		Dequeued:            db.autoBatcher.dequeued.Load(),
		QueueHighWater:      db.autoBatcher.queueHighWater.Load(),
		ExecutedBatches:     db.autoBatcher.executedBatches.Load(),
		MultiRequestBatches: db.autoBatcher.multiReqBatches.Load(),
		MultiRequestOps:     db.autoBatcher.multiReqOps.Load(),
		BatchSize1:          db.autoBatcher.batchSize1.Load(),
		BatchSize2To4:       db.autoBatcher.batchSize2To4.Load(),
		BatchSize5To8:       db.autoBatcher.batchSize5To8.Load(),
		BatchSize9Plus:      db.autoBatcher.batchSize9Plus.Load(),
		MaxBatchSeen:        db.autoBatcher.maxBatchSeen.Load(),
		CallbackOps:         db.autoBatcher.callbackOps.Load(),
		CoalescedSetDelete:  db.autoBatcher.coalescedSetDelete.Load(),
		CoalesceWaits:       db.autoBatcher.coalesceWaits.Load(),
		CoalesceWaitTime:    time.Duration(db.autoBatcher.coalesceWaitNanos.Load()),
		QueueWaitTime:       time.Duration(db.autoBatcher.queueWaitNanos.Load()),
		ExecuteTime:         time.Duration(db.autoBatcher.executeNanos.Load()),
		FallbackClosed:      db.autoBatcher.fallbackClosed.Load(),
		UniqueRejected:      db.autoBatcher.uniqueRejected.Load(),
		TxBeginErrors:       db.autoBatcher.txBeginErrors.Load(),
		TxOpErrors:          db.autoBatcher.txOpErrors.Load(),
		TxCommitErrors:      db.autoBatcher.txCommitErrors.Load(),
		CallbackErrors:      db.autoBatcher.callbackErrors.Load(),
	}

	db.autoBatcher.mu.Lock()
	out.QueueLen = db.autoBatcher.queueSize
	out.QueueCap = cap(db.autoBatcher.queue)
	out.WorkerRunning = db.autoBatcher.running
	out.HotWindowActive = db.autoBatcher.window > 0 &&
		time.Now().Before(db.autoBatcher.hotUntil) &&
		(db.autoBatcher.hotBatchSize == 0 || db.autoBatcher.hotBatchSize >= 3)
	db.autoBatcher.mu.Unlock()

	if out.ExecutedBatches > 0 {
		out.AvgBatchSize = float64(out.Dequeued) / float64(out.ExecutedBatches)
	}
	return out
}

// PlannerStats returns the last published planner statistics snapshot together
// with current planner analyze/trace settings.
//
// In indexed mode the returned payload reflects the most recently published
// planner stats snapshot, whether it was loaded from a persisted sidecar or
// rebuilt in memory.
//
// In transparent mode planner stats are not built or refreshed automatically,
// so the returned planner payload stays zero-valued: Version,
// UniverseCardinality, FieldCount, GeneratedAt, and per-field stats are all
// unset. AnalyzeInterval and TraceSampleEvery still reflect the configured
// options.
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

// CalibrationStats returns current planner calibration settings and state.
//
// When planner calibration is enabled, it returns the current calibration
// multipliers and sample counters.
//
// When calibration is disabled, or no calibration state has been initialized yet,
// it returns the current settings with zeroed calibration data.
//
// In indexed mode calibration state is initialized on startup when
// calibration is enabled. In transparent mode startup skips calibration
// initialization, so this method usually returns configured settings with zero
// calibration payload unless state was explicitly injected through
// SetCalibrationSnapshot.
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

	db.autoBatcher.mu.Lock()
	if db.autoBatcher.cond != nil {
		db.autoBatcher.cond.Broadcast()
	}
	db.autoBatcher.mu.Unlock()

	db.mu.Lock()
	defer db.mu.Unlock()

	var err error

	if !db.transparent && !db.options.DisableIndexStore && !db.broken.Load() {
		err = db.storeIndex()
	}

	if e := db.persistCalibrationOnClose(); e != nil {
		if err == nil {
			err = e
		} else {
			db.logger.Println("rbi: failed to persist planner calibration:", e)
		}
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
	if err := db.beginOp(); err != nil {
		return err
	}
	defer db.endOp()

	// Keep writer lock order consistent with batched/single write paths:
	// open bbolt write tx first, then take db.mu. This avoids tx<->mu inversion
	// deadlocks against executeAutoBatch (which already uses that order).
	tx, err := db.bolt.Begin(true)
	if err != nil {
		return fmt.Errorf("tx error: %w", err)
	}
	defer rollback(tx)

	db.mu.Lock()
	defer db.mu.Unlock()

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
	if db.transparent {
		if _, err = bucket.NextSequence(); err != nil {
			return fmt.Errorf("advance bucket sequence: %w", err)
		}
		if err = db.commit(tx, "truncate"); err != nil {
			return fmt.Errorf("commit error: %w", err)
		}
		return nil
	}
	seq, err := bucket.NextSequence()
	if err != nil {
		return fmt.Errorf("advance bucket sequence: %w", err)
	}

	nextIndex := fieldIndexStorageSlicePool.Get()
	nextIndex.SetLen(len(db.indexedFieldAccess))
	nextNilIndex := fieldIndexStorageSlicePool.Get()
	nextNilIndex.SetLen(len(db.indexedFieldAccess))
	nextLenIndex := fieldIndexStorageSlicePool.Get()
	nextLenIndex.SetLen(len(db.indexedFieldAccess))
	nextLenZeroComplement := fieldIndexBoolSlicePool.Get()
	nextLenZeroComplement.SetLen(len(db.indexedFieldAccess))
	nextUniverse := posting.List{}
	var nextStrMap *strMapSnapshot
	if db.strkey {
		nextStrMap = &strMapSnapshot{}
	}
	snap := &indexSnapshot{
		seq:                seq,
		index:              nextIndex,
		nilIndex:           nextNilIndex,
		lenIndex:           nextLenIndex,
		lenZeroComplement:  nextLenZeroComplement,
		indexedFieldByName: db.indexedFieldByName,
		universe:           nextUniverse,
		strmap:             nextStrMap,
	}
	db.initSnapshotRuntimeCaches(snap)
	db.stageSnapshot(snap)
	if err = db.commit(tx, "truncate"); err != nil {
		db.dropStagedSnapshot(seq)
		return fmt.Errorf("commit error: %w", err)
	}

	db.strmap.truncate()
	db.index = nextIndex
	db.nilIndex = nextNilIndex
	db.lenIndex = nextLenIndex
	db.lenZeroComplement = nextLenZeroComplement

	// Keep previously published snapshots immutable for concurrent readers.
	db.universe = nextUniverse
	if err = db.publishAfterCommitLocked(seq, "truncate", func() {
		db.finishSnapshotPublishNoLock(snap)
	}); err != nil {
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

type strMapSnapshot struct {
	Next      uint64
	Keys      map[string]uint64
	Strs      map[uint64]string
	DenseStrs []string
	DenseUsed []bool
	base      *strMapSnapshot
	anchor    *strMapSnapshot
	depth     int
	readDirs  []*strMapReadDir
	keysOnce  sync.Once
}

const (
	strMapReadPageShift = 8
	strMapReadPageSize  = 1 << strMapReadPageShift
	strMapReadDirShift  = 8
	strMapReadDirSize   = 1 << strMapReadDirShift
	strMapReadDirMask   = strMapReadDirSize - 1
)

type strMapReadDir struct {
	Pages [strMapReadDirSize]*strMapReadPage
}

type strMapReadPage struct {
	Start     uint64
	Next      uint64
	Strs      map[uint64]string
	DenseStrs []string
	DenseUsed []bool
}

func (s *strMapSnapshot) baseNextNoLock() uint64 {
	if s == nil || s.base == nil {
		return 0
	}
	return s.base.Next
}

func (s *strMapSnapshot) getOwnStringNoLock(idx uint64) (string, bool) {
	if s == nil || idx > s.Next {
		return "", false
	}
	if len(s.DenseStrs) > 0 || len(s.DenseUsed) > 0 {
		if idx > uint64(^uint(0)>>1) {
			return "", false
		}
		i := int(idx)
		if i < len(s.DenseStrs) && i < len(s.DenseUsed) && s.DenseUsed[i] {
			return s.DenseStrs[i], true
		}
		return "", false
	}
	if s.Strs == nil {
		return "", false
	}
	v, ok := s.Strs[idx]
	return v, ok
}

func strMapDenseWindowNoLock(strs []string, used []bool, start, next uint64) ([]string, []bool) {
	if start > next || start > uint64(^uint(0)>>1) {
		return nil, nil
	}
	limit := min(len(strs), len(used))
	if limit == 0 {
		return nil, nil
	}
	pos := int(start)
	if pos < 0 || pos >= limit {
		return nil, nil
	}
	end := limit
	if next < uint64(^uint(0)>>1) {
		maxPos := int(next) + 1
		if maxPos < end {
			end = maxPos
		}
	}
	if pos >= end {
		return nil, nil
	}
	return strs[pos:end], used[pos:end]
}

func newStrMapReadPageNoLock(node *strMapSnapshot, start, next uint64) *strMapReadPage {
	if node == nil || start > next {
		return nil
	}
	if len(node.DenseStrs) > 0 || len(node.DenseUsed) > 0 {
		denseStrs, denseUsed := strMapDenseWindowNoLock(node.DenseStrs, node.DenseUsed, start, next)
		if len(denseStrs) == 0 && len(denseUsed) == 0 {
			return nil
		}
		return &strMapReadPage{
			Start:     start,
			Next:      next,
			DenseStrs: denseStrs,
			DenseUsed: denseUsed,
		}
	}
	if node.Strs == nil {
		return nil
	}
	return &strMapReadPage{
		Start: start,
		Next:  next,
		Strs:  node.Strs,
	}
}

func materializeStrMapReadPageNoLock(prefix *strMapReadPage, delta *strMapSnapshot, deltaStart, start, next uint64) *strMapReadPage {
	if start > next || start > uint64(^uint(0)>>1) {
		return nil
	}
	size := int(next-start) + 1
	denseStrs := make([]string, size)
	denseUsed := make([]bool, size)
	if prefix != nil {
		prefixNext := min(next, deltaStart-1)
		for idx := start; idx <= prefixNext; idx++ {
			value, ok := prefix.getStringNoLock(idx)
			if !ok {
				continue
			}
			pos := int(idx - start)
			denseStrs[pos] = value
			denseUsed[pos] = true
		}
	}
	if delta != nil {
		deltaPos := max(start, deltaStart)
		for idx := deltaPos; idx <= next; idx++ {
			value, ok := delta.getOwnStringNoLock(idx)
			if !ok {
				continue
			}
			pos := int(idx - start)
			denseStrs[pos] = value
			denseUsed[pos] = true
		}
	}
	return &strMapReadPage{
		Start:     start,
		Next:      next,
		DenseStrs: denseStrs,
		DenseUsed: denseUsed,
	}
}

func buildStrMapSparsePageMapsNoLock(strs map[uint64]string, start, next uint64) map[int]map[uint64]string {
	if len(strs) == 0 || start > next {
		return nil
	}
	pageMaps := make(map[int]map[uint64]string)
	for idx, value := range strs {
		if idx < start || idx > next {
			continue
		}
		page := strMapReadPageIndex(idx)
		pageStrs := pageMaps[page]
		if pageStrs == nil {
			pageStrs = make(map[uint64]string)
			pageMaps[page] = pageStrs
		}
		pageStrs[idx] = value
	}
	return pageMaps
}

func (page *strMapReadPage) getStringNoLock(idx uint64) (string, bool) {
	if page == nil || idx < page.Start || idx > page.Next {
		return "", false
	}
	if len(page.DenseStrs) > 0 || len(page.DenseUsed) > 0 {
		if idx-page.Start > uint64(^uint(0)>>1) {
			return "", false
		}
		i := int(idx - page.Start)
		if i < len(page.DenseStrs) && i < len(page.DenseUsed) && page.DenseUsed[i] {
			return page.DenseStrs[i], true
		}
		return "", false
	}
	if page.Strs == nil {
		return "", false
	}
	v, ok := page.Strs[idx]
	return v, ok
}

func (page *strMapReadPage) appendKeysNoLock(dst map[string]uint64) {
	if page == nil {
		return
	}
	if page.Strs != nil {
		for idx, value := range page.Strs {
			if idx >= page.Start && idx <= page.Next {
				dst[value] = idx
			}
		}
		return
	}
	limit := min(len(page.DenseStrs), len(page.DenseUsed))
	for i := 0; i < limit; i++ {
		if !page.DenseUsed[i] {
			continue
		}
		dst[page.DenseStrs[i]] = page.Start + uint64(i)
	}
}

func (page *strMapReadPage) usedCountNoLock() int {
	if page == nil {
		return 0
	}
	if page.Strs != nil {
		count := 0
		for idx := range page.Strs {
			if idx >= page.Start && idx <= page.Next {
				count++
			}
		}
		return count
	}
	limit := min(len(page.DenseStrs), len(page.DenseUsed))
	count := 0
	for i := 0; i < limit; i++ {
		if !page.DenseUsed[i] {
			continue
		}
		count++
	}
	return count
}

func strMapReadPageCount(next uint64) int {
	if next == 0 {
		return 0
	}
	return int(((next - 1) >> strMapReadPageShift) + 1)
}

func strMapReadPageIndex(idx uint64) int {
	return int((idx - 1) >> strMapReadPageShift)
}

func strMapReadPageBounds(page int, next uint64) (uint64, uint64) {
	start := uint64(page)<<strMapReadPageShift + 1
	end := start + strMapReadPageSize - 1
	if end > next {
		end = next
	}
	return start, end
}

func (s *strMapSnapshot) readPageAtNoLock(page int) *strMapReadPage {
	if s == nil || page < 0 || len(s.readDirs) == 0 {
		return nil
	}
	dirIdx := page >> strMapReadDirShift
	if dirIdx >= len(s.readDirs) {
		return nil
	}
	dir := s.readDirs[dirIdx]
	if dir == nil {
		return nil
	}
	return dir.Pages[page&strMapReadDirMask]
}

func (s *strMapSnapshot) readPageNoLock(idx uint64) *strMapReadPage {
	if s == nil || idx == 0 || idx > s.Next {
		return nil
	}
	return s.readPageAtNoLock(strMapReadPageIndex(idx))
}

func (s *strMapSnapshot) buildKeysNoLock() map[string]uint64 {
	if s == nil || s.Next == 0 {
		return nil
	}
	usedCount := strMapSnapshotUsedCountNoLock(s)
	if usedCount == 0 {
		return nil
	}
	keys := make(map[string]uint64, usedCount)
	if len(s.readDirs) > 0 {
		for _, dir := range s.readDirs {
			if dir == nil {
				continue
			}
			for i := range dir.Pages {
				dir.Pages[i].appendKeysNoLock(keys)
			}
		}
		return keys
	}
	if s.base == nil {
		newStrMapReadPageNoLock(s, 1, s.Next).appendKeysNoLock(keys)
		return keys
	}
	for cur := s; cur != nil; cur = cur.base {
		start := cur.baseNextNoLock() + 1
		if cur.base == nil {
			start = 1
		}
		newStrMapReadPageNoLock(cur, start, cur.Next).appendKeysNoLock(keys)
	}
	return keys
}

func (s *strMapSnapshot) ensureKeysNoLock() map[string]uint64 {
	if s == nil {
		return nil
	}
	s.keysOnce.Do(func() {
		if s.Keys == nil {
			s.Keys = s.buildKeysNoLock()
		}
	})
	return s.Keys
}

func (s *strMapSnapshot) getIdxNoLock(key string) (uint64, bool) {
	if s == nil {
		return 0, false
	}
	if len(s.readDirs) > 0 {
		keys := s.ensureKeysNoLock()
		if keys == nil {
			return 0, false
		}
		v, ok := keys[key]
		return v, ok
	}
	if s.base == nil {
		keys := s.ensureKeysNoLock()
		if keys == nil {
			return 0, false
		}
		v, ok := keys[key]
		return v, ok
	}
	for cur := s; cur != nil; cur = cur.base {
		if cur.base == nil {
			keys := cur.ensureKeysNoLock()
			if keys == nil {
				return 0, false
			}
			v, ok := keys[key]
			return v, ok
		}
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
	if s == nil || idx == 0 || idx > s.Next {
		return "", false
	}
	if len(s.readDirs) > 0 {
		page := s.readPageNoLock(idx)
		if page == nil {
			return "", false
		}
		return page.getStringNoLock(idx)
	}
	if s.base == nil {
		return s.getOwnStringNoLock(idx)
	}
	for cur := s; cur != nil; {
		if len(cur.DenseStrs) > 0 || len(cur.DenseUsed) > 0 {
			if value, ok := cur.getOwnStringNoLock(idx); ok {
				return value, true
			}
			if cur.base == nil {
				return "", false
			}
			if idx <= cur.base.Next {
				if cur.anchor != nil && cur.anchor != cur.base && idx <= cur.anchor.Next {
					cur = cur.anchor
				} else {
					cur = cur.base
				}
				continue
			}
			return "", false
		}
		if cur.Strs != nil {
			v, ok := cur.Strs[idx]
			if ok {
				return v, true
			}
		}
		if cur.base == nil {
			return "", false
		}
		if idx <= cur.base.Next {
			if cur.anchor != nil && cur.anchor != cur.base && idx <= cur.anchor.Next {
				cur = cur.anchor
			} else {
				cur = cur.base
			}
			continue
		}
		return "", false
	}
	return "", false
}

type strMapper struct {
	Next uint64
	Keys map[string]uint64
	Strs []string

	sparseStrs   map[uint64]string
	strsUsed     []bool
	snap         *strMapSnapshot
	published    *strMapSnapshot
	pubSource    *strMapSnapshot
	committed    *strMapSnapshot
	committedPub *strMapSnapshot
	dirty        bool
	compactAt    int

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
	empty := new(strMapSnapshot)
	sm.snap = empty
	sm.published = empty
	sm.pubSource = empty
	sm.committed = empty
	sm.committedPub = empty
	return sm
}

func (sm *strMapper) truncate() {
	sm.Lock()
	defer sm.Unlock()

	sm.Next = 0
	sm.Keys = make(map[string]uint64)
	sm.Strs = sm.Strs[:1]
	sm.Strs[0] = ""
	sm.sparseStrs = nil
	sm.strsUsed = sm.strsUsed[:1]
	sm.strsUsed[0] = false
	empty := &strMapSnapshot{}
	sm.snap = empty
	sm.published = empty
	sm.pubSource = empty
	sm.committed = empty
	sm.committedPub = empty
	sm.dirty = false
}

func (sm *strMapper) createIdxNoLock(s string) uint64 {
	if v, ok := sm.Keys[s]; ok {
		return v
	}
	sm.Next++
	idx := sm.Next
	sm.Keys[s] = idx
	if sm.sparseStrs != nil {
		sm.sparseStrs[idx] = s
		sm.dirty = true
		return idx
	}
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
	sm.dirty = true
	return idx
}

func (sm *strMapper) replaceAllDenseNoLock(keys map[string]uint64, strs []string, used []bool, next uint64) {
	sm.Next = next
	sm.Keys = keys
	sm.Strs = strs
	sm.sparseStrs = nil
	sm.strsUsed = used
	sm.snap = &strMapSnapshot{
		Next:      next,
		Strs:      nil,
		DenseStrs: slices.Clone(strs),
		DenseUsed: slices.Clone(used),
		depth:     1,
	}
	sm.published = &strMapSnapshot{
		Next:      next,
		DenseStrs: sm.snap.DenseStrs,
		DenseUsed: sm.snap.DenseUsed,
	}
	sm.pubSource = sm.snap
	sm.committed = sm.snap
	sm.committedPub = sm.published
	sm.dirty = false
}

func (sm *strMapper) replaceAllSparseNoLock(keys map[string]uint64, strs map[uint64]string, next uint64) {
	sm.Next = next
	sm.Keys = keys
	sm.Strs = nil
	sm.sparseStrs = strs
	sm.strsUsed = nil
	sm.snap = &strMapSnapshot{
		Next:      next,
		Strs:      maps.Clone(strs),
		DenseStrs: nil,
		DenseUsed: nil,
		depth:     1,
	}
	sm.published = &strMapSnapshot{
		Next: next,
		Strs: sm.snap.Strs,
	}
	sm.pubSource = sm.snap
	sm.committed = sm.snap
	sm.committedPub = sm.published
	sm.dirty = false
}

func (sm *strMapper) snapshot() *strMapSnapshot {
	sm.Lock()
	defer sm.Unlock()
	return sm.snapshotNoLock()
}

func (sm *strMapper) snapshotNoLock() *strMapSnapshot {
	base := sm.stateSnapshotNoLock()
	if sm.published != nil && sm.pubSource == base {
		return sm.published
	}
	sm.published = buildPublishedStrMapSnapshot(base, sm.pubSource, sm.published)
	sm.pubSource = base
	return sm.published
}

func (sm *strMapper) stateSnapshotNoLock() *strMapSnapshot {
	if sm.snap == nil {
		sm.snap = &strMapSnapshot{}
	}
	if !sm.dirty {
		return sm.snap
	}
	if next, ok := sm.deltaSnapshotNoLock(sm.snap); ok {
		sm.snap = next
		sm.dirty = false
		return next
	}
	sm.snap = sm.fullSnapshotNoLock()
	sm.dirty = false
	return sm.snap
}

func (sm *strMapper) fullSnapshotNoLock() *strMapSnapshot {
	snap := &strMapSnapshot{
		Next:  sm.Next,
		depth: 1,
	}
	if sm.sparseStrs != nil {
		snap.Strs = maps.Clone(sm.sparseStrs)
		return snap
	}
	snap.DenseStrs = slices.Clone(sm.Strs)
	snap.DenseUsed = slices.Clone(sm.strsUsed)
	return snap
}

func (sm *strMapper) deltaSnapshotNoLock(base *strMapSnapshot) (*strMapSnapshot, bool) {
	if base == nil {
		return nil, false
	}
	if sm.compactAt > 0 && base.depth >= sm.compactAt {
		return nil, false
	}
	if sm.Next < base.Next {
		return nil, false
	}
	if sm.Next == base.Next {
		return base, true
	}

	start := base.Next + 1
	count := 0
	if sm.sparseStrs != nil {
		for idx := start; idx <= sm.Next; idx++ {
			if _, ok := sm.sparseStrs[idx]; ok {
				count++
			}
		}
	} else {
		if sm.Next > uint64(^uint(0)>>1) {
			return nil, false
		}
		end := int(sm.Next)
		for i := int(start); i <= end; i++ {
			if i < len(sm.strsUsed) && sm.strsUsed[i] {
				count++
			}
		}
	}
	if count == 0 {
		return base, true
	}

	strs := make(map[uint64]string, count)
	if sm.sparseStrs != nil {
		for idx := start; idx <= sm.Next; idx++ {
			s, ok := sm.sparseStrs[idx]
			if !ok {
				continue
			}
			strs[idx] = s
		}
	} else {
		end := int(sm.Next)
		for i := int(start); i <= end; i++ {
			if i >= len(sm.strsUsed) || !sm.strsUsed[i] {
				continue
			}
			s := sm.Strs[i]
			idx := uint64(i)
			strs[idx] = s
		}
	}

	anchor := base
	if anchor.depth > 1 && anchor.anchor != nil {
		anchor = anchor.anchor
	}

	return &strMapSnapshot{
		Next:   sm.Next,
		Strs:   strs,
		base:   base,
		anchor: anchor,
		depth:  base.depth + 1,
	}, true
}

func (sm *strMapper) restoreCommittedNoLock() {
	baseNext := uint64(0)
	if sm.committed != nil {
		baseNext = sm.committed.Next
	}
	if sm.committed == nil {
		base := &strMapSnapshot{}
		sm.snap = base
		sm.published = base
		sm.pubSource = base
		sm.committed = base
		sm.committedPub = base
		sm.dirty = sm.Next != 0
		return
	}
	sm.snap = sm.committed
	sm.published = sm.committedPub
	sm.pubSource = sm.committed
	sm.dirty = sm.Next != baseNext
}

func (sm *strMapper) markCommittedPublished(published *strMapSnapshot) {
	sm.Lock()
	defer sm.Unlock()
	sm.markCommittedPublishedNoLock(published)
}

func (sm *strMapper) markCommittedPublishedNoLock(published *strMapSnapshot) {
	if sm.published == published && sm.pubSource != nil {
		sm.committed = sm.pubSource
		sm.committedPub = published
		return
	}
	if sm.snap != nil && !sm.dirty {
		sm.committed = sm.snap
		if published != nil {
			sm.committedPub = published
			return
		}
		sm.committedPub = buildPublishedStrMapSnapshot(sm.snap, nil, nil)
	}
}

type strMapReadBuilder struct {
	dirs   []*strMapReadDir
	shared []*strMapReadDir
}

func newStrMapReadBuilder(pageCount int, shared []*strMapReadDir) strMapReadBuilder {
	dirCount := (pageCount + strMapReadDirSize - 1) >> strMapReadDirShift
	dirs := make([]*strMapReadDir, dirCount)
	if len(shared) > 0 {
		copy(dirs, shared[:min(len(shared), len(dirs))])
	}
	return strMapReadBuilder{
		dirs:   dirs,
		shared: shared,
	}
}

func (b *strMapReadBuilder) pageAtNoLock(page int) *strMapReadPage {
	if b == nil || page < 0 {
		return nil
	}
	dirIdx := page >> strMapReadDirShift
	if dirIdx >= len(b.dirs) {
		return nil
	}
	dir := b.dirs[dirIdx]
	if dir == nil {
		return nil
	}
	return dir.Pages[page&strMapReadDirMask]
}

func (b *strMapReadBuilder) setPageNoLock(page int, readPage *strMapReadPage) {
	if b == nil || page < 0 {
		return
	}
	dirIdx := page >> strMapReadDirShift
	if dirIdx >= len(b.dirs) {
		return
	}
	dir := b.dirs[dirIdx]
	if dir == nil {
		dir = &strMapReadDir{}
		b.dirs[dirIdx] = dir
	} else if dirIdx < len(b.shared) && dir == b.shared[dirIdx] {
		cloned := *dir
		dir = &cloned
		b.dirs[dirIdx] = dir
	}
	dir.Pages[page&strMapReadDirMask] = readPage
}

func appendStrMapReadPagesNoLock(builder *strMapReadBuilder, node *strMapSnapshot, start, next uint64) {
	if builder == nil || node == nil || start > next || next == 0 {
		return
	}

	firstPage := strMapReadPageIndex(start)
	lastPage := strMapReadPageIndex(next)
	if node.Strs == nil {
		for page := firstPage; page <= lastPage; page++ {
			pageStart, pageNext := strMapReadPageBounds(page, next)
			if pageStart < start {
				existing := builder.pageAtNoLock(page)
				if existing != nil {
					builder.setPageNoLock(page, materializeStrMapReadPageNoLock(existing, node, start, pageStart, pageNext))
				} else {
					builder.setPageNoLock(page, newStrMapReadPageNoLock(node, start, pageNext))
				}
				continue
			}
			builder.setPageNoLock(page, newStrMapReadPageNoLock(node, pageStart, pageNext))
		}
		return
	}

	if firstPage == lastPage {
		pageStart, pageNext := strMapReadPageBounds(firstPage, next)
		if pageStart < start {
			existing := builder.pageAtNoLock(firstPage)
			if existing != nil {
				builder.setPageNoLock(firstPage, materializeStrMapReadPageNoLock(existing, node, start, pageStart, pageNext))
				return
			}
			pageStart = start
		}
		builder.setPageNoLock(firstPage, newStrMapReadPageNoLock(node, pageStart, pageNext))
		return
	}

	pageMaps := buildStrMapSparsePageMapsNoLock(node.Strs, start, next)
	for page := firstPage; page <= lastPage; page++ {
		pageStart, pageNext := strMapReadPageBounds(page, next)
		if pageStart < start {
			existing := builder.pageAtNoLock(page)
			if existing != nil {
				builder.setPageNoLock(page, materializeStrMapReadPageNoLock(existing, node, start, pageStart, pageNext))
				continue
			}
			pageStart = start
		}
		pageStrs := pageMaps[page]
		if len(pageStrs) == 0 {
			continue
		}
		builder.setPageNoLock(page, &strMapReadPage{
			Start: pageStart,
			Next:  pageNext,
			Strs:  pageStrs,
		})
	}
}

func buildPublishedStrMapSnapshotFromChain(state *strMapSnapshot) *strMapSnapshot {
	if state == nil || state.Next == 0 {
		return &strMapSnapshot{}
	}
	depth := 0
	for cur := state; cur != nil; cur = cur.base {
		depth++
	}
	chain := make([]*strMapSnapshot, depth)
	i := depth
	for cur := state; cur != nil; cur = cur.base {
		i--
		chain[i] = cur
	}
	chain = chain[i:]

	builder := newStrMapReadBuilder(strMapReadPageCount(state.Next), nil)
	for _, node := range chain {
		start := node.baseNextNoLock() + 1
		if node.base == nil {
			start = 1
		}
		if node.Next == 0 || start > node.Next {
			continue
		}
		appendStrMapReadPagesNoLock(&builder, node, start, node.Next)
	}
	return &strMapSnapshot{
		Next:     state.Next,
		readDirs: builder.dirs,
	}
}

func buildPublishedStrMapSnapshotFromDelta(state *strMapSnapshot, prev *strMapSnapshot) *strMapSnapshot {
	if state == nil || state.Next == 0 {
		return &strMapSnapshot{}
	}
	if prev == nil || state.base == nil {
		return buildPublishedStrMapSnapshotFromChain(state)
	}

	builder := newStrMapReadBuilder(strMapReadPageCount(state.Next), prev.readDirs)
	start := state.baseNextNoLock() + 1
	appendStrMapReadPagesNoLock(&builder, state, start, state.Next)
	return &strMapSnapshot{
		Next:     state.Next,
		readDirs: builder.dirs,
	}
}

func buildPublishedStrMapSnapshot(state, prevSource, prevPublished *strMapSnapshot) *strMapSnapshot {
	if state == nil || state.Next == 0 {
		return &strMapSnapshot{}
	}
	if state.base == nil {
		return &strMapSnapshot{
			Next:      state.Next,
			Strs:      state.Strs,
			DenseStrs: state.DenseStrs,
			DenseUsed: state.DenseUsed,
		}
	}
	if prevPublished != nil && prevSource == state.base && len(prevPublished.readDirs) > 0 {
		return buildPublishedStrMapSnapshotFromDelta(state, prevPublished)
	}
	return buildPublishedStrMapSnapshotFromChain(state)
}

func strMapSnapshotUsedCountNoLock(s *strMapSnapshot) int {
	if s == nil {
		return 0
	}
	if len(s.readDirs) > 0 {
		count := 0
		for _, dir := range s.readDirs {
			if dir == nil {
				continue
			}
			for i := range dir.Pages {
				count += dir.Pages[i].usedCountNoLock()
			}
		}
		return count
	}
	return strMapSnapshotOwnUsedCount(s)
}
