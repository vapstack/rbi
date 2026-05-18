package rbi

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/qexec"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/internal/strmap"
	"go.etcd.io/bbolt"
)

var (
	ErrNotStructType     = errors.New("value is not a struct")
	ErrClosed            = errors.New("database closed")
	ErrBroken            = errors.New("index is broken")
	ErrNoIndex           = errors.New("index is disabled (transparent mode)")
	ErrRebuildInProgress = errors.New("index rebuild in progress")
	ErrInvalidQuery      = qexec.ErrInvalidQuery
	ErrInvalidBucketName = errors.New("invalid bucket name")
	ErrUniqueViolation   = errors.New("unique constraint violation")
	ErrNoValidKeyIndex   = errors.New("no valid key for index")
	ErrNilValue          = errors.New("value is nil")

	errPersistedIndexStale   = errors.New("persisted index is stale")
	errPersistedIndexInvalid = errors.New("persisted index is invalid")
)

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

	db = &DB[K, V]{
		vtype:     vtype,
		bolt:      bolt,
		bucket:    []byte(vname),
		options:   &options,
		logger:    options.Logger,
		rebuilder: new(rebuilder),

		rbiFile: bolt.Path() + "." + vname + ".rbi",

		execOptions: defaultExecOptions,
		encodeFn:    encodeFn,
		decodeFn:    decodeFn,
	}
	db.rebuilder.cond = sync.NewCond(&db.rebuilder.mu)

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

	db.engine, db.strMap, err = newQueryEngineForRuntime(db.schema, db.options, db.strKey)
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
	if db.engine != nil {
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

	if db.engine != nil {
		db.stats.FieldCount = len(db.schema.Fields) + len(db.schema.MeasureFields)
		if err = db.engine.publishCurrentSequenceSnapshotNoLock(db.bolt, db.bucket, db.strMap); err != nil {
			return nil, fmt.Errorf("failed to publish initial snapshot: %w", err)
		}

		if loadedPlannerStats != nil && loadedOrdinaryFieldCount == len(db.schema.Fields) {
			db.engine.exec.PublishLoadedPlannerStats(loadedPlannerStats, db.engine.snapshot.Current())

		} else {
			if err = db.RefreshPlannerStats(); err != nil {
				return nil, fmt.Errorf("failed to build planner stats snapshot: %w", err)
			}
			if buildMode != "" {
				forceMemoryCleanup(true)
			}
		}

		db.startPlannerAnalyzeLoop()
	}

	return db, nil
}

func newQueryEngineForRuntime(rt *schema.Runtime, options *Options, strKey bool) (*queryEngine, *strmap.Mapper, error) {
	if !rt.HasQueryFields() {
		return nil, nil, nil
	}

	var strMap *strmap.Mapper
	if strKey {
		strMap = strmap.New(0, defaultSnapshotStrMapCompactDepth)
	}

	qe := &queryEngine{
		schema:                 rt,
		universe:               posting.List{},
		matPredCacheMaxEntries: max(0, options.SnapshotMaterializedPredCacheMaxEntries),
		matPredCacheMaxCard:    qcache.MaterializedPredMaxCardinality(options.SnapshotMaterializedPredCacheMaxCardinality),
		snapshot:               snapshot.NewManager(options.EnableSnapshotStats),
		exec: qexec.NewRuntime(qexec.Config{
			Schema:                         rt,
			NumericRangeBucketSize:         options.NumericRangeBucketSize,
			NumericRangeBucketMinFieldKeys: options.NumericRangeBucketMinFieldKeys,
			NumericRangeBucketMinSpanKeys:  options.NumericRangeBucketMinSpanKeys,
			AnalyzeInterval:                plannerAnalyzeInterval(options.AnalyzeInterval),
			AnalyzeSoftBudget:              defaultAnalyzeSoftBudget,
			TraceSink:                      options.TraceSink,
			TraceSampleEvery:               options.TraceSampleEvery,
		}),
	}
	slotCount := len(rt.Indexed)
	qe.index = indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	qe.nilIndex = indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	qe.lenIndex = indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	qe.lenZeroComplement = pooled.GetBoolSlice(slotCount)[:slotCount]
	clear(qe.lenZeroComplement)
	measureSlotCount := len(rt.Measures)
	qe.measure = indexdata.GetMeasureStorageSlice(measureSlotCount)[:measureSlotCount]
	return qe, strMap, nil
}

func (db *DB[K, V]) initBatcher() {
	runtime := &autoBatchRuntime{
		bolt:               db.bolt,
		bucket:             db.bucket,
		bucketFillPercent:  db.options.BucketFillPercent,
		strKey:             db.strKey,
		strMap:             db.strMap,
		engine:             db.engine,
		schema:             db.schema,
		testHookAccessor:   func() *testHooks { return db.testHooks },
		broken:             &db.broken,
		logger:             db.logger,
		mu:                 &db.mu,
		closed:             &db.closed,
		rejectEmptyPayload: db.decodeFn == nil,
		ops: autoBatchRecordOps{
			encode: func(ptr unsafe.Pointer, buf *bytes.Buffer) error {
				return db.encode((*V)(ptr), buf)
			},
			decode: func(data []byte) (unsafe.Pointer, error) {
				val, err := db.decode(data)
				if err != nil {
					return nil, err
				}
				return unsafe.Pointer(val), nil
			},
			release: func(ptr unsafe.Pointer) {
				db.ReleaseRecords((*V)(ptr))
			},
			validateIndex: func(ptr unsafe.Pointer) error {
				return db.validateIndexedStringValues((*V)(ptr))
			},
		},
	}
	db.autoBatcher = newAutoBatcher(db.options, runtime)
}

func newAutoBatcher(options *Options, runtime *autoBatchRuntime) *autoBatcher {
	ab := &autoBatcher{
		autoBatchScheduler: newAutoBatchScheduler(options),
		runtime:            runtime,
		repeatUintIDPool: pooled.Maps[uint64, int]{
			NewCap: 8,
		},
		repeatStringIDPool: pooled.Maps[string, int]{
			NewCap: 8,
		},
		requestScratchPool: pooled.NewSlicePool[*autoBatchRequest](uint(max(defaultAutoBatchMax, options.AutoBatchMax)), pooled.ClearCap),
		attemptStatePool: pooled.Pointers[autoBatchAttemptState]{
			Cleanup: func(st *autoBatchAttemptState) {
				st.autoBatchAttemptCore.cleanup()

				clear(st.prepared)
				st.prepared = st.prepared[:0]

				clear(st.accepted)
				st.accepted = st.accepted[:0]

				clear(st.states)
				st.states = st.states[:0]

				if st.stateByUintID != nil {
					clear(st.stateByUintID)
				}
				if st.stateByStringID != nil {
					clear(st.stateByStringID)
				}
			},
		},
		requestPool: pooled.Pointers[autoBatchRequest]{
			Init: func(req *autoBatchRequest) {
				if req.done == nil {
					req.done = make(chan error, 1)
				}
			},
			Cleanup: func(req *autoBatchRequest) {
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
				req.id = keycodec.DataKey{}
				req.err = nil
			},
		},
	}
	ab.cond = sync.NewCond(&ab.mu)
	return ab
}

func newAutoBatchScheduler(options *Options) autoBatchScheduler {
	maxOps := options.AutoBatchMax
	window := options.AutoBatchWindow
	if maxOps <= 1 || window <= 0 {
		maxOps = 1
		window = 0
	}
	maxQueue := options.AutoBatchMaxQueue
	if maxQueue < 0 {
		maxQueue = 0
	}
	capHint := max(64, maxOps*4)
	if maxQueue > 0 && maxQueue < capHint {
		capHint = maxQueue
	}
	return autoBatchScheduler{
		statsEnabled: options.EnableAutoBatchStats,
		window:       window,
		maxOps:       maxOps,
		maxQ:         maxQueue,
		waitNotify:   make(chan struct{}, 1),
		queue:        make([]*autoBatchJob, capHint),
		jobPool: pooled.Pointers[autoBatchJob]{
			Init: func(job *autoBatchJob) {
				if job.done == nil {
					job.done = make(chan error, 1)
				}
			},
			Cleanup: func(job *autoBatchJob) {
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
}

type autoBatchScheduler struct {
	statsEnabled bool
	window       time.Duration
	maxOps       int
	maxQ         int

	queue        []*autoBatchJob
	batchScratch []*autoBatchJob
	jobPool      pooled.Pointers[autoBatchJob]

	mu           sync.Mutex
	cond         *sync.Cond
	waitNotify   chan struct{}
	waitTimer    *time.Timer
	running      bool
	queueHead    int
	queueSize    int
	hotUntil     time.Time
	hotBatchSize int

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

type autoBatcher struct {
	autoBatchScheduler

	runtime            *autoBatchRuntime
	repeatUintIDPool   pooled.Maps[uint64, int]
	repeatStringIDPool pooled.Maps[string, int]
	requestScratchPool *pooled.SlicePool[*autoBatchRequest]
	attemptStatePool   pooled.Pointers[autoBatchAttemptState]
	requestPool        pooled.Pointers[autoBatchRequest]
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
		vtype reflect.Type

		bolt   *bbolt.DB
		bucket []byte

		options *Options
		logger  *log.Logger

		strKey bool
		strMap *strmap.Mapper

		schema *schema.Runtime

		engine    *queryEngine // nil in transparent mode
		rebuilder *rebuilder

		closed atomic.Bool
		broken atomic.Bool

		autoBatcher *autoBatcher

		execOptions execOptions[K, V]

		rbiFile string

		encodeFn func(*V, io.Writer) error
		decodeFn func(*V, io.Reader) error

		recPool pooled.Pointers[V]

		mu sync.RWMutex

		stats Stats[K]

		testHooks *testHooks
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

func (qe *queryEngine) publishCurrentSequenceSnapshotNoLock(bolt *bbolt.DB, bucket []byte, strMap *strmap.Mapper) error {
	seq, err := currentBucketSequence(bolt, bucket)
	if err != nil {
		return err
	}
	var strSnap *strmap.Snapshot
	if strMap != nil {
		strSnap = strMap.Snapshot()
	}
	prev := qe.snapshot.Current()
	var snap *snapshot.View
	if prev != nil {
		lenZeroComplement := pooled.GetBoolSlice(len(qe.schema.Indexed))[:len(qe.schema.Indexed)]
		clear(lenZeroComplement)
		copy(lenZeroComplement, qe.lenZeroComplement)
		snap = snapshot.NewView(seq, prev, qe.schema, qe.snapshotCacheConfig(), snapshot.Storage{
			Index:             indexdata.CloneFieldStorageSlots(qe.index, len(qe.schema.Indexed)),
			NilIndex:          indexdata.CloneFieldStorageSlots(qe.nilIndex, len(qe.schema.Indexed)),
			LenIndex:          indexdata.CloneFieldStorageSlots(qe.lenIndex, len(qe.schema.Indexed)),
			LenZeroComplement: lenZeroComplement,
			Measure:           indexdata.CloneMeasureStorageSlots(qe.measure, len(qe.schema.Measures)),
			Universe:          qe.universe,
			StrMap:            strSnap,
		})
	} else {
		snap = snapshot.NewView(seq, nil, qe.schema, qe.snapshotCacheConfig(), snapshot.Storage{
			Index:             qe.index,
			NilIndex:          qe.nilIndex,
			LenIndex:          qe.lenIndex,
			LenZeroComplement: qe.lenZeroComplement,
			Measure:           qe.measure,
			Universe:          qe.universe,
			StrMap:            strSnap,
		})
	}
	qe.installViewNoLock(snap)
	if strMap != nil {
		strMap.MarkCommittedPublished(strSnap)
	}
	return nil
}

func (qe *queryEngine) publishStorageSnapshotNoLock(seq uint64, strMap *strmap.Mapper, st snapshot.Storage) {
	if strMap != nil {
		st.StrMap = strMap.Snapshot()
	}
	snap := snapshot.NewView(seq, qe.snapshot.Current(), qe.schema, qe.snapshotCacheConfig(), st)
	qe.installViewNoLock(snap)
	if strMap != nil {
		strMap.MarkCommittedPublished(st.StrMap)
	}
}

func publishAfterCommitLocked(testHooks *testHooks, broken *atomic.Bool, logger *log.Logger, engine *queryEngine, seq uint64, op string, publish func()) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if broken.CompareAndSwap(false, true) {
				logger.Printf("rbi: index entered broken state: post-commit snapshot publish failed (%v): %v", op, r)
				if stop := engine.exec.Analyzer.Stop; stop != nil {
					select {
					case <-stop:
					default:
						close(stop)
					}
				}
			}
			err = ErrBroken
			engine.snapshot.DropStaged(seq)
		}
	}()
	if testHooks != nil {
		if hook := testHooks.afterCommitPublish; hook != nil {
			hook(op)
		}
	}
	publish()
	return nil
}

func commitTx(tx *bbolt.Tx, op string, testHooks *testHooks) error {
	if testHooks != nil {
		if hook := testHooks.beforeCommit; hook != nil {
			if err := hook(op); err != nil {
				return err
			}
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
	if db.engine == nil {
		return ErrNoIndex
	}

	if err := db.buildIndex(nil, nil); err != nil {
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

	db.mu.RLock()
	out := db.stats
	db.mu.RUnlock()

	if db.engine != nil {
		snap, seq, ref := db.engine.snapshot.PinCurrent()
		defer db.engine.snapshot.Unpin(seq, ref)

		out.KeyCount = snap.UniverseCardinality()
		out.LastKey = db.statsLastKeyFromSnapshot(snap, out.KeyCount)
		out.SnapshotSequence = snap.Seq
	}

	db.autoBatcher.mu.Lock()
	out.AutoBatchQueueLen = db.autoBatcher.queueSize
	db.autoBatcher.mu.Unlock()
	out.AutoBatchQueueMax = db.autoBatcher.queueHighWater.Load()

	out.AutoBatchCount = db.autoBatcher.executedBatches.Load()
	if out.AutoBatchCount > 0 {
		out.AutoBatchAvgSize = float64(db.autoBatcher.dequeued.Load()) / float64(out.AutoBatchCount)
	}

	return out
}

func (db *DB[K, V]) statsLastKeyFromSnapshot(snap *snapshot.View, keyCount uint64) K {
	var zero K
	if snap == nil || keyCount == 0 {
		return zero
	}

	var maxIdx uint64
	if snap.Universe.IsEmpty() {
		return zero
	}
	maxIdx, _ = snap.Universe.Maximum()

	return db.statsKeyFromIdx(snap, maxIdx)
}

func (db *DB[K, V]) statsKeyFromIdx(snap *snapshot.View, idx uint64) K {
	var zero K
	if db.strKey {
		if snap == nil || snap.StrMap == nil {
			return zero
		}
		if key, ok := snap.StrMap.String(idx); ok {
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

	if db.engine == nil {
		return IndexStats{}
	}

	snap, seq, ref := db.engine.snapshot.PinCurrent()
	defer db.engine.snapshot.Unpin(seq, ref)

	return db.engine.indexStats(snap)
}

func (qe *queryEngine) indexStats(snap *snapshot.View) IndexStats {
	idx := IndexStats{
		UniqueFieldKeys:        make(map[string]uint64),
		FieldSize:              make(map[string]uint64),
		FieldKeyBytes:          make(map[string]uint64),
		FieldTotalCardinality:  make(map[string]uint64),
		FieldApproxStructBytes: make(map[string]uint64),
		FieldApproxHeapBytes:   make(map[string]uint64),
	}
	sharedStructBytes := indexdata.FieldStorageSlotsApproxBytes(snap.Index) + indexdata.FieldStorageSlotsApproxBytes(snap.NilIndex)
	fieldCount := len(qe.schema.Indexed)
	sharedStructPerField := uint64(0)
	sharedStructRemainder := uint64(0)
	if fieldCount > 0 {
		sharedStructPerField = sharedStructBytes / uint64(fieldCount)
		sharedStructRemainder = sharedStructBytes % uint64(fieldCount)
	}

	for i, acc := range qe.schema.Indexed {
		name := acc.Name
		fieldStats := snap.Index[acc.Ordinal].Stats(true)
		nilStats := snap.NilIndex[acc.Ordinal].Stats(false)

		unique := fieldStats.Unique + nilStats.Unique
		size := fieldStats.PostingBytes + nilStats.PostingBytes
		keyLen := fieldStats.KeyBytes + nilStats.KeyBytes
		card := fieldStats.PostingCardinality + nilStats.PostingCardinality

		idx.UniqueFieldKeys[name] = unique
		idx.FieldSize[name] = size
		idx.FieldKeyBytes[name] = keyLen
		idx.FieldTotalCardinality[name] = card
		fieldStructBytes := fieldStats.ApproxStructBytes + nilStats.ApproxStructBytes + sharedStructPerField
		if uint64(i) < sharedStructRemainder {
			fieldStructBytes++
		}
		idx.FieldApproxStructBytes[name] = fieldStructBytes
		idx.FieldApproxHeapBytes[name] = size + keyLen + fieldStructBytes

		idx.Size += size
		idx.EntryCount += fieldStats.EntryCount + nilStats.EntryCount
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

// AutoBatchStats returns auto-batcher queue, batch, and availability diagnostics.
//
// Runtime counters are collected only when Options.EnableAutoBatchStats was
// enabled for this DB instance; otherwise the method returns a zero value.
//
// The method is independent of indexed versus transparent mode. Write-path
// batching remains active in both modes, so when stats collection is enabled
// the returned counters and queue state reflect the current write workload.
func (db *DB[K, V]) AutoBatchStats() AutoBatchStats {
	return db.autoBatcher.autoBatchScheduler.snapshotStats()
}

func (ab *autoBatchScheduler) snapshotStats() AutoBatchStats {
	if !ab.statsEnabled {
		return AutoBatchStats{}
	}
	out := AutoBatchStats{
		Window:              ab.window,
		MaxBatch:            ab.maxOps,
		MaxQueue:            ab.maxQ,
		Submitted:           ab.submitted.Load(),
		Enqueued:            ab.enqueued.Load(),
		Dequeued:            ab.dequeued.Load(),
		QueueHighWater:      ab.queueHighWater.Load(),
		ExecutedBatches:     ab.executedBatches.Load(),
		MultiRequestBatches: ab.multiReqBatches.Load(),
		MultiRequestOps:     ab.multiReqOps.Load(),
		BatchSize1:          ab.batchSize1.Load(),
		BatchSize2To4:       ab.batchSize2To4.Load(),
		BatchSize5To8:       ab.batchSize5To8.Load(),
		BatchSize9Plus:      ab.batchSize9Plus.Load(),
		MaxBatchSeen:        ab.maxBatchSeen.Load(),
		CallbackOps:         ab.callbackOps.Load(),
		CoalescedSetDelete:  ab.coalescedSetDelete.Load(),
		CoalesceWaits:       ab.coalesceWaits.Load(),
		CoalesceWaitTime:    time.Duration(ab.coalesceWaitNanos.Load()),
		QueueWaitTime:       time.Duration(ab.queueWaitNanos.Load()),
		ExecuteTime:         time.Duration(ab.executeNanos.Load()),
		FallbackClosed:      ab.fallbackClosed.Load(),
		UniqueRejected:      ab.uniqueRejected.Load(),
		TxBeginErrors:       ab.txBeginErrors.Load(),
		TxOpErrors:          ab.txOpErrors.Load(),
		TxCommitErrors:      ab.txCommitErrors.Load(),
		CallbackErrors:      ab.callbackErrors.Load(),
	}

	ab.mu.Lock()
	out.QueueLen = ab.queueSize
	out.QueueCap = cap(ab.queue)
	out.WorkerRunning = ab.running
	out.HotWindowActive = ab.window > 0 &&
		time.Now().Before(ab.hotUntil) &&
		(ab.hotBatchSize == 0 || ab.hotBatchSize >= 3)
	ab.mu.Unlock()

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
// In transparent mode no query engine and no runtime planner exist, so the
// returned planner payload is zero-valued.
func (db *DB[K, V]) PlannerStats() PlannerStats {
	var out PlannerStats
	if db.engine == nil {
		return out
	}
	out.Fields = make(map[string]PlannerFieldStats)

	if ps := db.engine.exec.Stats.Load(); ps != nil {
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
	out.TraceSampleEvery = db.engine.exec.Tracer.SampleEvery()
	db.engine.exec.Analyzer.Lock()
	out.AnalyzeInterval = db.engine.exec.Analyzer.Interval
	db.engine.exec.Analyzer.Unlock()
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

	if db.engine != nil && !db.options.DisableIndexStore && !db.broken.Load() {
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
	if db.engine == nil {
		if _, err = bucket.NextSequence(); err != nil {
			return fmt.Errorf("advance bucket sequence: %w", err)
		}
		if err = commitTx(tx, "truncate", db.testHooks); err != nil {
			return fmt.Errorf("commit error: %w", err)
		}
		return nil
	}
	seq, err := bucket.NextSequence()
	if err != nil {
		return fmt.Errorf("advance bucket sequence: %w", err)
	}

	slotCount := len(db.engine.schema.Indexed)
	nextIndex := indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	nextNilIndex := indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	nextLenIndex := indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	nextLenZeroComplement := pooled.GetBoolSlice(slotCount)[:slotCount]
	clear(nextLenZeroComplement)
	measureSlotCount := len(db.engine.schema.Measures)
	nextMeasure := indexdata.GetMeasureStorageSlice(measureSlotCount)[:measureSlotCount]
	nextUniverse := posting.List{}
	var nextStrMap *strmap.Snapshot
	if db.strKey {
		nextStrMap = strmap.EmptySnapshot()
	}
	snap := snapshot.NewView(seq, db.engine.snapshot.Current(), db.engine.schema, db.engine.snapshotCacheConfig(), snapshot.Storage{
		Index:             nextIndex,
		NilIndex:          nextNilIndex,
		LenIndex:          nextLenIndex,
		LenZeroComplement: nextLenZeroComplement,
		Measure:           nextMeasure,
		Universe:          nextUniverse,
		StrMap:            nextStrMap,
	})
	db.engine.snapshot.Stage(snap)
	if err = commitTx(tx, "truncate", db.testHooks); err != nil {
		db.engine.snapshot.DropStaged(seq)
		return fmt.Errorf("commit error: %w", err)
	}

	if db.strKey {
		db.strMap.Truncate()
	}
	db.engine.index = nextIndex
	db.engine.nilIndex = nextNilIndex
	db.engine.lenIndex = nextLenIndex
	db.engine.lenZeroComplement = nextLenZeroComplement
	db.engine.measure = nextMeasure

	// Keep previously published snapshots immutable for concurrent readers.
	db.engine.universe = nextUniverse
	if err = publishAfterCommitLocked(db.testHooks, &db.broken, db.logger, db.engine, seq, "truncate", func() {
		db.engine.installViewNoLock(snap)
		if db.strMap != nil {
			db.strMap.MarkCommittedPublished(snap.StrMap)
		}
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
