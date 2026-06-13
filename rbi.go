package rbi

import (
	"bytes"
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
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/wexec"
	"github.com/vapstack/rbi/rbierrors"
	"github.com/vapstack/rbi/rbistats"
	"github.com/vapstack/rbi/rbitrace"
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

	// EnableStringKeyIndex enables synthetic string primary-key index.
	// When set on DB[string,V], queries and filters can reference "$key".
	// This can be the only queryable field and still enables indexed mode.
	//
	// It affects only DB[string,V] and ignored by numeric-key databases
	// (as they expose "$key" automatically in indexed mode).
	//
	// It is disabled by default to minimize memory usage.
	EnableStringKeyIndex bool

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
	TraceSink func(rbitrace.Event)

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

const stringMapBucketSuffix = ".rbimap"

// DB wraps a bbolt database and maintains secondary indexes over values of type *V
// stored in a single bucket. It supports efficient equality and range queries,
// as well as array membership and array-length queries for slice fields.
//
// DB is safe for concurrent use.
type DB[K ~string | ~uint64, V any] struct {
	vtype  reflect.Type
	strKey bool

	bolt         *bbolt.DB
	dataBucket   []byte
	strmapBucket []byte

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

	stats rbistats.DB[K]
}

// New creates a new indexed DB that uses the provided bbolt database.
//
// The generic type V must be a struct; otherwise rbierrors.ErrNotStructType is returned.
//
// Zero-valued option fields use defaults.
//
// If options.BucketName is empty,
// the name of the value type V is used as the bucket name.
// New ensures the bucket exists, optionally loads a persisted index from disk,
// builds missing or incompatible index data from bbolt, and sets up field
// metadata and accessors.
//
// Any ExecOptions passed to New become defaults applied to subsequent write
// operations on the returned DB.
//
// The resulting DB does not manage the underlying *bbolt.DB lifecycle.
func New[K ~uint64 | ~string, V any](bolt *bbolt.DB, options Options, execOpts ...ExecOption[K, V]) (db *DB[K, V], err error) {
	var v V
	vtype := reflect.TypeOf(v)
	if vtype == nil {
		return nil, rbierrors.ErrNotStructType
	}
	if vtype.Kind() != reflect.Struct {
		return nil, rbierrors.ErrNotStructType
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
		vtype:      vtype,
		bolt:       bolt,
		dataBucket: []byte(vname),
		options:    &options,
		logger:     options.Logger,

		rbiFile: bolt.Path() + "." + vname + ".rbi",

		execOptions: defaultExecOptions,
		encodeFn:    encodeFn,
		decodeFn:    decodeFn,
	}

	var k K
	if reflect.TypeOf(k).Kind() == reflect.String {
		db.strKey = true
		db.strmapBucket = append(db.dataBucket, stringMapBucketSuffix...)
	}
	strKeyIndex := db.strKey && options.EnableStringKeyIndex

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
		Schema:      db.schema,
		StrKey:      db.strKey,
		StrKeyIndex: strKeyIndex,

		SnapshotStats:                               db.options.EnableSnapshotStats,
		SnapshotMaterializedPredCacheMaxEntries:     db.options.SnapshotMaterializedPredCacheMaxEntries,
		SnapshotMaterializedPredCacheMaxCardinality: db.options.SnapshotMaterializedPredCacheMaxCardinality,

		NumericRangeBucketSize:         db.options.NumericRangeBucketSize,
		NumericRangeBucketMinFieldKeys: db.options.NumericRangeBucketMinFieldKeys,
		NumericRangeBucketMinSpanKeys:  db.options.NumericRangeBucketMinSpanKeys,

		AnalyzeInterval: plannerAnalyzeInterval(db.options.AnalyzeInterval),

		TraceSink:        db.options.TraceSink,
		TraceSampleEvery: db.options.TraceSampleEvery,
	})
	if err != nil {
		return nil, err
	}
	db.stats.Mode = rbistats.ModeIndexed
	if db.index == nil {
		db.stats.Mode = rbistats.ModeTransparent
	}
	db.stats.StringKeys = db.strKey
	db.stats.IndexFieldCount = len(db.schema.Fields) + len(db.schema.MeasureFields)
	db.stats.MeasureFieldCount = len(db.schema.MeasureFields)
	db.stats.UniqueFieldCount = len(db.schema.Unique)

	err = bolt.Update(func(tx *bbolt.Tx) error {
		if _, e := tx.CreateBucketIfNotExists(db.dataBucket); e != nil {
			return e
		}
		if db.strKey {
			_, e := createStrMapBucket(tx, db.dataBucket)
			return e
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	var (
		loadedPlannerStats       *rbistats.PlannerSnapshot
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
		if err = db.index.PublishCurrentSnapshot(db.bolt, db.dataBucket); err != nil {
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

func createStrMapBucket(tx *bbolt.Tx, dataBucket []byte) (*bbolt.Bucket, error) {
	data := tx.Bucket(dataBucket)
	if data == nil {
		return nil, fmt.Errorf("data bucket %q does not exist", dataBucket)
	}

	mapName := append(append([]byte(nil), dataBucket...), stringMapBucketSuffix...)
	m := tx.Bucket(mapName)
	if m != nil {
		return m, nil
	}

	k, _ := data.Cursor().First()
	if k != nil {
		return nil, fmt.Errorf("%w: missing string map bucket %q for non-empty data bucket", rbierrors.ErrInvalidStringStorageFormat, mapName)
	}

	m, err := tx.CreateBucket(mapName)
	if err != nil {
		return nil, fmt.Errorf("create string map bucket %q: %w", mapName, err)
	}
	return m, nil
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
		MaxOps:             db.options.AutoBatchMax,
		Window:             db.options.AutoBatchWindow,
		MaxQueue:           db.options.AutoBatchMaxQueue,
		StatsEnabled:       db.options.EnableAutoBatchStats,
		Unavailable:        db.unavailableErr,
		Bolt:               db.bolt,
		DataBucket:         db.dataBucket,
		StrMapBucket:       db.strmapBucket,
		BucketFillPercent:  db.options.BucketFillPercent,
		RejectEmptyPayload: db.decodeFn == nil,
		PublishMu:          &db.publishMu,
		StrKey:             db.strKey,
		Indexed:            db.index != nil,
		Ops:                &ops,
		Schema:             db.schema,
	}

	if db.index != nil {
		db.index.ConfigureWrite(&cfg, &db.broken, db.logger)
	}
	db.batcher = wexec.NewBatcher(cfg)
}

func (db *DB[K, V]) buildIndex(skipFields map[string]struct{}, skipMeasureFields map[string]struct{}) error {
	result, err := db.index.BuildIndex(
		db.bolt,
		db.dataBucket,
		db.strmapBucket,
		skipFields,
		skipMeasureFields,
		db.decodeBuildIndexRecord,
		db.releaseBuildIndexRecord,
	)
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
	plannerStats *rbistats.PlannerSnapshot,
	err error,
) {
	currentSeq, err := currentBucketSequence(db.bolt, db.dataBucket)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("decode: reading current bucket sequence: %w", err)
	}
	start := time.Now()
	result, err := db.index.LoadIndex(db.rbiFile, db.bolt.Path(), db.dataBucket, currentSeq)
	if err != nil {
		return nil, nil, nil, err
	}
	db.stats.LoadTime = time.Since(start)

	return result.SkipFields, result.SkipMeasureFields, result.PlannerStats, nil
}

func (db *DB[K, V]) storeIndex() error {
	return db.index.StoreIndex(db.rbiFile, db.bolt, db.dataBucket)
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
		return rbierrors.ErrClosed
	}
	if db.broken.Load() {
		return rbierrors.ErrBroken
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

// Stats returns a lightweight database status snapshot.
//
// In indexed mode it reports startup build/load timings together with the
// current published snapshot cardinality and last key.
//
// In transparent mode no runtime index snapshot is maintained, so fields that
// depend on indexed state remain zero-valued: BuildTime, BuildRPS, LoadTime,
// and KeyCount.
//
// LastKey, SnapshotSequence, and auto-batcher fields are populated in both modes.
func (db *DB[K, V]) Stats() (rbistats.DB[K], error) {
	if err := db.unavailableErr(); err != nil {
		return rbistats.DB[K]{}, err
	}

	// do not hold a bbolt read transaction while taking publishMu: writers open
	// a write transaction before publishing and commit while holding publishMu
	db.publishMu.RLock()
	out := db.stats
	db.publishMu.RUnlock()

	var seq uint64
	if db.index != nil {
		st, err := db.index.DBStats(db.bolt, db.dataBucket, db.unavailableErr)
		if err != nil {
			return rbistats.DB[K]{}, err
		}
		seq = st.Sequence
		out.KeyCount = st.KeyCount
		if st.HasLast {
			out.LastKey = keycodec.UserKeyFromDataKey[K](st.LastKey, db.strKey)
		}

	} else {
		tx, err := db.bolt.Begin(false)
		if err != nil {
			return rbistats.DB[K]{}, fmt.Errorf("tx error: %w", err)
		}
		defer rollback(tx)

		b := tx.Bucket(db.dataBucket)
		if b == nil {
			return rbistats.DB[K]{}, fmt.Errorf("bucket does not exist")
		}
		seq = b.Sequence()
		key, _ := b.Cursor().Last()
		if key != nil {
			if !db.strKey && len(key) != 8 {
				return rbistats.DB[K]{}, fmt.Errorf("invalid numeric data key length: %d", len(key))
			}
			out.LastKey = keycodec.UserKeyFromDataKey[K](keycodec.DataKeyFromBytes(key, db.strKey), db.strKey)
		}
	}

	out.SnapshotSequence = seq

	queueLen, queueMax, executed, dequeued := db.batcher.BasicStats()
	out.AutoBatchQueueLen = queueLen
	out.AutoBatchQueueMax = queueMax

	out.AutoBatchCount = executed
	if out.AutoBatchCount > 0 {
		out.AutoBatchAvgSize = float64(dequeued) / float64(out.AutoBatchCount)
	}

	return out, nil
}

// IndexStats returns current expensive index shape and memory diagnostics.
//
// In indexed mode it walks the published index snapshot and reports per-field
// entry counts, key bytes, posting cardinalities, and approximate memory
// usage for regular field value indexes plus synthetic nil-family indexes.
// When the string key index is enabled, its stats are reported in
// StringKeyIndex field.
//
// On large databases IndexStats can be expensive.
//
// In transparent mode it returns a zero-valued IndexStats because no secondary
// indexes, universe bitmap, or string-key runtime mapping are maintained.
//
// If DB is closed or broken, IndexStats returns a zero value.
func (db *DB[K, V]) IndexStats() rbistats.Index {
	if err := db.unavailableErr(); err != nil {
		return rbistats.Index{}
	}

	if db.index == nil {
		return rbistats.Index{}
	}

	return db.index.IndexStats()
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
func (db *DB[K, V]) AutoBatchStats() rbistats.AutoBatch {
	return db.batcher.Stats()
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
func (db *DB[K, V]) PlannerStats() rbistats.Planner {
	if db.index == nil {
		return rbistats.Planner{}
	}
	return db.index.PlannerStats()
}

// Close closes the indexer and persists the current index state
// to the .rbi file unless index persistence is disabled.
//
// After Close, all other methods return rbierrors.ErrClosed.
// Subsequent calls to Close are no-op.
//
// It does not close the underlying *bbolt.DB.
func (db *DB[K, V]) Close() error {
	if !db.closed.CompareAndSwap(false, true) {
		return nil
	}
	defer unregInstance(db.bolt.Path(), string(db.dataBucket))

	db.stopAnalyzeLoop()

	db.batcher.WakeWaiters()

	db.publishMu.Lock()
	defer db.publishMu.Unlock()

	var err error

	if db.index != nil && !db.options.DisableIndexStore && !db.broken.Load() {
		err = db.storeIndex()
	}

	if err == nil && db.broken.Load() {
		err = rbierrors.ErrBroken
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
	if bucket := tx.Bucket(db.dataBucket); bucket != nil {
		prevSeq = bucket.Sequence()
		if err = tx.DeleteBucket(db.dataBucket); err != nil {
			return fmt.Errorf("error deleting bucket: %w", err)
		}
	}
	if db.strKey && tx.Bucket(db.strmapBucket) != nil {
		if err = tx.DeleteBucket(db.strmapBucket); err != nil {
			return fmt.Errorf("error deleting string map bucket: %w", err)
		}
	}

	bucket, err := tx.CreateBucketIfNotExists(db.dataBucket)
	if err != nil {
		return fmt.Errorf("error creating bucket: %w", err)
	}
	if err = bucket.SetSequence(prevSeq); err != nil {
		return fmt.Errorf("restore bucket sequence: %w", err)
	}
	if db.strKey {
		if _, err = createStrMapBucket(tx, db.dataBucket); err != nil {
			return err
		}
	}
	if db.index == nil {
		if _, err = bucket.NextSequence(); err != nil {
			return fmt.Errorf("advance bucket sequence: %w", err)
		}
		if err = tx.Commit(); err != nil {
			return fmt.Errorf("commit error: %w", err)
		}
		return nil
	}
	seq, err := bucket.NextSequence()
	if err != nil {
		return fmt.Errorf("advance bucket sequence: %w", err)
	}

	staged := db.index.StageTruncate(seq)
	if err = tx.Commit(); err != nil {
		db.index.DropStaged(seq)
		return fmt.Errorf("commit error: %w", err)
	}

	if err = db.index.PublishCommittedStaged(&db.broken, db.logger, seq, "truncate", staged); err != nil {
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
//
// Should be used with caution; callers must not mutate buckets managed by RBI,
// including sequence counters.
func (db *DB[K, V]) Bolt() *bbolt.DB {
	return db.bolt
}

// BucketName returns a name of the bucket at which the data is stored.
func (db *DB[K, V]) BucketName() []byte {
	return slices.Clone(db.dataBucket)
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
		return fmt.Errorf("%w: empty", rbierrors.ErrInvalidBucketName)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if i == 0 {
			if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' {
				continue
			}
			return fmt.Errorf(
				"%w %q: allowed pattern is [A-Za-z_][A-Za-z0-9_]*",
				rbierrors.ErrInvalidBucketName,
				name,
			)
		}
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			continue
		}
		return fmt.Errorf(
			"%w %q: allowed pattern is [A-Za-z_][A-Za-z0-9_]*",
			rbierrors.ErrInvalidBucketName,
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
