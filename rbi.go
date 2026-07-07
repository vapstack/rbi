package rbi

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"time"
	"unsafe"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/engine"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/qexec"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/internal/wexec"
	"github.com/vapstack/rbi/rbierrors"
	"github.com/vapstack/rbi/rbistats"
	"github.com/vapstack/rbi/rbitrace"
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
// representation used as an index key.
//
// IndexingValue method must return a stable, deterministic string.
// Equal field values must produce equal indexing strings.
// The returned string must not exceed 65535 bytes.
//
// Indexed fields that use ValueIndexer must be declared with concrete types,
// not with ValueIndexer interface or any other interface type.
//
// For a type T that implements ValueIndexer with a value receiver, nil *T
// scalar values are indexed as null. In slice indexes, nil *T elements for
// value-receiver T do not emit index keys.
//
// Nil pointer-receiver values are passed to IndexingValue;
// nil handling is the responsibility of the implementation.
//
// The returned string is compared lexicographically when evaluating
// range queries (>, >=, <, <=). Implementation must ensure that the
// produced ordering matches the intent.
type ValueIndexer = schema.ValueIndexer

// Key is the reserved synthetic query field name for record primary keys.
const Key = "$key"

// Options configures collection and how it works with a bbolt database.
//
// Zero-valued option fields use defaults.
type Options struct {
	// Index overrides struct rbi tags when non-nil.
	//
	// Keys may be Go field names or db tag values. A nil map means indexes are
	// declared by rbi tags. A non-nil empty map disables all indexed fields.
	Index map[string]IndexKind

	// EnableStringKeyIndex enables synthetic string primary-key index.
	// When set on Collection[string,V], queries can reference "$key".
	//
	// This can be the only queryable field and still enables indexed mode.
	//
	// It affects only Collection[string,V] and ignored by numeric-key
	// collections (as they expose "$key" automatically in indexed mode).
	//
	// It is disabled by default to minimize memory usage.
	EnableStringKeyIndex bool

	// DisableIndexLoad prevents collection from loading previously persisted
	// index data from the .rbi file on startup.
	// If set, indexer rebuilds the index from the underlying bucket.
	DisableIndexLoad bool

	// DisableIndexStore prevents collection from saving the index state
	// to the .rbi file on Close.
	DisableIndexStore bool

	// PersistedIndexPath overrides the .rbi file path used for persisted index
	// load/store. If empty, the path is derived from the absolute bbolt path
	// and bucket name. Relative paths are resolved during Open.
	PersistedIndexPath string

	// Logger receives informational messages.
	//
	// Default: standard logger from log package.
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

	// BatchSoftLimit is the max number of write operations per Bolt write tx.
	//
	// One logical Tx is never split, a Tx with more operations still commits
	// as one unit. For writes touching multiple collections, the smallest
	// collection limit is used.
	//
	// Negative value forces the write scheduler to process one request per
	// Bolt transaction.
	//
	// Default: 64
	//
	// Typical range: 4..1024
	//
	// Very high values can create commit-size spikes and tail-latency variance.
	BatchSoftLimit int

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
	// Values too high on churn-heavy workloads can increase write latency.
	BucketFillPercent float64

	// MaterializedPredicateCacheMaxEntries controls max number of cached
	// materialized predicate bitmaps per published snapshot.
	//
	// Negative value disables cache.
	//
	// Default: 32
	//
	// Typical range: 16..256
	//
	// High values on diverse workloads can cause memory growth.
	MaterializedPredicateCacheMaxEntries int

	// MaterializedPredicateCacheMaxCardinality skips caching very large
	// materialized postings to reduce retained heap.
	//
	// Negative value disables the guard.
	//
	// Default: 128K
	//
	// Negative (disabled) or very large values can increase memory usage.
	MaterializedPredicateCacheMaxCardinality int

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

	// NumericRangeSpanCacheMaxEntries controls the number of snapshot-local
	// cached full-bucket numeric range spans.
	//
	// Negative value disables full-span admission/storage.
	//
	// Default: 128
	NumericRangeSpanCacheMaxEntries int

	// NumericRangeSpanCacheMaxEntryBytes rejects cached full-bucket numeric
	// range spans larger than this size.
	//
	// Negative value disables full-span admission/storage.
	//
	// Default: 16MiB
	NumericRangeSpanCacheMaxEntryBytes int64

	// NumericRangeExactCacheMaxEntries controls the number of snapshot-local
	// cached exact numeric range results promoted after repeated use.
	//
	// Negative value disables exact-result admission/storage.
	//
	// Default: 32
	NumericRangeExactCacheMaxEntries int

	// NumericRangeExactCacheMaxEntryBytes rejects cached exact numeric range
	// results larger than this size.
	//
	// Negative value disables exact-result admission/storage.
	//
	// Default: 16MiB
	NumericRangeExactCacheMaxEntryBytes int64
}

const (
	defaultOptionsAnalyzeInterval                         = time.Hour
	defaultBucketFillPercent                              = 0.8
	defaultMaterializedPredicateCacheMaxEntries           = 32
	defaultMaterializedPredicateCacheMaxCardinality       = 128 << 10
	defaultBatchSoftLimit                                 = 64
	defaultNumericRangeBucketSize                         = 512
	defaultNumericRangeBucketMinFieldKeys                 = 8192
	defaultNumericRangeBucketMinSpanKeys                  = 1024
	defaultNumericRangeSpanCacheMaxEntries                = 128
	defaultNumericRangeSpanCacheMaxEntryBytes       int64 = 16 << 20
	defaultNumericRangeExactCacheMaxEntries               = 32
	defaultNumericRangeExactCacheMaxEntryBytes      int64 = 16 << 20
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
	if o.MaterializedPredicateCacheMaxEntries == 0 {
		o.MaterializedPredicateCacheMaxEntries = defaultMaterializedPredicateCacheMaxEntries
	}
	if o.MaterializedPredicateCacheMaxCardinality == 0 {
		o.MaterializedPredicateCacheMaxCardinality = defaultMaterializedPredicateCacheMaxCardinality
	}
	if o.BatchSoftLimit == 0 {
		o.BatchSoftLimit = defaultBatchSoftLimit
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
	if o.NumericRangeSpanCacheMaxEntries == 0 {
		o.NumericRangeSpanCacheMaxEntries = defaultNumericRangeSpanCacheMaxEntries
	}
	if o.NumericRangeSpanCacheMaxEntryBytes == 0 {
		o.NumericRangeSpanCacheMaxEntryBytes = defaultNumericRangeSpanCacheMaxEntryBytes
	}
	if o.NumericRangeExactCacheMaxEntries == 0 {
		o.NumericRangeExactCacheMaxEntries = defaultNumericRangeExactCacheMaxEntries
	}
	if o.NumericRangeExactCacheMaxEntryBytes == 0 {
		o.NumericRangeExactCacheMaxEntryBytes = defaultNumericRangeExactCacheMaxEntryBytes
	}
}

func (o Options) validate() error {
	if math.IsNaN(o.BucketFillPercent) || o.BucketFillPercent < 0 || o.BucketFillPercent > 1 {
		return fmt.Errorf("invalid BucketFillPercent: %v", o.BucketFillPercent)
	}
	return nil
}

// Field represents a single field assignment used by Patch.
type Field struct {
	// Name is the logical name of the field to patch.
	// It can be a struct field name, a "db" tag value, or a "json" tag value.
	Name string
	// Value is converted to the matched field type.
	Value any
}

const (
	stringMapBucketSuffix = ".rbimap"
)

// Collection maintains secondary indexes over values of type *V
// stored in a single bbolt bucket.
//
// Collection is safe for concurrent use.
type Collection[K ~string | ~uint64, V any] struct {
	*collection

	vtype reflect.Type

	execOptions execOptions[K, V]

	recPool pooled.Pointers[V]

	stats rbistats.Collection[K]
}

// Open opens or creates a Collection that uses the provided bbolt database.
//
// The generic type V must be a struct; otherwise rbierrors.ErrNotStructType is
// returned. Only exported fields are part of the schema, and unsupported
// exported field types make Open return an error.
//
// Zero-valued option fields use defaults.
//
// If options.BucketName is empty,
// the name of the value type V is used as the bucket name.
// Open ensures the bucket exists, optionally loads a persisted index from disk,
// builds missing or incompatible index data from bbolt, and sets up field
// metadata and accessors.
//
// Any ExecOptions passed to Open become defaults applied to all write
// operations on the returned Collection.
//
// The resulting Collection does not manage the underlying *bbolt.DB lifecycle.
func Open[K ~uint64 | ~string, V any](bolt *bbolt.DB, options Options, execOpts ...ExecOption[K, V]) (c *Collection[K, V], err error) {
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
	for _, opt := range execOpts {
		if opt != nil {
			opt(&defaultExecOptions)
		}
	}
	defaultExecOptions = freezeExecOptions(defaultExecOptions)
	boltPath, err := filepath.Abs(bolt.Path())
	if err != nil {
		return nil, fmt.Errorf("error getting absolute file path: %w", err)
	}
	rbiFile := boltPath + "." + vname + ".rbi"
	if options.PersistedIndexPath != "" {
		rbiFile, err = filepath.Abs(options.PersistedIndexPath)
		if err != nil {
			return nil, fmt.Errorf("error getting absolute persisted index path: %w", err)
		}
		if rbiFile == boltPath {
			return nil, fmt.Errorf("PersistedIndexPath cannot match Bolt database path: %q", rbiFile)
		}
	}

	root, part, err := reserveCollection(bolt, boltPath, vname, options.Logger)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			unreserveCollection(part)
		}
	}()

	c = &Collection[K, V]{
		collection: part,
		vtype:      vtype,

		execOptions: defaultExecOptions,
	}
	part.options = &options
	part.boltPath = boltPath
	part.rbiFile = rbiFile

	var k K
	if reflect.TypeOf(k).Kind() == reflect.String {
		part.strKey = true
		part.strmapBucket = append(slices.Clone(part.dataBucket), stringMapBucketSuffix...)
	}
	strKeyIndex := part.strKey && options.EnableStringKeyIndex

	var schemaIndex map[string]schema.IndexKind
	if options.Index != nil {
		schemaIndex = make(map[string]schema.IndexKind, len(options.Index))
		for name, kind := range options.Index {
			schemaIndex[name] = schema.IndexKind(kind)
		}
	}
	c.schema, err = schema.Compile(vtype, schema.Config{Index: schemaIndex})
	if err != nil {
		return nil, err
	}

	idx, err := engine.NewIndex(engine.Config{
		Schema:      c.schema,
		StrKey:      c.strKey,
		StrKeyIndex: strKeyIndex,

		MaterializedPredCacheMaxEntries:     c.options.MaterializedPredicateCacheMaxEntries,
		MaterializedPredCacheMaxCardinality: c.options.MaterializedPredicateCacheMaxCardinality,
		RuntimeCachesDirtyOwner:             &root.registry.runtimeCachesDirty,
		RuntimeCachesRetireEpoch:            &root.registry.runtimeEpoch.issued,

		NumericRangeBucketSize:              c.options.NumericRangeBucketSize,
		NumericRangeBucketMinFieldKeys:      c.options.NumericRangeBucketMinFieldKeys,
		NumericRangeBucketMinSpanKeys:       c.options.NumericRangeBucketMinSpanKeys,
		NumericRangeSpanCacheMaxEntries:     c.options.NumericRangeSpanCacheMaxEntries,
		NumericRangeSpanCacheMaxEntryBytes:  c.options.NumericRangeSpanCacheMaxEntryBytes,
		NumericRangeExactCacheMaxEntries:    c.options.NumericRangeExactCacheMaxEntries,
		NumericRangeExactCacheMaxEntryBytes: c.options.NumericRangeExactCacheMaxEntryBytes,

		AnalyzeInterval: plannerAnalyzeInterval(c.options.AnalyzeInterval),

		TraceSink:        c.options.TraceSink,
		TraceSampleEvery: c.options.TraceSampleEvery,
	})
	if err != nil {
		return nil, err
	}

	root.mu.Lock()
	c.index = idx
	root.mu.Unlock()

	startupIndexOwned := false
	if c.index != nil {
		startupIndexOwned = true
		startupIndex := c.index
		defer func() {
			if err != nil && startupIndexOwned {
				startupIndex.ReleaseUnpublishedSnapshot()
			}
		}()
	}

	c.stats.Indexed = c.index != nil
	c.stats.StringKeys = c.strKey
	c.stats.IndexFieldCount = len(c.schema.Fields) + len(c.schema.MeasureFields)
	c.stats.MeasureFieldCount = len(c.schema.MeasureFields)
	c.stats.UniqueFieldCount = len(c.schema.Unique)

	err = root.bolt.Update(func(tx *bbolt.Tx) error {
		dataExists := tx.Bucket(c.dataBucket) != nil
		if _, e := tx.CreateBucketIfNotExists(c.dataBucket); e != nil {
			return e
		}
		rbiMeta, e := tx.CreateBucketIfNotExists(root.metaBucket)
		if e != nil {
			return e
		}
		uidKey := rootCollectionUIDKey(c.dataBucket)
		id := rbiMeta.Get(uidKey)
		if id == nil || !dataExists {
			if _, e = rand.Read(c.rbiUID[:]); e != nil {
				return fmt.Errorf("generate rbi uid: %w", e)
			}
			if e = rbiMeta.Put(uidKey, c.rbiUID[:]); e != nil {
				return fmt.Errorf("store rbi uid: %w", e)
			}
		} else {
			if len(id) != len(c.rbiUID) {
				return fmt.Errorf("invalid rbi uid length: %d", len(id))
			}
			copy(c.rbiUID[:], id)
		}
		if c.strKey {
			_, e = createStrMapBucket(tx, c.dataBucket)
			return e
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if err = root.ensureRegistryCurrent(); err != nil {
		return nil, err
	}

	var (
		loadedPlannerStats       *rbistats.PlannerSnapshot
		loadedFieldCount         int
		loadedOrdinaryFieldCount int
		buildMode                string
	)
	if c.index != nil {
		var (
			skipFields        map[string]struct{}
			skipMeasureFields map[string]struct{}
			rebuildReason     string
		)
		if _, err = os.Stat(c.rbiFile); err == nil {
			if options.DisableIndexLoad {
				rebuildReason = "persisted index load disabled"
			} else {
				skipFields, skipMeasureFields, loadedPlannerStats, err = c.loadIndex()
				if err != nil {
					rebuildReason = fmt.Sprintf("persisted index unavailable (%v)", err)
				}
			}
		} else if os.IsNotExist(err) {
			rebuildReason = fmt.Sprintf("persisted index missing (file=%q)", c.rbiFile)
		} else if !os.IsNotExist(err) {
			rebuildReason = fmt.Sprintf("persisted index stat failed (%v)", err)
		}

		loadedOrdinaryFieldCount = len(skipFields)
		loadedFieldCount = loadedOrdinaryFieldCount + len(skipMeasureFields)
		totalFieldCount := len(c.schema.Fields) + len(c.schema.MeasureFields)

		if rebuildReason != "" {
			buildMode = "full"
			c.logger.Printf("rbi: %s", rebuildReason)
			c.logger.Printf(
				"rbi: rebuilding index from bbolt (mode=full loaded_fields=%d/%d)",
				loadedFieldCount,
				totalFieldCount,
			)

		} else if totalFieldCount > 0 {
			if loadedFieldCount == 0 {
				buildMode = "full"
				c.logger.Printf("rbi: persisted index has no compatible field indexes (file=%q)", c.rbiFile)
				c.logger.Printf("rbi: rebuilding index from bbolt (mode=full loaded_fields=0/%d)", totalFieldCount)
			} else if loadedFieldCount < totalFieldCount {
				buildMode = "partial"
				c.logger.Printf(
					"rbi: partially rebuilding index from bbolt (loaded_fields=%d/%d missing_fields=%d)",
					loadedFieldCount,
					totalFieldCount,
					totalFieldCount-loadedFieldCount,
				)
			}
		}

		buildStarted := time.Now()
		if err = c.buildIndex(skipFields, skipMeasureFields); err != nil {
			return nil, fmt.Errorf("error building index: %w", err)
		}
		if buildMode != "" {
			c.logger.Printf("rbi: index build completed (mode=%s duration=%s)", buildMode, time.Since(buildStarted))
		}
	}

	c.initWriteExecutor()

	if c.index != nil {
		if err = c.index.InstallCurrentSnapshot(root.bolt, c.dataBucket); err != nil {
			return nil, fmt.Errorf("failed to publish initial snapshot: %w", err)
		}

		if loadedPlannerStats != nil && loadedOrdinaryFieldCount == len(c.schema.Fields) {
			c.index.PublishLoadedPlannerStats(loadedPlannerStats)

		} else {
			if err = c.index.RefreshPlannerStatsOnSnapshot(c.index.CurrentSnapshot(), c.unavailableErr); err != nil {
				return nil, fmt.Errorf("failed to build planner stats snapshot: %w", err)
			}
		}
	}

	rootOwnsReadState, publishErr := publishCollectionRegistration(root, c.collection)
	if rootOwnsReadState {
		startupIndexOwned = false
	}
	if publishErr != nil {
		err = publishErr
		return nil, err
	}

	c.root.mu.Lock()
	broken := c.root.broken.Load()
	if !broken {
		if c.index != nil {
			c.configureAsyncMaterializedPredSnapshots()
			c.startPlannerAnalyzeLoop()
		}
	}
	c.root.mu.Unlock()

	if broken {
		publishCollectionClose(c.collection)
		return nil, rbierrors.ErrBroken
	}

	return c, nil
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

func rootCollectionUIDKey(dataBucket []byte) []byte {
	key := make([]byte, 1+binary.MaxVarintLen64+len(dataBucket))
	key[0] = rootCollectionUIDKeyKind
	n := binary.PutUvarint(key[1:], uint64(len(dataBucket)))
	key = key[:1+n+len(dataBucket)]
	copy(key[1+n:], dataBucket)
	return key
}

func (c *Collection[K, V]) initWriteExecutor() {
	c.root.scheduler.configure(c.options.BatchSoftLimit)

	ops := wexec.RecordOps{
		Encode: func(ptr unsafe.Pointer, buf *bytes.Buffer) {
			c.encode((*V)(ptr), buf)
		},
		Decode: func(data []byte) (unsafe.Pointer, error) {
			val, err := c.decode(data)
			if err != nil {
				return nil, err
			}
			return unsafe.Pointer(val), nil
		},
		Acquire: func() unsafe.Pointer {
			return unsafe.Pointer(c.recPool.Get())
		},
		CloneInto: func(src unsafe.Pointer, dst unsafe.Pointer) {
			c.schema.Clone.CloneInto(src, dst)
		},
		Release: func(ptr unsafe.Pointer) {
			c.ReleaseRecords((*V)(ptr))
		},
		ValidateIndex: func(ptr unsafe.Pointer) error {
			return c.validateIndexedStringValues(ptr)
		},
	}

	cfg := wexec.Config{
		StatsEnabled:      c.root.statsEnabled,
		DataBucket:        c.dataBucket,
		StrMapBucket:      c.strmapBucket,
		BucketFillPercent: c.options.BucketFillPercent,
		StrKey:            c.strKey,
		Indexed:           c.index != nil,
		Ops:               &ops,
		Schema:            c.schema,
	}

	if c.index != nil {
		c.index.ConfigureWrite(&cfg)
	}
	executor := wexec.NewExecutor(cfg)

	c.root.mu.Lock()
	c.executor = executor
	c.root.mu.Unlock()
}

func (c *Collection[K, V]) configureAsyncMaterializedPredSnapshots() {
	c.index.ConfigureAsyncMaterializedPredSnapshotOps(qexec.AsyncMaterializedPredSnapshotOps{
		CurrentSeq: c.currentSnapshotSeq,
		PinCurrentBySeq: func(seq uint64) (*snapshot.View, qexec.AsyncSnapshotPin, bool) {
			snap, pin, ok := c.pinCurrentReadStateBySnapshotSeq(seq)
			if !ok {
				return nil, nil, false
			}
			return snap, pin, true
		},
	})
}

func (c *Collection[K, V]) currentSnapshotSeq() uint64 {
	return c.collection.currentSnapshotSeq()
}

func (c *Collection[K, V]) buildIndex(skipFields map[string]struct{}, skipMeasureFields map[string]struct{}) error {
	result, err := c.index.BuildIndex(
		c.root.bolt,
		c.dataBucket,
		c.strmapBucket,
		skipFields,
		skipMeasureFields,
		c.decodeBuildIndexRecord,
		c.releaseBuildIndexRecord,
	)
	if err != nil {
		return err
	}
	if result.Stats {
		c.stats.BuildTime = result.BuildTime
		c.stats.BuildRPS = result.BuildRPS
	}
	return nil
}

func (c *Collection[K, V]) decodeBuildIndexRecord(data []byte) (unsafe.Pointer, error) {
	val, err := c.decode(data)
	if err != nil {
		return nil, err
	}
	return unsafe.Pointer(val), nil
}

func (c *Collection[K, V]) releaseBuildIndexRecord(ptr unsafe.Pointer) {
	var zero V
	val := (*V)(ptr)
	*val = zero
	c.recPool.Put(val)
}

func (c *Collection[K, V]) validateIndexedStringValues(ptr unsafe.Pointer) error {
	if c.index == nil || ptr == nil {
		return nil
	}
	return c.index.ValidateStringValues(ptr)
}

func (c *Collection[K, V]) loadIndex() (
	skipFields map[string]struct{},
	skipMeasureFields map[string]struct{},
	plannerStats *rbistats.PlannerSnapshot,
	err error,
) {
	currentSeq, err := currentBucketSequence(c.root.bolt, c.dataBucket)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("decode: reading current bucket sequence: %w", err)
	}
	start := time.Now()
	result, err := c.index.LoadIndex(c.rbiFile, c.boltPath, c.dataBucket, currentSeq, c.rbiUID)
	if err != nil {
		return nil, nil, nil, err
	}
	c.stats.LoadTime = time.Since(start)

	return result.SkipFields, result.SkipMeasureFields, result.PlannerStats, nil
}

func (c *Collection[K, V]) storeIndex() error {
	tx := BeginIndexView()
	defer tx.Release()

	snap, err := tx.closedCollectionSnapshot(c.collection)
	if err != nil {
		return err
	}
	return c.index.StoreIndexSnapshot(c.rbiFile, snap.Seq, c.rbiUID, snap)
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

// Stats returns a lightweight collection status snapshot.
//
// In indexed mode it reports startup build/load timings together with the
// current published snapshot cardinality and last key.
//
// In transparent mode no runtime index snapshot is maintained, so fields that
// depend on indexed state remain zero-valued: BuildTime, BuildRPS, LoadTime,
// and KeyCount.
//
// LastKey and SnapshotSequence are populated in both modes.
func (c *Collection[K, V]) Stats() (rbistats.Collection[K], error) {
	if err := c.unavailableErr(); err != nil {
		return rbistats.Collection[K]{}, err
	}

	tx := BeginView()
	defer tx.Release()

	bucket, read, err := tx.collectionBucket(c.collection)
	if err != nil {
		return rbistats.Collection[K]{}, err
	}

	out := c.stats
	var seq uint64

	if c.index != nil {
		if read.snap == nil {
			return rbistats.Collection[K]{}, rbierrors.ErrNoIndex
		}
		seq = read.snap.Seq
		out.KeyCount = read.snap.UniverseCardinality()
	} else {
		seq = read.dataSeq
	}

	key, _ := bucket.Cursor().Last()
	if key != nil {
		if !c.strKey && len(key) != 8 {
			return rbistats.Collection[K]{}, fmt.Errorf("invalid numeric data key length: %d", len(key))
		}
		out.LastKey = keycodec.UserKeyFromDataKey[K](keycodec.DataKeyFromBytes(key, c.strKey), c.strKey)
	}

	out.SnapshotSequence = seq

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
// On large collections IndexStats can be expensive.
//
// In transparent mode it returns a zero-valued IndexStats because no secondary
// indexes, universe bitmap, or string-key runtime mapping are maintained.
//
// If Collection is closed or broken, IndexStats returns a zero value.
func (c *Collection[K, V]) IndexStats() rbistats.Index {
	if err := c.collection.unavailableErr(); err != nil {
		return rbistats.Index{}
	}

	if c.index == nil {
		return rbistats.Index{}
	}

	tx := BeginIndexView()
	defer tx.Release()
	snap, err := tx.collectionSnapshot(c.collection)
	if err != nil {
		return rbistats.Index{}
	}

	return c.index.IndexStatsOnSnapshot(snap)
}

// StoreStats returns store generation diagnostics if store stats were enabled
// before it was opened. See EnableStoreStats.
func (c *Collection[K, V]) StoreStats() rbistats.Store {
	if c.state.Load()&collectionClosed != 0 {
		return rbistats.Store{}
	}
	r := c.root
	if !r.statsEnabled {
		return rbistats.Store{}
	}

	var out rbistats.Store
	r.mu.Lock()
	out.OpenCollections = len(r.collections)
	out.CollectionHighWater = r.collectionHighWater
	executors := make([]*wexec.Executor, 0, len(r.collections))
	for _, p := range r.collections {
		if p.executor != nil {
			executors = append(executors, p.executor)
		}
	}
	r.mu.Unlock()

	st := r.scheduler.snapshot()
	out.QueueLen = st.QueueLen
	out.QueueCap = st.QueueCap
	out.WorkerRunning = st.WorkerRunning
	out.QueueHighWater = st.QueueHighWater
	out.LogicalUnitsSubmitted = st.Submitted
	out.LogicalUnitsEnqueued = st.Enqueued
	out.LogicalUnitsDequeued = st.Dequeued
	out.ExecutedBatches = st.ExecutedBatches
	out.MultiUnitBatches = st.MultiUnitBatches
	out.MultiUnitOps = st.MultiUnitOps
	out.BatchSize1 = st.BatchSize1
	out.BatchSize2To4 = st.BatchSize2To4
	out.BatchSize5To8 = st.BatchSize5To8
	out.BatchSize9Plus = st.BatchSize9Plus
	out.AvgBatchSize = st.AvgBatchSize
	out.MaxBatchSeen = st.MaxBatchSeen
	out.RejectedClosed = st.RejectedClosed
	out.TxBeginErrors = st.TxBeginErrors
	out.TxOpErrors = st.TxOpErrors
	out.TxCommitErrors = st.TxCommitErrors

	for _, executor := range executors {
		st := executor.ExecutorStats()
		out.CallbackOps += st.CallbackOps
		out.UniqueRejected += st.UniqueRejected
		out.TxOpErrors += st.TxOpErrors
		out.CallbackErrors += st.CallbackErrors
	}
	out.Broken = r.broken.Load()

	rr := &r.registry
	rr.mu.RLock()
	current := rr.current.Load()
	if current == nil {
		out.Reaped = true
	} else {
		out.CurrentEpoch = current.epoch
	}
	out.RegistrySize = len(rr.byEpoch)
	out.StagedGenerations = len(rr.staged)
	for epoch, ref := range rr.byEpoch {
		if ref.refs.Load() != 0 {
			out.PinnedRefs++
			if out.OldestPinnedEpoch == 0 || epoch < out.OldestPinnedEpoch {
				out.OldestPinnedEpoch = epoch
			}
		}
		out.RetiredGenerations += len(ref.retired)
	}
	rr.mu.RUnlock()
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
func (c *Collection[K, V]) PlannerStats() rbistats.Planner {
	if c.index == nil {
		return rbistats.Planner{}
	}
	return c.index.PlannerStats()
}

// Close closes the indexer and persists the current index state
// to the .rbi file unless index persistence is disabled.
//
// After Close, methods that need to bind a new operation to this Collection return
// rbierrors.ErrClosed.
// Subsequent calls to Close are no-op.
//
// Close waits for explicit write Tx values that already touched this Collection until
// user code commits, rolls back, or closes those transactions.
//
// Close does not wait for open read or index Tx values. Read and index Tx
// values that already pinned a root generation continue to read that pinned
// state until Tx.Close. A read Tx that opened a bbolt read transaction still
// owns it; if the caller closes the underlying *bbolt.DB while such a Tx is
// open, bbolt.Close waits for the transaction to close.
//
// It does not close the underlying *bbolt.DB.
func (c *Collection[K, V]) Close() error {
	if !c.collection.beginClose() {
		return nil
	}
	defer unreserveCollection(c.collection)

	c.stopAnalyzeLoop()

	c.root.scheduler.wakeWaiters()

	var err error

	if c.index != nil && !c.options.DisableIndexStore && !c.root.broken.Load() {
		err = c.storeIndex()
	}

	if err == nil && c.root.broken.Load() {
		err = rbierrors.ErrBroken
	}

	publishCollectionClose(c.collection)

	return err
}

// Truncate deletes all values stored in the collection. This cannot be undone.
//
// Do not call Truncate within a transaction, as this will cause a deadlock.
//
// Truncate does not reclaim disk space.
func (c *Collection[K, V]) Truncate() error {
	if err := c.collection.retain(); err != nil {
		return err
	}
	unit := writeUnit{root: c.root, truncate: c.collection}
	err := c.root.scheduler.submit(&unit)
	c.collection.releaseRetain()
	return err
}

// ReleaseRecords returns records to the record pool.
//
// The caller transfers ownership of each passed pointer back to Collection.
// Passed records must not be used after this call.
// There is no internal protection against double-release.
// If unsure about record ownership or lifecycle, do not use this method.
func (c *Collection[K, V]) ReleaseRecords(v ...*V) {
	var zero V
	for _, rec := range v {
		if rec != nil {
			*rec = zero
			c.recPool.Put(rec)
		}
	}
}

// BucketName returns a name of the bucket at which the data is stored.
func (c *Collection[K, V]) BucketName() []byte {
	return slices.Clone(c.dataBucket)
}

func (c *Collection[K, V]) decode(b []byte) (*V, error) {
	v := c.recPool.Get()
	if err := c.schema.Codec.Decode(b, unsafe.Pointer(v)); err != nil {
		c.ReleaseRecords(v)
		return nil, err
	}
	return v, nil
}

func (c *Collection[K, V]) encode(v *V, b *bytes.Buffer) {
	c.schema.Codec.Encode(unsafe.Pointer(v), b)
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
