package rbi

import (
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"go.etcd.io/bbolt"
)

var (
	ErrNotStructType   = errors.New("value is not a struct")
	ErrClosed          = errors.New("database closed")
	ErrInvalidQuery    = errors.New("invalid query")
	ErrIndexDisabled   = errors.New("index is disabled")
	ErrUniqueViolation = errors.New("unique constraint violation")
	ErrRecordNotFound  = errors.New("record not found")
	ErrNoValidKeyIndex = errors.New("no valid key for index")
)

var BucketFillPercent = 0.8

// Options configures how indexer works a bbolt database.
// All fields are optional; a nil *Options is equivalent to using zero values.
type Options[K ~string | ~uint64, V any] struct {

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
	// By default, the bucket name is derived from the name of the value type V.
	BucketName string

	// AutoClose controls whether the indexer should close the underlying Bolt
	// database when DB.Close is called. Open sets this to true automatically;
	// Wrap leaves it as provided by the caller.
	AutoClose bool

	// BoltOptions are forwarded to bbolt.Open when using Open. They are
	// ignored by Wrap, which operates on an already opened *bbolt.DB.
	BoltOptions *bbolt.Options

	// AnalyzeInterval configures how often planner statistics should be
	// refreshed in the background. A zero value means "use default",
	// while a negative value disables periodic refresh.
	AnalyzeInterval time.Duration

	// TraceSink receives optional per-query planner tracing events.
	// If nil, planner tracing is disabled.
	TraceSink func(TraceEvent)

	// TraceSampleEvery controls trace sampling:
	//   - 0: when sink is set, sample every query (equivalent to 1)
	//   - 1: sample every query
	//   - N>1: sample every Nth query
	TraceSampleEvery uint64

	// CalibrationEnabled enables online self-calibration of planner
	// cost coefficients using sampled query traces.
	CalibrationEnabled bool

	// CalibrationSampleEvery controls calibration sampling:
	//   - 0: use default sampling interval
	//   - 1: calibrate every query
	//   - N>1: calibrate every Nth query
	// The value is ignored when CalibrationEnabled is false.
	CalibrationSampleEvery uint64

	// CalibrationPersistPath enables optional auto load/save of planner
	// calibration state from/to this JSON file.
	CalibrationPersistPath string
}

// PreCommitFunc is a callback invoked inside the write transaction just before
// it is committed.
//
// The callback:
//   - Must not modify oldValue or newValue.
//   - Must not commit or roll back the transaction.
//   - May perform additional reads or writes within the same transaction.
//   - May return an error to abort the operation; in this case the
//     transaction will be rolled back and index state will not be updated.
//
// PreCommitFunc is invoked only for records that exist or are being written.
// Patch/Delete operations skip missing records and do not invoke callbacks for them.
type PreCommitFunc[K ~string | ~uint64, V any] = func(tx *bbolt.Tx, key K, oldValue, newValue *V) error

// Field represents a single field assignment used by Patch and PatchMany.
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

// Wrap creates a new indexed DB that uses the provided bbolt database.
//
// The generic type V must be a struct; otherwise ErrNotStructType is returned.
//
// If options is nil, default options are used. If options.BucketName is empty,
// the name of the value type V is used as the bucket name. Wrap ensures the
// bucket exists, optionally loads a persisted index from disk, rebuilds
// missing parts of the index if allowed, and sets up field metadata and
// accessors.
//
// The returned DB does not own the underlying *bbolt.DB unless options.AutoClose
// is true; in that case DB.Close will also close the bbolt database.
func Wrap[K ~uint64 | ~string, V any](bolt *bbolt.DB, options *Options[K, V]) (*DB[K, V], error) {
	var v V
	vtype := reflect.TypeOf(v)
	if vtype == nil {
		return nil, ErrNotStructType
	}
	if vtype.Kind() != reflect.Struct {
		return nil, ErrNotStructType
	}
	if options == nil {
		options = new(Options[K, V])
	}

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

	if err := regInstance(boltPath, vname); err != nil {
		return nil, err
	}

	db := &DB[K, V]{
		bolt:   bolt,
		vtype:  vtype,
		strmap: newStrMapper(0),
		bucket: []byte(vname),

		fields:   make(map[string]*field),
		getters:  make(map[string]getterFn),
		index:    make(map[string]*[]index),
		lenIndex: make(map[string]*[]index),

		universe: roaring64.NewBitmap(),

		rbiFile: bolt.Path() + "." + sanitizeSuffix(vname) + ".rbi",
		opnFile: bolt.Path() + "." + sanitizeSuffix(vname) + ".rbo",

		pool: sync.Pool{
			New: func() any {
				return new(V)
			},
		},

		noSave: options.DisableIndexStore,

		autoclose: options.AutoClose,

		planner: planner{
			analyzer: analyzer{
				interval:   resolvePlannerAnalyzeInterval(options.AnalyzeInterval),
				softBudget: defaultAnalyzeSoftBudget,
			},
			tracer: tracer{
				sink:        options.TraceSink,
				sampleEvery: resolveTraceSampleEvery(options.TraceSampleEvery, options.TraceSink),
			},
			calibrator: calibrator{
				enabled:     options.CalibrationEnabled,
				sampleEvery: resolveCalibrationSampleEvery(options.CalibrationEnabled, options.CalibrationSampleEvery),
				persistPath: options.CalibrationPersistPath,
			},
		},
	}

	var k K
	if reflect.TypeOf(k).Kind() == reflect.String {
		db.strkey = true
	}
	var err error

	if err = db.populateFields(vtype, nil); err != nil {
		return nil, fmt.Errorf("failed to populate index fields: %w", err)
	}

	for _, f := range db.fields {
		if db.getters[f.DBName], err = db.makeGetter(f); err != nil {
			unregInstance(boltPath, vname)
			return nil, fmt.Errorf("failed to create accessor func for %v: %w", f.Name, err)
		}
	}

	err = bolt.Update(func(tx *bbolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists(db.bucket)
		return e
	})
	if err != nil {
		_ = bolt.Close()
		unregInstance(boltPath, vname)
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
					log.Println("rbi: failed to load index:", err)
				}
			}
		}
	}

	if !options.DisableIndexRebuild {
		if err = db.buildIndex(skipFields); err != nil {
			_ = bolt.Close()
			unregInstance(boltPath, vname)
			return nil, fmt.Errorf("error building index: %w", err)
		}
	}

	db.patchMap = make(map[string]*field)
	if err = db.populatePatcher(vtype, nil); err != nil {
		_ = bolt.Close()
		unregInstance(boltPath, vname)
		return nil, fmt.Errorf("failed to populate patch fields: %w", err)
	}

	for name := range db.fields {
		db.fieldSlice = append(db.fieldSlice, name)
	}

	if err = db.RefreshPlannerStats(); err != nil {
		_ = bolt.Close()
		unregInstance(boltPath, vname)
		return nil, fmt.Errorf("failed to build planner stats snapshot: %w", err)
	}

	db.initCalibration()

	if err = touch(db.opnFile); err != nil {
		_ = bolt.Close()
		unregInstance(boltPath, vname)
		return nil, fmt.Errorf("error creating flag-file: %w", err)
	}

	db.startPlannerAnalyzeLoop()

	return db, nil
}

// Open opens or creates a bbolt database at the given filename with the
// provided file mode, and wraps it with an indexer.
//
// It is equivalent to calling bbolt.Open and then Wrap with AutoClose enabled.
// If options is nil, a default Options value is used. Open returns ErrNotStructType
// if V is not a struct or any error returned by bbolt.Open or Wrap.
func Open[K ~uint64 | ~string, V any](filename string, mode os.FileMode, options *Options[K, V]) (*DB[K, V], error) {

	var v V
	t := reflect.TypeOf(v)
	if t == nil {
		return nil, ErrNotStructType
	}
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, ErrNotStructType
	}
	if options == nil {
		options = new(Options[K, V])
	}

	bolt, err := bbolt.Open(filename, mode, options.BoltOptions)
	if err != nil {
		return nil, err
	}

	options.AutoClose = true

	db, err := Wrap(bolt, options)
	if err != nil {
		_ = bolt.Close()
	}
	return db, err
}

type planner struct {
	statsVersion atomic.Uint64
	stats        atomic.Pointer[PlannerStatsSnapshot]

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

		strkey   bool
		strmap   *strMapper
		universe *roaring64.Bitmap

		fields     map[string]*field
		fieldSlice []string
		hasUnique  bool

		getters  map[string]getterFn
		index    map[string]*[]index
		lenIndex map[string]*[]index

		patchMap map[string]*field

		rbiFile string
		opnFile string

		pool sync.Pool

		mu     sync.RWMutex
		closed atomic.Bool

		noIndex atomic.Bool
		noSave  bool

		autoclose bool

		stats Stats[K]

		planner planner
	}
	index struct {
		Key string
		IDs *roaring64.Bitmap
	}

	// Stats contains some metrics about the current index and key space.
	Stats[K ~uint64 | ~string] struct {
		// IndexBuildTime is the duration of the last index rebuild.
		IndexBuildTime time.Duration
		// IndexBuildRPS is an approximate throughput (records per second) of the last index rebuild.
		IndexBuildRPS int
		// IndexLoadTime is the time spent loading a persisted index from disk on the last successful load.
		IndexLoadTime time.Duration
		// UniqueFieldKeys contains the number of unique index keys per indexed field name.
		UniqueFieldKeys map[string]uint64
		// IndexSize contains the total size of the index, in bytes.
		IndexSize uint64
		// IndexFieldSize contains the size of the index for each indexed field.
		IndexFieldSize map[string]uint64
		// LastKey is the largest key present in the database according to the
		// current universe bitmap. For string keys this is derived from the
		// internal string mapping.
		LastKey K
		// KeyCount is the total number of keys currently present in the database.
		KeyCount uint64
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
//   - QueryItems, QueryKeys and Count will return an error, because the index is considered invalid.
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

// RebuildIndex discards and rebuilds all in-memory index data.
// It acquires an exclusive lock for its duration.
// While it is safe to call at any time, it might be expensive for large datasets.
func (db *DB[K, V]) RebuildIndex() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.buildIndex(nil); err != nil {
		return err
	}

	db.refreshPlannerStatsLocked()
	return nil
}

// Stats returns basic information about the index, load times and key space.
// On large databases Stats can be slow.
func (db *DB[K, V]) Stats() Stats[K] {
	db.mu.RLock()
	defer db.mu.RUnlock()

	s := db.stats

	s.UniqueFieldKeys = make(map[string]uint64)
	s.IndexFieldSize = make(map[string]uint64)

	for name, i := range db.index {
		s.UniqueFieldKeys[name] = uint64(len(*i))
		var size uint64
		for _, bm := range *i {
			size += bm.IDs.GetSizeInBytes()
		}
		s.IndexFieldSize[name] = size
		s.IndexSize += size
	}

	s.LastKey = db.lastIDNoLock()
	s.KeyCount = db.universe.GetCardinality()

	return s
}

// Close closes the indexed DB and optionally the underlying Bolt database.
//
// On the first call, Close:
//   - Persists the current index state to the .rbi file unless index
//     persistence is disabled.
//   - Removes the .rbo flag file used to detect unsafe shutdowns.
//   - Closes the underlying Bolt database if AutoClose was set.
//
// Subsequent calls to Close are no-op.
// After Close, all other methods return ErrClosed.
func (db *DB[K, V]) Close() error {
	if !db.closed.CompareAndSwap(false, true) {
		return nil
	}

	db.stopAnalyzeLoop()

	db.mu.Lock()
	defer db.mu.Unlock()

	unregInstance(db.bolt.Path(), string(db.bucket))

	var err error

	if !db.noSave {
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

	if db.autoclose {
		if e := db.bolt.Close(); e != nil {
			return e // bolt errors are more important
		}
	}

	return err
}

// Truncate deletes all values stored in the database. This cannot be undone.
//
// Truncate does not reclaim disk space.
func (db *DB[K, V]) Truncate() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed.Load() {
		return ErrClosed
	}

	tx, err := db.bolt.Begin(true)
	if err != nil {
		return fmt.Errorf("tx error: %w", err)
	}
	defer rollback(tx)

	if tx.Bucket(db.bucket) != nil {
		if err = tx.DeleteBucket(db.bucket); err != nil {
			return fmt.Errorf("error deleting bucket: %w", err)
		}
	}

	if _, err = tx.CreateBucketIfNotExists(db.bucket); err != nil {
		return fmt.Errorf("error creating bucket: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit error: %w", err)
	}

	db.strmap.truncate()

	// for k := range db.index {
	// 	s := make([]index, 0, initialIndexLen)
	// 	db.index[k] = &s
	// }
	db.index = make(map[string]*[]index)

	// for k := range db.lenIndex {
	// 	s := make([]index, 0, 8)
	// 	db.lenIndex[k] = &s
	// }
	db.lenIndex = make(map[string]*[]index)

	db.universe.Clear()

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
			db.pool.Put(rec)
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

type strMapper struct {
	Next uint64
	Keys map[string]uint64
	Strs map[uint64]string

	sync.RWMutex
}

func newStrMapper(size uint64) *strMapper {
	return &strMapper{
		Keys: make(map[string]uint64, size),
		Strs: make(map[uint64]string, size),
	}
}

func (sm *strMapper) truncate() {
	sm.Lock()
	defer sm.Unlock()
	sm.Next = 0
	sm.Keys = make(map[string]uint64)
	sm.Strs = make(map[uint64]string)
}

func (sm *strMapper) createIdxNoLock(s string) uint64 {
	if v, ok := sm.Keys[s]; ok {
		return v
	}
	sm.Next++
	sm.Keys[s] = sm.Next
	sm.Strs[sm.Next] = s
	return sm.Next
}

func (sm *strMapper) createIdxsNoLock(s []string) []uint64 {
	r := make([]uint64, len(s))
	for i, v := range s {
		r[i] = sm.createIdxNoLock(v)
	}
	return r
}

func (sm *strMapper) getStringNoLock(idx uint64) (string, bool) {
	v, ok := sm.Strs[idx]
	return v, ok
}

func (sm *strMapper) mustGetStringNoLock(idx uint64) string {
	if v, ok := sm.Strs[idx]; ok {
		return v
	}
	panic(fmt.Errorf("no id associated with idx %v", idx))
}
