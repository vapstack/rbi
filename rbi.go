package rbi

import (
	"bytes"
	"errors"
	"fmt"
	"log"
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
	ErrNotStructType   = errors.New("value is not a struct")
	ErrClosed          = errors.New("database closed")
	ErrInvalidQuery    = errors.New("invalid query")
	ErrIndexDisabled   = errors.New("index is disabled")
	ErrUniqueViolation = errors.New("unique constraint violation")
	ErrRecordNotFound  = errors.New("record not found")
)

var BucketFillPercent = 0.75

// Options configures how indexer works a bbolt database.
// All fields are optional; a nil *Options is equivalent to using zero values.
type Options[K ~string | ~uint64, V any] struct {

	// DisableIndexLoad prevents indexer from loading previously persisted index
	// data from the .rbi file on startup. If set, indexer will either rebuild
	// the index from the underlying bucket (unless DisableIndexRebuild is
	// also set) or operate with an empty index until rebuilt.
	DisableIndexLoad bool

	// DisableIndexStore prevents indexer from saving the in-memory index state
	// to the .rbi file on Close. This is useful when index persistence is
	// not required or when the caller prefers to manage index lifecycle
	// manually.
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

		noSave: options.DisableIndexStore,

		autoclose: options.AutoClose,
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
		log.Println("rbi: unsafe shutdown: rebuilding index...")
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

	if err = touch(db.opnFile); err != nil {
		_ = bolt.Close()
		unregInstance(boltPath, vname)
		return nil, fmt.Errorf("error creating flag-file: %w", err)
	}

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

type (
	// DB wraps a bbolt database and maintains secondary indexes over values of type V
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

		mu     sync.RWMutex
		closed atomic.Bool

		noIndex atomic.Bool
		noSave  bool

		autoclose bool

		stats Stats[K]
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
func (db *DB[K, V]) EnableSync() { db.bolt.NoSync = false }

// DisableIndexing disables index updates for subsequent write operations .
//
// When indexing is disabled:
//   - Index structures are no longer kept up to date.
//   - QueryItems, QueryKeys, QueryBitmap and Count will return an error, because the index is considered invalid.
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

	return db.buildIndex(nil)
}

// Stats returns basic information about the index, load times and key space.
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

	db.mu.Lock()
	defer db.mu.Unlock()

	if !db.closed.CompareAndSwap(false, true) {
		return nil
	}

	unregInstance(db.bolt.Path(), string(db.bucket))

	var err error

	if !db.noSave {
		err = db.storeIndex()
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

// Get returns the value stored by id or nil if key was not found.
func (db *DB[K, V]) Get(id K) (*V, error) {

	if db.closed.Load() {
		return nil, ErrClosed
	}

	tx, err := db.bolt.Begin(false)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)

	v := tx.Bucket(db.bucket).Get(db.keyFromID(id))
	if v == nil {
		return nil, nil
	}
	return db.decode(v)
}

// GetMany retrieves multiple values by their IDs in a single read transaction.
// The returned slice has the same length as ids; any missing keys have a nil
// entry at the corresponding index.
func (db *DB[K, V]) GetMany(ids ...K) ([]*V, error) {

	if db.closed.Load() {
		return nil, ErrClosed
	}

	tx, err := db.bolt.Begin(false)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)

	bucket := tx.Bucket(db.bucket)

	s := make([]*V, len(ids))

	for i, id := range ids {
		v := bucket.Get(db.keyFromID(id))
		if v == nil {
			continue
		}
		value, e := db.decode(v)
		if e != nil {
			return s, e
		}
		s[i] = value
	}
	return s, nil
}

// SeqScan performs a sequential scan over all records starting at the given
// key (inclusive), decoding each value and passing it to the provided fn.
// SeqScan stops reading when the fn returns false or a non-nil error.
// The scan runs inside a read-only transaction which remains open for the
// duration of the scan.
func (db *DB[K, V]) SeqScan(seek K, fn func(K, *V) (bool, error)) error {

	if db.closed.Load() {
		return ErrClosed
	}

	tx, err := db.bolt.Begin(false)
	if err != nil {
		return err
	}
	defer rollback(tx)

	b := tx.Bucket(db.bucket)
	c := b.Cursor()

	key, value := c.Seek(db.keyFromID(seek))
	if key == nil {
		return nil
	}
	val, err := db.decode(value)
	if err != nil {
		return err
	}

	more, err := fn(db.idFromKey(key), val)
	if err != nil {
		return err
	}
	for more {
		key, value = c.Next()
		if key == nil {
			return nil
		}
		if val, err = db.decode(value); err != nil {
			return err
		}
		if more, err = fn(db.idFromKey(key), val); err != nil {
			return err
		}
	}
	return nil
}

// SeqScanRaw performs a sequential scan over all records starting at the
// given key (inclusive), passing raw bytes to the provided fn.
// These bytes are msgpack-encoded representation of the values.
//
// SeqScanRaw stops reading when the provided fn returns false or a non-nil error.
// The database transaction remains open during the scan.
//
// Bytes passed to fn must not be modified.
func (db *DB[K, V]) SeqScanRaw(seek K, fn func(K, []byte) (bool, error)) error {

	if db.closed.Load() {
		return ErrClosed
	}

	tx, err := db.bolt.Begin(false)
	if err != nil {
		return err
	}
	defer rollback(tx)

	b := tx.Bucket(db.bucket)
	c := b.Cursor()

	key, value := c.Seek(db.keyFromID(seek))
	if key == nil {
		return nil
	}

	more, err := fn(db.idFromKey(key), value)
	if err != nil {
		return err
	}
	for more {
		key, value = c.Next()
		if key == nil {
			return nil
		}
		if more, err = fn(db.idFromKey(key), value); err != nil {
			return err
		}
	}
	return nil
}

// Set stores the given value under the specified key ID.
//
// The value is msgpack-encoded and written inside a single write
// transaction. Any existing value for the key is decoded and passed as oldValue
// to all PreCommitFunc fns. If any fn returns an error, the
// transaction is rolled back and the error is returned.
func (db *DB[K, V]) Set(id K, newVal *V, fns ...PreCommitFunc[K, V]) error {

	b := getEncodeBuf()
	defer releaseEncodeBuf(b)

	if err := db.encode(newVal, b); err != nil {
		return err
	}

	tx, err := db.bolt.Begin(true)
	if err != nil {
		return err
	}
	defer rollback(tx)

	key := db.keyFromID(id)
	idx := db.idxFromID(id)

	bucket := tx.Bucket(db.bucket)
	if bucket == nil {
		return fmt.Errorf("bucket does not exist")
	}

	var oldVal *V
	if prev := bucket.Get(key); prev != nil {
		if oldVal, err = db.decode(prev); err != nil {
			return err
		}
	}

	bucket.FillPercent = BucketFillPercent

	if err = bucket.Put(key, b.Bytes()); err != nil {
		return err
	}

	for _, fn := range fns {
		if err = fn(tx, id, oldVal, newVal); err != nil {
			return err
		}
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed.Load() {
		return ErrClosed
	}

	modified := db.getModifiedIndexedFields(oldVal, newVal)

	if db.hasUnique {
		if err = db.checkUniqueOnWrite(idx, newVal, modified); err != nil {
			return err
		}
	}

	return db.setIndexOnSuccess(tx.Commit(), idx, oldVal, newVal, modified)
}

// SetMany stores multiple values under the provided IDs in a single write
// transaction. The length of the ids and values must be equal.
//
// For each key, any existing value is decoded and passed as oldValue to all
// PreCommitFunc fns. If an error is encountered during any of the processing steps,
// the transaction is rolled back and the error is returned.
//
// After a successful commit, the in-memory index state is updated for all
// modified keys unless indexing is disabled.
func (db *DB[K, V]) SetMany(ids []K, newVals []*V, fns ...PreCommitFunc[K, V]) error {

	if len(ids) != len(newVals) {
		return fmt.Errorf("different slice lengths")
	}
	if len(ids) == 0 {
		return nil
	}
	if len(ids) == 1 {
		return db.Set(ids[0], newVals[0], fns...)
	}

	bufs := make([]*bytes.Buffer, 0, len(ids))
	defer func() {
		for _, buf := range bufs {
			if buf != nil {
				releaseEncodeBuf(buf)
			}
		}
	}()

	for _, v := range newVals {
		b := getEncodeBuf()
		bufs = append(bufs, b)
		if err := db.encode(v, b); err != nil {
			return err
		}
	}

	tx, txerr := db.bolt.Begin(true)
	if txerr != nil {
		return txerr
	}
	defer rollback(tx)

	oldVals := make([]*V, len(ids))
	idxs := db.idxsFromID(ids)

	bucket := tx.Bucket(db.bucket)
	if bucket == nil {
		return fmt.Errorf("bucket does not exist")
	}
	bucket.FillPercent = BucketFillPercent

	var err error
	for i, id := range ids {

		key := db.keyFromID(id)

		var oldVal *V
		if prev := bucket.Get(key); prev != nil {
			if oldVal, err = db.decode(prev); err != nil {
				return err
			}
		}
		oldVals[i] = oldVal

		if err = bucket.Put(key, bufs[i].Bytes()); err != nil {
			return err
		}
	}

	if len(fns) > 0 {
		for i, id := range ids {
			for _, fn := range fns {
				if err = fn(tx, id, oldVals[i], newVals[i]); err != nil {
					return err
				}
			}
		}
	}

	/**/

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed.Load() {
		return ErrClosed
	}

	modified := make([][]string, len(idxs))
	for i := range idxs {
		modified[i] = db.getModifiedIndexedFields(oldVals[i], newVals[i])
	}

	if db.hasUnique {
		if err = db.checkUniqueOnWriteMulti(idxs, oldVals, newVals, modified); err != nil {
			return err
		}
	}

	return db.setIndexOnSuccessMulti(tx.Commit(), idxs, oldVals, newVals, modified)
}

// Patch applies a partial update to the value stored under the given id,
// updating only the fields listed in patch.
//
// Unknown field names in patch are silently ignored. All fields, indexed or
// not, are eligible to be patched. Patch attempts to convert the provided
// values to the appropriate field type, if possible.
// If conversion fails for any field, Patch returns an error and no changes are committed.
//
// If no item is found for the specified id, ErrRecordNotFound is returned.
//
// All PreCommitFunc fns are invoked with the original (old) and patched (new) values
// before commit. After a successful commit, the in-memory index is updated.
func (db *DB[K, V]) Patch(id K, patch []Field, fns ...PreCommitFunc[K, V]) error {
	return db.patch(id, patch, true, fns...)
}

// PatchStrict is like Patch, but returns an error if the patch contains field names
// that cannot be resolved to a known struct field (by name, db or json tag).
func (db *DB[K, V]) PatchStrict(id K, patch []Field, fns ...PreCommitFunc[K, V]) error {
	return db.patch(id, patch, false, fns...)
}

func (db *DB[K, V]) patch(id K, fields []Field, ignoreUnknown bool, fns ...PreCommitFunc[K, V]) error {

	if len(fields) == 0 {
		return nil
	}

	tx, err := db.bolt.Begin(true)
	if err != nil {
		return err
	}
	defer rollback(tx)

	bucket := tx.Bucket(db.bucket)
	if bucket == nil {
		return fmt.Errorf("bucket does not exist")
	}

	key := db.keyFromID(id)
	idx := db.idxFromID(id)

	var oldVal *V
	oldBytes := bucket.Get(key)
	if oldBytes == nil {
		return ErrRecordNotFound
	}

	if oldVal, err = db.decode(oldBytes); err != nil {
		return fmt.Errorf("failed to decode existing value: %w", err)
	}

	newVal, err := db.decode(oldBytes)
	if err != nil {
		return fmt.Errorf("failed to re-decode value for patching: %w", err)
	}

	if err = db.applyPatch(newVal, fields, ignoreUnknown); err != nil {
		return fmt.Errorf("failed to apply patch: %w", err)
	}

	b := getEncodeBuf()
	defer releaseEncodeBuf(b)

	if err = db.encode(newVal, b); err != nil {
		return err
	}

	bucket.FillPercent = BucketFillPercent

	if err = bucket.Put(key, b.Bytes()); err != nil {
		return err
	}

	for _, fn := range fns {
		if err = fn(tx, id, oldVal, newVal); err != nil {
			return err
		}
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed.Load() {
		return ErrClosed
	}

	modified := db.getModifiedIndexedFields(oldVal, newVal)

	if db.hasUnique {
		if err = db.checkUniqueOnWrite(idx, newVal, modified); err != nil {
			return err
		}
	}

	return db.setIndexOnSuccess(tx.Commit(), idx, oldVal, newVal, modified)
}

// PatchMany applies the same patch to all values stored under the given IDs
// in a single write transaction.
//
// Unknown fields are ignored (as in Patch).
// Any errors during processing aborts the entire batch and rolls back the transaction.
//
// Non-existent IDs are skipped.
func (db *DB[K, V]) PatchMany(ids []K, patch []Field, fns ...PreCommitFunc[K, V]) error {
	return db.patchMany(ids, patch, true, fns...)
}

// PatchManyStrict is like PatchMany, but returns an error if patch contains
// any field names that cannot be resolved to known struct fields.
//
// Non-existent IDs are skipped.
func (db *DB[K, V]) PatchManyStrict(ids []K, patch []Field, fns ...PreCommitFunc[K, V]) error {
	return db.patchMany(ids, patch, false, fns...)
}

func (db *DB[K, V]) patchMany(ids []K, patch []Field, ignoreUnknown bool, fns ...PreCommitFunc[K, V]) error {

	if len(ids) == 0 || len(patch) == 0 {
		return nil
	}
	if len(ids) == 1 {
		return db.patch(ids[0], patch, ignoreUnknown, fns...)
	}

	bufs := make([]*bytes.Buffer, 0, len(ids))
	defer func() {
		for _, buf := range bufs {
			if buf != nil {
				releaseEncodeBuf(buf)
			}
		}
	}()

	tx, txerr := db.bolt.Begin(true)
	if txerr != nil {
		return txerr
	}
	defer rollback(tx)

	oldVals := make([]*V, len(ids))
	newVals := make([]*V, len(ids))

	idxs := db.idxsFromID(ids)

	bucket := tx.Bucket(db.bucket)
	if bucket == nil {
		return fmt.Errorf("bucket does not exist")
	}
	bucket.FillPercent = BucketFillPercent

	var err error
	for i, id := range ids {
		key := db.keyFromID(id)

		oldBytes := bucket.Get(key)
		if oldBytes == nil {
			continue
			// return fmt.Errorf("item with id %v does not exist", id)
		}

		var (
			oldVal *V
			newVal *V
		)
		if oldVal, err = db.decode(oldBytes); err != nil {
			return fmt.Errorf("failed to decode existing value for id %v: %w", id, err)
		}
		oldVals[i] = oldVal

		if newVal, err = db.decode(oldBytes); err != nil {
			return fmt.Errorf("failed to re-decode value for patching id %v: %w", id, err)
		}

		if err = db.applyPatch(newVal, patch, ignoreUnknown); err != nil {
			return fmt.Errorf("failed to apply patch for id %v: %w", id, err)
		}
		newVals[i] = newVal

		b := getEncodeBuf()
		bufs = append(bufs, b)
		if err = db.encode(newVal, b); err != nil {
			return err
		}

		if err = bucket.Put(key, b.Bytes()); err != nil {
			return err
		}
	}

	if len(fns) > 0 {
		for i, id := range ids {
			for _, fn := range fns {
				if err = fn(tx, id, oldVals[i], newVals[i]); err != nil {
					return err
				}
			}
		}
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed.Load() {
		return ErrClosed
	}

	modified := make([][]string, len(idxs))
	for i := range idxs {
		modified[i] = db.getModifiedIndexedFields(oldVals[i], newVals[i])
	}

	if db.hasUnique {
		if err = db.checkUniqueOnWriteMulti(idxs, oldVals, newVals, modified); err != nil {
			return err
		}
	}

	return db.setIndexOnSuccessMulti(tx.Commit(), idxs, oldVals, newVals, modified)
}

// Delete removes the value stored under the given id, if any.
//
// The existing value (if present) is decoded and passed as oldValue to all
// PreCommitFunc fns. If any fn returns an error, the operation is aborted.
func (db *DB[K, V]) Delete(id K, fns ...PreCommitFunc[K, V]) error {

	tx, err := db.bolt.Begin(true)
	if err != nil {
		return err
	}
	defer rollback(tx)

	bucket := tx.Bucket(db.bucket)
	if bucket == nil {
		return fmt.Errorf("bucket does not exist")
	}
	bucket.FillPercent = BucketFillPercent

	key := db.keyFromID(id)
	idx := db.idxFromID(id)

	var oldVal *V
	if prev := bucket.Get(key); prev != nil {
		if oldVal, err = db.decode(prev); err != nil {
			return err
		}
	}

	if err = bucket.Delete(key); err != nil {
		return err
	}

	for _, fn := range fns {
		if err = fn(tx, id, oldVal, nil); err != nil {
			return err
		}
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed.Load() {
		return ErrClosed
	}

	modified := db.getModifiedIndexedFields(oldVal, nil)

	return db.setIndexOnSuccess(tx.Commit(), idx, oldVal, nil, modified)
}

// DeleteMany removes all values stored under the provided ids in a single
// write transaction.
//
// For each key, any existing value is decoded and passed as oldValue to all
// PreCommitFunc fns. If an error is encountered during processing,
// the entire operation is rolled back.
func (db *DB[K, V]) DeleteMany(ids []K, fns ...PreCommitFunc[K, V]) error {

	if len(ids) == 0 {
		return nil
	}
	if len(ids) == 1 {
		return db.Delete(ids[0], fns...)
	}

	tx, txerr := db.bolt.Begin(true)
	if txerr != nil {
		return txerr
	}
	defer rollback(tx)

	oldVals := make([]*V, len(ids))
	idxs := db.idxsFromID(ids)

	bucket := tx.Bucket(db.bucket)
	if bucket == nil {
		return fmt.Errorf("bucket does not exist")
	}
	bucket.FillPercent = BucketFillPercent

	var err error
	for i, id := range ids {

		key := db.keyFromID(id)

		var oldVal *V
		if prev := bucket.Get(key); prev != nil {
			if oldVal, err = db.decode(prev); err != nil {
				return err
			}
		}
		oldVals[i] = oldVal

		if err = bucket.Delete(key); err != nil {
			return err
		}
	}

	if len(fns) > 0 {
		for i, id := range ids {
			for _, fn := range fns {
				if err = fn(tx, id, oldVals[i], nil); err != nil {
					return err
				}
			}
		}
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed.Load() {
		return ErrClosed
	}

	return db.setIndexOnSuccessMulti(tx.Commit(), idxs, oldVals, nil, nil)
}

// ToKey converts an internal bitmap index value into the corresponding user key.
//
// QueryBitmap returns a bitmap containing internal record identifiers.
// When the database uses numeric keys (uint64), these identifiers are the keys
// themselves. When the database uses string keys, the identifiers are internal
// indices and must be translated back to user-facing keys.
//
// ToKey performs this translation. It is primarily intended for advanced use
// cases where the caller iterates over a bitmap returned by QueryBitmap and
// needs to map bitmap entries back to actual record keys.
//
// The second return value reports whether the key was successfully resolved.
// For numeric-key databases this always returns true. For string-key databases
// it may return false if the index value does not correspond to a known key.
func (db *DB[K, V]) ToKey(idx uint64) (K, bool) {
	if db.strkey {
		db.strmap.RLock()
		v, ok := db.strmap.getStringNoLock(idx)
		db.strmap.RUnlock()
		return *(*K)(unsafe.Pointer(&v)), ok
	}
	return *(*K)(unsafe.Pointer(&idx)), true
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

/**/

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
