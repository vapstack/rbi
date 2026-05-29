package rbi

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/strmap"
	"github.com/vapstack/rbi/internal/wexec"
	"go.etcd.io/bbolt"
)

// ExecOption configures execution behavior for a write operation.
//
// Different write methods may honor different options.
type ExecOption[K ~string | ~uint64, V any] func(*execOptions[K, V])

// BeforeProcess registers a lightweight pre-processing callback for Set,
// BatchSet, Patch, and BatchPatch.
//
// BeforeProcess runs wherever RBI first has a mutable value available:
//   - For Set/BatchSet it runs on the caller-owned input value before RBI
//     performs encoding, cloning, queueing, or transactional work.
//   - For Patch/BatchPatch it runs on the mutable post-patch working copy
//     after RBI applies the patch and before BeforeStore/BeforeCommit.
//
// BeforeProcess:
//   - Receives only the key and mutable input value.
//   - May modify the value in place.
//   - May return an error to abort the operation.
//   - Must not retain the value pointer after it returns, because the pointer
//     may refer either to caller-owned input (Set/BatchSet) or to an
//     RBI-owned working copy (Patch/BatchPatch).
//   - For Set/BatchSet, the caller must avoid concurrent mutations of the same
//     value until the write returns.
//   - For Patch/BatchPatch, the hook may be invoked more than once for the
//     same logical operation when the auto-batcher retries a request.
//   - Is ignored by Delete and BatchDelete.
//
// Because BeforeProcess may run on caller-owned values for Set/BatchSet, RBI
// does not protect against aliasing, repeated pointers inside BatchSet, or
// mutations remaining visible to the caller after a later write failure. Use
// BeforeStore when isolation or oldValue access is required.
//
// BeforeProcess may be passed more than once. Hooks passed to New are invoked
// before per-operation hooks.
func BeforeProcess[K ~string | ~uint64, V any](fn func(key K, value *V) error) ExecOption[K, V] {
	if fn == nil {
		return nil
	}

	if reflect.TypeFor[K]().Kind() == reflect.String {
		f := func(key keycodec.DataKey, value unsafe.Pointer) error {
			return fn(keycodec.UserKeyFromDataKey[K](key, true), (*V)(value))
		}
		return func(cfg *execOptions[K, V]) { cfg.beforeProcess = append(cfg.beforeProcess, f) }
	}

	f := func(key keycodec.DataKey, value unsafe.Pointer) error {
		return fn(keycodec.UserKeyFromDataKey[K](key, false), (*V)(value))
	}
	return func(cfg *execOptions[K, V]) { cfg.beforeProcess = append(cfg.beforeProcess, f) }
}

// NoBatch forces a write call to execute in its own internal batch.
//
// For Set, Patch, and Delete it keeps the request from being coalesced with
// neighboring writes.
//
// For BatchSet, BatchPatch, and BatchDelete it is redundant because those
// methods already execute as isolated internal batches.
func NoBatch[K ~string | ~uint64, V any](cfg *execOptions[K, V]) {
	cfg.noBatch = true
}

// BeforeStore registers a callback invoked for every insert or update just
// before RBI starts processing the value for storage and index maintenance.
//
// The callback receives oldValue as the currently stored record (or nil for
// inserts) and newValue as a mutable working copy that will be encoded,
// persisted, passed to BeforeCommit, and used for index updates.
//
// BeforeStore:
//   - May modify newValue.
//   - Must not modify oldValue.
//   - Must not retain oldValue or newValue pointers after it returns.
//   - May be invoked more than once for the same logical write operation;
//     external side effects must therefore be idempotent or avoided.
//
// BeforeStore is invoked only for records that are being inserted or updated.
// Delete operations and missing Patch targets do not invoke it.
//
// For Set/BatchSet, RBI normally snapshots the input value through
// encode/decode before BeforeStore runs so it can safely isolate retries and
// caller-owned objects. If the value only becomes encodable after
// BeforeStore normalizes it, or if the caller can produce an independent copy
// faster than RBI's fallback snapshotting, pass CloneFunc. If CloneFunc is not
// provided and *V implements `Clone() *V`, RBI uses that method automatically.
//
// BeforeStore may be passed more than once. Hooks passed to New are invoked
// before per-operation hooks.
func BeforeStore[K ~string | ~uint64, V any](fn func(key K, oldValue, newValue *V) error) ExecOption[K, V] {
	if fn == nil {
		return nil
	}

	if reflect.TypeFor[K]().Kind() == reflect.String {
		f := func(key keycodec.DataKey, oldValue, newValue unsafe.Pointer) error {
			return fn(keycodec.UserKeyFromDataKey[K](key, true), (*V)(oldValue), (*V)(newValue))
		}
		return func(cfg *execOptions[K, V]) { cfg.beforeStore = append(cfg.beforeStore, f) }
	}

	f := func(key keycodec.DataKey, oldValue, newValue unsafe.Pointer) error {
		return fn(keycodec.UserKeyFromDataKey[K](key, false), (*V)(oldValue), (*V)(newValue))
	}
	return func(cfg *execOptions[K, V]) { cfg.beforeStore = append(cfg.beforeStore, f) }
}

// CloneFunc registers a cloning function used by Set/BatchSet when BeforeStore
// hooks are present.
//
// When provided, RBI uses CloneFunc to create an internal baseline copy before
// BeforeStore runs instead of snapshotting the value through encode/decode.
// This is useful for values that only become encodable after
// BeforeStore normalizes them, and it may also reduce cloning overhead when
// the caller can produce an independent copy faster than RBI's fallback
// snapshotting.
//
// CloneFunc:
//   - Receives the write key and source value.
//   - Must return an independent copy that does not alias the input.
//   - Must not mutate the input.
//   - Must return non-nil for non-nil input.
//   - Must be deterministic and free of external side effects.
//
// If CloneFunc is not provided and *V implements Clone() *V, RBI uses that
// method automatically. Otherwise, RBI falls back to encode/decode snapshotting
// before BeforeStore for Set/BatchSet.
// Patch/BatchPatch ignore CloneFunc because they already rebuild a fresh
// working copy from stored bytes.
//
// CloneFunc may be passed more than once. The last non-nil CloneFunc wins.
// When passed to New, CloneFunc becomes the default for matching write
// operations on the returned DB.
func CloneFunc[K ~string | ~uint64, V any](fn func(key K, v *V) *V) ExecOption[K, V] {
	if fn == nil {
		return nil
	}

	if reflect.TypeFor[K]().Kind() == reflect.String {
		f := func(key keycodec.DataKey, value unsafe.Pointer) (unsafe.Pointer, error) {
			cloned := fn(keycodec.UserKeyFromDataKey[K](key, true), (*V)(value))
			if cloned == nil {
				return nil, errCloneNil
			}
			return unsafe.Pointer(cloned), nil
		}
		return func(cfg *execOptions[K, V]) { cfg.cloneValue = f }
	}

	f := func(key keycodec.DataKey, value unsafe.Pointer) (unsafe.Pointer, error) {
		cloned := fn(keycodec.UserKeyFromDataKey[K](key, false), (*V)(value))
		if cloned == nil {
			return nil, errCloneNil
		}
		return unsafe.Pointer(cloned), nil
	}
	return func(cfg *execOptions[K, V]) { cfg.cloneValue = f }
}

// BeforeCommit registers a callback invoked inside the write transaction just
// before it is committed.
//
// The callback:
//   - May perform additional reads or writes through the provided tx.
//   - May return an error to abort the operation; in this case the
//     transaction will be rolled back and index state will not be updated.
//   - Must not modify oldValue or newValue.
//   - Must not retain oldValue or newValue.
//   - Must not commit or roll back the transaction.
//   - Must not modify records or bucket sequence in the bucket managed by this DB instance
//     (or by any other DB instance with enabled indexing),
//     because such writes bypass index synchronization.
//   - Must not call any methods on the DB instance. Depending on execution mode,
//     BeforeCommit may run while RBI holds internal locks, so re-entering the
//     same DB can deadlock or become mode-dependent.
//
// BeforeCommit is invoked only for records that exist or are being written.
// Patch/Delete operations skip missing records and do not invoke callbacks for them.
//
// BeforeCommit may be passed more than once. Callbacks passed to New are invoked
// before per-operation callbacks.
func BeforeCommit[K ~string | ~uint64, V any](fn func(tx *bbolt.Tx, key K, oldValue, newValue *V) error) ExecOption[K, V] {
	if fn == nil {
		return nil
	}
	if reflect.TypeFor[K]().Kind() == reflect.String {
		f := func(tx *bbolt.Tx, key keycodec.DataKey, oldValue, newValue unsafe.Pointer) error {
			return fn(tx, keycodec.UserKeyFromDataKey[K](key, true), (*V)(oldValue), (*V)(newValue))
		}
		return func(cfg *execOptions[K, V]) { cfg.beforeCommit = append(cfg.beforeCommit, f) }
	}

	f := func(tx *bbolt.Tx, key keycodec.DataKey, oldValue, newValue unsafe.Pointer) error {
		return fn(tx, keycodec.UserKeyFromDataKey[K](key, false), (*V)(oldValue), (*V)(newValue))
	}
	return func(cfg *execOptions[K, V]) { cfg.beforeCommit = append(cfg.beforeCommit, f) }
}

// PatchStrict configures Patch and BatchPatch to return an error if the patch
// contains unknown field names.
//
// When passed to New, PatchStrict becomes the default for all patch operations
// on the returned DB. Passing PatchStrict to non-patch write methods has no effect.
func PatchStrict[K ~string | ~uint64, V any](cfg *execOptions[K, V]) {
	cfg.patchStrict = true
}

type execOptions[K ~string | ~uint64, V any] struct {
	beforeProcess []wexec.BeforeProcessHook
	beforeStore   []wexec.BeforeStoreHook
	beforeCommit  []wexec.BeforeCommitHook
	cloneValue    wexec.CloneFunc
	noBatch       bool
	patchStrict   bool
}

var errCloneNil = errors.New("clone returned nil")

func applyExecOptions[K ~string | ~uint64, V any](cfg *execOptions[K, V], opts []ExecOption[K, V]) {
	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}
}

func defaultCloneValue[V any]() func(*V) *V {
	method, ok := reflect.TypeFor[*V]().MethodByName("Clone")
	if !ok {
		return nil
	}
	clone, ok := method.Func.Interface().(func(*V) *V)
	if !ok {
		return nil
	}
	return clone
}

func freezeExecOptions[K ~string | ~uint64, V any](cfg execOptions[K, V]) execOptions[K, V] {
	if cfg.cloneValue == nil {
		clone := defaultCloneValue[V]()
		if clone != nil {
			cfg.cloneValue = func(_ keycodec.DataKey, value unsafe.Pointer) (unsafe.Pointer, error) {
				cloned := clone((*V)(value))
				if cloned == nil {
					return nil, errCloneNil
				}
				return unsafe.Pointer(cloned), nil
			}
		}
	}
	if len(cfg.beforeProcess) > 0 {
		cfg.beforeProcess = append([]wexec.BeforeProcessHook(nil), cfg.beforeProcess...)
		cfg.beforeProcess = cfg.beforeProcess[:len(cfg.beforeProcess):len(cfg.beforeProcess)]
	}
	if len(cfg.beforeStore) > 0 {
		cfg.beforeStore = append([]wexec.BeforeStoreHook(nil), cfg.beforeStore...)
		cfg.beforeStore = cfg.beforeStore[:len(cfg.beforeStore):len(cfg.beforeStore)]
	}
	if len(cfg.beforeCommit) > 0 {
		cfg.beforeCommit = append([]wexec.BeforeCommitHook(nil), cfg.beforeCommit...)
		cfg.beforeCommit = cfg.beforeCommit[:len(cfg.beforeCommit):len(cfg.beforeCommit)]
	}
	return cfg
}

func (db *DB[K, V]) resolveExecOptions(opts []ExecOption[K, V]) execOptions[K, V] {
	cfg := db.execOptions
	applyExecOptions(&cfg, opts)
	return cfg
}

// PatchOption controls MakePatch behaviour.
type PatchOption uint8

const (
	// PatchJSON makes MakePatch emit json tag names when present.
	// Fields without a json tag fall back to their Go struct field name.
	PatchJSON PatchOption = 1 << iota
)

// MakePatch builds and returns a patch describing fields that changed between
// oldVal and newVal.
//
// The patch includes both indexed and non-indexed fields. For every modified
// field it adds a Field entry whose Name uses the db tag when present or
// Go struct field name otherwise.
//
// When PatchJSON is passed, Name uses the json tag when present or
// Go struct field name otherwise.
//
// Value is always a deep copy taken from newVal.
//
// If newVal is nil, it returns an empty slice.
func (db *DB[K, V]) MakePatch(oldVal, newVal *V, opts ...PatchOption) []Field {
	useJSON := false
	for _, opt := range opts {
		if opt == PatchJSON {
			useJSON = true
		}
	}
	return db.makePatch(oldVal, newVal, nil, useJSON)
}

// MakePatchInto is like MakePatch, but writes the result into the provided
// buffer to reduce allocations.
//
// dst is treated as scratch space: it will be reset to length 0 and then filled
// with the resulting patch. The returned slice may refer to the same underlying
// array or a grown one if capacity is insufficient.
//
// If newVal is nil, it returns an empty slice.
func (db *DB[K, V]) MakePatchInto(oldVal, newVal *V, dst []Field, opts ...PatchOption) []Field {
	useJSON := false
	for _, opt := range opts {
		if opt == PatchJSON {
			useJSON = true
		}
	}
	return db.makePatch(oldVal, newVal, dst, useJSON)
}

type patchScratch struct {
	seen []bool
}

var patchScratchPool = pooled.Pointers[patchScratch]{
	Cleanup: func(scratch *patchScratch) {
		clear(scratch.seen[:cap(scratch.seen)])
		scratch.seen = scratch.seen[:0]
	},
}

func (db *DB[K, V]) makePatch(oldVal, newVal *V, target []Field, useJSON bool) []Field {
	target = target[:0]

	if newVal == nil {
		return target
	}

	var rvOld, rvNew reflect.Value
	if oldVal != nil {
		rvOld = reflect.ValueOf(oldVal).Elem()
	}
	rvNew = reflect.ValueOf(newVal).Elem()

	scratch := patchScratchPool.Get()
	patchAccess := db.schema.Patch.Access
	scratch.seen = slices.Grow(scratch.seen[:0], len(patchAccess))[:len(patchAccess)]
	defer patchScratchPool.Put(scratch)

	newPtr := unsafe.Pointer(newVal)
	oldPtr := unsafe.Pointer(nil)
	if oldVal != nil {
		oldPtr = unsafe.Pointer(oldVal)
	}

	db.forEachModifiedIndexedField(oldVal, newVal, func(acc schema.IndexedFieldAccessor) bool {
		if acc.PatchOrdinal < 0 {
			return true
		}
		patchAcc := patchAccess[acc.PatchOrdinal]
		var value any
		if patchAcc.CopyValue != nil {
			value = patchAcc.CopyValue(newPtr)
		} else {
			value = deepCopyValue(rvNew.FieldByIndex(patchAcc.Field.Index).Interface())
		}
		name := patchAcc.Field.DBName
		if useJSON {
			name = patchAcc.Field.JSONName
		}
		scratch.seen[acc.PatchOrdinal] = true
		target = append(target, Field{
			Name:  name,
			Value: value,
		})
		return true
	})

	for ordinal, patchAcc := range patchAccess {
		if scratch.seen[ordinal] {
			continue
		}

		var newValue any
		if rvOld.IsValid() {
			if patchAcc.ValueEqual != nil {
				if patchAcc.ValueEqual(oldPtr, newPtr) {
					continue
				}
			} else {
				oldValue := rvOld.FieldByIndex(patchAcc.Field.Index).Interface()
				newValue = rvNew.FieldByIndex(patchAcc.Field.Index).Interface()
				if reflect.DeepEqual(oldValue, newValue) {
					continue
				}
			}
		}
		if patchAcc.CopyValue != nil {
			newValue = patchAcc.CopyValue(newPtr)
		} else if newValue == nil {
			newValue = deepCopyValue(rvNew.FieldByIndex(patchAcc.Field.Index).Interface())
		} else {
			newValue = deepCopyValue(newValue)
		}
		name := patchAcc.Field.DBName
		if useJSON {
			name = patchAcc.Field.JSONName
		}

		target = append(target, Field{
			Name:  name,
			Value: newValue,
		})
	}

	return target
}

func patchItemsForWrite(fields []Field) []schema.PatchItem {
	// Field and schema.PatchItem are layout-identical; wexec copies this view
	// into request-owned storage immediately.
	return unsafe.Slice((*schema.PatchItem)(unsafe.SliceData(fields)), len(fields))
}

func (db *DB[K, V]) userKeyFromBytes(b []byte) K {
	return keycodec.UserKeyFromBytes[K](b, db.strKey)
}

func (db *DB[K, V]) forEachModifiedAccessor(accessors []schema.IndexedFieldAccessor, v1 *V, v2 *V, fn func(schema.IndexedFieldAccessor) bool) {
	if fn == nil {
		return
	}
	if len(accessors) == 0 {
		return
	}
	if v1 == nil || v2 == nil {
		for _, acc := range accessors {
			if !fn(acc) {
				return
			}
		}
		return
	}
	ptr1 := unsafe.Pointer(v1)
	ptr2 := unsafe.Pointer(v2)
	for _, acc := range accessors {
		if acc.Modified(ptr1, ptr2) && !fn(acc) {
			return
		}
	}
}

func (db *DB[K, V]) forEachModifiedIndexedField(v1 *V, v2 *V, fn func(schema.IndexedFieldAccessor) bool) {
	if db.engine == nil {
		return
	}
	db.forEachModifiedAccessor(db.engine.schema.Indexed, v1, v2, fn)
}

func deepCopyValue(src any) any {
	if src == nil {
		return nil
	}
	origin := reflect.ValueOf(src)

	for origin.Kind() == reflect.Interface {
		if origin.IsNil() {
			return nil
		}
		origin = origin.Elem()
	}

	switch origin.Kind() {
	case reflect.Bool, reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64,
		reflect.Complex64, reflect.Complex128:
		return origin.Interface()
	}

	visited := make(map[uintptr]reflect.Value)
	clone := deepCopy(origin, visited)
	return clone.Interface()
}

func deepCopy(origin reflect.Value, visited map[uintptr]reflect.Value) reflect.Value {
	if !origin.IsValid() {
		return origin
	}

	kind := origin.Kind()

	switch kind {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Interface:
		if origin.IsNil() {
			return origin
		}
	}

	switch kind {
	case reflect.Ptr, reflect.Map, reflect.Slice:
		addr := origin.Pointer()
		if clone, ok := visited[addr]; ok {
			return clone
		}
	}

	switch kind {
	case reflect.Bool, reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64,
		reflect.Complex64, reflect.Complex128:
		return origin

	case reflect.Struct:
		s := reflect.New(origin.Type()).Elem()
		s.Set(origin)
		for i := 0; i < origin.NumField(); i++ {
			sf := s.Field(i)
			if !sf.CanSet() {
				continue
			}
			clone := deepCopy(origin.Field(i), visited)
			sf.Set(clone)
		}
		return s

	case reflect.Ptr:
		ptr := reflect.New(origin.Elem().Type())
		visited[origin.Pointer()] = ptr
		clone := deepCopy(origin.Elem(), visited)
		ptr.Elem().Set(clone)
		return ptr

	case reflect.Slice:
		s := reflect.MakeSlice(origin.Type(), origin.Len(), origin.Cap())
		visited[origin.Pointer()] = s
		for i := 0; i < origin.Len(); i++ {
			clone := deepCopy(origin.Index(i), visited)
			s.Index(i).Set(clone)
		}
		return s

	case reflect.Map:
		m := reflect.MakeMap(origin.Type())
		visited[origin.Pointer()] = m
		for _, key := range origin.MapKeys() {
			keyClone := deepCopy(key, visited)
			valClone := deepCopy(origin.MapIndex(key), visited)
			m.SetMapIndex(keyClone, valClone)
		}
		return m

	case reflect.Array:
		a := reflect.New(origin.Type()).Elem()
		for i := 0; i < origin.Len(); i++ {
			clone := deepCopy(origin.Index(i), visited)
			a.Index(i).Set(clone)
		}
		return a

	case reflect.Interface:
		clone := deepCopy(origin.Elem(), visited)
		if !clone.IsValid() {
			return reflect.Zero(origin.Type())
		}
		return clone.Convert(origin.Type())

	case reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return reflect.Zero(origin.Type())

	default:
		panic(fmt.Errorf("rbi: deepCopy: unsupported value kind: %v", kind))
	}
}

// Get returns the value stored by id or nil if key was not found.
func (db *DB[K, V]) Get(id K) (*V, error) {
	if err := db.beginOp(); err != nil {
		return nil, err
	}
	defer db.endOp()

	tx, err := db.bolt.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("tx error: %w", err)
	}
	defer rollback(tx)

	bucket := tx.Bucket(db.bucket)
	if bucket == nil {
		return nil, fmt.Errorf("bucket does not exist")
	}

	var keyBuf [8]byte
	v := bucket.Get(keycodec.UserKeyBytesWithBuf(id, db.strKey, &keyBuf))
	if v == nil {
		return nil, nil
	}
	r, err := db.decode(v)
	if err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}

	return r, nil
}

// BatchGet retrieves multiple values by their IDs in a single read transaction.
// The returned slice has the same length as ids; any missing keys have a nil
// entry at the corresponding index.
func (db *DB[K, V]) BatchGet(ids ...K) ([]*V, error) {
	if err := db.beginOp(); err != nil {
		return nil, err
	}
	defer db.endOp()

	if len(ids) == 0 {
		return nil, nil
	}

	tx, err := db.bolt.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("tx error: %w", err)
	}
	defer rollback(tx)

	return db.batchGetTx(tx, ids...)
}

// batchGetTx retrieves multiple values by IDs using an existing read transaction.
func (db *DB[K, V]) batchGetTx(tx *bbolt.Tx, ids ...K) ([]*V, error) {
	bucket := tx.Bucket(db.bucket)
	if bucket == nil {
		return nil, fmt.Errorf("bucket does not exist")
	}

	s := make([]*V, len(ids))
	var keyBuf [8]byte
	for i, id := range ids {
		v := bucket.Get(keycodec.UserKeyBytesWithBuf(id, db.strKey, &keyBuf))
		if v == nil {
			continue
		}
		value, err := db.decode(v)
		if err != nil {
			return s, fmt.Errorf("decode: %w", err)
		}
		s[i] = value
	}
	return s, nil
}

// ScanKeys iterates over keys in the in-memory index snapshot and calls fn for
// each key greater than or equal to seek.
//
// In transparent mode it returns ErrNoIndex.
//
// The scan stops when fn returns false or a non-nil error. The scan does not
// open a Bolt transaction and may not reflect concurrent writes.
//
// For string keys, iteration order follows internal key-index order, not
// lexicographic order. In that mode, seek is still applied as a plain
// `key >= seek` value filter, but not as a lexicographic seek in traversal
// order. As a result, string-key ScanKeys is not suitable for prefix scans,
// resume/pagination cursors, or ordered iteration. Use QueryKeys(PREFIX(...))
// for prefix matching or SeqScan/SeqScanRaw for ordered key traversal.
func (db *DB[K, V]) ScanKeys(seek K, fn func(K) (bool, error)) error {
	if err := db.beginOp(); err != nil {
		return err
	}
	defer db.endOp()

	if db.engine == nil {
		return ErrNoIndex
	}

	snap, seq, ref := db.engine.snapshot.PinCurrent()
	defer db.engine.snapshot.Unpin(seq, ref)

	universe := snap.Universe
	iter := universe.Iter()
	defer iter.Release()

	if db.strKey {
		return db.scanStringKeys(snap.StrMap, universe, iter, seek, fn)
	}

	for iter.HasNext() {
		idx := iter.Next()
		key := *(*K)(unsafe.Pointer(&idx))
		if key < seek {
			continue
		}
		cont, err := fn(key)
		if err != nil {
			return err
		}
		if !cont {
			break
		}
	}

	return nil
}

func (db *DB[K, V]) scanStringKeys(snap *strmap.Snapshot, universe posting.List, iter posting.Iterator, seek K, fn func(K) (bool, error)) error {
	if snap == nil || universe.IsEmpty() {
		return nil
	}

	seekStr := *(*string)(unsafe.Pointer(&seek))
	next := snap.Next()

	card := universe.Cardinality()
	minIdx, hasMin := universe.Minimum()
	maxIdx, hasMax := universe.Maximum()

	lookup := snap.Lookup()
	if card == next && card > 0 && hasMin && hasMax && minIdx == 1 && maxIdx == next {
		for idx := uint64(1); idx <= next; idx++ {
			s, ok := lookup.String(idx)
			if !ok {
				return fmt.Errorf("%w: %v", ErrNoValidKeyIndex, idx)
			}
			cont, err := db.emitScannedStringKey(seekStr, s, fn)
			if err != nil {
				return err
			}
			if !cont {
				return nil
			}
		}
		return nil
	}

	for iter.HasNext() {
		idx := iter.Next()
		s, ok := lookup.String(idx)
		if !ok {
			return fmt.Errorf("%w: %v", ErrNoValidKeyIndex, idx)
		}

		cont, err := db.emitScannedStringKey(seekStr, s, fn)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return nil
}

func (db *DB[K, V]) emitScannedStringKey(seek string, s string, fn func(K) (bool, error)) (bool, error) {
	if s < seek {
		return true, nil
	}
	key := *(*K)(unsafe.Pointer(&s))
	return fn(key)
}

// SeqScan performs a sequential scan over all records starting at the given
// key (inclusive), decoding each value and passing it to the provided fn.
// SeqScan stops reading when the fn returns false or a non-nil error.
// The scan runs inside a read-only transaction which remains open for the
// duration of the scan.
//
// Records passed to fn can optionally be returned back using ReleaseRecords
// to minimize GC pressure.
func (db *DB[K, V]) SeqScan(seek K, fn func(K, *V) (bool, error)) error {
	if err := db.beginOp(); err != nil {
		return err
	}
	defer db.endOp()

	tx, err := db.bolt.Begin(false)
	if err != nil {
		return fmt.Errorf("tx error: %w", err)
	}
	defer rollback(tx)

	b := tx.Bucket(db.bucket)
	if b == nil {
		return fmt.Errorf("bucket does not exist")
	}
	c := b.Cursor()

	var keyBuf [8]byte
	key, value := c.Seek(keycodec.UserKeyBytesWithBuf(seek, db.strKey, &keyBuf))
	if key == nil {
		return nil
	}
	val, err := db.decode(value)
	if err != nil {
		return fmt.Errorf("decode: %w", err)
	}

	more, err := fn(db.userKeyFromBytes(key), val)
	if err != nil {
		return err
	}
	for more {
		key, value = c.Next()
		if key == nil {
			return nil
		}
		if val, err = db.decode(value); err != nil {
			return fmt.Errorf("decode: %w", err)
		}
		if more, err = fn(db.userKeyFromBytes(key), val); err != nil {
			return err
		}
	}
	return nil
}

// SeqScanRaw performs a sequential scan over all records starting at the
// given key (inclusive), passing raw bytes to the provided fn.
// These bytes are encoded representation of the values.
//
// SeqScanRaw stops reading when the provided fn returns false or a non-nil error.
// The database transaction remains open during the scan.
//
// Bytes passed to fn must not be modified.
func (db *DB[K, V]) SeqScanRaw(seek K, fn func(K, []byte) (bool, error)) error {
	if err := db.beginOp(); err != nil {
		return err
	}
	defer db.endOp()

	tx, err := db.bolt.Begin(false)
	if err != nil {
		return fmt.Errorf("tx error: %w", err)
	}
	defer rollback(tx)

	b := tx.Bucket(db.bucket)
	if b == nil {
		return fmt.Errorf("bucket does not exist")
	}
	c := b.Cursor()

	var keyBuf [8]byte
	key, value := c.Seek(keycodec.UserKeyBytesWithBuf(seek, db.strKey, &keyBuf))
	if key == nil {
		return nil
	}

	more, err := fn(db.userKeyFromBytes(key), value)
	if err != nil {
		return err
	}
	for more {
		key, value = c.Next()
		if key == nil {
			return nil
		}
		if more, err = fn(db.userKeyFromBytes(key), value); err != nil {
			return err
		}
	}
	return nil
}

// Set stores the given value under the specified key ID.
//
// BeforeProcess hooks, if any, run on the caller-owned input value before RBI
// starts encoding, cloning, queueing, or transactional work for Set.
//
// The value is encoded and written inside a single write transaction.
// Any existing value for the key is decoded and passed as oldValue to BeforeStore
// and BeforeCommit hooks. BeforeStore may modify the stored value before it is
// encoded. If *V implements Codec, it is used directly; otherwise msgpack is used.
//
// If BeforeStore is used and CloneFunc is not provided, Set checks
// whether *V implements Clone() *V and uses it when available.
// Otherwise, the input value must already be encodable because Set falls back
// to encode/decode snapshotting before the hook runs.
//
// CloneFunc can also be used as a faster cloning path when the caller can
// produce an independent copy more cheaply than RBI's fallback snapshotting.
// If any hook returns an error, the transaction is rolled back and the error is
// returned.
func (db *DB[K, V]) Set(id K, newVal *V, execOpts ...ExecOption[K, V]) error {
	if newVal == nil {
		return ErrNilValue
	}

	cfg := db.resolveExecOptions(execOpts)
	key := keycodec.DataKeyFromUserKey(id, db.strKey)
	if len(cfg.beforeProcess) != 0 {
		for _, fn := range cfg.beforeProcess {
			if err := fn(key, unsafe.Pointer(newVal)); err != nil {
				return err
			}
		}
	}
	if err := db.beginOp(); err != nil {
		return err
	}
	defer db.endOp()

	batch := db.batcher.NewBatch(1)
	if err := batch.AddSet(key, unsafe.Pointer(newVal), cfg.beforeStore, cfg.beforeCommit, cfg.cloneValue); err != nil {
		batch.Cancel()
		return err
	}
	return batch.Submit(cfg.noBatch)
}

// BatchSet stores multiple values under the provided IDs in a single write
// transaction. The length of the ids and values must be equal.
//
// BeforeProcess hooks, if any, run on each caller-owned input value before RBI
// starts encoding, cloning, queueing, or transactional work for BatchSet.
//
// For each key, any existing value is decoded and passed as oldValue to all
// configured BeforeStore and BeforeCommit hooks.
// If an error is encountered during any of the processing steps,
// the transaction is rolled back and the error is returned.
// If BeforeStore is used and CloneFunc is not provided, BatchSet first checks
// whether *V implements `Clone() *V` and uses it when available.
// Otherwise, each input value must already be encodable because BatchSet
// falls back to encode/decode snapshotting before the hook runs.
//
// CloneFunc can also be used as a faster cloning path when the caller can
// produce an independent copy more cheaply than RBI's fallback snapshotting.
//
// After a successful commit, the in-memory index is updated for all modified keys.
//
// BatchSet allocates a buffer for each encoded value.
// Storing a large number of values will consume a proportional amount of memory.
func (db *DB[K, V]) BatchSet(ids []K, newVals []*V, execOpts ...ExecOption[K, V]) error {
	if len(ids) != len(newVals) {
		return fmt.Errorf("different slice lengths")
	}
	if len(ids) == 0 {
		return nil
	}

	for i := range newVals {
		if newVals[i] == nil {
			return fmt.Errorf("%w: values[%v]", ErrNilValue, i)
		}
	}

	cfg := db.resolveExecOptions(execOpts)
	var keyScratch []keycodec.DataKey
	if len(cfg.beforeProcess) != 0 {
		keyScratch = keycodec.GetDataKeySlice(len(ids))

		for i := range newVals {
			key := keycodec.DataKeyFromUserKey(ids[i], db.strKey)
			for _, fn := range cfg.beforeProcess {
				if err := fn(key, unsafe.Pointer(newVals[i])); err != nil {
					keycodec.ReleaseDataKeySlice(keyScratch)
					return err
				}
			}
			keyScratch = append(keyScratch, key)
		}
		defer keycodec.ReleaseDataKeySlice(keyScratch)
	}
	if err := db.beginOp(); err != nil {
		return err
	}
	defer db.endOp()

	batch := db.batcher.NewBatch(len(ids))

	for i := range ids {
		key := keycodec.DataKeyFromUserKey(ids[i], db.strKey)
		if keyScratch != nil {
			key = keyScratch[i]
		}
		if err := batch.AddSet(key, unsafe.Pointer(newVals[i]), cfg.beforeStore, cfg.beforeCommit, cfg.cloneValue); err != nil {
			batch.Cancel()
			return err
		}
	}
	return batch.Submit(true)
}

// Patch applies a partial update to the value stored under the given id,
// updating only the fields listed in patch.
//
// Unknown field names in patch are silently ignored. All fields, indexed or
// not, are eligible to be patched. Patch attempts to convert the provided
// values to the appropriate field type, if possible.
// If conversion fails for any field, Patch returns an error and no changes are committed.
//
// If no item is found for the specified id, Patch is a no-op and callbacks are
// not invoked.
//
// BeforeProcess hooks, if any, run on the mutable post-patch working copy
// before BeforeStore and BeforeCommit.
//
// All configured BeforeStore and BeforeCommit hooks are invoked with the
// original (old) and final (new) values before commit. After a successful
// commit, the in-memory index is updated.
func (db *DB[K, V]) Patch(id K, patch []Field, execOpts ...ExecOption[K, V]) error {
	if len(patch) == 0 {
		return nil
	}
	cfg := db.resolveExecOptions(execOpts)
	ignoreUnknown := !cfg.patchStrict
	if err := db.beginOp(); err != nil {
		return err
	}
	defer db.endOp()

	patchItems := patchItemsForWrite(patch)
	batch := db.batcher.NewBatch(1)
	batch.AddPatch(keycodec.DataKeyFromUserKey(id, db.strKey), patchItems, ignoreUnknown, cfg.beforeProcess, cfg.beforeStore, cfg.beforeCommit)
	return batch.Submit(cfg.noBatch)
}

// BatchPatch applies the same patch to all values stored under the given IDs
// in a single write transaction.
//
// Unknown fields are ignored (as in Patch).
// Any errors during processing aborts the entire batch and rolls back the transaction.
//
// Non-existent IDs are skipped and do not trigger hooks.
//
// BatchPatch allocates a buffer for each encoded value.
// Patching a large number of values will consume a proportional amount of memory.
func (db *DB[K, V]) BatchPatch(ids []K, patch []Field, execOpts ...ExecOption[K, V]) error {
	if len(ids) == 0 || len(patch) == 0 {
		return nil
	}
	cfg := db.resolveExecOptions(execOpts)
	ignoreUnknown := !cfg.patchStrict

	if err := db.beginOp(); err != nil {
		return err
	}
	defer db.endOp()

	patchItems := patchItemsForWrite(patch)
	batch := db.batcher.NewBatch(len(ids))

	for i := range ids {
		batch.AddPatch(keycodec.DataKeyFromUserKey(ids[i], db.strKey), patchItems, ignoreUnknown, cfg.beforeProcess, cfg.beforeStore, cfg.beforeCommit)
	}
	return batch.Submit(true)
}

// Delete removes the value stored under the given id, if any.
//
// The existing value (if present) is decoded and passed as oldValue to all
// configured BeforeCommit callbacks. If any callback returns an error, the
// operation is aborted. If the record does not exist, Delete is a no-op and no
// callbacks are invoked.
func (db *DB[K, V]) Delete(id K, execOpts ...ExecOption[K, V]) error {
	cfg := db.resolveExecOptions(execOpts)

	if err := db.beginOp(); err != nil {
		return err
	}
	defer db.endOp()

	batch := db.batcher.NewBatch(1)
	batch.AddDelete(keycodec.DataKeyFromUserKey(id, db.strKey), cfg.beforeCommit)
	return batch.Submit(cfg.noBatch)
}

// BatchDelete removes all values stored under the provided ids in a single
// write transaction.
//
// For each key, any existing value is decoded and passed as oldValue to all
// configured BeforeCommit callbacks. If an error is encountered during
// processing, the entire operation is rolled back.
// Missing IDs are skipped and do not trigger callbacks.
func (db *DB[K, V]) BatchDelete(ids []K, execOpts ...ExecOption[K, V]) error {
	if len(ids) == 0 {
		return nil
	}

	cfg := db.resolveExecOptions(execOpts)

	if err := db.beginOp(); err != nil {
		return err
	}
	defer db.endOp()

	batch := db.batcher.NewBatch(len(ids))

	for i := range ids {
		batch.AddDelete(keycodec.DataKeyFromUserKey(ids[i], db.strKey), cfg.beforeCommit)
	}
	return batch.Submit(true)
}
