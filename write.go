package rbi

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/wexec"
	"github.com/vapstack/rbi/rbierrors"
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
//   - For Patch/BatchPatch, the hook may be invoked more than once for the
//     same logical operation when the auto-batcher retries a request.
//   - Is ignored by Delete and BatchDelete.
//
// The callback must not call methods on the DB instance. Write execution may
// hold internal locks or transactions while invoking callbacks, and re-entering
// DB from the callback can deadlock.
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
//   - Must not retain oldValue or newValue after it returns.
//
// The callback must not call methods on the DB instance. Write execution may
// hold internal locks or transactions while invoking callbacks, and re-entering
// DB from the callback can deadlock.
//
// BeforeStore may be invoked more than once for the same logical write operation;
// external side effects must therefore be idempotent or avoided.
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
//   - Must return a fully independent copy that does not alias the input.
//   - Must not mutate the input.
//   - Must return non-nil for non-nil input.
//   - Must be deterministic and free of external side effects.
//
// The callback must not call methods on the DB instance. Write execution may
// hold internal locks or transactions while invoking callbacks, and re-entering
// DB from the callback can deadlock.
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
				return nil, rbierrors.ErrCloneNil
			}
			return unsafe.Pointer(cloned), nil
		}
		return func(cfg *execOptions[K, V]) { cfg.cloneValue = f }
	}

	f := func(key keycodec.DataKey, value unsafe.Pointer) (unsafe.Pointer, error) {
		cloned := fn(keycodec.UserKeyFromDataKey[K](key, false), (*V)(value))
		if cloned == nil {
			return nil, rbierrors.ErrCloneNil
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
//   - Must not modify records or bucket sequences in the buckets managed by RBI.
//
// The callback must not call methods on the DB instance. It runs inside the
// write transaction while internal write execution is active, and re-entering
// DB from the callback can deadlock.
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
					return nil, rbierrors.ErrCloneNil
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
		return rbierrors.ErrNilValue
	}

	cfg := db.resolveExecOptions(execOpts)
	if err := db.unavailableErr(); err != nil {
		return err
	}
	key := keycodec.DataKeyFromUserKey(id, db.strKey)
	if len(cfg.beforeProcess) != 0 {
		for _, fn := range cfg.beforeProcess {
			if err := fn(key, unsafe.Pointer(newVal)); err != nil {
				return err
			}
		}
	}
	if err := db.unavailableErr(); err != nil {
		return err
	}

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
			return fmt.Errorf("%w: values[%v]", rbierrors.ErrNilValue, i)
		}
	}

	cfg := db.resolveExecOptions(execOpts)
	if err := db.unavailableErr(); err != nil {
		return err
	}
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
	if err := db.unavailableErr(); err != nil {
		return err
	}

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
	if err := db.unavailableErr(); err != nil {
		return err
	}

	patchItems := patchItemsForWrite(patch)
	if !ignoreUnknown {
		if err := db.schema.Patch.ValidateNames(patchItems); err != nil {
			return err
		}
	}
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

	if err := db.unavailableErr(); err != nil {
		return err
	}

	patchItems := patchItemsForWrite(patch)
	if !ignoreUnknown {
		if err := db.schema.Patch.ValidateNames(patchItems); err != nil {
			return err
		}
	}
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

	if err := db.unavailableErr(); err != nil {
		return err
	}

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

	if err := db.unavailableErr(); err != nil {
		return err
	}

	batch := db.batcher.NewBatch(len(ids))

	for i := range ids {
		batch.AddDelete(keycodec.DataKeyFromUserKey(ids[i], db.strKey), cfg.beforeCommit)
	}
	return batch.Submit(true)
}
