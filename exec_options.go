package rbi

import (
	"reflect"

	"go.etcd.io/bbolt"
)

type beforeProcessFunc[K ~string | ~uint64, V any] = func(key K, value *V) error
type beforeStoreFunc[K ~string | ~uint64, V any] = func(key K, oldValue, newValue *V) error
type beforeCommitFunc[K ~string | ~uint64, V any] = func(tx *bbolt.Tx, key K, oldValue, newValue *V) error

type execOptions[K ~string | ~uint64, V any] struct {
	beforeProcess []beforeProcessFunc[K, V]
	beforeStore   []beforeStoreFunc[K, V]
	beforeCommit  []beforeCommitFunc[K, V]
	cloneValue    func(K, *V) *V
	noBatch       bool
	patchStrict   bool
}

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
	return func(cfg *execOptions[K, V]) {
		if cfg == nil || fn == nil {
			return
		}
		cfg.beforeProcess = append(cfg.beforeProcess, fn)
	}
}

// NoBatch forces a write call to execute in its own internal batch.
//
// For Set, Patch, and Delete it keeps the request from being coalesced with
// neighboring writes.
//
// For BatchSet, BatchPatch, and BatchDelete it is redundant because those
// methods already execute as isolated internal batches.
func NoBatch[K ~string | ~uint64, V any](cfg *execOptions[K, V]) {
	if cfg == nil {
		return
	}
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
// For Set/BatchSet, RBI normally snapshots the input value through msgpack
// encode/decode before BeforeStore runs so it can safely isolate retries and
// caller-owned objects. If the value only becomes encodable after BeforeStore
// normalizes it, or if the caller can produce an independent copy faster than
// RBI's fallback msgpack snapshotting, pass CloneFunc. If CloneFunc is not
// provided and *V implements Clone() *V, RBI uses that method automatically.
//
// BeforeStore may be passed more than once. Hooks passed to New are invoked
// before per-operation hooks.
func BeforeStore[K ~string | ~uint64, V any](fn func(key K, oldValue, newValue *V) error) ExecOption[K, V] {
	return func(cfg *execOptions[K, V]) {
		if cfg == nil || fn == nil {
			return
		}
		cfg.beforeStore = append(cfg.beforeStore, fn)
	}
}

// CloneFunc registers a cloning function used by Set/BatchSet when BeforeStore
// hooks are present.
//
// When provided, RBI uses CloneFunc to create an internal baseline copy before
// BeforeStore runs instead of snapshotting the value through msgpack
// encode/decode. This is useful for values that only become encodable after
// BeforeStore normalizes them, and it may also reduce cloning overhead when
// the caller can produce an independent copy faster than RBI's fallback
// msgpack snapshotting.
//
// CloneFunc:
//   - Receives the write key and source value.
//   - Must return an independent copy that does not alias the input.
//   - Must not mutate the input.
//   - Must return non-nil for non-nil input.
//   - Must be deterministic and free of external side effects.
//
// If CloneFunc is not provided and *V implements Clone() *V, RBI uses that
// method automatically. Otherwise RBI falls back to msgpack snapshotting before
// BeforeStore for Set/BatchSet. Patch/BatchPatch ignore CloneFunc because they
// already rebuild a fresh working copy from stored bytes.
//
// CloneFunc may be passed more than once. The last non-nil CloneFunc wins.
// When passed to New, CloneFunc becomes the default for matching write
// operations on the returned DB.
func CloneFunc[K ~string | ~uint64, V any](fn func(key K, v *V) *V) ExecOption[K, V] {
	return func(cfg *execOptions[K, V]) {
		if cfg == nil || fn == nil {
			return
		}
		cfg.cloneValue = fn
	}
}

// BeforeCommit registers a callback invoked inside the write transaction just
// before it is committed.
//
// The callback:
//   - May perform additional reads or writes through the provided tx.
//   - May return an error to abort the operation; in this case the
//     transaction will be rolled back and index state will not be updated.
//   - Must not modify oldValue or newValue.
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
	return func(cfg *execOptions[K, V]) {
		if cfg == nil || fn == nil {
			return
		}
		cfg.beforeCommit = append(cfg.beforeCommit, fn)
	}
}

// PatchStrict configures Patch and BatchPatch to return an error if the patch
// contains unknown field names.
//
// When passed to New, PatchStrict becomes the default for all patch operations
// on the returned DB. Passing PatchStrict to non-patch write methods has no effect.
func PatchStrict[K ~string | ~uint64, V any](cfg *execOptions[K, V]) {
	if cfg == nil {
		return
	}
	cfg.patchStrict = true
}

func applyExecOptions[K ~string | ~uint64, V any](cfg *execOptions[K, V], opts []ExecOption[K, V]) {
	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}
}

func defaultCloneValue[K ~string | ~uint64, V any]() func(K, *V) *V {
	method, ok := reflect.TypeFor[*V]().MethodByName("Clone")
	if !ok {
		return nil
	}
	clone, ok := method.Func.Interface().(func(*V) *V)
	if !ok {
		return nil
	}
	return func(_ K, v *V) *V {
		if v == nil {
			return nil
		}
		return clone(v)
	}
}

func freezeExecOptions[K ~string | ~uint64, V any](cfg execOptions[K, V]) execOptions[K, V] {
	if cfg.cloneValue == nil {
		cfg.cloneValue = defaultCloneValue[K, V]()
	}
	if len(cfg.beforeProcess) > 0 {
		cfg.beforeProcess = append([]beforeProcessFunc[K, V](nil), cfg.beforeProcess...)
		cfg.beforeProcess = cfg.beforeProcess[:len(cfg.beforeProcess):len(cfg.beforeProcess)]
	}
	if len(cfg.beforeStore) > 0 {
		cfg.beforeStore = append([]beforeStoreFunc[K, V](nil), cfg.beforeStore...)
		cfg.beforeStore = cfg.beforeStore[:len(cfg.beforeStore):len(cfg.beforeStore)]
	}
	if len(cfg.beforeCommit) > 0 {
		cfg.beforeCommit = append([]beforeCommitFunc[K, V](nil), cfg.beforeCommit...)
		cfg.beforeCommit = cfg.beforeCommit[:len(cfg.beforeCommit):len(cfg.beforeCommit)]
	}
	return cfg
}

func (db *DB[K, V]) resolveExecOptions(opts []ExecOption[K, V]) execOptions[K, V] {
	cfg := db.execOptions
	applyExecOptions(&cfg, opts)
	return cfg
}
