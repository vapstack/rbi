package rbi

import "go.etcd.io/bbolt"

type beforeStoreFunc[K ~string | ~uint64, V any] = func(key K, oldValue, newValue *V) error
type beforeCommitFunc[K ~string | ~uint64, V any] = func(tx *bbolt.Tx, key K, oldValue, newValue *V) error

type execOptions[K ~string | ~uint64, V any] struct {
	beforeStore  []beforeStoreFunc[K, V]
	beforeCommit []beforeCommitFunc[K, V]
	cloneValue   func(K, *V) *V
	patchStrict  bool
}

// ExecOption configures execution behavior for a write operation.
//
// Different write methods may honor different options.
type ExecOption[K ~string | ~uint64, V any] func(*execOptions[K, V])

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
// RBI's fallback msgpack snapshotting, pass CloneFunc.
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
// If CloneFunc is not provided, RBI falls back to msgpack snapshotting before
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
//   - Must not modify oldValue or newValue.
//   - Must not commit or roll back the transaction.
//   - Must not modify records or bucket sequence in the bucket managed by this DB instance
//     (or by any other DB instance with enabled indexing),
//     because such writes bypass index synchronization.
//   - May perform additional reads or writes within the same transaction.
//   - May return an error to abort the operation; in this case the
//     transaction will be rolled back and index state will not be updated.
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

func freezeExecOptions[K ~string | ~uint64, V any](cfg execOptions[K, V]) execOptions[K, V] {
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
