package rbi

import "go.etcd.io/bbolt"

type beforeCommitFunc[K ~string | ~uint64, V any] = func(tx *bbolt.Tx, key K, oldValue, newValue *V) error

type execConfig[K ~string | ~uint64, V any] struct {
	beforeCommit []beforeCommitFunc[K, V]
	patchStrict  bool
}

// ExecOption configures execution behavior for a write operation.
//
// Different write methods may honor different options.
type ExecOption[K ~string | ~uint64, V any] func(*execConfig[K, V])

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
	return func(cfg *execConfig[K, V]) {
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
func PatchStrict[K ~string | ~uint64, V any](cfg *execConfig[K, V]) {
	if cfg == nil {
		return
	}
	cfg.patchStrict = true
}

func applyExecOptions[K ~string | ~uint64, V any](cfg *execConfig[K, V], opts []ExecOption[K, V]) {
	for _, opt := range opts {
		if cfg != nil && opt != nil {
			opt(cfg)
		}
	}
}

func (db *DB[K, V]) resolveExecOptions(opts []ExecOption[K, V]) execConfig[K, V] {
	cfg := execConfig[K, V]{}
	applyExecOptions(&cfg, db.defaultExecOptions)
	applyExecOptions(&cfg, opts)
	return cfg
}
