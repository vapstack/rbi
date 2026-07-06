package rbi

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/wexec"
	"github.com/vapstack/rbi/rbierrors"
	berrors "go.etcd.io/bbolt/errors"
)

// ExecOption configures execution behavior for a write operation.
//
// Different write methods may honor different options.
type ExecOption[K ~string | ~uint64, V any] func(*execOptions[K, V])

// OnChange registers a callback invoked for each record affected by a write.
//
// oldValue is the previously stored record and must be treated as read-only;
// it is nil for inserts. newValue is the record that will be stored and indexed
// after the callback returns; it is nil for deletions.
//
// The callback may modify non-nil newValue and may return an error to abort
// the write operation. Missing Patch/Delete targets do not invoke the callback.
//
// The callback must not retain oldValue or newValue after it returns. Writes
// issued from the callback must use the provided tx; using any other *Tx from
// inside OnChange is unsupported.
//
// Callback panics are recovered and returned as write errors.
func OnChange[K ~string | ~uint64, V any](fn func(tx *Tx, key K, oldValue, newValue *V) error) ExecOption[K, V] {
	if fn == nil {
		return nil
	}

	strKey := reflect.TypeFor[K]().Kind() == reflect.String
	hook := func(txp unsafe.Pointer, depth uint8, key keycodec.DataKey, oldValue, newValue unsafe.Pointer) (err error) {
		tx := (*Tx)(txp)
		prevDepth := tx.hookDepth
		tx.hookDepth = depth
		defer func() {
			tx.hookDepth = prevDepth
			if v := recover(); v != nil {
				if p, ok := v.(txLifecyclePanic); ok {
					err = p
					return
				}
				if e, ok := v.(error); ok {
					err = fmt.Errorf("on change panic: %w", e)
				} else {
					err = fmt.Errorf("on change panic: %v", v)
				}
			}
		}()
		err = fn(tx, keycodec.UserKeyFromDataKey[K](key, strKey), (*V)(oldValue), (*V)(newValue))
		if err != nil {
			return err
		}
		return tx.err
	}
	return func(cfg *execOptions[K, V]) {
		if len(cfg.onChange) == 0 {
			cfg.onChangeInline[0] = hook
			cfg.onChange = cfg.onChangeInline[:1]
			return
		}
		cfg.onChange = append(cfg.onChange, hook)
	}
}

// PatchStrict configures Patch to return an error if the patch contains
// unknown field names.
//
// When passed to Open, PatchStrict becomes the default for all patch operations
// on the returned Collection.
//
// Passing PatchStrict to non-patch write methods has no effect.
func PatchStrict[K ~string | ~uint64, V any](cfg *execOptions[K, V]) {
	cfg.patchStrict = true
}

type execOptions[K ~string | ~uint64, V any] struct {
	onChange       []wexec.OnChangeHook
	onChangeInline [1]wexec.OnChangeHook
	patchStrict    bool
}

func freezeExecOptions[K ~string | ~uint64, V any](cfg execOptions[K, V]) execOptions[K, V] {
	if len(cfg.onChange) > 0 {
		cfg.onChange = append([]wexec.OnChangeHook(nil), cfg.onChange...)
		cfg.onChange = cfg.onChange[:len(cfg.onChange):len(cfg.onChange)]
		cfg.onChangeInline[0] = nil
	}
	return cfg
}

func (c *Collection[K, V]) resolveExecOptions(opts []ExecOption[K, V]) execOptions[K, V] {
	cfg := c.execOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

// Set queues newVal to be stored under the given id.
func (c *Collection[K, V]) Set(tx *Tx, id K, newVal *V, execOpts ...ExecOption[K, V]) error {
	if tx == nil {
		return rbierrors.ErrNilTx
	}
	if err := tx.usableWrite(); err != nil {
		return err
	}
	if newVal == nil {
		return rbierrors.ErrNilValue
	}

	cfg := c.execOptions
	if len(execOpts) != 0 {
		cfg = c.resolveExecOptions(execOpts)
	}

	key := keycodec.DataKeyFromUserKey(id, c.strKey)
	if c.strKey && key.String() == "" {
		return berrors.ErrKeyRequired
	}
	segmentCount := len(tx.unit.segments)
	collectionCount := len(tx.collections)
	rootWasNil := tx.root == nil
	seg, err := tx.collectionSegment(c.collection)
	if err != nil {
		return err
	}
	if err = seg.ops.AddSet(key, unsafe.Pointer(newVal), cfg.onChange, tx.generatedDepth()); err != nil {
		if tx.state == writeTxOpen && len(tx.unit.segments) > segmentCount && tx.unit.segments[segmentCount].work == 0 {
			tx.unit.segments[segmentCount].ops.Cancel()
			clear(tx.unit.segments[segmentCount:])
			tx.unit.segments = tx.unit.segments[:segmentCount]
			for i := len(tx.collections) - 1; i >= collectionCount; i-- {
				tx.collections[i].releaseRetain()
			}
			clear(tx.collections[collectionCount:])
			tx.collections = tx.collections[:collectionCount]
			if rootWasNil {
				tx.root = nil
			}
		}
		return err
	}
	seg.work++
	return nil
}

// Patch queues a partial update for the value stored under the given id.
//
// Only fields listed in the patch are updated.
// Unknown field names are ignored unless PatchStrict is passed,
// in which case Patch returns an error.
//
// Patch may update indexed and non-indexed fields.
// Each patch value is converted to the target field type when possible.
// If any conversion fails, Patch returns an error.
//
// If the provided id does not exist, Patch is a no-op.
func (c *Collection[K, V]) Patch(tx *Tx, id K, patch []Field, execOpts ...ExecOption[K, V]) error {
	if tx == nil {
		return rbierrors.ErrNilTx
	}
	if err := tx.usableWrite(); err != nil {
		return err
	}
	if len(patch) == 0 {
		return nil
	}
	cfg := c.execOptions
	if len(execOpts) != 0 {
		cfg = c.resolveExecOptions(execOpts)
	}
	ignoreUnknown := !cfg.patchStrict

	key := keycodec.DataKeyFromUserKey(id, c.strKey)
	if c.strKey && key.String() == "" {
		return berrors.ErrKeyRequired
	}
	patchItems, err := c.patchItemsForWrite(patch, ignoreUnknown)
	if err != nil {
		return err
	}
	if len(patchItems) == 0 {
		schema.ReleasePatchItemSlice(patchItems)
		return nil
	}
	seg, err := tx.collectionSegment(c.collection)
	if err != nil {
		schema.ReleasePatchItemSlice(patchItems)
		return err
	}
	seg.ops.AddPatch(key, patchItems, ignoreUnknown, cfg.onChange, tx.generatedDepth())
	seg.work++
	return nil
}

// Delete queues deletion of the record stored under the given id.
//
// If the record does not exist, Delete is a no-op.
func (c *Collection[K, V]) Delete(tx *Tx, id K, execOpts ...ExecOption[K, V]) error {
	if tx == nil {
		return rbierrors.ErrNilTx
	}
	if err := tx.usableWrite(); err != nil {
		return err
	}
	cfg := c.execOptions
	if len(execOpts) != 0 {
		cfg = c.resolveExecOptions(execOpts)
	}

	key := keycodec.DataKeyFromUserKey(id, c.strKey)
	if c.strKey && key.String() == "" {
		return berrors.ErrKeyRequired
	}
	seg, err := tx.collectionSegment(c.collection)
	if err != nil {
		return err
	}
	seg.ops.AddDelete(key, cfg.onChange, tx.generatedDepth())
	seg.work++
	return nil
}
