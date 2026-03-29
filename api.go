package rbi

import (
	"fmt"
	"unsafe"

	"github.com/vapstack/rbi/internal/posting"
	"go.etcd.io/bbolt"
)

func runBeforeProcessHooks[K ~string | ~uint64, V any](id K, newVal *V, hooks []beforeProcessFunc[K, V]) error {
	for _, fn := range hooks {
		if err := fn(id, newVal); err != nil {
			return err
		}
	}
	return nil
}

func cloneBeforeStoreValue[K ~string | ~uint64, V any](id K, v *V, cloneFn func(K, *V) *V) (*V, error) {
	if cloneFn == nil || v == nil {
		return v, nil
	}
	cloned := cloneFn(id, v)
	if cloned == nil {
		return nil, fmt.Errorf("clone returned nil")
	}
	return cloned, nil
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

	v := bucket.Get(db.keyFromID(id))
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
	for i, id := range ids {
		v := bucket.Get(db.keyFromID(id))
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

// batchGetTxCompact retrieves multiple values by IDs using an existing read
// transaction and returns only existing records (without nil holes).
func (db *DB[K, V]) batchGetTxCompact(tx *bbolt.Tx, ids []K) ([]*V, error) {
	bucket := tx.Bucket(db.bucket)
	if bucket == nil {
		return nil, fmt.Errorf("bucket does not exist")
	}
	if len(ids) == 0 {
		return nil, nil
	}

	out := make([]*V, 0, len(ids))
	for _, id := range ids {
		v := bucket.Get(db.keyFromID(id))
		if v == nil {
			continue
		}
		value, err := db.decode(v)
		if err != nil {
			return out, fmt.Errorf("decode: %w", err)
		}
		out = append(out, value)
	}
	return out, nil
}

// ScanKeys iterates over keys in the in-memory index snapshot and calls fn for
// each key greater than or equal to seek.
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

	snap := db.getSnapshot()
	universe := snap.universe
	iter := universe.Iter()
	defer iter.Release()

	if db.strkey {
		return db.scanStringKeys(snap.strmap, universe, iter, seek, fn)
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

func (db *DB[K, V]) scanStringKeys(snap *strMapSnapshot, universe posting.List, iter posting.Iterator, seek K, fn func(K) (bool, error)) error {
	if snap == nil || universe.IsEmpty() {
		return nil
	}

	seekStr := *(*string)(unsafe.Pointer(&seek))

	emit := func(s string) (bool, error) {
		if s < seekStr {
			return true, nil
		}
		key := *(*K)(unsafe.Pointer(&s))
		return fn(key)
	}

	card := universe.Cardinality()
	minIdx, hasMin := universe.Minimum()
	maxIdx, hasMax := universe.Maximum()
	if card == snap.Next && card > 0 && hasMin && hasMax && minIdx == 1 && maxIdx == snap.Next {
		if len(snap.readDirs) == 0 {
			for idx := uint64(1); idx <= snap.Next; idx++ {
				s, ok := snap.getStringNoLock(idx)
				if !ok {
					return fmt.Errorf("%w: %v", ErrNoValidKeyIndex, idx)
				}
				cont, err := emit(s)
				if err != nil {
					return err
				}
				if !cont {
					return nil
				}
			}
			return nil
		}

		pageCount := strMapReadPageCount(snap.Next)
		for page := 0; page < pageCount; page++ {
			readPage := snap.readPageAtNoLock(page)
			if readPage == nil {
				pageStart, _ := strMapReadPageBounds(page, snap.Next)
				return fmt.Errorf("%w: %v", ErrNoValidKeyIndex, pageStart)
			}
			for idx := readPage.Start; idx <= readPage.Next; idx++ {
				s, ok := readPage.getStringNoLock(idx)
				if !ok {
					return fmt.Errorf("%w: %v", ErrNoValidKeyIndex, idx)
				}
				cont, err := emit(s)
				if err != nil {
					return err
				}
				if !cont {
					return nil
				}
			}
		}
		return nil
	}

	var (
		pageIdx  = -1
		readPage *strMapReadPage
	)
	for iter.HasNext() {
		idx := iter.Next()
		s := ""
		ok := false
		if len(snap.readDirs) > 0 {
			curPage := strMapReadPageIndex(idx)
			if curPage != pageIdx {
				pageIdx = curPage
				readPage = snap.readPageAtNoLock(curPage)
			}
			if readPage != nil {
				s, ok = readPage.getStringNoLock(idx)
			}
		}
		if !ok {
			s, ok = snap.getStringNoLock(idx)
		}
		if !ok {
			return fmt.Errorf("%w: %v", ErrNoValidKeyIndex, idx)
		}

		cont, err := emit(s)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return nil
}

// SeqScan performs a sequential scan over all records starting at the given
// key (inclusive), decoding each value and passing it to the provided fn.
// SeqScan stops reading when the fn returns false or a non-nil error.
// The scan runs inside a read-only transaction which remains open for the
// duration of the scan.
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

	key, value := c.Seek(db.keyFromID(seek))
	if key == nil {
		return nil
	}
	val, err := db.decode(value)
	if err != nil {
		return fmt.Errorf("decode: %w", err)
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
			return fmt.Errorf("decode: %w", err)
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
// BeforeProcess hooks, if any, run on the caller-owned input value before RBI
// starts encoding, cloning, queueing, or transactional work for Set.
//
// The value is msgpack-encoded and written inside a single write
// transaction. Any existing value for the key is decoded and passed as oldValue
// to BeforeStore and BeforeCommit hooks. BeforeStore may modify the stored
// value before it is encoded. If BeforeStore is used and CloneFunc is not
// provided, RBI first checks whether *V implements Clone() *V and uses it when
// available. Otherwise the input value must already be msgpack-encodable
// because RBI falls back to encode/decode snapshotting before the hook runs.
// CloneFunc can also be used as a faster cloning path when the caller can
// produce an independent copy more cheaply than msgpack snapshotting. If any
// hook returns an error, the transaction is rolled back and the error is
// returned.
func (db *DB[K, V]) Set(id K, newVal *V, execOpts ...ExecOption[K, V]) error {
	if newVal == nil {
		return ErrNilValue
	}

	cfg := db.resolveExecOptions(execOpts)
	if err := runBeforeProcessHooks(id, newVal, cfg.beforeProcess); err != nil {
		return err
	}
	if err := db.beginOp(); err != nil {
		return err
	}
	defer db.endOp()

	req, err := db.buildSetAutoBatchRequest(id, newVal, cfg.beforeStore, cfg.beforeCommit, cfg.cloneValue)
	if err != nil {
		return err
	}
	return db.submitAutoBatchRequests([]*autoBatchRequest[K, V]{req}, cfg.noBatch)
}

// BatchSet stores multiple values under the provided IDs in a single write
// transaction. The length of the ids and values must be equal.
//
// BeforeProcess hooks, if any, run on each caller-owned input value before RBI
// starts encoding, cloning, queueing, or transactional work for BatchSet.
//
// For each key, any existing value is decoded and passed as oldValue to all
// configured BeforeStore and BeforeCommit hooks. If an error is encountered during any of
// the processing steps, the transaction is rolled back and the error is returned.
// If BeforeStore is used and CloneFunc is not provided, RBI first checks
// whether *V implements Clone() *V and uses it when available. Otherwise each
// input value must already be msgpack-encodable because RBI falls back to
// encode/decode snapshotting before the hook runs. CloneFunc can also be used
// as a faster cloning path when the caller can produce an independent copy
// more cheaply than msgpack snapshotting.
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
	for i := range newVals {
		if err := runBeforeProcessHooks(ids[i], newVals[i], cfg.beforeProcess); err != nil {
			return err
		}
	}
	if err := db.beginOp(); err != nil {
		return err
	}
	defer db.endOp()

	reqs := make([]*autoBatchRequest[K, V], len(ids))
	for i := range ids {
		req, err := db.buildSetAutoBatchRequest(ids[i], newVals[i], cfg.beforeStore, cfg.beforeCommit, cfg.cloneValue)
		if err != nil {
			return err
		}
		reqs[i] = req
	}
	return db.submitAutoBatchRequests(reqs, true)
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
	return db.patch(id, patch, execOpts...)
}

// patch handles patch.
func (db *DB[K, V]) patch(id K, fields []Field, execOpts ...ExecOption[K, V]) error {
	if len(fields) == 0 {
		return nil
	}
	cfg := db.resolveExecOptions(execOpts)
	ignoreUnknown := !cfg.patchStrict
	if err := db.beginOp(); err != nil {
		return err
	}
	defer db.endOp()

	req := db.buildPatchAutoBatchRequest(id, fields, ignoreUnknown, cfg.beforeProcess, cfg.beforeStore, cfg.beforeCommit)
	return db.submitAutoBatchRequests([]*autoBatchRequest[K, V]{req}, cfg.noBatch)
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
	return db.batchPatch(ids, patch, execOpts...)
}

func (db *DB[K, V]) batchPatch(ids []K, patch []Field, execOpts ...ExecOption[K, V]) error {
	if len(ids) == 0 || len(patch) == 0 {
		return nil
	}
	if err := db.beginOp(); err != nil {
		return err
	}
	defer db.endOp()

	cfg := db.resolveExecOptions(execOpts)
	ignoreUnknown := !cfg.patchStrict

	reqs := make([]*autoBatchRequest[K, V], len(ids))
	for i := range ids {
		reqs[i] = db.buildPatchAutoBatchRequest(ids[i], patch, ignoreUnknown, cfg.beforeProcess, cfg.beforeStore, cfg.beforeCommit)
	}
	return db.submitAutoBatchRequests(reqs, true)
}

// Delete removes the value stored under the given id, if any.
//
// The existing value (if present) is decoded and passed as oldValue to all
// configured BeforeCommit callbacks. If any callback returns an error, the
// operation is aborted. If the record does not exist, Delete is a no-op and no
// callbacks are invoked.
func (db *DB[K, V]) Delete(id K, execOpts ...ExecOption[K, V]) error {
	if err := db.beginOp(); err != nil {
		return err
	}
	defer db.endOp()

	cfg := db.resolveExecOptions(execOpts)
	req := db.buildDeleteAutoBatchRequest(id, cfg.beforeCommit)
	return db.submitAutoBatchRequests([]*autoBatchRequest[K, V]{req}, cfg.noBatch)
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
	if err := db.beginOp(); err != nil {
		return err
	}
	defer db.endOp()

	cfg := db.resolveExecOptions(execOpts)

	reqs := make([]*autoBatchRequest[K, V], len(ids))
	for i := range ids {
		reqs[i] = db.buildDeleteAutoBatchRequest(ids[i], cfg.beforeCommit)
	}
	return db.submitAutoBatchRequests(reqs, true)
}
