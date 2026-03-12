package rbi

import (
	"bytes"
	"fmt"

	"go.etcd.io/bbolt"
)

func hasExecHooks[K ~string | ~uint64, V any](beforeStore []beforeStoreFunc[K, V], beforeCommit []beforeCommitFunc[K, V]) bool {
	return len(beforeStore) > 0 || len(beforeCommit) > 0
}

func runBeforeProcessHooks[K ~string | ~uint64, V any](id K, newVal *V, hooks []beforeProcessFunc[K, V]) error {
	for _, fn := range hooks {
		if err := fn(id, newVal); err != nil {
			return err
		}
	}
	return nil
}

func runBeforeStoreHooks[K ~string | ~uint64, V any](id K, oldVal, newVal *V, hooks []beforeStoreFunc[K, V]) error {
	for _, fn := range hooks {
		if err := fn(id, oldVal, newVal); err != nil {
			return err
		}
	}
	return nil
}

func runBeforeCommitHooks[K ~string | ~uint64, V any](tx *bbolt.Tx, id K, oldVal, newVal *V, hooks []beforeCommitFunc[K, V]) error {
	for _, fn := range hooks {
		if err := fn(tx, id, oldVal, newVal); err != nil {
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
		return nil, fmt.Errorf("clone func returned nil")
	}
	return cloned, nil
}

func (db *DB[K, V]) snapshotValueBytes(v *V) ([]byte, error) {
	b := getEncodeBuf()
	defer releaseEncodeBuf(b)

	if err := db.encode(v, b); err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}
	return append([]byte(nil), b.Bytes()...), nil
}

func (db *DB[K, V]) rebuildStoredValueFromBaseline(id K, oldVal *V, baseline []byte, hooks []beforeStoreFunc[K, V]) (*V, []byte, error) {
	newVal, err := db.decode(baseline)
	if err != nil {
		return nil, nil, fmt.Errorf("decode prepared value: %w", err)
	}
	if err = runBeforeStoreHooks(id, oldVal, newVal, hooks); err != nil {
		return nil, nil, err
	}

	b := getEncodeBuf()
	defer releaseEncodeBuf(b)

	if err = db.encode(newVal, b); err != nil {
		return nil, nil, fmt.Errorf("encode: %w", err)
	}
	return newVal, append([]byte(nil), b.Bytes()...), nil
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
// For string keys, iteration order follows internal key index order,
// not lexicographic order; seek is applied only as a prefix filter.
func (db *DB[K, V]) ScanKeys(seek K, fn func(K) (bool, error)) error {
	if err := db.beginOp(); err != nil {
		return err
	}
	defer db.endOp()

	universe, owned := db.snapshotUniverseView()
	if owned {
		defer releaseRoaringBuf(universe)
	}

	iter := universe.Iterator()

	for iter.HasNext() {
		idx := iter.Next()
		key, ok := db.keyFromIdx(idx)
		if !ok {
			return fmt.Errorf("%w: %v", ErrNoValidKeyIndex, idx)
		}
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
// provided, the input value must already be msgpack-encodable because RBI
// falls back to encode/decode snapshotting before the hook runs. CloneFunc can
// also be used as a faster cloning path when the caller can produce an
// independent copy more cheaply than msgpack snapshotting. If any hook returns
// an error, the transaction is rolled back and the error is returned.
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
	var err error

	if db.combiner.enabled {
		if err, handled := db.tryQueueSetCombine(id, newVal, cfg.beforeStore, cfg.beforeCommit, cfg.cloneValue); handled {
			return err
		}
	}

	var (
		storedVal = newVal
		payload   []byte
		baseline  []byte
	)
	if len(cfg.beforeStore) == 0 {
		b := getEncodeBuf()
		defer releaseEncodeBuf(b)
		if err := db.encode(newVal, b); err != nil {
			return fmt.Errorf("encode: %w", err)
		}
		payload = b.Bytes()
	} else {
		if cfg.cloneValue != nil {
			if storedVal, err = cloneBeforeStoreValue(id, newVal, cfg.cloneValue); err != nil {
				return err
			}
		} else {
			if baseline, err = db.snapshotValueBytes(newVal); err != nil {
				return err
			}
		}
	}

	tx, err := db.bolt.Begin(true)
	if err != nil {
		return fmt.Errorf("tx error: %w", err)
	}
	defer rollback(tx)

	key := db.keyFromID(id)

	bucket := tx.Bucket(db.bucket)
	if bucket == nil {
		return fmt.Errorf("bucket does not exist")
	}

	var oldVal *V
	if prev := bucket.Get(key); prev != nil {
		if oldVal, err = db.decode(prev); err != nil {
			return fmt.Errorf("decode: %w", err)
		}
	}

	if len(cfg.beforeStore) > 0 {
		if cfg.cloneValue != nil {
			if err = runBeforeStoreHooks(id, oldVal, storedVal, cfg.beforeStore); err != nil {
				return err
			}
			b := getEncodeBuf()
			defer releaseEncodeBuf(b)
			if err = db.encode(storedVal, b); err != nil {
				return fmt.Errorf("encode: %w", err)
			}
			payload = b.Bytes()
		} else {
			if storedVal, payload, err = db.rebuildStoredValueFromBaseline(id, oldVal, baseline, cfg.beforeStore); err != nil {
				return err
			}
		}
	}

	bucket.FillPercent = db.options.BucketFillPercent

	if err = bucket.Put(key, payload); err != nil {
		return fmt.Errorf("put: %w", err)
	}

	if err = runBeforeCommitHooks(tx, id, oldVal, storedVal, cfg.beforeCommit); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed.Load() {
		return ErrClosed
	}

	idx, idxCreated := db.idxFromIDWithCreated(id)
	cleanupCreated := db.strkey && idxCreated && oldVal == nil
	cleanupOnErr := true
	if cleanupCreated {
		defer func() {
			if cleanupOnErr {
				db.rollbackCreatedStrIdx(id, idx)
			}
		}()
	}

	modified := db.getModifiedIndexedFields(oldVal, storedVal)
	if db.hasUnique {
		if err = db.checkUniqueOnWrite(idx, oldVal, storedVal, modified); err != nil {
			return err
		}
	}
	if err = advanceBucketSequence(bucket); err != nil {
		return err
	}

	txID := uint64(tx.ID())
	db.markPending(txID)

	if err = db.commit(tx, "set"); err != nil {
		db.clearPending(txID)
		return err
	}
	db.publishWriteDelta(txID, idx, oldVal, storedVal, modified)
	cleanupOnErr = false
	return nil
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
// If BeforeStore is used and CloneFunc is not provided, each input value must
// already be msgpack-encodable because RBI falls back to encode/decode
// snapshotting before the hook runs. CloneFunc can also be used as a faster
// cloning path when the caller can produce an independent copy more cheaply
// than msgpack snapshotting.
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
	if len(ids) == 1 {
		return db.Set(ids[0], newVals[0], execOpts...)
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
	var err error

	var getbuf func() *bytes.Buffer
	var payloads [][]byte
	storedVals := newVals
	if len(cfg.beforeStore) == 0 || cfg.cloneValue != nil {
		// bbolt requires that value (bytes) remain valid for the life of the transaction

		bufs := make([]*bytes.Buffer, 0, len(ids))

		if len(ids) < 1024 {
			defer func() {
				for _, buf := range bufs {
					if buf != nil {
						releaseEncodeBuf(buf)
					}
				}
			}()
			getbuf = func() *bytes.Buffer {
				b := getEncodeBuf()
				bufs = append(bufs, b)
				return b
			}

		} else {
			getbuf = func() *bytes.Buffer {
				b := new(bytes.Buffer)
				bufs = append(bufs, b)
				return b
			}
		}

		if len(cfg.beforeStore) == 0 {
			for _, v := range newVals {
				b := getbuf()
				if err := db.encode(v, b); err != nil {
					return fmt.Errorf("encode: %w", err)
				}
			}

			payloads = make([][]byte, len(bufs))
			for i := range bufs {
				payloads[i] = bufs[i].Bytes()
			}
		} else {
			storedVals = make([]*V, len(newVals))
			for i, v := range newVals {
				if storedVals[i], err = cloneBeforeStoreValue(ids[i], v, cfg.cloneValue); err != nil {
					return err
				}
			}
		}
	} else {
		payloads = make([][]byte, len(newVals))
		storedVals = make([]*V, len(newVals))
		for i, v := range newVals {
			if payloads[i], err = db.snapshotValueBytes(v); err != nil {
				return err
			}
		}
	}

	tx, txerr := db.bolt.Begin(true)
	if txerr != nil {
		return fmt.Errorf("tx error: %w", txerr)
	}
	defer rollback(tx)

	oldVals := make([]*V, len(ids))

	bucket := tx.Bucket(db.bucket)
	if bucket == nil {
		return fmt.Errorf("bucket does not exist")
	}
	bucket.FillPercent = db.options.BucketFillPercent

	for i, id := range ids {

		key := db.keyFromID(id)

		var oldVal *V
		if prev := bucket.Get(key); prev != nil {
			if oldVal, err = db.decode(prev); err != nil {
				return fmt.Errorf("decode: %w", err)
			}
		}
		oldVals[i] = oldVal

		var payload []byte
		if len(payloads) > 0 {
			payload = payloads[i]
		}
		if len(cfg.beforeStore) > 0 {
			if cfg.cloneValue != nil {
				if err = runBeforeStoreHooks(id, oldVal, storedVals[i], cfg.beforeStore); err != nil {
					return err
				}
				b := getbuf()
				if err = db.encode(storedVals[i], b); err != nil {
					return fmt.Errorf("encode: %w", err)
				}
				payload = b.Bytes()
			} else {
				if storedVals[i], payload, err = db.rebuildStoredValueFromBaseline(id, oldVal, payloads[i], cfg.beforeStore); err != nil {
					return err
				}
			}
		}

		if err = bucket.Put(key, payload); err != nil {
			return fmt.Errorf("put: %w", err)
		}
	}

	if len(cfg.beforeCommit) > 0 {
		for i, id := range ids {
			if err = runBeforeCommitHooks(tx, id, oldVals[i], storedVals[i], cfg.beforeCommit); err != nil {
				return err
			}
		}
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed.Load() {
		return ErrClosed
	}

	idxs, idxCreated := db.idxsFromIDWithCreated(ids)
	var cleanupCreated map[uint64]K
	if db.strkey {
		cleanupCreated = make(map[uint64]K, len(ids))
		for i := range idxs {
			if idxCreated[i] && oldVals[i] == nil {
				if _, seen := cleanupCreated[idxs[i]]; !seen {
					cleanupCreated[idxs[i]] = ids[i]
				}
			}
		}
	}
	cleanupOnErr := true
	if len(cleanupCreated) > 0 {
		defer func() {
			if !cleanupOnErr {
				return
			}
			for idx, id := range cleanupCreated {
				db.rollbackCreatedStrIdx(id, idx)
			}
		}()
	}

	modified := make([][]string, len(idxs))
	for i := range idxs {
		modified[i] = db.getModifiedIndexedFields(oldVals[i], storedVals[i])
	}

	if db.hasUnique {
		if err = db.checkUniqueOnWriteMulti(idxs, oldVals, storedVals, modified); err != nil {
			return err
		}
	}
	if err = advanceBucketSequence(bucket); err != nil {
		return err
	}

	txID := uint64(tx.ID())
	db.markPending(txID)

	if err = db.commit(tx, "batch_set"); err != nil {
		db.clearPending(txID)
		return err
	}
	db.publishWriteDeltaBatch(txID, idxs, oldVals, storedVals, modified)
	cleanupOnErr = false
	return nil
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
	if err := db.beginOp(); err != nil {
		return err
	}
	defer db.endOp()

	if len(fields) == 0 {
		return nil
	}
	cfg := db.resolveExecOptions(execOpts)
	ignoreUnknown := !cfg.patchStrict
	if db.combiner.enabled {
		if err, handled := db.tryQueuePatchCombine(id, fields, ignoreUnknown, cfg.beforeProcess, cfg.beforeStore, cfg.beforeCommit); handled {
			return err
		}
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

	var oldVal *V
	oldBytes := bucket.Get(key)
	if oldBytes == nil {
		return nil
	}
	idx := db.idxFromID(id)

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
	if err = runBeforeProcessHooks(id, newVal, cfg.beforeProcess); err != nil {
		return err
	}

	if err = runBeforeStoreHooks(id, oldVal, newVal, cfg.beforeStore); err != nil {
		return err
	}

	b := getEncodeBuf()
	defer releaseEncodeBuf(b)

	if err = db.encode(newVal, b); err != nil {
		return fmt.Errorf("encode: %w", err)
	}

	bucket.FillPercent = db.options.BucketFillPercent

	if err = bucket.Put(key, b.Bytes()); err != nil {
		return fmt.Errorf("put: %w", err)
	}

	if err = runBeforeCommitHooks(tx, id, oldVal, newVal, cfg.beforeCommit); err != nil {
		return err
	}

	modified := db.getModifiedIndexedFields(oldVal, newVal)
	delta := db.prepareSnapshotWriteDelta(idx, oldVal, newVal, modified)
	deltaOwned := true
	defer func() {
		if deltaOwned {
			delta.release()
		}
	}()

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed.Load() {
		return ErrClosed
	}

	if db.hasUnique {
		if err = db.checkUniqueOnWrite(idx, oldVal, newVal, modified); err != nil {
			return err
		}
	}
	if err = advanceBucketSequence(bucket); err != nil {
		return err
	}

	txID := uint64(tx.ID())
	db.markPending(txID)

	if err = db.commit(tx, "patch"); err != nil {
		db.clearPending(txID)
		return err
	}
	db.publishPreparedSnapshotWithAccumDeltaNoLock(txID, &delta)
	deltaOwned = false
	return nil
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
	if len(ids) == 1 {
		return db.patch(ids[0], patch, execOpts...)
	}
	if err := db.beginOp(); err != nil {
		return err
	}
	defer db.endOp()

	cfg := db.resolveExecOptions(execOpts)
	ignoreUnknown := !cfg.patchStrict

	var getbuf func() *bytes.Buffer

	// bbolt requires that value (bytes) remain valid for the life of the transaction

	if len(ids) < 1024 {

		bufs := make([]*bytes.Buffer, 0, len(ids))
		defer func() {
			for _, buf := range bufs {
				if buf != nil {
					releaseEncodeBuf(buf)
				}
			}
		}()
		getbuf = func() *bytes.Buffer {
			b := getEncodeBuf()
			bufs = append(bufs, b)
			return b
		}

	} else {
		getbuf = func() *bytes.Buffer { return new(bytes.Buffer) }
	}

	tx, txerr := db.bolt.Begin(true)
	if txerr != nil {
		return fmt.Errorf("tx error: %w", txerr)
	}
	defer rollback(tx)

	oldVals := make([]*V, 0, len(ids))
	newVals := make([]*V, 0, len(ids))
	idxs := make([]uint64, 0, len(ids))
	foundIDs := make([]K, 0, len(ids))

	bucket := tx.Bucket(db.bucket)
	if bucket == nil {
		return fmt.Errorf("bucket does not exist")
	}
	bucket.FillPercent = db.options.BucketFillPercent

	var err error
	for _, id := range ids {
		key := db.keyFromID(id)

		oldBytes := bucket.Get(key)
		if oldBytes == nil {
			continue
		}

		var (
			oldVal *V
			newVal *V
		)
		if oldVal, err = db.decode(oldBytes); err != nil {
			return fmt.Errorf("failed to decode existing value for id %v: %w", id, err)
		}
		oldVals = append(oldVals, oldVal)

		if newVal, err = db.decode(oldBytes); err != nil {
			return fmt.Errorf("failed to re-decode value for patching id %v: %w", id, err)
		}

		if err = db.applyPatch(newVal, patch, ignoreUnknown); err != nil {
			return fmt.Errorf("failed to apply patch for id %v: %w", id, err)
		}
		if err = runBeforeProcessHooks(id, newVal, cfg.beforeProcess); err != nil {
			return err
		}

		if err = runBeforeStoreHooks(id, oldVal, newVal, cfg.beforeStore); err != nil {
			return err
		}
		newVals = append(newVals, newVal)

		b := getbuf()
		if err = db.encode(newVal, b); err != nil {
			return fmt.Errorf("encode: %w", err)
		}
		if err = bucket.Put(key, b.Bytes()); err != nil {
			return err
		}

		idxs = append(idxs, db.idxFromID(id))
		foundIDs = append(foundIDs, id)
	}

	if len(foundIDs) == 0 {
		return nil
	}

	if len(cfg.beforeCommit) > 0 {
		for i, id := range foundIDs {
			if err = runBeforeCommitHooks(tx, id, oldVals[i], newVals[i], cfg.beforeCommit); err != nil {
				return err
			}
		}
	}

	modified := make([][]string, len(idxs))
	for i := range idxs {
		modified[i] = db.getModifiedIndexedFields(oldVals[i], newVals[i])
	}
	delta := db.prepareSnapshotWriteDeltaBatch(idxs, oldVals, newVals, modified)
	deltaOwned := true
	defer func() {
		if deltaOwned {
			delta.release()
		}
	}()

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed.Load() {
		return ErrClosed
	}

	if db.hasUnique {
		if err = db.checkUniqueOnWriteMulti(idxs, oldVals, newVals, modified); err != nil {
			return err
		}
	}
	if err = advanceBucketSequence(bucket); err != nil {
		return err
	}

	txID := uint64(tx.ID())
	db.markPending(txID)

	if err = db.commit(tx, "batch_patch"); err != nil {
		db.clearPending(txID)
		return err
	}
	db.publishPreparedSnapshotWithAccumDeltaNoLock(txID, &delta)
	deltaOwned = false
	return nil
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

	if db.combiner.enabled {
		if err, handled := db.tryQueueDeleteCombine(id, cfg.beforeCommit); handled {
			return err
		}
	}

	tx, err := db.bolt.Begin(true)
	if err != nil {
		return fmt.Errorf("tx error: %w", err)
	}
	defer rollback(tx)

	bucket := tx.Bucket(db.bucket)
	if bucket == nil {
		return fmt.Errorf("bucket does not exist")
	}
	bucket.FillPercent = db.options.BucketFillPercent

	key := db.keyFromID(id)

	var oldVal *V
	if prev := bucket.Get(key); prev != nil {
		if oldVal, err = db.decode(prev); err != nil {
			return fmt.Errorf("decode: %w", err)
		}
	} else {
		return nil
	}

	idx := db.idxFromID(id)

	if err = bucket.Delete(key); err != nil {
		return fmt.Errorf("delete: %w", err)
	}

	if err = runBeforeCommitHooks(tx, id, oldVal, nil, cfg.beforeCommit); err != nil {
		return err
	}

	modified := db.getModifiedIndexedFields(oldVal, nil)
	delta := db.prepareSnapshotWriteDelta(idx, oldVal, nil, modified)
	deltaOwned := true
	defer func() {
		if deltaOwned {
			delta.release()
		}
	}()

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed.Load() {
		return ErrClosed
	}

	if err = advanceBucketSequence(bucket); err != nil {
		return err
	}

	txID := uint64(tx.ID())
	db.markPending(txID)

	if err = db.commit(tx, "delete"); err != nil {
		db.clearPending(txID)
		return err
	}
	db.publishPreparedSnapshotWithAccumDeltaNoLock(txID, &delta)
	deltaOwned = false
	return nil
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
	if len(ids) == 1 {
		return db.Delete(ids[0], execOpts...)
	}
	if err := db.beginOp(); err != nil {
		return err
	}
	defer db.endOp()

	cfg := db.resolveExecOptions(execOpts)

	tx, txerr := db.bolt.Begin(true)
	if txerr != nil {
		return fmt.Errorf("tx error: %w", txerr)
	}
	defer rollback(tx)

	oldVals := make([]*V, 0, len(ids))
	idxs := make([]uint64, 0, len(ids))
	foundIDs := make([]K, 0, len(ids))

	bucket := tx.Bucket(db.bucket)
	if bucket == nil {
		return fmt.Errorf("bucket does not exist")
	}
	bucket.FillPercent = db.options.BucketFillPercent

	var err error
	for _, id := range ids {

		key := db.keyFromID(id)

		var oldVal *V
		if prev := bucket.Get(key); prev != nil {
			if oldVal, err = db.decode(prev); err != nil {
				return fmt.Errorf("decode: %w", err)
			}
		} else {
			continue
		}
		oldVals = append(oldVals, oldVal)

		if err = bucket.Delete(key); err != nil {
			return fmt.Errorf("delete: %w", err)
		}

		idxs = append(idxs, db.idxFromID(id))
		foundIDs = append(foundIDs, id)
	}

	if len(foundIDs) == 0 {
		return nil
	}

	if len(cfg.beforeCommit) > 0 {
		for i, id := range foundIDs {
			if err = runBeforeCommitHooks(tx, id, oldVals[i], nil, cfg.beforeCommit); err != nil {
				return err
			}
		}
	}

	modified := make([][]string, len(idxs))
	for i := range idxs {
		modified[i] = db.getModifiedIndexedFields(oldVals[i], nil)
	}
	delta := db.prepareSnapshotWriteDeltaBatch(idxs, oldVals, nil, modified)
	deltaOwned := true
	defer func() {
		if deltaOwned {
			delta.release()
		}
	}()

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed.Load() {
		return ErrClosed
	}

	if err = advanceBucketSequence(bucket); err != nil {
		return err
	}

	txID := uint64(tx.ID())
	db.markPending(txID)

	if err = db.commit(tx, "batch_delete"); err != nil {
		db.clearPending(txID)
		return err
	}
	db.publishPreparedSnapshotWithAccumDeltaNoLock(txID, &delta)
	deltaOwned = false
	return nil
}
