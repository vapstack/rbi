package rbi

import (
	"bytes"
	"fmt"

	"go.etcd.io/bbolt"
)

// Get returns the value stored by id or nil if key was not found.
func (db *DB[K, V]) Get(id K) (*V, error) {

	if db.closed.Load() {
		return nil, ErrClosed
	}

	tx, err := db.bolt.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("tx error: %w", err)
	}
	defer rollback(tx)

	v := tx.Bucket(db.bucket).Get(db.keyFromID(id))
	if v == nil {
		return nil, nil
	}
	r, err := db.decode(v)
	if err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return r, nil
}

// GetMany retrieves multiple values by their IDs in a single read transaction.
// The returned slice has the same length as ids; any missing keys have a nil
// entry at the corresponding index.
func (db *DB[K, V]) GetMany(ids ...K) ([]*V, error) {

	if db.closed.Load() {
		return nil, ErrClosed
	}
	if len(ids) == 0 {
		return nil, nil
	}

	tx, err := db.bolt.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("tx error: %w", err)
	}
	defer rollback(tx)
	return db.getManyTx(tx, ids...)
}

// getManyTx retrieves multiple values by IDs using an existing read transaction.
func (db *DB[K, V]) getManyTx(tx *bbolt.Tx, ids ...K) ([]*V, error) {
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

// ScanKeys iterates over keys in the in-memory index snapshot and calls fn for
// each key greater than or equal to seek.
//
// The scan stops when fn returns false or a non-nil error. The scan does not
// open a Bolt transaction and may not reflect concurrent writes.
//
// For string keys, iteration order follows internal key index order,
// not lexicographic order; seek is applied only as a prefix filter.
func (db *DB[K, V]) ScanKeys(seek K, fn func(K) (bool, error)) error {
	db.mu.RLock()
	closed := db.closed.Load()
	noIndex := db.noIndex.Load()
	universe := db.universe.Clone()
	db.mu.RUnlock()

	if closed {
		return ErrClosed
	}
	if noIndex {
		return ErrIndexDisabled
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

	if db.closed.Load() {
		return ErrClosed
	}

	tx, err := db.bolt.Begin(false)
	if err != nil {
		return fmt.Errorf("tx error: %w", err)
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

	if db.closed.Load() {
		return ErrClosed
	}

	tx, err := db.bolt.Begin(false)
	if err != nil {
		return fmt.Errorf("tx error: %w", err)
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
		return fmt.Errorf("encode: %w", err)
	}

	tx, err := db.bolt.Begin(true)
	if err != nil {
		return fmt.Errorf("tx error: %w", err)
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
			return fmt.Errorf("decode: %w", err)
		}
	}

	bucket.FillPercent = BucketFillPercent

	if err = bucket.Put(key, b.Bytes()); err != nil {
		return fmt.Errorf("put: %w", err)
	}

	for _, fn := range fns {
		if fn != nil {
			if err = fn(tx, id, oldVal, newVal); err != nil {
				return err
			}
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
// After a successful commit, the in-memory index is updated for all
// modified keys unless indexing is disabled.
//
// SetMany allocates a buffer for each encoded value.
// Storing a large number of values will consume a proportional amount of memory.
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

	var getbuf func() *bytes.Buffer

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

	for _, v := range newVals {
		b := getbuf()
		if err := db.encode(v, b); err != nil {
			return fmt.Errorf("encode: %w", err)
		}
	}

	tx, txerr := db.bolt.Begin(true)
	if txerr != nil {
		return fmt.Errorf("tx error: %w", txerr)
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
				return fmt.Errorf("decode: %w", err)
			}
		}
		oldVals[i] = oldVal

		if err = bucket.Put(key, bufs[i].Bytes()); err != nil {
			return fmt.Errorf("put: %w", err)
		}
	}

	if len(fns) > 0 {
		for i, id := range ids {
			for _, fn := range fns {
				if fn != nil {
					if err = fn(tx, id, oldVals[i], newVals[i]); err != nil {
						return err
					}
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

// Patch applies a partial update to the value stored under the given id,
// updating only the fields listed in patch.
//
// Unknown field names in patch are silently ignored. All fields, indexed or
// not, are eligible to be patched. Patch attempts to convert the provided
// values to the appropriate field type, if possible.
// If conversion fails for any field, Patch returns an error and no changes are committed.
//
// If no item is found for the specified id, ErrRecordNotFound is returned
// and callbacks are not invoked.
//
// All PreCommitFunc fns are invoked with the original (old) and patched (new) values
// before commit. After a successful commit, the in-memory index is updated.
func (db *DB[K, V]) Patch(id K, patch []Field, fns ...PreCommitFunc[K, V]) error {
	return db.patch(id, patch, true, false, fns...)
}

// PatchStrict is like Patch, but returns an error if the patch contains field names
// that cannot be resolved to a known struct field (by name, db or json tag).
func (db *DB[K, V]) PatchStrict(id K, patch []Field, fns ...PreCommitFunc[K, V]) error {
	return db.patch(id, patch, false, false, fns...)
}

// PatchIfExists is like Patch, but missing records are skipped.
func (db *DB[K, V]) PatchIfExists(id K, patch []Field, fns ...PreCommitFunc[K, V]) error {
	return db.patch(id, patch, true, true, fns...)
}

// PatchStrictIfExists is like PatchStrict, but missing records are skipped.
func (db *DB[K, V]) PatchStrictIfExists(id K, patch []Field, fns ...PreCommitFunc[K, V]) error {
	return db.patch(id, patch, false, true, fns...)
}

func (db *DB[K, V]) patch(id K, fields []Field, ignoreUnknown bool, allowMissing bool, fns ...PreCommitFunc[K, V]) error {

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

	var oldVal *V
	oldBytes := bucket.Get(key)
	if oldBytes == nil {
		if allowMissing {
			return nil
		}
		return ErrRecordNotFound
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

	b := getEncodeBuf()
	defer releaseEncodeBuf(b)

	if err = db.encode(newVal, b); err != nil {
		return fmt.Errorf("encode: %w", err)
	}

	bucket.FillPercent = BucketFillPercent

	if err = bucket.Put(key, b.Bytes()); err != nil {
		return fmt.Errorf("put: %w", err)
	}

	for _, fn := range fns {
		if fn != nil {
			if err = fn(tx, id, oldVal, newVal); err != nil {
				return err
			}
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
// Non-existent IDs are skipped and do not trigger callbacks.
//
// PatchMany allocates a buffer for each encoded value.
// Patching a large number of values will consume a proportional amount of memory.
func (db *DB[K, V]) PatchMany(ids []K, patch []Field, fns ...PreCommitFunc[K, V]) error {
	return db.patchMany(ids, patch, true, fns...)
}

// PatchManyStrict is like PatchMany, but returns an error if the patch contains field names
// that cannot be resolved to a known struct field (by name, db or json tag).
func (db *DB[K, V]) PatchManyStrict(ids []K, patch []Field, fns ...PreCommitFunc[K, V]) error {
	return db.patchMany(ids, patch, false, fns...)
}

func (db *DB[K, V]) patchMany(ids []K, patch []Field, ignoreUnknown bool, fns ...PreCommitFunc[K, V]) error {

	if len(ids) == 0 || len(patch) == 0 {
		return nil
	}
	if len(ids) == 1 {
		return db.patch(ids[0], patch, ignoreUnknown, true, fns...)
	}

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
	bucket.FillPercent = BucketFillPercent

	var err error
	for _, id := range ids {
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
		oldVals = append(oldVals, oldVal)

		if newVal, err = db.decode(oldBytes); err != nil {
			return fmt.Errorf("failed to re-decode value for patching id %v: %w", id, err)
		}

		if err = db.applyPatch(newVal, patch, ignoreUnknown); err != nil {
			return fmt.Errorf("failed to apply patch for id %v: %w", id, err)
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

	if len(fns) > 0 {
		for i, id := range foundIDs {
			for _, fn := range fns {
				if fn != nil {
					if err = fn(tx, id, oldVals[i], newVals[i]); err != nil {
						return err
					}
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
// If the record does not exist, Delete is a no-op and no callbacks are invoked.
func (db *DB[K, V]) Delete(id K, fns ...PreCommitFunc[K, V]) error {

	tx, err := db.bolt.Begin(true)
	if err != nil {
		return fmt.Errorf("tx error: %w", err)
	}
	defer rollback(tx)

	bucket := tx.Bucket(db.bucket)
	if bucket == nil {
		return fmt.Errorf("bucket does not exist")
	}
	bucket.FillPercent = BucketFillPercent

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

	for _, fn := range fns {
		if fn != nil {
			if err = fn(tx, id, oldVal, nil); err != nil {
				return err
			}
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
// Missing IDs are skipped and do not trigger callbacks.
func (db *DB[K, V]) DeleteMany(ids []K, fns ...PreCommitFunc[K, V]) error {

	if len(ids) == 0 {
		return nil
	}
	if len(ids) == 1 {
		return db.Delete(ids[0], fns...)
	}

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
	bucket.FillPercent = BucketFillPercent

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

	if len(fns) > 0 {
		for i, id := range foundIDs {
			for _, fn := range fns {
				if fn != nil {
					if err = fn(tx, id, oldVals[i], nil); err != nil {
						return err
					}
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
