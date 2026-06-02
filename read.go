package rbi

import (
	"fmt"

	"github.com/vapstack/rbi/internal/keycodec"
	"go.etcd.io/bbolt"
)

// Get returns the value stored by id or nil if key was not found.
func (db *DB[K, V]) Get(id K) (*V, error) {
	if err := db.unavailableErr(); err != nil {
		return nil, err
	}

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
	if err := db.unavailableErr(); err != nil {
		return nil, err
	}

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
// The scan stops when fn returns false or a non-nil error. ScanKeys pins one
// index snapshot for the whole operation. The scan does not open a Bolt
// transaction and does not reflect concurrent writes.
//
// Other DB operations may technically be called from fn, but they run as
// independent operations. They are not tied to the pinned index snapshot used
// by ScanKeys and may observe a different database state.
//
// For string keys, iteration order follows internal key-index order, not
// lexicographic order. In that mode, seek is still applied as a plain
// `key >= seek` value filter, but not as a lexicographic seek in traversal
// order. As a result, string-key ScanKeys is not suitable for prefix scans,
// resume/pagination cursors, or ordered iteration. Use QueryKeys(PREFIX(...))
// for prefix matching or SeqScan/SeqScanRaw for ordered key traversal.
func (db *DB[K, V]) ScanKeys(seek K, fn func(K) (bool, error)) error {
	if err := db.unavailableErr(); err != nil {
		return err
	}

	if db.index == nil {
		return ErrNoIndex
	}

	if db.strKey {
		return db.index.ScanStringKeys(
			keycodec.UserKeyString(seek),
			ErrNoValidKeyIndex,
			keycodec.UserKeyStringScanFunc(fn),
		)
	}

	return db.index.ScanUintKeys(
		keycodec.UserKeyUint(seek),
		keycodec.UserKeyUintScanFunc(fn),
	)
}

// SeqScan performs a sequential scan over all records starting at the given
// key (inclusive), decoding each value and passing it to the provided fn.
// SeqScan stops reading when the fn returns false or a non-nil error.
// The scan runs inside a read-only transaction which remains open for the
// duration of the scan.
//
// The callback must not call methods on the DB instance. SeqScan keeps the
// read transaction open while fn runs, and re-entering DB from the callback can
// deadlock.
//
// Records passed to fn can optionally be returned back using ReleaseRecords
// to minimize GC pressure.
func (db *DB[K, V]) SeqScan(seek K, fn func(K, *V) (bool, error)) error {
	if err := db.unavailableErr(); err != nil {
		return err
	}

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

	more, err := fn(keycodec.UserKeyFromBytes[K](key, db.strKey), val)
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
		if more, err = fn(keycodec.UserKeyFromBytes[K](key, db.strKey), val); err != nil {
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
// The scan runs inside a read-only transaction which remains open for the
// duration of the scan.
//
// The callback must not call methods on the DB instance. SeqScanRaw keeps the
// read transaction open while fn runs, and re-entering DB from the callback can
// deadlock.
//
// Bytes passed to fn must not be modified.
func (db *DB[K, V]) SeqScanRaw(seek K, fn func(K, []byte) (bool, error)) error {
	if err := db.unavailableErr(); err != nil {
		return err
	}

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

	more, err := fn(keycodec.UserKeyFromBytes[K](key, db.strKey), value)
	if err != nil {
		return err
	}
	for more {
		key, value = c.Next()
		if key == nil {
			return nil
		}
		if more, err = fn(keycodec.UserKeyFromBytes[K](key, db.strKey), value); err != nil {
			return err
		}
	}
	return nil
}
