package rbi

import (
	"fmt"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/rbierrors"
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

	bucket := tx.Bucket(db.dataBucket)
	if bucket == nil {
		return nil, fmt.Errorf("bucket does not exist")
	}

	var keyBuf [8]byte
	v := bucket.Get(keycodec.UserKeyBytesWithBuf(id, db.strKey, &keyBuf))
	if v == nil {
		return nil, nil
	}
	r, err := db.decodeStoredValue(v)
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
	bucket := tx.Bucket(db.dataBucket)
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
		value, err := db.decodeStoredValue(v)
		if err != nil {
			return s, fmt.Errorf("decode: %w", err)
		}
		s[i] = value
	}
	return s, nil
}

// ScanKeys iterates over live keys greater than or equal to seek and calls fn
// for each key. Scan stops when fn returns false or a non-nil error.
//
// Indexed DB iterates the current in-memory snapshot.
// Other modes scan the data bucket inside a read-only transaction which remains
// open for the duration of the scan.
//
// The callback must not call methods on the DB instance.
//
// Numeric keys are returned in numeric order,
// string keys are returned in bbolt lexicographic byte order.
func (db *DB[K, V]) ScanKeys(seek K, fn func(K) (bool, error)) error {
	if err := db.unavailableErr(); err != nil {
		return err
	}

	if !db.strKey && db.index != nil {
		return db.index.ScanKeys(keycodec.UserKeyUint(seek), keycodec.UserKeyUintScanFunc(fn))
	}
	if db.strKey && db.index != nil && db.index.HasStringKeyIndex() {
		return db.index.ScanStringKeys(keycodec.UserKeyString(seek), keycodec.UserKeyStringScanFunc(fn))
	}

	tx, err := db.bolt.Begin(false)
	if err != nil {
		return fmt.Errorf("tx error: %w", err)
	}
	defer rollback(tx)

	b := tx.Bucket(db.dataBucket)
	if b == nil {
		return fmt.Errorf("bucket does not exist")
	}

	c := b.Cursor()
	var keyBuf [8]byte
	if db.strKey {
		call := keycodec.UserKeyStringScanFunc(fn)
		for key, _ := c.Seek(keycodec.UserKeyBytesWithBuf(seek, true, &keyBuf)); key != nil; key, _ = c.Next() {
			more, err := call(string(key))
			if err != nil || !more {
				return err
			}
		}
		return nil
	}

	call := keycodec.UserKeyUintScanFunc(fn)
	for key, _ := c.Seek(keycodec.UserKeyBytesWithBuf(seek, false, &keyBuf)); key != nil; key, _ = c.Next() {
		if len(key) != 8 {
			return fmt.Errorf("invalid numeric data key length: %d", len(key))
		}
		more, err := call(keycodec.U64FromBytes(key))
		if err != nil || !more {
			return err
		}
	}
	return nil
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

	b := tx.Bucket(db.dataBucket)
	if b == nil {
		return fmt.Errorf("bucket does not exist")
	}
	c := b.Cursor()

	var keyBuf [8]byte
	key, value := c.Seek(keycodec.UserKeyBytesWithBuf(seek, db.strKey, &keyBuf))
	if key == nil {
		return nil
	}
	val, err := db.decodeStoredValue(value)
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
		if val, err = db.decodeStoredValue(value); err != nil {
			return fmt.Errorf("decode: %w", err)
		}
		if more, err = fn(keycodec.UserKeyFromBytes[K](key, db.strKey), val); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB[K, V]) decodeStoredValue(data []byte) (*V, error) {
	if db.strKey {
		if len(data) < 8 {
			return nil, fmt.Errorf("%w: value shorter than 8 bytes", rbierrors.ErrInvalidStringStorageFormat)
		}
		if keycodec.U64FromBytes(data[:8]) == 0 {
			return nil, fmt.Errorf("%w: zero string id", rbierrors.ErrInvalidStringStorageFormat)
		}
		data = data[8:]
	}
	return db.decode(data)
}
