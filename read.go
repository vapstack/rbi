package rbi

import (
	"fmt"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/rbierrors"
)

// Get returns the value stored by id or nil if key was not found.
func (c *Collection[K, V]) Get(tx *Tx, id K) (*V, error) {
	if tx == nil {
		return nil, rbierrors.ErrNilTx
	}
	bucket, _, err := tx.collectionBucket(c.collection)
	if err != nil {
		return nil, err
	}

	var keyBuf [8]byte
	v := bucket.Get(keycodec.UserKeyBytesWithBuf(id, c.strKey, &keyBuf))
	if v == nil {
		return nil, nil
	}

	r, err := c.decodeStoredValue(v)
	if err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return r, nil
}

// ScanKeys iterates over live keys greater than or equal to seek and calls fn
// for each key. Scan stops when fn returns false or a non-nil error.
//
// Indexed collection iterates the current in-memory snapshot.
// Other modes scan the data bucket inside a read-only transaction which remains
// open for the duration of the scan.
//
// Numeric keys are returned in numeric order,
// string keys are returned in bbolt lexicographic byte order.
func (c *Collection[K, V]) ScanKeys(tx *Tx, seek K, fn func(K) (bool, error)) error {
	if tx == nil {
		return rbierrors.ErrNilTx
	}
	if !c.strKey && c.index != nil {
		snap, err := tx.collectionSnapshot(c.collection)
		if err != nil {
			return err
		}
		return c.index.ScanKeysOnSnapshot(snap, keycodec.UserKeyUint(seek), keycodec.UserKeyUintScanFunc(fn))
	}
	if c.strKey && c.index != nil && c.index.HasStringKeyIndex() {
		snap, err := tx.collectionSnapshot(c.collection)
		if err != nil {
			return err
		}
		return c.index.ScanStringKeysOnSnapshot(snap, keycodec.UserKeyString(seek), keycodec.UserKeyStringScanFunc(fn))
	}

	b, _, err := tx.collectionBucket(c.collection)
	if err != nil {
		return err
	}

	cur := b.Cursor()
	var keyBuf [8]byte
	if c.strKey {
		call := keycodec.UserKeyStringScanFunc(fn)
		for key, _ := cur.Seek(keycodec.UserKeyBytesWithBuf(seek, true, &keyBuf)); key != nil; key, _ = cur.Next() {
			more, err := call(string(key))
			if err != nil || !more {
				return err
			}
		}
		return nil
	}

	call := keycodec.UserKeyUintScanFunc(fn)
	for key, _ := cur.Seek(keycodec.UserKeyBytesWithBuf(seek, false, &keyBuf)); key != nil; key, _ = cur.Next() {
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
// Records passed to fn can optionally be returned back using ReleaseRecords
// to minimize GC pressure.
func (c *Collection[K, V]) SeqScan(tx *Tx, seek K, fn func(K, *V) (bool, error)) error {
	if tx == nil {
		return rbierrors.ErrNilTx
	}
	b, _, err := tx.collectionBucket(c.collection)
	if err != nil {
		return err
	}
	cur := b.Cursor()

	var keyBuf [8]byte
	key, value := cur.Seek(keycodec.UserKeyBytesWithBuf(seek, c.strKey, &keyBuf))
	if c.strKey {
		for key != nil {
			val, err := c.decodeStoredValue(value)
			if err != nil {
				return fmt.Errorf("decode: %w", err)
			}
			more, err := fn(keycodec.UserKeyFromBytes[K](key, true), val)
			if err != nil || !more {
				return err
			}
			key, value = cur.Next()
		}
		return nil
	}
	for key != nil {
		if len(key) != 8 {
			return fmt.Errorf("invalid numeric data key length: %d", len(key))
		}
		val, err := c.decodeStoredValue(value)
		if err != nil {
			return fmt.Errorf("decode: %w", err)
		}
		more, err := fn(keycodec.UserKeyFromBytes[K](key, false), val)
		if err != nil || !more {
			return err
		}
		key, value = cur.Next()
	}
	return nil
}

func (c *Collection[K, V]) decodeStoredValue(data []byte) (*V, error) {
	if c.strKey {
		if len(data) < 8 {
			return nil, fmt.Errorf("%w: value shorter than 8 bytes", rbierrors.ErrInvalidStringStorageFormat)
		}
		if keycodec.U64FromBytes(data[:8]) == 0 {
			return nil, fmt.Errorf("%w: zero string id", rbierrors.ErrInvalidStringStorageFormat)
		}
		data = data[8:]
	}
	return c.decode(data)
}
