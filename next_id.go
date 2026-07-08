package rbi

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"

	"github.com/vapstack/monotime"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/persist"
	"github.com/vapstack/rbi/rbierrors"
	"go.etcd.io/bbolt"
)

const (
	autoUintSeqBits = 48
	autoUintSeqMask = uint64(1<<autoUintSeqBits) - 1
	autoUUIDLen     = 16
)

// NextID returns the next strictly increasing generated key
// for this collection and the configured NodeID.
//
// For uint64-key collections with non-zero NodeID, the high 16 bits contain
// NodeID and the low 48 bits contain a sequence. When NodeID is 0,
// the whole uint64 key is used as the sequence. For string-key collections,
// the key is a UUIDv7 string that embeds NodeID.
//
// NextID only reserves a key. The in-memory generator advances immediately
// and is not rolled back if tx is released without commit.
// The generated watermark is persisted only when tx commits,
// so an uncommitted key may be issued again after restart.
// Gaps are allowed.
//
// Unlike bbolt's sequence, it cannot be set manually and can only be
// advanced through AdvanceID.
func (c *Collection[K, V]) NextID(tx *Tx) (K, error) {
	var zero K
	if tx == nil {
		return zero, rbierrors.ErrNilTx
	}
	if err := tx.usableWrite(); err != nil {
		return zero, err
	}
	if err := tx.bindAutoIncCollection(c.collection); err != nil {
		return zero, err
	}

	var id K

	if c.strKey {
		s := c.autoUUID.Next().String()
		id = *(*K)(unsafe.Pointer(&s))

	} else {
		for {
			cur := c.autoUint.Load()
			if c.options.NodeID == 0 {
				if cur == math.MaxUint64 {
					return zero, rbierrors.ErrSequenceExhausted
				}
			} else if cur&autoUintSeqMask == autoUintSeqMask {
				return zero, rbierrors.ErrSequenceExhausted
			}
			next := cur + 1
			if c.autoUint.CompareAndSwap(cur, next) {
				id = *(*K)(unsafe.Pointer(&next))
				break
			}
		}
	}

	tx.markAutoIncCollection(c.collection)
	return id, nil
}

// AdvanceID advances the automatic uint64 ID sequence so NextID returns a key
// greater than id.
//
// For collections with a non-zero NodeID, id is the 48-bit per-node sequence
// portion. For NodeID 0, id is the full uint64 key.
// Values lower than the current watermark are ignored. For non-zero NodeID,
// values above the 48-bit sequence range return ErrAutoIncExhausted.
//
// AdvanceID reserves no record. The in-memory generator is advanced immediately
// and is not rolled back if tx is released without commit. The advanced
// watermark is persisted only when tx commits.
//
// For string-key collections AdvanceID is a no-op and tx is ignored.
func (c *Collection[K, V]) AdvanceID(tx *Tx, id uint64) error {
	if c.strKey {
		return nil
	}
	if tx == nil {
		return rbierrors.ErrNilTx
	}
	if err := tx.usableWrite(); err != nil {
		return err
	}
	if c.options.NodeID != 0 && id > autoUintSeqMask {
		return rbierrors.ErrSequenceExhausted
	}
	if err := tx.bindAutoIncCollection(c.collection); err != nil {
		return err
	}

	watermark := id
	if c.options.NodeID != 0 {
		watermark = uint64(c.options.NodeID)<<autoUintSeqBits | id
	}
	for {
		cur := c.autoUint.Load()
		if watermark <= cur {
			break
		}
		if c.autoUint.CompareAndSwap(cur, watermark) {
			break
		}
	}

	tx.markAutoIncCollection(c.collection)
	return nil
}

func (c *Collection[K, V]) initAutoIncID(tx *bbolt.Tx, meta *bbolt.Bucket) error {
	if c.strKey {
		c.autoMetaKey = rootAutoIncIDKey(rootAutoStringKeyKind, c.dataBucket, c.options.NodeID)
		last, ok, err := readRootAutoIncUUID(meta, c.collection)
		if err != nil {
			return err
		}
		if !ok {
			// Missing metadata is treated as a generator reset.
			last = monotime.ZeroUUID
		}
		gen, err := monotime.NewGenUUID(int(c.options.NodeID), last)
		if err != nil {
			return fmt.Errorf("initialize automatic UUID generator: %w", err)
		}
		c.autoUUID = gen
		return nil
	}

	c.autoMetaKey = rootAutoIncIDKey(rootAutoUintKeyKind, c.dataBucket, c.options.NodeID)
	watermark, ok, err := readRootAutoIncUint(meta, c.collection)
	if err != nil {
		return err
	}
	if !ok {
		watermark, err = maxAutoIncUintKey(tx, c.collection)
		if err != nil {
			return err
		}
	}
	c.autoUint.Store(watermark)
	return nil
}

func (tx *Tx) bindAutoIncCollection(c *collection) error {
	if tx.state != writeTxHook {
		return tx.bindCollection(c)
	}
	if err := tx.checkGeneratedCollection(c); err != nil {
		return err
	}
	unit := tx.hookUnit
	for i := range unit.segments {
		if unit.segments[i].collection == c {
			return nil
		}
	}
	for i := range unit.generatedCollections {
		if unit.generatedCollections[i] == c {
			return nil
		}
	}
	if err := c.retain(); err != nil {
		return err
	}
	unit.generatedCollections = append(unit.generatedCollections, c)
	if maxOps := c.options.BatchSoftLimit; maxOps < unit.limit {
		unit.limit = maxOps
	}
	return nil
}

func (tx *Tx) markAutoIncCollection(c *collection) {
	unit := &tx.unit
	if tx.state == writeTxHook {
		unit = tx.hookUnit
	}
	for i := range unit.autoIncCollections {
		if unit.autoIncCollections[i] == c {
			return
		}
	}
	unit.autoIncCollections = append(unit.autoIncCollections, c)
}

func readRootAutoIncUint(meta *bbolt.Bucket, c *collection) (uint64, bool, error) {
	value := meta.Get(c.autoMetaKey)
	if value == nil {
		return 0, false, nil
	}
	if len(value) != persist.UIDLen+8 {
		return 0, false, fmt.Errorf("invalid automatic uint64 id metadata length: %d", len(value))
	}
	if !bytes.Equal(value[:persist.UIDLen], c.rbiUID[:]) {
		return 0, false, nil
	}
	watermark := binary.BigEndian.Uint64(value[persist.UIDLen:])
	if c.options.NodeID != 0 && uint16(watermark>>autoUintSeqBits) != c.options.NodeID {
		return 0, false, fmt.Errorf("automatic uint64 id metadata node mismatch")
	}
	return watermark, true, nil
}

func readRootAutoIncUUID(meta *bbolt.Bucket, c *collection) (monotime.UUID, bool, error) {
	value := meta.Get(c.autoMetaKey)
	if value == nil {
		return monotime.ZeroUUID, false, nil
	}
	if len(value) != persist.UIDLen+autoUUIDLen {
		return monotime.ZeroUUID, false, fmt.Errorf("invalid automatic UUID metadata length: %d", len(value))
	}
	if !bytes.Equal(value[:persist.UIDLen], c.rbiUID[:]) {
		return monotime.ZeroUUID, false, nil
	}
	var id monotime.UUID
	copy(id[:], value[persist.UIDLen:])
	return id, true, nil
}

func maxAutoIncUintKey(tx *bbolt.Tx, c *collection) (uint64, error) {
	prefix := uint64(c.options.NodeID) << autoUintSeqBits
	b := tx.Bucket(c.dataBucket)
	if b == nil {
		return 0, fmt.Errorf("data bucket %q does not exist", c.dataBucket)
	}

	cur := b.Cursor()
	var key []byte
	if c.options.NodeID == 0 || c.options.NodeID == ^uint16(0) {
		key, _ = cur.Last()
	} else {
		var seek [8]byte
		binary.BigEndian.PutUint64(seek[:], (uint64(c.options.NodeID)+1)<<autoUintSeqBits)
		key, _ = cur.Seek(seek[:])
		if key == nil {
			key, _ = cur.Last()
		} else {
			key, _ = cur.Prev()
		}
	}
	if key == nil {
		return prefix, nil
	}
	if len(key) != 8 {
		return 0, fmt.Errorf("invalid numeric data key length: %d", len(key))
	}
	watermark := keycodec.U64FromBytes(key)
	if c.options.NodeID != 0 && uint16(watermark>>autoUintSeqBits) != c.options.NodeID {
		return prefix, nil
	}
	return watermark, nil
}

func rootAutoIncIDKey(kind byte, dataBucket []byte, nodeID uint16) []byte {
	key := make([]byte, 1+binary.MaxVarintLen64+len(dataBucket)+2)
	key[0] = kind
	n := binary.PutUvarint(key[1:], uint64(len(dataBucket)))
	key = key[:1+n+len(dataBucket)+2]
	copy(key[1+n:], dataBucket)
	binary.BigEndian.PutUint16(key[len(key)-2:], nodeID)
	return key
}
