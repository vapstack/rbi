package rbi

import (
	"errors"
	"fmt"
	"time"
	"unsafe"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/qagg"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/rbierrors"
	"github.com/vapstack/rbi/rbistats"
	"go.etcd.io/bbolt"
)

type (
	Result    = qagg.Result
	Row       = qagg.Row
	Value     = qagg.Value
	ValueKind = qagg.ValueKind
)

const (
	ValueKindNone   = qagg.ValueKindNone
	ValueKindAny    = qagg.ValueKindAny
	ValueKindBool   = qagg.ValueKindBool
	ValueKindInt    = qagg.ValueKindInt
	ValueKindUint   = qagg.ValueKindUint
	ValueKindFloat  = qagg.ValueKindFloat
	ValueKindString = qagg.ValueKindString
)

// Aggregate evaluates a reduction query against the transaction's index snapshot.
func (c *Collection[K, V]) Aggregate(tx *Tx, q *qx.QX) (Result, error) {
	if tx == nil {
		return Result{}, rbierrors.ErrNilTx
	}
	snap, err := c.indexSnapshot(tx)
	if err != nil {
		return Result{}, err
	}
	return c.index.AggregateOnSnapshot(snap, q)
}

// Count evaluates the given filter predicates and returns the number of matching records.
// Zero predicates mean match-all.
func (c *Collection[K, V]) Count(tx *Tx, exprs ...qx.Expr) (uint64, error) {
	if tx == nil {
		return 0, rbierrors.ErrNilTx
	}
	snap, err := c.indexSnapshot(tx)
	if err != nil {
		return 0, err
	}
	return c.index.CountOnSnapshot(snap, exprs...)
}

func (c *Collection[K, V]) indexSnapshot(tx *Tx) (*snapshot.View, error) {
	return tx.collectionSnapshot(c.collection)
}

// Query evaluates the given query against the index and returns all matching values.
func (c *Collection[K, V]) Query(tx *Tx, q *qx.QX) ([]*V, error) {
	if tx == nil {
		return nil, rbierrors.ErrNilTx
	}
	boltTx, ids, err := c.queryIDs(tx, q, true)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return c.loadQueryValues(boltTx, ids)
}

func (c *Collection[K, V]) loadQueryValues(tx *bbolt.Tx, ids []uint64) ([]*V, error) {
	bucket := tx.Bucket(c.dataBucket)
	if bucket == nil {
		return nil, fmt.Errorf("bucket does not exist")
	}

	out := make([]*V, 0, len(ids))
	if c.strKey {
		m, err := c.stringMap(tx)
		if err != nil {
			return nil, err
		}
		var mapKey [8]byte
		for _, idx := range ids {
			key := m.Get(keycodec.U64BytesWithBuf(idx, &mapKey))
			if key == nil {
				c.ReleaseRecords(out...)
				return nil, fmt.Errorf("%w: missing string reverse key for idx %d", rbierrors.ErrInvalidStringStorageFormat, idx)
			}
			v := bucket.Get(key)
			if v == nil {
				c.ReleaseRecords(out...)
				return nil, fmt.Errorf("%w: missing string data for idx %d", rbierrors.ErrInvalidStringStorageFormat, idx)
			}
			if len(v) < 8 {
				c.ReleaseRecords(out...)
				return nil, fmt.Errorf("%w: value shorter than %d bytes", rbierrors.ErrInvalidStringStorageFormat, 8)
			}
			storedIdx := keycodec.U64FromBytes(v[:8])
			if storedIdx == 0 {
				c.ReleaseRecords(out...)
				return nil, fmt.Errorf("%w: zero string id", rbierrors.ErrInvalidStringStorageFormat)
			}
			if storedIdx != idx {
				c.ReleaseRecords(out...)
				return nil, fmt.Errorf("%w: string idx mismatch rev=%d value=%d", rbierrors.ErrInvalidStringStorageFormat, idx, storedIdx)
			}
			value, err := c.decode(v[8:])
			if err != nil {
				c.ReleaseRecords(out...)
				return nil, fmt.Errorf("decode: %w", err)
			}
			out = append(out, value)
		}
		return out, nil
	}

	var key [8]byte
	for _, idx := range ids {
		v := bucket.Get(keycodec.U64BytesWithBuf(idx, &key))
		if v == nil {
			c.ReleaseRecords(out...)
			return nil, fmt.Errorf("missing numeric data for id %d", idx)
		}
		value, err := c.decodeStoredValue(v)
		if err != nil {
			c.ReleaseRecords(out...)
			return nil, fmt.Errorf("decode: %w", err)
		}
		out = append(out, value)
	}
	return out, nil
}

// QueryKeys evaluates the given query against the index and returns all matching ids.
func (c *Collection[K, V]) QueryKeys(tx *Tx, q *qx.QX) ([]K, error) {
	if tx == nil {
		return nil, rbierrors.ErrNilTx
	}
	if c.strKey {
		boltTx, ids, err := c.queryIDs(tx, q, true)
		if err != nil {
			return nil, err
		}
		m, err := c.stringMap(boltTx)
		if err != nil {
			return nil, err
		}
		if len(ids) == 0 {
			return nil, nil
		}
		out := make([]K, 0, len(ids))
		var mapKey [8]byte
		for _, idx := range ids {
			key := m.Get(keycodec.U64BytesWithBuf(idx, &mapKey))
			if key == nil {
				return nil, fmt.Errorf("%w: missing string reverse key for idx %d", rbierrors.ErrInvalidStringStorageFormat, idx)
			}
			s := string(key)
			out = append(out, *(*K)(unsafe.Pointer(&s)))
		}
		return out, nil
	}
	ids, err := c.queryKeyIDs(tx, q, true)
	if err != nil {
		return nil, err
	}
	return c.queryKeysFromIDs(ids), nil
}

func (c *Collection[K, V]) queryKeysFromIDs(ids []uint64) []K {
	if len(ids) == 0 {
		return nil
	}
	return unsafe.Slice((*K)(unsafe.Pointer(&ids[0])), len(ids))
}

func (c *Collection[K, V]) queryIDs(tx *Tx, q *qx.QX, tryEmpty bool) (*bbolt.Tx, []uint64, error) {
	_, read, err := tx.collectionBucket(c.collection)
	if err != nil {
		return nil, nil, err
	}
	if read.snap == nil {
		return nil, nil, rbierrors.ErrNoIndex
	}

	ids, err := c.index.QueryIDsOnSnapshot(read.snap, q, tryEmpty)
	if err != nil {
		return nil, nil, err
	}
	return tx.boltTx, ids, nil
}

func (c *Collection[K, V]) queryKeyIDs(tx *Tx, q *qx.QX, tryEmpty bool) ([]uint64, error) {
	snap, err := c.indexSnapshot(tx)
	if err != nil {
		return nil, err
	}
	return c.index.QueryIDsOnSnapshot(snap, q, tryEmpty)
}

func (c *Collection[K, V]) stringMap(tx *bbolt.Tx) (*bbolt.Bucket, error) {
	m := tx.Bucket(c.strmapBucket)
	if m == nil {
		return nil, fmt.Errorf("%w: missing string map bucket", rbierrors.ErrInvalidStringStorageFormat)
	}
	return m, nil
}

// RefreshPlannerStats rebuilds planner statistics from the current in-memory index.
//
// This is a synchronous, full refresh: the call returns after the planner stats
// snapshot is rebuilt and published, or after an error.
//
// In indexed mode it scans the current published index snapshot and replaces
// the planner stats payload atomically.
//
// In transparent mode planner stats are disabled because no runtime index
// exists; the method returns rbierrors.ErrNoIndex.
func (c *Collection[K, V]) RefreshPlannerStats() error {
	if err := c.unavailableErr(); err != nil {
		return err
	}

	if c.index == nil {
		return rbierrors.ErrNoIndex
	}

	tx := BeginIndexView()
	defer tx.Release()
	snap, err := tx.collectionSnapshot(c.collection)
	if err != nil {
		return err
	}

	return c.index.RefreshPlannerStatsOnSnapshot(snap, c.unavailableErr)
}

func plannerAnalyzeInterval(v time.Duration) time.Duration {
	if v < 0 {
		return 0
	}
	if v == 0 {
		return defaultOptionsAnalyzeInterval
	}
	return v
}

func (c *Collection[K, V]) startPlannerAnalyzeLoop() {
	c.index.StartAnalyzeLoop(c.RefreshPlannerStats, plannerAnalyzeTerminalError, plannerAnalyzeResetFailure)
}

func (c *Collection[K, V]) stopAnalyzeLoop() {
	if c.index == nil {
		return
	}
	c.index.StopAnalyzeLoop()
}

func plannerAnalyzeTerminalError(err error) bool {
	return errors.Is(err, rbierrors.ErrClosed) || errors.Is(err, rbierrors.ErrBroken)
}

func plannerAnalyzeResetFailure(error) bool {
	return false
}

// SnapshotStats returns diagnostics for published index snapshots.
//
// In indexed mode it reports Collection's current published snapshot
// sequence and universe cardinality.
//
// In transparent mode no published index snapshots are maintained, so the
// returned diagnostics remain zero-valued.
//
// On closed or broken Collection, SnapshotStats returns zero value.
func (c *Collection[K, V]) SnapshotStats() rbistats.Snapshot {
	if c.index == nil {
		return rbistats.Snapshot{}
	}
	if err := c.unavailableErr(); err != nil {
		return rbistats.Snapshot{}
	}

	return c.rootSnapshotStats()
}

func (c *Collection[K, V]) rootSnapshotStats() rbistats.Snapshot {
	rr := &c.root.registry
	rr.mu.RLock()
	ref := rr.currentRef.Load()
	if ref == nil || ref.gen == nil || int(c.ordinal) >= len(ref.gen.entries) {
		rr.mu.RUnlock()
		return rbistats.Snapshot{}
	}
	entry := ref.gen.entries[c.ordinal]
	if entry.collection != c.collection || entry.read == nil || entry.read.snap == nil {
		rr.mu.RUnlock()
		return rbistats.Snapshot{}
	}
	snap := entry.read.snap
	out := rbistats.Snapshot{Sequence: snap.Seq}
	if !snap.Universe.IsEmpty() {
		out.UniverseCard = snap.Universe.Cardinality()
	}
	rr.mu.RUnlock()
	return out
}
