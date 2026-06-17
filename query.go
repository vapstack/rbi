package rbi

import (
	"errors"
	"fmt"
	"time"
	"unsafe"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/qagg"
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

// Aggregate evaluates a reduction query against the current index snapshot.
func (db *DB[K, V]) Aggregate(q *qx.QX) (Result, error) {
	if err := db.unavailableErr(); err != nil {
		return Result{}, err
	}

	if db.index == nil {
		return Result{}, rbierrors.ErrNoIndex
	}

	return db.index.Aggregate(q)
}

// Count evaluates the given filter predicates and returns the number of matching records.
// Zero predicates mean match-all.
func (db *DB[K, V]) Count(exprs ...qx.Expr) (uint64, error) {
	if err := db.unavailableErr(); err != nil {
		return 0, err
	}

	if db.index == nil {
		return 0, rbierrors.ErrNoIndex
	}
	return db.index.Count(exprs...)
}

// Query evaluates the given query against the index and returns all matching values.
func (db *DB[K, V]) Query(q *qx.QX) ([]*V, error) {
	if err := db.unavailableErr(); err != nil {
		return nil, err
	}

	if db.index == nil {
		return nil, rbierrors.ErrNoIndex
	}
	var out []*V
	err := db.index.Query(q, db.bolt, db.dataBucket, db.unavailableErr, func(tx *bbolt.Tx, ids []uint64) error {
		var err error
		out, err = db.batchGetTxCompact(tx, ids)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (db *DB[K, V]) batchGetTxCompact(tx *bbolt.Tx, ids []uint64) ([]*V, error) {
	bucket := tx.Bucket(db.dataBucket)
	if bucket == nil {
		return nil, fmt.Errorf("bucket does not exist")
	}
	if len(ids) == 0 {
		return nil, nil
	}

	out := make([]*V, 0, len(ids))
	if db.strKey {
		m, err := db.stringMap(tx)
		if err != nil {
			return nil, err
		}
		var mapKey [8]byte
		for _, idx := range ids {
			key := m.Get(keycodec.U64BytesWithBuf(idx, &mapKey))
			if key == nil {
				db.ReleaseRecords(out...)
				return nil, fmt.Errorf("%w: missing string reverse key for idx %d", rbierrors.ErrInvalidStringStorageFormat, idx)
			}
			v := bucket.Get(key)
			if v == nil {
				db.ReleaseRecords(out...)
				return nil, fmt.Errorf("%w: missing string data for idx %d", rbierrors.ErrInvalidStringStorageFormat, idx)
			}
			if len(v) < 8 {
				db.ReleaseRecords(out...)
				return nil, fmt.Errorf("%w: value shorter than %d bytes", rbierrors.ErrInvalidStringStorageFormat, 8)
			}
			storedIdx := keycodec.U64FromBytes(v[:8])
			if storedIdx == 0 {
				db.ReleaseRecords(out...)
				return nil, fmt.Errorf("%w: zero string id", rbierrors.ErrInvalidStringStorageFormat)
			}
			if storedIdx != idx {
				db.ReleaseRecords(out...)
				return nil, fmt.Errorf("%w: string idx mismatch rev=%d value=%d", rbierrors.ErrInvalidStringStorageFormat, idx, storedIdx)
			}
			value, err := db.decode(v[8:])
			if err != nil {
				db.ReleaseRecords(out...)
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
			db.ReleaseRecords(out...)
			return nil, fmt.Errorf("missing numeric data for id %d", idx)
		}
		value, err := db.decodeStoredValue(v)
		if err != nil {
			db.ReleaseRecords(out...)
			return nil, fmt.Errorf("decode: %w", err)
		}
		out = append(out, value)
	}
	return out, nil
}

// QueryKeys evaluates the given query against the index and returns all matching ids.
func (db *DB[K, V]) QueryKeys(q *qx.QX) ([]K, error) {
	if err := db.unavailableErr(); err != nil {
		return nil, err
	}

	if db.index == nil {
		return nil, rbierrors.ErrNoIndex
	}

	var out []K
	if db.strKey {
		err := db.index.Query(q, db.bolt, db.dataBucket, db.unavailableErr, func(tx *bbolt.Tx, ids []uint64) error {
			m, err := db.stringMap(tx)
			if err != nil {
				return err
			}
			if len(ids) == 0 {
				return nil
			}
			out = make([]K, 0, len(ids))
			var mapKey [8]byte
			for _, idx := range ids {
				key := m.Get(keycodec.U64BytesWithBuf(idx, &mapKey))
				if key == nil {
					return fmt.Errorf("%w: missing string reverse key for idx %d", rbierrors.ErrInvalidStringStorageFormat, idx)
				}
				s := string(key)
				out = append(out, *(*K)(unsafe.Pointer(&s)))
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		return out, nil
	}

	if err := db.index.QueryKeys(q, func(ids []uint64) error {
		out = db.queryKeysFromIDs(ids)
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}

func (db *DB[K, V]) queryKeysFromIDs(ids []uint64) []K {
	if len(ids) == 0 {
		return nil
	}
	return unsafe.Slice((*K)(unsafe.Pointer(&ids[0])), len(ids))
}

func (db *DB[K, V]) stringMap(tx *bbolt.Tx) (*bbolt.Bucket, error) {
	m := tx.Bucket(db.strmapBucket)
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
func (db *DB[K, V]) RefreshPlannerStats() error {
	if err := db.unavailableErr(); err != nil {
		return err
	}

	if db.index == nil {
		return rbierrors.ErrNoIndex
	}

	return db.index.RefreshPlannerStats(db.unavailableErr)
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

func (db *DB[K, V]) startPlannerAnalyzeLoop() {
	db.index.StartAnalyzeLoop(db.RefreshPlannerStats, plannerAnalyzeTerminalError, plannerAnalyzeResetFailure)
}

func (db *DB[K, V]) stopAnalyzeLoop() {
	if db.index == nil {
		return
	}
	db.index.StopAnalyzeLoop()
}

func plannerAnalyzeTerminalError(err error) bool {
	return errors.Is(err, rbierrors.ErrClosed) || errors.Is(err, rbierrors.ErrBroken)
}

func plannerAnalyzeResetFailure(error) bool {
	return false
}

// SnapshotStats returns diagnostics for published index snapshots.
//
// Runtime snapshot diagnostics are collected only when
// Options.EnableSnapshotStats was enabled for this DB instance;
// otherwise SnapshotStats returns zero value.
//
// In indexed mode it reports the current published snapshot sequence,
// universe cardinality, registry size, and pin counts.
//
// In transparent mode no published index snapshots are maintained, so the
// returned diagnostics remain zero-valued even when snapshot stats collection
// is enabled.
//
// On closed or broken DB, SnapshotStats returns zero value.
func (db *DB[K, V]) SnapshotStats() rbistats.Snapshot {
	if db.index == nil {
		return rbistats.Snapshot{}
	}
	if !db.options.EnableSnapshotStats {
		return rbistats.Snapshot{}
	}
	if err := db.unavailableErr(); err != nil {
		return rbistats.Snapshot{}
	}

	return db.index.SnapshotStats()
}
