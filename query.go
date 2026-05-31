package rbi

import (
	"errors"
	"fmt"
	"time"
	"unsafe"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/engine"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/qagg"
	"github.com/vapstack/rbi/internal/qexec"
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

type PlanName = qexec.PlanName

const (
	PlanMaterialized       = qexec.PlanMaterialized
	PlanAggregate          = qagg.PlanAggregate
	PlanCountMaterialized  = qagg.PlanCountMaterialized
	PlanCountUniqueEq      = qagg.PlanCountUniqueEq
	PlanCountScalarLookup  = qagg.PlanCountScalarLookup
	PlanCountScalarInSplit = qagg.PlanCountScalarInSplit
	PlanCountPredicates    = qagg.PlanCountPredicates
	PlanCountORPredicates  = qagg.PlanCountORPredicates
	PlanCountORHybrid      = qagg.PlanCountORHybrid

	PlanCandidateNoOrder = qexec.PlanCandidateNoOrder
	PlanCandidateOrder   = qexec.PlanCandidateOrder

	PlanORMergeNoOrder     = qexec.PlanORMergeNoOrder
	PlanORMergeOrderMerge  = qexec.PlanORMergeOrderMerge
	PlanORMergeOrderStream = qexec.PlanORMergeOrderStream

	PlanOrdered        = qexec.PlanOrdered
	PlanOrderedNoOrder = qexec.PlanOrderedNoOrder
	PlanOrderedAnchor  = qexec.PlanOrderedAnchor
	PlanOrderedLead    = qexec.PlanOrderedLead

	PlanLimit              = qexec.PlanLimit
	PlanLimitOrderBasic    = qexec.PlanLimitOrderBasic
	PlanLimitOrderPrefix   = qexec.PlanLimitOrderPrefix
	PlanLimitPrefixNoOrder = qexec.PlanLimitPrefixNoOrder
	PlanLimitRangeNoOrder  = qexec.PlanLimitRangeNoOrder
	PlanUniqueEq           = qexec.PlanUniqueEq
)

type (
	TraceEvent        = qexec.TraceEvent
	TraceORRoute      = qexec.TraceORRoute
	TraceORBranch     = qexec.TraceORBranch
	PlannerFieldStats = qexec.PlannerFieldStats
)

const randStreamMix uint64 = 0x9e3779b97f4a7c15

// Aggregate evaluates a reduction query against the current index snapshot.
func (db *DB[K, V]) Aggregate(q *qx.QX) (Result, error) {
	if err := db.unavailableErr(); err != nil {
		return Result{}, err
	}

	if db.index == nil {
		return Result{}, ErrNoIndex
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
		return 0, ErrNoIndex
	}
	return db.index.Count(exprs...)
}

// Query evaluates the given query against the index and returns all matching values.
func (db *DB[K, V]) Query(q *qx.QX) ([]*V, error) {
	if err := db.unavailableErr(); err != nil {
		return nil, err
	}

	if db.index == nil {
		return nil, ErrNoIndex
	}
	var out []*V
	err := db.index.Query(q, db.bolt, db.bucket, db.unavailableErr, func(tx *bbolt.Tx, keys engine.KeySet) error {
		var err error
		out, err = db.batchGetTxCompact(tx, keys)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (db *DB[K, V]) batchGetTxCompact(tx *bbolt.Tx, keys engine.KeySet) ([]*V, error) {
	bucket := tx.Bucket(db.bucket)
	if bucket == nil {
		return nil, fmt.Errorf("bucket does not exist")
	}
	if len(keys.IDs) == 0 {
		return nil, nil
	}

	out := make([]*V, 0, len(keys.IDs))
	if db.strKey {
		for i := range keys.IDs {
			v := bucket.Get(keycodec.StringBytes(keys.StringAt(i)))
			if v == nil {
				continue
			}
			value, err := db.decode(v)
			if err != nil {
				db.ReleaseRecords(out...)
				return nil, fmt.Errorf("decode: %w", err)
			}
			out = append(out, value)
		}
		return out, nil
	}

	var key [8]byte
	for _, idx := range keys.IDs {
		v := bucket.Get(keycodec.U64BytesWithBuf(idx, &key))
		if v == nil {
			continue
		}
		value, err := db.decode(v)
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
		return nil, ErrNoIndex
	}

	var out []K
	if err := db.index.QueryKeys(q, func(keys engine.KeySet) error {
		if db.strKey {
			out = db.queryKeysFromStrings(keys)
		} else {
			out = db.queryKeysFromIDs(keys.IDs)
		}
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

func (db *DB[K, V]) queryKeysFromStrings(keys engine.KeySet) []K {
	if len(keys.IDs) == 0 {
		return nil
	}
	out := make([]K, len(keys.IDs))
	for i := range keys.IDs {
		s := keys.StringAt(i)
		out[i] = *(*K)(unsafe.Pointer(&s))
	}
	return out
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
// exists; the method returns ErrNoIndex.
func (db *DB[K, V]) RefreshPlannerStats() error {
	if err := db.unavailableErr(); err != nil {
		return err
	}

	if db.index == nil {
		return ErrNoIndex
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
	return errors.Is(err, ErrClosed) || errors.Is(err, ErrBroken)
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
func (db *DB[K, V]) SnapshotStats() SnapshotStats {
	if db.index == nil {
		return SnapshotStats{}
	}
	if !db.options.EnableSnapshotStats {
		return SnapshotStats{}
	}
	if err := db.unavailableErr(); err != nil {
		return SnapshotStats{}
	}

	stats := db.index.SnapshotStats()
	return SnapshotStats{
		Sequence:     stats.Sequence,
		UniverseCard: stats.UniverseCard,
		RegistrySize: stats.RegistrySize,
		PinnedRefs:   stats.PinnedRefs,
	}
}
