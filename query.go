package rbi

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
	"unsafe"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qagg"
	"github.com/vapstack/rbi/internal/qexec"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
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
	if err := db.beginOp(); err != nil {
		return Result{}, err
	}
	defer db.endOp()

	if db.engine == nil {
		return Result{}, ErrNoIndex
	}

	prepared, err := qagg.Prepare(q, db.engine.schema)
	if err != nil {
		return Result{}, err
	}
	defer prepared.Release()

	snap, seq, ref := db.engine.snapshot.PinCurrent()
	defer db.engine.snapshot.Unpin(seq, ref)

	view := db.engine.exec.AcquireView(snap)
	defer db.engine.exec.ReleaseView(view)

	return qagg.Execute(view, snap, prepared)
}

// Count evaluates the given filter predicates and returns the number of matching records.
// Zero predicates mean match-all.
func (db *DB[K, V]) Count(exprs ...qx.Expr) (uint64, error) {
	if err := db.beginOp(); err != nil {
		return 0, err
	}
	defer db.endOp()

	if db.engine == nil {
		return 0, ErrNoIndex
	}
	prepared, err := qagg.PrepareCount(db.engine.schema, exprs...)
	if err != nil {
		return 0, err
	}
	defer prepared.Release()

	snap, seq, ref := db.engine.snapshot.PinCurrent()
	defer db.engine.snapshot.Unpin(seq, ref)

	view := db.engine.exec.AcquireView(snap)
	defer db.engine.exec.ReleaseView(view)
	return qagg.Count(view, prepared, true)
}

// Query evaluates the given query against the index and returns all matching values.
func (db *DB[K, V]) Query(q *qx.QX) ([]*V, error) {
	if err := db.beginOp(); err != nil {
		return nil, err
	}
	defer db.endOp()

	if db.engine == nil {
		return nil, ErrNoIndex
	}
	if q == nil {
		return nil, fmt.Errorf("QX is nil")
	}
	prepared, err := qir.PrepareQuery(q, db.engine.schema.IndexedByName)
	if err != nil {
		return nil, err
	}
	defer prepared.Release()
	viewQ := qir.NewShape(prepared)

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		return nil, err
	}
	defer db.engine.snapshot.Unpin(seq, ref)
	defer rollback(tx)

	return db.queryRecords(tx, snap, &viewQ)
}

func (db *DB[K, V]) beginQueryTxSnapshot() (*bbolt.Tx, *snapshot.View, uint64, *snapshot.Ref, error) {
	// Hold the registry read lock across Begin(false) -> Sequence() -> pin so a
	// writer cannot publish/retire away the exact snapshot needed by this read tx
	// in the gap between opening the tx and pinning its sequence-aligned snapshot.
	guard := db.engine.snapshot.LockPin()
	tx, err := db.bolt.Begin(false)
	if err != nil {
		guard.Unlock()
		return nil, nil, 0, nil, fmt.Errorf("tx error: %w", err)
	}

	bucket := tx.Bucket(db.bucket)
	if bucket == nil {
		guard.Unlock()
		_ = tx.Rollback()
		return nil, nil, 0, nil, fmt.Errorf("bucket does not exist")
	}

	seq := bucket.Sequence()
	snap, ref, ok := guard.PinBySeq(seq)
	if !ok {
		guard.Unlock()
		_ = tx.Rollback()
		if err = db.unavailableErr(); err != nil {
			return nil, nil, 0, nil, err
		}
		return nil, nil, 0, nil, fmt.Errorf("snapshot sequence %d is not available", seq)
	}
	guard.Unlock()
	return tx, snap, seq, ref, nil
}

func (db *DB[K, V]) queryRecords(tx *bbolt.Tx, snap *snapshot.View, q *qir.Shape) ([]*V, error) {
	view := db.engine.exec.AcquireView(snap)
	defer db.engine.exec.ReleaseView(view)

	if !db.engine.exec.TraceSamplingEnabled() {
		if empty, err := view.TryQueryEmptyOnSnapshot(q); empty || err != nil {
			return nil, err
		}
	}

	ids, err := view.Query(q, true)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	values, err := db.batchGetTxCompactByIdx(tx, snap, ids)
	if err != nil {
		return nil, err
	}
	return values, nil
}

func (db *DB[K, V]) batchGetTxCompactByIdx(tx *bbolt.Tx, snap *snapshot.View, ids []uint64) ([]*V, error) {
	bucket := tx.Bucket(db.bucket)
	if bucket == nil {
		return nil, fmt.Errorf("bucket does not exist")
	}
	if len(ids) == 0 {
		return nil, nil
	}

	out := make([]*V, 0, len(ids))
	if db.strKey {
		strmapView := snap.StrMap
		lookup := strmapView.Lookup()
		for _, idx := range ids {
			s, ok := lookup.String(idx)
			if !ok {
				panic("rbi: no string key associated with snapshot idx")
			}
			v := bucket.Get(keycodec.StringBytes(s))
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

	var key [8]byte
	for _, idx := range ids {
		v := bucket.Get(keycodec.U64BytesWithBuf(idx, &key))
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

// QueryKeys evaluates the given query against the index and returns all matching ids.
func (db *DB[K, V]) QueryKeys(q *qx.QX) ([]K, error) {
	if err := db.beginOp(); err != nil {
		return nil, err
	}
	defer db.endOp()

	if db.engine == nil {
		return nil, ErrNoIndex
	}
	if q == nil {
		return nil, fmt.Errorf("QX is nil")
	}
	prepared, err := qir.PrepareQuery(q, db.engine.schema.IndexedByName)
	if err != nil {
		return nil, err
	}
	defer prepared.Release()
	viewQ := qir.NewShape(prepared)

	snap, seq, ref := db.engine.snapshot.PinCurrent()
	defer db.engine.snapshot.Unpin(seq, ref)

	view := db.engine.exec.AcquireView(snap)
	defer db.engine.exec.ReleaseView(view)

	ids, err := view.Query(&viewQ, true)
	if err != nil {
		return nil, err
	}
	return db.queryKeysFromIDs(snap, ids), nil
}

func (db *DB[K, V]) queryKeysFromIDs(snap *snapshot.View, ids []uint64) []K {
	if len(ids) == 0 {
		return nil
	}
	if !db.strKey {
		return unsafe.Slice((*K)(unsafe.Pointer(&ids[0])), len(ids))
	}
	out := make([]K, len(ids))
	strmapView := snap.StrMap
	lookup := strmapView.Lookup()
	for i, idx := range ids {
		s, ok := lookup.String(idx)
		if !ok {
			panic("rbi: no string key associated with snapshot idx")
		}
		out[i] = *(*K)(unsafe.Pointer(&s))
	}
	return out
}

type queryEngine struct {
	snapshot               *snapshot.Manager
	schema                 *schema.Runtime
	index                  []indexdata.FieldStorage
	nilIndex               []indexdata.FieldStorage
	lenIndex               []indexdata.FieldStorage
	lenZeroComplement      []bool
	measure                []indexdata.MeasureStorage
	universe               posting.List
	lenIndexLoaded         bool
	matPredCacheMaxEntries int
	matPredCacheMaxCard    uint64

	exec *qexec.Runtime
}

func (qe *queryEngine) snapshotCacheConfig() snapshot.CacheConfig {
	return snapshot.CacheConfig{
		MatPredMaxEntries: qe.matPredCacheMaxEntries,
		MatPredMaxCard:    qe.matPredCacheMaxCard,
	}
}

func (qe *queryEngine) installViewNoLock(s *snapshot.View) {
	qe.index = s.Index
	qe.nilIndex = s.NilIndex
	qe.lenIndex = s.LenIndex
	qe.lenZeroComplement = s.LenZeroComplement
	qe.measure = s.Measure
	qe.universe = s.Universe
	qe.snapshot.Publish(s)
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
	if err := db.beginOp(); err != nil {
		return err
	}
	defer db.endOp()

	if db.engine == nil {
		return ErrNoIndex
	}

	db.engine.exec.Analyzer.Lock()
	defer db.engine.exec.Analyzer.Unlock()

	if err := db.unavailableErr(); err != nil {
		return err
	}
	snap, seq, ref := db.engine.snapshot.PinCurrent()
	defer db.engine.snapshot.Unpin(seq, ref)

	db.engine.exec.RefreshPlannerStatsOnSnapshot(snap)
	return nil
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
	interval := db.engine.exec.Analyzer.Interval
	if interval <= 0 {
		return
	}
	if db.engine.exec.Analyzer.Stop != nil {
		return
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	db.engine.exec.Analyzer.Stop = stop
	db.engine.exec.Analyzer.Done = done

	go db.runPlannerAnalyzeLoop(stop, done, interval)
}

func (db *DB[K, V]) stopAnalyzeLoop() {
	if db.engine == nil {
		return
	}
	stop := db.engine.exec.Analyzer.Stop
	done := db.engine.exec.Analyzer.Done

	if stop != nil {
		select {
		case <-stop:
		default:
			close(stop)
		}
	}
	if done != nil {
		<-done
	}

	db.engine.exec.Analyzer.Stop = nil
	db.engine.exec.Analyzer.Done = nil
}

func (db *DB[K, V]) runPlannerAnalyzeLoop(stop <-chan struct{}, done chan<- struct{}, base time.Duration) {
	defer close(done)

	rng := newRand(time.Now().UnixNano())
	failures := 0

	timer := time.NewTimer(nextAnalyzeDelay(base, failures, rng))
	defer timer.Stop()

	for {
		select {
		case <-stop:
			return
		case <-timer.C:
		}

		err := db.RefreshPlannerStats()
		if err != nil {
			if errors.Is(err, ErrClosed) || errors.Is(err, ErrBroken) {
				return
			}
			if errors.Is(err, ErrRebuildInProgress) {
				failures = 0
			} else {
				failures++
			}
		} else {
			failures = 0
		}

		next := nextAnalyzeDelay(base, failures, rng)
		timer.Reset(next)
	}
}

func nextAnalyzeDelay(base time.Duration, failures int, rng *rand.Rand) time.Duration {
	d := base
	if failures > 0 {
		pow := failures
		if pow > 3 {
			pow = 3
		}
		d = base * time.Duration(1<<pow)
		m := base * 8
		if d > m {
			d = m
		}
	}
	return addPositiveJitter(d, rng)
}

func addPositiveJitter(d time.Duration, rng *rand.Rand) time.Duration {
	if d <= 0 {
		return d
	}
	// Add 0..20% positive jitter to de-synchronize multiple instances.
	j := d / 5
	if j <= 0 {
		return d
	}
	return d + time.Duration(rng.Int64N(int64(j)+1))
}

func (db *DB[K, V]) refreshPlannerStatsLocked() {
	version := db.engine.exec.StatsVersion.Add(1)
	snap, seq, ref := db.engine.snapshot.PinCurrent()
	defer db.engine.snapshot.Unpin(seq, ref)
	s := db.engine.exec.BuildPlannerStatsSnapshot(snap, version)
	db.engine.exec.Stats.Store(s)
}

func newRand(seed int64) *rand.Rand {
	s := uint64(seed)
	return rand.New(rand.NewPCG(s, s^randStreamMix))
}

// SnapshotStats returns diagnostics for published index snapshots.
//
// Runtime snapshot diagnostics are collected only when
// Options.EnableSnapshotStats was enabled for this DB instance; otherwise the
// method returns a zero value.
//
// In indexed mode it reports the current published snapshot sequence,
// universe cardinality, registry size, and pin counts.
//
// In transparent mode no published index snapshots are maintained, so the
// returned diagnostics remain zero-valued even when snapshot stats collection
// is enabled.
func (db *DB[K, V]) SnapshotStats() SnapshotStats {
	if db.engine == nil {
		return SnapshotStats{}
	}
	if !db.engine.snapshot.StatsEnabled() {
		return SnapshotStats{}
	}
	if !db.beginOpWait() {
		return SnapshotStats{}
	}
	defer db.endOp()

	snap, seq, ref := db.engine.snapshot.PinCurrent()
	if snap == nil {
		return SnapshotStats{}
	}
	defer db.engine.snapshot.Unpin(seq, ref)

	stats := db.engine.snapshot.Stats(snap, ref)
	return SnapshotStats{
		Sequence:     stats.Sequence,
		UniverseCard: stats.UniverseCard,
		RegistrySize: stats.RegistrySize,
		PinnedRefs:   stats.PinnedRefs,
	}
}
