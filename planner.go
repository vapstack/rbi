package rbi

import (
	"errors"
	"math/rand/v2"
	"time"

	"github.com/vapstack/rbi/internal/qagg"
	"github.com/vapstack/rbi/internal/qexec"
)

type PlanName = qexec.PlanName

const (
	PlanMaterialized       = qexec.PlanMaterialized
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

func (qe *queryEngine) plannerStatsSnapshotForPersistLocked(version uint64) *qexec.PlannerStatsSnapshot {
	snap, seq, ref := qe.snapshot.PinCurrent()
	defer qe.snapshot.Unpin(seq, ref)

	return qe.exec.PlannerStatsSnapshotForPersist(snap, version)
}
