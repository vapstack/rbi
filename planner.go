package rbi

import (
	"encoding/json"
	"errors"
	"math/rand/v2"
	"os"
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
	TraceEvent          = qexec.TraceEvent
	TraceORRoute        = qexec.TraceORRoute
	TraceORBranch       = qexec.TraceORBranch
	PlannerFieldStats   = qexec.PlannerFieldStats
	CalibrationSnapshot = qexec.CalibrationSnapshot
)

func (db *DB[K, V]) initCalibration() {
	db.engine.exec.Calibrator.Init()

	path := db.engine.exec.Calibrator.PersistPath()
	if path == "" {
		return
	}

	if err := db.LoadCalibration(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		db.logger.Printf("rbi: failed to load planner calibration (%v): %v", path, err)
	}
}

func (db *DB[K, V]) persistCalibrationOnClose() error {
	path := db.engine.exec.Calibrator.PersistPath()
	if path == "" {
		return nil
	}
	if !db.engine.exec.Calibrator.HasState() {
		return nil
	}
	return db.saveCalibration(path)
}

// GetCalibrationSnapshot returns a copy of current planner calibration state.
//
// The bool result is false if state was not initialized yet.
//
// In indexed mode startup initializes calibration state when calibration is
// enabled. In transparent mode no runtime planner exists, so this method
// returns (zero, false).
func (db *DB[K, V]) GetCalibrationSnapshot() (CalibrationSnapshot, bool) {
	if db.engine == nil {
		return CalibrationSnapshot{}, false
	}
	return db.engine.exec.Calibrator.Snapshot()
}

// SetCalibrationSnapshot replaces planner calibration state with the provided snapshot.
//
// In transparent mode no runtime planner exists, so the method returns ErrNoIndex.
func (db *DB[K, V]) SetCalibrationSnapshot(s CalibrationSnapshot) error {
	if err := db.unavailableErr(); err != nil {
		return err
	}
	if db.engine == nil {
		return ErrNoIndex
	}
	return db.engine.exec.Calibrator.SetSnapshot(s)
}

// SaveCalibration writes planner calibration snapshot to a JSON file.
func (db *DB[K, V]) SaveCalibration(path string) error {
	if db.engine == nil {
		return ErrNoIndex
	}
	return db.saveCalibration(path)
}

func (db *DB[K, V]) saveCalibration(path string) error {
	if path == "" {
		return errors.New("planner calibration path is empty")
	}

	snap, ok := db.GetCalibrationSnapshot()
	if !ok {
		snap = db.engine.exec.Calibrator.SnapshotOrNew()
	}

	raw, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return os.WriteFile(path, raw, 0o644)
}

// LoadCalibration reads planner calibration snapshot from a JSON file.
func (db *DB[K, V]) LoadCalibration(path string) error {
	if err := db.unavailableErr(); err != nil {
		return err
	}
	if db.engine == nil {
		return ErrNoIndex
	}
	if path == "" {
		return errors.New("planner calibration path is empty")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var snap CalibrationSnapshot
	if err = json.Unmarshal(raw, &snap); err != nil {
		return err
	}
	return db.SetCalibrationSnapshot(snap)
}

const (
	// defaultAnalyzeSoftBudget bounds one periodic analyze cycle.
	// A zero/negative value means no time budget.
	defaultAnalyzeSoftBudget = 100 * time.Millisecond
)

// RefreshPlannerStats rebuilds planner statistics from the current in-memory index.
//
// This is a synchronous, full refresh intended for explicit/manual calls.
//
// In indexed mode it scans the current published index snapshot and replaces
// the planner stats payload atomically.
//
// In transparent mode planner stats are disabled because no runtime index
// exists; the method returns ErrNoIndex.
func (db *DB[K, V]) RefreshPlannerStats() error {
	return db.refreshPlannerStatsWithBudget(0, false)
}

func (db *DB[K, V]) refreshPlannerStatsWithBudget(softBudget time.Duration, useCursor bool) error {
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

	db.engine.exec.RefreshPlannerStatsOnSnapshot(snap, softBudget, useCursor)
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

		err := db.refreshPlannerStatsWithBudget(db.engine.exec.Analyzer.SoftBudget, true)
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
