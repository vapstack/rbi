package rbi

import (
	"encoding/json"
	"errors"
	"log"
	"math"
	"os"
	"time"
)

const (
	calibrationMinEstimatedRows = 16
	calibrationRatioMin         = 0.25
	calibrationRatioMax         = 8.0
	calibrationMultiplierMin    = 0.40
	calibrationMultiplierMax    = 4.0
)

type plannerCalPlan uint8

const (
	plannerCalORNoOrder plannerCalPlan = iota
	plannerCalOROrderMerge
	plannerCalOROrderStream
	plannerCalOrdered
	plannerCalLimitOrderBasic
	plannerCalLimitOrderPrefix
	plannerCalPlanCount
)

var plannerCalPlanNames = [plannerCalPlanCount]PlanName{
	PlanORMergeNoOrder,
	PlanORMergeOrderMerge,
	PlanORMergeOrderStream,
	PlanOrdered,
	PlanLimitOrderBasic,
	PlanLimitOrderPrefix,
}

type calibration struct {
	UpdatedAt   time.Time
	Multipliers [plannerCalPlanCount]float64
	Samples     [plannerCalPlanCount]uint64
}

// CalibrationSnapshot is a serializable view of planner calibration
// coefficients and sample counts.
type CalibrationSnapshot struct {
	UpdatedAt   time.Time          `json:"updated_at"`
	Multipliers map[string]float64 `json:"multipliers"`
	Samples     map[string]uint64  `json:"samples"`
}

func calibrationSampleEvery(enabled bool, sampleEvery int) uint64 {
	if !enabled {
		return 0
	}
	if sampleEvery < 0 {
		return 0
	}
	if sampleEvery == 0 {
		return defaultOptionsCalibrationSampleEvery
	}
	return uint64(sampleEvery)
}

func newCalibration() *calibration {
	s := &calibration{
		UpdatedAt: time.Now(),
	}
	for i := range s.Multipliers {
		s.Multipliers[i] = 1.0
	}
	return s
}

func plannerCalPlanByName(name string) (plannerCalPlan, bool) {
	for i := range plannerCalPlanNames {
		if string(plannerCalPlanNames[i]) == name {
			return plannerCalPlan(i), true
		}
	}
	return 0, false
}

func plannerCalPlanName(plan plannerCalPlan) PlanName {
	if plan >= plannerCalPlanCount {
		return ""
	}
	return plannerCalPlanNames[plan]
}

func calibrationAlpha(samples uint64) float64 {
	switch {
	case samples < 8:
		return 0.22
	case samples < 32:
		return 0.14
	case samples < 128:
		return 0.08
	default:
		return 0.04
	}
}

func plannerClampFloat(v, lo, hi float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return lo
	}
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func (db *DB[K, V]) plannerCalibrationRoot() *DB[K, V] {
	if db.traceRoot != nil {
		return db.traceRoot
	}
	return db
}

func (db *DB[K, V]) shouldSampleCalibration() bool {
	every := db.planner.calibrator.sampleEvery
	if every <= 1 {
		return true
	}
	seq := db.planner.calibrator.seq.Add(1)
	return seq%every == 0
}

func (db *DB[K, V]) plannerCostMultiplier(plan plannerCalPlan) float64 {
	root := db.plannerCalibrationRoot()
	cur := root.planner.calibrator.state.Load()
	if cur == nil {
		return 1.0
	}
	m := cur.Multipliers[plan]
	if m <= 0 || math.IsNaN(m) || math.IsInf(m, 0) {
		return 1.0
	}
	return m
}

func (db *DB[K, V]) observeCalibration(ev TraceEvent) {
	root := db.plannerCalibrationRoot()
	if !root.planner.calibrator.enabled {
		return
	}

	plan, ok := plannerCalPlanByName(ev.Plan)
	if !ok {
		return
	}

	if ev.EstimatedRows < calibrationMinEstimatedRows {
		return
	}

	observed := ev.RowsExamined
	if observed == 0 {
		observed = ev.RowsReturned
	}
	if observed == 0 {
		return
	}

	ratio := float64(observed) / float64(ev.EstimatedRows)
	ratio = plannerClampFloat(ratio, calibrationRatioMin, calibrationRatioMax)

	root.planner.calibrator.Lock()
	defer root.planner.calibrator.Unlock()

	cur := root.planner.calibrator.state.Load()
	if cur == nil {
		cur = newCalibration()
	}

	next := *cur
	old := next.Multipliers[plan]
	if old <= 0 || math.IsNaN(old) || math.IsInf(old, 0) {
		old = 1.0
	}

	alpha := calibrationAlpha(next.Samples[plan])
	next.Multipliers[plan] = plannerClampFloat(
		old+alpha*(ratio-old),
		calibrationMultiplierMin,
		calibrationMultiplierMax,
	)
	next.Samples[plan] = next.Samples[plan] + 1
	next.UpdatedAt = time.Now()

	root.planner.calibrator.state.Store(&next)
}

func calibrationSnapshotFromState(s *calibration) CalibrationSnapshot {
	out := CalibrationSnapshot{
		UpdatedAt:   time.Now(),
		Multipliers: make(map[string]float64, int(plannerCalPlanCount)),
		Samples:     make(map[string]uint64, int(plannerCalPlanCount)),
	}
	if s == nil {
		return out
	}

	out.UpdatedAt = s.UpdatedAt
	for i := plannerCalPlan(0); i < plannerCalPlanCount; i++ {
		name := plannerCalPlanName(i)
		if name == "" {
			continue
		}
		out.Multipliers[string(name)] = s.Multipliers[i]
		out.Samples[string(name)] = s.Samples[i]
	}
	return out
}

func calibrationStateFromSnapshot(s CalibrationSnapshot) (*calibration, error) {
	st := newCalibration()
	if !s.UpdatedAt.IsZero() {
		st.UpdatedAt = s.UpdatedAt
	}

	for name, m := range s.Multipliers {
		plan, ok := plannerCalPlanByName(name)
		if !ok {
			return nil, errors.New("unknown planner calibration plan: " + name)
		}
		st.Multipliers[plan] = plannerClampFloat(m, calibrationMultiplierMin, calibrationMultiplierMax)
	}
	for name, n := range s.Samples {
		plan, ok := plannerCalPlanByName(name)
		if !ok {
			return nil, errors.New("unknown planner calibration plan: " + name)
		}
		st.Samples[plan] = n
	}
	return st, nil
}

func (db *DB[K, V]) initCalibration() {
	if db.planner.calibrator.enabled && db.planner.calibrator.state.Load() == nil {
		db.planner.calibrator.state.Store(newCalibration())
	}

	path := db.planner.calibrator.persistPath
	if path == "" {
		return
	}

	if err := db.LoadCalibration(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Printf("rbi: failed to load planner calibration (%v): %v", path, err)
	}
}

func (db *DB[K, V]) persistCalibrationOnClose() error {
	path := db.planner.calibrator.persistPath
	if path == "" {
		return nil
	}
	if db.planner.calibrator.state.Load() == nil {
		return nil
	}
	return db.SaveCalibration(path)
}

// GetCalibrationSnapshot returns a copy of current planner calibration state.
// The bool result is false if state was not initialized yet.
func (db *DB[K, V]) GetCalibrationSnapshot() (CalibrationSnapshot, bool) {
	root := db.plannerCalibrationRoot()
	cur := root.planner.calibrator.state.Load()
	if cur == nil {
		return CalibrationSnapshot{}, false
	}
	return calibrationSnapshotFromState(cur), true
}

// SetCalibrationSnapshot replaces planner calibration state with the provided snapshot.
func (db *DB[K, V]) SetCalibrationSnapshot(s CalibrationSnapshot) error {
	if err := db.unavailableErr(); err != nil {
		return err
	}
	root := db.plannerCalibrationRoot()
	state, err := calibrationStateFromSnapshot(s)
	if err != nil {
		return err
	}

	root.planner.calibrator.Lock()
	root.planner.calibrator.state.Store(state)
	root.planner.calibrator.Unlock()
	return nil
}

// SaveCalibration writes planner calibration snapshot to a JSON file.
func (db *DB[K, V]) SaveCalibration(path string) error {
	if path == "" {
		return errors.New("planner calibration path is empty")
	}

	snap, ok := db.GetCalibrationSnapshot()
	if !ok {
		snap = calibrationSnapshotFromState(newCalibration())
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
