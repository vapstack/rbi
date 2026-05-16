package qexec

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
)

const DefaultCalibrationSampleEvery = 16

const (
	calibrationMinEstimatedRows = 16
	calibrationRatioMin         = 0.25
	calibrationRatioMax         = 8.0
	calibrationMultiplierMin    = 0.40
	calibrationMultiplierMax    = 4.0
)

type Config struct {
	Schema *schema.Runtime

	NumericRangeBucketSize         int
	NumericRangeBucketMinFieldKeys int
	NumericRangeBucketMinSpanKeys  int

	AnalyzeInterval   time.Duration
	AnalyzeSoftBudget time.Duration

	TraceSink        func(TraceEvent)
	TraceSampleEvery int

	CalibrationEnabled     bool
	CalibrationSampleEvery int
	CalibrationPersistPath string
}

type Runtime struct {
	Schema *schema.Runtime

	NumericRangeBucketSize         int
	NumericRangeBucketMinFieldKeys int
	NumericRangeBucketMinSpanKeys  int

	StatsVersion atomic.Uint64
	Stats        atomic.Pointer[PlannerStatsSnapshot]

	Analyzer   *Analyzer
	Tracer     *Tracer
	Calibrator *Calibrator

	viewPool pooled.Pointers[View]
}

func NewRuntime(cfg Config) *Runtime {
	return &Runtime{
		Schema:                         cfg.Schema,
		NumericRangeBucketSize:         cfg.NumericRangeBucketSize,
		NumericRangeBucketMinFieldKeys: cfg.NumericRangeBucketMinFieldKeys,
		NumericRangeBucketMinSpanKeys:  cfg.NumericRangeBucketMinSpanKeys,
		Analyzer: &Analyzer{
			Interval:   cfg.AnalyzeInterval,
			SoftBudget: cfg.AnalyzeSoftBudget,
		},
		Tracer:     NewTracer(cfg.TraceSink, cfg.TraceSampleEvery),
		Calibrator: NewCalibrator(cfg.CalibrationEnabled, cfg.CalibrationSampleEvery, cfg.CalibrationPersistPath),
		viewPool: pooled.Pointers[View]{
			Clear: true,
		},
	}
}

func (r *Runtime) AcquireView(snap *snapshot.View) *View {
	view := r.viewPool.Get()
	*view = newView(snap, r)
	return view
}

func (r *Runtime) ReleaseView(view *View) {
	r.viewPool.Put(view)
}

func (r *Runtime) FieldNameByOrdinal(ordinal int) string {
	if ordinal < 0 || ordinal >= len(r.Schema.Indexed) {
		return ""
	}
	return r.Schema.Indexed[ordinal].Name
}

type Analyzer struct {
	Interval   time.Duration
	Stop       chan struct{}
	Done       chan struct{}
	SoftBudget time.Duration
	Cursor     int

	sync.Mutex
}

type Tracer struct {
	sink        func(TraceEvent)
	sampleEvery uint64
	seq         atomic.Uint64
}

func NewTracer(sink func(TraceEvent), sampleEvery int) *Tracer {
	return &Tracer{
		sink:        sink,
		sampleEvery: TraceSampleEvery(sampleEvery, sink),
	}
}

func TraceSampleEvery(sampleEvery int, sink func(TraceEvent)) uint64 {
	if sink == nil {
		return 0
	}
	if sampleEvery < 0 {
		return 0
	}
	if sampleEvery == 0 {
		return 1
	}
	return uint64(sampleEvery)
}

func (t *Tracer) Enabled() bool {
	return t.sink != nil && t.sampleEvery > 0
}

func (t *Tracer) SampleEvery() uint64 {
	return t.sampleEvery
}

func (t *Tracer) SampleSink() func(TraceEvent) {
	sink := t.sink
	every := t.sampleEvery
	if sink == nil || every == 0 {
		return nil
	}
	if every <= 1 {
		return sink
	}
	if t.seq.Add(1)%every == 0 {
		return sink
	}
	return nil
}

type CalPlan uint8

const (
	CalORNoOrder CalPlan = iota
	CalOROrderMerge
	CalOROrderStream
	CalOrdered
	CalLimitOrderBasic
	CalLimitOrderPrefix
	CalPlanCount
)

var calPlanNames = [CalPlanCount]PlanName{
	PlanORMergeNoOrder,
	PlanORMergeOrderMerge,
	PlanORMergeOrderStream,
	PlanOrdered,
	PlanLimitOrderBasic,
	PlanLimitOrderPrefix,
}

type Calibration struct {
	UpdatedAt   time.Time
	Multipliers [CalPlanCount]float64
	Samples     [CalPlanCount]uint64
}

type Calibrator struct {
	enabled     bool
	sampleEvery uint64
	seq         atomic.Uint64
	state       atomic.Pointer[Calibration]
	persistPath string

	sync.Mutex
}

func NewCalibrator(enabled bool, sampleEvery int, persistPath string) *Calibrator {
	return &Calibrator{
		enabled:     enabled,
		sampleEvery: CalibrationSampleEvery(enabled, sampleEvery),
		persistPath: persistPath,
	}
}

func CalibrationSampleEvery(enabled bool, sampleEvery int) uint64 {
	if !enabled {
		return 0
	}
	if sampleEvery < 0 {
		return 0
	}
	if sampleEvery == 0 {
		return DefaultCalibrationSampleEvery
	}
	return uint64(sampleEvery)
}

func NewCalibration() *Calibration {
	s := &Calibration{
		UpdatedAt: time.Now(),
	}
	for i := range s.Multipliers {
		s.Multipliers[i] = 1.0
	}
	return s
}

func (c *Calibrator) Enabled() bool {
	return c.enabled
}

func (c *Calibrator) SampleEvery() uint64 {
	return c.sampleEvery
}

func (c *Calibrator) PersistPath() string {
	return c.persistPath
}

func (c *Calibrator) SamplingEnabled() bool {
	return c.enabled && c.sampleEvery > 0
}

func (c *Calibrator) TakeSample() bool {
	if !c.enabled {
		return false
	}
	every := c.sampleEvery
	if every == 0 {
		return false
	}
	if every <= 1 {
		return true
	}
	return c.seq.Add(1)%every == 0
}

func (c *Calibrator) Init() {
	if c.enabled && c.state.Load() == nil {
		c.state.Store(NewCalibration())
	}
}

func (c *Calibrator) HasState() bool {
	return c.state.Load() != nil
}

func (c *Calibrator) Snapshot() (CalibrationSnapshot, bool) {
	cur := c.state.Load()
	if cur == nil {
		return CalibrationSnapshot{}, false
	}
	return CalibrationSnapshotFromState(cur), true
}

func (c *Calibrator) SnapshotOrNew() CalibrationSnapshot {
	cur := c.state.Load()
	if cur == nil {
		return CalibrationSnapshotFromState(NewCalibration())
	}
	return CalibrationSnapshotFromState(cur)
}

func (c *Calibrator) SetSnapshot(s CalibrationSnapshot) error {
	state, err := CalibrationStateFromSnapshot(s)
	if err != nil {
		return err
	}
	c.Lock()
	c.state.Store(state)
	c.Unlock()
	return nil
}

func (c *Calibrator) CostMultiplier(plan CalPlan) float64 {
	cur := c.state.Load()
	if cur == nil {
		return 1.0
	}
	m := cur.Multipliers[plan]
	if m <= 0 || math.IsNaN(m) || math.IsInf(m, 0) {
		return 1.0
	}
	return m
}

func (c *Calibrator) Observe(ev TraceEvent) {
	if !c.enabled {
		return
	}

	plan, ok := calPlanByName(ev.Plan)
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
	ratio = ClampFloat(ratio, calibrationRatioMin, calibrationRatioMax)

	c.Lock()
	defer c.Unlock()

	cur := c.state.Load()
	if cur == nil {
		cur = NewCalibration()
	}

	next := *cur
	old := next.Multipliers[plan]
	if old <= 0 || math.IsNaN(old) || math.IsInf(old, 0) {
		old = 1.0
	}

	alpha := calibrationAlpha(next.Samples[plan])
	next.Multipliers[plan] = ClampFloat(
		old+alpha*(ratio-old),
		calibrationMultiplierMin,
		calibrationMultiplierMax,
	)
	next.Samples[plan] = next.Samples[plan] + 1
	next.UpdatedAt = time.Now()

	c.state.Store(&next)
}

func calPlanByName(name string) (CalPlan, bool) {
	for i := range calPlanNames {
		if string(calPlanNames[i]) == name {
			return CalPlan(i), true
		}
	}
	return 0, false
}

func calPlanName(plan CalPlan) PlanName {
	if plan >= CalPlanCount {
		return ""
	}
	return calPlanNames[plan]
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

func ClampFloat(v, lo, hi float64) float64 {
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

func CalibrationSnapshotFromState(s *Calibration) CalibrationSnapshot {
	out := CalibrationSnapshot{
		UpdatedAt:   time.Now(),
		Multipliers: make(map[string]float64, int(CalPlanCount)),
		Samples:     make(map[string]uint64, int(CalPlanCount)),
	}
	if s == nil {
		return out
	}

	out.UpdatedAt = s.UpdatedAt
	for i := CalPlan(0); i < CalPlanCount; i++ {
		name := calPlanName(i)
		if name == "" {
			continue
		}
		out.Multipliers[string(name)] = s.Multipliers[i]
		out.Samples[string(name)] = s.Samples[i]
	}
	return out
}

func CalibrationStateFromSnapshot(s CalibrationSnapshot) (*Calibration, error) {
	st := NewCalibration()
	if !s.UpdatedAt.IsZero() {
		st.UpdatedAt = s.UpdatedAt
	}

	for name, m := range s.Multipliers {
		plan, ok := calPlanByName(name)
		if !ok {
			return nil, errors.New("unknown planner calibration plan: " + name)
		}
		st.Multipliers[plan] = ClampFloat(m, calibrationMultiplierMin, calibrationMultiplierMax)
	}
	for name, n := range s.Samples {
		plan, ok := calPlanByName(name)
		if !ok {
			return nil, errors.New("unknown planner calibration plan: " + name)
		}
		st.Samples[plan] = n
	}
	return st, nil
}
