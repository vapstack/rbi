package qexec

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
)

type Config struct {
	Schema *schema.Schema

	NumericRangeBucketSize         int
	NumericRangeBucketMinFieldKeys int
	NumericRangeBucketMinSpanKeys  int

	AnalyzeInterval time.Duration

	TraceSink        func(TraceEvent)
	TraceSampleEvery int
}

type Runtime struct {
	Schema *schema.Schema

	NumericRangeBucketSize         int
	NumericRangeBucketMinFieldKeys int
	NumericRangeBucketMinSpanKeys  int

	StatsVersion atomic.Uint64
	Stats        atomic.Pointer[PlannerStatsSnapshot]

	Analyzer *Analyzer
	Tracer   *Tracer

	viewPool pooled.Pointers[View]
}

func NewRuntime(cfg Config) *Runtime {
	return &Runtime{
		Schema:                         cfg.Schema,
		NumericRangeBucketSize:         cfg.NumericRangeBucketSize,
		NumericRangeBucketMinFieldKeys: cfg.NumericRangeBucketMinFieldKeys,
		NumericRangeBucketMinSpanKeys:  cfg.NumericRangeBucketMinSpanKeys,
		Analyzer: &Analyzer{
			Interval: cfg.AnalyzeInterval,
		},
		Tracer: NewTracer(cfg.TraceSink, cfg.TraceSampleEvery),
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
	Interval time.Duration
	Stop     chan struct{}
	Done     chan struct{}

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
