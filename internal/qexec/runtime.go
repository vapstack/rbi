package qexec

import (
	"math"
	goruntime "runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/rbistats"
	"github.com/vapstack/rbi/rbitrace"
)

type KeyMode uint8

const (
	KeyModeNone KeyMode = iota
	KeyModeString
	KeyModeNumeric
)

type queryFieldKind uint8

const (
	queryFieldOrdinary queryFieldKind = iota
	queryFieldStringKey
	queryFieldNumericKey
)

type queryField struct {
	name           string
	kind           queryFieldKind
	ordinal        int
	storageOrdinal int
	meta           *schema.Field
}

type Config struct {
	Schema  *schema.Schema
	StrKey  bool
	KeyMode KeyMode

	AsyncMaterializedPredMaxWorkers int
	asyncMaterializedPredStats      bool

	NumericRangeBucketSize              int
	NumericRangeBucketMinFieldKeys      int
	NumericRangeBucketMinSpanKeys       int
	NumericRangeSpanCacheMaxEntries     int
	NumericRangeSpanCacheMaxEntryBytes  int64
	NumericRangeExactCacheMaxEntries    int
	NumericRangeExactCacheMaxEntryBytes int64

	AnalyzeInterval time.Duration

	TraceSink        func(rbitrace.Event)
	TraceSampleEvery int
}

type Runtime struct {
	Schema  *schema.Schema
	StrKey  bool
	KeyMode KeyMode

	NumericRangeBucketSize              int
	NumericRangeBucketMinFieldKeys      int
	NumericRangeBucketMinSpanKeys       int
	NumericRangeSpanCacheMaxEntries     int
	NumericRangeSpanCacheMaxEntryBytes  int64
	NumericRangeExactCacheMaxEntries    int
	NumericRangeExactCacheMaxEntryBytes int64

	StatsVersion atomic.Uint64
	Stats        atomic.Pointer[rbistats.PlannerSnapshot]

	fields      []queryField
	fieldByName map[string]int
	keyOrdinal  int

	Analyzer *Analyzer
	Tracer   *Tracer

	viewPool                  pooled.Pointers[View]
	asyncMaterializedPredWarm *asyncMaterializedPredScheduler
}

func NewRuntime(cfg Config) *Runtime {
	fields, fieldByName, keyOrdinal := buildQueryFieldCatalog(cfg.Schema, cfg.KeyMode)
	return &Runtime{
		Schema:                              cfg.Schema,
		StrKey:                              cfg.StrKey,
		KeyMode:                             cfg.KeyMode,
		NumericRangeBucketSize:              cfg.NumericRangeBucketSize,
		NumericRangeBucketMinFieldKeys:      cfg.NumericRangeBucketMinFieldKeys,
		NumericRangeBucketMinSpanKeys:       cfg.NumericRangeBucketMinSpanKeys,
		NumericRangeSpanCacheMaxEntries:     cfg.NumericRangeSpanCacheMaxEntries,
		NumericRangeSpanCacheMaxEntryBytes:  cfg.NumericRangeSpanCacheMaxEntryBytes,
		NumericRangeExactCacheMaxEntries:    cfg.NumericRangeExactCacheMaxEntries,
		NumericRangeExactCacheMaxEntryBytes: cfg.NumericRangeExactCacheMaxEntryBytes,
		Analyzer: &Analyzer{
			Interval: cfg.AnalyzeInterval,
		},
		Tracer:      NewTracer(cfg.TraceSink, cfg.TraceSampleEvery),
		fields:      fields,
		fieldByName: fieldByName,
		keyOrdinal:  keyOrdinal,
		viewPool: pooled.Pointers[View]{
			Clear: true,
		},
		asyncMaterializedPredWarm: newAsyncMaterializedPredScheduler(
			asyncMaterializedPredWorkerLimit(cfg.AsyncMaterializedPredMaxWorkers),
			cfg.asyncMaterializedPredStats,
		),
	}
}

func asyncMaterializedPredWorkerLimit(configured int) int {
	if configured > 0 {
		return configured
	}
	n := goruntime.GOMAXPROCS(0) / 4
	if n < 1 {
		return 1
	}
	return n
}

func buildQueryFieldCatalog(s *schema.Schema, mode KeyMode) ([]queryField, map[string]int, int) {
	if s == nil {
		return nil, nil, -1
	}
	n := len(s.Indexed)
	extra := 0
	keyOrdinal := -1
	if mode == KeyModeString || mode == KeyModeNumeric {
		extra = 1
		keyOrdinal = n
	}
	fields := make([]queryField, n+extra)
	fieldByName := make(map[string]int, n+extra)

	for i := range s.Indexed {
		acc := s.Indexed[i]
		fields[i] = queryField{
			name:           acc.Name,
			kind:           queryFieldOrdinary,
			ordinal:        i,
			storageOrdinal: acc.Ordinal,
			meta:           acc.Field,
		}
		fieldByName[acc.Name] = i
	}

	switch mode {

	case KeyModeString:
		fields[keyOrdinal] = queryField{
			name:           schema.ReservedKeyFieldName,
			kind:           queryFieldStringKey,
			ordinal:        keyOrdinal,
			storageOrdinal: -1,
			meta:           &stringKeyField,
		}
		fieldByName[schema.ReservedKeyFieldName] = keyOrdinal

	case KeyModeNumeric:
		fields[keyOrdinal] = queryField{
			name:           schema.ReservedKeyFieldName,
			kind:           queryFieldNumericKey,
			ordinal:        keyOrdinal,
			storageOrdinal: -1,
			meta:           &numericKeyField,
		}
		fieldByName[schema.ReservedKeyFieldName] = keyOrdinal
	}

	return fields, fieldByName, keyOrdinal
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
	if ordinal < 0 || ordinal >= len(r.fields) {
		return ""
	}
	return r.fields[ordinal].name
}

func (r *Runtime) ResolveField(name string) (qir.FieldInfo, bool) {
	ordinal, ok := r.fieldByName[name]
	if !ok {
		return qir.FieldInfo{Ordinal: qir.NoFieldOrdinal}, false
	}
	field := r.fields[ordinal]
	info := qir.FieldInfo{Ordinal: field.ordinal}
	if field.kind == queryFieldOrdinary {
		info.Caps = schema.FieldQueryCaps(field.meta)
	}
	return info, true
}

type Analyzer struct {
	Interval time.Duration
	Stop     chan struct{}
	Done     chan struct{}

	sync.Mutex
}

type Tracer struct {
	sink        func(rbitrace.Event)
	sampleEvery uint64
	seq         atomic.Uint64
}

func NewTracer(sink func(rbitrace.Event), sampleEvery int) *Tracer {
	return &Tracer{
		sink:        sink,
		sampleEvery: TraceSampleEvery(sampleEvery, sink),
	}
}

func TraceSampleEvery(sampleEvery int, sink func(rbitrace.Event)) uint64 {
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

func (t *Tracer) SampleSink() func(rbitrace.Event) {
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
