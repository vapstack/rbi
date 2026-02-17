package rbi

import (
	"time"

	"github.com/vapstack/qx"
)

// TraceEvent is an optional per-query planner execution trace.
// It is emitted only when TraceSink is configured.
type TraceEvent struct {
	Timestamp time.Time
	Duration  time.Duration

	Plan string

	HasOrder   bool
	OrderField string
	OrderDesc  bool
	Offset     uint64
	Limit      uint64

	LeafCount int
	HasNeg    bool
	HasPrefix bool

	EstimatedRows uint64
	EstimatedCost float64
	FallbackCost  float64

	RowsExamined uint64
	RowsReturned uint64

	// ORBranches contains per-branch runtime metrics for OR plans.
	ORBranches []TraceORBranch

	// OrderIndexScanWidth is the number of non-empty order-index buckets
	// traversed while producing query output.
	OrderIndexScanWidth uint64

	// DedupeCount is the number of duplicate candidates dropped globally.
	DedupeCount uint64

	// EarlyStopReason explains why execution stopped early.
	// Examples: "limit_reached", "input_exhausted", "candidates_exhausted".
	EarlyStopReason string

	Error string
}

type TraceORBranch struct {
	Index int

	RowsExamined uint64
	RowsEmitted  uint64

	Skipped    bool
	SkipReason string
}

type queryTrace struct {
	sink     func(TraceEvent)
	onFinish func(TraceEvent)
	start    time.Time
	ev       TraceEvent
}

func resolveTraceSampleEvery(sampleEvery uint64, sink func(TraceEvent)) uint64 {
	if sink == nil {
		return 0
	}
	if sampleEvery == 0 {
		return 1
	}
	return sampleEvery
}

func (db *DB[K, V]) beginTrace(q *qx.QX) *queryTrace {
	emitTrace := false
	sink := db.planner.tracer.sink
	if sink != nil && db.planner.tracer.sampleEvery > 0 {
		emitTrace = db.shouldSampleTrace()
	}

	emitCalibration := false
	if db.planner.calibrator.enabled && db.planner.calibrator.sampleEvery > 0 {
		emitCalibration = db.shouldSampleCalibration()
	}

	if !emitTrace && !emitCalibration {
		return nil
	}

	ev := TraceEvent{
		Timestamp: time.Now(),
		Offset:    q.Offset,
		Limit:     q.Limit,
	}
	if len(q.Order) > 0 {
		ev.HasOrder = true
		ev.OrderField = q.Order[0].Field
		ev.OrderDesc = q.Order[0].Desc
	}

	leaves, ok := collectAndLeaves(q.Expr)
	if ok {
		ev.LeafCount = len(leaves)
		for _, e := range leaves {
			if e.Not {
				ev.HasNeg = true
			}
			if e.Op == qx.OpPREFIX {
				ev.HasPrefix = true
			}
		}
	}

	tr := &queryTrace{
		start: ev.Timestamp,
		ev:    ev,
	}
	if emitTrace {
		tr.sink = sink
	}
	if emitCalibration {
		tr.onFinish = db.observeCalibration
	}
	return tr
}

func (db *DB[K, V]) shouldSampleTrace() bool {
	every := db.planner.tracer.sampleEvery
	if every <= 1 {
		return true
	}
	seq := db.planner.tracer.seq.Add(1)
	return seq%every == 0
}

func (t *queryTrace) setPlan(plan PlanName) {
	if t == nil {
		return
	}
	t.ev.Plan = string(plan)
}

func (t *queryTrace) addExamined(n uint64) {
	if t == nil || n == 0 {
		return
	}
	t.ev.RowsExamined += n
}

func (t *queryTrace) addOrderScanWidth(n uint64) {
	if t == nil || n == 0 {
		return
	}
	t.ev.OrderIndexScanWidth += n
}

func (t *queryTrace) addDedupe(n uint64) {
	if t == nil || n == 0 {
		return
	}
	t.ev.DedupeCount += n
}

func (t *queryTrace) setEarlyStopReason(reason string) {
	if t == nil || reason == "" {
		return
	}
	if t.ev.EarlyStopReason != "" {
		return
	}
	t.ev.EarlyStopReason = reason
}

func (t *queryTrace) setORBranches(branches []TraceORBranch) {
	if t == nil {
		return
	}
	if len(branches) == 0 {
		t.ev.ORBranches = nil
		return
	}
	t.ev.ORBranches = append(t.ev.ORBranches[:0], branches...)
}

func (t *queryTrace) setEstimated(rows uint64, estCost, fallbackCost float64) {
	if t == nil {
		return
	}
	t.ev.EstimatedRows = rows
	t.ev.EstimatedCost = estCost
	t.ev.FallbackCost = fallbackCost
}

func (t *queryTrace) finish(rowsReturned uint64, err error) {
	if t == nil {
		return
	}
	t.ev.Duration = time.Since(t.start)
	t.ev.RowsReturned = rowsReturned
	if err != nil {
		t.ev.Error = err.Error()
	}

	if t.onFinish != nil {
		func() {
			// defer func() {
			// 	_ = recover()
			// }()
			t.onFinish(t.ev)
		}()
	}

	if t.sink != nil {
		func() {
			// defer func() {
			// 	_ = recover()
			// }()
			t.sink(t.ev)
		}()
	}
}
