package rbi

import (
	"math"
	"time"

	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/qir"
)

type queryEngine struct {
	strkey bool

	fields             map[string]*field
	indexedFieldAccess []indexedFieldAccessor
	indexedFieldMap    indexedFieldMap
	measureFieldAccess []measureFieldAccessor
	measureFieldMap    measureFieldMap

	planner *planner
	options *Options

	viewPool *pooled.Pointers[queryView]
}

func (e *queryEngine) fieldNameByOrdinal(ordinal int) string {
	if ordinal < 0 || ordinal >= len(e.indexedFieldAccess) {
		return ""
	}
	return e.indexedFieldAccess[ordinal].name
}

func (e *queryEngine) traceOrCalibrationSamplingEnabled() bool {
	if e.planner.tracer.sink != nil && e.planner.tracer.sampleEvery > 0 {
		return true
	}
	return e.planner.calibrator.enabled && e.planner.calibrator.sampleEvery > 0
}

func (e *queryEngine) beginTrace(q qir.Shape) *queryTrace {
	emitTrace := false
	sink := e.planner.tracer.sink
	if sink != nil && e.planner.tracer.sampleEvery > 0 {
		emitTrace = e.shouldSampleTrace()
	}

	emitCalibration := false
	if e.planner.calibrator.enabled && e.planner.calibrator.sampleEvery > 0 {
		emitCalibration = e.shouldSampleCalibration()
	}

	if !emitTrace && !emitCalibration {
		return nil
	}

	tr := new(queryTrace)
	if emitTrace {
		tr.ev = TraceEvent{
			Timestamp: time.Now(),
			Offset:    q.Offset,
			Limit:     q.Limit,
		}
		if q.HasOrder {
			tr.ev.HasOrder = true
			tr.ev.OrderField = e.fieldNameByOrdinal(q.Order.FieldOrdinal)
			tr.ev.OrderDesc = q.Order.Desc
		}

		var leavesBuf [8]qir.Expr
		leaves, ok := collectAndLeavesModeScratch(q.Expr, leavesBuf[:0], andLeafModeCollect)
		if ok {
			tr.ev.LeafCount = len(leaves)
			for _, expr := range leaves {
				if expr.Not {
					tr.ev.HasNeg = true
				}
				if expr.Op == qir.OpPREFIX {
					tr.ev.HasPrefix = true
				}
			}
		}

		tr.start = tr.ev.Timestamp
		tr.sink = sink
	}
	if emitCalibration {
		tr.onFinish = e.observeCalibration
	}
	return tr
}

func (e *queryEngine) shouldSampleTrace() bool {
	every := e.planner.tracer.sampleEvery
	if every <= 1 {
		return true
	}
	seq := e.planner.tracer.seq.Add(1)
	return seq%every == 0
}

func (e *queryEngine) shouldSampleCalibration() bool {
	every := e.planner.calibrator.sampleEvery
	if every <= 1 {
		return true
	}
	seq := e.planner.calibrator.seq.Add(1)
	return seq%every == 0
}

func (e *queryEngine) plannerCostMultiplier(plan plannerCalPlan) float64 {
	cur := e.planner.calibrator.state.Load()
	if cur == nil {
		return 1.0
	}
	m := cur.Multipliers[plan]
	if m <= 0 || math.IsNaN(m) || math.IsInf(m, 0) {
		return 1.0
	}
	return m
}

func (e *queryEngine) observeCalibration(ev TraceEvent) {
	if !e.planner.calibrator.enabled {
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

	e.planner.calibrator.Lock()
	defer e.planner.calibrator.Unlock()

	cur := e.planner.calibrator.state.Load()
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

	e.planner.calibrator.state.Store(&next)
}
