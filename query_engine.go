package rbi

import (
	"math"
	"time"

	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/qir"
)

type queryEngine struct {
	strKey bool

	fields             map[string]*field
	indexedFieldAccess []indexedFieldAccessor
	indexedFieldMap    indexedFieldMap
	measureFieldAccess []measureFieldAccessor
	measureFieldMap    measureFieldMap

	planner *planner
	options *Options

	viewPool *pooled.Pointers[queryView]
}

func (qe *queryEngine) fieldNameByOrdinal(ordinal int) string {
	if ordinal < 0 || ordinal >= len(qe.indexedFieldAccess) {
		return ""
	}
	return qe.indexedFieldAccess[ordinal].name
}

func (qe *queryEngine) traceOrCalibrationSamplingEnabled() bool {
	if qe.planner.tracer.sink != nil && qe.planner.tracer.sampleEvery > 0 {
		return true
	}
	return qe.planner.calibrator.enabled && qe.planner.calibrator.sampleEvery > 0
}

func (qe *queryEngine) beginTrace(q qir.Shape) *queryTrace {
	emitTrace := false
	sink := qe.planner.tracer.sink
	if sink != nil && qe.planner.tracer.sampleEvery > 0 {
		emitTrace = qe.shouldSampleTrace()
	}

	emitCalibration := false
	if qe.planner.calibrator.enabled && qe.planner.calibrator.sampleEvery > 0 {
		emitCalibration = qe.shouldSampleCalibration()
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
			tr.ev.OrderField = qe.fieldNameByOrdinal(q.Order.FieldOrdinal)
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
		tr.onFinish = qe.observeCalibration
	}
	return tr
}

func (qe *queryEngine) shouldSampleTrace() bool {
	every := qe.planner.tracer.sampleEvery
	if every <= 1 {
		return true
	}
	seq := qe.planner.tracer.seq.Add(1)
	return seq%every == 0
}

func (qe *queryEngine) shouldSampleCalibration() bool {
	every := qe.planner.calibrator.sampleEvery
	if every <= 1 {
		return true
	}
	seq := qe.planner.calibrator.seq.Add(1)
	return seq%every == 0
}

func (qe *queryEngine) plannerCostMultiplier(plan plannerCalPlan) float64 {
	cur := qe.planner.calibrator.state.Load()
	if cur == nil {
		return 1.0
	}
	m := cur.Multipliers[plan]
	if m <= 0 || math.IsNaN(m) || math.IsInf(m, 0) {
		return 1.0
	}
	return m
}

func (qe *queryEngine) observeCalibration(ev TraceEvent) {
	if !qe.planner.calibrator.enabled {
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

	qe.planner.calibrator.Lock()
	defer qe.planner.calibrator.Unlock()

	cur := qe.planner.calibrator.state.Load()
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

	qe.planner.calibrator.state.Store(&next)
}
