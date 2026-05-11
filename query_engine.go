package rbi

import (
	"github.com/vapstack/rbi/internal/indexdata"
	"math"
	"time"

	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/strmap"
)

type queryEngine struct {
	snapshot                       *snapshotManager
	schema                         *schema.Runtime
	index                          []indexdata.FieldStorage
	nilIndex                       []indexdata.FieldStorage
	lenIndex                       []indexdata.FieldStorage
	lenZeroComplement              []bool
	measure                        []indexdata.MeasureStorage
	universe                       posting.List
	lenIndexLoaded                 bool
	matPredCacheMaxEntries         int
	matPredCacheMaxCard            uint64
	numericRangeBucketSize         int
	numericRangeBucketMinFieldKeys int
	numericRangeBucketMinSpanKeys  int

	planner *planner

	viewPool *pooled.Pointers[queryView]
}

func (qe *queryEngine) makeQueryView(snap *indexSnapshot) *queryView {
	view := qe.viewPool.Get()
	*view = queryView{
		engine:            qe,
		snap:              snap,
		strKey:            snap.strmap != nil,
		strMapView:        snap.strmap,
		planner:           qe.planner,
		lenZeroComplement: snap.lenZeroComplement,
	}
	return view
}

func (qe *queryEngine) releaseQueryView(view *queryView) {
	qe.viewPool.Put(view)
}

func (qe *queryEngine) initSnapshotRuntimeCaches(s *indexSnapshot) {
	s.numericRangeBucketCache = numericRangeBucketCachePool.Get()
	s.numericRangeBucketCache.init(len(qe.schema.Indexed))
	s.matPredCacheMaxEntries = qe.matPredCacheMaxEntries
	s.matPredCacheMaxCard = qe.matPredCacheMaxCard
	if s.matPredCacheMaxEntries > 0 {
		s.matPredCache = materializedPredCachePool.Get()
		s.matPredCache.refs.Store(1)
		s.matPredCache.init(s.matPredCacheMaxEntries)
	}
}

func (qe *queryEngine) buildPublishedSnapshotNoLock(seq uint64, sm *strmap.Snapshot) *indexSnapshot {
	snap := &indexSnapshot{
		seq:                seq,
		index:              qe.index,
		nilIndex:           qe.nilIndex,
		lenIndex:           qe.lenIndex,
		lenZeroComplement:  qe.lenZeroComplement,
		measure:            qe.measure,
		indexedFieldByName: qe.schema.IndexedByName,
		universe:           qe.universe,
		strmap:             sm,
	}
	qe.initSnapshotRuntimeCaches(snap)
	return snap
}

func (qe *queryEngine) publishSnapshotNoLock(seq uint64, sm *strmap.Snapshot) {
	prev := qe.snapshot.current.Load()
	snap := qe.buildPublishedSnapshotNoLock(seq, sm)
	if prev != nil {
		snap.index = indexdata.CloneFieldStorageSlots(qe.index, len(qe.schema.Indexed))
		snap.nilIndex = indexdata.CloneFieldStorageSlots(qe.nilIndex, len(qe.schema.Indexed))
		snap.lenIndex = indexdata.CloneFieldStorageSlots(qe.lenIndex, len(qe.schema.Indexed))
		snap.lenZeroComplement = cloneFieldIndexBoolSlots(qe.lenZeroComplement, len(qe.schema.Indexed))
		snap.measure = indexdata.CloneMeasureStorageSlots(qe.measure, len(qe.schema.Measures))
	}
	snap.retainSharedOwnedStorageFrom(prev)
	qe.finishSnapshotPublishNoLock(snap)
}

func (qe *queryEngine) finishSnapshotPublishNoLock(s *indexSnapshot) {
	qe.index = s.index
	qe.nilIndex = s.nilIndex
	qe.lenIndex = s.lenIndex
	qe.lenZeroComplement = s.lenZeroComplement
	qe.measure = s.measure
	qe.universe = s.universe
	retired := qe.snapshot.publishRef(s)
	releaseRetiredSnapshots(retired)
}

func (qe *queryEngine) getSnapshot() *indexSnapshot {
	if s := qe.snapshot.current.Load(); s != nil {
		return s
	}
	return qe.buildPublishedSnapshotNoLock(0, nil)
}

func (qe *queryEngine) pinCurrentSnapshot() (*indexSnapshot, uint64, *snapshotRef, bool) {
	for {
		qe.snapshot.mu.RLock()
		ref := qe.snapshot.currentRef.Load()
		if ref == nil {
			qe.snapshot.mu.RUnlock()
			return qe.buildPublishedSnapshotNoLock(0, nil), 0, nil, false
		}
		ref.refs.Add(1)
		if qe.snapshot.currentRef.Load() != ref {
			qe.snapshot.mu.RUnlock()
			ref.refs.Add(-1)
			continue
		}
		snap := ref.snap
		qe.snapshot.mu.RUnlock()
		if snap == nil {
			ref.refs.Add(-1)
			continue
		}
		return snap, snap.seq, ref, true
	}
}

func (qe *queryEngine) unpinCurrentSnapshot(seq uint64, ref *snapshotRef, pinned bool) {
	if !pinned {
		return
	}
	qe.snapshot.unpinRef(seq, ref)
}

func (qe *queryEngine) fieldNameByOrdinal(ordinal int) string {
	if ordinal < 0 || ordinal >= len(qe.schema.Indexed) {
		return ""
	}
	return qe.schema.Indexed[ordinal].Name
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
