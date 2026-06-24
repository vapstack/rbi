package qexec

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/snapshot"
)

const asyncMaterializedPredCancelProbeInterval = 2 * time.Millisecond

type AsyncMaterializedPredSnapshotOps struct {
	CurrentSeq func() uint64
	PinBySeq   func(uint64) (*snapshot.View, *snapshot.Ref, bool)
	Unpin      func(uint64, *snapshot.Ref)
}

type AsyncMaterializedPredStats struct {
	Scheduled        uint64
	DroppedInFlight  uint64
	DroppedCapacity  uint64
	Canceled         uint64
	SkippedCached    uint64
	BuildFailed      uint64
	Stored           uint64
	StoreRejected    uint64
	InFlight         int64
	MaxWorkerSlots   int
	WorkerSlotsInUse int
}

type asyncMaterializedPredPlanKind uint8

const (
	asyncMaterializedPredPlanNone asyncMaterializedPredPlanKind = iota
	asyncMaterializedPredPlanRange
	asyncMaterializedPredPlanRangeComplement
)

type asyncMaterializedPredPlan struct {
	seq          uint64
	key          qcache.MaterializedPredKey
	bounds       indexdata.Bounds
	fieldOrdinal int
	kind         asyncMaterializedPredPlanKind
}

type asyncMaterializedPredTaskKey struct {
	seq uint64
	key qcache.MaterializedPredKey
}

type asyncMaterializedPredScheduler struct {
	ops   AsyncMaterializedPredSnapshotOps
	slots chan struct{}

	mu       sync.Mutex
	inFlight map[asyncMaterializedPredTaskKey]struct{}

	scheduled       atomic.Uint64
	droppedInFlight atomic.Uint64
	droppedCapacity atomic.Uint64
	canceled        atomic.Uint64
	skippedCached   atomic.Uint64
	buildFailed     atomic.Uint64
	stored          atomic.Uint64
	storeRejected   atomic.Uint64
	inFlightCount   atomic.Int64
}

type asyncMaterializedPredCancel = indexdata.RangePostingsUnionCancel

type asyncMaterializedPredBuildStatus uint8

const (
	asyncMaterializedPredBuildOK asyncMaterializedPredBuildStatus = iota
	asyncMaterializedPredBuildCanceled
	asyncMaterializedPredBuildFailed
)

func newAsyncMaterializedPredScheduler(workers int) *asyncMaterializedPredScheduler {
	return &asyncMaterializedPredScheduler{
		slots:    make(chan struct{}, workers),
		inFlight: make(map[asyncMaterializedPredTaskKey]struct{}, workers),
	}
}

func (r *Runtime) ConfigureAsyncMaterializedPredSnapshotOps(ops AsyncMaterializedPredSnapshotOps) {
	r.asyncMaterializedPredWarm.ops = ops
}

func (r *Runtime) AsyncMaterializedPredStats() AsyncMaterializedPredStats {
	s := r.asyncMaterializedPredWarm
	return AsyncMaterializedPredStats{
		Scheduled:        s.scheduled.Load(),
		DroppedInFlight:  s.droppedInFlight.Load(),
		DroppedCapacity:  s.droppedCapacity.Load(),
		Canceled:         s.canceled.Load(),
		SkippedCached:    s.skippedCached.Load(),
		BuildFailed:      s.buildFailed.Load(),
		Stored:           s.stored.Load(),
		StoreRejected:    s.storeRejected.Load(),
		InFlight:         s.inFlightCount.Load(),
		MaxWorkerSlots:   cap(s.slots),
		WorkerSlotsInUse: len(s.slots),
	}
}

func (qv *View) scheduleAsyncMaterializedPredWarmup(plan asyncMaterializedPredPlan) bool {
	if qv.snap == nil || plan.key.IsZero() || plan.kind == asyncMaterializedPredPlanNone {
		return false
	}
	plan.seq = qv.snap.Seq
	return qv.exec.asyncMaterializedPredWarm.schedule(qv.exec, plan)
}

func (s *asyncMaterializedPredScheduler) schedule(exec *Runtime, plan asyncMaterializedPredPlan) bool {
	ops := s.ops
	if ops.CurrentSeq == nil || ops.PinBySeq == nil || ops.Unpin == nil {
		return false
	}
	if ops.CurrentSeq() != plan.seq {
		s.canceled.Add(1)
		return false
	}

	taskKey := asyncMaterializedPredTaskKey{seq: plan.seq, key: plan.key}
	s.mu.Lock()
	if _, ok := s.inFlight[taskKey]; ok {
		s.mu.Unlock()
		s.droppedInFlight.Add(1)
		return false
	}
	select {
	case s.slots <- struct{}{}:
	default:
		s.mu.Unlock()
		s.droppedCapacity.Add(1)
		return false
	}
	s.inFlight[taskKey] = struct{}{}
	s.inFlightCount.Add(1)
	s.mu.Unlock()

	snap, ref, ok := ops.PinBySeq(plan.seq)
	if !ok {
		s.releaseTaskSlot(taskKey)
		s.canceled.Add(1)
		return false
	}
	if ops.CurrentSeq() != plan.seq {
		ops.Unpin(plan.seq, ref)
		s.releaseTaskSlot(taskKey)
		s.canceled.Add(1)
		return false
	}

	s.scheduled.Add(1)
	go s.run(exec, plan, snap, ref)
	return true
}

func (s *asyncMaterializedPredScheduler) run(exec *Runtime, plan asyncMaterializedPredPlan, snap *snapshot.View, ref *snapshot.Ref) {
	ops := s.ops
	taskKey := asyncMaterializedPredTaskKey{seq: plan.seq, key: plan.key}
	defer s.finish(ops, plan.seq, ref, taskKey)

	if ops.CurrentSeq() != plan.seq {
		s.canceled.Add(1)
		return
	}
	if snap.HasMaterializedPredKey(plan.key) {
		s.skippedCached.Add(1)
		return
	}

	view := exec.AcquireView(snap)
	cancel := asyncMaterializedPredCancel{
		CurrentSeq:    ops.CurrentSeq,
		Seq:           plan.seq,
		NextProbe:     time.Now().Add(asyncMaterializedPredCancelProbeInterval).UnixNano(),
		ProbeInterval: int64(asyncMaterializedPredCancelProbeInterval),
	}
	ids, status := view.buildAsyncMaterializedPred(plan, &cancel)
	exec.ReleaseView(view)

	switch status {
	case asyncMaterializedPredBuildOK:
	case asyncMaterializedPredBuildCanceled:
		ids.Release()
		s.canceled.Add(1)
		return
	default:
		ids.Release()
		s.buildFailed.Add(1)
		return
	}

	if ops.CurrentSeq() != plan.seq {
		ids.Release()
		s.canceled.Add(1)
		return
	}
	if snap.HasMaterializedPredKey(plan.key) {
		ids.Release()
		s.skippedCached.Add(1)
		return
	}
	if storeAsyncMaterializedPred(snap, plan.key, ids) {
		s.stored.Add(1)
		return
	}
	s.storeRejected.Add(1)
}

func (s *asyncMaterializedPredScheduler) finish(ops AsyncMaterializedPredSnapshotOps, seq uint64, ref *snapshot.Ref, key asyncMaterializedPredTaskKey) {
	ops.Unpin(seq, ref)
	s.releaseTaskSlot(key)
}

func (s *asyncMaterializedPredScheduler) releaseTaskSlot(key asyncMaterializedPredTaskKey) {
	s.mu.Lock()
	delete(s.inFlight, key)
	s.mu.Unlock()
	<-s.slots
	s.inFlightCount.Add(-1)
}

func asyncMaterializedPredCheck(c *asyncMaterializedPredCancel) bool {
	now := time.Now().UnixNano()
	if now < c.NextProbe {
		return false
	}
	c.NextProbe = now + c.ProbeInterval
	return c.CurrentSeq() != c.Seq
}

func storeAsyncMaterializedPred(snap *snapshot.View, key qcache.MaterializedPredKey, ids posting.List) bool {
	if ids.IsEmpty() {
		snap.StoreMaterializedPredKey(key, posting.List{})
		return snap.HasMaterializedPredKey(key)
	}
	if snap.AllowsMaterializedPredCard(ids.Cardinality()) {
		snap.StoreMaterializedPredKey(key, ids)
		return snap.HasMaterializedPredKey(key)
	}
	if snap.TryStoreMaterializedPredOversizedKey(key, ids) {
		return true
	}
	ids.Release()
	return false
}

func asyncMaterializedPredObservedThreshold(buildWork uint64) uint64 {
	return buildWork
}

func (qv *View) asyncMaterializedPredPlanForOrderedORPredicate(p predicate) (asyncMaterializedPredPlan, bool) {
	candidate, ok := qv.preparePredicateScalarRangeRoutingCandidate(p)
	if !ok {
		return asyncMaterializedPredPlan{}, false
	}

	if candidate.plan.useComplement {
		plan, ok := candidate.core.prepareComplementMaterialization()
		if !ok {
			return asyncMaterializedPredPlan{}, false
		}
		if !candidate.shouldPreferPositiveMaterializationForNullableComplement(plan) {
			return qv.asyncMaterializedPredPlanForPredicate(p, true)
		}
	}
	return qv.asyncMaterializedPredPlanForPredicate(p, false)
}

func (qv *View) asyncMaterializedPredPlanForOrderBasicBaseCore(core orderBasicBaseCore) (asyncMaterializedPredPlan, bool) {
	switch core.kind {
	case orderBasicBaseCoreCollapsedRange:
		if core.collapsed.cacheKey.IsZero() {
			return asyncMaterializedPredPlan{}, false
		}
		return asyncMaterializedPredPlan{
			key:          core.collapsed.cacheKey,
			bounds:       core.collapsed.bounds,
			fieldOrdinal: core.collapsed.fieldOrdinal,
			kind:         asyncMaterializedPredPlanRange,
		}, true

	case orderBasicBaseCoreRawExpr:
		stats, ok := qv.orderBasicRawBaseOpStats(core.expr, qv.snap.Universe.Cardinality())
		if !ok || stats.cacheKey.IsZero() {
			return asyncMaterializedPredPlan{}, false
		}
		bound, isSlice, err := qv.normalizedScalarBoundForExpr(core.expr)
		if err != nil || isSlice || bound.empty || bound.full {
			return asyncMaterializedPredPlan{}, false
		}
		kind := asyncMaterializedPredPlanRange
		if stats.buildComplement {
			kind = asyncMaterializedPredPlanRangeComplement
		}
		return asyncMaterializedPredPlan{
			key:          stats.cacheKey,
			bounds:       rangeBoundsForNormalizedScalarBound(bound),
			fieldOrdinal: core.expr.FieldOrdinal,
			kind:         kind,
		}, true
	}
	return asyncMaterializedPredPlan{}, false
}

func (qv *View) asyncMaterializedPredPlanForPredicate(p predicate, complement bool) (asyncMaterializedPredPlan, bool) {
	if p.hasEffectiveBounds {
		fieldName := qv.exec.FieldNameByOrdinal(p.expr.FieldOrdinal)
		key := qv.materializedPredKeyForExactScalarRange(fieldName, p.effectiveBounds)
		kind := asyncMaterializedPredPlanRange
		if complement {
			key = qv.materializedPredComplementKeyForExactScalarRange(fieldName, p.effectiveBounds)
			kind = asyncMaterializedPredPlanRangeComplement
		}
		if key.IsZero() {
			return asyncMaterializedPredPlan{}, false
		}
		return asyncMaterializedPredPlan{
			key:          key,
			bounds:       p.effectiveBounds,
			fieldOrdinal: p.expr.FieldOrdinal,
			kind:         kind,
		}, true
	}

	bound, isSlice, err := qv.normalizedScalarBoundForExpr(p.expr)
	if err != nil || isSlice || bound.empty || bound.full {
		return asyncMaterializedPredPlan{}, false
	}
	fieldName := qv.exec.FieldNameByOrdinal(p.expr.FieldOrdinal)
	key := qv.materializedPredKeyForNormalizedScalarBound(fieldName, bound)
	kind := asyncMaterializedPredPlanRange
	if complement {
		key = qv.materializedPredComplementKeyForNormalizedScalarBound(fieldName, bound)
		kind = asyncMaterializedPredPlanRangeComplement
	}
	if key.IsZero() {
		return asyncMaterializedPredPlan{}, false
	}
	return asyncMaterializedPredPlan{
		key:          key,
		bounds:       rangeBoundsForNormalizedScalarBound(bound),
		fieldOrdinal: p.expr.FieldOrdinal,
		kind:         kind,
	}, true
}

func (qv *View) buildAsyncMaterializedPred(plan asyncMaterializedPredPlan, cancel *asyncMaterializedPredCancel) (posting.List, asyncMaterializedPredBuildStatus) {
	switch plan.kind {
	case asyncMaterializedPredPlanRange:
		return qv.buildAsyncRangeMaterializedPred(plan.fieldOrdinal, plan.bounds, cancel)
	case asyncMaterializedPredPlanRangeComplement:
		return qv.buildAsyncRangeComplementMaterializedPred(plan.fieldOrdinal, plan.bounds, cancel)
	default:
		return posting.List{}, asyncMaterializedPredBuildFailed
	}
}

func (qv *View) buildAsyncRangeMaterializedPred(fieldOrdinal int, bounds indexdata.Bounds, cancel *asyncMaterializedPredCancel) (posting.List, asyncMaterializedPredBuildStatus) {
	ov := qv.indexViewByOrdinal(fieldOrdinal)
	if !ov.HasData() {
		return posting.List{}, asyncMaterializedPredBuildOK
	}
	br := ov.RangeForBounds(bounds)
	if br.Empty() {
		return posting.List{}, asyncMaterializedPredBuildOK
	}
	if asyncMaterializedPredCheck(cancel) {
		return posting.List{}, asyncMaterializedPredBuildCanceled
	}
	fieldName := qv.exec.FieldNameByOrdinal(fieldOrdinal)
	if out, ok := qv.tryEvalNumericRangeBucketsUntil(fieldName, fieldOrdinal, qv.fieldMetaByOrdinal(fieldOrdinal), ov, br, cancel); ok {
		if asyncMaterializedPredCheck(cancel) {
			out.ids.Release()
			return posting.List{}, asyncMaterializedPredBuildCanceled
		}
		return ownedAsyncMaterializedPredPosting(out.ids), asyncMaterializedPredBuildOK
	}
	if cancel.Canceled() {
		return posting.List{}, asyncMaterializedPredBuildCanceled
	}
	ids, ok := ov.UnionRangePostingsUntil(br, indexdata.FieldIndexRange{}, cancel)
	if !ok {
		return posting.List{}, asyncMaterializedPredBuildCanceled
	}
	return ownedAsyncMaterializedPredPosting(ids), asyncMaterializedPredBuildOK
}

func (qv *View) buildAsyncRangeComplementMaterializedPred(fieldOrdinal int, bounds indexdata.Bounds, cancel *asyncMaterializedPredCancel) (posting.List, asyncMaterializedPredBuildStatus) {
	ov := qv.indexViewByOrdinal(fieldOrdinal)
	if !ov.HasData() {
		return posting.List{}, asyncMaterializedPredBuildFailed
	}
	br := ov.RangeForBounds(bounds)
	before, after, ok := fieldIndexComplementRangeSpans(ov, br)
	if !ok {
		return posting.List{}, asyncMaterializedPredBuildFailed
	}
	nilPosting := qv.nilIndexViewByOrdinal(fieldOrdinal).LookupPostingRetained(indexdata.NilIndexEntryKey)
	if before.Empty() && after.Empty() {
		if nilPosting.IsEmpty() {
			return posting.List{}, asyncMaterializedPredBuildOK
		}
		if cancel.Canceled() {
			return posting.List{}, asyncMaterializedPredBuildCanceled
		}
		return ownedAsyncMaterializedPredPosting(nilPosting), asyncMaterializedPredBuildOK
	}
	if asyncMaterializedPredCheck(cancel) {
		return posting.List{}, asyncMaterializedPredBuildCanceled
	}
	fieldName := qv.exec.FieldNameByOrdinal(fieldOrdinal)
	fm := qv.fieldMetaByOrdinal(fieldOrdinal)

	var (
		ids           posting.List
		pendingBefore indexdata.FieldIndexRange
		pendingAfter  indexdata.FieldIndexRange
	)
	if !before.Empty() {
		if out, ok := qv.tryEvalNumericRangeBucketsUntil(fieldName, fieldOrdinal, fm, ov, before, cancel); ok {
			ids = out.ids
		} else {
			if cancel.Canceled() {
				return posting.List{}, asyncMaterializedPredBuildCanceled
			}
			pendingBefore = before
		}
		if asyncMaterializedPredCheck(cancel) {
			ids.Release()
			return posting.List{}, asyncMaterializedPredBuildCanceled
		}
	}
	if !after.Empty() {
		if out, ok := qv.tryEvalNumericRangeBucketsUntil(fieldName, fieldOrdinal, fm, ov, after, cancel); ok {
			if ids.IsEmpty() {
				ids = out.ids
			} else {
				ids = ids.BuildMergedOwned(out.ids)
			}
		} else {
			if cancel.Canceled() {
				ids.Release()
				return posting.List{}, asyncMaterializedPredBuildCanceled
			}
			pendingAfter = after
		}
		if asyncMaterializedPredCheck(cancel) {
			ids.Release()
			return posting.List{}, asyncMaterializedPredBuildCanceled
		}
	}
	if !pendingBefore.Empty() || !pendingAfter.Empty() {
		pending, ok := ov.UnionRangePostingsUntil(pendingBefore, pendingAfter, cancel)
		if !ok {
			ids.Release()
			return posting.List{}, asyncMaterializedPredBuildCanceled
		}
		if ids.IsEmpty() {
			ids = pending
		} else {
			ids = ids.BuildMergedOwned(pending)
		}
	}
	if !nilPosting.IsEmpty() {
		if cancel.Canceled() {
			ids.Release()
			return posting.List{}, asyncMaterializedPredBuildCanceled
		}
		if ids.IsEmpty() {
			ids = nilPosting
		} else {
			ids = ids.BuildOr(nilPosting)
		}
	}
	if asyncMaterializedPredCheck(cancel) {
		ids.Release()
		return posting.List{}, asyncMaterializedPredBuildCanceled
	}
	return ownedAsyncMaterializedPredPosting(ids), asyncMaterializedPredBuildOK
}

func ownedAsyncMaterializedPredPosting(ids posting.List) posting.List {
	if !ids.IsBorrowed() {
		return ids
	}
	owned := ids.Clone()
	ids.Release()
	return owned
}
