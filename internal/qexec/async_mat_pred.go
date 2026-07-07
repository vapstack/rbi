package qexec

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
)

const asyncMaterializedPredCancelProbeInterval = 2 * time.Millisecond

type AsyncMaterializedPredSnapshotOps struct {
	CurrentSeq      func() uint64
	PinCurrentBySeq func(uint64) (*snapshot.View, AsyncSnapshotPin, bool)
}

type AsyncSnapshotPin interface {
	Unpin()
}

type AsyncMaterializedPredStats struct {
	Scheduled                 uint64
	DroppedInFlight           uint64
	DroppedCapacity           uint64
	Canceled                  uint64
	SkippedCached             uint64
	BuildFailed               uint64
	Stored                    uint64
	StoreRejected             uint64
	NumericSpanScheduled      uint64
	NumericSpanCanceled       uint64
	NumericSpanSkippedCached  uint64
	NumericSpanBuildFailed    uint64
	NumericSpanStored         uint64
	NumericSpanStoreRejected  uint64
	NumericExactScheduled     uint64
	NumericExactCanceled      uint64
	NumericExactSkippedCached uint64
	NumericExactBuildFailed   uint64
	NumericExactStored        uint64
	NumericExactStoreRejected uint64
	InFlight                  int64
	MaxWorkerSlots            int
	WorkerSlotsInUse          int
}

type asyncMaterializedPredPlanKind uint8

const (
	asyncMaterializedPredPlanNone asyncMaterializedPredPlanKind = iota
	asyncMaterializedPredPlanRange
	asyncMaterializedPredPlanRangeComplement
	asyncMaterializedPredPlanNumericSpan
	asyncMaterializedPredPlanNumericExact
)

func (kind asyncMaterializedPredPlanKind) numericRange() bool {
	return kind == asyncMaterializedPredPlanNumericSpan || kind == asyncMaterializedPredPlanNumericExact
}

type asyncMaterializedPredPlan struct {
	seq          uint64
	key          qcache.MaterializedPredKey
	bounds       indexdata.Bounds
	fieldOrdinal int
	fieldSlot    int
	startBucket  int
	endBucket    int
	startRank    int
	endRank      int
	kind         asyncMaterializedPredPlanKind
}

type asyncMaterializedPredTaskKey struct {
	seq         uint64
	key         qcache.MaterializedPredKey
	kind        asyncMaterializedPredPlanKind
	fieldSlot   int
	startBucket int
	endBucket   int
	startRank   int
	endRank     int
}

type asyncMaterializedPredScheduler struct {
	ops          AsyncMaterializedPredSnapshotOps
	slots        chan struct{}
	statsEnabled bool

	mu       sync.Mutex
	inFlight map[asyncMaterializedPredTaskKey]struct{}

	scheduled                 atomic.Uint64
	droppedInFlight           atomic.Uint64
	droppedCapacity           atomic.Uint64
	canceled                  atomic.Uint64
	skippedCached             atomic.Uint64
	buildFailed               atomic.Uint64
	stored                    atomic.Uint64
	storeRejected             atomic.Uint64
	numericSpanScheduled      atomic.Uint64
	numericSpanCanceled       atomic.Uint64
	numericSpanSkippedCached  atomic.Uint64
	numericSpanBuildFailed    atomic.Uint64
	numericSpanStored         atomic.Uint64
	numericSpanStoreRejected  atomic.Uint64
	numericExactScheduled     atomic.Uint64
	numericExactCanceled      atomic.Uint64
	numericExactSkippedCached atomic.Uint64
	numericExactBuildFailed   atomic.Uint64
	numericExactStored        atomic.Uint64
	numericExactStoreRejected atomic.Uint64
	inFlightCount             atomic.Int64
}

type asyncMaterializedPredCancel = indexdata.RangePostingsUnionCancel

type asyncMaterializedPredBuildStatus uint8

const (
	asyncMaterializedPredBuildOK asyncMaterializedPredBuildStatus = iota
	asyncMaterializedPredBuildCanceled
	asyncMaterializedPredBuildFailed
	asyncMaterializedPredBuildStoreRejected
	asyncMaterializedPredBuildSkippedCached
)

func newAsyncMaterializedPredScheduler(workers int, statsEnabled bool) *asyncMaterializedPredScheduler {
	return &asyncMaterializedPredScheduler{
		slots:        make(chan struct{}, workers),
		statsEnabled: statsEnabled,
		inFlight:     make(map[asyncMaterializedPredTaskKey]struct{}, workers),
	}
}

func (r *Runtime) ConfigureAsyncMaterializedPredSnapshotOps(ops AsyncMaterializedPredSnapshotOps) {
	r.asyncMaterializedPredWarm.ops = ops
}

func (r *Runtime) AsyncMaterializedPredStats() AsyncMaterializedPredStats {
	s := r.asyncMaterializedPredWarm
	if !s.statsEnabled {
		return AsyncMaterializedPredStats{}
	}
	return AsyncMaterializedPredStats{
		Scheduled:                 s.scheduled.Load(),
		DroppedInFlight:           s.droppedInFlight.Load(),
		DroppedCapacity:           s.droppedCapacity.Load(),
		Canceled:                  s.canceled.Load(),
		SkippedCached:             s.skippedCached.Load(),
		BuildFailed:               s.buildFailed.Load(),
		Stored:                    s.stored.Load(),
		StoreRejected:             s.storeRejected.Load(),
		NumericSpanScheduled:      s.numericSpanScheduled.Load(),
		NumericSpanCanceled:       s.numericSpanCanceled.Load(),
		NumericSpanSkippedCached:  s.numericSpanSkippedCached.Load(),
		NumericSpanBuildFailed:    s.numericSpanBuildFailed.Load(),
		NumericSpanStored:         s.numericSpanStored.Load(),
		NumericSpanStoreRejected:  s.numericSpanStoreRejected.Load(),
		NumericExactScheduled:     s.numericExactScheduled.Load(),
		NumericExactCanceled:      s.numericExactCanceled.Load(),
		NumericExactSkippedCached: s.numericExactSkippedCached.Load(),
		NumericExactBuildFailed:   s.numericExactBuildFailed.Load(),
		NumericExactStored:        s.numericExactStored.Load(),
		NumericExactStoreRejected: s.numericExactStoreRejected.Load(),
		InFlight:                  s.inFlightCount.Load(),
		MaxWorkerSlots:            cap(s.slots),
		WorkerSlotsInUse:          len(s.slots),
	}
}

func (qv *View) scheduleAsyncMaterializedPredWarmup(plan asyncMaterializedPredPlan) bool {
	if qv.snap == nil || plan.kind == asyncMaterializedPredPlanNone {
		return false
	}
	if !plan.kind.numericRange() && plan.key.IsZero() {
		return false
	}
	plan.seq = qv.snap.Seq
	return qv.exec.asyncMaterializedPredWarm.schedule(qv.exec, plan)
}

func (s *asyncMaterializedPredScheduler) schedule(exec *Runtime, plan asyncMaterializedPredPlan) bool {
	ops := s.ops
	if ops.CurrentSeq == nil || ops.PinCurrentBySeq == nil {
		return false
	}
	stats := s.statsEnabled
	if ops.CurrentSeq() != plan.seq {
		if stats {
			s.addCanceled(plan)
		}
		return false
	}

	taskKey := plan.taskKey()
	s.mu.Lock()
	if _, ok := s.inFlight[taskKey]; ok {
		s.mu.Unlock()
		if stats {
			s.droppedInFlight.Add(1)
		}
		return false
	}
	select {
	case s.slots <- struct{}{}:
	default:
		s.mu.Unlock()
		if stats {
			s.droppedCapacity.Add(1)
		}
		return false
	}
	s.inFlight[taskKey] = struct{}{}
	if stats {
		s.inFlightCount.Add(1)
	}
	s.mu.Unlock()

	snap, pin, ok := ops.PinCurrentBySeq(plan.seq)
	if !ok {
		s.releaseTaskSlot(taskKey)
		if stats {
			s.addCanceled(plan)
		}
		return false
	}
	if ops.CurrentSeq() != plan.seq {
		pin.Unpin()
		s.releaseTaskSlot(taskKey)
		if stats {
			s.addCanceled(plan)
		}
		return false
	}

	if stats {
		switch plan.kind {
		case asyncMaterializedPredPlanNumericSpan:
			s.numericSpanScheduled.Add(1)
		case asyncMaterializedPredPlanNumericExact:
			s.numericExactScheduled.Add(1)
		default:
			s.scheduled.Add(1)
		}
	}
	go s.run(exec, plan, snap, pin)
	return true
}

func (s *asyncMaterializedPredScheduler) run(exec *Runtime, plan asyncMaterializedPredPlan, snap *snapshot.View, pin AsyncSnapshotPin) {
	ops := s.ops
	stats := s.statsEnabled
	taskKey := plan.taskKey()
	defer s.finish(pin, taskKey)

	if ops.CurrentSeq() != plan.seq {
		if stats {
			s.addCanceled(plan)
		}
		return
	}
	if !plan.kind.numericRange() && snap.HasMaterializedPredKey(plan.key) {
		if stats {
			s.skippedCached.Add(1)
		}
		return
	}

	view := exec.AcquireView(snap)
	cancel := asyncMaterializedPredCancel{
		CurrentSeq:    ops.CurrentSeq,
		Seq:           plan.seq,
		NextProbe:     time.Now().Add(asyncMaterializedPredCancelProbeInterval).UnixNano(),
		ProbeInterval: int64(asyncMaterializedPredCancelProbeInterval),
	}
	if plan.kind.numericRange() {
		status := asyncMaterializedPredBuildFailed
		if plan.kind == asyncMaterializedPredPlanNumericSpan {
			status = view.buildAndStoreAsyncNumericRangeSpan(plan, &cancel)
		} else {
			status = view.buildAndStoreAsyncNumericRangeExact(plan, &cancel)
		}
		exec.ReleaseView(view)
		switch status {
		case asyncMaterializedPredBuildOK:
			if stats {
				if plan.kind == asyncMaterializedPredPlanNumericSpan {
					s.numericSpanStored.Add(1)
				} else {
					s.numericExactStored.Add(1)
				}
			}
		case asyncMaterializedPredBuildCanceled:
			if stats {
				if plan.kind == asyncMaterializedPredPlanNumericSpan {
					s.numericSpanCanceled.Add(1)
				} else {
					s.numericExactCanceled.Add(1)
				}
			}
		case asyncMaterializedPredBuildFailed:
			if stats {
				if plan.kind == asyncMaterializedPredPlanNumericSpan {
					s.numericSpanBuildFailed.Add(1)
				} else {
					s.numericExactBuildFailed.Add(1)
				}
			}
		case asyncMaterializedPredBuildSkippedCached:
			if stats {
				if plan.kind == asyncMaterializedPredPlanNumericSpan {
					s.numericSpanSkippedCached.Add(1)
				} else {
					s.numericExactSkippedCached.Add(1)
				}
			}
		default:
			if stats {
				if plan.kind == asyncMaterializedPredPlanNumericSpan {
					s.numericSpanStoreRejected.Add(1)
				} else {
					s.numericExactStoreRejected.Add(1)
				}
			}
		}
		return
	}

	ids, status := view.buildAsyncMaterializedPred(plan, &cancel)
	exec.ReleaseView(view)

	switch status {
	case asyncMaterializedPredBuildOK:
	case asyncMaterializedPredBuildCanceled:
		ids.Release()
		if stats {
			s.canceled.Add(1)
		}
		return
	default:
		ids.Release()
		if stats {
			s.buildFailed.Add(1)
		}
		return
	}

	if ops.CurrentSeq() != plan.seq {
		ids.Release()
		if stats {
			s.canceled.Add(1)
		}
		return
	}
	if snap.HasMaterializedPredKey(plan.key) {
		ids.Release()
		if stats {
			s.skippedCached.Add(1)
		}
		return
	}
	if storeAsyncMaterializedPred(snap, plan.key, ids) {
		if stats {
			s.stored.Add(1)
		}
		return
	}
	if stats {
		s.storeRejected.Add(1)
	}
}

func (plan asyncMaterializedPredPlan) taskKey() asyncMaterializedPredTaskKey {
	if plan.kind == asyncMaterializedPredPlanNumericSpan {
		return asyncMaterializedPredTaskKey{
			seq:         plan.seq,
			kind:        plan.kind,
			fieldSlot:   plan.fieldSlot,
			startBucket: plan.startBucket,
			endBucket:   plan.endBucket,
		}
	}
	if plan.kind == asyncMaterializedPredPlanNumericExact {
		return asyncMaterializedPredTaskKey{
			seq:       plan.seq,
			kind:      plan.kind,
			fieldSlot: plan.fieldSlot,
			startRank: plan.startRank,
			endRank:   plan.endRank,
		}
	}
	return asyncMaterializedPredTaskKey{seq: plan.seq, key: plan.key, kind: plan.kind}
}

func (s *asyncMaterializedPredScheduler) addCanceled(plan asyncMaterializedPredPlan) {
	if plan.kind == asyncMaterializedPredPlanNumericSpan {
		s.numericSpanCanceled.Add(1)
		return
	}
	if plan.kind == asyncMaterializedPredPlanNumericExact {
		s.numericExactCanceled.Add(1)
		return
	}
	s.canceled.Add(1)
}

func (s *asyncMaterializedPredScheduler) finish(pin AsyncSnapshotPin, key asyncMaterializedPredTaskKey) {
	pin.Unpin()
	s.releaseTaskSlot(key)
}

func (s *asyncMaterializedPredScheduler) releaseTaskSlot(key asyncMaterializedPredTaskKey) {
	s.mu.Lock()
	delete(s.inFlight, key)
	s.mu.Unlock()
	<-s.slots
	if s.statsEnabled {
		s.inFlightCount.Add(-1)
	}
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
		if plan, ok, handled := qv.asyncNumericRangePlanForBounds(core.collapsed.fieldOrdinal, core.collapsed.bounds); ok {
			return plan, true
		} else if handled {
			return asyncMaterializedPredPlan{}, false
		}
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
		bounds := rangeBoundsForNormalizedScalarBound(bound)
		if kind == asyncMaterializedPredPlanRange {
			if plan, ok, handled := qv.asyncNumericRangePlanForBounds(core.expr.FieldOrdinal, bounds); ok {
				return plan, true
			} else if handled {
				return asyncMaterializedPredPlan{}, false
			}
		}
		return asyncMaterializedPredPlan{
			key:          stats.cacheKey,
			bounds:       bounds,
			fieldOrdinal: core.expr.FieldOrdinal,
			kind:         kind,
		}, true
	}
	return asyncMaterializedPredPlan{}, false
}

func (qv *View) asyncMaterializedPredPlanForPredicate(p predicate, complement bool) (asyncMaterializedPredPlan, bool) {
	if p.hasEffectiveBounds {
		if !complement {
			if plan, ok, handled := qv.asyncNumericRangePlanForBounds(p.expr.FieldOrdinal, p.effectiveBounds); ok {
				return plan, true
			} else if handled {
				return asyncMaterializedPredPlan{}, false
			}
		}
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
	bounds := rangeBoundsForNormalizedScalarBound(bound)
	if !complement {
		if plan, ok, handled := qv.asyncNumericRangePlanForBounds(p.expr.FieldOrdinal, bounds); ok {
			return plan, true
		} else if handled {
			return asyncMaterializedPredPlan{}, false
		}
	}
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
		bounds:       bounds,
		fieldOrdinal: p.expr.FieldOrdinal,
		kind:         kind,
	}, true
}

func (qv *View) asyncNumericRangePlanForBounds(fieldOrdinal int, bounds indexdata.Bounds) (asyncMaterializedPredPlan, bool, bool) {
	fm := qv.fieldMetaByOrdinal(fieldOrdinal)
	if !schema.FieldUsesOrderedNumericKeys(fm) {
		return asyncMaterializedPredPlan{}, false, false
	}

	ov := qv.indexViewByOrdinal(fieldOrdinal)
	if !ov.HasData() {
		return asyncMaterializedPredPlan{}, false, false
	}

	br := ov.RangeForBounds(bounds)
	if br.Empty() {
		return asyncMaterializedPredPlan{}, false, false
	}

	field := qv.exec.FieldNameByOrdinal(fieldOrdinal)
	span, ok := qv.numericRangeBucketSpan(field, fieldOrdinal, fm, ov, br)
	if !ok {
		return asyncMaterializedPredPlan{}, false, false
	}

	leftEnd := min(br.BaseEnd, span.index.BucketStart(span.start))
	rightStart := max(br.BaseStart, span.index.BucketEnd(span.end))

	if leftEnd > br.BaseStart || rightStart < br.BaseEnd {
		if qcache.NumericRangeExactCacheMaxEntries(qv.exec.NumericRangeExactCacheMaxEntries) > 0 &&
			qcache.NumericRangeExactCacheMaxEntryBytes(qv.exec.NumericRangeExactCacheMaxEntryBytes) != 0 {
			if cached, ok := qv.snap.LoadNumericRangeExactResult(span.fieldSlot, br.BaseStart, br.BaseEnd); ok {
				cached.Release()
				return asyncMaterializedPredPlan{}, false, true
			}
			if !qv.snap.NumericRangeExactResultRejectedTooLarge(span.fieldSlot, br.BaseStart, br.BaseEnd) {
				return asyncMaterializedPredPlan{
					bounds:       bounds,
					fieldOrdinal: fieldOrdinal,
					fieldSlot:    span.fieldSlot,
					startRank:    br.BaseStart,
					endRank:      br.BaseEnd,
					kind:         asyncMaterializedPredPlanNumericExact,
				}, true, true
			}
		}
	}

	return qv.asyncNumericSpanPlanForBounds(fieldOrdinal, bounds)
}

func (qv *View) asyncNumericSpanPlanForBounds(fieldOrdinal int, bounds indexdata.Bounds) (asyncMaterializedPredPlan, bool, bool) {
	fm := qv.fieldMetaByOrdinal(fieldOrdinal)
	if !schema.FieldUsesOrderedNumericKeys(fm) {
		return asyncMaterializedPredPlan{}, false, false
	}

	bucketSize := qv.exec.NumericRangeBucketSize
	minFieldKeys := qv.exec.NumericRangeBucketMinFieldKeys
	minSpan := qv.exec.NumericRangeBucketMinSpanKeys
	if bucketSize <= 0 || minFieldKeys <= 0 || minSpan <= 0 {
		return asyncMaterializedPredPlan{}, false, false
	}

	ov := qv.indexViewByOrdinal(fieldOrdinal)
	if !ov.HasData() {
		return asyncMaterializedPredPlan{}, false, false
	}

	br := ov.RangeForBounds(bounds)
	if br.Empty() || br.BaseEnd-br.BaseStart < minSpan {
		return asyncMaterializedPredPlan{}, false, false
	}

	desc := qv.exec.fields[fieldOrdinal]
	storage := qv.snap.Index[desc.storageOrdinal]
	entry := qv.snap.NumericRangeBucketCacheEntry(qv.exec.FieldNameByOrdinal(fieldOrdinal), desc.storageOrdinal, storage, bucketSize, minFieldKeys)
	if entry == nil {
		return asyncMaterializedPredPlan{}, false, false
	}

	idx := entry.Index()
	if idx.BucketCount() == 0 || idx.KeyCount() != ov.KeyCount() {
		return asyncMaterializedPredPlan{}, false, false
	}

	startFull, endFull, ok := idx.FullBucketSpan(br)
	if !ok {
		return asyncMaterializedPredPlan{}, false, false
	}

	if qcache.NumericRangeSpanCacheMaxEntries(qv.exec.NumericRangeSpanCacheMaxEntries) <= 0 ||
		qcache.NumericRangeSpanCacheMaxEntryBytes(qv.exec.NumericRangeSpanCacheMaxEntryBytes) == 0 {
		return asyncMaterializedPredPlan{}, false, true
	}

	if cached, ok := qv.snap.LoadNumericRangeFullSpan(entry, desc.storageOrdinal, startFull, endFull); ok {
		cached.Release()
		return asyncMaterializedPredPlan{}, false, true
	}

	if qv.snap.NumericRangeFullSpanRejectedTooLarge(desc.storageOrdinal, startFull, endFull) {
		return asyncMaterializedPredPlan{}, false, true
	}

	return asyncMaterializedPredPlan{
		bounds:       bounds,
		fieldOrdinal: fieldOrdinal,
		fieldSlot:    desc.storageOrdinal,
		startBucket:  startFull,
		endBucket:    endFull,
		kind:         asyncMaterializedPredPlanNumericSpan,
	}, true, true
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

func (qv *View) buildAndStoreAsyncNumericRangeSpan(plan asyncMaterializedPredPlan, cancel *asyncMaterializedPredCancel) asyncMaterializedPredBuildStatus {
	ov := qv.indexViewByOrdinal(plan.fieldOrdinal)
	if !ov.HasData() {
		return asyncMaterializedPredBuildFailed
	}

	fm := qv.fieldMetaByOrdinal(plan.fieldOrdinal)
	if !schema.FieldUsesOrderedNumericKeys(fm) {
		return asyncMaterializedPredBuildFailed
	}

	desc := qv.exec.fields[plan.fieldOrdinal]
	storage := qv.snap.Index[desc.storageOrdinal]
	entry := qv.snap.NumericRangeBucketCacheEntry(qv.exec.FieldNameByOrdinal(plan.fieldOrdinal), desc.storageOrdinal, storage, qv.exec.NumericRangeBucketSize, qv.exec.NumericRangeBucketMinFieldKeys)
	if entry == nil || desc.storageOrdinal != plan.fieldSlot {
		return asyncMaterializedPredBuildFailed
	}

	idx := entry.Index()
	if idx.BucketCount() == 0 || idx.KeyCount() != ov.KeyCount() {
		return asyncMaterializedPredBuildFailed
	}

	if cached, ok := qv.snap.LoadNumericRangeFullSpan(entry, plan.fieldSlot, plan.startBucket, plan.endBucket); ok {
		cached.Release()
		return asyncMaterializedPredBuildSkippedCached
	}

	if qv.snap.NumericRangeFullSpanRejectedTooLarge(plan.fieldSlot, plan.startBucket, plan.endBucket) {
		return asyncMaterializedPredBuildStoreRejected
	}
	if asyncMaterializedPredCheck(cancel) {
		return asyncMaterializedPredBuildCanceled
	}

	pending := ov.RangeByRanks(idx.BucketStart(plan.startBucket), idx.BucketEnd(plan.endBucket))
	ids, ok := ov.UnionRangePostingsUntil(pending, indexdata.FieldIndexRange{}, cancel)
	if !ok {
		return asyncMaterializedPredBuildCanceled
	}

	if cancel.Canceled() {
		ids.Release()
		return asyncMaterializedPredBuildCanceled
	}

	stored, ok := qv.snap.TryStoreNumericRangeFullSpan(entry, plan.fieldSlot, plan.startBucket, plan.endBucket, ids)
	if !ok {
		ids.Release()
		return asyncMaterializedPredBuildStoreRejected
	}
	stored.Release()

	return asyncMaterializedPredBuildOK
}

func (qv *View) buildAndStoreAsyncNumericRangeExact(plan asyncMaterializedPredPlan, cancel *asyncMaterializedPredCancel) asyncMaterializedPredBuildStatus {
	ov := qv.indexViewByOrdinal(plan.fieldOrdinal)
	if !ov.HasData() {
		return asyncMaterializedPredBuildFailed
	}

	fm := qv.fieldMetaByOrdinal(plan.fieldOrdinal)
	if !schema.FieldUsesOrderedNumericKeys(fm) {
		return asyncMaterializedPredBuildFailed
	}

	br := ov.RangeForBounds(plan.bounds)
	if br.Empty() || br.BaseStart != plan.startRank || br.BaseEnd != plan.endRank {
		return asyncMaterializedPredBuildFailed
	}

	field := qv.exec.FieldNameByOrdinal(plan.fieldOrdinal)
	span, ok := qv.numericRangeBucketSpan(field, plan.fieldOrdinal, fm, ov, br)
	if !ok || span.fieldSlot != plan.fieldSlot {
		return asyncMaterializedPredBuildFailed
	}

	if cached, ok := qv.snap.LoadNumericRangeExactResult(plan.fieldSlot, plan.startRank, plan.endRank); ok {
		cached.Release()
		return asyncMaterializedPredBuildSkippedCached
	}

	if qv.snap.NumericRangeExactResultRejectedTooLarge(plan.fieldSlot, plan.startRank, plan.endRank) {
		return asyncMaterializedPredBuildStoreRejected
	}
	if asyncMaterializedPredCheck(cancel) {
		return asyncMaterializedPredBuildCanceled
	}

	out, ok := evalNumericRangeBucketSpanUntil(ov, br, span, cancel)
	if !ok {
		return asyncMaterializedPredBuildCanceled
	}

	if cancel.Canceled() {
		out.ids.Release()
		return asyncMaterializedPredBuildCanceled
	}
	stored, ok := qv.snap.TryStorePromotedNumericRangeExactResult(plan.fieldSlot, plan.startRank, plan.endRank, out.ids)
	if !ok {
		out.ids.Release()
		return asyncMaterializedPredBuildStoreRejected
	}
	stored.Release()

	return asyncMaterializedPredBuildOK
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
