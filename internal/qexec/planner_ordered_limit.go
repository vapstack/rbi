package qexec

import (
	"math"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/schema"
)

type plannerMaterializedCacheState uint8

const (
	plannerMaterializedCacheDisabled plannerMaterializedCacheState = iota
	plannerMaterializedCacheWarmHit
	plannerMaterializedCacheColdRegularAdmissible
	plannerMaterializedCacheColdOversizedAdmissible
	plannerMaterializedCacheColdUnretainedByCardinality
	plannerMaterializedCacheColdUnretainedByPolicy
	plannerMaterializedCacheColdSecondHitRequired
)

func (s plannerMaterializedCacheState) String() string {
	switch s {
	case plannerMaterializedCacheWarmHit:
		return "warm_hit"
	case plannerMaterializedCacheColdRegularAdmissible:
		return "cold_regular_admissible"
	case plannerMaterializedCacheColdOversizedAdmissible:
		return "cold_oversized_admissible"
	case plannerMaterializedCacheColdUnretainedByCardinality:
		return "cold_unretained_by_cardinality"
	case plannerMaterializedCacheColdUnretainedByPolicy:
		return "cold_unretained_by_policy"
	case plannerMaterializedCacheColdSecondHitRequired:
		return "cold_second_hit_required"
	default:
		return "disabled"
	}
}

type plannerOrderedLimitCandidateKind uint8

const (
	plannerOrderedLimitCandidateNone plannerOrderedLimitCandidateKind = iota
	plannerOrderedLimitCandidateEmpty
	plannerOrderedLimitCandidateOrderScan
	plannerOrderedLimitCandidateOrderedAnchor
	plannerOrderedLimitCandidateWarmBaseCore
	plannerOrderedLimitCandidateColdRetainedBaseCore
	plannerOrderedLimitCandidateColdUnretainedBaseCore
	plannerOrderedLimitCandidateCandidateOrder
	plannerOrderedLimitCandidateMaterializedFallback
)

func (k plannerOrderedLimitCandidateKind) String() string {
	switch k {
	case plannerOrderedLimitCandidateEmpty:
		return "empty"
	case plannerOrderedLimitCandidateOrderScan:
		return "order_scan"
	case plannerOrderedLimitCandidateOrderedAnchor:
		return "ordered_anchor"
	case plannerOrderedLimitCandidateWarmBaseCore:
		return "warm_base_core"
	case plannerOrderedLimitCandidateColdRetainedBaseCore:
		return "cold_retained_base_core"
	case plannerOrderedLimitCandidateColdUnretainedBaseCore:
		return "cold_unretained_base_core"
	case plannerOrderedLimitCandidateCandidateOrder:
		return "candidate_order"
	case plannerOrderedLimitCandidateMaterializedFallback:
		return "materialized_fallback"
	default:
		return ""
	}
}

func (k plannerOrderedLimitCandidateKind) usesBaseCore() bool {
	return k == plannerOrderedLimitCandidateWarmBaseCore ||
		k == plannerOrderedLimitCandidateColdRetainedBaseCore ||
		k == plannerOrderedLimitCandidateColdUnretainedBaseCore
}

type plannerOrderedLimitCandidate struct {
	kind         plannerOrderedLimitCandidateKind
	cost         float64
	expectedRows uint64
	buckets      uint64
	checks       uint64
	exactFilters uint64
	buildWork    uint64
	cacheState   plannerMaterializedCacheState
}

type plannerOrderedLimitDecision struct {
	selected             plannerOrderedLimitCandidate
	runtimeFallback      plannerOrderedLimitCandidate
	materializedFallback plannerOrderedLimitCandidate
	rejected             plannerOrderedLimitCandidate
}

type plannerOrderedLimitRuntimeGuard struct {
	enabled      bool
	minExamined  uint64
	needWindow   uint64
	fallbackCost float64
	rowCost      float64
	reason       string
}

type orderedLimitFacts struct {
	order        qir.Order
	orderField   string
	orderMeta    *schema.Field
	needWindow   int
	ops          []qir.Expr
	opsBuf       []qir.Expr
	baseOps      []qir.Expr
	baseOpsBuf   []qir.Expr
	bounds       indexdata.Bounds
	ov           indexdata.FieldIndexView
	br           indexdata.FieldIndexRange
	nilTailField string
	empty        bool
	validateBase bool
}

var orderedLimitFactsPool = pooled.Pointers[orderedLimitFacts]{
	Cleanup: func(facts *orderedLimitFacts) {
		opsBuf := facts.opsBuf
		baseOpsBuf := facts.baseOpsBuf
		if cap(opsBuf) > 0 {
			clear(opsBuf[:cap(opsBuf)])
			opsBuf = opsBuf[:0]
		}
		if cap(baseOpsBuf) > 0 {
			clear(baseOpsBuf[:cap(baseOpsBuf)])
			baseOpsBuf = baseOpsBuf[:0]
		}
		*facts = orderedLimitFacts{
			opsBuf:     opsBuf,
			baseOpsBuf: baseOpsBuf,
		}
	},
}

func (facts *orderedLimitFacts) Release() {
	orderedLimitFactsPool.Put(facts)
}

func (d plannerOrderedLimitDecision) usesBaseCore() bool {
	return d.selected.kind.usesBaseCore()
}

func (d plannerOrderedLimitDecision) selectedKind() plannerOrderedLimitCandidateKind {
	return d.selected.kind
}

func (d plannerOrderedLimitDecision) traceRoute() TraceOrderedLimitRoute {
	return TraceOrderedLimitRoute{
		Selected:        d.selected.kind.String(),
		Rejected:        d.rejected.kind.String(),
		CacheState:      d.selected.cacheState.String(),
		SelectedCost:    d.selected.cost,
		RejectedCost:    d.rejected.cost,
		ExpectedRows:    d.selected.expectedRows,
		OrderBuckets:    d.selected.buckets,
		PredicateChecks: d.selected.checks,
		ExactFilters:    d.selected.exactFilters,
		PostingBuild:    d.selected.buildWork,
	}
}

func (d plannerOrderedLimitDecision) runtimeGuard(q *qir.Shape) plannerOrderedLimitRuntimeGuard {
	if d.selected.kind != plannerOrderedLimitCandidateOrderScan &&
		d.selected.kind != plannerOrderedLimitCandidateOrderedAnchor {
		return plannerOrderedLimitRuntimeGuard{}
	}
	if d.runtimeFallback.kind == plannerOrderedLimitCandidateNone || d.runtimeFallback.cost <= 0 {
		return plannerOrderedLimitRuntimeGuard{}
	}
	need, ok := orderWindow(q)
	if !ok || need <= 0 {
		return plannerOrderedLimitRuntimeGuard{}
	}
	expected := d.selected.expectedRows
	if expected < uint64(need) {
		expected = uint64(need)
	}
	minExamined := expected * 2
	minByNeed := uint64(need) * 64
	if minExamined > minByNeed {
		minExamined = minByNeed
	}
	if minExamined < 512 {
		minExamined = 512
	}
	rowCost := 1.0 + float64(d.selected.checks)*0.75
	if rowCost < 1 {
		rowCost = 1
	}
	fallbackCost := d.runtimeFallback.cost
	placementFallbackCap := float64(need) * (48.0 + float64(d.selected.checks)*8.0)
	if placementFallbackCap > 0 && fallbackCost > placementFallbackCap {
		fallbackCost = placementFallbackCap
	}
	return plannerOrderedLimitRuntimeGuard{
		enabled:      true,
		minExamined:  minExamined,
		needWindow:   uint64(need),
		fallbackCost: fallbackCost,
		rowCost:      rowCost,
		reason:       "placement_guard",
	}
}

func (g plannerOrderedLimitRuntimeGuard) shouldFallback(examined uint64, emitted int) bool {
	if !g.enabled || examined < g.minExamined {
		return false
	}
	if emitted > 0 && uint64(emitted)*4 >= g.needWindow {
		return false
	}
	return float64(examined)*g.rowCost > g.fallbackCost*1.25
}

type plannerOrderedLimitBaseCoreStats struct {
	cacheKey   qcache.MaterializedPredKey
	est        uint64
	buildWork  uint64
	probeWork  uint64
	warm       bool
	useful     bool
	cacheState plannerMaterializedCacheState
}

func (qv *View) classifyPlannerMaterializedCacheKey(key qcache.MaterializedPredKey, est uint64, requiresSecondHit bool) plannerMaterializedCacheState {
	if key.IsZero() || qv.snap.MaterializedPredCacheLimit() <= 0 {
		return plannerMaterializedCacheDisabled
	}
	if qv.snap.HasMaterializedPredKey(key) {
		return plannerMaterializedCacheWarmHit
	}
	if requiresSecondHit && !qv.snap.HasRuntimeMaterializedPredSeenKey(key) {
		return plannerMaterializedCacheColdSecondHitRequired
	}
	cache := qv.snap.MaterializedPredCache()
	if cache == nil {
		return plannerMaterializedCacheDisabled
	}
	maxCard := cache.MaxCardinality()
	if maxCard == 0 || est <= maxCard {
		return plannerMaterializedCacheColdRegularAdmissible
	}
	if qcache.MaterializedPredOversizedLimit(cache.Limit()) > 0 {
		return plannerMaterializedCacheColdOversizedAdmissible
	}
	return plannerMaterializedCacheColdUnretainedByCardinality
}

func plannerMaterializedCacheStateRetained(s plannerMaterializedCacheState) bool {
	return s == plannerMaterializedCacheWarmHit ||
		s == plannerMaterializedCacheColdRegularAdmissible ||
		s == plannerMaterializedCacheColdOversizedAdmissible
}

func (qv *View) orderedLimitCollapsedRangeStats(op preparedScalarExactRange) (scalarMaterializationStats, bool) {
	ov := qv.fieldIndexViewFromSlotsByName(qv.snap.Index, op.field)
	if !ov.HasData() {
		return scalarMaterializationStats{}, false
	}
	br := ov.RangeForBounds(op.bounds)
	if br.Empty() {
		return scalarMaterializationStats{}, false
	}
	buckets, est := ov.RangeStats(br)
	if buckets == 0 || est == 0 {
		return scalarMaterializationStats{}, false
	}
	return scalarMaterializationStats{
		cacheKey:     op.cacheKey,
		probeBuckets: buckets,
		probeEst:     est,
		buildBuckets: buckets,
		buildEst:     est,
	}, true
}

func (qv *View) orderedLimitBaseCoreStats(
	core orderBasicBaseCore,
	universe uint64,
	needWindow int,
	hasOrderBounds bool,
) (plannerOrderedLimitBaseCoreStats, bool) {
	var (
		stats             scalarMaterializationStats
		ok                bool
		requiresSecondHit bool
	)

	switch core.kind {
	case orderBasicBaseCoreCollapsedRange:
		stats, ok = qv.orderedLimitCollapsedRangeStats(core.collapsed)
	case orderBasicBaseCoreRawExpr:
		stats, ok = qv.orderBasicRawBaseOpStats(core.expr, universe)
		requiresSecondHit = true
	default:
		return plannerOrderedLimitBaseCoreStats{}, false
	}
	if !ok || stats.buildBuckets == 0 || stats.buildEst == 0 {
		return plannerOrderedLimitBaseCoreStats{}, false
	}

	buildWork := rangeProbeMaterializeWork(stats.buildBuckets, stats.buildEst)
	if buildWork == 0 {
		return plannerOrderedLimitBaseCoreStats{}, false
	}

	expectedRows := orderedPredicateExpectedRows(needWindow, stats.probeEst, universe)
	probeWork := rangeProbeTotalWorkForRows(clampUint64ToInt(expectedRows), stats.probeBuckets, stats.probeEst)
	retainedPenalty := satMulUint64(stats.probeEst, postingContainsLookupWork(stats.probeEst))

	useful := probeWork >= satAddUint64(buildWork, retainedPenalty)
	if !useful && hasOrderBounds {
		minRows := satMulUint64(uint64(needWindow), orderBasicBoundedRangeBaseMinRowsPerNeed)
		useful = stats.probeBuckets <= rangePostingFilterKeepProbeMaxBuckets && stats.probeEst >= minRows
	}

	cacheState := qv.classifyPlannerMaterializedCacheKey(stats.cacheKey, stats.buildEst, requiresSecondHit)
	return plannerOrderedLimitBaseCoreStats{
		cacheKey:   stats.cacheKey,
		est:        stats.buildEst,
		buildWork:  buildWork,
		probeWork:  probeWork,
		warm:       cacheState == plannerMaterializedCacheWarmHit,
		useful:     useful,
		cacheState: cacheState,
	}, true
}

func plannerOrderedLimitCacheGroupState(cacheState plannerMaterializedCacheState, next plannerMaterializedCacheState) plannerMaterializedCacheState {
	if cacheState == plannerMaterializedCacheDisabled {
		return next
	}
	if next == plannerMaterializedCacheWarmHit {
		return cacheState
	}
	if cacheState == plannerMaterializedCacheWarmHit {
		return next
	}
	if next == plannerMaterializedCacheColdSecondHitRequired ||
		cacheState == plannerMaterializedCacheColdSecondHitRequired {
		return plannerMaterializedCacheColdSecondHitRequired
	}
	if next == plannerMaterializedCacheColdUnretainedByCardinality ||
		cacheState == plannerMaterializedCacheColdUnretainedByCardinality {
		return plannerMaterializedCacheColdUnretainedByCardinality
	}
	if next == plannerMaterializedCacheColdUnretainedByPolicy ||
		cacheState == plannerMaterializedCacheColdUnretainedByPolicy {
		return plannerMaterializedCacheColdUnretainedByPolicy
	}
	if next == plannerMaterializedCacheColdOversizedAdmissible ||
		cacheState == plannerMaterializedCacheColdOversizedAdmissible {
		return plannerMaterializedCacheColdOversizedAdmissible
	}
	return plannerMaterializedCacheColdRegularAdmissible
}

func plannerOrderedLimitBaseKind(warm bool, state plannerMaterializedCacheState) plannerOrderedLimitCandidateKind {
	if warm {
		return plannerOrderedLimitCandidateWarmBaseCore
	}
	if plannerMaterializedCacheStateRetained(state) {
		return plannerOrderedLimitCandidateColdRetainedBaseCore
	}
	return plannerOrderedLimitCandidateColdUnretainedBaseCore
}

func plannerOrderedLimitCost(work uint64) float64 {
	if work == ^uint64(0) {
		return math.MaxFloat64
	}
	return float64(work)
}

func (qv *View) orderedLimitOrderScanCost(
	q *qir.Shape,
	leaves []qir.Expr,
	ov indexdata.FieldIndexView,
	br indexdata.FieldIndexRange,
	needWindow int,
	orderedDecision plannerOrderedDecision,
) plannerOrderedLimitCandidate {
	pureOrder := len(leaves) == 0
	if !pureOrder {
		pureOrder = true
		for i := range leaves {
			e := leaves[i]
			if e.Not || e.FieldOrdinal != q.Order.FieldOrdinal || len(e.Operands) != 0 ||
				(!isScalarRangeEqOp(e.Op) && e.Op != qir.OpPREFIX) {
				pureOrder = false
				break
			}
		}
	}
	if pureOrder {
		_, rows := ov.RangeStats(br)
		expectedRows := uint64(needWindow)
		if rows > 0 && expectedRows > rows {
			expectedRows = rows
		}
		buckets := uint64(ov.KeyCount())
		if !br.Empty() && br.BaseEnd >= br.BaseStart {
			buckets = uint64(br.BaseEnd - br.BaseStart)
		}
		return plannerOrderedLimitCandidate{
			kind:         plannerOrderedLimitCandidateOrderScan,
			cost:         float64(expectedRows),
			expectedRows: expectedRows,
			buckets:      buckets,
			cacheState:   plannerMaterializedCacheDisabled,
		}
	}

	execDecision := qv.decideExecutionOrderByCost(q, leaves)

	expectedRows := execDecision.expectedProbeRows
	cost := execDecision.executionCost
	if cost <= 0 {
		expectedRows = orderedDecision.expectedProbeRows
		cost = orderedDecision.orderedCost
	}
	if cost <= 0 {
		_, rows := ov.RangeStats(br)
		expectedRows = rows
		if expectedRows == 0 {
			expectedRows = uint64(needWindow)
		}
		cost = float64(expectedRows) * (1.0 + float64(len(leaves))*0.75)
	}

	_, orderRows := ov.RangeStats(br)
	if expectedRows == 0 || (orderRows > 0 && expectedRows > orderRows) {
		expectedRows = orderRows
	}
	if expectedRows == 0 {
		expectedRows = uint64(needWindow)
	}

	buckets := uint64(ov.KeyCount())
	if !br.Empty() && br.BaseEnd >= br.BaseStart {
		buckets = uint64(br.BaseEnd - br.BaseStart)
	}

	return plannerOrderedLimitCandidate{
		kind:         plannerOrderedLimitCandidateOrderScan,
		cost:         cost,
		expectedRows: expectedRows,
		buckets:      buckets,
		checks:       uint64(len(leaves)),
		cacheState:   plannerMaterializedCacheDisabled,
	}
}

func orderedLimitAnchorCandidate(decision plannerOrderedDecision, leaves []qir.Expr) (plannerOrderedLimitCandidate, bool) {
	if !decision.use || decision.orderedCost <= 0 {
		return plannerOrderedLimitCandidate{}, false
	}
	return plannerOrderedLimitCandidate{
		kind:         plannerOrderedLimitCandidateOrderedAnchor,
		cost:         decision.orderedCost,
		expectedRows: decision.expectedProbeRows,
		checks:       uint64(len(leaves)),
		cacheState:   plannerMaterializedCacheDisabled,
	}, true
}

func orderedLimitMaterializedFallbackCandidate(
	q *qir.Shape,
	leaves []qir.Expr,
	orderCost float64,
	decision plannerOrderedDecision,
) plannerOrderedLimitCandidate {
	cost := decision.fallbackCost
	if cost <= 0 {
		cost = orderCost * 4
		if cost <= 0 {
			cost = float64(q.Offset+q.Limit) * (8.0 + float64(len(leaves)))
		}
	}
	return plannerOrderedLimitCandidate{
		kind:       plannerOrderedLimitCandidateMaterializedFallback,
		cost:       cost,
		checks:     uint64(len(leaves)),
		cacheState: plannerMaterializedCacheDisabled,
	}
}

func (qv *View) orderedLimitBaseOpsCandidate(
	baseOps []qir.Expr,
	needWindow int,
	hasOrderBounds bool,
	hasOffset bool,
	orderCandidate plannerOrderedLimitCandidate,
) (plannerOrderedLimitCandidate, bool, bool, error) {
	if len(baseOps) == 0 || needWindow <= 0 {
		return plannerOrderedLimitCandidate{}, false, false, nil
	}
	universe := qv.snap.Universe.Cardinality()
	if universe == 0 {
		return plannerOrderedLimitCandidate{}, false, false, nil
	}

	cache := qv.snap.MaterializedPredCache()
	limit := 0
	oversizedLimit := int32(0)
	maxCard := uint64(0)
	if cache != nil {
		limit = cache.Limit()
		oversizedLimit = qcache.MaterializedPredOversizedLimit(limit)
		maxCard = cache.MaxCardinality()
	}

	var (
		cacheState   plannerMaterializedCacheState
		buildWork    uint64
		probeWork    uint64
		warm         bool
		useful       bool
		coldRegular  int
		coldOversize int32
		coreCount    int
	)

	var keys [limitQueryFastPathMaxLeaves]qcache.MaterializedPredKey
	keyCount := 0

	for i, op := range baseOps {
		core := orderBasicBaseCore{
			kind: orderBasicBaseCoreRawExpr,
			expr: op,
		}

		fm := qv.fieldMetaByExpr(op)
		if isOrderBasicCollapsibleScalarRangeExpr(op, fm) {
			fieldName := qv.exec.FieldNameByOrdinal(op.FieldOrdinal)
			var rb indexdata.Bounds
			groupCount := 0
			first := true
			for j := 0; j < len(baseOps); j++ {
				other := baseOps[j]
				if other.FieldOrdinal != op.FieldOrdinal || !isOrderBasicCollapsibleScalarRangeExpr(other, fm) {
					continue
				}
				if j < i {
					first = false
				}
				nextRB, ok, err := qv.rangeBoundsForScalarExpr(other)
				if err != nil {
					return plannerOrderedLimitCandidate{}, false, false, err
				}
				if !ok {
					continue
				}
				mergeRangeBounds(&rb, nextRB)
				groupCount++
			}
			if groupCount > 1 {
				if rb.Empty {
					return plannerOrderedLimitCandidate{}, false, true, nil
				}
				if qv.shouldCollapseOrderBasicScalarRange(fieldName, op.FieldOrdinal, rb) {
					if !first {
						continue
					}
					core = orderBasicBaseCore{
						kind: orderBasicBaseCoreCollapsedRange,
						collapsed: preparedScalarExactRange{
							field:    fieldName,
							bounds:   rb,
							cacheKey: qv.materializedPredKeyForExactScalarRange(fieldName, rb),
						},
					}
				}
			}
		}

		coreCount++
		st, ok := qv.orderedLimitBaseCoreStats(core, universe, needWindow, hasOrderBounds)
		if !ok {
			continue
		}
		if st.warm {
			warm = true
		} else {
			buildWork = satAddUint64(buildWork, st.buildWork)
		}
		probeWork = satAddUint64(probeWork, st.probeWork)
		if st.useful {
			useful = true
		}

		state := st.cacheState
		if state != plannerMaterializedCacheWarmHit && !st.cacheKey.IsZero() {
			seen := false
			for j := 0; j < keyCount; j++ {
				if keys[j] == st.cacheKey {
					seen = true
					break
				}
			}
			if !seen {
				if keyCount < len(keys) {
					keys[keyCount] = st.cacheKey
					keyCount++
				}
				if state == plannerMaterializedCacheColdRegularAdmissible {
					coldRegular++
				} else if state == plannerMaterializedCacheColdOversizedAdmissible {
					coldOversize++
				}
			}
		}
		cacheState = plannerOrderedLimitCacheGroupState(cacheState, state)
	}

	placementFallback := false
	if !warm && !useful {
		if hasOffset || orderCandidate.cost <= 0 || buildWork == 0 {
			return plannerOrderedLimitCandidate{}, false, false, nil
		}
		placementFallback = true
	}
	if limit <= 0 {
		cacheState = plannerMaterializedCacheDisabled
	} else if coldRegular+int(coldOversize) > limit || coldOversize > oversizedLimit {
		cacheState = plannerMaterializedCacheColdUnretainedByPolicy
	}

	expectedRows := orderCandidate.expectedRows
	if expectedRows == 0 {
		expectedRows = uint64(needWindow)
	}
	baseCost := float64(expectedRows)*(0.60+float64(coreCount)*0.16) + plannerOrderedLimitCost(buildWork)
	if warm {
		baseCost *= 0.82
	}
	if !plannerMaterializedCacheStateRetained(cacheState) {
		baseCost += plannerOrderedLimitCost(buildWork) * 0.35
	}
	if useful && probeWork > buildWork {
		saved := plannerOrderedLimitCost(probeWork - buildWork)
		maxSaved := orderCandidate.cost * 0.70
		if saved > maxSaved {
			saved = maxSaved
		}
		baseCost = math.Max(1, baseCost-saved*0.35)
	}
	if placementFallback && baseCost <= orderCandidate.cost {
		baseCost = orderCandidate.cost * 1.08
	}
	placementCapAllowed := buildWork == 0 || (maxCard > 0 && buildWork <= maxCard)
	if placementCapAllowed && hasOffset && hasOrderBounds && orderCandidate.cost > 0 && orderCandidate.expectedRows > uint64(needWindow) {
		placementCost := orderCandidate.cost * float64(needWindow) / float64(orderCandidate.expectedRows)
		if placementCost < baseCost {
			baseCost = placementCost
		}
	}

	return plannerOrderedLimitCandidate{
		kind:         plannerOrderedLimitBaseKind(warm, cacheState),
		cost:         baseCost,
		expectedRows: expectedRows,
		buckets:      orderCandidate.buckets,
		checks:       uint64(coreCount),
		buildWork:    buildWork,
		cacheState:   cacheState,
	}, true, false, nil
}

func plannerOrderedLimitPick(candidates []plannerOrderedLimitCandidate) plannerOrderedLimitDecision {
	var d plannerOrderedLimitDecision
	if len(candidates) == 0 {
		return d
	}
	best := 0
	for i := 1; i < len(candidates); i++ {
		if candidates[i].cost < candidates[best].cost {
			best = i
		}
	}
	d.selected = candidates[best]
	for i := range candidates {
		if candidates[i].kind == plannerOrderedLimitCandidateMaterializedFallback {
			d.materializedFallback = candidates[i]
		}
		if i == best {
			continue
		}
		if d.rejected.kind == plannerOrderedLimitCandidateNone || candidates[i].cost < d.rejected.cost {
			d.rejected = candidates[i]
		}
	}
	d.runtimeFallback = d.rejected
	return d
}

func (qv *View) collectOrderedLimitFacts(
	q *qir.Shape,
	facts *orderedLimitFacts,
) (bool, error) {
	if !q.HasOrder || q.Limit == 0 {
		return false, nil
	}
	needWindow, ok := orderWindow(q)
	if !ok || needWindow <= 0 {
		return false, nil
	}

	order := q.Order
	if order.Kind != qir.OrderKindBasic {
		return false, nil
	}
	if q.Expr.Not && (q.Expr.Op == qir.OpAND || q.Expr.Op == qir.OpOR) {
		return false, nil
	}

	facts.order = order
	facts.needWindow = needWindow
	facts.orderField = qv.exec.FieldNameByOrdinal(order.FieldOrdinal)

	fm := qv.fieldMetaByOrder(order)
	if fm == nil || fm.Slice {
		return false, nil
	}
	facts.orderMeta = fm

	if qir.IsTrueConst(q.Expr) {
		facts.ops = nil
	} else if q.Expr.Op == qir.OpAND {
		facts.ops = q.Expr.Operands
	} else {
		facts.opsBuf = append(facts.opsBuf[:0], q.Expr)
		facts.ops = facts.opsBuf
	}

	orderFirst := -1
	orderLast := -1
	for i := 0; i < len(facts.ops); i++ {
		if facts.ops[i].FieldOrdinal == order.FieldOrdinal {
			if orderFirst < 0 {
				orderFirst = i
			}
			orderLast = i
		}
	}

	var (
		rb                    indexdata.Bounds
		orderEqNil            bool
		orderEqNilConflict    bool
		orderNonNilConstraint bool
	)

	if orderFirst < 0 {
		facts.baseOps = facts.ops
	} else {
		orderOpsContiguous := true
		for i := orderFirst + 1; i < orderLast; i++ {
			if facts.ops[i].FieldOrdinal != order.FieldOrdinal {
				orderOpsContiguous = false
				break
			}
		}
		baseOpsContiguous := true
		if orderOpsContiguous && orderFirst == 0 {
			facts.baseOps = facts.ops[orderLast+1:]
		} else if orderOpsContiguous && orderLast == len(facts.ops)-1 {
			facts.baseOps = facts.ops[:orderFirst]
		} else {
			baseOpsContiguous = false
			facts.baseOpsBuf = facts.baseOpsBuf[:0]
			facts.baseOps = facts.baseOpsBuf
		}

		for _, op := range facts.ops {
			if op.FieldOrdinal == order.FieldOrdinal {
				if op.Not || (!isScalarRangeEqOp(op.Op) && op.Op != qir.OpPREFIX) {
					return false, nil
				}
				if op.Op == qir.OpEQ {
					_, isSlice, isNil, err := qv.exprValueToLookupKey(op)
					if err != nil {
						return true, err
					}
					if isSlice {
						return false, nil
					}
					if isNil {
						if orderNonNilConstraint {
							orderEqNilConflict = true
						}
						orderEqNil = true
						rb.SetEmpty()
						continue
					}
				}
				orderNonNilConstraint = true

				if orderEqNil {
					orderEqNilConflict = true
				}

				nextRB, ok, err := qv.rangeBoundsForScalarExpr(op)
				if err != nil {
					return true, err
				}
				if !ok {
					return false, nil
				}
				mergeRangeBounds(&rb, nextRB)
				continue
			}
			if !baseOpsContiguous {
				facts.baseOpsBuf = append(facts.baseOpsBuf, op)
				facts.baseOps = facts.baseOpsBuf
			}
		}
	}

	facts.bounds = rb

	if orderEqNilConflict {
		facts.empty = true
		facts.validateBase = true
		return true, nil
	}

	facts.nilTailField = orderNilTailField(fm, facts.orderField, rb)
	if orderEqNil {
		if !fm.Ptr {
			facts.empty = true
			facts.validateBase = true
			return true, nil
		}
		facts.nilTailField = facts.orderField
	}

	facts.ov = qv.fieldIndexViewFromSlotsForOrder(qv.snap.Index, order)
	if !facts.ov.HasData() && facts.nilTailField == "" {
		if !qv.hasIndexedFieldForOrder(order) {
			return false, nil
		}
		facts.empty = true
		facts.validateBase = true
		return true, nil
	}

	facts.br = facts.ov.RangeForBounds(rb)
	if facts.br.Empty() && facts.nilTailField == "" {
		facts.empty = true
		facts.validateBase = true
		return true, nil
	}

	return true, nil
}

func (qv *View) selectOrderedLimit(
	q *qir.Shape,
	facts *orderedLimitFacts,
) (plannerOrderedLimitDecision, bool, error) {
	if facts.empty {
		return plannerOrderedLimitDecision{
			selected: plannerOrderedLimitCandidate{
				kind:         plannerOrderedLimitCandidateOrderScan,
				cost:         1,
				expectedRows: 0,
				buckets:      0,
				checks:       uint64(len(facts.ops)),
				cacheState:   plannerMaterializedCacheDisabled,
			},
		}, true, nil
	}

	var candidates [7]plannerOrderedLimitCandidate
	n := 0

	snap := qv.exec.Stats.Load()
	universe := snap.UniverseOr(qv.snap.Universe.Cardinality())
	baseSupported := true
	emptySupported := false
	for i := 0; i < len(facts.baseOps); i++ {
		e := facts.baseOps[i]
		if !qv.supportsLimitLeafPredExpr(e) {
			baseSupported = false
		}
		sel, _, _, _, ok := qv.estimateLeafOrderCost(e, snap, universe, facts.orderField, facts.ov.HasData())
		if !ok {
			baseSupported = false
			continue
		}
		if !e.Not && sel == 0 {
			emptySupported = true
			continue
		}
	}
	if emptySupported && baseSupported {
		return plannerOrderedLimitDecision{
			selected: plannerOrderedLimitCandidate{
				kind: plannerOrderedLimitCandidateEmpty,
				cost: 1,
			},
		}, true, nil
	}

	if q.Offset == 0 && facts.bounds.HasPrefix && len(facts.baseOps) > 0 && len(facts.baseOps) <= 2 {
		fastPrefix := true
		for i := 0; i < len(facts.baseOps); i++ {
			e := facts.baseOps[i]
			if e.Not || e.FieldOrdinal < 0 || len(e.Operands) != 0 || !isScalarEqOrInOp(e.Op) {
				fastPrefix = false
				break
			}
			fm := qv.fieldMetaByExpr(e)
			if fm == nil || fm.Slice {
				fastPrefix = false
				break
			}
		}
		if fastPrefix {
			expectedRows := uint64(facts.needWindow)
			_, rows := facts.ov.RangeStats(facts.br)
			if rows > 0 && expectedRows > rows {
				expectedRows = rows
			}
			buckets := uint64(0)
			if !facts.br.Empty() && facts.br.BaseEnd >= facts.br.BaseStart {
				buckets = uint64(facts.br.BaseEnd - facts.br.BaseStart)
			}
			orderScan := plannerOrderedLimitCandidate{
				kind:         plannerOrderedLimitCandidateOrderScan,
				cost:         float64(expectedRows) * (1.0 + float64(len(facts.baseOps))*0.35),
				expectedRows: expectedRows,
				buckets:      buckets,
				checks:       uint64(len(facts.baseOps)),
				cacheState:   plannerMaterializedCacheDisabled,
			}
			fallback := orderedLimitMaterializedFallbackCandidate(q, facts.ops, orderScan.cost, plannerOrderedDecision{})
			return plannerOrderedLimitDecision{
				selected:             orderScan,
				runtimeFallback:      fallback,
				materializedFallback: fallback,
				rejected:             fallback,
			}, true, nil
		}
	}

	var orderedDecision plannerOrderedDecision
	if len(facts.baseOps) != 0 {
		needOrderedDecision := true
		if q.Offset == 0 && !facts.bounds.HasLo && !facts.bounds.HasHi && !facts.bounds.HasPrefix {
			needOrderedDecision = false
			for i := 0; i < len(facts.ops); i++ {
				if facts.ops[i].Not {
					needOrderedDecision = true
					break
				}
			}
		}
		if needOrderedDecision {
			orderedDecision = qv.decideOrderedByCost(q, facts.ops)
		}
	}

	orderScan := qv.orderedLimitOrderScanCost(q, facts.ops, facts.ov, facts.br, facts.needWindow, orderedDecision)
	if len(facts.baseOps) == 0 {
		return plannerOrderedLimitDecision{selected: orderScan}, true, nil
	}
	candidates[n] = orderScan
	n++
	orderCandidate := orderScan

	if anchor, ok := orderedLimitAnchorCandidate(orderedDecision, facts.ops); ok {
		candidates[n] = anchor
		n++
		if anchor.cost > 0 && (orderCandidate.cost <= 0 || anchor.cost < orderCandidate.cost) {
			orderCandidate = anchor
		}
	}

	hasOrderBounds := facts.bounds.HasLo || facts.bounds.HasHi || facts.bounds.HasPrefix
	base, ok, noMatch, err := qv.orderedLimitBaseOpsCandidate(
		facts.baseOps,
		facts.needWindow,
		hasOrderBounds,
		q.Offset > 0,
		orderCandidate,
	)
	if err != nil {
		return plannerOrderedLimitDecision{}, false, err
	}
	if noMatch {
		facts.empty = true
		facts.validateBase = false
		return plannerOrderedLimitDecision{
			selected: plannerOrderedLimitCandidate{
				kind:         plannerOrderedLimitCandidateOrderScan,
				cost:         1,
				expectedRows: 0,
				buckets:      orderScan.buckets,
				checks:       uint64(len(facts.ops)),
				cacheState:   plannerMaterializedCacheDisabled,
			},
		}, true, nil
	}
	if ok {
		candidates[n] = base
		n++
	}

	if qv.shouldUseCandidateOrder(facts.order, facts.ops) {
		cost := orderScan.cost * 0.88
		if cost <= 0 {
			cost = float64(facts.needWindow) * (2.0 + float64(len(facts.ops))*0.55)
		}
		candidates[n] = plannerOrderedLimitCandidate{
			kind:         plannerOrderedLimitCandidateCandidateOrder,
			cost:         cost,
			expectedRows: orderScan.expectedRows,
			buckets:      orderScan.buckets,
			checks:       uint64(len(facts.ops)),
			cacheState:   plannerMaterializedCacheDisabled,
		}
		n++
	}

	candidates[n] = orderedLimitMaterializedFallbackCandidate(q, facts.ops, orderScan.cost, orderedDecision)
	n++

	return plannerOrderedLimitPick(candidates[:n]), true, nil
}
