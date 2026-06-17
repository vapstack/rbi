package qexec

import (
	"math"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/mathutil"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/rbitrace"
)

type plannerMaterializedCacheState byte

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
	plannerOrderedLimitCandidateOrderedBasic
	plannerOrderedLimitCandidateWarmBaseCore
	plannerOrderedLimitCandidateColdRetainedBaseCore
	plannerOrderedLimitCandidateColdUnretainedBaseCore
	plannerOrderedLimitCandidateCandidateOrder
	plannerOrderedLimitCandidateNumericKeyScan
	plannerOrderedLimitCandidateMaterializedFallback
)

func (k plannerOrderedLimitCandidateKind) String() string {
	switch k {
	case plannerOrderedLimitCandidateEmpty:
		return "empty"
	case plannerOrderedLimitCandidateOrderScan:
		return "order_scan"
	case plannerOrderedLimitCandidateOrderedBasic:
		return "ordered_basic"
	case plannerOrderedLimitCandidateWarmBaseCore:
		return "warm_base_core"
	case plannerOrderedLimitCandidateColdRetainedBaseCore:
		return "cold_retained_base_core"
	case plannerOrderedLimitCandidateColdUnretainedBaseCore:
		return "cold_unretained_base_core"
	case plannerOrderedLimitCandidateCandidateOrder:
		return "candidate_order"
	case plannerOrderedLimitCandidateNumericKeyScan:
		return "numeric_key_scan"
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
	uncappedCost float64
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
		if cap(opsBuf) > plannerLimitFactsRetainExprCap {
			opsBuf = nil
		} else if cap(opsBuf) > 0 {
			clear(opsBuf[:cap(opsBuf)])
			opsBuf = opsBuf[:0]
		}
		if cap(baseOpsBuf) > plannerLimitFactsRetainExprCap {
			baseOpsBuf = nil
		} else if cap(baseOpsBuf) > 0 {
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

func (d plannerOrderedLimitDecision) usesBaseCore() bool { // nolint:unused
	return d.selected.kind.usesBaseCore()
}

func (d plannerOrderedLimitDecision) selectedKind() plannerOrderedLimitCandidateKind {
	return d.selected.kind
}

func (d plannerOrderedLimitDecision) traceRoute() rbitrace.OrderedLimitRoute {
	return rbitrace.OrderedLimitRoute{
		Selected:        d.selected.kind.String(),
		Rejected:        d.rejected.kind.String(),
		CacheState:      d.selected.cacheState.String(),
		SelectedCost:    d.selected.cost,
		RejectedCost:    d.rejected.cost,
		SelectedWork:    d.selected.traceWork(),
		RejectedWork:    d.rejected.traceWork(),
		ExpectedRows:    d.selected.expectedRows,
		OrderBuckets:    d.selected.buckets,
		PredicateChecks: d.selected.checks,
		ExactFilters:    d.selected.exactFilters,
		PostingBuild:    d.selected.buildWork,
	}
}

func (c plannerOrderedLimitCandidate) traceWork() rbitrace.RouteWork {
	if c.cost <= 0 {
		return rbitrace.RouteWork{}
	}
	switch c.kind {
	case plannerOrderedLimitCandidateEmpty:
		return rbitrace.RouteWork{CandidateScan: c.cost}
	case plannerOrderedLimitCandidateMaterializedFallback:
		return rbitrace.RouteWork{MaterializedBuild: c.cost}
	case plannerOrderedLimitCandidateWarmBaseCore,
		plannerOrderedLimitCandidateColdRetainedBaseCore,
		plannerOrderedLimitCandidateColdUnretainedBaseCore:
		scan := float64(c.expectedRows) * (0.60 + float64(c.checks)*0.16)
		build := float64(c.buildWork)
		work := rbitrace.RouteWork{CandidateScan: scan, MaterializedBuild: build}
		total := scan + build
		if total > c.cost {
			work.RetainedCacheBenefit = total - c.cost
		} else if total < c.cost {
			if plannerMaterializedCacheStateUnretained(c.cacheState) {
				work.UnretainedRebuildPenalty = c.cost - total
			} else {
				work.TailRiskPenalty = c.cost - total
			}
		}
		return work
	default:
		scan := float64(c.expectedRows)
		if scan <= 0 {
			scan = c.cost
		}
		if scan > c.cost {
			scan = c.cost
		}
		return rbitrace.RouteWork{CandidateScan: scan, PostingContains: c.cost - scan}
	}
}

func (qv *View) traceOrderedLimitRoute(q *qir.Shape, facts *orderedLimitFacts, d plannerOrderedLimitDecision) rbitrace.OrderedLimitRoute {
	route := d.traceRoute()
	route.SelectedWork = qv.orderedLimitCandidateTraceWork(q, facts, d.selected)
	route.RejectedWork = qv.orderedLimitCandidateTraceWork(q, facts, d.rejected)
	return route
}

func (qv *View) orderedLimitCandidateTraceWork(q *qir.Shape, facts *orderedLimitFacts, c plannerOrderedLimitCandidate) rbitrace.RouteWork {
	if c.cost <= 0 || q == nil || facts == nil {
		return c.traceWork()
	}
	switch c.kind {
	case plannerOrderedLimitCandidateOrderScan, plannerOrderedLimitCandidateOrderedBasic, plannerOrderedLimitCandidateCandidateOrder, plannerOrderedLimitCandidateNumericKeyScan:
		return qv.orderedLimitScanTraceWork(facts, c)
	default:
		return c.traceWork()
	}
}

func (qv *View) orderedLimitScanTraceWork(facts *orderedLimitFacts, c plannerOrderedLimitCandidate) rbitrace.RouteWork {
	scan := float64(c.expectedRows)
	if scan <= 0 {
		scan = c.cost
	}
	if scan > c.cost {
		scan = c.cost
	}
	remaining := c.cost - scan
	work := rbitrace.RouteWork{CandidateScan: scan}
	if remaining <= 0 {
		return work
	}

	exactFilters := 0
	for i := 0; i < len(facts.baseOps); i++ {
		if orderedLimitResidualExactFilterCandidate(facts.baseOps[i]) {
			exactFilters++
		}
	}
	if exactFilters > 0 {
		exact := float64(c.expectedRows) * float64(exactFilters) * 0.35
		if exact > remaining {
			exact = remaining
		}
		work.ExactBucketFilter = exact
		remaining -= exact
	}

	if c.kind == plannerOrderedLimitCandidateOrderScan && c.buckets > c.expectedRows && remaining > 0 {
		probe := float64(c.buckets-c.expectedRows) * 0.0001
		if probe > remaining {
			probe = remaining
		}
		work.RangeProbe = probe
		remaining -= probe
	}

	work.PostingContains = remaining
	return work
}

func (qv *View) collectNumericKeyOrderedLimitFacts(q *qir.Shape, facts *orderedLimitFacts) (bool, error) {
	bounds, residuals, ok, err := qv.numericKeyBoundsAndResiduals(q.Expr, q.Order.FieldOrdinal, facts.baseOpsBuf)
	if err != nil || !ok {
		return ok, err
	}
	facts.bounds = bounds
	facts.baseOpsBuf = residuals
	facts.baseOps = residuals
	return true, nil
}

func (qv *View) supportsNumericKeyOrderedLimitResidual(e qir.Expr) bool {
	if !qv.hasFieldOrdinal(e.FieldOrdinal) {
		return false
	}
	if qv.isNumericKeyOrdinal(e.FieldOrdinal) {
		return !e.Not && len(e.Operands) == 0 && (isScalarRangeEqOp(e.Op) || e.Op == qir.OpIN)
	}
	return qv.supportsLimitLeafPredExpr(e)
}

func (qv *View) numericKeyOrderedLimitResidualsSupported(baseOps []qir.Expr) bool {
	for i := 0; i < len(baseOps); i++ {
		if !qv.supportsNumericKeyOrderedLimitResidual(baseOps[i]) {
			return false
		}
	}
	return true
}

func (qv *View) numericKeyOrderedLimitScanCandidate(facts *orderedLimitFacts) plannerOrderedLimitCandidate {
	expected := uint64(facts.needWindow)
	if universe := qv.snap.Universe.Cardinality(); expected > universe {
		expected = universe
	}
	if facts.bounds.Empty {
		expected = 0
	}
	costRows := expected
	if costRows == 0 {
		costRows = 1
	}
	return plannerOrderedLimitCandidate{
		kind:         plannerOrderedLimitCandidateNumericKeyScan,
		cost:         float64(costRows) * (1.0 + float64(len(facts.baseOps))*0.35),
		expectedRows: expected,
		checks:       uint64(len(facts.baseOps)),
		cacheState:   plannerMaterializedCacheDisabled,
	}
}

func (qv *View) selectNumericKeyOrderedLimit(q *qir.Shape, facts *orderedLimitFacts) (plannerOrderedLimitDecision, bool, error) {
	scan := qv.numericKeyOrderedLimitScanCandidate(facts)
	fallback := orderedLimitMaterializedFallbackCandidate(q, facts.ops, scan.cost, plannerOrderedDecision{})
	if !qv.numericKeyOrderedLimitResidualsSupported(facts.baseOps) {
		return plannerOrderedLimitDecision{
			selected:             fallback,
			materializedFallback: fallback,
			rejected:             scan,
		}, true, nil
	}
	candidates := [...]plannerOrderedLimitCandidate{scan, fallback}
	return plannerOrderedLimitPick(candidates[:]), true, nil
}

func (d plannerOrderedLimitDecision) runtimeGuard(q *qir.Shape) plannerOrderedLimitRuntimeGuard {
	if d.selected.kind != plannerOrderedLimitCandidateOrderScan {
		return plannerOrderedLimitRuntimeGuard{}
	}
	if d.runtimeFallback.kind == plannerOrderedLimitCandidateNone || d.runtimeFallback.cost <= 0 {
		return plannerOrderedLimitRuntimeGuard{}
	}
	if d.runtimeFallback.kind != plannerOrderedLimitCandidateOrderedBasic &&
		d.runtimeFallback.kind != plannerOrderedLimitCandidateMaterializedFallback {
		return plannerOrderedLimitRuntimeGuard{}
	}
	if q.Offset > 0 {
		return plannerOrderedLimitRuntimeGuard{}
	}
	need, ok := orderWindow(q)
	if !ok || need <= 0 {
		return plannerOrderedLimitRuntimeGuard{}
	}
	return plannerOrderedLimitRuntimeGuardForCandidate(d.selected, d.runtimeFallback.cost, need, "bounds_scan_guard", true)
}

func (d plannerOrderedLimitDecision) baseCoreRuntimeGuard(q *qir.Shape, baseOpCount int) plannerOrderedLimitRuntimeGuard {
	if !d.selected.kind.usesBaseCore() || baseOpCount == 0 {
		return plannerOrderedLimitRuntimeGuard{}
	}
	if d.materializedFallback.kind != plannerOrderedLimitCandidateMaterializedFallback || d.materializedFallback.cost <= 0 {
		return plannerOrderedLimitRuntimeGuard{}
	}
	if q.Offset > 0 {
		return plannerOrderedLimitRuntimeGuard{}
	}
	need, ok := orderWindow(q)
	if !ok || need <= 0 {
		return plannerOrderedLimitRuntimeGuard{}
	}
	// The placement cap is only valid when the selected base scan is expected
	// to stay near the requested window; broad scans must keep materialized
	// build cost visible or sparse result ranges fall back to a slower plan.
	capFallback := d.selected.expectedRows <= uint64(need)*128
	return plannerOrderedLimitRuntimeGuardForCandidate(d.selected, d.materializedFallback.cost, need, "base_core_scan_guard", capFallback)
}

func plannerOrderedLimitRuntimeGuardForCandidate(
	selected plannerOrderedLimitCandidate,
	fallbackCost float64,
	need int,
	reason string,
	capFallback bool,
) plannerOrderedLimitRuntimeGuard {
	expected := selected.expectedRows
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
	rowCost := 1.0 + float64(selected.checks)*0.75
	if rowCost < 1 {
		rowCost = 1
	}
	uncappedCost := fallbackCost
	if capFallback {
		placementFallbackCap := float64(need) * (48.0 + float64(selected.checks)*8.0)
		if placementFallbackCap > 0 && fallbackCost > placementFallbackCap {
			fallbackCost = placementFallbackCap
		}
	}
	return plannerOrderedLimitRuntimeGuard{
		enabled:      true,
		minExamined:  minExamined,
		needWindow:   uint64(need),
		fallbackCost: fallbackCost,
		uncappedCost: uncappedCost,
		rowCost:      rowCost,
		reason:       reason,
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

func (g plannerOrderedLimitRuntimeGuard) shouldFallbackFullWindow(examined uint64, emitted int, scanRows uint64, need int) bool {
	if !g.enabled || examined < g.minExamined || examined >= scanRows || g.uncappedCost <= 0 {
		return false
	}
	if emitted > 0 && uint64(emitted)*4 >= uint64(need) {
		return false
	}
	fallbackCost := g.uncappedCost * 1.25
	return float64(examined)*g.rowCost > fallbackCost &&
		float64(scanRows-examined)*g.rowCost > fallbackCost
}

type plannerOrderedLimitBaseCoreStats struct {
	cacheKey   qcache.MaterializedPredKey
	est        uint64
	buildWork  uint64
	probeWork  uint64
	routeWork  uint64
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
	if qv.snap.AllowsMaterializedPredCard(est) {
		return plannerMaterializedCacheColdRegularAdmissible
	}
	if qv.snap.MaterializedPredCacheOversizedLimit() > 0 {
		return plannerMaterializedCacheColdOversizedAdmissible
	}
	return plannerMaterializedCacheColdUnretainedByCardinality
}

func plannerMaterializedCacheStateRetained(s plannerMaterializedCacheState) bool {
	return s == plannerMaterializedCacheWarmHit ||
		s == plannerMaterializedCacheColdRegularAdmissible ||
		s == plannerMaterializedCacheColdOversizedAdmissible
}

func plannerMaterializedCacheStateUnretained(s plannerMaterializedCacheState) bool {
	return s == plannerMaterializedCacheColdUnretainedByCardinality ||
		s == plannerMaterializedCacheColdUnretainedByPolicy
}

const plannerMaterializedCacheRouteKeyMax = plannerORBranchLimit * plannerPredicateFastPathMaxLeaves

type plannerMaterializedCacheRouteSet struct {
	keys           [plannerMaterializedCacheRouteKeyMax]qcache.MaterializedPredKey
	limit          int
	oversizedLimit int32
	maxCard        uint64
	keyCount       int
	warmCount      int
	oversizedCount int32
	overflow       bool
	state          plannerMaterializedCacheState
}

func (set *plannerMaterializedCacheRouteSet) init(qv *View) {
	*set = plannerMaterializedCacheRouteSet{state: plannerMaterializedCacheDisabled}
	if qv == nil || qv.snap == nil {
		return
	}
	set.limit = qv.snap.MaterializedPredCacheLimit()
	if set.limit <= 0 {
		return
	}
	set.oversizedLimit = qv.snap.MaterializedPredCacheOversizedLimit()
	set.maxCard = qv.snap.MaterializedPredCacheMaxCardinality()
}

func (set *plannerMaterializedCacheRouteSet) add(key qcache.MaterializedPredKey, est uint64, state plannerMaterializedCacheState) bool {
	set.state = plannerOrderedLimitCacheGroupState(set.state, state)
	if key.IsZero() || state == plannerMaterializedCacheDisabled {
		return false
	}
	for i := 0; i < set.keyCount; i++ {
		if set.keys[i] == key {
			return false
		}
	}
	if set.keyCount == len(set.keys) {
		set.overflow = true
		return true
	}
	set.keys[set.keyCount] = key
	set.keyCount++
	if state == plannerMaterializedCacheWarmHit {
		set.warmCount++
	}
	if set.maxCard > 0 && est > set.maxCard {
		set.oversizedCount++
	}
	return true
}

func (set *plannerMaterializedCacheRouteSet) finish() plannerMaterializedCacheState {
	if set.limit <= 0 || set.state == plannerMaterializedCacheDisabled {
		return plannerMaterializedCacheDisabled
	}
	if set.overflow || set.keyCount > set.limit || set.oversizedCount > set.oversizedLimit {
		return plannerMaterializedCacheColdUnretainedByPolicy
	}
	return set.state
}

func (set *plannerMaterializedCacheRouteSet) allWarm() bool {
	return !set.overflow && set.keyCount > 0 && set.keyCount == set.warmCount
}

func (set *plannerMaterializedCacheRouteSet) pressurePenalty() float64 {
	if set.limit <= 0 || set.keyCount == 0 || !plannerMaterializedCacheStateUnretained(set.finish()) {
		return 1
	}
	keyCount := set.keyCount
	if set.overflow {
		keyCount++
	}
	penalty := 1.0 + float64(keyCount)/float64(set.limit)
	if set.oversizedLimit >= 0 && set.oversizedCount > set.oversizedLimit {
		penalty += float64(set.oversizedCount-set.oversizedLimit) * 0.5
	}
	if set.overflow {
		penalty += 1
	}
	if penalty > 8 {
		return 8
	}
	return penalty
}

func (qv *View) orderedLimitCollapsedRangeStats(op preparedScalarExactRange) (scalarMaterializationStats, bool) {
	ov := qv.indexViewByOrdinal(op.fieldOrdinal)
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
	emptyComplement := stats.buildComplement && stats.buildEst == 0
	if !ok || (!emptyComplement && (stats.buildBuckets == 0 || stats.buildEst == 0)) {
		return plannerOrderedLimitBaseCoreStats{}, false
	}

	buildWork := rangeProbeMaterializeWork(stats.buildBuckets, stats.buildEst)
	if buildWork == 0 {
		return plannerOrderedLimitBaseCoreStats{}, false
	}

	expectedRows := orderedPredicateExpectedRows(needWindow, stats.probeEst, universe)
	probeWork := rangeProbeTotalWorkForRows(clampUint64ToInt(expectedRows), stats.probeBuckets, stats.probeEst)
	retainedPenalty := mathutil.SatMulUint64(stats.buildEst, postingContainsLookupWork(stats.buildEst))
	routeWork := qv.snap.ObservedMaterializedPredWork(stats.cacheKey)
	if routeWork >= buildWork {
		requiresSecondHit = false
		if routeWork > probeWork {
			probeWork = routeWork
		}
	}

	useful := probeWork >= mathutil.SatAddUint64(buildWork, retainedPenalty)
	if !useful && hasOrderBounds {
		minRows := mathutil.SatMulUint64(uint64(needWindow), orderBasicBoundedRangeBaseMinRowsPerNeed)
		useful = stats.probeBuckets <= rangePostingFilterKeepProbeMaxBuckets && stats.probeEst >= minRows
	}
	if routeWork >= buildWork {
		useful = true
	}

	cacheState := qv.classifyPlannerMaterializedCacheKey(stats.cacheKey, stats.buildEst, requiresSecondHit)
	return plannerOrderedLimitBaseCoreStats{
		cacheKey:   stats.cacheKey,
		est:        stats.buildEst,
		buildWork:  buildWork,
		probeWork:  probeWork,
		routeWork:  routeWork,
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
		expectedRows := uint64(needWindow)
		if br.BaseStart != 0 || br.BaseEnd != ov.KeyCount() {
			_, rows := ov.RangeStats(br)
			if rows > 0 && expectedRows > rows {
				expectedRows = rows
			}
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

	var orderRows uint64
	if br.BaseStart == 0 && br.BaseEnd == ov.KeyCount() {
		snap := qv.exec.Stats.Load()
		stats := qv.plannerOrderFieldStats(
			qv.exec.FieldNameByOrdinal(q.Order.FieldOrdinal),
			snap,
			qv.plannerUniverseCardinality(snap),
			uint64(ov.KeyCount()),
		)
		orderRows = stats.TotalBucketCard
	} else {
		_, orderRows = ov.RangeStats(br)
	}
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

func orderedLimitBasicCandidate(decision plannerOrderedDecision, leaves []qir.Expr, orderScan plannerOrderedLimitCandidate) (plannerOrderedLimitCandidate, bool) {
	if !decision.use || decision.orderedCost <= 0 {
		return plannerOrderedLimitCandidate{}, false
	}
	expectedRows := decision.expectedProbeRows
	if expectedRows == 0 {
		expectedRows = orderScan.expectedRows
	}
	return plannerOrderedLimitCandidate{
		kind:         plannerOrderedLimitCandidateOrderedBasic,
		cost:         decision.orderedCost,
		expectedRows: expectedRows,
		buckets:      orderScan.buckets,
		checks:       uint64(len(leaves)),
		cacheState:   plannerMaterializedCacheDisabled,
	}, true
}

func orderedLimitResidualExactFilterCandidate(e qir.Expr) bool {
	switch e.Op {
	case qir.OpEQ, qir.OpIN, qir.OpHASALL, qir.OpHASANY:
		return true
	default:
		return false
	}
}

func (qv *View) orderedLimitScalarRangeHasCheapBucketFilter(
	e qir.Expr,
	bounds indexdata.Bounds,
	useBounds bool,
	needWindow int,
	universe uint64,
) bool {
	var plan preparedFieldIndexRangePredicatePlan
	useRuntimeComplement := false
	if useBounds {
		fm := qv.fieldMetaByOrdinal(e.FieldOrdinal)
		if fm == nil || fm.Slice {
			return false
		}
		var core preparedScalarRangePredicate
		qv.initPreparedExactScalarRangePredicate(&core, e, fm, bounds)
		ov := qv.indexViewByOrdinal(e.FieldOrdinal)
		if !ov.HasData() {
			return false
		}
		var done bool
		plan, _, done = core.planFieldIndexRange(ov)
		if done {
			return false
		}
		useRuntimeComplement = core.usesRuntimeComplement(plan.useComplement)
	} else {
		candidate, ok := qv.prepareScalarRangeRoutingCandidate(e)
		if !ok {
			return false
		}
		plan = candidate.plan
		useRuntimeComplement = candidate.core.usesRuntimeComplement(plan.useComplement)
	}
	if plan.est == 0 {
		return false
	}
	if plan.orderedEagerMaterializeUseful(needWindow, universe) {
		return true
	}
	return rangeProbeSupportsCheapPostingFilter(useRuntimeComplement == e.Not, plan.runtimeProbeBuckets)
}

func (qv *View) orderedLimitBoundsScanCandidate(
	q *qir.Shape,
	facts *orderedLimitFacts,
	orderScan plannerOrderedLimitCandidate,
) (plannerOrderedLimitCandidate, bool) {
	if len(facts.baseOps) == 0 {
		return orderScan, true
	}
	if q.Offset > 0 && schema.FieldUsesNilIndex(facts.orderMeta) && facts.nilTailField == "" {
		return orderScan, false
	}
	maxResiduals := 3
	if len(facts.baseOps) > maxResiduals {
		return orderScan, false
	}

	snap := qv.exec.Stats.Load()
	universe := qv.plannerUniverseCardinality(snap)
	if universe == 0 {
		return orderScan, false
	}

	if q.Offset > 0 {
		residualSel := 1.0
		exactFilters := 0
		for i := 0; i < len(facts.baseOps); i++ {
			e := facts.baseOps[i]
			sel, _, _, _, ok := qv.estimateLeafOrderCost(e, snap, universe, facts.orderField, facts.ov.HasData())
			if !ok {
				return orderScan, false
			}
			if sel <= 0 {
				return orderScan, true
			}
			residualSel *= sel
			switch e.Op {
			case qir.OpEQ, qir.OpIN, qir.OpHASALL, qir.OpHASANY,
				qir.OpGT, qir.OpGTE, qir.OpLT, qir.OpLTE, qir.OpPREFIX:
				exactFilters++
			}
		}
		if exactFilters == 0 {
			return orderScan, false
		}
		residualRows := uint64(residualSel * float64(universe))
		if residualRows == 0 {
			return orderScan, false
		}
		need := uint64(facts.needWindow)
		if need > residualRows/2 {
			return orderScan, false
		}
		orderScan.exactFilters = uint64(exactFilters)
		return orderScan, true
	}

	residualSel := 1.0
	exactFilters := 0
	residualChecks := 0
	rangeResiduals := 0
	broadRangeResiduals := 0
	cheapRangeFilters := 0
	var mergedRangesArr [plannerOrderedLeafMax]orderedMergedScalarRangeField
	mergedRanges, mergeOK := qv.collectOrderedMergedScalarRangeFields(facts.orderField, facts.baseOps, mergedRangesArr[:0])
	if !mergeOK {
		return orderScan, false
	}
	for i := 0; i < len(facts.baseOps); i++ {
		e := facts.baseOps[i]
		if !e.Not && qv.hasFieldOrdinal(e.FieldOrdinal) && len(e.Operands) == 0 && isScalarRangeEqOp(e.Op) {
			fieldName := qv.exec.FieldNameByOrdinal(e.FieldOrdinal)
			if fieldName != facts.orderField {
				idx := findOrderedMergedScalarRangeField(mergedRanges, fieldName)
				if idx >= 0 {
					merged := mergedRanges[idx]
					if merged.count > 1 {
						if merged.first != i {
							continue
						}
						fieldStats := qv.plannerFieldStats(merged.field, snap, universe)
						mergedSel, _, ok := qv.estimateMergedScalarRangeOrderCost(merged.field, merged.bounds, universe, fieldStats)
						if !ok {
							return orderScan, false
						}
						if mergedSel <= 0 {
							return orderScan, true
						}
						residualSel *= mergedSel
						rangeResiduals++
						if qv.orderedLimitScalarRangeHasCheapBucketFilter(merged.expr, merged.bounds, true, facts.needWindow, universe) {
							exactFilters++
							cheapRangeFilters++
							continue
						}
						residualChecks++
						broadRangeResiduals++
						continue
					}
				}
			}
		}

		sel, _, _, _, ok := qv.estimateLeafOrderCost(e, snap, universe, facts.orderField, facts.ov.HasData())
		if !ok {
			return orderScan, false
		}
		if sel <= 0 {
			return orderScan, true
		}
		residualSel *= sel

		switch e.Op {

		case qir.OpEQ, qir.OpIN, qir.OpHASALL, qir.OpHASANY:
			if !e.Not {
				exactFilters++
				continue
			}

		case qir.OpGT, qir.OpGTE, qir.OpLT, qir.OpLTE:
			rangeResiduals++
			if !e.Not && qv.orderedLimitScalarRangeHasCheapBucketFilter(e, indexdata.Bounds{}, false, facts.needWindow, universe) {
				exactFilters++
				cheapRangeFilters++
				continue
			}
			if !e.Not {
				broadRangeResiduals++
			}

		case qir.OpPREFIX:
			rangeResiduals++
			prefix, ok, err := qv.prepareScalarPrefixRoute(e)
			if !e.Not && err == nil && ok && prefix.hasData && !prefix.br.Empty() && prefix.br.Len() <= rangePostingFilterKeepProbeMaxBuckets {
				exactFilters++
				cheapRangeFilters++
				continue
			}
			if !e.Not {
				broadRangeResiduals++
			}
		}
		residualChecks++
	}
	orderScan.checks = uint64(exactFilters + residualChecks)
	orderScan.exactFilters = uint64(exactFilters)

	if residualChecks > 0 {
		expectedRows := orderScan.expectedRows
		need := uint64(facts.needWindow)
		if expectedRows < need {
			expectedRows = need
		}

		rowsForContains := float64(expectedRows)
		if exactFilters > 0 {
			rowsForContains *= residualSel
			if rowsForContains < float64(need) {
				rowsForContains = float64(need)
			}
		}

		orderStats := qv.plannerOrderFieldStats(facts.orderField, snap, universe, uint64(facts.ov.KeyCount()))
		bucketAmp := 1.0
		if facts.needWindow > 0 && orderStats.AvgBucketCard > float64(facts.needWindow) {
			bucketAmp += ClampFloat((orderStats.AvgBucketCard/float64(facts.needWindow)-1.0)*0.18, 0, 2.0)
		}
		if orderSkew := plannerFieldStatsSkew(orderStats); orderSkew > 1 {
			bucketAmp += (orderSkew - 1) * 0.12
		}

		residualCost := rowsForContains * float64(residualChecks) * bucketAmp
		if rangeResiduals == 0 {
			residualCost = 0
		} else {
			residualCost *= 1.0 + float64(rangeResiduals)*0.45
		}
		if broadRangeResiduals > 0 && (facts.bounds.HasLo || facts.bounds.HasHi || facts.bounds.HasPrefix) {
			residualCost *= 1.0 + float64(broadRangeResiduals)*0.55
		}
		exactCost := float64(expectedRows) * float64(exactFilters) * 0.22
		if cheapRangeFilters > 0 {
			exactCost += float64(expectedRows) * float64(cheapRangeFilters) * 0.18
		}

		charged := float64(expectedRows) + exactCost + residualCost
		if charged > orderScan.cost {
			orderScan.cost = charged
		}
	}
	if exactFilters == 0 {
		return orderScan, false
	}
	if residualChecks == 0 &&
		rangeResiduals == 0 &&
		(facts.bounds.HasLo || facts.bounds.HasHi || facts.bounds.HasPrefix) {
		return orderScan, true
	}

	residualRows := uint64(residualSel * float64(universe))
	if residualRows == 0 {
		return orderScan, false
	}
	need := uint64(facts.needWindow)
	if q.Offset > 0 && need > residualRows/2 {
		return orderScan, false
	}
	if q.Offset > 0 {
		return orderScan, true
	}
	expectedRows := orderScan.expectedRows
	if expectedRows == 0 {
		expectedRows = need
	}
	if broadRangeResiduals > 0 &&
		(facts.bounds.HasLo || facts.bounds.HasHi || facts.bounds.HasPrefix) &&
		exactFilters == cheapRangeFilters {
		return orderScan, false
	}
	if len(facts.baseOps) >= 3 &&
		(facts.bounds.HasLo || facts.bounds.HasHi || facts.bounds.HasPrefix) &&
		expectedRows/32 > need {
		return orderScan, false
	}
	if len(facts.baseOps) == 1 {
		return orderScan, expectedRows <= residualRows
	}
	return orderScan, expectedRows <= residualRows/2
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

	limit := qv.snap.MaterializedPredCacheLimit()
	maxCard := qv.snap.MaterializedPredCacheMaxCardinality()

	var (
		cacheState plannerMaterializedCacheState
		buildWork  uint64
		probeWork  uint64
		routeWork  uint64
		hasWarm    bool
		useful     bool
		coreCount  int
	)

	var routeSet plannerMaterializedCacheRouteSet
	routeSet.init(qv)

	for i, op := range baseOps {
		core := orderBasicBaseCore{
			kind: orderBasicBaseCoreRawExpr,
			expr: op,
		}

		fm := qv.fieldMetaByOrdinal(op.FieldOrdinal)
		if qv.isOrderBasicCollapsibleScalarRangeExpr(op, fm) {
			fieldName := qv.exec.FieldNameByOrdinal(op.FieldOrdinal)
			var rb indexdata.Bounds
			groupCount := 0
			first := true
			for j := 0; j < len(baseOps); j++ {
				other := baseOps[j]
				if other.FieldOrdinal != op.FieldOrdinal || !qv.isOrderBasicCollapsibleScalarRangeExpr(other, fm) {
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
							field:        fieldName,
							fieldOrdinal: op.FieldOrdinal,
							bounds:       rb,
							cacheKey:     qv.materializedPredKeyForExactScalarRange(fieldName, rb),
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
			hasWarm = true
		} else {
			buildWork = mathutil.SatAddUint64(buildWork, st.buildWork)
		}
		probeWork = mathutil.SatAddUint64(probeWork, st.probeWork)
		if !st.warm && st.routeWork >= st.buildWork {
			routeWork = mathutil.SatAddUint64(routeWork, st.routeWork)
		}
		if st.useful {
			useful = true
		}

		state := st.cacheState
		routeSet.add(st.cacheKey, st.est, state)
	}

	placementFallback := false
	if !hasWarm && !useful {
		if hasOffset || orderCandidate.cost <= 0 || buildWork == 0 {
			return plannerOrderedLimitCandidate{}, false, false, nil
		}
		placementFallback = true
	}
	if limit <= 0 {
		cacheState = plannerMaterializedCacheDisabled
	} else {
		cacheState = routeSet.finish()
	}

	expectedRows := orderCandidate.expectedRows
	if expectedRows == 0 {
		expectedRows = uint64(needWindow)
	}
	baseCost := float64(expectedRows)*(0.60+float64(coreCount)*0.16) + plannerOrderedLimitCost(buildWork)
	if hasWarm {
		baseCost *= 0.82
	}
	if !plannerMaterializedCacheStateRetained(cacheState) {
		baseCost += plannerOrderedLimitCost(buildWork) * 0.35
	}
	if routeWork >= buildWork && buildWork > 0 && orderCandidate.cost > 0 && plannerMaterializedCacheStateRetained(cacheState) {
		buildCost := orderCandidate.cost * float64(buildWork) / float64(routeWork)
		if buildCost < baseCost {
			baseCost = buildCost
		}
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
		kind:         plannerOrderedLimitBaseKind(routeSet.allWarm(), cacheState),
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
	if d.selected.kind == plannerOrderedLimitCandidateOrderScan {
		d.runtimeFallback = plannerOrderedLimitCandidate{}
		for i := range candidates {
			if candidates[i].kind != plannerOrderedLimitCandidateOrderedBasic &&
				candidates[i].kind != plannerOrderedLimitCandidateMaterializedFallback {
				continue
			}
			if d.runtimeFallback.kind == plannerOrderedLimitCandidateNone || candidates[i].cost < d.runtimeFallback.cost {
				d.runtimeFallback = candidates[i]
			}
		}
	}
	return d
}

func (qv *View) collectOrderedLimitFacts(q *qir.Shape, facts *orderedLimitFacts) (bool, error) {
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

	fm := qv.fieldMetaByOrdinal(order.FieldOrdinal)
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

	if qv.isNumericKeyOrdinal(order.FieldOrdinal) {
		return qv.collectNumericKeyOrderedLimitFacts(q, facts)
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
	if len(facts.baseOps) == 2 {
		prefixExpr, merged, err := qv.mergeAdjacentStringPrefixLeaves(facts.orderField, facts.baseOps[0], facts.baseOps[1])
		if err != nil {
			return true, err
		}
		if merged {
			if orderFirst < 0 {
				facts.opsBuf = append(facts.opsBuf[:0], prefixExpr)
				facts.ops = facts.opsBuf
				facts.baseOps = facts.ops
			} else {
				facts.baseOpsBuf = append(facts.baseOpsBuf[:0], prefixExpr)
				facts.baseOps = facts.baseOpsBuf
			}
		}
	}

	if orderEqNilConflict {
		facts.empty = true
		facts.validateBase = true
		return true, nil
	}

	facts.nilTailField = orderNilTailField(fm, facts.orderField, rb)
	if orderEqNil {
		if !schema.FieldUsesNilIndex(fm) {
			facts.empty = true
			facts.validateBase = true
			return true, nil
		}
		facts.nilTailField = facts.orderField
	}

	facts.ov = qv.indexViewByOrdinal(order.FieldOrdinal)
	if !facts.ov.HasData() && facts.nilTailField == "" {
		if !qv.hasIndexedFieldOrdinal(order.FieldOrdinal) {
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
	if qv.isNumericKeyOrdinal(facts.order.FieldOrdinal) {
		return qv.selectNumericKeyOrderedLimit(q, facts)
	}

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

	if q.Offset == 0 && facts.bounds.HasPrefix && len(facts.baseOps) > 0 && len(facts.baseOps) <= 2 {
		fastPrefix := true
		for i := 0; i < len(facts.baseOps); i++ {
			e := facts.baseOps[i]
			if e.Not || !qv.hasFieldOrdinal(e.FieldOrdinal) || len(e.Operands) != 0 || !isScalarEqOrInOp(e.Op) {
				fastPrefix = false
				break
			}
			fm := qv.fieldMetaByOrdinal(e.FieldOrdinal)
			if fm == nil || fm.Slice {
				fastPrefix = false
				break
			}
		}
		if fastPrefix {
			buckets := uint64(0)
			if !facts.br.Empty() && facts.br.BaseEnd >= facts.br.BaseStart {
				buckets = uint64(facts.br.BaseEnd - facts.br.BaseStart)
			}
			orderScan := plannerOrderedLimitCandidate{
				kind:         plannerOrderedLimitCandidateOrderScan,
				cost:         float64(facts.needWindow) * (1.0 + float64(len(facts.baseOps))*0.35),
				expectedRows: uint64(facts.needWindow),
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

	baseSupported := true
	emptySupported := false
	if len(facts.baseOps) != 0 {
		snap := qv.exec.Stats.Load()
		universe := qv.plannerUniverseCardinality(snap)
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
	}
	if emptySupported && baseSupported {
		return plannerOrderedLimitDecision{
			selected: plannerOrderedLimitCandidate{
				kind: plannerOrderedLimitCandidateEmpty,
				cost: 1,
			},
		}, true, nil
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

	basic, hasBasic := orderedLimitBasicCandidate(orderedDecision, facts.ops, orderScan)
	orderScan, boundsScanAllowed := qv.orderedLimitBoundsScanCandidate(q, facts, orderScan)
	orderCandidate := orderScan
	useCandidateOrder := qv.shouldUseCandidateOrder(facts.order, facts.ops)
	if boundsScanAllowed {
		candidates[n] = orderScan
		n++
	}
	if hasBasic {
		candidates[n] = basic
		n++
		if !boundsScanAllowed && basic.cost > 0 && (orderCandidate.cost <= 0 || basic.cost < orderCandidate.cost) {
			orderCandidate = basic
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

	if useCandidateOrder {
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

	materialized := orderedLimitMaterializedFallbackCandidate(q, facts.ops, orderScan.cost, orderedDecision)
	candidates[n] = materialized
	n++

	return plannerOrderedLimitPick(candidates[:n]), true, nil
}
