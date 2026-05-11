package rbi

import (
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/schema"
)

type preparedScalarRangePredicate struct {
	qv                 *queryView
	expr               qir.Expr
	fm                 *schema.Field
	bound              normalizedScalarBound
	bounds             indexdata.Bounds
	complementCacheKey materializedPredKey
	loadReuse          materializedPredReuse
	sharedReuse        materializedPredReuse
	secondHitReuse     materializedPredReuse
	usePostingFilter   bool
}

type preparedOverlayRangePredicatePlan struct {
	br                  indexdata.OverlayRange
	bucketCount         int
	est                 uint64
	useComplement       bool
	runtimeProbeBuckets int
	runtimeProbeEst     uint64
}

type scalarMaterializationStats struct {
	cacheKey        materializedPredKey
	probeBuckets    int
	probeEst        uint64
	buildBuckets    int
	buildEst        uint64
	buildComplement bool
}

type scalarComplementMaterializationPlan struct {
	sharedReuse materializedPredReuse
	before      indexdata.OverlayRange
	after       indexdata.OverlayRange
	nilPosting  posting.List
	buckets     int
	est         uint64
}

type preparedScalarExactRange struct {
	field    string
	bounds   indexdata.Bounds
	cacheKey materializedPredKey
}

type orderFieldScalarLeafKind uint8

const (
	orderFieldScalarLeafOther orderFieldScalarLeafKind = iota
	orderFieldScalarLeafRange
	orderFieldScalarLeafPrefix
	orderFieldScalarLeafInvalid
)

type preparedScalarRangeRoutingCandidate struct {
	core    preparedScalarRangePredicate
	plan    preparedOverlayRangePredicatePlan
	numeric bool
}

type orderedMergedScalarRangeField struct {
	field  string
	expr   qir.Expr
	bounds indexdata.Bounds
	first  int
	count  int
}

type preparedScalarPrefixSpan struct {
	prefix  string
	ov      indexdata.FieldOverlay
	br      indexdata.OverlayRange
	hasData bool
	span    int
	rows    uint64
}

type preparedScalarOverlaySpan struct {
	ov      indexdata.FieldOverlay
	br      indexdata.OverlayRange
	hasData bool
}

func storeEmptyScalarComplementMaterialization(plan scalarComplementMaterializationPlan) {
	if plan.sharedReuse.snap == nil || plan.sharedReuse.cacheKey.isZero() {
		return
	}
	plan.sharedReuse.snap.storeMaterializedPredKey(plan.sharedReuse.cacheKey, posting.List{})
}

func (qv *queryView) classifyOrderFieldScalarLeaf(orderField string, e qir.Expr) orderFieldScalarLeafKind {
	if e.FieldOrdinal < 0 || qv.engine.fieldNameByOrdinal(e.FieldOrdinal) != orderField {
		return orderFieldScalarLeafOther
	}
	if e.Not {
		return orderFieldScalarLeafInvalid
	}
	if isScalarRangeEqOp(e.Op) {
		return orderFieldScalarLeafRange
	}
	if e.Op == qir.OpPREFIX {
		return orderFieldScalarLeafPrefix
	}
	return orderFieldScalarLeafInvalid
}

func isNumericRangeOp(op qir.Op) bool {
	switch op {
	case qir.OpGT, qir.OpGTE, qir.OpLT, qir.OpLTE:
		return true
	default:
		return false
	}
}

func isScalarEqOp(op qir.Op) bool {
	return op == qir.OpEQ
}

func isScalarEqOrInOp(op qir.Op) bool {
	return op == qir.OpEQ || op == qir.OpIN
}

func isScalarRangeEqOp(op qir.Op) bool {
	return isScalarEqOp(op) || isNumericRangeOp(op)
}

func isPositiveScalarEqLeaf(e qir.Expr) bool {
	return !e.Not && e.FieldOrdinal >= 0 && len(e.Operands) == 0 && isScalarEqOp(e.Op)
}

func allPositiveScalarEqLeaves(leaves []qir.Expr) bool {
	if len(leaves) == 0 {
		return false
	}
	for _, e := range leaves {
		if !isPositiveScalarEqLeaf(e) {
			return false
		}
	}
	return true
}

func isPositiveScalarPrefixLeaf(e qir.Expr) bool {
	return !e.Not && e.FieldOrdinal >= 0 && len(e.Operands) == 0 && e.Op == qir.OpPREFIX
}

func (qv *queryView) isPositiveNonOrderScalarPrefixLeaf(orderField string, e qir.Expr) bool {
	return qv.engine.fieldNameByOrdinal(e.FieldOrdinal) != orderField && isPositiveScalarPrefixLeaf(e)
}

func (qv *queryView) isPositiveMergedNumericRangeLeaf(e qir.Expr) bool {
	if e.Not || e.FieldOrdinal < 0 || !isScalarRangeEqOp(e.Op) {
		return false
	}
	fm := qv.fieldMetaByExpr(e)
	return schema.FieldUsesOrderedNumericKeys(fm)
}

func isSimpleScalarRangeOrPrefixLeaf(e qir.Expr) bool {
	if e.Not || e.FieldOrdinal < 0 {
		return false
	}
	return isScalarRangeOrPrefixOp(e.Op)
}

func isScalarRangeOrPrefixOp(op qir.Op) bool {
	if isNumericRangeOp(op) || op == qir.OpPREFIX {
		return true
	}
	return false
}

func isMaterializedScalarCacheOp(op qir.Op) bool {
	switch op {
	case qir.OpSUFFIX, qir.OpCONTAINS:
		return true
	default:
		return isScalarRangeOrPrefixOp(op)
	}
}

func (qv *queryView) normalizedScalarBoundForExpr(e qir.Expr) (normalizedScalarBound, bool, error) {
	return qv.exprValueToNormalizedScalarBound(qir.Expr{
		Op:           e.Op,
		FieldOrdinal: e.FieldOrdinal,
		Value:        e.Value,
	})
}

func rangeBoundsForNormalizedScalarBound(bound normalizedScalarBound) indexdata.Bounds {
	rb := indexdata.Bounds{Has: true}
	applyNormalizedScalarBound(&rb, bound)
	return rb
}

func (qv *queryView) initPreparedScalarRangePredicateFromBound(
	core *preparedScalarRangePredicate,
	e qir.Expr,
	fm *schema.Field,
	bound normalizedScalarBound,
) {
	cacheKey := materializedPredKey{}
	complementCacheKey := materializedPredKey{}
	fieldName := qv.engine.fieldNameByOrdinal(e.FieldOrdinal)
	if !bound.full {
		cacheKey = qv.materializedPredKeyForNormalizedScalarBound(fieldName, bound)
		complementCacheKey = qv.materializedPredComplementKeyForNormalizedScalarBound(fieldName, bound)
	}
	loadReuse := newMaterializedPredReadOnlyReuse(qv.snap, cacheKey)
	sharedReuse := newMaterializedPredSharedReuse(qv.snap, cacheKey)
	secondHitReuse := newMaterializedPredSecondHitSharedReuse(qv.snap, cacheKey)
	*core = preparedScalarRangePredicate{
		qv:                 qv,
		expr:               e,
		fm:                 fm,
		bound:              bound,
		bounds:             rangeBoundsForNormalizedScalarBound(bound),
		complementCacheKey: complementCacheKey,
		loadReuse:          loadReuse,
		sharedReuse:        sharedReuse,
		secondHitReuse:     secondHitReuse,
		usePostingFilter:   shouldUseNumericRangePostingFilter(e, fm),
	}
}

func (qv *queryView) initPreparedExactScalarRangePredicate(
	core *preparedScalarRangePredicate,
	e qir.Expr,
	fm *schema.Field,
	bounds indexdata.Bounds,
) {
	cacheKey := qv.materializedPredKeyForExactScalarRange(qv.engine.fieldNameByOrdinal(e.FieldOrdinal), bounds)
	complementCacheKey := materializedPredKey{}
	if schema.FieldUsesOrderedNumericKeys(fm) && isNumericRangeOp(e.Op) {
		complementCacheKey = qv.materializedPredComplementKeyForExactScalarRange(qv.engine.fieldNameByOrdinal(e.FieldOrdinal), bounds)
	}
	loadReuse := newMaterializedPredReadOnlyReuse(qv.snap, cacheKey)
	sharedReuse := newMaterializedPredSharedReuse(qv.snap, cacheKey)
	secondHitReuse := newMaterializedPredSecondHitSharedReuse(qv.snap, cacheKey)
	*core = preparedScalarRangePredicate{
		qv:                 qv,
		expr:               e,
		fm:                 fm,
		bounds:             bounds,
		complementCacheKey: complementCacheKey,
		loadReuse:          loadReuse,
		sharedReuse:        sharedReuse,
		secondHitReuse:     secondHitReuse,
		usePostingFilter:   schema.FieldUsesOrderedNumericKeys(fm),
	}
}

func (qv *queryView) initPreparedScalarRangePredicate(
	core *preparedScalarRangePredicate,
	e qir.Expr,
	fm *schema.Field,
) (predicate, bool, bool) {
	if fm == nil || fm.Slice {
		return predicate{}, false, false
	}

	bound, isSlice, err := qv.normalizedScalarBoundForExpr(e)
	if err != nil || isSlice {
		return predicate{}, false, false
	}
	if bound.empty {
		if e.Not {
			return predicate{expr: e, alwaysTrue: true}, true, true
		}
		return predicate{expr: e, alwaysFalse: true}, true, true
	}

	qv.initPreparedScalarRangePredicateFromBound(core, e, fm, bound)
	return predicate{}, false, true
}

func (qv *queryView) prepareScalarRangeRoutingCandidate(
	e qir.Expr,
) (preparedScalarRangeRoutingCandidate, bool) {
	if e.Not || e.FieldOrdinal < 0 {
		return preparedScalarRangeRoutingCandidate{}, false
	}
	fm := qv.fieldMetaByExpr(e)
	if fm == nil || fm.Slice {
		return preparedScalarRangeRoutingCandidate{}, false
	}
	var core preparedScalarRangePredicate
	_, done, ok := qv.initPreparedScalarRangePredicate(&core, e, fm)
	if !ok || done {
		return preparedScalarRangeRoutingCandidate{}, false
	}
	ov := qv.fieldOverlayForExpr(e)
	if !ov.HasData() {
		return preparedScalarRangeRoutingCandidate{}, false
	}
	plan, _, done := core.planOverlay(ov)
	if done {
		return preparedScalarRangeRoutingCandidate{}, false
	}
	return preparedScalarRangeRoutingCandidate{
		core:    core,
		plan:    plan,
		numeric: schema.FieldUsesOrderedNumericKeys(fm),
	}, true
}

func (qv *queryView) preparePredicateScalarRangeRoutingCandidate(
	p predicate,
) (preparedScalarRangeRoutingCandidate, bool) {
	if p.expr.Not || p.expr.FieldOrdinal < 0 {
		return preparedScalarRangeRoutingCandidate{}, false
	}
	fm := qv.fieldMetaByExpr(p.expr)
	if fm == nil || fm.Slice {
		return preparedScalarRangeRoutingCandidate{}, false
	}
	if !p.hasEffectiveBounds {
		return qv.prepareScalarRangeRoutingCandidate(p.expr)
	}
	ov := qv.fieldOverlayForExpr(p.expr)
	if !ov.HasData() {
		return preparedScalarRangeRoutingCandidate{}, false
	}
	var core preparedScalarRangePredicate
	qv.initPreparedExactScalarRangePredicate(&core, p.expr, fm, p.effectiveBounds)
	plan, _, done := core.planOverlay(ov)
	if done {
		return preparedScalarRangeRoutingCandidate{}, false
	}
	return preparedScalarRangeRoutingCandidate{
		core:    core,
		plan:    plan,
		numeric: schema.FieldUsesOrderedNumericKeys(fm),
	}, true
}

func (candidate preparedScalarRangeRoutingCandidate) broadComplementCardinality(universe uint64) bool {
	return universe > 0 &&
		candidate.plan.est > 0 &&
		candidate.plan.est < universe &&
		candidate.plan.est > universe-candidate.plan.est
}

func (candidate preparedScalarRangeRoutingCandidate) shouldPreferPositiveMaterializationForNullableComplement(
	plan scalarComplementMaterializationPlan,
) bool {
	return !plan.nilPosting.IsEmpty() &&
		candidate.plan.est > 0 &&
		plan.est > candidate.plan.est
}

func (core *preparedScalarRangePredicate) orderedEagerMaterializeUseful(orderedWindow int) bool {
	if orderedWindow <= 0 {
		return false
	}
	universe := core.qv.snapshotUniverseCardinality()
	if universe == 0 {
		return false
	}
	ov := core.qv.fieldOverlayForExpr(core.expr)
	if !ov.HasData() {
		return false
	}
	plan, _, done := core.planOverlay(ov)
	if done {
		return false
	}
	return plan.orderedEagerMaterializeUseful(orderedWindow, universe)
}

func (qv *queryView) prepareScalarOverlaySpan(e qir.Expr) (preparedScalarOverlaySpan, bool, error) {
	if !isSimpleScalarRangeOrPrefixLeaf(e) {
		return preparedScalarOverlaySpan{}, false, nil
	}
	fm := qv.fieldMetaByExpr(e)
	if fm == nil || fm.Slice {
		return preparedScalarOverlaySpan{}, false, nil
	}
	rb, ok, err := qv.rangeBoundsForScalarExpr(e)
	if err != nil {
		return preparedScalarOverlaySpan{}, false, err
	}
	if !ok || rb.Empty {
		return preparedScalarOverlaySpan{}, false, nil
	}

	out := preparedScalarOverlaySpan{
		ov: qv.fieldOverlayForExpr(e),
	}
	if !out.ov.HasData() {
		return out, true, nil
	}
	out.hasData = true
	out.br = out.ov.RangeForBounds(rb)
	return out, true, nil
}

func (qv *queryView) prepareScalarPrefixSpan(e qir.Expr) (preparedScalarPrefixSpan, bool, error) {
	out, ok, err := qv.prepareScalarPrefixRoute(e)
	if err != nil || !ok {
		return preparedScalarPrefixSpan{}, ok, err
	}
	if !out.hasData || out.br.Empty() {
		return out, true, nil
	}

	cur := out.ov.NewCursor(out.br, false)
	for {
		_, ids, ok := cur.Next()
		if !ok {
			break
		}
		out.span++
		card := ids.Cardinality()
		if ^uint64(0)-out.rows < card {
			out.rows = ^uint64(0)
		} else {
			out.rows += card
		}
	}
	return out, true, nil
}

func (qv *queryView) prepareScalarPrefixRoute(e qir.Expr) (preparedScalarPrefixSpan, bool, error) {
	if e.Not || e.Op != qir.OpPREFIX || e.FieldOrdinal < 0 {
		return preparedScalarPrefixSpan{}, false, nil
	}
	span, ok, err := qv.prepareScalarOverlaySpan(e)
	if err != nil || !ok {
		return preparedScalarPrefixSpan{}, false, err
	}
	rb, ok, err := qv.rangeBoundsForScalarExpr(e)
	if err != nil {
		return preparedScalarPrefixSpan{}, false, err
	}
	if !ok || rb.Empty || !rb.HasPrefix || rb.Prefix == "" {
		return preparedScalarPrefixSpan{}, false, nil
	}

	out := preparedScalarPrefixSpan{
		prefix: rb.Prefix,
		ov:     span.ov,
		br:     span.br,
	}
	if !span.hasData {
		return out, true, nil
	}

	out.hasData = true
	return out, true, nil
}

func (qv *queryView) applyScalarExprToRangeBounds(e qir.Expr, rb *indexdata.Bounds) (bool, bool) {
	if isScalarRangeEqOp(e.Op) {
		bound, isSlice, err := qv.normalizedScalarBoundForExpr(e)
		if err != nil || isSlice {
			return false, false
		}
		applyNormalizedScalarBound(rb, bound)
		return true, true
	}
	if e.Op == qir.OpPREFIX {
		bound, isSlice, err := qv.normalizedScalarBoundForExpr(e)
		if err != nil || isSlice || bound.empty || bound.full {
			return false, false
		}
		rb.ApplyPrefix(bound.key)
		return true, true
	}
	return false, true
}

func (qv *queryView) rangeBoundsForScalarExpr(e qir.Expr) (indexdata.Bounds, bool, error) {
	bound, isSlice, err := qv.normalizedScalarBoundForExpr(e)
	if err != nil {
		return indexdata.Bounds{}, false, err
	}
	if isSlice {
		return indexdata.Bounds{}, false, nil
	}
	return rangeBoundsForNormalizedScalarBound(bound), true, nil
}

func findOrderedMergedScalarRangeField(groups *pooled.Slice[orderedMergedScalarRangeField], field string) int {
	if groups == nil {
		return -1
	}
	for i := 0; i < groups.Len(); i++ {
		if groups.Get(i).field == field {
			return i
		}
	}
	return -1
}

func orderedMergedScalarRangeFieldCount(groups *pooled.Slice[orderedMergedScalarRangeField], field string) int {
	idx := findOrderedMergedScalarRangeField(groups, field)
	if idx < 0 {
		return 0
	}
	return groups.Get(idx).count
}

func (qv *queryView) collectOrderedMergedScalarRangeFields(
	orderField string,
	leaves []qir.Expr,
	dst *pooled.Slice[orderedMergedScalarRangeField],
) bool {
	dst.Truncate()
	for i, e := range leaves {
		if !qv.isPositiveOrderedNumericRangeLeaf(e, orderField) {
			continue
		}
		rb, ok, err := qv.rangeBoundsForScalarExpr(e)
		if err != nil || !ok {
			return false
		}
		fieldName := qv.engine.fieldNameByOrdinal(e.FieldOrdinal)
		idx := findOrderedMergedScalarRangeField(dst, fieldName)
		if idx < 0 {
			dst.Append(orderedMergedScalarRangeField{
				field:  fieldName,
				expr:   e,
				bounds: rb,
				first:  i,
				count:  1,
			})
			continue
		}
		group := dst.Get(idx)
		mergeRangeBounds(&group.bounds, rb)
		group.count++
		dst.Set(idx, group)
	}
	return true
}

func (qv *queryView) collectMergedNumericRangeFields(
	leaves []qir.Expr,
	dst *pooled.Slice[orderedMergedScalarRangeField],
) bool {
	dst.Truncate()
	for i, e := range leaves {
		if !qv.isPositiveMergedNumericRangeLeaf(e) {
			continue
		}
		rb, ok, err := qv.rangeBoundsForScalarExpr(e)
		if err != nil || !ok {
			return false
		}
		fieldName := qv.engine.fieldNameByOrdinal(e.FieldOrdinal)
		idx := findOrderedMergedScalarRangeField(dst, fieldName)
		if idx < 0 {
			dst.Append(orderedMergedScalarRangeField{
				field:  fieldName,
				expr:   e,
				bounds: rb,
				first:  i,
				count:  1,
			})
			continue
		}
		group := dst.Get(idx)
		mergeRangeBounds(&group.bounds, rb)
		group.count++
		dst.Set(idx, group)
	}
	return true
}

func mergeRangeBounds(dst *indexdata.Bounds, src indexdata.Bounds) {
	if !dst.Has {
		*dst = src
		return
	}
	if src.Empty {
		dst.SetEmpty()
		return
	}
	if src.HasLo {
		if src.LoNumeric {
			dst.ApplyLoIndex(src.LoIndex, src.LoInc)
		} else {
			dst.ApplyLo(src.LoKey, src.LoInc)
		}
	}
	if src.HasHi {
		if src.HiNumeric {
			dst.ApplyHiIndex(src.HiIndex, src.HiInc)
		} else {
			dst.ApplyHi(src.HiKey, src.HiInc)
		}
	}
	if src.HasPrefix {
		dst.ApplyPrefix(src.Prefix)
	}
	if src.Has {
		dst.Has = true
	}
}

func (core *preparedScalarRangePredicate) runtimeReuse(est uint64, useComplement bool) materializedPredReuse {
	if useComplement {
		return newMaterializedPredReadOnlyReuse(core.qv.snap, core.complementCacheKey)
	}
	stateReuse := core.loadReuse
	if core.secondHitReuse.canSecondHitShareModeratelyOversizedEstimate(est) &&
		allowRuntimePositiveRangeSecondHitShare(est, core.qv.snapshotUniverseCardinality()) {
		stateReuse = core.secondHitReuse
	}
	return stateReuse
}

func (core *preparedScalarRangePredicate) hasNilTail() bool {
	return core.fm != nil &&
		core.fm.Ptr &&
		core.qv.nilFieldOverlayForExpr(core.expr).LookupCardinality(nilIndexEntryKey) > 0
}

func (core *preparedScalarRangePredicate) usesRuntimeComplement(useComplement bool) bool {
	return useComplement && !core.hasNilTail()
}

func (core *preparedScalarRangePredicate) planOverlay(ov indexdata.FieldOverlay) (preparedOverlayRangePredicatePlan, predicate, bool) {
	br := ov.RangeForBounds(core.bounds)
	if br.Empty() {
		if core.expr.Not {
			return preparedOverlayRangePredicatePlan{}, predicate{expr: core.expr, alwaysTrue: true}, true
		}
		return preparedOverlayRangePredicatePlan{}, predicate{expr: core.expr, alwaysFalse: true}, true
	}

	bucketCount, est := ov.RangeStats(br)
	if bucketCount == 0 {
		if core.expr.Not {
			return preparedOverlayRangePredicatePlan{}, predicate{expr: core.expr, alwaysTrue: true}, true
		}
		return preparedOverlayRangePredicatePlan{}, predicate{expr: core.expr, alwaysFalse: true}, true
	}

	totalBuckets := ov.KeyCount()
	ptrHasNilTail := core.hasNilTail()
	fullSpanHasNilTail := bucketCount == totalBuckets && ptrHasNilTail
	if bucketCount == totalBuckets && !fullSpanHasNilTail {
		if core.expr.Not {
			return preparedOverlayRangePredicatePlan{}, predicate{expr: core.expr, alwaysFalse: true}, true
		}
		return preparedOverlayRangePredicatePlan{}, predicate{expr: core.expr, alwaysTrue: true}, true
	}

	plan := preparedOverlayRangePredicatePlan{
		br:                  br,
		bucketCount:         bucketCount,
		est:                 est,
		useComplement:       totalBuckets-bucketCount < bucketCount,
		runtimeProbeBuckets: bucketCount,
		runtimeProbeEst:     est,
	}

	if core.usesRuntimeComplement(plan.useComplement) {
		plan.runtimeProbeBuckets = totalBuckets - bucketCount
		universe := core.qv.snapshotUniverseCardinality()
		if universe > est {
			plan.runtimeProbeEst = universe - est
		} else {
			plan.runtimeProbeEst = 0
		}
		if core.qv.nilFieldOverlayForExpr(core.expr).LookupCardinality(nilIndexEntryKey) > 0 {
			plan.runtimeProbeBuckets++
		}
		if plan.runtimeProbeBuckets == 0 && plan.runtimeProbeEst > 0 {
			plan.runtimeProbeBuckets = 1
		}
	}

	return plan, predicate{}, false
}

func (core *preparedScalarRangePredicate) buildFromOverlay(
	ov indexdata.FieldOverlay,
	allowMaterialize bool,
	lazyColdMaterialize bool,
	allowWarmLoad bool,
) (predicate, bool) {
	plan, pred, done := core.planOverlay(ov)
	if done {
		return pred, true
	}

	if allowWarmLoad {
		if cached, ok := core.loadReuse.load(); ok {
			return materializedRangePredicateWithMode(core.expr, cached), true
		}
	}

	useRuntimeComplement := core.usesRuntimeComplement(plan.useComplement)
	probeLen := plan.bucketCount
	probeEst := plan.est
	if useRuntimeComplement {
		probeLen = -1
		probeEst = 0
	}
	probe := newOverlayRangeProbe(ov, plan.br, useRuntimeComplement, probeLen, probeEst)

	coldMaterializeAllowed := allowMaterialize
	if coldMaterializeAllowed && lazyColdMaterialize && core.usePostingFilter {
		totalBuckets := probe.ov.KeyCount()
		inBuckets := 0
		for i := 0; i < probe.spanCnt; i++ {
			inBuckets += probe.spans[i].BaseEnd - probe.spans[i].BaseStart
		}
		if probe.useComplement {
			inBuckets = totalBuckets - inBuckets
		}
		if totalBuckets > 0 && inBuckets > 0 && inBuckets < totalBuckets && probe.probeLen > 0 &&
			rangePostingFilterMaterializeAfterForProbe(probe.probeLen, probe.probeEst) > 1 {
			coldMaterializeAllowed = false
		}
	}

	if allowMaterialize {
		fieldName := core.qv.engine.fieldNameByOrdinal(core.expr.FieldOrdinal)
		if coldMaterializeAllowed {
			if out, ok := core.qv.tryEvalNumericRangeBuckets(fieldName, core.fm, ov, plan.br); ok {
				out.ids = core.sharedReuse.share(out.ids)
				return materializedRangePredicateWithMode(core.expr, out.ids), true
			}
		} else if out, ok := core.qv.tryLoadNumericRangeBuckets(fieldName, core.fm, ov, plan.br); ok {
			out.ids = core.sharedReuse.share(out.ids)
			return materializedRangePredicateWithMode(core.expr, out.ids), true
		}
	}
	reuse := core.runtimeReuse(plan.est, useRuntimeComplement)
	if allowMaterialize && !useRuntimeComplement && !schema.FieldUsesOrderedNumericKeys(core.fm) {
		reuse = core.sharedReuse
	}
	materializeAfter := rangeMaterializeAfterForProbe(probe.probeLen, probe.probeEst)
	state := overlayRangePredicateStatePool.Get()
	state.ov = ov
	state.br = plan.br
	state.probe = probe
	state.reuse = reuse
	state.neg = core.expr.Not
	state.bucketCount = plan.bucketCount
	state.linearContainsMax = rangeLinearContainsLimit(probe.probeLen, probe.probeEst)
	state.materializeAfter = materializeAfter
	state.rangeMaterializeAt = rangePostingFilterMaterializeAfterForProbe(plan.bucketCount, plan.est)
	state.keepProbeHits = probe.useComplement == core.expr.Not
	state.probePostingFilter = false
	state.postingFilterCheap = false
	state.probeMaterializeAt = 0
	if core.usePostingFilter {
		totalBuckets := probe.ov.KeyCount()
		inBuckets := 0
		for i := 0; i < probe.spanCnt; i++ {
			inBuckets += probe.spans[i].BaseEnd - probe.spans[i].BaseStart
		}
		if probe.useComplement {
			inBuckets = totalBuckets - inBuckets
		}
		if totalBuckets > 0 && inBuckets > 0 && inBuckets < totalBuckets && probe.probeLen > 0 {
			state.probePostingFilter = true
			state.postingFilterCheap = rangeProbeSupportsCheapPostingFilter(state.keepProbeHits, probe.probeLen)
			state.probeMaterializeAt = rangePostingFilterMaterializeAfterForProbe(probe.probeLen, probe.probeEst)
		}
	}
	state.setExpectedContainsCalls(materializeAfter)

	if core.expr.Not {
		return predicate{
			expr:               core.expr,
			overlayState:       state,
			postingFilterCheap: state.postingFilterCheap,
		}, true
	}

	return predicate{
		expr:               core.expr,
		overlayState:       state,
		estCard:            plan.est,
		postingFilterCheap: state.postingFilterCheap,
	}, true
}

func (core *preparedScalarRangePredicate) evalMaterializedPostingResult(ov indexdata.FieldOverlay) postingResult {
	if cached, ok := core.loadReuse.load(); ok {
		if cached.IsEmpty() {
			return postingResult{}
		}
		return postingResult{ids: cached}
	}

	br := ov.RangeForBounds(core.bounds)
	if br.Empty() {
		if core.sharedReuse.snap != nil && !core.sharedReuse.cacheKey.isZero() {
			core.sharedReuse.snap.storeMaterializedPredKey(core.sharedReuse.cacheKey, posting.List{})
		}
		return postingResult{}
	}

	if core.expr.Op != qir.OpPREFIX {
		if out, ok := core.qv.tryEvalNumericRangeBuckets(core.qv.engine.fieldNameByOrdinal(core.expr.FieldOrdinal), core.fm, ov, br); ok {
			out.ids = core.sharedReuse.share(out.ids)
			if out.ids.IsEmpty() {
				return postingResult{}
			}
			return out
		}
	}

	ids := ov.UnionRangePostings(br, indexdata.OverlayRange{})
	ids = core.sharedReuse.share(ids)
	if ids.IsEmpty() {
		return postingResult{}
	}
	return postingResult{ids: ids}
}

func (core *preparedScalarRangePredicate) orderBasicMaterializationStats(universe uint64) (scalarMaterializationStats, bool) {
	if core.expr.Not || core.expr.FieldOrdinal < 0 {
		return scalarMaterializationStats{}, false
	}

	ov := core.qv.fieldOverlayForExpr(core.expr)
	if !ov.HasData() {
		return scalarMaterializationStats{}, false
	}
	plan, _, done := core.planOverlay(ov)
	if done || plan.bucketCount == 0 || plan.est == 0 {
		return scalarMaterializationStats{}, false
	}

	stats := scalarMaterializationStats{
		cacheKey:     core.sharedReuse.cacheKey,
		probeBuckets: plan.bucketCount,
		probeEst:     plan.est,
		buildBuckets: plan.bucketCount,
		buildEst:     plan.est,
	}

	if plan.useComplement {
		complementBuckets := ov.KeyCount() - plan.bucketCount
		complementEst := uint64(0)
		if universe > plan.est {
			complementEst = universe - plan.est
		}
		if core.qv.nilFieldOverlayForExpr(core.expr).LookupCardinality(nilIndexEntryKey) > 0 {
			complementBuckets++
		}
		if complementBuckets == 0 && complementEst > 0 {
			complementBuckets = 1
		}

		stats.probeBuckets = complementBuckets
		stats.probeEst = complementEst

		if isNumericRangeOp(core.expr.Op) {
			stats.cacheKey = core.complementCacheKey
			stats.buildBuckets = complementBuckets
			stats.buildEst = complementEst
			stats.buildComplement = true
		}
	}

	return stats, true
}

func (core *preparedScalarRangePredicate) loadWarmScalarPostingResult() (postingResult, bool) {
	stats, ok := core.orderBasicMaterializationStats(core.qv.snapshotUniverseCardinality())
	if !ok {
		return postingResult{}, false
	}
	if !stats.cacheKey.isZero() {
		if cached, ok := core.qv.snap.loadMaterializedPredKey(stats.cacheKey); ok {
			return postingResult{ids: cached, neg: stats.buildComplement}, true
		}
	}

	ov := core.qv.fieldOverlayForExpr(core.expr)
	if !ov.HasData() {
		return postingResult{}, false
	}
	plan, _, done := core.planOverlay(ov)
	if done {
		return postingResult{}, false
	}
	if out, ok := core.qv.tryLoadNumericRangeBuckets(core.qv.engine.fieldNameByOrdinal(core.expr.FieldOrdinal), core.fm, ov, plan.br); ok {
		return out, true
	}
	return postingResult{}, false
}

func (qv *queryView) loadWarmPreparedScalarExactRange(op preparedScalarExactRange) (postingResult, bool) {
	if !op.cacheKey.isZero() {
		if cached, ok := qv.snap.loadMaterializedPredKey(op.cacheKey); ok {
			return postingResult{ids: cached}, true
		}
	}
	fm := qv.engine.schema.Fields[op.field]
	if !schema.FieldUsesOrderedNumericKeys(fm) {
		return postingResult{}, false
	}
	ov := qv.fieldOverlay(op.field)
	if !ov.HasData() {
		return postingResult{}, false
	}
	br := ov.RangeForBounds(op.bounds)
	if br.Empty() {
		return postingResult{}, false
	}
	return qv.tryLoadNumericRangeBuckets(op.field, fm, ov, br)
}

func (qv *queryView) evalPreparedScalarExactRange(op preparedScalarExactRange) (postingResult, error) {
	if !op.cacheKey.isZero() {
		if cached, ok := qv.snap.loadMaterializedPredKey(op.cacheKey); ok {
			return postingResult{ids: cached}, nil
		}
	}
	fm := qv.engine.schema.Fields[op.field]
	if !schema.FieldUsesOrderedNumericKeys(fm) {
		return postingResult{}, nil
	}
	ov := qv.fieldOverlay(op.field)
	if !ov.HasData() {
		return postingResult{}, nil
	}
	br := ov.RangeForBounds(op.bounds)
	if br.Empty() {
		return postingResult{}, nil
	}
	if out, ok := qv.tryEvalNumericRangeBuckets(op.field, fm, ov, br); ok {
		if !op.cacheKey.isZero() {
			out.ids = qv.tryShareMaterializedPred(op.cacheKey, out.ids)
		}
		return out, nil
	}
	ids := ov.UnionRangePostings(br, indexdata.OverlayRange{})
	if !op.cacheKey.isZero() {
		ids = qv.tryShareMaterializedPred(op.cacheKey, ids)
	}
	return postingResult{ids: ids}, nil
}

func (qv *queryView) shouldPromoteObservedPreparedScalarExactRange(
	op preparedScalarExactRange,
	observedRows uint64,
	needWindow uint64,
) bool {
	if observedRows == 0 {
		return false
	}
	if needWindow == 0 {
		needWindow = 1
	}
	if observedRows <= needWindow {
		return false
	}
	ov := qv.fieldOverlay(op.field)
	if !ov.HasData() {
		return false
	}
	br := ov.RangeForBounds(op.bounds)
	return !br.Empty()
}

func (core *preparedScalarRangePredicate) prepareComplementMaterialization() (scalarComplementMaterializationPlan, bool) {
	if !isNumericRangeOp(core.expr.Op) {
		return scalarComplementMaterializationPlan{}, false
	}

	ov := core.qv.fieldOverlayForExpr(core.expr)
	if !ov.HasData() {
		return scalarComplementMaterializationPlan{}, false
	}

	br := ov.RangeForBounds(core.bounds)
	before, after := overlayComplementRangeSpans(ov, br)
	nilPosting := core.qv.nilFieldOverlayForExpr(core.expr).LookupPostingRetained(nilIndexEntryKey)

	plan := scalarComplementMaterializationPlan{
		sharedReuse: newMaterializedPredSharedReuse(core.qv.snap, core.complementCacheKey),
		before:      before,
		after:       after,
		nilPosting:  nilPosting,
	}
	addScalarComplementPlanSpan(&plan, ov, before)
	addScalarComplementPlanSpan(&plan, ov, after)
	if !nilPosting.IsEmpty() {
		plan.buckets++
		plan.est = satAddUint64(plan.est, nilPosting.Cardinality())
	}

	return plan, true
}

func addScalarComplementPlanSpan(plan *scalarComplementMaterializationPlan, ov indexdata.FieldOverlay, span indexdata.OverlayRange) {
	if span.Empty() {
		return
	}
	buckets, est := ov.RangeStats(span)
	plan.buckets += buckets
	plan.est = satAddUint64(plan.est, est)
}

func (core *preparedScalarRangePredicate) loadComplementMaterialization() (scalarComplementMaterializationPlan, posting.List, bool, bool, bool) {
	plan, ok := core.prepareComplementMaterialization()
	if !ok {
		return scalarComplementMaterializationPlan{}, posting.List{}, false, false, false
	}
	if cached, ok := plan.sharedReuse.load(); ok {
		return plan, cached, true, cached.IsEmpty(), true
	}
	if plan.est == 0 {
		return plan, posting.List{}, false, true, true
	}
	return plan, posting.List{}, false, false, true
}

func (core *preparedScalarRangePredicate) materializeComplement(plan scalarComplementMaterializationPlan) posting.List {
	var ids posting.List
	ov := core.qv.fieldOverlayForExpr(core.expr)
	fieldName := core.qv.engine.fieldNameByOrdinal(core.expr.FieldOrdinal)
	var pendingBefore, pendingAfter indexdata.OverlayRange
	if !plan.before.Empty() {
		if out, ok := core.qv.tryEvalNumericRangeBuckets(fieldName, core.fm, ov, plan.before); ok {
			if ids.IsEmpty() {
				ids = out.ids
			} else {
				ids = ids.BuildMergedOwned(out.ids)
			}
		} else {
			pendingBefore = plan.before
		}
	}
	if !plan.after.Empty() {
		if out, ok := core.qv.tryEvalNumericRangeBuckets(fieldName, core.fm, ov, plan.after); ok {
			if ids.IsEmpty() {
				ids = out.ids
			} else {
				ids = ids.BuildMergedOwned(out.ids)
			}
		} else {
			pendingAfter = plan.after
		}
	}
	if !pendingBefore.Empty() || !pendingAfter.Empty() {
		if ids.IsEmpty() {
			ids = ov.UnionRangePostings(pendingBefore, pendingAfter)
		} else {
			ids = ov.MergeRangePostingsInto(ids, pendingBefore, pendingAfter)
		}
	}
	if !plan.nilPosting.IsEmpty() {
		if ids.IsEmpty() {
			return plan.nilPosting.Borrow()
		}
		return ids.BuildOr(plan.nilPosting)
	}
	return ids
}

func (plan preparedOverlayRangePredicatePlan) orderedEagerMaterializeUseful(orderedWindow int, universe uint64) bool {
	if orderedWindow <= 0 || universe == 0 || plan.bucketCount == 0 || plan.est == 0 {
		return false
	}
	buildWork := rangeProbeMaterializeWork(plan.bucketCount, plan.est)
	expectedRows := orderedPredicateExpectedRows(orderedWindow, plan.est, universe)
	if expectedRows == 0 {
		return false
	}
	probeWork := rangeProbeTotalWorkForRows(int(expectedRows), plan.runtimeProbeBuckets, plan.runtimeProbeEst)
	if probeWork < buildWork {
		return false
	}
	retainedPenalty := satMulUint64(plan.est, postingContainsLookupWork(plan.est))
	return probeWork >= satAddUint64(buildWork, retainedPenalty)
}
