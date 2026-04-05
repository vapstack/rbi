package rbi

import (
	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/posting"
)

type preparedScalarRangePredicate[K ~string | ~uint64, V any] struct {
	qv               *queryView[K, V]
	expr             qx.Expr
	fm               *field
	bound            normalizedScalarBound
	bounds           rangeBounds
	loadReuse        materializedPredReuse
	sharedReuse      materializedPredReuse
	secondHitReuse   materializedPredReuse
	usePostingFilter bool
}

type preparedBaseRangePredicatePlan struct {
	start         int
	end           int
	inBuckets     int
	est           uint64
	useComplement bool
}

type preparedOverlayRangePredicatePlan struct {
	br                  overlayRange
	bucketCount         int
	est                 uint64
	useComplement       bool
	runtimeProbeBuckets int
	runtimeProbeEst     uint64
}

type scalarMaterializationStats struct {
	cacheKey        string
	probeBuckets    int
	probeEst        uint64
	buildBuckets    int
	buildEst        uint64
	buildComplement bool
}

type scalarComplementMaterializationPlan struct {
	sharedReuse materializedPredReuse
	before      overlayRange
	after       overlayRange
	nilPosting  posting.List
	buckets     int
	est         uint64
}

type preparedScalarExactRange struct {
	field  string
	bounds rangeBounds
}

type orderFieldScalarLeafKind uint8

const (
	orderFieldScalarLeafOther orderFieldScalarLeafKind = iota
	orderFieldScalarLeafRange
	orderFieldScalarLeafPrefix
	orderFieldScalarLeafInvalid
)

type preparedScalarRangeRoutingCandidate[K ~string | ~uint64, V any] struct {
	core    preparedScalarRangePredicate[K, V]
	plan    preparedOverlayRangePredicatePlan
	numeric bool
}

type orderedMergedScalarRangeField struct {
	field  string
	expr   qx.Expr
	bounds rangeBounds
	first  int
	count  int
}

type preparedScalarPrefixSpan struct {
	prefix  string
	ov      fieldOverlay
	br      overlayRange
	hasData bool
	span    int
	rows    uint64
}

type preparedScalarOverlaySpan struct {
	ov      fieldOverlay
	br      overlayRange
	hasData bool
}

func storeEmptyScalarComplementMaterialization(plan scalarComplementMaterializationPlan) {
	if plan.sharedReuse.snap == nil || plan.sharedReuse.cacheKey == "" {
		return
	}
	plan.sharedReuse.snap.storeMaterializedPred(plan.sharedReuse.cacheKey, posting.List{})
}

func classifyOrderFieldScalarLeaf(orderField string, e qx.Expr) orderFieldScalarLeafKind {
	if e.Field == "" || e.Field != orderField {
		return orderFieldScalarLeafOther
	}
	if e.Not {
		return orderFieldScalarLeafInvalid
	}
	if isScalarRangeEqOp(e.Op) {
		return orderFieldScalarLeafRange
	}
	if e.Op == qx.OpPREFIX {
		return orderFieldScalarLeafPrefix
	}
	return orderFieldScalarLeafInvalid
}

func isNumericRangeOp(op qx.Op) bool {
	switch op {
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE:
		return true
	default:
		return false
	}
}

func isScalarEqOp(op qx.Op) bool {
	return op == qx.OpEQ
}

func isScalarEqOrInOp(op qx.Op) bool {
	return op == qx.OpEQ || op == qx.OpIN
}

func isScalarRangeEqOp(op qx.Op) bool {
	return isScalarEqOp(op) || isNumericRangeOp(op)
}

func isPositiveScalarEqLeaf(e qx.Expr) bool {
	return !e.Not && e.Field != "" && len(e.Operands) == 0 && isScalarEqOp(e.Op)
}

func allPositiveScalarEqLeaves(leaves []qx.Expr) bool {
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

func isPositiveScalarPrefixLeaf(e qx.Expr) bool {
	return !e.Not && e.Field != "" && len(e.Operands) == 0 && e.Op == qx.OpPREFIX
}

func isPositiveNonOrderScalarPrefixLeaf(orderField string, e qx.Expr) bool {
	return e.Field != orderField && isPositiveScalarPrefixLeaf(e)
}

func (qv *queryView[K, V]) isPositiveMergedNumericRangeLeaf(e qx.Expr) bool {
	if e.Not || e.Field == "" || !isScalarRangeEqOp(e.Op) {
		return false
	}
	fm := qv.fields[e.Field]
	return fm != nil && !fm.Slice && isNumericScalarKind(fm.Kind)
}

func isSimpleScalarRangeOrPrefixLeaf(e qx.Expr) bool {
	if e.Not || e.Field == "" {
		return false
	}
	return isScalarRangeOrPrefixOp(e.Op)
}

func isScalarRangeOrPrefixOp(op qx.Op) bool {
	if isNumericRangeOp(op) || op == qx.OpPREFIX {
		return true
	}
	return false
}

func isMaterializedScalarCacheOp(op qx.Op) bool {
	switch op {
	case qx.OpSUFFIX, qx.OpCONTAINS:
		return true
	default:
		return isScalarRangeOrPrefixOp(op)
	}
}

func (qv *queryView[K, V]) normalizedScalarBoundForExpr(e qx.Expr) (normalizedScalarBound, bool, error) {
	return qv.exprValueToNormalizedScalarBound(qx.Expr{
		Op:    e.Op,
		Field: e.Field,
		Value: e.Value,
	})
}

func rangeBoundsForNormalizedScalarBound(bound normalizedScalarBound) rangeBounds {
	rb := rangeBounds{has: true}
	applyNormalizedScalarBound(&rb, bound)
	return rb
}

func (qv *queryView[K, V]) prepareScalarRangePredicateFromBound(
	e qx.Expr,
	fm *field,
	bound normalizedScalarBound,
) preparedScalarRangePredicate[K, V] {
	return preparedScalarRangePredicate[K, V]{
		qv:               qv,
		expr:             e,
		fm:               fm,
		bound:            bound,
		bounds:           rangeBoundsForNormalizedScalarBound(bound),
		loadReuse:        qv.materializedPredReadOnlyReuseForScalar(e.Field, bound, false),
		sharedReuse:      qv.materializedPredSharedReuseForScalar(e.Field, bound),
		secondHitReuse:   qv.materializedPredSecondHitSharedReuseForScalar(e.Field, bound),
		usePostingFilter: shouldUseNumericRangePostingFilter(e, fm),
	}
}

func (qv *queryView[K, V]) prepareExactScalarRangePredicate(
	e qx.Expr,
	fm *field,
	bounds rangeBounds,
) preparedScalarRangePredicate[K, V] {
	return preparedScalarRangePredicate[K, V]{
		qv:               qv,
		expr:             e,
		fm:               fm,
		bounds:           bounds,
		loadReuse:        qv.materializedPredReadOnlyReuseForExactScalarRange(e.Field, bounds),
		sharedReuse:      qv.materializedPredSharedReuseForExactScalarRange(e.Field, bounds),
		secondHitReuse:   qv.materializedPredSecondHitSharedReuseForExactScalarRange(e.Field, bounds),
		usePostingFilter: fm != nil && !fm.Slice && isNumericScalarKind(fm.Kind),
	}
}

func (qv *queryView[K, V]) prepareScalarRangePredicate(
	e qx.Expr,
	fm *field,
) (preparedScalarRangePredicate[K, V], predicate, bool, bool) {
	if fm == nil || fm.Slice {
		return preparedScalarRangePredicate[K, V]{}, predicate{}, false, false
	}

	bound, isSlice, err := qv.normalizedScalarBoundForExpr(e)
	if err != nil || isSlice {
		return preparedScalarRangePredicate[K, V]{}, predicate{}, false, false
	}
	if bound.empty {
		if e.Not {
			return preparedScalarRangePredicate[K, V]{}, predicate{expr: e, alwaysTrue: true}, true, true
		}
		return preparedScalarRangePredicate[K, V]{}, predicate{expr: e, alwaysFalse: true}, true, true
	}

	return qv.prepareScalarRangePredicateFromBound(e, fm, bound), predicate{}, false, true
}

func (qv *queryView[K, V]) prepareScalarRangeRoutingCandidate(
	e qx.Expr,
) (preparedScalarRangeRoutingCandidate[K, V], bool) {
	if e.Not || e.Field == "" {
		return preparedScalarRangeRoutingCandidate[K, V]{}, false
	}
	fm := qv.fields[e.Field]
	if fm == nil || fm.Slice {
		return preparedScalarRangeRoutingCandidate[K, V]{}, false
	}
	core, _, done, ok := qv.prepareScalarRangePredicate(e, fm)
	if !ok || done {
		return preparedScalarRangeRoutingCandidate[K, V]{}, false
	}
	ov := qv.fieldOverlay(e.Field)
	if !ov.hasData() {
		return preparedScalarRangeRoutingCandidate[K, V]{}, false
	}
	plan, _, done := core.planOverlay(ov)
	if done {
		return preparedScalarRangeRoutingCandidate[K, V]{}, false
	}
	return preparedScalarRangeRoutingCandidate[K, V]{
		core:    core,
		plan:    plan,
		numeric: isNumericScalarKind(fm.Kind),
	}, true
}

func (qv *queryView[K, V]) preparePredicateScalarRangeRoutingCandidate(
	p predicate,
) (preparedScalarRangeRoutingCandidate[K, V], bool) {
	if p.expr.Not || p.expr.Field == "" {
		return preparedScalarRangeRoutingCandidate[K, V]{}, false
	}
	fm := qv.fields[p.expr.Field]
	if fm == nil || fm.Slice {
		return preparedScalarRangeRoutingCandidate[K, V]{}, false
	}
	if !p.hasEffectiveBounds {
		return qv.prepareScalarRangeRoutingCandidate(p.expr)
	}
	ov := qv.fieldOverlay(p.expr.Field)
	if !ov.hasData() {
		return preparedScalarRangeRoutingCandidate[K, V]{}, false
	}
	core := qv.prepareExactScalarRangePredicate(p.expr, fm, p.effectiveBounds)
	plan, _, done := core.planOverlay(ov)
	if done {
		return preparedScalarRangeRoutingCandidate[K, V]{}, false
	}
	return preparedScalarRangeRoutingCandidate[K, V]{
		core:    core,
		plan:    plan,
		numeric: isNumericScalarKind(fm.Kind),
	}, true
}

func (candidate preparedScalarRangeRoutingCandidate[K, V]) broadComplementCardinality(universe uint64) bool {
	return universe > 0 &&
		candidate.plan.est > 0 &&
		candidate.plan.est < universe &&
		candidate.plan.est > universe-candidate.plan.est
}

func (core preparedScalarRangePredicate[K, V]) orderedEagerMaterializeUseful(orderedWindow int) bool {
	if orderedWindow <= 0 {
		return false
	}
	universe := core.qv.snapshotUniverseCardinality()
	if universe == 0 {
		return false
	}
	ov := core.qv.fieldOverlay(core.expr.Field)
	if ov.hasData() {
		plan, _, done := core.planOverlay(ov)
		if done {
			return false
		}
		return plan.orderedEagerMaterializeUseful(orderedWindow, universe)
	}
	slice := core.qv.snapshotFieldIndexSlice(core.expr.Field)
	if slice == nil {
		return false
	}
	plan, _, done := core.planBase(*slice)
	if done {
		return false
	}
	buildWork := rangeProbeMaterializeWork(plan.inBuckets, plan.est)
	expectedRows := orderedPredicateExpectedRows(orderedWindow, plan.est, universe)
	if expectedRows == 0 {
		return false
	}
	probeBuckets := plan.inBuckets
	probeEst := plan.est
	if plan.useComplement {
		probeBuckets = len(*slice) - plan.inBuckets
		if universe > plan.est {
			probeEst = universe - plan.est
		} else {
			probeEst = 0
		}
	}
	probeWork := rangeProbeTotalWorkForRows(int(expectedRows), probeBuckets, probeEst)
	if probeWork < buildWork {
		return false
	}
	retainedPenalty := satMulUint64(plan.est, postingContainsLookupWork(plan.est))
	return probeWork >= satAddUint64(buildWork, retainedPenalty)
}

func (qv *queryView[K, V]) prepareScalarOverlaySpan(e qx.Expr) (preparedScalarOverlaySpan, bool, error) {
	if !isSimpleScalarRangeOrPrefixLeaf(e) {
		return preparedScalarOverlaySpan{}, false, nil
	}
	fm := qv.fields[e.Field]
	if fm == nil || fm.Slice {
		return preparedScalarOverlaySpan{}, false, nil
	}
	rb, ok, err := qv.rangeBoundsForScalarExpr(e)
	if err != nil {
		return preparedScalarOverlaySpan{}, false, err
	}
	if !ok || rb.empty {
		return preparedScalarOverlaySpan{}, false, nil
	}

	out := preparedScalarOverlaySpan{
		ov: qv.fieldOverlay(e.Field),
	}
	if !out.ov.hasData() {
		return out, true, nil
	}
	out.hasData = true
	out.br = out.ov.rangeForBounds(rb)
	return out, true, nil
}

func (qv *queryView[K, V]) prepareScalarPrefixSpan(e qx.Expr) (preparedScalarPrefixSpan, bool, error) {
	out, ok, err := qv.prepareScalarPrefixRoute(e)
	if err != nil || !ok {
		return preparedScalarPrefixSpan{}, ok, err
	}
	if !out.hasData || overlayRangeEmpty(out.br) {
		return out, true, nil
	}

	cur := out.ov.newCursor(out.br, false)
	for {
		_, ids, ok := cur.next()
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

func (qv *queryView[K, V]) prepareScalarPrefixRoute(e qx.Expr) (preparedScalarPrefixSpan, bool, error) {
	if e.Not || e.Op != qx.OpPREFIX || e.Field == "" {
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
	if !ok || rb.empty || !rb.hasPrefix || rb.prefix == "" {
		return preparedScalarPrefixSpan{}, false, nil
	}

	out := preparedScalarPrefixSpan{
		prefix: rb.prefix,
		ov:     span.ov,
		br:     span.br,
	}
	if !span.hasData {
		return out, true, nil
	}

	out.hasData = true
	return out, true, nil
}

func (qv *queryView[K, V]) applyScalarExprToRangeBounds(e qx.Expr, rb *rangeBounds) (bool, bool) {
	if isScalarRangeEqOp(e.Op) {
		bound, isSlice, err := qv.normalizedScalarBoundForExpr(e)
		if err != nil || isSlice {
			return false, false
		}
		applyNormalizedScalarBound(rb, bound)
		return true, true
	}
	if e.Op == qx.OpPREFIX {
		bound, isSlice, err := qv.normalizedScalarBoundForExpr(e)
		if err != nil || isSlice || bound.empty || bound.full {
			return false, false
		}
		rb.applyPrefix(bound.key)
		return true, true
	}
	return false, true
}

func (qv *queryView[K, V]) rangeBoundsForScalarExpr(e qx.Expr) (rangeBounds, bool, error) {
	bound, isSlice, err := qv.normalizedScalarBoundForExpr(e)
	if err != nil {
		return rangeBounds{}, false, err
	}
	if isSlice {
		return rangeBounds{}, false, nil
	}
	return rangeBoundsForNormalizedScalarBound(bound), true, nil
}

func findOrderedMergedScalarRangeField(groups []orderedMergedScalarRangeField, field string) int {
	for i := range groups {
		if groups[i].field == field {
			return i
		}
	}
	return -1
}

func orderedMergedScalarRangeFieldCount(groups []orderedMergedScalarRangeField, field string) int {
	idx := findOrderedMergedScalarRangeField(groups, field)
	if idx < 0 {
		return 0
	}
	return groups[idx].count
}

func (qv *queryView[K, V]) collectOrderedMergedScalarRangeFields(
	orderField string,
	leaves []qx.Expr,
	dst []orderedMergedScalarRangeField,
) ([]orderedMergedScalarRangeField, bool) {
	dst = dst[:0]
	for i, e := range leaves {
		if !qv.isPositiveOrderedNumericRangeLeaf(e, orderField) {
			continue
		}
		rb, ok, err := qv.rangeBoundsForScalarExpr(e)
		if err != nil || !ok {
			return nil, false
		}
		idx := findOrderedMergedScalarRangeField(dst, e.Field)
		if idx < 0 {
			dst = append(dst, orderedMergedScalarRangeField{
				field:  e.Field,
				expr:   e,
				bounds: rb,
				first:  i,
				count:  1,
			})
			continue
		}
		mergeRangeBounds(&dst[idx].bounds, rb)
		dst[idx].count++
	}
	return dst, true
}

func (qv *queryView[K, V]) collectMergedNumericRangeFields(
	leaves []qx.Expr,
	dst []orderedMergedScalarRangeField,
) ([]orderedMergedScalarRangeField, bool) {
	dst = dst[:0]
	for i, e := range leaves {
		if !qv.isPositiveMergedNumericRangeLeaf(e) {
			continue
		}
		rb, ok, err := qv.rangeBoundsForScalarExpr(e)
		if err != nil || !ok {
			return nil, false
		}
		idx := findOrderedMergedScalarRangeField(dst, e.Field)
		if idx < 0 {
			dst = append(dst, orderedMergedScalarRangeField{
				field:  e.Field,
				expr:   e,
				bounds: rb,
				first:  i,
				count:  1,
			})
			continue
		}
		mergeRangeBounds(&dst[idx].bounds, rb)
		dst[idx].count++
	}
	return dst, true
}

func mergeRangeBounds(dst *rangeBounds, src rangeBounds) {
	if !dst.has {
		*dst = src
		return
	}
	if src.empty {
		dst.setEmpty()
		return
	}
	if src.hasLo {
		dst.applyLo(src.loKey, src.loInc)
	}
	if src.hasHi {
		dst.applyHi(src.hiKey, src.hiInc)
	}
	if src.hasPrefix {
		dst.applyPrefix(src.prefix)
	}
	if src.has {
		dst.has = true
	}
}

func (core preparedScalarRangePredicate[K, V]) runtimeReuse(est uint64, useComplement bool) materializedPredReuse {
	if useComplement {
		return core.qv.materializedPredReadOnlyReuseForScalar(core.expr.Field, core.bound, true)
	}
	stateReuse := core.loadReuse
	if core.secondHitReuse.canSecondHitShareModeratelyOversizedEstimate(est) &&
		allowRuntimePositiveRangeSecondHitShare(est, core.qv.snapshotUniverseCardinality()) {
		stateReuse = core.secondHitReuse
	}
	return stateReuse
}

func (core preparedScalarRangePredicate[K, V]) planBase(slice []index) (preparedBaseRangePredicatePlan, predicate, bool) {
	start, end := applyBoundsToIndexRange(slice, core.bounds)
	if start >= end {
		if core.expr.Not {
			return preparedBaseRangePredicatePlan{}, predicate{expr: core.expr, alwaysTrue: true}, true
		}
		return preparedBaseRangePredicatePlan{}, predicate{expr: core.expr, alwaysFalse: true}, true
	}
	if start == 0 && end == len(slice) {
		if core.expr.Not {
			return preparedBaseRangePredicatePlan{}, predicate{expr: core.expr, alwaysFalse: true}, true
		}
		return preparedBaseRangePredicatePlan{}, predicate{expr: core.expr, alwaysTrue: true}, true
	}

	inBuckets := end - start
	est := uint64(0)
	ov := fieldOverlay{base: slice}
	if exact, ok := core.qv.tryCountSnapshotNumericRange(core.expr.Field, core.fm, ov, start, end); ok {
		est = exact
	} else if inBuckets == 1 {
		if ids := slice[start].IDs; !ids.IsEmpty() {
			est = ids.Cardinality()
		}
	} else {
		ix0 := start
		ix1 := start + inBuckets/2
		ix2 := end - 1
		var sum uint64
		var n uint64

		if ids := slice[ix0].IDs; !ids.IsEmpty() {
			sum += ids.Cardinality()
			n++
		}
		if ix1 != ix0 && ix1 != ix2 {
			if ids := slice[ix1].IDs; !ids.IsEmpty() {
				sum += ids.Cardinality()
				n++
			}
		}
		if ix2 != ix0 {
			if ids := slice[ix2].IDs; !ids.IsEmpty() {
				sum += ids.Cardinality()
				n++
			}
		}
		if n > 0 {
			est = (sum / n) * uint64(inBuckets)
		}
	}
	if est == 0 {
		est = uint64(inBuckets)
	}

	return preparedBaseRangePredicatePlan{
		start:         start,
		end:           end,
		inBuckets:     inBuckets,
		est:           est,
		useComplement: len(slice)-inBuckets < inBuckets,
	}, predicate{}, false
}

func (core preparedScalarRangePredicate[K, V]) planOverlay(ov fieldOverlay) (preparedOverlayRangePredicatePlan, predicate, bool) {
	br := ov.rangeForBounds(core.bounds)
	if overlayRangeEmpty(br) {
		if core.expr.Not {
			return preparedOverlayRangePredicatePlan{}, predicate{expr: core.expr, alwaysTrue: true}, true
		}
		return preparedOverlayRangePredicatePlan{}, predicate{expr: core.expr, alwaysFalse: true}, true
	}

	bucketCount, est := overlayRangeStats(ov, br)
	if bucketCount == 0 {
		if core.expr.Not {
			return preparedOverlayRangePredicatePlan{}, predicate{expr: core.expr, alwaysTrue: true}, true
		}
		return preparedOverlayRangePredicatePlan{}, predicate{expr: core.expr, alwaysFalse: true}, true
	}

	totalBuckets := ov.keyCount()
	if bucketCount == totalBuckets {
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

	if plan.useComplement {
		plan.runtimeProbeBuckets = totalBuckets - bucketCount
		universe := core.qv.snapshotUniverseCardinality()
		if universe > est {
			plan.runtimeProbeEst = universe - est
		} else {
			plan.runtimeProbeEst = 0
		}
		if core.qv.nilFieldOverlay(core.expr.Field).lookupCardinality(nilIndexEntryKey) > 0 {
			plan.runtimeProbeBuckets++
		}
		if plan.runtimeProbeBuckets == 0 && plan.runtimeProbeEst > 0 {
			plan.runtimeProbeBuckets = 1
		}
	}

	return plan, predicate{}, false
}

func (core preparedScalarRangePredicate[K, V]) buildFromSlice(slice []index, allowMaterialize bool) (predicate, bool) {
	plan, pred, done := core.planBase(slice)
	if done {
		return pred, true
	}

	if cached, ok := core.loadReuse.load(); ok {
		return materializedRangePredicateWithMode(core.expr, cached), true
	}

	if allowMaterialize {
		ov := fieldOverlay{base: slice}
		if out, ok := core.qv.tryEvalNumericRangeBuckets(core.expr.Field, core.fm, ov, overlayRange{
			baseStart: plan.start,
			baseEnd:   plan.end,
		}); ok {
			out.ids = core.sharedReuse.share(out.ids)
			return materializedRangePredicateWithMode(core.expr, out.ids), true
		}
	}

	probe := newBaseRangeProbe(slice, plan.start, plan.end, plan.useComplement)
	reuse := core.runtimeReuse(plan.est, plan.useComplement)
	if allowMaterialize && !plan.useComplement && core.fm != nil && !isNumericScalarKind(core.fm.Kind) {
		reuse = core.sharedReuse
	}
	state := acquireBaseRangePredicateState(probe, core.expr.Not, reuse)
	postingFilterCheap := false
	if core.usePostingFilter && probe.probeLen != 0 {
		postingFilterCheap = !state.keepProbeHits
	}

	if core.expr.Not {
		return predicate{
			expr:               core.expr,
			baseRangeState:     state,
			postingFilterCheap: postingFilterCheap,
		}, true
	}

	return predicate{
		expr:               core.expr,
		baseRangeState:     state,
		estCard:            plan.est,
		postingFilterCheap: postingFilterCheap,
	}, true
}

func (core preparedScalarRangePredicate[K, V]) buildFromOverlay(ov fieldOverlay, allowMaterialize bool) (predicate, bool) {
	plan, pred, done := core.planOverlay(ov)
	if done {
		return pred, true
	}

	if cached, ok := core.loadReuse.load(); ok {
		return materializedRangePredicateWithMode(core.expr, cached), true
	}

	if allowMaterialize {
		if out, ok := core.qv.tryEvalNumericRangeBuckets(core.expr.Field, core.fm, ov, plan.br); ok {
			out.ids = core.sharedReuse.share(out.ids)
			return materializedRangePredicateWithMode(core.expr, out.ids), true
		}
	}

	probe := newOverlayRangeProbe(ov, plan.br, plan.useComplement)
	reuse := core.runtimeReuse(plan.est, plan.useComplement)
	if allowMaterialize && !plan.useComplement && core.fm != nil && !isNumericScalarKind(core.fm.Kind) {
		reuse = core.sharedReuse
	}
	state := acquireOverlayRangePredicateState(
		ov,
		plan.br,
		probe,
		plan.bucketCount,
		plan.est,
		core.expr.Not,
		reuse,
		core.usePostingFilter,
	)

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

func (core preparedScalarRangePredicate[K, V]) evalMaterializedPostingResult(ov fieldOverlay) postingResult {
	if cached, ok := core.loadReuse.load(); ok {
		if cached.IsEmpty() {
			return postingResult{}
		}
		return postingResult{ids: cached}
	}

	br := ov.rangeForBounds(core.bounds)
	if overlayRangeEmpty(br) {
		if core.sharedReuse.snap != nil && core.sharedReuse.cacheKey != "" {
			core.sharedReuse.snap.storeMaterializedPred(core.sharedReuse.cacheKey, posting.List{})
		}
		return postingResult{}
	}

	if core.expr.Op != qx.OpPREFIX {
		if out, ok := core.qv.tryEvalNumericRangeBuckets(core.expr.Field, core.fm, ov, br); ok {
			out.ids = core.sharedReuse.share(out.ids)
			if out.ids.IsEmpty() {
				return postingResult{}
			}
			return out
		}
	}

	ids := overlayUnionRange(ov, br)
	ids = core.sharedReuse.share(ids)
	if ids.IsEmpty() {
		return postingResult{}
	}
	return postingResult{ids: ids}
}

func (core preparedScalarRangePredicate[K, V]) orderBasicMaterializationStats(universe uint64) (scalarMaterializationStats, bool) {
	if core.expr.Not || core.expr.Field == "" {
		return scalarMaterializationStats{}, false
	}

	ov := core.qv.fieldOverlay(core.expr.Field)
	if !ov.hasData() {
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
		complementBuckets := ov.keyCount() - plan.bucketCount
		complementEst := uint64(0)
		if universe > plan.est {
			complementEst = universe - plan.est
		}
		if core.qv.nilFieldOverlay(core.expr.Field).lookupCardinality(nilIndexEntryKey) > 0 {
			complementBuckets++
		}
		if complementBuckets == 0 && complementEst > 0 {
			complementBuckets = 1
		}

		stats.probeBuckets = complementBuckets
		stats.probeEst = complementEst

		if isNumericRangeOp(core.expr.Op) {
			stats.cacheKey = core.qv.materializedPredComplementCacheKeyForScalar(
				core.expr.Field,
				core.bound.op,
				core.bound.key,
			)
			stats.buildBuckets = complementBuckets
			stats.buildEst = complementEst
			stats.buildComplement = true
		}
	}

	return stats, true
}

func (core preparedScalarRangePredicate[K, V]) loadWarmScalarPostingResult() (postingResult, bool) {
	stats, ok := core.orderBasicMaterializationStats(core.qv.snapshotUniverseCardinality())
	if !ok {
		return postingResult{}, false
	}
	if stats.cacheKey != "" {
		if cached, ok := core.qv.snap.loadMaterializedPred(stats.cacheKey); ok {
			return postingResult{ids: cached, neg: stats.buildComplement}, true
		}
	}

	ov := core.qv.fieldOverlay(core.expr.Field)
	if !ov.hasData() {
		return postingResult{}, false
	}
	plan, _, done := core.planOverlay(ov)
	if done {
		return postingResult{}, false
	}
	if out, ok := core.qv.tryLoadNumericRangeBuckets(core.expr.Field, core.fm, ov, plan.br); ok {
		return out, true
	}
	return postingResult{}, false
}

func materializedPredCacheKeyForExactScalarRange(field string, bounds rangeBounds) string {
	if field == "" || bounds.empty || (!bounds.hasLo && !bounds.hasHi) {
		return ""
	}
	key := field + "\x1f" + "range_exact" + "\x1f"
	if bounds.hasLo {
		if bounds.loInc {
			key += "["
		} else {
			key += "("
		}
		key += bounds.loKey
	}
	key += "\x1f"
	if bounds.hasHi {
		if bounds.hiInc {
			key += "]"
		} else {
			key += ")"
		}
		key += bounds.hiKey
	}
	return key
}

func (qv *queryView[K, V]) loadWarmPreparedScalarExactRange(op preparedScalarExactRange) (postingResult, bool) {
	cacheKey := materializedPredCacheKeyForExactScalarRange(op.field, op.bounds)
	if cacheKey != "" {
		if cached, ok := qv.snap.loadMaterializedPred(cacheKey); ok {
			return postingResult{ids: cached}, true
		}
	}
	fm := qv.fields[op.field]
	if fm == nil || fm.Slice || !isNumericScalarKind(fm.Kind) {
		return postingResult{}, false
	}
	ov := qv.fieldOverlay(op.field)
	if !ov.hasData() {
		return postingResult{}, false
	}
	br := ov.rangeForBounds(op.bounds)
	if overlayRangeEmpty(br) {
		return postingResult{}, false
	}
	return qv.tryLoadNumericRangeBuckets(op.field, fm, ov, br)
}

func (qv *queryView[K, V]) evalPreparedScalarExactRange(op preparedScalarExactRange) (postingResult, error) {
	cacheKey := materializedPredCacheKeyForExactScalarRange(op.field, op.bounds)
	if cacheKey != "" {
		if cached, ok := qv.snap.loadMaterializedPred(cacheKey); ok {
			return postingResult{ids: cached}, nil
		}
	}
	fm := qv.fields[op.field]
	if fm == nil || fm.Slice || !isNumericScalarKind(fm.Kind) {
		return postingResult{}, nil
	}
	ov := qv.fieldOverlay(op.field)
	if !ov.hasData() {
		return postingResult{}, nil
	}
	br := ov.rangeForBounds(op.bounds)
	if overlayRangeEmpty(br) {
		return postingResult{}, nil
	}
	if out, ok := qv.tryEvalNumericRangeBuckets(op.field, fm, ov, br); ok {
		if cacheKey != "" {
			out.ids = qv.tryShareMaterializedPred(cacheKey, out.ids)
		}
		return out, nil
	}
	ids := overlayUnionRange(ov, br)
	if cacheKey != "" {
		ids = qv.tryShareMaterializedPred(cacheKey, ids)
	}
	return postingResult{ids: ids}, nil
}

func (qv *queryView[K, V]) shouldPromoteObservedPreparedScalarExactRange(
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
	if !ov.hasData() {
		return false
	}
	br := ov.rangeForBounds(op.bounds)
	return !overlayRangeEmpty(br)
}

func (core preparedScalarRangePredicate[K, V]) prepareComplementMaterialization() (scalarComplementMaterializationPlan, bool) {
	if !isNumericRangeOp(core.expr.Op) {
		return scalarComplementMaterializationPlan{}, false
	}

	ov := core.qv.fieldOverlay(core.expr.Field)
	if !ov.hasData() {
		return scalarComplementMaterializationPlan{}, false
	}

	br := ov.rangeForBounds(core.bounds)
	before, after := overlayComplementRangeSpans(ov, br)
	nilPosting := core.qv.nilFieldOverlay(core.expr.Field).lookupPostingRetained(nilIndexEntryKey)

	cacheKey := ""
	if !core.bound.full {
		cacheKey = core.qv.materializedPredComplementCacheKeyForScalar(
			core.expr.Field,
			core.bound.op,
			core.bound.key,
		)
	}
	plan := scalarComplementMaterializationPlan{
		sharedReuse: newMaterializedPredSharedReuse(core.qv.snap, cacheKey),
		before:      before,
		after:       after,
		nilPosting:  nilPosting,
	}
	addSpan := func(span overlayRange) {
		if overlayRangeEmpty(span) {
			return
		}
		buckets, est := overlayRangeStats(ov, span)
		plan.buckets += buckets
		plan.est = satAddUint64(plan.est, est)
	}
	addSpan(before)
	addSpan(after)
	if !nilPosting.IsEmpty() {
		plan.buckets++
		plan.est = satAddUint64(plan.est, nilPosting.Cardinality())
	}

	return plan, true
}

func (core preparedScalarRangePredicate[K, V]) loadComplementMaterialization() (scalarComplementMaterializationPlan, posting.List, bool, bool, bool) {
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

func (core preparedScalarRangePredicate[K, V]) materializeComplement(plan scalarComplementMaterializationPlan) posting.List {
	var ids posting.List
	ov := core.qv.fieldOverlay(core.expr.Field)
	if !overlayRangeEmpty(plan.before) {
		if out, ok := core.qv.tryEvalNumericRangeBuckets(core.expr.Field, core.fm, ov, plan.before); ok {
			ids = ids.BuildMergedOwned(out.ids)
		} else {
			ids = ids.BuildMergedOwned(overlayUnionRange(ov, plan.before))
		}
	}
	if !overlayRangeEmpty(plan.after) {
		if out, ok := core.qv.tryEvalNumericRangeBuckets(core.expr.Field, core.fm, ov, plan.after); ok {
			ids = ids.BuildMergedOwned(out.ids)
		} else {
			ids = ids.BuildMergedOwned(overlayUnionRange(ov, plan.after))
		}
	}
	return ids.BuildOr(plan.nilPosting)
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
