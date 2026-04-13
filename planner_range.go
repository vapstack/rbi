package rbi

import (
	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

type preparedScalarRangePredicate[K ~string | ~uint64, V any] struct {
	qv                 *queryView[K, V]
	expr               qx.Expr
	fm                 *field
	bound              normalizedScalarBound
	bounds             rangeBounds
	complementCacheKey materializedPredKey
	loadReuse          materializedPredReuse
	sharedReuse        materializedPredReuse
	secondHitReuse     materializedPredReuse
	usePostingFilter   bool
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
	cacheKey        materializedPredKey
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
	field    string
	bounds   rangeBounds
	cacheKey materializedPredKey
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
	if plan.sharedReuse.snap == nil || plan.sharedReuse.cacheKey.isZero() {
		return
	}
	plan.sharedReuse.snap.storeMaterializedPredKey(plan.sharedReuse.cacheKey, posting.List{})
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

func (qv *queryView[K, V]) initPreparedScalarRangePredicateFromBound(
	core *preparedScalarRangePredicate[K, V],
	e qx.Expr,
	fm *field,
	bound normalizedScalarBound,
) {
	cacheKey := materializedPredKey{}
	complementCacheKey := materializedPredKey{}
	if !bound.full {
		cacheKey = qv.materializedPredKeyForNormalizedScalarBound(e.Field, bound)
		complementCacheKey = qv.materializedPredComplementKeyForNormalizedScalarBound(e.Field, bound)
	}
	loadReuse := newMaterializedPredReadOnlyReuse(qv.snap, cacheKey)
	sharedReuse := newMaterializedPredSharedReuse(qv.snap, cacheKey)
	secondHitReuse := newMaterializedPredSecondHitSharedReuse(qv.snap, cacheKey)
	*core = preparedScalarRangePredicate[K, V]{
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

func (qv *queryView[K, V]) initPreparedExactScalarRangePredicate(
	core *preparedScalarRangePredicate[K, V],
	e qx.Expr,
	fm *field,
	bounds rangeBounds,
) {
	cacheKey := qv.materializedPredKeyForExactScalarRange(e.Field, bounds)
	loadReuse := newMaterializedPredReadOnlyReuse(qv.snap, cacheKey)
	sharedReuse := newMaterializedPredSharedReuse(qv.snap, cacheKey)
	secondHitReuse := newMaterializedPredSecondHitSharedReuse(qv.snap, cacheKey)
	*core = preparedScalarRangePredicate[K, V]{
		qv:               qv,
		expr:             e,
		fm:               fm,
		bounds:           bounds,
		loadReuse:        loadReuse,
		sharedReuse:      sharedReuse,
		secondHitReuse:   secondHitReuse,
		usePostingFilter: fm != nil && !fm.Slice && isNumericScalarKind(fm.Kind),
	}
}

func (qv *queryView[K, V]) initPreparedScalarRangePredicate(
	core *preparedScalarRangePredicate[K, V],
	e qx.Expr,
	fm *field,
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
	var core preparedScalarRangePredicate[K, V]
	_, done, ok := qv.initPreparedScalarRangePredicate(&core, e, fm)
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
	var core preparedScalarRangePredicate[K, V]
	qv.initPreparedExactScalarRangePredicate(&core, p.expr, fm, p.effectiveBounds)
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

func (core *preparedScalarRangePredicate[K, V]) orderedEagerMaterializeUseful(orderedWindow int) bool {
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

func findOrderedMergedScalarRangeField(groups *pooled.SliceBuf[orderedMergedScalarRangeField], field string) int {
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

func orderedMergedScalarRangeFieldCount(groups *pooled.SliceBuf[orderedMergedScalarRangeField], field string) int {
	idx := findOrderedMergedScalarRangeField(groups, field)
	if idx < 0 {
		return 0
	}
	return groups.Get(idx).count
}

func (qv *queryView[K, V]) collectOrderedMergedScalarRangeFields(
	orderField string,
	leaves []qx.Expr,
	dst *pooled.SliceBuf[orderedMergedScalarRangeField],
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
		idx := findOrderedMergedScalarRangeField(dst, e.Field)
		if idx < 0 {
			dst.Append(orderedMergedScalarRangeField{
				field:  e.Field,
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

func (qv *queryView[K, V]) collectOrderedMergedScalarRangeFieldsBuf(
	orderField string,
	leaves *pooled.SliceBuf[qx.Expr],
	dst *pooled.SliceBuf[orderedMergedScalarRangeField],
) bool {
	dst.Truncate()
	for i := 0; i < leaves.Len(); i++ {
		e := leaves.Get(i)
		if !qv.isPositiveOrderedNumericRangeLeaf(e, orderField) {
			continue
		}
		rb, ok, err := qv.rangeBoundsForScalarExpr(e)
		if err != nil || !ok {
			return false
		}
		idx := findOrderedMergedScalarRangeField(dst, e.Field)
		if idx < 0 {
			dst.Append(orderedMergedScalarRangeField{
				field:  e.Field,
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

func (qv *queryView[K, V]) collectMergedNumericRangeFields(
	leaves []qx.Expr,
	dst *pooled.SliceBuf[orderedMergedScalarRangeField],
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
		idx := findOrderedMergedScalarRangeField(dst, e.Field)
		if idx < 0 {
			dst.Append(orderedMergedScalarRangeField{
				field:  e.Field,
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
		if src.loNumeric {
			dst.applyLoIndex(src.loIndex, src.loInc)
		} else {
			dst.applyLo(src.loKey, src.loInc)
		}
	}
	if src.hasHi {
		if src.hiNumeric {
			dst.applyHiIndex(src.hiIndex, src.hiInc)
		} else {
			dst.applyHi(src.hiKey, src.hiInc)
		}
	}
	if src.hasPrefix {
		dst.applyPrefix(src.prefix)
	}
	if src.has {
		dst.has = true
	}
}

func (core *preparedScalarRangePredicate[K, V]) runtimeReuse(est uint64, useComplement bool) materializedPredReuse {
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

func (core *preparedScalarRangePredicate[K, V]) planBase(slice []index) (preparedBaseRangePredicatePlan, predicate, bool) {
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

func (core *preparedScalarRangePredicate[K, V]) planOverlay(ov fieldOverlay) (preparedOverlayRangePredicatePlan, predicate, bool) {
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

func (core *preparedScalarRangePredicate[K, V]) buildFromSlice(slice []index, allowMaterialize bool, lazyColdMaterialize bool) (predicate, bool) {
	plan, pred, done := core.planBase(slice)
	if done {
		return pred, true
	}

	if cached, ok := core.loadReuse.load(); ok {
		return materializedRangePredicateWithMode(core.expr, cached), true
	}

	probe := newBaseRangeProbe(slice, plan.start, plan.end, plan.useComplement)
	coldMaterializeAllowed := allowMaterialize
	if coldMaterializeAllowed && lazyColdMaterialize && core.usePostingFilter &&
		rangePostingFilterMaterializeAfterForProbe(probe.probeLen, probe.probeEst) > 1 {
		coldMaterializeAllowed = false
	}

	if allowMaterialize {
		ov := fieldOverlay{base: slice}
		br := overlayRange{
			baseStart: plan.start,
			baseEnd:   plan.end,
		}
		if coldMaterializeAllowed {
			if out, ok := core.qv.tryEvalNumericRangeBuckets(core.expr.Field, core.fm, ov, br); ok {
				out.ids = core.sharedReuse.share(out.ids)
				return materializedRangePredicateWithMode(core.expr, out.ids), true
			}
		} else if out, ok := core.qv.tryLoadNumericRangeBuckets(core.expr.Field, core.fm, ov, br); ok {
			out.ids = core.sharedReuse.share(out.ids)
			return materializedRangePredicateWithMode(core.expr, out.ids), true
		}
	}
	reuse := core.runtimeReuse(plan.est, plan.useComplement)
	if allowMaterialize && !plan.useComplement && core.fm != nil && !isNumericScalarKind(core.fm.Kind) {
		reuse = core.sharedReuse
	}
	keepProbeHits := probe.useComplement == core.expr.Not
	materializeAfter := rangeMaterializeAfterForProbe(probe.probeLen, probe.probeEst)
	state := baseRangePredicateStatePool.Get()
	state.probe = probe
	state.reuse = reuse
	state.keepProbeHits = keepProbeHits
	state.neg = core.expr.Not
	state.linearContainsMax = rangeLinearContainsLimit(probe.probeLen, probe.probeEst)
	state.hashSetAfter = rangeHashSetAfterForProbe(probe.probeLen, probe.probeEst)
	state.materializeAfter = materializeAfter
	state.postingFilterMaterializeAt = rangePostingFilterMaterializeAfterForProbe(probe.probeLen, probe.probeEst)
	state.setExpectedContainsCalls(materializeAfter)
	postingFilterCheap := false
	if core.usePostingFilter && probe.probeLen != 0 {
		postingFilterCheap = rangeProbeSupportsCheapPostingFilter(state.keepProbeHits, probe.probeLen)
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

func (core *preparedScalarRangePredicate[K, V]) buildFromOverlay(ov fieldOverlay, allowMaterialize bool, lazyColdMaterialize bool) (predicate, bool) {
	plan, pred, done := core.planOverlay(ov)
	if done {
		return pred, true
	}

	if cached, ok := core.loadReuse.load(); ok {
		return materializedRangePredicateWithMode(core.expr, cached), true
	}

	probeLen := -1
	probeEst := uint64(0)
	if !plan.useComplement {
		probeLen = plan.runtimeProbeBuckets
		probeEst = plan.runtimeProbeEst
	}
	probe := newOverlayRangeProbe(ov, plan.br, plan.useComplement, probeLen, probeEst)

	coldMaterializeAllowed := allowMaterialize
	if coldMaterializeAllowed && lazyColdMaterialize && core.usePostingFilter {
		totalBuckets := probe.ov.keyCount()
		inBuckets := 0
		for i := 0; i < probe.spanCnt; i++ {
			inBuckets += probe.spans[i].baseEnd - probe.spans[i].baseStart
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
		if coldMaterializeAllowed {
			if out, ok := core.qv.tryEvalNumericRangeBuckets(core.expr.Field, core.fm, ov, plan.br); ok {
				out.ids = core.sharedReuse.share(out.ids)
				return materializedRangePredicateWithMode(core.expr, out.ids), true
			}
		} else if out, ok := core.qv.tryLoadNumericRangeBuckets(core.expr.Field, core.fm, ov, plan.br); ok {
			out.ids = core.sharedReuse.share(out.ids)
			return materializedRangePredicateWithMode(core.expr, out.ids), true
		}
	}
	reuse := core.runtimeReuse(plan.est, plan.useComplement)
	if allowMaterialize && !plan.useComplement && core.fm != nil && !isNumericScalarKind(core.fm.Kind) {
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
		totalBuckets := probe.ov.keyCount()
		inBuckets := 0
		for i := 0; i < probe.spanCnt; i++ {
			inBuckets += probe.spans[i].baseEnd - probe.spans[i].baseStart
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

func (core *preparedScalarRangePredicate[K, V]) evalMaterializedPostingResult(ov fieldOverlay) postingResult {
	if cached, ok := core.loadReuse.load(); ok {
		if cached.IsEmpty() {
			return postingResult{}
		}
		return postingResult{ids: cached}
	}

	br := ov.rangeForBounds(core.bounds)
	if overlayRangeEmpty(br) {
		if core.sharedReuse.snap != nil && !core.sharedReuse.cacheKey.isZero() {
			core.sharedReuse.snap.storeMaterializedPredKey(core.sharedReuse.cacheKey, posting.List{})
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

func (core *preparedScalarRangePredicate[K, V]) orderBasicMaterializationStats(universe uint64) (scalarMaterializationStats, bool) {
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
			stats.cacheKey = core.complementCacheKey
			stats.buildBuckets = complementBuckets
			stats.buildEst = complementEst
			stats.buildComplement = true
		}
	}

	return stats, true
}

func (core *preparedScalarRangePredicate[K, V]) loadWarmScalarPostingResult() (postingResult, bool) {
	stats, ok := core.orderBasicMaterializationStats(core.qv.snapshotUniverseCardinality())
	if !ok {
		return postingResult{}, false
	}
	if !stats.cacheKey.isZero() {
		if cached, ok := core.qv.snap.loadMaterializedPredKey(stats.cacheKey); ok {
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
	return materializedPredKeyForExactScalarRange(field, bounds).String()
}

func (qv *queryView[K, V]) loadWarmPreparedScalarExactRange(op preparedScalarExactRange) (postingResult, bool) {
	if !op.cacheKey.isZero() {
		if cached, ok := qv.snap.loadMaterializedPredKey(op.cacheKey); ok {
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
	if !op.cacheKey.isZero() {
		if cached, ok := qv.snap.loadMaterializedPredKey(op.cacheKey); ok {
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
		if !op.cacheKey.isZero() {
			out.ids = qv.tryShareMaterializedPred(op.cacheKey, out.ids)
		}
		return out, nil
	}
	ids := overlayUnionRange(ov, br)
	if !op.cacheKey.isZero() {
		ids = qv.tryShareMaterializedPred(op.cacheKey, ids)
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

func (core *preparedScalarRangePredicate[K, V]) prepareComplementMaterialization() (scalarComplementMaterializationPlan, bool) {
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

func addScalarComplementPlanSpan(plan *scalarComplementMaterializationPlan, ov fieldOverlay, span overlayRange) {
	if overlayRangeEmpty(span) {
		return
	}
	buckets, est := overlayRangeStats(ov, span)
	plan.buckets += buckets
	plan.est = satAddUint64(plan.est, est)
}

func (core *preparedScalarRangePredicate[K, V]) loadComplementMaterialization() (scalarComplementMaterializationPlan, posting.List, bool, bool, bool) {
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

func (core *preparedScalarRangePredicate[K, V]) materializeComplement(plan scalarComplementMaterializationPlan) posting.List {
	var ids posting.List
	ov := core.qv.fieldOverlay(core.expr.Field)
	var pendingBefore, pendingAfter overlayRange
	if !overlayRangeEmpty(plan.before) {
		if out, ok := core.qv.tryEvalNumericRangeBuckets(core.expr.Field, core.fm, ov, plan.before); ok {
			if ids.IsEmpty() {
				ids = out.ids
			} else {
				ids = ids.BuildMergedOwned(out.ids)
			}
		} else {
			pendingBefore = plan.before
		}
	}
	if !overlayRangeEmpty(plan.after) {
		if out, ok := core.qv.tryEvalNumericRangeBuckets(core.expr.Field, core.fm, ov, plan.after); ok {
			if ids.IsEmpty() {
				ids = out.ids
			} else {
				ids = ids.BuildMergedOwned(out.ids)
			}
		} else {
			pendingAfter = plan.after
		}
	}
	if !overlayRangeEmpty(pendingBefore) || !overlayRangeEmpty(pendingAfter) {
		if ids.IsEmpty() {
			ids = overlayUnionRanges(ov, pendingBefore, pendingAfter)
		} else {
			ids = mergeOverlayRangesInto(ids, ov, pendingBefore, pendingAfter)
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
