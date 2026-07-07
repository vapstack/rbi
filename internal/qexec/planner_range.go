package qexec

import (
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/mathutil"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/schema"
)

type preparedScalarRangePredicate struct {
	qv                 *View
	expr               qir.Expr
	fm                 *schema.Field
	bound              normalizedScalarBound
	bounds             indexdata.Bounds
	complementCacheKey qcache.MaterializedPredKey
	loadReuse          materializedPredReuse
	sharedReuse        materializedPredReuse
	secondHitReuse     materializedPredReuse
	usePostingFilter   bool
}

type preparedFieldIndexRangePredicatePlan struct {
	br                  indexdata.FieldIndexRange
	bucketCount         int
	est                 uint64
	useComplement       bool
	runtimeProbeBuckets int
	runtimeProbeEst     uint64
}

type scalarMaterializationStats struct {
	cacheKey        qcache.MaterializedPredKey
	probeBuckets    int
	probeEst        uint64
	buildBuckets    int
	buildEst        uint64
	buildComplement bool
}

type scalarComplementMaterializationPlan struct {
	sharedReuse materializedPredReuse
	before      indexdata.FieldIndexRange
	after       indexdata.FieldIndexRange
	nilPosting  posting.List
	buckets     int
	est         uint64
}

type preparedScalarExactRange struct {
	field        string
	fieldOrdinal int
	bounds       indexdata.Bounds
	cacheKey     qcache.MaterializedPredKey
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
	plan    preparedFieldIndexRangePredicatePlan
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
	ov      indexdata.FieldIndexView
	br      indexdata.FieldIndexRange
	hasData bool
}

type preparedScalarIndexSpan struct {
	ov      indexdata.FieldIndexView
	br      indexdata.FieldIndexRange
	hasData bool
}

func storeEmptyScalarComplementMaterialization(plan scalarComplementMaterializationPlan) {
	if plan.sharedReuse.snap == nil || plan.sharedReuse.cacheKey.IsZero() {
		return
	}
	plan.sharedReuse.snap.StoreMaterializedPredKey(plan.sharedReuse.cacheKey, posting.List{})
}

func (qv *View) classifyOrderFieldScalarLeaf(orderField string, e qir.Expr) orderFieldScalarLeafKind {
	if !qv.hasFieldOrdinal(e.FieldOrdinal) || qv.exec.FieldNameByOrdinal(e.FieldOrdinal) != orderField {
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

func isScalarEqOp(op qir.Op) bool {
	return op == qir.OpEQ
}

func isScalarEqOrInOp(op qir.Op) bool {
	return op == qir.OpEQ || op == qir.OpIN
}

func isScalarRangeEqOp(op qir.Op) bool {
	return isScalarEqOp(op) || op.IsNumericRange()
}

func (qv *View) isPositiveScalarEqLeaf(e qir.Expr) bool {
	return !e.Not && qv.hasFieldOrdinal(e.FieldOrdinal) && len(e.Operands) == 0 && isScalarEqOp(e.Op)
}

func (qv *View) isPositiveScalarPrefixLeaf(e qir.Expr) bool {
	return !e.Not && qv.hasFieldOrdinal(e.FieldOrdinal) && len(e.Operands) == 0 && e.Op == qir.OpPREFIX
}

func (qv *View) isPositiveNonOrderScalarPrefixLeaf(orderField string, e qir.Expr) bool {
	return qv.exec.FieldNameByOrdinal(e.FieldOrdinal) != orderField && qv.isPositiveScalarPrefixLeaf(e)
}

func (qv *View) isPositiveMergedNumericRangeLeaf(e qir.Expr) bool {
	if e.Not || !qv.hasFieldOrdinal(e.FieldOrdinal) || !isScalarRangeEqOp(e.Op) {
		return false
	}
	fm := qv.fieldMetaByOrdinal(e.FieldOrdinal)
	return schema.FieldUsesOrderedNumericKeys(fm)
}

func (qv *View) isSimpleScalarRangeOrPrefixLeaf(e qir.Expr) bool {
	if e.Not || !qv.hasFieldOrdinal(e.FieldOrdinal) {
		return false
	}
	return e.Op.IsScalarRangeOrPrefix()
}

func (qv *View) normalizedScalarBoundForExpr(e qir.Expr) (normalizedScalarBound, bool, error) {
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

func (qv *View) initPreparedScalarRangePredicateFromBound(
	core *preparedScalarRangePredicate,
	e qir.Expr,
	fm *schema.Field,
	bound normalizedScalarBound,
) {
	var (
		cacheKey           qcache.MaterializedPredKey
		complementCacheKey qcache.MaterializedPredKey
	)
	fieldName := qv.exec.FieldNameByOrdinal(e.FieldOrdinal)

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

func (qv *View) initPreparedExactScalarRangePredicate(
	core *preparedScalarRangePredicate,
	e qir.Expr,
	fm *schema.Field,
	bounds indexdata.Bounds,
) {

	cacheKey := qv.materializedPredKeyForExactScalarRange(qv.exec.FieldNameByOrdinal(e.FieldOrdinal), bounds)

	var complementCacheKey qcache.MaterializedPredKey
	if schema.FieldUsesOrderedNumericKeys(fm) && e.Op.IsNumericRange() {
		complementCacheKey = qv.materializedPredComplementKeyForExactScalarRange(qv.exec.FieldNameByOrdinal(e.FieldOrdinal), bounds)
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

func (qv *View) initPreparedScalarRangePredicate(core *preparedScalarRangePredicate, e qir.Expr, fm *schema.Field) (predicate, bool, bool) {

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

func (qv *View) prepareScalarRangeRoutingCandidate(e qir.Expr) (preparedScalarRangeRoutingCandidate, bool) {
	if e.Not || !qv.hasFieldOrdinal(e.FieldOrdinal) {
		return preparedScalarRangeRoutingCandidate{}, false
	}

	fm := qv.fieldMetaByOrdinal(e.FieldOrdinal)
	if fm == nil || fm.Slice {
		return preparedScalarRangeRoutingCandidate{}, false
	}

	var core preparedScalarRangePredicate
	_, done, ok := qv.initPreparedScalarRangePredicate(&core, e, fm)
	if !ok || done {
		return preparedScalarRangeRoutingCandidate{}, false
	}

	ov := qv.indexViewByOrdinal(e.FieldOrdinal)
	if !ov.HasData() {
		return preparedScalarRangeRoutingCandidate{}, false
	}

	plan, _, done := core.planFieldIndexRange(ov)
	if done {
		return preparedScalarRangeRoutingCandidate{}, false
	}

	return preparedScalarRangeRoutingCandidate{
		core:    core,
		plan:    plan,
		numeric: schema.FieldUsesOrderedNumericKeys(fm),
	}, true
}

func (qv *View) preparePredicateScalarRangeRoutingCandidate(p predicate) (preparedScalarRangeRoutingCandidate, bool) {
	if p.expr.Not || !qv.hasFieldOrdinal(p.expr.FieldOrdinal) {
		return preparedScalarRangeRoutingCandidate{}, false
	}

	fm := qv.fieldMetaByOrdinal(p.expr.FieldOrdinal)
	if fm == nil || fm.Slice {
		return preparedScalarRangeRoutingCandidate{}, false
	}

	if !p.hasEffectiveBounds {
		return qv.prepareScalarRangeRoutingCandidate(p.expr)
	}

	ov := qv.indexViewByOrdinal(p.expr.FieldOrdinal)
	if !ov.HasData() {
		return preparedScalarRangeRoutingCandidate{}, false
	}

	var core preparedScalarRangePredicate
	qv.initPreparedExactScalarRangePredicate(&core, p.expr, fm, p.effectiveBounds)
	plan, _, done := core.planFieldIndexRange(ov)
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
	universe := core.qv.snap.Universe.Cardinality()
	if universe == 0 {
		return false
	}
	ov := core.qv.indexViewByOrdinal(core.expr.FieldOrdinal)
	if !ov.HasData() {
		return false
	}
	plan, _, done := core.planFieldIndexRange(ov)
	if done {
		return false
	}
	return plan.orderedEagerMaterializeUseful(orderedWindow, universe)
}

func (qv *View) prepareScalarIndexSpan(e qir.Expr) (preparedScalarIndexSpan, bool, error) {
	if !qv.isSimpleScalarRangeOrPrefixLeaf(e) {
		return preparedScalarIndexSpan{}, false, nil
	}
	fm := qv.fieldMetaByOrdinal(e.FieldOrdinal)
	if fm == nil || fm.Slice {
		return preparedScalarIndexSpan{}, false, nil
	}
	rb, ok, err := qv.rangeBoundsForScalarExpr(e)
	if err != nil {
		return preparedScalarIndexSpan{}, false, err
	}
	if !ok || rb.Empty {
		return preparedScalarIndexSpan{}, false, nil
	}

	out := preparedScalarIndexSpan{
		ov: qv.indexViewByOrdinal(e.FieldOrdinal),
	}
	if !out.ov.HasData() {
		return out, true, nil
	}
	out.hasData = true
	out.br = out.ov.RangeForBounds(rb)
	return out, true, nil
}

func (qv *View) prepareScalarPrefixRoute(e qir.Expr) (preparedScalarPrefixSpan, bool, error) {
	if e.Not || e.Op != qir.OpPREFIX || !qv.hasFieldOrdinal(e.FieldOrdinal) {
		return preparedScalarPrefixSpan{}, false, nil
	}
	span, ok, err := qv.prepareScalarIndexSpan(e)
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

func (qv *View) applyScalarExprToRangeBounds(e qir.Expr, rb *indexdata.Bounds) (bool, bool) {
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

func (qv *View) rangeBoundsForScalarExpr(e qir.Expr) (indexdata.Bounds, bool, error) {
	bound, isSlice, err := qv.normalizedScalarBoundForExpr(e)
	if err != nil {
		return indexdata.Bounds{}, false, err
	}
	if isSlice {
		return indexdata.Bounds{}, false, nil
	}
	return rangeBoundsForNormalizedScalarBound(bound), true, nil
}

func findOrderedMergedScalarRangeField(groups []orderedMergedScalarRangeField, field string) int {
	for i := 0; i < len(groups); i++ {
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

func (qv *View) collectOrderedMergedScalarRangeFields(
	orderField string,
	leaves []qir.Expr,
	dst []orderedMergedScalarRangeField,
) ([]orderedMergedScalarRangeField, bool) {

	dst = dst[:0]
	for i, e := range leaves {
		if !qv.isPositiveOrderedMergedScalarRangeLeaf(e, orderField) {
			continue
		}
		rb, ok, err := qv.rangeBoundsForScalarExpr(e)
		if err != nil || !ok {
			return dst, false
		}
		fieldName := qv.exec.FieldNameByOrdinal(e.FieldOrdinal)
		idx := findOrderedMergedScalarRangeField(dst, fieldName)
		if idx < 0 {
			dst = append(dst, orderedMergedScalarRangeField{
				field:  fieldName,
				expr:   e,
				bounds: rb,
				first:  i,
				count:  1,
			})
			continue
		}
		group := dst[idx]
		mergeRangeBounds(&group.bounds, rb)
		group.count++
		dst[idx] = group
	}
	return dst, true
}

func (qv *View) collectMergedNumericRangeFields(
	leaves []qir.Expr,
	dst []orderedMergedScalarRangeField,
) ([]orderedMergedScalarRangeField, bool) {

	dst = dst[:0]
	for i, e := range leaves {
		if !qv.isPositiveMergedNumericRangeLeaf(e) {
			continue
		}
		rb, ok, err := qv.rangeBoundsForScalarExpr(e)
		if err != nil || !ok {
			return dst, false
		}

		fieldName := qv.exec.FieldNameByOrdinal(e.FieldOrdinal)
		idx := findOrderedMergedScalarRangeField(dst, fieldName)
		if idx < 0 {
			dst = append(dst, orderedMergedScalarRangeField{
				field:  fieldName,
				expr:   e,
				bounds: rb,
				first:  i,
				count:  1,
			})
			continue
		}
		group := dst[idx]
		mergeRangeBounds(&group.bounds, rb)
		group.count++
		dst[idx] = group
	}
	return dst, true
}

func mergeRangeBounds(dst *indexdata.Bounds, src indexdata.Bounds) {
	if !dst.Has {
		*dst = src
		normalizeAdjacentStringPrefixBounds(dst)
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
	normalizeAdjacentStringPrefixBounds(dst)
}

func normalizeAdjacentStringPrefixBounds(b *indexdata.Bounds) {
	if b.Empty || b.HasPrefix || !b.HasLo || !b.HasHi || !b.LoInc || b.HiInc || b.LoNumeric || b.HiNumeric || b.LoKey == "" {
		return
	}
	upper, ok := keycodec.NewPrefixUpperBound(b.LoKey)
	if !ok || keycodec.CompareStringPrefixUpperBound(b.HiKey, upper) != 0 {
		return
	}
	b.ApplyPrefix(b.LoKey)
}

func boundsExactStringPrefix(b indexdata.Bounds) bool {
	if b.Empty || !b.HasPrefix || b.Prefix == "" {
		return false
	}
	if b.HasLo && (b.LoNumeric || !b.LoInc || b.LoKey != b.Prefix) {
		return false
	}
	if !b.HasHi {
		return true
	}
	if b.HiNumeric || b.HiInc {
		return false
	}
	upper, ok := keycodec.NewPrefixUpperBound(b.Prefix)
	return ok && keycodec.CompareStringPrefixUpperBound(b.HiKey, upper) == 0
}

func (qv *View) mergeAdjacentStringPrefixLeaves(excludedField string, a, b qir.Expr) (qir.Expr, bool, error) {
	if a.Not || b.Not ||
		!qv.hasFieldOrdinal(a.FieldOrdinal) ||
		a.FieldOrdinal != b.FieldOrdinal ||
		len(a.Operands) != 0 ||
		len(b.Operands) != 0 ||
		!isScalarRangeEqOp(a.Op) ||
		!isScalarRangeEqOp(b.Op) {
		return qir.Expr{}, false, nil
	}
	if qv.exec.FieldNameByOrdinal(a.FieldOrdinal) == excludedField {
		return qir.Expr{}, false, nil
	}
	fm := qv.fieldMetaByOrdinal(a.FieldOrdinal)
	if fm == nil || fm.Slice {
		return qir.Expr{}, false, nil
	}
	if !fm.UseVI && fm.KeyKind == schema.FieldWriteKeysString {
		lo := ""
		hi := ""
		var loValue any
		if a.Op == qir.OpGTE && b.Op == qir.OpLT {
			var ok bool
			lo, ok = a.Value.(string)
			if ok {
				hi, ok = b.Value.(string)
			}
			if !ok {
				lo = ""
			} else {
				loValue = a.Value
			}
		} else if b.Op == qir.OpGTE && a.Op == qir.OpLT {
			var ok bool
			lo, ok = b.Value.(string)
			if ok {
				hi, ok = a.Value.(string)
			}
			if !ok {
				lo = ""
			} else {
				loValue = b.Value
			}
		}
		if lo != "" {
			upper, ok := keycodec.NewPrefixUpperBound(lo)
			if ok && keycodec.CompareStringPrefixUpperBound(hi, upper) == 0 {
				out := a
				out.Op = qir.OpPREFIX
				out.Value = loValue
				return out, true, nil
			}
		}
	}
	rb0, ok0, err := qv.rangeBoundsForScalarExpr(a)
	if err != nil {
		return qir.Expr{}, false, err
	}
	rb1, ok1, err := qv.rangeBoundsForScalarExpr(b)
	if err != nil {
		return qir.Expr{}, false, err
	}
	if !ok0 || !ok1 {
		return qir.Expr{}, false, nil
	}
	var bounds indexdata.Bounds
	mergeRangeBounds(&bounds, rb0)
	mergeRangeBounds(&bounds, rb1)
	if !bounds.HasPrefix || bounds.Prefix == "" {
		return qir.Expr{}, false, nil
	}
	out := a
	out.Op = qir.OpPREFIX
	out.Value = bounds.Prefix
	return out, true, nil
}

func (core *preparedScalarRangePredicate) runtimeReuse(est uint64, useComplement bool) materializedPredReuse {
	if useComplement {
		return newMaterializedPredReadOnlyReuse(core.qv.snap, core.complementCacheKey)
	}
	stateReuse := core.loadReuse
	if core.secondHitReuse.canSecondHitShareModeratelyOversizedEstimate(est) &&
		allowRuntimePositiveRangeSecondHitShare(est, core.qv.snap.Universe.Cardinality()) {
		stateReuse = core.secondHitReuse
	}
	return stateReuse
}

func (core *preparedScalarRangePredicate) hasNilTail() bool {
	return core.fm != nil &&
		schema.FieldUsesNilIndex(core.fm) &&
		core.qv.nilIndexViewByOrdinal(core.expr.FieldOrdinal).LookupCardinality(indexdata.NilIndexEntryKey) > 0
}

func (core *preparedScalarRangePredicate) usesRuntimeComplement(useComplement bool) bool {
	return useComplement && !core.hasNilTail()
}

func (core *preparedScalarRangePredicate) planFieldIndexRange(ov indexdata.FieldIndexView) (preparedFieldIndexRangePredicatePlan, predicate, bool) {
	br := ov.RangeForBounds(core.bounds)
	if br.Empty() {
		if core.expr.Not {
			return preparedFieldIndexRangePredicatePlan{}, predicate{expr: core.expr, alwaysTrue: true}, true
		}
		return preparedFieldIndexRangePredicatePlan{}, predicate{expr: core.expr, alwaysFalse: true}, true
	}

	bucketCount, est := ov.RangeStats(br)
	if bucketCount == 0 {
		if core.expr.Not {
			return preparedFieldIndexRangePredicatePlan{}, predicate{expr: core.expr, alwaysTrue: true}, true
		}
		return preparedFieldIndexRangePredicatePlan{}, predicate{expr: core.expr, alwaysFalse: true}, true
	}

	totalBuckets := ov.KeyCount()
	ptrHasNilTail := core.hasNilTail()
	fullSpanHasNilTail := bucketCount == totalBuckets && ptrHasNilTail
	if bucketCount == totalBuckets && !fullSpanHasNilTail {
		if core.expr.Not {
			return preparedFieldIndexRangePredicatePlan{}, predicate{expr: core.expr, alwaysFalse: true}, true
		}
		return preparedFieldIndexRangePredicatePlan{}, predicate{expr: core.expr, alwaysTrue: true}, true
	}

	plan := preparedFieldIndexRangePredicatePlan{
		br:                  br,
		bucketCount:         bucketCount,
		est:                 est,
		useComplement:       br.ExactRankSpan() && totalBuckets-bucketCount < bucketCount,
		runtimeProbeBuckets: bucketCount,
		runtimeProbeEst:     est,
	}

	if core.usesRuntimeComplement(plan.useComplement) {
		plan.runtimeProbeBuckets = totalBuckets - bucketCount
		universe := core.qv.snap.Universe.Cardinality()
		if universe > est {
			plan.runtimeProbeEst = universe - est
		} else {
			plan.runtimeProbeEst = 0
		}
		if core.qv.nilIndexViewByOrdinal(core.expr.FieldOrdinal).LookupCardinality(indexdata.NilIndexEntryKey) > 0 {
			plan.runtimeProbeBuckets++
		}
		if plan.runtimeProbeBuckets == 0 && plan.runtimeProbeEst > 0 {
			plan.runtimeProbeBuckets = 1
		}
	}

	return plan, predicate{}, false
}

func (core *preparedScalarRangePredicate) buildFromFieldIndexRange(
	ov indexdata.FieldIndexView,
	allowMaterialize, lazyColdMaterialize, allowWarmLoad bool,
) (predicate, bool) {

	plan, pred, done := core.planFieldIndexRange(ov)
	if done {
		return pred, true
	}

	var numericSpan numericRangeBucketSpan
	if !core.expr.Not && core.expr.Op.IsNumericRange() {
		numericSpan, _ = core.qv.numericRangeBucketSpan(core.qv.exec.FieldNameByOrdinal(core.expr.FieldOrdinal), core.expr.FieldOrdinal, core.fm, ov, plan.br)
	}

	if allowWarmLoad {
		if numericSpan.entry != nil {
			if out, ok := loadNumericRangeBucketSpan(ov, plan.br, numericSpan); ok {
				return materializedRangePredicateWithMode(core.expr, out.ids), true
			}
		}
		if cached, ok := core.loadReuse.load(); ok {
			return materializedRangePredicateWithMode(core.expr, cached), true
		}
	}

	useRuntimeComplement := core.usesRuntimeComplement(plan.useComplement)
	probeLen := plan.bucketCount
	probeEst := plan.est
	if useRuntimeComplement {
		probeLen = plan.runtimeProbeBuckets
		probeEst = plan.runtimeProbeEst
	}
	probe := newFieldIndexRangeProbe(ov, plan.br, useRuntimeComplement, probeLen, probeEst)

	allowPositiveMaterialize := allowMaterialize
	if useRuntimeComplement && !core.qv.snap.AllowsMaterializedPredCard(plan.est) {
		allowPositiveMaterialize = false
	}

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

	if core.expr.Not && core.expr.Op.IsNumericRange() {
		numericSpan, _ = core.qv.numericRangeBucketSpan(core.qv.exec.FieldNameByOrdinal(core.expr.FieldOrdinal), core.expr.FieldOrdinal, core.fm, ov, plan.br)
	}

	if allowMaterialize && core.expr.Op.IsNumericRange() && (allowPositiveMaterialize || core.qv.snap.MaterializedPredCacheLimit() <= 0) {
		if numericSpan.entry != nil {
			if coldMaterializeAllowed {
				out := evalNumericRangeBucketSpan(ov, plan.br, numericSpan)
				return materializedRangePredicateWithMode(core.expr, out.ids), true
			} else if out, ok := loadNumericRangeBucketSpan(ov, plan.br, numericSpan); ok {
				return materializedRangePredicateWithMode(core.expr, out.ids), true
			}
		}
	}

	reuse := core.runtimeReuse(plan.est, useRuntimeComplement)
	if allowMaterialize && !useRuntimeComplement &&
		(!schema.FieldUsesOrderedNumericKeys(core.fm) || (core.usePostingFilter && numericSpan.entry == nil)) {
		reuse = core.sharedReuse
	}

	materializeAfter := rangeMaterializeAfterForProbe(probe.probeLen, probe.probeEst)
	state := fieldIndexRangePredicateStatePool.Get()
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
	state.numericSpan = numericSpan

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
			expr:                 core.expr,
			fieldIndexRangeState: state,
			postingFilterCheap:   state.postingFilterCheap,
		}, true
	}

	return predicate{
		expr:                 core.expr,
		fieldIndexRangeState: state,
		estCard:              plan.est,
		postingFilterCheap:   state.postingFilterCheap,
	}, true
}

func (core *preparedScalarRangePredicate) evalMaterializedPostingResult(ov indexdata.FieldIndexView) postingResult {
	if cached, ok := core.loadReuse.load(); ok {
		if cached.IsEmpty() {
			return postingResult{}
		}
		return postingResult{ids: cached}
	}

	br := ov.RangeForBounds(core.bounds)
	if br.Empty() {
		if core.sharedReuse.snap != nil && !core.sharedReuse.cacheKey.IsZero() {
			core.sharedReuse.snap.StoreMaterializedPredKey(core.sharedReuse.cacheKey, posting.List{})
		}
		return postingResult{}
	}

	if core.expr.Op != qir.OpPREFIX {
		if out, ok := core.qv.tryEvalNumericRangeBuckets(core.qv.exec.FieldNameByOrdinal(core.expr.FieldOrdinal), core.expr.FieldOrdinal, core.fm, ov, br); ok {
			if out.ids.IsEmpty() {
				return postingResult{}
			}
			return out
		}
	}

	ids := ov.UnionRangePostings(br, indexdata.FieldIndexRange{})
	ids = core.sharedReuse.share(ids)
	if ids.IsEmpty() {
		return postingResult{}
	}
	return postingResult{ids: ids}
}

func (core *preparedScalarRangePredicate) orderBasicMaterializationStats(universe uint64) (scalarMaterializationStats, bool) {
	if core.expr.Not || !core.qv.hasFieldOrdinal(core.expr.FieldOrdinal) {
		return scalarMaterializationStats{}, false
	}

	ov := core.qv.indexViewByOrdinal(core.expr.FieldOrdinal)
	if !ov.HasData() {
		return scalarMaterializationStats{}, false
	}
	plan, _, done := core.planFieldIndexRange(ov)
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

	useRuntimeComplement := core.usesRuntimeComplement(plan.useComplement)
	if plan.useComplement {
		complementBuckets := ov.KeyCount() - plan.bucketCount
		complementEst := uint64(0)
		if universe > plan.est {
			complementEst = universe - plan.est
		}
		if core.qv.nilIndexViewByOrdinal(core.expr.FieldOrdinal).LookupCardinality(indexdata.NilIndexEntryKey) > 0 {
			complementBuckets++
		}
		if complementBuckets == 0 && complementEst > 0 {
			complementBuckets = 1
		}

		if useRuntimeComplement {
			stats.probeBuckets = complementBuckets
			stats.probeEst = complementEst
		}

		if core.expr.Op.IsNumericRange() {
			stats.cacheKey = core.complementCacheKey
			stats.buildBuckets = complementBuckets
			stats.buildEst = complementEst
			stats.buildComplement = true
		}
	}

	return stats, true
}

func (core *preparedScalarRangePredicate) loadWarmScalarPostingResult() (postingResult, bool) {
	stats, ok := core.orderBasicMaterializationStats(core.qv.snap.Universe.Cardinality())
	if !ok {
		return postingResult{}, false
	}
	if !stats.cacheKey.IsZero() {
		if cached, ok := core.qv.snap.LoadMaterializedPredKey(stats.cacheKey); ok {
			return postingResult{ids: cached, neg: stats.buildComplement}, true
		}
	}

	ov := core.qv.indexViewByOrdinal(core.expr.FieldOrdinal)
	if !ov.HasData() {
		return postingResult{}, false
	}
	plan, _, done := core.planFieldIndexRange(ov)
	if done {
		return postingResult{}, false
	}
	if out, ok := core.qv.tryLoadNumericRangeBuckets(core.qv.exec.FieldNameByOrdinal(core.expr.FieldOrdinal), core.expr.FieldOrdinal, core.fm, ov, plan.br); ok {
		return out, true
	}
	return postingResult{}, false
}

func (qv *View) loadWarmPreparedScalarExactRange(op preparedScalarExactRange) (postingResult, bool) {
	fm := qv.fieldMeta(op.field, op.fieldOrdinal)
	if schema.FieldUsesOrderedNumericKeys(fm) {
		ov := qv.indexViewByOrdinal(op.fieldOrdinal)
		if ov.HasData() {
			br := ov.RangeForBounds(op.bounds)
			if !br.Empty() {
				if out, ok := qv.tryLoadNumericRangeBuckets(op.field, op.fieldOrdinal, fm, ov, br); ok {
					return out, true
				}
			}
		}
	}

	if !op.cacheKey.IsZero() {
		if cached, ok := qv.snap.LoadMaterializedPredKey(op.cacheKey); ok {
			return postingResult{ids: cached}, true
		}
	}
	return postingResult{}, false
}

func (qv *View) evalPreparedScalarExactRange(op preparedScalarExactRange) (postingResult, error) {
	fm := qv.fieldMeta(op.field, op.fieldOrdinal)
	ov := qv.indexViewByOrdinal(op.fieldOrdinal)
	if !ov.HasData() {
		return postingResult{}, nil
	}

	br := ov.RangeForBounds(op.bounds)
	if br.Empty() {
		return postingResult{}, nil
	}

	if schema.FieldUsesOrderedNumericKeys(fm) {
		if out, ok := qv.tryEvalNumericRangeBuckets(op.field, op.fieldOrdinal, fm, ov, br); ok {
			return out, nil
		}
	}

	if !op.cacheKey.IsZero() {
		if cached, ok := qv.snap.LoadMaterializedPredKey(op.cacheKey); ok {
			return postingResult{ids: cached}, nil
		}
	}

	ids := ov.UnionRangePostings(br, indexdata.FieldIndexRange{})
	if !op.cacheKey.IsZero() {
		ids = qv.tryShareMaterializedPred(op.cacheKey, ids)
	}

	return postingResult{ids: ids}, nil
}

func (qv *View) shouldScheduleObservedPreparedScalarExactRange(op preparedScalarExactRange, observedRows, needWindow uint64) bool {
	if observedRows == 0 {
		return false
	}
	if needWindow == 0 {
		needWindow = 1
	}
	if observedRows <= needWindow {
		return false
	}
	ov := qv.indexViewByOrdinal(op.fieldOrdinal)
	if !ov.HasData() {
		return false
	}
	br := ov.RangeForBounds(op.bounds)
	if br.Empty() {
		return false
	}
	buckets, est := ov.RangeStats(br)
	if buckets == 0 || est == 0 {
		return false
	}
	fm := qv.fieldMeta(op.field, op.fieldOrdinal)
	universe := qv.snap.Universe.Cardinality()
	nilTail := schema.FieldUsesNilIndex(fm) &&
		qv.nilIndexViewByOrdinal(op.fieldOrdinal).LookupCardinality(indexdata.NilIndexEntryKey) > 0
	totalBuckets := ov.KeyCount()
	useComplementProbe := !nilTail && totalBuckets > buckets && totalBuckets-buckets < buckets
	if schema.FieldUsesOrderedNumericKeys(fm) && useComplementProbe {
		if universe > est && needWindow < universe-est {
			return false
		}
	}
	buildWork := rangeProbeMaterializeWork(buckets, est)
	if buildWork == 0 {
		return false
	}

	var numericPlan asyncMaterializedPredPlan
	numericHandled := false
	if plan, ok, handled := qv.asyncNumericRangePlanForBounds(op.fieldOrdinal, op.bounds); handled {
		if !ok {
			return false
		}
		numericPlan = plan
		numericHandled = true
	} else if !qv.materializedPredObservationCandidate(op.cacheKey, est, buildWork) {
		return false
	}

	probeBuckets := buckets
	probeEst := est
	if useComplementProbe {
		probeBuckets = totalBuckets - buckets
		if universe > est {
			probeEst = universe - est
		} else {
			probeEst = 0
		}
		if probeBuckets == 0 && probeEst > 0 {
			probeBuckets = 1
		}
	}
	probeWork := rangeAdaptiveProbeWorkForRows(observedRows, probeBuckets, probeEst)
	if probeWork == 0 {
		return false
	}
	if numericHandled {
		return qv.shouldPromoteObservedNumericWarmup(numericPlan, probeWork, buildWork)
	}
	return qv.snap.ShouldPromoteObservedMaterializedPredKey(op.cacheKey, probeWork, asyncMaterializedPredObservedThreshold(buildWork))
}

func (core *preparedScalarRangePredicate) prepareComplementMaterialization() (scalarComplementMaterializationPlan, bool) {
	if !core.expr.Op.IsNumericRange() {
		return scalarComplementMaterializationPlan{}, false
	}

	ov := core.qv.indexViewByOrdinal(core.expr.FieldOrdinal)
	if !ov.HasData() {
		return scalarComplementMaterializationPlan{}, false
	}

	br := ov.RangeForBounds(core.bounds)
	before, after, ok := fieldIndexComplementRangeSpans(ov, br)
	if !ok {
		return scalarComplementMaterializationPlan{}, false
	}
	nilPosting := core.qv.nilIndexViewByOrdinal(core.expr.FieldOrdinal).LookupPostingRetained(indexdata.NilIndexEntryKey)

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
		plan.est = mathutil.SatAddUint64(plan.est, nilPosting.Cardinality())
	}

	return plan, true
}

func addScalarComplementPlanSpan(plan *scalarComplementMaterializationPlan, ov indexdata.FieldIndexView, span indexdata.FieldIndexRange) {
	if span.Empty() {
		return
	}
	buckets, est := ov.RangeStats(span)
	plan.buckets += buckets
	plan.est = mathutil.SatAddUint64(plan.est, est)
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
	ov := core.qv.indexViewByOrdinal(core.expr.FieldOrdinal)
	fieldName := core.qv.exec.FieldNameByOrdinal(core.expr.FieldOrdinal)

	var (
		ids           posting.List
		pendingBefore indexdata.FieldIndexRange
		pendingAfter  indexdata.FieldIndexRange
	)
	if !plan.before.Empty() {
		if out, ok := core.qv.tryEvalNumericRangeBuckets(fieldName, core.expr.FieldOrdinal, core.fm, ov, plan.before); ok {
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
		if out, ok := core.qv.tryEvalNumericRangeBuckets(fieldName, core.expr.FieldOrdinal, core.fm, ov, plan.after); ok {
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

func (plan preparedFieldIndexRangePredicatePlan) orderedEagerMaterializeUseful(orderedWindow int, universe uint64) bool {
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
	retainedPenalty := mathutil.SatMulUint64(plan.est, postingContainsLookupWork(plan.est))
	return probeWork >= mathutil.SatAddUint64(buildWork, retainedPenalty)
}
