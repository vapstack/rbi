package rbi

import (
	"math"
	"math/bits"
	"strconv"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/posting"
)

type predicate struct {
	expr qx.Expr

	effectiveBounds    rangeBounds
	hasEffectiveBounds bool

	contains func(uint64) bool
	iter     func() posting.Iterator
	estCard  uint64

	kind           predicateKind
	iterKind       predicateIterKind
	posting        posting.List
	posts          []posting.List
	ids            posting.List
	rangeMat       bool
	postsBuf       *postingSliceBuf
	releaseIDs     bool
	postsAnyState  *postsAnyFilterState
	baseRangeState *baseRangePredicateState
	overlayState   *overlayRangePredicateState
	lazyMatState   *lazyMaterializedPredicateState

	alwaysTrue  bool
	alwaysFalse bool
	covered     bool

	// exact bucket match count when known; ok=false means unknown.
	bucketCount func(posting.List) (uint64, bool)
	// exact posting application when available without full predicate
	// materialization.
	postingFilter      func(posting.List) (posting.List, bool)
	postingFilterCheap bool
}

type predicateKind uint8

const (
	predicateKindCustom predicateKind = iota
	predicateKindPosting
	predicateKindPostingNot
	predicateKindPostsAny
	predicateKindPostsAnyNot
	predicateKindPostsAll
	predicateKindPostsAllNot
	predicateKindMaterialized
	predicateKindMaterializedNot
)

type predicateIterKind uint8

const (
	predicateIterNone predicateIterKind = iota
	predicateIterPosting
	predicateIterPostsConcat
	predicateIterPostsUnion
	predicateIterMaterialized
)

const predicatePostsAnyNotExactPostingMaxTerms = 2
const predicateSlicePoolMinLen = 3

type predicatePostingFilterCapability uint8

const (
	predicatePostingFilterApply predicatePostingFilterCapability = 1 << iota
	predicatePostingFilterExactBucket
	predicatePostingFilterPreferExact
	predicatePostingFilterCheap
)

func (cap predicatePostingFilterCapability) supportsApply() bool {
	return cap&predicatePostingFilterApply != 0
}

func (cap predicatePostingFilterCapability) supportsExactBucket() bool {
	return cap&predicatePostingFilterExactBucket != 0
}

func (cap predicatePostingFilterCapability) prefersExactBucket() bool {
	return cap&predicatePostingFilterPreferExact != 0
}

func (cap predicatePostingFilterCapability) isCheap() bool {
	return cap&predicatePostingFilterCheap != 0
}

func (p *predicate) hasRuntimeRangeState() bool {
	return p.baseRangeState != nil || p.overlayState != nil
}

func (p *predicate) isMaterializedLike() bool {
	return p.rangeMat || p.kind == predicateKindMaterialized || p.kind == predicateKindMaterializedNot
}

func (p *predicate) isCustomUnmaterialized() bool {
	return !p.rangeMat && p.kind == predicateKindCustom
}

func (p *predicate) setExpectedContainsCalls(expected int) {
	if p == nil {
		return
	}
	if p.baseRangeState != nil {
		p.baseRangeState.setExpectedContainsCalls(expected)
	}
	if p.overlayState != nil {
		p.overlayState.setExpectedContainsCalls(expected)
	}
}

func (p *predicate) runtimeRangeIter() (posting.Iterator, bool) {
	if p.baseRangeState != nil {
		return p.baseRangeState.newIter(), true
	}
	if p.overlayState != nil {
		return p.overlayState.newIter(), true
	}
	return nil, false
}

func (p *predicate) runtimeRangeMatches(idx uint64) (bool, bool) {
	if p.baseRangeState != nil {
		return p.baseRangeState.matches(idx), true
	}
	if p.overlayState != nil {
		return p.overlayState.matches(idx), true
	}
	return false, false
}

func (p *predicate) runtimeRangeCountBucket(bucket posting.List) (uint64, bool, bool) {
	if p.baseRangeState != nil {
		in, ok := p.baseRangeState.countBucket(bucket)
		return in, ok, true
	}
	if p.overlayState != nil {
		in, ok := p.overlayState.countBucket(bucket)
		return in, ok, true
	}
	return 0, false, false
}

func (p *predicate) runtimeRangeApply(dst posting.List) (posting.List, bool, bool) {
	if p.baseRangeState != nil {
		next, ok := p.baseRangeState.applyToPosting(dst)
		return next, ok, true
	}
	if p.overlayState != nil {
		next, ok := p.overlayState.applyToPosting(dst)
		return next, ok, true
	}
	return posting.List{}, false, false
}

func (p predicate) postingFilterCapability() predicatePostingFilterCapability {
	if p.alwaysTrue || p.alwaysFalse || p.covered {
		return predicatePostingFilterApply
	}
	if p.rangeMat {
		return predicatePostingFilterApply |
			predicatePostingFilterExactBucket |
			predicatePostingFilterCheap
	}
	if p.hasRuntimeRangeState() {
		cap := predicatePostingFilterApply
		if p.postingFilterCheap {
			cap |= predicatePostingFilterExactBucket |
				predicatePostingFilterPreferExact |
				predicatePostingFilterCheap
		}
		return cap
	}
	switch p.kind {
	case predicateKindPosting,
		predicateKindPostingNot,
		predicateKindPostsAll,
		predicateKindMaterialized,
		predicateKindMaterializedNot:
		return predicatePostingFilterApply |
			predicatePostingFilterExactBucket |
			predicatePostingFilterCheap
	case predicateKindPostsAnyNot:
		if len(p.posts) > predicatePostsAnyNotExactPostingMaxTerms {
			return 0
		}
		return predicatePostingFilterApply |
			predicatePostingFilterExactBucket |
			predicatePostingFilterCheap
	case predicateKindPostsAny:
		if p.postsAnyState != nil || p.postingFilter != nil {
			return predicatePostingFilterExactBucket |
				predicatePostingFilterPreferExact |
				predicatePostingFilterCheap
		}
		return 0
	default:
		if p.postingFilter != nil && p.postingFilterCheap {
			return predicatePostingFilterApply | predicatePostingFilterCheap
		}
		return 0
	}
}

func (p *predicate) hasContains() bool {
	if p.hasRuntimeRangeState() {
		return true
	}
	if p.lazyMatState != nil || p.rangeMat {
		return true
	}
	switch p.kind {
	case predicateKindPosting,
		predicateKindPostingNot,
		predicateKindPostsAny,
		predicateKindPostsAnyNot,
		predicateKindPostsAll,
		predicateKindPostsAllNot,
		predicateKindMaterialized,
		predicateKindMaterializedNot:
		return true
	default:
		return p.contains != nil
	}
}

func (p *predicate) hasIter() bool {
	if p.hasRuntimeRangeState() {
		return !p.expr.Not
	}
	if p.lazyMatState != nil || p.rangeMat {
		return !p.expr.Not
	}
	switch p.kind {
	case predicateKindMaterialized:
		return true
	case predicateKindMaterializedNot:
		return false
	}
	switch p.iterKind {
	case predicateIterPosting, predicateIterPostsConcat, predicateIterPostsUnion, predicateIterMaterialized:
		return true
	default:
		return p.iter != nil
	}
}

func (p *predicate) leadIterNeedsContainsCheck() bool {
	// HAS(list) uses a single posting as iterator seed and validates the rest
	// via contains checks; iterator alone is not exact when term count > 1.
	return p.kind == predicateKindPostsAll && len(p.posts) > 1
}

func (p predicate) supportsPostingApply() bool {
	return p.postingFilterCapability().supportsApply()
}

func (p predicate) supportsCheapPostingApply() bool {
	return p.postingFilterCapability().isCheap()
}

func (p predicate) supportsExactBucketPostingFilter() bool {
	return p.postingFilterCapability().supportsExactBucket()
}

func (p predicate) prefersExactBucketPostingFilter() bool {
	return p.postingFilterCapability().prefersExactBucket()
}

func (p *predicate) newIter() posting.Iterator {
	if it, ok := p.runtimeRangeIter(); ok {
		return it
	}
	switch p.kind {
	case predicateKindMaterialized:
		if p.ids.IsEmpty() {
			return emptyIter{}
		}
		return p.ids.Iter()
	case predicateKindMaterializedNot:
		return nil
	}
	if p.rangeMat {
		if p.expr.Not {
			return nil
		}
		if p.ids.IsEmpty() {
			return emptyIter{}
		}
		return p.ids.Iter()
	}
	if p.lazyMatState != nil {
		if p.expr.Not {
			return nil
		}
		ids := p.lazyMatState.materialize()
		if ids.IsEmpty() {
			return emptyIter{}
		}
		return ids.Iter()
	}
	switch p.iterKind {
	case predicateIterPosting:
		return p.posting.Iter()
	case predicateIterPostsConcat:
		return newPostingConcatIter(p.posts)
	case predicateIterPostsUnion:
		return newPostingUnionIter(p.posts)
	case predicateIterMaterialized:
		if p.ids.IsEmpty() {
			return nil
		}
		return p.ids.Iter()
	default:
		if p.iter == nil {
			return nil
		}
		return p.iter()
	}
}

func predicateLeadIterMayDuplicate(p predicate) bool {
	if p.hasRuntimeRangeState() {
		// Scalar range iterators walk disjoint value buckets; even complement
		// spans stay non-overlapping, so duplicate ids are not expected here.
		return false
	}
	if p.lazyMatState != nil || p.rangeMat {
		return false
	}
	switch p.kind {
	case predicateKindMaterialized, predicateKindMaterializedNot:
		return false
	}
	switch p.iterKind {
	case predicateIterPosting, predicateIterPostsUnion, predicateIterMaterialized:
		return false
	case predicateIterPostsConcat:
		return true
	default:
		return p.iter != nil
	}
}

func (p *predicate) matches(idx uint64) bool {
	if p.alwaysTrue {
		return true
	}
	if p.alwaysFalse {
		return false
	}
	if hit, ok := p.runtimeRangeMatches(idx); ok {
		return hit
	}
	switch p.kind {
	case predicateKindPosting:
		return p.posting.Contains(idx)
	case predicateKindPostingNot:
		return !p.posting.Contains(idx)
	case predicateKindPostsAny:
		if p.postsAnyState != nil {
			return p.postsAnyState.matches(idx)
		}
		for _, ids := range p.posts {
			if ids.Contains(idx) {
				return true
			}
		}
		return false
	case predicateKindPostsAnyNot:
		for _, ids := range p.posts {
			if ids.Contains(idx) {
				return false
			}
		}
		return true
	case predicateKindPostsAll:
		for _, ids := range p.posts {
			if !ids.Contains(idx) {
				return false
			}
		}
		return true
	case predicateKindPostsAllNot:
		for _, ids := range p.posts {
			if !ids.Contains(idx) {
				return true
			}
		}
		return false
	case predicateKindMaterialized:
		return p.ids.Contains(idx)
	case predicateKindMaterializedNot:
		return p.ids.IsEmpty() || !p.ids.Contains(idx)
	default:
		if p.rangeMat {
			if p.expr.Not {
				return p.ids.IsEmpty() || !p.ids.Contains(idx)
			}
			return p.ids.Contains(idx)
		}
		if p.lazyMatState != nil {
			ids := p.lazyMatState.materialize()
			if p.expr.Not {
				return ids.IsEmpty() || !ids.Contains(idx)
			}
			return ids.Contains(idx)
		}
		if p.contains == nil {
			return false
		}
		return p.contains(idx)
	}
}

func (p predicate) countBucket(bucket posting.List) (uint64, bool) {
	if in, ok, handled := (&p).runtimeRangeCountBucket(bucket); handled {
		return in, ok
	}
	switch p.kind {
	case predicateKindPosting:
		return p.posting.AndCardinality(bucket), true
	case predicateKindPostingNot:
		bc := bucket.Cardinality()
		hit := p.posting.AndCardinality(bucket)
		if hit >= bc {
			return 0, true
		}
		return bc - hit, true
	case predicateKindPostsAny:
		return countBucketPostsAny(p.posts, bucket)
	case predicateKindPostsAnyNot:
		return countBucketPostsAnyNot(p.posts, bucket)
	case predicateKindPostsAll:
		return countBucketPostsAll(p.posts, bucket)
	case predicateKindPostsAllNot:
		return countBucketPostsAllNot(p.posts, bucket)
	case predicateKindMaterialized:
		return p.ids.AndCardinality(bucket), true
	case predicateKindMaterializedNot:
		bc := bucket.Cardinality()
		if p.ids.IsEmpty() {
			return bc, true
		}
		hit := p.ids.AndCardinality(bucket)
		if hit >= bc {
			return 0, true
		}
		return bc - hit, true
	default:
		if p.rangeMat {
			in := bucket.AndCardinality(p.ids)
			if !p.expr.Not {
				return in, true
			}
			bc := bucket.Cardinality()
			if in >= bc {
				return 0, true
			}
			return bc - in, true
		}
		if p.lazyMatState != nil {
			ids := p.lazyMatState.materialize()
			in := bucket.AndCardinality(ids)
			if !p.expr.Not {
				return in, true
			}
			bc := bucket.Cardinality()
			if in >= bc {
				return 0, true
			}
			return bc - in, true
		}
		if p.bucketCount == nil {
			return 0, false
		}
		return p.bucketCount(bucket)
	}
}

// applyToPosting intersects (or subtracts for NOT variants) predicate matches
// with dst in place. Returns false when exact postingResult application is not
// available for this predicate shape.
func (p predicate) applyToPosting(dst posting.List) (posting.List, bool) {
	if p.alwaysTrue || p.covered {
		return dst, true
	}
	if p.alwaysFalse {
		dst.Release()
		return posting.List{}, true
	}

	switch p.kind {
	case predicateKindPosting:
		return dst.BuildAnd(p.posting), true

	case predicateKindPostingNot:
		return dst.BuildAndNot(p.posting), true

	case predicateKindPostsAnyNot:
		if len(p.posts) > predicatePostsAnyNotExactPostingMaxTerms {
			return posting.List{}, false
		}
		for _, ids := range p.posts {
			dst = dst.BuildAndNot(ids)
			if dst.IsEmpty() {
				return dst, true
			}
		}
		return dst, true

	case predicateKindPostsAny:
		if p.postsAnyState != nil {
			return p.postsAnyState.apply(dst)
		}
		if p.postingFilter != nil {
			next, ok := p.postingFilter(dst)
			if !ok {
				return posting.List{}, false
			}
			return next, true
		}
		return posting.List{}, false

	case predicateKindPostsAll:
		if len(p.posts) == 0 {
			dst.Release()
			return posting.List{}, true
		}
		for _, ids := range p.posts {
			if ids.IsEmpty() {
				dst.Release()
				return posting.List{}, true
			}
			dst = dst.BuildAnd(ids)
			if dst.IsEmpty() {
				return dst, true
			}
		}
		return dst, true

	case predicateKindMaterialized:
		if p.ids.IsEmpty() {
			dst.Release()
			return posting.List{}, true
		}
		return dst.BuildAnd(p.ids), true

	case predicateKindMaterializedNot:
		return dst.BuildAndNot(p.ids), true
	}

	if next, ok, handled := p.runtimeRangeApply(dst); handled {
		return next, ok
	}
	if p.postingFilter != nil {
		next, ok := p.postingFilter(dst)
		if !ok {
			return posting.List{}, false
		}
		return next, true
	}
	return posting.List{}, false
}

func countBucketPostsAny(posts []posting.List, bucket posting.List) (uint64, bool) {
	if bucket.IsEmpty() {
		return 0, true
	}
	if len(posts) == 0 {
		return 0, true
	}
	if len(posts) == 1 {
		return posts[0].AndCardinality(bucket), true
	}
	for _, ids := range posts {
		if ids.Intersects(bucket) {
			return 0, false
		}
	}
	return 0, true
}

func countBucketPostsAnyNot(posts []posting.List, bucket posting.List) (uint64, bool) {
	if bucket.IsEmpty() {
		return 0, true
	}
	bc := bucket.Cardinality()
	if len(posts) == 0 {
		return bc, true
	}
	if len(posts) == 1 {
		hit := posts[0].AndCardinality(bucket)
		if hit >= bc {
			return 0, true
		}
		return bc - hit, true
	}
	for _, ids := range posts {
		if ids.Intersects(bucket) {
			return 0, false
		}
	}
	return bc, true
}

func countBucketPostsAll(posts []posting.List, bucket posting.List) (uint64, bool) {
	if bucket.IsEmpty() {
		return 0, true
	}
	if len(posts) == 0 {
		return 0, true
	}
	if len(posts) == 1 {
		return posts[0].AndCardinality(bucket), true
	}
	for _, ids := range posts {
		if !ids.Intersects(bucket) {
			return 0, true
		}
	}
	return 0, false
}

func countBucketPostsAllNot(posts []posting.List, bucket posting.List) (uint64, bool) {
	if bucket.IsEmpty() {
		return 0, true
	}
	bc := bucket.Cardinality()
	if len(posts) == 0 {
		return bc, true
	}
	if len(posts) == 1 {
		hit := posts[0].AndCardinality(bucket)
		if hit >= bc {
			return 0, true
		}
		return bc - hit, true
	}
	for _, ids := range posts {
		if !ids.Intersects(bucket) {
			return bc, true
		}
	}
	return 0, false
}

// tryPlanCandidate attempts the internal plan candidate path and returns ok=false when fast-path preconditions are not satisfied.
func (qv *queryView[K, V]) tryPlanCandidate(q *qx.QX, trace *queryTrace) ([]K, bool, error) {

	// candidate planner is focused on the latency-critical paged path.
	if q.Limit == 0 {
		return nil, false, nil
	}
	if len(q.Order) > 1 {
		return nil, false, nil
	}

	leavesBuf := getExprSliceBuf(8)
	defer releaseExprSliceBuf(leavesBuf)
	leaves, ok := collectAndLeavesScratch(q.Expr, leavesBuf.values[:0])
	if !ok {
		return nil, false, nil
	}
	if len(q.Order) == 1 {
		o := q.Order[0]
		if o.Type != qx.OrderBasic {
			return nil, false, nil
		}
		if q.Offset != 0 {
			return nil, false, nil
		}
		if !qv.shouldUseCandidateOrder(o, leaves) {
			return nil, false, nil
		}

		preds, predsBuf, ok := qv.buildPredicatesCandidate(leaves)
		if !ok {
			return nil, false, nil
		}
		defer releasePredicates(preds, predsBuf)

		for i := range preds {
			if preds[i].alwaysFalse {
				return nil, true, nil
			}
		}

		if trace != nil {
			trace.setPlan(PlanCandidateOrder)
		}
		return qv.execPlanCandidateOrderBasic(q, preds), true, nil
	}

	// baseline fast-paths are better for trivial single-field range/prefix scans with limit;
	// candidate strategy is intended for mixed/complex predicates
	if len(leaves) == 1 && isSimpleScalarRangeOrPrefixLeaf(leaves[0]) {
		return nil, false, nil
	}

	// pure EQ conjunctions are already handled very well by baseline paths
	if allPositiveScalarEqLeaves(leaves) {
		return nil, false, nil
	}

	preds, predsBuf, ok := qv.buildPredicatesCandidate(leaves)
	if !ok {
		return nil, false, nil
	}
	defer releasePredicates(preds, predsBuf)

	for i := range preds {
		if preds[i].alwaysFalse {
			return nil, true, nil
		}
	}

	if trace != nil {
		trace.setPlan(PlanCandidateNoOrder)
	}
	return qv.execPlanLeadScanNoOrder(q, preds, nil), true, nil
}

func (qv *queryView[K, V]) shouldUseCandidateOrder(o qx.Order, leaves []qx.Expr) bool {
	fm := qv.fields[o.Field]
	if fm == nil || fm.Slice {
		return false
	}
	if !qv.fieldOverlay(o.Field).hasData() {
		return false
	}
	if len(leaves) < 2 {
		return false
	}

	hasNeg := false
	orderOnly := true

	for _, e := range leaves {
		// keep candidate ORDER conservative: only scalar equality/set predicates
		if e.Field == "" || len(e.Operands) != 0 || !isScalarEqOrInOp(e.Op) {
			return false
		}
		if ef := qv.fields[e.Field]; ef == nil || ef.Slice {
			return false
		}
		if e.Not {
			hasNeg = true
		}
		if e.Field != o.Field {
			orderOnly = false
		}
	}

	if !hasNeg {
		return false
	}
	if orderOnly {
		return false
	}

	return true
}

func (qv *queryView[K, V]) buildPredicatesCandidate(leaves []qx.Expr) ([]predicate, *predicateSliceBuf, bool) {
	if len(leaves) == 1 && leaves[0].Op == qx.OpNOOP && leaves[0].Not {
		return []predicate{{alwaysFalse: true}}, nil, true
	}

	var predsBuf *predicateSliceBuf
	var preds []predicate
	if len(leaves) >= predicateSlicePoolMinLen {
		predsBuf = getPredicateSliceBuf(len(leaves))
		preds = predsBuf.values[:0]
	} else {
		preds = make([]predicate, 0, len(leaves))
	}
	for _, e := range leaves {
		p, ok := qv.buildPredicateCandidate(e)
		if !ok {
			releasePredicates(preds, predsBuf)
			return nil, nil, false
		}
		preds = append(preds, p)
	}
	return preds, predsBuf, true
}

func (qv *queryView[K, V]) buildPredicateCandidate(e qx.Expr) (predicate, bool) {
	if e.Op == qx.OpNOOP {
		if e.Not {
			return predicate{expr: e, alwaysFalse: true}, true
		}
		return predicate{expr: e, alwaysTrue: true}, true
	}
	if e.Field == "" {
		return predicate{}, false
	}

	fm := qv.fields[e.Field]
	if fm == nil {
		return predicate{}, false
	}
	ov := qv.fieldOverlay(e.Field)

	switch e.Op {

	case qx.OpEQ:
		if !ov.hasData() {
			return predicate{}, false
		}
		return qv.buildPredEqCandidate(e, fm, ov)

	case qx.OpIN:
		if !ov.hasData() {
			return predicate{}, false
		}
		return qv.buildPredInCandidate(e, fm, ov)

	case qx.OpHAS:
		if !ov.hasData() {
			return predicate{}, false
		}
		return qv.buildPredHasCandidate(e, fm, ov)

	case qx.OpHASANY:
		if !ov.hasData() {
			return predicate{}, false
		}
		return qv.buildPredHasAnyCandidate(e, fm, ov)

	case qx.OpSUFFIX, qx.OpCONTAINS:
		return qv.buildPredMaterializedCandidate(e)

	default:
		if isScalarRangeOrPrefixOp(e.Op) {
			if !ov.hasData() {
				return predicate{}, false
			}
			return qv.buildPredRangeCandidateWithMode(e, fm, ov, true)
		}
		return predicate{}, false
	}
}

func (qv *queryView[K, V]) buildPredEqCandidate(e qx.Expr, fm *field, ov fieldOverlay) (predicate, bool) {
	key, isSlice, isNil, err := qv.exprValueToIdxScalar(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil {
		return predicate{}, false
	}

	if !fm.Slice {
		if isSlice {
			return predicate{}, false
		}
		if isNil {
			ids := qv.nilFieldOverlay(e.Field).lookupPostingRetained(nilIndexEntryKey)
			if e.Not {
				if ids.IsEmpty() {
					return predicate{expr: e, alwaysTrue: true}, true
				}
				return predicate{
					expr:    e,
					kind:    predicateKindPostingNot,
					posting: ids,
				}, true
			}
			if ids.IsEmpty() {
				return predicate{expr: e, alwaysFalse: true}, true
			}
			return predicate{
				expr:     e,
				kind:     predicateKindPosting,
				iterKind: predicateIterPosting,
				posting:  ids,
				estCard:  ids.Cardinality(),
			}, true
		} else {
			ids := ov.lookupPostingRetained(key)
			if e.Not {
				if ids.IsEmpty() {
					return predicate{expr: e, alwaysTrue: true}, true
				}
				return predicate{
					expr:    e,
					kind:    predicateKindPostingNot,
					posting: ids,
				}, true
			}
			if ids.IsEmpty() {
				return predicate{expr: e, alwaysFalse: true}, true
			}
			return predicate{
				expr:     e,
				kind:     predicateKindPosting,
				iterKind: predicateIterPosting,
				posting:  ids,
				estCard:  ids.Cardinality(),
			}, true
		}
	}

	if !isSlice {
		return predicate{}, false
	}

	vals, _, _, err := qv.exprValueToDistinctIdxOwned(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil {
		return predicate{}, false
	}

	b, err := qv.evalSliceEQ(e.Field, vals)
	if err != nil {
		return predicate{}, false
	}

	if e.Not {
		if b.ids.IsEmpty() {
			b.release()
			return predicate{expr: e, alwaysTrue: true}, true
		}
		ids := b.ids
		return predicate{
			expr:       e,
			kind:       predicateKindMaterializedNot,
			ids:        ids,
			releaseIDs: true,
		}, true
	}

	if b.ids.IsEmpty() {
		b.release()
		return predicate{expr: e, alwaysFalse: true}, true
	}

	ids := b.ids
	return predicate{
		expr:       e,
		kind:       predicateKindMaterialized,
		iterKind:   predicateIterMaterialized,
		ids:        ids,
		estCard:    ids.Cardinality(),
		releaseIDs: true,
	}, true
}

func (qv *queryView[K, V]) buildPredInCandidate(e qx.Expr, fm *field, ov fieldOverlay) (predicate, bool) {
	if fm.Slice {
		return predicate{}, false
	}

	vals, isSlice, hasNil, err := qv.exprValueToDistinctIdxOwned(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil {
		return predicate{}, false
	}
	if !isSlice || (len(vals) == 0 && !hasNil) {
		return predicate{}, false
	}
	posts, est, postsBuf := qv.scalarLookupPostings(e.Field, vals, hasNil)

	if e.Not {
		if len(posts) == 0 {
			if postsBuf != nil {
				releasePostingSliceBuf(postsBuf)
			}
			return predicate{expr: e, alwaysTrue: true}, true
		}
		if len(posts) == 1 {
			return predicate{
				expr:     e,
				kind:     predicateKindPostingNot,
				posting:  posts[0],
				posts:    posts,
				postsBuf: postsBuf,
			}, true
		}
		return predicate{
			expr:     e,
			kind:     predicateKindPostsAnyNot,
			posts:    posts,
			postsBuf: postsBuf,
		}, true
	}

	if len(posts) == 0 {
		if postsBuf != nil {
			releasePostingSliceBuf(postsBuf)
		}
		return predicate{expr: e, alwaysFalse: true}, true
	}
	if len(posts) == 1 {
		ids := posts[0]
		return predicate{
			expr:     e,
			kind:     predicateKindPosting,
			iterKind: predicateIterPosting,
			posting:  ids,
			estCard:  ids.Cardinality(),
			posts:    posts,
			postsBuf: postsBuf,
		}, true
	}

	return predicate{
		expr:          e,
		kind:          predicateKindPostsAny,
		iterKind:      predicateIterPostsConcat,
		posts:         posts,
		estCard:       est,
		postsBuf:      postsBuf,
		postsAnyState: acquirePostsAnyFilterState(posts),
	}, true
}

func (qv *queryView[K, V]) buildPredHasCandidate(e qx.Expr, fm *field, ov fieldOverlay) (predicate, bool) {
	if !fm.Slice {
		return predicate{}, false
	}

	vals, isSlice, _, err := qv.exprValueToDistinctIdxOwned(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil {
		return predicate{}, false
	}
	if !isSlice || len(vals) == 0 {
		return predicate{}, false
	}

	postsBuf := getPostingSliceBuf(len(vals))
	posts := postsBuf.values

	var minCard uint64

	for _, v := range vals {
		ids := ov.lookupPostingRetained(v)
		if ids.IsEmpty() {
			postsBuf.values = posts
			releasePostingSliceBuf(postsBuf)
			if e.Not {
				return predicate{expr: e, alwaysTrue: true}, true
			}
			return predicate{expr: e, alwaysFalse: true}, true
		}
		posts = append(posts, ids)
		c := ids.Cardinality()
		if minCard == 0 || c < minCard {
			minCard = c
		}
	}

	if e.Not {
		if len(posts) == 1 {
			return predicate{
				expr:     e,
				kind:     predicateKindPostingNot,
				posting:  posts[0],
				posts:    posts,
				postsBuf: postsBuf,
			}, true
		}
		return predicate{
			expr:     e,
			kind:     predicateKindPostsAllNot,
			posts:    posts,
			postsBuf: postsBuf,
		}, true
	}

	lead := minCardPosting(posts)
	return predicate{
		expr:     e,
		kind:     predicateKindPostsAll,
		iterKind: predicateIterPosting,
		posting:  lead,
		posts:    posts,
		estCard:  minCard,
		postsBuf: postsBuf,
	}, true
}

func (qv *queryView[K, V]) buildPredHasAnyCandidate(e qx.Expr, fm *field, ov fieldOverlay) (predicate, bool) {
	if !fm.Slice {
		return predicate{}, false
	}

	vals, isSlice, _, err := qv.exprValueToDistinctIdxOwned(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil {
		return predicate{}, false
	}
	if !isSlice || len(vals) == 0 {
		return predicate{}, false
	}

	postsBuf := getPostingSliceBuf(len(vals))
	posts := postsBuf.values

	var est uint64

	for _, v := range vals {
		ids := ov.lookupPostingRetained(v)
		if ids.IsEmpty() {
			continue
		}
		posts = append(posts, ids)
		est += ids.Cardinality()
	}
	if e.Not {
		if len(posts) == 0 {
			postsBuf.values = posts
			releasePostingSliceBuf(postsBuf)
			return predicate{expr: e, alwaysTrue: true}, true
		}
		if len(posts) == 1 {
			return predicate{
				expr:     e,
				kind:     predicateKindPostingNot,
				posting:  posts[0],
				posts:    posts,
				postsBuf: postsBuf,
			}, true
		}
		return predicate{
			expr:     e,
			kind:     predicateKindPostsAnyNot,
			posts:    posts,
			postsBuf: postsBuf,
		}, true
	}

	if len(posts) == 0 {
		postsBuf.values = posts
		releasePostingSliceBuf(postsBuf)
		return predicate{expr: e, alwaysFalse: true}, true
	}

	if len(posts) == 1 {
		ids := posts[0]
		return predicate{
			expr:     e,
			kind:     predicateKindPosting,
			iterKind: predicateIterPosting,
			posting:  ids,
			estCard:  ids.Cardinality(),
			posts:    posts,
			postsBuf: postsBuf,
		}, true
	}

	return predicate{
		expr:          e,
		kind:          predicateKindPostsAny,
		iterKind:      predicateIterPostsUnion,
		posts:         posts,
		estCard:       est,
		postsBuf:      postsBuf,
		postsAnyState: acquirePostsAnyFilterState(posts),
	}, true
}

func rangeBoundsForOp(op qx.Op, key string) (rangeBounds, bool) {
	rb := rangeBounds{has: true}
	if !rb.applyOp(op, key) {
		return rangeBounds{}, false
	}
	return rb, true
}

func overlayRangeStats(ov fieldOverlay, br overlayRange) (int, uint64) {
	if br.baseStart >= br.baseEnd {
		return 0, 0
	}
	if ov.chunked != nil {
		startPos := ov.chunked.posForRank(br.baseStart)
		endPos := ov.chunked.posForRank(br.baseEnd)
		return br.baseEnd - br.baseStart, ov.chunked.rangeRows(startPos, endPos)
	}
	cur := ov.newCursor(br, false)
	n := 0
	est := uint64(0)
	for {
		_, ids, ok := cur.next()
		if !ok {
			break
		}
		card := ids.Cardinality()
		if card == 0 {
			continue
		}
		n++
		est += card
	}
	return n, est
}

func overlayUnionRange(ov fieldOverlay, br overlayRange) posting.List {
	builder := newPostingUnionBuilder(br.baseEnd > br.baseStart && br.baseEnd-br.baseStart <= singleAdaptiveMaxLen)
	defer builder.release()
	cur := ov.newCursor(br, false)
	for {
		_, ids, ok := cur.next()
		if !ok {
			break
		}
		builder.addPosting(ids)
	}
	return builder.finish(true)
}

func overlayComplementRangeSpans(ov fieldOverlay, br overlayRange) (before, after overlayRange) {
	before = overlayRange{
		baseStart: 0,
		baseEnd:   br.baseStart,
	}
	after = overlayRange{
		baseStart: br.baseEnd,
		baseEnd:   ov.keyCount(),
	}
	if ov.chunked != nil {
		startPos := ov.chunked.startPos()
		endPos := ov.chunked.endPos()
		before.startPos = startPos
		before.endPos = br.startPos
		after.startPos = br.endPos
		after.endPos = endPos
	}
	return before, after
}

type overlayRangeIter struct {
	cur    overlayKeyCursor
	curIt  posting.Iterator
	single struct {
		set bool
		v   uint64
	}
}

func acquireOverlayRangeIter(ov fieldOverlay, br overlayRange) *overlayRangeIter {
	if v := overlayRangeIterPool.Get(); v != nil {
		it := v.(*overlayRangeIter)
		it.cur = ov.newCursor(br, false)
		return it
	}
	return &overlayRangeIter{cur: ov.newCursor(br, false)}
}

func releaseOverlayRangeIter(it *overlayRangeIter) {
	if it == nil {
		return
	}
	if it.curIt != nil {
		it.curIt.Release()
		it.curIt = nil
	}
	it.cur = overlayKeyCursor{}
	it.single = struct {
		set bool
		v   uint64
	}{}
	overlayRangeIterPool.Put(it)
}

func (it *overlayRangeIter) Release() {
	releaseOverlayRangeIter(it)
}

func (it *overlayRangeIter) HasNext() bool {
	for {
		if it.single.set {
			return true
		}
		if it.curIt != nil && it.curIt.HasNext() {
			return true
		}
		if it.curIt != nil {
			it.curIt.Release()
			it.curIt = nil
		}

		_, ids, ok := it.cur.next()
		if !ok {
			return false
		}
		if ids.IsEmpty() {
			continue
		}
		if idx, ok := ids.TrySingle(); ok {
			it.single.set = true
			it.single.v = idx
			continue
		}
		it.curIt = ids.Iter()
	}
}

func (it *overlayRangeIter) Next() uint64 {
	if !it.HasNext() {
		return 0
	}
	if it.single.set {
		it.single.set = false
		return it.single.v
	}
	return it.curIt.Next()
}

func (qv *queryView[K, V]) buildPredRangeCandidateWithMode(e qx.Expr, fm *field, ov fieldOverlay, allowMaterialize bool) (predicate, bool) {
	core, pred, done, ok := qv.prepareScalarRangePredicate(e, fm)
	if !ok {
		return predicate{}, false
	}
	if done {
		return pred, true
	}
	return core.buildFromOverlay(ov, allowMaterialize)
}

func (qv *queryView[K, V]) buildPredMaterializedCandidate(e qx.Expr) (predicate, bool) {
	raw := e
	raw.Not = false
	cacheKey := qv.materializedPredCacheKey(raw)
	state := acquireLazyMaterializedPredicateState(qv, raw, cacheKey)

	est := qv.snapshotUniverseCardinality()
	if est == 0 {
		est = 1
	}

	if e.Not {
		return predicate{
			expr:         e,
			estCard:      est,
			lazyMatState: state,
		}, true
	}

	return predicate{
		expr:         e,
		estCard:      est,
		lazyMatState: state,
	}, true
}

func (qv *queryView[K, V]) evalLazyMaterializedPredicate(raw qx.Expr, cacheKey string) posting.List {
	if cacheKey != "" {
		if cached, ok := qv.snap.loadMaterializedPred(cacheKey); ok {
			return cached
		}
	}

	if isScalarRangeOrPrefixOp(raw.Op) {
		fm := qv.fields[raw.Field]
		core, pred, done, ok := qv.prepareScalarRangePredicate(raw, fm)
		if ok {
			if done {
				if pred.alwaysFalse && cacheKey != "" {
					qv.snap.storeMaterializedPred(cacheKey, posting.List{})
				}
				return posting.List{}
			}
			ov := qv.fieldOverlay(raw.Field)
			if !ov.hasData() {
				if cacheKey != "" {
					qv.snap.storeMaterializedPred(cacheKey, posting.List{})
				}
				return posting.List{}
			}
			return core.evalMaterializedPostingResult(ov).ids
		}
	}

	b, err := qv.evalSimple(raw)
	if err != nil {
		if cacheKey != "" {
			qv.snap.storeMaterializedPred(cacheKey, posting.List{})
		}
		return posting.List{}
	}
	if b.ids.IsEmpty() {
		b.release()
		if cacheKey != "" {
			qv.snap.storeMaterializedPred(cacheKey, posting.List{})
		}
		return posting.List{}
	}
	ids := b.ids
	if cacheKey != "" {
		ids = qv.tryShareMaterializedPred(cacheKey, ids)
	}
	return ids
}

func (qv *queryView[K, V]) materializedPredCacheKeyForScalar(field string, op qx.Op, key string) string {
	if qv.snap.matPredCacheMaxEntries <= 0 {
		return ""
	}
	return materializedPredCacheKeyFromScalar(field, op, key)
}

func (qv *queryView[K, V]) materializedPredComplementCacheKeyForScalar(field string, op qx.Op, key string) string {
	if qv.snap.matPredCacheMaxEntries <= 0 {
		return ""
	}
	if !isNumericRangeOp(op) {
		return ""
	}
	if field == "" || key == "" {
		return ""
	}
	return field + "\x1f" + "count_range_complement" + "\x1f" +
		strconv.Itoa(int(op)) + "\x1f" + key
}

func (qv *queryView[K, V]) materializedPredCacheKey(e qx.Expr) string {
	if isScalarRangeOrPrefixOp(e.Op) {
		bound, isSlice, err := qv.normalizedScalarBoundForExpr(e)
		if err != nil || isSlice || bound.empty || bound.full {
			return ""
		}
		return qv.materializedPredCacheKeyForScalar(e.Field, bound.op, bound.key)
	}
	key, isSlice, isNil, err := qv.exprValueToIdxScalar(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil || isSlice || isNil {
		return ""
	}
	return qv.materializedPredCacheKeyForScalar(e.Field, e.Op, key)
}

func materializedPredCacheKeyFromScalar(field string, op qx.Op, key string) string {
	if field == "" || !isMaterializedScalarCacheOp(op) {
		return ""
	}
	return field + "\x1f" + strconv.Itoa(int(op)) + "\x1f" + key
}

func materializedPredCacheKeyForNumericBucketSpan(field string, startBucket, endBucket int) string {
	if field == "" || startBucket < 0 || endBucket < startBucket {
		return ""
	}
	return field + "\x1f" + "range_bucket" + "\x1f" +
		strconv.Itoa(startBucket) + "\x1f" + strconv.Itoa(endBucket)
}

// tryShareMaterializedPred caches ids and, on success, rewrites the caller
// handle to a borrowed alias of the cached payload.
func (qv *queryView[K, V]) tryShareMaterializedPred(cacheKey string, ids posting.List) posting.List {
	return tryShareMaterializedPredOnSnapshot(qv.snap, cacheKey, ids)
}

type rangeIter struct {
	s      []index
	i      int
	end    int
	curIt  posting.Iterator
	single struct {
		set bool
		v   uint64
	}
}

func newRangeIter(s []index, start, end int) posting.Iterator {
	return &rangeIter{s: s, i: start, end: end}
}

func (it *rangeIter) HasNext() bool {
	for {
		if it.single.set {
			return true
		}
		if it.curIt != nil && it.curIt.HasNext() {
			return true
		}
		if it.curIt != nil {
			it.curIt.Release()
			it.curIt = nil
		}
		if it.i >= it.end {
			return false
		}
		bm := it.s[it.i].IDs
		it.i++
		if bm.IsEmpty() {
			continue
		}
		if id, ok := bm.TrySingle(); ok {
			it.single.set = true
			it.single.v = id
			continue
		}
		it.curIt = bm.Iter()
	}
}

func (it *rangeIter) Release() {
	if it.curIt != nil {
		it.curIt.Release()
		it.curIt = nil
	}
	it.s = nil
	it.i = 0
	it.end = 0
	it.single = struct {
		set bool
		v   uint64
	}{}
}

func (it *rangeIter) Next() uint64 {
	if !it.HasNext() {
		return 0
	}
	if it.single.set {
		it.single.set = false
		return it.single.v
	}
	return it.curIt.Next()
}

func (qv *queryView[K, V]) execPlanLeadScanNoOrder(q *qx.QX, preds []predicate, trace *queryTrace) []K {
	var leadIdx = -1
	var best uint64
	for i := range preds {
		p := preds[i]
		if p.alwaysTrue || p.alwaysFalse || !p.hasIter() {
			continue
		}
		if leadIdx == -1 || p.estCard < best {
			leadIdx = i
			best = p.estCard
		}
	}

	var it posting.Iterator
	leadNeedsCheck := false
	if leadIdx >= 0 {
		it = preds[leadIdx].newIter()
		leadNeedsCheck = preds[leadIdx].leadIterNeedsContainsCheck()
	} else {
		it = qv.snapshotUniverseView().Iter()
	}
	if it != nil {
		defer it.Release()
	}
	activeBuf := getIntSliceBuf(len(preds))
	active := activeBuf.values
	defer func() {
		activeBuf.values = active
		releaseIntSliceBuf(activeBuf)
	}()
	for i := range preds {
		if i == leadIdx && !leadNeedsCheck {
			continue
		}
		p := preds[i]
		if p.covered || p.alwaysTrue {
			continue
		}
		if p.alwaysFalse {
			return nil
		}
		if !p.hasContains() {
			return nil
		}
		active = append(active, i)
	}
	sortActivePredicates(active, preds)

	out := make([]K, 0, int(q.Limit))
	cursor := qv.newQueryCursor(out, q.Offset, q.Limit, q.Limit == 0, 0)
	singleActive := -1
	if len(active) == 1 {
		singleActive = active[0]
	}

	if out, ok := qv.tryExecLeadScanNoOrderBuckets(q, preds, leadIdx, active, trace); ok {
		return out
	}

	if trace != nil {
		if leadIdx >= 0 && best > 0 {
			trace.addExamined(best)
		} else {
			trace.addExamined(qv.snapshotUniverseCardinality())
		}
	}

	for it.HasNext() {
		idx := it.Next()

		if singleActive >= 0 {
			p := preds[singleActive]
			if !p.matches(idx) {
				continue
			}
			if cursor.emit(idx) {
				break
			}
			continue
		}

		pass := true
		for _, pi := range active {
			if !preds[pi].matches(idx) {
				pass = false
				break
			}
		}
		if !pass {
			continue
		}

		if cursor.emit(idx) {
			break
		}
	}

	return cursor.out
}

func (qv *queryView[K, V]) tryExecLeadScanNoOrderBuckets(q *qx.QX, preds []predicate, leadIdx int, active []int, trace *queryTrace) ([]K, bool) {
	if leadIdx < 0 || leadIdx >= len(preds) {
		return nil, false
	}
	lead := preds[leadIdx]
	e := lead.expr
	span, ok, err := qv.prepareScalarOverlaySpan(e)
	if err != nil || !ok {
		return nil, false
	}
	if !span.hasData {
		return nil, true
	}
	if overlayRangeEmpty(span.br) {
		return nil, true
	}
	if span.br.baseEnd-span.br.baseStart < 4 {
		return nil, false
	}
	out, _ := qv.scanOrderLimitWithPredicates(q, span.ov, span.br, false, preds, "", trace)
	return out, true
}

func (qv *queryView[K, V]) execPlanCandidateOrderBasic(q *qx.QX, preds []predicate) []K {
	o := q.Order[0]

	fm := qv.fields[o.Field]
	if fm == nil || fm.Slice {
		return nil
	}

	ov := qv.fieldOverlay(o.Field)
	if !ov.hasData() {
		return nil
	}

	br := ov.rangeForBounds(rangeBounds{has: true})
	var rangeCovered []bool
	if coveredRange, covered, ok := qv.extractOrderRangeCoverageOverlay(o.Field, preds, ov); ok {
		br = coveredRange
		rangeCovered = covered
	}
	if overlayRangeEmpty(br) {
		return nil
	}
	for i := range rangeCovered {
		if rangeCovered[i] {
			preds[i].covered = true
		}
	}

	activeBuf := getIntSliceBuf(len(preds))
	active := activeBuf.values
	defer func() {
		activeBuf.values = active
		releaseIntSliceBuf(activeBuf)
	}()
	for i := range preds {
		p := preds[i]
		if p.covered || p.alwaysTrue {
			continue
		}
		if p.alwaysFalse {
			return nil
		}
		active = append(active, i)
	}
	sortActivePredicates(active, preds)
	if len(active) == 0 {
		out, _ := qv.scanOrderLimitNoPredicates(q, ov, br, o.Desc, "", nil)
		return out
	}
	out, _ := qv.scanOrderLimitWithPredicates(q, ov, br, o.Desc, preds, "", nil)
	return out
}

func (qv *queryView[K, V]) extractOrderRangeCoverageOverlayWithBounds(field string, preds []predicate, ov fieldOverlay) (rangeBounds, overlayRange, []bool, bool) {
	rb, covered, _, ok := qv.collectPredicateRangeBounds(field, preds)
	if !ok {
		return rangeBounds{}, overlayRange{}, nil, false
	}

	return rb, ov.rangeForBounds(rb), covered, true
}

func (qv *queryView[K, V]) extractOrderRangeCoverageWithBounds(field string, preds []predicate, s []index) (rangeBounds, int, int, []bool, bool) {
	ov := fieldOverlay{base: s}
	rb, br, covered, ok := qv.extractOrderRangeCoverageOverlayWithBounds(field, preds, ov)
	if !ok {
		return rangeBounds{}, 0, 0, nil, false
	}
	return rb, br.baseStart, br.baseEnd, covered, true
}

func (qv *queryView[K, V]) extractOrderRangeCoverage(field string, preds []predicate, s []index) (int, int, []bool, bool) {
	_, st, en, covered, ok := qv.extractOrderRangeCoverageWithBounds(field, preds, s)
	return st, en, covered, ok
}

func (qv *queryView[K, V]) extractOrderRangeCoverageOverlay(field string, preds []predicate, ov fieldOverlay) (overlayRange, []bool, bool) {
	_, br, covered, ok := qv.extractOrderRangeCoverageOverlayWithBounds(field, preds, ov)
	return br, covered, ok
}

const (
	rangeLinearContainsMax = 16
	rangeMaterializeAfter  = 64
)

func rangeLinearContainsLimit(probeLen int, est uint64) int {
	if probeLen <= 0 {
		return rangeLinearContainsMax
	}
	buildWork := rangeProbeMaterializeWork(probeLen, est)
	perContains := rangeProbeContainsWork(probeLen, est)
	if perContains == 0 {
		return rangeLinearContainsMax
	}
	limit := int(buildWork / perContains)
	if buildWork%perContains != 0 {
		limit++
	}
	if limit < 8 {
		return 8
	}
	if limit > 32 {
		return 32
	}
	return limit
}

func rangeProbeMaterializeWork(probeLen int, est uint64) uint64 {
	if probeLen <= 0 {
		return 1
	}
	buildWork := est
	if buildWork < uint64(probeLen) {
		buildWork = uint64(probeLen)
	}
	avgPerBucket := uint64(1)
	if est > 0 {
		avgPerBucket = est / uint64(probeLen)
		if avgPerBucket == 0 {
			avgPerBucket = 1
		}
	}
	structuralWork := uint64(probeLen) * uint64(max(bits.Len64(avgPerBucket+1), 1))
	if ^uint64(0)-buildWork < structuralWork {
		return ^uint64(0)
	}
	return buildWork + structuralWork
}

func rangeProbeContainsWork(probeLen int, est uint64) uint64 {
	if probeLen <= 0 {
		return 1
	}
	avgPerBucket := uint64(1)
	if est > 0 {
		avgPerBucket = est / uint64(probeLen)
		if avgPerBucket == 0 {
			avgPerBucket = 1
		}
	}
	work := uint64(probeLen) * uint64(max(bits.Len64(avgPerBucket+1), 1))
	if work == 0 {
		return 1
	}
	return work
}

func rangeProbePostingFilterWork(probeLen int, est uint64) uint64 {
	work := rangeProbeContainsWork(probeLen, est)
	rowBudget := uint64(rangePostingFilterKeepRowsMaxCard)
	if est > 0 && est < rowBudget {
		rowBudget = est
	}
	if rowBudget == 0 {
		rowBudget = 1
	}
	if work < rowBudget {
		return rowBudget
	}
	return work
}

func materializedShareStructuralFactor(bucketCount int) uint64 {
	if bucketCount <= 0 {
		return 1
	}
	return uint64(max(bits.Len64(uint64(bucketCount)+1), 1))
}

func rangeMaterializeAfterForProbe(probeLen int, est uint64) int {
	if probeLen <= 0 {
		return rangeMaterializeAfter
	}
	buildWork := rangeProbeMaterializeWork(probeLen, est)
	perCall := rangeProbeContainsWork(probeLen, est)
	after := buildWork / perCall
	if buildWork%perCall != 0 {
		after++
	}
	if after < 1 {
		return 1
	}
	if after > rangeMaterializeAfter {
		return rangeMaterializeAfter
	}
	return int(after)
}

func rangePostingFilterMaterializeAfterForProbe(probeLen int, est uint64) int {
	buildWork := rangeProbeMaterializeWork(probeLen, est)
	perCall := rangeProbePostingFilterWork(probeLen, est)
	after := buildWork / perCall
	if buildWork%perCall != 0 {
		after++
	}
	if after < 1 {
		return 1
	}
	if after > rangeMaterializeAfter {
		return rangeMaterializeAfter
	}
	return int(after)
}

type baseRangeContainsMode uint8

const (
	baseRangeContainsLinear baseRangeContainsMode = iota
	baseRangeContainsHashSet
	baseRangeContainsPosting
)

func allowRuntimePositiveRangeSecondHitShare(est, universe uint64) bool {
	if est == 0 || universe == 0 || est >= universe {
		return false
	}
	return est <= universe-est
}

func postingContainsLookupWork(card uint64) uint64 {
	if card == 0 {
		return 1
	}
	return uint64(max(bits.Len64(card+1), 1))
}

func rangeHashSetBuildWork(est uint64) uint64 {
	if est == 0 {
		return 0
	}
	return satMulUint64(est, 2)
}

func rangeHashSetLookupWork() uint64 {
	return 1
}

func rangeBreakEvenAfter(buildWork, currentPerCall, futurePerCall uint64, maxAfter int) int {
	if buildWork == 0 {
		return 1
	}
	if currentPerCall <= futurePerCall {
		return maxAfter
	}
	savedPerCall := currentPerCall - futurePerCall
	after := buildWork / savedPerCall
	if buildWork%savedPerCall != 0 {
		after++
	}
	if after < 1 {
		return 1
	}
	if after > uint64(maxAfter) {
		return maxAfter
	}
	return int(after)
}

func rangeHashSetAfterForProbe(probeLen int, est uint64) int {
	if probeLen <= 0 || est == 0 {
		return 0
	}
	if est > uint64(u64SetPoolMaxCap/2) {
		return 0
	}
	return rangeBreakEvenAfter(
		rangeHashSetBuildWork(est),
		rangeProbeContainsWork(probeLen, est),
		rangeHashSetLookupWork(),
		rangeMaterializeAfter,
	)
}

func rangeLinearContainsTotalWork(probeLen int, est uint64, calls int) uint64 {
	if calls <= 0 {
		return 0
	}
	return satMulUint64(uint64(calls), rangeProbeContainsWork(probeLen, est))
}

func rangeHashSetTotalWork(probeLen int, est uint64, calls int) uint64 {
	if calls <= 0 {
		return 0
	}
	if rangeHashSetAfterForProbe(probeLen, est) == 0 {
		return ^uint64(0)
	}
	return satAddUint64(
		rangeHashSetBuildWork(est),
		satMulUint64(uint64(calls), rangeHashSetLookupWork()),
	)
}

func rangeMaterializedContainsTotalWork(probeLen int, est uint64, calls int) uint64 {
	if calls <= 0 {
		return 0
	}
	return satAddUint64(
		rangeProbeMaterializeWork(probeLen, est),
		satMulUint64(uint64(calls), postingContainsLookupWork(est)),
	)
}

func chooseBaseRangeContainsMode(
	probeLen int,
	est uint64,
	expectedCalls int,
	hashSetAfter int,
	materializeAfter int,
) baseRangeContainsMode {
	if probeLen <= 0 || est == 0 || expectedCalls <= 0 {
		return baseRangeContainsLinear
	}
	bestWork := rangeLinearContainsTotalWork(probeLen, est, expectedCalls)
	bestMode := baseRangeContainsLinear
	if hashSetAfter > 0 {
		hashSetWork := rangeHashSetTotalWork(probeLen, est, expectedCalls)
		if hashSetWork < bestWork {
			bestWork = hashSetWork
			bestMode = baseRangeContainsHashSet
		}
	}
	if materializeAfter > 0 {
		materializedWork := rangeMaterializedContainsTotalWork(probeLen, est, expectedCalls)
		if materializedWork < bestWork {
			bestMode = baseRangeContainsPosting
		}
	}
	return bestMode
}

func rangeProbeCountBucketWork(probeLen int, est, bucketCard uint64) uint64 {
	if probeLen <= 0 || bucketCard == 0 {
		return 0
	}
	avgPerBucket := uint64(1)
	if est > 0 {
		avgPerBucket = est / uint64(probeLen)
		if avgPerBucket == 0 {
			avgPerBucket = 1
		}
	}
	if bucketCard < avgPerBucket {
		avgPerBucket = bucketCard
	}
	return satMulUint64(uint64(probeLen), avgPerBucket)
}

func rangeCountBucketUseful(probeLen int, est, bucketCard uint64) bool {
	if probeLen <= 0 || bucketCard == 0 {
		return true
	}
	countWork := rangeProbeCountBucketWork(probeLen, est, bucketCard)
	rowWork := satMulUint64(bucketCard, rangeProbeContainsWork(probeLen, est))
	return countWork <= rowWork
}

func orderedPredicateExpectedRows(orderedWindow int, est, universe uint64) uint64 {
	if orderedWindow <= 0 || est == 0 || universe == 0 {
		return 0
	}
	need := uint64(orderedWindow)
	if est >= universe {
		if need > universe {
			return universe
		}
		return need
	}
	if need > ^uint64(0)/universe {
		return universe
	}
	rows := (need * universe) / est
	if (need*universe)%est != 0 {
		rows++
	}
	if rows < need {
		rows = need
	}
	if rows > universe {
		rows = universe
	}
	return rows
}

func orderedBaseRangeExpectedContainsCalls(state *baseRangePredicateState, orderedWindow int, universe uint64) int {
	if state == nil || orderedWindow <= 0 || universe == 0 {
		return 0
	}
	est := state.probe.probeEst
	if !state.keepProbeHits {
		if est >= universe {
			est = 0
		} else {
			est = universe - est
		}
	}
	return clampUint64ToInt(orderedPredicateExpectedRows(orderedWindow, est, universe))
}

func rangeProbeTotalWorkForRows(rows, probeLen int, est uint64) uint64 {
	if rows <= 0 {
		return 0
	}
	perRow := rangeProbeContainsWork(probeLen, est)
	if perRow == 0 {
		return 0
	}
	if ^uint64(0)/perRow < uint64(rows) {
		return ^uint64(0)
	}
	return uint64(rows) * perRow
}

func postingUnionLinearWork(probeLen int, est uint64) uint64 {
	if probeLen <= 0 {
		return 0
	}
	work := rangeProbeMaterializeWork(probeLen, est)
	mergeCalls := uint64(probeLen) * uint64(max(bits.Len64(uint64(probeLen)+1), 1))
	if ^uint64(0)-work < mergeCalls {
		return ^uint64(0)
	}
	return work + mergeCalls
}

func postingUnionFastSinglesWork(probeLen, singletonCount int, est uint64) uint64 {
	if probeLen <= 0 || singletonCount <= 0 {
		return ^uint64(0)
	}
	singles := uint64(singletonCount)
	multiCount := probeLen - singletonCount
	multiEst := est
	if multiEst > singles {
		multiEst -= singles
	} else {
		multiEst = 0
	}
	work := singles * uint64(max(bits.Len64(singles+1), 1))
	if ^uint64(0)-work < singles {
		return ^uint64(0)
	}
	work += singles
	if multiCount == 0 {
		return work
	}
	multiWork := rangeProbeMaterializeWork(multiCount, multiEst)
	if ^uint64(0)-work < multiWork {
		return ^uint64(0)
	}
	return work + multiWork
}

func shouldUseFastSinglesUnion(probeLen, singletonCount int, est uint64) bool {
	if probeLen <= 1 || singletonCount <= 0 {
		return false
	}
	return postingUnionFastSinglesWork(probeLen, singletonCount, est) < postingUnionLinearWork(probeLen, est)
}

// tryPlanOrdered attempts the internal plan ordered path and returns false when fast-path preconditions are not satisfied.
func (qv *queryView[K, V]) tryPlanOrdered(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	if q.Limit == 0 {
		return nil, false, nil
	}
	if len(q.Order) > 1 {
		return nil, false, nil
	}

	leavesBuf := getExprSliceBuf(8)
	defer releaseExprSliceBuf(leavesBuf)
	leaves, ok := collectAndLeavesScratch(q.Expr, leavesBuf.values[:0])
	if !ok {
		return nil, false, nil
	}

	if len(q.Order) == 1 {
		o := q.Order[0]
		if o.Type != qx.OrderBasic {
			return nil, false, nil
		}
		decision := qv.decideOrderedByCost(q, leaves)
		if trace != nil {
			trace.setEstimated(decision.expectedProbeRows, decision.orderedCost, decision.fallbackCost)
		}
		if !decision.use {
			return nil, false, nil
		}
		window, _ := orderWindow(q)
		preds, predsBuf, ok := qv.buildPredicatesOrderedWithMode(leaves, o.Field, false, window, true, true)
		if !ok {
			return nil, false, nil
		}
		defer releasePredicates(preds, predsBuf)

		for i := range preds {
			if preds[i].alwaysFalse {
				return nil, true, nil
			}
		}

		if trace != nil {
			trace.setPlan(PlanOrdered)
		}
		execTrace := trace
		var observedTrace queryTrace
		observedStart := uint64(0)
		if execTrace == nil {
			execTrace = &observedTrace
		} else {
			observedStart = execTrace.ev.RowsExamined
		}
		out, ok := qv.execPlanOrderedBasic(q, preds, execTrace)
		if !ok {
			return nil, false, nil
		}
		needWindow, _ := orderWindow(q)
		baseOpsBuf := getExprSliceBuf(len(leaves))
		baseOpsBuf.values = baseOpsBuf.values[:0]
		for _, op := range leaves {
			if op.Field == o.Field {
				continue
			}
			baseOpsBuf.values = append(baseOpsBuf.values, op)
		}
		qv.promoteOrderBasicLimitMaterializedBaseOps(o.Field, baseOpsBuf.values, execTrace.ev.RowsExamined-observedStart, uint64(needWindow))
		releaseExprSliceBuf(baseOpsBuf)
		return out, true, nil
	}

	// keep baseline paths for trivial patterns where ordered strategy usually has no advantage
	if len(leaves) == 1 && isSimpleScalarRangeOrPrefixLeaf(leaves[0]) {
		return nil, false, nil
	}
	if allPositiveScalarEqLeaves(leaves) {
		return nil, false, nil
	}

	preds, predsBuf, ok := qv.buildPredicates(leaves)
	if !ok {
		return nil, false, nil
	}
	defer releasePredicates(preds, predsBuf)

	for i := range preds {
		if preds[i].alwaysFalse {
			return nil, true, nil
		}
	}

	if trace != nil {
		trace.setPlan(PlanOrderedNoOrder)
	}
	return qv.execPlanLeadScanNoOrder(q, preds, trace), true, nil
}

func (p predicate) checkCost() uint64 {
	if p.alwaysFalse {
		return 0
	}
	cost := uint64(6)
	switch {
	case p.rangeMat || p.kind == predicateKindMaterialized || p.kind == predicateKindMaterializedNot:
		cost = 1
	case p.kind == predicateKindPosting || p.kind == predicateKindPostingNot:
		cost = 1
	case p.kind == predicateKindPostsAny ||
		p.kind == predicateKindPostsAnyNot ||
		p.kind == predicateKindPostsAll ||
		p.kind == predicateKindPostsAllNot:
		n := len(p.posts)
		if n <= 1 {
			cost = 1
		} else {
			cost = uint64(n)
		}
	case p.baseRangeState != nil:
		if !p.baseRangeState.ids.IsEmpty() {
			cost = 1
		} else if p.baseRangeState.probe.probeLen <= p.baseRangeState.linearContainsMax {
			cost = uint64(max(p.baseRangeState.probe.probeLen, 1))
		} else {
			cost = 2
		}
	case p.overlayState != nil:
		if !p.overlayState.rangeIDs.IsEmpty() {
			cost = 1
		} else if p.overlayState.bucketCount <= 1 {
			cost = 1
		} else if p.overlayState.bucketCount <= 4 {
			cost = uint64(p.overlayState.bucketCount)
		} else {
			cost = 3
		}
	case p.postsAnyState != nil:
		n := len(p.posts)
		if n <= 1 {
			cost = 1
		} else {
			cost = uint64(n)
		}
	case p.contains != nil:
		cost = 8
	}
	if p.expr.Not {
		cost++
	}
	return cost
}

func sortActivePredicates(active []int, preds []predicate) {
	if len(active) <= 1 {
		return
	}

	estOf := func(pi int) uint64 {
		if pi < 0 || pi >= len(preds) {
			return math.MaxUint64
		}
		est := preds[pi].estCard
		if est == 0 {
			return math.MaxUint64
		}
		return est
	}

	less := func(a, b int) bool {
		pa := preds[a]
		pb := preds[b]
		ca := pa.checkCost()
		cb := pb.checkCost()
		ea := estOf(a)
		eb := estOf(b)
		sa := satMulUint64(ca, ea)
		sb := satMulUint64(cb, eb)
		if sa != sb {
			return sa < sb
		}
		if ca != cb {
			return ca < cb
		}
		if ea != eb {
			return ea < eb
		}
		return a < b
	}

	// Small in-place insertion sort keeps overhead allocation-free.
	for i := 1; i < len(active); i++ {
		cur := active[i]
		j := i
		for j > 0 && less(cur, active[j-1]) {
			active[j] = active[j-1]
			j--
		}
		active[j] = cur
	}
}

func buildPostingApplyActive(dst, active []int, preds []predicate) []int {
	dst = dst[:0]
	for _, pi := range active {
		if preds[pi].supportsPostingApply() {
			dst = append(dst, pi)
		}
	}
	return dst
}

type plannerExactBucketPostingFilterPredicate interface {
	supportsExactBucketPostingFilter() bool
}

func buildExactBucketPostingFilterActive[T plannerExactBucketPostingFilterPredicate](dst, active []int, preds []T) []int {
	dst = dst[:0]
	for _, pi := range active {
		if preds[pi].supportsExactBucketPostingFilter() {
			dst = append(dst, pi)
		}
	}
	return dst
}

func releasePredicateRuntimeState(p *predicate) {
	if p == nil {
		return
	}
	if p.baseRangeState != nil {
		releaseBaseRangePredicateState(p.baseRangeState)
		p.baseRangeState = nil
	}
	if p.overlayState != nil {
		releaseOverlayRangePredicateState(p.overlayState)
		p.overlayState = nil
	}
	if p.postsAnyState != nil {
		releasePostsAnyFilterState(p.postsAnyState)
		p.postsAnyState = nil
	}
	if p.lazyMatState != nil {
		releaseLazyMaterializedPredicateState(p.lazyMatState)
		p.lazyMatState = nil
	}
	if p.postsBuf != nil {
		p.postsBuf.values = p.posts
		releasePostingSliceBuf(p.postsBuf)
		p.postsBuf = nil
	}
}

func releasePredicateOwnedState(p *predicate) {
	if p == nil {
		return
	}
	releasePredicateRuntimeState(p)
	if p.releaseIDs {
		p.ids.Release()
		p.releaseIDs = false
	}
	p.posting = posting.List{}
	p.posts = nil
	p.ids = posting.List{}
	p.rangeMat = false
	p.contains = nil
	p.iter = nil
	p.bucketCount = nil
	p.postingFilter = nil
	p.postingFilterCheap = false
	p.estCard = 0
	p.alwaysTrue = false
	p.alwaysFalse = false
	p.covered = false
}

func setPredicateAlwaysTrue(p *predicate) {
	if p == nil {
		return
	}
	releasePredicateOwnedState(p)
	p.kind = predicateKindCustom
	p.iterKind = predicateIterNone
	p.alwaysTrue = true
	p.alwaysFalse = false
}

func setPredicateMaterializedNot(p *predicate, ids posting.List) {
	if p == nil {
		return
	}
	if ids.IsEmpty() {
		setPredicateAlwaysTrue(p)
		return
	}
	releasePredicateOwnedState(p)
	p.kind = predicateKindMaterializedNot
	p.iterKind = predicateIterNone
	p.ids = ids
	p.releaseIDs = true
	p.alwaysTrue = false
	p.alwaysFalse = false
}

func releasePredicates(preds []predicate, owner ...*predicateSliceBuf) {
	var buf *predicateSliceBuf
	if len(owner) != 0 {
		buf = owner[0]
	}
	for i := range preds {
		releasePredicateOwnedState(&preds[i])
		preds[i] = predicate{}
	}
	if buf != nil {
		buf.values = preds
		releasePredicateSliceBuf(buf)
	}
}

func (qv *queryView[K, V]) buildPredicatesWithMode(leaves []qx.Expr, allowMaterialize bool) ([]predicate, *predicateSliceBuf, bool) {
	if len(leaves) == 1 && leaves[0].Op == qx.OpNOOP && leaves[0].Not {
		return []predicate{{alwaysFalse: true}}, nil, true
	}

	var predsBuf *predicateSliceBuf
	var preds []predicate
	if len(leaves) >= predicateSlicePoolMinLen {
		predsBuf = getPredicateSliceBuf(len(leaves))
		preds = predsBuf.values[:0]
	} else {
		preds = make([]predicate, 0, len(leaves))
	}
	for _, e := range leaves {
		p, ok := qv.buildPredicateWithMode(e, allowMaterialize)
		if !ok {
			releasePredicates(preds, predsBuf)
			return nil, nil, false
		}
		preds = append(preds, p)
	}
	return preds, predsBuf, true
}

func (qv *queryView[K, V]) buildPredicates(leaves []qx.Expr) ([]predicate, *predicateSliceBuf, bool) {
	return qv.buildPredicatesWithMode(leaves, true)
}

func isOrderRangeCoveredLeaf(orderField string, e qx.Expr) bool {
	switch classifyOrderFieldScalarLeaf(orderField, e) {
	case orderFieldScalarLeafRange, orderFieldScalarLeafPrefix:
		return true
	default:
		return false
	}
}

func (qv *queryView[K, V]) buildPredicatesOrdered(leaves []qx.Expr, orderField string) ([]predicate, *predicateSliceBuf, bool) {
	return qv.buildPredicatesOrderedWithMode(leaves, orderField, true, 0, true, true)
}

type orderedScalarRangeRouting struct {
	eagerMaterialize bool
	broadComplement  bool
}

func (qv *queryView[K, V]) orderedScalarRangeRouting(e qx.Expr, orderedWindow int, universe uint64) orderedScalarRangeRouting {
	if universe == 0 || e.Not || e.Field == "" || !isNumericRangeOp(e.Op) {
		return orderedScalarRangeRouting{}
	}
	candidate, ok := qv.prepareScalarRangeRoutingCandidate(e)
	if !ok || !candidate.numeric {
		return orderedScalarRangeRouting{}
	}

	route := orderedScalarRangeRouting{
		eagerMaterialize: candidate.plan.orderedEagerMaterializeUseful(orderedWindow, universe),
	}
	route.broadComplement = candidate.broadComplementCardinality(universe)
	return route
}

func overlayRangeEmpty(br overlayRange) bool {
	return br.baseStart >= br.baseEnd
}

func (qv *queryView[K, V]) tryMaterializeBroadRangeComplementPredicateForOrdered(p *predicate, broadComplement bool, universe uint64, orderedWindow int) bool {
	if p == nil || !broadComplement {
		return false
	}

	fm := qv.fields[p.expr.Field]
	if fm == nil || fm.Slice {
		return false
	}
	candidate, ok := qv.preparePredicateScalarRangeRoutingCandidate(*p)
	if !ok {
		return false
	}
	plan, cached, cacheHit, empty, ok := candidate.core.loadComplementMaterialization()
	if !ok {
		return false
	}
	if cacheHit {
		setPredicateMaterializedNot(p, cached)
		return true
	}
	if empty {
		storeEmptyScalarComplementMaterialization(plan)
		setPredicateAlwaysTrue(p)
		return true
	}
	if plan.est >= p.estCard {
		return false
	}
	expectedRows := orderedPredicateExpectedRows(orderedWindow, p.estCard, universe)
	if expectedRows == 0 {
		return false
	}
	buildWork := postingUnionLinearWork(plan.buckets, plan.est)
	materializedWork := satAddUint64(buildWork, satMulUint64(expectedRows, postingContainsLookupWork(plan.est)))
	probeWork := rangeProbeTotalWorkForRows(int(expectedRows), plan.buckets, plan.est)
	if materializedWork >= probeWork {
		return false
	}
	if probeWork < satMulUint64(buildWork, materializedShareStructuralFactor(plan.buckets)) {
		return false
	}
	ids := candidate.core.materializeComplement(plan)
	if ids.IsEmpty() {
		storeEmptyScalarComplementMaterialization(plan)
		setPredicateAlwaysTrue(p)
		return true
	}
	ids = plan.sharedReuse.share(ids)
	setPredicateMaterializedNot(p, ids)
	return true
}

func (qv *queryView[K, V]) isPositiveOrderedNumericRangeLeaf(e qx.Expr, orderField string) bool {
	if e.Not || e.Field == "" || e.Field == orderField || !isNumericRangeOp(e.Op) {
		return false
	}
	fm := qv.fields[e.Field]
	return fm != nil && !fm.Slice && isNumericScalarKind(fm.Kind)
}

func (qv *queryView[K, V]) buildMergedNumericRangePredicate(
	e qx.Expr,
	bounds rangeBounds,
	allowMaterialize bool,
) (predicate, bool) {
	fm := qv.fields[e.Field]
	if fm == nil || fm.Slice {
		return predicate{}, false
	}
	core := qv.prepareExactScalarRangePredicate(e, fm, bounds)
	ov := qv.fieldOverlay(e.Field)
	if !ov.hasData() {
		return predicate{}, false
	}
	if ov.chunked != nil {
		p, ok := core.buildFromOverlay(ov, allowMaterialize)
		if ok {
			p.effectiveBounds = bounds
			p.hasEffectiveBounds = true
		}
		return p, ok
	}
	slice := qv.snapshotFieldIndexSlice(e.Field)
	if slice == nil {
		return predicate{}, false
	}
	p, ok := core.buildFromSlice(*slice, allowMaterialize)
	if ok {
		p.effectiveBounds = bounds
		p.hasEffectiveBounds = true
	}
	return p, ok
}

func (qv *queryView[K, V]) buildPredicatesOrderedWithMode(leaves []qx.Expr, orderField string, allowMaterialize bool, orderedWindow int, coverOrderRange bool, allowOrderedEagerMaterialize bool) ([]predicate, *predicateSliceBuf, bool) {
	if len(leaves) == 1 && leaves[0].Op == qx.OpNOOP && leaves[0].Not {
		return []predicate{{alwaysFalse: true}}, nil, true
	}

	var predsBuf *predicateSliceBuf
	var preds []predicate
	if len(leaves) >= predicateSlicePoolMinLen {
		predsBuf = getPredicateSliceBuf(len(leaves))
		preds = predsBuf.values[:0]
	} else {
		preds = make([]predicate, 0, len(leaves))
	}
	universe := uint64(0)
	if !allowMaterialize {
		universe = qv.snapshotUniverseCardinality()
	}
	var mergedRangesBuf *orderedMergedScalarRangeFieldSliceBuf
	var mergedRanges []orderedMergedScalarRangeField
	if len(leaves) > 1 {
		mergedRangesBuf = getOrderedMergedScalarRangeFieldSliceBuf(len(leaves))
		var ok bool
		mergedRanges, ok = qv.collectOrderedMergedScalarRangeFields(orderField, leaves, mergedRangesBuf.values[:0])
		if !ok {
			releasePredicates(preds, predsBuf)
			releaseOrderedMergedScalarRangeFieldSliceBuf(mergedRangesBuf)
			return nil, nil, false
		}
		defer func() {
			mergedRangesBuf.values = mergedRanges
			releaseOrderedMergedScalarRangeFieldSliceBuf(mergedRangesBuf)
		}()
	}
	for i, e := range leaves {
		orderRangeDeferred := !coverOrderRange && isOrderRangeCoveredLeaf(orderField, e)
		if coverOrderRange && isOrderRangeCoveredLeaf(orderField, e) {
			// Keep conversion/typing validation, but avoid expensive predicate
			// materialization for leaves that are fully covered by order range.
			rb, ok, err := qv.rangeBoundsForScalarExpr(e)
			if err != nil || !ok || rb.empty {
				releasePredicates(preds, predsBuf)
				return nil, nil, false
			}
			preds = append(preds, predicate{
				expr:    e,
				covered: true,
			})
			continue
		}

		if qv.isPositiveOrderedNumericRangeLeaf(e, orderField) {
			idx := findOrderedMergedScalarRangeField(mergedRanges, e.Field)
			if idx >= 0 && mergedRanges[idx].count > 1 {
				if mergedRanges[idx].first != i {
					continue
				}
				p, ok := qv.buildMergedNumericRangePredicate(mergedRanges[idx].expr, mergedRanges[idx].bounds, allowMaterialize)
				if !ok {
					releasePredicates(preds, predsBuf)
					return nil, nil, false
				}
				if p.baseRangeState != nil && orderedWindow > 0 && universe > 0 {
					p.setExpectedContainsCalls(
						orderedBaseRangeExpectedContainsCalls(p.baseRangeState, orderedWindow, universe),
					)
				}
				preds = append(preds, p)
				continue
			}
		}

		predAllowMaterialize := allowMaterialize
		orderedEagerMaterialize := false
		orderedRoute := orderedScalarRangeRouting{}
		if !predAllowMaterialize {
			orderedRoute = qv.orderedScalarRangeRouting(e, orderedWindow, universe)
		}
		if !predAllowMaterialize && allowOrderedEagerMaterialize &&
			!orderRangeDeferred &&
			orderedMergedScalarRangeFieldCount(mergedRanges, e.Field) <= 1 &&
			e.Field != orderField &&
			orderedRoute.eagerMaterialize {
			predAllowMaterialize = true
			orderedEagerMaterialize = true
		}
		p, ok := qv.buildPredicateWithMode(e, predAllowMaterialize)
		if !ok {
			releasePredicates(preds, predsBuf)
			return nil, nil, false
		}
		if p.baseRangeState != nil && orderedWindow > 0 && universe > 0 {
			p.setExpectedContainsCalls(
				orderedBaseRangeExpectedContainsCalls(p.baseRangeState, orderedWindow, universe),
			)
		}
		if !orderRangeDeferred && orderedEagerMaterialize &&
			p.hasRuntimeRangeState() {
			qv.tryMaterializeBroadRangeComplementPredicateForOrdered(&p, orderedRoute.broadComplement, universe, orderedWindow)
		} else if !predAllowMaterialize && !orderRangeDeferred {
			qv.tryMaterializeBroadRangeComplementPredicateForOrdered(&p, orderedRoute.broadComplement, universe, orderedWindow)
		}
		preds = append(preds, p)
	}
	return preds, predsBuf, true
}

func (qv *queryView[K, V]) buildPredicateWithMode(e qx.Expr, allowMaterialize bool) (predicate, bool) {
	if e.Op == qx.OpNOOP {
		if e.Not {
			return predicate{expr: e, alwaysFalse: true}, true
		}
		return predicate{expr: e, alwaysTrue: true}, true
	}

	if e.Field == "" {
		return predicate{}, false
	}

	fm := qv.fields[e.Field]
	if fm == nil {
		return predicate{}, false
	}

	if isScalarRangeOrPrefixOp(e.Op) {
		ov := qv.fieldOverlay(e.Field)
		if !ov.hasData() {
			return predicate{}, false
		}
		if ov.chunked != nil {
			return qv.buildPredRangeCandidateWithMode(e, fm, ov, allowMaterialize)
		}
		slice := qv.snapshotFieldIndexSlice(e.Field)
		if slice == nil {
			return predicate{}, false
		}
		return qv.buildPredRangeWithMode(e, fm, slice, allowMaterialize)
	}
	p, ok := qv.buildPredicateCandidate(e)
	if !ok {
		return predicate{}, false
	}
	return p, true
}

func (qv *queryView[K, V]) buildPredRangeWithMode(e qx.Expr, fm *field, slice *[]index, allowMaterialize bool) (predicate, bool) {
	core, pred, done, ok := qv.prepareScalarRangePredicate(e, fm)
	if !ok {
		return predicate{}, false
	}
	if done {
		return pred, true
	}
	return core.buildFromSlice(*slice, allowMaterialize)
}

type baseRangeProbe struct {
	s              []index
	start          int
	end            int
	useComplement  bool
	probeLen       int
	probeEst       uint64
	singletonCount int
}

func newBaseRangeProbe(s []index, start, end int, useComplement bool) baseRangeProbe {
	p := baseRangeProbe{
		s:             s,
		start:         start,
		end:           end,
		useComplement: useComplement,
	}
	p.scanStats()
	return p
}

func (p *baseRangeProbe) scanStats() {
	if p == nil {
		return
	}
	p.forEachPosting(func(ids posting.List) bool {
		p.probeLen++
		card := ids.Cardinality()
		if card == 1 {
			p.singletonCount++
		}
		if ^uint64(0)-p.probeEst < card {
			p.probeEst = ^uint64(0)
		} else {
			p.probeEst += card
		}
		return true
	})
}

func (p baseRangeProbe) forEachPosting(fn func(posting.List) bool) bool {
	if p.useComplement {
		for i := 0; i < p.start; i++ {
			if ids := p.s[i].IDs; !ids.IsEmpty() {
				if !fn(ids) {
					return false
				}
			}
		}
		for i := p.end; i < len(p.s); i++ {
			if ids := p.s[i].IDs; !ids.IsEmpty() {
				if !fn(ids) {
					return false
				}
			}
		}
		return true
	}

	for i := p.start; i < p.end; i++ {
		if ids := p.s[i].IDs; !ids.IsEmpty() {
			if !fn(ids) {
				return false
			}
		}
	}
	return true
}

func (p baseRangeProbe) linearContains(idx uint64) bool {
	if p.useComplement {
		for i := 0; i < p.start; i++ {
			if ids := p.s[i].IDs; !ids.IsEmpty() && ids.Contains(idx) {
				return true
			}
		}
		for i := p.end; i < len(p.s); i++ {
			if ids := p.s[i].IDs; !ids.IsEmpty() && ids.Contains(idx) {
				return true
			}
		}
		return false
	}

	for i := p.start; i < p.end; i++ {
		if ids := p.s[i].IDs; !ids.IsEmpty() && ids.Contains(idx) {
			return true
		}
	}
	return false
}

func (p baseRangeProbe) countBucket(bucket posting.List) (uint64, bool) {
	if bucket.IsEmpty() {
		return 0, true
	}
	if !rangeCountBucketUseful(p.probeLen, p.probeEst, bucket.Cardinality()) {
		return 0, false
	}

	var hit uint64
	if p.useComplement {
		for i := 0; i < p.start; i++ {
			if ids := p.s[i].IDs; !ids.IsEmpty() {
				hit += ids.AndCardinality(bucket)
			}
		}
		for i := p.end; i < len(p.s); i++ {
			if ids := p.s[i].IDs; !ids.IsEmpty() {
				hit += ids.AndCardinality(bucket)
			}
		}
	} else {
		for i := p.start; i < p.end; i++ {
			if ids := p.s[i].IDs; !ids.IsEmpty() {
				hit += ids.AndCardinality(bucket)
			}
		}
	}

	if !p.useComplement {
		return hit, true
	}

	bc := bucket.Cardinality()
	if hit >= bc {
		return 0, true
	}
	return bc - hit, true
}

func (p baseRangeProbe) singletonHeavy() bool {
	return shouldUseFastSinglesUnion(p.probeLen, p.singletonCount, p.probeEst)
}

const (
	rangePostingFilterKeepProbeMaxBuckets = 24
	rangePostingFilterKeepRowsMaxCard     = 8_192
)

func shouldUseNumericRangePostingFilter(e qx.Expr, fm *field) bool {
	if fm == nil || fm.Slice || !isNumericScalarKind(fm.Kind) || !isNumericRangeOp(e.Op) {
		return false
	}
	return true
}

func applyRangeProbePostingFilter(
	dst posting.List,
	keepProbeHits bool,
	probeLen int,
	probeContains func(uint64) bool,
	forEachPosting func(func(posting.List) bool) bool,
) (posting.List, bool) {
	if dst.IsEmpty() {
		return dst, true
	}
	if probeLen == 0 {
		if keepProbeHits {
			dst.Release()
			dst = posting.List{}
		}
		return dst, true
	}
	rowProbeBudget := rangePostingFilterKeepRowsMaxCard
	if rowProbeBudget < 1 {
		rowProbeBudget = 1
	}
	maxRowsForContains := rowProbeBudget / probeLen
	if maxRowsForContains < 1 {
		maxRowsForContains = 1
	}
	if !keepProbeHits {
		forEachPosting(func(ids posting.List) bool {
			dst = dst.BuildAndNot(ids)
			return !dst.IsEmpty()
		})
		return dst, true
	}
	if dst.Cardinality() <= posting.MidCap {
		var matched [posting.MidCap]uint64
		n := 0
		dst.ForEach(func(idx uint64) bool {
			if probeContains(idx) {
				matched[n] = idx
				n++
			}
			return true
		})
		dst.Release()
		return posting.BuildFromSorted(matched[:n]), true
	}
	if dst.Cardinality() <= uint64(maxRowsForContains) {
		builder := newPostingUnionBuilder(true)
		defer builder.release()
		dst.ForEach(func(idx uint64) bool {
			if probeContains(idx) {
				builder.addSingle(idx)
			}
			return true
		})
		dst.Release()
		return builder.finish(false), true
	}
	if probeLen <= rangePostingFilterKeepProbeMaxBuckets {
		builder := newPostingUnionBuilder(true)
		defer builder.release()
		forEachPosting(func(ids posting.List) bool {
			ids.ForEachIntersecting(dst, func(idx uint64) bool {
				builder.addSingle(idx)
				return false
			})
			return true
		})
		dst.Release()
		return builder.finish(false), true
	}
	if probeLen > rangePostingFilterKeepProbeMaxBuckets && dst.Cardinality() > rangePostingFilterKeepRowsMaxCard {
		return posting.List{}, false
	}

	builder := newPostingUnionBuilder(true)
	defer builder.release()
	dst.ForEach(func(idx uint64) bool {
		if probeContains(idx) {
			builder.addSingle(idx)
		}
		return true
	})
	dst.Release()
	return builder.finish(false), true
}

type overlayRangeProbe struct {
	ov            fieldOverlay
	spans         [2]overlayRange
	spanCnt       int
	useComplement bool
	probeLen      int
	probeEst      uint64
}

func newOverlayRangeProbe(ov fieldOverlay, br overlayRange, useComplement bool) overlayRangeProbe {
	p := overlayRangeProbe{ov: ov, useComplement: useComplement}
	if useComplement {
		before, after := overlayComplementRangeSpans(ov, br)
		if !overlayRangeEmpty(before) {
			p.spans[p.spanCnt] = before
			p.spanCnt++
		}
		if !overlayRangeEmpty(after) {
			p.spans[p.spanCnt] = after
			p.spanCnt++
		}
	} else if !overlayRangeEmpty(br) {
		p.spans[0] = br
		p.spanCnt = 1
	}
	p.scanStats()
	return p
}

func (p *overlayRangeProbe) scanStats() {
	if p == nil {
		return
	}
	p.forEachPosting(func(ids posting.List) bool {
		p.probeLen++
		card := ids.Cardinality()
		if ^uint64(0)-p.probeEst < card {
			p.probeEst = ^uint64(0)
		} else {
			p.probeEst += card
		}
		return true
	})
}

func (p overlayRangeProbe) forEachPosting(fn func(posting.List) bool) bool {
	for i := 0; i < p.spanCnt; i++ {
		cur := p.ov.newCursor(p.spans[i], false)
		for {
			_, ids, ok := cur.next()
			if !ok {
				break
			}
			if ids.IsEmpty() {
				continue
			}
			if !fn(ids) {
				return false
			}
		}
	}
	return true
}

func (p overlayRangeProbe) linearContains(idx uint64) bool {
	for i := 0; i < p.spanCnt; i++ {
		cur := p.ov.newCursor(p.spans[i], false)
		for {
			_, ids, ok := cur.next()
			if !ok {
				break
			}
			if !ids.IsEmpty() && ids.Contains(idx) {
				return true
			}
		}
	}
	return false
}

func (p overlayRangeProbe) countBucket(bucket posting.List) (uint64, bool) {
	if bucket.IsEmpty() {
		return 0, true
	}
	if !rangeCountBucketUseful(p.probeLen, p.probeEst, bucket.Cardinality()) {
		return 0, false
	}

	var hit uint64
	p.forEachPosting(func(ids posting.List) bool {
		hit += ids.AndCardinality(bucket)
		return true
	})

	if !p.useComplement {
		return hit, true
	}

	bc := bucket.Cardinality()
	if hit >= bc {
		return 0, true
	}
	return bc - hit, true
}

func materializedRangePredicateWithMode(e qx.Expr, ids posting.List) predicate {
	if ids.IsEmpty() {
		if e.Not {
			return predicate{expr: e, alwaysTrue: true}
		}
		return predicate{expr: e, alwaysFalse: true}
	}

	if e.Not {
		return predicate{
			expr:       e,
			ids:        ids,
			rangeMat:   true,
			releaseIDs: true,
		}
	}

	return predicate{
		expr:       e,
		ids:        ids,
		rangeMat:   true,
		estCard:    ids.Cardinality(),
		releaseIDs: true,
	}
}

func materializeBaseRangeProbeFast(probe baseRangeProbe) posting.List {
	builder := newPostingUnionBuilder(true)
	defer builder.release()

	probe.forEachPosting(func(p posting.List) bool {
		builder.addPosting(p)
		return true
	})
	return builder.finish(true)
}

func isSingletonHeavyProbe(probe []posting.List) bool {
	probeLen := 0
	singletons := 0
	est := uint64(0)
	for _, p := range probe {
		if p.IsEmpty() {
			continue
		}
		probeLen++
		card := p.Cardinality()
		if card == 1 {
			singletons++
		}
		if ^uint64(0)-est < card {
			est = ^uint64(0)
		} else {
			est += card
		}
	}
	return shouldUseFastSinglesUnion(probeLen, singletons, est)
}

func materializeProbeFast(probe []posting.List) posting.List {
	builder := newPostingUnionBuilder(true)
	defer builder.release()

	for _, p := range probe {
		builder.addPosting(p)
	}
	return builder.finish(true)
}

const (
	plannerOrderedAnchorMinActive        = 2
	plannerOrderedAnchorSpanMin          = 256
	plannerOrderedAnchorLeadBudgetMin    = 4_096
	plannerOrderedAnchorLeadBudgetMax    = 350_000
	plannerOrderedAnchorLeadNeedMul      = 32
	plannerOrderedAnchorCandidateCapMin  = 8_192
	plannerOrderedAnchorCandidateCapMax  = 300_000
	plannerOrderedAnchorCandidateNeedMul = 64
)

func plannerOrderedAnchorBudgets(needWindow int, activeCount int, leadEst uint64) (leadBudget uint64, candidateCap uint64) {
	leadBudget = uint64(max(
		plannerOrderedAnchorLeadBudgetMin,
		min(plannerOrderedAnchorLeadBudgetMax, needWindow*plannerOrderedAnchorLeadNeedMul),
	))
	candidateCap = uint64(max(
		plannerOrderedAnchorCandidateCapMin,
		min(plannerOrderedAnchorCandidateCapMax, needWindow*plannerOrderedAnchorCandidateNeedMul),
	))

	if activeCount >= 4 {
		leadBudget = leadBudget * 3 / 4
		candidateCap = candidateCap * 3 / 4
	} else if activeCount == 2 {
		leadBudget = leadBudget * 9 / 8
	}

	if leadEst > 0 {
		estLeadCap := leadEst * 6
		if estLeadCap < leadEst {
			estLeadCap = leadEst
		}
		if estLeadCap < leadBudget {
			leadBudget = estLeadCap
		}

		estCandidateCap := leadEst * 8
		if estCandidateCap < leadEst {
			estCandidateCap = leadEst
		}
		if estCandidateCap < candidateCap {
			floor := uint64(max(needWindow*2, plannerOrderedAnchorCandidateCapMin/2))
			if estCandidateCap < floor {
				estCandidateCap = floor
			}
			candidateCap = estCandidateCap
		}
	}

	minLeadBudget := uint64(plannerOrderedAnchorLeadBudgetMin)
	maxLeadBudget := uint64(plannerOrderedAnchorLeadBudgetMax)
	if leadBudget < minLeadBudget {
		leadBudget = minLeadBudget
	}
	if leadBudget > maxLeadBudget {
		leadBudget = maxLeadBudget
	}

	minCandidateCap := uint64(plannerOrderedAnchorCandidateCapMin)
	maxCandidateCap := uint64(plannerOrderedAnchorCandidateCapMax)
	if candidateCap < minCandidateCap {
		candidateCap = minCandidateCap
	}
	if candidateCap > maxCandidateCap {
		candidateCap = maxCandidateCap
	}

	return leadBudget, candidateCap
}

func orderedAnchorLeadOpWeight(op qx.Op) float64 {
	switch op {
	case qx.OpEQ:
		return 1.0
	case qx.OpIN, qx.OpHAS, qx.OpHASANY:
		return 1.35
	default:
		if isScalarRangeOrPrefixOp(op) {
			return 1.9
		}
		return 2.5
	}
}

func orderedPredicateIsRangeLike(p predicate) bool {
	return isScalarRangeOrPrefixOp(p.expr.Op)
}

func chooseOrderedAnchorLead(orderField string, preds []predicate, active []int) (int, bool) {
	lead := -1
	bestCard := uint64(0)
	bestScore := 0.0
	bestWeight := 0.0

	for _, pi := range active {
		p := preds[pi]
		if !p.hasIter() || p.estCard == 0 {
			continue
		}
		weight := orderedAnchorLeadOpWeight(p.expr.Op)
		if p.expr.Field == orderField {
			// Predicates on the order field usually provide weak anchor signal.
			weight *= 2.8
		}
		score := float64(p.estCard) * weight

		if lead == -1 ||
			score < bestScore ||
			(score == bestScore && p.estCard < bestCard) ||
			(score == bestScore && p.estCard == bestCard && weight < bestWeight) {
			lead = pi
			bestCard = p.estCard
			bestScore = score
			bestWeight = weight
		}
	}

	return lead, lead >= 0
}

func (qv *queryView[K, V]) execPlanOrderedBasicAnchored(q *qx.QX, preds []predicate, active []int, ov fieldOverlay, br overlayRange, trace *queryTrace) ([]K, bool) {
	if len(active) < plannerOrderedAnchorMinActive {
		return nil, false
	}
	if br.baseEnd-br.baseStart < plannerOrderedAnchorSpanMin {
		return nil, false
	}

	leadIdx, ok := chooseOrderedAnchorLead(q.Order[0].Field, preds, active)
	if !ok {
		return nil, false
	}

	needWindow, ok := orderWindow(q)
	if !ok || needWindow <= 0 {
		return nil, false
	}

	if orderedPredicateIsRangeLike(preds[leadIdx]) {
		rangeLeadMax := uint64(max(needWindow*64, 65_536))
		if preds[leadIdx].estCard > rangeLeadMax {
			return nil, false
		}
	}

	leadBudget, candidateCap := plannerOrderedAnchorBudgets(needWindow, len(active), preds[leadIdx].estCard)
	if br.baseStart == 0 && br.baseEnd == ov.keyCount() {
		wideLeadMax := candidateCap * 8
		if wideLeadMax < candidateCap {
			wideLeadMax = ^uint64(0)
		}
		if preds[leadIdx].estCard > wideLeadMax {
			return nil, false
		}
	}

	leadIt := preds[leadIdx].newIter()
	if leadIt == nil {
		return nil, false
	}
	defer leadIt.Release()
	leadNeedsCheck := preds[leadIdx].leadIterNeedsContainsCheck()

	exactChecksBuf := getIntSliceBuf(len(active))
	exactChecksBuf.values = exactChecksBuf.values[:0]
	defer func() {
		releaseIntSliceBuf(exactChecksBuf)
	}()

	residualChecksBuf := getIntSliceBuf(len(active))
	residualChecksBuf.values = residualChecksBuf.values[:0]
	defer func() {
		releaseIntSliceBuf(residualChecksBuf)
	}()
	deferredChecksBuf := getIntSliceBuf(len(active))
	deferredChecksBuf.values = deferredChecksBuf.values[:0]
	defer func() {
		releaseIntSliceBuf(deferredChecksBuf)
	}()

	orderedExactPreferred := false
	for _, pi := range active {
		if pi == leadIdx && !leadNeedsCheck {
			continue
		}
		deferredChecksBuf.values = append(deferredChecksBuf.values, pi)
	}
	exactChecksBuf.values = buildExactBucketPostingFilterActive(exactChecksBuf.values[:0], deferredChecksBuf.values, preds)
	for _, pi := range exactChecksBuf.values {
		if preds[pi].prefersExactBucketPostingFilter() {
			orderedExactPreferred = true
			break
		}
	}
	residualChecksBuf.values = plannerResidualChecks(residualChecksBuf.values[:0], deferredChecksBuf.values, exactChecksBuf.values)
	useExactPosting := len(exactChecksBuf.values) > 0 && (len(residualChecksBuf.values) == 0 || orderedExactPreferred)

	var candidates posting.List

	leadExamined := uint64(0)
	if useExactPosting {
		builder := newPostingUnionBuilder(postingBatchSinglesEnabled(leadBudget))
		defer builder.release()
		for leadIt.HasNext() {
			leadExamined++
			if leadExamined > leadBudget {
				return nil, false
			}
			if trace != nil {
				trace.addExamined(1)
			}
			builder.addSingle(leadIt.Next())
		}
		candidates = builder.finish(false)
	} else {
		if predicateLeadIterMayDuplicate(preds[leadIdx]) {
			builder := newPostingSetBuilder(candidateCap, postingBatchSinglesEnabled(candidateCap))
			defer builder.release()
			for leadIt.HasNext() {
				leadExamined++
				if leadExamined > leadBudget {
					return nil, false
				}
				if trace != nil {
					trace.addExamined(1)
				}

				idx := leadIt.Next()

				pass := true
				for _, pi := range active {
					if pi == leadIdx && !leadNeedsCheck {
						continue
					}
					p := preds[pi]
					if !p.hasContains() || !p.matches(idx) {
						pass = false
						break
					}
				}
				if !pass {
					continue
				}

				if builder.addChecked(idx) && builder.cardinality() > candidateCap {
					return nil, false
				}
			}
			candidates = builder.finish(false)
		} else {
			builder := newPostingUnionBuilder(postingBatchSinglesEnabled(candidateCap))
			defer builder.release()
			candidateCount := uint64(0)
			for leadIt.HasNext() {
				leadExamined++
				if leadExamined > leadBudget {
					return nil, false
				}
				if trace != nil {
					trace.addExamined(1)
				}

				idx := leadIt.Next()

				pass := true
				for _, pi := range active {
					if pi == leadIdx && !leadNeedsCheck {
						continue
					}
					p := preds[pi]
					if !p.hasContains() || !p.matches(idx) {
						pass = false
						break
					}
				}
				if !pass {
					continue
				}

				builder.addSingle(idx)
				candidateCount++
				if candidateCount > candidateCap {
					return nil, false
				}
			}
			candidates = builder.finish(false)
		}
	}

	if candidates.IsEmpty() {
		if trace != nil {
			trace.setEarlyStopReason("no_candidates")
		}
		return nil, true
	}

	candidateIDs := candidates
	if useExactPosting {
		exactApplied := false
		exactWork := posting.List{}
		mode, exactIDs, exactWork, _ := plannerFilterPostingByChecks(preds, exactChecksBuf.values, candidates, exactWork, true)
		defer exactWork.Release()
		switch mode {
		case plannerPredicateBucketEmpty:
			if trace != nil {
				trace.setEarlyStopReason("no_candidates")
			}
			return nil, true
		case plannerPredicateBucketAll:
			candidateIDs = candidates
			exactApplied = true
		case plannerPredicateBucketExact:
			if trace != nil {
				trace.addPostingExactFilter(1)
			}
			candidateIDs = exactIDs
			exactApplied = true
		default:
			candidateIDs = candidates
		}
		if candidateIDs.IsEmpty() {
			if trace != nil {
				trace.setEarlyStopReason("no_candidates")
			}
			return nil, true
		}
		if exactApplied && len(residualChecksBuf.values) > 0 {
			builder := newPostingUnionBuilder(postingBatchSinglesEnabled(candidateIDs.Cardinality()))
			candidateIDs.ForEach(func(idx uint64) bool {
				pass := true
				for _, pi := range residualChecksBuf.values {
					p := preds[pi]
					if !p.hasContains() || !p.matches(idx) {
						pass = false
						break
					}
				}
				if pass {
					builder.addSingle(idx)
				}
				return true
			})
			filtered := builder.finish(false)
			if filtered.IsEmpty() {
				if trace != nil {
					trace.setEarlyStopReason("no_candidates")
				}
				return nil, true
			}
			candidateIDs = filtered
		}
		if exactApplied {
			deferredChecksBuf.values = deferredChecksBuf.values[:0]
		}
		if candidateIDs.Cardinality() > candidateCap {
			return nil, false
		}
	}

	o := q.Order[0]
	skip := q.Offset
	need := q.Limit
	out := make([]K, 0, need)
	totalCandidates := candidateIDs.Cardinality()
	seenCandidates := uint64(0)
	scanWidth := uint64(0)
	singleDeferred := -1
	if len(deferredChecksBuf.values) == 1 {
		singleDeferred = deferredChecksBuf.values[0]
	}

	emitBucket := func(ids posting.List) bool {
		if ids.IsEmpty() {
			return false
		}
		scanWidth++
		if trace != nil {
			trace.addExamined(ids.Cardinality())
		}

		stop := false
		ids.ForEach(func(idx uint64) bool {
			if !candidateIDs.Contains(idx) {
				return true
			}
			seenCandidates++
			if singleDeferred >= 0 {
				p := preds[singleDeferred]
				if !p.hasContains() || !p.matches(idx) {
					return true
				}
			} else {
				for _, pi := range deferredChecksBuf.values {
					p := preds[pi]
					if !p.hasContains() || !p.matches(idx) {
						return true
					}
				}
			}

			if skip > 0 {
				skip--
				return true
			}

			out = append(out, qv.idFromIdxNoLock(idx))
			need--
			if need == 0 {
				stop = true
				return false
			}
			return true
		})
		return stop
	}

	keyCur := ov.newCursor(br, o.Desc)
	for {
		_, ids, ok := keyCur.next()
		if !ok {
			break
		}
		if emitBucket(ids) {
			break
		}
		if seenCandidates == totalCandidates {
			break
		}
	}

	if trace != nil {
		trace.addOrderScanWidth(scanWidth)
		if need == 0 {
			trace.setEarlyStopReason("limit_reached")
		} else if seenCandidates == totalCandidates {
			trace.setEarlyStopReason("candidates_exhausted")
		} else {
			trace.setEarlyStopReason("order_index_exhausted")
		}
	}

	return out, true
}

func (qv *queryView[K, V]) execPlanOrderedBasic(q *qx.QX, preds []predicate, trace *queryTrace) ([]K, bool) {
	o := q.Order[0]

	fm := qv.fields[o.Field]
	if fm == nil || fm.Slice {
		return nil, false
	}
	ov := qv.fieldOverlay(o.Field)
	nilOV := qv.nilFieldOverlay(o.Field)
	if !ov.hasData() && !nilOV.hasData() {
		return nil, false
	}

	rb, br, rangeCovered, ok := qv.extractOrderRangeCoverageOverlayWithBounds(o.Field, preds, ov)
	if !ok {
		return nil, false
	}
	nilTailField := orderNilTailField(fm, o.Field, rb)
	if overlayRangeEmpty(br) && nilTailField == "" {
		return nil, true
	}
	for i := range rangeCovered {
		if rangeCovered[i] {
			preds[i].covered = true
		}
	}

	activeBuf := getIntSliceBuf(len(preds))
	active := activeBuf.values
	defer func() {
		activeBuf.values = active
		releaseIntSliceBuf(activeBuf)
	}()
	for i := range preds {
		p := preds[i]
		if p.covered || p.alwaysTrue {
			continue
		}
		if p.alwaysFalse {
			return nil, true
		}
		active = append(active, i)
	}

	if !fm.Ptr {
		if out, ok := qv.execPlanOrderedBasicAnchored(q, preds, active, ov, br, trace); ok {
			if trace != nil {
				trace.setPlan(PlanOrderedAnchor)
			}
			return out, true
		}
	}

	sortActivePredicates(active, preds)
	if len(active) == 0 {
		return qv.scanOrderLimitNoPredicates(q, ov, br, o.Desc, nilTailField, trace)
	}
	return qv.scanOrderLimitWithPredicates(q, ov, br, o.Desc, preds, nilTailField, trace)
}

// execPlanOrderedBasicFallback scans ordered buckets and applies remaining
// predicates when ordered-anchor shortcuts are not available.
func (qv *queryView[K, V]) execPlanOrderedBasicFallback(q *qx.QX, preds []predicate, active []int, start, end int, s []index, trace *queryTrace) []K {
	return qv.execPlanOrderedBasicFallbackWithNilTail(q, preds, active, start, end, s, "", trace)
}

func (qv *queryView[K, V]) execPlanOrderedBasicFallbackWithNilTail(q *qx.QX, preds []predicate, active []int, start, end int, s []index, nilTailField string, trace *queryTrace) []K {
	return qv.scanOrderedBaseSliceWithPredicatesWithNilTail(q, preds, active, start, end, s, q.Order[0].Desc, nilTailField, trace)
}

func (qv *queryView[K, V]) scanOrderedBaseSliceNoPredicatesWithNilTail(q *qx.QX, start, end int, s []index, desc bool, nilTailField string, trace *queryTrace) []K {
	out := make([]K, 0, int(q.Limit))
	cursor := qv.newQueryCursor(out, q.Offset, q.Limit, false, 0)

	var scanWidth uint64

	emitMatched := func(idx uint64) bool {
		if trace != nil {
			trace.addMatched(1)
		}
		return cursor.emit(idx)
	}

	emitBucket := func(bucket posting.List) bool {
		if bucket.IsEmpty() {
			return false
		}
		scanWidth++
		card := bucket.Cardinality()
		if trace != nil {
			trace.addExamined(card)
		}
		if idx, ok := bucket.TrySingle(); ok {
			return emitMatched(idx)
		}
		if cursor.skip >= card {
			cursor.skip -= card
			if trace != nil {
				trace.addMatched(card)
			}
			return false
		}
		stop := false
		bucket.ForEach(func(idx uint64) bool {
			if emitMatched(idx) {
				stop = true
				return false
			}
			return true
		})
		return stop
	}

	if !desc {
		for i := start; i < end; i++ {
			if emitBucket(s[i].IDs) {
				break
			}
		}
	} else {
		for i := end - 1; i >= start; i-- {
			if emitBucket(s[i].IDs) {
				break
			}
			if i == start {
				break
			}
		}
	}

	if cursor.need > 0 && nilTailField != "" {
		ids := qv.nilFieldOverlay(nilTailField).lookupPostingRetained(nilIndexEntryKey)
		emitBucket(ids)
	}

	if trace != nil {
		trace.addOrderScanWidth(scanWidth)
		if cursor.need == 0 {
			trace.setEarlyStopReason("limit_reached")
		} else {
			trace.setEarlyStopReason("order_index_exhausted")
		}
	}

	return cursor.out
}

func (qv *queryView[K, V]) scanOrderedBaseSliceWithPredicatesWithNilTail(q *qx.QX, preds []predicate, active []int, start, end int, s []index, desc bool, nilTailField string, trace *queryTrace) []K {
	if len(active) == 0 {
		return qv.scanOrderedBaseSliceNoPredicatesWithNilTail(q, start, end, s, desc, nilTailField, trace)
	}

	out := make([]K, 0, int(q.Limit))
	cursor := qv.newQueryCursor(out, q.Offset, q.Limit, false, 0)

	exactActiveBuf := getIntSliceBuf(len(active))
	exactActive := buildExactBucketPostingFilterActive(exactActiveBuf.values, active, preds)
	defer func() {
		exactActiveBuf.values = exactActive
		releaseIntSliceBuf(exactActiveBuf)
	}()
	exactOnly := len(active) > 0 && len(active) == len(exactActive)
	residualActiveBuf := getIntSliceBuf(len(active))
	residualActive := plannerResidualChecks(residualActiveBuf.values[:0], active, exactActive)
	defer func() {
		residualActiveBuf.values = residualActive
		releaseIntSliceBuf(residualActiveBuf)
	}()

	singleActive := -1
	if len(active) == 1 {
		singleActive = active[0]
	}

	var exactWork posting.List

	var scanWidth uint64

	emitMatched := func(idx uint64) bool {
		if trace != nil {
			trace.addMatched(1)
		}
		return cursor.emit(idx)
	}

	emitMatchedPosting := func(ids posting.List, card uint64) bool {
		if trace != nil {
			trace.addMatched(card)
		}
		if cursor.skip >= card {
			cursor.skip -= card
			return false
		}
		if idx, ok := ids.TrySingle(); ok {
			return cursor.emit(idx)
		}
		stop := false
		ids.ForEach(func(idx uint64) bool {
			if cursor.emit(idx) {
				stop = true
				return false
			}
			return true
		})
		return stop
	}

	emitBucket := func(bucket posting.List) bool {
		if bucket.IsEmpty() {
			return false
		}
		scanWidth++
		card := bucket.Cardinality()
		if trace != nil {
			trace.addExamined(card)
		}

		if len(active) == 0 {
			if idx, ok := bucket.TrySingle(); ok {
				return emitMatched(idx)
			}
			if cursor.skip >= card {
				cursor.skip -= card
				if trace != nil {
					trace.addMatched(card)
				}
				return false
			}
			stop := false
			bucket.ForEach(func(idx uint64) bool {
				if emitMatched(idx) {
					stop = true
					return false
				}
				return true
			})
			return stop
		}

		if idx, ok := bucket.TrySingle(); ok {
			for _, pi := range active {
				p := preds[pi]
				if !p.hasContains() || !p.matches(idx) {
					return false
				}
			}
			return emitMatched(idx)
		}

		exactApplied := false
		iterSrc := bucket.Iter()
		defer func() {
			if iterSrc != nil {
				iterSrc.Release()
			}
		}()
		if len(exactActive) > 0 {
			allowExact := plannerAllowExactBucketFilter(cursor.skip, cursor.need, card, exactOnly, len(exactActive))
			mode, exactIDs, nextExactWork, _ := plannerFilterPostingByChecks(preds, exactActive, bucket, exactWork, allowExact)
			exactWork = nextExactWork
			switch mode {
			case plannerPredicateBucketEmpty:
				return false
			case plannerPredicateBucketAll:
				if exactOnly {
					return emitMatchedPosting(exactIDs, card)
				}
				iterSrc.Release()
				iterSrc = exactIDs.Iter()
				exactApplied = true
			case plannerPredicateBucketExact:
				if trace != nil {
					trace.addPostingExactFilter(1)
				}
				if exactIDs.IsEmpty() {
					return false
				}
				if exactOnly {
					return emitMatchedPosting(exactIDs, exactIDs.Cardinality())
				}
				iterSrc.Release()
				iterSrc = exactIDs.Iter()
				exactApplied = true
			}
		}

		if !exactApplied && singleActive >= 0 && cursor.skip > 0 {
			cnt, ok := preds[singleActive].countBucket(bucket)
			if ok && cnt <= cursor.skip {
				cursor.skip -= cnt
				if trace != nil {
					trace.addMatched(cnt)
				}
				return false
			}
		}

		if !exactApplied && singleActive >= 0 {
			pred := preds[singleActive]
			stop := false
			bucket.ForEach(func(idx uint64) bool {
				if !pred.hasContains() || !pred.matches(idx) {
					return true
				}
				if emitMatched(idx) {
					stop = true
					return false
				}
				return true
			})
			return stop
		}

		stop := false
		for iterSrc.HasNext() {
			idx := iterSrc.Next()
			checks := active
			if exactApplied {
				checks = residualActive
			}
			for _, pi := range checks {
				p := preds[pi]
				if !p.hasContains() || !p.matches(idx) {
					goto nextCandidate
				}
			}
			if emitMatched(idx) {
				stop = true
				break
			}
		nextCandidate:
		}
		return stop
	}

	if !desc {
		for i := start; i < end; i++ {
			if emitBucket(s[i].IDs) {
				break
			}
		}
	} else {
		for i := end - 1; i >= start; i-- {
			if emitBucket(s[i].IDs) {
				break
			}
			if i == start {
				break
			}
		}
	}

	if cursor.need > 0 && nilTailField != "" {
		ids := qv.nilFieldOverlay(nilTailField).lookupPostingRetained(nilIndexEntryKey)
		emitBucket(ids)
	}

	if trace != nil {
		trace.addOrderScanWidth(scanWidth)
		if cursor.need == 0 {
			trace.setEarlyStopReason("limit_reached")
		} else {
			trace.setEarlyStopReason("order_index_exhausted")
		}
	}
	exactWork.Release()

	return cursor.out
}

func (qv *queryView[K, V]) collectOrderRangeBounds(field string, n int, exprAt func(i int) qx.Expr) (rangeBounds, []bool, bool, bool) {
	var rb rangeBounds
	var covered []bool
	has := false
	markCovered := func(i int) {
		if covered == nil {
			covered = make([]bool, n)
		}
		covered[i] = true
	}

	for i := 0; i < n; i++ {
		e := exprAt(i)
		if e.Not || e.Field != field {
			continue
		}
		if applied, ok := qv.applyScalarExprToRangeBounds(e, &rb); !ok {
			return rangeBounds{}, nil, false, false
		} else if applied {
			markCovered(i)
			has = true
		}
	}

	return rb, covered, has, true
}

func (qv *queryView[K, V]) collectPredicateRangeBounds(field string, preds []predicate) (rangeBounds, []bool, bool, bool) {
	var rb rangeBounds
	var covered []bool
	has := false
	markCovered := func(i int) {
		if covered == nil {
			covered = make([]bool, len(preds))
		}
		covered[i] = true
	}

	for i := range preds {
		p := preds[i]
		if p.expr.Not || p.expr.Field != field {
			continue
		}
		if p.hasEffectiveBounds {
			mergeRangeBounds(&rb, p.effectiveBounds)
			markCovered(i)
			has = true
			continue
		}
		if applied, ok := qv.applyScalarExprToRangeBounds(p.expr, &rb); !ok {
			return rangeBounds{}, nil, false, false
		} else if applied {
			markCovered(i)
			has = true
		}
	}

	return rb, covered, has, true
}
