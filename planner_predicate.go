package rbi

import (
	"math"
	"math/bits"
	"strconv"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/pooled"
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
	ids            posting.List
	rangeMat       bool
	postsBuf       *pooled.SliceBuf[posting.List]
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
const plannerPredicateFastPathMaxLeaves = 8

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

type predicateSet struct {
	owner *pooled.SliceBuf[predicate]
}

type predicateReader interface {
	Len() int
	Get(i int) predicate
	GetPtr(i int) *predicate
}

type predicateSliceView []predicate

func (ps predicateSliceView) Len() int {
	return len(ps)
}

func (ps predicateSliceView) Get(i int) predicate {
	return ps[i]
}

func (ps predicateSliceView) GetPtr(i int) *predicate {
	return &ps[i]
}

func newPredicateSet(capHint int) predicateSet {
	owner := predicateSlicePool.Get()
	if capHint > 0 {
		owner.Grow(capHint)
	}
	return predicateSet{
		owner: owner,
	}
}

func (ps predicateSet) Len() int {
	if ps.owner == nil {
		return 0
	}
	return ps.owner.Len()
}

func (ps predicateSet) Get(i int) predicate {
	return ps.owner.Get(i)
}

func (ps predicateSet) GetPtr(i int) *predicate {
	return ps.owner.GetPtr(i)
}

func (ps *predicateSet) Set(i int, p predicate) {
	ps.owner.Set(i, p)
}

func (ps *predicateSet) Append(p predicate) {
	if ps.owner == nil {
		ps.owner = predicateSlicePool.Get()
	}
	ps.owner.Append(p)
}

func (ps *predicateSet) Release() {
	if ps == nil {
		return
	}
	if ps.owner != nil {
		predicateSlicePool.Put(ps.owner)
		ps.owner = nil
	}
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

func (p predicate) postCount() int {
	return postingBufLen(p.postsBuf)
}

func (p predicate) postAt(i int) posting.List {
	return p.postsBuf.Get(i)
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
		c := predicatePostingFilterApply
		if p.postingFilterCheap {
			c |= predicatePostingFilterExactBucket |
				predicatePostingFilterPreferExact |
				predicatePostingFilterCheap
		}
		return c
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
		if p.postCount() > predicatePostsAnyNotExactPostingMaxTerms {
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
	return p.kind == predicateKindPostsAll && p.postCount() > 1
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
		return newPostingConcatBufIter(p.postsBuf)
	case predicateIterPostsUnion:
		return newPostingUnionBufIter(p.postsBuf)
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
		for i := 0; i < p.postCount(); i++ {
			ids := p.postAt(i)
			if ids.Contains(idx) {
				return true
			}
		}
		return false
	case predicateKindPostsAnyNot:
		for i := 0; i < p.postCount(); i++ {
			ids := p.postAt(i)
			if ids.Contains(idx) {
				return false
			}
		}
		return true
	case predicateKindPostsAll:
		for i := 0; i < p.postCount(); i++ {
			ids := p.postAt(i)
			if !ids.Contains(idx) {
				return false
			}
		}
		return true
	case predicateKindPostsAllNot:
		for i := 0; i < p.postCount(); i++ {
			ids := p.postAt(i)
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
		return countBucketPostsAnyBuf(p.postsBuf, bucket)
	case predicateKindPostsAnyNot:
		return countBucketPostsAnyNotBuf(p.postsBuf, bucket)
	case predicateKindPostsAll:
		return countBucketPostsAllBuf(p.postsBuf, bucket)
	case predicateKindPostsAllNot:
		return countBucketPostsAllNotBuf(p.postsBuf, bucket)
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
		if p.postCount() > predicatePostsAnyNotExactPostingMaxTerms {
			return posting.List{}, false
		}
		for i := 0; i < p.postCount(); i++ {
			ids := p.postAt(i)
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
		if p.postCount() == 0 {
			dst.Release()
			return posting.List{}, true
		}
		for i := 0; i < p.postCount(); i++ {
			ids := p.postAt(i)
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

	if p.rangeMat {
		if p.ids.IsEmpty() {
			if p.expr.Not {
				return dst, true
			}
			dst.Release()
			return posting.List{}, true
		}
		if p.expr.Not {
			return dst.BuildAndNot(p.ids), true
		}
		return dst.BuildAnd(p.ids), true
	}
	if p.lazyMatState != nil {
		ids := p.lazyMatState.materialize()
		if ids.IsEmpty() {
			if p.expr.Not {
				return dst, true
			}
			dst.Release()
			return posting.List{}, true
		}
		if p.expr.Not {
			return dst.BuildAndNot(ids), true
		}
		return dst.BuildAnd(ids), true
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

func countBucketPostsAnyBuf(posts *pooled.SliceBuf[posting.List], bucket posting.List) (uint64, bool) {
	if bucket.IsEmpty() {
		return 0, true
	}
	if posts == nil || posts.Len() == 0 {
		return 0, true
	}
	if posts.Len() == 1 {
		return posts.Get(0).AndCardinality(bucket), true
	}
	for i := 0; i < posts.Len(); i++ {
		if posts.Get(i).Intersects(bucket) {
			return 0, false
		}
	}
	return 0, true
}

func countBucketPostsAnyNotBuf(posts *pooled.SliceBuf[posting.List], bucket posting.List) (uint64, bool) {
	if bucket.IsEmpty() {
		return 0, true
	}
	bc := bucket.Cardinality()
	if posts == nil || posts.Len() == 0 {
		return bc, true
	}
	if posts.Len() == 1 {
		hit := posts.Get(0).AndCardinality(bucket)
		if hit >= bc {
			return 0, true
		}
		return bc - hit, true
	}
	for i := 0; i < posts.Len(); i++ {
		if posts.Get(i).Intersects(bucket) {
			return 0, false
		}
	}
	return bc, true
}

func countBucketPostsAllBuf(posts *pooled.SliceBuf[posting.List], bucket posting.List) (uint64, bool) {
	if bucket.IsEmpty() {
		return 0, true
	}
	if posts == nil || posts.Len() == 0 {
		return 0, true
	}
	if posts.Len() == 1 {
		return posts.Get(0).AndCardinality(bucket), true
	}
	for i := 0; i < posts.Len(); i++ {
		if !posts.Get(i).Intersects(bucket) {
			return 0, true
		}
	}
	return 0, false
}

func countBucketPostsAllNotBuf(posts *pooled.SliceBuf[posting.List], bucket posting.List) (uint64, bool) {
	if bucket.IsEmpty() {
		return 0, true
	}
	bc := bucket.Cardinality()
	if posts == nil || posts.Len() == 0 {
		return bc, true
	}
	if posts.Len() == 1 {
		hit := posts.Get(0).AndCardinality(bucket)
		if hit >= bc {
			return 0, true
		}
		return bc - hit, true
	}
	for i := 0; i < posts.Len(); i++ {
		if !posts.Get(i).Intersects(bucket) {
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

	var leavesBuf [plannerPredicateFastPathMaxLeaves]qx.Expr
	leaves, ok := collectAndLeavesScratch(q.Expr, leavesBuf[:0])
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

		predSet, ok := qv.buildPredicatesCandidate(leaves)
		if !ok {
			return nil, false, nil
		}
		defer predSet.Release()
		for i := 0; i < predSet.Len(); i++ {
			if predSet.Get(i).alwaysFalse {
				return nil, true, nil
			}
		}

		if trace != nil {
			trace.setPlan(PlanCandidateOrder)
		}
		return qv.execPlanCandidateOrderBasic(q, predSet), true, nil
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

	predSet, ok := qv.buildPredicatesCandidate(leaves)
	if !ok {
		return nil, false, nil
	}
	defer predSet.Release()
	for i := 0; i < predSet.Len(); i++ {
		if predSet.Get(i).alwaysFalse {
			return nil, true, nil
		}
	}

	if trace != nil {
		trace.setPlan(PlanCandidateNoOrder)
	}
	return qv.execPlanLeadScanNoOrder(q, predSet, nil), true, nil
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

func (qv *queryView[K, V]) buildPredicatesCandidate(leaves []qx.Expr) (predicateSet, bool) {
	if len(leaves) == 1 && leaves[0].Op == qx.OpNOOP && leaves[0].Not {
		preds := newPredicateSet(1)
		preds.Append(predicate{alwaysFalse: true})
		return preds, true
	}

	preds := newPredicateSet(len(leaves))
	for _, e := range leaves {
		p, ok := qv.buildPredicateCandidate(e)
		if !ok {
			preds.Release()
			return predicateSet{}, false
		}
		preds.Append(p)
	}
	return preds, true
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

	valsBuf, _, _, err := qv.exprValueToDistinctIdxBuf(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil {
		return predicate{}, false
	}
	if valsBuf != nil {
		defer stringSlicePool.Put(valsBuf)
	}

	b, err := qv.evalSliceEQ(e.Field, valsBuf)
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

	valsBuf, isSlice, hasNil, err := qv.exprValueToDistinctIdxBuf(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil {
		return predicate{}, false
	}
	if valsBuf != nil {
		defer stringSlicePool.Put(valsBuf)
	}
	valCount := 0
	if valsBuf != nil {
		valCount = valsBuf.Len()
	}
	if !isSlice || (valCount == 0 && !hasNil) {
		return predicate{}, false
	}
	postsBuf, est := qv.scalarLookupPostings(e.Field, valsBuf, hasNil)

	if e.Not {
		if postsBuf.Len() == 0 {
			postingSlicePool.Put(postsBuf)
			return predicate{expr: e, alwaysTrue: true}, true
		}
		if postsBuf.Len() == 1 {
			return predicate{
				expr:     e,
				kind:     predicateKindPostingNot,
				posting:  postsBuf.Get(0),
				postsBuf: postsBuf,
			}, true
		}
		return predicate{
			expr:     e,
			kind:     predicateKindPostsAnyNot,
			postsBuf: postsBuf,
		}, true
	}

	if postsBuf.Len() == 0 {
		postingSlicePool.Put(postsBuf)
		return predicate{expr: e, alwaysFalse: true}, true
	}
	if postsBuf.Len() == 1 {
		ids := postsBuf.Get(0)
		return predicate{
			expr:     e,
			kind:     predicateKindPosting,
			iterKind: predicateIterPosting,
			posting:  ids,
			estCard:  ids.Cardinality(),
			postsBuf: postsBuf,
		}, true
	}

	postsAnyState := postsAnyFilterStatePool.Get()
	postsAnyState.postsBuf = postsBuf
	postsAnyState.containsMaterializeAt = postsAnyContainsMaterializeAfterBuf(postsBuf)

	return predicate{
		expr:          e,
		kind:          predicateKindPostsAny,
		iterKind:      predicateIterPostsConcat,
		estCard:       est,
		postsBuf:      postsBuf,
		postsAnyState: postsAnyState,
	}, true
}

func (qv *queryView[K, V]) buildPredHasCandidate(e qx.Expr, fm *field, ov fieldOverlay) (predicate, bool) {
	if !fm.Slice {
		return predicate{}, false
	}

	valsBuf, isSlice, _, err := qv.exprValueToDistinctIdxBuf(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil {
		return predicate{}, false
	}
	if valsBuf != nil {
		defer stringSlicePool.Put(valsBuf)
	}
	if !isSlice || valsBuf == nil || valsBuf.Len() == 0 {
		return predicate{}, false
	}

	postsBuf := postingSlicePool.Get()
	postsBuf.Grow(valsBuf.Len())

	var minCard uint64

	for i := 0; i < valsBuf.Len(); i++ {
		ids := ov.lookupPostingRetained(valsBuf.Get(i))
		if ids.IsEmpty() {
			postingSlicePool.Put(postsBuf)
			if e.Not {
				return predicate{expr: e, alwaysTrue: true}, true
			}
			return predicate{expr: e, alwaysFalse: true}, true
		}
		postsBuf.Append(ids)
		c := ids.Cardinality()
		if minCard == 0 || c < minCard {
			minCard = c
		}
	}

	if e.Not {
		if postsBuf.Len() == 1 {
			return predicate{
				expr:     e,
				kind:     predicateKindPostingNot,
				posting:  postsBuf.Get(0),
				postsBuf: postsBuf,
			}, true
		}
		return predicate{
			expr:     e,
			kind:     predicateKindPostsAllNot,
			postsBuf: postsBuf,
		}, true
	}

	lead := minCardPostingBuf(postsBuf)
	return predicate{
		expr:     e,
		kind:     predicateKindPostsAll,
		iterKind: predicateIterPosting,
		posting:  lead,
		estCard:  minCard,
		postsBuf: postsBuf,
	}, true
}

func (qv *queryView[K, V]) buildPredHasAnyCandidate(e qx.Expr, fm *field, ov fieldOverlay) (predicate, bool) {
	if !fm.Slice {
		return predicate{}, false
	}

	valsBuf, isSlice, _, err := qv.exprValueToDistinctIdxBuf(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil {
		return predicate{}, false
	}
	if valsBuf != nil {
		defer stringSlicePool.Put(valsBuf)
	}
	if !isSlice || valsBuf == nil || valsBuf.Len() == 0 {
		return predicate{}, false
	}

	postsBuf := postingSlicePool.Get()
	postsBuf.Grow(valsBuf.Len())

	var est uint64

	for i := 0; i < valsBuf.Len(); i++ {
		ids := ov.lookupPostingRetained(valsBuf.Get(i))
		if ids.IsEmpty() {
			continue
		}
		postsBuf.Append(ids)
		est += ids.Cardinality()
	}
	if e.Not {
		if postsBuf.Len() == 0 {
			postingSlicePool.Put(postsBuf)
			return predicate{expr: e, alwaysTrue: true}, true
		}
		if postsBuf.Len() == 1 {
			return predicate{
				expr:     e,
				kind:     predicateKindPostingNot,
				posting:  postsBuf.Get(0),
				postsBuf: postsBuf,
			}, true
		}
		return predicate{
			expr:     e,
			kind:     predicateKindPostsAnyNot,
			postsBuf: postsBuf,
		}, true
	}

	if postsBuf.Len() == 0 {
		postingSlicePool.Put(postsBuf)
		return predicate{expr: e, alwaysFalse: true}, true
	}

	if postsBuf.Len() == 1 {
		ids := postsBuf.Get(0)
		return predicate{
			expr:     e,
			kind:     predicateKindPosting,
			iterKind: predicateIterPosting,
			posting:  ids,
			estCard:  ids.Cardinality(),
			postsBuf: postsBuf,
		}, true
	}

	postsAnyState := postsAnyFilterStatePool.Get()
	postsAnyState.postsBuf = postsBuf
	postsAnyState.containsMaterializeAt = postsAnyContainsMaterializeAfterBuf(postsBuf)

	return predicate{
		expr:          e,
		kind:          predicateKindPostsAny,
		iterKind:      predicateIterPostsUnion,
		estCard:       est,
		postsBuf:      postsBuf,
		postsAnyState: postsAnyState,
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
	return overlayUnionRanges(ov, br, overlayRange{})
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

func (it *overlayRangeIter) Release() {
	if it != nil {
		overlayRangeIterPool.Put(it)
	}
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
	return qv.buildPredRangeCandidateWithColdMode(e, fm, ov, allowMaterialize, false)
}

func (qv *queryView[K, V]) buildPredRangeCandidateWithColdMode(e qx.Expr, fm *field, ov fieldOverlay, allowMaterialize bool, lazyColdMaterialize bool) (predicate, bool) {
	var core preparedScalarRangePredicate[K, V]
	pred, done, ok := qv.initPreparedScalarRangePredicate(&core, e, fm)
	if !ok {
		return predicate{}, false
	}
	if done {
		return pred, true
	}
	return core.buildFromOverlay(ov, allowMaterialize, lazyColdMaterialize)
}

func (qv *queryView[K, V]) buildPredMaterializedCandidate(e qx.Expr) (predicate, bool) {
	raw := e
	raw.Not = false
	cacheKey := qv.materializedPredKey(raw)
	state := lazyMaterializedPredicateStatePool.Get()
	state.loader = qv
	state.raw = raw
	state.cacheKey = cacheKey

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

func (qv *queryView[K, V]) evalLazyMaterializedPredicateWithKey(raw qx.Expr, cacheKey materializedPredKey) posting.List {
	if !cacheKey.isZero() {
		if cached, ok := qv.snap.loadMaterializedPredKey(cacheKey); ok {
			return cached
		}
	}

	if isScalarRangeOrPrefixOp(raw.Op) {
		fm := qv.fields[raw.Field]
		var core preparedScalarRangePredicate[K, V]
		pred, done, ok := qv.initPreparedScalarRangePredicate(&core, raw, fm)
		if ok {
			if done {
				if pred.alwaysFalse && !cacheKey.isZero() {
					qv.snap.storeMaterializedPredKey(cacheKey, posting.List{})
				}
				return posting.List{}
			}
			ov := qv.fieldOverlay(raw.Field)
			if !ov.hasData() {
				if !cacheKey.isZero() {
					qv.snap.storeMaterializedPredKey(cacheKey, posting.List{})
				}
				return posting.List{}
			}
			return core.evalMaterializedPostingResult(ov).ids
		}
	}

	b, err := qv.evalSimple(raw)
	if err != nil {
		if !cacheKey.isZero() {
			qv.snap.storeMaterializedPredKey(cacheKey, posting.List{})
		}
		return posting.List{}
	}
	if b.ids.IsEmpty() {
		b.release()
		if !cacheKey.isZero() {
			qv.snap.storeMaterializedPredKey(cacheKey, posting.List{})
		}
		return posting.List{}
	}
	return b.ids
}

func (qv *queryView[K, V]) evalLazyMaterializedPredicate(raw qx.Expr, cacheKey string) posting.List {
	parsed, _ := materializedPredKeyFromEncoded(cacheKey)
	return qv.evalLazyMaterializedPredicateWithKey(raw, parsed)
}

func (qv *queryView[K, V]) materializedPredKeyForScalar(field string, op qx.Op, key string) materializedPredKey {
	if qv.snap.matPredCacheMaxEntries <= 0 {
		return materializedPredKey{}
	}
	if fm := qv.fields[field]; fm != nil && !fm.Slice && isNumericScalarKind(fm.Kind) && len(key) == 8 {
		return materializedPredKeyForNumericScalar(field, op, indexKeyFromFixed8String(key))
	}
	return materializedPredKeyForScalar(field, op, key)
}

func (qv *queryView[K, V]) materializedPredCacheKeyForScalar(field string, op qx.Op, key string) string {
	return qv.materializedPredKeyForScalar(field, op, key).String()
}

func (qv *queryView[K, V]) materializedPredComplementKeyForScalar(field string, op qx.Op, key string) materializedPredKey {
	if qv.snap.matPredCacheMaxEntries <= 0 {
		return materializedPredKey{}
	}
	if fm := qv.fields[field]; fm != nil && !fm.Slice && isNumericScalarKind(fm.Kind) && len(key) == 8 {
		return materializedPredComplementKeyForNumericScalar(field, op, indexKeyFromFixed8String(key))
	}
	return materializedPredComplementKeyForScalar(field, op, key)
}

func (qv *queryView[K, V]) materializedPredComplementCacheKeyForScalar(field string, op qx.Op, key string) string {
	return qv.materializedPredComplementKeyForScalar(field, op, key).String()
}

func (qv *queryView[K, V]) materializedPredKeyForNormalizedScalarBound(field string, bound normalizedScalarBound) materializedPredKey {
	if bound.hasIndexKey {
		return materializedPredKeyForNumericScalar(field, bound.op, bound.keyIndex)
	}
	return qv.materializedPredKeyForScalar(field, bound.op, bound.key)
}

func (qv *queryView[K, V]) materializedPredComplementKeyForNormalizedScalarBound(field string, bound normalizedScalarBound) materializedPredKey {
	if bound.hasIndexKey {
		return materializedPredComplementKeyForNumericScalar(field, bound.op, bound.keyIndex)
	}
	return qv.materializedPredComplementKeyForScalar(field, bound.op, bound.key)
}

func (qv *queryView[K, V]) materializedPredKey(e qx.Expr) materializedPredKey {
	if isScalarRangeOrPrefixOp(e.Op) {
		bound, isSlice, err := qv.normalizedScalarBoundForExpr(e)
		if err != nil || isSlice || bound.empty || bound.full {
			return materializedPredKey{}
		}
		return qv.materializedPredKeyForNormalizedScalarBound(e.Field, bound)
	}
	key, isSlice, isNil, err := qv.exprValueToIdxScalar(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil || isSlice || isNil {
		return materializedPredKey{}
	}
	return qv.materializedPredKeyForScalar(e.Field, e.Op, key)
}

func (qv *queryView[K, V]) materializedPredCacheKey(e qx.Expr) string {
	return qv.materializedPredKey(e).String()
}

func materializedPredCacheKeyFromScalar(field string, op qx.Op, key string) string {
	if field == "" || !isMaterializedScalarCacheOp(op) {
		return ""
	}
	return field + "\x1f" + strconv.Itoa(int(op)) + "\x1f" + key
}

// tryShareMaterializedPred caches ids and, on success, rewrites the caller
// handle to a borrowed alias of the cached payload.
func (qv *queryView[K, V]) tryShareMaterializedPred(cacheKey materializedPredKey, ids posting.List) posting.List {
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

var rangeIterPool = pooled.Pointers[rangeIter]{
	Cleanup: func(it *rangeIter) {
		if it.curIt != nil {
			it.curIt.Release()
		}
	},
	Clear: true,
}

func newRangeIter(s []index, start, end int) posting.Iterator {
	it := rangeIterPool.Get()
	it.s = s
	it.i = start
	it.end = end
	return it
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
	rangeIterPool.Put(it)
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

func (qv *queryView[K, V]) execPlanLeadScanNoOrder(q *qx.QX, preds predicateReader, trace *queryTrace) []K {
	var leadIdx = -1
	var best uint64
	for i := 0; i < preds.Len(); i++ {
		p := preds.Get(i)
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
		it = preds.GetPtr(leadIdx).newIter()
		leadNeedsCheck = preds.GetPtr(leadIdx).leadIterNeedsContainsCheck()
	} else {
		it = qv.snapshotUniverseView().Iter()
	}
	if it != nil {
		defer it.Release()
	}
	var activeBuf [plannerPredicateFastPathMaxLeaves]int
	active := activeBuf[:0]
	for i := 0; i < preds.Len(); i++ {
		if i == leadIdx && !leadNeedsCheck {
			continue
		}
		p := preds.Get(i)
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
	sortActivePredicatesReader(active, preds)

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
			p := preds.GetPtr(singleActive)
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
			if !preds.GetPtr(pi).matches(idx) {
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

func (qv *queryView[K, V]) tryExecLeadScanNoOrderBuckets(q *qx.QX, preds predicateReader, leadIdx int, active []int, trace *queryTrace) ([]K, bool) {
	if leadIdx < 0 || leadIdx >= preds.Len() {
		return nil, false
	}
	lead := preds.Get(leadIdx)
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
	out, _ := qv.scanOrderLimitWithPredicatesReader(q, span.ov, span.br, false, preds, "", trace)
	return out, true
}

func (qv *queryView[K, V]) execPlanCandidateOrderBasic(q *qx.QX, preds predicateReader) []K {
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
	var rangeCovered *pooled.SliceBuf[bool]
	if coveredRange, covered, ok := qv.extractOrderRangeCoverageOverlayReader(o.Field, preds, ov); ok {
		br = coveredRange
		rangeCovered = covered
	}
	if overlayRangeEmpty(br) {
		if rangeCovered != nil {
			boolSlicePool.Put(rangeCovered)
		}
		return nil
	}
	for i := 0; i < rangeCovered.Len(); i++ {
		if rangeCovered.Get(i) {
			preds.GetPtr(i).covered = true
		}
	}
	boolSlicePool.Put(rangeCovered)

	var activeBuf [plannerPredicateFastPathMaxLeaves]int
	active := activeBuf[:0]
	for i := 0; i < preds.Len(); i++ {
		p := preds.Get(i)
		if p.covered || p.alwaysTrue {
			continue
		}
		if p.alwaysFalse {
			return nil
		}
		active = append(active, i)
	}
	sortActivePredicatesReader(active, preds)
	if len(active) == 0 {
		out, _ := qv.scanOrderLimitNoPredicates(q, ov, br, o.Desc, "", nil)
		return out
	}
	out, _ := qv.scanOrderLimitWithPredicatesReader(q, ov, br, o.Desc, preds, "", nil)
	return out
}

func (qv *queryView[K, V]) extractOrderRangeCoverageOverlayWithBoundsReader(field string, preds predicateReader, ov fieldOverlay) (rangeBounds, overlayRange, *pooled.SliceBuf[bool], bool) {
	rb, covered, _, ok := qv.collectPredicateRangeBoundsReader(field, preds)
	if !ok {
		return rangeBounds{}, overlayRange{}, nil, false
	}

	return rb, ov.rangeForBounds(rb), covered, true
}

func (qv *queryView[K, V]) extractOrderRangeCoverageOverlayReader(field string, preds predicateReader, ov fieldOverlay) (overlayRange, *pooled.SliceBuf[bool], bool) {
	_, br, covered, ok := qv.extractOrderRangeCoverageOverlayWithBoundsReader(field, preds, ov)
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

	var leavesBuf [plannerPredicateFastPathMaxLeaves]qx.Expr
	leaves, ok := collectAndLeavesScratch(q.Expr, leavesBuf[:0])
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
		predSet, ok := qv.buildPredicatesOrderedWithMode(leaves, o.Field, false, window, q.Offset, true, true)
		if !ok {
			return nil, false, nil
		}
		defer predSet.Release()
		for i := 0; i < predSet.Len(); i++ {
			if predSet.Get(i).alwaysFalse {
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
		out, ok := qv.execPlanOrderedBasicReader(q, predSet, execTrace)
		if !ok {
			return nil, false, nil
		}
		needWindow, _ := orderWindow(q)
		var baseOpsBuf [plannerPredicateFastPathMaxLeaves]qx.Expr
		baseOps := baseOpsBuf[:0]
		for _, op := range leaves {
			if op.Field == o.Field {
				continue
			}
			baseOps = append(baseOps, op)
		}
		qv.promoteOrderBasicLimitMaterializedBaseOps(o.Field, baseOps, execTrace.ev.RowsExamined-observedStart, uint64(needWindow))
		return out, true, nil
	}

	// keep baseline paths for trivial patterns where ordered strategy usually has no advantage
	if len(leaves) == 1 && isSimpleScalarRangeOrPrefixLeaf(leaves[0]) {
		return nil, false, nil
	}
	if allPositiveScalarEqLeaves(leaves) {
		return nil, false, nil
	}

	predSet, ok := qv.buildPredicates(leaves)
	if !ok {
		return nil, false, nil
	}
	defer predSet.Release()
	for i := 0; i < predSet.Len(); i++ {
		if predSet.Get(i).alwaysFalse {
			return nil, true, nil
		}
	}

	if trace != nil {
		trace.setPlan(PlanOrderedNoOrder)
	}
	return qv.execPlanLeadScanNoOrder(q, predSet, trace), true, nil
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
		n := p.postCount()
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
		n := p.postCount()
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

func predicateOrderEstimate(preds predicateReader, pi int) uint64 {
	if pi < 0 || pi >= preds.Len() {
		return math.MaxUint64
	}
	est := preds.Get(pi).estCard
	if est == 0 {
		return math.MaxUint64
	}
	return est
}

func predicateOrderLess(preds predicateReader, a, b int) bool {
	pa := preds.Get(a)
	pb := preds.Get(b)
	ca := pa.checkCost()
	cb := pb.checkCost()
	ea := predicateOrderEstimate(preds, a)
	eb := predicateOrderEstimate(preds, b)
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

func predicateOrderEstimateSet(preds predicateSet, pi int) uint64 {
	if pi < 0 || pi >= preds.Len() {
		return math.MaxUint64
	}
	est := preds.Get(pi).estCard
	if est == 0 {
		return math.MaxUint64
	}
	return est
}

func predicateOrderLessSet(preds predicateSet, a, b int) bool {
	pa := preds.Get(a)
	pb := preds.Get(b)
	ca := pa.checkCost()
	cb := pb.checkCost()
	ea := predicateOrderEstimateSet(preds, a)
	eb := predicateOrderEstimateSet(preds, b)
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

func sortActivePredicatesReader(active []int, preds predicateReader) {
	if len(active) <= 1 {
		return
	}

	// Small in-place insertion sort keeps overhead allocation-free.
	for i := 1; i < len(active); i++ {
		cur := active[i]
		j := i
		for j > 0 && predicateOrderLess(preds, cur, active[j-1]) {
			active[j] = active[j-1]
			j--
		}
		active[j] = cur
	}
}

func sortActivePredicatesSet(active []int, preds predicateSet) {
	if len(active) <= 1 {
		return
	}

	for i := 1; i < len(active); i++ {
		cur := active[i]
		j := i
		for j > 0 && predicateOrderLessSet(preds, cur, active[j-1]) {
			active[j] = active[j-1]
			j--
		}
		active[j] = cur
	}
}

func sortActivePredicates(active []int, preds []predicate) {
	if len(active) <= 1 {
		return
	}

	view := predicateSliceView(preds)
	for i := 1; i < len(active); i++ {
		cur := active[i]
		j := i
		for j > 0 && predicateOrderLess(view, cur, active[j-1]) {
			active[j] = active[j-1]
			j--
		}
		active[j] = cur
	}
}

func sortActivePredicatesBufReader(active *pooled.SliceBuf[int], preds predicateReader) {
	if active.Len() <= 1 {
		return
	}

	for i := 1; i < active.Len(); i++ {
		cur := active.Get(i)
		j := i
		for j > 0 && predicateOrderLess(preds, cur, active.Get(j-1)) {
			active.Set(j, active.Get(j-1))
			j--
		}
		active.Set(j, cur)
	}
}

func orderedBasicAnchoredMatchesActiveReader(preds predicateReader, active []int, leadIdx int, leadNeedsCheck bool, idx uint64) bool {
	for _, pi := range active {
		if pi == leadIdx && !leadNeedsCheck {
			continue
		}
		p := preds.GetPtr(pi)
		if !p.hasContains() || !p.matches(idx) {
			return false
		}
	}
	return true
}

func orderedBasicAnchoredMatchesActiveBufReader(preds predicateReader, active *pooled.SliceBuf[int], leadIdx int, leadNeedsCheck bool, idx uint64) bool {
	for i := 0; i < active.Len(); i++ {
		pi := active.Get(i)
		if pi == leadIdx && !leadNeedsCheck {
			continue
		}
		p := preds.GetPtr(pi)
		if !p.hasContains() || !p.matches(idx) {
			return false
		}
	}
	return true
}

func orderedPredicateChecksMatchReader(preds predicateReader, checks []int, singleCheck int, idx uint64) bool {
	if singleCheck >= 0 {
		p := preds.GetPtr(singleCheck)
		return p.hasContains() && p.matches(idx)
	}
	for _, pi := range checks {
		p := preds.GetPtr(pi)
		if !p.hasContains() || !p.matches(idx) {
			return false
		}
	}
	return true
}

func orderedPredicateChecksMatchBufReader(preds predicateReader, checks *pooled.SliceBuf[int], singleCheck int, idx uint64) bool {
	if singleCheck >= 0 {
		p := preds.GetPtr(singleCheck)
		return p.hasContains() && p.matches(idx)
	}
	for i := 0; i < checks.Len(); i++ {
		pi := checks.Get(i)
		p := preds.GetPtr(pi)
		if !p.hasContains() || !p.matches(idx) {
			return false
		}
	}
	return true
}

func orderedBasicAnchoredFilterPostingReader(preds predicateReader, ids posting.List, checks []int, singleCheck int) posting.List {
	if ids.IsEmpty() {
		return posting.List{}
	}
	builder := newPostingUnionBuilder(postingBatchSinglesEnabled(ids.Cardinality()))
	if idx, ok := ids.TrySingle(); ok {
		if orderedPredicateChecksMatchReader(preds, checks, singleCheck, idx) {
			builder.addSingle(idx)
		}
		return builder.finish(false)
	}
	it := ids.Iter()
	for it.HasNext() {
		idx := it.Next()
		if orderedPredicateChecksMatchReader(preds, checks, singleCheck, idx) {
			builder.addSingle(idx)
		}
	}
	it.Release()
	return builder.finish(false)
}

func orderedBasicAnchoredFilterPostingBufReader(preds predicateReader, ids posting.List, checks *pooled.SliceBuf[int], singleCheck int) posting.List {
	if ids.IsEmpty() {
		return posting.List{}
	}
	builder := newPostingUnionBuilder(postingBatchSinglesEnabled(ids.Cardinality()))
	if idx, ok := ids.TrySingle(); ok {
		if orderedPredicateChecksMatchBufReader(preds, checks, singleCheck, idx) {
			builder.addSingle(idx)
		}
		return builder.finish(false)
	}
	it := ids.Iter()
	for it.HasNext() {
		idx := it.Next()
		if orderedPredicateChecksMatchBufReader(preds, checks, singleCheck, idx) {
			builder.addSingle(idx)
		}
	}
	it.Release()
	return builder.finish(false)
}

func orderedScanEmitMatched[K ~uint64 | ~string, V any](cursor *queryCursor[K, V], trace *queryTrace, idx uint64) bool {
	if trace != nil {
		trace.addMatched(1)
	}
	return cursor.emit(idx)
}

func orderedScanEmitPostingNoPredicates[K ~uint64 | ~string, V any](
	cursor *queryCursor[K, V],
	ids posting.List,
	card uint64,
	trace *queryTrace,
) bool {
	if idx, ok := ids.TrySingle(); ok {
		return orderedScanEmitMatched(cursor, trace, idx)
	}
	if cursor.skip >= card {
		cursor.skip -= card
		if trace != nil {
			trace.addMatched(card)
		}
		return false
	}
	it := ids.Iter()
	for it.HasNext() {
		if orderedScanEmitMatched(cursor, trace, it.Next()) {
			it.Release()
			return true
		}
	}
	it.Release()
	return false
}

func orderedScanEmitPostingMatchedAll[K ~uint64 | ~string, V any](
	cursor *queryCursor[K, V],
	ids posting.List,
	card uint64,
	trace *queryTrace,
) bool {
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
	it := ids.Iter()
	for it.HasNext() {
		if cursor.emit(it.Next()) {
			it.Release()
			return true
		}
	}
	it.Release()
	return false
}

func orderedScanEmitPostingByChecks[K ~uint64 | ~string, V any](
	cursor *queryCursor[K, V],
	ids posting.List,
	preds predicateReader,
	checks []int,
	singleCheck int,
	trace *queryTrace,
) bool {
	if ids.IsEmpty() {
		return false
	}
	if idx, ok := ids.TrySingle(); ok {
		if !orderedPredicateChecksMatchReader(preds, checks, singleCheck, idx) {
			return false
		}
		return orderedScanEmitMatched(cursor, trace, idx)
	}
	it := ids.Iter()
	for it.HasNext() {
		idx := it.Next()
		if !orderedPredicateChecksMatchReader(preds, checks, singleCheck, idx) {
			continue
		}
		if orderedScanEmitMatched(cursor, trace, idx) {
			it.Release()
			return true
		}
	}
	it.Release()
	return false
}

func orderedScanEmitBucketNoPredicates[K ~uint64 | ~string, V any](
	cursor *queryCursor[K, V],
	bucket posting.List,
	trace *queryTrace,
	scanWidth *uint64,
) bool {
	if bucket.IsEmpty() {
		return false
	}
	*scanWidth++
	card := bucket.Cardinality()
	if trace != nil {
		trace.addExamined(card)
	}
	return orderedScanEmitPostingNoPredicates(cursor, bucket, card, trace)
}

func orderedScanEmitBucketWithPredicates[K ~uint64 | ~string, V any](
	cursor *queryCursor[K, V],
	preds predicateReader,
	active []int,
	singleActive int,
	exactActive []int,
	exactOnly bool,
	residualActive []int,
	singleResidual int,
	bucket posting.List,
	exactWork posting.List,
	trace *queryTrace,
	scanWidth *uint64,
) (bool, posting.List) {
	if bucket.IsEmpty() {
		return false, exactWork
	}
	*scanWidth++
	card := bucket.Cardinality()
	if trace != nil {
		trace.addExamined(card)
	}
	if idx, ok := bucket.TrySingle(); ok {
		if !orderedPredicateChecksMatchReader(preds, active, singleActive, idx) {
			return false, exactWork
		}
		return orderedScanEmitMatched(cursor, trace, idx), exactWork
	}

	if len(exactActive) > 0 {
		allowExact := plannerAllowExactBucketFilter(cursor.skip, cursor.need, card, exactOnly, len(exactActive))
		mode, exactIDs, nextExactWork, _ := plannerFilterPostingByPredicateChecks(preds, exactActive, bucket, exactWork, allowExact)
		exactWork = nextExactWork
		switch mode {
		case plannerPredicateBucketEmpty:
			return false, exactWork
		case plannerPredicateBucketAll:
			if exactOnly {
				return orderedScanEmitPostingMatchedAll(cursor, exactIDs, card, trace), exactWork
			}
			return orderedScanEmitPostingByChecks(cursor, exactIDs, preds, residualActive, singleResidual, trace), exactWork
		case plannerPredicateBucketExact:
			if trace != nil {
				trace.addPostingExactFilter(1)
			}
			if exactIDs.IsEmpty() {
				return false, exactWork
			}
			if exactOnly {
				return orderedScanEmitPostingMatchedAll(cursor, exactIDs, exactIDs.Cardinality(), trace), exactWork
			}
			return orderedScanEmitPostingByChecks(cursor, exactIDs, preds, residualActive, singleResidual, trace), exactWork
		}
	}

	if singleActive >= 0 && cursor.skip > 0 {
		cnt, ok := preds.Get(singleActive).countBucket(bucket)
		if ok && cnt <= cursor.skip {
			cursor.skip -= cnt
			if trace != nil {
				trace.addMatched(cnt)
			}
			return false, exactWork
		}
	}

	return orderedScanEmitPostingByChecks(cursor, bucket, preds, active, singleActive, trace), exactWork
}

func orderedBasicAnchoredEmitPosting[K ~uint64 | ~string, V any](
	qv *queryView[K, V],
	cursor *queryCursor[K, V],
	candidateIDs posting.List,
	preds predicateReader,
	deferredChecks []int,
	singleDeferred int,
	ids posting.List,
	trace *queryTrace,
	seenCandidates *uint64,
	scanWidth *uint64,
) bool {
	if ids.IsEmpty() {
		return false
	}
	*scanWidth = *scanWidth + 1
	if trace != nil {
		trace.addExamined(ids.Cardinality())
	}
	if idx, ok := ids.TrySingle(); ok {
		if !candidateIDs.Contains(idx) {
			return false
		}
		*seenCandidates = *seenCandidates + 1
		if !orderedPredicateChecksMatchReader(preds, deferredChecks, singleDeferred, idx) {
			return false
		}
		return cursor.emit(idx)
	}
	it := ids.Iter()
	for it.HasNext() {
		idx := it.Next()
		if !candidateIDs.Contains(idx) {
			continue
		}
		*seenCandidates = *seenCandidates + 1
		if !orderedPredicateChecksMatchReader(preds, deferredChecks, singleDeferred, idx) {
			continue
		}
		if cursor.emit(idx) {
			it.Release()
			return true
		}
	}
	it.Release()
	return false
}

func orderedBasicAnchoredEmitPostingBuf[K ~uint64 | ~string, V any](
	qv *queryView[K, V],
	cursor *queryCursor[K, V],
	candidateIDs posting.List,
	preds predicateReader,
	deferredChecks *pooled.SliceBuf[int],
	singleDeferred int,
	ids posting.List,
	trace *queryTrace,
	seenCandidates *uint64,
	scanWidth *uint64,
) bool {
	if ids.IsEmpty() {
		return false
	}
	*scanWidth = *scanWidth + 1
	if trace != nil {
		trace.addExamined(ids.Cardinality())
	}
	if idx, ok := ids.TrySingle(); ok {
		if !candidateIDs.Contains(idx) {
			return false
		}
		*seenCandidates = *seenCandidates + 1
		if !orderedPredicateChecksMatchBufReader(preds, deferredChecks, singleDeferred, idx) {
			return false
		}
		return cursor.emit(idx)
	}
	it := ids.Iter()
	for it.HasNext() {
		idx := it.Next()
		if !candidateIDs.Contains(idx) {
			continue
		}
		*seenCandidates = *seenCandidates + 1
		if !orderedPredicateChecksMatchBufReader(preds, deferredChecks, singleDeferred, idx) {
			continue
		}
		if cursor.emit(idx) {
			it.Release()
			return true
		}
	}
	it.Release()
	return false
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

func buildExactBucketPostingFilterActiveReader(dst, active []int, preds predicateReader) []int {
	dst = dst[:0]
	for _, pi := range active {
		if preds.Get(pi).supportsExactBucketPostingFilter() {
			dst = append(dst, pi)
		}
	}
	return dst
}

func buildExactBucketPostingFilterActiveBufReader(dst, active *pooled.SliceBuf[int], preds predicateReader) {
	dst.Truncate()
	for i := 0; i < active.Len(); i++ {
		pi := active.Get(i)
		if preds.Get(pi).supportsExactBucketPostingFilter() {
			dst.Append(pi)
		}
	}
}

func releasePredicateRuntimeState(p *predicate) {
	if p == nil {
		return
	}
	if p.baseRangeState != nil {
		baseRangePredicateStatePool.Put(p.baseRangeState)
		p.baseRangeState = nil
	}
	if p.overlayState != nil {
		overlayRangePredicateStatePool.Put(p.overlayState)
		p.overlayState = nil
	}
	if p.postsAnyState != nil {
		postsAnyFilterStatePool.Put(p.postsAnyState)
		p.postsAnyState = nil
	}
	if p.lazyMatState != nil {
		lazyMaterializedPredicateStatePool.Put(p.lazyMatState)
		p.lazyMatState = nil
	}
	if p.postsBuf != nil {
		postingSlicePool.Put(p.postsBuf)
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

func releasePredicates(preds []predicate) {
	for i := range preds {
		releasePredicateOwnedState(&preds[i])
		preds[i] = predicate{}
	}
}

func (qv *queryView[K, V]) buildPredicatesWithMode(leaves []qx.Expr, allowMaterialize bool) (predicateSet, bool) {
	if len(leaves) == 1 && leaves[0].Op == qx.OpNOOP && leaves[0].Not {
		preds := newPredicateSet(1)
		preds.Append(predicate{alwaysFalse: true})
		return preds, true
	}

	preds := newPredicateSet(len(leaves))
	for _, e := range leaves {
		p, ok := qv.buildPredicateWithMode(e, allowMaterialize)
		if !ok {
			preds.Release()
			return predicateSet{}, false
		}
		preds.Append(p)
	}
	return preds, true
}

func (qv *queryView[K, V]) buildPredicates(leaves []qx.Expr) (predicateSet, bool) {
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

func (qv *queryView[K, V]) buildPredicatesOrdered(leaves []qx.Expr, orderField string) (predicateSet, bool) {
	return qv.buildPredicatesOrderedWithMode(leaves, orderField, true, 0, 0, true, true)
}

type orderedScalarRangeRouting struct {
	eagerMaterialize bool
	broadComplement  bool
	cacheKey         materializedPredKey
	requirePromotion bool
}

func (qv *queryView[K, V]) orderedScalarRangeRouting(e qx.Expr, orderedWindow int, orderedOffset uint64, universe uint64) orderedScalarRangeRouting {
	if universe == 0 || e.Not || e.Field == "" || !isNumericRangeOp(e.Op) {
		return orderedScalarRangeRouting{}
	}
	candidate, ok := qv.prepareScalarRangeRoutingCandidate(e)
	if !ok || !candidate.numeric {
		return orderedScalarRangeRouting{}
	}

	route := orderedScalarRangeRouting{
		eagerMaterialize: candidate.plan.orderedEagerMaterializeUseful(orderedWindow, universe),
		cacheKey:         candidate.core.sharedReuse.cacheKey,
	}
	expectedRows := orderedPredicateExpectedRows(orderedWindow, candidate.plan.est, universe)
	if orderedOffset == 0 && expectedRows > 0 && expectedRows < candidate.plan.est {
		route.requirePromotion = satMulUint64(expectedRows, 2) < candidate.plan.est
	}
	route.broadComplement = candidate.broadComplementCardinality(universe)
	return route
}

func (qv *queryView[K, V]) orderedScalarRangeCanEagerMaterialize(route orderedScalarRangeRouting) bool {
	if !route.eagerMaterialize {
		return false
	}
	if !route.requirePromotion || qv.snap == nil || route.cacheKey.isZero() {
		return true
	}
	return qv.snap.shouldPromoteRuntimeMaterializedPredKey(route.cacheKey)
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
	if qv.snap == nil || candidate.core.complementCacheKey.isZero() ||
		!qv.snap.shouldPromoteRuntimeMaterializedPredKey(candidate.core.complementCacheKey) {
		return false
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
	return qv.buildMergedNumericRangePredicateWithColdMode(e, bounds, allowMaterialize, false)
}

func (qv *queryView[K, V]) buildMergedNumericRangePredicateWithColdMode(
	e qx.Expr,
	bounds rangeBounds,
	allowMaterialize bool,
	lazyColdMaterialize bool,
) (predicate, bool) {
	fm := qv.fields[e.Field]
	if fm == nil || fm.Slice {
		return predicate{}, false
	}
	var core preparedScalarRangePredicate[K, V]
	qv.initPreparedExactScalarRangePredicate(&core, e, fm, bounds)
	ov := qv.fieldOverlay(e.Field)
	if !ov.hasData() {
		return predicate{}, false
	}
	if ov.chunked != nil {
		p, ok := core.buildFromOverlay(ov, allowMaterialize, lazyColdMaterialize)
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
	p, ok := core.buildFromSlice(*slice, allowMaterialize, lazyColdMaterialize)
	if ok {
		p.effectiveBounds = bounds
		p.hasEffectiveBounds = true
	}
	return p, ok
}

func (qv *queryView[K, V]) buildPredicatesOrderedWithMode(leaves []qx.Expr, orderField string, allowMaterialize bool, orderedWindow int, orderedOffset uint64, coverOrderRange bool, allowOrderedEagerMaterialize bool) (predicateSet, bool) {
	if len(leaves) == 1 && leaves[0].Op == qx.OpNOOP && leaves[0].Not {
		preds := newPredicateSet(1)
		preds.Append(predicate{alwaysFalse: true})
		return preds, true
	}

	preds := newPredicateSet(len(leaves))
	universe := uint64(0)
	if !allowMaterialize {
		universe = qv.snapshotUniverseCardinality()
	}
	routeUniverse := universe
	if routeUniverse == 0 && orderedWindow > 0 {
		routeUniverse = qv.snapshotUniverseCardinality()
	}
	var mergedRangesBuf *pooled.SliceBuf[orderedMergedScalarRangeField]
	if len(leaves) > 1 {
		mergedRangesBuf = orderedMergedScalarRangeFieldSlicePool.Get()
		mergedRangesBuf.Grow(len(leaves))
		if !qv.collectOrderedMergedScalarRangeFields(orderField, leaves, mergedRangesBuf) {
			preds.Release()
			orderedMergedScalarRangeFieldSlicePool.Put(mergedRangesBuf)
			return predicateSet{}, false
		}
		defer orderedMergedScalarRangeFieldSlicePool.Put(mergedRangesBuf)
	}
	for i, e := range leaves {
		orderRangeDeferred := !coverOrderRange && isOrderRangeCoveredLeaf(orderField, e)
		if coverOrderRange && isOrderRangeCoveredLeaf(orderField, e) {
			// Keep conversion/typing validation, but avoid expensive predicate
			// materialization for leaves that are fully covered by order range.
			rb, ok, err := qv.rangeBoundsForScalarExpr(e)
			if err != nil || !ok || rb.empty {
				preds.Release()
				return predicateSet{}, false
			}
			preds.Append(predicate{
				expr:    e,
				covered: true,
			})
			continue
		}

		if qv.isPositiveOrderedNumericRangeLeaf(e, orderField) {
			idx := findOrderedMergedScalarRangeField(mergedRangesBuf, e.Field)
			if idx >= 0 {
				merged := mergedRangesBuf.Get(idx)
				if merged.count > 1 {
					if merged.first != i {
						continue
					}
					p, ok := qv.buildMergedNumericRangePredicateWithColdMode(merged.expr, merged.bounds, allowMaterialize, true)
					if !ok {
						preds.Release()
						return predicateSet{}, false
					}
					if p.baseRangeState != nil && orderedWindow > 0 && universe > 0 {
						p.setExpectedContainsCalls(
							orderedBaseRangeExpectedContainsCalls(p.baseRangeState, orderedWindow, universe),
						)
					}
					preds.Append(p)
					continue
				}
			}
		}
		predAllowMaterialize := allowMaterialize
		orderedEagerMaterialize := false
		orderedRoute := qv.orderedScalarRangeRouting(e, orderedWindow, orderedOffset, routeUniverse)
		if !predAllowMaterialize {
			orderedRoute = qv.orderedScalarRangeRouting(e, orderedWindow, orderedOffset, universe)
		}
		if !predAllowMaterialize && allowOrderedEagerMaterialize &&
			!orderRangeDeferred &&
			orderedMergedScalarRangeFieldCount(mergedRangesBuf, e.Field) <= 1 &&
			e.Field != orderField &&
			qv.orderedScalarRangeCanEagerMaterialize(orderedRoute) {
			predAllowMaterialize = true
			orderedEagerMaterialize = true
		}
		lazyColdMaterialize := predAllowMaterialize && !orderedRoute.eagerMaterialize
		p, ok := qv.buildPredicateWithColdMode(e, predAllowMaterialize, lazyColdMaterialize)
		if !ok {
			preds.Release()
			return predicateSet{}, false
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
		preds.Append(p)
	}
	return preds, true
}

func (qv *queryView[K, V]) buildPredicatesOrderedWithModeBuf(
	leaves *pooled.SliceBuf[qx.Expr],
	orderField string,
	allowMaterialize bool,
	orderedWindow int,
	orderedOffset uint64,
	coverOrderRange bool,
	allowOrderedEagerMaterialize bool,
) (predicateSet, bool) {
	if leaves.Len() == 1 {
		leaf := leaves.Get(0)
		if leaf.Op == qx.OpNOOP && leaf.Not {
			preds := newPredicateSet(1)
			preds.Append(predicate{alwaysFalse: true})
			return preds, true
		}
	}

	preds := newPredicateSet(leaves.Len())
	universe := uint64(0)
	if !allowMaterialize {
		universe = qv.snapshotUniverseCardinality()
	}
	routeUniverse := universe
	if routeUniverse == 0 && orderedWindow > 0 {
		routeUniverse = qv.snapshotUniverseCardinality()
	}
	var mergedRangesBuf *pooled.SliceBuf[orderedMergedScalarRangeField]
	if leaves.Len() > 1 {
		mergedRangesBuf = orderedMergedScalarRangeFieldSlicePool.Get()
		mergedRangesBuf.Grow(leaves.Len())
		if !qv.collectOrderedMergedScalarRangeFieldsBuf(orderField, leaves, mergedRangesBuf) {
			preds.Release()
			orderedMergedScalarRangeFieldSlicePool.Put(mergedRangesBuf)
			return predicateSet{}, false
		}
		defer orderedMergedScalarRangeFieldSlicePool.Put(mergedRangesBuf)
	}
	for i := 0; i < leaves.Len(); i++ {
		e := leaves.Get(i)
		orderRangeDeferred := !coverOrderRange && isOrderRangeCoveredLeaf(orderField, e)
		if coverOrderRange && isOrderRangeCoveredLeaf(orderField, e) {
			rb, ok, err := qv.rangeBoundsForScalarExpr(e)
			if err != nil || !ok || rb.empty {
				preds.Release()
				return predicateSet{}, false
			}
			preds.Append(predicate{
				expr:    e,
				covered: true,
			})
			continue
		}

		if qv.isPositiveOrderedNumericRangeLeaf(e, orderField) {
			idx := findOrderedMergedScalarRangeField(mergedRangesBuf, e.Field)
			if idx >= 0 {
				merged := mergedRangesBuf.Get(idx)
				if merged.count > 1 {
					if merged.first != i {
						continue
					}
					p, ok := qv.buildMergedNumericRangePredicateWithColdMode(merged.expr, merged.bounds, allowMaterialize, true)
					if !ok {
						preds.Release()
						return predicateSet{}, false
					}
					if p.baseRangeState != nil && orderedWindow > 0 && universe > 0 {
						p.setExpectedContainsCalls(
							orderedBaseRangeExpectedContainsCalls(p.baseRangeState, orderedWindow, universe),
						)
					}
					preds.Append(p)
					continue
				}
			}
		}
		predAllowMaterialize := allowMaterialize
		orderedEagerMaterialize := false
		orderedRoute := qv.orderedScalarRangeRouting(e, orderedWindow, orderedOffset, routeUniverse)
		if !predAllowMaterialize {
			orderedRoute = qv.orderedScalarRangeRouting(e, orderedWindow, orderedOffset, universe)
		}
		if !predAllowMaterialize && allowOrderedEagerMaterialize &&
			!orderRangeDeferred &&
			orderedMergedScalarRangeFieldCount(mergedRangesBuf, e.Field) <= 1 &&
			e.Field != orderField &&
			qv.orderedScalarRangeCanEagerMaterialize(orderedRoute) {
			predAllowMaterialize = true
			orderedEagerMaterialize = true
		}
		lazyColdMaterialize := predAllowMaterialize && !orderedRoute.eagerMaterialize
		p, ok := qv.buildPredicateWithColdMode(e, predAllowMaterialize, lazyColdMaterialize)
		if !ok {
			preds.Release()
			return predicateSet{}, false
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
		preds.Append(p)
	}
	return preds, true
}

func (qv *queryView[K, V]) buildPredicateWithMode(e qx.Expr, allowMaterialize bool) (predicate, bool) {
	return qv.buildPredicateWithColdMode(e, allowMaterialize, false)
}

func (qv *queryView[K, V]) buildPredicateWithColdMode(e qx.Expr, allowMaterialize bool, lazyColdMaterialize bool) (predicate, bool) {
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
			return qv.buildPredRangeCandidateWithColdMode(e, fm, ov, allowMaterialize, lazyColdMaterialize)
		}
		slice := qv.snapshotFieldIndexSlice(e.Field)
		if slice == nil {
			return predicate{}, false
		}
		return qv.buildPredRangeWithColdMode(e, fm, slice, allowMaterialize, lazyColdMaterialize)
	}
	p, ok := qv.buildPredicateCandidate(e)
	if !ok {
		return predicate{}, false
	}
	return p, true
}

func (qv *queryView[K, V]) buildPredRangeWithColdMode(e qx.Expr, fm *field, slice *[]index, allowMaterialize bool, lazyColdMaterialize bool) (predicate, bool) {
	var core preparedScalarRangePredicate[K, V]
	pred, done, ok := qv.initPreparedScalarRangePredicate(&core, e, fm)
	if !ok {
		return predicate{}, false
	}
	if done {
		return pred, true
	}
	return core.buildFromSlice(*slice, allowMaterialize, lazyColdMaterialize)
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
	if p.useComplement {
		for i := 0; i < p.start; i++ {
			if ids := p.s[i].IDs; !ids.IsEmpty() {
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
			}
		}
		for i := p.end; i < len(p.s); i++ {
			if ids := p.s[i].IDs; !ids.IsEmpty() {
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
			}
		}
		return
	}

	for i := p.start; i < p.end; i++ {
		if ids := p.s[i].IDs; !ids.IsEmpty() {
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
		}
	}
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

func postingListAddToU64Set(ids posting.List, set *u64set) {
	if set == nil || ids.IsEmpty() {
		return
	}
	if idx, ok := ids.TrySingle(); ok {
		set.Add(idx)
		return
	}
	it := ids.Iter()
	for it.HasNext() {
		set.Add(it.Next())
	}
	it.Release()
}

func postingListAppendIntersecting(dst, ids posting.List, builder postingUnionBuilder) postingUnionBuilder {
	if dst.IsEmpty() || ids.IsEmpty() {
		return builder
	}
	if idx, ok := ids.TrySingle(); ok {
		if dst.Contains(idx) {
			builder.addSingle(idx)
		}
		return builder
	}
	if idx, ok := dst.TrySingle(); ok {
		if ids.Contains(idx) {
			builder.addSingle(idx)
		}
		return builder
	}
	iterSrc := ids
	other := dst
	if dst.Cardinality() < ids.Cardinality() {
		iterSrc = dst
		other = ids
	}
	it := iterSrc.Iter()
	for it.HasNext() {
		idx := it.Next()
		if other.Contains(idx) {
			builder.addSingle(idx)
		}
	}
	it.Release()
	return builder
}

func (p baseRangeProbe) appendPostings(dst *pooled.SliceBuf[posting.List]) {
	if dst == nil {
		return
	}
	if p.useComplement {
		for i := 0; i < p.start; i++ {
			if ids := p.s[i].IDs; !ids.IsEmpty() {
				dst.Append(ids)
			}
		}
		for i := p.end; i < len(p.s); i++ {
			if ids := p.s[i].IDs; !ids.IsEmpty() {
				dst.Append(ids)
			}
		}
		return
	}
	for i := p.start; i < p.end; i++ {
		if ids := p.s[i].IDs; !ids.IsEmpty() {
			dst.Append(ids)
		}
	}
}

func (p baseRangeProbe) addToSet(set *u64set) {
	if set == nil {
		return
	}
	if p.useComplement {
		for i := 0; i < p.start; i++ {
			postingListAddToU64Set(p.s[i].IDs, set)
		}
		for i := p.end; i < len(p.s); i++ {
			postingListAddToU64Set(p.s[i].IDs, set)
		}
		return
	}
	for i := p.start; i < p.end; i++ {
		postingListAddToU64Set(p.s[i].IDs, set)
	}
}

func (p baseRangeProbe) addToPostingUnion(builder postingUnionBuilder) postingUnionBuilder {
	if p.useComplement {
		for i := 0; i < p.start; i++ {
			builder.addPosting(p.s[i].IDs)
		}
		for i := p.end; i < len(p.s); i++ {
			builder.addPosting(p.s[i].IDs)
		}
		return builder
	}
	for i := p.start; i < p.end; i++ {
		builder.addPosting(p.s[i].IDs)
	}
	return builder
}

func (p baseRangeProbe) buildAndNotPosting(dst posting.List) posting.List {
	if dst.IsEmpty() {
		return dst
	}
	if p.useComplement {
		for i := 0; i < p.start; i++ {
			dst = dst.BuildAndNot(p.s[i].IDs)
			if dst.IsEmpty() {
				return dst
			}
		}
		for i := p.end; i < len(p.s); i++ {
			dst = dst.BuildAndNot(p.s[i].IDs)
			if dst.IsEmpty() {
				return dst
			}
		}
		return dst
	}
	for i := p.start; i < p.end; i++ {
		dst = dst.BuildAndNot(p.s[i].IDs)
		if dst.IsEmpty() {
			return dst
		}
	}
	return dst
}

func (p baseRangeProbe) appendIntersectingPosting(dst posting.List, builder postingUnionBuilder) postingUnionBuilder {
	if dst.IsEmpty() {
		return builder
	}
	if p.useComplement {
		for i := 0; i < p.start; i++ {
			builder = postingListAppendIntersecting(dst, p.s[i].IDs, builder)
		}
		for i := p.end; i < len(p.s); i++ {
			builder = postingListAppendIntersecting(dst, p.s[i].IDs, builder)
		}
		return builder
	}
	for i := p.start; i < p.end; i++ {
		builder = postingListAppendIntersecting(dst, p.s[i].IDs, builder)
	}
	return builder
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

type rangePostingFilterProbe interface {
	linearContains(uint64) bool
	buildAndNotPosting(posting.List) posting.List
	appendIntersectingPosting(posting.List, postingUnionBuilder) postingUnionBuilder
}

func shouldUseNumericRangePostingFilter(e qx.Expr, fm *field) bool {
	if fm == nil || fm.Slice || !isNumericScalarKind(fm.Kind) || !isNumericRangeOp(e.Op) {
		return false
	}
	return true
}

func rangeProbeSupportsCheapPostingFilter(keepProbeHits bool, probeLen int) bool {
	if probeLen <= 0 {
		return false
	}
	if !keepProbeHits {
		return true
	}
	return probeLen <= rangePostingFilterKeepProbeMaxBuckets
}

func applyRangeProbePostingFilter[T rangePostingFilterProbe](
	dst posting.List,
	keepProbeHits bool,
	probeLen int,
	probe T,
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
		dst = probe.buildAndNotPosting(dst)
		return dst, true
	}
	if dst.Cardinality() <= posting.MidCap {
		var matched [posting.MidCap]uint64
		n := 0
		if idx, ok := dst.TrySingle(); ok {
			if probe.linearContains(idx) {
				matched[n] = idx
				n++
			}
		} else {
			it := dst.Iter()
			for it.HasNext() {
				idx := it.Next()
				if probe.linearContains(idx) {
					matched[n] = idx
					n++
				}
			}
			it.Release()
		}
		dst.Release()
		return posting.BuildFromSorted(matched[:n]), true
	}
	if dst.Cardinality() <= uint64(maxRowsForContains) {
		builder := newPostingUnionBuilder(true)
		if idx, ok := dst.TrySingle(); ok {
			if probe.linearContains(idx) {
				builder.addSingle(idx)
			}
		} else {
			it := dst.Iter()
			for it.HasNext() {
				idx := it.Next()
				if probe.linearContains(idx) {
					builder.addSingle(idx)
				}
			}
			it.Release()
		}
		dst.Release()
		out := builder.finish(false)
		builder.release()
		return out, true
	}
	if probeLen <= rangePostingFilterKeepProbeMaxBuckets {
		builder := newPostingUnionBuilder(true)
		builder = probe.appendIntersectingPosting(dst, builder)
		dst.Release()
		out := builder.finish(false)
		builder.release()
		return out, true
	}
	if probeLen > rangePostingFilterKeepProbeMaxBuckets && dst.Cardinality() > rangePostingFilterKeepRowsMaxCard {
		return posting.List{}, false
	}

	builder := newPostingUnionBuilder(true)
	if idx, ok := dst.TrySingle(); ok {
		if probe.linearContains(idx) {
			builder.addSingle(idx)
		}
	} else {
		it := dst.Iter()
		for it.HasNext() {
			idx := it.Next()
			if probe.linearContains(idx) {
				builder.addSingle(idx)
			}
		}
		it.Release()
	}
	dst.Release()
	out := builder.finish(false)
	builder.release()
	return out, true
}

type overlayRangeProbe struct {
	ov            fieldOverlay
	spans         [2]overlayRange
	spanCnt       int
	useComplement bool
	probeLen      int
	probeEst      uint64
}

func newOverlayRangeProbe(
	ov fieldOverlay,
	br overlayRange,
	useComplement bool,
	precomputedProbeLen int,
	precomputedProbeEst uint64,
) overlayRangeProbe {
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
	if precomputedProbeLen >= 0 {
		p.probeLen = precomputedProbeLen
		p.probeEst = precomputedProbeEst
		return p
	}
	p.scanStats()
	return p
}

func (p *overlayRangeProbe) scanStats() {
	if p == nil {
		return
	}
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
			p.probeLen++
			card := ids.Cardinality()
			if ^uint64(0)-p.probeEst < card {
				p.probeEst = ^uint64(0)
			} else {
				p.probeEst += card
			}
		}
	}
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

func (p overlayRangeProbe) buildAndNotPosting(dst posting.List) posting.List {
	if dst.IsEmpty() {
		return dst
	}
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
			dst = dst.BuildAndNot(ids)
			if dst.IsEmpty() {
				return dst
			}
		}
	}
	return dst
}

func (p overlayRangeProbe) appendIntersectingPosting(dst posting.List, builder postingUnionBuilder) postingUnionBuilder {
	if dst.IsEmpty() {
		return builder
	}
	for i := 0; i < p.spanCnt; i++ {
		cur := p.ov.newCursor(p.spans[i], false)
		for {
			_, ids, ok := cur.next()
			if !ok {
				break
			}
			builder = postingListAppendIntersecting(dst, ids, builder)
		}
	}
	return builder
}

func (p overlayRangeProbe) countBucket(bucket posting.List) (uint64, bool) {
	if bucket.IsEmpty() {
		return 0, true
	}
	if !rangeCountBucketUseful(p.probeLen, p.probeEst, bucket.Cardinality()) {
		return 0, false
	}

	var hit uint64
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
			hit += ids.AndCardinality(bucket)
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
	builder = probe.addToPostingUnion(builder)
	out := builder.finish(true)
	builder.release()
	return out
}

func isSingletonHeavyPostingBuf(probe *pooled.SliceBuf[posting.List]) bool {
	probeLen := 0
	singletons := 0
	est := uint64(0)
	if probe == nil {
		return false
	}
	for i := 0; i < probe.Len(); i++ {
		p := probe.Get(i)
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

func materializePostingBufFast(probe *pooled.SliceBuf[posting.List]) posting.List {
	builder := newPostingUnionBuilder(true)
	defer builder.release()

	if probe == nil {
		return posting.List{}
	}
	for i := 0; i < probe.Len(); i++ {
		builder.addPosting(probe.Get(i))
	}
	return builder.finish(true)
}

func materializePostingUnionBufOwned(posts *pooled.SliceBuf[posting.List]) posting.List {
	builder := newPostingUnionBuilder(true)
	defer builder.release()

	if posts == nil {
		return posting.List{}
	}
	for i := 0; i < posts.Len(); i++ {
		builder.addPosting(posts.Get(i))
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

func chooseOrderedAnchorLeadReader(orderField string, preds predicateReader, active []int) (int, bool) {
	lead := -1
	bestCard := uint64(0)
	bestScore := 0.0
	bestWeight := 0.0

	for _, pi := range active {
		p := preds.Get(pi)
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

func chooseOrderedAnchorLeadBufReader(orderField string, preds predicateReader, active *pooled.SliceBuf[int]) (int, bool) {
	lead := -1
	bestCard := uint64(0)
	bestScore := 0.0
	bestWeight := 0.0

	for i := 0; i < active.Len(); i++ {
		pi := active.Get(i)
		p := preds.Get(pi)
		if !p.hasIter() || p.estCard == 0 {
			continue
		}
		weight := orderedAnchorLeadOpWeight(p.expr.Op)
		if p.expr.Field == orderField {
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

func (qv *queryView[K, V]) execPlanOrderedBasicAnchored(q *qx.QX, preds predicateReader, active []int, ov fieldOverlay, br overlayRange, trace *queryTrace) ([]K, bool) {
	if len(active) < plannerOrderedAnchorMinActive {
		return nil, false
	}
	if br.baseEnd-br.baseStart < plannerOrderedAnchorSpanMin {
		return nil, false
	}

	leadIdx, ok := chooseOrderedAnchorLeadReader(q.Order[0].Field, preds, active)
	if !ok {
		return nil, false
	}

	needWindow, ok := orderWindow(q)
	if !ok || needWindow <= 0 {
		return nil, false
	}

	if orderedPredicateIsRangeLike(preds.Get(leadIdx)) {
		rangeLeadMax := uint64(max(needWindow*64, 65_536))
		if preds.Get(leadIdx).estCard > rangeLeadMax {
			return nil, false
		}
	}

	leadBudget, candidateCap := plannerOrderedAnchorBudgets(needWindow, len(active), preds.Get(leadIdx).estCard)
	if br.baseStart == 0 && br.baseEnd == ov.keyCount() {
		wideLeadMax := candidateCap * 8
		if wideLeadMax < candidateCap {
			wideLeadMax = ^uint64(0)
		}
		if preds.Get(leadIdx).estCard > wideLeadMax {
			return nil, false
		}
	}

	leadIt := preds.GetPtr(leadIdx).newIter()
	if leadIt == nil {
		return nil, false
	}
	defer leadIt.Release()
	leadNeedsCheck := preds.GetPtr(leadIdx).leadIterNeedsContainsCheck()

	var exactChecksInline [plannerPredicateFastPathMaxLeaves]int
	var residualChecksInline [plannerPredicateFastPathMaxLeaves]int
	var deferredChecksInline [plannerPredicateFastPathMaxLeaves]int
	exactChecks := exactChecksInline[:0]
	residualChecks := residualChecksInline[:0]
	deferredChecks := deferredChecksInline[:0]

	orderedExactPreferred := false
	for _, pi := range active {
		if pi == leadIdx && !leadNeedsCheck {
			continue
		}
		deferredChecks = append(deferredChecks, pi)
	}
	exactChecks = buildExactBucketPostingFilterActiveReader(exactChecks, deferredChecks, preds)
	for _, pi := range exactChecks {
		if preds.Get(pi).prefersExactBucketPostingFilter() {
			orderedExactPreferred = true
			break
		}
	}
	residualChecks = plannerResidualChecks(residualChecks, deferredChecks, exactChecks)
	useExactPosting := len(exactChecks) > 0 && (len(residualChecks) == 0 || orderedExactPreferred)

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
		if predicateLeadIterMayDuplicate(preds.Get(leadIdx)) {
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
				if !orderedBasicAnchoredMatchesActiveReader(preds, active, leadIdx, leadNeedsCheck, idx) {
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
				if !orderedBasicAnchoredMatchesActiveReader(preds, active, leadIdx, leadNeedsCheck, idx) {
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
	var exactWork posting.List
	if useExactPosting {
		exactApplied := false
		mode, exactIDs, exactWork, _ := plannerFilterPostingByPredicateChecks(preds, exactChecks, candidates, exactWork, true)
		switch mode {
		case plannerPredicateBucketEmpty:
			if trace != nil {
				trace.setEarlyStopReason("no_candidates")
			}
			exactWork.Release()
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
			exactWork.Release()
			return nil, true
		}
		if exactApplied && len(residualChecks) > 0 {
			singleResidual := -1
			if len(residualChecks) == 1 {
				singleResidual = residualChecks[0]
			}
			filtered := orderedBasicAnchoredFilterPostingReader(preds, candidateIDs, residualChecks, singleResidual)
			if filtered.IsEmpty() {
				if trace != nil {
					trace.setEarlyStopReason("no_candidates")
				}
				exactWork.Release()
				return nil, true
			}
			candidateIDs = filtered
		}
		if exactApplied {
			deferredChecks = deferredChecks[:0]
		}
		if candidateIDs.Cardinality() > candidateCap {
			exactWork.Release()
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
	if len(deferredChecks) == 1 {
		singleDeferred = deferredChecks[0]
	}
	cursor := qv.newQueryCursor(out, skip, need, false, 0)

	keyCur := ov.newCursor(br, o.Desc)
	for {
		_, ids, ok := keyCur.next()
		if !ok {
			break
		}
		if orderedBasicAnchoredEmitPosting(
			qv,
			&cursor,
			candidateIDs,
			preds,
			deferredChecks,
			singleDeferred,
			ids,
			trace,
			&seenCandidates,
			&scanWidth,
		) {
			break
		}
		if seenCandidates == totalCandidates {
			break
		}
	}

	if trace != nil {
		trace.addOrderScanWidth(scanWidth)
		if cursor.need == 0 {
			trace.setEarlyStopReason("limit_reached")
		} else if seenCandidates == totalCandidates {
			trace.setEarlyStopReason("candidates_exhausted")
		} else {
			trace.setEarlyStopReason("order_index_exhausted")
		}
	}

	exactWork.Release()
	return cursor.out, true
}

func (qv *queryView[K, V]) execPlanOrderedBasicAnchoredBuf(q *qx.QX, preds predicateReader, active *pooled.SliceBuf[int], ov fieldOverlay, br overlayRange, trace *queryTrace) ([]K, bool) {
	if active.Len() < plannerOrderedAnchorMinActive {
		return nil, false
	}
	if br.baseEnd-br.baseStart < plannerOrderedAnchorSpanMin {
		return nil, false
	}

	leadIdx, ok := chooseOrderedAnchorLeadBufReader(q.Order[0].Field, preds, active)
	if !ok {
		return nil, false
	}

	needWindow, ok := orderWindow(q)
	if !ok || needWindow <= 0 {
		return nil, false
	}

	if orderedPredicateIsRangeLike(preds.Get(leadIdx)) {
		rangeLeadMax := uint64(max(needWindow*64, 65_536))
		if preds.Get(leadIdx).estCard > rangeLeadMax {
			return nil, false
		}
	}

	leadBudget, candidateCap := plannerOrderedAnchorBudgets(needWindow, active.Len(), preds.Get(leadIdx).estCard)
	if br.baseStart == 0 && br.baseEnd == ov.keyCount() {
		wideLeadMax := candidateCap * 8
		if wideLeadMax < candidateCap {
			wideLeadMax = ^uint64(0)
		}
		if preds.Get(leadIdx).estCard > wideLeadMax {
			return nil, false
		}
	}

	leadIt := preds.GetPtr(leadIdx).newIter()
	if leadIt == nil {
		return nil, false
	}
	defer leadIt.Release()
	leadNeedsCheck := preds.GetPtr(leadIdx).leadIterNeedsContainsCheck()

	exactChecksBuf := predicateCheckSlicePool.Get()
	exactChecksBuf.Grow(active.Len())
	defer predicateCheckSlicePool.Put(exactChecksBuf)
	residualChecksBuf := predicateCheckSlicePool.Get()
	residualChecksBuf.Grow(active.Len())
	defer predicateCheckSlicePool.Put(residualChecksBuf)
	deferredChecksBuf := predicateCheckSlicePool.Get()
	deferredChecksBuf.Grow(active.Len())
	defer predicateCheckSlicePool.Put(deferredChecksBuf)

	orderedExactPreferred := false
	for i := 0; i < active.Len(); i++ {
		pi := active.Get(i)
		if pi == leadIdx && !leadNeedsCheck {
			continue
		}
		deferredChecksBuf.Append(pi)
	}
	buildExactBucketPostingFilterActiveBufReader(exactChecksBuf, deferredChecksBuf, preds)
	for i := 0; i < exactChecksBuf.Len(); i++ {
		if preds.Get(exactChecksBuf.Get(i)).prefersExactBucketPostingFilter() {
			orderedExactPreferred = true
			break
		}
	}
	plannerResidualChecksBuf(residualChecksBuf, deferredChecksBuf, exactChecksBuf)
	useExactPosting := exactChecksBuf.Len() > 0 && (residualChecksBuf.Len() == 0 || orderedExactPreferred)

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
		if predicateLeadIterMayDuplicate(preds.Get(leadIdx)) {
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
				if !orderedBasicAnchoredMatchesActiveBufReader(preds, active, leadIdx, leadNeedsCheck, idx) {
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
				if !orderedBasicAnchoredMatchesActiveBufReader(preds, active, leadIdx, leadNeedsCheck, idx) {
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
	var exactWork posting.List
	if useExactPosting {
		exactApplied := false
		mode, exactIDs, exactWork, _ := plannerFilterPostingByPredicateChecksBuf(preds, exactChecksBuf, candidates, exactWork, true)
		switch mode {
		case plannerPredicateBucketEmpty:
			if trace != nil {
				trace.setEarlyStopReason("no_candidates")
			}
			exactWork.Release()
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
			exactWork.Release()
			return nil, true
		}
		if exactApplied && residualChecksBuf.Len() > 0 {
			singleResidual := -1
			if residualChecksBuf.Len() == 1 {
				singleResidual = residualChecksBuf.Get(0)
			}
			filtered := orderedBasicAnchoredFilterPostingBufReader(preds, candidateIDs, residualChecksBuf, singleResidual)
			if filtered.IsEmpty() {
				if trace != nil {
					trace.setEarlyStopReason("no_candidates")
				}
				exactWork.Release()
				return nil, true
			}
			candidateIDs = filtered
		}
		if exactApplied {
			deferredChecksBuf.Truncate()
		}
		if candidateIDs.Cardinality() > candidateCap {
			exactWork.Release()
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
	if deferredChecksBuf.Len() == 1 {
		singleDeferred = deferredChecksBuf.Get(0)
	}
	cursor := qv.newQueryCursor(out, skip, need, false, 0)

	keyCur := ov.newCursor(br, o.Desc)
	for {
		_, ids, ok := keyCur.next()
		if !ok {
			break
		}
		if orderedBasicAnchoredEmitPostingBuf(
			qv,
			&cursor,
			candidateIDs,
			preds,
			deferredChecksBuf,
			singleDeferred,
			ids,
			trace,
			&seenCandidates,
			&scanWidth,
		) {
			break
		}
		if seenCandidates == totalCandidates {
			break
		}
	}

	if trace != nil {
		trace.addOrderScanWidth(scanWidth)
		if cursor.need == 0 {
			trace.setEarlyStopReason("limit_reached")
		} else if seenCandidates == totalCandidates {
			trace.setEarlyStopReason("candidates_exhausted")
		} else {
			trace.setEarlyStopReason("order_index_exhausted")
		}
	}

	exactWork.Release()
	return cursor.out, true
}

func (qv *queryView[K, V]) execPlanOrderedBasicReader(q *qx.QX, preds predicateReader, trace *queryTrace) ([]K, bool) {
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

	rb, br, rangeCovered, ok := qv.extractOrderRangeCoverageOverlayWithBoundsReader(o.Field, preds, ov)
	if !ok {
		return nil, false
	}
	nilTailField := orderNilTailField(fm, o.Field, rb)
	if overlayRangeEmpty(br) && nilTailField == "" {
		boolSlicePool.Put(rangeCovered)
		return nil, true
	}
	for i := 0; i < rangeCovered.Len(); i++ {
		if rangeCovered.Get(i) {
			preds.GetPtr(i).covered = true
		}
	}
	boolSlicePool.Put(rangeCovered)

	if preds.Len() > plannerPredicateFastPathMaxLeaves {
		activeBuf := predicateCheckSlicePool.Get()
		activeBuf.Grow(preds.Len())
		defer predicateCheckSlicePool.Put(activeBuf)
		for i := 0; i < preds.Len(); i++ {
			p := preds.Get(i)
			if p.covered || p.alwaysTrue {
				continue
			}
			if p.alwaysFalse {
				return nil, true
			}
			activeBuf.Append(i)
		}
		if !fm.Ptr {
			if out, ok := qv.execPlanOrderedBasicAnchoredBuf(q, preds, activeBuf, ov, br, trace); ok {
				if trace != nil {
					trace.setPlan(PlanOrderedAnchor)
				}
				return out, true
			}
		}
		if activeBuf.Len() == 0 {
			return qv.scanOrderLimitNoPredicates(q, ov, br, o.Desc, nilTailField, trace)
		}
		return qv.scanOrderLimitWithPredicatesReader(q, ov, br, o.Desc, preds, nilTailField, trace)
	}

	var activeInline [plannerPredicateFastPathMaxLeaves]int
	active := activeInline[:0]
	for i := 0; i < preds.Len(); i++ {
		p := preds.Get(i)
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

	sortActivePredicatesReader(active, preds)
	if len(active) == 0 {
		return qv.scanOrderLimitNoPredicates(q, ov, br, o.Desc, nilTailField, trace)
	}
	return qv.scanOrderLimitWithPredicatesReader(q, ov, br, o.Desc, preds, nilTailField, trace)
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

	if !desc {
		for i := start; i < end; i++ {
			if orderedScanEmitBucketNoPredicates(&cursor, s[i].IDs, trace, &scanWidth) {
				break
			}
		}
	} else {
		for i := end - 1; i >= start; i-- {
			if orderedScanEmitBucketNoPredicates(&cursor, s[i].IDs, trace, &scanWidth) {
				break
			}
			if i == start {
				break
			}
		}
	}

	if cursor.need > 0 && nilTailField != "" {
		ids := qv.nilFieldOverlay(nilTailField).lookupPostingRetained(nilIndexEntryKey)
		orderedScanEmitBucketNoPredicates(&cursor, ids, trace, &scanWidth)
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

	var exactActiveInline [plannerPredicateFastPathMaxLeaves]int
	var residualActiveInline [plannerPredicateFastPathMaxLeaves]int
	exactActive := buildExactBucketPostingFilterActive(exactActiveInline[:0], active, preds)
	residualActive := plannerResidualChecks(residualActiveInline[:0], active, exactActive)
	exactOnly := len(active) == len(exactActive)

	singleActive := -1
	if len(active) == 1 {
		singleActive = active[0]
	}
	singleResidual := -1
	if len(residualActive) == 1 {
		singleResidual = residualActive[0]
	}

	var exactWork posting.List

	var scanWidth uint64

	if !desc {
		for i := start; i < end; i++ {
			stop, nextExactWork := orderedScanEmitBucketWithPredicates(
				&cursor,
				predicateSliceView(preds),
				active,
				singleActive,
				exactActive,
				exactOnly,
				residualActive,
				singleResidual,
				s[i].IDs,
				exactWork,
				trace,
				&scanWidth,
			)
			exactWork = nextExactWork
			if stop {
				break
			}
		}
	} else {
		for i := end - 1; i >= start; i-- {
			stop, nextExactWork := orderedScanEmitBucketWithPredicates(
				&cursor,
				predicateSliceView(preds),
				active,
				singleActive,
				exactActive,
				exactOnly,
				residualActive,
				singleResidual,
				s[i].IDs,
				exactWork,
				trace,
				&scanWidth,
			)
			exactWork = nextExactWork
			if stop {
				break
			}
			if i == start {
				break
			}
		}
	}

	if cursor.need > 0 && nilTailField != "" {
		ids := qv.nilFieldOverlay(nilTailField).lookupPostingRetained(nilIndexEntryKey)
		_, exactWork = orderedScanEmitBucketWithPredicates(
			&cursor,
			predicateSliceView(preds),
			active,
			singleActive,
			exactActive,
			exactOnly,
			residualActive,
			singleResidual,
			ids,
			exactWork,
			trace,
			&scanWidth,
		)
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

func (qv *queryView[K, V]) collectOrderRangeBounds(field string, n int, exprAt func(i int) qx.Expr) (rangeBounds, *pooled.SliceBuf[bool], bool, bool) {
	var rb rangeBounds
	covered := boolSlicePool.Get()
	covered.SetLen(n)
	has := false

	for i := 0; i < n; i++ {
		e := exprAt(i)
		if e.Not || e.Field != field {
			continue
		}
		if applied, ok := qv.applyScalarExprToRangeBounds(e, &rb); !ok {
			boolSlicePool.Put(covered)
			return rangeBounds{}, nil, false, false
		} else if applied {
			covered.Set(i, true)
			has = true
		}
	}
	if !has {
		covered.Truncate()
	}

	return rb, covered, has, true
}

func (qv *queryView[K, V]) collectPredicateRangeBoundsReader(field string, preds predicateReader) (rangeBounds, *pooled.SliceBuf[bool], bool, bool) {
	var rb rangeBounds
	covered := boolSlicePool.Get()
	covered.SetLen(preds.Len())
	has := false

	for i := 0; i < preds.Len(); i++ {
		p := preds.Get(i)
		if p.expr.Not || p.expr.Field != field {
			continue
		}
		if p.hasEffectiveBounds {
			mergeRangeBounds(&rb, p.effectiveBounds)
			covered.Set(i, true)
			has = true
			continue
		}
		if applied, ok := qv.applyScalarExprToRangeBounds(p.expr, &rb); !ok {
			boolSlicePool.Put(covered)
			return rangeBounds{}, nil, false, false
		} else if applied {
			covered.Set(i, true)
			has = true
		}
	}
	if !has {
		covered.Truncate()
	}

	return rb, covered, has, true
}

func (qv *queryView[K, V]) collectPredicateRangeBoundsSet(field string, preds predicateSet) (rangeBounds, *pooled.SliceBuf[bool], bool, bool) {
	var rb rangeBounds
	covered := boolSlicePool.Get()
	covered.SetLen(preds.Len())
	has := false

	for i := 0; i < preds.Len(); i++ {
		p := preds.Get(i)
		if p.expr.Not || p.expr.Field != field {
			continue
		}
		if p.hasEffectiveBounds {
			mergeRangeBounds(&rb, p.effectiveBounds)
			covered.Set(i, true)
			has = true
			continue
		}
		if applied, ok := qv.applyScalarExprToRangeBounds(p.expr, &rb); !ok {
			boolSlicePool.Put(covered)
			return rangeBounds{}, nil, false, false
		} else if applied {
			covered.Set(i, true)
			has = true
		}
	}
	if !has {
		covered.Truncate()
	}

	return rb, covered, has, true
}
