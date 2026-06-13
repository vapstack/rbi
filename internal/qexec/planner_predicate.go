package qexec

import (
	"math"
	"math/bits"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/mathutil"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/rbitrace"
)

type predicate struct {
	expr qir.Expr

	effectiveBounds    indexdata.Bounds
	hasEffectiveBounds bool

	contains func(uint64) bool
	iter     func() posting.Iterator
	estCard  uint64

	kind                 predicateKind
	iterKind             predicateIterKind
	posting              posting.List
	ids                  posting.List
	rangeMat             bool
	postsBuf             []posting.List
	releaseIDs           bool
	postsAnyState        *postsAnyFilterState
	fieldIndexRangeState *fieldIndexRangePredicateState
	lazyMatState         *lazyMaterializedPredicateState

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
	owner []predicate
}

type predicateReader = []predicate

func newPredicateSet(capHint int) predicateSet {
	owner := predicateSlicePool.Get(capHint)
	return predicateSet{
		owner: owner,
	}
}

func (ps predicateSet) Len() int {
	if ps.owner == nil {
		return 0
	}
	return len(ps.owner)
}

func (ps *predicateSet) Append(p predicate) {
	if ps.owner == nil {
		ps.owner = predicateSlicePool.Get(1)
	}
	ps.owner = append(ps.owner, p)
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
	return p.fieldIndexRangeState != nil
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
	if p.postsAnyState != nil {
		p.postsAnyState.setExpectedContainsCalls(expected)
	}
	if p.fieldIndexRangeState != nil {
		p.fieldIndexRangeState.setExpectedContainsCalls(expected)
	}
}

func (p *predicate) runtimeRangeIter() (posting.Iterator, bool) {
	if p.fieldIndexRangeState != nil {
		return p.fieldIndexRangeState.newIter(), true
	}
	return nil, false
}

func (p *predicate) runtimeRangeMatches(idx uint64) (bool, bool) {
	if p.fieldIndexRangeState != nil {
		return p.fieldIndexRangeState.matches(idx), true
	}
	return false, false
}

func (p *predicate) runtimeRangeCountBucket(bucket posting.List) (uint64, bool, bool) {
	if p.fieldIndexRangeState != nil {
		in, ok := p.fieldIndexRangeState.countBucket(bucket)
		return in, ok, true
	}
	return 0, false, false
}

func (p *predicate) runtimeRangeApply(dst posting.List) (posting.List, bool, bool) {
	if p.fieldIndexRangeState != nil {
		next, ok := p.fieldIndexRangeState.applyToPosting(dst)
		return next, ok, true
	}
	return posting.List{}, false, false
}

func (p predicate) postingFilterCapability() predicatePostingFilterCapability {
	if p.alwaysTrue || p.alwaysFalse || p.covered {
		return predicatePostingFilterApply
	}

	if p.rangeMat {
		return predicatePostingFilterApply | predicatePostingFilterExactBucket | predicatePostingFilterCheap
	}

	if p.hasRuntimeRangeState() {
		c := predicatePostingFilterApply
		if p.postingFilterCheap {
			c |= predicatePostingFilterExactBucket | predicatePostingFilterPreferExact | predicatePostingFilterCheap
		}
		return c
	}

	switch p.kind {

	case predicateKindPosting,
		predicateKindPostingNot,
		predicateKindPostsAll,
		predicateKindMaterialized,
		predicateKindMaterializedNot:

		return predicatePostingFilterApply | predicatePostingFilterExactBucket | predicatePostingFilterCheap

	case predicateKindPostsAnyNot:
		if p.postsAnyState != nil {
			return predicatePostingFilterApply | predicatePostingFilterExactBucket | predicatePostingFilterCheap
		}
		if len(p.postsBuf) > predicatePostsAnyNotExactPostingMaxTerms {
			return 0
		}
		return predicatePostingFilterApply | predicatePostingFilterExactBucket | predicatePostingFilterCheap

	case predicateKindPostsAny:
		if p.postsAnyState != nil || p.postingFilter != nil {
			return predicatePostingFilterExactBucket | predicatePostingFilterPreferExact | predicatePostingFilterCheap
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
	return p.kind == predicateKindPostsAll && len(p.postsBuf) > 1
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
		return newPostingUnionIter(p.postsBuf)
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
		for i := 0; i < len(p.postsBuf); i++ {
			ids := p.postsBuf[i]
			if ids.Contains(idx) {
				return true
			}
		}
		return false

	case predicateKindPostsAnyNot:
		if p.postsAnyState != nil {
			return p.postsAnyState.matches(idx)
		}
		for i := 0; i < len(p.postsBuf); i++ {
			ids := p.postsBuf[i]
			if ids.Contains(idx) {
				return false
			}
		}
		return true

	case predicateKindPostsAll:
		for i := 0; i < len(p.postsBuf); i++ {
			ids := p.postsBuf[i]
			if !ids.Contains(idx) {
				return false
			}
		}
		return true

	case predicateKindPostsAllNot:
		for i := 0; i < len(p.postsBuf); i++ {
			ids := p.postsBuf[i]
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
		// Distinct scalar values for one field are mutually exclusive, so IN term postings do not overlap.
		if p.expr.Op == qir.OpIN {
			return countBucketDisjointPostsAnyBuf(p.postsBuf, bucket), true
		}
		if p.postsAnyState != nil {
			return p.postsAnyState.countBucket(bucket)
		}
		return countBucketPostsAnyBuf(p.postsBuf, bucket)

	case predicateKindPostsAnyNot:
		if p.expr.Op == qir.OpIN {
			bc := bucket.Cardinality()
			hit := countBucketDisjointPostsAnyBuf(p.postsBuf, bucket)
			return bc - hit, true
		}
		if p.postsAnyState != nil {
			return p.postsAnyState.countBucket(bucket)
		}
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
		if p.postsAnyState != nil {
			return p.postsAnyState.apply(dst)
		}
		if len(p.postsBuf) > predicatePostsAnyNotExactPostingMaxTerms {
			return posting.List{}, false
		}
		for i := 0; i < len(p.postsBuf); i++ {
			ids := p.postsBuf[i]
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
		if len(p.postsBuf) == 0 {
			dst.Release()
			return posting.List{}, true
		}
		for i := 0; i < len(p.postsBuf); i++ {
			ids := p.postsBuf[i]
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

func countBucketPostsAnyBuf(posts []posting.List, bucket posting.List) (uint64, bool) {
	if bucket.IsEmpty() {
		return 0, true
	}
	if len(posts) == 0 {
		return 0, true
	}
	if len(posts) == 1 {
		return posts[0].AndCardinality(bucket), true
	}
	for i := 0; i < len(posts); i++ {
		if posts[i].Intersects(bucket) {
			return 0, false
		}
	}
	return 0, true
}

func countBucketDisjointPostsAnyBuf(posts []posting.List, bucket posting.List) uint64 {
	var hit uint64
	for i := 0; i < len(posts); i++ {
		hit += posts[i].AndCardinality(bucket)
	}
	return hit
}

func countBucketPostsAnyNotBuf(posts []posting.List, bucket posting.List) (uint64, bool) {
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
	for i := 0; i < len(posts); i++ {
		if posts[i].Intersects(bucket) {
			return 0, false
		}
	}
	return bc, true
}

func countBucketPostsAllBuf(posts []posting.List, bucket posting.List) (uint64, bool) {
	if bucket.IsEmpty() {
		return 0, true
	}
	if len(posts) == 0 {
		return 0, true
	}
	if len(posts) == 1 {
		return posts[0].AndCardinality(bucket), true
	}
	for i := 0; i < len(posts); i++ {
		if !posts[i].Intersects(bucket) {
			return 0, true
		}
	}
	return 0, false
}

func countBucketPostsAllNotBuf(posts []posting.List, bucket posting.List) (uint64, bool) {
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
	for i := 0; i < len(posts); i++ {
		if !posts[i].Intersects(bucket) {
			return bc, true
		}
	}
	return 0, false
}

func (qv *View) shouldUseCandidateOrder(o qir.Order, leaves []qir.Expr) bool {
	fm := qv.fieldMetaByOrdinal(o.FieldOrdinal)
	if fm == nil || fm.Slice || fm.Ptr {
		return false
	}
	if !qv.indexViewByOrdinal(o.FieldOrdinal).HasData() {
		return false
	}
	if len(leaves) < 2 {
		return false
	}

	hasNeg := false
	orderOnly := true

	for _, e := range leaves {
		// keep candidate ORDER conservative: only scalar equality/set predicates
		if !qv.hasFieldOrdinal(e.FieldOrdinal) || len(e.Operands) != 0 || !isScalarEqOrInOp(e.Op) {
			return false
		}
		if ef := qv.fieldMetaByOrdinal(e.FieldOrdinal); ef == nil || ef.Slice {
			return false
		}
		if e.Not {
			hasNeg = true
		}
		if e.FieldOrdinal != o.FieldOrdinal {
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

func (qv *View) buildPredicatesCandidate(leaves []qir.Expr) (predicateSet, bool) {
	if len(leaves) == 1 && leaves[0].Op == qir.OpConst && leaves[0].Not {
		preds := newPredicateSet(1)
		preds.Append(predicate{alwaysFalse: true})
		return preds, true
	}
	if len(leaves) > 2 && qv.hasMergeableNumericRangeLeaves(leaves) {
		return qv.buildPredicatesWithMergedNumericRanges(leaves, true, false)
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

func (qv *View) buildPredicateCandidate(e qir.Expr) (predicate, bool) {
	if e.Op == qir.OpConst {
		if e.Not {
			return predicate{expr: e, alwaysFalse: true}, true
		}
		return predicate{expr: e, alwaysTrue: true}, true
	}
	if !qv.hasFieldOrdinal(e.FieldOrdinal) {
		return predicate{}, false
	}

	fm := qv.fieldMetaByOrdinal(e.FieldOrdinal)
	if fm == nil {
		return predicate{}, false
	}
	ov := qv.indexViewByOrdinal(e.FieldOrdinal)

	switch e.Op {

	case qir.OpEQ:
		if !ov.HasData() {
			return predicate{}, false
		}
		return qv.buildPredEqCandidate(e, fm, ov)

	case qir.OpIN:
		if !ov.HasData() {
			return predicate{}, false
		}
		return qv.buildPredInCandidate(e, fm)

	case qir.OpHASALL:
		if !ov.HasData() {
			return predicate{}, false
		}
		return qv.buildPredHasCandidate(e, fm, ov)

	case qir.OpHASANY:
		if !ov.HasData() {
			return predicate{}, false
		}
		return qv.buildPredHasAnyCandidate(e, fm, ov)

	case qir.OpSUFFIX, qir.OpCONTAINS:
		if qv.isNumericKeyOrdinal(e.FieldOrdinal) {
			return predicate{}, false
		}
		return qv.buildPredMaterializedCandidate(e)

	default:
		if e.Op.IsScalarRangeOrPrefix() {
			if !ov.HasData() {
				return predicate{}, false
			}
			return qv.buildPredRangeCandidateWithMode(e, fm, ov, true)
		}
		return predicate{}, false
	}
}

func (qv *View) buildPredEqCandidate(e qir.Expr, fm *schema.Field, ov indexdata.FieldIndexView) (predicate, bool) {
	key, isSlice, isNil, err := qv.exprValueToLookupKey(qir.Expr{
		Op:           e.Op,
		FieldOrdinal: e.FieldOrdinal,
		Value:        e.Value,
	})
	if err != nil {
		return predicate{}, false
	}

	if !fm.Slice {
		if isSlice {
			return predicate{}, false
		}

		if isNil {
			ids := qv.nilIndexViewByOrdinal(e.FieldOrdinal).LookupPostingRetained(indexdata.NilIndexEntryKey)
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

		ids := lookupScalarPostingRetained(ov, key)
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

	if !isSlice {
		return predicate{}, false
	}

	valsBuf, _, _, err := qv.exprValueToDistinctLookupKeyBuf(qir.Expr{
		Op:           e.Op,
		FieldOrdinal: e.FieldOrdinal,
		Value:        e.Value,
	})
	if err != nil {
		return predicate{}, false
	}
	if valsBuf != nil {
		defer keycodec.ReleaseIndexLookupKeySlice(valsBuf)
	}

	b, err := qv.evalSliceEQ(qv.exec.FieldNameByOrdinal(e.FieldOrdinal), e.FieldOrdinal, valsBuf)
	if err != nil {
		return predicate{}, false
	}

	if e.Not {
		if b.ids.IsEmpty() {
			b.ids.Release()
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
		b.ids.Release()
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

func (qv *View) buildPredEqCandidateWithEst(e qir.Expr, est uint64) (predicate, bool) {
	if e.Op != qir.OpEQ || e.Not || !qv.hasFieldOrdinal(e.FieldOrdinal) || est == 0 {
		return qv.buildPredicateCandidate(e)
	}

	fm := qv.fieldMetaByOrdinal(e.FieldOrdinal)
	if fm == nil || fm.Slice {
		return qv.buildPredicateCandidate(e)
	}

	ov := qv.indexViewByOrdinal(e.FieldOrdinal)
	if !ov.HasData() {
		return predicate{}, false
	}

	key, isSlice, isNil, err := qv.exprValueToLookupKey(qir.Expr{
		Op:           e.Op,
		FieldOrdinal: e.FieldOrdinal,
		Value:        e.Value,
	})
	if err != nil || isSlice {
		return predicate{}, false
	}

	var ids posting.List
	if isNil {
		ids = qv.nilIndexViewByOrdinal(e.FieldOrdinal).LookupPostingRetained(indexdata.NilIndexEntryKey)
	} else {
		ids = lookupScalarPostingRetained(ov, key)
	}
	if ids.IsEmpty() {
		return predicate{expr: e, alwaysFalse: true}, true
	}
	return predicate{
		expr:     e,
		kind:     predicateKindPosting,
		iterKind: predicateIterPosting,
		posting:  ids,
		estCard:  est,
	}, true
}

func (qv *View) buildPredInCandidate(e qir.Expr, fm *schema.Field) (predicate, bool) {
	if fm.Slice {
		return predicate{}, false
	}

	valsBuf, isSlice, hasNil, err := qv.exprValueToDistinctLookupKeyBuf(qir.Expr{
		Op:           e.Op,
		FieldOrdinal: e.FieldOrdinal,
		Value:        e.Value,
	})
	if err != nil {
		return predicate{}, false
	}
	if valsBuf != nil {
		defer keycodec.ReleaseIndexLookupKeySlice(valsBuf)
	}

	valCount := len(valsBuf)
	if !isSlice || (valCount == 0 && !hasNil) {
		return predicate{}, false
	}

	fieldName := qv.exec.FieldNameByOrdinal(e.FieldOrdinal)
	postsBuf, est := qv.scalarLookupPostings(fieldName, e.FieldOrdinal, valsBuf, hasNil)
	cacheKey := qcache.MaterializedPredKey{}

	if qv.snap != nil && qv.snap.MaterializedPredCacheLimit() > 0 {
		cacheKey = qcache.MaterializedPredKeyForDistinctLookupKeys(fieldName, e.Op, valsBuf, hasNil)
	}

	if e.Not {
		if len(postsBuf) == 0 {
			posting.ReleaseSlice(postsBuf)
			return predicate{expr: e, alwaysTrue: true}, true
		}

		if len(postsBuf) == 1 {
			return predicate{
				expr:     e,
				kind:     predicateKindPostingNot,
				posting:  postsBuf[0],
				postsBuf: postsBuf,
			}, true
		}

		postsAnyState := postsAnyFilterStatePool.Get()
		postsAnyState.postsBuf = postsBuf
		postsAnyState.containsMaterializeAt = postsAnyContainsMaterializeAfterBuf(postsBuf)
		postsAnyState.neg = true
		postsAnyState.reuse = newMaterializedPredSecondHitSharedReuse(qv.snap, cacheKey)

		return predicate{
			expr:          e,
			kind:          predicateKindPostsAnyNot,
			postsBuf:      postsBuf,
			postsAnyState: postsAnyState,
		}, true
	}

	if len(postsBuf) == 0 {
		posting.ReleaseSlice(postsBuf)
		return predicate{expr: e, alwaysFalse: true}, true
	}
	if len(postsBuf) == 1 {
		ids := postsBuf[0]
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
	postsAnyState.reuse = newMaterializedPredSecondHitSharedReuse(qv.snap, cacheKey)

	return predicate{
		expr:          e,
		kind:          predicateKindPostsAny,
		iterKind:      predicateIterPostsConcat,
		estCard:       est,
		postsBuf:      postsBuf,
		postsAnyState: postsAnyState,
	}, true
}

func (qv *View) buildPredHasCandidate(e qir.Expr, fm *schema.Field, ov indexdata.FieldIndexView) (predicate, bool) {
	if !fm.Slice {
		return predicate{}, false
	}

	valsBuf, isSlice, _, err := qv.exprValueToDistinctLookupKeyBuf(qir.Expr{
		Op:           e.Op,
		FieldOrdinal: e.FieldOrdinal,
		Value:        e.Value,
	})
	if err != nil {
		return predicate{}, false
	}
	if valsBuf != nil {
		defer keycodec.ReleaseIndexLookupKeySlice(valsBuf)
	}
	if !isSlice || len(valsBuf) == 0 {
		return predicate{}, false
	}

	raw := e
	raw.Not = false
	var cacheKey qcache.MaterializedPredKey
	if qv.snap != nil && qv.snap.MaterializedPredCacheLimit() > 0 {
		cacheKey = qcache.MaterializedPredKeyForDistinctLookupKeys(qv.exec.FieldNameByOrdinal(raw.FieldOrdinal), raw.Op, valsBuf, false)

		if !cacheKey.IsZero() {

			if cached, ok := qv.snap.LoadMaterializedPredKey(cacheKey); ok {
				if e.Not {
					return predicate{
						expr: e,
						kind: predicateKindMaterializedNot,
						ids:  cached,
					}, true
				}
				return predicate{
					expr:     e,
					kind:     predicateKindMaterialized,
					iterKind: predicateIterMaterialized,
					ids:      cached,
					estCard:  cached.Cardinality(),
				}, true
			}

			if qv.snap.ShouldPromoteRuntimeMaterializedPredKey(cacheKey) {
				ids := qv.evalLazyMaterializedPredicateWithKey(raw, cacheKey)
				if ids.IsEmpty() {
					if e.Not {
						return predicate{expr: e, alwaysTrue: true}, true
					}
					return predicate{expr: e, alwaysFalse: true}, true
				}
				releaseIDs := true
				if cached, ok := qv.snap.LoadMaterializedPredKey(cacheKey); ok {
					if !ids.SharesPayload(cached) {
						ids.Release()
					}
					ids = cached
					releaseIDs = false
				}
				if e.Not {
					return predicate{
						expr:       e,
						kind:       predicateKindMaterializedNot,
						ids:        ids,
						releaseIDs: releaseIDs,
					}, true
				}
				return predicate{
					expr:       e,
					kind:       predicateKindMaterialized,
					iterKind:   predicateIterMaterialized,
					ids:        ids,
					estCard:    ids.Cardinality(),
					releaseIDs: releaseIDs,
				}, true
			}
		}
	}

	postsBuf := posting.GetSlice(len(valsBuf))

	var minCard uint64

	for i := 0; i < len(valsBuf); i++ {
		ids := lookupScalarPostingRetained(ov, valsBuf[i])
		if ids.IsEmpty() {
			posting.ReleaseSlice(postsBuf)
			if e.Not {
				return predicate{expr: e, alwaysTrue: true}, true
			}
			return predicate{expr: e, alwaysFalse: true}, true
		}
		postsBuf = append(postsBuf, ids)
		c := ids.Cardinality()
		if minCard == 0 || c < minCard {
			minCard = c
		}
	}

	if e.Not {
		if len(postsBuf) == 1 {
			return predicate{
				expr:     e,
				kind:     predicateKindPostingNot,
				posting:  postsBuf[0],
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

func (qv *View) buildPredHasAnyCandidate(e qir.Expr, fm *schema.Field, ov indexdata.FieldIndexView) (predicate, bool) {
	if !fm.Slice {
		return predicate{}, false
	}

	valsBuf, isSlice, _, err := qv.exprValueToDistinctLookupKeyBuf(qir.Expr{
		Op:           e.Op,
		FieldOrdinal: e.FieldOrdinal,
		Value:        e.Value,
	})
	if err != nil {
		return predicate{}, false
	}
	if valsBuf != nil {
		defer keycodec.ReleaseIndexLookupKeySlice(valsBuf)
	}
	if !isSlice || len(valsBuf) == 0 {
		return predicate{}, false
	}

	postsBuf := posting.GetSlice(len(valsBuf))

	var est uint64

	for i := 0; i < len(valsBuf); i++ {
		ids := lookupScalarPostingRetained(ov, valsBuf[i])
		if ids.IsEmpty() {
			continue
		}
		postsBuf = append(postsBuf, ids)
		est += ids.Cardinality()
	}

	cacheKey := qcache.MaterializedPredKey{}
	if qv.snap != nil && qv.snap.MaterializedPredCacheLimit() > 0 {
		cacheKey = qcache.MaterializedPredKeyForDistinctLookupKeys(qv.exec.FieldNameByOrdinal(e.FieldOrdinal), e.Op, valsBuf, false)
	}

	if e.Not {
		if len(postsBuf) == 0 {
			posting.ReleaseSlice(postsBuf)
			return predicate{expr: e, alwaysTrue: true}, true
		}
		if len(postsBuf) == 1 {
			return predicate{
				expr:     e,
				kind:     predicateKindPostingNot,
				posting:  postsBuf[0],
				postsBuf: postsBuf,
			}, true
		}

		postsAnyState := postsAnyFilterStatePool.Get()
		postsAnyState.postsBuf = postsBuf
		postsAnyState.containsMaterializeAt = postsAnyContainsMaterializeAfterBuf(postsBuf)
		postsAnyState.neg = true
		postsAnyState.reuse = newMaterializedPredSecondHitSharedReuse(qv.snap, cacheKey)

		return predicate{
			expr:          e,
			kind:          predicateKindPostsAnyNot,
			postsBuf:      postsBuf,
			postsAnyState: postsAnyState,
		}, true
	}

	if len(postsBuf) == 0 {
		posting.ReleaseSlice(postsBuf)
		return predicate{expr: e, alwaysFalse: true}, true
	}

	if len(postsBuf) == 1 {
		ids := postsBuf[0]
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
	postsAnyState.reuse = newMaterializedPredSecondHitSharedReuse(qv.snap, cacheKey)

	return predicate{
		expr:          e,
		kind:          predicateKindPostsAny,
		iterKind:      predicateIterPostsUnion,
		estCard:       est,
		postsBuf:      postsBuf,
		postsAnyState: postsAnyState,
	}, true
}

func rangeBoundsForOp(op qir.Op, key string) (indexdata.Bounds, bool) {
	rb := indexdata.Bounds{Has: true}
	if !applyRangeOp(&rb, op, key) {
		return indexdata.Bounds{}, false
	}
	return rb, true
}

func fieldIndexComplementRangeSpans(ov indexdata.FieldIndexView, br indexdata.FieldIndexRange) (before, after indexdata.FieldIndexRange) {
	return ov.RangeByRanks(0, br.BaseStart), ov.RangeByRanks(br.BaseEnd, ov.KeyCount())
}

type fieldIndexRangeIter struct {
	cur    indexdata.FieldIndexCursor
	curIt  posting.Iterator
	single struct {
		set bool
		v   uint64
	}
}

func (it *fieldIndexRangeIter) Release() {
	if it != nil {
		fieldIndexRangeIterPool.Put(it)
	}
}

func (it *fieldIndexRangeIter) HasNext() bool {
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

		_, ids, ok := it.cur.Next()
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

func (it *fieldIndexRangeIter) Next() uint64 {
	if !it.HasNext() {
		return 0
	}
	if it.single.set {
		it.single.set = false
		return it.single.v
	}
	return it.curIt.Next()
}

func (qv *View) buildPredRangeCandidateWithMode(e qir.Expr, fm *schema.Field, ov indexdata.FieldIndexView, allowMaterialize bool) (predicate, bool) {
	return qv.buildPredRangeCandidateWithColdMode(e, fm, ov, allowMaterialize, false)
}

func (qv *View) buildPredRangeCandidateWithColdMode(e qir.Expr, fm *schema.Field, ov indexdata.FieldIndexView, allowMaterialize bool, lazyColdMaterialize bool) (predicate, bool) {
	return qv.buildPredRangeCandidateWithColdModeAndWarmLoad(
		e,
		fm,
		ov,
		allowMaterialize,
		lazyColdMaterialize,
		true,
	)
}

func (qv *View) buildPredRangeCandidateWithColdModeAndWarmLoad(
	e qir.Expr,
	fm *schema.Field,
	ov indexdata.FieldIndexView,
	allowMaterialize, lazyColdMaterialize, allowWarmLoad bool,
) (predicate, bool) {

	var core preparedScalarRangePredicate
	pred, done, ok := qv.initPreparedScalarRangePredicate(&core, e, fm)
	if !ok {
		return predicate{}, false
	}
	if done {
		return pred, true
	}
	return core.buildFromFieldIndexRange(ov, allowMaterialize, lazyColdMaterialize, allowWarmLoad)
}

func (qv *View) buildPredMaterializedCandidate(e qir.Expr) (predicate, bool) {
	raw := e
	raw.Not = false
	key, isSlice, isNil, err := qv.exprValueToLookupKey(raw)
	if err != nil || isSlice {
		return predicate{}, false
	}
	cacheKey := qcache.MaterializedPredKey{}
	if !isNil {
		cacheKey = qcache.MaterializedPredKeyForLookupKey(qv.exec.FieldNameByOrdinal(e.FieldOrdinal), e.Op, key)
	}
	state := lazyMaterializedPredicateStatePool.Get()
	state.loader = qv
	state.raw = raw
	state.cacheKey = cacheKey

	est := qv.snap.Universe.Cardinality()
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

func (qv *View) evalLazyMaterializedPredicateWithKey(raw qir.Expr, cacheKey qcache.MaterializedPredKey) posting.List {
	if !cacheKey.IsZero() {
		if cached, ok := qv.snap.LoadMaterializedPredKey(cacheKey); ok {
			return cached
		}
	}

	if raw.Op.IsScalarRangeOrPrefix() {
		fm := qv.fieldMetaByOrdinal(raw.FieldOrdinal)
		var core preparedScalarRangePredicate
		pred, done, ok := qv.initPreparedScalarRangePredicate(&core, raw, fm)
		if ok {
			if done {
				if pred.alwaysFalse && !cacheKey.IsZero() {
					qv.snap.StoreMaterializedPredKey(cacheKey, posting.List{})
				}
				return posting.List{}
			}
			ov := qv.indexViewByOrdinal(raw.FieldOrdinal)
			if !ov.HasData() {
				if !cacheKey.IsZero() {
					qv.snap.StoreMaterializedPredKey(cacheKey, posting.List{})
				}
				return posting.List{}
			}
			return tryShareMaterializedPredOnSnapshot(qv.snap, cacheKey, core.evalMaterializedPostingResult(ov).ids)
		}
	}

	b, err := qv.evalSimple(raw)
	if err != nil {
		if !cacheKey.IsZero() {
			qv.snap.StoreMaterializedPredKey(cacheKey, posting.List{})
		}
		return posting.List{}
	}

	if b.ids.IsEmpty() {
		b.ids.Release()
		if !cacheKey.IsZero() {
			qv.snap.StoreMaterializedPredKey(cacheKey, posting.List{})
		}
		return posting.List{}
	}

	return tryShareMaterializedPredOnSnapshot(qv.snap, cacheKey, b.ids)
}

func (qv *View) materializedPredKeyForScalar(field string, op qir.Op, key string) qcache.MaterializedPredKey {
	if qv.snap.MaterializedPredCacheLimit() <= 0 {
		return qcache.MaterializedPredKey{}
	}
	return qcache.MaterializedPredKeyForLookupKey(field, op, keycodec.IndexLookupString(key))
}

func (qv *View) materializedPredComplementKeyForScalar(field string, op qir.Op, key string) qcache.MaterializedPredKey {
	if qv.snap.MaterializedPredCacheLimit() <= 0 {
		return qcache.MaterializedPredKey{}
	}
	return qcache.MaterializedPredComplementKeyForLookupKey(field, op, keycodec.IndexLookupString(key))
}

func (qv *View) materializedPredKeyForNormalizedScalarBound(field string, bound normalizedScalarBound) qcache.MaterializedPredKey {
	if bound.hasIndexKey {
		return qcache.MaterializedPredKeyForLookupKey(field, bound.op, keycodec.IndexLookupU64(bound.keyIndex.U64()))
	}
	return qv.materializedPredKeyForScalar(field, bound.op, bound.key)
}

func (qv *View) materializedPredComplementKeyForNormalizedScalarBound(field string, bound normalizedScalarBound) qcache.MaterializedPredKey {
	if bound.hasIndexKey {
		return qcache.MaterializedPredComplementKeyForLookupKey(field, bound.op, keycodec.IndexLookupU64(bound.keyIndex.U64()))
	}
	return qv.materializedPredComplementKeyForScalar(field, bound.op, bound.key)
}

func (qv *View) materializedPredKey(e qir.Expr) qcache.MaterializedPredKey {
	fieldName := qv.exec.FieldNameByOrdinal(e.FieldOrdinal)
	if e.Op.IsScalarRangeOrPrefix() {
		bound, isSlice, err := qv.normalizedScalarBoundForExpr(e)
		if err != nil || isSlice || bound.empty || bound.full {
			return qcache.MaterializedPredKey{}
		}
		return qv.materializedPredKeyForNormalizedScalarBound(fieldName, bound)
	}

	key, isSlice, isNil, err := qv.exprValueToLookupKey(qir.Expr{
		Op:           e.Op,
		FieldOrdinal: e.FieldOrdinal,
		Value:        e.Value,
	})
	if err != nil || isSlice || isNil {
		return qcache.MaterializedPredKey{}
	}

	return qcache.MaterializedPredKeyForLookupKey(fieldName, e.Op, key)
}

// tryShareMaterializedPred caches ids and, on success, rewrites the caller
// handle to a borrowed alias of the cached payload.
func (qv *View) tryShareMaterializedPred(cacheKey qcache.MaterializedPredKey, ids posting.List) posting.List {
	return tryShareMaterializedPredOnSnapshot(qv.snap, cacheKey, ids)
}

func (qv *View) execPlanLeadScanNoOrder(q *qir.Shape, preds predicateReader, leadIdx int, trace *Trace) []uint64 {
	var best uint64
	if leadIdx >= 0 {
		best = preds[leadIdx].estCard
	} else {
		for i := 0; i < len(preds); i++ {
			p := preds[i]
			if p.alwaysTrue || p.alwaysFalse || !p.hasIter() {
				continue
			}
			if leadIdx == -1 || p.estCard < best {
				leadIdx = i
				best = p.estCard
			}
		}
	}

	leadNeedsCheck := false
	if leadIdx >= 0 {
		leadNeedsCheck = (&preds[leadIdx]).leadIterNeedsContainsCheck()
	}

	var activeBuf [plannerPredicateFastPathMaxLeaves]int
	active := activeBuf[:0]

	for i := 0; i < len(preds); i++ {
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

	sortActivePredicatesReader(active, preds)

	if out, ok := qv.execLeadScanNoOrderBuckets(q, preds, leadIdx, trace); ok {
		return out
	}

	capHint := q.Limit
	cardHint := best
	if leadIdx < 0 {
		cardHint = qv.snap.Universe.Cardinality()
	}
	if q.Offset >= cardHint {
		capHint = 0
	} else if remaining := cardHint - q.Offset; q.Limit == 0 || capHint > remaining {
		capHint = remaining
	}
	out := make([]uint64, 0, clampUint64ToInt(capHint))
	cursor := newQueryCursor(out, q.Offset, q.Limit, q.Limit == 0, 0)
	singleActive := -1
	if len(active) == 1 {
		singleActive = active[0]
	}

	var it posting.Iterator
	if leadIdx >= 0 {
		it = (&preds[leadIdx]).newIter()
	} else {
		it = qv.snap.Universe.Borrow().Iter()
	}
	if it != nil {
		defer it.Release()
	}

	if trace != nil {
		if leadIdx >= 0 && best > 0 {
			trace.AddExamined(best)
		} else {
			trace.AddExamined(qv.snap.Universe.Cardinality())
		}
	}

	for it.HasNext() {
		idx := it.Next()

		if singleActive >= 0 {
			p := &preds[singleActive]
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
			if !(&preds[pi]).matches(idx) {
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

func (qv *View) execLeadScanNoOrderBuckets(q *qir.Shape, preds predicateReader, leadIdx int, trace *Trace) ([]uint64, bool) {
	if leadIdx < 0 || leadIdx >= len(preds) {
		return nil, false
	}
	lead := preds[leadIdx]
	var span preparedScalarIndexSpan
	ok := false
	if lead.hasEffectiveBounds {
		if lead.effectiveBounds.Empty {
			return nil, true
		}
		e := lead.expr
		if !qv.isSimpleScalarRangeOrPrefixLeaf(e) {
			return nil, false
		}
		fm := qv.fieldMetaByOrdinal(e.FieldOrdinal)
		if fm == nil || fm.Slice {
			return nil, false
		}
		span.ov = qv.indexViewByOrdinal(e.FieldOrdinal)
		if !span.ov.HasData() {
			return nil, true
		}
		span.hasData = true
		span.br = span.ov.RangeForBounds(lead.effectiveBounds)
		ok = true
	} else {
		var err error
		span, ok, err = qv.prepareScalarIndexSpan(lead.expr)
		if err != nil {
			return nil, false
		}
	}
	if !ok {
		return nil, false
	}
	if !span.hasData {
		return nil, true
	}
	if span.br.Empty() {
		return nil, true
	}
	if span.br.BaseEnd-span.br.BaseStart < 4 {
		return nil, false
	}
	preds[leadIdx].covered = true
	out, _ := qv.scanOrderLimitWithPredicatesReader(q, span.ov, span.br, false, preds, "", trace)
	return out, true
}

func (qv *View) execPlanCandidateOrderBasic(q *qir.Shape, preds predicateReader, trace *Trace) []uint64 {
	o := q.Order

	fm := qv.fieldMetaByOrdinal(o.FieldOrdinal)
	if fm == nil || fm.Slice {
		return nil
	}

	ov := qv.indexViewByOrdinal(o.FieldOrdinal)
	if !ov.HasData() {
		return nil
	}

	br := ov.RangeForBounds(indexdata.Bounds{Has: true})
	var rangeCovered []bool
	orderField := qv.exec.FieldNameByOrdinal(o.FieldOrdinal)
	if coveredRange, covered, ok := qv.extractOrderRangeCoverageIndexViewReader(orderField, preds, ov); ok {
		br = coveredRange
		rangeCovered = covered
	}

	if br.Empty() {
		if rangeCovered != nil {
			pooled.ReleaseBoolSlice(rangeCovered)
		}
		return nil
	}

	for i, is := range rangeCovered {
		if is {
			(&preds[i]).covered = true
		}
	}
	pooled.ReleaseBoolSlice(rangeCovered)

	activeCount := 0
	for i := 0; i < len(preds); i++ {
		p := preds[i]
		if p.covered || p.alwaysTrue {
			continue
		}
		if p.alwaysFalse {
			return nil
		}
		activeCount++
	}

	if activeCount == 0 {
		out, _ := qv.scanOrderLimitNoPredicates(q, ov, br, o.Desc, "", trace)
		return out
	}
	out, _ := qv.scanOrderLimitWithPredicatesReader(q, ov, br, o.Desc, preds, "", trace)
	return out
}

func (qv *View) extractOrderRangeCoverageIndexViewWithBoundsReader(field string, preds predicateReader, ov indexdata.FieldIndexView) (indexdata.Bounds, indexdata.FieldIndexRange, []bool, bool) {
	rb, covered, _, ok := qv.collectPredicateRangeBoundsReader(field, preds)
	if !ok {
		return indexdata.Bounds{}, indexdata.FieldIndexRange{}, nil, false
	}

	return rb, ov.RangeForBounds(rb), covered, true
}

func (qv *View) extractOrderRangeCoverageIndexViewReader(field string, preds predicateReader, ov indexdata.FieldIndexView) (indexdata.FieldIndexRange, []bool, bool) {
	_, br, covered, ok := qv.extractOrderRangeCoverageIndexViewWithBoundsReader(field, preds, ov)
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
	return mathutil.SatAddUint64(buildWork, structuralWork)
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

type rangeContainsMode uint8

const (
	rangeContainsLinear rangeContainsMode = iota
	rangeContainsHashSet
	rangeContainsPosting
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
	return mathutil.SatMulUint64(est, 2)
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
	return mathutil.SatMulUint64(uint64(calls), rangeProbeContainsWork(probeLen, est))
}

func rangeHashSetTotalWork(probeLen int, est uint64, calls int) uint64 {
	if calls <= 0 {
		return 0
	}
	if rangeHashSetAfterForProbe(probeLen, est) == 0 {
		return ^uint64(0)
	}
	return mathutil.SatAddUint64(
		rangeHashSetBuildWork(est),
		mathutil.SatMulUint64(uint64(calls), rangeHashSetLookupWork()),
	)
}

func rangeMaterializedContainsTotalWork(probeLen int, est uint64, calls int) uint64 {
	if calls <= 0 {
		return 0
	}
	return mathutil.SatAddUint64(
		rangeProbeMaterializeWork(probeLen, est),
		mathutil.SatMulUint64(uint64(calls), postingContainsLookupWork(est)),
	)
}

func chooseRangeContainsMode(
	probeLen int,
	est uint64,
	expectedCalls, hashSetAfter, materializeAfter int,
) rangeContainsMode {

	if probeLen <= 0 || est == 0 || expectedCalls <= 0 {
		return rangeContainsLinear
	}

	bestWork := rangeLinearContainsTotalWork(probeLen, est, expectedCalls)
	bestMode := rangeContainsLinear

	if hashSetAfter > 0 {
		hashSetWork := rangeHashSetTotalWork(probeLen, est, expectedCalls)
		if hashSetWork < bestWork {
			bestWork = hashSetWork
			bestMode = rangeContainsHashSet
		}
	}
	if materializeAfter > 0 {
		materializedWork := rangeMaterializedContainsTotalWork(probeLen, est, expectedCalls)
		if materializedWork < bestWork {
			bestMode = rangeContainsPosting
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
	return mathutil.SatMulUint64(uint64(probeLen), avgPerBucket)
}

func rangeCountBucketUseful(probeLen int, est, bucketCard uint64) bool {
	if probeLen <= 0 || bucketCard == 0 {
		return true
	}
	countWork := rangeProbeCountBucketWork(probeLen, est, bucketCard)
	rowWork := mathutil.SatMulUint64(bucketCard, rangeProbeContainsWork(probeLen, est))
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

func orderedFieldIndexRangeExpectedContainsCalls(state *fieldIndexRangePredicateState, orderedWindow int, universe uint64) int {
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
	return mathutil.SatMulUint64(uint64(rows), perRow)
}

func rangeAdaptiveProbeWorkForRows(rows uint64, probeLen int, est uint64) uint64 {
	if rows == 0 {
		return 0
	}
	perCall := rangeProbeContainsWork(probeLen, est)
	materializeAt := rangeAdaptiveProbeMaterializeAt(probeLen, est)
	if materializeAt == 0 || rows < materializeAt {
		return mathutil.SatMulUint64(rows, perCall)
	}
	lookupWork := postingContainsLookupWork(est)
	buildWork := rangeProbeMaterializeWork(probeLen, est)
	linearCalls := materializeAt - 1
	work := mathutil.SatMulUint64(linearCalls, perCall)
	work = mathutil.SatAddUint64(work, buildWork)
	return mathutil.SatAddUint64(
		work,
		mathutil.SatMulUint64(rows-linearCalls, lookupWork),
	)
}

func rangeAdaptiveProbeMaterializeAt(probeLen int, est uint64) uint64 {
	if probeLen <= rangeLinearContainsLimit(probeLen, est) {
		return 0
	}
	materializeAfter := rangeMaterializeAfterForProbe(probeLen, est)
	if materializeAfter <= 0 {
		return 0
	}
	perCall := rangeProbeContainsWork(probeLen, est)
	lookupWork := postingContainsLookupWork(est)
	if perCall <= lookupWork {
		return 0
	}
	buildWork := rangeProbeMaterializeWork(probeLen, est)
	savedPerCall := perCall - lookupWork
	materializeAt := buildWork / savedPerCall
	if materializeAt == ^uint64(0) {
		return 0
	}
	materializeAt++
	if materializeAt < uint64(materializeAfter) {
		materializeAt = uint64(materializeAfter)
	}
	return materializeAt
}

func postingUnionLinearWork(probeLen int, est uint64) uint64 {
	if probeLen <= 0 {
		return 0
	}
	work := rangeProbeMaterializeWork(probeLen, est)
	mergeCalls := uint64(probeLen) * uint64(max(bits.Len64(uint64(probeLen)+1), 1))
	return mathutil.SatAddUint64(work, mergeCalls)
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
	work = mathutil.SatAddUint64(work, singles)
	if multiCount == 0 {
		return work
	}
	multiWork := rangeProbeMaterializeWork(multiCount, multiEst)
	return mathutil.SatAddUint64(work, multiWork)
}

func shouldUseFastSinglesUnion(probeLen, singletonCount int, est uint64) bool {
	if probeLen <= 1 || singletonCount <= 0 {
		return false
	}
	return postingUnionFastSinglesWork(probeLen, singletonCount, est) < postingUnionLinearWork(probeLen, est)
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
		n := len(p.postsBuf)
		if n <= 1 {
			cost = 1
		} else {
			cost = uint64(n)
		}

	case p.fieldIndexRangeState != nil:
		if !p.fieldIndexRangeState.rangeIDs.IsEmpty() {
			cost = 1
		} else if p.fieldIndexRangeState.bucketCount <= 1 {
			cost = 1
		} else if p.fieldIndexRangeState.bucketCount <= 4 {
			cost = uint64(p.fieldIndexRangeState.bucketCount)
		} else {
			cost = 3
		}

	case p.postsAnyState != nil:
		n := len(p.postsBuf)
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
	if pi < 0 || pi >= len(preds) {
		return math.MaxUint64
	}
	est := preds[pi].estCard
	if est == 0 {
		return math.MaxUint64
	}
	return est
}

func predicateOrderLess(preds predicateReader, a, b int) bool {
	pa := preds[a]
	pb := preds[b]
	ca := pa.checkCost()
	cb := pb.checkCost()
	ea := predicateOrderEstimate(preds, a)
	eb := predicateOrderEstimate(preds, b)
	sa := mathutil.SatMulUint64(ca, ea)
	sb := mathutil.SatMulUint64(cb, eb)
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

func orderedBasicAnchoredMatchesActiveReader(preds predicateReader, active []int, leadIdx int, leadNeedsCheck bool, idx uint64) bool {
	for _, pi := range active {
		if pi == leadIdx && !leadNeedsCheck {
			continue
		}
		p := &preds[pi]
		if !p.hasContains() || !p.matches(idx) {
			return false
		}
	}
	return true
}

func orderedPredicateChecksMatchReader(preds predicateReader, checks []int, singleCheck int, idx uint64) bool {
	if singleCheck >= 0 {
		p := &preds[singleCheck]
		return p.hasContains() && p.matches(idx)
	}
	for _, pi := range checks {
		p := &preds[pi]
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

func orderedScanEmitMatched(cursor *queryCursor, trace *Trace, idx uint64) bool {
	if trace != nil {
		trace.AddMatched(1)
	}
	return cursor.emit(idx)
}

func orderedScanEmitPostingNoPredicates(
	cursor *queryCursor,
	ids posting.List,
	card uint64,
	trace *Trace,
) bool {

	if idx, ok := ids.TrySingle(); ok {
		return orderedScanEmitMatched(cursor, trace, idx)
	}

	if cursor.skip >= card {
		cursor.skip -= card
		if trace != nil {
			trace.AddMatched(card)
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

func orderedScanEmitPostingMatchedAll(
	cursor *queryCursor,
	ids posting.List,
	card uint64,
	trace *Trace,
) bool {

	if trace != nil {
		trace.AddMatched(card)
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

func orderedSkipSingleCheckBucket(
	cursor *queryCursor,
	preds predicateReader,
	singleCheck int,
	ids posting.List,
	trace *Trace,
) bool {

	if singleCheck < 0 || cursor.skip == 0 || ids.IsEmpty() {
		return false
	}

	cnt, ok := preds[singleCheck].countBucket(ids)
	if !ok || cnt > cursor.skip {
		return false
	}

	cursor.skip -= cnt
	if trace != nil {
		trace.AddMatched(cnt)
	}
	return true
}

func orderedScanEmitPostingByChecks(
	cursor *queryCursor,
	ids posting.List,
	preds predicateReader,
	checks []int,
	singleCheck int,
	trace *Trace,
) bool {

	if ids.IsEmpty() {
		return false
	}
	if orderedSkipSingleCheckBucket(cursor, preds, singleCheck, ids, trace) {
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

func orderedScanEmitBucketNoPredicates(
	cursor *queryCursor,
	bucket posting.List,
	trace *Trace,
	scanWidth *uint64,
) bool {
	if bucket.IsEmpty() {
		return false
	}
	*scanWidth++
	card := bucket.Cardinality()
	if trace != nil {
		trace.AddExamined(card)
	}
	return orderedScanEmitPostingNoPredicates(cursor, bucket, card, trace)
}

func orderedScanEmitBucketWithPredicates(
	cursor *queryCursor,
	preds predicateReader,
	active []int,
	singleActive int,
	exactActive []int,
	exactOnly bool,
	residualActive []int,
	singleResidual int,
	bucket, exactWork posting.List,
	trace *Trace,
	scanWidth *uint64,
) (bool, posting.List) {

	if bucket.IsEmpty() {
		return false, exactWork
	}
	*scanWidth++
	card := bucket.Cardinality()
	if trace != nil {
		trace.AddExamined(card)
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
				trace.AddPostingExactFilter(1)
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

	return orderedScanEmitPostingByChecks(cursor, bucket, preds, active, singleActive, trace), exactWork
}

func orderedBasicAnchoredEmitPosting(
	cursor *queryCursor,
	candidateIDs posting.List,
	candidateHash *u64set,
	preds predicateReader,
	deferredChecks []int,
	singleDeferred int,
	ids posting.List,
	trace *Trace,
	seenCandidates, scanWidth *uint64,
) bool {
	if ids.IsEmpty() {
		return false
	}
	*scanWidth = *scanWidth + 1

	if trace != nil {
		trace.AddExamined(ids.Cardinality())
	}

	if idx, ok := ids.TrySingle(); ok {
		if candidateHash != nil {
			if !candidateHash.Has(idx) {
				return false
			}
		} else if !candidateIDs.Contains(idx) {
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
		if candidateHash != nil {
			if !candidateHash.Has(idx) {
				continue
			}
		} else if !candidateIDs.Contains(idx) {
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

func buildExactBucketPostingFilterActiveReader(dst, active []int, preds predicateReader) []int {
	dst = dst[:0]
	for _, pi := range active {
		if preds[pi].postingFilterCapability().supportsExactBucket() {
			dst = append(dst, pi)
		}
	}
	return dst
}

func releasePredicateRuntimeState(p *predicate) {
	if p == nil {
		return
	}
	if p.fieldIndexRangeState != nil {
		fieldIndexRangePredicateStatePool.Put(p.fieldIndexRangeState)
		p.fieldIndexRangeState = nil
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
		posting.ReleaseSlice(p.postsBuf)
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
	estCard := p.estCard
	releasePredicateOwnedState(p)
	p.kind = predicateKindMaterializedNot
	p.iterKind = predicateIterNone
	p.ids = ids
	p.releaseIDs = true
	p.estCard = estCard
	p.alwaysTrue = false
	p.alwaysFalse = false
}

func releasePredicates(preds []predicate) {
	for i := range preds {
		releasePredicateOwnedState(&preds[i])
		preds[i] = predicate{}
	}
}

func (qv *View) buildPredicatesWithColdMode(leaves []qir.Expr, allowMaterialize bool, lazyColdMaterialize bool) (predicateSet, bool) {
	if len(leaves) == 1 && leaves[0].Op == qir.OpConst && leaves[0].Not {
		preds := newPredicateSet(1)
		preds.Append(predicate{alwaysFalse: true})
		return preds, true
	}

	if len(leaves) > 2 && qv.hasMergeableNumericRangeLeaves(leaves) {
		return qv.buildPredicatesWithMergedNumericRanges(leaves, allowMaterialize, lazyColdMaterialize)
	}

	preds := newPredicateSet(len(leaves))
	for _, e := range leaves {
		p, ok := qv.buildPredicateWithColdMode(e, allowMaterialize, lazyColdMaterialize)
		if !ok {
			preds.Release()
			return predicateSet{}, false
		}
		preds.Append(p)
	}
	return preds, true
}

func (qv *View) hasMergeableNumericRangeLeaves(leaves []qir.Expr) bool {
	for i := 0; i < len(leaves)-1; i++ {
		e := leaves[i]
		if !qv.isPositiveMergedNumericRangeLeaf(e) {
			continue
		}
		fieldName := qv.exec.FieldNameByOrdinal(e.FieldOrdinal)
		count := 1
		for j := i + 1; j < len(leaves); j++ {
			next := leaves[j]
			if qv.isPositiveMergedNumericRangeLeaf(next) &&
				qv.exec.FieldNameByOrdinal(next.FieldOrdinal) == fieldName {
				count++
			}
		}
		if count > 1 && len(leaves) > count {
			return true
		}
	}
	return false
}

func (qv *View) buildPredicatesWithMergedNumericRanges(leaves []qir.Expr, allowMaterialize bool, lazyColdMaterialize bool) (predicateSet, bool) {
	preds := newPredicateSet(len(leaves))
	mergedRangesBuf := orderedMergedScalarRangeFieldSlicePool.Get(len(leaves))
	var ok bool
	mergedRangesBuf, ok = qv.collectMergedNumericRangeFields(leaves, mergedRangesBuf)
	if !ok {
		preds.Release()
		orderedMergedScalarRangeFieldSlicePool.Put(mergedRangesBuf)
		return predicateSet{}, false
	}
	defer orderedMergedScalarRangeFieldSlicePool.Put(mergedRangesBuf)

	for i, e := range leaves {
		if qv.isPositiveMergedNumericRangeLeaf(e) {
			idx := findOrderedMergedScalarRangeField(mergedRangesBuf, qv.exec.FieldNameByOrdinal(e.FieldOrdinal))
			if idx >= 0 {
				merged := mergedRangesBuf[idx]
				if merged.count > 1 && len(leaves) > merged.count {
					if merged.first != i {
						continue
					}
					p, ok := qv.buildMergedScalarRangePredicateWithColdMode(merged.expr, merged.bounds, allowMaterialize, lazyColdMaterialize)
					if !ok {
						preds.Release()
						return predicateSet{}, false
					}
					preds.Append(p)
					continue
				}
			}
		}

		p, ok := qv.buildPredicateWithColdMode(e, allowMaterialize, lazyColdMaterialize)
		if !ok {
			preds.Release()
			return predicateSet{}, false
		}
		preds.Append(p)
	}
	return preds, true
}

func (qv *View) buildPredicatesWithMode(leaves []qir.Expr, allowMaterialize bool) (predicateSet, bool) {
	return qv.buildPredicatesWithColdMode(leaves, allowMaterialize, allowMaterialize)
}

func (qv *View) isOrderRangeCoveredLeaf(orderField string, e qir.Expr) bool {
	switch qv.classifyOrderFieldScalarLeaf(orderField, e) {
	case orderFieldScalarLeafRange, orderFieldScalarLeafPrefix:
		return true
	default:
		return false
	}
}

type orderedScalarRangeRouting struct {
	eagerMaterialize   bool
	broadComplement    bool
	cacheKey           qcache.MaterializedPredKey
	complementCacheKey qcache.MaterializedPredKey
	requirePromotion   bool
	forceComplement    bool
	useComplement      bool
}

func (qv *View) orderedScalarRangeRouting(e qir.Expr, orderedWindow int, orderedOffset uint64, universe uint64) orderedScalarRangeRouting {
	return qv.orderedPredicateScalarRangeRouting(predicate{expr: e}, orderedWindow, orderedOffset, universe)
}

func (qv *View) orderedPredicateScalarRangeRouting(
	p predicate,
	orderedWindow int,
	orderedOffset uint64,
	universe uint64,
) orderedScalarRangeRouting {

	if universe == 0 || p.expr.Not || !qv.hasFieldOrdinal(p.expr.FieldOrdinal) || !p.expr.Op.IsNumericRange() {
		return orderedScalarRangeRouting{}
	}

	candidate, ok := qv.preparePredicateScalarRangeRoutingCandidate(p)
	if !ok || !candidate.numeric {
		return orderedScalarRangeRouting{}
	}

	route := orderedScalarRangeRouting{
		eagerMaterialize:   candidate.plan.orderedEagerMaterializeUseful(orderedWindow, universe),
		cacheKey:           candidate.core.sharedReuse.cacheKey,
		complementCacheKey: candidate.core.complementCacheKey,
		useComplement:      candidate.plan.useComplement,
	}

	expectedRows := orderedPredicateExpectedRows(orderedWindow, candidate.plan.est, universe)
	if orderedOffset == 0 && expectedRows > 0 && expectedRows < candidate.plan.est {
		route.requirePromotion = mathutil.SatMulUint64(expectedRows, 2) < candidate.plan.est
	}

	route.broadComplement = candidate.broadComplementCardinality(universe)
	route.forceComplement = candidate.plan.useComplement &&
		candidate.core.qv.nilIndexViewByOrdinal(candidate.core.expr.FieldOrdinal).LookupCardinality(indexdata.NilIndexEntryKey) > 0

	return route
}

func (qv *View) orderedScalarRangeRoutingForBuild(
	p predicate,
	predAllowMaterialize bool,
	orderedWindow int,
	orderedOffset, universe, routeUniverse uint64,
) orderedScalarRangeRouting {

	route := qv.orderedPredicateScalarRangeRouting(p, orderedWindow, orderedOffset, routeUniverse)
	if !predAllowMaterialize {
		route = qv.orderedPredicateScalarRangeRouting(p, orderedWindow, orderedOffset, universe)
	}
	return route
}

func (qv *View) orderedScalarRangeCanEagerMaterialize(route orderedScalarRangeRouting) bool {
	if !route.eagerMaterialize {
		return false
	}
	promotionKey := route.cacheKey
	if route.useComplement && !route.complementCacheKey.IsZero() {
		promotionKey = route.complementCacheKey
	}
	if !route.requirePromotion || qv.snap == nil || promotionKey.IsZero() {
		return true
	}
	return qv.snap.ShouldPromoteRuntimeMaterializedPredKey(promotionKey)
}

func (qv *View) orderedScalarRangeHasWarmComplement(route orderedScalarRangeRouting) bool {
	if !route.useComplement || route.complementCacheKey.IsZero() {
		return false
	}
	cached, ok := qv.snap.LoadMaterializedPredKey(route.complementCacheKey)
	if ok {
		cached.Release()
	}
	return ok
}

func (qv *View) tryMaterializeBroadRangeComplementPredicateForOrdered(
	p *predicate,
	broadComplement bool,
	universe uint64,
	orderedWindow int,
	force bool,
) bool {
	if p == nil || (!broadComplement && !force) {
		return false
	}

	fm := qv.fieldMetaByOrdinal(p.expr.FieldOrdinal)
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
	if force && !cacheHit && !empty && candidate.shouldPreferPositiveMaterializationForNullableComplement(plan) {
		return qv.materializePositiveScalarRangePredicateWithCandidate(p, candidate)
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
	if force {
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
	if qv.snap == nil || candidate.core.complementCacheKey.IsZero() {
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
	materializedWork := mathutil.SatAddUint64(buildWork, mathutil.SatMulUint64(expectedRows, postingContainsLookupWork(plan.est)))

	probeWork := rangeProbeTotalWorkForRows(
		int(expectedRows),
		candidate.plan.runtimeProbeBuckets,
		candidate.plan.runtimeProbeEst,
	)

	if materializedWork >= probeWork {
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

func (qv *View) loadWarmComplementPredicateForOrdered(p *predicate, useComplement bool) bool {
	if p == nil || !useComplement {
		return false
	}
	candidate, ok := qv.preparePredicateScalarRangeRoutingCandidate(*p)
	if !ok {
		return false
	}
	plan, cached, cacheHit, _, ok := candidate.core.loadComplementMaterialization()
	if !ok || !cacheHit {
		return false
	}
	if candidate.shouldPreferPositiveMaterializationForNullableComplement(plan) {
		return false
	}
	setPredicateMaterializedNot(p, cached)
	return true
}

func (qv *View) isPositiveOrderedMergedScalarRangeLeaf(e qir.Expr, orderField string) bool {
	if e.Not || !qv.hasFieldOrdinal(e.FieldOrdinal) || qv.exec.FieldNameByOrdinal(e.FieldOrdinal) == orderField || len(e.Operands) != 0 || !isScalarRangeEqOp(e.Op) {
		return false
	}
	fm := qv.fieldMetaByOrdinal(e.FieldOrdinal)
	return fm != nil && !fm.Slice
}

func (qv *View) buildMergedScalarRangePredicate(e qir.Expr, bounds indexdata.Bounds, allowMaterialize bool) (predicate, bool) {
	return qv.buildMergedScalarRangePredicateWithColdMode(e, bounds, allowMaterialize, false)
}

func (qv *View) buildMergedScalarRangePredicateWithColdMode(e qir.Expr, bounds indexdata.Bounds, allowMaterialize, lazyColdMaterialize bool) (predicate, bool) {
	return qv.buildMergedScalarRangePredicateWithColdModeAndWarmLoad(
		e,
		bounds,
		allowMaterialize,
		lazyColdMaterialize,
		true,
	)
}

func (qv *View) buildMergedScalarRangePredicateWithColdModeAndWarmLoad(
	e qir.Expr,
	bounds indexdata.Bounds,
	allowMaterialize, lazyColdMaterialize, allowWarmLoad bool,
) (predicate, bool) {

	fm := qv.fieldMetaByOrdinal(e.FieldOrdinal)
	if fm == nil || fm.Slice {
		return predicate{}, false
	}

	var core preparedScalarRangePredicate
	qv.initPreparedExactScalarRangePredicate(&core, e, fm, bounds)
	ov := qv.indexViewByOrdinal(e.FieldOrdinal)
	if !ov.HasData() {
		return predicate{}, false
	}

	p, ok := core.buildFromFieldIndexRange(ov, allowMaterialize, lazyColdMaterialize, allowWarmLoad)
	if ok {
		p.effectiveBounds = bounds
		p.hasEffectiveBounds = true
	}
	return p, ok
}

func (qv *View) initOrderedPredicateExpectedContainsCalls(p *predicate, orderedWindow int, universe uint64) {
	if p == nil || orderedWindow <= 0 || universe == 0 {
		return
	}
	if p.fieldIndexRangeState != nil {
		p.setExpectedContainsCalls(
			orderedFieldIndexRangeExpectedContainsCalls(p.fieldIndexRangeState, orderedWindow, universe),
		)
	}
	if p.postsAnyState != nil {
		p.postsAnyState.setExpectedContainsCalls(
			clampUint64ToInt(orderedPredicateExpectedRows(orderedWindow, p.estCard, universe)),
		)
	}
}

func (qv *View) postprocessBuiltOrderedPredicate(
	p *predicate,
	orderedRoute orderedScalarRangeRouting,
	predAllowMaterialize, orderedEagerMaterialize bool,
	orderRangeDeferred, allowWarmOrderedRangeLoad bool,
	orderedWindow int,
	universe uint64,
) {
	if p == nil {
		return
	}
	qv.initOrderedPredicateExpectedContainsCalls(p, orderedWindow, universe)
	if orderRangeDeferred || !p.hasRuntimeRangeState() {
		return
	}

	// Warm-cache reloads are cheap and should stay available even when ordered
	// build-time eager materialization is intentionally disabled for this shape.
	if allowWarmOrderedRangeLoad &&
		qv.loadWarmComplementPredicateForOrdered(p, orderedRoute.useComplement) {
		return
	}

	if orderedRoute.forceComplement {
		qv.tryMaterializeBroadRangeComplementPredicateForOrdered(
			p,
			orderedRoute.broadComplement,
			universe,
			orderedWindow,
			true,
		)
		return
	}

	if orderedEagerMaterialize {
		qv.tryMaterializeBroadRangeComplementPredicateForOrdered(
			p,
			orderedRoute.broadComplement,
			universe,
			orderedWindow,
			false,
		)
		return
	}

	if !predAllowMaterialize && orderedRoute.requirePromotion {
		qv.tryMaterializeBroadRangeComplementPredicateForOrdered(
			p,
			orderedRoute.broadComplement,
			universe,
			orderedWindow,
			false,
		)
	}
}

func (qv *View) buildPredicatesOrderedWithMode(
	leaves []qir.Expr,
	orderField string,
	allowMaterialize bool,
	orderedWindow int,
	orderedOffset uint64,
	coverOrderRange, allowOrderedEagerMaterialize bool,
) (predicateSet, bool) {

	if len(leaves) == 1 && leaves[0].Op == qir.OpConst && leaves[0].Not {
		preds := newPredicateSet(1)
		preds.Append(predicate{alwaysFalse: true})
		return preds, true
	}

	preds := newPredicateSet(len(leaves))
	universe := uint64(0)
	if !allowMaterialize {
		universe = qv.snap.Universe.Cardinality()
	}

	allowWarmOrderedRangeLoad := allowMaterialize || allowOrderedEagerMaterialize || !coverOrderRange || orderedOffset == 0
	allowWarmScalarRangeLoad := allowWarmOrderedRangeLoad

	routeUniverse := universe
	if routeUniverse == 0 && orderedWindow > 0 {
		routeUniverse = qv.snap.Universe.Cardinality()
	}

	var mergedRangesBuf []orderedMergedScalarRangeField
	if len(leaves) > 1 {
		mergedRangesBuf = orderedMergedScalarRangeFieldSlicePool.Get(len(leaves))
		var ok bool
		mergedRangesBuf, ok = qv.collectOrderedMergedScalarRangeFields(orderField, leaves, mergedRangesBuf)
		if !ok {
			preds.Release()
			orderedMergedScalarRangeFieldSlicePool.Put(mergedRangesBuf)
			return predicateSet{}, false
		}
		defer orderedMergedScalarRangeFieldSlicePool.Put(mergedRangesBuf)
	}

	for i, e := range leaves {
		fieldName := qv.exec.FieldNameByOrdinal(e.FieldOrdinal)
		orderRangeDeferred := !coverOrderRange && qv.isOrderRangeCoveredLeaf(orderField, e)

		if coverOrderRange && qv.isOrderRangeCoveredLeaf(orderField, e) {
			// Keep conversion/typing validation, but avoid expensive predicate
			// materialization for leaves that are fully covered by order range.
			rb, ok, err := qv.rangeBoundsForScalarExpr(e)
			if err != nil || !ok || rb.Empty {
				preds.Release()
				return predicateSet{}, false
			}
			preds.Append(predicate{
				expr:    e,
				covered: true,
			})
			continue
		}

		if qv.isPositiveOrderedMergedScalarRangeLeaf(e, orderField) {
			idx := findOrderedMergedScalarRangeField(mergedRangesBuf, fieldName)
			if idx >= 0 {
				merged := mergedRangesBuf[idx]
				if merged.count > 1 {
					if merged.first != i {
						continue
					}
					if boundsExactStringPrefix(merged.bounds) {
						prefixExpr := merged.expr
						prefixExpr.Op = qir.OpPREFIX
						prefixExpr.Value = merged.bounds.Prefix

						predAllowMaterialize := allowMaterialize
						orderedRoute := qv.orderedScalarRangeRoutingForBuild(
							predicate{expr: prefixExpr},
							predAllowMaterialize,
							orderedWindow,
							orderedOffset,
							universe,
							routeUniverse,
						)

						orderedEagerMaterialize := false
						if !predAllowMaterialize &&
							allowOrderedEagerMaterialize &&
							!qv.orderedScalarRangeHasWarmComplement(orderedRoute) &&
							qv.orderedScalarRangeCanEagerMaterialize(orderedRoute) {
							predAllowMaterialize = true
							orderedEagerMaterialize = true
						}

						lazyColdMaterialize := predAllowMaterialize && !orderedRoute.eagerMaterialize
						p, ok := qv.buildPredicateWithColdModeAndWarmLoad(
							prefixExpr,
							predAllowMaterialize,
							lazyColdMaterialize,
							allowWarmScalarRangeLoad,
						)
						if !ok {
							preds.Release()
							return predicateSet{}, false
						}
						p.hasEffectiveBounds = true
						p.effectiveBounds = merged.bounds

						qv.postprocessBuiltOrderedPredicate(
							&p,
							orderedRoute,
							predAllowMaterialize,
							orderedEagerMaterialize,
							false,
							allowWarmOrderedRangeLoad,
							orderedWindow,
							universe,
						)
						preds.Append(p)
						continue
					}
					mergedPred := predicate{
						expr:               merged.expr,
						hasEffectiveBounds: true,
						effectiveBounds:    merged.bounds,
					}

					predAllowMaterialize := allowMaterialize
					orderedRoute := qv.orderedScalarRangeRoutingForBuild(
						mergedPred,
						predAllowMaterialize,
						orderedWindow,
						orderedOffset,
						universe,
						routeUniverse,
					)

					orderedEagerMaterialize := false
					if !predAllowMaterialize &&
						allowOrderedEagerMaterialize &&
						!qv.orderedScalarRangeHasWarmComplement(orderedRoute) &&
						qv.orderedScalarRangeCanEagerMaterialize(orderedRoute) {
						predAllowMaterialize = true
						orderedEagerMaterialize = true
					}

					lazyColdMaterialize := predAllowMaterialize && !orderedRoute.eagerMaterialize
					p, ok := qv.buildMergedScalarRangePredicateWithColdModeAndWarmLoad(
						merged.expr,
						merged.bounds,
						predAllowMaterialize,
						lazyColdMaterialize,
						allowWarmScalarRangeLoad,
					)
					if !ok {
						preds.Release()
						return predicateSet{}, false
					}

					qv.postprocessBuiltOrderedPredicate(
						&p,
						orderedRoute,
						predAllowMaterialize,
						orderedEagerMaterialize,
						false,
						allowWarmOrderedRangeLoad,
						orderedWindow,
						universe,
					)
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
			orderedMergedScalarRangeFieldCount(mergedRangesBuf, fieldName) <= 1 &&
			fieldName != orderField &&
			!qv.orderedScalarRangeHasWarmComplement(orderedRoute) &&
			qv.orderedScalarRangeCanEagerMaterialize(orderedRoute) {
			predAllowMaterialize = true
			orderedEagerMaterialize = true
		}

		lazyColdMaterialize := predAllowMaterialize && !orderedRoute.eagerMaterialize
		p, ok := qv.buildPredicateWithColdModeAndWarmLoad(
			e,
			predAllowMaterialize,
			lazyColdMaterialize,
			allowWarmScalarRangeLoad,
		)
		if !ok {
			preds.Release()
			return predicateSet{}, false
		}

		qv.postprocessBuiltOrderedPredicate(
			&p,
			orderedRoute,
			predAllowMaterialize,
			orderedEagerMaterialize,
			orderRangeDeferred,
			allowWarmOrderedRangeLoad,
			orderedWindow,
			universe,
		)
		preds.Append(p)
	}

	return preds, true
}

func (qv *View) buildPredicateWithMode(e qir.Expr, allowMaterialize bool) (predicate, bool) {
	return qv.buildPredicateWithColdMode(e, allowMaterialize, false)
}

func (qv *View) buildPredicateWithColdMode(e qir.Expr, allowMaterialize bool, lazyColdMaterialize bool) (predicate, bool) {
	return qv.buildPredicateWithColdModeAndWarmLoad(e, allowMaterialize, lazyColdMaterialize, true)
}

func (qv *View) buildPredicateWithColdModeAndWarmLoad(
	e qir.Expr,
	allowMaterialize, lazyColdMaterialize, allowWarmLoad bool,
) (predicate, bool) {

	if e.Op == qir.OpConst {
		if e.Not {
			return predicate{expr: e, alwaysFalse: true}, true
		}
		return predicate{expr: e, alwaysTrue: true}, true
	}

	if !qv.hasFieldOrdinal(e.FieldOrdinal) {
		return predicate{}, false
	}

	fm := qv.fieldMetaByOrdinal(e.FieldOrdinal)
	if fm == nil {
		return predicate{}, false
	}

	if e.Op.IsScalarRangeOrPrefix() {
		ov := qv.indexViewByOrdinal(e.FieldOrdinal)
		if !ov.HasData() {
			return predicate{}, false
		}
		return qv.buildPredRangeCandidateWithColdModeAndWarmLoad(
			e,
			fm,
			ov,
			allowMaterialize,
			lazyColdMaterialize,
			allowWarmLoad,
		)
	}

	p, ok := qv.buildPredicateCandidate(e)
	if !ok {
		return predicate{}, false
	}
	return p, true
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

const (
	rangePostingFilterKeepProbeMaxBuckets = 24
	rangePostingFilterKeepRowsMaxCard     = 8_192
)

func shouldUseNumericRangePostingFilter(e qir.Expr, fm *schema.Field) bool {
	if !schema.FieldUsesOrderedNumericKeys(fm) || !e.Op.IsNumericRange() {
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

func applyRangeProbePostingFilter(
	dst posting.List,
	keepProbeHits bool,
	probeLen int,
	probe fieldIndexRangeProbe,
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

	if rowProbeBudget < 1 { // keep it
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

type fieldIndexRangeProbe struct {
	ov            indexdata.FieldIndexView
	spans         [2]indexdata.FieldIndexRange
	spanCnt       int
	useComplement bool
	probeLen      int
	probeEst      uint64
}

func newFieldIndexRangeProbe(
	ov indexdata.FieldIndexView,
	br indexdata.FieldIndexRange,
	useComplement bool,
	precomputedProbeLen int,
	precomputedProbeEst uint64,
) fieldIndexRangeProbe {

	p := fieldIndexRangeProbe{ov: ov, useComplement: useComplement}

	if useComplement {
		before, after := fieldIndexComplementRangeSpans(ov, br)
		if !before.Empty() {
			p.spans[p.spanCnt] = before
			p.spanCnt++
		}
		if !after.Empty() {
			p.spans[p.spanCnt] = after
			p.spanCnt++
		}

	} else if !br.Empty() {
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

func (p *fieldIndexRangeProbe) scanStats() {
	if p == nil {
		return
	}
	for i := 0; i < p.spanCnt; i++ {
		cur := p.ov.NewCursor(p.spans[i], false)
		for {
			ids, _, single, ok := cur.NextPostingOrSingle()
			if !ok {
				break
			}
			card := uint64(1)
			if !single {
				if ids.IsEmpty() {
					continue
				}
				card = ids.Cardinality()
			}
			p.probeLen++
			p.probeEst = mathutil.SatAddUint64(p.probeEst, card)
		}
	}
}

func (p fieldIndexRangeProbe) linearContains(idx uint64) bool {
	for i := 0; i < p.spanCnt; i++ {
		cur := p.ov.NewCursor(p.spans[i], false)
		for {
			ids, singleIdx, single, ok := cur.NextPostingOrSingle()
			if !ok {
				break
			}
			if single {
				if singleIdx == idx {
					return true
				}
				continue
			}
			if !ids.IsEmpty() && ids.Contains(idx) {
				return true
			}
		}
	}
	return false
}

func (p fieldIndexRangeProbe) buildAndNotPosting(dst posting.List) posting.List {
	if dst.IsEmpty() {
		return dst
	}
	for i := 0; i < p.spanCnt; i++ {
		cur := p.ov.NewCursor(p.spans[i], false)
		for {
			ids, idx, single, ok := cur.NextPostingOrSingle()
			if !ok {
				break
			}
			if single {
				dst = dst.BuildRemoved(idx)
				if dst.IsEmpty() {
					return dst
				}
				continue
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

func (p fieldIndexRangeProbe) appendIntersectingPosting(dst posting.List, builder postingUnionBuilder) postingUnionBuilder {
	if dst.IsEmpty() {
		return builder
	}
	for i := 0; i < p.spanCnt; i++ {
		cur := p.ov.NewCursor(p.spans[i], false)
		for {
			ids, idx, single, ok := cur.NextPostingOrSingle()
			if !ok {
				break
			}
			if single {
				if dst.Contains(idx) {
					builder.addSingle(idx)
				}
				continue
			}
			builder = postingListAppendIntersecting(dst, ids, builder)
		}
	}
	return builder
}

func (p fieldIndexRangeProbe) countBucket(bucket posting.List) (uint64, bool) {
	if bucket.IsEmpty() {
		return 0, true
	}
	if !rangeCountBucketUseful(p.probeLen, p.probeEst, bucket.Cardinality()) {
		return 0, false
	}

	var hit uint64
	for i := 0; i < p.spanCnt; i++ {
		cur := p.ov.NewCursor(p.spans[i], false)
		for {
			ids, idx, single, ok := cur.NextPostingOrSingle()
			if !ok {
				break
			}
			if single {
				if bucket.Contains(idx) {
					hit++
				}
				continue
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

func materializedRangePredicateWithMode(e qir.Expr, ids posting.List) predicate {
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

func isSingletonHeavyPostingBuf(probe []posting.List) bool {
	probeLen := 0
	singletons := 0
	est := uint64(0)
	if probe == nil {
		return false
	}
	for i := 0; i < len(probe); i++ {
		p := probe[i]
		if p.IsEmpty() {
			continue
		}
		probeLen++
		card := p.Cardinality()
		if card == 1 {
			singletons++
		}
		est = mathutil.SatAddUint64(est, card)
	}
	return shouldUseFastSinglesUnion(probeLen, singletons, est)
}

func materializePostingBufFast(probe []posting.List) posting.List {
	builder := newPostingUnionBuilder(true)
	defer builder.release()

	if probe == nil {
		return posting.List{}
	}
	for i := 0; i < len(probe); i++ {
		builder.addPosting(probe[i])
	}
	return builder.finish(true)
}

const postingUnionSubsetProbeMaxRatio = 4 // Keep failed two-term subset probes cheaper than the full union path they guard.

func materializePostingUnionBufOwned(posts []posting.List) posting.List {
	if len(posts) == 2 {
		a := posts[0]
		b := posts[1]
		if a.IsEmpty() {
			return b.Clone().BuildOptimized()
		}
		if b.IsEmpty() {
			return a.Clone().BuildOptimized()
		}
		ac := a.Cardinality()
		bc := b.Cardinality()
		if ac <= bc/postingUnionSubsetProbeMaxRatio && a.AndCardinality(b) == ac {
			return b.Clone().BuildOptimized()
		}
		if bc <= ac/postingUnionSubsetProbeMaxRatio && b.AndCardinality(a) == bc {
			return a.Clone().BuildOptimized()
		}
	}

	builder := newPostingUnionBuilder(true)
	defer builder.release()

	if posts == nil {
		return posting.List{}
	}
	for i := 0; i < len(posts); i++ {
		builder.addPosting(posts[i])
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
	plannerOrderedAnchorHashCandidateMin = 64                   // Below this, posting.Contains is cheaper than building a pooled hash for order-anchor probes.
	plannerOrderedAnchorHashCandidateMax = u64SetPoolMaxCap / 2 // Larger candidate sets would leave pooled hash classes and increase reader heap pressure.
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

func orderedAnchorLeadOpWeight(op qir.Op) float64 {
	switch op {
	case qir.OpEQ:
		return 1.0
	case qir.OpIN, qir.OpHASALL, qir.OpHASANY:
		return 1.35
	default:
		if op.IsScalarRangeOrPrefix() {
			return 1.9
		}
		return 2.5
	}
}

func orderedPredicateIsRangeLike(p predicate) bool {
	return p.expr.Op.IsScalarRangeOrPrefix()
}

func chooseOrderedAnchorLeadReader(qv *View, orderField string, preds predicateReader, active []int) (int, bool) {
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
		if qv.hasFieldOrdinal(p.expr.FieldOrdinal) && qv.exec.FieldNameByOrdinal(p.expr.FieldOrdinal) == orderField {
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

func (qv *View) execPlanOrderedBasicAnchoredWithScratch(
	q *qir.Shape,
	preds predicateReader,
	active []int,
	ov indexdata.FieldIndexView,
	br indexdata.FieldIndexRange,
	trace *Trace,
	exactChecks, residualChecks, deferredChecks []int,
) ([]uint64, bool) {

	if len(active) < plannerOrderedAnchorMinActive {
		if len(active) != 1 ||
			!orderedPredicateIsRangeLike(preds[active[0]]) ||
			qv.exec.FieldNameByOrdinal(preds[active[0]].expr.FieldOrdinal) == qv.exec.FieldNameByOrdinal(q.Order.FieldOrdinal) {
			return nil, false
		}
	}
	if len(active) == 0 {
		return nil, false
	}
	if br.BaseEnd-br.BaseStart < plannerOrderedAnchorSpanMin {
		return nil, false
	}

	leadIdx, ok := chooseOrderedAnchorLeadReader(qv, qv.exec.FieldNameByOrdinal(q.Order.FieldOrdinal), preds, active)
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
	if br.BaseStart == 0 && br.BaseEnd == ov.KeyCount() {
		wideLeadMax := candidateCap * 8
		if wideLeadMax < candidateCap {
			wideLeadMax = ^uint64(0)
		}
		if preds[leadIdx].estCard > wideLeadMax {
			return nil, false
		}
	}

	leadIt := (&preds[leadIdx]).newIter()
	if leadIt == nil {
		return nil, false
	}
	defer leadIt.Release()

	leadNeedsCheck := (&preds[leadIdx]).leadIterNeedsContainsCheck()

	exactChecks = exactChecks[:0]
	residualChecks = residualChecks[:0]
	deferredChecks = deferredChecks[:0]

	orderedExactPreferred := false
	for _, pi := range active {
		if pi == leadIdx && !leadNeedsCheck {
			continue
		}
		deferredChecks = append(deferredChecks, pi)
	}

	exactChecks = buildExactBucketPostingFilterActiveReader(exactChecks, deferredChecks, preds)

	for _, pi := range exactChecks {
		if preds[pi].postingFilterCapability().prefersExactBucket() {
			orderedExactPreferred = true
			break
		}
	}

	residualChecks = plannerResidualChecks(residualChecks, deferredChecks, exactChecks)
	useExactPosting := len(exactChecks) > 0 && (len(residualChecks) == 0 || orderedExactPreferred)

	var candidates posting.List
	var candidateIDs posting.List
	var candidateHash u64set
	var candidateHashPtr *u64set
	var exactWork posting.List
	totalCandidates := uint64(0)

	leadExamined := uint64(0)
	directHash := candidateCap <= uint64(plannerOrderedAnchorHashCandidateMax) &&
		br.Len() > int(candidateCap)*4
	if directHash {
		candidateHash = getU64Set(int(candidateCap))
		defer releaseU64Set(&candidateHash)
		candidateHashPtr = &candidateHash
		for leadIt.HasNext() {
			leadExamined++
			if leadExamined > leadBudget {
				return nil, false
			}
			if trace != nil {
				trace.AddExamined(1)
			}
			idx := leadIt.Next()
			if !orderedBasicAnchoredMatchesActiveReader(preds, active, leadIdx, leadNeedsCheck, idx) {
				continue
			}
			if candidateHash.Add(idx) {
				totalCandidates++
				if totalCandidates > candidateCap {
					return nil, false
				}
			}
		}
		deferredChecks = deferredChecks[:0]

	} else if useExactPosting {
		builder := newPostingUnionBuilder(postingBatchSinglesEnabled(leadBudget))
		defer builder.release()
		for leadIt.HasNext() {
			leadExamined++
			if leadExamined > leadBudget {
				return nil, false
			}
			if trace != nil {
				trace.AddExamined(1)
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
					trace.AddExamined(1)
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
					trace.AddExamined(1)
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

	if directHash {
		if totalCandidates == 0 {
			if trace != nil {
				trace.SetEarlyStopReason("no_candidates")
			}
			return nil, true
		}

	} else if candidates.IsEmpty() {
		if trace != nil {
			trace.SetEarlyStopReason("no_candidates")
		}
		return nil, true

	} else if useExactPosting {
		candidateIDs = candidates
		exactApplied := false
		mode, exactIDs, exactWork, _ := plannerFilterPostingByPredicateChecks(preds, exactChecks, candidates, exactWork, true)
		switch mode {

		case plannerPredicateBucketEmpty:
			if trace != nil {
				trace.SetEarlyStopReason("no_candidates")
			}
			exactWork.Release()
			return nil, true

		case plannerPredicateBucketAll:
			candidateIDs = candidates
			exactApplied = true

		case plannerPredicateBucketExact:
			if trace != nil {
				trace.AddPostingExactFilter(1)
			}
			candidateIDs = exactIDs
			exactApplied = true

		default:
			candidateIDs = candidates
		}

		if candidateIDs.IsEmpty() {
			if trace != nil {
				trace.SetEarlyStopReason("no_candidates")
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
					trace.SetEarlyStopReason("no_candidates")
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

	} else {
		candidateIDs = candidates
	}
	if !directHash {
		totalCandidates = candidateIDs.Cardinality()
	}

	o := q.Order
	skip := q.Offset
	need := q.Limit
	capHint, _ := boundedWindowCap(totalCandidates, skip, need)
	out := make([]uint64, 0, clampUint64ToInt(capHint))
	seenCandidates := uint64(0)
	scanWidth := uint64(0)
	singleDeferred := -1

	if len(deferredChecks) == 1 {
		singleDeferred = deferredChecks[0]
	}
	if candidateHashPtr == nil &&
		totalCandidates >= plannerOrderedAnchorHashCandidateMin &&
		totalCandidates <= uint64(plannerOrderedAnchorHashCandidateMax) &&
		br.Len() > int(totalCandidates)*4 {
		candidateHash = getU64Set(int(totalCandidates))
		it := candidateIDs.Iter()
		for it.HasNext() {
			candidateHash.Add(it.Next())
		}
		it.Release()
		candidateHashPtr = &candidateHash
		defer releaseU64Set(&candidateHash)
	}
	cursor := newQueryCursor(out, skip, need, false, 0)

	keyCur := ov.NewCursor(br, o.Desc)
	for {
		ids, idx, single, ok := keyCur.NextPostingOrSingle()
		if !ok {
			break
		}
		if single {
			scanWidth++
			if trace != nil {
				trace.AddExamined(1)
			}
			if candidateHashPtr != nil {
				if !candidateHashPtr.Has(idx) {
					continue
				}
			} else if !candidateIDs.Contains(idx) {
				continue
			}
			seenCandidates++
			if !orderedPredicateChecksMatchReader(preds, deferredChecks, singleDeferred, idx) {
				if seenCandidates == totalCandidates {
					break
				}
				continue
			}
			if cursor.emit(idx) {
				break
			}
			if seenCandidates == totalCandidates {
				break
			}
			continue
		}
		if orderedBasicAnchoredEmitPosting(
			&cursor,
			candidateIDs,
			candidateHashPtr,
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
		trace.AddOrderScanWidth(scanWidth)
		if cursor.need == 0 {
			trace.SetEarlyStopReason("limit_reached")
		} else if seenCandidates == totalCandidates {
			trace.SetEarlyStopReason("candidates_exhausted")
		} else {
			trace.SetEarlyStopReason("order_index_exhausted")
		}
	}
	exactWork.Release()

	return cursor.out, true
}

func (qv *View) execPlanOrderedBasicReaderGuarded(q *qir.Shape, preds predicateReader, guard plannerOrderedLimitRuntimeGuard, trace *Trace) ([]uint64, bool) {
	o := q.Order

	fm := qv.fieldMetaByOrdinal(o.FieldOrdinal)
	if fm == nil || fm.Slice {
		return nil, false
	}
	ov := qv.indexViewByOrdinal(o.FieldOrdinal)
	nilOV := qv.nilIndexViewByOrdinal(o.FieldOrdinal)
	if !ov.HasData() && !nilOV.HasData() {
		return nil, false
	}

	orderField := qv.exec.FieldNameByOrdinal(o.FieldOrdinal)
	rb, br, rangeCovered, ok := qv.extractOrderRangeCoverageIndexViewWithBoundsReader(orderField, preds, ov)
	if !ok {
		return nil, false
	}
	nilTailField := orderNilTailField(fm, orderField, rb)
	if br.Empty() && nilTailField == "" {
		pooled.ReleaseBoolSlice(rangeCovered)
		return nil, true
	}
	for i, is := range rangeCovered {
		if is {
			(&preds[i]).covered = true
		}
	}
	pooled.ReleaseBoolSlice(rangeCovered)

	if len(preds) > plannerPredicateFastPathMaxLeaves {

		activeBuf := pooled.GetIntSlice(len(preds))
		defer pooled.ReleaseIntSlice(activeBuf)

		for i := 0; i < len(preds); i++ {
			p := preds[i]
			if p.covered || p.alwaysTrue {
				continue
			}
			if p.alwaysFalse {
				return nil, true
			}
			activeBuf = append(activeBuf, i)
		}

		if !fm.Ptr {
			exactChecksBuf := pooled.GetIntSlice(len(activeBuf))
			defer pooled.ReleaseIntSlice(exactChecksBuf)

			residualChecksBuf := pooled.GetIntSlice(len(activeBuf))
			defer pooled.ReleaseIntSlice(residualChecksBuf)

			deferredChecksBuf := pooled.GetIntSlice(len(activeBuf))
			defer pooled.ReleaseIntSlice(deferredChecksBuf)

			if out, ok := qv.execPlanOrderedBasicAnchoredWithScratch(
				q,
				preds,
				activeBuf,
				ov,
				br,
				trace,
				exactChecksBuf,
				residualChecksBuf,
				deferredChecksBuf,
			); ok {
				if trace != nil {
					trace.SetPlan(rbitrace.PlanOrderedAnchor)
				}
				return out, true
			}
		}
		if len(activeBuf) == 0 {
			return qv.scanOrderLimitNoPredicates(q, ov, br, o.Desc, nilTailField, trace)
		}
		return qv.scanOrderLimitWithPredicatesReaderGuarded(q, ov, br, o.Desc, preds, nilTailField, guard, trace)
	}

	var activeInline [plannerPredicateFastPathMaxLeaves]int
	active := activeInline[:0]
	for i := 0; i < len(preds); i++ {
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
		var (
			exactChecksInline    [plannerPredicateFastPathMaxLeaves]int
			residualChecksInline [plannerPredicateFastPathMaxLeaves]int
			deferredChecksInline [plannerPredicateFastPathMaxLeaves]int
		)
		if out, ok := qv.execPlanOrderedBasicAnchoredWithScratch(
			q,
			preds,
			active,
			ov,
			br,
			trace,
			exactChecksInline[:0],
			residualChecksInline[:0],
			deferredChecksInline[:0],
		); ok {
			if trace != nil {
				trace.SetPlan(rbitrace.PlanOrderedAnchor)
			}
			return out, true
		}
	}

	sortActivePredicatesReader(active, preds)
	if len(active) == 0 {
		return qv.scanOrderLimitNoPredicates(q, ov, br, o.Desc, nilTailField, trace)
	}
	return qv.scanOrderLimitWithPredicatesReaderGuarded(q, ov, br, o.Desc, preds, nilTailField, guard, trace)
}

// execPlanOrderedBasicFallback scans ordered buckets and applies remaining
// predicates when ordered-anchor shortcuts are not available.
func (qv *View) execPlanOrderedBasicFallback(q *qir.Shape, preds []predicate, active []int, ov indexdata.FieldIndexView, br indexdata.FieldIndexRange, trace *Trace) []uint64 {
	return qv.execPlanOrderedBasicFallbackWithNilTail(q, preds, active, ov, br, "", trace)
}

func (qv *View) execPlanOrderedBasicFallbackWithNilTail(q *qir.Shape, preds []predicate, active []int, ov indexdata.FieldIndexView, br indexdata.FieldIndexRange, nilTailField string, trace *Trace) []uint64 {
	return qv.scanOrderedIndexViewWithPredicatesWithNilTail(q, preds, active, ov, br, q.Order.Desc, nilTailField, trace)
}

func (qv *View) scanOrderedIndexViewNoPredicatesWithNilTail(q *qir.Shape, ov indexdata.FieldIndexView, br indexdata.FieldIndexRange, desc bool, nilTailField string, trace *Trace) []uint64 {
	extraRows := uint64(0)
	if nilTailField != "" {
		extraRows = qv.nilIndexViewByName(nilTailField).LookupCardinality(indexdata.NilIndexEntryKey)
	}
	capHint, exhausted := fieldIndexRangeWindowCap(ov, br, q.Offset, q.Limit, extraRows)
	if exhausted {
		if trace != nil {
			trace.AddOrderScanWidth(0)
			trace.SetEarlyStopReason("input_exhausted")
		}
		return nil
	}
	out := make([]uint64, 0, clampUint64ToInt(capHint))
	cursor := newQueryCursor(out, q.Offset, q.Limit, false, 0)

	var scanWidth uint64

	cur := ov.NewCursor(br, desc)
	for {
		ids, idx, single, ok := cur.NextPostingOrSingle()
		if !ok {
			break
		}
		if single {
			scanWidth++
			if trace != nil {
				trace.AddExamined(1)
				trace.AddMatched(1)
			}
			if cursor.emit(idx) {
				break
			}
			continue
		}
		if orderedScanEmitBucketNoPredicates(&cursor, ids, trace, &scanWidth) {
			break
		}
	}

	if cursor.need > 0 && nilTailField != "" {
		ids := qv.nilIndexViewByName(nilTailField).LookupPostingRetained(indexdata.NilIndexEntryKey)
		orderedScanEmitBucketNoPredicates(&cursor, ids, trace, &scanWidth)
	}

	if trace != nil {
		trace.AddOrderScanWidth(scanWidth)
		if cursor.need == 0 {
			trace.SetEarlyStopReason("limit_reached")
		} else {
			trace.SetEarlyStopReason("order_index_exhausted")
		}
	}

	return cursor.out
}

func (qv *View) scanOrderedIndexViewWithPredicatesWithNilTail(q *qir.Shape, preds []predicate, active []int, ov indexdata.FieldIndexView, br indexdata.FieldIndexRange, desc bool, nilTailField string, trace *Trace) []uint64 {
	if len(active) == 0 {
		return qv.scanOrderedIndexViewNoPredicatesWithNilTail(q, ov, br, desc, nilTailField, trace)
	}

	extraRows := uint64(0)
	if nilTailField != "" {
		extraRows = qv.nilIndexViewByName(nilTailField).LookupCardinality(indexdata.NilIndexEntryKey)
	}
	capHint, exhausted := fieldIndexRangeWindowCap(ov, br, q.Offset, q.Limit, extraRows)
	if exhausted {
		if trace != nil {
			trace.AddOrderScanWidth(0)
			trace.SetEarlyStopReason("input_exhausted")
		}
		return nil
	}
	out := make([]uint64, 0, clampUint64ToInt(capHint))
	cursor := newQueryCursor(out, q.Offset, q.Limit, false, 0)

	var exactActiveInline [plannerPredicateFastPathMaxLeaves]int
	var residualActiveInline [plannerPredicateFastPathMaxLeaves]int

	exactActive := buildExactBucketPostingFilterActiveReader(exactActiveInline[:0], active, preds)
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

	var (
		exactWork posting.List
		scanWidth uint64
	)

	cur := ov.NewCursor(br, desc)
	for {
		ids, idx, single, ok := cur.NextPostingOrSingle()
		if !ok {
			break
		}
		if single {
			scanWidth++
			if trace != nil {
				trace.AddExamined(1)
			}
			if orderedPredicateChecksMatchReader(preds, active, singleActive, idx) &&
				orderedScanEmitMatched(&cursor, trace, idx) {
				break
			}
			continue
		}
		stop, nextExactWork := orderedScanEmitBucketWithPredicates(
			&cursor,
			preds,
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
		exactWork = nextExactWork
		if stop {
			break
		}
	}

	if cursor.need > 0 && nilTailField != "" {
		ids := qv.nilIndexViewByName(nilTailField).LookupPostingRetained(indexdata.NilIndexEntryKey)
		_, exactWork = orderedScanEmitBucketWithPredicates(
			&cursor,
			preds,
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
		trace.AddOrderScanWidth(scanWidth)
		if cursor.need == 0 {
			trace.SetEarlyStopReason("limit_reached")
		} else {
			trace.SetEarlyStopReason("order_index_exhausted")
		}
	}
	exactWork.Release()

	return cursor.out
}

func (qv *View) collectPredicateRangeBoundsReader(field string, preds predicateReader) (indexdata.Bounds, []bool, bool, bool) {
	var rb indexdata.Bounds

	covered := pooled.GetBoolSlice(len(preds))[:len(preds)]
	clear(covered)

	has := false
	for i := 0; i < len(preds); i++ {
		p := preds[i]
		if p.expr.Not || qv.exec.FieldNameByOrdinal(p.expr.FieldOrdinal) != field {
			continue
		}
		if p.hasEffectiveBounds {
			mergeRangeBounds(&rb, p.effectiveBounds)
			covered[i] = true
			has = true
			continue
		}
		if applied, ok := qv.applyScalarExprToRangeBounds(p.expr, &rb); !ok {
			pooled.ReleaseBoolSlice(covered)
			return indexdata.Bounds{}, nil, false, false
		} else if applied {
			covered[i] = true
			has = true
		}
	}
	if !has {
		covered = covered[:0]
	}

	return rb, covered, has, true
}
