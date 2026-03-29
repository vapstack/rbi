package rbi

import (
	"math"
	"slices"
	"strconv"
	"sync"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/posting"
)

type predicate struct {
	expr qx.Expr

	contains func(uint64) bool
	iter     func() posting.Iterator
	estCard  uint64

	kind     predicateKind
	iterKind predicateIterKind
	posting  posting.List
	posts    []posting.List
	ids      posting.List

	alwaysTrue  bool
	alwaysFalse bool
	covered     bool

	// exact bucket match count when known; ok=false means unknown.
	bucketCount func(posting.List) (uint64, bool)
	// exact posting application when available without full predicate
	// materialization.
	postingFilter      func(*posting.List) bool
	postingFilterCheap bool

	cleanup func()
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

func (p predicate) hasContains() bool {
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

func (p predicate) hasIter() bool {
	switch p.iterKind {
	case predicateIterPosting, predicateIterPostsConcat, predicateIterPostsUnion, predicateIterMaterialized:
		return true
	default:
		return p.iter != nil
	}
}

func (p predicate) leadIterNeedsContainsCheck() bool {
	// HAS(list) uses a single posting as iterator seed and validates the rest
	// via contains checks; iterator alone is not exact when term count > 1.
	return p.kind == predicateKindPostsAll && len(p.posts) > 1
}

func (p predicate) supportsPostingFilter() bool {
	if p.alwaysTrue || p.alwaysFalse || p.covered {
		return true
	}
	switch p.kind {
	case predicateKindPosting,
		predicateKindPostingNot,
		predicateKindPostsAll,
		predicateKindPostsAnyNot,
		predicateKindMaterialized,
		predicateKindMaterializedNot:
		if p.kind == predicateKindPostsAnyNot && len(p.posts) > predicatePostsAnyNotExactPostingMaxTerms {
			return false
		}
		return true
	case predicateKindPostsAny:
		return false
	default:
		return p.postingFilter != nil && p.postingFilterCheap
	}
}

func (p predicate) supportsExactBucketPostingFilter() bool {
	if p.alwaysTrue || p.alwaysFalse || p.covered {
		return false
	}
	switch p.kind {
	case predicateKindPosting,
		predicateKindPostingNot,
		predicateKindPostsAll,
		predicateKindPostsAnyNot,
		predicateKindMaterialized,
		predicateKindMaterializedNot:
		if p.kind == predicateKindPostsAnyNot && len(p.posts) > predicatePostsAnyNotExactPostingMaxTerms {
			return false
		}
		return true
	case predicateKindPostsAny:
		return p.postingFilter != nil
	default:
		return false
	}
}

func (p predicate) prefersExactBucketPostingFilter() bool {
	return p.kind == predicateKindPostsAny && p.postingFilter != nil
}

func (p predicate) newIter() posting.Iterator {
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

func (p predicate) matches(idx uint64) bool {
	if p.alwaysTrue {
		return true
	}
	if p.alwaysFalse {
		return false
	}
	switch p.kind {
	case predicateKindPosting:
		return p.posting.Contains(idx)
	case predicateKindPostingNot:
		return !p.posting.Contains(idx)
	case predicateKindPostsAny:
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
		if p.contains == nil {
			return false
		}
		return p.contains(idx)
	}
}

func (p predicate) countBucket(bucket posting.List) (uint64, bool) {
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
		if p.bucketCount == nil {
			return 0, false
		}
		return p.bucketCount(bucket)
	}
}

// applyToPosting intersects (or subtracts for NOT variants) predicate matches
// with dst in place. Returns false when exact postingResult application is not
// available for this predicate shape.
func (p predicate) applyToPosting(dst *posting.List) bool {
	if dst == nil {
		return false
	}
	if p.alwaysTrue || p.covered {
		return true
	}
	if p.alwaysFalse {
		dst.Clear()
		return true
	}

	switch p.kind {
	case predicateKindPosting:
		dst.AndInPlace(p.posting)
		return true

	case predicateKindPostingNot:
		dst.AndNotInPlace(p.posting)
		return true

	case predicateKindPostsAnyNot:
		if len(p.posts) > predicatePostsAnyNotExactPostingMaxTerms {
			return false
		}
		for _, ids := range p.posts {
			dst.AndNotInPlace(ids)
			if dst.IsEmpty() {
				return true
			}
		}
		return true

	case predicateKindPostsAny:
		if p.postingFilter != nil {
			return p.postingFilter(dst)
		}
		return false

	case predicateKindPostsAll:
		if len(p.posts) == 0 {
			dst.Clear()
			return true
		}
		for _, ids := range p.posts {
			if ids.IsEmpty() {
				dst.Clear()
				return true
			}
			dst.AndInPlace(ids)
			if dst.IsEmpty() {
				return true
			}
		}
		return true

	case predicateKindMaterialized:
		if p.ids.IsEmpty() {
			dst.Clear()
			return true
		}
		dst.AndInPlace(p.ids)
		return true

	case predicateKindMaterializedNot:
		dst.AndNotInPlace(p.ids)
		return true
	}

	if p.postingFilter != nil {
		return p.postingFilter(dst)
	}
	return false
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

func (qv *queryView[K, V]) buildPostsAnyPostingFilter(posts []posting.List) (func(*posting.List) bool, func()) {
	if len(posts) <= 1 {
		return nil, nil
	}

	var (
		once sync.Once
		mat  posting.List
	)
	ensure := func() posting.List {
		once.Do(func() {
			if isSingletonHeavyProbe(posts, 16, 32) {
				mat = materializeProbeFast(posts)
			} else {
				mat = qv.materializeProbeUnion(posts)
			}
		})
		return mat
	}
	filter := func(dst *posting.List) bool {
		if dst == nil {
			return false
		}
		if dst.IsEmpty() {
			return true
		}
		union := ensure()
		if union.IsEmpty() {
			dst.Clear()
			return true
		}
		dst.AndInPlace(union)
		return true
	}
	cleanup := func() {
		mat.Release()
	}
	return filter, cleanup
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

	var leavesBuf [8]qx.Expr
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

		preds, ok := qv.buildPredicatesCandidate(leaves)
		if !ok {
			return nil, false, nil
		}
		defer releasePredicates(preds)

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
	if len(leaves) == 1 && !leaves[0].Not {
		switch leaves[0].Op {
		case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
			return nil, false, nil
		}
	}

	// pure EQ conjunctions are already handled very well by baseline paths
	allEq := len(leaves) > 0
	for _, e := range leaves {
		if e.Not || e.Op != qx.OpEQ {
			allEq = false
			break
		}
	}
	if allEq {
		return nil, false, nil
	}

	preds, ok := qv.buildPredicatesCandidate(leaves)
	if !ok {
		return nil, false, nil
	}
	defer releasePredicates(preds)

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
		if e.Op != qx.OpEQ && e.Op != qx.OpIN {
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

func (qv *queryView[K, V]) buildPredicatesCandidate(leaves []qx.Expr) ([]predicate, bool) {
	if len(leaves) == 1 && leaves[0].Op == qx.OpNOOP && leaves[0].Not {
		return []predicate{{alwaysFalse: true}}, true
	}

	preds := make([]predicate, 0, len(leaves))
	for _, e := range leaves {
		p, ok := qv.buildPredicateCandidate(e)
		if !ok {
			releasePredicates(preds)
			return nil, false
		}
		preds = append(preds, p)
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

	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
		if !ov.hasData() {
			return predicate{}, false
		}
		return qv.buildPredRangeCandidateWithMode(e, fm, ov, true)

	case qx.OpSUFFIX, qx.OpCONTAINS:
		return qv.buildPredMaterializedCandidate(e)

	default:
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
			expr: e,
			kind: predicateKindMaterializedNot,
			ids:  ids,
			cleanup: func() {
				ids.Release()
			},
		}, true
	}

	if b.ids.IsEmpty() {
		b.release()
		return predicate{expr: e, alwaysFalse: true}, true
	}

	ids := b.ids
	return predicate{
		expr:     e,
		kind:     predicateKindMaterialized,
		iterKind: predicateIterMaterialized,
		ids:      ids,
		estCard:  ids.Cardinality(),
		cleanup: func() {
			ids.Release()
		},
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
	posts, est, cleanup := qv.scalarLookupPostings(e.Field, vals, hasNil)

	if e.Not {
		if len(posts) == 0 {
			cleanup()
			return predicate{expr: e, alwaysTrue: true}, true
		}
		if len(posts) == 1 {
			return predicate{
				expr:    e,
				kind:    predicateKindPostingNot,
				posting: posts[0],
				cleanup: cleanup,
			}, true
		}
		return predicate{
			expr:    e,
			kind:    predicateKindPostsAnyNot,
			posts:   posts,
			cleanup: cleanup,
		}, true
	}

	if len(posts) == 0 {
		cleanup()
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
			cleanup:  cleanup,
		}, true
	}

	anyPostingFilter, anyPostingCleanup := qv.buildPostsAnyPostingFilter(posts)
	if anyPostingCleanup != nil {
		prevCleanup := cleanup
		cleanup = func() {
			anyPostingCleanup()
			prevCleanup()
		}
	}

	return predicate{
		expr:          e,
		kind:          predicateKindPostsAny,
		iterKind:      predicateIterPostsConcat,
		posts:         posts,
		estCard:       est,
		postingFilter: anyPostingFilter,
		cleanup:       cleanup,
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

	cleanup := func() {
		postsBuf.values = posts
		releasePostingSliceBuf(postsBuf)
	}

	if e.Not {
		if len(posts) == 1 {
			return predicate{
				expr:    e,
				kind:    predicateKindPostingNot,
				posting: posts[0],
				cleanup: cleanup,
			}, true
		}
		return predicate{
			expr:    e,
			kind:    predicateKindPostsAllNot,
			posts:   posts,
			cleanup: cleanup,
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
		cleanup:  cleanup,
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
	cleanup := func() {
		postsBuf.values = posts
		releasePostingSliceBuf(postsBuf)
	}

	if e.Not {
		if len(posts) == 0 {
			cleanup()
			return predicate{expr: e, alwaysTrue: true}, true
		}
		if len(posts) == 1 {
			return predicate{
				expr:    e,
				kind:    predicateKindPostingNot,
				posting: posts[0],
				cleanup: cleanup,
			}, true
		}
		return predicate{
			expr:    e,
			kind:    predicateKindPostsAnyNot,
			posts:   posts,
			cleanup: cleanup,
		}, true
	}

	if len(posts) == 0 {
		cleanup()
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
			cleanup:  cleanup,
		}, true
	}

	anyPostingFilter, anyPostingCleanup := qv.buildPostsAnyPostingFilter(posts)
	if anyPostingCleanup != nil {
		prevCleanup := cleanup
		cleanup = func() {
			anyPostingCleanup()
			prevCleanup()
		}
	}

	return predicate{
		expr:          e,
		kind:          predicateKindPostsAny,
		iterKind:      predicateIterPostsUnion,
		posts:         posts,
		estCard:       est,
		postingFilter: anyPostingFilter,
		cleanup:       cleanup,
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

func overlayRangeContains(ov fieldOverlay, br overlayRange, idx uint64) bool {
	cur := ov.newCursor(br, false)
	for {
		_, ids, ok := cur.next()
		if !ok {
			return false
		}
		if ids.Contains(idx) {
			return true
		}
	}
}

func overlayUnionRange(ov fieldOverlay, br overlayRange) posting.List {
	var out posting.List
	cur := ov.newCursor(br, false)
	for {
		_, ids, ok := cur.next()
		if !ok {
			break
		}
		ids.OrInto(&out)
	}
	out.Optimize()
	return out
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
	cur      overlayKeyCursor
	curIt    posting.Iterator
	released bool
}

func newOverlayRangeIter(ov fieldOverlay, br overlayRange) *overlayRangeIter {
	return &overlayRangeIter{
		cur: ov.newCursor(br, false),
	}
}

func (it *overlayRangeIter) release() {
	if it.released {
		return
	}
	it.released = true
	if it.curIt != nil {
		it.curIt.Release()
		it.curIt = nil
	}
}

func (it *overlayRangeIter) Release() {
	it.release()
}

func (it *overlayRangeIter) HasNext() bool {
	if it.released {
		return false
	}
	for {
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
		it.curIt = ids.Iter()
	}
}

func (it *overlayRangeIter) Next() uint64 {
	if !it.HasNext() {
		return 0
	}
	return it.curIt.Next()
}

func (qv *queryView[K, V]) buildPredRangeCandidateWithMode(e qx.Expr, fm *field, ov fieldOverlay, allowMaterialize bool) (predicate, bool) {
	if fm.Slice {
		return predicate{}, false
	}

	bound, isSlice, err := qv.exprValueToNormalizedScalarBound(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil {
		return predicate{}, false
	}
	if isSlice {
		return predicate{}, false
	}
	if bound.empty {
		if e.Not {
			return predicate{expr: e, alwaysTrue: true}, true
		}
		return predicate{expr: e, alwaysFalse: true}, true
	}
	cacheKey := ""
	if !bound.full {
		cacheKey = qv.materializedPredCacheKeyForScalar(e.Field, bound.op, bound.key)
	}
	if cacheKey != "" {
		if cached, ok := qv.snap.loadMaterializedPred(cacheKey); ok {
			return materializedRangePredicateWithMode(e, cached), true
		}
	}

	rb := rangeBounds{has: true}
	applyNormalizedScalarBound(&rb, bound)
	br := ov.rangeForBounds(rb)
	if overlayRangeEmpty(br) {
		if e.Not {
			return predicate{expr: e, alwaysTrue: true}, true
		}
		return predicate{expr: e, alwaysFalse: true}, true
	}

	// Numeric-range bucket routing remains useful even when predicate sharing
	// is disabled; the snapshot bucket cache is managed independently.
	if allowMaterialize {
		if out, ok := qv.tryEvalNumericRangeBuckets(e.Field, fm, ov, br); ok {
			if cacheKey != "" {
				qv.tryShareMaterializedPred(cacheKey, &out.ids)
			}
			return materializedRangePredicateWithMode(e, out.ids), true
		}
	}

	bucketCount, est := overlayRangeStats(ov, br)
	if bucketCount == 0 {
		if e.Not {
			return predicate{expr: e, alwaysTrue: true}, true
		}
		return predicate{expr: e, alwaysFalse: true}, true
	}
	totalBuckets := ov.keyCount()
	if bucketCount == totalBuckets {
		if e.Not {
			return predicate{expr: e, alwaysFalse: true}, true
		}
		return predicate{expr: e, alwaysTrue: true}, true
	}
	useComplement := totalBuckets-bucketCount < bucketCount
	probe := newOverlayRangeProbe(ov, br, useComplement)
	var (
		once      sync.Once
		probeOnce sync.Once
		rangeIDs  posting.List
		probeIDs  posting.List

		postingFilter      func(*posting.List) bool
		postingFilterCheap bool
	)
	ensureRangeMaterialized := func() posting.List {
		once.Do(func() {
			rangeIDs = overlayUnionRange(ov, br)
			if cacheKey != "" && !useComplement {
				qv.tryShareMaterializedPred(cacheKey, &rangeIDs)
			}
		})
		return rangeIDs
	}
	ensureProbeMaterialized := func() posting.List {
		if !useComplement {
			return ensureRangeMaterialized()
		}
		probeOnce.Do(func() {
			for i := 0; i < probe.spanCnt; i++ {
				part := overlayUnionRange(ov, probe.spans[i])
				part.OrInto(&probeIDs)
				part.Release()
			}
		})
		return probeIDs
	}
	if filter, cheap, ok := buildOverlayNumericRangePostingFilter(e, fm, probe, ensureProbeMaterialized); ok {
		postingFilter = filter
		postingFilterCheap = cheap
	}
	if postingFilter == nil {
		materializeAfter := rangePostingFilterMaterializeAfterForProbe(bucketCount, est)
		filterCalls := 0
		postingFilter = func(dst *posting.List) bool {
			if dst == nil {
				return false
			}
			if dst.IsEmpty() {
				return true
			}
			filterCalls++
			if filterCalls < materializeAfter {
				return false
			}
			ids := ensureRangeMaterialized()
			if ids.IsEmpty() {
				if e.Not {
					return true
				}
				dst.Clear()
				return true
			}
			if e.Not {
				dst.AndNotInPlace(ids)
			} else {
				dst.AndInPlace(ids)
			}
			return true
		}
	}
	var (
		itMu sync.Mutex
		its  []*overlayRangeIter
	)
	containsInRange := func(idx uint64) bool {
		if bucketCount <= 16 {
			return overlayRangeContains(ov, br, idx)
		}
		return ensureRangeMaterialized().Contains(idx)
	}
	cleanup := func() {
		probeIDs.Release()
		rangeIDs.Release()
		itMu.Lock()
		for _, it := range its {
			if it != nil {
				it.release()
			}
		}
		its = nil
		itMu.Unlock()
	}

	if e.Not {
		return predicate{
			expr: e,
			contains: func(idx uint64) bool {
				return !containsInRange(idx)
			},
			postingFilter:      postingFilter,
			postingFilterCheap: postingFilterCheap,
			cleanup:            cleanup,
		}, true
	}

	return predicate{
		expr: e,
		iter: func() posting.Iterator {
			it := newOverlayRangeIter(ov, br)
			itMu.Lock()
			its = append(its, it)
			itMu.Unlock()
			return it
		},
		contains:           containsInRange,
		estCard:            est,
		postingFilter:      postingFilter,
		postingFilterCheap: postingFilterCheap,
		cleanup:            cleanup,
	}, true
}

func (qv *queryView[K, V]) buildPredMaterializedCandidate(e qx.Expr) (predicate, bool) {
	raw := e
	raw.Not = false
	cacheKey := qv.materializedPredCacheKey(raw)

	var (
		once sync.Once
		ids  posting.List
	)
	load := func() {
		if cacheKey != "" {
			if cached, ok := qv.snap.loadMaterializedPred(cacheKey); ok {
				ids = cached
				return
			}
		}

		b, err := qv.evalSimple(raw)
		if err != nil {
			if cacheKey != "" {
				qv.snap.storeMaterializedPred(cacheKey, posting.List{})
			}
			return
		}
		if b.ids.IsEmpty() {
			b.release()
			if cacheKey != "" {
				qv.snap.storeMaterializedPred(cacheKey, posting.List{})
			}
			return
		}
		ids = b.ids
		if cacheKey != "" {
			qv.tryShareMaterializedPred(cacheKey, &ids)
		}
	}
	cleanup := func() {
		ids.Release()
	}

	est := qv.snapshotUniverseCardinality()
	if est == 0 {
		est = 1
	}

	if e.Not {
		return predicate{
			expr: e,
			contains: func(idx uint64) bool {
				once.Do(load)
				if ids.IsEmpty() {
					return true
				}
				return !ids.Contains(idx)
			},
			estCard: est,
			cleanup: cleanup,
		}, true
	}

	return predicate{
		expr: e,
		iter: func() posting.Iterator {
			once.Do(load)
			if ids.IsEmpty() {
				return emptyIter{}
			}
			return ids.Iter()
		},
		contains: func(idx uint64) bool {
			once.Do(load)
			return ids.Contains(idx)
		},
		estCard: est,
		cleanup: cleanup,
	}, true
}

func (qv *queryView[K, V]) materializedPredCacheEnabled() bool {
	return qv.snap.matPredCacheMaxEntries > 0
}

func (qv *queryView[K, V]) materializedPredCacheKeyForScalar(field string, op qx.Op, key string) string {
	if !qv.materializedPredCacheEnabled() {
		return ""
	}
	return materializedPredCacheKeyFromScalar(field, op, key)
}

func (qv *queryView[K, V]) materializedPredComplementCacheKeyForScalar(field string, op qx.Op, key string) string {
	if !qv.materializedPredCacheEnabled() {
		return ""
	}
	switch op {
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE:
	default:
		return ""
	}
	if field == "" || key == "" {
		return ""
	}
	return field + "\x1f" + "count_range_complement" + "\x1f" +
		strconv.Itoa(int(op)) + "\x1f" + key
}

func (qv *queryView[K, V]) materializedPredCacheKey(e qx.Expr) string {
	switch e.Op {
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
		bound, isSlice, err := qv.exprValueToNormalizedScalarBound(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
		if err != nil || isSlice || bound.empty || bound.full {
			return ""
		}
		return qv.materializedPredCacheKeyForScalar(e.Field, bound.op, bound.key)
	default:
		key, isSlice, isNil, err := qv.exprValueToIdxScalar(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
		if err != nil || isSlice || isNil {
			return ""
		}
		return qv.materializedPredCacheKeyForScalar(e.Field, e.Op, key)
	}
}

func materializedPredCacheKeyFromScalar(field string, op qx.Op, key string) string {
	if field == "" {
		return ""
	}
	switch op {
	case qx.OpSUFFIX, qx.OpCONTAINS, qx.OpPREFIX, qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE:
	default:
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
func (qv *queryView[K, V]) tryShareMaterializedPred(cacheKey string, ids *posting.List) bool {
	if cacheKey == "" || ids == nil {
		return false
	}
	if qv.snap.matPredCacheMaxEntries <= 0 {
		return false
	}
	if ids.IsEmpty() {
		qv.snap.storeMaterializedPred(cacheKey, posting.List{})
		return false
	}
	if qv.snap.loadOrStoreMaterializedPred(cacheKey, ids) {
		return true
	}
	return qv.snap.tryLoadOrStoreMaterializedPredOversized(cacheKey, ids)
}

func resolveRange(s []index, op qx.Op, key string) (start, end int, ok bool) {
	lo := lowerBoundIndex(s, key)

	start = 0
	end = len(s)

	switch op {
	case qx.OpGT:
		start = lo
		if start < len(s) && indexKeyEqualsString(s[start].Key, key) {
			start++
		}
	case qx.OpGTE:
		start = lo
	case qx.OpLT:
		end = lo
	case qx.OpLTE:
		end = upperBoundIndex(s, key)
	case qx.OpPREFIX:
		start = lo
		end = prefixRangeEndIndex(s, key, start)
	default:
		return 0, 0, false
	}

	if start < 0 {
		start = 0
	}
	if end > len(s) {
		end = len(s)
	}
	if start > end {
		start = end
	}
	return start, end, true
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
	cursor := qv.newQueryCursor(out, q.Offset, q.Limit, q.Limit == 0, nil)
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
	if e.Not {
		return nil, false
	}
	switch e.Op {
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
	default:
		return nil, false
	}

	fm := qv.fields[e.Field]
	if fm == nil || fm.Slice {
		return nil, false
	}

	ov := qv.fieldOverlay(e.Field)
	if !ov.hasData() {
		return nil, true
	}

	bound, isSlice, err := qv.exprValueToNormalizedScalarBound(qx.Expr{
		Op:    e.Op,
		Field: e.Field,
		Value: e.Value,
	})
	if err != nil || isSlice || bound.empty {
		return nil, false
	}
	rb := rangeBounds{has: true}
	applyNormalizedScalarBound(&rb, bound)
	br := ov.rangeForBounds(rb)
	if overlayRangeEmpty(br) {
		return nil, true
	}
	if br.baseEnd-br.baseStart < 4 {
		return nil, false
	}
	out, _ := qv.scanOrderLimitWithPredicates(q, ov, br, false, preds, "", trace)
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
	rb, covered, _, ok := qv.collectOrderRangeBounds(field, len(preds), func(i int) qx.Expr {
		return preds[i].expr
	})
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
	rangeLinearContainsMax                  = 16
	rangeMaterializeAfter                   = 64
	rangeHashSetBucketsMin                  = 1024
	rangeHashSetMaxCard                     = 50_000
	rangeBucketCountMaxProbe                = 64
	rangeFastSinglesMinProbe                = 4096
	orderedPredEagerMaterializeMinWindow    = 1024
	orderedPredBroadRangeLazyMinCard        = 65_536
	orderedPredBroadRangeComplementMaxCard  = 512_000
	orderedPredBroadRangeComplementMaxSlots = 65_536
)

func rangeLinearContainsLimit(probeLen int, est uint64) int {
	limit := rangeLinearContainsMax
	if probeLen > 0 {
		if probeLen >= 8_192 {
			limit -= 3
		} else if probeLen >= 2_048 {
			limit -= 2
		} else if probeLen >= 512 {
			limit--
		}
	}

	if probeLen > 0 && est > 0 {
		avgPerBucket := est / uint64(probeLen)
		switch {
		case avgPerBucket <= 2:
			// Sparse probes keep linear path cheap; avoid eager materialization.
			limit += 3
		case avgPerBucket <= 8:
			limit += 2
		case avgPerBucket >= 128:
			limit -= 2
		case avgPerBucket >= 64:
			limit--
		}
	}
	if limit < 8 {
		limit = 8
	}
	if limit > 32 {
		limit = 32
	}
	return limit
}

func rangeMaterializeAfterForProbe(probeLen int, est uint64) int {
	if probeLen <= 0 {
		return rangeMaterializeAfter
	}

	// Scale smoothly with probe width. Around 1k buckets this converges to ~4
	// calls, and for very wide probes drops to 1-2 calls.
	after := (rangeMaterializeAfter * rangeHashSetBucketsMin) / probeLen
	if after < 1 {
		after = 1
	}
	if after > rangeMaterializeAfter {
		after = rangeMaterializeAfter
	}

	if est > 0 {
		avgPerBucket := est / uint64(probeLen)
		switch {
		case avgPerBucket <= 2:
			after = after * 3 / 4
		case avgPerBucket <= 8:
			after = after * 7 / 8
		case avgPerBucket >= 128:
			after = after * 5 / 4
		case avgPerBucket >= 64:
			after = after * 9 / 8
		}
	}

	// Keep very wide probes from burning repeated linear scans.
	if probeLen >= 16_384 && after > 2 {
		after = 2
	}
	if probeLen >= 65_536 && after > 1 {
		after = 1
	}

	if after < 1 {
		after = 1
	}
	if after > rangeMaterializeAfter {
		after = rangeMaterializeAfter
	}
	return after
}

func rangePostingFilterMaterializeAfterForProbe(probeLen int, est uint64) int {
	after := rangeMaterializeAfterForProbe(probeLen, est)
	switch {
	case probeLen >= rangePostingFilterKeepProbeMaxBuckets*4:
		after = 1
	case probeLen >= rangePostingFilterKeepProbeMaxBuckets*2:
		if after > 2 {
			after = 2
		}
	case probeLen > rangePostingFilterKeepProbeMaxBuckets:
		if after > 4 {
			after = 4
		}
	}
	if after < 1 {
		after = 1
	}
	return after
}

func rangeHashSetBucketsMinForProbe(probeLen int, est uint64) int {
	minBuckets := rangeHashSetBucketsMin
	if est > 0 && est <= 6_000 && probeLen >= 16_384 {
		minBuckets = 896
	}
	return max(minBuckets, 128)
}

func rangeHashSetMaxCardForProbe(probeLen int, est uint64) uint64 {
	maxCard := uint64(rangeHashSetMaxCard)
	if probeLen >= 4_096 {
		maxCard = 60_000
	} else if probeLen <= 256 {
		maxCard = 45_000
	}
	return maxCard
}

func rangeBucketCountMaxProbeForShape(probeLen int, est uint64) int {
	maxProbe := rangeBucketCountMaxProbe
	if probeLen <= 16 && est > 0 && est <= 8_192 {
		maxProbe = 96
	}
	return maxProbe
}

func rangeFastSinglesMinProbeForShape(probeLen int, est uint64) int {
	minProbe := rangeFastSinglesMinProbe
	if est > 0 && est <= 8_000 && probeLen >= 16_384 {
		minProbe = 3_072
	}
	return minProbe
}

// tryPlanOrdered attempts the internal plan ordered path and returns false when fast-path preconditions are not satisfied.
func (qv *queryView[K, V]) tryPlanOrdered(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	if q.Limit == 0 {
		return nil, false, nil
	}
	if len(q.Order) > 1 {
		return nil, false, nil
	}

	var leavesBuf [8]qx.Expr
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
		preds, ok := qv.buildPredicatesOrderedWithMode(leaves, o.Field, false, window, true, true)
		if !ok {
			return nil, false, nil
		}
		defer releasePredicates(preds)

		for i := range preds {
			if preds[i].alwaysFalse {
				return nil, true, nil
			}
		}

		if trace != nil {
			trace.setPlan(PlanOrdered)
		}
		out, ok := qv.execPlanOrderedBasic(q, preds, trace)
		if !ok {
			return nil, false, nil
		}
		return out, true, nil
	}

	// keep baseline paths for trivial patterns where ordered strategy usually has no advantage
	if len(leaves) == 1 && !leaves[0].Not {
		switch leaves[0].Op {
		case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
			return nil, false, nil
		}
	}
	allEq := len(leaves) > 0
	for _, e := range leaves {
		if e.Not || e.Op != qx.OpEQ {
			allEq = false
			break
		}
	}
	if allEq {
		return nil, false, nil
	}

	preds, ok := qv.buildPredicates(leaves)
	if !ok {
		return nil, false, nil
	}
	defer releasePredicates(preds)

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

func (p predicate) checkCost() int {
	if p.alwaysFalse {
		return -1
	}

	cost := 5
	switch p.expr.Op {
	case qx.OpEQ:
		cost = 0
	case qx.OpIN, qx.OpHAS, qx.OpHASANY:
		cost = 1
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE:
		cost = 2
	case qx.OpNOOP:
		cost = 0
	case qx.OpPREFIX, qx.OpSUFFIX, qx.OpCONTAINS:
		cost = 4
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
		if ca != cb {
			return ca < cb
		}
		ea := estOf(a)
		eb := estOf(b)
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

func buildPostingFilterActive(dst, active []int, preds []predicate) []int {
	dst = dst[:0]
	for _, pi := range active {
		if preds[pi].supportsPostingFilter() {
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

func releasePredicates(preds []predicate) {
	for i := range preds {
		if preds[i].cleanup != nil {
			preds[i].cleanup()
		}
	}
}

func (qv *queryView[K, V]) buildPredicatesWithMode(leaves []qx.Expr, allowMaterialize bool) ([]predicate, bool) {
	if len(leaves) == 1 && leaves[0].Op == qx.OpNOOP && leaves[0].Not {
		return []predicate{{alwaysFalse: true}}, true
	}

	preds := make([]predicate, 0, len(leaves))
	for _, e := range leaves {
		p, ok := qv.buildPredicateWithMode(e, allowMaterialize)
		if !ok {
			releasePredicates(preds)
			return nil, false
		}
		preds = append(preds, p)
	}
	return preds, true
}

func (qv *queryView[K, V]) buildPredicates(leaves []qx.Expr) ([]predicate, bool) {
	return qv.buildPredicatesWithMode(leaves, true)
}

func isOrderRangeCoveredLeaf(orderField string, e qx.Expr) bool {
	if e.Not || e.Field != orderField {
		return false
	}
	switch e.Op {
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpEQ, qx.OpPREFIX:
		return true
	default:
		return false
	}
}

func (qv *queryView[K, V]) buildPredicatesOrdered(leaves []qx.Expr, orderField string) ([]predicate, bool) {
	return qv.buildPredicatesOrderedWithMode(leaves, orderField, true, 0, true, true)
}

func shouldUseOrderedBroadRangeComplementMaterialization(p predicate, ov fieldOverlay, universe uint64) bool {
	if !ov.hasData() || universe == 0 || p.kind != predicateKindCustom {
		return false
	}
	if p.expr.Not || p.estCard < orderedPredBroadRangeLazyMinCard {
		return false
	}
	switch p.expr.Op {
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE:
	default:
		return false
	}
	threshold := universe - universe/4
	return p.estCard >= threshold
}

func overlayRangeEmpty(br overlayRange) bool {
	return br.baseStart >= br.baseEnd
}

func orderedPredBroadRangeComplementProjectedMisses(needWindow int, estCard, complementEst uint64) uint64 {
	if needWindow <= 0 || estCard == 0 || complementEst == 0 {
		return 0
	}
	need := uint64(needWindow)
	if need > ^uint64(0)/complementEst {
		return ^uint64(0) / estCard
	}
	misses := (need * complementEst) / estCard
	if (need*complementEst)%estCard != 0 {
		misses++
	}
	return misses
}

func (qv *queryView[K, V]) tryMaterializeBroadRangeComplementPredicateForOrdered(p *predicate, ov fieldOverlay, universe uint64, orderedWindow int) bool {
	if p == nil {
		return false
	}
	if !shouldUseOrderedBroadRangeComplementMaterialization(*p, ov, universe) {
		return false
	}

	bound, isSlice, err := qv.exprValueToNormalizedScalarBound(qx.Expr{Op: p.expr.Op, Field: p.expr.Field, Value: p.expr.Value})
	if err != nil || isSlice || bound.empty {
		return false
	}
	cacheKey := ""
	if !bound.full {
		cacheKey = qv.materializedPredComplementCacheKeyForScalar(p.expr.Field, bound.op, bound.key)
	}

	setAlwaysTrue := func() bool {
		if cacheKey != "" {
			qv.snap.storeMaterializedPred(cacheKey, posting.List{})
		}
		p.kind = predicateKindCustom
		p.iterKind = predicateIterNone
		p.posting = posting.List{}
		p.posts = nil
		p.ids = posting.List{}
		p.contains = nil
		p.iter = nil
		p.bucketCount = nil
		p.estCard = 0
		p.alwaysTrue = true
		p.alwaysFalse = false
		return true
	}
	setMaterializedNot := func(ids posting.List) bool {
		if ids.IsEmpty() {
			return setAlwaysTrue()
		}
		p.cleanup = chainPredicateCleanup(p.cleanup, func() {
			ids.Release()
		})
		p.kind = predicateKindMaterializedNot
		p.iterKind = predicateIterNone
		p.posting = posting.List{}
		p.posts = nil
		p.ids = ids
		p.contains = nil
		p.iter = nil
		p.bucketCount = nil
		p.estCard = 0
		p.alwaysTrue = false
		p.alwaysFalse = false
		return true
	}
	if cacheKey != "" {
		if cached, ok := qv.snap.loadMaterializedPred(cacheKey); ok {
			return setMaterializedNot(cached)
		}
	}
	rb := rangeBounds{has: true}
	applyNormalizedScalarBound(&rb, bound)
	br := ov.rangeForBounds(rb)
	before, after := overlayComplementRangeSpans(ov, br)
	nilPosting := qv.nilFieldOverlay(p.expr.Field).lookupPostingRetained(nilIndexEntryKey)

	complementBuckets := 0
	complementEst := uint64(0)
	addRangeStats := func(span overlayRange) {
		if overlayRangeEmpty(span) {
			return
		}
		buckets, est := overlayRangeStats(ov, span)
		complementBuckets += buckets
		if ^uint64(0)-complementEst < est {
			complementEst = ^uint64(0)
		} else {
			complementEst += est
		}
	}
	addRangeStats(before)
	addRangeStats(after)
	if !nilPosting.IsEmpty() {
		complementBuckets++
		if ^uint64(0)-complementEst < nilPosting.Cardinality() {
			complementEst = ^uint64(0)
		} else {
			complementEst += nilPosting.Cardinality()
		}
	}
	if complementEst == 0 {
		return setAlwaysTrue()
	}
	maxComplementSlots := orderedPredBroadRangeComplementMaxSlots
	if complementEst > 0 {
		estSlots := int(min(complementEst, uint64(orderedPredBroadRangeComplementMaxCard)))
		if estSlots > maxComplementSlots {
			maxComplementSlots = estSlots
		}
	}
	if complementEst > orderedPredBroadRangeComplementMaxCard || complementBuckets > maxComplementSlots {
		return false
	}
	if cacheKey == "" {
		projectedMisses := orderedPredBroadRangeComplementProjectedMisses(orderedWindow, p.estCard, complementEst)
		if projectedMisses == 0 {
			return false
		}
		maxUsefulRows := projectedMisses * 8
		minUsefulRows := uint64(orderedWindow)
		if maxUsefulRows < minUsefulRows {
			maxUsefulRows = minUsefulRows
		}
		if complementEst > maxUsefulRows {
			return false
		}
		maxUsefulBuckets := int(projectedMisses * 4)
		if maxUsefulBuckets < orderedWindow {
			maxUsefulBuckets = orderedWindow
		}
		if maxUsefulBuckets < 32 {
			maxUsefulBuckets = 32
		}
		if complementBuckets > maxUsefulBuckets {
			return false
		}
	}

	var ids posting.List
	appendComplement := func(span overlayRange) {
		if overlayRangeEmpty(span) {
			return
		}
		part := overlayUnionRange(ov, span)
		part.OrInto(&ids)
		part.Release()
	}
	appendComplement(before)
	appendComplement(after)
	nilPosting.OrInto(&ids)
	if cacheKey != "" {
		qv.tryShareMaterializedPred(cacheKey, &ids)
	}
	return setMaterializedNot(ids)
}

func shouldEagerMaterializeOrderedPredicate(e qx.Expr, orderField string, orderedWindow int) bool {
	if orderedWindow < orderedPredEagerMaterializeMinWindow {
		return false
	}
	if e.Not || e.Field == "" || e.Field == orderField {
		return false
	}
	switch e.Op {
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE:
		return true
	default:
		return false
	}
}

func (qv *queryView[K, V]) buildPredicatesOrderedWithMode(leaves []qx.Expr, orderField string, allowMaterialize bool, orderedWindow int, coverOrderRange bool, allowOrderedEagerMaterialize bool) ([]predicate, bool) {
	if len(leaves) == 1 && leaves[0].Op == qx.OpNOOP && leaves[0].Not {
		return []predicate{{alwaysFalse: true}}, true
	}

	preds := make([]predicate, 0, len(leaves))
	universe := uint64(0)
	if !allowMaterialize {
		universe = qv.snapshotUniverseCardinality()
	}
	for _, e := range leaves {
		orderRangeDeferred := !coverOrderRange && isOrderRangeCoveredLeaf(orderField, e)
		if coverOrderRange && isOrderRangeCoveredLeaf(orderField, e) {
			// Keep conversion/typing validation, but avoid expensive predicate
			// materialization for leaves that are fully covered by order range.
			bound, isSlice, err := qv.exprValueToNormalizedScalarBound(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
			if err != nil || isSlice || bound.empty {
				releasePredicates(preds)
				return nil, false
			}
			preds = append(preds, predicate{
				expr:    e,
				covered: true,
			})
			continue
		}

		predAllowMaterialize := allowMaterialize
		if !predAllowMaterialize && allowOrderedEagerMaterialize &&
			!orderRangeDeferred &&
			shouldEagerMaterializeOrderedPredicate(e, orderField, orderedWindow) {
			predAllowMaterialize = true
		}
		var ov fieldOverlay
		if !allowMaterialize {
			ov = qv.fieldOverlay(e.Field)
		}

		p, ok := qv.buildPredicateWithMode(e, predAllowMaterialize)
		if !ok {
			releasePredicates(preds)
			return nil, false
		}
		if !predAllowMaterialize && !orderRangeDeferred {
			qv.tryMaterializeBroadRangeComplementPredicateForOrdered(&p, ov, universe, orderedWindow)
		}
		preds = append(preds, p)
	}
	return preds, true
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

	switch e.Op {
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
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
	default:
		p, ok := qv.buildPredicateCandidate(e)
		if !ok {
			return predicate{}, false
		}
		return p, true
	}
}

func (qv *queryView[K, V]) buildPredRangeWithMode(e qx.Expr, fm *field, slice *[]index, allowMaterialize bool) (predicate, bool) {
	if fm.Slice {
		return predicate{}, false
	}

	bound, isSlice, err := qv.exprValueToNormalizedScalarBound(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil {
		return predicate{}, false
	}
	if isSlice {
		return predicate{}, false
	}
	if bound.empty {
		if e.Not {
			return predicate{expr: e, alwaysTrue: true}, true
		}
		return predicate{expr: e, alwaysFalse: true}, true
	}
	cacheKey := ""
	if !bound.full {
		cacheKey = qv.materializedPredCacheKeyForScalar(e.Field, bound.op, bound.key)
	}
	if cacheKey != "" {
		if cached, ok := qv.snap.loadMaterializedPred(cacheKey); ok {
			return materializedRangePredicateWithMode(e, cached), true
		}
	}

	s := *slice
	ov := fieldOverlay{base: s}
	rb := rangeBounds{has: true}
	applyNormalizedScalarBound(&rb, bound)
	start, end := applyBoundsToIndexRange(s, rb)

	if start >= end {
		if e.Not {
			return predicate{expr: e, alwaysTrue: true}, true
		}
		return predicate{expr: e, alwaysFalse: true}, true
	}

	if start == 0 && end == len(s) {
		if e.Not {
			return predicate{expr: e, alwaysFalse: true}, true
		}
		return predicate{expr: e, alwaysTrue: true}, true
	}

	// See buildPredRangeCandidateWithMode: keep numeric bucket routing available
	// independently from predicate cache sharing.
	if allowMaterialize {
		if out, ok := qv.tryEvalNumericRangeBuckets(e.Field, fm, ov, overlayRange{
			baseStart: start,
			baseEnd:   end,
		}); ok {
			if cacheKey != "" {
				qv.tryShareMaterializedPred(cacheKey, &out.ids)
			}
			return materializedRangePredicateWithMode(e, out.ids), true
		}
	}

	inBuckets := end - start
	outBuckets := len(s) - inBuckets
	useComplement := outBuckets < inBuckets

	var est uint64
	if exact, ok := qv.tryCountSnapshotNumericRange(e.Field, fm, ov, start, end); ok {
		est = exact
	} else if inBuckets == 1 {
		if ids := s[start].IDs; !ids.IsEmpty() {
			est = ids.Cardinality()
		}
	} else {
		// Fallback estimate is only for lead selection;
		// keep it cheap when exact numeric stats are not available.
		ix0 := start
		ix1 := start + inBuckets/2
		ix2 := end - 1
		var sum uint64
		var n uint64

		if ids := s[ix0].IDs; !ids.IsEmpty() {
			sum += ids.Cardinality()
			n++
		}
		if ix1 != ix0 && ix1 != ix2 {
			if ids := s[ix1].IDs; !ids.IsEmpty() {
				sum += ids.Cardinality()
				n++
			}
		}
		if ix2 != ix0 {
			if ids := s[ix2].IDs; !ids.IsEmpty() {
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

	probe := newBaseRangeProbe(s, start, end, useComplement)
	var onMaterialized func(*posting.List) bool
	if cacheKey != "" && !useComplement {
		onMaterialized = func(ids *posting.List) bool { return qv.tryShareMaterializedPred(cacheKey, ids) }
	}
	containsInRange, materializeRange, cleanupRange := qv.buildRangeContains(probe, onMaterialized)
	postingFilter, postingFilterCheap, _ := qv.buildBaseNumericRangePostingFilter(e, fm, probe, materializeRange)
	bucketCountMaxProbe := rangeBucketCountMaxProbeForShape(probe.probeLen, est)
	cleanup := func() {
		if cleanupRange != nil {
			cleanupRange()
		}
	}

	countInRange := func(bucket posting.List) (uint64, bool) {
		return probe.countBucket(bucket, bucketCountMaxProbe)
	}

	if e.Not {
		return predicate{
			expr: e,
			contains: func(idx uint64) bool {
				return !containsInRange(idx)
			},
			bucketCount: func(bucket posting.List) (uint64, bool) {
				in, ok := countInRange(bucket)
				if !ok {
					return 0, false
				}
				bc := bucket.Cardinality()
				if in >= bc {
					return 0, true
				}
				return bc - in, true
			},
			postingFilter:      postingFilter,
			postingFilterCheap: postingFilterCheap,
			cleanup:            cleanup,
		}, true
	}

	return predicate{
		expr: e,
		iter: func() posting.Iterator {
			return newRangeIter(s, start, end)
		},
		contains: containsInRange,
		estCard:  est,
		bucketCount: func(bucket posting.List) (uint64, bool) {
			return countInRange(bucket)
		},
		postingFilter:      postingFilter,
		postingFilterCheap: postingFilterCheap,
		cleanup:            cleanup,
	}, true
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
	hit := false
	p.forEachPosting(func(ids posting.List) bool {
		if ids.Contains(idx) {
			hit = true
			return false
		}
		return true
	})
	return hit
}

func (p baseRangeProbe) countBucket(bucket posting.List, maxProbe int) (uint64, bool) {
	if bucket.IsEmpty() {
		return 0, true
	}
	if p.probeLen > maxProbe {
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

func (p baseRangeProbe) singletonHeavy(minProbe int) bool {
	if p.probeLen < minProbe || p.probeLen == 0 {
		return false
	}
	return p.singletonCount*4 >= p.probeLen*3
}

const (
	rangePostingFilterKeepProbeMaxBuckets = 24
	rangePostingFilterKeepRowsMaxCard     = 8_192
)

func shouldUseNumericRangePostingFilter(e qx.Expr, fm *field) bool {
	if fm == nil || fm.Slice || !isNumericScalarKind(fm.Kind) {
		return false
	}
	switch e.Op {
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE:
		return true
	default:
		return false
	}
}

func applyRangeProbePostingFilter(
	dst *posting.List,
	keepProbeHits bool,
	probeLen int,
	probeContains func(uint64) bool,
	forEachPosting func(func(posting.List) bool) bool,
) bool {
	if dst == nil {
		return false
	}
	if dst.IsEmpty() {
		return true
	}
	if probeLen == 0 {
		if keepProbeHits {
			dst.Clear()
		}
		return true
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
			dst.AndNotInPlace(ids)
			return !dst.IsEmpty()
		})
		return true
	}
	if dst.Cardinality() <= uint64(maxRowsForContains) {
		var work posting.List
		dst.ForEach(func(idx uint64) bool {
			if probeContains(idx) {
				work.Add(idx)
			}
			return true
		})
		dst.Release()
		*dst = work
		return true
	}
	if probeLen <= rangePostingFilterKeepProbeMaxBuckets {
		var work posting.List
		forEachPosting(func(ids posting.List) bool {
			ids.ForEachIntersecting(*dst, func(idx uint64) bool {
				work.Add(idx)
				return false
			})
			return true
		})
		dst.Release()
		*dst = work
		return true
	}
	if probeLen > rangePostingFilterKeepProbeMaxBuckets && dst.Cardinality() > rangePostingFilterKeepRowsMaxCard {
		return false
	}

	var work posting.List
	dst.ForEach(func(idx uint64) bool {
		if probeContains(idx) {
			work.Add(idx)
		}
		return true
	})
	dst.Release()
	*dst = work
	return true
}

func (qv *queryView[K, V]) buildBaseNumericRangePostingFilter(
	e qx.Expr,
	fm *field,
	probe baseRangeProbe,
	materialize func() posting.List,
) (func(*posting.List) bool, bool, bool) {
	if !shouldUseNumericRangePostingFilter(e, fm) || probe.probeLen == 0 {
		return nil, false, false
	}
	keepProbeHits := probe.useComplement == e.Not
	materializeAfter := rangePostingFilterMaterializeAfterForProbe(probe.probeLen, probe.probeEst)
	if probe.probeLen > rangePostingFilterKeepProbeMaxBuckets && probe.probeEst > rangePostingFilterKeepRowsMaxCard {
		materializeAfter = 1
	}
	filterCalls := 0
	return func(dst *posting.List) bool {
		if dst == nil {
			return false
		}
		if dst.IsEmpty() {
			return true
		}
		filterCalls++
		forceMaterialize := materialize != nil &&
			filterCalls >= materializeAfter &&
			probe.probeLen > rangePostingFilterKeepProbeMaxBuckets
		if !forceMaterialize {
			return applyRangeProbePostingFilter(dst, keepProbeHits, probe.probeLen, probe.linearContains, probe.forEachPosting)
		}
		if applyRangeProbePostingFilter(dst, keepProbeHits, probe.probeLen, probe.linearContains, probe.forEachPosting) {
			return true
		}
		ids := materialize()
		if ids.IsEmpty() {
			if keepProbeHits {
				dst.Clear()
			}
			return true
		}
		if keepProbeHits {
			dst.AndInPlace(ids)
		} else {
			dst.AndNotInPlace(ids)
		}
		return true
	}, !keepProbeHits, true
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
	hit := false
	p.forEachPosting(func(ids posting.List) bool {
		if ids.Contains(idx) {
			hit = true
			return false
		}
		return true
	})
	return hit
}

func buildOverlayNumericRangePostingFilter(
	e qx.Expr,
	fm *field,
	probe overlayRangeProbe,
	materialize func() posting.List,
) (func(*posting.List) bool, bool, bool) {
	if !shouldUseNumericRangePostingFilter(e, fm) {
		return nil, false, false
	}
	totalBuckets := probe.ov.keyCount()
	inBuckets := 0
	for i := 0; i < probe.spanCnt; i++ {
		inBuckets += probe.spans[i].baseEnd - probe.spans[i].baseStart
	}
	if probe.useComplement {
		inBuckets = totalBuckets - inBuckets
	}
	if totalBuckets <= 0 || inBuckets <= 0 || inBuckets >= totalBuckets {
		return nil, false, false
	}
	if probe.probeLen == 0 {
		return nil, false, false
	}
	keepProbeHits := probe.useComplement == e.Not
	materializeAfter := rangePostingFilterMaterializeAfterForProbe(probe.probeLen, probe.probeEst)
	filterCalls := 0
	return func(dst *posting.List) bool {
		if dst == nil {
			return false
		}
		if dst.IsEmpty() {
			return true
		}
		filterCalls++
		forceMaterialize := materialize != nil &&
			filterCalls >= materializeAfter &&
			probe.probeLen > rangePostingFilterKeepProbeMaxBuckets
		if !forceMaterialize {
			return applyRangeProbePostingFilter(dst, keepProbeHits, probe.probeLen, probe.linearContains, probe.forEachPosting)
		}
		if applyRangeProbePostingFilter(dst, keepProbeHits, probe.probeLen, probe.linearContains, probe.forEachPosting) {
			return true
		}
		ids := materialize()
		if ids.IsEmpty() {
			if keepProbeHits {
				dst.Clear()
			}
			return true
		}
		if keepProbeHits {
			dst.AndInPlace(ids)
		} else {
			dst.AndNotInPlace(ids)
		}
		return true
	}, !keepProbeHits, true
}

func materializedRangePredicateWithMode(e qx.Expr, ids posting.List) predicate {
	if ids.IsEmpty() {
		if e.Not {
			return predicate{expr: e, alwaysTrue: true}
		}
		return predicate{expr: e, alwaysFalse: true}
	}

	containsInRange := func(idx uint64) bool {
		return ids.Contains(idx)
	}
	countInRange := func(bucket posting.List) (uint64, bool) {
		return bucket.AndCardinality(ids), true
	}
	cleanup := func() {
		ids.Release()
	}

	if e.Not {
		return predicate{
			expr: e,
			contains: func(idx uint64) bool {
				return !containsInRange(idx)
			},
			bucketCount: func(bucket posting.List) (uint64, bool) {
				in, _ := countInRange(bucket)
				bc := bucket.Cardinality()
				if in >= bc {
					return 0, true
				}
				return bc - in, true
			},
			cleanup: cleanup,
		}
	}

	return predicate{
		expr: e,
		iter: func() posting.Iterator {
			return ids.Iter()
		},
		contains: containsInRange,
		estCard:  ids.Cardinality(),
		bucketCount: func(bucket posting.List) (uint64, bool) {
			return countInRange(bucket)
		},
		cleanup: cleanup,
	}
}

// buildRangeContains builds a membership checker for a range probe set.
//
// The checker starts cheap (linear Contains over probe buckets) and upgrades
// to hash/posting materialization only after enough repeated calls.
func (qv *queryView[K, V]) buildRangeContains(
	probe baseRangeProbe,
	onMaterialized func(*posting.List) bool,
) (func(uint64) bool, func() posting.List, func()) {
	if probe.probeLen == 0 {
		if probe.useComplement {
			return func(uint64) bool { return true }, func() posting.List { return posting.List{} }, nil
		}
		return func(uint64) bool { return false }, func() posting.List { return posting.List{} }, nil
	}
	linearContainsMax := rangeLinearContainsLimit(probe.probeLen, probe.probeEst)
	materializeAfter := rangeMaterializeAfterForProbe(probe.probeLen, probe.probeEst)
	hashSetBucketsMin := rangeHashSetBucketsMinForProbe(probe.probeLen, probe.probeEst)
	hashSetMaxCard := rangeHashSetMaxCardForProbe(probe.probeLen, probe.probeEst)
	fastSinglesMinProbe := rangeFastSinglesMinProbeForShape(probe.probeLen, probe.probeEst)

	linearContains := func(idx uint64) bool {
		return probe.linearContains(idx)
	}

	project := func(hit bool) bool {
		// Invert is projected at the very end so all internal heuristics operate
		// on the same positive-hit semantics.
		if probe.useComplement {
			return !hit
		}
		return hit
	}

	if probe.probeLen <= linearContainsMax {
		return func(idx uint64) bool {
			return project(linearContains(idx))
		}, func() posting.List { return posting.List{} }, nil
	}

	var (
		calls       int
		set         *u64set
		ids         posting.List
		idsReadonly bool
	)

	materializePosting := func() posting.List {
		if !ids.IsEmpty() {
			return ids
		}
		if probe.singletonHeavy(fastSinglesMinProbe) {
			ids = materializeBaseRangeProbeFast(probe)
		} else {
			ids = qv.materializeBaseRangeProbeUnion(probe)
		}
		if !ids.IsEmpty() && onMaterialized != nil {
			idsReadonly = onMaterialized(&ids)
		}
		return ids
	}

	contains := func(idx uint64) bool {
		if set != nil {
			return project(set.Has(idx))
		}
		if !ids.IsEmpty() {
			return project(ids.Contains(idx))
		}

		calls++
		if calls >= materializeAfter {
			// Hash-set representation wins when estimated cardinality is bounded;
			// it avoids the high constant factor of huge roaring unions.
			if probe.probeLen >= hashSetBucketsMin && probe.probeEst > 0 && probe.probeEst <= hashSetMaxCard {
				capHint := int(probe.probeEst)
				if capHint < 64 {
					capHint = 64
				}
				hs := newU64Set(capHint)
				probe.forEachPosting(func(b posting.List) bool {
					b.ForEach(func(v uint64) bool {
						hs.Add(v)
						return true
					})
					return true
				})
				set = &hs
				return project(set.Has(idx))
			}

			if probe.probeLen > linearContainsMax {
				return project(materializePosting().Contains(idx))
			}
		}

		return project(linearContains(idx))
	}

	cleanup := func() {
		if set != nil {
			releaseU64Set(set)
			set = nil
		}
		if !idsReadonly {
			ids.Release()
		}
	}

	return contains, materializePosting, cleanup
}

func materializeBaseRangeProbeFast(probe baseRangeProbe) posting.List {
	var out posting.List
	singles := getSingleIDsBuf()
	defer releaseSingleIDs(singles)

	flushSingles := func() {
		if len(singles.values) == 0 {
			return
		}
		slices.Sort(singles.values)
		out.AddMany(singles.values)
		singles.values = singles.values[:0]
	}

	probe.forEachPosting(func(p posting.List) bool {
		if p.Cardinality() == 1 {
			id, _ := p.Minimum()
			singles.values = append(singles.values, id)
			if len(singles.values) == cap(singles.values) {
				flushSingles()
			}
			return true
		}

		flushSingles()
		p.OrInto(&out)
		return true
	})
	flushSingles()
	out.Optimize()
	return out
}

func (qv *queryView[K, V]) materializeBaseRangeProbeUnion(probe baseRangeProbe) posting.List {
	posts := make([]posting.List, 0, probe.probeLen)
	probe.forEachPosting(func(p posting.List) bool {
		posts = append(posts, p)
		return true
	})
	return qv.unionPostings(posts)
}

func isSingletonHeavyProbe(probe []posting.List, minProbe int, sampleN int) bool {
	if len(probe) < minProbe {
		return false
	}
	if sampleN <= 0 {
		return false
	}
	step := len(probe) / sampleN
	if step < 1 {
		step = 1
	}
	nonEmpty := 0
	singletons := 0
	for i := 0; i < len(probe) && nonEmpty < sampleN; i += step {
		p := probe[i]
		if p.IsEmpty() {
			continue
		}
		nonEmpty++
		if p.Cardinality() == 1 {
			singletons++
		}
	}
	if nonEmpty == 0 {
		return false
	}
	// 75%+ singleton buckets are enough to justify fast path.
	return singletons*4 >= nonEmpty*3
}

func materializeProbeFast(probe []posting.List) posting.List {
	var out posting.List
	singles := getSingleIDsBuf()
	defer releaseSingleIDs(singles)

	flushSingles := func() {
		if len(singles.values) == 0 {
			return
		}
		slices.Sort(singles.values)
		out.AddMany(singles.values)
		singles.values = singles.values[:0]
	}

	for _, p := range probe {
		if p.IsEmpty() {
			continue
		}
		if id, ok := p.TrySingle(); ok {
			singles.values = append(singles.values, id)
			if len(singles.values) == cap(singles.values) {
				flushSingles()
			}
			continue
		}

		flushSingles()
		p.OrInto(&out)
	}
	flushSingles()
	out.Optimize()
	return out
}

func (qv *queryView[K, V]) materializeProbeUnion(probe []posting.List) posting.List {
	return qv.unionPostings(probe)
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
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
		return 1.9
	default:
		return 2.5
	}
}

func orderedPredicateIsRangeLike(p predicate) bool {
	switch p.expr.Op {
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
		return true
	default:
		return false
	}
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
	exactChecks := exactChecksBuf.values[:0]
	defer func() {
		exactChecksBuf.values = exactChecks
		releaseIntSliceBuf(exactChecksBuf)
	}()

	residualChecksBuf := getIntSliceBuf(len(active))
	residualChecks := residualChecksBuf.values[:0]
	defer func() {
		residualChecksBuf.values = residualChecks
		releaseIntSliceBuf(residualChecksBuf)
	}()
	deferredChecksBuf := getIntSliceBuf(len(active))
	deferredChecks := deferredChecksBuf.values[:0]
	defer func() {
		deferredChecksBuf.values = deferredChecks
		releaseIntSliceBuf(deferredChecksBuf)
	}()

	orderedExactPreferred := false
	for _, pi := range active {
		if pi == leadIdx && !leadNeedsCheck {
			continue
		}
		deferredChecks = append(deferredChecks, pi)
		if preds[pi].supportsExactBucketPostingFilter() {
			exactChecks = append(exactChecks, pi)
			if preds[pi].prefersExactBucketPostingFilter() {
				orderedExactPreferred = true
			}
			continue
		}
		residualChecks = append(residualChecks, pi)
	}
	useExactPosting := len(exactChecks) > 0 && (len(residualChecks) == 0 || orderedExactPreferred)

	var candidates posting.List

	leadExamined := uint64(0)
	if useExactPosting {
		for leadIt.HasNext() {
			leadExamined++
			if leadExamined > leadBudget {
				return nil, false
			}
			if trace != nil {
				trace.addExamined(1)
			}
			candidates.Add(leadIt.Next())
		}
	} else {
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

			candidates.Add(idx)
			if candidates.Cardinality() > candidateCap {
				return nil, false
			}
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
	defer exactWork.Release()
	if useExactPosting {
		exactApplied := false
		mode, exactIDs, _ := plannerFilterPostingByChecks(preds, exactChecks, candidates, &exactWork, true)
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
		if exactApplied && len(residualChecks) > 0 {
			var filtered posting.List
			candidateIDs.ForEach(func(idx uint64) bool {
				pass := true
				for _, pi := range residualChecks {
					p := preds[pi]
					if !p.hasContains() || !p.matches(idx) {
						pass = false
						break
					}
				}
				if pass {
					filtered.Add(idx)
				}
				return true
			})
			if filtered.IsEmpty() {
				if trace != nil {
					trace.setEarlyStopReason("no_candidates")
				}
				return nil, true
			}
			candidateIDs = filtered
		}
		if exactApplied {
			deferredChecks = deferredChecks[:0]
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
	if len(deferredChecks) == 1 {
		singleDeferred = deferredChecks[0]
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
				for _, pi := range deferredChecks {
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
	cursor := qv.newQueryCursor(out, q.Offset, q.Limit, false, nil)

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
	cursor := qv.newQueryCursor(out, q.Offset, q.Limit, false, nil)

	exactActiveBuf := getIntSliceBuf(len(active))
	exactActive := buildExactBucketPostingFilterActive(exactActiveBuf.values, active, preds)
	defer func() {
		exactActiveBuf.values = exactActive
		releaseIntSliceBuf(exactActiveBuf)
	}()
	exactOnly := len(active) > 0 && len(active) == len(exactActive)
	residualActiveBuf := getIntSliceBuf(len(active))
	residualActive := residualActiveBuf.values[:0]
	defer func() {
		residualActiveBuf.values = residualActive
		releaseIntSliceBuf(residualActiveBuf)
	}()
	if len(exactActive) > 0 && len(exactActive) < len(active) {
		for _, pi := range active {
			if countIndexSliceContains(exactActive, pi) {
				continue
			}
			residualActive = append(residualActive, pi)
		}
	}

	singleActive := -1
	if len(active) == 1 {
		singleActive = active[0]
	}

	var exactWork posting.List
	defer exactWork.Release()

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
			mode, exactIDs, _ := plannerFilterPostingByChecks(preds, exactActive, bucket, &exactWork, allowExact)
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

		switch e.Op {
		case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpEQ:
			bound, isSlice, err := qv.exprValueToNormalizedScalarBound(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
			if err != nil || isSlice {
				return rangeBounds{}, nil, false, false
			}
			applyNormalizedScalarBound(&rb, bound)
			markCovered(i)
			has = true
		case qx.OpPREFIX:
			bound, isSlice, err := qv.exprValueToNormalizedScalarBound(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
			if err != nil || isSlice || bound.empty || bound.full {
				return rangeBounds{}, nil, false, false
			}
			rb.applyPrefix(bound.key)
			markCovered(i)
			has = true
		}
	}

	return rb, covered, has, true
}
