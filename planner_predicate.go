package rbi

import (
	"math"
	"strconv"
	"sync"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/roaring64"
)

type predicate struct {
	expr qx.Expr

	contains func(uint64) bool
	iter     func() roaringIter
	estCard  uint64

	kind     predicateKind
	iterKind predicateIterKind
	posting  postingList
	posts    []postingList
	bm       *roaring64.Bitmap

	alwaysTrue  bool
	alwaysFalse bool
	covered     bool

	// exact bucket match count when known; ok=false means unknown.
	bucketCount func(*roaring64.Bitmap) (uint64, bool)
	// exact bitmap application when available without full predicate
	// materialization.
	bitmapFilter      func(*roaring64.Bitmap) bool
	bitmapFilterCheap bool

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
	predicateKindBitmap
	predicateKindBitmapNot
)

type predicateIterKind uint8

const (
	predicateIterNone predicateIterKind = iota
	predicateIterPosting
	predicateIterPostsConcat
	predicateIterPostsUnion
	predicateIterBitmap
)

const predicatePostsAnyNotExactBitmapMaxTerms = 2

func (p predicate) hasContains() bool {
	switch p.kind {
	case predicateKindPosting,
		predicateKindPostingNot,
		predicateKindPostsAny,
		predicateKindPostsAnyNot,
		predicateKindPostsAll,
		predicateKindPostsAllNot,
		predicateKindBitmap,
		predicateKindBitmapNot:
		return true
	default:
		return p.contains != nil
	}
}

func (p predicate) hasIter() bool {
	switch p.iterKind {
	case predicateIterPosting, predicateIterPostsConcat, predicateIterPostsUnion, predicateIterBitmap:
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

func (p predicate) supportsBitmapFilter() bool {
	if p.alwaysTrue || p.alwaysFalse || p.covered {
		return true
	}
	switch p.kind {
	case predicateKindPosting,
		predicateKindPostingNot,
		predicateKindPostsAll,
		predicateKindPostsAnyNot,
		predicateKindBitmap,
		predicateKindBitmapNot:
		if p.kind == predicateKindPostsAnyNot && len(p.posts) > predicatePostsAnyNotExactBitmapMaxTerms {
			return false
		}
		return true
	default:
		return p.bitmapFilter != nil
	}
}

func (p predicate) newIter() roaringIter {
	switch p.iterKind {
	case predicateIterPosting:
		return p.posting.Iter()
	case predicateIterPostsConcat:
		return newPostingConcatIter(p.posts)
	case predicateIterPostsUnion:
		return newPostingUnionIter(p.posts)
	case predicateIterBitmap:
		if p.bm == nil {
			return nil
		}
		return p.bm.Iterator()
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
	case predicateKindBitmap:
		return p.bm != nil && p.bm.Contains(idx)
	case predicateKindBitmapNot:
		return p.bm == nil || !p.bm.Contains(idx)
	default:
		if p.contains == nil {
			return false
		}
		return p.contains(idx)
	}
}

func (p predicate) countBucket(bucket *roaring64.Bitmap) (uint64, bool) {
	switch p.kind {
	case predicateKindPosting:
		if bucket == nil || bucket.IsEmpty() {
			return 0, true
		}
		return p.posting.AndCardinalityBitmap(bucket), true
	case predicateKindPostingNot:
		if bucket == nil || bucket.IsEmpty() {
			return 0, true
		}
		bc := bucket.GetCardinality()
		hit := p.posting.AndCardinalityBitmap(bucket)
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
	case predicateKindBitmap:
		if bucket == nil || bucket.IsEmpty() || p.bm == nil {
			return 0, true
		}
		return p.bm.AndCardinality(bucket), true
	case predicateKindBitmapNot:
		if bucket == nil || bucket.IsEmpty() {
			return 0, true
		}
		bc := bucket.GetCardinality()
		if p.bm == nil {
			return bc, true
		}
		hit := p.bm.AndCardinality(bucket)
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

// applyToBitmap intersects (or subtracts for NOT variants) predicate matches
// with dst in place. Returns false when exact bitmap application is not
// available for this predicate shape.
func (p predicate) applyToBitmap(dst *roaring64.Bitmap) bool {
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
		if p.posting.IsEmpty() {
			dst.Clear()
			return true
		}
		if p.posting.isSingleton() {
			if !dst.Contains(p.posting.single) {
				dst.Clear()
				return true
			}
			// Intersection with singleton is the singleton itself.
			dst.Clear()
			dst.Add(p.posting.single)
			return true
		}
		if bm := p.posting.bitmap(); bm != nil {
			dst.And(bm)
			return true
		}
		dst.Clear()
		return true

	case predicateKindPostingNot:
		p.posting.AndNotFrom(dst)
		return true

	case predicateKindPostsAnyNot:
		if len(p.posts) > predicatePostsAnyNotExactBitmapMaxTerms {
			return false
		}
		for _, ids := range p.posts {
			ids.AndNotFrom(dst)
			if dst.IsEmpty() {
				return true
			}
		}
		return true

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
			if ids.isSingleton() {
				if !dst.Contains(ids.single) {
					dst.Clear()
					return true
				}
				dst.Clear()
				dst.Add(ids.single)
			} else if bm := ids.bitmap(); bm != nil {
				dst.And(bm)
			} else {
				dst.Clear()
				return true
			}
			if dst.IsEmpty() {
				return true
			}
		}
		return true

	case predicateKindBitmap:
		if p.bm == nil {
			dst.Clear()
			return true
		}
		dst.And(p.bm)
		return true

	case predicateKindBitmapNot:
		if p.bm != nil {
			dst.AndNot(p.bm)
		}
		return true
	}

	if p.bitmapFilter != nil {
		return p.bitmapFilter(dst)
	}
	return false
}

func countBucketPostsAny(posts []postingList, bucket *roaring64.Bitmap) (uint64, bool) {
	if bucket == nil || bucket.IsEmpty() {
		return 0, true
	}
	if len(posts) == 0 {
		return 0, true
	}
	if len(posts) == 1 {
		return posts[0].AndCardinalityBitmap(bucket), true
	}
	for _, ids := range posts {
		if ids.IntersectsBitmap(bucket) {
			return 0, false
		}
	}
	return 0, true
}

func countBucketPostsAnyNot(posts []postingList, bucket *roaring64.Bitmap) (uint64, bool) {
	if bucket == nil || bucket.IsEmpty() {
		return 0, true
	}
	bc := bucket.GetCardinality()
	if len(posts) == 0 {
		return bc, true
	}
	if len(posts) == 1 {
		hit := posts[0].AndCardinalityBitmap(bucket)
		if hit >= bc {
			return 0, true
		}
		return bc - hit, true
	}
	for _, ids := range posts {
		if ids.IntersectsBitmap(bucket) {
			return 0, false
		}
	}
	return bc, true
}

func countBucketPostsAll(posts []postingList, bucket *roaring64.Bitmap) (uint64, bool) {
	if bucket == nil || bucket.IsEmpty() {
		return 0, true
	}
	if len(posts) == 0 {
		return 0, true
	}
	if len(posts) == 1 {
		return posts[0].AndCardinalityBitmap(bucket), true
	}
	for _, ids := range posts {
		if !ids.IntersectsBitmap(bucket) {
			return 0, true
		}
	}
	return 0, false
}

func countBucketPostsAllNot(posts []postingList, bucket *roaring64.Bitmap) (uint64, bool) {
	if bucket == nil || bucket.IsEmpty() {
		return 0, true
	}
	bc := bucket.GetCardinality()
	if len(posts) == 0 {
		return bc, true
	}
	if len(posts) == 1 {
		hit := posts[0].AndCardinalityBitmap(bucket)
		if hit >= bc {
			return 0, true
		}
		return bc - hit, true
	}
	for _, ids := range posts {
		if !ids.IntersectsBitmap(bucket) {
			return bc, true
		}
	}
	return 0, false
}

// tryPlanCandidate attempts the internal plan candidate path and returns ok=false when fast-path preconditions are not satisfied.
func (db *DB[K, V]) tryPlanCandidate(q *qx.QX, trace *queryTrace) ([]K, bool, error) {

	// candidate planner is focused on the latency-critical paged path.
	if q.Limit == 0 {
		return nil, false, nil
	}
	if len(q.Order) > 1 {
		return nil, false, nil
	}

	leaves, ok := collectAndLeaves(q.Expr)
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
		if !db.shouldUseCandidateOrder(o, leaves) {
			return nil, false, nil
		}

		preds, ok := db.buildPredicatesCandidate(leaves)
		if !ok {
			return nil, false, nil
		}
		defer releasePredicatesCandidate(preds)

		for i := range preds {
			if preds[i].alwaysFalse {
				return nil, true, nil
			}
		}

		if trace != nil {
			trace.setPlan(PlanCandidateOrder)
		}
		return db.execPlanCandidateOrderBasic(q, preds), true, nil
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

	preds, ok := db.buildPredicatesCandidate(leaves)
	if !ok {
		return nil, false, nil
	}
	defer releasePredicatesCandidate(preds)

	for i := range preds {
		if preds[i].alwaysFalse {
			return nil, true, nil
		}
	}

	if trace != nil {
		trace.setPlan(PlanCandidateNoOrder)
	}
	return db.execPlanLeadScanNoOrder(q, preds, nil), true, nil
}

func (db *DB[K, V]) shouldUseCandidateOrder(o qx.Order, leaves []qx.Expr) bool {
	fm := db.fields[o.Field]
	if fm == nil || fm.Slice {
		return false
	}
	if !db.fieldOverlay(o.Field).hasData() {
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
		if ef := db.fields[e.Field]; ef == nil || ef.Slice {
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

func collectAndLeaves(e qx.Expr) ([]qx.Expr, bool) {
	n, ok := countCollectAndLeaves(e)
	if !ok {
		return nil, false
	}
	if n == 0 {
		return nil, true
	}
	out := make([]qx.Expr, 0, n)
	out, ok = appendCollectAndLeaves(out, e)
	if !ok {
		return nil, false
	}
	return out, true
}

func countCollectAndLeaves(e qx.Expr) (int, bool) {
	switch e.Op {
	case qx.OpNOOP:
		if e.Not {
			return 1, true
		}
		return 0, true
	case qx.OpAND:
		if e.Not {
			return 0, false
		}
		if len(e.Operands) == 0 {
			return 0, false
		}
		total := 0
		for _, ch := range e.Operands {
			n, ok := countCollectAndLeaves(ch)
			if !ok {
				return 0, false
			}
			total += n
		}
		return total, true
	case qx.OpOR:
		return 0, false
	default:
		if len(e.Operands) != 0 {
			return 0, false
		}
		return 1, true
	}
}

func appendCollectAndLeaves(dst []qx.Expr, e qx.Expr) ([]qx.Expr, bool) {
	switch e.Op {
	case qx.OpNOOP:
		if e.Not {
			return append(dst, qx.Expr{Op: qx.OpNOOP, Not: true}), true
		}
		return dst, true
	case qx.OpAND:
		if e.Not || len(e.Operands) == 0 {
			return nil, false
		}
		for _, ch := range e.Operands {
			var ok bool
			dst, ok = appendCollectAndLeaves(dst, ch)
			if !ok {
				return nil, false
			}
		}
		return dst, true
	case qx.OpOR:
		return nil, false
	default:
		if len(e.Operands) != 0 {
			return nil, false
		}
		return append(dst, e), true
	}
}

func releasePredicatesCandidate(preds []predicate) {
	for i := range preds {
		if preds[i].cleanup != nil {
			preds[i].cleanup()
		}
	}
}

func (db *DB[K, V]) buildPredicatesCandidate(leaves []qx.Expr) ([]predicate, bool) {
	return db.buildPredicatesCandidateWithMode(leaves, true)
}

func (db *DB[K, V]) buildPredicatesCandidateWithMode(leaves []qx.Expr, allowMaterialize bool) ([]predicate, bool) {
	if len(leaves) == 1 && leaves[0].Op == qx.OpNOOP && leaves[0].Not {
		return []predicate{{alwaysFalse: true}}, true
	}

	preds := make([]predicate, 0, len(leaves))
	for _, e := range leaves {
		p, ok := db.buildPredicateCandidateWithMode(e, allowMaterialize)
		if !ok {
			releasePredicatesCandidate(preds)
			return nil, false
		}
		preds = append(preds, p)
	}
	return preds, true
}

func (db *DB[K, V]) buildPredicateCandidate(e qx.Expr) (predicate, bool) {
	return db.buildPredicateCandidateWithMode(e, true)
}

func (db *DB[K, V]) buildPredicateCandidateWithMode(e qx.Expr, allowMaterialize bool) (predicate, bool) {
	if e.Op == qx.OpNOOP {
		if e.Not {
			return predicate{expr: e, alwaysFalse: true}, true
		}
		return predicate{expr: e, alwaysTrue: true}, true
	}
	if e.Field == "" {
		return predicate{}, false
	}

	fm := db.fields[e.Field]
	if fm == nil {
		return predicate{}, false
	}
	ov := db.fieldOverlay(e.Field)

	switch e.Op {

	case qx.OpEQ:
		if !ov.hasData() {
			return predicate{}, false
		}
		return db.buildPredEqCandidate(e, fm, ov)

	case qx.OpIN:
		if !ov.hasData() {
			return predicate{}, false
		}
		return db.buildPredInCandidate(e, fm, ov)

	case qx.OpHAS:
		if !ov.hasData() {
			return predicate{}, false
		}
		return db.buildPredHasCandidate(e, fm, ov)

	case qx.OpHASANY:
		if !ov.hasData() {
			return predicate{}, false
		}
		return db.buildPredHasAnyCandidate(e, fm, ov)

	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
		if !ov.hasData() {
			return predicate{}, false
		}
		return db.buildPredRangeCandidateWithMode(e, fm, ov, allowMaterialize)

	case qx.OpSUFFIX, qx.OpCONTAINS:
		return db.buildPredMaterializedCandidate(e)

	default:
		return predicate{}, false
	}
}

func releaseOwnedBitmapSlice(owned []*roaring64.Bitmap) {
	for _, bm := range owned {
		if bm != nil {
			releaseRoaringBuf(bm)
		}
	}
}

func (db *DB[K, V]) buildPredEqCandidate(e qx.Expr, fm *field, ov fieldOverlay) (predicate, bool) {
	key, isSlice, err := db.exprValueToIdxScalar(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil {
		return predicate{}, false
	}

	if !fm.Slice {
		if isSlice {
			return predicate{}, false
		}
		ids, owned := ov.lookupPostingRetained(key)
		var cleanup func()
		if owned != nil {
			keep := owned
			cleanup = func() { releaseRoaringBuf(keep) }
		}

		if e.Not {
			if ids.IsEmpty() {
				if cleanup != nil {
					cleanup()
				}
				return predicate{expr: e, alwaysTrue: true}, true
			}
			return predicate{
				expr:    e,
				kind:    predicateKindPostingNot,
				posting: ids,
				cleanup: cleanup,
			}, true
		}
		if ids.IsEmpty() {
			if cleanup != nil {
				cleanup()
			}
			return predicate{expr: e, alwaysFalse: true}, true
		}
		return predicate{
			expr:     e,
			kind:     predicateKindPosting,
			iterKind: predicateIterPosting,
			posting:  ids,
			estCard:  ids.Cardinality(),
			cleanup:  cleanup,
		}, true
	}

	if !isSlice {
		return predicate{}, false
	}

	vals, _, err := db.exprValueToIdxOwned(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil {
		return predicate{}, false
	}

	b, err := db.evalSliceEQ(e.Field, vals)
	if err != nil {
		return predicate{}, false
	}

	if e.Not {
		if b.bm == nil || b.bm.IsEmpty() {
			b.release()
			return predicate{expr: e, alwaysTrue: true}, true
		}
		bm := b.bm
		readonly := b.readonly
		return predicate{
			expr: e,
			kind: predicateKindBitmapNot,
			bm:   bm,
			cleanup: func() {
				if !readonly {
					releaseRoaringBuf(bm)
				}
			},
		}, true
	}

	if b.bm == nil || b.bm.IsEmpty() {
		b.release()
		return predicate{expr: e, alwaysFalse: true}, true
	}

	bm := b.bm
	readonly := b.readonly
	return predicate{
		expr:     e,
		kind:     predicateKindBitmap,
		iterKind: predicateIterBitmap,
		bm:       bm,
		estCard:  bm.GetCardinality(),
		cleanup: func() {
			if !readonly {
				releaseRoaringBuf(bm)
			}
		},
	}, true
}

func (db *DB[K, V]) buildPredInCandidate(e qx.Expr, fm *field, ov fieldOverlay) (predicate, bool) {
	if fm.Slice {
		return predicate{}, false
	}

	vals, isSlice, err := db.exprValueToIdxOwned(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil {
		return predicate{}, false
	}
	if !isSlice || len(vals) == 0 {
		return predicate{}, false
	}
	vals = dedupStringsInplace(vals)

	postsBuf := getPostingListSliceBuf(len(vals))
	posts := postsBuf.values

	var ownedInline [8]*roaring64.Bitmap
	owned := ownedInline[:0]

	var est uint64
	for _, v := range vals {
		ids, keep := ov.lookupPostingRetained(v)
		if ids.IsEmpty() {
			continue
		}
		posts = append(posts, ids)
		est += ids.Cardinality()
		if keep != nil {
			owned = append(owned, keep)
		}
	}
	cleanup := func() {
		if len(owned) > 0 {
			releaseOwnedBitmapSlice(owned)
		}
		postsBuf.values = posts
		releasePostingListSliceBuf(postsBuf)
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

	return predicate{
		expr:     e,
		kind:     predicateKindPostsAny,
		iterKind: predicateIterPostsConcat,
		posts:    posts,
		estCard:  est,
		cleanup:  cleanup,
	}, true
}

func (db *DB[K, V]) buildPredHasCandidate(e qx.Expr, fm *field, ov fieldOverlay) (predicate, bool) {
	if !fm.Slice {
		return predicate{}, false
	}

	vals, isSlice, err := db.exprValueToIdxOwned(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil {
		return predicate{}, false
	}
	if !isSlice || len(vals) == 0 {
		return predicate{}, false
	}
	vals = dedupStringsInplace(vals)

	postsBuf := getPostingListSliceBuf(len(vals))
	posts := postsBuf.values

	var ownedInline [8]*roaring64.Bitmap
	owned := ownedInline[:0]

	var minCard uint64

	for _, v := range vals {
		ids, keep := ov.lookupPostingRetained(v)
		if ids.IsEmpty() {
			if len(owned) > 0 {
				releaseOwnedBitmapSlice(owned)
			}
			postsBuf.values = posts
			releasePostingListSliceBuf(postsBuf)
			if e.Not {
				return predicate{expr: e, alwaysTrue: true}, true
			}
			return predicate{expr: e, alwaysFalse: true}, true
		}
		posts = append(posts, ids)
		if keep != nil {
			owned = append(owned, keep)
		}
		c := ids.Cardinality()
		if minCard == 0 || c < minCard {
			minCard = c
		}
	}

	cleanup := func() {
		if len(owned) > 0 {
			releaseOwnedBitmapSlice(owned)
		}
		postsBuf.values = posts
		releasePostingListSliceBuf(postsBuf)
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

func (db *DB[K, V]) buildPredHasAnyCandidate(e qx.Expr, fm *field, ov fieldOverlay) (predicate, bool) {
	if !fm.Slice {
		return predicate{}, false
	}

	vals, isSlice, err := db.exprValueToIdxOwned(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil {
		return predicate{}, false
	}
	if !isSlice || len(vals) == 0 {
		return predicate{}, false
	}
	vals = dedupStringsInplace(vals)

	postsBuf := getPostingListSliceBuf(len(vals))
	posts := postsBuf.values

	var ownedInline [8]*roaring64.Bitmap
	owned := ownedInline[:0]

	var est uint64

	for _, v := range vals {
		ids, keep := ov.lookupPostingRetained(v)
		if ids.IsEmpty() {
			continue
		}
		posts = append(posts, ids)
		est += ids.Cardinality()
		if keep != nil {
			owned = append(owned, keep)
		}
	}
	cleanup := func() {
		if len(owned) > 0 {
			releaseOwnedBitmapSlice(owned)
		}
		postsBuf.values = posts
		releasePostingListSliceBuf(postsBuf)
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

	return predicate{
		expr:     e,
		kind:     predicateKindPostsAny,
		iterKind: predicateIterPostsUnion,
		posts:    posts,
		estCard:  est,
		cleanup:  cleanup,
	}, true
}

func rangeBoundsForOp(op qx.Op, key string) (rangeBounds, bool) {
	rb := rangeBounds{has: true}
	switch op {
	case qx.OpGT:
		rb.applyLo(key, false)
	case qx.OpGTE:
		rb.applyLo(key, true)
	case qx.OpLT:
		rb.applyHi(key, false)
	case qx.OpLTE:
		rb.applyHi(key, true)
	case qx.OpPREFIX:
		rb.hasPrefix = true
		rb.prefix = key
	default:
		return rangeBounds{}, false
	}
	return rb, true
}

func overlayRangeStats(ov fieldOverlay, br overlayRange) (int, uint64) {
	cur := ov.newCursor(br, false)
	n := 0
	est := uint64(0)
	for {
		_, baseIDs, de, ok := cur.next()
		if !ok {
			break
		}
		card := composePostingCardinality(baseIDs, de)
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
		_, baseIDs, de, ok := cur.next()
		if !ok {
			return false
		}
		if composePostingContains(baseIDs, de, idx) {
			return true
		}
	}
}

func overlayUnionRange(ov fieldOverlay, br overlayRange) *roaring64.Bitmap {
	out := getRoaringBuf()
	cur := ov.newCursor(br, false)
	scratch := getRoaringBuf()
	defer releaseRoaringBuf(scratch)
	for {
		_, baseBM, de, ok := cur.next()
		if !ok {
			break
		}
		bm, owned := composePostingOwned(baseBM, de, scratch)
		if bm != nil && !bm.IsEmpty() {
			out.Or(bm)
		}
		if owned && bm != nil && bm != scratch {
			releaseRoaringBuf(bm)
		}
	}
	return out
}

type overlayRangeIter struct {
	cur      overlayKeyCursor
	curIt    roaringIter
	curBM    *roaring64.Bitmap
	curOwned bool
	scratch  *roaring64.Bitmap
	released bool
}

func newOverlayRangeIter(ov fieldOverlay, br overlayRange) *overlayRangeIter {
	return &overlayRangeIter{
		cur:     ov.newCursor(br, false),
		scratch: getRoaringBuf(),
	}
}

func (it *overlayRangeIter) release() {
	if it.released {
		return
	}
	it.released = true
	if it.curIt != nil {
		releaseRoaringIter(it.curIt)
		it.curIt = nil
	}
	if it.curOwned && it.curBM != nil && it.curBM != it.scratch {
		releaseRoaringBuf(it.curBM)
	}
	it.curOwned = false
	it.curBM = nil
	if it.scratch != nil {
		releaseRoaringBuf(it.scratch)
		it.scratch = nil
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
			releaseRoaringIter(it.curIt)
			it.curIt = nil
		}
		if it.curOwned && it.curBM != nil && it.curBM != it.scratch {
			releaseRoaringBuf(it.curBM)
		}
		it.curOwned = false
		it.curBM = nil

		_, baseBM, de, ok := it.cur.next()
		if !ok {
			return false
		}
		bm, owned := composePostingOwned(baseBM, de, it.scratch)
		if bm == nil || bm.IsEmpty() {
			if owned && bm != nil && bm != it.scratch {
				releaseRoaringBuf(bm)
			}
			continue
		}
		it.curBM = bm
		it.curOwned = owned
		it.curIt = bm.Iterator()
	}
}

func (it *overlayRangeIter) Next() uint64 {
	if !it.HasNext() {
		return 0
	}
	return it.curIt.Next()
}

func (db *DB[K, V]) buildPredRangeCandidateWithMode(e qx.Expr, fm *field, ov fieldOverlay, allowMaterialize bool) (predicate, bool) {
	if fm.Slice {
		return predicate{}, false
	}
	if ov.delta == nil {
		s := ov.base
		p, ok := db.buildPredRangeWithMode(e, fm, &s, allowMaterialize)
		if !ok {
			return predicate{}, false
		}
		return p, true
	}

	key, isSlice, err := db.exprValueToIdxScalar(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil {
		return predicate{}, false
	}
	if isSlice {
		return predicate{}, false
	}
	cacheKey := db.materializedPredCacheKeyForScalar(e.Field, e.Op, key)
	if cacheKey != "" {
		if cached, ok := db.getSnapshot().loadMaterializedPred(cacheKey); ok {
			return materializedRangePredicateReadonly(e, cached), true
		}
	}

	rb, ok := rangeBoundsForOp(e.Op, key)
	if !ok {
		return predicate{}, false
	}
	br := ov.rangeForBounds(rb)
	if br.baseStart >= br.baseEnd && br.deltaStart >= br.deltaEnd {
		if e.Not {
			return predicate{expr: e, alwaysTrue: true}, true
		}
		return predicate{expr: e, alwaysFalse: true}, true
	}

	if allowMaterialize {
		if out, ok := db.tryEvalNumericRangeBuckets(e.Field, fm, ov, br); ok {
			if db.tryShareMaterializedPred(cacheKey, out.bm) {
				out.readonly = true
			}
			return materializedRangePredicateWithMode(e, out.bm, out.readonly), true
		}
	}

	bucketCount, est := overlayRangeStats(ov, br)
	if bucketCount == 0 {
		if e.Not {
			return predicate{expr: e, alwaysTrue: true}, true
		}
		return predicate{expr: e, alwaysFalse: true}, true
	}

	var (
		bitmapFilter      func(*roaring64.Bitmap) bool
		bitmapFilterCheap bool
	)
	if filter, cheap, ok := buildOverlayNumericRangeBitmapFilter(e, fm, ov, br); ok {
		bitmapFilter = filter
		bitmapFilterCheap = cheap
	}

	var (
		once sync.Once
		bm   *roaring64.Bitmap
		ro   bool
		itMu sync.Mutex
		its  []*overlayRangeIter
	)
	contains := func(idx uint64) bool {
		if bucketCount <= 16 {
			return overlayRangeContains(ov, br, idx)
		}

		once.Do(func() {
			bm = overlayUnionRange(ov, br)
			ro = db.tryShareMaterializedPred(cacheKey, bm)
		})
		if bm == nil {
			return false
		}
		return bm.Contains(idx)
	}

	cleanup := func() {
		if bm != nil && !ro {
			releaseRoaringBuf(bm)
		}
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
				return !contains(idx)
			},
			bitmapFilter:      bitmapFilter,
			bitmapFilterCheap: bitmapFilterCheap,
			cleanup:           cleanup,
		}, true
	}

	return predicate{
		expr: e,
		iter: func() roaringIter {
			it := newOverlayRangeIter(ov, br)
			itMu.Lock()
			its = append(its, it)
			itMu.Unlock()
			return it
		},
		contains:          contains,
		estCard:           est,
		bitmapFilter:      bitmapFilter,
		bitmapFilterCheap: bitmapFilterCheap,
		cleanup:           cleanup,
	}, true
}

func (db *DB[K, V]) buildPredMaterializedCandidate(e qx.Expr) (predicate, bool) {
	raw := e
	raw.Not = false
	cacheKey := db.materializedPredCacheKey(raw)

	var (
		once     sync.Once
		bm       *roaring64.Bitmap
		readonly bool
	)
	load := func() {
		if cacheKey != "" {
			if cached, ok := db.getSnapshot().loadMaterializedPred(cacheKey); ok {
				bm = cached
				readonly = true
				return
			}
		}

		b, err := db.evalSimple(raw)
		if err != nil {
			if cacheKey != "" {
				db.getSnapshot().storeMaterializedPred(cacheKey, nil)
			}
			return
		}
		if b.bm == nil || b.bm.IsEmpty() {
			b.release()
			if cacheKey != "" {
				db.getSnapshot().storeMaterializedPred(cacheKey, nil)
			}
			return
		}
		bm = b.bm
		readonly = b.readonly
		if cacheKey != "" {
			if db.tryShareMaterializedPred(cacheKey, bm) {
				// cached bitmap is shared and must not be returned to pool
				// by this predicate cleanup.
				readonly = true
			}
		}
	}
	cleanup := func() {
		if bm != nil && !readonly {
			releaseRoaringBuf(bm)
		}
	}

	est := db.snapshotUniverseCardinality()
	if est == 0 {
		est = 1
	}

	if e.Not {
		return predicate{
			expr: e,
			contains: func(idx uint64) bool {
				once.Do(load)
				if bm == nil {
					return true
				}
				return !bm.Contains(idx)
			},
			estCard: est,
			cleanup: cleanup,
		}, true
	}

	return predicate{
		expr: e,
		iter: func() roaringIter {
			once.Do(load)
			if bm == nil {
				return emptyIter{}
			}
			return bm.Iterator()
		},
		contains: func(idx uint64) bool {
			once.Do(load)
			return bm != nil && bm.Contains(idx)
		},
		estCard: est,
		cleanup: cleanup,
	}, true
}

func (db *DB[K, V]) materializedPredCacheEnabled() bool {
	snap := db.getSnapshot()
	return snap != nil && snap.materializedPredCacheLimit() > 0
}

func (db *DB[K, V]) materializedPredCacheKeyForScalar(field string, op qx.Op, key string) string {
	if !db.materializedPredCacheEnabled() {
		return ""
	}
	return materializedPredCacheKeyFromScalar(field, op, key)
}

func (db *DB[K, V]) materializedPredComplementCacheKeyForScalar(field string, op qx.Op, key string) string {
	if !db.materializedPredCacheEnabled() {
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

func (db *DB[K, V]) materializedPredCacheKey(e qx.Expr) string {
	key, isSlice, err := db.exprValueToIdxScalar(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil || isSlice {
		return ""
	}
	return db.materializedPredCacheKeyForScalar(e.Field, e.Op, key)
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

// tryShareMaterializedPred caches bm and reports whether the snapshot cache
// ended up reusing the same bitmap pointer, so caller may keep using bm as
// readonly without cloning.
func (db *DB[K, V]) tryShareMaterializedPred(cacheKey string, bm *roaring64.Bitmap) bool {
	if cacheKey == "" || bm == nil {
		return false
	}
	snap := db.getSnapshot()
	if snap == nil || snap.materializedPredCacheLimit() <= 0 {
		return false
	}
	if bm.IsEmpty() {
		snap.storeMaterializedPred(cacheKey, nil)
		return false
	}
	snap.storeMaterializedPred(cacheKey, bm)
	cached, ok := snap.loadMaterializedPred(cacheKey)
	if ok && cached == bm {
		return true
	}
	if snap.tryStoreMaterializedPredOversized(cacheKey, bm) {
		cached, ok = snap.loadMaterializedPred(cacheKey)
		return ok && cached == bm
	}
	return false
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
	curIt  roaringIter
	single struct {
		set bool
		v   uint64
	}
}

type singletonIter struct {
	v   uint64
	has bool
}

func newSingletonIter(v uint64) roaringIter {
	return &singletonIter{v: v, has: true}
}

func (it *singletonIter) HasNext() bool { return it.has }

func (it *singletonIter) Next() uint64 {
	if !it.has {
		return 0
	}
	it.has = false
	return it.v
}

func newRangeIter(s []index, start, end int) roaringIter {
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
			releaseRoaringIter(it.curIt)
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
		if bm.isSingleton() {
			it.single.set = true
			it.single.v = bm.single
			continue
		}
		bmRO := bm.bitmap()
		it.curIt = bmRO.Iterator()
	}
}

func (it *rangeIter) Release() {
	if it.curIt != nil {
		releaseRoaringIter(it.curIt)
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

func (db *DB[K, V]) execPlanLeadScanNoOrder(q *qx.QX, preds []predicate, trace *queryTrace) []K {
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

	var it roaringIter
	var universe *roaring64.Bitmap
	leadNeedsCheck := false
	if leadIdx >= 0 {
		it = preds[leadIdx].newIter()
		leadNeedsCheck = preds[leadIdx].leadIterNeedsContainsCheck()
	} else {
		var owned bool
		universe, owned = db.snapshotUniverseView()
		if owned {
			defer releaseRoaringBuf(universe)
		}
		it = universe.Iterator()
	}
	defer releaseRoaringIter(it)
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
	cursor := db.newQueryCursor(out, q.Offset, q.Limit, q.Limit == 0, nil)
	singleActive := -1
	if len(active) == 1 {
		singleActive = active[0]
	}

	if out, ok := db.tryExecLeadScanNoOrderBuckets(q, preds, leadIdx, active, trace); ok {
		return out
	}

	if trace != nil {
		if leadIdx >= 0 && best > 0 {
			trace.addExamined(best)
		} else {
			trace.addExamined(db.snapshotUniverseCardinality())
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

func (db *DB[K, V]) tryExecLeadScanNoOrderBuckets(q *qx.QX, preds []predicate, leadIdx int, active []int, trace *queryTrace) ([]K, bool) {
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

	fm := db.fields[e.Field]
	if fm == nil || fm.Slice {
		return nil, false
	}

	snap := db.getSnapshot()
	if snap == nil || snap.fieldDelta(e.Field) != nil {
		return nil, false
	}

	slice := snap.fieldIndexSlice(e.Field)
	if slice == nil || len(*slice) == 0 {
		return nil, true
	}

	key, isSlice, err := db.exprValueToIdxScalar(qx.Expr{
		Op:    e.Op,
		Field: e.Field,
		Value: e.Value,
	})
	if err != nil || isSlice {
		return nil, false
	}

	s := *slice
	start, end, ok := resolveRange(s, e.Op, key)
	if !ok {
		return nil, false
	}
	if start >= end {
		return nil, true
	}

	return db.scanOrderedBaseSliceWithPredicates(q, preds, active, start, end, s, false, trace), true
}

func (db *DB[K, V]) execPlanCandidateOrderBasic(q *qx.QX, preds []predicate) []K {
	o := q.Order[0]

	fm := db.fields[o.Field]
	if fm == nil || fm.Slice {
		return nil
	}

	ov := db.fieldOverlay(o.Field)
	if !ov.hasData() {
		return nil
	}

	// Fast base-only path preserves legacy low-overhead order scan behavior.
	if ov.delta == nil {
		s := ov.base
		if len(s) == 0 {
			return nil
		}

		start, end := 0, len(s)
		var rangeCovered []bool
		if st, en, cov, ok := db.extractOrderRangeCoverage(o.Field, preds, s); ok {
			start, end = st, en
			rangeCovered = cov
		}
		if start >= end {
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
		return db.scanOrderedBaseSliceWithPredicates(q, preds, active, start, end, s, o.Desc, nil)
	}

	br, rangeCovered, ok := db.extractOrderRangeCoverageOverlay(o.Field, preds, ov)
	if !ok {
		return nil
	}
	if br.baseStart >= br.baseEnd && br.deltaStart >= br.deltaEnd {
		return nil
	}
	for i := range rangeCovered {
		if rangeCovered[i] {
			preds[i].covered = true
		}
	}

	out, ok := db.scanOrderLimitWithPredicates(q, ov, br, o.Desc, preds, nil)
	if !ok {
		return nil
	}
	return out
}

func (db *DB[K, V]) extractOrderRangeCoverage(field string, preds []predicate, s []index) (int, int, []bool, bool) {
	rb, covered, has, ok := db.collectOrderRangeBounds(field, len(preds), func(i int) qx.Expr {
		return preds[i].expr
	})
	if !ok {
		return 0, 0, nil, false
	}

	if !has {
		return 0, len(s), covered, true
	}

	st, en := applyBoundsToIndexRange(s, rb)
	return st, en, covered, true
}

func (db *DB[K, V]) extractOrderRangeCoverageOverlay(field string, preds []predicate, ov fieldOverlay) (overlayRange, []bool, bool) {
	rb, covered, _, ok := db.collectOrderRangeBounds(field, len(preds), func(i int) qx.Expr {
		return preds[i].expr
	})
	if !ok {
		return overlayRange{}, nil, false
	}

	return ov.rangeForBounds(rb), covered, true
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

func rangeHashSetBucketsMinForProbe(probeLen int, est uint64) int {
	minBuckets := rangeHashSetBucketsMin
	if est > 0 && est <= 6_000 && probeLen >= 16_384 {
		minBuckets = 896
	}
	if minBuckets < 128 {
		minBuckets = 128
	}
	return minBuckets
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

func rangeSingletonProbeSampleSize(probeLen int) int {
	sampleN := rangeBucketCountMaxProbe
	if probeLen > 4_096 {
		sampleN = 72
	}
	if probeLen < sampleN {
		sampleN = probeLen
	}
	if sampleN <= 0 {
		sampleN = 1
	}
	return sampleN
}

// tryPlanOrdered attempts the internal plan ordered path and returns false when fast-path preconditions are not satisfied.
func (db *DB[K, V]) tryPlanOrdered(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	if q.Limit == 0 {
		return nil, false, nil
	}
	if len(q.Order) > 1 {
		return nil, false, nil
	}

	leaves, ok := collectAndLeaves(q.Expr)
	if !ok {
		return nil, false, nil
	}

	if len(q.Order) == 1 {
		o := q.Order[0]
		if o.Type != qx.OrderBasic {
			return nil, false, nil
		}
		decision := db.decideOrderedByCost(q, leaves)
		if trace != nil {
			trace.setEstimated(decision.expectedProbeRows, decision.orderedCost, decision.fallbackCost)
		}
		if !decision.use {
			return nil, false, nil
		}

		window, _ := orderWindow(q)
		preds, ok := db.buildPredicatesOrderedWithMode(leaves, o.Field, false, window)
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
		out, ok := db.execPlanOrderedBasic(q, preds, trace)
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

	preds, ok := db.buildPredicates(leaves)
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
	return db.execPlanLeadScanNoOrder(q, preds, trace), true, nil
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

func buildBitmapFilterActive(dst, active []int, preds []predicate) []int {
	dst = dst[:0]
	for _, pi := range active {
		if preds[pi].supportsBitmapFilter() {
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

func (db *DB[K, V]) buildPredicatesWithMode(leaves []qx.Expr, allowMaterialize bool) ([]predicate, bool) {
	if len(leaves) == 1 && leaves[0].Op == qx.OpNOOP && leaves[0].Not {
		return []predicate{{alwaysFalse: true}}, true
	}

	preds := make([]predicate, 0, len(leaves))
	for _, e := range leaves {
		p, ok := db.buildPredicateWithMode(e, allowMaterialize)
		if !ok {
			releasePredicates(preds)
			return nil, false
		}
		preds = append(preds, p)
	}
	return preds, true
}

func (db *DB[K, V]) buildPredicates(leaves []qx.Expr) ([]predicate, bool) {
	return db.buildPredicatesWithMode(leaves, true)
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

func (db *DB[K, V]) buildPredicatesOrdered(leaves []qx.Expr, orderField string) ([]predicate, bool) {
	return db.buildPredicatesOrderedWithMode(leaves, orderField, true, 0)
}

func shouldUseOrderedBroadRangeComplementMaterialization(p predicate, ov fieldOverlay, universe uint64) bool {
	if ov.delta == nil || universe == 0 || p.kind != predicateKindCustom {
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
	return br.baseStart >= br.baseEnd && br.deltaStart >= br.deltaEnd
}

func (db *DB[K, V]) tryMaterializeBroadRangeComplementPredicateForOrdered(p *predicate, ov fieldOverlay, universe uint64) bool {
	if p == nil {
		return false
	}
	if !shouldUseOrderedBroadRangeComplementMaterialization(*p, ov, universe) {
		return false
	}

	key, isSlice, err := db.exprValueToIdxScalar(qx.Expr{Op: p.expr.Op, Field: p.expr.Field, Value: p.expr.Value})
	if err != nil || isSlice {
		return false
	}
	rb, ok := rangeBoundsForOp(p.expr.Op, key)
	if !ok {
		return false
	}
	br := ov.rangeForBounds(rb)

	totalDelta := 0
	if ov.delta != nil {
		totalDelta = ov.delta.keyCount()
	}
	before := overlayRange{
		baseStart:  0,
		baseEnd:    br.baseStart,
		deltaStart: 0,
		deltaEnd:   br.deltaStart,
	}
	after := overlayRange{
		baseStart:  br.baseEnd,
		baseEnd:    len(ov.base),
		deltaStart: br.deltaEnd,
		deltaEnd:   totalDelta,
	}

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
	if complementEst == 0 {
		p.kind = predicateKindCustom
		p.iterKind = predicateIterNone
		p.posting = postingList{}
		p.posts = nil
		p.bm = nil
		p.contains = nil
		p.iter = nil
		p.bucketCount = nil
		p.estCard = 0
		p.alwaysTrue = true
		p.alwaysFalse = false
		return true
	}
	if complementEst > orderedPredBroadRangeComplementMaxCard || complementBuckets > orderedPredBroadRangeComplementMaxSlots {
		return false
	}

	bm := getRoaringBuf()
	appendComplement := func(span overlayRange) {
		if overlayRangeEmpty(span) {
			return
		}
		part := overlayUnionRange(ov, span)
		if part == nil {
			return
		}
		if !part.IsEmpty() {
			bm.Or(part)
		}
		releaseRoaringBuf(part)
	}
	appendComplement(before)
	appendComplement(after)
	if bm.IsEmpty() {
		releaseRoaringBuf(bm)
		p.kind = predicateKindCustom
		p.iterKind = predicateIterNone
		p.posting = postingList{}
		p.posts = nil
		p.bm = nil
		p.contains = nil
		p.iter = nil
		p.bucketCount = nil
		p.estCard = 0
		p.alwaysTrue = true
		p.alwaysFalse = false
		return true
	}

	p.cleanup = chainPredicateCleanup(p.cleanup, func() {
		releaseRoaringBuf(bm)
	})
	p.kind = predicateKindBitmapNot
	p.iterKind = predicateIterNone
	p.posting = postingList{}
	p.posts = nil
	p.bm = bm
	p.contains = nil
	p.iter = nil
	p.bucketCount = nil
	p.estCard = 0
	p.alwaysTrue = false
	p.alwaysFalse = false
	return true
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

func (db *DB[K, V]) buildPredicatesOrderedWithMode(leaves []qx.Expr, orderField string, allowMaterialize bool, orderedWindow int) ([]predicate, bool) {
	if len(leaves) == 1 && leaves[0].Op == qx.OpNOOP && leaves[0].Not {
		return []predicate{{alwaysFalse: true}}, true
	}

	preds := make([]predicate, 0, len(leaves))
	universe := uint64(0)
	if !allowMaterialize {
		universe = db.snapshotUniverseCardinality()
	}
	for _, e := range leaves {
		if isOrderRangeCoveredLeaf(orderField, e) {
			// Keep conversion/typing validation, but avoid expensive predicate
			// materialization for leaves that are fully covered by order range.
			_, isSlice, err := db.exprValueToIdxScalar(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
			if err != nil || isSlice {
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
		if !predAllowMaterialize && shouldEagerMaterializeOrderedPredicate(e, orderField, orderedWindow) {
			predAllowMaterialize = true
		}
		var ov fieldOverlay
		if !allowMaterialize {
			ov = db.fieldOverlay(e.Field)
		}

		p, ok := db.buildPredicateWithMode(e, predAllowMaterialize)
		if !ok {
			releasePredicates(preds)
			return nil, false
		}
		if !predAllowMaterialize {
			db.tryMaterializeBroadRangeComplementPredicateForOrdered(&p, ov, universe)
		}
		preds = append(preds, p)
	}
	return preds, true
}

func (db *DB[K, V]) buildPredicateWithMode(e qx.Expr, allowMaterialize bool) (predicate, bool) {
	if e.Op == qx.OpNOOP {
		if e.Not {
			return predicate{expr: e, alwaysFalse: true}, true
		}
		return predicate{expr: e, alwaysTrue: true}, true
	}

	if e.Field == "" {
		return predicate{}, false
	}

	fm := db.fields[e.Field]
	if fm == nil {
		return predicate{}, false
	}

	switch e.Op {
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
		ov := db.fieldOverlay(e.Field)
		if ov.hasData() && ov.delta != nil {
			p, ok := db.buildPredRangeCandidateWithMode(e, fm, ov, allowMaterialize)
			if !ok {
				return predicate{}, false
			}
			return p, true
		}
		slice := db.snapshotFieldIndexSlice(e.Field)
		if slice == nil {
			return predicate{}, false
		}
		return db.buildPredRangeWithMode(e, fm, slice, allowMaterialize)
	default:
		p, ok := db.buildPredicateCandidate(e)
		if !ok {
			return predicate{}, false
		}
		return p, true
	}
}

func (db *DB[K, V]) buildPredRange(e qx.Expr, fm *field, slice *[]index) (predicate, bool) {
	return db.buildPredRangeWithMode(e, fm, slice, true)
}

func (db *DB[K, V]) buildPredRangeWithMode(e qx.Expr, fm *field, slice *[]index, allowMaterialize bool) (predicate, bool) {
	if fm.Slice {
		return predicate{}, false
	}

	key, isSlice, err := db.exprValueToIdxScalar(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
	if err != nil {
		return predicate{}, false
	}
	if isSlice {
		return predicate{}, false
	}
	cacheKey := db.materializedPredCacheKeyForScalar(e.Field, e.Op, key)
	if cacheKey != "" {
		if cached, ok := db.getSnapshot().loadMaterializedPred(cacheKey); ok {
			return materializedRangePredicateReadonly(e, cached), true
		}
	}

	s := *slice
	start, end, ok := resolveRange(s, e.Op, key)
	if !ok {
		return predicate{}, false
	}

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

	if allowMaterialize {
		if out, ok := db.tryEvalNumericRangeBuckets(e.Field, fm, fieldOverlay{base: s}, overlayRange{
			baseStart: start,
			baseEnd:   end,
		}); ok {
			if db.tryShareMaterializedPred(cacheKey, out.bm) {
				out.readonly = true
			}
			return materializedRangePredicateWithMode(e, out.bm, out.readonly), true
		}
	}

	inBuckets := end - start
	outBuckets := len(s) - inBuckets
	useComplement := outBuckets < inBuckets

	var est uint64
	if exact, ok := db.tryCountSnapshotNumericRange(e.Field, fm, slice, start, end); ok {
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
	bitmapFilter, bitmapFilterCheap, _ := buildBaseNumericRangeBitmapFilter(e, fm, probe)

	var onMaterialized func(*roaring64.Bitmap) bool
	if cacheKey != "" && !useComplement {
		onMaterialized = func(bm *roaring64.Bitmap) bool {
			return db.tryShareMaterializedPred(cacheKey, bm)
		}
	}
	containsInRange, cleanupRange := db.buildRangeContains(probe, onMaterialized)
	bucketCountMaxProbe := rangeBucketCountMaxProbeForShape(probe.probeLen, est)
	cleanup := func() {
		if cleanupRange != nil {
			cleanupRange()
		}
	}

	countInRange := func(bucket *roaring64.Bitmap) (uint64, bool) {
		return probe.countBucket(bucket, bucketCountMaxProbe)
	}

	if e.Not {
		return predicate{
			expr: e,
			contains: func(idx uint64) bool {
				return !containsInRange(idx)
			},
			bucketCount: func(bucket *roaring64.Bitmap) (uint64, bool) {
				in, ok := countInRange(bucket)
				if !ok {
					return 0, false
				}
				bc := bucket.GetCardinality()
				if in >= bc {
					return 0, true
				}
				return bc - in, true
			},
			bitmapFilter:      bitmapFilter,
			bitmapFilterCheap: bitmapFilterCheap,
			cleanup:           cleanup,
		}, true
	}

	return predicate{
		expr: e,
		iter: func() roaringIter {
			return newRangeIter(s, start, end)
		},
		contains: containsInRange,
		estCard:  est,
		bucketCount: func(bucket *roaring64.Bitmap) (uint64, bool) {
			return countInRange(bucket)
		},
		bitmapFilter:      bitmapFilter,
		bitmapFilterCheap: bitmapFilterCheap,
		cleanup:           cleanup,
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
	p.forEachPosting(func(ids postingList) bool {
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

func (p baseRangeProbe) forEachPosting(fn func(postingList) bool) bool {
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
	p.forEachPosting(func(ids postingList) bool {
		if ids.Contains(idx) {
			hit = true
			return false
		}
		return true
	})
	return hit
}

func (p baseRangeProbe) countBucket(bucket *roaring64.Bitmap, maxProbe int) (uint64, bool) {
	if bucket == nil || bucket.IsEmpty() {
		return 0, true
	}
	if p.probeLen > maxProbe {
		return 0, false
	}

	var hit uint64
	p.forEachPosting(func(ids postingList) bool {
		hit += ids.AndCardinalityBitmap(bucket)
		return true
	})

	if !p.useComplement {
		return hit, true
	}

	bc := bucket.GetCardinality()
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
	rangeBitmapFilterKeepProbeMaxBuckets = 24
	rangeBitmapFilterKeepRowsMaxCard     = 8_192
)

func shouldUseNumericRangeBitmapFilter(e qx.Expr, fm *field) bool {
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

func applyRangeProbeBitmapFilter(
	dst *roaring64.Bitmap,
	keepProbeHits bool,
	probeLen int,
	probeContains func(uint64) bool,
	forEachPosting func(func(postingList) bool) bool,
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
	if !keepProbeHits {
		forEachPosting(func(ids postingList) bool {
			ids.AndNotFrom(dst)
			return !dst.IsEmpty()
		})
		return true
	}
	if probeLen > rangeBitmapFilterKeepProbeMaxBuckets && dst.GetCardinality() > rangeBitmapFilterKeepRowsMaxCard {
		return false
	}

	work := getRoaringBuf()
	defer releaseRoaringBuf(work)

	it := dst.Iterator()
	for it.HasNext() {
		idx := it.Next()
		if probeContains(idx) {
			work.Add(idx)
		}
	}
	releaseRoaringBitmapIterator(it)

	dst.Clear()
	dst.Or(work)
	return true
}

func buildBaseNumericRangeBitmapFilter(e qx.Expr, fm *field, probe baseRangeProbe) (func(*roaring64.Bitmap) bool, bool, bool) {
	if !shouldUseNumericRangeBitmapFilter(e, fm) || probe.probeLen == 0 {
		return nil, false, false
	}
	keepProbeHits := probe.useComplement == e.Not
	return func(dst *roaring64.Bitmap) bool {
		return applyRangeProbeBitmapFilter(dst, keepProbeHits, probe.probeLen, probe.linearContains, probe.forEachPosting)
	}, !keepProbeHits, true
}

type overlayRangeProbe struct {
	ov       fieldOverlay
	spans    [2]overlayRange
	spanCnt  int
	probeLen int
}

func newOverlayRangeProbe(ov fieldOverlay, br overlayRange, useComplement bool) overlayRangeProbe {
	p := overlayRangeProbe{ov: ov}
	if useComplement {
		before := overlayRange{
			baseStart:  0,
			baseEnd:    br.baseStart,
			deltaStart: 0,
			deltaEnd:   br.deltaStart,
		}
		after := overlayRange{
			baseStart:  br.baseEnd,
			baseEnd:    len(ov.base),
			deltaStart: br.deltaEnd,
			deltaEnd: func() int {
				if ov.delta == nil {
					return 0
				}
				return ov.delta.keyCount()
			}(),
		}
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
	p.forEachPosting(func(ids postingList) bool {
		p.probeLen++
		return true
	})
}

func (p overlayRangeProbe) forEachPosting(fn func(postingList) bool) bool {
	for i := 0; i < p.spanCnt; i++ {
		cur := p.ov.newCursor(p.spans[i], false)
		for {
			_, baseIDs, de, ok := cur.next()
			if !ok {
				break
			}
			ids, keep := composePostingRetained(baseIDs, de)
			if ids.IsEmpty() {
				if keep != nil {
					releaseRoaringBuf(keep)
				}
				continue
			}
			if !fn(ids) {
				if keep != nil {
					releaseRoaringBuf(keep)
				}
				return false
			}
			if keep != nil {
				releaseRoaringBuf(keep)
			}
		}
	}
	return true
}

func (p overlayRangeProbe) contains(idx uint64) bool {
	for i := 0; i < p.spanCnt; i++ {
		if overlayRangeContains(p.ov, p.spans[i], idx) {
			return true
		}
	}
	return false
}

func buildOverlayNumericRangeBitmapFilter(e qx.Expr, fm *field, ov fieldOverlay, br overlayRange) (func(*roaring64.Bitmap) bool, bool, bool) {
	if !shouldUseNumericRangeBitmapFilter(e, fm) {
		return nil, false, false
	}
	totalBuckets := len(ov.base)
	if ov.delta != nil {
		totalBuckets += ov.delta.keyCount()
	}
	inBuckets := (br.baseEnd - br.baseStart) + (br.deltaEnd - br.deltaStart)
	if totalBuckets <= 0 || inBuckets <= 0 || inBuckets >= totalBuckets {
		return nil, false, false
	}
	useComplement := totalBuckets-inBuckets < inBuckets
	probe := newOverlayRangeProbe(ov, br, useComplement)
	if probe.probeLen == 0 {
		return nil, false, false
	}
	keepProbeHits := useComplement == e.Not
	return func(dst *roaring64.Bitmap) bool {
		return applyRangeProbeBitmapFilter(dst, keepProbeHits, probe.probeLen, probe.contains, probe.forEachPosting)
	}, !keepProbeHits, true
}

func materializedRangePredicateReadonly(e qx.Expr, bm *roaring64.Bitmap) predicate {
	return materializedRangePredicateWithMode(e, bm, true)
}

func materializedRangePredicateWithMode(e qx.Expr, bm *roaring64.Bitmap, readonly bool) predicate {
	if bm == nil || bm.IsEmpty() {
		if bm != nil && !readonly {
			releaseRoaringBuf(bm)
		}
		if e.Not {
			return predicate{expr: e, alwaysTrue: true}
		}
		return predicate{expr: e, alwaysFalse: true}
	}

	containsInRange := func(idx uint64) bool {
		return bm.Contains(idx)
	}
	countInRange := func(bucket *roaring64.Bitmap) (uint64, bool) {
		if bucket == nil || bucket.IsEmpty() {
			return 0, true
		}
		return bucket.AndCardinality(bm), true
	}
	cleanup := func() {
		if !readonly {
			releaseRoaringBuf(bm)
		}
	}

	if e.Not {
		return predicate{
			expr: e,
			contains: func(idx uint64) bool {
				return !containsInRange(idx)
			},
			bucketCount: func(bucket *roaring64.Bitmap) (uint64, bool) {
				if bucket == nil || bucket.IsEmpty() {
					return 0, true
				}
				in, _ := countInRange(bucket)
				bc := bucket.GetCardinality()
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
		iter: func() roaringIter {
			return bm.Iterator()
		},
		contains: containsInRange,
		estCard:  bm.GetCardinality(),
		bucketCount: func(bucket *roaring64.Bitmap) (uint64, bool) {
			return countInRange(bucket)
		},
		cleanup: cleanup,
	}
}

// buildRangeContains builds a membership checker for a range probe set.
//
// The checker starts cheap (linear Contains over probe buckets) and upgrades
// to hash/bitmap materialization only after enough repeated calls.
func (db *DB[K, V]) buildRangeContains(
	probe baseRangeProbe,
	onMaterialized func(*roaring64.Bitmap) bool,
) (func(uint64) bool, func()) {
	if probe.probeLen == 0 {
		if probe.useComplement {
			return func(uint64) bool { return true }, nil
		}
		return func(uint64) bool { return false }, nil
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
		}, nil
	}

	var (
		calls      int
		set        *u64set
		bm         *roaring64.Bitmap
		bmReadonly bool
	)

	contains := func(idx uint64) bool {
		if set != nil {
			return project(set.Has(idx))
		}
		if bm != nil {
			return project(bm.Contains(idx))
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
				probe.forEachPosting(func(b postingList) bool {
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
				// High-card ranges (e.g. score>=X on dense numeric fields) are
				// often represented by a huge number of singleton buckets.
				// Materializing such probes via OR across all buckets is expensive.
				// Fast-path singleton-heavy probes through batched AddMany.
				if probe.singletonHeavy(fastSinglesMinProbe) {
					bm = materializeBaseRangeProbeFast(probe)
				} else {
					bm = db.materializeBaseRangeProbeUnion(probe)
				}
				if bm != nil && onMaterialized != nil {
					bmReadonly = onMaterialized(bm)
				}
				return project(bm.Contains(idx))
			}
		}

		return project(linearContains(idx))
	}

	cleanup := func() {
		if set != nil {
			releaseU64Set(set)
			set = nil
		}
		if bm != nil && !bmReadonly {
			releaseRoaringBuf(bm)
		}
	}

	return contains, cleanup
}

func materializeBaseRangeProbeFast(probe baseRangeProbe) *roaring64.Bitmap {
	out := getRoaringBuf()
	singles := getSingleIDsBuf()
	defer releaseSingleIDs(singles)

	flushSingles := func() {
		if len(singles.values) == 0 {
			return
		}
		out.AddMany(singles.values)
		singles.values = singles.values[:0]
	}

	probe.forEachPosting(func(p postingList) bool {
		if p.Cardinality() == 1 {
			id, _ := p.Minimum()
			singles.values = append(singles.values, id)
			if len(singles.values) == cap(singles.values) {
				flushSingles()
			}
			return true
		}

		flushSingles()
		p.OrInto(out)
		return true
	})
	flushSingles()
	return out
}

func (db *DB[K, V]) materializeBaseRangeProbeUnion(probe baseRangeProbe) *roaring64.Bitmap {
	bmBuf := getRoaringSliceBuf(probe.probeLen)
	bitmaps := bmBuf.values
	singles := getSingleIDsBuf()
	defer releaseSingleIDs(singles)

	probe.forEachPosting(func(p postingList) bool {
		if p.isSingleton() {
			singles.values = append(singles.values, p.single)
			return true
		}
		bitmaps = append(bitmaps, p.bitmap())
		return true
	})

	var out *roaring64.Bitmap
	switch len(bitmaps) {
	case 0:
		out = getRoaringBuf()
	case 1:
		out = getRoaringBuf()
		out.Or(bitmaps[0])
	default:
		out = db.unionBitmaps(bitmaps)
	}
	if len(singles.values) > 0 {
		out.AddMany(singles.values)
	}

	bmBuf.values = bitmaps
	releaseRoaringSliceBuf(bmBuf)
	return out
}

func isSingletonHeavyProbe(probe []postingList, minProbe int, sampleN int) bool {
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

func materializeProbeFast(probe []postingList) *roaring64.Bitmap {
	out := getRoaringBuf()
	singles := getSingleIDsBuf()
	defer releaseSingleIDs(singles)

	flushSingles := func() {
		if len(singles.values) == 0 {
			return
		}
		out.AddMany(singles.values)
		singles.values = singles.values[:0]
	}

	for _, p := range probe {
		if p.IsEmpty() {
			continue
		}
		if p.Cardinality() == 1 {
			id, _ := p.Minimum()
			singles.values = append(singles.values, id)
			if len(singles.values) == cap(singles.values) {
				flushSingles()
			}
			continue
		}

		flushSingles()
		p.OrInto(out)
	}
	flushSingles()
	return out
}

func (db *DB[K, V]) materializeProbeUnion(probe []postingList) *roaring64.Bitmap {
	bmBuf := getRoaringSliceBuf(len(probe))
	bitmaps := bmBuf.values
	singles := getSingleIDsBuf()
	defer releaseSingleIDs(singles)

	for _, p := range probe {
		if p.IsEmpty() {
			continue
		}
		if p.isSingleton() {
			singles.values = append(singles.values, p.single)
			continue
		}
		bitmaps = append(bitmaps, p.bitmap())
	}

	var out *roaring64.Bitmap
	switch len(bitmaps) {
	case 0:
		out = getRoaringBuf()
	case 1:
		out = getRoaringBuf()
		out.Or(bitmaps[0])
	default:
		out = db.unionBitmaps(bitmaps)
	}
	if len(singles.values) > 0 {
		out.AddMany(singles.values)
	}

	bmBuf.values = bitmaps
	releaseRoaringSliceBuf(bmBuf)
	return out
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

func (db *DB[K, V]) execPlanOrderedBasicAnchored(q *qx.QX, preds []predicate, active []int, start, end int, s []index, trace *queryTrace) ([]K, bool) {
	if len(active) < plannerOrderedAnchorMinActive {
		return nil, false
	}
	if end-start < plannerOrderedAnchorSpanMin {
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
	if start == 0 && end == len(s) {
		wideLeadMax := candidateCap * 8
		if wideLeadMax < candidateCap {
			wideLeadMax = ^uint64(0)
		}
		if preds[leadIdx].estCard > wideLeadMax {
			return nil, false
		}
	}

	leadIt := preds[leadIdx].newIter()
	defer releaseRoaringIter(leadIt)
	if leadIt == nil {
		return nil, false
	}
	leadNeedsCheck := preds[leadIdx].leadIterNeedsContainsCheck()

	candidates := getRoaringBuf()
	defer releaseRoaringBuf(candidates)

	leadExamined := uint64(0)
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
		if candidates.GetCardinality() > candidateCap {
			return nil, false
		}
	}

	if candidates.IsEmpty() {
		if trace != nil {
			trace.setEarlyStopReason("no_candidates")
		}
		return nil, true
	}

	o := q.Order[0]
	skip := q.Offset
	need := q.Limit
	out := make([]K, 0, need)
	totalCandidates := candidates.GetCardinality()
	seenCandidates := uint64(0)
	scanWidth := uint64(0)

	emitBucket := func(bm postingList) bool {
		if bm.IsEmpty() {
			return false
		}
		scanWidth++
		if trace != nil {
			trace.addExamined(bm.Cardinality())
		}

		stop := false
		bm.ForEach(func(idx uint64) bool {
			if !candidates.Contains(idx) {
				return true
			}
			seenCandidates++

			if skip > 0 {
				skip--
				return true
			}

			out = append(out, db.idFromIdxNoLock(idx))
			need--
			if need == 0 {
				stop = true
				return false
			}
			return true
		})
		return stop
	}

	if !o.Desc {
		for i := start; i < end; i++ {
			if emitBucket(s[i].IDs) {
				break
			}
			if seenCandidates == totalCandidates {
				break
			}
		}
	} else {
		for i := end - 1; i >= start; i-- {
			if emitBucket(s[i].IDs) {
				break
			}
			if seenCandidates == totalCandidates {
				break
			}
			if i == start {
				break
			}
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

func (db *DB[K, V]) execPlanOrderedBasic(q *qx.QX, preds []predicate, trace *queryTrace) ([]K, bool) {
	o := q.Order[0]

	fm := db.fields[o.Field]
	if fm == nil || fm.Slice {
		return nil, false
	}
	ov := db.fieldOverlay(o.Field)
	if !ov.hasData() {
		return nil, false
	}

	// Fast base-only path preserves anchored/fallback heuristics.
	if ov.delta == nil {
		s := ov.base
		if len(s) == 0 {
			return nil, true
		}

		start, end := 0, len(s)
		var rangeCovered []bool
		if st, en, cov, ok := db.extractOrderRangeCoverage(o.Field, preds, s); ok {
			start, end = st, en
			rangeCovered = cov
		}
		if start >= end {
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

		if out, ok := db.execPlanOrderedBasicAnchored(q, preds, active, start, end, s, trace); ok {
			if trace != nil {
				trace.setPlan(PlanOrderedAnchor)
			}
			return out, true
		}

		sortActivePredicates(active, preds)
		return db.execPlanOrderedBasicFallback(q, preds, active, start, end, s, trace), true
	}

	br, rangeCovered, ok := db.extractOrderRangeCoverageOverlay(o.Field, preds, ov)
	if !ok {
		return nil, false
	}
	if br.baseStart >= br.baseEnd && br.deltaStart >= br.deltaEnd {
		return nil, true
	}
	for i := range rangeCovered {
		if rangeCovered[i] {
			preds[i].covered = true
		}
	}
	out, ok := db.scanOrderLimitWithPredicates(q, ov, br, o.Desc, preds, trace)
	if !ok {
		return nil, false
	}
	return out, true
}

// execPlanOrderedBasicFallback scans ordered buckets and applies remaining
// predicates when ordered-anchor shortcuts are not available.
func (db *DB[K, V]) execPlanOrderedBasicFallback(q *qx.QX, preds []predicate, active []int, start, end int, s []index, trace *queryTrace) []K {
	return db.scanOrderedBaseSliceWithPredicates(q, preds, active, start, end, s, q.Order[0].Desc, trace)
}

func (db *DB[K, V]) scanOrderedBaseSliceNoPredicates(q *qx.QX, start, end int, s []index, desc bool, trace *queryTrace) []K {
	out := make([]K, 0, int(q.Limit))
	cursor := db.newQueryCursor(out, q.Offset, q.Limit, false, nil)

	var scanWidth uint64

	emitMatched := func(idx uint64) bool {
		if trace != nil {
			trace.addMatched(1)
		}
		return cursor.emit(idx)
	}

	emitMatchedBitmap := func(bm *roaring64.Bitmap, card uint64) bool {
		if trace != nil {
			trace.addMatched(card)
		}
		if cursor.skip >= card {
			cursor.skip -= card
			return false
		}
		return cursor.emitBitmap(bm)
	}

	emitBucket := func(bucket postingList) bool {
		if bucket.IsEmpty() {
			return false
		}
		scanWidth++
		card := bucket.Cardinality()
		if trace != nil {
			trace.addExamined(card)
		}
		if bucket.isSingleton() {
			return emitMatched(bucket.single)
		}
		if bm := bucket.bitmap(); bm != nil {
			return emitMatchedBitmap(bm, card)
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

func (db *DB[K, V]) scanOrderedBaseSliceWithPredicates(q *qx.QX, preds []predicate, active []int, start, end int, s []index, desc bool, trace *queryTrace) []K {
	if len(active) == 0 {
		return db.scanOrderedBaseSliceNoPredicates(q, start, end, s, desc, trace)
	}

	out := make([]K, 0, int(q.Limit))
	cursor := db.newQueryCursor(out, q.Offset, q.Limit, false, nil)

	exactActiveBuf := getIntSliceBuf(len(active))
	exactActive := buildBitmapFilterActive(exactActiveBuf.values, active, preds)
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

	bucketScratch := getRoaringBuf()
	defer releaseRoaringBuf(bucketScratch)
	var exactWork *roaring64.Bitmap
	if len(exactActive) > 0 {
		exactWork = getRoaringBuf()
		defer releaseRoaringBuf(exactWork)
	}

	var scanWidth uint64

	emitMatched := func(idx uint64) bool {
		if trace != nil {
			trace.addMatched(1)
		}
		return cursor.emit(idx)
	}

	emitMatchedBitmap := func(bm *roaring64.Bitmap, card uint64) bool {
		if trace != nil {
			trace.addMatched(card)
		}
		if cursor.skip >= card {
			cursor.skip -= card
			return false
		}
		return cursor.emitBitmap(bm)
	}

	emitBucket := func(bucket postingList) bool {
		if bucket.IsEmpty() {
			return false
		}
		scanWidth++
		card := bucket.Cardinality()
		if trace != nil {
			trace.addExamined(card)
		}

		if len(active) == 0 {
			if bucket.isSingleton() {
				return emitMatched(bucket.single)
			}
			if bm := bucket.bitmap(); bm != nil {
				return emitMatchedBitmap(bm, card)
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

		if bucket.isSingleton() {
			idx := bucket.single
			for _, pi := range active {
				p := preds[pi]
				if !p.hasContains() || !p.matches(idx) {
					return false
				}
			}
			return emitMatched(idx)
		}

		var (
			bucketBM    *roaring64.Bitmap
			bucketOwned bool
			bucketReady bool
		)
		ensureBucketBM := func() *roaring64.Bitmap {
			if bucketReady {
				return bucketBM
			}
			bucketBM, bucketOwned = bucket.ToBitmapOwned(bucketScratch)
			if bucketOwned && trace != nil {
				trace.addBitmapMaterialized(1)
			}
			bucketReady = true
			return bucketBM
		}
		defer func() {
			if bucketOwned && bucketBM != nil && bucketBM != bucketScratch {
				releaseRoaringBuf(bucketBM)
			}
		}()

		exactApplied := false
		iterSrc := bucket.Iter()
		defer func() {
			releaseRoaringIter(iterSrc)
		}()
		if len(exactActive) > 0 {
			allowExact := plannerAllowOrderedExactBitmapFilter(cursor.skip, cursor.need, card, exactOnly, len(exactActive))
			mode, exactBM, _ := plannerFilterBitmapByChecks(preds, exactActive, ensureBucketBM(), exactWork, allowExact)
			switch mode {
			case plannerPredicateBucketEmpty:
				return false
			case plannerPredicateBucketAll:
				if exactOnly {
					return emitMatchedBitmap(exactBM, card)
				}
				releaseRoaringIter(iterSrc)
				iterSrc = exactBM.Iterator()
				exactApplied = true
			case plannerPredicateBucketExact:
				if trace != nil {
					trace.addBitmapExactFilter(1)
				}
				if exactBM == nil || exactBM.IsEmpty() {
					return false
				}
				if exactOnly {
					return emitMatchedBitmap(exactBM, exactBM.GetCardinality())
				}
				releaseRoaringIter(iterSrc)
				iterSrc = exactBM.Iterator()
				exactApplied = true
			}
		}

		if !exactApplied && singleActive >= 0 && cursor.skip > 0 {
			cnt, ok := preds[singleActive].countBucket(ensureBucketBM())
			if ok && uint64(cnt) <= cursor.skip {
				cursor.skip -= uint64(cnt)
				if trace != nil {
					trace.addMatched(uint64(cnt))
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

func (db *DB[K, V]) collectOrderRangeBounds(field string, n int, exprAt func(i int) qx.Expr) (rangeBounds, []bool, bool, bool) {
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
			k, isSlice, err := db.exprValueToIdxScalar(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
			if err != nil || isSlice {
				return rangeBounds{}, nil, false, false
			}
			switch e.Op {
			case qx.OpGT:
				rb.applyLo(k, false)
			case qx.OpGTE:
				rb.applyLo(k, true)
			case qx.OpLT:
				rb.applyHi(k, false)
			case qx.OpLTE:
				rb.applyHi(k, true)
			case qx.OpEQ:
				rb.applyLo(k, true)
				rb.applyHi(k, true)
			}
			markCovered(i)
			has = true
		case qx.OpPREFIX:
			p, isSlice, err := db.exprValueToIdxScalar(qx.Expr{Op: e.Op, Field: e.Field, Value: e.Value})
			if err != nil || isSlice {
				return rangeBounds{}, nil, false, false
			}
			rb.hasPrefix = true
			rb.prefix = p
			markCovered(i)
			has = true
		}
	}

	return rb, covered, has, true
}
