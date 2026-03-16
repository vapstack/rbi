package rbi

import (
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/vapstack/qx"
)

type leafPred struct {
	kind    leafPredKind
	posting postingList
	posts   []postingList
	estCard uint64
	release func()
}

type roaringIter interface {
	HasNext() bool
	Next() uint64
}

type leafPredKind uint8

const (
	leafPredKindEmpty leafPredKind = iota
	leafPredKindPosting
	leafPredKindPostsConcat
	leafPredKindPostsUnion
	leafPredKindPostsAll
)

func (p leafPred) hasIter() bool {
	switch p.kind {
	case leafPredKindEmpty, leafPredKindPosting, leafPredKindPostsConcat, leafPredKindPostsUnion, leafPredKindPostsAll:
		return true
	default:
		return false
	}
}

func (p leafPred) leadIterNeedsContainsCheck() bool {
	// HAS(list) uses one posting as iterator seed and must still validate
	// remaining terms for each candidate.
	return p.kind == leafPredKindPostsAll && len(p.posts) > 1
}

func (p leafPred) iterNew() roaringIter {
	switch p.kind {
	case leafPredKindEmpty:
		return emptyIter{}
	case leafPredKindPosting:
		return p.posting.Iter()
	case leafPredKindPostsConcat:
		return newPostingConcatIter(p.posts)
	case leafPredKindPostsUnion:
		return newPostingUnionIter(p.posts)
	case leafPredKindPostsAll:
		return p.posting.Iter()
	default:
		return emptyIter{}
	}
}

func (p leafPred) containsIdx(idx uint64) bool {
	switch p.kind {
	case leafPredKindPosting:
		return p.posting.Contains(idx)
	case leafPredKindPostsConcat, leafPredKindPostsUnion:
		for _, ids := range p.posts {
			if ids.Contains(idx) {
				return true
			}
		}
		return false
	case leafPredKindPostsAll:
		for _, ids := range p.posts {
			if !ids.Contains(idx) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func (db *DB[K, V]) tryLimitQuery(q *qx.QX, trace *queryTrace) ([]K, bool, PlanName, error) {
	if q.Limit == 0 || q.Offset != 0 {
		return nil, false, "", nil
	}
	if q.Expr.Not {
		return nil, false, "", nil
	}

	leaves, ok := extractAndLeaves(q.Expr)
	if !ok || len(leaves) == 0 {
		return nil, false, "", nil
	}

	if len(q.Order) == 1 && q.Order[0].Type == qx.OrderBasic {
		out, used, err := db.tryLimitQueryOrderBasic(q, leaves, trace)
		if !used {
			return nil, false, "", err
		}
		plan := PlanLimitOrderBasic
		if hasPrefixBoundForField(leaves, q.Order[0].Field) {
			plan = PlanLimitOrderPrefix
		}
		return out, true, plan, err
	}

	f, bounds, rest, ok, err := db.extractNoOrderBoundsAndRest(leaves)
	if err != nil {
		return nil, false, "", err
	}
	if ok {
		out, used, err := db.tryLimitQueryRangeNoOrderByField(q, f, bounds, rest, trace)
		if !used {
			return nil, false, "", err
		}
		return out, true, PlanLimitRangeNoOrder, err
	}

	out, used, err := db.tryLimitQueryNoOrder(q, leaves, trace)
	if !used {
		return nil, false, "", err
	}
	return out, true, PlanLimit, err
}

func (db *DB[K, V]) extractNoOrderBoundsAndRest(leaves []qx.Expr) (string, rangeBounds, []qx.Expr, bool, error) {
	var (
		f          string
		bounds     rangeBounds
		found      bool
		firstBound = -1
		boundPos   [8]int
		positions  = boundPos[:0]
		rest       []qx.Expr
	)

	for i, e := range leaves {
		if !isBoundOp(e.Op) {
			if rest != nil {
				rest = append(rest, e)
			} else if found {
				rest = make([]qx.Expr, 0, len(leaves)-len(positions))
				if firstBound > 0 {
					for j := 0; j < firstBound; j++ {
						if !isBoundOp(leaves[j].Op) {
							rest = append(rest, leaves[j])
						}
					}
				}
				rest = append(rest, e)
			}
			continue
		}
		if e.Not || e.Field == "" {
			return "", rangeBounds{}, nil, false, nil
		}
		if !found {
			found = true
			f = e.Field
			firstBound = i
		} else if e.Field != f {
			return "", rangeBounds{}, nil, false, nil
		}
		positions = append(positions, i)
	}
	if !found {
		return "", rangeBounds{}, nil, false, nil
	}
	if rest == nil && firstBound > 0 {
		rest = make([]qx.Expr, 0, firstBound)
		for j := 0; j < firstBound; j++ {
			if !isBoundOp(leaves[j].Op) {
				rest = append(rest, leaves[j])
			}
		}
	}

	bounds.has = true
	for _, i := range positions {
		e := leaves[i]
		k, isSlice, err := db.exprValueToIdxScalar(e)
		if err != nil {
			return "", rangeBounds{}, nil, true, err
		}
		if isSlice {
			return "", rangeBounds{}, nil, false, nil
		}
		switch e.Op {
		case qx.OpGT:
			bounds.applyLo(k, false)
		case qx.OpGTE:
			bounds.applyLo(k, true)
		case qx.OpLT:
			bounds.applyHi(k, false)
		case qx.OpLTE:
			bounds.applyHi(k, true)
		case qx.OpPREFIX:
			bounds.hasPrefix = true
			bounds.prefix = k
		}
	}

	return f, bounds, rest, true, nil
}

// tryUniqueEqNoOrder executes a direct no-order path for conjunctions that
// contain at least one positive EQ predicate on a unique scalar field.
//
// The goal is to keep EQ(unique) queries on the same fast path regardless of
// whether caller sets Max(1) explicitly.
func (db *DB[K, V]) tryUniqueEqNoOrder(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	if q == nil || q.Expr.Not || q.Offset != 0 || len(q.Order) != 0 {
		return nil, false, nil
	}

	leaves, ok := extractAndLeaves(q.Expr)
	if !ok || len(leaves) == 0 {
		return nil, false, nil
	}

	preds := make([]leafPred, 0, len(leaves))
	defer func() { releaseLeafPreds(preds) }()

	uniqueLead := -1
	sawEmpty := false

	for _, e := range leaves {
		lp, ok, err := db.buildLeafPred(e)
		if err != nil {
			return nil, true, err
		}
		if !ok {
			return nil, false, nil
		}
		preds = append(preds, lp)
		if lp.kind == leafPredKindEmpty {
			sawEmpty = true
		}

		if !db.isPositiveUniqueEqExpr(e) {
			continue
		}

		idx := len(preds) - 1
		if uniqueLead == -1 || preds[idx].estCard < preds[uniqueLead].estCard {
			uniqueLead = idx
		}
	}

	if uniqueLead < 0 {
		return nil, false, nil
	}
	if sawEmpty {
		if trace != nil {
			trace.setPlan(PlanUniqueEq)
			trace.setEarlyStopReason("empty_leaf")
		}
		return nil, true, nil
	}

	lead := preds[uniqueLead]
	iter := lead.iterNew()

	if trace != nil {
		trace.setPlan(PlanUniqueEq)
	}

	needAll := q.Limit == 0
	out := make([]K, 0, 1)
	cursor := db.newQueryCursor(out, 0, q.Limit, needAll, nil)

	var examined uint64
	for iter.HasNext() {
		idx := iter.Next()
		examined++

		pass := true
		for i := range preds {
			if i == uniqueLead {
				continue
			}
			if !preds[i].containsIdx(idx) {
				pass = false
				break
			}
		}
		if !pass {
			continue
		}

		if cursor.emit(idx) {
			if trace != nil {
				trace.addExamined(examined)
				trace.setEarlyStopReason("limit_reached")
			}
			return cursor.out, true, nil
		}
	}

	if trace != nil {
		trace.addExamined(examined)
		trace.setEarlyStopReason("input_exhausted")
	}
	return cursor.out, true, nil
}

func hasPrefixBoundForField(leaves []qx.Expr, field string) bool {
	for _, e := range leaves {
		if e.Not {
			continue
		}
		if e.Field == field && e.Op == qx.OpPREFIX {
			return true
		}
	}
	return false
}

func (db *DB[K, V]) tryLimitQueryNoOrder(q *qx.QX, leaves []qx.Expr, trace *queryTrace) ([]K, bool, error) {

	preds := make([]leafPred, 0, len(leaves))
	defer func() { releaseLeafPreds(preds) }()

	for _, e := range leaves {
		if isBoundOp(e.Op) {
			return nil, false, nil
		}
		lp, ok, err := db.buildLeafPred(e)
		if err != nil {
			return nil, true, err
		}
		if !ok {
			return nil, false, nil
		}
		if lp.kind == leafPredKindEmpty {
			// AND with an empty leaf is empty; avoid selecting a non-iterable lead.
			return nil, true, nil
		}
		preds = append(preds, lp)
	}

	leadIdx := pickLeadIndex(preds)
	if leadIdx < 0 {
		return nil, false, nil
	}
	lead := preds[leadIdx]
	leadNeedsCheck := lead.leadIterNeedsContainsCheck()

	limit := int(q.Limit)
	out := make([]K, 0, limit)
	cursor := db.newQueryCursor(out, 0, q.Limit, false, nil)

	iter := lead.iterNew()
	var examined uint64
	for iter.HasNext() {
		idx := iter.Next()
		examined++

		pass := true
		for i := range preds {
			if i == leadIdx && !leadNeedsCheck {
				continue
			}
			if !preds[i].containsIdx(idx) {
				pass = false
				break
			}
		}
		if !pass {
			continue
		}

		if cursor.emit(idx) {
			trace.addExamined(examined)
			trace.setEarlyStopReason("limit_reached")
			return cursor.out, true, nil
		}
	}

	trace.addExamined(examined)
	trace.setEarlyStopReason("input_exhausted")
	return cursor.out, true, nil
}

func (db *DB[K, V]) tryLimitQueryOrderBasic(q *qx.QX, leaves []qx.Expr, trace *queryTrace) ([]K, bool, error) {
	order := q.Order[0]

	f := order.Field
	if f == "" {
		return nil, false, nil
	}

	fm := db.fields[f]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}

	ov := db.fieldOverlay(f)
	if !ov.hasData() {
		if !db.hasFieldIndex(f) {
			return nil, false, nil
		}
		return nil, true, nil
	}

	bounds, rest, ok, err := db.extractBoundsForField(f, leaves)
	if err != nil {
		return nil, true, err
	}
	if !ok {
		return nil, false, nil
	}

	preds, ok, err := db.buildLeafPredsNoBounds(rest)
	if err != nil {
		return nil, true, err
	}
	if !ok {
		return nil, false, nil
	}
	defer func() { releaseLeafPreds(preds) }()
	if hasEmptyLeafPred(preds) {
		return nil, true, nil
	}

	br := ov.rangeForBounds(bounds)
	if br.baseStart >= br.baseEnd && br.deltaStart >= br.deltaEnd {
		return nil, true, nil
	}
	return db.scanLimitByOverlayBounds(q, ov, br, order.Desc, preds, trace), true, nil
}

func (db *DB[K, V]) tryLimitQueryRangeNoOrderByField(q *qx.QX, field string, bounds rangeBounds, rest []qx.Expr, trace *queryTrace) ([]K, bool, error) {

	fm := db.fields[field]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}
	ov := db.fieldOverlay(field)
	if !ov.hasData() {
		if !db.hasFieldIndex(field) {
			return nil, false, nil
		}
		return nil, true, nil
	}

	preds, ok, err := db.buildLeafPredsNoBounds(rest)
	if err != nil {
		return nil, true, err
	}
	if !ok {
		return nil, false, nil
	}
	defer func() { releaseLeafPreds(preds) }()
	if hasEmptyLeafPred(preds) {
		return nil, true, nil
	}

	br := ov.rangeForBounds(bounds)
	if br.baseStart >= br.baseEnd && br.deltaStart >= br.deltaEnd {
		return nil, true, nil
	}
	return db.scanLimitByOverlayBounds(q, ov, br, false, preds, trace), true, nil
}

func (db *DB[K, V]) buildLeafPredsNoBounds(rest []qx.Expr) ([]leafPred, bool, error) {
	preds := make([]leafPred, 0, len(rest))
	for _, e := range rest {
		if isBoundOp(e.Op) {
			releaseLeafPreds(preds)
			return nil, false, nil
		}
		p, ok, err := db.buildLeafPred(e)
		if err != nil {
			releaseLeafPreds(preds)
			return nil, true, err
		}
		if !ok {
			releaseLeafPreds(preds)
			return nil, false, nil
		}
		preds = append(preds, p)
	}
	return preds, true, nil
}

func (db *DB[K, V]) scanLimitByOverlayBounds(q *qx.QX, ov fieldOverlay, br overlayRange, desc bool, preds []leafPred, trace *queryTrace) []K {
	limit := int(q.Limit)
	out := make([]K, 0, limit)
	cursor := db.newQueryCursor(out, 0, q.Limit, false, nil)
	trackScanWidth := len(q.Order) == 1
	var (
		examined  uint64
		scanWidth uint64
	)

	emitCandidate := func(idx uint64) bool {
		examined++
		for _, p := range preds {
			if !p.containsIdx(idx) {
				return false
			}
		}
		return cursor.emit(idx)
	}
	emitBucketPosting := func(ids postingList) bool {
		if ids.IsEmpty() {
			return false
		}
		if ids.isSingleton() {
			return emitCandidate(ids.single)
		}
		it := ids.bitmap().Iterator()
		for it.HasNext() {
			if emitCandidate(it.Next()) {
				return true
			}
		}
		return false
	}

	keyCur := ov.newCursor(br, desc)
	if ov.delta == nil {
		for {
			_, ids, _, ok := keyCur.next()
			if !ok {
				break
			}
			if trackScanWidth && !ids.IsEmpty() {
				scanWidth++
			}
			if emitBucketPosting(ids) {
				trace.addExamined(examined)
				trace.addOrderScanWidth(scanWidth)
				trace.setEarlyStopReason("limit_reached")
				return cursor.out
			}
		}
		trace.addExamined(examined)
		if trackScanWidth {
			trace.addOrderScanWidth(scanWidth)
		}
		trace.setEarlyStopReason("input_exhausted")
		return cursor.out
	}

	for {
		_, baseBM, de, ok := keyCur.next()
		if !ok {
			break
		}
		ids, keep := composePostingRetained(baseBM, de)
		if ids.IsEmpty() {
			continue
		}
		if trackScanWidth {
			scanWidth++
		}
		if emitBucketPosting(ids) {
			if keep != nil {
				releaseRoaringBuf(keep)
			}
			trace.addExamined(examined)
			if trackScanWidth {
				trace.addOrderScanWidth(scanWidth)
			}
			trace.setEarlyStopReason("limit_reached")
			return cursor.out
		}
		if keep != nil {
			releaseRoaringBuf(keep)
		}
	}

	trace.addExamined(examined)
	if trackScanWidth {
		trace.addOrderScanWidth(scanWidth)
	}
	trace.setEarlyStopReason("input_exhausted")
	return cursor.out
}

func isBoundOp(op qx.Op) bool {
	switch op {
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
		return true
	default:
		return false
	}
}

func (db *DB[K, V]) extractBoundsForField(field string, leaves []qx.Expr) (rangeBounds, []qx.Expr, bool, error) {
	var b rangeBounds
	rest := make([]qx.Expr, 0, len(leaves))

	for _, e := range leaves {
		if !isBoundOp(e.Op) {
			rest = append(rest, e)
			continue
		}
		if e.Not || e.Field == "" {
			return b, nil, false, nil
		}
		if e.Field != field {
			return b, nil, false, nil
		}

		k, isSlice, err := db.exprValueToIdxScalar(e)
		if err != nil {
			return b, nil, true, err
		}
		if isSlice {
			return b, nil, false, nil
		}

		b.has = true

		switch e.Op {
		case qx.OpGT:
			b.applyLo(k, false)
		case qx.OpGTE:
			b.applyLo(k, true)
		case qx.OpLT:
			b.applyHi(k, false)
		case qx.OpLTE:
			b.applyHi(k, true)
		case qx.OpPREFIX:
			b.hasPrefix = true
			b.prefix = k
		}
	}

	return b, rest, true, nil
}

func applyBoundsToIndexRange(s []index, b rangeBounds) (start, end int) {
	start = 0
	end = len(s)

	if b.hasPrefix {
		p := b.prefix
		start = lowerBoundIndex(s, p)
		end = prefixRangeEndIndex(s, p, start)
		return start, end
	}

	if b.hasLo {
		start = lowerBoundIndex(s, b.loKey)
		if !b.loInc {
			if start < len(s) && indexKeyEqualsString(s[start].Key, b.loKey) {
				start++
			}
		}
	}
	if b.hasHi {
		if b.hiInc {
			end = upperBoundIndex(s, b.hiKey)
		} else {
			end = lowerBoundIndex(s, b.hiKey)
		}
	}

	if start < 0 {
		start = 0
	}
	if end > len(s) {
		end = len(s)
	}
	if start >= end {
		return 0, 0
	}
	return start, end
}

func (db *DB[K, V]) buildLeafPred(e qx.Expr) (leafPred, bool, error) {
	if e.Not || e.Field == "" {
		return leafPred{}, false, nil
	}

	ov := db.fieldOverlay(e.Field)
	if !ov.hasData() && !db.hasFieldIndex(e.Field) {
		return leafPred{}, false, nil
	}

	fm := db.fields[e.Field]
	if fm == nil {
		return leafPred{}, false, nil
	}

	switch e.Op {

	case qx.OpEQ:
		if fm.Slice {
			return leafPred{}, false, nil
		}

		key, isSlice, err := db.exprValueToIdxScalar(e)
		if err != nil {
			return leafPred{}, true, err
		}
		if isSlice {
			return leafPred{}, false, nil
		}

		ids, keep := ov.lookupPostingRetained(key)
		if ids.IsEmpty() {
			return emptyLeaf(), true, nil
		}

		var release func()
		if keep != nil {
			retained := keep
			release = func() { releaseRoaringBuf(retained) }
		}

		return leafPred{
			kind:    leafPredKindPosting,
			posting: ids,
			estCard: ids.Cardinality(),
			release: release,
		}, true, nil

	case qx.OpIN:
		if fm.Slice {
			return leafPred{}, false, nil
		}

		keys, isSlice, err := db.exprValueToIdx(e)
		if err != nil {
			return leafPred{}, true, err
		}
		if !isSlice || len(keys) == 0 {
			return leafPred{}, false, nil
		}

		posts, est, release := ov.lookupPostings(keys)
		if len(posts) == 0 {
			if release != nil {
				release()
			}
			return emptyLeaf(), true, nil
		}

		return leafPred{
			kind:    leafPredKindPostsConcat,
			posts:   posts,
			estCard: est,
			release: release,
		}, true, nil

	case qx.OpHAS:
		if !fm.Slice {
			return leafPred{}, false, nil
		}

		keys, isSlice, err := db.exprValueToIdx(e)
		if err != nil {
			return leafPred{}, true, err
		}
		if !isSlice || len(keys) == 0 {
			return leafPred{}, false, nil
		}

		postsBuf := getPostingListSliceBuf(len(keys))
		posts := postsBuf.values

		var ownedInline [8]*roaring64.Bitmap
		ownedBMs := ownedInline[:0]
		var est uint64
		for _, k := range keys {
			ids, keep := ov.lookupPostingRetained(k)
			if ids.IsEmpty() {
				if len(ownedBMs) > 0 {
					releaseOwnedBitmapSlice(ownedBMs)
				}
				postsBuf.values = posts
				releasePostingListSliceBuf(postsBuf)
				return emptyLeaf(), true, nil
			}
			posts = append(posts, ids)
			if keep != nil {
				ownedBMs = append(ownedBMs, keep)
			}
			c := ids.Cardinality()
			if est == 0 || c < est {
				est = c
			}
		}

		lead := minCardPosting(posts)
		var release func()
		release = func() {
			if len(ownedBMs) > 0 {
				releaseOwnedBitmapSlice(ownedBMs)
			}
			postsBuf.values = posts
			releasePostingListSliceBuf(postsBuf)
		}

		return leafPred{
			kind:    leafPredKindPostsAll,
			posting: lead,
			posts:   posts,
			estCard: est,
			release: release,
		}, true, nil

	case qx.OpHASANY:
		if !fm.Slice {
			return leafPred{}, false, nil
		}

		keys, isSlice, err := db.exprValueToIdx(e)
		if err != nil {
			return leafPred{}, true, err
		}
		if !isSlice || len(keys) == 0 {
			return leafPred{}, false, nil
		}

		posts, est, release := ov.lookupPostings(keys)
		if len(posts) == 0 {
			if release != nil {
				release()
			}
			return emptyLeaf(), true, nil
		}

		return leafPred{
			kind:    leafPredKindPostsUnion,
			posts:   posts,
			estCard: est,
			release: release,
		}, true, nil
	}

	return leafPred{}, false, nil
}

func pickLeadIndex(ps []leafPred) int {
	if len(ps) == 0 {
		return -1
	}
	best := -1
	for i := range ps {
		if !ps[i].hasIter() {
			continue
		}
		if best < 0 || ps[i].estCard < ps[best].estCard {
			best = i
		}
	}
	return best
}

func hasEmptyLeafPred(preds []leafPred) bool {
	for i := range preds {
		if preds[i].kind == leafPredKindEmpty {
			return true
		}
	}
	return false
}

func extractAndLeaves(e qx.Expr) ([]qx.Expr, bool) {
	n, ok := countExtractAndLeaves(e)
	if !ok {
		return nil, false
	}
	if n == 0 {
		return nil, true
	}
	out := make([]qx.Expr, 0, n)
	out, ok = appendExtractAndLeaves(out, e)
	if !ok {
		return nil, false
	}
	return out, true
}

func countExtractAndLeaves(e qx.Expr) (int, bool) {
	switch e.Op {
	case qx.OpNOOP:
		return 0, false
	case qx.OpAND:
		if len(e.Operands) == 0 {
			return 0, false
		}
		total := 0
		for _, ch := range e.Operands {
			n, ok := countExtractAndLeaves(ch)
			if !ok {
				return 0, false
			}
			total += n
		}
		return total, true
	default:
		if e.Not {
			return 0, false
		}
		return 1, true
	}
}

func appendExtractAndLeaves(dst []qx.Expr, e qx.Expr) ([]qx.Expr, bool) {
	switch e.Op {
	case qx.OpNOOP:
		return nil, false
	case qx.OpAND:
		if len(e.Operands) == 0 {
			return nil, false
		}
		for _, ch := range e.Operands {
			var ok bool
			dst, ok = appendExtractAndLeaves(dst, ch)
			if !ok {
				return nil, false
			}
		}
		return dst, true
	default:
		if e.Not {
			return nil, false
		}
		return append(dst, e), true
	}
}

func minCardPosting(posts []postingList) postingList {
	if len(posts) == 0 {
		return postingList{}
	}
	best := posts[0]
	bestC := best.Cardinality()
	for i := 1; i < len(posts); i++ {
		if c := posts[i].Cardinality(); c < bestC {
			best = posts[i]
			bestC = c
		}
	}
	return best
}

func emptyLeaf() leafPred {
	return leafPred{
		kind:    leafPredKindEmpty,
		estCard: 0,
		release: nil,
	}
}

func releaseLeafPreds(preds []leafPred) {
	for i := range preds {
		if preds[i].release != nil {
			preds[i].release()
		}
	}
}

type emptyIter struct{}

func (emptyIter) HasNext() bool { return false }
func (emptyIter) Next() uint64  { return 0 }

type postingConcatIter struct {
	posts []postingList
	i     int
	curIt roaringIter
}

func newPostingConcatIter(posts []postingList) roaringIter {
	return &postingConcatIter{posts: posts}
}

func (it *postingConcatIter) HasNext() bool {
	for {
		if it.curIt != nil && it.curIt.HasNext() {
			return true
		}
		if it.i >= len(it.posts) {
			return false
		}
		p := it.posts[it.i]
		it.i++
		if p.IsEmpty() {
			continue
		}
		it.curIt = p.Iter()
	}
}

func (it *postingConcatIter) Next() uint64 {
	if !it.HasNext() {
		return 0
	}
	return it.curIt.Next()
}

type postingUnionIter struct {
	posts []postingList
	i     int
	curIt roaringIter
	seen  u64set
	next  uint64
	has   bool
}

type postingSmallUnionIter struct {
	posts []postingList
	i     int
	curIt roaringIter
	next  uint64
	has   bool
}

func newPostingUnionIter(posts []postingList) roaringIter {
	if len(posts) > 1 && len(posts) <= 3 {
		return &postingSmallUnionIter{posts: posts}
	}
	capHint := len(posts) * 16
	if capHint < 64 {
		capHint = 64
	}
	if capHint > 1024 {
		capHint = 1024
	}
	return &postingUnionIter{
		posts: posts,
		seen:  newU64Set(capHint),
	}
}

func (u *postingSmallUnionIter) HasNext() bool {
	if u.has {
		return true
	}
	for {
		if u.curIt != nil {
			for u.curIt.HasNext() {
				v := u.curIt.Next()
				dup := false
				for j := 0; j < u.i-1; j++ {
					if u.posts[j].Contains(v) {
						dup = true
						break
					}
				}
				if dup {
					continue
				}
				u.next = v
				u.has = true
				return true
			}
			u.curIt = nil
		}
		if u.i >= len(u.posts) {
			return false
		}
		p := u.posts[u.i]
		u.i++
		if p.IsEmpty() {
			continue
		}
		u.curIt = p.Iter()
	}
}

func (u *postingSmallUnionIter) Next() uint64 {
	if !u.HasNext() {
		return 0
	}
	u.has = false
	return u.next
}

func (u *postingUnionIter) HasNext() bool {
	if u.has {
		return true
	}
	for {
		if u.curIt != nil {
			for u.curIt.HasNext() {
				v := u.curIt.Next()
				if u.seen.Add(v) {
					u.next = v
					u.has = true
					return true
				}
			}
			u.curIt = nil
		}
		if u.i >= len(u.posts) {
			return false
		}
		p := u.posts[u.i]
		u.i++
		if p.IsEmpty() {
			continue
		}
		u.curIt = p.Iter()
	}
}

func (u *postingUnionIter) Next() uint64 {
	if !u.HasNext() {
		return 0
	}
	u.has = false
	return u.next
}

type u64set struct {
	keys []uint64
	used []byte
	mask uint64
	n    int
}

func newU64Set(capHint int) u64set {
	n := 1
	for n < capHint*2 {
		n <<= 1
	}
	return u64set{
		keys: make([]uint64, n),
		used: make([]byte, n),
		mask: uint64(n - 1),
	}
}

func (s *u64set) Add(x uint64) bool {
	if s.n*2 >= len(s.keys) {
		s.grow()
	}
	i := mix64(x) & s.mask
	for {
		if s.used[i] == 0 {
			s.used[i] = 1
			s.keys[i] = x
			s.n++
			return true
		}
		if s.keys[i] == x {
			return false
		}
		i = (i + 1) & s.mask
	}
}

func (s *u64set) Has(x uint64) bool {
	if len(s.keys) == 0 {
		return false
	}
	i := mix64(x) & s.mask
	for {
		if s.used[i] == 0 {
			return false
		}
		if s.keys[i] == x {
			return true
		}
		i = (i + 1) & s.mask
	}
}

func (s *u64set) Len() int {
	return s.n
}

func (s *u64set) grow() {
	oldK := s.keys
	oldU := s.used

	n := len(oldK) << 1
	s.keys = make([]uint64, n)
	s.used = make([]byte, n)
	s.mask = uint64(n - 1)
	s.n = 0

	for i := 0; i < len(oldK); i++ {
		if oldU[i] != 0 {
			_ = s.Add(oldK[i])
		}
	}
}

func mix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}
