package qexec

import (
	"fmt"
	"reflect"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/rbitrace"
)

func shouldUseOnePassIntersectionPosting(a posting.List, b posting.List, need uint64, all bool) bool {
	if b.IsEmpty() || a.IsEmpty() {
		return false
	}
	if !all && need > 0 && need <= 1024 {
		return true
	}
	minCard := a.Cardinality()
	if bc := b.Cardinality(); bc < minCard {
		minCard = bc
	}
	return minCard <= iteratorThreshold*4
}

func emitArrayCountZeroBucket(cursor *queryCursor, resultBM, nonEmpty posting.List) bool {
	if resultBM.IsEmpty() {
		return false
	}

	card := resultBM.Cardinality()

	if nonEmpty.IsEmpty() {
		if cursor.skip >= card {
			cursor.skip -= card
			return false
		}
		it := resultBM.Iter()
		defer it.Release()
		for it.HasNext() {
			if cursor.emit(it.Next()) {
				return true
			}
		}
		return false
	}

	if cursor.skip > 0 {
		nonEmptyCount := nonEmpty.AndCardinality(resultBM)
		if nonEmptyCount >= card {
			return false
		}

		zeroCount := card - nonEmptyCount
		if cursor.skip >= zeroCount {
			cursor.skip -= zeroCount
			return false
		}

		if nonEmptyCount == 0 {
			it := resultBM.Iter()
			defer it.Release()
			for it.HasNext() {
				if cursor.emit(it.Next()) {
					return true
				}
			}
			return false
		}
	}

	resultIt := resultBM.Iter()
	defer resultIt.Release()

	nonEmptyIt := nonEmpty.Iter()
	defer nonEmptyIt.Release()

	var exclude uint64
	hasExclude := false

	for resultIt.HasNext() {
		idx := resultIt.Next()
		for {
			if !hasExclude {
				if !nonEmptyIt.HasNext() {
					if cursor.emit(idx) {
						return true
					}
					for resultIt.HasNext() {
						if cursor.emit(resultIt.Next()) {
							return true
						}
					}
					return false
				}
				exclude = nonEmptyIt.Next()
				hasExclude = true
			}
			if exclude < idx {
				hasExclude = false
				continue
			}
			break
		}
		if exclude == idx {
			hasExclude = false
			continue
		}
		if cursor.emit(idx) {
			return true
		}
	}
	return false
}

func tmpIntersectPosting(tmp posting.List, a, b posting.List) posting.List {
	tmp.Release()
	tmp = posting.List{}
	if a.IsEmpty() || b.IsEmpty() {
		return tmp
	}
	if a.Cardinality() > b.Cardinality() {
		a, b = b, a
	}
	return a.Clone().BuildAnd(b)
}

func makeOutSlice(cardinality, limit uint64) []uint64 {
	var out []uint64
	if limit > 0 {
		out = make([]uint64, 0, min(limit, cardinality))
	} else {
		out = make([]uint64, 0, cardinality)
	}
	return out
}

func shouldMaterializeOrderedAllNumericBuckets(
	qv *View,
	skip uint64,
	all bool,
	resultCard uint64,
) bool {
	return !qv.strKey && all && skip == 0 && resultCard >= 64_000
}

func appendMaterializedNumericPostingKeys(out []uint64, ids posting.List) []uint64 {
	if ids.IsEmpty() {
		return out
	}
	if idx, ok := ids.TrySingle(); ok {
		return append(out, idx)
	}
	card := int(ids.Cardinality())
	if card == 0 {
		return out
	}
	base := len(out)
	out = out[:base+card]

	it := ids.Iter()
	for i := 0; i < card; i++ {
		out[base+i] = it.Next()
	}
	it.Release()

	return out
}

func appendMaterializedNumericPostingResultKeys(
	out []uint64,
	ids posting.List,
	result postingResult,
) []uint64 {
	if ids.IsEmpty() {
		return out
	}

	if result.neg {
		matched := ids.Borrow().BuildAndNot(result.ids)
		out = appendMaterializedNumericPostingKeys(out, matched)
		matched.Release()
		return out
	}

	matched := ids.Borrow().BuildAnd(result.ids)
	out = appendMaterializedNumericPostingKeys(out, matched)
	matched.Release()

	return out
}

func emitPostingResultBucketToCursor(
	qv *View,
	cursor *queryCursor,
	tmp, ids posting.List,
	result postingResult,
) (posting.List, bool) {
	if ids.IsEmpty() {
		return tmp, false
	}

	if idx, ok := ids.TrySingle(); ok {
		if result.neg {
			if !result.ids.Contains(idx) {
				return tmp, cursor.emit(idx)
			}
			return tmp, false
		}
		if result.ids.Contains(idx) {
			return tmp, cursor.emit(idx)
		}
		return tmp, false
	}

	if !result.neg && !ids.Intersects(result.ids) {
		return tmp, false
	}

	if shouldUseOnePassPostingResultFilter(ids, result, cursor.need, cursor.all) {
		return tmp, qv.forEachPostingResultBucket(ids, result, cursor.emit)
	}

	tmp = tmpIntersectPosting(tmp, ids, result.ids)

	return tmp, cursor.emitPosting(tmp)
}

func shouldUseOnePassPostingResultFilter(ids posting.List, result postingResult, need uint64, all bool) bool {
	if result.neg {
		return true
	}
	return shouldUseOnePassIntersectionPosting(ids, result.ids, need, all)
}

func (qv *View) postingResultCardinality(result postingResult) uint64 {
	if !result.neg {
		return result.ids.Cardinality()
	}
	return qv.snap.Universe.Cardinality() - result.ids.Cardinality()
}

const orderBasicSingletonChunkCap = 1024

func emitOrderBasicSingletonChunk(cursor *queryCursor, result postingResult, singles []uint64, minID, maxID, resultMin, resultMax uint64, ordered bool) bool {
	if len(singles) == 0 || maxID < resultMin || minID > resultMax {
		return false
	}
	if ordered {
		for i := 0; i < len(singles); i++ {
			idx := singles[i]
			if result.ids.Contains(idx) && cursor.emit(idx) {
				return true
			}
		}
		return false
	}
	builder := newPostingUnionBuilder(true)
	for i := 0; i < len(singles); i++ {
		builder.addSingle(singles[i])
	}
	chunk := builder.finish(false)
	if chunk.IsEmpty() || !chunk.Intersects(result.ids) {
		chunk.Release()
		return false
	}
	chunk.Release()
	for i := 0; i < len(singles); i++ {
		idx := singles[i]
		if result.ids.Contains(idx) && cursor.emit(idx) {
			return true
		}
	}
	return false
}

func (qv *View) queryOrderBasicSingletonChunks(result postingResult, ov indexdata.FieldIndexView, br indexdata.FieldIndexRange, o qir.Order, resultCard, skip, need uint64) []uint64 {
	out := makeOutSlice(resultCard, need)
	cursor := newQueryCursor(out, skip, need, false, 0)

	resultMin, _ := result.ids.Minimum()
	resultMax, _ := result.ids.Maximum()

	var singlesBuf [orderBasicSingletonChunkCap]uint64
	singles := singlesBuf[:0]
	var minID uint64
	var maxID uint64
	inc := true
	dec := true

	var tmp posting.List
	cur := ov.NewCursor(br, o.Desc)
	for {
		ids, idx, single, ok := cur.NextPostingOrSingle()
		if !ok {
			break
		}
		if single {
			n := len(singles)
			if n == 0 {
				minID = idx
				maxID = idx
			} else {
				prev := singles[n-1]
				if idx < prev {
					inc = false
				} else if idx > prev {
					dec = false
				}
				if idx < minID {
					minID = idx
				} else if idx > maxID {
					maxID = idx
				}
			}
			singles = append(singles, idx)
			if len(singles) < orderBasicSingletonChunkCap {
				continue
			}
			if emitOrderBasicSingletonChunk(&cursor, result, singles, minID, maxID, resultMin, resultMax, inc || dec) {
				tmp.Release()
				return cursor.out
			}
			singles = singles[:0]
			inc = true
			dec = true
			continue
		}

		if len(singles) > 0 {
			if emitOrderBasicSingletonChunk(&cursor, result, singles, minID, maxID, resultMin, resultMax, inc || dec) {
				tmp.Release()
				return cursor.out
			}
			singles = singles[:0]
			inc = true
			dec = true
		}

		var done bool
		tmp, done = emitPostingResultBucketToCursor(qv, &cursor, tmp, ids, result)
		if done {
			tmp.Release()
			return cursor.out
		}
	}
	if len(singles) > 0 && emitOrderBasicSingletonChunk(&cursor, result, singles, minID, maxID, resultMin, resultMax, inc || dec) {
		tmp.Release()
		return cursor.out
	}
	tmp.Release()
	return cursor.out
}

func (qv *View) forEachPostingResultBucket(ids posting.List, result postingResult, fn func(uint64) bool) bool {
	if ids.IsEmpty() {
		return false
	}
	if !result.neg {
		return ids.ForEachIntersecting(result.ids, fn)
	}

	exclude := result.ids

	it := ids.Iter()
	defer it.Release()

	for it.HasNext() {
		idx := it.Next()
		if !exclude.IsEmpty() && exclude.Contains(idx) {
			continue
		}
		if fn(idx) {
			return true
		}
	}
	return false
}

func (qv *View) forEachPostingResultAll(result postingResult, fn func(uint64) bool) bool {
	if !result.neg {
		it := result.ids.Iter()
		defer it.Release()

		for it.HasNext() {
			if fn(it.Next()) {
				return true
			}
		}
		return false
	}

	exclude := result.ids

	it := qv.snap.Universe.Borrow().Iter()
	defer it.Release()

	for it.HasNext() {
		idx := it.Next()
		if !exclude.IsEmpty() && exclude.Contains(idx) {
			continue
		}
		if fn(idx) {
			return true
		}
	}
	return false
}

func (qv *View) emitArrayCountZeroBucketResult(cursor *queryCursor, result postingResult, nonEmpty posting.List) bool {
	if !result.neg {
		return emitArrayCountZeroBucket(cursor, result.ids, nonEmpty)
	}

	universeCard := qv.snap.Universe.Cardinality()
	if universeCard == 0 {
		return false
	}

	zeroCount := universeCard
	if !nonEmpty.IsEmpty() {
		zeroCount -= nonEmpty.Cardinality()
	}
	if zeroCount == 0 {
		return false
	}

	excludedZero := result.ids.Cardinality()
	if !nonEmpty.IsEmpty() {
		excludedZero -= result.ids.AndCardinality(nonEmpty)
	}
	if excludedZero >= zeroCount {
		return false
	}

	zeroCount -= excludedZero
	if cursor.skip >= zeroCount {
		cursor.skip -= zeroCount
		return false
	}

	exclude := result.ids

	it := qv.snap.Universe.Borrow().Iter()
	defer it.Release()

	for it.HasNext() {
		idx := it.Next()
		if !exclude.IsEmpty() && exclude.Contains(idx) {
			continue
		}
		if !nonEmpty.IsEmpty() && nonEmpty.Contains(idx) {
			continue
		}
		if cursor.emit(idx) {
			return true
		}
	}
	return false
}

func (qv *View) queryOrderBasic(result postingResult, ov indexdata.FieldIndexView, o qir.Order, skip, need uint64, all bool) ([]uint64, error) {
	br := ov.RangeForBounds(indexdata.Bounds{Has: true})
	return qv.queryOrderBasicRange(result, ov, br, o, skip, need, all, true)
}

func (qv *View) queryOrderBasicRange(result postingResult, ov indexdata.FieldIndexView, br indexdata.FieldIndexRange, o qir.Order, skip, need uint64, all bool, includeNilTail bool) ([]uint64, error) {
	resultCard := qv.postingResultCardinality(result)
	if resultCard == 0 {
		return nil, nil
	}
	if skip >= resultCard {
		return nil, nil
	}
	if !all {
		if remaining := resultCard - skip; need > remaining {
			need = remaining
		}
	}

	fm := qv.fieldMetaByOrdinal(o.FieldOrdinal)
	isSliceOrderField := fm != nil && fm.Slice
	if !result.neg && !isSliceOrderField && !all && need > 0 && resultCard <= 4096 {
		if !schema.FieldUsesNilIndex(fm) && uint64(ov.KeyCount()) >= resultCard*8 {
			return qv.queryOrderBasicSingletonChunks(result, ov, br, o, resultCard, skip, need), nil
		}
	}

	var tmp posting.List

	out := makeOutSlice(resultCard, need)
	if !isSliceOrderField && shouldMaterializeOrderedAllNumericBuckets(qv, skip, all, resultCard) {
		cur := ov.NewCursor(br, o.Desc)
		for {
			_, ids, ok := cur.Next()
			if !ok {
				break
			}
			out = appendMaterializedNumericPostingResultKeys(out, ids, result)
		}
		if includeNilTail && schema.FieldUsesNilIndex(fm) {
			out = appendMaterializedNumericPostingResultKeys(
				out,
				qv.nilIndexViewByOrdinal(o.FieldOrdinal).LookupPostingRetained(indexdata.NilIndexEntryKey),
				result,
			)
		}
		return out, nil
	}

	dedupeCap := uint64(0)
	if isSliceOrderField {
		dedupeCap = queryCursorDedupeCap(resultCard, skip, need, all)
	}

	cursor := newQueryCursor(out, skip, need, all, dedupeCap)
	defer cursor.release()

	cur := ov.NewCursor(br, o.Desc)
	for {
		_, ids, ok := cur.Next()
		if !ok {
			break
		}
		var done bool
		tmp, done = emitPostingResultBucketToCursor(qv, &cursor, tmp, ids, result)
		if done {
			tmp.Release()
			return cursor.out, nil
		}
	}

	if includeNilTail && schema.FieldUsesNilIndex(fm) {
		nilIDs := qv.nilIndexViewByOrdinal(o.FieldOrdinal).LookupPostingRetained(indexdata.NilIndexEntryKey)
		if !nilIDs.IsEmpty() {
			var done bool
			tmp, done = emitPostingResultBucketToCursor(qv, &cursor, tmp, nilIDs, result)
			if done {
				tmp.Release()
				return cursor.out, nil
			}
		}
	}
	tmp.Release()

	return cursor.out, nil
}

func orderedDistinctLookupKeys(vals []keycodec.IndexLookupKey, desc bool) []keycodec.IndexLookupKey {
	if len(vals) < 2 {
		return vals
	}

	var fixed u64set
	var set map[string]struct{}

	write := 0
	for _, v := range vals {
		if v.IsNumeric() {
			if len(fixed.keys) == 0 {
				fixed = getU64Set(len(vals))
			}
			if !fixed.Add(v.U64()) {
				continue
			}
		} else {
			s := v.StringKey()
			if set == nil {
				set = stringSetPool.Get()
			}
			if _, ok := set[s]; ok {
				continue
			}
			set[s] = struct{}{}
		}
		vals[write] = v
		write++
	}
	if set != nil {
		stringSetPool.Put(set)
	}
	if len(fixed.keys) != 0 {
		releaseU64Set(&fixed)
	}
	out := vals[:write]
	if desc {
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}
	}
	return out
}

func scalarArrayPosPriorityCoversAllKeysIndexView(ov indexdata.FieldIndexView, vals []keycodec.IndexLookupKey) bool {
	keyCount := ov.KeyCount()
	if keyCount == 0 {
		return true
	}
	if len(vals) < keyCount {
		return false
	}

	var fixed u64set

	var set map[string]struct{}
	for _, v := range vals {
		if v.IsNumeric() {
			if len(fixed.keys) == 0 {
				fixed = getU64Set(len(vals))
			}
			fixed.Add(v.U64())
			continue
		}
		s := v.StringKey()
		if set == nil {
			set = stringSetPool.Get()
		}
		set[s] = struct{}{}
	}

	ok := true
	cur := ov.NewCursor(ov.RangeByRanks(0, keyCount), false)
	for {
		key, _, more := cur.Next()
		if !more {
			break
		}

		if key.IsNumeric() {
			if !fixed.Has(key.U64()) {
				ok = false
				break
			}
			continue
		}

		s := key.UnsafeString()
		if set == nil {
			ok = false
			break
		}
		if _, found := set[s]; !found {
			ok = false
			break
		}
	}

	if set != nil {
		stringSetPool.Put(set)
	}
	if len(fixed.keys) != 0 {
		releaseU64Set(&fixed)
	}
	return ok
}

func scalarArrayPosPriorityCoversAllResultsIndexView(resultBM posting.List, ov, nilOV indexdata.FieldIndexView, vals []keycodec.IndexLookupKey, hasNilPriority bool) bool {
	if !scalarArrayPosPriorityCoversAllKeysIndexView(ov, vals) {
		return false
	}
	if !nilOV.HasData() || hasNilPriority {
		return true
	}
	return !nilOV.LookupPostingRetained(indexdata.NilIndexEntryKey).Intersects(resultBM)
}

func (qv *View) queryOrderArrayPosIndexView(result postingResult, ov indexdata.FieldIndexView, o qir.Order, skip, need uint64, all bool) ([]uint64, error) {
	resultCard := qv.postingResultCardinality(result)
	if resultCard == 0 {
		return nil, nil
	}

	vals, nilRank, err := qv.orderDataValues(o.Data, qv.fieldMetaByOrdinal(o.FieldOrdinal))
	if err != nil {
		return nil, err
	}
	if vals != nil {
		defer keycodec.ReleaseIndexLookupKeySlice(vals)
	}
	if fm := qv.fieldMetaByOrdinal(o.FieldOrdinal); fm != nil && !fm.Slice {
		return qv.queryOrderArrayPosScalarIndexView(result, qv.exec.FieldNameByOrdinal(o.FieldOrdinal), o.FieldOrdinal, ov, vals, nilRank, o.Desc, skip, need, all)
	}

	out := makeOutSlice(resultCard, need)

	if !o.Desc {
		var tmp posting.List
		cursor := newQueryCursor(out, skip, need, all, queryCursorDedupeCap(resultCard, skip, need, all))
		defer cursor.release()

		for _, v := range vals {
			var done bool
			tmp, done = emitPostingResultBucketToCursor(qv, &cursor, tmp, lookupScalarPostingRetained(ov, v), result)
			if done {
				tmp.Release()
				return cursor.out, nil
			}
		}

		qv.forEachPostingResultAll(result, cursor.emit)

		tmp.Release()
		return cursor.out, nil
	}

	cursor := newQueryCursor(out, skip, need, all, 0)
	buckets := posting.GetSlice(len(vals))
	var assigned posting.List
	var assignedCard uint64
	for i := 0; i < len(vals); i++ {
		ids := lookupScalarPostingRetained(ov, vals[i])
		if ids.IsEmpty() {
			continue
		}

		bucket := ids.Borrow()
		if result.neg {
			bucket = bucket.BuildAndNot(result.ids)
		} else {
			bucket = bucket.BuildAnd(result.ids)
		}
		if bucket.IsEmpty() {
			continue
		}
		if !assigned.IsEmpty() {
			bucket = bucket.BuildAndNot(assigned)
			if bucket.IsEmpty() {
				continue
			}
		}
		if assigned.IsEmpty() {
			assigned = bucket.Borrow()
		} else {
			assigned = assigned.BuildOr(bucket)
		}
		assignedCard += bucket.Cardinality()
		buckets = append(buckets, bucket)
		if assignedCard == resultCard {
			break
		}
	}

	for i := len(buckets) - 1; i >= 0; i-- {
		if cursor.emitPosting(buckets[i]) {
			assigned.Release()
			posting.ReleaseAll(buckets)
			posting.ReleaseSlice(buckets)
			return cursor.out, nil
		}
	}

	if assignedCard != resultCard {
		skipAssigned := assignedCard != 0
		if !result.neg {
			it := result.ids.Iter()
			for it.HasNext() {
				idx := it.Next()
				if skipAssigned && assigned.Contains(idx) {
					continue
				}
				if cursor.emit(idx) {
					break
				}
			}
			it.Release()
		} else {
			exclude := result.ids
			it := qv.snap.Universe.Borrow().Iter()
			for it.HasNext() {
				idx := it.Next()
				if !exclude.IsEmpty() && exclude.Contains(idx) {
					continue
				}
				if skipAssigned && assigned.Contains(idx) {
					continue
				}
				if cursor.emit(idx) {
					break
				}
			}
			it.Release()
		}
	}

	assigned.Release()
	posting.ReleaseAll(buckets)
	posting.ReleaseSlice(buckets)

	return cursor.out, nil
}

type plannerArrayPosOrderCandidateKind uint8

const (
	plannerArrayPosOrderCandidateNone plannerArrayPosOrderCandidateKind = iota
	plannerArrayPosOrderCandidateSingleHasAny
	plannerArrayPosOrderCandidateMaterializedFallback
)

func (k plannerArrayPosOrderCandidateKind) String() string {
	switch k {
	case plannerArrayPosOrderCandidateSingleHasAny:
		return "single_hasany"
	case plannerArrayPosOrderCandidateMaterializedFallback:
		return "materialized_fallback"
	default:
		return ""
	}
}

type plannerArrayPosOrderCandidate struct {
	kind         plannerArrayPosOrderCandidateKind
	cost         float64
	expectedRows uint64
}

type plannerArrayPosOrderDecision struct {
	selected             plannerArrayPosOrderCandidate
	materializedFallback plannerArrayPosOrderCandidate
	rejected             plannerArrayPosOrderCandidate
}

func (d plannerArrayPosOrderDecision) traceRoute() rbitrace.ArrayPosOrderRoute {
	return rbitrace.ArrayPosOrderRoute{
		Selected:     d.selected.kind.String(),
		Rejected:     d.rejected.kind.String(),
		SelectedCost: d.selected.cost,
		RejectedCost: d.rejected.cost,
		ExpectedRows: d.selected.expectedRows,
	}
}

type plannerArrayPosOrderFacts struct {
	expr  qir.Expr
	order qir.Order
	fm    *schema.Field
	ov    indexdata.FieldIndexView
}

func (qv *View) collectArrayPosOrderFacts(q *qir.Shape, facts *plannerArrayPosOrderFacts) bool {
	if !q.HasOrder || q.Order.Kind != qir.OrderKindArrayPos {
		return false
	}

	e := q.Expr
	if e.Op == qir.OpHASANY {
		if e.Not || e.FieldOrdinal != q.Order.FieldOrdinal || len(e.Operands) != 0 {
			return false
		}
		facts.expr = e
	} else {
		if e.Op != qir.OpAND || e.Not || len(e.Operands) == 0 {
			return false
		}
		hasFilter := false
		for i := 0; i < len(e.Operands); i++ {
			op := e.Operands[i]
			if op.Op == qir.OpHASANY && !op.Not && op.FieldOrdinal == q.Order.FieldOrdinal && len(op.Operands) == 0 {
				if hasFilter {
					return false
				}
				facts.expr = op
				hasFilter = true
				continue
			}
			if !qv.arrayPosOrderResidualAlwaysTrue(op) {
				return false
			}
		}
		if !hasFilter {
			return false
		}
	}

	fm := qv.fieldMetaByOrdinal(facts.expr.FieldOrdinal)
	if fm == nil || !fm.Slice {
		return false
	}

	facts.order = q.Order
	facts.fm = fm
	facts.ov = qv.indexViewByOrdinal(q.Order.FieldOrdinal)
	return true
}

func (qv *View) arrayPosOrderResidualAlwaysTrue(e qir.Expr) bool {
	if e.Op == qir.OpConst {
		return !e.Not
	}
	if e.Not || len(e.Operands) != 0 || !e.Op.IsScalarRangeOrPrefix() || !qv.hasFieldOrdinal(e.FieldOrdinal) {
		return false
	}
	fm := qv.fieldMetaByOrdinal(e.FieldOrdinal)
	if fm == nil || fm.Slice {
		return false
	}
	ov := qv.indexViewByOrdinal(e.FieldOrdinal)
	if !ov.HasData() {
		return false
	}
	bounds, ok, err := qv.rangeBoundsForScalarExpr(e)
	if err != nil || !ok {
		return false
	}
	br := ov.RangeForBounds(bounds)
	if br.Empty() {
		return false
	}
	if br.BaseStart != 0 || br.BaseEnd != ov.KeyCount() || !br.ExactRankSpan() {
		return false
	}
	return !schema.FieldUsesNilIndex(fm) || qv.nilIndexViewByOrdinal(e.FieldOrdinal).LookupCardinality(indexdata.NilIndexEntryKey) == 0
}

func (qv *View) selectArrayPosOrder(q *qir.Shape) (plannerArrayPosOrderDecision, bool) {
	expected := q.Limit
	if expected == 0 {
		expected = qv.snap.Universe.Cardinality()
	}

	if q.Limit != 0 && q.Offset <= uint64(iteratorThreshold) && q.Limit <= uint64(iteratorThreshold)-q.Offset {
		return plannerArrayPosOrderDecision{
			selected: plannerArrayPosOrderCandidate{
				kind:         plannerArrayPosOrderCandidateMaterializedFallback,
				cost:         float64(expected),
				expectedRows: expected,
			},
			materializedFallback: plannerArrayPosOrderCandidate{
				kind:         plannerArrayPosOrderCandidateMaterializedFallback,
				cost:         float64(expected),
				expectedRows: expected,
			},
		}, true
	}

	return plannerArrayPosOrderDecision{
		selected: plannerArrayPosOrderCandidate{
			kind:         plannerArrayPosOrderCandidateSingleHasAny,
			cost:         float64(expected) * 0.75,
			expectedRows: expected,
		},
		materializedFallback: plannerArrayPosOrderCandidate{
			kind:         plannerArrayPosOrderCandidateMaterializedFallback,
			cost:         float64(expected),
			expectedRows: expected,
		},
		rejected: plannerArrayPosOrderCandidate{
			kind:         plannerArrayPosOrderCandidateMaterializedFallback,
			cost:         float64(expected),
			expectedRows: expected,
		},
	}, true
}

func (qv *View) executeArrayPosOrder(q *qir.Shape, trace *Trace) ([]uint64, bool, rbitrace.PlanName, error) {
	var facts plannerArrayPosOrderFacts
	if !qv.collectArrayPosOrderFacts(q, &facts) {
		return nil, false, "", nil
	}

	decision, ok := qv.selectArrayPosOrder(q)
	if !ok {
		return nil, false, "", nil
	}
	if trace != nil {
		trace.SetArrayPosOrderRoute(decision.traceRoute())
	}

	return qv.dispatchArrayPosOrder(q, &facts, decision, trace)
}

func (qv *View) dispatchArrayPosOrder(
	q *qir.Shape,
	facts *plannerArrayPosOrderFacts,
	decision plannerArrayPosOrderDecision,
	trace *Trace,
) ([]uint64, bool, rbitrace.PlanName, error) {
	dispatchShape := *q
	dispatchShape.Expr = facts.expr

	switch decision.selected.kind {
	case plannerArrayPosOrderCandidateMaterializedFallback:
		return qv.dispatchLimitMaterialized(&dispatchShape)
	case plannerArrayPosOrderCandidateSingleHasAny:
		out, used, err := qv.execSelectedArrayPosOrderSingleHasAny(q, facts, trace)
		if err != nil {
			return nil, true, "", err
		}
		if !used {
			if decision.materializedFallback.kind == plannerArrayPosOrderCandidateMaterializedFallback {
				return qv.dispatchLimitMaterialized(&dispatchShape)
			}
			return nil, true, "", fmt.Errorf("selected ArrayPos ORDER route %s was not executable", decision.selected.kind.String())
		}
		return out, true, rbitrace.PlanMaterialized, nil
	}
	return nil, false, "", nil
}

func (qv *View) execSelectedArrayPosOrderSingleHasAny(q *qir.Shape, facts *plannerArrayPosOrderFacts, trace *Trace) ([]uint64, bool, error) {
	valsBuf, isSlice, _, err := qv.exprValueToDistinctLookupKeyBuf(facts.expr)
	if valsBuf != nil {
		defer keycodec.ReleaseIndexLookupKeySlice(valsBuf)
	}
	if err != nil {
		return nil, false, err
	}
	if (!isSlice && facts.expr.Value != nil) || len(valsBuf) == 0 {
		return nil, false, nil
	}

	orderVals, _, err := qv.orderDataValues(facts.order.Data, facts.fm)
	if err != nil || len(orderVals) == 0 {
		return nil, false, err
	}
	defer keycodec.ReleaseIndexLookupKeySlice(orderVals)

	ov := facts.ov
	if !ov.HasData() && !qv.hasIndexedFieldOrdinal(facts.order.FieldOrdinal) {
		return nil, false, nil
	}

	var filterKey keycodec.IndexLookupKey
	var filterIDs posting.List
	filterSet := false
	defer func() {
		if filterSet {
			filterIDs.Release()
		}
	}()
	for i := 0; i < len(valsBuf); i++ {
		ids := lookupScalarPostingRetained(ov, valsBuf[i])
		if ids.IsEmpty() {
			ids.Release()
			continue
		}
		if filterSet {
			ids.Release()
			return nil, false, nil
		}
		filterKey = valsBuf[i]
		filterIDs = ids
		filterSet = true
	}
	if !filterSet {
		return nil, true, nil
	}

	filterCard := filterIDs.Cardinality()
	// Small bounded windows are cheaper on the existing materialized route; this
	// path is for avoiding large union/dedupe work.
	if q.Limit != 0 && filterCard <= iteratorThreshold*4 {
		return nil, false, nil
	}

	for i := 0; i < len(orderVals); i++ {
		v := orderVals[i]
		if compareLookupKey(v, filterKey) == 0 {
			return emitSinglePostingOrderArrayPosResult(filterIDs, filterCard, q.Offset, q.Limit, trace), true, nil
		}
		ids := lookupScalarPostingRetained(ov, v)
		intersects := !ids.IsEmpty() && ids.Intersects(filterIDs)
		ids.Release()
		if intersects {
			return nil, false, nil
		}
	}

	return emitSinglePostingOrderArrayPosResult(filterIDs, filterCard, q.Offset, q.Limit, trace), true, nil
}

func emitSinglePostingOrderArrayPosResult(ids posting.List, card, skip, need uint64, trace *Trace) []uint64 {
	if ids.IsEmpty() {
		return nil
	}

	all := need == 0
	if skip == 0 && all {
		if trace != nil {
			trace.AddMatched(card)
		}
		return ids.ToArray()
	}
	if skip >= card {
		return nil
	}

	out := makeOutSlice(card, need)
	cursor := newQueryCursor(out, skip, need, all, 0)
	cursor.emitPosting(ids)
	if trace != nil {
		trace.AddMatched(uint64(len(cursor.out)))
	}
	return cursor.out
}

func (qv *View) queryOrderArrayPosScalarIndexView(result postingResult, field string, fieldOrdinal int, ov indexdata.FieldIndexView, vals []keycodec.IndexLookupKey, nilRank int, desc bool, skip, need uint64, all bool) ([]uint64, error) {
	resultCard := qv.postingResultCardinality(result)
	if resultCard == 0 {
		return nil, nil
	}

	nilOV := indexdata.FieldIndexView{}
	if schema.FieldUsesNilIndex(qv.fieldMeta(field, fieldOrdinal)) {
		nilOV = qv.nilIndexViewRef(field, fieldOrdinal)
	}

	orderedVals := orderedDistinctLookupKeys(vals, desc)
	hasNilPriority := nilRank >= 0
	if hasNilPriority && desc {
		nilRank = len(orderedVals) - nilRank
	}
	hasPriorities := len(orderedVals) > 0 || hasNilPriority
	coversAll := !result.neg && hasPriorities &&
		scalarArrayPosPriorityCoversAllResultsIndexView(result.ids, ov, nilOV, orderedVals, hasNilPriority)

	// Empty priorities still require a full fallback pass; they just do not need
	// duplicate tracking against priority buckets because none were emitted.
	needFallback := result.neg || !coversAll
	needSeen := needFallback && hasPriorities

	out := makeOutSlice(resultCard, need)
	if !needFallback &&
		!result.neg &&
		shouldMaterializeOrderedAllNumericBuckets(qv, skip, all, resultCard) {
		if !hasNilPriority {
			for _, v := range orderedVals {
				ids := lookupScalarPostingRetained(ov, v).Borrow().BuildAnd(result.ids)
				out = appendMaterializedNumericPostingKeys(out, ids)
				ids.Release()
			}
			return out, nil
		}
		for i := 0; i <= len(orderedVals); i++ {
			if i == nilRank {
				out = appendMaterializedNumericPostingResultKeys(
					out,
					nilOV.LookupPostingRetained(indexdata.NilIndexEntryKey),
					result,
				)
			}
			if i < len(orderedVals) {
				ids := lookupScalarPostingRetained(ov, orderedVals[i]).Borrow().BuildAnd(result.ids)
				out = appendMaterializedNumericPostingKeys(out, ids)
				ids.Release()
			}
		}
		return out, nil
	}

	dedupeCap := uint64(0)
	if needSeen {
		dedupeCap = queryCursorDedupeCap(resultCard, skip, need, all)
	}

	cursor := newQueryCursor(out, skip, need, all, dedupeCap)
	defer cursor.release()

	var tmp posting.List

	if !hasNilPriority {
		for _, v := range orderedVals {
			var done bool
			tmp, done = emitPostingResultBucketToCursor(qv, &cursor, tmp, lookupScalarPostingRetained(ov, v), result)
			if done {
				tmp.Release()
				return cursor.out, nil
			}
		}
	} else {
		for i := 0; i <= len(orderedVals); i++ {
			if i == nilRank {
				nilIDs := nilOV.LookupPostingRetained(indexdata.NilIndexEntryKey)
				var done bool
				tmp, done = emitPostingResultBucketToCursor(qv, &cursor, tmp, nilIDs, result)
				if done {
					tmp.Release()
					return cursor.out, nil
				}
			}
			if i < len(orderedVals) {
				var done bool
				tmp, done = emitPostingResultBucketToCursor(qv, &cursor, tmp, lookupScalarPostingRetained(ov, orderedVals[i]), result)
				if done {
					tmp.Release()
					return cursor.out, nil
				}
			}
		}
	}

	if !cursor.all && cursor.need == 0 {
		tmp.Release()
		return cursor.out, nil
	}

	if !needFallback {
		tmp.Release()
		return cursor.out, nil
	}

	qv.forEachPostingResultAll(result, cursor.emit)

	tmp.Release()

	return cursor.out, nil
}

const orderDataNilRankNone = -1

func (qv *View) orderDataValues(v any, fm *schema.Field) ([]keycodec.IndexLookupKey, int, error) {
	if vals, ok := v.([]string); ok {
		if len(vals) == 0 {
			return nil, orderDataNilRankNone, nil
		}
		out := keycodec.GetIndexLookupKeySlice(len(vals))
		for i := range vals {
			out = append(out, keycodec.IndexLookupString(vals[i]))
		}
		return out, orderDataNilRankNone, nil
	}

	if s, ok := v.(string); ok {
		out := keycodec.GetIndexLookupKeySlice(1)
		out = append(out, keycodec.IndexLookupString(s))
		return out, orderDataNilRankNone, nil
	}

	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return nil, orderDataNilRankNone, nil
	}
	rv, isNil := schema.UnwrapQueryValue(rv)
	if isNil {
		return nil, orderDataNilRankNone, nil
	}

	sliceOrArray := rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array
	if sliceOrArray && fm != nil && fm.UseVI && !fm.Slice {
		if _, ok := v.(schema.ValueIndexer); ok {
			sliceOrArray = false
		} else if rv.CanInterface() {
			if _, ok = rv.Interface().(schema.ValueIndexer); ok {
				sliceOrArray = false
			}
		}
	}

	if sliceOrArray {
		if rv.Len() == 0 {
			return nil, orderDataNilRankNone, nil
		}
		valsBuf := keycodec.GetIndexLookupKeySlice(rv.Len())
		nilRank := orderDataNilRankNone
		for i := 0; i < rv.Len(); i++ {
			elem := rv.Index(i)
			raw := any(nil)
			if elem.IsValid() && elem.CanInterface() {
				raw = elem.Interface()
			}

			elem, elemNil := schema.UnwrapQueryValue(elem)
			if elemNil {
				if nilRank < 0 {
					nilRank = 0
					var fixed u64set
					var set map[string]struct{}
					for j := 0; j < len(valsBuf); j++ {
						key := valsBuf[j]
						if key.IsNumeric() {
							if len(fixed.keys) == 0 {
								fixed = getU64Set(len(valsBuf))
							}
							if fixed.Add(key.U64()) {
								nilRank++
							}
							continue
						}

						s := key.StringKey()
						if set == nil {
							set = stringSetPool.Get()
						}
						if _, ok := set[s]; ok {
							continue
						}
						set[s] = struct{}{}
						nilRank++
					}
					if set != nil {
						stringSetPool.Put(set)
					}
					if len(fixed.keys) != 0 {
						releaseU64Set(&fixed)
					}
				}
				continue
			}

			if fm != nil && fm.UseVI {
				// ValueIndexer elements stay scalar even when their underlying kind is slice.
				if vi, ok := raw.(schema.ValueIndexer); ok {
					valsBuf = append(valsBuf, keycodec.IndexLookupString(vi.IndexingValue()))
					continue
				}
				if elem.IsValid() && elem.CanInterface() {
					if vi, ok := elem.Interface().(schema.ValueIndexer); ok {
						valsBuf = append(valsBuf, keycodec.IndexLookupString(vi.IndexingValue()))
						continue
					}
				}
			}

			if elem.Kind() == reflect.Slice {
				keycodec.ReleaseIndexLookupKeySlice(valsBuf)
				return nil, orderDataNilRankNone, fmt.Errorf("unsupported slice element type: %v", elem.Type())
			}
			key, err := scalarValueToLookupKeyField(raw, elem, fm)
			if err != nil {
				keycodec.ReleaseIndexLookupKeySlice(valsBuf)
				return nil, orderDataNilRankNone, err
			}
			valsBuf = append(valsBuf, key)
		}
		if len(valsBuf) == 0 {
			keycodec.ReleaseIndexLookupKeySlice(valsBuf)
			return nil, nilRank, nil
		}
		return valsBuf, nilRank, nil
	}

	key, err := scalarValueToLookupKeyField(v, rv, fm)
	if err != nil {
		return nil, orderDataNilRankNone, err
	}
	out := keycodec.GetIndexLookupKeySlice(1)
	out = append(out, key)
	return out, orderDataNilRankNone, nil
}

func (qv *View) queryOrderArrayCount(result postingResult, ov indexdata.FieldIndexView, o qir.Order, skip, need uint64, all bool, useZeroComplement bool) ([]uint64, error) {
	resultCard := qv.postingResultCardinality(result)
	if resultCard == 0 {
		return nil, nil
	}

	out := makeOutSlice(resultCard, need)

	var tmp posting.List

	cursor := newQueryCursor(out, skip, need, all, 0)
	defer cursor.release()

	var nonEmpty posting.List

	if useZeroComplement {
		nonEmpty = ov.LookupPostingRetained(indexdata.LenIndexNonEmptyKey)
	}

	if !result.neg && shouldMaterializeOrderedAllNumericBuckets(qv, skip, all, resultCard) {
		br := ov.RangeByRanks(0, ov.KeyCount())
		if !o.Desc {
			if useZeroComplement {
				ids := result.ids.Borrow().BuildAndNot(nonEmpty)
				out = appendMaterializedNumericPostingKeys(out, ids)
				ids.Release()
			}
			cur := ov.NewCursor(br, false)
			for {
				key, ids, ok := cur.Next()
				if !ok {
					break
				}
				if keycodec.EqualsString(key, indexdata.LenIndexNonEmptyKey) || ids.IsEmpty() {
					continue
				}
				out = appendMaterializedNumericPostingResultKeys(out, ids, result)
			}
			return out, nil
		}

		cur := ov.NewCursor(br, true)
		for {
			key, ids, ok := cur.Next()
			if !ok {
				break
			}
			if keycodec.EqualsString(key, indexdata.LenIndexNonEmptyKey) || ids.IsEmpty() {
				continue
			}
			out = appendMaterializedNumericPostingResultKeys(out, ids, result)
		}

		if useZeroComplement {
			ids := result.ids.Borrow().BuildAndNot(nonEmpty)
			out = appendMaterializedNumericPostingKeys(out, ids)
			ids.Release()
		}

		return out, nil
	}

	br := ov.RangeByRanks(0, ov.KeyCount())
	if !o.Desc {
		if useZeroComplement && qv.emitArrayCountZeroBucketResult(&cursor, result, nonEmpty) {
			tmp.Release()
			return cursor.out, nil
		}

		cur := ov.NewCursor(br, false)
		for {
			key, ids, ok := cur.Next()
			if !ok {
				break
			}
			if keycodec.EqualsString(key, indexdata.LenIndexNonEmptyKey) {
				continue
			}
			if ids.IsEmpty() {
				continue
			}

			var done bool
			tmp, done = emitPostingResultBucketToCursor(qv, &cursor, tmp, ids, result)
			if done {
				tmp.Release()
				return cursor.out, nil
			}
		}

	} else {
		cur := ov.NewCursor(br, true)
		for {
			key, ids, ok := cur.Next()
			if !ok {
				break
			}
			if keycodec.EqualsString(key, indexdata.LenIndexNonEmptyKey) {
				continue
			}
			if ids.IsEmpty() {
				continue
			}

			var done bool
			tmp, done = emitPostingResultBucketToCursor(qv, &cursor, tmp, ids, result)
			if done {
				tmp.Release()
				return cursor.out, nil
			}
		}

		if useZeroComplement && qv.emitArrayCountZeroBucketResult(&cursor, result, nonEmpty) {
			tmp.Release()
			return cursor.out, nil
		}
	}

	tmp.Release()
	return cursor.out, nil
}
