package rbi

import (
	"fmt"
	"strings"
	"unsafe"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/normalize"
	"github.com/vapstack/rbi/internal/posting"
)

func (qv *queryView[K, V]) tryExecutionPlan(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	// Execution-plan fast paths support only single-column basic order.
	// Non-basic order types must stay on planner/postingResult routes.
	if len(q.Order) > 0 {
		if len(q.Order) != 1 || q.Order[0].Type != qx.OrderBasic {
			return nil, false, nil
		}
	}

	// optimized path for LIMIT without OFFSET
	if out, ok, plan, err := qv.tryLimitQuery(q, trace); ok {
		if trace != nil {
			trace.setPlan(plan)
		}
		return out, ok, err
	}

	// optimization for simple ORDER + LIMIT without complex filters (and OFFSET)
	if out, ok, err := qv.tryQueryOrderBasicWithLimit(q, trace); ok {
		if trace != nil {
			trace.setPlan(PlanLimitOrderBasic)
		}
		return out, ok, err
	}

	// optimization for PREFIX + ORDER + LIMIT without complex filters
	if out, ok, err := qv.tryQueryOrderPrefixWithLimit(q, trace); ok {
		if trace != nil {
			trace.setPlan(PlanLimitOrderPrefix)
		}
		return out, ok, err
	}

	// optimization for simple PREFIX + LIMIT without ORDER
	if out, ok, err := qv.tryQueryPrefixNoOrderWithLimit(q, trace); ok {
		if trace != nil {
			trace.setPlan(PlanLimitPrefixNoOrder)
		}
		return out, ok, err
	}

	// optimization for simple range + LIMIT without ORDER
	if out, ok, err := qv.tryQueryRangeNoOrderWithLimit(q, trace); ok {
		if trace != nil {
			trace.setPlan(PlanLimitRangeNoOrder)
		}
		return out, ok, err
	}

	return nil, false, nil
}

func emitAcceptedPostingNoOrder[K ~uint64 | ~string, V any](cursor *queryCursor[K, V], ids posting.List, examined *uint64) bool {
	if ids.IsEmpty() {
		return false
	}
	card := ids.Cardinality()
	*examined += card
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

type rangeBounds struct {
	has bool

	empty bool

	hasLo bool
	loKey string
	loInc bool

	hasHi bool
	hiKey string
	hiInc bool

	hasPrefix bool
	prefix    string
}

func (rb *rangeBounds) setEmpty() {
	rb.has = true
	rb.empty = true
}

func (rb *rangeBounds) normalize() {
	if rb.empty {
		return
	}

	if rb.hasLo && rb.hasHi {
		cmp := strings.Compare(rb.loKey, rb.hiKey)
		if cmp > 0 || (cmp == 0 && (!rb.loInc || !rb.hiInc)) {
			rb.setEmpty()
			return
		}
	}

	if !rb.hasPrefix {
		return
	}

	if rb.hasHi {
		cmp := strings.Compare(rb.hiKey, rb.prefix)
		if cmp < 0 || (cmp == 0 && !rb.hiInc) {
			rb.setEmpty()
			return
		}
	}

	if upper, ok := nextPrefixUpperBound(rb.prefix); ok && rb.hasLo {
		if strings.Compare(rb.loKey, upper) >= 0 {
			rb.setEmpty()
		}
	}
}

func (rb *rangeBounds) applyLo(key string, inc bool) {
	if rb.empty {
		return
	}
	if !rb.hasLo || rb.loKey < key || (rb.loKey == key && !rb.loInc && inc) {
		rb.hasLo = true
		rb.loKey = key
		rb.loInc = inc
	}
	rb.normalize()
}

func (rb *rangeBounds) applyHi(key string, inc bool) {
	if rb.empty {
		return
	}
	if !rb.hasHi || rb.hiKey > key || (rb.hiKey == key && !rb.hiInc && inc) {
		rb.hasHi = true
		rb.hiKey = key
		rb.hiInc = inc
	}
	rb.normalize()
}

func (rb *rangeBounds) applyPrefix(prefix string) {
	if rb.empty {
		return
	}
	if rb.hasPrefix {
		switch {
		case strings.HasPrefix(rb.prefix, prefix):
		case strings.HasPrefix(prefix, rb.prefix):
			rb.prefix = prefix
		default:
			rb.setEmpty()
			return
		}
	} else {
		rb.hasPrefix = true
		rb.prefix = prefix
	}
	rb.normalize()
}

func (rb *rangeBounds) applyOp(op qx.Op, key string) bool {
	rb.has = true
	switch op {
	case qx.OpGT:
		rb.applyLo(key, false)
	case qx.OpGTE:
		rb.applyLo(key, true)
	case qx.OpLT:
		rb.applyHi(key, false)
	case qx.OpLTE:
		rb.applyHi(key, true)
	case qx.OpEQ:
		rb.applyLo(key, true)
		rb.applyHi(key, true)
	case qx.OpPREFIX:
		rb.applyPrefix(key)
	default:
		return false
	}
	return true
}

func orderNilTailField(fm *field, field string, bounds rangeBounds) string {
	if fm == nil || !fm.Ptr || field == "" {
		return ""
	}
	if bounds.empty || bounds.hasLo || bounds.hasHi || bounds.hasPrefix {
		return ""
	}
	return field
}

func lowerBoundIndex(s []index, key string) int {
	lo, hi := 0, len(s)
	for lo < hi {
		mid := (lo + hi) >> 1
		if compareIndexKeyString(s[mid].Key, key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func upperBoundIndex(s []index, key string) int {
	lo, hi := 0, len(s)
	for lo < hi {
		mid := (lo + hi) >> 1
		if compareIndexKeyString(s[mid].Key, key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func nextPrefixUpperBound(prefix string) (string, bool) {
	if prefix == "" {
		return "", false
	}
	buf := []byte(prefix)
	for i := len(buf) - 1; i >= 0; i-- {
		if buf[i] == 0xFF {
			continue
		}
		buf[i]++
		result := buf[:i+1]
		return unsafe.String(unsafe.SliceData(result), len(result)), true
	}
	return "", false
}

func prefixRangeEndIndex(s []index, prefix string, start int) int {
	if start < 0 || start >= len(s) {
		return start
	}
	if compareIndexKeyString(s[start].Key, prefix) < 0 || !indexKeyHasPrefixString(s[start].Key, prefix) {
		return start
	}
	if upper, ok := nextPrefixUpperBound(prefix); ok {
		end := lowerBoundIndex(s, upper)
		if end < start {
			end = start
		}
		return end
	}

	// Extremely rare fallback when prefix consists entirely of 0xFF bytes.
	end := start
	for end < len(s) {
		if !indexKeyHasPrefixString(s[end].Key, prefix) {
			break
		}
		end++
	}
	return end
}

const iteratorThreshold = 2048 // 256

func shouldTryOrderedPredicateExecutionBaseOps(orderField string, baseOps []qx.Expr) bool {
	for _, op := range baseOps {
		if op.Not || op.Field == orderField {
			continue
		}
		switch op.Op {
		case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE:
			return true
		}
	}
	return false
}

func (qv *queryView[K, V]) shouldSkipOrderBasicLimitForHighCardNonOrderPrefix(orderField string, baseOps []qx.Expr) bool {
	if len(baseOps) == 0 {
		return false
	}

	snap := qv.planner.stats.Load()
	universe := snap.universeOr(qv.snapshotUniverseCardinality())
	if universe == 0 {
		return false
	}

	ov := qv.fieldOverlay(orderField)
	if !ov.hasData() {
		return false
	}

	orderDistinct := uint64(ov.keyCount())
	if orderDistinct == 0 || orderDistinct*2 < universe {
		return false
	}

	for _, op := range baseOps {
		if op.Not || op.Field == "" || op.Field == orderField || op.Op != qx.OpPREFIX {
			continue
		}
		sel, _, _, _, ok := qv.estimateLeafOrderCost(op, snap, universe, orderField, orderDistinct > 0)
		if ok && sel <= 0.10 {
			return true
		}
	}

	return false
}

func (qv *queryView[K, V]) tryQueryOrderBasicWithLimit(q *qx.QX, trace *queryTrace) ([]K, bool, error) {

	if len(q.Order) != 1 || q.Limit == 0 {
		return nil, false, nil
	}

	if q.Expr.Not {
		return nil, false, nil
	}

	order := q.Order[0]
	if order.Type != qx.OrderBasic {
		return nil, false, nil
	}

	ops := q.Expr.Operands

	if normalize.IsTrueConst(q.Expr) {
		ops = nil
	} else if q.Expr.Op != qx.OpAND {
		var single [1]qx.Expr
		single[0] = q.Expr
		ops = single[:]
	}

	f := order.Field
	fm := qv.fields[f]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}

	var rb rangeBounds

	var baseOps []qx.Expr
	var baseOpsStack [8]qx.Expr
	if len(ops) <= len(baseOpsStack) {
		baseOps = baseOpsStack[:0]
	} else {
		baseOps = make([]qx.Expr, 0, len(ops))
	}

	for _, op := range ops {
		if op.Field == f {
			if op.Not {
				return nil, false, nil
			}
			switch op.Op {
			case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpEQ:
				bound, isSlice, err := qv.exprValueToNormalizedScalarBound(op)
				if err != nil {
					return nil, true, err
				}
				if isSlice {
					return nil, false, nil
				}
				applyNormalizedScalarBound(&rb, bound)
				continue
			default:
				return nil, false, nil
			}
		}
		baseOps = append(baseOps, op)
	}

	nilTailField := orderNilTailField(fm, f, rb)
	ov := qv.fieldOverlay(f)
	if !ov.hasData() && nilTailField == "" {
		if !qv.hasFieldIndex(f) {
			return nil, false, nil
		}
		return nil, true, nil
	}

	br := ov.rangeForBounds(rb)
	if overlayRangeEmpty(br) && nilTailField == "" {
		return nil, true, nil
	}

	if !rb.hasLo && !rb.hasHi && !rb.hasPrefix &&
		q.Offset == 0 &&
		qv.shouldSkipOrderBasicLimitForHighCardNonOrderPrefix(f, baseOps) {
		return nil, false, nil
	}

	if !fm.Ptr && len(baseOps) > 0 && shouldTryOrderedPredicateExecutionBaseOps(f, baseOps) {
		var leavesBuf [8]qx.Expr
		if leaves, ok := collectAndLeavesScratch(q.Expr, leavesBuf[:0]); ok {
			window, _ := orderWindow(q)
			preds, ok := qv.buildPredicatesOrderedWithMode(leaves, f, false, window, true, true)
			if ok {
				defer releasePredicates(preds)
				for i := range preds {
					if preds[i].alwaysFalse {
						return nil, true, nil
					}
				}
				if out, ok := qv.execPlanOrderedBasic(q, preds, trace); ok {
					return out, true, nil
				}
			}
		}
	}

	if len(baseOps) == 0 {
		out, _ := qv.scanOrderLimitWithPredicates(q, ov, br, order.Desc, nil, nilTailField, trace)
		return out, true, nil
	}

	var base postingResult

	if len(baseOps) == 1 {
		b, err := qv.evalExpr(baseOps[0])
		if err != nil {
			return nil, true, err
		}
		if b.ids.IsEmpty() {
			b.release()
			return nil, true, nil
		}
		base = b
	} else {
		b, err := qv.evalExpr(qx.Expr{Op: qx.OpAND, Operands: baseOps})
		if err != nil {
			return nil, true, err
		}
		if b.ids.IsEmpty() {
			b.release()
			return nil, true, nil
		}
		base = b
	}
	defer base.release()

	baseBM := base.ids
	baseNegUniverse := base.neg && baseBM.IsEmpty()
	if !base.neg && baseBM.IsEmpty() {
		return nil, true, nil
	}

	skip := q.Offset
	need := q.Limit

	out := make([]K, 0, need)
	cursor := qv.newQueryCursor(out, skip, need, false, nil)

	var tmp posting.List
	defer tmp.Release()

	contains := func(idx uint64) bool {
		if base.neg {
			if baseNegUniverse {
				return true // NOT empty == universe
			}
			return !baseBM.Contains(idx)
		}
		return baseBM.Contains(idx)
	}

	keyCur := ov.newCursor(br, order.Desc)
	var (
		examined  uint64
		scanWidth uint64
	)
	nilOV := qv.nilFieldOverlay(nilTailField)
	emitFilteredPosting := func(ids posting.List) bool {
		if ids.IsEmpty() {
			return false
		}

		if card := ids.Cardinality(); card <= iteratorThreshold || (0 < cursor.need && cursor.need < 1000) {
			if idx, ok := ids.TrySingle(); ok {
				examined++
				return contains(idx) && cursor.emit(idx)
			}

			it := ids.Iter()
			for it.HasNext() {
				idx := it.Next()
				examined++
				if !contains(idx) {
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

		examined += ids.Cardinality()
		if base.neg {
			tmp.Clear()
			tmp = ids.Clone()
			if !baseNegUniverse {
				tmp.AndNotInPlace(baseBM)
			}
		} else {
			tmpIntersectPosting(&tmp, ids, baseBM)
		}
		if tmp.IsEmpty() {
			return false
		}
		return cursor.emitPosting(tmp)
	}

	for {
		_, ids, ok := keyCur.next()
		if !ok {
			break
		}
		if ids.IsEmpty() {
			continue
		}
		scanWidth++
		if emitFilteredPosting(ids) {
			trace.addExamined(examined)
			trace.addOrderScanWidth(scanWidth)
			trace.setEarlyStopReason("limit_reached")
			return cursor.out, true, nil
		}
	}

	if nilTailField != "" {
		ids := nilOV.lookupPostingRetained(nilIndexEntryKey)
		if !ids.IsEmpty() {
			scanWidth++
			if emitFilteredPosting(ids) {
				trace.addExamined(examined)
				trace.addOrderScanWidth(scanWidth)
				trace.setEarlyStopReason("limit_reached")
				return cursor.out, true, nil
			}
		}
	}

	trace.addExamined(examined)
	trace.addOrderScanWidth(scanWidth)
	trace.setEarlyStopReason("input_exhausted")
	return cursor.out, true, nil
}

func (qv *queryView[K, V]) scanOrderLimitNoPredicates(q *qx.QX, ov fieldOverlay, br overlayRange, desc bool, nilTailField string, trace *queryTrace) ([]K, bool) {
	out := make([]K, 0, q.Limit)
	cursor := qv.newQueryCursor(out, q.Offset, q.Limit, false, nil)

	var (
		examined  uint64
		scanWidth uint64
	)

	emitPosting := func(ids posting.List) bool {
		if ids.IsEmpty() {
			return false
		}
		if idx, ok := ids.TrySingle(); ok {
			examined++
			if trace != nil {
				trace.addMatched(1)
			}
			return cursor.emit(idx)
		}
		card := ids.Cardinality()
		if cursor.skip >= card {
			cursor.skip -= card
			examined += card
			if trace != nil {
				trace.addMatched(card)
			}
			return false
		}
		stop := false
		ids.ForEach(func(idx uint64) bool {
			examined++
			if trace != nil {
				trace.addMatched(1)
			}
			if cursor.emit(idx) {
				stop = true
				return false
			}
			return true
		})
		return stop
	}

	keyCur := ov.newCursor(br, desc)
	for {
		_, ids, ok := keyCur.next()
		if !ok {
			break
		}
		if ids.IsEmpty() {
			continue
		}
		scanWidth++
		if emitPosting(ids) {
			if trace != nil {
				trace.addExamined(examined)
				trace.addOrderScanWidth(scanWidth)
				trace.setEarlyStopReason("limit_reached")
			}
			return cursor.out, true
		}
	}

	if nilTailField != "" {
		ids := qv.nilFieldOverlay(nilTailField).lookupPostingRetained(nilIndexEntryKey)
		if !ids.IsEmpty() {
			scanWidth++
			if emitPosting(ids) {
				if trace != nil {
					trace.addExamined(examined)
					trace.addOrderScanWidth(scanWidth)
					trace.setEarlyStopReason("limit_reached")
				}
				return cursor.out, true
			}
		}
	}

	if trace != nil {
		trace.addExamined(examined)
		trace.addOrderScanWidth(scanWidth)
		trace.setEarlyStopReason("input_exhausted")
	}
	return cursor.out, true
}

func (qv *queryView[K, V]) scanOrderLimitWithPredicates(q *qx.QX, ov fieldOverlay, br overlayRange, desc bool, preds []predicate, nilTailField string, trace *queryTrace) ([]K, bool) {
	activeBuf := getIntSliceBuf(len(preds))
	active := activeBuf.values
	defer func() {
		activeBuf.values = active
		releaseIntSliceBuf(activeBuf)
	}()

	for i := range preds {
		p := preds[i]
		if p.alwaysFalse {
			if trace != nil {
				trace.setEarlyStopReason("empty_predicate")
			}
			return nil, true
		}
		if p.covered || p.alwaysTrue {
			continue
		}
		if !p.hasContains() {
			return nil, false
		}
		active = append(active, i)
	}
	if len(active) == 0 {
		return qv.scanOrderLimitNoPredicates(q, ov, br, desc, nilTailField, trace)
	}
	sortActivePredicates(active, preds)

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

	out := make([]K, 0, q.Limit)
	cursor := qv.newQueryCursor(out, q.Offset, q.Limit, false, nil)

	var (
		examined  uint64
		scanWidth uint64
	)

	var exactWork posting.List
	defer exactWork.Release()

	emitCandidate := func(idx uint64) bool {
		examined++
		for _, pi := range active {
			if !preds[pi].matches(idx) {
				return false
			}
		}
		if trace != nil {
			trace.addMatched(1)
		}
		return cursor.emit(idx)
	}

	emitAcceptedPosting := func(ids posting.List, card uint64) bool {
		examined += card
		if trace != nil {
			trace.addMatched(card)
		}
		if cursor.skip >= card {
			cursor.skip -= card
			return false
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

	tryBucketPosting := func(ids posting.List) (current posting.List, exactApplied bool, handled bool, stop bool) {
		if ids.IsEmpty() {
			return posting.List{}, false, true, false
		}
		card := ids.Cardinality()
		if len(active) == 0 {
			return ids, false, true, emitAcceptedPosting(ids, card)
		}
		if len(exactActive) == 0 {
			return ids, false, false, false
		}

		allowExact := plannerAllowExactBucketFilter(cursor.skip, cursor.need, card, exactOnly, len(exactActive))
		mode, exactIDs, _ := plannerFilterPostingByChecks(preds, exactActive, ids, &exactWork, allowExact)
		switch mode {
		case plannerPredicateBucketEmpty:
			examined += card
			return posting.List{}, false, true, false
		case plannerPredicateBucketAll:
			if exactOnly {
				return exactIDs, true, true, emitAcceptedPosting(exactIDs, card)
			}
			return exactIDs, true, false, false
		case plannerPredicateBucketExact:
			if trace != nil {
				trace.addPostingExactFilter(1)
			}
			if exactIDs.IsEmpty() {
				examined += card
				return posting.List{}, true, true, false
			}
			if exactOnly {
				return exactIDs, true, true, emitAcceptedPosting(exactIDs, exactIDs.Cardinality())
			}
			return exactIDs, true, false, false
		default:
			return ids, false, false, false
		}
	}

	emitPosting := func(ids posting.List) bool {
		if ids.IsEmpty() {
			return false
		}
		if idx, ok := ids.TrySingle(); ok {
			return emitCandidate(idx)
		}

		iterSrc := ids.Iter()
		defer func() {
			if iterSrc != nil {
				iterSrc.Release()
			}
		}()
		exactApplied := false
		if len(active) == 0 || len(exactActive) > 0 {
			if current, applied, handled, stop := tryBucketPosting(ids); handled {
				return stop
			} else if applied && !current.IsEmpty() {
				iterSrc.Release()
				iterSrc = current.Iter()
				exactApplied = true
			}
		}

		for iterSrc.HasNext() {
			idx := iterSrc.Next()
			examined++
			checks := active
			if exactApplied {
				checks = residualActive
			}
			pass := true
			for _, pi := range checks {
				if !preds[pi].matches(idx) {
					pass = false
					break
				}
			}
			if !pass {
				continue
			}
			if trace != nil {
				trace.addMatched(1)
			}
			if cursor.emit(idx) {
				return true
			}
		}
		return false
	}

	keyCur := ov.newCursor(br, desc)
	for {
		_, ids, ok := keyCur.next()
		if !ok {
			break
		}
		if !ids.IsEmpty() {
			scanWidth++
		}
		if emitPosting(ids) {
			if trace != nil {
				trace.addExamined(examined)
				trace.addOrderScanWidth(scanWidth)
				trace.setEarlyStopReason("limit_reached")
			}
			return cursor.out, true
		}
	}

	if nilTailField != "" {
		ids := qv.nilFieldOverlay(nilTailField).lookupPostingRetained(nilIndexEntryKey)
		if !ids.IsEmpty() {
			scanWidth++
			if emitPosting(ids) {
				if trace != nil {
					trace.addExamined(examined)
					trace.addOrderScanWidth(scanWidth)
					trace.setEarlyStopReason("limit_reached")
				}
				return cursor.out, true
			}
		}
	}

	if trace != nil {
		trace.addExamined(examined)
		trace.addOrderScanWidth(scanWidth)
		trace.setEarlyStopReason("input_exhausted")
	}
	return cursor.out, true
}

func (qv *queryView[K, V]) tryQueryOrderPrefixWithLimit(q *qx.QX, trace *queryTrace) ([]K, bool, error) {

	if len(q.Order) != 1 || q.Limit == 0 {
		return nil, false, nil
	}

	ord := q.Order[0]
	if ord.Type != qx.OrderBasic {
		return nil, false, nil
	}
	if q.Expr.Not {
		return nil, false, nil
	}

	f := ord.Field
	fm := qv.fields[f]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}

	ov := qv.fieldOverlay(f)
	if !ov.hasData() {
		return nil, true, nil
	}

	expr := q.Expr
	ops := expr.Operands
	if expr.Op != qx.OpAND {
		var single [1]qx.Expr
		single[0] = expr
		ops = single[:]
	}

	var (
		hasPrefix bool
		prefix    string
		baseOps   []qx.Expr
	)
	var baseOpsStack [8]qx.Expr
	if len(ops) <= len(baseOpsStack) {
		baseOps = baseOpsStack[:0]
	} else {
		baseOps = make([]qx.Expr, 0, len(ops))
	}

	for _, op := range ops {
		if op.Not {
			return nil, false, nil
		}
		if op.Field == f && op.Op == qx.OpPREFIX {
			if op.Not {
				return nil, false, nil
			}
			bound, isSlice, err := qv.exprValueToNormalizedScalarBound(op)
			if err != nil {
				return nil, true, err
			}
			if isSlice || bound.empty || bound.full {
				return nil, false, nil
			}
			hasPrefix = true
			prefix = bound.key
			continue
		}
		baseOps = append(baseOps, op)
	}

	if !hasPrefix {
		return nil, false, nil
	}

	br := ov.rangeForBounds(rangeBounds{
		has:       true,
		hasPrefix: true,
		prefix:    prefix,
	})
	if overlayRangeEmpty(br) {
		return nil, true, nil
	}

	if len(baseOps) == 0 {
		out, _ := qv.scanOrderLimitNoPredicates(q, ov, br, ord.Desc, "", trace)
		return out, true, nil
	}

	var base postingResult
	if len(baseOps) == 1 {
		b, err := qv.evalExpr(baseOps[0])
		if err != nil {
			return nil, true, err
		}
		if b.ids.IsEmpty() {
			b.release()
			return nil, true, nil
		}
		base = b

	} else {
		b, err := qv.evalExpr(qx.Expr{Op: qx.OpAND, Operands: baseOps})
		if err != nil {
			return nil, true, err
		}
		if b.ids.IsEmpty() {
			b.release()
			return nil, true, nil
		}
		base = b
	}
	defer base.release()

	baseBM := base.ids
	baseNegUniverse := base.neg && baseBM.IsEmpty()
	if !base.neg && baseBM.IsEmpty() {
		return nil, true, nil
	}

	skip := q.Offset
	need := q.Limit
	out := make([]K, 0, need)
	cursor := qv.newQueryCursor(out, skip, need, false, nil)

	contains := func(idx uint64) bool {
		if base.neg {
			if baseNegUniverse {
				return true // NOT empty == universe
			}
			return !baseBM.Contains(idx)
		}
		return baseBM.Contains(idx)
	}

	keyCur := ov.newCursor(br, ord.Desc)
	var (
		examined  uint64
		scanWidth uint64
	)
	for {
		_, ids, ok := keyCur.next()
		if !ok {
			break
		}
		if ids.IsEmpty() {
			continue
		}
		scanWidth++
		stop := false
		ids.ForEach(func(idx uint64) bool {
			examined++
			if !contains(idx) {
				return true
			}
			if cursor.emit(idx) {
				stop = true
				return false
			}
			return true
		})
		if stop {
			trace.addExamined(examined)
			trace.addOrderScanWidth(scanWidth)
			trace.setEarlyStopReason("limit_reached")
			return cursor.out, true, nil
		}
	}

	trace.addExamined(examined)
	trace.addOrderScanWidth(scanWidth)
	trace.setEarlyStopReason("input_exhausted")
	return cursor.out, true, nil
}

func (qv *queryView[K, V]) tryQueryRangeNoOrderWithLimit(q *qx.QX, trace *queryTrace) ([]K, bool, error) {

	if len(q.Order) > 0 || q.Limit == 0 {
		return nil, false, nil
	}

	if q.Expr.Not {
		return nil, false, nil
	}

	e := q.Expr
	if e.Op == qx.OpAND || e.Op == qx.OpOR || len(e.Operands) != 0 {
		return nil, false, nil
	}

	switch e.Op {
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpEQ:
	default:
		return nil, false, nil
	}

	f := e.Field
	if f == "" {
		return nil, false, nil
	}

	fm := qv.fields[f]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}

	ov := qv.fieldOverlay(f)
	if !ov.hasData() {
		return nil, true, nil
	}

	isNil := false
	rb := rangeBounds{has: true}
	if e.Op == qx.OpEQ {
		key, isSlice, eqNil, err := qv.exprValueToIdxScalar(e)
		if err != nil {
			return nil, true, err
		}
		if isSlice {
			return nil, false, nil
		}
		isNil = eqNil
		rb.applyLo(key, true)
		rb.applyHi(key, true)
	} else {
		bound, isSlice, err := qv.exprValueToNormalizedScalarBound(e)
		if err != nil {
			return nil, true, err
		}
		if isSlice {
			return nil, false, nil
		}
		applyNormalizedScalarBound(&rb, bound)
	}

	br := ov.rangeForBounds(rb)
	if overlayRangeEmpty(br) {
		return nil, true, nil
	}

	skip := q.Offset
	need := q.Limit

	if isNil {
		if e.Op != qx.OpEQ {
			return nil, true, nil
		}
		ids := qv.nilFieldOverlay(f).lookupPostingRetained(nilIndexEntryKey)
		if ids.IsEmpty() {
			return nil, true, nil
		}
		out := make([]K, 0, need)
		cursor := qv.newQueryCursor(out, skip, need, false, nil)
		var examined uint64
		if emitAcceptedPostingNoOrder(&cursor, ids, &examined) {
			trace.addExamined(examined)
			trace.setEarlyStopReason("limit_reached")
			return cursor.out, true, nil
		}
		trace.addExamined(examined)
		trace.setEarlyStopReason("input_exhausted")
		return cursor.out, true, nil
	}

	out := make([]K, 0, need)
	cursor := qv.newQueryCursor(out, skip, need, false, nil)

	keyCur := ov.newCursor(br, false)
	var examined uint64
	for {
		_, ids, ok := keyCur.next()
		if !ok {
			break
		}
		if ids.IsEmpty() {
			continue
		}
		if emitAcceptedPostingNoOrder(&cursor, ids, &examined) {
			trace.addExamined(examined)
			trace.setEarlyStopReason("limit_reached")
			return cursor.out, true, nil
		}
	}

	trace.addExamined(examined)
	trace.setEarlyStopReason("input_exhausted")
	return cursor.out, true, nil
}

func (qv *queryView[K, V]) tryQueryPrefixNoOrderWithLimit(q *qx.QX, trace *queryTrace) ([]K, bool, error) {

	if len(q.Order) > 0 || q.Limit == 0 {
		return nil, false, nil
	}

	if q.Expr.Not {
		return nil, false, nil
	}

	e := q.Expr
	if e.Op != qx.OpPREFIX || e.Field == "" {
		return nil, false, nil
	}
	if len(e.Operands) != 0 {
		return nil, false, nil
	}

	fm := qv.fields[e.Field]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}

	bound, isSlice, err := qv.exprValueToNormalizedScalarBound(e)
	if err != nil {
		return nil, true, err
	}
	if isSlice {
		return nil, false, nil
	}
	if bound.empty || bound.full {
		return nil, true, nil
	}
	prefix := bound.key

	ov := qv.fieldOverlay(e.Field)
	if !ov.hasData() {
		if !qv.hasFieldIndex(e.Field) {
			return nil, true, fmt.Errorf("no index for field: %v", e.Field)
		}
		return nil, true, nil
	}

	br := ov.rangeForBounds(rangeBounds{
		has:       true,
		hasPrefix: true,
		prefix:    prefix,
	})
	if overlayRangeEmpty(br) {
		return nil, true, nil
	}

	skip := q.Offset
	need := q.Limit
	out := make([]K, 0, need)
	cursor := qv.newQueryCursor(out, skip, need, false, nil)

	keyCur := ov.newCursor(br, false)
	var examined uint64
	for {
		_, ids, ok := keyCur.next()
		if !ok {
			break
		}
		if ids.IsEmpty() {
			continue
		}
		if emitAcceptedPostingNoOrder(&cursor, ids, &examined) {
			trace.addExamined(examined)
			trace.setEarlyStopReason("limit_reached")
			return cursor.out, true, nil
		}
	}

	trace.addExamined(examined)
	trace.setEarlyStopReason("input_exhausted")
	return cursor.out, true, nil
}
