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

func predicatesMatchActive(preds []predicate, active []int, idx uint64) bool {
	switch len(active) {
	case 0:
		return true
	case 1:
		return preds[active[0]].matches(idx)
	case 2:
		return preds[active[0]].matches(idx) &&
			preds[active[1]].matches(idx)
	case 3:
		return preds[active[0]].matches(idx) &&
			preds[active[1]].matches(idx) &&
			preds[active[2]].matches(idx)
	case 4:
		return preds[active[0]].matches(idx) &&
			preds[active[1]].matches(idx) &&
			preds[active[2]].matches(idx) &&
			preds[active[3]].matches(idx)
	}
	for _, pi := range active {
		if !preds[pi].matches(idx) {
			return false
		}
	}
	return true
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

type orderBasicBaseCoreKind uint8

const (
	orderBasicBaseCoreRawExpr orderBasicBaseCoreKind = iota
	orderBasicBaseCoreCollapsedRange
)

type orderBasicBaseCore struct {
	kind      orderBasicBaseCoreKind
	expr      qx.Expr
	collapsed preparedScalarExactRange
}

func isOrderBasicCollapsibleNumericRangeExpr(op qx.Expr, fm *field) bool {
	if op.Not || op.Field == "" || fm == nil || fm.Slice || !isNumericScalarKind(fm.Kind) {
		return false
	}
	return isScalarRangeEqOp(op.Op)
}

func (qv *queryView[K, V]) shouldCollapseOrderBasicNumericRange(field string, rb rangeBounds) bool {
	if field == "" || rb.empty {
		return false
	}
	ov := qv.fieldOverlay(field)
	if !ov.hasData() {
		return true
	}
	br := ov.rangeForBounds(rb)
	if overlayRangeEmpty(br) {
		return true
	}
	bucketCount, est := overlayRangeStats(ov, br)
	if bucketCount == 0 || est == 0 {
		return true
	}
	universe := qv.snapshotUniverseCardinality()
	if universe == 0 {
		return false
	}
	positiveWork := rangeProbeMaterializeWork(bucketCount, est)
	if positiveWork == 0 {
		return false
	}
	complementBuckets := ov.keyCount() - bucketCount
	if qv.nilFieldOverlay(field).lookupCardinality(nilIndexEntryKey) > 0 {
		complementBuckets++
	}
	complementEst := uint64(0)
	if universe > est {
		complementEst = universe - est
	}
	complementWork := rangeProbeMaterializeWork(complementBuckets, complementEst)
	if complementWork == 0 {
		return false
	}
	return positiveWork <= complementWork
}

func (qv *queryView[K, V]) prepareOrderBasicBaseCores(baseOps []qx.Expr) (*orderBasicBaseCoreSliceBuf, *intSliceBuf, bool, error) {
	if len(baseOps) == 0 {
		return nil, nil, false, nil
	}
	coresBuf := getOrderBasicBaseCoreSliceBuf(len(baseOps))
	coresBuf.values = coresBuf.values[:0]
	rawCoreIdxBuf := getIntSliceBuf(len(baseOps))
	rawCoreIdxBuf.values = rawCoreIdxBuf.values[:len(baseOps)]
	for i := range rawCoreIdxBuf.values {
		rawCoreIdxBuf.values[i] = -1
	}

	for i, op := range baseOps {
		if rawCoreIdxBuf.values[i] >= 0 {
			continue
		}
		fm := qv.fields[op.Field]
		if !isOrderBasicCollapsibleNumericRangeExpr(op, fm) {
			continue
		}

		var rb rangeBounds
		groupCount := 0
		for j := i; j < len(baseOps); j++ {
			if rawCoreIdxBuf.values[j] >= 0 {
				continue
			}
			other := baseOps[j]
			if other.Field != op.Field || !isOrderBasicCollapsibleNumericRangeExpr(other, fm) {
				continue
			}
			nextRB, ok, err := qv.rangeBoundsForScalarExpr(other)
			if err != nil {
				releaseOrderBasicBaseCoreSliceBuf(coresBuf)
				releaseIntSliceBuf(rawCoreIdxBuf)
				return nil, nil, false, err
			}
			if !ok {
				continue
			}
			mergeRangeBounds(&rb, nextRB)
			groupCount++
		}
		if groupCount <= 1 {
			continue
		}
		if rb.empty {
			releaseOrderBasicBaseCoreSliceBuf(coresBuf)
			releaseIntSliceBuf(rawCoreIdxBuf)
			return nil, nil, true, nil
		}
		if !qv.shouldCollapseOrderBasicNumericRange(op.Field, rb) {
			continue
		}

		coreIdx := len(coresBuf.values)
		coresBuf.values = append(coresBuf.values, orderBasicBaseCore{
			kind: orderBasicBaseCoreCollapsedRange,
			collapsed: preparedScalarExactRange{
				field:  op.Field,
				bounds: rb,
			},
		})
		for j := i; j < len(baseOps); j++ {
			other := baseOps[j]
			if other.Field == op.Field && isOrderBasicCollapsibleNumericRangeExpr(other, fm) {
				rawCoreIdxBuf.values[j] = coreIdx
			}
		}
	}

	for i, op := range baseOps {
		if rawCoreIdxBuf.values[i] >= 0 {
			continue
		}
		rawCoreIdxBuf.values[i] = len(coresBuf.values)
		coresBuf.values = append(coresBuf.values, orderBasicBaseCore{
			kind: orderBasicBaseCoreRawExpr,
			expr: op,
		})
	}
	return coresBuf, rawCoreIdxBuf, false, nil
}

func (qv *queryView[K, V]) loadWarmOrderBasicBaseCore(core orderBasicBaseCore) (postingResult, bool) {
	switch core.kind {
	case orderBasicBaseCoreCollapsedRange:
		return qv.loadWarmPreparedScalarExactRange(core.collapsed)
	case orderBasicBaseCoreRawExpr:
		return qv.loadWarmOrderBasicRawBaseOp(core.expr)
	default:
		return postingResult{}, false
	}
}

func (qv *queryView[K, V]) evalOrderBasicBaseCore(core orderBasicBaseCore) (postingResult, error) {
	switch core.kind {
	case orderBasicBaseCoreCollapsedRange:
		return qv.evalPreparedScalarExactRange(core.collapsed)
	case orderBasicBaseCoreRawExpr:
		return qv.evalOrderBasicRawBaseOp(core.expr)
	default:
		return postingResult{}, nil
	}
}

func (qv *queryView[K, V]) shouldPromoteObservedOrderBasicBaseCore(
	orderField string,
	core orderBasicBaseCore,
	universe uint64,
	observedRows uint64,
	needWindow uint64,
) bool {
	switch core.kind {
	case orderBasicBaseCoreCollapsedRange:
		return qv.shouldPromoteObservedPreparedScalarExactRange(core.collapsed, observedRows, needWindow)
	case orderBasicBaseCoreRawExpr:
		if qv.snap.materializedPredCacheLimit() <= 0 {
			return false
		}
		return qv.shouldPromoteObservedOrderBasicRawBaseOp(
			orderField,
			core.expr,
			universe,
			observedRows,
			needWindow,
		)
	default:
		return false
	}
}

func (qv *queryView[K, V]) promoteObservedOrderBasicBaseCore(core orderBasicBaseCore) {
	switch core.kind {
	case orderBasicBaseCoreCollapsedRange:
		cacheKey := materializedPredCacheKeyForExactScalarRange(core.collapsed.field, core.collapsed.bounds)
		if cacheKey != "" {
			if _, ok := qv.snap.loadMaterializedPred(cacheKey); ok {
				return
			}
		}
		ids, err := qv.evalPreparedScalarExactRange(core.collapsed)
		if err == nil {
			ids.release()
		}
	case orderBasicBaseCoreRawExpr:
		stats, ok := qv.orderBasicRawBaseOpStats(core.expr, qv.snapshotUniverseCardinality())
		cacheKey := ""
		if ok {
			cacheKey = stats.cacheKey
		}
		if cacheKey == "" {
			return
		}
		if _, ok := qv.snap.loadMaterializedPred(cacheKey); ok {
			return
		}
		scalarKey := qv.materializedPredCacheKey(core.expr)
		if cacheKey == scalarKey {
			ids := qv.evalLazyMaterializedPredicate(core.expr, cacheKey)
			ids.Release()
			return
		}
		qv.materializeOrderBasicLimitComplementBaseOp(core.expr, cacheKey)
	}
}

func (qv *queryView[K, V]) hasWarmOrderBasicBaseCores(cores *orderBasicBaseCoreSliceBuf) bool {
	if cores == nil {
		return false
	}
	for i := range cores.values {
		if hit, ok := qv.loadWarmOrderBasicBaseCore(cores.values[i]); ok {
			hit.release()
			return true
		}
	}
	return false
}

func (qv *queryView[K, V]) orderBasicRawBaseOpStats(
	op qx.Expr,
	universe uint64,
) (scalarMaterializationStats, bool) {
	if !isSimpleScalarRangeOrPrefixLeaf(op) {
		return scalarMaterializationStats{}, false
	}

	candidate, ok := qv.prepareScalarRangeRoutingCandidate(op)
	if !ok {
		return scalarMaterializationStats{}, false
	}
	return candidate.core.orderBasicMaterializationStats(universe)
}

func (qv *queryView[K, V]) shouldPromoteObservedOrderBasicRawBaseOp(
	orderField string,
	op qx.Expr,
	universe uint64,
	observedRows uint64,
	needWindow uint64,
) bool {
	if universe == 0 || observedRows == 0 || op.Not || op.Field == "" || op.Field == orderField {
		return false
	}
	if needWindow == 0 {
		needWindow = 1
	}
	if observedRows <= needWindow {
		return false
	}
	stats, ok := qv.orderBasicRawBaseOpStats(op, universe)
	if !ok || stats.cacheKey == "" || stats.probeBuckets == 0 || stats.probeEst == 0 {
		return false
	}
	buildWork := rangeProbeMaterializeWork(stats.buildBuckets, stats.buildEst)
	if buildWork == 0 {
		return false
	}
	if observedRows > universe {
		observedRows = universe
	}
	excessRows := observedRows - needWindow
	probeWork := rangeProbeTotalWorkForRows(clampUint64ToInt(excessRows), stats.probeBuckets, stats.probeEst)
	return probeWork >= buildWork
}

func (qv *queryView[K, V]) materializeOrderBasicLimitComplementBaseOp(op qx.Expr, cacheKey string) bool {
	if cacheKey == "" || op.Field == "" {
		return false
	}
	candidate, ok := qv.prepareScalarRangeRoutingCandidate(op)
	if !ok {
		return false
	}
	plan, ok := candidate.core.prepareComplementMaterialization()
	if !ok {
		return false
	}
	if plan.est == 0 {
		qv.snap.storeMaterializedPred(cacheKey, posting.List{})
		return true
	}
	ids := candidate.core.materializeComplement(plan)
	if ids.IsEmpty() {
		qv.snap.storeMaterializedPred(cacheKey, posting.List{})
		return true
	}
	ids = tryShareMaterializedPredOnSnapshot(qv.snap, cacheKey, ids)
	ids.Release()
	return true
}

func (qv *queryView[K, V]) materializeOrderMaterializedBaseOps(orderField string, baseOps []qx.Expr) {
	if qv.snap == nil || qv.snap.materializedPredCacheLimit() <= 0 || len(baseOps) == 0 {
		return
	}
	coresBuf, rawCoreIdxBuf, empty, err := qv.prepareOrderBasicBaseCores(baseOps)
	if err != nil || empty {
		if coresBuf != nil {
			releaseOrderBasicBaseCoreSliceBuf(coresBuf)
		}
		if rawCoreIdxBuf != nil {
			releaseIntSliceBuf(rawCoreIdxBuf)
		}
		return
	}
	defer releaseOrderBasicBaseCoreSliceBuf(coresBuf)
	defer releaseIntSliceBuf(rawCoreIdxBuf)

	keysBuf := getStringSliceBuf(len(coresBuf.values))
	keysBuf.values = keysBuf.values[:0]
	defer releaseStringSliceBuf(keysBuf)

	for i := range coresBuf.values {
		core := coresBuf.values[i]
		key := ""
		switch core.kind {
		case orderBasicBaseCoreCollapsedRange:
			key = materializedPredCacheKeyForExactScalarRange(core.collapsed.field, core.collapsed.bounds)
		case orderBasicBaseCoreRawExpr:
			if core.expr.Not || core.expr.Field == "" || core.expr.Field == orderField {
				continue
			}
			stats, ok := qv.orderBasicRawBaseOpStats(core.expr, qv.snapshotUniverseCardinality())
			if ok {
				key = stats.cacheKey
			}
		}
		if key == "" {
			continue
		}
		seen := false
		for j := range keysBuf.values {
			if keysBuf.values[j] == key {
				seen = true
				break
			}
		}
		if seen {
			continue
		}
		keysBuf.values = append(keysBuf.values, key)
		qv.promoteObservedOrderBasicBaseCore(core)
	}
}

func (qv *queryView[K, V]) promoteOrderBasicLimitMaterializedBaseOps(orderField string, baseOps []qx.Expr, observedRows uint64, needWindow uint64) {
	if qv.snap == nil || len(baseOps) == 0 || observedRows == 0 {
		return
	}
	universe := qv.snapshotUniverseCardinality()
	if universe == 0 {
		return
	}
	coresBuf, rawCoreIdxBuf, empty, err := qv.prepareOrderBasicBaseCores(baseOps)
	if err != nil || empty {
		if coresBuf != nil {
			releaseOrderBasicBaseCoreSliceBuf(coresBuf)
		}
		if rawCoreIdxBuf != nil {
			releaseIntSliceBuf(rawCoreIdxBuf)
		}
		return
	}
	defer releaseOrderBasicBaseCoreSliceBuf(coresBuf)
	defer releaseIntSliceBuf(rawCoreIdxBuf)

	keysBuf := getStringSliceBuf(len(coresBuf.values))
	keysBuf.values = keysBuf.values[:0]
	defer releaseStringSliceBuf(keysBuf)

	for i := range coresBuf.values {
		core := coresBuf.values[i]
		if !qv.shouldPromoteObservedOrderBasicBaseCore(orderField, core, universe, observedRows, needWindow) {
			continue
		}
		key := ""
		switch core.kind {
		case orderBasicBaseCoreCollapsedRange:
			key = materializedPredCacheKeyForExactScalarRange(core.collapsed.field, core.collapsed.bounds)
		case orderBasicBaseCoreRawExpr:
			stats, ok := qv.orderBasicRawBaseOpStats(core.expr, qv.snapshotUniverseCardinality())
			if ok {
				key = stats.cacheKey
			}
		}
		if key != "" {
			seen := false
			for j := range keysBuf.values {
				if keysBuf.values[j] == key {
					seen = true
					break
				}
			}
			if seen {
				continue
			}
			keysBuf.values = append(keysBuf.values, key)
		}
		qv.promoteObservedOrderBasicBaseCore(core)
	}
}

func (qv *queryView[K, V]) promoteObservedLimitLeafPreds(orderField string, preds []leafPred, observedRows uint64, needWindow uint64) {
	if qv.snap == nil || len(preds) == 0 || observedRows == 0 {
		return
	}
	universe := qv.snapshotUniverseCardinality()
	if universe == 0 {
		return
	}

	keysBuf := getStringSliceBuf(len(preds))
	keysBuf.values = keysBuf.values[:0]
	defer releaseStringSliceBuf(keysBuf)

	for i := range preds {
		if preds[i].kind != leafPredKindPredicate {
			continue
		}
		var (
			core orderBasicBaseCore
			ok   bool
		)
		if preds[i].hasBaseCore {
			core = preds[i].baseCore
			ok = true
		} else {
			op := preds[i].pred.expr
			if !isSimpleScalarRangeOrPrefixLeaf(op) || op.Field == "" || op.Field == orderField || op.Not {
				continue
			}
			core = orderBasicBaseCore{
				kind: orderBasicBaseCoreRawExpr,
				expr: op,
			}
			ok = true
		}
		if !ok {
			continue
		}
		if !qv.shouldPromoteObservedOrderBasicBaseCore(orderField, core, universe, observedRows, needWindow) {
			continue
		}

		key := ""
		requiresSecondHit := false
		switch core.kind {
		case orderBasicBaseCoreCollapsedRange:
			key = materializedPredCacheKeyForExactScalarRange(core.collapsed.field, core.collapsed.bounds)
		case orderBasicBaseCoreRawExpr:
			stats, ok := qv.orderBasicRawBaseOpStats(core.expr, universe)
			if ok {
				key = stats.cacheKey
				requiresSecondHit = true
			}
		}
		if key == "" {
			continue
		}
		if requiresSecondHit && !qv.snap.shouldPromoteRuntimeMaterializedPred(key) {
			continue
		}

		seen := false
		for j := range keysBuf.values {
			if keysBuf.values[j] == key {
				seen = true
				break
			}
		}
		if seen {
			continue
		}
		keysBuf.values = append(keysBuf.values, key)
		qv.promoteObservedOrderBasicBaseCore(core)
	}
}

func (qv *queryView[K, V]) evalOrderBasicRawBaseOp(op qx.Expr) (postingResult, error) {
	stats, ok := qv.orderBasicRawBaseOpStats(op, qv.snapshotUniverseCardinality())
	cacheKey := ""
	if ok {
		cacheKey = stats.cacheKey
	}
	if cacheKey != "" {
		if cached, ok := qv.snap.loadMaterializedPred(cacheKey); ok {
			if cacheKey == qv.materializedPredCacheKey(op) {
				return postingResult{ids: cached}, nil
			}
			return postingResult{ids: cached, neg: true}, nil
		}
	}
	return qv.evalExpr(op)
}

func (qv *queryView[K, V]) loadWarmOrderBasicRawBaseOp(op qx.Expr) (postingResult, bool) {
	candidate, ok := qv.prepareScalarRangeRoutingCandidate(op)
	if !ok {
		return postingResult{}, false
	}
	return candidate.core.loadWarmScalarPostingResult()
}

func (qv *queryView[K, V]) shouldPreferOrderBasicBaseCorePathForNonOrderPrefix(q *qx.QX, orderField string, baseOps []qx.Expr) bool {
	if len(baseOps) == 0 {
		return false
	}
	window, _ := orderWindow(q)
	if window <= 0 {
		return false
	}

	universe := qv.snapshotUniverseCardinality()
	if universe == 0 {
		return false
	}

	for _, op := range baseOps {
		if !isPositiveNonOrderScalarPrefixLeaf(orderField, op) {
			continue
		}
		candidate, ok := qv.prepareScalarRangeRoutingCandidate(op)
		if !ok {
			continue
		}
		if candidate.plan.orderedEagerMaterializeUseful(window, universe) {
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
	needWindow, _ := orderWindow(q)
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
			if op.Not || !isScalarRangeEqOp(op.Op) {
				return nil, false, nil
			}
			nextRB, ok, err := qv.rangeBoundsForScalarExpr(op)
			if err != nil {
				return nil, true, err
			}
			if !ok {
				return nil, false, nil
			}
			mergeRangeBounds(&rb, nextRB)
			continue
		}
		baseOps = append(baseOps, op)
	}
	baseCoresBuf, baseRawCoreIdxBuf, noMatch, err := qv.prepareOrderBasicBaseCores(baseOps)
	if err != nil {
		return nil, true, err
	}
	if baseCoresBuf != nil {
		defer releaseOrderBasicBaseCoreSliceBuf(baseCoresBuf)
	}
	if baseRawCoreIdxBuf != nil {
		defer releaseIntSliceBuf(baseRawCoreIdxBuf)
	}
	if noMatch {
		return nil, true, nil
	}

	nilTailField := orderNilTailField(fm, f, rb)
	ov := qv.fieldOverlay(f)
	if !ov.hasData() && nilTailField == "" {
		if !qv.hasIndexedField(f) {
			return nil, false, nil
		}
		return nil, true, nil
	}

	br := ov.rangeForBounds(rb)
	if overlayRangeEmpty(br) && nilTailField == "" {
		return nil, true, nil
	}

	preferBaseCores := !rb.hasLo && !rb.hasHi && !rb.hasPrefix &&
		q.Offset == 0 &&
		qv.shouldPreferOrderBasicBaseCorePathForNonOrderPrefix(q, f, baseOps)

	hasWarmBaseOps := baseCoresBuf != nil && len(baseCoresBuf.values) > 0 && qv.hasWarmOrderBasicBaseCores(baseCoresBuf)
	if !preferBaseCores && !fm.Ptr && len(baseOps) > 0 {
		leavesBuf := getExprSliceBuf(len(baseOps) + 2)
		leaves, ok := collectAndLeavesScratch(q.Expr, leavesBuf.values[:0])
		if ok {
			execDecision := qv.decideExecutionOrderByCost(q, leaves)
			if execDecision.use {
				window, _ := orderWindow(q)
				preds, predsBuf, ok := qv.buildPredicatesOrderedWithMode(leaves, f, false, window, true, true)
				if ok {
					defer releasePredicates(preds, predsBuf)
					for i := range preds {
						if preds[i].alwaysFalse {
							leavesBuf.values = leaves
							releaseExprSliceBuf(leavesBuf)
							return nil, true, nil
						}
					}
					execTrace := trace
					var observedTrace queryTrace
					observedStart := uint64(0)
					if execTrace == nil {
						execTrace = &observedTrace
					} else {
						observedStart = execTrace.ev.RowsExamined
					}
					if out, ok := qv.execPlanOrderedBasic(q, preds, execTrace); ok {
						leavesBuf.values = leaves
						releaseExprSliceBuf(leavesBuf)
						qv.promoteOrderBasicLimitMaterializedBaseOps(f, baseOps, execTrace.ev.RowsExamined-observedStart, uint64(needWindow))
						return out, true, nil
					}
				}
			}
		}
		leavesBuf.values = leaves
		releaseExprSliceBuf(leavesBuf)
		if !hasWarmBaseOps {
			return nil, false, nil
		}
	}

	if len(baseOps) == 0 {
		out, _ := qv.scanOrderLimitWithPredicates(q, ov, br, order.Desc, nil, nilTailField, trace)
		return out, true, nil
	}

	var base postingResult
	var residualPreds []predicate
	var residualPredsBuf *predicateSliceBuf
	var residualActive []int
	var residualActiveBuf *intSliceBuf
	defer func() {
		if residualActiveBuf != nil {
			residualActiveBuf.values = residualActive
			releaseIntSliceBuf(residualActiveBuf)
		}
		if residualPreds != nil || residualPredsBuf != nil {
			releasePredicates(residualPreds, residualPredsBuf)
		}
	}()

	if hasWarmBaseOps {
		residualOpsBuf := getExprSliceBuf(len(baseOps))
		residualOps := residualOpsBuf.values[:0]
		defer func() {
			residualOpsBuf.values = residualOps
			releaseExprSliceBuf(residualOpsBuf)
		}()
		var loadedCoreBuf *boolSliceBuf
		if baseCoresBuf != nil && len(baseCoresBuf.values) > 0 {
			loadedCoreBuf = getBoolSliceBuf(len(baseCoresBuf.values))
			loadedCoreBuf.values = loadedCoreBuf.values[:len(baseCoresBuf.values)]
			defer releaseBoolSliceBuf(loadedCoreBuf)
		}

		baseBuilt := false
		for i := range baseCoresBuf.values {
			b, ok := qv.loadWarmOrderBasicBaseCore(baseCoresBuf.values[i])
			if !ok {
				continue
			}
			loadedCoreBuf.values[i] = true
			if !baseBuilt {
				base = b
				baseBuilt = true
				continue
			}
			var err error
			base, err = qv.andPostingResult(base, b)
			if err != nil {
				base.release()
				return nil, true, err
			}
		}
		for i, op := range baseOps {
			if loadedCoreBuf.values[baseRawCoreIdxBuf.values[i]] {
				continue
			}
			residualOps = append(residualOps, op)
		}
		if !baseBuilt {
			hasWarmBaseOps = false
		} else if len(residualOps) > 0 {
			window, _ := orderWindow(q)
			var ok bool
			residualPreds, residualPredsBuf, ok = qv.buildPredicatesOrderedWithMode(residualOps, f, false, window, false, true)
			if !ok {
				base.release()
				base = postingResult{}
				hasWarmBaseOps = false
				if residualPreds != nil || residualPredsBuf != nil {
					releasePredicates(residualPreds, residualPredsBuf)
					residualPreds = nil
					residualPredsBuf = nil
				}
			} else {
				residualActiveBuf = getIntSliceBuf(len(residualPreds))
				residualActive = residualActiveBuf.values[:0]
				for i := range residualPreds {
					if residualPreds[i].alwaysFalse {
						base.release()
						return nil, true, nil
					}
					if residualPreds[i].covered || residualPreds[i].alwaysTrue {
						continue
					}
					residualActive = append(residualActive, i)
				}
			}
		}
	}

	if !hasWarmBaseOps {
		baseBuilt := false
		for i := range baseCoresBuf.values {
			b, err := qv.evalOrderBasicBaseCore(baseCoresBuf.values[i])
			if err != nil {
				base.release()
				return nil, true, err
			}
			if !baseBuilt {
				base = b
				baseBuilt = true
				continue
			}
			base, err = qv.andPostingResult(base, b)
			if err != nil {
				base.release()
				return nil, true, err
			}
		}
	}
	if !base.neg && base.ids.IsEmpty() {
		base.release()
		return nil, true, nil
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
	cursor := qv.newQueryCursor(out, skip, need, false, 0)

	var tmp posting.List
	defer tmp.Release()

	contains := func(idx uint64) bool {
		if base.neg {
			if baseNegUniverse {
				return predicatesMatchActive(residualPreds, residualActive, idx)
			}
			if baseBM.Contains(idx) {
				return false
			}
		} else if !baseBM.Contains(idx) {
			return false
		}
		return predicatesMatchActive(residualPreds, residualActive, idx)
	}
	residualMatches := func(idx uint64) bool {
		return predicatesMatchActive(residualPreds, residualActive, idx)
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
			tmp.Release()
			tmp = posting.List{}
			tmp = ids.Clone()
			if !baseNegUniverse {
				tmp = tmp.BuildAndNot(baseBM)
			}
		} else {
			tmp = tmpIntersectPosting(tmp, ids, baseBM)
		}
		if tmp.IsEmpty() {
			return false
		}
		if len(residualActive) == 0 {
			return cursor.emitPosting(tmp)
		}
		it := tmp.Iter()
		defer it.Release()
		for it.HasNext() {
			idx := it.Next()
			if residualMatches(idx) && cursor.emit(idx) {
				return true
			}
		}
		return false
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
			qv.promoteOrderBasicLimitMaterializedBaseOps(f, baseOps, examined, uint64(needWindow))
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
				qv.promoteOrderBasicLimitMaterializedBaseOps(f, baseOps, examined, uint64(needWindow))
				return cursor.out, true, nil
			}
		}
	}

	trace.addExamined(examined)
	trace.addOrderScanWidth(scanWidth)
	trace.setEarlyStopReason("input_exhausted")
	qv.promoteOrderBasicLimitMaterializedBaseOps(f, baseOps, examined, uint64(needWindow))
	return cursor.out, true, nil
}

func (qv *queryView[K, V]) scanOrderLimitNoPredicates(q *qx.QX, ov fieldOverlay, br overlayRange, desc bool, nilTailField string, trace *queryTrace) ([]K, bool) {
	out := make([]K, 0, q.Limit)
	cursor := qv.newQueryCursor(out, q.Offset, q.Limit, false, 0)

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
	cursor := qv.newQueryCursor(out, q.Offset, q.Limit, false, 0)

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
		mode, exactIDs, nextExactWork, _ := plannerFilterPostingByChecks(preds, exactActive, ids, exactWork, allowExact)
		exactWork = nextExactWork
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
		if classifyOrderFieldScalarLeaf(f, op) == orderFieldScalarLeafPrefix {
			prefixState, ok, err := qv.prepareScalarPrefixRoute(op)
			if err != nil {
				return nil, true, err
			}
			if !ok {
				return nil, false, nil
			}
			if !prefixState.hasData || overlayRangeEmpty(prefixState.br) {
				return nil, false, nil
			}
			hasPrefix = true
			prefix = prefixState.prefix
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
	cursor := qv.newQueryCursor(out, skip, need, false, 0)

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

	if !isScalarRangeEqOp(e.Op) {
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
		nextRB, ok, err := qv.rangeBoundsForScalarExpr(e)
		if err != nil {
			return nil, true, err
		}
		if !ok {
			return nil, false, nil
		}
		rb = nextRB
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
		cursor := qv.newQueryCursor(out, skip, need, false, 0)
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
	cursor := qv.newQueryCursor(out, skip, need, false, 0)

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
	if !isPositiveScalarPrefixLeaf(e) {
		return nil, false, nil
	}

	fm := qv.fields[e.Field]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}

	prefixState, ok, err := qv.prepareScalarPrefixRoute(e)
	if err != nil {
		return nil, true, err
	}
	if !ok {
		return nil, false, nil
	}
	if !prefixState.hasData {
		if !qv.hasIndexedField(e.Field) {
			return nil, true, fmt.Errorf("no index for field: %v", e.Field)
		}
		return nil, true, nil
	}
	if overlayRangeEmpty(prefixState.br) {
		return nil, true, nil
	}

	skip := q.Offset
	need := q.Limit
	out := make([]K, 0, need)
	cursor := qv.newQueryCursor(out, skip, need, false, 0)

	keyCur := prefixState.ov.newCursor(prefixState.br, false)
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
