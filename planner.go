package rbi

import (
	"container/heap"
	"math"
	"sort"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/vapstack/qx"
)

// PlanName is a stable plan identifier used by tracing and calibration.
type PlanName string

const (
	PlanBitmap PlanName = "plan_bitmap"

	PlanCandidateNoOrder PlanName = "plan_candidate_no_order"
	PlanCandidateOrder   PlanName = "plan_candidate_order"

	PlanORMergeNoOrder     PlanName = "plan_or_merge_no_order"
	PlanORMergeOrderMerge  PlanName = "plan_or_merge_order_merge"
	PlanORMergeOrderStream PlanName = "plan_or_merge_order_stream"

	PlanOrdered        PlanName = "plan_ordered"
	PlanOrderedNoOrder PlanName = "plan_ordered_no_order"
	PlanOrderedAnchor  PlanName = "plan_ordered_anchor"
	PlanOrderedLead    PlanName = "plan_ordered_lead"

	PlanLimit              PlanName = "plan_limit"
	PlanLimitOrderBasic    PlanName = "plan_limit_order_basic"
	PlanLimitOrderPrefix   PlanName = "plan_limit_order_prefix"
	PlanLimitPrefixNoOrder PlanName = "plan_limit_prefix_no_order"
	PlanLimitRangeNoOrder  PlanName = "plan_limit_range_no_order"
)

const (
	PlanPrefixORMerge      = "plan_or_merge_"
	PlanPrefixORMergeOrder = "plan_or_merge_order_"
)

const (
	plannerORBranchLimit       = 8
	plannerORNoOrderLimitMax   = 10_000
	plannerOROrderLimitMax     = 10_000
	plannerOROrderMergeNeedMax = 4_096
	plannerOROrderMergeAvgMax  = 8
	plannerORNoOrderOffsetMax  = 1_000_000
	plannerOROrderOffsetMax    = 1_000_000
	plannerORMinOperandCount   = 2

	plannerORNoOrderInitActiveBranches = 2
	plannerORNoOrderMinBranchBudget    = 64
	plannerORNoOrderMaxBranchBudget    = 65_536

	plannerOROrderFallbackFirstNeedMin       = 256
	plannerOROrderFallbackFirstOffsetMin     = 128
	plannerOROrderFallbackFirstAvgChecksMin  = 3.0
	plannerOROrderFallbackFirstWideChecksMin = 4.5

	plannerORKWayRuntimeNeedMin              = 64
	plannerORKWayRuntimeMinPops              = 32
	plannerORKWayRuntimeMinUnique            = 8
	plannerORKWayRuntimeMinExamined          = 16_384
	plannerORKWayRuntimeProjectedExaminedMax = 2_500_000.0
	plannerORKWayRuntimeExaminedPerUniqueMin = 1_024.0
	plannerORKWayRuntimeAvgChecksEnableMin   = 2.0
)

type plannerORBranch struct {
	expr qx.Expr

	preds      []plannerPred
	alwaysTrue bool
	lead       *plannerPred
	leadIdx    int
}

func (b *plannerORBranch) matches(idx uint64) bool {
	if b.alwaysTrue {
		return true
	}
	for i := range b.preds {
		p := b.preds[i]
		if p.alwaysTrue {
			continue
		}
		if p.alwaysFalse {
			return false
		}
		if p.contains == nil || !p.contains(idx) {
			return false
		}
	}
	return true
}

func (b *plannerORBranch) matchesFromLead(idx uint64) bool {
	if b.alwaysTrue {
		return true
	}
	for i := range b.preds {
		if i == b.leadIdx {
			continue
		}
		p := b.preds[i]
		if p.alwaysTrue {
			continue
		}
		if p.alwaysFalse {
			return false
		}
		if p.contains == nil || !p.contains(idx) {
			return false
		}
	}
	return true
}

type plannerORIter struct {
	it     roaringIter
	branch *plannerORBranch

	examined       *uint64
	branchExamined *uint64
	branchEmitted  *uint64

	has bool
	cur uint64
}

func (db *DB[K, V]) tryPlan(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	if out, ok, err := db.tryPlanORMerge(q, trace); ok {
		return out, true, err
	}
	if out, ok, err := db.tryPlanOrdered(q, trace); ok {
		return out, true, err
	}
	if out, ok, err := db.tryPlanCandidate(q, trace); ok {
		return out, true, err
	}
	return nil, false, nil
}

func (it *plannerORIter) advance() {
	it.advanceWithBudget(0)
}

func (it *plannerORIter) advanceWithBudget(budget uint64) (budgetHit bool) {
	it.has = false
	var scanned uint64
	for it.it.HasNext() {
		if budget > 0 && scanned >= budget {
			return true
		}
		idx := it.it.Next()
		scanned++
		if it.examined != nil {
			*it.examined = *it.examined + 1
		}
		if it.branchExamined != nil {
			*it.branchExamined = *it.branchExamined + 1
		}
		if it.branch.matchesFromLead(idx) {
			it.cur = idx
			it.has = true
			if it.branchEmitted != nil {
				*it.branchEmitted = *it.branchEmitted + 1
			}
			return false
		}
	}
	return false
}

func (db *DB[K, V]) tryPlanORMerge(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	if q.Limit == 0 {
		return nil, false, nil
	}
	if len(q.Order) > 1 {
		return nil, false, nil
	}
	if q.Expr.Op != qx.OpOR || q.Expr.Not {
		return nil, false, nil
	}
	if len(q.Expr.Operands) < plannerORMinOperandCount {
		return nil, false, nil
	}
	if len(q.Expr.Operands) > plannerORBranchLimit {
		return nil, false, nil
	}
	if len(q.Order) == 0 && !hasNoOrderLeadCandidatesOR(q.Expr.Operands) {
		return nil, false, nil
	}

	branches, alwaysFalse, ok := db.buildORBranches(q.Expr.Operands)
	if !ok {
		return nil, false, nil
	}
	if alwaysFalse {
		return nil, true, nil
	}
	defer releaseORBranches(branches)

	if len(q.Order) == 0 {
		if q.Limit > plannerORNoOrderLimitMax || q.Offset > plannerORNoOrderOffsetMax {
			return nil, false, nil
		}
		noOrderDecision := db.decidePlanORNoOrder(q, branches)
		if trace != nil {
			trace.setEstimated(noOrderDecision.expectedRows, noOrderDecision.kWayCost, noOrderDecision.fallbackCost)
		}
		if !noOrderDecision.use {
			return nil, false, nil
		}
		out, ok := db.execPlanORNoOrder(q, branches, trace)
		if !ok {
			return nil, false, nil
		}
		if trace != nil {
			trace.setPlan(PlanORMergeNoOrder)
		}
		return out, true, nil
	}

	o := q.Order[0]
	if o.Type != qx.OrderBasic {
		return nil, false, nil
	}
	if q.Limit > plannerOROrderLimitMax || q.Offset > plannerOROrderOffsetMax {
		return nil, false, nil
	}

	orderDecision := db.decidePlanOROrder(q, branches)
	if trace != nil {
		trace.setEstimated(orderDecision.expectedRows, orderDecision.bestCost, orderDecision.fallbackCost)
	}

	switch orderDecision.plan {
	case plannerOROrderMerge:
		out, ok, err := db.execPlanOROrderMerge(q, branches, trace)
		if ok {
			if trace != nil {
				trace.setPlan(PlanORMergeOrderMerge)
			}
			return out, true, err
		}
		// merge estimate can be optimistic if branch overlap is high;
		// fall back to streaming OR order plan before giving up.
		out2, ok2 := db.execPlanOROrderBasic(q, branches, trace)
		if ok2 {
			if trace != nil {
				trace.setPlan(PlanORMergeOrderStream)
			}
			return out2, true, nil
		}
		return nil, false, nil
	case plannerOROrderStream:
		out, ok := db.execPlanOROrderBasic(q, branches, trace)
		if !ok {
			return nil, false, nil
		}
		if trace != nil {
			trace.setPlan(PlanORMergeOrderStream)
		}
		return out, true, nil
	default:
		return nil, false, nil
	}
}

func releaseORBranches(branches []plannerORBranch) {
	for i := range branches {
		releasePredicates(branches[i].preds)
	}
}

func hasNoOrderLeadCandidatesOR(ops []qx.Expr) bool {
	for _, op := range ops {
		if !branchHasPositiveLeafOR(op) {
			return false
		}
	}
	return true
}

func branchHasPositiveLeafOR(e qx.Expr) bool {
	switch e.Op {
	case qx.OpAND:
		if e.Not || len(e.Operands) == 0 {
			return false
		}
		for _, ch := range e.Operands {
			if branchHasPositiveLeafOR(ch) {
				return true
			}
		}
		return false
	case qx.OpNOOP, qx.OpOR:
		return false
	default:
		if e.Not {
			return false
		}
		return true
	}
}

func (db *DB[K, V]) buildORBranches(ops []qx.Expr) ([]plannerORBranch, bool, bool) {
	out := make([]plannerORBranch, 0, len(ops))

	for _, op := range ops {
		leaves, ok := collectAndLeaves(op)
		if !ok {
			releaseORBranches(out)
			return nil, false, false
		}

		preds, ok := db.buildPredicates(leaves)
		if !ok {
			releaseORBranches(out)
			return nil, false, false
		}

		branch := plannerORBranch{
			expr:  op,
			preds: preds,
		}

		hasFalse := false
		allTrue := true
		leadIdx := -1

		for i := range preds {
			p := &preds[i]
			if p.alwaysFalse {
				hasFalse = true
				break
			}
			if p.alwaysTrue {
				continue
			}
			allTrue = false
			if p.contains == nil {
				releasePredicates(preds)
				releaseORBranches(out)
				return nil, false, false
			}
			if p.iter != nil {
				if leadIdx == -1 || p.estCard < preds[leadIdx].estCard {
					leadIdx = i
				}
			}
		}

		if hasFalse {
			releasePredicates(preds)
			continue
		}
		if allTrue {
			branch.alwaysTrue = true
		} else {
			branch.leadIdx = leadIdx
			if leadIdx >= 0 {
				branch.lead = &preds[leadIdx]
			}
		}

		out = append(out, branch)
	}

	if len(out) == 0 {
		return nil, true, true
	}

	return out, false, true
}

func (db *DB[K, V]) execPlanOROrderBasic(q *qx.QX, branches []plannerORBranch, trace *queryTrace) ([]K, bool) {
	o := q.Order[0]
	f := o.Field
	if f == "" {
		return nil, false
	}

	fm := db.fields[f]
	if fm == nil || fm.Slice {
		return nil, false
	}

	slice := db.index[f]
	if slice == nil {
		return nil, false
	}
	s := *slice
	if len(s) == 0 {
		return nil, true
	}

	alwaysTrue := false
	alwaysTrueBranch := -1
	for i := range branches {
		if branches[i].alwaysTrue {
			alwaysTrue = true
			alwaysTrueBranch = i
			break
		}
	}

	skip := q.Offset
	need := int(q.Limit)
	out := make([]K, 0, need)

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	matches := func(idx uint64) bool {
		if alwaysTrue {
			return true
		}
		for i := range branches {
			if branches[i].matches(idx) {
				return true
			}
		}
		return false
	}

	if trace == nil {
		if !o.Desc {
			for i := 0; i < len(s); i++ {
				bm := s[i].IDs
				if bm == nil || bm.IsEmpty() {
					continue
				}
				it := bm.Iterator()
				for it.HasNext() {
					idx := it.Next()
					if !matches(idx) {
						continue
					}
					if skip > 0 {
						skip--
						continue
					}
					out = append(out, db.idFromIdxNoLock(idx))
					need--
					if need == 0 {
						return out, true
					}
				}
			}
			return out, true
		}

		for i := len(s) - 1; i >= 0; i-- {
			bm := s[i].IDs
			if bm == nil || bm.IsEmpty() {
				if i == 0 {
					break
				}
				continue
			}
			it := bm.Iterator()
			for it.HasNext() {
				idx := it.Next()
				if !matches(idx) {
					continue
				}
				if skip > 0 {
					skip--
					continue
				}
				out = append(out, db.idFromIdxNoLock(idx))
				need--
				if need == 0 {
					return out, true
				}
			}
			if i == 0 {
				break
			}
		}
		return out, true
	}

	branchMetrics := make([]TraceORBranch, len(branches))
	for i := range branchMetrics {
		branchMetrics[i].Index = i
	}

	matchWithMetrics := func(idx uint64) bool {
		if alwaysTrue {
			if alwaysTrueBranch >= 0 {
				branchMetrics[alwaysTrueBranch].RowsExamined++
				branchMetrics[alwaysTrueBranch].RowsEmitted++
			}
			return true
		}

		matchedCount := 0
		for i := range branches {
			branchMetrics[i].RowsExamined++
			if branches[i].matches(idx) {
				branchMetrics[i].RowsEmitted++
				matchedCount++
			}
		}
		if matchedCount > 1 {
			trace.addDedupe(uint64(matchedCount - 1))
		}
		return matchedCount > 0
	}

	scanWidth := uint64(0)

	if !o.Desc {
		for i := 0; i < len(s); i++ {
			bm := s[i].IDs
			if bm == nil || bm.IsEmpty() {
				continue
			}
			scanWidth++
			trace.addExamined(bm.GetCardinality())
			it := bm.Iterator()
			for it.HasNext() {
				idx := it.Next()
				if !matchWithMetrics(idx) {
					continue
				}
				if skip > 0 {
					skip--
					continue
				}
				out = append(out, db.idFromIdxNoLock(idx))
				need--
				if need == 0 {
					trace.addOrderScanWidth(scanWidth)
					trace.setORBranches(branchMetrics)
					trace.setEarlyStopReason("limit_reached")
					return out, true
				}
			}
		}
		trace.addOrderScanWidth(scanWidth)
		trace.setORBranches(branchMetrics)
		trace.setEarlyStopReason("input_exhausted")
		return out, true
	}

	for i := len(s) - 1; i >= 0; i-- {
		bm := s[i].IDs
		if bm == nil || bm.IsEmpty() {
			if i == 0 {
				break
			}
			continue
		}
		scanWidth++
		trace.addExamined(bm.GetCardinality())
		it := bm.Iterator()
		for it.HasNext() {
			idx := it.Next()
			if !matchWithMetrics(idx) {
				continue
			}
			if skip > 0 {
				skip--
				continue
			}
			out = append(out, db.idFromIdxNoLock(idx))
			need--
			if need == 0 {
				trace.addOrderScanWidth(scanWidth)
				trace.setORBranches(branchMetrics)
				trace.setEarlyStopReason("limit_reached")
				return out, true
			}
		}
		if i == 0 {
			break
		}
	}

	trace.addOrderScanWidth(scanWidth)
	trace.setORBranches(branchMetrics)
	trace.setEarlyStopReason("input_exhausted")
	return out, true
}

func orderWindow(q *qx.QX) (int, bool) {
	if q.Limit == 0 {
		return 0, false
	}

	need := q.Offset + q.Limit
	if need < q.Offset {
		return 0, false
	}
	if need > uint64(math.MaxInt) {
		return 0, false
	}
	return int(need), true
}

func plannerORBranchContainsChecks(branch plannerORBranch) int {
	if branch.alwaysTrue {
		return 0
	}
	checks := 0
	for i := range branch.preds {
		p := branch.preds[i]
		if p.alwaysTrue || p.alwaysFalse || p.contains == nil {
			continue
		}
		checks++
	}
	return checks
}

func plannerORAvgContainsChecks(branches []plannerORBranch) float64 {
	totalChecks := 0.0
	activeBranches := 0.0
	for i := range branches {
		if branches[i].alwaysTrue {
			continue
		}
		totalChecks += float64(plannerORBranchContainsChecks(branches[i]))
		activeBranches++
	}
	if activeBranches == 0 {
		return 0
	}
	return totalChecks / activeBranches
}

func (db *DB[K, V]) shouldPreferOROrderFallbackFirst(q *qx.QX, branches []plannerORBranch) bool {
	need, ok := orderWindow(q)
	if !ok || need <= 0 {
		return false
	}
	if len(q.Order) != 1 {
		return false
	}

	orderField := q.Order[0].Field
	hasPrefixNonOrder := hasPrefixOnNonOrderFieldOR(branches, orderField)

	avgChecks := plannerORAvgContainsChecks(branches)
	if avgChecks <= 0 {
		return false
	}

	if q.Offset >= plannerOROrderFallbackFirstOffsetMin && need >= plannerOROrderFallbackFirstNeedMin && avgChecks >= plannerOROrderFallbackFirstAvgChecksMin {
		return true
	}
	if need >= plannerOROrderMergeNeedMax/4 && avgChecks >= plannerOROrderFallbackFirstWideChecksMin {
		return true
	}
	if hasPrefixNonOrder && q.Offset > 0 && avgChecks >= plannerOROrderFallbackFirstAvgChecksMin {
		return true
	}
	return false
}

func (db *DB[K, V]) shouldUseOROrderKWayRuntimeFallback(q *qx.QX, branches []plannerORBranch, needWindow int) bool {
	if needWindow < plannerORKWayRuntimeNeedMin {
		return false
	}
	if len(q.Order) != 1 || len(branches) < 2 {
		return false
	}

	orderField := q.Order[0].Field
	if orderField == "" {
		return false
	}

	avgChecks := plannerORAvgContainsChecks(branches)
	if avgChecks < plannerORKWayRuntimeAvgChecksEnableMin {
		return false
	}

	// Small unpaged windows often favor k-way despite predicate complexity.
	if q.Offset == 0 && needWindow < plannerOROrderFallbackFirstNeedMin {
		return false
	}

	hasPrefixNonOrder := hasPrefixOnNonOrderFieldOR(branches, orderField)
	if hasPrefixNonOrder && q.Offset > 0 {
		return true
	}

	return q.Offset > 0 || needWindow >= plannerOROrderFallbackFirstNeedMin
}

func plannerORKWayShouldFallbackRuntime(needWindow int, pops int, unique uint64, examined uint64) bool {
	if needWindow < plannerORKWayRuntimeNeedMin {
		return false
	}
	if pops < plannerORKWayRuntimeMinPops {
		return false
	}
	if unique < plannerORKWayRuntimeMinUnique {
		return false
	}
	if examined < plannerORKWayRuntimeMinExamined {
		return false
	}

	examinedPerUnique := float64(examined) / float64(unique)
	if examinedPerUnique < plannerORKWayRuntimeExaminedPerUniqueMin {
		return false
	}

	projectedExamined := examinedPerUnique * float64(needWindow)
	return projectedExamined >= plannerORKWayRuntimeProjectedExaminedMax
}

func (db *DB[K, V]) execPlanOROrderMerge(q *qx.QX, branches []plannerORBranch, trace *queryTrace) ([]K, bool, error) {
	if db.shouldPreferOROrderFallbackFirst(q, branches) {
		return db.execPlanOROrderMergeFallback(q, branches, trace)
	}
	out, ok, err := db.execPlanOROrderKWay(q, branches, trace)
	if ok || err != nil {
		return out, ok, err
	}
	return db.execPlanOROrderMergeFallback(q, branches, trace)
}

type plannerOROrderMergeItem struct {
	branch int
	bucket int
	idx    uint64
}

type plannerOROrderMergeHeap struct {
	desc  bool
	items []plannerOROrderMergeItem
}

func (h plannerOROrderMergeHeap) Len() int { return len(h.items) }

func (h plannerOROrderMergeHeap) Less(i, j int) bool {
	a := h.items[i]
	b := h.items[j]
	if a.bucket != b.bucket {
		if h.desc {
			return a.bucket > b.bucket
		}
		return a.bucket < b.bucket
	}
	if a.idx != b.idx {
		// Keep deterministic in-bucket ordering aligned with roaring iterator.
		return a.idx < b.idx
	}
	return a.branch < b.branch
}

func (h plannerOROrderMergeHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

func (h *plannerOROrderMergeHeap) Push(x any) { h.items = append(h.items, x.(plannerOROrderMergeItem)) }

func (h *plannerOROrderMergeHeap) Pop() any {
	old := h.items
	n := len(old)
	x := old[n-1]
	h.items = old[:n-1]
	return x
}

type plannerOROrderBranchIter struct {
	branch  *plannerORBranch
	buckets []index
	desc    bool

	nextBucket int
	curBucket  int
	curIt      roaringIter

	onBucketVisit func(int)

	totalExamined  *uint64
	branchExamined *uint64
	branchEmitted  *uint64

	has bool
	cur uint64
}

func (it *plannerOROrderBranchIter) init() {
	if it.desc {
		it.nextBucket = len(it.buckets) - 1
	} else {
		it.nextBucket = 0
	}
	it.curBucket = -1
}

func (it *plannerOROrderBranchIter) advance() bool {
	it.has = false
	for {
		if it.curIt != nil {
			for it.curIt.HasNext() {
				idx := it.curIt.Next()
				if it.totalExamined != nil {
					*it.totalExamined = *it.totalExamined + 1
				}
				if it.branchExamined != nil {
					*it.branchExamined = *it.branchExamined + 1
				}
				if it.branch.matches(idx) {
					it.cur = idx
					it.has = true
					if it.branchEmitted != nil {
						*it.branchEmitted = *it.branchEmitted + 1
					}
					return true
				}
			}
			it.curIt = nil
		}

		if it.desc {
			if it.nextBucket < 0 {
				return false
			}
			b := it.nextBucket
			it.nextBucket--
			bm := it.buckets[b].IDs
			if bm == nil || bm.IsEmpty() {
				continue
			}
			if it.onBucketVisit != nil {
				it.onBucketVisit(b)
			}
			it.curBucket = b
			it.curIt = bm.Iterator()
			continue
		}

		if it.nextBucket >= len(it.buckets) {
			return false
		}
		b := it.nextBucket
		it.nextBucket++
		bm := it.buckets[b].IDs
		if bm == nil || bm.IsEmpty() {
			continue
		}
		if it.onBucketVisit != nil {
			it.onBucketVisit(b)
		}
		it.curBucket = b
		it.curIt = bm.Iterator()
	}
}

func (db *DB[K, V]) execPlanOROrderKWay(q *qx.QX, branches []plannerORBranch, trace *queryTrace) ([]K, bool, error) {
	needWindow, ok := orderWindow(q)
	if !ok || needWindow <= 0 {
		return nil, false, nil
	}

	o := q.Order[0]
	fm := db.fields[o.Field]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}
	slice := db.index[o.Field]
	if slice == nil {
		return nil, false, nil
	}
	s := *slice
	if len(s) == 0 {
		return nil, true, nil
	}

	for i := range branches {
		if branches[i].alwaysTrue {
			return nil, false, nil
		}
	}
	allowRuntimeFallback := db.shouldUseOROrderKWayRuntimeFallback(q, branches, needWindow)

	var (
		branchMetrics []TraceORBranch
		examined      uint64
		scanWidth     uint64
		dedupe        uint64
	)
	var examinedPtr *uint64
	if trace != nil {
		branchMetrics = make([]TraceORBranch, len(branches))
		for i := range branchMetrics {
			branchMetrics[i].Index = i
		}
		examinedPtr = &examined
	}

	bucketSeen := make([]bool, len(s))
	markBucketVisited := func(bucket int) {
		if trace == nil || bucket < 0 || bucket >= len(bucketSeen) {
			return
		}
		if bucketSeen[bucket] {
			return
		}
		bucketSeen[bucket] = true
		scanWidth++
	}

	iters := make([]plannerOROrderBranchIter, len(branches))
	h := &plannerOROrderMergeHeap{
		desc:  o.Desc,
		items: make([]plannerOROrderMergeItem, 0, len(branches)),
	}

	for i := range branches {
		var bePtr *uint64
		var bmPtr *uint64
		if trace != nil {
			bePtr = &branchMetrics[i].RowsExamined
			bmPtr = &branchMetrics[i].RowsEmitted
		}
		iters[i] = plannerOROrderBranchIter{
			branch:         &branches[i],
			buckets:        s,
			desc:           o.Desc,
			onBucketVisit:  markBucketVisited,
			totalExamined:  examinedPtr,
			branchExamined: bePtr,
			branchEmitted:  bmPtr,
		}
		iters[i].init()
		if iters[i].advance() {
			heap.Push(h, plannerOROrderMergeItem{
				branch: i,
				bucket: iters[i].curBucket,
				idx:    iters[i].cur,
			})
		} else if trace != nil {
			branchMetrics[i].Skipped = true
			branchMetrics[i].SkipReason = "stream_empty"
		}
	}

	if h.Len() == 0 {
		if trace != nil {
			trace.addExamined(examined)
			trace.addOrderScanWidth(scanWidth)
			trace.setORBranches(branchMetrics)
			trace.setEarlyStopReason("no_candidates")
		}
		return nil, true, nil
	}

	outCap := int(q.Limit)
	if outCap <= 0 {
		outCap = needWindow
	}
	out := make([]K, 0, outCap)
	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	seen := newU64Set(max(64, needWindow*2))
	skip := q.Offset
	needOut := q.Limit
	stopReason := "input_exhausted"
	pops := 0
	unique := uint64(0)

	for h.Len() > 0 {
		item := heap.Pop(h).(plannerOROrderMergeItem)
		bi := item.branch
		pops++

		if iters[bi].advance() {
			heap.Push(h, plannerOROrderMergeItem{
				branch: bi,
				bucket: iters[bi].curBucket,
				idx:    iters[bi].cur,
			})
		}

		if !seen.Add(item.idx) {
			dedupe++
			continue
		}
		unique++
		if skip > 0 {
			skip--
			if allowRuntimeFallback && plannerORKWayShouldFallbackRuntime(needWindow, pops, unique, examined) {
				return nil, false, nil
			}
			continue
		}

		out = append(out, db.idFromIdxNoLock(item.idx))
		needOut--
		if needOut == 0 {
			stopReason = "limit_reached"
			break
		}
		if allowRuntimeFallback && plannerORKWayShouldFallbackRuntime(needWindow, pops, unique, examined) {
			return nil, false, nil
		}
	}

	if trace != nil {
		trace.addExamined(examined)
		trace.addDedupe(dedupe)
		trace.addOrderScanWidth(scanWidth)
		trace.setORBranches(branchMetrics)
		trace.setEarlyStopReason(stopReason)
	}
	return out, true, nil
}

func (db *DB[K, V]) execPlanOROrderMergeFallback(q *qx.QX, branches []plannerORBranch, trace *queryTrace) ([]K, bool, error) {
	need, ok := orderWindow(q)
	if !ok || need <= 0 {
		return nil, false, nil
	}

	candidateCap := need * len(branches)
	if candidateCap < 64 {
		candidateCap = 64
	}
	candidateSet := newU64Set(candidateCap)

	var branchMetrics []TraceORBranch
	if trace != nil {
		branchMetrics = make([]TraceORBranch, len(branches))
		for i := range branchMetrics {
			branchMetrics[i].Index = i
		}
	}

	for i := range branches {
		branch := branches[i]
		if branch.alwaysTrue {
			return nil, false, nil
		}

		branchLimit, err := db.planOROrderMergeBranchLimit(branch, need)
		if err != nil {
			return nil, true, err
		}
		if branchLimit <= 0 {
			if trace != nil {
				branchMetrics[i].Skipped = true
				branchMetrics[i].SkipReason = "branch_limit_zero"
			}
			continue
		}

		subQ := &qx.QX{
			Expr:  branch.expr,
			Order: q.Order,
			Limit: uint64(branchLimit),
		}

		ids, err := db.queryNoTrace(subQ)
		if err != nil {
			return nil, true, err
		}
		if trace != nil {
			trace.addExamined(uint64(len(ids)))
			branchMetrics[i].RowsExamined += uint64(len(ids))
			branchMetrics[i].RowsEmitted += uint64(len(ids))
		}
		if len(ids) == 0 {
			continue
		}

		idxs := db.idxsFromID(ids)
		for _, idx := range idxs {
			if candidateSet.Has(idx) {
				if trace != nil {
					trace.addDedupe(1)
				}
				continue
			}
			candidateSet.Add(idx)
		}
	}

	if candidateSet.Len() == 0 {
		if trace != nil {
			trace.setORBranches(branchMetrics)
			trace.setEarlyStopReason("no_candidates")
		}
		return nil, true, nil
	}

	o := q.Order[0]
	fm := db.fields[o.Field]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}
	slice := db.index[o.Field]
	if slice == nil {
		return nil, false, nil
	}
	s := *slice
	if len(s) == 0 {
		return nil, true, nil
	}

	skip := q.Offset
	needOut := q.Limit
	if needOut == 0 {
		return nil, true, nil
	}

	out := make([]K, 0, int(min(needOut, uint64(candidateSet.Len()))))
	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	seenCandidates := 0
	totalCandidates := candidateSet.Len()
	scanWidth := uint64(0)

	emit := func(idx uint64) bool {
		if !candidateSet.Has(idx) {
			return false
		}
		seenCandidates++
		if skip > 0 {
			skip--
			return false
		}
		out = append(out, db.idFromIdxNoLock(idx))
		needOut--
		return needOut == 0
	}

	if !o.Desc {
		for i := 0; i < len(s); i++ {
			bm := s[i].IDs
			if bm == nil || bm.IsEmpty() {
				continue
			}
			scanWidth++
			it := bm.Iterator()
			for it.HasNext() {
				if emit(it.Next()) {
					if trace != nil {
						trace.addExamined(uint64(seenCandidates))
						trace.addOrderScanWidth(scanWidth)
						trace.setORBranches(branchMetrics)
						trace.setEarlyStopReason("limit_reached")
					}
					return out, true, nil
				}
			}
			if seenCandidates == totalCandidates {
				break
			}
		}
	} else {
		for i := len(s) - 1; i >= 0; i-- {
			bm := s[i].IDs
			if bm == nil || bm.IsEmpty() {
				if i == 0 {
					break
				}
				continue
			}
			scanWidth++
			it := bm.Iterator()
			for it.HasNext() {
				if emit(it.Next()) {
					if trace != nil {
						trace.addExamined(uint64(seenCandidates))
						trace.addOrderScanWidth(scanWidth)
						trace.setORBranches(branchMetrics)
						trace.setEarlyStopReason("limit_reached")
					}
					return out, true, nil
				}
			}
			if seenCandidates == totalCandidates {
				break
			}
			if i == 0 {
				break
			}
		}
	}

	if trace != nil {
		trace.addExamined(uint64(seenCandidates))
		trace.addOrderScanWidth(scanWidth)
		trace.setORBranches(branchMetrics)
		if seenCandidates == totalCandidates {
			trace.setEarlyStopReason("candidates_exhausted")
		} else {
			trace.setEarlyStopReason("order_index_exhausted")
		}
	}
	return out, true, nil
}

func (db *DB[K, V]) planOROrderMergeBranchLimit(branch plannerORBranch, need int) (int, error) {
	if need <= 0 {
		return 0, nil
	}

	// Default safe limit: full window size.
	limit := need
	if need < 32 {
		return limit, nil
	}

	leaves, ok := collectAndLeaves(branch.expr)
	if !ok || len(leaves) == 0 || len(leaves) > 4 {
		return limit, nil
	}

	// Keep branch pre-counting conservative:
	// for broad/range/string scans it is usually not worth extra Count().
	for _, e := range leaves {
		if e.Not {
			return limit, nil
		}
		switch e.Op {
		case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX, qx.OpSUFFIX, qx.OpCONTAINS:
			return limit, nil
		}
	}

	cnt, err := db.Count(&qx.QX{Expr: branch.expr})
	if err != nil {
		return 0, err
	}
	if cnt == 0 {
		return 0, nil
	}
	if cnt < uint64(limit) {
		return int(cnt), nil
	}
	return limit, nil
}

type plannerORNoOrderBranchState struct {
	index int

	score  float64
	budget uint64

	initialized bool
	capped      bool
	exhausted   bool

	iter plannerORIter
}

func plannerORNoOrderBranchScore(branch plannerORBranch) float64 {
	if branch.alwaysTrue {
		return 0
	}
	var leadEst uint64 = 1
	if branch.lead != nil && branch.lead.estCard > 0 {
		leadEst = branch.lead.estCard
	}
	extraChecks := 1.0
	for i := range branch.preds {
		if i == branch.leadIdx {
			continue
		}
		p := branch.preds[i]
		if p.alwaysTrue {
			continue
		}
		extraChecks += 1.0
	}
	return float64(leadEst) * extraChecks
}

func plannerORNoOrderBranchBudget(branch plannerORBranch) uint64 {
	if branch.alwaysTrue {
		return plannerORNoOrderMinBranchBudget
	}

	leadEst := uint64(0)
	if branch.lead != nil {
		leadEst = branch.lead.estCard
	}
	if leadEst == 0 {
		leadEst = plannerORNoOrderMinBranchBudget
	}

	budget := leadEst / 64
	if budget < plannerORNoOrderMinBranchBudget {
		budget = plannerORNoOrderMinBranchBudget
	}
	if budget > plannerORNoOrderMaxBranchBudget {
		budget = plannerORNoOrderMaxBranchBudget
	}
	return budget
}

func plannerORNoOrderInsertTopN(top []uint64, idx uint64, n int) ([]uint64, bool) {
	if n <= 0 {
		return top, false
	}
	pos := sort.Search(len(top), func(i int) bool { return top[i] >= idx })
	if pos < len(top) && top[pos] == idx {
		return top, false
	}

	if len(top) < n {
		top = append(top, 0)
		copy(top[pos+1:], top[pos:])
		top[pos] = idx
		return top, true
	}

	if idx > top[len(top)-1] {
		return top, false
	}
	copy(top[pos+1:], top[pos:len(top)-1])
	top[pos] = idx
	return top, true
}

func (db *DB[K, V]) execPlanORNoOrder(q *qx.QX, branches []plannerORBranch, trace *queryTrace) ([]K, bool) {
	for i := range branches {
		if branches[i].alwaysTrue {
			return db.execPlanORUniverseNoOrder(q, trace), true
		}
		if branches[i].lead == nil || branches[i].lead.iter == nil {
			return nil, false
		}
	}

	if q.Offset == 0 && len(branches) >= 3 {
		if out, ok := db.execPlanORNoOrderAdaptive(q, branches, trace); ok {
			return out, true
		}
	}
	return db.execPlanORNoOrderBaseline(q, branches, trace)
}

func (db *DB[K, V]) execPlanORNoOrderAdaptive(q *qx.QX, branches []plannerORBranch, trace *queryTrace) ([]K, bool) {
	if q.Limit == 0 || q.Offset != 0 {
		return nil, false
	}

	need := int(q.Limit)
	if need <= 0 {
		return nil, true
	}

	var (
		examined    uint64
		examinedPtr *uint64
	)
	var branchMetrics []TraceORBranch
	if trace != nil {
		examinedPtr = &examined
		branchMetrics = make([]TraceORBranch, len(branches))
		for i := range branchMetrics {
			branchMetrics[i].Index = i
		}
	}

	states := make([]plannerORNoOrderBranchState, len(branches))
	order := make([]int, len(branches))
	for i := range branches {
		state := &states[i]
		state.index = i
		state.score = plannerORNoOrderBranchScore(branches[i])
		state.budget = plannerORNoOrderBranchBudget(branches[i])
		order[i] = i

		var branchExaminedPtr *uint64
		var branchEmittedPtr *uint64
		if trace != nil {
			branchExaminedPtr = &branchMetrics[i].RowsExamined
			branchEmittedPtr = &branchMetrics[i].RowsEmitted
		}
		state.iter = plannerORIter{
			it:             branches[i].lead.iter(),
			branch:         &branches[i],
			examined:       examinedPtr,
			branchExamined: branchExaminedPtr,
			branchEmitted:  branchEmittedPtr,
		}
	}

	sort.Slice(order, func(i, j int) bool {
		a := states[order[i]]
		b := states[order[j]]
		if a.score == b.score {
			return a.index < b.index
		}
		return a.score < b.score
	})

	nextBudget := func(cur uint64) uint64 {
		if cur < plannerORNoOrderMinBranchBudget {
			return plannerORNoOrderMinBranchBudget
		}
		n := cur * 2
		if n > plannerORNoOrderMaxBranchBudget {
			n = plannerORNoOrderMaxBranchBudget
		}
		return n
	}

	probeBranch := func(st *plannerORNoOrderBranchState) {
		if st.exhausted || st.iter.has {
			return
		}
		st.initialized = true
		budgetHit := st.iter.advanceWithBudget(st.budget)
		if st.iter.has {
			st.capped = false
			return
		}
		if budgetHit {
			st.capped = true
			st.budget = nextBudget(st.budget)
			return
		}
		st.capped = false
		st.exhausted = true
	}

	consumeBranch := func(st *plannerORNoOrderBranchState) {
		if !st.iter.has {
			return
		}
		budgetHit := st.iter.advanceWithBudget(st.budget)
		if st.iter.has {
			st.capped = false
			return
		}
		if budgetHit {
			st.capped = true
			st.budget = nextBudget(st.budget)
			return
		}
		st.capped = false
		st.exhausted = true
	}

	findReadyMin := func(maxInclusive uint64, useMax bool) *plannerORNoOrderBranchState {
		var best *plannerORNoOrderBranchState
		for i := range states {
			st := &states[i]
			if !st.iter.has {
				continue
			}
			if useMax && st.iter.cur > maxInclusive {
				continue
			}
			if best == nil || st.iter.cur < best.iter.cur {
				best = st
			}
		}
		return best
	}

	findNextUninitialized := func() *plannerORNoOrderBranchState {
		for _, idx := range order {
			st := &states[idx]
			if st.initialized || st.exhausted {
				continue
			}
			return st
		}
		return nil
	}

	findNextCapped := func() *plannerORNoOrderBranchState {
		for _, idx := range order {
			st := &states[idx]
			if st.exhausted || !st.capped || st.iter.has {
				continue
			}
			return st
		}
		return nil
	}

	seen := newU64Set(max(64, need*2))
	top := make([]uint64, 0, need)

	addCandidate := func(idx uint64) {
		if len(top) == need && idx > top[len(top)-1] {
			return
		}
		if seen.Has(idx) {
			if trace != nil {
				trace.addDedupe(1)
			}
			return
		}
		top2, inserted := plannerORNoOrderInsertTopN(top, idx, need)
		if !inserted {
			return
		}
		top = top2
		seen.Add(idx)
	}

	initialActive := plannerORNoOrderInitActiveBranches
	if initialActive > len(order) {
		initialActive = len(order)
	}
	for i := 0; i < initialActive; i++ {
		probeBranch(&states[order[i]])
	}

	// Phase 1: gather enough unique candidates from cheap-first branches.
	for len(top) < need {
		st := findReadyMin(0, false)
		if st != nil {
			addCandidate(st.iter.cur)
			consumeBranch(st)
			continue
		}

		if st = findNextUninitialized(); st != nil {
			probeBranch(st)
			continue
		}
		if st = findNextCapped(); st != nil {
			probeBranch(st)
			continue
		}
		break
	}

	earlyStopReason := "input_exhausted"

	// Phase 2: if LIMIT is already filled, prove all unseen branches cannot
	// contribute ids <= current threshold.
	for len(top) == need {
		threshold := top[len(top)-1]

		st := findReadyMin(threshold, true)
		if st != nil {
			addCandidate(st.iter.cur)
			consumeBranch(st)
			continue
		}

		if st = findNextUninitialized(); st != nil {
			probeBranch(st)
			continue
		}
		if st = findNextCapped(); st != nil {
			probeBranch(st)
			continue
		}

		earlyStopReason = "limit_reached"
		break
	}

	if trace != nil {
		trace.addExamined(examined)

		if len(top) == need {
			threshold := top[len(top)-1]
			for i := range states {
				st := &states[i]
				if st.exhausted {
					continue
				}
				if st.iter.has && st.iter.cur > threshold {
					branchMetrics[i].Skipped = true
					branchMetrics[i].SkipReason = "threshold_pruned"
				}
			}
		}

		trace.setORBranches(branchMetrics)
		trace.setEarlyStopReason(earlyStopReason)
	}

	if len(top) == 0 {
		return nil, true
	}

	out := make([]K, 0, len(top))
	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}
	for _, idx := range top {
		out = append(out, db.idFromIdxNoLock(idx))
	}
	return out, true
}

func (db *DB[K, V]) execPlanORNoOrderBaseline(q *qx.QX, branches []plannerORBranch, trace *queryTrace) ([]K, bool) {
	var (
		examined    uint64
		examinedPtr *uint64
	)
	var branchMetrics []TraceORBranch
	if trace != nil {
		examinedPtr = &examined
		branchMetrics = make([]TraceORBranch, len(branches))
		for i := range branchMetrics {
			branchMetrics[i].Index = i
		}
	}
	iters := make([]plannerORIter, 0, len(branches))
	for i := range branches {
		var branchExaminedPtr *uint64
		var branchEmittedPtr *uint64
		if trace != nil {
			branchExaminedPtr = &branchMetrics[i].RowsExamined
			branchEmittedPtr = &branchMetrics[i].RowsEmitted
		}
		iters = append(iters, plannerORIter{
			it:             branches[i].lead.iter(),
			branch:         &branches[i],
			examined:       examinedPtr,
			branchExamined: branchExaminedPtr,
			branchEmitted:  branchEmittedPtr,
		})
	}
	for i := range iters {
		iters[i].advance()
	}

	skip := q.Offset
	need := int(q.Limit)
	out := make([]K, 0, need)

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	for need > 0 {
		minIdx := uint64(math.MaxUint64)
		minPos := -1

		for i := range iters {
			if !iters[i].has {
				continue
			}
			if minPos == -1 || iters[i].cur < minIdx {
				minIdx = iters[i].cur
				minPos = i
			}
		}

		if minPos == -1 {
			if trace != nil {
				trace.setEarlyStopReason("input_exhausted")
			}
			break
		}

		dupHits := uint64(0)
		for i := range iters {
			if iters[i].has && iters[i].cur == minIdx {
				dupHits++
				iters[i].advance()
			}
		}
		if trace != nil && dupHits > 1 {
			trace.addDedupe(dupHits - 1)
		}

		if skip > 0 {
			skip--
			continue
		}

		out = append(out, db.idFromIdxNoLock(minIdx))
		need--
		if need == 0 && trace != nil {
			trace.setEarlyStopReason("limit_reached")
		}
	}

	if trace != nil {
		trace.addExamined(examined)
		trace.setORBranches(branchMetrics)
		trace.setEarlyStopReason("input_exhausted")
	}
	return out, true
}

func (db *DB[K, V]) execPlanORUniverseNoOrder(q *qx.QX, trace *queryTrace) []K {
	skip := q.Offset
	need := int(q.Limit)
	out := make([]K, 0, need)

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}
	if trace != nil {
		trace.addExamined(db.universe.GetCardinality())
	}

	it := db.universe.Iterator()
	for it.HasNext() {
		idx := it.Next()
		if skip > 0 {
			skip--
			continue
		}
		out = append(out, db.idFromIdxNoLock(idx))
		need--
		if need == 0 {
			if trace != nil {
				trace.setEarlyStopReason("limit_reached")
			}
			break
		}
	}
	if trace != nil {
		trace.setEarlyStopReason("input_exhausted")
	}
	return out
}

const (
	plannerOrderLeadLimitMax         = 256
	plannerOrderLeadLeadCardMax      = 200_000
	plannerOrderLeadCandidateCardMax = 300_000
)

func (db *DB[K, V]) tryPlanOrderLead(q *qx.QX, trace *queryTrace) ([]K, bool, error) {
	if q.Limit == 0 || q.Limit > plannerOrderLeadLimitMax {
		return nil, false, nil
	}
	if len(q.Order) != 1 || q.Order[0].Type != qx.OrderBasic {
		return nil, false, nil
	}

	leaves, ok := collectAndLeaves(q.Expr)
	if !ok || len(leaves) == 0 {
		return nil, false, nil
	}

	hasPrefix := false
	hasNeg := false
	for _, e := range leaves {
		if e.Op == qx.OpPREFIX {
			hasPrefix = true
		}
		if e.Not {
			hasNeg = true
		}
	}
	// Targeted at autocomplete-like queries where baseline path materializes
	// expensive intermediate bitmaps.
	if !hasPrefix || !hasNeg {
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

	out, ok := db.execPlanOrderLead(q, preds, trace)
	if !ok {
		return nil, false, nil
	}
	if trace != nil {
		trace.setPlan(PlanOrderedLead)
	}
	return out, true, nil
}

func (db *DB[K, V]) execPlanOrderLead(q *qx.QX, preds []plannerPred, trace *queryTrace) ([]K, bool) {
	o := q.Order[0]

	fm := db.fields[o.Field]
	if fm == nil || fm.Slice {
		return nil, false
	}
	slice := db.index[o.Field]
	if slice == nil {
		return nil, false
	}
	s := *slice
	if len(s) == 0 {
		return nil, true
	}

	start, end := 0, len(s)
	rangeCovered := make([]bool, len(preds))
	if st, en, cov, ok := db.extractOrderRangeCoveragePreds(o.Field, preds, s); ok {
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

	active := make([]int, 0, len(preds))
	lead := -1
	var leadCard uint64

	for i := range preds {
		p := preds[i]
		if p.covered || p.alwaysTrue {
			continue
		}
		if p.alwaysFalse {
			return nil, true
		}
		active = append(active, i)
		if p.iter != nil && (lead == -1 || p.estCard < leadCard) {
			lead = i
			leadCard = p.estCard
		}
	}
	if lead == -1 {
		return nil, false
	}
	if leadCard > plannerOrderLeadLeadCardMax {
		return nil, false
	}

	candidates := getRoaringBuf()
	defer releaseRoaringBuf(candidates)

	it := preds[lead].iter()
	for it.HasNext() {
		idx := it.Next()
		if trace != nil {
			trace.addExamined(1)
		}

		pass := true
		for _, pi := range active {
			if pi == lead {
				continue
			}
			p := preds[pi]
			if p.contains == nil || !p.contains(idx) {
				pass = false
				break
			}
		}
		if !pass {
			continue
		}

		candidates.Add(idx)
		if candidates.GetCardinality() > plannerOrderLeadCandidateCardMax {
			return nil, false
		}
	}
	if candidates.IsEmpty() {
		return nil, true
	}

	skip := q.Offset
	need := int(q.Limit)
	out := make([]K, 0, need)

	if db.strkey {
		db.strmap.RLock()
		defer db.strmap.RUnlock()
	}

	emit := func(bm *roaring64.Bitmap) bool {
		it := bm.Iterator()
		for it.HasNext() {
			idx := it.Next()
			if trace != nil {
				trace.addExamined(1)
			}
			if !candidates.Contains(idx) {
				continue
			}
			if skip > 0 {
				skip--
				continue
			}
			out = append(out, db.idFromIdxNoLock(idx))
			need--
			if need == 0 {
				return true
			}
		}
		return false
	}

	if !o.Desc {
		for i := start; i < end; i++ {
			bm := s[i].IDs
			if bm == nil || bm.IsEmpty() {
				continue
			}
			if emit(bm) {
				return out, true
			}
		}
		return out, true
	}

	for i := end - 1; i >= start; i-- {
		bm := s[i].IDs
		if bm == nil || bm.IsEmpty() {
			if i == start {
				break
			}
			continue
		}
		if emit(bm) {
			return out, true
		}
		if i == start {
			break
		}
	}

	return out, true
}
