package qexec

import (
	"fmt"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/qir"
)

type plannerNoOrderLimitCandidateKind uint8

const (
	plannerNoOrderLimitCandidateNone plannerNoOrderLimitCandidateKind = iota
	plannerNoOrderLimitCandidateEmpty
	plannerNoOrderLimitCandidateNoFilter
	plannerNoOrderLimitCandidateUniqueEq
	plannerNoOrderLimitCandidateDirectPrefix
	plannerNoOrderLimitCandidateDirectRange
	plannerNoOrderLimitCandidateSameFieldBounds
	plannerNoOrderLimitCandidateUniverseScan
	plannerNoOrderLimitCandidateLeadScan
	plannerNoOrderLimitCandidateMaterializedFallback
)

func (k plannerNoOrderLimitCandidateKind) String() string {
	switch k {
	case plannerNoOrderLimitCandidateEmpty:
		return "empty"
	case plannerNoOrderLimitCandidateNoFilter:
		return "no_filter"
	case plannerNoOrderLimitCandidateUniqueEq:
		return "unique_eq"
	case plannerNoOrderLimitCandidateDirectPrefix:
		return "direct_prefix"
	case plannerNoOrderLimitCandidateDirectRange:
		return "direct_range"
	case plannerNoOrderLimitCandidateSameFieldBounds:
		return "same_field_bounds"
	case plannerNoOrderLimitCandidateUniverseScan:
		return "universe_scan"
	case plannerNoOrderLimitCandidateLeadScan:
		return "lead_scan"
	case plannerNoOrderLimitCandidateMaterializedFallback:
		return "materialized_fallback"
	default:
		return ""
	}
}

type plannerNoOrderLimitCandidate struct {
	kind         plannerNoOrderLimitCandidateKind
	cost         float64
	expectedRows uint64
	leadRows     uint64
	leadIdx      int
	checks       uint64
	buildWork    uint64
}

type plannerNoOrderLimitDecision struct {
	selected             plannerNoOrderLimitCandidate
	runtimeFallback      plannerNoOrderLimitCandidate
	materializedFallback plannerNoOrderLimitCandidate
	rejected             plannerNoOrderLimitCandidate
}

type plannerNoOrderLimitRuntimeGuard struct {
	enabled      bool
	minExamined  uint64
	needWindow   uint64
	fallbackCost float64
	rowCost      float64
	reason       string
}

type noOrderLimitFacts struct {
	leaves       []qir.Expr
	leavesBuf    []qir.Expr
	leavesInline [plannerPredicateFastPathMaxLeaves]qir.Expr
	leafEst      [plannerPredicateFastPathMaxLeaves]uint64

	directField              string
	directBounds             indexdata.Bounds
	directBoundsOK           bool
	directBoundLeaves        int
	directResiduals          int
	directResidualsSupported bool
	singlePrefix             bool
	singleRange              bool
	noFilter                 bool
	hasUnique                bool
}

const plannerLimitFactsRetainExprCap = 64

var noOrderLimitFactsPool = pooled.Pointers[noOrderLimitFacts]{
	Cleanup: func(facts *noOrderLimitFacts) {
		leavesBuf := facts.leavesBuf
		if cap(leavesBuf) > plannerLimitFactsRetainExprCap {
			leavesBuf = nil
		} else if cap(leavesBuf) > 0 {
			clear(leavesBuf[:cap(leavesBuf)])
			leavesBuf = leavesBuf[:0]
		}
		*facts = noOrderLimitFacts{leavesBuf: leavesBuf}
	},
}

func (facts *noOrderLimitFacts) Release() {
	noOrderLimitFactsPool.Put(facts)
}

func (d plannerNoOrderLimitDecision) selectedKind() plannerNoOrderLimitCandidateKind { // nolint:unused
	return d.selected.kind
}

func (d plannerNoOrderLimitDecision) traceRoute() TraceNoOrderLimitRoute {
	return TraceNoOrderLimitRoute{
		Selected:     d.selected.kind.String(),
		Rejected:     d.rejected.kind.String(),
		SelectedCost: d.selected.cost,
		RejectedCost: d.rejected.cost,
		SelectedWork: d.selected.traceWork(),
		RejectedWork: d.rejected.traceWork(),
		ExpectedRows: d.selected.expectedRows,
		LeadRows:     d.selected.leadRows,
		Checks:       d.selected.checks,
		PostingBuild: d.selected.buildWork,
	}
}

func (c plannerNoOrderLimitCandidate) traceWork() TraceRouteWork {
	if c.cost <= 0 {
		return TraceRouteWork{}
	}
	switch c.kind {
	case plannerNoOrderLimitCandidateEmpty, plannerNoOrderLimitCandidateNoFilter:
		return TraceRouteWork{CandidateScan: c.cost}
	case plannerNoOrderLimitCandidateUniqueEq:
		return TraceRouteWork{PostingContains: c.cost}
	case plannerNoOrderLimitCandidateDirectPrefix, plannerNoOrderLimitCandidateDirectRange, plannerNoOrderLimitCandidateSameFieldBounds:
		if c.checks == 0 {
			return TraceRouteWork{RangeProbe: c.cost}
		}
		scan := float64(c.expectedRows)
		if scan > c.cost {
			scan = c.cost
		}
		return TraceRouteWork{CandidateScan: scan, PostingContains: c.cost - scan}
	case plannerNoOrderLimitCandidateLeadScan, plannerNoOrderLimitCandidateUniverseScan:
		scan := float64(c.expectedRows)
		if scan > c.cost {
			scan = c.cost
		}
		return TraceRouteWork{CandidateScan: scan, PostingContains: c.cost - scan}
	case plannerNoOrderLimitCandidateMaterializedFallback:
		build := float64(c.buildWork)
		if build <= 0 || build > c.cost {
			build = c.cost
		}
		return TraceRouteWork{CandidateScan: c.cost - build, MaterializedBuild: build}
	default:
		return TraceRouteWork{}
	}
}

func (qv *View) traceNoOrderLimitRoute(q *qir.Shape, facts *noOrderLimitFacts, d plannerNoOrderLimitDecision) TraceNoOrderLimitRoute {
	route := d.traceRoute()
	route.SelectedWork = qv.noOrderLimitCandidateTraceWork(q, facts, d.selected)
	route.RejectedWork = qv.noOrderLimitCandidateTraceWork(q, facts, d.rejected)
	return route
}

func (qv *View) noOrderLimitCandidateTraceWork(q *qir.Shape, facts *noOrderLimitFacts, c plannerNoOrderLimitCandidate) TraceRouteWork {
	if c.cost <= 0 {
		return TraceRouteWork{}
	}
	if c.kind != plannerNoOrderLimitCandidateDirectRange &&
		c.kind != plannerNoOrderLimitCandidateSameFieldBounds {
		return c.traceWork()
	}
	if c.checks == 0 || q == nil || facts == nil || !facts.directBoundsOK {
		return c.traceWork()
	}
	needWindow := q.Offset + q.Limit
	if needWindow < q.Offset {
		return c.traceWork()
	}
	ov := qv.fieldIndexViewFromSlotsByName(qv.snap.Index, facts.directField)
	if !ov.HasData() {
		return c.traceWork()
	}
	br := ov.RangeForBounds(facts.directBounds)
	if br.Empty() || br.BaseEnd < br.BaseStart {
		return c.traceWork()
	}
	width := float64(br.BaseEnd - br.BaseStart)
	residuals := float64(c.checks)
	expected := float64(c.expectedRows)

	best := TraceRouteWork{
		CandidateScan:   expected * 0.75,
		PostingContains: expected*residuals*0.35 + residuals*16.0,
	}
	bestCost := best.CandidateScan + best.PostingContains
	bestDelta := bestCost - c.cost
	if bestDelta < 0 {
		bestDelta = -bestDelta
	}

	if qv.hasPrefixBoundForField(facts.leaves, facts.directField) && qv.shouldPreferExecutionNoOrderPrefix(q, facts.leaves) {
		work := TraceRouteWork{
			CandidateScan:   float64(needWindow) * 0.70,
			PostingContains: float64(needWindow) * residuals * 0.15,
			RangeProbe:      width * 0.0001,
		}
		cost := work.CandidateScan + work.PostingContains + work.RangeProbe
		delta := cost - c.cost
		if delta < 0 {
			delta = -delta
		}
		if delta < bestDelta {
			best = work
			bestDelta = delta
		}
	}

	work := TraceRouteWork{
		CandidateScan:   expected,
		PostingContains: expected * residuals * 0.70,
		RangeProbe:      width * 0.20,
	}
	cost := work.CandidateScan + work.PostingContains + work.RangeProbe
	delta := cost - c.cost
	if delta < 0 {
		delta = -delta
	}
	if delta < bestDelta {
		best = work
		bestDelta = delta
	}

	rows := float64(c.leadRows)
	if rows > 0 {
		work = TraceRouteWork{
			CandidateScan:   rows,
			PostingContains: rows * residuals * 0.45,
			RangeProbe:      width * 0.20,
		}
		cost = work.CandidateScan + work.PostingContains + work.RangeProbe
		delta = cost - c.cost
		if delta < 0 {
			delta = -delta
		}
		if delta < bestDelta {
			best = work
		}
	}
	return best
}

func plannerNoOrderLimitPick(candidates []plannerNoOrderLimitCandidate) plannerNoOrderLimitDecision {
	var d plannerNoOrderLimitDecision
	for i := range candidates {
		if candidates[i].kind == plannerNoOrderLimitCandidateNone {
			continue
		}
		if candidates[i].kind == plannerNoOrderLimitCandidateMaterializedFallback {
			d.materializedFallback = candidates[i]
		}
		if d.selected.kind == plannerNoOrderLimitCandidateNone || candidates[i].cost < d.selected.cost {
			if d.selected.kind != plannerNoOrderLimitCandidateNone {
				if d.rejected.kind == plannerNoOrderLimitCandidateNone || d.selected.cost < d.rejected.cost {
					d.rejected = d.selected
				}
			}
			d.selected = candidates[i]
			continue
		}
		if d.rejected.kind == plannerNoOrderLimitCandidateNone || candidates[i].cost < d.rejected.cost {
			d.rejected = candidates[i]
		}
	}
	if d.selected.kind == plannerNoOrderLimitCandidateDirectRange ||
		d.selected.kind == plannerNoOrderLimitCandidateSameFieldBounds {
		for i := range candidates {
			switch candidates[i].kind {
			case plannerNoOrderLimitCandidateLeadScan, plannerNoOrderLimitCandidateMaterializedFallback:
			default:
				continue
			}
			if candidates[i].cost <= 0 {
				continue
			}
			if d.runtimeFallback.kind == plannerNoOrderLimitCandidateNone ||
				candidates[i].cost < d.runtimeFallback.cost {
				d.runtimeFallback = candidates[i]
			}
		}
	}
	return d
}

func (d plannerNoOrderLimitDecision) runtimeGuard(q *qir.Shape) plannerNoOrderLimitRuntimeGuard {
	if d.selected.kind != plannerNoOrderLimitCandidateDirectRange &&
		d.selected.kind != plannerNoOrderLimitCandidateSameFieldBounds {
		return plannerNoOrderLimitRuntimeGuard{}
	}
	if d.selected.checks == 0 || q.Offset > 0 || q.Limit == 0 {
		return plannerNoOrderLimitRuntimeGuard{}
	}
	if d.runtimeFallback.kind != plannerNoOrderLimitCandidateLeadScan &&
		d.runtimeFallback.kind != plannerNoOrderLimitCandidateMaterializedFallback {
		return plannerNoOrderLimitRuntimeGuard{}
	}
	if d.runtimeFallback.cost <= 0 {
		return plannerNoOrderLimitRuntimeGuard{}
	}
	selectiveFallback := false
	if d.selected.expectedRows > 1 && d.runtimeFallback.expectedRows > 0 {
		threshold := d.selected.expectedRows - d.selected.expectedRows/4
		if threshold == d.selected.expectedRows {
			threshold--
		}
		selectiveFallback = d.runtimeFallback.expectedRows <= threshold
	}
	competitiveFallback := d.selected.cost > 0 && d.runtimeFallback.cost <= d.selected.cost*1.25
	if !selectiveFallback && !competitiveFallback {
		return plannerNoOrderLimitRuntimeGuard{}
	}

	minExamined := d.runtimeFallback.expectedRows
	needMin := satMulUint64(q.Limit, 4)
	if minExamined < needMin {
		minExamined = needMin
	}
	needMax := satMulUint64(q.Limit, 64)
	if minExamined > needMax {
		minExamined = needMax
	}
	if minExamined < 128 {
		minExamined = 128
	}

	return plannerNoOrderLimitRuntimeGuard{
		enabled:      true,
		minExamined:  minExamined,
		needWindow:   q.Limit,
		fallbackCost: d.runtimeFallback.cost,
		rowCost:      0.75 + float64(d.selected.checks)*0.35,
		reason:       "direct_range_guard",
	}
}

func (g plannerNoOrderLimitRuntimeGuard) shouldFallback(examined uint64, emitted int) bool {
	if !g.enabled || examined < g.minExamined {
		return false
	}
	if emitted > 0 && uint64(emitted)*4 >= g.needWindow {
		return false
	}
	return float64(examined)*g.rowCost > g.fallbackCost*1.15
}

func (qv *View) collectNoOrderLimitFacts(q *qir.Shape, facts *noOrderLimitFacts) (bool, error) {
	if q.Expr.Not && (q.Expr.FieldOrdinal < 0 || len(q.Expr.Operands) != 0) {
		return false, nil
	}
	if qir.IsTrueConst(q.Expr) {
		if q.Limit == 0 || q.Offset != 0 {
			return false, nil
		}
		facts.noFilter = true
		return true, nil
	}

	n, ok := qir.AndLeafLen(q.Expr, qir.LeafModeCollect)
	if !ok || n == 0 {
		return false, nil
	}

	if n <= len(facts.leavesInline) {
		facts.leaves, ok = qir.CollectAndLeavesScratch(q.Expr, facts.leavesInline[:0], qir.LeafModeCollect)
	} else {
		if cap(facts.leavesBuf) < n {
			facts.leavesBuf = make([]qir.Expr, 0, n)
		}
		facts.leaves, ok = qir.CollectAndLeavesScratch(q.Expr, facts.leavesBuf[:0], qir.LeafModeCollect)
		facts.leavesBuf = facts.leaves
	}
	if !ok || len(facts.leaves) == 0 {
		return false, nil
	}

	leaves := facts.leaves
	if q.Offset == 0 {
		uniqueOK := true
		for i := 0; i < len(leaves); i++ {
			if leaves[i].Not {
				uniqueOK = false
				break
			}
			if qv.isPositiveUniqueEqExpr(leaves[i]) {
				facts.hasUnique = true
			}
		}
		facts.hasUnique = facts.hasUnique && uniqueOK
	}

	if q.Limit == 0 {
		return facts.hasUnique, nil
	}

	if len(leaves) == 1 {
		e := leaves[0]
		if isPositiveScalarPrefixLeaf(e) {
			if fm := qv.fieldMetaByExpr(e); fm != nil && !fm.Slice {
				facts.singlePrefix = true
			}
		}
		if !e.Not && e.FieldOrdinal >= 0 && len(e.Operands) == 0 && isScalarRangeEqOp(e.Op) {
			if fm := qv.fieldMetaByExpr(e); fm != nil && !fm.Slice {
				facts.singleRange = true
			}
		}
		if facts.singlePrefix || facts.singleRange {
			return true, nil
		}
	}

	boundLeaves := 0
	fieldOrdinal := qir.NoFieldOrdinal
	sameField := true
	for i := 0; i < len(leaves); i++ {
		e := leaves[i]
		if !isBoundOp(e.Op) {
			continue
		}
		boundLeaves++
		if e.Not || e.FieldOrdinal < 0 {
			sameField = false
			continue
		}
		if fieldOrdinal == qir.NoFieldOrdinal {
			fieldOrdinal = e.FieldOrdinal
		} else if fieldOrdinal != e.FieldOrdinal {
			sameField = false
		}
	}

	if boundLeaves > 0 && sameField && fieldOrdinal >= 0 {
		field := qv.exec.FieldNameByOrdinal(fieldOrdinal)
		bounds, ok, err := qv.extractBoundsForField(field, leaves)
		if err != nil {
			return true, err
		}
		if ok {
			facts.directField = field
			facts.directBounds = bounds
			facts.directBoundsOK = true
			facts.directBoundLeaves = boundLeaves
			facts.directResiduals = len(leaves) - boundLeaves
		}
	}

	if !facts.directBoundsOK {
		for i := 0; i < len(leaves); i++ {
			e := leaves[i]
			if e.Not || e.FieldOrdinal < 0 {
				continue
			}
			switch e.Op {
			case qir.OpGT, qir.OpGTE, qir.OpLT, qir.OpLTE:
			default:
				continue
			}
			field := qv.exec.FieldNameByOrdinal(e.FieldOrdinal)
			bounds, ok, err := qv.extractBoundsForField(field, leaves)
			if err != nil {
				return true, err
			}
			if ok {
				facts.directField = field
				facts.directBounds = bounds
				facts.directBoundsOK = true
				facts.directBoundLeaves = 0
				for j := 0; j < len(leaves); j++ {
					other := leaves[j]
					if isBoundOp(other.Op) && !other.Not && other.FieldOrdinal >= 0 &&
						qv.exec.FieldNameByOrdinal(other.FieldOrdinal) == field {
						facts.directBoundLeaves++
					}
				}
				facts.directResiduals = len(leaves) - facts.directBoundLeaves
			}
			break
		}
	}

	if facts.directBoundsOK {
		facts.directResidualsSupported = true
		for i := 0; i < len(leaves); i++ {
			e := leaves[i]
			if isBoundOp(e.Op) && !e.Not && e.FieldOrdinal >= 0 &&
				qv.exec.FieldNameByOrdinal(e.FieldOrdinal) == facts.directField {
				continue
			}
			if !qv.supportsLimitLeafPredExpr(e) {
				facts.directResidualsSupported = false
				break
			}
		}
	}

	return true, nil
}

func (qv *View) selectNoOrderLimit(q *qir.Shape, facts *noOrderLimitFacts) plannerNoOrderLimitDecision {
	leaves := facts.leaves
	needWindow := q.Offset + q.Limit
	if needWindow < q.Offset {
		return plannerNoOrderLimitDecision{}
	}

	if facts.noFilter {
		expected := q.Limit
		return plannerNoOrderLimitDecision{
			selected: plannerNoOrderLimitCandidate{
				kind:         plannerNoOrderLimitCandidateNoFilter,
				cost:         float64(expected),
				expectedRows: expected,
			},
		}
	}

	if facts.hasUnique {
		snap := qv.exec.Stats.Load()
		universe := qv.plannerUniverseCardinality(snap)
		expected := q.Limit
		if expected == 0 || expected > 1 {
			expected = 1
		}
		fallback := plannerNoOrderLimitCandidate{
			kind:         plannerNoOrderLimitCandidateMaterializedFallback,
			cost:         float64(universe) + float64(len(leaves))*18.0,
			expectedRows: expected,
		}
		return plannerNoOrderLimitDecision{
			selected: plannerNoOrderLimitCandidate{
				kind:         plannerNoOrderLimitCandidateUniqueEq,
				cost:         1,
				expectedRows: expected,
			},
			materializedFallback: fallback,
			rejected:             fallback,
		}
	}
	if q.Limit == 0 {
		return plannerNoOrderLimitDecision{}
	}

	if facts.singlePrefix {
		expected := needWindow
		fallback := plannerNoOrderLimitCandidate{
			kind:         plannerNoOrderLimitCandidateMaterializedFallback,
			cost:         float64(expected) + 18.0,
			expectedRows: expected,
		}
		selected := plannerNoOrderLimitCandidate{
			kind:         plannerNoOrderLimitCandidateDirectPrefix,
			cost:         float64(expected) * 0.72,
			expectedRows: expected,
			leadRows:     expected,
		}
		return plannerNoOrderLimitDecision{
			selected:             selected,
			materializedFallback: fallback,
			rejected:             fallback,
		}
	}

	if facts.singleRange {
		expected := needWindow
		fallback := plannerNoOrderLimitCandidate{
			kind:         plannerNoOrderLimitCandidateMaterializedFallback,
			cost:         float64(expected) + 18.0,
			expectedRows: expected,
		}
		selected := plannerNoOrderLimitCandidate{
			kind:         plannerNoOrderLimitCandidateDirectRange,
			cost:         float64(expected) * 0.72,
			expectedRows: expected,
		}
		return plannerNoOrderLimitDecision{
			selected:             selected,
			materializedFallback: fallback,
			rejected:             fallback,
		}
	}

	snap := qv.exec.Stats.Load()
	universe := qv.plannerUniverseCardinality(snap)
	if universe == 0 {
		return plannerNoOrderLimitDecision{
			selected:             plannerNoOrderLimitCandidate{kind: plannerNoOrderLimitCandidateMaterializedFallback, cost: 1},
			materializedFallback: plannerNoOrderLimitCandidate{kind: plannerNoOrderLimitCandidateMaterializedFallback, cost: 1},
		}
	}

	combinedSel := 1.0
	materializedWork := 0.0
	leadFound := false
	leadRows := uint64(0)
	leadScore := 0.0
	leadSel := 0.0
	leadIdx := -1
	checks := 0
	allSupported := true
	emptySupported := false

	var directBR indexdata.FieldIndexRange
	directRows := uint64(0)
	directBucketCount := 0
	directBucketTotal := 0
	directHasNilTail := false
	directRangeOK := false
	directRangeEmpty := false
	if facts.directBoundsOK {
		fm := qv.exec.Schema.Fields[facts.directField]
		ov := qv.fieldIndexViewFromSlotsByName(qv.snap.Index, facts.directField)
		if fm != nil && !fm.Slice && ov.HasData() {
			directBR = ov.RangeForBounds(facts.directBounds)
			directBucketTotal = ov.KeyCount()
			directHasNilTail = fm.Ptr &&
				qv.fieldIndexViewFromSlotsByName(qv.snap.NilIndex, facts.directField).LookupCardinality(indexdata.NilIndexEntryKey) > 0
			directRangeOK = true
			if directBR.Empty() {
				directRangeEmpty = true
			} else {
				buckets, rows := ov.RangeStats(directBR)
				if rows == 0 {
					rows = 1
				}
				directBucketCount = buckets
				directRows = rows
				sel := float64(rows) / float64(universe)
				if sel <= 0 {
					sel = 1.0 / float64(universe)
				}
				if sel > 1 {
					sel = 1
				}
				combinedSel *= sel
				if combinedSel < 1.0/float64(universe) {
					combinedSel = 1.0 / float64(universe)
				}
				materializedWork += float64(rows) * 1.05
			}
		}
	}

	for i, e := range leaves {
		if directRangeOK && isBoundOp(e.Op) && !e.Not && e.FieldOrdinal >= 0 &&
			qv.exec.FieldNameByOrdinal(e.FieldOrdinal) == facts.directField {
			continue
		}
		leafSupported := qv.supportsLimitLeafPredExpr(e)
		if !leafSupported {
			allSupported = false
		}
		sel, work, _, _, ok := qv.estimateLeafOrderCost(e, snap, universe, "", false)
		if !ok {
			allSupported = false
			continue
		}
		if !e.Not && sel == 0 {
			emptySupported = true
			continue
		}
		if sel > 1 {
			sel = 1
		}
		if sel <= 0 {
			sel = 1.0 / float64(universe)
		}
		est := uint64(sel * float64(universe))
		if est == 0 {
			est = 1
		}
		if i < len(facts.leafEst) {
			facts.leafEst[i] = est
		}
		combinedSel *= sel
		if combinedSel < 1.0/float64(universe) {
			combinedSel = 1.0 / float64(universe)
		}
		materializedWork += work

		if !e.Not && leafSupported {
			weight := 1.0
			switch e.Op {
			case qir.OpGT, qir.OpGTE, qir.OpLT, qir.OpLTE, qir.OpPREFIX:
				weight = 1.35
			case qir.OpHASANY:
				weight = 1.20
			case qir.OpHASALL:
				weight = 0.90
			}
			score := float64(est) * weight
			if !leadFound || score < leadScore {
				leadFound = true
				leadRows = est
				leadScore = score
				leadSel = sel
				leadIdx = i
			}
		}
	}

	if emptySupported && allSupported {
		return plannerNoOrderLimitDecision{
			selected: plannerNoOrderLimitCandidate{
				kind: plannerNoOrderLimitCandidateEmpty,
				cost: 1,
			},
		}
	}

	var candidates [6]plannerNoOrderLimitCandidate
	n := 0

	materializedRows := uint64(combinedSel * float64(universe))
	if materializedRows == 0 {
		materializedRows = 1
	}
	materializedCost := materializedWork*1.15 + float64(materializedRows)*0.35 + float64(len(leaves))*18.0
	if !facts.directBoundsOK && !leadFound && allSupported && qv.snap.AllowsMaterializedPredCard(materializedRows) {
		materializedCost = float64(needWindow)*0.60 + float64(len(leaves))*18.0
	}
	candidates[n] = plannerNoOrderLimitCandidate{
		kind:         plannerNoOrderLimitCandidateMaterializedFallback,
		cost:         materializedCost,
		expectedRows: materializedRows,
		buildWork:    uint64(materializedWork),
	}
	n++

	leadExpectedRows := uint64(0)
	if leadFound && allSupported {
		restSel := combinedSel / leadSel
		if restSel <= 0 {
			restSel = 1.0 / float64(universe)
		}
		if restSel > 1 {
			restSel = 1
		}
		leadExpectedRows = uint64(float64(needWindow) / restSel)
		if leadExpectedRows < needWindow {
			leadExpectedRows = needWindow
		}
		if leadExpectedRows > leadRows {
			leadExpectedRows = leadRows
		}
	}

	if facts.directBoundsOK && directRangeOK {
		kind := plannerNoOrderLimitCandidateDirectRange
		if facts.directBoundLeaves > 1 {
			kind = plannerNoOrderLimitCandidateSameFieldBounds
		}
		residuals := facts.directResiduals
		if directRangeEmpty {
			candidates[n] = plannerNoOrderLimitCandidate{
				kind:   kind,
				checks: uint64(residuals),
			}
			n++
		} else if facts.directResidualsSupported {
			rows := directRows
			restSel := combinedSel / (float64(rows) / float64(universe))
			if restSel <= 0 {
				restSel = 1.0 / float64(universe)
			}
			if restSel > 1 {
				restSel = 1
			}
			expected := uint64(float64(needWindow) / restSel)
			if expected < needWindow {
				expected = needWindow
			}
			if expected > rows {
				expected = rows
			}
			cost := float64(expected) * 0.72
			if residuals > 0 {
				cost = float64(expected)*(0.75+float64(residuals)*0.35) + float64(residuals)*16.0
				broadMin := satMulUint64(needWindow, uint64(limitRangeNoOrderBroadResidualRowsPerNeed))
				closeToLead := leadExpectedRows > 0 && expected >= leadExpectedRows
				if leadExpectedRows > 0 && !closeToLead {
					closeToLead = expected >= leadExpectedRows-leadExpectedRows/4
				}
				selectiveLead := leadRows < rows && leadRows <= rows-rows/3
				negativeResiduals := 0
				hasResidualPrefix := false
				for i := 0; i < len(leaves); i++ {
					e := leaves[i]
					if isBoundOp(e.Op) && !e.Not && e.FieldOrdinal >= 0 &&
						qv.exec.FieldNameByOrdinal(e.FieldOrdinal) == facts.directField {
						continue
					}
					if e.Not {
						negativeResiduals++
					} else if e.Op == qir.OpPREFIX {
						hasResidualPrefix = true
					}
				}
				if residuals >= 3 && leadFound && rows > broadMin && !hasResidualPrefix {
					if selectiveLead && closeToLead {
						cost = float64(expected)*(1.0+float64(residuals)*0.70) + float64(directBR.BaseEnd-directBR.BaseStart)*0.20
					} else if negativeResiduals >= 2 && leadExpectedRows > 0 && leadExpectedRows <= rows {
						cost = float64(rows)*(1.0+float64(residuals)*0.45) + float64(directBR.BaseEnd-directBR.BaseStart)*0.20
					}
				}
			}
			if qv.hasPrefixBoundForField(leaves, facts.directField) && qv.shouldPreferExecutionNoOrderPrefix(q, leaves) {
				cost = float64(needWindow)*(0.70+float64(residuals)*0.15) + float64(directBR.BaseEnd-directBR.BaseStart)*0.0001
			}
			candidates[n] = plannerNoOrderLimitCandidate{
				kind:         kind,
				cost:         cost,
				expectedRows: expected,
				leadRows:     rows,
				checks:       uint64(residuals),
			}
			n++
		}
	}

	if leadFound && allSupported {
		expected := leadExpectedRows
		if len(leaves) > 1 {
			checks = len(leaves) - 1
		}
		buildWork := uint64(0)
		if directRangeOK && !directRangeEmpty && facts.directBoundLeaves > 1 && directBucketCount > 0 && directRows > 0 {
			probeBuckets := directBucketCount
			probeEst := directRows
			directSel := float64(directRows) / float64(universe)
			useProbeComplement := false
			if !directHasNilTail && directBucketTotal > directBucketCount && directBucketTotal-directBucketCount < directBucketCount {
				probeBuckets = directBucketTotal - directBucketCount
				useProbeComplement = true
				if universe > directRows {
					probeEst = universe - directRows
				} else {
					probeEst = 0
				}
			}
			if useProbeComplement && probeBuckets > 0 && probeEst > 0 && probeBuckets > rangeLinearContainsLimit(probeBuckets, probeEst) {
				otherSel := combinedSel / (leadSel * directSel)
				if otherSel <= 0 {
					otherSel = 1.0 / float64(universe)
				}
				if otherSel > 1 {
					otherSel = 1
				}
				rangeCalls := uint64(float64(expected) * otherSel)
				if rangeCalls == 0 && expected > 0 {
					rangeCalls = 1
				}
				materializeAt := rangeAdaptiveProbeMaterializeAt(probeBuckets, probeEst)
				if materializeAt > 0 && rangeCalls >= materializeAt {
					buildWork = rangeProbeMaterializeWork(probeBuckets, probeEst)
				}
			}
		}
		candidates[n] = plannerNoOrderLimitCandidate{
			kind:         plannerNoOrderLimitCandidateLeadScan,
			cost:         float64(expected)*(0.92+float64(checks)*1.05) + float64(buildWork) + float64(len(leaves))*10.0,
			expectedRows: expected,
			leadRows:     leadRows,
			leadIdx:      leadIdx,
			checks:       uint64(checks),
			buildWork:    buildWork,
		}
		n++
	}

	if !leadFound && allSupported {
		expected := uint64(float64(needWindow) / combinedSel)
		if expected < needWindow {
			expected = needWindow
		}
		if expected > universe {
			expected = universe
		}
		checks = len(leaves)
		candidates[n] = plannerNoOrderLimitCandidate{
			kind:         plannerNoOrderLimitCandidateUniverseScan,
			cost:         float64(expected)*(0.80+float64(checks)*0.70) + float64(checks)*10.0,
			expectedRows: expected,
			leadRows:     universe,
			checks:       uint64(checks),
		}
		n++
	}

	return plannerNoOrderLimitPick(candidates[:n])
}

func (qv *View) executeNoOrderLimit(q *qir.Shape, trace *Trace) ([]uint64, bool, PlanName, error) {
	facts := noOrderLimitFactsPool.Get()
	defer facts.Release()

	ok, err := qv.collectNoOrderLimitFacts(q, facts)
	if err != nil {
		return nil, ok, "", err
	}
	if !ok {
		return nil, false, "", nil
	}

	decision := qv.selectNoOrderLimit(q, facts)
	guard := decision.runtimeGuard(q)
	if trace != nil {
		trace.SetNoOrderLimitRoute(qv.traceNoOrderLimitRoute(q, facts, decision))
		fallbackCost := decision.materializedFallback.cost
		if fallbackCost <= 0 {
			fallbackCost = decision.rejected.cost
		}
		trace.SetEstimated(decision.selected.expectedRows, decision.selected.cost, fallbackCost)
		trace.SetNoOrderLimitRuntimeGuard(guard.enabled, guard.reason)
	}

	return qv.dispatchNoOrderLimit(q, facts, decision, guard, trace)
}

func (qv *View) dispatchNoOrderLimitFallback(
	q *qir.Shape,
	decision plannerNoOrderLimitDecision,
) ([]uint64, bool, PlanName, error) {
	if decision.materializedFallback.kind == plannerNoOrderLimitCandidateMaterializedFallback {
		return qv.dispatchLimitMaterialized(q)
	}
	return nil, true, "", fmt.Errorf("selected no-order LIMIT route %s was not executable", decision.selected.kind.String())
}

func (qv *View) dispatchNoOrderLimit(
	q *qir.Shape,
	facts *noOrderLimitFacts,
	decision plannerNoOrderLimitDecision,
	guard plannerNoOrderLimitRuntimeGuard,
	trace *Trace,
) ([]uint64, bool, PlanName, error) {
	leaves := facts.leaves

	selected := decision.selected

DISPATCH:
	switch selected.kind {
	case plannerNoOrderLimitCandidateEmpty:
		return nil, true, PlanCandidateNoOrder, nil

	case plannerNoOrderLimitCandidateNoFilter:
		out, used, err := qv.execSelectedNoOrderNoFilter(q, trace)
		if !used {
			if err != nil {
				return nil, true, "", err
			}
			return qv.dispatchNoOrderLimitFallback(q, decision)
		}
		return out, true, PlanLimit, err

	case plannerNoOrderLimitCandidateUniqueEq:
		out, used, err := qv.execSelectedNoOrderUniqueEq(q, leaves, trace)
		if !used {
			if err != nil {
				return nil, true, "", err
			}
			return qv.dispatchNoOrderLimitFallback(q, decision)
		}
		return out, true, PlanUniqueEq, err

	case plannerNoOrderLimitCandidateDirectPrefix:
		out, used, err := qv.execSelectedNoOrderDirectPrefix(q, trace)
		if !used {
			if err != nil {
				return nil, true, "", err
			}
			return qv.dispatchNoOrderLimitFallback(q, decision)
		}
		return out, true, PlanLimitPrefixNoOrder, err

	case plannerNoOrderLimitCandidateDirectRange:
		if facts.singleRange && len(leaves) == 1 {
			out, used, err := qv.execSelectedNoOrderDirectRange(q, trace)
			if !used {
				if err != nil {
					return nil, true, "", err
				}
				return qv.dispatchNoOrderLimitFallback(q, decision)
			}
			return out, true, PlanLimitRangeNoOrder, err
		}
		if !facts.directBoundsOK {
			return qv.dispatchNoOrderLimitFallback(q, decision)
		}
		out, used, runtimeFallback, err := qv.execNoOrderBounds(q, facts.directField, facts.directBounds, leaves, guard, trace)
		if !used {
			if err != nil {
				return nil, true, "", err
			}
			if runtimeFallback && decision.runtimeFallback.kind != plannerNoOrderLimitCandidateNone {
				selected = decision.runtimeFallback
				guard = plannerNoOrderLimitRuntimeGuard{}
				goto DISPATCH
			}
			return qv.dispatchNoOrderLimitFallback(q, decision)
		}
		return out, true, PlanLimitRangeNoOrder, err

	case plannerNoOrderLimitCandidateSameFieldBounds:
		if !facts.directBoundsOK {
			return qv.dispatchNoOrderLimitFallback(q, decision)
		}
		out, used, runtimeFallback, err := qv.execNoOrderBounds(q, facts.directField, facts.directBounds, leaves, guard, trace)
		if !used {
			if err != nil {
				return nil, true, "", err
			}
			if runtimeFallback && decision.runtimeFallback.kind != plannerNoOrderLimitCandidateNone {
				selected = decision.runtimeFallback
				guard = plannerNoOrderLimitRuntimeGuard{}
				goto DISPATCH
			}
			return qv.dispatchNoOrderLimitFallback(q, decision)
		}
		return out, true, PlanLimitRangeNoOrder, err

	case plannerNoOrderLimitCandidateLeadScan:
		out, used, err := qv.execSelectedNoOrderLeadScan(q, facts, selected.leadIdx, trace)
		if !used {
			if err != nil {
				return nil, true, "", err
			}
			return qv.dispatchNoOrderLimitFallback(q, decision)
		}
		return out, true, PlanCandidateNoOrder, err

	case plannerNoOrderLimitCandidateUniverseScan:
		out, used, err := qv.execSelectedNoOrderUniverseScan(q, leaves, selected.expectedRows, trace)
		if !used {
			if err != nil {
				return nil, true, "", err
			}
			return qv.dispatchNoOrderLimitFallback(q, decision)
		}
		return out, true, PlanCandidateNoOrder, err

	case plannerNoOrderLimitCandidateMaterializedFallback:
		return qv.dispatchLimitMaterialized(q)
	}

	return nil, false, "", nil
}

func (qv *View) execSelectedNoOrderLeadScan(q *qir.Shape, facts *noOrderLimitFacts, leadIdx int, trace *Trace) ([]uint64, bool, error) {
	leaves := facts.leaves
	var predSet predicateSet

	if facts.directBoundsOK && facts.directBoundLeaves > 1 {
		predSet = newPredicateSet(len(leaves) - facts.directBoundLeaves + 1)
		leadPredIdx := -1
		rangePredBuilt := false

		for i, e := range leaves {
			if isBoundOp(e.Op) && !e.Not && e.FieldOrdinal >= 0 &&
				qv.exec.FieldNameByOrdinal(e.FieldOrdinal) == facts.directField {
				if rangePredBuilt {
					continue
				}
				p, ok := qv.buildMergedScalarRangePredicate(e, facts.directBounds, true)
				if !ok {
					predSet.Release()
					return nil, false, nil
				}
				predSet.Append(p)
				rangePredBuilt = true
				continue
			}

			var (
				p  predicate
				ok bool
			)
			if e.Op == qir.OpEQ && i < len(facts.leafEst) && facts.leafEst[i] > 0 {
				p, ok = qv.buildPredEqCandidateWithEst(e, facts.leafEst[i])
			} else {
				p, ok = qv.buildPredicateCandidate(e)
			}
			if !ok {
				predSet.Release()
				return nil, false, nil
			}
			predSet.Append(p)
			if i == leadIdx {
				leadPredIdx = predSet.Len() - 1
			}
		}

		leadIdx = leadPredIdx
	} else {
		var ok bool
		predSet, ok = qv.buildPredicatesCandidate(leaves)
		if !ok {
			return nil, false, nil
		}
		if predSet.Len() != len(leaves) {
			leadIdx = -1
		}
	}
	defer predSet.Release()
	for i := 0; i < predSet.Len(); i++ {
		if predSet.owner[i].alwaysFalse {
			return nil, true, nil
		}
	}
	return qv.execPlanLeadScanNoOrder(q, predSet.owner, leadIdx, trace), true, nil
}
