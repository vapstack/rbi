package qexec

import (
	"fmt"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/qir"
)

type plannerNoOrderLimitCandidateKind uint8

const (
	plannerNoOrderLimitCandidateNone plannerNoOrderLimitCandidateKind = iota
	plannerNoOrderLimitCandidateNoFilter
	plannerNoOrderLimitCandidateUniqueEq
	plannerNoOrderLimitCandidateDirectPrefix
	plannerNoOrderLimitCandidateDirectRange
	plannerNoOrderLimitCandidateSameFieldBounds
	plannerNoOrderLimitCandidateLeadScan
	plannerNoOrderLimitCandidateMaterializedFallback
)

func (k plannerNoOrderLimitCandidateKind) String() string {
	switch k {
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
	materializedFallback plannerNoOrderLimitCandidate
	rejected             plannerNoOrderLimitCandidate
}

type noOrderLimitFacts struct {
	leaves       []qir.Expr
	leavesBuf    []qir.Expr
	leavesInline [plannerPredicateFastPathMaxLeaves]qir.Expr

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

var noOrderLimitFactsPool = pooled.Pointers[noOrderLimitFacts]{
	Cleanup: func(facts *noOrderLimitFacts) {
		leavesBuf := facts.leavesBuf
		if cap(leavesBuf) > 0 {
			clear(leavesBuf[:cap(leavesBuf)])
			leavesBuf = leavesBuf[:0]
		}
		*facts = noOrderLimitFacts{leavesBuf: leavesBuf}
	},
}

func (facts *noOrderLimitFacts) Release() {
	noOrderLimitFactsPool.Put(facts)
}

func (d plannerNoOrderLimitDecision) selectedKind() plannerNoOrderLimitCandidateKind {
	return d.selected.kind
}

func (d plannerNoOrderLimitDecision) traceRoute() TraceNoOrderLimitRoute {
	return TraceNoOrderLimitRoute{
		Selected:     d.selected.kind.String(),
		Rejected:     d.rejected.kind.String(),
		SelectedCost: d.selected.cost,
		RejectedCost: d.rejected.cost,
		ExpectedRows: d.selected.expectedRows,
		LeadRows:     d.selected.leadRows,
		Checks:       d.selected.checks,
		PostingBuild: d.selected.buildWork,
	}
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
	return d
}

func (qv *View) collectNoOrderLimitFacts(q *qir.Shape, facts *noOrderLimitFacts) (bool, error) {
	if q.Expr.Not {
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

	snap := qv.exec.Stats.Load()
	universe := snap.UniverseOr(qv.snap.Universe.Cardinality())
	if facts.noFilter {
		expected := q.Limit
		if expected > universe {
			expected = universe
		}
		return plannerNoOrderLimitDecision{
			selected: plannerNoOrderLimitCandidate{
				kind:         plannerNoOrderLimitCandidateNoFilter,
				cost:         float64(expected),
				expectedRows: expected,
			},
		}
	}
	if facts.hasUnique {
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

	for i, e := range leaves {
		sel, work, _, _, ok := qv.estimateLeafOrderCost(e, snap, universe, "", false)
		if !ok {
			allSupported = false
			continue
		}
		if sel > 1 {
			sel = 1
		}
		if sel <= 0 {
			sel = 1.0 / float64(universe)
		}
		combinedSel *= sel
		if combinedSel < 1.0/float64(universe) {
			combinedSel = 1.0 / float64(universe)
		}
		materializedWork += work

		if !e.Not && qv.supportsLimitLeafPredExpr(e) {
			est := uint64(sel * float64(universe))
			if est == 0 {
				est = 1
			}
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

	var candidates [5]plannerNoOrderLimitCandidate
	n := 0

	materializedRows := uint64(combinedSel * float64(universe))
	if materializedRows == 0 {
		materializedRows = 1
	}
	materializedCost := materializedWork*1.15 + float64(materializedRows)*0.35 + float64(len(leaves))*18.0
	candidates[n] = plannerNoOrderLimitCandidate{
		kind:         plannerNoOrderLimitCandidateMaterializedFallback,
		cost:         materializedCost,
		expectedRows: materializedRows,
		buildWork:    uint64(materializedWork),
	}
	n++

	if facts.singlePrefix {
		prefix, ok, err := qv.prepareScalarPrefixRoute(leaves[0])
		if err == nil && ok {
			rows := universe
			span := 0
			if prefix.hasData && !prefix.br.Empty() {
				span, rows = prefix.ov.RangeStats(prefix.br)
				if rows == 0 {
					rows = 1
				}
			}
			expected := rows
			if expected > needWindow {
				expected = needWindow
			}
			candidates[n] = plannerNoOrderLimitCandidate{
				kind:         plannerNoOrderLimitCandidateDirectPrefix,
				cost:         float64(expected)*0.72 + float64(span)*0.25 + 4.0,
				expectedRows: expected,
				leadRows:     rows,
			}
			n++
		}
	}

	if facts.singleRange && !facts.singlePrefix {
		candidates[n] = plannerNoOrderLimitCandidate{
			kind:         plannerNoOrderLimitCandidateDirectRange,
			cost:         float64(needWindow) * 0.72,
			expectedRows: needWindow,
		}
		n++
	} else if facts.directBoundsOK && !facts.singlePrefix {
		fm := qv.exec.Schema.Fields[facts.directField]
		ov := qv.fieldIndexViewFromSlotsByName(qv.snap.Index, facts.directField)
		if fm != nil && !fm.Slice && ov.HasData() {
			br := ov.RangeForBounds(facts.directBounds)
			kind := plannerNoOrderLimitCandidateDirectRange
			if facts.directBoundLeaves > 1 {
				kind = plannerNoOrderLimitCandidateSameFieldBounds
			}
			residuals := facts.directResiduals
			if br.Empty() {
				candidates[n] = plannerNoOrderLimitCandidate{
					kind:   kind,
					checks: uint64(residuals),
				}
				n++
			} else if facts.directResidualsSupported {
				_, rows := ov.RangeStats(br)
				if rows == 0 {
					rows = 1
				}
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
					if residuals >= 3 && leadFound && rows > broadMin && expected > leadRows {
						cost = float64(expected)*(1.0+float64(residuals)*0.70) + float64(br.BaseEnd-br.BaseStart)*0.20
					}
				}
				if qv.hasPrefixBoundForField(leaves, facts.directField) && qv.shouldPreferExecutionNoOrderPrefix(q, leaves) {
					cost = float64(needWindow)*(0.70+float64(residuals)*0.15) + float64(br.BaseEnd-br.BaseStart)*0.001
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
	}

	if leadFound && allSupported {
		restSel := combinedSel / leadSel
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
		if expected > leadRows {
			expected = leadRows
		}
		if len(leaves) > 1 {
			checks = len(leaves) - 1
		}
		candidates[n] = plannerNoOrderLimitCandidate{
			kind:         plannerNoOrderLimitCandidateLeadScan,
			cost:         float64(expected)*(0.92+float64(checks)*1.05) + float64(len(leaves))*10.0,
			expectedRows: expected,
			leadRows:     leadRows,
			leadIdx:      leadIdx,
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
	if trace != nil {
		trace.SetNoOrderLimitRoute(decision.traceRoute())
		fallbackCost := decision.materializedFallback.cost
		if fallbackCost <= 0 {
			fallbackCost = decision.rejected.cost
		}
		trace.SetEstimated(decision.selected.expectedRows, decision.selected.cost, fallbackCost)
	}

	return qv.dispatchNoOrderLimit(q, facts, decision, trace)
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
	trace *Trace,
) ([]uint64, bool, PlanName, error) {
	leaves := facts.leaves

	switch decision.selectedKind() {
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
		out, used, err := qv.execSelectedNoOrderBounds(q, facts.directField, facts.directBounds, leaves, trace)
		if !used {
			if err != nil {
				return nil, true, "", err
			}
			return qv.dispatchNoOrderLimitFallback(q, decision)
		}
		return out, true, PlanLimitRangeNoOrder, err

	case plannerNoOrderLimitCandidateSameFieldBounds:
		if !facts.directBoundsOK {
			return qv.dispatchNoOrderLimitFallback(q, decision)
		}
		out, used, err := qv.execSelectedNoOrderBounds(q, facts.directField, facts.directBounds, leaves, trace)
		if !used {
			if err != nil {
				return nil, true, "", err
			}
			return qv.dispatchNoOrderLimitFallback(q, decision)
		}
		return out, true, PlanLimitRangeNoOrder, err

	case plannerNoOrderLimitCandidateLeadScan:
		out, used, err := qv.execSelectedNoOrderLeadScan(q, leaves, decision.selected.leadIdx, trace)
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

func (qv *View) execSelectedNoOrderLeadScan(q *qir.Shape, leaves []qir.Expr, leadIdx int, trace *Trace) ([]uint64, bool, error) {
	predSet, ok := qv.buildPredicatesCandidate(leaves)
	if !ok {
		return nil, false, nil
	}
	defer predSet.Release()
	for i := 0; i < predSet.Len(); i++ {
		if predSet.owner[i].alwaysFalse {
			return nil, true, nil
		}
	}
	return qv.execPlanLeadScanNoOrder(q, predSet.owner, leadIdx, trace), true, nil
}
