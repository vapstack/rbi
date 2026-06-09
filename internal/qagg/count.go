package qagg

import (
	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/qexec"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/rbitrace"
)

func PrepareCount(s *schema.Schema, exprs ...qx.Expr) (*qir.Query, error) {
	switch len(exprs) {
	case 1:
		return qir.PrepareCountExprResolved(s.IndexedByName, exprs[0])
	default:
		return qir.PrepareCountExprsResolved(s.IndexedByName, exprs...)
	}
}

func Count(view *qexec.View, q *qir.Query, emitTrace bool) (uint64, error) {
	shape := qir.NewShape(q)
	expr := shape.Expr
	traceEnabled := emitTrace && view.TraceSamplingEnabled()
	if !traceEnabled {
		if expr.Op == qir.OpConst {
			if expr.Not {
				return 0, nil
			}
			return view.SnapshotUniverseCardinality(), nil
		}
		if expr.FieldOrdinal >= 0 && len(expr.Operands) == 0 {
			if out, ok, err := view.TryFilterCardinalityByScalarLookup(expr, nil); ok || err != nil {
				return out, err
			}
			if out, ok, err := view.TryFilterCardinalityBySliceLookup(expr, nil); ok || err != nil {
				return out, err
			}
		}
	}

	var trace *qexec.Trace
	if traceEnabled {
		trace = view.BeginTrace(shape.WithExpr(expr))
	}

	if expr.Op == qir.OpConst {
		out := view.SnapshotUniverseCardinality()
		if expr.Not {
			out = 0
		}
		if trace != nil {
			trace.SetPlan(rbitrace.PlanCountMaterialized)
			if !expr.Not {
				trace.AddExamined(out)
			}
		}
		return countFinishTrace(trace, out, nil)
	}

	if out, ok, err := view.TryFilterCardinalityByUniqueEq(expr, trace); ok || err != nil {
		return countFinishTrace(trace, out, err)
	}
	if out, ok, err := view.TryFilterCardinalityByScalarLookup(expr, trace); ok || err != nil {
		return countFinishTrace(trace, out, err)
	}
	if out, ok, err := view.TryFilterCardinalityBySliceLookup(expr, trace); ok || err != nil {
		return countFinishTrace(trace, out, err)
	}
	if out, ok, err := view.TryFilterCardinalityByScalarInSplit(expr, trace); ok || err != nil {
		return countFinishTrace(trace, out, err)
	}
	if out, ok, err := view.TryFilterCardinalityPreparedAndReordered(expr); ok || err != nil {
		if trace != nil && ok {
			trace.SetPlan(rbitrace.PlanCountMaterialized)
		}
		return countFinishTrace(trace, out, err)
	}
	if out, ok, err := view.TryFilterCardinalityByPredicates(expr, trace); ok || err != nil {
		return countFinishTrace(trace, out, err)
	}
	if out, ok, err := view.TryFilterCardinalityORByPredicates(expr, trace); ok || err != nil {
		return countFinishTrace(trace, out, err)
	}

	out, err := view.FilterCardinalityByMaterializedExpr(expr, trace)
	return countFinishTrace(trace, out, err)
}

func countFinishTrace(trace *qexec.Trace, out uint64, err error) (uint64, error) {
	if trace != nil {
		trace.Finish(out, err)
	}
	return out, err
}
