package rbi

import (
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/qagg"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/qir"
)

func (qe *queryEngine) fieldNameByOrdinal(ordinal int) string {
	return qe.exec.FieldNameByOrdinal(ordinal)
}

func testExprFieldName(qe *queryEngine, expr qir.Expr) string {
	if qe == nil {
		return ""
	}
	return qe.fieldNameByOrdinal(expr.FieldOrdinal)
}

func testOrderFieldName(qe *queryEngine, order qir.Order) string {
	if qe == nil {
		return ""
	}
	return qe.fieldNameByOrdinal(order.FieldOrdinal)
}

func prepareTestQuery(qe *queryEngine, q *qx.QX) (*qir.Query, qir.Shape, error) {
	var (
		prepared *qir.Query
		err      error
	)
	if qe == nil {
		prepared, err = qir.PrepareQueryNoResolve(q)
	} else {
		prepared, err = qir.PrepareQuery(q, qe.schema.IndexedByName)
	}
	if err != nil {
		return nil, qir.Shape{}, err
	}
	return prepared, qir.NewShape(prepared), nil
}

func prepareTestExpr(qe *queryEngine, expr qx.Expr) (*qir.Query, qir.Expr, error) {
	var (
		prepared *qir.Query
		err      error
	)
	if qe == nil {
		prepared, err = qir.PrepareCountExprsNoResolve(expr)
	} else {
		prepared, err = qir.PrepareCountExprsResolved(qe.schema.IndexedByName, expr)
	}
	if err != nil {
		return nil, qir.Expr{}, err
	}
	return prepared, prepared.Expr, nil
}

func prepareTestExprs(qe *queryEngine, exprs []qx.Expr) ([]*qir.Query, []qir.Expr, error) {
	prepared := make([]*qir.Query, 0, len(exprs))
	out := make([]qir.Expr, 0, len(exprs))
	for i := range exprs {
		p, expr, err := prepareTestExpr(qe, exprs[i])
		if err != nil {
			releasePreparedQueriesForTest(prepared)
			return nil, nil, err
		}
		prepared = append(prepared, p)
		out = append(out, detachTestQIRExpr(expr))
	}
	return prepared, out, nil
}

func releasePreparedQueriesForTest(prepared []*qir.Query) {
	for i := range prepared {
		prepared[i].Release()
	}
}

func unwrapExprValue(v reflect.Value) (reflect.Value, bool) {
	for v.IsValid() {
		switch v.Kind() {
		case reflect.Interface, reflect.Pointer:
			if v.IsNil() {
				return reflect.Value{}, true
			}
			v = v.Elem()
		default:
			return v, false
		}
	}
	return reflect.Value{}, true
}

func detachTestQIRExpr(expr qir.Expr) qir.Expr {
	if len(expr.Operands) == 0 {
		return expr
	}
	out := expr
	out.Operands = make([]qir.Expr, len(expr.Operands))
	for i := range expr.Operands {
		out.Operands[i] = detachTestQIRExpr(expr.Operands[i])
	}
	return out
}

func mustTestQIRExpr(t testing.TB, expr qx.Expr) qir.Expr {
	t.Helper()
	prepared, compiled, err := prepareTestExpr(nil, expr)
	if err != nil {
		t.Fatalf("prepareTestExpr(%+v): %v", expr, err)
	}
	compiled = detachTestQIRExpr(compiled)
	prepared.Release()
	return compiled
}

func compileScalarOpForTest(op qx.Op) qir.Op {
	switch op {
	case qx.OpEQ:
		return qir.OpEQ
	case qx.OpGT:
		return qir.OpGT
	case qx.OpGTE:
		return qir.OpGTE
	case qx.OpLT:
		return qir.OpLT
	case qx.OpLTE:
		return qir.OpLTE
	case qx.OpIN:
		return qir.OpIN
	case qx.OpHASANY:
		return qir.OpHASANY
	case qx.OpHASALL:
		return qir.OpHASALL
	case qx.OpPREFIX:
		return qir.OpPREFIX
	case qx.OpSUFFIX:
		return qir.OpSUFFIX
	case qx.OpCONTAINS:
		return qir.OpCONTAINS
	default:
		panic(fmt.Sprintf("unsupported qx op in test helper: %q", op))
	}
}

func (qe *queryEngine) filterCardinalityForTests(expr qx.Expr) (uint64, error) {
	prepared, err := qagg.PrepareCount(qe.schema, expr)
	if err != nil {
		return 0, err
	}
	defer prepared.Release()

	snap, seq, ref := qe.snapshot.PinCurrent()
	defer qe.snapshot.Unpin(seq, ref)

	view := qe.exec.AcquireView(snap)
	defer qe.exec.ReleaseView(view)
	return qagg.Count(view, prepared, false)
}

func (qe *queryEngine) checkUsedQuery(q *qx.QX) error {
	prepared, _, err := prepareTestQuery(qe, q)
	if err != nil {
		return err
	}
	prepared.Release()
	return nil
}

func setNumericBucketKnobs(t *testing.T, db *DB[uint64, Rec], size, minFieldKeys, minSpan int) {
	t.Helper()

	prevSize := db.options.NumericRangeBucketSize
	prevMinField := db.options.NumericRangeBucketMinFieldKeys
	prevMinSpan := db.options.NumericRangeBucketMinSpanKeys
	prevEngineSize := db.engine.exec.NumericRangeBucketSize
	prevEngineMinField := db.engine.exec.NumericRangeBucketMinFieldKeys
	prevEngineMinSpan := db.engine.exec.NumericRangeBucketMinSpanKeys

	db.options.NumericRangeBucketSize = size
	db.options.NumericRangeBucketMinFieldKeys = minFieldKeys
	db.options.NumericRangeBucketMinSpanKeys = minSpan
	db.engine.exec.NumericRangeBucketSize = size
	db.engine.exec.NumericRangeBucketMinFieldKeys = minFieldKeys
	db.engine.exec.NumericRangeBucketMinSpanKeys = minSpan

	t.Cleanup(func() {
		db.options.NumericRangeBucketSize = prevSize
		db.options.NumericRangeBucketMinFieldKeys = prevMinField
		db.options.NumericRangeBucketMinSpanKeys = prevMinSpan
		db.engine.exec.NumericRangeBucketSize = prevEngineSize
		db.engine.exec.NumericRangeBucketMinFieldKeys = prevEngineMinField
		db.engine.exec.NumericRangeBucketMinSpanKeys = prevEngineMinSpan
	})
}

func (qe *queryEngine) materializedPredCacheKey(expr qx.Expr) string {
	prepared, compiled, err := prepareTestExpr(qe, expr)
	if err != nil {
		return ""
	}
	defer prepared.Release()
	field := qe.fieldNameByOrdinal(compiled.FieldOrdinal)
	switch v := compiled.Value.(type) {
	case string:
		return qcache.MaterializedPredKeyForScalar(field, compiled.Op, v).String()
	case nil:
		return ""
	default:
		return ""
	}
}

func (qe *queryEngine) exprValueToIdxOwned(expr qx.Expr) ([]string, bool, bool, error) {
	prepared, compiled, err := prepareTestExpr(nil, expr)
	if err != nil {
		return nil, false, false, err
	}
	defer prepared.Release()

	if compiled.Value == nil {
		if compiled.Op == qir.OpIN {
			return nil, true, false, nil
		}
		return nil, false, true, nil
	}
	switch v := compiled.Value.(type) {
	case []string:
		return slices.Clone(v), true, false, nil
	case string:
		return []string{v}, false, false, nil
	default:
		return nil, false, false, fmt.Errorf("test helper: unsupported value type %T", compiled.Value)
	}
}

func (qe *queryEngine) exprValueToDistinctIdxOwned(expr qx.Expr) ([]string, bool, bool, error) {
	vals, isSlice, hasNil, err := qe.exprValueToIdxOwned(expr)
	if err != nil || len(vals) < 2 {
		return vals, isSlice, hasNil, err
	}
	slices.Sort(vals)
	n := 1
	for i := 1; i < len(vals); i++ {
		if vals[i] != vals[n-1] {
			vals[n] = vals[i]
			n++
		}
	}
	return vals[:n], isSlice, hasNil, nil
}
