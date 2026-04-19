package qir

import (
	"reflect"
	"testing"

	"github.com/vapstack/qx"
)

type testPrepareResolver struct{}

func (testPrepareResolver) ResolveField(name string) (int, bool) {
	switch name {
	case "status":
		return 1, true
	case "tags":
		return 2, true
	default:
		return 0, false
	}
}

var testPrepareFieldResolver testPrepareResolver

func TestPrepareCountExpr_TrimmedManualOpNameRejected(t *testing.T) {
	_, err := PrepareCountExprResolved(testPrepareFieldResolver, qx.Expr{
		Kind: qx.KindOP,
		Name: "  eq  ",
		Args: []qx.Expr{
			qx.REF("status"),
			qx.LIT("active"),
		},
	})
	if err == nil {
		t.Fatal("expected PrepareCountExpr to reject non-canonical op name")
	}
}

func TestPrepareQuery_TrimmedManualOrderOpNameRejected(t *testing.T) {
	_, err := PrepareQueryResolved(&qx.QX{
		Filter: qx.EQ("status", "active"),
		Order: []qx.Order{{
			By: qx.Expr{
				Kind: qx.KindOP,
				Name: "  pos  ",
				Args: []qx.Expr{
					qx.REF("tags"),
					qx.LIT("go"),
				},
			},
			Desc: true,
		}},
	}, testPrepareFieldResolver)
	if err == nil {
		t.Fatal("expected PrepareQuery to reject non-canonical order op name")
	}
}

func TestPrepareCountExpr_MalformedKindNoneRejected(t *testing.T) {
	_, err := PrepareCountExprResolved(testPrepareFieldResolver, qx.Expr{
		Kind:  qx.KindNONE,
		Name:  qx.OpEQ,
		Value: "active",
		Args: []qx.Expr{
			qx.REF("status"),
			qx.LIT("active"),
		},
	})
	if err == nil {
		t.Fatal("expected PrepareCountExpr to reject malformed KindNONE expression")
	}
}

func TestPrepareQuery_POSScalarStringRejected(t *testing.T) {
	_, err := PrepareQueryResolved(
		qx.Query().SortBy(qx.POS("status", "alice bob"), qx.ASC),
		testPrepareFieldResolver,
	)
	if err == nil {
		t.Fatal("expected PrepareQuery to reject scalar-string POS order literal")
	}
}

func TestQueryReleaseOwned_ClearsInlineOrderStorage(t *testing.T) {
	q := queryPool.Get()
	q.setOrder(Order{
		FieldOrdinal: 2,
		Kind:         OrderKindArrayPos,
		Data:         []string{"go", "ops"},
		Desc:         true,
	})

	q.releaseOwned()

	if q.Order != nil {
		t.Fatalf("expected releaseOwned to drop query order slice")
	}
	if q.orderStorage[0].FieldOrdinal != 0 || q.orderStorage[0].Kind != 0 || q.orderStorage[0].Data != nil || q.orderStorage[0].Desc {
		t.Fatalf("expected releaseOwned to clear inline order storage, got %+v", q.orderStorage[0])
	}
}

func TestPrepareCountExprs_NilResolverPreservesDistinctFieldIdentity(t *testing.T) {
	prepared, err := PrepareCountExprsNoResolve(
		qx.EQ("status", "active"),
		qx.EQ("country", "active"),
	)
	if err != nil {
		t.Fatalf("PrepareCountExprs(nil): %v", err)
	}
	defer prepared.Release()

	if prepared.Expr.Op != OpAND || len(prepared.Expr.Operands) != 2 {
		t.Fatalf("expected AND with two leaves, got %+v", prepared.Expr)
	}

	left := prepared.Expr.Operands[0]
	right := prepared.Expr.Operands[1]
	if left.FieldOrdinal == NoFieldOrdinal || right.FieldOrdinal == NoFieldOrdinal {
		t.Fatalf("expected synthetic field ordinals, got left=%d right=%d", left.FieldOrdinal, right.FieldOrdinal)
	}
	if left.FieldOrdinal == right.FieldOrdinal {
		t.Fatalf("distinct fields collapsed to one identity: left=%+v right=%+v", left, right)
	}
}

func TestPrepareCountExprs_NilResolverKeepsSameFieldComplementFold(t *testing.T) {
	prepared, err := PrepareCountExprsNoResolve(
		qx.EQ("status", "active"),
		qx.NOT(qx.EQ("status", "active")),
	)
	if err != nil {
		t.Fatalf("PrepareCountExprs(nil): %v", err)
	}
	defer prepared.Release()

	if !IsFalseConst(prepared.Expr) {
		t.Fatalf("expected same-field complement to normalize to false, got %+v", prepared.Expr)
	}
}

func TestPrepareCountExpr_NormalizedTreeAvoidsExtraOwnedCopy(t *testing.T) {
	prepared, err := PrepareCountExprResolved(testPrepareFieldResolver, qx.AND(
		qx.AND(
			qx.EQ("status", "active"),
			qx.PREFIX("tags", "go"),
		),
		qx.NOT(qx.EQ("status", "banned")),
	))
	if err != nil {
		t.Fatalf("PrepareCountExpr: %v", err)
	}
	defer prepared.Release()

	if prepared.Expr.Op != OpAND || len(prepared.Expr.Operands) != 3 {
		t.Fatalf("expected flattened AND with three leaves, got %+v", prepared.Expr)
	}
	if prepared.exprOwnersUsed != 3 {
		t.Fatalf("expected one raw outer slice, one raw inner slice, and one normalized root slice; got exprOwnersUsed=%d", prepared.exprOwnersUsed)
	}
}

func TestNormalizeExpr_OrFalseCollapsesToLeaf(t *testing.T) {
	leaf := Expr{Op: OpGTE, FieldOrdinal: 1, Value: 21}
	in := Expr{
		Op:           OpOR,
		FieldOrdinal: NoFieldOrdinal,
		Operands: []Expr{
			leaf,
			{Op: OpNOOP, Not: true, FieldOrdinal: NoFieldOrdinal},
		},
	}

	out, changed := NormalizeExpr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}
	if !reflect.DeepEqual(out, leaf) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, leaf)
	}
}

func TestNormalizeExpr_DeMorganForNotAnd(t *testing.T) {
	in := Expr{
		Op:           OpAND,
		Not:          true,
		FieldOrdinal: NoFieldOrdinal,
		Operands: []Expr{
			{Op: OpEQ, FieldOrdinal: 1, Value: true},
			{Op: OpIN, FieldOrdinal: 2, Value: []string{"DE", "NL"}},
		},
	}

	out, changed := NormalizeExpr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}

	want := Expr{
		Op:           OpOR,
		FieldOrdinal: NoFieldOrdinal,
		Operands: []Expr{
			{Op: OpEQ, FieldOrdinal: 1, Value: true, Not: true},
			{Op: OpIN, FieldOrdinal: 2, Value: []string{"DE", "NL"}, Not: true},
		},
	}

	if !reflect.DeepEqual(out, want) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, want)
	}
}

func TestNormalizeExpr_FlattensAndNoopNoise(t *testing.T) {
	leafA := Expr{Op: OpEQ, FieldOrdinal: 1, Value: true}
	leafB := Expr{Op: OpIN, FieldOrdinal: 2, Value: []string{"DE", "NL"}}

	in := Expr{
		Op:           OpAND,
		FieldOrdinal: NoFieldOrdinal,
		Operands: []Expr{
			{Op: OpNOOP, FieldOrdinal: NoFieldOrdinal},
			{
				Op:           OpAND,
				FieldOrdinal: NoFieldOrdinal,
				Operands: []Expr{
					leafA,
					{Op: OpNOOP, FieldOrdinal: NoFieldOrdinal},
				},
			},
			leafB,
		},
	}

	out, changed := NormalizeExpr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}

	want := Expr{
		Op:           OpAND,
		FieldOrdinal: NoFieldOrdinal,
		Operands: []Expr{
			leafA,
			leafB,
		},
	}

	if !reflect.DeepEqual(out, want) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, want)
	}
}

func TestNormalizeExpr_DoubleNotEliminates(t *testing.T) {
	leaf := Expr{Op: OpEQ, FieldOrdinal: 1, Value: 33}
	in := Expr{
		Op:           OpOR,
		Not:          true,
		FieldOrdinal: NoFieldOrdinal,
		Operands: []Expr{
			{Op: OpAND, Not: true, FieldOrdinal: NoFieldOrdinal, Operands: []Expr{leaf}},
		},
	}

	out, changed := NormalizeExpr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}
	if !reflect.DeepEqual(out, leaf) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, leaf)
	}
}

func TestNormalizeExpr_ExactDuplicateANDCollapses(t *testing.T) {
	leaf := Expr{Op: OpEQ, FieldOrdinal: 1, Value: true}
	in := Expr{
		Op:           OpAND,
		FieldOrdinal: NoFieldOrdinal,
		Operands: []Expr{
			leaf,
			leaf,
		},
	}

	out, changed := NormalizeExpr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}
	if !reflect.DeepEqual(out, leaf) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, leaf)
	}
}

func TestNormalizeExpr_ExactDuplicateORCollapses(t *testing.T) {
	leaf := Expr{Op: OpPREFIX, FieldOrdinal: 1, Value: "ali"}
	in := Expr{
		Op:           OpOR,
		FieldOrdinal: NoFieldOrdinal,
		Operands: []Expr{
			leaf,
			leaf,
		},
	}

	out, changed := NormalizeExpr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}
	if !reflect.DeepEqual(out, leaf) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, leaf)
	}
}

func TestNormalizeExpr_ExactLeafComplementANDToFalse(t *testing.T) {
	leaf := Expr{Op: OpEQ, FieldOrdinal: 1, Value: true}
	in := Expr{
		Op:           OpAND,
		FieldOrdinal: NoFieldOrdinal,
		Operands: []Expr{
			leaf,
			{Op: leaf.Op, FieldOrdinal: leaf.FieldOrdinal, Value: leaf.Value, Not: true},
		},
	}

	out, changed := NormalizeExpr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}
	want := Expr{Op: OpNOOP, Not: true, FieldOrdinal: NoFieldOrdinal}
	if !reflect.DeepEqual(out, want) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, want)
	}
}

func TestNormalizeExpr_ExactLeafComplementORToTrue(t *testing.T) {
	leaf := Expr{Op: OpEQ, FieldOrdinal: 1, Value: true}
	in := Expr{
		Op:           OpOR,
		FieldOrdinal: NoFieldOrdinal,
		Operands: []Expr{
			leaf,
			{Op: leaf.Op, FieldOrdinal: leaf.FieldOrdinal, Value: leaf.Value, Not: true},
		},
	}

	out, changed := NormalizeExpr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}
	want := Expr{Op: OpNOOP, FieldOrdinal: NoFieldOrdinal}
	if !reflect.DeepEqual(out, want) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, want)
	}
}

func TestNormalizeExpr_MalformedNoopComplementStaysMalformed(t *testing.T) {
	in := Expr{
		Op:           OpAND,
		FieldOrdinal: NoFieldOrdinal,
		Operands: []Expr{
			{Op: OpNOOP, FieldOrdinal: 1},
			{Op: OpNOOP, FieldOrdinal: 1, Not: true},
		},
	}

	out, changed := NormalizeExpr(in)
	if changed {
		t.Fatalf("expected malformed NOOP pair to stay untouched, got=%+v", out)
	}
	if !reflect.DeepEqual(out, in) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, in)
	}
}
