package normalize

import (
	"reflect"
	"testing"

	"github.com/vapstack/qx"
)

func TestNormalizeExpr_OrFalseCollapsesToLeaf(t *testing.T) {
	leaf := qx.Expr{Op: qx.OpGTE, Field: "age", Value: 21}
	in := qx.Expr{
		Op: qx.OpOR,
		Operands: []qx.Expr{
			leaf,
			{Op: qx.OpNOOP, Not: true},
		},
	}

	out, changed := Expr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}
	if !reflect.DeepEqual(out, leaf) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, leaf)
	}
}

func TestNormalizeExpr_DeMorganForNotAnd(t *testing.T) {
	in := qx.Expr{
		Op:  qx.OpAND,
		Not: true,
		Operands: []qx.Expr{
			{Op: qx.OpEQ, Field: "active", Value: true},
			{Op: qx.OpIN, Field: "country", Value: []string{"DE", "NL"}},
		},
	}

	out, changed := Expr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}

	want := qx.Expr{
		Op: qx.OpOR,
		Operands: []qx.Expr{
			{Op: qx.OpEQ, Field: "active", Value: true, Not: true},
			{Op: qx.OpIN, Field: "country", Value: []string{"DE", "NL"}, Not: true},
		},
	}

	if !reflect.DeepEqual(out, want) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, want)
	}
}

func TestNormalizeExpr_FlattensAndNoopNoise(t *testing.T) {
	leafA := qx.Expr{Op: qx.OpEQ, Field: "active", Value: true}
	leafB := qx.Expr{Op: qx.OpIN, Field: "country", Value: []string{"DE", "NL"}}

	in := qx.Expr{
		Op: qx.OpAND,
		Operands: []qx.Expr{
			{Op: qx.OpNOOP},
			{
				Op: qx.OpAND,
				Operands: []qx.Expr{
					leafA,
					{Op: qx.OpNOOP},
				},
			},
			leafB,
		},
	}

	out, changed := Expr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}

	want := qx.Expr{
		Op: qx.OpAND,
		Operands: []qx.Expr{
			leafA,
			leafB,
		},
	}

	if !reflect.DeepEqual(out, want) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, want)
	}
}

func TestNormalizeExpr_DoubleNotEliminates(t *testing.T) {
	leaf := qx.Expr{Op: qx.OpEQ, Field: "age", Value: 33}
	in := qx.Expr{
		Op:  qx.OpOR,
		Not: true,
		Operands: []qx.Expr{
			{Op: qx.OpAND, Not: true, Operands: []qx.Expr{leaf}},
		},
	}

	out, changed := Expr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}
	if !reflect.DeepEqual(out, leaf) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, leaf)
	}
}

func TestNormalizeExpr_ExactDuplicateANDCollapses(t *testing.T) {
	leaf := qx.Expr{Op: qx.OpEQ, Field: "active", Value: true}
	in := qx.Expr{
		Op: qx.OpAND,
		Operands: []qx.Expr{
			leaf,
			leaf,
		},
	}

	out, changed := Expr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}
	if !reflect.DeepEqual(out, leaf) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, leaf)
	}
}

func TestNormalizeExpr_ExactDuplicateORCollapses(t *testing.T) {
	leaf := qx.Expr{Op: qx.OpPREFIX, Field: "email", Value: "ali"}
	in := qx.Expr{
		Op: qx.OpOR,
		Operands: []qx.Expr{
			leaf,
			leaf,
		},
	}

	out, changed := Expr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}
	if !reflect.DeepEqual(out, leaf) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, leaf)
	}
}

func TestNormalizeExpr_ExactLeafComplementANDToFalse(t *testing.T) {
	leaf := qx.Expr{Op: qx.OpEQ, Field: "active", Value: true}
	in := qx.Expr{
		Op: qx.OpAND,
		Operands: []qx.Expr{
			leaf,
			{Op: leaf.Op, Field: leaf.Field, Value: leaf.Value, Not: true},
		},
	}

	out, changed := Expr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}
	want := qx.Expr{Op: qx.OpNOOP, Not: true}
	if !reflect.DeepEqual(out, want) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, want)
	}
}

func TestNormalizeExpr_ExactLeafComplementORToTrue(t *testing.T) {
	leaf := qx.Expr{Op: qx.OpEQ, Field: "active", Value: true}
	in := qx.Expr{
		Op: qx.OpOR,
		Operands: []qx.Expr{
			leaf,
			{Op: leaf.Op, Field: leaf.Field, Value: leaf.Value, Not: true},
		},
	}

	out, changed := Expr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}
	want := qx.Expr{Op: qx.OpNOOP}
	if !reflect.DeepEqual(out, want) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, want)
	}
}

func TestNormalizeExpr_MalformedNoopComplementStaysMalformed(t *testing.T) {
	in := qx.Expr{
		Op: qx.OpAND,
		Operands: []qx.Expr{
			{Op: qx.OpNOOP, Field: "broken"},
			{Op: qx.OpNOOP, Field: "broken", Not: true},
		},
	}

	out, changed := Expr(in)
	if changed {
		t.Fatalf("expected malformed NOOP pair to stay untouched, got=%+v", out)
	}
	if !reflect.DeepEqual(out, in) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, in)
	}
}
