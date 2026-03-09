package rbi

import (
	"reflect"
	"slices"
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

	out, changed := normalizeExpr(in)
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

	out, changed := normalizeExpr(in)
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

	out, changed := normalizeExpr(in)
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

	out, changed := normalizeExpr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}
	if !reflect.DeepEqual(out, leaf) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, leaf)
	}
}

func TestNormalize_WrappedQueryMatchesDirectResults(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 10_000)

	direct := qx.Query(
		qx.GTE("age", 18),
		qx.EQ("active", true),
	).By("score", qx.DESC).Skip(500).Max(100)

	wrapped := &qx.QX{
		Expr: qx.Expr{
			Op: qx.OpOR,
			Operands: []qx.Expr{
				direct.Expr,
				{Op: qx.OpNOOP, Not: true},
			},
		},
		Order:  direct.Order,
		Offset: direct.Offset,
		Limit:  direct.Limit,
	}

	gotDirect, err := db.QueryKeys(direct)
	if err != nil {
		t.Fatalf("QueryKeys(direct): %v", err)
	}
	gotWrapped, err := db.QueryKeys(wrapped)
	if err != nil {
		t.Fatalf("QueryKeys(wrapped): %v", err)
	}

	if !slices.Equal(gotWrapped, gotDirect) {
		t.Fatalf("results mismatch:\n wrapped=%v\n direct=%v", gotWrapped, gotDirect)
	}
}
