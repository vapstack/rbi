package rbi

import (
	"slices"
	"testing"

	"github.com/vapstack/qx"
)

func TestExprValueToIdxOwnedClonesStringSlice(t *testing.T) {
	db := &DB[uint64, Rec]{}
	src := []string{"x"}

	vals, isSlice, hasNil, err := db.exprValueToIdxOwned(qx.Expr{Op: qx.OpIN, Value: src})
	if err != nil {
		t.Fatalf("exprValueToIdxOwned: %v", err)
	}
	if !isSlice {
		t.Fatalf("expected slice input")
	}
	if hasNil {
		t.Fatalf("did not expect nil values")
	}

	vals[0] = "y"
	if src[0] != "x" {
		t.Fatalf("owned values mutated source slice: got=%q want=%q", src[0], "x")
	}
}

func TestExprValueToDistinctIdxOwnedDoesNotMutateSource(t *testing.T) {
	db := &DB[uint64, Rec]{}
	src := []string{"b", "a", "a"}

	vals, isSlice, hasNil, err := db.exprValueToDistinctIdxOwned(qx.Expr{Op: qx.OpIN, Value: src})
	if err != nil {
		t.Fatalf("exprValueToDistinctIdxOwned: %v", err)
	}
	if !isSlice {
		t.Fatalf("expected slice input")
	}
	if hasNil {
		t.Fatalf("did not expect nil values")
	}

	if !slices.Equal(vals, []string{"a", "b"}) {
		t.Fatalf("distinct values mismatch: got=%v want=%v", vals, []string{"a", "b"})
	}
	if !slices.Equal(src, []string{"b", "a", "a"}) {
		t.Fatalf("source slice was mutated: got=%v want=%v", src, []string{"b", "a", "a"})
	}
}
