package qexec

import (
	"slices"
	"testing"

	"github.com/vapstack/pooled"
	"github.com/vapstack/qx"
)

func TestExprValueToDistinctIdxBufClonesStringSlice(t *testing.T) {
	db, _ := openTempDBUint64(t)
	src := []string{"x"}
	expr := mustTestQIRExprForDB(t, db, qx.IN("name", src))

	vals, isSlice, hasNil, err := db.engine.currentQueryViewForTests().exprValueToDistinctIdxBuf(expr)
	if err != nil {
		t.Fatalf("exprValueToDistinctIdxBuf: %v", err)
	}
	defer pooled.ReleaseStringSlice(vals)
	if !isSlice {
		t.Fatalf("expected slice input")
	}
	if hasNil {
		t.Fatalf("did not expect nil values")
	}
	if !slices.Equal(vals, []string{"x"}) {
		t.Fatalf("owned values mismatch: got=%v want=%v", vals, []string{"x"})
	}

	vals[0] = "y"
	if src[0] != "x" {
		t.Fatalf("owned values mutated source slice: got=%q want=%q", src[0], "x")
	}
}

func TestExprValueToDistinctIdxBufDoesNotMutateSource(t *testing.T) {
	db, _ := openTempDBUint64(t)
	src := []string{"b", "a", "a"}
	expr := mustTestQIRExprForDB(t, db, qx.IN("name", src))

	vals, isSlice, hasNil, err := db.engine.currentQueryViewForTests().exprValueToDistinctIdxBuf(expr)
	if err != nil {
		t.Fatalf("exprValueToDistinctIdxBuf: %v", err)
	}
	defer pooled.ReleaseStringSlice(vals)
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
