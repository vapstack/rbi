package qexec

import (
	"slices"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/keycodec"
)

type queryValuesFoldedString string

func (s *queryValuesFoldedString) IndexingValue() string {
	if s == nil {
		return "<nil>"
	}
	return string(*s)
}

type queryValuesValueIndexerRec struct {
	Key *queryValuesFoldedString `db:"key" rbi:"index"`
}

func TestExprValueToDistinctIdxBufClonesStringSlice(t *testing.T) {
	db, _ := openTempDBUint64(t)
	src := []string{"x"}
	expr := mustTestQIRExprForDB(t, db, qx.IN("name", src))

	vals, isSlice, hasNil, err := db.engine.currentQueryViewForTests().exprValueToDistinctLookupKeyBuf(expr)
	if err != nil {
		t.Fatalf("exprValueToDistinctLookupKeyBuf: %v", err)
	}
	defer keycodec.ReleaseIndexLookupKeySlice(vals)
	if !isSlice {
		t.Fatalf("expected slice input")
	}
	if hasNil {
		t.Fatalf("did not expect nil values")
	}
	if !slices.Equal(vals, []keycodec.IndexLookupKey{keycodec.IndexLookupString("x")}) {
		t.Fatalf("owned values mismatch: got=%v want=%v", vals, []string{"x"})
	}

	vals[0] = keycodec.IndexLookupString("y")
	if src[0] != "x" {
		t.Fatalf("owned values mutated source slice: got=%q want=%q", src[0], "x")
	}
}

func TestExprValueToDistinctIdxBufDoesNotMutateSource(t *testing.T) {
	db, _ := openTempDBUint64(t)
	src := []string{"b", "a", "a"}
	expr := mustTestQIRExprForDB(t, db, qx.IN("name", src))

	vals, isSlice, hasNil, err := db.engine.currentQueryViewForTests().exprValueToDistinctLookupKeyBuf(expr)
	if err != nil {
		t.Fatalf("exprValueToDistinctLookupKeyBuf: %v", err)
	}
	defer keycodec.ReleaseIndexLookupKeySlice(vals)
	if !isSlice {
		t.Fatalf("expected slice input")
	}
	if hasNil {
		t.Fatalf("did not expect nil values")
	}

	if !slices.Equal(vals, []keycodec.IndexLookupKey{keycodec.IndexLookupString("a"), keycodec.IndexLookupString("b")}) {
		t.Fatalf("distinct values mismatch: got=%v want=%v", vals, []string{"a", "b"})
	}
	if !slices.Equal(src, []string{"b", "a", "a"}) {
		t.Fatalf("source slice was mutated: got=%v want=%v", src, []string{"b", "a", "a"})
	}
}

func TestValueIndexerTypedNilQueryMatchesNilReceiverKey(t *testing.T) {
	db := newFixtureDB[uint64, queryValuesValueIndexerRec](t, "", Options{})
	var folded *queryValuesFoldedString
	if err := db.Set(1, &queryValuesValueIndexerRec{Key: folded}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	count, err := db.Count(qx.EQ("key", folded))
	if err != nil {
		t.Fatalf("EQ typed nil: %v", err)
	}
	if count != 1 {
		t.Fatalf("EQ typed nil count=%d want 1", count)
	}

	count, err = db.Count(qx.IN("key", []*queryValuesFoldedString{folded}))
	if err != nil {
		t.Fatalf("IN typed nil: %v", err)
	}
	if count != 1 {
		t.Fatalf("IN typed nil count=%d want 1", count)
	}

	count, err = db.Count(qx.EQ("key", nil))
	if err != nil {
		t.Fatalf("EQ nil: %v", err)
	}
	if count != 0 {
		t.Fatalf("EQ nil count=%d want 0", count)
	}
}
