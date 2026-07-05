package qexec

import (
	"slices"
	"strings"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/schema"
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

type queryValuesValueReceiverString string

func (s queryValuesValueReceiverString) IndexingValue() string {
	return strings.ToLower(string(s))
}

type queryValuesValueReceiverPtrRec struct {
	Key  *queryValuesValueReceiverString   `db:"key" rbi:"index"`
	Tags []*queryValuesValueReceiverString `db:"tags" rbi:"index"`
}

type queryValuesNamedSliceValueIndexer []string

func (v queryValuesNamedSliceValueIndexer) IndexingValue() string {
	return strings.Join(v, ":")
}

type queryValuesNamedSliceValueIndexerRec struct {
	Key  queryValuesNamedSliceValueIndexer    `db:"key" rbi:"index"`
	Ptr  *queryValuesNamedSliceValueIndexer   `db:"ptr" rbi:"index"`
	Tags []*queryValuesNamedSliceValueIndexer `db:"tags" rbi:"index"`
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

func TestValueReceiverValueIndexerPointerNilQueriesAsNil(t *testing.T) {
	db := newFixtureDB[uint64, queryValuesValueReceiverPtrRec](t, "", Options{})
	a := queryValuesValueReceiverString("AA")
	b := queryValuesValueReceiverString("BB")
	if err := db.Set(1, &queryValuesValueReceiverPtrRec{Key: nil, Tags: []*queryValuesValueReceiverString{nil, &a}}); err != nil {
		t.Fatalf("Set nil: %v", err)
	}
	if err := db.Set(2, &queryValuesValueReceiverPtrRec{Key: &a, Tags: []*queryValuesValueReceiverString{nil, &b, &b}}); err != nil {
		t.Fatalf("Set value: %v", err)
	}

	var nilKey *queryValuesValueReceiverString
	count, err := db.Count(qx.EQ("key", nilKey))
	if err != nil {
		t.Fatalf("EQ typed nil: %v", err)
	}
	if count != 1 {
		t.Fatalf("EQ typed nil count=%d want 1", count)
	}

	count, err = db.Count(qx.EQ("key", nil))
	if err != nil {
		t.Fatalf("EQ nil: %v", err)
	}
	if count != 1 {
		t.Fatalf("EQ nil count=%d want 1", count)
	}

	count, err = db.Count(qx.IN("key", []*queryValuesValueReceiverString{nilKey, &a}))
	if err != nil {
		t.Fatalf("IN typed nil and value: %v", err)
	}
	if count != 2 {
		t.Fatalf("IN typed nil and value count=%d want 2", count)
	}

	count, err = db.Count(qx.HASANY("tags", []*queryValuesValueReceiverString{nil, &b}))
	if err != nil {
		t.Fatalf("HASANY typed nil and value: %v", err)
	}
	if count != 1 {
		t.Fatalf("HASANY typed nil and value count=%d want 1", count)
	}
}

func TestNamedSliceValueIndexerCollectionQueriesUseIndexingValue(t *testing.T) {
	db := newFixtureDB[uint64, queryValuesNamedSliceValueIndexerRec](t, "", Options{})
	a := queryValuesNamedSliceValueIndexer{"a", "b"}
	b := queryValuesNamedSliceValueIndexer{"c", "d"}
	if err := db.Set(1, &queryValuesNamedSliceValueIndexerRec{Key: a, Ptr: &a, Tags: []*queryValuesNamedSliceValueIndexer{&a}}); err != nil {
		t.Fatalf("Set a: %v", err)
	}
	if err := db.Set(2, &queryValuesNamedSliceValueIndexerRec{Key: b, Ptr: &b, Tags: []*queryValuesNamedSliceValueIndexer{&b}}); err != nil {
		t.Fatalf("Set b: %v", err)
	}

	count, err := db.Count(qx.IN("key", []queryValuesNamedSliceValueIndexer{a}))
	if err != nil {
		t.Fatalf("IN named slice ValueIndexer: %v", err)
	}
	if count != 1 {
		t.Fatalf("IN named slice ValueIndexer count=%d want 1", count)
	}

	count, err = db.Count(qx.IN("ptr", []*queryValuesNamedSliceValueIndexer{&a}))
	if err != nil {
		t.Fatalf("IN pointer named slice ValueIndexer: %v", err)
	}
	if count != 1 {
		t.Fatalf("IN pointer named slice ValueIndexer count=%d want 1", count)
	}

	count, err = db.Count(qx.HASANY("tags", []*queryValuesNamedSliceValueIndexer{&a}))
	if err != nil {
		t.Fatalf("HASANY pointer named slice ValueIndexer: %v", err)
	}
	if count != 1 {
		t.Fatalf("HASANY pointer named slice ValueIndexer count=%d want 1", count)
	}

	got, err := db.QueryKeys(qx.Query().SortBy(qx.POS("key", []queryValuesNamedSliceValueIndexer{b, a}), qx.ASC))
	if err != nil {
		t.Fatalf("POS named slice ValueIndexer: %v", err)
	}
	if !slices.Equal(got, []uint64{2, 1}) {
		t.Fatalf("POS named slice ValueIndexer keys=%v want [2 1]", got)
	}

	got, err = db.QueryKeys(qx.Query().SortBy(qx.POS("ptr", []*queryValuesNamedSliceValueIndexer{&b, &a}), qx.ASC))
	if err != nil {
		t.Fatalf("POS pointer named slice ValueIndexer: %v", err)
	}
	if !slices.Equal(got, []uint64{2, 1}) {
		t.Fatalf("POS pointer named slice ValueIndexer keys=%v want [2 1]", got)
	}
}

func TestValueReceiverValueIndexerInterfaceQueryValues(t *testing.T) {
	db := newFixtureDB[uint64, queryValuesValueReceiverPtrRec](t, "", Options{})
	a := queryValuesValueReceiverString("AA")
	b := queryValuesValueReceiverString("BB")
	var nilKey *queryValuesValueReceiverString
	if err := db.Set(1, &queryValuesValueReceiverPtrRec{Key: nilKey, Tags: []*queryValuesValueReceiverString{nilKey, &a}}); err != nil {
		t.Fatalf("Set typed nil: %v", err)
	}
	if err := db.Set(2, &queryValuesValueReceiverPtrRec{Key: &a, Tags: []*queryValuesValueReceiverString{nilKey, &b}}); err != nil {
		t.Fatalf("Set value: %v", err)
	}

	count, err := db.Count(qx.EQ("key", nil))
	if err != nil {
		t.Fatalf("EQ nil: %v", err)
	}
	if count != 1 {
		t.Fatalf("EQ nil count=%d want 1", count)
	}

	count, err = db.Count(qx.IN("key", []schema.ValueIndexer{nilKey, &a}))
	if err != nil {
		t.Fatalf("IN interface values: %v", err)
	}
	if count != 2 {
		t.Fatalf("IN interface values count=%d want 2", count)
	}

	count, err = db.Count(qx.HASANY("tags", []schema.ValueIndexer{nilKey, &b}))
	if err != nil {
		t.Fatalf("HASANY interface values: %v", err)
	}
	if count != 1 {
		t.Fatalf("HASANY interface values count=%d want 1", count)
	}

	got, err := db.QueryKeys(qx.Query().SortBy(qx.POS("key", []schema.ValueIndexer{nilKey, &a}), qx.ASC))
	if err != nil {
		t.Fatalf("POS interface values priority: %v", err)
	}
	if !slices.Equal(got, []uint64{1, 2}) {
		t.Fatalf("POS interface values priority keys=%v want [1 2]", got)
	}

	got, err = db.QueryKeys(qx.Query().SortBy(qx.POS("key", []schema.ValueIndexer{&a}), qx.ASC))
	if err != nil {
		t.Fatalf("POS interface value priority with nil tail: %v", err)
	}
	if !slices.Equal(got, []uint64{2, 1}) {
		t.Fatalf("POS interface value priority with nil tail keys=%v want [2 1]", got)
	}

	got, err = db.QueryKeys(qx.Query().Sort("key", qx.ASC))
	if err != nil {
		t.Fatalf("Sort key: %v", err)
	}
	if !slices.Equal(got, []uint64{2, 1}) {
		t.Fatalf("Sort key keys=%v want [2 1]", got)
	}
}
