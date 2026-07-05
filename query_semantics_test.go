package rbi

import (
	"fmt"
	"strings"
	"testing"

	"github.com/vapstack/qx"
)

func TestQueryUnknownFieldReturnsError(t *testing.T) {
	c, _ := openTempUint64Collection(t)
	_ = seedData(t, c, 10)

	_, err := readQueryKeys(c, qx.Query(qx.EQ("no_such_field", 1)))
	if err == nil {
		t.Fatalf("expected error for unknown field")
	}
}

func TestEmptySliceQueries(t *testing.T) {
	c, _ := openTempUint64Collection(t)
	if err := writeSet(c, 1, &Rec{Tags: []string{"go"}}); err != nil {
		t.Fatal(err)
	}

	_, err := readQueryKeys(c, qx.Query(qx.HASANY("tags", []string{})))
	if err == nil {
		t.Fatal("HASANY with empty slice: error expected, got nil")
	}

	_, err = readQueryKeys(c, qx.Query(qx.HASALL("tags", []string{})))
	if err == nil {
		t.Fatal("HAS with empty slice: error expected, got nil")
	}
}

func TestQuery_SliceEQ_EmptyDBAndAfterLastDelete(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	q := qx.Query(qx.EQ("tags", []string{}))

	got, err := readQueryKeys(c, q)
	if err != nil {
		t.Fatalf("QueryKeys(empty db): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty result on empty db, got %v", got)
	}
	cnt, err := readCount(c, q.Filter)
	if err != nil {
		t.Fatalf("Count(empty db): %v", err)
	}
	if cnt != 0 {
		t.Fatalf("expected zero count on empty db, got %d", cnt)
	}

	if err := writeSet(c, 1, &Rec{Name: "u1", Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := writeDelete(c, 1); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	got, err = readQueryKeys(c, q)
	if err != nil {
		t.Fatalf("QueryKeys(after last delete): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty result after last delete, got %v", got)
	}
	cnt, err = readCount(c, q.Filter)
	if err != nil {
		t.Fatalf("Count(after last delete): %v", err)
	}
	if cnt != 0 {
		t.Fatalf("expected zero count after last delete, got %d", cnt)
	}
}

func TestQuery_PointerField_NilVsZeroValue(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	sEmpty := ""
	sVal := "val"

	if err := writeSet(c, 1, &Rec{Name: "nil_opt", Opt: nil}); err != nil {
		t.Fatal(err)
	}
	if err := writeSet(c, 2, &Rec{Name: "empty_opt", Opt: &sEmpty}); err != nil {
		t.Fatal(err)
	}
	if err := writeSet(c, 3, &Rec{Name: "val_opt", Opt: &sVal}); err != nil {
		t.Fatal(err)
	}

	ids, err := readQueryKeys(c, qx.Query(qx.EQ("opt", nil)))
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != 1 {
		t.Errorf("Query NIL: expected [1], got %v", ids)
	}

	// find empty string (value should be "" string, not pointer)
	ids, err = readQueryKeys(c, qx.Query(qx.EQ("opt", "")))
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != 2 {
		t.Errorf("expected [2], got %v", ids)
	}
}

func TestQuery_PointerField_ISNULLAndNOTNULLAliases(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	sEmpty := ""
	sVal := "val"

	if err := writeSet(c, 1, &Rec{Name: "nil_opt", Opt: nil}); err != nil {
		t.Fatal(err)
	}
	if err := writeSet(c, 2, &Rec{Name: "empty_opt", Opt: &sEmpty}); err != nil {
		t.Fatal(err)
	}
	if err := writeSet(c, 3, &Rec{Name: "val_opt", Opt: &sVal}); err != nil {
		t.Fatal(err)
	}

	gotEqNil, err := readQueryKeys(c, qx.Query(qx.EQ("opt", nil)))
	if err != nil {
		t.Fatalf("QueryKeys(EQ nil): %v", err)
	}
	gotIsNull, err := readQueryKeys(c, qx.Query(qx.ISNULL("opt")))
	if err != nil {
		t.Fatalf("QueryKeys(ISNULL): %v", err)
	}
	assertSameSlice(t, gotIsNull, gotEqNil)

	gotNeNil, err := readQueryKeys(c, qx.Query(qx.NE("opt", nil)))
	if err != nil {
		t.Fatalf("QueryKeys(NE nil): %v", err)
	}
	gotNotNull, err := readQueryKeys(c, qx.Query(qx.NOTNULL("opt")))
	if err != nil {
		t.Fatalf("QueryKeys(NOTNULL): %v", err)
	}
	assertSameSet(t, gotNotNull, gotNeNil)

	cntEqNil, err := readCount(c, qx.Query(qx.EQ("opt", nil)).Filter)
	if err != nil {
		t.Fatalf("Count(EQ nil): %v", err)
	}
	cntIsNull, err := readCount(c, qx.Query(qx.ISNULL("opt")).Filter)
	if err != nil {
		t.Fatalf("Count(ISNULL): %v", err)
	}
	if cntIsNull != cntEqNil {
		t.Fatalf("Count(ISNULL): got=%d want=%d", cntIsNull, cntEqNil)
	}

	cntNeNil, err := readCount(c, qx.Query(qx.NE("opt", nil)).Filter)
	if err != nil {
		t.Fatalf("Count(NE nil): %v", err)
	}
	cntNotNull, err := readCount(c, qx.Query(qx.NOTNULL("opt")).Filter)
	if err != nil {
		t.Fatalf("Count(NOTNULL): %v", err)
	}
	if cntNotNull != cntNeNil {
		t.Fatalf("Count(NOTNULL): got=%d want=%d", cntNotNull, cntNeNil)
	}
}

func TestQuery_Iterator_KeepsEmptyStringKey(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	if err := writeSet(c, 1, &Rec{Name: "", Email: "empty@example.test"}); err != nil {
		t.Fatal(err)
	}
	if err := writeSet(c, 2, &Rec{Name: "a", Email: "a@example.test"}); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name  string
		q     *qx.QX
		want  []uint64
		count uint64
	}{
		{
			name:  "prefix-empty",
			q:     qx.Query(qx.PREFIX("name", "")),
			want:  []uint64{1, 2},
			count: 2,
		},
		{
			name:  "suffix-empty",
			q:     qx.Query(qx.SUFFIX("name", "")),
			want:  []uint64{1, 2},
			count: 2,
		},
		{
			name:  "contains-empty",
			q:     qx.Query(qx.CONTAINS("name", "")),
			want:  []uint64{1, 2},
			count: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := readQueryKeys(c, tc.q)
			if err != nil {
				t.Fatalf("QueryKeys(%s): %v", tc.name, err)
			}
			assertSameSlice(t, got, tc.want)

			cnt, err := readCount(c, tc.q.Filter)
			if err != nil {
				t.Fatalf("Count(%s): %v", tc.name, err)
			}
			if cnt != tc.count {
				t.Fatalf("Count(%s): got=%d want=%d", tc.name, cnt, tc.count)
			}
		})
	}
}

func TestQuery_INNilMatchesEmptyListSemantics(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	if err := writeSet(c, 1, &Rec{Name: "alice"}); err != nil {
		t.Fatal(err)
	}

	var nilStrings []string

	tests := []struct {
		name string
		q    *qx.QX
	}{
		{name: "nil", q: qx.Query(qx.IN("name", []string(nil)))},
		{name: "empty-slice", q: qx.Query(qx.IN("name", []string{}))},
		{name: "nil-slice", q: qx.Query(qx.IN("name", nilStrings))},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := readQueryKeys(c, tc.q)
			if err == nil || !strings.Contains(err.Error(), "no values provided") {
				t.Fatalf("QueryKeys expected no-values error, got: %v", err)
			}

			_, err = readCount(c, tc.q.Filter)
			if err == nil || !strings.Contains(err.Error(), "no values provided") {
				t.Fatalf("Count expected no-values error, got: %v", err)
			}
		})
	}

	got, err := readQueryKeys(c, qx.Query(qx.OP(qx.OpIN, qx.REF("name"), qx.LIT([]any{"alice", nil}))))
	if err != nil {
		t.Fatalf("QueryKeys(mixed IN): %v", err)
	}
	assertSameSlice(t, got, []uint64{1})
}

func TestQueryPrefix_MatchingSemantics(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	names := []string{"item", "item-1", "item-10", "items", "iterator"}
	for i, n := range names {
		if err := writeSet(c, uint64(i), &Rec{Name: n}); err != nil {
			t.Fatal(err)
		}
	}

	q := qx.Query(qx.PREFIX("name", "item"))
	ids, err := readQueryKeys(c, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 4 {
		t.Errorf("PREFIX 'item': expected 4, got %d", len(ids))
	}

	q = qx.Query(qx.PREFIX("name", "iter"))
	ids, err = readQueryKeys(c, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 {
		t.Errorf("PREFIX 'iter': expected 1, got %d", len(ids))
	}
}

func TestQuery_OR_WithNegativeBranch_EqualsUniverse(t *testing.T) {
	c, _ := openTempUint64Collection(t)
	ids := seedData(t, c, 120)

	// OR( NOT EQ(name,"alice"), EQ(name,"alice") ) == universe
	q := qx.Query(
		qx.OR(
			qx.NE("name", "alice"),
			qx.EQ("name", "alice"),
		),
	)

	got, err := readQueryKeys(c, q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}

	want, err := expectedKeysUint64(t, c, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}

	assertQueryIDsEqual(t, q, got, want)

	if uint64(len(got)) != uint64(len(ids)) {
		t.Fatalf("expected universe size %d, got %d", len(ids), len(got))
	}
}

func TestQuery_AND_WithNegativeBranch_Empty(t *testing.T) {
	c, _ := openTempUint64Collection(t)
	_ = seedData(t, c, 120)

	// AND( EQ(name,"alice"), NOT EQ(name,"alice") ) == empty
	q := qx.Query(
		qx.AND(
			qx.EQ("name", "alice"),
			qx.NE("name", "alice"),
		),
	)

	got, err := readQueryKeys(c, q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty result, got %v", got)
	}
}

func TestQuery_DoubleNot_SameAsOriginal(t *testing.T) {
	c, _ := openTempUint64Collection(t)
	_ = seedData(t, c, 150)

	inner := qx.AND(
		qx.GTE("age", 25),
		qx.EQ("active", true),
		qx.HASANY("tags", []string{"go", "java"}),
	)

	q := qx.Query(qx.NOT(qx.NOT(inner)))

	got, err := readQueryKeys(c, q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	want, err := expectedKeysUint64(t, c, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertQueryIDsEqual(t, q, got, want)
}

func TestQuery_RangeBoundaries_Int_Correctness(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	// deterministic ages so boundary conditions are obvious
	for i := 0; i < 100; i++ {
		rec := &Rec{
			Meta:     Meta{Country: "NL"},
			Name:     "n",
			Age:      i, // 0..99
			Score:    float64(i),
			Active:   true,
			Tags:     []string{},
			FullName: fmt.Sprintf("FN-%02d", i),
		}
		if err := writeSet(c, uint64(i+1), rec); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	tests := []struct {
		name string
		q    *qx.QX
	}{
		{"GT_50", qx.Query(qx.GT("age", 50))},
		{"GTE_50", qx.Query(qx.GTE("age", 50))},
		{"LT_50", qx.Query(qx.LT("age", 50))},
		{"LTE_50", qx.Query(qx.LTE("age", 50))},
		{"AND_GTE_10_LT_20", qx.Query(qx.GTE("age", 10), qx.LT("age", 20))},
		{"AND_GTE_10_LTE_20", qx.Query(qx.GTE("age", 10), qx.LTE("age", 20))},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := readQueryKeys(c, tc.q)
			if err != nil {
				t.Fatalf("QueryKeys: %v", err)
			}
			want, err := expectedKeysUint64(t, c, tc.q)
			if err != nil {
				t.Fatalf("expectedKeysUint64: %v", err)
			}
			assertQueryIDsEqual(t, tc.q, got, want)
		})
	}
}

func TestQuery_IN_WithDuplicates_DoesNotDuplicateResults(t *testing.T) {
	c, _ := openTempUint64Collection(t)
	_ = seedData(t, c, 160)

	// duplicate values in IN should not cause duplicated ids
	q := qx.Query(qx.IN("country", []string{"NL", "NL", "DE", "DE"})).Sort("age", qx.ASC).Limit(50)

	got, err := readQueryKeys(c, q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	// ensure no duplicates in output slice (ordering + limit still applies)
	seen := make(map[uint64]struct{}, len(got))
	for _, id := range got {
		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate id in result: %d (got=%v)", id, got)
		}
		seen[id] = struct{}{}
	}

	want, err := expectedKeysUint64(t, c, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertQueryIDsEqual(t, q, got, want)
}

func TestQuery_SliceField_HASANY_WithDuplicateNeedles(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	// force duplicates in both data and needles
	if err := writeSet(c, 1, &Rec{Tags: []string{"go", "go", "db"}}); err != nil {
		t.Fatal(err)
	}
	if err := writeSet(c, 2, &Rec{Tags: []string{"rust"}}); err != nil {
		t.Fatal(err)
	}
	if err := writeSet(c, 3, &Rec{Tags: []string{}}); err != nil {
		t.Fatal(err)
	}

	q := qx.Query(qx.HASANY("tags", []string{"go", "go"}))

	got, err := readQueryKeys(c, q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}

	want, err := expectedKeysUint64(t, c, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}

	assertQueryIDsEqual(t, q, got, want)
}

func TestQuery_SliceField_HAS_DuplicateNeedles_MatchesAccordingToHarness(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	// this test locks in the current reference semantics:
	// containsAll() in harness treats duplicates as requiring multiple occurrences,
	// index implementation may choose a different semantics; this test will catch drift

	if err := writeSet(c, 1, &Rec{Tags: []string{"go", "db"}}); err != nil {
		t.Fatal(err)
	}
	if err := writeSet(c, 2, &Rec{Tags: []string{"go", "go", "db"}}); err != nil {
		t.Fatal(err)
	}

	q := qx.Query(qx.HASALL("tags", []string{"go", "go"}))

	got, err := readQueryKeys(c, q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}

	want, err := expectedKeysUint64(t, c, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}

	assertQueryIDsEqual(t, q, got, want)
}
