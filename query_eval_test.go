package rbi

import (
	"fmt"
	"testing"

	"github.com/vapstack/qx"
)

func TestQueryUnknownFieldReturnsError(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	_ = seedData(t, db, 10)

	_, err := db.QueryKeys(qx.Query(qx.EQ("no_such_field", 1)))
	if err == nil {
		t.Fatalf("expected error for unknown field")
	}
}

func TestEmptySliceQueries(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	if err := db.Set(1, &Rec{Tags: []string{"go"}}); err != nil {
		t.Fatal(err)
	}

	_, err := db.QueryKeys(qx.Query(qx.HASANY("tags", []string{})))
	if err == nil {
		t.Fatal("HASANY with empty slice: error expected, got nil")
	}

	_, err = db.QueryKeys(qx.Query(qx.HAS("tags", []string{})))
	if err == nil {
		t.Fatal("HAS with empty slice: error expected, got nil")
	}
}

func TestQuery_PointerField_NilVsZeroValue(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	sEmpty := ""
	sVal := "val"

	if err := db.Set(1, &Rec{Name: "nil_opt", Opt: nil}); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(2, &Rec{Name: "empty_opt", Opt: &sEmpty}); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(3, &Rec{Name: "val_opt", Opt: &sVal}); err != nil {
		t.Fatal(err)
	}

	ids, err := db.QueryKeys(qx.Query(qx.EQ("opt", nil)))
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != 1 {
		t.Errorf("Query NIL: expected [1], got %v", ids)
	}

	// find empty string (value should be "" string, not pointer)
	ids, err = db.QueryKeys(qx.Query(qx.EQ("opt", "")))
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != 2 {
		t.Errorf("expected [2], got %v", ids)
	}
}

func TestQueryPrefix_MatchingSemantics(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	names := []string{"item", "item-1", "item-10", "items", "iterator"}
	for i, n := range names {
		if err := db.Set(uint64(i), &Rec{Name: n}); err != nil {
			t.Fatal(err)
		}
	}

	q := qx.Query(qx.PREFIX("name", "item"))
	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 4 {
		t.Errorf("PREFIX 'item': expected 4, got %d", len(ids))
	}

	q = qx.Query(qx.PREFIX("name", "iter"))
	ids, err = db.QueryKeys(q)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 {
		t.Errorf("PREFIX 'iter': expected 1, got %d", len(ids))
	}
}

func TestQuery_OR_WithNegativeBranch_EqualsUniverse(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	ids := seedData(t, db, 120)

	// OR( NOT EQ(name,"alice"), EQ(name,"alice") ) == universe
	q := qx.Query(
		qx.OR(
			qx.NE("name", "alice"),
			qx.EQ("name", "alice"),
		),
	)

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}

	assertSameSlice(t, got, want)

	if uint64(len(got)) != uint64(len(ids)) {
		t.Fatalf("expected universe size %d, got %d", len(ids), len(got))
	}
}

func TestQuery_AND_WithNegativeBranch_Empty(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	_ = seedData(t, db, 120)

	// AND( EQ(name,"alice"), NOT EQ(name,"alice") ) == empty
	q := qx.Query(
		qx.AND(
			qx.EQ("name", "alice"),
			qx.NE("name", "alice"),
		),
	)

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty result, got %v", got)
	}
}

func TestQuery_DoubleNot_SameAsOriginal(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	_ = seedData(t, db, 150)

	inner := qx.AND(
		qx.GTE("age", 25),
		qx.EQ("active", true),
		qx.HASANY("tags", []string{"go", "java"}),
	)

	q := qx.Query(qx.NOT(qx.NOT(inner)))

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertSameSlice(t, got, want)
}

func TestQuery_RangeBoundaries_Int_Correctness(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

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
		if err := db.Set(uint64(i+1), rec); err != nil {
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
			got, err := db.QueryKeys(tc.q)
			if err != nil {
				t.Fatalf("QueryKeys: %v", err)
			}
			want, err := expectedKeysUint64(t, db, tc.q)
			if err != nil {
				t.Fatalf("expectedKeysUint64: %v", err)
			}
			assertSameSlice(t, got, want)
		})
	}
}

func TestQuery_IN_WithDuplicates_DoesNotDuplicateResults(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	_ = seedData(t, db, 160)

	// duplicate values in IN should not cause duplicated ids
	q := qx.Query(qx.IN("country", []string{"NL", "NL", "DE", "DE"})).By("age", qx.ASC).Max(50)

	got, err := db.QueryKeys(q)
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

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertSameSlice(t, got, want)
}

func TestQuery_SliceField_HASANY_WithDuplicateNeedles(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	// force duplicates in both data and needles
	if err := db.Set(1, &Rec{Tags: []string{"go", "go", "db"}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(2, &Rec{Tags: []string{"rust"}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(3, &Rec{Tags: []string{}}); err != nil {
		t.Fatal(err)
	}

	q := qx.Query(qx.HASANY("tags", []string{"go", "go"}))

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}

	assertSameSlice(t, got, want)
}

func TestQuery_SliceField_HAS_DuplicateNeedles_MatchesAccordingToHarness(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	// this test locks in the current reference semantics:
	// containsAll() in harness treats duplicates as requiring multiple occurrences,
	// index implementation may choose a different semantics; this test will catch drift

	if err := db.Set(1, &Rec{Tags: []string{"go", "db"}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(2, &Rec{Tags: []string{"go", "go", "db"}}); err != nil {
		t.Fatal(err)
	}

	q := qx.Query(qx.HAS("tags", []string{"go", "go"}))

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}

	assertSameSlice(t, got, want)
}
