package rbi

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/vapstack/qx"
)

func TestQuery_LimitNoOrder_UnsatisfiableLeafs_ReturnEmpty(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Email: "alice@example.com", Tags: []string{"go", "db"}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(2, &Rec{Name: "bob", Email: "bob@example.com", Tags: []string{"ops"}}); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		q    *qx.QX
	}{
		{
			name: "eq_missing",
			q:    qx.Query(qx.EQ("email", "missing@example.com")).Max(8),
		},
		{
			name: "in_all_missing",
			q:    qx.Query(qx.IN("email", []string{"x@example.com", "y@example.com"})).Max(8),
		},
		{
			name: "has_missing",
			q:    qx.Query(qx.HAS("tags", []string{"missing"})).Max(8),
		},
		{
			name: "hasany_all_missing",
			q:    qx.Query(qx.HASANY("tags", []string{"missing-1", "missing-2"})).Max(8),
		},
		{
			name: "and_hit_plus_missing",
			q: qx.Query(
				qx.EQ("name", "alice"),
				qx.EQ("email", "missing@example.com"),
			).Max(8),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ids, err := db.QueryKeys(tt.q)
			if err != nil {
				t.Fatalf("QueryKeys: %v", err)
			}
			if len(ids) != 0 {
				t.Fatalf("expected empty ids, got %v", ids)
			}

			items, err := db.Query(tt.q)
			if err != nil {
				t.Fatalf("Query: %v", err)
			}
			if len(items) != 0 {
				t.Fatalf("expected empty items, got len=%d", len(items))
			}
			db.ReleaseRecords(items...)
		})
	}
}

func TestQuery_LimitOrderAndRange_UnsatisfiableRest_ReturnEmpty(t *testing.T) {
	db, _ := openTempDBUint64(t)

	for i := 1; i <= 120; i++ {
		email := fmt.Sprintf("user-%d@example.com", i)
		if err := db.Set(uint64(i), &Rec{
			Name:  fmt.Sprintf("user-%d", i),
			Email: email,
			Age:   18 + i%50,
			Tags:  []string{"go", "db"},
		}); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}

	qOrder := qx.Query(
		qx.EQ("email", "missing@example.com"),
	).By("age", qx.ASC).Max(10)
	leaves, ok := extractAndLeaves(qOrder.Expr)
	if !ok || len(leaves) == 0 {
		t.Fatalf("extractAndLeaves(order): ok=%v len=%d", ok, len(leaves))
	}
	out, used, err := db.tryLimitQueryOrderBasic(qOrder, leaves)
	if err != nil {
		t.Fatalf("tryLimitQueryOrderBasic: %v", err)
	}
	if !used {
		t.Fatalf("expected tryLimitQueryOrderBasic to be used")
	}
	if len(out) != 0 {
		t.Fatalf("expected empty result from tryLimitQueryOrderBasic, got %v", out)
	}

	qRange := qx.Query(
		qx.GTE("age", 20),
		qx.LT("age", 40),
		qx.EQ("email", "missing@example.com"),
	).Max(10)
	f, bounds, rest, ok, err := db.extractNoOrderBoundsAndRest(mustExtractAndLeaves(t, qRange.Expr))
	if err != nil {
		t.Fatalf("extractNoOrderBoundsAndRest: %v", err)
	}
	if !ok {
		t.Fatalf("expected no-order range bounds to be recognized")
	}
	out, used, err = db.tryLimitQueryRangeNoOrderByField(qRange, f, bounds, rest)
	if err != nil {
		t.Fatalf("tryLimitQueryRangeNoOrderByField: %v", err)
	}
	if !used {
		t.Fatalf("expected tryLimitQueryRangeNoOrderByField to be used")
	}
	if len(out) != 0 {
		t.Fatalf("expected empty result from tryLimitQueryRangeNoOrderByField, got %v", out)
	}
}

func TestQuery_OffsetBeyondResult_ReturnsEmpty(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 80)

	q := qx.Query(qx.EQ("country", "NL")).Skip(10_000).Max(50)

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty slice, got %v", got)
	}

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertSameSlice(t, got, want)
}

func TestQuery_NoOrder_UnboundedLimit_RespectsOffset(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 140)

	q := qx.Query(qx.GTE("age", 18)).Skip(35)

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertSameSlice(t, got, want)

	if len(got) == 0 {
		t.Fatalf("expected non-empty paged result")
	}
}

func TestQuery_NoOrder_UnboundedLimit_RandomizedOffsetConsistency(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 260)

	r := newRand(20260327)
	countries := []string{"NL", "PL", "DE", "Finland", "Iceland", "Thailand", "Switzerland"}
	names := []string{"alice", "albert", "bob", "bobby", "carol", "dave", "eve"}
	tags := []string{"go", "db", "ops", "rust", "java"}

	randomLeaf := func() qx.Expr {
		switch r.IntN(10) {
		case 0:
			return qx.EQ("active", r.IntN(2) == 0)
		case 1:
			return qx.EQ("country", countries[r.IntN(len(countries))])
		case 2:
			return qx.NE("name", names[r.IntN(len(names))])
		case 3:
			return qx.GTE("age", 18+r.IntN(30))
		case 4:
			return qx.LTE("age", 20+r.IntN(45))
		case 5:
			return qx.GT("score", float64(r.IntN(90)))
		case 6:
			return qx.HASANY("tags", []string{
				tags[r.IntN(len(tags))],
				tags[r.IntN(len(tags))],
			})
		case 7:
			return qx.IN("country", []string{
				countries[r.IntN(len(countries))],
				countries[r.IntN(len(countries))],
			})
		case 8:
			return qx.PREFIX("full_name", fmt.Sprintf("FN-%d", r.IntN(3)))
		default:
			return qx.CONTAINS("name", "a")
		}
	}

	randomExpr := func() qx.Expr {
		a := randomLeaf()
		switch r.IntN(4) {
		case 0:
			return a
		case 1:
			return qx.AND(a, randomLeaf())
		case 2:
			return qx.OR(a, randomLeaf())
		default:
			return qx.NOT(a)
		}
	}

	for step := 0; step < 220; step++ {
		q := &qx.QX{
			Expr:   randomExpr(),
			Offset: uint64(r.IntN(180)),
			Limit:  0, // unbounded
		}

		gotKeys, err := db.QueryKeys(q)
		if err != nil {
			t.Fatalf("step=%d QueryKeys(%+v): %v", step, q, err)
		}
		wantKeys, err := expectedKeysUint64(t, db, q)
		if err != nil {
			t.Fatalf("step=%d expectedKeys(%+v): %v", step, q, err)
		}
		assertSameSlice(t, gotKeys, wantKeys)

		gotItems, err := db.Query(q)
		if err != nil {
			t.Fatalf("step=%d Query(%+v): %v", step, q, err)
		}
		wantItems, err := db.BatchGet(wantKeys...)
		if err != nil {
			t.Fatalf("step=%d BatchGet(wantKeys): %v", step, err)
		}
		if len(gotItems) != len(wantItems) {
			t.Fatalf("step=%d items len mismatch: got=%d want=%d q=%+v", step, len(gotItems), len(wantItems), q)
		}
		for i := range wantItems {
			if gotItems[i] == nil || wantItems[i] == nil {
				t.Fatalf("step=%d nil item mismatch at i=%d got=%#v want=%#v q=%+v", step, i, gotItems[i], wantItems[i], q)
			}
			if !reflect.DeepEqual(*gotItems[i], *wantItems[i]) {
				t.Fatalf("step=%d item mismatch at i=%d got=%#v want=%#v q=%+v", step, i, gotItems[i], wantItems[i], q)
			}
		}
	}
}
