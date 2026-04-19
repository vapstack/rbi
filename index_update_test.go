package rbi

import (
	"fmt"
	"slices"
	"testing"

	"github.com/vapstack/qx"
)

func TestSet_ReindexesAllSliceValues_OnReplace(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 420)

	old, err := db.Get(215)
	if err != nil {
		t.Fatalf("Get(before 215): %v", err)
	}

	rec := &Rec{
		Meta:     Meta{Country: "PL"},
		Name:     "alice",
		Email:    "alice-215@example.test",
		Age:      73,
		Score:    35.20025742868052,
		Active:   false,
		Tags:     []string{"rust", "db", "ops"},
		FullName: "FN-00215",
	}
	mods := db.getModifiedIndexedFields(old, rec)
	if !slices.Contains(mods, "tags") {
		t.Fatalf("expected tags in modified fields, got: %v", mods)
	}

	if err := db.Set(215, rec); err != nil {
		t.Fatalf("Set(215): %v", err)
	}

	has := db.fieldLookupPostingRetained("tags", "db").Contains(215)
	if !has {
		v, err := db.Get(215)
		if err != nil {
			t.Fatalf("Get(215): %v", err)
		}
		t.Fatalf("expected tags=db index to contain id=215 after Set, rec=%#v old=%#v mods=%v", v, old, mods)
	}
}

func TestQuery_DeleteUpdatesIndex_Correctness(t *testing.T) {
	db, _ := openTempDBUint64(t)

	// arrange deterministic values so the query result is known
	for i := 1; i <= 50; i++ {
		rec := &Rec{
			Meta:     Meta{Country: "NL"},
			Name:     "alice",
			Age:      30,
			Score:    1.0,
			Active:   true,
			Tags:     []string{"go"},
			FullName: fmt.Sprintf("FN-%02d", i),
		}
		if err := db.Set(uint64(i), rec); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	q := qx.Query(
		qx.EQ("country", "NL"),
		qx.EQ("name", "alice"),
		qx.EQ("active", true),
	)

	got0, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys baseline: %v", err)
	}
	if len(got0) != 50 {
		t.Fatalf("expected 50, got %d", len(got0))
	}

	// delete a subset and verify indexes reflect it
	for _, id := range []uint64{3, 7, 10, 25, 50} {
		if err := db.Delete(id); err != nil {
			t.Fatalf("Delete(%d): %v", id, err)
		}
	}

	got1, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys after delete: %v", err)
	}

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertSameSlice(t, got1, want)
}

func TestQuery_PatchUpdatesIndex_Correctness(t *testing.T) {
	db, _ := openTempDBUint64(t)

	// put some records with age=10, then patch some to age=40 and ensure range queries reflect changes
	for i := 1; i <= 60; i++ {
		rec := &Rec{
			Meta:     Meta{Country: "NL"},
			Name:     "u",
			Age:      10,
			Score:    0.1,
			Active:   true,
			Tags:     []string{"go"},
			FullName: fmt.Sprintf("FN-%02d", i),
		}
		if err := db.Set(uint64(i), rec); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	for _, id := range []uint64{2, 5, 9, 11, 17, 31} {
		if err := db.Patch(id, []Field{{Name: "age", Value: float64(40)}}); err != nil {
			t.Fatalf("Patch(%d): %v", id, err)
		}
	}

	q := qx.Query(qx.GTE("age", 35)).Sort("age", qx.ASC)

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertSameSlice(t, got, want)

	cnt, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if cnt != uint64(len(want)) {
		t.Fatalf("Count mismatch: got=%d want=%d", cnt, len(want))
	}
}
