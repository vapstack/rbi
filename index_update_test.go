package rbi

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
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

	has := db.engine.currentQueryViewForTests().snap.fieldLookupPostingRetained("tags", "db").Contains(215)
	if !has {
		v, err := db.Get(215)
		if err != nil {
			t.Fatalf("Get(215): %v", err)
		}
		t.Fatalf("expected tags=db index to contain id=215 after Set, rec=%#v old=%#v mods=%v", v, old, mods)
	}
}

func TestSet_ReindexesScalarString_OnReplace(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 420)

	old, err := db.Get(215)
	if err != nil {
		t.Fatalf("Get(before 215): %v", err)
	}
	oldFullName := old.FullName
	db.ReleaseRecords(old)

	rec := &Rec{
		Meta:     Meta{Country: "PL"},
		Name:     "alice",
		Email:    "alice-215@example.test",
		Age:      73,
		Score:    35.20025742868052,
		Active:   false,
		Tags:     []string{"rust", "db", "ops"},
		FullName: "FN-99999",
	}
	if err := db.Set(215, rec); err != nil {
		t.Fatalf("Set(215): %v", err)
	}

	snap := db.engine.currentQueryViewForTests().snap
	if snap.fieldLookupPostingRetained("full_name", oldFullName).Contains(215) {
		t.Fatalf("old full_name index still contains id=215")
	}
	if !snap.fieldLookupPostingRetained("full_name", rec.FullName).Contains(215) {
		t.Fatalf("new full_name index does not contain id=215")
	}
}

func TestBatchSet_RepeatedIDReindexesScalarString(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 420)

	old, err := db.Get(215)
	if err != nil {
		t.Fatalf("Get(before 215): %v", err)
	}
	oldFullName := old.FullName
	db.ReleaseRecords(old)

	first := &Rec{
		Meta:     Meta{Country: "PL"},
		Name:     "alice",
		Email:    "alice-215-a@example.test",
		Age:      73,
		Score:    35.20025742868052,
		Active:   false,
		Tags:     []string{"rust", "db", "ops"},
		FullName: "FN-88888",
	}
	second := *first
	second.Email = "alice-215-b@example.test"
	second.FullName = "FN-99999"
	if err := db.BatchSet([]uint64{215, 215}, []*Rec{first, &second}); err != nil {
		t.Fatalf("BatchSet repeated id: %v", err)
	}

	snap := db.engine.currentQueryViewForTests().snap
	if snap.fieldLookupPostingRetained("full_name", oldFullName).Contains(215) {
		t.Fatalf("old full_name index still contains id=215")
	}
	if snap.fieldLookupPostingRetained("full_name", first.FullName).Contains(215) {
		t.Fatalf("intermediate full_name index contains id=215")
	}
	if !snap.fieldLookupPostingRetained("full_name", second.FullName).Contains(215) {
		t.Fatalf("final full_name index does not contain id=215")
	}
}

func TestDelete_ReindexesScalarString(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 420)

	old, err := db.Get(215)
	if err != nil {
		t.Fatalf("Get(before 215): %v", err)
	}
	oldFullName := old.FullName
	db.ReleaseRecords(old)

	if err := db.Delete(215); err != nil {
		t.Fatalf("Delete(215): %v", err)
	}
	if db.engine.currentQueryViewForTests().snap.fieldLookupPostingRetained("full_name", oldFullName).Contains(215) {
		t.Fatalf("old full_name index still contains id=215 after delete")
	}
}

func TestSequentialSetChurnMaintainsScalarStringCardinality(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1, AutoBatchMax: 1})
	_ = seedData(t, db, 1_600)

	countries := []string{"NL", "PL", "DE", "Finland", "Iceland", "Thailand", "US"}
	tagPool := []string{"go", "db", "ops", "rust", "java", "infra"}
	names := []string{"alice", "albert", "bob", "carol", "dave", "eve"}
	r := newRand(20260304)
	for i := 0; i < 1_200; i++ {
		id := uint64(1 + r.IntN(24_000))
		name := names[r.IntN(len(names))]
		country := countries[r.IntN(len(countries))]
		age := 18 + r.IntN(65)
		score := float64(r.IntN(20_000))/10.0 + r.Float64()*0.001
		active := r.IntN(2) == 0
		tagA := tagPool[r.IntN(len(tagPool))]
		tagB := tagPool[r.IntN(len(tagPool))]
		fullName := fmt.Sprintf("FN-%05d", 1+r.IntN(30_000))
		if err := db.Set(id, &Rec{
			Meta:     Meta{Country: country},
			Name:     name,
			Email:    fmt.Sprintf("%s-%d@example.test", name, id),
			Age:      age,
			Score:    score,
			Active:   active,
			Tags:     []string{tagA, tagB},
			FullName: fullName,
		}); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
		if order := scalarStringOrderBreak(t, db, "full_name"); order != "" {
			t.Fatalf("iter=%d id=%d order=%s", i, id, order)
		}
	}

	stats := db.Stats()
	indexStats := db.IndexStats()
	if got := indexStats.FieldTotalCardinality["full_name"]; got != stats.KeyCount {
		t.Fatalf("full_name cardinality=%d key_count=%d duplicates=%s", got, stats.KeyCount, scalarStringDuplicateSample(t, db, "full_name"))
	}
}

func TestConcurrentSetPatchDeleteMaintainsScalarStringCardinality(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 1_600)

	countries := []string{"NL", "PL", "DE", "Finland", "Iceland", "Thailand", "US"}
	tagPool := []string{"go", "db", "ops", "rust", "java", "infra"}
	names := []string{"alice", "albert", "bob", "carol", "dave", "eve"}

	var firstErr atomic.Pointer[error]
	reportErr := func(err error) {
		if err == nil {
			return
		}
		firstErr.CompareAndSwap(nil, &err)
	}
	var wg sync.WaitGroup
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()

			r := newRand(seed)
			randomRec := func(id uint64) *Rec {
				name := names[r.IntN(len(names))]
				score := float64(r.IntN(20_000))/10.0 + r.Float64()*0.001
				if r.IntN(100) < 5 {
					score = 9_000 + float64(r.IntN(1_000)) + r.Float64()*0.001
				}
				return &Rec{
					Meta:     Meta{Country: countries[r.IntN(len(countries))]},
					Name:     name,
					Email:    fmt.Sprintf("%s-%d@example.test", name, id),
					Age:      18 + r.IntN(65),
					Score:    score,
					Active:   r.IntN(2) == 0,
					Tags:     []string{tagPool[r.IntN(len(tagPool))], tagPool[r.IntN(len(tagPool))]},
					FullName: fmt.Sprintf("FN-%05d", 1+r.IntN(30_000)),
				}
			}
			randomPatch := func() []Field {
				switch r.IntN(5) {
				case 0:
					return []Field{{Name: "score", Value: float64(r.IntN(20_000))/10.0 + r.Float64()*0.001}}
				case 1:
					return []Field{
						{Name: "score", Value: float64(r.IntN(20_000))/10.0 + r.Float64()*0.001},
						{Name: "active", Value: r.IntN(2) == 0},
					}
				case 2:
					return []Field{
						{Name: "score", Value: float64(r.IntN(20_000))/10.0 + r.Float64()*0.001},
						{Name: "country", Value: countries[r.IntN(len(countries))]},
					}
				case 3:
					return []Field{
						{Name: "score", Value: float64(r.IntN(20_000))/10.0 + r.Float64()*0.001},
						{Name: "tags", Value: []string{tagPool[r.IntN(len(tagPool))], tagPool[r.IntN(len(tagPool))]}},
					}
				default:
					return []Field{
						{Name: "score", Value: float64(r.IntN(20_000))/10.0 + r.Float64()*0.001},
						{Name: "age", Value: float64(18 + r.IntN(65))},
					}
				}
			}

			for i := 0; i < 800; i++ {
				id := uint64(1 + r.IntN(24_000))
				switch r.IntN(10) {
				case 0, 1, 2, 3:
					rec := randomRec(id)
					reportErr(db.Set(id, rec))
				case 4, 5, 6, 7, 8:
					reportErr(db.Patch(id, randomPatch()))
				default:
					reportErr(db.Delete(id))
				}
				if firstErr.Load() != nil {
					return
				}
			}
		}(int64(20260304 + w))
	}
	wg.Wait()
	if errPtr := firstErr.Load(); errPtr != nil {
		t.Fatalf("writer error: %v", *errPtr)
	}

	stats := db.Stats()
	indexStats := db.IndexStats()
	if got := indexStats.FieldTotalCardinality["full_name"]; got != stats.KeyCount {
		t.Fatalf("full_name cardinality=%d key_count=%d duplicates=%s", got, stats.KeyCount, scalarStringDuplicateSample(t, db, "full_name"))
	}
}

func scalarStringDuplicateSample(t testing.TB, db *DB[uint64, Rec], field string) string {
	t.Helper()

	view := db.engine.currentQueryViewForTests()

	acc := db.engine.indexedFieldMap[field]
	ov := view.fieldOverlayByOrdinal(acc.ordinal)
	cur := ov.NewCursor(ov.RangeByRanks(0, ov.KeyCount()), false)
	seen := make(map[uint64]string, db.Stats().KeyCount)
	out := ""
	for {
		key, ids, ok := cur.Next()
		if !ok {
			break
		}
		keyString := key.UnsafeString()
		it := ids.Iter()
		for it.HasNext() {
			id := it.Next()
			if prev, ok := seen[id]; ok && prev != keyString {
				rec, err := db.Get(id)
				if err != nil {
					out += fmt.Sprintf("%d:%s/%s get_err=%v;", id, prev, keyString, err)
				} else {
					out += fmt.Sprintf("%d:%s/%s actual=%s;", id, prev, keyString, rec.FullName)
					db.ReleaseRecords(rec)
				}
				if len(out) > 512 {
					it.Release()
					return out
				}
			} else {
				seen[id] = keyString
			}
		}
		it.Release()
	}
	return out
}

func scalarStringOrderBreak(t testing.TB, db *DB[uint64, Rec], field string) string {
	t.Helper()

	view := db.engine.currentQueryViewForTests()
	acc := db.engine.indexedFieldMap[field]
	ov := view.fieldOverlayByOrdinal(acc.ordinal)
	cur := ov.NewCursor(ov.RangeByRanks(0, ov.KeyCount()), false)
	prev := ""
	for {
		key, _, ok := cur.Next()
		if !ok {
			return ""
		}
		curKey := key.UnsafeString()
		if prev != "" && prev > curKey {
			return prev + ">" + curKey
		}
		prev = curKey
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
