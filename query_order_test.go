package rbi

import (
	"fmt"
	"testing"

	"github.com/vapstack/qx"
)

func hasLenIndexKey(slice *[]index, key string) bool {
	if slice == nil {
		return false
	}
	for _, ix := range *slice {
		if indexKeyEqualsString(ix.Key, key) {
			return true
		}
	}
	return false
}

func collectIDsByTagDistinctLen(t *testing.T, db *DB[uint64, Rec], wantLen int) []uint64 {
	t.Helper()
	out := make([]uint64, 0, 64)
	err := db.ScanKeys(0, func(id uint64) (bool, error) {
		rec, err := db.Get(id)
		if err != nil {
			return false, err
		}
		if rec == nil {
			return true, nil
		}
		if distinctCount(rec.Tags) == wantLen {
			out = append(out, id)
		}
		db.ReleaseRecords(rec)
		return true, nil
	})
	if err != nil {
		t.Fatalf("collectIDsByTagDistinctLen scan: %v", err)
	}
	return out
}

func TestLenIndex_ZeroComplement_BaseQueryAndOrder(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	for i := 1; i <= 200; i++ {
		rec := &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("u_%d@example.test", i),
			Age:    i,
			Active: i%2 == 0,
		}
		switch {
		case i%5 == 0:
			rec.Tags = []string{"go", "db"}
		case i%7 == 0:
			rec.Tags = []string{"rust"}
		default:
			rec.Tags = nil
		}
		if err := db.Set(uint64(i), rec); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	if !db.isLenZeroComplementField("tags") {
		t.Fatalf("expected zero-complement mode for tags")
	}
	lenSlice := db.snapshotLenFieldIndexSlice("tags")
	if !hasLenIndexKey(lenSlice, lenIndexNonEmptyKey) {
		t.Fatalf("expected non-empty marker key in len index")
	}
	if hasLenIndexKey(lenSlice, uint64ByteStr(0)) {
		t.Fatalf("len=0 key should not be materialized in complement mode")
	}

	emptyQ := qx.Query(qx.EQ("tags", []string{}))
	gotEmpty, err := db.QueryKeys(emptyQ)
	if err != nil {
		t.Fatalf("QueryKeys(empty tags): %v", err)
	}
	wantEmpty := collectIDsByTagDistinctLen(t, db, 0)
	assertSameSlice(t, gotEmpty, wantEmpty)

	ascQ := qx.Query().ByArrayCount("tags", qx.ASC).Skip(3).Max(120)
	gotAsc, err := db.QueryKeys(ascQ)
	if err != nil {
		t.Fatalf("QueryKeys(ASC array count): %v", err)
	}
	wantAsc, err := expectedKeysUint64(t, db, ascQ)
	if err != nil {
		t.Fatalf("expectedKeysUint64(ASC array count): %v", err)
	}
	assertSameSlice(t, gotAsc, wantAsc)

	descQ := qx.Query().ByArrayCount("tags", qx.DESC).Skip(2).Max(100)
	gotDesc, err := db.QueryKeys(descQ)
	if err != nil {
		t.Fatalf("QueryKeys(DESC array count): %v", err)
	}
	wantDesc, err := expectedKeysUint64(t, db, descQ)
	if err != nil {
		t.Fatalf("expectedKeysUint64(DESC array count): %v", err)
	}
	assertSameSlice(t, gotDesc, wantDesc)
}

func TestLenIndex_ZeroComplement_WorksWithFieldDelta(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options{
		SnapshotCompactorRequestEveryNWrites: 1 << 30,
		SnapshotCompactorIdleInterval:        -1,
		SnapshotDeltaLayerMaxDepth:           1 << 30,
	})

	for i := 1; i <= 220; i++ {
		rec := &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("u_%d@example.test", i),
			Age:    i,
			Active: i%2 == 0,
		}
		switch {
		case i%6 == 0:
			rec.Tags = []string{"go", "db"}
		case i%11 == 0:
			rec.Tags = []string{"rust"}
		default:
			rec.Tags = nil
		}
		if err := db.Set(uint64(i), rec); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}
	if !db.isLenZeroComplementField("tags") {
		t.Fatalf("expected zero-complement mode for tags after rebuild")
	}

	for i := 1; i <= 40; i++ {
		var tags []string
		if i%3 == 0 {
			tags = []string{"go"}
		}
		if err := db.Patch(uint64(i), []Field{{Name: "tags", Value: tags}}); err != nil {
			t.Fatalf("Patch(%d): %v", i, err)
		}
	}

	s := db.getSnapshot()
	if s.lenFieldDelta("tags") == nil {
		t.Fatalf("expected len delta for tags")
	}

	emptyQ := qx.Query(qx.EQ("tags", []string{}))
	gotEmpty, err := db.QueryKeys(emptyQ)
	if err != nil {
		t.Fatalf("QueryKeys(empty tags): %v", err)
	}
	wantEmpty := collectIDsByTagDistinctLen(t, db, 0)
	assertSameSlice(t, gotEmpty, wantEmpty)

	ascQ := qx.Query(qx.EQ("active", true)).ByArrayCount("tags", qx.ASC).Skip(1).Max(110)
	gotAsc, err := db.QueryKeys(ascQ)
	if err != nil {
		t.Fatalf("QueryKeys(ASC array count overlay): %v", err)
	}
	wantAsc, err := expectedKeysUint64(t, db, ascQ)
	if err != nil {
		t.Fatalf("expectedKeysUint64(ASC array count overlay): %v", err)
	}
	assertSameSlice(t, gotAsc, wantAsc)

	descQ := qx.Query(qx.EQ("active", false)).ByArrayCount("tags", qx.DESC).Skip(2).Max(110)
	gotDesc, err := db.QueryKeys(descQ)
	if err != nil {
		t.Fatalf("QueryKeys(DESC array count overlay): %v", err)
	}
	wantDesc, err := expectedKeysUint64(t, db, descQ)
	if err != nil {
		t.Fatalf("expectedKeysUint64(DESC array count overlay): %v", err)
	}
	assertSameSlice(t, gotDesc, wantDesc)
}
func TestQuery_OrderBy_WithNegationAndLimit(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	_ = seedData(t, db, 200)

	// order + NOT branch
	// this forces the "negative + ordering" path to materialize (universe AND NOT set)
	q := qx.Query(
		qx.NOT(qx.EQ("country", "NL")),
	).By("age", qx.ASC).Skip(3).Max(25)

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

func TestQuery_Prefix_OrderBySameField_Limit(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	for i := 1; i <= 120; i++ {
		email := fmt.Sprintf("user%03d@example.com", i)
		if i%3 == 0 {
			email = fmt.Sprintf("user10%03d@example.com", i)
		}
		rec := &Rec{
			Meta:     Meta{Country: "NL"},
			Name:     "alice",
			Email:    email,
			Age:      18 + (i % 50),
			Score:    float64(i) / 10.0,
			Active:   i%2 == 0,
			Tags:     []string{"go"},
			FullName: fmt.Sprintf("FN-%03d", i),
		}
		if err := db.Set(uint64(i), rec); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	q := qx.Query(
		qx.PREFIX("email", "user10"),
	).By("email", qx.ASC).Max(15)

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

func TestQuery_ByArrayPos_WithLimitAndNegation(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	_ = seedData(t, db, 220)

	priority := []string{"go", "java", "ops"}
	q := qx.Query(
		qx.NOT(qx.CONTAINS("country", "land")),
	).ByArrayPos("tags", priority, qx.ASC).Skip(2).Max(30)

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

func TestQuery_ByArrayCount_WithLimitAndNegation(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	_ = seedData(t, db, 220)

	q := qx.Query(
		qx.NOT(qx.EQ("active", true)),
	).ByArrayCount("tags", qx.DESC).Skip(1).Max(40)

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

func TestQuery_SortWithNegativeResult_NoDuplicates(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	_ = seedData(t, db, 300)

	// negative result + ORDER triggers materialization path; ensure no duplicates in output
	q := qx.Query(
		qx.NOT(qx.EQ("country", "NL")),
	).By("age", qx.ASC).Max(120)

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}

	seen := make(map[uint64]struct{}, len(got))
	for _, id := range got {
		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate id in ordered result: %d", id)
		}
		seen[id] = struct{}{}
	}

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertSameSlice(t, got, want)
}
