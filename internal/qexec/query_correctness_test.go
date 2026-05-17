package qexec

import (
	"slices"
	"strings"
	"testing"

	"github.com/vapstack/qx"
)

type correctnessRowLess func(aID uint64, a *Rec, bID uint64, b *Rec) bool
type correctnessRowMatch func(id uint64, r *Rec) bool

func newCorrectnessDB(t *testing.T, options ...Options) *DB[uint64, Rec] {
	t.Helper()

	db, _ := openTempDBUint64(t, options...)
	optA, optB, optC, optD := "aa", "ab", "ac", "ad"
	optE, optF, optG := "ae", "af", "ag"

	ids := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	vals := []*Rec{
		{Meta: Meta{Country: "US"}, Name: "n01", Email: "n01@test", Age: 25, Score: 10, Active: true, Tags: []string{"go", "db"}, FullName: "FN-01"},
		{Meta: Meta{Country: "DE"}, Name: "n02", Email: "n02@test", Age: 35, Score: 30, Active: false, Tags: []string{"tag_mid", "go"}, FullName: "FN-02", Opt: &optA},
		{Meta: Meta{Country: "PL"}, Name: "n03", Email: "n03@test", Age: 45, Score: 20, Active: true, Tags: []string{"tag_broad"}, FullName: "FN-03"},
		{Meta: Meta{Country: "US"}, Name: "n04", Email: "n04@test", Age: 55, Score: 40, Active: true, Tags: []string{"ops"}, FullName: "FN-04", Opt: &optB},
		{Meta: Meta{Country: "JP"}, Name: "n05", Email: "n05@test", Age: 65, Score: 50, Active: false, Tags: []string{"tag_mid"}, FullName: "FN-05", Opt: &optC},
		{Meta: Meta{Country: "NL"}, Name: "n06", Email: "n06@test", Age: 28, Score: 35, Active: true, Tags: []string{"go"}, FullName: "FN-06"},
		{Meta: Meta{Country: "US"}, Name: "n07", Email: "n07@test", Age: 38, Score: 25, Active: false, Tags: []string{"db"}, FullName: "FN-07", Opt: &optD},
		{Meta: Meta{Country: "CA"}, Name: "n08", Email: "n08@test", Age: 48, Score: 45, Active: true, Tags: []string{"tag_mid", "db"}, FullName: "FN-08"},
		{Meta: Meta{Country: "BR"}, Name: "n09", Email: "n09@test", Age: 58, Score: 15, Active: true, FullName: "FN-09", Opt: &optE},
		{Meta: Meta{Country: "US"}, Name: "n10", Email: "n10@test", Age: 32, Score: 55, Active: true, Tags: []string{"go", "ops"}, FullName: "FN-10", Opt: &optF},
		{Meta: Meta{Country: "DE"}, Name: "n11", Email: "n11@test", Age: 42, Score: 5, Active: true, Tags: []string{"tag_tiny"}, FullName: "FN-11"},
		{Meta: Meta{Country: "IN"}, Name: "n12", Email: "n12@test", Age: 52, Score: 60, Active: false, Tags: []string{"java"}, FullName: "FN-12", Opt: &optG},
	}
	if err := db.BatchSet(ids, vals); err != nil {
		t.Fatalf("BatchSet: %v", err)
	}
	return db
}

func expectedRecIDs(db *DB[uint64, Rec], match correctnessRowMatch, less correctnessRowLess, offset, limit int) []uint64 {
	ids := make([]uint64, 0, len(db.values))
	for id, rec := range db.values {
		if match(id, rec) {
			ids = append(ids, id)
		}
	}
	if less == nil {
		slices.Sort(ids)
	} else {
		slices.SortFunc(ids, func(a, b uint64) int {
			ra, rb := db.values[a], db.values[b]
			if less(a, ra, b, rb) {
				return -1
			}
			if less(b, rb, a, ra) {
				return 1
			}
			if a < b {
				return -1
			}
			if a > b {
				return 1
			}
			return 0
		})
	}
	if offset > len(ids) {
		return nil
	}
	ids = ids[offset:]
	if limit > 0 && limit < len(ids) {
		ids = ids[:limit]
	}
	return ids
}

func assertCorrectQuery(t *testing.T, db *DB[uint64, Rec], q *qx.QX, want []uint64) {
	t.Helper()
	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	assertQueryIDsEqual(t, q, got, want)
}

func countExpected(db *DB[uint64, Rec], match correctnessRowMatch) uint64 {
	var n uint64
	for id, rec := range db.values {
		if match(id, rec) {
			n++
		}
	}
	return n
}

func hasAnyString(vals []string, needles ...string) bool {
	for i := range vals {
		for j := range needles {
			if vals[i] == needles[j] {
				return true
			}
		}
	}
	return false
}

func hasNoneString(vals []string, needles ...string) bool {
	return !hasAnyString(vals, needles...)
}

func countryNotIn(r *Rec, vals ...string) bool {
	for i := range vals {
		if r.Country == vals[i] {
			return false
		}
	}
	return true
}

func scoreDesc(aID uint64, a *Rec, bID uint64, b *Rec) bool {
	if a.Score == b.Score {
		return aID < bID
	}
	return a.Score > b.Score
}

func scoreAsc(aID uint64, a *Rec, bID uint64, b *Rec) bool {
	if a.Score == b.Score {
		return aID < bID
	}
	return a.Score < b.Score
}

func tagPosAsc(priority []string) correctnessRowLess {
	return func(aID uint64, a *Rec, bID uint64, b *Rec) bool {
		ar, br := tagRank(a.Tags, priority), tagRank(b.Tags, priority)
		if ar == br {
			return aID < bID
		}
		return ar < br
	}
}

func tagRank(tags []string, priority []string) int {
	for i := range priority {
		for j := range tags {
			if tags[j] == priority[i] {
				return i
			}
		}
	}
	return len(priority)
}

func TestQueryCorrectness_NegativeNilComplementAndCount(t *testing.T) {
	db := newCorrectnessDB(t)

	activeAge := func(_ uint64, r *Rec) bool {
		return r.Active && r.Age >= 20 && r.Age < 50
	}
	q := qx.Query(
		qx.NOT(qx.EQ("active", false)),
		qx.GTE("age", 20),
		qx.LT("age", 50),
	).Sort("score", qx.DESC).Limit(3)
	assertCorrectQuery(t, db, q, expectedRecIDs(db, activeAge, scoreDesc, 0, 3))

	notInRange := func(_ uint64, r *Rec) bool {
		return countryNotIn(r, "DE", "PL") && r.Age >= 20 && r.Age < 60
	}
	q = qx.Query(
		qx.NOTIN("country", []string{"DE", "PL"}),
		qx.GTE("age", 20),
		qx.LT("age", 60),
	).Limit(32)
	assertCorrectQuery(t, db, q, expectedRecIDs(db, notInRange, nil, 0, 32))

	hasNoneRange := func(_ uint64, r *Rec) bool {
		return hasNoneString(r.Tags, "go", "db") && r.Age >= 20 && r.Age < 60
	}
	q = qx.Query(
		qx.HASNONE("tags", []string{"go", "db"}),
		qx.GTE("age", 20),
		qx.LT("age", 60),
	).Limit(32)
	assertCorrectQuery(t, db, q, expectedRecIDs(db, hasNoneRange, nil, 0, 32))

	nilOpt := func(_ uint64, r *Rec) bool {
		return r.Opt == nil
	}
	q = qx.Query(qx.EQ("opt", nil)).Sort("score", qx.ASC)
	assertCorrectQuery(t, db, q, expectedRecIDs(db, nilOpt, scoreAsc, 0, 0))

	countCases := []struct {
		name  string
		exprs []qx.Expr
		match correctnessRowMatch
	}{
		{name: "not_nil", exprs: []qx.Expr{qx.NOT(qx.EQ("opt", nil))}, match: func(_ uint64, r *Rec) bool { return r.Opt != nil }},
		{name: "nil", exprs: []qx.Expr{qx.EQ("opt", nil)}, match: nilOpt},
		{name: "notin_range", exprs: []qx.Expr{qx.NOTIN("country", []string{"DE", "PL"}), qx.GTE("age", 20), qx.LT("age", 60)}, match: notInRange},
		{name: "hasnone_range", exprs: []qx.Expr{qx.HASNONE("tags", []string{"go", "db"}), qx.GTE("age", 20), qx.LT("age", 60)}, match: hasNoneRange},
	}
	for _, c := range countCases {
		got, err := db.Count(c.exprs...)
		if err != nil {
			t.Fatalf("Count(%s): %v", c.name, err)
		}
		if want := countExpected(db, c.match); got != want {
			t.Fatalf("Count(%s) mismatch: got=%d want=%d", c.name, got, want)
		}
	}
}

func TestQueryCorrectness_WindowArrayOrderAndPreparedQuery(t *testing.T) {
	db := newCorrectnessDB(t)

	wideAge := func(_ uint64, r *Rec) bool {
		return r.Age >= 25 && r.Age < 60
	}
	q := qx.Query(qx.GTE("age", 25), qx.LT("age", 60)).Sort("score", qx.DESC).Offset(2).Limit(5)
	assertCorrectQuery(t, db, q, expectedRecIDs(db, wideAge, scoreDesc, 2, 5))

	prefixActive := func(_ uint64, r *Rec) bool {
		return r.Active && strings.HasPrefix(r.FullName, "FN-0")
	}
	q = qx.Query(qx.PREFIX("full_name", "FN-0"), qx.EQ("active", true)).Limit(32)
	assertCorrectQuery(t, db, q, expectedRecIDs(db, prefixActive, nil, 0, 32))

	priority := []string{"tag_mid", "go", "db"}
	hasPriorityTag := func(_ uint64, r *Rec) bool {
		return hasAnyString(r.Tags, "tag_mid", "go", "db")
	}
	q = qx.Query(qx.HASANY("tags", priority)).SortBy(qx.POS("tags", priority), qx.ASC).Offset(1).Limit(5)
	assertCorrectQuery(t, db, q, expectedRecIDs(db, hasPriorityTag, tagPosAsc(priority), 1, 5))

	prepared, shape, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer prepared.Release()
	got, err := db.engine.currentQueryViewForTests().PreparedQuery(&shape)
	if err != nil {
		t.Fatalf("PreparedQuery: %v", err)
	}
	assertQueryIDsEqual(t, q, got, expectedRecIDs(db, hasPriorityTag, tagPosAsc(priority), 1, 5))
}

func TestQueryCorrectness_PublicWrappersAndCacheModes(t *testing.T) {
	match := func(_ uint64, r *Rec) bool {
		return r.Country == "US" && hasAnyString(r.Tags, "go", "missing")
	}
	q := qx.Query(qx.EQ("country", "US"), qx.HASANY("tags", []string{"go", "missing"})).Sort("score", qx.DESC).Limit(4)

	db := newCorrectnessDB(t)
	want := expectedRecIDs(db, match, scoreDesc, 0, 4)
	assertCorrectQuery(t, db, q, want)

	recs, err := db.Query(q)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(recs) != len(want) {
		t.Fatalf("Query length mismatch: got=%d want=%d", len(recs), len(want))
	}
	for i := range recs {
		if !match(0, recs[i]) {
			t.Fatalf("Query returned non-matching record at %d: %#v", i, recs[i])
		}
	}

	opts := []Options{
		{SnapshotMaterializedPredCacheMaxEntries: -1},
		{SnapshotMaterializedPredCacheMaxEntries: 1, SnapshotMaterializedPredCacheMaxCardinality: 2},
	}
	for i := range opts {
		db = newCorrectnessDB(t, opts[i])
		assertCorrectQuery(t, db, q, expectedRecIDs(db, match, scoreDesc, 0, 4))
	}
}
