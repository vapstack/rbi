package qexec

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"unsafe"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/rbitrace"
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

func assertCorrectNoOrderPage(t *testing.T, db *DB[uint64, Rec], q *qx.QX, match correctnessRowMatch) {
	t.Helper()
	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	full := expectedRecIDs(db, match, nil, 0, 0)
	if err := testValidateNoOrderPage(q, got, full); err != nil {
		t.Fatalf("no-order page mismatch: %v\ngot=%v\nfull=%v", err, got, full)
	}
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
	assertCorrectNoOrderPage(t, db, q, notInRange)

	hasNoneRange := func(_ uint64, r *Rec) bool {
		return hasNoneString(r.Tags, "go", "db") && r.Age >= 20 && r.Age < 60
	}
	q = qx.Query(
		qx.HASNONE("tags", []string{"go", "db"}),
		qx.GTE("age", 20),
		qx.LT("age", 60),
	).Limit(32)
	assertCorrectNoOrderPage(t, db, q, hasNoneRange)

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

func TestCountScalarInSplit_TautologicalResidualCountsINPostings(t *testing.T) {
	db := newCorrectnessDB(t)

	expr := qx.AND(
		qx.IN("country", []string{"US", "DE"}),
		qx.NOT(qx.EQ("name", "missing")),
	)
	prepared, compiled, err := prepareTestExpr(db.engine, expr)
	if err != nil {
		t.Fatalf("prepareTestExpr: %v", err)
	}
	defer prepared.Release()

	trace := &Trace{}
	got, ok, err := db.engine.currentQueryViewForTests().TryFilterCardinalityByScalarInSplit(compiled, trace)
	if err != nil {
		t.Fatalf("TryFilterCardinalityByScalarInSplit: %v", err)
	}
	if !ok {
		t.Fatalf("TryFilterCardinalityByScalarInSplit was not used")
	}
	want := countExpected(db, func(_ uint64, r *Rec) bool {
		return (r.Country == "US" || r.Country == "DE") && r.Name != "missing"
	})
	if got != want {
		t.Fatalf("count mismatch: got=%d want=%d", got, want)
	}
	if trace.Event().Plan != rbitrace.PlanCountScalarInSplit {
		t.Fatalf("expected plan %q, got %q", rbitrace.PlanCountScalarInSplit, trace.Event().Plan)
	}
}

func TestCardinalityORByPredicates_DisjointScalarEQBranches(t *testing.T) {
	db := newCorrectnessDB(t)

	disjoint := qx.OR(
		qx.AND(qx.EQ("country", "US"), qx.PREFIX("full_name", "FN-0")),
		qx.AND(qx.EQ("country", "DE"), qx.GTE("age", 30)),
		qx.AND(qx.EQ("country", "NL"), qx.EQ("active", true)),
	)
	trace := &Trace{}
	got, ok, err := db.engine.tryFilterCardinalityORByPredicates(disjoint, trace)
	if err != nil {
		t.Fatalf("disjoint cardinality OR: %v", err)
	}
	if !ok {
		t.Fatalf("disjoint cardinality OR route was not used")
	}
	want := countExpected(db, func(_ uint64, r *Rec) bool {
		return r.Country == "US" && strings.HasPrefix(r.FullName, "FN-0") ||
			r.Country == "DE" && r.Age >= 30 ||
			r.Country == "NL" && r.Active
	})
	if got != want {
		t.Fatalf("disjoint cardinality OR mismatch: got=%d want=%d", got, want)
	}
	if trace.Event().Plan != rbitrace.PlanCountORPredicates {
		t.Fatalf("disjoint cardinality OR plan=%q, want %q", trace.Event().Plan, rbitrace.PlanCountORPredicates)
	}

	overlap := qx.OR(
		qx.AND(qx.EQ("country", "US"), qx.PREFIX("full_name", "FN-0")),
		qx.AND(qx.EQ("country", "US"), qx.GTE("age", 30)),
		qx.AND(qx.EQ("country", "DE"), qx.EQ("active", true)),
	)
	got, ok, err = db.engine.tryFilterCardinalityORByPredicates(overlap, nil)
	if err != nil {
		t.Fatalf("overlap cardinality OR: %v", err)
	}
	if !ok {
		t.Fatalf("overlap cardinality OR route was not used")
	}
	want = countExpected(db, func(_ uint64, r *Rec) bool {
		return r.Country == "US" && strings.HasPrefix(r.FullName, "FN-0") ||
			r.Country == "US" && r.Age >= 30 ||
			r.Country == "DE" && r.Active
	})
	if got != want {
		t.Fatalf("overlap cardinality OR mismatch: got=%d want=%d", got, want)
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
	assertCorrectNoOrderPage(t, db, q, prefixActive)

	priority := []string{"tag_mid", "go", "db"}
	hasPriorityTag := func(_ uint64, r *Rec) bool {
		return hasAnyString(r.Tags, "tag_mid", "go", "db")
	}
	q = qx.Query(qx.HASANY("tags", priority)).SortBy(qx.POS("tags", priority), qx.ASC).Offset(1).Limit(5)
	assertCorrectQuery(t, db, q, expectedRecIDs(db, hasPriorityTag, tagPosAsc(priority), 1, 5))

	hasSingleIndexedPriorityTag := func(_ uint64, r *Rec) bool {
		return hasAnyString(r.Tags, "tag_mid", "missing_tag")
	}
	q = qx.Query(qx.HASANY("tags", []string{"tag_mid", "missing_tag"})).SortBy(qx.POS("tags", priority), qx.ASC).Offset(1).Limit(5)
	assertCorrectQuery(t, db, q, expectedRecIDs(db, hasSingleIndexedPriorityTag, tagPosAsc(priority), 1, 5))
	q = qx.Query(qx.HASANY("tags", []string{"tag_mid", "missing_tag"})).SortBy(qx.POS("tags", priority), qx.ASC)
	assertCorrectQuery(t, db, q, expectedRecIDs(db, hasSingleIndexedPriorityTag, tagPosAsc(priority), 0, 0))

	prepared, shape, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer prepared.Release()
	got, err := db.engine.currentQueryViewForTests().PreparedQuery(&shape)
	if err != nil {
		t.Fatalf("PreparedQuery: %v", err)
	}
	assertQueryIDsEqual(t, q, got, expectedRecIDs(db, hasSingleIndexedPriorityTag, tagPosAsc(priority), 0, 0))
}

func TestQueryCorrectness_PreparedQueryConcurrentReadOnlyViews(t *testing.T) {
	db := newCorrectnessDB(t)

	priority := []string{"tag_mid", "go", "db"}
	activeWideAge := func(_ uint64, r *Rec) bool {
		return r.Active && r.Age >= 25
	}
	hasPriorityTag := func(_ uint64, r *Rec) bool {
		return hasAnyString(r.Tags, priority...)
	}
	cases := []struct {
		q    *qx.QX
		want []uint64
	}{
		{
			q:    qx.Query(qx.EQ("active", true), qx.GTE("age", 25)).Sort("score", qx.DESC).Offset(1).Limit(4),
			want: expectedRecIDs(db, activeWideAge, scoreDesc, 1, 4),
		},
		{
			q:    qx.Query(qx.HASANY("tags", priority)).SortBy(qx.POS("tags", priority), qx.ASC).Offset(1).Limit(5),
			want: expectedRecIDs(db, hasPriorityTag, tagPosAsc(priority), 1, 5),
		},
	}

	start := make(chan struct{})
	errCh := make(chan string, 16)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			<-start
			for i := 0; i < 50; i++ {
				tc := cases[(gid+i)&1]
				prepared, shape, err := prepareTestQuery(db.engine, tc.q)
				if err != nil {
					errCh <- "prepareTestQuery: " + err.Error()
					return
				}
				got, err := db.engine.currentQueryViewForTests().PreparedQuery(&shape)
				prepared.Release()
				if err != nil {
					errCh <- "PreparedQuery: " + err.Error()
					return
				}
				if !queryIDsEqual(tc.q, got, tc.want) {
					errCh <- "PreparedQuery mismatch"
					return
				}
			}
		}(g)
	}

	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func TestQueryFilterReleaseReuseKeepsPostingResultsOwned(t *testing.T) {
	db := newCorrectnessDB(t)
	cases := []struct {
		name string
		q    *qx.QX
		want []uint64
	}{
		{
			name: "scalar_and_range",
			q:    qx.Query(qx.EQ("country", "US"), qx.GTE("age", 30)),
			want: []uint64{4, 7, 10},
		},
		{
			name: "or_scalar_slice",
			q:    qx.Query(qx.OR(qx.EQ("country", "DE"), qx.HASANY("tags", []string{"ops"}))),
			want: []uint64{2, 4, 10, 11},
		},
		{
			name: "slice_all",
			q:    qx.Query(qx.HASALL("tags", []string{"go", "db"})),
			want: []uint64{1},
		},
		{
			name: "negative_and_range",
			q: qx.Query(
				qx.NOTIN("country", []string{"DE", "PL"}),
				qx.GTE("age", 20),
				qx.LT("age", 60),
			),
			want: []uint64{1, 4, 6, 7, 8, 9, 10, 12},
		},
		{
			name: "prefix_and_bool",
			q:    qx.Query(qx.PREFIX("full_name", "FN-0"), qx.EQ("active", true)),
			want: []uint64{1, 3, 4, 6, 8, 9},
		},
	}
	type retainedResult struct {
		ids  posting.List
		want []uint64
	}
	retained := make([]retainedResult, 0, len(cases)*8)
	defer func() {
		for i := range retained {
			retained[i].ids.Release()
		}
	}()

	snap := db.engine.snapshot.Current()
	for iter := 0; iter < 8; iter++ {
		for i := range cases {
			tc := cases[i]
			prepared, _, err := prepareTestQuery(db.engine, tc.q)
			if err != nil {
				t.Fatalf("%s prepareTestQuery: %v", tc.name, err)
			}
			view := db.engine.exec.AcquireView(snap)
			ids, err := view.Filter(prepared)
			db.engine.exec.ReleaseView(view)
			prepared.Release()
			if err != nil {
				t.Fatalf("%s Filter: %v", tc.name, err)
			}

			if got := ids.ToArray(); !slices.Equal(got, tc.want) {
				t.Fatalf("%s ids=%v want=%v", tc.name, got, tc.want)
			}
			retained = append(retained, retainedResult{ids: ids, want: tc.want})
			for j := range retained {
				if got := retained[j].ids.ToArray(); !slices.Equal(got, retained[j].want) {
					t.Fatalf("retained[%d] ids=%v want=%v", j, got, retained[j].want)
				}
			}
		}
	}
}

func qexecPreparedCurrentQuery(db *DB[uint64, Rec], q *qx.QX) ([]uint64, error) {
	prepared, shape, err := prepareTestQuery(db.engine, q)
	if err != nil {
		return nil, err
	}
	snap, seq, ref := db.engine.snapshot.PinCurrent()
	view := db.engine.exec.AcquireView(snap)
	got, err := view.PreparedQuery(&shape)
	db.engine.exec.ReleaseView(view)
	db.engine.snapshot.Unpin(seq, ref)
	prepared.Release()
	return got, err
}

func qexecAssertPreparedRouteMatchesQuery(t testing.TB, db *DB[uint64, Rec], q *qx.QX) {
	t.Helper()

	nq := qx.Normalize(qx.Clone(q))
	got, err := qexecPreparedCurrentQuery(db, nq)
	if err != nil {
		t.Fatalf("PreparedQuery(%+v): %v", nq, err)
	}
	if err = testValidateNoDuplicateKeys("prepared", got); err != nil {
		t.Fatal(err)
	}

	if testQueryNoOrderPage(nq) {
		fullQ := qx.Clone(nq)
		fullQ.Window.Offset = 0
		fullQ.Window.Limit = 0
		full, err := db.QueryKeys(fullQ)
		if err != nil {
			t.Fatalf("QueryKeys(full %+v): %v", fullQ, err)
		}
		if err = testValidateNoOrderPage(nq, got, full); err != nil {
			t.Fatalf("prepared no-order page mismatch: %v\ngot=%v\nfull=%v", err, got, full)
		}
		return
	}

	want, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys(%+v): %v", q, err)
	}
	if !queryIDsEqual(nq, got, want) {
		t.Fatalf("prepared query mismatch:\nq=%+v\nnq=%+v\ngot=%v\nwant=%v", q, nq, got, want)
	}
}

func qexecSeedNotInOrderOffsetData(t *testing.T, db *DB[uint64, Rec]) {
	t.Helper()

	countries := []string{"NL", "PL", "DE", "Finland", "Iceland", "Thailand", "Switzerland"}
	names := []string{"alice", "albert", "bob", "bobby", "carol", "dave", "eve"}
	tags := [][]string{{"go", "db"}, {"java"}, {"rust", "go"}, {"ops"}, {"go", "go", "db"}, {}}
	seedGeneratedUint64Data(t, db, 8_000, func(i int) *Rec {
		var opt *string
		if i&3 == 0 {
			s := fmt.Sprintf("opt-%d", i)
			opt = &s
		}
		return &Rec{
			Meta:     Meta{Country: countries[(i*17+3)%len(countries)]},
			Name:     names[(i*11+5)%len(names)],
			Age:      18 + i%50,
			Score:    float64((i*7919)%30_000) + float64(i%997)/1000,
			Active:   i&1 == 0,
			Tags:     append([]string(nil), tags[(i*7+1)%len(tags)]...),
			FullName: fmt.Sprintf("FN-%05d", i),
			Opt:      opt,
		}
	})
}

func TestQueryCorrectness_PreparedRouteNotInOrderOffset(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	qexecSeedNotInOrderOffsetData(t, db)

	tests := []struct {
		name string
		q    *qx.QX
	}{
		{
			name: "captured_shape",
			q: qx.Query(
				qx.NOTIN("country", []string{"Iceland", "Finland"}),
				qx.NOTIN("country", []string{"Iceland", "DE"}),
			).Sort("score", qx.ASC).Offset(446).Limit(70),
		},
		{
			name: "different_values",
			q: qx.Query(
				qx.NOTIN("country", []string{"DE", "PL"}),
				qx.NOTIN("country", []string{"Thailand", "US"}),
			).Sort("score", qx.ASC).Offset(210).Limit(90),
		},
		{
			name: "desc_order",
			q: qx.Query(
				qx.NOTIN("country", []string{"Iceland", "Finland"}),
				qx.NOTIN("country", []string{"Iceland", "DE"}),
			).Sort("score", qx.DESC).Offset(446).Limit(70),
		},
		{
			name: "without_offset",
			q: qx.Query(
				qx.NOTIN("country", []string{"Iceland", "Finland"}),
				qx.NOTIN("country", []string{"Iceland", "DE"}),
			).Sort("score", qx.ASC).Limit(70),
		},
	}

	for i := range tests {
		t.Run(tests[i].name, func(t *testing.T) {
			qexecAssertPreparedRouteMatchesQuery(t, db, tests[i].q)
		})
	}
}

func TestQueryCorrectness_PreparedRouteMultiTermHASLead(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	seedGeneratedUint64Data(t, db, 512, func(i int) *Rec {
		tags := []string{"go"}
		if i <= 180 {
			tags = []string{"db", "rust", "infra"}
		} else if i&1 == 0 {
			tags = []string{"db", "rust"}
		} else if i%3 == 0 {
			tags = []string{"db", "infra"}
		}
		return &Rec{
			Meta:     Meta{Country: []string{"NL", "PL", "DE", "US"}[i&3]},
			Name:     fmt.Sprintf("user-%03d", i),
			Age:      18 + i%71,
			Score:    float64(i % 97),
			Active:   i&1 == 0,
			Tags:     tags,
			FullName: fmt.Sprintf("FN-%05d", i),
		}
	})

	tests := []struct {
		name string
		q    *qx.QX
	}{
		{
			name: "no_order_limit",
			q: qx.Query(
				qx.HASALL("tags", []string{"db", "rust"}),
				qx.HASALL("tags", []string{"db", "infra"}),
				qx.HASANY("tags", []string{"infra", "rust"}),
			).Limit(120),
		},
		{
			name: "no_order_offset_limit",
			q: qx.Query(
				qx.HASALL("tags", []string{"db", "rust"}),
				qx.HASALL("tags", []string{"db", "infra"}),
				qx.HASANY("tags", []string{"infra", "rust"}),
			).Offset(40).Limit(80),
		},
		{
			name: "ordered_offset_limit",
			q: qx.Query(
				qx.HASALL("tags", []string{"db", "rust"}),
				qx.HASALL("tags", []string{"db", "infra"}),
				qx.HASANY("tags", []string{"infra", "rust"}),
			).Sort("age", qx.ASC).Offset(20).Limit(90),
		},
	}

	for i := range tests {
		t.Run(tests[i].name, func(t *testing.T) {
			qexecAssertPreparedRouteMatchesQuery(t, db, tests[i].q)
		})
	}
}

func TestQueryCorrectness_PreparedPinnedSnapshotStableDuringConcurrentPublish(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	ids := make([]uint64, 420)
	vals := make([]*Rec, 420)
	for i := range ids {
		id := uint64(i + 1)
		name := fmt.Sprintf("zz/%03d", id)
		if id <= 180 {
			name = fmt.Sprintf("aa/%03d", id)
		} else if id <= 320 {
			name = fmt.Sprintf("ab/%03d", id)
		}
		ids[i] = id
		vals[i] = &Rec{Name: name, Age: int(id)}
	}
	if err := db.BatchSet(ids, vals); err != nil {
		t.Fatalf("BatchSet(seed): %v", err)
	}

	snap := db.engine.snapshot.Current()
	pinned, ref, ok := db.engine.snapshot.PinBySeq(snap.Seq)
	if !ok {
		t.Fatalf("PinBySeq(%d): false", snap.Seq)
	}
	defer db.engine.snapshot.Unpin(pinned.Seq, ref)

	q := qx.Query(qx.PREFIX("name", "aa/")).Sort("name", qx.ASC)
	prepared, shape, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer prepared.Release()

	view := db.engine.exec.AcquireView(pinned)
	want, err := view.PreparedQuery(&shape)
	db.engine.exec.ReleaseView(view)
	if err != nil {
		t.Fatalf("PreparedQuery(seed): %v", err)
	}

	start := make(chan struct{})
	errCh := make(chan string, 1)
	var wg sync.WaitGroup

	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 200; i++ {
				view := db.engine.exec.AcquireView(pinned)
				got, err := view.PreparedQuery(&shape)
				db.engine.exec.ReleaseView(view)
				if err != nil {
					select {
					case errCh <- "PreparedQuery: " + err.Error():
					default:
					}
					return
				}
				if !slices.Equal(got, want) {
					select {
					case errCh <- "pinned snapshot query changed under concurrent publish":
					default:
					}
					return
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 200; i++ {
			id := uint64(i%420 + 1)
			if err := db.Patch(id, []Field{{Name: "name", Value: fmt.Sprintf("mut/%03d/%03d", i, id)}}); err != nil {
				select {
				case errCh <- fmt.Sprintf("Patch(%d): %v", id, err):
				default:
				}
				return
			}
		}
	}()

	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func TestQueryCorrectness_PreparedPinnedNumericRangeSnapshotStableDuringConcurrentPublish(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	const total = 2048
	ids := make([]uint64, total)
	vals := make([]*Rec, total)
	for i := range ids {
		id := uint64(i + 1)
		ids[i] = id
		vals[i] = &Rec{Name: fmt.Sprintf("user-%04d", id), Age: int(id)}
	}
	if err := db.BatchSet(ids, vals); err != nil {
		t.Fatalf("BatchSet(seed): %v", err)
	}
	setNumericBucketKnobs(t, db, 64, 1, 1)

	snap := db.engine.snapshot.Current()
	pinned, ref, ok := db.engine.snapshot.PinBySeq(snap.Seq)
	if !ok {
		t.Fatalf("PinBySeq(%d): false", snap.Seq)
	}
	defer db.engine.snapshot.Unpin(pinned.Seq, ref)

	q := qx.Query(qx.GTE("age", 1536))
	prepared, shape, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer prepared.Release()

	view := db.engine.exec.AcquireView(pinned)
	want, err := view.PreparedQuery(&shape)
	db.engine.exec.ReleaseView(view)
	if err != nil {
		t.Fatalf("PreparedQuery(seed): %v", err)
	}

	start := make(chan struct{})
	errCh := make(chan string, 1)
	var wg sync.WaitGroup

	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 200; i++ {
				view := db.engine.exec.AcquireView(pinned)
				got, err := view.PreparedQuery(&shape)
				db.engine.exec.ReleaseView(view)
				if err != nil {
					select {
					case errCh <- "PreparedQuery: " + err.Error():
					default:
					}
					return
				}
				if !queryIDsEqual(q, got, want) {
					select {
					case errCh <- "pinned numeric range query changed under concurrent publish":
					default:
					}
					return
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 200; i++ {
			id := uint64(i%total + 1)
			if err := db.Patch(id, []Field{{Name: "age", Value: total + i + 1}}); err != nil {
				select {
				case errCh <- fmt.Sprintf("Patch(%d): %v", id, err):
				default:
				}
				return
			}
		}
	}()

	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func TestQueryCorrectness_PreparedQueryConcurrentPublishKeepsWholeSnapshot(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	optAA, optBB, optCC, optDD := "aa", "bb", "cc", "dd"
	empty := ""
	ids := []uint64{1, 2, 3, 4, 5, 6}
	stateA := []*Rec{
		{Name: "A1-nil", Opt: nil, Active: true, Tags: []string{"go"}},
		{Name: "A2-aa", Opt: &optAA, Active: true, Tags: []string{"db"}},
		{Name: "A3-bb", Opt: &optBB, Active: true, Tags: []string{"ops"}},
		{Name: "A4-nil-off", Opt: nil, Active: false, Tags: []string{"rust"}},
		{Name: "A5-cc", Opt: &optCC, Active: true, Tags: []string{"go", "db"}},
		{Name: "A6-empty", Opt: &empty, Active: true, Tags: []string{"ops", "go"}},
	}
	stateB := []*Rec{
		{Name: "B1-dd", Opt: &optDD, Active: true, Tags: []string{"go"}},
		{Name: "B2-nil", Opt: nil, Active: true, Tags: []string{"db"}},
		{Name: "B3-aa-off", Opt: &optAA, Active: false, Tags: []string{"ops"}},
		{Name: "B4-empty", Opt: &empty, Active: true, Tags: []string{"rust"}},
		{Name: "B5-bb", Opt: &optBB, Active: true, Tags: []string{"go", "db"}},
		{Name: "B6-nil-off", Opt: nil, Active: false, Tags: []string{"ops", "go"}},
	}
	q := qx.Query(qx.EQ("active", true)).Sort("opt", qx.ASC).Offset(1).Limit(3)

	if err := db.BatchSet(ids, stateA); err != nil {
		t.Fatalf("BatchSet(stateA): %v", err)
	}
	wantA, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys(stateA): %v", err)
	}
	if err = db.BatchSet(ids, stateB); err != nil {
		t.Fatalf("BatchSet(stateB): %v", err)
	}
	wantB, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys(stateB): %v", err)
	}
	if err = db.BatchSet(ids, stateA); err != nil {
		t.Fatalf("BatchSet(stateA reset): %v", err)
	}

	start := make(chan struct{})
	errCh := make(chan string, 1)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 240; i++ {
			vals := stateB
			if i&1 == 1 {
				vals = stateA
			}
			if err := db.BatchSet(ids, vals); err != nil {
				select {
				case errCh <- "BatchSet: " + err.Error():
				default:
				}
				return
			}
		}
	}()

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 320; i++ {
				got, err := qexecPreparedCurrentQuery(db, q)
				if err != nil {
					select {
					case errCh <- "PreparedQuery: " + err.Error():
					default:
					}
					return
				}
				if queryIDsEqual(q, got, wantA) || queryIDsEqual(q, got, wantB) {
					continue
				}
				select {
				case errCh <- "PreparedQuery returned hybrid snapshot":
				default:
				}
				return
			}
		}()
	}

	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func TestQueryCorrectness_PreparedOrderedORConcurrentPublishKeepsWholeSnapshot(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	ids := []uint64{1, 2, 3, 4, 5, 6, 7, 8}
	stateA := []*Rec{
		{Meta: Meta{Country: "US"}, Name: "A1", Age: 31, Score: 80, Active: true, Tags: []string{"go"}},
		{Meta: Meta{Country: "DE"}, Name: "A2", Age: 26, Score: 70, Active: false, Tags: []string{"db"}},
		{Meta: Meta{Country: "NL"}, Name: "A3", Age: 40, Score: 60, Active: true, Tags: []string{"ops"}},
		{Meta: Meta{Country: "PL"}, Name: "A4", Age: 20, Score: 50, Active: true, Tags: []string{"rust"}},
		{Meta: Meta{Country: "US"}, Name: "A5", Age: 45, Score: 40, Active: false, Tags: []string{"go", "ops"}},
		{Meta: Meta{Country: "DE"}, Name: "A6", Age: 33, Score: 30, Active: true, Tags: []string{"java"}},
		{Meta: Meta{Country: "JP"}, Name: "A7", Age: 55, Score: 20, Active: true, Tags: []string{"db"}},
		{Meta: Meta{Country: "US"}, Name: "A8", Age: 29, Score: 10, Active: true, Tags: []string{"ops"}},
	}
	stateB := []*Rec{
		{Meta: Meta{Country: "US"}, Name: "B1", Age: 28, Score: 15, Active: false, Tags: []string{"go"}},
		{Meta: Meta{Country: "DE"}, Name: "B2", Age: 30, Score: 25, Active: true, Tags: []string{"ops"}},
		{Meta: Meta{Country: "NL"}, Name: "B3", Age: 41, Score: 35, Active: true, Tags: []string{"go"}},
		{Meta: Meta{Country: "PL"}, Name: "B4", Age: 22, Score: 45, Active: true, Tags: []string{"db"}},
		{Meta: Meta{Country: "US"}, Name: "B5", Age: 47, Score: 55, Active: true, Tags: []string{"ops"}},
		{Meta: Meta{Country: "DE"}, Name: "B6", Age: 21, Score: 65, Active: false, Tags: []string{"java"}},
		{Meta: Meta{Country: "JP"}, Name: "B7", Age: 57, Score: 75, Active: true, Tags: []string{"go", "db"}},
		{Meta: Meta{Country: "US"}, Name: "B8", Age: 35, Score: 85, Active: true, Tags: []string{"rust"}},
	}
	q := qx.Query(qx.OR(
		qx.AND(qx.EQ("active", true), qx.HASANY("tags", []string{"go", "ops"})),
		qx.AND(qx.EQ("country", "DE"), qx.GTE("age", 25)),
	)).Sort("score", qx.DESC).Offset(1).Limit(5)

	if err := db.BatchSet(ids, stateA); err != nil {
		t.Fatalf("BatchSet(stateA): %v", err)
	}
	wantA, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys(stateA): %v", err)
	}
	if err = db.BatchSet(ids, stateB); err != nil {
		t.Fatalf("BatchSet(stateB): %v", err)
	}
	wantB, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys(stateB): %v", err)
	}
	if err = db.BatchSet(ids, stateA); err != nil {
		t.Fatalf("BatchSet(stateA reset): %v", err)
	}

	start := make(chan struct{})
	errCh := make(chan string, 1)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 220; i++ {
			vals := stateB
			if i&1 == 1 {
				vals = stateA
			}
			if err := db.BatchSet(ids, vals); err != nil {
				select {
				case errCh <- "BatchSet: " + err.Error():
				default:
				}
				return
			}
		}
	}()

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 320; i++ {
				got, err := qexecPreparedCurrentQuery(db, q)
				if err != nil {
					select {
					case errCh <- "PreparedQuery: " + err.Error():
					default:
					}
					return
				}
				if queryIDsEqual(q, got, wantA) || queryIDsEqual(q, got, wantB) {
					continue
				}
				select {
				case errCh <- "PreparedQuery returned hybrid ordered OR snapshot":
				default:
				}
				return
			}
		}()
	}

	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func TestQueryCorrectness_StringPreparedOrderedORConcurrentPublishKeepsWholeSnapshot(t *testing.T) {
	var zero Rec
	rt, err := schema.Compile(reflect.TypeOf(zero), schema.Config{})
	if err != nil {
		t.Fatalf("schema.Compile: %v", err)
	}
	exec := NewRuntime(Config{
		Schema:                         rt,
		StrKey:                         true,
		AnalyzeInterval:                -1,
		NumericRangeBucketSize:         testNumericRangeBucketSize,
		NumericRangeBucketMinFieldKeys: testNumericRangeMinKeys,
		NumericRangeBucketMinSpanKeys:  testNumericRangeMinSpan,
	})
	cfg := snapshot.CacheConfig{
		MatPredMaxEntries: testMatPredCacheMaxEntries,
		MatPredMaxCard:    testMatPredCacheMaxCard,
	}
	qe := &queryEngine{
		snapshot: snapshot.NewRegistry(false),
		schema:   rt,
		exec:     exec,
		cfg:      cfg,
	}
	ids := make([]uint64, 7)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}

	stateA := []*Rec{
		{Name: "aa/00", Active: true, Score: 10, Meta: Meta{Country: "US"}},
		{Name: "aa/01", Active: false, Score: 40, Meta: Meta{Country: "FR"}},
		{Name: "aa/02", Active: true, Score: 70, Meta: Meta{Country: "NL"}},
		{Name: "ab/00", Active: false, Score: 30, Meta: Meta{Country: "NL"}},
		{Name: "ab/01", Active: true, Score: 60, Meta: Meta{Country: "US"}},
		{Name: "ac/00", Active: true, Score: 50, Meta: Meta{Country: "FR"}},
		{Name: "zz/00", Active: true, Score: 5, Meta: Meta{Country: "DE"}},
	}
	stateB := []*Rec{
		{Name: "aa/00", Active: false, Score: 10, Meta: Meta{Country: "US"}},
		{Name: "aa/03", Active: true, Score: 20, Meta: Meta{Country: "FR"}},
		{Name: "aa/04", Active: true, Score: 40, Meta: Meta{Country: "DE"}},
		{Name: "ab/00", Active: true, Score: 30, Meta: Meta{Country: "NL"}},
		{Name: "ab/02", Active: false, Score: 60, Meta: Meta{Country: "US"}},
		{Name: "ab/03", Active: true, Score: 80, Meta: Meta{Country: "NL"}},
		{Name: "ac/00", Active: true, Score: 50, Meta: Meta{Country: "FR"}},
	}
	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.PREFIX("name", "aa/"),
				qx.EQ("active", true),
			),
			qx.AND(
				qx.PREFIX("name", "ab/"),
				qx.EQ("country", "NL"),
			),
			qx.AND(
				qx.EQ("name", "aa/03"),
				qx.LT("score", 25.0),
			),
		),
	).Sort("name", qx.ASC).Offset(1).Limit(3)

	cur := make([]*Rec, len(ids))
	seq := uint64(1)
	publish := func(vals []*Rec) {
		entries := make([]snapshot.BatchEntry, len(ids))
		for i := range ids {
			var old unsafe.Pointer
			if cur[i] != nil {
				old = unsafe.Pointer(cur[i])
			}
			cur[i] = vals[i]
			entries[i] = snapshot.BatchEntry{ID: ids[i], Old: old, New: unsafe.Pointer(vals[i])}
		}
		next := snapshot.Build(seq, qe.snapshot.Current(), rt, cfg, nil, entries)
		qe.snapshot.Publish(next)
		seq++
	}
	queryCurrent := func() ([]uint64, error) {
		prepared, shape, err := prepareTestQuery(qe, q)
		if err != nil {
			return nil, err
		}
		snap, snapSeq, ref := qe.snapshot.PinCurrent()
		view := qe.exec.AcquireView(snap)
		got, err := view.PreparedQuery(&shape)
		qe.exec.ReleaseView(view)
		qe.snapshot.Unpin(snapSeq, ref)
		prepared.Release()
		return got, err
	}

	publish(stateA)
	wantA, err := queryCurrent()
	if err != nil {
		t.Fatalf("PreparedQuery(stateA): %v", err)
	}
	publish(stateB)
	wantB, err := queryCurrent()
	if err != nil {
		t.Fatalf("PreparedQuery(stateB): %v", err)
	}
	publish(stateA)

	start := make(chan struct{})
	errCh := make(chan string, 1)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 320; i++ {
			vals := stateB
			if i&1 == 1 {
				vals = stateA
			}
			publish(vals)
		}
	}()

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 480; i++ {
				got, err := queryCurrent()
				if err != nil {
					select {
					case errCh <- "PreparedQuery: " + err.Error():
					default:
					}
					return
				}
				if slices.Equal(got, wantA) || slices.Equal(got, wantB) {
					continue
				}
				select {
				case errCh <- "PreparedQuery returned hybrid string-key ordered OR snapshot":
				default:
				}
				return
			}
		}()
	}

	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
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
