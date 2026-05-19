package rbi

import (
	"fmt"
	"github.com/vapstack/qx"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestQuery_RouteEquivalence_StringKeys_Randomized(t *testing.T) {
	db, _ := openTempDBString(t, Options{AnalyzeInterval: -1})

	countries := []string{"NL", "PL", "DE", "Finland", "Iceland", "Thailand", "US"}
	names := []string{"alice", "albert", "bob", "bobby", "carol", "dave", "eve", "zoe", "nik"}
	tagPool := []string{"go", "db", "ops", "rust", "java", "ml", "devops", "api", "infra"}
	r := newRand(20260302)

	for i := 1; i <= 2_000; i++ {
		tagN := 1 + r.IntN(3)
		tags := make([]string, 0, tagN)
		for len(tags) < tagN {
			tags = append(tags, tagPool[r.IntN(len(tagPool))])
		}
		name := names[r.IntN(len(names))]
		rec := &Rec{
			Meta:     Meta{Country: countries[r.IntN(len(countries))]},
			Name:     name,
			Email:    fmt.Sprintf("%s-%04d@example.test", name, i),
			Age:      18 + r.IntN(60),
			Score:    float64(r.IntN(10_000))/100 + r.Float64()*0.001,
			Active:   r.IntN(2) == 0,
			Tags:     tags,
			FullName: fmt.Sprintf("FN-%05d", i),
		}
		if i%11 == 0 {
			s := fmt.Sprintf("opt-%d", i)
			rec.Opt = &s
		}
		if err := db.Set(fmt.Sprintf("id-%05d", i), rec); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}

	pickVals := func(src []string, n int) []string {
		out := make([]string, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, src[r.IntN(len(src))])
		}
		return out
	}
	randomLeaf := func() qx.Expr {
		switch r.IntN(12) {
		case 0:
			return qx.EQ("active", r.IntN(2) == 0)
		case 1:
			return qx.IN("country", pickVals(countries, 2))
		case 2:
			return qx.NOTIN("country", pickVals(countries, 2))
		case 3:
			return qx.GTE("age", 18+r.IntN(35))
		case 4:
			return qx.LTE("age", 25+r.IntN(45))
		case 5:
			return qx.HASANY("tags", pickVals(tagPool, 2))
		case 6:
			return qx.HASALL("tags", pickVals(tagPool, 2))
		case 7:
			return qx.HASNONE("tags", pickVals(tagPool, 2))
		case 8:
			return qx.PREFIX("full_name", fmt.Sprintf("FN-%d", 1+r.IntN(3)))
		case 9:
			return qx.SUFFIX("country", "land")
		case 10:
			return qx.CONTAINS("country", "land")
		default:
			return qx.GT("score", float64(20+r.IntN(70)))
		}
	}
	randomExpr := func() qx.Expr {
		if r.IntN(100) < 35 {
			return qx.OR(
				qx.AND(randomLeaf(), randomLeaf()),
				qx.AND(randomLeaf(), randomLeaf()),
			)
		}
		if r.IntN(100) < 60 {
			return qx.AND(randomLeaf(), randomLeaf(), randomLeaf())
		}
		return randomLeaf()
	}
	randomOrder := func() []qx.Order {
		switch r.IntN(5) {
		case 0, 1:
			return []qx.Order{testOrderBasic("score", r.IntN(2) == 0)}
		case 2:
			return []qx.Order{testOrderBasic("age", r.IntN(2) == 0)}
		case 3:
			return []qx.Order{testOrderByArrayPos("tags", pickVals(tagPool, 3), r.IntN(2) == 0)}
		default:
			if r.IntN(100) < 50 {
				return []qx.Order{testOrderByArrayPos("country", pickVals(countries, 3), r.IntN(2) == 0)}
			}
			return []qx.Order{testOrderByArrayCount("tags", r.IntN(2) == 0)}
		}
	}

	for step := 0; step < 180; step++ {
		q := &qx.QX{Filter: randomExpr()}
		mode := r.IntN(100)
		switch {
		case mode < 45:
			q.Order = randomOrder()
			q.Window.Offset = uint64(r.IntN(120))
			q.Window.Limit = uint64(20 + r.IntN(120))
		case mode < 65:
			q.Window.Offset = uint64(r.IntN(180))
			q.Window.Limit = uint64(10 + r.IntN(120))
		case mode < 80:
			q.Order = randomOrder()
		}

		gotKeys, err := db.QueryKeys(q)
		if err != nil {
			t.Fatalf("step=%d QueryKeys(%+v): %v", step, q, err)
		}
		var wantPage []string
		var wantFull []string
		if len(q.Order) == 0 && (q.Window.Offset > 0 || q.Window.Limit > 0) {
			fullQ := cloneQuery(q)
			fullQ.Window.Offset = 0
			fullQ.Window.Limit = 0
			var err error
			wantFull, err = expectedKeysString(t, db, fullQ)
			if err != nil {
				t.Fatalf("step=%d expectedKeysString(full %+v): %v", step, q, err)
			}
			assertNoOrderWindowSubsetString(t, q, gotKeys, wantFull, fmt.Sprintf("step=%d QueryKeys", step))
		} else {
			var err error
			wantPage, err = expectedKeysString(t, db, q)
			if err != nil {
				t.Fatalf("step=%d expectedKeysString(page %+v): %v", step, q, err)
			}
			if !queryStringIDsEqual(q, gotKeys, wantPage) {
				t.Fatalf("step=%d page mismatch q=%+v\ngot=%v\nwant=%v", step, q, gotKeys, wantPage)
			}
		}

		gotItems, err := db.Query(q)
		if err != nil {
			t.Fatalf("step=%d Query(%+v): %v", step, q, err)
		}
		wantItems, err := db.BatchGet(gotKeys...)
		if err != nil {
			t.Fatalf("step=%d BatchGet(got): %v", step, err)
		}
		if len(gotItems) != len(wantItems) {
			t.Fatalf("step=%d items len mismatch: got=%d want=%d q=%+v", step, len(gotItems), len(wantItems), q)
		}
		for i := range wantItems {
			if gotItems[i] == nil || wantItems[i] == nil {
				t.Fatalf("step=%d nil item mismatch at i=%d q=%+v got=%#v want=%#v", step, i, q, gotItems[i], wantItems[i])
			}
			if !reflect.DeepEqual(*gotItems[i], *wantItems[i]) {
				t.Fatalf("step=%d item mismatch at i=%d q=%+v got=%#v want=%#v", step, i, q, gotItems[i], wantItems[i])
			}
		}

		countQ := cloneQuery(q)
		countQ.Order = nil
		countQ.Window.Offset = 0
		countQ.Window.Limit = 0
		wantCount := 0
		switch {
		case q.Window.Offset == 0 && q.Window.Limit == 0:
			wantCount = len(wantPage)
		case wantFull != nil:
			wantCount = len(wantFull)
		default:
			wantAll, err := expectedKeysString(t, db, countQ)
			if err != nil {
				t.Fatalf("step=%d expectedKeysString(all %+v): %v", step, q, err)
			}
			wantCount = len(wantAll)
		}
		cnt, err := db.Count(q.Filter)
		if err != nil {
			t.Fatalf("step=%d Count(%+v): %v", step, q, err)
		}
		if cnt != uint64(wantCount) {
			t.Fatalf("step=%d count mismatch q=%+v got=%d want=%d", step, q, cnt, wantCount)
		}
	}
}

func TestQuery_RouteEquivalence_PreparedExecutionPlanner_StringKeys(t *testing.T) {
	db, _ := openTempDBString(t, Options{AnalyzeInterval: -1})
	r := newRand(20260303)
	countries := []string{"NL", "PL", "DE", "Finland", "Iceland", "Thailand", "US"}
	names := []string{"alice", "albert", "bob", "bobby", "carol", "dave", "eve", "zoe", "nik"}
	tagPool := []string{"go", "db", "ops", "rust", "java", "ml", "devops", "api", "infra"}

	for i := 1; i <= 6_000; i++ {
		tagN := 1 + r.IntN(3)
		tags := make([]string, 0, tagN)
		for len(tags) < tagN {
			tags = append(tags, tagPool[r.IntN(len(tagPool))])
		}
		name := names[r.IntN(len(names))]
		rec := &Rec{
			Meta:     Meta{Country: countries[r.IntN(len(countries))]},
			Name:     name,
			Email:    fmt.Sprintf("%s-%05d@example.test", name, i),
			Age:      18 + r.IntN(60),
			Score:    float64(r.IntN(10_000))/100 + r.Float64()*0.001,
			Active:   r.IntN(2) == 0,
			Tags:     tags,
			FullName: fmt.Sprintf("FN-%05d", i),
		}
		if err := db.Set(fmt.Sprintf("id-%05d", i), rec); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}

	queries := []*qx.QX{
		qx.Query(
			qx.EQ("active", true),
			qx.GTE("age", 22),
			qx.LT("age", 50),
		).Sort("age", qx.ASC).Limit(120),
		qx.Query(
			qx.PREFIX("full_name", "FN-1"),
			qx.EQ("active", true),
		).Sort("full_name", qx.ASC).Limit(90),
		qx.Query(
			qx.OR(
				qx.EQ("active", true),
				qx.EQ("name", "alice"),
			),
		).Limit(140),
		qx.Query(
			qx.OR(
				qx.AND(
					qx.EQ("active", true),
					qx.HASANY("tags", []string{"go", "ops"}),
				),
				qx.AND(
					qx.EQ("country", "DE"),
					qx.GTE("age", 25),
				),
			),
		).Sort("score", qx.DESC).Offset(40).Limit(90),
		queryOrderSortByArrayCount(qx.Query(
			qx.GTE("age", 20),
		), "tags", qx.DESC).Offset(7).Limit(55),
		qx.Query(
			qx.EQ("active", true),
			qx.HASANY("tags", []string{"go", "db"}),
			qx.NOTIN("country", []string{"PL", "DE"}),
		).Limit(85),
		qx.Query(
			qx.HASALL("tags", []string{"go", "db"}),
			qx.HASANY("tags", []string{"go", "ops"}),
		).Sort("score", qx.DESC).Offset(30).Limit(70),
	}

	var sawExec bool
	var sawPlan bool
	for i := range queries {
		q := queries[i]

		nq, ref, usedExec, usedPlan := assertPreparedRouteEquivalenceString(t, db, q)
		sawExec = sawExec || usedExec
		sawPlan = sawPlan || usedPlan

		got, err := db.QueryKeys(q)
		if err != nil {
			t.Fatalf("q%d QueryKeys: %v", i, err)
		}
		want, err := expectedKeysString(t, db, q)
		if err != nil {
			t.Fatalf("q%d expectedKeysString: %v", i, err)
		}
		if len(q.Order) == 0 && (q.Window.Limit > 0 || q.Window.Offset > 0) {
			fullQ := cloneQuery(q)
			fullQ.Window.Offset = 0
			fullQ.Window.Limit = 0
			fullWant, err := expectedKeysString(t, db, fullQ)
			if err != nil {
				t.Fatalf("q%d expectedKeysString(full): %v", i, err)
			}
			assertNoOrderWindowSubsetString(t, q, got, fullWant, fmt.Sprintf("q%d QueryKeys", i))
			assertNoOrderWindowSubsetString(t, nq, ref, fullWant, fmt.Sprintf("q%d prepared", i))
		} else {
			if !queryStringIDsEqual(q, got, want) {
				t.Fatalf("q%d QueryKeys mismatch:\n got=%v\nwant=%v", i, got, want)
			}
			if !queryStringIDsEqual(nq, ref, want) {
				t.Fatalf("q%d prepared mismatch:\n got=%v\nwant=%v", i, ref, want)
			}
		}
	}

	if !sawExec {
		t.Fatalf("expected at least one execution fast-path route")
	}
	if !sawPlan {
		t.Fatalf("expected at least one planner fast-path route")
	}
}

func TestQueryCorrectnessAgainstSeqScan_Uint64Keys(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 80)

	queries := []*qx.QX{
		qx.Query(qx.EQ("name", "alice")),
		qx.Query(qx.PREFIX("name", "al")),
		qx.Query(qx.SUFFIX("country", "land")),
		qx.Query(qx.CONTAINS("country", "land")),

		qx.Query(qx.GT("age", 30)),
		qx.Query(qx.GTE("age", 30)),
		qx.Query(qx.LT("age", 25)),
		qx.Query(qx.LTE("age", 25)),
		qx.Query(qx.IN("age", []int{18, 19, 20})),

		qx.Query(qx.HASALL("tags", []string{"go", "db"})),
		qx.Query(qx.HASANY("tags", []string{"go", "java"})),
		qx.Query(qx.HASNONE("tags", []string{"rust"})),

		qx.Query(
			qx.OR(
				qx.AND(
					qx.GT("age", 30),
					qx.EQ("active", true),
				),
				qx.PREFIX("name", "al"),
			),
		),

		qx.Query(qx.NOT(qx.CONTAINS("country", "land"))),
	}

	queries = append(queries,
		qx.Query(qx.GT("age", 20)).Sort("age", qx.ASC).Offset(5).Limit(10),
		queryOrderSortByArrayCount(qx.Query(), "tags", qx.DESC),
		queryOrderSortByArrayPos(qx.Query(), "tags", []string{"go", "java"}, qx.ASC),
	)

	for i, q := range queries {
		q := q
		t.Run(fmt.Sprintf("q%d_%v", i, plannerExtExprOp(q.Filter)), func(t *testing.T) {
			gotKeys, err := db.QueryKeys(q)
			if err != nil {
				t.Fatalf("QueryKeys: %v", err)
			}

			wantPageKeys, err := expectedKeysUint64(t, db, q)
			if err != nil {
				t.Fatalf("expectedKeysUint64(page): %v", err)
			}
			assertQueryIDsEqual(t, q, gotKeys, wantPageKeys)

			gotItems, err := db.Query(q)
			if err != nil {
				t.Fatalf("Query: %v", err)
			}
			if len(gotItems) != len(wantPageKeys) {
				t.Fatalf("Query len mismatch: got=%d want=%d", len(gotItems), len(wantPageKeys))
			}

			qNoPage := *q
			qNoPage.Window.Offset = 0
			qNoPage.Window.Limit = 0

			wantAllKeys, err := expectedKeysUint64(t, db, &qNoPage)
			if err != nil {
				t.Fatalf("expectedKeysUint64(all): %v", err)
			}

			cnt, err := db.Count(q.Filter)
			if err != nil {
				t.Fatalf("Count: %v", err)
			}
			if cnt != uint64(len(wantAllKeys)) {
				t.Fatalf("Count mismatch: got=%d want=%d", cnt, len(wantAllKeys))
			}
		})
	}
}

func TestQuerySetEquivalence_StringKeys(t *testing.T) {
	db, path := openTempDBString(t)

	for i := 1; i <= 20; i++ {
		id := fmt.Sprintf("id-%02d", i)
		r := &Rec{
			Meta:     Meta{Country: "NL"},
			Name:     "alice",
			Age:      i,
			Score:    float64(i),
			Active:   i%2 == 0,
			Tags:     []string{"go"},
			FullName: "X",
		}
		if err := db.Set(id, r); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	q := qx.Query(qx.GT("age", 10))

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}

	wantSet := map[string]struct{}{}
	err = db.SeqScan("", func(id string, v *Rec) (bool, error) {
		ok, e := evalExprBool(v, q.Filter)
		if e != nil {
			return false, e
		}
		if ok {
			wantSet[id] = struct{}{}
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("SeqScan: %v", err)
	}

	gotSet := map[string]struct{}{}
	for _, id := range got {
		gotSet[id] = struct{}{}
	}

	if len(gotSet) != len(wantSet) {
		t.Fatalf("set size mismatch: got=%d want=%d (db=%s)", len(gotSet), len(wantSet), path)
	}
	for k := range wantSet {
		if _, ok := gotSet[k]; !ok {
			t.Fatalf("missing id %q in result set", k)
		}
	}
}

func TestScanKeysUint64_SeekOrder(t *testing.T) {
	db, _ := openTempDBUint64(t)

	for i := 1; i <= 5; i++ {
		r := &Rec{Name: fmt.Sprintf("n%d", i), Age: i}
		if err := db.Set(uint64(i), r); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	var got []uint64
	err := db.ScanKeys(3, func(id uint64) (bool, error) {
		got = append(got, id)
		return true, nil
	})
	if err != nil {
		t.Fatalf("ScanKeys: %v", err)
	}

	want := []uint64{3, 4, 5}
	if !slices.Equal(want, got) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestScanKeys_String_SeekLowerBound(t *testing.T) {
	db, _ := openTempDBString(t)

	for i := 1; i <= 5; i++ {
		id := fmt.Sprintf("id-%02d", i)
		r := &Rec{Name: "x", Age: i}
		if err := db.Set(id, r); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	seek := "id-03"
	got := make(map[string]struct{})
	err := db.ScanKeys(seek, func(id string) (bool, error) {
		if id < seek {
			t.Fatalf("unexpected id below seek: %v", id)
		}
		got[id] = struct{}{}
		return true, nil
	})
	if err != nil {
		t.Fatalf("ScanKeys: %v", err)
	}

	want := map[string]struct{}{
		"id-03": {},
		"id-04": {},
		"id-05": {},
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d keys, got %d: %v", len(want), len(got), got)
	}
	for id := range want {
		if _, ok := got[id]; !ok {
			t.Fatalf("missing key: %v (got=%v)", id, got)
		}
	}
}

func TestQuery_NegativeNoOrder_ExcludesCorrectly_WithPaging(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 220)

	q := qx.Query(
		qx.NOT(qx.EQ("name", "alice")),
	).Offset(10).Limit(40)

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}

	fullQ := cloneQuery(q)
	clearQueryOrderWindowForTest(fullQ)
	full, err := expectedKeysUint64(t, db, fullQ)
	if err != nil {
		t.Fatalf("expectedKeysUint64(full): %v", err)
	}
	assertNoOrderWindowSubset(t, q, got, full, "QueryKeys")
}

func TestQuery_RandomMixedMultiWrites_MatchSeqScanModel(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 240)

	r := newRand(20260326)
	countries := []string{"NL", "PL", "DE", "Finland", "Iceland", "Thailand", "US"}
	tagPool := []string{"go", "db", "ops", "rust", "java", "infra", "ml"}
	names := []string{"alice", "bob", "carol", "dave", "eve", "nik"}

	randomTags := func() []string {
		n := 1 + r.IntN(4)
		out := make([]string, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, tagPool[r.IntN(len(tagPool))])
		}
		return out
	}
	randomRec := func(id uint64) *Rec {
		name := names[r.IntN(len(names))]
		return &Rec{
			Meta:     Meta{Country: countries[r.IntN(len(countries))]},
			Name:     name,
			Email:    fmt.Sprintf("%s-%d-%d@example.test", name, id, r.IntN(10_000)),
			Age:      18 + r.IntN(62),
			Score:    float64(r.IntN(1000))/10.0 + r.Float64()*0.0001,
			Active:   r.IntN(2) == 0,
			Tags:     randomTags(),
			FullName: fmt.Sprintf("FN-%05d", id),
		}
	}
	randomPatch := func() []Field {
		switch r.IntN(6) {
		case 0:
			return []Field{{Name: "age", Value: 18 + r.IntN(62)}}
		case 1:
			return []Field{{Name: "country", Value: countries[r.IntN(len(countries))]}}
		case 2:
			return []Field{{Name: "tags", Value: randomTags()}}
		case 3:
			return []Field{{Name: "active", Value: r.IntN(2) == 0}}
		case 4:
			return []Field{{Name: "score", Value: float64(r.IntN(1000)) / 10.0}}
		default:
			return []Field{{Name: "name", Value: names[r.IntN(len(names))]}}
		}
	}
	pickVals := func(src []string, n int) []string {
		out := make([]string, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, src[r.IntN(len(src))])
		}
		return out
	}

	randomLeaf := func() qx.Expr {
		switch r.IntN(8) {
		case 0:
			return qx.EQ("active", r.IntN(2) == 0)
		case 1:
			return qx.IN("country", pickVals(countries, 2))
		case 2:
			return qx.NOTIN("country", pickVals(countries, 2))
		case 3:
			return qx.GTE("age", 18+r.IntN(40))
		case 4:
			return qx.LTE("age", 20+r.IntN(45))
		case 5:
			return qx.HASANY("tags", pickVals(tagPool, 2))
		case 6:
			return qx.PREFIX("full_name", fmt.Sprintf("FN-%d", r.IntN(3)))
		default:
			return qx.GT("score", float64(r.IntN(85)))
		}
	}
	randomExpr := func() qx.Expr {
		a := randomLeaf()
		if r.IntN(100) < 45 {
			b := randomLeaf()
			if r.IntN(2) == 0 {
				return qx.AND(a, b)
			}
			return qx.OR(a, b)
		}
		return a
	}
	randomQuery := func() *qx.QX {
		q := &qx.QX{Filter: randomExpr()}
		switch r.IntN(4) {
		case 0:
			q.Order = []qx.Order{testOrderBasic("age", r.IntN(2) == 0)}
		case 1:
			q.Order = []qx.Order{testOrderByArrayPos("tags", pickVals(tagPool, 3), r.IntN(2) == 0)}
		case 2:
			q.Order = []qx.Order{testOrderByArrayPos("country", pickVals(countries, 3), r.IntN(2) == 0)}
		default:
			q.Order = []qx.Order{testOrderByArrayCount("tags", r.IntN(2) == 0)}
		}
		q.Window.Offset = uint64(r.IntN(40))
		if r.IntN(100) < 10 {
			q.Window.Limit = 0
		} else {
			q.Window.Limit = uint64(1 + r.IntN(120))
		}
		return q
	}

	opsLog := make([]string, 0, 256)

	for step := 0; step < 180; step++ {
		id := uint64(1 + r.IntN(360))
		switch x := r.IntN(100); {
		case x < 22:
			rec := randomRec(id)
			if err := db.Set(id, rec); err != nil {
				t.Fatalf("step=%d Set(id=%d): %v", step, id, err)
			}
			opsLog = append(opsLog, fmt.Sprintf("step=%d set id=%d tags=%v country=%s age=%d active=%v", step, id, rec.Tags, rec.Country, rec.Age, rec.Active))
		case x < 44:
			n := 2 + r.IntN(5)
			ids := make([]uint64, n)
			vals := make([]*Rec, n)
			pairs := make([]string, n)
			for i := 0; i < n; i++ {
				cid := uint64(1 + r.IntN(360))
				ids[i] = cid
				vals[i] = randomRec(cid)
				pairs[i] = fmt.Sprintf("%d:%v", cid, vals[i].Tags)
			}
			if err := db.BatchSet(ids, vals); err != nil {
				t.Fatalf("step=%d BatchSet(ids=%v): %v", step, ids, err)
			}
			opsLog = append(opsLog, fmt.Sprintf("step=%d set_many %s", step, strings.Join(pairs, " | ")))
		case x < 58:
			patch := randomPatch()
			if err := db.Patch(id, patch); err != nil {
				t.Fatalf("step=%d Patch(id=%d): %v", step, id, err)
			}
			opsLog = append(opsLog, fmt.Sprintf("step=%d patch id=%d patch=%v", step, id, patch))
		case x < 74:
			n := 2 + r.IntN(5)
			ids := make([]uint64, n)
			for i := 0; i < n; i++ {
				ids[i] = uint64(1 + r.IntN(360))
			}
			patch := randomPatch()
			if err := db.BatchPatch(ids, patch); err != nil {
				t.Fatalf("step=%d BatchPatch(ids=%v,patch=%v): %v", step, ids, patch, err)
			}
			opsLog = append(opsLog, fmt.Sprintf("step=%d patch_many ids=%v patch=%v", step, ids, patch))
		case x < 88:
			if err := db.Delete(id); err != nil {
				t.Fatalf("step=%d Delete(id=%d): %v", step, id, err)
			}
			opsLog = append(opsLog, fmt.Sprintf("step=%d delete id=%d", step, id))
		default:
			n := 1 + r.IntN(5)
			ids := make([]uint64, n)
			for i := 0; i < n; i++ {
				ids[i] = uint64(1 + r.IntN(360))
			}
			if err := db.BatchDelete(ids); err != nil {
				t.Fatalf("step=%d BatchDelete(ids=%v): %v", step, ids, err)
			}
			opsLog = append(opsLog, fmt.Sprintf("step=%d delete_many ids=%v", step, ids))
		}

		for qi := 0; qi < 3; qi++ {
			q := randomQuery()

			gotKeys, err := db.QueryKeys(q)
			if err != nil {
				t.Fatalf("step=%d qi=%d QueryKeys: %v q=%+v", step, qi, err, q)
			}
			wantKeys, err := expectedKeysUint64(t, db, q)
			if err != nil {
				t.Fatalf("step=%d qi=%d expectedKeys: %v q=%+v", step, qi, err, q)
			}
			if !slices.Equal(gotKeys, wantKeys) {
				first := -1
				maxCmp := len(gotKeys)
				if len(wantKeys) < maxCmp {
					maxCmp = len(wantKeys)
				}
				for i := 0; i < maxCmp; i++ {
					if gotKeys[i] != wantKeys[i] {
						first = i
						break
					}
				}
				if first == -1 && len(gotKeys) != len(wantKeys) {
					first = maxCmp
				}

				extra := ""
				if first >= 0 && first < len(gotKeys) && first < len(wantKeys) {
					gid := gotKeys[first]
					wid := wantKeys[first]
					gv, _ := db.Get(gid)
					wv, _ := db.Get(wid)
					if queryTestOrderIsArrayPosOnField(q, "country") {
						vals := queryTestOrderValues(q)
						var gm, wm []string
						for _, country := range vals {
							ids := db.engine.snapshot.Current().FieldLookupPostingRetained("country", country)
							gHas := ids.Contains(gid)
							wHas := ids.Contains(wid)
							gm = append(gm, fmt.Sprintf("%s=%v", country, gHas))
							wm = append(wm, fmt.Sprintf("%s=%v", country, wHas))
						}
						countryDebug := func(country string) string {
							qSimple := qx.Query(qx.AND(
								qx.EQ("active", true),
								qx.EQ("country", country),
							))
							gotIDs, gerr := db.QueryKeys(qSimple)
							wantIDs, werr := expectedKeysUint64(t, db, qSimple)
							if gerr != nil || werr != nil {
								return fmt.Sprintf("%s: gotErr=%v wantErr=%v", country, gerr, werr)
							}
							if len(gotIDs) > 8 {
								gotIDs = gotIDs[:8]
							}
							if len(wantIDs) > 8 {
								wantIDs = wantIDs[:8]
							}
							return fmt.Sprintf("%s: got=%v want=%v", country, gotIDs, wantIDs)
						}
						extra = fmt.Sprintf(
							"\nfirst_mismatch=%d gotID=%d wantID=%d\ngotRec=%#v countryMembership={%s}\nwantRec=%#v countryMembership={%s}\ncountryDebug:\n%s\n%s",
							first,
							gid,
							wid,
							gv,
							strings.Join(gm, ","),
							wv,
							strings.Join(wm, ","),
							countryDebug("NL"),
							countryDebug("Finland"),
						)
					} else {
						var gl, wl int
						if gv != nil {
							gl = distinctCountStrings(gv.Tags)
						}
						if wv != nil {
							wl = distinctCountStrings(wv.Tags)
						}
						var gm, wm []string
						for _, tag := range []string{"rust", "java", "infra", "go", "db"} {
							ids := db.engine.snapshot.Current().FieldLookupPostingRetained("tags", tag)
							gHas := ids.Contains(gid)
							wHas := ids.Contains(wid)
							gm = append(gm, fmt.Sprintf("%s=%v", tag, gHas))
							wm = append(wm, fmt.Sprintf("%s=%v", tag, wHas))
						}
						extra = fmt.Sprintf(
							"\nfirst_mismatch=%d gotID=%d wantID=%d\ngotRec=%#v gotDistinctTags=%d tagMembership={%s}\nwantRec=%#v wantDistinctTags=%d tagMembership={%s}",
							first, gid, wid, gv, gl, strings.Join(gm, ","), wv, wl, strings.Join(wm, ","),
						)
					}
				}
				t.Fatalf("step=%d qi=%d keys mismatch q=%+v got=%v want=%v%s\nops:\n%s", step, qi, q, gotKeys, wantKeys, extra, strings.Join(opsLog, "\n"))
			}

			gotItems, err := db.Query(q)
			if err != nil {
				t.Fatalf("step=%d qi=%d Query: %v q=%+v", step, qi, err, q)
			}
			wantItems, err := db.BatchGet(wantKeys...)
			if err != nil {
				t.Fatalf("step=%d qi=%d BatchGet(wantKeys): %v", step, qi, err)
			}
			if len(gotItems) != len(wantItems) {
				t.Fatalf("step=%d qi=%d items len mismatch q=%+v got=%d want=%d", step, qi, q, len(gotItems), len(wantItems))
			}
			for i := range wantItems {
				if gotItems[i] == nil || wantItems[i] == nil {
					t.Fatalf("step=%d qi=%d nil item at i=%d q=%+v got=%#v want=%#v", step, qi, i, q, gotItems[i], wantItems[i])
				}
				if !reflect.DeepEqual(*gotItems[i], *wantItems[i]) {
					t.Fatalf("step=%d qi=%d item mismatch at i=%d q=%+v got=%#v want=%#v", step, qi, i, q, gotItems[i], wantItems[i])
				}
			}

			qNoPage := *q
			qNoPage.Window.Offset = 0
			qNoPage.Window.Limit = 0
			wantAllKeys, err := expectedKeysUint64(t, db, &qNoPage)
			if err != nil {
				t.Fatalf("step=%d qi=%d expectedKeys(all): %v q=%+v", step, qi, err, q)
			}
			cnt, err := db.Count(q.Filter)
			if err != nil {
				t.Fatalf("step=%d qi=%d Count: %v q=%+v", step, qi, err, q)
			}
			if cnt != uint64(len(wantAllKeys)) {
				t.Fatalf("step=%d qi=%d count mismatch q=%+v got=%d want=%d", step, qi, q, cnt, len(wantAllKeys))
			}
		}
	}
}

func TestQuery_Metamorphic_NormalizeAndNoiseEquivalence(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 20_000)

	tests := []struct {
		name string
		q    *qx.QX
	}{
		{
			name: "OR_Order_Offset",
			q: qx.Query(
				qx.OR(
					qx.AND(
						qx.EQ("active", true),
						qx.IN("country", []string{"NL", "DE", "PL"}),
						qx.GTE("score", 30.0),
					),
					qx.AND(
						qx.PREFIX("full_name", "FN-1"),
						qx.NOTIN("country", []string{"Thailand"}),
						qx.GTE("age", 20),
					),
					qx.AND(
						qx.HASANY("tags", []string{"go", "db"}),
						qx.NE("name", "alice"),
					),
				),
			).Sort("score", qx.DESC).Offset(300).Limit(120),
		},
		{
			name: "AND_NoOrder_Complex",
			q: qx.Query(
				qx.EQ("active", true),
				qx.NOTIN("country", []string{"NL", "PL"}),
				qx.HASANY("tags", []string{"go", "ops"}),
				qx.GTE("age", 22),
			),
		},
		{
			name: "AutocompleteLike",
			q: qx.Query(
				qx.PREFIX("full_name", "FN-1"),
				qx.EQ("active", true),
				qx.NOTIN("country", []string{"NL"}),
			).Sort("score", qx.DESC).Limit(80),
		},
		{
			name: "OrderRange",
			q: qx.Query(
				qx.GTE("age", 25),
				qx.LTE("age", 40),
				qx.GT("score", 20.0),
			).Sort("score", qx.DESC).Offset(100).Limit(90),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			base := runQueryKeysChecked(t, db, tc.q)
			normalizedQ := normalizeQueryForTest(tc.q)
			normalized := runQueryKeysChecked(t, db, normalizedQ)
			assertQueryIDsEqual(t, tc.q, base, normalized)

			noisyQ := withNoisyEquivalentQuery(tc.q, len(tc.name))
			noisy := runQueryKeysChecked(t, db, noisyQ)
			assertQueryIDsEqual(t, tc.q, base, noisy)
		})
	}
}

func TestQuery_Metamorphic_RandomizedProfiles_RouteEquivalence(t *testing.T) {
	profiles := []metamorphicDataProfile{
		{
			name:        "Uniform",
			scoreLevels: 50_000,
			activeTrue:  0.50,
			hotCountryP: 0.15,
			hotTagP:     0.30,
		},
		{
			name:        "Skewed",
			scoreLevels: 30_000,
			activeTrue:  0.88,
			hotCountryP: 0.75,
			hotTagP:     0.85,
		},
		{
			name:        "HighCardOrder",
			scoreLevels: 100_000,
			activeTrue:  0.52,
			hotCountryP: 0.20,
			hotTagP:     0.35,
		},
		{
			name:        "LowCardOrder",
			scoreLevels: 16,
			activeTrue:  0.55,
			hotCountryP: 0.30,
			hotTagP:     0.50,
		},
	}

	for pi := range profiles {
		p := profiles[pi]
		t.Run(p.name, func(t *testing.T) {
			t.Parallel()

			db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
			seedMetamorphicDataProfile(t, db, 8_000, p)

			r := newRand(777 + int64(pi)*1000)
			const queryCount = 70

			for i := 0; i < queryCount; i++ {
				q := randomMetamorphicQuery(r)

				countQ := cloneQuery(q)
				countQ.Order = nil
				countQ.Window.Offset = 0
				countQ.Window.Limit = 0

				wantCountKeys, err := expectedKeysUint64(t, db, countQ)
				if err != nil {
					t.Fatalf(
						"expectedKeysUint64(count profile=%s i=%d): %v\nq=%+v",
						p.name, i, err, q,
					)
				}

				base := runQueryKeysChecked(t, db, q)

				nq := normalizeQueryForTest(q)
				normalized := runQueryKeysChecked(t, db, nq)
				if queryContractNoOrderWindow(q) {
					assertNoOrderWindowSubset(t, q, base, wantCountKeys, fmt.Sprintf("base profile=%s i=%d", p.name, i))
					assertNoOrderWindowSubset(t, nq, normalized, wantCountKeys, fmt.Sprintf("normalize profile=%s i=%d", p.name, i))
				} else if !queryIDsEqual(q, base, normalized) {
					t.Fatalf("normalize mismatch (profile=%s, i=%d):\nq=%+v\nnq=%+v\nbase=%v\nnorm=%v", p.name, i, q, nq, base, normalized)
				}

				noisyQ := withNoisyEquivalentQuery(q, i)
				noisy := runQueryKeysChecked(t, db, noisyQ)
				if queryContractNoOrderWindow(q) {
					assertNoOrderWindowSubset(t, noisyQ, noisy, wantCountKeys, fmt.Sprintf("noisy profile=%s i=%d", p.name, i))
				} else {
					assertQueryIDsEqual(t, q, base, noisy)
				}

				_, prepared, _, _ := assertPreparedRouteEquivalence(t, db, q)
				if queryContractNoOrderWindow(q) {
					assertNoOrderWindowSubset(t, q, prepared, wantCountKeys, fmt.Sprintf("prepared profile=%s i=%d", p.name, i))
				} else {
					assertQueryIDsEqual(t, q, base, prepared)
				}
				wantCount := uint64(len(wantCountKeys))

				gotCount, err := db.Count(q.Filter)
				if err != nil {
					t.Fatalf("Count(profile=%s i=%d): %v\nq=%+v", p.name, i, err, q)
				}
				if gotCount != wantCount {
					t.Fatalf(
						"count mismatch (profile=%s i=%d): got=%d want=%d\nq=%+v\ncountQ=%+v",
						p.name, i, gotCount, wantCount, q, countQ,
					)
				}

				preparedCardinality, err := db.engine.filterCardinalityForTests(normalizeQueryForTest(q).Filter)
				if err != nil {
					t.Fatalf("filterCardinalityForTests(profile=%s i=%d): %v\nq=%+v", p.name, i, err, q)
				}
				if preparedCardinality != wantCount {
					t.Fatalf(
						"prepared count mismatch (profile=%s i=%d): got=%d want=%d\nq=%+v\ncountQ=%+v",
						p.name, i, preparedCardinality, wantCount, q, countQ,
					)
				}
			}
		})
	}
}

func TestQuery_RouteEquivalence_PreparedExecutionPlanner_BaseAndMutated(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 8_000)

	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex(base): %v", err)
	}

	queries := []*qx.QX{
		qx.Query(
			qx.EQ("active", true),
			qx.GTE("age", 22),
			qx.LT("age", 50),
		).Sort("age", qx.ASC).Limit(120),
		qx.Query(
			qx.PREFIX("full_name", "FN-1"),
			qx.EQ("active", true),
		).Sort("full_name", qx.ASC).Limit(90),
		qx.Query(
			qx.OR(
				qx.EQ("active", true),
				qx.EQ("name", "alice"),
			),
		).Limit(140),
		qx.Query(
			qx.OR(
				qx.AND(
					qx.EQ("active", true),
					qx.HASANY("tags", []string{"go", "ops"}),
				),
				qx.AND(
					qx.EQ("country", "DE"),
					qx.GTE("age", 25),
				),
			),
		).Sort("score", qx.DESC).Offset(40).Limit(90),
		queryOrderSortByArrayCount(qx.Query(
			qx.GTE("age", 20),
		), "tags", qx.DESC).Offset(7).Limit(55),
		qx.Query(
			qx.EQ("active", true),
			qx.HASANY("tags", []string{"go", "db"}),
			qx.NOTIN("country", []string{"PL", "DE"}),
		).Limit(85),
		qx.Query(
			qx.GTE("age", 24),
			qx.LTE("age", 42),
			qx.EQ("active", true),
		).Limit(95),
		qx.Query(
			qx.HASANY("tags", []string{"go", "db"}),
			qx.NOTIN("country", []string{"Thailand", "Iceland"}),
		),
	}

	runChecks := func(label string) {
		t.Helper()

		var sawExec bool
		var sawPlan bool

		for i := range queries {
			q := queries[i]
			nq, ref, usedExec, usedPlan := assertPreparedRouteEquivalence(t, db, q)
			sawExec = sawExec || usedExec
			sawPlan = sawPlan || usedPlan

			got, err := db.QueryKeys(q)
			if err != nil {
				t.Fatalf("%s q%d QueryKeys: %v", label, i, err)
			}

			want, err := expectedKeysUint64(t, db, q)
			if err != nil {
				t.Fatalf("%s q%d expectedKeysUint64: %v", label, i, err)
			}

			if len(q.Order) == 0 && (q.Window.Limit > 0 || q.Window.Offset > 0) {
				fullQ := cloneQuery(q)
				fullQ.Window.Offset = 0
				fullQ.Window.Limit = 0
				fullWant, err := expectedKeysUint64(t, db, fullQ)
				if err != nil {
					t.Fatalf("%s q%d expectedKeysUint64(full): %v", label, i, err)
				}
				assertNoOrderWindowSubset(t, q, got, fullWant, fmt.Sprintf("%s q%d QueryKeys", label, i))
				assertNoOrderWindowSubset(t, nq, ref, fullWant, fmt.Sprintf("%s q%d prepared", label, i))
			} else {
				assertQueryIDsEqual(t, q, got, want)
				assertQueryIDsEqual(t, nq, ref, want)
			}

			if q.Window.Limit == 0 && q.Window.Offset == 0 {
				cnt, err := db.engine.filterCardinalityForTests(nq.Filter)
				if err != nil {
					t.Fatalf("%s q%d filterCardinalityForTests: %v", label, i, err)
				}
				if cnt != uint64(len(ref)) {
					t.Fatalf("%s q%d count mismatch on prepared route: got=%d want=%d", label, i, cnt, len(ref))
				}
			}
		}

		if !sawExec {
			t.Fatalf("%s: expected at least one execution fast-path route", label)
		}
		if !sawPlan {
			t.Fatalf("%s: expected at least one planner fast-path route", label)
		}
	}

	runChecks("base")

	r := newRand(20260227)
	countries := []string{"NL", "PL", "DE", "Finland", "Iceland", "Thailand", "US"}
	tagPool := []string{"go", "db", "ops", "rust", "java", "infra"}
	names := []string{"alice", "albert", "bob", "carol", "dave", "eve"}
	for i := 0; i < 320; i++ {
		id := uint64(1 + r.IntN(10_500))
		switch r.IntN(3) {
		case 0:
			name := names[r.IntN(len(names))]
			rec := &Rec{
				Meta:     Meta{Country: countries[r.IntN(len(countries))]},
				Name:     name,
				Email:    fmt.Sprintf("%s-%d@example.test", name, id),
				Age:      18 + r.IntN(65),
				Score:    float64(r.IntN(2_000))/10.0 + r.Float64()*0.001,
				Active:   r.IntN(2) == 0,
				Tags:     []string{tagPool[r.IntN(len(tagPool))], tagPool[r.IntN(len(tagPool))]},
				FullName: fmt.Sprintf("FN-%05d", 1+r.IntN(15_000)),
			}
			if err := db.Set(id, rec); err != nil {
				t.Fatalf("mutated Set(id=%d): %v", id, err)
			}
		case 1:
			patch := []Field{{Name: "age", Value: float64(18 + r.IntN(65))}}
			if err := db.Patch(id, patch); err != nil {
				t.Fatalf("mutated Patch(id=%d): %v", id, err)
			}
		default:
			if err := db.Delete(id); err != nil {
				t.Fatalf("mutated Delete(id=%d): %v", id, err)
			}
		}
	}

	runChecks("mutated")
}

func TestNormalize_WrappedQueryMatchesDirectResults(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 10_000)

	direct := qx.Query(
		qx.GTE("age", 18),
		qx.EQ("active", true),
	).Sort("score", qx.DESC).Offset(500).Limit(100)

	wrapped := &qx.QX{
		Filter: qx.OR(
			direct.Filter,
			qx.NOT(qx.Expr{}),
		),
		Order:  direct.Order,
		Window: direct.Window,
	}

	gotDirect, err := db.QueryKeys(direct)
	if err != nil {
		t.Fatalf("QueryKeys(direct): %v", err)
	}
	gotWrapped, err := db.QueryKeys(wrapped)
	if err != nil {
		t.Fatalf("QueryKeys(wrapped): %v", err)
	}

	if !slices.Equal(gotWrapped, gotDirect) {
		t.Fatalf("results mismatch:\n wrapped=%v\n direct=%v", gotWrapped, gotDirect)
	}
}
