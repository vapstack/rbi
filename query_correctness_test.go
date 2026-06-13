package rbi

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/vapstack/qx"
	"go.etcd.io/bbolt"
)

func TestAPI_Query_ReturnedRecordsDetachedFromStore(t *testing.T) {
	db, _ := openTempDBUint64(t)

	mustSetAPIRec(t, db, 1, &Rec{Name: "alice", Age: 30, Tags: []string{"go", "db"}})

	items, err := db.Query(qx.Query(qx.EQ("age", 30)))
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(items) != 1 || items[0] == nil {
		t.Fatalf("unexpected query result: %#v", items)
	}

	items[0].Name = "mutated"
	items[0].Tags[0] = "changed"
	db.ReleaseRecords(items...)

	again, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	defer releaseUniqueRecords(db, again)
	if again == nil || again.Name != "alice" || again.Tags[0] != "go" {
		t.Fatalf("stored value changed after Query result mutation: %#v", again)
	}

	keys, err := db.QueryKeys(qx.Query(qx.EQ("name", "alice")))
	if err != nil {
		t.Fatalf("QueryKeys(alice): %v", err)
	}
	if !slices.Equal(keys, []uint64{1}) {
		t.Fatalf("query index changed after Query result release: %v", keys)
	}
	keys, err = db.QueryKeys(qx.Query(qx.EQ("name", "mutated")))
	if err != nil {
		t.Fatalf("QueryKeys(mutated): %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("released Query result leaked into index: %v", keys)
	}
}

func TestAPI_Query_ReturnOrderMatchesQueryKeys(t *testing.T) {
	db, _ := openTempDBUint64(t)

	mustSetAPIRecs(t, db, map[uint64]*Rec{
		1: {Name: "id-1", Age: 20},
		2: {Name: "id-2", Age: 40},
		3: {Name: "id-3", Age: 30},
	})

	q := qx.Query(qx.GTE("age", 20)).Sort("age", qx.DESC)

	keys, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	items, err := db.Query(q)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer releaseUniqueRecords(db, items...)

	if len(keys) != len(items) {
		t.Fatalf("Query/QueryKeys length mismatch: keys=%v items=%d", keys, len(items))
	}

	wantNames := map[uint64]string{
		1: "id-1",
		2: "id-2",
		3: "id-3",
	}
	for i, id := range keys {
		if items[i] == nil || items[i].Name != wantNames[id] {
			t.Fatalf("position %d mismatch: key=%d item=%#v", i, id, items[i])
		}
	}
}

func TestAPI_Count_IgnoresOrderOffsetLimit(t *testing.T) {
	db, _ := openTempDBUint64(t)

	mustSetAPIRecs(t, db, map[uint64]*Rec{
		1: {Name: "a", Age: 18},
		2: {Name: "b", Age: 25},
		3: {Name: "c", Age: 30},
		4: {Name: "d", Age: 35},
	})

	got, err := db.Count(qx.Query(qx.GTE("age", 25)).Sort("age", qx.ASC).Offset(2).Limit(1).Filter)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if got != 3 {
		t.Fatalf("expected full match count=3, got %d", got)
	}
}

func TestAPI_Count_IgnoresInvalidOrderField(t *testing.T) {
	db, _ := openTempDBUint64(t)

	mustSetAPIRecs(t, db, map[uint64]*Rec{
		1: {Name: "a", Age: 25},
		2: {Name: "b", Age: 30},
	})

	got, err := db.Count(qx.Query(qx.GTE("age", 25)).Sort("does_not_exist", qx.ASC).Offset(1).Limit(1).Filter)
	if err != nil {
		t.Fatalf("Count should ignore order fields entirely, got err=%v", err)
	}
	if got != 2 {
		t.Fatalf("expected count=2, got %d", got)
	}
}

func TestQuery_MissingBucket_EmptyIndexResultStillRequiresSequenceTx(t *testing.T) {
	db, _ := openTempDBUint64(t)
	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.Bolt().Update(func(tx *bbolt.Tx) error {
		if tx.Bucket(db.BucketName()) == nil {
			return nil
		}
		return tx.DeleteBucket(db.BucketName())
	}); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}

	items, err := db.Query(qx.Query(qx.EQ("age", 999_999)))
	if err == nil {
		t.Fatalf("expected missing bucket error, got items=%#v", items)
	}
}

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

	for i := range queries {
		q := queries[i]

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
		} else {
			if !queryStringIDsEqual(q, got, want) {
				t.Fatalf("q%d QueryKeys mismatch:\n got=%v\nwant=%v", i, got, want)
			}
		}
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
							gHas := gv != nil && gv.Country == country
							wHas := wv != nil && wv.Country == country
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
							gHas := gv != nil && slices.Contains(gv.Tags, tag)
							wHas := wv != nil && slices.Contains(wv.Tags, tag)
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

			}
		})
	}
}

func TestQuery_RouteEquivalence_PreparedExecutionPlanner_BaseAndMutated(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 8_000)

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

		for i := range queries {
			q := queries[i]

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
			} else {
				assertQueryIDsEqual(t, q, got, want)
			}

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
			testFalseFilterExpr(),
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

type queryMetamorphicTransform struct {
	name  string
	apply func(*qx.QX) (*qx.QX, bool)
}

type queryMetamorphicCase struct {
	name string
	q    *qx.QX
}

type queryMetamorphicSource struct {
	name  string
	open  func(*testing.T) *DB[uint64, Rec]
	cases func() []queryMetamorphicCase
}

type queryMetamorphicBaseline[K ~uint64 | ~string] struct {
	q        *qx.QX
	exact    bool
	keys     []K
	fullKeys []K
}

func newQueryMetamorphicBaseline[K ~uint64 | ~string](
	c queryContract[K],
	q *qx.QX,
) queryMetamorphicBaseline[K] {
	c.t.Helper()
	ref := c.assertAllReadPathsMatchReference(q)

	base := queryMetamorphicBaseline[K]{
		q:     cloneQuery(q),
		exact: !queryContractNoOrderWindow(q),
	}
	if base.exact {
		base.keys = ref.page(c, q)
		return base
	}

	base.fullKeys = ref.full(c, q)
	return base
}

func (c queryContract[K]) AssertMetamorphicEquivalent(
	base queryMetamorphicBaseline[K],
	label string,
	q *qx.QX,
) {
	c.t.Helper()
	ref := c.assertAllReadPathsMatchReference(q)

	if base.exact {
		got := ref.page(c, q)
		if !c.equal(base.q, base.keys, got) {
			c.t.Fatalf(
				"%s exact equivalence mismatch:\nbase=%+v\nq=%+v\nbaseKeys=%v\ngot=%v",
				label, base.q, q, base.keys, got,
			)
		}
		return
	}

	got := ref.full(c, q)
	fullQ := cloneQuery(base.q)
	clearQueryOrderWindowForTest(fullQ)
	if !c.equal(fullQ, base.fullKeys, got) {
		c.t.Fatalf(
			"%s full-set equivalence mismatch:\nbase=%+v\nq=%+v\nbaseFull=%v\ngotFull=%v",
			label, base.q, q, base.fullKeys, got,
		)
	}
}

func queryMetamorphicTransforms() []queryMetamorphicTransform {
	return []queryMetamorphicTransform{
		{
			name: "Normalize",
			apply: func(q *qx.QX) (*qx.QX, bool) {
				return normalizeQueryForTest(q), true
			},
		},
		{
			name: "AndTrue",
			apply: func(q *qx.QX) (*qx.QX, bool) {
				return withNoisyEquivalentQuery(q, 0), true
			},
		},
		{
			name: "OrFalse",
			apply: func(q *qx.QX) (*qx.QX, bool) {
				return withNoisyEquivalentQuery(q, 1), true
			},
		},
		{
			name: "DuplicateAND",
			apply: func(q *qx.QX) (*qx.QX, bool) {
				out := cloneQuery(q)
				left := cloneMetamorphicFilter(q.Filter)
				right := cloneMetamorphicFilter(q.Filter)
				out.Filter = qx.AND(left, right)
				return out, true
			},
		},
		{
			name: "DuplicateOR",
			apply: func(q *qx.QX) (*qx.QX, bool) {
				out := cloneQuery(q)
				left := cloneMetamorphicFilter(q.Filter)
				right := cloneMetamorphicFilter(q.Filter)
				out.Filter = qx.OR(left, right)
				return out, true
			},
		},
		{
			name: "DoubleNegation",
			apply: func(q *qx.QX) (*qx.QX, bool) {
				out := cloneQuery(q)
				out.Filter = qx.NOT(qx.NOT(cloneMetamorphicFilter(q.Filter)))
				return out, true
			},
		},
		{
			name: "PermuteAND",
			apply: func(q *qx.QX) (*qx.QX, bool) {
				filter, ok := metamorphicReverseBoolArgs(q.Filter, qx.OpAND)
				if !ok {
					return nil, false
				}
				out := cloneQuery(q)
				out.Filter = filter
				return out, true
			},
		},
		{
			name: "PermuteOR",
			apply: func(q *qx.QX) (*qx.QX, bool) {
				filter, ok := metamorphicReverseBoolArgs(q.Filter, qx.OpOR)
				if !ok {
					return nil, false
				}
				out := cloneQuery(q)
				out.Filter = filter
				return out, true
			},
		},
		{
			name:  "DeMorgan",
			apply: metamorphicApplyDeMorgan,
		},
	}
}

func cloneMetamorphicFilter(expr qx.Expr) qx.Expr {
	if expr.IsZero() {
		return testTrueFilterExpr()
	}
	return cloneMetamorphicExpr(expr)
}

func cloneMetamorphicExpr(expr qx.Expr) qx.Expr {
	out := expr
	if len(expr.Args) == 0 {
		return out
	}

	out.Args = make([]qx.Expr, len(expr.Args))
	for i := range expr.Args {
		out.Args[i] = cloneMetamorphicExpr(expr.Args[i])
	}
	return out
}

func metamorphicReverseBoolArgs(expr qx.Expr, op string) (qx.Expr, bool) {
	out := expr
	if len(expr.Args) == 0 {
		return out, false
	}

	reverse := expr.Kind == qx.KindOP && expr.Name == op && len(expr.Args) > 1
	out.Args = make([]qx.Expr, len(expr.Args))
	changed := reverse
	for i := range out.Args {
		src := i
		if reverse {
			src = len(expr.Args) - 1 - i
		}
		child, childChanged := metamorphicReverseBoolArgs(expr.Args[src], op)
		out.Args[i] = child
		if childChanged {
			changed = true
		}
	}
	return out, changed
}

func metamorphicApplyDeMorgan(q *qx.QX) (*qx.QX, bool) {
	if q == nil {
		return nil, false
	}
	if q.Filter.Kind != qx.KindOP || len(q.Filter.Args) < 2 {
		return nil, false
	}

	var inner qx.Expr
	switch q.Filter.Name {
	case qx.OpAND:
		args := make([]qx.Expr, len(q.Filter.Args))
		for i := range q.Filter.Args {
			args[i] = qx.NOT(cloneMetamorphicExpr(q.Filter.Args[i]))
		}
		inner = qx.OR(args...)
	case qx.OpOR:
		args := make([]qx.Expr, len(q.Filter.Args))
		for i := range q.Filter.Args {
			args[i] = qx.NOT(cloneMetamorphicExpr(q.Filter.Args[i]))
		}
		inner = qx.AND(args...)
	default:
		return nil, false
	}

	out := cloneQuery(q)
	out.Filter = qx.NOT(inner)
	return out, true
}

func queryMetamorphicSmallWorldCases() []queryMetamorphicCase {
	exprs := smallWorldExprCases()
	orders := smallWorldOrderCases()
	windows := smallWorldWindowCases()
	cases := make([]queryMetamorphicCase, 0, len(exprs)*len(orders)*len(windows))
	for _, exprCase := range exprs {
		for _, orderCase := range orders {
			for _, windowCase := range windows {
				cases = append(cases, queryMetamorphicCase{
					name: fmt.Sprintf("%s/%s/%s", exprCase.name, orderCase.name, windowCase.name),
					q:    buildSmallWorldQuery(exprCase, orderCase, windowCase),
				})
			}
		}
	}
	return cases
}

func queryMetamorphicSeededCases() []queryMetamorphicCase {
	return []queryMetamorphicCase{
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
}

func queryMetamorphicSkewedNotInCases() []queryMetamorphicCase {
	return []queryMetamorphicCase{
		{
			name: "CapturedShape",
			q:    capturedNotInOrderOffsetQuery(),
		},
		{
			name: "DifferentValues",
			q: qx.Query(
				qx.NOTIN("country", []string{"DE", "PL"}),
				qx.NOTIN("country", []string{"Thailand", "US"}),
			).Sort("score", qx.ASC).Offset(210).Limit(90),
		},
		{
			name: "DescOrder",
			q: qx.Query(
				qx.NOTIN("country", []string{"Iceland", "Finland"}),
				qx.NOTIN("country", []string{"Iceland", "DE"}),
			).Sort("score", qx.DESC).Offset(446).Limit(70),
		},
		{
			name: "WithoutOffset",
			q: qx.Query(
				qx.NOTIN("country", []string{"Iceland", "Finland"}),
				qx.NOTIN("country", []string{"Iceland", "DE"}),
			).Sort("score", qx.ASC).Limit(70),
		},
	}
}

func queryMetamorphicUniformProfileCases() []queryMetamorphicCase {
	return []queryMetamorphicCase{
		{
			name: "NoOrderLimit",
			q: qx.Query(
				qx.HASALL("tags", []string{"db", "rust"}),
				qx.HASALL("tags", []string{"db", "infra"}),
				qx.HASANY("tags", []string{"infra", "rust"}),
			).Limit(120),
		},
		{
			name: "NoOrderOffsetLimit",
			q: qx.Query(
				qx.HASALL("tags", []string{"db", "rust"}),
				qx.HASALL("tags", []string{"db", "infra"}),
				qx.HASANY("tags", []string{"infra", "rust"}),
			).Offset(40).Limit(80),
		},
		{
			name: "OrderedOffsetLimit",
			q: qx.Query(
				qx.HASALL("tags", []string{"db", "rust"}),
				qx.HASALL("tags", []string{"db", "infra"}),
				qx.HASANY("tags", []string{"infra", "rust"}),
			).Sort("age", qx.ASC).Offset(20).Limit(90),
		},
	}
}

func queryMetamorphicCuratedSources() []queryMetamorphicSource {
	return []queryMetamorphicSource{
		{
			name: "Seeded",
			open: func(t *testing.T) *DB[uint64, Rec] {
				db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
				_ = seedData(t, db, 8_000)
				return db
			},
			cases: queryMetamorphicSeededCases,
		},
		{
			name:  "SkewedNotIn",
			open:  openSkewedNotInRegressionDB,
			cases: queryMetamorphicSkewedNotInCases,
		},
		{
			name: "UniformProfile",
			open: func(t *testing.T) *DB[uint64, Rec] {
				db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
				seedMetamorphicDataProfile(t, db, 8_000, metamorphicDataProfile{
					name:        "Uniform",
					scoreLevels: 50_000,
					activeTrue:  0.50,
					hotCountryP: 0.15,
					hotTagP:     0.30,
				})
				return db
			},
			cases: queryMetamorphicUniformProfileCases,
		},
	}
}

func runQueryMetamorphicTransforms(
	t *testing.T,
	db *DB[uint64, Rec],
	tc queryMetamorphicCase,
) {
	t.Helper()
	baseQ := cloneQuery(tc.q)
	contract := newUint64QueryContract(t, db)
	baseline := newQueryMetamorphicBaseline(contract, baseQ)
	for _, transform := range queryMetamorphicTransforms() {
		derived, ok := transform.apply(baseQ)
		if !ok {
			continue
		}
		contract.AssertMetamorphicEquivalent(
			baseline,
			fmt.Sprintf("%s/%s", tc.name, transform.name),
			derived,
		)
	}
}

func runQueryMetamorphicExecutionRounds(
	t *testing.T,
	db *DB[uint64, Rec],
	tc queryMetamorphicCase,
) {
	t.Helper()
	baseQ := cloneQuery(tc.q)
	contract := newUint64QueryContract(t, db)
	baseline := newQueryMetamorphicBaseline(contract, baseQ)

	contract.AssertMetamorphicEquivalent(baseline, tc.name+"/WarmCacheRoundTrip", baseQ)

	if err := db.RefreshPlannerStats(); err != nil {
		t.Fatalf("RefreshPlannerStats(%s): %v", tc.name, err)
	}
	contract.AssertMetamorphicEquivalent(baseline, tc.name+"/RefreshPlannerStatsRoundTrip", baseQ)
}

func TestQueryMetamorphic_SmallWorldTransforms(t *testing.T) {
	for _, world := range smallWorldCases() {
		t.Run(world.name, func(t *testing.T) {
			t.Parallel()

			db := openSmallWorldDB(t, world)
			for _, tc := range queryMetamorphicSmallWorldCases() {
				t.Run(tc.name, func(t *testing.T) {
					runQueryMetamorphicTransforms(t, db, tc)
				})
			}
		})
	}
}

func TestQueryMetamorphic_CuratedCorpusTransforms(t *testing.T) {
	for _, source := range queryMetamorphicCuratedSources() {
		t.Run(source.name, func(t *testing.T) {
			t.Parallel()

			db := source.open(t)
			for _, tc := range source.cases() {
				t.Run(tc.name, func(t *testing.T) {
					runQueryMetamorphicTransforms(t, db, tc)
				})
			}
		})
	}
}

func TestQueryMetamorphic_CuratedExecutionRounds(t *testing.T) {
	for _, source := range queryMetamorphicCuratedSources() {
		t.Run(source.name, func(t *testing.T) {
			t.Parallel()

			db := source.open(t)
			for _, tc := range source.cases() {
				t.Run(tc.name, func(t *testing.T) {
					runQueryMetamorphicExecutionRounds(t, db, tc)
				})
			}
		})
	}
}

type smallWorldRow struct {
	id  uint64
	rec *Rec
}

type smallWorldCase struct {
	name string
	rows []smallWorldRow
}

type smallWorldExprCase struct {
	name     string
	noFilter bool
	expr     qx.Expr
}

type smallWorldOrderCase struct {
	name  string
	apply func(*qx.QX)
}

type smallWorldWindowCase struct {
	name  string
	apply func(*qx.QX)
}

func smallWorldCases() []smallWorldCase {
	return []smallWorldCase{
		{
			name: "Empty",
		},
		{
			name: "SingleNil",
			rows: []smallWorldRow{
				{
					id: 1,
					rec: &Rec{
						Meta:     Meta{Country: ""},
						Name:     "solo",
						Email:    "solo@example.test",
						Age:      0,
						Score:    0,
						Active:   false,
						Tags:     nil,
						FullName: "FN-000",
						Opt:      nil,
					},
				},
			},
		},
		{
			name: "Overlap",
			rows: []smallWorldRow{
				{
					id: 1,
					rec: &Rec{
						Meta:     Meta{Country: "NL"},
						Name:     "alice",
						Email:    "alice-1@example.test",
						Age:      20,
						Score:    10,
						Active:   true,
						Tags:     []string{"go", "db"},
						FullName: "FN-001",
					},
				},
				{
					id: 2,
					rec: &Rec{
						Meta:     Meta{Country: "DE"},
						Name:     "bob",
						Email:    "bob@example.test",
						Age:      30,
						Score:    20,
						Active:   false,
						Tags:     []string{"go"},
						FullName: "FN-002",
						Opt:      strPtr("opt-a"),
					},
				},
				{
					id: 3,
					rec: &Rec{
						Meta:     Meta{Country: "NL"},
						Name:     "carol",
						Email:    "carol@example.test",
						Age:      30,
						Score:    20,
						Active:   true,
						Tags:     []string{"db", "ops"},
						FullName: "FN-003",
						Opt:      strPtr(""),
					},
				},
				{
					id: 4,
					rec: &Rec{
						Meta:     Meta{Country: "PL"},
						Name:     "dave",
						Email:    "dave@example.test",
						Age:      40,
						Score:    20,
						Active:   false,
						Tags:     nil,
						FullName: "ZZ-004",
						Opt:      strPtr("opt-b"),
					},
				},
				{
					id: 5,
					rec: &Rec{
						Meta:     Meta{Country: "Finland"},
						Name:     "eve",
						Email:    "eve@example.test",
						Age:      25,
						Score:    15,
						Active:   true,
						Tags:     []string{"go", "go", "db"},
						FullName: "FN-005",
					},
				},
				{
					id: 6,
					rec: &Rec{
						Meta:     Meta{Country: "Thailand"},
						Name:     "alice",
						Email:    "alice-2@example.test",
						Age:      18,
						Score:    30,
						Active:   false,
						Tags:     []string{},
						FullName: "AA-006",
						Opt:      strPtr("opt-a"),
					},
				},
			},
		},
		{
			name: "TiesAndDuplicates",
			rows: []smallWorldRow{
				{
					id: 10,
					rec: &Rec{
						Meta:     Meta{Country: "NL"},
						Name:     "tie",
						Email:    "tie-1@example.test",
						Age:      10,
						Score:    1,
						Active:   true,
						Tags:     []string{"go"},
						FullName: "FN-tie",
					},
				},
				{
					id: 11,
					rec: &Rec{
						Meta:     Meta{Country: "NL"},
						Name:     "tie",
						Email:    "tie-2@example.test",
						Age:      10,
						Score:    1,
						Active:   true,
						Tags:     []string{"go", "db"},
						FullName: "FN-tie",
						Opt:      strPtr("opt-a"),
					},
				},
				{
					id: 12,
					rec: &Rec{
						Meta:     Meta{Country: "NL"},
						Name:     "tie",
						Email:    "tie-3@example.test",
						Age:      10,
						Score:    1,
						Active:   false,
						Tags:     []string{"db"},
						FullName: "FN-tie",
					},
				},
				{
					id: 20,
					rec: &Rec{
						Meta:     Meta{Country: "Iceland"},
						Name:     "cold",
						Email:    "cold@example.test",
						Age:      99,
						Score:    100,
						Active:   false,
						Tags:     []string{"ops"},
						FullName: "ZZ-cold",
						Opt:      strPtr("opt-z"),
					},
				},
			},
		},
	}
}

func smallWorldExprCases() []smallWorldExprCase {
	return []smallWorldExprCase{
		{name: "NoFilter", noFilter: true},
		{name: "ActiveTrue", expr: qx.EQ("active", true)},
		{name: "ActiveFalse", expr: qx.EQ("active", false)},
		{name: "NotActiveTrue", expr: qx.NOT(qx.EQ("active", true))},
		{name: "CountryEqNL", expr: qx.EQ("country", "NL")},
		{name: "CountryIN", expr: qx.IN("country", []string{"NL", "DE"})},
		{name: "CountryNOTIN", expr: qx.NOTIN("country", []string{"Thailand", "Iceland"})},
		{name: "CountrySuffixLand", expr: qx.SUFFIX("country", "land")},
		{name: "CountryContainsLand", expr: qx.CONTAINS("country", "land")},
		{name: "NameEqAlice", expr: qx.EQ("name", "alice")},
		{name: "NameNotBob", expr: qx.NE("name", "bob")},
		{name: "AgeGTE30", expr: qx.GTE("age", 30)},
		{name: "AgeRange", expr: qx.AND(qx.GTE("age", 20), qx.LTE("age", 30))},
		{name: "ScoreGT15", expr: qx.GT("score", 15.0)},
		{name: "ScoreLTE20", expr: qx.LTE("score", 20.0)},
		{name: "FullNamePrefixFN", expr: qx.PREFIX("full_name", "FN-")},
		{name: "OptEqNil", expr: qx.EQ("opt", nil)},
		{name: "OptEqValue", expr: qx.EQ("opt", "opt-a")},
		{name: "OptPrefix", expr: qx.PREFIX("opt", "opt-")},
		{name: "TagsHasGo", expr: qx.HASALL("tags", []string{"go"})},
		{name: "TagsHasAny", expr: qx.HASANY("tags", []string{"db", "ops"})},
		{name: "TagsHasAll", expr: qx.HASALL("tags", []string{"go", "db"})},
		{name: "TagsHasNone", expr: qx.HASNONE("tags", []string{"go", "ops"})},
		{name: "AndSelective", expr: qx.AND(qx.EQ("active", true), qx.EQ("country", "NL"))},
		{name: "AndRange", expr: qx.AND(qx.GTE("age", 20), qx.LTE("age", 30), qx.LTE("score", 20.0))},
		{name: "OrOverlap", expr: qx.OR(qx.EQ("country", "NL"), qx.HASALL("tags", []string{"go"}))},
		{
			name: "OrResidualBranches",
			expr: qx.OR(
				qx.AND(qx.EQ("active", true), qx.HASALL("tags", []string{"db"})),
				qx.AND(qx.EQ("active", false), qx.GTE("score", 20.0)),
			),
		},
		{name: "OrDuplicateBranch", expr: qx.OR(qx.EQ("country", "NL"), qx.EQ("country", "NL"))},
		{name: "NotGroup", expr: qx.NOT(qx.OR(qx.EQ("country", "Thailand"), qx.EQ("active", false)))},
	}
}

func smallWorldOrderCases() []smallWorldOrderCase {
	return []smallWorldOrderCase{
		{name: "NoOrder"},
		{name: "AgeAsc", apply: func(q *qx.QX) { q.Sort("age", qx.ASC) }},
		{name: "ScoreDesc", apply: func(q *qx.QX) { q.Sort("score", qx.DESC) }},
		{name: "FullNameAsc", apply: func(q *qx.QX) { q.Sort("full_name", qx.ASC) }},
		{name: "OptAsc", apply: func(q *qx.QX) { q.Sort("opt", qx.ASC) }},
		{name: "TagsPosAsc", apply: func(q *qx.QX) { queryOrderSortByArrayPos(q, "tags", []string{"go", "db", "ops"}, qx.ASC) }},
		{name: "TagsCountDesc", apply: func(q *qx.QX) { queryOrderSortByArrayCount(q, "tags", qx.DESC) }},
	}
}

func smallWorldWindowCases() []smallWorldWindowCase {
	return []smallWorldWindowCase{
		{name: "Full"},
		{name: "Limit1", apply: func(q *qx.QX) { q.Limit(1) }},
		{name: "Offset1Limit2", apply: func(q *qx.QX) { q.Offset(1).Limit(2) }},
		{name: "OffsetPastLimit3", apply: func(q *qx.QX) { q.Offset(10).Limit(3) }},
	}
}

func openSmallWorldDB(t *testing.T, world smallWorldCase) *DB[uint64, Rec] {
	t.Helper()
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	for _, row := range world.rows {
		if err := db.Set(row.id, row.rec); err != nil {
			t.Fatalf("Set(%d): %v", row.id, err)
		}
	}
	if err := db.RefreshPlannerStats(); err != nil {
		t.Fatalf("RefreshPlannerStats: %v", err)
	}
	return db
}

func buildSmallWorldQuery(exprCase smallWorldExprCase, orderCase smallWorldOrderCase, windowCase smallWorldWindowCase) *qx.QX {
	var q *qx.QX
	if exprCase.noFilter {
		q = qx.Query()
	} else {
		q = qx.Query(exprCase.expr)
	}
	if orderCase.apply != nil {
		orderCase.apply(q)
	}
	if windowCase.apply != nil {
		windowCase.apply(q)
	}
	return q
}

func TestQuerySmallWorld_BoundedExhaustiveContract(t *testing.T) {
	exprs := smallWorldExprCases()
	orders := smallWorldOrderCases()
	windows := smallWorldWindowCases()

	for _, world := range smallWorldCases() {
		t.Run(world.name, func(t *testing.T) {
			t.Parallel()

			db := openSmallWorldDB(t, world)

			for _, exprCase := range exprs {
				t.Run(exprCase.name, func(t *testing.T) {
					for _, orderCase := range orders {
						for _, windowCase := range windows {
							q := buildSmallWorldQuery(exprCase, orderCase, windowCase)
							contract := newUint64QueryContract(t, db)
							contract.assertAllReadPathsMatchReference(q)
						}
					}
				})
			}
		})
	}
}
