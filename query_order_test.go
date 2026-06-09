package rbi

import (
	"errors"
	"fmt"
	"github.com/vapstack/rbi/rbierrors"
	"slices"
	"strings"
	"testing"

	"github.com/vapstack/qx"
)

func countDistinct(s []string) int {
	n := len(s)
	switch n {
	case 0:
		return 0
	case 1:
		return 1
	case 2:
		if s[0] != s[1] {
			return 2
		}
		return 1
	case 3:
		a, b, c := s[0], s[1], s[2]
		if a == b {
			if b == c {
				return 1
			}
			return 2
		}
		if a == c || b == c {
			return 2
		}
		return 3
	}
	if n <= 8 {
		return countDistinctLinear(s, n)
	}
	return len(dedupStringsInplace(s))
}

func countDistinctLinear(s []string, n int) int {
	uniq := 0
OUTER:
	for i := 0; i < n; i++ {
		v := s[i]
		for k := 0; k < i; k++ {
			if s[k] == v {
				continue OUTER
			}
		}
		uniq++
	}
	return uniq
}

func dedupStringsInplace(s []string) []string {
	if len(s) < 2 {
		return s
	}
	slices.Sort(s)
	w := 1
	for i := 1; i < len(s); i++ {
		if s[i] != s[w-1] {
			s[w] = s[i]
			w++
		}
	}
	return s[:w]
}

func TestSort_OrderStability_WithDupValues(t *testing.T) {
	db, _ := openTempDBUint64(t)

	for i := 1; i <= 10; i++ {
		if err := db.Set(uint64(i), &Rec{Age: 20}); err != nil {
			t.Fatal(err)
		}
	}

	q := qx.Query(qx.EQ("age", 20)).Sort("age", qx.ASC)

	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < len(ids)-1; i++ {
		if ids[i] > ids[i+1] {
			t.Errorf("unstable sort for dup values at index %d: %d > %d", i, ids[i], ids[i+1])
		}
	}
}

func TestQuery_OrderVariants_AcrossSnapshots_MatchSeqScanModel(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 320)

	runChecks := func(label string, queries []*qx.QX) {
		t.Helper()
		for i, q := range queries {
			q := q
			t.Run(fmt.Sprintf("%s_q%d_%s", label, i, queryTestOrderLabel(q)), func(t *testing.T) {
				got, err := db.QueryKeys(q)
				if err != nil {
					t.Fatalf("QueryKeys: %v", err)
				}
				want, err := expectedKeysUint64(t, db, q)
				if err != nil {
					t.Fatalf("expectedKeysUint64: %v", err)
				}
				assertSameSlice(t, got, want)
			})
		}
	}

	baseQueries := []*qx.QX{
		// Base basic-order fast path (skip=0, limit<=256) and non-fast variant.
		qx.Query(qx.GTE("age", 20)).Sort("age", qx.ASC).Limit(64),
		qx.Query(qx.GTE("age", 20)).Sort("age", qx.DESC).Offset(7).Limit(41),

		// Base array order over slice field.
		queryOrderSortByArrayPos(qx.Query(qx.NOT(qx.EQ("active", false))), "tags", []string{"go", "java", "ops"}, qx.ASC).Offset(3).Limit(33),
		queryOrderSortByArrayCount(qx.Query(qx.NOT(qx.EQ("active", true))), "tags", qx.DESC).Offset(2).Limit(37),

		// ArrayPos over scalar field to cover scalar ordering variants.
		queryOrderSortByArrayPos(qx.Query(), "country", []string{"NL", "DE", "PL"}, qx.ASC).Limit(60),
		queryOrderSortByArrayPos(qx.Query(), "country", []string{"NL", "DE", "PL"}, qx.DESC),
	}
	runChecks("base", baseQueries)

	// Mutate basic/slice/scalar order fields and verify subsequent snapshots.
	if err := db.Patch(1, []Field{{Name: "age", Value: 99}}); err != nil {
		t.Fatalf("Patch(age): %v", err)
	}
	if err := db.Patch(2, []Field{{Name: "tags", Value: []string{"ops", "go"}}}); err != nil {
		t.Fatalf("Patch(tags): %v", err)
	}
	if err := db.Patch(3, []Field{{Name: "country", Value: "NL"}}); err != nil {
		t.Fatalf("Patch(country): %v", err)
	}
	if err := db.Set(1001, &Rec{
		Meta:     Meta{Country: "DE"},
		Name:     "mutated-user",
		Email:    "mutated-user@example.com",
		Age:      37,
		Score:    77.7,
		Active:   true,
		Tags:     []string{"java", "ops"},
		FullName: "mutated-user",
	}); err != nil {
		t.Fatalf("Set(mutated): %v", err)
	}
	if err := db.Delete(4); err != nil {
		t.Fatalf("Delete(mutated): %v", err)
	}

	mutatedQueries := []*qx.QX{
		qx.Query(qx.GTE("age", 20)).Sort("age", qx.ASC).Limit(64),
		qx.Query(qx.GTE("age", 20)).Sort("age", qx.DESC).Offset(7).Limit(41),
		queryOrderSortByArrayPos(qx.Query(qx.NOT(qx.EQ("active", false))), "tags", []string{"go", "java", "ops"}, qx.DESC).Offset(3).Limit(33),
		queryOrderSortByArrayCount(qx.Query(qx.NOT(qx.EQ("active", true))), "tags", qx.ASC).Offset(2).Limit(37),
		queryOrderSortByArrayPos(qx.Query(), "country", []string{"NL", "DE", "PL"}, qx.ASC).Limit(60),
		queryOrderSortByArrayPos(qx.Query(), "country", []string{"NL", "DE", "PL"}, qx.DESC),
	}
	runChecks("mutated", mutatedQueries)
}

func TestQuery_ArrayOrder_RandomMutations_MatchSeqScan(t *testing.T) {
	cases := []struct {
		name  string
		seed  int64
		steps int
	}{
		{name: "seed_20260224", seed: 20260224, steps: 160},
		{name: "seed_20260301", seed: 20260301, steps: 120},
		{name: "seed_20260317", seed: 20260317, steps: 120},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db, _ := openTempDBUint64(t)
			_ = seedData(t, db, 420)

			r := newRand(tc.seed)
			countries := []string{"NL", "PL", "DE", "Finland", "Iceland", "Thailand", "US", "JP"}
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
					Email:    fmt.Sprintf("%s-%d@example.test", name, id),
					Age:      18 + r.IntN(60),
					Score:    float64(r.IntN(1000))/10.0 + r.Float64()*0.001,
					Active:   r.IntN(2) == 0,
					Tags:     randomTags(),
					FullName: fmt.Sprintf("FN-%05d", id),
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
					return qx.GTE("age", 18+r.IntN(35))
				case 4:
					return qx.LTE("age", 25+r.IntN(40))
				case 5:
					return qx.HASANY("tags", pickVals(tagPool, 2))
				case 6:
					return qx.PREFIX("full_name", fmt.Sprintf("FN-%d", r.IntN(3)))
				default:
					return qx.GT("score", float64(r.IntN(80)))
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

				switch r.IntN(100) {
				case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9:
					q.Window.Offset = 0
					q.Window.Limit = 0
				case 10, 11, 12, 13, 14, 15, 16, 17, 18, 19:
					q.Window.Offset = uint64(350 + r.IntN(300))
					q.Window.Limit = uint64(25 + r.IntN(60))
				default:
					q.Window.Offset = uint64(r.IntN(45))
					q.Window.Limit = uint64(1 + r.IntN(120))
				}
				return q
			}
			randomPatch := func() []Field {
				switch r.IntN(5) {
				case 0:
					return []Field{{Name: "age", Value: 18 + r.IntN(60)}}
				case 1:
					return []Field{{Name: "country", Value: countries[r.IntN(len(countries))]}}
				case 2:
					return []Field{{Name: "tags", Value: randomTags()}}
				case 3:
					return []Field{{Name: "active", Value: r.IntN(2) == 0}}
				default:
					return []Field{{Name: "score", Value: float64(r.IntN(1000))/10.0 + r.Float64()*0.001}}
				}
			}

			var opsLog []string

			for step := 0; step < tc.steps; step++ {
				id := uint64(1 + r.IntN(560))
				switch x := r.IntN(100); {
				case x < 45:
					rec := randomRec(id)
					if err := db.Set(id, rec); err != nil {
						t.Fatalf("Set(seed=%d,step=%d,id=%d): %v", tc.seed, step, id, err)
					}
					opsLog = append(opsLog, fmt.Sprintf("step=%d set id=%d tags=%v country=%s age=%d active=%v", step, id, rec.Tags, rec.Country, rec.Age, rec.Active))
				case x < 80:
					patch := randomPatch()
					if err := db.Patch(id, patch); err != nil {
						t.Fatalf("Patch(seed=%d,step=%d,id=%d): %v", tc.seed, step, id, err)
					}
					opsLog = append(opsLog, fmt.Sprintf("step=%d patch id=%d patch=%v", step, id, patch))
				default:
					if err := db.Delete(id); err != nil {
						t.Fatalf("Delete(seed=%d,step=%d,id=%d): %v", tc.seed, step, id, err)
					}
					opsLog = append(opsLog, fmt.Sprintf("step=%d delete id=%d", step, id))
				}

				for qi := 0; qi < 3; qi++ {
					q := randomQuery()
					got, err := db.QueryKeys(q)
					if err != nil {
						t.Fatalf("QueryKeys(seed=%d,step=%d,qi=%d): %v\nq=%+v", tc.seed, step, qi, err, q)
					}
					want, err := expectedKeysUint64(t, db, q)
					if err != nil {
						t.Fatalf("expectedKeysUint64(seed=%d,step=%d,qi=%d): %v\nq=%+v", tc.seed, step, qi, err, q)
					}
					if !slices.Equal(got, want) {
						first := -1
						maxCmp := len(got)
						if len(want) < maxCmp {
							maxCmp = len(want)
						}
						for i := 0; i < maxCmp; i++ {
							if got[i] != want[i] {
								first = i
								break
							}
						}
						if first == -1 && len(got) != len(want) {
							first = maxCmp
						}

						extra := ""
						if first >= 0 && first < len(got) && first < len(want) && len(q.Order) > 0 {
							if queryTestOrderIsArrayPosOnField(q, "tags") {
								gid := got[first]
								wid := want[first]
								gv, _ := db.Get(gid)
								wv, _ := db.Get(wid)
								vals := queryTestOrderValues(q)
								var bits []string
								for _, tag := range vals {
									gHas := gv != nil && slices.Contains(gv.Tags, tag)
									wHas := wv != nil && slices.Contains(wv.Tags, tag)
									bits = append(bits, fmt.Sprintf("%s:g=%v,w=%v", tag, gHas, wHas))
								}
								extra = fmt.Sprintf(
									"\nfirst_mismatch=%d gotID=%d wantID=%d\ngotRec=%#v\nwantRec=%#v\ntagMembership={%s}",
									first,
									gid,
									wid,
									gv,
									wv,
									strings.Join(bits, ", "),
								)
							} else if queryTestOrderIsArrayPosOnField(q, "country") {
								gid := got[first]
								wid := want[first]
								gv, _ := db.Get(gid)
								wv, _ := db.Get(wid)
								vals := queryTestOrderValues(q)
								var bits []string
								for _, country := range vals {
									gHas := gv != nil && gv.Country == country
									wHas := wv != nil && wv.Country == country
									bits = append(bits, fmt.Sprintf("%s:g=%v,w=%v", country, gHas, wHas))
								}
								extra = fmt.Sprintf(
									"\nfirst_mismatch=%d gotID=%d wantID=%d\ngotRec=%#v\nwantRec=%#v\ncountryMembership={%s}",
									first,
									gid,
									wid,
									gv,
									wv,
									strings.Join(bits, ", "),
								)
							}
						}
						t.Fatalf(
							"query mismatch seed=%d step=%d qi=%d q=%+v\ngot=%v\nwant=%v%s\nops:\n%s",
							tc.seed,
							step,
							qi,
							q,
							got,
							want,
							extra,
							strings.Join(opsLog, "\n"),
						)
					}
				}
			}
		})
	}
}

func TestQuery_ByArrayPos_Scalar_DuplicatePriority_BaseAndFieldIndexView(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 260)

	check := func(label string, q *qx.QX) {
		t.Helper()
		got, err := db.QueryKeys(q)
		if err != nil {
			t.Fatalf("%s QueryKeys: %v", label, err)
		}
		want, err := expectedKeysUint64(t, db, q)
		if err != nil {
			t.Fatalf("%s expectedKeysUint64: %v", label, err)
		}
		assertSameSlice(t, got, want)
		seen := make(map[uint64]struct{}, len(got))
		for _, id := range got {
			if _, ok := seen[id]; ok {
				t.Fatalf("%s duplicate id in result: %d (got=%v)", label, id, got)
			}
			seen[id] = struct{}{}
		}
	}

	baseQ := queryOrderSortByArrayPos(qx.Query(qx.EQ("active", false)), "country", []string{"NL", "NL", "PL"}, qx.ASC).Offset(3).Limit(120)
	check("base", baseQ)
	baseDescQ := queryOrderSortByArrayPos(qx.Query(qx.EQ("active", false)), "country", []string{"NL", "NL", "PL"}, qx.DESC).Offset(3).Limit(120)
	check("base-desc", baseDescQ)

	if err := db.Patch(5, []Field{{Name: "country", Value: "NL"}}); err != nil {
		t.Fatalf("Patch(5 country): %v", err)
	}
	if err := db.Patch(6, []Field{{Name: "country", Value: "PL"}}); err != nil {
		t.Fatalf("Patch(6 country): %v", err)
	}
	if err := db.Set(1001, &Rec{
		Meta:     Meta{Country: "NL"},
		Name:     "dup-priority",
		Email:    "dup-priority@example.test",
		Age:      39,
		Score:    77.7,
		Active:   false,
		Tags:     []string{"go"},
		FullName: "FN-1001",
	}); err != nil {
		t.Fatalf("Set(1001): %v", err)
	}

	overlayQ := queryOrderSortByArrayPos(qx.Query(qx.EQ("active", false)), "country", []string{"NL", "NL", "PL"}, qx.ASC).Offset(3).Limit(120)
	check("overlay", overlayQ)
	overlayDescQ := queryOrderSortByArrayPos(qx.Query(qx.EQ("active", false)), "country", []string{"NL", "NL", "PL"}, qx.DESC).Offset(3).Limit(120)
	check("overlay-desc", overlayDescQ)
}

func collectIDsByTagDistinctLen(t *testing.T, db *DB[uint64, Rec], wantLen int) []uint64 {
	t.Helper()
	ids := make([]uint64, 0, 64)
	err := db.ScanKeys(0, func(id uint64) (bool, error) {
		ids = append(ids, id)
		return true, nil
	})
	if err != nil {
		t.Fatalf("collectIDsByTagDistinctLen scan: %v", err)
	}

	out := make([]uint64, 0, len(ids))
	for _, id := range ids {
		rec, err := db.Get(id)
		if err != nil {
			t.Fatalf("collectIDsByTagDistinctLen Get(%d): %v", id, err)
		}
		if rec != nil && countDistinct(rec.Tags) == wantLen {
			out = append(out, id)
		}
		db.ReleaseRecords(rec)
	}
	return out
}

func queryOrderSortByArrayCount(q *qx.QX, field string, dir qx.OrderDirection) *qx.QX {
	return q.SortBy(qx.LEN(field), dir)
}

func queryOrderSortByArrayPos(q *qx.QX, field string, priority []string, dir qx.OrderDirection) *qx.QX {
	return q.SortBy(qx.POS(field, priority), dir)
}

func TestLenIndex_ZeroComplement_BaseQueryAndOrder(t *testing.T) {
	db, _ := openTempDBUint64(t)

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

	emptyQ := qx.Query(qx.EQ("tags", []string{}))
	gotEmpty, err := db.QueryKeys(emptyQ)
	if err != nil {
		t.Fatalf("QueryKeys(empty tags): %v", err)
	}
	wantEmpty := collectIDsByTagDistinctLen(t, db, 0)
	assertSameSlice(t, gotEmpty, wantEmpty)

	ascQ := queryOrderSortByArrayCount(qx.Query(), "tags", qx.ASC).Offset(3).Limit(120)
	gotAsc, err := db.QueryKeys(ascQ)
	if err != nil {
		t.Fatalf("QueryKeys(ASC array count): %v", err)
	}
	wantAsc, err := expectedKeysUint64(t, db, ascQ)
	if err != nil {
		t.Fatalf("expectedKeysUint64(ASC array count): %v", err)
	}
	assertSameSlice(t, gotAsc, wantAsc)

	descQ := queryOrderSortByArrayCount(qx.Query(), "tags", qx.DESC).Offset(2).Limit(100)
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

func TestQuery_ByArrayCount_AfterLenIndexEmptied(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "one", Email: "one@example.test", Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "two", Email: "two@example.test"}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}
	if err := db.Set(1, &Rec{Name: "one", Email: "one@example.test"}); err != nil {
		t.Fatalf("Set(1 empty tags): %v", err)
	}

	q := queryOrderSortByArrayCount(qx.Query(), "tags", qx.ASC)
	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys(array count after empty len index): %v", err)
	}
	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(array count after empty len index): %v", err)
	}
	assertSameSlice(t, got, want)
}

func TestQuery_ByArrayCount_ZeroComplementSkipWholeBucket(t *testing.T) {
	db, _ := openTempDBUint64(t)

	for i := 1; i <= 240; i++ {
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

	zeroCount := len(collectIDsByTagDistinctLen(t, db, 0))
	q := queryOrderSortByArrayCount(qx.Query(), "tags", qx.ASC).Offset(zeroCount + 5).Limit(80)

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

func TestLenIndex_ZeroComplement_WorksAfterPatches(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{})

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

	for i := 1; i <= 40; i++ {
		var tags []string
		if i%3 == 0 {
			tags = []string{"go"}
		}
		if err := db.Patch(uint64(i), []Field{{Name: "tags", Value: tags}}); err != nil {
			t.Fatalf("Patch(%d): %v", i, err)
		}
	}

	emptyQ := qx.Query(qx.EQ("tags", []string{}))
	gotEmpty, err := db.QueryKeys(emptyQ)
	if err != nil {
		t.Fatalf("QueryKeys(empty tags): %v", err)
	}
	wantEmpty := collectIDsByTagDistinctLen(t, db, 0)
	assertSameSlice(t, gotEmpty, wantEmpty)

	ascQ := queryOrderSortByArrayCount(qx.Query(qx.EQ("active", true)), "tags", qx.ASC).Offset(1).Limit(110)
	gotAsc, err := db.QueryKeys(ascQ)
	if err != nil {
		t.Fatalf("QueryKeys(ASC array count overlay): %v", err)
	}
	wantAsc, err := expectedKeysUint64(t, db, ascQ)
	if err != nil {
		t.Fatalf("expectedKeysUint64(ASC array count overlay): %v", err)
	}
	assertSameSlice(t, gotAsc, wantAsc)

	descQ := queryOrderSortByArrayCount(qx.Query(qx.EQ("active", false)), "tags", qx.DESC).Offset(2).Limit(110)
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

func TestLenIndex_ZeroComplement_WorksAfterInsertAndDelete(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{})

	for i := 1; i <= 180; i++ {
		rec := &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("u_%d@example.test", i),
			Age:    i,
			Active: i%2 == 0,
		}
		switch {
		case i%8 == 0:
			rec.Tags = []string{"go", "db"}
		case i%13 == 0:
			rec.Tags = []string{"rust"}
		default:
			rec.Tags = nil
		}
		if err := db.Set(uint64(i), rec); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}

	if err := db.Set(1001, &Rec{
		Name:   "u_1001",
		Email:  "u_1001@example.test",
		Age:    1001,
		Active: true,
		Tags:   nil,
	}); err != nil {
		t.Fatalf("Set(1001): %v", err)
	}
	if err := db.Set(1002, &Rec{
		Name:   "u_1002",
		Email:  "u_1002@example.test",
		Age:    1002,
		Active: false,
		Tags:   []string{"go"},
	}); err != nil {
		t.Fatalf("Set(1002): %v", err)
	}
	if err := db.Delete(1); err != nil {
		t.Fatalf("Delete(1): %v", err)
	}

	emptyQ := qx.Query(qx.EQ("tags", []string{}))
	gotEmpty, err := db.QueryKeys(emptyQ)
	if err != nil {
		t.Fatalf("QueryKeys(empty tags): %v", err)
	}
	wantEmpty := collectIDsByTagDistinctLen(t, db, 0)
	assertSameSlice(t, gotEmpty, wantEmpty)

	ascQ := queryOrderSortByArrayCount(qx.Query(qx.EQ("active", true)), "tags", qx.ASC).Offset(1).Limit(120)
	gotAsc, err := db.QueryKeys(ascQ)
	if err != nil {
		t.Fatalf("QueryKeys(ASC array count insert/delete): %v", err)
	}
	wantAsc, err := expectedKeysUint64(t, db, ascQ)
	if err != nil {
		t.Fatalf("expectedKeysUint64(ASC array count insert/delete): %v", err)
	}
	assertSameSlice(t, gotAsc, wantAsc)

	descQ := queryOrderSortByArrayCount(qx.Query(qx.EQ("active", false)), "tags", qx.DESC).Offset(2).Limit(120)
	gotDesc, err := db.QueryKeys(descQ)
	if err != nil {
		t.Fatalf("QueryKeys(DESC array count insert/delete): %v", err)
	}
	wantDesc, err := expectedKeysUint64(t, db, descQ)
	if err != nil {
		t.Fatalf("expectedKeysUint64(DESC array count insert/delete): %v", err)
	}
	assertSameSlice(t, gotDesc, wantDesc)
}

func TestQuery_OrderBy_WithNegationAndLimit(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 200)

	// order + NOT branch
	// exercise the negative ordered path directly without forcing universe materialization
	q := qx.Query(
		qx.NOT(qx.EQ("country", "NL")),
	).Sort("age", qx.ASC).Offset(3).Limit(25)

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
	db, _ := openTempDBUint64(t)

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
	).Sort("email", qx.ASC).Limit(15)

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
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 220)

	priority := []string{"go", "java", "ops"}
	q := queryOrderSortByArrayPos(qx.Query(
		qx.NOT(qx.CONTAINS("country", "land")),
	), "tags", priority, qx.ASC).Offset(2).Limit(30)

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

func TestQuery_ByArrayPos_WithNegation_NoLimit(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 220)

	priority := []string{"go", "java", "ops"}
	q := queryOrderSortByArrayPos(qx.Query(
		qx.NOT(qx.CONTAINS("country", "land")),
	), "tags", priority, qx.ASC)

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
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 220)

	q := queryOrderSortByArrayCount(qx.Query(
		qx.NOT(qx.EQ("active", true)),
	), "tags", qx.DESC).Offset(1).Limit(40)

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

func TestQuery_ByArrayCount_WithNegation_NoLimit(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 220)

	q := queryOrderSortByArrayCount(qx.Query(
		qx.NOT(qx.EQ("active", true)),
	), "tags", qx.DESC)

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

func TestQuery_BroadArrayOrder_NoLimit_MatchesExpected(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 70_000)

	queries := []*qx.QX{
		queryOrderSortByArrayPos(qx.Query(), "country", []string{
			"NL", "PL", "DE", "Finland", "Iceland", "Thailand", "Switzerland",
		}, qx.ASC),
		queryOrderSortByArrayCount(qx.Query(), "tags", qx.DESC),
	}

	for _, q := range queries {
		got, err := db.QueryKeys(q)
		if err != nil {
			t.Fatalf("QueryKeys(%+v): %v", q, err)
		}
		if len(got) < 64_000 {
			t.Fatalf("expected broad ordered result to exercise bucket materialization route, got %d rows for %+v", len(got), q)
		}

		want, err := expectedKeysUint64(t, db, q)
		if err != nil {
			t.Fatalf("expectedKeysUint64(%+v): %v", q, err)
		}
		assertSameSlice(t, got, want)
	}

	followUp := queryOrderSortByArrayCount(qx.Query(
		qx.NOT(qx.EQ("active", true)),
	), "tags", qx.DESC)
	gotFollowUp, err := db.QueryKeys(followUp)
	if err != nil {
		t.Fatalf("QueryKeys(follow-up): %v", err)
	}
	wantFollowUp, err := expectedKeysUint64(t, db, followUp)
	if err != nil {
		t.Fatalf("expectedKeysUint64(follow-up): %v", err)
	}
	assertSameSlice(t, gotFollowUp, wantFollowUp)
}

func TestQuery_BroadBasicOrder_NoLimit_MatchesExpected(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 80_000)

	queries := []*qx.QX{
		qx.Query().Sort("age", qx.ASC),
		qx.Query(
			qx.NOT(qx.EQ("country", "NL")),
		).Sort("score", qx.DESC),
	}

	for _, q := range queries {
		got, err := db.QueryKeys(q)
		if err != nil {
			t.Fatalf("QueryKeys(%+v): %v", q, err)
		}
		if len(got) < 64_000 {
			t.Fatalf("expected broad ordered result, got %d rows for %+v", len(got), q)
		}

		want, err := expectedKeysUint64(t, db, q)
		if err != nil {
			t.Fatalf("expectedKeysUint64(%+v): %v", q, err)
		}
		assertSameSlice(t, got, want)
	}

	followUp := qx.Query(
		qx.NOT(qx.EQ("active", true)),
	).Sort("age", qx.ASC)
	gotFollowUp, err := db.QueryKeys(followUp)
	if err != nil {
		t.Fatalf("QueryKeys(follow-up): %v", err)
	}
	wantFollowUp, err := expectedKeysUint64(t, db, followUp)
	if err != nil {
		t.Fatalf("expectedKeysUint64(follow-up): %v", err)
	}
	assertSameSlice(t, gotFollowUp, wantFollowUp)
}

func TestQuery_SortWithNegativeResult_NoDuplicates(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 300)

	// negative result + ORDER must preserve ordered output without duplicates
	q := qx.Query(
		qx.NOT(qx.EQ("country", "NL")),
	).Sort("age", qx.ASC).Limit(120)

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

func TestQueryExt_ArrayPosPointer_AllPriorities_ASC_IncludesNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_AllPriorities_DESC_IncludesNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", queryExtOptPriority(), qx.DESC))
}

func TestQueryExt_ArrayPosPointer_ActiveFilter_ASC_IncludesNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(qx.EQ("active", true)), "opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_ActiveFilter_DESC_IncludesNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(qx.EQ("active", true)), "opt", queryExtOptPriority(), qx.DESC))
}

func TestQueryExt_ArrayPosPointer_EqNilOnly_ASC_ReturnsNilRows(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(qx.EQ("opt", nil)), "opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_EqNilOnly_DESC_ReturnsNilRows(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(qx.EQ("opt", nil)), "opt", queryExtOptPriority(), qx.DESC))
}

func TestQueryExt_ArrayPosPointer_SkipWindowIntoNilTail_ASC(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", queryExtOptPriority(), qx.ASC).Offset(4).Limit(8))
}

func TestQueryExt_ArrayPosPointer_SkipWindowIntoNilTail_DESC(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", queryExtOptPriority(), qx.DESC).Offset(3).Limit(3))
}

func TestQueryExt_ArrayPosPointer_LimitWindowCrossesIntoNilTail_ASC(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", queryExtOptPriority(), qx.ASC).Limit(5))
}

func TestQueryExt_ArrayPosPointer_NegativePredicate_RetainsNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(qx.NOT(qx.EQ("active", true))), "opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_PatchToNil_RetainsExpandedNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	if err := db.Patch(6, []Field{{Name: "opt", Value: (*string)(nil)}}); err != nil {
		t.Fatalf("Patch(6 opt=nil): %v", err)
	}
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_PatchFromNilToValue_RetainsRemainingNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	if err := db.Patch(1, []Field{{Name: "opt", Value: "delta"}}); err != nil {
		t.Fatalf("Patch(1 opt=delta): %v", err)
	}
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", []string{"alpha", "beta", "", "gamma", "delta"}, qx.ASC))
}

func TestQueryExt_ArrayPosPointer_DeleteNonNil_RetainsNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	if err := db.Delete(6); err != nil {
		t.Fatalf("Delete(6): %v", err)
	}
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_InsertNil_RetainsNewNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	if err := db.Set(7, &Rec{Name: "nil-3", Opt: nil, Active: true}); err != nil {
		t.Fatalf("Set(7): %v", err)
	}
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_QueryValues_AllPriorities_ASC_RetainsNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtItemsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_CountMatchesOrderedResultSet_WhenUnbounded(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtCountMatchesOrderedResultSet(t, db, queryExtSortByArrayPos(qx.Query(), "opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_AllNilDataset_ASC_WithPriorities(t *testing.T) {
	db := seedQueryExtOptAllNilDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_AllNilDataset_DESC_WithPriorities(t *testing.T) {
	db := seedQueryExtOptAllNilDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", queryExtOptPriority(), qx.DESC))
}

func TestQueryExt_ArrayPosPointer_AllNilDataset_ASC_EmptyPriorities(t *testing.T) {
	db := seedQueryExtOptAllNilDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", []string{}, qx.ASC))
}

func TestQueryExt_ArrayPosPointer_ASC_EmptyPriorities_FallsBackToIDOrder(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", []string{}, qx.ASC))
}

func TestQueryExt_ArrayPosPointer_AllNilDataset_Window_EmptyPriorities(t *testing.T) {
	db := seedQueryExtOptAllNilDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", []string{}, qx.ASC).Offset(1).Limit(2))
}

func TestQueryExt_ArrayPosPointer_AllNilAfterPatches_ASC(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	for _, id := range []uint64{2, 3, 4, 6} {
		if err := db.Patch(id, []Field{{Name: "opt", Value: (*string)(nil)}}); err != nil {
			t.Fatalf("Patch(%d opt=nil): %v", id, err)
		}
	}
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_AllNilPredicate_DESC(t *testing.T) {
	db := seedQueryExtOptAllNilDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(qx.EQ("active", true)), "opt", []string{"alpha"}, qx.DESC))
}

func TestQueryExt_CountIgnoresUnknownBasicOrderField(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 64)
	assertQueryExtCountMatchesBaseQuery(t, db, qx.Query(qx.EQ("active", true)).Sort("no_such_field", qx.ASC).Offset(7).Limit(9))
}

func TestQueryExt_CountIgnoresUnknownArrayPosOrderField(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 64)
	assertQueryExtCountMatchesBaseQuery(t, db, queryExtSortByArrayPos(qx.Query(qx.EQ("active", true)), "no_such_field", []string{"a", "b"}, qx.ASC))
}

func TestQueryExt_CountIgnoresUnknownArrayCountOrderField(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 64)
	assertQueryExtCountMatchesBaseQuery(t, db, queryExtSortByArrayCount(qx.Query(qx.EQ("active", true)), "no_such_field", qx.DESC))
}

func TestQueryExt_CountIgnoresUnknownSecondaryOrderField(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 64)

	q := qx.Query(qx.EQ("active", true))
	q.Order = []qx.Order{
		{By: qx.REF("age")},
		{By: qx.REF("no_such_field"), Desc: true},
	}
	assertQueryExtCountMatchesBaseQuery(t, db, q)
}

func TestQueryExt_CountIgnoresUnknownOrderFieldOnNoopQuery(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 64)
	assertQueryExtCountMatchesBaseQuery(t, db, qx.Query().Sort("no_such_field", qx.DESC))
}

func TestQueryExt_CountIgnoresUnknownOrderFieldOnEmptyDB(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	assertQueryExtCountMatchesBaseQuery(t, db, qx.Query(qx.EQ("active", true)).Sort("no_such_field", qx.ASC))
}

func TestQueryExt_OrderBasicPointer_NilTailChurn_MatchesSeqScanAndPrepared(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	rows := map[uint64]*Rec{
		1:  {Name: "nil-a", Opt: nil, Active: true, Tags: []string{"go"}},
		2:  {Name: "aa-a", Opt: strPtr("aa"), Active: true, Tags: []string{"db"}},
		3:  {Name: "empty-a", Opt: strPtr(""), Active: false, Tags: []string{"ops"}},
		4:  {Name: "bb-a", Opt: strPtr("bb"), Active: true, Tags: []string{"rust"}},
		5:  {Name: "nil-b", Opt: nil, Active: false, Tags: []string{"go", "db"}},
		6:  {Name: "aa-b", Opt: strPtr("aa"), Active: false, Tags: []string{"ops", "db"}},
		7:  {Name: "cc-a", Opt: strPtr("cc"), Active: true, Tags: []string{"go", "ops"}},
		8:  {Name: "dd-a", Opt: strPtr("dd"), Active: true, Tags: []string{"java"}},
		9:  {Name: "nil-c", Opt: nil, Active: true, Tags: []string{"db", "db"}},
		10: {Name: "bb-b", Opt: strPtr("bb"), Active: false, Tags: []string{"go", "go"}},
		11: {Name: "empty-b", Opt: strPtr(""), Active: true, Tags: []string{"ops", "ops"}},
		12: {Name: "zz-a", Opt: strPtr("zz"), Active: false, Tags: []string{"rust", "go"}},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}

	tests := []struct {
		name string
		q    *qx.QX
	}{
		{
			name: "asc_window",
			q:    qx.Query().Sort("opt", qx.ASC).Offset(2).Limit(7),
		},
		{
			name: "desc_active_window",
			q:    qx.Query(qx.EQ("active", true)).Sort("opt", qx.DESC).Offset(1).Limit(5),
		},
		{
			name: "negated_filter_asc",
			q:    qx.Query(qx.NOT(qx.EQ("active", true))).Sort("opt", qx.ASC).Offset(1).Limit(6),
		},
	}

	check := func(step string) {
		for _, tc := range tests {
			t.Run(step+"_"+tc.name, func(t *testing.T) {
				assertQueryExtAllReadPathsMatchExpected(t, db, tc.q)
			})
		}
	}

	check("initial")

	if err := db.Patch(2, []Field{{Name: "opt", Value: (*string)(nil)}}); err != nil {
		t.Fatalf("Patch(2 opt=nil): %v", err)
	}
	if err := db.Patch(5, []Field{{Name: "opt", Value: "ab"}}); err != nil {
		t.Fatalf("Patch(5 opt=ab): %v", err)
	}
	if err := db.Patch(11, []Field{{Name: "opt", Value: (*string)(nil)}}); err != nil {
		t.Fatalf("Patch(11 opt=nil): %v", err)
	}
	check("after_nil_flip")

	if err := db.Patch(1, []Field{{Name: "opt", Value: ""}}); err != nil {
		t.Fatalf("Patch(1 opt=''): %v", err)
	}
	if err := db.Delete(4); err != nil {
		t.Fatalf("Delete(4): %v", err)
	}
	if err := db.Set(13, &Rec{Name: "nil-new", Opt: nil, Active: true, Tags: []string{"db", "ops"}}); err != nil {
		t.Fatalf("Set(13): %v", err)
	}
	if err := db.Set(14, &Rec{Name: "az-new", Opt: strPtr("az"), Active: false, Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set(14): %v", err)
	}
	check("after_insert_delete")

	if err := db.Patch(7, []Field{{Name: "active", Value: false}}); err != nil {
		t.Fatalf("Patch(7 active=false): %v", err)
	}
	if err := db.Patch(8, []Field{{Name: "opt", Value: "aa"}}); err != nil {
		t.Fatalf("Patch(8 opt=aa): %v", err)
	}
	if err := db.Delete(9); err != nil {
		t.Fatalf("Delete(9): %v", err)
	}
	check("after_tail_churn")
}

func TestQueryExt_ArrayCount_DistinctLengthChurn_MatchesSeqScanAndPrepared(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	rows := map[uint64]*Rec{
		1:  {Name: "zero-nil-1", Tags: nil, Active: true},
		2:  {Name: "zero-empty-1", Tags: []string{}, Active: false},
		3:  {Name: "zero-nil-2", Tags: nil, Active: true},
		4:  {Name: "zero-empty-2", Tags: []string{}, Active: false},
		5:  {Name: "zero-nil-3", Tags: nil, Active: true},
		6:  {Name: "zero-empty-3", Tags: []string{}, Active: false},
		7:  {Name: "zero-nil-4", Tags: nil, Active: true},
		8:  {Name: "one-dup", Tags: []string{"go", "go"}, Active: false},
		9:  {Name: "two-distinct", Tags: []string{"go", "db", "db"}, Active: true},
		10: {Name: "three-distinct", Tags: []string{"ops", "go", "db"}, Active: false},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}

	tests := []struct {
		name string
		q    *qx.QX
	}{
		{
			name: "asc_window",
			q:    queryExtSortByArrayCount(qx.Query(), "tags", qx.ASC).Offset(1).Limit(7),
		},
		{
			name: "desc_active_window",
			q:    queryExtSortByArrayCount(qx.Query(qx.EQ("active", true)), "tags", qx.DESC).Offset(1).Limit(4),
		},
		{
			name: "asc_negated_filter",
			q:    queryExtSortByArrayCount(qx.Query(qx.NOT(qx.EQ("active", false))), "tags", qx.ASC).Limit(6),
		},
	}

	check := func(step string) {
		for _, tc := range tests {
			t.Run(step+"_"+tc.name, func(t *testing.T) {
				assertQueryExtAllReadPathsMatchExpected(t, db, tc.q)
			})
		}
	}

	check("initial")

	if err := db.BatchPatch([]uint64{1, 2, 3, 4}, []Field{{Name: "tags", Value: []string{"go"}}}); err != nil {
		t.Fatalf("BatchPatch(densify zeros): %v", err)
	}
	check("after_dense_nonempty")

	if err := db.BatchPatch([]uint64{1, 2, 3, 4, 5}, []Field{{Name: "tags", Value: []string{}}}); err != nil {
		t.Fatalf("BatchPatch(restore zeros): %v", err)
	}
	check("after_zero_complement_return")

	if err := db.Patch(8, []Field{{Name: "tags", Value: []string{"ops", "ops", "ops"}}}); err != nil {
		t.Fatalf("Patch(8 tags): %v", err)
	}
	if err := db.Patch(9, []Field{{Name: "tags", Value: []string{"go", "db", "ops", "ops"}}}); err != nil {
		t.Fatalf("Patch(9 tags): %v", err)
	}
	if err := db.Patch(10, []Field{{Name: "tags", Value: []string{"rust", "rust"}}}); err != nil {
		t.Fatalf("Patch(10 tags): %v", err)
	}
	if err := db.Delete(6); err != nil {
		t.Fatalf("Delete(6): %v", err)
	}
	if err := db.Set(11, &Rec{Name: "zero-new", Tags: nil, Active: true}); err != nil {
		t.Fatalf("Set(11): %v", err)
	}
	if err := db.Set(12, &Rec{Name: "two-new", Tags: []string{"ops", "go", "go"}, Active: false}); err != nil {
		t.Fatalf("Set(12): %v", err)
	}
	check("after_distinct_churn")
}

func TestQueryExt_OrderBasicPointerBounds_DoNotLeakNilTail(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	rows := map[uint64]*Rec{
		1: {Name: "nil-a", Opt: nil, Active: true},
		2: {Name: "empty", Opt: strPtr(""), Active: true},
		3: {Name: "aa-a", Opt: strPtr("aa"), Active: true},
		4: {Name: "ab-a", Opt: strPtr("ab"), Active: true},
		5: {Name: "ac-off", Opt: strPtr("ac"), Active: false},
		6: {Name: "ba-a", Opt: strPtr("ba"), Active: true},
		7: {Name: "nil-b", Opt: nil, Active: true},
		8: {Name: "aa-off", Opt: strPtr("aa"), Active: false},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}

	tests := []struct {
		name string
		q    *qx.QX
	}{
		{
			name: "gt_asc",
			q:    qx.Query(qx.GT("opt", "aa")).Sort("opt", qx.ASC).Limit(8),
		},
		{
			name: "lte_active_desc",
			q:    qx.Query(qx.LTE("opt", "aa"), qx.EQ("active", true)).Sort("opt", qx.DESC).Limit(8),
		},
		{
			name: "gte_window",
			q:    qx.Query(qx.GTE("opt", "ab")).Sort("opt", qx.ASC).Offset(1).Limit(2),
		},
	}

	check := func(step string) {
		for _, tc := range tests {
			t.Run(step+"_"+tc.name, func(t *testing.T) {
				assertQueryExtraPublicReadPathsMatchExpected(t, db, tc.q)
			})
		}
	}

	check("initial")

	if err := db.Patch(1, []Field{{Name: "opt", Value: "ad"}}); err != nil {
		t.Fatalf("Patch(1 opt=ad): %v", err)
	}
	if err := db.Patch(4, []Field{{Name: "opt", Value: (*string)(nil)}}); err != nil {
		t.Fatalf("Patch(4 opt=nil): %v", err)
	}
	if err := db.Patch(7, []Field{{Name: "opt", Value: "aa"}}); err != nil {
		t.Fatalf("Patch(7 opt=aa): %v", err)
	}

	check("after_patch")
}

func TestQueryExt_OrderBasicPointerNilContradictions_RemainEmpty(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	rows := map[uint64]*Rec{
		1: {Name: "nil-a", Opt: nil, Active: true},
		2: {Name: "empty", Opt: strPtr(""), Active: true},
		3: {Name: "aa-a", Opt: strPtr("aa"), Active: true},
		4: {Name: "ab-a", Opt: strPtr("ab"), Active: true},
		5: {Name: "ac-off", Opt: strPtr("ac"), Active: false},
		6: {Name: "ba-a", Opt: strPtr("ba"), Active: true},
		7: {Name: "nil-b", Opt: nil, Active: false},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}

	tests := []struct {
		name string
		q    *qx.QX
	}{
		{
			name: "eq_nil_and_gt",
			q:    qx.Query(qx.EQ("opt", nil), qx.GT("opt", "aa")).Sort("opt", qx.ASC).Limit(8),
		},
		{
			name: "eq_nil_and_eq_value",
			q:    qx.Query(qx.EQ("opt", nil), qx.EQ("opt", "aa")).Sort("opt", qx.ASC).Limit(8),
		},
		{
			name: "eq_nil_and_prefix",
			q:    qx.Query(qx.EQ("opt", nil), qx.PREFIX("opt", "a")).Sort("opt", qx.DESC).Offset(1).Limit(3),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertQueryExtAllReadPathsMatchExpected(t, db, tc.q)

			got, err := db.QueryKeys(tc.q)
			if err != nil {
				t.Fatalf("QueryKeys(%+v): %v", tc.q, err)
			}
			if len(got) != 0 {
				t.Fatalf("expected empty result, got=%v", got)
			}
		})
	}
}

func TestQueryExt_OrderBasicNilShortCircuit_PreservesResidualValidation(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	rows := map[uint64]*Rec{
		1: {Name: "nil-a", Opt: nil, Age: 10, Active: true},
		2: {Name: "empty", Opt: strPtr(""), Age: 20, Active: true},
		3: {Name: "aa-a", Opt: strPtr("aa"), Age: 30, Active: true},
		4: {Name: "ab-a", Opt: strPtr("ab"), Age: 40, Active: true},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}

	tests := []struct {
		name string
		q    *qx.QX
	}{
		{
			name: "pointer_conflict_invalid_residual",
			q: qx.Query(
				qx.EQ("opt", nil),
				qx.GT("opt", "aa"),
				qx.HASALL("name", []string{"x"}),
			).Sort("opt", qx.ASC).Limit(8),
		},
		{
			name: "non_pointer_nil_invalid_residual",
			q: qx.Query(
				qx.EQ("age", nil),
				qx.HASALL("name", []string{"x"}),
			).Sort("age", qx.ASC).Limit(8),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.QueryKeys(tc.q)
			if !errors.Is(err, rbierrors.ErrInvalidQuery) {
				t.Fatalf("QueryKeys(%+v) err=%v, want rbierrors.ErrInvalidQuery", tc.q, err)
			}
		})
	}
}

func TestQueryExt_OrderBasicNilShortCircuit_ReturnsEmptyResult(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	rows := map[uint64]*Rec{
		1: {Name: "nil-a", Opt: nil, Active: true},
		2: {Name: "alpha", Opt: strPtr("aa"), Active: true},
		3: {Name: "bravo", Opt: strPtr("ab"), Active: true},
		4: {Name: "charlie", Opt: strPtr("ac"), Active: true},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}

	q := qx.Query(
		qx.EQ("opt", nil),
		qx.GT("opt", "aa"),
		qx.CONTAINS("name", "a"),
	).Sort("opt", qx.ASC).Limit(8)

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys(%+v): %v", q, err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty result, got=%v", got)
	}
}

func TestQueryExt_OrderBasicPointerPrefixWithNegativeBaseOps_RemainsExact(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	rows := map[uint64]*Rec{
		1:  {Name: "nil-a", Opt: nil, Active: true},
		2:  {Name: "aa-on", Opt: strPtr("aa"), Active: true},
		3:  {Name: "ab-off", Opt: strPtr("ab"), Active: false},
		4:  {Name: "ac-on", Opt: strPtr("ac"), Active: true},
		5:  {Name: "ad-off", Opt: strPtr("ad"), Active: false},
		6:  {Name: "ba-off", Opt: strPtr("ba"), Active: false},
		7:  {Name: "empty-off", Opt: strPtr(""), Active: false},
		8:  {Name: "az-on", Opt: strPtr("az"), Active: true},
		9:  {Name: "nil-b", Opt: nil, Active: false},
		10: {Name: "ax-off", Opt: strPtr("ax"), Active: false},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}

	tests := []struct {
		name string
		q    *qx.QX
	}{
		{
			name: "prefix_not_active_asc",
			q: qx.Query(
				qx.PREFIX("opt", "a"),
				qx.NOT(qx.EQ("active", true)),
			).Sort("opt", qx.ASC).Offset(1).Limit(4),
		},
		{
			name: "prefix_not_active_desc",
			q: qx.Query(
				qx.PREFIX("opt", "a"),
				qx.NOT(qx.EQ("active", true)),
			).Sort("opt", qx.DESC).Limit(5),
		},
		{
			name: "prefix_not_name",
			q: qx.Query(
				qx.PREFIX("opt", "a"),
				qx.NOT(qx.EQ("name", "ax-off")),
			).Sort("opt", qx.ASC).Limit(6),
		},
	}

	check := func(step string) {
		for _, tc := range tests {
			t.Run(step+"_"+tc.name, func(t *testing.T) {
				assertQueryExtraPublicReadPathsMatchExpected(t, db, tc.q)
			})
		}
	}

	check("initial")

	if err := db.Patch(1, []Field{{Name: "opt", Value: "ae"}, {Name: "active", Value: false}}); err != nil {
		t.Fatalf("Patch(1): %v", err)
	}
	if err := db.Patch(4, []Field{{Name: "opt", Value: (*string)(nil)}}); err != nil {
		t.Fatalf("Patch(4): %v", err)
	}
	if err := db.Patch(8, []Field{{Name: "active", Value: false}}); err != nil {
		t.Fatalf("Patch(8): %v", err)
	}
	if err := db.Set(11, &Rec{Name: "af-off", Opt: strPtr("af"), Active: false}); err != nil {
		t.Fatalf("Set(11): %v", err)
	}

	check("after_patch")
}
