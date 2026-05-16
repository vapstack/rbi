package rbi

import (
	"fmt"
	"github.com/vapstack/qx"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestRegression_NotInOrderOffset_RouteEquivalence(t *testing.T) {
	tests := []struct {
		name string
		q    *qx.QX
	}{
		{
			name: "captured_shape",
			q:    capturedNotInOrderOffsetQuery(),
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
			assertNotInOrderOffsetRouteEquivalence(t, tests[i].q)
		})
	}
}

func TestRegression_NotInOrderOffset_NoStateCorruption(t *testing.T) {
	db := openSkewedNotInRegressionDB(t)
	q := capturedNotInOrderOffsetQuery()

	before := runQueryKeysChecked(t, db, q)
	_, _, _, _ = assertPreparedRouteEquivalence(t, db, q)
	mid := runQueryKeysChecked(t, db, q)
	_, _, _, _ = assertPreparedRouteEquivalence(t, db, q)
	after := runQueryKeysChecked(t, db, q)

	if !slices.Equal(before, mid) || !slices.Equal(before, after) {
		t.Fatalf("query results changed after interleaved route checks:\nbefore=%v\nmid=%v\nafter=%v", before, mid, after)
	}
}

func TestRegression_MultiTermHAS_LeadSelfCheck_RouteAndCount(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	seedMetamorphicDataProfile(t, db, 8_000, metamorphicDataProfile{
		name:        "Uniform",
		scoreLevels: 50_000,
		activeTrue:  0.50,
		hotCountryP: 0.15,
		hotTagP:     0.30,
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
		tc := tests[i]
		t.Run(tc.name, func(t *testing.T) {
			got, err := db.QueryKeys(tc.q)
			if err != nil {
				t.Fatalf("QueryKeys: %v", err)
			}
			want, err := expectedKeysUint64(t, db, tc.q)
			if err != nil {
				t.Fatalf("expectedKeysUint64: %v", err)
			}
			assertQueryIDsEqual(t, tc.q, got, want)

			_, prepared, _, _ := assertPreparedRouteEquivalence(t, db, tc.q)
			assertQueryIDsEqual(t, tc.q, got, prepared)

			countQ := cloneQuery(tc.q)
			countQ.Order = nil
			countQ.Window.Offset = 0
			countQ.Window.Limit = 0
			wantCountKeys, err := expectedKeysUint64(t, db, countQ)
			if err != nil {
				t.Fatalf("expectedKeysUint64(count): %v", err)
			}
			wantCount := uint64(len(wantCountKeys))

			cnt, err := db.Count(tc.q.Filter)
			if err != nil {
				t.Fatalf("Count: %v", err)
			}
			if cnt != wantCount {
				t.Fatalf("Count mismatch: got=%d want=%d", cnt, wantCount)
			}
		})
	}
}

func TestRegression_CountORByPredicates_MultiTermHASLead(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	seedMetamorphicDataProfile(t, db, 8_000, metamorphicDataProfile{
		name:        "Skewed",
		scoreLevels: 30_000,
		activeTrue:  0.88,
		hotCountryP: 0.75,
		hotTagP:     0.85,
	})

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.HASALL("tags", []string{"db", "rust"}),
				qx.HASALL("tags", []string{"db", "infra"}),
				qx.SUFFIX("country", "land"),
			),
			qx.AND(
				qx.HASALL("tags", []string{"go", "db"}),
				qx.PREFIX("full_name", "FN-1"),
			),
		),
	)

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}

	cnt, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if cnt != uint64(len(want)) {
		t.Fatalf("Count mismatch: got=%d want=%d", cnt, len(want))
	}
}

func TestQueryKeys_NoOrderBroadNegativeAll_MatchesExpected(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	countries := []string{"US", "DE", "FR", "GB"}
	seedGeneratedUint64Data(t, db, 100_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%06d@example.com", i),
			Age:    18 + (i % 50),
			Score:  float64(i % 1_000),
			Active: i%2 == 0,
			Meta: Meta{
				Country: countries[i%len(countries)],
			},
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(qx.NOTIN("country", []string{"US"}))

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(got) < 64_000 {
		t.Fatalf("expected broad negative result to exercise large no-order route, got %d rows", len(got))
	}

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertQueryIDsEqual(t, q, got, want)
}

func TestQueryExt_ConcurrentSharedOrderBasicQuery_IsStable(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 1_500)
	q := qx.Query(
		qx.EQ("active", true),
		qx.NOTIN("country", []string{"Iceland"}),
		qx.GTE("age", 20),
	).Sort("score", qx.DESC).Offset(10).Limit(80)
	assertQueryExtConcurrentReadStable(t, db, q, true)
}

func TestQueryExt_ConcurrentSharedArrayCountQuery_IsStable(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 1_200)
	q := queryExtSortByArrayCount(qx.Query(
		qx.NOT(qx.EQ("active", false)),
	), "tags", qx.DESC).Offset(3).Limit(70)
	assertQueryExtConcurrentReadStable(t, db, q, true)
}

func TestQueryExt_ConcurrentAtomicBatchSetSnapshotConsistency_OnPointerOrder(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	ids := []uint64{1, 2, 3, 4, 5, 6}
	stateA := []*Rec{
		{Name: "A1-nil", Opt: nil, Active: true, Tags: []string{"go"}},
		{Name: "A2-aa", Opt: strPtr("aa"), Active: true, Tags: []string{"db"}},
		{Name: "A3-bb", Opt: strPtr("bb"), Active: true, Tags: []string{"ops"}},
		{Name: "A4-nil-off", Opt: nil, Active: false, Tags: []string{"rust"}},
		{Name: "A5-cc", Opt: strPtr("cc"), Active: true, Tags: []string{"go", "db"}},
		{Name: "A6-empty", Opt: strPtr(""), Active: true, Tags: []string{"ops", "go"}},
	}
	stateB := []*Rec{
		{Name: "B1-dd", Opt: strPtr("dd"), Active: true, Tags: []string{"go"}},
		{Name: "B2-nil", Opt: nil, Active: true, Tags: []string{"db"}},
		{Name: "B3-aa-off", Opt: strPtr("aa"), Active: false, Tags: []string{"ops"}},
		{Name: "B4-empty", Opt: strPtr(""), Active: true, Tags: []string{"rust"}},
		{Name: "B5-bb", Opt: strPtr("bb"), Active: true, Tags: []string{"go", "db"}},
		{Name: "B6-nil-off", Opt: nil, Active: false, Tags: []string{"ops", "go"}},
	}

	setState := func(vals []*Rec) error {
		return db.BatchSet(ids, vals)
	}

	q := qx.Query(qx.EQ("active", true)).Sort("opt", qx.ASC).Offset(1).Limit(3)

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA): %v", err)
	}
	wantA, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(stateA): %v", err)
	}
	itemsA, err := db.Query(q)
	if err != nil {
		t.Fatalf("Query(stateA): %v", err)
	}
	namesA := queryExtItemNames(t, itemsA)
	countA, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count(stateA): %v", err)
	}

	if err := setState(stateB); err != nil {
		t.Fatalf("BatchSet(stateB): %v", err)
	}
	wantB, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(stateB): %v", err)
	}
	itemsB, err := db.Query(q)
	if err != nil {
		t.Fatalf("Query(stateB): %v", err)
	}
	namesB := queryExtItemNames(t, itemsB)
	countB, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count(stateB): %v", err)
	}

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA reset): %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 16)
	start := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 80; i++ {
			vals := stateB
			label := "stateB"
			if i%2 == 1 {
				vals = stateA
				label = "stateA"
			}
			if err := setState(vals); err != nil {
				errCh <- fmt.Errorf("writer %s: %w", label, err)
				return
			}
		}
	}()

	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			<-start
			for i := 0; i < 120; i++ {
				gotKeys, err := db.QueryKeys(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys: %w", gid, i, err)
					return
				}
				if !queryIDsEqual(q, gotKeys, wantA) && !queryIDsEqual(q, gotKeys, wantB) {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys hybrid snapshot: got=%v wantA=%v wantB=%v", gid, i, gotKeys, wantA, wantB)
					return
				}

				gotItems, err := db.Query(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Query: %w", gid, i, err)
					return
				}
				gotNames, ok := queryExtItemNamesOK(gotItems)
				if !ok {
					errCh <- fmt.Errorf("g=%d i=%d Query returned nil item: %#v", gid, i, gotItems)
					return
				}
				if !slices.Equal(gotNames, namesA) && !slices.Equal(gotNames, namesB) {
					errCh <- fmt.Errorf("g=%d i=%d Query hybrid snapshot: got=%v wantA=%v wantB=%v", gid, i, gotNames, namesA, namesB)
					return
				}

				gotCount, err := db.Count(q.Filter)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Count: %w", gid, i, err)
					return
				}
				if gotCount != countA && gotCount != countB {
					errCh <- fmt.Errorf("g=%d i=%d Count hybrid snapshot: got=%d wantA=%d wantB=%d", gid, i, gotCount, countA, countB)
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

func TestQueryExt_ConcurrentWriter_RootExecPreparedQuery_ReturnsHybridResults(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	ids := []uint64{1, 2, 3, 4, 5, 6}
	stateA := []*Rec{
		{Name: "A1-nil", Opt: nil, Active: true, Tags: []string{"go"}},
		{Name: "A2-aa", Opt: strPtr("aa"), Active: true, Tags: []string{"db"}},
		{Name: "A3-bb", Opt: strPtr("bb"), Active: true, Tags: []string{"ops"}},
		{Name: "A4-nil-off", Opt: nil, Active: false, Tags: []string{"rust"}},
		{Name: "A5-cc", Opt: strPtr("cc"), Active: true, Tags: []string{"go", "db"}},
		{Name: "A6-empty", Opt: strPtr(""), Active: true, Tags: []string{"ops", "go"}},
	}
	stateB := []*Rec{
		{Name: "B1-dd", Opt: strPtr("dd"), Active: true, Tags: []string{"go"}},
		{Name: "B2-nil", Opt: nil, Active: true, Tags: []string{"db"}},
		{Name: "B3-aa-off", Opt: strPtr("aa"), Active: false, Tags: []string{"ops"}},
		{Name: "B4-empty", Opt: strPtr(""), Active: true, Tags: []string{"rust"}},
		{Name: "B5-bb", Opt: strPtr("bb"), Active: true, Tags: []string{"go", "db"}},
		{Name: "B6-nil-off", Opt: nil, Active: false, Tags: []string{"ops", "go"}},
	}

	setState := func(vals []*Rec) error {
		return db.BatchSet(ids, vals)
	}

	q := qx.Query(qx.EQ("active", true)).Sort("opt", qx.ASC).Offset(1).Limit(3)
	nq := normalizeQueryForTest(q)
	if err := db.engine.checkUsedQuery(nq); err != nil {
		t.Fatalf("checkUsedQuery(%+v): %v", nq, err)
	}

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA): %v", err)
	}
	wantA, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(stateA): %v", err)
	}

	if err := setState(stateB); err != nil {
		t.Fatalf("BatchSet(stateB): %v", err)
	}
	wantB, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(stateB): %v", err)
	}

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA reset): %v", err)
	}

	start := make(chan struct{})
	found := make(chan []uint64, 1)
	errCh := make(chan error, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 400; i++ {
			vals := stateB
			if i%2 == 1 {
				vals = stateA
			}
			if err := setState(vals); err != nil {
				select {
				case errCh <- fmt.Errorf("writer: %w", err):
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
			for i := 0; i < 500; i++ {
				got, err := execPreparedQueryExt(db, nq)
				if err != nil {
					select {
					case errCh <- fmt.Errorf("execPreparedQuery: %w", err):
					default:
					}
					return
				}
				if queryIDsEqual(q, got, wantA) || queryIDsEqual(q, got, wantB) {
					continue
				}
				select {
				case found <- append([]uint64(nil), got...):
				default:
				}
				return
			}
		}()
	}

	close(start)
	wg.Wait()
	close(found)
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	for got := range found {
		t.Fatalf("root execPreparedQuery returned hybrid result under concurrent writes: got=%v wantA=%v wantB=%v", got, wantA, wantB)
	}
}

func TestQueryExt_OrderedORNegativeResidualPlannerTrace_MatchesExpected(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSink:        func(ev TraceEvent) { mu.Lock(); events = append(events, ev); mu.Unlock() },
		TraceSampleEvery: 1,
	})

	ids, stateA, _, q := queryExtOrderedORNegativeResidualFixture()
	if err := db.BatchSet(ids, stateA); err != nil {
		t.Fatalf("BatchSet(stateA): %v", err)
	}

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys(%+v): %v", q, err)
	}
	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(%+v): %v", q, err)
	}
	assertQueryIDsEqual(t, q, got, want)

	mu.Lock()
	if len(events) == 0 {
		mu.Unlock()
		t.Fatalf("expected trace event for ordered OR query")
	}
	ev := events[len(events)-1]
	mu.Unlock()

	if ev.Plan != string(PlanMaterialized) &&
		ev.Plan != string(PlanORMergeOrderMerge) &&
		ev.Plan != string(PlanORMergeOrderStream) {
		t.Fatalf("unexpected planner route for ordered OR fixture: got %q", ev.Plan)
	}
	if ev.RowsReturned != uint64(len(want)) {
		t.Fatalf("trace rows returned mismatch: got=%d want=%d", ev.RowsReturned, len(want))
	}

	assertQueryExtItemsMatchExpected(t, db, q)
	assertQueryExtCountMatchesBaseQuery(t, db, q)
	assertQueryExtPreparedMatchesExpected(t, db, q)
}

func TestQueryExt_OrderedORPrefixBoundaryChurn_MatchesSeqScanAndPrepared(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	rows := map[uint64]*Rec{
		1: {Name: "alpha-00", FullName: "grp/a/00", Active: true, Score: 10},
		2: {Name: "alpha-01", FullName: "grp/a/01", Active: false, Score: 40},
		3: {Name: "alpha-02", FullName: "grp/a/02", Active: true, Score: 60},
		4: {Name: "alpha-out", FullName: "grp/a0", Active: true, Score: 80},
		5: {Name: "alpha-zz", FullName: "grp/a/zz", Active: false, Score: 55},
		6: {Name: "beta-00", FullName: "grp/b/00", Active: true, Score: 15},
		7: {Name: "beta-05", FullName: "grp/b/05", Active: false, Score: 25},
		8: {Name: "beta-09", FullName: "grp/b/09", Active: true, Score: 65},
		9: {Name: "gamma-00", FullName: "grp/c/00", Active: true, Score: 75},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.PREFIX("full_name", "grp/a/"),
				qx.GTE("full_name", "grp/a/01"),
			),
			qx.AND(
				qx.PREFIX("full_name", "grp/a/"),
				qx.GTE("score", 55.0),
			),
			qx.AND(
				qx.PREFIX("full_name", "grp/b/"),
				qx.EQ("active", true),
			),
		),
	).Sort("full_name", qx.ASC).Offset(1).Limit(6)

	check := func(step string) {
		t.Run(step, func(t *testing.T) {
			assertQueryExtAllReadPathsMatchExpected(t, db, q)
		})
	}

	check("initial")

	if err := db.Patch(4, []Field{{Name: "full_name", Value: "grp/a/10"}}); err != nil {
		t.Fatalf("Patch(4 full_name): %v", err)
	}
	if err := db.Patch(6, []Field{{Name: "active", Value: false}}); err != nil {
		t.Fatalf("Patch(6 active): %v", err)
	}
	check("after_order_boundary_cross")

	if err := db.Patch(5, []Field{{Name: "full_name", Value: "grp/b/01"}, {Name: "active", Value: true}}); err != nil {
		t.Fatalf("Patch(5 full_name/active): %v", err)
	}
	if err := db.Delete(2); err != nil {
		t.Fatalf("Delete(2): %v", err)
	}
	if err := db.Set(10, &Rec{Name: "alpha-new", FullName: "grp/a/00", Active: true, Score: 70}); err != nil {
		t.Fatalf("Set(10): %v", err)
	}
	check("after_overlap_churn")
}

func TestQueryExt_ConcurrentAtomicBatchSetSnapshotConsistency_OnOrderedORNegativeResidual_WithAnalyzer(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: 5 * time.Millisecond})

	ids, stateA, stateB, q := queryExtOrderedORNegativeResidualFixture()
	setState := func(vals []*Rec) error {
		return db.BatchSet(ids, vals)
	}

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA): %v", err)
	}
	wantA, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(stateA): %v", err)
	}
	itemsA, err := db.Query(q)
	if err != nil {
		t.Fatalf("Query(stateA): %v", err)
	}
	namesA := queryExtItemNames(t, itemsA)
	countA, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count(stateA): %v", err)
	}

	if err := setState(stateB); err != nil {
		t.Fatalf("BatchSet(stateB): %v", err)
	}
	wantB, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(stateB): %v", err)
	}
	itemsB, err := db.Query(q)
	if err != nil {
		t.Fatalf("Query(stateB): %v", err)
	}
	namesB := queryExtItemNames(t, itemsB)
	countB, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count(stateB): %v", err)
	}

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA reset): %v", err)
	}

	startVersion := db.PlannerStats().Version
	if latest, ok := waitPlannerStatsVersionGreater(db, startVersion, 250*time.Millisecond); !ok {
		t.Fatalf("expected analyzer to publish planner stats during ordered OR test: start=%d latest=%d", startVersion, latest)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 16)
	start := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 120; i++ {
			vals := stateB
			if i%2 == 1 {
				vals = stateA
			}
			if err := setState(vals); err != nil {
				errCh <- fmt.Errorf("writer: %w", err)
				return
			}
		}
	}()

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			<-start
			for i := 0; i < 160; i++ {
				gotKeys, err := db.QueryKeys(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys: %w", gid, i, err)
					return
				}
				if !queryIDsEqual(q, gotKeys, wantA) && !queryIDsEqual(q, gotKeys, wantB) {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys hybrid snapshot: got=%v wantA=%v wantB=%v", gid, i, gotKeys, wantA, wantB)
					return
				}

				gotItems, err := db.Query(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Query: %w", gid, i, err)
					return
				}
				gotNames, ok := queryExtItemNamesOK(gotItems)
				if !ok {
					errCh <- fmt.Errorf("g=%d i=%d Query returned nil item: %#v", gid, i, gotItems)
					return
				}
				if !slices.Equal(gotNames, namesA) && !slices.Equal(gotNames, namesB) {
					errCh <- fmt.Errorf("g=%d i=%d Query hybrid snapshot: got=%v wantA=%v wantB=%v", gid, i, gotNames, namesA, namesB)
					return
				}

				gotCount, err := db.Count(q.Filter)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Count: %w", gid, i, err)
					return
				}
				if gotCount != countA && gotCount != countB {
					errCh <- fmt.Errorf("g=%d i=%d Count hybrid snapshot: got=%d wantA=%d wantB=%d", gid, i, gotCount, countA, countB)
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

func TestQueryExt_ConcurrentWriter_RootExecPreparedQuery_OnOrderedORNegativeResidual_ReturnsNoHybridResults(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: 5 * time.Millisecond})

	ids, stateA, stateB, q := queryExtOrderedORNegativeResidualFixture()
	setState := func(vals []*Rec) error {
		return db.BatchSet(ids, vals)
	}

	nq := normalizeQueryForTest(q)
	if err := db.engine.checkUsedQuery(nq); err != nil {
		t.Fatalf("checkUsedQuery(%+v): %v", nq, err)
	}

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA): %v", err)
	}
	wantA, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(stateA): %v", err)
	}

	if err := setState(stateB); err != nil {
		t.Fatalf("BatchSet(stateB): %v", err)
	}
	wantB, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(stateB): %v", err)
	}

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA reset): %v", err)
	}

	startVersion := db.PlannerStats().Version
	if latest, ok := waitPlannerStatsVersionGreater(db, startVersion, 250*time.Millisecond); !ok {
		t.Fatalf("expected analyzer to publish planner stats during prepared ordered OR test: start=%d latest=%d", startVersion, latest)
	}

	start := make(chan struct{})
	found := make(chan []uint64, 1)
	errCh := make(chan error, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 320; i++ {
			vals := stateB
			if i%2 == 1 {
				vals = stateA
			}
			if err := setState(vals); err != nil {
				select {
				case errCh <- fmt.Errorf("writer: %w", err):
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
			for i := 0; i < 480; i++ {
				got, err := execPreparedQueryExt(db, nq)
				if err != nil {
					select {
					case errCh <- fmt.Errorf("execPreparedQuery: %w", err):
					default:
					}
					return
				}
				if queryIDsEqual(q, got, wantA) || queryIDsEqual(q, got, wantB) {
					continue
				}
				select {
				case found <- append([]uint64(nil), got...):
				default:
				}
				return
			}
		}()
	}

	close(start)
	wg.Wait()
	close(found)
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	for got := range found {
		t.Fatalf("root execPreparedQuery returned hybrid ordered OR result under concurrent writes: got=%v wantA=%v wantB=%v", got, wantA, wantB)
	}
}

func TestQueryExt_StringKeys_ConcurrentAtomicBatchSetSnapshotConsistency_OnOrderedOR(t *testing.T) {
	db, _ := openTempDBString(t, Options{AnalyzeInterval: -1})

	ids, stateA, stateB, q := queryExtStringOrderedORFixture()
	setState := func(vals []*Rec) error {
		return db.BatchSet(ids, vals)
	}

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA): %v", err)
	}
	wantA, err := expectedKeysString(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysString(stateA): %v", err)
	}
	itemsA, err := db.Query(q)
	if err != nil {
		t.Fatalf("Query(stateA): %v", err)
	}
	namesA := queryExtItemNames(t, itemsA)
	countA, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count(stateA): %v", err)
	}

	if err := setState(stateB); err != nil {
		t.Fatalf("BatchSet(stateB): %v", err)
	}
	wantB, err := expectedKeysString(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysString(stateB): %v", err)
	}
	itemsB, err := db.Query(q)
	if err != nil {
		t.Fatalf("Query(stateB): %v", err)
	}
	namesB := queryExtItemNames(t, itemsB)
	countB, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count(stateB): %v", err)
	}

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA reset): %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 16)
	start := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 120; i++ {
			vals := stateB
			if i%2 == 1 {
				vals = stateA
			}
			if err := setState(vals); err != nil {
				errCh <- fmt.Errorf("writer: %w", err)
				return
			}
		}
	}()

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			<-start
			for i := 0; i < 160; i++ {
				gotKeys, err := db.QueryKeys(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys: %w", gid, i, err)
					return
				}
				if !queryStringIDsEqual(q, gotKeys, wantA) && !queryStringIDsEqual(q, gotKeys, wantB) {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys hybrid snapshot: got=%v wantA=%v wantB=%v", gid, i, gotKeys, wantA, wantB)
					return
				}

				gotItems, err := db.Query(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Query: %w", gid, i, err)
					return
				}
				gotNames, ok := queryExtItemNamesOK(gotItems)
				if !ok {
					errCh <- fmt.Errorf("g=%d i=%d Query returned nil item: %#v", gid, i, gotItems)
					return
				}
				if !slices.Equal(gotNames, namesA) && !slices.Equal(gotNames, namesB) {
					errCh <- fmt.Errorf("g=%d i=%d Query hybrid snapshot: got=%v wantA=%v wantB=%v", gid, i, gotNames, namesA, namesB)
					return
				}

				gotCount, err := db.Count(q.Filter)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Count: %w", gid, i, err)
					return
				}
				if gotCount != countA && gotCount != countB {
					errCh <- fmt.Errorf("g=%d i=%d Count hybrid snapshot: got=%d wantA=%d wantB=%d", gid, i, gotCount, countA, countB)
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

func TestQueryExt_StringKeys_ConcurrentWriter_RootExecPreparedQuery_OnOrderedOR_ReturnsNoHybridResults(t *testing.T) {
	db, _ := openTempDBString(t, Options{AnalyzeInterval: -1})

	ids, stateA, stateB, q := queryExtStringOrderedORFixture()
	setState := func(vals []*Rec) error {
		return db.BatchSet(ids, vals)
	}

	nq := normalizeQueryForTest(q)
	if err := db.engine.checkUsedQuery(nq); err != nil {
		t.Fatalf("checkUsedQuery(%+v): %v", nq, err)
	}

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA): %v", err)
	}
	wantA, err := expectedKeysString(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysString(stateA): %v", err)
	}

	if err := setState(stateB); err != nil {
		t.Fatalf("BatchSet(stateB): %v", err)
	}
	wantB, err := expectedKeysString(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysString(stateB): %v", err)
	}

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA reset): %v", err)
	}

	start := make(chan struct{})
	found := make(chan []string, 1)
	errCh := make(chan error, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 320; i++ {
			vals := stateB
			if i%2 == 1 {
				vals = stateA
			}
			if err := setState(vals); err != nil {
				select {
				case errCh <- fmt.Errorf("writer: %w", err):
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
			for i := 0; i < 480; i++ {
				got, err := execPreparedQueryExt(db, nq)
				if err != nil {
					select {
					case errCh <- fmt.Errorf("execPreparedQuery: %w", err):
					default:
					}
					return
				}
				if queryStringIDsEqual(q, got, wantA) || queryStringIDsEqual(q, got, wantB) {
					continue
				}
				select {
				case found <- append([]string(nil), got...):
				default:
				}
				return
			}
		}()
	}

	close(start)
	wg.Wait()
	close(found)
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	for got := range found {
		t.Fatalf("root execPreparedQuery returned hybrid string-key ordered OR result under concurrent writes: got=%v wantA=%v wantB=%v", got, wantA, wantB)
	}
}

func TestQueryExt_RefreshPlannerStatsDuringRuntimeFallbackEligibleOrderedOR_AllReadPathsStayExact(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 8_000)

	q := normalizeQueryForTest(
		qx.Query(
			qx.OR(
				qx.AND(
					qx.EQ("active", true),
					qx.EQ("country", "NL"),
					qx.GTE("score", 30.0),
				),
				qx.AND(
					qx.EQ("name", "alice"),
					qx.GTE("age", 25),
				),
				qx.PREFIX("full_name", "FN-1"),
			),
		).Sort("age", qx.ASC).Offset(140).Limit(120),
	)

	wantKeys, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(%+v): %v", q, err)
	}
	wantItems, err := db.BatchGet(wantKeys...)
	if err != nil {
		t.Fatalf("BatchGet(wantKeys): %v", err)
	}
	baseQ := cloneQuery(q)
	baseQ.Order = nil
	clearQueryExtOrderWindow(baseQ)
	wantCount, err := db.Count(baseQ.Filter)
	if err != nil {
		t.Fatalf("Count(baseQ): %v", err)
	}
	nq := normalizeQueryForTest(q)
	if err := db.engine.checkUsedQuery(nq); err != nil {
		t.Fatalf("checkUsedQuery(%+v): %v", nq, err)
	}

	errCh := make(chan error, 16)
	var wg sync.WaitGroup

	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				gotKeys, err := db.QueryKeys(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys: %w", gid, i, err)
					return
				}
				if !queryIDsEqual(q, gotKeys, wantKeys) {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys mismatch: got=%v want=%v", gid, i, gotKeys, wantKeys)
					return
				}

				gotItems, err := db.Query(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Query: %w", gid, i, err)
					return
				}
				if len(gotItems) != len(wantItems) {
					errCh <- fmt.Errorf("g=%d i=%d Query len mismatch: got=%d want=%d", gid, i, len(gotItems), len(wantItems))
					return
				}
				for j := range wantItems {
					if gotItems[j] == nil || wantItems[j] == nil || !reflect.DeepEqual(*gotItems[j], *wantItems[j]) {
						errCh <- fmt.Errorf("g=%d i=%d Query item mismatch at j=%d", gid, i, j)
						return
					}
				}

				gotCount, err := db.Count(q.Filter)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Count: %w", gid, i, err)
					return
				}
				if gotCount != wantCount {
					errCh <- fmt.Errorf("g=%d i=%d Count mismatch: got=%d want=%d", gid, i, gotCount, wantCount)
					return
				}

				gotPrepared, err := execPreparedQueryExt(db, nq)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d execPreparedQuery: %w", gid, i, err)
					return
				}
				if !queryIDsEqual(nq, gotPrepared, wantKeys) {
					errCh <- fmt.Errorf("g=%d i=%d execPreparedQuery mismatch: got=%v want=%v", gid, i, gotPrepared, wantKeys)
					return
				}
			}
		}(g)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 25; i++ {
			if err := db.RefreshPlannerStats(); err != nil {
				errCh <- fmt.Errorf("RefreshPlannerStats i=%d: %w", i, err)
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			s := db.PlannerStats()
			if s.Fields != nil {
				s.Fields["country"] = PlannerFieldStats{}
				delete(s.Fields, "age")
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func TestQueryExt_ConcurrentAtomicBatchSetSnapshotConsistency_OnNoOrderORWindow_WithAnalyzer(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: 5 * time.Millisecond})

	ids, stateA, stateB, q := queryExtNoOrderORDisjointFixture()
	setState := func(vals []*Rec) error {
		return db.BatchSet(ids, vals)
	}

	fullQ := cloneQuery(q)
	clearQueryExtOrderWindow(fullQ)

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA): %v", err)
	}
	fullA, err := expectedKeysUint64(t, db, fullQ)
	if err != nil {
		t.Fatalf("expectedKeysUint64(full stateA): %v", err)
	}
	itemsAFull, err := db.BatchGet(fullA...)
	if err != nil {
		t.Fatalf("BatchGet(fullA): %v", err)
	}
	sigsAFull := queryExtBuildSignatureCounts(itemsAFull)
	countA, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count(stateA): %v", err)
	}

	if err := setState(stateB); err != nil {
		t.Fatalf("BatchSet(stateB): %v", err)
	}
	fullB, err := expectedKeysUint64(t, db, fullQ)
	if err != nil {
		t.Fatalf("expectedKeysUint64(full stateB): %v", err)
	}
	itemsBFull, err := db.BatchGet(fullB...)
	if err != nil {
		t.Fatalf("BatchGet(fullB): %v", err)
	}
	sigsBFull := queryExtBuildSignatureCounts(itemsBFull)
	countB, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count(stateB): %v", err)
	}

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA reset): %v", err)
	}

	nq := normalizeQueryForTest(q)
	if err := db.engine.checkUsedQuery(nq); err != nil {
		t.Fatalf("checkUsedQuery(%+v): %v", nq, err)
	}

	startVersion := db.PlannerStats().Version
	if latest, ok := waitPlannerStatsVersionGreater(db, startVersion, 250*time.Millisecond); !ok {
		t.Fatalf("expected analyzer to publish planner stats during no-order OR test: start=%d latest=%d", startVersion, latest)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 16)
	start := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 120; i++ {
			vals := stateB
			if i%2 == 1 {
				vals = stateA
			}
			if err := setState(vals); err != nil {
				errCh <- fmt.Errorf("writer: %w", err)
				return
			}
		}
	}()

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			<-start
			for i := 0; i < 160; i++ {
				gotKeys, err := db.QueryKeys(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys: %w", gid, i, err)
					return
				}
				errKA := plannerExtValidateNoOrderWindow(q, gotKeys, fullA)
				errKB := plannerExtValidateNoOrderWindow(q, gotKeys, fullB)
				if errKA != nil && errKB != nil {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys hybrid snapshot: got=%v errA=%v errB=%v", gid, i, gotKeys, errKA, errKB)
					return
				}

				gotItems, err := db.Query(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Query: %w", gid, i, err)
					return
				}
				gotNames, ok := queryExtItemNamesOK(gotItems)
				if !ok {
					errCh <- fmt.Errorf("g=%d i=%d Query returned nil item: %#v", gid, i, gotItems)
					return
				}
				errIA := queryExtValidateNoOrderItemsAgainstFullSet(q, gotItems, sigsAFull, len(fullA))
				errIB := queryExtValidateNoOrderItemsAgainstFullSet(q, gotItems, sigsBFull, len(fullB))
				if errIA != nil && errIB != nil {
					errCh <- fmt.Errorf("g=%d i=%d Query hybrid snapshot: errA=%v errB=%v items=%v", gid, i, errIA, errIB, gotNames)
					return
				}

				gotCount, err := db.Count(q.Filter)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Count: %w", gid, i, err)
					return
				}
				if gotCount != countA && gotCount != countB {
					errCh <- fmt.Errorf("g=%d i=%d Count hybrid snapshot: got=%d wantA=%d wantB=%d", gid, i, gotCount, countA, countB)
					return
				}

				gotPrepared, err := execPreparedQueryExt(db, nq)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d execPreparedQuery: %w", gid, i, err)
					return
				}
				errPA := plannerExtValidateNoOrderWindow(q, gotPrepared, fullA)
				errPB := plannerExtValidateNoOrderWindow(q, gotPrepared, fullB)
				if errPA != nil && errPB != nil {
					errCh <- fmt.Errorf("g=%d i=%d execPreparedQuery hybrid snapshot: got=%v errA=%v errB=%v", gid, i, gotPrepared, errPA, errPB)
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

func TestQueryExt_RefreshPlannerStatsDuringAdversarialNoOrderOR_AllReadPathsStayValid(t *testing.T) {
	db := plannerExtOpenSeededDB(t, Options{AnalyzeInterval: -1})
	q := plannerExtQueryAdversarialNoOrderNegativeResidualOverlap()

	fullQ := cloneQuery(q)
	clearQueryExtOrderWindow(fullQ)
	full, err := expectedKeysUint64(t, db, fullQ)
	if err != nil {
		t.Fatalf("expectedKeysUint64(full %+v): %v", fullQ, err)
	}
	fullItems, err := db.BatchGet(full...)
	if err != nil {
		t.Fatalf("BatchGet(full): %v", err)
	}
	fullSigCounts := queryExtBuildSignatureCounts(fullItems)
	wantCount := uint64(len(full))

	nq := normalizeQueryForTest(q)
	if err := db.engine.checkUsedQuery(nq); err != nil {
		t.Fatalf("checkUsedQuery(%+v): %v", nq, err)
	}

	errCh := make(chan error, 16)
	var wg sync.WaitGroup

	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				gotKeys, err := db.QueryKeys(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys: %w", gid, i, err)
					return
				}
				if err = plannerExtValidateNoOrderWindow(q, gotKeys, full); err != nil {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys invalid: %v got=%v full=%v", gid, i, err, gotKeys, full)
					return
				}

				gotItems, err := db.Query(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Query: %w", gid, i, err)
					return
				}
				gotNames, ok := queryExtItemNamesOK(gotItems)
				if !ok {
					errCh <- fmt.Errorf("g=%d i=%d Query returned nil item: %#v", gid, i, gotItems)
					return
				}
				if err = queryExtValidateNoOrderItemsAgainstFullSet(q, gotItems, fullSigCounts, len(full)); err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Query invalid: %v items=%v", gid, i, err, gotNames)
					return
				}

				gotCount, err := db.Count(q.Filter)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Count: %w", gid, i, err)
					return
				}
				if gotCount != wantCount {
					errCh <- fmt.Errorf("g=%d i=%d Count mismatch: got=%d want=%d", gid, i, gotCount, wantCount)
					return
				}

				gotPrepared, err := execPreparedQueryExt(db, nq)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d execPreparedQuery: %w", gid, i, err)
					return
				}
				if err = plannerExtValidateNoOrderWindow(q, gotPrepared, full); err != nil {
					errCh <- fmt.Errorf("g=%d i=%d execPreparedQuery invalid: %v got=%v full=%v", gid, i, err, gotPrepared, full)
					return
				}
			}
		}(g)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if err := db.RefreshPlannerStats(); err != nil {
				errCh <- fmt.Errorf("RefreshPlannerStats i=%d: %w", i, err)
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 120; i++ {
			s := db.PlannerStats()
			if s.Fields != nil {
				s.Fields["country"] = PlannerFieldStats{}
				delete(s.Fields, "age")
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func TestQueryExt_Race_PlannerAnalyzeLoopVsSnapshotPublish_OnOrderedORFixture(t *testing.T) {
	if !testRaceEnabled {
		t.Skip("run with -race to detect planner analyzer vs snapshot publish race")
	}

	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: 2 * time.Millisecond})

	ids, stateA, stateB, q := queryExtOrderedORNegativeResidualFixture()
	setState := func(vals []*Rec) error {
		return db.BatchSet(ids, vals)
	}

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA): %v", err)
	}
	startVersion := db.PlannerStats().Version
	if latest, ok := waitPlannerStatsVersionGreater(db, startVersion, 250*time.Millisecond); !ok {
		t.Fatalf("expected analyzer to start before race reproducer: start=%d latest=%d", startVersion, latest)
	}

	wantA, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(stateA): %v", err)
	}
	if err := setState(stateB); err != nil {
		t.Fatalf("BatchSet(stateB): %v", err)
	}
	wantB, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(stateB): %v", err)
	}
	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA reset): %v", err)
	}

	errCh := make(chan error, 16)
	var wg sync.WaitGroup
	start := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 200; i++ {
			vals := stateB
			if i%2 == 1 {
				vals = stateA
			}
			if err := setState(vals); err != nil {
				errCh <- fmt.Errorf("writer: %w", err)
				return
			}
		}
	}()

	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			<-start
			for i := 0; i < 120; i++ {
				got, err := db.QueryKeys(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys: %w", gid, i, err)
					return
				}
				if !queryIDsEqual(q, got, wantA) && !queryIDsEqual(q, got, wantB) {
					errCh <- fmt.Errorf("g=%d i=%d hybrid ordered OR result: got=%v wantA=%v wantB=%v", gid, i, got, wantA, wantB)
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

func TestQueryExt_Race_RefreshPlannerStatsVsSnapshotPublish_OnOrderedORFixture(t *testing.T) {
	if !testRaceEnabled {
		t.Skip("run with -race to detect RefreshPlannerStats vs snapshot publish race")
	}

	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	ids, stateA, stateB, q := queryExtOrderedORNegativeResidualFixture()
	setState := func(vals []*Rec) error {
		return db.BatchSet(ids, vals)
	}

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA): %v", err)
	}
	wantA, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(stateA): %v", err)
	}
	if err := setState(stateB); err != nil {
		t.Fatalf("BatchSet(stateB): %v", err)
	}
	wantB, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(stateB): %v", err)
	}
	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA reset): %v", err)
	}

	errCh := make(chan error, 32)
	var wg sync.WaitGroup
	start := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 240; i++ {
			vals := stateB
			if i%2 == 1 {
				vals = stateA
			}
			if err := setState(vals); err != nil {
				errCh <- fmt.Errorf("writer: %w", err)
				return
			}
		}
	}()

	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			<-start
			for i := 0; i < 160; i++ {
				if err := db.RefreshPlannerStats(); err != nil {
					errCh <- fmt.Errorf("g=%d i=%d RefreshPlannerStats: %w", gid, i, err)
					return
				}
			}
		}(g)
	}

	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			<-start
			for i := 0; i < 120; i++ {
				got, err := db.QueryKeys(q)
				if err != nil {
					errCh <- fmt.Errorf("query g=%d i=%d QueryKeys: %w", gid, i, err)
					return
				}
				if !queryIDsEqual(q, got, wantA) && !queryIDsEqual(q, got, wantB) {
					errCh <- fmt.Errorf("query g=%d i=%d hybrid ordered OR result: got=%v wantA=%v wantB=%v", gid, i, got, wantA, wantB)
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

func TestQueryExt_PrefixRangeIntersections_StayExactAcrossBoundaryChurn(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	rows := map[uint64]*Rec{
		1: {Name: "aa", Active: true},
		2: {Name: "aa/", Active: true},
		3: {Name: "aa/0", Active: true},
		4: {Name: "aa/00", Active: true},
		5: {Name: "aa/9", Active: true},
		6: {Name: "aa0", Active: true},
		7: {Name: "aa1", Active: true},
		8: {Name: "ab", Active: true},
		9: {Name: "b", Active: true},
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
			name: "bounded_prefix_window",
			q: qx.Query(
				qx.PREFIX("name", "aa/"),
				qx.GTE("name", "aa/0"),
				qx.LT("name", "aa0"),
			).Sort("name", qx.ASC).Limit(10),
		},
		{
			name: "prefix_singleton_upper_edge",
			q: qx.Query(
				qx.PREFIX("name", "aa/"),
				qx.LTE("name", "aa/"),
			).Sort("name", qx.ASC).Limit(10),
		},
		{
			name: "prefix_empty_after_crossing_upper",
			q: qx.Query(
				qx.PREFIX("name", "aa/"),
				qx.GTE("name", "aa0"),
			).Sort("name", qx.ASC).Limit(10),
		},
		{
			name: "bounded_prefix_desc_window",
			q: qx.Query(
				qx.PREFIX("name", "aa/"),
				qx.GT("name", "aa/"),
				qx.LTE("name", "aa/zz"),
			).Sort("name", qx.DESC).Offset(1).Limit(3),
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

	if err := db.Patch(6, []Field{{Name: "name", Value: "aa/zz"}}); err != nil {
		t.Fatalf("Patch(6 name=aa/zz): %v", err)
	}
	if err := db.Delete(3); err != nil {
		t.Fatalf("Delete(3): %v", err)
	}
	if err := db.Set(10, &Rec{Name: "aa/5", Active: true}); err != nil {
		t.Fatalf("Set(10): %v", err)
	}

	check("after_patch")
}

func TestQueryExt_MixedCaching_NumericRangesRemainExactAcrossClearAndPublish(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 8,
	})

	seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
		countries := []string{"US", "DE", "FR", "NL"}
		return &Rec{
			Name:   fmt.Sprintf("user-%05d", i),
			Age:    i,
			Score:  float64(i),
			Active: i%2 == 0,
			Meta: Meta{
				Country: countries[i%len(countries)],
			},
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	setNumericBucketKnobs(t, db, 128, 1, 1)

	queries := []*qx.QX{
		qx.Query(
			qx.LT("score", 15_000.0),
			qx.EQ("active", true),
		).Sort("age", qx.ASC).Offset(3_500).Limit(80),
		qx.Query(
			qx.LT("score", 15_001.0),
			qx.EQ("active", true),
		).Sort("age", qx.ASC).Offset(3_500).Limit(80),
		qx.Query(
			qx.GTE("score", 6_000.0),
			qx.LT("score", 17_000.0),
			qx.EQ("country", "US"),
		).Sort("age", qx.DESC).Offset(900).Limit(60),
	}

	checkQueries := func(step string) {
		for i, q := range queries {
			t.Run(fmt.Sprintf("%s_q%d", step, i), func(t *testing.T) {
				assertQueryExtraPublicReadPathsMatchExpected(t, db, q)
			})
		}
	}

	checkQueries("warm")
	if got := db.engine.snapshot.Current().MaterializedPredCache().EntryCount(); got == 0 {
		t.Fatalf("expected shared runtime caches to warm up")
	}

	for i := 1; i <= 12; i++ {
		if err := db.Patch(uint64(i), []Field{{Name: "name", Value: fmt.Sprintf("mut-%d", i)}}); err != nil {
			t.Fatalf("Patch(%d name): %v", i, err)
		}
	}
	checkQueries("after_unrelated_publish")

	db.clearCurrentSnapshotCachesForTesting()
	snap := db.engine.snapshot.Current()
	if got := snap.MaterializedPredCache().EntryCount(); got != 0 {
		t.Fatalf("expected cleared materialized predicate cache, got=%d", got)
	}

	checkQueries("after_clear")
	if got := db.engine.snapshot.Current().MaterializedPredCache().EntryCount(); got == 0 {
		t.Fatalf("expected caches to repopulate after cold queries")
	}
}

func TestQueryExt_ConcurrentEvictingMaterializedPredicates_RemainStable(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 1,
	})

	setNumericBucketKnobs(t, db, 128, 1, 1)
	seedGeneratedUint64Data(t, db, 5_000, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%05d", i),
			Age:   i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	type expectation struct {
		q     *qx.QX
		ids   []uint64
		items []*Rec
		count uint64
	}

	queries := []*qx.QX{
		qx.Query(qx.LT("score", 4_000.0)).Sort("age", qx.ASC).Offset(2_000).Limit(40),
		qx.Query(qx.LT("score", 4_001.0)).Sort("age", qx.ASC).Offset(2_000).Limit(40),
		qx.Query(qx.LT("score", 3_999.0)).Sort("age", qx.ASC).Offset(2_000).Limit(40),
	}
	expects := make([]expectation, len(queries))
	for i, q := range queries {
		ids, err := expectedKeysUint64(t, db, q)
		if err != nil {
			t.Fatalf("expectedKeysUint64(q%d): %v", i, err)
		}
		items, err := db.BatchGet(ids...)
		if err != nil {
			t.Fatalf("BatchGet(q%d): %v", i, err)
		}
		countQ := cloneQuery(q)
		countQ.Order = nil
		clearQueryExtOrderWindow(countQ)
		count, err := db.Count(countQ.Filter)
		if err != nil {
			t.Fatalf("Count(q%d base): %v", i, err)
		}
		expects[i] = expectation{
			q:     q,
			ids:   ids,
			items: items,
			count: count,
		}
	}

	if _, err := db.QueryKeys(queries[0]); err != nil {
		t.Fatalf("warm QueryKeys: %v", err)
	}
	if got := db.engine.snapshot.Current().MaterializedPredCache().EntryCount(); got == 0 {
		t.Fatalf("expected deep ordered window to materialize a predicate cache entry")
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 16)

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 120; i++ {
				exp := expects[(gid+i)%len(expects)]

				gotKeys, err := db.QueryKeys(exp.q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys: %w", gid, i, err)
					return
				}
				if !queryIDsEqual(exp.q, gotKeys, exp.ids) {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys mismatch: got=%v want=%v", gid, i, gotKeys, exp.ids)
					return
				}

				gotItems, err := db.Query(exp.q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Query: %w", gid, i, err)
					return
				}
				if len(gotItems) != len(exp.items) {
					errCh <- fmt.Errorf("g=%d i=%d Query len mismatch: got=%d want=%d", gid, i, len(gotItems), len(exp.items))
					return
				}
				for j := range exp.items {
					if gotItems[j] == nil || exp.items[j] == nil || !reflect.DeepEqual(*gotItems[j], *exp.items[j]) {
						errCh <- fmt.Errorf("g=%d i=%d Query item mismatch at j=%d", gid, i, j)
						return
					}
				}

				gotCount, err := db.Count(exp.q.Filter)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Count: %w", gid, i, err)
					return
				}
				if gotCount != exp.count {
					errCh <- fmt.Errorf("g=%d i=%d Count mismatch: got=%d want=%d", gid, i, gotCount, exp.count)
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	if got := db.engine.snapshot.Current().MaterializedPredCache().EntryCount(); got > 1 {
		t.Fatalf("materialized predicate cache exceeded configured bound: got=%d", got)
	}
}

func TestQueryExt_StringKeys_ConcurrentPrefixRangeSnapshotConsistency(t *testing.T) {
	db, _ := openTempDBString(t, Options{AnalyzeInterval: -1})

	ids := []string{"id-1", "id-2", "id-3", "id-4", "id-5", "id-6", "id-7"}
	stateA := []*Rec{
		{Name: "aa/00", Active: true},
		{Name: "aa/01", Active: true},
		{Name: "aa/02", Active: false},
		{Name: "aa/zz", Active: true},
		{Name: "ab/00", Active: true},
		{Name: "aa0", Active: true},
		{Name: "aa/05", Active: true},
	}
	stateB := []*Rec{
		{Name: "aa/00", Active: false},
		{Name: "aa/03", Active: true},
		{Name: "aa/yy", Active: true},
		{Name: "aa/zz", Active: false},
		{Name: "aa/05", Active: true},
		{Name: "aa0", Active: true},
		{Name: "aa/04", Active: true},
	}

	setState := func(vals []*Rec) error {
		return db.BatchSet(ids, vals)
	}

	q := qx.Query(
		qx.PREFIX("name", "aa/"),
		qx.GT("name", "aa/00"),
		qx.LTE("name", "aa/zz"),
		qx.EQ("active", true),
	).Sort("name", qx.DESC).Offset(1).Limit(3)

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA): %v", err)
	}
	wantA, err := expectedKeysString(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysString(stateA): %v", err)
	}
	itemsA, err := db.Query(q)
	if err != nil {
		t.Fatalf("Query(stateA): %v", err)
	}
	namesA := queryExtItemNames(t, itemsA)
	countA, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count(stateA): %v", err)
	}

	if err := setState(stateB); err != nil {
		t.Fatalf("BatchSet(stateB): %v", err)
	}
	wantB, err := expectedKeysString(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysString(stateB): %v", err)
	}
	itemsB, err := db.Query(q)
	if err != nil {
		t.Fatalf("Query(stateB): %v", err)
	}
	namesB := queryExtItemNames(t, itemsB)
	countB, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count(stateB): %v", err)
	}

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA reset): %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 16)
	start := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 120; i++ {
			vals := stateB
			if i%2 == 1 {
				vals = stateA
			}
			if err := setState(vals); err != nil {
				errCh <- fmt.Errorf("writer: %w", err)
				return
			}
		}
	}()

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			<-start
			for i := 0; i < 160; i++ {
				gotKeys, err := db.QueryKeys(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys: %w", gid, i, err)
					return
				}
				if !queryStringIDsEqual(q, gotKeys, wantA) && !queryStringIDsEqual(q, gotKeys, wantB) {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys hybrid snapshot: got=%v wantA=%v wantB=%v", gid, i, gotKeys, wantA, wantB)
					return
				}

				gotItems, err := db.Query(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Query: %w", gid, i, err)
					return
				}
				gotNames, ok := queryExtItemNamesOK(gotItems)
				if !ok {
					errCh <- fmt.Errorf("g=%d i=%d Query returned nil item: %#v", gid, i, gotItems)
					return
				}
				if !reflect.DeepEqual(gotNames, namesA) && !reflect.DeepEqual(gotNames, namesB) {
					errCh <- fmt.Errorf("g=%d i=%d Query hybrid snapshot: got=%v wantA=%v wantB=%v", gid, i, gotNames, namesA, namesB)
					return
				}

				gotCount, err := db.Count(q.Filter)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Count: %w", gid, i, err)
					return
				}
				if gotCount != countA && gotCount != countB {
					errCh <- fmt.Errorf("g=%d i=%d Count hybrid snapshot: got=%d wantA=%d wantB=%d", gid, i, gotCount, countA, countB)
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

func TestQueryExt_NumericRangeFieldMutation_DoesNotReuseStaleCaches(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 8,
	})

	seedGeneratedUint64Data(t, db, 15_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("user-%05d", i),
			Age:    i,
			Score:  float64(i),
			Active: i%2 == 0,
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	setNumericBucketKnobs(t, db, 128, 1, 1)

	queries := []*qx.QX{
		qx.Query(
			qx.GTE("age", 2_500),
			qx.EQ("active", true),
		).Sort("age", qx.ASC).Offset(120).Limit(80),
		qx.Query(
			qx.GTE("age", 2_501),
			qx.EQ("active", true),
		).Sort("age", qx.ASC).Offset(120).Limit(80),
		qx.Query(
			qx.GTE("age", 2_400),
			qx.LT("age", 2_650),
		).Sort("age", qx.ASC).Limit(120),
	}

	checkQueries := func(step string) {
		for i, q := range queries {
			t.Run(fmt.Sprintf("%s_q%d", step, i), func(t *testing.T) {
				assertQueryExtraPublicReadPathsMatchExpected(t, db, q)
			})
		}
	}

	checkQueries("warm")
	if got := db.engine.snapshot.Current().MaterializedPredCache().EntryCount(); got == 0 {
		t.Fatalf("expected warmed numeric-range query to populate predicate cache")
	}

	for i := 2400; i <= 2480; i++ {
		if err := db.Patch(uint64(i), []Field{{Name: "age", Value: 9_000 + i}, {Name: "active", Value: true}}); err != nil {
			t.Fatalf("Patch(%d high): %v", i, err)
		}
	}
	for i := 14900; i <= 14980; i++ {
		if err := db.Patch(uint64(i), []Field{{Name: "age", Value: 2_430 + (i - 14900)}}); err != nil {
			t.Fatalf("Patch(%d low): %v", i, err)
		}
	}

	checkQueries("after_field_publish")
	db.clearCurrentSnapshotCachesForTesting()
	checkQueries("after_clear")
}

func TestQueryExt_OrderedOROverlap_DeduplicatesAcrossMutations(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	seedGeneratedUint64Data(t, db, 4_000, func(i int) *Rec {
		group := "grp-b"
		if i%2 == 0 {
			group = "grp-a"
		}
		country := "US"
		if i%5 == 0 {
			country = "NL"
		}
		return &Rec{
			Name:   fmt.Sprintf("u-%04d", i),
			Email:  fmt.Sprintf("%s/%04d@example.test", group, i),
			Age:    18 + (i % 70),
			Score:  float64(i % 100),
			Active: i%3 == 0,
			Meta: Meta{
				Country: country,
			},
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.OR(
			qx.AND(qx.PREFIX("email", "grp-a/"), qx.EQ("active", true)),
			qx.AND(qx.PREFIX("email", "grp-a/"), qx.GTE("age", 40)),
			qx.AND(qx.PREFIX("email", "grp-b/"), qx.EQ("country", "NL")),
			qx.AND(qx.PREFIX("email", "grp-a/"), qx.LTE("score", 15.0)),
		),
	).Sort("score", qx.DESC).Offset(10).Limit(120)

	assertQueryExtraPublicReadPathsMatchExpected(t, db, q)

	for _, id := range []uint64{12, 48, 96, 144, 192, 384} {
		if err := db.Patch(id, []Field{
			{Name: "email", Value: fmt.Sprintf("grp-b/%04d@example.test", id)},
			{Name: "country", Value: "NL"},
			{Name: "active", Value: false},
			{Name: "score", Value: 7.0},
		}); err != nil {
			t.Fatalf("Patch(%d demote): %v", id, err)
		}
	}
	for _, id := range []uint64{11, 33, 55, 77, 99, 121} {
		if err := db.Patch(id, []Field{
			{Name: "email", Value: fmt.Sprintf("grp-a/%04d@example.test", id)},
			{Name: "age", Value: 63},
			{Name: "active", Value: true},
			{Name: "score", Value: 99.0},
		}); err != nil {
			t.Fatalf("Patch(%d promote): %v", id, err)
		}
	}

	assertQueryExtraPublicReadPathsMatchExpected(t, db, q)
}

func TestQueryExt_ConcurrentAtomicBatchSetSnapshotConsistency_OnNumericRangeOrder(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 8,
	})

	setNumericBucketKnobs(t, db, 64, 1, 1)

	ids := make([]uint64, 0, 64)
	stateA := make([]*Rec, 0, 64)
	stateB := make([]*Rec, 0, 64)
	for i := 1; i <= 64; i++ {
		id := uint64(i)
		ids = append(ids, id)
		stateA = append(stateA, &Rec{
			Name:   fmt.Sprintf("A-%02d", i),
			Age:    100 + i*3,
			Score:  float64(100 + i*3),
			Active: i%3 != 0,
		})
		stateB = append(stateB, &Rec{
			Name:   fmt.Sprintf("B-%02d", i),
			Age:    200 + i*5,
			Score:  float64(200 + i*5),
			Active: i%4 != 0,
		})
	}

	setState := func(vals []*Rec) error {
		return db.BatchSet(ids, vals)
	}

	q := qx.Query(
		qx.GTE("age", 130),
		qx.LT("age", 420),
		qx.EQ("active", true),
	).Sort("age", qx.ASC).Offset(4).Limit(18)

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA): %v", err)
	}
	if _, err := db.QueryKeys(q); err != nil {
		t.Fatalf("warm QueryKeys(stateA): %v", err)
	}
	wantA, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(stateA): %v", err)
	}
	itemsA, err := db.Query(q)
	if err != nil {
		t.Fatalf("Query(stateA): %v", err)
	}
	namesA := queryExtItemNames(t, itemsA)
	countA, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count(stateA): %v", err)
	}

	if err = setState(stateB); err != nil {
		t.Fatalf("BatchSet(stateB): %v", err)
	}
	if _, err := db.QueryKeys(q); err != nil {
		t.Fatalf("warm QueryKeys(stateB): %v", err)
	}
	wantB, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(stateB): %v", err)
	}
	itemsB, err := db.Query(q)
	if err != nil {
		t.Fatalf("Query(stateB): %v", err)
	}
	namesB := queryExtItemNames(t, itemsB)
	countB, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count(stateB): %v", err)
	}

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA reset): %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 16)
	start := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 120; i++ {
			vals := stateB
			if i%2 == 1 {
				vals = stateA
			}
			if err := setState(vals); err != nil {
				errCh <- fmt.Errorf("writer: %w", err)
				return
			}
		}
	}()

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			<-start
			for i := 0; i < 160; i++ {
				gotKeys, err := db.QueryKeys(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys: %w", gid, i, err)
					return
				}
				if !queryIDsEqual(q, gotKeys, wantA) && !queryIDsEqual(q, gotKeys, wantB) {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys hybrid snapshot: got=%v wantA=%v wantB=%v", gid, i, gotKeys, wantA, wantB)
					return
				}

				gotItems, err := db.Query(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Query: %w", gid, i, err)
					return
				}
				gotNames, ok := queryExtItemNamesOK(gotItems)
				if !ok {
					errCh <- fmt.Errorf("g=%d i=%d Query returned nil item: %#v", gid, i, gotItems)
					return
				}
				if !reflect.DeepEqual(gotNames, namesA) && !reflect.DeepEqual(gotNames, namesB) {
					errCh <- fmt.Errorf("g=%d i=%d Query hybrid snapshot: got=%v wantA=%v wantB=%v", gid, i, gotNames, namesA, namesB)
					return
				}

				gotCount, err := db.Count(q.Filter)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Count: %w", gid, i, err)
					return
				}
				if gotCount != countA && gotCount != countB {
					errCh <- fmt.Errorf("g=%d i=%d Count hybrid snapshot: got=%d wantA=%d wantB=%d", gid, i, gotCount, countA, countB)
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
