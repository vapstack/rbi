package rbi

import (
	"fmt"
	"path/filepath"
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
	out, used, err := db.tryLimitQueryOrderBasic(qOrder, leaves, nil)
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
	out, used, err = db.tryLimitQueryRangeNoOrderByField(qRange, f, bounds, rest, nil)
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

func TestPostingUnionIter_SmallUnionAvoidsDuplicates(t *testing.T) {
	it := newPostingUnionIter([]postingList{
		postingOf(1, 2, 5),
		postingOf(2, 3),
		postingOf(1, 4),
	})

	var got []uint64
	for it.HasNext() {
		got = append(got, it.Next())
	}

	want := []uint64{1, 2, 5, 3, 4}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("small union mismatch: got=%v want=%v", got, want)
	}
}

func TestPostingUnionIter_ReusesScratchWithoutStaleState(t *testing.T) {
	posts := []postingList{
		postingOf(1, 2, 5),
		postingOf(2, 3),
		postingOf(1, 4),
		postingOf(6, 7, 8),
	}

	drain := func() []uint64 {
		it := newPostingUnionIter(posts)
		defer releaseRoaringIter(it)
		var out []uint64
		for it.HasNext() {
			out = append(out, it.Next())
		}
		return out
	}

	first := drain()
	second := drain()
	want := []uint64{1, 2, 5, 3, 4, 6, 7, 8}
	if !reflect.DeepEqual(first, want) {
		t.Fatalf("first union mismatch: got=%v want=%v", first, want)
	}
	if !reflect.DeepEqual(second, want) {
		t.Fatalf("second union mismatch: got=%v want=%v", second, want)
	}
}

func TestPostingUnionIter_AllocsPerRunStaysLowAfterWarmup(t *testing.T) {
	if testRaceEnabled {
		t.Skip("testing.AllocsPerRun is not stable under -race")
	}

	posts := []postingList{
		postingOf(1, 2, 5),
		postingOf(2, 3),
		postingOf(1, 4),
		postingOf(6, 7, 8),
	}

	warm := newPostingUnionIter(posts)
	releaseRoaringIter(warm)

	allocs := testing.AllocsPerRun(100, func() {
		it := newPostingUnionIter(posts)
		for it.HasNext() {
			_ = it.Next()
		}
		releaseRoaringIter(it)
	})
	if allocs > 0.2 {
		t.Fatalf("unexpected allocs per run: got=%v want<=0.2", allocs)
	}
}

type orderBasicHighCardPrefixRec struct {
	Score  float64 `db:"score"`
	Email  string  `db:"email"`
	Status string  `db:"status"`
	Plan   string  `db:"plan"`
}

func TestQuery_OrderBasicWithLimit_SkipsHighCardNonOrderPrefixShape(t *testing.T) {
	dir := t.TempDir()
	db, raw := openBoltAndNew[uint64, orderBasicHighCardPrefixRec](t, filepath.Join(dir, "test_order_basic_high_card.db"), Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	for i := 1; i <= 2_000; i++ {
		status := "paused"
		if i%2 == 0 {
			status = "active"
		}
		plan := "free"
		if i%4 != 0 {
			plan = "pro"
		}
		if err := db.Set(uint64(i), &orderBasicHighCardPrefixRec{
			Score:  float64(i),
			Email:  fmt.Sprintf("user%06d@example.com", i),
			Status: status,
			Plan:   plan,
		}); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.PREFIX("email", "user0019"),
		qx.EQ("status", "active"),
		qx.NOTIN("plan", []string{"free"}),
	).By("score", qx.DESC).Max(20)

	out, used, err := db.tryQueryOrderBasicWithLimit(q, nil)
	if err != nil {
		t.Fatalf("tryQueryOrderBasicWithLimit: %v", err)
	}
	if used {
		t.Fatalf("expected high-card non-order prefix shape to skip order-basic fast path, got %v", out)
	}

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	want := []uint64{1998, 1994, 1990, 1986, 1982, 1978, 1974, 1970, 1966, 1962, 1958, 1954, 1950, 1946, 1942, 1938, 1934, 1930, 1926, 1922}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ordered result mismatch: got=%v want=%v", got, want)
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

func TestQuery_OrderBasic_RangeBaseOpsSkipsEagerBaseBitmapBeforeOrderedPredicates(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                                  -1,
		SnapshotCompactorRequestEveryNWrites:             1 << 30,
		SnapshotCompactorIdleInterval:                    -1,
		SnapshotDeltaLayerMaxDepth:                       1 << 30,
		SnapshotMaterializedPredCacheMaxEntries:          16,
		SnapshotMaterializedPredCacheMaxEntriesWithDelta: 16,
	})

	setNumericBucketKnobs(t, db, 128, 1, 1)
	seedGeneratedUint64Data(t, db, 5_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Age:    i,
			Score:  float64(i),
			Active: true,
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}
	if err := db.Patch(1, []Field{{Name: "age", Value: 10_000}}); err != nil {
		t.Fatalf("Patch(age): %v", err)
	}

	snap := db.getSnapshot()
	if snap.fieldDelta("age") == nil {
		t.Fatalf("expected active age delta")
	}
	if got := snap.matPredCacheCount.Load(); got != 0 {
		t.Fatalf("unexpected materialized predicate cache before query: %d", got)
	}

	q := qx.Query(
		qx.LT("score", 4_000.0),
	).By("age", qx.ASC).Max(5)

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertSameSlice(t, got, want)

	after := db.getSnapshot()
	if got := after.matPredCacheCount.Load(); got != 0 {
		t.Fatalf("expected ordered predicate path to avoid eager base bitmap materialization, cache entries=%d", got)
	}
}

func TestQuery_OrderBasic_DeepWindowEagerMaterializesNonOrderNumericRange(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                                  -1,
		SnapshotCompactorRequestEveryNWrites:             1 << 30,
		SnapshotCompactorIdleInterval:                    -1,
		SnapshotDeltaLayerMaxDepth:                       1 << 30,
		SnapshotMaterializedPredCacheMaxEntries:          16,
		SnapshotMaterializedPredCacheMaxEntriesWithDelta: 16,
	})

	setNumericBucketKnobs(t, db, 128, 1, 1)
	seedGeneratedUint64Data(t, db, 5_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Age:    i,
			Score:  float64(i),
			Active: true,
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	small := qx.Query(
		qx.LT("score", 4_000.0),
	).By("age", qx.ASC).Max(5)
	if _, err := db.QueryKeys(small); err != nil {
		t.Fatalf("small QueryKeys: %v", err)
	}
	if got := db.getSnapshot().matPredCacheCount.Load(); got != 0 {
		t.Fatalf("unexpected materialized predicate cache for small ordered window: %d", got)
	}

	deep := qx.Query(
		qx.LT("score", 4_000.0),
	).By("age", qx.ASC).Skip(2_000).Max(10)
	got, err := db.QueryKeys(deep)
	if err != nil {
		t.Fatalf("deep QueryKeys: %v", err)
	}
	want, err := expectedKeysUint64(t, db, deep)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertSameSlice(t, got, want)

	if got := db.getSnapshot().matPredCacheCount.Load(); got == 0 {
		t.Fatalf("expected deep ordered window to materialize numeric range predicate")
	}
}

func TestQuery_OrderBasic_DeepWindowCachePersistsAcrossUnchangedFieldPatch(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                                  -1,
		SnapshotCompactorRequestEveryNWrites:             1 << 30,
		SnapshotCompactorIdleInterval:                    -1,
		SnapshotDeltaLayerMaxDepth:                       1 << 30,
		SnapshotMaterializedPredCacheMaxEntries:          16,
		SnapshotMaterializedPredCacheMaxEntriesWithDelta: 16,
	})

	setNumericBucketKnobs(t, db, 128, 1, 1)
	seedGeneratedUint64Data(t, db, 5_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Age:    i,
			Score:  float64(i),
			Active: true,
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.LT("score", 4_000.0),
	).By("age", qx.ASC).Skip(2_000).Max(10)
	if _, err := db.QueryKeys(q); err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}

	keyValue, isSlice, err := db.exprValueToIdxScalar(qx.Expr{Op: qx.OpLT, Field: "score", Value: 4_000.0})
	if err != nil {
		t.Fatalf("exprValueToIdxScalar: %v", err)
	}
	if isSlice {
		t.Fatalf("unexpected slice scalar key")
	}
	cacheKey := materializedPredCacheKeyFromScalar("score", qx.OpLT, keyValue)
	prevSnap := db.getSnapshot()
	prevBM, ok := prevSnap.loadMaterializedPred(cacheKey)
	if !ok || prevBM == nil {
		t.Fatalf("expected score range cache entry before unrelated patch")
	}

	if err := db.Patch(1, []Field{{Name: "active", Value: false}}); err != nil {
		t.Fatalf("Patch(active): %v", err)
	}

	nextSnap := db.getSnapshot()
	nextBM, ok := nextSnap.loadMaterializedPred(cacheKey)
	if !ok || nextBM == nil {
		t.Fatalf("expected score range cache entry after unrelated patch")
	}
	if nextBM != prevBM {
		t.Fatalf("expected unchanged-field cache entry to be inherited across snapshot publish")
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
