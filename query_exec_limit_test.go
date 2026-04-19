package rbi

import (
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qir"
)

func mustLimitQIRExpr[K ~string | ~uint64, V any](t testing.TB, db *DB[K, V], expr qx.Expr) qir.Expr {
	t.Helper()
	prepared, compiled, err := prepareTestExpr(db, expr)
	if err != nil {
		t.Fatalf("prepareTestExpr(%+v): %v", expr, err)
	}
	compiled = detachTestQIRExpr(compiled)
	prepared.Release()
	return compiled
}

func mustLimitQIRLeaves[K ~string | ~uint64, V any](t testing.TB, db *DB[K, V], expr qx.Expr) []qir.Expr {
	t.Helper()
	leaves, ok := collectAndLeaves(mustLimitQIRExpr(t, db, expr))
	if !ok {
		t.Fatalf("collectAndLeaves failed")
	}
	return leaves
}

func filterQIRLeavesByField[K ~string | ~uint64, V any](db *DB[K, V], leaves []qir.Expr, field string) []qir.Expr {
	out := make([]qir.Expr, 0, len(leaves))
	for _, leaf := range leaves {
		if testExprFieldName(db, leaf) == field {
			continue
		}
		out = append(out, leaf)
	}
	return out
}

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
			q:    qx.Query(qx.EQ("email", "missing@example.com")).Limit(8),
		},
		{
			name: "in_all_missing",
			q:    qx.Query(qx.IN("email", []string{"x@example.com", "y@example.com"})).Limit(8),
		},
		{
			name: "has_missing",
			q:    qx.Query(qx.HASALL("tags", []string{"missing"})).Limit(8),
		},
		{
			name: "hasany_all_missing",
			q:    qx.Query(qx.HASANY("tags", []string{"missing-1", "missing-2"})).Limit(8),
		},
		{
			name: "and_hit_plus_missing",
			q: qx.Query(
				qx.EQ("name", "alice"),
				qx.EQ("email", "missing@example.com"),
			).Limit(8),
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

func TestQuery_LimitNoFilterNoOrder_UsesDirectLimitPlan(t *testing.T) {
	var events []TraceEvent
	opts := Options{
		TraceSink: func(ev TraceEvent) {
			events = append(events, ev)
		},
		TraceSampleEvery: 1,
	}
	db, _ := openTempDBUint64(t, opts)

	for i := 1; i <= 5; i++ {
		if err := db.Set(uint64(i), &Rec{Name: fmt.Sprintf("u%d", i), Age: 20 + i}); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}

	ids, err := db.QueryKeys(qx.Query().Limit(3))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if !reflect.DeepEqual(ids, []uint64{1, 2, 3}) {
		t.Fatalf("unexpected ids: got=%v want=%v", ids, []uint64{1, 2, 3})
	}
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	last := events[len(events)-1]
	if last.Plan != string(PlanLimit) {
		t.Fatalf("expected plan %q, got %q", PlanLimit, last.Plan)
	}
	if last.RowsExamined != 3 {
		t.Fatalf("expected RowsExamined=3, got trace=%+v", last)
	}
}

func TestQuery_RangeNoOrderWithLimit_NilEQ_UsesNilOverlayInEarlyRoute(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "nil-a", Opt: nil}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "value"}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}
	if err := db.Set(3, &Rec{Name: "nil-b", Opt: nil}); err != nil {
		t.Fatalf("Set(3): %v", err)
	}

	ids, err := db.QueryKeys(qx.Query(qx.EQ("opt", nil)).Limit(1))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected one id from nil equality limit query, got %v", ids)
	}

	items, err := db.Query(qx.Query(qx.EQ("opt", nil)).Limit(1))
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(items) != 1 || items[0] == nil || items[0].Opt != nil {
		t.Fatalf("expected one nil-opt item, got %#v", items)
	}
	db.ReleaseRecords(items...)
}

func TestQuery_SingleExactOrderedLimit_PrefersPlannerLimitPath(t *testing.T) {
	db, _ := openTempDBUint64(t)

	for i := 1; i <= 32; i++ {
		if err := db.Set(uint64(i), &Rec{
			Name:   fmt.Sprintf("u%d", i),
			Age:    18 + (i % 17),
			Active: i%2 == 0,
		}); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}

	q := qx.Query(
		qx.EQ("active", true),
	).Sort("age", qx.ASC).Limit(8)

	if db.shouldPreferExecutionPlan(q) {
		t.Fatalf("expected single exact ordered limit to prefer planner/limit path")
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
	).Sort("age", qx.ASC).Limit(10)
	leaves := mustExtractAndLeaves(t, qOrder.Filter)
	out, used, err := db.tryLimitQueryOrderBasic(qOrder, leaves, nil)
	if err != nil {
		t.Fatalf("tryLimitQueryOrderBasic: %v", err)
	}
	if used && len(out) != 0 {
		t.Fatalf("expected empty result from tryLimitQueryOrderBasic, got %v", out)
	}
	gotOrder, err := db.QueryKeys(qOrder)
	if err != nil {
		t.Fatalf("QueryKeys(order): %v", err)
	}
	if len(gotOrder) != 0 {
		t.Fatalf("expected empty result from QueryKeys(order), got %v", gotOrder)
	}

	qRange := qx.Query(
		qx.GTE("age", 20),
		qx.LT("age", 40),
		qx.EQ("email", "missing@example.com"),
	).Limit(10)
	rangeLeaves := mustExtractAndLeaves(t, qRange.Filter)
	f, bounds, ok, err := db.extractNoOrderBounds(rangeLeaves)
	if err != nil {
		t.Fatalf("extractNoOrderBounds: %v", err)
	}
	if !ok {
		t.Fatalf("expected no-order range bounds to be recognized")
	}
	out, used, err = db.tryLimitQueryRangeNoOrderByField(qRange, f, bounds, rangeLeaves, nil)
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

func TestPlannerFilterPostingByLeafChecks_PreferredExactBypassesSmallBucketFallback(t *testing.T) {
	src := posting.BuildFromSorted([]uint64{1, 3, 5, 7, 9, 11, 13, 15})
	defer src.Release()

	postA := posting.BuildFromSorted([]uint64{3, 7, 11})
	postB := posting.BuildFromSorted([]uint64{5, 7, 13})
	defer postA.Release()
	defer postB.Release()

	postsBuf := postingSlicePool.Get()
	postsBuf.Append(postA)
	postsBuf.Append(postB)

	state := postsAnyFilterStatePool.Get()
	state.postsBuf = postsBuf

	preds := leafPredSlicePool.Get()
	preds.Append(leafPred{
		kind:          leafPredKindPostsUnion,
		postsBuf:      postsBuf,
		postsAnyState: state,
	})
	defer leafPredSlicePool.Put(preds)

	mode, exact, work, card := plannerFilterPostingByLeafChecks(preds, []int{0}, src.Borrow(), posting.List{}, false)
	defer work.Release()
	if mode != plannerPredicateBucketExact {
		t.Fatalf("unexpected mode: got=%v want=%v", mode, plannerPredicateBucketExact)
	}
	if card != src.Cardinality() {
		t.Fatalf("unexpected source cardinality: got=%d want=%d", card, src.Cardinality())
	}
	if got := exact.Cardinality(); got != 5 {
		t.Fatalf("unexpected exact cardinality: got=%d want=5", got)
	}
	for _, idx := range []uint64{3, 5, 7, 11, 13} {
		if !exact.Contains(idx) {
			t.Fatalf("exact posting is missing id %d", idx)
		}
	}
}

func TestLeafPred_PostsAnyStateContainsIdxAndCountBucketUseRuntimeState(t *testing.T) {
	postA := posting.BuildFromSorted([]uint64{1, 3, 5, 7, 9, 11, 13})
	postB := posting.BuildFromSorted([]uint64{5, 7, 9, 15, 17, 19})
	bucket := posting.BuildFromSorted([]uint64{1, 2, 5, 6, 7, 9, 14, 15, 17})
	defer bucket.Release()

	postsBuf := postingSlicePool.Get()
	postsBuf.Append(postA)
	postsBuf.Append(postB)

	state := postsAnyFilterStatePool.Get()
	state.postsBuf = postsBuf
	state.containsMaterializeAt = 1

	pred := leafPred{
		kind:          leafPredKindPostsUnion,
		postsBuf:      postsBuf,
		postsAnyState: state,
	}

	defer func() {
		postsAnyFilterStatePool.Put(state)
		for i := 0; i < postsBuf.Len(); i++ {
			postsBuf.Get(i).Release()
			postsBuf.Set(i, posting.List{})
		}
		postingSlicePool.Put(postsBuf)
	}()

	if !pred.containsIdx(7) {
		t.Fatalf("expected runtime state to match existing id")
	}
	if pred.containsIdx(6) {
		t.Fatalf("unexpected match for missing id")
	}
	if state.ids.IsEmpty() {
		t.Fatalf("expected containsIdx to materialize union through runtime state")
	}

	cnt, ok := pred.countBucket(bucket)
	if !ok {
		t.Fatalf("expected runtime state countBucket to stay exact")
	}
	if cnt != 6 {
		t.Fatalf("unexpected runtime state bucket count: got=%d want=6", cnt)
	}
}

func TestOrderPredicatesEmitPostingReader_SingleBucketCountSkipsWithoutMatches(t *testing.T) {
	ids := posting.BuildFromSorted([]uint64{1, 2, 3, 4, 5, 6, 7})
	defer ids.Release()

	preds := predicateSliceView([]predicate{
		{
			kind: predicateKindCustom,
			contains: func(uint64) bool {
				panic("matches must not be called when single-check bucket skip succeeds")
			},
			bucketCount: func(bucket posting.List) (uint64, bool) {
				if !bucket.SharesPayload(ids) {
					t.Fatalf("unexpected bucket passed to countBucket")
				}
				return 4, true
			},
		},
	})

	cursor := queryCursor[uint64, Rec]{
		skip: 4,
		need: 1,
	}
	examined := uint64(0)
	stop, nextWork := orderPredicatesEmitPostingReader(
		&cursor,
		preds,
		[]int{0},
		nil,
		nil,
		false,
		ids.Borrow(),
		posting.List{},
		nil,
		&examined,
	)
	defer nextWork.Release()

	if stop {
		t.Fatalf("unexpected stop on pure skip")
	}
	if cursor.skip != 0 {
		t.Fatalf("unexpected remaining skip: got=%d want=0", cursor.skip)
	}
	if len(cursor.out) != 0 {
		t.Fatalf("unexpected emitted keys during skip: %v", cursor.out)
	}
	if examined != ids.Cardinality() {
		t.Fatalf("unexpected examined rows: got=%d want=%d", examined, ids.Cardinality())
	}
}

func TestApplyBoundsToIndexRange_PrefixIntersectsRange(t *testing.T) {
	s := []index{
		{Key: indexKeyFromString("aa")},
		{Key: indexKeyFromString("ab")},
		{Key: indexKeyFromString("ac")},
		{Key: indexKeyFromString("ad")},
		{Key: indexKeyFromString("b0")},
	}

	rb := rangeBounds{has: true}
	rb.applyPrefix("a")
	rb.applyLo("ab", true)
	rb.applyHi("ad", false)

	start, end := applyBoundsToIndexRange(s, rb)
	if start != 1 || end != 3 {
		t.Fatalf("unexpected range: start=%d end=%d", start, end)
	}

	got := make([]string, 0, end-start)
	for _, ent := range s[start:end] {
		got = append(got, ent.Key.asUnsafeString())
	}
	want := []string{"ab", "ac"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("keys mismatch: got=%v want=%v", got, want)
	}

	empty := rangeBounds{has: true}
	empty.applyPrefix("b")
	empty.applyHi("aa", true)
	start, end = applyBoundsToIndexRange(s, empty)
	if start != 0 || end != 0 {
		t.Fatalf("expected empty range, got start=%d end=%d", start, end)
	}
}

func TestPostingUnionIter_SmallUnionAvoidsDuplicates(t *testing.T) {
	it := newPostingUnionIter([]posting.List{
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
	posts := []posting.List{
		postingOf(1, 2, 5),
		postingOf(2, 3),
		postingOf(1, 4),
		postingOf(6, 7, 8),
	}

	drain := func() []uint64 {
		it := newPostingUnionIter(posts)
		if it != nil {
			defer it.Release()
		}
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

	posts := []posting.List{
		postingOf(1, 2, 5),
		postingOf(2, 3),
		postingOf(1, 4),
		postingOf(6, 7, 8),
	}

	warm := newPostingUnionIter(posts)
	if warm != nil {
		warm.Release()
	}

	allocs := testing.AllocsPerRun(100, func() {
		it := newPostingUnionIter(posts)
		for it.HasNext() {
			_ = it.Next()
		}
		if it != nil {
			it.Release()
		}
	})
	if allocs > 0.2 {
		t.Fatalf("unexpected allocs per run: got=%v want<=0.2", allocs)
	}
}

func TestPostingUnionBufIter_SmallUnionAllocsPerRunStayZeroAfterWarmup(t *testing.T) {
	if testRaceEnabled {
		t.Skip("testing.AllocsPerRun is not stable under -race")
	}

	posts := [...]posting.List{
		postingOf(1, 2, 5),
		postingOf(2, 3),
		postingOf(1, 4),
	}
	for i := range posts {
		defer posts[i].Release()
	}

	postsBuf := postingSlicePool.Get()
	postsBuf.Append(posts[0])
	postsBuf.Append(posts[1])
	postsBuf.Append(posts[2])
	defer postingSlicePool.Put(postsBuf)

	warm := newPostingUnionBufIter(postsBuf)
	if warm != nil {
		warm.Release()
	}

	allocs := testing.AllocsPerRun(100, func() {
		it := newPostingUnionBufIter(postsBuf)
		for it.HasNext() {
			_ = it.Next()
		}
		if it != nil {
			it.Release()
		}
	})
	if allocs != 0 {
		t.Fatalf("expected zero allocs after warmup, got %.2f", allocs)
	}
}

func TestU64Set_AllocsPerRunStayZeroAfterWarmup(t *testing.T) {
	if testRaceEnabled {
		t.Skip("testing.AllocsPerRun is not stable under -race")
	}

	warm := newU64Set(64)
	for i := 0; i < 32; i++ {
		_ = warm.Add(uint64(i))
	}
	releaseU64Set(&warm)

	allocs := testing.AllocsPerRun(100, func() {
		set := newU64Set(64)
		for i := 0; i < 32; i++ {
			_ = set.Add(uint64(i))
		}
		releaseU64Set(&set)
	})
	if allocs > 0 {
		t.Fatalf("unexpected allocs per run: got=%v want=0", allocs)
	}
}

type orderBasicHighCardPrefixRec struct {
	Score  float64 `db:"score"  dbi:"default"`
	Email  string  `db:"email"  dbi:"default"`
	Status string  `db:"status" dbi:"default"`
	Plan   string  `db:"plan"   dbi:"default"`
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
	).Sort("score", qx.DESC).Limit(20)

	out, used, err := db.tryQueryOrderBasicWithLimit(q, nil)
	if err != nil {
		t.Fatalf("tryQueryOrderBasicWithLimit: %v", err)
	}
	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if used && !reflect.DeepEqual(out, got) {
		t.Fatalf("order-basic result mismatch: got=%v want=%v", out, got)
	}
	want := []uint64{1998, 1994, 1990, 1986, 1982, 1978, 1974, 1970, 1966, 1962, 1958, 1954, 1950, 1946, 1942, 1938, 1934, 1930, 1926, 1922}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ordered result mismatch: got=%v want=%v", got, want)
	}
}

func TestQuery_OffsetBeyondResult_ReturnsEmpty(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 80)

	q := qx.Query(qx.EQ("country", "NL")).Offset(10_000).Limit(50)

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

	q := qx.Query(qx.GTE("age", 18)).Offset(35)

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

func baselineEmitAcceptedPostingNoOrder[K ~uint64 | ~string, V any](cursor *queryCursor[K, V], ids posting.List, examined *uint64) bool {
	if ids.IsEmpty() {
		return false
	}
	stop := false
	ids.ForEach(func(idx uint64) bool {
		*examined++
		if cursor.emit(idx) {
			stop = true
			return false
		}
		return true
	})
	return stop
}

func (qv *queryView[K, V]) baselineTryQueryRangeNoOrderWithLimit(q *qx.QX) ([]K, bool, error) {
	prepared, viewQ, err := prepareTestQuery(qv.root, q)
	if err != nil {
		return nil, false, err
	}
	defer prepared.Release()

	if viewQ.HasOrder || viewQ.Limit == 0 {
		return nil, false, nil
	}
	if viewQ.Expr.Not {
		return nil, false, nil
	}

	e := viewQ.Expr
	if e.Op == qir.OpAND || e.Op == qir.OpOR || len(e.Operands) != 0 {
		return nil, false, nil
	}

	switch e.Op {
	case qir.OpGT, qir.OpGTE, qir.OpLT, qir.OpLTE, qir.OpEQ:
	default:
		return nil, false, nil
	}

	f := qv.fieldNameByExpr(e)
	if f == "" {
		return nil, false, nil
	}

	fm := qv.fields[f]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}

	ov := qv.fieldOverlay(f)
	if !ov.hasData() {
		return nil, true, nil
	}

	isNil := false
	rb := rangeBounds{has: true}
	if e.Op == qir.OpEQ {
		key, isSlice, eqNil, err := qv.exprValueToIdxScalar(e)
		if err != nil {
			return nil, true, err
		}
		if isSlice {
			return nil, false, nil
		}
		isNil = eqNil
		if isNil {
			ids := qv.nilFieldLookupPostingRetained(f)
			if ids.IsEmpty() {
				return nil, true, nil
			}
			out := make([]K, 0, viewQ.Limit)
			cursor := qv.newQueryCursor(out, viewQ.Offset, viewQ.Limit, false, 0)
			var examined uint64
			baselineEmitAcceptedPostingNoOrder(&cursor, ids, &examined)
			return cursor.out, true, nil
		}
		rb.applyLo(key, true)
		rb.applyHi(key, true)
	} else {
		bound, isSlice, err := qv.exprValueToNormalizedScalarBound(e)
		if err != nil {
			return nil, true, err
		}
		if isSlice {
			return nil, false, nil
		}
		applyNormalizedScalarBound(&rb, bound)
	}

	br := ov.rangeForBounds(rb)
	if overlayRangeEmpty(br) {
		return nil, true, nil
	}

	out := make([]K, 0, viewQ.Limit)
	cursor := qv.newQueryCursor(out, viewQ.Offset, viewQ.Limit, false, 0)

	keyCur := ov.newCursor(br, false)
	var examined uint64
	for {
		_, ids, ok := keyCur.next()
		if !ok {
			break
		}
		if ids.IsEmpty() {
			continue
		}
		if baselineEmitAcceptedPostingNoOrder(&cursor, ids, &examined) {
			return cursor.out, true, nil
		}
	}
	return cursor.out, true, nil
}

func (qv *queryView[K, V]) baselineTryQueryPrefixNoOrderWithLimit(q *qx.QX) ([]K, bool, error) {
	prepared, viewQ, err := prepareTestQuery(qv.root, q)
	if err != nil {
		return nil, false, err
	}
	defer prepared.Release()

	if viewQ.HasOrder || viewQ.Limit == 0 {
		return nil, false, nil
	}
	if viewQ.Expr.Not {
		return nil, false, nil
	}

	e := viewQ.Expr
	f := qv.fieldNameByExpr(e)
	if e.Op != qir.OpPREFIX || f == "" {
		return nil, false, nil
	}
	if len(e.Operands) != 0 {
		return nil, false, nil
	}

	fm := qv.fields[f]
	if fm == nil || fm.Slice {
		return nil, false, nil
	}

	bound, isSlice, err := qv.exprValueToNormalizedScalarBound(e)
	if err != nil {
		return nil, true, err
	}
	if isSlice {
		return nil, false, nil
	}
	if bound.empty || bound.full {
		return nil, true, nil
	}

	ov := qv.fieldOverlay(f)
	if !ov.hasData() {
		if !qv.hasIndexedField(f) {
			return nil, true, fmt.Errorf("no index for field: %v", f)
		}
		return nil, true, nil
	}

	br := ov.rangeForBounds(rangeBounds{
		has:       true,
		hasPrefix: true,
		prefix:    bound.key,
	})
	if overlayRangeEmpty(br) {
		return nil, true, nil
	}

	out := make([]K, 0, viewQ.Limit)
	cursor := qv.newQueryCursor(out, viewQ.Offset, viewQ.Limit, false, 0)

	keyCur := ov.newCursor(br, false)
	var examined uint64
	for {
		_, ids, ok := keyCur.next()
		if !ok {
			break
		}
		if ids.IsEmpty() {
			continue
		}
		if baselineEmitAcceptedPostingNoOrder(&cursor, ids, &examined) {
			return cursor.out, true, nil
		}
	}
	return cursor.out, true, nil
}

func baselineScanLimitByOverlayBounds[K ~uint64 | ~string, V any](db *queryView[K, V], q *qx.QX, ov fieldOverlay, br overlayRange, desc bool, preds *pooled.SliceBuf[leafPred], nilTailField string) []K {
	limit := int(q.Window.Limit)
	out := make([]K, 0, limit)
	cursor := db.newQueryCursor(out, 0, q.Window.Limit, false, 0)
	predCount := 0
	if preds != nil {
		predCount = preds.Len()
	}

	emitCandidate := func(idx uint64) bool {
		for i := 0; i < predCount; i++ {
			if !preds.Get(i).containsIdx(idx) {
				return false
			}
		}
		return cursor.emit(idx)
	}

	emitBucketPosting := func(ids posting.List) bool {
		if ids.IsEmpty() {
			return false
		}
		if idx, ok := ids.TrySingle(); ok {
			return emitCandidate(idx)
		}
		it := ids.Iter()
		defer it.Release()
		for it.HasNext() {
			if emitCandidate(it.Next()) {
				return true
			}
		}
		return false
	}

	keyCur := ov.newCursor(br, desc)
	for {
		_, ids, ok := keyCur.next()
		if !ok {
			break
		}
		if ids.IsEmpty() {
			continue
		}
		if emitBucketPosting(ids) {
			return cursor.out
		}
	}

	if nilTailField != "" {
		ids := db.nilFieldOverlay(nilTailField).lookupPostingRetained(nilIndexEntryKey)
		if !ids.IsEmpty() && emitBucketPosting(ids) {
			return cursor.out
		}
	}

	return cursor.out
}

func TestQuery_RangeNoOrderWithLimit_DeepOffset_MatchesExpected(t *testing.T) {
	db, _ := openTempDBUint64(t)
	seedGeneratedUint64Data(t, db, 10_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("u_%d@example.test", i),
			Age:    i % 64,
			Score:  float64(i),
			Active: i%2 == 0,
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(qx.GTE("age", 20)).Offset(4_000).Limit(25)

	got, used, err := db.tryQueryRangeNoOrderWithLimit(q, nil)
	if err != nil {
		t.Fatalf("tryQueryRangeNoOrderWithLimit: %v", err)
	}
	if !used {
		t.Fatalf("expected range no-order fast path to be used")
	}

	want, used, err := db.currentQueryViewForTests().baselineTryQueryRangeNoOrderWithLimit(q)
	if err != nil {
		t.Fatalf("baselineTryQueryRangeNoOrderWithLimit: %v", err)
	}
	if !used {
		t.Fatalf("expected baseline range no-order fast path to be used")
	}
	assertSameSlice(t, got, want)
}

func TestQuery_PrefixNoOrderWithLimit_DeepOffset_MatchesExpected(t *testing.T) {
	db, _ := openTempDBUint64(t)
	seedGeneratedUint64Data(t, db, 10_000, func(i int) *Rec {
		return &Rec{
			Name:     fmt.Sprintf("u_%d", i),
			Email:    fmt.Sprintf("u_%d@example.test", i),
			Age:      i % 64,
			Score:    float64(i),
			Active:   i%2 == 0,
			FullName: fmt.Sprintf("grp-%02d", i%100),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(qx.PREFIX("full_name", "grp-1")).Offset(750).Limit(30)

	got, used, err := db.tryQueryPrefixNoOrderWithLimit(q, nil)
	if err != nil {
		t.Fatalf("tryQueryPrefixNoOrderWithLimit: %v", err)
	}
	if !used {
		t.Fatalf("expected prefix no-order fast path to be used")
	}

	want, used, err := db.currentQueryViewForTests().baselineTryQueryPrefixNoOrderWithLimit(q)
	if err != nil {
		t.Fatalf("baselineTryQueryPrefixNoOrderWithLimit: %v", err)
	}
	if !used {
		t.Fatalf("expected baseline prefix no-order fast path to be used")
	}
	assertSameSlice(t, got, want)
}

func TestQuery_RangeNoOrderWithLimit_NilEQDeepOffset_MatchesExpected(t *testing.T) {
	db, _ := openTempDBUint64(t)
	seedGeneratedUint64Data(t, db, 9_000, func(i int) *Rec {
		var opt *string
		if i%3 == 0 {
			v := fmt.Sprintf("v-%d", i%17)
			opt = &v
		}
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("u_%d@example.test", i),
			Age:    i % 64,
			Score:  float64(i),
			Active: i%2 == 0,
			Opt:    opt,
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(qx.EQ("opt", nil)).Offset(2_500).Limit(40)

	got, used, err := db.tryQueryRangeNoOrderWithLimit(q, nil)
	if err != nil {
		t.Fatalf("tryQueryRangeNoOrderWithLimit(nil EQ): %v", err)
	}
	if !used {
		t.Fatalf("expected nil-equality range fast path to be used")
	}

	want, used, err := db.currentQueryViewForTests().baselineTryQueryRangeNoOrderWithLimit(q)
	if err != nil {
		t.Fatalf("baselineTryQueryRangeNoOrderWithLimit(nil EQ): %v", err)
	}
	if !used {
		t.Fatalf("expected baseline nil-equality range fast path to be used")
	}
	assertSameSlice(t, got, want)
}

func TestQuery_LimitRangeNoOrder_ResidualsUseBucketExactFilter(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	db, _ := openTempDBUint64(t, Options{
		TraceSink: func(ev TraceEvent) {
			mu.Lock()
			events = append(events, ev)
			mu.Unlock()
		},
		TraceSampleEvery: 1,
	})

	seedGeneratedUint64Data(t, db, 12_000, func(i int) *Rec {
		countries := [...]string{"NL", "DE", "US", "GB"}
		group := (i - 1) / 64
		return &Rec{
			Name:     fmt.Sprintf("u_%d", i),
			Email:    fmt.Sprintf("u_%d@example.test", i),
			Age:      i % 64,
			Active:   group%2 == 0,
			Score:    float64(i),
			Meta:     Meta{Country: countries[group%len(countries)]},
			FullName: fmt.Sprintf("grp-%02d", i%64),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.GTE("age", 0),
		qx.LT("age", 64),
		qx.IN("country", []string{"NL", "DE"}),
		qx.EQ("active", true),
	).Limit(25)

	leaves := mustExtractAndLeaves(t, q.Filter)
	f, bounds, ok, err := db.extractNoOrderBounds(leaves)
	if err != nil {
		t.Fatalf("extractNoOrderBounds: %v", err)
	}
	if !ok {
		t.Fatalf("expected no-order bounds to be recognized")
	}

	view := db.currentQueryViewForTests()
	qirLeaves := mustLimitQIRLeaves(t, db, q.Filter)
	predsBuf, ok, err := view.buildLeafPredsExcludingBounds(qirLeaves, f, 0)
	if err != nil {
		t.Fatalf("buildLeafPredsExcludingBounds: %v", err)
	}
	if !ok {
		t.Fatalf("expected residual leaf preds to be supported")
	}
	if predsBuf != nil {
		defer leafPredSlicePool.Put(predsBuf)
	}

	br := view.fieldOverlay(f).rangeForBounds(bounds)
	want := baselineScanLimitByOverlayBounds(view, q, view.fieldOverlay(f), br, false, predsBuf, "")

	preparedQ, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer preparedQ.Release()

	tr := db.beginTrace(viewQ)
	if tr == nil {
		t.Fatalf("expected trace to be enabled")
	}
	got, used, err := db.tryLimitQueryRangeNoOrderByField(q, f, bounds, leaves, tr)
	tr.finish(uint64(len(got)), err)
	if err != nil {
		t.Fatalf("tryLimitQueryRangeNoOrderByField: %v", err)
	}
	if !used {
		t.Fatalf("expected range limit fast path to be used")
	}
	assertSameSlice(t, got, want)

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	if ev.PostingExactFilters == 0 {
		t.Fatalf("expected exact bucket filter usage, trace=%+v", ev)
	}
	if ev.RowsExamined == 0 || ev.RowsMatched == 0 {
		t.Fatalf("expected non-zero trace counters, trace=%+v", ev)
	}
}

func TestQuery_LimitOrderBasic_ResidualsUseBucketExactFilter(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	db, _ := openTempDBUint64(t, Options{
		TraceSink: func(ev TraceEvent) {
			mu.Lock()
			events = append(events, ev)
			mu.Unlock()
		},
		TraceSampleEvery: 1,
	})

	seedGeneratedUint64Data(t, db, 12_000, func(i int) *Rec {
		countries := [...]string{"NL", "DE", "US", "GB"}
		group := (i - 1) / 64
		return &Rec{
			Name:     fmt.Sprintf("u_%d", i),
			Email:    fmt.Sprintf("u_%d@example.test", i),
			Age:      i % 64,
			Active:   group%2 == 0,
			Score:    float64(i),
			Meta:     Meta{Country: countries[group%len(countries)]},
			FullName: fmt.Sprintf("grp-%02d", i%64),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.GTE("age", 0),
		qx.LT("age", 64),
		qx.EQ("active", true),
		qx.EQ("country", "NL"),
	).Sort("age", qx.ASC).Limit(20)

	leaves := mustExtractAndLeaves(t, q.Filter)
	view := db.currentQueryViewForTests()
	qirLeaves := mustLimitQIRLeaves(t, db, q.Filter)
	bounds, ok, err := view.extractBoundsForField("age", qirLeaves)
	if err != nil {
		t.Fatalf("extractBoundsForField: %v", err)
	}
	if !ok {
		t.Fatalf("expected order bounds to be recognized")
	}

	predsBuf, ok, err := view.buildLeafPredsExcludingBounds(qirLeaves, "age", 0)
	if err != nil {
		t.Fatalf("buildLeafPredsExcludingBounds: %v", err)
	}
	if !ok {
		t.Fatalf("expected residual leaf preds to be supported")
	}
	if predsBuf != nil {
		defer leafPredSlicePool.Put(predsBuf)
	}

	ov := view.fieldOverlay("age")
	br := ov.rangeForBounds(bounds)
	want := baselineScanLimitByOverlayBounds(view, q, ov, br, false, predsBuf, "")

	preparedQ, viewQ, err := prepareTestQuery(db, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer preparedQ.Release()

	tr := db.beginTrace(viewQ)
	if tr == nil {
		t.Fatalf("expected trace to be enabled")
	}
	got, used, err := db.tryLimitQueryOrderBasic(q, leaves, tr)
	tr.finish(uint64(len(got)), err)
	if err != nil {
		t.Fatalf("tryLimitQueryOrderBasic: %v", err)
	}
	if !used {
		t.Fatalf("expected order-basic limit fast path to be used")
	}
	assertSameSlice(t, got, want)

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	if ev.PostingExactFilters == 0 {
		t.Fatalf("expected exact bucket filter usage, trace=%+v", ev)
	}
	if ev.OrderIndexScanWidth == 0 {
		t.Fatalf("expected non-zero order scan width, trace=%+v", ev)
	}
}

func TestQuery_OrderBasic_RangeBaseOpsMaterializeBroadComplementWithoutExactSiblings(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
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

	if got := db.getSnapshot().matPredCacheCount.Load(); got != 0 {
		t.Fatalf("unexpected materialized predicate cache before query: %d", got)
	}

	q := qx.Query(
		qx.LT("score", 4_000.0),
	).Sort("age", qx.ASC).Limit(5)

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
	if got := after.matPredCacheCount.Load(); got == 0 {
		t.Fatalf("expected ordered predicate path to materialize broad complement, cache entries=%d", got)
	}
}

func TestQuery_OrderBasic_SmallAndDeepWindowMaterializeNonOrderNumericRangeWhenCostWins(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
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
	).Sort("age", qx.ASC).Limit(5)
	if _, err := db.QueryKeys(small); err != nil {
		t.Fatalf("small QueryKeys: %v", err)
	}
	if got := db.getSnapshot().matPredCacheCount.Load(); got == 0 {
		t.Fatalf("expected materialized predicate cache for small ordered window: %d", got)
	}

	deep := qx.Query(
		qx.LT("score", 4_000.0),
	).Sort("age", qx.ASC).Offset(2_000).Limit(10)
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
		t.Fatalf("expected deep ordered window to keep materialized numeric range predicate")
	}
}

func TestQuery_OrderBasic_BuildLeafPredsExcludingBounds_MaterializesBroadComplementOnFirstSightWhenCostWins(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})

	setNumericBucketKnobs(t, db, 128, 1, 1)
	seedGeneratedUint64Data(t, db, 64, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   18 + i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.GTE("score", 0.0),
		qx.GTE("age", 40),
	).Sort("score", qx.ASC).Limit(50)
	leaves := mustLimitQIRLeaves(t, db, q.Filter)

	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)

	expr := mustLimitQIRExpr(t, db, qx.GTE("age", 40))
	bound, isSlice, err := view.exprValueToNormalizedScalarBound(expr)
	if err != nil {
		t.Fatalf("exprValueToNormalizedScalarBound: %v", err)
	}
	if isSlice || bound.full || bound.empty {
		t.Fatalf("unexpected normalized bound state: slice=%v full=%v empty=%v", isSlice, bound.full, bound.empty)
	}
	cacheKey := view.materializedPredComplementKeyForNormalizedScalarBound("age", bound).String()
	if cacheKey == "" {
		t.Fatalf("expected non-empty complement cache key")
	}

	preds1, ok, err := view.buildLeafPredsExcludingBounds(leaves, "score", 4096)
	if err != nil {
		t.Fatalf("first buildLeafPredsExcludingBounds: %v", err)
	}
	if !ok {
		t.Fatalf("expected first residual leaf preds to be supported")
	}
	if preds1 == nil || preds1.Len() != 1 {
		t.Fatalf("unexpected first predicate count: %d", preds1.Len())
	}
	first := preds1.Get(0)
	if first.kind != leafPredKindPredicate {
		leafPredSlicePool.Put(preds1)
		t.Fatalf("expected predicate leaf, got kind=%v", first.kind)
	}
	if first.pred.kind != predicateKindMaterializedNot {
		leafPredSlicePool.Put(preds1)
		t.Fatalf("expected first ordered broad complement to materialize, got kind=%v", first.pred.kind)
	}
	leafPredSlicePool.Put(preds1)
	if _, ok = db.getSnapshot().loadMaterializedPred(cacheKey); !ok {
		t.Fatalf("expected shared complement cache entry after first ordered leaf build")
	}

	preds2, ok, err := view.buildLeafPredsExcludingBounds(leaves, "score", 4096)
	if err != nil {
		t.Fatalf("second buildLeafPredsExcludingBounds: %v", err)
	}
	if !ok {
		t.Fatalf("expected second residual leaf preds to be supported")
	}
	if preds2 == nil || preds2.Len() != 1 {
		t.Fatalf("unexpected second predicate count: %d", preds2.Len())
	}
	defer leafPredSlicePool.Put(preds2)
	second := preds2.Get(0)
	if second.kind != leafPredKindPredicate {
		t.Fatalf("expected predicate leaf, got kind=%v", second.kind)
	}
	if second.pred.kind != predicateKindMaterializedNot {
		t.Fatalf("expected second ordered broad complement to materialize, got kind=%v", second.pred.kind)
	}
	if _, ok := db.getSnapshot().loadMaterializedPred(cacheKey); !ok {
		t.Fatalf("expected shared complement cache entry after second ordered leaf build")
	}
}

func TestQuery_OrderBasic_BuildLeafPredsExcludingBounds_DelaysBroadComplementWithMultipleExactSiblings(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})

	setNumericBucketKnobs(t, db, 128, 1, 1)
	seedGeneratedUint64Data(t, db, 256, func(i int) *Rec {
		country := "US"
		if i%3 == 0 {
			country = "DE"
		}
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Age:    18 + i,
			Score:  float64(i),
			Active: i%2 == 0,
			Meta: Meta{
				Country: country,
			},
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.EQ("active", true),
		qx.EQ("country", "DE"),
		qx.GTE("age", 40),
	).Sort("score", qx.ASC).Limit(50)
	leaves := mustLimitQIRLeaves(t, db, q.Filter)

	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)

	expr := mustLimitQIRExpr(t, db, qx.GTE("age", 40))
	bound, isSlice, err := view.exprValueToNormalizedScalarBound(expr)
	if err != nil {
		t.Fatalf("exprValueToNormalizedScalarBound: %v", err)
	}
	if isSlice || bound.full || bound.empty {
		t.Fatalf("unexpected normalized bound state: slice=%v full=%v empty=%v", isSlice, bound.full, bound.empty)
	}
	cacheKey := view.materializedPredComplementKeyForNormalizedScalarBound("age", bound).String()
	if cacheKey == "" {
		t.Fatalf("expected non-empty complement cache key")
	}

	preds, ok, err := view.buildLeafPredsExcludingBounds(leaves, "score", 4096)
	if err != nil {
		t.Fatalf("buildLeafPredsExcludingBounds: %v", err)
	}
	if !ok {
		t.Fatalf("expected residual leaf preds to be supported")
	}
	if preds == nil || preds.Len() != 3 {
		t.Fatalf("unexpected predicate count: %d", preds.Len())
	}
	defer leafPredSlicePool.Put(preds)

	rangeIdx := -1
	exactSiblingCount := 0
	for i := 0; i < preds.Len(); i++ {
		p := preds.Get(i)
		if p.kind == leafPredKindPredicate && testExprFieldName(db, p.pred.expr) == "age" && p.pred.expr.Op == qir.OpGTE {
			rangeIdx = i
			continue
		}
		if p.supportsExactBucketPostingFilter() {
			exactSiblingCount++
		}
	}
	if rangeIdx < 0 {
		t.Fatalf("expected age range leaf")
	}
	if exactSiblingCount < 2 {
		t.Fatalf("expected multiple exact siblings, got %d", exactSiblingCount)
	}

	rangePred := preds.Get(rangeIdx)
	if rangePred.kind != leafPredKindPredicate {
		t.Fatalf("expected predicate leaf, got kind=%v", rangePred.kind)
	}
	if rangePred.pred.kind == predicateKindMaterializedNot {
		t.Fatalf("expected broad complement to stay delayed with multiple exact siblings")
	}
	if _, ok := db.getSnapshot().loadMaterializedPred(cacheKey); ok {
		t.Fatalf("unexpected shared complement cache entry on first build with multiple exact siblings")
	}
}

func TestQuery_OrderBasic_DeepWindowCachePersistsAcrossUnchangedFieldPatch(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
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
	).Sort("age", qx.ASC).Offset(2_000).Limit(10)
	if _, err := db.QueryKeys(q); err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}

	keyValue, isSlice, isNil, err := db.exprValueToIdxScalar(qx.LT("score", 4_000.0))
	if err != nil {
		t.Fatalf("exprValueToIdxScalar: %v", err)
	}
	if isSlice || isNil {
		t.Fatalf("unexpected scalar flags: isSlice=%v isNil=%v", isSlice, isNil)
	}
	cacheKey := db.materializedPredCacheKeyForScalar("score", qx.OpLT, keyValue)
	prevSnap := db.getSnapshot()
	prevBM, ok := prevSnap.loadMaterializedPred(cacheKey)
	if !ok || prevBM.IsEmpty() {
		t.Fatalf("expected score range cache entry before unrelated patch")
	}

	if err := db.Patch(1, []Field{{Name: "active", Value: false}}); err != nil {
		t.Fatalf("Patch(active): %v", err)
	}

	nextSnap := db.getSnapshot()
	nextBM, ok := nextSnap.loadMaterializedPred(cacheKey)
	if !ok || nextBM.IsEmpty() {
		t.Fatalf("expected score range cache entry after unrelated patch")
	}
	if nextBM != prevBM {
		t.Fatalf("expected unchanged-field cache entry to be inherited across snapshot publish")
	}
}

func TestQuery_OrderBasic_ComplementCachedBaseOpCountsAsMaterialized(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})

	seedGeneratedUint64Data(t, db, 5_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Age:    18 + (i % 60),
			Score:  float64(i),
			Active: true,
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.GTE("age", 25),
		qx.LTE("age", 40),
		qx.GT("score", 0.5),
	).Sort("score", qx.DESC).Limit(100)
	if _, err := db.QueryKeys(q); err != nil {
		t.Fatalf("warm QueryKeys: %v", err)
	}

	fillView := db.currentQueryViewForTests()
	lteExpr := mustLimitQIRExpr(t, db, qx.LTE("age", 40))
	lteCacheKey := fillView.materializedPredCacheKey(lteExpr)
	lteIDs := fillView.evalLazyMaterializedPredicate(lteExpr, lteCacheKey)
	lteIDs.Release()
	db.releaseQueryView(fillView)

	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)
	gteBound, isSlice, err := view.exprValueToNormalizedScalarBound(mustLimitQIRExpr(t, db, qx.GTE("age", 25)))
	if err != nil || isSlice {
		t.Fatalf("exprValueToNormalizedScalarBound(GTE age): err=%v isSlice=%v", err, isSlice)
	}
	lteBound, isSlice, err := view.exprValueToNormalizedScalarBound(mustLimitQIRExpr(t, db, qx.LTE("age", 40)))
	if err != nil || isSlice {
		t.Fatalf("exprValueToNormalizedScalarBound(LTE age): err=%v isSlice=%v", err, isSlice)
	}
	gteComplementKey := view.materializedPredComplementKeyForNormalizedScalarBound("age", gteBound).String()
	lteScalarKey := view.materializedPredKeyForNormalizedScalarBound("age", lteBound).String()
	complementSeed := posting.List{}.BuildAdded(1)
	defer complementSeed.Release()
	db.getSnapshot().storeMaterializedPred(gteComplementKey, complementSeed)
	leaves := mustLimitQIRLeaves(t, db, q.Filter)
	baseOps := filterQIRLeavesByField(db, leaves, "score")
	coresBuf, rawCoreIdxBuf := mustPrepareOrderBasicBaseCoresForTest(t, view, baseOps)
	defer orderBasicBaseCoreSlicePool.Put(coresBuf)
	defer orderBasicBaseCoreIndexSlicePool.Put(rawCoreIdxBuf)
	if !view.hasWarmOrderBasicBaseCores(coresBuf) {
		_, gteHit := db.getSnapshot().loadMaterializedPred(gteComplementKey)
		_, lteHit := db.getSnapshot().loadMaterializedPred(lteScalarKey)
		t.Fatalf("expected complement-backed cached range base op to count as materialized: gteComplementHit=%v lteScalarHit=%v", gteHit, lteHit)
	}
}

func TestQuery_OrderBasic_WarmQueryLoadsCollapsedNumericRangeSpan(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 0,
		NumericRangeBucketSize:                  8,
		NumericRangeBucketMinFieldKeys:          16,
		NumericRangeBucketMinSpanKeys:           4,
	})

	seedGeneratedUint64Data(t, db, 5_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Age:    18 + (i % 60),
			Score:  float64(i),
			Active: true,
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.GTE("age", 25),
		qx.LTE("age", 40),
		qx.GT("score", 0.5),
	).Sort("score", qx.DESC).Limit(100)

	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)
	leaves := mustLimitQIRLeaves(t, db, q.Filter)
	baseOps := filterQIRLeavesByField(db, leaves, "score")
	coresBuf, rawCoreIdxBuf := mustPrepareOrderBasicBaseCoresForTest(t, view, baseOps)
	defer orderBasicBaseCoreSlicePool.Put(coresBuf)
	defer orderBasicBaseCoreIndexSlicePool.Put(rawCoreIdxBuf)
	collapsed := mustFindCollapsedOrderBasicBaseCoreForTest(t, coresBuf)
	view.promoteOrderBasicLimitMaterializedBaseOps("score", baseOps, 250, 100)
	spanHit, ok := view.loadWarmOrderBasicBaseCore(collapsed)
	if !ok {
		t.Fatalf("expected collapsed numeric range span to be directly reusable")
	}
	spanHit.release()
	if !view.hasWarmOrderBasicBaseCores(coresBuf) {
		t.Fatalf("expected collapsed numeric range span to be reusable as warm order-basic base op")
	}
}

func mustPrepareOrderBasicBaseCoresForTest[K ~string | ~uint64, V any](
	t *testing.T,
	view *queryView[K, V],
	baseOps []qir.Expr,
) (*pooled.SliceBuf[orderBasicBaseCore], *pooled.SliceBuf[int]) {
	t.Helper()
	coresBuf, rawCoreIdxBuf, noMatch, err := view.prepareOrderBasicBaseCores(baseOps)
	if err != nil {
		t.Fatalf("prepareOrderBasicBaseCores: %v", err)
	}
	if noMatch {
		t.Fatalf("prepareOrderBasicBaseCores: unexpected no-match")
	}
	return coresBuf, rawCoreIdxBuf
}

func mustFindCollapsedOrderBasicBaseCoreForTest(
	t *testing.T,
	coresBuf *pooled.SliceBuf[orderBasicBaseCore],
) orderBasicBaseCore {
	t.Helper()
	if coresBuf == nil {
		t.Fatalf("expected prepared order-basic base cores")
	}
	var (
		found orderBasicBaseCore
		hit   bool
	)
	for i := 0; i < coresBuf.Len(); i++ {
		core := coresBuf.Get(i)
		if core.kind != orderBasicBaseCoreCollapsedRange {
			continue
		}
		if hit {
			t.Fatalf("expected exactly one collapsed order-basic base core")
		}
		found = core
		hit = true
	}
	if !hit {
		t.Fatalf("expected collapsed order-basic base core")
	}
	return found
}

func TestQuery_OrderBasic_WarmQueryPromotesMaterializedRangeBaseOps(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})

	seedGeneratedUint64Data(t, db, 5_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Age:    18 + (i % 60),
			Score:  float64(i),
			Active: true,
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.GTE("age", 25),
		qx.LTE("age", 40),
		qx.GT("score", 0.5),
	).Sort("score", qx.DESC).Limit(100)
	if _, err := db.QueryKeys(q); err != nil {
		t.Fatalf("warm QueryKeys: %v", err)
	}

	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)
	leaves := mustLimitQIRLeaves(t, db, q.Filter)
	baseOps := filterQIRLeavesByField(db, leaves, "score")
	coresBuf, rawCoreIdxBuf := mustPrepareOrderBasicBaseCoresForTest(t, view, baseOps)
	defer orderBasicBaseCoreSlicePool.Put(coresBuf)
	defer orderBasicBaseCoreIndexSlicePool.Put(rawCoreIdxBuf)
	collapsed := mustFindCollapsedOrderBasicBaseCoreForTest(t, coresBuf)
	if !view.hasWarmOrderBasicBaseCores(coresBuf) {
		var missing []string
		for _, op := range baseOps {
			stats, ok := view.orderBasicRawBaseOpStats(op, view.snapshotUniverseCardinality())
			cacheKey := materializedPredKey{}
			if ok {
				cacheKey = stats.cacheKey
			}
			if cacheKey.isZero() {
				missing = append(missing, fmt.Sprintf("%s:%v=<no-key>", testExprFieldName(db, op), op.Op))
				continue
			}
			if _, ok := db.getSnapshot().loadMaterializedPredKey(cacheKey); !ok {
				missing = append(missing, fmt.Sprintf("%s:%v", testExprFieldName(db, op), op.Op))
			}
		}
		t.Fatalf("expected warm ordered query to promote materialized range base ops, missing=%v", missing)
	}
	exactKey := materializedPredCacheKeyForExactScalarRange(collapsed.collapsed.field, collapsed.collapsed.bounds)
	if exactKey == "" {
		t.Fatalf("expected collapsed exact range cache key")
	}
	if _, ok := db.getSnapshot().loadMaterializedPred(exactKey); !ok {
		t.Fatalf("expected warm ordered query to promote collapsed exact numeric range cache entry")
	}
}

func TestQuery_OrderBasic_WarmAnalyticsRangeUsesLimitOrderBasicPlan(t *testing.T) {
	var events []TraceEvent
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSampleEvery: 1,
		TraceSink: func(ev TraceEvent) {
			events = append(events, ev)
		},
	})

	seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Age:    18 + (i % 60),
			Score:  float64(i),
			Active: true,
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.GTE("age", 25),
		qx.LTE("age", 40),
		qx.GT("score", 0.5),
	).Sort("score", qx.DESC).Limit(100)
	if _, err := db.QueryKeys(q); err != nil {
		t.Fatalf("first QueryKeys: %v", err)
	}
	view := db.currentQueryViewForTests()
	leaves := mustLimitQIRLeaves(t, db, q.Filter)
	baseOps := filterQIRLeavesByField(db, leaves, "score")
	coresBuf, rawCoreIdxBuf := mustPrepareOrderBasicBaseCoresForTest(t, view, baseOps)
	defer orderBasicBaseCoreSlicePool.Put(coresBuf)
	defer orderBasicBaseCoreIndexSlicePool.Put(rawCoreIdxBuf)
	collapsed := mustFindCollapsedOrderBasicBaseCoreForTest(t, coresBuf)
	if hit, ok := view.loadWarmOrderBasicBaseCore(collapsed); !ok {
		db.releaseQueryView(view)
		t.Fatalf("expected collapsed warm range after first analytics query")
	} else {
		hit.release()
	}
	if !view.hasWarmOrderBasicBaseCores(coresBuf) {
		db.releaseQueryView(view)
		t.Fatalf("expected warm order-basic base ops after first analytics query")
	}
	db.releaseQueryView(view)
	if _, err := db.QueryKeys(q); err != nil {
		t.Fatalf("second QueryKeys: %v", err)
	}
	if len(events) < 2 {
		t.Fatalf("expected at least two trace events, got %d", len(events))
	}
	if events[len(events)-1].Plan != string(PlanLimitOrderBasic) {
		t.Fatalf("expected second query to use %q, got %q", PlanLimitOrderBasic, events[len(events)-1].Plan)
	}
}

func TestQuery_OrderBasic_WarmBroadExactAndRangeUsesLimitOrderBasicPlan(t *testing.T) {
	var events []TraceEvent
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSampleEvery: 1,
		TraceSink: func(ev TraceEvent) {
			events = append(events, ev)
		},
	})

	seedGeneratedUint64Data(t, db, 100_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Age:    18 + (i % 50),
			Score:  float64(i),
			Active: i%10 != 0 && i%7 != 0,
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.EQ("active", true),
		qx.GTE("score", 250.0),
		qx.GTE("age", 20),
	).Sort("score", qx.DESC).Limit(50)

	if _, err := db.QueryKeys(q); err != nil {
		t.Fatalf("first QueryKeys: %v", err)
	}
	if _, err := db.QueryKeys(q); err != nil {
		t.Fatalf("second QueryKeys: %v", err)
	}

	if len(events) < 2 {
		t.Fatalf("expected at least two trace events, got %d", len(events))
	}
	if events[len(events)-1].Plan != string(PlanLimitOrderBasic) {
		t.Fatalf("expected second query to use %q, got %q", PlanLimitOrderBasic, events[len(events)-1].Plan)
	}
}

func TestQuery_OrderBasic_ExtractBoundsForField_IgnoresSecondaryRangeBounds(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	q := qx.Query(
		qx.GTE("score", 100.0),
		qx.GTE("age", 25),
		qx.LTE("age", 40),
		qx.EQ("active", true),
	).Sort("score", qx.DESC).Limit(50)

	leaves := mustLimitQIRLeaves(t, db, q.Filter)

	view := db.currentQueryViewForTests()
	bounds, ok, err := view.extractBoundsForField("score", leaves)
	db.releaseQueryView(view)
	if err != nil {
		t.Fatalf("extractBoundsForField: %v", err)
	}
	if !ok {
		t.Fatalf("expected order-field bounds to be recognized")
	}
	if !bounds.hasLo {
		t.Fatalf("unexpected lower bound: %+v", bounds)
	}
	if bounds.hasHi {
		t.Fatalf("unexpected high bound: %+v", bounds)
	}
}

func TestBuildPredicatesOrdered_WarmMergedNumericRangeUsesExactRangeCache(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})

	seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Age:    18 + (i % 60),
			Score:  float64(i),
			Active: true,
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.GTE("age", 25),
		qx.LTE("age", 40),
		qx.GT("score", 0.5),
	).Sort("score", qx.DESC).Limit(100)
	if _, err := db.QueryKeys(q); err != nil {
		t.Fatalf("warm QueryKeys: %v", err)
	}

	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)
	leaves := mustLimitQIRLeaves(t, db, q.Filter)
	predSet, ok := view.buildPredicatesOrderedWithMode(leaves, "score", false, 100, 0, true, true)
	if !ok {
		t.Fatalf("buildPredicatesOrderedWithMode: ok=false")
	}
	defer predSet.Release()

	ageCount := 0
	for i := 0; i < predSet.Len(); i++ {
		pred := predSet.Get(i)
		if testExprFieldName(db, pred.expr) != "age" {
			continue
		}
		ageCount++
		if !pred.isMaterializedLike() {
			t.Fatalf("expected warm merged age predicate to load exact materialized range")
		}
	}
	if ageCount != 1 {
		t.Fatalf("expected exactly one merged age predicate, got %d", ageCount)
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
			Filter: randomExpr(),
			Window: qx.Window{
				Offset: uint64(r.IntN(180)),
				Limit:  0, // unbounded
			},
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
