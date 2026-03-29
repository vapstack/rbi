package rbi

import (
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/posting"
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
	rangeLeaves := mustExtractAndLeaves(t, qRange.Expr)
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

func legacyEmitAcceptedPostingNoOrder[K ~uint64 | ~string, V any](cursor *queryCursor[K, V], ids posting.List, examined *uint64) bool {
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

func (qv *queryView[K, V]) legacyTryQueryRangeNoOrderWithLimit(q *qx.QX) ([]K, bool, error) {
	if len(q.Order) > 0 || q.Limit == 0 {
		return nil, false, nil
	}
	if q.Expr.Not {
		return nil, false, nil
	}

	e := q.Expr
	if e.Op == qx.OpAND || e.Op == qx.OpOR || len(e.Operands) != 0 {
		return nil, false, nil
	}

	switch e.Op {
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpEQ:
	default:
		return nil, false, nil
	}

	f := e.Field
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
	if e.Op == qx.OpEQ {
		key, isSlice, eqNil, err := qv.exprValueToIdxScalar(e)
		if err != nil {
			return nil, true, err
		}
		if isSlice {
			return nil, false, nil
		}
		isNil = eqNil
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

	out := make([]K, 0, q.Limit)
	cursor := qv.newQueryCursor(out, q.Offset, q.Limit, false, nil)

	if isNil {
		if e.Op != qx.OpEQ {
			return nil, true, nil
		}
		ids := qv.nilFieldLookupPostingRetained(f)
		if ids.IsEmpty() {
			return nil, true, nil
		}
		var examined uint64
		emitAcceptedPostingNoOrder(&cursor, ids, &examined)
		return cursor.out, true, nil
	}

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
		if legacyEmitAcceptedPostingNoOrder(&cursor, ids, &examined) {
			return cursor.out, true, nil
		}
	}
	return cursor.out, true, nil
}

func (qv *queryView[K, V]) legacyTryQueryPrefixNoOrderWithLimit(q *qx.QX) ([]K, bool, error) {
	if len(q.Order) > 0 || q.Limit == 0 {
		return nil, false, nil
	}
	if q.Expr.Not {
		return nil, false, nil
	}

	e := q.Expr
	if e.Op != qx.OpPREFIX || e.Field == "" {
		return nil, false, nil
	}
	if len(e.Operands) != 0 {
		return nil, false, nil
	}

	fm := qv.fields[e.Field]
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

	ov := qv.fieldOverlay(e.Field)
	if !ov.hasData() {
		if !qv.hasFieldIndex(e.Field) {
			return nil, true, fmt.Errorf("no index for field: %v", e.Field)
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

	out := make([]K, 0, q.Limit)
	cursor := qv.newQueryCursor(out, q.Offset, q.Limit, false, nil)

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
		if legacyEmitAcceptedPostingNoOrder(&cursor, ids, &examined) {
			return cursor.out, true, nil
		}
	}
	return cursor.out, true, nil
}

func legacyScanLimitByOverlayBounds[K ~uint64 | ~string, V any](db *queryView[K, V], q *qx.QX, ov fieldOverlay, br overlayRange, desc bool, preds []leafPred, nilTailField string) []K {
	limit := int(q.Limit)
	out := make([]K, 0, limit)
	cursor := db.newQueryCursor(out, 0, q.Limit, false, nil)

	emitCandidate := func(idx uint64) bool {
		for _, p := range preds {
			if !p.containsIdx(idx) {
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

	q := qx.Query(qx.GTE("age", 20)).Skip(4_000).Max(25)

	got, used, err := db.tryQueryRangeNoOrderWithLimit(q, nil)
	if err != nil {
		t.Fatalf("tryQueryRangeNoOrderWithLimit: %v", err)
	}
	if !used {
		t.Fatalf("expected range no-order fast path to be used")
	}

	want, used, err := db.currentQueryViewForTests().legacyTryQueryRangeNoOrderWithLimit(q)
	if err != nil {
		t.Fatalf("legacyTryQueryRangeNoOrderWithLimit: %v", err)
	}
	if !used {
		t.Fatalf("expected legacy range no-order fast path to be used")
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

	q := qx.Query(qx.PREFIX("full_name", "grp-1")).Skip(750).Max(30)

	got, used, err := db.tryQueryPrefixNoOrderWithLimit(q, nil)
	if err != nil {
		t.Fatalf("tryQueryPrefixNoOrderWithLimit: %v", err)
	}
	if !used {
		t.Fatalf("expected prefix no-order fast path to be used")
	}

	want, used, err := db.currentQueryViewForTests().legacyTryQueryPrefixNoOrderWithLimit(q)
	if err != nil {
		t.Fatalf("legacyTryQueryPrefixNoOrderWithLimit: %v", err)
	}
	if !used {
		t.Fatalf("expected legacy prefix no-order fast path to be used")
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

	q := qx.Query(qx.EQ("opt", nil)).Skip(2_500).Max(40)

	got, used, err := db.tryQueryRangeNoOrderWithLimit(q, nil)
	if err != nil {
		t.Fatalf("tryQueryRangeNoOrderWithLimit(nil EQ): %v", err)
	}
	if !used {
		t.Fatalf("expected nil-equality range fast path to be used")
	}

	want, used, err := db.currentQueryViewForTests().legacyTryQueryRangeNoOrderWithLimit(q)
	if err != nil {
		t.Fatalf("legacyTryQueryRangeNoOrderWithLimit(nil EQ): %v", err)
	}
	if !used {
		t.Fatalf("expected legacy nil-equality range fast path to be used")
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
	).Max(25)

	leaves := mustExtractAndLeaves(t, q.Expr)
	f, bounds, ok, err := db.extractNoOrderBounds(leaves)
	if err != nil {
		t.Fatalf("extractNoOrderBounds: %v", err)
	}
	if !ok {
		t.Fatalf("expected no-order bounds to be recognized")
	}

	view := db.currentQueryViewForTests()
	preds, ok, err := view.buildLeafPredsExcludingBounds(leaves, f)
	if err != nil {
		t.Fatalf("buildLeafPredsExcludingBounds: %v", err)
	}
	if !ok {
		t.Fatalf("expected residual leaf preds to be supported")
	}
	defer releaseLeafPreds(preds)

	br := view.fieldOverlay(f).rangeForBounds(bounds)
	want := legacyScanLimitByOverlayBounds(view, q, view.fieldOverlay(f), br, false, preds, "")

	tr := db.beginTrace(q)
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
	).By("age", qx.ASC).Max(20)

	leaves := mustExtractAndLeaves(t, q.Expr)
	view := db.currentQueryViewForTests()
	bounds, ok, err := view.extractBoundsForField("age", leaves)
	if err != nil {
		t.Fatalf("extractBoundsForField: %v", err)
	}
	if !ok {
		t.Fatalf("expected order bounds to be recognized")
	}

	preds, ok, err := view.buildLeafPredsExcludingBounds(leaves, "age")
	if err != nil {
		t.Fatalf("buildLeafPredsExcludingBounds: %v", err)
	}
	if !ok {
		t.Fatalf("expected residual leaf preds to be supported")
	}
	defer releaseLeafPreds(preds)

	ov := view.fieldOverlay("age")
	br := ov.rangeForBounds(bounds)
	want := legacyScanLimitByOverlayBounds(view, q, ov, br, false, preds, "")

	tr := db.beginTrace(q)
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

func TestQuery_OrderBasic_RangeBaseOpsSkipsEagerBaseBitmapBeforeOrderedPredicatesAfterPatch(t *testing.T) {
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
	).By("age", qx.ASC).Skip(2_000).Max(10)
	if _, err := db.QueryKeys(q); err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}

	keyValue, isSlice, isNil, err := db.exprValueToIdxScalar(qx.Expr{Op: qx.OpLT, Field: "score", Value: 4_000.0})
	if err != nil {
		t.Fatalf("exprValueToIdxScalar: %v", err)
	}
	if isSlice || isNil {
		t.Fatalf("unexpected scalar flags: isSlice=%v isNil=%v", isSlice, isNil)
	}
	cacheKey := materializedPredCacheKeyFromScalar("score", qx.OpLT, keyValue)
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
