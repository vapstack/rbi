package rbi

import (
	"fmt"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/posting"
)

func predicateMatchIDs(pred predicate, universe posting.List) []uint64 {
	var out []uint64
	universe.ForEach(func(idx uint64) bool {
		if pred.matches(idx) {
			out = append(out, idx)
		}
		return true
	})
	return out
}

func runPredicatePostingFilter(t *testing.T, pred predicate, universe posting.List) posting.List {
	t.Helper()
	if pred.postingFilter == nil {
		t.Fatal("expected numeric predicate to expose postingFilter")
	}
	for i := 0; i < 8; i++ {
		dst := universe.Borrow()
		if pred.postingFilter(&dst) {
			return dst
		}
		dst.Release()
	}
	t.Fatal("expected postingFilter to reach materialized fallback")
	return posting.List{}
}

func TestBuildPredRange_PrefixMaterializationStoredInCache(t *testing.T) {
	db, _ := openTempDBUint64(t)

	for i := 0; i < 700; i++ {
		err := db.Set(uint64(i+1), &Rec{
			Name:  fmt.Sprintf("u_%03d", i),
			Age:   i + 1,
			Email: fmt.Sprintf("user%03d@example.com", i),
		})
		if err != nil {
			t.Fatalf("seed Set(%d): %v", i+1, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	expr := qx.Expr{Op: qx.OpPREFIX, Field: "email", Value: "user1"}
	cacheKey := db.materializedPredCacheKey(expr)
	if cacheKey == "" {
		t.Fatalf("expected non-empty materialized cache key for prefix predicate")
	}
	if _, ok := db.getSnapshot().loadMaterializedPred(cacheKey); ok {
		t.Fatalf("unexpected cache hit before predicate evaluation")
	}

	fm := db.fields["email"]
	if fm == nil {
		t.Fatalf("expected field metadata for email")
	}
	ov := db.fieldOverlay("email")
	if !ov.hasData() {
		t.Fatalf("expected field index data for email")
	}

	p, ok := db.buildPredRangeCandidateWithMode(expr, fm, ov, true)
	if !ok {
		t.Fatalf("expected range/prefix predicate build to succeed")
	}
	if !p.hasContains() {
		t.Fatalf("expected contains predicate for prefix path")
	}
	for i := 1; i <= 90; i++ {
		_ = p.matches(uint64(i))
	}
	releasePredicates([]predicate{p})

	cached, ok := db.getSnapshot().loadMaterializedPred(cacheKey)
	if !ok || cached.IsEmpty() {
		t.Fatalf("expected cached materialized bitmap for prefix predicate")
	}
}

func TestBuildPredRange_PrefixMaterializationSkippedWhenCacheDisabled(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		SnapshotMaterializedPredCacheMaxEntries: -1,
	})

	for i := 0; i < 700; i++ {
		err := db.Set(uint64(i+1), &Rec{
			Name:  fmt.Sprintf("u_%03d", i),
			Age:   i + 1,
			Email: fmt.Sprintf("user%03d@example.com", i),
		})
		if err != nil {
			t.Fatalf("seed Set(%d): %v", i+1, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	expr := qx.Expr{Op: qx.OpPREFIX, Field: "email", Value: "user1"}
	if cacheKey := db.materializedPredCacheKey(expr); cacheKey != "" {
		t.Fatalf("expected empty materialized cache key when cache is disabled, got %q", cacheKey)
	}

	fm := db.fields["email"]
	if fm == nil {
		t.Fatalf("expected field metadata for email")
	}
	ov := db.fieldOverlay("email")
	if !ov.hasData() {
		t.Fatalf("expected field index data for email")
	}

	p, ok := db.buildPredRangeCandidateWithMode(expr, fm, ov, true)
	if !ok {
		t.Fatalf("expected range/prefix predicate build to succeed")
	}
	if !p.hasContains() {
		t.Fatalf("expected contains predicate for prefix path")
	}
	for i := 1; i <= 90; i++ {
		_ = p.matches(uint64(i))
	}
	releasePredicates([]predicate{p})

	rawKey := materializedPredCacheKeyFromScalar("email", qx.OpPREFIX, "user1")
	if _, ok := db.getSnapshot().matPredCache.Load(rawKey); ok {
		t.Fatalf("expected no cache store when materialized predicate cache is disabled")
	}
	if got := db.getSnapshot().matPredCacheCount.Load(); got != 0 {
		t.Fatalf("expected zero materialized cache entries, got %d", got)
	}
}

func TestBuildPredRangeCandidate_FullUniversePrefixBecomesAlwaysTrue(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	for i := 0; i < 512; i++ {
		err := db.Set(uint64(i+1), &Rec{
			Name:   fmt.Sprintf("u_%03d", i),
			Age:    20 + i%30,
			Email:  fmt.Sprintf("user%03d@example.com", i),
			Active: i%2 == 0,
		})
		if err != nil {
			t.Fatalf("seed Set(%d): %v", i+1, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	p, ok := db.buildPredicateWithMode(qx.Expr{Op: qx.OpPREFIX, Field: "email", Value: "user"}, false)
	if !ok {
		t.Fatalf("expected range candidate build to succeed")
	}
	defer releasePredicates([]predicate{p})

	if !p.alwaysTrue {
		t.Fatalf("expected full-universe prefix to collapse to alwaysTrue, got predicate=%+v", p)
	}
	if p.hasContains() || p.hasIter() {
		t.Fatalf("expected collapsed prefix predicate to avoid residual contains/iter paths")
	}
}

func TestBuildPredRange_BaseNumericPostingFilter_NotComplementMaterializedFallback(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	seedGeneratedUint64Data(t, db, 24_000, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   i % 300,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}
	if ov := db.fieldOverlay("age"); ov.chunked != nil {
		t.Fatalf("expected base numeric index path for age, got chunked overlay")
	}

	expr := qx.Expr{Op: qx.OpGTE, Field: "age", Value: 30, Not: true}
	predExpected, ok := db.buildPredicateWithMode(expr, false)
	if !ok {
		t.Fatal("expected base numeric predicate to build")
	}
	defer releasePredicates([]predicate{predExpected})

	predFilter, ok := db.buildPredicateWithMode(expr, false)
	if !ok {
		t.Fatal("expected base numeric predicate to build for postingFilter")
	}
	defer releasePredicates([]predicate{predFilter})

	snap := db.getSnapshot()
	want := predicateMatchIDs(predExpected, snap.universe)
	got := runPredicatePostingFilter(t, predFilter, snap.universe)
	defer got.Release()
	assertPostingConsumerSet(t, got, want)
}

func TestBuildPredRange_OverlayNumericPostingFilter_NotComplementMaterializedFallback(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}
	if ov := db.fieldOverlay("age"); ov.chunked == nil {
		t.Fatalf("expected chunked overlay path for age")
	}

	expr := qx.Expr{Op: qx.OpGTE, Field: "age", Value: 30, Not: true}
	predExpected, ok := db.buildPredicateWithMode(expr, false)
	if !ok {
		t.Fatal("expected overlay numeric predicate to build")
	}
	defer releasePredicates([]predicate{predExpected})

	predFilter, ok := db.buildPredicateWithMode(expr, false)
	if !ok {
		t.Fatal("expected overlay numeric predicate to build for postingFilter")
	}
	defer releasePredicates([]predicate{predFilter})

	snap := db.getSnapshot()
	want := predicateMatchIDs(predExpected, snap.universe)
	got := runPredicatePostingFilter(t, predFilter, snap.universe)
	defer got.Release()
	assertPostingConsumerSet(t, got, want)
}

func TestOverlayRangeStats_ChunkedMatchesCursorScanOnSeededScore(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	seedGeneratedUint64Data(t, db, 120_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%05d@example.com", i),
			Age:    18 + (i % 60),
			Score:  float64(i % 1_000),
			Active: i%2 == 0,
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	ov := db.fieldOverlay("score")
	if ov.chunked == nil {
		t.Fatalf("expected score field to be chunked for seeded dataset")
	}

	slowStats := func(br overlayRange) (int, uint64) {
		cur := ov.newCursor(br, false)
		n := 0
		var est uint64
		for {
			_, ids, ok := cur.next()
			if !ok {
				break
			}
			card := ids.Cardinality()
			if card == 0 {
				continue
			}
			n++
			est += card
		}
		return n, est
	}

	cases := []rangeBounds{
		{has: true, hasLo: true, loKey: float64ByteStr(81), loInc: false},
		{has: true, hasLo: true, loKey: float64ByteStr(4), loInc: false},
		{has: true, hasLo: true, loKey: float64ByteStr(40), loInc: true},
		{has: true, hasHi: true, hiKey: float64ByteStr(50), hiInc: false},
		{has: true, hasLo: true, loKey: float64ByteStr(10), loInc: true, hasHi: true, hiKey: float64ByteStr(20), hiInc: true},
	}

	for i, rb := range cases {
		br := ov.rangeForBounds(rb)
		gotBuckets, gotEst := overlayRangeStats(ov, br)
		wantBuckets, wantEst := slowStats(br)
		if gotBuckets != wantBuckets || gotEst != wantEst {
			t.Fatalf("case %d mismatch: got buckets=%d est=%d want buckets=%d est=%d", i, gotBuckets, gotEst, wantBuckets, wantEst)
		}
	}
}

func TestBuildPredRangeCandidate_DoesNotCollapsePartialChunkedRangeToAlwaysTrue(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	seedGeneratedUint64Data(t, db, 120_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%05d@example.com", i),
			Age:    18 + (i % 60),
			Score:  float64(i % 1_000),
			Active: i%2 == 0,
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	p, ok := db.buildPredicateWithMode(qx.Expr{Op: qx.OpGT, Field: "score", Value: 4.0}, false)
	if !ok {
		t.Fatalf("expected predicate build to succeed")
	}
	defer releasePredicates([]predicate{p})

	if p.alwaysTrue {
		t.Fatalf("partial chunked range unexpectedly collapsed to alwaysTrue")
	}
	if !p.hasIter() && !p.hasContains() {
		t.Fatalf("expected usable predicate, got kind=%v", p.kind)
	}
}

func TestBuildPredRangeCandidate_ChunkedRangeMatchesBitmapBaseline(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	seedGeneratedUint64Data(t, db, 120_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%05d@example.com", i),
			Age:    18 + (i % 60),
			Score:  float64(i % 1_000),
			Active: i%2 == 0,
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	expr := qx.Expr{Op: qx.OpGT, Field: "score", Value: 4.0}
	p, ok := db.buildPredicateWithMode(expr, false)
	if !ok {
		t.Fatalf("expected predicate build to succeed")
	}
	defer releasePredicates([]predicate{p})

	if !p.hasIter() {
		t.Fatalf("expected iterable predicate, got kind=%v", p.kind)
	}

	var got posting.List
	it := p.newIter()
	for it.HasNext() {
		got.Add(it.Next())
	}
	it.Release()
	defer got.Release()

	wantCount := countByExprBitmap(t, db, expr)
	if got.Cardinality() != wantCount {
		t.Fatalf("range predicate mismatch: got=%d want=%d", got.Cardinality(), wantCount)
	}
}

func TestOverlayRangeStats_ChunkedMatchesCursorScanOnLargeUniqueRange(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	seedGeneratedUint64Data(t, db, 160_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%05d@example.com", i),
			Age:    i,
			Score:  float64(i),
			Active: i%2 == 0,
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	ov := db.fieldOverlay("age")
	if ov.chunked == nil {
		t.Fatalf("expected age field to be chunked")
	}

	slowStats := func(br overlayRange) (int, uint64) {
		cur := ov.newCursor(br, false)
		n := 0
		var est uint64
		for {
			_, ids, ok := cur.next()
			if !ok {
				break
			}
			card := ids.Cardinality()
			if card == 0 {
				continue
			}
			n++
			est += card
		}
		return n, est
	}

	cases := []rangeBounds{
		{has: true, hasLo: true, loKey: int64ByteStr(35_000), loInc: true},
		{has: true, hasLo: true, loKey: int64ByteStr(35_000), loInc: false},
		{has: true, hasHi: true, hiKey: int64ByteStr(35_000), hiInc: false},
		{has: true, hasLo: true, loKey: int64ByteStr(10_000), loInc: true, hasHi: true, hiKey: int64ByteStr(120_000), hiInc: false},
	}

	for i, rb := range cases {
		br := ov.rangeForBounds(rb)
		gotBuckets, gotEst := overlayRangeStats(ov, br)
		wantBuckets, wantEst := slowStats(br)
		if gotBuckets != wantBuckets || gotEst != wantEst {
			t.Fatalf("case %d mismatch: got buckets=%d est=%d want buckets=%d est=%d", i, gotBuckets, gotEst, wantBuckets, wantEst)
		}
	}
}

func TestMaterializedPredCacheKeyFromScalar_SupportsRangeAndPrefix(t *testing.T) {
	type tc struct {
		op      qx.Op
		wantHit bool
	}
	cases := []tc{
		{op: qx.OpGT, wantHit: true},
		{op: qx.OpGTE, wantHit: true},
		{op: qx.OpLT, wantHit: true},
		{op: qx.OpLTE, wantHit: true},
		{op: qx.OpPREFIX, wantHit: true},
		{op: qx.OpSUFFIX, wantHit: true},
		{op: qx.OpCONTAINS, wantHit: true},
		{op: qx.OpEQ, wantHit: false},
		{op: qx.OpIN, wantHit: false},
	}
	for _, c := range cases {
		got := materializedPredCacheKeyFromScalar("f", c.op, "v")
		if c.wantHit && got == "" {
			t.Fatalf("expected non-empty key for op=%v", c.op)
		}
		if !c.wantHit && got != "" {
			t.Fatalf("expected empty key for op=%v", c.op)
		}
	}
}

func TestCollectOrderRangeBounds_PrefixIntersection(t *testing.T) {
	db, _ := openTempDBUint64(t)

	cases := []struct {
		name       string
		exprs      []qx.Expr
		wantEmpty  bool
		wantPrefix string
	}{
		{
			name: "narrow_to_more_selective_prefix",
			exprs: []qx.Expr{
				qx.PREFIX("email", "user"),
				qx.PREFIX("email", "user1"),
			},
			wantPrefix: "user1",
		},
		{
			name: "disjoint_prefixes_become_empty",
			exprs: []qx.Expr{
				qx.PREFIX("email", "user1"),
				qx.PREFIX("email", "user2"),
			},
			wantEmpty: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rb, covered, has, ok := db.collectOrderRangeBounds("email", len(tc.exprs), func(i int) qx.Expr {
				return tc.exprs[i]
			})
			if !ok {
				t.Fatalf("collectOrderRangeBounds failed")
			}
			if !has {
				t.Fatalf("expected bounds")
			}
			if len(covered) != len(tc.exprs) {
				t.Fatalf("covered len mismatch: got=%d want=%d", len(covered), len(tc.exprs))
			}
			for i := range covered {
				if !covered[i] {
					t.Fatalf("expected expr %d to be covered", i)
				}
			}
			if rb.empty != tc.wantEmpty {
				t.Fatalf("empty mismatch: got=%v want=%v", rb.empty, tc.wantEmpty)
			}
			if !tc.wantEmpty && rb.prefix != tc.wantPrefix {
				t.Fatalf("prefix mismatch: got=%q want=%q", rb.prefix, tc.wantPrefix)
			}
		})
	}
}
