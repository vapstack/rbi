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
	if pred.postingFilter == nil && pred.baseRangeState == nil && pred.overlayState == nil {
		t.Fatal("expected numeric predicate to expose postingFilter")
	}
	for i := 0; i < 8; i++ {
		dst := universe.Borrow()
		ok := false
		if pred.postingFilter != nil {
			dst, ok = pred.postingFilter(dst)
		} else {
			dst, ok = pred.applyToPosting(dst)
		}
		if ok {
			return dst
		}
		dst.Release()
	}
	t.Fatal("expected postingFilter to reach materialized fallback")
	return posting.List{}
}

func TestReleasePredicateOwnedState_ClearsRangeMaterializedRewriteState(t *testing.T) {
	p := materializedRangePredicateWithMode(
		qx.Expr{Op: qx.OpGTE, Field: "age", Value: 10},
		posting.BuildFromSorted([]uint64{2, 4, 6}),
	)
	if !p.rangeMat || !p.hasIter() {
		t.Fatalf("expected initial cached range predicate to stay iterable")
	}

	releasePredicateOwnedState(&p)

	p.kind = predicateKindMaterializedNot
	p.ids = posting.BuildFromSorted([]uint64{2, 4, 6})
	p.releaseIDs = true
	defer releasePredicateOwnedState(&p)

	if p.rangeMat {
		t.Fatalf("releasePredicateOwnedState left stale rangeMat state behind")
	}
	if p.hasIter() {
		t.Fatalf("materialized-not rewrite must not keep range iterator semantics")
	}
	if it := p.newIter(); it != nil {
		t.Fatalf("materialized-not rewrite must not expose an iterator")
	}
	if !p.matches(1) || p.matches(2) {
		t.Fatalf("materialized-not rewrite matches wrong ids")
	}
}

func TestPredicateSupportsPostingFilter_RangeStateDoesNotRequireCheapFlag(t *testing.T) {
	t.Run("BaseRange", func(t *testing.T) {
		p := predicate{
			baseRangeState:     &baseRangePredicateState{},
			postingFilterCheap: false,
		}
		if !p.supportsPostingApply() {
			t.Fatalf("base range state lost posting-filter capability when filter is not cheap")
		}
	})

	t.Run("OverlayRange", func(t *testing.T) {
		p := predicate{
			overlayState:       &overlayRangePredicateState{},
			postingFilterCheap: false,
		}
		if !p.supportsPostingApply() {
			t.Fatalf("overlay range state lost posting-filter capability when filter is not cheap")
		}
	})
}

func TestPredicateSupportsPostingFilter_RangeMaterializedSupportsExact(t *testing.T) {
	p := materializedRangePredicateWithMode(
		qx.Expr{Op: qx.OpLTE, Field: "age", Value: 40},
		posting.BuildFromSorted([]uint64{1, 3, 5}),
	)
	defer releasePredicateOwnedState(&p)

	if !p.rangeMat {
		t.Fatalf("expected rangeMat predicate")
	}
	if !p.supportsPostingApply() {
		t.Fatalf("rangeMat predicate lost posting-filter capability")
	}
	if !p.supportsExactBucketPostingFilter() {
		t.Fatalf("rangeMat predicate lost exact bucket posting-filter capability")
	}
}

func TestPredicateSupportsExactBucketPostingFilter_RangeStatesCheapOnly(t *testing.T) {
	t.Run("BaseRange", func(t *testing.T) {
		p := predicate{baseRangeState: &baseRangePredicateState{}}
		if p.supportsExactBucketPostingFilter() {
			t.Fatalf("non-cheap base range state must stay out of exact bucket posting-filter path")
		}
		p.postingFilterCheap = true
		if !p.supportsExactBucketPostingFilter() {
			t.Fatalf("cheap base range state must enter exact bucket posting-filter path")
		}
	})

	t.Run("OverlayRange", func(t *testing.T) {
		p := predicate{overlayState: &overlayRangePredicateState{}}
		if p.supportsExactBucketPostingFilter() {
			t.Fatalf("non-cheap overlay range state must stay out of exact bucket posting-filter path")
		}
		p.postingFilterCheap = true
		if !p.supportsExactBucketPostingFilter() {
			t.Fatalf("cheap overlay range state must enter exact bucket posting-filter path")
		}
	})
}

func TestPredicateSupportsCheapPostingFilter_UsesUnifiedCapability(t *testing.T) {
	t.Run("BaseRangeNonCheap", func(t *testing.T) {
		p := predicate{baseRangeState: &baseRangePredicateState{}}
		if p.supportsCheapPostingApply() {
			t.Fatalf("non-cheap base range state must stay out of cheap posting-filter class")
		}
	})

	t.Run("BaseRangeCheap", func(t *testing.T) {
		p := predicate{
			baseRangeState:     &baseRangePredicateState{},
			postingFilterCheap: true,
		}
		if !p.supportsCheapPostingApply() {
			t.Fatalf("cheap base range state must stay in cheap posting-filter class")
		}
	})

	t.Run("PostsAnyExactOnly", func(t *testing.T) {
		p := predicate{kind: predicateKindPostsAny, postsAnyState: &postsAnyFilterState{}}
		if p.supportsPostingApply() {
			t.Fatalf("posts-any exact path must not enter generic posting-filter set")
		}
		if !p.supportsExactBucketPostingFilter() {
			t.Fatalf("posts-any exact path lost exact bucket posting-filter capability")
		}
		if !p.prefersExactBucketPostingFilter() {
			t.Fatalf("posts-any exact path must stay exact-preferred")
		}
		if !p.supportsCheapPostingApply() {
			t.Fatalf("posts-any exact path must stay in cheap posting-filter class")
		}
	})
}

func TestPredicateLeadIterMayDuplicate_RangeStatesStayUnique(t *testing.T) {
	if predicateLeadIterMayDuplicate(predicate{baseRangeState: &baseRangePredicateState{}}) {
		t.Fatalf("base range iterator should stay unique for ordered-anchor lead scans")
	}
	if predicateLeadIterMayDuplicate(predicate{overlayState: &overlayRangePredicateState{}}) {
		t.Fatalf("overlay range iterator should stay unique for ordered-anchor lead scans")
	}
}

func TestOverlayRangeProbe_CountBucket_ComplementProbe(t *testing.T) {
	probeIDs := posting.BuildFromSorted([]uint64{2, 4})
	defer probeIDs.Release()
	bucket := posting.BuildFromSorted([]uint64{1, 2, 3, 4})
	defer bucket.Release()

	probe := overlayRangeProbe{
		ov:            fieldOverlay{base: []index{{IDs: probeIDs.Borrow()}}},
		spanCnt:       1,
		useComplement: true,
		probeLen:      1,
		probeEst:      2,
	}
	probe.spans[0] = overlayRange{baseStart: 0, baseEnd: 1}

	in, ok := probe.countBucket(bucket.Borrow())
	if !ok {
		t.Fatalf("expected overlay complement probe to support countBucket")
	}
	if in != 2 {
		t.Fatalf("countBucket = %d, want 2", in)
	}
}

func TestOverlayRangePredicateState_Matches_MaterializedComplementProbe(t *testing.T) {
	state := &overlayRangePredicateState{
		probe: overlayRangeProbe{
			useComplement: true,
		},
		keepProbeHits:     false,
		probeMaterialized: true,
		probeIDs:          posting.BuildFromSorted([]uint64{2, 4}),
	}
	defer releaseOverlayRangePredicateState(state)

	if state.matches(2) {
		t.Fatalf("complement probe hit must be excluded from positive broad range")
	}
	if !state.matches(3) {
		t.Fatalf("non-probe id must stay matched for positive broad range")
	}
}

func TestPredicateSetExpectedContainsCalls_UpdatesOverlayRangeState(t *testing.T) {
	state := acquireOverlayRangePredicateState(
		fieldOverlay{base: []index{{Key: indexKeyFromString("a")}, {Key: indexKeyFromString("b")}, {Key: indexKeyFromString("c")}}},
		overlayRange{baseStart: 0, baseEnd: 3},
		overlayRangeProbe{
			ov:       fieldOverlay{base: []index{{Key: indexKeyFromString("a")}, {Key: indexKeyFromString("b")}, {Key: indexKeyFromString("c")}}},
			spans:    [2]overlayRange{{baseStart: 0, baseEnd: 3}},
			spanCnt:  1,
			probeLen: 32,
			probeEst: 2048,
		},
		3,
		2048,
		false,
		materializedPredReuse{},
		false,
	)
	defer releaseOverlayRangePredicateState(state)

	p := predicate{overlayState: state}
	p.setExpectedContainsCalls(1)
	if state.expectedContainsCalls != 1 {
		t.Fatalf("expected overlay expectedContainsCalls=1, got %d", state.expectedContainsCalls)
	}
	if state.containsMode != baseRangeContainsLinear {
		t.Fatalf("expected overlay contains mode to stay linear for single call, got %v", state.containsMode)
	}
}

func TestPredicateMaterializedIDsDriveContainsAndIterCapabilities(t *testing.T) {
	t.Run("LazyMaterializedPositive", func(t *testing.T) {
		p := predicate{
			expr: qx.Expr{Op: qx.OpPREFIX, Field: "email", Value: "user"},
			lazyMatState: &lazyMaterializedPredicateState{
				ids:    posting.BuildFromSorted([]uint64{1, 3, 5}),
				loaded: true,
			},
		}
		defer releasePredicateRuntimeState(&p)
		if !p.hasContains() {
			t.Fatalf("lazy materialized positive predicate lost contains capability")
		}
		if !p.hasIter() {
			t.Fatalf("lazy materialized positive predicate lost iter capability")
		}
		if predicateLeadIterMayDuplicate(p) {
			t.Fatalf("lazy materialized positive predicate must stay unique")
		}
	})

	t.Run("LazyMaterializedNegative", func(t *testing.T) {
		p := predicate{
			expr: qx.Expr{Op: qx.OpPREFIX, Field: "email", Value: "user", Not: true},
			lazyMatState: &lazyMaterializedPredicateState{
				ids:    posting.BuildFromSorted([]uint64{1, 3, 5}),
				loaded: true,
			},
		}
		defer releasePredicateRuntimeState(&p)
		if !p.hasContains() {
			t.Fatalf("lazy materialized negative predicate lost contains capability")
		}
		if p.hasIter() {
			t.Fatalf("lazy materialized negative predicate must not expose iterator capability")
		}
		if predicateLeadIterMayDuplicate(p) {
			t.Fatalf("lazy materialized negative predicate must stay non-duplicating")
		}
	})
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

func TestBuildPredRange_OverlayNumericPostingFilter_ComplementMaterializedFallback(t *testing.T) {
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
	fm := db.fields["age"]
	if fm == nil {
		t.Fatalf("expected field metadata for age")
	}
	ov := db.fieldOverlay("age")
	if ov.chunked == nil {
		t.Fatalf("expected chunked overlay path for age")
	}

	expr := qx.Expr{Op: qx.OpGTE, Field: "age", Value: 1_000}
	bound, isSlice, err := db.currentQueryViewForTests().exprValueToNormalizedScalarBound(qx.Expr{
		Op:    expr.Op,
		Field: expr.Field,
		Value: expr.Value,
	})
	if err != nil {
		t.Fatalf("exprValueToNormalizedScalarBound: %v", err)
	}
	if isSlice {
		t.Fatalf("unexpected slice bound for %v", expr)
	}
	rb := rangeBounds{has: true}
	applyNormalizedScalarBound(&rb, bound)
	br := ov.rangeForBounds(rb)
	bucketCount, _ := overlayRangeStats(ov, br)
	totalBuckets := ov.keyCount()
	if totalBuckets-bucketCount >= bucketCount {
		t.Fatalf("expected complement probe, buckets=%d total=%d", bucketCount, totalBuckets)
	}

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
		got = got.BuildAdded(it.Next())
	}
	it.Release()
	defer got.Release()

	wantCount := countByExprBitmap(t, db, expr)
	if got.Cardinality() != wantCount {
		t.Fatalf("range predicate mismatch: got=%d want=%d", got.Cardinality(), wantCount)
	}
}

func TestBuildPredicateWithMode_RuntimeNumericRangeMaterializationKeepsScalarCacheLocal(t *testing.T) {
	t.Run("OverlayState", func(t *testing.T) {
		db, _ := openTempDBUint64(t, Options{
			AnalyzeInterval:                         -1,
			SnapshotMaterializedPredCacheMaxEntries: 16,
		})
		seedGeneratedUint64Data(t, db, 120_000, func(i int) *Rec {
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

		expr := qx.Expr{Op: qx.OpGT, Field: "age", Value: 110_000}
		cacheKey := db.materializedPredCacheKey(expr)
		p, ok := db.buildPredicateWithMode(expr, false)
		if !ok {
			t.Fatalf("expected predicate build to succeed")
		}
		defer releasePredicates([]predicate{p})

		if p.overlayState == nil {
			t.Fatalf("expected chunked numeric range to stay on overlay runtime state")
		}
		if got := db.getSnapshot().matPredCacheCount.Load(); got != 0 {
			t.Fatalf("unexpected shared materialized predicate cache before runtime materialization: %d", got)
		}
		_ = p.matches(1)
		if !p.overlayState.rangeMaterialized || p.overlayState.rangeIDs.IsEmpty() {
			t.Fatalf("expected runtime overlay state to materialize locally")
		}
		if got := db.getSnapshot().matPredCacheCount.Load(); got != 0 {
			t.Fatalf("unexpected shared scalar cache entry from runtime overlay materialization: %d", got)
		}
		if cached, ok := db.getSnapshot().loadMaterializedPred(cacheKey); ok && !cached.IsEmpty() {
			t.Fatalf("unexpected shared scalar cache entry from runtime overlay materialization")
		}
	})

	t.Run("BaseState", func(t *testing.T) {
		db, _ := openTempDBUint64(t, Options{
			AnalyzeInterval:                         -1,
			SnapshotMaterializedPredCacheMaxEntries: 16,
		})
		seedGeneratedUint64Data(t, db, 256, func(i int) *Rec {
			return &Rec{
				Name:   fmt.Sprintf("u_%d", i),
				Email:  fmt.Sprintf("user%05d@example.com", i),
				Age:    18 + (i % 60),
				Score:  float64(i),
				Active: i%2 == 0,
			}
		})
		if err := db.RebuildIndex(); err != nil {
			t.Fatalf("RebuildIndex: %v", err)
		}

		expr := qx.Expr{Op: qx.OpGT, Field: "score", Value: 220.0}
		cacheKey := db.materializedPredCacheKey(expr)
		p, ok := db.buildPredicateWithMode(expr, false)
		if !ok {
			t.Fatalf("expected predicate build to succeed")
		}
		defer releasePredicates([]predicate{p})

		if p.baseRangeState == nil {
			t.Fatalf("expected flat numeric range to stay on base runtime state")
		}
		if got := db.getSnapshot().matPredCacheCount.Load(); got != 0 {
			t.Fatalf("unexpected shared materialized predicate cache before runtime materialization: %d", got)
		}
		for i := 1; i <= 128; i++ {
			_ = p.matches(uint64(i))
		}
		if p.baseRangeState.ids.IsEmpty() && !p.baseRangeState.hasSet {
			t.Fatalf("expected runtime base state to build local membership state")
		}
		if got := db.getSnapshot().matPredCacheCount.Load(); got != 0 {
			t.Fatalf("unexpected shared scalar cache entry from runtime base materialization: %d", got)
		}
		if cached, ok := db.getSnapshot().loadMaterializedPred(cacheKey); ok && !cached.IsEmpty() {
			t.Fatalf("unexpected shared scalar cache entry from runtime base materialization")
		}
	})
}

func TestBuildPredRange_BroadPositiveRuntimeKeepsComplementCacheLocal(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 256, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%05d@example.com", i),
			Age:    18 + i,
			Score:  float64(i),
			Active: i%2 == 0,
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	expr := qx.Expr{Op: qx.OpGTE, Field: "age", Value: 40}
	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)
	bound, isSlice, err := view.exprValueToNormalizedScalarBound(expr)
	if err != nil {
		t.Fatalf("exprValueToNormalizedScalarBound: %v", err)
	}
	if isSlice || bound.full || bound.empty {
		t.Fatalf("unexpected normalized bound state: slice=%v full=%v empty=%v", isSlice, bound.full, bound.empty)
	}
	fullCacheKey := db.materializedPredCacheKey(expr)
	complementCacheKey := db.materializedPredComplementCacheKeyForScalar(expr.Field, bound.op, bound.key)
	if fullCacheKey == "" || complementCacheKey == "" {
		t.Fatalf("expected non-empty scalar and complement cache keys")
	}

	p, ok := db.buildPredicateWithMode(expr, false)
	if !ok {
		t.Fatalf("expected predicate build to succeed")
	}
	defer releasePredicates([]predicate{p})

	if p.baseRangeState == nil {
		t.Fatalf("expected flat numeric range to stay on base runtime state")
	}
	if !p.baseRangeState.probe.useComplement {
		t.Fatalf("expected broad positive range to use complement probe")
	}
	if got := db.getSnapshot().matPredCacheCount.Load(); got != 0 {
		t.Fatalf("unexpected shared materialized predicate cache before runtime materialization: %d", got)
	}

	for i := 1; i <= 160; i++ {
		_ = p.matches(uint64(i))
	}

	if got := db.getSnapshot().matPredCacheCount.Load(); got != 0 {
		t.Fatalf("expected complement-backed runtime matches to stay local, cacheCount=%d", got)
	}
	if _, ok := db.getSnapshot().loadMaterializedPred(fullCacheKey); ok {
		t.Fatalf("unexpected positive scalar cache entry for complement-backed runtime state")
	}
	if _, ok := db.getSnapshot().loadMaterializedPred(complementCacheKey); ok {
		t.Fatalf("unexpected shared complement cache entry from runtime base materialization")
	}
}

func TestBuildPredRange_BroadPositivePostingFilterKeepsComplementCacheLocal(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 256, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%05d@example.com", i),
			Age:    18 + i,
			Score:  float64(i),
			Active: i%2 == 0,
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)
	expr := qx.Expr{Op: qx.OpGTE, Field: "age", Value: 40}
	bound, isSlice, err := view.exprValueToNormalizedScalarBound(expr)
	if err != nil {
		t.Fatalf("exprValueToNormalizedScalarBound: %v", err)
	}
	if isSlice || bound.full || bound.empty {
		t.Fatalf("unexpected normalized bound state: slice=%v full=%v empty=%v", isSlice, bound.full, bound.empty)
	}
	complementCacheKey := view.materializedPredComplementCacheKeyForScalar(expr.Field, bound.op, bound.key)

	p, ok := view.buildPredicateWithMode(expr, false)
	if !ok {
		t.Fatalf("expected predicate build to succeed")
	}
	defer releasePredicates([]predicate{p})
	if p.baseRangeState == nil || !p.baseRangeState.probe.useComplement {
		t.Fatalf("expected broad positive base range state with complement probe")
	}

	var universe posting.List
	defer universe.Release()
	for i := 1; i <= 256; i++ {
		universe = universe.BuildAdded(uint64(i))
	}

	for i := 0; i < 64; i++ {
		dst := universe.Borrow()
		next, ok := p.applyToPosting(dst)
		if !ok {
			dst.Release()
			t.Fatalf("expected posting filter to stay evaluable")
		}
		next.Release()
	}

	if got := db.getSnapshot().matPredCacheCount.Load(); got != 0 {
		t.Fatalf("expected complement-backed posting filters to stay local, cacheCount=%d", got)
	}
	if _, ok := db.getSnapshot().loadMaterializedPred(complementCacheKey); ok {
		t.Fatalf("unexpected shared complement cache entry from posting-filter path")
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
