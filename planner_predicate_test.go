package rbi

import (
	"fmt"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qir"
)

func mustTestQIRExpr(t testing.TB, expr qx.Expr) qir.Expr {
	t.Helper()
	prepared, compiled, err := prepareTestExpr[uint64, Rec](nil, expr)
	if err != nil {
		t.Fatalf("prepareTestExpr(%+v): %v", expr, err)
	}
	compiled = detachTestQIRExpr(compiled)
	prepared.Release()
	return compiled
}

func mustTestQIRExprForDB[K ~string | ~uint64, V any](t testing.TB, db *DB[K, V], expr qx.Expr) qir.Expr {
	t.Helper()
	prepared, compiled, err := prepareTestExpr(db, expr)
	if err != nil {
		t.Fatalf("prepareTestExpr(%+v): %v", expr, err)
	}
	compiled = detachTestQIRExpr(compiled)
	prepared.Release()
	return compiled
}

func detachTestQIRExpr(expr qir.Expr) qir.Expr {
	if len(expr.Operands) == 0 {
		return expr
	}
	out := expr
	out.Operands = make([]qir.Expr, len(expr.Operands))
	for i := range expr.Operands {
		out.Operands[i] = detachTestQIRExpr(expr.Operands[i])
	}
	return out
}

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
		mustTestQIRExpr(t, qx.GTE("age", 10)),
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

func TestSetPredicateMaterializedNot_PreservesEstCard(t *testing.T) {
	p := predicate{
		expr:    mustTestQIRExpr(t, qx.GTE("age", 10)),
		estCard: 123,
	}
	ids := posting.BuildFromSorted([]uint64{2, 4, 6})
	defer ids.Release()

	setPredicateMaterializedNot(&p, ids.Borrow())
	defer releasePredicateOwnedState(&p)

	if p.kind != predicateKindMaterializedNot {
		t.Fatalf("expected materialized-not kind, got=%v", p.kind)
	}
	if p.estCard != 123 {
		t.Fatalf("expected preserved estCard=123, got=%d", p.estCard)
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
		mustTestQIRExpr(t, qx.LTE("age", 40)),
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

	t.Run("PostsAnyNotRuntime", func(t *testing.T) {
		p := predicate{kind: predicateKindPostsAnyNot, postsAnyState: &postsAnyFilterState{}}
		if !p.supportsPostingApply() {
			t.Fatalf("posts-any-not runtime path lost posting-filter capability")
		}
		if !p.supportsExactBucketPostingFilter() {
			t.Fatalf("posts-any-not runtime path lost exact bucket posting-filter capability")
		}
		if !p.supportsCheapPostingApply() {
			t.Fatalf("posts-any-not runtime path must stay in cheap posting-filter class")
		}
		if p.prefersExactBucketPostingFilter() {
			t.Fatalf("posts-any-not runtime path must not force preferred exact routing")
		}
	})
}

func TestPredicatePostsAnyNotRuntimeApplyToPosting(t *testing.T) {
	posts := postingSlicePool.Get()
	defer postingSlicePool.Put(posts)
	posts.Append(posting.BuildFromSorted([]uint64{2, 4, 6}))
	posts.Append(posting.BuildFromSorted([]uint64{1, 4, 7}))
	posts.Append(posting.BuildFromSorted([]uint64{3, 5, 7}))
	defer func() {
		for i := 0; i < posts.Len(); i++ {
			posts.Get(i).Release()
		}
	}()

	p := predicate{
		kind: predicateKindPostsAnyNot,
		postsAnyState: &postsAnyFilterState{
			postsBuf: posts,
			neg:      true,
		},
		postsBuf: posts,
	}

	src := posting.BuildFromSorted([]uint64{1, 2, 3, 4, 5, 6, 7, 8})
	defer src.Release()

	got, ok := p.applyToPosting(src.Borrow())
	if !ok {
		t.Fatal("expected exact applyToPosting support for runtime posts-any-not")
	}
	defer got.Release()

	want := posting.BuildFromSorted([]uint64{8})
	defer want.Release()
	if got.Cardinality() != want.Cardinality() || got.AndCardinality(want) != want.Cardinality() {
		t.Fatalf("applyToPosting mismatch: got=%v want=%v", got.ToArray(), want.ToArray())
	}
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
	defer overlayRangePredicateStatePool.Put(state)

	if state.matches(2) {
		t.Fatalf("complement probe hit must be excluded from positive broad range")
	}
	if !state.matches(3) {
		t.Fatalf("non-probe id must stay matched for positive broad range")
	}
}

func TestPredicateSetExpectedContainsCalls_UpdatesOverlayRangeState(t *testing.T) {
	state := overlayRangePredicateStatePool.Get()
	state.ov = fieldOverlay{base: []index{{Key: indexKeyFromString("a")}, {Key: indexKeyFromString("b")}, {Key: indexKeyFromString("c")}}}
	state.br = overlayRange{baseStart: 0, baseEnd: 3}
	state.probe = overlayRangeProbe{
		ov:       fieldOverlay{base: []index{{Key: indexKeyFromString("a")}, {Key: indexKeyFromString("b")}, {Key: indexKeyFromString("c")}}},
		spans:    [2]overlayRange{{baseStart: 0, baseEnd: 3}},
		spanCnt:  1,
		probeLen: 32,
		probeEst: 2048,
	}
	state.bucketCount = 3
	state.linearContainsMax = rangeLinearContainsLimit(state.probe.probeLen, state.probe.probeEst)
	state.materializeAfter = rangeMaterializeAfterForProbe(state.probe.probeLen, state.probe.probeEst)
	state.rangeMaterializeAt = rangePostingFilterMaterializeAfterForProbe(3, 2048)
	state.keepProbeHits = false
	state.setExpectedContainsCalls(state.materializeAfter)
	defer overlayRangePredicateStatePool.Put(state)

	p := predicate{overlayState: state}
	p.setExpectedContainsCalls(1)
	if state.expectedContainsCalls != 1 {
		t.Fatalf("expected overlay expectedContainsCalls=1, got %d", state.expectedContainsCalls)
	}
	if state.containsMode != baseRangeContainsLinear {
		t.Fatalf("expected overlay contains mode to stay linear for single call, got %v", state.containsMode)
	}
}

func TestBaseRangePredicateState_LinearContains_NonLinearModeSkipsLinearPosts(t *testing.T) {
	state := baseRangePredicateStatePool.Get()
	state.probe = baseRangeProbe{
		s: []index{
			{IDs: posting.BuildFromSorted([]uint64{1, 2})},
			{IDs: posting.BuildFromSorted([]uint64{3, 4})},
		},
		start:    0,
		end:      2,
		probeLen: 2,
		probeEst: 4,
	}
	state.expectedContainsCalls = 8
	state.containsMode = baseRangeContainsPosting
	defer baseRangePredicateStatePool.Put(state)

	if !state.linearContains(3) {
		t.Fatalf("expected base linearContains to probe-match idx 3")
	}
	if state.linearPostsBuf != nil {
		t.Fatalf("base linearContains must not build linear posts outside linear mode")
	}
}

func TestOverlayRangePredicateState_LinearContains_NonLinearModeSkipsLinearPosts(t *testing.T) {
	base := []index{
		{Key: indexKeyFromString("a"), IDs: posting.BuildFromSorted([]uint64{1, 2})},
		{Key: indexKeyFromString("b"), IDs: posting.BuildFromSorted([]uint64{3, 4})},
	}
	state := overlayRangePredicateStatePool.Get()
	state.ov = fieldOverlay{base: base}
	state.probe = overlayRangeProbe{
		ov:       fieldOverlay{base: base},
		spans:    [2]overlayRange{{baseStart: 0, baseEnd: 2}},
		spanCnt:  1,
		probeLen: 2,
		probeEst: 4,
	}
	state.bucketCount = 2
	state.expectedContainsCalls = 8
	state.containsMode = baseRangeContainsPosting
	defer overlayRangePredicateStatePool.Put(state)

	if !state.linearContains(3) {
		t.Fatalf("expected overlay linearContains to probe-match idx 3")
	}
	if state.linearPostsBuf != nil {
		t.Fatalf("overlay linearContains must not build linear posts outside linear mode")
	}
}

func TestAcquireOverlayRangePredicateState_PositiveDirectProbeKeepsProbeHitsWithoutPostingFilter(t *testing.T) {
	state := overlayRangePredicateStatePool.Get()
	state.ov = fieldOverlay{base: []index{{Key: indexKeyFromString("a")}, {Key: indexKeyFromString("b")}}}
	state.br = overlayRange{baseStart: 0, baseEnd: 1}
	state.probe = overlayRangeProbe{
		ov:       fieldOverlay{base: []index{{Key: indexKeyFromString("a")}, {Key: indexKeyFromString("b")}}},
		spans:    [2]overlayRange{{baseStart: 0, baseEnd: 1}},
		spanCnt:  1,
		probeLen: 1,
		probeEst: 1,
	}
	state.bucketCount = 1
	state.linearContainsMax = rangeLinearContainsLimit(state.probe.probeLen, state.probe.probeEst)
	state.materializeAfter = rangeMaterializeAfterForProbe(state.probe.probeLen, state.probe.probeEst)
	state.rangeMaterializeAt = rangePostingFilterMaterializeAfterForProbe(1, 1)
	state.keepProbeHits = true
	state.setExpectedContainsCalls(state.materializeAfter)
	defer overlayRangePredicateStatePool.Put(state)

	if !state.keepProbeHits {
		t.Fatalf("positive direct overlay probe must keep probe hits even when posting filter is disabled")
	}
}

func TestPredicateMaterializedIDsDriveContainsAndIterCapabilities(t *testing.T) {
	t.Run("LazyMaterializedPositive", func(t *testing.T) {
		p := predicate{
			expr: mustTestQIRExpr(t, qx.PREFIX("email", "user")),
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
			expr: mustTestQIRExpr(t, qx.NOT(qx.PREFIX("email", "user"))),
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

	expr := qx.PREFIX("email", "user1")
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

	expr := qx.PREFIX("email", "user1")
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

	rawKey := materializedPredCacheKeyFromScalar("email", compileScalarOpForTest(qx.OpPREFIX), "user1")
	parsedKey, ok := materializedPredKeyFromEncoded(rawKey)
	if !ok {
		t.Fatalf("expected parseable materialized cache key %q", rawKey)
	}
	if _, ok := db.getSnapshot().loadMaterializedPredKey(parsedKey); ok {
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

	p, ok := db.buildPredicateWithMode(qx.PREFIX("email", "user"), false)
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

	expr := qx.NOT(qx.GTE("age", 30))
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

	expr := qx.NOT(qx.GTE("age", 30))
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

	expr := qx.GTE("age", 1_000)
	bound, isSlice, err := db.currentQueryViewForTests().exprValueToNormalizedScalarBound(mustTestQIRExprForDB(t, db, expr))
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
	seedGeneratedUint64Data(t, db, 12_000, func(i int) *Rec {
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
	seedGeneratedUint64Data(t, db, 12_000, func(i int) *Rec {
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

	p, ok := db.buildPredicateWithMode(qx.GT("score", 4.0), false)
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
	seedGeneratedUint64Data(t, db, 12_000, func(i int) *Rec {
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

	expr := qx.GT("score", 4.0)
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

func TestRangeIter_AllocsPerRunStayZeroAfterWarmup(t *testing.T) {
	if testRaceEnabled {
		t.Skip("testing.AllocsPerRun is not stable under -race")
	}

	buckets := []index{
		{IDs: posting.BuildFromSorted([]uint64{1, 3, 5})},
		{IDs: posting.BuildFromSorted([]uint64{7})},
		{IDs: posting.BuildFromSorted([]uint64{9, 11})},
	}
	defer func() {
		for i := range buckets {
			buckets[i].IDs.Release()
		}
	}()

	run := func() {
		it := newRangeIter(buckets, 0, len(buckets))
		var sum uint64
		for it.HasNext() {
			sum += it.Next()
		}
		it.Release()
		if sum != 36 {
			t.Fatalf("unexpected iterator sum: %d", sum)
		}
	}

	run()
	allocs := testing.AllocsPerRun(100, run)
	if allocs != 0 {
		t.Fatalf("unexpected allocs after warmup: got=%v want=0", allocs)
	}
}

func TestBuildPredicateWithMode_AllowMaterializeSkipsColdNumericRangeUnionWhenPostingFilterWins(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
		NumericRangeBucketSize:                  8,
		NumericRangeBucketMinFieldKeys:          16,
		NumericRangeBucketMinSpanKeys:           4,
	})
	seedGeneratedUint64Data(t, db, 12_000, func(i int) *Rec {
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

	expr := qx.LT("age", 6_000)
	p, ok := db.currentQueryViewForTests().buildPredicateWithColdMode(mustTestQIRExprForDB(t, db, expr), true, true)
	if !ok {
		t.Fatalf("expected predicate build to succeed")
	}
	defer releasePredicates([]predicate{p})

	if p.overlayState == nil {
		t.Fatalf("expected cold numeric range to stay on overlay runtime state")
	}
	if p.overlayState.probeMaterializeAt <= 1 {
		t.Fatalf("expected runtime posting filter to defer materialization, got probeMaterializeAt=%d", p.overlayState.probeMaterializeAt)
	}
	if p.kind == predicateKindMaterialized || p.kind == predicateKindMaterializedNot {
		t.Fatalf("unexpected eager materialized predicate kind=%v", p.kind)
	}
	if got := db.getSnapshot().matPredCacheCount.Load(); got != 0 {
		t.Fatalf("unexpected shared materialized predicate cache entry: %d", got)
	}
}

func TestBuildPredicateWithMode_RuntimeNumericRangeMaterializationKeepsScalarCacheLocal(t *testing.T) {
	t.Run("OverlayState", func(t *testing.T) {
		db, _ := openTempDBUint64(t, Options{
			AnalyzeInterval:                         -1,
			SnapshotMaterializedPredCacheMaxEntries: 16,
		})
		seedGeneratedUint64Data(t, db, 12_000, func(i int) *Rec {
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

		expr := qx.GT("age", 11_000)
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
		matchCalls := max(128, p.overlayState.materializeAfter+1)
		for i := 1; i <= matchCalls; i++ {
			_ = p.matches(uint64(i))
		}
		if !p.overlayState.rangeMaterialized && !p.overlayState.probeMaterialized {
			t.Fatalf("expected runtime overlay state to build local membership state")
		}
		if p.overlayState.rangeMaterialized && p.overlayState.rangeIDs.IsEmpty() {
			t.Fatalf("expected locally materialized overlay range ids")
		}
		if p.overlayState.probeMaterialized && p.overlayState.probeIDs.IsEmpty() {
			t.Fatalf("expected locally materialized overlay probe ids")
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

		expr := qx.GT("score", 220.0)
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

	expr := qx.GTE("age", 40)
	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)
	compiled := mustTestQIRExprForDB(t, db, expr)
	bound, isSlice, err := view.exprValueToNormalizedScalarBound(compiled)
	if err != nil {
		t.Fatalf("exprValueToNormalizedScalarBound: %v", err)
	}
	if isSlice || bound.full || bound.empty {
		t.Fatalf("unexpected normalized bound state: slice=%v full=%v empty=%v", isSlice, bound.full, bound.empty)
	}
	fullCacheKey := view.materializedPredKey(compiled).String()
	complementCacheKey := view.materializedPredComplementKeyForNormalizedScalarBound("age", bound).String()
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
	expr := qx.GTE("age", 40)
	compiled := mustTestQIRExprForDB(t, db, expr)
	bound, isSlice, err := view.exprValueToNormalizedScalarBound(compiled)
	if err != nil {
		t.Fatalf("exprValueToNormalizedScalarBound: %v", err)
	}
	if isSlice || bound.full || bound.empty {
		t.Fatalf("unexpected normalized bound state: slice=%v full=%v empty=%v", isSlice, bound.full, bound.empty)
	}
	complementCacheKey := view.materializedPredComplementKeyForNormalizedScalarBound("age", bound).String()

	p, ok := view.buildPredicateWithMode(compiled, false)
	if !ok {
		t.Fatalf("expected predicate build to succeed")
	}
	defer releasePredicates([]predicate{p})
	if p.baseRangeState == nil || !p.baseRangeState.probe.useComplement {
		t.Fatalf("expected broad positive base range state with complement probe")
	}

	var universe posting.List
	for i := 1; i <= 256; i++ {
		universe = universe.BuildAdded(uint64(i))
	}
	defer universe.Release()

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

func TestBuildPredicatesOrdered_BroadComplementMaterializesOnFirstSightWhenCostWins(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 256, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   18 + i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)

	expr := qx.GTE("age", 40)
	compiled := mustTestQIRExprForDB(t, db, expr)
	bound, isSlice, err := view.exprValueToNormalizedScalarBound(compiled)
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

	preds1, ok := db.buildPredicatesOrderedWithMode([]qx.Expr{expr}, "score", false, 4096, 0, false, true)
	if !ok {
		t.Fatalf("first buildPredicatesOrderedWithMode: ok=false")
	}
	defer releasePredicates(preds1)
	if len(preds1) != 1 {
		t.Fatalf("unexpected first predicate count: %d", len(preds1))
	}
	if preds1[0].kind != predicateKindMaterializedNot {
		t.Fatalf("expected first ordered broad complement to materialize, got kind=%v", preds1[0].kind)
	}
	if _, ok := db.getSnapshot().loadMaterializedPred(cacheKey); !ok {
		t.Fatalf("expected shared complement cache entry after first ordered build")
	}

	preds2, ok := db.buildPredicatesOrderedWithMode([]qx.Expr{expr}, "score", false, 4096, 0, false, true)
	if !ok {
		t.Fatalf("second buildPredicatesOrderedWithMode: ok=false")
	}
	defer releasePredicates(preds2)
	if len(preds2) != 1 {
		t.Fatalf("unexpected second predicate count: %d", len(preds2))
	}
	if preds2[0].kind != predicateKindMaterializedNot {
		t.Fatalf("expected second ordered broad complement to materialize, got kind=%v", preds2[0].kind)
	}
	if _, ok := db.getSnapshot().loadMaterializedPred(cacheKey); !ok {
		t.Fatalf("expected shared complement cache entry after second ordered build")
	}
}

func TestBuildPredicatesOrdered_BroadComplementWarmCacheHitLoadsWhenOrderedEagerMaterializeDisabled(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 256, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   18 + i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)

	expr := qx.GTE("age", 40)
	compiled := mustTestQIRExprForDB(t, db, expr)
	bound, isSlice, err := view.exprValueToNormalizedScalarBound(compiled)
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

	warm, ok := db.buildPredicatesOrderedWithMode([]qx.Expr{expr}, "score", false, 4096, 0, false, true)
	if !ok {
		t.Fatalf("warm buildPredicatesOrderedWithMode: ok=false")
	}
	releasePredicates(warm)
	if _, ok := db.getSnapshot().loadMaterializedPred(cacheKey); !ok {
		t.Fatalf("expected shared complement cache entry after warm build")
	}

	preds, ok := db.buildPredicatesOrderedWithMode([]qx.Expr{expr}, "score", false, 4096, 0, false, false)
	if !ok {
		t.Fatalf("buildPredicatesOrderedWithMode: ok=false")
	}
	defer releasePredicates(preds)
	if len(preds) != 1 {
		t.Fatalf("unexpected predicate count: %d", len(preds))
	}
	if preds[0].kind != predicateKindMaterializedNot {
		t.Fatalf("expected warm ordered broad complement cache hit, got kind=%v", preds[0].kind)
	}
	if preds[0].hasRuntimeRangeState() {
		t.Fatalf("expected warm cache hit to replace runtime range state")
	}
}

func TestBuildPredicatesOrdered_CoverOrderRangeBroadComplementWarmCacheHitLoadsWhenOrderedEagerMaterializeDisabled(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 256, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   18 + i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)

	expr := qx.GTE("age", 40)
	compiled := mustTestQIRExprForDB(t, db, expr)
	bound, isSlice, err := view.exprValueToNormalizedScalarBound(compiled)
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

	warm, ok := db.buildPredicatesOrderedWithMode([]qx.Expr{expr}, "score", false, 4096, 0, true, true)
	if !ok {
		t.Fatalf("warm buildPredicatesOrderedWithMode: ok=false")
	}
	releasePredicates(warm)
	if _, ok := db.getSnapshot().loadMaterializedPred(cacheKey); !ok {
		t.Fatalf("expected shared complement cache entry after warm build")
	}

	preds, ok := db.buildPredicatesOrderedWithMode([]qx.Expr{expr}, "score", false, 4096, 0, true, false)
	if !ok {
		t.Fatalf("buildPredicatesOrderedWithMode: ok=false")
	}
	defer releasePredicates(preds)
	if len(preds) != 1 {
		t.Fatalf("unexpected predicate count: %d", len(preds))
	}
	if preds[0].kind != predicateKindMaterializedNot {
		t.Fatalf("expected warm ordered broad complement cache hit in coverOrderRange mode, got kind=%v", preds[0].kind)
	}
	if preds[0].hasRuntimeRangeState() {
		t.Fatalf("expected warm cache hit in coverOrderRange mode to replace runtime range state")
	}
}

func TestBuildPredicatesOrdered_CoveredExactRangeWarmCacheHitLoadsWhenPredicateStaysLazy(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 256, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   18 + i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)

	leaves := []qx.Expr{qx.GTE("age", 30), qx.LTE("age", 250)}
	warm, ok := db.buildPredicatesOrderedWithMode(leaves, "score", false, 4096, 0, false, false)
	if !ok {
		t.Fatalf("warm buildPredicatesOrderedWithMode: ok=false")
	}
	if len(warm) != 1 {
		releasePredicates(warm)
		t.Fatalf("unexpected warm predicate count: %d", len(warm))
	}
	if !warm[0].hasEffectiveBounds {
		releasePredicates(warm)
		t.Fatalf("expected merged exact-range warm predicate")
	}
	cacheKey := view.materializedPredComplementKeyForExactScalarRange("age", warm[0].effectiveBounds)
	if cacheKey.isZero() {
		releasePredicates(warm)
		t.Fatalf("expected non-zero exact-range complement cache key")
	}
	if !view.materializeOrderedORPredicate(&warm[0]) {
		releasePredicates(warm)
		t.Fatalf("expected merged exact-range warm predicate to materialize")
	}
	releasePredicates(warm)
	cached, ok := db.getSnapshot().loadMaterializedPredKey(cacheKey)
	if !ok || cached.IsEmpty() {
		t.Fatalf("expected shared merged exact-range complement cache entry after warm materialization")
	}
	cached.Release()

	preds, ok := db.buildPredicatesOrderedWithMode(leaves, "score", false, 4096, 0, true, true)
	if !ok {
		t.Fatalf("buildPredicatesOrderedWithMode: ok=false")
	}
	defer releasePredicates(preds)
	if len(preds) != 1 {
		t.Fatalf("unexpected predicate count: %d", len(preds))
	}
	if preds[0].kind != predicateKindMaterializedNot {
		t.Fatalf("expected covered exact-range warm cache hit, got kind=%v", preds[0].kind)
	}
	if preds[0].hasRuntimeRangeState() {
		t.Fatalf("expected covered exact-range warm cache hit to avoid runtime range state")
	}
}

func TestBuildPredicatesOrderedBuf_CoveredExactRangeWarmCacheHitLoadsWhenPredicateStaysLazy(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 256, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   18 + i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)

	compiled := exprSlicePool.Get()
	defer exprSlicePool.Put(compiled)
	compiled.Append(mustTestQIRExprForDB(t, db, qx.GTE("age", 30)))
	compiled.Append(mustTestQIRExprForDB(t, db, qx.LTE("age", 250)))

	warm, ok := view.buildPredicatesOrderedWithModeBuf(compiled, "score", false, 4096, 0, false, false)
	if !ok {
		t.Fatalf("warm buildPredicatesOrderedWithModeBuf: ok=false")
	}
	if warm.Len() != 1 {
		warm.Release()
		t.Fatalf("unexpected warm predicate count: %d", warm.Len())
	}
	warmPred := warm.Get(0)
	if !warmPred.hasEffectiveBounds {
		warm.Release()
		t.Fatalf("expected merged exact-range warm predicate")
	}
	cacheKey := view.materializedPredComplementKeyForExactScalarRange("age", warmPred.effectiveBounds)
	if cacheKey.isZero() {
		warm.Release()
		t.Fatalf("expected non-zero exact-range complement cache key")
	}
	if !view.materializeOrderedORPredicate(warm.GetPtr(0)) {
		warm.Release()
		t.Fatalf("expected merged exact-range warm predicate to materialize")
	}
	warm.Release()
	cached, ok := db.getSnapshot().loadMaterializedPredKey(cacheKey)
	if !ok || cached.IsEmpty() {
		t.Fatalf("expected shared merged exact-range complement cache entry after warm materialization")
	}
	cached.Release()

	preds, ok := view.buildPredicatesOrderedWithModeBuf(compiled, "score", false, 4096, 0, true, true)
	if !ok {
		t.Fatalf("buildPredicatesOrderedWithModeBuf: ok=false")
	}
	defer preds.Release()
	if preds.Len() != 1 {
		t.Fatalf("unexpected predicate count: %d", preds.Len())
	}
	p := preds.Get(0)
	if p.kind != predicateKindMaterializedNot {
		t.Fatalf("expected covered exact-range warm cache hit, got kind=%v", p.kind)
	}
	if p.hasRuntimeRangeState() {
		t.Fatalf("expected covered exact-range warm cache hit to avoid runtime range state")
	}
}

func TestBuildPredicatesOrdered_BroadComplementStaysDeferredWhenOrderedEagerMaterializeDisabled(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 256, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   18 + i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)

	expr := qx.GTE("age", 40)
	compiled := mustTestQIRExprForDB(t, db, expr)
	bound, isSlice, err := view.exprValueToNormalizedScalarBound(compiled)
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

	preds, ok := db.buildPredicatesOrderedWithMode([]qx.Expr{expr}, "score", false, 4096, 0, false, false)
	if !ok {
		t.Fatalf("buildPredicatesOrderedWithMode: ok=false")
	}
	defer releasePredicates(preds)
	if len(preds) != 1 {
		t.Fatalf("unexpected predicate count: %d", len(preds))
	}
	if preds[0].isMaterializedLike() {
		t.Fatalf("expected ordered broad complement to stay deferred when eager materialization is disabled, got kind=%v", preds[0].kind)
	}
	if !preds[0].hasRuntimeRangeState() {
		t.Fatalf("expected deferred ordered broad complement to keep runtime range state")
	}
	if _, ok := db.getSnapshot().loadMaterializedPred(cacheKey); ok {
		t.Fatalf("unexpected shared complement cache entry with eager materialization disabled")
	}
}

func TestBuildPredicatesOrdered_BroadNullableComplementMaterializesWhenOrderedEagerMaterializeDisabled(t *testing.T) {
	db, _ := openTempDBUint64PtrInt(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	for i := 0; i < 4; i++ {
		rec := &PtrIntRec{Name: fmt.Sprintf("nil_%02d", i), Rank: nil, Active: i%2 == 0}
		if err := db.Set(uint64(i+1), rec); err != nil {
			t.Fatalf("Set(nil_%02d): %v", i, err)
		}
	}
	for i := 0; i < 24; i++ {
		rec := &PtrIntRec{Name: fmt.Sprintf("rank_%02d", i), Rank: intPtr(i), Active: i%2 == 0}
		if err := db.Set(uint64(i+5), rec); err != nil {
			t.Fatalf("Set(rank_%02d): %v", i, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)

	expr := qx.GTE("rank", 4)
	compiled := mustTestQIRExprForDB(t, db, expr)
	candidate, ok := view.prepareScalarRangeRoutingCandidate(compiled)
	if !ok {
		t.Fatalf("expected broad nullable range routing candidate")
	}
	if !candidate.plan.useComplement || !candidate.broadComplementCardinality(view.snapshotUniverseCardinality()) {
		t.Fatalf("expected broad complement route for nullable ordered predicate")
	}

	preds, ok := db.buildPredicatesOrderedWithMode([]qx.Expr{expr}, "name", false, 4096, 0, false, false)
	if !ok {
		t.Fatalf("buildPredicatesOrderedWithMode: ok=false")
	}
	defer releasePredicates(preds)
	if len(preds) != 1 {
		t.Fatalf("unexpected predicate count: %d", len(preds))
	}
	if !preds[0].isMaterializedLike() {
		t.Fatalf("expected nullable broad complement to materialize for correctness")
	}
	if preds[0].hasRuntimeRangeState() {
		t.Fatalf("expected nullable broad complement to avoid deferred runtime range state")
	}
	if preds[0].matches(1) || preds[0].matches(2) {
		t.Fatalf("nil rank rows must not match positive broad range")
	}
	if !preds[0].matches(9) {
		t.Fatalf("expected in-range row to match materialized nullable broad range")
	}
}

func TestBuildPredicatesOrdered_NonBroadNullableComplementMaterializesWhenOrderedEagerMaterializeDisabled(t *testing.T) {
	db, _ := openTempDBUint64PtrInt(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	for i := 0; i < 64; i++ {
		rec := &PtrIntRec{Name: fmt.Sprintf("nil_%02d", i), Rank: nil, Active: i%2 == 0}
		if err := db.Set(uint64(i+1), rec); err != nil {
			t.Fatalf("Set(nil_%02d): %v", i, err)
		}
	}
	for i := 0; i < 40; i++ {
		v := 0
		rec := &PtrIntRec{Name: fmt.Sprintf("zero_%02d", i), Rank: &v, Active: i%2 == 0}
		if err := db.Set(uint64(i+65), rec); err != nil {
			t.Fatalf("Set(zero_%02d): %v", i, err)
		}
	}
	for i := 1; i <= 9; i++ {
		v := i
		rec := &PtrIntRec{Name: fmt.Sprintf("rank_%02d", i), Rank: &v, Active: true}
		if err := db.Set(uint64(i+105), rec); err != nil {
			t.Fatalf("Set(rank_%02d): %v", i, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)

	expr := qx.GTE("rank", 1)
	compiled := mustTestQIRExprForDB(t, db, expr)
	candidate, ok := view.prepareScalarRangeRoutingCandidate(compiled)
	if !ok {
		t.Fatalf("expected nullable range routing candidate")
	}
	if !candidate.plan.useComplement {
		t.Fatalf("expected complement route for sparse nullable ordered predicate")
	}
	if candidate.broadComplementCardinality(view.snapshotUniverseCardinality()) {
		t.Fatalf("expected non-broad complement route for sparse nullable ordered predicate")
	}

	preds, ok := db.buildPredicatesOrderedWithMode([]qx.Expr{expr}, "name", false, 4096, 0, false, false)
	if !ok {
		t.Fatalf("buildPredicatesOrderedWithMode: ok=false")
	}
	defer releasePredicates(preds)
	if len(preds) != 1 {
		t.Fatalf("unexpected predicate count: %d", len(preds))
	}
	if !preds[0].isMaterializedLike() {
		t.Fatalf("expected nullable non-broad complement to materialize for correctness")
	}
	if preds[0].kind == predicateKindMaterializedNot || !preds[0].rangeMat {
		t.Fatalf("expected nil-heavy nullable non-broad range to materialize positive side, got kind=%v rangeMat=%v", preds[0].kind, preds[0].rangeMat)
	}
	if preds[0].hasRuntimeRangeState() {
		t.Fatalf("expected nullable non-broad complement to avoid deferred runtime range state")
	}
	if preds[0].matches(1) || preds[0].matches(3) {
		t.Fatalf("nil rank rows must not match positive non-broad range")
	}
	if !preds[0].matches(106) {
		t.Fatalf("expected in-range row to match materialized nullable non-broad range")
	}
}

func TestMaterializeOrderedORPredicate_ExactRangeComplementSharesCache(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 256, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   18 + i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)

	expr := mustTestQIRExprForDB(t, db, qx.GTE("age", 40))
	bound, isSlice, err := view.exprValueToNormalizedScalarBound(expr)
	if err != nil {
		t.Fatalf("exprValueToNormalizedScalarBound: %v", err)
	}
	if isSlice || bound.full || bound.empty {
		t.Fatalf("unexpected normalized bound state: slice=%v full=%v empty=%v", isSlice, bound.full, bound.empty)
	}
	bounds := rangeBoundsForNormalizedScalarBound(bound)
	cacheKey := view.materializedPredComplementKeyForExactScalarRange("age", bounds)
	if cacheKey.isZero() {
		t.Fatal("expected non-zero exact-range complement cache key")
	}

	p := predicate{
		expr:               expr,
		hasEffectiveBounds: true,
		effectiveBounds:    bounds,
	}
	gotKey, buildWork, checkWork, cachedCheckWork, ok := view.orderedORMaterializedExactRangePredicateCosts("score", p)
	if !ok {
		t.Fatal("expected ordered exact-range complement build info")
	}
	if gotKey != cacheKey {
		t.Fatalf("unexpected cache key: got=%q want=%q", gotKey.String(), cacheKey.String())
	}
	if buildWork == 0 || checkWork == 0 || cachedCheckWork == 0 {
		t.Fatalf("expected non-zero exact-range complement work metrics, got build=%d check=%d cached=%d", buildWork, checkWork, cachedCheckWork)
	}
	if !view.materializeOrderedORPredicate(&p) {
		t.Fatal("expected exact-range complement predicate to materialize")
	}
	defer releasePredicates([]predicate{p})
	if p.kind != predicateKindMaterializedNot {
		t.Fatalf("expected complement-backed materialized predicate, got kind=%v", p.kind)
	}

	cached, ok := db.getSnapshot().loadMaterializedPredKey(cacheKey)
	if !ok || cached.IsEmpty() {
		t.Fatal("expected shared exact-range complement cache entry after materialization")
	}
	cached.Release()
}

func TestOrderedORMaterializedRangeLeafCosts_NullableComplementPrefersPositiveCacheKey(t *testing.T) {
	db, _ := openTempDBUint64PtrInt(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	for i := 0; i < 64; i++ {
		rec := &PtrIntRec{Name: fmt.Sprintf("nil_%02d", i), Rank: nil, Active: i%2 == 0}
		if err := db.Set(uint64(i+1), rec); err != nil {
			t.Fatalf("Set(nil_%02d): %v", i, err)
		}
	}
	for i := 0; i < 40; i++ {
		v := 0
		rec := &PtrIntRec{Name: fmt.Sprintf("zero_%02d", i), Rank: &v, Active: i%2 == 0}
		if err := db.Set(uint64(i+65), rec); err != nil {
			t.Fatalf("Set(zero_%02d): %v", i, err)
		}
	}
	for i := 1; i <= 9; i++ {
		v := i
		rec := &PtrIntRec{Name: fmt.Sprintf("rank_%02d", i), Rank: &v, Active: true}
		if err := db.Set(uint64(i+105), rec); err != nil {
			t.Fatalf("Set(rank_%02d): %v", i, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)

	expr := mustTestQIRExprForDB(t, db, qx.GTE("rank", 1))
	candidate, ok := view.prepareScalarRangeRoutingCandidate(expr)
	if !ok {
		t.Fatalf("expected nullable range routing candidate")
	}
	if !candidate.plan.useComplement {
		t.Fatalf("expected complement route for nullable ordered range")
	}
	compPlan, ok := candidate.core.prepareComplementMaterialization()
	if !ok || !candidate.shouldPreferPositiveMaterializationForNullableComplement(compPlan) {
		t.Fatalf("expected nullable ordered range to prefer positive-side materialization")
	}
	wantKey := view.materializedPredKeyForNormalizedScalarBound("rank", candidate.core.bound)
	if wantKey.isZero() {
		t.Fatalf("expected non-zero positive-side cache key")
	}
	gotKey, buildWork, checkWork, cachedCheckWork, ok := view.orderedORMaterializedRangeLeafCosts("name", expr)
	if !ok {
		t.Fatalf("expected ordered range leaf build info")
	}
	if gotKey != wantKey {
		t.Fatalf("unexpected nullable ordered range cache key: got=%q want=%q", gotKey.String(), wantKey.String())
	}
	if buildWork == 0 || checkWork == 0 || cachedCheckWork == 0 {
		t.Fatalf("expected non-zero nullable ordered range work metrics, got build=%d check=%d cached=%d", buildWork, checkWork, cachedCheckWork)
	}
}

func TestOrderedORMaterializedExactRangePredicateCosts_NullableComplementPrefersPositiveCacheKey(t *testing.T) {
	db, _ := openTempDBUint64PtrInt(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	for i := 0; i < 64; i++ {
		rec := &PtrIntRec{Name: fmt.Sprintf("nil_%02d", i), Rank: nil, Active: i%2 == 0}
		if err := db.Set(uint64(i+1), rec); err != nil {
			t.Fatalf("Set(nil_%02d): %v", i, err)
		}
	}
	for i := 0; i < 40; i++ {
		v := 0
		rec := &PtrIntRec{Name: fmt.Sprintf("zero_%02d", i), Rank: &v, Active: i%2 == 0}
		if err := db.Set(uint64(i+65), rec); err != nil {
			t.Fatalf("Set(zero_%02d): %v", i, err)
		}
	}
	for i := 1; i <= 9; i++ {
		v := i
		rec := &PtrIntRec{Name: fmt.Sprintf("rank_%02d", i), Rank: &v, Active: true}
		if err := db.Set(uint64(i+105), rec); err != nil {
			t.Fatalf("Set(rank_%02d): %v", i, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)

	preds, ok := db.buildPredicatesOrderedWithMode(
		[]qx.Expr{qx.GTE("rank", 1), qx.LTE("rank", 9)},
		"name",
		false,
		4096,
		0,
		false,
		false,
	)
	if !ok {
		t.Fatalf("buildPredicatesOrderedWithMode: ok=false")
	}
	defer releasePredicates(preds)
	if len(preds) != 1 {
		t.Fatalf("unexpected predicate count: %d", len(preds))
	}
	if !preds[0].hasEffectiveBounds {
		t.Fatalf("expected merged exact-range predicate")
	}
	wantKey := view.materializedPredKeyForExactScalarRange("rank", preds[0].effectiveBounds)
	if wantKey.isZero() {
		t.Fatalf("expected non-zero positive-side exact-range cache key")
	}
	gotKey, buildWork, checkWork, cachedCheckWork, ok := view.orderedORMaterializedExactRangePredicateCosts("name", preds[0])
	if !ok {
		t.Fatalf("expected ordered exact-range build info")
	}
	if gotKey != wantKey {
		t.Fatalf("unexpected nullable ordered exact-range cache key: got=%q want=%q", gotKey.String(), wantKey.String())
	}
	if buildWork == 0 || checkWork == 0 || cachedCheckWork == 0 {
		t.Fatalf("expected non-zero nullable ordered exact-range work metrics, got build=%d check=%d cached=%d", buildWork, checkWork, cachedCheckWork)
	}
}

func TestMaterializeOrderedORPredicate_PreservesMergedExactRangeBounds(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 256, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   18 + i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)

	preds, ok := db.buildPredicatesOrderedWithMode(
		[]qx.Expr{qx.GTE("age", 30), qx.LTE("age", 60)},
		"score",
		false,
		4096,
		0,
		false,
		false,
	)
	if !ok {
		t.Fatalf("buildPredicatesOrderedWithMode: ok=false")
	}
	defer releasePredicates(preds)
	if len(preds) != 1 {
		t.Fatalf("unexpected predicate count: %d", len(preds))
	}
	if !preds[0].hasEffectiveBounds {
		t.Fatalf("expected merged exact-range predicate to retain effective bounds before materialization")
	}
	wantBounds := preds[0].effectiveBounds
	wantKey := view.materializedPredKeyForExactScalarRange("age", wantBounds)
	if wantKey.isZero() {
		t.Fatalf("expected non-zero exact-range cache key")
	}

	if !view.materializeOrderedORPredicate(&preds[0]) {
		t.Fatalf("expected merged exact-range predicate to materialize")
	}
	if !preds[0].hasEffectiveBounds {
		t.Fatalf("expected materialized merged exact-range predicate to keep effective bounds")
	}
	if preds[0].effectiveBounds != wantBounds {
		t.Fatalf("effectiveBounds changed after materialization")
	}

	gotKey, _, _, _, ok := view.orderedORMaterializedExactRangePredicateCosts("score", preds[0])
	if !ok {
		t.Fatalf("expected materialized merged exact-range predicate to remain routable as exact range")
	}
	if gotKey != wantKey {
		t.Fatalf("unexpected exact-range cache key after materialization: got=%q want=%q", gotKey.String(), wantKey.String())
	}
}

func TestBuildPredicatesOrdered_MergedExactComplementWarmCacheHitLoadsWhenOrderedEagerMaterializeDisabled(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 256, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   18 + i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)

	leaves := []qx.Expr{qx.GTE("age", 30), qx.LTE("age", 250)}
	warm, ok := db.buildPredicatesOrderedWithMode(leaves, "score", false, 4096, 0, false, false)
	if !ok {
		t.Fatalf("warm buildPredicatesOrderedWithMode: ok=false")
	}
	if len(warm) != 1 {
		releasePredicates(warm)
		t.Fatalf("unexpected warm predicate count: %d", len(warm))
	}
	if !warm[0].hasEffectiveBounds {
		releasePredicates(warm)
		t.Fatalf("expected merged exact-range warm predicate")
	}
	wantBounds := warm[0].effectiveBounds
	cacheKey := view.materializedPredComplementKeyForExactScalarRange("age", warm[0].effectiveBounds)
	if cacheKey.isZero() {
		releasePredicates(warm)
		t.Fatalf("expected non-zero exact-range complement cache key")
	}
	if !view.materializeOrderedORPredicate(&warm[0]) {
		releasePredicates(warm)
		t.Fatalf("expected merged exact-range warm predicate to materialize")
	}
	if !warm[0].hasEffectiveBounds {
		releasePredicates(warm)
		t.Fatalf("expected broad merged exact-range complement materialization to preserve effective bounds")
	}
	if warm[0].effectiveBounds != wantBounds {
		releasePredicates(warm)
		t.Fatalf("effectiveBounds changed after broad merged exact-range complement materialization")
	}
	gotKey, _, _, _, ok := view.orderedORMaterializedExactRangePredicateCosts("score", warm[0])
	if !ok {
		releasePredicates(warm)
		t.Fatalf("expected broad merged exact-range complement materialization to remain routable as exact range")
	}
	if gotKey != cacheKey {
		releasePredicates(warm)
		t.Fatalf("unexpected exact-range complement cache key after materialization: got=%q want=%q", gotKey.String(), cacheKey.String())
	}
	releasePredicates(warm)
	cached, ok := db.getSnapshot().loadMaterializedPredKey(cacheKey)
	if !ok || cached.IsEmpty() {
		t.Fatalf("expected shared merged exact-range complement cache entry after warm materialization")
	}
	cached.Release()

	preds, ok := db.buildPredicatesOrderedWithMode(leaves, "score", false, 4096, 0, false, false)
	if !ok {
		t.Fatalf("buildPredicatesOrderedWithMode: ok=false")
	}
	defer releasePredicates(preds)
	if len(preds) != 1 {
		t.Fatalf("unexpected predicate count: %d", len(preds))
	}
	if preds[0].kind != predicateKindMaterializedNot {
		t.Fatalf("expected merged exact-range complement warm cache hit, got kind=%v", preds[0].kind)
	}
	if preds[0].hasRuntimeRangeState() {
		t.Fatalf("expected merged exact-range complement warm cache hit to avoid runtime range state")
	}
	if !preds[0].hasEffectiveBounds {
		t.Fatalf("expected merged exact-range complement warm cache hit to preserve effective bounds")
	}
	if preds[0].effectiveBounds != wantBounds {
		t.Fatalf("effectiveBounds changed on merged exact-range complement warm cache hit")
	}
	gotKey, _, _, _, ok = view.orderedORMaterializedExactRangePredicateCosts("score", preds[0])
	if !ok {
		t.Fatalf("expected merged exact-range complement warm cache hit to remain routable as exact range")
	}
	if gotKey != cacheKey {
		t.Fatalf("unexpected exact-range complement cache key after warm cache hit: got=%q want=%q", gotKey.String(), cacheKey.String())
	}
}

func TestBuildPredicatesOrdered_MergedExactNonBroadComplementWarmCacheHitLoadsWhenOrderedEagerMaterializeDisabled(t *testing.T) {
	db, _ := openTempDBUint64PtrInt(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	for i := 0; i < 40; i++ {
		v := 0
		rec := &PtrIntRec{Name: fmt.Sprintf("zero_%02d", i), Rank: &v, Active: i%2 == 0}
		if err := db.Set(uint64(i+1), rec); err != nil {
			t.Fatalf("Set(zero_%02d): %v", i, err)
		}
	}
	for i := 1; i <= 9; i++ {
		v := i
		rec := &PtrIntRec{Name: fmt.Sprintf("rank_%02d", i), Rank: &v, Active: true}
		if err := db.Set(uint64(i+41), rec); err != nil {
			t.Fatalf("Set(rank_%02d): %v", i, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)

	leaves := []qx.Expr{qx.GTE("rank", 1), qx.LTE("rank", 9)}
	warm, ok := db.buildPredicatesOrderedWithMode(leaves, "name", false, 4096, 0, false, false)
	if !ok {
		t.Fatalf("warm buildPredicatesOrderedWithMode: ok=false")
	}
	if len(warm) != 1 {
		releasePredicates(warm)
		t.Fatalf("unexpected warm predicate count: %d", len(warm))
	}
	if !warm[0].hasEffectiveBounds {
		releasePredicates(warm)
		t.Fatalf("expected merged exact-range warm predicate")
	}
	cacheKey := view.materializedPredComplementKeyForExactScalarRange("rank", warm[0].effectiveBounds)
	if cacheKey.isZero() {
		releasePredicates(warm)
		t.Fatalf("expected non-zero exact-range complement cache key")
	}
	if warm[0].isMaterializedLike() {
		releasePredicates(warm)
		t.Fatalf("expected first build to keep non-broad complement on runtime state")
	}
	if !view.materializeOrderedORPredicate(&warm[0]) {
		releasePredicates(warm)
		t.Fatalf("expected merged exact-range predicate to materialize")
	}
	if warm[0].kind != predicateKindMaterializedNot {
		releasePredicates(warm)
		t.Fatalf("expected complement-backed merged exact-range predicate, got kind=%v", warm[0].kind)
	}
	releasePredicates(warm)

	cached, ok := db.getSnapshot().loadMaterializedPredKey(cacheKey)
	if !ok || cached.IsEmpty() {
		t.Fatalf("expected shared merged exact-range complement cache entry after warm materialization")
	}
	cached.Release()

	preds, ok := db.buildPredicatesOrderedWithMode(leaves, "name", false, 4096, 0, false, false)
	if !ok {
		t.Fatalf("buildPredicatesOrderedWithMode: ok=false")
	}
	defer releasePredicates(preds)
	if len(preds) != 1 {
		t.Fatalf("unexpected predicate count: %d", len(preds))
	}
	if preds[0].kind != predicateKindMaterializedNot {
		t.Fatalf("expected merged exact-range non-broad complement warm cache hit, got kind=%v", preds[0].kind)
	}
	if preds[0].hasRuntimeRangeState() {
		t.Fatalf("expected merged exact-range non-broad complement warm cache hit to avoid runtime range state")
	}
}

func TestBuildPredicatesOrdered_CoverOrderRangeMergedExactComplementWarmCacheHitLoadsWhenOrderedEagerMaterializeDisabled(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 256, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   18 + i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)

	leaves := []qx.Expr{qx.GTE("age", 30), qx.LTE("age", 250)}
	warm, ok := db.buildPredicatesOrderedWithMode(leaves, "score", false, 4096, 0, true, false)
	if !ok {
		t.Fatalf("warm buildPredicatesOrderedWithMode: ok=false")
	}
	if len(warm) != 1 {
		releasePredicates(warm)
		t.Fatalf("unexpected warm predicate count: %d", len(warm))
	}
	if !warm[0].hasEffectiveBounds {
		releasePredicates(warm)
		t.Fatalf("expected merged exact-range warm predicate")
	}
	wantBounds := warm[0].effectiveBounds
	cacheKey := view.materializedPredComplementKeyForExactScalarRange("age", warm[0].effectiveBounds)
	if cacheKey.isZero() {
		releasePredicates(warm)
		t.Fatalf("expected non-zero exact-range complement cache key")
	}
	if !view.materializeOrderedORPredicate(&warm[0]) {
		releasePredicates(warm)
		t.Fatalf("expected merged exact-range warm predicate to materialize")
	}
	releasePredicates(warm)
	cached, ok := db.getSnapshot().loadMaterializedPredKey(cacheKey)
	if !ok || cached.IsEmpty() {
		t.Fatalf("expected shared merged exact-range complement cache entry after warm materialization")
	}
	cached.Release()

	preds, ok := db.buildPredicatesOrderedWithMode(leaves, "score", false, 4096, 0, true, false)
	if !ok {
		t.Fatalf("buildPredicatesOrderedWithMode: ok=false")
	}
	defer releasePredicates(preds)
	if len(preds) != 1 {
		t.Fatalf("unexpected predicate count: %d", len(preds))
	}
	if preds[0].kind != predicateKindMaterializedNot {
		t.Fatalf("expected merged exact-range complement warm cache hit in coverOrderRange mode, got kind=%v", preds[0].kind)
	}
	if preds[0].hasRuntimeRangeState() {
		t.Fatalf("expected merged exact-range complement warm cache hit in coverOrderRange mode to avoid runtime range state")
	}
	if !preds[0].hasEffectiveBounds {
		t.Fatalf("expected merged exact-range complement warm cache hit in coverOrderRange mode to preserve effective bounds")
	}
	if preds[0].effectiveBounds != wantBounds {
		t.Fatalf("effectiveBounds changed on coverOrderRange merged exact-range complement warm cache hit")
	}
	gotKey, _, _, _, ok := view.orderedORMaterializedExactRangePredicateCosts("score", preds[0])
	if !ok {
		t.Fatalf("expected coverOrderRange merged exact-range complement warm cache hit to remain routable as exact range")
	}
	if gotKey != cacheKey {
		t.Fatalf("unexpected exact-range complement cache key after coverOrderRange warm cache hit: got=%q want=%q", gotKey.String(), cacheKey.String())
	}
}

func TestOrderedORMaterializedPrefixLeafBuildWork_RejectsBroadComplementPrefix(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 256, func(i int) *Rec {
		email := fmt.Sprintf("user%05d@example.com", i)
		if i >= 224 {
			email = fmt.Sprintf("other%05d@example.com", i)
		}
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Email: email,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)

	expr := mustTestQIRExprForDB(t, db, qx.PREFIX("email", "user"))
	candidate, ok := view.prepareScalarRangeRoutingCandidate(expr)
	if !ok {
		t.Fatal("expected broad prefix routing candidate")
	}
	if !candidate.plan.useComplement {
		t.Fatal("expected broad prefix candidate to choose complement probe")
	}

	cacheKey, buildWork, ok := view.orderedORMaterializedPrefixLeafBuildWork("score", expr)
	if ok || buildWork != 0 || !cacheKey.isZero() {
		t.Fatalf("expected broad prefix complement candidate to be rejected, got ok=%v build=%d key=%q", ok, buildWork, cacheKey.String())
	}
}

func TestBuildPredicateWithMode_RuntimeExactUnionPromotesOnSecondMaterialize(t *testing.T) {
	run := func(
		t *testing.T,
		expr qx.Expr,
		wantKind predicateKind,
		cacheKey materializedPredKey,
		seed func(*DB[uint64, Rec]),
	) {
		db, _ := openTempDBUint64(t, Options{
			AnalyzeInterval:                         -1,
			SnapshotMaterializedPredCacheMaxEntries: 16,
		})
		seed(db)
		if err := db.RebuildIndex(); err != nil {
			t.Fatalf("RebuildIndex: %v", err)
		}

		materialize := func() predicate {
			p, ok := db.buildPredicateWithMode(expr, false)
			if !ok {
				t.Fatalf("expected predicate build to succeed")
			}
			if p.kind != wantKind || p.postsAnyState == nil {
				releasePredicates([]predicate{p})
				t.Fatalf("expected runtime exact-union predicate kind=%v, got kind=%v", wantKind, p.kind)
			}
			calls := p.postsAnyState.containsMaterializeAt + 1
			for i := 0; i < calls; i++ {
				_ = p.matches(uint64(i + 1))
			}
			if p.postsAnyState.ids.IsEmpty() {
				releasePredicates([]predicate{p})
				t.Fatalf("expected runtime exact union materialization after %d calls", calls)
			}
			return p
		}

		first := materialize()
		if _, ok := db.getSnapshot().loadMaterializedPredKey(cacheKey); ok {
			releasePredicates([]predicate{first})
			t.Fatalf("unexpected shared exact-union cache entry after first materialize")
		}
		releasePredicates([]predicate{first})

		second := materialize()
		releasePredicates([]predicate{second})

		cached, ok := db.getSnapshot().loadMaterializedPredKey(cacheKey)
		if !ok || cached.IsEmpty() {
			t.Fatalf("expected shared exact-union cache entry after second materialize")
		}
		defer cached.Release()

		third, ok := db.buildPredicateWithMode(expr, false)
		if !ok {
			t.Fatalf("expected third predicate build to succeed")
		}
		defer releasePredicates([]predicate{third})
		if third.kind != wantKind || third.postsAnyState == nil {
			t.Fatalf("expected third predicate to stay runtime exact-union kind=%v, got kind=%v", wantKind, third.kind)
		}
		ids := third.postsAnyState.materialize()
		if ids.IsEmpty() {
			t.Fatalf("expected cached exact-union materialization ids")
		}
		if !ids.SharesPayload(cached) {
			t.Fatalf("expected third materialize to load shared exact-union cache payload")
		}
	}

	t.Run("IN", func(t *testing.T) {
		vals := stringSlicePool.Get()
		vals.Append("DE")
		vals.Append("FR")
		cacheKey := materializedPredKeyForDistinctSetTerms("country", compileScalarOpForTest(qx.OpIN), vals, false)
		stringSlicePool.Put(vals)

		run(t,
			qx.IN("country", []string{"DE", "FR"}),
			predicateKindPostsAny,
			cacheKey,
			func(db *DB[uint64, Rec]) {
				seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
					return &Rec{
						Name:   fmt.Sprintf("u_%d", i),
						Email:  fmt.Sprintf("user%05d@example.com", i),
						Age:    18 + (i % 60),
						Score:  float64(20_000 - i),
						Active: i%2 == 0,
						Meta: Meta{
							Country: [...]string{"US", "NL", "DE", "PL", "SE", "FR", "GB", "ES"}[i%8],
						},
					}
				})
			},
		)
	})

	t.Run("HASANY", func(t *testing.T) {
		vals := stringSlicePool.Get()
		vals.Append("db")
		vals.Append("go")
		cacheKey := materializedPredKeyForDistinctSetTerms("tags", compileScalarOpForTest(qx.OpHASANY), vals, false)
		stringSlicePool.Put(vals)

		run(t,
			qx.HASANY("tags", []string{"go", "db"}),
			predicateKindPostsAny,
			cacheKey,
			func(db *DB[uint64, Rec]) {
				seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
					return &Rec{
						Name:   fmt.Sprintf("u_%d", i),
						Email:  fmt.Sprintf("user%05d@example.com", i),
						Age:    18 + (i % 60),
						Score:  float64(i),
						Active: i%2 == 0,
						Tags: [...][]string{
							{"go", "db"},
							{"db", "ops"},
							{"go", "perf"},
							{"security"},
						}[i%4],
					}
				})
			},
		)
	})

	t.Run("NOTIN", func(t *testing.T) {
		vals := stringSlicePool.Get()
		vals.Append("DE")
		vals.Append("FR")
		cacheKey := materializedPredKeyForDistinctSetTerms("country", compileScalarOpForTest(qx.OpIN), vals, false)
		stringSlicePool.Put(vals)

		run(t,
			qx.NOT(qx.IN("country", []string{"DE", "FR"})),
			predicateKindPostsAnyNot,
			cacheKey,
			func(db *DB[uint64, Rec]) {
				seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
					return &Rec{
						Name:   fmt.Sprintf("u_%d", i),
						Email:  fmt.Sprintf("user%05d@example.com", i),
						Age:    18 + (i % 60),
						Score:  float64(20_000 - i),
						Active: i%2 == 0,
						Meta: Meta{
							Country: [...]string{"US", "NL", "DE", "PL", "SE", "FR", "GB", "ES"}[i%8],
						},
					}
				})
			},
		)
	})

	t.Run("HASNONE", func(t *testing.T) {
		vals := stringSlicePool.Get()
		vals.Append("db")
		vals.Append("go")
		cacheKey := materializedPredKeyForDistinctSetTerms("tags", compileScalarOpForTest(qx.OpHASANY), vals, false)
		stringSlicePool.Put(vals)

		run(t,
			qx.NOT(qx.HASANY("tags", []string{"go", "db"})),
			predicateKindPostsAnyNot,
			cacheKey,
			func(db *DB[uint64, Rec]) {
				seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
					return &Rec{
						Name:   fmt.Sprintf("u_%d", i),
						Email:  fmt.Sprintf("user%05d@example.com", i),
						Age:    18 + (i % 60),
						Score:  float64(i),
						Active: i%2 == 0,
						Tags: [...][]string{
							{"go", "db"},
							{"db", "ops"},
							{"go", "perf"},
							{"security"},
						}[i%4],
					}
				})
			},
		)
	})
}

func TestBuildPredicateWithMode_HasPromotesOnSecondBuild(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})

	seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%05d@example.com", i),
			Age:    18 + (i % 60),
			Score:  float64(i),
			Active: i%2 == 0,
			Tags: [...][]string{
				{"go", "db"},
				{"db", "ops"},
				{"go", "perf"},
				{"security"},
			}[i%4],
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	vals := stringSlicePool.Get()
	vals.Append("db")
	vals.Append("go")
	cacheKey := materializedPredKeyForDistinctSetTerms("tags", compileScalarOpForTest(qx.OpHASALL), vals, false)
	stringSlicePool.Put(vals)

	expr := qx.HASALL("tags", []string{"go", "db"})

	first, ok := db.buildPredicateWithMode(expr, false)
	if !ok {
		t.Fatalf("expected first predicate build to succeed")
	}
	if first.kind != predicateKindPostsAll {
		releasePredicates([]predicate{first})
		t.Fatalf("expected first predicate to stay posts-all, got kind=%v", first.kind)
	}
	releasePredicates([]predicate{first})

	second, ok := db.buildPredicateWithMode(expr, false)
	if !ok {
		t.Fatalf("expected second predicate build to succeed")
	}
	defer releasePredicates([]predicate{second})
	if second.kind != predicateKindMaterialized {
		t.Fatalf("expected second predicate to materialize, got kind=%v", second.kind)
	}
	if second.ids.IsEmpty() {
		t.Fatalf("expected second predicate to hold materialized ids")
	}

	cached, ok := db.getSnapshot().loadMaterializedPredKey(cacheKey)
	if !ok || cached.IsEmpty() {
		t.Fatalf("expected shared HAS cache entry after second build")
	}
	defer cached.Release()
	if !second.ids.SharesPayload(cached) {
		t.Fatalf("expected second predicate ids to use shared cache payload")
	}
}

func TestOverlayRangeStats_ChunkedMatchesCursorScanOnLargeUniqueRange(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	seedGeneratedUint64Data(t, db, 16_000, func(i int) *Rec {
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
		{has: true, hasLo: true, loKey: int64ByteStr(3_500), loInc: true},
		{has: true, hasLo: true, loKey: int64ByteStr(3_500), loInc: false},
		{has: true, hasHi: true, hiKey: int64ByteStr(3_500), hiInc: false},
		{has: true, hasLo: true, loKey: int64ByteStr(1_000), loInc: true, hasHi: true, hiKey: int64ByteStr(12_000), hiInc: false},
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

func TestOrderedScalarRangeCanEagerMaterialize_RequiresPromotionOnlyForSmallOrderedWindows(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	seedGeneratedUint64Data(t, db, 32, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)

	expr := qx.GTE("age", 10)
	cacheKey := view.materializedPredKey(mustTestQIRExprForDB(t, db, expr))
	if cacheKey.isZero() {
		t.Fatalf("expected non-zero materialized predicate cache key")
	}

	route := orderedScalarRangeRouting{
		eagerMaterialize: true,
		cacheKey:         cacheKey,
		requirePromotion: true,
	}
	if view.orderedScalarRangeCanEagerMaterialize(route) {
		t.Fatalf("expected first-hit ordered eager materialization to stay disabled")
	}
	if !view.orderedScalarRangeCanEagerMaterialize(route) {
		t.Fatalf("expected second-hit ordered eager materialization to become enabled")
	}

	if !view.orderedScalarRangeCanEagerMaterialize(orderedScalarRangeRouting{
		eagerMaterialize: true,
		cacheKey:         cacheKey,
	}) {
		t.Fatalf("expected wide ordered window eager materialization to stay enabled")
	}
	if !view.orderedScalarRangeCanEagerMaterialize(orderedScalarRangeRouting{eagerMaterialize: true}) {
		t.Fatalf("expected zero-cache-key ordered eager materialization to stay enabled")
	}
}

func TestOrderedScalarRangeCanEagerMaterialize_UsesComplementPromotionKey(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	seedGeneratedUint64Data(t, db, 256, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   18 + i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	view := db.currentQueryViewForTests()
	defer db.releaseQueryView(view)

	preds, ok := db.buildPredicatesOrderedWithMode(
		[]qx.Expr{qx.GTE("age", 30), qx.LTE("age", 250)},
		"score",
		false,
		16,
		0,
		false,
		false,
	)
	if !ok {
		t.Fatalf("buildPredicatesOrderedWithMode: ok=false")
	}
	defer releasePredicates(preds)
	if len(preds) != 1 {
		t.Fatalf("unexpected predicate count: %d", len(preds))
	}
	if !preds[0].hasEffectiveBounds {
		t.Fatalf("expected merged exact-range predicate")
	}

	route := view.orderedPredicateScalarRangeRouting(
		preds[0],
		16,
		0,
		view.snapshotUniverseCardinality(),
	)
	if !route.useComplement {
		t.Fatalf("expected merged exact-range ordered route to use complement")
	}
	if route.cacheKey.isZero() || route.complementCacheKey.isZero() {
		t.Fatalf("expected both exact-range cache keys to be non-zero")
	}
	if route.cacheKey == route.complementCacheKey {
		t.Fatalf("expected complement promotion key to differ from positive-side cache key")
	}
	route.requirePromotion = true
	route.eagerMaterialize = true

	if view.orderedScalarRangeCanEagerMaterialize(route) {
		t.Fatalf("expected cold complement promotion gate to stay disabled")
	}
	if !db.getSnapshot().shouldPromoteRuntimeMaterializedPredKey(route.complementCacheKey) {
		t.Fatalf("expected warmed complement key to trigger promotion gate")
	}
	if !view.orderedScalarRangeCanEagerMaterialize(route) {
		t.Fatalf("expected ordered eager materialization to honor complement promotion key")
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
		got := materializedPredCacheKeyFromScalar("f", compileScalarOpForTest(c.op), "v")
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
