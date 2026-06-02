package qexec

import (
	"fmt"
	"testing"

	"github.com/vapstack/pooled"
	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/qir"
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
	if pred.postingFilter == nil && pred.fieldIndexRangeState == nil {
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

func TestExecPlanLeadScanNoOrderUsesSelectedLead(t *testing.T) {
	narrow := posting.BuildFromSorted([]uint64{3})
	wide := posting.BuildFromSorted([]uint64{1, 2, 3, 4, 5})
	defer narrow.Release()
	defer wide.Release()

	preds := []predicate{
		{
			expr:     qir.Expr{Op: qir.OpEQ},
			contains: narrow.Contains,
			iter:     narrow.Iter,
			estCard:  1,
		},
		{
			expr:     qir.Expr{Op: qir.OpEQ},
			contains: wide.Contains,
			iter:     wide.Iter,
			estCard:  5,
		},
	}

	var qv View
	q := qir.Shape{Limit: 1}
	var trace Trace
	out := qv.execPlanLeadScanNoOrder(&q, preds, 1, &trace)
	if len(out) != 1 || out[0] != 3 {
		t.Fatalf("unexpected output: %v", out)
	}
	if trace.RowsExamined() != 5 {
		t.Fatalf("expected selected lead to drive scan, RowsExamined=%d", trace.RowsExamined())
	}
}

func TestExecPlanLeadScanNoOrderMergedRangeLeadUsesEffectiveBounds(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	for age := 11; age <= 20; age++ {
		if err := db.Set(uint64(age), &Rec{
			Meta:  Meta{Country: "US"},
			Age:   age,
			Score: float64(age),
		}); err != nil {
			t.Fatalf("Set(%d): %v", age, err)
		}
	}

	q := qx.Query(
		qx.GT("age", 10),
		qx.EQ("age", 15),
		qx.EQ("country", "US"),
	).Limit(8)
	prepared, shape, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer prepared.Release()

	var leavesBuf [plannerPredicateFastPathMaxLeaves]qir.Expr
	leaves, ok := qir.CollectAndLeavesScratch(shape.Expr, leavesBuf[:0], qir.LeafModeCollect)
	if !ok {
		t.Fatalf("CollectAndLeavesScratch: ok=false")
	}

	view := db.engine.currentQueryViewForTests()
	preds, ok := view.buildPredicates(leaves)
	if !ok {
		t.Fatalf("buildPredicates: ok=false")
	}
	defer preds.Release()

	leadIdx := -1
	for i := 0; i < preds.Len(); i++ {
		if preds.owner[i].hasEffectiveBounds && preds.owner[i].expr.FieldOrdinal == shape.Expr.Operands[0].FieldOrdinal {
			leadIdx = i
			break
		}
	}
	if leadIdx < 0 {
		t.Fatalf("expected merged range lead predicate")
	}

	got := view.execPlanLeadScanNoOrder(&shape, preds.owner, leadIdx, nil)
	if len(got) != 1 || got[0] != 15 {
		t.Fatalf("execPlanLeadScanNoOrder returned %v, want [15]", got)
	}
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
	p := predicate{
		fieldIndexRangeState: &fieldIndexRangePredicateState{},
		postingFilterCheap:   false,
	}
	if !p.postingFilterCapability().supportsApply() {
		t.Fatalf("overlay range state lost posting-filter capability when filter is not cheap")
	}
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
	if !p.postingFilterCapability().supportsApply() {
		t.Fatalf("rangeMat predicate lost posting-filter capability")
	}
	if !p.postingFilterCapability().supportsExactBucket() {
		t.Fatalf("rangeMat predicate lost exact bucket posting-filter capability")
	}
}

func TestPredicateSupportsExactBucketPostingFilter_RangeStatesCheapOnly(t *testing.T) {
	p := predicate{fieldIndexRangeState: &fieldIndexRangePredicateState{}}
	if p.postingFilterCapability().supportsExactBucket() {
		t.Fatalf("non-cheap overlay range state must stay out of exact bucket posting-filter path")
	}
	p.postingFilterCheap = true
	if !p.postingFilterCapability().supportsExactBucket() {
		t.Fatalf("cheap overlay range state must enter exact bucket posting-filter path")
	}
}

func TestPredicateSupportsCheapPostingFilter_UsesUnifiedCapability(t *testing.T) {
	t.Run("FieldIndexRangeNonCheap", func(t *testing.T) {
		p := predicate{fieldIndexRangeState: &fieldIndexRangePredicateState{}}
		if p.postingFilterCapability().isCheap() {
			t.Fatalf("non-cheap overlay range state must stay out of cheap posting-filter class")
		}
	})

	t.Run("FieldIndexRangeCheap", func(t *testing.T) {
		p := predicate{
			fieldIndexRangeState: &fieldIndexRangePredicateState{},
			postingFilterCheap:   true,
		}
		if !p.postingFilterCapability().isCheap() {
			t.Fatalf("cheap overlay range state must stay in cheap posting-filter class")
		}
	})

	t.Run("PostsAnyExactOnly", func(t *testing.T) {
		p := predicate{kind: predicateKindPostsAny, postsAnyState: &postsAnyFilterState{}}
		if p.postingFilterCapability().supportsApply() {
			t.Fatalf("posts-any exact path must not enter generic posting-filter set")
		}
		if !p.postingFilterCapability().supportsExactBucket() {
			t.Fatalf("posts-any exact path lost exact bucket posting-filter capability")
		}
		if !p.postingFilterCapability().prefersExactBucket() {
			t.Fatalf("posts-any exact path must stay exact-preferred")
		}
		if !p.postingFilterCapability().isCheap() {
			t.Fatalf("posts-any exact path must stay in cheap posting-filter class")
		}
	})

	t.Run("PostsAnyNotRuntime", func(t *testing.T) {
		p := predicate{kind: predicateKindPostsAnyNot, postsAnyState: &postsAnyFilterState{}}
		if !p.postingFilterCapability().supportsApply() {
			t.Fatalf("posts-any-not runtime path lost posting-filter capability")
		}
		if !p.postingFilterCapability().supportsExactBucket() {
			t.Fatalf("posts-any-not runtime path lost exact bucket posting-filter capability")
		}
		if !p.postingFilterCapability().isCheap() {
			t.Fatalf("posts-any-not runtime path must stay in cheap posting-filter class")
		}
		if p.postingFilterCapability().prefersExactBucket() {
			t.Fatalf("posts-any-not runtime path must not force preferred exact routing")
		}
	})
}

func TestPredicatePostsAnyNotRuntimeApplyToPosting(t *testing.T) {
	posts := posting.GetSlice(3)
	posts = append(posts,
		posting.BuildFromSorted([]uint64{2, 4, 6}),
		posting.BuildFromSorted([]uint64{1, 4, 7}),
		posting.BuildFromSorted([]uint64{3, 5, 7}),
	)
	defer posting.ReleaseSlice(posts)
	defer func() {
		for i := 0; i < len(posts); i++ {
			posts[i].Release()
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
	if predicateLeadIterMayDuplicate(predicate{fieldIndexRangeState: &fieldIndexRangePredicateState{}}) {
		t.Fatalf("overlay range iterator should stay unique for ordered-anchor lead scans")
	}
}

func TestIndexViewRangeProbe_CountBucket_ComplementProbe(t *testing.T) {
	probeIDs := posting.BuildFromSorted([]uint64{2, 4})
	defer probeIDs.Release()
	bucket := posting.BuildFromSorted([]uint64{1, 2, 3, 4, 5})
	defer bucket.Release()

	probe := fieldIndexRangeProbe{
		ov:            indexdata.NewFieldIndexView(&[]indexdata.Entry{{IDs: probeIDs.Borrow()}}),
		spanCnt:       1,
		useComplement: true,
		probeLen:      1,
		probeEst:      2,
	}
	probe.spans[0] = indexdata.FieldIndexRange{BaseStart: 0, BaseEnd: 1}

	in, ok := probe.countBucket(bucket.Borrow())
	if !ok {
		t.Fatalf("expected overlay complement probe to support countBucket")
	}
	if in != 3 {
		t.Fatalf("countBucket = %d, want 3", in)
	}
}

func TestIndexViewRangePredicateState_CountBucket_ComplementProbe(t *testing.T) {
	probeIDs := posting.BuildFromSorted([]uint64{2, 4})
	defer probeIDs.Release()
	bucket := posting.BuildFromSorted([]uint64{1, 2, 3, 4, 5})
	defer bucket.Release()

	state := &fieldIndexRangePredicateState{
		probe: fieldIndexRangeProbe{
			ov:            indexdata.NewFieldIndexView(&[]indexdata.Entry{{IDs: probeIDs.Borrow()}}),
			spanCnt:       1,
			useComplement: true,
			probeLen:      1,
			probeEst:      2,
		},
	}
	state.probe.spans[0] = indexdata.FieldIndexRange{BaseStart: 0, BaseEnd: 1}

	in, ok := state.countBucket(bucket.Borrow())
	if !ok {
		t.Fatalf("expected positive complement-backed range count")
	}
	if in != 3 {
		t.Fatalf("positive countBucket = %d, want 3", in)
	}

	state.neg = true
	in, ok = state.countBucket(bucket.Borrow())
	if !ok {
		t.Fatalf("expected negative complement-backed range count")
	}
	if in != 2 {
		t.Fatalf("negative countBucket = %d, want 2", in)
	}
}

func TestIndexViewRangePredicateState_Matches_MaterializedComplementProbe(t *testing.T) {
	state := &fieldIndexRangePredicateState{
		probe: fieldIndexRangeProbe{
			useComplement: true,
		},
		keepProbeHits:     false,
		probeMaterialized: true,
		probeIDs:          posting.BuildFromSorted([]uint64{2, 4}),
	}
	defer fieldIndexRangePredicateStatePool.Put(state)

	if state.matches(2) {
		t.Fatalf("complement probe hit must be excluded from positive broad range")
	}
	if !state.matches(3) {
		t.Fatalf("non-probe id must stay matched for positive broad range")
	}
}

func TestPredicateSetExpectedContainsCalls_UpdatesFieldIndexRangeState(t *testing.T) {
	state := fieldIndexRangePredicateStatePool.Get()
	state.ov = indexdata.NewFieldIndexView(&[]indexdata.Entry{{Key: keycodec.FromString("a")}, {Key: keycodec.FromString("b")}, {Key: keycodec.FromString("c")}})
	state.br = indexdata.FieldIndexRange{BaseStart: 0, BaseEnd: 3}
	state.probe = fieldIndexRangeProbe{
		ov:       indexdata.NewFieldIndexView(&[]indexdata.Entry{{Key: keycodec.FromString("a")}, {Key: keycodec.FromString("b")}, {Key: keycodec.FromString("c")}}),
		spans:    [2]indexdata.FieldIndexRange{{BaseStart: 0, BaseEnd: 3}},
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
	defer fieldIndexRangePredicateStatePool.Put(state)

	p := predicate{fieldIndexRangeState: state}
	p.setExpectedContainsCalls(1)
	if state.expectedContainsCalls != 1 {
		t.Fatalf("expected overlay expectedContainsCalls=1, got %d", state.expectedContainsCalls)
	}
	if state.containsMode != rangeContainsLinear {
		t.Fatalf("expected overlay contains mode to stay linear for single call, got %v", state.containsMode)
	}
}

func TestIndexViewRangePredicateState_LinearContains_NonLinearModeSkipsLinearPosts(t *testing.T) {
	base := []indexdata.Entry{
		{Key: keycodec.FromString("a"), IDs: posting.BuildFromSorted([]uint64{1, 2})},
		{Key: keycodec.FromString("b"), IDs: posting.BuildFromSorted([]uint64{3, 4})},
	}
	state := fieldIndexRangePredicateStatePool.Get()
	state.ov = indexdata.NewFieldIndexView(&base)
	state.probe = fieldIndexRangeProbe{
		ov:       indexdata.NewFieldIndexView(&base),
		spans:    [2]indexdata.FieldIndexRange{{BaseStart: 0, BaseEnd: 2}},
		spanCnt:  1,
		probeLen: 2,
		probeEst: 4,
	}
	state.bucketCount = 2
	state.expectedContainsCalls = 8
	state.containsMode = rangeContainsPosting
	defer fieldIndexRangePredicateStatePool.Put(state)

	if !state.linearContains(3) {
		t.Fatalf("expected overlay linearContains to probe-match idx 3")
	}
	if state.linearPostsBuf != nil {
		t.Fatalf("overlay linearContains must not build linear posts outside linear mode")
	}
}

func TestAcquireFieldIndexRangePredicateState_PositiveDirectProbeKeepsProbeHitsWithoutPostingFilter(t *testing.T) {
	state := fieldIndexRangePredicateStatePool.Get()
	state.ov = indexdata.NewFieldIndexView(&[]indexdata.Entry{{Key: keycodec.FromString("a")}, {Key: keycodec.FromString("b")}})
	state.br = indexdata.FieldIndexRange{BaseStart: 0, BaseEnd: 1}
	state.probe = fieldIndexRangeProbe{
		ov:       indexdata.NewFieldIndexView(&[]indexdata.Entry{{Key: keycodec.FromString("a")}, {Key: keycodec.FromString("b")}}),
		spans:    [2]indexdata.FieldIndexRange{{BaseStart: 0, BaseEnd: 1}},
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
	defer fieldIndexRangePredicateStatePool.Put(state)

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

	expr := qx.PREFIX("email", "user1")
	cacheKey := db.engine.materializedPredCacheKey(expr)
	if cacheKey == "" {
		t.Fatalf("expected non-empty materialized cache key for prefix predicate")
	}
	if _, ok := snapshotExtLoadMaterializedPred(db.engine.snapshot.Current(), cacheKey); ok {
		t.Fatalf("unexpected cache hit before predicate evaluation")
	}

	fm := db.engine.schema.Fields["email"]
	if fm == nil {
		t.Fatalf("expected field metadata for email")
	}
	view := db.engine.currentQueryViewForTests()
	ov := view.fieldIndexViewFromSlotsByName(view.snap.Index, "email")
	if !ov.HasData() {
		t.Fatalf("expected field index data for email")
	}

	p, ok := db.engine.buildPredRangeCandidateWithMode(expr, fm, ov, true)
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

	cached, ok := snapshotExtLoadMaterializedPred(db.engine.snapshot.Current(), cacheKey)
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

	expr := qx.PREFIX("email", "user1")
	if cacheKey := db.engine.materializedPredCacheKey(expr); cacheKey != "" {
		t.Fatalf("expected empty materialized cache key when cache is disabled, got %q", cacheKey)
	}

	fm := db.engine.schema.Fields["email"]
	if fm == nil {
		t.Fatalf("expected field metadata for email")
	}
	view := db.engine.currentQueryViewForTests()
	ov := view.fieldIndexViewFromSlotsByName(view.snap.Index, "email")
	if !ov.HasData() {
		t.Fatalf("expected field index data for email")
	}

	p, ok := db.engine.buildPredRangeCandidateWithMode(expr, fm, ov, true)
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
	parsedKey, ok := qcache.MaterializedPredKeyFromEncoded(rawKey)
	if !ok {
		t.Fatalf("expected parseable materialized cache key %q", rawKey)
	}
	if db.engine.snapshot.Current().HasMaterializedPredKey(parsedKey) {
		t.Fatalf("expected no cache store when materialized predicate cache is disabled")
	}
	if cache := db.engine.snapshot.Current().MaterializedPredCache(); cache != nil {
		if got := cache.EntryCount(); got != 0 {
			t.Fatalf("expected zero materialized cache entries, got %d", got)
		}
	}
}

func TestBuildPredRange_NumericBucketsRunWhenPredicateCacheDisabled(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: -1,
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
			Active: true,
		}
	})

	view := db.engine.currentQueryViewForTests()
	expr := mustTestQIRExprForDB(t, db, qx.LT("age", 11_000))
	fm := view.fieldMetaByExpr(expr)
	if fm == nil {
		t.Fatal("expected field metadata")
	}
	ov := view.fieldIndexViewFromSlotsForExpr(view.snap.Index, expr)
	if !ov.HasData() {
		t.Fatal("expected index view data")
	}
	candidate, ok := view.prepareScalarRangeRoutingCandidate(expr)
	if !ok {
		t.Fatal("prepareScalarRangeRoutingCandidate: ok=false")
	}
	if !candidate.plan.useComplement {
		t.Fatal("expected broad numeric range to use complement probing when it stays lazy")
	}

	p, ok := view.buildPredRangeCandidateWithMode(expr, fm, ov, true)
	if !ok {
		t.Fatal("buildPredRangeCandidateWithMode: ok=false")
	}
	defer releasePredicates([]predicate{p})
	if !p.isMaterializedLike() {
		t.Fatalf("expected numeric bucket path to materialize current-query predicate, kind=%v", p.kind)
	}
	if p.fieldIndexRangeState != nil {
		t.Fatal("expected numeric bucket path instead of repeated runtime range probes")
	}
	if !p.matches(1) || !p.matches(10_999) || p.matches(11_000) {
		t.Fatal("numeric bucket materialized predicate returned wrong matches")
	}
	if view.snap.MaterializedPredCache() != nil {
		t.Fatal("materialized predicate cache must stay disabled")
	}
	if view.snap.NumericRangeBucketCache().EntryCount() == 0 {
		t.Fatal("expected numeric range bucket cache entry")
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

	p, ok := db.engine.buildPredicateWithMode(qx.PREFIX("email", "user"), false)
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
	view := db.engine.currentQueryViewForTests()
	if ov := view.fieldIndexViewFromSlotsByName(view.snap.Index, "age"); ov.IsChunked() {
		t.Fatalf("expected base numeric index path for age, got chunked overlay")
	}

	expr := qx.NOT(qx.GTE("age", 30))
	predExpected, ok := db.engine.buildPredicateWithMode(expr, false)
	if !ok {
		t.Fatal("expected base numeric predicate to build")
	}
	defer releasePredicates([]predicate{predExpected})

	predFilter, ok := db.engine.buildPredicateWithMode(expr, false)
	if !ok {
		t.Fatal("expected base numeric predicate to build for postingFilter")
	}
	defer releasePredicates([]predicate{predFilter})

	snap := db.engine.snapshot.Current()
	want := predicateMatchIDs(predExpected, snap.Universe)
	got := runPredicatePostingFilter(t, predFilter, snap.Universe)
	defer got.Release()
	assertPostingConsumerSet(t, got, want)
}

func TestBuildPredRange_FieldIndexNumericPostingFilter_NotComplementMaterializedFallback(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   i,
			Score: float64(i),
		}
	})
	view := db.engine.currentQueryViewForTests()
	if ov := view.fieldIndexViewFromSlotsByName(view.snap.Index, "age"); !ov.IsChunked() {
		t.Fatalf("expected chunked overlay path for age")
	}

	expr := qx.NOT(qx.GTE("age", 30))
	predExpected, ok := db.engine.buildPredicateWithMode(expr, false)
	if !ok {
		t.Fatal("expected overlay numeric predicate to build")
	}
	defer releasePredicates([]predicate{predExpected})

	predFilter, ok := db.engine.buildPredicateWithMode(expr, false)
	if !ok {
		t.Fatal("expected overlay numeric predicate to build for postingFilter")
	}
	defer releasePredicates([]predicate{predFilter})

	snap := db.engine.snapshot.Current()
	want := predicateMatchIDs(predExpected, snap.Universe)
	got := runPredicatePostingFilter(t, predFilter, snap.Universe)
	defer got.Release()
	assertPostingConsumerSet(t, got, want)
}

func TestBuildPredRange_FieldIndexNumericPostingFilter_ComplementMaterializedFallback(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   i,
			Score: float64(i),
		}
	})
	fm := db.engine.schema.Fields["age"]
	if fm == nil {
		t.Fatalf("expected field metadata for age")
	}
	view := db.engine.currentQueryViewForTests()
	ov := view.fieldIndexViewFromSlotsByName(view.snap.Index, "age")
	if !ov.IsChunked() {
		t.Fatalf("expected chunked overlay path for age")
	}

	expr := qx.GTE("age", 1_000)
	bound, isSlice, err := db.engine.currentQueryViewForTests().exprValueToNormalizedScalarBound(mustTestQIRExprForDB(t, db, expr))
	if err != nil {
		t.Fatalf("exprValueToNormalizedScalarBound: %v", err)
	}
	if isSlice {
		t.Fatalf("unexpected slice bound for %v", expr)
	}
	rb := indexdata.Bounds{Has: true}
	applyNormalizedScalarBound(&rb, bound)
	br := ov.RangeForBounds(rb)
	bucketCount, _ := ov.RangeStats(br)
	totalBuckets := ov.KeyCount()
	if totalBuckets-bucketCount >= bucketCount {
		t.Fatalf("expected complement probe, buckets=%d total=%d", bucketCount, totalBuckets)
	}

	predExpected, ok := db.engine.buildPredicateWithMode(expr, false)
	if !ok {
		t.Fatal("expected overlay numeric predicate to build")
	}
	defer releasePredicates([]predicate{predExpected})

	predFilter, ok := db.engine.buildPredicateWithMode(expr, false)
	if !ok {
		t.Fatal("expected overlay numeric predicate to build for postingFilter")
	}
	defer releasePredicates([]predicate{predFilter})

	snap := db.engine.snapshot.Current()
	want := predicateMatchIDs(predExpected, snap.Universe)
	got := runPredicatePostingFilter(t, predFilter, snap.Universe)
	defer got.Release()
	assertPostingConsumerSet(t, got, want)
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

	p, ok := db.engine.buildPredicateWithMode(qx.GT("score", 4.0), false)
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

	expr := qx.GT("score", 4.0)
	p, ok := db.engine.buildPredicateWithMode(expr, false)
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

	wantCount := cardinalityByExprBitmap(t, db, expr)
	if got.Cardinality() != wantCount {
		t.Fatalf("range predicate mismatch: got=%d want=%d", got.Cardinality(), wantCount)
	}
}

func TestIndexViewRangeIter_AllocsPerRunStayZeroAfterWarmup(t *testing.T) {
	if testRaceEnabled {
		t.Skip("testing.AllocsPerRun is not stable under -race")
	}

	buckets := []indexdata.Entry{
		{IDs: posting.BuildFromSorted([]uint64{1, 3, 5})},
		{IDs: posting.BuildFromSorted([]uint64{7})},
		{IDs: posting.BuildFromSorted([]uint64{9, 11})},
	}
	defer func() {
		for i := range buckets {
			buckets[i].IDs.Release()
		}
	}()
	ov := indexdata.NewFieldIndexView(&buckets)
	br := ov.RangeByRanks(0, ov.KeyCount())

	run := func() {
		it := fieldIndexRangeIterPool.Get()
		it.cur = ov.NewCursor(br, false)
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

	expr := qx.LT("age", 6_000)
	p, ok := db.engine.currentQueryViewForTests().buildPredicateWithColdMode(mustTestQIRExprForDB(t, db, expr), true, true)
	if !ok {
		t.Fatalf("expected predicate build to succeed")
	}
	defer releasePredicates([]predicate{p})

	if p.fieldIndexRangeState == nil {
		t.Fatalf("expected cold numeric range to stay on overlay runtime state")
	}
	if p.fieldIndexRangeState.probeMaterializeAt <= 1 {
		t.Fatalf("expected runtime posting filter to defer materialization, got probeMaterializeAt=%d", p.fieldIndexRangeState.probeMaterializeAt)
	}
	if p.kind == predicateKindMaterialized || p.kind == predicateKindMaterializedNot {
		t.Fatalf("unexpected eager materialized predicate kind=%v", p.kind)
	}
	if got := db.engine.snapshot.Current().MaterializedPredCache().EntryCount(); got != 0 {
		t.Fatalf("unexpected shared materialized predicate cache entry: %d", got)
	}
}

func TestBuildPredicates_DefaultBuildKeepsColdNumericRangesLazy(t *testing.T) {
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

	preds, ok := db.engine.buildPredicates([]qx.Expr{
		qx.EQ("active", true),
		qx.GTE("age", 6_000),
		qx.GTE("score", 6_000.0),
	})
	if !ok {
		t.Fatalf("buildPredicates: ok=false")
	}
	defer releasePredicates(preds)

	rangeStates := 0
	for i := range preds {
		p := preds[i]
		if p.expr.Op.IsNumericRange() {
			if p.fieldIndexRangeState == nil {
				t.Fatalf("expected numeric range predicate to stay lazy: %+v", p.expr)
			}
			if p.isMaterializedLike() {
				t.Fatalf("unexpected eager materialized numeric range: %+v", p.expr)
			}
			rangeStates++
		}
	}
	if rangeStates != 2 {
		t.Fatalf("range predicate count=%d want=2", rangeStates)
	}
	if got := db.engine.snapshot.Current().MaterializedPredCache().EntryCount(); got != 0 {
		t.Fatalf("unexpected shared materialized predicate cache entry: %d", got)
	}
}

func TestBuildPredicateWithMode_RuntimeNumericRangeMaterializationKeepsScalarCacheLocal(t *testing.T) {
	t.Run("FieldIndexRangeState", func(t *testing.T) {
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

		expr := qx.GT("age", 11_000)
		cacheKey := db.engine.materializedPredCacheKey(expr)
		p, ok := db.engine.buildPredicateWithMode(expr, false)
		if !ok {
			t.Fatalf("expected predicate build to succeed")
		}
		defer releasePredicates([]predicate{p})

		if p.fieldIndexRangeState == nil {
			t.Fatalf("expected chunked numeric range to stay on overlay runtime state")
		}
		if got := db.engine.snapshot.Current().MaterializedPredCache().EntryCount(); got != 0 {
			t.Fatalf("unexpected shared materialized predicate cache before runtime materialization: %d", got)
		}
		matchCalls := max(128, p.fieldIndexRangeState.materializeAfter+1)
		for i := 1; i <= matchCalls; i++ {
			_ = p.matches(uint64(i))
		}
		if !p.fieldIndexRangeState.rangeMaterialized && !p.fieldIndexRangeState.probeMaterialized {
			t.Fatalf("expected runtime overlay state to build local membership state")
		}
		if p.fieldIndexRangeState.rangeMaterialized && p.fieldIndexRangeState.rangeIDs.IsEmpty() {
			t.Fatalf("expected locally materialized overlay range ids")
		}
		if p.fieldIndexRangeState.probeMaterialized && p.fieldIndexRangeState.probeIDs.IsEmpty() {
			t.Fatalf("expected locally materialized overlay probe ids")
		}
		if got := db.engine.snapshot.Current().MaterializedPredCache().EntryCount(); got != 0 {
			t.Fatalf("unexpected shared scalar cache entry from runtime overlay materialization: %d", got)
		}
		if cached, ok := snapshotExtLoadMaterializedPred(db.engine.snapshot.Current(), cacheKey); ok && !cached.IsEmpty() {
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

		expr := qx.GT("score", 220.0)
		cacheKey := db.engine.materializedPredCacheKey(expr)
		p, ok := db.engine.buildPredicateWithMode(expr, false)
		if !ok {
			t.Fatalf("expected predicate build to succeed")
		}
		defer releasePredicates([]predicate{p})

		if p.fieldIndexRangeState == nil {
			t.Fatalf("expected numeric range to stay on runtime overlay state")
		}
		if got := db.engine.snapshot.Current().MaterializedPredCache().EntryCount(); got != 0 {
			t.Fatalf("unexpected shared materialized predicate cache before runtime materialization: %d", got)
		}
		for i := 1; i <= 128; i++ {
			_ = p.matches(uint64(i))
		}
		if got := db.engine.snapshot.Current().MaterializedPredCache().EntryCount(); got != 0 {
			t.Fatalf("unexpected shared scalar cache entry from runtime overlay materialization: %d", got)
		}
		if cached, ok := snapshotExtLoadMaterializedPred(db.engine.snapshot.Current(), cacheKey); ok && !cached.IsEmpty() {
			t.Fatalf("unexpected shared scalar cache entry from runtime overlay materialization")
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

	expr := qx.GTE("age", 40)
	view := db.engine.currentQueryViewForTests()
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

	p, ok := db.engine.buildPredicateWithMode(expr, false)
	if !ok {
		t.Fatalf("expected predicate build to succeed")
	}
	defer releasePredicates([]predicate{p})

	if p.fieldIndexRangeState == nil {
		t.Fatalf("expected numeric range to stay on runtime overlay state")
	}
	if !p.fieldIndexRangeState.probe.useComplement {
		t.Fatalf("expected broad positive range to use complement probe")
	}
	if got := db.engine.snapshot.Current().MaterializedPredCache().EntryCount(); got != 0 {
		t.Fatalf("unexpected shared materialized predicate cache before runtime materialization: %d", got)
	}

	for i := 1; i <= 160; i++ {
		_ = p.matches(uint64(i))
	}

	if got := db.engine.snapshot.Current().MaterializedPredCache().EntryCount(); got != 0 {
		t.Fatalf("expected complement-backed runtime matches to stay local, cacheCount=%d", got)
	}
	if _, ok := snapshotExtLoadMaterializedPred(db.engine.snapshot.Current(), fullCacheKey); ok {
		t.Fatalf("unexpected positive scalar cache entry for complement-backed runtime state")
	}
	if _, ok := snapshotExtLoadMaterializedPred(db.engine.snapshot.Current(), complementCacheKey); ok {
		t.Fatalf("unexpected shared complement cache entry from runtime overlay materialization")
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

	view := db.engine.currentQueryViewForTests()
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
	if p.fieldIndexRangeState == nil || !p.fieldIndexRangeState.probe.useComplement {
		t.Fatalf("expected broad positive overlay range state with complement probe")
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

	if got := db.engine.snapshot.Current().MaterializedPredCache().EntryCount(); got != 0 {
		t.Fatalf("expected complement-backed posting filters to stay local, cacheCount=%d", got)
	}
	if _, ok := snapshotExtLoadMaterializedPred(db.engine.snapshot.Current(), complementCacheKey); ok {
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

	view := db.engine.currentQueryViewForTests()

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

	preds1, ok := db.engine.buildPredicatesOrderedWithMode([]qx.Expr{expr}, "score", false, 4096, 0, false, true)
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
	if _, ok := snapshotExtLoadMaterializedPred(db.engine.snapshot.Current(), cacheKey); !ok {
		t.Fatalf("expected shared complement cache entry after first ordered build")
	}

	preds2, ok := db.engine.buildPredicatesOrderedWithMode([]qx.Expr{expr}, "score", false, 4096, 0, false, true)
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
	if _, ok := snapshotExtLoadMaterializedPred(db.engine.snapshot.Current(), cacheKey); !ok {
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

	view := db.engine.currentQueryViewForTests()

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

	warm, ok := db.engine.buildPredicatesOrderedWithMode([]qx.Expr{expr}, "score", false, 4096, 0, false, true)
	if !ok {
		t.Fatalf("warm buildPredicatesOrderedWithMode: ok=false")
	}
	releasePredicates(warm)
	if _, ok := snapshotExtLoadMaterializedPred(db.engine.snapshot.Current(), cacheKey); !ok {
		t.Fatalf("expected shared complement cache entry after warm build")
	}

	preds, ok := db.engine.buildPredicatesOrderedWithMode([]qx.Expr{expr}, "score", false, 4096, 0, false, false)
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

	view := db.engine.currentQueryViewForTests()

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

	warm, ok := db.engine.buildPredicatesOrderedWithMode([]qx.Expr{expr}, "score", false, 4096, 0, true, true)
	if !ok {
		t.Fatalf("warm buildPredicatesOrderedWithMode: ok=false")
	}
	releasePredicates(warm)
	if _, ok := snapshotExtLoadMaterializedPred(db.engine.snapshot.Current(), cacheKey); !ok {
		t.Fatalf("expected shared complement cache entry after warm build")
	}

	preds, ok := db.engine.buildPredicatesOrderedWithMode([]qx.Expr{expr}, "score", false, 4096, 0, true, false)
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

	view := db.engine.currentQueryViewForTests()

	leaves := []qx.Expr{qx.GTE("age", 30), qx.LTE("age", 250)}
	warm, ok := db.engine.buildPredicatesOrderedWithMode(leaves, "score", false, 4096, 0, false, false)
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
	if cacheKey.IsZero() {
		releasePredicates(warm)
		t.Fatalf("expected non-zero exact-range complement cache key")
	}
	if !view.materializeOrderedORPredicate(&warm[0]) {
		releasePredicates(warm)
		t.Fatalf("expected merged exact-range warm predicate to materialize")
	}
	releasePredicates(warm)
	cached, ok := db.engine.snapshot.Current().LoadMaterializedPredKey(cacheKey)
	if !ok || cached.IsEmpty() {
		t.Fatalf("expected shared merged exact-range complement cache entry after warm materialization")
	}
	cached.Release()

	preds, ok := db.engine.buildPredicatesOrderedWithMode(leaves, "score", false, 4096, 0, true, true)
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

func TestBuildPredicatesOrdered_MergesStringScalarRangeBounds(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	seedGeneratedUint64Data(t, db, 2_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%06d@example.com", i),
			Score:  float64(i),
			Active: true,
		}
	})

	leaves := []qx.Expr{
		qx.GTE("email", "user000400@example.com"),
		qx.LT("email", "user001600@example.com"),
	}
	preds, ok := db.engine.buildPredicatesOrderedWithMode(leaves, "score", false, 128, 0, true, true)
	if !ok {
		t.Fatalf("buildPredicatesOrderedWithMode: ok=false")
	}
	defer releasePredicates(preds)

	if len(preds) != 1 {
		t.Fatalf("expected merged string range predicate, got %d predicates", len(preds))
	}
	if !preds[0].hasEffectiveBounds {
		t.Fatalf("expected merged string range effective bounds")
	}
	if preds[0].fieldIndexRangeState == nil {
		t.Fatalf("expected merged string range to keep runtime range state")
	}
	if preds[0].isMaterializedLike() {
		t.Fatalf("expected cold merged string range to avoid eager materialization")
	}
}

func TestBuildPredicatesOrdered_MergedStringPrefixPreservesTightenedBounds(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	rows := [...]struct {
		email string
		score float64
	}{
		{"user1@example.com", 1},
		{"user8@example.com", 2},
		{"user9@example.com", 3},
		{"user90@example.com", 4},
		{"uses0@example.com", 5},
	}
	for i := range rows {
		if err := db.Set(uint64(i+1), &Rec{
			Name:   rows[i].email,
			Email:  rows[i].email,
			Score:  rows[i].score,
			Active: true,
		}); err != nil {
			t.Fatalf("Set(%d): %v", i+1, err)
		}
	}

	leaves := []qx.Expr{
		qx.GTE("email", "user"),
		qx.LT("email", "uses"),
		qx.GTE("email", "user9"),
	}
	preds, ok := db.engine.buildPredicatesOrderedWithMode(leaves, "score", false, 10, 0, true, true)
	if !ok {
		t.Fatalf("buildPredicatesOrderedWithMode: ok=false")
	}
	defer releasePredicates(preds)

	if len(preds) != 1 {
		t.Fatalf("expected merged string range predicate, got %d predicates", len(preds))
	}
	if !preds[0].matches(3) || !preds[0].matches(4) {
		t.Fatalf("tightened prefix range should match user9/user90 rows")
	}
	if preds[0].matches(1) || preds[0].matches(2) || preds[0].matches(5) {
		t.Fatalf("tightened prefix range matched rows outside the merged bounds")
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

	view := db.engine.currentQueryViewForTests()

	compiled := qir.GetExprSlice(2)
	defer qir.ReleaseExprSlice(compiled)
	compiled = append(compiled, mustTestQIRExprForDB(t, db, qx.GTE("age", 30)))
	compiled = append(compiled, mustTestQIRExprForDB(t, db, qx.LTE("age", 250)))

	warm, ok := view.buildPredicatesOrderedWithMode(compiled, "score", false, 4096, 0, false, false)
	if !ok {
		t.Fatalf("warm buildPredicatesOrderedWithMode: ok=false")
	}
	if warm.Len() != 1 {
		warm.Release()
		t.Fatalf("unexpected warm predicate count: %d", warm.Len())
	}
	warmPred := warm.owner[0]
	if !warmPred.hasEffectiveBounds {
		warm.Release()
		t.Fatalf("expected merged exact-range warm predicate")
	}
	cacheKey := view.materializedPredComplementKeyForExactScalarRange("age", warmPred.effectiveBounds)
	if cacheKey.IsZero() {
		warm.Release()
		t.Fatalf("expected non-zero exact-range complement cache key")
	}
	if !view.materializeOrderedORPredicate(&warm.owner[0]) {
		warm.Release()
		t.Fatalf("expected merged exact-range warm predicate to materialize")
	}
	warm.Release()
	cached, ok := db.engine.snapshot.Current().LoadMaterializedPredKey(cacheKey)
	if !ok || cached.IsEmpty() {
		t.Fatalf("expected shared merged exact-range complement cache entry after warm materialization")
	}
	cached.Release()

	preds, ok := view.buildPredicatesOrderedWithMode(compiled, "score", false, 4096, 0, true, true)
	if !ok {
		t.Fatalf("buildPredicatesOrderedWithMode: ok=false")
	}
	defer preds.Release()
	if len(preds.owner) != 1 {
		t.Fatalf("unexpected predicate count: %d", len(preds.owner))
	}
	p := preds.owner[0]
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

	view := db.engine.currentQueryViewForTests()

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

	preds, ok := db.engine.buildPredicatesOrderedWithMode([]qx.Expr{expr}, "score", false, 4096, 0, false, false)
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
	if _, ok := snapshotExtLoadMaterializedPred(db.engine.snapshot.Current(), cacheKey); ok {
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

	view := db.engine.currentQueryViewForTests()

	expr := qx.GTE("rank", 4)
	compiled := mustTestQIRExprForDB(t, db, expr)
	candidate, ok := view.prepareScalarRangeRoutingCandidate(compiled)
	if !ok {
		t.Fatalf("expected broad nullable range routing candidate")
	}
	if !candidate.plan.useComplement || !candidate.broadComplementCardinality(view.snap.Universe.Cardinality()) {
		t.Fatalf("expected broad complement route for nullable ordered predicate")
	}

	preds, ok := db.engine.buildPredicatesOrderedWithMode([]qx.Expr{expr}, "name", false, 4096, 0, false, false)
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

	view := db.engine.currentQueryViewForTests()

	expr := qx.GTE("rank", 1)
	compiled := mustTestQIRExprForDB(t, db, expr)
	candidate, ok := view.prepareScalarRangeRoutingCandidate(compiled)
	if !ok {
		t.Fatalf("expected nullable range routing candidate")
	}
	if !candidate.plan.useComplement {
		t.Fatalf("expected complement route for sparse nullable ordered predicate")
	}
	if candidate.broadComplementCardinality(view.snap.Universe.Cardinality()) {
		t.Fatalf("expected non-broad complement route for sparse nullable ordered predicate")
	}

	preds, ok := db.engine.buildPredicatesOrderedWithMode([]qx.Expr{expr}, "name", false, 4096, 0, false, false)
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

	view := db.engine.currentQueryViewForTests()

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
	if cacheKey.IsZero() {
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

	cached, ok := db.engine.snapshot.Current().LoadMaterializedPredKey(cacheKey)
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

	view := db.engine.currentQueryViewForTests()

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
	if wantKey.IsZero() {
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

	view := db.engine.currentQueryViewForTests()

	preds, ok := db.engine.buildPredicatesOrderedWithMode(
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
	if wantKey.IsZero() {
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

	view := db.engine.currentQueryViewForTests()

	preds, ok := db.engine.buildPredicatesOrderedWithMode(
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
	if wantKey.IsZero() {
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

	view := db.engine.currentQueryViewForTests()

	leaves := []qx.Expr{qx.GTE("age", 30), qx.LTE("age", 250)}
	warm, ok := db.engine.buildPredicatesOrderedWithMode(leaves, "score", false, 4096, 0, false, false)
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
	if cacheKey.IsZero() {
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
	cached, ok := db.engine.snapshot.Current().LoadMaterializedPredKey(cacheKey)
	if !ok || cached.IsEmpty() {
		t.Fatalf("expected shared merged exact-range complement cache entry after warm materialization")
	}
	cached.Release()

	preds, ok := db.engine.buildPredicatesOrderedWithMode(leaves, "score", false, 4096, 0, false, false)
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

	view := db.engine.currentQueryViewForTests()

	leaves := []qx.Expr{qx.GTE("rank", 1), qx.LTE("rank", 9)}
	warm, ok := db.engine.buildPredicatesOrderedWithMode(leaves, "name", false, 4096, 0, false, false)
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
	if cacheKey.IsZero() {
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

	cached, ok := db.engine.snapshot.Current().LoadMaterializedPredKey(cacheKey)
	if !ok || cached.IsEmpty() {
		t.Fatalf("expected shared merged exact-range complement cache entry after warm materialization")
	}
	cached.Release()

	preds, ok := db.engine.buildPredicatesOrderedWithMode(leaves, "name", false, 4096, 0, false, false)
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

	view := db.engine.currentQueryViewForTests()

	leaves := []qx.Expr{qx.GTE("age", 30), qx.LTE("age", 250)}
	warm, ok := db.engine.buildPredicatesOrderedWithMode(leaves, "score", false, 4096, 0, true, false)
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
	if cacheKey.IsZero() {
		releasePredicates(warm)
		t.Fatalf("expected non-zero exact-range complement cache key")
	}
	if !view.materializeOrderedORPredicate(&warm[0]) {
		releasePredicates(warm)
		t.Fatalf("expected merged exact-range warm predicate to materialize")
	}
	releasePredicates(warm)
	cached, ok := db.engine.snapshot.Current().LoadMaterializedPredKey(cacheKey)
	if !ok || cached.IsEmpty() {
		t.Fatalf("expected shared merged exact-range complement cache entry after warm materialization")
	}
	cached.Release()

	preds, ok := db.engine.buildPredicatesOrderedWithMode(leaves, "score", false, 4096, 0, true, false)
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

	view := db.engine.currentQueryViewForTests()

	expr := mustTestQIRExprForDB(t, db, qx.PREFIX("email", "user"))
	candidate, ok := view.prepareScalarRangeRoutingCandidate(expr)
	if !ok {
		t.Fatal("expected broad prefix routing candidate")
	}
	if !candidate.plan.useComplement {
		t.Fatal("expected broad prefix candidate to choose complement probe")
	}

	cacheKey, buildWork, ok := view.orderedORMaterializedPrefixLeafBuildWork("score", expr)
	if ok || buildWork != 0 || !cacheKey.IsZero() {
		t.Fatalf("expected broad prefix complement candidate to be rejected, got ok=%v build=%d key=%q", ok, buildWork, cacheKey.String())
	}
}

func TestBuildPredicateWithMode_RuntimeExactUnionPromotesOnSecondMaterialize(t *testing.T) {
	run := func(
		t *testing.T,
		expr qx.Expr,
		wantKind predicateKind,
		cacheKey qcache.MaterializedPredKey,
		seed func(*DB[uint64, Rec]),
	) {
		db, _ := openTempDBUint64(t, Options{
			AnalyzeInterval:                         -1,
			SnapshotMaterializedPredCacheMaxEntries: 16,
		})
		seed(db)

		materialize := func() predicate {
			p, ok := db.engine.buildPredicateWithMode(expr, false)
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
		if db.engine.snapshot.Current().HasMaterializedPredKey(cacheKey) {
			releasePredicates([]predicate{first})
			t.Fatalf("unexpected shared exact-union cache entry after first materialize")
		}
		releasePredicates([]predicate{first})

		second := materialize()
		releasePredicates([]predicate{second})

		cached, ok := db.engine.snapshot.Current().LoadMaterializedPredKey(cacheKey)
		if !ok || cached.IsEmpty() {
			t.Fatalf("expected shared exact-union cache entry after second materialize")
		}
		defer cached.Release()

		third, ok := db.engine.buildPredicateWithMode(expr, false)
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
		vals := pooled.GetStringSlice(2)
		vals = append(vals, "DE", "FR")
		cacheKey := qcache.MaterializedPredKeyForDistinctSetTerms("country", compileScalarOpForTest(qx.OpIN), vals, false)
		pooled.ReleaseStringSlice(vals)

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
		vals := pooled.GetStringSlice(2)
		vals = append(vals, "db", "go")
		cacheKey := qcache.MaterializedPredKeyForDistinctSetTerms("tags", compileScalarOpForTest(qx.OpHASANY), vals, false)
		pooled.ReleaseStringSlice(vals)

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
		vals := pooled.GetStringSlice(2)
		vals = append(vals, "DE", "FR")
		cacheKey := qcache.MaterializedPredKeyForDistinctSetTerms("country", compileScalarOpForTest(qx.OpIN), vals, false)
		pooled.ReleaseStringSlice(vals)

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
		vals := pooled.GetStringSlice(2)
		vals = append(vals, "db", "go")
		cacheKey := qcache.MaterializedPredKeyForDistinctSetTerms("tags", compileScalarOpForTest(qx.OpHASANY), vals, false)
		pooled.ReleaseStringSlice(vals)

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

	vals := pooled.GetStringSlice(2)
	vals = append(vals, "db", "go")
	cacheKey := qcache.MaterializedPredKeyForDistinctSetTerms("tags", compileScalarOpForTest(qx.OpHASALL), vals, false)
	pooled.ReleaseStringSlice(vals)

	expr := qx.HASALL("tags", []string{"go", "db"})

	first, ok := db.engine.buildPredicateWithMode(expr, false)
	if !ok {
		t.Fatalf("expected first predicate build to succeed")
	}
	if first.kind != predicateKindPostsAll {
		releasePredicates([]predicate{first})
		t.Fatalf("expected first predicate to stay posts-all, got kind=%v", first.kind)
	}
	releasePredicates([]predicate{first})

	second, ok := db.engine.buildPredicateWithMode(expr, false)
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

	cached, ok := db.engine.snapshot.Current().LoadMaterializedPredKey(cacheKey)
	if !ok || cached.IsEmpty() {
		t.Fatalf("expected shared HAS cache entry after second build")
	}
	defer cached.Release()
	if !second.ids.SharesPayload(cached) {
		t.Fatalf("expected second predicate ids to use shared cache payload")
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

	view := db.engine.currentQueryViewForTests()

	expr := qx.GTE("age", 10)
	cacheKey := view.materializedPredKey(mustTestQIRExprForDB(t, db, expr))
	if cacheKey.IsZero() {
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

	view := db.engine.currentQueryViewForTests()

	preds, ok := db.engine.buildPredicatesOrderedWithMode(
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
		view.snap.Universe.Cardinality(),
	)
	if !route.useComplement {
		t.Fatalf("expected merged exact-range ordered route to use complement")
	}
	if route.cacheKey.IsZero() || route.complementCacheKey.IsZero() {
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
	if !db.engine.snapshot.Current().ShouldPromoteRuntimeMaterializedPredKey(route.complementCacheKey) {
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
			rb, covered, has, ok := db.engine.collectOrderRangeBounds("email", len(tc.exprs), func(i int) qx.Expr {
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
			if rb.Empty != tc.wantEmpty {
				t.Fatalf("empty mismatch: got=%v want=%v", rb.Empty, tc.wantEmpty)
			}
			if !tc.wantEmpty && rb.Prefix != tc.wantPrefix {
				t.Fatalf("prefix mismatch: got=%q want=%q", rb.Prefix, tc.wantPrefix)
			}
		})
	}
}
