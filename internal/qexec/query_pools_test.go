package qexec

import (
	"slices"
	"testing"

	"github.com/vapstack/rbi/internal/posting"
)

func collectPoolsTestIter(it posting.Iterator) []uint64 {
	out := make([]uint64, 0, 16)
	for it.HasNext() {
		out = append(out, it.Next())
	}
	return out
}

func poolsSmall(ids ...uint64) posting.List {
	var out posting.List
	for _, id := range ids {
		out = out.BuildAdded(id)
	}
	return out
}

func poolsSingleton(id uint64) posting.List {
	return (posting.List{}).BuildAdded(id)
}

func TestPostingUnionIterPool_ReusedIteratorDoesNotLeakSeenState(t *testing.T) {
	first := newPostingUnionIter([]posting.List{
		poolsSmall(1, 2),
		poolsSmall(2, 3),
		poolsSmall(3, 4),
		poolsSmall(4),
	})
	u1, ok := first.(*postingUnionIter)
	if !ok {
		t.Fatalf("expected pooled postingUnionIter, got %T", first)
	}

	got1 := collectPoolsTestIter(u1)
	if !slices.Equal(got1, []uint64{1, 2, 3, 4}) {
		t.Fatalf("first union mismatch: got=%v", got1)
	}
	u1.Release()

	second := newPostingUnionIter([]posting.List{
		poolsSmall(2),
		poolsSmall(4),
		poolsSmall(5),
		poolsSmall(5, 6),
	})
	u2, ok := second.(*postingUnionIter)
	if !ok {
		t.Fatalf("expected pooled postingUnionIter, got %T", second)
	}
	defer u2.Release()
	if !testRaceEnabled && u2 != u1 {
		t.Fatalf("postingUnionIter pool did not reuse iterator")
	}

	got2 := collectPoolsTestIter(u2)
	if !slices.Equal(got2, []uint64{2, 4, 5, 6}) {
		t.Fatalf("reused union leaked seen-state: got=%v", got2)
	}
}

func TestCountLeadResidualExactFilterSliceBufReleaseClearsFullCapacity(t *testing.T) {
	buf := cardinalityLeadResidualExactFilterSlicePool.Get(2)
	buf = append(buf, cardinalityLeadResidualExactFilter{idx: 1, ids: poolsSmall(3, 7, 11)})
	buf = append(buf, cardinalityLeadResidualExactFilter{idx: 2, ids: poolsSmall(5, 9, 13)})
	cardinalityLeadResidualExactFilterSlicePool.Put(buf)

	buf = cardinalityLeadResidualExactFilterSlicePool.Get(2)
	if cap(buf) < 2 {
		t.Fatalf("unexpected pooled capacity: got=%d", cap(buf))
	}
	buf = buf[:2]
	defer cardinalityLeadResidualExactFilterSlicePool.Put(buf)
	for i := 0; i < 2; i++ {
		entry := buf[i]
		if entry.idx != 0 || !entry.ids.IsEmpty() {
			t.Fatalf("pooled extraExact buffer retained stale entry at %d: %+v", i, entry)
		}
	}
}

func TestCountORBranchSliceBufReleaseClearsFullCapacity(t *testing.T) {
	preds := newPredicateSet(1)
	preds.Append(predicate{
		kind:     predicateKindPostsAny,
		postsBuf: posting.GetSlice(0),
	})
	var checks [cardinalityPredicateScanMaxLeaves]int
	checks[0] = 7

	buf := cardinalityORBranchSlicePool.Get(1)
	buf = append(buf, newCardinalityORBranch(3, preds, 1, checks[:1], 42))
	cardinalityORBranchSlicePool.Put(buf)

	buf = cardinalityORBranchSlicePool.Get(1)
	if cap(buf) < 1 {
		t.Fatalf("unexpected pooled capacity: got=%d", cap(buf))
	}
	buf = buf[:1]
	defer cardinalityORBranchSlicePool.Put(buf)
	entry := buf[0]
	if entry.index != 0 ||
		entry.preds.Len() != 0 ||
		entry.preds.owner != nil ||
		entry.lead != 0 ||
		entry.checksLen != 0 ||
		entry.checks[0] != 0 ||
		entry.est != 0 {
		t.Fatalf("pooled cardinalityORBranch buffer retained stale entry: %+v", entry)
	}
}

func TestLeafPredSliceBufReleaseClearsFullCapacity(t *testing.T) {
	buf := leafPredSlicePool.Get(2)
	buf = append(buf, leafPred{
		kind:          leafPredKindPostsUnion,
		postingFilter: func(ids posting.List) (posting.List, bool) { return ids, true },
		postsBuf:      posting.GetSlice(0),
		postsAnyState: postsAnyFilterStatePool.Get(),
	})
	buf = append(buf, leafPred{
		kind:          leafPredKindPostsAll,
		posting:       poolsSingleton(3),
		estCard:       1,
		postingFilter: func(ids posting.List) (posting.List, bool) { return ids, true },
		postsBuf:      posting.GetSlice(0),
		postsAnyState: postsAnyFilterStatePool.Get(),
	})
	leafPredSlicePool.Put(buf)

	buf = leafPredSlicePool.Get(2)
	if cap(buf) < 2 {
		t.Fatalf("unexpected pooled capacity: got=%d", cap(buf))
	}
	buf = buf[:2]
	defer leafPredSlicePool.Put(buf)
	for i := 0; i < 2; i++ {
		entry := buf[i]
		if entry.kind != leafPredKindEmpty ||
			!entry.posting.IsEmpty() ||
			entry.estCard != 0 ||
			entry.postingFilter != nil ||
			entry.postsBuf != nil ||
			entry.postsAnyState != nil {
			t.Fatalf("pooled leafPred buffer retained stale entry at %d", i)
		}
	}
}

func TestPostsAnyFilterStatePoolClearsOwnedIDsOnly(t *testing.T) {
	post := poolsSmall(11, 13)
	posts := posting.GetSlice(1)
	posts = append(posts, post)

	state := postsAnyFilterStatePool.Get()
	state.postsBuf = posts
	state.ids = poolsSmall(3, 5, 7)
	state.containsCalls = 9
	state.containsMaterializeAt = 4
	state.applyObservedSavings = 123
	state.neg = true

	postsAnyFilterStatePool.Put(state)
	defer func() {
		posts[0].Release()
		posting.ReleaseSlice(posts)
	}()

	if state.postsBuf != nil ||
		!state.ids.IsEmpty() ||
		state.containsCalls != 0 ||
		state.containsMaterializeAt != 0 ||
		state.applyObservedSavings != 0 ||
		state.neg {
		t.Fatalf("pooled postsAnyFilterState retained stale state: %+v", state)
	}
	if !posts[0].Contains(11) || !posts[0].Contains(13) {
		t.Fatalf("postsAnyFilterState pool released caller-owned postsBuf")
	}
}

func TestLazyMaterializedPredicateStatePoolClearsOwnedIDs(t *testing.T) {
	state := lazyMaterializedPredicateStatePool.Get()
	state.ids = poolsSmall(21, 34, 55)
	state.loaded = true

	lazyMaterializedPredicateStatePool.Put(state)

	if !state.ids.IsEmpty() || state.loaded {
		t.Fatalf("pooled lazy materialized state retained stale state: %+v", state)
	}
}

func TestFieldIndexRangePredicateStatePoolClearsOwnedPostings(t *testing.T) {
	linear := posting.GetSlice(2)
	linear = append(linear, poolsSmall(1, 3), poolsSmall(5, 7))

	state := fieldIndexRangePredicateStatePool.Get()
	state.bucketCount = 2
	state.keepProbeHits = true
	state.rangeMaterialized = true
	state.probeMaterialized = true
	state.linearPostsBuf = linear
	state.rangeIDs = poolsSmall(1, 3, 5, 7)
	state.probeIDs = poolsSmall(3, 7)

	fieldIndexRangePredicateStatePool.Put(state)

	if state.linearPostsBuf != nil ||
		!state.rangeIDs.IsEmpty() ||
		!state.probeIDs.IsEmpty() ||
		state.bucketCount != 0 ||
		state.keepProbeHits ||
		state.rangeMaterialized ||
		state.probeMaterialized {
		t.Fatalf("pooled fieldIndexRangePredicateState retained stale state: %+v", state)
	}
	full := linear[:cap(linear)]
	for i := range full {
		if !full[i].IsEmpty() {
			t.Fatalf("released linearPostsBuf retained posting at slot %d", i)
		}
	}
}

func TestU64SetReleaseClearsUsedBitmap(t *testing.T) {
	set := getU64Set(4)
	if set.pooled == nil {
		t.Fatalf("expected pooled u64set")
	}
	if !set.Add(17) || !set.Add(33) || !set.Add(65) {
		t.Fatalf("unexpected duplicate in fresh set")
	}
	if !set.Has(17) || !set.Has(33) || !set.Has(65) {
		t.Fatalf("fresh set lookup failed")
	}

	used := set.used
	releaseU64Set(&set)

	if set.keys != nil || set.used != nil || set.n != 0 || set.pooled != nil {
		t.Fatalf("released u64set handle retained state: %+v", set)
	}
	for i := range used {
		if used[i] != 0 {
			t.Fatalf("released u64set used bitmap retained slot %d", i)
		}
	}
}
