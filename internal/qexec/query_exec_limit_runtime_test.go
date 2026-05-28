package qexec

import (
	"reflect"
	"testing"

	"github.com/vapstack/rbi/internal/posting"
)

func postingOf(ids ...uint64) posting.List {
	return posting.BuildFromSorted(ids)
}

func TestPlannerFilterPostingByLeafChecks_PreferredExactBypassesSmallBucketFallback(t *testing.T) {
	src := posting.BuildFromSorted([]uint64{1, 3, 5, 7, 9, 11, 13, 15})
	defer src.Release()

	postA := posting.BuildFromSorted([]uint64{3, 7, 11})
	postB := posting.BuildFromSorted([]uint64{5, 7, 13})
	defer postA.Release()
	defer postB.Release()

	postsBuf := posting.GetSlice(2)
	postsBuf = append(postsBuf, postA, postB)

	state := postsAnyFilterStatePool.Get()
	state.postsBuf = postsBuf

	preds := leafPredSlicePool.Get(1)
	preds = append(preds, leafPred{
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

	postsBuf := posting.GetSlice(2)
	postsBuf = append(postsBuf, postA, postB)

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
		for i := 0; i < len(postsBuf); i++ {
			postsBuf[i].Release()
		}
		posting.ReleaseSlice(postsBuf)
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

	preds := []predicate{
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
	}

	cursor := queryCursor{
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
	defer func() {
		for i := range posts {
			posts[i].Release()
		}
	}()

	postsBuf := posting.GetSlice(3)
	postsBuf = append(postsBuf, posts[0], posts[1], posts[2])
	defer posting.ReleaseSlice(postsBuf)

	warm := newPostingUnionIter(postsBuf)
	if warm != nil {
		warm.Release()
	}

	allocs := testing.AllocsPerRun(100, func() {
		it := newPostingUnionIter(postsBuf)
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

	warm := getU64Set(64)
	for i := 0; i < 32; i++ {
		_ = warm.Add(uint64(i))
	}
	releaseU64Set(&warm)

	allocs := testing.AllocsPerRun(100, func() {
		set := getU64Set(64)
		for i := 0; i < 32; i++ {
			_ = set.Add(uint64(i))
		}
		releaseU64Set(&set)
	})
	if allocs > 0 {
		t.Fatalf("unexpected allocs per run: got=%v want=0", allocs)
	}
}
