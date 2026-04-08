package rbi

import (
	"slices"
	"testing"

	"github.com/vapstack/rbi/internal/posting"
)

func buildQueryRuntimeTestLargePosting() posting.List {
	ids := make([]uint64, 0, posting.MidCap+16)
	for i := uint64(0); i < uint64(posting.MidCap+16); i++ {
		ids = append(ids, i*3+1)
	}
	return posting.BuildFromSorted(ids)
}

func TestPostingUnionBuilder_SinglePostingKeepsBorrowedPayload(t *testing.T) {
	base := buildQueryRuntimeTestLargePosting()
	defer base.Release()

	builder := newPostingUnionBuilder(true)
	builder.addPosting(base)
	out := builder.finish(true)
	defer out.Release()

	if !out.IsBorrowed() {
		t.Fatalf("single posting union must keep borrowed payload")
	}
	if !out.SharesPayload(base) {
		t.Fatalf("single posting union lost source payload sharing")
	}
	if got, want := out.Cardinality(), base.Cardinality(); got != want {
		t.Fatalf("cardinality mismatch: got=%d want=%d", got, want)
	}
}

func TestPostingUnionBuilder_MutationDetachesBorrowedPayload(t *testing.T) {
	base := buildQueryRuntimeTestLargePosting()
	defer base.Release()

	extra := uint64(1<<32 | 7)
	if base.Contains(extra) {
		t.Fatalf("test setup chose existing id %d", extra)
	}

	builder := newPostingUnionBuilder(true)
	builder.addPosting(base)
	builder.addSingle(extra)
	out := builder.finish(true)
	defer out.Release()

	if out.SharesPayload(base) {
		t.Fatalf("mutated union must detach from borrowed source payload")
	}
	if !out.Contains(extra) {
		t.Fatalf("mutated union missing appended id %d", extra)
	}
	if base.Contains(extra) {
		t.Fatalf("mutated union leaked appended id into base payload")
	}
}

func TestPostsAnyFilterStateApply_DirectIntersectMatchesMaterializedUnion(t *testing.T) {
	dstIDs := make([]uint64, 0, 192)
	for i := uint64(0); i < 192; i++ {
		dstIDs = append(dstIDs, i*3+1)
	}
	dst := posting.BuildFromSorted(dstIDs)
	defer dst.Release()

	postA := posting.BuildFromSorted([]uint64{
		dstIDs[5], dstIDs[8], dstIDs[11], dstIDs[14], dstIDs[17], dstIDs[20],
		dstIDs[23], dstIDs[26], dstIDs[29], dstIDs[32], dstIDs[35], dstIDs[38],
	})
	postB := posting.BuildFromSorted([]uint64{
		dstIDs[11], dstIDs[14], dstIDs[40], dstIDs[44], dstIDs[48], dstIDs[52],
		dstIDs[56], dstIDs[60], dstIDs[64], dstIDs[68], dstIDs[72], dstIDs[76],
	})

	postsBuf := postingSlicePool.Get()
	postsBuf.Append(postA)
	postsBuf.Append(postB)

	state := postsAnyFilterStatePool.Get()
	state.postsBuf = postsBuf

	defer func() {
		postsAnyFilterStatePool.Put(state)
		for i := 0; i < postsBuf.Len(); i++ {
			postsBuf.Get(i).Release()
			postsBuf.Set(i, posting.List{})
		}
		postingSlicePool.Put(postsBuf)
	}()

	expectedBuilder := newPostingUnionBuilder(true)
	expectedBuilder.addPosting(postA)
	expectedBuilder.addPosting(postB)
	expectedUnion := expectedBuilder.finish(true)
	expectedBuilder.release()
	defer expectedUnion.Release()

	expected := dst.Borrow().BuildAnd(expectedUnion)
	defer expected.Release()

	got, ok := state.apply(dst.Borrow())
	if !ok {
		t.Fatalf("postsAny apply must stay exact")
	}
	defer got.Release()

	if !state.ids.IsEmpty() {
		t.Fatalf("direct-intersect path should not materialize union eagerly")
	}
	if !slices.Equal(got.ToArray(), expected.ToArray()) {
		t.Fatalf("postsAny direct intersect mismatch: got=%v want=%v", got.ToArray(), expected.ToArray())
	}
}

func TestPostingUnionBuilder_SmallPostingsBatchSinglesMatchesBaseline(t *testing.T) {
	left := posting.BuildFromSorted([]uint64{3, 5, 7, 9, 11, 13, 15, 17})
	right := posting.BuildFromSorted([]uint64{5, 6, 7, 18, 19, 20, 21, 22, 23, 24})
	third := posting.BuildFromSorted([]uint64{1, 2, 3, 24, 25, 26})
	defer left.Release()
	defer right.Release()
	defer third.Release()

	builder := newPostingUnionBuilder(true)
	builder.addPosting(left)
	builder.addPosting(right)
	builder.addPosting(third)
	got := builder.finish(true)
	defer got.Release()

	want := left.Clone()
	want = want.BuildOr(right)
	want = want.BuildOr(third)
	want = want.BuildOptimized()
	defer want.Release()

	if !slices.Equal(got.ToArray(), want.ToArray()) {
		t.Fatalf("small posting union mismatch: got=%v want=%v", got.ToArray(), want.ToArray())
	}
}
