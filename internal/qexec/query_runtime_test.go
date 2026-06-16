package qexec

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/rbierrors"
	"github.com/vapstack/rbi/rbitrace"
)

func TestRuntimeQueryFieldCatalogResolvesStringKey(t *testing.T) {
	db := newTestDB(t, testOptions{})
	ageOrdinal := db.rt.IndexedByName["age"].Ordinal
	info, ok := db.exec.ResolveField("age")
	if !ok || info.Ordinal != ageOrdinal {
		t.Fatalf("ResolveField(age)=(%+v,%v), want ordinal %d", info, ok, ageOrdinal)
	}
	if _, ok = db.exec.ResolveField(schema.ReservedKeyFieldName); ok {
		t.Fatalf("disabled %s resolved", schema.ReservedKeyFieldName)
	}

	db.enableStringKeyCatalog()
	info, ok = db.exec.ResolveField(schema.ReservedKeyFieldName)
	if !ok {
		t.Fatalf("ResolveField(%s) failed", schema.ReservedKeyFieldName)
	}
	if info.Ordinal < 0 || info.Ordinal != len(db.rt.Indexed) {
		t.Fatalf("%s ordinal=%d want %d", schema.ReservedKeyFieldName, info.Ordinal, len(db.rt.Indexed))
	}
	if info.Caps != 0 {
		t.Fatalf("%s caps=%d want 0", schema.ReservedKeyFieldName, info.Caps)
	}
	if db.exec.FieldNameByOrdinal(info.Ordinal) != schema.ReservedKeyFieldName {
		t.Fatalf("FieldNameByOrdinal(%d)=%q", info.Ordinal, db.exec.FieldNameByOrdinal(info.Ordinal))
	}
	if _, ok = db.rt.IndexedByName[schema.ReservedKeyFieldName]; ok {
		t.Fatalf("%s inserted into schema.IndexedByName", schema.ReservedKeyFieldName)
	}
	info, ok = db.exec.ResolveField("age")
	if !ok || info.Ordinal != ageOrdinal {
		t.Fatalf("ResolveField(age) after key catalog=(%+v,%v), want ordinal %d", info, ok, ageOrdinal)
	}
}

func TestRuntimeNumericKeyPredicatesAndOrderUseUniverse(t *testing.T) {
	db := newTestDB(t, testOptions{})
	db.enableNumericKeyCatalog()
	db.seedGeneratedData(t, 8, func(id uint64) testRec {
		return testRec{
			Name:   fmt.Sprintf("name-%02d", id),
			Age:    int(20 + id),
			Active: id%2 == 0,
		}
	})

	cases := []struct {
		name string
		q    *qx.QX
		want []uint64
	}{
		{name: "eq", q: qx.Query(qx.EQ(schema.ReservedKeyFieldName, uint64(3))), want: []uint64{3}},
		{name: "eq_negative_empty", q: qx.Query(qx.EQ(schema.ReservedKeyFieldName, -1)), want: nil},
		{name: "eq_fraction_empty", q: qx.Query(qx.EQ(schema.ReservedKeyFieldName, 2.5)), want: nil},
		{name: "eq_float_2_64_empty", q: qx.Query(qx.EQ(schema.ReservedKeyFieldName, math.Ldexp(1, 64))), want: nil},
		{name: "in", q: qx.Query(qx.IN(schema.ReservedKeyFieldName, []any{uint64(1), int64(-1), 4.5, 6})), want: []uint64{1, 6}},
		{name: "in_float_2_64_skipped", q: qx.Query(qx.IN(schema.ReservedKeyFieldName, []any{uint64(2), math.Ldexp(1, 64), math.Ldexp(1, 65), 8})), want: []uint64{2, 8}},
		{name: "in_all_impossible_empty", q: qx.Query(qx.IN(schema.ReservedKeyFieldName, []any{int64(-1), 4.5})), want: nil},
		{name: "range", q: qx.Query(qx.GTE(schema.ReservedKeyFieldName, 3), qx.LT(schema.ReservedKeyFieldName, 7)), want: []uint64{3, 4, 5, 6}},
		{name: "range_gte_float_2_64_empty", q: qx.Query(qx.GTE(schema.ReservedKeyFieldName, math.Ldexp(1, 64))), want: nil},
		{name: "range_lt_float_2_64_full", q: qx.Query(qx.LT(schema.ReservedKeyFieldName, math.Ldexp(1, 64))), want: []uint64{1, 2, 3, 4, 5, 6, 7, 8}},
		{name: "range_lte_float_above_2_64_full", q: qx.Query(qx.LTE(schema.ReservedKeyFieldName, math.Ldexp(1, 65))), want: []uint64{1, 2, 3, 4, 5, 6, 7, 8}},
		{name: "range_active", q: qx.Query(qx.GTE(schema.ReservedKeyFieldName, 2), qx.LTE(schema.ReservedKeyFieldName, 7), qx.EQ("active", true)), want: []uint64{2, 4, 6}},
		{name: "order_asc", q: qx.Query(qx.GTE(schema.ReservedKeyFieldName, 3)).Sort(schema.ReservedKeyFieldName, qx.ASC).Limit(3), want: []uint64{3, 4, 5}},
		{name: "order_desc", q: qx.Query(qx.LTE(schema.ReservedKeyFieldName, 6)).Sort(schema.ReservedKeyFieldName, qx.DESC).Limit(3), want: []uint64{6, 5, 4}},
		{name: "order_contradictory_const_false", q: qx.Query(qx.EQ(schema.ReservedKeyFieldName, uint64(1)), qx.NOT(qx.EQ(schema.ReservedKeyFieldName, uint64(1)))).Sort(schema.ReservedKeyFieldName, qx.ASC).Limit(3), want: nil},
		{name: "order_contradictory_key", q: qx.Query(qx.EQ(schema.ReservedKeyFieldName, uint64(5)), qx.NOT(qx.EQ(schema.ReservedKeyFieldName, uint64(5)))).Sort(schema.ReservedKeyFieldName, qx.ASC).Limit(3), want: nil},
		{name: "order_or_residual", q: qx.Query(qx.GTE(schema.ReservedKeyFieldName, 0), qx.OR(qx.EQ("active", true), qx.EQ("name", "name-03"))).Sort(schema.ReservedKeyFieldName, qx.ASC).Limit(4), want: []uint64{2, 3, 4, 6}},
		{name: "or_materialized_order", q: qx.Query(qx.OR(qx.EQ(schema.ReservedKeyFieldName, 2), qx.EQ("active", true))).Sort(schema.ReservedKeyFieldName, qx.DESC).Limit(3), want: []uint64{8, 6, 4}},
	}
	for _, tc := range cases {
		got, err := db.query(tc.q)
		if err != nil {
			t.Fatalf("%s query: %v", tc.name, err)
		}
		if !slices.Equal(got, tc.want) {
			t.Fatalf("%s query=%v want %v", tc.name, got, tc.want)
		}
	}

	if _, err := db.query(qx.Query(qx.EQ(schema.ReservedKeyFieldName, "3"))); err == nil {
		t.Fatal("numeric $key string predicate succeeded")
	}
	if _, err := db.query(qx.Query(qx.PREFIX(schema.ReservedKeyFieldName, 3))); err == nil {
		t.Fatal("numeric $key PREFIX predicate succeeded")
	}
}

func TestRuntimeNumericKeyRejectsEmptyIN(t *testing.T) {
	db := newTestDB(t, testOptions{})
	db.enableNumericKeyCatalog()
	db.seedData(t, 4)

	for _, expr := range []qx.Expr{
		qx.IN(schema.ReservedKeyFieldName, []uint64{}),
		qx.IN(schema.ReservedKeyFieldName, []any{}),
	} {
		q := qx.Query(expr)
		if _, err := db.query(q); !errors.Is(err, rbierrors.ErrInvalidQuery) {
			t.Fatalf("query err=%v, want ErrInvalidQuery", err)
		}

		prepared, err := qir.PrepareCountExprsResolved(db.exec, expr)
		if err != nil {
			t.Fatalf("PrepareCountExprsResolved: %v", err)
		}
		ids, err := db.view().Filter(prepared)
		prepared.Release()
		if err == nil {
			ids.Release()
		}
		if !errors.Is(err, rbierrors.ErrInvalidQuery) {
			t.Fatalf("Filter err=%v, want ErrInvalidQuery", err)
		}
	}
}

func TestRuntimeNumericKeyOrderedLimitSelectorRoutes(t *testing.T) {
	recorder := &traceContractRecorder{}
	db := newTestDB(t, testOptions{
		TraceSink:        recorder.sink,
		TraceSampleEvery: 1,
	})
	db.enableNumericKeyCatalog()
	db.seedGeneratedData(t, 8, func(id uint64) testRec {
		return testRec{
			Name:   fmt.Sprintf("name-%02d", id),
			Age:    int(20 + id),
			Active: id%2 == 0,
		}
	})

	cases := []struct {
		name     string
		q        *qx.QX
		want     []uint64
		plan     rbitrace.PlanName
		selected string
		rejected string
	}{
		{
			name:     "supported_residual",
			q:        qx.Query(qx.GTE(schema.ReservedKeyFieldName, 3), qx.EQ("active", true)).Sort(schema.ReservedKeyFieldName, qx.ASC).Limit(2),
			want:     []uint64{4, 6},
			plan:     rbitrace.PlanLimitOrderBasic,
			selected: plannerOrderedLimitCandidateNumericKeyScan.String(),
			rejected: plannerOrderedLimitCandidateMaterializedFallback.String(),
		},
		{
			name:     "unsupported_or_residual",
			q:        qx.Query(qx.GTE(schema.ReservedKeyFieldName, 0), qx.OR(qx.EQ("active", true), qx.EQ("name", "name-03"))).Sort(schema.ReservedKeyFieldName, qx.ASC).Limit(4),
			want:     []uint64{2, 3, 4, 6},
			plan:     rbitrace.PlanMaterialized,
			selected: plannerOrderedLimitCandidateMaterializedFallback.String(),
			rejected: plannerOrderedLimitCandidateNumericKeyScan.String(),
		},
	}

	for _, tc := range cases {
		prepared, shape, err := db.prepareQuery(tc.q)
		if err != nil {
			t.Fatalf("%s prepareQuery: %v", tc.name, err)
		}
		mark := recorder.mark()
		got, err := db.view().Query(&shape, true)
		prepared.Release()
		if err != nil {
			t.Fatalf("%s Query: %v", tc.name, err)
		}
		if !slices.Equal(got, tc.want) {
			t.Fatalf("%s query=%v want %v", tc.name, got, tc.want)
		}
		ev := recorder.lastSince(t, mark)
		if ev.Plan != tc.plan {
			t.Fatalf("%s plan=%q want %q trace=%+v", tc.name, ev.Plan, tc.plan, ev)
		}
		if ev.OrderedLimitRoute.Selected != tc.selected {
			t.Fatalf("%s selected=%q want %q route=%+v", tc.name, ev.OrderedLimitRoute.Selected, tc.selected, ev.OrderedLimitRoute)
		}
		if ev.OrderedLimitRoute.Rejected != tc.rejected {
			t.Fatalf("%s rejected=%q want %q route=%+v", tc.name, ev.OrderedLimitRoute.Rejected, tc.rejected, ev.OrderedLimitRoute)
		}
	}
}

func TestQuery_TryQueryEmptyOnSnapshot_SimpleScalarLeaf(t *testing.T) {
	db, _ := openTempDBUint64(t)

	for i := 1; i <= 8; i++ {
		if err := db.Set(uint64(i), &Rec{
			Name:   fmt.Sprintf("u%d", i),
			Age:    18 + i,
			Active: i%2 == 0,
		}); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}

	prepared, viewQ, err := prepareTestQuery(db.engine, qx.Query(qx.GT("age", 100)))
	if err != nil {
		t.Fatalf("prepareTestQuery(no-match): %v", err)
	}
	snap, seq, ref := db.engine.snapshot.PinCurrent()
	view := db.engine.exec.AcquireView(snap)
	empty, err := view.TryQueryEmptyOnSnapshot(&viewQ)
	db.engine.exec.ReleaseView(view)
	db.engine.snapshot.Unpin(seq, ref)
	prepared.Release()
	if err != nil {
		t.Fatalf("tryQueryEmptyOnSnapshot(no-match): %v", err)
	}
	if !empty {
		t.Fatalf("expected GT(age,100) to be proven empty without tx")
	}

	prepared, viewQ, err = prepareTestQuery(db.engine, qx.Query(qx.GTE("age", 20)))
	if err != nil {
		t.Fatalf("prepareTestQuery(hit): %v", err)
	}
	snap, seq, ref = db.engine.snapshot.PinCurrent()
	view = db.engine.exec.AcquireView(snap)
	empty, err = view.TryQueryEmptyOnSnapshot(&viewQ)
	db.engine.exec.ReleaseView(view)
	db.engine.snapshot.Unpin(seq, ref)
	prepared.Release()
	if err != nil {
		t.Fatalf("tryQueryEmptyOnSnapshot(hit): %v", err)
	}
	if empty {
		t.Fatalf("did not expect GTE(age,20) hit query to be proven empty")
	}
}

func buildQueryRuntimeTestLargePosting() posting.List {
	ids := make([]uint64, 0, posting.MidCap+16)
	for i := uint64(0); i < uint64(posting.MidCap+16); i++ {
		ids = append(ids, i*3+1)
	}
	return posting.BuildFromSorted(ids)
}

func TestPostingUnionBuilder_SinglePostingReturnsOwnedPayload(t *testing.T) {
	base := buildQueryRuntimeTestLargePosting()
	defer base.Release()

	builder := newPostingUnionBuilder(true)
	builder.addPosting(base)
	out := builder.finish(true)
	defer out.Release()

	if out.IsBorrowed() {
		t.Fatalf("single posting union must return owned payload")
	}
	if out.SharesPayload(base) {
		t.Fatalf("single posting union must detach from source payload")
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

	postsBuf := posting.GetSlice(2)
	postsBuf = append(postsBuf, postA, postB)

	state := postsAnyFilterStatePool.Get()
	state.postsBuf = postsBuf

	defer func() {
		postsAnyFilterStatePool.Put(state)
		for i := 0; i < len(postsBuf); i++ {
			postsBuf[i].Release()
		}
		posting.ReleaseSlice(postsBuf)
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

func TestPostsAnyFilterStateApply_RepeatedDirectIntersectPromotesMaterializedUnion(t *testing.T) {
	dstIDs := make([]uint64, 0, 128)
	for i := uint64(0); i < 128; i++ {
		dstIDs = append(dstIDs, i*2+1)
	}
	dst := posting.BuildFromSorted(dstIDs)
	defer dst.Release()

	postAIDs := make([]uint64, 0, 2048)
	postBIDs := make([]uint64, 0, 1365)
	for i := uint64(0); i < 2048; i++ {
		v := i*2 + 1
		postAIDs = append(postAIDs, v)
		if i%3 != 0 {
			postBIDs = append(postBIDs, v)
		}
	}
	postA := posting.BuildFromSorted(postAIDs)
	postB := posting.BuildFromSorted(postBIDs)

	postsBuf := posting.GetSlice(2)
	postsBuf = append(postsBuf, postA, postB)

	state := postsAnyFilterStatePool.Get()
	state.postsBuf = postsBuf

	defer func() {
		postsAnyFilterStatePool.Put(state)
		for i := 0; i < len(postsBuf); i++ {
			postsBuf[i].Release()
		}
		posting.ReleaseSlice(postsBuf)
	}()

	expectedBuilder := newPostingUnionBuilder(true)
	expectedBuilder.addPosting(postA)
	expectedBuilder.addPosting(postB)
	expectedUnion := expectedBuilder.finish(true)
	expectedBuilder.release()
	defer expectedUnion.Release()

	expected := dst.Borrow().BuildAnd(expectedUnion)
	defer expected.Release()

	promoted := false
	for i := 0; i < 16; i++ {
		got, ok := state.apply(dst.Borrow())
		if !ok {
			t.Fatalf("postsAny repeated apply must stay exact")
		}
		if !slices.Equal(got.ToArray(), expected.ToArray()) {
			got.Release()
			t.Fatalf("postsAny repeated direct intersect mismatch on iter=%d", i)
		}
		got.Release()
		if !state.ids.IsEmpty() {
			promoted = true
			break
		}
	}
	if !promoted {
		t.Fatalf("expected repeated direct intersect to promote materialized union")
	}
}

func TestPostsAnyFilterStateSetExpectedContainsCalls_PromotesFirstUseMaterialization(t *testing.T) {
	postAIDs := make([]uint64, 0, 4096)
	postBIDs := make([]uint64, 0, 3072)
	for i := uint64(0); i < 4096; i++ {
		v := i*2 + 1
		postAIDs = append(postAIDs, v)
		if i%4 != 0 {
			postBIDs = append(postBIDs, v)
		}
	}
	postA := posting.BuildFromSorted(postAIDs)
	postB := posting.BuildFromSorted(postBIDs)

	postsBuf := posting.GetSlice(2)
	postsBuf = append(postsBuf, postA, postB)

	state := postsAnyFilterStatePool.Get()
	state.postsBuf = postsBuf
	state.containsMaterializeAt = postsAnyContainsMaterializeAfterBuf(postsBuf)

	defer func() {
		postsAnyFilterStatePool.Put(state)
		for i := 0; i < len(postsBuf); i++ {
			postsBuf[i].Release()
		}
		posting.ReleaseSlice(postsBuf)
	}()

	if state.containsMaterializeAt == 1 {
		t.Fatalf("expected setup to start above first-use materialization threshold")
	}
	state.setExpectedContainsCalls(8192)
	if state.containsMaterializeAt != 1 {
		t.Fatalf("expected expected-work routing to promote first-use materialization, got=%d", state.containsMaterializeAt)
	}
	if !state.matches(postAIDs[7]) {
		t.Fatalf("expected promoted state to match union member")
	}
	if state.ids.IsEmpty() {
		t.Fatalf("expected first contains after promotion to materialize union")
	}
}

func TestMaterializePostingUnionBufOwned_TwoTermSubsetFastPath(t *testing.T) {
	largeIDs := make([]uint64, 0, 128)
	subsetIDs := make([]uint64, 0, 16)
	controlIDs := make([]uint64, 0, 16)
	for i := uint64(0); i < 128; i++ {
		id := i*2 + 2
		largeIDs = append(largeIDs, id)
		if i&7 == 0 {
			subsetIDs = append(subsetIDs, id)
			controlIDs = append(controlIDs, id-1)
		}
	}

	large := posting.BuildFromSorted(largeIDs)
	subset := posting.BuildFromSorted(subsetIDs)
	control := posting.BuildFromSorted(controlIDs)
	defer large.Release()
	defer subset.Release()
	defer control.Release()

	subsetPosts := []posting.List{subset, large}
	got := materializePostingUnionBufOwned(subsetPosts)
	if !slices.Equal(got.ToArray(), largeIDs) {
		t.Fatalf("subset union mismatch: got=%v want=%v", got.ToArray(), largeIDs)
	}
	got.Release()

	controlPosts := []posting.List{control, large}
	got = materializePostingUnionBufOwned(controlPosts)
	want := append(controlIDs[:len(controlIDs):len(controlIDs)], largeIDs...)
	slices.Sort(want)
	if !slices.Equal(got.ToArray(), want) {
		t.Fatalf("control union mismatch: got=%v want=%v", got.ToArray(), want)
	}
	got.Release()
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

func TestPostingUnionBuilder_CompactPostingsBatchSinglesAllocsPerRunStayZeroAfterWarmup(t *testing.T) {
	if testRaceEnabled {
		t.Skip("testing.AllocsPerRun is not stable under -race")
	}

	posts := [...]posting.List{
		posting.BuildFromSorted([]uint64{1, 3, 5, 7, 9, 11, 13, 15}),
		posting.BuildFromSorted([]uint64{17, 19, 21, 23, 25, 27, 29, 31}),
		posting.BuildFromSorted([]uint64{33, 35, 37, 39, 41, 43, 45, 47}),
		posting.BuildFromSorted([]uint64{49, 51, 53, 55, 57, 59, 61, 63}),
		posting.BuildFromSorted([]uint64{65, 67, 69, 71, 73, 75, 77, 79}),
	}
	defer func() {
		for i := range posts {
			posts[i].Release()
		}
	}()

	run := func() {
		builder := newPostingUnionBuilder(true)
		for i := range posts {
			builder.addPosting(posts[i])
		}
		out := builder.finish(true)
		out.Release()
	}

	run()
	allocs := testing.AllocsPerRun(100, run)
	if allocs != 0 {
		t.Fatalf("expected zero allocs after warmup, got %.2f", allocs)
	}
}
