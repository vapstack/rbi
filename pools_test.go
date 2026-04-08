package rbi

import (
	"path/filepath"
	"slices"
	"testing"
	"unsafe"

	"github.com/vapstack/rbi/internal/posting"
	"github.com/vmihailenco/msgpack/v5"
)

type pooledSparseRec struct {
	Name string   `db:"name"`
	Tags []string `db:"tags"`
	Opt  *string  `db:"opt"`
}

func (r *pooledSparseRec) MarshalMsgpack() ([]byte, error) {
	m := make(map[string]any, 3)
	if r.Name != "" {
		m["name"] = r.Name
	}
	if len(r.Tags) != 0 {
		m["tags"] = r.Tags
	}
	if r.Opt != nil {
		m["opt"] = *r.Opt
	}
	return msgpack.Marshal(m)
}

func (r *pooledSparseRec) UnmarshalMsgpack(b []byte) error {
	// Intentionally update only fields present in the payload. If decode reuses a
	// dirty pooled struct without zeroing it first, absent fields leak through.
	var tmp struct {
		Name *string   `msgpack:"name"`
		Tags *[]string `msgpack:"tags"`
		Opt  *string   `msgpack:"opt"`
	}
	if err := msgpack.Unmarshal(b, &tmp); err != nil {
		return err
	}
	if tmp.Name != nil {
		r.Name = *tmp.Name
	}
	if tmp.Tags != nil {
		r.Tags = append(r.Tags[:0], *tmp.Tags...)
	}
	if tmp.Opt != nil {
		v := *tmp.Opt
		r.Opt = &v
	}
	return nil
}

func openTempDBUint64PooledSparseRec(t *testing.T, options ...Options) (*DB[uint64, pooledSparseRec], string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_pooled_sparse.db")
	db, raw := openBoltAndNew[uint64, pooledSparseRec](t, path, options...)
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	return db, path
}

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

func TestRecordPool_ReusedDecodeLeaksAbsentFields(t *testing.T) {
	db, _ := openTempDBUint64PooledSparseRec(t, Options{AutoBatchMax: 1})

	opt := "sticky"
	if err := db.Set(1, &pooledSparseRec{
		Name: "first",
		Tags: []string{"go", "db"},
		Opt:  &opt,
	}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &pooledSparseRec{Name: "second"}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	const attempts = 256
	for attempt := 0; attempt < attempts; attempt++ {
		first, err := db.Get(1)
		if err != nil {
			t.Fatalf("Get(1) attempt=%d: %v", attempt, err)
		}
		ptr1 := uintptr(unsafe.Pointer(first))
		if first.Name != "first" || !slices.Equal(first.Tags, []string{"go", "db"}) || first.Opt == nil || *first.Opt != opt {
			t.Fatalf("unexpected first record before release: %#v", first)
		}
		db.ReleaseRecords(first)

		second, err := db.Get(2)
		if err != nil {
			t.Fatalf("Get(2) attempt=%d: %v", attempt, err)
		}
		ptr2 := uintptr(unsafe.Pointer(second))
		if ptr1 != ptr2 {
			db.ReleaseRecords(second)
			continue
		}
		if second.Name != "second" {
			t.Fatalf("pooled decode returned wrong name: %#v", second)
		}
		if len(second.Tags) != 0 || second.Opt != nil {
			t.Fatalf("pooled decode leaked fields from previous record: %#v", second)
		}
		db.ReleaseRecords(second)
		return
	}

	if testRaceEnabled {
		t.Skipf("sync.Pool reuse is not guaranteed under -race after %d attempts", attempts)
	}
	t.Fatalf("failed to observe record pool reuse after %d attempts", attempts)
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
	buf := countLeadResidualExactFilterSlicePool.Get()
	buf.Append(countLeadResidualExactFilter{idx: 1, ids: poolsSmall(3, 7, 11)})
	buf.Append(countLeadResidualExactFilter{idx: 2, ids: poolsSmall(5, 9, 13)})
	countLeadResidualExactFilterSlicePool.Put(buf)

	buf = countLeadResidualExactFilterSlicePool.Get()
	defer countLeadResidualExactFilterSlicePool.Put(buf)
	if buf.Cap() < 2 {
		t.Fatalf("unexpected pooled capacity: got=%d", buf.Cap())
	}
	buf.SetLen(2)
	for i := 0; i < 2; i++ {
		entry := buf.Get(i)
		if entry.idx != 0 || !entry.ids.IsEmpty() {
			t.Fatalf("pooled extraExact buffer retained stale entry at %d: %+v", i, entry)
		}
	}
}

func TestCountORBranchSliceBufReleaseClearsFullCapacity(t *testing.T) {
	preds := newPredicateSet(1)
	preds.Append(predicate{
		kind:     predicateKindPostsAny,
		postsBuf: postingSlicePool.Get(),
	})
	var checks [countPredicateScanMaxLeaves]int
	checks[0] = 7

	buf := countORBranchSlicePool.Get()
	buf.Append(newCountORBranch(3, preds, 1, checks[:1], 42))
	countORBranchSlicePool.Put(buf)

	buf = countORBranchSlicePool.Get()
	defer countORBranchSlicePool.Put(buf)
	if buf.Cap() < 1 {
		t.Fatalf("unexpected pooled capacity: got=%d", buf.Cap())
	}
	buf.SetLen(1)
	entry := buf.Get(0)
	if entry.index != 0 ||
		entry.preds.Len() != 0 ||
		entry.preds.owner != nil ||
		entry.lead != 0 ||
		entry.checksLen != 0 ||
		entry.checks[0] != 0 ||
		entry.est != 0 {
		t.Fatalf("pooled countORBranch buffer retained stale entry: %+v", entry)
	}
}

func TestLeafPredSliceBufReleaseClearsFullCapacity(t *testing.T) {
	buf := leafPredSlicePool.Get()
	buf.Append(leafPred{
		kind:          leafPredKindPostsUnion,
		postingFilter: func(ids posting.List) (posting.List, bool) { return ids, true },
		postsBuf:      postingSlicePool.Get(),
		postsAnyState: postsAnyFilterStatePool.Get(),
	})
	buf.Append(leafPred{
		kind:          leafPredKindPostsAll,
		posting:       poolsSingleton(3),
		estCard:       1,
		postingFilter: func(ids posting.List) (posting.List, bool) { return ids, true },
		postsBuf:      postingSlicePool.Get(),
		postsAnyState: postsAnyFilterStatePool.Get(),
	})
	leafPredSlicePool.Put(buf)

	buf = leafPredSlicePool.Get()
	defer leafPredSlicePool.Put(buf)
	if buf.Cap() < 2 {
		t.Fatalf("unexpected pooled capacity: got=%d", buf.Cap())
	}
	buf.SetLen(2)
	for i := 0; i < 2; i++ {
		entry := buf.Get(i)
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
func TestBuildIndexFieldLocalStateReleaseClearsVals(t *testing.T) {
	state := newBuildIndexFieldLocalState(false, false)
	for i := 1; i <= posting.MidCap+1; i++ {
		state.addValue("name", uint64(i))
	}

	state.release()
	if state.vals != nil {
		t.Fatalf("expected vals map to be cleared")
	}
}

func TestBuildIndexFieldLocalStateReleaseClearsFixed(t *testing.T) {
	state := newBuildIndexFieldLocalState(true, false)
	for i := 1; i <= posting.MidCap+1; i++ {
		state.addFixedValue(42, uint64(i))
	}

	state.release()
	if state.fixed != nil {
		t.Fatalf("expected fixed map to be cleared")
	}
}

func TestBuildIndexFieldLocalStateReleaseClearsLenMap(t *testing.T) {
	state := newBuildIndexFieldLocalState(false, true)
	for i := 1; i <= posting.MidCap+1; i++ {
		state.addLen(3, uint64(i))
	}

	state.release()
	if state.lenMap != nil {
		t.Fatalf("expected len map to be cleared")
	}
}

func TestBuildIndexFieldLocalStateReleaseClearsNils(t *testing.T) {
	state := newBuildIndexFieldLocalState(false, false)
	for i := 1; i <= posting.MidCap+1; i++ {
		state.addNil(uint64(i))
	}

	state.release()
	if !state.nils.IsEmpty() {
		t.Fatalf("expected nil postings to be cleared")
	}
}

func TestBuildIndexFieldLocalStateFlushAllIntoMergesNilSource(t *testing.T) {
	state := newBuildIndexFieldLocalState(false, false)
	for i := 1; i <= posting.MidCap+1; i++ {
		state.addNil(uint64(i))
	}

	dst := newBuildIndexFieldState(false, false)
	dst.nils = poolsSingleton(777)

	state.flushAllInto(dst)
	if !dst.nils.Contains(777) || !dst.nils.Contains(1) || !dst.nils.Contains(posting.MidCap+1) {
		t.Fatalf("merged nil postings lost data")
	}
	if !state.nils.IsEmpty() {
		t.Fatalf("expected source nil postings to be drained")
	}
}

func TestBuildIndexFieldLocalStateFlushAllIntoMergesLenSource(t *testing.T) {
	state := newBuildIndexFieldLocalState(false, true)
	for i := 1; i <= posting.MidCap+1; i++ {
		state.addLen(3, uint64(i))
	}

	dst := newBuildIndexFieldState(false, true)
	dst.lenMap[3] = poolsSingleton(888)

	state.flushAllInto(dst)
	if !dst.lenMap[3].Contains(888) || !dst.lenMap[3].Contains(1) || !dst.lenMap[3].Contains(posting.MidCap+1) {
		t.Fatalf("merged len postings lost data")
	}
	if len(state.lenMap) != 0 {
		t.Fatalf("expected source len map to be drained")
	}
}

func TestBuildIndexFieldStateMaterializeStorageMergesRunSource(t *testing.T) {
	left := postingMapPool.Get()
	left["name"] = left["name"].BuildAdded(777)
	runLeft := buildIndexStringRunFromPostingMap(left)

	right := postingMapPool.Get()
	for i := 1; i <= posting.MidCap+1; i++ {
		right["name"] = right["name"].BuildAdded(uint64(i))
	}
	runRight := buildIndexStringRunFromPostingMap(right)

	state := newBuildIndexFieldState(false, false)
	state.runs = []buildIndexFieldRun{runLeft, runRight}

	storage := state.materializeStorage()
	if storage.keyCount() != 1 {
		t.Fatalf("unexpected key count after materialize: %d", storage.keyCount())
	}
	if len(state.runs) != 0 {
		t.Fatalf("expected runs to be detached during materialization")
	}
}

func TestBuildIndexFieldStateReleaseClearsRuns(t *testing.T) {
	m := postingMapPool.Get()
	for i := 1; i <= posting.MidCap+1; i++ {
		m["name"] = m["name"].BuildAdded(uint64(i))
	}
	run := buildIndexStringRunFromPostingMap(m)

	state := newBuildIndexFieldState(false, false)
	state.runs = []buildIndexFieldRun{run}
	state.release()
	if state.runs != nil {
		t.Fatalf("expected runs to be cleared")
	}
}

func TestBuildIndexFieldStateReleaseClearsNils(t *testing.T) {
	state := newBuildIndexFieldState(false, false)
	for i := 1; i <= posting.MidCap+1; i++ {
		state.nils = state.nils.BuildAdded(uint64(i))
	}

	state.release()
	if !state.nils.IsEmpty() {
		t.Fatalf("expected nil postings to be cleared")
	}
}

func TestBuildIndexFieldStateReleaseClearsLenMap(t *testing.T) {
	state := newBuildIndexFieldState(false, true)
	for i := 1; i <= posting.MidCap+1; i++ {
		state.lenMap[3] = state.lenMap[3].BuildAdded(uint64(i))
	}

	state.release()
	if state.lenMap != nil {
		t.Fatalf("expected len map to be cleared")
	}
}

func TestUnionPostingListsOwnedMergesAddPosting(t *testing.T) {
	base := poolsSingleton(777)
	var add posting.List
	for i := 1; i <= posting.MidCap+1; i++ {
		add = add.BuildAdded(uint64(i))
	}

	merged := unionPostingListsOwned(base, add)
	defer merged.Release()
	if !merged.Contains(777) || !merged.Contains(1) || !merged.Contains(posting.MidCap+1) {
		t.Fatalf("merged posting list lost data")
	}
}
