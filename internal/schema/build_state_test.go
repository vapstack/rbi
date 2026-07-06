package schema

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
)

func buildStateSingleton(id uint64) posting.List {
	return (posting.List{}).BuildAdded(id)
}

func TestBuildFieldLocalStateOwnsStringKeyUntilFlush(t *testing.T) {
	buf := []byte("mutable-name")
	state := NewBuildFieldLocalState(false, false, BuildFieldRunTargetEntries)
	state.addValue(unsafe.String(unsafe.SliceData(buf), len(buf)), 11)

	for i := range buf {
		buf[i] = 0xa5
	}

	dst := NewBuildFieldState(false)
	state.FlushAllInto(dst)
	state.Release()

	storage := dst.MaterializeStorage()
	defer storage.Release()

	ids := indexdata.NewFieldIndexViewFromStorage(storage).LookupPostingRetained("mutable-name")
	if !ids.Contains(11) {
		t.Fatalf("materialized storage lost string key after source buffer poison")
	}
	ids.Release()
}

func TestBuildFieldLocalStateReleaseClearsVals(t *testing.T) {
	state := NewBuildFieldLocalState(false, false, BuildFieldRunTargetEntries)
	for i := 1; i <= posting.MidCap+1; i++ {
		state.addValue("name", uint64(i))
	}

	state.Release()
	if state.vals != nil {
		t.Fatalf("expected vals map to be cleared")
	}
}

func TestBuildFieldLocalStateReleaseClearsFixed(t *testing.T) {
	state := NewBuildFieldLocalState(true, false, BuildFieldRunTargetEntries)
	for i := 1; i <= posting.MidCap+1; i++ {
		state.addFixedValue(42, uint64(i))
	}

	state.Release()
	if state.fixed != nil {
		t.Fatalf("expected fixed map to be cleared")
	}
}

func TestBuildFieldLocalStateReleaseClearsLenMap(t *testing.T) {
	state := NewBuildFieldLocalState(false, true, BuildFieldRunTargetEntries)
	for i := 1; i <= posting.MidCap+1; i++ {
		state.addLen(3, uint64(i))
	}

	state.Release()
	if state.lenMap != nil {
		t.Fatalf("expected len map to be cleared")
	}
}

func TestBuildFieldLocalStateReleaseClearsNils(t *testing.T) {
	state := NewBuildFieldLocalState(false, false, BuildFieldRunTargetEntries)
	for i := 1; i <= posting.MidCap+1; i++ {
		state.addNil(uint64(i))
	}

	state.Release()
	if !state.nils.IsEmpty() {
		t.Fatalf("expected nil postings to be cleared")
	}
}

func TestBuildFieldLocalStateFlushAllIntoMergesNilSource(t *testing.T) {
	state := NewBuildFieldLocalState(false, false, BuildFieldRunTargetEntries)
	for i := 1; i <= posting.MidCap+1; i++ {
		state.addNil(uint64(i))
	}

	dst := NewBuildFieldState(false)
	dst.nils = buildStateSingleton(777)

	state.FlushAllInto(dst)
	if !dst.nils.Contains(777) || !dst.nils.Contains(1) || !dst.nils.Contains(posting.MidCap+1) {
		t.Fatalf("merged nil postings lost data")
	}
	if !state.nils.IsEmpty() {
		t.Fatalf("expected source nil postings to be drained")
	}
}

func TestBuildFieldLocalStateFlushAllIntoMergesLenSource(t *testing.T) {
	state := NewBuildFieldLocalState(false, true, BuildFieldRunTargetEntries)
	for i := 1; i <= posting.MidCap+1; i++ {
		state.addLen(3, uint64(i))
	}

	dst := NewBuildFieldState(true)
	dst.lenMap[3] = buildStateSingleton(888)

	state.FlushAllInto(dst)
	if !dst.lenMap[3].Contains(888) || !dst.lenMap[3].Contains(1) || !dst.lenMap[3].Contains(posting.MidCap+1) {
		t.Fatalf("merged len postings lost data")
	}
	if len(state.lenMap) != 0 {
		t.Fatalf("expected source len map to be drained")
	}
}

func TestBuildFieldLocalStateFlushAllIntoDropsOversizedLenMap(t *testing.T) {
	state := NewBuildFieldLocalState(false, true, BuildFieldRunTargetEntries)
	for i := 0; i <= indexdata.LenPostingMapMaxRetainedLen; i++ {
		state.addLen(i, uint64(i+1))
	}

	dst := NewBuildFieldState(true)
	state.FlushAllInto(dst)
	defer dst.Release()

	if state.lenMap != nil {
		t.Fatalf("expected oversized source len map to be detached")
	}
}

func TestBuildFieldLocalStateUsesRunTarget(t *testing.T) {
	stringState := NewBuildFieldLocalState(false, false, 3)
	stringState.addValue("a", 1)
	stringState.addValue("b", 2)
	if stringState.ShouldFlushRegular() {
		t.Fatalf("string state flushed before run target")
	}
	stringState.addValue("c", 3)
	if !stringState.ShouldFlushRegular() {
		t.Fatalf("string state did not flush at run target")
	}
	stringState.Release()

	fixedState := NewBuildFieldLocalState(true, false, 2)
	fixedState.addFixedValue(1, 1)
	if fixedState.ShouldFlushRegular() {
		t.Fatalf("fixed state flushed before run target")
	}
	fixedState.addFixedValue(2, 2)
	if !fixedState.ShouldFlushRegular() {
		t.Fatalf("fixed state did not flush at run target")
	}
	fixedState.Release()
}

func TestBuildFieldLocalStateFlushesByPostingAdds(t *testing.T) {
	if !buildFieldFlushByPostingAdds {
		t.Skip("posting-add flush disabled")
	}

	state := NewBuildFieldLocalState(false, false, 2)
	defer state.Release()

	target := 2 * buildFieldRunTargetPostingAddMultiplier
	for i := 1; i < target; i++ {
		state.addValue("hot", uint64(i))
	}
	if state.ShouldFlushRegular() {
		t.Fatalf("state flushed before posting-add target")
	}
	state.addValue("hot", uint64(target))
	if !state.ShouldFlushRegular() {
		t.Fatalf("state did not flush at posting-add target")
	}

	dst := NewBuildFieldState(false)
	defer dst.Release()
	state.FlushRegularInto(dst)
	if state.regularAdds != 0 {
		t.Fatalf("regularAdds=%d want 0 after flush", state.regularAdds)
	}
	if state.ShouldFlushRegular() {
		t.Fatalf("state still wants regular flush after flush")
	}
}

func TestBuildFieldStateMaterializeStorageDetachesRuns(t *testing.T) {
	state := NewBuildFieldState(false)
	state.runs = []indexdata.FieldStorageRun{{}}

	storage := state.MaterializeStorage()
	defer storage.Release()
	if len(state.runs) != 0 {
		t.Fatalf("expected runs to be detached during materialization")
	}
}

func TestBuildFieldStateMaterializeStatsCountsRunKeys(t *testing.T) {
	left := indexdata.GetPostingMap()
	ids := posting.List{}
	ids = ids.BuildAdded(1)
	ids = ids.BuildAdded(2)
	left["a"] = ids
	left["b"] = buildStateSingleton(3)
	runLeft := indexdata.NewStringFieldStorageRunFromPostingMap(left)
	indexdata.ReleasePostingMap(left)

	right := indexdata.GetPostingMap()
	ids = posting.List{}
	ids = ids.BuildAdded(4)
	ids = ids.BuildAdded(5)
	ids = ids.BuildAdded(6)
	right["c"] = ids
	runRight := indexdata.NewStringFieldStorageRunFromPostingMap(right)
	indexdata.ReleasePostingMap(right)

	state := NewBuildFieldState(false)
	state.runs = []indexdata.FieldStorageRun{runLeft, runRight}
	defer state.Release()

	keys, work := state.MaterializeStats()
	if keys != 3 {
		t.Fatalf("keys=%d want 3", keys)
	}
	if work != 3 {
		t.Fatalf("work=%d want 3", work)
	}
}

func TestBuildFieldStateReleaseClearsRuns(t *testing.T) {
	state := NewBuildFieldState(false)
	state.runs = []indexdata.FieldStorageRun{{}}
	state.Release()
	if state.runs != nil {
		t.Fatalf("expected runs to be cleared")
	}
}

func TestBuildFieldStateReleaseClearsNils(t *testing.T) {
	state := NewBuildFieldState(false)
	for i := 1; i <= posting.MidCap+1; i++ {
		state.nils = state.nils.BuildAdded(uint64(i))
	}

	state.Release()
	if !state.nils.IsEmpty() {
		t.Fatalf("expected nil postings to be cleared")
	}
}

func TestBuildFieldStateReleaseClearsLenMap(t *testing.T) {
	state := NewBuildFieldState(true)
	for i := 1; i <= posting.MidCap+1; i++ {
		state.lenMap[3] = state.lenMap[3].BuildAdded(uint64(i))
	}

	state.Release()
	if state.lenMap != nil {
		t.Fatalf("expected len map to be cleared")
	}
}

func TestBuildFieldStateHotPaths(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	rec := schemaTestAccessorRec{
		Name:   "alice",
		Scores: []int64{5, 7, 5},
	}
	ptr := unsafe.Pointer(&rec)

	stringLocal := NewBuildFieldLocalState(false, false, BuildFieldRunTargetEntries)
	rt.IndexedByName["name"].WriteBuild(ptr, BuildSink{State: &stringLocal, Idx: 1})
	stringState := NewBuildFieldState(false)
	stringLocal.FlushRegularInto(stringState)
	stringStorage := stringState.MaterializeStorage()
	defer stringStorage.Release()
	if !schemaTestIndexViewContains(stringStorage, "alice", 1) {
		t.Fatal("string build materialization lost posting")
	}

	fixedLocal := NewBuildFieldLocalState(true, true, BuildFieldRunTargetEntries)
	rt.IndexedByName["scores"].WriteBuild(ptr, BuildSink{State: &fixedLocal, Idx: 2})
	fixedState := NewBuildFieldState(true)
	fixedLocal.FlushAllInto(fixedState)
	fixedStorage := fixedState.MaterializeStorage()
	defer fixedStorage.Release()
	if !schemaTestIndexViewContainsKey(fixedStorage, keycodec.OrderedInt64Key(5), 2) {
		t.Fatal("fixed build materialization lost posting")
	}
	universe := (posting.List{}).BuildAdded(2)
	lenStorage, _ := fixedState.MaterializeLenStorage(universe)
	defer lenStorage.Release()
	if !schemaTestIndexViewContainsKey(lenStorage, 2, 2) {
		t.Fatal("len build materialization lost posting")
	}

	nilLocal := NewBuildFieldLocalState(true, false, BuildFieldRunTargetEntries)
	rt.IndexedByName["maybe"].WriteBuild(ptr, BuildSink{State: &nilLocal, Idx: 3})
	nilState := NewBuildFieldState(false)
	nilLocal.FlushAllInto(nilState)
	nilStorage := nilState.MaterializeNilStorage()
	defer nilStorage.Release()
	if !schemaTestIndexViewContains(nilStorage, indexdata.NilIndexEntryKey, 3) {
		t.Fatal("nil build materialization lost posting")
	}

	flushLocal := NewBuildFieldLocalState(false, false, BuildFieldRunTargetEntries)
	for i := 0; i < BuildFieldRunTargetEntries; i++ {
		flushLocal.addValue(keycodec.U64ByteString(uint64(i)), uint64(i+10))
	}
	if !flushLocal.ShouldFlushRegular() {
		t.Fatal("string local state should be ready to flush")
	}
	flushFixed := NewBuildFieldLocalState(true, false, BuildFieldRunTargetEntries)
	for i := 0; i < BuildFieldRunTargetEntries; i++ {
		flushFixed.addFixedValue(uint64(i), uint64(i+10))
	}
	if !flushFixed.ShouldFlushRegular() {
		t.Fatal("fixed local state should be ready to flush")
	}
}
