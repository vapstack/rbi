package schema

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
)

func TestIndexStateCollectAndMaterialize(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	rec := schemaTestAccessorRec{
		Name:   "alice",
		Scores: []int64{5, 7, 5},
	}
	ptr := unsafe.Pointer(&rec)

	var stringState IndexState
	rt.IndexedByName["name"].CollectIndexValue(ptr, 1, &stringState)
	if !stringState.Changed() {
		t.Fatal("string overlay state not marked changed")
	}
	stringStorage := stringState.MaterializeStorage(false)
	defer stringStorage.Release()
	if !schemaTestIndexViewContains(stringStorage, "alice", 1) {
		t.Fatal("string overlay materialization lost posting")
	}

	var fixedState IndexState
	rt.IndexedByName["scores"].CollectIndexValue(ptr, 2, &fixedState)
	fixedStorage := fixedState.MaterializeStorage(true)
	defer fixedStorage.Release()
	if !schemaTestIndexViewContainsKey(fixedStorage, keycodec.OrderedInt64Key(7), 2) {
		t.Fatal("fixed overlay materialization lost posting")
	}
	lenStorage, _ := fixedState.MaterializeLenStorage((posting.List{}).BuildAdded(2))
	defer lenStorage.Release()
	if !schemaTestIndexViewContainsKey(lenStorage, 2, 2) {
		t.Fatal("len overlay materialization lost posting")
	}

	var nilState IndexState
	rt.IndexedByName["maybe"].CollectIndexValue(ptr, 3, &nilState)
	nilStorage := nilState.MaterializeNilStorage()
	defer nilStorage.Release()
	if !schemaTestIndexViewContains(nilStorage, indexdata.NilIndexEntryKey, 3) {
		t.Fatal("nil overlay materialization lost posting")
	}
}

func TestIndexStateMaterializedStorageSurvivesStateRelease(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	rec := schemaTestAccessorRec{
		Name:   "alice",
		Tags:   []string{"go", "db"},
		Scores: []int64{5, 7},
	}
	ptr := unsafe.Pointer(&rec)
	states := GetIndexStates(len(rt.Indexed))

	nameOrdinal := rt.IndexedByName["name"].Ordinal
	tagsOrdinal := rt.IndexedByName["tags"].Ordinal
	maybeOrdinal := rt.IndexedByName["maybe"].Ordinal
	rt.IndexedByName["name"].CollectIndexValue(ptr, 1, &states[nameOrdinal])
	rt.IndexedByName["tags"].CollectIndexValue(ptr, 1, &states[tagsOrdinal])
	rt.IndexedByName["maybe"].CollectIndexValue(ptr, 1, &states[maybeOrdinal])

	nameStorage := states[nameOrdinal].MaterializeStorage(false)
	defer nameStorage.Release()
	tagStorage := states[tagsOrdinal].MaterializeStorage(false)
	defer tagStorage.Release()
	nilStorage := states[maybeOrdinal].MaterializeNilStorage()
	defer nilStorage.Release()
	universe := (posting.List{}).BuildAdded(1)
	lenStorage, _ := states[tagsOrdinal].MaterializeLenStorage(universe)
	defer lenStorage.Release()
	defer universe.Release()

	ReleaseIndexStates(states)

	if !schemaTestIndexViewContains(nameStorage, "alice", 1) {
		t.Fatal("materialized scalar storage did not survive index state release")
	}
	if !schemaTestIndexViewContains(tagStorage, "go", 1) || !schemaTestIndexViewContains(tagStorage, "db", 1) {
		t.Fatal("materialized slice storage did not survive index state release")
	}
	if !schemaTestIndexViewContains(nilStorage, indexdata.NilIndexEntryKey, 1) {
		t.Fatal("materialized nil storage did not survive index state release")
	}
	if !schemaTestIndexViewContainsKey(lenStorage, 2, 1) {
		t.Fatal("materialized len storage did not survive index state release")
	}
}

func TestIndexStateReleaseDiscardsUnmaterializedPostingLists(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	rec := schemaTestAccessorRec{
		Name:   "alice",
		Scores: []int64{5, 7},
	}
	ptr := unsafe.Pointer(&rec)
	states := GetIndexStates(len(rt.Indexed))

	nameOrdinal := rt.IndexedByName["name"].Ordinal
	scoresOrdinal := rt.IndexedByName["scores"].Ordinal
	maybeOrdinal := rt.IndexedByName["maybe"].Ordinal
	for id := uint64(1); id <= 8; id++ {
		rt.IndexedByName["name"].CollectIndexValue(ptr, id, &states[nameOrdinal])
		rt.IndexedByName["scores"].CollectIndexValue(ptr, id, &states[scoresOrdinal])
		rt.IndexedByName["maybe"].CollectIndexValue(ptr, id, &states[maybeOrdinal])
	}

	ReleaseIndexStates(states)
	states = GetIndexStates(len(rt.Indexed))
	defer ReleaseIndexStates(states)

	if states[nameOrdinal].Changed() || states[nameOrdinal].index != nil {
		t.Fatalf("released string index state was not reset: %+v", states[nameOrdinal])
	}
	if states[scoresOrdinal].Changed() || states[scoresOrdinal].fixed != nil || states[scoresOrdinal].lengths != nil {
		t.Fatalf("released fixed index state was not reset: %+v", states[scoresOrdinal])
	}
	if states[maybeOrdinal].Changed() || states[maybeOrdinal].nils != nil {
		t.Fatalf("released nil index state was not reset: %+v", states[maybeOrdinal])
	}
}

func TestInsertStateCollectMergeResetAndHints(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	rec := schemaTestAccessorRec{
		Name:   "alice",
		Scores: []int64{5, 7, 5},
	}
	ptr := unsafe.Pointer(&rec)

	var state InsertState
	rt.IndexedByName["scores"].CollectInsertValue(ptr, 1, false, &state)
	if !state.Changed() {
		t.Fatal("insert state not marked changed")
	}
	storage := rt.IndexedByName["scores"].MergeInsertStorageOwned(indexdata.FieldStorage{}, &state, false)
	defer storage.Release()
	if !schemaTestIndexViewContainsKey(storage, keycodec.OrderedInt64Key(5), 1) {
		t.Fatal("insert merge lost fixed posting")
	}
	if diff := state.LenDiff(); diff == nil {
		t.Fatal("insert state missing len diff")
	}
	state.Reset()
	if state.Changed() {
		t.Fatal("insert reset left changed flag set")
	}
	state.discard()
	if state.Changed() || state.lengths != nil {
		t.Fatal("insert discard kept owned internals")
	}

	states := GetInsertStates(len(rt.Indexed))
	defer ReleaseInsertStates(states)
	prev := make([]indexdata.FieldStorage, len(rt.Indexed))
	nameOrdinal := rt.IndexedByName["name"].Ordinal
	prevMap := indexdata.GetPostingMap()
	prevMap["alice"] = (posting.List{}).BuildAdded(1)
	prev[nameOrdinal] = indexdata.NewRegularFieldStorageFromPostingMapOwned(prevMap)
	defer prev[nameOrdinal].Release()
	InitInsertStateHints(states, rt.Indexed, prev, nil, nil, 4)
	if states[nameOrdinal].indexHint != 4 {
		t.Fatalf("index hint=%d want 4", states[nameOrdinal].indexHint)
	}

	var stringState InsertState
	rt.IndexedByName["name"].CollectInsertValue(ptr, 11, false, &stringState)
	stringStorage := rt.IndexedByName["name"].MergeInsertStorageOwned(indexdata.FieldStorage{}, &stringState, false)
	defer stringStorage.Release()
	if !schemaTestIndexViewContains(stringStorage, "alice", 11) {
		t.Fatal("insert merge lost string posting")
	}
	stringState.discard()

	var nilState InsertState
	rt.IndexedByName["maybe"].CollectInsertValue(ptr, 12, false, &nilState)
	nilStorage := rt.IndexedByName["maybe"].MergeInsertNilStorageOwned(indexdata.FieldStorage{}, &nilState)
	defer nilStorage.Release()
	if !schemaTestIndexViewContains(nilStorage, indexdata.NilIndexEntryKey, 12) {
		t.Fatal("insert merge lost nil posting")
	}
	nilState.discard()
}

func TestInsertStateResetDropsPreviousLogicalEntries(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	oldRec := schemaTestAccessorRec{Tags: []string{"old"}}
	newRec := schemaTestAccessorRec{Tags: []string{"new", "next"}}

	var state InsertState
	rt.IndexedByName["tags"].CollectInsertValue(unsafe.Pointer(&oldRec), 1, false, &state)
	state.Reset()
	rt.IndexedByName["tags"].CollectInsertValue(unsafe.Pointer(&newRec), 2, false, &state)

	storage := rt.IndexedByName["tags"].MergeInsertStorageOwned(indexdata.FieldStorage{}, &state, false)
	defer storage.Release()
	lenStorage := (indexdata.FieldStorage{}).ApplyLenPostingDiff(state.LenDiff())
	defer lenStorage.Release()
	state.Reset()

	if schemaTestIndexViewContains(storage, "old", 1) {
		t.Fatal("insert state reset kept old string posting")
	}
	if !schemaTestIndexViewContains(storage, "new", 2) || !schemaTestIndexViewContains(storage, "next", 2) {
		t.Fatal("insert state reset lost new string postings")
	}
	if schemaTestIndexViewContainsKey(lenStorage, 1, 1) {
		t.Fatal("insert state reset kept old len posting")
	}
	if !schemaTestIndexViewContainsKey(lenStorage, 2, 2) {
		t.Fatal("insert state reset lost new len posting")
	}
	state.discard()
}

func TestInsertStateSliceCleanupDropsPreviousLogicalEntries(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	oldRec := schemaTestAccessorRec{Tags: []string{"old"}}
	newRec := schemaTestAccessorRec{Tags: []string{"new", "next"}}
	acc := rt.IndexedByName["tags"]
	states := make([]InsertState, len(rt.Indexed))
	state := &states[acc.Ordinal]

	acc.CollectInsertValue(unsafe.Pointer(&oldRec), 1, false, state)
	cleanupSnapshotFieldInsertStateSlice(states)
	if state.Changed() {
		t.Fatal("insert state cleanup kept changed flag")
	}
	acc.CollectInsertValue(unsafe.Pointer(&newRec), 2, false, state)

	storage := acc.MergeInsertStorageOwned(indexdata.FieldStorage{}, state, false)
	defer storage.Release()
	lenStorage := (indexdata.FieldStorage{}).ApplyLenPostingDiff(state.LenDiff())
	defer lenStorage.Release()
	state.Reset()

	if schemaTestIndexViewContains(storage, "old", 1) {
		t.Fatal("insert state cleanup kept old string posting")
	}
	if !schemaTestIndexViewContains(storage, "new", 2) || !schemaTestIndexViewContains(storage, "next", 2) {
		t.Fatal("insert state cleanup lost new string postings")
	}
	if schemaTestIndexViewContainsKey(lenStorage, 1, 1) {
		t.Fatal("insert state cleanup kept old len posting")
	}
	if !schemaTestIndexViewContainsKey(lenStorage, 2, 2) {
		t.Fatal("insert state cleanup lost new len posting")
	}
	state.discard()
}

func TestBatchStateCollectApplyReset(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	maybe := int64(5)
	oldRec := schemaTestAccessorRec{
		Name:   "alice",
		Age:    -7,
		Maybe:  &maybe,
		Tags:   []string{"go", "db"},
		Scores: []int64{5, 7},
	}
	newRec := schemaTestAccessorRec{
		Name:   "bob",
		Age:    9,
		Tags:   []string{"db", "search"},
		Scores: []int64{7, 9, 11},
	}
	oldPtr := unsafe.Pointer(&oldRec)
	newPtr := unsafe.Pointer(&newRec)

	var scalar BatchState
	rt.IndexedByName["name"].CollectBatchDiff(1, oldPtr, newPtr, false, &scalar)
	if !scalar.Changed() {
		t.Fatal("scalar batch state not marked changed")
	}
	scalarStorage := rt.IndexedByName["name"].ApplyBatchStorageOwned(indexdata.FieldStorage{}, &scalar, false)
	defer scalarStorage.Release()
	if !schemaTestIndexViewContains(scalarStorage, "bob", 1) {
		t.Fatal("scalar batch apply lost add posting")
	}
	scalar.Reset()
	if scalar.Changed() {
		t.Fatal("scalar batch reset left changed flag set")
	}

	var fixedScalar BatchState
	rt.IndexedByName["age"].CollectBatchDiff(4, oldPtr, newPtr, false, &fixedScalar)
	if !fixedScalar.Changed() {
		t.Fatal("fixed scalar batch state not marked changed")
	}
	fixedScalarStorage := rt.IndexedByName["age"].ApplyBatchStorageOwned(indexdata.FieldStorage{}, &fixedScalar, false)
	defer fixedScalarStorage.Release()
	if !schemaTestIndexViewContainsKey(fixedScalarStorage, keycodec.OrderedInt64Key(9), 4) {
		t.Fatal("fixed scalar batch apply lost add posting")
	}
	fixedScalar.Reset()

	var stringSlice BatchState
	rt.IndexedByName["tags"].CollectBatchDiff(5, oldPtr, newPtr, false, &stringSlice)
	if !stringSlice.Changed() {
		t.Fatal("string slice batch state not marked changed")
	}
	stringSliceStorage := rt.IndexedByName["tags"].ApplyBatchStorageOwned(indexdata.FieldStorage{}, &stringSlice, false)
	defer stringSliceStorage.Release()
	if !schemaTestIndexViewContains(stringSliceStorage, "search", 5) {
		t.Fatal("string slice batch apply lost add posting")
	}
	stringSlice.Reset()

	var slice BatchState
	rt.IndexedByName["scores"].CollectBatchDiff(2, oldPtr, newPtr, false, &slice)
	if !slice.Changed() {
		t.Fatal("slice batch state not marked changed")
	}
	sliceStorage := rt.IndexedByName["scores"].ApplyBatchStorageOwned(indexdata.FieldStorage{}, &slice, false)
	defer sliceStorage.Release()
	if !schemaTestIndexViewContainsKey(sliceStorage, keycodec.OrderedInt64Key(9), 2) {
		t.Fatal("slice batch apply lost fixed add posting")
	}
	if diff := slice.LenDiff(); diff == nil {
		t.Fatal("slice batch state missing len diff")
	}
	slice.discard()
	if slice.Changed() || slice.lengths != nil {
		t.Fatal("batch discard kept owned internals")
	}

	var nilDiff BatchState
	rt.IndexedByName["maybe"].CollectBatchDiff(6, oldPtr, newPtr, false, &nilDiff)
	if !nilDiff.Changed() {
		t.Fatal("nil batch state not marked changed")
	}
	nilStorage := rt.IndexedByName["maybe"].ApplyBatchNilStorageOwned(indexdata.FieldStorage{}, &nilDiff)
	defer nilStorage.Release()
	if !schemaTestIndexViewContains(nilStorage, indexdata.NilIndexEntryKey, 6) {
		t.Fatal("batch nil apply lost nil posting")
	}
	nilDiff.discard()
}

func TestBatchStateResetDropsPreviousLogicalEntries(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	oldRec := schemaTestAccessorRec{Tags: []string{"old"}}
	newRec := schemaTestAccessorRec{Tags: []string{"old", "stale"}}
	resetOld := schemaTestAccessorRec{Tags: []string{"drop", "other"}}
	resetNew := schemaTestAccessorRec{Tags: []string{"fresh"}}

	var state BatchState
	rt.IndexedByName["tags"].CollectBatchDiff(1, unsafe.Pointer(&oldRec), unsafe.Pointer(&newRec), false, &state)
	state.Reset()
	rt.IndexedByName["tags"].CollectBatchDiff(2, unsafe.Pointer(&resetOld), unsafe.Pointer(&resetNew), false, &state)

	storage := rt.IndexedByName["tags"].ApplyBatchStorageOwned(indexdata.FieldStorage{}, &state, false)
	defer storage.Release()
	lenStorage := (indexdata.FieldStorage{}).ApplyLenPostingDiff(state.LenDiff())
	defer lenStorage.Release()
	state.Reset()

	if schemaTestIndexViewContains(storage, "stale", 1) {
		t.Fatal("batch state reset kept old string diff")
	}
	if !schemaTestIndexViewContains(storage, "fresh", 2) {
		t.Fatal("batch state reset lost new string diff")
	}
	if schemaTestIndexViewContainsKey(lenStorage, 2, 1) {
		t.Fatal("batch state reset kept old len diff")
	}
	if !schemaTestIndexViewContainsKey(lenStorage, 1, 2) {
		t.Fatal("batch state reset lost new len diff")
	}
	state.discard()
}

func TestBatchStateSliceCleanupDropsPreviousLogicalEntries(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	oldRec := schemaTestAccessorRec{Tags: []string{"old"}}
	newRec := schemaTestAccessorRec{Tags: []string{"old", "stale"}}
	resetOld := schemaTestAccessorRec{Tags: []string{"drop", "other"}}
	resetNew := schemaTestAccessorRec{Tags: []string{"fresh"}}
	acc := rt.IndexedByName["tags"]
	states := make([]BatchState, len(rt.Indexed))
	state := &states[acc.Ordinal]

	acc.CollectBatchDiff(1, unsafe.Pointer(&oldRec), unsafe.Pointer(&newRec), false, state)
	cleanupSnapshotFieldBatchStateSlice(states)
	if state.Changed() || state.old.ok || state.new.ok || state.old.length != 0 || state.new.length != 0 {
		t.Fatalf("batch state cleanup kept logical state: %+v", state)
	}
	acc.CollectBatchDiff(2, unsafe.Pointer(&resetOld), unsafe.Pointer(&resetNew), false, state)

	storage := acc.ApplyBatchStorageOwned(indexdata.FieldStorage{}, state, false)
	defer storage.Release()
	lenStorage := (indexdata.FieldStorage{}).ApplyLenPostingDiff(state.LenDiff())
	defer lenStorage.Release()
	state.Reset()

	if schemaTestIndexViewContains(storage, "stale", 1) {
		t.Fatal("batch state cleanup kept old string diff")
	}
	if !schemaTestIndexViewContains(storage, "fresh", 2) {
		t.Fatal("batch state cleanup lost new string diff")
	}
	if schemaTestIndexViewContainsKey(lenStorage, 2, 1) {
		t.Fatal("batch state cleanup kept old len diff")
	}
	if !schemaTestIndexViewContainsKey(lenStorage, 1, 2) {
		t.Fatal("batch state cleanup lost new len diff")
	}
	state.discard()
}

func TestWriteScratchResetAndDiscardClearLogicalState(t *testing.T) {
	var scratch WriteScratch
	scratch.addString("old")
	scratch.addFixed(7)
	scratch.setLen(3)
	scratch.setNil()
	scratch.reset()
	if scratch.ok || scratch.isNil || scratch.length != 0 || len(scratch.strings) != 0 || len(scratch.fixed) != 0 {
		t.Fatalf("reset scratch kept logical state: %+v", scratch)
	}
	scratch.addString("next")
	scratch.addFixed(9)
	scratch.setLen(2)
	scratch.discard()
	if scratch.ok || scratch.isNil || scratch.length != 0 || scratch.strings != nil || scratch.fixed != nil {
		t.Fatalf("discard scratch kept owned buffers: %+v", scratch)
	}
}

func TestBatchStateReorderedSliceValuesProduceNoDeltas(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	oldRec := schemaTestAccessorRec{
		Tags:   []string{"go", "db", "go"},
		Scores: []int64{3, 1, 3},
	}
	newRec := schemaTestAccessorRec{
		Tags:   []string{"db", "go"},
		Scores: []int64{1, 3},
	}
	oldPtr := unsafe.Pointer(&oldRec)
	newPtr := unsafe.Pointer(&newRec)

	for _, name := range []string{"tags", "scores"} {
		var state BatchState
		rt.IndexedByName[name].CollectBatchDiff(1, oldPtr, newPtr, false, &state)
		if state.Changed() {
			state.Reset()
			t.Fatalf("%s: reorder-only diff unexpectedly marked field as changed", name)
		}
		state.Reset()
	}
}

func TestBatchStateSlicePoolLifecycle(t *testing.T) {
	states := GetBatchStates(3)
	if len(states) != 3 {
		t.Fatalf("GetBatchStates len=%d want 3", len(states))
	}
	states[0].changed = true
	ReleaseBatchStates(states)
}
