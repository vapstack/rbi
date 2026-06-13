package snapshot

import (
	"reflect"
	"slices"
	"testing"
	"unsafe"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/schema"
)

type snapshotBatchRec struct {
	Name  string  `rbi:"index"`
	Age   int     `rbi:"index"`
	Score float64 `rbi:"measure"`
}

type snapshotBatchStorageRec struct {
	Name string   `rbi:"index"`
	Tags []string `rbi:"index"`
	Opt  *string  `rbi:"index"`
}

func snapshotBatchStorageRuntime(t *testing.T) *schema.Schema {
	t.Helper()
	rt, err := schema.Compile(reflect.TypeOf(snapshotBatchStorageRec{}), schema.Config{})
	if err != nil {
		t.Fatal(err)
	}
	return rt
}

func snapshotBatchFieldContains(s *View, field, key string, id uint64) bool {
	ids := testFieldLookupPostingRetained(s, field, key)
	ok := ids.Contains(id)
	ids.Release()
	return ok
}

func snapshotBatchNilContains(s *View, field string, id uint64) bool {
	acc := s.IndexedFieldByName[field]
	ids := indexdata.NewFieldIndexViewFromStorage(s.NilIndex[acc.Ordinal]).LookupPostingRetained(indexdata.NilIndexEntryKey)
	ok := ids.Contains(id)
	ids.Release()
	return ok
}

func snapshotBatchLenContains(s *View, field string, ln uint64, id uint64) bool {
	acc := s.IndexedFieldByName[field]
	ids := indexdata.NewFieldIndexViewFromStorage(s.LenIndex[acc.Ordinal]).LookupPostingRetainedKey(keycodec.FromU64(ln))
	ok := ids.Contains(id)
	ids.Release()
	return ok
}

func snapshotBatchKeyStorage(key string, id uint64) indexdata.FieldStorage {
	m := indexdata.GetPostingMap()
	m[key] = (posting.List{}).BuildAdded(id)
	return indexdata.NewRegularFieldStorageFromPostingMapOwned(m)
}

func assertSnapshotEmptyForSchema(t testing.TB, snap *View, s *schema.Schema) {
	t.Helper()
	if !snap.Universe.IsEmpty() {
		t.Fatalf("universe cardinality=%d want 0", snap.Universe.Cardinality())
	}
	if snap.universeOwner == nil {
		t.Fatal("expected empty snapshot to own universe")
	}
	if snap.KeyIndex.KeyCount() != 0 {
		t.Fatalf("key index key count=%d want 0", snap.KeyIndex.KeyCount())
	}
	if got, want := len(snap.Index), len(s.Indexed); got != want {
		t.Fatalf("index slots=%d want %d", got, want)
	}
	if got, want := len(snap.NilIndex), len(s.Indexed); got != want {
		t.Fatalf("nil index slots=%d want %d", got, want)
	}
	if got, want := len(snap.LenIndex), len(s.Indexed); got != want {
		t.Fatalf("len index slots=%d want %d", got, want)
	}
	if got, want := len(snap.LenZeroComplement), len(s.Indexed); got != want {
		t.Fatalf("len zero-complement slots=%d want %d", got, want)
	}
	if got, want := len(snap.Measure), len(s.Measures); got != want {
		t.Fatalf("measure slots=%d want %d", got, want)
	}
}

func TestBuildPreparedFromEmptyBaseOwnsUniverse(t *testing.T) {
	var rec struct{}
	snap := Build(1, nil, &schema.Schema{}, CacheConfig{}, nil, []BatchEntry{
		{ID: 7, New: unsafe.Pointer(&rec)},
	})
	defer snap.releaseRuntimeCaches()
	defer snap.releaseStorage()

	if snap.universeOwner == nil {
		t.Fatal("expected from-empty prepared snapshot to own universe")
	}
	if !snap.Universe.Contains(7) {
		t.Fatal("expected prepared snapshot universe to contain inserted id")
	}
}

func TestBuildPreparedEmptyNilBaseBuildsEmptySnapshot(t *testing.T) {
	rt := snapshotBatchStorageRuntime(t)
	snap := Build(1, nil, rt, CacheConfig{}, rt.Patch.Fields, nil)
	defer snap.releaseRuntimeCaches()
	defer snap.releaseStorage()

	assertSnapshotEmptyForSchema(t, snap, rt)
}

func TestBuildPreparedNilBaseNoopBuildsEmptySnapshot(t *testing.T) {
	rt := snapshotBatchStorageRuntime(t)
	rec := snapshotBatchStorageRec{Name: "alice", Tags: []string{"red"}}

	snap := Build(1, nil, rt, CacheConfig{}, rt.Patch.Fields, []BatchEntry{
		{ID: 7, New: unsafe.Pointer(&rec)},
		{ID: 7, Old: unsafe.Pointer(&rec)},
	})
	defer snap.releaseRuntimeCaches()
	defer snap.releaseStorage()

	assertSnapshotEmptyForSchema(t, snap, rt)

	snapInPlace := BuildInPlace(2, nil, rt, CacheConfig{}, rt.Patch.Fields, []BatchEntry{
		{ID: 7, New: unsafe.Pointer(&rec)},
		{ID: 7, Old: unsafe.Pointer(&rec)},
	})
	defer snapInPlace.releaseRuntimeCaches()
	defer snapInPlace.releaseStorage()

	assertSnapshotEmptyForSchema(t, snapInPlace, rt)
}

func TestBuildPreparedAppliesKeyDeltas(t *testing.T) {
	var rec struct{}
	snap := BuildWithKeyDeltas(1, nil, &schema.Schema{}, CacheConfig{}, nil, []BatchEntry{
		{ID: 7, New: unsafe.Pointer(&rec)},
	}, []KeyDelta{
		{ID: 7, Key: "user-7", Add: true},
	})
	defer snap.releaseRuntimeCaches()
	defer snap.releaseStorage()

	ids := indexdata.NewFieldIndexViewFromStorage(snap.KeyIndex).LookupPostingRetained("user-7")
	if ids.Cardinality() != 1 || !ids.Contains(7) {
		t.Fatalf("key index posting cardinality=%d contains=%v", ids.Cardinality(), ids.Contains(7))
	}
	ids.Release()
}

func TestBuildPreparedKeyDeltasReplaceDurableID(t *testing.T) {
	var oldRec, newRec struct{}
	keyStorage := snapshotBatchKeyStorage("user", 1)
	universe := (posting.List{}).BuildAdded(1)
	universe = universe.BuildAdded(3)
	prev := NewView(1, nil, &schema.Schema{}, CacheConfig{}, Storage{
		KeyIndex: keyStorage,
		Universe: universe,
	})
	defer prev.releaseRuntimeCaches()
	defer prev.releaseStorage()

	next := BuildWithKeyDeltas(2, prev, &schema.Schema{}, CacheConfig{}, nil, []BatchEntry{
		{ID: 1, Old: unsafe.Pointer(&oldRec)},
		{ID: 3, New: unsafe.Pointer(&newRec)},
	}, []KeyDelta{
		{ID: 1, Key: "user"},
		{ID: 3, Key: "user", Add: true},
	})
	defer next.releaseRuntimeCaches()
	defer next.releaseStorage()

	ids := indexdata.NewFieldIndexViewFromStorage(next.KeyIndex).LookupPostingRetained("user")
	if ids.Cardinality() != 1 || ids.Contains(1) || !ids.Contains(3) {
		t.Fatalf("key index replacement cardinality=%d old=%v new=%v", ids.Cardinality(), ids.Contains(1), ids.Contains(3))
	}
	ids.Release()
}

func TestBuildPreparedKeyDeltasLastOperationWinsForSameKeyID(t *testing.T) {
	var rec struct{}
	base := NewView(0, nil, &schema.Schema{}, CacheConfig{}, Storage{})
	defer base.releaseRuntimeCaches()
	defer base.releaseStorage()

	snap := BuildWithKeyDeltas(1, base, &schema.Schema{}, CacheConfig{}, nil, []BatchEntry{
		{ID: 7, New: unsafe.Pointer(&rec)},
		{ID: 7, Old: unsafe.Pointer(&rec)},
	}, []KeyDelta{
		{ID: 7, Key: "user", Add: true},
		{ID: 7, Key: "user"},
	})
	defer snap.releaseRuntimeCaches()
	defer snap.releaseStorage()

	ids := indexdata.NewFieldIndexViewFromStorage(snap.KeyIndex).LookupPostingRetained("user")
	if !ids.IsEmpty() {
		t.Fatalf("add then remove key delta left posting cardinality=%d", ids.Cardinality())
	}
	ids.Release()

	keyStorage := snapshotBatchKeyStorage("user", 7)
	prev := NewView(1, nil, &schema.Schema{}, CacheConfig{}, Storage{
		KeyIndex: keyStorage,
		Universe: posting.BuildFromSorted([]uint64{
			7,
		}),
	})
	defer prev.releaseRuntimeCaches()
	defer prev.releaseStorage()

	next := BuildWithKeyDeltas(2, prev, &schema.Schema{}, CacheConfig{}, nil, []BatchEntry{
		{ID: 7, Old: unsafe.Pointer(&rec)},
		{ID: 7, New: unsafe.Pointer(&rec)},
	}, []KeyDelta{
		{ID: 7, Key: "user"},
		{ID: 7, Key: "user", Add: true},
	})
	defer next.releaseRuntimeCaches()
	defer next.releaseStorage()

	ids = indexdata.NewFieldIndexViewFromStorage(next.KeyIndex).LookupPostingRetained("user")
	if ids.Cardinality() != 1 || !ids.Contains(7) {
		t.Fatalf("remove then add key delta cardinality=%d contains=%v", ids.Cardinality(), ids.Contains(7))
	}
	ids.Release()
}

func TestBuildPreparedFullReplaceRebuildsStorage(t *testing.T) {
	rt, err := schema.Compile(reflect.TypeOf(snapshotBatchRec{}), schema.Config{})
	if err != nil {
		t.Fatal(err)
	}
	oldRecords := []snapshotBatchRec{{Name: "old-a", Age: 10, Score: 1}, {Name: "old-b", Age: 20, Score: 2}}
	prev := Build(1, nil, rt, CacheConfig{}, rt.Patch.Fields, []BatchEntry{
		{ID: 1, New: unsafe.Pointer(&oldRecords[0])},
		{ID: 2, New: unsafe.Pointer(&oldRecords[1])},
	})
	defer prev.releaseRuntimeCaches()
	defer prev.releaseStorage()

	newRecords := []snapshotBatchRec{{Name: "new-a", Age: 30, Score: 3}, {Name: "new-b", Age: 40, Score: 4}}
	next := Build(2, prev, rt, CacheConfig{}, rt.Patch.Fields, []BatchEntry{
		{ID: 1, Old: unsafe.Pointer(&oldRecords[0]), New: unsafe.Pointer(&newRecords[0])},
		{ID: 2, Old: unsafe.Pointer(&oldRecords[1]), New: unsafe.Pointer(&newRecords[1])},
	})
	defer next.releaseRuntimeCaches()
	defer next.releaseStorage()

	if got := next.Universe.Cardinality(); got != 2 {
		t.Fatalf("universe cardinality=%d want 2", got)
	}
	ids := testFieldLookupPostingRetained(next, "Name", "new-a")
	if !ids.Contains(1) {
		t.Fatal("expected rebuilt snapshot to contain new-a id")
	}
	ids.Release()
	ids = testFieldLookupPostingRetained(next, "Name", "old-a")
	if !ids.IsEmpty() {
		ids.Release()
		t.Fatal("expected rebuilt snapshot to drop old-a id")
	}
}

func TestBuildPreparedDeleteAllBuildsEmptySnapshot(t *testing.T) {
	rt, err := schema.Compile(reflect.TypeOf(snapshotBatchRec{}), schema.Config{})
	if err != nil {
		t.Fatal(err)
	}
	oldRecords := []snapshotBatchRec{{Name: "old-a", Age: 10, Score: 1}, {Name: "old-b", Age: 20, Score: 2}}
	prev := Build(1, nil, rt, CacheConfig{}, rt.Patch.Fields, []BatchEntry{
		{ID: 1, New: unsafe.Pointer(&oldRecords[0])},
		{ID: 2, New: unsafe.Pointer(&oldRecords[1])},
	})
	defer prev.releaseRuntimeCaches()
	defer prev.releaseStorage()

	next := Build(2, prev, rt, CacheConfig{}, rt.Patch.Fields, []BatchEntry{
		{ID: 1, Old: unsafe.Pointer(&oldRecords[0])},
		{ID: 2, Old: unsafe.Pointer(&oldRecords[1])},
	})
	defer next.releaseRuntimeCaches()
	defer next.releaseStorage()

	if !next.Universe.IsEmpty() {
		t.Fatal("expected delete-all snapshot to have empty universe")
	}
	ids := testFieldLookupPostingRetained(next, "Name", "old-a")
	if !ids.IsEmpty() {
		ids.Release()
		t.Fatal("expected delete-all snapshot to drop index storage")
	}
}

func TestBuildPreparedDoesNotMutateCallerEntries(t *testing.T) {
	rt := snapshotBatchStorageRuntime(t)
	oldRec := snapshotBatchStorageRec{Name: "bob", Tags: []string{"yellow"}}
	prev := Build(1, nil, rt, CacheConfig{}, rt.Patch.Fields, []BatchEntry{
		{ID: 1, New: unsafe.Pointer(&oldRec)},
	})
	defer prev.releaseRuntimeCaches()
	defer prev.releaseStorage()

	midRec := snapshotBatchStorageRec{Name: "bob", Tags: []string{"green"}}
	newRec := snapshotBatchStorageRec{Name: "bobby", Tags: []string{"green", "blue"}}
	entries := []BatchEntry{
		{
			ID:        1,
			Old:       unsafe.Pointer(&oldRec),
			New:       unsafe.Pointer(&midRec),
			Patch:     []schema.PatchItem{{Name: "Tags", Value: midRec.Tags}},
			PatchOnly: true,
		},
		{ID: 2},
		{
			ID:        1,
			Old:       unsafe.Pointer(&midRec),
			New:       unsafe.Pointer(&newRec),
			Patch:     []schema.PatchItem{{Name: "Name", Value: newRec.Name}, {Name: "Tags", Value: newRec.Tags}},
			PatchOnly: true,
		},
	}
	original := make([]BatchEntry, len(entries))
	copy(original, entries)

	next := Build(2, prev, rt, CacheConfig{}, rt.Patch.Fields, entries)
	defer next.releaseRuntimeCaches()
	defer next.releaseStorage()

	if !reflect.DeepEqual(entries, original) {
		t.Fatalf("BuildPrepared mutated caller entries:\n got=%#v\nwant=%#v", entries, original)
	}
	if snapshotBatchFieldContains(next, "Name", "bob", 1) {
		t.Fatal("repeated-id snapshot kept stale scalar posting")
	}
	if !snapshotBatchFieldContains(next, "Name", "bobby", 1) {
		t.Fatal("repeated-id snapshot is missing updated scalar posting")
	}
}

func TestBuildPreparedKeepsPreviousSnapshotStorageImmutableAcrossUpdateAndDelete(t *testing.T) {
	rt := snapshotBatchStorageRuntime(t)
	opt := "zzz"
	oldRecords := []snapshotBatchStorageRec{
		{Name: "bob", Tags: []string{"go", "db"}},
		{Name: "alice", Tags: []string{"go"}},
	}
	prev := Build(1, nil, rt, CacheConfig{}, rt.Patch.Fields, []BatchEntry{
		{ID: 1, New: unsafe.Pointer(&oldRecords[0])},
		{ID: 2, New: unsafe.Pointer(&oldRecords[1])},
	})
	defer prev.releaseRuntimeCaches()
	defer prev.releaseStorage()

	newRec := snapshotBatchStorageRec{Name: "charlie", Tags: []string{"go"}, Opt: &opt}
	next := Build(2, prev, rt, CacheConfig{}, rt.Patch.Fields, []BatchEntry{
		{ID: 1, Old: unsafe.Pointer(&oldRecords[0]), New: unsafe.Pointer(&newRec)},
		{ID: 2, Old: unsafe.Pointer(&oldRecords[1])},
	})
	defer next.releaseRuntimeCaches()
	defer next.releaseStorage()

	if !snapshotBatchFieldContains(prev, "Name", "bob", 1) {
		t.Fatal("previous snapshot lost original scalar posting")
	}
	if snapshotBatchFieldContains(prev, "Name", "charlie", 1) {
		t.Fatal("previous snapshot sees updated scalar posting")
	}
	if !snapshotBatchFieldContains(next, "Name", "charlie", 1) {
		t.Fatal("next snapshot is missing updated scalar posting")
	}
	if snapshotBatchFieldContains(next, "Name", "bob", 1) {
		t.Fatal("next snapshot kept stale scalar posting")
	}
	if !snapshotBatchLenContains(prev, "Tags", 2, 1) {
		t.Fatal("previous snapshot lost original len posting")
	}
	if snapshotBatchLenContains(prev, "Tags", 1, 1) {
		t.Fatal("previous snapshot sees updated len posting")
	}
	if !snapshotBatchLenContains(next, "Tags", 1, 1) {
		t.Fatal("next snapshot is missing updated len posting")
	}
	if snapshotBatchLenContains(next, "Tags", 2, 1) {
		t.Fatal("next snapshot kept stale len posting")
	}
	if !snapshotBatchNilContains(prev, "Opt", 1) {
		t.Fatal("previous snapshot lost nil-index membership")
	}
	if snapshotBatchNilContains(next, "Opt", 1) {
		t.Fatal("next snapshot kept stale nil-index membership")
	}
	if !snapshotBatchFieldContains(next, "Opt", opt, 1) {
		t.Fatal("next snapshot is missing updated opt posting")
	}
	if !prev.Universe.Contains(2) || prev.Universe.Cardinality() != 2 {
		t.Fatal("previous snapshot universe changed after delete")
	}
	if next.Universe.Contains(2) || next.Universe.Cardinality() != 1 {
		t.Fatal("next snapshot universe is inconsistent after delete")
	}
}

func TestBuildPreparedKeepsPreviousZeroComplementLenIndexImmutableAcrossEmptyUpdate(t *testing.T) {
	rt := snapshotBatchStorageRuntime(t)
	oldRecords := []snapshotBatchStorageRec{
		{Name: "u1", Tags: []string{"go"}},
		{Name: "u2"},
		{Name: "u3"},
		{Name: "u4"},
	}
	prev := Build(1, nil, rt, CacheConfig{}, rt.Patch.Fields, []BatchEntry{
		{ID: 1, New: unsafe.Pointer(&oldRecords[0])},
		{ID: 2, New: unsafe.Pointer(&oldRecords[1])},
		{ID: 3, New: unsafe.Pointer(&oldRecords[2])},
		{ID: 4, New: unsafe.Pointer(&oldRecords[3])},
	})
	defer prev.releaseRuntimeCaches()
	defer prev.releaseStorage()

	acc := prev.IndexedFieldByName["Tags"]
	if !prev.LenZeroComplement[acc.Ordinal] {
		t.Fatal("expected previous snapshot to use zero-complement len index")
	}
	if !snapshotBatchLenContains(prev, "Tags", 1, 1) {
		t.Fatal("previous snapshot len=1 posting is missing id=1")
	}

	newRec := snapshotBatchStorageRec{Name: "u1", Tags: []string{}}
	next := Build(2, prev, rt, CacheConfig{}, rt.Patch.Fields, []BatchEntry{
		{ID: 1, Old: unsafe.Pointer(&oldRecords[0]), New: unsafe.Pointer(&newRec)},
	})
	defer next.releaseRuntimeCaches()
	defer next.releaseStorage()

	if !snapshotBatchLenContains(prev, "Tags", 1, 1) {
		t.Fatal("previous snapshot lost len=1 posting")
	}
	if snapshotBatchLenContains(next, "Tags", 1, 1) {
		t.Fatal("next snapshot kept stale len=1 posting")
	}
}

// Build views must own or retain all storage they expose before the
// caller can retire the predecessor view.
func TestBuildPreparedImmediateRetireChainKeepsCurrentSnapshotOwned(t *testing.T) {
	rt := snapshotBatchStorageRuntime(t)
	cfg := CacheConfig{}

	opt2 := "opt2"
	rec1 := snapshotBatchStorageRec{Name: "alice", Tags: []string{"red"}}
	rec2 := snapshotBatchStorageRec{Name: "bob", Tags: []string{"green", "yellow"}, Opt: &opt2}
	current := Build(1, nil, rt, cfg, rt.Patch.Fields, []BatchEntry{
		{ID: 1, New: unsafe.Pointer(&rec1)},
		{ID: 2, New: unsafe.Pointer(&rec2)},
	})
	if current.Universe.Cardinality() != 2 || !current.Universe.Contains(1) || !current.Universe.Contains(2) {
		t.Fatal("initial snapshot universe is inconsistent")
	}

	opt3 := "opt3"
	rec3 := snapshotBatchStorageRec{Name: "cara", Tags: []string{"blue"}, Opt: &opt3}
	next := Build(2, current, rt, cfg, rt.Patch.Fields, []BatchEntry{
		{ID: 3, New: unsafe.Pointer(&rec3)},
	})
	current.releaseRuntimeCaches()
	current.releaseStorage()
	current = next
	if !snapshotBatchFieldContains(current, "Name", "alice", 1) {
		t.Fatal("insert-only snapshot lost retained predecessor field storage")
	}
	if !snapshotBatchFieldContains(current, "Name", "cara", 3) {
		t.Fatal("insert-only snapshot is missing inserted field storage")
	}
	if !snapshotBatchLenContains(current, "Tags", 2, 2) || !snapshotBatchLenContains(current, "Tags", 1, 3) {
		t.Fatal("insert-only snapshot len index is inconsistent")
	}
	if current.Universe.Cardinality() != 3 || !current.Universe.Contains(3) {
		t.Fatal("insert-only snapshot universe is inconsistent")
	}

	rec1Patch := snapshotBatchStorageRec{Name: "alicia", Tags: rec1.Tags}
	next = Build(3, current, rt, cfg, rt.Patch.Fields, []BatchEntry{
		{
			ID:        1,
			Old:       unsafe.Pointer(&rec1),
			New:       unsafe.Pointer(&rec1Patch),
			Patch:     []schema.PatchItem{{Name: "Name", Value: rec1Patch.Name}},
			PatchOnly: true,
		},
	})
	current.releaseRuntimeCaches()
	current.releaseStorage()
	current = next
	if snapshotBatchFieldContains(current, "Name", "alice", 1) {
		t.Fatal("patch-only snapshot kept stale scalar posting")
	}
	if !snapshotBatchFieldContains(current, "Name", "alicia", 1) {
		t.Fatal("patch-only snapshot is missing updated scalar posting")
	}
	if !snapshotBatchFieldContains(current, "Tags", "red", 1) {
		t.Fatal("patch-only snapshot lost untouched retained field storage")
	}

	rec2Mid := snapshotBatchStorageRec{Name: "bob", Tags: []string{"green"}, Opt: &opt2}
	rec2Patch := snapshotBatchStorageRec{Name: "bobby", Tags: []string{"green", "blue"}, Opt: &opt2}
	next = Build(4, current, rt, cfg, rt.Patch.Fields, []BatchEntry{
		{
			ID:        2,
			Old:       unsafe.Pointer(&rec2),
			New:       unsafe.Pointer(&rec2Mid),
			Patch:     []schema.PatchItem{{Name: "Tags", Value: rec2Mid.Tags}},
			PatchOnly: true,
		},
		{
			ID:        2,
			Old:       unsafe.Pointer(&rec2Mid),
			New:       unsafe.Pointer(&rec2Patch),
			Patch:     []schema.PatchItem{{Name: "Name", Value: rec2Patch.Name}, {Name: "Tags", Value: rec2Patch.Tags}},
			PatchOnly: true,
		},
	})
	current.releaseRuntimeCaches()
	current.releaseStorage()
	current = next
	if snapshotBatchFieldContains(current, "Name", "bob", 2) {
		t.Fatal("aggregated patch snapshot kept stale repeated-id scalar posting")
	}
	if !snapshotBatchFieldContains(current, "Name", "bobby", 2) {
		t.Fatal("aggregated patch snapshot is missing repeated-id scalar posting")
	}
	if snapshotBatchFieldContains(current, "Tags", "yellow", 2) {
		t.Fatal("aggregated patch snapshot kept stale repeated-id slice posting")
	}
	if !snapshotBatchFieldContains(current, "Tags", "blue", 2) {
		t.Fatal("aggregated patch snapshot is missing repeated-id slice posting")
	}

	rec3Replace := snapshotBatchStorageRec{Name: "dana", Tags: []string{"orange"}}
	next = Build(5, current, rt, cfg, rt.Patch.Fields, []BatchEntry{
		{ID: 3, Old: unsafe.Pointer(&rec3), New: unsafe.Pointer(&rec3Replace)},
	})
	current.releaseRuntimeCaches()
	current.releaseStorage()
	current = next
	if snapshotBatchFieldContains(current, "Name", "cara", 3) {
		t.Fatal("set-replace snapshot kept stale scalar posting")
	}
	if !snapshotBatchFieldContains(current, "Name", "dana", 3) {
		t.Fatal("set-replace snapshot is missing new scalar posting")
	}
	if !snapshotBatchNilContains(current, "Opt", 3) {
		t.Fatal("set-replace snapshot is missing nil opt membership")
	}

	next = Build(6, current, rt, cfg, rt.Patch.Fields, []BatchEntry{
		{ID: 1, Old: unsafe.Pointer(&rec1Patch)},
	})
	current.releaseRuntimeCaches()
	current.releaseStorage()
	current = next
	if current.Universe.Contains(1) || current.Universe.Cardinality() != 2 {
		t.Fatal("delete snapshot universe is inconsistent")
	}
	if snapshotBatchFieldContains(current, "Name", "alicia", 1) {
		t.Fatal("delete snapshot kept deleted scalar posting")
	}
	if snapshotBatchFieldContains(current, "Tags", "red", 1) {
		t.Fatal("delete snapshot kept deleted slice posting")
	}

	next = Build(7, current, rt, cfg, rt.Patch.Fields, []BatchEntry{
		{ID: 2, Old: unsafe.Pointer(&rec2Patch)},
		{ID: 3, Old: unsafe.Pointer(&rec3Replace)},
	})
	current.releaseRuntimeCaches()
	current.releaseStorage()
	current = next
	if !current.Universe.IsEmpty() {
		t.Fatal("delete-all snapshot universe is not empty")
	}
	if snapshotBatchFieldContains(current, "Name", "bobby", 2) || snapshotBatchFieldContains(current, "Name", "dana", 3) {
		t.Fatal("delete-all snapshot kept deleted field postings")
	}

	opt5 := "opt5"
	rec4 := snapshotBatchStorageRec{Name: "erin", Tags: []string{"silver"}}
	rec5 := snapshotBatchStorageRec{Name: "frank", Tags: []string{"gold", "white"}, Opt: &opt5}
	next = Build(8, current, rt, cfg, rt.Patch.Fields, []BatchEntry{
		{ID: 4, New: unsafe.Pointer(&rec4)},
		{ID: 5, New: unsafe.Pointer(&rec5)},
	})
	current.releaseRuntimeCaches()
	current.releaseStorage()
	current = next
	if current.Universe.Cardinality() != 2 || !current.Universe.Contains(4) || !current.Universe.Contains(5) {
		t.Fatal("post-empty insert snapshot universe is inconsistent")
	}

	opt4Replace := "opt4"
	rec4Replace := snapshotBatchStorageRec{Name: "erin2", Tags: []string{"black"}, Opt: &opt4Replace}
	rec5Replace := snapshotBatchStorageRec{Name: "frank2"}
	next = Build(9, current, rt, cfg, rt.Patch.Fields, []BatchEntry{
		{ID: 4, Old: unsafe.Pointer(&rec4), New: unsafe.Pointer(&rec4Replace)},
		{ID: 5, Old: unsafe.Pointer(&rec5), New: unsafe.Pointer(&rec5Replace)},
	})
	current.releaseRuntimeCaches()
	current.releaseStorage()
	current = next
	defer current.releaseRuntimeCaches()
	defer current.releaseStorage()

	if snapshotBatchFieldContains(current, "Name", "erin", 4) || snapshotBatchFieldContains(current, "Name", "frank", 5) {
		t.Fatal("full-replace snapshot kept stale scalar postings")
	}
	if !snapshotBatchFieldContains(current, "Name", "erin2", 4) || !snapshotBatchFieldContains(current, "Name", "frank2", 5) {
		t.Fatal("full-replace snapshot is missing new scalar postings")
	}
	if !snapshotBatchFieldContains(current, "Opt", opt4Replace, 4) {
		t.Fatal("full-replace snapshot is missing new opt posting")
	}
	if !snapshotBatchNilContains(current, "Opt", 5) {
		t.Fatal("full-replace snapshot is missing nil opt membership")
	}
	if current.Universe.Cardinality() != 2 || !current.Universe.Contains(4) || !current.Universe.Contains(5) {
		t.Fatal("full-replace snapshot universe is inconsistent")
	}
}

func TestBuildPreparedModelReplayImmediateRetireWithBorrowedInputs(t *testing.T) {
	rt := snapshotBatchStorageRuntime(t)
	cfg := CacheConfig{}

	type modelRec struct {
		name   string
		tags   []string
		opt    string
		hasOpt bool
	}

	model := make(map[uint64]modelRec, 8)
	knownNames := make(map[string]struct{}, 16)
	knownTags := make(map[string]struct{}, 16)
	knownOpts := make(map[string]struct{}, 16)
	knownLens := make(map[uint64]struct{}, 4)

	record := func(name string, tags []string, opt string, hasOpt bool) modelRec {
		out := modelRec{name: name, opt: opt, hasOpt: hasOpt}
		if len(tags) > 0 {
			out.tags = append([]string(nil), tags...)
		}
		knownNames[name] = struct{}{}
		knownLens[uint64(len(tags))] = struct{}{}
		for i := range tags {
			knownTags[tags[i]] = struct{}{}
		}
		if hasOpt {
			knownOpts[opt] = struct{}{}
		}
		return out
	}

	borrowString := func(s string, bufs *[][]byte) string {
		buf := []byte(s)
		*bufs = append(*bufs, buf)
		return unsafe.String(unsafe.SliceData(buf), len(buf))
	}

	makeInput := func(src modelRec, bufs *[][]byte, inputs *[]*snapshotBatchStorageRec) *snapshotBatchStorageRec {
		rec := &snapshotBatchStorageRec{Name: borrowString(src.name, bufs)}
		if len(src.tags) > 0 {
			rec.Tags = make([]string, len(src.tags))
			for i := range src.tags {
				rec.Tags[i] = borrowString(src.tags[i], bufs)
			}
		}
		if src.hasOpt {
			opt := borrowString(src.opt, bufs)
			rec.Opt = &opt
		}
		*inputs = append(*inputs, rec)
		return rec
	}

	poisonInputs := func(bufs [][]byte, inputs []*snapshotBatchStorageRec) {
		for i := range bufs {
			buf := bufs[i]
			for j := range buf {
				buf[j] = 0x7f
			}
		}
		for i := range inputs {
			input := inputs[i]
			input.Name = "poison"
			for j := range input.Tags {
				input.Tags[j] = "poison"
			}
			if input.Opt != nil {
				*input.Opt = "poison"
			}
		}
	}

	assertPosting := func(label string, ids posting.List, want map[uint64]struct{}) {
		t.Helper()
		if ids.Cardinality() != uint64(len(want)) {
			got := ids.ToArray()
			ids.Release()
			t.Fatalf("%s: cardinality got %d ids=%v want %d", label, len(got), got, len(want))
		}
		for id := range want {
			if !ids.Contains(id) {
				got := ids.ToArray()
				ids.Release()
				t.Fatalf("%s: missing id %d in %v", label, id, got)
			}
		}
		ids.Release()
	}

	assertStoragePosting := func(label string, storage indexdata.FieldStorage, key string, want map[uint64]struct{}) {
		t.Helper()
		ids := indexdata.NewFieldIndexViewFromStorage(storage).LookupPostingRetained(key)
		assertPosting(label, ids, want)
	}
	assertStoragePostingKey := func(label string, storage indexdata.FieldStorage, key keycodec.IndexKey, want map[uint64]struct{}) {
		t.Helper()
		ids := indexdata.NewFieldIndexViewFromStorage(storage).LookupPostingRetainedKey(key)
		assertPosting(label, ids, want)
	}

	assertModel := func(label string, snap *View) {
		t.Helper()
		if snap.Universe.Cardinality() != uint64(len(model)) {
			t.Fatalf("%s: universe cardinality got %d want %d", label, snap.Universe.Cardinality(), len(model))
		}
		for id := range model {
			if !snap.Universe.Contains(id) {
				t.Fatalf("%s: universe missing id %d", label, id)
			}
		}

		nameWant := make(map[string]map[uint64]struct{}, len(knownNames))
		tagWant := make(map[string]map[uint64]struct{}, len(knownTags))
		optWant := make(map[string]map[uint64]struct{}, len(knownOpts))
		lenWant := make(map[uint64]map[uint64]struct{}, len(knownLens))
		nilWant := make(map[uint64]struct{})
		nonEmptyWant := make(map[uint64]struct{})

		addStringID := func(dst map[string]map[uint64]struct{}, key string, id uint64) {
			set := dst[key]
			if set == nil {
				set = make(map[uint64]struct{}, 1)
				dst[key] = set
			}
			set[id] = struct{}{}
		}
		addLenID := func(key uint64, id uint64) {
			set := lenWant[key]
			if set == nil {
				set = make(map[uint64]struct{}, 1)
				lenWant[key] = set
			}
			set[id] = struct{}{}
		}

		for id, rec := range model {
			addStringID(nameWant, rec.name, id)
			for i := range rec.tags {
				addStringID(tagWant, rec.tags[i], id)
			}
			addLenID(uint64(len(rec.tags)), id)
			if len(rec.tags) > 0 {
				nonEmptyWant[id] = struct{}{}
			}
			if rec.hasOpt {
				addStringID(optWant, rec.opt, id)
			} else {
				nilWant[id] = struct{}{}
			}
		}

		names := make([]string, 0, len(knownNames))
		for key := range knownNames {
			names = append(names, key)
		}
		slices.Sort(names)
		for _, key := range names {
			ids := testFieldLookupPostingRetained(snap, "Name", key)
			assertPosting(label+": Name "+key, ids, nameWant[key])
		}

		tags := make([]string, 0, len(knownTags))
		for key := range knownTags {
			tags = append(tags, key)
		}
		slices.Sort(tags)
		for _, key := range tags {
			ids := testFieldLookupPostingRetained(snap, "Tags", key)
			assertPosting(label+": Tags "+key, ids, tagWant[key])
		}

		opts := make([]string, 0, len(knownOpts))
		for key := range knownOpts {
			opts = append(opts, key)
		}
		slices.Sort(opts)
		for _, key := range opts {
			ids := testFieldLookupPostingRetained(snap, "Opt", key)
			assertPosting(label+": Opt "+key, ids, optWant[key])
		}

		optAcc := snap.IndexedFieldByName["Opt"]
		var nilStorage indexdata.FieldStorage
		if optAcc.Ordinal < len(snap.NilIndex) {
			nilStorage = snap.NilIndex[optAcc.Ordinal]
		}
		assertStoragePosting(label+": Opt nil", nilStorage, indexdata.NilIndexEntryKey, nilWant)

		tagsAcc := snap.IndexedFieldByName["Tags"]
		var lenStorage indexdata.FieldStorage
		if tagsAcc.Ordinal < len(snap.LenIndex) {
			lenStorage = snap.LenIndex[tagsAcc.Ordinal]
		}
		useZeroComplement := tagsAcc.Ordinal < len(snap.LenZeroComplement) && snap.LenZeroComplement[tagsAcc.Ordinal]
		lens := make([]uint64, 0, len(knownLens))
		for ln := range knownLens {
			lens = append(lens, ln)
		}
		slices.Sort(lens)
		for _, ln := range lens {
			want := lenWant[ln]
			if useZeroComplement && ln == 0 {
				want = nil
			}
			assertStoragePostingKey(label+": Tags len", lenStorage, keycodec.FromU64(ln), want)
		}
		if useZeroComplement {
			assertStoragePosting(label+": Tags non-empty", lenStorage, indexdata.LenIndexNonEmptyKey, nonEmptyWant)
		} else {
			assertStoragePosting(label+": Tags non-empty", lenStorage, indexdata.LenIndexNonEmptyKey, nil)
		}
	}

	var current *View
	defer func() {
		if current != nil {
			current.releaseRuntimeCaches()
			current.releaseStorage()
		}
	}()

	var seq uint64
	run := func(label string, build func(*[][]byte, *[]*snapshotBatchStorageRec) []BatchEntry, update func()) {
		t.Helper()
		var bufs [][]byte
		var inputs []*snapshotBatchStorageRec
		entries := build(&bufs, &inputs)
		seq++
		next := Build(seq, current, rt, cfg, rt.Patch.Fields, entries)
		if current != nil {
			current.releaseRuntimeCaches()
			current.releaseStorage()
		}
		current = next
		poisonInputs(bufs, inputs)
		update()
		assertModel(label, current)
	}

	rec1 := record("alice", []string{"red"}, "", false)
	rec2 := record("bob", []string{"green", "yellow"}, "opt2", true)
	rec3 := record("cara", nil, "opt3", true)
	run("insert", func(bufs *[][]byte, inputs *[]*snapshotBatchStorageRec) []BatchEntry {
		return []BatchEntry{
			{ID: 1, New: unsafe.Pointer(makeInput(rec1, bufs, inputs))},
			{ID: 2, New: unsafe.Pointer(makeInput(rec2, bufs, inputs))},
			{ID: 3, New: unsafe.Pointer(makeInput(rec3, bufs, inputs))},
		}
	}, func() {
		model[1] = rec1
		model[2] = rec2
		model[3] = rec3
	})

	rec1Patch := record("alicia", []string{"red"}, "", false)
	run("patch-name", func(bufs *[][]byte, inputs *[]*snapshotBatchStorageRec) []BatchEntry {
		oldPtr := makeInput(model[1], bufs, inputs)
		newPtr := makeInput(rec1Patch, bufs, inputs)
		return []BatchEntry{{
			ID:        1,
			Old:       unsafe.Pointer(oldPtr),
			New:       unsafe.Pointer(newPtr),
			Patch:     []schema.PatchItem{{Name: "Name", Value: newPtr.Name}},
			PatchOnly: true,
		}}
	}, func() {
		model[1] = rec1Patch
	})

	rec2Mid := record("bob", []string{"green"}, "opt2", true)
	rec2Patch := record("bobby", []string{"green", "blue"}, "opt2b", true)
	run("repeated-patch", func(bufs *[][]byte, inputs *[]*snapshotBatchStorageRec) []BatchEntry {
		oldPtr := makeInput(model[2], bufs, inputs)
		midPtr := makeInput(rec2Mid, bufs, inputs)
		newPtr := makeInput(rec2Patch, bufs, inputs)
		return []BatchEntry{
			{
				ID:        2,
				Old:       unsafe.Pointer(oldPtr),
				New:       unsafe.Pointer(midPtr),
				Patch:     []schema.PatchItem{{Name: "Tags", Value: midPtr.Tags}},
				PatchOnly: true,
			},
			{
				ID:  2,
				Old: unsafe.Pointer(midPtr),
				New: unsafe.Pointer(newPtr),
				Patch: []schema.PatchItem{
					{Name: "Name", Value: newPtr.Name},
					{Name: "Tags", Value: newPtr.Tags},
					{Name: "Opt", Value: rec2Patch.opt},
				},
				PatchOnly: true,
			},
		}
	}, func() {
		model[2] = rec2Patch
	})

	rec3Replace := record("dana", []string{"orange", "violet"}, "", false)
	run("replace", func(bufs *[][]byte, inputs *[]*snapshotBatchStorageRec) []BatchEntry {
		return []BatchEntry{{
			ID:  3,
			Old: unsafe.Pointer(makeInput(model[3], bufs, inputs)),
			New: unsafe.Pointer(makeInput(rec3Replace, bufs, inputs)),
		}}
	}, func() {
		model[3] = rec3Replace
	})

	run("delete", func(bufs *[][]byte, inputs *[]*snapshotBatchStorageRec) []BatchEntry {
		return []BatchEntry{{
			ID:  1,
			Old: unsafe.Pointer(makeInput(model[1], bufs, inputs)),
		}}
	}, func() {
		delete(model, 1)
	})

	rec4 := record("erin", nil, "opt4", true)
	rec5 := record("frank", []string{"gold", "white"}, "", false)
	run("insert-more", func(bufs *[][]byte, inputs *[]*snapshotBatchStorageRec) []BatchEntry {
		return []BatchEntry{
			{ID: 4, New: unsafe.Pointer(makeInput(rec4, bufs, inputs))},
			{ID: 5, New: unsafe.Pointer(makeInput(rec5, bufs, inputs))},
		}
	}, func() {
		model[4] = rec4
		model[5] = rec5
	})

	rec2Replace := record("bruno", []string{"cyan"}, "", false)
	rec3Replace2 := record("daria", nil, "opt3r", true)
	rec4Replace := record("ella", []string{"black"}, "opt4r", true)
	rec5Replace := record("felix", []string{"white", "silver"}, "", false)
	run("full-replace", func(bufs *[][]byte, inputs *[]*snapshotBatchStorageRec) []BatchEntry {
		return []BatchEntry{
			{ID: 2, Old: unsafe.Pointer(makeInput(model[2], bufs, inputs)), New: unsafe.Pointer(makeInput(rec2Replace, bufs, inputs))},
			{ID: 3, Old: unsafe.Pointer(makeInput(model[3], bufs, inputs)), New: unsafe.Pointer(makeInput(rec3Replace2, bufs, inputs))},
			{ID: 4, Old: unsafe.Pointer(makeInput(model[4], bufs, inputs)), New: unsafe.Pointer(makeInput(rec4Replace, bufs, inputs))},
			{ID: 5, Old: unsafe.Pointer(makeInput(model[5], bufs, inputs)), New: unsafe.Pointer(makeInput(rec5Replace, bufs, inputs))},
		}
	}, func() {
		model[2] = rec2Replace
		model[3] = rec3Replace2
		model[4] = rec4Replace
		model[5] = rec5Replace
	})

	run("delete-all", func(bufs *[][]byte, inputs *[]*snapshotBatchStorageRec) []BatchEntry {
		return []BatchEntry{
			{ID: 2, Old: unsafe.Pointer(makeInput(model[2], bufs, inputs))},
			{ID: 3, Old: unsafe.Pointer(makeInput(model[3], bufs, inputs))},
			{ID: 4, Old: unsafe.Pointer(makeInput(model[4], bufs, inputs))},
			{ID: 5, Old: unsafe.Pointer(makeInput(model[5], bufs, inputs))},
		}
	}, func() {
		delete(model, 2)
		delete(model, 3)
		delete(model, 4)
		delete(model, 5)
	})
}
