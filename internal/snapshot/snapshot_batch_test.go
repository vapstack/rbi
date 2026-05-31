package snapshot

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
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
	ids := s.FieldLookupPostingRetained(field, key)
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
	ids := indexdata.NewFieldIndexViewFromStorage(s.LenIndex[acc.Ordinal]).LookupPostingRetained(keycodec.U64ByteString(ln))
	ok := ids.Contains(id)
	ids.Release()
	return ok
}

func TestBuildPreparedFromEmptyBaseOwnsUniverse(t *testing.T) {
	var rec struct{}
	snap := BuildPrepared(1, nil, &schema.Schema{}, CacheConfig{}, nil, nil, []BatchEntry{
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

func TestBuildPreparedFullReplaceRebuildsStorage(t *testing.T) {
	rt, err := schema.Compile(reflect.TypeOf(snapshotBatchRec{}), schema.Config{})
	if err != nil {
		t.Fatal(err)
	}
	oldRecords := []snapshotBatchRec{{Name: "old-a", Age: 10, Score: 1}, {Name: "old-b", Age: 20, Score: 2}}
	prev := BuildPrepared(1, nil, rt, CacheConfig{}, nil, rt.Patch.Fields, []BatchEntry{
		{ID: 1, New: unsafe.Pointer(&oldRecords[0])},
		{ID: 2, New: unsafe.Pointer(&oldRecords[1])},
	})
	defer prev.releaseRuntimeCaches()
	defer prev.releaseStorage()

	newRecords := []snapshotBatchRec{{Name: "new-a", Age: 30, Score: 3}, {Name: "new-b", Age: 40, Score: 4}}
	next := BuildPrepared(2, prev, rt, CacheConfig{}, nil, rt.Patch.Fields, []BatchEntry{
		{ID: 1, Old: unsafe.Pointer(&oldRecords[0]), New: unsafe.Pointer(&newRecords[0])},
		{ID: 2, Old: unsafe.Pointer(&oldRecords[1]), New: unsafe.Pointer(&newRecords[1])},
	})
	defer next.releaseRuntimeCaches()
	defer next.releaseStorage()

	if got := next.Universe.Cardinality(); got != 2 {
		t.Fatalf("universe cardinality=%d want 2", got)
	}
	ids := next.FieldLookupPostingRetained("Name", "new-a")
	if !ids.Contains(1) {
		t.Fatal("expected rebuilt snapshot to contain new-a id")
	}
	ids.Release()
	ids = next.FieldLookupPostingRetained("Name", "old-a")
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
	prev := BuildPrepared(1, nil, rt, CacheConfig{}, nil, rt.Patch.Fields, []BatchEntry{
		{ID: 1, New: unsafe.Pointer(&oldRecords[0])},
		{ID: 2, New: unsafe.Pointer(&oldRecords[1])},
	})
	defer prev.releaseRuntimeCaches()
	defer prev.releaseStorage()

	next := BuildPrepared(2, prev, rt, CacheConfig{}, nil, rt.Patch.Fields, []BatchEntry{
		{ID: 1, Old: unsafe.Pointer(&oldRecords[0])},
		{ID: 2, Old: unsafe.Pointer(&oldRecords[1])},
	})
	defer next.releaseRuntimeCaches()
	defer next.releaseStorage()

	if !next.Universe.IsEmpty() {
		t.Fatal("expected delete-all snapshot to have empty universe")
	}
	ids := next.FieldLookupPostingRetained("Name", "old-a")
	if !ids.IsEmpty() {
		ids.Release()
		t.Fatal("expected delete-all snapshot to drop index storage")
	}
}

func TestBuildPreparedKeepsPreviousSnapshotStorageImmutableAcrossUpdateAndDelete(t *testing.T) {
	rt := snapshotBatchStorageRuntime(t)
	opt := "zzz"
	oldRecords := []snapshotBatchStorageRec{
		{Name: "bob", Tags: []string{"go", "db"}},
		{Name: "alice", Tags: []string{"go"}},
	}
	prev := BuildPrepared(1, nil, rt, CacheConfig{}, nil, rt.Patch.Fields, []BatchEntry{
		{ID: 1, New: unsafe.Pointer(&oldRecords[0])},
		{ID: 2, New: unsafe.Pointer(&oldRecords[1])},
	})
	defer prev.releaseRuntimeCaches()
	defer prev.releaseStorage()

	newRec := snapshotBatchStorageRec{Name: "charlie", Tags: []string{"go"}, Opt: &opt}
	next := BuildPrepared(2, prev, rt, CacheConfig{}, nil, rt.Patch.Fields, []BatchEntry{
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
	prev := BuildPrepared(1, nil, rt, CacheConfig{}, nil, rt.Patch.Fields, []BatchEntry{
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
	next := BuildPrepared(2, prev, rt, CacheConfig{}, nil, rt.Patch.Fields, []BatchEntry{
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
