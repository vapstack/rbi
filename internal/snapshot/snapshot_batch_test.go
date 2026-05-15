package snapshot

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/vapstack/rbi/internal/schema"
)

type snapshotBatchRec struct {
	Name  string  `rbi:"index"`
	Age   int     `rbi:"index"`
	Score float64 `rbi:"measure"`
}

func TestBuildPreparedFromEmptyBaseOwnsUniverse(t *testing.T) {
	var rec struct{}
	snap := BuildPrepared(1, nil, &schema.Runtime{}, CacheConfig{}, nil, nil, []BatchEntry{
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
