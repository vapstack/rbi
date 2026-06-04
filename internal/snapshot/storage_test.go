package snapshot

import (
	"testing"

	"github.com/vapstack/rbi/internal/indexdata"
)

func TestViewReleaseStorageKeepsSharedFlatRootRetained(t *testing.T) {
	shared := testPosting()
	for i := uint64(0); i < 96; i++ {
		shared = shared.BuildAdded((i << 1) + 1)
	}
	sharedMap := indexdata.GetPostingMap()
	sharedMap["shared"] = shared
	sharedStorage := indexdata.NewFlatFieldStorageFromPostingMapOwned(sharedMap)

	old := testView(map[string]indexdata.FieldStorage{"f": sharedStorage})
	old.Seq = 1
	current := testView(map[string]indexdata.FieldStorage{"f": sharedStorage})
	current.Seq = 2
	current.retainSharedOwnedStorageFrom(old)

	old.releaseStorage()
	if !current.FieldLookupPostingRetained("f", "shared").Contains(1) {
		t.Fatalf("current snapshot lost shared posting after old prune")
	}
	if !current.FieldLookupPostingRetained("f", "shared").Contains(191) {
		t.Fatalf("current snapshot shared posting corrupted after old prune")
	}

	shared.Release()
}
