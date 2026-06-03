package schema

import (
	"testing"
	"unsafe"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/posting"
)

func buildStateSingleton(id uint64) posting.List {
	return (posting.List{}).BuildAdded(id)
}

func TestBuildFieldLocalStateOwnsStringKeyUntilFlush(t *testing.T) {
	buf := []byte("mutable-name")
	state := NewBuildFieldLocalState(false, false)
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
	state := NewBuildFieldLocalState(false, false)
	for i := 1; i <= posting.MidCap+1; i++ {
		state.addValue("name", uint64(i))
	}

	state.Release()
	if state.vals != nil {
		t.Fatalf("expected vals map to be cleared")
	}
}

func TestBuildFieldLocalStateReleaseClearsFixed(t *testing.T) {
	state := NewBuildFieldLocalState(true, false)
	for i := 1; i <= posting.MidCap+1; i++ {
		state.addFixedValue(42, uint64(i))
	}

	state.Release()
	if state.fixed != nil {
		t.Fatalf("expected fixed map to be cleared")
	}
}

func TestBuildFieldLocalStateReleaseClearsLenMap(t *testing.T) {
	state := NewBuildFieldLocalState(false, true)
	for i := 1; i <= posting.MidCap+1; i++ {
		state.addLen(3, uint64(i))
	}

	state.Release()
	if state.lenMap != nil {
		t.Fatalf("expected len map to be cleared")
	}
}

func TestBuildFieldLocalStateReleaseClearsNils(t *testing.T) {
	state := NewBuildFieldLocalState(false, false)
	for i := 1; i <= posting.MidCap+1; i++ {
		state.addNil(uint64(i))
	}

	state.Release()
	if !state.nils.IsEmpty() {
		t.Fatalf("expected nil postings to be cleared")
	}
}

func TestBuildFieldLocalStateFlushAllIntoMergesNilSource(t *testing.T) {
	state := NewBuildFieldLocalState(false, false)
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
	state := NewBuildFieldLocalState(false, true)
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
	state := NewBuildFieldLocalState(false, true)
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

func TestBuildFieldStateMaterializeStorageDetachesRuns(t *testing.T) {
	state := NewBuildFieldState(false)
	state.runs = []indexdata.FieldStorageRun{{}}

	storage := state.MaterializeStorage()
	defer storage.Release()
	if len(state.runs) != 0 {
		t.Fatalf("expected runs to be detached during materialization")
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
