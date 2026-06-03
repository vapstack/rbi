package snapshot

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vapstack/rbi/internal/posting"
)

func TestManagerRace_UnpinDoesNotDrainCurrentCacheAfterRefRevived(t *testing.T) {
	if !testRaceEnabled {
		t.Skip("run with -race to detect borrowed posting release with active snapshot ref")
	}
	m := NewRegistry(true)
	snap := testMatPredView(1, 0)
	snap.Seq = 1
	defer snap.releaseRuntimeCaches()

	m.Publish(snap)

	_, seq, ref := m.PinCurrent()
	if ref == nil {
		t.Fatal("expected current snapshot ref")
	}

	key := testMatPredKey("race-large")
	otherKey := testMatPredKey("race-other")

	ids := make([]uint64, 8192)
	for i := range ids {
		ids[i] = uint64(i)<<32 | 1
	}
	snap.StoreMaterializedPredKey(key, posting.BuildFromSorted(ids))

	borrowed, ok := snap.LoadMaterializedPredKey(key)
	if !ok || borrowed.IsEmpty() {
		t.Fatal("expected cached materialized posting")
	}
	wantLen := len(ids)

	snap.StoreMaterializedPredKey(otherKey, (posting.List{}).BuildAdded(1))

	m.mu.Lock()
	unpinDone := make(chan struct{})
	go func() {
		m.Unpin(seq, ref)
		close(unpinDone)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for ref.refs.Load() != 0 {
		if time.Now().After(deadline) {
			m.mu.Unlock()
			<-unpinDone
			t.Fatal("timed out waiting for Unpin to drop the ref before locking registry")
		}
		runtime.Gosched()
	}

	// Force the state produced by a new pin landing after refs reached zero but
	// before the old Unpin observes the ref under sm.mu.
	ref.refs.Add(1)
	defer m.Unpin(seq, ref)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var readerPanic atomic.Bool
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if recover() != nil {
				readerPanic.Store(true)
			}
		}()
		for {
			select {
			case <-stop:
				return
			default:
				got := borrowed.ToArray()
				runtime.KeepAlive(got)
			}
		}
	}()

	runtime.Gosched()
	m.mu.Unlock()
	<-unpinDone
	close(stop)
	wg.Wait()

	if readerPanic.Load() {
		t.Fatal("borrowed posting read panicked while snapshot ref was active")
	}

	gotLen := -1
	func() {
		defer func() {
			if recover() != nil {
				gotLen = -1
			}
		}()
		gotLen = len(borrowed.ToArray())
	}()
	if gotLen != wantLen {
		t.Fatalf("borrowed posting was released while snapshot ref was active: got len=%d want=%d", gotLen, wantLen)
	}
}
