package snapshot

import (
	"sync"
	"sync/atomic"

	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/rbistats"
)

type Registry struct {
	current      atomic.Pointer[View]
	currentRef   atomic.Pointer[Ref]
	statsEnabled bool

	bySeq map[uint64]*Ref

	mu sync.RWMutex
}

type Ref struct {
	snap    *View
	retired []*View
	refs    atomic.Int64
}

func NewRegistry(statsEnabled bool) *Registry {
	return &Registry{
		bySeq:        make(map[uint64]*Ref, 128),
		statsEnabled: statsEnabled,
	}
}

func appendRetiredSnapshot(buf []*View, snap *View) []*View {
	if buf == nil {
		buf = snapshotRetiredListPool.Get(1)
	}
	return append(buf, snap)
}

func releaseRetiredSnapshotList(buf []*View) {
	if buf == nil {
		return
	}
	for i := range buf {
		retired := buf[i]
		retired.releaseStorage()
		retired.releaseRuntimeCaches()
	}
	snapshotRetiredListPool.Put(buf)
}

func (sm *Registry) releaseRetiredSnapshots(buf []*View) {
	if buf == nil {
		return
	}
	releaseRetiredSnapshotList(buf)

	var (
		matPredRetired  qcache.MaterializedPredRetired
		numRangeRetired qcache.NumericRangeBucketRetired
	)

	sm.mu.Lock()
	ref := sm.currentRef.Load()
	if ref != nil && ref.refs.Load() == 0 && ref.snap != nil {
		if ref.snap.matPredCache != nil {
			matPredRetired = ref.snap.matPredCache.TakeRetired()
		}
		if ref.snap.numericRangeBucketCache != nil {
			numRangeRetired = ref.snap.numericRangeBucketCache.TakeRetired()
		}
	}
	sm.mu.Unlock()

	matPredRetired.Release()
	numRangeRetired.Release()
}

func (sm *Registry) publishRef(s *View) ([]*View, []*View) {
	sm.mu.Lock()

	prev := sm.current.Load()
	ref := sm.bySeq[s.Seq]
	if ref == nil {
		ref = snapshotRefPool.Get()
		sm.bySeq[s.Seq] = ref
	}
	var retired []*View
	if ref.snap != nil && ref.snap != s {
		if ref.refs.Load() != 0 {
			ref.retired = appendRetiredSnapshot(ref.retired, ref.snap)
		} else {
			retired = appendRetiredSnapshot(retired, ref.snap)
		}
	}
	ref.snap = s
	sm.current.Store(s)
	sm.currentRef.Store(ref)
	var prevRetired []*View
	if prev != nil && prev.Seq != s.Seq {
		prevRetired = sm.retireSnapshotLocked(prev.Seq)
	}
	sm.mu.Unlock()
	return retired, prevRetired
}

func (sm *Registry) Stage(s *View) {
	sm.mu.Lock()

	ref := sm.bySeq[s.Seq]
	if ref == nil {
		ref = snapshotRefPool.Get()
		sm.bySeq[s.Seq] = ref
	}
	var retired []*View
	if ref.snap != nil && ref.snap != s {
		if ref.refs.Load() != 0 {
			ref.retired = appendRetiredSnapshot(ref.retired, ref.snap)
		} else {
			retired = appendRetiredSnapshot(retired, ref.snap)
		}
	}
	ref.snap = s
	sm.mu.Unlock()

	if retired != nil {
		sm.releaseRetiredSnapshots(retired)
	}
}

func (sm *Registry) DropStaged(seq uint64) {
	sm.mu.Lock()

	ref := sm.bySeq[seq]
	if ref == nil {
		sm.mu.Unlock()
		return
	}
	if ref.snap == nil {
		sm.mu.Unlock()
		return
	}
	if current := sm.current.Load(); current != nil && current.Seq == seq {
		sm.mu.Unlock()
		return
	}
	var retired []*View
	if ref.refs.Load() <= 0 {
		retired = sm.releaseRetiredSnapshotRefLocked(seq, ref)
		sm.mu.Unlock()
		sm.releaseRetiredSnapshots(retired)
		return
	}
	ref.retired = appendRetiredSnapshot(ref.retired, ref.snap)
	ref.snap = nil
	sm.mu.Unlock()
}

func (sm *Registry) PinBySeq(seq uint64) (*View, *Ref, bool) {
	sm.mu.RLock()
	snap, ref, ok := sm.pinBySeqLocked(seq)
	sm.mu.RUnlock()
	return snap, ref, ok
}

func (sm *Registry) pinBySeqLocked(seq uint64) (*View, *Ref, bool) {
	ref := sm.bySeq[seq]
	if ref == nil || ref.snap == nil {
		return nil, nil, false
	}
	ref.refs.Add(1)
	return ref.snap, ref, true
}

func (sm *Registry) Unpin(seq uint64, ref *Ref) {
	if ref == nil {
		return
	}
	if ref.refs.Add(-1) != 0 {
		return
	}
	var (
		matPredRetired  qcache.MaterializedPredRetired
		numRangeRetired qcache.NumericRangeBucketRetired
		retired         []*View
	)

	sm.mu.Lock()
	held := sm.bySeq[seq]
	refsZero := held == ref && held.refs.Load() == 0
	if held == ref && refsZero && held.snap == nil {
		retired = sm.releaseRetiredSnapshotRefLocked(seq, held)
	} else if refsZero && held.snap != nil {
		if held.retired != nil {
			retired = held.retired
			held.retired = nil
		}
		if held == sm.currentRef.Load() {
			if held.snap.matPredCache != nil {
				matPredRetired = held.snap.matPredCache.TakeRetired()
			}
			if held.snap.numericRangeBucketCache != nil {
				numRangeRetired = held.snap.numericRangeBucketCache.TakeRetired()
			}
		}
	}
	sm.mu.Unlock()

	sm.releaseRetiredSnapshots(retired)
	matPredRetired.Release()
	numRangeRetired.Release()
}

func (sm *Registry) retireSnapshotLocked(seq uint64) []*View {
	ref := sm.bySeq[seq]
	if current := sm.current.Load(); current != nil && current.Seq == seq {
		return nil
	}
	if ref.refs.Load() != 0 {
		ref.retired = appendRetiredSnapshot(ref.retired, ref.snap)
		ref.snap = nil
		return nil
	}
	return sm.releaseRetiredSnapshotRefLocked(seq, ref)
}

func (sm *Registry) releaseRetiredSnapshotRefLocked(seq uint64, ref *Ref) []*View {
	if current := sm.current.Load(); current != nil && current.Seq == seq {
		return nil
	}
	if ref.refs.Load() != 0 {
		return nil
	}
	retired := ref.retired
	if ref.snap != nil {
		retired = appendRetiredSnapshot(retired, ref.snap)
	}
	ref.snap = nil
	ref.retired = nil
	delete(sm.bySeq, seq)
	if sm.currentRef.Load() == ref {
		sm.currentRef.Store(nil)
	}
	snapshotRefPool.Put(ref)
	return retired
}

func (sm *Registry) StatsEnabled() bool {
	return sm.statsEnabled
}

func (sm *Registry) Current() *View {
	return sm.current.Load()
}

func (sm *Registry) Publish(s *View) {
	retired, prevRetired := sm.publishRef(s)
	if retired != nil {
		sm.releaseRetiredSnapshots(retired)
	}
	if prevRetired != nil {
		sm.releaseRetiredSnapshots(prevRetired)
	}
}

func (sm *Registry) PinCurrent() (*View, uint64, *Ref) {
	sm.mu.RLock()
	ref := sm.currentRef.Load()
	if ref == nil {
		sm.mu.RUnlock()
		return nil, 0, nil
	}
	snap := ref.snap
	ref.refs.Add(1)
	sm.mu.RUnlock()
	return snap, snap.Seq, ref
}

func (sm *Registry) Stats(s *View, exclude *Ref) rbistats.Snapshot {
	diag := rbistats.Snapshot{
		Sequence: s.Seq,
	}
	if !s.Universe.IsEmpty() {
		diag.UniverseCard = s.Universe.Cardinality()
	}

	sm.mu.RLock()
	diag.RegistrySize = len(sm.bySeq)
	for _, held := range sm.bySeq {
		refs := held.refs.Load()
		if held == exclude {
			refs--
		}
		if refs > 0 {
			diag.PinnedRefs++
		}
	}
	sm.mu.RUnlock()

	return diag
}
