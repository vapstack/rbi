package snapshot

import (
	"sync"
	"sync/atomic"

	"github.com/vapstack/rbi/internal/qcache"
)

type RegistryStats struct {
	Sequence     uint64
	UniverseCard uint64
	RegistrySize int
	PinnedRefs   int
}

type Manager struct {
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

type PinGuard struct {
	m *Manager
}

func NewManager(statsEnabled bool) *Manager {
	return &Manager{
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

func releaseRetiredSnapshots(buf []*View) {
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

func (sm *Manager) publishRef(s *View) []*View {
	sm.mu.Lock()

	prev := sm.current.Load()
	ref := sm.bySeq[s.Seq]
	if ref == nil {
		ref = snapshotRefPool.Get()
		sm.bySeq[s.Seq] = ref
	}
	var retired []*View
	if prev != nil && prev.Seq == s.Seq && ref.snap != nil && ref.snap != s {
		if ref.refs.Load() != 0 {
			ref.retired = appendRetiredSnapshot(ref.retired, ref.snap)
		} else {
			retired = appendRetiredSnapshot(retired, ref.snap)
		}
	}
	ref.snap = s
	sm.current.Store(s)
	sm.currentRef.Store(ref)
	if prev != nil && prev.Seq != s.Seq {
		retired = sm.retireSnapshotLocked(prev.Seq)
	}
	sm.mu.Unlock()
	return retired
}

func (sm *Manager) Stage(s *View) {
	sm.mu.Lock()

	ref := sm.bySeq[s.Seq]
	if ref == nil {
		ref = snapshotRefPool.Get()
		sm.bySeq[s.Seq] = ref
	}
	ref.snap = s
	sm.mu.Unlock()
}

func (sm *Manager) DropStaged(seq uint64) {
	sm.mu.Lock()

	ref := sm.bySeq[seq]
	if ref == nil {
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
		releaseRetiredSnapshots(retired)
		return
	}
	ref.retired = appendRetiredSnapshot(ref.retired, ref.snap)
	ref.snap = nil
	sm.mu.Unlock()
}

func (sm *Manager) PinBySeq(seq uint64) (*View, *Ref, bool) {
	sm.mu.RLock()
	snap, ref, ok := sm.pinBySeqLocked(seq)
	sm.mu.RUnlock()
	return snap, ref, ok
}

func (sm *Manager) LockPin() PinGuard {
	sm.mu.RLock()
	return PinGuard{m: sm}
}

func (g PinGuard) PinBySeq(seq uint64) (*View, *Ref, bool) {
	return g.m.pinBySeqLocked(seq)
}

func (g PinGuard) Unlock() {
	g.m.mu.RUnlock()
}

func (sm *Manager) pinBySeqLocked(seq uint64) (*View, *Ref, bool) {
	ref := sm.bySeq[seq]
	if ref == nil || ref.snap == nil {
		return nil, nil, false
	}
	ref.refs.Add(1)
	return ref.snap, ref, true
}

func (sm *Manager) Unpin(seq uint64, ref *Ref) {
	if ref == nil {
		return
	}
	if ref.refs.Add(-1) != 0 {
		return
	}
	var (
		drainCache *qcache.MaterializedPredCache
		retired    []*View
	)

	sm.mu.Lock()
	held := sm.bySeq[seq]
	if held != ref || held.refs.Load() != 0 || held.snap != nil {
		if held == ref && held.refs.Load() == 0 && held.snap != nil &&
			held == sm.currentRef.Load() && held.retired != nil {
			retired = held.retired
			held.retired = nil
		}
		if held == ref && held.snap != nil &&
			held == sm.currentRef.Load() {
			drainCache = held.snap.matPredCache
			if drainCache != nil {
				drainCache.Retain()
			}
		}
		sm.mu.Unlock()

		releaseRetiredSnapshots(retired)
		if drainCache != nil {
			drainCache.DrainRetired()
			drainCache.ReleaseRef()
		}
		return
	}

	retired = sm.releaseRetiredSnapshotRefLocked(seq, held)

	sm.mu.Unlock()

	releaseRetiredSnapshots(retired)
}

func (sm *Manager) retireSnapshotLocked(seq uint64) []*View {
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

func (sm *Manager) releaseRetiredSnapshotRefLocked(seq uint64, ref *Ref) []*View {
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

func (sm *Manager) StatsEnabled() bool {
	return sm.statsEnabled
}

func (sm *Manager) Current() *View {
	return sm.current.Load()
}

func (sm *Manager) Publish(s *View) {
	retired := sm.publishRef(s)
	releaseRetiredSnapshots(retired)
}

func (sm *Manager) PinCurrent() (*View, uint64, *Ref) {
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

func (sm *Manager) Stats(s *View, exclude *Ref) RegistryStats {
	diag := RegistryStats{
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
