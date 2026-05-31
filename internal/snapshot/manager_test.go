package snapshot

import "testing"

func managerTestRef(m *Registry, seq uint64) *Ref {
	m.mu.RLock()
	ref := m.bySeq[seq]
	m.mu.RUnlock()
	return ref
}

func managerTestRegistrySize(m *Registry) int {
	m.mu.RLock()
	n := len(m.bySeq)
	m.mu.RUnlock()
	return n
}

func TestManagerPublishSameSeqReusesRefAndUpdatesCurrent(t *testing.T) {
	m := NewRegistry(true)

	first := &View{Seq: 7}
	m.Publish(first)

	gotFirst, ref, ok := m.PinBySeq(first.Seq)
	if !ok || gotFirst != first {
		t.Fatalf("expected first snapshot to be pinnable")
	}

	second := &View{Seq: first.Seq}
	m.Publish(second)

	if got := m.Current(); got != second {
		t.Fatalf("expected current snapshot to be replaced on same-seq publish")
	}

	held := managerTestRef(m, first.Seq)
	if held != ref {
		t.Fatalf("expected registry entry to reuse existing ref")
	}
	if got := managerTestRegistrySize(m); got != 1 {
		t.Fatalf("expected single registry entry after same-seq publish, got=%d", got)
	}

	gotSecond, ref2, ok := m.PinBySeq(first.Seq)
	if !ok || gotSecond != second || ref2 != ref {
		t.Fatalf("expected second snapshot to reuse same ref")
	}

	m.Unpin(first.Seq, ref)
	m.Unpin(first.Seq, ref2)

	if managerTestRef(m, first.Seq) == nil {
		t.Fatalf("expected current same-seq snapshot to remain registered after unpin")
	}
}

func TestManagerDropStagedPinnedEntryBecomesUnpinnableUntilUnpin(t *testing.T) {
	m := NewRegistry(true)

	staged := &View{Seq: 11}
	m.Stage(staged)

	got, ref, ok := m.PinBySeq(staged.Seq)
	if !ok || got != staged {
		t.Fatalf("expected staged snapshot to be pinnable before drop")
	}

	m.DropStaged(staged.Seq)

	held := managerTestRef(m, staged.Seq)
	if held != ref || held == nil || held.snap != nil {
		t.Fatalf("expected dropped pinned staged snapshot to keep ref but clear snap")
	}
	if _, _, ok = m.PinBySeq(staged.Seq); ok {
		t.Fatalf("expected dropped staged snapshot to become unpinnable")
	}

	m.Unpin(staged.Seq, ref)

	if managerTestRef(m, staged.Seq) != nil {
		t.Fatalf("expected staged snapshot registry entry to be pruned after last unpin")
	}
}

func TestManagerDropStagedUnpinnedDeletesEntry(t *testing.T) {
	m := NewRegistry(true)

	staged := &View{Seq: 12}
	m.Stage(staged)
	m.DropStaged(staged.Seq)

	if managerTestRef(m, staged.Seq) != nil {
		t.Fatalf("expected unpinned staged snapshot to be removed immediately")
	}
}

func TestManagerPinCurrentIgnoresStagedFutureSnapshot(t *testing.T) {
	m := NewRegistry(true)

	current := &View{Seq: 1}
	staged := &View{Seq: 2}
	m.Publish(current)
	m.Stage(staged)

	got, seq, ref := m.PinCurrent()
	if got != current || seq != current.Seq || ref == nil {
		t.Fatalf("expected PinCurrent to return published current snapshot")
	}
	m.Unpin(seq, ref)
	m.DropStaged(staged.Seq)

	if m.Current() != current {
		t.Fatal("expected staged snapshot not to replace current")
	}
	if managerTestRef(m, staged.Seq) != nil {
		t.Fatal("expected staged snapshot ref to be removed after drop")
	}
}

func TestManagerRetirePinnedPublishedSnapshotNilsEntryUntilUnpin(t *testing.T) {
	m := NewRegistry(true)

	first := &View{Seq: 1}
	second := &View{Seq: 2}
	m.Publish(first)

	got, ref, ok := m.PinBySeq(first.Seq)
	if !ok || got != first {
		t.Fatalf("expected first snapshot to be pinnable")
	}

	m.Publish(second)

	oldHeld := managerTestRef(m, first.Seq)
	if oldHeld != ref || oldHeld == nil || oldHeld.snap != nil {
		t.Fatalf("expected pinned retired snapshot to keep ref with nil snap")
	}
	if managerTestRef(m, second.Seq) == nil {
		t.Fatalf("expected latest snapshot to remain registered")
	}
	if _, _, ok = m.PinBySeq(first.Seq); ok {
		t.Fatalf("expected retired pinned snapshot to stop accepting new pins")
	}

	m.Unpin(first.Seq, ref)

	if managerTestRef(m, first.Seq) != nil {
		t.Fatalf("expected retired snapshot to be pruned after last unpin")
	}
	if managerTestRef(m, second.Seq) == nil {
		t.Fatalf("expected latest snapshot to survive old-snapshot prune")
	}
}

func TestManagerPinCurrentTracksReaderAcrossPublishAndPrunesOnUnpin(t *testing.T) {
	m := NewRegistry(true)

	first := &View{Seq: 1}
	second := &View{Seq: 2}
	m.Publish(first)

	got, seq, ref := m.PinCurrent()
	if got != first || seq != first.Seq || ref == nil {
		t.Fatalf("expected current snapshot pin")
	}
	if stats := m.Stats(first, nil); stats.PinnedRefs != 1 {
		t.Fatalf("expected one current snapshot ref, stats=%+v", stats)
	}

	m.Publish(second)

	stats := m.Stats(second, nil)
	if stats.RegistrySize != 2 || stats.PinnedRefs != 1 {
		t.Fatalf("expected old snapshot ref pin to survive publish, stats=%+v", stats)
	}

	m.Unpin(seq, ref)
	stats = m.Stats(second, nil)
	if stats.RegistrySize != 1 || stats.PinnedRefs != 0 {
		t.Fatalf("expected current snapshot ref pins to drop after unpin, stats=%+v", stats)
	}
	if m.Current() != second {
		t.Fatal("expected latest snapshot to remain current after old unpin")
	}
}

func TestManagerDuplicatePinsOnRetiredSnapshotPruneOnlyOnLastUnpin(t *testing.T) {
	m := NewRegistry(true)

	first := &View{Seq: 1}
	second := &View{Seq: 2}
	m.Publish(first)

	got1, ref1, ok := m.PinBySeq(first.Seq)
	if !ok || got1 != first || ref1 == nil {
		t.Fatalf("expected first pin to return first snapshot")
	}
	got2, ref2, ok := m.PinBySeq(first.Seq)
	if !ok || got2 != first || ref2 != ref1 {
		t.Fatalf("expected duplicate pin to reuse first snapshot ref")
	}

	if stats := m.Stats(first, nil); stats.Sequence != first.Seq || stats.RegistrySize != 1 || stats.PinnedRefs != 1 {
		t.Fatalf("unexpected stats before retire: %+v", stats)
	}

	m.Publish(second)
	if m.Current() != second {
		t.Fatal("expected second snapshot to become current")
	}
	if held := managerTestRef(m, first.Seq); held != ref1 || held.snap != nil {
		t.Fatalf("expected retired pinned snapshot to keep nilled ref")
	}
	if stats := m.Stats(second, nil); stats.Sequence != second.Seq || stats.RegistrySize != 2 || stats.PinnedRefs != 1 {
		t.Fatalf("unexpected stats after retire: %+v", stats)
	}

	m.Unpin(first.Seq, ref1)
	if held := managerTestRef(m, first.Seq); held != ref1 || held.refs.Load() != 1 {
		t.Fatalf("expected first unpin to keep retired snapshot ref")
	}
	if stats := m.Stats(second, nil); stats.RegistrySize != 2 || stats.PinnedRefs != 1 {
		t.Fatalf("unexpected stats after first unpin: %+v", stats)
	}

	m.Unpin(first.Seq, ref2)
	if managerTestRef(m, first.Seq) != nil {
		t.Fatalf("expected retired snapshot ref to be pruned after last unpin")
	}
	if m.Current() != second {
		t.Fatal("expected latest snapshot to remain current after last unpin")
	}
	if stats := m.Stats(second, nil); stats.Sequence != second.Seq || stats.RegistrySize != 1 || stats.PinnedRefs != 0 {
		t.Fatalf("unexpected stats after last unpin: %+v", stats)
	}
}

func TestManagerStatsCountsDistinctPinnedSnapshotsAcrossRotation(t *testing.T) {
	m := NewRegistry(true)

	first := &View{Seq: 1}
	second := &View{Seq: 2}
	third := &View{Seq: 3}
	m.Publish(first)

	_, refA, ok := m.PinBySeq(first.Seq)
	if !ok || refA == nil {
		t.Fatalf("expected first snapshot to be pinnable")
	}

	m.Publish(second)

	_, refB, ok := m.PinBySeq(second.Seq)
	if !ok || refB == nil {
		t.Fatalf("expected second snapshot to be pinnable")
	}

	m.Publish(third)

	if stats := m.Stats(third, nil); stats.Sequence != third.Seq || stats.RegistrySize != 3 || stats.PinnedRefs != 2 {
		t.Fatalf("unexpected stats with two distinct retired pinned snapshots: %+v", stats)
	}

	m.Unpin(first.Seq, refA)
	if stats := m.Stats(third, nil); stats.Sequence != third.Seq || stats.RegistrySize != 2 || stats.PinnedRefs != 1 {
		t.Fatalf("unexpected stats after first unpin: %+v", stats)
	}

	m.Unpin(second.Seq, refB)
	if stats := m.Stats(third, nil); stats.Sequence != third.Seq || stats.RegistrySize != 1 || stats.PinnedRefs != 0 {
		t.Fatalf("unexpected stats after second unpin: %+v", stats)
	}
	if m.Current() != third {
		t.Fatal("expected latest snapshot to remain current")
	}
}
