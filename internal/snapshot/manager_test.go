package snapshot

import "testing"

func managerTestRef(m *Manager, seq uint64) *Ref {
	m.mu.RLock()
	ref := m.bySeq[seq]
	m.mu.RUnlock()
	return ref
}

func managerTestRegistrySize(m *Manager) int {
	m.mu.RLock()
	n := len(m.bySeq)
	m.mu.RUnlock()
	return n
}

func TestManagerPublishSameSeqReusesRefAndUpdatesCurrent(t *testing.T) {
	m := NewManager(true)

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
	m := NewManager(true)

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
	m := NewManager(true)

	staged := &View{Seq: 12}
	m.Stage(staged)
	m.DropStaged(staged.Seq)

	if managerTestRef(m, staged.Seq) != nil {
		t.Fatalf("expected unpinned staged snapshot to be removed immediately")
	}
}

func TestManagerRetirePinnedPublishedSnapshotNilsEntryUntilUnpin(t *testing.T) {
	m := NewManager(true)

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
