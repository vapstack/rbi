package rbi

import "github.com/vapstack/rbi/internal/posting"

// SnapshotStats returns diagnostics for published index snapshots.
//
// Runtime snapshot diagnostics are collected only when
// Options.EnableSnapshotStats was enabled for this DB instance; otherwise the
// method returns a zero value.
//
// In indexed mode it reports the current published snapshot sequence,
// universe cardinality, registry size, and pin counts.
//
// In transparent mode no published index snapshots are maintained, so the
// returned diagnostics remain zero-valued even when snapshot stats collection
// is enabled.
func (db *DB[K, V]) SnapshotStats() SnapshotStats {
	if db.engine == nil {
		return SnapshotStats{}
	}
	if !db.engine.snapshot.StatsEnabled() {
		return SnapshotStats{}
	}
	if !db.beginOpWait() {
		return SnapshotStats{}
	}
	defer db.endOp()

	snap, seq, ref := db.engine.snapshot.PinCurrent()
	if snap == nil {
		return SnapshotStats{}
	}
	defer db.engine.snapshot.Unpin(seq, ref)

	stats := db.engine.snapshot.Stats(snap, ref)
	return SnapshotStats{
		Sequence:     stats.Sequence,
		UniverseCard: stats.UniverseCard,
		RegistrySize: stats.RegistrySize,
		PinnedRefs:   stats.PinnedRefs,
	}
}

func unionPostingListsOwned(base, add posting.List) posting.List {
	merged := base.Clone()
	merged = merged.BuildMergedOwned(add)
	return merged
}
