package rbi

import (
	"unsafe"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/strmap"
)

type snapshotBatchEntry struct {
	idx       uint64
	oldVal    unsafe.Pointer
	newVal    unsafe.Pointer
	patch     []schema.PatchItem
	patchOnly bool
}

func cloneFieldIndexBoolSlots(src []bool, size int) []bool {
	out := pooled.GetBoolSlice(size)[:size]
	clear(out)
	copy(out, src[:min(size, len(src))])
	return out
}

func ensureSnapshotUniverseOwned(next *indexSnapshot, universeOwned *bool) {
	if *universeOwned {
		return
	}
	next.universe = next.universe.Clone()
	next.universeOwner = nil
	*universeOwned = true
}

type indexedFieldBatchDeltas struct {
	fields  []schema.BatchState
	touched []int
	changed []bool
}

func (deltas *indexedFieldBatchDeltas) markTouched(ordinal int) {
	if deltas.changed[ordinal] {
		return
	}
	deltas.changed[ordinal] = true
	deltas.touched = append(deltas.touched, ordinal)
}

func normalizePreparedBatchForSnapshot(entries []snapshotBatchEntry) []snapshotBatchEntry {
	if len(entries) == 0 {
		return nil
	}
	if len(entries) == 1 {
		op := entries[0]
		if op.oldVal == nil && op.newVal == nil {
			return nil
		}
		return entries[:1]
	}

	pos := uint64IntMapPool.Get(len(entries))
	defer uint64IntMapPool.Put(pos)

	n := 0
	for i := range entries {
		op := entries[i]
		if p := pos[op.idx]; p != 0 {
			// Repeated-id entries collapse to first oldVal -> last newVal.
			// Patch metadata from any individual request is no longer sufficient
			// to describe the aggregated diff across the whole chain, so disable
			// the patch-only fast path for this normalized entry.
			entries[p-1].patch = nil
			entries[p-1].patchOnly = false
			entries[p-1].newVal = op.newVal
			continue
		}
		pos[op.idx] = n + 1
		if n != i {
			entries[n] = op
		}
		n++
	}

	write := 0
	for i := 0; i < n; i++ {
		if entries[i].oldVal == nil && entries[i].newVal == nil {
			continue
		}
		if write != i {
			entries[write] = entries[i]
		}
		write++
	}
	return entries[:write]
}

func (qe *queryEngine) collectSnapshotBatchEntryDiffs(
	op snapshotBatchEntry,
	deltas *indexedFieldBatchDeltas,
	lenZeroComplement []bool,
	patchFields map[string]*schema.Field,
) {
	if op.patchOnly {
		for i, patchField := range op.patch {
			fieldDef, ok := patchFields[patchField.Name]
			if !ok {
				continue
			}
			acc, ok := qe.schema.IndexedByName[fieldDef.DBName]
			if !ok {
				continue
			}
			duplicate := false
			for j := 0; j < i; j++ {
				prev, ok := patchFields[op.patch[j].Name]
				if ok && prev.DBName == fieldDef.DBName {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
			deltas.markTouched(acc.Ordinal)
			useZeroComplement := acc.Ordinal < len(lenZeroComplement) && lenZeroComplement[acc.Ordinal]
			acc.CollectBatchDiff(op.idx, op.oldVal, op.newVal, useZeroComplement, &deltas.fields[acc.Ordinal])
		}
		return
	}

	if op.oldVal == nil || op.newVal == nil {
		for _, acc := range qe.schema.Indexed {
			deltas.markTouched(acc.Ordinal)
			useZeroComplement := acc.Ordinal < len(lenZeroComplement) && lenZeroComplement[acc.Ordinal]
			acc.CollectBatchDiff(op.idx, op.oldVal, op.newVal, useZeroComplement, &deltas.fields[acc.Ordinal])
		}
		return
	}

	for _, acc := range qe.schema.Indexed {
		if !acc.Modified(op.oldVal, op.newVal) {
			continue
		}
		deltas.markTouched(acc.Ordinal)
		useZeroComplement := acc.Ordinal < len(lenZeroComplement) && lenZeroComplement[acc.Ordinal]
		acc.CollectBatchDiff(op.idx, op.oldVal, op.newVal, useZeroComplement, &deltas.fields[acc.Ordinal])
	}
}

func (qe *queryEngine) buildPreparedSnapshotAggregatedNoLock(
	seq uint64,
	prev *indexSnapshot,
	strMap *strmap.Mapper,
	patchFields map[string]*schema.Field,
	entries []snapshotBatchEntry,
) *indexSnapshot {

	var sm *strmap.Snapshot
	if strMap != nil {
		sm = strMap.Snapshot()
	}
	next := &indexSnapshot{
		seq: seq,

		index:              indexdata.CloneFieldStorageSlots(prev.index, len(qe.schema.Indexed)),
		nilIndex:           indexdata.CloneFieldStorageSlots(prev.nilIndex, len(qe.schema.Indexed)),
		lenIndex:           indexdata.CloneFieldStorageSlots(prev.lenIndex, len(qe.schema.Indexed)),
		lenZeroComplement:  cloneFieldIndexBoolSlots(prev.lenZeroComplement, len(qe.schema.Indexed)),
		measure:            indexdata.CloneMeasureStorageSlots(prev.measure, len(qe.schema.Measures)),
		indexedFieldByName: qe.schema.IndexedByName,
		universe:           prev.universe,
		universeOwner:      prev.universeOwner,
		strmap:             sm,
	}
	qe.initSnapshotRuntimeCaches(next)

	normalized := normalizePreparedBatchForSnapshot(entries)
	deltas := indexedFieldBatchDeltas{
		fields:  schema.GetBatchStates(len(qe.schema.Indexed)),
		touched: pooled.GetIntSlice(len(qe.schema.Indexed)),
		changed: pooled.GetBoolSlice(len(qe.schema.Indexed))[:len(qe.schema.Indexed)],
	}
	clear(deltas.changed)
	measureDeltas := indexdata.NewMeasureDeltaBatch(len(qe.schema.Measures))

	universeOwned := false

	for i := range normalized {
		op := normalized[i]
		switch {
		case op.oldVal == nil && op.newVal != nil:
			ensureSnapshotUniverseOwned(next, &universeOwned)
			next.universe = next.universe.BuildAdded(op.idx)
		case op.oldVal != nil && op.newVal == nil:
			ensureSnapshotUniverseOwned(next, &universeOwned)
			next.universe = next.universe.BuildRemoved(op.idx)
		}
		qe.collectSnapshotBatchEntryDiffs(op, &deltas, prev.lenZeroComplement, patchFields)
		qe.collectSnapshotMeasureEntryDiffs(op, &measureDeltas, patchFields)
	}

	for i := range deltas.touched {
		deltas.changed[deltas.touched[i]] = false
	}

	changedCount := 0

	for i := range deltas.touched {
		ordinal := deltas.touched[i]
		acc := qe.schema.Indexed[ordinal]
		state := &deltas.fields[ordinal]
		baseIndex := next.index[ordinal]
		if storage := acc.ApplyBatchStorageOwned(baseIndex, state, true); storage.KeyCount() == 0 {
			if baseIndex.KeyCount() > 0 {
				next.index[ordinal] = indexdata.FieldStorage{}
			}
		} else if storage != baseIndex {
			next.index[ordinal] = storage
		}
		baseNil := next.nilIndex[ordinal]
		if storage := acc.ApplyBatchNilStorageOwned(baseNil, state); storage.KeyCount() == 0 {
			if baseNil.KeyCount() > 0 {
				next.nilIndex[ordinal] = indexdata.FieldStorage{}
			}
		} else if storage != baseNil {
			next.nilIndex[ordinal] = storage
		}
		if lenDiff := state.LenDiff(); lenDiff != nil {
			baseLen := next.lenIndex[ordinal]
			if storage := baseLen.ApplyLenPostingDiffRetainOwned(lenDiff); storage != baseLen {
				next.lenIndex[ordinal] = storage
			}
		}
		if state.Changed() {
			changedCount++
			deltas.changed[ordinal] = true
		}
		state.Reset()
	}
	measureDeltas.ApplyToMeasureStorageSlotsOwned(next.measure)
	measureDeltas.Release()
	inheritNumericRangeBucketCache(next, prev)
	if changedCount > 0 {
		inheritMaterializedPredCache(next, prev, qe.schema.IndexedByName, deltas.changed)
	} else {
		inheritMaterializedPredCache(next, prev, qe.schema.IndexedByName, nil)
	}
	schema.ReleaseBatchStates(deltas.fields)
	pooled.ReleaseBoolSlice(deltas.changed)
	pooled.ReleaseIntSlice(deltas.touched)
	next.retainSharedOwnedStorageFrom(prev)

	return next
}
