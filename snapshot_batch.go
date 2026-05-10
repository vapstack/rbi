package rbi

import (
	"unsafe"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/pooled"
)

type snapshotBatchEntry struct {
	idx       uint64
	oldVal    unsafe.Pointer
	newVal    unsafe.Pointer
	patch     []Field
	patchOnly bool
}

type batchLenDiff struct {
	oldExists bool
	oldLen    int
	newExists bool
	newLen    int
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
	fields  []snapshotFieldBatchState
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

func collectFieldBatchPostingDiffBuf(
	fieldDelta map[string]uint32,
	arena **indexdata.PostingDiffArena,
	idx uint64,
	oldVals, newVals []string,
) (map[string]uint32, bool) {
	oldLen := len(oldVals)
	newLen := len(newVals)
	if oldLen == 0 && newLen == 0 {
		return fieldDelta, false
	}

	var changed bool
	i, j := 0, 0
	for i < oldLen && j < newLen {
		oldVal := oldVals[i]
		newVal := newVals[j]
		switch {
		case oldVal < newVal:
			fieldDelta = indexdata.AddStringPostingDiff(fieldDelta, arena, oldVal, idx, false, 0)
			changed = true
			i++
		case oldVal > newVal:
			fieldDelta = indexdata.AddStringPostingDiff(fieldDelta, arena, newVal, idx, true, 0)
			changed = true
			j++
		default:
			i++
			j++
		}
	}
	for ; i < oldLen; i++ {
		fieldDelta = indexdata.AddStringPostingDiff(fieldDelta, arena, oldVals[i], idx, false, 0)
		changed = true
	}
	for ; j < newLen; j++ {
		fieldDelta = indexdata.AddStringPostingDiff(fieldDelta, arena, newVals[j], idx, true, 0)
		changed = true
	}
	return fieldDelta, changed
}

func collectFixedFieldBatchPostingDiffBuf(
	fieldDelta map[uint64]uint32,
	arena **indexdata.PostingDiffArena,
	idx uint64,
	oldVals, newVals []uint64,
) (map[uint64]uint32, bool) {
	oldLen := len(oldVals)
	newLen := len(newVals)
	if oldLen == 0 && newLen == 0 {
		return fieldDelta, false
	}

	var changed bool
	i, j := 0, 0
	for i < oldLen && j < newLen {
		oldVal := oldVals[i]
		newVal := newVals[j]
		switch {
		case oldVal < newVal:
			fieldDelta = indexdata.AddFixedPostingDiff(fieldDelta, arena, oldVal, idx, false, 0)
			changed = true
			i++
		case oldVal > newVal:
			fieldDelta = indexdata.AddFixedPostingDiff(fieldDelta, arena, newVal, idx, true, 0)
			changed = true
			j++
		default:
			i++
			j++
		}
	}
	for ; i < oldLen; i++ {
		fieldDelta = indexdata.AddFixedPostingDiff(fieldDelta, arena, oldVals[i], idx, false, 0)
		changed = true
	}
	for ; j < newLen; j++ {
		fieldDelta = indexdata.AddFixedPostingDiff(fieldDelta, arena, newVals[j], idx, true, 0)
		changed = true
	}
	return fieldDelta, changed
}

func collectScalarFieldBatchPostingDiff(
	fieldDelta map[string]uint32,
	arena **indexdata.PostingDiffArena,
	idx uint64,
	oldOK bool,
	oldVal string,
	newOK bool,
	newVal string,
) (map[string]uint32, bool) {
	switch {
	case oldOK && newOK:
		if oldVal == newVal {
			return fieldDelta, false
		}
		fieldDelta = indexdata.AddStringPostingDiff(fieldDelta, arena, oldVal, idx, false, 0)
		fieldDelta = indexdata.AddStringPostingDiff(fieldDelta, arena, newVal, idx, true, 0)
		return fieldDelta, true
	case oldOK:
		fieldDelta = indexdata.AddStringPostingDiff(fieldDelta, arena, oldVal, idx, false, 0)
		return fieldDelta, true
	case newOK:
		fieldDelta = indexdata.AddStringPostingDiff(fieldDelta, arena, newVal, idx, true, 0)
		return fieldDelta, true
	default:
		return fieldDelta, false
	}
}

func collectScalarFixedFieldBatchPostingDiff(
	fieldDelta map[uint64]uint32,
	arena **indexdata.PostingDiffArena,
	idx uint64,
	oldOK bool,
	oldVal uint64,
	newOK bool,
	newVal uint64,
) (map[uint64]uint32, bool) {
	switch {
	case oldOK && newOK:
		if oldVal == newVal {
			return fieldDelta, false
		}
		fieldDelta = indexdata.AddFixedPostingDiff(fieldDelta, arena, oldVal, idx, false, 0)
		fieldDelta = indexdata.AddFixedPostingDiff(fieldDelta, arena, newVal, idx, true, 0)
		return fieldDelta, true
	case oldOK:
		fieldDelta = indexdata.AddFixedPostingDiff(fieldDelta, arena, oldVal, idx, false, 0)
		return fieldDelta, true
	case newOK:
		fieldDelta = indexdata.AddFixedPostingDiff(fieldDelta, arena, newVal, idx, true, 0)
		return fieldDelta, true
	default:
		return fieldDelta, false
	}
}

func collectFieldBatchLenDiff(
	fieldDelta *indexdata.LenPostingDiff,
	idx uint64,
	diff batchLenDiff,
	useZeroComplement bool,
) (*indexdata.LenPostingDiff, bool) {
	logicalChange := diff.oldExists != diff.newExists || diff.oldLen != diff.newLen
	if !logicalChange {
		return fieldDelta, false
	}

	var changed bool

	if !useZeroComplement {
		if diff.oldExists {
			indexdata.AddLenPostingBucketDiff(&fieldDelta, idx, diff.oldLen, false)
			changed = true
		}
		if diff.newExists {
			indexdata.AddLenPostingBucketDiff(&fieldDelta, idx, diff.newLen, true)
			changed = true
		}
		return fieldDelta, changed
	}

	if diff.oldExists {
		if diff.oldLen > 0 {
			indexdata.AddLenPostingBucketDiff(&fieldDelta, idx, diff.oldLen, false)
			changed = true
		}
		if diff.oldLen > 0 && (!diff.newExists || diff.newLen == 0) {
			indexdata.AddLenPostingNonEmptyDiff(&fieldDelta, idx, false)
			changed = true
		}
	}
	if diff.newExists {
		if diff.newLen > 0 {
			indexdata.AddLenPostingBucketDiff(&fieldDelta, idx, diff.newLen, true)
			changed = true
		}
		if diff.newLen > 0 && (!diff.oldExists || diff.oldLen == 0) {
			indexdata.AddLenPostingNonEmptyDiff(&fieldDelta, idx, true)
			changed = true
		}
	}

	return fieldDelta, changed || logicalChange
}

func (qe *queryEngine) collectSnapshotBatchEntryDiffs(
	op snapshotBatchEntry,
	deltas *indexedFieldBatchDeltas,
	lenZeroComplement []bool,
	patchMap map[string]*field,
) {
	if op.patchOnly {
		for i, patchField := range op.patch {
			fieldDef, ok := patchMap[patchField.Name]
			if !ok {
				continue
			}
			acc, ok := qe.indexedFieldMap[fieldDef.DBName]
			if !ok {
				continue
			}
			duplicate := false
			for j := 0; j < i; j++ {
				prev, ok := patchMap[op.patch[j].Name]
				if ok && prev.DBName == fieldDef.DBName {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
			deltas.markTouched(acc.ordinal)
			useZeroComplement := acc.ordinal < len(lenZeroComplement) && lenZeroComplement[acc.ordinal]
			acc.collectSnapshotBatchDiff(op.idx, op.oldVal, op.newVal, useZeroComplement, &deltas.fields[acc.ordinal])
		}
		return
	}

	if op.oldVal == nil || op.newVal == nil {
		for _, acc := range qe.indexedFieldAccess {
			deltas.markTouched(acc.ordinal)
			useZeroComplement := acc.ordinal < len(lenZeroComplement) && lenZeroComplement[acc.ordinal]
			acc.collectSnapshotBatchDiff(op.idx, op.oldVal, op.newVal, useZeroComplement, &deltas.fields[acc.ordinal])
		}
		return
	}

	for _, acc := range qe.indexedFieldAccess {
		if acc.modified == nil || !acc.modified(op.oldVal, op.newVal) {
			continue
		}
		deltas.markTouched(acc.ordinal)
		useZeroComplement := acc.ordinal < len(lenZeroComplement) && lenZeroComplement[acc.ordinal]
		acc.collectSnapshotBatchDiff(op.idx, op.oldVal, op.newVal, useZeroComplement, &deltas.fields[acc.ordinal])
	}
}

func (qe *queryEngine) buildPreparedSnapshotAggregatedNoLock(
	seq uint64,
	prev *indexSnapshot,
	strMap *strMapper,
	patchMap map[string]*field,
	entries []snapshotBatchEntry,
) *indexSnapshot {

	var strmap *strMapSnapshot
	if strMap != nil {
		strmap = strMap.snapshot()
	}
	next := &indexSnapshot{
		seq: seq,

		index:              indexdata.CloneFieldStorageSlots(prev.index, len(qe.indexedFieldAccess)),
		nilIndex:           indexdata.CloneFieldStorageSlots(prev.nilIndex, len(qe.indexedFieldAccess)),
		lenIndex:           indexdata.CloneFieldStorageSlots(prev.lenIndex, len(qe.indexedFieldAccess)),
		lenZeroComplement:  cloneFieldIndexBoolSlots(prev.lenZeroComplement, len(qe.indexedFieldAccess)),
		measure:            indexdata.CloneMeasureStorageSlots(prev.measure, len(qe.measureFieldAccess)),
		indexedFieldByName: qe.indexedFieldMap,
		universe:           prev.universe,
		universeOwner:      prev.universeOwner,
		strmap:             strmap,
	}
	qe.initSnapshotRuntimeCaches(next)

	normalized := normalizePreparedBatchForSnapshot(entries)
	deltas := indexedFieldBatchDeltas{
		fields:  snapshotFieldBatchStateSlicePool.Get(len(qe.indexedFieldAccess)),
		touched: pooled.GetIntSlice(len(qe.indexedFieldAccess)),
		changed: pooled.GetBoolSlice(len(qe.indexedFieldAccess))[:len(qe.indexedFieldAccess)],
	}
	deltas.fields = deltas.fields[:len(qe.indexedFieldAccess)]
	clear(deltas.changed)
	measureDeltas := indexdata.NewMeasureDeltaBatch(len(qe.measureFieldAccess))

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
		qe.collectSnapshotBatchEntryDiffs(op, &deltas, prev.lenZeroComplement, patchMap)
		qe.collectSnapshotMeasureEntryDiffs(op, &measureDeltas, patchMap)
	}

	for i := range deltas.touched {
		deltas.changed[deltas.touched[i]] = false
	}

	changedCount := 0

	for i := range deltas.touched {
		ordinal := deltas.touched[i]
		acc := qe.indexedFieldAccess[ordinal]
		state := &deltas.fields[ordinal]
		baseIndex := next.index[ordinal]
		if storage := acc.applySnapshotBatchStorageOwned(baseIndex, state, true); storage.KeyCount() == 0 {
			if baseIndex.KeyCount() > 0 {
				next.index[ordinal] = indexdata.FieldStorage{}
			}
		} else if storage != baseIndex {
			next.index[ordinal] = storage
		}
		baseNil := next.nilIndex[ordinal]
		if storage := acc.applySnapshotBatchNilStorageOwned(baseNil, state); storage.KeyCount() == 0 {
			if baseNil.KeyCount() > 0 {
				next.nilIndex[ordinal] = indexdata.FieldStorage{}
			}
		} else if storage != baseNil {
			next.nilIndex[ordinal] = storage
		}
		if state.lengths != nil {
			baseLen := next.lenIndex[ordinal]
			if storage := baseLen.ApplyLenPostingDiffRetainOwned(state.lengths); storage != baseLen {
				next.lenIndex[ordinal] = storage
			}
		}
		if state.changed {
			changedCount++
			deltas.changed[ordinal] = true
		}
		state.release()
	}
	measureDeltas.ApplyToMeasureStorageSlotsOwned(next.measure)
	measureDeltas.Release()
	inheritNumericRangeBucketCache(next, prev)
	if changedCount > 0 {
		inheritMaterializedPredCache(next, prev, qe.indexedFieldMap, deltas.changed)
	} else {
		inheritMaterializedPredCache(next, prev, qe.indexedFieldMap, nil)
	}
	snapshotFieldBatchStateSlicePool.Put(deltas.fields)
	pooled.ReleaseBoolSlice(deltas.changed)
	pooled.ReleaseIntSlice(deltas.touched)
	next.retainSharedOwnedStorageFrom(prev)

	return next
}
