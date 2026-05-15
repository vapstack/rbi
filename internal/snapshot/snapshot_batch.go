package snapshot

import (
	"unsafe"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/strmap"
)

type BatchEntry struct {
	ID        uint64
	Old       unsafe.Pointer
	New       unsafe.Pointer
	Patch     []schema.PatchItem
	PatchOnly bool
}

func BuildPrepared(seq uint64, prev *View, rt *schema.Runtime, cfg CacheConfig, strMap *strmap.Mapper, patchFields map[string]*schema.Field, entries []BatchEntry) *View {
	if snap, ok := buildPreparedSnapshotFromEmptyBase(seq, prev, rt, cfg, strMap, entries); ok {
		return snap
	}
	if snap, ok := buildPreparedSnapshotInsertOnly(seq, prev, rt, cfg, strMap, entries); ok {
		return snap
	}
	return buildPreparedSnapshotAggregated(seq, prev, rt, cfg, strMap, patchFields, entries)
}

func cloneFieldIndexBoolSlots(src []bool, size int) []bool {
	out := pooled.GetBoolSlice(size)[:size]
	n := min(size, len(src))
	copy(out, src[:n])
	clear(out[n:])
	return out
}

func ensureSnapshotUniverseOwned(next *View, universeOwned *bool) {
	if *universeOwned {
		return
	}
	next.Universe = next.Universe.Clone()
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

func normalizePreparedBatchForSnapshot(entries []BatchEntry) []BatchEntry {
	if len(entries) == 0 {
		return nil
	}
	if len(entries) == 1 {
		op := entries[0]
		if op.Old == nil && op.New == nil {
			return nil
		}
		return entries[:1]
	}

	ordered := true
	hasNoop := entries[0].Old == nil && entries[0].New == nil
	prevID := entries[0].ID
	for i := 1; i < len(entries); i++ {
		op := entries[i]
		if op.Old == nil && op.New == nil {
			hasNoop = true
		}
		if op.ID <= prevID {
			ordered = false
			break
		}
		prevID = op.ID
	}
	if ordered {
		if !hasNoop {
			return entries
		}
		write := 0
		for i := range entries {
			if entries[i].Old == nil && entries[i].New == nil {
				continue
			}
			if write != i {
				entries[write] = entries[i]
			}
			write++
		}
		return entries[:write]
	}

	pos := uint64IntMapPool.Get(len(entries))

	n := 0
	for i := range entries {
		op := entries[i]
		if p := pos[op.ID]; p != 0 {
			// Repeated-id entries collapse to first oldVal -> last newVal.
			// Patch metadata from any individual request is no longer sufficient
			// to describe the aggregated diff across the whole chain, so disable
			// the patch-only fast path for this normalized entry.
			entries[p-1].Patch = nil
			entries[p-1].PatchOnly = false
			entries[p-1].New = op.New
			continue
		}
		pos[op.ID] = n + 1
		if n != i {
			entries[n] = op
		}
		n++
	}

	write := 0
	for i := 0; i < n; i++ {
		if entries[i].Old == nil && entries[i].New == nil {
			continue
		}
		if write != i {
			entries[write] = entries[i]
		}
		write++
	}
	normalized := entries[:write]
	uint64IntMapPool.Put(pos)
	return normalized
}

func collectSnapshotBatchEntryDiffs(
	rt *schema.Runtime,
	op BatchEntry,
	deltas *indexedFieldBatchDeltas,
	measureDeltas *indexdata.MeasureDeltaBatch,
	lenZeroComplement []bool,
	patchFields map[string]*schema.Field,
) {
	if op.PatchOnly {
		for i, patchField := range op.Patch {
			fieldDef, ok := patchFields[patchField.Name]
			if !ok {
				continue
			}
			indexAcc, hasIndex := rt.IndexedByName[fieldDef.DBName]
			measureAcc, hasMeasure := rt.MeasuresByName[fieldDef.DBName]
			if !hasIndex && !hasMeasure {
				continue
			}
			duplicate := false
			for j := 0; j < i; j++ {
				prev, ok := patchFields[op.Patch[j].Name]
				if ok && prev.DBName == fieldDef.DBName {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
			if hasIndex {
				deltas.markTouched(indexAcc.Ordinal)
				useZeroComplement := indexAcc.Ordinal < len(lenZeroComplement) && lenZeroComplement[indexAcc.Ordinal]
				indexAcc.CollectBatchDiff(op.ID, op.Old, op.New, useZeroComplement, &deltas.fields[indexAcc.Ordinal])
			}
			if hasMeasure {
				collectSnapshotMeasureDelta(measureAcc, op.ID, op.Old, op.New, measureDeltas)
			}
		}
		return
	}

	if op.Old == nil || op.New == nil {
		for _, acc := range rt.Indexed {
			deltas.markTouched(acc.Ordinal)
			useZeroComplement := acc.Ordinal < len(lenZeroComplement) && lenZeroComplement[acc.Ordinal]
			acc.CollectBatchDiff(op.ID, op.Old, op.New, useZeroComplement, &deltas.fields[acc.Ordinal])
		}
		for _, acc := range rt.Measures {
			collectSnapshotMeasureDelta(acc, op.ID, op.Old, op.New, measureDeltas)
		}
		return
	}

	for _, acc := range rt.Indexed {
		if !acc.Modified(op.Old, op.New) {
			continue
		}
		deltas.markTouched(acc.Ordinal)
		useZeroComplement := acc.Ordinal < len(lenZeroComplement) && lenZeroComplement[acc.Ordinal]
		acc.CollectBatchDiff(op.ID, op.Old, op.New, useZeroComplement, &deltas.fields[acc.Ordinal])
	}
	for _, acc := range rt.Measures {
		if acc.Modified(op.Old, op.New) {
			collectSnapshotMeasureDelta(acc, op.ID, op.Old, op.New, measureDeltas)
		}
	}
}

func buildPreparedSnapshotFromEmptyBase(seq uint64, prev *View, rt *schema.Runtime, cfg CacheConfig, strMap *strmap.Mapper, entries []BatchEntry) (*View, bool) {
	if prev != nil && !prev.Universe.IsEmpty() {
		return nil, false
	}
	if len(entries) == 0 {
		return nil, false
	}
	for i := range entries {
		if entries[i].Old != nil || entries[i].New == nil {
			return nil, false
		}
	}

	var universe posting.List
	hasRepeated := false
	for i := range entries {
		var added bool
		universe, added = universe.BuildAddedChecked(entries[i].ID)
		if !added {
			hasRepeated = true
		}
	}

	var lastByIdx map[uint64]int
	if hasRepeated {
		lastByIdx = uint64IntMapPool.Get(len(entries))
		for i := range entries {
			lastByIdx[entries[i].ID] = i
		}
	}

	fieldStates := schema.GetOverlayStates(len(rt.Indexed))
	measureStates := indexdata.GetMeasureEntrySlots(len(rt.Measures))

	for i := range entries {
		if hasRepeated && lastByIdx[entries[i].ID] != i {
			continue
		}
		op := entries[i]
		ptr := op.New

		for _, acc := range rt.Indexed {
			acc.CollectOverlayValue(ptr, op.ID, &fieldStates[acc.Ordinal])
		}
		for _, acc := range rt.Measures {
			if value, ok := acc.Read(ptr); ok {
				buf := measureStates[acc.Ordinal]
				if buf == nil {
					buf = indexdata.GetMeasureEntrySlice(0)
					measureStates[acc.Ordinal] = buf
				}
				buf = append(buf, indexdata.MeasureEntry{ID: op.ID, Value: value})
				measureStates[acc.Ordinal] = buf
			}
		}
	}
	if hasRepeated {
		uint64IntMapPool.Put(lastByIdx)
	}

	slotCount := len(rt.Indexed)
	nextIndex := indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	for i, acc := range rt.Indexed {
		state := &fieldStates[i]
		storage := state.MaterializeStorage(acc.Field.KeyKind == schema.FieldWriteKeysOrderedU64)
		if storage.KeyCount() > 0 {
			nextIndex[i] = storage
		}
	}

	nextNilIndex := indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	for i := range rt.Indexed {
		if storage := fieldStates[i].MaterializeNilStorage(); storage.KeyCount() > 0 {
			nextNilIndex[i] = storage
		}
	}

	nextLenIndex := indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	nextLenZeroComplement := pooled.GetBoolSlice(slotCount)[:slotCount]
	clear(nextLenZeroComplement)
	for i, acc := range rt.Indexed {
		if !acc.Field.Slice {
			continue
		}
		storage, useZeroComplement := fieldStates[i].MaterializeLenStorage(universe)
		nextLenIndex[i] = storage
		if useZeroComplement {
			nextLenZeroComplement[i] = true
		}
	}
	measureSlotCount := len(rt.Measures)
	nextMeasure := indexdata.GetMeasureStorageSlice(measureSlotCount)[:measureSlotCount]
	for i := range rt.Measures {
		storage := indexdata.NewMeasureStorageFromEntriesOwned(measureStates[i])
		nextMeasure[i] = storage
		measureStates[i] = nil
	}

	var sm *strmap.Snapshot
	if strMap != nil {
		sm = strMap.Snapshot()
	}
	snap := &View{
		Seq:                seq,
		Index:              nextIndex,
		NilIndex:           nextNilIndex,
		LenIndex:           nextLenIndex,
		LenZeroComplement:  nextLenZeroComplement,
		Measure:            nextMeasure,
		IndexedFieldByName: rt.IndexedByName,
		Universe:           universe,
		StrMap:             sm,
	}
	snap.initRuntimeCaches(rt, cfg)
	inheritNumericRangeBucketCache(snap, prev)
	if prev != nil && snap.matPredCache != nil && prev.matPredCache != nil {
		var changed []bool
		for i := range fieldStates {
			if fieldStates[i].Changed() {
				if changed == nil {
					changed = pooled.GetBoolSlice(len(rt.Indexed))[:len(rt.Indexed)]
					clear(changed)
				}
				changed[i] = true
			}
		}
		inheritMaterializedPredCache(snap, prev, rt.IndexedByName, changed)
		if changed != nil {
			pooled.ReleaseBoolSlice(changed)
		}
	}
	schema.ReleaseOverlayStates(fieldStates)
	indexdata.ReleaseMeasureEntrySlots(measureStates)
	snap.ensureUniverseOwner()
	return snap, true
}

func buildPreparedSnapshotInsertOnly(seq uint64, prev *View, rt *schema.Runtime, cfg CacheConfig, strMap *strmap.Mapper, entries []BatchEntry) (*View, bool) {
	if len(entries) == 0 {
		return nil, false
	}
	for i := range entries {
		if entries[i].Old != nil || entries[i].New == nil {
			return nil, false
		}
	}
	var addedUniverse posting.List
	for i := range entries {
		var added bool
		addedUniverse, added = addedUniverse.BuildAddedChecked(entries[i].ID)
		if !added {
			addedUniverse.Release()
			return nil, false
		}
	}

	var sm *strmap.Snapshot
	if strMap != nil {
		sm = strMap.Snapshot()
	}
	next := &View{
		Seq: seq,

		Index:              indexdata.CloneFieldStorageSlots(prev.Index, len(rt.Indexed)),
		NilIndex:           indexdata.CloneFieldStorageSlots(prev.NilIndex, len(rt.Indexed)),
		LenIndex:           indexdata.CloneFieldStorageSlots(prev.LenIndex, len(rt.Indexed)),
		LenZeroComplement:  cloneFieldIndexBoolSlots(prev.LenZeroComplement, len(rt.Indexed)),
		Measure:            indexdata.CloneMeasureStorageSlots(prev.Measure, len(rt.Measures)),
		IndexedFieldByName: rt.IndexedByName,
		Universe:           prev.Universe.Clone(),
		StrMap:             sm,
	}
	next.Universe = next.Universe.BuildMergedOwned(addedUniverse)
	next.initRuntimeCaches(rt, cfg)

	fieldStates := schema.GetInsertStates(len(rt.Indexed))
	schema.InitInsertStateHints(fieldStates, rt.Indexed, prev.Index, prev.NilIndex, prev.LenIndex, len(entries))
	measureDeltas := indexdata.NewMeasureDeltaBatch(len(rt.Measures))

	for i := range entries {
		op := entries[i]
		ptr := op.New

		for _, acc := range rt.Indexed {
			useZeroComplement := acc.Ordinal < len(prev.LenZeroComplement) && prev.LenZeroComplement[acc.Ordinal]
			acc.CollectInsertValue(ptr, op.ID, useZeroComplement, &fieldStates[acc.Ordinal])
		}
		for _, acc := range rt.Measures {
			if value, ok := acc.Read(ptr); ok {
				measureDeltas.Append(acc.Ordinal, op.ID, true, value)
			}
		}
	}

	inheritMatPred := prev != nil && next.matPredCache != nil && prev.matPredCache != nil
	var changed []bool
	for i, acc := range rt.Indexed {
		state := &fieldStates[i]
		baseIndex := next.Index[i]
		if storage := acc.MergeInsertStorageOwned(baseIndex, state, true); storage.KeyCount() > 0 {
			if storage != baseIndex {
				next.Index[i] = storage
			}
		} else if baseIndex.KeyCount() > 0 {
			next.Index[i] = indexdata.FieldStorage{}
		}
		baseNil := next.NilIndex[i]
		if storage := acc.MergeInsertNilStorageOwned(baseNil, state); storage.KeyCount() > 0 {
			if storage != baseNil {
				next.NilIndex[i] = storage
			}
		} else if baseNil.KeyCount() > 0 {
			next.NilIndex[i] = indexdata.FieldStorage{}
		}
		if lenDiff := state.LenDiff(); lenDiff != nil {
			baseLen := next.LenIndex[i]
			if storage := baseLen.ApplyLenPostingDiffRetainOwned(lenDiff); storage != baseLen {
				next.LenIndex[i] = storage
			}
		}
		if inheritMatPred && state.Changed() {
			if changed == nil {
				changed = pooled.GetBoolSlice(len(rt.Indexed))[:len(rt.Indexed)]
				clear(changed)
			}
			changed[i] = true
		}
		state.Reset()
	}
	measureDeltas.ApplyToMeasureStorageSlotsOwned(next.Measure)
	measureDeltas.Release()
	inheritNumericRangeBucketCache(next, prev)

	if inheritMatPred {
		inheritMaterializedPredCache(next, prev, rt.IndexedByName, changed)
		if changed != nil {
			pooled.ReleaseBoolSlice(changed)
		}
	}
	next.retainSharedOwnedStorageFrom(prev)
	schema.ReleaseInsertStates(fieldStates)

	return next, true
}

func buildPreparedSnapshotDeletedAll(seq uint64, rt *schema.Runtime, cfg CacheConfig, strMap *strmap.Mapper) *View {
	slotCount := len(rt.Indexed)
	nextIndex := indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	nextNilIndex := indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	nextLenIndex := indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	nextLenZeroComplement := pooled.GetBoolSlice(slotCount)[:slotCount]
	clear(nextLenZeroComplement)

	measureSlotCount := len(rt.Measures)
	nextMeasure := indexdata.GetMeasureStorageSlice(measureSlotCount)[:measureSlotCount]

	var sm *strmap.Snapshot
	if strMap != nil {
		sm = strMap.Snapshot()
	}
	next := &View{
		Seq:                seq,
		Index:              nextIndex,
		NilIndex:           nextNilIndex,
		LenIndex:           nextLenIndex,
		LenZeroComplement:  nextLenZeroComplement,
		Measure:            nextMeasure,
		IndexedFieldByName: rt.IndexedByName,
		StrMap:             sm,
	}
	next.initRuntimeCaches(rt, cfg)
	next.ensureUniverseOwner()
	return next
}

func buildPreparedSnapshotFullReplace(seq uint64, prev *View, rt *schema.Runtime, cfg CacheConfig, strMap *strmap.Mapper, entries []BatchEntry) *View {
	fieldStates := schema.GetOverlayStates(len(rt.Indexed))
	measureStates := indexdata.GetMeasureEntrySlots(len(rt.Measures))

	for i := range entries {
		op := entries[i]
		ptr := op.New

		for _, acc := range rt.Indexed {
			acc.CollectOverlayValue(ptr, op.ID, &fieldStates[acc.Ordinal])
		}
		for _, acc := range rt.Measures {
			if value, ok := acc.Read(ptr); ok {
				buf := measureStates[acc.Ordinal]
				if buf == nil {
					buf = indexdata.GetMeasureEntrySlice(0)
					measureStates[acc.Ordinal] = buf
				}
				buf = append(buf, indexdata.MeasureEntry{ID: op.ID, Value: value})
				measureStates[acc.Ordinal] = buf
			}
		}
	}

	slotCount := len(rt.Indexed)
	nextIndex := indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	for i, acc := range rt.Indexed {
		state := &fieldStates[i]
		storage := state.MaterializeStorage(acc.Field.KeyKind == schema.FieldWriteKeysOrderedU64)
		if storage.KeyCount() > 0 {
			nextIndex[i] = storage
		}
	}

	nextNilIndex := indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	for i := range rt.Indexed {
		if storage := fieldStates[i].MaterializeNilStorage(); storage.KeyCount() > 0 {
			nextNilIndex[i] = storage
		}
	}

	nextLenIndex := indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	nextLenZeroComplement := pooled.GetBoolSlice(slotCount)[:slotCount]
	clear(nextLenZeroComplement)
	for i, acc := range rt.Indexed {
		if !acc.Field.Slice {
			continue
		}
		storage, useZeroComplement := fieldStates[i].MaterializeLenStorage(prev.Universe)
		nextLenIndex[i] = storage
		if useZeroComplement {
			nextLenZeroComplement[i] = true
		}
	}

	measureSlotCount := len(rt.Measures)
	nextMeasure := indexdata.GetMeasureStorageSlice(measureSlotCount)[:measureSlotCount]
	for i := range rt.Measures {
		storage := indexdata.NewMeasureStorageFromEntriesOwned(measureStates[i])
		nextMeasure[i] = storage
		measureStates[i] = nil
	}

	var sm *strmap.Snapshot
	if strMap != nil {
		sm = strMap.Snapshot()
	}
	next := &View{
		Seq: seq,

		Index:              nextIndex,
		NilIndex:           nextNilIndex,
		LenIndex:           nextLenIndex,
		LenZeroComplement:  nextLenZeroComplement,
		Measure:            nextMeasure,
		IndexedFieldByName: rt.IndexedByName,
		Universe:           prev.Universe,
		universeOwner:      prev.universeOwner,
		StrMap:             sm,
	}
	next.initRuntimeCaches(rt, cfg)
	schema.ReleaseOverlayStates(fieldStates)
	indexdata.ReleaseMeasureEntrySlots(measureStates)
	next.retainSharedOwnedStorageFrom(prev)
	return next
}

func buildPreparedSnapshotAggregated(
	seq uint64,
	prev *View,
	rt *schema.Runtime,
	cfg CacheConfig,
	strMap *strmap.Mapper,
	patchFields map[string]*schema.Field,
	entries []BatchEntry,
) *View {
	normalized := normalizePreparedBatchForSnapshot(entries)
	if prevCard := prev.Universe.Cardinality(); prevCard == uint64(len(normalized)) && prevCard > 0 {
		allDelete := true
		allReplace := true
		var ids posting.List
		for i := range normalized {
			op := normalized[i]
			if op.Old == nil || op.New != nil {
				allDelete = false
			}
			if op.Old == nil || op.New == nil || op.PatchOnly {
				allReplace = false
			}
			if !allDelete && !allReplace {
				break
			}
			ids = ids.BuildAdded(op.ID)
		}
		if allDelete || allReplace {
			if ids.Cardinality() == prevCard && prev.Universe.AndCardinality(ids) == prevCard {
				if allDelete {
					ids.Release()
					return buildPreparedSnapshotDeletedAll(seq, rt, cfg, strMap)
				}
				if (prev.matPredCache == nil || prev.matPredCache.EntryCount() == 0) &&
					(prev.numericRangeBucketCache == nil || prev.numericRangeBucketCache.EntryCount() == 0) {
					ids.Release()
					return buildPreparedSnapshotFullReplace(seq, prev, rt, cfg, strMap, normalized)
				}
			}
		}
		ids.Release()
	}

	var sm *strmap.Snapshot
	if strMap != nil {
		sm = strMap.Snapshot()
	}
	next := &View{
		Seq: seq,

		Index:              indexdata.CloneFieldStorageSlots(prev.Index, len(rt.Indexed)),
		NilIndex:           indexdata.CloneFieldStorageSlots(prev.NilIndex, len(rt.Indexed)),
		LenIndex:           indexdata.CloneFieldStorageSlots(prev.LenIndex, len(rt.Indexed)),
		LenZeroComplement:  cloneFieldIndexBoolSlots(prev.LenZeroComplement, len(rt.Indexed)),
		Measure:            indexdata.CloneMeasureStorageSlots(prev.Measure, len(rt.Measures)),
		IndexedFieldByName: rt.IndexedByName,
		Universe:           prev.Universe,
		universeOwner:      prev.universeOwner,
		StrMap:             sm,
	}
	next.initRuntimeCaches(rt, cfg)

	deltas := indexedFieldBatchDeltas{
		fields:  schema.GetBatchStates(len(rt.Indexed)),
		touched: pooled.GetIntSlice(len(rt.Indexed)),
		changed: pooled.GetBoolSlice(len(rt.Indexed))[:len(rt.Indexed)],
	}
	clear(deltas.changed)
	measureDeltas := indexdata.NewMeasureDeltaBatch(len(rt.Measures))

	universeOwned := false

	for i := range normalized {
		op := normalized[i]
		switch {
		case op.Old == nil && op.New != nil:
			ensureSnapshotUniverseOwned(next, &universeOwned)
			next.Universe = next.Universe.BuildAdded(op.ID)
		case op.Old != nil && op.New == nil:
			ensureSnapshotUniverseOwned(next, &universeOwned)
			next.Universe = next.Universe.BuildRemoved(op.ID)
		}
		collectSnapshotBatchEntryDiffs(rt, op, &deltas, &measureDeltas, prev.LenZeroComplement, patchFields)
	}

	inheritMatPred := prev != nil && next.matPredCache != nil && prev.matPredCache != nil
	if inheritMatPred {
		for i := range deltas.touched {
			deltas.changed[deltas.touched[i]] = false
		}
	}

	changedAny := false
	for i := range deltas.touched {
		ordinal := deltas.touched[i]
		acc := rt.Indexed[ordinal]
		state := &deltas.fields[ordinal]
		baseIndex := next.Index[ordinal]
		if storage := acc.ApplyBatchStorageOwned(baseIndex, state, true); storage.KeyCount() == 0 {
			if baseIndex.KeyCount() > 0 {
				next.Index[ordinal] = indexdata.FieldStorage{}
			}
		} else if storage != baseIndex {
			next.Index[ordinal] = storage
		}
		baseNil := next.NilIndex[ordinal]
		if storage := acc.ApplyBatchNilStorageOwned(baseNil, state); storage.KeyCount() == 0 {
			if baseNil.KeyCount() > 0 {
				next.NilIndex[ordinal] = indexdata.FieldStorage{}
			}
		} else if storage != baseNil {
			next.NilIndex[ordinal] = storage
		}
		if lenDiff := state.LenDiff(); lenDiff != nil {
			baseLen := next.LenIndex[ordinal]
			if storage := baseLen.ApplyLenPostingDiffRetainOwned(lenDiff); storage != baseLen {
				next.LenIndex[ordinal] = storage
			}
		}
		if inheritMatPred && state.Changed() {
			changedAny = true
			deltas.changed[ordinal] = true
		}
		state.Reset()
	}
	measureDeltas.ApplyToMeasureStorageSlotsOwned(next.Measure)
	measureDeltas.Release()
	inheritNumericRangeBucketCache(next, prev)
	if inheritMatPred {
		if changedAny {
			inheritMaterializedPredCache(next, prev, rt.IndexedByName, deltas.changed)
		} else {
			inheritMaterializedPredCache(next, prev, rt.IndexedByName, nil)
		}
	}
	schema.ReleaseBatchStates(deltas.fields)
	pooled.ReleaseBoolSlice(deltas.changed)
	pooled.ReleaseIntSlice(deltas.touched)
	next.retainSharedOwnedStorageFrom(prev)

	return next
}
