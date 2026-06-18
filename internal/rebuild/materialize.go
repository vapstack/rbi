package rebuild

import (
	"cmp"
	"fmt"
	"runtime"
	"slices"
	"time"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"go.etcd.io/bbolt"
)

const fieldMaterializeGCTargets = 4

func finishLoadedStorage(cfg Config, state State) (Result, error) {
	if cfg.StrKey {
		maxStringIdx, ok := state.Universe.Maximum()
		if ok {
			err := cfg.Bolt.View(func(tx *bbolt.Tx) error {
				stringMap := tx.Bucket(cfg.StrMapBucket)
				if stringMap == nil {
					return fmt.Errorf("string storage format: missing string map bucket %q", cfg.StrMapBucket)
				}
				seq := stringMap.Sequence()
				if seq < maxStringIdx {
					return fmt.Errorf("string storage format: string map sequence %d lower than max live idx %d", seq, maxStringIdx)
				}
				return nil
			})
			if err != nil {
				return Result{}, err
			}
		}
	}
	if state.LenLoaded {
		return Result{KeyIndexLoaded: cfg.KeyIndexLoaded}, nil
	}
	nextLenIndex, nextLenZeroComplement := buildLenIndexStorage(cfg.Schema, state.Index, state.Universe)
	return Result{
		Publish:        true,
		LenLoaded:      false,
		KeyIndexLoaded: cfg.KeyIndexLoaded,
		Storage: snapshot.Storage{
			Index:             indexdata.CloneFieldStorageSlots(state.Index, len(cfg.Schema.Indexed)),
			KeyIndex:          state.KeyIndex,
			NilIndex:          indexdata.CloneFieldStorageSlots(state.NilIndex, len(cfg.Schema.Indexed)),
			LenIndex:          nextLenIndex,
			LenZeroComplement: nextLenZeroComplement,
			Measure:           indexdata.CloneMeasureStorageSlots(state.Measure, len(cfg.Schema.Measures)),
			Universe:          state.Universe,
		},
	}, nil
}

func materialize(cfg Config, state State, active []buildField, activeMeasures []schema.MeasureFieldAccessor, build *buildData, start time.Time) Result {
	slotCount := len(cfg.Schema.Indexed)
	firstFullBuild := cfg.Current == nil && len(cfg.SkipFields) == 0 && len(cfg.SkipMeasureFields) == 0

	var nextIndex []indexdata.FieldStorage
	if firstFullBuild {
		for i := range state.Index {
			state.Index[i].Release()
			state.Index[i] = indexdata.FieldStorage{}
		}
		nextIndex = state.Index

	} else if len(cfg.SkipFields) == 0 {
		nextIndex = indexdata.GetFieldStorageSlice(slotCount)[:slotCount]

	} else {
		nextIndex = indexdata.CloneFieldStorageSlots(state.Index, slotCount)
	}

	var nextNilIndex []indexdata.FieldStorage
	if firstFullBuild {
		for i := range state.NilIndex {
			state.NilIndex[i].Release()
			state.NilIndex[i] = indexdata.FieldStorage{}
		}
		nextNilIndex = state.NilIndex

	} else if len(cfg.SkipFields) == 0 {
		nextNilIndex = indexdata.GetFieldStorageSlice(slotCount)[:slotCount]

	} else {
		nextNilIndex = indexdata.CloneFieldStorageSlots(state.NilIndex, slotCount)
	}

	var nextLenIndex []indexdata.FieldStorage
	var nextLenZeroComplement []bool

	if firstFullBuild {
		for i := range state.LenIndex {
			state.LenIndex[i].Release()
			state.LenIndex[i] = indexdata.FieldStorage{}
		}
		nextLenIndex = state.LenIndex
		nextLenZeroComplement = state.LenZeroComplement
		clear(nextLenZeroComplement)

	} else if len(cfg.SkipFields) == 0 {
		nextLenIndex = indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
		nextLenZeroComplement = pooled.GetBoolSlice(slotCount)[:slotCount]
		clear(nextLenZeroComplement)

	} else {
		nextLenIndex = indexdata.CloneFieldStorageSlots(state.LenIndex, slotCount)
		nextLenZeroComplement = pooled.GetBoolSlice(slotCount)[:slotCount]
		clear(nextLenZeroComplement)
		copy(nextLenZeroComplement, state.LenZeroComplement[:min(slotCount, len(state.LenZeroComplement))])
	}

	measureSlotCount := len(cfg.Schema.Measures)
	var nextMeasure []indexdata.MeasureStorage

	if firstFullBuild {
		for i := range state.Measure {
			state.Measure[i].Release()
			state.Measure[i] = indexdata.MeasureStorage{}
		}
		nextMeasure = state.Measure
		state.Universe.Release()
		state.Universe = posting.List{}

	} else if len(cfg.SkipMeasureFields) == 0 {
		nextMeasure = indexdata.GetMeasureStorageSlice(measureSlotCount)[:measureSlotCount]

	} else {
		nextMeasure = indexdata.CloneMeasureStorageSlots(state.Measure, measureSlotCount)
	}

	nextUniverse := posting.List{}
	for i := range build.localUniverse {
		if build.localUniverse[i].IsEmpty() {
			continue
		}
		nextUniverse = nextUniverse.BuildMergedOwned(build.localUniverse[i])
		build.localUniverse[i] = posting.List{}
	}

	nextKeyIndex := state.KeyIndex
	keyIndexLoaded := cfg.KeyIndexLoaded
	if cfg.StrKey && cfg.StrKeyIndex && !cfg.KeyIndexLoaded {
		nextKeyIndex = build.keyIndex
		build.keyIndex = indexdata.FieldStorage{}
		keyIndexLoaded = true
	}

	fieldOrder := pooled.GetIntSlice(len(build.fieldStates))
	fieldRunKeys := pooled.GetIntSlice(len(build.fieldStates))[:len(build.fieldStates)]
	fieldWork := pooled.GetUint64Slice(len(build.fieldStates))[:len(build.fieldStates)]

	var totalFieldWork uint64
	for i := range build.fieldStates {
		runKeys, work := build.fieldStates[i].MaterializeStats()
		fieldRunKeys[i] = runKeys
		fieldWork[i] = work
		totalFieldWork += work
		fieldOrder = append(fieldOrder, i)
	}
	for i := 1; i < len(fieldOrder); i++ {
		field := fieldOrder[i]
		keys := fieldRunKeys[field]
		j := i
		for j > 0 && fieldRunKeys[fieldOrder[j-1]] < keys {
			fieldOrder[j] = fieldOrder[j-1]
			j--
		}
		fieldOrder[j] = field
	}
	var fieldGCWork uint64
	if totalFieldWork > 0 {
		fieldGCWork = totalFieldWork / fieldMaterializeGCTargets
		if totalFieldWork%fieldMaterializeGCTargets != 0 {
			fieldGCWork++
		}
	}

	var pendingFieldGCWork uint64
	for pos, i := range fieldOrder {
		ordinal := active[i].acc.Ordinal
		if storage := build.fieldStates[i].MaterializeStorage(); storage.KeyCount() > 0 {
			nextIndex[ordinal] = storage
		} else {
			nextIndex[ordinal] = indexdata.FieldStorage{}
		}
		if storage := build.fieldStates[i].MaterializeNilStorage(); storage.KeyCount() > 0 {
			nextNilIndex[ordinal] = storage
		} else {
			nextNilIndex[ordinal] = indexdata.FieldStorage{}
		}
		if storage, useZeroComplement := build.fieldStates[i].MaterializeLenStorage(nextUniverse); active[i].slice {
			if storage.KeyCount() > 0 {
				nextLenIndex[ordinal] = storage
			} else {
				nextLenIndex[ordinal] = indexdata.FieldStorage{}
			}
			nextLenZeroComplement[ordinal] = false
			if useZeroComplement {
				nextLenZeroComplement[ordinal] = true
			}
		}
		pendingFieldGCWork += fieldWork[i]
		if fieldGCWork > 0 &&
			(pos+1 < len(fieldOrder) || len(activeMeasures) > 0) &&
			(pendingFieldGCWork >= fieldGCWork || (pos+1 == len(fieldOrder) && pendingFieldGCWork > 0)) {
			runtime.GC()
			pendingFieldGCWork = 0
		}
	}
	pooled.ReleaseUint64Slice(fieldWork)
	pooled.ReleaseIntSlice(fieldRunKeys)
	pooled.ReleaseIntSlice(fieldOrder)

	if len(activeMeasures) > 0 {
		for _, acc := range activeMeasures {
			i := acc.Ordinal
			runs := indexdata.GetMeasureEntrySlots(0)
			for worker := range build.localMeasureStates {
				if i >= len(build.localMeasureStates[worker]) {
					continue
				}
				bufs := build.localMeasureStates[worker][i]
				if bufs == nil {
					continue
				}
				if cfg.StrKey {
					for j := range bufs {
						if len(bufs[j]) > 1 {
							// Bolt scans string keys lexicographically; durable string ids can follow older assignment order.
							slices.SortFunc(bufs[j], func(a, b indexdata.MeasureEntry) int {
								return cmp.Compare(a.ID, b.ID)
							})
						}
					}
				}
				runs = append(runs, bufs...)
				clear(bufs)
				indexdata.ReleaseMeasureEntrySlots(bufs)
				build.localMeasureStates[worker][i] = nil
			}
			nextMeasure[i] = indexdata.NewMeasureStorageFromSortedRunsOwned(runs)
		}
	}

	recordCount := nextUniverse.Cardinality()
	buildTime := time.Since(start)
	buildRPS := int(float64(recordCount) / max(buildTime.Seconds(), 1))

	for name, f := range cfg.Schema.Fields {
		if f.Slice {
			acc, ok := cfg.Schema.IndexedByName[name]
			if !ok || nextLenIndex[acc.Ordinal].KeyCount() == 0 {
				for i := range active {
					if active[i].slice {
						nextLenIndex[active[i].acc.Ordinal].Release()
					}
				}
				indexdata.ReleaseFieldStorageSlice(nextLenIndex)
				pooled.ReleaseBoolSlice(nextLenZeroComplement)
				nextLenIndex, nextLenZeroComplement = buildLenIndexStorage(cfg.Schema, nextIndex, nextUniverse)
				break
			}
		}
	}

	return Result{
		Storage: snapshot.Storage{
			Index:             nextIndex,
			KeyIndex:          nextKeyIndex,
			NilIndex:          nextNilIndex,
			LenIndex:          nextLenIndex,
			LenZeroComplement: nextLenZeroComplement,
			Measure:           nextMeasure,
			Universe:          nextUniverse,
		},
		Publish:        true,
		Stats:          true,
		BuildTime:      buildTime,
		BuildRPS:       buildRPS,
		LenLoaded:      false,
		KeyIndexLoaded: keyIndexLoaded,
	}
}

func buildLenIndexStorage(s *schema.Schema, index []indexdata.FieldStorage, universe posting.List) ([]indexdata.FieldStorage, []bool) {
	slotCount := len(s.Indexed)
	lenIndex := indexdata.GetFieldStorageSlice(slotCount)[:slotCount]
	lenZeroComplement := pooled.GetBoolSlice(slotCount)[:slotCount]
	clear(lenZeroComplement)

	for _, acc := range s.Indexed {
		if !acc.Field.Slice {
			continue
		}
		storage, useZeroComplement := indexdata.RebuildLenFieldStorageFromIndexView(universe, indexdata.NewFieldIndexViewFromStorage(index[acc.Ordinal]))
		lenIndex[acc.Ordinal] = storage
		if useZeroComplement {
			lenZeroComplement[acc.Ordinal] = true
		}
	}
	return lenIndex, lenZeroComplement
}
