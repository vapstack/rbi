package rebuild

import (
	"time"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
)

func buildNoActive(cfg Config, state State) (Result, error) {
	if state.LenLoaded {
		return Result{}, nil
	}
	nextLenIndex, nextLenZeroComplement := buildLenIndexStorage(cfg.Schema, state.Index, state.Universe)
	return Result{
		Publish:   true,
		LenLoaded: false,
		Storage: snapshot.Storage{
			Index:             indexdata.CloneFieldStorageSlots(state.Index, len(cfg.Schema.Indexed)),
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

	for i := range build.fieldStates {
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
	}

	if len(activeMeasures) > 0 {
		for _, acc := range activeMeasures {
			i := acc.Ordinal
			entries := indexdata.GetMeasureEntrySlice(0)
			for worker := range build.localMeasureStates {
				if i >= len(build.localMeasureStates[worker]) {
					continue
				}
				buf := build.localMeasureStates[worker][i]
				if buf == nil {
					continue
				}
				for entryPos := 0; entryPos < len(buf); entryPos++ {
					entries = append(entries, buf[entryPos])
				}
				indexdata.ReleaseMeasureEntrySlice(buf)
				build.localMeasureStates[worker][i] = nil
			}
			storage := indexdata.NewMeasureStorageFromEntriesOwned(entries)
			nextMeasure[i] = storage
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
			NilIndex:          nextNilIndex,
			LenIndex:          nextLenIndex,
			LenZeroComplement: nextLenZeroComplement,
			Measure:           nextMeasure,
			Universe:          nextUniverse,
		},
		Publish:   true,
		Stats:     true,
		BuildTime: buildTime,
		BuildRPS:  buildRPS,
		LenLoaded: false,
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
