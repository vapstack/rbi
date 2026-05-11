package schema

import (
	"fmt"
	"sync"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/posting"
)

type BuildFieldState struct {
	runsMu sync.Mutex
	runs   []indexdata.FieldStorageRun

	nilMu sync.Mutex
	nils  posting.List

	lenMu  sync.Mutex
	lenMap map[uint32]posting.List
}

type BuildFieldLocalState struct {
	numeric bool
	vals    map[string]posting.List
	fixed   map[uint64]posting.List
	lenMap  map[uint32]posting.List
	nils    posting.List
}

type BuildSink struct {
	State *BuildFieldLocalState
	Idx   uint64
	Field string
	Err   *error
}

const buildIndexRunTargetEntries = max(4<<10, indexdata.FieldChunkTargetEntries*16)

func NewBuildFieldState(slice bool) *BuildFieldState {
	state := &BuildFieldState{}
	if slice {
		state.lenMap = make(map[uint32]posting.List, 8)
	}
	return state
}

func NewBuildFieldLocalState(numeric bool, slice bool) BuildFieldLocalState {
	state := BuildFieldLocalState{numeric: numeric}
	if slice {
		state.lenMap = make(map[uint32]posting.List, 8)
	}
	return state
}

func (s *BuildFieldLocalState) addValue(key string, idx uint64) {
	if s.vals == nil {
		s.vals = indexdata.GetPostingMap()
	}
	s.vals[key] = s.vals[key].BuildAdded(idx)
}

func (s *BuildFieldLocalState) addFixedValue(key uint64, idx uint64) {
	if s.fixed == nil {
		s.fixed = indexdata.GetFixedPostingMap()
	}
	s.fixed[key] = s.fixed[key].BuildAdded(idx)
}

func (s *BuildFieldLocalState) addNil(idx uint64) {
	s.nils = s.nils.BuildAdded(idx)
}

func (s *BuildFieldLocalState) addLen(length int, idx uint64) {
	if s.lenMap == nil {
		s.lenMap = make(map[uint32]posting.List, 8)
	}
	ln := uint32(length)
	s.lenMap[ln] = s.lenMap[ln].BuildAdded(idx)
}

func (s *BuildFieldLocalState) ShouldFlushRegular() bool {
	if s.numeric {
		return len(s.fixed) >= buildIndexRunTargetEntries
	}
	return len(s.vals) >= buildIndexRunTargetEntries
}

func (s *BuildFieldState) appendRun(run indexdata.FieldStorageRun) {
	if run.KeyCount() == 0 {
		return
	}
	s.runsMu.Lock()
	s.runs = append(s.runs, run)
	s.runsMu.Unlock()
}

func (s *BuildFieldLocalState) FlushRegularInto(dst *BuildFieldState) {
	if s.numeric {
		if run := indexdata.NewFixedFieldStorageRunFromPostingMap(s.fixed); run.KeyCount() > 0 {
			dst.appendRun(run)
		}
		return
	}
	if run := indexdata.NewStringFieldStorageRunFromPostingMap(s.vals); run.KeyCount() > 0 {
		dst.appendRun(run)
	}
}

func (s *BuildFieldLocalState) FlushAllInto(dst *BuildFieldState) {
	s.FlushRegularInto(dst)

	if !s.nils.IsEmpty() {
		dst.nilMu.Lock()
		ids := dst.nils
		ids = ids.BuildMergedOwned(s.nils)
		dst.nils = ids
		dst.nilMu.Unlock()
		s.nils = posting.List{}
	}

	if len(s.lenMap) > 0 {
		dst.lenMu.Lock()
		for ln, ids := range s.lenMap {
			merged := dst.lenMap[ln]
			merged = merged.BuildMergedOwned(ids)
			dst.lenMap[ln] = merged
		}
		dst.lenMu.Unlock()
		clear(s.lenMap)
	}
}

func (s *BuildFieldLocalState) Release() {
	posting.ReleaseMap(s.vals)
	indexdata.ReleasePostingMap(s.vals)
	s.vals = nil

	posting.ReleaseMap(s.fixed)
	indexdata.ReleaseFixedPostingMap(s.fixed)
	s.fixed = nil

	posting.ReleaseMap(s.lenMap)
	s.lenMap = nil

	s.nils.Release()
	s.nils = posting.List{}
}

func (s *BuildFieldState) MaterializeStorage() indexdata.FieldStorage {
	if len(s.runs) == 0 {
		return indexdata.FieldStorage{}
	}
	runs := s.runs
	s.runs = nil
	return indexdata.NewRegularFieldStorageFromRunsOwned(runs)
}

func (s *BuildFieldState) Release() {
	indexdata.ReleaseFieldStorageRunsOwned(s.runs)
	s.runs = nil

	s.nils.Release()
	s.nils = posting.List{}

	posting.ReleaseMap(s.lenMap)
	s.lenMap = nil
}

func (s *BuildFieldState) MaterializeNilStorage() indexdata.FieldStorage {
	ids := s.nils
	s.nils = posting.List{}
	return indexdata.NewNilFieldStorageOwned(ids)
}

func (s *BuildFieldState) MaterializeLenStorage(universe posting.List) (indexdata.FieldStorage, bool) {
	lenMap := s.lenMap
	s.lenMap = nil
	return indexdata.NewLenFieldStorageFromMapOwned(universe, lenMap)
}

func (s BuildSink) setNil() {
	if s.State == nil {
		return
	}
	s.State.addNil(s.Idx)
}

func (s BuildSink) setLen(length int) {
	if s.State == nil {
		return
	}
	s.State.addLen(length, s.Idx)
}

func (s BuildSink) addString(key string) {
	if s.Err != nil && *s.Err == nil && len(key) > indexdata.FieldStringRefMax {
		if s.Field != "" {
			*s.Err = fmt.Errorf("field %q indexed string value len %d exceeds limit %d", s.Field, len(key), indexdata.FieldStringRefMax)
		} else {
			*s.Err = indexdata.ValidateIndexedStringKeyLen(len(key))
		}
		return
	}
	if s.State == nil {
		return
	}
	s.State.addValue(key, s.Idx)
}

func (s BuildSink) addFixed(key uint64) {
	if s.State == nil {
		return
	}
	s.State.addFixedValue(key, s.Idx)
}
