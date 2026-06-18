package schema

import (
	"strings"
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
	numeric     bool
	runTarget   int
	regularAdds int
	vals        map[string]posting.List
	fixed       map[uint64]posting.List
	lenMap      map[uint32]posting.List
	nils        posting.List
}

type BuildSink struct {
	State *BuildFieldLocalState
	Idx   uint64
}

const (
	BuildFieldRunTargetEntries = max(16<<10, indexdata.FieldChunkTargetEntries*16)

	buildFieldFlushByPostingAdds            = false
	buildFieldRunTargetPostingAddMultiplier = 64
)

func NewBuildFieldState(slice bool) *BuildFieldState {
	state := &BuildFieldState{}
	if slice {
		state.lenMap = indexdata.GetLenPostingMap()
	}
	return state
}

func NewBuildFieldLocalState(numeric bool, slice bool, runTarget int) BuildFieldLocalState {
	state := BuildFieldLocalState{numeric: numeric, runTarget: runTarget}
	if slice {
		state.lenMap = indexdata.GetLenPostingMap()
	}
	return state
}

func (s *BuildFieldLocalState) addValue(key string, idx uint64) {
	if s.vals == nil {
		s.vals = indexdata.GetPostingMap()
	}
	ids := s.vals[key]
	if ids.IsEmpty() {
		key = strings.Clone(key)
	}
	s.vals[key] = ids.BuildAdded(idx)

	if buildFieldFlushByPostingAdds {
		s.regularAdds++
	}
}

func (s *BuildFieldLocalState) addFixedValue(key uint64, idx uint64) {
	if s.fixed == nil {
		s.fixed = indexdata.GetFixedPostingMap()
	}
	s.fixed[key] = s.fixed[key].BuildAdded(idx)
	s.regularAdds++
}

func (s *BuildFieldLocalState) addNil(idx uint64) {
	s.nils = s.nils.BuildAdded(idx)
}

func (s *BuildFieldLocalState) addLen(length int, idx uint64) {
	if s.lenMap == nil {
		s.lenMap = indexdata.GetLenPostingMap()
	}
	ln := uint32(length)
	s.lenMap[ln] = s.lenMap[ln].BuildAdded(idx)
}

func (s *BuildFieldLocalState) ShouldFlushRegular() bool {
	if buildFieldFlushByPostingAdds && s.regularAdds >= s.runTarget*buildFieldRunTargetPostingAddMultiplier {
		return true
	}
	if s.numeric {
		return len(s.fixed) >= s.runTarget
	}
	return len(s.vals) >= s.runTarget
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
	s.regularAdds = 0
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
		lenMapLen := len(s.lenMap)
		dst.lenMu.Lock()
		for ln, ids := range s.lenMap {
			merged := dst.lenMap[ln]
			merged = merged.BuildMergedOwned(ids)
			dst.lenMap[ln] = merged
		}
		dst.lenMu.Unlock()
		clear(s.lenMap)
		if lenMapLen > indexdata.LenPostingMapMaxRetainedLen {
			s.lenMap = nil
		}
	}
}

func (s *BuildFieldLocalState) Release() {
	s.regularAdds = 0

	posting.ReleaseMapString(s.vals)
	indexdata.ReleasePostingMap(s.vals)
	s.vals = nil

	posting.ReleaseMapU64(s.fixed)
	indexdata.ReleaseFixedPostingMap(s.fixed)
	s.fixed = nil

	posting.ReleaseMapU32(s.lenMap)
	indexdata.ReleaseLenPostingMap(s.lenMap)
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

func (s *BuildFieldState) MaterializeStats() (int, uint64) {
	var keys int
	for i := range s.runs {
		keys += s.runs[i].KeyCount()
	}
	return keys, uint64(keys)
}

func (s *BuildFieldState) Release() {
	indexdata.ReleaseFieldStorageRunsOwned(s.runs)
	s.runs = nil

	s.nils.Release()
	s.nils = posting.List{}

	posting.ReleaseMapU32(s.lenMap)
	indexdata.ReleaseLenPostingMap(s.lenMap)
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
	lenMapLen := len(lenMap)
	storage, useZeroComplement := indexdata.NewLenFieldStorageFromMapOwned(universe, lenMap)
	if lenMapLen <= indexdata.LenPostingMapMaxRetainedLen {
		indexdata.ReleaseLenPostingMap(lenMap)
	}
	return storage, useZeroComplement
}

func (s BuildSink) setNil() {
	s.State.addNil(s.Idx)
}

func (s BuildSink) setLen(length int) {
	s.State.addLen(length, s.Idx)
}

func (s BuildSink) addString(key string) {
	s.State.addValue(key, s.Idx)
}

func (s BuildSink) addFixed(key uint64) {
	s.State.addFixedValue(key, s.Idx)
}
