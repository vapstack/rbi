package snapshot

import (
	"sync/atomic"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
)

func (s *View) FieldIndexStorage(field string) (indexdata.FieldStorage, bool) {
	if s.Index == nil {
		return indexdata.FieldStorage{}, false
	}
	acc, ok := s.IndexedFieldByName[field]
	if !ok || acc.Ordinal >= len(s.Index) {
		return indexdata.FieldStorage{}, false
	}
	storage := s.Index[acc.Ordinal]
	return storage, storage.KeyCount() > 0
}

func (s *View) NilFieldNameSet() map[string]struct{} {
	if s.NilIndex == nil {
		return nil
	}
	fields := make(map[string]struct{}, len(s.IndexedFieldByName))
	for f, acc := range s.IndexedFieldByName {
		if acc.Ordinal < len(s.NilIndex) && s.NilIndex[acc.Ordinal].KeyCount() > 0 {
			fields[f] = struct{}{}
		}
	}
	return fields
}

func (s *View) FieldNameSet() map[string]struct{} {
	if s.Index == nil {
		return nil
	}
	fields := make(map[string]struct{}, len(s.IndexedFieldByName))
	for f, acc := range s.IndexedFieldByName {
		if acc.Ordinal < len(s.Index) && s.Index[acc.Ordinal].KeyCount() > 0 {
			fields[f] = struct{}{}
		}
	}
	return fields
}

func (s *View) LenFieldNameSet() map[string]struct{} {
	if s.LenIndex == nil {
		return nil
	}
	fields := make(map[string]struct{}, len(s.IndexedFieldByName))
	for f, acc := range s.IndexedFieldByName {
		if !acc.Field.Slice || acc.Ordinal >= len(s.LenIndex) || s.LenIndex[acc.Ordinal].KeyCount() == 0 {
			continue
		}
		fields[f] = struct{}{}
	}
	return fields
}

func (s *View) FieldLookupPostingRetained(field, key string) posting.List {
	if s.Index == nil {
		return posting.List{}
	}
	acc, ok := s.IndexedFieldByName[field]
	if !ok || acc.Ordinal >= len(s.Index) {
		return posting.List{}
	}
	return indexdata.NewFieldIndexViewFromStorage(s.Index[acc.Ordinal]).LookupPostingRetained(key)
}

func (s *View) FieldLookupPostingRetainedKey(field string, key keycodec.IndexLookupKey) posting.List {
	if s.Index == nil {
		return posting.List{}
	}
	acc, ok := s.IndexedFieldByName[field]
	if !ok || acc.Ordinal >= len(s.Index) {
		return posting.List{}
	}
	ov := indexdata.NewFieldIndexViewFromStorage(s.Index[acc.Ordinal])
	if key.IsNumeric() {
		return ov.LookupPostingRetainedKey(key.IndexKey())
	}
	return ov.LookupPostingRetained(key.StringKey())
}

func (s *View) UniverseCardinality() uint64 {
	return s.Universe.Cardinality()
}

type universeOwner struct {
	refs atomic.Int32
	ids  posting.List
}

func newSnapshotUniverseOwner(ids posting.List) *universeOwner {
	owner := snapshotUniverseOwnerPool.Get()
	owner.ids = ids
	if owner.ids.IsBorrowed() {
		owner.ids = owner.ids.Clone()
	}
	owner.refs.Store(1)
	return owner
}

func (o *universeOwner) retain() {
	o.refs.Add(1)
}

func (o *universeOwner) release() {
	if o.refs.Add(-1) != 0 {
		return
	}
	o.ids.Release()
	snapshotUniverseOwnerPool.Put(o)
}

func (s *View) ensureUniverseOwner() {
	if s.universeOwner != nil {
		return
	}
	s.universeOwner = newSnapshotUniverseOwner(s.Universe)
	s.Universe = s.universeOwner.ids
}

func (s *View) retainSharedOwnedStorageFrom(prev *View) {
	if prev != nil && s.universeOwner == nil && prev.universeOwner != nil && s.Universe == prev.Universe {
		s.universeOwner = prev.universeOwner
	}
	if s.universeOwner != nil {
		if prev != nil && s.universeOwner == prev.universeOwner {
			s.universeOwner.retain()
		}
		s.Universe = s.universeOwner.ids
	} else {
		s.ensureUniverseOwner()
	}
	if prev != nil {
		indexdata.RetainSharedFieldStorageSlots(s.Index, prev.Index)
		indexdata.RetainSharedFieldStorageSlots(s.NilIndex, prev.NilIndex)
		indexdata.RetainSharedFieldStorageSlots(s.LenIndex, prev.LenIndex)
		indexdata.RetainSharedMeasureStorageSlots(s.Measure, prev.Measure)
	}
}

func (s *View) releaseStorage() {
	if s.universeOwner != nil {
		s.universeOwner.release()
	}
	indexdata.ReleaseFieldStorageSlots(s.Index)
	indexdata.ReleaseFieldStorageSlots(s.NilIndex)
	indexdata.ReleaseFieldStorageSlots(s.LenIndex)
	indexdata.ReleaseMeasureStorageSlots(s.Measure)
	if s.LenZeroComplement != nil {
		pooled.ReleaseBoolSlice(s.LenZeroComplement)
	}
	s.Index = nil
	s.NilIndex = nil
	s.LenIndex = nil
	s.Measure = nil
	s.LenZeroComplement = nil
}
