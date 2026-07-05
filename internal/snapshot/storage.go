package snapshot

import (
	"sync/atomic"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
)

func (v *View) FieldIndexStorage(field string) (indexdata.FieldStorage, bool) {
	if v.Index == nil {
		return indexdata.FieldStorage{}, false
	}
	acc, ok := v.IndexedFieldByName[field]
	if !ok || acc.Ordinal >= len(v.Index) {
		return indexdata.FieldStorage{}, false
	}
	storage := v.Index[acc.Ordinal]
	return storage, storage.KeyCount() > 0
}

func (v *View) NilFieldNameSet() map[string]struct{} {
	if v.NilIndex == nil {
		return nil
	}
	fields := make(map[string]struct{}, len(v.IndexedFieldByName))
	for f, acc := range v.IndexedFieldByName {
		if acc.Ordinal < len(v.NilIndex) && v.NilIndex[acc.Ordinal].KeyCount() > 0 {
			fields[f] = struct{}{}
		}
	}
	return fields
}

func (v *View) FieldNameSet() map[string]struct{} {
	if v.Index == nil {
		return nil
	}
	fields := make(map[string]struct{}, len(v.IndexedFieldByName))
	for f, acc := range v.IndexedFieldByName {
		if acc.Ordinal < len(v.Index) && v.Index[acc.Ordinal].KeyCount() > 0 {
			fields[f] = struct{}{}
		}
	}
	return fields
}

func (v *View) LenFieldNameSet() map[string]struct{} {
	if v.LenIndex == nil {
		return nil
	}
	fields := make(map[string]struct{}, len(v.IndexedFieldByName))
	for f, acc := range v.IndexedFieldByName {
		if !acc.Field.Slice || acc.Ordinal >= len(v.LenIndex) || v.LenIndex[acc.Ordinal].KeyCount() == 0 {
			continue
		}
		fields[f] = struct{}{}
	}
	return fields
}

func (v *View) FieldLookupPostingRetainedKey(field string, key keycodec.IndexLookupKey) posting.List {
	if v.Index == nil {
		return posting.List{}
	}
	acc, ok := v.IndexedFieldByName[field]
	if !ok || acc.Ordinal >= len(v.Index) {
		return posting.List{}
	}
	ov := indexdata.NewFieldIndexViewFromStorage(v.Index[acc.Ordinal])
	if key.IsNumeric() {
		return ov.LookupPostingRetainedKey(key.IndexKey())
	}
	return ov.LookupPostingRetained(key.StringKey())
}

func (v *View) UniverseCardinality() uint64 {
	return v.Universe.Cardinality()
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

func (v *View) ensureUniverseOwner() {
	if v.universeOwner != nil {
		return
	}
	v.universeOwner = newSnapshotUniverseOwner(v.Universe)
	v.Universe = v.universeOwner.ids
}

func (v *View) retainSharedOwnedStorageFrom(prev *View) {
	if prev != nil && v.universeOwner == nil && prev.universeOwner != nil && v.Universe == prev.Universe {
		v.universeOwner = prev.universeOwner
	}
	if v.universeOwner != nil {
		if prev != nil && v.universeOwner == prev.universeOwner {
			v.universeOwner.retain()
		}
		v.Universe = v.universeOwner.ids
	} else {
		v.ensureUniverseOwner()
	}
	if prev != nil {
		indexdata.RetainSharedFieldStorageSlots(v.Index, prev.Index)
		indexdata.RetainSharedFieldStorage(v.KeyIndex, prev.KeyIndex)
		indexdata.RetainSharedFieldStorageSlots(v.NilIndex, prev.NilIndex)
		indexdata.RetainSharedFieldStorageSlots(v.LenIndex, prev.LenIndex)
		indexdata.RetainSharedMeasureStorageSlots(v.Measure, prev.Measure)
	}
}

func (v *View) releaseStorage() {
	if v.universeOwner != nil {
		v.universeOwner.release()
	}
	indexdata.ReleaseFieldStorageSlots(v.Index)
	v.KeyIndex.Release()
	indexdata.ReleaseFieldStorageSlots(v.NilIndex)
	indexdata.ReleaseFieldStorageSlots(v.LenIndex)
	indexdata.ReleaseMeasureStorageSlots(v.Measure)
	if v.LenZeroComplement != nil {
		pooled.ReleaseBoolSlice(v.LenZeroComplement)
	}
	v.Index = nil
	v.KeyIndex = indexdata.FieldStorage{}
	v.NilIndex = nil
	v.LenIndex = nil
	v.Measure = nil
	v.LenZeroComplement = nil
	v.Universe = posting.List{}
	v.universeOwner = nil
}

func (v *View) Release() {
	v.releaseStorage()
	v.releaseRuntimeCaches()
}
