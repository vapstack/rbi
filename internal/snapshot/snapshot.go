package snapshot

import (
	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/schema"
)

// View is an immutable read-view published atomically for query paths.
type View struct {
	Seq uint64

	Index              []indexdata.FieldStorage
	KeyIndex           indexdata.FieldStorage
	NilIndex           []indexdata.FieldStorage
	LenIndex           []indexdata.FieldStorage
	LenZeroComplement  []bool
	Measure            []indexdata.MeasureStorage
	IndexedFieldByName schema.IndexedFieldMap
	Universe           posting.List
	universeOwner      *universeOwner

	numericRangeBucketCache *qcache.NumericRangeBucketCache

	matPredCache           *qcache.MaterializedPredCache
	runtimeMatPredSeen     qcache.RecentKeyCache
	runtimeMatPredObserved qcache.RecentKeyCache
	runtimeMatPredDirty    qcache.RecentKeyCache
}

type CacheConfig struct {
	MatPredMaxEntries int
	MatPredMaxCard    uint64
}

type Storage struct {
	Index             []indexdata.FieldStorage
	KeyIndex          indexdata.FieldStorage
	NilIndex          []indexdata.FieldStorage
	LenIndex          []indexdata.FieldStorage
	LenZeroComplement []bool
	Measure           []indexdata.MeasureStorage
	Universe          posting.List
}

func (st *Storage) Release() {
	indexdata.ReleaseFieldStorageSlots(st.Index)
	st.KeyIndex.Release()
	indexdata.ReleaseFieldStorageSlots(st.NilIndex)
	indexdata.ReleaseFieldStorageSlots(st.LenIndex)
	indexdata.ReleaseMeasureStorageSlots(st.Measure)
	if st.LenZeroComplement != nil {
		pooled.ReleaseBoolSlice(st.LenZeroComplement)
	}
	st.Universe.Release()
	*st = Storage{}
}

func NewView(seq uint64, prev *View, s *schema.Schema, cfg CacheConfig, st Storage) *View {
	v := &View{
		Seq:                seq,
		Index:              st.Index,
		KeyIndex:           st.KeyIndex,
		NilIndex:           st.NilIndex,
		LenIndex:           st.LenIndex,
		LenZeroComplement:  st.LenZeroComplement,
		Measure:            st.Measure,
		IndexedFieldByName: s.IndexedByName,
		Universe:           st.Universe,
	}
	v.initRuntimeCaches(s, cfg)
	v.retainSharedOwnedStorageFrom(prev)
	return v
}
