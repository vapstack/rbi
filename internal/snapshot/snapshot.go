package snapshot

import (
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/strmap"
)

// View is an immutable read-view published atomically for query paths.
type View struct {
	Seq uint64

	Index              []indexdata.FieldStorage
	NilIndex           []indexdata.FieldStorage
	LenIndex           []indexdata.FieldStorage
	LenZeroComplement  []bool
	Measure            []indexdata.MeasureStorage
	IndexedFieldByName schema.IndexedFieldMap
	Universe           posting.List
	universeOwner      *universeOwner
	StrMap             *strmap.Snapshot

	numericRangeBucketCache *qcache.NumericRangeBucketCache

	matPredCache           *qcache.MaterializedPredCache
	runtimeMatPredSeen     qcache.RecentKeyCache
	orderORMatPredObserved qcache.RecentKeyCache
}

type CacheConfig struct {
	MatPredMaxEntries int
	MatPredMaxCard    uint64
}

type Storage struct {
	Index             []indexdata.FieldStorage
	NilIndex          []indexdata.FieldStorage
	LenIndex          []indexdata.FieldStorage
	LenZeroComplement []bool
	Measure           []indexdata.MeasureStorage
	Universe          posting.List
	StrMap            *strmap.Snapshot
}

func NewView(seq uint64, prev *View, rt *schema.Runtime, cfg CacheConfig, st Storage) *View {
	v := &View{
		Seq:                seq,
		Index:              st.Index,
		NilIndex:           st.NilIndex,
		LenIndex:           st.LenIndex,
		LenZeroComplement:  st.LenZeroComplement,
		Measure:            st.Measure,
		IndexedFieldByName: rt.IndexedByName,
		Universe:           st.Universe,
		StrMap:             st.StrMap,
	}
	v.initRuntimeCaches(rt, cfg)
	v.retainSharedOwnedStorageFrom(prev)
	return v
}
