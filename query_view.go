package rbi

import (
	"unsafe"

	"github.com/vapstack/rbi/internal/posting"
)

// queryView is a fully initialized, snapshot-bound query state.
// Zero value is invalid; construct it only via makeQueryView.
type queryView[K ~string | ~uint64, V any] struct {
	root *DB[K, V]
	snap *indexSnapshot

	strkey            bool
	strmapView        *strMapSnapshot
	fields            map[string]*field
	planner           *planner
	options           *Options
	lenZeroComplement map[string]bool
}

func (qv *queryView[K, V]) snapshotFieldIndexSlice(field string) *[]index {
	return qv.snap.index[field].flatSlice()
}

func (qv *queryView[K, V]) snapshotLenFieldIndexSlice(field string) *[]index {
	return qv.snap.lenIndex[field].flatSlice()
}

func (qv *queryView[K, V]) snapshotUniverseCardinality() uint64 {
	return qv.snap.universe.Cardinality()
}

func (qv *queryView[K, V]) snapshotUniverseView() posting.List {
	return qv.snap.universe.Borrow()
}

func (qv *queryView[K, V]) fieldOverlay(field string) fieldOverlay {
	return newFieldOverlayStorage(qv.snap.index[field])
}

func (qv *queryView[K, V]) nilFieldOverlay(field string) fieldOverlay {
	return newFieldOverlayStorage(qv.snap.nilIndex[field])
}

func (qv *queryView[K, V]) lenFieldOverlay(field string) fieldOverlay {
	return newFieldOverlayStorage(qv.snap.lenIndex[field])
}

func (qv *queryView[K, V]) hasFieldIndex(field string) bool {
	_, ok := qv.fields[field]
	return ok
}

func (qv *queryView[K, V]) hasLenFieldIndex(field string) bool {
	f := qv.fields[field]
	return f != nil && f.Slice
}

func (qv *queryView[K, V]) keyFromIdx(idx uint64) (K, bool) {
	var zero K
	if qv.strkey {
		if qv.strmapView == nil {
			panic("rbi: string queryView has no strmap snapshot")
		}
		s, ok := qv.strmapView.getStringNoLock(idx)
		if !ok {
			return zero, false
		}
		return *(*K)(unsafe.Pointer(&s)), true
	}
	return *(*K)(unsafe.Pointer(&idx)), true
}

func (qv *queryView[K, V]) idFromIdxNoLock(idx uint64) K {
	if qv.strkey {
		if qv.strmapView == nil {
			panic("rbi: string queryView has no strmap snapshot")
		}
		s, ok := qv.strmapView.getStringNoLock(idx)
		if !ok {
			panic("rbi: no id associated with snapshot idx")
		}
		return *(*K)(unsafe.Pointer(&s))
	}
	return *(*K)(unsafe.Pointer(&idx))
}

func (qv *queryView[K, V]) isLenZeroComplementField(field string) bool {
	return qv.lenZeroComplement[field]
}
