package rbi

import (
	"math"
	"reflect"
	"unsafe"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/posting"
)

const normalizedScalarBoundCacheMaxEntries = 8

type normalizedScalarBoundCacheKind uint8

const (
	normalizedScalarBoundCacheNone normalizedScalarBoundCacheKind = iota
	normalizedScalarBoundCacheString
	normalizedScalarBoundCacheSigned
	normalizedScalarBoundCacheUnsigned
	normalizedScalarBoundCacheFloat
)

type normalizedScalarBoundCacheEntry struct {
	field string
	op    qx.Op
	kind  normalizedScalarBoundCacheKind
	str   string
	i64   int64
	u64   uint64
	f64   uint64
	bound normalizedScalarBound
}

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

	normalizedScalarBoundCacheLen uint8
	normalizedScalarBoundCache    [normalizedScalarBoundCacheMaxEntries]normalizedScalarBoundCacheEntry
}

func normalizedScalarBoundCacheValue(v reflect.Value) (normalizedScalarBoundCacheEntry, bool) {
	if !v.IsValid() {
		return normalizedScalarBoundCacheEntry{}, false
	}
	switch v.Kind() {
	case reflect.String:
		return normalizedScalarBoundCacheEntry{
			kind: normalizedScalarBoundCacheString,
			str:  v.String(),
		}, true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return normalizedScalarBoundCacheEntry{
			kind: normalizedScalarBoundCacheSigned,
			i64:  v.Int(),
		}, true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return normalizedScalarBoundCacheEntry{
			kind: normalizedScalarBoundCacheUnsigned,
			u64:  v.Uint(),
		}, true
	case reflect.Float32, reflect.Float64:
		return normalizedScalarBoundCacheEntry{
			kind: normalizedScalarBoundCacheFloat,
			f64:  math.Float64bits(v.Float()),
		}, true
	default:
		return normalizedScalarBoundCacheEntry{}, false
	}
}

func (qv *queryView[K, V]) loadNormalizedScalarBound(expr qx.Expr, v reflect.Value) (normalizedScalarBound, bool) {
	if qv == nil {
		return normalizedScalarBound{}, false
	}
	key, ok := normalizedScalarBoundCacheValue(v)
	if !ok {
		return normalizedScalarBound{}, false
	}
	key.field = expr.Field
	key.op = expr.Op
	n := int(qv.normalizedScalarBoundCacheLen)
	for i := 0; i < n; i++ {
		entry := qv.normalizedScalarBoundCache[i]
		if entry.field != key.field || entry.op != key.op || entry.kind != key.kind {
			continue
		}
		switch entry.kind {
		case normalizedScalarBoundCacheString:
			if entry.str == key.str {
				return entry.bound, true
			}
		case normalizedScalarBoundCacheSigned:
			if entry.i64 == key.i64 {
				return entry.bound, true
			}
		case normalizedScalarBoundCacheUnsigned:
			if entry.u64 == key.u64 {
				return entry.bound, true
			}
		case normalizedScalarBoundCacheFloat:
			if entry.f64 == key.f64 {
				return entry.bound, true
			}
		}
	}
	return normalizedScalarBound{}, false
}

func (qv *queryView[K, V]) storeNormalizedScalarBound(expr qx.Expr, v reflect.Value, bound normalizedScalarBound) {
	if qv == nil {
		return
	}
	key, ok := normalizedScalarBoundCacheValue(v)
	if !ok {
		return
	}
	n := int(qv.normalizedScalarBoundCacheLen)
	if n >= len(qv.normalizedScalarBoundCache) {
		return
	}
	key.field = expr.Field
	key.op = expr.Op
	key.bound = bound
	qv.normalizedScalarBoundCache[n] = key
	qv.normalizedScalarBoundCacheLen++
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

func (qv *queryView[K, V]) hasIndexedField(field string) bool {
	_, ok := qv.fields[field]
	return ok
}

func (qv *queryView[K, V]) hasIndexedLenField(field string) bool {
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
