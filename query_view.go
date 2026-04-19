package rbi

import (
	"math"
	"reflect"
	"unsafe"

	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qir"
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
	fieldOrdinal int
	op           qir.Op
	kind         normalizedScalarBoundCacheKind
	str          string
	i64          int64
	u64          uint64
	f64          uint64
	bound        normalizedScalarBound
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
	lenZeroComplement *pooled.SliceBuf[bool]

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

func (qv *queryView[K, V]) loadNormalizedScalarBound(expr qir.Expr, v reflect.Value) (normalizedScalarBound, bool) {
	if qv == nil {
		return normalizedScalarBound{}, false
	}
	key, ok := normalizedScalarBoundCacheValue(v)
	if !ok {
		return normalizedScalarBound{}, false
	}
	key.fieldOrdinal = expr.FieldOrdinal
	key.op = expr.Op
	n := int(qv.normalizedScalarBoundCacheLen)
	for i := 0; i < n; i++ {
		entry := qv.normalizedScalarBoundCache[i]
		if entry.fieldOrdinal != key.fieldOrdinal ||
			entry.op != key.op ||
			entry.kind != key.kind {
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

func (qv *queryView[K, V]) storeNormalizedScalarBound(expr qir.Expr, v reflect.Value, bound normalizedScalarBound) {
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
	key.fieldOrdinal = expr.FieldOrdinal
	key.op = expr.Op
	key.bound = bound
	qv.normalizedScalarBoundCache[n] = key
	qv.normalizedScalarBoundCacheLen++
}

func (qv *queryView[K, V]) indexedFieldAccessorByName(field string) (indexedFieldAccessor, bool) {
	acc, ok := qv.root.indexedFieldByName[field]
	return acc, ok
}

func (qv *queryView[K, V]) indexedFieldAccessor(field string, ordinal int) (indexedFieldAccessor, bool) {
	if acc, ok := qv.indexedFieldAccessorByOrdinal(ordinal); ok {
		return acc, true
	}
	if field == "" {
		return indexedFieldAccessor{}, false
	}
	return qv.indexedFieldAccessorByName(field)
}

func (qv *queryView[K, V]) fieldOrdinalByName(field string) int {
	acc, ok := qv.indexedFieldAccessorByName(field)
	if !ok {
		return -1
	}
	return acc.ordinal
}

func (qv *queryView[K, V]) indexedFieldAccessorByOrdinal(ordinal int) (indexedFieldAccessor, bool) {
	if ordinal < 0 || ordinal >= len(qv.root.indexedFieldAccess) {
		return indexedFieldAccessor{}, false
	}
	return qv.root.indexedFieldAccess[ordinal], true
}

func (qv *queryView[K, V]) indexedFieldAccessorForExpr(expr qir.Expr) (indexedFieldAccessor, bool) {
	return qv.indexedFieldAccessorByOrdinal(expr.FieldOrdinal)
}

func (qv *queryView[K, V]) indexedFieldAccessorForOrder(order qir.Order) (indexedFieldAccessor, bool) {
	return qv.indexedFieldAccessorByOrdinal(order.FieldOrdinal)
}

func (qv *queryView[K, V]) fieldMetaByOrdinal(ordinal int) *field {
	acc, ok := qv.indexedFieldAccessorByOrdinal(ordinal)
	if !ok {
		return nil
	}
	return acc.field
}

func (db *DB[K, V]) fieldNameByOrdinal(ordinal int) string {
	if ordinal < 0 || ordinal >= len(db.indexedFieldAccess) {
		return ""
	}
	return db.indexedFieldAccess[ordinal].name
}

func (qv *queryView[K, V]) fieldNameByOrdinal(ordinal int) string {
	if qv == nil || qv.root == nil {
		return ""
	}
	return qv.root.fieldNameByOrdinal(ordinal)
}

func (qv *queryView[K, V]) fieldNameByExpr(expr qir.Expr) string {
	return qv.fieldNameByOrdinal(expr.FieldOrdinal)
}

func (qv *queryView[K, V]) fieldNameByOrder(order qir.Order) string {
	return qv.fieldNameByOrdinal(order.FieldOrdinal)
}

func (qv *queryView[K, V]) fieldMeta(field string, ordinal int) *field {
	acc, ok := qv.indexedFieldAccessor(field, ordinal)
	if !ok {
		return nil
	}
	return acc.field
}

func (qv *queryView[K, V]) fieldMetaByExpr(expr qir.Expr) *field {
	acc, ok := qv.indexedFieldAccessorForExpr(expr)
	if !ok {
		return nil
	}
	return acc.field
}

func (qv *queryView[K, V]) fieldMetaByOrder(order qir.Order) *field {
	acc, ok := qv.indexedFieldAccessorForOrder(order)
	if !ok {
		return nil
	}
	return acc.field
}

func (qv *queryView[K, V]) snapshotFieldIndexSliceByOrdinal(ordinal int) *[]index {
	acc, ok := qv.indexedFieldAccessorByOrdinal(ordinal)
	if !ok || qv.snap.index == nil || acc.ordinal >= qv.snap.index.Len() {
		return nil
	}
	return qv.snap.index.Get(acc.ordinal).flatSlice()
}

func (qv *queryView[K, V]) snapshotFieldIndexSliceRef(field string, ordinal int) *[]index {
	acc, ok := qv.indexedFieldAccessor(field, ordinal)
	if !ok {
		return nil
	}
	return qv.snapshotFieldIndexSliceByOrdinal(acc.ordinal)
}

func (qv *queryView[K, V]) snapshotFieldIndexSlice(field string) *[]index {
	acc, ok := qv.indexedFieldAccessorByName(field)
	if !ok {
		return nil
	}
	return qv.snapshotFieldIndexSliceByOrdinal(acc.ordinal)
}

func (qv *queryView[K, V]) snapshotFieldIndexSliceForExpr(expr qir.Expr) *[]index {
	acc, ok := qv.indexedFieldAccessorForExpr(expr)
	if !ok {
		return nil
	}
	return qv.snapshotFieldIndexSliceByOrdinal(acc.ordinal)
}

func (qv *queryView[K, V]) snapshotFieldIndexSliceForOrder(order qir.Order) *[]index {
	acc, ok := qv.indexedFieldAccessorForOrder(order)
	if !ok {
		return nil
	}
	return qv.snapshotFieldIndexSliceByOrdinal(acc.ordinal)
}

func (qv *queryView[K, V]) snapshotLenFieldIndexSliceByOrdinal(ordinal int) *[]index {
	acc, ok := qv.indexedFieldAccessorByOrdinal(ordinal)
	if !ok || qv.snap.lenIndex == nil || acc.ordinal >= qv.snap.lenIndex.Len() {
		return nil
	}
	return qv.snap.lenIndex.Get(acc.ordinal).flatSlice()
}

func (qv *queryView[K, V]) snapshotLenFieldIndexSliceRef(field string, ordinal int) *[]index {
	acc, ok := qv.indexedFieldAccessor(field, ordinal)
	if !ok {
		return nil
	}
	return qv.snapshotLenFieldIndexSliceByOrdinal(acc.ordinal)
}

func (qv *queryView[K, V]) snapshotLenFieldIndexSlice(field string) *[]index {
	acc, ok := qv.indexedFieldAccessorByName(field)
	if !ok {
		return nil
	}
	return qv.snapshotLenFieldIndexSliceByOrdinal(acc.ordinal)
}

func (qv *queryView[K, V]) snapshotLenFieldIndexSliceForOrder(order qir.Order) *[]index {
	acc, ok := qv.indexedFieldAccessorForOrder(order)
	if !ok {
		return nil
	}
	return qv.snapshotLenFieldIndexSliceByOrdinal(acc.ordinal)
}

func (qv *queryView[K, V]) snapshotUniverseCardinality() uint64 {
	return qv.snap.universe.Cardinality()
}

func (qv *queryView[K, V]) snapshotUniverseView() posting.List {
	return qv.snap.universe.Borrow()
}

func (qv *queryView[K, V]) fieldOverlayByOrdinal(ordinal int) fieldOverlay {
	acc, ok := qv.indexedFieldAccessorByOrdinal(ordinal)
	if !ok || qv.snap.index == nil || acc.ordinal >= qv.snap.index.Len() {
		return fieldOverlay{}
	}
	return newFieldOverlayStorage(qv.snap.index.Get(acc.ordinal))
}

func (qv *queryView[K, V]) fieldOverlayRef(field string, ordinal int) fieldOverlay {
	acc, ok := qv.indexedFieldAccessor(field, ordinal)
	if !ok {
		return fieldOverlay{}
	}
	return qv.fieldOverlayByOrdinal(acc.ordinal)
}

func (qv *queryView[K, V]) fieldOverlay(field string) fieldOverlay {
	acc, ok := qv.indexedFieldAccessorByName(field)
	if !ok {
		return fieldOverlay{}
	}
	return qv.fieldOverlayByOrdinal(acc.ordinal)
}

func (qv *queryView[K, V]) fieldOverlayForExpr(expr qir.Expr) fieldOverlay {
	acc, ok := qv.indexedFieldAccessorForExpr(expr)
	if !ok {
		return fieldOverlay{}
	}
	return qv.fieldOverlayByOrdinal(acc.ordinal)
}

func (qv *queryView[K, V]) fieldOverlayForOrder(order qir.Order) fieldOverlay {
	acc, ok := qv.indexedFieldAccessorForOrder(order)
	if !ok {
		return fieldOverlay{}
	}
	return qv.fieldOverlayByOrdinal(acc.ordinal)
}

func (qv *queryView[K, V]) nilFieldOverlayByOrdinal(ordinal int) fieldOverlay {
	acc, ok := qv.indexedFieldAccessorByOrdinal(ordinal)
	if !ok || qv.snap.nilIndex == nil || acc.ordinal >= qv.snap.nilIndex.Len() {
		return fieldOverlay{}
	}
	return newFieldOverlayStorage(qv.snap.nilIndex.Get(acc.ordinal))
}

func (qv *queryView[K, V]) nilFieldOverlayRef(field string, ordinal int) fieldOverlay {
	acc, ok := qv.indexedFieldAccessor(field, ordinal)
	if !ok {
		return fieldOverlay{}
	}
	return qv.nilFieldOverlayByOrdinal(acc.ordinal)
}

func (qv *queryView[K, V]) nilFieldOverlay(field string) fieldOverlay {
	acc, ok := qv.indexedFieldAccessorByName(field)
	if !ok {
		return fieldOverlay{}
	}
	return qv.nilFieldOverlayByOrdinal(acc.ordinal)
}

func (qv *queryView[K, V]) nilFieldOverlayForExpr(expr qir.Expr) fieldOverlay {
	acc, ok := qv.indexedFieldAccessorForExpr(expr)
	if !ok {
		return fieldOverlay{}
	}
	return qv.nilFieldOverlayByOrdinal(acc.ordinal)
}

func (qv *queryView[K, V]) nilFieldOverlayForOrder(order qir.Order) fieldOverlay {
	acc, ok := qv.indexedFieldAccessorForOrder(order)
	if !ok {
		return fieldOverlay{}
	}
	return qv.nilFieldOverlayByOrdinal(acc.ordinal)
}

func (qv *queryView[K, V]) lenFieldOverlayByOrdinal(ordinal int) fieldOverlay {
	acc, ok := qv.indexedFieldAccessorByOrdinal(ordinal)
	if !ok || qv.snap.lenIndex == nil || acc.ordinal >= qv.snap.lenIndex.Len() {
		return fieldOverlay{}
	}
	return newFieldOverlayStorage(qv.snap.lenIndex.Get(acc.ordinal))
}

func (qv *queryView[K, V]) lenFieldOverlayRef(field string, ordinal int) fieldOverlay {
	acc, ok := qv.indexedFieldAccessor(field, ordinal)
	if !ok {
		return fieldOverlay{}
	}
	return qv.lenFieldOverlayByOrdinal(acc.ordinal)
}

func (qv *queryView[K, V]) lenFieldOverlay(field string) fieldOverlay {
	acc, ok := qv.indexedFieldAccessorByName(field)
	if !ok {
		return fieldOverlay{}
	}
	return qv.lenFieldOverlayByOrdinal(acc.ordinal)
}

func (qv *queryView[K, V]) lenFieldOverlayForExpr(expr qir.Expr) fieldOverlay {
	acc, ok := qv.indexedFieldAccessorForExpr(expr)
	if !ok {
		return fieldOverlay{}
	}
	return qv.lenFieldOverlayByOrdinal(acc.ordinal)
}

func (qv *queryView[K, V]) lenFieldOverlayForOrder(order qir.Order) fieldOverlay {
	acc, ok := qv.indexedFieldAccessorForOrder(order)
	if !ok {
		return fieldOverlay{}
	}
	return qv.lenFieldOverlayByOrdinal(acc.ordinal)
}

func (qv *queryView[K, V]) hasIndexedFieldOrdinal(ordinal int) bool {
	_, ok := qv.indexedFieldAccessorByOrdinal(ordinal)
	return ok
}

func (qv *queryView[K, V]) hasIndexedFieldRef(field string, ordinal int) bool {
	_, ok := qv.indexedFieldAccessor(field, ordinal)
	return ok
}

func (qv *queryView[K, V]) hasIndexedFieldForExpr(expr qir.Expr) bool {
	_, ok := qv.indexedFieldAccessorForExpr(expr)
	return ok
}

func (qv *queryView[K, V]) hasIndexedFieldForOrder(order qir.Order) bool {
	_, ok := qv.indexedFieldAccessorForOrder(order)
	return ok
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

func (qv *queryView[K, V]) isLenZeroComplementOrdinal(ordinal int) bool {
	acc, ok := qv.indexedFieldAccessorByOrdinal(ordinal)
	if !ok || qv.lenZeroComplement == nil || acc.ordinal >= qv.lenZeroComplement.Len() {
		return false
	}
	return qv.lenZeroComplement.Get(acc.ordinal)
}

func (qv *queryView[K, V]) isLenZeroComplementRef(field string, ordinal int) bool {
	acc, ok := qv.indexedFieldAccessor(field, ordinal)
	if !ok {
		return false
	}
	return qv.isLenZeroComplementOrdinal(acc.ordinal)
}

func (qv *queryView[K, V]) isLenZeroComplementForOrder(order qir.Order) bool {
	return qv.isLenZeroComplementOrdinal(order.FieldOrdinal)
}

func (qv *queryView[K, V]) isLenZeroComplementField(field string) bool {
	acc, ok := qv.indexedFieldAccessorByName(field)
	if !ok {
		return false
	}
	return qv.isLenZeroComplementOrdinal(acc.ordinal)
}
