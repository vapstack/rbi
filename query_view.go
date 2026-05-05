package rbi

import (
	"math"
	"reflect"

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
	normalizedScalarBoundCacheUnixTime
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
// Zero value is invalid; construct it via makeQueryView.
type queryView struct {
	engine *queryEngine
	snap   *indexSnapshot

	strKey            bool
	strMapView        *strMapSnapshot
	fields            map[string]*field
	planner           *planner
	options           *Options
	lenZeroComplement *pooled.Slice[bool]

	normalizedScalarBoundCacheLen uint8
	normalizedScalarBoundCache    [normalizedScalarBoundCacheMaxEntries]normalizedScalarBoundCacheEntry
}

func normalizedScalarBoundCacheValue(v reflect.Value, fm *field) (normalizedScalarBoundCacheEntry, bool) {
	if !v.IsValid() {
		return normalizedScalarBoundCacheEntry{}, false
	}

	if isNativeTimeField(fm) {
		if unix, ok := queryValueToUnixSeconds(v); ok {
			return normalizedScalarBoundCacheEntry{
				kind: normalizedScalarBoundCacheUnixTime,
				i64:  unix,
			}, true
		}
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

func (qv *queryView) loadNormalizedScalarBound(expr qir.Expr, v reflect.Value) (normalizedScalarBound, bool) {
	key, ok := normalizedScalarBoundCacheValue(v, qv.fieldMetaByExpr(expr))
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
		case normalizedScalarBoundCacheUnixTime:
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

func (qv *queryView) storeNormalizedScalarBound(expr qir.Expr, v reflect.Value, bound normalizedScalarBound) {
	key, ok := normalizedScalarBoundCacheValue(v, qv.fieldMetaByExpr(expr))
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

func (qv *queryView) indexedFieldAccessorByName(field string) (indexedFieldAccessor, bool) {
	acc, ok := qv.engine.indexedFieldMap[field]
	return acc, ok
}

func (qv *queryView) indexedFieldAccessor(field string, ordinal int) (indexedFieldAccessor, bool) {
	if acc, ok := qv.indexedFieldAccessorByOrdinal(ordinal); ok {
		return acc, true
	}
	if field == "" {
		return indexedFieldAccessor{}, false
	}
	return qv.indexedFieldAccessorByName(field)
}

func (qv *queryView) fieldOrdinalByName(field string) int {
	acc, ok := qv.indexedFieldAccessorByName(field)
	if !ok {
		return -1
	}
	return acc.ordinal
}

func (qv *queryView) indexedFieldAccessorByOrdinal(ordinal int) (indexedFieldAccessor, bool) {
	if ordinal < 0 || ordinal >= len(qv.engine.indexedFieldAccess) {
		return indexedFieldAccessor{}, false
	}
	return qv.engine.indexedFieldAccess[ordinal], true
}

func (qv *queryView) fieldMetaByOrdinal(ordinal int) *field {
	acc, ok := qv.indexedFieldAccessorByOrdinal(ordinal)
	if !ok {
		return nil
	}
	return acc.field
}

func (qv *queryView) fieldMeta(field string, ordinal int) *field {
	acc, ok := qv.indexedFieldAccessor(field, ordinal)
	if !ok {
		return nil
	}
	return acc.field
}

func (qv *queryView) fieldMetaByExpr(expr qir.Expr) *field {
	acc, ok := qv.indexedFieldAccessorByOrdinal(expr.FieldOrdinal)
	if !ok {
		return nil
	}
	return acc.field
}

func (qv *queryView) fieldMetaByOrder(order qir.Order) *field {
	acc, ok := qv.indexedFieldAccessorByOrdinal(order.FieldOrdinal)
	if !ok {
		return nil
	}
	return acc.field
}

func (qv *queryView) snapshotFieldIndexSliceByOrdinal(ordinal int) *[]index {
	acc, ok := qv.indexedFieldAccessorByOrdinal(ordinal)
	if !ok || qv.snap.index == nil || acc.ordinal >= qv.snap.index.Len() {
		return nil
	}
	return qv.snap.index.Get(acc.ordinal).flatSlice()
}

func (qv *queryView) snapshotFieldIndexSliceRef(field string, ordinal int) *[]index {
	acc, ok := qv.indexedFieldAccessor(field, ordinal)
	if !ok {
		return nil
	}
	return qv.snapshotFieldIndexSliceByOrdinal(acc.ordinal)
}

func (qv *queryView) snapshotFieldIndexSlice(field string) *[]index {
	acc, ok := qv.indexedFieldAccessorByName(field)
	if !ok {
		return nil
	}
	return qv.snapshotFieldIndexSliceByOrdinal(acc.ordinal)
}

func (qv *queryView) snapshotFieldIndexSliceForExpr(expr qir.Expr) *[]index {
	acc, ok := qv.indexedFieldAccessorByOrdinal(expr.FieldOrdinal)
	if !ok {
		return nil
	}
	return qv.snapshotFieldIndexSliceByOrdinal(acc.ordinal)
}

func (qv *queryView) snapshotFieldIndexSliceForOrder(order qir.Order) *[]index {
	acc, ok := qv.indexedFieldAccessorByOrdinal(order.FieldOrdinal)
	if !ok {
		return nil
	}
	return qv.snapshotFieldIndexSliceByOrdinal(acc.ordinal)
}

func (qv *queryView) snapshotLenFieldIndexSliceByOrdinal(ordinal int) *[]index {
	acc, ok := qv.indexedFieldAccessorByOrdinal(ordinal)
	if !ok || qv.snap.lenIndex == nil || acc.ordinal >= qv.snap.lenIndex.Len() {
		return nil
	}
	return qv.snap.lenIndex.Get(acc.ordinal).flatSlice()
}

func (qv *queryView) snapshotLenFieldIndexSliceRef(field string, ordinal int) *[]index {
	acc, ok := qv.indexedFieldAccessor(field, ordinal)
	if !ok {
		return nil
	}
	return qv.snapshotLenFieldIndexSliceByOrdinal(acc.ordinal)
}

func (qv *queryView) snapshotLenFieldIndexSlice(field string) *[]index {
	acc, ok := qv.indexedFieldAccessorByName(field)
	if !ok {
		return nil
	}
	return qv.snapshotLenFieldIndexSliceByOrdinal(acc.ordinal)
}

func (qv *queryView) snapshotLenFieldIndexSliceForOrder(order qir.Order) *[]index {
	acc, ok := qv.indexedFieldAccessorByOrdinal(order.FieldOrdinal)
	if !ok {
		return nil
	}
	return qv.snapshotLenFieldIndexSliceByOrdinal(acc.ordinal)
}

func (qv *queryView) snapshotUniverseCardinality() uint64 {
	return qv.snap.universe.Cardinality()
}

func (qv *queryView) snapshotUniverseView() posting.List {
	return qv.snap.universe.Borrow()
}

func (qv *queryView) fieldOverlayByOrdinal(ordinal int) fieldOverlay {
	acc, ok := qv.indexedFieldAccessorByOrdinal(ordinal)
	if !ok || qv.snap.index == nil || acc.ordinal >= qv.snap.index.Len() {
		return fieldOverlay{}
	}
	return newFieldOverlayStorage(qv.snap.index.Get(acc.ordinal))
}

func (qv *queryView) fieldOverlayRef(field string, ordinal int) fieldOverlay {
	acc, ok := qv.indexedFieldAccessor(field, ordinal)
	if !ok {
		return fieldOverlay{}
	}
	return qv.fieldOverlayByOrdinal(acc.ordinal)
}

func (qv *queryView) fieldOverlay(field string) fieldOverlay {
	acc, ok := qv.indexedFieldAccessorByName(field)
	if !ok {
		return fieldOverlay{}
	}
	return qv.fieldOverlayByOrdinal(acc.ordinal)
}

func (qv *queryView) fieldOverlayForExpr(expr qir.Expr) fieldOverlay {
	acc, ok := qv.indexedFieldAccessorByOrdinal(expr.FieldOrdinal)
	if !ok {
		return fieldOverlay{}
	}
	return qv.fieldOverlayByOrdinal(acc.ordinal)
}

func (qv *queryView) fieldOverlayForOrder(order qir.Order) fieldOverlay {
	acc, ok := qv.indexedFieldAccessorByOrdinal(order.FieldOrdinal)
	if !ok {
		return fieldOverlay{}
	}
	return qv.fieldOverlayByOrdinal(acc.ordinal)
}

func (qv *queryView) nilFieldOverlayByOrdinal(ordinal int) fieldOverlay {
	acc, ok := qv.indexedFieldAccessorByOrdinal(ordinal)
	if !ok || qv.snap.nilIndex == nil || acc.ordinal >= qv.snap.nilIndex.Len() {
		return fieldOverlay{}
	}
	return newFieldOverlayStorage(qv.snap.nilIndex.Get(acc.ordinal))
}

func (qv *queryView) nilFieldOverlayRef(field string, ordinal int) fieldOverlay {
	acc, ok := qv.indexedFieldAccessor(field, ordinal)
	if !ok {
		return fieldOverlay{}
	}
	return qv.nilFieldOverlayByOrdinal(acc.ordinal)
}

func (qv *queryView) nilFieldOverlay(field string) fieldOverlay {
	acc, ok := qv.indexedFieldAccessorByName(field)
	if !ok {
		return fieldOverlay{}
	}
	return qv.nilFieldOverlayByOrdinal(acc.ordinal)
}

func (qv *queryView) nilFieldOverlayForExpr(expr qir.Expr) fieldOverlay {
	acc, ok := qv.indexedFieldAccessorByOrdinal(expr.FieldOrdinal)
	if !ok {
		return fieldOverlay{}
	}
	return qv.nilFieldOverlayByOrdinal(acc.ordinal)
}

func (qv *queryView) nilFieldOverlayForOrder(order qir.Order) fieldOverlay {
	acc, ok := qv.indexedFieldAccessorByOrdinal(order.FieldOrdinal)
	if !ok {
		return fieldOverlay{}
	}
	return qv.nilFieldOverlayByOrdinal(acc.ordinal)
}

func (qv *queryView) lenFieldOverlayByOrdinal(ordinal int) fieldOverlay {
	acc, ok := qv.indexedFieldAccessorByOrdinal(ordinal)
	if !ok || qv.snap.lenIndex == nil || acc.ordinal >= qv.snap.lenIndex.Len() {
		return fieldOverlay{}
	}
	return newFieldOverlayStorage(qv.snap.lenIndex.Get(acc.ordinal))
}

func (qv *queryView) lenFieldOverlayRef(field string, ordinal int) fieldOverlay {
	acc, ok := qv.indexedFieldAccessor(field, ordinal)
	if !ok {
		return fieldOverlay{}
	}
	return qv.lenFieldOverlayByOrdinal(acc.ordinal)
}

func (qv *queryView) lenFieldOverlay(field string) fieldOverlay {
	acc, ok := qv.indexedFieldAccessorByName(field)
	if !ok {
		return fieldOverlay{}
	}
	return qv.lenFieldOverlayByOrdinal(acc.ordinal)
}

func (qv *queryView) lenFieldOverlayForExpr(expr qir.Expr) fieldOverlay {
	acc, ok := qv.indexedFieldAccessorByOrdinal(expr.FieldOrdinal)
	if !ok {
		return fieldOverlay{}
	}
	return qv.lenFieldOverlayByOrdinal(acc.ordinal)
}

func (qv *queryView) lenFieldOverlayForOrder(order qir.Order) fieldOverlay {
	acc, ok := qv.indexedFieldAccessorByOrdinal(order.FieldOrdinal)
	if !ok {
		return fieldOverlay{}
	}
	return qv.lenFieldOverlayByOrdinal(acc.ordinal)
}

func (qv *queryView) hasIndexedFieldOrdinal(ordinal int) bool {
	_, ok := qv.indexedFieldAccessorByOrdinal(ordinal)
	return ok
}

func (qv *queryView) hasIndexedFieldRef(field string, ordinal int) bool {
	_, ok := qv.indexedFieldAccessor(field, ordinal)
	return ok
}

func (qv *queryView) hasIndexedFieldForExpr(expr qir.Expr) bool {
	_, ok := qv.indexedFieldAccessorByOrdinal(expr.FieldOrdinal)
	return ok
}

func (qv *queryView) hasIndexedFieldForOrder(order qir.Order) bool {
	_, ok := qv.indexedFieldAccessorByOrdinal(order.FieldOrdinal)
	return ok
}

func (qv *queryView) hasIndexedField(field string) bool {
	_, ok := qv.fields[field]
	return ok
}

func (qv *queryView) hasIndexedLenField(field string) bool {
	f := qv.fields[field]
	return f != nil && f.Slice
}

func (qv *queryView) isLenZeroComplementOrdinal(ordinal int) bool {
	acc, ok := qv.indexedFieldAccessorByOrdinal(ordinal)
	if !ok || qv.lenZeroComplement == nil || acc.ordinal >= qv.lenZeroComplement.Len() {
		return false
	}
	return qv.lenZeroComplement.Get(acc.ordinal)
}

func (qv *queryView) isLenZeroComplementRef(field string, ordinal int) bool {
	acc, ok := qv.indexedFieldAccessor(field, ordinal)
	if !ok {
		return false
	}
	return qv.isLenZeroComplementOrdinal(acc.ordinal)
}

func (qv *queryView) isLenZeroComplementField(field string) bool {
	acc, ok := qv.indexedFieldAccessorByName(field)
	if !ok {
		return false
	}
	return qv.isLenZeroComplementOrdinal(acc.ordinal)
}
