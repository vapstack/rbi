package rbi

import (
	"math"
	"reflect"

	"github.com/vapstack/rbi/internal/indexdata"

	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/internal/strmap"
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
	engine            *queryEngine
	snap              *snapshot.View
	strKey            bool
	strMapView        *strmap.Snapshot
	planner           *planner
	lenZeroComplement []bool

	normalizedScalarBoundCacheLen uint8
	normalizedScalarBoundCache    [normalizedScalarBoundCacheMaxEntries]normalizedScalarBoundCacheEntry
}

func normalizedScalarBoundCacheValue(v reflect.Value, fm *schema.Field) (normalizedScalarBoundCacheEntry, bool) {
	if !v.IsValid() {
		return normalizedScalarBoundCacheEntry{}, false
	}

	if schema.IsNativeTimeField(fm) {
		if unix, ok := schema.QueryValueToUnixSeconds(v); ok {
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

func (qv *queryView) indexedFieldAccessorByName(field string) (schema.IndexedFieldAccessor, bool) {
	acc, ok := qv.engine.schema.IndexedByName[field]
	return acc, ok
}

func (qv *queryView) indexedFieldAccessor(field string, ordinal int) (schema.IndexedFieldAccessor, bool) {
	if acc, ok := qv.indexedFieldAccessorByOrdinal(ordinal); ok {
		return acc, true
	}
	if field == "" {
		return schema.IndexedFieldAccessor{}, false
	}
	return qv.indexedFieldAccessorByName(field)
}

func (qv *queryView) fieldOrdinalByName(field string) int {
	acc, ok := qv.indexedFieldAccessorByName(field)
	if !ok {
		return -1
	}
	return acc.Ordinal
}

func (qv *queryView) indexedFieldAccessorByOrdinal(ordinal int) (schema.IndexedFieldAccessor, bool) {
	if ordinal < 0 || ordinal >= len(qv.engine.schema.Indexed) {
		return schema.IndexedFieldAccessor{}, false
	}
	return qv.engine.schema.Indexed[ordinal], true
}

func (qv *queryView) fieldMetaByOrdinal(ordinal int) *schema.Field {
	acc, ok := qv.indexedFieldAccessorByOrdinal(ordinal)
	if !ok {
		return nil
	}
	return acc.Field
}

func (qv *queryView) fieldMeta(field string, ordinal int) *schema.Field {
	acc, ok := qv.indexedFieldAccessor(field, ordinal)
	if !ok {
		return nil
	}
	return acc.Field
}

func (qv *queryView) fieldMetaByExpr(expr qir.Expr) *schema.Field {
	acc, ok := qv.indexedFieldAccessorByOrdinal(expr.FieldOrdinal)
	if !ok {
		return nil
	}
	return acc.Field
}

func (qv *queryView) fieldMetaByOrder(order qir.Order) *schema.Field {
	acc, ok := qv.indexedFieldAccessorByOrdinal(order.FieldOrdinal)
	if !ok {
		return nil
	}
	return acc.Field
}

func (qv *queryView) snapshotUniverseCardinality() uint64 {
	return qv.snap.Universe.Cardinality()
}

func (qv *queryView) snapshotUniverseView() posting.List {
	return qv.snap.Universe.Borrow()
}

func fieldIndexViewFromSlots(slots []indexdata.FieldStorage, acc schema.IndexedFieldAccessor) indexdata.FieldIndexView {
	if acc.Ordinal >= len(slots) {
		return indexdata.FieldIndexView{}
	}
	return indexdata.NewFieldIndexViewFromStorage(slots[acc.Ordinal])
}

func (qv *queryView) fieldIndexViewFromSlotsByOrdinal(slots []indexdata.FieldStorage, ordinal int) indexdata.FieldIndexView {
	acc, ok := qv.indexedFieldAccessorByOrdinal(ordinal)
	if !ok {
		return indexdata.FieldIndexView{}
	}
	return fieldIndexViewFromSlots(slots, acc)
}

func (qv *queryView) fieldIndexViewFromSlotsRef(slots []indexdata.FieldStorage, field string, ordinal int) indexdata.FieldIndexView {
	acc, ok := qv.indexedFieldAccessor(field, ordinal)
	if !ok {
		return indexdata.FieldIndexView{}
	}
	return fieldIndexViewFromSlots(slots, acc)
}

func (qv *queryView) fieldIndexViewFromSlotsByName(slots []indexdata.FieldStorage, field string) indexdata.FieldIndexView {
	acc, ok := qv.indexedFieldAccessorByName(field)
	if !ok {
		return indexdata.FieldIndexView{}
	}
	return fieldIndexViewFromSlots(slots, acc)
}

func (qv *queryView) fieldIndexViewFromSlotsForExpr(slots []indexdata.FieldStorage, expr qir.Expr) indexdata.FieldIndexView {
	acc, ok := qv.indexedFieldAccessorByOrdinal(expr.FieldOrdinal)
	if !ok {
		return indexdata.FieldIndexView{}
	}
	return fieldIndexViewFromSlots(slots, acc)
}

func (qv *queryView) fieldIndexViewFromSlotsForOrder(slots []indexdata.FieldStorage, order qir.Order) indexdata.FieldIndexView {
	acc, ok := qv.indexedFieldAccessorByOrdinal(order.FieldOrdinal)
	if !ok {
		return indexdata.FieldIndexView{}
	}
	return fieldIndexViewFromSlots(slots, acc)
}

func (qv *queryView) fieldIndexViewByOrdinal(ordinal int) indexdata.FieldIndexView {
	return qv.fieldIndexViewFromSlotsByOrdinal(qv.snap.Index, ordinal)
}

func (qv *queryView) fieldIndexViewRef(field string, ordinal int) indexdata.FieldIndexView {
	return qv.fieldIndexViewFromSlotsRef(qv.snap.Index, field, ordinal)
}

func (qv *queryView) fieldIndexView(field string) indexdata.FieldIndexView {
	return qv.fieldIndexViewFromSlotsByName(qv.snap.Index, field)
}

func (qv *queryView) fieldIndexViewForExpr(expr qir.Expr) indexdata.FieldIndexView {
	return qv.fieldIndexViewFromSlotsForExpr(qv.snap.Index, expr)
}

func (qv *queryView) fieldIndexViewForOrder(order qir.Order) indexdata.FieldIndexView {
	return qv.fieldIndexViewFromSlotsForOrder(qv.snap.Index, order)
}

func (qv *queryView) nilFieldIndexViewByOrdinal(ordinal int) indexdata.FieldIndexView {
	return qv.fieldIndexViewFromSlotsByOrdinal(qv.snap.NilIndex, ordinal)
}

func (qv *queryView) nilFieldIndexViewRef(field string, ordinal int) indexdata.FieldIndexView {
	return qv.fieldIndexViewFromSlotsRef(qv.snap.NilIndex, field, ordinal)
}

func (qv *queryView) nilFieldIndexView(field string) indexdata.FieldIndexView {
	return qv.fieldIndexViewFromSlotsByName(qv.snap.NilIndex, field)
}

func (qv *queryView) nilFieldIndexViewForExpr(expr qir.Expr) indexdata.FieldIndexView {
	return qv.fieldIndexViewFromSlotsForExpr(qv.snap.NilIndex, expr)
}

func (qv *queryView) nilFieldIndexViewForOrder(order qir.Order) indexdata.FieldIndexView {
	return qv.fieldIndexViewFromSlotsForOrder(qv.snap.NilIndex, order)
}

func (qv *queryView) lenFieldIndexViewByOrdinal(ordinal int) indexdata.FieldIndexView {
	return qv.fieldIndexViewFromSlotsByOrdinal(qv.snap.LenIndex, ordinal)
}

func (qv *queryView) lenFieldIndexViewRef(field string, ordinal int) indexdata.FieldIndexView {
	return qv.fieldIndexViewFromSlotsRef(qv.snap.LenIndex, field, ordinal)
}

func (qv *queryView) lenFieldIndexView(field string) indexdata.FieldIndexView {
	return qv.fieldIndexViewFromSlotsByName(qv.snap.LenIndex, field)
}

func (qv *queryView) lenFieldIndexViewForExpr(expr qir.Expr) indexdata.FieldIndexView {
	return qv.fieldIndexViewFromSlotsForExpr(qv.snap.LenIndex, expr)
}

func (qv *queryView) lenFieldIndexViewForOrder(order qir.Order) indexdata.FieldIndexView {
	return qv.fieldIndexViewFromSlotsForOrder(qv.snap.LenIndex, order)
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
	_, ok := qv.engine.schema.Fields[field]
	return ok
}

func (qv *queryView) isLenZeroComplementOrdinal(ordinal int) bool {
	acc, ok := qv.indexedFieldAccessorByOrdinal(ordinal)
	if !ok || acc.Ordinal >= len(qv.lenZeroComplement) {
		return false
	}
	return qv.lenZeroComplement[acc.Ordinal]
}

func (qv *queryView) isLenZeroComplementRef(field string, ordinal int) bool {
	acc, ok := qv.indexedFieldAccessor(field, ordinal)
	if !ok {
		return false
	}
	return qv.isLenZeroComplementOrdinal(acc.Ordinal)
}

func (qv *queryView) isLenZeroComplementField(field string) bool {
	acc, ok := qv.indexedFieldAccessorByName(field)
	if !ok {
		return false
	}
	return qv.isLenZeroComplementOrdinal(acc.Ordinal)
}
