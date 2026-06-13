package qexec

import (
	"math"
	"reflect"

	"github.com/vapstack/rbi/internal/indexdata"

	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
)

const normalizedScalarBoundCacheMaxEntries = 8

var stringKeyField = schema.Field{
	Name:      schema.ReservedKeyFieldName,
	Unique:    true,
	IndexKind: schema.IndexUnique,
	Kind:      reflect.String,
	KeyKind:   schema.FieldWriteKeysString,
	DBName:    schema.ReservedKeyFieldName,
	JSONName:  schema.ReservedKeyFieldName,
}

var numericKeyField = schema.Field{
	Name:      schema.ReservedKeyFieldName,
	Unique:    true,
	IndexKind: schema.IndexUnique,
	Kind:      reflect.Uint64,
	KeyKind:   schema.FieldWriteKeysOrderedU64,
	DBName:    schema.ReservedKeyFieldName,
	JSONName:  schema.ReservedKeyFieldName,
}

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

// View is a fully initialized, snapshot-bound query state.
// Zero value is invalid; construct it via Runtime.AcquireView.
type View struct {
	snap              *snapshot.View
	strKey            bool
	exec              *Runtime
	lenZeroComplement []bool

	normalizedScalarBoundCacheLen uint8
	normalizedScalarBoundCache    [normalizedScalarBoundCacheMaxEntries]normalizedScalarBoundCacheEntry
}

func newView(snap *snapshot.View, exec *Runtime) View {
	return View{
		snap:              snap,
		strKey:            exec.StrKey,
		exec:              exec,
		lenZeroComplement: snap.LenZeroComplement,
	}
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

func (qv *View) loadNormalizedScalarBound(expr qir.Expr, v reflect.Value) (normalizedScalarBound, bool) {
	key, ok := normalizedScalarBoundCacheValue(v, qv.fieldMetaByOrdinal(expr.FieldOrdinal))
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

func (qv *View) storeNormalizedScalarBound(expr qir.Expr, v reflect.Value, bound normalizedScalarBound) {
	key, ok := normalizedScalarBoundCacheValue(v, qv.fieldMetaByOrdinal(expr.FieldOrdinal))
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

func (qv *View) BeginTrace(q qir.Shape) *Trace {
	orderField := ""
	if q.HasOrder {
		orderField = qv.exec.FieldNameByOrdinal(q.Order.FieldOrdinal)
	}
	return qv.exec.BeginTrace(q, orderField)
}

func (qv *View) indexedFieldAccessorByName(field string) (schema.IndexedFieldAccessor, bool) {
	acc, ok := qv.exec.Schema.IndexedByName[field]
	return acc, ok
}

func (qv *View) fieldByOrdinal(ordinal int) (queryField, bool) {
	if ordinal < 0 || ordinal >= len(qv.exec.fields) {
		return queryField{}, false
	}
	return qv.exec.fields[ordinal], true
}

func (qv *View) indexedFieldAccessor(field string, ordinal int) (schema.IndexedFieldAccessor, bool) {
	if acc, ok := qv.indexedFieldAccessorByOrdinal(ordinal); ok {
		return acc, true
	}
	if field == "" {
		return schema.IndexedFieldAccessor{}, false
	}
	return qv.indexedFieldAccessorByName(field)
}

func (qv *View) fieldOrdinalByName(field string) int {
	ordinal, ok := qv.exec.fieldByName[field]
	if !ok {
		return qir.NoFieldOrdinal
	}
	return ordinal
}

func (qv *View) indexedFieldAccessorByOrdinal(ordinal int) (schema.IndexedFieldAccessor, bool) {
	field, ok := qv.fieldByOrdinal(ordinal)
	if !ok || field.kind != queryFieldOrdinary {
		return schema.IndexedFieldAccessor{}, false
	}
	return qv.exec.Schema.Indexed[field.storageOrdinal], true
}

func (qv *View) fieldMeta(field string, ordinal int) *schema.Field {
	if f, ok := qv.fieldByOrdinal(ordinal); ok {
		return f.meta
	}
	if field != "" {
		if f, ok := qv.fieldByOrdinal(qv.fieldOrdinalByName(field)); ok {
			return f.meta
		}
	}
	return nil
}

func (qv *View) fieldMetaByOrdinal(ordinal int) *schema.Field {
	field, ok := qv.fieldByOrdinal(ordinal)
	if !ok {
		return nil
	}
	return field.meta
}

func (qv *View) SnapshotUniverseCardinality() uint64 {
	return qv.snap.Universe.Cardinality()
}

func (qv *View) fieldNameByOrdinal(ordinal int) string {
	field, ok := qv.fieldByOrdinal(ordinal)
	if !ok {
		return ""
	}
	return field.name
}

func (qv *View) hasFieldOrdinal(ordinal int) bool {
	_, ok := qv.fieldByOrdinal(ordinal)
	return ok
}

func (qv *View) hasIndexedFieldOrdinal(ordinal int) bool {
	return qv.hasFieldOrdinal(ordinal)
}

func (qv *View) indexViewByOrdinal(ordinal int) indexdata.FieldIndexView {
	field, ok := qv.fieldByOrdinal(ordinal)
	if !ok {
		return indexdata.FieldIndexView{}
	}
	switch field.kind {
	case queryFieldOrdinary:
		return indexdata.NewFieldIndexViewFromStorage(qv.snap.Index[field.storageOrdinal])
	case queryFieldStringKey:
		return indexdata.NewFieldIndexViewFromStorage(qv.snap.KeyIndex)
	default:
		return indexdata.FieldIndexView{}
	}
}

func (qv *View) nilIndexViewByOrdinal(ordinal int) indexdata.FieldIndexView {
	field, ok := qv.fieldByOrdinal(ordinal)
	if !ok || field.kind != queryFieldOrdinary {
		return indexdata.FieldIndexView{}
	}
	return indexdata.NewFieldIndexViewFromStorage(qv.snap.NilIndex[field.storageOrdinal])
}

func (qv *View) lenIndexViewByOrdinal(ordinal int) indexdata.FieldIndexView {
	field, ok := qv.fieldByOrdinal(ordinal)
	if !ok || field.kind != queryFieldOrdinary {
		return indexdata.FieldIndexView{}
	}
	return indexdata.NewFieldIndexViewFromStorage(qv.snap.LenIndex[field.storageOrdinal])
}

func (qv *View) indexViewRef(field string, ordinal int) indexdata.FieldIndexView {
	if qv.hasFieldOrdinal(ordinal) {
		return qv.indexViewByOrdinal(ordinal)
	}
	if field == "" {
		return indexdata.FieldIndexView{}
	}
	return qv.indexViewByOrdinal(qv.fieldOrdinalByName(field))
}

func (qv *View) nilIndexViewRef(field string, ordinal int) indexdata.FieldIndexView {
	if qv.hasFieldOrdinal(ordinal) {
		return qv.nilIndexViewByOrdinal(ordinal)
	}
	if field == "" {
		return indexdata.FieldIndexView{}
	}
	return qv.nilIndexViewByOrdinal(qv.fieldOrdinalByName(field))
}

func (qv *View) lenIndexViewRef(field string, ordinal int) indexdata.FieldIndexView {
	if qv.hasFieldOrdinal(ordinal) {
		return qv.lenIndexViewByOrdinal(ordinal)
	}
	if field == "" {
		return indexdata.FieldIndexView{}
	}
	return qv.lenIndexViewByOrdinal(qv.fieldOrdinalByName(field))
}

func (qv *View) indexViewByName(field string) indexdata.FieldIndexView {
	return qv.indexViewByOrdinal(qv.fieldOrdinalByName(field))
}

func (qv *View) nilIndexViewByName(field string) indexdata.FieldIndexView {
	return qv.nilIndexViewByOrdinal(qv.fieldOrdinalByName(field))
}

func (qv *View) lenIndexViewByName(field string) indexdata.FieldIndexView {
	return qv.lenIndexViewByOrdinal(qv.fieldOrdinalByName(field))
}

func (qv *View) isLenZeroComplementOrdinal(ordinal int) bool {
	field, ok := qv.fieldByOrdinal(ordinal)
	if !ok || field.kind != queryFieldOrdinary {
		return false
	}
	return qv.lenZeroComplement[field.storageOrdinal]
}

func (qv *View) isLenZeroComplementRef(field string, ordinal int) bool {
	if qv.hasFieldOrdinal(ordinal) {
		return qv.isLenZeroComplementOrdinal(ordinal)
	}
	return qv.isLenZeroComplementOrdinal(qv.fieldOrdinalByName(field))
}
