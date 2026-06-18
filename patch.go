package rbi

import (
	"fmt"
	"reflect"
	"slices"
	"time"
	"unsafe"

	"github.com/vapstack/pooled"
	"github.com/vapstack/rbi/internal/schema"
)

// PatchOption controls MakePatch behaviour.
type PatchOption uint8

const (
	// PatchJSON makes MakePatch emit json tag names when present.
	// Fields without a json tag fall back to their Go struct field name only
	// when that name is an unambiguous patch identifier for the field.
	// A changed field with `json:"-"` or without a safe JSON name makes
	// MakePatch return an error instead of silently omitting the change.
	PatchJSON PatchOption = 1 << iota
)

// MakePatch builds and returns a patch describing fields that changed between
// oldVal and newVal.
//
// The patch includes both indexed and non-indexed fields. For every modified
// field it adds a Field entry whose Name uses the db tag when present.
// Fields without a db tag use Go struct field name only when that name
// is an unambiguous patch identifier for the field. If a modified field cannot
// be represented by a safe patch name, MakePatch returns an error.
//
// When PatchJSON is passed, Name uses the json tag when present.
// Fields without an explicit json name use their Go struct field name
// only if that name is an unambiguous patch identifier for the field.
// If a modified field cannot be represented by a safe JSON patch name,
// including fields tagged json:"-", MakePatch returns an error.
// PatchJSON still builds a full patch,
// it does not silently drop changes outside the JSON representation.
//
// Value is a deep copy taken from newVal, including nested unexported fields.
// MakePatch supports normal record data graphs: scalars, structs, slices,
// maps, pointers, and interfaces containing data values.
// It is not a general object cloner. Runtime state such as sync/atomic values,
// locks, channels, functions, and other unsafe resources are not supported
// and are not diagnosed. MakePatch does not return errors for such values
// and copy safety is provided on a best-effort basis.
//
// If newVal is nil, it returns an empty slice.
func (db *DB[K, V]) MakePatch(oldVal, newVal *V, opts ...PatchOption) ([]Field, error) {
	useJSON := false
	for _, opt := range opts {
		if opt == PatchJSON {
			useJSON = true
		}
	}
	return db.makePatch(oldVal, newVal, nil, useJSON)
}

// MakePatchInto is like MakePatch, but writes the result into the provided
// buffer to reduce allocations.
//
// dst is treated as scratch space: it will be reset to length 0 and then filled
// with the resulting patch. The returned slice may refer to the same underlying
// array or a grown one if capacity is insufficient.
//
// If newVal is nil, it returns an empty slice.
// On error, returned slice is reset to length 0.
func (db *DB[K, V]) MakePatchInto(oldVal, newVal *V, dst []Field, opts ...PatchOption) ([]Field, error) {
	useJSON := false
	for _, opt := range opts {
		if opt == PatchJSON {
			useJSON = true
		}
	}
	return db.makePatch(oldVal, newVal, dst, useJSON)
}

type patchScratch struct {
	seen []bool
}

var patchScratchPool = pooled.Pointers[patchScratch]{
	Cleanup: func(scratch *patchScratch) {
		clear(scratch.seen[:cap(scratch.seen)])
		scratch.seen = scratch.seen[:0]
	},
}

func (db *DB[K, V]) makePatch(oldVal, newVal *V, target []Field, useJSON bool) ([]Field, error) {
	target = target[:0]

	if newVal == nil {
		return target, nil
	}

	var rvOld, rvNew reflect.Value
	if oldVal != nil {
		rvOld = reflect.ValueOf(oldVal).Elem()
	}
	rvNew = reflect.ValueOf(newVal).Elem()

	scratch := patchScratchPool.Get()
	patchAccess := db.schema.Patch.Access
	scratch.seen = slices.Grow(scratch.seen[:0], len(patchAccess))[:len(patchAccess)]
	defer patchScratchPool.Put(scratch)

	newPtr := unsafe.Pointer(newVal)
	oldPtr := unsafe.Pointer(nil)
	if oldVal != nil {
		oldPtr = unsafe.Pointer(oldVal)
	}

	var patchErr error
	db.forEachModifiedIndexedField(oldVal, newVal, func(acc schema.IndexedFieldAccessor) bool {
		if acc.PatchOrdinal < 0 {
			return true
		}
		ordinal := patchCoverOrdinal(patchAccess, acc.PatchOrdinal, useJSON)
		if scratch.seen[ordinal] {
			return true
		}
		patchAcc := patchAccess[ordinal]
		name := patchFieldName(patchAcc.Field, useJSON)
		if useJSON {
			if name == "" {
				patchErr = fmt.Errorf("field %v with db name %q cannot be emitted with PatchJSON: add an explicit non-empty json tag", patchAcc.Field.Name, patchAcc.Field.DBName)
				return false
			}
		} else if name == "" {
			patchErr = fmt.Errorf("field %v cannot be emitted by MakePatch: add an explicit non-empty db tag", patchAcc.Field.Name)
			return false
		}
		var value any
		if patchAcc.CopyValue != nil {
			value = patchAcc.CopyValue(newPtr)
		} else {
			value = deepCopyValue(rvNew.FieldByIndex(patchAcc.Field.Index).Interface())
		}
		markPatchSubtreeSeen(scratch.seen, patchAccess, ordinal)
		target = append(target, Field{
			Name:  name,
			Value: value,
		})
		return true
	})
	if patchErr != nil {
		return target[:0], patchErr
	}

	for ordinal, patchAcc := range patchAccess {
		if scratch.seen[ordinal] {
			continue
		}

		var newValue any
		if rvOld.IsValid() {
			if patchAcc.ValueEqual != nil {
				if patchAcc.ValueEqual(oldPtr, newPtr) {
					continue
				}
			} else {
				oldValue := rvOld.FieldByIndex(patchAcc.Field.Index).Interface()
				newValue = rvNew.FieldByIndex(patchAcc.Field.Index).Interface()
				if reflect.DeepEqual(oldValue, newValue) {
					continue
				}
			}
		}

		name := patchFieldName(patchAcc.Field, useJSON)
		if useJSON {
			if name == "" {
				return target[:0], fmt.Errorf("field %v with db name %q cannot be emitted with PatchJSON: add an explicit non-empty json tag", patchAcc.Field.Name, patchAcc.Field.DBName)
			}
		} else if name == "" {
			return target[:0], fmt.Errorf("field %v cannot be emitted by MakePatch: add an explicit non-empty db tag", patchAcc.Field.Name)
		}

		if patchAcc.CopyValue != nil {
			newValue = patchAcc.CopyValue(newPtr)
		} else if newValue == nil {
			newValue = deepCopyValue(rvNew.FieldByIndex(patchAcc.Field.Index).Interface())
		} else {
			newValue = deepCopyValue(newValue)
		}

		target = append(target, Field{
			Name:  name,
			Value: newValue,
		})
		markPatchSubtreeSeen(scratch.seen, patchAccess, ordinal)
	}

	return target, nil
}

func patchFieldName(f *schema.Field, useJSON bool) string {
	if useJSON {
		return f.JSONName
	}
	return f.DBName
}

func patchCoverOrdinal(access []schema.PatchFieldAccessor, ordinal int, useJSON bool) int {
	index := access[ordinal].Field.Index
	if len(index) == 1 {
		return ordinal
	}

	cover := ordinal
	first := index[0]
	for i := ordinal - 1; i >= 0; i-- {
		parent := access[i].Field.Index
		if parent[0] != first {
			break
		}
		if len(parent) >= len(index) || !slices.Equal(index[:len(parent)], parent) {
			continue
		}
		if patchFieldName(access[i].Field, useJSON) != "" {
			cover = i
		}
	}
	return cover
}

func markPatchSubtreeSeen(seen []bool, access []schema.PatchFieldAccessor, ordinal int) {
	seen[ordinal] = true
	parentIndex := access[ordinal].Field.Index
	for ordinal++; ordinal < len(access); ordinal++ {
		childIndex := access[ordinal].Field.Index
		if len(childIndex) <= len(parentIndex) || !slices.Equal(childIndex[:len(parentIndex)], parentIndex) {
			break
		}
		seen[ordinal] = true
	}
}

func patchItemsForWrite(fields []Field) []schema.PatchItem {
	// Field and schema.PatchItem are layout-identical; wexec copies this view
	// into request-owned storage immediately.
	return unsafe.Slice((*schema.PatchItem)(unsafe.SliceData(fields)), len(fields))
}

func (db *DB[K, V]) forEachModifiedAccessor(accessors []schema.IndexedFieldAccessor, v1 *V, v2 *V, fn func(schema.IndexedFieldAccessor) bool) {
	if fn == nil {
		return
	}
	if len(accessors) == 0 {
		return
	}
	if v1 == nil || v2 == nil {
		for _, acc := range accessors {
			if !fn(acc) {
				return
			}
		}
		return
	}
	ptr1 := unsafe.Pointer(v1)
	ptr2 := unsafe.Pointer(v2)
	for _, acc := range accessors {
		if acc.Modified(ptr1, ptr2) && !fn(acc) {
			return
		}
	}
}

func (db *DB[K, V]) forEachModifiedIndexedField(v1 *V, v2 *V, fn func(schema.IndexedFieldAccessor) bool) {
	if db.index == nil {
		return
	}
	db.forEachModifiedAccessor(db.schema.Indexed, v1, v2, fn)
}

func deepCopyValue(src any) any {
	if src == nil {
		return nil
	}
	origin := reflect.ValueOf(src)

	for origin.Kind() == reflect.Interface {
		if origin.IsNil() {
			return nil
		}
		origin = origin.Elem()
	}

	switch origin.Kind() {
	case reflect.Bool, reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64,
		reflect.Complex64, reflect.Complex128:
		return origin.Interface()
	}

	var state deepCopyState
	clone := deepCopy(origin, &state)
	return clone.Interface()
}

type deepCopyState struct {
	refs   map[refCopyKey]reflect.Value
	slices map[sliceCopyKey]reflect.Value
}

type refCopyKey struct {
	ptr uintptr
	typ reflect.Type
}

type sliceCopyKey struct {
	ptr uintptr
	typ reflect.Type
	len int
	cap int
}

var timeTimeType = reflect.TypeFor[time.Time]()

func deepCopy(origin reflect.Value, state *deepCopyState) reflect.Value {
	if !origin.IsValid() {
		return origin
	}

	kind := origin.Kind()

	switch kind {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Interface:
		if origin.IsNil() {
			return origin
		}
	}

	var refKey refCopyKey
	switch kind {
	case reflect.Ptr:
		typ := origin.Type()
		if typ.Name() != "" {
			typ = reflect.PointerTo(typ.Elem())
		}
		refKey = refCopyKey{ptr: origin.Pointer(), typ: typ}
	case reflect.Map:
		typ := origin.Type()
		if typ.Name() != "" {
			typ = reflect.MapOf(typ.Key(), typ.Elem())
		}
		refKey = refCopyKey{ptr: origin.Pointer(), typ: typ}
	}
	if refKey.typ != nil {
		if state.refs != nil {
			if clone, ok := state.refs[refKey]; ok {
				if clone.Type() != origin.Type() {
					clone = clone.Convert(origin.Type())
				}
				return clone
			}
		}
	}
	if kind == reflect.Slice {
		if origin.Len() != 0 && state.slices != nil {
			key := sliceCopyKey{ptr: origin.Pointer(), typ: origin.Type(), len: origin.Len(), cap: origin.Cap()}
			if clone, ok := state.slices[key]; ok {
				return clone
			}
		}
	}

	switch kind {
	case reflect.Bool, reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64,
		reflect.Complex64, reflect.Complex128:
		return origin

	case reflect.Struct:
		typ := origin.Type()
		if typ == timeTimeType ||
			typ.NumField() == timeTimeType.NumField() && typ.ConvertibleTo(timeTimeType) && timeTimeType.ConvertibleTo(typ) {
			return origin
		}
		s := reflect.New(origin.Type()).Elem()
		s.Set(origin)
		for i := 0; i < origin.NumField(); i++ {
			sf := s.Field(i)
			if !sf.CanSet() {
				sf = reflect.NewAt(sf.Type(), unsafe.Pointer(sf.UnsafeAddr())).Elem()
			}
			clone := deepCopy(sf, state)
			sf.Set(clone)
		}
		return s

	case reflect.Ptr:
		ptr := reflect.New(origin.Type().Elem())
		if state.refs == nil {
			state.refs = make(map[refCopyKey]reflect.Value)
		}
		state.refs[refKey] = ptr
		clone := deepCopy(origin.Elem(), state)
		ptr.Elem().Set(clone)
		if ptr.Type() != origin.Type() {
			return ptr.Convert(origin.Type())
		}
		return ptr

	case reflect.Slice:
		s := reflect.MakeSlice(origin.Type(), origin.Len(), origin.Cap())
		if origin.Len() != 0 {
			if state.slices == nil {
				state.slices = make(map[sliceCopyKey]reflect.Value)
			}
			state.slices[sliceCopyKey{ptr: origin.Pointer(), typ: origin.Type(), len: origin.Len(), cap: origin.Cap()}] = s
		}
		for i := 0; i < origin.Len(); i++ {
			clone := deepCopy(origin.Index(i), state)
			s.Index(i).Set(clone)
		}
		return s

	case reflect.Map:
		m := reflect.MakeMap(origin.Type())
		if state.refs == nil {
			state.refs = make(map[refCopyKey]reflect.Value)
		}
		state.refs[refKey] = m
		for _, key := range origin.MapKeys() {
			keyClone := deepCopy(key, state)
			valClone := deepCopy(origin.MapIndex(key), state)
			m.SetMapIndex(keyClone, valClone)
		}
		return m

	case reflect.Array:
		a := reflect.New(origin.Type()).Elem()
		for i := 0; i < origin.Len(); i++ {
			clone := deepCopy(origin.Index(i), state)
			a.Index(i).Set(clone)
		}
		return a

	case reflect.Interface:
		clone := deepCopy(origin.Elem(), state)
		if !clone.IsValid() {
			return reflect.Zero(origin.Type())
		}
		return clone.Convert(origin.Type())

	case reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return reflect.Zero(origin.Type())

	default:
		panic(fmt.Errorf("rbi: deepCopy: unsupported value kind: %v", kind))
	}
}
