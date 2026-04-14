package rbi

import (
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"unsafe"
)

type field struct {
	Name     string
	Unique   bool
	Kind     reflect.Kind
	Ptr      bool
	Slice    bool
	UseVI    bool
	KeyKind  fieldWriteKeyKind
	DBName   string
	JSONName string
	Index    []int
}

type fieldWriteKeyKind uint8

const (
	fieldWriteKeysString fieldWriteKeyKind = iota
	fieldWriteKeysOrderedU64
)

// ValueIndexer defines how a field value is converted into a canonical string
// representation used as an index key in rbi.
//
// A type that implements ValueIndexer is responsible for ensuring that
// IndexingValue returns a valid and stable string for every value that may
// appear in indexed data. This includes handling nil receivers if the type
// is a pointer or otherwise nillable. The caller does not perform nil
// checks before invoking IndexingValue.
//
// IndexingValue must return a deterministic string: the same value must
// always produce the same indexing key.
//
// The returned string is compared lexicographically when evaluating
// range queries (>, >=, <, <=). Implementation must ensure that the
// produced ordering matches the intent.
type ValueIndexer interface {
	IndexingValue() string
}

var viType = reflect.TypeFor[ValueIndexer]()

func inferFieldWriteKeyKind(kind reflect.Kind, useVI bool) fieldWriteKeyKind {
	if useVI {
		return fieldWriteKeysString
	}
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return fieldWriteKeysOrderedU64
	default:
		return fieldWriteKeysString
	}
}

func (db *DB[K, V]) populateFields(t reflect.Type, idx []int) error {

	for i := 0; i < t.NumField(); i++ {

		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		tag := f.Tag.Get("rbi")
		if tag == "" {
			tag = f.Tag.Get("dbi")
		}
		var (
			use    bool
			skip   bool
			unique bool
		)
		switch value := strings.Split(tag, ",")[0]; value {
		case "":
			// do not use, do not skip (for embedded structs)
		case "-":
			skip = true
		case "default":
			use = true
		case "unique":
			use = true
			unique = true
		default:
			return fmt.Errorf("invalid index tag value %q on field %v", value, f.Name)
		}
		if f.Anonymous {
			if f.Type.Kind() == reflect.Struct {
				if unique {
					return fmt.Errorf("unique is not supported for anonymous embedded struct field %v", f.Name)
				}
				if skip {
					continue
				}
				newIdx := append(slices.Clone(idx), i)
				if err := db.populateFields(f.Type, newIdx); err != nil {
					return err
				}
			}
			continue
		}
		if skip || !use {
			continue
		}
		dbname := f.Name
		if dbTag := f.Tag.Get("db"); dbTag != "" && dbTag != "-" {
			dbname = dbTag
		}

		kind := f.Type.Kind()

		var (
			ptr   bool
			slice bool
			useVI bool
		)

		useVI = f.Type.Implements(viType)

		if kind == reflect.Slice && !useVI {
			slice = true
			elem := f.Type.Elem()
			kind = elem.Kind()
			useVI = elem.Implements(viType)
			if !useVI {
				switch kind {
				case reflect.Bool,
					reflect.Int,
					reflect.Int8,
					reflect.Int16,
					reflect.Int32,
					reflect.Int64,
					reflect.Uint,
					reflect.Uint8,
					reflect.Uint16,
					reflect.Uint32,
					reflect.Uint64,
					reflect.Uintptr,
					reflect.Float32,
					reflect.Float64,
					reflect.String:
					// OK
				default:
					return fmt.Errorf("slice elements must either be of a simple type or implement the ValueIndexer interface")
				}
			}
		} else if !useVI {

			switch kind {

			case reflect.Struct:
				return fmt.Errorf("cannot index field %v of type %v, consider implementing ValueIndexer interface", f.Name, f.Type)

			case reflect.Array:
				return fmt.Errorf("cannot index field %v of array type, use slice instead", f.Name)

			case reflect.Invalid,
				reflect.Func,
				reflect.Map,
				reflect.Chan,
				reflect.Complex64,
				reflect.Complex128,
				reflect.Interface,
				reflect.UnsafePointer,
				reflect.Uintptr:
				return fmt.Errorf("cannot index field %v of type %v", f.Name, f.Type)

			case reflect.Pointer:
				ptr = true
				kind = f.Type.Elem().Kind()
				switch kind {
				case reflect.Struct:
					return fmt.Errorf("cannot index field %v of type %v, consider implementing ValueIndexer interface", f.Name, f.Type)
				case reflect.Invalid,
					reflect.Func,
					reflect.Map,
					reflect.Slice,
					reflect.Array,
					reflect.Chan,
					reflect.Complex64,
					reflect.Complex128,
					reflect.Interface,
					reflect.UnsafePointer,
					reflect.Uintptr,
					reflect.Pointer:
					return fmt.Errorf("cannot index field %v of type %v", f.Name, f.Type)
				}

			}
		}

		if unique {
			if slice {
				return fmt.Errorf("%v (%v): unique is not supported for slice fields", f.Name, f.Type)
			}
			db.hasUnique = true
		}

		db.fields[dbname] = &field{ // last wins
			Name:    f.Name,
			Unique:  unique,
			Kind:    kind,
			Ptr:     ptr,
			Slice:   slice,
			UseVI:   useVI,
			KeyKind: inferFieldWriteKeyKind(kind, useVI),
			DBName:  dbname,
			Index:   append(slices.Clone(idx), i),
		}
	}
	return nil
}

type (
	uniqueScalarGetterFn        func(ptr unsafe.Pointer) (string, bool, bool)
	buildFieldWriteAccessorFn   func(ptr unsafe.Pointer, sink buildFieldWriteSink)
	overlayFieldWriteAccessorFn func(ptr unsafe.Pointer, sink snapshotOverlayWriteSink)
	insertFieldWriteAccessorFn  func(ptr unsafe.Pointer, sink snapshotInsertWriteSink)
	scratchFieldWriteAccessorFn func(ptr unsafe.Pointer, sink *fieldWriteScratch)
)

func (db *DB[K, V]) forEachModifiedAccessor(accessors []indexedFieldAccessor, v1 *V, v2 *V, fn func(indexedFieldAccessor) bool) {
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
		if acc.modified != nil && acc.modified(ptr1, ptr2) && !fn(acc) {
			return
		}
	}
}

func (db *DB[K, V]) forEachModifiedIndexedField(v1 *V, v2 *V, fn func(indexedFieldAccessor) bool) {
	db.forEachModifiedAccessor(db.indexedFieldAccess, v1, v2, fn)
}

func (db *DB[K, V]) getModifiedIndexedFields(v1 *V, v2 *V) []string {
	var modified []string
	db.forEachModifiedIndexedField(v1, v2, func(acc indexedFieldAccessor) bool {
		modified = append(modified, acc.name)
		return true
	})
	return modified
}

func (db *DB[K, V]) applyPatch(v *V, patch []Field, ignoreUnknown bool) error {
	rv := reflect.ValueOf(v).Elem()

	for _, p := range patch {
		f, ok := db.patchMap[p.Name]
		if !ok {
			if ignoreUnknown {
				continue
			}
			return fmt.Errorf("cannot patch field %v: field information is missing", p.Name)
		}

		fv := rv.FieldByIndex(f.Index)
		if !fv.CanSet() {
			continue
		}

		if p.Value == nil {
			switch fv.Kind() {
			case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Interface, reflect.Func, reflect.Chan:
				if !fv.IsNil() {
					fv.Set(reflect.Zero(fv.Type()))
				}
			default:
				return fmt.Errorf("field %v: cannot assign nil to non-nillable field", p.Name)
			}
			continue
		}

		if err := db.setReflectValue(fv, p.Value); err != nil {
			return fmt.Errorf("field %v: %w", p.Name, err)
		}
	}
	return nil
}

func (db *DB[K, V]) setReflectValue(fv reflect.Value, val any) error {

	sv := reflect.ValueOf(val)
	if !sv.IsValid() {
		return nil
	}

	if fv.Kind() == reflect.Slice {
		if sv.Kind() != reflect.Slice {
			return fmt.Errorf("source is not a slice, but destination is")
		}

		vt := fv.Type()
		et := vt.Elem()

		slice := reflect.MakeSlice(vt, sv.Len(), sv.Len())

		for i := 0; i < sv.Len(); i++ {
			src := sv.Index(i).Interface()
			rsv := slice.Index(i)

			if src == nil {
				switch et.Kind() {
				case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Interface, reflect.Func, reflect.Chan:
					continue
				default:
					return fmt.Errorf("cannot set nil to non-pointer slice element at index %v", i)
				}
			}
			if err := db.setReflectValue(rsv, src); err != nil {
				return fmt.Errorf("slice index %v: %w", i, err)
			}
		}

		fv.Set(slice)

		return nil
	}

	if fv.Kind() == reflect.Pointer {

		if sv.Kind() == reflect.Pointer && sv.IsNil() {
			fv.Set(reflect.Zero(fv.Type()))
			return nil
		}

		ptr := reflect.New(fv.Type().Elem())

		if err := db.setReflectValue(ptr.Elem(), val); err != nil {
			return err
		}

		fv.Set(ptr)

		return nil
	}

	if sv.Kind() == reflect.Pointer {
		if sv.IsNil() {
			return fmt.Errorf("cannot assign nil to non-pointer field")
		}
		sv = sv.Elem()
	}

	srcKind := sv.Kind()
	dstKind := fv.Kind()

	if handled, err := setReflectNumericValue(fv, sv, srcKind, dstKind); handled {
		return err
	}

	if sv.Type().ConvertibleTo(fv.Type()) {
		fv.Set(sv.Convert(fv.Type()))
		return nil
	}

	return fmt.Errorf("type mismatch: cannot convert %v to %v", sv.Type(), fv.Type())
}

func setReflectNumericValue(fv, sv reflect.Value, srcKind, dstKind reflect.Kind) (bool, error) {
	if !isReflectNumericKind(srcKind) || !isReflectNumericKind(dstKind) {
		return false, nil
	}

	switch {
	case isReflectFloatKind(srcKind):
		f64 := sv.Float()

		if isReflectFloatKind(dstKind) {
			if fv.OverflowFloat(f64) {
				return true, fmt.Errorf("value %v overflows float field (%v)", f64, fv.Type())
			}
			fv.SetFloat(f64)
			return true, nil
		}

		if math.IsNaN(f64) || math.IsInf(f64, 0) {
			return true, fmt.Errorf("cannot assign float %v to %v field", f64, dstKind)
		}

		if f64 != math.Trunc(f64) {
			if isReflectIntKind(dstKind) {
				return true, fmt.Errorf("cannot assign float %v to int field (loss of precision)", f64)
			}
			return true, fmt.Errorf("cannot assign float %v to uint field", f64)
		}

		if isReflectIntKind(dstKind) {
			minInt := -math.Ldexp(1, fv.Type().Bits()-1)
			maxIntExclusive := math.Ldexp(1, fv.Type().Bits()-1)
			if f64 < minInt || f64 >= maxIntExclusive {
				return true, fmt.Errorf("value %v overflows int field (%v)", f64, fv.Type())
			}

			i64 := int64(f64)
			if fv.OverflowInt(i64) {
				return true, fmt.Errorf("value %v overflows int field (%v)", i64, fv.Type())
			}

			fv.SetInt(i64)
			return true, nil
		}

		if f64 < 0 {
			return true, fmt.Errorf("cannot assign float %v to uint field", f64)
		}

		maxUintExclusive := math.Ldexp(1, fv.Type().Bits())
		if f64 >= maxUintExclusive {
			return true, fmt.Errorf("value %v overflows uint field (%v)", f64, fv.Type())
		}

		u64 := uint64(f64)
		if fv.OverflowUint(u64) {
			return true, fmt.Errorf("value %v overflows uint field (%v)", u64, fv.Type())
		}

		fv.SetUint(u64)
		return true, nil

	case isReflectIntKind(srcKind):
		i64 := sv.Int()

		if isReflectFloatKind(dstKind) {
			f64 := float64(i64)
			if fv.OverflowFloat(f64) {
				return true, fmt.Errorf("value %v overflows float field (%v)", f64, fv.Type())
			}

			fv.SetFloat(f64)
			return true, nil
		}

		if isReflectIntKind(dstKind) {
			if fv.OverflowInt(i64) {
				return true, fmt.Errorf("value %v overflows int field (%v)", i64, fv.Type())
			}

			fv.SetInt(i64)
			return true, nil
		}

		if i64 < 0 {
			return true, fmt.Errorf("cannot assign negative int %v to uint field", i64)
		}

		u64 := uint64(i64)
		if fv.OverflowUint(u64) {
			return true, fmt.Errorf("value %v overflows uint field (%v)", u64, fv.Type())
		}

		fv.SetUint(u64)
		return true, nil

	default:
		u64 := sv.Uint()

		if isReflectFloatKind(dstKind) {
			f64 := float64(u64)
			if fv.OverflowFloat(f64) {
				return true, fmt.Errorf("value %v overflows float field (%v)", f64, fv.Type())
			}

			fv.SetFloat(f64)
			return true, nil
		}

		if isReflectUintKind(dstKind) {
			if fv.OverflowUint(u64) {
				return true, fmt.Errorf("value %v overflows uint field (%v)", u64, fv.Type())
			}

			fv.SetUint(u64)
			return true, nil
		}

		if u64 > math.MaxInt64 {
			return true, fmt.Errorf("value %v overflows int field (%v)", u64, fv.Type())
		}

		i64 := int64(u64)
		if fv.OverflowInt(i64) {
			return true, fmt.Errorf("value %v overflows int field (%v)", i64, fv.Type())
		}

		fv.SetInt(i64)
		return true, nil
	}
}

func isReflectNumericKind(kind reflect.Kind) bool {
	return isReflectIntKind(kind) || isReflectUintKind(kind) || isReflectFloatKind(kind)
}

func isReflectIntKind(kind reflect.Kind) bool {
	return kind >= reflect.Int && kind <= reflect.Int64
}

func isReflectUintKind(kind reflect.Kind) bool {
	return kind >= reflect.Uint && kind <= reflect.Uint64
}

func isReflectFloatKind(kind reflect.Kind) bool {
	return kind == reflect.Float32 || kind == reflect.Float64
}

func (db *DB[K, V]) populatePatcher(t reflect.Type, idx []int) error {
	for i := 0; i < t.NumField(); i++ {

		rf := t.Field(i)

		if !rf.IsExported() {
			continue
		}

		if rf.Anonymous {
			if rf.Type.Kind() == reflect.Struct {
				nidx := append(slices.Clone(idx), i)
				if err := db.populatePatcher(rf.Type, nidx); err != nil {
					return err
				}
			}
			continue
		}

		f := &field{
			Name:     rf.Name,
			DBName:   rf.Name,
			JSONName: rf.Name,
			Kind:     rf.Type.Kind(),
			Index:    append(slices.Clone(idx), i),
		}

		f.UseVI = rf.Type.Implements(viType)

		if f.Kind == reflect.Slice && !f.UseVI {
			f.Slice = true
			elem := rf.Type.Elem()
			f.Kind = elem.Kind()
			f.UseVI = elem.Implements(viType)
		}

		if f.Kind == reflect.Pointer {
			f.Ptr = true
			elem := rf.Type.Elem()
			f.Kind = elem.Kind()
			f.UseVI = elem.Implements(viType)
		}

		db.patchMap[rf.Name] = f

		if dbTag := rf.Tag.Get("db"); dbTag != "" && dbTag != "-" {
			f.DBName = dbTag
			if existing, ok := db.patchMap[dbTag]; ok && existing.Name != f.Name {
				return fmt.Errorf("ambiguous db tag '%v' used by fields %v and %v", dbTag, existing.Name, f.Name)
			}
			db.patchMap[dbTag] = f
		}

		if jsonTag := rf.Tag.Get("json"); jsonTag != "" && jsonTag != "-" {
			tagParts := strings.Split(jsonTag, ",")
			jsonName := tagParts[0]
			if jsonName == "" {
				continue
			}
			f.JSONName = jsonName
			if existing, ok := db.patchMap[jsonName]; ok && existing.Name != f.Name {
				return fmt.Errorf("ambiguous json tag '%v' used by fields %v and %v", jsonName, existing.Name, f.Name)
			}
			db.patchMap[jsonName] = f
		}
	}
	return nil
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

	visited := make(map[uintptr]reflect.Value)
	clone := deepCopy(origin, visited)
	return clone.Interface()
}

func deepCopy(origin reflect.Value, visited map[uintptr]reflect.Value) reflect.Value {
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

	switch kind {
	case reflect.Ptr, reflect.Map, reflect.Slice:
		addr := origin.Pointer()
		if clone, ok := visited[addr]; ok {
			return clone
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
		s := reflect.New(origin.Type()).Elem()
		s.Set(origin)
		for i := 0; i < origin.NumField(); i++ {
			sf := s.Field(i)
			if !sf.CanSet() {
				continue
			}
			clone := deepCopy(origin.Field(i), visited)
			sf.Set(clone)
		}
		return s

	case reflect.Ptr:
		ptr := reflect.New(origin.Elem().Type())
		visited[origin.Pointer()] = ptr
		clone := deepCopy(origin.Elem(), visited)
		ptr.Elem().Set(clone)
		return ptr

	case reflect.Slice:
		s := reflect.MakeSlice(origin.Type(), origin.Len(), origin.Cap())
		visited[origin.Pointer()] = s
		for i := 0; i < origin.Len(); i++ {
			clone := deepCopy(origin.Index(i), visited)
			s.Index(i).Set(clone)
		}
		return s

	case reflect.Map:
		m := reflect.MakeMap(origin.Type())
		visited[origin.Pointer()] = m
		for _, key := range origin.MapKeys() {
			keyClone := deepCopy(key, visited)
			valClone := deepCopy(origin.MapIndex(key), visited)
			m.SetMapIndex(keyClone, valClone)
		}
		return m

	case reflect.Array:
		a := reflect.New(origin.Type()).Elem()
		for i := 0; i < origin.Len(); i++ {
			clone := deepCopy(origin.Index(i), visited)
			a.Index(i).Set(clone)
		}
		return a

	case reflect.Interface:
		clone := deepCopy(origin.Elem(), visited)
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
