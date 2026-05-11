package rbi

import (
	"fmt"
	"math"
	"reflect"
	"unsafe"

	"github.com/vapstack/rbi/internal/schema"
)

// IndexKind declares how a struct field participates in RBI secondary storage.
type IndexKind uint8

const (
	IndexDefault IndexKind = iota
	IndexUnique
	IndexMeasure
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
	if db.engine == nil {
		return
	}
	db.forEachModifiedAccessor(db.engine.schema.Indexed, v1, v2, fn)
}

func (db *DB[K, V]) getModifiedIndexedFields(v1 *V, v2 *V) []string {
	var modified []string
	db.forEachModifiedIndexedField(v1, v2, func(acc schema.IndexedFieldAccessor) bool {
		modified = append(modified, acc.Name)
		return true
	})
	return modified
}

func (db *DB[K, V]) applyPatch(v *V, patch []Field, ignoreUnknown bool) error {
	rv := reflect.ValueOf(v).Elem()

	for _, p := range patch {
		f, ok := db.schema.Patch.Fields[p.Name]
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

		if err := setReflectValue(fv, p.Value); err != nil {
			return fmt.Errorf("field %v: %w", p.Name, err)
		}
	}
	return nil
}

func setReflectValue(fv reflect.Value, val any) error {
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
			if err := setReflectValue(rsv, src); err != nil {
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
		if err := setReflectValue(ptr.Elem(), val); err != nil {
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
