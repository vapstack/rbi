package schema

import (
	"fmt"
	"math"
	"reflect"
	"unsafe"
)

func (patch *PatchRuntime) Apply(ptr unsafe.Pointer, items []PatchItem, ignoreUnknown bool) error {
	rv := reflect.NewAt(patch.typ, ptr).Elem()

	for _, p := range items {
		f, ok := patch.Fields[p.Name]
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
			if isReflectNillableKind(fv.Kind()) {
				if !fv.IsNil() {
					fv.Set(reflect.Zero(fv.Type()))
				}
				continue
			}
			return fmt.Errorf("field %v: cannot assign nil to non-nillable field", p.Name)
		}

		if err := setReflectValue(fv, p.Value); err != nil {
			return fmt.Errorf("field %v: %w", p.Name, err)
		}
	}
	return nil
}

func (patch *PatchRuntime) ApplyCopied(ptr unsafe.Pointer, items []PatchItem, ignoreUnknown bool) error {
	rv := reflect.NewAt(patch.typ, ptr).Elem()

	for _, p := range items {
		acc, ok := patch.AccessByName[p.Name]
		if !ok {
			if ignoreUnknown {
				continue
			}
			return fmt.Errorf("cannot patch field %v: field information is missing", p.Name)
		}

		fv := rv.FieldByIndex(acc.Field.Index)
		if !fv.CanSet() {
			continue
		}

		if p.Value == nil {
			if isReflectNillableKind(fv.Kind()) {
				if !fv.IsNil() {
					fv.Set(reflect.Zero(fv.Type()))
				}
				continue
			}
			return fmt.Errorf("field %v: cannot assign nil to non-nillable field", p.Name)
		}

		sv := reflect.ValueOf(p.Value)
		if !sv.Type().AssignableTo(acc.Type) {
			return fmt.Errorf("field %v: copied value type %v is not assignable to %v", p.Name, sv.Type(), acc.Type)
		}
		fv.Set(sv)
	}
	return nil
}

func (patch *PatchRuntime) CopyItemValue(name string, value any, ignoreUnknown bool) (any, bool, error) {
	acc, ok := patch.AccessByName[name]
	if !ok {
		if ignoreUnknown {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("cannot patch field %v: field information is missing", name)
	}

	if value == nil {
		if !isReflectNillableKind(acc.Type.Kind()) {
			return nil, true, fmt.Errorf("field %v: cannot assign nil to non-nillable field", name)
		}
		return nil, true, nil
	}

	if sv := reflect.ValueOf(value); sv.Type().AssignableTo(acc.Type) {
		switch acc.Type.Kind() {
		case reflect.Bool,
			reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Float32, reflect.Float64,
			reflect.String:
			return value, true, nil

		case reflect.Struct:
			if isNativeTimeScalarType(acc.Type) {
				return value, true, nil
			}
		}
		fv := reflect.New(acc.Type).Elem()
		fv.Set(sv)
		return acc.CopyFieldValue(unsafe.Pointer(fv.UnsafeAddr())), true, nil
	}

	fv := reflect.New(acc.Type).Elem()
	if err := setReflectValue(fv, value); err != nil {
		return nil, true, fmt.Errorf("field %v: %w", name, err)
	}
	return acc.CopyConvertedValue(unsafe.Pointer(fv.UnsafeAddr())), true, nil
}

func (patch *PatchRuntime) ValidateNames(items []PatchItem) error {
	for _, p := range items {
		if _, ok := patch.Fields[p.Name]; !ok {
			return fmt.Errorf("cannot patch field %v: field information is missing", p.Name)
		}
	}
	return nil
}

func setReflectValue(fv reflect.Value, val any) error {
	sv := reflect.ValueOf(val)
	if !sv.IsValid() {
		return nil
	}

	if fv.Kind() == reflect.Interface {
		if sv.Type().AssignableTo(fv.Type()) {
			fv.Set(sv)
			return nil
		}
		if sv.Type().ConvertibleTo(fv.Type()) {
			fv.Set(sv.Convert(fv.Type()))
			return nil
		}
		return fmt.Errorf("type mismatch: cannot convert %v to %v", sv.Type(), fv.Type())
	}

	if fv.Kind() == reflect.Slice {
		if sv.Kind() != reflect.Slice {
			return fmt.Errorf("source is not a slice, but destination is")
		}
		if sv.IsNil() {
			fv.Set(reflect.Zero(fv.Type()))
			return nil
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
		if sv.Kind() == reflect.Pointer {
			if sv.IsNil() {
				fv.Set(reflect.Zero(fv.Type()))
				return nil
			}
			if sv.Type().AssignableTo(fv.Type()) {
				ptr := reflect.New(fv.Type().Elem())
				if err := setReflectValue(ptr.Elem(), sv.Elem().Interface()); err != nil {
					return err
				}
				fv.Set(ptr)
				return nil
			}
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

	if dstKind == reflect.String && isReflectNumericKind(srcKind) {
		return fmt.Errorf("type mismatch: cannot convert %v to %v", sv.Type(), fv.Type())
	}

	if sv.Type().ConvertibleTo(fv.Type()) {
		fv.Set(sv.Convert(fv.Type()))
		return nil
	}

	return fmt.Errorf("type mismatch: cannot convert %v to %v", sv.Type(), fv.Type())
}

func isReflectNillableKind(kind reflect.Kind) bool {
	switch kind {
	case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Interface, reflect.Func, reflect.Chan:
		return true
	default:
		return false
	}
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
	return kind >= reflect.Uint && kind <= reflect.Uint64 || kind == reflect.Uintptr
}

func isReflectFloatKind(kind reflect.Kind) bool {
	return kind == reflect.Float32 || kind == reflect.Float64
}
