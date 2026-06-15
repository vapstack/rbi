package schema

import (
	"fmt"
	"math"
	"reflect"
	"slices"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
)

type MeasureValueKind uint8

const (
	MeasureValueSigned MeasureValueKind = iota
	MeasureValueUnsigned
	MeasureValueFloat
)

type MeasureReadFn func(ptr unsafe.Pointer) (uint64, bool)

func makeMeasureFieldAccessors(vtype reflect.Type, fields map[string]*Field) ([]MeasureFieldAccessor, MeasureFieldMap, error) {
	if len(fields) == 0 {
		return nil, nil, nil
	}

	access := make([]MeasureFieldAccessor, 0, len(fields))
	fieldMap := make(map[string]MeasureFieldAccessor, len(fields))

	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	slices.Sort(names)

	for _, name := range names {
		f := fields[name]
		acc, err := makeMeasureFieldAccessor(vtype, f)
		if err != nil {
			return nil, nil, err
		}
		acc.Ordinal = len(access)
		access = append(access, acc)
		fieldMap[f.DBName] = acc
	}
	return access, fieldMap, nil
}

func makeMeasureFieldAccessor(vtype reflect.Type, f *Field) (MeasureFieldAccessor, error) {
	acc := MeasureFieldAccessor{
		Name:  f.DBName,
		Field: f,
	}

	fieldType, offset := resolveFieldTypeAndOffset(vtype, f.Index)
	kind, read, modified, err := buildMeasureAccessorFns(f, fieldType, offset)
	if err != nil {
		return MeasureFieldAccessor{}, err
	}
	acc.Kind = kind
	acc.Read = read
	acc.Modified = modified
	return acc, nil
}

func buildMeasureAccessorFns(f *Field, fieldType reflect.Type, offset uintptr) (MeasureValueKind, MeasureReadFn, FieldModifiedFn, error) {
	if f.Ptr {
		fieldType = fieldType.Elem()
	}
	switch f.Kind {
	case reflect.Int:
		return MeasureValueSigned, measurePtrOrScalarIntFns[int](offset, f.Ptr), measureScalarModified[int](offset, f.Ptr), nil
	case reflect.Int8:
		return MeasureValueSigned, measurePtrOrScalarIntFns[int8](offset, f.Ptr), measureScalarModified[int8](offset, f.Ptr), nil
	case reflect.Int16:
		return MeasureValueSigned, measurePtrOrScalarIntFns[int16](offset, f.Ptr), measureScalarModified[int16](offset, f.Ptr), nil
	case reflect.Int32:
		return MeasureValueSigned, measurePtrOrScalarIntFns[int32](offset, f.Ptr), measureScalarModified[int32](offset, f.Ptr), nil
	case reflect.Int64:
		return MeasureValueSigned, measurePtrOrScalarIntFns[int64](offset, f.Ptr), measureScalarModified[int64](offset, f.Ptr), nil
	case reflect.Uint:
		return MeasureValueUnsigned, measurePtrOrScalarUintFns[uint](offset, f.Ptr), measureScalarModified[uint](offset, f.Ptr), nil
	case reflect.Uint8:
		return MeasureValueUnsigned, measurePtrOrScalarUintFns[uint8](offset, f.Ptr), measureScalarModified[uint8](offset, f.Ptr), nil
	case reflect.Uint16:
		return MeasureValueUnsigned, measurePtrOrScalarUintFns[uint16](offset, f.Ptr), measureScalarModified[uint16](offset, f.Ptr), nil
	case reflect.Uint32:
		return MeasureValueUnsigned, measurePtrOrScalarUintFns[uint32](offset, f.Ptr), measureScalarModified[uint32](offset, f.Ptr), nil
	case reflect.Uint64:
		return MeasureValueUnsigned, measurePtrOrScalarUintFns[uint64](offset, f.Ptr), measureScalarModified[uint64](offset, f.Ptr), nil
	case reflect.Float32:
		return MeasureValueFloat, measurePtrOrScalarFloatFns[float32](offset, f.Ptr), measureFloatModified[float32](offset, f.Ptr), nil
	case reflect.Float64:
		return MeasureValueFloat, measurePtrOrScalarFloatFns[float64](offset, f.Ptr), measureFloatModified[float64](offset, f.Ptr), nil
	default:
		return 0, nil, nil, fmt.Errorf("unsupported measure field kind %v for %v", f.Kind, fieldType)
	}
}

func measureScalarModified[T comparable](offset uintptr, ptr bool) FieldModifiedFn {
	if ptr {
		return func(v1, v2 unsafe.Pointer) bool {
			p1 := ptrFieldValue[T](v1, offset)
			p2 := ptrFieldValue[T](v2, offset)
			if p1 == nil || p2 == nil {
				return p1 != p2
			}
			return *p1 != *p2
		}
	}
	return func(v1, v2 unsafe.Pointer) bool {
		return scalarFieldValue[T](v1, offset) != scalarFieldValue[T](v2, offset)
	}
}

func measureFloatModified[T floatFieldValue](offset uintptr, ptr bool) FieldModifiedFn {
	if ptr {
		return func(v1, v2 unsafe.Pointer) bool {
			p1 := ptrFieldValue[T](v1, offset)
			p2 := ptrFieldValue[T](v2, offset)
			if p1 == nil || p2 == nil {
				return p1 != p2
			}
			return !floatsEqualForIndex(*p1, *p2)
		}
	}
	return func(v1, v2 unsafe.Pointer) bool {
		f1 := scalarFieldValue[T](v1, offset)
		f2 := scalarFieldValue[T](v2, offset)
		return !floatsEqualForIndex(f1, f2)
	}
}

func measurePtrOrScalarIntFns[T signedFieldValue](offset uintptr, ptr bool) MeasureReadFn {
	if ptr {
		return func(root unsafe.Pointer) (uint64, bool) {
			v := ptrFieldValue[T](root, offset)
			if v == nil {
				return 0, false
			}
			return uint64(int64(*v)), true
		}
	}
	return func(root unsafe.Pointer) (uint64, bool) {
		return uint64(int64(scalarFieldValue[T](root, offset))), true
	}
}

func measurePtrOrScalarUintFns[T unsignedFieldValue](offset uintptr, ptr bool) MeasureReadFn {
	if ptr {
		return func(root unsafe.Pointer) (uint64, bool) {
			v := ptrFieldValue[T](root, offset)
			if v == nil {
				return 0, false
			}
			return uint64(*v), true
		}
	}
	return func(root unsafe.Pointer) (uint64, bool) {
		return uint64(scalarFieldValue[T](root, offset)), true
	}
}

func measurePtrOrScalarFloatFns[T floatFieldValue](offset uintptr, ptr bool) MeasureReadFn {
	if ptr {
		return func(root unsafe.Pointer) (uint64, bool) {
			v := ptrFieldValue[T](root, offset)
			if v == nil {
				return 0, false
			}
			return math.Float64bits(keycodec.CanonicalizeFloat64ForIndex(float64(*v))), true
		}
	}
	return func(root unsafe.Pointer) (uint64, bool) {
		return math.Float64bits(keycodec.CanonicalizeFloat64ForIndex(float64(scalarFieldValue[T](root, offset)))), true
	}
}
