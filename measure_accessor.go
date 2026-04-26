package rbi

import (
	"fmt"
	"math"
	"reflect"
	"slices"
	"unsafe"
)

type measureValueKind uint8

const (
	measureValueSigned measureValueKind = iota
	measureValueUnsigned
	measureValueFloat
)

type measureReadFn func(ptr unsafe.Pointer) (uint64, bool)

type measureFieldAccessor struct {
	ordinal int
	name    string
	field   *field
	kind    measureValueKind

	read     measureReadFn
	modified fieldModifiedFn
}

func (db *DB[K, V]) initMeasureFieldAccessors() error {
	if len(db.measureFields) == 0 {
		db.measureFieldAccess = nil
		db.measureFieldByName = nil
		return nil
	}

	db.measureFieldAccess = make([]measureFieldAccessor, 0, len(db.measureFields))
	db.measureFieldByName = make(map[string]measureFieldAccessor, len(db.measureFields))

	names := make([]string, 0, len(db.measureFields))
	for name := range db.measureFields {
		names = append(names, name)
	}
	slices.Sort(names)

	for _, name := range names {
		f := db.measureFields[name]
		acc, err := db.makeMeasureFieldAccessor(f)
		if err != nil {
			return err
		}
		acc.ordinal = len(db.measureFieldAccess)
		db.measureFieldAccess = append(db.measureFieldAccess, acc)
		db.measureFieldByName[f.DBName] = acc
	}
	return nil
}

func (db *DB[K, V]) makeMeasureFieldAccessor(f *field) (measureFieldAccessor, error) {
	acc := measureFieldAccessor{
		name:  f.DBName,
		field: f,
	}

	fieldType, offset := resolveFieldTypeAndOffset(db.vtype, f.Index)
	kind, read, modified, err := buildMeasureAccessorFns(f, fieldType, offset)
	if err != nil {
		return measureFieldAccessor{}, err
	}
	acc.kind = kind
	acc.read = read
	acc.modified = modified
	return acc, nil
}

func buildMeasureAccessorFns(f *field, fieldType reflect.Type, offset uintptr) (measureValueKind, measureReadFn, fieldModifiedFn, error) {
	if f.Ptr {
		fieldType = fieldType.Elem()
	}
	switch f.Kind {
	case reflect.Int:
		return measureValueSigned, measurePtrOrScalarIntFns[int](offset, f.Ptr), measureScalarModified[int](offset, f.Ptr), nil
	case reflect.Int8:
		return measureValueSigned, measurePtrOrScalarIntFns[int8](offset, f.Ptr), measureScalarModified[int8](offset, f.Ptr), nil
	case reflect.Int16:
		return measureValueSigned, measurePtrOrScalarIntFns[int16](offset, f.Ptr), measureScalarModified[int16](offset, f.Ptr), nil
	case reflect.Int32:
		return measureValueSigned, measurePtrOrScalarIntFns[int32](offset, f.Ptr), measureScalarModified[int32](offset, f.Ptr), nil
	case reflect.Int64:
		return measureValueSigned, measurePtrOrScalarIntFns[int64](offset, f.Ptr), measureScalarModified[int64](offset, f.Ptr), nil
	case reflect.Uint:
		return measureValueUnsigned, measurePtrOrScalarUintFns[uint](offset, f.Ptr), measureScalarModified[uint](offset, f.Ptr), nil
	case reflect.Uint8:
		return measureValueUnsigned, measurePtrOrScalarUintFns[uint8](offset, f.Ptr), measureScalarModified[uint8](offset, f.Ptr), nil
	case reflect.Uint16:
		return measureValueUnsigned, measurePtrOrScalarUintFns[uint16](offset, f.Ptr), measureScalarModified[uint16](offset, f.Ptr), nil
	case reflect.Uint32:
		return measureValueUnsigned, measurePtrOrScalarUintFns[uint32](offset, f.Ptr), measureScalarModified[uint32](offset, f.Ptr), nil
	case reflect.Uint64:
		return measureValueUnsigned, measurePtrOrScalarUintFns[uint64](offset, f.Ptr), measureScalarModified[uint64](offset, f.Ptr), nil
	case reflect.Float32:
		return measureValueFloat, measurePtrOrScalarFloatFns[float32](offset, f.Ptr), measureScalarModified[float32](offset, f.Ptr), nil
	case reflect.Float64:
		return measureValueFloat, measurePtrOrScalarFloatFns[float64](offset, f.Ptr), measureScalarModified[float64](offset, f.Ptr), nil
	default:
		return 0, nil, nil, fmt.Errorf("unsupported measure field kind %v for %v", f.Kind, fieldType)
	}
}

func measureScalarModified[T comparable](offset uintptr, ptr bool) fieldModifiedFn {
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

func measurePtrOrScalarIntFns[T signedFieldValue](offset uintptr, ptr bool) measureReadFn {
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

func measurePtrOrScalarUintFns[T unsignedFieldValue](offset uintptr, ptr bool) measureReadFn {
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

func measurePtrOrScalarFloatFns[T floatFieldValue](offset uintptr, ptr bool) measureReadFn {
	if ptr {
		return func(root unsafe.Pointer) (uint64, bool) {
			v := ptrFieldValue[T](root, offset)
			if v == nil {
				return 0, false
			}
			return math.Float64bits(float64(*v)), true
		}
	}
	return func(root unsafe.Pointer) (uint64, bool) {
		return math.Float64bits(float64(scalarFieldValue[T](root, offset))), true
	}
}
