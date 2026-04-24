package rbi

import (
	"fmt"
	"reflect"
	"slices"
	"time"
	"unsafe"
)

type fieldModifiedFn func(v1, v2 unsafe.Pointer) bool
type patchValueEqualFn func(v1, v2 unsafe.Pointer) bool
type patchValueCopyFn func(ptr unsafe.Pointer) any

type fieldAccessorBundle struct {
	unique       uniqueScalarGetterFn
	writeBuild   buildFieldWriteAccessorFn
	writeOverlay overlayFieldWriteAccessorFn
	writeInsert  insertFieldWriteAccessorFn
	writeScratch scratchFieldWriteAccessorFn
	modified     fieldModifiedFn
}

type unsafeSliceHeader struct {
	data unsafe.Pointer
	len  int
	cap  int
}

type signedFieldValue interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}

type unsignedFieldValue interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

type floatFieldValue interface {
	~float32 | ~float64
}

type stringValueSink interface {
	addString(string)
}

type fixedValueSink interface {
	addFixed(uint64)
}

type nilStringValueSink interface {
	setNil()
	addString(string)
}

type nilFixedValueSink interface {
	setNil()
	addFixed(uint64)
}

type lenStringValueSink interface {
	setLen(int)
	addString(string)
}

type lenFixedValueSink interface {
	setLen(int)
	addFixed(uint64)
}

func resolveFieldTypeAndOffset(root reflect.Type, index []int) (reflect.Type, uintptr) {
	cur := root
	var offset uintptr
	for _, idx := range index {
		sf := cur.Field(idx)
		offset += sf.Offset
		cur = sf.Type
	}
	return cur, offset
}

func scalarFieldValue[T any](ptr unsafe.Pointer, offset uintptr) T {
	return *(*T)(unsafe.Add(ptr, offset))
}

func ptrFieldValue[T any](ptr unsafe.Pointer, offset uintptr) *T {
	data := *(*unsafe.Pointer)(unsafe.Add(ptr, offset))
	if data == nil {
		return nil
	}
	return (*T)(data)
}

func sliceFieldValue[T any](ptr unsafe.Pointer, offset uintptr) []T {
	hdr := *(*unsafeSliceHeader)(unsafe.Add(ptr, offset))
	return unsafe.Slice((*T)(hdr.data), hdr.len)
}

func slicesEqualExact[T comparable](lhs, rhs []T) bool {
	if (lhs == nil) != (rhs == nil) {
		return false
	}
	if len(lhs) != len(rhs) {
		return false
	}
	for i := range lhs {
		if lhs[i] != rhs[i] {
			return false
		}
	}
	return true
}

func scalarPatchValueEqual[T comparable](offset uintptr, ptr bool) patchValueEqualFn {
	if ptr {
		return func(v1, v2 unsafe.Pointer) bool {
			p1 := ptrFieldValue[T](v1, offset)
			p2 := ptrFieldValue[T](v2, offset)
			if p1 == nil || p2 == nil {
				return p1 == p2
			}
			return *p1 == *p2
		}
	}
	return func(v1, v2 unsafe.Pointer) bool {
		return scalarFieldValue[T](v1, offset) == scalarFieldValue[T](v2, offset)
	}
}

func slicePatchValueEqual[T comparable](offset uintptr) patchValueEqualFn {
	return func(v1, v2 unsafe.Pointer) bool {
		return slicesEqualExact(sliceFieldValue[T](v1, offset), sliceFieldValue[T](v2, offset))
	}
}

func reflectSlicePatchValueCopy(fieldType reflect.Type, offset uintptr) patchValueCopyFn {
	return func(root unsafe.Pointer) any {
		src := reflect.NewAt(fieldType, unsafe.Add(root, offset)).Elem()
		if src.IsNil() {
			return src.Interface()
		}
		dst := reflect.MakeSlice(fieldType, src.Len(), src.Len())
		reflect.Copy(dst, src)
		return dst.Interface()
	}
}

func typeHasMutableReferencePayload(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return true
	case reflect.Array:
		return typeHasMutableReferencePayload(t.Elem())
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if typeHasMutableReferencePayload(t.Field(i).Type) {
				return true
			}
		}
	}
	return false
}

func addDistinctFixedKeys(n int, keyAt func(int) uint64, add func(uint64)) int {
	if n == 0 {
		return 0
	}
	if n == 1 {
		add(keyAt(0))
		return 1
	}
	seen := newU64Set(n)
	defer releaseU64Set(&seen)
	distinct := 0
	for i := 0; i < n; i++ {
		cur := keyAt(i)
		if !seen.Add(cur) {
			continue
		}
		distinct++
		add(cur)
	}
	return distinct
}

func addDistinctStringsToSink[S stringValueSink](vals []string, sink S) int {
	if len(vals) == 0 {
		return 0
	}
	if len(vals) == 1 {
		sink.addString(vals[0])
		return 1
	}
	seen := stringSetPool.Get(len(vals))
	defer stringSetPool.Put(seen)
	distinct := 0
	for i := range vals {
		cur := vals[i]
		if _, ok := seen[cur]; ok {
			continue
		}
		seen[cur] = struct{}{}
		distinct++
		sink.addString(cur)
	}
	return distinct
}

func addDistinctValueIndexerStringsToSink[S stringValueSink](vals reflect.Value, sink S) int {
	n := vals.Len()
	if n == 0 {
		return 0
	}
	if n == 1 {
		sink.addString(vals.Index(0).Interface().(ValueIndexer).IndexingValue())
		return 1
	}
	seen := stringSetPool.Get(n)
	defer stringSetPool.Put(seen)
	distinct := 0
	for i := 0; i < n; i++ {
		cur := vals.Index(i).Interface().(ValueIndexer).IndexingValue()
		if _, ok := seen[cur]; ok {
			continue
		}
		seen[cur] = struct{}{}
		distinct++
		sink.addString(cur)
	}
	return distinct
}

func addDistinctSignedFixedKeysToSink[S fixedValueSink, T signedFieldValue](vals []T, sink S) int {
	if len(vals) == 0 {
		return 0
	}
	if len(vals) == 1 {
		sink.addFixed(buildInt64Key(int64(vals[0])))
		return 1
	}
	seen := newU64Set(len(vals))
	defer releaseU64Set(&seen)
	distinct := 0
	for i := range vals {
		cur := buildInt64Key(int64(vals[i]))
		if !seen.Add(cur) {
			continue
		}
		distinct++
		sink.addFixed(cur)
	}
	return distinct
}

func addDistinctUnsignedFixedKeysToSink[S fixedValueSink, T unsignedFieldValue](vals []T, sink S) int {
	if len(vals) == 0 {
		return 0
	}
	if len(vals) == 1 {
		sink.addFixed(uint64(vals[0]))
		return 1
	}
	seen := newU64Set(len(vals))
	defer releaseU64Set(&seen)
	distinct := 0
	for i := range vals {
		cur := uint64(vals[i])
		if !seen.Add(cur) {
			continue
		}
		distinct++
		sink.addFixed(cur)
	}
	return distinct
}

func addDistinctFloatFixedKeysToSink[S fixedValueSink, T floatFieldValue](vals []T, sink S) int {
	if len(vals) == 0 {
		return 0
	}
	if len(vals) == 1 {
		sink.addFixed(buildFloat64Key(float64(vals[0])))
		return 1
	}
	seen := newU64Set(len(vals))
	defer releaseU64Set(&seen)
	distinct := 0
	for i := range vals {
		cur := buildFloat64Key(float64(vals[i]))
		if !seen.Add(cur) {
			continue
		}
		distinct++
		sink.addFixed(cur)
	}
	return distinct
}

func addDistinctBoolValuesToSink[S stringValueSink](vals []bool, sink S) int {
	if len(vals) == 0 {
		return 0
	}
	seenFalse := false
	seenTrue := false
	distinct := 0
	for i := range vals {
		if vals[i] {
			if seenTrue {
				continue
			}
			seenTrue = true
			distinct++
			sink.addString("1")
			continue
		}
		if seenFalse {
			continue
		}
		seenFalse = true
		distinct++
		sink.addString("0")
	}
	return distinct
}

func writePtrStringField[S nilStringValueSink](ptr unsafe.Pointer, sink S, offset uintptr) {
	if ptr == nil {
		return
	}
	v := ptrFieldValue[string](ptr, offset)
	if v == nil {
		sink.setNil()
		return
	}
	sink.addString(*v)
}

func writeScalarStringField[S stringValueSink](ptr unsafe.Pointer, sink S, offset uintptr) {
	if ptr == nil {
		return
	}
	sink.addString(scalarFieldValue[string](ptr, offset))
}

func writePtrBoolField[S nilStringValueSink](ptr unsafe.Pointer, sink S, offset uintptr) {
	if ptr == nil {
		return
	}
	v := ptrFieldValue[bool](ptr, offset)
	if v == nil {
		sink.setNil()
		return
	}
	if *v {
		sink.addString("1")
		return
	}
	sink.addString("0")
}

func writeScalarBoolField[S stringValueSink](ptr unsafe.Pointer, sink S, offset uintptr) {
	if ptr == nil {
		return
	}
	if scalarFieldValue[bool](ptr, offset) {
		sink.addString("1")
		return
	}
	sink.addString("0")
}

func writePtrIntField[S nilFixedValueSink, T signedFieldValue](ptr unsafe.Pointer, sink S, offset uintptr) {
	if ptr == nil {
		return
	}
	v := ptrFieldValue[T](ptr, offset)
	if v == nil {
		sink.setNil()
		return
	}
	sink.addFixed(buildInt64Key(int64(*v)))
}

func writeScalarIntField[S fixedValueSink, T signedFieldValue](ptr unsafe.Pointer, sink S, offset uintptr) {
	if ptr == nil {
		return
	}
	sink.addFixed(buildInt64Key(int64(scalarFieldValue[T](ptr, offset))))
}

func writePtrUintField[S nilFixedValueSink, T unsignedFieldValue](ptr unsafe.Pointer, sink S, offset uintptr) {
	if ptr == nil {
		return
	}
	v := ptrFieldValue[T](ptr, offset)
	if v == nil {
		sink.setNil()
		return
	}
	sink.addFixed(uint64(*v))
}

func writeScalarUintField[S fixedValueSink, T unsignedFieldValue](ptr unsafe.Pointer, sink S, offset uintptr) {
	if ptr == nil {
		return
	}
	sink.addFixed(uint64(scalarFieldValue[T](ptr, offset)))
}

func writePtrFloatField[S nilFixedValueSink, T floatFieldValue](ptr unsafe.Pointer, sink S, offset uintptr) {
	if ptr == nil {
		return
	}
	v := ptrFieldValue[T](ptr, offset)
	if v == nil {
		sink.setNil()
		return
	}
	sink.addFixed(buildFloat64Key(float64(*v)))
}

func writeScalarFloatField[S fixedValueSink, T floatFieldValue](ptr unsafe.Pointer, sink S, offset uintptr) {
	if ptr == nil {
		return
	}
	sink.addFixed(buildFloat64Key(float64(scalarFieldValue[T](ptr, offset))))
}

func writeStringSliceField[S lenStringValueSink](ptr unsafe.Pointer, sink S, offset uintptr) {
	if ptr == nil {
		return
	}
	sink.setLen(addDistinctStringsToSink(sliceFieldValue[string](ptr, offset), sink))
}

func writeBoolSliceField[S lenStringValueSink](ptr unsafe.Pointer, sink S, offset uintptr) {
	if ptr == nil {
		return
	}
	sink.setLen(addDistinctBoolValuesToSink(sliceFieldValue[bool](ptr, offset), sink))
}

func writeIntSliceField[S lenFixedValueSink, T signedFieldValue](ptr unsafe.Pointer, sink S, offset uintptr) {
	if ptr == nil {
		return
	}
	sink.setLen(addDistinctSignedFixedKeysToSink(sliceFieldValue[T](ptr, offset), sink))
}

func writeUintSliceField[S lenFixedValueSink, T unsignedFieldValue](ptr unsafe.Pointer, sink S, offset uintptr) {
	if ptr == nil {
		return
	}
	sink.setLen(addDistinctUnsignedFixedKeysToSink(sliceFieldValue[T](ptr, offset), sink))
}

func writeFloatSliceField[S lenFixedValueSink, T floatFieldValue](ptr unsafe.Pointer, sink S, offset uintptr) {
	if ptr == nil {
		return
	}
	sink.setLen(addDistinctFloatFixedKeysToSink(sliceFieldValue[T](ptr, offset), sink))
}

func writePtrTimeField[S nilFixedValueSink](ptr unsafe.Pointer, sink S, offset uintptr) {
	if ptr == nil {
		return
	}
	v := ptrFieldValue[time.Time](ptr, offset)
	if v == nil {
		sink.setNil()
		return
	}
	sink.addFixed(buildInt64Key(v.Unix()))
}

func writeScalarTimeField[S fixedValueSink](ptr unsafe.Pointer, sink S, offset uintptr) {
	if ptr == nil {
		return
	}
	sink.addFixed(buildInt64Key(scalarFieldValue[time.Time](ptr, offset).Unix()))
}

func slicesModified[T comparable](lhs, rhs []T) bool {
	if len(lhs) != len(rhs) {
		return true
	}
	for i := range lhs {
		if lhs[i] != rhs[i] {
			return true
		}
	}
	return false
}

func valueIndexerScalarReflectAccessorBundle(fieldType reflect.Type, offset uintptr) fieldAccessorBundle {
	return fieldAccessorBundle{
		unique: func(ptr unsafe.Pointer) (string, bool, bool) {
			if ptr == nil {
				return "", false, false
			}
			fv := reflect.NewAt(fieldType, unsafe.Add(ptr, offset)).Elem()
			return fv.Interface().(ValueIndexer).IndexingValue(), true, false
		},
		writeBuild: func(ptr unsafe.Pointer, sink buildFieldWriteSink) {
			if ptr == nil {
				return
			}
			fv := reflect.NewAt(fieldType, unsafe.Add(ptr, offset)).Elem()
			sink.addString(fv.Interface().(ValueIndexer).IndexingValue())
		},
		writeOverlay: func(ptr unsafe.Pointer, sink snapshotOverlayWriteSink) {
			if ptr == nil {
				return
			}
			fv := reflect.NewAt(fieldType, unsafe.Add(ptr, offset)).Elem()
			sink.addString(fv.Interface().(ValueIndexer).IndexingValue())
		},
		writeInsert: func(ptr unsafe.Pointer, sink snapshotInsertWriteSink) {
			if ptr == nil {
				return
			}
			fv := reflect.NewAt(fieldType, unsafe.Add(ptr, offset)).Elem()
			sink.addString(fv.Interface().(ValueIndexer).IndexingValue())
		},
		writeScratch: func(ptr unsafe.Pointer, sink *fieldWriteScratch) {
			if ptr == nil {
				return
			}
			fv := reflect.NewAt(fieldType, unsafe.Add(ptr, offset)).Elem()
			sink.addString(fv.Interface().(ValueIndexer).IndexingValue())
		},
		modified: func(v1, v2 unsafe.Pointer) bool {
			fv1 := reflect.NewAt(fieldType, unsafe.Add(v1, offset)).Elem()
			fv2 := reflect.NewAt(fieldType, unsafe.Add(v2, offset)).Elem()
			return fv1.Interface().(ValueIndexer).IndexingValue() != fv2.Interface().(ValueIndexer).IndexingValue()
		},
	}
}

func valueIndexerSliceReflectAccessorBundle(sliceType reflect.Type, offset uintptr) fieldAccessorBundle {
	return fieldAccessorBundle{
		writeBuild: func(ptr unsafe.Pointer, sink buildFieldWriteSink) {
			if ptr == nil {
				return
			}
			fv := reflect.NewAt(sliceType, unsafe.Add(ptr, offset)).Elem()
			sink.setLen(addDistinctValueIndexerStringsToSink(fv, sink))
		},
		writeOverlay: func(ptr unsafe.Pointer, sink snapshotOverlayWriteSink) {
			if ptr == nil {
				return
			}
			fv := reflect.NewAt(sliceType, unsafe.Add(ptr, offset)).Elem()
			sink.setLen(addDistinctValueIndexerStringsToSink(fv, sink))
		},
		writeInsert: func(ptr unsafe.Pointer, sink snapshotInsertWriteSink) {
			if ptr == nil {
				return
			}
			fv := reflect.NewAt(sliceType, unsafe.Add(ptr, offset)).Elem()
			sink.setLen(addDistinctValueIndexerStringsToSink(fv, sink))
		},
		writeScratch: func(ptr unsafe.Pointer, sink *fieldWriteScratch) {
			if ptr == nil {
				return
			}
			fv := reflect.NewAt(sliceType, unsafe.Add(ptr, offset)).Elem()
			sink.setLen(addDistinctValueIndexerStringsToSink(fv, sink))
		},
		modified: func(v1, v2 unsafe.Pointer) bool {
			fv1 := reflect.NewAt(sliceType, unsafe.Add(v1, offset)).Elem()
			fv2 := reflect.NewAt(sliceType, unsafe.Add(v2, offset)).Elem()
			if fv1.Len() != fv2.Len() {
				return true
			}
			for i := 0; i < fv1.Len(); i++ {
				if fv1.Index(i).Interface().(ValueIndexer).IndexingValue() !=
					fv2.Index(i).Interface().(ValueIndexer).IndexingValue() {
					return true
				}
			}
			return false
		},
	}
}

func timeFieldAccessorBundle(offset uintptr, ptr bool) fieldAccessorBundle {
	if ptr {
		return fieldAccessorBundle{
			unique: func(ptr unsafe.Pointer) (string, bool, bool) {
				if ptr == nil {
					return "", false, false
				}
				v := ptrFieldValue[time.Time](ptr, offset)
				if v == nil {
					return "", true, true
				}
				return int64ByteStr(v.Unix()), true, false
			},
			writeBuild:   func(ptr unsafe.Pointer, sink buildFieldWriteSink) { writePtrTimeField(ptr, sink, offset) },
			writeOverlay: func(ptr unsafe.Pointer, sink snapshotOverlayWriteSink) { writePtrTimeField(ptr, sink, offset) },
			writeInsert:  func(ptr unsafe.Pointer, sink snapshotInsertWriteSink) { writePtrTimeField(ptr, sink, offset) },
			writeScratch: func(ptr unsafe.Pointer, sink *fieldWriteScratch) { writePtrTimeField(ptr, sink, offset) },
			modified: func(v1, v2 unsafe.Pointer) bool {
				p1 := ptrFieldValue[time.Time](v1, offset)
				p2 := ptrFieldValue[time.Time](v2, offset)
				if p1 == nil || p2 == nil {
					return p1 != p2
				}
				return p1.Unix() != p2.Unix()
			},
		}
	}
	return fieldAccessorBundle{
		unique: func(ptr unsafe.Pointer) (string, bool, bool) {
			if ptr == nil {
				return "", false, false
			}
			return int64ByteStr(scalarFieldValue[time.Time](ptr, offset).Unix()), true, false
		},
		writeBuild:   func(ptr unsafe.Pointer, sink buildFieldWriteSink) { writeScalarTimeField(ptr, sink, offset) },
		writeOverlay: func(ptr unsafe.Pointer, sink snapshotOverlayWriteSink) { writeScalarTimeField(ptr, sink, offset) },
		writeInsert:  func(ptr unsafe.Pointer, sink snapshotInsertWriteSink) { writeScalarTimeField(ptr, sink, offset) },
		writeScratch: func(ptr unsafe.Pointer, sink *fieldWriteScratch) { writeScalarTimeField(ptr, sink, offset) },
		modified: func(v1, v2 unsafe.Pointer) bool {
			return scalarFieldValue[time.Time](v1, offset).Unix() != scalarFieldValue[time.Time](v2, offset).Unix()
		},
	}
}

func stringFieldAccessorBundle(offset uintptr, ptr bool) fieldAccessorBundle {
	if ptr {
		return fieldAccessorBundle{
			unique: func(ptr unsafe.Pointer) (string, bool, bool) {
				if ptr == nil {
					return "", false, false
				}
				v := ptrFieldValue[string](ptr, offset)
				if v == nil {
					return "", true, true
				}
				return *v, true, false
			},
			writeBuild:   func(ptr unsafe.Pointer, sink buildFieldWriteSink) { writePtrStringField(ptr, sink, offset) },
			writeOverlay: func(ptr unsafe.Pointer, sink snapshotOverlayWriteSink) { writePtrStringField(ptr, sink, offset) },
			writeInsert:  func(ptr unsafe.Pointer, sink snapshotInsertWriteSink) { writePtrStringField(ptr, sink, offset) },
			writeScratch: func(ptr unsafe.Pointer, sink *fieldWriteScratch) { writePtrStringField(ptr, sink, offset) },
			modified: func(v1, v2 unsafe.Pointer) bool {
				p1 := ptrFieldValue[string](v1, offset)
				p2 := ptrFieldValue[string](v2, offset)
				if p1 == nil || p2 == nil {
					return p1 != p2
				}
				return *p1 != *p2
			},
		}
	}
	return fieldAccessorBundle{
		unique: func(ptr unsafe.Pointer) (string, bool, bool) {
			if ptr == nil {
				return "", false, false
			}
			return scalarFieldValue[string](ptr, offset), true, false
		},
		writeBuild:   func(ptr unsafe.Pointer, sink buildFieldWriteSink) { writeScalarStringField(ptr, sink, offset) },
		writeOverlay: func(ptr unsafe.Pointer, sink snapshotOverlayWriteSink) { writeScalarStringField(ptr, sink, offset) },
		writeInsert:  func(ptr unsafe.Pointer, sink snapshotInsertWriteSink) { writeScalarStringField(ptr, sink, offset) },
		writeScratch: func(ptr unsafe.Pointer, sink *fieldWriteScratch) { writeScalarStringField(ptr, sink, offset) },
		modified: func(v1, v2 unsafe.Pointer) bool {
			return scalarFieldValue[string](v1, offset) != scalarFieldValue[string](v2, offset)
		},
	}
}

func boolFieldAccessorBundle(offset uintptr, ptr bool) fieldAccessorBundle {
	if ptr {
		return fieldAccessorBundle{
			unique: func(ptr unsafe.Pointer) (string, bool, bool) {
				if ptr == nil {
					return "", false, false
				}
				v := ptrFieldValue[bool](ptr, offset)
				if v == nil {
					return "", true, true
				}
				if *v {
					return "1", true, false
				}
				return "0", true, false
			},
			writeBuild:   func(ptr unsafe.Pointer, sink buildFieldWriteSink) { writePtrBoolField(ptr, sink, offset) },
			writeOverlay: func(ptr unsafe.Pointer, sink snapshotOverlayWriteSink) { writePtrBoolField(ptr, sink, offset) },
			writeInsert:  func(ptr unsafe.Pointer, sink snapshotInsertWriteSink) { writePtrBoolField(ptr, sink, offset) },
			writeScratch: func(ptr unsafe.Pointer, sink *fieldWriteScratch) { writePtrBoolField(ptr, sink, offset) },
			modified: func(v1, v2 unsafe.Pointer) bool {
				p1 := ptrFieldValue[bool](v1, offset)
				p2 := ptrFieldValue[bool](v2, offset)
				if p1 == nil || p2 == nil {
					return p1 != p2
				}
				return *p1 != *p2
			},
		}
	}
	return fieldAccessorBundle{
		unique: func(ptr unsafe.Pointer) (string, bool, bool) {
			if ptr == nil {
				return "", false, false
			}
			if scalarFieldValue[bool](ptr, offset) {
				return "1", true, false
			}
			return "0", true, false
		},
		writeBuild:   func(ptr unsafe.Pointer, sink buildFieldWriteSink) { writeScalarBoolField(ptr, sink, offset) },
		writeOverlay: func(ptr unsafe.Pointer, sink snapshotOverlayWriteSink) { writeScalarBoolField(ptr, sink, offset) },
		writeInsert:  func(ptr unsafe.Pointer, sink snapshotInsertWriteSink) { writeScalarBoolField(ptr, sink, offset) },
		writeScratch: func(ptr unsafe.Pointer, sink *fieldWriteScratch) { writeScalarBoolField(ptr, sink, offset) },
		modified: func(v1, v2 unsafe.Pointer) bool {
			return scalarFieldValue[bool](v1, offset) != scalarFieldValue[bool](v2, offset)
		},
	}
}

func intFieldAccessorBundle[T signedFieldValue](offset uintptr, ptr bool) fieldAccessorBundle {
	if ptr {
		return fieldAccessorBundle{
			unique: func(ptr unsafe.Pointer) (string, bool, bool) {
				if ptr == nil {
					return "", false, false
				}
				v := ptrFieldValue[T](ptr, offset)
				if v == nil {
					return "", true, true
				}
				return int64ByteStr(int64(*v)), true, false
			},
			writeBuild: func(ptr unsafe.Pointer, sink buildFieldWriteSink) {
				writePtrIntField[buildFieldWriteSink, T](ptr, sink, offset)
			},
			writeOverlay: func(ptr unsafe.Pointer, sink snapshotOverlayWriteSink) {
				writePtrIntField[snapshotOverlayWriteSink, T](ptr, sink, offset)
			},
			writeInsert: func(ptr unsafe.Pointer, sink snapshotInsertWriteSink) {
				writePtrIntField[snapshotInsertWriteSink, T](ptr, sink, offset)
			},
			writeScratch: func(ptr unsafe.Pointer, sink *fieldWriteScratch) {
				writePtrIntField[*fieldWriteScratch, T](ptr, sink, offset)
			},
			modified: func(v1, v2 unsafe.Pointer) bool {
				p1 := ptrFieldValue[T](v1, offset)
				p2 := ptrFieldValue[T](v2, offset)
				if p1 == nil || p2 == nil {
					return p1 != p2
				}
				return *p1 != *p2
			},
		}
	}
	return fieldAccessorBundle{
		unique: func(ptr unsafe.Pointer) (string, bool, bool) {
			if ptr == nil {
				return "", false, false
			}
			return int64ByteStr(int64(scalarFieldValue[T](ptr, offset))), true, false
		},
		writeBuild: func(ptr unsafe.Pointer, sink buildFieldWriteSink) {
			writeScalarIntField[buildFieldWriteSink, T](ptr, sink, offset)
		},
		writeOverlay: func(ptr unsafe.Pointer, sink snapshotOverlayWriteSink) {
			writeScalarIntField[snapshotOverlayWriteSink, T](ptr, sink, offset)
		},
		writeInsert: func(ptr unsafe.Pointer, sink snapshotInsertWriteSink) {
			writeScalarIntField[snapshotInsertWriteSink, T](ptr, sink, offset)
		},
		writeScratch: func(ptr unsafe.Pointer, sink *fieldWriteScratch) {
			writeScalarIntField[*fieldWriteScratch, T](ptr, sink, offset)
		},
		modified: func(v1, v2 unsafe.Pointer) bool {
			return scalarFieldValue[T](v1, offset) != scalarFieldValue[T](v2, offset)
		},
	}
}

func uintFieldAccessorBundle[T unsignedFieldValue](offset uintptr, ptr bool) fieldAccessorBundle {
	if ptr {
		return fieldAccessorBundle{
			unique: func(ptr unsafe.Pointer) (string, bool, bool) {
				if ptr == nil {
					return "", false, false
				}
				v := ptrFieldValue[T](ptr, offset)
				if v == nil {
					return "", true, true
				}
				return uint64ByteStr(uint64(*v)), true, false
			},
			writeBuild: func(ptr unsafe.Pointer, sink buildFieldWriteSink) {
				writePtrUintField[buildFieldWriteSink, T](ptr, sink, offset)
			},
			writeOverlay: func(ptr unsafe.Pointer, sink snapshotOverlayWriteSink) {
				writePtrUintField[snapshotOverlayWriteSink, T](ptr, sink, offset)
			},
			writeInsert: func(ptr unsafe.Pointer, sink snapshotInsertWriteSink) {
				writePtrUintField[snapshotInsertWriteSink, T](ptr, sink, offset)
			},
			writeScratch: func(ptr unsafe.Pointer, sink *fieldWriteScratch) {
				writePtrUintField[*fieldWriteScratch, T](ptr, sink, offset)
			},
			modified: func(v1, v2 unsafe.Pointer) bool {
				p1 := ptrFieldValue[T](v1, offset)
				p2 := ptrFieldValue[T](v2, offset)
				if p1 == nil || p2 == nil {
					return p1 != p2
				}
				return *p1 != *p2
			},
		}
	}
	return fieldAccessorBundle{
		unique: func(ptr unsafe.Pointer) (string, bool, bool) {
			if ptr == nil {
				return "", false, false
			}
			return uint64ByteStr(uint64(scalarFieldValue[T](ptr, offset))), true, false
		},
		writeBuild: func(ptr unsafe.Pointer, sink buildFieldWriteSink) {
			writeScalarUintField[buildFieldWriteSink, T](ptr, sink, offset)
		},
		writeOverlay: func(ptr unsafe.Pointer, sink snapshotOverlayWriteSink) {
			writeScalarUintField[snapshotOverlayWriteSink, T](ptr, sink, offset)
		},
		writeInsert: func(ptr unsafe.Pointer, sink snapshotInsertWriteSink) {
			writeScalarUintField[snapshotInsertWriteSink, T](ptr, sink, offset)
		},
		writeScratch: func(ptr unsafe.Pointer, sink *fieldWriteScratch) {
			writeScalarUintField[*fieldWriteScratch, T](ptr, sink, offset)
		},
		modified: func(v1, v2 unsafe.Pointer) bool {
			return scalarFieldValue[T](v1, offset) != scalarFieldValue[T](v2, offset)
		},
	}
}

func floatFieldAccessorBundle[T floatFieldValue](offset uintptr, ptr bool) fieldAccessorBundle {
	if ptr {
		return fieldAccessorBundle{
			unique: func(ptr unsafe.Pointer) (string, bool, bool) {
				if ptr == nil {
					return "", false, false
				}
				v := ptrFieldValue[T](ptr, offset)
				if v == nil {
					return "", true, true
				}
				return float64ByteStr(float64(*v)), true, false
			},
			writeBuild: func(ptr unsafe.Pointer, sink buildFieldWriteSink) {
				writePtrFloatField[buildFieldWriteSink, T](ptr, sink, offset)
			},
			writeOverlay: func(ptr unsafe.Pointer, sink snapshotOverlayWriteSink) {
				writePtrFloatField[snapshotOverlayWriteSink, T](ptr, sink, offset)
			},
			writeInsert: func(ptr unsafe.Pointer, sink snapshotInsertWriteSink) {
				writePtrFloatField[snapshotInsertWriteSink, T](ptr, sink, offset)
			},
			writeScratch: func(ptr unsafe.Pointer, sink *fieldWriteScratch) {
				writePtrFloatField[*fieldWriteScratch, T](ptr, sink, offset)
			},
			modified: func(v1, v2 unsafe.Pointer) bool {
				p1 := ptrFieldValue[T](v1, offset)
				p2 := ptrFieldValue[T](v2, offset)
				if p1 == nil || p2 == nil {
					return p1 != p2
				}
				return *p1 != *p2
			},
		}
	}
	return fieldAccessorBundle{
		unique: func(ptr unsafe.Pointer) (string, bool, bool) {
			if ptr == nil {
				return "", false, false
			}
			return float64ByteStr(float64(scalarFieldValue[T](ptr, offset))), true, false
		},
		writeBuild: func(ptr unsafe.Pointer, sink buildFieldWriteSink) {
			writeScalarFloatField[buildFieldWriteSink, T](ptr, sink, offset)
		},
		writeOverlay: func(ptr unsafe.Pointer, sink snapshotOverlayWriteSink) {
			writeScalarFloatField[snapshotOverlayWriteSink, T](ptr, sink, offset)
		},
		writeInsert: func(ptr unsafe.Pointer, sink snapshotInsertWriteSink) {
			writeScalarFloatField[snapshotInsertWriteSink, T](ptr, sink, offset)
		},
		writeScratch: func(ptr unsafe.Pointer, sink *fieldWriteScratch) {
			writeScalarFloatField[*fieldWriteScratch, T](ptr, sink, offset)
		},
		modified: func(v1, v2 unsafe.Pointer) bool {
			return scalarFieldValue[T](v1, offset) != scalarFieldValue[T](v2, offset)
		},
	}
}

func stringSliceAccessorBundle(offset uintptr) fieldAccessorBundle {
	return fieldAccessorBundle{
		writeBuild:   func(ptr unsafe.Pointer, sink buildFieldWriteSink) { writeStringSliceField(ptr, sink, offset) },
		writeOverlay: func(ptr unsafe.Pointer, sink snapshotOverlayWriteSink) { writeStringSliceField(ptr, sink, offset) },
		writeInsert:  func(ptr unsafe.Pointer, sink snapshotInsertWriteSink) { writeStringSliceField(ptr, sink, offset) },
		writeScratch: func(ptr unsafe.Pointer, sink *fieldWriteScratch) { writeStringSliceField(ptr, sink, offset) },
		modified: func(v1, v2 unsafe.Pointer) bool {
			return slicesModified(sliceFieldValue[string](v1, offset), sliceFieldValue[string](v2, offset))
		},
	}
}

func boolSliceAccessorBundle(offset uintptr) fieldAccessorBundle {
	return fieldAccessorBundle{
		writeBuild:   func(ptr unsafe.Pointer, sink buildFieldWriteSink) { writeBoolSliceField(ptr, sink, offset) },
		writeOverlay: func(ptr unsafe.Pointer, sink snapshotOverlayWriteSink) { writeBoolSliceField(ptr, sink, offset) },
		writeInsert:  func(ptr unsafe.Pointer, sink snapshotInsertWriteSink) { writeBoolSliceField(ptr, sink, offset) },
		writeScratch: func(ptr unsafe.Pointer, sink *fieldWriteScratch) { writeBoolSliceField(ptr, sink, offset) },
		modified: func(v1, v2 unsafe.Pointer) bool {
			return slicesModified(sliceFieldValue[bool](v1, offset), sliceFieldValue[bool](v2, offset))
		},
	}
}

func intSliceAccessorBundle[T signedFieldValue](offset uintptr) fieldAccessorBundle {
	return fieldAccessorBundle{
		writeBuild: func(ptr unsafe.Pointer, sink buildFieldWriteSink) {
			writeIntSliceField[buildFieldWriteSink, T](ptr, sink, offset)
		},
		writeOverlay: func(ptr unsafe.Pointer, sink snapshotOverlayWriteSink) {
			writeIntSliceField[snapshotOverlayWriteSink, T](ptr, sink, offset)
		},
		writeInsert: func(ptr unsafe.Pointer, sink snapshotInsertWriteSink) {
			writeIntSliceField[snapshotInsertWriteSink, T](ptr, sink, offset)
		},
		writeScratch: func(ptr unsafe.Pointer, sink *fieldWriteScratch) {
			writeIntSliceField[*fieldWriteScratch, T](ptr, sink, offset)
		},
		modified: func(v1, v2 unsafe.Pointer) bool {
			return slicesModified(sliceFieldValue[T](v1, offset), sliceFieldValue[T](v2, offset))
		},
	}
}

func uintSliceAccessorBundle[T unsignedFieldValue](offset uintptr) fieldAccessorBundle {
	return fieldAccessorBundle{
		writeBuild: func(ptr unsafe.Pointer, sink buildFieldWriteSink) {
			writeUintSliceField[buildFieldWriteSink, T](ptr, sink, offset)
		},
		writeOverlay: func(ptr unsafe.Pointer, sink snapshotOverlayWriteSink) {
			writeUintSliceField[snapshotOverlayWriteSink, T](ptr, sink, offset)
		},
		writeInsert: func(ptr unsafe.Pointer, sink snapshotInsertWriteSink) {
			writeUintSliceField[snapshotInsertWriteSink, T](ptr, sink, offset)
		},
		writeScratch: func(ptr unsafe.Pointer, sink *fieldWriteScratch) {
			writeUintSliceField[*fieldWriteScratch, T](ptr, sink, offset)
		},
		modified: func(v1, v2 unsafe.Pointer) bool {
			return slicesModified(sliceFieldValue[T](v1, offset), sliceFieldValue[T](v2, offset))
		},
	}
}

func floatSliceAccessorBundle[T floatFieldValue](offset uintptr) fieldAccessorBundle {
	return fieldAccessorBundle{
		writeBuild: func(ptr unsafe.Pointer, sink buildFieldWriteSink) {
			writeFloatSliceField[buildFieldWriteSink, T](ptr, sink, offset)
		},
		writeOverlay: func(ptr unsafe.Pointer, sink snapshotOverlayWriteSink) {
			writeFloatSliceField[snapshotOverlayWriteSink, T](ptr, sink, offset)
		},
		writeInsert: func(ptr unsafe.Pointer, sink snapshotInsertWriteSink) {
			writeFloatSliceField[snapshotInsertWriteSink, T](ptr, sink, offset)
		},
		writeScratch: func(ptr unsafe.Pointer, sink *fieldWriteScratch) {
			writeFloatSliceField[*fieldWriteScratch, T](ptr, sink, offset)
		},
		modified: func(v1, v2 unsafe.Pointer) bool {
			return slicesModified(sliceFieldValue[T](v1, offset), sliceFieldValue[T](v2, offset))
		},
	}
}

func buildPatchValueEqualFn(f *field, fieldType reflect.Type, offset uintptr) patchValueEqualFn {
	if f == nil {
		return nil
	}
	if f.UseVI {
		return nil
	}

	if f.Slice && fieldType.Kind() == reflect.Slice {
		switch f.Kind {
		case reflect.String:
			return slicePatchValueEqual[string](offset)
		case reflect.Bool:
			return slicePatchValueEqual[bool](offset)
		case reflect.Int:
			return slicePatchValueEqual[int](offset)
		case reflect.Int8:
			return slicePatchValueEqual[int8](offset)
		case reflect.Int16:
			return slicePatchValueEqual[int16](offset)
		case reflect.Int32:
			return slicePatchValueEqual[int32](offset)
		case reflect.Int64:
			return slicePatchValueEqual[int64](offset)
		case reflect.Uint:
			return slicePatchValueEqual[uint](offset)
		case reflect.Uint8:
			return slicePatchValueEqual[uint8](offset)
		case reflect.Uint16:
			return slicePatchValueEqual[uint16](offset)
		case reflect.Uint32:
			return slicePatchValueEqual[uint32](offset)
		case reflect.Uint64:
			return slicePatchValueEqual[uint64](offset)
		case reflect.Float32:
			return slicePatchValueEqual[float32](offset)
		case reflect.Float64:
			return slicePatchValueEqual[float64](offset)
		default:
			return nil
		}
	}

	switch f.Kind {
	case reflect.String:
		return scalarPatchValueEqual[string](offset, f.Ptr)
	case reflect.Bool:
		return scalarPatchValueEqual[bool](offset, f.Ptr)
	case reflect.Int:
		return scalarPatchValueEqual[int](offset, f.Ptr)
	case reflect.Int8:
		return scalarPatchValueEqual[int8](offset, f.Ptr)
	case reflect.Int16:
		return scalarPatchValueEqual[int16](offset, f.Ptr)
	case reflect.Int32:
		return scalarPatchValueEqual[int32](offset, f.Ptr)
	case reflect.Int64:
		return scalarPatchValueEqual[int64](offset, f.Ptr)
	case reflect.Uint:
		return scalarPatchValueEqual[uint](offset, f.Ptr)
	case reflect.Uint8:
		return scalarPatchValueEqual[uint8](offset, f.Ptr)
	case reflect.Uint16:
		return scalarPatchValueEqual[uint16](offset, f.Ptr)
	case reflect.Uint32:
		return scalarPatchValueEqual[uint32](offset, f.Ptr)
	case reflect.Uint64:
		return scalarPatchValueEqual[uint64](offset, f.Ptr)
	case reflect.Float32:
		return scalarPatchValueEqual[float32](offset, f.Ptr)
	case reflect.Float64:
		return scalarPatchValueEqual[float64](offset, f.Ptr)
	default:
		return nil
	}
}

func buildPatchValueCopyFn(f *field, fieldType reflect.Type, offset uintptr) patchValueCopyFn {
	if f == nil {
		return nil
	}
	if f.UseVI {
		return nil
	}

	if f.Slice && fieldType.Kind() == reflect.Slice {
		switch fieldType {
		case reflect.TypeFor[[]string]():
			return func(root unsafe.Pointer) any {
				return slices.Clone(sliceFieldValue[string](root, offset))
			}
		case reflect.TypeFor[[]bool]():
			return func(root unsafe.Pointer) any {
				return slices.Clone(sliceFieldValue[bool](root, offset))
			}
		case reflect.TypeFor[[]int]():
			return func(root unsafe.Pointer) any {
				return slices.Clone(sliceFieldValue[int](root, offset))
			}
		case reflect.TypeFor[[]int8]():
			return func(root unsafe.Pointer) any {
				return slices.Clone(sliceFieldValue[int8](root, offset))
			}
		case reflect.TypeFor[[]int16]():
			return func(root unsafe.Pointer) any {
				return slices.Clone(sliceFieldValue[int16](root, offset))
			}
		case reflect.TypeFor[[]int32]():
			return func(root unsafe.Pointer) any {
				return slices.Clone(sliceFieldValue[int32](root, offset))
			}
		case reflect.TypeFor[[]int64]():
			return func(root unsafe.Pointer) any {
				return slices.Clone(sliceFieldValue[int64](root, offset))
			}
		case reflect.TypeFor[[]uint]():
			return func(root unsafe.Pointer) any {
				return slices.Clone(sliceFieldValue[uint](root, offset))
			}
		case reflect.TypeFor[[]uint8]():
			return func(root unsafe.Pointer) any {
				return slices.Clone(sliceFieldValue[uint8](root, offset))
			}
		case reflect.TypeFor[[]uint16]():
			return func(root unsafe.Pointer) any {
				return slices.Clone(sliceFieldValue[uint16](root, offset))
			}
		case reflect.TypeFor[[]uint32]():
			return func(root unsafe.Pointer) any {
				return slices.Clone(sliceFieldValue[uint32](root, offset))
			}
		case reflect.TypeFor[[]uint64]():
			return func(root unsafe.Pointer) any {
				return slices.Clone(sliceFieldValue[uint64](root, offset))
			}
		case reflect.TypeFor[[]float32]():
			return func(root unsafe.Pointer) any {
				return slices.Clone(sliceFieldValue[float32](root, offset))
			}
		case reflect.TypeFor[[]float64]():
			return func(root unsafe.Pointer) any {
				return slices.Clone(sliceFieldValue[float64](root, offset))
			}
		default:
			if typeHasMutableReferencePayload(fieldType.Elem()) {
				return nil
			}
			return reflectSlicePatchValueCopy(fieldType, offset)
		}
	}
	return nil
}

func buildFieldAccessorBundle(f *field, fieldType reflect.Type, offset uintptr) (fieldAccessorBundle, error) {
	if f == nil {
		return fieldAccessorBundle{}, nil
	}

	if isNativeTimeField(f) {
		return timeFieldAccessorBundle(offset, f.Ptr), nil
	}

	if f.UseVI {
		if f.Slice {
			return valueIndexerSliceReflectAccessorBundle(fieldType, offset), nil
		}
		return valueIndexerScalarReflectAccessorBundle(fieldType, offset), nil
	}

	if f.Slice {
		switch f.Kind {
		case reflect.String:
			return stringSliceAccessorBundle(offset), nil
		case reflect.Bool:
			return boolSliceAccessorBundle(offset), nil
		case reflect.Int:
			return intSliceAccessorBundle[int](offset), nil
		case reflect.Int8:
			return intSliceAccessorBundle[int8](offset), nil
		case reflect.Int16:
			return intSliceAccessorBundle[int16](offset), nil
		case reflect.Int32:
			return intSliceAccessorBundle[int32](offset), nil
		case reflect.Int64:
			return intSliceAccessorBundle[int64](offset), nil
		case reflect.Uint:
			return uintSliceAccessorBundle[uint](offset), nil
		case reflect.Uint8:
			return uintSliceAccessorBundle[uint8](offset), nil
		case reflect.Uint16:
			return uintSliceAccessorBundle[uint16](offset), nil
		case reflect.Uint32:
			return uintSliceAccessorBundle[uint32](offset), nil
		case reflect.Uint64:
			return uintSliceAccessorBundle[uint64](offset), nil
		case reflect.Float32:
			return floatSliceAccessorBundle[float32](offset), nil
		case reflect.Float64:
			return floatSliceAccessorBundle[float64](offset), nil
		default:
			return fieldAccessorBundle{}, fmt.Errorf("unsupported slice field kind %v", f.Kind)
		}
	}

	switch f.Kind {
	case reflect.String:
		return stringFieldAccessorBundle(offset, f.Ptr), nil
	case reflect.Bool:
		return boolFieldAccessorBundle(offset, f.Ptr), nil
	case reflect.Int:
		return intFieldAccessorBundle[int](offset, f.Ptr), nil
	case reflect.Int8:
		return intFieldAccessorBundle[int8](offset, f.Ptr), nil
	case reflect.Int16:
		return intFieldAccessorBundle[int16](offset, f.Ptr), nil
	case reflect.Int32:
		return intFieldAccessorBundle[int32](offset, f.Ptr), nil
	case reflect.Int64:
		return intFieldAccessorBundle[int64](offset, f.Ptr), nil
	case reflect.Uint:
		return uintFieldAccessorBundle[uint](offset, f.Ptr), nil
	case reflect.Uint8:
		return uintFieldAccessorBundle[uint8](offset, f.Ptr), nil
	case reflect.Uint16:
		return uintFieldAccessorBundle[uint16](offset, f.Ptr), nil
	case reflect.Uint32:
		return uintFieldAccessorBundle[uint32](offset, f.Ptr), nil
	case reflect.Uint64:
		return uintFieldAccessorBundle[uint64](offset, f.Ptr), nil
	case reflect.Float32:
		return floatFieldAccessorBundle[float32](offset, f.Ptr), nil
	case reflect.Float64:
		return floatFieldAccessorBundle[float64](offset, f.Ptr), nil
	default:
		return fieldAccessorBundle{}, fmt.Errorf("unsupported field kind %v", f.Kind)
	}
}

func (db *DB[K, V]) makeIndexedFieldAccessor(f *field) (indexedFieldAccessor, error) {
	acc := indexedFieldAccessor{
		name:  f.DBName,
		field: f,
	}

	fieldType, offset := resolveFieldTypeAndOffset(db.vtype, f.Index)
	bundle, err := buildFieldAccessorBundle(f, fieldType, offset)
	if err != nil {
		return indexedFieldAccessor{}, err
	}

	acc.writeBuild = bundle.writeBuild
	acc.writeOverlay = bundle.writeOverlay
	acc.writeInsert = bundle.writeInsert
	acc.writeScratch = bundle.writeScratch
	acc.modified = bundle.modified
	if f.Unique && !f.Slice {
		acc.uniqueGetter = bundle.unique
	}
	return acc, nil
}
