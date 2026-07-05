package schema

import (
	"fmt"
	"reflect"
	"slices"
	"time"
	"unsafe"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
)

type (
	FieldModifiedFn   func(v1, v2 unsafe.Pointer) bool
	PatchValueEqualFn func(v1, v2 unsafe.Pointer) bool
	PatchValueCopyFn  func(ptr unsafe.Pointer) any
)

const smallDistinctLimit = 8

type fieldAccessorBundle struct {
	unique       UniqueScalarGetterFn
	writeBuild   BuildFieldWriteAccessorFn
	writeChecked BuildFieldWriteCheckedAccessorFn
	writeIndex   IndexFieldWriteAccessorFn
	writeInsert  InsertFieldWriteAccessorFn
	writeScratch ScratchFieldWriteAccessorFn
	validate     StringValidationAccessorFn
	modified     FieldModifiedFn
}

type ifaceWords struct {
	tab  unsafe.Pointer
	data unsafe.Pointer
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

type checkedBuildSink struct {
	sink  BuildSink
	field string
	err   error
}

func validateStringKey(field string, key string) error {
	if len(key) <= indexdata.FieldStringRefMax {
		return nil
	}
	return fmt.Errorf("field %q indexed string value len %d exceeds limit %d", field, len(key), indexdata.FieldStringRefMax)
}

func (s *checkedBuildSink) setNil() {
	if s.err == nil {
		s.sink.setNil()
	}
}

func (s *checkedBuildSink) setLen(length int) {
	if s.err == nil {
		s.sink.setLen(length)
	}
}

func (s *checkedBuildSink) addString(key string) {
	if s.err != nil {
		return
	}
	if err := validateStringKey(s.field, key); err != nil {
		s.err = err
		return
	}
	s.sink.addString(key)
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
	return *(*[]T)(unsafe.Add(ptr, offset))
}

func sliceFieldData(ptr unsafe.Pointer, offset uintptr) (unsafe.Pointer, int) {
	// The element type is only known at runtime on this path; the []byte view
	// is used only to read the slice data pointer and element count.
	vals := *(*[]byte)(unsafe.Add(ptr, offset))
	return unsafe.Pointer(unsafe.SliceData(vals)), len(vals)
}

func valueIndexerPtrTab(t reflect.Type) unsafe.Pointer {
	vi := reflect.New(t).Interface().(ValueIndexer)
	return (*ifaceWords)(unsafe.Pointer(&vi)).tab
}

func valueIndexerAt(tab unsafe.Pointer, data unsafe.Pointer) ValueIndexer {
	// The compiled accessor already proved that *T implements ValueIndexer;
	// rebuilding the interface avoids reflect.NewAt on every indexed value.
	var vi ValueIndexer
	words := (*ifaceWords)(unsafe.Pointer(&vi))
	words.tab = tab
	words.data = data
	return vi
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

func floatsEqualForIndex[T floatFieldValue](lhs, rhs T) bool {
	return lhs == rhs || lhs != lhs && rhs != rhs
}

func scalarPatchValueEqual[T comparable](offset uintptr, ptr bool) PatchValueEqualFn {
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

func floatPatchValueEqual[T floatFieldValue](offset uintptr, ptr bool) PatchValueEqualFn {
	if ptr {
		return func(v1, v2 unsafe.Pointer) bool {
			p1 := ptrFieldValue[T](v1, offset)
			p2 := ptrFieldValue[T](v2, offset)
			if p1 == nil || p2 == nil {
				return p1 == p2
			}
			return floatsEqualForIndex(*p1, *p2)
		}
	}
	return func(v1, v2 unsafe.Pointer) bool {
		return floatsEqualForIndex(scalarFieldValue[T](v1, offset), scalarFieldValue[T](v2, offset))
	}
}

func slicePatchValueEqual[T comparable](offset uintptr) PatchValueEqualFn {
	return func(v1, v2 unsafe.Pointer) bool {
		return slicesEqualExact(sliceFieldValue[T](v1, offset), sliceFieldValue[T](v2, offset))
	}
}

func floatSlicePatchValueEqual[T floatFieldValue](offset uintptr) PatchValueEqualFn {
	return func(v1, v2 unsafe.Pointer) bool {
		lhs := sliceFieldValue[T](v1, offset)
		rhs := sliceFieldValue[T](v2, offset)
		if (lhs == nil) != (rhs == nil) {
			return false
		}
		if len(lhs) != len(rhs) {
			return false
		}
		for i := range lhs {
			if !floatsEqualForIndex(lhs[i], rhs[i]) {
				return false
			}
		}
		return true
	}
}

func reflectFloatSlicePatchValueEqual(fieldType reflect.Type, offset uintptr) PatchValueEqualFn {
	return func(v1, v2 unsafe.Pointer) bool {
		return reflectPatchFloatSlicesEqual(
			reflect.NewAt(fieldType, unsafe.Add(v1, offset)).Elem(),
			reflect.NewAt(fieldType, unsafe.Add(v2, offset)).Elem(),
		)
	}
}

func reflectSlicePatchValueCopy(fieldType reflect.Type, offset uintptr) PatchValueCopyFn {
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

func scalarPatchValueCopy[T any](offset uintptr) PatchValueCopyFn {
	return func(root unsafe.Pointer) any {
		return scalarFieldValue[T](root, offset)
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

func typeSupportsPatchFloatEqual(t reflect.Type) bool {
	ok, needs := patchFloatEqualType(t)
	return ok && needs
}

func patchFloatEqualType(t reflect.Type) (bool, bool) {
	switch t.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.String,
		reflect.Complex64, reflect.Complex128:
		return true, false
	case reflect.Float32, reflect.Float64:
		return true, true
	case reflect.Array:
		return patchFloatEqualType(t.Elem())
	case reflect.Slice:
		switch t.Elem().Kind() {
		case reflect.Float32, reflect.Float64:
			return true, true
		default:
			return false, false
		}
	case reflect.Struct:
		needs := false
		for i := 0; i < t.NumField(); i++ {
			ok, fieldNeeds := patchFloatEqualType(t.Field(i).Type)
			if !ok {
				return false, false
			}
			needs = needs || fieldNeeds
		}
		return true, needs
	}
	return false, false
}

func reflectPatchValueEqual(fieldType reflect.Type, offset uintptr) PatchValueEqualFn {
	return func(v1, v2 unsafe.Pointer) bool {
		return reflectPatchValuesEqual(
			reflect.NewAt(fieldType, unsafe.Add(v1, offset)).Elem(),
			reflect.NewAt(fieldType, unsafe.Add(v2, offset)).Elem(),
		)
	}
}

func reflectPatchValuesEqual(v1, v2 reflect.Value) bool {
	switch v1.Kind() {
	case reflect.Bool:
		return v1.Bool() == v2.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v1.Int() == v2.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v1.Uint() == v2.Uint()
	case reflect.String:
		return v1.String() == v2.String()
	case reflect.Float32, reflect.Float64:
		return floatsEqualForIndex(v1.Float(), v2.Float())
	case reflect.Complex64, reflect.Complex128:
		return v1.Complex() == v2.Complex()
	case reflect.Array:
		for i := 0; i < v1.Len(); i++ {
			if !reflectPatchValuesEqual(v1.Index(i), v2.Index(i)) {
				return false
			}
		}
		return true
	case reflect.Slice:
		return reflectPatchFloatSlicesEqual(v1, v2)
	case reflect.Struct:
		for i := 0; i < v1.NumField(); i++ {
			if !reflectPatchValuesEqual(v1.Field(i), v2.Field(i)) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func reflectPatchFloatSlicesEqual(v1, v2 reflect.Value) bool {
	if v1.IsNil() || v2.IsNil() {
		return v1.IsNil() == v2.IsNil()
	}
	if v1.Len() != v2.Len() {
		return false
	}
	switch v1.Type().Elem().Kind() {
	case reflect.Float32, reflect.Float64:
		for i := 0; i < v1.Len(); i++ {
			if !floatsEqualForIndex(v1.Index(i).Float(), v2.Index(i).Float()) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func addDistinctStringsToSink[S stringValueSink](vals []string, sink S) int {
	if len(vals) == 0 {
		return 0
	}
	if len(vals) == 1 {
		sink.addString(vals[0])
		return 1
	}
	if len(vals) <= smallDistinctLimit {
		distinct := 0
		for i := range vals {
			cur := vals[i]
			seen := false
			for j := 0; j < i; j++ {
				if vals[j] == cur {
					seen = true
					break
				}
			}
			if seen {
				continue
			}
			distinct++
			sink.addString(cur)
		}
		return distinct
	}
	seen := stringSetPool.Get()
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
	stringSetPool.Put(seen)
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
	if n <= smallDistinctLimit {
		var keys [smallDistinctLimit]string
		distinct := 0
		for i := 0; i < n; i++ {
			cur := vals.Index(i).Interface().(ValueIndexer).IndexingValue()
			seen := false
			for j := 0; j < distinct; j++ {
				if keys[j] == cur {
					seen = true
					break
				}
			}
			if seen {
				continue
			}
			keys[distinct] = cur
			distinct++
			sink.addString(cur)
		}
		return distinct
	}
	seen := stringSetPool.Get()
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
	stringSetPool.Put(seen)
	return distinct
}

func addDistinctValueIndexerPtrStringsToSink[S stringValueSink](data unsafe.Pointer, n int, size uintptr, tab unsafe.Pointer, sink S) int {
	if n == 0 {
		return 0
	}
	if n == 1 {
		sink.addString(valueIndexerAt(tab, data).IndexingValue())
		return 1
	}
	if n <= smallDistinctLimit {
		var keys [smallDistinctLimit]string
		distinct := 0
		for i := 0; i < n; i++ {
			cur := valueIndexerAt(tab, unsafe.Add(data, uintptr(i)*size)).IndexingValue()
			seen := false
			for j := 0; j < distinct; j++ {
				if keys[j] == cur {
					seen = true
					break
				}
			}
			if seen {
				continue
			}
			keys[distinct] = cur
			distinct++
			sink.addString(cur)
		}
		return distinct
	}
	seen := stringSetPool.Get()
	distinct := 0
	for i := 0; i < n; i++ {
		cur := valueIndexerAt(tab, unsafe.Add(data, uintptr(i)*size)).IndexingValue()
		if _, ok := seen[cur]; ok {
			continue
		}
		seen[cur] = struct{}{}
		distinct++
		sink.addString(cur)
	}
	stringSetPool.Put(seen)
	return distinct
}

func addDistinctValueIndexerValuePtrStringsToSink[S stringValueSink](data unsafe.Pointer, n int, size uintptr, tab unsafe.Pointer, sink S) int {
	if n == 0 {
		return 0
	}
	if n == 1 {
		elem := *(*unsafe.Pointer)(data)
		if elem == nil {
			return 0
		}
		sink.addString(valueIndexerAt(tab, elem).IndexingValue())
		return 1
	}
	if n <= smallDistinctLimit {
		var keys [smallDistinctLimit]string
		distinct := 0
		for i := 0; i < n; i++ {
			elem := *(*unsafe.Pointer)(unsafe.Add(data, uintptr(i)*size))
			if elem == nil {
				continue
			}
			cur := valueIndexerAt(tab, elem).IndexingValue()
			seen := false
			for j := 0; j < distinct; j++ {
				if keys[j] == cur {
					seen = true
					break
				}
			}
			if seen {
				continue
			}
			keys[distinct] = cur
			distinct++
			sink.addString(cur)
		}
		return distinct
	}
	seen := stringSetPool.Get()
	distinct := 0
	for i := 0; i < n; i++ {
		elem := *(*unsafe.Pointer)(unsafe.Add(data, uintptr(i)*size))
		if elem == nil {
			continue
		}
		cur := valueIndexerAt(tab, elem).IndexingValue()
		if _, ok := seen[cur]; ok {
			continue
		}
		seen[cur] = struct{}{}
		distinct++
		sink.addString(cur)
	}
	stringSetPool.Put(seen)
	return distinct
}

func addDistinctSignedFixedKeysToSink[S fixedValueSink, T signedFieldValue](vals []T, sink S) int {
	if len(vals) == 0 {
		return 0
	}
	if len(vals) == 1 {
		sink.addFixed(keycodec.OrderedInt64Key(int64(vals[0])))
		return 1
	}
	if len(vals) <= smallDistinctLimit {
		var keys [smallDistinctLimit]uint64
		distinct := 0
		for i := range vals {
			cur := keycodec.OrderedInt64Key(int64(vals[i]))
			seen := false
			for j := 0; j < distinct; j++ {
				if keys[j] == cur {
					seen = true
					break
				}
			}
			if seen {
				continue
			}
			keys[distinct] = cur
			distinct++
			sink.addFixed(cur)
		}
		return distinct
	}
	seen := newU64Set(len(vals))
	distinct := 0
	for i := range vals {
		cur := keycodec.OrderedInt64Key(int64(vals[i]))
		if !seen.Add(cur) {
			continue
		}
		distinct++
		sink.addFixed(cur)
	}
	releaseU64Set(&seen)
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
	if len(vals) <= smallDistinctLimit {
		var keys [smallDistinctLimit]uint64
		distinct := 0
		for i := range vals {
			cur := uint64(vals[i])
			seen := false
			for j := 0; j < distinct; j++ {
				if keys[j] == cur {
					seen = true
					break
				}
			}
			if seen {
				continue
			}
			keys[distinct] = cur
			distinct++
			sink.addFixed(cur)
		}
		return distinct
	}
	seen := newU64Set(len(vals))
	distinct := 0
	for i := range vals {
		cur := uint64(vals[i])
		if !seen.Add(cur) {
			continue
		}
		distinct++
		sink.addFixed(cur)
	}
	releaseU64Set(&seen)
	return distinct
}

func addDistinctFloatFixedKeysToSink[S fixedValueSink, T floatFieldValue](vals []T, sink S) int {
	if len(vals) == 0 {
		return 0
	}
	if len(vals) == 1 {
		sink.addFixed(keycodec.OrderedFloat64Key(float64(vals[0])))
		return 1
	}
	if len(vals) <= smallDistinctLimit {
		var keys [smallDistinctLimit]uint64
		distinct := 0
		for i := range vals {
			cur := keycodec.OrderedFloat64Key(float64(vals[i]))
			seen := false
			for j := 0; j < distinct; j++ {
				if keys[j] == cur {
					seen = true
					break
				}
			}
			if seen {
				continue
			}
			keys[distinct] = cur
			distinct++
			sink.addFixed(cur)
		}
		return distinct
	}
	seen := newU64Set(len(vals))
	distinct := 0
	for i := range vals {
		cur := keycodec.OrderedFloat64Key(float64(vals[i]))
		if !seen.Add(cur) {
			continue
		}
		distinct++
		sink.addFixed(cur)
	}
	releaseU64Set(&seen)
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
	v := ptrFieldValue[string](ptr, offset)
	if v == nil {
		sink.setNil()
		return
	}
	sink.addString(*v)
}

func writeScalarStringField[S stringValueSink](ptr unsafe.Pointer, sink S, offset uintptr) {
	sink.addString(scalarFieldValue[string](ptr, offset))
}

func writePtrBoolField[S nilStringValueSink](ptr unsafe.Pointer, sink S, offset uintptr) {
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
	if scalarFieldValue[bool](ptr, offset) {
		sink.addString("1")
		return
	}
	sink.addString("0")
}

func writePtrIntField[S nilFixedValueSink, T signedFieldValue](ptr unsafe.Pointer, sink S, offset uintptr) {
	v := ptrFieldValue[T](ptr, offset)
	if v == nil {
		sink.setNil()
		return
	}
	sink.addFixed(keycodec.OrderedInt64Key(int64(*v)))
}

func writeScalarIntField[S fixedValueSink, T signedFieldValue](ptr unsafe.Pointer, sink S, offset uintptr) {
	sink.addFixed(keycodec.OrderedInt64Key(int64(scalarFieldValue[T](ptr, offset))))
}

func writePtrUintField[S nilFixedValueSink, T unsignedFieldValue](ptr unsafe.Pointer, sink S, offset uintptr) {
	v := ptrFieldValue[T](ptr, offset)
	if v == nil {
		sink.setNil()
		return
	}
	sink.addFixed(uint64(*v))
}

func writeScalarUintField[S fixedValueSink, T unsignedFieldValue](ptr unsafe.Pointer, sink S, offset uintptr) {
	sink.addFixed(uint64(scalarFieldValue[T](ptr, offset)))
}

func writePtrFloatField[S nilFixedValueSink, T floatFieldValue](ptr unsafe.Pointer, sink S, offset uintptr) {
	v := ptrFieldValue[T](ptr, offset)
	if v == nil {
		sink.setNil()
		return
	}
	sink.addFixed(keycodec.OrderedFloat64Key(float64(*v)))
}

func writeScalarFloatField[S fixedValueSink, T floatFieldValue](ptr unsafe.Pointer, sink S, offset uintptr) {
	sink.addFixed(keycodec.OrderedFloat64Key(float64(scalarFieldValue[T](ptr, offset))))
}

func writeStringSliceField[S lenStringValueSink](ptr unsafe.Pointer, sink S, offset uintptr) {
	sink.setLen(addDistinctStringsToSink(sliceFieldValue[string](ptr, offset), sink))
}

func writeBoolSliceField[S lenStringValueSink](ptr unsafe.Pointer, sink S, offset uintptr) {
	sink.setLen(addDistinctBoolValuesToSink(sliceFieldValue[bool](ptr, offset), sink))
}

func writeIntSliceField[S lenFixedValueSink, T signedFieldValue](ptr unsafe.Pointer, sink S, offset uintptr) {
	sink.setLen(addDistinctSignedFixedKeysToSink(sliceFieldValue[T](ptr, offset), sink))
}

func writeUintSliceField[S lenFixedValueSink, T unsignedFieldValue](ptr unsafe.Pointer, sink S, offset uintptr) {
	sink.setLen(addDistinctUnsignedFixedKeysToSink(sliceFieldValue[T](ptr, offset), sink))
}

func writeFloatSliceField[S lenFixedValueSink, T floatFieldValue](ptr unsafe.Pointer, sink S, offset uintptr) {
	sink.setLen(addDistinctFloatFixedKeysToSink(sliceFieldValue[T](ptr, offset), sink))
}

func writePtrTimeField[S nilFixedValueSink](ptr unsafe.Pointer, sink S, offset uintptr) {
	v := ptrFieldValue[time.Time](ptr, offset)
	if v == nil {
		sink.setNil()
		return
	}
	sink.addFixed(keycodec.OrderedInt64Key(v.Unix()))
}

func writeScalarTimeField[S fixedValueSink](ptr unsafe.Pointer, sink S, offset uintptr) {
	sink.addFixed(keycodec.OrderedInt64Key(scalarFieldValue[time.Time](ptr, offset).Unix()))
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

func floatSlicesModified[T floatFieldValue](lhs, rhs []T) bool {
	if len(lhs) != len(rhs) {
		return true
	}
	for i := range lhs {
		if !floatsEqualForIndex(lhs[i], rhs[i]) {
			return true
		}
	}
	return false
}

func valueIndexerScalarReflectAccessorBundle(field string, fieldType reflect.Type, offset uintptr) fieldAccessorBundle {
	switch fieldType.Kind() {
	case reflect.Pointer:
		if isValueReceiverValueIndexerPointer(fieldType) {
			return valueIndexerScalarValuePtrAccessorBundle(field, fieldType, offset)
		}
	default:
		return valueIndexerScalarPtrAccessorBundle(field, fieldType, offset)
	}

	return fieldAccessorBundle{
		unique: func(ptr unsafe.Pointer) (keycodec.IndexLookupKey, bool, bool) {
			fv := reflect.NewAt(fieldType, unsafe.Add(ptr, offset)).Elem()
			return keycodec.IndexLookupString(fv.Interface().(ValueIndexer).IndexingValue()), true, false
		},
		writeBuild: func(ptr unsafe.Pointer, sink BuildSink) {
			fv := reflect.NewAt(fieldType, unsafe.Add(ptr, offset)).Elem()
			sink.addString(fv.Interface().(ValueIndexer).IndexingValue())
		},
		writeChecked: func(ptr unsafe.Pointer, sink BuildSink) error {
			fv := reflect.NewAt(fieldType, unsafe.Add(ptr, offset)).Elem()
			key := fv.Interface().(ValueIndexer).IndexingValue()
			if err := validateStringKey(field, key); err != nil {
				return err
			}
			sink.addString(key)
			return nil
		},
		writeIndex: func(ptr unsafe.Pointer, sink IndexSink) {
			fv := reflect.NewAt(fieldType, unsafe.Add(ptr, offset)).Elem()
			sink.addString(fv.Interface().(ValueIndexer).IndexingValue())
		},
		writeInsert: func(ptr unsafe.Pointer, sink InsertSink) {
			fv := reflect.NewAt(fieldType, unsafe.Add(ptr, offset)).Elem()
			sink.addString(fv.Interface().(ValueIndexer).IndexingValue())
		},
		writeScratch: func(ptr unsafe.Pointer, sink *WriteScratch) {
			fv := reflect.NewAt(fieldType, unsafe.Add(ptr, offset)).Elem()
			sink.addString(fv.Interface().(ValueIndexer).IndexingValue())
		},
		validate: func(ptr unsafe.Pointer) error {
			fv := reflect.NewAt(fieldType, unsafe.Add(ptr, offset)).Elem()
			return validateStringKey(field, fv.Interface().(ValueIndexer).IndexingValue())
		},
		modified: func(v1, v2 unsafe.Pointer) bool {
			fv1 := reflect.NewAt(fieldType, unsafe.Add(v1, offset)).Elem()
			fv2 := reflect.NewAt(fieldType, unsafe.Add(v2, offset)).Elem()
			return fv1.Interface().(ValueIndexer).IndexingValue() != fv2.Interface().(ValueIndexer).IndexingValue()
		},
	}
}

func valueIndexerScalarValuePtrAccessorBundle(field string, fieldType reflect.Type, offset uintptr) fieldAccessorBundle {
	tab := valueIndexerPtrTab(fieldType.Elem())
	return fieldAccessorBundle{
		unique: func(ptr unsafe.Pointer) (keycodec.IndexLookupKey, bool, bool) {
			data := *(*unsafe.Pointer)(unsafe.Add(ptr, offset))
			if data == nil {
				return keycodec.IndexLookupKey{}, true, true
			}
			return keycodec.IndexLookupString(valueIndexerAt(tab, data).IndexingValue()), true, false
		},
		writeBuild: func(ptr unsafe.Pointer, sink BuildSink) {
			data := *(*unsafe.Pointer)(unsafe.Add(ptr, offset))
			if data == nil {
				sink.setNil()
				return
			}
			sink.addString(valueIndexerAt(tab, data).IndexingValue())
		},
		writeChecked: func(ptr unsafe.Pointer, sink BuildSink) error {
			data := *(*unsafe.Pointer)(unsafe.Add(ptr, offset))
			if data == nil {
				sink.setNil()
				return nil
			}
			key := valueIndexerAt(tab, data).IndexingValue()
			if err := validateStringKey(field, key); err != nil {
				return err
			}
			sink.addString(key)
			return nil
		},
		writeIndex: func(ptr unsafe.Pointer, sink IndexSink) {
			data := *(*unsafe.Pointer)(unsafe.Add(ptr, offset))
			if data == nil {
				sink.setNil()
				return
			}
			sink.addString(valueIndexerAt(tab, data).IndexingValue())
		},
		writeInsert: func(ptr unsafe.Pointer, sink InsertSink) {
			data := *(*unsafe.Pointer)(unsafe.Add(ptr, offset))
			if data == nil {
				sink.setNil()
				return
			}
			sink.addString(valueIndexerAt(tab, data).IndexingValue())
		},
		writeScratch: func(ptr unsafe.Pointer, sink *WriteScratch) {
			data := *(*unsafe.Pointer)(unsafe.Add(ptr, offset))
			if data == nil {
				sink.setNil()
				return
			}
			sink.addString(valueIndexerAt(tab, data).IndexingValue())
		},
		validate: func(ptr unsafe.Pointer) error {
			data := *(*unsafe.Pointer)(unsafe.Add(ptr, offset))
			if data == nil {
				return nil
			}
			return validateStringKey(field, valueIndexerAt(tab, data).IndexingValue())
		},
		modified: func(v1, v2 unsafe.Pointer) bool {
			data1 := *(*unsafe.Pointer)(unsafe.Add(v1, offset))
			data2 := *(*unsafe.Pointer)(unsafe.Add(v2, offset))
			if data1 == nil || data2 == nil {
				return data1 != data2
			}
			return valueIndexerAt(tab, data1).IndexingValue() != valueIndexerAt(tab, data2).IndexingValue()
		},
	}
}

func valueIndexerScalarPtrAccessorBundle(field string, fieldType reflect.Type, offset uintptr) fieldAccessorBundle {
	tab := valueIndexerPtrTab(fieldType)
	return fieldAccessorBundle{
		unique: func(ptr unsafe.Pointer) (keycodec.IndexLookupKey, bool, bool) {
			vi := valueIndexerAt(tab, unsafe.Add(ptr, offset))
			return keycodec.IndexLookupString(vi.IndexingValue()), true, false
		},
		writeBuild: func(ptr unsafe.Pointer, sink BuildSink) {
			vi := valueIndexerAt(tab, unsafe.Add(ptr, offset))
			sink.addString(vi.IndexingValue())
		},
		writeChecked: func(ptr unsafe.Pointer, sink BuildSink) error {
			vi := valueIndexerAt(tab, unsafe.Add(ptr, offset))
			key := vi.IndexingValue()
			if err := validateStringKey(field, key); err != nil {
				return err
			}
			sink.addString(key)
			return nil
		},
		writeIndex: func(ptr unsafe.Pointer, sink IndexSink) {
			vi := valueIndexerAt(tab, unsafe.Add(ptr, offset))
			sink.addString(vi.IndexingValue())
		},
		writeInsert: func(ptr unsafe.Pointer, sink InsertSink) {
			vi := valueIndexerAt(tab, unsafe.Add(ptr, offset))
			sink.addString(vi.IndexingValue())
		},
		writeScratch: func(ptr unsafe.Pointer, sink *WriteScratch) {
			vi := valueIndexerAt(tab, unsafe.Add(ptr, offset))
			sink.addString(vi.IndexingValue())
		},
		validate: func(ptr unsafe.Pointer) error {
			vi := valueIndexerAt(tab, unsafe.Add(ptr, offset))
			return validateStringKey(field, vi.IndexingValue())
		},
		modified: func(v1, v2 unsafe.Pointer) bool {
			vi1 := valueIndexerAt(tab, unsafe.Add(v1, offset))
			vi2 := valueIndexerAt(tab, unsafe.Add(v2, offset))
			return vi1.IndexingValue() != vi2.IndexingValue()
		},
	}
}

func valueIndexerSliceReflectAccessorBundle(field string, sliceType reflect.Type, offset uintptr) fieldAccessorBundle {
	elemType := sliceType.Elem()
	switch elemType.Kind() {
	case reflect.Pointer:
		if isValueReceiverValueIndexerPointer(elemType) {
			return valueIndexerSliceValuePtrAccessorBundle(field, sliceType, offset)
		}
	default:
		return valueIndexerSlicePtrAccessorBundle(field, sliceType, offset)
	}

	return fieldAccessorBundle{
		writeBuild: func(ptr unsafe.Pointer, sink BuildSink) {
			fv := reflect.NewAt(sliceType, unsafe.Add(ptr, offset)).Elem()
			sink.setLen(addDistinctValueIndexerStringsToSink(fv, sink))
		},
		writeChecked: func(ptr unsafe.Pointer, sink BuildSink) error {
			fv := reflect.NewAt(sliceType, unsafe.Add(ptr, offset)).Elem()
			checked := checkedBuildSink{sink: sink, field: field}
			addDistinct := addDistinctValueIndexerStringsToSink(fv, &checked)
			checked.setLen(addDistinct)
			return checked.err
		},
		writeIndex: func(ptr unsafe.Pointer, sink IndexSink) {
			fv := reflect.NewAt(sliceType, unsafe.Add(ptr, offset)).Elem()
			sink.setLen(addDistinctValueIndexerStringsToSink(fv, sink))
		},
		writeInsert: func(ptr unsafe.Pointer, sink InsertSink) {
			fv := reflect.NewAt(sliceType, unsafe.Add(ptr, offset)).Elem()
			sink.setLen(addDistinctValueIndexerStringsToSink(fv, sink))
		},
		writeScratch: func(ptr unsafe.Pointer, sink *WriteScratch) {
			fv := reflect.NewAt(sliceType, unsafe.Add(ptr, offset)).Elem()
			sink.setLen(addDistinctValueIndexerStringsToSink(fv, sink))
		},
		validate: func(ptr unsafe.Pointer) error {
			fv := reflect.NewAt(sliceType, unsafe.Add(ptr, offset)).Elem()
			for i := 0; i < fv.Len(); i++ {
				if err := validateStringKey(field, fv.Index(i).Interface().(ValueIndexer).IndexingValue()); err != nil {
					return err
				}
			}
			return nil
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

func valueIndexerSliceValuePtrAccessorBundle(field string, sliceType reflect.Type, offset uintptr) fieldAccessorBundle {
	elemType := sliceType.Elem()
	tab := valueIndexerPtrTab(elemType.Elem())
	size := elemType.Size()
	return fieldAccessorBundle{
		writeBuild: func(ptr unsafe.Pointer, sink BuildSink) {
			data, n := sliceFieldData(ptr, offset)
			sink.setLen(addDistinctValueIndexerValuePtrStringsToSink(data, n, size, tab, sink))
		},
		writeChecked: func(ptr unsafe.Pointer, sink BuildSink) error {
			data, n := sliceFieldData(ptr, offset)
			checked := checkedBuildSink{sink: sink, field: field}
			addDistinct := addDistinctValueIndexerValuePtrStringsToSink(data, n, size, tab, &checked)
			checked.setLen(addDistinct)
			return checked.err
		},
		writeIndex: func(ptr unsafe.Pointer, sink IndexSink) {
			data, n := sliceFieldData(ptr, offset)
			sink.setLen(addDistinctValueIndexerValuePtrStringsToSink(data, n, size, tab, sink))
		},
		writeInsert: func(ptr unsafe.Pointer, sink InsertSink) {
			data, n := sliceFieldData(ptr, offset)
			sink.setLen(addDistinctValueIndexerValuePtrStringsToSink(data, n, size, tab, sink))
		},
		writeScratch: func(ptr unsafe.Pointer, sink *WriteScratch) {
			data, n := sliceFieldData(ptr, offset)
			sink.setLen(addDistinctValueIndexerValuePtrStringsToSink(data, n, size, tab, sink))
		},
		validate: func(ptr unsafe.Pointer) error {
			data, n := sliceFieldData(ptr, offset)
			for i := 0; i < n; i++ {
				elem := *(*unsafe.Pointer)(unsafe.Add(data, uintptr(i)*size))
				if elem == nil {
					continue
				}
				if err := validateStringKey(field, valueIndexerAt(tab, elem).IndexingValue()); err != nil {
					return err
				}
			}
			return nil
		},
		modified: func(v1, v2 unsafe.Pointer) bool {
			data1, n1 := sliceFieldData(v1, offset)
			data2, n2 := sliceFieldData(v2, offset)
			if n1 != n2 {
				return true
			}
			for i := 0; i < n1; i++ {
				elem1 := *(*unsafe.Pointer)(unsafe.Add(data1, uintptr(i)*size))
				elem2 := *(*unsafe.Pointer)(unsafe.Add(data2, uintptr(i)*size))
				if elem1 == nil || elem2 == nil {
					if elem1 != elem2 {
						return true
					}
					continue
				}
				if valueIndexerAt(tab, elem1).IndexingValue() != valueIndexerAt(tab, elem2).IndexingValue() {
					return true
				}
			}
			return false
		},
	}
}

func valueIndexerSlicePtrAccessorBundle(field string, sliceType reflect.Type, offset uintptr) fieldAccessorBundle {
	elemType := sliceType.Elem()
	tab := valueIndexerPtrTab(elemType)
	size := elemType.Size()
	return fieldAccessorBundle{
		writeBuild: func(ptr unsafe.Pointer, sink BuildSink) {
			data, n := sliceFieldData(ptr, offset)
			sink.setLen(addDistinctValueIndexerPtrStringsToSink(data, n, size, tab, sink))
		},
		writeChecked: func(ptr unsafe.Pointer, sink BuildSink) error {
			data, n := sliceFieldData(ptr, offset)
			checked := checkedBuildSink{sink: sink, field: field}
			addDistinct := addDistinctValueIndexerPtrStringsToSink(data, n, size, tab, &checked)
			checked.setLen(addDistinct)
			return checked.err
		},
		writeIndex: func(ptr unsafe.Pointer, sink IndexSink) {
			data, n := sliceFieldData(ptr, offset)
			sink.setLen(addDistinctValueIndexerPtrStringsToSink(data, n, size, tab, sink))
		},
		writeInsert: func(ptr unsafe.Pointer, sink InsertSink) {
			data, n := sliceFieldData(ptr, offset)
			sink.setLen(addDistinctValueIndexerPtrStringsToSink(data, n, size, tab, sink))
		},
		writeScratch: func(ptr unsafe.Pointer, sink *WriteScratch) {
			data, n := sliceFieldData(ptr, offset)
			sink.setLen(addDistinctValueIndexerPtrStringsToSink(data, n, size, tab, sink))
		},
		validate: func(ptr unsafe.Pointer) error {
			data, n := sliceFieldData(ptr, offset)
			for i := 0; i < n; i++ {
				vi := valueIndexerAt(tab, unsafe.Add(data, uintptr(i)*size))
				if err := validateStringKey(field, vi.IndexingValue()); err != nil {
					return err
				}
			}
			return nil
		},
		modified: func(v1, v2 unsafe.Pointer) bool {
			data1, n1 := sliceFieldData(v1, offset)
			data2, n2 := sliceFieldData(v2, offset)
			if n1 != n2 {
				return true
			}
			for i := 0; i < n1; i++ {
				if valueIndexerAt(tab, unsafe.Add(data1, uintptr(i)*size)).IndexingValue() !=
					valueIndexerAt(tab, unsafe.Add(data2, uintptr(i)*size)).IndexingValue() {
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
			unique: func(ptr unsafe.Pointer) (keycodec.IndexLookupKey, bool, bool) {
				v := ptrFieldValue[time.Time](ptr, offset)
				if v == nil {
					return keycodec.IndexLookupKey{}, true, true
				}
				return keycodec.IndexLookupU64(keycodec.OrderedInt64Key(v.Unix())), true, false
			},

			writeBuild:   func(ptr unsafe.Pointer, sink BuildSink) { writePtrTimeField(ptr, sink, offset) },
			writeIndex:   func(ptr unsafe.Pointer, sink IndexSink) { writePtrTimeField(ptr, sink, offset) },
			writeInsert:  func(ptr unsafe.Pointer, sink InsertSink) { writePtrTimeField(ptr, sink, offset) },
			writeScratch: func(ptr unsafe.Pointer, sink *WriteScratch) { writePtrTimeField(ptr, sink, offset) },

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
		unique: func(ptr unsafe.Pointer) (keycodec.IndexLookupKey, bool, bool) {
			return keycodec.IndexLookupU64(keycodec.OrderedInt64Key(scalarFieldValue[time.Time](ptr, offset).Unix())), true, false
		},

		writeBuild:   func(ptr unsafe.Pointer, sink BuildSink) { writeScalarTimeField(ptr, sink, offset) },
		writeIndex:   func(ptr unsafe.Pointer, sink IndexSink) { writeScalarTimeField(ptr, sink, offset) },
		writeInsert:  func(ptr unsafe.Pointer, sink InsertSink) { writeScalarTimeField(ptr, sink, offset) },
		writeScratch: func(ptr unsafe.Pointer, sink *WriteScratch) { writeScalarTimeField(ptr, sink, offset) },

		modified: func(v1, v2 unsafe.Pointer) bool {
			return scalarFieldValue[time.Time](v1, offset).Unix() != scalarFieldValue[time.Time](v2, offset).Unix()
		},
	}
}

func stringFieldAccessorBundle(field string, offset uintptr, ptr bool) fieldAccessorBundle {
	if ptr {
		return fieldAccessorBundle{
			unique: func(ptr unsafe.Pointer) (keycodec.IndexLookupKey, bool, bool) {
				v := ptrFieldValue[string](ptr, offset)
				if v == nil {
					return keycodec.IndexLookupKey{}, true, true
				}
				return keycodec.IndexLookupString(*v), true, false
			},

			writeBuild: func(ptr unsafe.Pointer, sink BuildSink) { writePtrStringField(ptr, sink, offset) },
			writeChecked: func(ptr unsafe.Pointer, sink BuildSink) error {
				v := ptrFieldValue[string](ptr, offset)
				if v == nil {
					sink.setNil()
					return nil
				}
				if err := validateStringKey(field, *v); err != nil {
					return err
				}
				sink.addString(*v)
				return nil
			},
			writeIndex:   func(ptr unsafe.Pointer, sink IndexSink) { writePtrStringField(ptr, sink, offset) },
			writeInsert:  func(ptr unsafe.Pointer, sink InsertSink) { writePtrStringField(ptr, sink, offset) },
			writeScratch: func(ptr unsafe.Pointer, sink *WriteScratch) { writePtrStringField(ptr, sink, offset) },
			validate: func(ptr unsafe.Pointer) error {
				v := ptrFieldValue[string](ptr, offset)
				if v == nil {
					return nil
				}
				return validateStringKey(field, *v)
			},

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
		unique: func(ptr unsafe.Pointer) (keycodec.IndexLookupKey, bool, bool) {
			return keycodec.IndexLookupString(scalarFieldValue[string](ptr, offset)), true, false
		},

		writeBuild: func(ptr unsafe.Pointer, sink BuildSink) { writeScalarStringField(ptr, sink, offset) },
		writeChecked: func(ptr unsafe.Pointer, sink BuildSink) error {
			key := scalarFieldValue[string](ptr, offset)
			if err := validateStringKey(field, key); err != nil {
				return err
			}
			sink.addString(key)
			return nil
		},
		writeIndex:   func(ptr unsafe.Pointer, sink IndexSink) { writeScalarStringField(ptr, sink, offset) },
		writeInsert:  func(ptr unsafe.Pointer, sink InsertSink) { writeScalarStringField(ptr, sink, offset) },
		writeScratch: func(ptr unsafe.Pointer, sink *WriteScratch) { writeScalarStringField(ptr, sink, offset) },
		validate: func(ptr unsafe.Pointer) error {
			return validateStringKey(field, scalarFieldValue[string](ptr, offset))
		},

		modified: func(v1, v2 unsafe.Pointer) bool {
			return scalarFieldValue[string](v1, offset) != scalarFieldValue[string](v2, offset)
		},
	}
}

func boolFieldAccessorBundle(offset uintptr, ptr bool) fieldAccessorBundle {
	if ptr {
		return fieldAccessorBundle{
			unique: func(ptr unsafe.Pointer) (keycodec.IndexLookupKey, bool, bool) {
				v := ptrFieldValue[bool](ptr, offset)
				if v == nil {
					return keycodec.IndexLookupKey{}, true, true
				}
				if *v {
					return keycodec.IndexLookupString("1"), true, false
				}
				return keycodec.IndexLookupString("0"), true, false
			},

			writeBuild:   func(ptr unsafe.Pointer, sink BuildSink) { writePtrBoolField(ptr, sink, offset) },
			writeIndex:   func(ptr unsafe.Pointer, sink IndexSink) { writePtrBoolField(ptr, sink, offset) },
			writeInsert:  func(ptr unsafe.Pointer, sink InsertSink) { writePtrBoolField(ptr, sink, offset) },
			writeScratch: func(ptr unsafe.Pointer, sink *WriteScratch) { writePtrBoolField(ptr, sink, offset) },

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
		unique: func(ptr unsafe.Pointer) (keycodec.IndexLookupKey, bool, bool) {
			if scalarFieldValue[bool](ptr, offset) {
				return keycodec.IndexLookupString("1"), true, false
			}
			return keycodec.IndexLookupString("0"), true, false
		},

		writeBuild:   func(ptr unsafe.Pointer, sink BuildSink) { writeScalarBoolField(ptr, sink, offset) },
		writeIndex:   func(ptr unsafe.Pointer, sink IndexSink) { writeScalarBoolField(ptr, sink, offset) },
		writeInsert:  func(ptr unsafe.Pointer, sink InsertSink) { writeScalarBoolField(ptr, sink, offset) },
		writeScratch: func(ptr unsafe.Pointer, sink *WriteScratch) { writeScalarBoolField(ptr, sink, offset) },

		modified: func(v1, v2 unsafe.Pointer) bool {
			return scalarFieldValue[bool](v1, offset) != scalarFieldValue[bool](v2, offset)
		},
	}
}

func intFieldAccessorBundle[T signedFieldValue](offset uintptr, ptr bool) fieldAccessorBundle {
	if ptr {
		return fieldAccessorBundle{
			unique: func(ptr unsafe.Pointer) (keycodec.IndexLookupKey, bool, bool) {
				v := ptrFieldValue[T](ptr, offset)
				if v == nil {
					return keycodec.IndexLookupKey{}, true, true
				}
				return keycodec.IndexLookupU64(keycodec.OrderedInt64Key(int64(*v))), true, false
			},

			writeBuild: func(ptr unsafe.Pointer, sink BuildSink) {
				writePtrIntField[BuildSink, T](ptr, sink, offset)
			},
			writeIndex: func(ptr unsafe.Pointer, sink IndexSink) {
				writePtrIntField[IndexSink, T](ptr, sink, offset)
			},
			writeInsert: func(ptr unsafe.Pointer, sink InsertSink) {
				writePtrIntField[InsertSink, T](ptr, sink, offset)
			},
			writeScratch: func(ptr unsafe.Pointer, sink *WriteScratch) {
				writePtrIntField[*WriteScratch, T](ptr, sink, offset)
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
		unique: func(ptr unsafe.Pointer) (keycodec.IndexLookupKey, bool, bool) {
			return keycodec.IndexLookupU64(keycodec.OrderedInt64Key(int64(scalarFieldValue[T](ptr, offset)))), true, false
		},

		writeBuild: func(ptr unsafe.Pointer, sink BuildSink) {
			writeScalarIntField[BuildSink, T](ptr, sink, offset)
		},
		writeIndex: func(ptr unsafe.Pointer, sink IndexSink) {
			writeScalarIntField[IndexSink, T](ptr, sink, offset)
		},
		writeInsert: func(ptr unsafe.Pointer, sink InsertSink) {
			writeScalarIntField[InsertSink, T](ptr, sink, offset)
		},
		writeScratch: func(ptr unsafe.Pointer, sink *WriteScratch) {
			writeScalarIntField[*WriteScratch, T](ptr, sink, offset)
		},

		modified: func(v1, v2 unsafe.Pointer) bool {
			return scalarFieldValue[T](v1, offset) != scalarFieldValue[T](v2, offset)
		},
	}
}

func uintFieldAccessorBundle[T unsignedFieldValue](offset uintptr, ptr bool) fieldAccessorBundle {
	if ptr {
		return fieldAccessorBundle{
			unique: func(ptr unsafe.Pointer) (keycodec.IndexLookupKey, bool, bool) {
				v := ptrFieldValue[T](ptr, offset)
				if v == nil {
					return keycodec.IndexLookupKey{}, true, true
				}
				return keycodec.IndexLookupU64(uint64(*v)), true, false
			},

			writeBuild: func(ptr unsafe.Pointer, sink BuildSink) {
				writePtrUintField[BuildSink, T](ptr, sink, offset)
			},
			writeIndex: func(ptr unsafe.Pointer, sink IndexSink) {
				writePtrUintField[IndexSink, T](ptr, sink, offset)
			},
			writeInsert: func(ptr unsafe.Pointer, sink InsertSink) {
				writePtrUintField[InsertSink, T](ptr, sink, offset)
			},
			writeScratch: func(ptr unsafe.Pointer, sink *WriteScratch) {
				writePtrUintField[*WriteScratch, T](ptr, sink, offset)
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
		unique: func(ptr unsafe.Pointer) (keycodec.IndexLookupKey, bool, bool) {
			return keycodec.IndexLookupU64(uint64(scalarFieldValue[T](ptr, offset))), true, false
		},

		writeBuild: func(ptr unsafe.Pointer, sink BuildSink) {
			writeScalarUintField[BuildSink, T](ptr, sink, offset)
		},
		writeIndex: func(ptr unsafe.Pointer, sink IndexSink) {
			writeScalarUintField[IndexSink, T](ptr, sink, offset)
		},
		writeInsert: func(ptr unsafe.Pointer, sink InsertSink) {
			writeScalarUintField[InsertSink, T](ptr, sink, offset)
		},
		writeScratch: func(ptr unsafe.Pointer, sink *WriteScratch) {
			writeScalarUintField[*WriteScratch, T](ptr, sink, offset)
		},

		modified: func(v1, v2 unsafe.Pointer) bool {
			return scalarFieldValue[T](v1, offset) != scalarFieldValue[T](v2, offset)
		},
	}
}

func floatFieldAccessorBundle[T floatFieldValue](offset uintptr, ptr bool) fieldAccessorBundle {
	if ptr {
		return fieldAccessorBundle{
			unique: func(ptr unsafe.Pointer) (keycodec.IndexLookupKey, bool, bool) {
				v := ptrFieldValue[T](ptr, offset)
				if v == nil {
					return keycodec.IndexLookupKey{}, true, true
				}
				return keycodec.IndexLookupU64(keycodec.OrderedFloat64Key(float64(*v))), true, false
			},

			writeBuild: func(ptr unsafe.Pointer, sink BuildSink) {
				writePtrFloatField[BuildSink, T](ptr, sink, offset)
			},
			writeIndex: func(ptr unsafe.Pointer, sink IndexSink) {
				writePtrFloatField[IndexSink, T](ptr, sink, offset)
			},
			writeInsert: func(ptr unsafe.Pointer, sink InsertSink) {
				writePtrFloatField[InsertSink, T](ptr, sink, offset)
			},
			writeScratch: func(ptr unsafe.Pointer, sink *WriteScratch) {
				writePtrFloatField[*WriteScratch, T](ptr, sink, offset)
			},

			modified: func(v1, v2 unsafe.Pointer) bool {
				p1 := ptrFieldValue[T](v1, offset)
				p2 := ptrFieldValue[T](v2, offset)
				if p1 == nil || p2 == nil {
					return p1 != p2
				}
				return !floatsEqualForIndex(*p1, *p2)
			},
		}
	}
	return fieldAccessorBundle{
		unique: func(ptr unsafe.Pointer) (keycodec.IndexLookupKey, bool, bool) {
			return keycodec.IndexLookupU64(keycodec.OrderedFloat64Key(float64(scalarFieldValue[T](ptr, offset)))), true, false
		},

		writeBuild: func(ptr unsafe.Pointer, sink BuildSink) {
			writeScalarFloatField[BuildSink, T](ptr, sink, offset)
		},
		writeIndex: func(ptr unsafe.Pointer, sink IndexSink) {
			writeScalarFloatField[IndexSink, T](ptr, sink, offset)
		},
		writeInsert: func(ptr unsafe.Pointer, sink InsertSink) {
			writeScalarFloatField[InsertSink, T](ptr, sink, offset)
		},
		writeScratch: func(ptr unsafe.Pointer, sink *WriteScratch) {
			writeScalarFloatField[*WriteScratch, T](ptr, sink, offset)
		},

		modified: func(v1, v2 unsafe.Pointer) bool {
			return !floatsEqualForIndex(scalarFieldValue[T](v1, offset), scalarFieldValue[T](v2, offset))
		},
	}
}

func stringSliceAccessorBundle(field string, offset uintptr) fieldAccessorBundle {
	return fieldAccessorBundle{
		writeBuild: func(ptr unsafe.Pointer, sink BuildSink) { writeStringSliceField(ptr, sink, offset) },
		writeChecked: func(ptr unsafe.Pointer, sink BuildSink) error {
			vals := sliceFieldValue[string](ptr, offset)
			if len(vals) == 0 {
				sink.setLen(0)
				return nil
			}
			if len(vals) == 1 {
				if err := validateStringKey(field, vals[0]); err != nil {
					return err
				}
				sink.addString(vals[0])
				sink.setLen(1)
				return nil
			}
			if len(vals) <= smallDistinctLimit {
				distinct := 0
				for i := range vals {
					cur := vals[i]
					seen := false
					for j := 0; j < i; j++ {
						if vals[j] == cur {
							seen = true
							break
						}
					}
					if seen {
						continue
					}
					if err := validateStringKey(field, cur); err != nil {
						return err
					}
					distinct++
					sink.addString(cur)
				}
				sink.setLen(distinct)
				return nil
			}
			seen := stringSetPool.Get()
			distinct := 0
			for i := range vals {
				cur := vals[i]
				if _, ok := seen[cur]; ok {
					continue
				}
				if err := validateStringKey(field, cur); err != nil {
					stringSetPool.Put(seen)
					return err
				}
				seen[cur] = struct{}{}
				distinct++
				sink.addString(cur)
			}
			stringSetPool.Put(seen)
			sink.setLen(distinct)
			return nil
		},
		writeIndex:   func(ptr unsafe.Pointer, sink IndexSink) { writeStringSliceField(ptr, sink, offset) },
		writeInsert:  func(ptr unsafe.Pointer, sink InsertSink) { writeStringSliceField(ptr, sink, offset) },
		writeScratch: func(ptr unsafe.Pointer, sink *WriteScratch) { writeStringSliceField(ptr, sink, offset) },
		validate: func(ptr unsafe.Pointer) error {
			vals := sliceFieldValue[string](ptr, offset)
			for i := range vals {
				if err := validateStringKey(field, vals[i]); err != nil {
					return err
				}
			}
			return nil
		},

		modified: func(v1, v2 unsafe.Pointer) bool {
			return slicesModified(sliceFieldValue[string](v1, offset), sliceFieldValue[string](v2, offset))
		},
	}
}

func boolSliceAccessorBundle(offset uintptr) fieldAccessorBundle {
	return fieldAccessorBundle{
		writeBuild:   func(ptr unsafe.Pointer, sink BuildSink) { writeBoolSliceField(ptr, sink, offset) },
		writeIndex:   func(ptr unsafe.Pointer, sink IndexSink) { writeBoolSliceField(ptr, sink, offset) },
		writeInsert:  func(ptr unsafe.Pointer, sink InsertSink) { writeBoolSliceField(ptr, sink, offset) },
		writeScratch: func(ptr unsafe.Pointer, sink *WriteScratch) { writeBoolSliceField(ptr, sink, offset) },

		modified: func(v1, v2 unsafe.Pointer) bool {
			return slicesModified(sliceFieldValue[bool](v1, offset), sliceFieldValue[bool](v2, offset))
		},
	}
}

func intSliceAccessorBundle[T signedFieldValue](offset uintptr) fieldAccessorBundle {
	return fieldAccessorBundle{
		writeBuild: func(ptr unsafe.Pointer, sink BuildSink) {
			writeIntSliceField[BuildSink, T](ptr, sink, offset)
		},
		writeIndex: func(ptr unsafe.Pointer, sink IndexSink) {
			writeIntSliceField[IndexSink, T](ptr, sink, offset)
		},
		writeInsert: func(ptr unsafe.Pointer, sink InsertSink) {
			writeIntSliceField[InsertSink, T](ptr, sink, offset)
		},
		writeScratch: func(ptr unsafe.Pointer, sink *WriteScratch) {
			writeIntSliceField[*WriteScratch, T](ptr, sink, offset)
		},

		modified: func(v1, v2 unsafe.Pointer) bool {
			return slicesModified(sliceFieldValue[T](v1, offset), sliceFieldValue[T](v2, offset))
		},
	}
}

func uintSliceAccessorBundle[T unsignedFieldValue](offset uintptr) fieldAccessorBundle {
	return fieldAccessorBundle{
		writeBuild: func(ptr unsafe.Pointer, sink BuildSink) {
			writeUintSliceField[BuildSink, T](ptr, sink, offset)
		},
		writeIndex: func(ptr unsafe.Pointer, sink IndexSink) {
			writeUintSliceField[IndexSink, T](ptr, sink, offset)
		},
		writeInsert: func(ptr unsafe.Pointer, sink InsertSink) {
			writeUintSliceField[InsertSink, T](ptr, sink, offset)
		},
		writeScratch: func(ptr unsafe.Pointer, sink *WriteScratch) {
			writeUintSliceField[*WriteScratch, T](ptr, sink, offset)
		},

		modified: func(v1, v2 unsafe.Pointer) bool {
			return slicesModified(sliceFieldValue[T](v1, offset), sliceFieldValue[T](v2, offset))
		},
	}
}

func floatSliceAccessorBundle[T floatFieldValue](offset uintptr) fieldAccessorBundle {
	return fieldAccessorBundle{
		writeBuild: func(ptr unsafe.Pointer, sink BuildSink) {
			writeFloatSliceField[BuildSink, T](ptr, sink, offset)
		},
		writeIndex: func(ptr unsafe.Pointer, sink IndexSink) {
			writeFloatSliceField[IndexSink, T](ptr, sink, offset)
		},
		writeInsert: func(ptr unsafe.Pointer, sink InsertSink) {
			writeFloatSliceField[InsertSink, T](ptr, sink, offset)
		},
		writeScratch: func(ptr unsafe.Pointer, sink *WriteScratch) {
			writeFloatSliceField[*WriteScratch, T](ptr, sink, offset)
		},

		modified: func(v1, v2 unsafe.Pointer) bool {
			return floatSlicesModified(sliceFieldValue[T](v1, offset), sliceFieldValue[T](v2, offset))
		},
	}
}

func buildPatchValueEqualFn(f *Field, fieldType reflect.Type, offset uintptr) PatchValueEqualFn {
	if f.UseVI {
		if f.Slice {
			return nil
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
			return floatPatchValueEqual[float32](offset, f.Ptr)
		case reflect.Float64:
			return floatPatchValueEqual[float64](offset, f.Ptr)
		}
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
			if fieldType == reflect.TypeFor[[]float32]() {
				return floatSlicePatchValueEqual[float32](offset)
			}
			return reflectFloatSlicePatchValueEqual(fieldType, offset)
		case reflect.Float64:
			if fieldType == reflect.TypeFor[[]float64]() {
				return floatSlicePatchValueEqual[float64](offset)
			}
			return reflectFloatSlicePatchValueEqual(fieldType, offset)
		}
		return nil
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
		return floatPatchValueEqual[float32](offset, f.Ptr)
	case reflect.Float64:
		return floatPatchValueEqual[float64](offset, f.Ptr)
	default:
		if typeSupportsPatchFloatEqual(fieldType) {
			return reflectPatchValueEqual(fieldType, offset)
		}
		return nil
	}
}

func buildPatchValueCopyFn(f *Field, fieldType reflect.Type, offset uintptr) PatchValueCopyFn {
	if f.UseVI {
		return nil
	}

	if !f.Ptr && !f.Slice {
		switch fieldType {
		case reflect.TypeFor[string]():
			return scalarPatchValueCopy[string](offset)
		case reflect.TypeFor[bool]():
			return scalarPatchValueCopy[bool](offset)
		case reflect.TypeFor[int]():
			return scalarPatchValueCopy[int](offset)
		case reflect.TypeFor[int8]():
			return scalarPatchValueCopy[int8](offset)
		case reflect.TypeFor[int16]():
			return scalarPatchValueCopy[int16](offset)
		case reflect.TypeFor[int32]():
			return scalarPatchValueCopy[int32](offset)
		case reflect.TypeFor[int64]():
			return scalarPatchValueCopy[int64](offset)
		case reflect.TypeFor[uint]():
			return scalarPatchValueCopy[uint](offset)
		case reflect.TypeFor[uint8]():
			return scalarPatchValueCopy[uint8](offset)
		case reflect.TypeFor[uint16]():
			return scalarPatchValueCopy[uint16](offset)
		case reflect.TypeFor[uint32]():
			return scalarPatchValueCopy[uint32](offset)
		case reflect.TypeFor[uint64]():
			return scalarPatchValueCopy[uint64](offset)
		case reflect.TypeFor[float32]():
			return scalarPatchValueCopy[float32](offset)
		case reflect.TypeFor[float64]():
			return scalarPatchValueCopy[float64](offset)
		}
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

func buildFieldAccessorBundle(f *Field, fieldType reflect.Type, offset uintptr) (fieldAccessorBundle, error) {
	if IsNativeTimeField(f) {
		return timeFieldAccessorBundle(offset, f.Ptr), nil
	}

	if f.UseVI {
		if f.Slice {
			return valueIndexerSliceReflectAccessorBundle(f.DBName, fieldType, offset), nil
		}
		return valueIndexerScalarReflectAccessorBundle(f.DBName, fieldType, offset), nil
	}

	if f.Slice {
		switch f.Kind {
		case reflect.String:
			return stringSliceAccessorBundle(f.DBName, offset), nil
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
		return stringFieldAccessorBundle(f.DBName, offset, f.Ptr), nil
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

func makeIndexedFieldAccessor(vtype reflect.Type, f *Field) (IndexedFieldAccessor, error) {
	acc := IndexedFieldAccessor{
		Name:  f.DBName,
		Field: f,
	}

	fieldType, offset := resolveFieldTypeAndOffset(vtype, f.Index)
	bundle, err := buildFieldAccessorBundle(f, fieldType, offset)
	if err != nil {
		return IndexedFieldAccessor{}, err
	}

	acc.WriteBuild = bundle.writeBuild
	acc.WriteBuildChecked = bundle.writeChecked
	acc.WriteIndex = bundle.writeIndex
	acc.WriteInsert = bundle.writeInsert
	acc.WriteScratch = bundle.writeScratch
	acc.Validate = bundle.validate
	acc.Modified = bundle.modified

	if f.Unique && !f.Slice {
		acc.UniqueGetter = bundle.unique
	}
	return acc, nil
}

func makeIndexedFieldAccessors(vtype reflect.Type, fields map[string]*Field) ([]IndexedFieldAccessor, error) {
	if len(fields) == 0 {
		return nil, nil
	}

	access := make([]IndexedFieldAccessor, 0, len(fields))

	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	slices.Sort(names)

	for _, name := range names {
		f := fields[name]
		acc, err := makeIndexedFieldAccessor(vtype, f)
		if err != nil {
			return nil, err
		}
		acc.Ordinal = len(access)
		access = append(access, acc)
	}

	return access, nil
}
