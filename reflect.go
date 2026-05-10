package rbi

import (
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"time"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
)

type field struct {
	Name      string
	Unique    bool
	IndexKind IndexKind
	Kind      reflect.Kind
	Ptr       bool
	Slice     bool
	UseVI     bool
	KeyKind   fieldWriteKeyKind
	DBName    string
	JSONName  string
	Index     []int
}

// IndexKind declares how a struct field participates in RBI secondary storage.
type IndexKind uint8

const (
	IndexDefault IndexKind = iota
	IndexUnique
	IndexMeasure
)

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

var (
	viType         = reflect.TypeFor[ValueIndexer]()
	nativeTimeType = reflect.TypeFor[time.Time]()
	nativeTimePtr  = reflect.TypeFor[*time.Time]()
)

func isNativeTimeScalarType(t reflect.Type) bool {
	return t != nil && t.ConvertibleTo(nativeTimeType)
}

func isNativeTimePointerType(t reflect.Type) bool {
	return t == nativeTimePtr
}

func isNativeTimeField(f *field) bool {
	return f != nil && !f.Slice && !f.UseVI && f.Kind == reflect.Struct && f.KeyKind == fieldWriteKeysOrderedU64
}

func fieldUsesOrderedNumericKeys(f *field) bool {
	return f != nil && !f.Slice && f.KeyKind == fieldWriteKeysOrderedU64
}

func queryValueToUnixSeconds(v reflect.Value) (int64, bool) {
	if !v.IsValid() || !v.Type().ConvertibleTo(nativeTimeType) {
		return 0, false
	}
	return v.Convert(nativeTimeType).Interface().(time.Time).Unix(), true
}

func inferFieldWriteKeyKind(kind reflect.Kind, useVI, nativeTime bool) fieldWriteKeyKind {
	if nativeTime {
		return fieldWriteKeysOrderedU64
	}
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

type fieldCollector struct {
	options       *Options
	indexFields   map[string]*field
	measureFields map[string]*field
	hasUnique     bool
}

func (collector *fieldCollector) populateFields(t reflect.Type, idx []int) error {
	if idx == nil && collector.options != nil && collector.options.Index != nil {
		return collector.populateFieldsFromOptions(t)
	}
	return collector.populateFieldsFromTags(t, idx)
}

func (collector *fieldCollector) populateFieldsFromTags(t reflect.Type, idx []int) error {

	for i := 0; i < t.NumField(); i++ {

		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		tag := f.Tag.Get("rbi")
		indexKind, use, skip, err := parseRBITag(tag, f.Name)
		if err != nil {
			return err
		}
		if f.Anonymous {
			if indexKind == IndexUnique {
				return fmt.Errorf("unique is not supported for anonymous embedded struct field %v", f.Name)
			}
			if indexKind == IndexMeasure {
				return fmt.Errorf("measure is not supported for anonymous embedded struct field %v", f.Name)
			}
			if f.Type.Kind() == reflect.Struct {
				if skip {
					continue
				}
				newIdx := append(slices.Clone(idx), i)
				if err := collector.populateFieldsFromTags(f.Type, newIdx); err != nil {
					return err
				}
			}
			continue
		}
		if skip || !use {
			continue
		}
		if err := collector.addIndexedField(f, append(slices.Clone(idx), i), indexKind); err != nil {
			return err
		}
	}
	return nil
}

func parseRBITag(tag string, fieldName string) (IndexKind, bool, bool, error) {
	if tag == "" {
		return IndexDefault, false, false, nil
	}
	if strings.Contains(tag, ",") {
		return IndexDefault, false, false, fmt.Errorf("invalid index tag value %q on field %v", tag, fieldName)
	}
	switch tag {
	case "-":
		return IndexDefault, false, true, nil
	case "index":
		return IndexDefault, true, false, nil
	case "unique":
		return IndexUnique, true, false, nil
	case "measure":
		return IndexMeasure, true, false, nil
	default:
		return IndexDefault, false, false, fmt.Errorf("invalid index tag value %q on field %v", tag, fieldName)
	}
}

func validateIndexKind(indexKind IndexKind) error {
	switch indexKind {
	case IndexDefault, IndexUnique, IndexMeasure:
		return nil
	default:
		return fmt.Errorf("invalid IndexKind %d", indexKind)
	}
}

type optionIndexField struct {
	structField reflect.StructField
	index       []int
	dbName      string
}

func (collector *fieldCollector) populateFieldsFromOptions(t reflect.Type) error {
	byGo := make(map[string]optionIndexField, t.NumField())
	byDB := make(map[string]optionIndexField, t.NumField())
	if err := collectOptionIndexFields(t, nil, byGo, byDB); err != nil {
		return err
	}

	seen := make(map[string]string, len(collector.options.Index))
	for name, indexKind := range collector.options.Index {
		if err := validateIndexKind(indexKind); err != nil {
			return fmt.Errorf("field %q: %w", name, err)
		}

		info, ok := byGo[name]
		if dbInfo, dbOK := byDB[name]; dbOK {
			if ok && info.index != nil && !slices.Equal(info.index, dbInfo.index) {
				return fmt.Errorf("index field %q is ambiguous", name)
			}
			info = dbInfo
			ok = true
		}
		if !ok {
			return fmt.Errorf("unknown index field %q", name)
		}
		if info.index == nil {
			return fmt.Errorf("ambiguous Go field name %q", name)
		}

		fieldID := fieldIndexID(info.index)
		if previous, exists := seen[fieldID]; exists {
			return fmt.Errorf("field %v is indexed more than once via %q and %q", info.structField.Name, previous, name)
		}
		seen[fieldID] = name

		if err := collector.addIndexedField(info.structField, info.index, indexKind); err != nil {
			return err
		}
	}
	return nil
}

func collectOptionIndexFields(t reflect.Type, idx []int, byGo map[string]optionIndexField, byDB map[string]optionIndexField) error {
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if !sf.IsExported() {
			continue
		}
		nextIdx := append(slices.Clone(idx), i)
		if sf.Anonymous && sf.Type.Kind() == reflect.Struct {
			if err := collectOptionIndexFields(sf.Type, nextIdx, byGo, byDB); err != nil {
				return err
			}
			continue
		}

		dbName := sf.Name
		hasDBName := false
		if dbTag := sf.Tag.Get("db"); dbTag != "" && dbTag != "-" {
			dbName = dbTag
			hasDBName = true
		}
		info := optionIndexField{
			structField: sf,
			index:       nextIdx,
			dbName:      dbName,
		}
		if existing, ok := byGo[sf.Name]; ok && !slices.Equal(existing.index, nextIdx) {
			byGo[sf.Name] = optionIndexField{}
		} else {
			byGo[sf.Name] = info
		}
		if hasDBName {
			if existing, ok := byDB[dbName]; ok && !slices.Equal(existing.index, nextIdx) {
				return fmt.Errorf("ambiguous db tag %q", dbName)
			}
			byDB[dbName] = info
		}
	}
	return nil
}

func fieldIndexID(index []int) string {
	var b strings.Builder
	for i, v := range index {
		if i > 0 {
			b.WriteByte('.')
		}
		b.WriteString(fmt.Sprint(v))
	}
	return b.String()
}

func (collector *fieldCollector) addIndexedField(sf reflect.StructField, index []int, indexKind IndexKind) error {
	if err := validateIndexKind(indexKind); err != nil {
		return fmt.Errorf("field %v: %w", sf.Name, err)
	}
	f, err := buildFieldDefinition(sf, index, indexKind)
	if err != nil {
		return err
	}
	if indexKind == IndexUnique {
		collector.hasUnique = true
	}
	if indexKind == IndexMeasure {
		if collector.measureFields == nil {
			collector.measureFields = make(map[string]*field)
		}
		collector.measureFields[f.DBName] = f
		return nil
	}
	if collector.indexFields == nil {
		collector.indexFields = make(map[string]*field)
	}
	collector.indexFields[f.DBName] = f // last wins
	return nil
}

func buildFieldDefinition(sf reflect.StructField, index []int, indexKind IndexKind) (*field, error) {
	dbname := sf.Name
	if dbTag := sf.Tag.Get("db"); dbTag != "" && dbTag != "-" {
		dbname = dbTag
	}

	kind := sf.Type.Kind()
	var (
		ptr        bool
		slice      bool
		useVI      bool
		nativeTime bool
	)

	useVI = sf.Type.Implements(viType)
	nativeTime = !useVI && isNativeTimeScalarType(sf.Type)

	if indexKind == IndexMeasure {
		if useVI || nativeTime {
			return nil, fmt.Errorf("measure field %v has unsupported type %v", sf.Name, sf.Type)
		}
		if kind == reflect.Pointer {
			ptr = true
			kind = sf.Type.Elem().Kind()
		}
		switch kind {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Float32, reflect.Float64:
			// OK
		default:
			return nil, fmt.Errorf("measure field %v has unsupported type %v", sf.Name, sf.Type)
		}
		return &field{
			Name:      sf.Name,
			IndexKind: indexKind,
			Kind:      kind,
			Ptr:       ptr,
			KeyKind:   inferFieldWriteKeyKind(kind, false, false),
			DBName:    dbname,
			Index:     slices.Clone(index),
		}, nil
	}

	if kind == reflect.Slice && !useVI {
		slice = true
		elem := sf.Type.Elem()
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
				return nil, fmt.Errorf("slice elements must either be of a simple type or implement the ValueIndexer interface")
			}
		}
	} else if !useVI {
		switch kind {
		case reflect.Struct:
			if !nativeTime {
				return nil, fmt.Errorf("cannot index field %v of type %v, consider implementing ValueIndexer interface", sf.Name, sf.Type)
			}
		case reflect.Array:
			return nil, fmt.Errorf("cannot index field %v of array type, use slice instead", sf.Name)
		case reflect.Invalid,
			reflect.Func,
			reflect.Map,
			reflect.Chan,
			reflect.Complex64,
			reflect.Complex128,
			reflect.Interface,
			reflect.UnsafePointer,
			reflect.Uintptr:
			return nil, fmt.Errorf("cannot index field %v of type %v", sf.Name, sf.Type)
		case reflect.Pointer:
			if isNativeTimePointerType(sf.Type) {
				ptr = true
				kind = reflect.Struct
				nativeTime = true
				break
			}
			ptr = true
			kind = sf.Type.Elem().Kind()
			switch kind {
			case reflect.Struct:
				return nil, fmt.Errorf("cannot index field %v of type %v, consider implementing ValueIndexer interface", sf.Name, sf.Type)
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
				return nil, fmt.Errorf("cannot index field %v of type %v", sf.Name, sf.Type)
			}
		}
	}

	if indexKind == IndexUnique && slice {
		return nil, fmt.Errorf("%v (%v): unique is not supported for slice fields", sf.Name, sf.Type)
	}

	return &field{
		Name:      sf.Name,
		Unique:    indexKind == IndexUnique,
		IndexKind: indexKind,
		Kind:      kind,
		Ptr:       ptr,
		Slice:     slice,
		UseVI:     useVI,
		KeyKind:   inferFieldWriteKeyKind(kind, useVI, nativeTime),
		DBName:    dbname,
		Index:     slices.Clone(index),
	}, nil
}

type (
	uniqueScalarGetterFn        func(ptr unsafe.Pointer) (keycodec.IndexLookupKey, bool, bool)
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
	if db.engine == nil {
		return
	}
	db.forEachModifiedAccessor(db.engine.indexedFieldAccess, v1, v2, fn)
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

func populatePatcher(patchMap map[string]*field, t reflect.Type, idx []int) error {
	for i := 0; i < t.NumField(); i++ {

		rf := t.Field(i)

		if !rf.IsExported() {
			continue
		}

		if rf.Anonymous {
			if rf.Type.Kind() == reflect.Struct {
				nidx := append(slices.Clone(idx), i)
				if err := populatePatcher(patchMap, rf.Type, nidx); err != nil {
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

		patchMap[rf.Name] = f

		if dbTag := rf.Tag.Get("db"); dbTag != "" && dbTag != "-" {
			f.DBName = dbTag
			if existing, ok := patchMap[dbTag]; ok && existing.Name != f.Name {
				return fmt.Errorf("ambiguous db tag '%v' used by fields %v and %v", dbTag, existing.Name, f.Name)
			}
			patchMap[dbTag] = f
		}

		if jsonTag := rf.Tag.Get("json"); jsonTag != "" && jsonTag != "-" {
			tagParts := strings.Split(jsonTag, ",")
			jsonName := tagParts[0]
			if jsonName == "" {
				continue
			}
			f.JSONName = jsonName
			if existing, ok := patchMap[jsonName]; ok && existing.Name != f.Name {
				return fmt.Errorf("ambiguous json tag '%v' used by fields %v and %v", jsonName, existing.Name, f.Name)
			}
			patchMap[jsonName] = f
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
