package schema

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/qir"
)

type Field struct {
	Name       string
	Unique     bool
	IndexKind  IndexKind
	Kind       reflect.Kind
	Ptr        bool
	Slice      bool
	UseVI      bool
	KeyKind    FieldKeyKind
	DBName     string
	QueryName  string
	QueryNames []string
	DBTagged   bool
	JSONName   string
	JSONTagged bool
	Index      []int
}

// IndexKind declares how a struct Field participates in RBI secondary storage.
type IndexKind uint8

const (
	IndexDefault IndexKind = iota
	IndexUnique
	IndexMeasure
)

type FieldKeyKind uint8

const (
	FieldWriteKeysString FieldKeyKind = iota
	FieldWriteKeysOrderedU64
)

const ReservedKeyFieldName = "$key"

// ValueIndexer defines how a Field value is converted into a canonical string
// representation used as an index key in rbi.
//
// A type that implements ValueIndexer is responsible for ensuring that
// IndexingValue returns a valid and stable string for every value that may
// appear in indexed data. This includes handling nil receivers if the type
// is a pointer or otherwise nillable. Nil scalar interface values are
// indexed as null, and nil interface elements in slices do not emit index
// keys. Other nil receivers are passed to IndexingValue.
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
	viType           = reflect.TypeFor[ValueIndexer]()
	ValueIndexerType = viType
	nativeTimeType   = reflect.TypeFor[time.Time]()
)

func isNativeTimeScalarType(t reflect.Type) bool {
	return t != nil && t.ConvertibleTo(nativeTimeType)
}

func isNativeTimePointerType(t reflect.Type) bool {
	return t.Kind() == reflect.Pointer && isNativeTimeScalarType(t.Elem())
}

func isEmbeddedContainerType(t reflect.Type) bool {
	return t.Kind() == reflect.Struct && !isNativeTimeScalarType(t)
}

func isEmbeddedPointerContainerType(t reflect.Type) bool {
	return t.Kind() == reflect.Pointer && t.Elem().Kind() == reflect.Struct && !isNativeTimeScalarType(t.Elem())
}

func IsNativeTimeField(f *Field) bool {
	return f != nil && !f.Slice && !f.UseVI && f.Kind == reflect.Struct && f.KeyKind == FieldWriteKeysOrderedU64
}

func FieldUsesOrderedNumericKeys(f *Field) bool {
	return f != nil && !f.Slice && f.KeyKind == FieldWriteKeysOrderedU64
}

func QueryValueToUnixSeconds(v reflect.Value) (int64, bool) {
	if !v.IsValid() {
		return 0, false
	}
	t := v.Type()
	if t == nativeTimeType {
		return v.Interface().(time.Time).Unix(), true
	}
	if !t.ConvertibleTo(nativeTimeType) {
		return 0, false
	}
	return v.Convert(nativeTimeType).Interface().(time.Time).Unix(), true
}

func UnwrapQueryValue(v reflect.Value) (reflect.Value, bool) {
	for {
		switch v.Kind() {
		case reflect.Interface:
			if v.IsNil() {
				return v, true
			}
			v = v.Elem()
			continue

		case reflect.Pointer:
			if v.IsNil() {
				if v.Type().Implements(viType) {
					return v, false
				}
				return v, true
			}
			v = v.Elem()
			continue
		}
		return v, false
	}
}

func inferFieldWriteKeyKind(kind reflect.Kind, useVI, nativeTime bool) FieldKeyKind {
	if nativeTime {
		return FieldWriteKeysOrderedU64
	}
	if useVI {
		return FieldWriteKeysString
	}
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return FieldWriteKeysOrderedU64
	default:
		return FieldWriteKeysString
	}
}

type fieldCollector struct {
	config        Config
	indexFields   map[string]*Field
	measureFields map[string]*Field
	hasUnique     bool
}

type Config struct {
	Index map[string]IndexKind
}

type Schema struct {
	Fields           map[string]*Field
	MeasureFields    map[string]*Field
	Indexed          []IndexedFieldAccessor
	IndexedByName    IndexedFieldMap
	StringValidation []IndexedFieldAccessor
	Unique           []IndexedFieldAccessor
	Measures         []MeasureFieldAccessor
	MeasuresByName   MeasureFieldMap
	Patch            PatchRuntime
	HasUnique        bool
}

func (s *Schema) HasQueryFields() bool {
	return len(s.Fields) != 0 || len(s.MeasureFields) != 0
}

func (s *Schema) PatchNameTouchesUnique(name string) bool {
	f, ok := s.Patch.Fields[name]
	return ok && f.Unique
}

type IndexedFieldMap map[string]IndexedFieldAccessor

func (m IndexedFieldMap) ResolveField(name string) (qir.FieldInfo, bool) {
	acc, ok := m[name]
	if !ok {
		return qir.FieldInfo{Ordinal: qir.NoFieldOrdinal}, false
	}
	return qir.FieldInfo{Ordinal: acc.Ordinal, Caps: qir.FieldCapAll}, true
}

type MeasureFieldMap map[string]MeasureFieldAccessor

type IndexedFieldAccessor struct {
	Ordinal           int
	PatchOrdinal      int
	Name              string
	Field             *Field
	UniqueGetter      UniqueScalarGetterFn
	WriteBuild        BuildFieldWriteAccessorFn
	WriteBuildChecked BuildFieldWriteCheckedAccessorFn
	WriteIndex        IndexFieldWriteAccessorFn
	WriteInsert       InsertFieldWriteAccessorFn
	WriteScratch      ScratchFieldWriteAccessorFn
	Validate          StringValidationAccessorFn
	Modified          FieldModifiedFn
}

type MeasureFieldAccessor struct {
	Ordinal  int
	Name     string
	Field    *Field
	Kind     MeasureValueKind
	Read     MeasureReadFn
	Modified FieldModifiedFn
}

type PatchFieldAccessor struct {
	Field      *Field
	ValueEqual PatchValueEqualFn
	CopyValue  PatchValueCopyFn
}

type PatchItem struct {
	Name  string
	Value any
}

type PatchRuntime struct {
	Fields map[string]*Field
	Access []PatchFieldAccessor
	typ    reflect.Type
}

func Compile(vtype reflect.Type, config Config) (*Schema, error) {
	collector := fieldCollector{config: config}
	if err := collector.populateFields(vtype, nil); err != nil {
		return nil, fmt.Errorf("failed to populate index fields: %w", err)
	}

	patchFieldCount := len(collector.indexFields) + len(collector.measureFields)
	var patchIndexed map[string]struct{}
	if patchFieldCount != 0 {
		patchIndexed = make(map[string]struct{}, patchFieldCount)
		for _, f := range collector.indexFields {
			patchIndexed[fieldIndexID(f.Index)] = struct{}{}
		}
		for _, f := range collector.measureFields {
			patchIndexed[fieldIndexID(f.Index)] = struct{}{}
		}
	}

	patch, err := makePatchRuntime(vtype, patchIndexed)
	if err != nil {
		return nil, fmt.Errorf("failed to populate patch fields: %w", err)
	}

	access, err := makeIndexedFieldAccessors(vtype, collector.indexFields)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize field accessors: %w", err)
	}
	for i := range access {
		access[i].PatchOrdinal = -1
		queryIndex := access[i].Field.Index
		queryName := access[i].Field.DBName
		for ordinal := range patch.Access {
			f := patch.Access[ordinal].Field
			if slices.Equal(f.Index, queryIndex) {
				access[i].PatchOrdinal = ordinal
				f.setQueryName(queryName)
				if access[i].Field.Unique {
					f.Unique = true
				}
			} else if fieldIndexesOverlap(f.Index, queryIndex) {
				f.addQueryName(queryName)
				if access[i].Field.Unique {
					f.Unique = true
				}
			}
		}
	}
	fieldMap := make(IndexedFieldMap, len(access))
	validationAccess := make([]IndexedFieldAccessor, 0, len(access))
	uniqueAccess := make([]IndexedFieldAccessor, 0, 4)
	for i := range access {
		acc := access[i]
		fieldMap[acc.Name] = acc
		if acc.Validate != nil {
			validationAccess = append(validationAccess, acc)
		}
		if acc.UniqueGetter != nil {
			uniqueAccess = append(uniqueAccess, acc)
		}
	}

	measureAccess, measureMap, err := makeMeasureFieldAccessors(vtype, collector.measureFields)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize measure field accessors: %w", err)
	}
	for i := range measureAccess {
		queryIndex := measureAccess[i].Field.Index
		queryName := measureAccess[i].Field.DBName
		for ordinal := range patch.Access {
			f := patch.Access[ordinal].Field
			if slices.Equal(f.Index, queryIndex) {
				f.setQueryName(queryName)
			} else if fieldIndexesOverlap(f.Index, queryIndex) {
				f.addQueryName(queryName)
			}
		}
	}

	return &Schema{
		Fields:           collector.indexFields,
		MeasureFields:    collector.measureFields,
		Indexed:          access,
		IndexedByName:    fieldMap,
		StringValidation: validationAccess,
		Unique:           uniqueAccess,
		Measures:         measureAccess,
		MeasuresByName:   measureMap,
		Patch:            patch,
		HasUnique:        collector.hasUnique,
	}, nil
}

func (collector *fieldCollector) populateFields(t reflect.Type, idx []int) error {
	if idx == nil && collector.config.Index != nil {
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
		indexKind, use, skip, err := parseTag(tag, f.Name)
		if err != nil {
			return err
		}
		if f.Anonymous {
			if skip {
				continue
			}
			newIdx := append(idx, i)
			if use {
				if err = collector.addIndexedField(f, newIdx, indexKind); err != nil {
					return err
				}
				continue
			}
			if isEmbeddedContainerType(f.Type) {
				if err = collector.populateFieldsFromTags(f.Type, newIdx); err != nil {
					return err
				}
			} else if isEmbeddedPointerContainerType(f.Type) {
				hasIndexTags, err := hasEmbeddedIndexTags(f.Type.Elem(), nil)
				if err != nil {
					return err
				}
				if hasIndexTags {
					return fmt.Errorf("anonymous embedded pointer field %v cannot promote rbi tags, embed %v by value", f.Name, f.Type.Elem())
				}
			}
			continue
		}
		if skip || !use {
			continue
		}
		if err := collector.addIndexedField(f, append(idx, i), indexKind); err != nil {
			return err
		}
	}
	return nil
}

func parseTag(tag string, fieldName string) (IndexKind, bool, bool, error) {
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

func ValidateIndexKind(indexKind IndexKind) error {
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
	depth       int
}

func (collector *fieldCollector) populateFieldsFromOptions(t reflect.Type) error {
	byGo := make(map[string]optionIndexField, t.NumField())
	byDB := make(map[string]optionIndexField, t.NumField())
	if err := collectOptionIndexFields(t, nil, byGo, byDB); err != nil {
		return err
	}

	seen := make(map[string]string, len(collector.config.Index))
	for name, indexKind := range collector.config.Index {
		if name == ReservedKeyFieldName {
			return fmt.Errorf("reserved index field %q", name)
		}
		if err := ValidateIndexKind(indexKind); err != nil {
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
		nextIdx := append(idx, i)
		if sf.Anonymous && isEmbeddedContainerType(sf.Type) {
			if err := collectOptionIndexFields(sf.Type, nextIdx, byGo, byDB); err != nil {
				return err
			}
			if !sf.Type.Implements(viType) {
				continue
			}
		}

		dbName := sf.Name
		hasDBName := false
		if dbTag := sf.Tag.Get("db"); dbTag != "" && dbTag != "-" {
			if dbTag == ReservedKeyFieldName {
				return fmt.Errorf("field %v uses reserved db tag %q", sf.Name, dbTag)
			}
			dbName = dbTag
			hasDBName = true
		}
		info := optionIndexField{
			structField: sf,
			index:       slices.Clone(nextIdx),
			dbName:      dbName,
			depth:       len(nextIdx),
		}
		if existing, ok := byGo[sf.Name]; !ok || slices.Equal(existing.index, nextIdx) || existing.depth > info.depth {
			byGo[sf.Name] = info
		} else if existing.depth == info.depth {
			byGo[sf.Name] = optionIndexField{depth: info.depth}
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
	if len(index) == 1 {
		return strconv.Itoa(index[0])
	}
	var scratch [64]byte
	buf := scratch[:0]
	for i := range index {
		if i != 0 {
			buf = append(buf, '.')
		}
		buf = strconv.AppendInt(buf, int64(index[i]), 10)
	}
	return string(buf)
}

type embeddedIndexTagScan struct {
	typ  reflect.Type
	prev *embeddedIndexTagScan
}

func hasEmbeddedIndexTags(t reflect.Type, prev *embeddedIndexTagScan) (bool, error) {
	for cur := prev; cur != nil; cur = cur.prev {
		if cur.typ == t {
			return false, nil
		}
	}
	frame := embeddedIndexTagScan{typ: t, prev: prev}

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		_, use, skip, err := parseTag(f.Tag.Get("rbi"), f.Name)
		if err != nil {
			return false, err
		}
		if use {
			return true, nil
		}
		if skip {
			continue
		}
		if f.Anonymous {
			if isEmbeddedContainerType(f.Type) {
				hasIndexTags, err := hasEmbeddedIndexTags(f.Type, &frame)
				if err != nil {
					return false, err
				}
				if hasIndexTags {
					return true, nil
				}
			} else if isEmbeddedPointerContainerType(f.Type) {
				hasIndexTags, err := hasEmbeddedIndexTags(f.Type.Elem(), &frame)
				if err != nil {
					return false, err
				}
				if hasIndexTags {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

func fieldIndexesOverlap(a, b []int) bool {
	if len(a) > len(b) {
		a, b = b, a
	}
	return slices.Equal(b[:len(a)], a)
}

func (f *Field) setQueryName(queryName string) {
	if f.QueryName == queryName {
		return
	}
	previous := f.QueryName
	f.QueryName = queryName
	if previous != "" {
		f.addQueryName(previous)
	}
}

func (f *Field) addQueryName(queryName string) {
	if f.QueryName == "" {
		f.QueryName = queryName
		return
	}
	if f.QueryName == queryName {
		return
	}
	for i := range f.QueryNames {
		if f.QueryNames[i] == queryName {
			return
		}
	}
	f.QueryNames = append(f.QueryNames, queryName)
}

func (collector *fieldCollector) addIndexedField(sf reflect.StructField, index []int, indexKind IndexKind) error {
	if err := ValidateIndexKind(indexKind); err != nil {
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
			collector.measureFields = make(map[string]*Field)
		}
		if existing, ok := collector.measureFields[f.DBName]; ok {
			return fmt.Errorf("ambiguous measure field name %q used by fields %v and %v", f.DBName, existing.Name, f.Name)
		}
		collector.measureFields[f.DBName] = f
		return nil
	}
	if collector.indexFields == nil {
		collector.indexFields = make(map[string]*Field)
	}
	if existing, ok := collector.indexFields[f.DBName]; ok {
		return fmt.Errorf("ambiguous index field name %q used by fields %v and %v", f.DBName, existing.Name, f.Name)
	}
	collector.indexFields[f.DBName] = f
	return nil
}

func buildFieldDefinition(sf reflect.StructField, index []int, indexKind IndexKind) (*Field, error) {
	dbname := sf.Name
	if dbTag := sf.Tag.Get("db"); dbTag != "" && dbTag != "-" {
		dbname = dbTag
	}
	if dbname == ReservedKeyFieldName {
		return nil, fmt.Errorf("field %v uses reserved db tag %q", sf.Name, dbname)
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
		return &Field{
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

	return &Field{
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
	UniqueScalarGetterFn             func(ptr unsafe.Pointer) (keycodec.IndexLookupKey, bool, bool)
	BuildFieldWriteAccessorFn        func(ptr unsafe.Pointer, sink BuildSink)
	BuildFieldWriteCheckedAccessorFn func(ptr unsafe.Pointer, sink BuildSink) error
	IndexFieldWriteAccessorFn        func(ptr unsafe.Pointer, sink IndexSink)
	InsertFieldWriteAccessorFn       func(ptr unsafe.Pointer, sink InsertSink)
	ScratchFieldWriteAccessorFn      func(ptr unsafe.Pointer, sink *WriteScratch)
	StringValidationAccessorFn       func(ptr unsafe.Pointer) error
)

func populatePatcher(patchMap map[string]*Field, patchFields *[]*Field, ambiguous map[string]int, t reflect.Type, idx []int, single []int, patchIndexed map[string]struct{}, jsonHidden bool) error {
	for i := 0; i < t.NumField(); i++ {

		rf := t.Field(i)

		if !rf.IsExported() {
			continue
		}

		if rf.Anonymous && rf.Type.Kind() == reflect.Struct {
			recurse := isEmbeddedContainerType(rf.Type)
			if recurse {
				nidx := append(idx, i)
				selfIndexed := false
				if patchIndexed != nil {
					_, selfIndexed = patchIndexed[fieldIndexID(nidx)]
				}
				if err := populatePatcher(patchMap, patchFields, ambiguous, rf.Type, nidx, single, patchIndexed, jsonHidden || rf.Tag.Get("json") == "-"); err != nil {
					return err
				}
				if !selfIndexed {
					continue
				}
			}
		}

		var fieldIndex []int
		if len(idx) == 0 {
			single[i] = i
			fieldIndex = single[i : i+1 : i+1]
		} else {
			fieldIndex = append(slices.Clone(idx), i)
		}
		f := &Field{
			Name:     rf.Name,
			DBName:   rf.Name,
			JSONName: rf.Name,
			Kind:     rf.Type.Kind(),
			Index:    fieldIndex,
		}
		if jsonHidden {
			f.JSONName = ""
		}
		*patchFields = append(*patchFields, f)

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

		fieldDepth := len(f.Index)
		dbNameUnsafe := false
		jsonNameUnsafe := false

		if depth, ok := ambiguous[rf.Name]; ok {
			if fieldDepth < depth {
				delete(ambiguous, rf.Name)
				patchMap[rf.Name] = f
			} else {
				dbNameUnsafe = true
				jsonNameUnsafe = true
			}

		} else {
			if existing, ok := patchMap[rf.Name]; ok {
				if existing.Name != f.Name {
					return fmt.Errorf("ambiguous patch field name '%v' used by fields %v and %v", rf.Name, existing.Name, f.Name)
				}
				existingDepth := len(existing.Index)
				switch {

				case existingDepth < fieldDepth:
					dbNameUnsafe = true
					jsonNameUnsafe = true

				case existingDepth > fieldDepth:
					if !existing.DBTagged && existing.DBName == existing.Name {
						existing.DBName = ""
					}
					if !existing.JSONTagged && existing.JSONName == existing.Name {
						existing.JSONName = ""
					}
					patchMap[rf.Name] = f

				case !slices.Equal(existing.Index, f.Index):
					if !existing.DBTagged && existing.DBName == existing.Name {
						existing.DBName = ""
					}
					if !existing.JSONTagged && existing.JSONName == existing.Name {
						existing.JSONName = ""
					}
					delete(patchMap, rf.Name)
					ambiguous[rf.Name] = fieldDepth
					dbNameUnsafe = true
					jsonNameUnsafe = true

				default:
					patchMap[rf.Name] = f
				}
			} else {
				patchMap[rf.Name] = f
			}
		}

		if dbTag := rf.Tag.Get("db"); dbTag != "" && dbTag != "-" {
			if dbTag == ReservedKeyFieldName {
				return fmt.Errorf("field %v uses reserved db tag %q", rf.Name, dbTag)
			}
			if _, ok := ambiguous[dbTag]; ok {
				return fmt.Errorf("ambiguous db tag '%v' used by field %v and promoted field name", dbTag, f.Name)
			}
			f.DBName = dbTag
			f.DBTagged = true
			if existing, ok := patchMap[dbTag]; ok && existing != f {
				return fmt.Errorf("ambiguous db tag '%v' used by fields %v and %v", dbTag, existing.Name, f.Name)
			}
			patchMap[dbTag] = f
		}

		if !jsonHidden {
			jsonTag := rf.Tag.Get("json")
			if jsonTag == "-" {
				f.JSONName = ""
			} else if jsonTag != "" {
				jsonName, _, _ := strings.Cut(jsonTag, ",")
				if jsonName != "" {
					if jsonName == ReservedKeyFieldName {
						return fmt.Errorf("field %v uses reserved json tag %q", rf.Name, jsonName)
					}
					if _, ok := ambiguous[jsonName]; ok {
						return fmt.Errorf("ambiguous json tag '%v' used by field %v and promoted field name", jsonName, f.Name)
					}
					f.JSONName = jsonName
					f.JSONTagged = true
					if existing, ok := patchMap[jsonName]; ok && existing != f {
						return fmt.Errorf("ambiguous json tag '%v' used by fields %v and %v", jsonName, existing.Name, f.Name)
					}
					patchMap[jsonName] = f
				}
			}
		}
		if dbNameUnsafe && !f.DBTagged && f.DBName == f.Name {
			f.DBName = ""
		}
		if jsonNameUnsafe && !f.JSONTagged && f.JSONName == f.Name {
			f.JSONName = ""
		}
	}
	return nil
}

func makePatchRuntime(vtype reflect.Type, patchIndexed map[string]struct{}) (PatchRuntime, error) {
	patchMap := make(map[string]*Field, vtype.NumField()*2)
	patchFields := make([]*Field, 0, vtype.NumField())
	if err := populatePatcher(patchMap, &patchFields, make(map[string]int), vtype, nil, make([]int, vtype.NumField()), patchIndexed, false); err != nil {
		return PatchRuntime{}, err
	}
	for _, f := range patchFields {
		if f.DBName != "" {
			existing := patchMap[f.DBName]
			if existing != f {
				if f.DBTagged {
					if existing == nil {
						return PatchRuntime{}, fmt.Errorf("ambiguous db tag '%v' used by field %v and promoted field name", f.DBName, f.Name)
					}
					return PatchRuntime{}, fmt.Errorf("ambiguous db tag '%v' used by fields %v and %v", f.DBName, f.Name, existing.Name)
				}
				f.DBName = ""
			}
		}
		if f.JSONName != "" {
			existing := patchMap[f.JSONName]
			if existing != f {
				if f.JSONTagged {
					if existing == nil {
						return PatchRuntime{}, fmt.Errorf("ambiguous json tag '%v' used by field %v and promoted field name", f.JSONName, f.Name)
					}
					return PatchRuntime{}, fmt.Errorf("ambiguous json tag '%v' used by fields %v and %v", f.JSONName, f.Name, existing.Name)
				}
				f.JSONName = ""
			}
		}
	}
	access := makePatchFieldAccessors(vtype, patchFields)
	return PatchRuntime{
		Fields: patchMap,
		Access: access,
		typ:    vtype,
	}, nil
}

func makePatchFieldAccessors(vtype reflect.Type, fields []*Field) []PatchFieldAccessor {
	if len(fields) == 0 {
		return nil
	}

	patchFields := fields
	slices.SortFunc(patchFields, func(a, b *Field) int {
		return slices.Compare(a.Index, b.Index)
	})

	access := make([]PatchFieldAccessor, 0, len(patchFields))

	var prev *Field
	for _, f := range patchFields {
		if prev != nil && slices.Equal(prev.Index, f.Index) {
			continue
		}
		prev = f

		acc := PatchFieldAccessor{Field: f}
		fieldType, offset := resolveFieldTypeAndOffset(vtype, f.Index)
		acc.ValueEqual = buildPatchValueEqualFn(f, fieldType, offset)
		acc.CopyValue = buildPatchValueCopyFn(f, fieldType, offset)

		access = append(access, acc)
	}

	return access
}
