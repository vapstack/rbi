package rbi

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"strings"
	"unsafe"
)

type field struct {
	Name   string
	Unique bool // currently not used
	Kind   reflect.Kind
	Ptr    bool
	Slice  bool
	UseVI  bool
	DBName string
	Index  []int
}

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

func (db *DB[K, V]) populateFields(t reflect.Type, idx []int) error {

	for i := 0; i < t.NumField(); i++ {

		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		if f.Anonymous {
			if f.Type.Kind() == reflect.Struct {
				newIdx := append(append([]int{}, idx...), i)
				if err := db.populateFields(f.Type, newIdx); err != nil {
					return err
				}
			}
			continue
		}
		dbname := f.Name
		if dbTag := f.Tag.Get("db"); dbTag != "" {
			if dbTag == "-" {
				continue
			}
			dbname = dbTag
		}
		tag := f.Tag.Get("rbi")
		if tag == "" {
			tag = f.Tag.Get("dbi")
		}
		if tag == "-" {
			continue
		}

		kind := f.Type.Kind()

		var (
			ptr   bool
			slice bool
			useVI bool
		)

		useVI = f.Type.Implements(viType)

		if !useVI {

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

			case reflect.Slice:
				slice = true
				elem := f.Type.Elem()
				kind = elem.Kind()
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
					useVI = elem.Implements(viType)
					if !useVI {
						return fmt.Errorf("slice elements must either be of a simple type or implement the ValueIndexer interface")
					}
				}
			}
		}

		unique := false
		if tag != "" {
			for _, p := range strings.Split(tag, ",") {
				if strings.TrimSpace(p) == "unique" {
					if slice {
						return fmt.Errorf("%v (%v): unique is not supported for slice fields", f.Name, f.Type)
					}
					db.hasUnique = true
					unique = true
					break
				}
			}
		}

		db.fields[dbname] = &field{ // last wins
			Name:   f.Name,
			Unique: unique,
			Kind:   kind,
			Ptr:    ptr,
			Slice:  slice,
			UseVI:  useVI,
			DBName: dbname,
			Index:  append(append([]int{}, idx...), i),
		}
	}
	return nil
}

type getterFn func(ptr unsafe.Pointer) (string, []string, bool)

var nilValue = "\x00NIL"

func (db *DB[K, V]) makeGetter(f *field) (getterFn, error) {

	if f.UseVI {
		if f.Slice {
			return db.fieldGetter(f.Index, func(fv reflect.Value) (single string, multi []string) {
				if fv.Len() == 0 {
					return "", nil
				}
				s := make([]string, fv.Len())
				for i := 0; i < len(s); i++ {
					s[i] = fv.Index(i).Interface().(ValueIndexer).IndexingValue()
				}
				return "", s
			}), nil
		} else {
			return db.fieldGetter(f.Index, func(fv reflect.Value) (single string, multi []string) {
				return fv.Interface().(ValueIndexer).IndexingValue(), nil
			}), nil
		}
	}

	var fn func(fv reflect.Value) (string, []string)

	switch f.Kind {

	case reflect.String:
		if f.Slice {
			fn = func(fv reflect.Value) (string, []string) {
				if fv.Len() == 0 {
					return "", nil
				}
				s := make([]string, fv.Len())
				for i := 0; i < len(s); i++ {
					s[i] = fv.Index(i).String()
				}
				return "", s
			}
		} else {
			fn = func(fv reflect.Value) (string, []string) {
				if f.Ptr {
					if fv.IsNil() {
						return nilValue, nil
					}
					fv = fv.Elem()
				}
				return fv.String(), nil
			}
		}

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if f.Slice {
			fn = func(fv reflect.Value) (string, []string) {
				if fv.Len() == 0 {
					return "", nil
				}
				s := make([]string, fv.Len())
				for i := 0; i < len(s); i++ {
					s[i] = uint64ByteStr(fv.Index(i).Uint())
				}
				return "", s
			}
		} else {
			fn = func(fv reflect.Value) (string, []string) {
				if f.Ptr {
					if fv.IsNil() {
						return nilValue, nil
					}
					fv = fv.Elem()
				}
				return uint64ByteStr(fv.Uint()), nil
			}
		}

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if f.Slice {
			fn = func(fv reflect.Value) (string, []string) {
				if fv.Len() == 0 {
					return "", nil
				}
				s := make([]string, fv.Len())
				for i := 0; i < len(s); i++ {
					s[i] = int64ByteStr(fv.Index(i).Int())
				}
				return "", s
			}
		} else {
			fn = func(fv reflect.Value) (string, []string) {
				if f.Ptr {
					if fv.IsNil() {
						return nilValue, nil
					}
					fv = fv.Elem()
				}
				return int64ByteStr(fv.Int()), nil
			}
		}

	case reflect.Bool:
		if f.Slice {
			fn = func(fv reflect.Value) (string, []string) {
				if fv.Len() == 0 {
					return "", nil
				}
				s := make([]string, fv.Len())
				for i := 0; i < len(s); i++ {
					if fv.Index(i).Bool() {
						s[i] = "1"
					} else {
						s[i] = "0"
					}
				}
				return "", s
			}
		} else {
			fn = func(fv reflect.Value) (string, []string) {
				if f.Ptr {
					if fv.IsNil() {
						return nilValue, nil
					}
					fv = fv.Elem()
				}
				if fv.Bool() {
					return "1", nil
				} else {
					return "0", nil
				}
			}
		}

	case reflect.Float32, reflect.Float64:
		if f.Slice {
			fn = func(fv reflect.Value) (string, []string) {
				if fv.Len() == 0 {
					return "", nil
				}
				s := make([]string, fv.Len())
				for i := 0; i < len(s); i++ {
					s[i] = float64ByteStr(fv.Index(i).Float())
				}
				return "", s
			}
		} else {
			fn = func(fv reflect.Value) (string, []string) {
				if f.Ptr {
					if fv.IsNil() {
						return nilValue, nil
					}
					fv = fv.Elem()
				}
				return float64ByteStr(fv.Float()), nil
			}
		}

	default:
		return nil, fmt.Errorf("unknown or unsupported field kind: %v", f.Kind)
	}

	return db.fieldGetter(f.Index, fn), nil
}

func (db *DB[K, V]) fieldGetter(idx []int, sub func(fv reflect.Value) (string, []string)) getterFn {
	return func(ptr unsafe.Pointer) (string, []string, bool) {
		if ptr == nil {
			return "", nil, false
		}
		fv := reflect.NewAt(db.vtype, ptr).Elem()
		if !fv.IsValid() {
			return "", nil, false
		}
		single, multi := sub(fv.FieldByIndex(idx))
		return single, multi, true
	}
}

func (db *DB[K, V]) getModifiedIndexedFields(v1 *V, v2 *V) []string {

	if v1 == nil || v2 == nil {
		return db.fieldSlice
	}

	rv1 := reflect.ValueOf(v1).Elem()
	rv2 := reflect.ValueOf(v2).Elem()

	var modified []string
	for _, f := range db.fields {

		fv1 := rv1.FieldByIndex(f.Index)
		fv2 := rv2.FieldByIndex(f.Index)

		if f.Slice {
			l := fv1.Len()
			if l != fv2.Len() {
				modified = append(modified, f.DBName)
			} else {
				for i := 0; i < l; i++ {
					sv1 := fv1.Index(i)
					sv2 := fv2.Index(i)

					if f.UseVI {
						// if canNil(f.Kind) {
						// 	sv1nil := sv1.IsNil()
						// 	sv2nil := sv2.IsNil()
						// 	if sv1nil != sv2nil {
						// 		modified = append(modified, f.DBName)
						// 		break
						// 	}
						// 	if sv1nil && sv2nil {
						// 		continue
						// 	}
						// }
						if sv1.Interface().(ValueIndexer).IndexingValue() != sv2.Interface().(ValueIndexer).IndexingValue() {
							modified = append(modified, f.DBName)
							break
						}
					} else {
						switch f.Kind {
						case reflect.String:
							if sv1.String() != sv2.String() {
								modified = append(modified, f.DBName)
								break
							}
						case reflect.Bool:
							if sv1.Bool() != sv2.Bool() {
								modified = append(modified, f.DBName)
								break
							}
						case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
							if sv1.Int() != sv2.Int() {
								modified = append(modified, f.DBName)
								break
							}
						case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
							if sv1.Uint() != sv2.Uint() {
								modified = append(modified, f.DBName)
								break
							}
						case reflect.Float32, reflect.Float64:
							if sv1.Float() != sv2.Float() {
								modified = append(modified, f.DBName)
								break
							}
						default:
							// should not be, but anyway
							if !reflect.DeepEqual(sv1.Interface(), sv2.Interface()) {
								modified = append(modified, f.DBName)
								break
							}
						}
					}
				}
			}
			continue
		}

		if f.UseVI {
			// if canNil(f.Kind) {
			// 	fv1nil := fv1.IsNil()
			// 	fv2nil := fv2.IsNil()
			// 	if fv1nil != fv2nil {
			// 		modified = append(modified, f.DBName)
			// 		continue
			// 	}
			// 	if fv1nil && fv2nil {
			// 		continue
			// 	}
			// }
			if fv1.Interface().(ValueIndexer).IndexingValue() != fv2.Interface().(ValueIndexer).IndexingValue() {
				modified = append(modified, f.DBName)
			}
			continue
		}

		if f.Ptr {
			v1nil := fv1.IsNil()
			v2nil := fv2.IsNil()
			if v1nil != v2nil {
				modified = append(modified, f.DBName)
				continue
			}
			if v1nil && v2nil {
				continue
			}
			fv1 = fv1.Elem()
			fv2 = fv2.Elem()
		}

		switch f.Kind {
		case reflect.String:
			if fv1.String() != fv2.String() {
				modified = append(modified, f.DBName)
			}
		case reflect.Bool:
			if fv1.Bool() != fv2.Bool() {
				modified = append(modified, f.DBName)
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if fv1.Int() != fv2.Int() {
				modified = append(modified, f.DBName)
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			if fv1.Uint() != fv2.Uint() {
				modified = append(modified, f.DBName)
			}
		case reflect.Float32, reflect.Float64:
			if fv1.Float() != fv2.Float() {
				modified = append(modified, f.DBName)
			}
		default:
			if !reflect.DeepEqual(fv1.Interface(), fv2.Interface()) {
				modified = append(modified, f.DBName)
			}
		}
	}
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
				return fmt.Errorf("rbi: field %v: cannot assign nil to non-nillable field", p.Name)
			}
			continue
		}

		if err := db.setReflectValue(fv, p.Value); err != nil {
			return fmt.Errorf("rbi: field %v: %w", p.Name, err)
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

	if srcKind == reflect.Float32 || srcKind == reflect.Float64 {
		f64 := sv.Float()

		if dstKind >= reflect.Int && dstKind <= reflect.Int64 {

			if f64 != math.Trunc(f64) {
				return fmt.Errorf("cannot assign float %v to int field (loss of precision)", f64)
			}
			i64 := int64(f64)
			if fv.OverflowInt(i64) {
				return fmt.Errorf("value %v overflows int field (%v)", i64, fv.Type())
			}

			fv.SetInt(i64)

			return nil
		}

		if dstKind >= reflect.Uint && dstKind <= reflect.Uint64 {

			if f64 < 0 || f64 != math.Trunc(f64) {
				return fmt.Errorf("cannot assign float %v to uint field", f64)
			}
			u64 := uint64(f64)
			if fv.OverflowUint(u64) {
				return fmt.Errorf("value %v overflows uint field (%v)", u64, fv.Type())
			}

			fv.SetUint(u64)

			return nil
		}
	}

	if srcKind >= reflect.Int && srcKind <= reflect.Int64 {

		if dstKind == reflect.Float32 || dstKind == reflect.Float64 {
			f64 := float64(sv.Int())
			if fv.OverflowFloat(f64) {
				return fmt.Errorf("value %v overflows float field (%v)", f64, fv.Type())
			}

			fv.SetFloat(f64)

			return nil
		}
	}

	if srcKind >= reflect.Uint && srcKind <= reflect.Uint64 {

		if dstKind == reflect.Float32 || dstKind == reflect.Float64 {
			f64 := float64(sv.Uint())
			if fv.OverflowFloat(f64) {
				return fmt.Errorf("value %v overflows float field (%v)", f64, fv.Type())
			}

			fv.SetFloat(f64)

			return nil
		}
	}

	if sv.Type().ConvertibleTo(fv.Type()) {
		fv.Set(sv.Convert(fv.Type()))
		return nil
	}

	return fmt.Errorf("type mismatch: cannot convert %v to %v", sv.Type(), fv.Type())
}

func (db *DB[K, V]) populatePatcher(t reflect.Type, idx []int) error {
	for i := 0; i < t.NumField(); i++ {

		rf := t.Field(i)

		if !rf.IsExported() {
			continue
		}

		if rf.Anonymous {
			if rf.Type.Kind() == reflect.Struct {
				nidx := append(append([]int{}, idx...), i)
				if err := db.populatePatcher(rf.Type, nidx); err != nil {
					return err
				}
			}
			continue
		}

		f := &field{
			Name:  rf.Name,
			Kind:  rf.Type.Kind(),
			Index: append(append([]int{}, idx...), i),
		}

		if f.Kind == reflect.Pointer {
			f.Ptr = true
			f.Kind = rf.Type.Elem().Kind()
		}

		db.patchMap[rf.Name] = f

		if dbTag := rf.Tag.Get("db"); dbTag != "" && dbTag != "-" {
			if existing, ok := db.patchMap[dbTag]; ok && existing.Name != f.Name {
				return fmt.Errorf(
					"rbi: ambiguous db tag '%v' used by fields %v and %v",
					dbTag, existing.Name, f.Name,
				)
			}
			db.patchMap[dbTag] = f
		}

		if jsonTag := rf.Tag.Get("json"); jsonTag != "" && jsonTag != "-" {
			tagParts := strings.Split(jsonTag, ",")
			jsonName := tagParts[0]
			if jsonName == "" {
				continue
			}
			if existing, ok := db.patchMap[jsonName]; ok && existing.Name != f.Name {
				return fmt.Errorf(
					"rbi: ambiguous json tag '%v' used by fields %v and %v",
					jsonName, existing.Name, f.Name,
				)
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
	visited := make(map[uintptr]reflect.Value)
	origin := reflect.ValueOf(src)
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

	case reflect.Ptr:
		ptr := reflect.New(origin.Elem().Type())
		visited[origin.Pointer()] = ptr
		clone := deepCopy(origin.Elem(), visited)
		ptr.Elem().Set(clone)
		return ptr

	case reflect.Struct:
		s := reflect.New(origin.Type()).Elem()
		for i := 0; i < origin.NumField(); i++ {
			if s.Field(i).CanSet() {
				clone := deepCopy(origin.Field(i), visited)
				s.Field(i).Set(clone)
			}
		}
		return s

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

func uint64Bytes(v uint64) []byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], v)
	return key[:]
}

func uint64ByteStr(v uint64) string {
	b := uint64Bytes(v)
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func int64ByteStr(v int64) string {
	return uint64ByteStr(uint64(v) ^ (uint64(1) << 63))
}

func float64ByteStr(f float64) string {
	u := math.Float64bits(f)
	const sign = uint64(1) << 63
	if u&sign != 0 {
		u = ^u // negative: flip all bits
	} else {
		u ^= sign // non-negative (includes +0): flip sign bit
	}
	return uint64ByteStr(u)
}
