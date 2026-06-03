package schema

import (
	"fmt"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/posting"
)

type benchmarkSchemaValue string

func (v benchmarkSchemaValue) IndexingValue() string {
	if v == "ALPHA" {
		return "alpha"
	}
	return string(v)
}

type benchmarkSchemaNamedStrings []string

type benchmarkSchemaNested struct {
	IDs   []int
	Tags  map[string]string
	Child *benchmarkSchemaChild
}

type benchmarkSchemaChild struct {
	Name string
}

type benchmarkSchemaRecord struct {
	String   string               `db:"string" rbi:"index"`
	Bool     bool                 `db:"bool" rbi:"index"`
	Signed   int64                `db:"signed" rbi:"index"`
	Unsigned uint64               `db:"unsigned" rbi:"index"`
	Float    float64              `db:"float" rbi:"index"`
	Time     time.Time            `db:"time" rbi:"index"`
	Ptr      *int64               `db:"ptr" rbi:"index"`
	Tags     []string             `db:"tags" rbi:"index"`
	Scores   []int                `db:"scores" rbi:"index"`
	VI       benchmarkSchemaValue `db:"vi" rbi:"index"`
	Unique   string               `db:"unique" rbi:"unique"`

	MeasureSigned   int64    `db:"measure_signed" rbi:"measure"`
	MeasureUnsigned uint64   `db:"measure_unsigned" rbi:"measure"`
	MeasureFloat    float64  `db:"measure_float" rbi:"measure"`
	MeasurePtr      *uint64  `db:"measure_ptr" rbi:"measure"`
	PatchScalar     int64    `db:"patch_scalar"`
	PatchTyped      []string `db:"patch_typed"`
	PatchNamed      benchmarkSchemaNamedStrings
	PatchNested     benchmarkSchemaNested
	PatchVI         benchmarkSchemaValue
}

var (
	benchmarkSchemaRuntimeSink *Schema
	benchmarkSchemaAnySink     any
	benchmarkSchemaBoolSink    bool
	benchmarkSchemaIntSink     int
	benchmarkSchemaU64Sink     uint64
)

func benchmarkSchemaMustCompile(b *testing.B) *Schema {
	b.Helper()
	rt, err := Compile(reflect.TypeFor[benchmarkSchemaRecord](), Config{})
	if err != nil {
		b.Fatal(err)
	}
	return rt
}

func benchmarkSchemaRecordValue() benchmarkSchemaRecord {
	ptr := int64(23)
	measurePtr := uint64(42)
	return benchmarkSchemaRecord{
		String:          "alpha",
		Bool:            true,
		Signed:          -17,
		Unsigned:        19,
		Float:           1.25,
		Time:            time.Unix(1_700_000_000, 123).UTC(),
		Ptr:             &ptr,
		Tags:            []string{"go", "db", "go"},
		Scores:          []int{3, 1, 3},
		VI:              "ALPHA",
		Unique:          "unique-alpha",
		MeasureSigned:   -77,
		MeasureUnsigned: 88,
		MeasureFloat:    9.5,
		MeasurePtr:      &measurePtr,
		PatchScalar:     11,
		PatchTyped:      []string{"a", "b", "c"},
		PatchNamed:      benchmarkSchemaNamedStrings{"n1", "n2"},
		PatchNested: benchmarkSchemaNested{
			IDs:   []int{1, 2, 3},
			Tags:  map[string]string{"a": "b"},
			Child: &benchmarkSchemaChild{Name: "child"},
		},
		PatchVI: "ALPHA",
	}
}

func benchmarkSchemaChangedRecordValue() benchmarkSchemaRecord {
	rec := benchmarkSchemaRecordValue()
	ptr := int64(24)
	measurePtr := uint64(43)
	rec.String = "beta"
	rec.Bool = false
	rec.Signed = 18
	rec.Unsigned = 20
	rec.Float = 2.5
	rec.Time = rec.Time.Add(time.Second)
	rec.Ptr = &ptr
	rec.Tags = []string{"go", "query"}
	rec.Scores = []int{4, 2}
	rec.VI = "BETA"
	rec.Unique = "unique-beta"
	rec.MeasureSigned = -78
	rec.MeasureUnsigned = 89
	rec.MeasureFloat = 10.5
	rec.MeasurePtr = &measurePtr
	rec.PatchScalar = 12
	rec.PatchTyped = []string{"a", "b", "d"}
	rec.PatchNamed = benchmarkSchemaNamedStrings{"n1", "n3"}
	rec.PatchNested = benchmarkSchemaNested{
		IDs:   []int{1, 2, 4},
		Tags:  map[string]string{"a": "c"},
		Child: &benchmarkSchemaChild{Name: "next"},
	}
	rec.PatchVI = "BETA"
	return rec
}

func BenchmarkCompileTags(b *testing.B) {
	vtype := reflect.TypeFor[benchmarkSchemaRecord]()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rt, err := Compile(vtype, Config{})
		if err != nil {
			b.Fatal(err)
		}
		benchmarkSchemaRuntimeSink = rt
	}
}

func BenchmarkCompileOptions(b *testing.B) {
	vtype := reflect.TypeFor[benchmarkSchemaRecord]()
	config := Config{Index: map[string]IndexKind{
		"String":          IndexDefault,
		"Bool":            IndexDefault,
		"Signed":          IndexDefault,
		"Unsigned":        IndexDefault,
		"Float":           IndexDefault,
		"Time":            IndexDefault,
		"Ptr":             IndexDefault,
		"Tags":            IndexDefault,
		"Scores":          IndexDefault,
		"VI":              IndexDefault,
		"Unique":          IndexUnique,
		"MeasureSigned":   IndexMeasure,
		"MeasureUnsigned": IndexMeasure,
		"MeasureFloat":    IndexMeasure,
		"MeasurePtr":      IndexMeasure,
	}}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rt, err := Compile(vtype, config)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkSchemaRuntimeSink = rt
	}
}

func BenchmarkIndexedAccessorWriteScratch(b *testing.B) {
	rt := benchmarkSchemaMustCompile(b)
	rec := benchmarkSchemaRecordValue()
	ptr := unsafe.Pointer(&rec)
	cases := [...]string{"string", "bool", "signed", "unsigned", "float", "time", "ptr", "tags", "vi"}

	for _, name := range cases {
		acc := rt.IndexedByName[name]
		b.Run(name, func(b *testing.B) {
			var scratch WriteScratch
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				scratch.reset()
				acc.WriteScratch(ptr, &scratch)
			}
			benchmarkSchemaIntSink = scratch.stringLen() + scratch.fixedLen() + scratch.length
		})
	}
}

func BenchmarkIndexedAccessorModified(b *testing.B) {
	rt := benchmarkSchemaMustCompile(b)
	oldRec := benchmarkSchemaRecordValue()
	newRec := benchmarkSchemaChangedRecordValue()
	oldPtr := unsafe.Pointer(&oldRec)
	newPtr := unsafe.Pointer(&newRec)
	cases := [...]string{"string", "ptr", "tags", "time", "vi"}

	for _, name := range cases {
		acc := rt.IndexedByName[name]
		b.Run(name, func(b *testing.B) {
			changed := false
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				changed = acc.Modified(oldPtr, newPtr)
			}
			benchmarkSchemaBoolSink = changed
		})
	}
}

func BenchmarkUniqueScalarGetter(b *testing.B) {
	rt := benchmarkSchemaMustCompile(b)
	rec := benchmarkSchemaRecordValue()
	ptr := unsafe.Pointer(&rec)
	acc := rt.IndexedByName["unique"]

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key, ok, isNil := acc.UniqueGetter(ptr)
		benchmarkSchemaBoolSink = ok && !isNil
		benchmarkSchemaU64Sink = uint64(len(key.StringKey()))
	}
}

func BenchmarkRuntimeLookupHelpers(b *testing.B) {
	rt := benchmarkSchemaMustCompile(b)
	timeValue := reflect.ValueOf(benchmarkSchemaRecordValue().Time)
	fieldMap := rt.IndexedByName

	b.Run("HasQueryFields", func(b *testing.B) {
		ok := false
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ok = rt.HasQueryFields()
		}
		benchmarkSchemaBoolSink = ok
	})
	b.Run("PatchNameTouchesUnique", func(b *testing.B) {
		ok := false
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ok = rt.PatchNameTouchesUnique("Unique")
		}
		benchmarkSchemaBoolSink = ok
	})
	b.Run("ResolveField", func(b *testing.B) {
		ordinal := 0
		ok := false
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ordinal, ok = fieldMap.ResolveField("string")
		}
		benchmarkSchemaIntSink = ordinal
		benchmarkSchemaBoolSink = ok
	})
	b.Run("FieldPredicates", func(b *testing.B) {
		ok := false
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ok = IsNativeTimeField(rt.Fields["time"]) && FieldUsesOrderedNumericKeys(rt.Fields["signed"])
		}
		benchmarkSchemaBoolSink = ok
	})
	b.Run("QueryValueToUnixSeconds", func(b *testing.B) {
		var unix int64
		ok := false
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			unix, ok = QueryValueToUnixSeconds(timeValue)
		}
		benchmarkSchemaU64Sink = uint64(unix)
		benchmarkSchemaBoolSink = ok
	})
}

func BenchmarkIndexedAccessorWriteBuild(b *testing.B) {
	rt := benchmarkSchemaMustCompile(b)
	rec := benchmarkSchemaRecordValue()
	ptr := unsafe.Pointer(&rec)
	cases := [...]string{"string", "signed", "tags", "scores"}

	for _, name := range cases {
		acc := rt.IndexedByName[name]
		numeric := acc.Field.KeyKind == FieldWriteKeysOrderedU64
		slice := acc.Field.Slice
		b.Run(name, func(b *testing.B) {
			var err error
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				state := NewBuildFieldLocalState(numeric, slice)
				acc.WriteBuild(ptr, BuildSink{
					State: &state,
					Idx:   uint64(i + 1),
					Field: acc.Name,
					Err:   &err,
				})
				benchmarkSchemaIntSink = len(state.vals) + len(state.fixed) + len(state.lenMap)
				state.Release()
			}
			benchmarkSchemaBoolSink = err == nil
		})
	}
}

func BenchmarkBuildFieldStateFlushMaterialize(b *testing.B) {
	rt := benchmarkSchemaMustCompile(b)
	rec := benchmarkSchemaRecordValue()
	ptr := unsafe.Pointer(&rec)
	acc := rt.IndexedByName["tags"]

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		local := NewBuildFieldLocalState(false, true)
		state := NewBuildFieldState(true)
		acc.WriteBuild(ptr, BuildSink{
			State: &local,
			Idx:   uint64(i + 1),
			Field: acc.Name,
		})
		local.FlushAllInto(state)
		storage := state.MaterializeStorage()
		nilStorage := state.MaterializeNilStorage()
		universe := (posting.List{}).BuildAdded(uint64(i + 1))
		lenStorage, _ := state.MaterializeLenStorage(universe)
		benchmarkSchemaIntSink = storage.KeyCount() + nilStorage.KeyCount() + lenStorage.KeyCount()
		storage.Release()
		nilStorage.Release()
		lenStorage.Release()
		local.Release()
		state.Release()
	}
}

func BenchmarkIndexStateCollectMaterialize(b *testing.B) {
	rt := benchmarkSchemaMustCompile(b)
	rec := benchmarkSchemaRecordValue()
	ptr := unsafe.Pointer(&rec)
	cases := [...]string{"string", "signed", "tags", "scores"}

	for _, name := range cases {
		acc := rt.IndexedByName[name]
		numeric := acc.Field.KeyKind == FieldWriteKeysOrderedU64
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var state IndexState
				acc.CollectIndexValue(ptr, uint64(i+1), &state)
				storage := state.MaterializeStorage(numeric)
				nilStorage := state.MaterializeNilStorage()
				benchmarkSchemaIntSink = storage.KeyCount() + nilStorage.KeyCount()
				storage.Release()
				nilStorage.Release()
				if acc.Field.Slice {
					universe := (posting.List{}).BuildAdded(uint64(i + 1))
					lenStorage, _ := state.MaterializeLenStorage(universe)
					benchmarkSchemaIntSink += lenStorage.KeyCount()
					lenStorage.Release()
				}
			}
		})
	}
}

func BenchmarkInsertStateCollectMergeReset(b *testing.B) {
	rt := benchmarkSchemaMustCompile(b)
	rec := benchmarkSchemaRecordValue()
	ptr := unsafe.Pointer(&rec)
	cases := [...]string{"string", "signed", "tags", "scores"}

	for _, name := range cases {
		acc := rt.IndexedByName[name]
		b.Run(name, func(b *testing.B) {
			var state InsertState
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				acc.CollectInsertValue(ptr, uint64(i+1), false, &state)
				storage := acc.MergeInsertStorageOwned(indexdata.FieldStorage{}, &state, false)
				nilStorage := acc.MergeInsertNilStorageOwned(indexdata.FieldStorage{}, &state)
				benchmarkSchemaBoolSink = state.Changed()
				benchmarkSchemaIntSink = storage.KeyCount() + nilStorage.KeyCount()
				storage.Release()
				nilStorage.Release()
				if diff := state.LenDiff(); diff != nil {
					lenStorage := (indexdata.FieldStorage{}).ApplyLenPostingDiff(diff)
					benchmarkSchemaIntSink += lenStorage.KeyCount()
					lenStorage.Release()
				}
				state.Reset()
			}
			state.discard()
		})
	}
}

func BenchmarkBatchStateCollectApplyReset(b *testing.B) {
	rt := benchmarkSchemaMustCompile(b)
	oldRec := benchmarkSchemaRecordValue()
	newRec := benchmarkSchemaChangedRecordValue()
	oldPtr := unsafe.Pointer(&oldRec)
	newPtr := unsafe.Pointer(&newRec)
	cases := [...]string{"string", "signed", "tags", "scores"}

	for _, name := range cases {
		acc := rt.IndexedByName[name]
		b.Run(name, func(b *testing.B) {
			var state BatchState
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				acc.CollectBatchDiff(uint64(i+1), oldPtr, newPtr, false, &state)
				storage := acc.ApplyBatchStorageOwned(indexdata.FieldStorage{}, &state, false)
				nilStorage := acc.ApplyBatchNilStorageOwned(indexdata.FieldStorage{}, &state)
				benchmarkSchemaBoolSink = state.Changed()
				benchmarkSchemaIntSink = storage.KeyCount() + nilStorage.KeyCount()
				storage.Release()
				nilStorage.Release()
				if diff := state.LenDiff(); diff != nil {
					lenStorage := (indexdata.FieldStorage{}).ApplyLenPostingDiff(diff)
					benchmarkSchemaIntSink += lenStorage.KeyCount()
					lenStorage.Release()
				}
				state.Reset()
			}
			state.discard()
		})
	}
}

func BenchmarkStateSlicePools(b *testing.B) {
	b.Run("InsertStates", func(b *testing.B) {
		benchmarkSchemaIntSink = 0
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			states := GetInsertStates(8)
			benchmarkSchemaIntSink += len(states)
			ReleaseInsertStates(states)
		}
	})
	b.Run("BatchStates", func(b *testing.B) {
		benchmarkSchemaIntSink = 0
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			states := GetBatchStates(8)
			benchmarkSchemaIntSink += len(states)
			ReleaseBatchStates(states)
		}
	})
}

func BenchmarkMeasureAccessorRead(b *testing.B) {
	rt := benchmarkSchemaMustCompile(b)
	rec := benchmarkSchemaRecordValue()
	ptr := unsafe.Pointer(&rec)
	cases := [...]string{"measure_signed", "measure_unsigned", "measure_float", "measure_ptr"}

	for _, name := range cases {
		acc := rt.MeasuresByName[name]
		b.Run(name, func(b *testing.B) {
			var value uint64
			var ok bool
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				value, ok = acc.Read(ptr)
			}
			benchmarkSchemaU64Sink = value
			benchmarkSchemaBoolSink = ok
		})
	}
}

func BenchmarkMeasureAccessorModified(b *testing.B) {
	rt := benchmarkSchemaMustCompile(b)
	oldRec := benchmarkSchemaRecordValue()
	newRec := benchmarkSchemaChangedRecordValue()
	oldPtr := unsafe.Pointer(&oldRec)
	newPtr := unsafe.Pointer(&newRec)
	cases := [...]string{"measure_signed", "measure_unsigned", "measure_float", "measure_ptr"}

	for _, name := range cases {
		acc := rt.MeasuresByName[name]
		b.Run(name, func(b *testing.B) {
			changed := false
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				changed = acc.Modified(oldPtr, newPtr)
			}
			benchmarkSchemaBoolSink = changed
		})
	}
}

func BenchmarkPatchValueEqual(b *testing.B) {
	rt := benchmarkSchemaMustCompile(b)
	oldRec := benchmarkSchemaRecordValue()
	newRec := benchmarkSchemaChangedRecordValue()
	oldPtr := unsafe.Pointer(&oldRec)
	newPtr := unsafe.Pointer(&newRec)
	oldValue := reflect.ValueOf(&oldRec).Elem()
	newValue := reflect.ValueOf(&newRec).Elem()
	cases := [...]string{"PatchScalar", "PatchTyped", "PatchNamed", "PatchNested", "PatchVI"}

	for _, name := range cases {
		acc := rt.Patch.Access[rt.Patch.Ordinals[name]]
		fieldIndex := acc.Field.Index
		b.Run(name, func(b *testing.B) {
			equal := false
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if acc.ValueEqual != nil {
					equal = acc.ValueEqual(oldPtr, newPtr)
				} else {
					equal = reflect.DeepEqual(oldValue.FieldByIndex(fieldIndex).Interface(), newValue.FieldByIndex(fieldIndex).Interface())
				}
			}
			benchmarkSchemaBoolSink = equal
		})
	}
}

func BenchmarkPatchValueCopy(b *testing.B) {
	rt := benchmarkSchemaMustCompile(b)
	rec := benchmarkSchemaRecordValue()
	ptr := unsafe.Pointer(&rec)
	value := reflect.ValueOf(&rec).Elem()
	cases := [...]string{"PatchScalar", "PatchTyped", "PatchNamed", "PatchNested", "PatchVI"}

	for _, name := range cases {
		acc := rt.Patch.Access[rt.Patch.Ordinals[name]]
		fieldIndex := acc.Field.Index
		b.Run(name, func(b *testing.B) {
			var copied any
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if acc.CopyValue != nil {
					copied = acc.CopyValue(ptr)
				} else {
					copied = benchmarkSchemaDeepCopyValue(value.FieldByIndex(fieldIndex).Interface())
				}
			}
			benchmarkSchemaAnySink = copied
		})
	}
}

func BenchmarkPatchApply(b *testing.B) {
	rt := benchmarkSchemaMustCompile(b)
	rec := benchmarkSchemaRecordValue()
	items := []PatchItem{
		{Name: "string", Value: "beta"},
		{Name: "signed", Value: int32(18)},
		{Name: "ptr", Value: int64(24)},
		{Name: "tags", Value: []string{"go", "query"}},
		{Name: "scores", Value: []int16{4, 2}},
	}
	ptr := unsafe.Pointer(&rec)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := rt.Patch.Apply(ptr, items, false); err != nil {
			b.Fatal(err)
		}
	}
	benchmarkSchemaAnySink = rec.Tags
}

func benchmarkSchemaDeepCopyValue(src any) any {
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
	clone := benchmarkSchemaDeepCopy(origin, visited)
	return clone.Interface()
}

func benchmarkSchemaDeepCopy(origin reflect.Value, visited map[uintptr]reflect.Value) reflect.Value {
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
			clone := benchmarkSchemaDeepCopy(origin.Field(i), visited)
			sf.Set(clone)
		}
		return s
	case reflect.Ptr:
		ptr := reflect.New(origin.Elem().Type())
		visited[origin.Pointer()] = ptr
		clone := benchmarkSchemaDeepCopy(origin.Elem(), visited)
		ptr.Elem().Set(clone)
		return ptr
	case reflect.Slice:
		s := reflect.MakeSlice(origin.Type(), origin.Len(), origin.Cap())
		visited[origin.Pointer()] = s
		for i := 0; i < origin.Len(); i++ {
			clone := benchmarkSchemaDeepCopy(origin.Index(i), visited)
			s.Index(i).Set(clone)
		}
		return s
	case reflect.Map:
		m := reflect.MakeMap(origin.Type())
		visited[origin.Pointer()] = m
		for _, key := range origin.MapKeys() {
			keyClone := benchmarkSchemaDeepCopy(key, visited)
			valClone := benchmarkSchemaDeepCopy(origin.MapIndex(key), visited)
			m.SetMapIndex(keyClone, valClone)
		}
		return m
	case reflect.Array:
		a := reflect.New(origin.Type()).Elem()
		for i := 0; i < origin.Len(); i++ {
			clone := benchmarkSchemaDeepCopy(origin.Index(i), visited)
			a.Index(i).Set(clone)
		}
		return a
	case reflect.Interface:
		clone := benchmarkSchemaDeepCopy(origin.Elem(), visited)
		if !clone.IsValid() {
			return reflect.Zero(origin.Type())
		}
		return clone.Convert(origin.Type())
	case reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return reflect.Zero(origin.Type())
	default:
		panic(fmt.Errorf("rbi: benchmarkSchemaDeepCopy: unsupported value kind: %v", kind))
	}
}
