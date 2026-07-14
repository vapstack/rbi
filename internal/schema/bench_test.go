package schema

import (
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qir"
)

type benchmarkSchemaValue string

func (v benchmarkSchemaValue) IndexingValue() string {
	if v == "ALPHA" {
		return "alpha"
	}
	return string(v)
}

type benchmarkSchemaNamedStrings []string

type benchmarkSchemaCloneNamedTags []string

type benchmarkSchemaCloneNamedCounters map[uint64]int

type benchmarkSchemaCloneScalarContainers struct {
	Tags          benchmarkSchemaCloneNamedTags
	Times         []time.Time
	Counters      map[uint64]int
	NamedCounters benchmarkSchemaCloneNamedCounters
	Labels        map[string]string
}

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
		info := qir.FieldInfo{}
		ok := false
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			info, ok = fieldMap.ResolveField("string")
		}
		benchmarkSchemaIntSink = info.Ordinal
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
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				state := NewBuildFieldLocalState(numeric, slice, BuildFieldRunTargetEntries)
				acc.WriteBuild(ptr, BuildSink{
					State: &state,
					Idx:   uint64(i + 1),
				})
				benchmarkSchemaIntSink = len(state.vals) + len(state.fixed) + len(state.lenMap)
				state.Release()
			}
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
		local := NewBuildFieldLocalState(false, true, BuildFieldRunTargetEntries)
		state := NewBuildFieldState(true)
		acc.WriteBuild(ptr, BuildSink{
			State: &local,
			Idx:   uint64(i + 1),
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

func BenchmarkPatchValueCompareChanged(b *testing.B) {
	rt := benchmarkSchemaMustCompile(b)
	recordType := reflect.TypeFor[benchmarkSchemaRecord]()
	oldRec := benchmarkSchemaRecordValue()
	newRec := benchmarkSchemaChangedRecordValue()
	oldPtr := unsafe.Pointer(&oldRec)
	newPtr := unsafe.Pointer(&newRec)
	cases := [...]string{"PatchScalar", "PatchTyped", "PatchNamed", "PatchNested", "PatchVI"}

	for _, name := range cases {
		acc := benchmarkSchemaPatchAccess(b, rt, name)
		fieldType, offset := resolveFieldTypeAndOffset(recordType, acc.Field.Index)
		equal := buildPatchValueEqualFn(acc.Field, fieldType, offset)
		b.Run(name, func(b *testing.B) {
			result := false
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result = equal(oldPtr, newPtr)
			}
			benchmarkSchemaBoolSink = result
		})
	}
}

func BenchmarkPatchValueEqual(b *testing.B) {
	rt := benchmarkSchemaMustCompile(b)
	oldRec := benchmarkSchemaRecordValue()
	equalRec := benchmarkSchemaRecordValue()
	oldPtr := unsafe.Pointer(&oldRec)
	equalPtr := unsafe.Pointer(&equalRec)
	cases := [...]string{"PatchScalar", "PatchTyped", "PatchNamed", "PatchNested", "PatchVI"}

	for _, name := range cases {
		acc := benchmarkSchemaPatchAccess(b, rt, name)
		b.Run(name, func(b *testing.B) {
			changed := false
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, changed, _ = acc.value(oldPtr, equalPtr, true, nil)
			}
			benchmarkSchemaBoolSink = changed
		})
	}
}

func BenchmarkPatchValueChanged(b *testing.B) {
	rt := benchmarkSchemaMustCompile(b)
	oldRec := benchmarkSchemaRecordValue()
	newRec := benchmarkSchemaChangedRecordValue()
	oldPtr := unsafe.Pointer(&oldRec)
	newPtr := unsafe.Pointer(&newRec)
	cases := [...]string{"PatchScalar", "PatchTyped", "PatchNamed", "PatchNested", "PatchVI"}

	for _, name := range cases {
		acc := benchmarkSchemaPatchAccess(b, rt, name)
		b.Run(name, func(b *testing.B) {
			var value any
			changed := false
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				value, changed, _ = acc.value(oldPtr, newPtr, true, nil)
			}
			benchmarkSchemaAnySink = value
			benchmarkSchemaBoolSink = changed
		})
	}
}

func BenchmarkPatchValueCopy(b *testing.B) {
	rt := benchmarkSchemaMustCompile(b)
	rec := benchmarkSchemaRecordValue()
	ptr := unsafe.Pointer(&rec)
	cases := [...]string{"PatchScalar", "PatchTyped", "PatchNamed", "PatchNested", "PatchVI"}

	for _, name := range cases {
		acc := benchmarkSchemaPatchAccess(b, rt, name)
		b.Run(name, func(b *testing.B) {
			var copied any
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				copied, _, _ = acc.value(nil, ptr, false, nil)
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

func BenchmarkCloneRuntimeScalarContainers(b *testing.B) {
	rt, err := Compile(reflect.TypeFor[benchmarkSchemaCloneScalarContainers](), Config{})
	if err != nil {
		b.Fatal(err)
	}
	src := benchmarkSchemaCloneScalarContainers{
		Tags:          benchmarkSchemaCloneNamedTags{"go", "db", "hot"},
		Times:         []time.Time{time.Unix(10, 0).UTC(), time.Unix(20, 0).UTC()},
		Counters:      map[uint64]int{1: 10, 2: 20},
		NamedCounters: benchmarkSchemaCloneNamedCounters{3: 30, 4: 40},
		Labels:        map[string]string{"a": "one", "b": "two"},
	}

	var dst benchmarkSchemaCloneScalarContainers
	sink := 0
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rt.Clone.CloneInto(unsafe.Pointer(&src), unsafe.Pointer(&dst))
		sink += len(dst.Tags) + len(dst.Times) + len(dst.Counters) + len(dst.NamedCounters) + len(dst.Labels)
	}
	benchmarkSchemaIntSink = sink
}

func benchmarkSchemaPatchAccess(b *testing.B, rt *Schema, name string) PatchFieldAccessor {
	b.Helper()
	for i := range rt.Patch.Access {
		acc := rt.Patch.Access[i]
		if acc.Field.Name == name {
			return acc
		}
	}
	b.Fatalf("missing patch accessor %q", name)
	return PatchFieldAccessor{}
}
