package schema

import (
	"reflect"
	"slices"
	"testing"
	"time"
	"unsafe"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
)

type schemaTestAccessorVariantRec struct {
	PtrString *string        `db:"ptr_string" rbi:"index"`
	Bool      bool           `db:"bool" rbi:"index"`
	PtrBool   *bool          `db:"ptr_bool" rbi:"index"`
	Bools     []bool         `db:"bools" rbi:"index"`
	Uint      uint64         `db:"uint" rbi:"index"`
	PtrUint   *uint64        `db:"ptr_uint" rbi:"index"`
	Uints     []uint64       `db:"uints" rbi:"index"`
	Float     float32        `db:"float" rbi:"index"`
	PtrFloat  *float64       `db:"ptr_float" rbi:"index"`
	Floats    []float64      `db:"floats" rbi:"index"`
	PtrTime   *time.Time     `db:"ptr_time" rbi:"index"`
	VIs       []schemaTestVI `db:"vis" rbi:"index"`
}

type schemaTestValueIndexerInterfaceNilRec struct {
	Key  ValueIndexer   `db:"key" rbi:"unique"`
	Tags []ValueIndexer `db:"tags" rbi:"index"`
}

type schemaTestValueIndexerInterfaceSliceRec struct {
	Tags []ValueIndexer `db:"tags" rbi:"index"`
}

type schemaTestValueIndexerCustomInterface interface {
	ValueIndexer
}

type schemaTestValueIndexerCustomInterfaceNilRec struct {
	Key  schemaTestValueIndexerCustomInterface   `db:"key" rbi:"unique"`
	Tags []schemaTestValueIndexerCustomInterface `db:"tags" rbi:"index"`
}

type schemaTestValueIndexerCustomInterfaceSliceRec struct {
	Tags []schemaTestValueIndexerCustomInterface `db:"tags" rbi:"index"`
}

func TestIndexedAccessorVariantsEmitKeys(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorVariantRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	name := "alice"
	ptrBool := false
	ptrUint := uint64(42)
	ptrFloat := 2.5
	ptrTime := time.Unix(123, 999).UTC()
	rec := schemaTestAccessorVariantRec{
		PtrString: &name,
		Bool:      true,
		PtrBool:   &ptrBool,
		Bools:     []bool{true, false, true},
		Uint:      7,
		PtrUint:   &ptrUint,
		Uints:     []uint64{9, 7, 9},
		Float:     1.25,
		PtrFloat:  &ptrFloat,
		Floats:    []float64{3.5, 1.5, 3.5},
		PtrTime:   &ptrTime,
		VIs:       []schemaTestVI{"AA", "BB", "AA"},
	}
	ptr := unsafe.Pointer(&rec)
	var scratch WriteScratch

	rt.IndexedByName["ptr_string"].WriteScratch(ptr, &scratch)
	if !scratch.ok || !slices.Equal(scratch.strings, []string{"alice"}) {
		t.Fatalf("ptr string scratch=%+v", scratch)
	}
	scratch.reset()
	rt.IndexedByName["bool"].WriteScratch(ptr, &scratch)
	if !scratch.ok || !slices.Equal(scratch.strings, []string{"1"}) {
		t.Fatalf("bool scratch=%+v", scratch)
	}
	scratch.reset()
	rt.IndexedByName["ptr_bool"].WriteScratch(ptr, &scratch)
	if !scratch.ok || !slices.Equal(scratch.strings, []string{"0"}) {
		t.Fatalf("ptr bool scratch=%+v", scratch)
	}
	scratch.reset()
	rt.IndexedByName["bools"].WriteScratch(ptr, &scratch)
	if !scratch.ok || scratch.length != 2 || !slices.Equal(scratch.strings, []string{"1", "0"}) {
		t.Fatalf("bool slice scratch=%+v", scratch)
	}
	scratch.reset()
	rt.IndexedByName["uint"].WriteScratch(ptr, &scratch)
	if !scratch.ok || !slices.Equal(scratch.fixed, []uint64{7}) {
		t.Fatalf("uint scratch=%+v", scratch)
	}
	scratch.reset()
	rt.IndexedByName["ptr_uint"].WriteScratch(ptr, &scratch)
	if !scratch.ok || !slices.Equal(scratch.fixed, []uint64{42}) {
		t.Fatalf("ptr uint scratch=%+v", scratch)
	}
	scratch.reset()
	rt.IndexedByName["uints"].WriteScratch(ptr, &scratch)
	if !scratch.ok || scratch.length != 2 || !slices.Equal(scratch.fixed, []uint64{9, 7}) {
		t.Fatalf("uint slice scratch=%+v", scratch)
	}
	scratch.reset()
	rt.IndexedByName["float"].WriteScratch(ptr, &scratch)
	if !scratch.ok || !slices.Equal(scratch.fixed, []uint64{keycodec.OrderedFloat64Key(1.25)}) {
		t.Fatalf("float scratch=%+v", scratch)
	}
	scratch.reset()
	rt.IndexedByName["ptr_float"].WriteScratch(ptr, &scratch)
	if !scratch.ok || !slices.Equal(scratch.fixed, []uint64{keycodec.OrderedFloat64Key(2.5)}) {
		t.Fatalf("ptr float scratch=%+v", scratch)
	}
	scratch.reset()
	rt.IndexedByName["floats"].WriteScratch(ptr, &scratch)
	if !scratch.ok || scratch.length != 2 || !slices.Equal(scratch.fixed, []uint64{keycodec.OrderedFloat64Key(3.5), keycodec.OrderedFloat64Key(1.5)}) {
		t.Fatalf("float slice scratch=%+v", scratch)
	}
	scratch.reset()
	rt.IndexedByName["ptr_time"].WriteScratch(ptr, &scratch)
	if !scratch.ok || !slices.Equal(scratch.fixed, []uint64{keycodec.OrderedInt64Key(123)}) {
		t.Fatalf("ptr time scratch=%+v", scratch)
	}
	scratch.reset()
	rt.IndexedByName["vis"].WriteScratch(ptr, &scratch)
	if !scratch.ok || scratch.length != 2 || !slices.Equal(scratch.strings, []string{"aa", "bb"}) {
		t.Fatalf("ValueIndexer slice scratch=%+v", scratch)
	}
}

func TestIndexedAccessorVariantsEmitNilForNilPointers(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorVariantRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	rec := schemaTestAccessorVariantRec{}
	ptr := unsafe.Pointer(&rec)
	for _, name := range []string{"ptr_string", "ptr_bool", "ptr_uint", "ptr_float", "ptr_time"} {
		var scratch WriteScratch
		rt.IndexedByName[name].WriteScratch(ptr, &scratch)
		if !scratch.ok || !scratch.isNil {
			t.Fatalf("%s scratch=%+v, want nil marker", name, scratch)
		}
	}
}

func TestIndexedAccessorValueReceiverValueIndexerPointerNil(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestValueReceiverVIPtrRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	rec := schemaTestValueReceiverVIPtrRec{}
	ptr := unsafe.Pointer(&rec)

	if err = rt.IndexedByName["key"].Validate(ptr); err != nil {
		t.Fatalf("Validate nil pointer: %v", err)
	}

	var scratch WriteScratch
	rt.IndexedByName["key"].WriteScratch(ptr, &scratch)
	if !scratch.ok || !scratch.isNil {
		t.Fatalf("nil pointer scratch=%+v, want nil marker", scratch)
	}

	var state IndexState
	rt.IndexedByName["key"].CollectIndexValue(ptr, 7, &state)
	nilStorage := state.MaterializeNilStorage()
	defer nilStorage.Release()
	if !schemaTestIndexViewContains(nilStorage, indexdata.NilIndexEntryKey, 7) {
		t.Fatal("nil pointer field was not added to nil index")
	}

	same := rec
	if rt.IndexedByName["key"].Modified(ptr, unsafe.Pointer(&same)) {
		t.Fatal("two nil pointers must not be modified")
	}

	value := schemaTestVI("AA")
	withValue := schemaTestValueReceiverVIPtrRec{Key: &value}
	if !rt.IndexedByName["key"].Modified(ptr, unsafe.Pointer(&withValue)) {
		t.Fatal("nil/non-nil pointer change was not detected")
	}

	scratch.reset()
	rt.IndexedByName["key"].WriteScratch(unsafe.Pointer(&withValue), &scratch)
	if !scratch.ok || scratch.isNil || !slices.Equal(scratch.strings, []string{"aa"}) {
		t.Fatalf("value pointer scratch=%+v, want canonical key", scratch)
	}
}

func TestIndexedAccessorValueReceiverValueIndexerPointerSliceSkipsNilElements(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestValueReceiverVIPtrSliceRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	a := schemaTestVI("AA")
	b := schemaTestVI("BB")
	rec := schemaTestValueReceiverVIPtrSliceRec{Keys: []*schemaTestVI{nil, &a, nil, &b, &a}}
	ptr := unsafe.Pointer(&rec)

	if err = rt.IndexedByName["keys"].Validate(ptr); err != nil {
		t.Fatalf("Validate pointer slice: %v", err)
	}

	var scratch WriteScratch
	rt.IndexedByName["keys"].WriteScratch(ptr, &scratch)
	if !scratch.ok || scratch.length != 2 || !slices.Equal(scratch.strings, []string{"aa", "bb"}) {
		t.Fatalf("pointer slice scratch=%+v", scratch)
	}

	nilOnly := schemaTestValueReceiverVIPtrSliceRec{Keys: []*schemaTestVI{nil, nil}}
	scratch.reset()
	rt.IndexedByName["keys"].WriteScratch(unsafe.Pointer(&nilOnly), &scratch)
	if !scratch.ok || scratch.length != 0 || len(scratch.strings) != 0 {
		t.Fatalf("nil-only pointer slice scratch=%+v, want empty indexed value set", scratch)
	}
}

func TestCompileRejectsValueIndexerInterfaceScalarField(t *testing.T) {
	if _, err := Compile(reflect.TypeFor[schemaTestValueIndexerInterfaceNilRec](), Config{}); err == nil {
		t.Fatal("Compile accepted ValueIndexer interface scalar field")
	}
}

func TestCompileRejectsValueIndexerInterfaceSliceField(t *testing.T) {
	if _, err := Compile(reflect.TypeFor[schemaTestValueIndexerInterfaceSliceRec](), Config{}); err == nil {
		t.Fatal("Compile accepted ValueIndexer interface slice field")
	}
}

func TestCompileRejectsCustomValueIndexerInterfaceFields(t *testing.T) {
	if _, err := Compile(reflect.TypeFor[schemaTestValueIndexerCustomInterfaceNilRec](), Config{}); err == nil {
		t.Fatal("Compile accepted custom ValueIndexer interface scalar field")
	}
	if _, err := Compile(reflect.TypeFor[schemaTestValueIndexerCustomInterfaceSliceRec](), Config{}); err == nil {
		t.Fatal("Compile accepted custom ValueIndexer interface slice field")
	}
}
