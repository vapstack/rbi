package schema

import (
	"reflect"
	"slices"
	"strings"
	"testing"
	"unsafe"
)

func TestPatchApplyAmbiguousPromotedGoNameNoop(t *testing.T) {
	rec := schemaTestPatchSameNamePromotedRec{
		SchemaTestPatchSameNameLeft:  SchemaTestPatchSameNameLeft{ID: "left"},
		SchemaTestPatchSameNameRight: SchemaTestPatchSameNameRight{ID: "right"},
	}
	patch := PatchRuntime{
		Fields: map[string]*Field{},
		typ:    reflect.TypeFor[schemaTestPatchSameNamePromotedRec](),
	}
	if err := patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "ID", Value: "mutated"}}, false); err == nil || !strings.Contains(err.Error(), "cannot patch field ID") {
		t.Fatalf("Apply ambiguous promoted Go name err=%v", err)
	}
	if rec.SchemaTestPatchSameNameLeft.ID != "left" || rec.SchemaTestPatchSameNameRight.ID != "right" {
		t.Fatalf("ambiguous promoted Go name changed record: %+v", rec)
	}
}

type schemaTestNamedStrings []string

type schemaTestPatchNested struct {
	Values []int
}

type schemaTestPatchRec struct {
	Scalar int64
	Typed  []string
	Named  schemaTestNamedStrings
	Nested schemaTestPatchNested
	VI     schemaTestVI
	VIPtr  *schemaTestVI
}

type SchemaTestPatchApplyEmbedded struct {
	Embedded string `db:"embedded_db" json:"embeddedJSON"`
}

type schemaTestPatchApplyRec struct {
	SchemaTestPatchApplyEmbedded
	GoName    string
	DBName    string `db:"db_name"`
	JSONName  string `json:"jsonName"`
	Ptr       *int
	PtrTags   *[]int16
	PtrPtr    **int
	StringPtr *string
	Strings   []string
	Tags      []int16
	Named     schemaTestNamedStrings
	Nested    schemaTestPatchNested
	I8        int8
	U8        uint8
	U64       uint64
	F32       float32
	VI        schemaTestVI
	VIPtr     *schemaTestVI
}

type schemaTestNamedIntPtr *int

type schemaTestNamedPointerApplyRec struct {
	Ptr schemaTestNamedIntPtr
}

func TestPatchAccessorsEqualCopyAndOrdinalCopies(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestPatchRec](), Config{Index: map[string]IndexKind{"Scalar": IndexDefault, "VI": IndexUnique}})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	oldVI := schemaTestVI("AA")
	newVI := schemaTestVI("BB")
	oldRec := schemaTestPatchRec{Scalar: 1, Typed: []string{"a"}, Named: schemaTestNamedStrings{"n1"}, Nested: schemaTestPatchNested{Values: []int{1}}, VI: "AA", VIPtr: &oldVI}
	newRec := schemaTestPatchRec{Scalar: 2, Typed: []string{"b"}, Named: schemaTestNamedStrings{"n2"}, Nested: schemaTestPatchNested{Values: []int{2}}, VI: "BB", VIPtr: &newVI}
	oldPtr := unsafe.Pointer(&oldRec)
	newPtr := unsafe.Pointer(&newRec)

	scalar := schemaTestPatchAccess(t, rt, "Scalar")
	scalarValue, scalarChanged, scalarErr := scalar.value(oldPtr, newPtr, true, nil)
	if scalarErr != nil || !scalarChanged {
		t.Fatal("scalar patch equality failed")
	}
	if scalarValue.(int64) != 2 {
		t.Fatal("scalar patch copy failed")
	}

	typed := schemaTestPatchAccess(t, rt, "Typed")
	copiedTypedValue, _, _ := typed.value(oldPtr, newPtr, false, nil)
	copiedTyped := copiedTypedValue.([]string)
	copiedTyped[0] = "mutated"
	if newRec.Typed[0] != "b" {
		t.Fatal("typed slice copy aliases source")
	}

	named := schemaTestPatchAccess(t, rt, "Named")
	copiedNamedValue, _, _ := named.value(oldPtr, newPtr, false, nil)
	copiedNamed := copiedNamedValue.(schemaTestNamedStrings)
	copiedNamed[0] = "mutated"
	if newRec.Named[0] != "n2" {
		t.Fatal("named immutable slice copy aliases source")
	}

	nested := schemaTestPatchAccess(t, rt, "Nested")
	copiedNestedValue, nestedChanged, nestedErr := nested.value(oldPtr, newPtr, true, nil)
	if nestedErr != nil || !nestedChanged {
		t.Fatal("nested patch equality/copy failed")
	}
	copiedNested := copiedNestedValue.(schemaTestPatchNested)
	copiedNested.Values[0] = 9
	if newRec.Nested.Values[0] != 2 {
		t.Fatal("nested patch copy aliases source")
	}
	vi := schemaTestPatchAccess(t, rt, "VI")
	viValue, viChanged, viErr := vi.value(oldPtr, newPtr, true, nil)
	if viErr != nil || !viChanged {
		t.Fatal("ValueIndexer scalar patch equality failed")
	}
	if viValue.(schemaTestVI) != "BB" {
		t.Fatal("ValueIndexer scalar patch copy failed")
	}
	viPtr := schemaTestPatchAccess(t, rt, "VIPtr")
	viPtrValue, viPtrChanged, viPtrErr := viPtr.value(oldPtr, newPtr, true, nil)
	if viPtrErr != nil || !viPtrChanged {
		t.Fatal("ValueIndexer pointer patch equality failed")
	}
	copiedVIPtr := viPtrValue.(*schemaTestVI)
	if copiedVIPtr == newRec.VIPtr || *copiedVIPtr != "BB" {
		t.Fatal("ValueIndexer pointer patch copy failed")
	}
	newRec.VIPtr = &oldVI
	_, viPtrChanged, _ = viPtr.value(oldPtr, newPtr, true, nil)
	if viPtrChanged {
		t.Fatal("ValueIndexer pointer patch equality should compare pointed value")
	}
	oldRec.VIPtr = nil
	newRec.VIPtr = nil
	_, viPtrChanged, _ = viPtr.value(oldPtr, newPtr, true, nil)
	if viPtrChanged {
		t.Fatal("ValueIndexer pointer patch equality should treat nil pointers as equal")
	}
	newRec.VIPtr = &newVI
	_, viPtrChanged, _ = viPtr.value(oldPtr, newPtr, true, nil)
	if !viPtrChanged {
		t.Fatal("ValueIndexer pointer patch equality should detect nil/non-nil change")
	}

	for _, acc := range rt.Indexed {
		if rt.IndexedByName[acc.Name].PatchOrdinal != acc.PatchOrdinal {
			t.Fatalf("IndexedByName PatchOrdinal mismatch for %s", acc.Name)
		}
	}
	for _, acc := range rt.StringValidation {
		if rt.IndexedByName[acc.Name].PatchOrdinal != acc.PatchOrdinal {
			t.Fatalf("StringValidation PatchOrdinal mismatch for %s", acc.Name)
		}
	}
	for _, acc := range rt.Unique {
		if rt.IndexedByName[acc.Name].PatchOrdinal != acc.PatchOrdinal {
			t.Fatalf("Unique PatchOrdinal mismatch for %s", acc.Name)
		}
	}
}

func schemaTestPatchAccess(t *testing.T, rt *Schema, name string) PatchFieldAccessor {
	t.Helper()
	for i := range rt.Patch.Access {
		acc := rt.Patch.Access[i]
		if acc.Field.Name == name {
			return acc
		}
	}
	t.Fatalf("missing patch accessor %q", name)
	return PatchFieldAccessor{}
}

func TestPatchRuntimeApplyNamesAndConversions(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestPatchApplyRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	vi := schemaTestVI("BB")
	ptrTags := []int16{3, 4}
	ptrPtrValue := 88
	ptrPtr := &ptrPtrValue
	rec := schemaTestPatchApplyRec{}
	err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{
		{Name: "GoName", Value: "go"},
		{Name: "db_name", Value: "db"},
		{Name: "jsonName", Value: "json"},
		{Name: "embedded_db", Value: "embedded"},
		{Name: "Ptr", Value: int64(42)},
		{Name: "PtrTags", Value: &ptrTags},
		{Name: "PtrPtr", Value: &ptrPtr},
		{Name: "Tags", Value: []int{1, 2}},
		{Name: "Named", Value: []string{"n1", "n2"}},
		{Name: "Nested", Value: schemaTestPatchNested{Values: []int{7, 8}}},
		{Name: "I8", Value: int64(12)},
		{Name: "U8", Value: float64(13)},
		{Name: "U64", Value: uint64(15)},
		{Name: "F32", Value: uint64(14)},
		{Name: "VI", Value: "aa"},
		{Name: "VIPtr", Value: vi},
	}, false)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if rec.GoName != "go" || rec.DBName != "db" || rec.JSONName != "json" || rec.Embedded != "embedded" {
		t.Fatalf("name resolution failed: %+v", rec)
	}
	if rec.Ptr == nil || *rec.Ptr != 42 {
		t.Fatalf("pointer conversion failed: %+v", rec.Ptr)
	}
	if rec.PtrTags == nil || !slices.Equal(*rec.PtrTags, []int16{3, 4}) {
		t.Fatalf("same-type pointer-to-slice assignment failed: %#v", rec.PtrTags)
	}
	if rec.PtrPtr == nil || *rec.PtrPtr == nil || **rec.PtrPtr != 88 {
		t.Fatalf("same-type nested pointer assignment failed: %#v", rec.PtrPtr)
	}
	if !slices.Equal(rec.Tags, []int16{1, 2}) {
		t.Fatalf("slice numeric conversion failed: %#v", rec.Tags)
	}
	if !slices.Equal(rec.Named, schemaTestNamedStrings{"n1", "n2"}) {
		t.Fatalf("named slice conversion failed: %#v", rec.Named)
	}
	if !slices.Equal(rec.Nested.Values, []int{7, 8}) {
		t.Fatalf("nested field assignment failed: %#v", rec.Nested)
	}
	if rec.I8 != 12 || rec.U8 != 13 || rec.U64 != 15 || rec.F32 != 14 {
		t.Fatalf("numeric conversion failed: %+v", rec)
	}
	if rec.VI != "aa" || rec.VIPtr == nil || *rec.VIPtr != "BB" {
		t.Fatalf("ValueIndexer assignment failed: VI=%q VIPtr=%v", rec.VI, rec.VIPtr)
	}
}

func TestPatchRuntimeApplyCopiedUsesCopiedStorage(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestPatchApplyRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	inputTags := []int16{3, 4}
	operation := rt.Patch.BeginOperation()
	defer rt.Patch.EndOperation(&operation)
	tags, ok, err := rt.Patch.CopyItemValue("Tags", inputTags, false, &operation)
	if err != nil || !ok {
		t.Fatalf("CopyItemValue(Tags) ok=%v err=%v", ok, err)
	}
	inputTags[0] = 9

	inputPtr := 42
	ptr, ok, err := rt.Patch.CopyItemValue("Ptr", &inputPtr, false, &operation)
	if err != nil || !ok {
		t.Fatalf("CopyItemValue(Ptr) ok=%v err=%v", ok, err)
	}
	inputPtr = 99

	var rec schemaTestPatchApplyRec
	if err = rt.Patch.ApplyCopied(unsafe.Pointer(&rec), []PatchItem{
		{Name: "Tags", Value: tags},
		{Name: "Ptr", Value: ptr},
	}, false); err != nil {
		t.Fatalf("ApplyCopied: %v", err)
	}

	copiedTags := tags.([]int16)
	if !slices.Equal(rec.Tags, []int16{3, 4}) || !slices.Equal(copiedTags, []int16{3, 4}) {
		t.Fatalf("copied tags changed: rec=%v copied=%v", rec.Tags, copiedTags)
	}
	if len(rec.Tags) != 0 && &rec.Tags[0] != &copiedTags[0] {
		t.Fatal("ApplyCopied copied slice storage again")
	}

	copiedPtr := ptr.(*int)
	if rec.Ptr != copiedPtr || rec.Ptr == &inputPtr {
		t.Fatalf("ApplyCopied pointer storage rec=%p copied=%p input=%p", rec.Ptr, copiedPtr, &inputPtr)
	}
	if rec.Ptr == nil || *rec.Ptr != 42 {
		t.Fatalf("ApplyCopied pointer value=%v", rec.Ptr)
	}
}

func TestPatchRuntimeApplyPreservesNamedPointerTypes(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestNamedPointerApplyRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	value := 11
	ptr := schemaTestNamedIntPtr(&value)
	var rec schemaTestNamedPointerApplyRec
	err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{
		{Name: "Ptr", Value: ptr},
	}, false)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if rec.Ptr == nil || *rec.Ptr != 11 {
		t.Fatalf("named pointer assignment failed: %#v", rec.Ptr)
	}
	if rec.Ptr == ptr {
		t.Fatal("named pointer assignment aliases source")
	}
}

func TestPatchRuntimeApplyRejectsNumericStringConversions(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestPatchApplyRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	ptr := "old"
	rec := schemaTestPatchApplyRec{
		GoName:    "old",
		StringPtr: &ptr,
		Strings:   []string{"old"},
		VI:        "old",
	}

	for _, p := range []PatchItem{
		{Name: "GoName", Value: 65},
		{Name: "StringPtr", Value: 65},
		{Name: "Strings", Value: []int{65}},
		{Name: "VI", Value: 65},
	} {
		if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{p}, false); err == nil || !strings.Contains(err.Error(), "type mismatch") {
			t.Fatalf("Apply(%s) error=%v, want type mismatch", p.Name, err)
		}
	}

	if rec.GoName != "old" || rec.StringPtr != &ptr || !slices.Equal(rec.Strings, []string{"old"}) || rec.VI != "old" {
		t.Fatalf("failed numeric string patch changed record: %+v", rec)
	}
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "VI", Value: "new"}}, false); err != nil {
		t.Fatalf("Apply named string: %v", err)
	}
	if rec.VI != "new" {
		t.Fatalf("named string patch failed: %q", rec.VI)
	}
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{
		{Name: "GoName", Value: []byte("bytes")},
		{Name: "StringPtr", Value: []rune("runes")},
	}, false); err != nil {
		t.Fatalf("Apply byte/rune string conversions: %v", err)
	}
	if rec.GoName != "bytes" || rec.StringPtr == nil || *rec.StringPtr != "runes" {
		t.Fatalf("byte/rune string conversions failed: %+v", rec)
	}
}

func TestPatchRuntimeApplyUnknownNilAndConversionErrors(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestPatchApplyRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	n := 5
	rec := schemaTestPatchApplyRec{GoName: "old", Ptr: &n, Tags: []int16{1}}
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "Missing", Value: 1}}, true); err != nil {
		t.Fatalf("Apply ignored unknown: %v", err)
	}
	if rec.GoName != "old" {
		t.Fatalf("ignored unknown changed record: %+v", rec)
	}
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "Missing", Value: 1}}, false); err == nil || !strings.Contains(err.Error(), "cannot patch field Missing") {
		t.Fatalf("strict unknown error=%v", err)
	}

	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "Ptr", Value: nil}, {Name: "Tags", Value: nil}}, false); err != nil {
		t.Fatalf("Apply nil nillable: %v", err)
	}
	if rec.Ptr != nil || rec.Tags != nil {
		t.Fatalf("nil nillable assignment failed: %+v", rec)
	}
	rec.Tags = []int16{1}
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "Tags", Value: []int16(nil)}}, false); err != nil {
		t.Fatalf("Apply typed nil slice: %v", err)
	}
	if rec.Tags != nil {
		t.Fatalf("typed nil slice assignment failed: %#v", rec.Tags)
	}
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "GoName", Value: nil}}, false); err == nil || !strings.Contains(err.Error(), "cannot assign nil to non-nillable field") {
		t.Fatalf("nil non-nillable error=%v", err)
	}
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "I8", Value: int64(200)}}, false); err == nil || !strings.Contains(err.Error(), "overflows int field") {
		t.Fatalf("int overflow error=%v", err)
	}
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "U8", Value: uint64(300)}}, false); err == nil || !strings.Contains(err.Error(), "overflows uint field") {
		t.Fatalf("uint source overflow error=%v", err)
	}
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "U64", Value: -1}}, false); err == nil || !strings.Contains(err.Error(), "negative int") {
		t.Fatalf("negative uint error=%v", err)
	}
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "U64", Value: 1.5}}, false); err == nil || !strings.Contains(err.Error(), "cannot assign float") {
		t.Fatalf("fractional uint error=%v", err)
	}
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "Tags", Value: []any{nil}}}, false); err == nil || !strings.Contains(err.Error(), "cannot set nil to non-pointer slice element") {
		t.Fatalf("slice nil element error=%v", err)
	}
}
