package schema

import (
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
)

type schemaTestVI string

func (v schemaTestVI) IndexingValue() string {
	return strings.ToLower(string(v))
}

type SchemaTestTagEmbedded struct {
	Email string `db:"email" rbi:"unique"`
}

type schemaTestTagRec struct {
	SchemaTestTagEmbedded
	Name     string `db:"name" rbi:"index"`
	Amount   int64  `db:"amount" rbi:"measure"`
	Disabled string `db:"disabled" rbi:"-"`
	Plain    string
}

func TestCompileTagsCollectsIndexedUniqueMeasureAndPatch(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestTagRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if len(rt.Fields) != 2 {
		t.Fatalf("indexed fields=%d want 2", len(rt.Fields))
	}
	if _, ok := rt.Fields["name"]; !ok {
		t.Fatal("missing indexed db field name")
	}
	if f, ok := rt.Fields["email"]; !ok || !f.Unique {
		t.Fatalf("unique embedded field metadata=(%+v,%v)", f, ok)
	}
	if _, ok := rt.Fields["disabled"]; ok {
		t.Fatal("disabled field must not be indexed")
	}
	if _, ok := rt.MeasureFields["amount"]; !ok {
		t.Fatal("missing measure field")
	}
	if len(rt.Patch.Access) == 0 || rt.Patch.Fields["name"] == nil || rt.Patch.Fields["email"] == nil {
		t.Fatal("patch runtime did not include exported fields")
	}
	if !rt.HasUnique {
		t.Fatal("expected HasUnique")
	}
}

type SchemaTestOptionLeft struct {
	ID string `db:"left_id"`
}

type SchemaTestOptionRight struct {
	ID string `db:"right_id"`
}

type schemaTestOptionRec struct {
	SchemaTestOptionLeft
	SchemaTestOptionRight
	Name   string `db:"name" rbi:"index"`
	Score  int64  `db:"score_db" rbi:"index"`
	Amount int64  `db:"amount"`
}

type SchemaTestShadowedUniqueEmbedded struct {
	Code string `db:"inner_code" json:"innerCode" rbi:"unique"`
}

type schemaTestShadowedPatchRec struct {
	SchemaTestShadowedUniqueEmbedded
	Code string `db:"outer_code" json:"outerCode"`
}

func TestPatchNameTouchesUniqueWithShadowedEmbeddedAliases(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestShadowedPatchRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if !rt.PatchNameTouchesUnique("inner_code") {
		t.Fatal("db alias for shadowed embedded unique field was not marked unique")
	}
	if !rt.PatchNameTouchesUnique("innerCode") {
		t.Fatal("json alias for shadowed embedded unique field was not marked unique")
	}
	innerAcc := rt.IndexedByName["inner_code"]
	if innerAcc.PatchOrdinal < 0 {
		t.Fatal("shadowed embedded unique field did not get a patch ordinal")
	}
	if rt.Patch.Access[innerAcc.PatchOrdinal].Field.DBName != "inner_code" {
		t.Fatalf("shadowed embedded unique PatchOrdinal=%d field=%+v", innerAcc.PatchOrdinal, rt.Patch.Access[innerAcc.PatchOrdinal].Field)
	}
	if rt.PatchNameTouchesUnique("Code") {
		t.Fatal("shadowing Go name was marked unique for the outer field")
	}
	if rt.PatchNameTouchesUnique("outer_code") {
		t.Fatal("outer non-indexed db alias was marked unique")
	}
	if rt.PatchNameTouchesUnique("outerCode") {
		t.Fatal("outer non-indexed json alias was marked unique")
	}
}

func TestCompileOptionsNilEmptyAndNameResolution(t *testing.T) {
	vtype := reflect.TypeFor[schemaTestOptionRec]()
	rt, err := Compile(vtype, Config{})
	if err != nil {
		t.Fatalf("Compile tags: %v", err)
	}
	if _, ok := rt.Fields["name"]; !ok {
		t.Fatal("nil Options.Index must collect tags")
	}

	rt, err = Compile(vtype, Config{Index: map[string]IndexKind{}})
	if err != nil {
		t.Fatalf("Compile empty options: %v", err)
	}
	if rt.HasQueryFields() {
		t.Fatal("empty Options.Index must disable indexed/measure fields")
	}

	rt, err = Compile(vtype, Config{Index: map[string]IndexKind{
		"Name":     IndexDefault,
		"score_db": IndexUnique,
		"Amount":   IndexMeasure,
		"left_id":  IndexDefault,
		"right_id": IndexDefault,
	}})
	if err != nil {
		t.Fatalf("Compile options: %v", err)
	}
	if _, ok := rt.Fields["name"]; !ok {
		t.Fatal("Go-name option did not resolve")
	}
	if f := rt.Fields["score_db"]; f == nil || !f.Unique {
		t.Fatalf("db-name unique option metadata=%+v", f)
	}
	if _, ok := rt.MeasureFields["amount"]; !ok {
		t.Fatal("measure option did not resolve")
	}
	if _, ok := rt.Fields["left_id"]; !ok {
		t.Fatal("left db tag option did not resolve")
	}
	if _, ok := rt.Fields["right_id"]; !ok {
		t.Fatal("right db tag option did not resolve")
	}

	if _, err = Compile(vtype, Config{Index: map[string]IndexKind{"ID": IndexDefault}}); err == nil || !strings.Contains(err.Error(), `ambiguous Go field name "ID"`) {
		t.Fatalf("ambiguous Go name err=%v", err)
	}
	if _, err = Compile(vtype, Config{Index: map[string]IndexKind{"Missing": IndexDefault}}); err == nil || !strings.Contains(err.Error(), `unknown index field "Missing"`) {
		t.Fatalf("unknown option err=%v", err)
	}
	if _, err = Compile(vtype, Config{Index: map[string]IndexKind{"Name": IndexKind(99)}}); err == nil || !strings.Contains(err.Error(), `invalid IndexKind 99`) {
		t.Fatalf("invalid kind err=%v", err)
	}
	if _, err = Compile(vtype, Config{Index: map[string]IndexKind{"Score": IndexDefault, "score_db": IndexDefault}}); err == nil || !strings.Contains(err.Error(), `indexed more than once`) {
		t.Fatalf("duplicate option err=%v", err)
	}
}

type schemaTestAccessorRec struct {
	Name   string       `db:"name" rbi:"index"`
	Age    int64        `db:"age" rbi:"index"`
	Score  float64      `db:"score" rbi:"index"`
	When   time.Time    `db:"when" rbi:"index"`
	Maybe  *int64       `db:"maybe" rbi:"index"`
	Tags   []string     `db:"tags" rbi:"index"`
	Scores []int64      `db:"scores" rbi:"index"`
	Code   schemaTestVI `db:"code" rbi:"index"`
	Unique string       `db:"unique" rbi:"unique"`
}

type schemaTestStableOrdinalRec struct {
	Z string `rbi:"index"`
	A string `rbi:"index"`
	M int    `rbi:"index"`
}

func TestIndexedAccessorsAssignStableSortedOrdinals(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestStableOrdinalRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	got := make([]string, len(rt.Indexed))
	for i, acc := range rt.Indexed {
		got[i] = acc.Name
		if acc.Ordinal != i {
			t.Fatalf("ordinal for %q=%d want %d", acc.Name, acc.Ordinal, i)
		}
	}
	if !reflect.DeepEqual(got, []string{"A", "M", "Z"}) {
		t.Fatalf("ordinals=%v", got)
	}
}

func TestIndexedAccessorsEmitKeysAndDetectChanges(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	maybe := int64(-3)
	oldRec := schemaTestAccessorRec{
		Name:   "alice",
		Age:    -7,
		Score:  1.5,
		When:   time.Unix(10, 99).UTC(),
		Maybe:  &maybe,
		Tags:   []string{"go", "db", "go"},
		Scores: []int64{5, 7, 5},
		Code:   "ABC",
		Unique: "u1",
	}
	newRec := oldRec
	oldPtr := unsafe.Pointer(&oldRec)
	newPtr := unsafe.Pointer(&newRec)

	var scratch WriteScratch
	rt.IndexedByName["name"].WriteScratch(oldPtr, &scratch)
	if !scratch.ok || !slices.Equal(scratch.strings, []string{"alice"}) {
		t.Fatalf("name scratch=%+v", scratch)
	}
	scratch.reset()
	rt.IndexedByName["age"].WriteScratch(oldPtr, &scratch)
	if !scratch.ok || !slices.Equal(scratch.fixed, []uint64{keycodec.OrderedInt64Key(-7)}) {
		t.Fatalf("age scratch=%+v", scratch)
	}
	scratch.reset()
	rt.IndexedByName["score"].WriteScratch(oldPtr, &scratch)
	if !scratch.ok || !slices.Equal(scratch.fixed, []uint64{keycodec.OrderedFloat64Key(1.5)}) {
		t.Fatalf("score scratch=%+v", scratch)
	}
	scratch.reset()
	rt.IndexedByName["when"].WriteScratch(oldPtr, &scratch)
	if !scratch.ok || !slices.Equal(scratch.fixed, []uint64{keycodec.OrderedInt64Key(10)}) {
		t.Fatalf("time scratch=%+v", scratch)
	}
	scratch.reset()
	rt.IndexedByName["maybe"].WriteScratch(oldPtr, &scratch)
	if !scratch.ok || !slices.Equal(scratch.fixed, []uint64{keycodec.OrderedInt64Key(-3)}) {
		t.Fatalf("ptr scratch=%+v", scratch)
	}
	scratch.reset()
	rt.IndexedByName["tags"].WriteScratch(oldPtr, &scratch)
	if !scratch.ok || scratch.length != 2 || !slices.Equal(scratch.strings, []string{"go", "db"}) {
		t.Fatalf("tags scratch=%+v", scratch)
	}
	scratch.reset()
	rt.IndexedByName["scores"].WriteScratch(oldPtr, &scratch)
	if !scratch.ok || scratch.length != 2 || !slices.Equal(scratch.fixed, []uint64{keycodec.OrderedInt64Key(5), keycodec.OrderedInt64Key(7)}) {
		t.Fatalf("scores scratch=%+v", scratch)
	}
	scratch.reset()
	rt.IndexedByName["code"].WriteScratch(oldPtr, &scratch)
	if !scratch.ok || !slices.Equal(scratch.strings, []string{"abc"}) {
		t.Fatalf("ValueIndexer scratch=%+v", scratch)
	}

	if rt.IndexedByName["tags"].Modified(oldPtr, newPtr) {
		t.Fatal("identical slice values must not be modified")
	}
	newRec.Tags = []string{"go", "search"}
	if !rt.IndexedByName["tags"].Modified(oldPtr, unsafe.Pointer(&newRec)) {
		t.Fatal("changed slice value was not detected")
	}
	key, ok, isNil := rt.IndexedByName["unique"].UniqueGetter(oldPtr)
	if !ok || isNil || key.StringKey() != "u1" {
		t.Fatalf("unique getter key=%+v ok=%v isNil=%v", key, ok, isNil)
	}
}

type schemaTestStringValidationRec struct {
	Name   string `db:"name" rbi:"index"`
	Active bool   `db:"active" rbi:"index"`
	Tags   []int  `db:"tags" rbi:"index"`
}

func TestStringValidationUsesRealStringAccessors(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestStringValidationRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if len(rt.StringValidation) != 1 || rt.StringValidation[0].Name != "name" {
		t.Fatalf("StringValidation=%+v, want only name", rt.StringValidation)
	}
	rec := schemaTestStringValidationRec{Name: strings.Repeat("x", indexdata.FieldStringRefMax+1)}
	var fieldErr error
	rt.StringValidation[0].WriteBuild(unsafe.Pointer(&rec), BuildSink{
		Field: rt.StringValidation[0].Name,
		Err:   &fieldErr,
	})
	if fieldErr == nil || !strings.Contains(fieldErr.Error(), "exceeds limit") {
		t.Fatalf("WriteBuild validation err=%v, want indexed string limit error", fieldErr)
	}
}

type schemaTestMeasureRec struct {
	Signed   int64    `db:"signed" rbi:"measure"`
	Unsigned uint64   `db:"unsigned" rbi:"measure"`
	Float    float64  `db:"float" rbi:"measure"`
	Ptr      *uint64  `db:"ptr" rbi:"measure"`
	Invalid  string   `db:"invalid"`
	Indexed  []string `db:"indexed" rbi:"index"`
}

func TestMeasureAccessorsReadAndDetectChanges(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestMeasureRec](), Config{Index: map[string]IndexKind{
		"Signed":   IndexMeasure,
		"Unsigned": IndexMeasure,
		"Float":    IndexMeasure,
		"Ptr":      IndexMeasure,
	}})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	ptr := uint64(9)
	oldRec := schemaTestMeasureRec{Signed: -3, Unsigned: 7, Float: 1.25, Ptr: &ptr}
	newPtr := uint64(10)
	newRec := schemaTestMeasureRec{Signed: -4, Unsigned: 8, Float: 1.5, Ptr: &newPtr}
	oldPtr := unsafe.Pointer(&oldRec)
	newPtrUnsafe := unsafe.Pointer(&newRec)

	if got, ok := rt.MeasuresByName["signed"].Read(oldPtr); !ok || got != uint64(oldRec.Signed) {
		t.Fatalf("signed read=(%d,%v)", got, ok)
	}
	if got, ok := rt.MeasuresByName["unsigned"].Read(oldPtr); !ok || got != 7 {
		t.Fatalf("unsigned read=(%d,%v)", got, ok)
	}
	if got, ok := rt.MeasuresByName["float"].Read(oldPtr); !ok || got != math.Float64bits(1.25) {
		t.Fatalf("float read=(%d,%v)", got, ok)
	}
	if got, ok := rt.MeasuresByName["ptr"].Read(oldPtr); !ok || got != 9 {
		t.Fatalf("ptr read=(%d,%v)", got, ok)
	}
	if !rt.MeasuresByName["signed"].Modified(oldPtr, newPtrUnsafe) {
		t.Fatal("signed change was not detected")
	}
	if !rt.MeasuresByName["ptr"].Modified(oldPtr, newPtrUnsafe) {
		t.Fatal("ptr change was not detected")
	}

	if _, err = Compile(reflect.TypeFor[schemaTestMeasureRec](), Config{Index: map[string]IndexKind{"Invalid": IndexMeasure}}); err == nil || !strings.Contains(err.Error(), "measure field Invalid has unsupported type") {
		t.Fatalf("invalid measure err=%v", err)
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
	GoName   string
	DBName   string `db:"db_name"`
	JSONName string `json:"jsonName"`
	Ptr      *int
	Tags     []int16
	Named    schemaTestNamedStrings
	Nested   schemaTestPatchNested
	I8       int8
	U8       uint8
	F32      float32
	VI       schemaTestVI
	VIPtr    *schemaTestVI
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

	scalar := rt.Patch.Access[rt.Patch.Ordinals["Scalar"]]
	if scalar.ValueEqual == nil || scalar.ValueEqual(oldPtr, newPtr) {
		t.Fatal("scalar patch equality failed")
	}
	if scalar.CopyValue == nil || scalar.CopyValue(newPtr).(int64) != 2 {
		t.Fatal("scalar patch copy failed")
	}

	typed := rt.Patch.Access[rt.Patch.Ordinals["Typed"]]
	copiedTyped := typed.CopyValue(newPtr).([]string)
	copiedTyped[0] = "mutated"
	if newRec.Typed[0] != "b" {
		t.Fatal("typed slice copy aliases source")
	}

	named := rt.Patch.Access[rt.Patch.Ordinals["Named"]]
	copiedNamed := named.CopyValue(newPtr).(schemaTestNamedStrings)
	copiedNamed[0] = "mutated"
	if newRec.Named[0] != "n2" {
		t.Fatal("named immutable slice copy aliases source")
	}

	nested := rt.Patch.Access[rt.Patch.Ordinals["Nested"]]
	if nested.ValueEqual != nil || nested.CopyValue != nil {
		t.Fatal("mutable nested patch field must use root fallback")
	}
	vi := rt.Patch.Access[rt.Patch.Ordinals["VI"]]
	if vi.ValueEqual == nil || vi.ValueEqual(oldPtr, newPtr) {
		t.Fatal("ValueIndexer scalar patch equality failed")
	}
	if vi.CopyValue != nil {
		t.Fatal("ValueIndexer copy should use root fallback")
	}
	viPtr := rt.Patch.Access[rt.Patch.Ordinals["VIPtr"]]
	if viPtr.ValueEqual == nil || viPtr.ValueEqual(oldPtr, newPtr) {
		t.Fatal("ValueIndexer pointer patch equality failed")
	}
	newRec.VIPtr = &oldVI
	if !viPtr.ValueEqual(oldPtr, newPtr) {
		t.Fatal("ValueIndexer pointer patch equality should compare pointed value")
	}
	oldRec.VIPtr = nil
	newRec.VIPtr = nil
	if !viPtr.ValueEqual(oldPtr, newPtr) {
		t.Fatal("ValueIndexer pointer patch equality should treat nil pointers as equal")
	}
	newRec.VIPtr = &newVI
	if viPtr.ValueEqual(oldPtr, newPtr) {
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

func TestPatchRuntimeApplyNamesAndConversions(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestPatchApplyRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	vi := schemaTestVI("BB")
	rec := schemaTestPatchApplyRec{}
	err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{
		{Name: "GoName", Value: "go"},
		{Name: "db_name", Value: "db"},
		{Name: "jsonName", Value: "json"},
		{Name: "embedded_db", Value: "embedded"},
		{Name: "Ptr", Value: int64(42)},
		{Name: "Tags", Value: []int{1, 2}},
		{Name: "Named", Value: []string{"n1", "n2"}},
		{Name: "Nested", Value: schemaTestPatchNested{Values: []int{7, 8}}},
		{Name: "I8", Value: int64(12)},
		{Name: "U8", Value: float64(13)},
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
	if !slices.Equal(rec.Tags, []int16{1, 2}) {
		t.Fatalf("slice numeric conversion failed: %#v", rec.Tags)
	}
	if !slices.Equal(rec.Named, schemaTestNamedStrings{"n1", "n2"}) {
		t.Fatalf("named slice conversion failed: %#v", rec.Named)
	}
	if !slices.Equal(rec.Nested.Values, []int{7, 8}) {
		t.Fatalf("nested field assignment failed: %#v", rec.Nested)
	}
	if rec.I8 != 12 || rec.U8 != 13 || rec.F32 != 14 {
		t.Fatalf("numeric conversion failed: %+v", rec)
	}
	if rec.VI != "aa" || rec.VIPtr == nil || *rec.VIPtr != "BB" {
		t.Fatalf("ValueIndexer assignment failed: VI=%q VIPtr=%v", rec.VI, rec.VIPtr)
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
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "GoName", Value: nil}}, false); err == nil || !strings.Contains(err.Error(), "cannot assign nil to non-nillable field") {
		t.Fatalf("nil non-nillable error=%v", err)
	}
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "I8", Value: int64(200)}}, false); err == nil || !strings.Contains(err.Error(), "overflows int field") {
		t.Fatalf("int overflow error=%v", err)
	}
	if err = rt.Patch.Apply(unsafe.Pointer(&rec), []PatchItem{{Name: "Tags", Value: []any{nil}}}, false); err == nil || !strings.Contains(err.Error(), "cannot set nil to non-pointer slice element") {
		t.Fatalf("slice nil element error=%v", err)
	}
}

func schemaTestOverlayContains(storage indexdata.FieldStorage, key string, id uint64) bool {
	return indexdata.NewFieldIndexViewFromStorage(storage).LookupPostingRetained(key).Contains(id)
}

func schemaTestOverlayContainsKey(storage indexdata.FieldStorage, key uint64, id uint64) bool {
	return indexdata.NewFieldIndexViewFromStorage(storage).LookupPostingRetainedKey(keycodec.FromU64(key)).Contains(id)
}

func TestRuntimeLookupHelpers(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	ordinal, ok := rt.IndexedByName.ResolveField("name")
	if !ok || ordinal != rt.IndexedByName["name"].Ordinal {
		t.Fatalf("ResolveField(name)=(%d,%v)", ordinal, ok)
	}
	if _, ok = rt.IndexedByName.ResolveField("missing"); ok {
		t.Fatal("ResolveField(missing) succeeded")
	}
	if !rt.PatchNameTouchesUnique("Unique") {
		t.Fatal("unique patch field was not recognized")
	}
	if rt.PatchNameTouchesUnique("Name") {
		t.Fatal("non-unique patch field reported unique")
	}
	if !FieldUsesOrderedNumericKeys(rt.Fields["age"]) {
		t.Fatal("age should use ordered numeric keys")
	}
	if FieldUsesOrderedNumericKeys(rt.Fields["tags"]) {
		t.Fatal("tags should not use ordered numeric keys")
	}
	if unix, ok := QueryValueToUnixSeconds(reflect.ValueOf(time.Unix(77, 123).UTC())); !ok || unix != 77 {
		t.Fatalf("QueryValueToUnixSeconds=(%d,%v)", unix, ok)
	}
	if !reflect.TypeFor[schemaTestVI]().Implements(ValueIndexerType) {
		t.Fatal("schemaTestVI must implement ValueIndexer")
	}
}

func TestBuildFieldStateHotPaths(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	rec := schemaTestAccessorRec{
		Name:   "alice",
		Scores: []int64{5, 7, 5},
	}
	ptr := unsafe.Pointer(&rec)

	stringLocal := NewBuildFieldLocalState(false, false)
	rt.IndexedByName["name"].WriteBuild(ptr, BuildSink{State: &stringLocal, Idx: 1, Field: "name"})
	stringState := NewBuildFieldState(false)
	stringLocal.FlushRegularInto(stringState)
	stringStorage := stringState.MaterializeStorage()
	defer stringStorage.Release()
	if !schemaTestOverlayContains(stringStorage, "alice", 1) {
		t.Fatal("string build materialization lost posting")
	}

	fixedLocal := NewBuildFieldLocalState(true, true)
	rt.IndexedByName["scores"].WriteBuild(ptr, BuildSink{State: &fixedLocal, Idx: 2, Field: "scores"})
	fixedState := NewBuildFieldState(true)
	fixedLocal.FlushAllInto(fixedState)
	fixedStorage := fixedState.MaterializeStorage()
	defer fixedStorage.Release()
	if !schemaTestOverlayContainsKey(fixedStorage, keycodec.OrderedInt64Key(5), 2) {
		t.Fatal("fixed build materialization lost posting")
	}
	universe := (posting.List{}).BuildAdded(2)
	lenStorage, _ := fixedState.MaterializeLenStorage(universe)
	defer lenStorage.Release()
	if !schemaTestOverlayContains(lenStorage, keycodec.U64ByteString(2), 2) {
		t.Fatal("len build materialization lost posting")
	}

	nilLocal := NewBuildFieldLocalState(true, false)
	rt.IndexedByName["maybe"].WriteBuild(ptr, BuildSink{State: &nilLocal, Idx: 3, Field: "maybe"})
	nilState := NewBuildFieldState(false)
	nilLocal.FlushAllInto(nilState)
	nilStorage := nilState.MaterializeNilStorage()
	defer nilStorage.Release()
	if !schemaTestOverlayContains(nilStorage, indexdata.NilIndexEntryKey, 3) {
		t.Fatal("nil build materialization lost posting")
	}

	flushLocal := NewBuildFieldLocalState(false, false)
	for i := 0; i < buildIndexRunTargetEntries; i++ {
		flushLocal.addValue(keycodec.U64ByteString(uint64(i)), uint64(i+10))
	}
	if !flushLocal.ShouldFlushRegular() {
		t.Fatal("string local state should be ready to flush")
	}
	flushFixed := NewBuildFieldLocalState(true, false)
	for i := 0; i < buildIndexRunTargetEntries; i++ {
		flushFixed.addFixedValue(uint64(i), uint64(i+10))
	}
	if !flushFixed.ShouldFlushRegular() {
		t.Fatal("fixed local state should be ready to flush")
	}
}

func TestOverlayStateCollectAndMaterialize(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	rec := schemaTestAccessorRec{
		Name:   "alice",
		Scores: []int64{5, 7, 5},
	}
	ptr := unsafe.Pointer(&rec)

	var stringState OverlayState
	rt.IndexedByName["name"].CollectOverlayValue(ptr, 1, &stringState)
	if !stringState.Changed() {
		t.Fatal("string overlay state not marked changed")
	}
	stringStorage := stringState.MaterializeStorage(false)
	defer stringStorage.Release()
	if !schemaTestOverlayContains(stringStorage, "alice", 1) {
		t.Fatal("string overlay materialization lost posting")
	}

	var fixedState OverlayState
	rt.IndexedByName["scores"].CollectOverlayValue(ptr, 2, &fixedState)
	fixedStorage := fixedState.MaterializeStorage(true)
	defer fixedStorage.Release()
	if !schemaTestOverlayContainsKey(fixedStorage, keycodec.OrderedInt64Key(7), 2) {
		t.Fatal("fixed overlay materialization lost posting")
	}
	lenStorage, _ := fixedState.MaterializeLenStorage((posting.List{}).BuildAdded(2))
	defer lenStorage.Release()
	if !schemaTestOverlayContains(lenStorage, keycodec.U64ByteString(2), 2) {
		t.Fatal("len overlay materialization lost posting")
	}

	var nilState OverlayState
	rt.IndexedByName["maybe"].CollectOverlayValue(ptr, 3, &nilState)
	nilStorage := nilState.MaterializeNilStorage()
	defer nilStorage.Release()
	if !schemaTestOverlayContains(nilStorage, indexdata.NilIndexEntryKey, 3) {
		t.Fatal("nil overlay materialization lost posting")
	}
}

func TestInsertStateCollectMergeResetAndHints(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	rec := schemaTestAccessorRec{
		Name:   "alice",
		Scores: []int64{5, 7, 5},
	}
	ptr := unsafe.Pointer(&rec)

	var state InsertState
	rt.IndexedByName["scores"].CollectInsertValue(ptr, 1, false, &state)
	if !state.Changed() {
		t.Fatal("insert state not marked changed")
	}
	storage := rt.IndexedByName["scores"].MergeInsertStorageOwned(indexdata.FieldStorage{}, &state, false)
	defer storage.Release()
	if !schemaTestOverlayContainsKey(storage, keycodec.OrderedInt64Key(5), 1) {
		t.Fatal("insert merge lost fixed posting")
	}
	if diff := state.LenDiff(); diff == nil {
		t.Fatal("insert state missing len diff")
	}
	state.Reset()
	if state.Changed() {
		t.Fatal("insert reset left changed flag set")
	}
	state.discard()
	if state.arena != nil || state.lengths != nil {
		t.Fatal("insert discard kept owned internals")
	}

	states := GetInsertStates(len(rt.Indexed))
	defer ReleaseInsertStates(states)
	prev := make([]indexdata.FieldStorage, len(rt.Indexed))
	nameOrdinal := rt.IndexedByName["name"].Ordinal
	prevMap := indexdata.GetPostingMap()
	prevMap["alice"] = (posting.List{}).BuildAdded(1)
	prev[nameOrdinal] = indexdata.NewRegularFieldStorageFromPostingMapOwned(prevMap, false)
	defer prev[nameOrdinal].Release()
	InitInsertStateHints(states, rt.Indexed, prev, nil, nil, 4)
	if states[nameOrdinal].indexHint != 4 {
		t.Fatalf("index hint=%d want 4", states[nameOrdinal].indexHint)
	}

	var stringState InsertState
	rt.IndexedByName["name"].CollectInsertValue(ptr, 11, false, &stringState)
	stringStorage := rt.IndexedByName["name"].MergeInsertStorageOwned(indexdata.FieldStorage{}, &stringState, false)
	defer stringStorage.Release()
	if !schemaTestOverlayContains(stringStorage, "alice", 11) {
		t.Fatal("insert merge lost string posting")
	}
	stringState.discard()

	var nilState InsertState
	rt.IndexedByName["maybe"].CollectInsertValue(ptr, 12, false, &nilState)
	nilStorage := rt.IndexedByName["maybe"].MergeInsertNilStorageOwned(indexdata.FieldStorage{}, &nilState)
	defer nilStorage.Release()
	if !schemaTestOverlayContains(nilStorage, indexdata.NilIndexEntryKey, 12) {
		t.Fatal("insert merge lost nil posting")
	}
	nilState.discard()
}

func TestBatchStateCollectApplyReset(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	maybe := int64(5)
	oldRec := schemaTestAccessorRec{
		Name:   "alice",
		Age:    -7,
		Maybe:  &maybe,
		Tags:   []string{"go", "db"},
		Scores: []int64{5, 7},
	}
	newRec := schemaTestAccessorRec{
		Name:   "bob",
		Age:    9,
		Tags:   []string{"db", "search"},
		Scores: []int64{7, 9, 11},
	}
	oldPtr := unsafe.Pointer(&oldRec)
	newPtr := unsafe.Pointer(&newRec)

	var scalar BatchState
	rt.IndexedByName["name"].CollectBatchDiff(1, oldPtr, newPtr, false, &scalar)
	if !scalar.Changed() {
		t.Fatal("scalar batch state not marked changed")
	}
	scalarStorage := rt.IndexedByName["name"].ApplyBatchStorageOwned(indexdata.FieldStorage{}, &scalar, false)
	defer scalarStorage.Release()
	if !schemaTestOverlayContains(scalarStorage, "bob", 1) {
		t.Fatal("scalar batch apply lost add posting")
	}
	scalar.Reset()
	if scalar.Changed() {
		t.Fatal("scalar batch reset left changed flag set")
	}

	var fixedScalar BatchState
	rt.IndexedByName["age"].CollectBatchDiff(4, oldPtr, newPtr, false, &fixedScalar)
	if !fixedScalar.Changed() {
		t.Fatal("fixed scalar batch state not marked changed")
	}
	fixedScalarStorage := rt.IndexedByName["age"].ApplyBatchStorageOwned(indexdata.FieldStorage{}, &fixedScalar, false)
	defer fixedScalarStorage.Release()
	if !schemaTestOverlayContainsKey(fixedScalarStorage, keycodec.OrderedInt64Key(9), 4) {
		t.Fatal("fixed scalar batch apply lost add posting")
	}
	fixedScalar.Reset()

	var stringSlice BatchState
	rt.IndexedByName["tags"].CollectBatchDiff(5, oldPtr, newPtr, false, &stringSlice)
	if !stringSlice.Changed() {
		t.Fatal("string slice batch state not marked changed")
	}
	stringSliceStorage := rt.IndexedByName["tags"].ApplyBatchStorageOwned(indexdata.FieldStorage{}, &stringSlice, false)
	defer stringSliceStorage.Release()
	if !schemaTestOverlayContains(stringSliceStorage, "search", 5) {
		t.Fatal("string slice batch apply lost add posting")
	}
	stringSlice.Reset()

	var slice BatchState
	rt.IndexedByName["scores"].CollectBatchDiff(2, oldPtr, newPtr, false, &slice)
	if !slice.Changed() {
		t.Fatal("slice batch state not marked changed")
	}
	sliceStorage := rt.IndexedByName["scores"].ApplyBatchStorageOwned(indexdata.FieldStorage{}, &slice, false)
	defer sliceStorage.Release()
	if !schemaTestOverlayContainsKey(sliceStorage, keycodec.OrderedInt64Key(9), 2) {
		t.Fatal("slice batch apply lost fixed add posting")
	}
	if diff := slice.LenDiff(); diff == nil {
		t.Fatal("slice batch state missing len diff")
	}
	slice.discard()
	if slice.arena != nil || slice.lengths != nil {
		t.Fatal("batch discard kept owned internals")
	}

	var nilDiff BatchState
	rt.IndexedByName["maybe"].CollectBatchDiff(6, oldPtr, newPtr, false, &nilDiff)
	if !nilDiff.Changed() {
		t.Fatal("nil batch state not marked changed")
	}
	nilStorage := rt.IndexedByName["maybe"].ApplyBatchNilStorageOwned(indexdata.FieldStorage{}, &nilDiff)
	defer nilStorage.Release()
	if !schemaTestOverlayContains(nilStorage, indexdata.NilIndexEntryKey, 6) {
		t.Fatal("batch nil apply lost nil posting")
	}
	nilDiff.discard()
}

func TestBatchStateReorderedSliceValuesProduceNoDeltas(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	oldRec := schemaTestAccessorRec{
		Tags:   []string{"go", "db", "go"},
		Scores: []int64{3, 1, 3},
	}
	newRec := schemaTestAccessorRec{
		Tags:   []string{"db", "go"},
		Scores: []int64{1, 3},
	}
	oldPtr := unsafe.Pointer(&oldRec)
	newPtr := unsafe.Pointer(&newRec)

	for _, name := range []string{"tags", "scores"} {
		var state BatchState
		rt.IndexedByName[name].CollectBatchDiff(1, oldPtr, newPtr, false, &state)
		if state.Changed() {
			state.Reset()
			t.Fatalf("%s: reorder-only diff unexpectedly marked field as changed", name)
		}
		state.Reset()
	}
}

func TestBatchStateSlicePoolLifecycle(t *testing.T) {
	states := GetBatchStates(3)
	if len(states) != 3 {
		t.Fatalf("GetBatchStates len=%d want 3", len(states))
	}
	states[0].changed = true
	ReleaseBatchStates(states)
}
