package schema

import (
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/qir"
)

type schemaTestValueReceiverVIPtrRec struct {
	Key *schemaTestVI `db:"key" rbi:"index"`
}

type schemaTestValueReceiverVIPtrSliceRec struct {
	Keys []*schemaTestVI `db:"keys" rbi:"index"`
}

type schemaTestPointerReceiverVIPtrRec struct {
	Key  *schemaTestPtrFoldedString   `db:"key" rbi:"index"`
	Keys []*schemaTestPtrFoldedString `db:"keys" rbi:"index"`
}

func TestCompileAllowsValueReceiverValueIndexerPointers(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestValueReceiverVIPtrRec](), Config{})
	if err != nil {
		t.Fatalf("scalar pointer Compile: %v", err)
	}
	if f := rt.Fields["key"]; f == nil || !f.UseVI || !f.Ptr || f.Slice || f.Kind != reflect.String {
		t.Fatalf("scalar pointer field=%+v", f)
	}

	rt, err = Compile(reflect.TypeFor[schemaTestValueReceiverVIPtrSliceRec](), Config{})
	if err != nil {
		t.Fatalf("pointer slice Compile: %v", err)
	}
	if f := rt.Fields["keys"]; f == nil || !f.UseVI || f.Ptr || !f.Slice {
		t.Fatalf("pointer slice field=%+v", f)
	}

	if _, err := Compile(reflect.TypeFor[schemaTestPointerReceiverVIPtrRec](), Config{}); err != nil {
		t.Fatalf("pointer receiver Compile: %v", err)
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

type schemaTestPtrFoldedString string

func (s *schemaTestPtrFoldedString) IndexingValue() string {
	if s == nil {
		return "<nil>"
	}
	return strings.ToLower(string(*s))
}

type SchemaTestUnsafeEmbeddedIndexed struct {
	Code  *schemaTestPtrFoldedString `db:"code" rbi:"unique"`
	Score int                        `db:"score" rbi:"index"`
	Tags  []string                   `db:"tags" rbi:"index"`
	Count *uint64                    `db:"count" rbi:"index"`
}

type schemaTestUnsafeAccessorRec struct {
	Name string `db:"name" rbi:"index"`
	SchemaTestUnsafeEmbeddedIndexed
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

func TestIndexedAccessorsDetectEmbeddedUnsafePathChanges(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestUnsafeAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	oldCode := schemaTestPtrFoldedString("MiXeD")
	sameCode := schemaTestPtrFoldedString("mixed")
	oldCount := uint64(7)
	sameCount := uint64(7)

	base := schemaTestUnsafeAccessorRec{
		Name: "alice",
		SchemaTestUnsafeEmbeddedIndexed: SchemaTestUnsafeEmbeddedIndexed{
			Code:  &oldCode,
			Score: 3,
			Tags:  []string{"x", "y"},
			Count: &oldCount,
		},
	}
	same := schemaTestUnsafeAccessorRec{
		Name: "alice",
		SchemaTestUnsafeEmbeddedIndexed: SchemaTestUnsafeEmbeddedIndexed{
			Code:  &sameCode,
			Score: 3,
			Tags:  []string{"x", "y"},
			Count: &sameCount,
		},
	}

	basePtr := unsafe.Pointer(&base)
	samePtr := unsafe.Pointer(&same)

	var mods []string
	for i := range rt.Indexed {
		if rt.Indexed[i].Modified(basePtr, samePtr) {
			mods = append(mods, rt.Indexed[i].Name)
		}
	}
	if len(mods) != 0 {
		t.Fatalf("expected no modified indexed fields, got %v", mods)
	}
	var uniqueMods []string
	for i := range rt.Unique {
		if rt.Unique[i].Modified(basePtr, samePtr) {
			uniqueMods = append(uniqueMods, rt.Unique[i].Name)
		}
	}
	if len(uniqueMods) != 0 {
		t.Fatalf("expected no modified unique fields, got %v", uniqueMods)
	}

	nextCount := uint64(9)
	countChanged := schemaTestUnsafeAccessorRec{
		Name: "alice",
		SchemaTestUnsafeEmbeddedIndexed: SchemaTestUnsafeEmbeddedIndexed{
			Code:  &sameCode,
			Score: 3,
			Tags:  []string{"x", "y"},
			Count: &nextCount,
		},
	}
	mods = mods[:0]
	nextPtr := unsafe.Pointer(&countChanged)
	for i := range rt.Indexed {
		if rt.Indexed[i].Modified(basePtr, nextPtr) {
			mods = append(mods, rt.Indexed[i].Name)
		}
	}
	if !slices.Equal(mods, []string{"count"}) {
		t.Fatalf("expected only count to change, got %v", mods)
	}

	scoreChanged := schemaTestUnsafeAccessorRec{
		Name: "alice",
		SchemaTestUnsafeEmbeddedIndexed: SchemaTestUnsafeEmbeddedIndexed{
			Code:  &sameCode,
			Score: 4,
			Tags:  []string{"x", "y"},
			Count: &sameCount,
		},
	}
	mods = mods[:0]
	nextPtr = unsafe.Pointer(&scoreChanged)
	for i := range rt.Indexed {
		if rt.Indexed[i].Modified(basePtr, nextPtr) {
			mods = append(mods, rt.Indexed[i].Name)
		}
	}
	if !slices.Equal(mods, []string{"score"}) {
		t.Fatalf("expected only score to change, got %v", mods)
	}

	tagsChanged := schemaTestUnsafeAccessorRec{
		Name: "alice",
		SchemaTestUnsafeEmbeddedIndexed: SchemaTestUnsafeEmbeddedIndexed{
			Code:  &sameCode,
			Score: 3,
			Tags:  []string{"x", "z"},
			Count: &sameCount,
		},
	}
	mods = mods[:0]
	nextPtr = unsafe.Pointer(&tagsChanged)
	for i := range rt.Indexed {
		if rt.Indexed[i].Modified(basePtr, nextPtr) {
			mods = append(mods, rt.Indexed[i].Name)
		}
	}
	if !slices.Equal(mods, []string{"tags"}) {
		t.Fatalf("expected only tags to change, got %v", mods)
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
	fieldErr := rt.StringValidation[0].Validate(unsafe.Pointer(&rec))
	if fieldErr == nil || !strings.Contains(fieldErr.Error(), "exceeds limit") {
		t.Fatalf("Validate err=%v, want indexed string limit error", fieldErr)
	}
}

type schemaTestStringValidationSlicesRec struct {
	Tags   []string       `db:"tags" rbi:"index"`
	Values []schemaTestVI `db:"values" rbi:"index"`
}

func TestStringValidationChecksSlices(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestStringValidationSlicesRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if len(rt.StringValidation) != 2 {
		t.Fatalf("StringValidation len=%d want 2", len(rt.StringValidation))
	}
	long := strings.Repeat("x", indexdata.FieldStringRefMax+1)
	rec := schemaTestStringValidationSlicesRec{Tags: []string{"ok", long}, Values: []schemaTestVI{"ok"}}
	if err = rt.IndexedByName["tags"].Validate(unsafe.Pointer(&rec)); err == nil || !strings.Contains(err.Error(), `field "tags"`) {
		t.Fatalf("tags validation err=%v, want field tags limit error", err)
	}
	rec = schemaTestStringValidationSlicesRec{Tags: []string{"ok"}, Values: []schemaTestVI{schemaTestVI(long)}}
	if err = rt.IndexedByName["values"].Validate(unsafe.Pointer(&rec)); err == nil || !strings.Contains(err.Error(), `field "values"`) {
		t.Fatalf("values validation err=%v, want field values limit error", err)
	}
}

func schemaTestIndexViewContains(storage indexdata.FieldStorage, key string, id uint64) bool {
	ids := indexdata.NewFieldIndexViewFromStorage(storage).LookupPostingRetained(key)
	ok := ids.Contains(id)
	ids.Release()
	return ok
}

func schemaTestIndexViewContainsKey(storage indexdata.FieldStorage, key uint64, id uint64) bool {
	ids := indexdata.NewFieldIndexViewFromStorage(storage).LookupPostingRetainedKey(keycodec.FromU64(key))
	ok := ids.Contains(id)
	ids.Release()
	return ok
}

func TestRuntimeLookupHelpers(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestAccessorRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	info, ok := rt.IndexedByName.ResolveField("name")
	if !ok || info.Ordinal != rt.IndexedByName["name"].Ordinal {
		t.Fatalf("ResolveField(name)=(%d,%v)", info.Ordinal, ok)
	}
	wantScalarCaps := qir.FieldCapNilPredicate | qir.FieldCapPosOrder
	if info.Caps != wantScalarCaps {
		t.Fatalf("ResolveField(name) caps=%d want %d", info.Caps, wantScalarCaps)
	}
	info, ok = rt.IndexedByName.ResolveField("tags")
	if !ok || info.Caps != qir.FieldCapAll {
		t.Fatalf("ResolveField(tags) caps=%d ok=%v want %d", info.Caps, ok, qir.FieldCapAll)
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

func TestUnwrapQueryValuePreservesTypedNilValueIndexer(t *testing.T) {
	var folded *schemaTestPtrFoldedString
	v, isNil := UnwrapQueryValue(reflect.ValueOf(folded))
	if isNil {
		t.Fatal("typed nil ValueIndexer pointer was reported as nil")
	}
	if v.Kind() != reflect.Pointer || !v.IsNil() {
		t.Fatalf("unwrapped value=%v kind=%v want nil pointer", v, v.Kind())
	}
	if got := v.Interface().(ValueIndexer).IndexingValue(); got != "<nil>" {
		t.Fatalf("typed nil ValueIndexer key=%q want %q", got, "<nil>")
	}
}

func TestUnwrapQueryValueTreatsNilValueReceiverValueIndexerPointerAsNil(t *testing.T) {
	var vi *schemaTestVI
	if _, isNil := UnwrapQueryValue(reflect.ValueOf(vi)); !isNil {
		t.Fatal("typed nil pointer to value-receiver ValueIndexer was not reported as nil")
	}
}

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
