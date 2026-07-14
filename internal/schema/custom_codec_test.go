package schema

import (
	"encoding/binary"
	"errors"
	"reflect"
	"strings"
	"testing"
	"unsafe"
)

type customStringValue struct {
	value string
}

type customBytesValue struct {
	data []byte
}

func (v customBytesValue) EncodeRBI(dst []byte) ([]byte, error) { return append(dst, v.data...), nil }
func (v *customBytesValue) DecodeRBI(src []byte) error {
	v.data = append(v.data[:0], src...)
	return nil
}

func (v *customStringValue) EncodeRBI(dst []byte) ([]byte, error) {
	return append(dst, v.value...), nil
}

func (v *customStringValue) DecodeRBI(src []byte) error {
	v.value = string(src)
	return nil
}

var customCodecEncodeCalls int
var errCustomCodec = errors.New("custom codec error")

type customCountingValue struct {
	value string
	fail  bool
}

func (v customCountingValue) EncodeRBI(dst []byte) ([]byte, error) {
	customCodecEncodeCalls++
	dst = append(dst, v.value...)
	if v.fail {
		return dst, errCustomCodec
	}
	return dst, nil
}
func (v *customCountingValue) DecodeRBI(src []byte) error {
	if string(src) == "decode-error" {
		return errCustomCodec
	}
	v.value = string(src)
	return nil
}

type customValueCodec struct{}

func (customValueCodec) EncodeRBI(dst []byte) ([]byte, error) { return dst, nil }
func (customValueCodec) DecodeRBI([]byte) error               { return nil }

type customMixedCodec struct{}

func (*customMixedCodec) EncodeRBI(dst []byte) ([]byte, error) { return dst, nil }
func (customMixedCodec) DecodeRBI([]byte) error                { return nil }

type customRecursive struct {
	next *customRecursive
	n    byte
}

func (v customRecursive) EncodeRBI(dst []byte) ([]byte, error) { return append(dst, v.n), nil }
func (v *customRecursive) DecodeRBI(src []byte) error {
	v.n = src[0]
	return nil
}

type customNamedSlice []string

func (v customNamedSlice) EncodeRBI(dst []byte) ([]byte, error) {
	for i := range v {
		dst = append(dst, v[i]...)
	}
	return dst, nil
}

type customNamedMap map[string]string

func (v customNamedMap) EncodeRBI(dst []byte) ([]byte, error) { return append(dst, v["key"]...), nil }
func (v *customNamedMap) DecodeRBI(src []byte) error {
	*v = customNamedMap{"key": string(src)}
	return nil
}

type customFunc func()

func (customFunc) EncodeRBI(dst []byte) ([]byte, error) { return append(dst, 1), nil }
func (v *customFunc) DecodeRBI([]byte) error {
	*v = func() {}
	return nil
}
func (v *customNamedSlice) DecodeRBI(src []byte) error {
	*v = customNamedSlice{string(src)}
	return nil
}

type customMapKey struct{ n int }

func (v customMapKey) EncodeRBI(dst []byte) ([]byte, error) { return append(dst, byte(v.n)), nil }
func (v *customMapKey) DecodeRBI(src []byte) error {
	v.n = int(src[0])
	return nil
}

type customByte byte

func (v customByte) EncodeRBI(dst []byte) ([]byte, error) {
	if v == 0xff {
		return dst, errCustomCodec
	}
	return append(dst, byte(v)^0xa5), nil
}

func (v *customByte) DecodeRBI(src []byte) error {
	if len(src) != 1 {
		return errCustomCodec
	}
	*v = customByte(src[0] ^ 0xa5)
	return nil
}

type customRootRecord struct {
	Value int
}

type CustomAnonymous struct {
	Child  string `db:"child" rbi:"unique"`
	hidden string
}

func (v CustomAnonymous) EncodeRBI(dst []byte) ([]byte, error) {
	return append(dst, v.hidden...), nil
}
func (v *CustomAnonymous) DecodeRBI(src []byte) error {
	v.hidden = string(src)
	return nil
}
func (v CustomAnonymous) IndexingValue() string { return v.hidden }

func (v customRootRecord) EncodeRBI(dst []byte) ([]byte, error) { return append(dst, 0xff), nil }
func (v *customRootRecord) DecodeRBI([]byte) error {
	v.Value = -1
	return nil
}

func TestCustomCodecResolverContracts(t *testing.T) {
	for _, typ := range []reflect.Type{
		reflect.TypeFor[struct{ Value customValueCodec }](),
		reflect.TypeFor[struct{ Value customMixedCodec }](),
	} {
		_, err := Compile(typ, Config{})
		if err == nil || !strings.Contains(err.Error(), "pointer receiver") || !strings.Contains(err.Error(), "Value") {
			t.Fatalf("Compile value receiver codec err=%v", err)
		}
	}

	if _, err := Compile(reflect.TypeFor[struct{ Value any }](), Config{}); err == nil {
		t.Fatal("Compile accepted declared interface field")
	}
	if _, err := Compile(reflect.TypeFor[struct{ Value Codec }](), Config{}); err == nil {
		t.Fatal("Compile accepted declared Codec interface field")
	}
	if _, err := Compile(reflect.TypeFor[struct{ Value customRecursive }](), Config{}); err != nil {
		t.Fatalf("Compile recursive codec leaf: %v", err)
	}
	if _, err := Compile(reflect.TypeFor[struct{ Value *customRecursive }](), Config{}); err != nil {
		t.Fatalf("Compile pointer to recursive codec leaf: %v", err)
	}
	if _, err := Compile(reflect.TypeFor[struct{ Value customNamedSlice }](), Config{}); err != nil {
		t.Fatalf("Compile named slice codec leaf: %v", err)
	}
	if _, err := Compile(reflect.TypeFor[struct{ Value customNamedMap }](), Config{}); err != nil {
		t.Fatalf("Compile named map codec leaf: %v", err)
	}
	if _, err := Compile(reflect.TypeFor[struct{ Value customFunc }](), Config{}); err != nil {
		t.Fatalf("Compile func codec leaf: %v", err)
	}
	if _, err := Compile(reflect.TypeFor[struct{ Values map[customMapKey]string }](), Config{}); err == nil || !strings.Contains(err.Error(), "map key") {
		t.Fatalf("Compile codec map key err=%v", err)
	}
	if _, err := Compile(reflect.TypeFor[struct {
		Value customStringValue `rbi:"index"`
	}](), Config{}); err == nil || !strings.Contains(err.Error(), "ValueIndexer") {
		t.Fatalf("Compile indexed codec without ValueIndexer err=%v", err)
	}
}

func TestCustomCodecRootRemainsRecordEnvelope(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[customRootRecord](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	src := customRootRecord{Value: 42}
	payload, err := rt.Codec.Encode(unsafe.Pointer(&src), nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var dst customRootRecord
	if err = rt.Codec.Decode(payload, unsafe.Pointer(&dst)); err != nil || dst.Value != 42 {
		t.Fatalf("root envelope dst=%+v err=%v", dst, err)
	}
	dst = customRootRecord{}
	if err = rt.Clone.CloneInto(unsafe.Pointer(&src), unsafe.Pointer(&dst)); err != nil || dst.Value != 42 {
		t.Fatalf("root clone dst=%+v err=%v", dst, err)
	}
}

func TestCustomCodecAnonymousFieldIsAtomic(t *testing.T) {
	type record struct {
		CustomAnonymous `db:"opaque" rbi:"index"`
	}
	rt, err := Compile(reflect.TypeFor[record](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if _, ok := rt.Fields["opaque"]; !ok {
		t.Fatalf("anonymous codec index fields=%v", rt.Fields)
	}
	if _, ok := rt.Fields["child"]; ok {
		t.Fatalf("anonymous codec promoted child index: %v", rt.Fields)
	}
	if _, ok := rt.Patch.Fields["opaque"]; !ok {
		t.Fatalf("anonymous codec patch fields=%v", rt.Patch.Fields)
	}
	if _, ok := rt.Patch.Fields["child"]; ok {
		t.Fatalf("anonymous codec promoted child patch: %v", rt.Patch.Fields)
	}
	if len(rt.Codec.fields) != 1 || rt.Codec.fields[0].name != "opaque" || rt.Codec.fields[0].wire != codecWireCustom {
		t.Fatalf("anonymous codec wire fields=%+v", rt.Codec.fields)
	}
}

func TestCustomCodecAnonymousPointerFieldIsAtomic(t *testing.T) {
	type record struct {
		*CustomAnonymous
	}
	rt, err := Compile(reflect.TypeFor[record](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if _, ok := rt.Patch.Fields["CustomAnonymous"]; !ok {
		t.Fatalf("anonymous pointer codec patch fields=%v", rt.Patch.Fields)
	}
	if _, ok := rt.Patch.Fields["child"]; ok {
		t.Fatalf("anonymous pointer codec promoted child patch: %v", rt.Patch.Fields)
	}
	if len(rt.Codec.fields) != 1 || rt.Codec.fields[0].name != "CustomAnonymous" || rt.Codec.fields[0].wire != codecWirePointer {
		t.Fatalf("anonymous pointer codec wire fields=%+v", rt.Codec.fields)
	}
}

func TestCustomCodecErrors(t *testing.T) {
	type record struct {
		String  customStringValue
		Counted customCountingValue
	}
	rt, err := Compile(reflect.TypeFor[record](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	customCodecEncodeCalls = 0
	src := record{
		String:  customStringValue{value: "string"},
		Counted: customCountingValue{value: "counted"},
	}
	payload, err := rt.Codec.Encode(unsafe.Pointer(&src), nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if customCodecEncodeCalls != 1 {
		t.Fatalf("EncodeRBI calls=%d", customCodecEncodeCalls)
	}
	var dst record
	if err = rt.Codec.Decode(payload, unsafe.Pointer(&dst)); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if dst.String.value != "string" || dst.Counted.value != "counted" {
		t.Fatalf("decoded=%+v", dst)
	}

	src.Counted.fail = true
	partial, err := rt.Codec.Encode(unsafe.Pointer(&src), make([]byte, 0, 64))
	if !errors.Is(err, errCustomCodec) || len(partial) == 0 {
		t.Fatalf("EncodeRBI error partial_len=%d err=%v", len(partial), err)
	}

	src.Counted = customCountingValue{value: "decode-error"}
	payload, err = rt.Codec.Encode(unsafe.Pointer(&src), nil)
	if err != nil {
		t.Fatalf("Encode decode error payload: %v", err)
	}
	if err = rt.Codec.Decode(payload, unsafe.Pointer(&dst)); !errors.Is(err, errCustomCodec) || !strings.Contains(err.Error(), "Counted") {
		t.Fatalf("Decode err=%v", err)
	}
}

func TestCustomCodecCloneDoesNotSharePrivateStorage(t *testing.T) {
	type record struct {
		Value  customBytesValue
		Ptr    *customBytesValue
		Slice  []customBytesValue
		Values map[string]customBytesValue
	}
	rt, err := Compile(reflect.TypeFor[record](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	src := record{
		Value: customBytesValue{data: []byte("value")},
		Ptr:   &customBytesValue{data: []byte("pointer")},
		Slice: []customBytesValue{{data: []byte("slice")}},
		Values: map[string]customBytesValue{
			"key": {data: []byte("map")},
		},
	}
	var dst record
	if err = rt.Clone.CloneInto(unsafe.Pointer(&src), unsafe.Pointer(&dst)); err != nil {
		t.Fatalf("CloneInto: %v", err)
	}
	src.Value.data[0] = 'X'
	src.Ptr.data[0] = 'X'
	src.Slice[0].data[0] = 'X'
	mapped := src.Values["key"]
	mapped.data[0] = 'X'
	src.Values["key"] = mapped
	if string(dst.Value.data) != "value" || string(dst.Ptr.data) != "pointer" ||
		string(dst.Slice[0].data) != "slice" || string(dst.Values["key"].data) != "map" {
		t.Fatalf("clone aliases source: %#v", dst)
	}
}

func TestCustomCodecCloneNestedArrayElements(t *testing.T) {
	type record struct {
		Values [1][1]customByte
	}
	rt, err := Compile(reflect.TypeFor[record](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	src := record{Values: [1][1]customByte{{0xff}}}
	var dst record
	if err = rt.Clone.CloneInto(unsafe.Pointer(&src), unsafe.Pointer(&dst)); !errors.Is(err, errCustomCodec) || !strings.Contains(err.Error(), "Values[][]") {
		t.Fatalf("CloneInto error=%v want nested custom codec error", err)
	}
}

func TestCustomCodecExactPatchEncodesNewOnce(t *testing.T) {
	type record struct{ Value customCountingValue }
	rt, err := Compile(reflect.TypeFor[record](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	oldValue := record{Value: customCountingValue{value: "old"}}
	newValue := record{Value: customCountingValue{value: "new"}}
	acc := rt.Patch.AccessByName["Value"]
	customCodecEncodeCalls = 0
	op := rt.Patch.BeginOperation()
	value, changed, err := rt.Patch.Value(&acc, unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), true, &op)
	rt.Patch.EndOperation(&op)
	if err != nil || !changed || value.(customCountingValue).value != "new" {
		t.Fatalf("patch value=%#v changed=%v err=%v", value, changed, err)
	}
	if customCodecEncodeCalls != 2 {
		t.Fatalf("patch EncodeRBI calls=%d", customCodecEncodeCalls)
	}
}

func TestCustomCodecPatchMapComparisonAndCopy(t *testing.T) {
	type mapValue struct {
		Value customBytesValue
		Count int
	}
	type record struct {
		Values map[string]mapValue
	}
	rt, err := Compile(reflect.TypeFor[record](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	oldValue := record{Values: map[string]mapValue{
		"first":  {Value: customBytesValue{data: []byte("same")}, Count: 1},
		"second": {Value: customBytesValue{data: []byte("stable")}, Count: 2},
	}}
	equalValue := record{Values: map[string]mapValue{
		"first":  {Value: customBytesValue{data: []byte("same")}, Count: 1},
		"second": {Value: customBytesValue{data: []byte("stable")}, Count: 2},
	}}
	missingKeyValue := record{Values: map[string]mapValue{
		"first": {Value: customBytesValue{data: []byte("same")}, Count: 1},
		"third": {Value: customBytesValue{data: []byte("stable")}, Count: 2},
	}}
	newValue := record{Values: map[string]mapValue{
		"first":  {Value: customBytesValue{data: []byte("changed")}, Count: 1},
		"second": {Value: customBytesValue{data: []byte("stable")}, Count: 2},
	}}
	acc := rt.Patch.AccessByName["Values"]
	op := rt.Patch.BeginOperation()
	defer rt.Patch.EndOperation(&op)

	value, changed, err := rt.Patch.Value(&acc, unsafe.Pointer(&oldValue), unsafe.Pointer(&equalValue), true, &op)
	if err != nil || changed || value != nil {
		t.Fatalf("equal map patch value=%#v changed=%v err=%v", value, changed, err)
	}
	value, changed, err = rt.Patch.Value(&acc, unsafe.Pointer(&oldValue), unsafe.Pointer(&missingKeyValue), true, &op)
	if err != nil || !changed {
		t.Fatalf("missing map key patch value=%#v changed=%v err=%v", value, changed, err)
	}
	value, changed, err = rt.Patch.Value(&acc, unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), true, &op)
	if err != nil || !changed {
		t.Fatalf("changed map patch value=%#v changed=%v err=%v", value, changed, err)
	}
	copied := value.(map[string]mapValue)
	if string(copied["first"].Value.data) != "changed" || copied["second"].Count != 2 {
		t.Fatalf("copied map=%#v", copied)
	}
	mapped := newValue.Values["first"]
	mapped.Value.data[0] = 'X'
	newValue.Values["first"] = mapped
	if string(copied["first"].Value.data) != "changed" {
		t.Fatalf("patch copy aliases source: %#v", copied)
	}
}

func TestCustomCodecPatchLargeStringMapValue(t *testing.T) {
	type mapValue struct {
		Padding [128]byte
		Value   customByte
	}
	type record struct {
		Values map[string]mapValue
	}
	rt, err := Compile(reflect.TypeFor[record](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	oldValue := record{Values: map[string]mapValue{
		"key": {Value: 1},
	}}
	equalValue := record{Values: map[string]mapValue{
		"key": {Value: 1},
	}}
	newValue := record{Values: map[string]mapValue{
		"key": {Value: 2},
	}}
	acc := rt.Patch.AccessByName["Values"]
	op := rt.Patch.BeginOperation()
	defer rt.Patch.EndOperation(&op)

	value, changed, err := rt.Patch.Value(&acc, unsafe.Pointer(&oldValue), unsafe.Pointer(&equalValue), true, &op)
	if err != nil || changed || value != nil {
		t.Fatalf("equal map patch value=%#v changed=%v err=%v", value, changed, err)
	}
	value, changed, err = rt.Patch.Value(&acc, unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), true, &op)
	if err != nil || !changed || value.(map[string]mapValue)["key"].Value != 2 {
		t.Fatalf("changed map patch value=%#v changed=%v err=%v", value, changed, err)
	}
}

func TestCustomCodecPatchMapGeneralLookup(t *testing.T) {
	type record struct {
		Values map[int]customStringValue
	}
	rt, err := Compile(reflect.TypeFor[record](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	oldValue := record{Values: map[int]customStringValue{
		1: {value: "first"},
		2: {value: "second"},
	}}
	equalValue := record{Values: map[int]customStringValue{
		1: {value: "first"},
		2: {value: "second"},
	}}
	missingKeyValue := record{Values: map[int]customStringValue{
		1: {value: "first"},
		3: {value: "second"},
	}}
	acc := rt.Patch.AccessByName["Values"]
	op := rt.Patch.BeginOperation()
	defer rt.Patch.EndOperation(&op)

	value, changed, err := rt.Patch.Value(&acc, unsafe.Pointer(&oldValue), unsafe.Pointer(&equalValue), true, &op)
	if err != nil || changed || value != nil {
		t.Fatalf("equal map patch value=%#v changed=%v err=%v", value, changed, err)
	}
	value, changed, err = rt.Patch.Value(&acc, unsafe.Pointer(&oldValue), unsafe.Pointer(&missingKeyValue), true, &op)
	if err != nil || !changed {
		t.Fatalf("missing map key patch value=%#v changed=%v err=%v", value, changed, err)
	}
}

func TestCustomCodecPatchMapComparisonAllocationsDoNotScale(t *testing.T) {
	type record struct {
		Values map[int]customStringValue
	}
	rt, err := Compile(reflect.TypeFor[record](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	acc := rt.Patch.AccessByName["Values"]
	measure := func(size int) float64 {
		oldValue := record{Values: make(map[int]customStringValue, size)}
		equalValue := record{Values: make(map[int]customStringValue, size)}
		for i := 0; i < size; i++ {
			oldValue.Values[i] = customStringValue{value: "value"}
			equalValue.Values[i] = customStringValue{value: "value"}
		}
		var changed bool
		var patchErr error
		allocs := testing.AllocsPerRun(100, func() {
			op := rt.Patch.BeginOperation()
			_, changed, patchErr = rt.Patch.Value(&acc, unsafe.Pointer(&oldValue), unsafe.Pointer(&equalValue), true, &op)
			rt.Patch.EndOperation(&op)
		})
		if patchErr != nil || changed {
			t.Fatalf("equal map size=%d changed=%v err=%v", size, changed, patchErr)
		}
		return allocs
	}

	small := measure(1)
	large := measure(100)
	if large > small+1 {
		t.Fatalf("map comparison allocations scale with elements: size 1=%v size 100=%v", small, large)
	}
}

func TestCustomCodecPatchPointerNilTransitions(t *testing.T) {
	type record struct{ Value *customStringValue }
	rt, err := Compile(reflect.TypeFor[record](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	value := &customStringValue{}
	oldValue := record{}
	newValue := record{Value: value}
	acc := rt.Patch.AccessByName["Value"]
	op := rt.Patch.BeginOperation()
	copied, changed, err := rt.Patch.Value(&acc, unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), true, &op)
	if err != nil || !changed || copied.(*customStringValue) == nil || copied.(*customStringValue) == value {
		rt.Patch.EndOperation(&op)
		t.Fatalf("nil to value copied=%#v changed=%v err=%v", copied, changed, err)
	}
	copied, changed, err = rt.Patch.Value(&acc, unsafe.Pointer(&newValue), unsafe.Pointer(&oldValue), true, &op)
	rt.Patch.EndOperation(&op)
	if err != nil || !changed || copied.(*customStringValue) != nil {
		t.Fatalf("value to nil copied=%#v changed=%v err=%v", copied, changed, err)
	}
}

func TestCustomCodecInnerLengthAndMissingField(t *testing.T) {
	type record struct {
		Values []customStringValue
	}
	rt, err := Compile(reflect.TypeFor[record](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	src := record{Values: []customStringValue{{value: "payload"}}}
	payload, err := rt.Codec.Encode(unsafe.Pointer(&src), nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	wire, fieldPayload := schemaCodecEncodedField(t, payload, "Values")
	if wire != codecWireSlice || len(fieldPayload) < 6 {
		t.Fatalf("wire=%d payload=%x", wire, fieldPayload)
	}
	binary.LittleEndian.PutUint32(fieldPayload[2:6], ^uint32(0))
	var dst record
	if err = rt.Codec.Decode(payload, unsafe.Pointer(&dst)); err == nil || !strings.Contains(err.Error(), "custom payload length") {
		t.Fatalf("Decode malformed length err=%v", err)
	}

	dst.Values = []customStringValue{{value: "stale"}}
	missing := schemaCodecTestPayload()
	if err = rt.Codec.Decode(missing, unsafe.Pointer(&dst)); err != nil || dst.Values != nil {
		t.Fatalf("Decode missing field dst=%+v err=%v", dst, err)
	}
	unknown := schemaCodecTestPayload(schemaCodecTestField{name: "Unknown", wire: codecWireCustom, payload: []byte("opaque")})
	if err = rt.Codec.Decode(unknown, unsafe.Pointer(&dst)); err != nil {
		t.Fatalf("Decode unknown custom field: %v", err)
	}
}

func TestCustomCodecPointerShapesAndEmptyPayload(t *testing.T) {
	type record struct {
		Value  customStringValue
		Ptr    *customStringValue
		Double **customStringValue
		Nil    *customStringValue
	}
	rt, err := Compile(reflect.TypeFor[record](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	value := &customStringValue{}
	double := &value
	src := record{Ptr: value, Double: double}
	payload, err := rt.Codec.Encode(unsafe.Pointer(&src), nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var dst record
	if err = rt.Codec.Decode(payload, unsafe.Pointer(&dst)); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if dst.Value.value != "" || dst.Ptr == nil || dst.Ptr.value != "" ||
		dst.Double == nil || *dst.Double == nil || (**dst.Double).value != "" || dst.Nil != nil {
		t.Fatalf("decoded pointer shapes=%#v", dst)
	}
}

func TestCustomCodecNamedByteSliceElements(t *testing.T) {
	type record struct {
		Values []customByte
	}
	rt, err := Compile(reflect.TypeFor[record](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	src := record{Values: []customByte{1, 2, 3}}
	payload, err := rt.Codec.Encode(unsafe.Pointer(&src), nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	wire, _ := schemaCodecEncodedField(t, payload, "Values")
	if wire != codecWireSlice {
		t.Fatalf("wire=%d want codec element slice wire %d", wire, codecWireSlice)
	}
	var dst record
	if err = rt.Codec.Decode(payload, unsafe.Pointer(&dst)); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !reflect.DeepEqual(dst.Values, src.Values) {
		t.Fatalf("decoded values=%v want %v", dst.Values, src.Values)
	}

	src.Values = []customByte{0xff}
	if _, err = rt.Codec.Encode(unsafe.Pointer(&src), nil); !errors.Is(err, errCustomCodec) || !strings.Contains(err.Error(), "Values") {
		t.Fatalf("Encode error=%v want field-wrapped custom codec error", err)
	}
}

func TestCustomNamedContainersAreOpaqueLeaves(t *testing.T) {
	type record struct {
		Slice customNamedSlice
		Map   customNamedMap
		Func  customFunc
	}
	rt, err := Compile(reflect.TypeFor[record](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	src := record{Slice: customNamedSlice{"slice"}, Map: customNamedMap{"key": "map"}}
	payload, err := rt.Codec.Encode(unsafe.Pointer(&src), nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var dst record
	if err = rt.Codec.Decode(payload, unsafe.Pointer(&dst)); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(dst.Slice) != 1 || dst.Slice[0] != "slice" || dst.Map["key"] != "map" || dst.Func == nil {
		t.Fatalf("decoded named containers=%#v", dst)
	}
}

func TestCustomCodecMissingDirectFieldIsZeroed(t *testing.T) {
	type record struct{ Value customStringValue }
	rt, err := Compile(reflect.TypeFor[record](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	dst := record{Value: customStringValue{value: "stale"}}
	if err = rt.Codec.Decode(schemaCodecTestPayload(), unsafe.Pointer(&dst)); err != nil || dst.Value.value != "" {
		t.Fatalf("Decode missing direct custom field dst=%+v err=%v", dst, err)
	}
}

var customCodecAllocsSink []byte

func TestCustomCodecCompiledItabDoesNotAllocate(t *testing.T) {
	leaf, err := resolveCodecLeaf(reflect.TypeFor[customStringValue](), "Value")
	if err != nil || leaf == nil {
		t.Fatalf("resolve leaf=%v err=%v", leaf, err)
	}
	value := customStringValue{value: "payload"}
	buf := make([]byte, 0, 64)
	allocs := testing.AllocsPerRun(1000, func() {
		var encodeErr error
		customCodecAllocsSink, encodeErr = leaf.encode(unsafe.Pointer(&value), buf[:0])
		if encodeErr != nil {
			panic(encodeErr)
		}
	})
	if allocs != 0 {
		t.Fatalf("compiled EncodeRBI allocs/run=%v", allocs)
	}
}

func TestCustomCodecRuntimeDoesNotAllocate(t *testing.T) {
	type record struct {
		Value customStringValue
	}
	rt, err := Compile(reflect.TypeFor[record](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	value := record{Value: customStringValue{value: "payload"}}
	buf := make([]byte, 0, 128)
	allocs := testing.AllocsPerRun(1000, func() {
		customCodecAllocsSink, err = rt.Codec.Encode(unsafe.Pointer(&value), buf[:0])
		if err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("CodecRuntime EncodeRBI allocs/run=%v", allocs)
	}
}

func BenchmarkCustomCodecEncode(b *testing.B) {
	type record struct{ Value customStringValue }
	rt, err := Compile(reflect.TypeFor[record](), Config{})
	if err != nil {
		b.Fatal(err)
	}
	value := record{Value: customStringValue{value: "custom-payload"}}
	buf := make([]byte, 0, 128)
	b.ReportAllocs()
	for b.Loop() {
		buf, err = rt.Codec.Encode(unsafe.Pointer(&value), buf[:0])
		if err != nil {
			b.Fatal(err)
		}
	}
	customCodecAllocsSink = buf
}

func BenchmarkCustomCodecDecode(b *testing.B) {
	type record struct{ Value customStringValue }
	rt, err := Compile(reflect.TypeFor[record](), Config{})
	if err != nil {
		b.Fatal(err)
	}
	src := record{Value: customStringValue{value: "custom-payload"}}
	payload, err := rt.Codec.Encode(unsafe.Pointer(&src), nil)
	if err != nil {
		b.Fatal(err)
	}
	var dst record
	b.ReportAllocs()
	for b.Loop() {
		if err = rt.Codec.Decode(payload, unsafe.Pointer(&dst)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCustomCodecEncodeSlice(b *testing.B) {
	type record struct{ Values []customStringValue }
	rt, err := Compile(reflect.TypeFor[record](), Config{})
	if err != nil {
		b.Fatal(err)
	}
	value := record{Values: []customStringValue{{value: "first"}, {value: "second"}, {value: "third"}}}
	buf := make([]byte, 0, 256)
	b.ReportAllocs()
	for b.Loop() {
		buf, err = rt.Codec.Encode(unsafe.Pointer(&value), buf[:0])
		if err != nil {
			b.Fatal(err)
		}
	}
	customCodecAllocsSink = buf
}

func BenchmarkCustomCodecClone(b *testing.B) {
	type record struct {
		First  customStringValue
		Second customStringValue
	}
	rt, err := Compile(reflect.TypeFor[record](), Config{})
	if err != nil {
		b.Fatal(err)
	}
	src := record{First: customStringValue{value: "first"}, Second: customStringValue{value: "second"}}
	var dst record
	b.ReportAllocs()
	for b.Loop() {
		if err = rt.Clone.CloneInto(unsafe.Pointer(&src), unsafe.Pointer(&dst)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCustomCodecPatch(b *testing.B) {
	type record struct{ Value customStringValue }
	rt, err := Compile(reflect.TypeFor[record](), Config{})
	if err != nil {
		b.Fatal(err)
	}
	oldValue := record{Value: customStringValue{value: "old"}}
	newValue := record{Value: customStringValue{value: "new"}}
	acc := rt.Patch.AccessByName["Value"]
	b.Run("Equal", func(b *testing.B) {
		for b.Loop() {
			op := rt.Patch.BeginOperation()
			_, _, err = rt.Patch.Value(&acc, unsafe.Pointer(&oldValue), unsafe.Pointer(&oldValue), true, &op)
			rt.Patch.EndOperation(&op)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Changed", func(b *testing.B) {
		for b.Loop() {
			op := rt.Patch.BeginOperation()
			_, _, err = rt.Patch.Value(&acc, unsafe.Pointer(&oldValue), unsafe.Pointer(&newValue), true, &op)
			rt.Patch.EndOperation(&op)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	type mapRecord struct {
		Values map[int]customStringValue
	}
	mapRT, err := Compile(reflect.TypeFor[mapRecord](), Config{})
	if err != nil {
		b.Fatal(err)
	}
	oldMap := mapRecord{Values: make(map[int]customStringValue, 100)}
	equalMap := mapRecord{Values: make(map[int]customStringValue, 100)}
	for i := 0; i < 100; i++ {
		oldMap.Values[i] = customStringValue{value: "value"}
		equalMap.Values[i] = customStringValue{value: "value"}
	}
	mapAcc := mapRT.Patch.AccessByName["Values"]
	b.Run("MapEqual100", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			op := mapRT.Patch.BeginOperation()
			_, changed, patchErr := mapRT.Patch.Value(&mapAcc, unsafe.Pointer(&oldMap), unsafe.Pointer(&equalMap), true, &op)
			mapRT.Patch.EndOperation(&op)
			if patchErr != nil {
				b.Fatal(patchErr)
			}
			if changed {
				b.Fatal("equal maps reported as changed")
			}
		}
	})
}
