package schema

import (
	"bytes"
	"encoding/binary"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"
)

type schemaCodecNestedRec struct {
	Code  string `db:"code"`
	Count int16  `db:"count"`
}

type schemaCodecSupportedRec struct {
	ID     uint64 `db:"id"`
	Count  int64
	Age    int32
	Score  float32
	Active bool
	Name   string `db:"name"`
	At     time.Time
	Nested schemaCodecNestedRec `db:"nested"`
}

type schemaCodecNumericRec struct {
	I32 int32  `db:"i32"`
	U16 uint16 `db:"u16"`
}

type SchemaCodecPromotedLeft struct {
	X string
}

type SchemaCodecPromotedRight struct {
	X string
}

type schemaCodecAmbiguousPromotedRec struct {
	SchemaCodecPromotedLeft
	SchemaCodecPromotedRight
}

type schemaCodecShadowedPromotedRec struct {
	SchemaCodecPromotedLeft
	X string
}

type SchemaCodecTaggedPromotedLeft struct {
	X string `db:"left_x"`
}

type SchemaCodecTaggedPromotedRight struct {
	X string `db:"right_x"`
}

type schemaCodecTaggedPromotedRec struct {
	SchemaCodecTaggedPromotedLeft
	SchemaCodecTaggedPromotedRight
}

type SchemaCodecMovedEmbedded struct {
	Value string `db:"value"`
	Count int32  `db:"count"`
}

type schemaCodecEmbeddedMoveRec struct {
	SchemaCodecMovedEmbedded
}

type schemaCodecFlatMoveRec struct {
	Value string `db:"value"`
	Count int32  `db:"count"`
}

type schemaCodecIntegerRec struct {
	Value int64  `db:"value"`
	U     uint64 `db:"u"`
}

type schemaCodecFloat64Rec struct {
	Value float64 `db:"value"`
	U     float64 `db:"u"`
}

type schemaCodecFloat32Rec struct {
	Value float32 `db:"value"`
}

type schemaCodecFloatCompositeNumericRec struct {
	Values []float64          `db:"values"`
	Ptr    *float64           `db:"ptr"`
	Labels map[string]float64 `db:"labels"`
}

type schemaCodecIntegerCompositeNumericRec struct {
	Values []int64           `db:"values"`
	Ptr    *int64            `db:"ptr"`
	Labels map[string]uint64 `db:"labels"`
}

type schemaCodecCompositeNested struct {
	Label  string
	Ptr    *string
	Fixed  [2]int16
	Values []schemaCodecNestedRec
}

type schemaCodecCompositeRec struct {
	Name       string
	IntPtr     *int64
	StringPtr  *string
	TimePtr    *time.Time
	StructPtr  *schemaCodecNestedRec
	NilPtr     *int64
	FixedInts  [3]int32
	FixedItems [2]schemaCodecNestedRec
	Tags       []string
	Scores     []int64
	Windows    []schemaCodecNestedRec
	EmptyTags  []string
	NilTags    []string
	Nested     schemaCodecCompositeNested
	ComplexPtr *schemaCodecCompositeNested
	Complexes  []schemaCodecCompositeNested
}

type schemaCodecNamedBytes []byte

type schemaCodecBytesRec struct {
	Blob  []byte                `db:"blob"`
	Empty []byte                `db:"empty"`
	Nil   []byte                `db:"nil"`
	Alias schemaCodecNamedBytes `db:"alias"`
}

type schemaCodecFloatSpecialRec struct {
	PosInf float32   `db:"pos_inf"`
	NegInf float32   `db:"neg_inf"`
	Values []float32 `db:"values"`
}

type schemaCodecUnsignedCompositeRec struct {
	Values []uint64 `db:"values"`
}

type schemaCodecSignedCompositeRec struct {
	Values []int64 `db:"values"`
}

type schemaCodecSignedPtrRec struct {
	Value *int64 `db:"value"`
}

type schemaCodecUnsignedPtrRec struct {
	Value *uint64 `db:"value"`
}

type schemaCodecUnsignedArrayMapRec struct {
	Fixed  [1]uint64         `db:"fixed"`
	Labels map[string]uint64 `db:"labels"`
}

type schemaCodecSignedArrayMapRec struct {
	Fixed  [1]int64         `db:"fixed"`
	Labels map[string]int64 `db:"labels"`
}

type schemaCodecMalformedCompositeLengthRec struct {
	Tags   []string          `db:"tags"`
	Labels map[string]string `db:"labels"`
}

type schemaCodecLargeArraySliceRec struct {
	Values [][16]uint64 `db:"values"`
}

type schemaCodecMapNested struct {
	Labels map[string]string
	Counts map[uint16]int32
}

type schemaCodecMapKeySub struct {
	Flag bool
	Code string
}

type schemaCodecMapKey struct {
	Tenant string
	Shard  uint16
	Parts  [2]int8
	Sub    schemaCodecMapKeySub
}

type schemaCodecMapRec struct {
	Name         string
	Strings      map[string]string
	Ints         map[string]int64
	Times        map[string]time.Time
	Structs      map[string]schemaCodecNestedRec
	Pointers     map[string]*schemaCodecNestedRec
	Arrays       map[string][2]int16
	Slices       map[string][]string
	SignedKeys   map[int16]string
	UnsignedKeys map[uint64]float64
	StructKeys   map[schemaCodecMapKey]string
	ArrayKeys    map[[2]schemaCodecMapKeySub]int64
	Empty        map[string]int64
	Nil          map[string]int64
	Nested       schemaCodecMapNested
}

type schemaCodecIgnoredNested struct {
	Keep     string
	DBSkip   string `db:"-"`
	RBISkip  string `db:"skip" rbi:"-"`
	Disabled []byte `rbi:"-"`
}

type schemaCodecIgnoredRec struct {
	Name     string
	DBSkip   string `db:"-"`
	RBISkip  string `db:"skip" rbi:"-"`
	Disabled []byte `rbi:"-"`
	Nested   schemaCodecIgnoredNested
}

type schemaCodecUnsupportedMapKeyRec struct {
	Bad map[float64]string
}

type schemaCodecUnsupportedMapValueRec struct {
	Bad map[string]complex64
}

type schemaCodecUnsupportedMapTimeKeyRec struct {
	Bad map[time.Time]string
}

type schemaCodecUnsupportedMapPointerKeyRec struct {
	Bad map[*int]string
}

type schemaCodecUnsupportedMapHiddenKey struct {
	Exported string
	hidden   string
}

type schemaCodecUnsupportedMapHiddenKeyRec struct {
	Bad map[schemaCodecUnsupportedMapHiddenKey]string
}

type schemaCodecUnsupportedMapDBIgnoredKey struct {
	Exported string
	Ignored  string `db:"-"`
}

type schemaCodecUnsupportedMapDBIgnoredKeyRec struct {
	Bad map[schemaCodecUnsupportedMapDBIgnoredKey]string
}

type schemaCodecUnsupportedMapRBIIgnoredKey struct {
	Exported string
	Ignored  string `rbi:"-"`
}

type schemaCodecUnsupportedMapRBIIgnoredKeyRec struct {
	Bad map[schemaCodecUnsupportedMapRBIIgnoredKey]string
}

func TestCodecRuntimeScalarStringTimeRoundTrip(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaCodecSupportedRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	src := schemaCodecSupportedRec{
		ID:     100,
		Count:  -20,
		Age:    33,
		Score:  1.25,
		Active: true,
		Name:   "alice",
		At:     time.Unix(1_700_000_000, 123).UTC(),
		Nested: schemaCodecNestedRec{
			Code:  "x",
			Count: 7,
		},
	}
	var buf bytes.Buffer
	rt.Codec.Encode(unsafe.Pointer(&src), &buf)

	var dst schemaCodecSupportedRec
	if err = rt.Codec.Decode(buf.Bytes(), unsafe.Pointer(&dst)); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if dst.ID != src.ID || dst.Count != src.Count || dst.Age != src.Age ||
		dst.Score != src.Score || dst.Active != src.Active || dst.Name != src.Name ||
		dst.Nested != src.Nested || !dst.At.Equal(src.At) {
		t.Fatalf("decoded record mismatch:\ngot  %#v\nwant %#v", dst, src)
	}
}

func TestCodecRuntimeRejectsInvalidTimeNanoseconds(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaCodecSupportedRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	payload := schemaCodecTestPayload(
		schemaCodecTestField{name: "At", wire: codecWireTime, payload: schemaCodecTestTimePayload(10, 1_000_000_000)},
	)
	var dst schemaCodecSupportedRec
	if err = rt.Codec.Decode(payload, unsafe.Pointer(&dst)); err == nil || !strings.Contains(err.Error(), "malformed time payload") {
		t.Fatalf("Decode invalid time field err=%v want malformed time payload", err)
	}

	type timeSliceRec struct {
		Times []time.Time
	}
	sliceRT, err := Compile(reflect.TypeFor[timeSliceRec](), Config{})
	if err != nil {
		t.Fatalf("Compile time slice: %v", err)
	}
	var slicePayload bytes.Buffer
	slicePayload.WriteByte(1)
	codecWriteUvarint(&slicePayload, 1)
	slicePayload.Write(schemaCodecTestTimePayload(10, 1_000_000_000))

	payload = schemaCodecTestPayload(
		schemaCodecTestField{name: "Times", wire: codecWireSlice, payload: slicePayload.Bytes()},
	)
	var sliceDst timeSliceRec
	if err = sliceRT.Codec.Decode(payload, unsafe.Pointer(&sliceDst)); err == nil || !strings.Contains(err.Error(), "malformed time payload") {
		t.Fatalf("Decode invalid time slice err=%v want malformed time payload", err)
	}
}

func TestCodecRuntimeSkipsUnknownAndZerosMissingFields(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaCodecSupportedRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	payload := schemaCodecTestPayload(
		schemaCodecTestField{name: "unknown", wire: codecWireString, payload: []byte("ignored")},
		schemaCodecTestField{name: "name", wire: codecWireString, payload: []byte("bob")},
	)
	dst := schemaCodecSupportedRec{
		ID:     999,
		Count:  888,
		Age:    77,
		Score:  66,
		Active: true,
		Name:   "stale",
		At:     time.Unix(1, 0),
		Nested: schemaCodecNestedRec{Code: "stale", Count: 9},
	}
	if err = rt.Codec.Decode(payload, unsafe.Pointer(&dst)); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if dst.Name != "bob" {
		t.Fatalf("known field not decoded: %#v", dst)
	}
	if dst.ID != 0 || dst.Count != 0 || dst.Age != 0 || dst.Score != 0 || dst.Active ||
		!dst.At.IsZero() || dst.Nested != (schemaCodecNestedRec{}) {
		t.Fatalf("missing fields were not zeroed: %#v", dst)
	}
}

func TestCodecRuntimeNumericSchemaChanges(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaCodecNumericRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	var iPayload [8]byte
	binary.LittleEndian.PutUint64(iPayload[:], uint64(int64(123)))
	var uPayload [8]byte
	binary.LittleEndian.PutUint64(uPayload[:], uint64(65535))
	payload := schemaCodecTestPayload(
		schemaCodecTestField{name: "i32", wire: codecWireInt, payload: iPayload[:]},
		schemaCodecTestField{name: "u16", wire: codecWireUint, payload: uPayload[:]},
	)
	var dst schemaCodecNumericRec
	if err = rt.Codec.Decode(payload, unsafe.Pointer(&dst)); err != nil {
		t.Fatalf("Decode compatible numeric changes: %v", err)
	}
	if dst.I32 != 123 || dst.U16 != math.MaxUint16 {
		t.Fatalf("decoded numeric values=%+v", dst)
	}

	binary.LittleEndian.PutUint64(iPayload[:], uint64(int64(math.MaxInt32)+1))
	overflow := schemaCodecTestPayload(schemaCodecTestField{name: "i32", wire: codecWireInt, payload: iPayload[:]})
	if err = rt.Codec.Decode(overflow, unsafe.Pointer(&dst)); err == nil || !strings.Contains(err.Error(), "overflows") {
		t.Fatalf("Decode overflow err=%v want overflow", err)
	}

	negative := int64(-1)
	binary.LittleEndian.PutUint64(iPayload[:], uint64(negative))
	negativeUnsigned := schemaCodecTestPayload(schemaCodecTestField{name: "u16", wire: codecWireInt, payload: iPayload[:]})
	if err = rt.Codec.Decode(negativeUnsigned, unsafe.Pointer(&dst)); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("Decode negative unsigned err=%v want negative overflow", err)
	}
}

func TestCodecRuntimeRejectsDroppedPromotedFields(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  reflect.Type
		want string
	}{
		{name: "ambiguous", typ: reflect.TypeFor[schemaCodecAmbiguousPromotedRec](), want: "ambiguous promoted field"},
		{name: "shadowed", typ: reflect.TypeFor[schemaCodecShadowedPromotedRec](), want: "shadowed by direct field"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Compile(tc.typ, Config{})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Compile err=%v want %q", err, tc.want)
			}
		})
	}
}

func TestCodecRuntimeTaggedPromotedFieldsRoundTrip(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaCodecTaggedPromotedRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	src := schemaCodecTaggedPromotedRec{
		SchemaCodecTaggedPromotedLeft:  SchemaCodecTaggedPromotedLeft{X: "left"},
		SchemaCodecTaggedPromotedRight: SchemaCodecTaggedPromotedRight{X: "right"},
	}
	var buf bytes.Buffer
	rt.Codec.Encode(unsafe.Pointer(&src), &buf)

	var dst schemaCodecTaggedPromotedRec
	if err = rt.Codec.Decode(buf.Bytes(), unsafe.Pointer(&dst)); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if dst != src {
		t.Fatalf("decoded tagged promoted fields=%+v want %+v", dst, src)
	}
}

func TestCodecRuntimeEmbeddedTopLevelSchemaMovement(t *testing.T) {
	embeddedRT, err := Compile(reflect.TypeFor[schemaCodecEmbeddedMoveRec](), Config{})
	if err != nil {
		t.Fatalf("Compile embedded: %v", err)
	}
	flatRT, err := Compile(reflect.TypeFor[schemaCodecFlatMoveRec](), Config{})
	if err != nil {
		t.Fatalf("Compile flat: %v", err)
	}

	embeddedSrc := schemaCodecEmbeddedMoveRec{SchemaCodecMovedEmbedded: SchemaCodecMovedEmbedded{Value: "embedded", Count: 7}}
	var buf bytes.Buffer
	embeddedRT.Codec.Encode(unsafe.Pointer(&embeddedSrc), &buf)
	var flatDst schemaCodecFlatMoveRec
	if err = flatRT.Codec.Decode(buf.Bytes(), unsafe.Pointer(&flatDst)); err != nil {
		t.Fatalf("Decode embedded as flat: %v", err)
	}
	if flatDst.Value != embeddedSrc.Value || flatDst.Count != embeddedSrc.Count {
		t.Fatalf("embedded to flat mismatch: %+v", flatDst)
	}

	flatSrc := schemaCodecFlatMoveRec{Value: "flat", Count: 9}
	buf.Reset()
	flatRT.Codec.Encode(unsafe.Pointer(&flatSrc), &buf)
	var embeddedDst schemaCodecEmbeddedMoveRec
	if err = embeddedRT.Codec.Decode(buf.Bytes(), unsafe.Pointer(&embeddedDst)); err != nil {
		t.Fatalf("Decode flat as embedded: %v", err)
	}
	if embeddedDst.Value != flatSrc.Value || embeddedDst.Count != flatSrc.Count {
		t.Fatalf("flat to embedded mismatch: %+v", embeddedDst)
	}
}

func TestCodecRuntimeIntegerWiresDecodeAsRepresentableFloats(t *testing.T) {
	intRT, err := Compile(reflect.TypeFor[schemaCodecIntegerRec](), Config{})
	if err != nil {
		t.Fatalf("Compile integer: %v", err)
	}
	floatRT, err := Compile(reflect.TypeFor[schemaCodecFloat64Rec](), Config{})
	if err != nil {
		t.Fatalf("Compile float: %v", err)
	}

	src := schemaCodecIntegerRec{Value: -123, U: 1 << 60}
	var buf bytes.Buffer
	intRT.Codec.Encode(unsafe.Pointer(&src), &buf)

	var dst schemaCodecFloat64Rec
	if err = floatRT.Codec.Decode(buf.Bytes(), unsafe.Pointer(&dst)); err != nil {
		t.Fatalf("Decode integer wires as floats: %v", err)
	}
	if dst.Value != -123 || dst.U != float64(uint64(1<<60)) {
		t.Fatalf("decoded floats=%+v", dst)
	}
}

func TestCodecRuntimeFloatWiresDecodeAsRepresentableIntegers(t *testing.T) {
	floatRT, err := Compile(reflect.TypeFor[schemaCodecFloat64Rec](), Config{})
	if err != nil {
		t.Fatalf("Compile float: %v", err)
	}
	intRT, err := Compile(reflect.TypeFor[schemaCodecIntegerRec](), Config{})
	if err != nil {
		t.Fatalf("Compile integer: %v", err)
	}

	src := schemaCodecFloat64Rec{Value: -123, U: 1 << 40}
	var buf bytes.Buffer
	floatRT.Codec.Encode(unsafe.Pointer(&src), &buf)

	var dst schemaCodecIntegerRec
	if err = intRT.Codec.Decode(buf.Bytes(), unsafe.Pointer(&dst)); err != nil {
		t.Fatalf("Decode float wires as integers: %v", err)
	}
	if dst.Value != -123 || dst.U != 1<<40 {
		t.Fatalf("decoded integers=%+v", dst)
	}
}

func TestCodecRuntimeRejectsLossyFloatIntegerEvolution(t *testing.T) {
	floatRT, err := Compile(reflect.TypeFor[schemaCodecFloat64Rec](), Config{})
	if err != nil {
		t.Fatalf("Compile float: %v", err)
	}
	intRT, err := Compile(reflect.TypeFor[schemaCodecIntegerRec](), Config{})
	if err != nil {
		t.Fatalf("Compile integer: %v", err)
	}

	src := schemaCodecFloat64Rec{Value: 1.5}
	var buf bytes.Buffer
	floatRT.Codec.Encode(unsafe.Pointer(&src), &buf)
	var dst schemaCodecIntegerRec
	if err = intRT.Codec.Decode(buf.Bytes(), unsafe.Pointer(&dst)); err == nil || !strings.Contains(err.Error(), "cannot be represented exactly") {
		t.Fatalf("Decode fractional float as integer err=%v want exactness error", err)
	}

	src = schemaCodecFloat64Rec{U: -1}
	buf.Reset()
	floatRT.Codec.Encode(unsafe.Pointer(&src), &buf)
	if err = intRT.Codec.Decode(buf.Bytes(), unsafe.Pointer(&dst)); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("Decode negative float as unsigned err=%v want negative overflow", err)
	}

	src = schemaCodecFloat64Rec{Value: math.Ldexp(1, 63)}
	buf.Reset()
	floatRT.Codec.Encode(unsafe.Pointer(&src), &buf)
	if err = intRT.Codec.Decode(buf.Bytes(), unsafe.Pointer(&dst)); err == nil || !strings.Contains(err.Error(), "overflows") {
		t.Fatalf("Decode overflowing float as integer err=%v want overflow", err)
	}
}

func TestCodecRuntimeCompositeFloatWiresDecodeAsRepresentableIntegers(t *testing.T) {
	floatRT, err := Compile(reflect.TypeFor[schemaCodecFloatCompositeNumericRec](), Config{})
	if err != nil {
		t.Fatalf("Compile float composite: %v", err)
	}
	intRT, err := Compile(reflect.TypeFor[schemaCodecIntegerCompositeNumericRec](), Config{})
	if err != nil {
		t.Fatalf("Compile integer composite: %v", err)
	}

	ptr := float64(-7)
	src := schemaCodecFloatCompositeNumericRec{
		Values: []float64{-1, 0, 42},
		Ptr:    &ptr,
		Labels: map[string]float64{"a": 1, "b": 1 << 40},
	}
	var buf bytes.Buffer
	floatRT.Codec.Encode(unsafe.Pointer(&src), &buf)

	var dst schemaCodecIntegerCompositeNumericRec
	if err = intRT.Codec.Decode(buf.Bytes(), unsafe.Pointer(&dst)); err != nil {
		t.Fatalf("Decode composite float wires as integers: %v", err)
	}
	if !reflect.DeepEqual(dst.Values, []int64{-1, 0, 42}) || dst.Ptr == nil || *dst.Ptr != -7 ||
		!reflect.DeepEqual(dst.Labels, map[string]uint64{"a": 1, "b": 1 << 40}) {
		t.Fatalf("decoded composite integers=%+v", dst)
	}
}

func TestCodecRuntimeRejectsInexactIntegerFloatEvolution(t *testing.T) {
	intRT, err := Compile(reflect.TypeFor[schemaCodecIntegerRec](), Config{})
	if err != nil {
		t.Fatalf("Compile integer: %v", err)
	}
	float64RT, err := Compile(reflect.TypeFor[schemaCodecFloat64Rec](), Config{})
	if err != nil {
		t.Fatalf("Compile float64: %v", err)
	}
	float32RT, err := Compile(reflect.TypeFor[schemaCodecFloat32Rec](), Config{})
	if err != nil {
		t.Fatalf("Compile float32: %v", err)
	}

	src := schemaCodecIntegerRec{Value: 1<<24 + 1}
	var buf bytes.Buffer
	intRT.Codec.Encode(unsafe.Pointer(&src), &buf)
	var f32 schemaCodecFloat32Rec
	if err = float32RT.Codec.Decode(buf.Bytes(), unsafe.Pointer(&f32)); err == nil || !strings.Contains(err.Error(), "cannot be represented exactly") {
		t.Fatalf("Decode inexact int64 as float32 err=%v want exactness error", err)
	}

	src = schemaCodecIntegerRec{U: 1<<53 + 1}
	buf.Reset()
	intRT.Codec.Encode(unsafe.Pointer(&src), &buf)
	var f64 schemaCodecFloat64Rec
	if err = float64RT.Codec.Decode(buf.Bytes(), unsafe.Pointer(&f64)); err == nil || !strings.Contains(err.Error(), "cannot be represented exactly") {
		t.Fatalf("Decode inexact uint64 as float64 err=%v want exactness error", err)
	}
}

func TestCodecRuntimeAllowsExactFloat32Evolution(t *testing.T) {
	intRT, err := Compile(reflect.TypeFor[schemaCodecIntegerRec](), Config{})
	if err != nil {
		t.Fatalf("Compile integer: %v", err)
	}
	float64RT, err := Compile(reflect.TypeFor[schemaCodecFloat64Rec](), Config{})
	if err != nil {
		t.Fatalf("Compile float64: %v", err)
	}
	float32RT, err := Compile(reflect.TypeFor[schemaCodecFloat32Rec](), Config{})
	if err != nil {
		t.Fatalf("Compile float32: %v", err)
	}

	intSrc := schemaCodecIntegerRec{Value: 1 << 24}
	var buf bytes.Buffer
	intRT.Codec.Encode(unsafe.Pointer(&intSrc), &buf)
	var f32 schemaCodecFloat32Rec
	if err = float32RT.Codec.Decode(buf.Bytes(), unsafe.Pointer(&f32)); err != nil {
		t.Fatalf("Decode exact int64 as float32: %v", err)
	}
	if f32.Value != 1<<24 {
		t.Fatalf("decoded exact int64 as float32=%v", f32.Value)
	}

	floatSrc := schemaCodecFloat64Rec{Value: 1.5}
	buf.Reset()
	float64RT.Codec.Encode(unsafe.Pointer(&floatSrc), &buf)
	if err = float32RT.Codec.Decode(buf.Bytes(), unsafe.Pointer(&f32)); err != nil {
		t.Fatalf("Decode exact float64 as float32: %v", err)
	}
	if f32.Value != 1.5 {
		t.Fatalf("decoded exact float64 as float32=%v", f32.Value)
	}
}

func TestCodecRuntimeRejectsLossyFloat64ToFloat32(t *testing.T) {
	float64RT, err := Compile(reflect.TypeFor[schemaCodecFloat64Rec](), Config{})
	if err != nil {
		t.Fatalf("Compile float64: %v", err)
	}
	float32RT, err := Compile(reflect.TypeFor[schemaCodecFloat32Rec](), Config{})
	if err != nil {
		t.Fatalf("Compile float32: %v", err)
	}

	src := schemaCodecFloat64Rec{Value: 0.1}
	var buf bytes.Buffer
	float64RT.Codec.Encode(unsafe.Pointer(&src), &buf)

	var dst schemaCodecFloat32Rec
	if err = float32RT.Codec.Decode(buf.Bytes(), unsafe.Pointer(&dst)); err == nil || !strings.Contains(err.Error(), "cannot be represented exactly") {
		t.Fatalf("Decode lossy float64 as float32 err=%v want exactness error", err)
	}
}

func TestCodecRuntimeCompositeArrayMapNumericSignednessEvolution(t *testing.T) {
	unsignedRT, err := Compile(reflect.TypeFor[schemaCodecUnsignedArrayMapRec](), Config{})
	if err != nil {
		t.Fatalf("Compile unsigned: %v", err)
	}
	signedRT, err := Compile(reflect.TypeFor[schemaCodecSignedArrayMapRec](), Config{})
	if err != nil {
		t.Fatalf("Compile signed: %v", err)
	}

	unsignedSrc := schemaCodecUnsignedArrayMapRec{Fixed: [1]uint64{^uint64(0)}, Labels: map[string]uint64{"ok": 1}}
	var buf bytes.Buffer
	unsignedRT.Codec.Encode(unsafe.Pointer(&unsignedSrc), &buf)
	var signedDst schemaCodecSignedArrayMapRec
	if err = signedRT.Codec.Decode(buf.Bytes(), unsafe.Pointer(&signedDst)); err == nil || !strings.Contains(err.Error(), "overflows int64") {
		t.Fatalf("Decode unsigned array as signed err=%v want overflow", err)
	}

	unsignedSrc = schemaCodecUnsignedArrayMapRec{Fixed: [1]uint64{1}, Labels: map[string]uint64{"bad": ^uint64(0)}}
	buf.Reset()
	unsignedRT.Codec.Encode(unsafe.Pointer(&unsignedSrc), &buf)
	if err = signedRT.Codec.Decode(buf.Bytes(), unsafe.Pointer(&signedDst)); err == nil || !strings.Contains(err.Error(), "overflows int64") {
		t.Fatalf("Decode unsigned map as signed err=%v want overflow", err)
	}

	signedSrc := schemaCodecSignedArrayMapRec{Fixed: [1]int64{-1}, Labels: map[string]int64{"ok": 1}}
	buf.Reset()
	signedRT.Codec.Encode(unsafe.Pointer(&signedSrc), &buf)
	var unsignedDst schemaCodecUnsignedArrayMapRec
	if err = unsignedRT.Codec.Decode(buf.Bytes(), unsafe.Pointer(&unsignedDst)); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("Decode negative signed array as unsigned err=%v want negative overflow", err)
	}

	signedSrc = schemaCodecSignedArrayMapRec{Fixed: [1]int64{1}, Labels: map[string]int64{"bad": -1}}
	buf.Reset()
	signedRT.Codec.Encode(unsafe.Pointer(&signedSrc), &buf)
	if err = unsignedRT.Codec.Decode(buf.Bytes(), unsafe.Pointer(&unsignedDst)); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("Decode negative signed map as unsigned err=%v want negative overflow", err)
	}
}

func TestCodecRuntimePointersArraysSlicesRoundTrip(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaCodecCompositeRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	intValue := int64(42)
	stringValue := "ptr-string"
	timeValue := time.Unix(1_700_000_000, 321).UTC()
	nestedPtrValue := &schemaCodecNestedRec{Code: "ptr-child", Count: 9}
	nestedString := "nested-ptr"
	src := schemaCodecCompositeRec{
		Name:      "composite",
		IntPtr:    &intValue,
		StringPtr: &stringValue,
		TimePtr:   &timeValue,
		StructPtr: nestedPtrValue,
		FixedInts: [3]int32{1, 2, 3},
		FixedItems: [2]schemaCodecNestedRec{
			{Code: "a", Count: 1},
			{Code: "b", Count: 2},
		},
		Tags:      []string{"go", "codec"},
		Scores:    []int64{10, 20, 30},
		Windows:   []schemaCodecNestedRec{{Code: "w1", Count: 11}, {Code: "w2", Count: 12}},
		EmptyTags: make([]string, 0),
		NilTags:   nil,
		Nested: schemaCodecCompositeNested{
			Label:  "nested",
			Ptr:    &nestedString,
			Fixed:  [2]int16{7, 8},
			Values: []schemaCodecNestedRec{{Code: "nv", Count: 5}},
		},
		ComplexPtr: &schemaCodecCompositeNested{
			Label:  "complex-ptr",
			Ptr:    &nestedString,
			Fixed:  [2]int16{9, 10},
			Values: []schemaCodecNestedRec{{Code: "cp", Count: 6}},
		},
		Complexes: []schemaCodecCompositeNested{
			{
				Label:  "complex-slice",
				Ptr:    &nestedString,
				Fixed:  [2]int16{11, 12},
				Values: []schemaCodecNestedRec{{Code: "cs", Count: 7}},
			},
		},
	}

	var buf bytes.Buffer
	rt.Codec.Encode(unsafe.Pointer(&src), &buf)
	var dst schemaCodecCompositeRec
	if err = rt.Codec.Decode(buf.Bytes(), unsafe.Pointer(&dst)); err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if dst.Name != src.Name || dst.IntPtr == nil || *dst.IntPtr != *src.IntPtr ||
		dst.StringPtr == nil || *dst.StringPtr != *src.StringPtr ||
		dst.TimePtr == nil || !dst.TimePtr.Equal(*src.TimePtr) ||
		dst.StructPtr == nil || *dst.StructPtr != *src.StructPtr ||
		dst.NilPtr != nil || dst.FixedInts != src.FixedInts || dst.FixedItems != src.FixedItems ||
		!reflect.DeepEqual(dst.Tags, src.Tags) || !reflect.DeepEqual(dst.Scores, src.Scores) ||
		!reflect.DeepEqual(dst.Windows, src.Windows) || !reflect.DeepEqual(dst.Nested, src.Nested) ||
		!reflect.DeepEqual(dst.ComplexPtr, src.ComplexPtr) || !reflect.DeepEqual(dst.Complexes, src.Complexes) {
		t.Fatalf("decoded composite mismatch:\ngot  %#v\nwant %#v", dst, src)
	}
	if dst.IntPtr == src.IntPtr || dst.StringPtr == src.StringPtr || dst.TimePtr == src.TimePtr || dst.StructPtr == src.StructPtr {
		t.Fatal("decoded pointers alias source")
	}
	if dst.ComplexPtr == src.ComplexPtr || dst.ComplexPtr.Ptr == src.ComplexPtr.Ptr || dst.Complexes[0].Ptr == src.Complexes[0].Ptr {
		t.Fatal("decoded nested composite pointers alias source")
	}
	if dst.EmptyTags == nil || len(dst.EmptyTags) != 0 {
		t.Fatalf("empty slice semantics lost: %#v", dst.EmptyTags)
	}
	if dst.NilTags != nil {
		t.Fatalf("nil slice semantics lost: %#v", dst.NilTags)
	}
}

func TestCodecRuntimeByteSlicesUseCompactPayload(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaCodecBytesRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	src := schemaCodecBytesRec{
		Blob:  []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 255},
		Empty: make([]byte, 0),
		Nil:   nil,
		Alias: schemaCodecNamedBytes{9, 8, 7},
	}
	var buf bytes.Buffer
	rt.Codec.Encode(unsafe.Pointer(&src), &buf)

	wire, payload := schemaCodecEncodedField(t, buf.Bytes(), "blob")
	if wire != codecWireBytes {
		t.Fatalf("blob wire=%d want bytes wire %d", wire, codecWireBytes)
	}
	if len(payload) != 2+len(src.Blob) || payload[0] != 1 {
		t.Fatalf("blob payload is not compact raw bytes: len=%d payload=%v", len(payload), payload)
	}
	n, pos, ok := codecReadUvarintAt(payload, 1)
	if !ok || n != uint64(len(src.Blob)) || !bytes.Equal(payload[pos:], src.Blob) {
		t.Fatalf("blob payload mismatch: n=%d pos=%d payload=%v src=%v", n, pos, payload, src.Blob)
	}

	var dst schemaCodecBytesRec
	if err = rt.Codec.Decode(buf.Bytes(), unsafe.Pointer(&dst)); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(dst.Blob, src.Blob) || dst.Empty == nil || len(dst.Empty) != 0 ||
		dst.Nil != nil || !bytes.Equal(dst.Alias, src.Alias) {
		t.Fatalf("decoded bytes mismatch:\ngot  %#v\nwant %#v", dst, src)
	}
}

func TestCodecRuntimeCompositeFieldLengthUsesCompactVarint(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaCodecUnsignedCompositeRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	src := schemaCodecUnsignedCompositeRec{Values: []uint64{1}}
	var buf bytes.Buffer
	rt.Codec.Encode(unsafe.Pointer(&src), &buf)

	payload := buf.Bytes()
	count, pos, ok := codecReadUvarintAt(payload, 1)
	if !ok || count != 1 {
		t.Fatalf("invalid field count n=%d ok=%v", count, ok)
	}
	nameLen, pos, ok := codecReadUvarintAt(payload, pos)
	if !ok || nameLen != uint64(len("values")) {
		t.Fatalf("invalid field name length n=%d ok=%v", nameLen, ok)
	}
	pos += int(nameLen) + 1
	payloadLen, n := binary.Uvarint(payload[pos:])
	if n != 1 {
		t.Fatalf("payload length varint uses %d bytes, want 1", n)
	}
	pos += n
	if payloadLen != uint64(len(payload)-pos) {
		t.Fatalf("payload length=%d remaining=%d", payloadLen, len(payload)-pos)
	}
}

func TestCodecRuntimeFloat32InfinitiesRoundTrip(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaCodecFloatSpecialRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	src := schemaCodecFloatSpecialRec{
		PosInf: float32(math.Inf(1)),
		NegInf: float32(math.Inf(-1)),
		Values: []float32{float32(math.Inf(1)), float32(math.Inf(-1)), float32(math.NaN())},
	}
	var buf bytes.Buffer
	rt.Codec.Encode(unsafe.Pointer(&src), &buf)

	var dst schemaCodecFloatSpecialRec
	if err = rt.Codec.Decode(buf.Bytes(), unsafe.Pointer(&dst)); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !math.IsInf(float64(dst.PosInf), 1) || !math.IsInf(float64(dst.NegInf), -1) ||
		len(dst.Values) != 3 || !math.IsInf(float64(dst.Values[0]), 1) ||
		!math.IsInf(float64(dst.Values[1]), -1) || !math.IsNaN(float64(dst.Values[2])) {
		t.Fatalf("decoded float specials mismatch: %#v", dst)
	}
}

func TestCodecRuntimeCompositeNumericSignednessEvolution(t *testing.T) {
	unsignedRT, err := Compile(reflect.TypeFor[schemaCodecUnsignedCompositeRec](), Config{})
	if err != nil {
		t.Fatalf("Compile unsigned slice: %v", err)
	}
	signedRT, err := Compile(reflect.TypeFor[schemaCodecSignedCompositeRec](), Config{})
	if err != nil {
		t.Fatalf("Compile signed slice: %v", err)
	}

	unsignedSrc := schemaCodecUnsignedCompositeRec{Values: []uint64{^uint64(0)}}
	var buf bytes.Buffer
	unsignedRT.Codec.Encode(unsafe.Pointer(&unsignedSrc), &buf)
	var signedDst schemaCodecSignedCompositeRec
	if err = signedRT.Codec.Decode(buf.Bytes(), unsafe.Pointer(&signedDst)); err == nil || !strings.Contains(err.Error(), "overflows int64") {
		t.Fatalf("Decode unsigned composite as signed err=%v want overflow", err)
	}

	signedPtrRT, err := Compile(reflect.TypeFor[schemaCodecSignedPtrRec](), Config{})
	if err != nil {
		t.Fatalf("Compile signed pointer: %v", err)
	}
	unsignedPtrRT, err := Compile(reflect.TypeFor[schemaCodecUnsignedPtrRec](), Config{})
	if err != nil {
		t.Fatalf("Compile unsigned pointer: %v", err)
	}
	negative := int64(-1)
	signedPtrSrc := schemaCodecSignedPtrRec{Value: &negative}
	buf.Reset()
	signedPtrRT.Codec.Encode(unsafe.Pointer(&signedPtrSrc), &buf)
	var unsignedPtrDst schemaCodecUnsignedPtrRec
	if err = unsignedPtrRT.Codec.Decode(buf.Bytes(), unsafe.Pointer(&unsignedPtrDst)); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("Decode negative signed pointer as unsigned err=%v want negative overflow", err)
	}
}

func TestCodecRuntimeRejectsMalformedCompositeLengthsBeforeAllocation(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaCodecMalformedCompositeLengthRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	var slicePayload bytes.Buffer
	slicePayload.WriteByte(1)
	codecWriteUvarint(&slicePayload, 1<<30)
	payload := schemaCodecTestPayload(schemaCodecTestField{name: "tags", wire: codecWireSlice, payload: slicePayload.Bytes()})
	var dst schemaCodecMalformedCompositeLengthRec
	if err = rt.Codec.Decode(payload, unsafe.Pointer(&dst)); err == nil || !strings.Contains(err.Error(), "malformed slice length") {
		t.Fatalf("Decode malformed slice length err=%v want length error", err)
	}

	largeRT, err := Compile(reflect.TypeFor[schemaCodecLargeArraySliceRec](), Config{})
	if err != nil {
		t.Fatalf("Compile large slice: %v", err)
	}
	slicePayload.Reset()
	slicePayload.WriteByte(1)
	codecWriteUvarint(&slicePayload, 2)
	slicePayload.WriteByte(0)
	slicePayload.WriteByte(0)
	payload = schemaCodecTestPayload(schemaCodecTestField{name: "values", wire: codecWireSlice, payload: slicePayload.Bytes()})
	var largeDst schemaCodecLargeArraySliceRec
	if err = largeRT.Codec.Decode(payload, unsafe.Pointer(&largeDst)); err == nil || !strings.Contains(err.Error(), "malformed slice length") {
		t.Fatalf("Decode short large slice payload err=%v want length error", err)
	}

	var mapPayload bytes.Buffer
	mapPayload.WriteByte(1)
	codecWriteUvarint(&mapPayload, 1<<30)
	payload = schemaCodecTestPayload(schemaCodecTestField{name: "labels", wire: codecWireMap, payload: mapPayload.Bytes()})
	if err = rt.Codec.Decode(payload, unsafe.Pointer(&dst)); err == nil || !strings.Contains(err.Error(), "malformed map length") {
		t.Fatalf("Decode malformed map length err=%v want length error", err)
	}
}

func TestCodecRuntimeIgnoresTaggedFields(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaCodecIgnoredRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	src := schemaCodecIgnoredRec{
		Name:     "root",
		DBSkip:   "db-skip",
		RBISkip:  "rbi-skip",
		Disabled: []byte("disabled"),
		Nested: schemaCodecIgnoredNested{
			Keep:     "nested",
			DBSkip:   "nested-db-skip",
			RBISkip:  "nested-rbi-skip",
			Disabled: []byte("nested-disabled"),
		},
	}
	var buf bytes.Buffer
	rt.Codec.Encode(unsafe.Pointer(&src), &buf)
	data := buf.Bytes()
	for _, name := range []string{"DBSkip", "skip", "Disabled"} {
		if bytes.Contains(data, []byte(name)) {
			t.Fatalf("encoded ignored field name %q in %x", name, data)
		}
	}

	var dst schemaCodecIgnoredRec
	if err = rt.Codec.Decode(data, unsafe.Pointer(&dst)); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if dst.Name != "root" || dst.Nested.Keep != "nested" {
		t.Fatalf("decoded stored fields wrong: %#v", dst)
	}
	if dst.DBSkip != "" || dst.RBISkip != "" || dst.Disabled != nil ||
		dst.Nested.DBSkip != "" || dst.Nested.RBISkip != "" || dst.Nested.Disabled != nil {
		t.Fatalf("decoded ignored fields: %#v", dst)
	}
}

func TestCodecRuntimeZerosMissingCompositeFields(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaCodecCompositeRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	payload := schemaCodecTestPayload(schemaCodecTestField{name: "Name", wire: codecWireString, payload: []byte("fresh")})
	staleInt := int64(10)
	staleString := "stale"
	dst := schemaCodecCompositeRec{
		Name:       "stale",
		IntPtr:     &staleInt,
		StringPtr:  &staleString,
		StructPtr:  &schemaCodecNestedRec{Code: "stale", Count: 1},
		FixedInts:  [3]int32{1, 2, 3},
		Tags:       []string{"stale"},
		EmptyTags:  []string{"stale"},
		Nested:     schemaCodecCompositeNested{Label: "stale", Ptr: &staleString, Fixed: [2]int16{1, 2}, Values: []schemaCodecNestedRec{{Code: "stale"}}},
		ComplexPtr: &schemaCodecCompositeNested{Label: "stale", Ptr: &staleString, Fixed: [2]int16{1, 2}, Values: []schemaCodecNestedRec{{Code: "stale"}}},
		Complexes:  []schemaCodecCompositeNested{{Label: "stale", Ptr: &staleString, Fixed: [2]int16{1, 2}, Values: []schemaCodecNestedRec{{Code: "stale"}}}},
		FixedItems: [2]schemaCodecNestedRec{{Code: "stale", Count: 1}},
	}
	if err = rt.Codec.Decode(payload, unsafe.Pointer(&dst)); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if dst.Name != "fresh" || dst.IntPtr != nil || dst.StringPtr != nil || dst.StructPtr != nil ||
		dst.FixedInts != ([3]int32{}) || dst.FixedItems != ([2]schemaCodecNestedRec{}) ||
		dst.Tags != nil || dst.EmptyTags != nil || !reflect.DeepEqual(dst.Nested, schemaCodecCompositeNested{}) ||
		dst.ComplexPtr != nil || dst.Complexes != nil {
		t.Fatalf("missing composite fields were not zeroed: %#v", dst)
	}
}

func TestCodecRuntimeMapsRoundTrip(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaCodecMapRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	pointerValue := &schemaCodecNestedRec{Code: "ptr", Count: 5}
	src := schemaCodecMapRec{
		Name: "maps",
		Strings: map[string]string{
			"a": "alpha",
			"b": "beta",
		},
		Ints: map[string]int64{
			"one": 1,
			"neg": -2,
		},
		Times: map[string]time.Time{
			"at": time.Unix(1_700_000_000, 123).UTC(),
		},
		Structs: map[string]schemaCodecNestedRec{
			"x": {Code: "x", Count: 10},
			"y": {Code: "y", Count: 20},
		},
		Pointers: map[string]*schemaCodecNestedRec{
			"p": pointerValue,
		},
		Arrays: map[string][2]int16{
			"pair": {7, 8},
		},
		Slices: map[string][]string{
			"tags": {"go", "codec"},
		},
		SignedKeys: map[int16]string{
			-1: "minus",
			2:  "plus",
		},
		UnsignedKeys: map[uint64]float64{
			10: 1.25,
			20: 2.5,
		},
		StructKeys: map[schemaCodecMapKey]string{
			{
				Tenant: "acme",
				Shard:  7,
				Parts:  [2]int8{-1, 2},
				Sub:    schemaCodecMapKeySub{Flag: true, Code: "blue"},
			}: "struct-key",
		},
		ArrayKeys: map[[2]schemaCodecMapKeySub]int64{
			{
				{Flag: true, Code: "a"},
				{Flag: false, Code: "b"},
			}: 99,
		},
		Empty: make(map[string]int64),
		Nil:   nil,
		Nested: schemaCodecMapNested{
			Labels: map[string]string{"nested": "yes"},
			Counts: map[uint16]int32{
				1: 100,
				2: 200,
			},
		},
	}

	var buf bytes.Buffer
	rt.Codec.Encode(unsafe.Pointer(&src), &buf)
	var dst schemaCodecMapRec
	if err = rt.Codec.Decode(buf.Bytes(), unsafe.Pointer(&dst)); err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if !reflect.DeepEqual(dst, src) {
		t.Fatalf("decoded map record mismatch:\ngot  %#v\nwant %#v", dst, src)
	}
	if dst.Pointers["p"] == src.Pointers["p"] {
		t.Fatal("decoded map pointer value aliases source")
	}
	if dst.Empty == nil || len(dst.Empty) != 0 {
		t.Fatalf("empty map semantics lost: %#v", dst.Empty)
	}
	if dst.Nil != nil {
		t.Fatalf("nil map semantics lost: %#v", dst.Nil)
	}
}

func TestCodecRuntimeZerosMissingMapFields(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaCodecMapRec](), Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	payload := schemaCodecTestPayload(schemaCodecTestField{name: "Name", wire: codecWireString, payload: []byte("fresh")})
	dst := schemaCodecMapRec{
		Name:         "stale",
		Strings:      map[string]string{"stale": "stale"},
		Ints:         map[string]int64{"stale": 1},
		Structs:      map[string]schemaCodecNestedRec{"stale": {Code: "stale", Count: 1}},
		Pointers:     map[string]*schemaCodecNestedRec{"stale": {Code: "stale", Count: 1}},
		Arrays:       map[string][2]int16{"stale": {1, 2}},
		Slices:       map[string][]string{"stale": {"stale"}},
		SignedKeys:   map[int16]string{1: "stale"},
		UnsignedKeys: map[uint64]float64{1: 1},
		StructKeys:   map[schemaCodecMapKey]string{{Tenant: "stale"}: "stale"},
		ArrayKeys:    map[[2]schemaCodecMapKeySub]int64{{}: 1},
		Empty:        map[string]int64{"stale": 1},
		Nil:          map[string]int64{"stale": 1},
		Nested: schemaCodecMapNested{
			Labels: map[string]string{"stale": "stale"},
			Counts: map[uint16]int32{1: 1},
		},
	}
	if err = rt.Codec.Decode(payload, unsafe.Pointer(&dst)); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if dst.Name != "fresh" || dst.Strings != nil || dst.Ints != nil || dst.Times != nil ||
		dst.Structs != nil || dst.Pointers != nil || dst.Arrays != nil || dst.Slices != nil ||
		dst.SignedKeys != nil || dst.UnsignedKeys != nil || dst.StructKeys != nil || dst.ArrayKeys != nil ||
		dst.Empty != nil || dst.Nil != nil ||
		dst.Nested.Labels != nil || dst.Nested.Counts != nil {
		t.Fatalf("missing map fields were not zeroed: %#v", dst)
	}
}

func TestCodecRuntimeRejectsUnsupportedShapesAtCompile(t *testing.T) {
	type zeroArray [0]int
	type zeroArrayField struct {
		Value [0]int
	}
	type namedZeroArrayField struct {
		Value zeroArray
	}
	type zeroArraySlice struct {
		Values [][0]int
	}
	type nestedZeroArray struct {
		Values [1][0]int
	}
	type zeroArrayMapKey struct {
		Values map[[0]int]string
	}
	type zeroArrayMapValue struct {
		Values map[string][0]int
	}

	for _, tc := range []struct {
		name string
		typ  reflect.Type
		want string
	}{
		{name: "float map key", typ: reflect.TypeFor[schemaCodecUnsupportedMapKeyRec](), want: "type float64"},
		{name: "complex map value", typ: reflect.TypeFor[schemaCodecUnsupportedMapValueRec](), want: "type complex64"},
		{name: "time map key", typ: reflect.TypeFor[schemaCodecUnsupportedMapTimeKeyRec](), want: "type time.Time"},
		{name: "zero-length array field", typ: reflect.TypeFor[zeroArrayField](), want: "zero-length array"},
		{name: "named zero-length array field", typ: reflect.TypeFor[namedZeroArrayField](), want: "zero-length array"},
		{name: "zero-length array slice element", typ: reflect.TypeFor[zeroArraySlice](), want: "zero-length array"},
		{name: "nested zero-length array", typ: reflect.TypeFor[nestedZeroArray](), want: "zero-length array"},
		{name: "zero-length array map key", typ: reflect.TypeFor[zeroArrayMapKey](), want: "zero-length array"},
		{name: "zero-length array map value", typ: reflect.TypeFor[zeroArrayMapValue](), want: "zero-length array"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Compile(tc.typ, Config{})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Compile err=%v want %q", err, tc.want)
			}
		})
	}
}

func TestCodecRuntimeRejectsInvalidMapKeysAtCompile(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  reflect.Type
		want string
	}{
		{name: "pointer", typ: reflect.TypeFor[schemaCodecUnsupportedMapPointerKeyRec](), want: "unsupported map key"},
		{name: "hidden", typ: reflect.TypeFor[schemaCodecUnsupportedMapHiddenKeyRec](), want: "unsupported map key"},
		{name: "db ignored", typ: reflect.TypeFor[schemaCodecUnsupportedMapDBIgnoredKeyRec](), want: "ignored map key"},
		{name: "rbi ignored", typ: reflect.TypeFor[schemaCodecUnsupportedMapRBIIgnoredKeyRec](), want: "ignored map key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Compile(tc.typ, Config{})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Compile err=%v want %q", err, tc.want)
			}
		})
	}
}

type schemaCodecTestField struct {
	name    string
	wire    byte
	payload []byte
}

func schemaCodecTestPayload(fields ...schemaCodecTestField) []byte {
	var buf bytes.Buffer
	buf.WriteByte(codecVersion)
	codecWriteUvarint(&buf, uint64(len(fields)))
	for i := range fields {
		codecWriteFieldHeader(&buf, fields[i].name, fields[i].wire, uint64(len(fields[i].payload)))
		buf.Write(fields[i].payload)
	}
	return buf.Bytes()
}

func schemaCodecTestTimePayload(sec int64, nsec int64) []byte {
	var payload [12]byte
	binary.LittleEndian.PutUint64(payload[:8], uint64(sec))
	binary.LittleEndian.PutUint32(payload[8:], uint32(nsec))
	return payload[:]
}

func schemaCodecEncodedField(t testing.TB, payload []byte, field string) (byte, []byte) {
	t.Helper()
	if len(payload) == 0 || payload[0] != codecVersion {
		t.Fatalf("invalid codec payload header: %v", payload)
	}
	count, pos, ok := codecReadUvarintAt(payload, 1)
	if !ok {
		t.Fatalf("invalid codec field count")
	}
	for i := uint64(0); i < count; i++ {
		var nameLen uint64
		nameLen, pos, ok = codecReadUvarintAt(payload, pos)
		if !ok || nameLen > uint64(len(payload)-pos) {
			t.Fatalf("invalid codec field name at %d", pos)
		}
		name := string(payload[pos : pos+int(nameLen)])
		pos += int(nameLen)
		if pos >= len(payload) {
			t.Fatalf("missing wire for field %q", name)
		}
		wire := payload[pos]
		pos++
		var payloadLen uint64
		payloadLen, pos, ok = codecReadUvarintAt(payload, pos)
		if !ok || payloadLen > uint64(len(payload)-pos) {
			t.Fatalf("invalid codec field payload for %q", name)
		}
		value := payload[pos : pos+int(payloadLen)]
		pos += int(payloadLen)
		if name == field {
			return wire, value
		}
	}
	t.Fatalf("field %q not found", field)
	return 0, nil
}
