package schema

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
)

type schemaTestMeasureRec struct {
	Signed   int64    `db:"signed" rbi:"measure"`
	Unsigned uint64   `db:"unsigned" rbi:"measure"`
	Float    float64  `db:"float" rbi:"measure"`
	Float32  float32  `db:"float32" rbi:"measure"`
	Ptr      *uint64  `db:"ptr" rbi:"measure"`
	FloatPtr *float64 `db:"float_ptr" rbi:"measure"`
	Invalid  string   `db:"invalid"`
	Indexed  []string `db:"indexed" rbi:"index"`
}

func TestMeasureAccessorsReadAndDetectChanges(t *testing.T) {
	rt, err := Compile(reflect.TypeFor[schemaTestMeasureRec](), Config{Index: map[string]IndexKind{
		"Signed":   IndexMeasure,
		"Unsigned": IndexMeasure,
		"Float":    IndexMeasure,
		"Float32":  IndexMeasure,
		"Ptr":      IndexMeasure,
		"FloatPtr": IndexMeasure,
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
	if got, ok := rt.MeasuresByName["float32"].Read(oldPtr); !ok || got != math.Float64bits(0) {
		t.Fatalf("float32 read=(%d,%v)", got, ok)
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
	negZero := math.Copysign(0, -1)
	posZero := 0.0
	oldZero := schemaTestMeasureRec{Float: negZero, Float32: float32(negZero), FloatPtr: &negZero}
	newZero := schemaTestMeasureRec{Float: posZero, Float32: float32(posZero), FloatPtr: &posZero}
	oldZeroPtr := unsafe.Pointer(&oldZero)
	newZeroPtr := unsafe.Pointer(&newZero)
	if rt.MeasuresByName["float"].Modified(oldZeroPtr, newZeroPtr) {
		t.Fatal("float -0/+0 canonical match was reported as modified")
	}
	if rt.MeasuresByName["float32"].Modified(oldZeroPtr, newZeroPtr) {
		t.Fatal("float32 -0/+0 canonical match was reported as modified")
	}
	if rt.MeasuresByName["float_ptr"].Modified(oldZeroPtr, newZeroPtr) {
		t.Fatal("float pointer -0/+0 canonical match was reported as modified")
	}
	if got, ok := rt.MeasuresByName["float"].Read(oldZeroPtr); !ok || got != math.Float64bits(0) {
		t.Fatalf("float -0 read=(%x,%v), want +0 bits", got, ok)
	}

	oldNaN := math.Float64frombits(0x7ff0000000000001)
	newNaN := math.Float64frombits(0x7ff8000000000001)
	oldNaNRec := schemaTestMeasureRec{Float: oldNaN, Float32: math.Float32frombits(0x7f800001), FloatPtr: &oldNaN}
	newNaNRec := schemaTestMeasureRec{Float: newNaN, Float32: math.Float32frombits(0x7fc00001), FloatPtr: &newNaN}
	oldNaNPtr := unsafe.Pointer(&oldNaNRec)
	newNaNPtr := unsafe.Pointer(&newNaNRec)
	if rt.MeasuresByName["float"].Modified(oldNaNPtr, newNaNPtr) {
		t.Fatal("float NaN canonical match was reported as modified")
	}
	if rt.MeasuresByName["float32"].Modified(oldNaNPtr, newNaNPtr) {
		t.Fatal("float32 NaN canonical match was reported as modified")
	}
	if rt.MeasuresByName["float_ptr"].Modified(oldNaNPtr, newNaNPtr) {
		t.Fatal("float pointer NaN canonical match was reported as modified")
	}
	if got, ok := rt.MeasuresByName["float"].Read(oldNaNPtr); !ok || got != keycodec.CanonicalFloat64NaNBits {
		t.Fatalf("float NaN read=(%x,%v), want canonical NaN bits", got, ok)
	}

	if _, err = Compile(reflect.TypeFor[schemaTestMeasureRec](), Config{Index: map[string]IndexKind{"Invalid": IndexMeasure}}); err == nil || !strings.Contains(err.Error(), "measure field Invalid has unsupported type") {
		t.Fatalf("invalid measure err=%v", err)
	}
}
