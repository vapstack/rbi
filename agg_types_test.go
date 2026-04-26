package rbi

import (
	"math"
	"testing"
)

type testValueStringer struct{}

func (testValueStringer) String() string {
	return "stringer-value"
}

func TestValueIntRejectsUintOverflow(t *testing.T) {
	v := Value{num: math.MaxInt64 + 1, any: ValueKindUint}

	if got, ok := v.ToInt(); ok {
		t.Fatalf("Int() succeeded for unrepresentable uint64: got=%d", got)
	}
}

func TestValueUintRejectsNegativeInt(t *testing.T) {
	v := Value{num: math.MaxUint64, any: ValueKindInt}

	if got, ok := v.ToUint(); ok {
		t.Fatalf("Uint() succeeded for negative int64: got=%d", got)
	}
}

func TestValueIntRejectsNaNAndInf(t *testing.T) {
	for _, f := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), maxInt64Float} {
		v := Value{num: math.Float64bits(f), any: ValueKindFloat}
		if got, ok := v.ToInt(); ok {
			t.Fatalf("Int() succeeded for unrepresentable float64 %v: got=%d", f, got)
		}
	}
}

func TestValueUintRejectsNegativeNaNAndInf(t *testing.T) {
	for _, f := range []float64{-0.5, math.NaN(), math.Inf(1), math.Inf(-1), maxUint64Float} {
		v := Value{num: math.Float64bits(f), any: ValueKindFloat}
		if got, ok := v.ToUint(); ok {
			t.Fatalf("Uint() succeeded for unrepresentable float64 %v: got=%d", f, got)
		}
	}
}

func TestValueFloatToIntAndUintAllowRounding(t *testing.T) {
	intVal := Value{num: math.Float64bits(1.9), any: ValueKindFloat}
	gotInt, ok := intVal.ToInt()
	if !ok || gotInt != 1 {
		t.Fatalf("Int() mismatch: got=%d ok=%v want=1 true", gotInt, ok)
	}

	uintVal := Value{num: math.Float64bits(2.9), any: ValueKindFloat}
	gotUint, ok := uintVal.ToUint()
	if !ok || gotUint != 2 {
		t.Fatalf("Uint() mismatch: got=%d ok=%v want=2 true", gotUint, ok)
	}
}

func TestValueStringConvertsPrimitiveKinds(t *testing.T) {
	tests := []struct {
		name string
		v    Value
		want string
	}{
		{
			name: "bool",
			v:    Value{num: 1, any: ValueKindBool},
			want: "true",
		},
		{
			name: "int",
			v:    Value{num: math.MaxUint64 - 41, any: ValueKindInt},
			want: "-42",
		},
		{
			name: "uint",
			v:    Value{num: 42, any: ValueKindUint},
			want: "42",
		},
		{
			name: "float",
			v:    Value{num: math.Float64bits(1.25), any: ValueKindFloat},
			want: "1.25",
		},
	}

	for _, tt := range tests {
		got, ok := tt.v.ToString()
		if !ok || got != tt.want {
			t.Fatalf("%s String() mismatch: got=%q ok=%v want=%q true", tt.name, got, ok, tt.want)
		}
	}
}

func TestValueStringUsesSprintForAny(t *testing.T) {
	v := Value{any: testValueStringer{}}

	got, ok := v.ToString()
	if !ok || got != "stringer-value" {
		t.Fatalf("String() mismatch: got=%q ok=%v want=%q true", got, ok, "stringer-value")
	}
}
