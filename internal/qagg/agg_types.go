package qagg

import (
	"fmt"
	"math"
	"strconv"
	"unsafe"

	"github.com/vapstack/rbi/internal/qexec"
)

const (
	PlanCountMaterialized  qexec.PlanName = "plan_count_materialized"
	PlanCountUniqueEq      qexec.PlanName = "plan_count_unique_eq"
	PlanCountScalarLookup  qexec.PlanName = "plan_count_scalar_lookup"
	PlanCountScalarInSplit qexec.PlanName = "plan_count_scalar_in_split"
	PlanCountPredicates    qexec.PlanName = "plan_count_predicates"
	PlanCountORPredicates  qexec.PlanName = "plan_count_or_predicates"
	PlanCountORHybrid      qexec.PlanName = "plan_count_or_hybrid"
)

type (
	Result struct {
		// Layout holds result aliases and their positions in result rows.
		Layout []string
		Rows   []Row
	}
	Row []Value
)

const (
	maxInt64Float  = float64(1 << 63)
	minInt64Float  = -maxInt64Float
	maxUint64Float = float64(1 << 64)
)

type (
	// ValueKind defines value underlying kind (int/uint/float/bool/any)
	ValueKind int

	// Value can hold strings, ints, booleans and floats without allocation.
	Value struct {
		_   [0]func() // idea is taken from slog to disallow equality checks (==)
		num uint64    // bool, int*, uint*, float*, string len
		any any       // ValueKind, stringptr, any
	}
	stringptr *byte
)

func valueFromUnsafeString(s string) Value {
	b := []byte(s) // copy
	return Value{num: uint64(len(b)), any: stringptr(unsafe.SliceData(b))}
}

func valueFromSafeString(s string) Value {
	return Value{num: uint64(len(s)), any: stringptr(unsafe.StringData(s))}
}

const (
	ValueKindNone ValueKind = iota

	ValueKindAny // currently not used

	ValueKindBool
	ValueKindInt
	ValueKindUint
	ValueKindFloat
	ValueKindString
)

func (v Value) Kind() ValueKind {
	if v.any == nil {
		return ValueKindNone
	}
	switch k := v.any.(type) {
	case ValueKind:
		return k
	case stringptr:
		return ValueKindString
	default:
		return ValueKind(v.num)
	}
}

func (v Value) String() (string, bool) {
	if sp, ok := v.any.(stringptr); ok {
		return unsafe.String(sp, v.num), true
	}
	return "", false
}

func (v Value) ToString() (string, bool) {
	switch k := v.any.(type) {
	case nil:
		return "", false
	case stringptr:
		return unsafe.String(k, v.num), true
	case ValueKind:
		switch k {
		case ValueKindBool:
			return strconv.FormatBool(v.num == 1), true
		case ValueKindInt:
			return strconv.FormatInt(int64(v.num), 10), true
		case ValueKindUint:
			return strconv.FormatUint(v.num, 10), true
		case ValueKindFloat:
			return strconv.FormatFloat(math.Float64frombits(v.num), 'f', -1, 64), true
		}
		return "", false
	}
	return fmt.Sprint(v.any), true
}

func (v Value) Int() (int64, bool) {
	if k, ok := v.any.(ValueKind); ok && k == ValueKindInt {
		return int64(v.num), true
	}
	return 0, false
}

func (v Value) ToInt() (int64, bool) {
	k, ok := v.any.(ValueKind)
	if !ok {
		return 0, false
	}
	switch k {
	case ValueKindInt:
		return int64(v.num), true
	case ValueKindUint:
		if v.num > math.MaxInt64 {
			return 0, false
		}
		return int64(v.num), true
	case ValueKindFloat:
		f := math.Float64frombits(v.num)
		if math.IsNaN(f) || math.IsInf(f, 0) || f < minInt64Float || f >= maxInt64Float {
			return 0, false
		}
		return int64(f), true
	}
	return 0, false
}

func (v Value) Uint() (uint64, bool) {
	if k, ok := v.any.(ValueKind); ok && k == ValueKindUint {
		return v.num, true
	}
	return 0, false
}

func (v Value) ToUint() (uint64, bool) {
	k, ok := v.any.(ValueKind)
	if !ok {
		return 0, false
	}
	switch k {
	case ValueKindUint:
		return v.num, true
	case ValueKindInt:
		i := int64(v.num)
		if i < 0 {
			return 0, false
		}
		return uint64(i), true
	case ValueKindFloat:
		f := math.Float64frombits(v.num)
		if math.IsNaN(f) || math.IsInf(f, 0) || f < 0 || f >= maxUint64Float {
			return 0, false
		}
		return uint64(f), true
	}
	return 0, false
}

func (v Value) Float() (float64, bool) {
	if k, ok := v.any.(ValueKind); ok && k == ValueKindFloat {
		return math.Float64frombits(v.num), true
	}
	return 0, false
}

func (v Value) ToFloat() (float64, bool) {
	k, ok := v.any.(ValueKind)
	if !ok {
		return 0, false
	}
	switch k {
	case ValueKindFloat:
		return math.Float64frombits(v.num), true
	case ValueKindInt:
		return float64(int64(v.num)), true
	case ValueKindUint:
		return float64(v.num), true
	}
	return 0, false
}

func (v Value) Bool() (bool, bool) {
	if k, ok := v.any.(ValueKind); ok && k == ValueKindBool {
		return v.num == 1, true
	}
	return false, false
}

func (v Value) ToBool() (bool, bool) {
	k, ok := v.any.(ValueKind)
	if !ok {
		return false, false
	}
	switch k {
	case ValueKindBool:
		return v.num == 1, true
	case ValueKindFloat:
		return math.Float64frombits(v.num) != 0, true
	case ValueKindInt:
		return int64(v.num) != 0, true
	case ValueKindUint:
		return float64(v.num) != 0, true
	}
	return false, false
}

/*
func (v Value) Any() any {
	switch k := v.any.(type) {
	case stringptr:
		return unsafe.String(k, v.num)
	case ValueKind:
		switch k {
		case ValueKindInt:
			return int64(v.num)
		case ValueKindUint:
			return v.num
		case ValueKindFloat:
			return math.Float64frombits(v.num)
		case ValueKindBool:
			return v.num == 1
		}
	}
	return v.any
}
*/
