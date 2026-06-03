package qir

type LeafMode uint8

const (
	LeafModeCollect LeafMode = iota
	LeafModeExtract
)

type leafStatus uint8

const (
	leafStatusOK leafStatus = iota
	leafStatusOverflow
	leafStatusInvalid
)

func CollectAndLeavesInto(e Expr, dst []Expr, mode LeafMode) ([]Expr, bool) {
	dst, status := appendAndLeaves(dst[:0], e, mode)
	if status != leafStatusOK {
		return nil, false
	}
	if len(dst) == 0 {
		return nil, true
	}
	return dst, true
}

func CollectAndLeavesPooledFallback(e Expr, dst []Expr, mode LeafMode) ([]Expr, []Expr, bool) {
	dst, status := appendAndLeaves(dst[:0], e, mode)
	switch status {

	case leafStatusOK:
		if len(dst) == 0 {
			return nil, nil, true
		}
		return dst, nil, true

	case leafStatusOverflow:
		n, ok := AndLeafLen(e, mode)
		if !ok {
			return nil, nil, false
		}
		heap := GetExprSlice(n)
		out, status := appendAndLeaves(heap[:0], e, mode)
		if status != leafStatusOK {
			ReleaseExprSlice(heap)
			return nil, nil, false
		}
		if len(out) == 0 {
			ReleaseExprSlice(heap)
			return nil, nil, true
		}
		return out, heap, true

	default:
		return nil, nil, false
	}
}

func AndLeafLen(e Expr, mode LeafMode) (int, bool) {
	switch e.Op {

	case OpConst:
		if mode == LeafModeCollect && e.Not {
			return 1, true
		}
		if mode == LeafModeCollect && !e.Not {
			return 0, true
		}
		return 0, false

	case OpAND:
		if e.Not || len(e.Operands) == 0 {
			return 0, false
		}
		total := 0
		for _, ch := range e.Operands {
			n, ok := AndLeafLen(ch, mode)
			if !ok {
				return 0, false
			}
			total += n
		}
		return total, true

	default:
		if e.Op == OpOR || len(e.Operands) != 0 {
			return 0, false
		}
		if mode == LeafModeExtract && e.Not {
			return 0, false
		}
		return 1, true
	}
}

func appendAndLeaves(dst []Expr, e Expr, mode LeafMode) ([]Expr, leafStatus) {
	switch e.Op {

	case OpConst:
		if mode != LeafModeCollect {
			return nil, leafStatusInvalid
		}
		if !e.Not {
			return dst, leafStatusOK
		}
		return appendAndLeaf(dst, Expr{Op: OpConst, Not: true, FieldOrdinal: NoFieldOrdinal})

	case OpAND:
		if e.Not || len(e.Operands) == 0 {
			return nil, leafStatusInvalid
		}
		for _, ch := range e.Operands {
			var status leafStatus
			dst, status = appendAndLeaves(dst, ch, mode)
			if status != leafStatusOK {
				return nil, status
			}
		}
		return dst, leafStatusOK

	default:
		if e.Op == OpOR || len(e.Operands) != 0 {
			return nil, leafStatusInvalid
		}
		if mode == LeafModeExtract && e.Not {
			return nil, leafStatusInvalid
		}
		return appendAndLeaf(dst, e)
	}
}

func appendAndLeaf(dst []Expr, e Expr) ([]Expr, leafStatus) {
	if len(dst) >= cap(dst) {
		return nil, leafStatusOverflow
	}
	return append(dst, e), leafStatusOK
}
