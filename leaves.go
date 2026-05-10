package rbi

import "github.com/vapstack/rbi/internal/qir"

type andLeafMode uint8

const (
	andLeafModeCollect andLeafMode = iota
	andLeafModeExtract
)

type andLeafStatus uint8

const (
	andLeafStatusOK andLeafStatus = iota
	andLeafStatusOverflow
	andLeafStatusInvalid
)

func collectAndLeavesFixed(e qir.Expr, dst []qir.Expr) ([]qir.Expr, bool) {
	dst, status := appendAndLeavesMode(dst[:0], e, andLeafModeCollect)
	if status != andLeafStatusOK || len(dst) == 0 {
		return nil, false
	}
	return dst, true
}

func collectAndLeavesMode(e qir.Expr, mode andLeafMode) ([]qir.Expr, bool) {
	n, ok := countAndLeavesMode(e, mode)
	if !ok {
		return nil, false
	}
	if n == 0 {
		return nil, true
	}
	out := make([]qir.Expr, 0, n)
	out, status := appendAndLeavesMode(out, e, mode)
	if status != andLeafStatusOK {
		return nil, false
	}
	return out, true
}

func collectAndLeavesModeScratch(e qir.Expr, dst []qir.Expr, mode andLeafMode) ([]qir.Expr, bool) {
	dst, status := appendAndLeavesMode(dst[:0], e, mode)
	switch status {
	case andLeafStatusOK:
		if len(dst) == 0 {
			return nil, true
		}
		return dst, true
	case andLeafStatusOverflow:
		return collectAndLeavesMode(e, mode)
	default:
		return nil, false
	}
}

func countAndLeavesMode(e qir.Expr, mode andLeafMode) (int, bool) {
	switch e.Op {
	case qir.OpNOOP:
		if mode == andLeafModeCollect && e.Not {
			return 1, true
		}
		if mode == andLeafModeCollect && !e.Not {
			return 0, true
		}
		return 0, false
	case qir.OpAND:
		if e.Not || len(e.Operands) == 0 {
			return 0, false
		}
		total := 0
		for _, ch := range e.Operands {
			n, ok := countAndLeavesMode(ch, mode)
			if !ok {
				return 0, false
			}
			total += n
		}
		return total, true
	default:
		if e.Op == qir.OpOR || len(e.Operands) != 0 {
			return 0, false
		}
		if mode == andLeafModeExtract && e.Not {
			return 0, false
		}
		return 1, true
	}
}

func appendAndLeavesMode(dst []qir.Expr, e qir.Expr, mode andLeafMode) ([]qir.Expr, andLeafStatus) {
	switch e.Op {
	case qir.OpNOOP:
		if mode != andLeafModeCollect {
			return nil, andLeafStatusInvalid
		}
		if !e.Not {
			return dst, andLeafStatusOK
		}
		return appendAndLeaf(dst, qir.Expr{Op: qir.OpNOOP, Not: true, FieldOrdinal: qir.NoFieldOrdinal})
	case qir.OpAND:
		if e.Not || len(e.Operands) == 0 {
			return nil, andLeafStatusInvalid
		}
		for _, ch := range e.Operands {
			var status andLeafStatus
			dst, status = appendAndLeavesMode(dst, ch, mode)
			if status != andLeafStatusOK {
				return nil, status
			}
		}
		return dst, andLeafStatusOK
	default:
		if e.Op == qir.OpOR || len(e.Operands) != 0 {
			return nil, andLeafStatusInvalid
		}
		if mode == andLeafModeExtract && e.Not {
			return nil, andLeafStatusInvalid
		}
		return appendAndLeaf(dst, e)
	}
}

func appendAndLeaf(dst []qir.Expr, e qir.Expr) ([]qir.Expr, andLeafStatus) {
	if len(dst) >= cap(dst) {
		return nil, andLeafStatusOverflow
	}
	return append(dst, e), andLeafStatusOK
}
