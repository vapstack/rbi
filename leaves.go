package rbi

import (
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/qir"
)

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

func collectAndLeaves(e qir.Expr) ([]qir.Expr, bool) {
	return collectAndLeavesMode(e, andLeafModeCollect)
}

func collectAndLeavesScratch(e qir.Expr, dst []qir.Expr) ([]qir.Expr, bool) {
	return collectAndLeavesModeScratch(e, dst, andLeafModeCollect)
}

func collectAndLeavesBuf(e qir.Expr, dst *pooled.SliceBuf[qir.Expr]) bool {
	if dst == nil {
		return false
	}
	return appendAndLeavesModeBuf(dst, e, andLeafModeCollect)
}

func extractAndLeaves(e qir.Expr) ([]qir.Expr, bool) {
	return collectAndLeavesMode(e, andLeafModeExtract)
}

func extractAndLeavesScratch(e qir.Expr, dst []qir.Expr) ([]qir.Expr, bool) {
	return collectAndLeavesModeScratch(e, dst, andLeafModeExtract)
}

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

func appendAndLeavesModeBuf(dst *pooled.SliceBuf[qir.Expr], e qir.Expr, mode andLeafMode) bool {
	switch e.Op {
	case qir.OpNOOP:
		if mode != andLeafModeCollect {
			return false
		}
		if !e.Not {
			return true
		}
		dst.Append(qir.Expr{Op: qir.OpNOOP, Not: true, FieldOrdinal: qir.NoFieldOrdinal})
		return true
	case qir.OpAND:
		if e.Not || len(e.Operands) == 0 {
			return false
		}
		for _, ch := range e.Operands {
			if !appendAndLeavesModeBuf(dst, ch, mode) {
				return false
			}
		}
		return true
	default:
		if e.Op == qir.OpOR || len(e.Operands) != 0 {
			return false
		}
		if mode == andLeafModeExtract && e.Not {
			return false
		}
		dst.Append(e)
		return true
	}
}
