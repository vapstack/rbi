package rbi

import (
	"sort"

	"github.com/vapstack/qx"
)

func normalizeQuery(q *qx.QX) *qx.QX {
	expr, changed := normalizeExpr(q.Expr)
	if !changed {
		return q
	}
	nq := *q
	nq.Expr = expr
	return &nq
}

func normalizeExpr(e qx.Expr) (qx.Expr, bool) {
	first := e
	c1 := false
	if shouldNormalizeExpr(e) {
		first, c1 = normalizeExprInverted(e, false)
	}
	second, c2 := normalizeExprPost(first)
	if c1 || c2 {
		return second, true
	}
	return e, false
}

func normalizeExprInverted(e qx.Expr, invert bool) (qx.Expr, bool) {
	switch e.Op {
	case qx.OpNOOP:
		return normalizeNoop(e, invert)
	case qx.OpAND, qx.OpOR:
		return normalizeBoolNode(e, invert)
	default:
		if !invert {
			return e, false
		}
		out := e
		out.Not = !out.Not
		return out, true
	}
}

func shouldNormalizeExpr(e qx.Expr) bool {
	switch e.Op {
	case qx.OpNOOP:
		// malformed NOOP or NOT NOOP should be canonicalized once
		return e.Not || e.Field != "" || e.Value != nil || len(e.Operands) != 0

	case qx.OpAND, qx.OpOR:
		if e.Not {
			return true
		}
		if len(e.Operands) <= 1 {
			return len(e.Operands) == 1
		}

		for _, ch := range e.Operands {
			if ch.Op == e.Op && !ch.Not {
				return true
			}
			if e.Op == qx.OpAND {
				if isTrueConst(ch) || isFalseConst(ch) {
					return true
				}
			} else {
				if isTrueConst(ch) || isFalseConst(ch) {
					return true
				}
			}
			if shouldNormalizeExpr(ch) {
				return true
			}
		}
		return false

	default:
		return false
	}
}

func normalizeNoop(e qx.Expr, invert bool) (qx.Expr, bool) {
	// keep malformed NOOP untouched so downstream validation keeps rejecting it
	if e.Field != "" || e.Value != nil || len(e.Operands) != 0 {
		if !invert {
			return e, false
		}
		out := e
		out.Not = !out.Not
		return out, true
	}

	neg := e.Not
	if invert {
		neg = !neg
	}

	if neg {
		return qx.Expr{Op: qx.OpNOOP, Not: true}, !isFalseConst(e)
	}
	return qx.Expr{Op: qx.OpNOOP}, !isTrueConst(e)
}

func normalizeBoolNode(e qx.Expr, invert bool) (qx.Expr, bool) {
	// preserve malformed composites to keep error behavior unchanged
	if len(e.Operands) == 0 {
		if !invert {
			return e, false
		}
		out := e
		out.Not = !out.Not
		return out, true
	}

	neg := e.Not
	if invert {
		neg = !neg
	}

	op := e.Op
	childInvert := false
	if neg {
		childInvert = true
		if op == qx.OpAND {
			op = qx.OpOR
		} else {
			op = qx.OpAND
		}
	}

	out := make([]qx.Expr, 0, len(e.Operands))
	changed := childInvert || e.Not

	for _, ch := range e.Operands {
		nc, c := normalizeExprInverted(ch, childInvert)
		if c {
			changed = true
		}

		if isMalformedNoop(nc) {
			out = append(out, nc)
			continue
		}

		if op == qx.OpAND {
			if isFalseConst(nc) {
				return qx.Expr{Op: qx.OpNOOP, Not: true}, true
			}
			if isTrueConst(nc) {
				changed = true
				continue
			}
		} else {
			if isTrueConst(nc) {
				return qx.Expr{Op: qx.OpNOOP}, true
			}
			if isFalseConst(nc) {
				changed = true
				continue
			}
		}

		if nc.Op == op && !nc.Not {
			out = append(out, nc.Operands...)
			changed = true
			continue
		}

		out = append(out, nc)
	}

	if len(out) == 0 {
		if op == qx.OpAND {
			return qx.Expr{Op: qx.OpNOOP}, true
		}
		return qx.Expr{Op: qx.OpNOOP, Not: true}, true
	}

	if len(out) == 1 {
		return out[0], true
	}

	if op == qx.OpAND && needSortExprs(out) {
		sort.SliceStable(out, func(i, j int) bool {
			return lessExpr(out[i], out[j])
		})
		changed = true
	}

	if !changed && op == e.Op && !e.Not && len(out) == len(e.Operands) {
		return e, false
	}

	return qx.Expr{Op: op, Operands: out}, true
}

func isMalformedNoop(e qx.Expr) bool {
	return e.Op == qx.OpNOOP && (e.Field != "" || e.Value != nil || len(e.Operands) != 0)
}

func isTrueConst(e qx.Expr) bool {
	return e.Op == qx.OpNOOP &&
		!e.Not &&
		e.Field == "" &&
		e.Value == nil &&
		len(e.Operands) == 0
}

func isFalseConst(e qx.Expr) bool {
	return e.Op == qx.OpNOOP &&
		e.Not &&
		e.Field == "" &&
		e.Value == nil &&
		len(e.Operands) == 0
}

func needSortExprs(exprs []qx.Expr) bool {
	for i := 1; i < len(exprs); i++ {
		if lessExpr(exprs[i], exprs[i-1]) {
			return true
		}
	}
	return false
}

func lessExpr(a, b qx.Expr) bool {
	wa, wb := exprWeight(a), exprWeight(b)
	if wa != wb {
		return wa < wb
	}
	if a.Op != b.Op {
		return a.Op < b.Op
	}
	if a.Not != b.Not {
		return !a.Not
	}
	if a.Field != b.Field {
		return a.Field < b.Field
	}
	if len(a.Operands) != len(b.Operands) {
		return len(a.Operands) < len(b.Operands)
	}
	return false
}

func exprWeight(e qx.Expr) int {
	if e.Not {
		return 32
	}

	switch e.Op {
	case qx.OpEQ:
		return 0
	case qx.OpIN:
		return 1
	case qx.OpHAS, qx.OpHASANY:
		return 2
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
		return 3
	case qx.OpOR:
		return 40
	case qx.OpAND:
		return 41
	default:
		return 50
	}
}

func normalizeExprPost(e qx.Expr) (qx.Expr, bool) {
	switch e.Op {
	case qx.OpAND:
		return normalizeANDPost(e)
	case qx.OpOR:
		return normalizeORPost(e)
	default:
		return e, false
	}
}

func normalizeANDPost(expr qx.Expr) (qx.Expr, bool) {
	out := make([]qx.Expr, 0, len(expr.Operands))
	changed := false

	for _, ch := range expr.Operands {
		n, c := normalizeExprPost(ch)
		if c {
			changed = true
		}

		if n.Op == qx.OpNOOP && !n.Not {
			changed = true
			continue
		}

		if n.Op == qx.OpAND && !n.Not {
			out = append(out, n.Operands...)
			changed = true
			continue
		}

		out = append(out, n)
	}

	if len(out) == 0 {
		return qx.Expr{Op: qx.OpNOOP}, true
	}
	if len(out) == 1 {
		return out[0], true
	}

	sort.SliceStable(out, func(i, j int) bool {
		return leafWeight(out[i]) < leafWeight(out[j])
	})

	if !changed {
		return expr, false
	}
	return qx.Expr{Op: qx.OpAND, Operands: out}, true
}

func normalizeORPost(e qx.Expr) (qx.Expr, bool) {
	out := make([]qx.Expr, 0, len(e.Operands))
	changed := false

	for _, ch := range e.Operands {
		n, c := normalizeExprPost(ch)
		if c {
			changed = true
		}
		if n.Op == qx.OpNOOP && !n.Not {
			changed = true
			continue
		}
		out = append(out, n)
	}

	if len(out) == 1 {
		return out[0], true
	}

	if !changed {
		return e, false
	}
	return qx.Expr{Op: qx.OpOR, Operands: out}, true
}

func leafWeight(e qx.Expr) int {
	switch e.Op {
	case qx.OpEQ:
		return 0
	case qx.OpIN:
		return 1
	case qx.OpHAS, qx.OpHASANY:
		return 2
	case qx.OpGT, qx.OpGTE, qx.OpLT, qx.OpLTE, qx.OpPREFIX:
		return 3
	default:
		return 4
	}
}
