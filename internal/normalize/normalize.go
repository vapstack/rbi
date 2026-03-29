package normalize

import (
	"reflect"
	"sort"

	"github.com/vapstack/qx"
)

func Query(q *qx.QX) *qx.QX {
	expr, changed := Expr(q.Expr)
	if !changed {
		return q
	}
	nq := *q
	nq.Expr = expr
	return &nq
}

func Expr(e qx.Expr) (qx.Expr, bool) {
	first, c1, postNeeded := normalizeExprInverted(e, false)
	if !c1 && !postNeeded {
		return e, false
	}

	second := first
	c2 := false
	if c1 || postNeeded {
		second, c2 = normalizeExprPost(first)
	}
	if c1 || c2 {
		return second, true
	}
	return e, false
}

func normalizeExprInverted(e qx.Expr, invert bool) (qx.Expr, bool, bool) {
	switch e.Op {
	case qx.OpNOOP:
		out, changed := normalizeNoop(e, invert)
		return out, changed, false
	case qx.OpAND, qx.OpOR:
		return normalizeBoolNode(e, invert)
	default:
		if !invert {
			return e, false, false
		}
		out := e
		out.Not = !out.Not
		return out, true, false
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
		return qx.Expr{Op: qx.OpNOOP, Not: true}, !IsFalseConst(e)
	}
	return qx.Expr{Op: qx.OpNOOP}, !IsTrueConst(e)
}

func normalizeBoolNode(e qx.Expr, invert bool) (qx.Expr, bool, bool) {
	// preserve malformed composites to keep error behavior unchanged
	if len(e.Operands) == 0 {
		if !invert {
			return e, false, e.Op == qx.OpAND
		}
		out := e
		out.Not = !out.Not
		return out, true, false
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

	changed := childInvert || e.Not
	postNeeded := false
	var out []qx.Expr
	if changed {
		out = make([]qx.Expr, 0, len(e.Operands))
	}

	for i, ch := range e.Operands {
		nc, c, childPost := normalizeExprInverted(ch, childInvert)
		if c {
			if !changed {
				changed = true
				out = make([]qx.Expr, 0, len(e.Operands))
				out = append(out, e.Operands[:i]...)
			}
		}

		if isMalformedNoop(nc) {
			if childPost {
				postNeeded = true
			}
			if changed {
				out = append(out, nc)
			}
			continue
		}

		if op == qx.OpAND {
			if IsFalseConst(nc) {
				return qx.Expr{Op: qx.OpNOOP, Not: true}, true, false
			}
			if IsTrueConst(nc) {
				if !changed {
					changed = true
					out = make([]qx.Expr, 0, len(e.Operands))
					out = append(out, e.Operands[:i]...)
				}
				continue
			}
		} else {
			if IsTrueConst(nc) {
				return qx.Expr{Op: qx.OpNOOP}, true, false
			}
			if IsFalseConst(nc) {
				if !changed {
					changed = true
					out = make([]qx.Expr, 0, len(e.Operands))
					out = append(out, e.Operands[:i]...)
				}
				continue
			}
		}

		if nc.Op == op && !nc.Not {
			if !changed {
				changed = true
				out = make([]qx.Expr, 0, len(e.Operands))
				out = append(out, e.Operands[:i]...)
			}
			out = append(out, nc.Operands...)
			if childPost {
				postNeeded = true
			}
			continue
		}

		if childPost {
			postNeeded = true
		}
		if changed {
			out = append(out, nc)
		}
	}

	if !changed {
		if simplified, exactChanged, constExpr, constOK := simplifyExactBoolTerms(op, e.Operands); constOK {
			return constExpr, true, false
		} else if exactChanged {
			out = simplified
		} else {
			return e, false, postNeeded
		}
	} else if simplified, exactChanged, constExpr, constOK := simplifyExactBoolTerms(op, out); constOK {
		return constExpr, true, false
	} else if exactChanged {
		out = simplified
	}

	if len(out) == 0 {
		if op == qx.OpAND {
			return qx.Expr{Op: qx.OpNOOP}, true, false
		}
		return qx.Expr{Op: qx.OpNOOP, Not: true}, true, false
	}

	if len(out) == 1 {
		return out[0], true, false
	}

	if op == qx.OpAND && needSortExprs(out) {
		sort.SliceStable(out, func(i, j int) bool {
			return lessExpr(out[i], out[j])
		})
	}
	return qx.Expr{Op: op, Operands: out}, true, false
}

func isMalformedNoop(e qx.Expr) bool {
	return e.Op == qx.OpNOOP && (e.Field != "" || e.Value != nil || len(e.Operands) != 0)
}

func IsTrueConst(e qx.Expr) bool {
	return e.Op == qx.OpNOOP &&
		!e.Not &&
		e.Field == "" &&
		e.Value == nil &&
		len(e.Operands) == 0
}

func IsFalseConst(e qx.Expr) bool {
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
	changed := false
	var out []qx.Expr

	for i, ch := range expr.Operands {
		n, c := normalizeExprPost(ch)
		drop := n.Op == qx.OpNOOP && !n.Not
		flatten := n.Op == qx.OpAND && !n.Not
		if !changed && !c && !drop && !flatten {
			continue
		}
		if !changed {
			changed = true
			out = make([]qx.Expr, 0, len(expr.Operands))
			out = append(out, expr.Operands[:i]...)
		}
		if drop {
			continue
		}
		if flatten {
			out = append(out, n.Operands...)
			continue
		}
		out = append(out, n)
	}

	if !changed {
		return expr, false
	}
	if simplified, exactChanged, constExpr, constOK := simplifyExactBoolTerms(qx.OpAND, out); constOK {
		return constExpr, true
	} else if exactChanged {
		out = simplified
	}

	if len(out) == 0 {
		return qx.Expr{Op: qx.OpNOOP}, true
	}
	if len(out) == 1 {
		return out[0], true
	}
	if needLeafSortExprs(out) {
		sort.SliceStable(out, func(i, j int) bool {
			return leafWeight(out[i]) < leafWeight(out[j])
		})
	}
	return qx.Expr{Op: qx.OpAND, Operands: out}, true
}

func normalizeORPost(e qx.Expr) (qx.Expr, bool) {
	changed := false
	var out []qx.Expr

	for i, ch := range e.Operands {
		n, c := normalizeExprPost(ch)
		drop := n.Op == qx.OpNOOP && !n.Not
		if !changed && !c && !drop {
			continue
		}
		if !changed {
			changed = true
			out = make([]qx.Expr, 0, len(e.Operands))
			out = append(out, e.Operands[:i]...)
		}
		if drop {
			continue
		}
		out = append(out, n)
	}

	if !changed {
		return e, false
	}
	if simplified, exactChanged, constExpr, constOK := simplifyExactBoolTerms(qx.OpOR, out); constOK {
		return constExpr, true
	} else if exactChanged {
		out = simplified
	}

	if len(out) == 1 {
		return out[0], true
	}
	return qx.Expr{Op: qx.OpOR, Operands: out}, true
}

func needLeafSortExprs(exprs []qx.Expr) bool {
	for i := 1; i < len(exprs); i++ {
		if leafWeight(exprs[i]) < leafWeight(exprs[i-1]) {
			return true
		}
	}
	return false
}

func simplifyExactBoolTerms(op qx.Op, terms []qx.Expr) ([]qx.Expr, bool, qx.Expr, bool) {
	if len(terms) < 2 {
		return terms, false, qx.Expr{}, false
	}

	changed := false
	var out []qx.Expr

	for i, cur := range terms {
		prevTerms := terms[:i]
		if changed {
			prevTerms = out
		}

		dup := false
		for _, prev := range prevTerms {
			if !exprExactLeafMatch(prev, cur) {
				continue
			}
			if prev.Not == cur.Not {
				dup = true
				break
			}
			if op == qx.OpAND {
				return nil, true, qx.Expr{Op: qx.OpNOOP, Not: true}, true
			}
			return nil, true, qx.Expr{Op: qx.OpNOOP}, true
		}

		if !dup {
			if changed {
				out = append(out, cur)
			}
			continue
		}

		if !changed {
			changed = true
			out = make([]qx.Expr, 0, len(terms)-1)
			out = append(out, terms[:i]...)
		}
	}

	if !changed {
		return terms, false, qx.Expr{}, false
	}
	return out, true, qx.Expr{}, false
}

func exprExactLeafMatch(a, b qx.Expr) bool {
	if len(a.Operands) != 0 || len(b.Operands) != 0 {
		return false
	}
	if a.Op == qx.OpNOOP || b.Op == qx.OpNOOP {
		return false
	}
	if a.Op != b.Op || a.Field != b.Field {
		return false
	}
	return reflect.DeepEqual(a.Value, b.Value)
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
