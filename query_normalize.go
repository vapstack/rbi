package rbi

import (
	"sort"

	"github.com/vapstack/qx"
)

func normalizeQuery(q *qx.QX) *qx.QX {
	changed := false

	expr, c := normalizeExpr(q.Expr)
	if c {
		changed = true
	}
	if !changed {
		return q
	}
	nq := *q
	nq.Expr = expr
	return &nq
}

func normalizeExpr(e qx.Expr) (qx.Expr, bool) {
	switch e.Op {
	case qx.OpAND:
		return normalizeAND(e)
	case qx.OpOR:
		return normalizeOR(e)
	default:
		return e, false
	}
}

func normalizeAND(expr qx.Expr) (qx.Expr, bool) {
	var out []qx.Expr
	changed := false

	for _, ch := range expr.Operands {
		e, c := normalizeExpr(ch)
		if c {
			changed = true
		}

		if e.Op == qx.OpNOOP && !e.Not {
			changed = true
			continue
		}

		if e.Op == qx.OpAND && !e.Not {
			out = append(out, e.Operands...)
			changed = true
			continue
		}

		out = append(out, e)
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

func normalizeOR(e qx.Expr) (qx.Expr, bool) {
	var out []qx.Expr
	changed := false

	for _, ch := range e.Operands {
		nc, c := normalizeExpr(ch)
		if c {
			changed = true
		}
		if nc.Op == qx.OpNOOP && !nc.Not {
			changed = true
			continue
		}
		out = append(out, nc)
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
