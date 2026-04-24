package qir

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/pooled"
)

type Op byte

const (
	OpNOOP Op = iota

	OpAND
	OpOR

	OpEQ
	OpGT
	OpGTE
	OpLT
	OpLTE
	OpIN

	OpHASALL
	OpHASANY

	OpPREFIX
	OpSUFFIX
	OpCONTAINS
)

var opNames = [...]string{
	OpNOOP:     "NOOP",
	OpAND:      "AND",
	OpOR:       "OR",
	OpEQ:       "EQ",
	OpGT:       "GT",
	OpGTE:      "GTE",
	OpLT:       "LT",
	OpLTE:      "LTE",
	OpIN:       "IN",
	OpHASALL:   "HASALL",
	OpHASANY:   "HASANY",
	OpPREFIX:   "PREFIX",
	OpSUFFIX:   "SUFFIX",
	OpCONTAINS: "CONTAINS",
}

func (op Op) String() string {
	if int(op) >= 0 && int(op) < len(opNames) {
		return opNames[op]
	}
	return fmt.Sprintf("Op(%d)", op)
}

type OrderKind byte

const (
	OrderKindBasic OrderKind = iota
	OrderKindArrayPos
	OrderKindArrayCount
)

const NoFieldOrdinal = -1

type Expr struct {
	Op           Op
	Not          bool
	FieldOrdinal int
	Value        any
	Operands     []Expr
}

type Order struct {
	FieldOrdinal int
	Kind         OrderKind
	Data         any
	Desc         bool
}

type Query struct {
	Expr           Expr
	Order          []Order
	Offset         uint64
	Limit          uint64
	exprOwners     [][]Expr
	exprOwnersUsed int
	orderStorage   [1]Order
}

type FieldResolver interface {
	ResolveField(name string) (int, bool)
}

type noFieldResolver struct{}

func (noFieldResolver) ResolveField(string) (int, bool) {
	return 0, false
}

const (
	queryExprOwnerInitCap = 4
	queryExprOwnerMaxCap  = 256
	queryExprOwnersMaxLen = 128
)

var queryPool = pooled.Pointers[Query]{
	Cleanup: func(q *Query) {
		q.releaseOwned()
	},
	Clear: false,
}

var nilPrepareFieldOrdinals = pooled.Maps[string, int]{
	NewCap: 8,
	MaxLen: 64,
}

type prepareCompiler[R FieldResolver] struct {
	resolve             R
	useResolver         bool
	nilFieldOrdinals    map[string]int
	nextNilFieldOrdinal int
}

func (q *Query) Release() {
	queryPool.Put(q)
}

func (q *Query) releaseOwned() {
	if q == nil {
		return
	}
	for i := 0; i < q.exprOwnersUsed; i++ {
		s := q.exprOwners[i]
		if cap(s) > queryExprOwnerMaxCap {
			q.exprOwners[i] = nil
			continue
		}
		clear(s[:cap(s)])
		q.exprOwners[i] = s[:0]
	}
	q.exprOwnersUsed = 0
	if len(q.exprOwners) > queryExprOwnersMaxLen {
		q.exprOwners = nil
	}
	q.Expr = Expr{}
	q.orderStorage[0] = Order{}
	q.Order = nil
	q.Offset = 0
	q.Limit = 0
}

func newPrepareCompilerResolved[R FieldResolver](resolve R) prepareCompiler[R] {
	return prepareCompiler[R]{
		resolve:             resolve,
		useResolver:         true,
		nextNilFieldOrdinal: NoFieldOrdinal - 1,
	}
}

func newPrepareCompilerNoResolve() prepareCompiler[noFieldResolver] {
	return prepareCompiler[noFieldResolver]{
		useResolver:         false,
		nextNilFieldOrdinal: NoFieldOrdinal - 1,
	}
}

func (c *prepareCompiler[R]) release() {
	if c.nilFieldOrdinals != nil {
		nilPrepareFieldOrdinals.Put(c.nilFieldOrdinals)
		c.nilFieldOrdinals = nil
	}
}

func (c *prepareCompiler[R]) fieldOrdinal(name string) (int, bool) {
	if c.useResolver {
		return c.resolve.ResolveField(name)
	}
	if c.nilFieldOrdinals == nil {
		c.nilFieldOrdinals = nilPrepareFieldOrdinals.Get()
	}
	if fieldOrdinal, ok := c.nilFieldOrdinals[name]; ok {
		return fieldOrdinal, true
	}
	fieldOrdinal := c.nextNilFieldOrdinal
	c.nilFieldOrdinals[name] = fieldOrdinal
	c.nextNilFieldOrdinal--
	return fieldOrdinal, true
}

func (q *Query) ownExprTree(expr Expr) Expr {
	if len(expr.Operands) == 0 {
		return expr
	}
	dst := q.newOwnedExprSlice(len(expr.Operands))
	for i := range expr.Operands {
		dst[i] = q.ownExprTree(expr.Operands[i])
	}
	expr.Operands = dst
	return expr
}

func (q *Query) newOwnedExprSlice(n int) []Expr {
	if q.exprOwnersUsed < len(q.exprOwners) {
		s := q.exprOwners[q.exprOwnersUsed]
		if cap(s) < n {
			s = make([]Expr, n)
		} else {
			s = s[:n]
		}
		q.exprOwners[q.exprOwnersUsed] = s
		q.exprOwnersUsed++
		return s
	}
	capHint := max(queryExprOwnerInitCap, n)
	s := make([]Expr, n, capHint)
	q.exprOwners = append(q.exprOwners, s[:0])
	q.exprOwners[q.exprOwnersUsed] = s
	q.exprOwnersUsed++
	return s
}

func (q *Query) setOrder(order Order) {
	q.orderStorage[0] = order
	q.Order = q.orderStorage[:]
}

func PrepareQueryResolved[R FieldResolver](src *qx.QX, resolve R) (*Query, error) {
	if src == nil {
		return nil, fmt.Errorf("QX is nil")
	}
	if src.HasReduction() {
		return nil, fmt.Errorf("rbi does not support reduction/group/having")
	}
	if len(src.Projection) > 0 {
		return nil, fmt.Errorf("rbi does not support projection/select")
	}
	if len(src.Order) > 1 {
		return nil, fmt.Errorf("rbi does not support multi-column ordering")
	}

	query := queryPool.Get()
	compiler := newPrepareCompilerResolved(resolve)
	defer compiler.release()

	raw, err := compileFilter(query, src.Filter, &compiler)
	if err != nil {
		query.Release()
		return nil, err
	}
	query.setNormalizedExpr(raw)
	query.Offset = src.Window.Offset
	query.Limit = src.Window.Limit

	if len(src.Order) == 1 {
		order, err := compileOrder(src.Order[0], &compiler)
		if err != nil {
			query.Release()
			return nil, err
		}
		query.setOrder(order)
	}

	return query, nil
}

func PrepareQueryNoResolve(src *qx.QX) (*Query, error) {
	if src == nil {
		return nil, fmt.Errorf("QX is nil")
	}
	if src.HasReduction() {
		return nil, fmt.Errorf("rbi does not support reduction/group/having")
	}
	if len(src.Projection) > 0 {
		return nil, fmt.Errorf("rbi does not support projection/select")
	}
	if len(src.Order) > 1 {
		return nil, fmt.Errorf("rbi does not support multi-column ordering")
	}

	query := queryPool.Get()
	compiler := newPrepareCompilerNoResolve()
	defer compiler.release()

	raw, err := compileFilter(query, src.Filter, &compiler)
	if err != nil {
		query.Release()
		return nil, err
	}
	query.setNormalizedExpr(raw)
	query.Offset = src.Window.Offset
	query.Limit = src.Window.Limit

	if len(src.Order) == 1 {
		order, err := compileOrder(src.Order[0], &compiler)
		if err != nil {
			query.Release()
			return nil, err
		}
		query.setOrder(order)
	}

	return query, nil
}

func PrepareCountExprsResolved[R FieldResolver](resolve R, exprs ...qx.Expr) (*Query, error) {
	switch len(exprs) {
	case 0:
		query := queryPool.Get()
		query.Expr = Expr{Op: OpNOOP, FieldOrdinal: NoFieldOrdinal}
		return query, nil
	case 1:
		return PrepareCountExprResolved(resolve, exprs[0])
	default:
		query := queryPool.Get()
		compiler := newPrepareCompilerResolved(resolve)
		defer compiler.release()
		ops := query.newOwnedExprSlice(len(exprs))
		for i := range exprs {
			expr, err := compileFilter(query, exprs[i], &compiler)
			if err != nil {
				query.Release()
				return nil, err
			}
			ops[i] = expr
		}
		query.setNormalizedExpr(Expr{Op: OpAND, FieldOrdinal: NoFieldOrdinal, Operands: ops})
		return query, nil
	}
}

func PrepareCountExprsNoResolve(exprs ...qx.Expr) (*Query, error) {
	switch len(exprs) {
	case 0:
		query := queryPool.Get()
		query.Expr = Expr{Op: OpNOOP, FieldOrdinal: NoFieldOrdinal}
		return query, nil
	case 1:
		return PrepareCountExprNoResolve(exprs[0])
	default:
		query := queryPool.Get()
		compiler := newPrepareCompilerNoResolve()
		defer compiler.release()
		ops := query.newOwnedExprSlice(len(exprs))
		for i := range exprs {
			expr, err := compileFilter(query, exprs[i], &compiler)
			if err != nil {
				query.Release()
				return nil, err
			}
			ops[i] = expr
		}
		query.setNormalizedExpr(Expr{Op: OpAND, FieldOrdinal: NoFieldOrdinal, Operands: ops})
		return query, nil
	}
}

func PrepareCountExprResolved[R FieldResolver](resolve R, expr qx.Expr) (*Query, error) {
	query := queryPool.Get()
	compiler := newPrepareCompilerResolved(resolve)
	defer compiler.release()

	raw, err := compileFilter(query, expr, &compiler)
	if err != nil {
		query.Release()
		return nil, err
	}
	query.setNormalizedExpr(raw)
	return query, nil
}

func PrepareCountExprNoResolve(expr qx.Expr) (*Query, error) {
	query := queryPool.Get()
	compiler := newPrepareCompilerNoResolve()
	defer compiler.release()

	raw, err := compileFilter(query, expr, &compiler)
	if err != nil {
		query.Release()
		return nil, err
	}
	query.setNormalizedExpr(raw)
	return query, nil
}

func (q *Query) setNormalizedExpr(raw Expr) {
	if raw.Op != OpAND && raw.Op != OpOR {
		q.Expr = raw
		return
	}
	expr, changed := normalizeExprWithAlloc(raw, q.newOwnedExprBuf)
	if changed {
		q.Expr = expr
		return
	}
	q.Expr = raw
}

func compileFilter[R FieldResolver](q *Query, src qx.Expr, compiler *prepareCompiler[R]) (Expr, error) {
	if src.Kind == qx.KindNONE {
		if src.Name != "" || src.Value != nil || len(src.Args) != 0 {
			return Expr{}, fmt.Errorf("rbi: invalid empty filter expression")
		}
		return Expr{Op: OpNOOP, FieldOrdinal: NoFieldOrdinal}, nil
	}
	if src.Kind != qx.KindOP {
		return Expr{}, fmt.Errorf("rbi does not support filter expression kind %q", src.Kind)
	}

	switch src.Name {
	case qx.OpAND:
		out := Expr{Op: OpAND, FieldOrdinal: NoFieldOrdinal}
		if len(src.Args) == 0 {
			return out, nil
		}
		out.Operands = q.newOwnedExprSlice(len(src.Args))
		for i := range src.Args {
			child, err := compileFilter(q, src.Args[i], compiler)
			if err != nil {
				return Expr{}, err
			}
			out.Operands[i] = child
		}
		return out, nil

	case qx.OpOR:
		out := Expr{Op: OpOR, FieldOrdinal: NoFieldOrdinal}
		if len(src.Args) == 0 {
			return out, nil
		}
		out.Operands = q.newOwnedExprSlice(len(src.Args))
		for i := range src.Args {
			child, err := compileFilter(q, src.Args[i], compiler)
			if err != nil {
				return Expr{}, err
			}
			out.Operands[i] = child
		}
		return out, nil

	case qx.OpNOT:
		if len(src.Args) != 1 {
			return Expr{}, fmt.Errorf("rbi: invalid NOT expression")
		}
		child, err := compileFilter(q, src.Args[0], compiler)
		if err != nil {
			return Expr{}, err
		}
		child.Not = !child.Not
		return child, nil

	case qx.OpEQ:
		return compileLeaf(OpEQ, false, src.Args, compiler)
	case qx.OpNE:
		return compileLeaf(OpEQ, true, src.Args, compiler)
	case qx.OpGT:
		return compileLeaf(OpGT, false, src.Args, compiler)
	case qx.OpGTE:
		return compileLeaf(OpGTE, false, src.Args, compiler)
	case qx.OpLT:
		return compileLeaf(OpLT, false, src.Args, compiler)
	case qx.OpLTE:
		return compileLeaf(OpLTE, false, src.Args, compiler)
	case qx.OpIN:
		return compileLeaf(OpIN, false, src.Args, compiler)
	case qx.OpHASALL:
		return compileLeaf(OpHASALL, false, src.Args, compiler)
	case qx.OpHASANY:
		return compileLeaf(OpHASANY, false, src.Args, compiler)
	case qx.OpISNULL:
		if len(src.Args) != 1 {
			return Expr{}, fmt.Errorf("rbi: invalid %s expression", qx.OpISNULL)
		}
		_, fieldOrdinal, err := compileFieldRef(src.Args[0], compiler)
		if err != nil {
			return Expr{}, err
		}
		return Expr{
			Op:           OpEQ,
			FieldOrdinal: fieldOrdinal,
			Value:        nil,
		}, nil
	case qx.OpPREFIX:
		return compileLeaf(OpPREFIX, false, src.Args, compiler)
	case qx.OpSUFFIX:
		return compileLeaf(OpSUFFIX, false, src.Args, compiler)
	case qx.OpCONTAINS:
		return compileLeaf(OpCONTAINS, false, src.Args, compiler)
	default:
		return Expr{}, fmt.Errorf("rbi does not support filter operation %q", src.Name)
	}
}

func compileLeaf[R FieldResolver](op Op, not bool, args []qx.Expr, compiler *prepareCompiler[R]) (Expr, error) {
	if len(args) != 2 {
		return Expr{}, fmt.Errorf("rbi: invalid %v expression", op)
	}
	field, fieldOrdinal, err := compileFieldRef(args[0], compiler)
	if err != nil {
		return Expr{}, err
	}
	if args[1].Kind != qx.KindLIT {
		return Expr{}, fmt.Errorf("rbi does not support computed predicate values for field %q", field)
	}
	return Expr{
		Op:           op,
		Not:          not,
		FieldOrdinal: fieldOrdinal,
		Value:        args[1].Value,
	}, nil
}

func compileFieldRef[R FieldResolver](src qx.Expr, compiler *prepareCompiler[R]) (string, int, error) {
	if src.Kind != qx.KindREF || src.Name == "" {
		return "", NoFieldOrdinal, fmt.Errorf("rbi supports only source-field refs in filters/order")
	}
	fieldOrdinal, ok := compiler.fieldOrdinal(src.Name)
	if !ok {
		return "", NoFieldOrdinal, fmt.Errorf("no index for field: %v", src.Name)
	}
	return src.Name, fieldOrdinal, nil
}

func compileOrder[R FieldResolver](src qx.Order, compiler *prepareCompiler[R]) (Order, error) {
	by := src.By
	switch by.Kind {
	case qx.KindREF:
		_, ordinal, err := compileFieldRef(by, compiler)
		if err != nil {
			return Order{}, err
		}
		return Order{
			FieldOrdinal: ordinal,
			Kind:         OrderKindBasic,
			Desc:         src.Desc,
		}, nil

	case qx.KindOP:
		switch by.Name {
		case qx.OpLEN:
			if len(by.Args) != 1 {
				return Order{}, fmt.Errorf("rbi: invalid LEN order expression")
			}
			_, ordinal, err := compileFieldRef(by.Args[0], compiler)
			if err != nil {
				return Order{}, err
			}
			return Order{
				FieldOrdinal: ordinal,
				Kind:         OrderKindArrayCount,
				Desc:         src.Desc,
			}, nil

		case qx.OpPOS:
			if len(by.Args) != 2 {
				return Order{}, fmt.Errorf("rbi: invalid POS order expression")
			}
			field, ordinal, err := compileFieldRef(by.Args[0], compiler)
			if err != nil {
				return Order{}, err
			}
			if by.Args[1].Kind != qx.KindLIT {
				return Order{}, fmt.Errorf("rbi does not support computed POS order values for field %q", field)
			}
			if posOrderLiteralIsScalarString(by.Args[1].Value) {
				return Order{}, fmt.Errorf("rbi does not support scalar-string POS order values for field %q", field)
			}
			return Order{
				FieldOrdinal: ordinal,
				Kind:         OrderKindArrayPos,
				Data:         by.Args[1].Value,
				Desc:         src.Desc,
			}, nil
		}
	default:
		return Order{}, fmt.Errorf("rbi does not support order expression")
	}

	return Order{}, fmt.Errorf("rbi does not support order expression")
}

func posOrderLiteralIsScalarString(v any) bool {
	rv := reflect.ValueOf(v)
	for rv.IsValid() {
		switch rv.Kind() {
		case reflect.Interface, reflect.Pointer:
			if rv.IsNil() {
				return false
			}
			rv = rv.Elem()
		default:
			return rv.Kind() == reflect.String
		}
	}
	return false
}

func NewQuery(exprs ...Expr) *Query {
	query := queryPool.Get()
	switch len(exprs) {
	case 0:
		query.Expr = Expr{Op: OpNOOP, FieldOrdinal: NoFieldOrdinal}
	case 1:
		query.Expr = query.ownExprTree(exprs[0])
	default:
		root := Expr{Op: OpAND, FieldOrdinal: NoFieldOrdinal, Operands: exprs}
		expr, _ := NormalizeExpr(root)
		query.Expr = query.ownExprTree(expr)
	}
	return query
}

func IsTrueConst(e Expr) bool {
	return e.Op == OpNOOP &&
		!e.Not &&
		e.FieldOrdinal == NoFieldOrdinal &&
		e.Value == nil &&
		len(e.Operands) == 0
}

func IsFalseConst(e Expr) bool {
	return e.Op == OpNOOP &&
		e.Not &&
		e.FieldOrdinal == NoFieldOrdinal &&
		e.Value == nil &&
		len(e.Operands) == 0
}

type exprBufAlloc func(capacity int) []Expr

func makeExprBuf(capacity int) []Expr {
	if capacity == 0 {
		return nil
	}
	return make([]Expr, 0, capacity)
}

func (q *Query) newOwnedExprBuf(capacity int) []Expr {
	if capacity == 0 {
		return nil
	}
	return q.newOwnedExprSlice(capacity)[:0]
}

func NormalizeExpr(e Expr) (Expr, bool) {
	return normalizeExprWithAlloc(e, makeExprBuf)
}

func normalizeExprWithAlloc(e Expr, alloc exprBufAlloc) (Expr, bool) {
	first, c1, postNeeded := normalizeExprInverted(e, false, alloc)
	if !c1 && !postNeeded {
		return e, false
	}

	second := first
	c2 := false
	if c1 || postNeeded {
		second, c2 = normalizeExprPost(first, alloc)
	}
	if c1 || c2 {
		return second, true
	}
	return e, false
}

func normalizeExprInverted(e Expr, invert bool, alloc exprBufAlloc) (Expr, bool, bool) {
	switch e.Op {
	case OpNOOP:
		out, changed := normalizeNoop(e, invert)
		return out, changed, false
	case OpAND, OpOR:
		return normalizeBoolNode(e, invert, alloc)
	default:
		if !invert {
			return e, false, false
		}
		out := e
		out.Not = !out.Not
		return out, true, false
	}
}

func normalizeNoop(e Expr, invert bool) (Expr, bool) {
	if e.FieldOrdinal != NoFieldOrdinal || e.Value != nil || len(e.Operands) != 0 {
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
		return Expr{Op: OpNOOP, Not: true, FieldOrdinal: NoFieldOrdinal}, !IsFalseConst(e)
	}
	return Expr{Op: OpNOOP, FieldOrdinal: NoFieldOrdinal}, !IsTrueConst(e)
}

func normalizeBoolNode(e Expr, invert bool, alloc exprBufAlloc) (Expr, bool, bool) {
	if len(e.Operands) == 0 {
		if !invert {
			return e, false, e.Op == OpAND
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
		if op == OpAND {
			op = OpOR
		} else {
			op = OpAND
		}
	}

	changed := childInvert || e.Not
	postNeeded := false
	var out []Expr
	if changed {
		out = alloc(len(e.Operands))
	}

	for i, ch := range e.Operands {
		nc, c, childPost := normalizeExprInverted(ch, childInvert, alloc)
		if c {
			if !changed {
				changed = true
				out = alloc(len(e.Operands))
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

		if op == OpAND {
			if IsFalseConst(nc) {
				return Expr{Op: OpNOOP, Not: true, FieldOrdinal: NoFieldOrdinal}, true, false
			}
			if IsTrueConst(nc) {
				if !changed {
					changed = true
					out = alloc(len(e.Operands))
					out = append(out, e.Operands[:i]...)
				}
				continue
			}
		} else {
			if IsTrueConst(nc) {
				return Expr{Op: OpNOOP, FieldOrdinal: NoFieldOrdinal}, true, false
			}
			if IsFalseConst(nc) {
				if !changed {
					changed = true
					out = alloc(len(e.Operands))
					out = append(out, e.Operands[:i]...)
				}
				continue
			}
		}

		if nc.Op == op && !nc.Not {
			if !changed {
				changed = true
				out = alloc(len(e.Operands))
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
		if simplified, exactChanged, constExpr, constOK := simplifyExactBoolTerms(op, e.Operands, alloc); constOK {
			return constExpr, true, false
		} else if exactChanged {
			out = simplified
		} else {
			return e, false, postNeeded
		}
	} else if simplified, exactChanged, constExpr, constOK := simplifyExactBoolTerms(op, out, alloc); constOK {
		return constExpr, true, false
	} else if exactChanged {
		out = simplified
	}

	if len(out) == 0 {
		if op == OpAND {
			return Expr{Op: OpNOOP, FieldOrdinal: NoFieldOrdinal}, true, false
		}
		return Expr{Op: OpNOOP, Not: true, FieldOrdinal: NoFieldOrdinal}, true, false
	}

	if len(out) == 1 {
		return out[0], true, false
	}

	if op == OpAND && needSortExprs(out) {
		sort.SliceStable(out, func(i, j int) bool {
			return lessExpr(out[i], out[j])
		})
	}
	return Expr{Op: op, FieldOrdinal: NoFieldOrdinal, Operands: out}, true, false
}

func isMalformedNoop(e Expr) bool {
	return e.Op == OpNOOP && (e.FieldOrdinal != NoFieldOrdinal || e.Value != nil || len(e.Operands) != 0)
}

func needSortExprs(exprs []Expr) bool {
	for i := 1; i < len(exprs); i++ {
		if lessExpr(exprs[i], exprs[i-1]) {
			return true
		}
	}
	return false
}

func lessExpr(a, b Expr) bool {
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
	if a.FieldOrdinal != b.FieldOrdinal {
		return a.FieldOrdinal < b.FieldOrdinal
	}
	if len(a.Operands) != len(b.Operands) {
		return len(a.Operands) < len(b.Operands)
	}
	return false
}

func exprWeight(e Expr) int {
	if e.Not {
		return 32
	}

	switch e.Op {
	case OpEQ:
		return 0
	case OpIN:
		return 1
	case OpHASALL, OpHASANY:
		return 2
	case OpGT, OpGTE, OpLT, OpLTE, OpPREFIX:
		return 3
	case OpOR:
		return 40
	case OpAND:
		return 41
	default:
		return 50
	}
}

func normalizeExprPost(e Expr, alloc exprBufAlloc) (Expr, bool) {
	switch e.Op {
	case OpAND:
		return normalizeANDPost(e, alloc)
	case OpOR:
		return normalizeORPost(e, alloc)
	default:
		return e, false
	}
}

func normalizeANDPost(expr Expr, alloc exprBufAlloc) (Expr, bool) {
	changed := false
	var out []Expr

	for i, ch := range expr.Operands {
		n, c := normalizeExprPost(ch, alloc)
		drop := n.Op == OpNOOP && !n.Not
		flatten := n.Op == OpAND && !n.Not
		if !changed && !c && !drop && !flatten {
			continue
		}
		if !changed {
			changed = true
			out = alloc(len(expr.Operands))
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
	if simplified, exactChanged, constExpr, constOK := simplifyExactBoolTerms(OpAND, out, alloc); constOK {
		return constExpr, true
	} else if exactChanged {
		out = simplified
	}

	if len(out) == 0 {
		return Expr{Op: OpNOOP, FieldOrdinal: NoFieldOrdinal}, true
	}
	if len(out) == 1 {
		return out[0], true
	}
	if needLeafSortExprs(out) {
		sort.SliceStable(out, func(i, j int) bool {
			return leafWeight(out[i]) < leafWeight(out[j])
		})
	}
	return Expr{Op: OpAND, FieldOrdinal: NoFieldOrdinal, Operands: out}, true
}

func normalizeORPost(e Expr, alloc exprBufAlloc) (Expr, bool) {
	changed := false
	var out []Expr

	for i, ch := range e.Operands {
		n, c := normalizeExprPost(ch, alloc)
		drop := n.Op == OpNOOP && !n.Not
		if !changed && !c && !drop {
			continue
		}
		if !changed {
			changed = true
			out = alloc(len(e.Operands))
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
	if simplified, exactChanged, constExpr, constOK := simplifyExactBoolTerms(OpOR, out, alloc); constOK {
		return constExpr, true
	} else if exactChanged {
		out = simplified
	}

	if len(out) == 1 {
		return out[0], true
	}
	return Expr{Op: OpOR, FieldOrdinal: NoFieldOrdinal, Operands: out}, true
}

func needLeafSortExprs(exprs []Expr) bool {
	for i := 1; i < len(exprs); i++ {
		if leafWeight(exprs[i]) < leafWeight(exprs[i-1]) {
			return true
		}
	}
	return false
}

func simplifyExactBoolTerms(op Op, terms []Expr, alloc exprBufAlloc) ([]Expr, bool, Expr, bool) {
	if len(terms) < 2 {
		return terms, false, Expr{}, false
	}

	changed := false
	var out []Expr

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
			if op == OpAND {
				return nil, true, Expr{Op: OpNOOP, Not: true, FieldOrdinal: NoFieldOrdinal}, true
			}
			return nil, true, Expr{Op: OpNOOP, FieldOrdinal: NoFieldOrdinal}, true
		}

		if !dup {
			if changed {
				out = append(out, cur)
			}
			continue
		}

		if !changed {
			changed = true
			out = alloc(len(terms) - 1)
			out = append(out, terms[:i]...)
		}
	}

	if !changed {
		return terms, false, Expr{}, false
	}
	return out, true, Expr{}, false
}

func exprExactLeafMatch(a, b Expr) bool {
	if len(a.Operands) != 0 || len(b.Operands) != 0 {
		return false
	}
	if a.Op == OpNOOP || b.Op == OpNOOP {
		return false
	}
	if a.Op != b.Op || a.FieldOrdinal != b.FieldOrdinal {
		return false
	}
	return reflect.DeepEqual(a.Value, b.Value)
}

func leafWeight(e Expr) int {
	switch e.Op {
	case OpEQ:
		return 0
	case OpIN:
		return 1
	case OpHASALL, OpHASANY:
		return 2
	case OpGT, OpGTE, OpLT, OpLTE, OpPREFIX:
		return 3
	default:
		return 4
	}
}
