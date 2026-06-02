package qir

import (
	"reflect"
	"testing"

	"github.com/vapstack/qx"
)

type testPrepareResolver struct{}

func (testPrepareResolver) ResolveField(name string) (int, bool) {
	switch name {
	case "status":
		return 1, true
	case "tags":
		return 2, true
	default:
		return 0, false
	}
}

var testPrepareFieldResolver testPrepareResolver

func TestPrepareCountExpr_TrimmedManualOpNameRejected(t *testing.T) {
	_, err := PrepareCountExprResolved(testPrepareFieldResolver, qx.Expr{
		Kind: qx.KindOP,
		Name: "  eq  ",
		Args: []qx.Expr{
			qx.REF("status"),
			qx.LIT("active"),
		},
	})
	if err == nil {
		t.Fatal("expected PrepareCountExpr to reject non-canonical op name")
	}
}

func TestPrepareQuery_TrimmedManualOrderOpNameRejected(t *testing.T) {
	_, err := PrepareQuery(&qx.QX{
		Filter: qx.EQ("status", "active"),
		Order: []qx.Order{{
			By: qx.Expr{
				Kind: qx.KindOP,
				Name: "  pos  ",
				Args: []qx.Expr{
					qx.REF("tags"),
					qx.LIT("go"),
				},
			},
			Desc: true,
		}},
	}, testPrepareFieldResolver)
	if err == nil {
		t.Fatal("expected PrepareQuery to reject non-canonical order op name")
	}
}

func TestPrepareCountExpr_MalformedKindNoneRejected(t *testing.T) {
	_, err := PrepareCountExprResolved(testPrepareFieldResolver, qx.Expr{
		Kind:  qx.KindNONE,
		Name:  qx.OpEQ,
		Value: "active",
		Args: []qx.Expr{
			qx.REF("status"),
			qx.LIT("active"),
		},
	})
	if err == nil {
		t.Fatal("expected PrepareCountExpr to reject malformed KindNONE expression")
	}
}

func TestPrepareQuery_POSScalarStringRejected(t *testing.T) {
	_, err := PrepareQuery(
		qx.Query().SortBy(qx.POS("status", "alice bob"), qx.ASC),
		testPrepareFieldResolver,
	)
	if err == nil {
		t.Fatal("expected PrepareQuery to reject scalar-string POS order literal")
	}
}

func TestPrepareCountExpr_ISNULLCompilesAsEqNil(t *testing.T) {
	got, err := PrepareCountExprResolved(testPrepareFieldResolver, qx.ISNULL("status"))
	if err != nil {
		t.Fatalf("PrepareCountExprResolved(ISNULL): %v", err)
	}
	defer got.Release()

	want, err := PrepareCountExprResolved(testPrepareFieldResolver, qx.EQ("status", nil))
	if err != nil {
		t.Fatalf("PrepareCountExprResolved(EQ nil): %v", err)
	}
	defer want.Release()

	if !reflect.DeepEqual(got.Expr, want.Expr) {
		t.Fatalf("unexpected compiled expr:\n got=%+v\nwant=%+v", got.Expr, want.Expr)
	}
}

func TestPrepareCountExpr_NOTNULLCompilesAsNeNil(t *testing.T) {
	got, err := PrepareCountExprResolved(testPrepareFieldResolver, qx.NOTNULL("status"))
	if err != nil {
		t.Fatalf("PrepareCountExprResolved(NOTNULL): %v", err)
	}
	defer got.Release()

	want, err := PrepareCountExprResolved(testPrepareFieldResolver, qx.NE("status", nil))
	if err != nil {
		t.Fatalf("PrepareCountExprResolved(NE nil): %v", err)
	}
	defer want.Release()

	if !reflect.DeepEqual(got.Expr, want.Expr) {
		t.Fatalf("unexpected compiled expr:\n got=%+v\nwant=%+v", got.Expr, want.Expr)
	}
}

func TestQueryReleaseOwned_ClearsInlineOrderStorage(t *testing.T) {
	q := queryPool.Get()
	q.orderStorage[0] = Order{
		FieldOrdinal: 2,
		Kind:         OrderKindArrayPos,
		Data:         []string{"go", "ops"},
		Desc:         true,
	}
	q.Order = q.orderStorage[:]

	q.releaseOwned()

	if q.Order != nil {
		t.Fatalf("expected releaseOwned to drop query order slice")
	}
	if q.orderStorage[0].FieldOrdinal != 0 || q.orderStorage[0].Kind != 0 || q.orderStorage[0].Data != nil || q.orderStorage[0].Desc {
		t.Fatalf("expected releaseOwned to clear inline order storage, got %+v", q.orderStorage[0])
	}
}

func TestQueryReleaseOwnedClearsExprOwnerStorage(t *testing.T) {
	q := queryPool.Get()
	owned := q.newOwnedExprSlice(2)
	owned[0] = Expr{Op: OpEQ, FieldOrdinal: 1, Value: "active"}
	owned[1] = Expr{Op: OpPREFIX, FieldOrdinal: 2, Value: "go"}
	q.Expr = Expr{Op: OpAND, FieldOrdinal: NoFieldOrdinal, Operands: owned}

	q.releaseOwned()
	if q.exprOwnersUsed != 0 || q.Expr.Op != 0 || q.Expr.Operands != nil {
		t.Fatalf("releaseOwned left query state: exprOwnersUsed=%d expr=%+v", q.exprOwnersUsed, q.Expr)
	}
	if len(q.exprOwners) == 0 {
		t.Fatalf("expected reusable expr owner storage")
	}
	reusable := q.exprOwners[0]
	for i, expr := range reusable[:cap(reusable)] {
		if expr.Op != 0 || expr.FieldOrdinal != 0 || expr.Value != nil || expr.Operands != nil {
			t.Fatalf("expr owner slot %d was not cleared: %+v", i, expr)
		}
	}

	next := q.newOwnedExprSlice(1)
	if next[0].Op != 0 || next[0].FieldOrdinal != 0 || next[0].Value != nil || next[0].Operands != nil {
		t.Fatalf("reused expr owner exposed stale predicate: %+v", next[0])
	}
	queryPool.Put(q)
}

func TestQueryReleaseOwnedDropsOversizedExprOwnerStorage(t *testing.T) {
	q := queryPool.Get()
	owned := q.newOwnedExprSlice(queryExprOwnerMaxCap + 1)
	owned[0] = Expr{Op: OpEQ, FieldOrdinal: 1, Value: "oversized"}

	q.releaseOwned()
	if len(q.exprOwners) == 0 || q.exprOwners[0] != nil {
		t.Fatalf("oversized expr owner storage was retained")
	}
	queryPool.Put(q)
}

func TestBuildQueryOwnsCallerOperandSlices(t *testing.T) {
	operands := []Expr{
		{Op: OpEQ, FieldOrdinal: 1, Value: "active"},
		{Op: OpPREFIX, FieldOrdinal: 2, Value: "go"},
	}
	source := Expr{Op: OpAND, FieldOrdinal: NoFieldOrdinal, Operands: operands}
	prepared := BuildQuery(source)
	defer prepared.Release()

	operands[0] = Expr{Op: OpEQ, FieldOrdinal: 9, Value: "mutated"}
	source.Operands[1] = Expr{Op: OpSUFFIX, FieldOrdinal: 8, Value: "poison"}

	if prepared.Expr.Op != OpAND || len(prepared.Expr.Operands) != 2 {
		t.Fatalf("expected AND with two owned operands, got %+v", prepared.Expr)
	}
	left := prepared.Expr.Operands[0]
	right := prepared.Expr.Operands[1]
	if left.Op != OpEQ || left.FieldOrdinal != 1 || left.Value != "active" {
		t.Fatalf("left operand aliases caller slice: %+v", left)
	}
	if right.Op != OpPREFIX || right.FieldOrdinal != 2 || right.Value != "go" {
		t.Fatalf("right operand aliases caller slice: %+v", right)
	}
}

func TestPrepareCountExprs_NilResolverPreservesDistinctFieldIdentity(t *testing.T) {
	prepared, err := PrepareCountExprsNoResolve(
		qx.EQ("status", "active"),
		qx.EQ("country", "active"),
	)
	if err != nil {
		t.Fatalf("PrepareCountExprs(nil): %v", err)
	}
	defer prepared.Release()

	if prepared.Expr.Op != OpAND || len(prepared.Expr.Operands) != 2 {
		t.Fatalf("expected AND with two leaves, got %+v", prepared.Expr)
	}

	left := prepared.Expr.Operands[0]
	right := prepared.Expr.Operands[1]
	if left.FieldOrdinal == NoFieldOrdinal || right.FieldOrdinal == NoFieldOrdinal {
		t.Fatalf("expected synthetic field ordinals, got left=%d right=%d", left.FieldOrdinal, right.FieldOrdinal)
	}
	if left.FieldOrdinal == right.FieldOrdinal {
		t.Fatalf("distinct fields collapsed to one identity: left=%+v right=%+v", left, right)
	}
}

func TestPrepareCountExprs_NilResolverKeepsSameFieldComplementFold(t *testing.T) {
	prepared, err := PrepareCountExprsNoResolve(
		qx.EQ("status", "active"),
		qx.NOT(qx.EQ("status", "active")),
	)
	if err != nil {
		t.Fatalf("PrepareCountExprs(nil): %v", err)
	}
	defer prepared.Release()

	if !IsFalseConst(prepared.Expr) {
		t.Fatalf("expected same-field complement to normalize to false, got %+v", prepared.Expr)
	}
}

func TestPrepareCompilerReleaseClearsNilFieldOrdinals(t *testing.T) {
	compiler := newPrepareCompilerNoResolve()
	status, ok := compiler.fieldOrdinal("status")
	if !ok {
		t.Fatal("status field ordinal was not assigned")
	}
	country, ok := compiler.fieldOrdinal("country")
	if !ok {
		t.Fatal("country field ordinal was not assigned")
	}
	again, ok := compiler.fieldOrdinal("status")
	if !ok || again != status {
		t.Fatalf("same field ordinal changed: got=%d want=%d ok=%v", again, status, ok)
	}
	if country == status {
		t.Fatalf("distinct no-resolve fields share ordinal %d", status)
	}

	owned := compiler.nilFieldOrdinals
	if len(owned) != 2 {
		t.Fatalf("nil ordinal map len=%d want 2", len(owned))
	}
	compiler.release()

	if compiler.nilFieldOrdinals != nil {
		t.Fatalf("compiler retained nil ordinal map after release")
	}
	if len(owned) != 0 {
		t.Fatalf("released nil ordinal map retained entries: %+v", owned)
	}
}

func TestPrepareCountExpr_NormalizedTreeAvoidsExtraOwnedCopy(t *testing.T) {
	prepared, err := PrepareCountExprResolved(testPrepareFieldResolver, qx.AND(
		qx.AND(
			qx.EQ("status", "active"),
			qx.PREFIX("tags", "go"),
		),
		qx.NOT(qx.EQ("status", "banned")),
	))
	if err != nil {
		t.Fatalf("PrepareCountExpr: %v", err)
	}
	defer prepared.Release()

	if prepared.Expr.Op != OpAND || len(prepared.Expr.Operands) != 3 {
		t.Fatalf("expected flattened AND with three leaves, got %+v", prepared.Expr)
	}
	if prepared.exprOwnersUsed != 3 {
		t.Fatalf("expected one raw outer slice, one raw inner slice, and one normalized root slice; got exprOwnersUsed=%d", prepared.exprOwnersUsed)
	}
}

func TestCollectAndLeaves_ExtractRejectsNegatedAndGroup(t *testing.T) {
	e := Expr{
		Op: OpAND,
		Operands: []Expr{
			{
				Op:  OpAND,
				Not: true,
				Operands: []Expr{
					{Op: OpEQ, FieldOrdinal: 1, Value: "a@example.com"},
					{Op: OpEQ, FieldOrdinal: 2, Value: 42},
				},
			},
			{Op: OpEQ, FieldOrdinal: 3, Value: true},
		},
	}

	leaves, ok := CollectAndLeaves(e, LeafModeExtract)
	if ok || leaves != nil {
		t.Fatalf("expected negated AND group to be rejected, got ok=%v leaves=%v", ok, leaves)
	}
}

func TestCollectAndLeavesFixed_RejectsNegatedAndGroup(t *testing.T) {
	e := Expr{
		Op: OpAND,
		Operands: []Expr{
			{
				Op:  OpAND,
				Not: true,
				Operands: []Expr{
					{Op: OpEQ, FieldOrdinal: 1, Value: "a@example.com"},
					{Op: OpEQ, FieldOrdinal: 2, Value: 42},
				},
			},
			{Op: OpEQ, FieldOrdinal: 3, Value: true},
		},
	}

	var buf [4]Expr
	leaves, ok := CollectAndLeavesFixed(e, buf[:0])
	if ok || leaves != nil {
		t.Fatalf("expected negated AND group to be rejected, got ok=%v leaves=%v", ok, leaves)
	}
}

func TestNormalizeExpr_OrFalseCollapsesToLeaf(t *testing.T) {
	leaf := Expr{Op: OpGTE, FieldOrdinal: 1, Value: 21}
	in := Expr{
		Op:           OpOR,
		FieldOrdinal: NoFieldOrdinal,
		Operands: []Expr{
			leaf,
			{Op: OpNOOP, Not: true, FieldOrdinal: NoFieldOrdinal},
		},
	}

	out, changed := NormalizeExpr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}
	if !reflect.DeepEqual(out, leaf) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, leaf)
	}
}

func TestNormalizeExpr_AlreadyCanonicalAND_AllocsPerRunStayZero(t *testing.T) {
	if testRaceEnabled {
		t.Skip("testing.AllocsPerRun is not stable under -race")
	}

	expr := Expr{
		Op: OpAND,
		Operands: []Expr{
			{Op: OpEQ, FieldOrdinal: 0, Value: true},
			{Op: OpIN, FieldOrdinal: 1, Value: []string{"DE", "NL"}},
			{Op: OpHASANY, FieldOrdinal: 2, Value: []string{"go", "ops"}},
			{Op: OpGTE, FieldOrdinal: 3, Value: 21},
		},
	}

	out, changed := NormalizeExpr(expr)
	if changed {
		t.Fatalf("expected canonical AND to stay unchanged")
	}
	if !reflect.DeepEqual(out, expr) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, expr)
	}

	allocs := testing.AllocsPerRun(100, func() {
		_, _ = NormalizeExpr(expr)
	})
	if allocs > 0 {
		t.Fatalf("unexpected allocs per run: got=%v want=0", allocs)
	}
}

func TestNormalizeExpr_AlreadyCanonicalOR_AllocsPerRunStayZero(t *testing.T) {
	if testRaceEnabled {
		t.Skip("testing.AllocsPerRun is not stable under -race")
	}

	expr := Expr{
		Op: OpOR,
		Operands: []Expr{
			{Op: OpEQ, FieldOrdinal: 0, Value: true},
			{Op: OpIN, FieldOrdinal: 1, Value: []string{"DE", "NL"}},
			{Op: OpPREFIX, FieldOrdinal: 2, Value: "ali"},
		},
	}

	out, changed := NormalizeExpr(expr)
	if changed {
		t.Fatalf("expected canonical OR to stay unchanged")
	}
	if !reflect.DeepEqual(out, expr) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, expr)
	}

	allocs := testing.AllocsPerRun(100, func() {
		_, _ = NormalizeExpr(expr)
	})
	if allocs > 0 {
		t.Fatalf("unexpected allocs per run: got=%v want=0", allocs)
	}
}

func TestNormalizeExpr_DeMorganForNotAnd(t *testing.T) {
	in := Expr{
		Op:           OpAND,
		Not:          true,
		FieldOrdinal: NoFieldOrdinal,
		Operands: []Expr{
			{Op: OpEQ, FieldOrdinal: 1, Value: true},
			{Op: OpIN, FieldOrdinal: 2, Value: []string{"DE", "NL"}},
		},
	}

	out, changed := NormalizeExpr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}

	want := Expr{
		Op:           OpOR,
		FieldOrdinal: NoFieldOrdinal,
		Operands: []Expr{
			{Op: OpEQ, FieldOrdinal: 1, Value: true, Not: true},
			{Op: OpIN, FieldOrdinal: 2, Value: []string{"DE", "NL"}, Not: true},
		},
	}

	if !reflect.DeepEqual(out, want) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, want)
	}
}

func TestNormalizeExpr_FlattensAndNoopNoise(t *testing.T) {
	leafA := Expr{Op: OpEQ, FieldOrdinal: 1, Value: true}
	leafB := Expr{Op: OpIN, FieldOrdinal: 2, Value: []string{"DE", "NL"}}

	in := Expr{
		Op:           OpAND,
		FieldOrdinal: NoFieldOrdinal,
		Operands: []Expr{
			{Op: OpNOOP, FieldOrdinal: NoFieldOrdinal},
			{
				Op:           OpAND,
				FieldOrdinal: NoFieldOrdinal,
				Operands: []Expr{
					leafA,
					{Op: OpNOOP, FieldOrdinal: NoFieldOrdinal},
				},
			},
			leafB,
		},
	}

	out, changed := NormalizeExpr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}

	want := Expr{
		Op:           OpAND,
		FieldOrdinal: NoFieldOrdinal,
		Operands: []Expr{
			leafA,
			leafB,
		},
	}

	if !reflect.DeepEqual(out, want) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, want)
	}
}

func TestNormalizeExpr_DoubleNotEliminates(t *testing.T) {
	leaf := Expr{Op: OpEQ, FieldOrdinal: 1, Value: 33}
	in := Expr{
		Op:           OpOR,
		Not:          true,
		FieldOrdinal: NoFieldOrdinal,
		Operands: []Expr{
			{Op: OpAND, Not: true, FieldOrdinal: NoFieldOrdinal, Operands: []Expr{leaf}},
		},
	}

	out, changed := NormalizeExpr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}
	if !reflect.DeepEqual(out, leaf) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, leaf)
	}
}

func TestNormalizeExpr_ExactDuplicateANDCollapses(t *testing.T) {
	leaf := Expr{Op: OpEQ, FieldOrdinal: 1, Value: true}
	in := Expr{
		Op:           OpAND,
		FieldOrdinal: NoFieldOrdinal,
		Operands: []Expr{
			leaf,
			leaf,
		},
	}

	out, changed := NormalizeExpr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}
	if !reflect.DeepEqual(out, leaf) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, leaf)
	}
}

func TestNormalizeExpr_ExactDuplicateORCollapses(t *testing.T) {
	leaf := Expr{Op: OpPREFIX, FieldOrdinal: 1, Value: "ali"}
	in := Expr{
		Op:           OpOR,
		FieldOrdinal: NoFieldOrdinal,
		Operands: []Expr{
			leaf,
			leaf,
		},
	}

	out, changed := NormalizeExpr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}
	if !reflect.DeepEqual(out, leaf) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, leaf)
	}
}

func TestNormalizeExpr_ExactLeafComplementANDToFalse(t *testing.T) {
	leaf := Expr{Op: OpEQ, FieldOrdinal: 1, Value: true}
	in := Expr{
		Op:           OpAND,
		FieldOrdinal: NoFieldOrdinal,
		Operands: []Expr{
			leaf,
			{Op: leaf.Op, FieldOrdinal: leaf.FieldOrdinal, Value: leaf.Value, Not: true},
		},
	}

	out, changed := NormalizeExpr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}
	want := Expr{Op: OpNOOP, Not: true, FieldOrdinal: NoFieldOrdinal}
	if !reflect.DeepEqual(out, want) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, want)
	}
}

func TestNormalizeExpr_ExactLeafComplementORToTrue(t *testing.T) {
	leaf := Expr{Op: OpEQ, FieldOrdinal: 1, Value: true}
	in := Expr{
		Op:           OpOR,
		FieldOrdinal: NoFieldOrdinal,
		Operands: []Expr{
			leaf,
			{Op: leaf.Op, FieldOrdinal: leaf.FieldOrdinal, Value: leaf.Value, Not: true},
		},
	}

	out, changed := NormalizeExpr(in)
	if !changed {
		t.Fatalf("expected changed=true")
	}
	want := Expr{Op: OpNOOP, FieldOrdinal: NoFieldOrdinal}
	if !reflect.DeepEqual(out, want) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, want)
	}
}

func TestNormalizeExpr_MalformedNoopComplementStaysMalformed(t *testing.T) {
	in := Expr{
		Op:           OpAND,
		FieldOrdinal: NoFieldOrdinal,
		Operands: []Expr{
			{Op: OpNOOP, FieldOrdinal: 1},
			{Op: OpNOOP, FieldOrdinal: 1, Not: true},
		},
	}

	out, changed := NormalizeExpr(in)
	if changed {
		t.Fatalf("expected malformed NOOP pair to stay untouched, got=%+v", out)
	}
	if !reflect.DeepEqual(out, in) {
		t.Fatalf("unexpected normalize result:\n got=%+v\nwant=%+v", out, in)
	}
}
