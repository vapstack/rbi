package rbi

import (
	"fmt"
	"testing"

	"github.com/vapstack/qx"
)

type queryMetamorphicTransform struct {
	name  string
	apply func(*qx.QX) (*qx.QX, bool)
}

type queryMetamorphicCase struct {
	name string
	q    *qx.QX
}

type queryMetamorphicSource struct {
	name  string
	open  func(*testing.T) *DB[uint64, Rec]
	cases func() []queryMetamorphicCase
}

type queryMetamorphicBaseline[K ~uint64 | ~string] struct {
	q        *qx.QX
	exact    bool
	keys     []K
	fullKeys []K
}

func newQueryMetamorphicBaseline[K ~uint64 | ~string](
	c queryContract[K],
	q *qx.QX,
) queryMetamorphicBaseline[K] {
	c.t.Helper()
	c.AssertAllReadPathsMatchReference(q)

	base := queryMetamorphicBaseline[K]{
		q:     cloneQuery(q),
		exact: !queryContractNoOrderWindow(q),
	}
	if base.exact {
		base.keys = c.ReferenceKeys(q)
		return base
	}

	base.fullKeys = c.ReferenceFullKeys(q)
	return base
}

func (c queryContract[K]) AssertMetamorphicEquivalent(
	base queryMetamorphicBaseline[K],
	label string,
	q *qx.QX,
) {
	c.t.Helper()
	c.AssertAllReadPathsMatchReference(q)

	if base.exact {
		got := c.ReferenceKeys(q)
		if !c.equal(base.q, base.keys, got) {
			c.t.Fatalf(
				"%s exact equivalence mismatch:\nbase=%+v\nq=%+v\nbaseKeys=%v\ngot=%v",
				label, base.q, q, base.keys, got,
			)
		}
		return
	}

	got := c.ReferenceFullKeys(q)
	fullQ := cloneQuery(base.q)
	clearQueryOrderWindowForTest(fullQ)
	if !c.equal(fullQ, base.fullKeys, got) {
		c.t.Fatalf(
			"%s full-set equivalence mismatch:\nbase=%+v\nq=%+v\nbaseFull=%v\ngotFull=%v",
			label, base.q, q, base.fullKeys, got,
		)
	}
}

func queryMetamorphicTransforms() []queryMetamorphicTransform {
	return []queryMetamorphicTransform{
		{
			name: "Normalize",
			apply: func(q *qx.QX) (*qx.QX, bool) {
				return normalizeQueryForTest(q), true
			},
		},
		{
			name: "AndTrue",
			apply: func(q *qx.QX) (*qx.QX, bool) {
				return withNoisyEquivalentQuery(q, 0), true
			},
		},
		{
			name: "OrFalse",
			apply: func(q *qx.QX) (*qx.QX, bool) {
				return withNoisyEquivalentQuery(q, 1), true
			},
		},
		{
			name: "DuplicateAND",
			apply: func(q *qx.QX) (*qx.QX, bool) {
				out := cloneQuery(q)
				left := cloneMetamorphicExpr(q.Filter)
				right := cloneMetamorphicExpr(q.Filter)
				out.Filter = qx.AND(left, right)
				return out, true
			},
		},
		{
			name: "DuplicateOR",
			apply: func(q *qx.QX) (*qx.QX, bool) {
				out := cloneQuery(q)
				left := cloneMetamorphicExpr(q.Filter)
				right := cloneMetamorphicExpr(q.Filter)
				out.Filter = qx.OR(left, right)
				return out, true
			},
		},
		{
			name: "DoubleNegation",
			apply: func(q *qx.QX) (*qx.QX, bool) {
				out := cloneQuery(q)
				out.Filter = qx.NOT(qx.NOT(cloneMetamorphicExpr(q.Filter)))
				return out, true
			},
		},
		{
			name: "PermuteAND",
			apply: func(q *qx.QX) (*qx.QX, bool) {
				filter, ok := metamorphicReverseBoolArgs(q.Filter, qx.OpAND)
				if !ok {
					return nil, false
				}
				out := cloneQuery(q)
				out.Filter = filter
				return out, true
			},
		},
		{
			name: "PermuteOR",
			apply: func(q *qx.QX) (*qx.QX, bool) {
				filter, ok := metamorphicReverseBoolArgs(q.Filter, qx.OpOR)
				if !ok {
					return nil, false
				}
				out := cloneQuery(q)
				out.Filter = filter
				return out, true
			},
		},
		{
			name:  "DeMorgan",
			apply: metamorphicApplyDeMorgan,
		},
	}
}

func cloneMetamorphicExpr(expr qx.Expr) qx.Expr {
	out := expr
	if len(expr.Args) == 0 {
		return out
	}

	out.Args = make([]qx.Expr, len(expr.Args))
	for i := range expr.Args {
		out.Args[i] = cloneMetamorphicExpr(expr.Args[i])
	}
	return out
}

func metamorphicReverseBoolArgs(expr qx.Expr, op string) (qx.Expr, bool) {
	out := expr
	if len(expr.Args) == 0 {
		return out, false
	}

	reverse := expr.Kind == qx.KindOP && expr.Name == op && len(expr.Args) > 1
	out.Args = make([]qx.Expr, len(expr.Args))
	changed := reverse
	for i := range out.Args {
		src := i
		if reverse {
			src = len(expr.Args) - 1 - i
		}
		child, childChanged := metamorphicReverseBoolArgs(expr.Args[src], op)
		out.Args[i] = child
		if childChanged {
			changed = true
		}
	}
	return out, changed
}

func metamorphicApplyDeMorgan(q *qx.QX) (*qx.QX, bool) {
	if q == nil {
		return nil, false
	}
	if q.Filter.Kind != qx.KindOP || len(q.Filter.Args) < 2 {
		return nil, false
	}

	var inner qx.Expr
	switch q.Filter.Name {
	case qx.OpAND:
		args := make([]qx.Expr, len(q.Filter.Args))
		for i := range q.Filter.Args {
			args[i] = qx.NOT(cloneMetamorphicExpr(q.Filter.Args[i]))
		}
		inner = qx.OR(args...)
	case qx.OpOR:
		args := make([]qx.Expr, len(q.Filter.Args))
		for i := range q.Filter.Args {
			args[i] = qx.NOT(cloneMetamorphicExpr(q.Filter.Args[i]))
		}
		inner = qx.AND(args...)
	default:
		return nil, false
	}

	out := cloneQuery(q)
	out.Filter = qx.NOT(inner)
	return out, true
}

func queryMetamorphicSmallWorldCases() []queryMetamorphicCase {
	exprs := smallWorldExprCases()
	orders := smallWorldOrderCases()
	windows := smallWorldWindowCases()
	cases := make([]queryMetamorphicCase, 0, len(exprs)*len(orders)*len(windows))
	for _, exprCase := range exprs {
		for _, orderCase := range orders {
			for _, windowCase := range windows {
				cases = append(cases, queryMetamorphicCase{
					name: fmt.Sprintf("%s/%s/%s", exprCase.name, orderCase.name, windowCase.name),
					q:    buildSmallWorldQuery(exprCase, orderCase, windowCase),
				})
			}
		}
	}
	return cases
}

func queryMetamorphicSeededCases() []queryMetamorphicCase {
	return []queryMetamorphicCase{
		{
			name: "OR_Order_Offset",
			q: qx.Query(
				qx.OR(
					qx.AND(
						qx.EQ("active", true),
						qx.IN("country", []string{"NL", "DE", "PL"}),
						qx.GTE("score", 30.0),
					),
					qx.AND(
						qx.PREFIX("full_name", "FN-1"),
						qx.NOTIN("country", []string{"Thailand"}),
						qx.GTE("age", 20),
					),
					qx.AND(
						qx.HASANY("tags", []string{"go", "db"}),
						qx.NE("name", "alice"),
					),
				),
			).Sort("score", qx.DESC).Offset(300).Limit(120),
		},
		{
			name: "AND_NoOrder_Complex",
			q: qx.Query(
				qx.EQ("active", true),
				qx.NOTIN("country", []string{"NL", "PL"}),
				qx.HASANY("tags", []string{"go", "ops"}),
				qx.GTE("age", 22),
			),
		},
		{
			name: "AutocompleteLike",
			q: qx.Query(
				qx.PREFIX("full_name", "FN-1"),
				qx.EQ("active", true),
				qx.NOTIN("country", []string{"NL"}),
			).Sort("score", qx.DESC).Limit(80),
		},
		{
			name: "OrderRange",
			q: qx.Query(
				qx.GTE("age", 25),
				qx.LTE("age", 40),
				qx.GT("score", 20.0),
			).Sort("score", qx.DESC).Offset(100).Limit(90),
		},
	}
}

func queryMetamorphicSkewedNotInCases() []queryMetamorphicCase {
	return []queryMetamorphicCase{
		{
			name: "CapturedShape",
			q:    capturedNotInOrderOffsetQuery(),
		},
		{
			name: "DifferentValues",
			q: qx.Query(
				qx.NOTIN("country", []string{"DE", "PL"}),
				qx.NOTIN("country", []string{"Thailand", "US"}),
			).Sort("score", qx.ASC).Offset(210).Limit(90),
		},
		{
			name: "DescOrder",
			q: qx.Query(
				qx.NOTIN("country", []string{"Iceland", "Finland"}),
				qx.NOTIN("country", []string{"Iceland", "DE"}),
			).Sort("score", qx.DESC).Offset(446).Limit(70),
		},
		{
			name: "WithoutOffset",
			q: qx.Query(
				qx.NOTIN("country", []string{"Iceland", "Finland"}),
				qx.NOTIN("country", []string{"Iceland", "DE"}),
			).Sort("score", qx.ASC).Limit(70),
		},
	}
}

func queryMetamorphicUniformProfileCases() []queryMetamorphicCase {
	return []queryMetamorphicCase{
		{
			name: "NoOrderLimit",
			q: qx.Query(
				qx.HASALL("tags", []string{"db", "rust"}),
				qx.HASALL("tags", []string{"db", "infra"}),
				qx.HASANY("tags", []string{"infra", "rust"}),
			).Limit(120),
		},
		{
			name: "NoOrderOffsetLimit",
			q: qx.Query(
				qx.HASALL("tags", []string{"db", "rust"}),
				qx.HASALL("tags", []string{"db", "infra"}),
				qx.HASANY("tags", []string{"infra", "rust"}),
			).Offset(40).Limit(80),
		},
		{
			name: "OrderedOffsetLimit",
			q: qx.Query(
				qx.HASALL("tags", []string{"db", "rust"}),
				qx.HASALL("tags", []string{"db", "infra"}),
				qx.HASANY("tags", []string{"infra", "rust"}),
			).Sort("age", qx.ASC).Offset(20).Limit(90),
		},
	}
}

func queryMetamorphicCuratedSources() []queryMetamorphicSource {
	return []queryMetamorphicSource{
		{
			name: "Seeded",
			open: func(t *testing.T) *DB[uint64, Rec] {
				db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
				_ = seedData(t, db, 8_000)
				return db
			},
			cases: queryMetamorphicSeededCases,
		},
		{
			name:  "SkewedNotIn",
			open:  openSkewedNotInRegressionDB,
			cases: queryMetamorphicSkewedNotInCases,
		},
		{
			name: "UniformProfile",
			open: func(t *testing.T) *DB[uint64, Rec] {
				db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
				seedMetamorphicDataProfile(t, db, 8_000, metamorphicDataProfile{
					name:        "Uniform",
					scoreLevels: 50_000,
					activeTrue:  0.50,
					hotCountryP: 0.15,
					hotTagP:     0.30,
				})
				return db
			},
			cases: queryMetamorphicUniformProfileCases,
		},
	}
}

func runQueryMetamorphicTransforms(
	t *testing.T,
	db *DB[uint64, Rec],
	tc queryMetamorphicCase,
) {
	t.Helper()
	baseQ := cloneQuery(tc.q)
	contract := newUint64QueryContract(t, db)
	baseline := newQueryMetamorphicBaseline(contract, baseQ)
	for _, transform := range queryMetamorphicTransforms() {
		derived, ok := transform.apply(baseQ)
		if !ok {
			continue
		}
		contract.AssertMetamorphicEquivalent(
			baseline,
			fmt.Sprintf("%s/%s", tc.name, transform.name),
			derived,
		)
	}
}

func runQueryMetamorphicExecutionRounds(
	t *testing.T,
	db *DB[uint64, Rec],
	tc queryMetamorphicCase,
) {
	t.Helper()
	baseQ := cloneQuery(tc.q)
	contract := newUint64QueryContract(t, db)
	baseline := newQueryMetamorphicBaseline(contract, baseQ)

	contract.AssertMetamorphicEquivalent(baseline, tc.name+"/WarmCacheRoundTrip", baseQ)

	if err := db.RefreshPlannerStats(); err != nil {
		t.Fatalf("RefreshPlannerStats(%s): %v", tc.name, err)
	}
	contract.AssertMetamorphicEquivalent(baseline, tc.name+"/RefreshPlannerStatsRoundTrip", baseQ)
}

func TestQueryMetamorphic_SmallWorldTransforms(t *testing.T) {
	for _, world := range smallWorldCases() {
		t.Run(world.name, func(t *testing.T) {
			db := openSmallWorldDB(t, world)
			for _, tc := range queryMetamorphicSmallWorldCases() {
				t.Run(tc.name, func(t *testing.T) {
					runQueryMetamorphicTransforms(t, db, tc)
				})
			}
		})
	}
}

func TestQueryMetamorphic_CuratedCorpusTransforms(t *testing.T) {
	for _, source := range queryMetamorphicCuratedSources() {
		t.Run(source.name, func(t *testing.T) {
			db := source.open(t)
			for _, tc := range source.cases() {
				t.Run(tc.name, func(t *testing.T) {
					runQueryMetamorphicTransforms(t, db, tc)
				})
			}
		})
	}
}

func TestQueryMetamorphic_CuratedExecutionRounds(t *testing.T) {
	for _, source := range queryMetamorphicCuratedSources() {
		t.Run(source.name, func(t *testing.T) {
			db := source.open(t)
			for _, tc := range source.cases() {
				t.Run(tc.name, func(t *testing.T) {
					runQueryMetamorphicExecutionRounds(t, db, tc)
				})
			}
		})
	}
}
