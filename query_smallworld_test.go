package rbi

import (
	"testing"

	"github.com/vapstack/qx"
)

type smallWorldRow struct {
	id  uint64
	rec *Rec
}

type smallWorldCase struct {
	name string
	rows []smallWorldRow
}

type smallWorldExprCase struct {
	name     string
	noFilter bool
	expr     qx.Expr
}

type smallWorldOrderCase struct {
	name  string
	apply func(*qx.QX)
}

type smallWorldWindowCase struct {
	name  string
	apply func(*qx.QX)
}

func smallWorldCases() []smallWorldCase {
	return []smallWorldCase{
		{
			name: "Empty",
		},
		{
			name: "SingleNil",
			rows: []smallWorldRow{
				{
					id: 1,
					rec: &Rec{
						Meta:     Meta{Country: ""},
						Name:     "solo",
						Email:    "solo@example.test",
						Age:      0,
						Score:    0,
						Active:   false,
						Tags:     nil,
						FullName: "FN-000",
						Opt:      nil,
					},
				},
			},
		},
		{
			name: "Overlap",
			rows: []smallWorldRow{
				{
					id: 1,
					rec: &Rec{
						Meta:     Meta{Country: "NL"},
						Name:     "alice",
						Email:    "alice-1@example.test",
						Age:      20,
						Score:    10,
						Active:   true,
						Tags:     []string{"go", "db"},
						FullName: "FN-001",
					},
				},
				{
					id: 2,
					rec: &Rec{
						Meta:     Meta{Country: "DE"},
						Name:     "bob",
						Email:    "bob@example.test",
						Age:      30,
						Score:    20,
						Active:   false,
						Tags:     []string{"go"},
						FullName: "FN-002",
						Opt:      strPtr("opt-a"),
					},
				},
				{
					id: 3,
					rec: &Rec{
						Meta:     Meta{Country: "NL"},
						Name:     "carol",
						Email:    "carol@example.test",
						Age:      30,
						Score:    20,
						Active:   true,
						Tags:     []string{"db", "ops"},
						FullName: "FN-003",
						Opt:      strPtr(""),
					},
				},
				{
					id: 4,
					rec: &Rec{
						Meta:     Meta{Country: "PL"},
						Name:     "dave",
						Email:    "dave@example.test",
						Age:      40,
						Score:    20,
						Active:   false,
						Tags:     nil,
						FullName: "ZZ-004",
						Opt:      strPtr("opt-b"),
					},
				},
				{
					id: 5,
					rec: &Rec{
						Meta:     Meta{Country: "Finland"},
						Name:     "eve",
						Email:    "eve@example.test",
						Age:      25,
						Score:    15,
						Active:   true,
						Tags:     []string{"go", "go", "db"},
						FullName: "FN-005",
					},
				},
				{
					id: 6,
					rec: &Rec{
						Meta:     Meta{Country: "Thailand"},
						Name:     "alice",
						Email:    "alice-2@example.test",
						Age:      18,
						Score:    30,
						Active:   false,
						Tags:     []string{},
						FullName: "AA-006",
						Opt:      strPtr("opt-a"),
					},
				},
			},
		},
		{
			name: "TiesAndDuplicates",
			rows: []smallWorldRow{
				{
					id: 10,
					rec: &Rec{
						Meta:     Meta{Country: "NL"},
						Name:     "tie",
						Email:    "tie-1@example.test",
						Age:      10,
						Score:    1,
						Active:   true,
						Tags:     []string{"go"},
						FullName: "FN-tie",
					},
				},
				{
					id: 11,
					rec: &Rec{
						Meta:     Meta{Country: "NL"},
						Name:     "tie",
						Email:    "tie-2@example.test",
						Age:      10,
						Score:    1,
						Active:   true,
						Tags:     []string{"go", "db"},
						FullName: "FN-tie",
						Opt:      strPtr("opt-a"),
					},
				},
				{
					id: 12,
					rec: &Rec{
						Meta:     Meta{Country: "NL"},
						Name:     "tie",
						Email:    "tie-3@example.test",
						Age:      10,
						Score:    1,
						Active:   false,
						Tags:     []string{"db"},
						FullName: "FN-tie",
					},
				},
				{
					id: 20,
					rec: &Rec{
						Meta:     Meta{Country: "Iceland"},
						Name:     "cold",
						Email:    "cold@example.test",
						Age:      99,
						Score:    100,
						Active:   false,
						Tags:     []string{"ops"},
						FullName: "ZZ-cold",
						Opt:      strPtr("opt-z"),
					},
				},
			},
		},
	}
}

func smallWorldExprCases() []smallWorldExprCase {
	return []smallWorldExprCase{
		{name: "NoFilter", noFilter: true},
		{name: "ActiveTrue", expr: qx.EQ("active", true)},
		{name: "ActiveFalse", expr: qx.EQ("active", false)},
		{name: "NotActiveTrue", expr: qx.NOT(qx.EQ("active", true))},
		{name: "CountryEqNL", expr: qx.EQ("country", "NL")},
		{name: "CountryIN", expr: qx.IN("country", []string{"NL", "DE"})},
		{name: "CountryNOTIN", expr: qx.NOTIN("country", []string{"Thailand", "Iceland"})},
		{name: "CountrySuffixLand", expr: qx.SUFFIX("country", "land")},
		{name: "CountryContainsLand", expr: qx.CONTAINS("country", "land")},
		{name: "NameEqAlice", expr: qx.EQ("name", "alice")},
		{name: "NameNotBob", expr: qx.NE("name", "bob")},
		{name: "AgeGTE30", expr: qx.GTE("age", 30)},
		{name: "AgeRange", expr: qx.AND(qx.GTE("age", 20), qx.LTE("age", 30))},
		{name: "ScoreGT15", expr: qx.GT("score", 15.0)},
		{name: "ScoreLTE20", expr: qx.LTE("score", 20.0)},
		{name: "FullNamePrefixFN", expr: qx.PREFIX("full_name", "FN-")},
		{name: "OptEqNil", expr: qx.EQ("opt", nil)},
		{name: "OptEqValue", expr: qx.EQ("opt", "opt-a")},
		{name: "OptPrefix", expr: qx.PREFIX("opt", "opt-")},
		{name: "TagsHasGo", expr: qx.HASALL("tags", []string{"go"})},
		{name: "TagsHasAny", expr: qx.HASANY("tags", []string{"db", "ops"})},
		{name: "TagsHasAll", expr: qx.HASALL("tags", []string{"go", "db"})},
		{name: "TagsHasNone", expr: qx.HASNONE("tags", []string{"go", "ops"})},
		{name: "AndSelective", expr: qx.AND(qx.EQ("active", true), qx.EQ("country", "NL"))},
		{name: "AndRange", expr: qx.AND(qx.GTE("age", 20), qx.LTE("age", 30), qx.LTE("score", 20.0))},
		{name: "OrOverlap", expr: qx.OR(qx.EQ("country", "NL"), qx.HASALL("tags", []string{"go"}))},
		{
			name: "OrResidualBranches",
			expr: qx.OR(
				qx.AND(qx.EQ("active", true), qx.HASALL("tags", []string{"db"})),
				qx.AND(qx.EQ("active", false), qx.GTE("score", 20.0)),
			),
		},
		{name: "OrDuplicateBranch", expr: qx.OR(qx.EQ("country", "NL"), qx.EQ("country", "NL"))},
		{name: "NotGroup", expr: qx.NOT(qx.OR(qx.EQ("country", "Thailand"), qx.EQ("active", false)))},
	}
}

func smallWorldOrderCases() []smallWorldOrderCase {
	return []smallWorldOrderCase{
		{name: "NoOrder"},
		{name: "AgeAsc", apply: func(q *qx.QX) { q.Sort("age", qx.ASC) }},
		{name: "ScoreDesc", apply: func(q *qx.QX) { q.Sort("score", qx.DESC) }},
		{name: "FullNameAsc", apply: func(q *qx.QX) { q.Sort("full_name", qx.ASC) }},
		{name: "OptAsc", apply: func(q *qx.QX) { q.Sort("opt", qx.ASC) }},
		{name: "TagsPosAsc", apply: func(q *qx.QX) { queryOrderSortByArrayPos(q, "tags", []string{"go", "db", "ops"}, qx.ASC) }},
		{name: "TagsCountDesc", apply: func(q *qx.QX) { queryOrderSortByArrayCount(q, "tags", qx.DESC) }},
	}
}

func smallWorldWindowCases() []smallWorldWindowCase {
	return []smallWorldWindowCase{
		{name: "Full"},
		{name: "Limit1", apply: func(q *qx.QX) { q.Limit(1) }},
		{name: "Offset1Limit2", apply: func(q *qx.QX) { q.Offset(1).Limit(2) }},
		{name: "OffsetPastLimit3", apply: func(q *qx.QX) { q.Offset(10).Limit(3) }},
	}
}

func openSmallWorldDB(t *testing.T, world smallWorldCase) *DB[uint64, Rec] {
	t.Helper()
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	for _, row := range world.rows {
		if err := db.Set(row.id, row.rec); err != nil {
			t.Fatalf("Set(%d): %v", row.id, err)
		}
	}
	if err := db.RefreshPlannerStats(); err != nil {
		t.Fatalf("RefreshPlannerStats: %v", err)
	}
	return db
}

func buildSmallWorldQuery(exprCase smallWorldExprCase, orderCase smallWorldOrderCase, windowCase smallWorldWindowCase) *qx.QX {
	var q *qx.QX
	if exprCase.noFilter {
		q = qx.Query()
	} else {
		q = qx.Query(exprCase.expr)
	}
	if orderCase.apply != nil {
		orderCase.apply(q)
	}
	if windowCase.apply != nil {
		windowCase.apply(q)
	}
	return q
}

func TestQuerySmallWorld_BoundedExhaustiveContract(t *testing.T) {
	exprs := smallWorldExprCases()
	orders := smallWorldOrderCases()
	windows := smallWorldWindowCases()

	for _, world := range smallWorldCases() {
		t.Run(world.name, func(t *testing.T) {
			db := openSmallWorldDB(t, world)

			for _, exprCase := range exprs {
				t.Run(exprCase.name, func(t *testing.T) {
					for _, orderCase := range orders {
						for _, windowCase := range windows {
							q := buildSmallWorldQuery(exprCase, orderCase, windowCase)
							contract := newUint64QueryContract(t, db)
							contract.AssertAllReadPathsMatchReference(q)
						}
					}
				})
			}
		})
	}
}
