package qir

import (
	"testing"

	"github.com/vapstack/qx"
)

type prepareBenchFieldResolver map[string]int

func (r prepareBenchFieldResolver) ResolveField(name string) (int, bool) {
	ordinal, ok := r[name]
	return ordinal, ok
}

var prepareBenchFieldOrdinals = prepareBenchFieldResolver{
	"country": 0,
	"plan":    1,
	"status":  2,
	"age":     3,
	"score":   4,
	"name":    5,
	"email":   6,
	"tags":    7,
	"roles":   8,
}

func benchmarkPrepareCountExpr(b *testing.B, expr qx.Expr) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		prepared, err := PrepareCountExprResolved(prepareBenchFieldOrdinals, expr)
		if err != nil {
			b.Fatal(err)
		}
		prepared.Release()
	}
}

func benchmarkPrepareQuery(b *testing.B, q *qx.QX) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		prepared, err := PrepareQueryResolved(q, prepareBenchFieldOrdinals)
		if err != nil {
			b.Fatal(err)
		}
		prepared.Release()
	}
}

func BenchmarkPrepareCountExpr(b *testing.B) {
	b.Run("Simple_EQ", func(b *testing.B) {
		benchmarkPrepareCountExpr(b, qx.EQ("country", "NL"))
	})

	b.Run("Simple_IN", func(b *testing.B) {
		benchmarkPrepareCountExpr(b, qx.IN("country", []string{"NL", "DE", "PL"}))
	})

	b.Run("Simple_NOTIN", func(b *testing.B) {
		benchmarkPrepareCountExpr(b, qx.NOTIN("status", []string{"banned"}))
	})

	b.Run("Realistic_FeedEligible", func(b *testing.B) {
		benchmarkPrepareCountExpr(
			b,
			qx.AND(
				qx.EQ("status", "active"),
				qx.NOTIN("plan", []string{"free"}),
				qx.GTE("score", 120.0),
				qx.HASANY("tags", []string{"go", "security", "ops"}),
			),
		)
	})
}

func BenchmarkPrepareQuery(b *testing.B) {
	b.Run("Read_GT_NoMatch", func(b *testing.B) {
		benchmarkPrepareQuery(b, qx.Query(qx.GT("age", 100)))
	})

	b.Run("Keys_ComplexSegment_Limit", func(b *testing.B) {
		benchmarkPrepareQuery(
			b,
			qx.Query(
				qx.EQ("status", "active"),
				qx.IN("country", []string{"US", "DE", "NL", "PL"}),
				qx.NE("plan", "free"),
				qx.HASANY("tags", []string{"go", "ops"}),
				qx.GTE("age", 22),
			).Limit(120),
		)
	})

	b.Run("Analytics_Range_Order_Limit", func(b *testing.B) {
		benchmarkPrepareQuery(
			b,
			qx.Query(
				qx.GTE("age", 25),
				qx.LTE("age", 40),
				qx.GT("score", 0.5),
			).Sort("score", qx.DESC).Limit(100),
		)
	})
}
