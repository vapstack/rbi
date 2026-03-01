package rbi

import (
	"fmt"
	"testing"

	"github.com/vapstack/qx"
)

func countByExprBitmap(t *testing.T, db *DB[uint64, Rec], expr qx.Expr) uint64 {
	t.Helper()
	b, err := db.evalExpr(expr)
	if err != nil {
		t.Fatalf("evalExpr: %v", err)
	}
	defer b.release()
	return db.countBitmapResult(b)
}

func TestCount_ByPredicates_BucketLead_MatchesBitmap(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options{AnalyzeInterval: -1})

	for i := 1; i <= 20_000; i++ {
		err := db.Set(uint64(i), &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%05d@example.com", i),
			Age:    i,
			Score:  float64(i % 1_000),
			Active: i%2 == 0,
			Meta: Meta{Country: func() string {
				if i%3 == 0 {
					return "NL"
				}
				return "US"
			}()},
		})
		if err != nil {
			t.Fatalf("seed Set(%d): %v", i, err)
		}
	}

	q := qx.Query(
		qx.GTE("age", 2_500),
		qx.LT("age", 15_000),
		qx.NOT(qx.EQ("active", false)),
	)

	got, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	want := countByExprBitmap(t, db, q.Expr)
	if got != want {
		t.Fatalf("count mismatch: got=%d want=%d", got, want)
	}
}

func TestCount_ORPredicates_FiveBranches_MatchesBitmap(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options{AnalyzeInterval: -1})

	for i := 1; i <= 25_000; i++ {
		err := db.Set(uint64(i), &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%05d@example.com", i),
			Age:    i,
			Score:  float64(i % 10_000),
			Active: i%2 == 0,
			Meta: Meta{Country: func() string {
				if i%3 == 0 {
					return "NL"
				}
				return "US"
			}()},
		})
		if err != nil {
			t.Fatalf("seed Set(%d): %v", i, err)
		}
	}

	expr := qx.OR(
		qx.AND(qx.PREFIX("email", "user10"), qx.EQ("active", true)),
		qx.AND(qx.PREFIX("email", "user11"), qx.EQ("country", "NL")),
		qx.AND(qx.PREFIX("email", "user12"), qx.EQ("active", false)),
		qx.AND(qx.PREFIX("email", "user13"), qx.EQ("country", "US")),
		qx.AND(qx.PREFIX("email", "user14"), qx.GTE("age", 14_000)),
	)
	q := qx.Query(expr)

	got, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	want := countByExprBitmap(t, db, expr)
	if got != want {
		t.Fatalf("count mismatch: got=%d want=%d", got, want)
	}
}

func TestCountORBranches_ShouldUseSeenDedup_Adaptive(t *testing.T) {
	highOverlap := countORBranches{
		{est: 90_000},
		{est: 85_000},
		{est: 80_000},
		{est: 75_000},
		{est: 70_000},
	}
	if !highOverlap.shouldUseSeenDedup(100_000, 250_000) {
		t.Fatalf("expected seen dedup for high-overlap wide OR")
	}

	lowOverlap := countORBranches{
		{est: 25_000},
		{est: 25_000},
		{est: 25_000},
		{est: 25_000},
	}
	if lowOverlap.shouldUseSeenDedup(2_000_000, 120_000) {
		t.Fatalf("unexpected seen dedup for low-overlap OR")
	}
}

func TestCountORDedupThresholds_Adaptive(t *testing.T) {
	loProbe := countORSeenUnionThreshold(2_000_000, 4, 200_000)
	hiProbe := countORSeenUnionThreshold(2_000_000, 4, 2_000_000)
	if hiProbe >= loProbe {
		t.Fatalf("expected lower union threshold for high probe share: low_probe=%d high_probe=%d", loProbe, hiProbe)
	}

	minShareLo, forceShareLo := countORDedupDupShareBounds(4, 2_000_000, 200_000)
	minShareHi, forceShareHi := countORDedupDupShareBounds(6, 2_000_000, 2_000_000)
	if minShareHi >= minShareLo {
		t.Fatalf("expected looser min dup share for wider/high-probe OR: low=%.3f high=%.3f", minShareLo, minShareHi)
	}
	if forceShareHi >= forceShareLo {
		t.Fatalf("expected looser force dup share for wider/high-probe OR: low=%.3f high=%.3f", forceShareLo, forceShareHi)
	}
}

func TestCountORPredicateBranchLimit_Adaptive(t *testing.T) {
	if got := countORPredicateBranchLimit(0); got != countORPredicateMaxBranchesBase {
		t.Fatalf("expected base branch limit for unknown universe, got=%d", got)
	}
	if got := countORPredicateBranchLimit(120_000); got >= countORPredicateMaxBranchesBase {
		t.Fatalf("expected stricter branch limit for small universe, got=%d", got)
	}
	if got := countORPredicateBranchLimit(5_000_000); got <= countORPredicateMaxBranchesBase {
		t.Fatalf("expected wider branch limit for large universe, got=%d", got)
	}
}

func TestCountPredicateMaterializationThresholds_Adaptive(t *testing.T) {
	setPred := predicate{
		kind:  predicateKindPostsAny,
		posts: make([]postingList, 12),
	}
	if shouldMaterializeCountSetPredicate(setPred, 500, 500_000) {
		t.Fatalf("set predicate should not materialize on low probe estimate")
	}
	if !shouldMaterializeCountSetPredicate(setPred, 5_000, 500_000) {
		t.Fatalf("set predicate should materialize on high probe estimate")
	}

	customPred := predicate{
		kind:    predicateKindCustom,
		expr:    qx.Expr{Op: qx.OpPREFIX, Field: "email"},
		estCard: 200_000,
	}
	if shouldMaterializeCustomCountPredicate(customPred, 2_000, 500_000) {
		t.Fatalf("custom predicate should not materialize below adaptive threshold")
	}
	if !shouldMaterializeCustomCountPredicate(customPred, 3_000, 500_000) {
		t.Fatalf("custom predicate should materialize above adaptive threshold")
	}

	if got := countSetMaterializeMinTerms(3_000, 32_000); got <= countPredSetMaterializeMinTermsBase {
		t.Fatalf("expected stricter set-term threshold on small universe, got=%d", got)
	}
	if got := countSetMaterializeMinTerms(8_000, 5_000_000); got >= countPredSetMaterializeMinTermsBase {
		t.Fatalf("expected looser set-term threshold on large universe, got=%d", got)
	}
}

func TestCount_ScalarINSplit_MatchesBitmap(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options{AnalyzeInterval: -1})

	countries := []string{"US", "DE", "FR", "IN", "NL"}
	for i := 1; i <= 30_000; i++ {
		err := db.Set(uint64(i), &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%05d@example.com", i),
			Age:    i % 20_000,
			Score:  float64(i % 2_000),
			Active: i%3 != 0,
			Meta: Meta{
				Country: countries[i%len(countries)],
			},
		})
		if err != nil {
			t.Fatalf("seed Set(%d): %v", i, err)
		}
	}

	expr := qx.AND(
		qx.GTE("age", 4_000),
		qx.NOTIN("active", []bool{false}),
		qx.IN("country", []string{"US", "DE", "FR", "IN"}),
	)
	q := qx.Query(expr)

	got, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	want := countByExprBitmap(t, db, expr)
	if got != want {
		t.Fatalf("count mismatch: got=%d want=%d", got, want)
	}
}

func TestCount_ScalarINSplit_WorksWithFieldDelta(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options{
		SnapshotCompactorRequestEveryNWrites: 1 << 30,
		SnapshotCompactorIdleInterval:        -1,
		SnapshotDeltaLayerMaxDepth:           1 << 30,
		AnalyzeInterval:                      -1,
	})

	countries := []string{"US", "DE", "FR", "IN", "NL"}
	for i := 1; i <= 12_000; i++ {
		err := db.Set(uint64(i), &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("user%05d@example.com", i),
			Age:    i % 10_000,
			Score:  float64(i % 2_000),
			Active: i%2 == 0,
			Meta: Meta{
				Country: countries[i%len(countries)],
			},
		})
		if err != nil {
			t.Fatalf("seed Set(%d): %v", i, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	for i := 1; i <= 512; i++ {
		c := countries[(i+2)%len(countries)]
		err := db.Patch(uint64(i), []Field{
			{Name: "country", Value: c},
			{Name: "active", Value: i%3 != 0},
			{Name: "age", Value: 8_000 + i},
		})
		if err != nil {
			t.Fatalf("Patch(%d): %v", i, err)
		}
	}

	if db.getSnapshot().fieldDelta("country") == nil {
		t.Fatalf("expected active country delta")
	}

	expr := qx.AND(
		qx.GTE("age", 7_500),
		qx.NOTIN("active", []bool{false}),
		qx.IN("country", []string{"US", "DE", "FR", "IN"}),
	)
	q := qx.Query(expr)

	got, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	want := countByExprBitmap(t, db, expr)
	if got != want {
		t.Fatalf("count mismatch with delta: got=%d want=%d", got, want)
	}
}
