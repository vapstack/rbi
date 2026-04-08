package rbi

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/posting"
)

var plannerExtSeeded struct {
	once sync.Once
	path string
	err  error
}

var (
	plannerExtCountries = []string{"NL", "PL", "DE", "Finland", "Iceland", "Thailand", "Switzerland"}
	plannerExtNames     = []string{"alice", "albert", "bob", "bobby", "carol", "dave", "eve"}
	plannerExtTags      = []string{"go", "db", "java", "rust", "ops"}
	plannerExtPrefixes  = []string{"FN-", "FN-0", "FN-1", "FN-2", "FN-3", "FN-7", "FN-9", "FN-10", "FN-99"}
)

const plannerExtPropertyCases = 128

func plannerExtSeedPath(t *testing.T) string {
	t.Helper()

	plannerExtSeeded.once.Do(func() {
		dir, err := os.MkdirTemp("", "rbi-planner-ext-")
		if err != nil {
			plannerExtSeeded.err = err
			return
		}

		path := filepath.Join(dir, "seed.db")
		db, raw := openBoltAndNew[uint64, Rec](t, path, Options{AnalyzeInterval: -1})
		_ = seedData(t, db, 8_000)

		if err = db.Close(); err != nil {
			plannerExtSeeded.err = err
			_ = raw.Close()
			return
		}
		if err = raw.Close(); err != nil {
			plannerExtSeeded.err = err
			return
		}
		plannerExtSeeded.path = path
	})

	if plannerExtSeeded.err != nil {
		t.Fatalf("planner ext seeded fixture: %v", plannerExtSeeded.err)
	}
	return plannerExtSeeded.path
}

func plannerExtCopyDB(t *testing.T, src string) string {
	t.Helper()

	return copySeededDBWithSidecars(t, src, "planner_ext.db")
}

func plannerExtOpenSeededDB(t *testing.T, opts Options) *DB[uint64, Rec] {
	t.Helper()

	path := plannerExtCopyDB(t, plannerExtSeedPath(t))
	db, raw := openBoltAndNew[uint64, Rec](t, path, opts)
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	return db
}

func plannerExtQuery005() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.EQ("country", "PL"),
			qx.IN("country", []string{"Iceland", "PL"}),
			qx.LT("score", 65.0),
		).By("age", qx.ASC).Max(10),
	)
}

func plannerExtQuery248() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.LT("score", 65.0),
			qx.LT("score", 55.0),
		).By("age", qx.DESC).Skip(10).Max(40),
	)
}

func plannerExtQuery296() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.OR(
				qx.AND(
					qx.IN("country", []string{"Finland", "Thailand"}),
					qx.IN("country", []string{"DE", "Thailand"}),
				),
				qx.AND(
					qx.IN("country", []string{"DE", "Thailand"}),
					qx.LT("score", 65.0),
				),
			),
		).By("age", qx.ASC).Max(25),
	)
}

func plannerExtQuery561() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.OR(
				qx.EQ("country", "NL"),
				qx.AND(
					qx.LT("score", 35.0),
					qx.LT("score", 65.0),
				),
			),
		).By("age", qx.DESC).Skip(20).Max(5),
	)
}

func plannerExtQuery570() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.OR(
				qx.AND(
					qx.IN("country", []string{"Finland", "Thailand"}),
					qx.LT("score", 15.0),
				),
				qx.AND(
					qx.LT("score", 15.0),
					qx.EQ("active", true),
				),
			),
		).Max(80),
	)
}

func plannerExtQuery584() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.LT("score", 65.0),
			qx.EQ("country", "PL"),
			qx.NOTIN("name", []string{"bob", "dave"}),
		).By("full_name", qx.ASC).Max(40),
	)
}

func plannerExtQuery651() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.OR(
				qx.AND(
					qx.LT("score", 45.0),
					qx.LT("score", 55.0),
				),
				qx.AND(
					qx.LT("score", 65.0),
					qx.EQ("active", false),
				),
			),
		).By("age", qx.ASC).Skip(5).Max(5),
	)
}

func plannerExtQuery652() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.GTE("score", 45.0),
			qx.NOTIN("name", []string{"alice", "eve"}),
			qx.EQ("country", "PL"),
			qx.LT("score", 55.0),
		).By("full_name", qx.ASC).Skip(50).Max(15),
	)
}

func plannerExtQuery717() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.HASANY("tags", []string{"db", "rust"}),
			qx.LT("score", 65.0),
		).By("age", qx.DESC).Max(10),
	)
}

func plannerExtQuery1145() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.HASANY("tags", []string{"db", "ops"}),
			qx.LT("score", 65.0),
		).By("age", qx.ASC).Max(40),
	)
}

func plannerExtQuery1216() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.GTE("age", 45),
			qx.GTE("age", 35),
			qx.LT("score", 55.0),
			qx.GTE("score", 25.0),
		).Skip(5).Max(5),
	)
}

func plannerExtQuery1269() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.HASANY("tags", []string{"go", "go"}),
			qx.LT("score", 65.0),
			qx.GTE("age", 22),
		).By("age", qx.DESC).Skip(3).Max(40),
	)
}

func plannerExtQuery1283() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.OR(
				qx.AND(
					qx.LT("score", 65.0),
					qx.EQ("name", "alice"),
				),
				qx.AND(
					qx.LT("score", 65.0),
					qx.EQ("name", "alice"),
				),
			),
		).By("age", qx.DESC).Max(80),
	)
}

func plannerExtQuery1363() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.LT("score", 55.0),
			qx.LT("score", 45.0),
		).By("full_name", qx.DESC).Max(15),
	)
}

func plannerExtQuery1383() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.OR(
				qx.AND(
					qx.IN("country", []string{"DE", "Thailand"}),
					qx.IN("country", []string{"Finland", "DE"}),
				),
				qx.AND(
					qx.IN("country", []string{"Finland", "DE"}),
					qx.LT("score", 65.0),
				),
			),
		).By("age", qx.ASC).Max(5),
	)
}

func plannerExtQuery1386() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.OR(
				qx.AND(
					qx.EQ("country", "NL"),
					qx.GTE("age", 22),
				),
				qx.AND(
					qx.GTE("age", 22),
					qx.LT("score", 55.0),
				),
			),
		).By("age", qx.ASC).Max(10),
	)
}

func plannerExtQuery1473() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.OR(
				qx.AND(
					qx.LT("score", 55.0),
					qx.EQ("active", false),
				),
				qx.AND(
					qx.LT("score", 75.0),
					qx.GTE("score", 55.0),
				),
			),
		).By("age", qx.ASC).Max(40),
	)
}

func plannerExtQueryAdversarialOrderedNegativeOnlyBranch() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.OR(
				qx.NOTIN("country", []string{"NL", "DE"}),
				qx.AND(
					qx.EQ("name", "alice"),
					qx.LT("score", 55.0),
				),
			),
		).By("age", qx.ASC).Skip(40).Max(35),
	)
}

func plannerExtQueryAdversarialOrderedNegativeResidualOverlap() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.OR(
				qx.AND(
					qx.HASANY("tags", []string{"go", "db", "go"}),
					qx.LT("score", 65.0),
					qx.NOTIN("country", []string{"NL", "DE"}),
				),
				qx.AND(
					qx.EQ("country", "PL"),
					qx.GTE("age", 22),
					qx.NOTIN("name", []string{"alice", "bob"}),
				),
				qx.AND(
					qx.EQ("country", "PL"),
					qx.GTE("age", 22),
					qx.LT("score", 65.0),
				),
			),
		).By("full_name", qx.DESC).Skip(10).Max(50),
	)
}

func plannerExtQueryAdversarialNoOrderNegativeResidualOverlap() *qx.QX {
	return normalizeQueryForTest(
		qx.Query(
			qx.OR(
				qx.AND(
					qx.LT("score", 55.0),
					qx.NOTIN("country", []string{"NL", "DE"}),
					qx.NOTIN("name", []string{"alice"}),
				),
				qx.AND(
					qx.HASANY("tags", []string{"go", "ops", "go"}),
					qx.GTE("age", 22),
					qx.LT("score", 65.0),
				),
				qx.AND(
					qx.EQ("active", true),
					qx.GTE("age", 22),
					qx.NOTIN("name", []string{"eve"}),
				),
			),
		).Max(120),
	)
}

func plannerExtRandomStrings(r *rand.Rand, pool []string, maxN int, allowDup bool) []string {
	n := 1 + r.IntN(maxN)
	out := make([]string, 0, n)
	if allowDup {
		for len(out) < n {
			out = append(out, pool[r.IntN(len(pool))])
		}
		return out
	}

	used := make(map[string]struct{}, n)
	for len(out) < n {
		v := pool[r.IntN(len(pool))]
		if _, ok := used[v]; ok {
			continue
		}
		used[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func plannerExtRandomPositiveLeaf(r *rand.Rand, orderField string) qx.Expr {
	switch r.IntN(10) {
	case 0:
		return qx.EQ("country", plannerExtCountries[r.IntN(len(plannerExtCountries))])
	case 1:
		return qx.IN("country", plannerExtRandomStrings(r, plannerExtCountries, 3, r.IntN(4) == 0))
	case 2:
		return qx.EQ("name", plannerExtNames[r.IntN(len(plannerExtNames))])
	case 3:
		return qx.EQ("active", r.IntN(2) == 0)
	case 4:
		if r.IntN(2) == 0 {
			return qx.GTE("age", 18+r.IntN(45))
		}
		return qx.LT("age", 20+r.IntN(48))
	case 5:
		if r.IntN(2) == 0 {
			return qx.GTE("score", float64(5*r.IntN(18)))
		}
		return qx.LT("score", float64(5+5*r.IntN(19)))
	case 6:
		return qx.HASANY("tags", plannerExtRandomStrings(r, plannerExtTags, 3, r.IntN(3) == 0))
	case 7:
		return qx.PREFIX("full_name", plannerExtPrefixes[r.IntN(len(plannerExtPrefixes))])
	case 8:
		switch orderField {
		case "age":
			if r.IntN(2) == 0 {
				return qx.GTE("age", 18+r.IntN(45))
			}
			return qx.LT("age", 20+r.IntN(48))
		case "score":
			if r.IntN(2) == 0 {
				return qx.GTE("score", float64(5*r.IntN(18)))
			}
			return qx.LT("score", float64(5+5*r.IntN(19)))
		case "full_name":
			if r.IntN(2) == 0 {
				return qx.PREFIX("full_name", plannerExtPrefixes[r.IntN(len(plannerExtPrefixes))])
			}
			return qx.EQ("full_name", fmt.Sprintf("FN-%02d", 1+r.IntN(200)))
		}
	}
	return qx.EQ("country", plannerExtCountries[r.IntN(len(plannerExtCountries))])
}

func plannerExtRandomLeaf(r *rand.Rand, orderField string, allowNegative bool) qx.Expr {
	if allowNegative && r.IntN(4) == 0 {
		switch r.IntN(2) {
		case 0:
			return qx.NOTIN("country", plannerExtRandomStrings(r, plannerExtCountries, 3, false))
		default:
			return qx.NOTIN("name", plannerExtRandomStrings(r, plannerExtNames, 3, false))
		}
	}
	return plannerExtRandomPositiveLeaf(r, orderField)
}

func plannerExtRandomBranch(r *rand.Rand, orderField string, allowNegativeOnly bool) qx.Expr {
	leafCount := 1 + r.IntN(3)
	ops := make([]qx.Expr, 0, leafCount)
	positive := 0

	for len(ops) < leafCount {
		leaf := plannerExtRandomLeaf(r, orderField, true)
		if !leaf.Not && leaf.Op != qx.OpNOOP {
			positive++
		}
		ops = append(ops, leaf)
	}
	if !allowNegativeOnly && positive == 0 {
		ops = append(ops, plannerExtRandomPositiveLeaf(r, orderField))
	}

	var out qx.Expr
	if len(ops) == 1 {
		out = ops[0]
	} else {
		out = qx.AND(ops...)
	}
	if r.IntN(3) == 0 {
		out = wrapExprWithNoise(out, r.IntN(4))
	}
	return out
}

func plannerExtRandomORQuery(
	t *testing.T,
	r *rand.Rand,
	ordered bool,
	allowNegativeOnly bool,
	offsetAllowed bool,
) *qx.QX {
	t.Helper()

	orderFields := []string{"age", "score", "full_name"}

	for attempt := 0; attempt < 256; attempt++ {
		orderField := ""
		if ordered {
			orderField = orderFields[r.IntN(len(orderFields))]
		}

		branchCount := 2 + r.IntN(3)
		ops := make([]qx.Expr, 0, branchCount)
		for len(ops) < branchCount {
			ops = append(ops, plannerExtRandomBranch(r, orderField, allowNegativeOnly))
		}

		q := qx.Query(qx.OR(ops...))
		if ordered {
			if r.IntN(2) == 0 {
				q = q.By(orderField, qx.ASC)
			} else {
				q = q.By(orderField, qx.DESC)
			}
		}
		if offsetAllowed && r.IntN(3) == 0 {
			q = q.Skip(r.IntN(40))
		}
		q = q.Max(1 + r.IntN(80))

		nq := normalizeQueryForTest(q)
		if nq.Expr.Op == qx.OpOR && !nq.Expr.Not && len(nq.Expr.Operands) >= 2 {
			return nq
		}
	}

	t.Fatal("failed to generate normalized OR query")
	return nil
}

func plannerExtValidateNoOrderWindow(q *qx.QX, got, full []uint64) error {
	seen := make(map[uint64]struct{}, len(got))
	for _, id := range got {
		if _, ok := seen[id]; ok {
			return fmt.Errorf("duplicate id=%d result=%v", id, got)
		}
		seen[id] = struct{}{}
	}

	allow := make(map[uint64]struct{}, len(full))
	for _, id := range full {
		allow[id] = struct{}{}
	}
	for _, id := range got {
		if _, ok := allow[id]; !ok {
			return fmt.Errorf("id=%d outside full result set", id)
		}
	}

	maxLen := len(full)
	if q.Offset >= uint64(len(full)) {
		maxLen = 0
	} else if q.Offset > 0 {
		maxLen = len(full) - int(q.Offset)
	}
	if q.Limit > 0 && int(q.Limit) < maxLen {
		maxLen = int(q.Limit)
	}
	if len(got) > maxLen {
		return fmt.Errorf("window overflow got=%d max=%d", len(got), maxLen)
	}
	return nil
}

func plannerExtRequireTrace(events []TraceEvent) error {
	if len(events) == 0 {
		return fmt.Errorf("expected trace event")
	}
	if events[len(events)-1].Plan == "" {
		return fmt.Errorf("expected non-empty trace plan")
	}
	return nil
}

func plannerExtAssertQueryContract(t *testing.T, q *qx.QX) {
	t.Helper()

	var (
		mu     sync.Mutex
		events []TraceEvent
	)
	db := plannerExtOpenSeededDB(t, Options{
		AnalyzeInterval:  -1,
		TraceSink:        func(ev TraceEvent) { mu.Lock(); events = append(events, ev); mu.Unlock() },
		TraceSampleEvery: 1,
	})

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys(%+v): %v", q, err)
	}

	if len(q.Order) > 0 || (q.Offset == 0 && q.Limit == 0) {
		want, err := expectedKeysUint64(t, db, q)
		if err != nil {
			t.Fatalf("expectedKeysUint64(%+v): %v", q, err)
		}
		if !queryIDsEqual(q, got, want) {
			t.Fatalf("query mismatch:\nq=%+v\ngot=%v\nwant=%v", q, got, want)
		}
	} else {
		fullQ := cloneQuery(q)
		fullQ.Offset = 0
		fullQ.Limit = 0
		full, err := expectedKeysUint64(t, db, fullQ)
		if err != nil {
			t.Fatalf("expectedKeysUint64(full %+v): %v", fullQ, err)
		}
		if err = plannerExtValidateNoOrderWindow(q, got, full); err != nil {
			t.Fatalf("no-order contract mismatch:\nq=%+v\nerr=%v\ngot=%v\nfull=%v", q, err, got, full)
		}
	}

	mu.Lock()
	err = plannerExtRequireTrace(events)
	mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

func plannerExtRunConcurrentQueries(
	t *testing.T,
	db *DB[uint64, Rec],
	q *qx.QX,
	workers int,
	rounds int,
	validate func([]uint64) error,
) {
	t.Helper()

	errCh := make(chan error, workers)
	var wg sync.WaitGroup

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for round := 0; round < rounds; round++ {
				got, err := db.QueryKeys(q)
				if err != nil {
					errCh <- fmt.Errorf("worker=%d round=%d QueryKeys: %w", worker, round, err)
					return
				}
				if err = validate(got); err != nil {
					errCh <- fmt.Errorf("worker=%d round=%d: %w", worker, round, err)
					return
				}
			}
		}(worker)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPlannerExt_Query005_OrderedEqAndInSameFieldAgeAsc(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery005())
}

func TestPlannerExt_Query248_OrderedRedundantUpperBoundsAgeDescOffset(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery248())
}

func TestPlannerExt_Query296_OrderedOROverlappingCountryINAgeAsc(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery296())
}

func TestPlannerExt_Query561_OrderedORBroadEqVsRangeAgeDescOffset(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery561())
}

func TestPlannerExt_Query570_NoOrderORDuplicatesOnOverlap(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery570())
}

func TestPlannerExt_Query584_OrderedNotInPlusCountryAndScoreFullNameAsc(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery584())
}

func TestPlannerExt_Query651_OrderedORRangeBandVsActiveFalseAgeAscOffset(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery651())
}

func TestPlannerExt_Query652_OrderedScoreBandWithNotInFullNameAscOffset(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery652())
}

func TestPlannerExt_Query717_OrderedHasAnyAndScoreAgeDesc(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery717())
}

func TestPlannerExt_Query1145_OrderedHasAnyAndScoreAgeAsc(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery1145())
}

func TestPlannerExt_Query1216_NoOrderResultEscapesFullSet(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery1216())
}

func TestPlannerExt_Query1269_OrderedHasAnyGoAndScoreAndAgeAgeDescOffset(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery1269())
}

func TestPlannerExt_Query1283_OrderedDuplicateORBranchesAgeDesc(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery1283())
}

func TestPlannerExt_Query1363_OrderedRedundantUpperBoundsFullNameDesc(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery1363())
}

func TestPlannerExt_Query1383_OrderedOROverlappingCountryDEFinlandAgeAsc(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery1383())
}

func TestPlannerExt_Query1386_OrderedORCountryNLOrScoreBandAgeAsc(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery1386())
}

func TestPlannerExt_Query1473_OrderedORScoreBandOrScoreWindowAgeAsc(t *testing.T) {
	plannerExtAssertQueryContract(t, plannerExtQuery1473())
}

func TestPlannerExt_AdversarialPlannerCorpus(t *testing.T) {
	tests := []struct {
		name string
		q    *qx.QX
	}{
		{
			name: "ordered_negative_only_branch",
			q:    plannerExtQueryAdversarialOrderedNegativeOnlyBranch(),
		},
		{
			name: "ordered_negative_residual_overlap",
			q:    plannerExtQueryAdversarialOrderedNegativeResidualOverlap(),
		},
		{
			name: "no_order_negative_residual_overlap",
			q:    plannerExtQueryAdversarialNoOrderNegativeResidualOverlap(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			plannerExtAssertQueryContract(t, tc.q)
		})
	}
}

func TestPlannerExt_Race_OrderedEqAndInSameFieldAgeAsc(t *testing.T) {
	db := plannerExtOpenSeededDB(t, Options{AnalyzeInterval: -1})
	q := plannerExtQuery005()
	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	plannerExtRunConcurrentQueries(t, db, q, 8, 20, func(got []uint64) error {
		if !queryIDsEqual(q, got, want) {
			return fmt.Errorf("got=%v want=%v", got, want)
		}
		return nil
	})
}

func TestPlannerExt_Race_OrderedOROverlappingCountryINAgeAsc(t *testing.T) {
	db := plannerExtOpenSeededDB(t, Options{AnalyzeInterval: -1})
	q := plannerExtQuery296()
	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	plannerExtRunConcurrentQueries(t, db, q, 8, 20, func(got []uint64) error {
		if !queryIDsEqual(q, got, want) {
			return fmt.Errorf("got=%v want=%v", got, want)
		}
		return nil
	})
}

func TestPlannerExt_Race_NoOrderORDuplicatesOnOverlap(t *testing.T) {
	db := plannerExtOpenSeededDB(t, Options{AnalyzeInterval: -1})
	q := plannerExtQuery570()
	fullQ := cloneQuery(q)
	fullQ.Offset = 0
	fullQ.Limit = 0
	full, err := expectedKeysUint64(t, db, fullQ)
	if err != nil {
		t.Fatalf("expectedKeysUint64(full): %v", err)
	}
	plannerExtRunConcurrentQueries(t, db, q, 8, 20, func(got []uint64) error {
		return plannerExtValidateNoOrderWindow(q, got, full)
	})
}

func TestPlannerExt_Race_TraceAndCalibrationUnderConcurrentQueries(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)
	db := plannerExtOpenSeededDB(t, Options{
		AnalyzeInterval:        -1,
		TraceSink:              func(ev TraceEvent) { mu.Lock(); events = append(events, ev); mu.Unlock() },
		TraceSampleEvery:       1,
		CalibrationEnabled:     true,
		CalibrationSampleEvery: 1,
	})

	q := plannerExtQuery717()
	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}

	errCh := make(chan error, 16)
	var wg sync.WaitGroup

	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := 0; round < 20; round++ {
				got, err := db.QueryKeys(q)
				if err != nil {
					errCh <- err
					return
				}
				if !queryIDsEqual(q, got, want) {
					errCh <- fmt.Errorf("got=%v want=%v", got, want)
					return
				}
			}
		}()
	}

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := 0; round < 50; round++ {
				snap, ok := db.GetCalibrationSnapshot()
				if !ok {
					errCh <- fmt.Errorf("expected initialized calibration snapshot")
					return
				}
				for name, mult := range snap.Multipliers {
					if mult <= 0 {
						errCh <- fmt.Errorf("invalid multiplier %q=%v", name, mult)
						return
					}
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace events")
	}
	for i, ev := range events {
		if ev.Plan == "" {
			t.Fatalf("event %d has empty plan", i)
		}
		if ev.RowsReturned != uint64(len(want)) {
			t.Fatalf("event %d rows returned mismatch: got=%d want=%d", i, ev.RowsReturned, len(want))
		}
	}
}

func TestPlannerExt_Race_RefreshPlannerStatsDuringQueries(t *testing.T) {
	db := plannerExtOpenSeededDB(t, Options{AnalyzeInterval: -1})
	q := plannerExtQuery1269()
	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}

	errCh := make(chan error, 16)
	var wg sync.WaitGroup

	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := 0; round < 20; round++ {
				got, err := db.QueryKeys(q)
				if err != nil {
					errCh <- err
					return
				}
				if !queryIDsEqual(q, got, want) {
					errCh <- fmt.Errorf("got=%v want=%v", got, want)
					return
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if err := db.RefreshPlannerStats(); err != nil {
				errCh <- err
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			s := db.PlannerStats()
			if s.Fields != nil {
				s.Fields["country"] = PlannerFieldStats{}
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPlannerExt_Race_PeriodicAnalyzerDuringAdversarialQueries(t *testing.T) {
	const analyzeInterval = 5 * time.Millisecond

	db := plannerExtOpenSeededDB(t, Options{
		AnalyzeInterval:  analyzeInterval,
		TraceSink:        func(TraceEvent) {},
		TraceSampleEvery: 1,
	})

	queries := []*qx.QX{
		plannerExtQueryAdversarialOrderedNegativeOnlyBranch(),
		plannerExtQueryAdversarialOrderedNegativeResidualOverlap(),
		plannerExtQueryAdversarialNoOrderNegativeResidualOverlap(),
	}

	type expectation struct {
		q     *qx.QX
		exact []uint64
		full  []uint64
	}

	exps := make([]expectation, 0, len(queries))
	for _, q := range queries {
		if len(q.Order) > 0 || (q.Offset == 0 && q.Limit == 0) {
			want, err := expectedKeysUint64(t, db, q)
			if err != nil {
				t.Fatalf("expectedKeysUint64(%+v): %v", q, err)
			}
			exps = append(exps, expectation{q: q, exact: want})
			continue
		}

		fullQ := cloneQuery(q)
		fullQ.Offset = 0
		fullQ.Limit = 0
		full, err := expectedKeysUint64(t, db, fullQ)
		if err != nil {
			t.Fatalf("expectedKeysUint64(full %+v): %v", fullQ, err)
		}
		exps = append(exps, expectation{q: q, full: full})
	}

	startVersion := db.PlannerStats().Version
	if latest, ok := waitPlannerStatsVersionGreater(db, startVersion, 250*time.Millisecond); !ok {
		t.Fatalf("expected periodic analyzer to advance planner stats: start=%d latest=%d", startVersion, latest)
	}

	errCh := make(chan error, 32)
	var wg sync.WaitGroup

	for worker := 0; worker < 6; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for round := 0; round < 20; round++ {
				for i := range exps {
					exp := exps[(worker+round+i)%len(exps)]
					got, err := db.QueryKeys(exp.q)
					if err != nil {
						errCh <- fmt.Errorf("worker=%d round=%d QueryKeys(%+v): %w", worker, round, exp.q, err)
						return
					}
					if exp.exact != nil {
						if !queryIDsEqual(exp.q, got, exp.exact) {
							errCh <- fmt.Errorf("worker=%d round=%d exact mismatch q=%+v got=%v want=%v", worker, round, exp.q, got, exp.exact)
							return
						}
						continue
					}
					if err = plannerExtValidateNoOrderWindow(exp.q, got, exp.full); err != nil {
						errCh <- fmt.Errorf("worker=%d round=%d no-order mismatch q=%+v err=%v got=%v full=%v", worker, round, exp.q, err, got, exp.full)
						return
					}
				}
			}
		}(worker)
	}

	for reader := 0; reader < 3; reader++ {
		wg.Add(1)
		go func(reader int) {
			defer wg.Done()
			lastVersion := uint64(0)
			for round := 0; round < 200; round++ {
				s := db.PlannerStats()
				if s.AnalyzeInterval != analyzeInterval {
					errCh <- fmt.Errorf("reader=%d round=%d analyze interval mismatch: got=%v want=%v", reader, round, s.AnalyzeInterval, analyzeInterval)
					return
				}
				if s.Version < lastVersion {
					errCh <- fmt.Errorf("reader=%d round=%d planner stats version regressed: got=%d prev=%d", reader, round, s.Version, lastVersion)
					return
				}
				if s.FieldCount != len(s.Fields) {
					errCh <- fmt.Errorf("reader=%d round=%d field count mismatch: count=%d len=%d", reader, round, s.FieldCount, len(s.Fields))
					return
				}
				if s.Version > 0 && s.GeneratedAt.IsZero() {
					errCh <- fmt.Errorf("reader=%d round=%d missing generated_at for version=%d", reader, round, s.Version)
					return
				}

				s.Fields["country"] = PlannerFieldStats{}
				delete(s.Fields, "age")
				lastVersion = s.Version
			}
		}(reader)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for round := 0; round < 25; round++ {
			if err := db.RefreshPlannerStats(); err != nil {
				errCh <- fmt.Errorf("RefreshPlannerStats round=%d: %w", round, err)
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPlannerExt_Bug_ExecPlanORNoOrderAdaptive_PanicsOnAlwaysTrueBranch(t *testing.T) {
	db := plannerExtOpenSeededDB(t, Options{AnalyzeInterval: -1})
	q := normalizeQueryForTest(
		qx.Query(
			qx.OR(
				qx.EQ("country", "DE"),
				qx.EQ("country", "PL"),
				qx.PREFIX("full_name", "FN-"),
			),
		).Max(43),
	)

	branches, alwaysFalse, ok := db.buildORBranches(q.Expr.Operands)
	if !ok {
		t.Fatalf("buildORBranches: ok=false q=%+v", q)
	}
	defer releaseORBranches(branches)
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for q=%+v", q)
	}
	if len(branches) != 3 {
		t.Fatalf("unexpected branch count: got=%d want=3", len(branches))
	}
	if !branches[2].alwaysTrue || branches[2].hasLead() || branches[2].leadPtr() != nil {
		t.Fatalf("expected broad prefix branch to collapse to alwaysTrue without lead: branch=%+v", branches[2])
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("execPlanORNoOrderAdaptive panicked on alwaysTrue branch for q=%+v: %v", q, r)
		}
	}()

	if _, ok = db.execPlanORNoOrderAdaptive(q, branches, nil); !ok {
		t.Fatalf("expected adaptive OR planner to return a result or degrade safely for q=%+v", q)
	}
}

func TestPlannerExt_Bug_ExecPlanORNoOrderBaseline_PanicsOnAlwaysTrueBranch(t *testing.T) {
	db := plannerExtOpenSeededDB(t, Options{AnalyzeInterval: -1})
	q := normalizeQueryForTest(
		qx.Query(
			qx.OR(
				qx.EQ("country", "DE"),
				qx.EQ("country", "PL"),
				qx.PREFIX("full_name", "FN-"),
			),
		).Max(43),
	)

	branches, alwaysFalse, ok := db.buildORBranches(q.Expr.Operands)
	if !ok {
		t.Fatalf("buildORBranches: ok=false q=%+v", q)
	}
	defer releaseORBranches(branches)
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for q=%+v", q)
	}
	if len(branches) != 3 {
		t.Fatalf("unexpected branch count: got=%d want=3", len(branches))
	}
	if !branches[2].alwaysTrue || branches[2].hasLead() || branches[2].leadPtr() != nil {
		t.Fatalf("expected broad prefix branch to collapse to alwaysTrue without lead: branch=%+v", branches[2])
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("execPlanORNoOrderBaseline panicked on alwaysTrue branch for q=%+v: %v", q, r)
		}
	}()

	if _, ok = db.execPlanORNoOrderBaseline(q, branches, nil); !ok {
		t.Fatalf("expected baseline OR planner to return a result or degrade safely for q=%+v", q)
	}
}

func TestPlannerExt_Property_OrderedORInternalPlansMatchSeqScan(t *testing.T) {
	db := plannerExtOpenSeededDB(t, Options{AnalyzeInterval: -1})
	r := rand.New(rand.NewPCG(0x71c0ffee, 0x1234abcd))

	var (
		sawBasic    int
		sawFallback int
		sawKWay     int
	)

	for i := 0; i < plannerExtPropertyCases; i++ {
		q := plannerExtRandomORQuery(t, r, true, true, true)

		want, err := expectedKeysUint64(t, db, q)
		if err != nil {
			t.Fatalf("case=%d expectedKeysUint64(%+v): %v", i, q, err)
		}

		gotQuery, err := db.QueryKeys(q)
		if err != nil {
			t.Fatalf("case=%d QueryKeys(%+v): %v", i, q, err)
		}
		if !queryIDsEqual(q, gotQuery, want) {
			t.Fatalf("case=%d public planner mismatch:\nq=%+v\ngot=%v\nwant=%v", i, q, gotQuery, want)
		}

		branchesBasic, alwaysFalse, ok := db.buildORBranches(q.Expr.Operands)
		if !ok {
			t.Fatalf("case=%d buildORBranches basic: ok=false q=%+v", i, q)
		}
		if alwaysFalse {
			releaseORBranches(branchesBasic)
			if len(want) != 0 || len(gotQuery) != 0 {
				t.Fatalf("case=%d alwaysFalse branch set returned rows: q=%+v got=%v want=%v", i, q, gotQuery, want)
			}
			continue
		}
		gotBasic, okBasic := db.execPlanOROrderBasic(q, branchesBasic, nil)
		releaseORBranches(branchesBasic)
		if okBasic {
			sawBasic++
			if !queryIDsEqual(q, gotBasic, want) {
				t.Fatalf("case=%d execPlanOROrderBasic mismatch:\nq=%+v\ngot=%v\nwant=%v", i, q, gotBasic, want)
			}
		}

		branchesFallback, alwaysFalse, ok := db.buildORBranches(q.Expr.Operands)
		if !ok {
			t.Fatalf("case=%d buildORBranches fallback: ok=false q=%+v", i, q)
		}
		if alwaysFalse {
			releaseORBranches(branchesFallback)
			continue
		}
		gotFallback, okFallback, err := db.execPlanOROrderMergeFallback(q, branchesFallback, nil)
		releaseORBranches(branchesFallback)
		if err != nil {
			t.Fatalf("case=%d execPlanOROrderMergeFallback(%+v): %v", i, q, err)
		}
		if okFallback {
			sawFallback++
			if !queryIDsEqual(q, gotFallback, want) {
				t.Fatalf("case=%d execPlanOROrderMergeFallback mismatch:\nq=%+v\ngot=%v\nwant=%v", i, q, gotFallback, want)
			}
		}

		branchesKWay, alwaysFalse, ok := db.buildORBranches(q.Expr.Operands)
		if !ok {
			t.Fatalf("case=%d buildORBranches kway: ok=false q=%+v", i, q)
		}
		if alwaysFalse {
			releaseORBranches(branchesKWay)
			continue
		}
		gotKWay, okKWay, err := db.execPlanOROrderKWay(q, branchesKWay, nil)
		releaseORBranches(branchesKWay)
		if err != nil {
			t.Fatalf("case=%d execPlanOROrderKWay(%+v): %v", i, q, err)
		}
		if okKWay {
			sawKWay++
			if !queryIDsEqual(q, gotKWay, want) {
				t.Fatalf("case=%d execPlanOROrderKWay mismatch:\nq=%+v\ngot=%v\nwant=%v", i, q, gotKWay, want)
			}
		}
	}

	if sawBasic == 0 {
		t.Fatalf("expected execPlanOROrderBasic to be exercised")
	}
	if sawFallback == 0 {
		t.Fatalf("expected execPlanOROrderMergeFallback to be exercised")
	}
	if sawKWay == 0 {
		t.Fatalf("expected execPlanOROrderKWay to be exercised")
	}
}

func TestPlannerExt_Property_NoOrderORInternalPlansPreserveWindow(t *testing.T) {
	db := plannerExtOpenSeededDB(t, Options{AnalyzeInterval: -1})
	r := rand.New(rand.NewPCG(0xfeedbabe, 0x55aa33cc))

	var (
		sawAdaptive int
		sawBaseline int
		skippedLead int
		skippedBase int
	)

	for i := 0; i < plannerExtPropertyCases; i++ {
		q := plannerExtRandomORQuery(t, r, false, false, false)
		fullQ := cloneQuery(q)
		fullQ.Offset = 0
		fullQ.Limit = 0

		full, err := expectedKeysUint64(t, db, fullQ)
		if err != nil {
			t.Fatalf("case=%d expectedKeysUint64(full %+v): %v", i, fullQ, err)
		}

		gotQuery, err := db.QueryKeys(q)
		if err != nil {
			t.Fatalf("case=%d QueryKeys(%+v): %v", i, q, err)
		}
		if err = plannerExtValidateNoOrderWindow(q, gotQuery, full); err != nil {
			t.Fatalf("case=%d public no-order mismatch:\nq=%+v\nerr=%v\ngot=%v\nfull=%v", i, q, err, gotQuery, full)
		}
		if len(full) <= int(q.Limit) && !queryIDsEqual(q, gotQuery, full) {
			t.Fatalf("case=%d expected full no-order result when limit covers all rows:\nq=%+v\ngot=%v\nfull=%v", i, q, gotQuery, full)
		}

		branchesAdaptive, alwaysFalse, ok := db.buildORBranches(q.Expr.Operands)
		if !ok {
			t.Fatalf("case=%d buildORBranches adaptive: ok=false q=%+v", i, q)
		}
		if alwaysFalse {
			releaseORBranches(branchesAdaptive)
			if len(full) != 0 || len(gotQuery) != 0 {
				t.Fatalf("case=%d alwaysFalse no-order branch set returned rows: q=%+v got=%v full=%v", i, q, gotQuery, full)
			}
			continue
		}
		skipAdaptive := false
		for bi := range branchesAdaptive {
			if branchesAdaptive[bi].leadPtr() == nil {
				skipAdaptive = true
				skippedLead++
				break
			}
		}
		if skipAdaptive {
			releaseORBranches(branchesAdaptive)
		} else {
			gotAdaptive, okAdaptive := db.execPlanORNoOrderAdaptive(q, branchesAdaptive, nil)
			releaseORBranches(branchesAdaptive)
			if okAdaptive {
				sawAdaptive++
				if err = plannerExtValidateNoOrderWindow(q, gotAdaptive, full); err != nil {
					t.Fatalf("case=%d execPlanORNoOrderAdaptive mismatch:\nq=%+v\nerr=%v\ngot=%v\nfull=%v", i, q, err, gotAdaptive, full)
				}
				if len(full) <= int(q.Limit) && !queryIDsEqual(q, gotAdaptive, full) {
					t.Fatalf("case=%d adaptive underfilled full no-order result:\nq=%+v\ngot=%v\nfull=%v", i, q, gotAdaptive, full)
				}
			}
		}

		branchesBaseline, alwaysFalse, ok := db.buildORBranches(q.Expr.Operands)
		if !ok {
			t.Fatalf("case=%d buildORBranches baseline: ok=false q=%+v", i, q)
		}
		if alwaysFalse {
			releaseORBranches(branchesBaseline)
			continue
		}
		skipBaseline := false
		for bi := range branchesBaseline {
			if branchesBaseline[bi].leadPtr() == nil {
				skipBaseline = true
				skippedBase++
				break
			}
		}
		if skipBaseline {
			releaseORBranches(branchesBaseline)
			continue
		}
		gotBaseline, okBaseline := db.execPlanORNoOrderBaseline(q, branchesBaseline, nil)
		releaseORBranches(branchesBaseline)
		if okBaseline {
			sawBaseline++
			if err = plannerExtValidateNoOrderWindow(q, gotBaseline, full); err != nil {
				t.Fatalf("case=%d execPlanORNoOrderBaseline mismatch:\nq=%+v\nerr=%v\ngot=%v\nfull=%v", i, q, err, gotBaseline, full)
			}
			if len(full) <= int(q.Limit) && !queryIDsEqual(q, gotBaseline, full) {
				t.Fatalf("case=%d baseline underfilled full no-order result:\nq=%+v\ngot=%v\nfull=%v", i, q, gotBaseline, full)
			}
		}
	}

	if sawAdaptive == 0 {
		t.Fatalf("expected execPlanORNoOrderAdaptive to be exercised")
	}
	if sawBaseline == 0 {
		t.Fatalf("expected execPlanORNoOrderBaseline to be exercised")
	}
	if skippedLead == 0 {
		t.Fatalf("expected to encounter at least one leadless no-order branch shape")
	}
	if skippedBase == 0 {
		t.Fatalf("expected to encounter at least one leadless no-order baseline shape")
	}
}

func TestPlannerExt_OrderWindowRejectsOverflow(t *testing.T) {
	if got, ok := orderWindow(&qx.QX{Offset: 3, Limit: 7}); !ok || got != 10 {
		t.Fatalf("unexpected normal order window: got=%d ok=%v", got, ok)
	}
	if _, ok := orderWindow(&qx.QX{Offset: ^uint64(0), Limit: 1}); ok {
		t.Fatalf("expected overflowing order window to be rejected")
	}
}

func TestPlannerExt_TraceSampleEveryNormalization(t *testing.T) {
	if got := traceSampleEvery(1, nil); got != 0 {
		t.Fatalf("nil sink must disable trace sampling, got=%d", got)
	}
	if got := traceSampleEvery(-1, func(TraceEvent) {}); got != 0 {
		t.Fatalf("negative sampleEvery must disable trace sampling, got=%d", got)
	}
	if got := traceSampleEvery(0, func(TraceEvent) {}); got != 1 {
		t.Fatalf("zero sampleEvery must map to 1, got=%d", got)
	}
	if got := traceSampleEvery(7, func(TraceEvent) {}); got != 7 {
		t.Fatalf("unexpected sampleEvery pass-through: got=%d", got)
	}
}

func TestPlannerExt_CalibrationSampleEveryNormalization(t *testing.T) {
	if got := calibrationSampleEvery(false, 1); got != 0 {
		t.Fatalf("disabled calibration must ignore sampleEvery, got=%d", got)
	}
	if got := calibrationSampleEvery(true, -1); got != 0 {
		t.Fatalf("negative sampleEvery must disable calibration sampling, got=%d", got)
	}
	if got := calibrationSampleEvery(true, 0); got != defaultOptionsCalibrationSampleEvery {
		t.Fatalf("zero sampleEvery must map to default, got=%d", got)
	}
	if got := calibrationSampleEvery(true, 5); got != 5 {
		t.Fatalf("unexpected sampleEvery pass-through: got=%d", got)
	}
}

func TestPlannerExt_NextAnalyzeDelayCapsBackoffAtEightX(t *testing.T) {
	base := 10 * time.Millisecond
	rng := rand.New(rand.NewPCG(1, 2))
	got := nextAnalyzeDelay(base, 9, rng)
	if got < 8*base {
		t.Fatalf("backoff must be capped at >=8x base before jitter, got=%v", got)
	}
	if got > 8*base+8*base/5 {
		t.Fatalf("positive jitter must stay within 20%% cap, got=%v", got)
	}
}

func TestPlannerExt_PlannerORNoOrderInsertTopNStaysSortedAndCapped(t *testing.T) {
	top := uint64SlicePool.Get()
	defer uint64SlicePool.Put(top)
	for _, id := range []uint64{5, 2, 4, 1, 3} {
		plannerORNoOrderInsertTopN(top, id, 3)
	}
	want := []uint64{1, 2, 3}
	if top.Len() != len(want) {
		t.Fatalf("unexpected len(top): got=%d want=%d", top.Len(), len(want))
	}
	for i := range want {
		if top.Get(i) != want[i] {
			t.Fatalf("unexpected top[%d]: got=%d want=%d", i, top.Get(i), want[i])
		}
	}
}

func TestPlannerExt_PlannerORNoOrderSortBranchOrderByAscendingScoreThenIndex(t *testing.T) {
	order := []int{0, 1, 2}
	states := []plannerORNoOrderBranchState{
		{index: 0, score: 1},
		{index: 2, score: 3},
		{index: 1, score: 3},
	}
	plannerORNoOrderSortBranchOrder(order, states)
	want := []int{0, 2, 1}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("unexpected order[%d]: got=%d want=%d full=%v", i, order[i], want[i], order)
		}
	}
}

func TestPlannerExt_BuildORBranchesDropsFalseBranchAndKeepsTautology(t *testing.T) {
	db := plannerExtOpenSeededDB(t, Options{AnalyzeInterval: -1})

	branches, alwaysFalse, ok := db.buildORBranches([]qx.Expr{
		{Op: qx.OpNOOP, Not: true},
		{Op: qx.OpNOOP},
	})
	if !ok {
		t.Fatalf("buildORBranches: ok=false")
	}
	defer releaseORBranches(branches)
	if alwaysFalse {
		t.Fatalf("expected tautology branch to keep OR satisfiable")
	}
	if len(branches) != 1 || !branches[0].alwaysTrue {
		t.Fatalf("expected surviving branch to collapse to alwaysTrue")
	}
}

func TestPlannerExt_BuildORBranches_KeepsNegativeOnlyBranchWithoutLead(t *testing.T) {
	db := plannerExtOpenSeededDB(t, Options{AnalyzeInterval: -1})

	branches, alwaysFalse, ok := db.buildORBranches([]qx.Expr{
		qx.NOTIN("country", []string{"NL", "DE"}),
		qx.AND(
			qx.EQ("name", "alice"),
			qx.LT("score", 55.0),
		),
	})
	if !ok {
		t.Fatalf("buildORBranches: ok=false")
	}
	defer releaseORBranches(branches)
	if alwaysFalse {
		t.Fatalf("unexpected alwaysFalse for negative-only branch shape")
	}
	if len(branches) != 2 {
		t.Fatalf("unexpected branch count: got=%d want=2", len(branches))
	}
	if branches[0].alwaysTrue {
		t.Fatalf("negative-only branch must not collapse to alwaysTrue")
	}
	if branches[0].hasLead() || branches[0].leadPtr() != nil || branches[0].leadIdx != -1 {
		t.Fatalf("negative-only branch must not synthesize lead iterator: lead=%v leadIdx=%d", branches[0].leadPtr(), branches[0].leadIdx)
	}
	if branches[1].leadPtr() == nil {
		t.Fatalf("positive branch must retain lead iterator")
	}

	if branches[0].predLen() > plannerPredicateFastPathMaxLeaves {
		t.Fatalf("unexpected predicate count: got=%d max=%d", branches[0].predLen(), plannerPredicateFastPathMaxLeaves)
	}
	var checksInline [plannerPredicateFastPathMaxLeaves]int
	checks := branches[0].buildMatchChecks(checksInline[:0])
	if len(checks) == 0 {
		t.Fatalf("negative-only branch must still emit residual checks")
	}
	for _, idx := range checks {
		if !branches[0].predPtr(idx).hasContains() {
			t.Fatalf("negative-only branch check %d must stay matchable", idx)
		}
	}
}

func TestPlannerExt_PlannerFilterPostingByChecksExactMode(t *testing.T) {
	var srcPosting posting.List
	var keepPosting posting.List
	var excludePosting posting.List
	want := make([]uint64, 0, 30)
	for i := uint64(1); i <= 64; i++ {
		srcPosting = srcPosting.BuildAdded(i)
		if i >= 16 && i <= 48 {
			keepPosting = keepPosting.BuildAdded(i)
			want = append(want, i)
		}
		if i%10 == 0 {
			excludePosting = excludePosting.BuildAdded(i)
			if i >= 16 && i <= 48 {
				want = slices.DeleteFunc(want, func(v uint64) bool { return v == i })
			}
		}
	}
	workPosting := srcPosting.Clone()

	preds := []predicate{
		{kind: predicateKindMaterialized, ids: keepPosting},
		{kind: predicateKindMaterializedNot, ids: excludePosting},
	}

	mode, exact, workPosting, card := plannerFilterPostingByChecks(preds, []int{0, 1}, srcPosting, workPosting, true)
	defer workPosting.Release()
	if mode != plannerPredicateBucketExact {
		t.Fatalf("unexpected mode: got=%v", mode)
	}
	if exact.IsEmpty() {
		t.Fatalf("expected exact posting result")
	}
	if card != 64 {
		t.Fatalf("unexpected source cardinality: got=%d", card)
	}
	if got := exact.ToArray(); !slices.Equal(got, want) {
		t.Fatalf("unexpected exact posting: got=%v want=%v", got, want)
	}
}
