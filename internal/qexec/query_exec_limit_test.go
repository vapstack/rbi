package qexec

import (
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/vapstack/pooled"
	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/qir"
)

func mustLimitQIRExpr[K ~string | ~uint64, V any](t testing.TB, db *DB[K, V], expr qx.Expr) qir.Expr {
	t.Helper()
	prepared, compiled, err := prepareTestExpr(db.engine, expr)
	if err != nil {
		t.Fatalf("prepareTestExpr(%+v): %v", expr, err)
	}
	compiled = detachTestQIRExpr(compiled)
	prepared.Release()
	return compiled
}

func mustLimitQIRLeaves[K ~string | ~uint64, V any](t testing.TB, db *DB[K, V], expr qx.Expr) []qir.Expr {
	t.Helper()
	var leavesBuf [plannerPredicateFastPathMaxLeaves]qir.Expr
	leaves, ok := qir.CollectAndLeavesInto(mustLimitQIRExpr(t, db, expr), leavesBuf[:0], qir.LeafModeCollect)
	if !ok {
		t.Fatalf("CollectAndLeavesInto failed")
	}
	return append([]qir.Expr(nil), leaves...)
}

func filterQIRLeavesByField[K ~string | ~uint64, V any](db *DB[K, V], leaves []qir.Expr, field string) []qir.Expr {
	out := make([]qir.Expr, 0, len(leaves))
	for _, leaf := range leaves {
		if testExprFieldName(db.engine, leaf) == field {
			continue
		}
		out = append(out, leaf)
	}
	return out
}

func warmMaterializedPredForLimitTest[K ~string | ~uint64, V any](t testing.TB, db *DB[K, V], expr qx.Expr) qcache.MaterializedPredKey {
	t.Helper()
	op := mustLimitQIRExpr(t, db, expr)
	view := db.engine.currentQueryViewForTests()
	key := view.materializedPredKey(op)
	if key.IsZero() {
		t.Fatalf("expected materialized predicate key for %+v", expr)
	}
	ids := view.evalLazyMaterializedPredicateWithKey(op, key)
	ids.Release()
	if !view.snap.HasMaterializedPredKey(key) {
		t.Fatalf("expected warmed materialized predicate for key %s", key.String())
	}
	return key
}

func hideLenIndexForTest[K ~string | ~uint64, V any](t *testing.T, db *DB[K, V], field string) {
	t.Helper()
	snap := db.engine.snapshot.Current()
	acc, ok := db.engine.schema.IndexedByName[field]
	if !ok {
		t.Fatalf("missing indexed field %q", field)
	}
	oldStorage := snap.LenIndex[acc.Ordinal]
	oldZeroComplement := false
	if acc.Ordinal < len(snap.LenZeroComplement) {
		oldZeroComplement = snap.LenZeroComplement[acc.Ordinal]
		snap.LenZeroComplement[acc.Ordinal] = false
	}
	snap.LenIndex[acc.Ordinal] = indexdata.FieldStorage{}
	t.Cleanup(func() {
		snap.LenIndex[acc.Ordinal] = oldStorage
		if acc.Ordinal < len(snap.LenZeroComplement) {
			snap.LenZeroComplement[acc.Ordinal] = oldZeroComplement
		}
	})
}

type limitBoolVI bool

func (v limitBoolVI) IndexingValue() string {
	if v {
		return "yes"
	}
	return "no"
}

type limitBoolVIRec struct {
	Flag  limitBoolVI `db:"flag"  rbi:"index"`
	Score int         `db:"score" rbi:"index"`
}

func TestQuery_SliceEQ_MissingLenIndexStorageReturnsError(t *testing.T) {
	db, _ := openTempDBUint64(t)
	if err := db.Set(1, &Rec{Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	hideLenIndexForTest(t, db, "tags")

	_, err := db.QueryKeys(qx.Query(qx.EQ("tags", []string{"go"})))
	if err == nil || !strings.Contains(err.Error(), "no lenIndex for slice field: tags") {
		t.Fatalf("expected missing lenIndex error, got err=%v", err)
	}
}

func TestQuery_ByArrayCount_MissingLenIndexStorageReturnsError(t *testing.T) {
	db, _ := openTempDBUint64(t)
	if err := db.Set(1, &Rec{Name: "one", Email: "one@example.test", Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	hideLenIndexForTest(t, db, "tags")

	q := qx.Query().SortBy(qx.LEN("tags"), qx.ASC)
	_, err := db.QueryKeys(q)
	if err == nil || !strings.Contains(err.Error(), "no lenIndex for slice field: tags") {
		t.Fatalf("expected missing lenIndex error, got %v", err)
	}
}

func TestQuery_LimitNoOrder_UnsatisfiableLeafs_ReturnEmpty(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Email: "alice@example.com", Tags: []string{"go", "db"}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(2, &Rec{Name: "bob", Email: "bob@example.com", Tags: []string{"ops"}}); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		q    *qx.QX
	}{
		{
			name: "eq_missing",
			q:    qx.Query(qx.EQ("email", "missing@example.com")).Limit(8),
		},
		{
			name: "in_all_missing",
			q:    qx.Query(qx.IN("email", []string{"x@example.com", "y@example.com"})).Limit(8),
		},
		{
			name: "has_missing",
			q:    qx.Query(qx.HASALL("tags", []string{"missing"})).Limit(8),
		},
		{
			name: "hasany_all_missing",
			q:    qx.Query(qx.HASANY("tags", []string{"missing-1", "missing-2"})).Limit(8),
		},
		{
			name: "and_hit_plus_missing",
			q: qx.Query(
				qx.EQ("name", "alice"),
				qx.EQ("email", "missing@example.com"),
			).Limit(8),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ids, err := db.QueryKeys(tt.q)
			if err != nil {
				t.Fatalf("QueryKeys: %v", err)
			}
			if len(ids) != 0 {
				t.Fatalf("expected empty ids, got %v", ids)
			}

			items, err := db.Query(tt.q)
			if err != nil {
				t.Fatalf("Query: %v", err)
			}
			if len(items) != 0 {
				t.Fatalf("expected empty items, got len=%d", len(items))
			}
			db.ReleaseRecords(items...)
		})
	}
}

func TestQuery_LimitNoFilterNoOrder_UsesDirectLimitPlan(t *testing.T) {
	var events []TraceEvent
	opts := Options{
		TraceSink: func(ev TraceEvent) {
			events = append(events, ev)
		},
		TraceSampleEvery: 1,
	}
	db, _ := openTempDBUint64(t, opts)

	for i := 1; i <= 5; i++ {
		if err := db.Set(uint64(i), &Rec{Name: fmt.Sprintf("u%d", i), Age: 20 + i}); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}

	ids, err := db.QueryKeys(qx.Query().Limit(3))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("expected 3 ids, got %d: %v", len(ids), ids)
	}
	if err := testValidateNoDuplicateKeys("no_filter_no_order", ids); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if id == 0 || id > 5 {
			t.Fatalf("id %d is outside seeded range: %v", id, ids)
		}
	}
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	last := events[len(events)-1]
	if last.Plan != string(PlanLimit) {
		t.Fatalf("expected plan %q, got %q", PlanLimit, last.Plan)
	}
	if got := last.NoOrderLimitRoute.Selected; got != plannerNoOrderLimitCandidateNoFilter.String() {
		t.Fatalf("selected=%q want %q route=%+v", got, plannerNoOrderLimitCandidateNoFilter.String(), last.NoOrderLimitRoute)
	}
	if last.RowsExamined != 3 {
		t.Fatalf("expected RowsExamined=3, got trace=%+v", last)
	}
}

func TestQuery_NoOrderLimitUniverseScanCapsLargeLimitAllocation(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	const rows = 5
	for i := 1; i <= rows; i++ {
		if err := db.Set(uint64(i), &Rec{
			Name:  fmt.Sprintf("u%d", i),
			Email: fmt.Sprintf("u%d@example.com", i),
			Age:   20 + i,
		}); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}

	tests := []struct {
		name string
		q    *qx.QX
	}{
		{
			name: "single_negative_fast_path",
			q:    qx.Query(qx.NOT(qx.EQ("email", "missing@example.com"))).Limit(100_000),
		},
		{
			name: "generic_negative_universe_scan",
			q: qx.Query(
				qx.NOT(qx.EQ("email", "missing@example.com")),
				qx.NOT(qx.EQ("name", "missing")),
			).Limit(100_000),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prepared, viewQ, err := prepareTestQuery(db.engine, tt.q)
			if err != nil {
				t.Fatalf("prepareTestQuery: %v", err)
			}
			defer prepared.Release()

			got, ok, plan, err := db.engine.currentQueryViewForTests().executeNoOrderLimit(&viewQ, nil)
			if err != nil {
				t.Fatalf("executeNoOrderLimit: %v", err)
			}
			if !ok {
				t.Fatalf("executeNoOrderLimit: ok=false")
			}
			if plan != PlanCandidateNoOrder {
				t.Fatalf("plan=%s want %s", plan, PlanCandidateNoOrder)
			}
			if len(got) != rows {
				t.Fatalf("len(got)=%d want %d: %v", len(got), rows, got)
			}
			if cap(got) > rows {
				t.Fatalf("cap(got)=%d want <= %d", cap(got), rows)
			}
			if err := testValidateNoDuplicateKeys(tt.name, got); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestQuery_LimitDirectScansCapLargeLimitAllocation(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	for i := 1; i <= 5; i++ {
		name := fmt.Sprintf("drop_%d", i)
		if i <= 3 {
			name = fmt.Sprintf("keep_%d", i)
		}
		if err := db.Set(uint64(i), &Rec{
			Name:  name,
			Age:   i,
			Score: float64(i),
		}); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}

	noOrder := []struct {
		name    string
		q       *qx.QX
		wantLen int
		wantCap int
	}{
		{name: "range", q: qx.Query(qx.GTE("age", 4)).Limit(100_000), wantLen: 2, wantCap: 2},
		{name: "prefix", q: qx.Query(qx.PREFIX("name", "keep_")).Limit(100_000), wantLen: 3, wantCap: 3},
	}
	for _, tt := range noOrder {
		t.Run(tt.name, func(t *testing.T) {
			prepared, viewQ, err := prepareTestQuery(db.engine, tt.q)
			if err != nil {
				t.Fatalf("prepareTestQuery: %v", err)
			}
			defer prepared.Release()
			got, ok, _, err := db.engine.currentQueryViewForTests().executeNoOrderLimit(&viewQ, nil)
			if err != nil {
				t.Fatalf("executeNoOrderLimit: %v", err)
			}
			if !ok {
				t.Fatalf("executeNoOrderLimit: ok=false")
			}
			if len(got) != tt.wantLen {
				t.Fatalf("len(got)=%d want %d: %v", len(got), tt.wantLen, got)
			}
			if cap(got) > tt.wantCap {
				t.Fatalf("cap(got)=%d want <= %d", cap(got), tt.wantCap)
			}
		})
	}

	ordered := []struct {
		name    string
		q       *qx.QX
		wantLen int
		wantCap int
	}{
		{name: "order_no_filter", q: qx.Query().Sort("score", qx.ASC).Limit(100_000), wantLen: 5, wantCap: 5},
		{name: "order_offset_exhausted", q: qx.Query().Sort("score", qx.ASC).Offset(5).Limit(100_000), wantLen: 0, wantCap: 0},
	}
	for _, tt := range ordered {
		t.Run(tt.name, func(t *testing.T) {
			got, used, err := db.engine.executeOrderedLimit(tt.q, nil)
			if err != nil {
				t.Fatalf("executeOrderedLimit: %v", err)
			}
			if !used {
				t.Fatalf("executeOrderedLimit: used=false")
			}
			if len(got) != tt.wantLen {
				t.Fatalf("len(got)=%d want %d: %v", len(got), tt.wantLen, got)
			}
			if cap(got) > tt.wantCap {
				t.Fatalf("cap(got)=%d want <= %d", cap(got), tt.wantCap)
			}
		})
	}
}

func TestQuery_NoOrderLimitWideNotINUsesGenericPostsAnyState(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	terms := []string{"C0", "C1", "C2", "C3", "C4", "C5"}
	for i := 1; i <= 24; i++ {
		country := terms[i%len(terms)]
		if i > 18 {
			country = "KEEP"
		}
		if err := db.Set(uint64(i), &Rec{
			Meta: Meta{Country: country},
			Name: fmt.Sprintf("u%d", i),
			Age:  20 + i,
		}); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}

	prepared, viewQ, err := prepareTestQuery(db.engine, qx.Query(qx.NOTIN("country", terms)).Limit(100_000))
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer prepared.Release()

	view := db.engine.currentQueryViewForTests()
	universe := view.snap.Universe.Borrow()
	out, ok, err := view.execSelectedNoOrderSingleNegativeScalarUniverseScan(&viewQ, viewQ.Expr, universe, view.snap.Universe.Cardinality(), nil)
	universe.Release()
	if err != nil {
		t.Fatalf("execSelectedNoOrderSingleNegativeScalarUniverseScan: %v", err)
	}
	if ok {
		t.Fatalf("wide NOT IN should use generic postsAnyState path, got shortcut output %v", out)
	}

	got, ok, plan, err := view.executeNoOrderLimit(&viewQ, nil)
	if err != nil {
		t.Fatalf("executeNoOrderLimit: %v", err)
	}
	if !ok {
		t.Fatalf("executeNoOrderLimit: ok=false")
	}
	if plan != PlanCandidateNoOrder {
		t.Fatalf("plan=%s want %s", plan, PlanCandidateNoOrder)
	}
	if len(got) != 6 {
		t.Fatalf("len(got)=%d want 6: %v", len(got), got)
	}
}

func TestQuery_UniqueEqNoOrder_UsesSelectorTrace(t *testing.T) {
	var events []TraceEvent
	db, raw := openBoltAndNew[uint64, plannerPrecountRec](t, t.TempDir()+"/unique_eq_no_order.db", Options{
		AnalyzeInterval: -1,
		TraceSink: func(ev TraceEvent) {
			events = append(events, ev)
		},
		TraceSampleEvery: 1,
	})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	rows := []plannerPrecountRec{
		{Email: "a@example.com", Country: "NL", Active: true, Score: 1},
		{Email: "b@example.com", Country: "US", Active: true, Score: 2},
	}
	for i := range rows {
		if err := db.Set(uint64(i+1), &rows[i]); err != nil {
			t.Fatalf("Set(%d): %v", i+1, err)
		}
	}

	ids, err := db.QueryKeys(qx.Query(qx.EQ("email", "b@example.com")))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) != 1 || ids[0] != 2 {
		t.Fatalf("ids=%v want [2]", ids)
	}
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	last := events[len(events)-1]
	if last.Plan != string(PlanUniqueEq) {
		t.Fatalf("expected plan %q, got %q", PlanUniqueEq, last.Plan)
	}
	if got := last.NoOrderLimitRoute.Selected; got != plannerNoOrderLimitCandidateUniqueEq.String() {
		t.Fatalf("selected=%q want %q route=%+v", got, plannerNoOrderLimitCandidateUniqueEq.String(), last.NoOrderLimitRoute)
	}
}

func TestQuery_DirectPrefixNoOrderWithLimit_UsesSelectorTrace(t *testing.T) {
	var events []TraceEvent
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
		TraceSink: func(ev TraceEvent) {
			events = append(events, ev)
		},
		TraceSampleEvery: 1,
	})

	rows := []Rec{
		{FullName: "Alpha One", Active: true},
		{FullName: "Beta One", Active: true},
		{FullName: "Alpha Two", Active: true},
	}
	for i := range rows {
		if err := db.Set(uint64(i+1), &rows[i]); err != nil {
			t.Fatalf("Set(%d): %v", i+1, err)
		}
	}

	ids, err := db.QueryKeys(qx.Query(qx.PREFIX("full_name", "Alpha")).Limit(2))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("ids=%v want 2 rows", ids)
	}
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	last := events[len(events)-1]
	if last.Plan != string(PlanLimitPrefixNoOrder) {
		t.Fatalf("expected plan %q, got %q", PlanLimitPrefixNoOrder, last.Plan)
	}
	if got := last.NoOrderLimitRoute.Selected; got != plannerNoOrderLimitCandidateDirectPrefix.String() {
		t.Fatalf("selected=%q want %q route=%+v", got, plannerNoOrderLimitCandidateDirectPrefix.String(), last.NoOrderLimitRoute)
	}
}

func TestQuery_DirectRangeNoOrderWithLimit_UsesSelectorTrace(t *testing.T) {
	var events []TraceEvent
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
		TraceSink: func(ev TraceEvent) {
			events = append(events, ev)
		},
		TraceSampleEvery: 1,
	})
	_ = seedData(t, db, 20_000)

	ids, err := db.QueryKeys(qx.Query(qx.GTE("age", 20)).Limit(16))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) != 16 {
		t.Fatalf("ids=%v want 16 rows", ids)
	}
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	last := events[len(events)-1]
	if last.Plan != string(PlanLimitRangeNoOrder) {
		t.Fatalf("expected plan %q, got %q", PlanLimitRangeNoOrder, last.Plan)
	}
	if got := last.NoOrderLimitRoute.Selected; got != plannerNoOrderLimitCandidateDirectRange.String() {
		t.Fatalf("selected=%q want %q route=%+v", got, plannerNoOrderLimitCandidateDirectRange.String(), last.NoOrderLimitRoute)
	}
}

func TestNoOrderLimitTraceWork_DirectRangeSeparatesRangeProbe(t *testing.T) {
	db := newTestDB(t, testOptions{})
	db.seedData(t, 20_000)

	q := qx.Query(
		qx.GTE("age", 20),
		qx.LT("age", 90),
		qx.NOTIN("country", []string{"DE", "PL"}),
		qx.NOT(qx.EQ("active", false)),
		qx.NOTIN("name", []string{"alice", "bob"}),
	).Limit(64)

	prepared, shape, err := db.prepareQuery(q)
	if err != nil {
		t.Fatalf("prepareQuery: %v", err)
	}
	defer prepared.Release()

	view := db.view()
	facts := noOrderLimitFactsPool.Get()
	defer facts.Release()

	ok, err := view.collectNoOrderLimitFacts(&shape, facts)
	if err != nil {
		t.Fatalf("collectNoOrderLimitFacts: %v", err)
	}
	if !ok || !facts.directBoundsOK || facts.directResiduals < 3 {
		t.Fatalf("fixture did not collect broad direct range facts: ok=%v direct=%v residuals=%d", ok, facts.directBoundsOK, facts.directResiduals)
	}

	ov := view.fieldIndexViewFromSlotsByName(view.snap.Index, facts.directField)
	br := ov.RangeForBounds(facts.directBounds)
	if br.Empty() || br.BaseEnd <= br.BaseStart {
		t.Fatalf("fixture direct range is empty: %+v", br)
	}

	expected := uint64(128)
	residuals := float64(facts.directResiduals)
	width := float64(br.BaseEnd - br.BaseStart)
	kind := plannerNoOrderLimitCandidateDirectRange
	if facts.directBoundLeaves > 1 {
		kind = plannerNoOrderLimitCandidateSameFieldBounds
	}
	c := plannerNoOrderLimitCandidate{
		kind:         kind,
		cost:         float64(expected)*(1.0+residuals*0.70) + width*0.20,
		expectedRows: expected,
		leadRows:     expected * 4,
		checks:       uint64(facts.directResiduals),
	}

	work := view.noOrderLimitCandidateTraceWork(&shape, facts, c)
	if work.RangeProbe <= 0 || work.CandidateScan <= 0 || work.PostingContains <= 0 {
		t.Fatalf("direct-range trace work did not preserve components: %+v", work)
	}
}

func TestQuery_ArrayPosSingleHasAny_UsesSelectorTrace(t *testing.T) {
	var events []TraceEvent
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
		TraceSink: func(ev TraceEvent) {
			events = append(events, ev)
		},
		TraceSampleEvery: 1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(qx.HASANY("tags", []string{"java", "missing_tag"})).
		SortBy(qx.POS("tags", []string{"java", "go", "db"}), qx.ASC)

	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) == 0 {
		t.Fatalf("expected non-empty ids")
	}
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	last := events[len(events)-1]
	if last.Plan != string(PlanMaterialized) {
		t.Fatalf("expected plan %q, got %q", PlanMaterialized, last.Plan)
	}
	if got := last.ArrayPosOrderRoute.Selected; got != plannerArrayPosOrderCandidateSingleHasAny.String() {
		t.Fatalf("selected=%q want %q route=%+v", got, plannerArrayPosOrderCandidateSingleHasAny.String(), last.ArrayPosOrderRoute)
	}
}

func TestArrayPosOrderPrioritiesKeepTypedLookupKeysDistinct(t *testing.T) {
	raw := string([]byte{0, 0, 0, 0, 0, 0, 0, 7})
	fixed := uint64(7)

	asc := []keycodec.IndexLookupKey{
		keycodec.IndexLookupU64(fixed),
		keycodec.IndexLookupString(raw),
		keycodec.IndexLookupString("tag"),
	}
	asc = orderedDistinctLookupKeys(asc, false)
	if len(asc) != 3 || asc[0] != keycodec.IndexLookupU64(fixed) || asc[1] != keycodec.IndexLookupString(raw) || asc[2] != keycodec.IndexLookupString("tag") {
		t.Fatalf("ascending distinct priorities mismatch: got=%+v", asc)
	}

	desc := []keycodec.IndexLookupKey{
		keycodec.IndexLookupU64(fixed),
		keycodec.IndexLookupString(raw),
		keycodec.IndexLookupString("tag"),
	}
	desc = orderedDistinctLookupKeys(desc, true)
	if len(desc) != 3 || desc[0] != keycodec.IndexLookupString("tag") || desc[1] != keycodec.IndexLookupString(raw) || desc[2] != keycodec.IndexLookupU64(fixed) {
		t.Fatalf("descending distinct priorities mismatch: got=%+v", desc)
	}
}

func TestArrayPosOrderNumericCoverageRejectsFixedWidthStringPriorities(t *testing.T) {
	fixed := indexdata.GetFixedPostingMap()
	fixed[7] = (posting.List{}).BuildAdded(1)
	fixed[11] = (posting.List{}).BuildAdded(2)
	storage := indexdata.NewRegularFieldStorageFromFixedPostingMapOwned(fixed)
	defer storage.Release()

	ov := indexdata.NewFieldIndexViewFromStorage(storage)
	var buf [8]byte
	vals := []keycodec.IndexLookupKey{
		keycodec.IndexLookupString(string(keycodec.AppendU64Bytes(buf[:0], 7))),
		keycodec.IndexLookupString(string(keycodec.AppendU64Bytes(buf[:0], 11))),
	}
	if scalarArrayPosPriorityCoversAllKeysIndexView(ov, vals) {
		t.Fatalf("fixed-width string priorities must not cover numeric index keys")
	}
}

func TestQuery_RangeNoOrderWithLimit_NilEQ_UsesNilFieldIndexViewInEarlyRoute(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "nil-a", Opt: nil}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "value"}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}
	if err := db.Set(3, &Rec{Name: "nil-b", Opt: nil}); err != nil {
		t.Fatalf("Set(3): %v", err)
	}

	ids, err := db.QueryKeys(qx.Query(qx.EQ("opt", nil)).Limit(1))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected one id from nil equality limit query, got %v", ids)
	}

	items, err := db.Query(qx.Query(qx.EQ("opt", nil)).Limit(1))
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(items) != 1 || items[0] == nil || items[0].Opt != nil {
		t.Fatalf("expected one nil-opt item, got %#v", items)
	}
	db.ReleaseRecords(items...)
}

func TestQuery_SingleExactOrderedLimit_PrefersPlannerLimitPath(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)
	db, _ := openTempDBUint64(t, Options{
		TraceSink: func(ev TraceEvent) {
			mu.Lock()
			events = append(events, ev)
			mu.Unlock()
		},
		TraceSampleEvery: 1,
	})

	for i := 1; i <= 32; i++ {
		if err := db.Set(uint64(i), &Rec{
			Name:   fmt.Sprintf("u%d", i),
			Age:    18 + (i % 17),
			Active: i%2 == 0,
		}); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}

	q := qx.Query(
		qx.EQ("active", true),
	).Sort("age", qx.ASC).Limit(8)

	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) != 8 {
		t.Fatalf("expected 8 ids, got %d", len(ids))
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	if plan := events[len(events)-1].Plan; plan != string(PlanLimitOrderBasic) {
		t.Fatalf("expected %q, got %q", PlanLimitOrderBasic, plan)
	}
}

func TestQuery_OrderBasicBaseFastPathCapsLargeLimitByBaseCard(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	const (
		rows     = 32
		baseRows = 10
	)
	for i := 1; i <= rows; i++ {
		country := "DE"
		if i <= baseRows {
			country = "US"
		}
		if err := db.Set(uint64(i), &Rec{
			Meta:  Meta{Country: country},
			Name:  fmt.Sprintf("u_%02d", i),
			Age:   i,
			Score: float64(i),
		}); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}

	tests := []struct {
		name    string
		offset  int
		wantLen int
		wantCap int
	}{
		{name: "limit_capped", offset: 3, wantLen: baseRows - 3, wantCap: baseRows - 3},
		{name: "offset_exhausts_base", offset: baseRows, wantLen: 0, wantCap: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prepared, shape, err := prepareTestQuery(db.engine, qx.Query(qx.EQ("country", "US")).Sort("score", qx.ASC).Offset(tt.offset).Limit(100_000))
			if err != nil {
				t.Fatalf("prepareTestQuery: %v", err)
			}
			defer prepared.Release()

			view := db.engine.currentQueryViewForTests()
			base, err := view.evalExpr(shape.Expr)
			if err != nil {
				t.Fatalf("evalExpr: %v", err)
			}
			ov := view.fieldIndexViewFromSlotsForOrder(view.snap.Index, shape.Order)
			br := ov.RangeForBounds(indexdata.Bounds{Has: true})

			got, used, err := view.runOrderBasicBaseQuery(&shape, "score", nil, 0, shape.Order, ov, br, "", base, nil, nil, plannerOrderedLimitRuntimeGuard{}, nil)
			if err != nil {
				t.Fatalf("runOrderBasicBaseQuery: %v", err)
			}
			if !used {
				t.Fatalf("runOrderBasicBaseQuery: used=false")
			}
			if len(got) != tt.wantLen {
				t.Fatalf("len(got)=%d want %d: %v", len(got), tt.wantLen, got)
			}
			if cap(got) > tt.wantCap {
				t.Fatalf("cap(got)=%d want <= %d", cap(got), tt.wantCap)
			}
		})
	}
}

func TestQuery_OrderedLimitDirectShapesUseSelectorTrace(t *testing.T) {
	recorder := &traceContractRecorder{}
	db, _ := openTempDBUint64(t, Options{
		TraceSink:        recorder.sink,
		TraceSampleEvery: 1,
	})
	_ = seedData(t, db, 2_000)

	cases := []struct {
		name string
		q    *qx.QX
		plan PlanName
	}{
		{
			name: "NoFilter",
			q:    qx.Query().Sort("age", qx.ASC).Limit(8),
			plan: PlanLimitOrderBasic,
		},
		{
			name: "OrderFieldBounds",
			q: qx.Query(
				qx.GTE("age", 25),
				qx.LT("age", 40),
			).Sort("age", qx.ASC).Limit(8),
			plan: PlanLimitOrderBasic,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mark := recorder.mark()
			ids, err := db.QueryKeys(tc.q)
			if err != nil {
				t.Fatalf("QueryKeys: %v", err)
			}
			if len(ids) == 0 {
				t.Fatalf("expected non-empty result")
			}
			ev := recorder.lastSince(t, mark)
			if ev.Plan != string(tc.plan) {
				t.Fatalf("plan=%q want %q", ev.Plan, tc.plan)
			}
			if ev.OrderedLimitRoute.Selected != plannerOrderedLimitCandidateOrderScan.String() {
				t.Fatalf("selected=%q want %q route=%+v", ev.OrderedLimitRoute.Selected, plannerOrderedLimitCandidateOrderScan.String(), ev.OrderedLimitRoute)
			}
		})
	}
}

func TestQuery_RangeNoOrderAndBoundsLimit_PrefersLimitRoute(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
		TraceSink: func(ev TraceEvent) {
			mu.Lock()
			events = append(events, ev)
			mu.Unlock()
		},
		TraceSampleEvery: 1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.GTE("age", 20),
		qx.LT("age", 40),
	).Limit(16)

	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) != 16 {
		t.Fatalf("expected 16 ids, got %d", len(ids))
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	if plan := events[len(events)-1].Plan; plan != string(PlanLimitRangeNoOrder) {
		t.Fatalf("expected %q, got %q", PlanLimitRangeNoOrder, plan)
	}
	if got := events[len(events)-1].NoOrderLimitRoute.Selected; got != plannerNoOrderLimitCandidateSameFieldBounds.String() {
		t.Fatalf("selected=%q want %q route=%+v", got, plannerNoOrderLimitCandidateSameFieldBounds.String(), events[len(events)-1].NoOrderLimitRoute)
	}
}

func TestQuery_RangeNoOrderNegativeResidualLimit_PrefersLimitRoute(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
		TraceSink: func(ev TraceEvent) {
			mu.Lock()
			events = append(events, ev)
			mu.Unlock()
		},
		TraceSampleEvery: 1,
	})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.NOTIN("country", []string{"DE", "PL"}),
		qx.GTE("age", 20),
		qx.LT("age", 40),
	).Limit(16)

	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) != 16 {
		t.Fatalf("expected 16 ids, got %d", len(ids))
	}

	rows, err := db.BatchGet(ids...)
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	for i, row := range rows {
		if row == nil {
			t.Fatalf("missing row for id %d", ids[i])
		}
		if row.Country == "DE" || row.Country == "PL" || row.Age < 20 || row.Age >= 40 {
			t.Fatalf("row %d does not satisfy query: %+v", ids[i], row)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	if plan := events[len(events)-1].Plan; plan != string(PlanLimitRangeNoOrder) {
		t.Fatalf("expected %q, got %q", PlanLimitRangeNoOrder, plan)
	}
}

func TestQuery_RangeNoOrderNegativeResidualLimit_NoTrace(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 20_000)

	q := qx.Query(
		qx.NOTIN("country", []string{"DE", "PL"}),
		qx.GTE("age", 20),
		qx.LT("age", 40),
	).Limit(16)

	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) != 16 {
		t.Fatalf("expected 16 ids, got %d", len(ids))
	}

	rows, err := db.BatchGet(ids...)
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	for i, row := range rows {
		if row == nil {
			t.Fatalf("missing row for id %d", ids[i])
		}
		if row.Country == "DE" || row.Country == "PL" || row.Age < 20 || row.Age >= 40 {
			t.Fatalf("row %d does not satisfy query: %+v", ids[i], row)
		}
	}
}

func TestQuery_LimitOrderAndRange_UnsatisfiableRest_ReturnEmpty(t *testing.T) {
	db, _ := openTempDBUint64(t)

	for i := 1; i <= 120; i++ {
		email := fmt.Sprintf("user-%d@example.com", i)
		if err := db.Set(uint64(i), &Rec{
			Name:  fmt.Sprintf("user-%d", i),
			Email: email,
			Age:   18 + i%50,
			Tags:  []string{"go", "db"},
		}); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}

	qOrder := qx.Query(
		qx.EQ("email", "missing@example.com"),
	).Sort("age", qx.ASC).Limit(10)
	out, used, err := db.engine.executeOrderedLimit(qOrder, nil)
	if err != nil {
		t.Fatalf("executeOrderedLimit: %v", err)
	}
	if used && len(out) != 0 {
		t.Fatalf("expected empty result from executeOrderedLimit, got %v", out)
	}
	gotOrder, err := db.QueryKeys(qOrder)
	if err != nil {
		t.Fatalf("QueryKeys(order): %v", err)
	}
	if len(gotOrder) != 0 {
		t.Fatalf("expected empty result from QueryKeys(order), got %v", gotOrder)
	}

	qRange := qx.Query(
		qx.GTE("age", 20),
		qx.LT("age", 40),
		qx.EQ("email", "missing@example.com"),
	).Limit(10)
	rangeLeaves := mustExtractAndLeaves(t, qRange.Filter)
	f, bounds, ok, err := db.engine.extractNoOrderBounds(rangeLeaves)
	if err != nil {
		t.Fatalf("extractNoOrderBounds: %v", err)
	}
	if !ok {
		t.Fatalf("expected no-order range bounds to be recognized")
	}
	out, used, err = db.engine.execSelectedNoOrderBounds(qRange, f, bounds, rangeLeaves, nil)
	if err != nil {
		t.Fatalf("execSelectedNoOrderBounds: %v", err)
	}
	if !used {
		t.Fatalf("expected execSelectedNoOrderBounds to be used")
	}
	if len(out) != 0 {
		t.Fatalf("expected empty result from execSelectedNoOrderBounds, got %v", out)
	}
}

func TestQuery_OrderBasicLimit_MergedStringPrefixPreservesTightenedBounds(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	rows := [...]struct {
		email string
		score float64
	}{
		{"user1@example.com", 1},
		{"user8@example.com", 2},
		{"user9@example.com", 3},
		{"user90@example.com", 4},
		{"uses0@example.com", 5},
	}
	for i := range rows {
		if err := db.Set(uint64(i+1), &Rec{
			Name:   rows[i].email,
			Email:  rows[i].email,
			Score:  rows[i].score,
			Active: true,
		}); err != nil {
			t.Fatalf("Set(%d): %v", i+1, err)
		}
	}

	q := qx.Query(
		qx.GTE("email", "user"),
		qx.LT("email", "uses"),
		qx.GTE("email", "user9"),
	).Sort("score", qx.ASC).Limit(10)

	view := db.engine.currentQueryViewForTests()
	leaves := mustLimitQIRLeaves(t, db, q.Filter)
	preds, ok, err := view.buildLeafPredsExcludingBounds(leaves, "score", view.fieldOrdinalByName("score"), 10)
	if err != nil {
		t.Fatalf("buildLeafPredsExcludingBounds: %v", err)
	}
	if !ok {
		t.Fatalf("expected merged residual leaf preds to be supported")
	}
	if len(preds) != 1 {
		if preds != nil {
			leafPredSlicePool.Put(preds)
		}
		t.Fatalf("expected one merged leaf predicate, got %d", len(preds))
	}
	defer leafPredSlicePool.Put(preds)

	if !preds[0].containsIdx(3) || !preds[0].containsIdx(4) {
		t.Fatalf("tightened LIMIT prefix range should match user9/user90 rows")
	}
	if preds[0].containsIdx(1) || preds[0].containsIdx(2) || preds[0].containsIdx(5) {
		t.Fatalf("tightened LIMIT prefix range matched rows outside the merged bounds")
	}

	out, used, err := db.engine.executeOrderedLimit(q, nil)
	if err != nil {
		t.Fatalf("executeOrderedLimit: %v", err)
	}
	if !used {
		t.Fatalf("expected executeOrderedLimit to handle the query")
	}
	if !slices.Equal(out, []uint64{3, 4}) {
		t.Fatalf("executeOrderedLimit ids=%v want [3 4]", out)
	}
}

type orderBasicHighCardPrefixRec struct {
	Score  float64 `db:"score"  rbi:"index"`
	Email  string  `db:"email"  rbi:"index"`
	Status string  `db:"status" rbi:"index"`
	Plan   string  `db:"plan"   rbi:"index"`
}

func TestQuery_OrderBasicWithLimit_SkipsHighCardNonOrderPrefixShape(t *testing.T) {
	dir := t.TempDir()
	db, raw := openBoltAndNew[uint64, orderBasicHighCardPrefixRec](t, filepath.Join(dir, "test_order_basic_high_card.db"), Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	for i := 1; i <= 2_000; i++ {
		status := "paused"
		if i%2 == 0 {
			status = "active"
		}
		plan := "free"
		if i%4 != 0 {
			plan = "pro"
		}
		if err := db.Set(uint64(i), &orderBasicHighCardPrefixRec{
			Score:  float64(i),
			Email:  fmt.Sprintf("user%06d@example.com", i),
			Status: status,
			Plan:   plan,
		}); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}

	q := qx.Query(
		qx.PREFIX("email", "user0019"),
		qx.EQ("status", "active"),
		qx.NOTIN("plan", []string{"free"}),
	).Sort("score", qx.DESC).Limit(20)

	out, used, err := db.engine.executeOrderedLimit(q, nil)
	if err != nil {
		t.Fatalf("executeOrderedLimit: %v", err)
	}
	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if used && !reflect.DeepEqual(out, got) {
		t.Fatalf("order-basic result mismatch: got=%v want=%v", out, got)
	}
	want := []uint64{1998, 1994, 1990, 1986, 1982, 1978, 1974, 1970, 1966, 1962, 1958, 1954, 1950, 1946, 1942, 1938, 1934, 1930, 1926, 1922}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ordered result mismatch: got=%v want=%v", got, want)
	}
}

func TestQuery_OrderBasic_CollapsesStringRangeBaseOps(t *testing.T) {
	dir := t.TempDir()
	db, raw := openBoltAndNew[uint64, orderBasicHighCardPrefixRec](t, filepath.Join(dir, "test_order_basic_string_range.db"), Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	for i := 1; i <= 2_000; i++ {
		if err := db.Set(uint64(i), &orderBasicHighCardPrefixRec{
			Score:  float64(i),
			Email:  fmt.Sprintf("user%06d@example.com", i),
			Status: "active",
			Plan:   "pro",
		}); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}

	q := qx.Query(
		qx.GTE("email", "user000400@example.com"),
		qx.LT("email", "user000600@example.com"),
	).Sort("score", qx.DESC).Limit(32)

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(got) != 32 {
		t.Fatalf("expected 32 rows, got %d", len(got))
	}

	view := db.engine.currentQueryViewForTests()
	leaves := mustLimitQIRLeaves(t, db, q.Filter)
	baseOps := filterQIRLeavesByField(db, leaves, "score")
	coresBuf, rawCoreIdxBuf := mustPrepareOrderBasicBaseCoresForTest(t, view, baseOps)
	defer orderBasicBaseCoreSlicePool.Put(coresBuf)
	defer pooled.ReleaseIntSlice(rawCoreIdxBuf)

	collapsed := mustFindCollapsedOrderBasicBaseCoreForTest(t, coresBuf)
	if collapsed.collapsed.field != "email" {
		t.Fatalf("expected collapsed email range, got %q", collapsed.collapsed.field)
	}
	if len(coresBuf) != 1 {
		t.Fatalf("expected one collapsed range core, got %d", len(coresBuf))
	}
}

func TestQuery_OffsetBeyondResult_ReturnsEmpty(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 80)

	q := qx.Query(qx.EQ("country", "NL")).Offset(10_000).Limit(50)

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty slice, got %v", got)
	}
}

func TestQuery_NoOrder_UnboundedLimit_ReturnsValidPage(t *testing.T) {
	db, _ := openTempDBUint64(t)
	_ = seedData(t, db, 140)

	q := qx.Query(qx.GTE("age", 18)).Offset(35)

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}

	fullQ := cloneQuery(q)
	fullQ.Window.Offset = 0
	fullQ.Window.Limit = 0
	full, err := db.QueryKeys(fullQ)
	if err != nil {
		t.Fatalf("full QueryKeys: %v", err)
	}
	assertNoOrderPage(t, q, got, full, "no_order_unbounded_limit")

	if len(got) == 0 {
		t.Fatalf("expected non-empty paged result")
	}
}

func TestQuery_NoOrderLimit_NegatedOverlappingSlicePredicateReturnsComplement(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	seedGeneratedUint64Data(t, db, 10, func(i int) *Rec {
		tags := []string{"c"}
		if i <= 6 {
			tags = []string{"a", "b"}
		}
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Active: true,
			Tags:   tags,
		}
	})

	q := qx.Query(
		qx.EQ("active", true),
		qx.NOT(qx.HASANY("tags", []string{"a", "b"})),
	).Limit(3)

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	want := []uint64{7, 8, 9}
	assertSameSlice(t, got, want)
}

func TestQuery_NoOrderLimit_EmptySnapshotValidatesPredicate(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	limitQ := qx.Query(qx.HASANY("tags", []string{})).Limit(5)
	if _, err := db.QueryKeys(limitQ); !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("limit QueryKeys err=%v, want ErrInvalidQuery", err)
	}

	noLimitQ := qx.Query(qx.HASANY("tags", []string{}))
	if _, err := db.QueryKeys(noLimitQ); !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("no-limit QueryKeys err=%v, want ErrInvalidQuery", err)
	}
}

func TestQuery_OrderBasicLimit_EmptySnapshotValidatesResidual(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	q := qx.Query(qx.HASANY("tags", []string{})).Sort("age", qx.ASC).Limit(5)
	if _, err := db.QueryKeys(q); !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("QueryKeys err=%v, want ErrInvalidQuery", err)
	}
}

func TestQuery_ORLimit_InvalidSuffixContainsBranchReturnsError(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	seedGeneratedUint64Data(t, db, 40, func(i int) *Rec {
		return &Rec{
			Email:  fmt.Sprintf("user%d@example.com", i),
			Active: i%2 == 0,
			Score:  float64(i),
		}
	})

	for _, invalid := range []qx.Expr{
		qx.SUFFIX("email", []string{"@example.com"}),
		qx.CONTAINS("email", []string{"example"}),
	} {
		q := qx.Query(qx.OR(qx.EQ("active", true), invalid)).Limit(3)
		if _, err := db.QueryKeys(q); !errors.Is(err, ErrInvalidQuery) {
			t.Fatalf("no-order QueryKeys err=%v, want ErrInvalidQuery", err)
		}

		q = qx.Query(qx.OR(qx.EQ("active", true), invalid)).Sort("score", qx.ASC).Limit(3)
		if _, err := db.QueryKeys(q); !errors.Is(err, ErrInvalidQuery) {
			t.Fatalf("ordered QueryKeys err=%v, want ErrInvalidQuery", err)
		}
	}
}

func TestQuery_NoOrderLimit_EmptyRangeValidatesResidual(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	seedGeneratedUint64Data(t, db, 20, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Score: float64(i),
		}
	})

	q := qx.Query(
		qx.GTE("score", 10.0),
		qx.LTE("score", 5.0),
		qx.HASALL("name", []string{"u_1"}),
	).Limit(5)
	if _, err := db.QueryKeys(q); !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("QueryKeys err=%v, want ErrInvalidQuery", err)
	}
}

func TestQuery_NoOrderLimit_EmptyLeafValidatesUnsupportedResidual(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	seedGeneratedUint64Data(t, db, 20, func(i int) *Rec {
		return &Rec{
			Name: fmt.Sprintf("u_%d", i),
			Age:  i,
		}
	})

	q := qx.Query(
		qx.HASANY("name", []string{"u_1"}),
		qx.EQ("age", -1),
	).Limit(5)
	if _, err := db.QueryKeys(q); !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("QueryKeys err=%v, want ErrInvalidQuery", err)
	}
}

func TestQuery_OrderLimit_EmptyBaseLeafValidatesUnsupportedBaseOp(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	seedGeneratedUint64Data(t, db, 20, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   i,
			Score: float64(i),
		}
	})

	q := qx.Query(
		qx.HASANY("name", []string{"u_1"}),
		qx.EQ("age", -1),
	).Sort("score", qx.ASC).Limit(5)
	if _, err := db.QueryKeys(q); !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("QueryKeys err=%v, want ErrInvalidQuery", err)
	}
}

func TestQuery_OrderLimit_NegatedBoolEQRespectsValueIndexer(t *testing.T) {
	db, raw := openBoltAndNew[uint64, limitBoolVIRec](t, filepath.Join(t.TempDir(), "bool_vi.db"), Options{AnalyzeInterval: -1})
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	rows := []limitBoolVIRec{
		{Flag: true, Score: 1},
		{Flag: false, Score: 2},
		{Flag: false, Score: 3},
	}
	for i := range rows {
		if err := db.Set(uint64(i+1), &rows[i]); err != nil {
			t.Fatalf("Set(%d): %v", i+1, err)
		}
	}

	invalid := qx.Query(qx.NOT(qx.EQ("flag", true))).Sort("score", qx.ASC).Limit(5)
	if _, err := db.QueryKeys(invalid); err == nil {
		t.Fatalf("plain bool QueryKeys succeeded, want error")
	}

	valid := qx.Query(qx.NOT(qx.EQ("flag", limitBoolVI(true)))).Sort("score", qx.ASC).Limit(5)
	got, err := db.QueryKeys(valid)
	if err != nil {
		t.Fatalf("typed bool QueryKeys: %v", err)
	}
	want := []uint64{2, 3}
	if !slices.Equal(got, want) {
		t.Fatalf("typed bool QueryKeys got=%v want=%v", got, want)
	}
}

func TestQuery_OrderBasicLimit_RuntimeGuardAppliesToLeafPredScan(t *testing.T) {
	var events []TraceEvent
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSampleEvery: 1,
		TraceSink: func(ev TraceEvent) {
			events = append(events, ev)
		},
	})

	const rows = 20_000
	seedGeneratedUint64Data(t, db, rows, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%05d", i),
			Score:  float64(i),
			Active: i <= rows/2,
		}
	})

	q := qx.Query(qx.EQ("active", true)).Sort("score", qx.DESC).Limit(10)
	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	want := []uint64{10_000, 9_999, 9_998, 9_997, 9_996, 9_995, 9_994, 9_993, 9_992, 9_991}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("keys=%v want=%v", got, want)
	}
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	if ev.OrderedLimitRoute.Selected != plannerOrderedLimitCandidateOrderScan.String() {
		t.Fatalf("selected=%q want %q route=%+v", ev.OrderedLimitRoute.Selected, plannerOrderedLimitCandidateOrderScan.String(), ev.OrderedLimitRoute)
	}
	if !ev.OrderedLimitRoute.RuntimeGuardEnabled {
		t.Fatalf("expected ordered LIMIT runtime guard, route=%+v", ev.OrderedLimitRoute)
	}
	if !ev.OrderedLimitRoute.RuntimeFallbackTriggered {
		t.Fatalf("expected ordered LIMIT runtime fallback, route=%+v", ev.OrderedLimitRoute)
	}
}

func TestQuery_NoOrderLimitRuntimeFallbackUsesFallbackLead(t *testing.T) {
	db := newTestDB(t, testOptions{})
	db.seedGeneratedData(t, 2_000, func(id uint64) testRec {
		country := "COMMON"
		if id == 2_000 {
			country = "RARE"
		}
		return testRec{
			Meta: Meta{Country: country},
			Age:  int(id),
		}
	})

	q := qx.Query(
		qx.GTE("age", 1),
		qx.EQ("country", "RARE"),
	).Limit(1)
	prepared, shape, err := db.prepareQuery(q)
	if err != nil {
		t.Fatalf("prepareQuery: %v", err)
	}
	defer prepared.Release()

	view := db.view()
	facts := noOrderLimitFactsPool.Get()
	defer facts.Release()

	ok, err := view.collectNoOrderLimitFacts(&shape, facts)
	if err != nil {
		t.Fatalf("collectNoOrderLimitFacts: %v", err)
	}
	if !ok {
		t.Fatalf("collectNoOrderLimitFacts: ok=false")
	}

	var trace Trace
	out, used, plan, err := view.dispatchNoOrderLimit(
		&shape,
		facts,
		plannerNoOrderLimitDecision{
			selected: plannerNoOrderLimitCandidate{
				kind:    plannerNoOrderLimitCandidateDirectRange,
				leadIdx: 0,
				checks:  1,
			},
			runtimeFallback: plannerNoOrderLimitCandidate{
				kind:    plannerNoOrderLimitCandidateLeadScan,
				leadIdx: 1,
			},
		},
		plannerNoOrderLimitRuntimeGuard{
			enabled:      true,
			minExamined:  1,
			needWindow:   shape.Limit,
			fallbackCost: 1,
			rowCost:      2,
			reason:       "test_direct_range_guard",
		},
		&trace,
	)
	if err != nil {
		t.Fatalf("dispatchNoOrderLimit: %v", err)
	}
	if !used {
		t.Fatalf("dispatchNoOrderLimit: used=false")
	}
	if plan != PlanCandidateNoOrder {
		t.Fatalf("plan=%s want %s", plan, PlanCandidateNoOrder)
	}
	if !slices.Equal(out, []uint64{2_000}) {
		t.Fatalf("out=%v want [2000]", out)
	}
	if trace.RowsExamined() > 10 {
		t.Fatalf("fallback scanned the broad selected lead, rowsExamined=%d", trace.RowsExamined())
	}
}

func TestQuery_OrderBasicLimit_RuntimeGuardAppliesToPostingFilterScanNoTrace(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	const rows = 20_000
	seedGeneratedUint64Data(t, db, rows, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%05d", i),
			Score:  float64(i),
			Active: i <= rows/2,
		}
	})

	q := qx.Query(qx.EQ("active", true)).Sort("score", qx.DESC).Limit(10)
	prepared, viewQ, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer prepared.Release()

	got, ok, plan, err := db.engine.currentQueryViewForTests().executeOrderedLimit(&viewQ, nil)
	if err != nil {
		t.Fatalf("executeOrderedLimit: %v", err)
	}
	if !ok {
		t.Fatalf("executeOrderedLimit: ok=false")
	}
	want := []uint64{10_000, 9_999, 9_998, 9_997, 9_996, 9_995, 9_994, 9_993, 9_992, 9_991}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("keys=%v want=%v", got, want)
	}
	if plan != PlanMaterialized {
		t.Fatalf("plan=%s want %s", plan, PlanMaterialized)
	}
}

func TestQuery_OrderBasicLimit_BaseCoreRuntimeGuardFallsBackToMaterialized(t *testing.T) {
	var events []TraceEvent
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSampleEvery: 1,
		TraceSink: func(ev TraceEvent) {
			events = append(events, ev)
		},
	})

	const rows = 20_000
	seedGeneratedUint64Data(t, db, rows, func(i int) *Rec {
		return &Rec{
			Email: fmt.Sprintf("user%05d@example.com", i),
			Score: float64(i),
		}
	})
	warmMaterializedPredForLimitTest(t, db, qx.PREFIX("email", "user10"))

	got, err := db.QueryKeys(qx.Query(qx.PREFIX("email", "user10")).Sort("score", qx.DESC).Limit(10))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	want := []uint64{10_999, 10_998, 10_997, 10_996, 10_995, 10_994, 10_993, 10_992, 10_991, 10_990}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("keys=%v want=%v", got, want)
	}
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	if ev.OrderedLimitRoute.Selected != plannerOrderedLimitCandidateWarmBaseCore.String() {
		t.Fatalf("selected=%q want %q route=%+v", ev.OrderedLimitRoute.Selected, plannerOrderedLimitCandidateWarmBaseCore.String(), ev.OrderedLimitRoute)
	}
	if !ev.OrderedLimitRoute.RuntimeGuardEnabled || ev.OrderedLimitRoute.RuntimeGuardReason != "base_core_scan_guard" {
		t.Fatalf("expected base-core runtime guard, route=%+v", ev.OrderedLimitRoute)
	}
	if !ev.OrderedLimitRoute.RuntimeFallbackTriggered || ev.OrderedLimitRoute.RuntimeFallbackReason != "base_core_scan_guard" {
		t.Fatalf("expected base-core runtime fallback, route=%+v", ev.OrderedLimitRoute)
	}
	if ev.Plan != string(PlanMaterialized) {
		t.Fatalf("plan=%s want %s", ev.Plan, PlanMaterialized)
	}
}

func TestQuery_OrderBasicLimit_BaseCoreRuntimeGuardKeepsGoodPlacement(t *testing.T) {
	var events []TraceEvent
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSampleEvery: 1,
		TraceSink: func(ev TraceEvent) {
			events = append(events, ev)
		},
	})

	const rows = 20_000
	seedGeneratedUint64Data(t, db, rows, func(i int) *Rec {
		return &Rec{
			Email: fmt.Sprintf("user%05d@example.com", i),
			Score: float64(i),
		}
	})
	warmMaterializedPredForLimitTest(t, db, qx.PREFIX("email", "user19"))

	got, err := db.QueryKeys(qx.Query(qx.PREFIX("email", "user19")).Sort("score", qx.DESC).Limit(10))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	want := []uint64{19_999, 19_998, 19_997, 19_996, 19_995, 19_994, 19_993, 19_992, 19_991, 19_990}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("keys=%v want=%v", got, want)
	}
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	if ev.OrderedLimitRoute.Selected != plannerOrderedLimitCandidateWarmBaseCore.String() {
		t.Fatalf("selected=%q want %q route=%+v", ev.OrderedLimitRoute.Selected, plannerOrderedLimitCandidateWarmBaseCore.String(), ev.OrderedLimitRoute)
	}
	if !ev.OrderedLimitRoute.RuntimeGuardEnabled || ev.OrderedLimitRoute.RuntimeGuardReason != "base_core_scan_guard" {
		t.Fatalf("expected base-core runtime guard, route=%+v", ev.OrderedLimitRoute)
	}
	if ev.OrderedLimitRoute.RuntimeFallbackTriggered {
		t.Fatalf("unexpected base-core runtime fallback, route=%+v", ev.OrderedLimitRoute)
	}
	if ev.Plan != string(PlanLimitOrderBasic) {
		t.Fatalf("plan=%s want %s", ev.Plan, PlanLimitOrderBasic)
	}
}

func TestQuery_OrderBasicLimit_BaseCoreRuntimeGuardKeepsExpensiveMaterializedFallback(t *testing.T) {
	var events []TraceEvent
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSampleEvery: 1,
		TraceSink: func(ev TraceEvent) {
			events = append(events, ev)
		},
	})

	const rows = 20_000
	seedGeneratedUint64Data(t, db, rows, func(i int) *Rec {
		id := uint64(i)
		return &Rec{
			Age:   int(id),
			Score: float64((id*7919)%rows) + float64(id&15)/16,
		}
	})

	q := qx.Query(
		qx.GTE("age", 8_192),
		qx.LT("age", 8_256),
	).Sort("score", qx.DESC).Limit(128)
	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("warm QueryKeys: %v", err)
	}
	if len(got) != 64 {
		t.Fatalf("warm rows=%d want 64", len(got))
	}

	events = events[:0]
	got, err = db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(got) != 64 {
		t.Fatalf("rows=%d want 64", len(got))
	}
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	route := ev.OrderedLimitRoute
	if route.Selected != plannerOrderedLimitCandidateWarmBaseCore.String() {
		t.Fatalf("selected=%q want %q route=%+v", route.Selected, plannerOrderedLimitCandidateWarmBaseCore.String(), route)
	}
	if route.Rejected != plannerOrderedLimitCandidateMaterializedFallback.String() || route.RejectedCost <= route.SelectedCost {
		t.Fatalf("fixture no longer has an expensive materialized fallback: route=%+v", route)
	}
	if !route.RuntimeGuardEnabled || route.RuntimeGuardReason != "base_core_scan_guard" {
		t.Fatalf("expected base-core runtime guard, route=%+v", route)
	}
	if route.RuntimeFallbackTriggered {
		t.Fatalf("unexpected base-core runtime fallback, route=%+v", route)
	}
	if ev.Plan != string(PlanLimitOrderBasic) {
		t.Fatalf("plan=%s want %s", ev.Plan, PlanLimitOrderBasic)
	}
}

func TestQuery_OrderBasicLimit_BaseCoreRuntimeGuardNoTraceKeepsSmallBasePath(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	const rows = 20_000
	seedGeneratedUint64Data(t, db, rows, func(i int) *Rec {
		email := fmt.Sprintf("miss%05d@example.com", i)
		if i%200 == 0 {
			email = fmt.Sprintf("target%03d@example.com", i/200)
		}
		return &Rec{
			Email: email,
			Score: float64(i),
		}
	})
	warmMaterializedPredForLimitTest(t, db, qx.PREFIX("email", "target"))

	q := qx.Query(qx.PREFIX("email", "target")).Sort("score", qx.DESC).Limit(10)
	prepared, viewQ, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer prepared.Release()

	view := db.engine.currentQueryViewForTests()
	facts := orderedLimitFactsPool.Get()
	defer facts.Release()
	ok, err := view.collectOrderedLimitFacts(&viewQ, facts)
	if err != nil {
		t.Fatalf("collectOrderedLimitFacts: %v", err)
	}
	if !ok {
		t.Fatalf("collectOrderedLimitFacts: ok=false")
	}
	decision, ok, err := view.selectOrderedLimit(&viewQ, facts)
	if err != nil {
		t.Fatalf("selectOrderedLimit: %v", err)
	}
	if !ok {
		t.Fatalf("selectOrderedLimit: ok=false")
	}
	if decision.selected.kind != plannerOrderedLimitCandidateWarmBaseCore {
		t.Fatalf("selected=%v want %v", decision.selected.kind, plannerOrderedLimitCandidateWarmBaseCore)
	}
	guard := decision.baseCoreRuntimeGuard(&viewQ, len(facts.baseOps))
	if !guard.enabled {
		t.Fatalf("expected base-core guard")
	}

	base, err := view.evalExpr(viewQ.Expr)
	if err != nil {
		t.Fatalf("evalExpr: %v", err)
	}
	baseCard := int(base.ids.Cardinality())
	if baseCard != 100 {
		base.ids.Release()
		t.Fatalf("baseCard=%d want 100", baseCard)
	}
	if _, _, denseOK, fallback := scanOrderLimitDenseBaseNoTrace(&viewQ, facts.ov, facts.br, viewQ.Order.Desc, base.ids, baseCard, nil, nil, guard); denseOK || fallback {
		base.ids.Release()
		t.Fatalf("dense path ok=%v fallback=%v, want small-path fixture", denseOK, fallback)
	}
	gotFast, _, fallback := scanOrderLimitSmallBaseNoTrace(&viewQ, facts.ov, facts.br, viewQ.Order.Desc, base.ids, baseCard, nil, nil, guard)
	base.ids.Release()
	if fallback {
		t.Fatalf("small no-trace path unexpectedly fell back")
	}

	want := []uint64{20_000, 19_800, 19_600, 19_400, 19_200, 19_000, 18_800, 18_600, 18_400, 18_200}
	if !reflect.DeepEqual(gotFast, want) {
		t.Fatalf("small fast path keys=%v want=%v", gotFast, want)
	}

	got, ok, plan, err := view.executeOrderedLimit(&viewQ, nil)
	if err != nil {
		t.Fatalf("executeOrderedLimit: %v", err)
	}
	if !ok {
		t.Fatalf("executeOrderedLimit: ok=false")
	}
	if plan != PlanLimitOrderBasic {
		t.Fatalf("plan=%s want %s", plan, PlanLimitOrderBasic)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("keys=%v want=%v", got, want)
	}
}

func TestQuery_OrderBasicLimit_BaseCoreRuntimeGuardNoTraceSparseWarmMaterializedBaseFallback(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	const rows = 20_000
	seedGeneratedUint64Data(t, db, rows, func(i int) *Rec {
		email := fmt.Sprintf("miss%05d@example.com", i)
		if i >= 10_001 && i <= 10_256 {
			email = fmt.Sprintf("target%03d@example.com", i-10_000)
		}
		return &Rec{
			Email: email,
			Score: float64(i),
		}
	})
	warmMaterializedPredForLimitTest(t, db, qx.PREFIX("email", "target"))

	q := qx.Query(qx.PREFIX("email", "target")).Sort("score", qx.DESC).Limit(10)
	prepared, viewQ, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer prepared.Release()

	view := db.engine.currentQueryViewForTests()
	facts := orderedLimitFactsPool.Get()
	defer facts.Release()
	ok, err := view.collectOrderedLimitFacts(&viewQ, facts)
	if err != nil {
		t.Fatalf("collectOrderedLimitFacts: %v", err)
	}
	if !ok {
		t.Fatalf("collectOrderedLimitFacts: ok=false")
	}
	decision, ok, err := view.selectOrderedLimit(&viewQ, facts)
	if err != nil {
		t.Fatalf("selectOrderedLimit: %v", err)
	}
	if !ok {
		t.Fatalf("selectOrderedLimit: ok=false")
	}
	if decision.selected.kind != plannerOrderedLimitCandidateWarmBaseCore {
		t.Fatalf("selected=%v want %v", decision.selected.kind, plannerOrderedLimitCandidateWarmBaseCore)
	}
	guard := decision.baseCoreRuntimeGuard(&viewQ, len(facts.baseOps))
	if !guard.enabled {
		t.Fatalf("expected base-core guard")
	}

	base, err := view.evalExpr(viewQ.Expr)
	if err != nil {
		t.Fatalf("evalExpr: %v", err)
	}
	baseCard := base.ids.Cardinality()
	base.ids.Release()
	keyCount := uint64(facts.ov.KeyCount())
	if baseCard != 256 || baseCard > 512 || baseCard <= 128 || keyCount < baseCard*8 {
		t.Fatalf("fixture is not sparseWarm: baseCard=%d keyCount=%d", baseCard, keyCount)
	}

	got, ok, plan, err := view.executeOrderedLimit(&viewQ, nil)
	if err != nil {
		t.Fatalf("executeOrderedLimit: %v", err)
	}
	if !ok {
		t.Fatalf("executeOrderedLimit: ok=false")
	}
	if plan != PlanLimitOrderBasic {
		t.Fatalf("plan=%s want %s", plan, PlanLimitOrderBasic)
	}
	want := []uint64{10_256, 10_255, 10_254, 10_253, 10_252, 10_251, 10_250, 10_249, 10_248, 10_247}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("keys=%v want=%v", got, want)
	}
}

func TestQuery_OrderBasicLimit_BaseCoreRuntimeGuardNoTraceStopsAtCappedBaseNeed(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	const rows = 2_000
	seedGeneratedUint64Data(t, db, rows, func(i int) *Rec {
		return &Rec{
			Age:   i,
			Score: float64(rows - i),
		}
	})

	q := qx.Query(qx.LTE("age", 200)).Sort("score", qx.DESC).Limit(1_000)
	prepared, viewQ, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer prepared.Release()

	view := db.engine.currentQueryViewForTests()
	facts := orderedLimitFactsPool.Get()
	defer facts.Release()
	ok, err := view.collectOrderedLimitFacts(&viewQ, facts)
	if err != nil {
		t.Fatalf("collectOrderedLimitFacts: %v", err)
	}
	if !ok {
		t.Fatalf("collectOrderedLimitFacts: ok=false")
	}

	base, err := view.evalExpr(viewQ.Expr)
	if err != nil {
		t.Fatalf("evalExpr: %v", err)
	}
	defer base.ids.Release()
	baseCard := base.ids.Cardinality()
	if baseCard != 200 {
		t.Fatalf("baseCard=%d want 200", baseCard)
	}
	need, exhausted := boundedWindowCap(baseCard, viewQ.Offset, viewQ.Limit)
	if exhausted || need != baseCard {
		t.Fatalf("need=%d exhausted=%v want need=%d", need, exhausted, baseCard)
	}

	guard := plannerOrderedLimitRuntimeGuard{
		enabled:      true,
		minExamined:  512,
		needWindow:   viewQ.Limit,
		fallbackCost: 1,
		rowCost:      1,
		reason:       "test_base_need_guard",
	}
	out, ok := scanLimitByFieldIndexBoundsPostingFilterNoTrace(&viewQ, facts.ov, facts.br, viewQ.Order.Desc, base.ids, need, guard)
	if !ok {
		t.Fatalf("posting-filter scan fell back after capped base need")
	}
	if len(out) != int(need) {
		t.Fatalf("posting-filter rows=%d want %d", len(out), need)
	}

	guard.minExamined = need
	out, examined, denseOK, fallback := scanOrderLimitDenseBaseNoTrace(&viewQ, facts.ov, facts.br, viewQ.Order.Desc, base.ids, int(baseCard), nil, nil, guard)
	if fallback {
		t.Fatalf("dense base scan fell back after completing result: examined=%d rows=%d", examined, len(out))
	}
	if !denseOK {
		t.Fatalf("dense base scan did not use dense path")
	}
	if len(out) != int(need) {
		t.Fatalf("dense base rows=%d want %d", len(out), need)
	}
}

func TestQuery_OrderBasicLimit_OffsetPromotionUsesFullWindow(t *testing.T) {
	var events []TraceEvent
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
		TraceSampleEvery:                        1,
		TraceSink: func(ev TraceEvent) {
			events = append(events, ev)
		},
	})

	const rows = 50_000
	seedGeneratedUint64Data(t, db, rows, func(i int) *Rec {
		age := 0
		if i > 1_000 {
			age = 100
		}
		return &Rec{
			Name:  fmt.Sprintf("u_%05d", i),
			Age:   age,
			Score: float64(i),
		}
	})

	view := db.engine.currentQueryViewForTests()
	expr := mustLimitQIRExpr(t, db, qx.GTE("age", 100))
	candidate, ok := view.prepareScalarRangeRoutingCandidate(expr)
	if !ok {
		t.Fatalf("prepareScalarRangeRoutingCandidate: ok=false")
	}
	stats, ok := candidate.core.orderBasicMaterializationStats(view.snap.Universe.Cardinality())
	if !ok || stats.cacheKey.IsZero() {
		t.Fatalf("orderBasicMaterializationStats: ok=%v key=%v", ok, stats.cacheKey)
	}

	q := qx.Query(
		qx.GTE("score", 1001.0),
		qx.GTE("age", 100),
	).Sort("score", qx.ASC).Offset(20_000).Limit(10)

	for i := 0; i < 2; i++ {
		got, err := db.QueryKeys(q)
		if err != nil {
			t.Fatalf("QueryKeys #%d: %v", i+1, err)
		}
		want := []uint64{21_001, 21_002, 21_003, 21_004, 21_005, 21_006, 21_007, 21_008, 21_009, 21_010}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("QueryKeys #%d keys=%v want=%v", i+1, got, want)
		}
	}

	if len(events) < 2 {
		t.Fatalf("expected trace events")
	}
	ev := events[len(events)-1]
	if ev.OrderedLimitRoute.Selected != plannerOrderedLimitCandidateOrderScan.String() {
		t.Fatalf("selected=%q want %q route=%+v", ev.OrderedLimitRoute.Selected, plannerOrderedLimitCandidateOrderScan.String(), ev.OrderedLimitRoute)
	}
	if ev.OrderedLimitRoute.RuntimeFallbackTriggered {
		t.Fatalf("unexpected runtime fallback, route=%+v", ev.OrderedLimitRoute)
	}
	if view.snap.HasMaterializedPredKey(stats.cacheKey) {
		t.Fatalf("unexpected materialized residual predicate after offset-only scan work")
	}
}

func baselineScanLimitByFieldIndexBounds(db *View, q *qx.QX, ov indexdata.FieldIndexView, br indexdata.FieldIndexRange, desc bool, preds []leafPred, nilTailField string) []uint64 {
	limit := int(q.Window.Limit)
	out := make([]uint64, 0, limit)
	cursor := newQueryCursor(out, 0, q.Window.Limit, false, 0)
	predCount := 0
	if preds != nil {
		predCount = len(preds)
	}

	emitCandidate := func(idx uint64) bool {
		for i := 0; i < predCount; i++ {
			if !preds[i].containsIdx(idx) {
				return false
			}
		}
		return cursor.emit(idx)
	}

	emitBucketPosting := func(ids posting.List) bool {
		if ids.IsEmpty() {
			return false
		}
		if idx, ok := ids.TrySingle(); ok {
			return emitCandidate(idx)
		}
		it := ids.Iter()
		defer it.Release()
		for it.HasNext() {
			if emitCandidate(it.Next()) {
				return true
			}
		}
		return false
	}

	keyCur := ov.NewCursor(br, desc)
	for {
		_, ids, ok := keyCur.Next()
		if !ok {
			break
		}
		if ids.IsEmpty() {
			continue
		}
		if emitBucketPosting(ids) {
			return cursor.out
		}
	}

	if nilTailField != "" {
		ids := db.fieldIndexViewFromSlotsByName(db.snap.NilIndex, nilTailField).LookupPostingRetained(nilIndexEntryKey)
		if !ids.IsEmpty() && emitBucketPosting(ids) {
			return cursor.out
		}
	}

	return cursor.out
}

func TestQuery_RangeNoOrderWithLimit_DeepOffset_ReturnsValidPage(t *testing.T) {
	db, _ := openTempDBUint64(t)
	seedGeneratedUint64Data(t, db, 10_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("u_%d@example.test", i),
			Age:    i % 64,
			Score:  float64(i),
			Active: i%2 == 0,
		}
	})

	q := qx.Query(qx.GTE("age", 20)).Offset(4_000).Limit(25)

	got, used, err := db.engine.execSelectedNoOrderDirectRange(q, nil)
	if err != nil {
		t.Fatalf("execSelectedNoOrderDirectRange: %v", err)
	}
	if !used {
		t.Fatalf("expected range no-order fast path to be used")
	}

	fullQ := cloneQuery(q)
	fullQ.Window.Offset = 0
	fullQ.Window.Limit = 0
	full, err := db.QueryKeys(fullQ)
	if err != nil {
		t.Fatalf("full QueryKeys: %v", err)
	}
	assertNoOrderPage(t, q, got, full, "range_no_order_deep_offset")
}

func TestQuery_PrefixNoOrderWithLimit_DeepOffset_ReturnsValidPage(t *testing.T) {
	db, _ := openTempDBUint64(t)
	seedGeneratedUint64Data(t, db, 10_000, func(i int) *Rec {
		return &Rec{
			Name:     fmt.Sprintf("u_%d", i),
			Email:    fmt.Sprintf("u_%d@example.test", i),
			Age:      i % 64,
			Score:    float64(i),
			Active:   i%2 == 0,
			FullName: fmt.Sprintf("grp-%02d", i%100),
		}
	})

	q := qx.Query(qx.PREFIX("full_name", "grp-1")).Offset(750).Limit(30)

	got, used, err := db.engine.execSelectedNoOrderDirectPrefix(q, nil)
	if err != nil {
		t.Fatalf("execSelectedNoOrderDirectPrefix: %v", err)
	}
	if !used {
		t.Fatalf("expected prefix no-order fast path to be used")
	}

	fullQ := cloneQuery(q)
	fullQ.Window.Offset = 0
	fullQ.Window.Limit = 0
	full, err := db.QueryKeys(fullQ)
	if err != nil {
		t.Fatalf("full QueryKeys: %v", err)
	}
	assertNoOrderPage(t, q, got, full, "prefix_no_order_deep_offset")
}

func TestQuery_RangeNoOrderWithLimit_NilEQDeepOffset_ReturnsValidPage(t *testing.T) {
	db, _ := openTempDBUint64(t)
	seedGeneratedUint64Data(t, db, 9_000, func(i int) *Rec {
		var opt *string
		if i%3 == 0 {
			v := fmt.Sprintf("v-%d", i%17)
			opt = &v
		}
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Email:  fmt.Sprintf("u_%d@example.test", i),
			Age:    i % 64,
			Score:  float64(i),
			Active: i%2 == 0,
			Opt:    opt,
		}
	})

	q := qx.Query(qx.EQ("opt", nil)).Offset(2_500).Limit(40)

	got, used, err := db.engine.execSelectedNoOrderDirectRange(q, nil)
	if err != nil {
		t.Fatalf("execSelectedNoOrderDirectRange(nil EQ): %v", err)
	}
	if !used {
		t.Fatalf("expected nil-equality range fast path to be used")
	}

	fullQ := cloneQuery(q)
	fullQ.Window.Offset = 0
	fullQ.Window.Limit = 0
	full, err := db.QueryKeys(fullQ)
	if err != nil {
		t.Fatalf("full QueryKeys: %v", err)
	}
	assertNoOrderPage(t, q, got, full, "nil_eq_no_order_deep_offset")
}

func TestQuery_LimitRangeNoOrder_ResidualsUseBucketExactFilter(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	db, _ := openTempDBUint64(t, Options{
		TraceSink: func(ev TraceEvent) {
			mu.Lock()
			events = append(events, ev)
			mu.Unlock()
		},
		TraceSampleEvery: 1,
	})

	seedGeneratedUint64Data(t, db, 12_000, func(i int) *Rec {
		countries := [...]string{"NL", "DE", "US", "GB"}
		group := (i - 1) / 64
		return &Rec{
			Name:     fmt.Sprintf("u_%d", i),
			Email:    fmt.Sprintf("u_%d@example.test", i),
			Age:      i % 64,
			Active:   group%2 == 0,
			Score:    float64(i),
			Meta:     Meta{Country: countries[group%len(countries)]},
			FullName: fmt.Sprintf("grp-%02d", i%64),
		}
	})

	q := qx.Query(
		qx.GTE("age", 0),
		qx.LT("age", 64),
		qx.IN("country", []string{"NL", "DE"}),
		qx.EQ("active", true),
	).Limit(25)

	leaves := mustExtractAndLeaves(t, q.Filter)
	f, bounds, ok, err := db.engine.extractNoOrderBounds(leaves)
	if err != nil {
		t.Fatalf("extractNoOrderBounds: %v", err)
	}
	if !ok {
		t.Fatalf("expected no-order bounds to be recognized")
	}

	preparedQ, viewQ, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer preparedQ.Release()

	tr := db.engine.beginTraceForTests(viewQ)
	if tr == nil {
		t.Fatalf("expected trace to be enabled")
	}
	got, used, err := db.engine.execSelectedNoOrderBounds(q, f, bounds, leaves, tr)
	tr.Finish(uint64(len(got)), err)
	if err != nil {
		t.Fatalf("execSelectedNoOrderBounds: %v", err)
	}
	if !used {
		t.Fatalf("expected range limit fast path to be used")
	}

	mu.Lock()
	if len(events) == 0 {
		mu.Unlock()
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	mu.Unlock()
	if ev.PostingExactFilters == 0 {
		t.Fatalf("expected exact bucket filter usage, trace=%+v", ev)
	}
	if ev.RowsExamined == 0 || ev.RowsMatched == 0 {
		t.Fatalf("expected non-zero trace counters, trace=%+v", ev)
	}

	fullQ := cloneQuery(q)
	fullQ.Window.Offset = 0
	fullQ.Window.Limit = 0
	full, err := db.QueryKeys(fullQ)
	if err != nil {
		t.Fatalf("full QueryKeys: %v", err)
	}
	assertNoOrderPage(t, q, got, full, "range_no_order_bucket_exact")
}

func TestQuery_LimitRangeNoOrder_NoResidualDoesNotTraceOrderScanWidth(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	db, _ := openTempDBUint64(t, Options{
		TraceSink: func(ev TraceEvent) {
			mu.Lock()
			events = append(events, ev)
			mu.Unlock()
		},
		TraceSampleEvery: 1,
	})

	seedGeneratedUint64Data(t, db, 512, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Age:    i,
			Score:  float64(i),
			Active: i%2 == 0,
		}
	})

	q := qx.Query(
		qx.GTE("age", 100),
		qx.LT("age", 400),
	).Limit(25)

	leaves := mustExtractAndLeaves(t, q.Filter)
	f, bounds, ok, err := db.engine.extractNoOrderBounds(leaves)
	if err != nil {
		t.Fatalf("extractNoOrderBounds: %v", err)
	}
	if !ok {
		t.Fatalf("expected no-order bounds to be recognized")
	}

	preparedQ, viewQ, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer preparedQ.Release()

	tr := db.engine.beginTraceForTests(viewQ)
	if tr == nil {
		t.Fatalf("expected trace to be enabled")
	}
	got, used, err := db.engine.execSelectedNoOrderBounds(q, f, bounds, leaves, tr)
	tr.Finish(uint64(len(got)), err)
	if err != nil {
		t.Fatalf("execSelectedNoOrderBounds: %v", err)
	}
	if !used {
		t.Fatalf("expected range limit fast path to be used")
	}
	if len(got) == 0 {
		t.Fatalf("expected non-empty result")
	}

	mu.Lock()
	if len(events) == 0 {
		mu.Unlock()
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	mu.Unlock()
	if ev.RowsExamined == 0 || ev.RowsMatched == 0 {
		t.Fatalf("expected non-zero row counters, trace=%+v", ev)
	}
	if ev.OrderIndexScanWidth != 0 {
		t.Fatalf("expected no order-index scan width for no-order bounds, trace=%+v", ev)
	}
}

func TestQuery_LimitOrderBasic_ResidualsUseBucketExactFilter(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	db, _ := openTempDBUint64(t, Options{
		TraceSink: func(ev TraceEvent) {
			mu.Lock()
			events = append(events, ev)
			mu.Unlock()
		},
		TraceSampleEvery: 1,
	})

	seedGeneratedUint64Data(t, db, 12_000, func(i int) *Rec {
		countries := [...]string{"NL", "DE", "US", "GB"}
		group := (i - 1) / 64
		return &Rec{
			Name:     fmt.Sprintf("u_%d", i),
			Email:    fmt.Sprintf("u_%d@example.test", i),
			Age:      i % 64,
			Active:   group%2 == 0,
			Score:    float64(i),
			Meta:     Meta{Country: countries[group%len(countries)]},
			FullName: fmt.Sprintf("grp-%02d", i%64),
		}
	})

	q := qx.Query(
		qx.GTE("age", 0),
		qx.LT("age", 64),
		qx.EQ("active", true),
		qx.EQ("country", "NL"),
	).Sort("age", qx.ASC).Limit(20)

	view := db.engine.currentQueryViewForTests()
	qirLeaves := mustLimitQIRLeaves(t, db, q.Filter)
	bounds, ok, err := view.extractBoundsForField("age", qirLeaves)
	if err != nil {
		t.Fatalf("extractBoundsForField: %v", err)
	}
	if !ok {
		t.Fatalf("expected order bounds to be recognized")
	}

	predsBuf, ok, err := view.buildLeafPredsExcludingBounds(qirLeaves, "age", view.fieldOrdinalByName("age"), 0)
	if err != nil {
		t.Fatalf("buildLeafPredsExcludingBounds: %v", err)
	}
	if !ok {
		t.Fatalf("expected residual leaf preds to be supported")
	}
	if predsBuf != nil {
		defer leafPredSlicePool.Put(predsBuf)
	}

	ov := view.fieldIndexViewFromSlotsByName(view.snap.Index, "age")
	br := ov.RangeForBounds(bounds)
	want := baselineScanLimitByFieldIndexBounds(view, q, ov, br, false, predsBuf, "")

	preparedQ, viewQ, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer preparedQ.Release()

	tr := db.engine.beginTraceForTests(viewQ)
	if tr == nil {
		t.Fatalf("expected trace to be enabled")
	}
	got, used, err := db.engine.executeOrderedLimit(q, tr)
	tr.Finish(uint64(len(got)), err)
	if err != nil {
		t.Fatalf("executeOrderedLimit: %v", err)
	}
	if !used {
		t.Fatalf("expected order-basic limit fast path to be used")
	}
	assertSameSlice(t, got, want)

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	ev := events[len(events)-1]
	if ev.PostingExactFilters == 0 {
		t.Fatalf("expected exact bucket filter usage, trace=%+v", ev)
	}
	if ev.OrderIndexScanWidth == 0 {
		t.Fatalf("expected non-zero order scan width, trace=%+v", ev)
	}
}

func TestQuery_LimitBoundExcludedMergePreservesSameFieldResidual(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	seedGeneratedUint64Data(t, db, 200, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Age:    i % 20,
			Score:  float64(i),
			Active: true,
		}
	})

	ordered := qx.Query(
		qx.GTE("age", 10),
		qx.EQ("age", 15),
	).Sort("age", qx.ASC).Limit(6)
	got, used, err := db.engine.executeOrderedLimit(ordered, nil)
	if err != nil {
		t.Fatalf("ordered executeOrderedLimit: %v", err)
	}
	if !used {
		t.Fatalf("expected ordered range/order limit path")
	}
	if len(got) != 6 {
		t.Fatalf("ordered residual page len=%d want 6, ids=%v", len(got), got)
	}
	for _, id := range got {
		if id%20 != 15 {
			t.Fatalf("ordered path dropped same-field EQ residual: ids=%v", got)
		}
	}

	contradictory := qx.Query(
		qx.GTE("age", 10),
		qx.EQ("age", 5),
	).Sort("age", qx.ASC).Limit(6)
	got, used, err = db.engine.executeOrderedLimit(contradictory, nil)
	if err != nil {
		t.Fatalf("contradictory executeOrderedLimit: %v", err)
	}
	if !used {
		t.Fatalf("expected ordered contradictory range/order limit path")
	}
	if len(got) != 0 {
		t.Fatalf("contradictory same-field range returned ids=%v", got)
	}

	noOrder := qx.Query(
		qx.GTE("age", 10),
		qx.EQ("age", 15),
	).Limit(6)
	view := db.engine.currentQueryViewForTests()
	qirLeaves := mustLimitQIRLeaves(t, db, noOrder.Filter)
	bounds, ok, err := view.extractBoundsForField("age", qirLeaves)
	if err != nil {
		t.Fatalf("extractBoundsForField: %v", err)
	}
	if !ok {
		t.Fatalf("expected age bounds")
	}
	got, used, err = db.engine.execSelectedNoOrderBounds(noOrder, "age", bounds, []qx.Expr{
		qx.GTE("age", 10),
		qx.EQ("age", 15),
	}, nil)
	if err != nil {
		t.Fatalf("no-order execSelectedNoOrderBounds: %v", err)
	}
	if !used {
		t.Fatalf("expected no-order range limit path")
	}
	if len(got) != 6 {
		t.Fatalf("no-order residual page len=%d want 6, ids=%v", len(got), got)
	}
	for _, id := range got {
		if id%20 != 15 {
			t.Fatalf("no-order path dropped same-field EQ residual: ids=%v", got)
		}
	}
}

func TestQuery_OrderBasicLimit_DeclinesNegatedCompositeShape(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	seedGeneratedUint64Data(t, db, 30, func(i int) *Rec {
		return &Rec{
			Name: fmt.Sprintf("u_%d", i),
			Age:  i,
		}
	})

	prepared, shape, err := prepareTestQuery(db.engine, qx.Query(
		qx.GTE("age", 10),
		qx.LTE("age", 20),
	).Sort("age", qx.ASC).Limit(6))
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer prepared.Release()

	shape.Expr.Not = true
	view := db.engine.currentQueryViewForTests()
	if out, ok, plan, err := view.executeOrderedLimit(&shape, nil); err != nil || ok {
		t.Fatalf("executeOrderedLimit out=%v ok=%v plan=%v err=%v, want selector decline", out, ok, plan, err)
	}

	got, err := view.Query(&shape, false)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	want := []uint64{1, 2, 3, 4, 5, 6}
	assertSameSlice(t, got, want)
}

func TestQuery_OrderBasicLimit_PreservesInterleavedResidual(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	seedGeneratedUint64Data(t, db, 40, func(i int) *Rec {
		tags := []string{"drop"}
		if i >= 20 && i <= 25 {
			tags = []string{"keep"}
		}
		return &Rec{
			Name: fmt.Sprintf("u_%d", i),
			Age:  i,
			Tags: tags,
		}
	})

	ageGTE := mustLimitQIRExpr(t, db, qx.GTE("age", 10))
	tagsEQ := mustLimitQIRExpr(t, db, qx.EQ("tags", []string{"keep"}))
	ageLTE := mustLimitQIRExpr(t, db, qx.LTE("age", 25))

	prepared, shape, err := prepareTestQuery(db.engine, qx.Query().Sort("age", qx.ASC).Limit(5))
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer prepared.Release()

	shape.Expr = qir.Expr{
		Op:           qir.OpAND,
		FieldOrdinal: qir.NoFieldOrdinal,
		Operands:     []qir.Expr{ageGTE, tagsEQ, ageLTE},
	}

	got, err := db.engine.currentQueryViewForTests().Query(&shape, false)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	want := []uint64{20, 21, 22, 23, 24}
	assertSameSlice(t, got, want)
}

func TestQuery_OrderBasicLimit_ValidatesResidualBeforeEmptyOrderRange(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	seedGeneratedUint64Data(t, db, 20, func(i int) *Rec {
		return &Rec{
			Name: fmt.Sprintf("u_%d", i),
			Age:  i,
		}
	})

	for _, offset := range []int{0, 1} {
		q := qx.Query(
			qx.GTE("age", 10),
			qx.LTE("age", 5),
			qx.HASALL("name", []string{"u_1"}),
		).Sort("age", qx.ASC).Offset(offset).Limit(5)

		_, err := db.QueryKeys(q)
		if !errors.Is(err, ErrInvalidQuery) {
			t.Fatalf("offset=%d err=%v, want ErrInvalidQuery", offset, err)
		}
	}
}

func TestQuery_OrderBasic_RangeBaseOpsMaterializeBroadComplementWithoutExactSiblings(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})

	setNumericBucketKnobs(t, db, 128, 1, 1)
	seedGeneratedUint64Data(t, db, 5_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Age:    i,
			Score:  float64(i),
			Active: true,
		}
	})
	if err := db.Patch(1, []Field{{Name: "age", Value: 10_000}}); err != nil {
		t.Fatalf("Patch(age): %v", err)
	}

	if got := db.engine.snapshot.Current().MaterializedPredCacheEntryCount(); got != 0 {
		t.Fatalf("unexpected materialized predicate cache before query: %d", got)
	}

	q := qx.Query(
		qx.LT("score", 4_000.0),
	).Sort("age", qx.ASC).Limit(5)

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertSameSlice(t, got, want)

	after := db.engine.snapshot.Current()
	if got := after.MaterializedPredCacheEntryCount(); got == 0 {
		t.Fatalf("expected ordered predicate path to materialize broad complement, cache entries=%d", got)
	}
}

func TestQuery_OrderBasic_SmallAndDeepWindowMaterializeNonOrderNumericRangeWhenCostWins(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})

	setNumericBucketKnobs(t, db, 128, 1, 1)
	seedGeneratedUint64Data(t, db, 5_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Age:    i,
			Score:  float64(i),
			Active: true,
		}
	})

	small := qx.Query(
		qx.LT("score", 4_000.0),
	).Sort("age", qx.ASC).Limit(5)
	if _, err := db.QueryKeys(small); err != nil {
		t.Fatalf("small QueryKeys: %v", err)
	}
	if got := db.engine.snapshot.Current().MaterializedPredCacheEntryCount(); got == 0 {
		t.Fatalf("expected materialized predicate cache for small ordered window: %d", got)
	}

	deep := qx.Query(
		qx.LT("score", 4_000.0),
	).Sort("age", qx.ASC).Offset(2_000).Limit(10)
	got, err := db.QueryKeys(deep)
	if err != nil {
		t.Fatalf("deep QueryKeys: %v", err)
	}
	want, err := expectedKeysUint64(t, db, deep)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertSameSlice(t, got, want)

	if got := db.engine.snapshot.Current().MaterializedPredCacheEntryCount(); got == 0 {
		t.Fatalf("expected deep ordered window to keep materialized numeric range predicate")
	}
}

func TestQuery_OrderBasic_BuildLeafPredsExcludingBounds_MaterializesBroadComplementOnFirstSightWhenCostWins(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})

	setNumericBucketKnobs(t, db, 128, 1, 1)
	seedGeneratedUint64Data(t, db, 64, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   18 + i,
			Score: float64(i),
		}
	})

	q := qx.Query(
		qx.GTE("score", 0.0),
		qx.GTE("age", 40),
	).Sort("score", qx.ASC).Limit(50)
	leaves := mustLimitQIRLeaves(t, db, q.Filter)

	view := db.engine.currentQueryViewForTests()

	expr := mustLimitQIRExpr(t, db, qx.GTE("age", 40))
	bound, isSlice, err := view.exprValueToNormalizedScalarBound(expr)
	if err != nil {
		t.Fatalf("exprValueToNormalizedScalarBound: %v", err)
	}
	if isSlice || bound.full || bound.empty {
		t.Fatalf("unexpected normalized bound state: slice=%v full=%v empty=%v", isSlice, bound.full, bound.empty)
	}
	cacheKey := view.materializedPredComplementKeyForNormalizedScalarBound("age", bound)
	if cacheKey.IsZero() {
		t.Fatalf("expected non-empty complement cache key")
	}

	preds1, ok, err := view.buildLeafPredsExcludingBounds(leaves, "score", view.fieldOrdinalByName("score"), 4096)
	if err != nil {
		t.Fatalf("first buildLeafPredsExcludingBounds: %v", err)
	}
	if !ok {
		t.Fatalf("expected first residual leaf preds to be supported")
	}
	if preds1 == nil || len(preds1) != 1 {
		t.Fatalf("unexpected first predicate count: %d", len(preds1))
	}
	first := preds1[0]
	if first.kind != leafPredKindPredicate {
		leafPredSlicePool.Put(preds1)
		t.Fatalf("expected predicate leaf, got kind=%v", first.kind)
	}
	if first.pred.kind != predicateKindMaterializedNot {
		leafPredSlicePool.Put(preds1)
		t.Fatalf("expected first ordered broad complement to materialize, got kind=%v", first.pred.kind)
	}
	leafPredSlicePool.Put(preds1)
	if _, ok = snapshotExtLoadMaterializedPred(db.engine.snapshot.Current(), cacheKey); !ok {
		t.Fatalf("expected shared complement cache entry after first ordered leaf build")
	}

	preds2, ok, err := view.buildLeafPredsExcludingBounds(leaves, "score", view.fieldOrdinalByName("score"), 4096)
	if err != nil {
		t.Fatalf("second buildLeafPredsExcludingBounds: %v", err)
	}
	if !ok {
		t.Fatalf("expected second residual leaf preds to be supported")
	}
	if preds2 == nil || len(preds2) != 1 {
		t.Fatalf("unexpected second predicate count: %d", len(preds2))
	}
	defer leafPredSlicePool.Put(preds2)
	second := preds2[0]
	if second.kind != leafPredKindPredicate {
		t.Fatalf("expected predicate leaf, got kind=%v", second.kind)
	}
	if second.pred.kind != predicateKindMaterializedNot {
		t.Fatalf("expected second ordered broad complement to materialize, got kind=%v", second.pred.kind)
	}
	if _, ok := snapshotExtLoadMaterializedPred(db.engine.snapshot.Current(), cacheKey); !ok {
		t.Fatalf("expected shared complement cache entry after second ordered leaf build")
	}
}

func TestQuery_OrderBasic_BuildLeafPredsExcludingBounds_DelaysBroadComplementWithMultipleExactSiblings(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})

	setNumericBucketKnobs(t, db, 128, 1, 1)
	seedGeneratedUint64Data(t, db, 256, func(i int) *Rec {
		country := "US"
		if i%3 == 0 {
			country = "DE"
		}
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Age:    18 + i,
			Score:  float64(i),
			Active: i%2 == 0,
			Meta: Meta{
				Country: country,
			},
		}
	})

	q := qx.Query(
		qx.EQ("active", true),
		qx.EQ("country", "DE"),
		qx.GTE("age", 40),
	).Sort("score", qx.ASC).Limit(50)
	leaves := mustLimitQIRLeaves(t, db, q.Filter)

	view := db.engine.currentQueryViewForTests()

	expr := mustLimitQIRExpr(t, db, qx.GTE("age", 40))
	bound, isSlice, err := view.exprValueToNormalizedScalarBound(expr)
	if err != nil {
		t.Fatalf("exprValueToNormalizedScalarBound: %v", err)
	}
	if isSlice || bound.full || bound.empty {
		t.Fatalf("unexpected normalized bound state: slice=%v full=%v empty=%v", isSlice, bound.full, bound.empty)
	}
	cacheKey := view.materializedPredComplementKeyForNormalizedScalarBound("age", bound)
	if cacheKey.IsZero() {
		t.Fatalf("expected non-empty complement cache key")
	}

	preds, ok, err := view.buildLeafPredsExcludingBounds(leaves, "score", view.fieldOrdinalByName("score"), 4096)
	if err != nil {
		t.Fatalf("buildLeafPredsExcludingBounds: %v", err)
	}
	if !ok {
		t.Fatalf("expected residual leaf preds to be supported")
	}
	if preds == nil || len(preds) != 3 {
		t.Fatalf("unexpected predicate count: %d", len(preds))
	}
	defer leafPredSlicePool.Put(preds)

	rangeIdx := -1
	exactSiblingCount := 0
	for i := 0; i < len(preds); i++ {
		p := preds[i]
		if p.kind == leafPredKindPredicate && testExprFieldName(db.engine, p.pred.expr) == "age" && p.pred.expr.Op == qir.OpGTE {
			rangeIdx = i
			continue
		}
		if p.supportsExactBucketPostingFilter() {
			exactSiblingCount++
		}
	}
	if rangeIdx < 0 {
		t.Fatalf("expected age range leaf")
	}
	if exactSiblingCount < 2 {
		t.Fatalf("expected multiple exact siblings, got %d", exactSiblingCount)
	}

	rangePred := preds[rangeIdx]
	if rangePred.kind != leafPredKindPredicate {
		t.Fatalf("expected predicate leaf, got kind=%v", rangePred.kind)
	}
	if rangePred.pred.kind == predicateKindMaterializedNot {
		t.Fatalf("expected broad complement to stay delayed with multiple exact siblings")
	}
	if _, ok := snapshotExtLoadMaterializedPred(db.engine.snapshot.Current(), cacheKey); ok {
		t.Fatalf("unexpected shared complement cache entry on first build with multiple exact siblings")
	}
}

func TestQuery_OrderBasic_BuildLeafPredsExcludingBounds_ForceMaterializesNonBroadNullableComplement(t *testing.T) {
	db, _ := openTempDBUint64PtrInt(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})
	for i := 0; i < 64; i++ {
		rec := &PtrIntRec{Name: fmt.Sprintf("nil_%02d", i), Rank: nil, Active: i%2 == 0}
		if err := db.Set(uint64(i+1), rec); err != nil {
			t.Fatalf("Set(nil_%02d): %v", i, err)
		}
	}
	for i := 0; i < 40; i++ {
		v := 0
		rec := &PtrIntRec{Name: fmt.Sprintf("zero_%02d", i), Rank: &v, Active: i%2 == 0}
		if err := db.Set(uint64(i+65), rec); err != nil {
			t.Fatalf("Set(zero_%02d): %v", i, err)
		}
	}
	for i := 1; i <= 9; i++ {
		v := i
		rec := &PtrIntRec{Name: fmt.Sprintf("rank_%02d", i), Rank: &v, Active: true}
		if err := db.Set(uint64(i+105), rec); err != nil {
			t.Fatalf("Set(rank_%02d): %v", i, err)
		}
	}

	q := qx.Query(
		qx.GTE("name", ""),
		qx.LT("name", "~"),
		qx.GTE("rank", 1),
	).Sort("name", qx.ASC).Limit(5)
	leaves := mustLimitQIRLeaves(t, db, q.Filter)
	window, ok := orderWindowForTest(q)
	if !ok || window <= 0 {
		t.Fatalf("expected ordered window")
	}

	view := db.engine.currentQueryViewForTests()

	expr := mustLimitQIRExpr(t, db, qx.GTE("rank", 1))
	route := view.orderedPredicateScalarRangeRouting(
		predicate{expr: expr},
		window,
		0,
		view.snap.Universe.Cardinality(),
	)
	if !route.forceComplement {
		t.Fatalf("expected nullable ordered route to force complement materialization")
	}
	if route.broadComplement {
		t.Fatalf("expected nullable ordered route to stay non-broad by row cardinality")
	}

	preds, ok, err := view.buildLeafPredsExcludingBounds(leaves, "name", view.fieldOrdinalByName("name"), window)
	if err != nil {
		t.Fatalf("buildLeafPredsExcludingBounds: %v", err)
	}
	if !ok {
		t.Fatalf("expected residual leaf preds to be supported")
	}
	predCount := 0
	if preds != nil {
		predCount = len(preds)
	}
	if preds == nil || predCount != 1 {
		if preds != nil {
			leafPredSlicePool.Put(preds)
		}
		t.Fatalf("unexpected predicate count: %d", predCount)
	}
	defer leafPredSlicePool.Put(preds)

	p := preds[0]
	if p.kind != leafPredKindPredicate {
		t.Fatalf("expected predicate leaf, got kind=%v", p.kind)
	}
	if p.pred.kind == predicateKindMaterializedNot || !p.pred.rangeMat {
		t.Fatalf("expected nil-heavy forced nullable ordered rewrite to materialize positive side, got kind=%v rangeMat=%v", p.pred.kind, p.pred.rangeMat)
	}
	if p.pred.hasRuntimeRangeState() {
		t.Fatalf("expected forced nullable complement rewrite to avoid runtime range state")
	}
	if p.pred.matches(1) || p.pred.matches(3) {
		t.Fatalf("nil rank rows must not match forced nullable ordered range")
	}
	if !p.pred.matches(106) {
		t.Fatalf("expected in-range row to match forced nullable ordered range")
	}

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	want := []uint64{106, 107, 108, 109, 110}
	assertSameSlice(t, got, want)
}

func TestQuery_OrderBasic_BuildLimitLeafPred_NullableComplementUsesPositiveProbeCosts(t *testing.T) {
	db, _ := openTempDBUint64PtrInt(t, Options{
		AnalyzeInterval: -1,
	})
	for i := 0; i < 4096; i++ {
		rec := &PtrIntRec{Name: fmt.Sprintf("nil_%04d", i), Rank: nil, Active: i%2 == 0}
		if err := db.Set(uint64(i+1), rec); err != nil {
			t.Fatalf("Set(nil_%04d): %v", i, err)
		}
	}
	v0 := 0
	if err := db.Set(4097, &PtrIntRec{Name: "zero", Rank: &v0, Active: true}); err != nil {
		t.Fatalf("Set(zero): %v", err)
	}
	for i := 0; i < 100; i++ {
		v := 1
		if err := db.Set(uint64(4098+i), &PtrIntRec{Name: fmt.Sprintf("one_%03d", i), Rank: &v, Active: true}); err != nil {
			t.Fatalf("Set(one_%03d): %v", i, err)
		}
	}
	for i := 0; i < 100; i++ {
		v := 2
		if err := db.Set(uint64(4198+i), &PtrIntRec{Name: fmt.Sprintf("two_%03d", i), Rank: &v, Active: true}); err != nil {
			t.Fatalf("Set(two_%03d): %v", i, err)
		}
	}

	view := db.engine.currentQueryViewForTests()

	expr := mustLimitQIRExpr(t, db, qx.GTE("rank", 1))
	candidate, ok := view.prepareScalarRangeRoutingCandidate(expr)
	if !ok {
		t.Fatalf("expected nullable range routing candidate")
	}
	if !candidate.plan.useComplement {
		t.Fatalf("expected complement route for nullable ordered range")
	}
	if candidate.plan.runtimeProbeBuckets != candidate.plan.bucketCount {
		t.Fatalf("runtimeProbeBuckets=%d want positive bucketCount=%d", candidate.plan.runtimeProbeBuckets, candidate.plan.bucketCount)
	}
	if candidate.plan.runtimeProbeEst != candidate.plan.est {
		t.Fatalf("runtimeProbeEst=%d want positive est=%d", candidate.plan.runtimeProbeEst, candidate.plan.est)
	}
	if candidate.plan.orderedEagerMaterializeUseful(5, view.snap.Universe.Cardinality()) {
		t.Fatalf("expected ordered eager materialization to stay disabled when positive runtime probe is cheaper")
	}

	lp, ok, err := view.buildLimitLeafPred(expr, 5)
	if err != nil {
		t.Fatalf("buildLimitLeafPred: %v", err)
	}
	if !ok {
		t.Fatalf("expected buildLimitLeafPred to support nullable range")
	}
	if lp.kind != leafPredKindPredicate {
		t.Fatalf("expected predicate leaf, got kind=%v", lp.kind)
	}
	if lp.pred.isMaterializedLike() {
		t.Fatalf("expected nullable ordered limit leaf to stay deferred")
	}
	if !lp.pred.hasRuntimeRangeState() {
		t.Fatalf("expected nullable ordered limit leaf to keep runtime range state")
	}
	if lp.pred.matches(1) || lp.pred.matches(2048) || lp.pred.matches(4097) {
		t.Fatalf("nil or out-of-range rows must not match deferred nullable range")
	}
	if !lp.pred.matches(4098) || !lp.pred.matches(4297) {
		t.Fatalf("expected in-range rows to match deferred nullable range")
	}
}

func TestQuery_OrderBasic_NullablePositiveRangeCheapFilterUsesRuntimeProbe(t *testing.T) {
	db, _ := openTempDBUint64PtrInt(t, Options{
		AnalyzeInterval: -1,
	})
	for i := 0; i < 32; i++ {
		if err := db.Set(uint64(i+1), &PtrIntRec{Name: fmt.Sprintf("nil_%02d", i), Rank: nil, Active: true}); err != nil {
			t.Fatalf("Set(nil_%02d): %v", i, err)
		}
	}
	for i := 0; i < 2000; i++ {
		v := i
		if err := db.Set(uint64(i+33), &PtrIntRec{Name: fmt.Sprintf("rank_%04d", i), Rank: &v, Active: true}); err != nil {
			t.Fatalf("Set(rank_%04d): %v", i, err)
		}
	}

	view := db.engine.currentQueryViewForTests()
	expr := mustLimitQIRExpr(t, db, qx.GTE("rank", 1))
	candidate, ok := view.prepareScalarRangeRoutingCandidate(expr)
	if !ok {
		t.Fatalf("expected nullable range routing candidate")
	}
	if !candidate.plan.useComplement {
		t.Fatalf("expected complement plan for broad nullable positive range")
	}
	if candidate.plan.runtimeProbeBuckets != candidate.plan.bucketCount {
		t.Fatalf("runtimeProbeBuckets=%d want positive bucketCount=%d", candidate.plan.runtimeProbeBuckets, candidate.plan.bucketCount)
	}
	if candidate.plan.runtimeProbeBuckets <= rangePostingFilterKeepProbeMaxBuckets {
		t.Fatalf("runtimeProbeBuckets=%d must exceed cheap keep threshold", candidate.plan.runtimeProbeBuckets)
	}
	universe := view.snap.Universe.Cardinality()
	if candidate.plan.orderedEagerMaterializeUseful(5, universe) {
		t.Fatalf("expected eager materialization to stay disabled")
	}
	if view.orderedLimitScalarRangeHasCheapBucketFilter(expr, indexdata.Bounds{}, false, 5, universe) {
		t.Fatalf("nullable positive runtime range probe must not be priced as cheap complement filter")
	}
}

func TestQuery_OrderBasic_DeepWindowCachePersistsAcrossUnchangedFieldPatch(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})

	setNumericBucketKnobs(t, db, 128, 1, 1)
	seedGeneratedUint64Data(t, db, 5_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Age:    i,
			Score:  float64(i),
			Active: true,
		}
	})

	q := qx.Query(
		qx.LT("score", 4_000.0),
	).Sort("age", qx.ASC).Offset(2_000).Limit(10)
	if _, err := db.QueryKeys(q); err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}

	cacheKey := db.engine.materializedPredCacheKey(qx.LT("score", 4_000.0))
	if cacheKey.IsZero() {
		t.Fatalf("expected score range cache key")
	}
	prevSnap := db.engine.snapshot.Current()
	prevBM, ok := snapshotExtLoadMaterializedPred(prevSnap, cacheKey)
	if !ok || prevBM.IsEmpty() {
		t.Fatalf("expected score range cache entry before unrelated patch")
	}

	if err := db.Patch(1, []Field{{Name: "active", Value: false}}); err != nil {
		t.Fatalf("Patch(active): %v", err)
	}

	nextSnap := db.engine.snapshot.Current()
	nextBM, ok := snapshotExtLoadMaterializedPred(nextSnap, cacheKey)
	if !ok || nextBM.IsEmpty() {
		t.Fatalf("expected score range cache entry after unrelated patch")
	}
	if nextBM != prevBM {
		t.Fatalf("expected unchanged-field cache entry to be inherited across snapshot publish")
	}
}

func TestQuery_OrderBasic_ComplementCachedBaseOpCountsAsMaterialized(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})

	seedGeneratedUint64Data(t, db, 5_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Age:    18 + (i % 60),
			Score:  float64(i),
			Active: true,
		}
	})

	q := qx.Query(
		qx.GTE("age", 25),
		qx.LTE("age", 40),
		qx.GT("score", 0.5),
	).Sort("score", qx.DESC).Limit(100)
	if _, err := db.QueryKeys(q); err != nil {
		t.Fatalf("warm QueryKeys: %v", err)
	}

	fillView := db.engine.currentQueryViewForTests()
	lteExpr := mustLimitQIRExpr(t, db, qx.LTE("age", 40))
	lteCacheKey := fillView.materializedPredKey(lteExpr)
	lteIDs := fillView.evalLazyMaterializedPredicateWithKey(lteExpr, lteCacheKey)
	lteIDs.Release()

	view := db.engine.currentQueryViewForTests()
	gteBound, isSlice, err := view.exprValueToNormalizedScalarBound(mustLimitQIRExpr(t, db, qx.GTE("age", 25)))
	if err != nil || isSlice {
		t.Fatalf("exprValueToNormalizedScalarBound(GTE age): err=%v isSlice=%v", err, isSlice)
	}
	lteBound, isSlice, err := view.exprValueToNormalizedScalarBound(mustLimitQIRExpr(t, db, qx.LTE("age", 40)))
	if err != nil || isSlice {
		t.Fatalf("exprValueToNormalizedScalarBound(LTE age): err=%v isSlice=%v", err, isSlice)
	}
	gteComplementKey := view.materializedPredComplementKeyForNormalizedScalarBound("age", gteBound)
	lteScalarKey := view.materializedPredKeyForNormalizedScalarBound("age", lteBound)
	complementSeed := posting.List{}.BuildAdded(1)
	defer complementSeed.Release()
	snapshotExtStoreMaterializedPred(db.engine.snapshot.Current(), gteComplementKey, complementSeed)
	leaves := mustLimitQIRLeaves(t, db, q.Filter)
	baseOps := filterQIRLeavesByField(db, leaves, "score")
	coresBuf, rawCoreIdxBuf := mustPrepareOrderBasicBaseCoresForTest(t, view, baseOps)
	defer orderBasicBaseCoreSlicePool.Put(coresBuf)
	defer pooled.ReleaseIntSlice(rawCoreIdxBuf)
	if !hasWarmOrderBasicBaseCoresForTest(view, coresBuf) {
		_, gteHit := snapshotExtLoadMaterializedPred(db.engine.snapshot.Current(), gteComplementKey)
		_, lteHit := snapshotExtLoadMaterializedPred(db.engine.snapshot.Current(), lteScalarKey)
		t.Fatalf("expected complement-backed cached range base op to count as materialized: gteComplementHit=%v lteScalarHit=%v", gteHit, lteHit)
	}
}

func TestQuery_OrderBasic_EvalRawBaseOpMaterializesPlannedComplement(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
		NumericRangeBucketSize:                  8,
		NumericRangeBucketMinFieldKeys:          16,
		NumericRangeBucketMinSpanKeys:           4,
	})

	seedGeneratedUint64Data(t, db, 1_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Age:    i,
			Score:  float64(i),
			Active: true,
		}
	})

	view := db.engine.currentQueryViewForTests()
	op := mustLimitQIRExpr(t, db, qx.LT("score", 800.0))
	stats, ok := view.orderBasicRawBaseOpStats(op, view.snap.Universe.Cardinality())
	if !ok || !stats.buildComplement {
		t.Fatalf("expected complement raw base op stats: ok=%v stats=%+v", ok, stats)
	}

	got, err := view.evalOrderBasicRawBaseOp(op)
	if err != nil {
		t.Fatalf("evalOrderBasicRawBaseOp: %v", err)
	}
	defer got.ids.Release()

	if !got.neg {
		t.Fatalf("expected complement-backed raw base op to return negative postingResult")
	}
	if got.ids.Cardinality() != stats.buildEst {
		t.Fatalf("complement cardinality=%d want=%d", got.ids.Cardinality(), stats.buildEst)
	}
	cached, ok := view.snap.LoadMaterializedPredKey(stats.cacheKey)
	if !ok {
		t.Fatalf("expected complement cache entry")
	}
	cached.Release()
}

func TestQuery_OrderBasic_WarmQueryLoadsCollapsedNumericRangeSpan(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 0,
		NumericRangeBucketSize:                  8,
		NumericRangeBucketMinFieldKeys:          16,
		NumericRangeBucketMinSpanKeys:           4,
	})

	seedGeneratedUint64Data(t, db, 5_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Age:    18 + (i % 60),
			Score:  float64(i),
			Active: true,
		}
	})

	q := qx.Query(
		qx.GTE("age", 25),
		qx.LTE("age", 40),
		qx.GT("score", 0.5),
	).Sort("score", qx.DESC).Limit(100)

	view := db.engine.currentQueryViewForTests()
	leaves := mustLimitQIRLeaves(t, db, q.Filter)
	baseOps := filterQIRLeavesByField(db, leaves, "score")
	coresBuf, rawCoreIdxBuf := mustPrepareOrderBasicBaseCoresForTest(t, view, baseOps)
	defer orderBasicBaseCoreSlicePool.Put(coresBuf)
	defer pooled.ReleaseIntSlice(rawCoreIdxBuf)
	collapsed := mustFindCollapsedOrderBasicBaseCoreForTest(t, coresBuf)
	view.promoteOrderBasicLimitMaterializedBaseOps("score", baseOps, 250, 100)
	spanHit, ok := view.loadWarmOrderBasicBaseCore(collapsed)
	if !ok {
		t.Fatalf("expected collapsed numeric range span to be directly reusable")
	}
	spanHit.ids.Release()
	if !hasWarmOrderBasicBaseCoresForTest(view, coresBuf) {
		t.Fatalf("expected collapsed numeric range span to be reusable as warm order-basic base op")
	}
}

func mustPrepareOrderBasicBaseCoresForTest(
	t *testing.T,
	view *View,
	baseOps []qir.Expr,
) ([]orderBasicBaseCore, []int) {
	t.Helper()
	coresBuf, rawCoreIdxBuf, noMatch, err := view.prepareOrderBasicBaseCores(baseOps)
	if err != nil {
		t.Fatalf("prepareOrderBasicBaseCores: %v", err)
	}
	if noMatch {
		t.Fatalf("prepareOrderBasicBaseCores: unexpected no-match")
	}
	return coresBuf, rawCoreIdxBuf
}

func hasWarmOrderBasicBaseCoresForTest(view *View, cores []orderBasicBaseCore) bool {
	_, hit, ok := view.loadFirstWarmOrderBasicBaseCore(cores)
	if ok {
		hit.ids.Release()
	}
	return ok
}

func mustFindCollapsedOrderBasicBaseCoreForTest(
	t *testing.T,
	coresBuf []orderBasicBaseCore,
) orderBasicBaseCore {
	t.Helper()
	if coresBuf == nil {
		t.Fatalf("expected prepared order-basic base cores")
	}
	var (
		found orderBasicBaseCore
		hit   bool
	)
	for i := 0; i < len(coresBuf); i++ {
		core := coresBuf[i]
		if core.kind != orderBasicBaseCoreCollapsedRange {
			continue
		}
		if hit {
			t.Fatalf("expected exactly one collapsed order-basic base core")
		}
		found = core
		hit = true
	}
	if !hit {
		t.Fatalf("expected collapsed order-basic base core")
	}
	return found
}

func TestQuery_OrderBasic_WarmQueryPromotesMaterializedRangeBaseOps(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})

	seedGeneratedUint64Data(t, db, 5_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Age:    18 + (i % 60),
			Score:  float64(i),
			Active: true,
		}
	})

	q := qx.Query(
		qx.GTE("age", 25),
		qx.LTE("age", 40),
		qx.GT("score", 0.5),
	).Sort("score", qx.DESC).Limit(100)
	if _, err := db.QueryKeys(q); err != nil {
		t.Fatalf("warm QueryKeys: %v", err)
	}

	view := db.engine.currentQueryViewForTests()
	leaves := mustLimitQIRLeaves(t, db, q.Filter)
	baseOps := filterQIRLeavesByField(db, leaves, "score")
	coresBuf, rawCoreIdxBuf := mustPrepareOrderBasicBaseCoresForTest(t, view, baseOps)
	defer orderBasicBaseCoreSlicePool.Put(coresBuf)
	defer pooled.ReleaseIntSlice(rawCoreIdxBuf)
	collapsed := mustFindCollapsedOrderBasicBaseCoreForTest(t, coresBuf)
	if !hasWarmOrderBasicBaseCoresForTest(view, coresBuf) {
		var missing []string
		for _, op := range baseOps {
			stats, ok := view.orderBasicRawBaseOpStats(op, view.snap.Universe.Cardinality())
			cacheKey := qcache.MaterializedPredKey{}
			if ok {
				cacheKey = stats.cacheKey
			}
			if cacheKey.IsZero() {
				missing = append(missing, fmt.Sprintf("%s:%v=<no-key>", testExprFieldName(db.engine, op), op.Op))
				continue
			}
			if !db.engine.snapshot.Current().HasMaterializedPredKey(cacheKey) {
				missing = append(missing, fmt.Sprintf("%s:%v", testExprFieldName(db.engine, op), op.Op))
			}
		}
		t.Fatalf("expected warm ordered query to promote materialized range base ops, missing=%v", missing)
	}
	exactKey := qcache.MaterializedPredKeyForExactScalarRange(collapsed.collapsed.field, collapsed.collapsed.bounds)
	if exactKey.IsZero() {
		t.Fatalf("expected collapsed exact range cache key")
	}
	if _, ok := snapshotExtLoadMaterializedPred(db.engine.snapshot.Current(), exactKey); !ok {
		t.Fatalf("expected warm ordered query to promote collapsed exact numeric range cache entry")
	}
}

func TestQuery_OrderBasic_BaseCoreObservedWorkAccumulatesBeforePromotion(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})

	const rows = 20_000
	seedGeneratedUint64Data(t, db, rows, func(i int) *Rec {
		age := 2
		if i <= rows/2 {
			age = 1
		}
		return &Rec{
			Age:   age,
			Score: float64(i),
		}
	})

	view := db.engine.currentQueryViewForTests()
	leaves := mustLimitQIRLeaves(t, db, qx.LTE("age", 1))
	baseOps := filterQIRLeavesByField(db, leaves, "score")
	if len(baseOps) != 1 {
		t.Fatalf("baseOps=%d want 1", len(baseOps))
	}
	stats, ok := view.orderBasicRawBaseOpStats(baseOps[0], view.snap.Universe.Cardinality())
	if !ok || stats.cacheKey.IsZero() {
		t.Fatalf("orderBasicRawBaseOpStats: ok=%v key=%v", ok, stats.cacheKey)
	}
	buildWork := rangeProbeMaterializeWork(stats.buildBuckets, stats.buildEst)
	observedRows := uint64(100)
	probeWork := rangeAdaptiveProbeWorkForRows(observedRows, stats.probeBuckets, stats.probeEst)
	if probeWork == 0 || satAddUint64(probeWork, probeWork) >= buildWork {
		t.Fatalf("fixture no longer exercises sub-half build work: probe=%d build=%d", probeWork, buildWork)
	}

	view.promoteOrderBasicLimitMaterializedBaseOps("score", baseOps, observedRows, 10)
	if view.snap.HasMaterializedPredKey(stats.cacheKey) {
		t.Fatalf("first observed base-core scan unexpectedly promoted materialized predicate")
	}

	tries := int((buildWork + probeWork - 1) / probeWork)
	for i := 1; i < tries; i++ {
		view.promoteOrderBasicLimitMaterializedBaseOps("score", baseOps, observedRows, 10)
	}
	if !view.snap.HasMaterializedPredKey(stats.cacheKey) {
		t.Fatalf("repeated observed base-core work did not promote materialized predicate: probe=%d build=%d tries=%d observed=%d", probeWork, buildWork, tries, view.snap.ObservedMaterializedPredWork(stats.cacheKey))
	}
}

func TestQuery_OrderBasic_BaseCoreOversizedPromotionStaysBounded(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                             -1,
		SnapshotMaterializedPredCacheMaxEntries:     4,
		SnapshotMaterializedPredCacheMaxCardinality: 64,
	})

	const rows = 2_000
	seedGeneratedUint64Data(t, db, rows, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("user%04d", i),
			Email:  fmt.Sprintf("user%04d@example.com", i),
			Score:  float64(i),
			Active: true,
		}
	})

	view := db.engine.currentQueryViewForTests()
	leaves := mustLimitQIRLeaves(t, db, qx.AND(
		qx.PREFIX("name", "user0"),
		qx.PREFIX("email", "user0"),
	))
	baseOps := filterQIRLeavesByField(db, leaves, "score")
	if len(baseOps) != 2 {
		t.Fatalf("baseOps=%d want 2", len(baseOps))
	}
	view.promoteOrderBasicLimitMaterializedBaseOps("score", baseOps, uint64(rows), 10)

	limit := view.snap.MaterializedPredCacheOversizedLimit()
	if got := view.snap.MaterializedPredCacheOversizedEntryCount(); got > limit {
		t.Fatalf("oversized materialized predicates=%d want <=%d", got, limit)
	}
	if view.snap.MaterializedPredCacheEntryCount() == 0 {
		t.Fatalf("expected at least one bounded oversized materialized predicate")
	}
}

func TestQuery_OrderBasic_WarmComplementDoesNotPromoteSplitRangeHalves(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                             -1,
		SnapshotMaterializedPredCacheMaxEntries:     16,
		SnapshotMaterializedPredCacheMaxCardinality: 64,
	})

	const rows = 20_000
	seedGeneratedUint64Data(t, db, rows, func(i int) *Rec {
		id := uint64(i)
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Age:    i,
			Score:  float64((id * 7919) % rows),
			Active: true,
		}
	})

	q := qx.Query(
		qx.GTE("age", 4_000),
		qx.LT("age", 16_000),
	).Sort("score", qx.DESC).Limit(32)

	if _, err := db.QueryKeys(q); err != nil {
		t.Fatalf("warm QueryKeys: %v", err)
	}
	afterWarm := db.engine.snapshot.Current().MaterializedPredCacheEntryCount()
	if afterWarm == 0 {
		t.Fatalf("expected warm query to cache broad range complement")
	}

	if _, err := db.QueryKeys(q); err != nil {
		t.Fatalf("second QueryKeys: %v", err)
	}
	afterSecond := db.engine.snapshot.Current().MaterializedPredCacheEntryCount()
	if afterSecond != afterWarm {
		t.Fatalf("expected warm complement to avoid split half-range promotion, entries after warm=%d after second=%d", afterWarm, afterSecond)
	}
}

func TestQuery_OrderBasic_WarmAnalyticsRangeUsesLimitOrderBasicPlan(t *testing.T) {
	var events []TraceEvent
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSampleEvery: 1,
		TraceSink: func(ev TraceEvent) {
			events = append(events, ev)
		},
	})

	seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Age:    18 + (i % 60),
			Score:  float64(i),
			Active: true,
		}
	})

	q := qx.Query(
		qx.GTE("age", 25),
		qx.LTE("age", 40),
		qx.GT("score", 0.5),
	).Sort("score", qx.DESC).Limit(100)
	if _, err := db.QueryKeys(q); err != nil {
		t.Fatalf("first QueryKeys: %v", err)
	}
	view := db.engine.currentQueryViewForTests()
	leaves := mustLimitQIRLeaves(t, db, q.Filter)
	baseOps := filterQIRLeavesByField(db, leaves, "score")
	coresBuf, rawCoreIdxBuf := mustPrepareOrderBasicBaseCoresForTest(t, view, baseOps)
	defer orderBasicBaseCoreSlicePool.Put(coresBuf)
	defer pooled.ReleaseIntSlice(rawCoreIdxBuf)
	collapsed := mustFindCollapsedOrderBasicBaseCoreForTest(t, coresBuf)
	if hit, ok := view.loadWarmOrderBasicBaseCore(collapsed); !ok {
		t.Fatalf("expected collapsed warm range after first analytics query")
	} else {
		hit.ids.Release()
	}
	if !hasWarmOrderBasicBaseCoresForTest(view, coresBuf) {
		t.Fatalf("expected warm order-basic base ops after first analytics query")
	}
	if _, err := db.QueryKeys(q); err != nil {
		t.Fatalf("second QueryKeys: %v", err)
	}
	if len(events) < 2 {
		t.Fatalf("expected at least two trace events, got %d", len(events))
	}
	if events[len(events)-1].Plan != string(PlanLimitOrderBasic) {
		t.Fatalf("expected second query to use %q, got %q", PlanLimitOrderBasic, events[len(events)-1].Plan)
	}
}

func TestQuery_OrderBasic_WarmBroadExactAndRangeUsesLimitOrderBasicPlan(t *testing.T) {
	var events []TraceEvent
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSampleEvery: 1,
		TraceSink: func(ev TraceEvent) {
			events = append(events, ev)
		},
	})

	seedGeneratedUint64Data(t, db, 100_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Age:    18 + (i % 50),
			Score:  float64(i),
			Active: i%10 != 0 && i%7 != 0,
		}
	})

	q := qx.Query(
		qx.EQ("active", true),
		qx.GTE("score", 250.0),
		qx.GTE("age", 20),
	).Sort("score", qx.DESC).Limit(50)

	if _, err := db.QueryKeys(q); err != nil {
		t.Fatalf("first QueryKeys: %v", err)
	}
	if _, err := db.QueryKeys(q); err != nil {
		t.Fatalf("second QueryKeys: %v", err)
	}

	if len(events) < 2 {
		t.Fatalf("expected at least two trace events, got %d", len(events))
	}
	if events[len(events)-1].Plan != string(PlanLimitOrderBasic) {
		t.Fatalf("expected second query to use %q, got %q", PlanLimitOrderBasic, events[len(events)-1].Plan)
	}
}

func TestQuery_OrderBasic_ExtractBoundsForField_IgnoresSecondaryRangeBounds(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	q := qx.Query(
		qx.GTE("score", 100.0),
		qx.GTE("age", 25),
		qx.LTE("age", 40),
		qx.EQ("active", true),
	).Sort("score", qx.DESC).Limit(50)

	leaves := mustLimitQIRLeaves(t, db, q.Filter)

	view := db.engine.currentQueryViewForTests()
	bounds, ok, err := view.extractBoundsForField("score", leaves)
	if err != nil {
		t.Fatalf("extractBoundsForField: %v", err)
	}
	if !ok {
		t.Fatalf("expected order-field bounds to be recognized")
	}
	if !bounds.HasLo {
		t.Fatalf("unexpected lower bound: %+v", bounds)
	}
	if bounds.HasHi {
		t.Fatalf("unexpected high bound: %+v", bounds)
	}
}

func TestBuildPredicatesOrdered_WarmMergedNumericRangeUsesExactRangeCache(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval: -1,
	})

	seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("u_%d", i),
			Age:    18 + (i % 60),
			Score:  float64(i),
			Active: true,
		}
	})

	q := qx.Query(
		qx.GTE("age", 25),
		qx.LTE("age", 40),
		qx.GT("score", 0.5),
	).Sort("score", qx.DESC).Limit(100)
	if _, err := db.QueryKeys(q); err != nil {
		t.Fatalf("warm QueryKeys: %v", err)
	}

	view := db.engine.currentQueryViewForTests()
	leaves := mustLimitQIRLeaves(t, db, q.Filter)
	predSet, ok := view.buildPredicatesOrderedWithMode(leaves, "score", false, 100, 0, true, true)
	if !ok {
		t.Fatalf("buildPredicatesOrderedWithMode: ok=false")
	}
	defer predSet.Release()

	ageCount := 0
	for i := 0; i < predSet.Len(); i++ {
		pred := predSet.owner[i]
		if testExprFieldName(db.engine, pred.expr) != "age" {
			continue
		}
		ageCount++
		if !pred.isMaterializedLike() {
			t.Fatalf("expected warm merged age predicate to load exact materialized range")
		}
	}
	if ageCount != 1 {
		t.Fatalf("expected exactly one merged age predicate, got %d", ageCount)
	}
}

func postingOf(ids ...uint64) posting.List {
	return posting.BuildFromSorted(ids)
}

func TestPlannerFilterPostingByLeafChecks_PreferredExactBypassesSmallBucketFallback(t *testing.T) {
	src := posting.BuildFromSorted([]uint64{1, 3, 5, 7, 9, 11, 13, 15})
	defer src.Release()

	postA := posting.BuildFromSorted([]uint64{3, 7, 11})
	postB := posting.BuildFromSorted([]uint64{5, 7, 13})
	defer postA.Release()
	defer postB.Release()

	postsBuf := posting.GetSlice(2)
	postsBuf = append(postsBuf, postA, postB)

	state := postsAnyFilterStatePool.Get()
	state.postsBuf = postsBuf

	preds := leafPredSlicePool.Get(1)
	preds = append(preds, leafPred{
		kind:          leafPredKindPostsUnion,
		postsBuf:      postsBuf,
		postsAnyState: state,
	})
	defer leafPredSlicePool.Put(preds)

	mode, exact, work, card := plannerFilterPostingByLeafChecks(preds, []int{0}, src.Borrow(), posting.List{}, false)
	defer work.Release()
	if mode != plannerPredicateBucketExact {
		t.Fatalf("unexpected mode: got=%v want=%v", mode, plannerPredicateBucketExact)
	}
	if card != src.Cardinality() {
		t.Fatalf("unexpected source cardinality: got=%d want=%d", card, src.Cardinality())
	}
	if got := exact.Cardinality(); got != 5 {
		t.Fatalf("unexpected exact cardinality: got=%d want=5", got)
	}
	for _, idx := range []uint64{3, 5, 7, 11, 13} {
		if !exact.Contains(idx) {
			t.Fatalf("exact posting is missing id %d", idx)
		}
	}
}

func TestLeafPred_PostsAnyStateContainsIdxAndCountBucketUseRuntimeState(t *testing.T) {
	postA := posting.BuildFromSorted([]uint64{1, 3, 5, 7, 9, 11, 13})
	postB := posting.BuildFromSorted([]uint64{5, 7, 9, 15, 17, 19})
	bucket := posting.BuildFromSorted([]uint64{1, 2, 5, 6, 7, 9, 14, 15, 17})
	defer bucket.Release()

	postsBuf := posting.GetSlice(2)
	postsBuf = append(postsBuf, postA, postB)

	state := postsAnyFilterStatePool.Get()
	state.postsBuf = postsBuf
	state.containsMaterializeAt = 1

	pred := leafPred{
		kind:          leafPredKindPostsUnion,
		postsBuf:      postsBuf,
		postsAnyState: state,
	}

	defer func() {
		postsAnyFilterStatePool.Put(state)
		for i := 0; i < len(postsBuf); i++ {
			postsBuf[i].Release()
		}
		posting.ReleaseSlice(postsBuf)
	}()

	if !pred.containsIdx(7) {
		t.Fatalf("expected runtime state to match existing id")
	}
	if pred.containsIdx(6) {
		t.Fatalf("unexpected match for missing id")
	}
	if state.ids.IsEmpty() {
		t.Fatalf("expected containsIdx to materialize union through runtime state")
	}

	cnt, ok := pred.countBucket(bucket)
	if !ok {
		t.Fatalf("expected runtime state countBucket to stay exact")
	}
	if cnt != 6 {
		t.Fatalf("unexpected runtime state bucket count: got=%d want=6", cnt)
	}
}

func TestOrderPredicatesEmitPostingReader_SingleBucketCountSkipsWithoutMatches(t *testing.T) {
	ids := posting.BuildFromSorted([]uint64{1, 2, 3, 4, 5, 6, 7})
	defer ids.Release()

	preds := []predicate{
		{
			kind: predicateKindCustom,
			contains: func(uint64) bool {
				panic("matches must not be called when single-check bucket skip succeeds")
			},
			bucketCount: func(bucket posting.List) (uint64, bool) {
				if !bucket.SharesPayload(ids) {
					t.Fatalf("unexpected bucket passed to countBucket")
				}
				return 4, true
			},
		},
	}

	cursor := queryCursor{
		skip: 4,
		need: 1,
	}
	examined := uint64(0)
	stop, nextWork := orderPredicatesEmitPostingReader(
		&cursor,
		preds,
		[]int{0},
		nil,
		nil,
		false,
		ids.Borrow(),
		posting.List{},
		nil,
		&examined,
	)
	defer nextWork.Release()

	if stop {
		t.Fatalf("unexpected stop on pure skip")
	}
	if cursor.skip != 0 {
		t.Fatalf("unexpected remaining skip: got=%d want=0", cursor.skip)
	}
	if len(cursor.out) != 0 {
		t.Fatalf("unexpected emitted keys during skip: %v", cursor.out)
	}
	if examined != ids.Cardinality() {
		t.Fatalf("unexpected examined rows: got=%d want=%d", examined, ids.Cardinality())
	}
}

func TestPostingUnionIter_SmallUnionAvoidsDuplicates(t *testing.T) {
	it := newPostingUnionIter([]posting.List{
		postingOf(1, 2, 5),
		postingOf(2, 3),
		postingOf(1, 4),
	})

	var got []uint64
	for it.HasNext() {
		got = append(got, it.Next())
	}

	want := []uint64{1, 2, 5, 3, 4}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("small union mismatch: got=%v want=%v", got, want)
	}
}

func TestPostingUnionIter_ReusesScratchWithoutStaleState(t *testing.T) {
	posts := []posting.List{
		postingOf(1, 2, 5),
		postingOf(2, 3),
		postingOf(1, 4),
		postingOf(6, 7, 8),
	}

	drain := func() []uint64 {
		it := newPostingUnionIter(posts)
		if it != nil {
			defer it.Release()
		}
		var out []uint64
		for it.HasNext() {
			out = append(out, it.Next())
		}
		return out
	}

	first := drain()
	second := drain()
	want := []uint64{1, 2, 5, 3, 4, 6, 7, 8}
	if !reflect.DeepEqual(first, want) {
		t.Fatalf("first union mismatch: got=%v want=%v", first, want)
	}
	if !reflect.DeepEqual(second, want) {
		t.Fatalf("second union mismatch: got=%v want=%v", second, want)
	}
}

func TestPostingUnionIter_AllocsPerRunStaysLowAfterWarmup(t *testing.T) {
	if testRaceEnabled {
		t.Skip("testing.AllocsPerRun is not stable under -race")
	}

	posts := []posting.List{
		postingOf(1, 2, 5),
		postingOf(2, 3),
		postingOf(1, 4),
		postingOf(6, 7, 8),
	}

	warm := newPostingUnionIter(posts)
	if warm != nil {
		warm.Release()
	}

	allocs := testing.AllocsPerRun(100, func() {
		it := newPostingUnionIter(posts)
		for it.HasNext() {
			_ = it.Next()
		}
		if it != nil {
			it.Release()
		}
	})
	if allocs > 0.2 {
		t.Fatalf("unexpected allocs per run: got=%v want<=0.2", allocs)
	}
}

func TestPostingUnionBufIter_SmallUnionAllocsPerRunStayZeroAfterWarmup(t *testing.T) {
	if testRaceEnabled {
		t.Skip("testing.AllocsPerRun is not stable under -race")
	}

	posts := [...]posting.List{
		postingOf(1, 2, 5),
		postingOf(2, 3),
		postingOf(1, 4),
	}
	defer func() {
		for i := range posts {
			posts[i].Release()
		}
	}()

	postsBuf := posting.GetSlice(3)
	postsBuf = append(postsBuf, posts[0], posts[1], posts[2])
	defer posting.ReleaseSlice(postsBuf)

	warm := newPostingUnionIter(postsBuf)
	if warm != nil {
		warm.Release()
	}

	allocs := testing.AllocsPerRun(100, func() {
		it := newPostingUnionIter(postsBuf)
		for it.HasNext() {
			_ = it.Next()
		}
		if it != nil {
			it.Release()
		}
	})
	if allocs != 0 {
		t.Fatalf("expected zero allocs after warmup, got %.2f", allocs)
	}
}

func TestU64Set_AllocsPerRunStayZeroAfterWarmup(t *testing.T) {
	if testRaceEnabled {
		t.Skip("testing.AllocsPerRun is not stable under -race")
	}

	warm := getU64Set(64)
	for i := 0; i < 32; i++ {
		_ = warm.Add(uint64(i))
	}
	releaseU64Set(&warm)

	allocs := testing.AllocsPerRun(100, func() {
		set := getU64Set(64)
		for i := 0; i < 32; i++ {
			_ = set.Add(uint64(i))
		}
		releaseU64Set(&set)
	})
	if allocs > 0 {
		t.Fatalf("unexpected allocs per run: got=%v want=0", allocs)
	}
}
