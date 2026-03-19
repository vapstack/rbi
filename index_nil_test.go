package rbi

import (
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"go.etcd.io/bbolt"
)

type PtrIntRec struct {
	Name   string `db:"name"`
	Rank   *int   `db:"rank"`
	Active bool   `db:"active"`
}

func intPtr(v int) *int {
	return &v
}

func strPtr(v string) *string {
	return &v
}

func openTempDBUint64PtrInt(t *testing.T, options ...Options) (*DB[uint64, PtrIntRec], string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_uint64_ptrint.db")
	db, raw := openBoltAndNew[uint64, PtrIntRec](t, path, options...)

	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	return db, path
}

func sortedIDsCopy(ids []uint64) []uint64 {
	out := append([]uint64(nil), ids...)
	slices.Sort(out)
	return out
}

func assertSameSet(t *testing.T, got, want []uint64) {
	t.Helper()
	assertSameSlice(t, sortedIDsCopy(got), sortedIDsCopy(want))
}

func TestPointerNil_StringQueriesAndOrder(t *testing.T) {
	db, _ := openTempDBUint64(t)

	rows := map[uint64]*Rec{
		1: {Name: "nil", Opt: nil, Active: true},
		2: {Name: "empty", Opt: strPtr(""), Active: true},
		3: {Name: "alpha", Opt: strPtr("alpha"), Active: false},
		4: {Name: "nilish", Opt: strPtr("nilish"), Active: true},
		5: {Name: "beta", Opt: strPtr("beta"), Active: false},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}

	gotNil, err := db.QueryKeys(qx.Query(qx.EQ("opt", nil)))
	if err != nil {
		t.Fatalf("QueryKeys(EQ nil): %v", err)
	}
	assertSameSlice(t, gotNil, []uint64{1})

	gotIn, err := db.QueryKeys(qx.Query(qx.IN("opt", []any{nil, "alpha"})))
	if err != nil {
		t.Fatalf("QueryKeys(IN nil,alpha): %v", err)
	}
	assertSameSet(t, gotIn, []uint64{1, 3})

	gotNE, err := db.QueryKeys(qx.Query(qx.NE("opt", nil)))
	if err != nil {
		t.Fatalf("QueryKeys(NE nil): %v", err)
	}
	assertSameSet(t, gotNE, []uint64{2, 3, 4, 5})

	gotPrefix, err := db.QueryKeys(qx.Query(qx.PREFIX("opt", "")))
	if err != nil {
		t.Fatalf("QueryKeys(PREFIX empty): %v", err)
	}
	assertSameSet(t, gotPrefix, []uint64{2, 3, 4, 5})

	gotSuffix, err := db.QueryKeys(qx.Query(qx.SUFFIX("opt", "ha")))
	if err != nil {
		t.Fatalf("QueryKeys(SUFFIX ha): %v", err)
	}
	assertSameSlice(t, gotSuffix, []uint64{3})

	gotContains, err := db.QueryKeys(qx.Query(qx.CONTAINS("opt", "il")))
	if err != nil {
		t.Fatalf("QueryKeys(CONTAINS il): %v", err)
	}
	assertSameSlice(t, gotContains, []uint64{4})

	for _, q := range []*qx.QX{
		qx.Query().By("opt", qx.ASC),
		qx.Query().By("opt", qx.DESC),
		qx.Query(qx.EQ("active", true)).By("opt", qx.ASC).Skip(1).Max(2),
		qx.Query(qx.EQ("active", true)).By("opt", qx.DESC).Skip(1).Max(2),
	} {
		got, err := db.QueryKeys(q)
		if err != nil {
			t.Fatalf("QueryKeys(%+v): %v", q, err)
		}
		want, err := expectedKeysUint64(t, db, q)
		if err != nil {
			t.Fatalf("expectedKeysUint64(%+v): %v", q, err)
		}
		assertSameSlice(t, got, want)

		_, prepared, _, _ := assertPreparedRouteEquivalence(t, db, q)
		assertSameSlice(t, prepared, want)
	}
}

func TestPointerNil_IntQueriesCountRebuildAndReopen(t *testing.T) {
	db, path := openTempDBUint64PtrInt(t)
	opts := Options{
		EnableAutoBatchStats: true,
		EnableSnapshotStats:  true,
	}

	rows := map[uint64]*PtrIntRec{
		1: {Name: "nil", Rank: nil, Active: true},
		2: {Name: "zero", Rank: intPtr(0), Active: true},
		3: {Name: "ten", Rank: intPtr(10), Active: false},
		4: {Name: "twenty", Rank: intPtr(20), Active: true},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}

	check := func(stage string, wantAsc, wantDesc, wantNil, wantIn []uint64, wantGT, wantCountNil, wantCountIn uint64) {
		t.Helper()

		gotNil, err := db.QueryKeys(qx.Query(qx.EQ("rank", nil)))
		if err != nil {
			t.Fatalf("%s QueryKeys(EQ nil): %v", stage, err)
		}
		assertSameSlice(t, gotNil, wantNil)

		gotIn, err := db.QueryKeys(qx.Query(qx.IN("rank", []any{nil, 10})))
		if err != nil {
			t.Fatalf("%s QueryKeys(IN nil,10): %v", stage, err)
		}
		assertSameSet(t, gotIn, wantIn)

		gotGT, err := db.QueryKeys(qx.Query(qx.GT("rank", 0)))
		if err != nil {
			t.Fatalf("%s QueryKeys(GT 0): %v", stage, err)
		}
		if uint64(len(gotGT)) != wantGT {
			t.Fatalf("%s GT count mismatch: got=%v wantCount=%d", stage, gotGT, wantGT)
		}

		gotAsc, err := db.QueryKeys(qx.Query().By("rank", qx.ASC))
		if err != nil {
			t.Fatalf("%s QueryKeys(By rank ASC): %v", stage, err)
		}
		assertSameSlice(t, gotAsc, wantAsc)

		gotDesc, err := db.QueryKeys(qx.Query().By("rank", qx.DESC))
		if err != nil {
			t.Fatalf("%s QueryKeys(By rank DESC): %v", stage, err)
		}
		assertSameSlice(t, gotDesc, wantDesc)

		gotAscPage, err := db.QueryKeys(qx.Query().By("rank", qx.ASC).Skip(1).Max(3))
		if err != nil {
			t.Fatalf("%s QueryKeys(By rank ASC page): %v", stage, err)
		}
		assertSameSlice(t, gotAscPage, wantAsc[1:])

		cntNil, err := db.Count(qx.Query(qx.EQ("rank", nil)))
		if err != nil {
			t.Fatalf("%s Count(EQ nil): %v", stage, err)
		}
		if cntNil != wantCountNil {
			t.Fatalf("%s Count(EQ nil) mismatch: got=%d want=%d", stage, cntNil, wantCountNil)
		}

		cntIn, err := db.Count(qx.Query(qx.IN("rank", []any{nil, 10})))
		if err != nil {
			t.Fatalf("%s Count(IN nil,10): %v", stage, err)
		}
		if cntIn != wantCountIn {
			t.Fatalf("%s Count(IN nil,10) mismatch: got=%d want=%d", stage, cntIn, wantCountIn)
		}
	}

	check("base", []uint64{2, 3, 4, 1}, []uint64{4, 3, 2, 1}, []uint64{1}, []uint64{1, 3}, 2, 1, 2)

	if err := db.Patch(1, []Field{{Name: "rank", Value: 15}}); err != nil {
		t.Fatalf("Patch(1 rank=15): %v", err)
	}
	if err := db.Patch(2, []Field{{Name: "rank", Value: (*int)(nil)}}); err != nil {
		t.Fatalf("Patch(2 rank=nil): %v", err)
	}
	if err := db.Patch(4, []Field{{Name: "rank", Value: 5}}); err != nil {
		t.Fatalf("Patch(4 rank=5): %v", err)
	}

	check("delta", []uint64{4, 3, 1, 2}, []uint64{1, 3, 4, 2}, []uint64{2}, []uint64{2, 3}, 3, 1, 2)

	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}
	check("rebuild", []uint64{4, 3, 1, 2}, []uint64{1, 3, 4, 2}, []uint64{2}, []uint64{2, 3}, 3, 1, 2)

	plannerStats := db.PlannerStats()
	fs, ok := plannerStats.Fields["rank"]
	if !ok {
		t.Fatalf("planner stats missing rank field")
	}
	if fs.DistinctKeys != 3 {
		t.Fatalf("planner stats distinct keys mismatch: got=%d want=3", fs.DistinctKeys)
	}

	indexStats := db.IndexStats()
	if got := indexStats.UniqueFieldKeys["rank"]; got != 3 {
		t.Fatalf("index stats unique keys mismatch: got=%d want=3", got)
	}
	if got := indexStats.FieldTotalCardinality["rank"]; got != 4 {
		t.Fatalf("index stats total cardinality mismatch: got=%d want=4", got)
	}
	if got := indexStats.FieldSize["rank"]; got == 0 {
		t.Fatalf("index stats field size should include nil family")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := db.bolt.Close(); err != nil {
		t.Fatalf("bolt.Close: %v", err)
	}
	db = nil

	raw, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("reopen raw: %v", err)
	}
	defer raw.Close()

	reopened, err := New[uint64, PtrIntRec](raw, opts)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopened.Close()
	db = reopened

	check("reopen", []uint64{4, 3, 1, 2}, []uint64{1, 3, 4, 2}, []uint64{2}, []uint64{2, 3}, 3, 1, 2)
}

func TestPointerNil_RebuildClearsStaleNilBase(t *testing.T) {
	db, _ := openTempDBUint64PtrInt(t)

	if err := db.Set(1, &PtrIntRec{Name: "nil", Rank: nil}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &PtrIntRec{Name: "ten", Rank: intPtr(10)}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex(initial): %v", err)
	}

	if err := db.Patch(1, []Field{{Name: "rank", Value: 20}}); err != nil {
		t.Fatalf("Patch(1 rank=20): %v", err)
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex(after patch): %v", err)
	}

	gotNil, err := db.QueryKeys(qx.Query(qx.EQ("rank", nil)))
	if err != nil {
		t.Fatalf("QueryKeys(EQ nil): %v", err)
	}
	if len(gotNil) != 0 {
		t.Fatalf("expected no nil rows after rebuild, got %v", gotNil)
	}

	gotIn, err := db.QueryKeys(qx.Query(qx.IN("rank", []any{nil, 20})))
	if err != nil {
		t.Fatalf("QueryKeys(IN nil,20): %v", err)
	}
	assertSameSlice(t, gotIn, []uint64{1})
}

func TestPlanCandidateOrder_SkipsPointerSortField(t *testing.T) {
	db, _ := openTempDBUint64(t)

	rows := map[uint64]*Rec{
		1: {Meta: Meta{Country: "NL"}, Name: "a", Opt: nil, Active: true},
		2: {Meta: Meta{Country: "DE"}, Name: "b", Opt: strPtr("beta"), Active: true},
		3: {Meta: Meta{Country: "PL"}, Name: "c", Opt: strPtr("alpha"), Active: true},
		4: {Meta: Meta{Country: "NL"}, Name: "d", Opt: strPtr("gamma"), Active: false},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}

	q := qx.Query(
		qx.NOT(qx.EQ("active", false)),
		qx.NOT(qx.EQ("country", "PL")),
	).By("opt", qx.ASC).Max(10)

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertSameSlice(t, got, want)

	nq := normalizeQueryForTest(q)
	planOut, ok, err := db.tryPlan(nq, nil)
	if err != nil {
		t.Fatalf("tryPlan: %v", err)
	}
	if ok {
		t.Fatalf("expected tryPlan to skip pointer ORDER fast paths, got %v", planOut)
	}
}

func TestPointerNil_OrderExecutionFastPaths(t *testing.T) {
	db, _ := openTempDBUint64(t)

	rows := map[uint64]*Rec{
		1: {Name: "nil", Opt: nil, Active: true},
		2: {Name: "empty", Opt: strPtr(""), Active: true},
		3: {Name: "alpha", Opt: strPtr("alpha"), Active: false},
		4: {Name: "nilish", Opt: strPtr("nilish"), Active: true},
		5: {Name: "beta", Opt: strPtr("beta"), Active: false},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}

	qLimit := qx.Query(qx.EQ("active", true)).By("opt", qx.ASC).Max(3)
	limitLeaves := mustExtractAndLeaves(t, qLimit.Expr)
	out, used, err := db.tryLimitQueryOrderBasic(qLimit, limitLeaves, nil)
	if err != nil {
		t.Fatalf("tryLimitQueryOrderBasic: %v", err)
	}
	if !used {
		t.Fatalf("expected tryLimitQueryOrderBasic to be used")
	}
	want, err := expectedKeysUint64(t, db, qLimit)
	if err != nil {
		t.Fatalf("expectedKeysUint64(limit): %v", err)
	}
	assertSameSlice(t, out, want)

	qOffset := qx.Query(qx.EQ("active", true)).By("opt", qx.ASC).Skip(1).Max(2)
	out, used, err = db.tryQueryOrderBasicWithLimit(qOffset, nil)
	if err != nil {
		t.Fatalf("tryQueryOrderBasicWithLimit: %v", err)
	}
	if !used {
		t.Fatalf("expected tryQueryOrderBasicWithLimit to be used")
	}
	want, err = expectedKeysUint64(t, db, qOffset)
	if err != nil {
		t.Fatalf("expectedKeysUint64(offset): %v", err)
	}
	assertSameSlice(t, out, want)

	qPrefix := qx.Query(qx.PREFIX("opt", "")).By("opt", qx.ASC).Skip(1).Max(2)
	out, used, err = db.tryQueryOrderPrefixWithLimit(qPrefix, nil)
	if err != nil {
		t.Fatalf("tryQueryOrderPrefixWithLimit: %v", err)
	}
	if !used {
		t.Fatalf("expected tryQueryOrderPrefixWithLimit to be used")
	}
	want, err = expectedKeysUint64(t, db, qPrefix)
	if err != nil {
		t.Fatalf("expectedKeysUint64(prefix): %v", err)
	}
	assertSameSlice(t, out, want)
}

func TestPointerNil_OrderSmallSlice_AllNilField(t *testing.T) {
	db, _ := openTempDBUint64PtrInt(t)

	if err := db.Set(1, &PtrIntRec{Name: "a", Rank: nil}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &PtrIntRec{Name: "b", Rank: nil}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query().By("rank", qx.ASC).Max(2)
	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	assertSameSlice(t, got, []uint64{1, 2})
}

func TestPointerNil_ExecPlanOrderedBasic_BaseNilTail(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	rows := map[uint64]*Rec{
		1: {Name: "nil", Opt: nil, Active: true},
		2: {Name: "empty", Opt: strPtr(""), Active: true},
		3: {Name: "alpha", Opt: strPtr("alpha"), Active: false},
		4: {Name: "nilish", Opt: strPtr("nilish"), Active: true},
		5: {Name: "beta", Opt: strPtr("beta"), Active: false},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := normalizeQueryForTest(qx.Query(
		qx.NOT(qx.EQ("active", false)),
	).By("opt", qx.ASC).Skip(1).Max(3))

	leaves, ok := collectAndLeaves(q.Expr)
	if !ok {
		t.Fatalf("collectAndLeaves: ok=false")
	}
	window, _ := orderWindow(q)
	preds, ok := db.buildPredicatesOrderedWithMode(leaves, "opt", false, window)
	if !ok {
		t.Fatalf("buildPredicatesOrderedWithMode: ok=false")
	}
	defer releasePredicates(preds)

	got, ok := db.execPlanOrderedBasic(q, preds, nil)
	if !ok {
		t.Fatalf("execPlanOrderedBasic: ok=false")
	}
	want, err := db.execPreparedQuery(q)
	if err != nil {
		t.Fatalf("execPreparedQuery: %v", err)
	}
	assertSameSlice(t, got, want)
}

func TestPointerNil_TryPlanOrdered_AllowsPointerSortField(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:        -1,
		CalibrationEnabled:     true,
		CalibrationSampleEvery: -1,
	})

	rows := map[uint64]*Rec{
		1: {Name: "nil", Opt: nil, Active: true},
		2: {Name: "empty", Opt: strPtr(""), Active: true},
		3: {Name: "alpha", Opt: strPtr("alpha"), Active: false},
		4: {Name: "nilish", Opt: strPtr("nilish"), Active: true},
		5: {Name: "beta", Opt: strPtr("beta"), Active: false},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}
	if err := db.Patch(1, []Field{{Name: "opt", Value: "zeta"}}); err != nil {
		t.Fatalf("Patch(1 opt=zeta): %v", err)
	}
	if err := db.Patch(2, []Field{{Name: "opt", Value: (*string)(nil)}}); err != nil {
		t.Fatalf("Patch(2 opt=nil): %v", err)
	}

	if err := db.SetCalibrationSnapshot(CalibrationSnapshot{
		UpdatedAt: time.Now(),
		Multipliers: map[string]float64{
			string(PlanOrdered):         0.01,
			string(PlanLimitOrderBasic): 100,
		},
		Samples: map[string]uint64{
			string(PlanOrdered):         1,
			string(PlanLimitOrderBasic): 1,
		},
	}); err != nil {
		t.Fatalf("SetCalibrationSnapshot: %v", err)
	}

	q := normalizeQueryForTest(qx.Query(
		qx.NOT(qx.EQ("active", false)),
	).By("opt", qx.DESC).Skip(1).Max(2))

	got, ok, err := db.tryPlan(q, nil)
	if err != nil {
		t.Fatalf("tryPlan: %v", err)
	}
	if !ok {
		t.Fatalf("expected tryPlan to use ordered planner path for pointer sort field")
	}
	want, err := db.execPreparedQuery(q)
	if err != nil {
		t.Fatalf("execPreparedQuery: %v", err)
	}
	assertSameSlice(t, got, want)
}
