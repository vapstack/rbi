package rbi

import (
	"fmt"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/vapstack/qx"
)

func seedQueryExtOptArrayPosDB(t *testing.T) *DB[uint64, Rec] {
	t.Helper()

	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	rows := map[uint64]*Rec{
		1: {Name: "nil-1", Opt: nil, Active: true},
		2: {Name: "alpha", Opt: strPtr("alpha"), Active: true},
		3: {Name: "beta", Opt: strPtr("beta"), Active: false},
		4: {Name: "empty", Opt: strPtr(""), Active: true},
		5: {Name: "nil-2", Opt: nil, Active: false},
		6: {Name: "gamma", Opt: strPtr("gamma"), Active: true},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}
	return db
}

func seedQueryExtOptAllNilDB(t *testing.T) *DB[uint64, Rec] {
	t.Helper()

	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	rows := map[uint64]*Rec{
		1: {Name: "nil-a", Opt: nil, Active: true},
		2: {Name: "nil-b", Opt: nil, Active: false},
		3: {Name: "nil-c", Opt: nil, Active: true},
		4: {Name: "nil-d", Opt: nil, Active: false},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}
	return db
}

func queryExtOptPriority() []string {
	return []string{"alpha", "beta", "", "gamma"}
}

func assertQueryExtIDsMatchExpected(t *testing.T, db *DB[uint64, Rec], q *qx.QX) []uint64 {
	t.Helper()

	got := runQueryKeysChecked(t, db, q)
	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(%+v): %v", q, err)
	}
	assertQueryIDsEqual(t, q, got, want)
	return want
}

func assertQueryExtItemsMatchExpected(t *testing.T, db *DB[uint64, Rec], q *qx.QX) {
	t.Helper()

	wantIDs, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(%+v): %v", q, err)
	}
	wantItems, err := db.BatchGet(wantIDs...)
	if err != nil {
		t.Fatalf("BatchGet(wantIDs): %v", err)
	}

	gotItems, err := db.Query(q)
	if err != nil {
		t.Fatalf("Query(%+v): %v", q, err)
	}
	if len(gotItems) != len(wantItems) {
		t.Fatalf("items len mismatch: got=%d want=%d", len(gotItems), len(wantItems))
	}
	for i := range wantItems {
		if gotItems[i] == nil || wantItems[i] == nil {
			t.Fatalf("nil item mismatch at i=%d: got=%#v want=%#v", i, gotItems[i], wantItems[i])
		}
		if !reflect.DeepEqual(*gotItems[i], *wantItems[i]) {
			t.Fatalf("item mismatch at i=%d: got=%#v want=%#v", i, gotItems[i], wantItems[i])
		}
	}
}

func assertQueryExtCountMatchesOrderedResultSet(t *testing.T, db *DB[uint64, Rec], q *qx.QX) {
	t.Helper()

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys(%+v): %v", q, err)
	}
	cnt, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count(%+v): %v", q, err)
	}
	if cnt != uint64(len(got)) {
		t.Fatalf("Count(%+v)=%d does not match len(QueryKeys)=%d", q, cnt, len(got))
	}
}

func assertQueryExtCountMatchesBaseQuery(t *testing.T, db *DB[uint64, Rec], q *qx.QX) {
	t.Helper()

	base := cloneQuery(q)
	base.Order = nil
	base.Offset = 0
	base.Limit = 0

	want, err := db.Count(base)
	if err != nil {
		t.Fatalf("Count(base %+v): %v", base, err)
	}
	got, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count(%+v): %v", q, err)
	}
	if got != want {
		t.Fatalf("Count mismatch: got=%d want=%d", got, want)
	}
}

func assertQueryExtPreparedMatchesExpected(t *testing.T, db *DB[uint64, Rec], q *qx.QX) {
	t.Helper()

	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(%+v): %v", q, err)
	}

	nq := normalizeQueryForTest(q)
	if err := db.checkUsedQuery(nq); err != nil {
		t.Fatalf("checkUsedQuery(%+v): %v", nq, err)
	}

	got, err := db.execPreparedQuery(nq)
	if err != nil {
		t.Fatalf("execPreparedQuery(%+v): %v", nq, err)
	}
	assertQueryIDsEqual(t, q, got, want)

	base := cloneQuery(q)
	base.Order = nil
	base.Offset = 0
	base.Limit = 0

	wantCount, err := db.Count(base)
	if err != nil {
		t.Fatalf("Count(base %+v): %v", base, err)
	}
	gotCount, err := db.countPreparedExpr(nq.Expr)
	if err != nil {
		t.Fatalf("countPreparedExpr(%+v): %v", nq.Expr, err)
	}
	if gotCount != wantCount {
		t.Fatalf("countPreparedExpr(%+v)=%d want=%d", nq.Expr, gotCount, wantCount)
	}
}

func assertQueryExtAllReadPathsMatchExpected(t *testing.T, db *DB[uint64, Rec], q *qx.QX) {
	t.Helper()

	assertQueryExtIDsMatchExpected(t, db, q)
	assertQueryExtItemsMatchExpected(t, db, q)
	assertQueryExtCountMatchesBaseQuery(t, db, q)
	assertQueryExtPreparedMatchesExpected(t, db, q)
}

func queryExtItemNames(t testing.TB, items []*Rec) []string {
	t.Helper()

	out, ok := queryExtItemNamesOK(items)
	if !ok {
		t.Fatalf("nil item in result: %#v", items)
	}
	return out
}

func queryExtItemNamesOK(items []*Rec) ([]string, bool) {
	out := make([]string, len(items))
	for i := range items {
		if items[i] == nil {
			return nil, false
		}
		out[i] = items[i].Name
	}
	return out, true
}

func assertQueryExtConcurrentReadStable(t *testing.T, db *DB[uint64, Rec], q *qx.QX, withPrepared bool) {
	t.Helper()

	wantPage, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(page %+v): %v", q, err)
	}
	wantItems, err := db.BatchGet(wantPage...)
	if err != nil {
		t.Fatalf("BatchGet(wantPage): %v", err)
	}
	countQ := cloneQuery(q)
	countQ.Order = nil
	countQ.Offset = 0
	countQ.Limit = 0
	wantAll, err := expectedKeysUint64(t, db, countQ)
	if err != nil {
		t.Fatalf("expectedKeysUint64(count %+v): %v", countQ, err)
	}
	wantCount := uint64(len(wantAll))

	var nq *qx.QX
	if withPrepared {
		nq = normalizeQueryForTest(q)
	}

	errCh := make(chan error, 16)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				gotKeys, err := db.QueryKeys(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys: %w", gid, i, err)
					return
				}
				if !queryIDsEqual(q, gotKeys, wantPage) {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys mismatch: got=%v want=%v", gid, i, gotKeys, wantPage)
					return
				}

				gotItems, err := db.Query(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Query: %w", gid, i, err)
					return
				}
				if len(gotItems) != len(wantItems) {
					errCh <- fmt.Errorf("g=%d i=%d Query len mismatch: got=%d want=%d", gid, i, len(gotItems), len(wantItems))
					return
				}
				for j := range wantItems {
					if gotItems[j] == nil || wantItems[j] == nil || !reflect.DeepEqual(*gotItems[j], *wantItems[j]) {
						errCh <- fmt.Errorf("g=%d i=%d Query item mismatch at j=%d", gid, i, j)
						return
					}
				}

				gotCount, err := db.Count(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Count: %w", gid, i, err)
					return
				}
				if gotCount != wantCount {
					errCh <- fmt.Errorf("g=%d i=%d Count mismatch: got=%d want=%d", gid, i, gotCount, wantCount)
					return
				}

				if withPrepared {
					gotPrepared, err := db.execPreparedQuery(nq)
					if err != nil {
						errCh <- fmt.Errorf("g=%d i=%d execPreparedQuery: %w", gid, i, err)
						return
					}
					if !queryIDsEqual(nq, gotPrepared, wantPage) {
						errCh <- fmt.Errorf("g=%d i=%d execPreparedQuery mismatch: got=%v want=%v", gid, i, gotPrepared, wantPage)
						return
					}
				}
			}
		}(g)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func TestQueryExt_ArrayPosPointer_AllPriorities_ASC_IncludesNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, qx.Query().ByArrayPos("opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_AllPriorities_DESC_IncludesNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, qx.Query().ByArrayPos("opt", queryExtOptPriority(), qx.DESC))
}

func TestQueryExt_ArrayPosPointer_ActiveFilter_ASC_IncludesNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, qx.Query(qx.EQ("active", true)).ByArrayPos("opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_ActiveFilter_DESC_IncludesNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, qx.Query(qx.EQ("active", true)).ByArrayPos("opt", queryExtOptPriority(), qx.DESC))
}

func TestQueryExt_ArrayPosPointer_EqNilOnly_ASC_ReturnsNilRows(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, qx.Query(qx.EQ("opt", nil)).ByArrayPos("opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_EqNilOnly_DESC_ReturnsNilRows(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, qx.Query(qx.EQ("opt", nil)).ByArrayPos("opt", queryExtOptPriority(), qx.DESC))
}

func TestQueryExt_ArrayPosPointer_SkipWindowIntoNilTail_ASC(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, qx.Query().ByArrayPos("opt", queryExtOptPriority(), qx.ASC).Skip(4).Max(8))
}

func TestQueryExt_ArrayPosPointer_SkipWindowIntoNilTail_DESC(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, qx.Query().ByArrayPos("opt", queryExtOptPriority(), qx.DESC).Skip(3).Max(3))
}

func TestQueryExt_ArrayPosPointer_LimitWindowCrossesIntoNilTail_ASC(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, qx.Query().ByArrayPos("opt", queryExtOptPriority(), qx.ASC).Max(5))
}

func TestQueryExt_ArrayPosPointer_NegativePredicate_RetainsNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, qx.Query(qx.NOT(qx.EQ("active", true))).ByArrayPos("opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_PatchToNil_RetainsExpandedNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	if err := db.Patch(6, []Field{{Name: "opt", Value: (*string)(nil)}}); err != nil {
		t.Fatalf("Patch(6 opt=nil): %v", err)
	}
	assertQueryExtIDsMatchExpected(t, db, qx.Query().ByArrayPos("opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_PatchFromNilToValue_RetainsRemainingNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	if err := db.Patch(1, []Field{{Name: "opt", Value: "delta"}}); err != nil {
		t.Fatalf("Patch(1 opt=delta): %v", err)
	}
	assertQueryExtIDsMatchExpected(t, db, qx.Query().ByArrayPos("opt", []string{"alpha", "beta", "", "gamma", "delta"}, qx.ASC))
}

func TestQueryExt_ArrayPosPointer_DeleteNonNil_RetainsNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	if err := db.Delete(6); err != nil {
		t.Fatalf("Delete(6): %v", err)
	}
	assertQueryExtIDsMatchExpected(t, db, qx.Query().ByArrayPos("opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_InsertNil_RetainsNewNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	if err := db.Set(7, &Rec{Name: "nil-3", Opt: nil, Active: true}); err != nil {
		t.Fatalf("Set(7): %v", err)
	}
	assertQueryExtIDsMatchExpected(t, db, qx.Query().ByArrayPos("opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_QueryValues_AllPriorities_ASC_RetainsNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtItemsMatchExpected(t, db, qx.Query().ByArrayPos("opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_CountMatchesOrderedResultSet_WhenUnbounded(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtCountMatchesOrderedResultSet(t, db, qx.Query().ByArrayPos("opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_AllNilDataset_ASC_WithPriorities(t *testing.T) {
	db := seedQueryExtOptAllNilDB(t)
	assertQueryExtIDsMatchExpected(t, db, qx.Query().ByArrayPos("opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_AllNilDataset_DESC_WithPriorities(t *testing.T) {
	db := seedQueryExtOptAllNilDB(t)
	assertQueryExtIDsMatchExpected(t, db, qx.Query().ByArrayPos("opt", queryExtOptPriority(), qx.DESC))
}

func TestQueryExt_ArrayPosPointer_AllNilDataset_ASC_EmptyPriorities(t *testing.T) {
	db := seedQueryExtOptAllNilDB(t)
	assertQueryExtIDsMatchExpected(t, db, qx.Query().ByArrayPos("opt", []string{}, qx.ASC))
}

func TestQueryExt_ArrayPosPointer_ASC_EmptyPriorities_FallsBackToIDOrder(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, qx.Query().ByArrayPos("opt", []string{}, qx.ASC))
}

func TestQueryExt_ArrayPosPointer_AllNilDataset_Window_EmptyPriorities(t *testing.T) {
	db := seedQueryExtOptAllNilDB(t)
	assertQueryExtIDsMatchExpected(t, db, qx.Query().ByArrayPos("opt", []string{}, qx.ASC).Skip(1).Max(2))
}

func TestQueryExt_ArrayPosPointer_AllNilAfterPatches_ASC(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	for _, id := range []uint64{2, 3, 4, 6} {
		if err := db.Patch(id, []Field{{Name: "opt", Value: (*string)(nil)}}); err != nil {
			t.Fatalf("Patch(%d opt=nil): %v", id, err)
		}
	}
	assertQueryExtIDsMatchExpected(t, db, qx.Query().ByArrayPos("opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_AllNilPredicate_DESC(t *testing.T) {
	db := seedQueryExtOptAllNilDB(t)
	assertQueryExtIDsMatchExpected(t, db, qx.Query(qx.EQ("active", true)).ByArrayPos("opt", []string{"alpha"}, qx.DESC))
}

func TestQueryExt_CountIgnoresUnknownBasicOrderField(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 64)
	assertQueryExtCountMatchesBaseQuery(t, db, qx.Query(qx.EQ("active", true)).By("no_such_field", qx.ASC).Skip(7).Max(9))
}

func TestQueryExt_CountIgnoresUnknownArrayPosOrderField(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 64)
	assertQueryExtCountMatchesBaseQuery(t, db, qx.Query(qx.EQ("active", true)).ByArrayPos("no_such_field", []string{"a", "b"}, qx.ASC))
}

func TestQueryExt_CountIgnoresUnknownArrayCountOrderField(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 64)
	assertQueryExtCountMatchesBaseQuery(t, db, qx.Query(qx.EQ("active", true)).ByArrayCount("no_such_field", qx.DESC))
}

func TestQueryExt_CountIgnoresUnknownSecondaryOrderField(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 64)

	q := qx.Query(qx.EQ("active", true))
	q.Order = []qx.Order{
		{Field: "age", Type: qx.OrderBasic, Desc: false},
		{Field: "no_such_field", Type: qx.OrderBasic, Desc: true},
	}
	assertQueryExtCountMatchesBaseQuery(t, db, q)
}

func TestQueryExt_CountIgnoresUnknownOrderFieldOnNoopQuery(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 64)
	assertQueryExtCountMatchesBaseQuery(t, db, qx.Query().By("no_such_field", qx.DESC))
}

func TestQueryExt_CountIgnoresUnknownOrderFieldOnEmptyDB(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	assertQueryExtCountMatchesBaseQuery(t, db, qx.Query(qx.EQ("active", true)).By("no_such_field", qx.ASC))
}

func TestQueryExt_OrderBasicPointer_NilTailChurn_MatchesSeqScanAndPrepared(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	rows := map[uint64]*Rec{
		1:  {Name: "nil-a", Opt: nil, Active: true, Tags: []string{"go"}},
		2:  {Name: "aa-a", Opt: strPtr("aa"), Active: true, Tags: []string{"db"}},
		3:  {Name: "empty-a", Opt: strPtr(""), Active: false, Tags: []string{"ops"}},
		4:  {Name: "bb-a", Opt: strPtr("bb"), Active: true, Tags: []string{"rust"}},
		5:  {Name: "nil-b", Opt: nil, Active: false, Tags: []string{"go", "db"}},
		6:  {Name: "aa-b", Opt: strPtr("aa"), Active: false, Tags: []string{"ops", "db"}},
		7:  {Name: "cc-a", Opt: strPtr("cc"), Active: true, Tags: []string{"go", "ops"}},
		8:  {Name: "dd-a", Opt: strPtr("dd"), Active: true, Tags: []string{"java"}},
		9:  {Name: "nil-c", Opt: nil, Active: true, Tags: []string{"db", "db"}},
		10: {Name: "bb-b", Opt: strPtr("bb"), Active: false, Tags: []string{"go", "go"}},
		11: {Name: "empty-b", Opt: strPtr(""), Active: true, Tags: []string{"ops", "ops"}},
		12: {Name: "zz-a", Opt: strPtr("zz"), Active: false, Tags: []string{"rust", "go"}},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}

	tests := []struct {
		name string
		q    *qx.QX
	}{
		{
			name: "asc_window",
			q:    qx.Query().By("opt", qx.ASC).Skip(2).Max(7),
		},
		{
			name: "desc_active_window",
			q:    qx.Query(qx.EQ("active", true)).By("opt", qx.DESC).Skip(1).Max(5),
		},
		{
			name: "negated_filter_asc",
			q:    qx.Query(qx.NOT(qx.EQ("active", true))).By("opt", qx.ASC).Skip(1).Max(6),
		},
	}

	check := func(step string) {
		for _, tc := range tests {
			t.Run(step+"_"+tc.name, func(t *testing.T) {
				assertQueryExtAllReadPathsMatchExpected(t, db, tc.q)
			})
		}
	}

	check("initial")

	if err := db.Patch(2, []Field{{Name: "opt", Value: (*string)(nil)}}); err != nil {
		t.Fatalf("Patch(2 opt=nil): %v", err)
	}
	if err := db.Patch(5, []Field{{Name: "opt", Value: "ab"}}); err != nil {
		t.Fatalf("Patch(5 opt=ab): %v", err)
	}
	if err := db.Patch(11, []Field{{Name: "opt", Value: (*string)(nil)}}); err != nil {
		t.Fatalf("Patch(11 opt=nil): %v", err)
	}
	check("after_nil_flip")

	if err := db.Patch(1, []Field{{Name: "opt", Value: ""}}); err != nil {
		t.Fatalf("Patch(1 opt=''): %v", err)
	}
	if err := db.Delete(4); err != nil {
		t.Fatalf("Delete(4): %v", err)
	}
	if err := db.Set(13, &Rec{Name: "nil-new", Opt: nil, Active: true, Tags: []string{"db", "ops"}}); err != nil {
		t.Fatalf("Set(13): %v", err)
	}
	if err := db.Set(14, &Rec{Name: "az-new", Opt: strPtr("az"), Active: false, Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set(14): %v", err)
	}
	check("after_insert_delete")

	if err := db.Patch(7, []Field{{Name: "active", Value: false}}); err != nil {
		t.Fatalf("Patch(7 active=false): %v", err)
	}
	if err := db.Patch(8, []Field{{Name: "opt", Value: "aa"}}); err != nil {
		t.Fatalf("Patch(8 opt=aa): %v", err)
	}
	if err := db.Delete(9); err != nil {
		t.Fatalf("Delete(9): %v", err)
	}
	check("after_tail_churn")
}

func TestQueryExt_ArrayCount_DistinctLengthChurn_MatchesSeqScanAndPrepared(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	rows := map[uint64]*Rec{
		1:  {Name: "zero-nil-1", Tags: nil, Active: true},
		2:  {Name: "zero-empty-1", Tags: []string{}, Active: false},
		3:  {Name: "zero-nil-2", Tags: nil, Active: true},
		4:  {Name: "zero-empty-2", Tags: []string{}, Active: false},
		5:  {Name: "zero-nil-3", Tags: nil, Active: true},
		6:  {Name: "zero-empty-3", Tags: []string{}, Active: false},
		7:  {Name: "zero-nil-4", Tags: nil, Active: true},
		8:  {Name: "one-dup", Tags: []string{"go", "go"}, Active: false},
		9:  {Name: "two-distinct", Tags: []string{"go", "db", "db"}, Active: true},
		10: {Name: "three-distinct", Tags: []string{"ops", "go", "db"}, Active: false},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}
	if !db.isLenZeroComplementField("tags") {
		t.Fatalf("expected zero-complement mode for tags at start")
	}

	tests := []struct {
		name string
		q    *qx.QX
	}{
		{
			name: "asc_window",
			q:    qx.Query().ByArrayCount("tags", qx.ASC).Skip(1).Max(7),
		},
		{
			name: "desc_active_window",
			q:    qx.Query(qx.EQ("active", true)).ByArrayCount("tags", qx.DESC).Skip(1).Max(4),
		},
		{
			name: "asc_negated_filter",
			q:    qx.Query(qx.NOT(qx.EQ("active", false))).ByArrayCount("tags", qx.ASC).Max(6),
		},
	}

	check := func(step string) {
		for _, tc := range tests {
			t.Run(step+"_"+tc.name, func(t *testing.T) {
				assertQueryExtAllReadPathsMatchExpected(t, db, tc.q)
			})
		}
	}

	check("initial")

	if err := db.BatchPatch([]uint64{1, 2, 3, 4}, []Field{{Name: "tags", Value: []string{"go"}}}); err != nil {
		t.Fatalf("BatchPatch(densify zeros): %v", err)
	}
	check("after_dense_nonempty")

	if err := db.BatchPatch([]uint64{1, 2, 3, 4, 5}, []Field{{Name: "tags", Value: []string{}}}); err != nil {
		t.Fatalf("BatchPatch(restore zeros): %v", err)
	}
	check("after_zero_complement_return")

	if err := db.Patch(8, []Field{{Name: "tags", Value: []string{"ops", "ops", "ops"}}}); err != nil {
		t.Fatalf("Patch(8 tags): %v", err)
	}
	if err := db.Patch(9, []Field{{Name: "tags", Value: []string{"go", "db", "ops", "ops"}}}); err != nil {
		t.Fatalf("Patch(9 tags): %v", err)
	}
	if err := db.Patch(10, []Field{{Name: "tags", Value: []string{"rust", "rust"}}}); err != nil {
		t.Fatalf("Patch(10 tags): %v", err)
	}
	if err := db.Delete(6); err != nil {
		t.Fatalf("Delete(6): %v", err)
	}
	if err := db.Set(11, &Rec{Name: "zero-new", Tags: nil, Active: true}); err != nil {
		t.Fatalf("Set(11): %v", err)
	}
	if err := db.Set(12, &Rec{Name: "two-new", Tags: []string{"ops", "go", "go"}, Active: false}); err != nil {
		t.Fatalf("Set(12): %v", err)
	}
	check("after_distinct_churn")
}

func TestQueryExt_ConcurrentSharedOrderBasicQuery_IsStable(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 1_500)
	q := qx.Query(
		qx.EQ("active", true),
		qx.NOTIN("country", []string{"Iceland"}),
		qx.GTE("age", 20),
	).By("score", qx.DESC).Skip(10).Max(80)
	assertQueryExtConcurrentReadStable(t, db, q, true)
}

func TestQueryExt_ConcurrentSharedArrayCountQuery_IsStable(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 1_200)
	q := qx.Query(
		qx.NOT(qx.EQ("active", false)),
	).ByArrayCount("tags", qx.DESC).Skip(3).Max(70)
	assertQueryExtConcurrentReadStable(t, db, q, true)
}

func TestQueryExt_ConcurrentAtomicBatchSetSnapshotConsistency_OnPointerOrder(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	ids := []uint64{1, 2, 3, 4, 5, 6}
	stateA := []*Rec{
		{Name: "A1-nil", Opt: nil, Active: true, Tags: []string{"go"}},
		{Name: "A2-aa", Opt: strPtr("aa"), Active: true, Tags: []string{"db"}},
		{Name: "A3-bb", Opt: strPtr("bb"), Active: true, Tags: []string{"ops"}},
		{Name: "A4-nil-off", Opt: nil, Active: false, Tags: []string{"rust"}},
		{Name: "A5-cc", Opt: strPtr("cc"), Active: true, Tags: []string{"go", "db"}},
		{Name: "A6-empty", Opt: strPtr(""), Active: true, Tags: []string{"ops", "go"}},
	}
	stateB := []*Rec{
		{Name: "B1-dd", Opt: strPtr("dd"), Active: true, Tags: []string{"go"}},
		{Name: "B2-nil", Opt: nil, Active: true, Tags: []string{"db"}},
		{Name: "B3-aa-off", Opt: strPtr("aa"), Active: false, Tags: []string{"ops"}},
		{Name: "B4-empty", Opt: strPtr(""), Active: true, Tags: []string{"rust"}},
		{Name: "B5-bb", Opt: strPtr("bb"), Active: true, Tags: []string{"go", "db"}},
		{Name: "B6-nil-off", Opt: nil, Active: false, Tags: []string{"ops", "go"}},
	}

	setState := func(vals []*Rec) error {
		return db.BatchSet(ids, vals)
	}

	q := qx.Query(qx.EQ("active", true)).By("opt", qx.ASC).Skip(1).Max(3)

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA): %v", err)
	}
	wantA, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(stateA): %v", err)
	}
	itemsA, err := db.Query(q)
	if err != nil {
		t.Fatalf("Query(stateA): %v", err)
	}
	namesA := queryExtItemNames(t, itemsA)
	countA, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count(stateA): %v", err)
	}

	if err := setState(stateB); err != nil {
		t.Fatalf("BatchSet(stateB): %v", err)
	}
	wantB, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(stateB): %v", err)
	}
	itemsB, err := db.Query(q)
	if err != nil {
		t.Fatalf("Query(stateB): %v", err)
	}
	namesB := queryExtItemNames(t, itemsB)
	countB, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count(stateB): %v", err)
	}

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA reset): %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 16)
	start := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 80; i++ {
			vals := stateB
			label := "stateB"
			if i%2 == 1 {
				vals = stateA
				label = "stateA"
			}
			if err := setState(vals); err != nil {
				errCh <- fmt.Errorf("writer %s: %w", label, err)
				return
			}
		}
	}()

	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			<-start
			for i := 0; i < 120; i++ {
				gotKeys, err := db.QueryKeys(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys: %w", gid, i, err)
					return
				}
				if !queryIDsEqual(q, gotKeys, wantA) && !queryIDsEqual(q, gotKeys, wantB) {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys hybrid snapshot: got=%v wantA=%v wantB=%v", gid, i, gotKeys, wantA, wantB)
					return
				}

				gotItems, err := db.Query(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Query: %w", gid, i, err)
					return
				}
				gotNames, ok := queryExtItemNamesOK(gotItems)
				if !ok {
					errCh <- fmt.Errorf("g=%d i=%d Query returned nil item: %#v", gid, i, gotItems)
					return
				}
				if !slices.Equal(gotNames, namesA) && !slices.Equal(gotNames, namesB) {
					errCh <- fmt.Errorf("g=%d i=%d Query hybrid snapshot: got=%v wantA=%v wantB=%v", gid, i, gotNames, namesA, namesB)
					return
				}

				gotCount, err := db.Count(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Count: %w", gid, i, err)
					return
				}
				if gotCount != countA && gotCount != countB {
					errCh <- fmt.Errorf("g=%d i=%d Count hybrid snapshot: got=%d wantA=%d wantB=%d", gid, i, gotCount, countA, countB)
					return
				}
			}
		}(g)
	}

	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func TestQueryExt_ConcurrentWriter_RootExecPreparedQuery_ReturnsHybridResults(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	ids := []uint64{1, 2, 3, 4, 5, 6}
	stateA := []*Rec{
		{Name: "A1-nil", Opt: nil, Active: true, Tags: []string{"go"}},
		{Name: "A2-aa", Opt: strPtr("aa"), Active: true, Tags: []string{"db"}},
		{Name: "A3-bb", Opt: strPtr("bb"), Active: true, Tags: []string{"ops"}},
		{Name: "A4-nil-off", Opt: nil, Active: false, Tags: []string{"rust"}},
		{Name: "A5-cc", Opt: strPtr("cc"), Active: true, Tags: []string{"go", "db"}},
		{Name: "A6-empty", Opt: strPtr(""), Active: true, Tags: []string{"ops", "go"}},
	}
	stateB := []*Rec{
		{Name: "B1-dd", Opt: strPtr("dd"), Active: true, Tags: []string{"go"}},
		{Name: "B2-nil", Opt: nil, Active: true, Tags: []string{"db"}},
		{Name: "B3-aa-off", Opt: strPtr("aa"), Active: false, Tags: []string{"ops"}},
		{Name: "B4-empty", Opt: strPtr(""), Active: true, Tags: []string{"rust"}},
		{Name: "B5-bb", Opt: strPtr("bb"), Active: true, Tags: []string{"go", "db"}},
		{Name: "B6-nil-off", Opt: nil, Active: false, Tags: []string{"ops", "go"}},
	}

	setState := func(vals []*Rec) error {
		return db.BatchSet(ids, vals)
	}

	q := qx.Query(qx.EQ("active", true)).By("opt", qx.ASC).Skip(1).Max(3)
	nq := normalizeQueryForTest(q)
	if err := db.checkUsedQuery(nq); err != nil {
		t.Fatalf("checkUsedQuery(%+v): %v", nq, err)
	}

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA): %v", err)
	}
	wantA, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(stateA): %v", err)
	}

	if err := setState(stateB); err != nil {
		t.Fatalf("BatchSet(stateB): %v", err)
	}
	wantB, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(stateB): %v", err)
	}

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA reset): %v", err)
	}

	start := make(chan struct{})
	found := make(chan []uint64, 1)
	errCh := make(chan error, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 400; i++ {
			vals := stateB
			if i%2 == 1 {
				vals = stateA
			}
			if err := setState(vals); err != nil {
				select {
				case errCh <- fmt.Errorf("writer: %w", err):
				default:
				}
				return
			}
		}
	}()

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 500; i++ {
				got, err := db.execPreparedQuery(nq)
				if err != nil {
					select {
					case errCh <- fmt.Errorf("execPreparedQuery: %w", err):
					default:
					}
					return
				}
				if queryIDsEqual(q, got, wantA) || queryIDsEqual(q, got, wantB) {
					continue
				}
				select {
				case found <- append([]uint64(nil), got...):
				default:
				}
				return
			}
		}()
	}

	close(start)
	wg.Wait()
	close(found)
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	for got := range found {
		t.Fatalf("root execPreparedQuery returned hybrid result under concurrent writes: got=%v wantA=%v wantB=%v", got, wantA, wantB)
	}
}
