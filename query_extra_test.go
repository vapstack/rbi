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

func assertQueryExtraPublicReadPathsMatchExpected(t *testing.T, db *DB[uint64, Rec], q *qx.QX) {
	t.Helper()

	assertQueryExtIDsMatchExpected(t, db, q)
	assertQueryExtItemsMatchExpected(t, db, q)
	assertQueryExtCountMatchesBaseQuery(t, db, q)
}

func runQueryKeysStringChecked(t *testing.T, db *DB[string, Rec], q *qx.QX) []string {
	t.Helper()

	ids, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys(%+v): %v", q, err)
	}
	return ids
}

func assertQueryExtraStringPublicReadPathsMatchExpected(t *testing.T, db *DB[string, Rec], q *qx.QX) {
	t.Helper()

	got := runQueryKeysStringChecked(t, db, q)
	want, err := expectedKeysString(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysString(%+v): %v", q, err)
	}
	if !queryStringIDsEqual(q, got, want) {
		t.Fatalf("QueryKeys mismatch: got=%v want=%v q=%+v", got, want, q)
	}

	wantItems, err := db.BatchGet(want...)
	if err != nil {
		t.Fatalf("BatchGet(want): %v", err)
	}
	gotItems, err := db.Query(q)
	if err != nil {
		t.Fatalf("Query(%+v): %v", q, err)
	}
	if len(gotItems) != len(wantItems) {
		t.Fatalf("Query len mismatch: got=%d want=%d q=%+v", len(gotItems), len(wantItems), q)
	}
	for i := range wantItems {
		if gotItems[i] == nil || wantItems[i] == nil || !reflect.DeepEqual(*gotItems[i], *wantItems[i]) {
			t.Fatalf("Query item mismatch at i=%d: got=%#v want=%#v q=%+v", i, gotItems[i], wantItems[i], q)
		}
	}

	base := cloneQuery(q)
	base.Order = nil
	base.Offset = 0
	base.Limit = 0
	wantCount, err := db.Count(base)
	if err != nil {
		t.Fatalf("Count(base %+v): %v", base, err)
	}
	gotCount, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count(%+v): %v", q, err)
	}
	if gotCount != wantCount {
		t.Fatalf("Count mismatch: got=%d want=%d q=%+v", gotCount, wantCount, q)
	}
}

func TestQueryExt_OrderBasicPointerBounds_DoNotLeakNilTail(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	rows := map[uint64]*Rec{
		1: {Name: "nil-a", Opt: nil, Active: true},
		2: {Name: "empty", Opt: strPtr(""), Active: true},
		3: {Name: "aa-a", Opt: strPtr("aa"), Active: true},
		4: {Name: "ab-a", Opt: strPtr("ab"), Active: true},
		5: {Name: "ac-off", Opt: strPtr("ac"), Active: false},
		6: {Name: "ba-a", Opt: strPtr("ba"), Active: true},
		7: {Name: "nil-b", Opt: nil, Active: true},
		8: {Name: "aa-off", Opt: strPtr("aa"), Active: false},
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
			name: "gt_asc",
			q:    qx.Query(qx.GT("opt", "aa")).By("opt", qx.ASC).Max(8),
		},
		{
			name: "lte_active_desc",
			q:    qx.Query(qx.LTE("opt", "aa"), qx.EQ("active", true)).By("opt", qx.DESC).Max(8),
		},
		{
			name: "gte_window",
			q:    qx.Query(qx.GTE("opt", "ab")).By("opt", qx.ASC).Skip(1).Max(2),
		},
	}

	check := func(step string) {
		for _, tc := range tests {
			t.Run(step+"_"+tc.name, func(t *testing.T) {
				assertQueryExtraPublicReadPathsMatchExpected(t, db, tc.q)
			})
		}
	}

	check("initial")

	if err := db.Patch(1, []Field{{Name: "opt", Value: "ad"}}); err != nil {
		t.Fatalf("Patch(1 opt=ad): %v", err)
	}
	if err := db.Patch(4, []Field{{Name: "opt", Value: (*string)(nil)}}); err != nil {
		t.Fatalf("Patch(4 opt=nil): %v", err)
	}
	if err := db.Patch(7, []Field{{Name: "opt", Value: "aa"}}); err != nil {
		t.Fatalf("Patch(7 opt=aa): %v", err)
	}

	check("after_patch")
}

func TestQueryExt_PrefixRangeIntersections_StayExactAcrossBoundaryChurn(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	rows := map[uint64]*Rec{
		1: {Name: "aa", Active: true},
		2: {Name: "aa/", Active: true},
		3: {Name: "aa/0", Active: true},
		4: {Name: "aa/00", Active: true},
		5: {Name: "aa/9", Active: true},
		6: {Name: "aa0", Active: true},
		7: {Name: "aa1", Active: true},
		8: {Name: "ab", Active: true},
		9: {Name: "b", Active: true},
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
			name: "bounded_prefix_window",
			q: qx.Query(
				qx.PREFIX("name", "aa/"),
				qx.GTE("name", "aa/0"),
				qx.LT("name", "aa0"),
			).By("name", qx.ASC).Max(10),
		},
		{
			name: "prefix_singleton_upper_edge",
			q: qx.Query(
				qx.PREFIX("name", "aa/"),
				qx.LTE("name", "aa/"),
			).By("name", qx.ASC).Max(10),
		},
		{
			name: "prefix_empty_after_crossing_upper",
			q: qx.Query(
				qx.PREFIX("name", "aa/"),
				qx.GTE("name", "aa0"),
			).By("name", qx.ASC).Max(10),
		},
		{
			name: "bounded_prefix_desc_window",
			q: qx.Query(
				qx.PREFIX("name", "aa/"),
				qx.GT("name", "aa/"),
				qx.LTE("name", "aa/zz"),
			).By("name", qx.DESC).Skip(1).Max(3),
		},
	}

	check := func(step string) {
		for _, tc := range tests {
			t.Run(step+"_"+tc.name, func(t *testing.T) {
				assertQueryExtraPublicReadPathsMatchExpected(t, db, tc.q)
			})
		}
	}

	check("initial")

	if err := db.Patch(6, []Field{{Name: "name", Value: "aa/zz"}}); err != nil {
		t.Fatalf("Patch(6 name=aa/zz): %v", err)
	}
	if err := db.Delete(3); err != nil {
		t.Fatalf("Delete(3): %v", err)
	}
	if err := db.Set(10, &Rec{Name: "aa/5", Active: true}); err != nil {
		t.Fatalf("Set(10): %v", err)
	}

	check("after_patch")
}

func TestQueryExt_MixedCaching_NumericRangesRemainExactAcrossClearAndPublish(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 8,
	})

	seedGeneratedUint64Data(t, db, 20_000, func(i int) *Rec {
		countries := []string{"US", "DE", "FR", "NL"}
		return &Rec{
			Name:   fmt.Sprintf("user-%05d", i),
			Age:    i,
			Score:  float64(i),
			Active: i%2 == 0,
			Meta: Meta{
				Country: countries[i%len(countries)],
			},
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	setNumericBucketKnobs(t, db, 128, 1, 1)

	queries := []*qx.QX{
		qx.Query(
			qx.LT("score", 15_000.0),
			qx.EQ("active", true),
		).By("age", qx.ASC).Skip(3_500).Max(80),
		qx.Query(
			qx.LT("score", 15_001.0),
			qx.EQ("active", true),
		).By("age", qx.ASC).Skip(3_500).Max(80),
		qx.Query(
			qx.GTE("score", 6_000.0),
			qx.LT("score", 17_000.0),
			qx.EQ("country", "US"),
		).By("age", qx.DESC).Skip(900).Max(60),
	}

	checkQueries := func(step string) {
		for i, q := range queries {
			t.Run(fmt.Sprintf("%s_q%d", step, i), func(t *testing.T) {
				assertQueryExtraPublicReadPathsMatchExpected(t, db, q)
			})
		}
	}

	checkQueries("warm")
	if got := db.getSnapshot().matPredCacheCount.Load(); got == 0 {
		t.Fatalf("expected shared runtime caches to warm up")
	}

	for i := 1; i <= 12; i++ {
		if err := db.Patch(uint64(i), []Field{{Name: "name", Value: fmt.Sprintf("mut-%d", i)}}); err != nil {
			t.Fatalf("Patch(%d name): %v", i, err)
		}
	}
	checkQueries("after_unrelated_publish")

	db.clearCurrentSnapshotCachesForTesting()
	snap := db.getSnapshot()
	if got := snap.matPredCacheCount.Load(); got != 0 {
		t.Fatalf("expected cleared materialized predicate cache, got=%d", got)
	}

	checkQueries("after_clear")
	if got := db.getSnapshot().matPredCacheCount.Load(); got == 0 {
		t.Fatalf("expected caches to repopulate after cold queries")
	}
}

func TestQueryExt_ConcurrentEvictingMaterializedPredicates_RemainStable(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 1,
	})

	setNumericBucketKnobs(t, db, 128, 1, 1)
	seedGeneratedUint64Data(t, db, 5_000, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%05d", i),
			Age:   i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	type expectation struct {
		q     *qx.QX
		ids   []uint64
		items []*Rec
		count uint64
	}

	queries := []*qx.QX{
		qx.Query(qx.LT("score", 4_000.0)).By("age", qx.ASC).Skip(2_000).Max(40),
		qx.Query(qx.LT("score", 4_001.0)).By("age", qx.ASC).Skip(2_000).Max(40),
		qx.Query(qx.LT("score", 3_999.0)).By("age", qx.ASC).Skip(2_000).Max(40),
	}
	expects := make([]expectation, len(queries))
	for i, q := range queries {
		ids, err := expectedKeysUint64(t, db, q)
		if err != nil {
			t.Fatalf("expectedKeysUint64(q%d): %v", i, err)
		}
		items, err := db.BatchGet(ids...)
		if err != nil {
			t.Fatalf("BatchGet(q%d): %v", i, err)
		}
		countQ := cloneQuery(q)
		countQ.Order = nil
		countQ.Offset = 0
		countQ.Limit = 0
		count, err := db.Count(countQ)
		if err != nil {
			t.Fatalf("Count(q%d base): %v", i, err)
		}
		expects[i] = expectation{
			q:     q,
			ids:   ids,
			items: items,
			count: count,
		}
	}

	if _, err := db.QueryKeys(queries[0]); err != nil {
		t.Fatalf("warm QueryKeys: %v", err)
	}
	if got := db.getSnapshot().matPredCacheCount.Load(); got == 0 {
		t.Fatalf("expected deep ordered window to materialize a predicate cache entry")
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 16)

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 120; i++ {
				exp := expects[(gid+i)%len(expects)]

				gotKeys, err := db.QueryKeys(exp.q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys: %w", gid, i, err)
					return
				}
				if !queryIDsEqual(exp.q, gotKeys, exp.ids) {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys mismatch: got=%v want=%v", gid, i, gotKeys, exp.ids)
					return
				}

				gotItems, err := db.Query(exp.q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Query: %w", gid, i, err)
					return
				}
				if len(gotItems) != len(exp.items) {
					errCh <- fmt.Errorf("g=%d i=%d Query len mismatch: got=%d want=%d", gid, i, len(gotItems), len(exp.items))
					return
				}
				for j := range exp.items {
					if gotItems[j] == nil || exp.items[j] == nil || !reflect.DeepEqual(*gotItems[j], *exp.items[j]) {
						errCh <- fmt.Errorf("g=%d i=%d Query item mismatch at j=%d", gid, i, j)
						return
					}
				}

				gotCount, err := db.Count(exp.q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Count: %w", gid, i, err)
					return
				}
				if gotCount != exp.count {
					errCh <- fmt.Errorf("g=%d i=%d Count mismatch: got=%d want=%d", gid, i, gotCount, exp.count)
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	if got := db.getSnapshot().matPredCacheCount.Load(); got > 1 {
		t.Fatalf("materialized predicate cache exceeded configured bound: got=%d", got)
	}
}

func TestQueryExt_StringKeys_ConcurrentPrefixRangeSnapshotConsistency(t *testing.T) {
	db, _ := openTempDBString(t, Options{AnalyzeInterval: -1})

	ids := []string{"id-1", "id-2", "id-3", "id-4", "id-5", "id-6", "id-7"}
	stateA := []*Rec{
		{Name: "aa/00", Active: true},
		{Name: "aa/01", Active: true},
		{Name: "aa/02", Active: false},
		{Name: "aa/zz", Active: true},
		{Name: "ab/00", Active: true},
		{Name: "aa0", Active: true},
		{Name: "aa/05", Active: true},
	}
	stateB := []*Rec{
		{Name: "aa/00", Active: false},
		{Name: "aa/03", Active: true},
		{Name: "aa/yy", Active: true},
		{Name: "aa/zz", Active: false},
		{Name: "aa/05", Active: true},
		{Name: "aa0", Active: true},
		{Name: "aa/04", Active: true},
	}

	setState := func(vals []*Rec) error {
		return db.BatchSet(ids, vals)
	}

	q := qx.Query(
		qx.PREFIX("name", "aa/"),
		qx.GT("name", "aa/00"),
		qx.LTE("name", "aa/zz"),
		qx.EQ("active", true),
	).By("name", qx.DESC).Skip(1).Max(3)

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA): %v", err)
	}
	wantA, err := expectedKeysString(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysString(stateA): %v", err)
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
	wantB, err := expectedKeysString(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysString(stateB): %v", err)
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
		for i := 0; i < 120; i++ {
			vals := stateB
			if i%2 == 1 {
				vals = stateA
			}
			if err := setState(vals); err != nil {
				errCh <- fmt.Errorf("writer: %w", err)
				return
			}
		}
	}()

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			<-start
			for i := 0; i < 160; i++ {
				gotKeys, err := db.QueryKeys(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys: %w", gid, i, err)
					return
				}
				if !queryStringIDsEqual(q, gotKeys, wantA) && !queryStringIDsEqual(q, gotKeys, wantB) {
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
				if !reflect.DeepEqual(gotNames, namesA) && !reflect.DeepEqual(gotNames, namesB) {
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

func TestQueryExt_NumericRangeFieldMutation_DoesNotReuseStaleCaches(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 8,
	})

	seedGeneratedUint64Data(t, db, 15_000, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("user-%05d", i),
			Age:    i,
			Score:  float64(i),
			Active: i%2 == 0,
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	setNumericBucketKnobs(t, db, 128, 1, 1)

	queries := []*qx.QX{
		qx.Query(
			qx.GTE("age", 2_500),
			qx.EQ("active", true),
		).By("age", qx.ASC).Skip(120).Max(80),
		qx.Query(
			qx.GTE("age", 2_501),
			qx.EQ("active", true),
		).By("age", qx.ASC).Skip(120).Max(80),
		qx.Query(
			qx.GTE("age", 2_400),
			qx.LT("age", 2_650),
		).By("age", qx.ASC).Max(120),
	}

	checkQueries := func(step string) {
		for i, q := range queries {
			t.Run(fmt.Sprintf("%s_q%d", step, i), func(t *testing.T) {
				assertQueryExtraPublicReadPathsMatchExpected(t, db, q)
			})
		}
	}

	checkQueries("warm")
	if got := db.getSnapshot().matPredCacheCount.Load(); got == 0 {
		t.Fatalf("expected warmed numeric-range query to populate predicate cache")
	}

	for i := 2400; i <= 2480; i++ {
		if err := db.Patch(uint64(i), []Field{{Name: "age", Value: 9_000 + i}, {Name: "active", Value: true}}); err != nil {
			t.Fatalf("Patch(%d high): %v", i, err)
		}
	}
	for i := 14900; i <= 14980; i++ {
		if err := db.Patch(uint64(i), []Field{{Name: "age", Value: 2_430 + (i - 14900)}}); err != nil {
			t.Fatalf("Patch(%d low): %v", i, err)
		}
	}

	checkQueries("after_field_publish")
	db.clearCurrentSnapshotCachesForTesting()
	checkQueries("after_clear")
}

func TestQueryExt_OrderedOROverlap_DeduplicatesAcrossMutations(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	seedGeneratedUint64Data(t, db, 4_000, func(i int) *Rec {
		group := "grp-b"
		if i%2 == 0 {
			group = "grp-a"
		}
		country := "US"
		if i%5 == 0 {
			country = "NL"
		}
		return &Rec{
			Name:   fmt.Sprintf("u-%04d", i),
			Email:  fmt.Sprintf("%s/%04d@example.test", group, i),
			Age:    18 + (i % 70),
			Score:  float64(i % 100),
			Active: i%3 == 0,
			Meta: Meta{
				Country: country,
			},
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query(
		qx.OR(
			qx.AND(qx.PREFIX("email", "grp-a/"), qx.EQ("active", true)),
			qx.AND(qx.PREFIX("email", "grp-a/"), qx.GTE("age", 40)),
			qx.AND(qx.PREFIX("email", "grp-b/"), qx.EQ("country", "NL")),
			qx.AND(qx.PREFIX("email", "grp-a/"), qx.LTE("score", 15.0)),
		),
	).By("score", qx.DESC).Skip(10).Max(120)

	assertQueryExtraPublicReadPathsMatchExpected(t, db, q)

	for _, id := range []uint64{12, 48, 96, 144, 192, 384} {
		if err := db.Patch(id, []Field{
			{Name: "email", Value: fmt.Sprintf("grp-b/%04d@example.test", id)},
			{Name: "country", Value: "NL"},
			{Name: "active", Value: false},
			{Name: "score", Value: 7.0},
		}); err != nil {
			t.Fatalf("Patch(%d demote): %v", id, err)
		}
	}
	for _, id := range []uint64{11, 33, 55, 77, 99, 121} {
		if err := db.Patch(id, []Field{
			{Name: "email", Value: fmt.Sprintf("grp-a/%04d@example.test", id)},
			{Name: "age", Value: 63},
			{Name: "active", Value: true},
			{Name: "score", Value: 99.0},
		}); err != nil {
			t.Fatalf("Patch(%d promote): %v", id, err)
		}
	}

	assertQueryExtraPublicReadPathsMatchExpected(t, db, q)
}

func TestQueryExt_OrderBasicPointerPrefixWithNegativeBaseOps_RemainsExact(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	rows := map[uint64]*Rec{
		1:  {Name: "nil-a", Opt: nil, Active: true},
		2:  {Name: "aa-on", Opt: strPtr("aa"), Active: true},
		3:  {Name: "ab-off", Opt: strPtr("ab"), Active: false},
		4:  {Name: "ac-on", Opt: strPtr("ac"), Active: true},
		5:  {Name: "ad-off", Opt: strPtr("ad"), Active: false},
		6:  {Name: "ba-off", Opt: strPtr("ba"), Active: false},
		7:  {Name: "empty-off", Opt: strPtr(""), Active: false},
		8:  {Name: "az-on", Opt: strPtr("az"), Active: true},
		9:  {Name: "nil-b", Opt: nil, Active: false},
		10: {Name: "ax-off", Opt: strPtr("ax"), Active: false},
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
			name: "prefix_not_active_asc",
			q: qx.Query(
				qx.PREFIX("opt", "a"),
				qx.NOT(qx.EQ("active", true)),
			).By("opt", qx.ASC).Skip(1).Max(4),
		},
		{
			name: "prefix_not_active_desc",
			q: qx.Query(
				qx.PREFIX("opt", "a"),
				qx.NOT(qx.EQ("active", true)),
			).By("opt", qx.DESC).Max(5),
		},
		{
			name: "prefix_not_name",
			q: qx.Query(
				qx.PREFIX("opt", "a"),
				qx.NOT(qx.EQ("name", "ax-off")),
			).By("opt", qx.ASC).Max(6),
		},
	}

	check := func(step string) {
		for _, tc := range tests {
			t.Run(step+"_"+tc.name, func(t *testing.T) {
				assertQueryExtraPublicReadPathsMatchExpected(t, db, tc.q)
			})
		}
	}

	check("initial")

	if err := db.Patch(1, []Field{{Name: "opt", Value: "ae"}, {Name: "active", Value: false}}); err != nil {
		t.Fatalf("Patch(1): %v", err)
	}
	if err := db.Patch(4, []Field{{Name: "opt", Value: (*string)(nil)}}); err != nil {
		t.Fatalf("Patch(4): %v", err)
	}
	if err := db.Patch(8, []Field{{Name: "active", Value: false}}); err != nil {
		t.Fatalf("Patch(8): %v", err)
	}
	if err := db.Set(11, &Rec{Name: "af-off", Opt: strPtr("af"), Active: false}); err != nil {
		t.Fatalf("Set(11): %v", err)
	}

	check("after_patch")
}

func TestQueryExt_ConcurrentAtomicBatchSetSnapshotConsistency_OnNumericRangeOrder(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 8,
	})

	setNumericBucketKnobs(t, db, 64, 1, 1)

	ids := make([]uint64, 0, 64)
	stateA := make([]*Rec, 0, 64)
	stateB := make([]*Rec, 0, 64)
	for i := 1; i <= 64; i++ {
		id := uint64(i)
		ids = append(ids, id)
		stateA = append(stateA, &Rec{
			Name:   fmt.Sprintf("A-%02d", i),
			Age:    100 + i*3,
			Score:  float64(100 + i*3),
			Active: i%3 != 0,
		})
		stateB = append(stateB, &Rec{
			Name:   fmt.Sprintf("B-%02d", i),
			Age:    200 + i*5,
			Score:  float64(200 + i*5),
			Active: i%4 != 0,
		})
	}

	setState := func(vals []*Rec) error {
		return db.BatchSet(ids, vals)
	}

	q := qx.Query(
		qx.GTE("age", 130),
		qx.LT("age", 420),
		qx.EQ("active", true),
	).By("age", qx.ASC).Skip(4).Max(18)

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA): %v", err)
	}
	if _, err := db.QueryKeys(q); err != nil {
		t.Fatalf("warm QueryKeys(stateA): %v", err)
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

	if err = setState(stateB); err != nil {
		t.Fatalf("BatchSet(stateB): %v", err)
	}
	if _, err := db.QueryKeys(q); err != nil {
		t.Fatalf("warm QueryKeys(stateB): %v", err)
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
		for i := 0; i < 120; i++ {
			vals := stateB
			if i%2 == 1 {
				vals = stateA
			}
			if err := setState(vals); err != nil {
				errCh <- fmt.Errorf("writer: %w", err)
				return
			}
		}
	}()

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			<-start
			for i := 0; i < 160; i++ {
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
				if !reflect.DeepEqual(gotNames, namesA) && !reflect.DeepEqual(gotNames, namesB) {
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
