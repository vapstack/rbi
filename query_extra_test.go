package rbi

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

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

func clearQueryExtOrderWindow(q *qx.QX) {
	clearQueryOrderWindowForTest(q)
}

func execPreparedQueryExt[K ~uint64 | ~string](db *DB[K, Rec], q *qx.QX) ([]K, error) {
	return execPreparedQueryForTest(db, q)
}

func queryExtSortByArrayPos(q *qx.QX, field string, priority []string, dir qx.OrderDirection) *qx.QX {
	return q.SortBy(qx.POS(field, priority), dir)
}

func queryExtSortByArrayCount(q *qx.QX, field string, dir qx.OrderDirection) *qx.QX {
	return q.SortBy(qx.LEN(field), dir)
}

func assertQueryExtIDsMatchExpected(t *testing.T, db *DB[uint64, Rec], q *qx.QX) []uint64 {
	t.Helper()
	contract := newUint64QueryContract(t, db)
	contract.AssertQueryKeysMatchReference(q)
	return contract.ReferenceKeys(q)
}

func assertQueryExtItemsMatchExpected(t *testing.T, db *DB[uint64, Rec], q *qx.QX) {
	t.Helper()
	newUint64QueryContract(t, db).AssertQueryRecordsMatchReference(q)
}

func assertQueryExtCountMatchesOrderedResultSet(t *testing.T, db *DB[uint64, Rec], q *qx.QX) {
	t.Helper()

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys(%+v): %v", q, err)
	}
	cnt, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count(%+v): %v", q, err)
	}
	if cnt != uint64(len(got)) {
		t.Fatalf("Count(%+v)=%d does not match len(QueryKeys)=%d", q, cnt, len(got))
	}
}

func assertQueryExtCountMatchesBaseQuery(t *testing.T, db *DB[uint64, Rec], q *qx.QX) {
	t.Helper()
	newUint64QueryContract(t, db).AssertCountMatchesReference(q)
}

func assertQueryExtPreparedMatchesExpected(t *testing.T, db *DB[uint64, Rec], q *qx.QX) {
	t.Helper()
	contract := newUint64QueryContract(t, db)
	contract.AssertPreparedKeysMatchReference(q)
	contract.AssertPreparedCountMatchesReference(q)
}

func assertQueryExtAllReadPathsMatchExpected(t *testing.T, db *DB[uint64, Rec], q *qx.QX) {
	t.Helper()
	newUint64QueryContract(t, db).AssertAllReadPathsMatchReference(q)
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
	clearQueryExtOrderWindow(countQ)
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

				gotCount, err := db.Count(q.Filter)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Count: %w", gid, i, err)
					return
				}
				if gotCount != wantCount {
					errCh <- fmt.Errorf("g=%d i=%d Count mismatch: got=%d want=%d", gid, i, gotCount, wantCount)
					return
				}

				if withPrepared {
					gotPrepared, err := execPreparedQueryExt(db, nq)
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
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_AllPriorities_DESC_IncludesNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", queryExtOptPriority(), qx.DESC))
}

func TestQueryExt_ArrayPosPointer_ActiveFilter_ASC_IncludesNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(qx.EQ("active", true)), "opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_ActiveFilter_DESC_IncludesNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(qx.EQ("active", true)), "opt", queryExtOptPriority(), qx.DESC))
}

func TestQueryExt_ArrayPosPointer_EqNilOnly_ASC_ReturnsNilRows(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(qx.EQ("opt", nil)), "opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_EqNilOnly_DESC_ReturnsNilRows(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(qx.EQ("opt", nil)), "opt", queryExtOptPriority(), qx.DESC))
}

func TestQueryExt_ArrayPosPointer_SkipWindowIntoNilTail_ASC(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", queryExtOptPriority(), qx.ASC).Offset(4).Limit(8))
}

func TestQueryExt_ArrayPosPointer_SkipWindowIntoNilTail_DESC(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", queryExtOptPriority(), qx.DESC).Offset(3).Limit(3))
}

func TestQueryExt_ArrayPosPointer_LimitWindowCrossesIntoNilTail_ASC(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", queryExtOptPriority(), qx.ASC).Limit(5))
}

func TestQueryExt_ArrayPosPointer_NegativePredicate_RetainsNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(qx.NOT(qx.EQ("active", true))), "opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_PatchToNil_RetainsExpandedNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	if err := db.Patch(6, []Field{{Name: "opt", Value: (*string)(nil)}}); err != nil {
		t.Fatalf("Patch(6 opt=nil): %v", err)
	}
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_PatchFromNilToValue_RetainsRemainingNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	if err := db.Patch(1, []Field{{Name: "opt", Value: "delta"}}); err != nil {
		t.Fatalf("Patch(1 opt=delta): %v", err)
	}
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", []string{"alpha", "beta", "", "gamma", "delta"}, qx.ASC))
}

func TestQueryExt_ArrayPosPointer_DeleteNonNil_RetainsNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	if err := db.Delete(6); err != nil {
		t.Fatalf("Delete(6): %v", err)
	}
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_InsertNil_RetainsNewNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	if err := db.Set(7, &Rec{Name: "nil-3", Opt: nil, Active: true}); err != nil {
		t.Fatalf("Set(7): %v", err)
	}
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_QueryValues_AllPriorities_ASC_RetainsNilTail(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtItemsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_CountMatchesOrderedResultSet_WhenUnbounded(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtCountMatchesOrderedResultSet(t, db, queryExtSortByArrayPos(qx.Query(), "opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_AllNilDataset_ASC_WithPriorities(t *testing.T) {
	db := seedQueryExtOptAllNilDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_AllNilDataset_DESC_WithPriorities(t *testing.T) {
	db := seedQueryExtOptAllNilDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", queryExtOptPriority(), qx.DESC))
}

func TestQueryExt_ArrayPosPointer_AllNilDataset_ASC_EmptyPriorities(t *testing.T) {
	db := seedQueryExtOptAllNilDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", []string{}, qx.ASC))
}

func TestQueryExt_ArrayPosPointer_ASC_EmptyPriorities_FallsBackToIDOrder(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", []string{}, qx.ASC))
}

func TestQueryExt_ArrayPosPointer_AllNilDataset_Window_EmptyPriorities(t *testing.T) {
	db := seedQueryExtOptAllNilDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", []string{}, qx.ASC).Offset(1).Limit(2))
}

func TestQueryExt_ArrayPosPointer_AllNilAfterPatches_ASC(t *testing.T) {
	db := seedQueryExtOptArrayPosDB(t)
	for _, id := range []uint64{2, 3, 4, 6} {
		if err := db.Patch(id, []Field{{Name: "opt", Value: (*string)(nil)}}); err != nil {
			t.Fatalf("Patch(%d opt=nil): %v", id, err)
		}
	}
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(), "opt", queryExtOptPriority(), qx.ASC))
}

func TestQueryExt_ArrayPosPointer_AllNilPredicate_DESC(t *testing.T) {
	db := seedQueryExtOptAllNilDB(t)
	assertQueryExtIDsMatchExpected(t, db, queryExtSortByArrayPos(qx.Query(qx.EQ("active", true)), "opt", []string{"alpha"}, qx.DESC))
}

func TestQueryExt_CountIgnoresUnknownBasicOrderField(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 64)
	assertQueryExtCountMatchesBaseQuery(t, db, qx.Query(qx.EQ("active", true)).Sort("no_such_field", qx.ASC).Offset(7).Limit(9))
}

func TestQueryExt_CountIgnoresUnknownArrayPosOrderField(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 64)
	assertQueryExtCountMatchesBaseQuery(t, db, queryExtSortByArrayPos(qx.Query(qx.EQ("active", true)), "no_such_field", []string{"a", "b"}, qx.ASC))
}

func TestQueryExt_CountIgnoresUnknownArrayCountOrderField(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 64)
	assertQueryExtCountMatchesBaseQuery(t, db, queryExtSortByArrayCount(qx.Query(qx.EQ("active", true)), "no_such_field", qx.DESC))
}

func TestQueryExt_CountIgnoresUnknownSecondaryOrderField(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 64)

	q := qx.Query(qx.EQ("active", true))
	q.Order = []qx.Order{
		{By: qx.REF("age")},
		{By: qx.REF("no_such_field"), Desc: true},
	}
	assertQueryExtCountMatchesBaseQuery(t, db, q)
}

func TestQueryExt_CountIgnoresUnknownOrderFieldOnNoopQuery(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 64)
	assertQueryExtCountMatchesBaseQuery(t, db, qx.Query().Sort("no_such_field", qx.DESC))
}

func TestQueryExt_CountIgnoresUnknownOrderFieldOnEmptyDB(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	assertQueryExtCountMatchesBaseQuery(t, db, qx.Query(qx.EQ("active", true)).Sort("no_such_field", qx.ASC))
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
			q:    qx.Query().Sort("opt", qx.ASC).Offset(2).Limit(7),
		},
		{
			name: "desc_active_window",
			q:    qx.Query(qx.EQ("active", true)).Sort("opt", qx.DESC).Offset(1).Limit(5),
		},
		{
			name: "negated_filter_asc",
			q:    qx.Query(qx.NOT(qx.EQ("active", true))).Sort("opt", qx.ASC).Offset(1).Limit(6),
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
			q:    queryExtSortByArrayCount(qx.Query(), "tags", qx.ASC).Offset(1).Limit(7),
		},
		{
			name: "desc_active_window",
			q:    queryExtSortByArrayCount(qx.Query(qx.EQ("active", true)), "tags", qx.DESC).Offset(1).Limit(4),
		},
		{
			name: "asc_negated_filter",
			q:    queryExtSortByArrayCount(qx.Query(qx.NOT(qx.EQ("active", false))), "tags", qx.ASC).Limit(6),
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
	).Sort("score", qx.DESC).Offset(10).Limit(80)
	assertQueryExtConcurrentReadStable(t, db, q, true)
}

func TestQueryExt_ConcurrentSharedArrayCountQuery_IsStable(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 1_200)
	q := queryExtSortByArrayCount(qx.Query(
		qx.NOT(qx.EQ("active", false)),
	), "tags", qx.DESC).Offset(3).Limit(70)
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

	q := qx.Query(qx.EQ("active", true)).Sort("opt", qx.ASC).Offset(1).Limit(3)

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
	countA, err := db.Count(q.Filter)
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
	countB, err := db.Count(q.Filter)
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

				gotCount, err := db.Count(q.Filter)
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

	q := qx.Query(qx.EQ("active", true)).Sort("opt", qx.ASC).Offset(1).Limit(3)
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
				got, err := execPreparedQueryExt(db, nq)
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

func queryExtOrderedORNegativeResidualFixture() ([]uint64, []*Rec, []*Rec, *qx.QX) {
	ids := []uint64{1, 2, 3, 4, 5, 6, 7, 8}

	stateA := []*Rec{
		{Name: "A1-fr-low", Age: 20, Score: 30, Active: false, Meta: Meta{Country: "FR"}},
		{Name: "alice", Age: 21, Score: 40, Active: false, Meta: Meta{Country: "NL"}},
		{Name: "A3-de-hi", Age: 22, Score: 70, Active: true, Meta: Meta{Country: "DE"}},
		{Name: "A4-us-mid", Age: 23, Score: 60, Active: false, Meta: Meta{Country: "US"}},
		{Name: "A5-pl-mid", Age: 24, Score: 50, Active: true, Meta: Meta{Country: "PL"}},
		{Name: "A6-nl-none", Age: 25, Score: 80, Active: false, Meta: Meta{Country: "NL"}},
		{Name: "alice", Age: 26, Score: 54, Active: false, Meta: Meta{Country: "FR"}},
		{Name: "alice", Age: 27, Score: 53, Active: true, Meta: Meta{Country: "DE"}},
	}

	stateB := []*Rec{
		{Name: "B1-nl-none", Age: 35, Score: 30, Active: false, Meta: Meta{Country: "NL"}},
		{Name: "alice", Age: 36, Score: 54, Active: false, Meta: Meta{Country: "FR"}},
		{Name: "B3-de-low", Age: 37, Score: 48, Active: true, Meta: Meta{Country: "DE"}},
		{Name: "B4-us-low", Age: 38, Score: 20, Active: false, Meta: Meta{Country: "US"}},
		{Name: "B5-de-none", Age: 39, Score: 90, Active: false, Meta: Meta{Country: "DE"}},
		{Name: "B6-pl-hi", Age: 40, Score: 80, Active: false, Meta: Meta{Country: "PL"}},
		{Name: "alice", Age: 41, Score: 40, Active: false, Meta: Meta{Country: "NL"}},
		{Name: "alice", Age: 42, Score: 53, Active: true, Meta: Meta{Country: "FR"}},
	}

	q := qx.Query(
		qx.OR(
			qx.NOTIN("country", []string{"NL", "DE"}),
			qx.AND(
				qx.EQ("name", "alice"),
				qx.LT("score", 55.0),
			),
			qx.AND(
				qx.EQ("active", true),
				qx.GTE("score", 45.0),
			),
		),
	).Sort("age", qx.ASC).Offset(2).Limit(4)

	return ids, stateA, stateB, q
}

func queryExtStringOrderedORFixture() ([]string, []*Rec, []*Rec, *qx.QX) {
	ids := []string{"id-1", "id-2", "id-3", "id-4", "id-5", "id-6", "id-7"}

	stateA := []*Rec{
		{Name: "aa/00", Active: true, Score: 10, Meta: Meta{Country: "US"}},
		{Name: "aa/01", Active: false, Score: 40, Meta: Meta{Country: "FR"}},
		{Name: "aa/02", Active: true, Score: 70, Meta: Meta{Country: "NL"}},
		{Name: "ab/00", Active: false, Score: 30, Meta: Meta{Country: "NL"}},
		{Name: "ab/01", Active: true, Score: 60, Meta: Meta{Country: "US"}},
		{Name: "ac/00", Active: true, Score: 50, Meta: Meta{Country: "FR"}},
		{Name: "zz/00", Active: true, Score: 5, Meta: Meta{Country: "DE"}},
	}

	stateB := []*Rec{
		{Name: "aa/00", Active: false, Score: 10, Meta: Meta{Country: "US"}},
		{Name: "aa/03", Active: true, Score: 20, Meta: Meta{Country: "FR"}},
		{Name: "aa/04", Active: true, Score: 40, Meta: Meta{Country: "DE"}},
		{Name: "ab/00", Active: true, Score: 30, Meta: Meta{Country: "NL"}},
		{Name: "ab/02", Active: false, Score: 60, Meta: Meta{Country: "US"}},
		{Name: "ab/03", Active: true, Score: 80, Meta: Meta{Country: "NL"}},
		{Name: "ac/00", Active: true, Score: 50, Meta: Meta{Country: "FR"}},
	}

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.PREFIX("name", "aa/"),
				qx.EQ("active", true),
			),
			qx.AND(
				qx.PREFIX("name", "ab/"),
				qx.EQ("country", "NL"),
			),
			qx.AND(
				qx.EQ("name", "aa/03"),
				qx.LT("score", 25.0),
			),
		),
	).Sort("name", qx.ASC).Offset(1).Limit(3)

	return ids, stateA, stateB, q
}

func queryExtNoOrderORDisjointFixture() ([]uint64, []*Rec, []*Rec, *qx.QX) {
	ids := []uint64{1, 2, 3, 4, 5, 6, 7, 8}

	stateA := []*Rec{
		{Name: "A1-fr", Email: "cold/01", Active: false, Age: 20, Score: 20, Meta: Meta{Country: "FR"}},
		{Name: "A2-hot", Email: "hot/02", Active: false, Age: 21, Score: 30, Meta: Meta{Country: "US"}},
		{Name: "A3-miss", Email: "cold/03", Active: false, Age: 22, Score: 40, Meta: Meta{Country: "PL"}},
		{Name: "A4-fr", Email: "cold/04", Active: false, Age: 23, Score: 50, Meta: Meta{Country: "FR"}},
		{Name: "A5-off", Email: "cold/05", Active: false, Age: 45, Score: 90, Meta: Meta{Country: "NL"}},
		{Name: "A6-off", Email: "hot/06", Active: false, Age: 46, Score: 80, Meta: Meta{Country: "DE"}},
		{Name: "A7-off", Email: "cold/07", Active: false, Age: 47, Score: 70, Meta: Meta{Country: "NL"}},
		{Name: "A8-off", Email: "hot/08", Active: false, Age: 48, Score: 60, Meta: Meta{Country: "DE"}},
	}

	stateB := []*Rec{
		{Name: "B1-off", Email: "cold/01", Active: false, Age: 20, Score: 80, Meta: Meta{Country: "NL"}},
		{Name: "B2-off", Email: "hot/02", Active: false, Age: 21, Score: 80, Meta: Meta{Country: "DE"}},
		{Name: "B3-off", Email: "cold/03", Active: false, Age: 22, Score: 80, Meta: Meta{Country: "NL"}},
		{Name: "B4-off", Email: "hot/04", Active: false, Age: 23, Score: 80, Meta: Meta{Country: "DE"}},
		{Name: "B5-nl", Email: "vip/05", Active: true, Age: 45, Score: 40, Meta: Meta{Country: "NL"}},
		{Name: "B6-nl", Email: "vip/06", Active: true, Age: 46, Score: 50, Meta: Meta{Country: "NL"}},
		{Name: "B7-nl", Email: "vip/07", Active: true, Age: 47, Score: 60, Meta: Meta{Country: "NL"}},
		{Name: "B8-nl", Email: "vip/08", Active: true, Age: 48, Score: 70, Meta: Meta{Country: "NL"}},
	}

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.EQ("country", "FR"),
				qx.LT("score", 55.0),
			),
			qx.AND(
				qx.PREFIX("email", "hot/"),
				qx.LT("score", 55.0),
			),
			qx.AND(
				qx.EQ("active", true),
				qx.GTE("age", 45),
				qx.EQ("country", "NL"),
			),
		),
	).Offset(1).Limit(3)

	return ids, stateA, stateB, q
}

func queryExtRecSignature(rec *Rec) string {
	return queryContractRecSignature(rec)
}

func queryExtBuildSignatureCounts(items []*Rec) map[string]int {
	return queryContractBuildRecSignatureCounts(items)
}

func queryExtValidateNoOrderItemsAgainstFullSet(q *qx.QX, items []*Rec, fullSigCounts map[string]int, fullLen int) error {
	seen := make(map[string]int, len(items))
	for i := range items {
		if items[i] == nil {
			return fmt.Errorf("nil item at i=%d", i)
		}
		sig := queryExtRecSignature(items[i])
		limit, ok := fullSigCounts[sig]
		if !ok {
			return fmt.Errorf("item %q outside full result set", sig)
		}
		seen[sig]++
		if seen[sig] > limit {
			return fmt.Errorf("duplicate item %q exceeds full-set multiplicity", sig)
		}
	}

	maxLen := fullLen
	if q.Window.Offset >= uint64(fullLen) {
		maxLen = 0
	} else if q.Window.Offset > 0 {
		maxLen = fullLen - int(q.Window.Offset)
	}
	if q.Window.Limit > 0 && int(q.Window.Limit) < maxLen {
		maxLen = int(q.Window.Limit)
	}
	if len(items) > maxLen {
		return fmt.Errorf("items window overflow got=%d max=%d", len(items), maxLen)
	}
	return nil
}

func TestQueryExt_OrderedORNegativeResidualPlannerTrace_MatchesExpected(t *testing.T) {
	var (
		mu     sync.Mutex
		events []TraceEvent
	)

	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:  -1,
		TraceSink:        func(ev TraceEvent) { mu.Lock(); events = append(events, ev); mu.Unlock() },
		TraceSampleEvery: 1,
	})

	ids, stateA, _, q := queryExtOrderedORNegativeResidualFixture()
	if err := db.BatchSet(ids, stateA); err != nil {
		t.Fatalf("BatchSet(stateA): %v", err)
	}

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys(%+v): %v", q, err)
	}
	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(%+v): %v", q, err)
	}
	assertQueryIDsEqual(t, q, got, want)

	mu.Lock()
	if len(events) == 0 {
		mu.Unlock()
		t.Fatalf("expected trace event for ordered OR query")
	}
	ev := events[len(events)-1]
	mu.Unlock()

	if ev.Plan != string(PlanMaterialized) &&
		ev.Plan != string(PlanORMergeOrderMerge) &&
		ev.Plan != string(PlanORMergeOrderStream) {
		t.Fatalf("unexpected planner route for ordered OR fixture: got %q", ev.Plan)
	}
	if ev.RowsReturned != uint64(len(want)) {
		t.Fatalf("trace rows returned mismatch: got=%d want=%d", ev.RowsReturned, len(want))
	}

	assertQueryExtItemsMatchExpected(t, db, q)
	assertQueryExtCountMatchesBaseQuery(t, db, q)
	assertQueryExtPreparedMatchesExpected(t, db, q)
}

func TestQueryExt_OrderedORPrefixBoundaryChurn_MatchesSeqScanAndPrepared(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	rows := map[uint64]*Rec{
		1: {Name: "alpha-00", FullName: "grp/a/00", Active: true, Score: 10},
		2: {Name: "alpha-01", FullName: "grp/a/01", Active: false, Score: 40},
		3: {Name: "alpha-02", FullName: "grp/a/02", Active: true, Score: 60},
		4: {Name: "alpha-out", FullName: "grp/a0", Active: true, Score: 80},
		5: {Name: "alpha-zz", FullName: "grp/a/zz", Active: false, Score: 55},
		6: {Name: "beta-00", FullName: "grp/b/00", Active: true, Score: 15},
		7: {Name: "beta-05", FullName: "grp/b/05", Active: false, Score: 25},
		8: {Name: "beta-09", FullName: "grp/b/09", Active: true, Score: 65},
		9: {Name: "gamma-00", FullName: "grp/c/00", Active: true, Score: 75},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}

	q := qx.Query(
		qx.OR(
			qx.AND(
				qx.PREFIX("full_name", "grp/a/"),
				qx.GTE("full_name", "grp/a/01"),
			),
			qx.AND(
				qx.PREFIX("full_name", "grp/a/"),
				qx.GTE("score", 55.0),
			),
			qx.AND(
				qx.PREFIX("full_name", "grp/b/"),
				qx.EQ("active", true),
			),
		),
	).Sort("full_name", qx.ASC).Offset(1).Limit(6)

	check := func(step string) {
		t.Run(step, func(t *testing.T) {
			assertQueryExtAllReadPathsMatchExpected(t, db, q)
		})
	}

	check("initial")

	if err := db.Patch(4, []Field{{Name: "full_name", Value: "grp/a/10"}}); err != nil {
		t.Fatalf("Patch(4 full_name): %v", err)
	}
	if err := db.Patch(6, []Field{{Name: "active", Value: false}}); err != nil {
		t.Fatalf("Patch(6 active): %v", err)
	}
	check("after_order_boundary_cross")

	if err := db.Patch(5, []Field{{Name: "full_name", Value: "grp/b/01"}, {Name: "active", Value: true}}); err != nil {
		t.Fatalf("Patch(5 full_name/active): %v", err)
	}
	if err := db.Delete(2); err != nil {
		t.Fatalf("Delete(2): %v", err)
	}
	if err := db.Set(10, &Rec{Name: "alpha-new", FullName: "grp/a/00", Active: true, Score: 70}); err != nil {
		t.Fatalf("Set(10): %v", err)
	}
	check("after_overlap_churn")
}

func TestQueryExt_ConcurrentAtomicBatchSetSnapshotConsistency_OnOrderedORNegativeResidual_WithAnalyzer(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: 5 * time.Millisecond})

	ids, stateA, stateB, q := queryExtOrderedORNegativeResidualFixture()
	setState := func(vals []*Rec) error {
		return db.BatchSet(ids, vals)
	}

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
	countA, err := db.Count(q.Filter)
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
	countB, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count(stateB): %v", err)
	}

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA reset): %v", err)
	}

	startVersion := db.PlannerStats().Version
	if latest, ok := waitPlannerStatsVersionGreater(db, startVersion, 250*time.Millisecond); !ok {
		t.Fatalf("expected analyzer to publish planner stats during ordered OR test: start=%d latest=%d", startVersion, latest)
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
				if !slices.Equal(gotNames, namesA) && !slices.Equal(gotNames, namesB) {
					errCh <- fmt.Errorf("g=%d i=%d Query hybrid snapshot: got=%v wantA=%v wantB=%v", gid, i, gotNames, namesA, namesB)
					return
				}

				gotCount, err := db.Count(q.Filter)
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

func TestQueryExt_ConcurrentWriter_RootExecPreparedQuery_OnOrderedORNegativeResidual_ReturnsNoHybridResults(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: 5 * time.Millisecond})

	ids, stateA, stateB, q := queryExtOrderedORNegativeResidualFixture()
	setState := func(vals []*Rec) error {
		return db.BatchSet(ids, vals)
	}

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

	startVersion := db.PlannerStats().Version
	if latest, ok := waitPlannerStatsVersionGreater(db, startVersion, 250*time.Millisecond); !ok {
		t.Fatalf("expected analyzer to publish planner stats during prepared ordered OR test: start=%d latest=%d", startVersion, latest)
	}

	start := make(chan struct{})
	found := make(chan []uint64, 1)
	errCh := make(chan error, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 320; i++ {
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
			for i := 0; i < 480; i++ {
				got, err := execPreparedQueryExt(db, nq)
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
		t.Fatalf("root execPreparedQuery returned hybrid ordered OR result under concurrent writes: got=%v wantA=%v wantB=%v", got, wantA, wantB)
	}
}

func TestQueryExt_StringKeys_ConcurrentAtomicBatchSetSnapshotConsistency_OnOrderedOR(t *testing.T) {
	db, _ := openTempDBString(t, Options{AnalyzeInterval: -1})

	ids, stateA, stateB, q := queryExtStringOrderedORFixture()
	setState := func(vals []*Rec) error {
		return db.BatchSet(ids, vals)
	}

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
	countA, err := db.Count(q.Filter)
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
	countB, err := db.Count(q.Filter)
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
				if !slices.Equal(gotNames, namesA) && !slices.Equal(gotNames, namesB) {
					errCh <- fmt.Errorf("g=%d i=%d Query hybrid snapshot: got=%v wantA=%v wantB=%v", gid, i, gotNames, namesA, namesB)
					return
				}

				gotCount, err := db.Count(q.Filter)
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

func TestQueryExt_StringKeys_ConcurrentWriter_RootExecPreparedQuery_OnOrderedOR_ReturnsNoHybridResults(t *testing.T) {
	db, _ := openTempDBString(t, Options{AnalyzeInterval: -1})

	ids, stateA, stateB, q := queryExtStringOrderedORFixture()
	setState := func(vals []*Rec) error {
		return db.BatchSet(ids, vals)
	}

	nq := normalizeQueryForTest(q)
	if err := db.checkUsedQuery(nq); err != nil {
		t.Fatalf("checkUsedQuery(%+v): %v", nq, err)
	}

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA): %v", err)
	}
	wantA, err := expectedKeysString(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysString(stateA): %v", err)
	}

	if err := setState(stateB); err != nil {
		t.Fatalf("BatchSet(stateB): %v", err)
	}
	wantB, err := expectedKeysString(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysString(stateB): %v", err)
	}

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA reset): %v", err)
	}

	start := make(chan struct{})
	found := make(chan []string, 1)
	errCh := make(chan error, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 320; i++ {
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
			for i := 0; i < 480; i++ {
				got, err := execPreparedQueryExt(db, nq)
				if err != nil {
					select {
					case errCh <- fmt.Errorf("execPreparedQuery: %w", err):
					default:
					}
					return
				}
				if queryStringIDsEqual(q, got, wantA) || queryStringIDsEqual(q, got, wantB) {
					continue
				}
				select {
				case found <- append([]string(nil), got...):
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
		t.Fatalf("root execPreparedQuery returned hybrid string-key ordered OR result under concurrent writes: got=%v wantA=%v wantB=%v", got, wantA, wantB)
	}
}

func TestQueryExt_RefreshPlannerStatsDuringRuntimeFallbackEligibleOrderedOR_AllReadPathsStayExact(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	_ = seedData(t, db, 8_000)

	q := normalizeQueryForTest(
		qx.Query(
			qx.OR(
				qx.AND(
					qx.EQ("active", true),
					qx.EQ("country", "NL"),
					qx.GTE("score", 30.0),
				),
				qx.AND(
					qx.EQ("name", "alice"),
					qx.GTE("age", 25),
				),
				qx.PREFIX("full_name", "FN-1"),
			),
		).Sort("age", qx.ASC).Offset(140).Limit(120),
	)

	wantKeys, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64(%+v): %v", q, err)
	}
	wantItems, err := db.BatchGet(wantKeys...)
	if err != nil {
		t.Fatalf("BatchGet(wantKeys): %v", err)
	}
	baseQ := cloneQuery(q)
	baseQ.Order = nil
	clearQueryExtOrderWindow(baseQ)
	wantCount, err := db.Count(baseQ.Filter)
	if err != nil {
		t.Fatalf("Count(baseQ): %v", err)
	}
	nq := normalizeQueryForTest(q)
	if err := db.checkUsedQuery(nq); err != nil {
		t.Fatalf("checkUsedQuery(%+v): %v", nq, err)
	}

	errCh := make(chan error, 16)
	var wg sync.WaitGroup

	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				gotKeys, err := db.QueryKeys(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys: %w", gid, i, err)
					return
				}
				if !queryIDsEqual(q, gotKeys, wantKeys) {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys mismatch: got=%v want=%v", gid, i, gotKeys, wantKeys)
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

				gotCount, err := db.Count(q.Filter)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Count: %w", gid, i, err)
					return
				}
				if gotCount != wantCount {
					errCh <- fmt.Errorf("g=%d i=%d Count mismatch: got=%d want=%d", gid, i, gotCount, wantCount)
					return
				}

				gotPrepared, err := execPreparedQueryExt(db, nq)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d execPreparedQuery: %w", gid, i, err)
					return
				}
				if !queryIDsEqual(nq, gotPrepared, wantKeys) {
					errCh <- fmt.Errorf("g=%d i=%d execPreparedQuery mismatch: got=%v want=%v", gid, i, gotPrepared, wantKeys)
					return
				}
			}
		}(g)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 25; i++ {
			if err := db.RefreshPlannerStats(); err != nil {
				errCh <- fmt.Errorf("RefreshPlannerStats i=%d: %w", i, err)
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			s := db.PlannerStats()
			if s.Fields != nil {
				s.Fields["country"] = PlannerFieldStats{}
				delete(s.Fields, "age")
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func TestQueryExt_ConcurrentAtomicBatchSetSnapshotConsistency_OnNoOrderORWindow_WithAnalyzer(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: 5 * time.Millisecond})

	ids, stateA, stateB, q := queryExtNoOrderORDisjointFixture()
	setState := func(vals []*Rec) error {
		return db.BatchSet(ids, vals)
	}

	fullQ := cloneQuery(q)
	clearQueryExtOrderWindow(fullQ)

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA): %v", err)
	}
	fullA, err := expectedKeysUint64(t, db, fullQ)
	if err != nil {
		t.Fatalf("expectedKeysUint64(full stateA): %v", err)
	}
	itemsAFull, err := db.BatchGet(fullA...)
	if err != nil {
		t.Fatalf("BatchGet(fullA): %v", err)
	}
	sigsAFull := queryExtBuildSignatureCounts(itemsAFull)
	countA, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count(stateA): %v", err)
	}

	if err := setState(stateB); err != nil {
		t.Fatalf("BatchSet(stateB): %v", err)
	}
	fullB, err := expectedKeysUint64(t, db, fullQ)
	if err != nil {
		t.Fatalf("expectedKeysUint64(full stateB): %v", err)
	}
	itemsBFull, err := db.BatchGet(fullB...)
	if err != nil {
		t.Fatalf("BatchGet(fullB): %v", err)
	}
	sigsBFull := queryExtBuildSignatureCounts(itemsBFull)
	countB, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count(stateB): %v", err)
	}

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA reset): %v", err)
	}

	nq := normalizeQueryForTest(q)
	if err := db.checkUsedQuery(nq); err != nil {
		t.Fatalf("checkUsedQuery(%+v): %v", nq, err)
	}

	startVersion := db.PlannerStats().Version
	if latest, ok := waitPlannerStatsVersionGreater(db, startVersion, 250*time.Millisecond); !ok {
		t.Fatalf("expected analyzer to publish planner stats during no-order OR test: start=%d latest=%d", startVersion, latest)
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
				errKA := plannerExtValidateNoOrderWindow(q, gotKeys, fullA)
				errKB := plannerExtValidateNoOrderWindow(q, gotKeys, fullB)
				if errKA != nil && errKB != nil {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys hybrid snapshot: got=%v errA=%v errB=%v", gid, i, gotKeys, errKA, errKB)
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
				errIA := queryExtValidateNoOrderItemsAgainstFullSet(q, gotItems, sigsAFull, len(fullA))
				errIB := queryExtValidateNoOrderItemsAgainstFullSet(q, gotItems, sigsBFull, len(fullB))
				if errIA != nil && errIB != nil {
					errCh <- fmt.Errorf("g=%d i=%d Query hybrid snapshot: errA=%v errB=%v items=%v", gid, i, errIA, errIB, gotNames)
					return
				}

				gotCount, err := db.Count(q.Filter)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Count: %w", gid, i, err)
					return
				}
				if gotCount != countA && gotCount != countB {
					errCh <- fmt.Errorf("g=%d i=%d Count hybrid snapshot: got=%d wantA=%d wantB=%d", gid, i, gotCount, countA, countB)
					return
				}

				gotPrepared, err := execPreparedQueryExt(db, nq)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d execPreparedQuery: %w", gid, i, err)
					return
				}
				errPA := plannerExtValidateNoOrderWindow(q, gotPrepared, fullA)
				errPB := plannerExtValidateNoOrderWindow(q, gotPrepared, fullB)
				if errPA != nil && errPB != nil {
					errCh <- fmt.Errorf("g=%d i=%d execPreparedQuery hybrid snapshot: got=%v errA=%v errB=%v", gid, i, gotPrepared, errPA, errPB)
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

func TestQueryExt_RefreshPlannerStatsDuringAdversarialNoOrderOR_AllReadPathsStayValid(t *testing.T) {
	db := plannerExtOpenSeededDB(t, Options{AnalyzeInterval: -1})
	q := plannerExtQueryAdversarialNoOrderNegativeResidualOverlap()

	fullQ := cloneQuery(q)
	clearQueryExtOrderWindow(fullQ)
	full, err := expectedKeysUint64(t, db, fullQ)
	if err != nil {
		t.Fatalf("expectedKeysUint64(full %+v): %v", fullQ, err)
	}
	fullItems, err := db.BatchGet(full...)
	if err != nil {
		t.Fatalf("BatchGet(full): %v", err)
	}
	fullSigCounts := queryExtBuildSignatureCounts(fullItems)
	wantCount := uint64(len(full))

	nq := normalizeQueryForTest(q)
	if err := db.checkUsedQuery(nq); err != nil {
		t.Fatalf("checkUsedQuery(%+v): %v", nq, err)
	}

	errCh := make(chan error, 16)
	var wg sync.WaitGroup

	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				gotKeys, err := db.QueryKeys(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys: %w", gid, i, err)
					return
				}
				if err = plannerExtValidateNoOrderWindow(q, gotKeys, full); err != nil {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys invalid: %v got=%v full=%v", gid, i, err, gotKeys, full)
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
				if err = queryExtValidateNoOrderItemsAgainstFullSet(q, gotItems, fullSigCounts, len(full)); err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Query invalid: %v items=%v", gid, i, err, gotNames)
					return
				}

				gotCount, err := db.Count(q.Filter)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d Count: %w", gid, i, err)
					return
				}
				if gotCount != wantCount {
					errCh <- fmt.Errorf("g=%d i=%d Count mismatch: got=%d want=%d", gid, i, gotCount, wantCount)
					return
				}

				gotPrepared, err := execPreparedQueryExt(db, nq)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d execPreparedQuery: %w", gid, i, err)
					return
				}
				if err = plannerExtValidateNoOrderWindow(q, gotPrepared, full); err != nil {
					errCh <- fmt.Errorf("g=%d i=%d execPreparedQuery invalid: %v got=%v full=%v", gid, i, err, gotPrepared, full)
					return
				}
			}
		}(g)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if err := db.RefreshPlannerStats(); err != nil {
				errCh <- fmt.Errorf("RefreshPlannerStats i=%d: %w", i, err)
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 120; i++ {
			s := db.PlannerStats()
			if s.Fields != nil {
				s.Fields["country"] = PlannerFieldStats{}
				delete(s.Fields, "age")
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func TestQueryExt_Race_PlannerAnalyzeLoopVsSnapshotPublish_OnOrderedORFixture(t *testing.T) {
	if !testRaceEnabled {
		t.Skip("run with -race to detect planner analyzer vs snapshot publish race")
	}

	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: 2 * time.Millisecond})

	ids, stateA, stateB, q := queryExtOrderedORNegativeResidualFixture()
	setState := func(vals []*Rec) error {
		return db.BatchSet(ids, vals)
	}

	if err := setState(stateA); err != nil {
		t.Fatalf("BatchSet(stateA): %v", err)
	}
	startVersion := db.PlannerStats().Version
	if latest, ok := waitPlannerStatsVersionGreater(db, startVersion, 250*time.Millisecond); !ok {
		t.Fatalf("expected analyzer to start before race reproducer: start=%d latest=%d", startVersion, latest)
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

	errCh := make(chan error, 16)
	var wg sync.WaitGroup
	start := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 200; i++ {
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

	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			<-start
			for i := 0; i < 120; i++ {
				got, err := db.QueryKeys(q)
				if err != nil {
					errCh <- fmt.Errorf("g=%d i=%d QueryKeys: %w", gid, i, err)
					return
				}
				if !queryIDsEqual(q, got, wantA) && !queryIDsEqual(q, got, wantB) {
					errCh <- fmt.Errorf("g=%d i=%d hybrid ordered OR result: got=%v wantA=%v wantB=%v", gid, i, got, wantA, wantB)
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

func TestQueryExt_Race_RefreshPlannerStatsVsSnapshotPublish_OnOrderedORFixture(t *testing.T) {
	if !testRaceEnabled {
		t.Skip("run with -race to detect RefreshPlannerStats vs snapshot publish race")
	}

	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	ids, stateA, stateB, q := queryExtOrderedORNegativeResidualFixture()
	setState := func(vals []*Rec) error {
		return db.BatchSet(ids, vals)
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

	errCh := make(chan error, 32)
	var wg sync.WaitGroup
	start := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 240; i++ {
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

	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			<-start
			for i := 0; i < 160; i++ {
				if err := db.RefreshPlannerStats(); err != nil {
					errCh <- fmt.Errorf("g=%d i=%d RefreshPlannerStats: %w", gid, i, err)
					return
				}
			}
		}(g)
	}

	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			<-start
			for i := 0; i < 120; i++ {
				got, err := db.QueryKeys(q)
				if err != nil {
					errCh <- fmt.Errorf("query g=%d i=%d QueryKeys: %w", gid, i, err)
					return
				}
				if !queryIDsEqual(q, got, wantA) && !queryIDsEqual(q, got, wantB) {
					errCh <- fmt.Errorf("query g=%d i=%d hybrid ordered OR result: got=%v wantA=%v wantB=%v", gid, i, got, wantA, wantB)
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
			q:    qx.Query(qx.GT("opt", "aa")).Sort("opt", qx.ASC).Limit(8),
		},
		{
			name: "lte_active_desc",
			q:    qx.Query(qx.LTE("opt", "aa"), qx.EQ("active", true)).Sort("opt", qx.DESC).Limit(8),
		},
		{
			name: "gte_window",
			q:    qx.Query(qx.GTE("opt", "ab")).Sort("opt", qx.ASC).Offset(1).Limit(2),
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

func TestQueryExt_OrderBasicPointerNilContradictions_RemainEmpty(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	rows := map[uint64]*Rec{
		1: {Name: "nil-a", Opt: nil, Active: true},
		2: {Name: "empty", Opt: strPtr(""), Active: true},
		3: {Name: "aa-a", Opt: strPtr("aa"), Active: true},
		4: {Name: "ab-a", Opt: strPtr("ab"), Active: true},
		5: {Name: "ac-off", Opt: strPtr("ac"), Active: false},
		6: {Name: "ba-a", Opt: strPtr("ba"), Active: true},
		7: {Name: "nil-b", Opt: nil, Active: false},
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
			name: "eq_nil_and_gt",
			q:    qx.Query(qx.EQ("opt", nil), qx.GT("opt", "aa")).Sort("opt", qx.ASC).Limit(8),
		},
		{
			name: "eq_nil_and_eq_value",
			q:    qx.Query(qx.EQ("opt", nil), qx.EQ("opt", "aa")).Sort("opt", qx.ASC).Limit(8),
		},
		{
			name: "eq_nil_and_prefix",
			q:    qx.Query(qx.EQ("opt", nil), qx.PREFIX("opt", "a")).Sort("opt", qx.DESC).Offset(1).Limit(3),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertQueryExtAllReadPathsMatchExpected(t, db, tc.q)

			got, used, err := db.tryExecutionPlan(tc.q, nil)
			if err != nil {
				t.Fatalf("tryExecutionPlan(%+v): %v", tc.q, err)
			}
			if !used {
				t.Fatalf("expected ordered execution fast path to be applicable")
			}
			if len(got) != 0 {
				t.Fatalf("expected empty fast-path result, got=%v", got)
			}
		})
	}
}

func TestQueryExt_OrderBasicNilShortCircuit_PreservesResidualValidation(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	rows := map[uint64]*Rec{
		1: {Name: "nil-a", Opt: nil, Age: 10, Active: true},
		2: {Name: "empty", Opt: strPtr(""), Age: 20, Active: true},
		3: {Name: "aa-a", Opt: strPtr("aa"), Age: 30, Active: true},
		4: {Name: "ab-a", Opt: strPtr("ab"), Age: 40, Active: true},
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
			name: "pointer_conflict_invalid_residual",
			q: qx.Query(
				qx.EQ("opt", nil),
				qx.GT("opt", "aa"),
				qx.HASALL("name", []string{"x"}),
			).Sort("opt", qx.ASC).Limit(8),
		},
		{
			name: "non_pointer_nil_invalid_residual",
			q: qx.Query(
				qx.EQ("age", nil),
				qx.HASALL("name", []string{"x"}),
			).Sort("age", qx.ASC).Limit(8),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, used, err := db.tryExecutionPlan(tc.q, nil)
			if !used {
				t.Fatalf("expected tryExecutionPlan to take ordered fast path")
			}
			if !errors.Is(err, ErrInvalidQuery) {
				t.Fatalf("tryExecutionPlan(%+v) err=%v, want ErrInvalidQuery", tc.q, err)
			}

			_, err = db.QueryKeys(tc.q)
			if !errors.Is(err, ErrInvalidQuery) {
				t.Fatalf("QueryKeys(%+v) err=%v, want ErrInvalidQuery", tc.q, err)
			}
		})
	}
}

func TestQueryExt_OrderBasicNilShortCircuit_DoesNotMaterializeResiduals(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		SnapshotMaterializedPredCacheMaxEntries: 16,
	})

	rows := map[uint64]*Rec{
		1: {Name: "nil-a", Opt: nil, Active: true},
		2: {Name: "alpha", Opt: strPtr("aa"), Active: true},
		3: {Name: "bravo", Opt: strPtr("ab"), Active: true},
		4: {Name: "charlie", Opt: strPtr("ac"), Active: true},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}

	q := qx.Query(
		qx.EQ("opt", nil),
		qx.GT("opt", "aa"),
		qx.CONTAINS("name", "a"),
	).Sort("opt", qx.ASC).Limit(8)

	cacheKey := db.materializedPredCacheKey(qx.CONTAINS("name", "a"))
	if cacheKey == "" {
		t.Fatalf("expected non-empty materialized cache key")
	}
	if _, ok := db.getSnapshot().loadMaterializedPred(cacheKey); ok {
		t.Fatalf("unexpected residual cache entry before execution")
	}

	got, used, err := db.tryExecutionPlan(q, nil)
	if err != nil {
		t.Fatalf("tryExecutionPlan(%+v): %v", q, err)
	}
	if !used {
		t.Fatalf("expected ordered execution fast path to be applicable")
	}
	if len(got) != 0 {
		t.Fatalf("expected empty fast-path result, got=%v", got)
	}
	if _, ok := db.getSnapshot().loadMaterializedPred(cacheKey); ok {
		t.Fatalf("unexpected residual cache entry after empty nil-order short-circuit")
	}
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
			).Sort("name", qx.ASC).Limit(10),
		},
		{
			name: "prefix_singleton_upper_edge",
			q: qx.Query(
				qx.PREFIX("name", "aa/"),
				qx.LTE("name", "aa/"),
			).Sort("name", qx.ASC).Limit(10),
		},
		{
			name: "prefix_empty_after_crossing_upper",
			q: qx.Query(
				qx.PREFIX("name", "aa/"),
				qx.GTE("name", "aa0"),
			).Sort("name", qx.ASC).Limit(10),
		},
		{
			name: "bounded_prefix_desc_window",
			q: qx.Query(
				qx.PREFIX("name", "aa/"),
				qx.GT("name", "aa/"),
				qx.LTE("name", "aa/zz"),
			).Sort("name", qx.DESC).Offset(1).Limit(3),
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
		).Sort("age", qx.ASC).Offset(3_500).Limit(80),
		qx.Query(
			qx.LT("score", 15_001.0),
			qx.EQ("active", true),
		).Sort("age", qx.ASC).Offset(3_500).Limit(80),
		qx.Query(
			qx.GTE("score", 6_000.0),
			qx.LT("score", 17_000.0),
			qx.EQ("country", "US"),
		).Sort("age", qx.DESC).Offset(900).Limit(60),
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
		qx.Query(qx.LT("score", 4_000.0)).Sort("age", qx.ASC).Offset(2_000).Limit(40),
		qx.Query(qx.LT("score", 4_001.0)).Sort("age", qx.ASC).Offset(2_000).Limit(40),
		qx.Query(qx.LT("score", 3_999.0)).Sort("age", qx.ASC).Offset(2_000).Limit(40),
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
		clearQueryExtOrderWindow(countQ)
		count, err := db.Count(countQ.Filter)
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

				gotCount, err := db.Count(exp.q.Filter)
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
	).Sort("name", qx.DESC).Offset(1).Limit(3)

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
	countA, err := db.Count(q.Filter)
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
	countB, err := db.Count(q.Filter)
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

				gotCount, err := db.Count(q.Filter)
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
		).Sort("age", qx.ASC).Offset(120).Limit(80),
		qx.Query(
			qx.GTE("age", 2_501),
			qx.EQ("active", true),
		).Sort("age", qx.ASC).Offset(120).Limit(80),
		qx.Query(
			qx.GTE("age", 2_400),
			qx.LT("age", 2_650),
		).Sort("age", qx.ASC).Limit(120),
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
	).Sort("score", qx.DESC).Offset(10).Limit(120)

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
			).Sort("opt", qx.ASC).Offset(1).Limit(4),
		},
		{
			name: "prefix_not_active_desc",
			q: qx.Query(
				qx.PREFIX("opt", "a"),
				qx.NOT(qx.EQ("active", true)),
			).Sort("opt", qx.DESC).Limit(5),
		},
		{
			name: "prefix_not_name",
			q: qx.Query(
				qx.PREFIX("opt", "a"),
				qx.NOT(qx.EQ("name", "ax-off")),
			).Sort("opt", qx.ASC).Limit(6),
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
	).Sort("age", qx.ASC).Offset(4).Limit(18)

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
	countA, err := db.Count(q.Filter)
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
	countB, err := db.Count(q.Filter)
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

				gotCount, err := db.Count(q.Filter)
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
