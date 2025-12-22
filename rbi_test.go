package rbi

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/vapstack/qx"
	"go.etcd.io/bbolt"
)

type Product struct {
	SKU   string   `db:"sku"`
	Price float64  `db:"price"`
	Tags  []string `db:"tags"`
}

func openRawBolt(t *testing.T) (*bbolt.DB, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "shared.db")
	db, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}
	return db, path
}

func TestMultiWrap_DifferentStructs(t *testing.T) {
	rawDB, path := openRawBolt(t)

	defer func() {
		if err := rawDB.Close(); err != nil {
			t.Fatalf("raw bolt close: %v", err)
		}
	}()

	recDB, err := Wrap[uint64, Rec](rawDB, nil)
	if err != nil {
		t.Fatalf("Wrap 1 (Rec): %v", err)
	}

	productDB, err := Wrap[string, Product](rawDB, nil)
	if err != nil {
		t.Fatalf("Wrap 2 (Product): %v", err)
	}

	if err = recDB.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatal(err)
	}

	if err = productDB.Set("p1", &Product{SKU: "p1", Price: 100.0}); err != nil {
		t.Fatal(err)
	}

	ids, err := recDB.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "age", Value: 30}})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 {
		t.Errorf("expected 1 record, got: %v", len(ids))
	}

	sids, err := productDB.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "price", Value: 100.0}})
	if err != nil {
		t.Fatal(err)
	}
	if len(sids) != 1 {
		t.Errorf("expected 1 record, got: %v", len(sids))
	}

	if err = recDB.Close(); err != nil {
		t.Fatal(err)
	}
	if err = productDB.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err = os.Stat(path + ".Rec.rbi"); os.IsNotExist(err) {
		t.Error("Rec.rbi index file missing")
	}
	if _, err = os.Stat(path + ".Product.rbi"); os.IsNotExist(err) {
		t.Error("Product.rbi index file missing")
	}
}

func TestMultiWrap_SameStruct_DifferentBuckets(t *testing.T) {
	rawDB, _ := openRawBolt(t)
	defer func() {
		if err := rawDB.Close(); err != nil {
			t.Fatalf("raw db close: %v", err)
		}
	}()

	dbUS, err := Wrap[uint64, Rec](rawDB, &Options[uint64, Rec]{BucketName: "users_us"})
	if err != nil {
		t.Fatal(err)
	}

	dbEU, err := Wrap[uint64, Rec](rawDB, &Options[uint64, Rec]{BucketName: "users_eu"})
	if err != nil {
		t.Fatal(err)
	}

	if err = dbUS.Set(1, &Rec{Name: "john", Meta: Meta{Country: "US"}}); err != nil {
		t.Fatal(err)
	}

	// same id, another bucket
	if err = dbEU.Set(1, &Rec{Name: "hans", Meta: Meta{Country: "DE"}}); err != nil {
		t.Fatal(err)
	}

	ids, err := dbUS.QueryKeys(qx.Query(qx.EQ("name", "john")))
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 item, got: %v", len(ids))
	}

	// must not contain
	ids, err = dbUS.QueryKeys(qx.Query(qx.EQ("name", "hans")))
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 0 {
		t.Fatal("bucket leaked")
	}

	ids, err = dbEU.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "name", Value: "hans"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 item, got: %v", len(ids))
	}
}

func TestMultiWrap_ConcurrentWrites(t *testing.T) {
	rawDB, _ := openRawBolt(t)
	defer func() {
		if err := rawDB.Close(); err != nil {
			t.Logf("raw db close error: %v", err)
		}
	}()

	db1, err := Wrap[uint64, Rec](rawDB, &Options[uint64, Rec]{BucketName: "b1"})
	if err != nil {
		t.Fatal(err)
	}
	db2, err := Wrap[uint64, Rec](rawDB, &Options[uint64, Rec]{BucketName: "b2"})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	count := 1000

	errCh := make(chan error, count*2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				errCh <- fmt.Errorf("panic in writer 1: %v", r)
			}
		}()
		for i := 0; i < count; i++ {
			if err := db1.Set(uint64(i), &Rec{Name: "w1", Age: 10}); err != nil {
				errCh <- fmt.Errorf("db1 set error at %d: %w", i, err)
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				errCh <- fmt.Errorf("panic in writer 2: %v", r)
			}
		}()

		for i := 0; i < count; i++ {
			if err := db2.Set(uint64(i), &Rec{Name: "w2", Age: 20}); err != nil {
				errCh <- fmt.Errorf("db2 set error at %d: %w", i, err)
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)

	for e := range errCh {
		t.Errorf("concurrent write failed: %v", e)
	}
	if t.Failed() {
		t.FailNow()
	}

	c1, err := db1.Count(nil)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := db2.Count(nil)
	if err != nil {
		t.Fatal(err)
	}

	if int(c1) != count {
		t.Errorf("db1 count mismatch: got %d, expected %d", c1, count)
	}
	if int(c2) != count {
		t.Errorf("db2 count mismatch: got %d, expected %d", c2, count)
	}

	idsLeak, err := db1.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "age", Value: 20}})
	if err != nil {
		t.Fatal(err)
	}
	if len(idsLeak) > 0 {
		t.Fatalf("isolation failed: db1 contains %d records from db2 (Age=20)", len(idsLeak))
	}

	idsCorrect, err := db1.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "age", Value: 10}})
	if err != nil {
		t.Fatal(err)
	}
	if len(idsCorrect) != count {
		t.Errorf("db1 integrity failure: found %d records with Age=10, expected %d", len(idsCorrect), count)
	}
}

func TestMultiWrap_ReopenPersistence(t *testing.T) {
	rawDB, path := openRawBolt(t)

	opts1 := &Options[uint64, Rec]{BucketName: "t1"}
	opts2 := &Options[uint64, Rec]{BucketName: "t2"}

	db1, _ := Wrap(rawDB, opts1)
	db2, _ := Wrap(rawDB, opts2)

	if err := db1.Set(1, &Rec{Name: "one"}); err != nil {
		t.Fatal(err)
	}
	if err := db2.Set(2, &Rec{Name: "two"}); err != nil {
		t.Fatal(err)
	}
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatal(err)
	}

	rawDB2, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rawDB2.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	db1New, err := Wrap(rawDB2, opts1)
	if err != nil {
		t.Fatal(err)
	}

	db2New, err := Wrap(rawDB2, opts2)
	if err != nil {
		t.Fatal(err)
	}

	ids1, err := db1New.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "name", Value: "one"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids1) != 1 {
		t.Error("t1 persistence failed")
	}

	ids2, err := db2New.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "name", Value: "two"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids2) != 1 {
		t.Error("t2 persistence failed")
	}

	idsCross, err := db1New.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "name", Value: "two"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(idsCross) != 0 {
		t.Error("cross contamination after reload")
	}
}

func TestMultiWrap_CloseBehavior(t *testing.T) {
	rawDB, _ := openRawBolt(t)
	// AutoClose: false

	db1, err := Wrap[uint64, Rec](rawDB, &Options[uint64, Rec]{BucketName: "b1"})
	if err != nil {
		t.Fatalf("Wrap 1: %v", err)
	}

	db2, err := Wrap[uint64, Rec](rawDB, &Options[uint64, Rec]{BucketName: "b2"})
	if err != nil {
		t.Fatalf("Wrap 2: %v", err)
	}

	if err = db1.Set(1, &Rec{}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err = db2.Set(1, &Rec{}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err = db1.Close(); err != nil {
		t.Fatalf("db1 close: %v", err)
	}

	// db2 must work
	if err = db2.Set(2, &Rec{}); err != nil {
		t.Fatalf("db2 failed after db1 closed: %v", err)
	}

	// db1 must not
	if err = db1.Set(3, &Rec{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("db1 should be closed, got: %v", err)
	}

	if err = db2.Close(); err != nil {
		t.Fatalf("db2 close: %v", err)
	}
	if err = rawDB.Close(); err != nil {
		t.Fatalf("bolt close: %v", err)
	}
}

func TestRebuildIndex_CleanState(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(2, &Rec{Name: "bob", Age: 30}); err != nil {
		t.Fatal(err)
	}

	db.DisableIndexing()
	if err := db.Set(3, &Rec{Name: "charlie", Age: 30}); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(2); err != nil {
		t.Fatal(err)
	}
	db.EnableIndexing()

	// verify broken state (index: 1, 2; data: 1, 3)
	// query uses index, so it should find 1 and 2 (ghost), and miss 3.
	cnt, err := db.Count(&qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "age", Value: 30}})
	if err != nil {
		t.Fatal(err)
	}

	// expecting index to be stale, indexer returns what's in bitmap:
	// 1, 2 (2 is deleted but index not updated); 3 is missing
	if cnt != 2 {
		t.Logf("State before rebuild: expected 2 (stale), got %d. (Implementation specific)", cnt)
	}

	if err = db.RebuildIndex(); err != nil {
		t.Fatal(err)
	}

	// verify state (1, 3)
	ids, err := db.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "age", Value: 30}})
	if err != nil {
		t.Fatal(err)
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 3 {
		t.Fatalf("after rebuild: expected [1, 3], got %v", ids)
	}
}

func TestLargeBatch_AtomicFailure(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &UniqueTestRec{Email: "exists@x", Code: 1}); err != nil {
		t.Fatal(err)
	}

	ids := []uint64{2, 3}
	vals := []*UniqueTestRec{
		{Email: "new@x", Code: 2},
		{Email: "exists@x", Code: 3}, // must fail
	}

	err := db.SetMany(ids, vals)
	if err == nil {
		t.Fatal("Expected error in SetMany")
	}

	// verify atomicity: id 2 should not exist
	v, err := db.Get(2)
	if err != nil {
		t.Fatal(err)
	}
	if v != nil {
		t.Fatal("SetMany was not atomic: ID 2 was inserted despite batch failure")
	}
}

/**/

type UniqueTestRec struct {
	Email string   `db:"email" rbi:"unique"`
	Code  int      `db:"code"  rbi:"unique"`
	Opt   *string  `db:"opt"   rbi:"unique"`
	Tags  []string `db:"tags"`
}

func openTempDBUint64Unique(t *testing.T, opts *Options[uint64, UniqueTestRec]) (*DB[uint64, UniqueTestRec], string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_unique.db")
	db, err := Open[uint64, UniqueTestRec](path, 0o600, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, path
}

func TestUnique_Set_DuplicateRejected(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	err := db.Set(2, &UniqueTestRec{Email: "a@x", Code: 2})
	if err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}
}

func TestUnique_Set_SameRecordUpdateAllowed(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}
}

func TestUnique_Patch_ConflictingUpdateRejected(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	err := db.PatchStrict(2, []Field{{Name: "email", Value: "a@x"}})
	if err == nil {
		t.Fatalf("expected unique violation")
	}
	if !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}
}

func TestUnique_NilAllowedMultipleTimes(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1, Opt: nil}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2, Opt: nil}); err != nil {
		t.Fatalf("Set: %v", err)
	}
}

func TestUnique_OptDuplicateRejected(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	v := "same"

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1, Opt: &v}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2, Opt: &v})
	if err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}
}

func TestUnique_SetMany_DuplicateWithinBatchRejected(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	ids := []uint64{1, 2}
	vals := []*UniqueTestRec{
		{Email: "a@x", Code: 1},
		{Email: "a@x", Code: 2},
	}

	err := db.SetMany(ids, vals)
	if err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}
}

func TestUnique_SetMany_DuplicateAgainstDBRejected(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	ids := []uint64{2, 3}
	vals := []*UniqueTestRec{
		{Email: "b@x", Code: 2},
		{Email: "a@x", Code: 3},
	}

	err := db.SetMany(ids, vals)
	if err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}
}

func TestUnique_SetMany_SwapValuesAllowed(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &UniqueTestRec{Email: "x@x", Code: 10}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "y@x", Code: 20}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	ids := []uint64{1, 2}
	vals := []*UniqueTestRec{
		{Email: "y@x", Code: 10},
		{Email: "x@x", Code: 20},
	}

	if err := db.SetMany(ids, vals); err != nil {
		t.Fatalf("SetMany swap should be allowed, got: %v", err)
	}
}

func TestUnique_DeleteThenReuseAllowed(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Delete(1); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "a@x", Code: 2}); err != nil {
		t.Fatalf("Set(2) after delete should be allowed: %v", err)
	}
}

func TestUnique_Smoke_HasUniqueWorks(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	err := db.Set(2, &UniqueTestRec{Email: "a@x", Code: 2})
	if err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected unique violation, got: %v", err)
	}
}

func TestUnique_PatchMany_DuplicateWithinBatchRejected(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	ids := []uint64{1, 2}
	err := db.PatchManyStrict(ids, []Field{{Name: "email", Value: "dup@x"}})
	if err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}
}

func TestUnique_PatchMany_DuplicateAgainstDBRejected(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(3, &UniqueTestRec{Email: "c@x", Code: 3}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	ids := []uint64{2, 3}
	err := db.PatchManyStrict(ids, []Field{{Name: "code", Value: 1}})
	if err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}
}

func TestUnique_PatchMany_SwapValuesAllowed(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)

	if err := db.Set(1, &UniqueTestRec{Email: "x@x", Code: 10}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "y@x", Code: 20}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.PatchStrict(1, []Field{{Name: "email", Value: "tmp@x"}}); err != nil {
		t.Fatalf("PatchStrict(1)->tmp: %v", err)
	}
	if err := db.PatchStrict(2, []Field{{Name: "email", Value: "x@x"}}); err != nil {
		t.Fatalf("PatchStrict(2)->x: %v", err)
	}
	if err := db.PatchStrict(1, []Field{{Name: "email", Value: "y@x"}}); err != nil {
		t.Fatalf("PatchStrict(1)->y: %v", err)
	}
	v1, err := db.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	v2, err := db.Get(2)
	if err != nil {
		t.Fatal(err)
	}
	if v1.Email != "y@x" || v2.Email != "x@x" {
		t.Fatalf("swap result mismatch: v1=%q v2=%q", v1.Email, v2.Email)
	}
}

func TestUnique_OptNilReleasesAndReuseAllowed(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, nil)
	s := "same"

	err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1, Opt: &s})
	if err != nil {
		t.Fatalf("Set: %v", err)
	}

	err = db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2, Opt: &s})
	if err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected unique violation on Opt duplicate, got: %v", err)
	}
	if err = db.PatchStrict(1, []Field{{Name: "opt", Value: nil}}); err != nil {
		t.Fatalf("PatchStrict(1) opt=nil: %v", err)
	}
	if err = db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2, Opt: &s}); err != nil {
		t.Fatalf("Set(2) after releasing opt should be allowed: %v", err)
	}
}

func TestStringKeys_ExoticCharacters(t *testing.T) {
	db, _ := openTempDBString(t, nil)

	keys := []string{
		"simple",
		"With Space",
		"Кириллица",
		"🔥emoji🔥",
		"user/123/profile",
		"\x00\x01\x02",
	}

	for i, k := range keys {
		rec := &Rec{Name: fmt.Sprintf("rec-%d", i), Age: i}
		if err := db.Set(k, rec); err != nil {
			t.Fatalf("Set(%q) failed: %v", k, err)
		}
	}

	for i, k := range keys {
		v, err := db.Get(k)
		if err != nil {
			t.Fatalf("Get(%q) failed: %v", k, err)
		}
		if v == nil {
			t.Fatalf("Get(%q) returned nil", k)
		}

		expectedName := fmt.Sprintf("rec-%d", i)
		if v.Name != expectedName {
			t.Errorf("Key %q: expected name %q, got %q", k, expectedName, v.Name)
		}

		// validate index lookup via string mapper
		q := &qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "name", Value: expectedName}}
		ids, err := db.QueryKeys(q)
		if err != nil {
			t.Fatalf("QueryKeys for %q failed: %v", k, err)
		}
		if len(ids) != 1 {
			t.Fatalf("QueryKeys for %q returned %d results, expected 1", k, len(ids))
		}
		if ids[0] != k {
			t.Errorf("QueryKeys mismatch: expected key %q, got %q", k, ids[0])
		}
	}
}

func TestStringKeys_Persistence_MappingStability(t *testing.T) {
	db, path := openTempDBString(t, nil)

	if err := db.Set("alice", &Rec{Age: 20}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set("bob", &Rec{Age: 30}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// force index and string map persistence
	if err := db.Close(); err != nil {
		t.Fatalf("db close: %v", err)
	}

	// reopen
	db2, err := Open[string, Rec](path, 0o600, nil)
	if err != nil {
		t.Fatalf("db open: %v", err)
	}
	defer func() {
		if err := db2.Close(); err != nil {
			t.Fatalf("db2 close: %v", err)
		}
	}()

	// validate correctness after reopen
	q := &qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "age", Value: 20}}
	ids, err := db2.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}

	if len(ids) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(ids))
	}
	if ids[0] != "alice" {
		t.Fatalf("Expected key 'alice', got %q (string-ID mapping corrupted)", ids[0])
	}

	// validate ID counter restoration
	if err = db2.Set("charlie", &Rec{Age: 40}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	ids, _ = db2.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "age", Value: 40}})
	if len(ids) != 1 || ids[0] != "charlie" {
		t.Errorf("Insert after reopen failed: %v", ids)
	}
}

func TestStringKeys_RebuildIndex_FromScratch(t *testing.T) {
	db, path := openTempDBString(t, nil)

	keys := []string{"k1", "k2", "k3"}
	for i, k := range keys {
		if err := db.Set(k, &Rec{Age: i * 10}); err != nil {
			t.Fatalf("Set (%q): %v", k, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db close: %v", err)
	}

	// remove file to force rebuild
	rbiFile := path + ".Rec.rbi"
	if err := os.Remove(rbiFile); err != nil && !os.IsNotExist(err) {
		t.Logf("Index file removal failed: %v", err)
	}

	// reopen and rebuild
	db2, err := Open[string, Rec](path, 0o600, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := db2.Close(); err != nil {
			t.Fatalf("db2 close: %v", err)
		}
	}()

	// validate rebuilt index
	for i, k := range keys {
		age := i * 10
		ids, err := db2.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "age", Value: age}})
		if err != nil {
			t.Fatalf("QueryKeys: %v", err)
		}
		if len(ids) != 1 || ids[0] != k {
			t.Errorf("rebuild mismatch for key %q: %v", k, ids)
		}
	}
}

func TestStringKeys_Concurrency_MapperStress(t *testing.T) {
	db, _ := openTempDBString(t, nil)

	var wg sync.WaitGroup
	workers := 10
	writes := 100

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < writes; j++ {
				key := fmt.Sprintf("w-%d-k-%d", workerID, j)
				rec := &Rec{Name: "worker", Age: workerID}

				if err := db.Set(key, rec); err != nil {
					t.Errorf("Set failed: %v", err)
					return
				}

				// immediate read to stress RLock paths
				v, err := db.Get(key)
				if err != nil || v == nil {
					t.Errorf("Get failed: %v", err)
				}
			}
		}(i)
	}
	wg.Wait()

	// validate total count
	totalExpected := workers * writes
	count, _ := db.Count(nil)
	if int(count) != totalExpected {
		t.Errorf("expected %d records, got %d", totalExpected, count)
	}

	// validate index query under load
	ids, _ := db.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "name", Value: "worker"}})
	if len(ids) != totalExpected {
		t.Errorf("expected %d keys, got %d", totalExpected, len(ids))
	}
}

func TestStringKeys_SortByStringKey(t *testing.T) {
	db, _ := openTempDBString(t, nil)

	inputs := []string{"b", "a", "d", "c"}
	for _, k := range inputs {
		err := db.Set(k, &Rec{Age: 1})
		if err != nil {
			t.Fatalf("Set (%q): %v", k, err)
		}
	}

	// returns results ordered by internal id (insertion order)
	ids, err := db.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "age", Value: 1}})
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}

	expectedOrder := []string{"b", "a", "d", "c"}
	if len(ids) != 4 {
		t.Fatalf("result count mismatch: want: %v, got: %v", len(expectedOrder), len(ids))
	}

	for i := range ids {
		if ids[i] != expectedOrder[i] {
			t.Errorf("order mismatch at %d: got %q, want %q", i, ids[i], expectedOrder[i])
		}
	}
}

func TestStringKeys_VeryLongKey(t *testing.T) {
	db, _ := openTempDBString(t, nil)

	longKey := strings.Repeat("A", 4096)

	err := db.Set(longKey, &Rec{Name: "long"})
	if err != nil {
		t.Fatalf("Set: %v", err)
	}

	v, err := db.Get(longKey)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if v == nil {
		t.Fatalf("get failed: %v", v)
	}
	if v.Name != "long" {
		t.Errorf("value mismatch: %v", v.Name)
	}

	ids, err := db.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "name", Value: "long"}})
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) != 1 || ids[0] != longKey {
		t.Errorf("index lookup failed")
	}
}

func TestStringKeys_ResultSafety_UnsafeCheck(t *testing.T) {
	db, _ := openTempDBString(t, nil)
	key := "my-key"
	if err := db.Set(key, &Rec{}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	ids, err := db.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpNOOP}})
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	resultKey := ids[0]

	// close to invalidate mmap regions
	if err = db.Close(); err != nil {
		t.Fatalf("db close: %v", err)
	}

	// force string access
	s := strings.Clone(resultKey)
	if s != key {
		t.Errorf("key corrupted after DB close: %q", s)
	}
}

func TestWrap_DoubleOpenCheck(t *testing.T) {
	rawDB, _ := openRawBolt(t)
	defer func() {
		if err := rawDB.Close(); err != nil {
			t.Fatalf("raw db close: %v", err)
		}
	}()

	db1, err := Wrap[uint64, Rec](rawDB, &Options[uint64, Rec]{BucketName: "users"})
	if err != nil {
		t.Fatalf("first open failed: %v", err)
	}

	_, err = Wrap[uint64, Rec](rawDB, &Options[uint64, Rec]{BucketName: "users"})
	if err == nil {
		t.Fatal("expected error on double open, got nil")
	}

	db2, err := Wrap[uint64, Rec](rawDB, &Options[uint64, Rec]{BucketName: "admins"})
	if err != nil {
		t.Fatalf("different bucket open failed: %v", err)
	}

	if err = db1.Close(); err != nil {
		t.Fatalf("db1 close: %v", err)
	}

	db1Reopen, err := Wrap[uint64, Rec](rawDB, &Options[uint64, Rec]{BucketName: "users"})
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	if err = db1Reopen.Close(); err != nil {
		t.Fatal(err)
	}
	if err = db2.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDisableIndexing_RebuildIndex(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	if err := db.Set(1, &Rec{Name: "alice", Age: 10, Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	db.DisableIndexing()
	if err := db.Set(2, &Rec{Name: "bob", Age: 20, Tags: []string{"java"}}); err != nil {
		t.Fatalf("Set (noindex): %v", err)
	}

	_, err := db.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpNOOP}})
	if !errors.Is(err, ErrIndexDisabled) {
		t.Fatalf("expected ErrIndexDisabled, got: %v", err)
	}

	if err = db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}
	db.EnableIndexing()

	ids, err := db.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpNOOP}})
	if err != nil {
		t.Fatalf("QueryKeys after rebuild: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 ids, got %d (%v)", len(ids), ids)
	}
}

func TestPatchStrict_StructTags_NumConversion(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	if err := db.Set(1, &Rec{
		Name:     "alice",
		Age:      10,
		Tags:     []string{"go"},
		FullName: "Alice A.",
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.PatchStrict(1, []Field{{Name: "age", Value: 42.0}}); err != nil {
		t.Fatalf("PatchStrict age float->int: %v", err)
	}
	v, err := db.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if v.Age != 42 {
		t.Fatalf("age not patched: got %d", v.Age)
	}

	if err = db.PatchStrict(1, []Field{{Name: "fullName", Value: "Alice Alpha"}}); err != nil {
		t.Fatalf("PatchStrict json tag: %v", err)
	}
	v, err = db.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if v.FullName != "Alice Alpha" {
		t.Fatalf("full name not patched: got %q", v.FullName)
	}

	err = db.PatchStrict(1, []Field{{Name: "age", Value: 1.25}})
	if err == nil {
		t.Fatalf("expected error on float->int with fraction")
	}
	v, err = db.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if v.Age != 42 {
		t.Fatalf("age changed despite failed patch: got %d", v.Age)
	}
}

func TestPatchStrict_NilRules(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	s := "opt"
	if err := db.Set(1, &Rec{
		Name: "alice", Age: 10, Opt: &s,
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.PatchStrict(1, []Field{{Name: "opt", Value: nil}}); err != nil {
		t.Fatalf("PatchStrict opt=nil: %v", err)
	}
	v, err := db.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if v.Opt != nil {
		t.Fatalf("expected opt=nil after patch")
	}

	err = db.PatchStrict(1, []Field{{Name: "age", Value: nil}})
	if err == nil {
		t.Fatalf("expected error for age=nil")
	}
}

func TestMakePatch_PreCommit_DeepCopy_SliceValues(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	rec := &Rec{
		Name: "alice",
		Age:  10,
		Tags: []string{"a"},
	}
	if err := db.Set(1, rec); err != nil {
		t.Fatalf("Set: %v", err)
	}

	patch := make([]Field, 0, 8)
	makePatch := db.CollectPatch(&patch)

	origTags := []string{"x", "y"} // will be mutated later to validate deep copy
	updated := &Rec{
		Name: "bob", // changed
		Age:  10,    // unchanged
		Tags: origTags,
	}

	if err := db.Set(1, updated, makePatch); err != nil {
		t.Fatalf("Set(update): %v", err)
	}

	got := make(map[string]any, len(patch))
	for _, f := range patch {
		got[f.Name] = f.Value
	}

	// expect changed fields: name and tags

	if _, ok := got["name"]; !ok {
		t.Fatalf("expected patch to include %q, got %#v", "name", patch)
	}
	if _, ok := got["tags"]; !ok {
		t.Fatalf("expected patch to include %q, got %#v", "tags", patch)
	}
	if _, ok := got["age"]; ok {
		t.Fatalf("did not expect patch to include unchanged field %q, got %#v", "age", patch)
	}

	if v, _ := got["name"].(string); v != "bob" {
		t.Fatalf("expected patched name %q, got %#v", "bob", got["name"])
	}

	gotTags, _ := got["tags"].([]string)
	if len(gotTags) != 2 || gotTags[0] != "x" || gotTags[1] != "y" {
		t.Fatalf("expected patched tags [x y], got %#v", gotTags)
	}

	origTags[0] = "MUTATED"

	gotTags2, _ := got["tags"].([]string)
	if len(gotTags2) != 2 || gotTags2[0] != "x" || gotTags2[1] != "y" {
		t.Fatalf("expected deep-copied tags [x y], got %#v", gotTags2)
	}
}

func TestCollectPatchMany_PreCommit_CollectsAndDeepCopies_SliceValues(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	ids := []uint64{1, 2}
	base := []*Rec{
		{Name: "alice", Age: 10, Tags: []string{"a"}},
		{Name: "carol", Age: 20, Tags: []string{"c"}},
	}
	if err := db.SetMany(ids, base); err != nil {
		t.Fatalf("SetMany(base): %v", err)
	}

	patchByID := make(map[uint64][]Field)
	makePatchMany := db.CollectPatchMany(patchByID)

	origTags1 := []string{"x", "y"} // will mutate later
	origTags2 := []string{"p", "q"} // will mutate later

	updated := []*Rec{
		{Name: "bob", Age: 10, Tags: origTags1},   // name+tags changed, age unchanged
		{Name: "carol", Age: 21, Tags: origTags2}, // age+tags changed, name unchanged
	}

	if err := db.SetMany(ids, updated, makePatchMany); err != nil {
		t.Fatalf("SetMany(update): %v", err)
	}

	// to map []Field -> map[name]value for assertions.
	toMap := func(fs []Field) map[string]any {
		m := make(map[string]any, len(fs))
		for _, f := range fs {
			m[f.Name] = f.Value
		}
		return m
	}

	p1, ok := patchByID[1]
	if !ok {
		t.Fatalf("expected patch for id=1, got keys: %#v", keysOfMap(patchByID))
	}
	m1 := toMap(p1)

	if _, ok = m1["name"]; !ok {
		t.Fatalf("id=1: expected patch to include %q, got %#v", "name", p1)
	}
	if _, ok = m1["tags"]; !ok {
		t.Fatalf("id=1: expected patch to include %q, got %#v", "tags", p1)
	}
	if _, ok = m1["age"]; ok {
		t.Fatalf("id=1: did not expect patch to include unchanged field %q, got %#v", "age", p1)
	}

	if v, _ := m1["name"].(string); v != "bob" {
		t.Fatalf("id=1: expected patched name %q, got %#v", "bob", m1["name"])
	}
	tags1, _ := m1["tags"].([]string)
	if len(tags1) != 2 || tags1[0] != "x" || tags1[1] != "y" {
		t.Fatalf("id=1: expected patched tags [x y], got %#v", tags1)
	}

	p2, ok := patchByID[2]
	if !ok {
		t.Fatalf("expected patch for id=2, got keys: %#v", keysOfMap(patchByID))
	}
	m2 := toMap(p2)

	if _, ok = m2["age"]; !ok {
		t.Fatalf("id=2: expected patch to include %q, got %#v", "age", p2)
	}
	if _, ok = m2["tags"]; !ok {
		t.Fatalf("id=2: expected patch to include %q, got %#v", "tags", p2)
	}
	if _, ok = m2["name"]; ok {
		t.Fatalf("id=2: did not expect patch to include unchanged field %q, got %#v", "name", p2)
	}

	if v, _ := m2["age"].(int); v != 21 {
		t.Fatalf("id=2: expected patched age %d, got %#v", 21, m2["age"])
	}
	tags2, _ := m2["tags"].([]string)
	if len(tags2) != 2 || tags2[0] != "p" || tags2[1] != "q" {
		t.Fatalf("id=2: expected patched tags [p q], got %#v", tags2)
	}

	origTags1[0] = "MUTATED1"
	origTags2[0] = "MUTATED2"

	tags1b, _ := m1["tags"].([]string)
	if len(tags1b) != 2 || tags1b[0] != "x" || tags1b[1] != "y" {
		t.Fatalf("id=1: expected deep-copied tags [x y], got %#v", tags1b)
	}
	tags2b, _ := m2["tags"].([]string)
	if len(tags2b) != 2 || tags2b[0] != "p" || tags2b[1] != "q" {
		t.Fatalf("id=2: expected deep-copied tags [p q], got %#v", tags2b)
	}
}

func keysOfMap[V any](m map[uint64]V) []uint64 {
	out := make([]uint64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func TestIndexPersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persist.db")

	db, err := Open[uint64, Rec](path, 0o600, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err = db.Set(1, &Rec{Name: "alice", Age: 10, Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err = db.Set(2, &Rec{Name: "bob", Age: 20, Tags: []string{"java"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err = db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open[uint64, Rec](path, 0o600, nil)
	if err != nil {
		t.Fatalf("Open(reopen): %v", err)
	}
	t.Cleanup(func() {
		if err := db2.Close(); err != nil {
			t.Fatal(err)
		}
	})

	ids, err := db2.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "name", Value: "alice"}})
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("expected [1], got %v", ids)
	}

	st := db2.Stats()
	if st.KeyCount != 2 {
		t.Fatalf("expected Stats.KeyCount=2, got %d", st.KeyCount)
	}
}
