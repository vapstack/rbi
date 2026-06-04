package rbi

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/keycodec"
	"go.etcd.io/bbolt"
)

type invalidUniqueSliceRec struct {
	Tags []string `rbi:"unique"`
}

func TestMultiWrap_DifferentStructs(t *testing.T) {
	rawDB, path := openRawBolt(t)

	defer func() {
		if err := rawDB.Close(); err != nil {
			t.Fatalf("raw bolt close: %v", err)
		}
	}()

	recDB, err := New[uint64, Rec](rawDB, testOptions(Options{}))
	if err != nil {
		t.Fatalf("Wrap 1 (Rec): %v", err)
	}

	productDB, err := New[string, Product](rawDB, testOptions(Options{}))
	if err != nil {
		t.Fatalf("Wrap 2 (Product): %v", err)
	}

	if err = recDB.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatal(err)
	}

	if err = productDB.Set("p1", &Product{SKU: "p1", Price: 100.0}); err != nil {
		t.Fatal(err)
	}

	ids, err := recDB.QueryKeys(qx.Query(qx.EQ("age", 30)))
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 {
		t.Errorf("expected 1 record, got: %v", len(ids))
	}

	sids, err := productDB.QueryKeys(qx.Query(qx.EQ("price", 100.0)))
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

func TestWrap_FailedPopulateFields_DoesNotLeakRegistry(t *testing.T) {
	rawDB, _ := openRawBolt(t)
	defer func() { _ = rawDB.Close() }()

	const bucket = "registry_cleanup_check"

	_, err := New[uint64, invalidUniqueSliceRec](rawDB, testOptions(Options{BucketName: bucket}))
	if err == nil {
		t.Fatalf("expected Wrap failure for invalidUniqueSliceRec")
	}

	db, err := New[uint64, Rec](rawDB, testOptions(Options{BucketName: bucket}))
	if err != nil {
		t.Fatalf("Wrap after failed Wrap must succeed, got: %v", err)
	}
	defer func() { _ = db.Close() }()
}

func TestWrap_BuildIndexError_DoesNotCloseCallerBolt(t *testing.T) {
	rawDB, _ := openRawBolt(t)
	defer func() { _ = rawDB.Close() }()

	const bucket = "broken_wrap_bucket"
	if err := rawDB.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		// Invalid msgpack payload to force buildIndex decode error in Wrap.
		var key [8]byte
		return b.Put(keycodec.U64BytesWithBuf(1, &key), []byte{0xc1})
	}); err != nil {
		t.Fatalf("seed invalid payload: %v", err)
	}

	_, err := New[uint64, Rec](rawDB, testOptions(Options{BucketName: bucket}))
	if err == nil {
		t.Fatalf("expected Wrap failure on malformed payload")
	}

	if err = rawDB.View(func(tx *bbolt.Tx) error {
		if tx.Bucket([]byte(bucket)) == nil {
			return fmt.Errorf("bucket is missing")
		}
		return nil
	}); err != nil {
		t.Fatalf("raw bbolt must stay open after Wrap error, got: %v", err)
	}
}

func TestWrap_InvalidBucketName_RejectsUnsafeCharacters(t *testing.T) {
	rawDB, _ := openRawBolt(t)
	defer func() { _ = rawDB.Close() }()

	for _, bucket := range []string{
		"bad-name",
		"bad.name",
		"bad name",
		"1bucket",
	} {
		_, err := New[uint64, Rec](rawDB, testOptions(Options{BucketName: bucket}))
		if !errors.Is(err, ErrInvalidBucketName) {
			t.Fatalf("bucket=%q: expected ErrInvalidBucketName, got=%v", bucket, err)
		}
	}

	db, err := New[uint64, Rec](rawDB, testOptions(Options{BucketName: "bucket_name_ok"}))
	if err != nil {
		t.Fatalf("expected valid bucket name to succeed, got: %v", err)
	}
	defer func() { _ = db.Close() }()
}

func TestMultiWrap_SameStruct_DifferentBuckets(t *testing.T) {
	rawDB, _ := openRawBolt(t)
	defer func() {
		if err := rawDB.Close(); err != nil {
			t.Fatalf("raw db close: %v", err)
		}
	}()

	dbUS, err := New[uint64, Rec](rawDB, testOptions(Options{BucketName: "users_us"}))
	if err != nil {
		t.Fatal(err)
	}

	dbEU, err := New[uint64, Rec](rawDB, testOptions(Options{BucketName: "users_eu"}))
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

	ids, err = dbEU.QueryKeys(qx.Query(qx.EQ("name", "hans")))
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

	db1, err := New[uint64, Rec](rawDB, testOptions(Options{BucketName: "b1"}))
	if err != nil {
		t.Fatal(err)
	}
	db2, err := New[uint64, Rec](rawDB, testOptions(Options{BucketName: "b2"}))
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

	c1, err := db1.Count()
	if err != nil {
		t.Fatal(err)
	}
	c2, err := db2.Count()
	if err != nil {
		t.Fatal(err)
	}

	if int(c1) != count {
		t.Errorf("db1 count mismatch: got %d, expected %d", c1, count)
	}
	if int(c2) != count {
		t.Errorf("db2 count mismatch: got %d, expected %d", c2, count)
	}

	idsLeak, err := db1.QueryKeys(qx.Query(qx.EQ("age", 20)))
	if err != nil {
		t.Fatal(err)
	}
	if len(idsLeak) > 0 {
		t.Fatalf("isolation failed: db1 contains %d records from db2 (Age=20)", len(idsLeak))
	}

	idsCorrect, err := db1.QueryKeys(qx.Query(qx.EQ("age", 10)))
	if err != nil {
		t.Fatal(err)
	}
	if len(idsCorrect) != count {
		t.Errorf("db1 integrity failure: found %d records with Age=10, expected %d", len(idsCorrect), count)
	}
}

func TestMultiWrap_ReopenPersistence(t *testing.T) {
	rawDB, path := openRawBolt(t)

	opts1 := testOptions(Options{BucketName: "t1"})
	opts2 := testOptions(Options{BucketName: "t2"})

	db1, _ := New[uint64, Rec](rawDB, opts1)
	db2, _ := New[uint64, Rec](rawDB, opts2)

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

	db1New, err := New[uint64, Rec](rawDB2, opts1)
	if err != nil {
		t.Fatal(err)
	}

	db2New, err := New[uint64, Rec](rawDB2, opts2)
	if err != nil {
		t.Fatal(err)
	}

	ids1, err := db1New.QueryKeys(qx.Query(qx.EQ("name", "one")))
	if err != nil {
		t.Fatal(err)
	}
	if len(ids1) != 1 {
		t.Error("t1 persistence failed")
	}

	ids2, err := db2New.QueryKeys(qx.Query(qx.EQ("name", "two")))
	if err != nil {
		t.Fatal(err)
	}
	if len(ids2) != 1 {
		t.Error("t2 persistence failed")
	}

	idsCross, err := db1New.QueryKeys(qx.Query(qx.EQ("name", "two")))
	if err != nil {
		t.Fatal(err)
	}
	if len(idsCross) != 0 {
		t.Error("cross contamination after reload")
	}
}

func TestMultiWrap_CloseBehavior(t *testing.T) {
	rawDB, _ := openRawBolt(t)
	// Wrap does not transfer ownership of rawDB to wrapped instances.

	db1, err := New[uint64, Rec](rawDB, testOptions(Options{BucketName: "b1"}))
	if err != nil {
		t.Fatalf("Wrap 1: %v", err)
	}

	db2, err := New[uint64, Rec](rawDB, testOptions(Options{BucketName: "b2"}))
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

func TestWrap_DoubleOpenCheck(t *testing.T) {
	rawDB, _ := openRawBolt(t)
	defer func() {
		if err := rawDB.Close(); err != nil {
			t.Fatalf("raw db close: %v", err)
		}
	}()

	db1, err := New[uint64, Rec](rawDB, testOptions(Options{BucketName: "users"}))
	if err != nil {
		t.Fatalf("first open failed: %v", err)
	}

	_, err = New[uint64, Rec](rawDB, testOptions(Options{BucketName: "users"}))
	if err == nil {
		t.Fatal("expected error on double open, got nil")
	}

	db2, err := New[uint64, Rec](rawDB, testOptions(Options{BucketName: "admins"}))
	if err != nil {
		t.Fatalf("different bucket open failed: %v", err)
	}

	if err = db1.Close(); err != nil {
		t.Fatalf("db1 close: %v", err)
	}

	db1Reopen, err := New[uint64, Rec](rawDB, testOptions(Options{BucketName: "users"}))
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

func TestTruncate(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1, Tags: []string{"t1"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2, Tags: []string{"t2"}}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	if cnt, err := db.Count(); err != nil || cnt != 2 {
		t.Fatalf("Count(before): cnt=%d err=%v", cnt, err)
	}
	if ids, err := db.QueryKeys(qx.Query(qx.EQ("email", "a@x"))); err != nil || len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("QueryKeys(before): ids=%v err=%v", ids, err)
	}

	// concurrent readers while truncate happens should not panic
	done := make(chan struct{})
	errCh := make(chan error, 16)

	var wg sync.WaitGroup
	reader := func(name string) {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				errCh <- fmt.Errorf("panic in %s: %v", name, r)
			}
		}()
		for {
			select {
			case <-done:
				return
			default:
			}
			_, _ = db.Get(1)
			_, _ = db.BatchGet(1, 2, 999)

			_ = db.SeqScan(0, func(_ uint64, _ *UniqueTestRec) (bool, error) { return true, nil })
			_ = db.SeqScanRaw(0, func(_ uint64, _ []byte) (bool, error) { return true, nil })

			_, _ = db.QueryKeys(qx.Query())
			_, _ = db.Count()
		}
	}

	wg.Add(2)
	go reader("r1")
	go reader("r2")

	if err := db.Truncate(); err != nil {
		close(done)
		wg.Wait()
		t.Fatalf("Truncate #1: %v", err)
	}
	if err := db.Truncate(); err != nil {
		close(done)
		wg.Wait()
		t.Fatalf("Truncate #2: %v", err)
	}

	close(done)
	wg.Wait()
	close(errCh)

	for e := range errCh {
		t.Errorf("%v", e)
	}
	if t.Failed() {
		t.FailNow()
	}

	if v, err := db.Get(1); err != nil {
		t.Fatalf("Get(after): %v", err)
	} else if v != nil {
		t.Fatalf("expected Get(1)==nil after truncate, got %#v", v)
	}
	if vals, err := db.BatchGet(1, 2); err != nil {
		t.Fatalf("BatchGet(after): %v", err)
	} else if len(vals) != 2 || vals[0] != nil || vals[1] != nil {
		t.Fatalf("expected BatchGet to return [nil nil], got %#v", vals)
	}

	if cnt, err := db.Count(); err != nil {
		t.Fatalf("Count(after): %v", err)
	} else if cnt != 0 {
		t.Fatalf("expected 0 records after truncate, got %d", cnt)
	}
	if ids, err := db.QueryKeys(qx.Query()); err != nil {
		t.Fatalf("QueryKeys(NOOP after): %v", err)
	} else if len(ids) != 0 {
		t.Fatalf("expected no keys after truncate, got %v", ids)
	}

	st := db.Stats()
	if st.KeyCount != 0 {
		t.Fatalf("expected Stats.KeyCount=0 after truncate, got %d", st.KeyCount)
	}
	var zero uint64
	if st.LastKey != zero {
		t.Fatalf("expected Stats.LastKey=%v after truncate, got %v", zero, st.LastKey)
	}

	if err := db.Set(10, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("expected unique values reusable after truncate, got: %v", err)
	}
	if err := db.Set(11, &UniqueTestRec{Email: "a@x", Code: 2}); err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation after truncate+reuse, got: %v", err)
	}

	if ids, err := db.QueryKeys(qx.Query(qx.EQ("email", "a@x"))); err != nil {
		t.Fatalf("QueryKeys(after reinsert): %v", err)
	} else if len(ids) != 1 || ids[0] != 10 {
		t.Fatalf("expected [10] after reinsert, got %v", ids)
	}
}

func TestTruncate_NoDeadlock_WithConcurrentWriteExecutor(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "truncate_deadlock.db")
	db, raw := openBoltAndNew[uint64, Rec](t, path)

	if err := db.Set(1, &Rec{Name: "seed", Age: 1}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	// Force lock-order contention window:
	// 1) truncate goroutine waits on db.mu
	// 2) write executor goroutine acquires writer tx and then waits on db.mu
	// 3) release db.mu and assert both operations complete.
	db.publishMu.Lock()
	truncateDone := make(chan error, 1)
	go func() {
		truncateDone <- db.Truncate()
	}()
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- db.Set(1, &Rec{Name: "after", Age: 2}, NoBatch[uint64, Rec])
	}()
	time.Sleep(20 * time.Millisecond)
	db.publishMu.Unlock()

	select {
	case err := <-truncateDone:
		if err != nil {
			t.Fatalf("Truncate: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("truncate timed out (possible deadlock)")
	}

	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("write executor error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("write executor timed out (possible deadlock)")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("raw.Close: %v", err)
	}
}

func TestAPI_New_InvalidBucketFillPercent_NegativeRejected(t *testing.T) {
	raw, _ := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	db, err := New[uint64, Rec](raw, testOptions(Options{
		BucketName:        "api_invalid_fill_negative",
		BucketFillPercent: -0.25,
	}))
	if err == nil {
		_ = db.Close()
		t.Fatalf("expected negative BucketFillPercent to be rejected")
	}
}

func TestAPI_New_InvalidBucketFillPercent_AboveOneRejected(t *testing.T) {
	raw, _ := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	db, err := New[uint64, Rec](raw, testOptions(Options{
		BucketName:        "api_invalid_fill_above_one",
		BucketFillPercent: 1.25,
	}))
	if err == nil {
		_ = db.Close()
		t.Fatalf("expected BucketFillPercent > 1 to be rejected")
	}
}

func TestAPI_BucketName_ReturnsClone(t *testing.T) {
	raw, _ := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	db, err := New[uint64, Rec](raw, testOptions(Options{BucketName: "api_bucket_clone"}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = db.Close() }()

	orig := db.BucketName()
	mutated := db.BucketName()
	mutated[0] ^= 0xff

	again := db.BucketName()
	if !bytes.Equal(orig, again) {
		t.Fatalf("bucket name was mutated through returned slice: before=%q after=%q", orig, again)
	}
}

func TestAPI_QueryKeysQueryCountReturnErrClosedAfterClose(t *testing.T) {
	db, _ := openTempDBUint64(t)
	mustSetAPIRec(t, db, 1, &Rec{Name: "alice", Age: 30})

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	q := qx.Query(qx.EQ("age", 30))

	if _, err := db.QueryKeys(q); !errors.Is(err, ErrClosed) {
		t.Fatalf("QueryKeys after Close: expected ErrClosed, got %v", err)
	}
	if _, err := db.Query(q); !errors.Is(err, ErrClosed) {
		t.Fatalf("Query after Close: expected ErrClosed, got %v", err)
	}
	if _, err := db.Count(q.Filter); !errors.Is(err, ErrClosed) {
		t.Fatalf("Count after Close: expected ErrClosed, got %v", err)
	}
}
