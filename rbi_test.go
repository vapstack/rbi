package rbi

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"go.etcd.io/bbolt"
)

type Product struct {
	SKU   string   `db:"sku"`
	Price float64  `db:"price"`
	Tags  []string `db:"tags"`
}

type invalidUniqueSliceRec struct {
	Tags []string `rbi:"unique"`
}

type schemaSubsetRec struct {
	Name string `db:"name"`
	Age  int    `db:"age"`
}

func openTempDBStringProduct(t *testing.T, options ...Options) (*DB[string, Product], string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_product.db")
	db, raw := openBoltAndNew[string, Product](t, path, options...)
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	return db, path
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

func readPersistedIndexSequence(tb testing.TB, path string) uint64 {
	tb.Helper()

	f, err := os.Open(path)
	if err != nil {
		tb.Fatalf("open persisted index: %v", err)
	}
	defer func() { _ = f.Close() }()

	reader := bufio.NewReader(f)
	if _, err = reader.ReadByte(); err != nil {
		tb.Fatalf("read persisted index format byte: %v", err)
	}
	seq, err := binary.ReadUvarint(reader)
	if err != nil {
		tb.Fatalf("read persisted index sequence: %v", err)
	}
	return seq
}

func readPersistedIndexFormatByte(tb testing.TB, path string) byte {
	tb.Helper()

	f, err := os.Open(path)
	if err != nil {
		tb.Fatalf("open persisted index: %v", err)
	}
	defer func() { _ = f.Close() }()

	reader := bufio.NewReader(f)
	ver, err := reader.ReadByte()
	if err != nil {
		tb.Fatalf("read persisted index format byte: %v", err)
	}
	return ver
}

func readBucketSequence(tb testing.TB, raw *bbolt.DB, bucket []byte) uint64 {
	tb.Helper()

	var seq uint64
	if err := raw.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		seq = b.Sequence()
		return nil
	}); err != nil {
		tb.Fatalf("read bucket sequence: %v", err)
	}
	return seq
}

func assertNoFutureSnapshotRefs[K ~uint64 | ~string, V any](tb testing.TB, db *DB[K, V]) {
	tb.Helper()

	current := db.getSnapshot()
	currentSeq := uint64(0)
	if current != nil {
		currentSeq = current.seq
	}

	db.snapshot.mu.RLock()
	defer db.snapshot.mu.RUnlock()
	for seq, ref := range db.snapshot.bySeq {
		if ref == nil || ref.snap == nil {
			tb.Fatalf("snapshot registry contains nil entry for seq=%d", seq)
		}
		if seq > currentSeq {
			tb.Fatalf("staged snapshot leaked into registry: seq=%d current=%d", seq, currentSeq)
		}
	}
}

func TestMultiWrap_DifferentStructs(t *testing.T) {
	rawDB, path := openRawBolt(t)

	defer func() {
		if err := rawDB.Close(); err != nil {
			t.Fatalf("raw bolt close: %v", err)
		}
	}()

	recDB, err := New[uint64, Rec](rawDB, Options{})
	if err != nil {
		t.Fatalf("Wrap 1 (Rec): %v", err)
	}

	productDB, err := New[string, Product](rawDB, Options{})
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

func TestWrap_FailedPopulateFields_DoesNotLeakRegistry(t *testing.T) {
	rawDB, _ := openRawBolt(t)
	defer func() { _ = rawDB.Close() }()

	const bucket = "registry_cleanup_check"

	_, err := New[uint64, invalidUniqueSliceRec](rawDB, Options{BucketName: bucket})
	if err == nil {
		t.Fatalf("expected Wrap failure for invalidUniqueSliceRec")
	}

	db, err := New[uint64, Rec](rawDB, Options{BucketName: bucket})
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
		return b.Put(uint64Bytes(1), []byte{0xc1})
	}); err != nil {
		t.Fatalf("seed invalid payload: %v", err)
	}

	_, err := New[uint64, Rec](rawDB, Options{BucketName: bucket})
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
		_, err := New[uint64, Rec](rawDB, Options{BucketName: bucket})
		if !errors.Is(err, ErrInvalidBucketName) {
			t.Fatalf("bucket=%q: expected ErrInvalidBucketName, got=%v", bucket, err)
		}
	}

	db, err := New[uint64, Rec](rawDB, Options{BucketName: "bucket_name_ok"})
	if err != nil {
		t.Fatalf("expected valid bucket name to succeed, got: %v", err)
	}
	defer func() { _ = db.Close() }()
}

func TestWrap_CorruptedPersistedIndex_RebuildsInsteadOfPanicking(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corrupted_index.db")

	rawDB, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}

	db, err := New[uint64, Rec](rawDB, Options{})
	if err != nil {
		t.Fatalf("initial New: %v", err)
	}
	rbiPath := db.rbiFile

	if err = db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	if err = db.Close(); err != nil {
		t.Fatalf("initial Close: %v", err)
	}
	seq := readBucketSequence(t, rawDB, db.bucket)
	if err = rawDB.Close(); err != nil {
		t.Fatalf("initial raw Close: %v", err)
	}

	var enc [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(enc[:], seq)
	corrupted := append([]byte{readPersistedIndexFormatByte(t, rbiPath)}, enc[:n]...)
	corrupted = append(corrupted, 0xff, 0xff, 0xff, 0xff)
	if err = os.WriteFile(rbiPath, corrupted, 0o600); err != nil {
		t.Fatalf("corrupt .rbi: %v", err)
	}

	rawDB2, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("reopen bbolt.Open: %v", err)
	}
	defer func() { _ = rawDB2.Close() }()

	var logBuf bytes.Buffer
	prevLogWriter := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(prevLogWriter)

	var db2 *DB[uint64, Rec]
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("reopen New panicked on corrupted persisted index: %v", r)
			}
		}()
		db2, err = New[uint64, Rec](rawDB2, Options{})
	}()
	if err != nil {
		t.Fatalf("reopen New: %v", err)
	}
	defer func() { _ = db2.Close() }()
	gotLog := logBuf.String()
	if !strings.Contains(gotLog, "persisted index unavailable") {
		t.Fatalf("expected corrupted persisted index reason in log, got: %q", gotLog)
	}
	if !strings.Contains(gotLog, "persisted index is invalid") {
		t.Fatalf("expected corrupted persisted index invalid marker in log, got: %q", gotLog)
	}
	if !strings.Contains(gotLog, "rbi: rebuilding index from bbolt") {
		t.Fatalf("expected rebuild start in log, got: %q", gotLog)
	}

	got, err := db2.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil || got.Name != "alice" || got.Age != 30 {
		t.Fatalf("expected rebuilt record after corrupted persisted index, got %#v", got)
	}
}

func TestWrap_PersistedIndexSchemaNarrowing_ReusesCompatibleIndexes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "schema_mismatch.db")

	const bucket = "schema_mismatch"

	rawDB, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}

	db, err := New[uint64, Rec](rawDB, Options{
		AnalyzeInterval: -1,
		BucketName:      bucket,
	})
	if err != nil {
		t.Fatalf("initial New: %v", err)
	}
	if err := db.Set(1, &Rec{Name: "alice", Age: 30, Email: "alice@example.test"}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("initial Close: %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("initial raw Close: %v", err)
	}

	rawDB2, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("reopen bbolt.Open: %v", err)
	}
	defer func() { _ = rawDB2.Close() }()

	var logBuf bytes.Buffer
	prevLogWriter := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(prevLogWriter)

	db2, err := New[uint64, schemaSubsetRec](rawDB2, Options{
		AnalyzeInterval: -1,
		BucketName:      bucket,
	})
	if err != nil {
		t.Fatalf("reopen New: %v", err)
	}
	defer func() { _ = db2.Close() }()

	gotLog := logBuf.String()
	if strings.Contains(gotLog, "persisted index unavailable") {
		t.Fatalf("expected compatible field indexes to be reused, got log: %q", gotLog)
	}
	if strings.Contains(gotLog, "rbi: rebuilding index from bbolt") {
		t.Fatalf("expected schema narrowing to avoid full rebuild, got log: %q", gotLog)
	}

	got, err := db2.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil || got.Name != "alice" || got.Age != 30 {
		t.Fatalf("expected record after partial persisted load, got %#v", got)
	}

	ids, err := db2.QueryKeys(qx.Query(qx.EQ("age", 30)))
	if err != nil {
		t.Fatalf("QueryKeys(age=30): %v", err)
	}
	if !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("unexpected rebuilt query result: got=%v want=[1]", ids)
	}
}

func TestMultiWrap_SameStruct_DifferentBuckets(t *testing.T) {
	rawDB, _ := openRawBolt(t)
	defer func() {
		if err := rawDB.Close(); err != nil {
			t.Fatalf("raw db close: %v", err)
		}
	}()

	dbUS, err := New[uint64, Rec](rawDB, Options{BucketName: "users_us"})
	if err != nil {
		t.Fatal(err)
	}

	dbEU, err := New[uint64, Rec](rawDB, Options{BucketName: "users_eu"})
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

	db1, err := New[uint64, Rec](rawDB, Options{BucketName: "b1"})
	if err != nil {
		t.Fatal(err)
	}
	db2, err := New[uint64, Rec](rawDB, Options{BucketName: "b2"})
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

	opts1 := Options{BucketName: "t1"}
	opts2 := Options{BucketName: "t2"}

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
	// Wrap does not transfer ownership of rawDB to wrapped instances.

	db1, err := New[uint64, Rec](rawDB, Options{BucketName: "b1"})
	if err != nil {
		t.Fatalf("Wrap 1: %v", err)
	}

	db2, err := New[uint64, Rec](rawDB, Options{BucketName: "b2"})
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

func TestClose_UnblocksQueuedBatchWriters(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   10 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 1024,
	})

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	closeDone := make(chan error, 1)

	go func() {
		firstDone <- db.Set(1, &Rec{Name: "first", Age: 10}, BeforeCommit(func(_ *bbolt.Tx, _ uint64, _, _ *Rec) error {
			close(firstStarted)
			<-releaseFirst
			return nil
		}))
	}()

	select {
	case <-firstStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("first batch callback did not start in time")
	}

	go func() {
		secondDone <- db.Set(2, &Rec{Name: "second", Age: 20})
	}()

	go func() {
		closeDone <- db.Close()
	}()

	deadline := time.Now().Add(2 * time.Second)
	for !db.closed.Load() && time.Now().Before(deadline) {
		time.Sleep(1 * time.Millisecond)
	}
	if !db.closed.Load() {
		close(releaseFirst)
		<-firstDone
		<-secondDone
		<-closeDone
		t.Fatal("db.closed was not set by Close in time")
	}

	close(releaseFirst)

	awaitErr := func(name string, ch <-chan error) error {
		t.Helper()
		select {
		case err := <-ch:
			return err
		case <-time.After(2 * time.Second):
			t.Fatalf("%s timed out", name)
			return nil
		}
	}

	if err := awaitErr("first Set", firstDone); err != nil && !errors.Is(err, ErrClosed) {
		t.Fatalf("first Set expected nil or ErrClosed, got: %v", err)
	}
	if err := awaitErr("second Set", secondDone); !errors.Is(err, ErrClosed) {
		t.Fatalf("second Set expected ErrClosed, got: %v", err)
	}
	if err := awaitErr("Close", closeDone); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestBatchSet_CommitFail_DoesNotGrowStrMap(t *testing.T) {
	db, _ := openTempDBStringProduct(t)

	if err := db.Set("p1", &Product{SKU: "p1", Price: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	initial := len(db.strmap.Keys)

	injected := errors.New("inject commit fail")
	var failOnce atomic.Bool
	failOnce.Store(true)
	db.testHooks.beforeCommit = func(op string) error {
		if op == "batch" && failOnce.CompareAndSwap(true, false) {
			return injected
		}
		return nil
	}
	defer func() {
		db.testHooks.beforeCommit = nil
	}()

	err := db.Set("ghost-commit", &Product{SKU: "ghost-commit", Price: 11})
	if !errors.Is(err, injected) {
		t.Fatalf("expected injected commit error, got: %v", err)
	}
	if v, err := db.Get("ghost-commit"); err != nil {
		t.Fatalf("Get(ghost-commit): %v", err)
	} else if v != nil {
		t.Fatalf("ghost-commit should not persist after commit fail, got %#v", v)
	}
	if after := len(db.strmap.Keys); after != initial {
		t.Fatalf("strmap grew after batch commit failure: initial=%d after=%d", initial, after)
	}

	bs := db.AutoBatchStats()
	if bs.TxCommitErrors == 0 {
		t.Fatalf("expected tx commit error in batch stats, got %+v", bs)
	}
}

type UniqueTestRec struct {
	Email string   `db:"email" rbi:"unique"`
	Code  int      `db:"code"  rbi:"unique"`
	Opt   *string  `db:"opt"   rbi:"unique"`
	Tags  []string `db:"tags"`
}

func openTempDBUint64Unique(t *testing.T, options ...Options) (*DB[uint64, UniqueTestRec], string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_unique.db")
	db, raw := openBoltAndNew[uint64, UniqueTestRec](t, path, options...)
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	return db, path
}

func openBenchDBUint64Unique(b *testing.B, n int) *DB[uint64, UniqueTestRec] {
	b.Helper()
	dir := b.TempDir()
	path := filepath.Join(dir, "bench_unique.db")

	db, raw := openBoltAndNew[uint64, UniqueTestRec](b, path)
	db.DisableSync()
	b.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	ids := make([]uint64, 0, 1000)
	vals := make([]*UniqueTestRec, 0, 1000)
	flush := func() {
		if len(ids) == 0 {
			return
		}
		if err := db.BatchSet(ids, vals); err != nil {
			b.Fatalf("BatchSet: %v", err)
		}
		ids = ids[:0]
		vals = vals[:0]
	}

	for i := 1; i <= n; i++ {
		ids = append(ids, uint64(i))
		vals = append(vals, &UniqueTestRec{
			Email: fmt.Sprintf("u%06d@example.com", i),
			Code:  i,
		})
		if len(ids) == cap(ids) {
			flush()
		}
	}
	flush()
	return db
}

func Benchmark_Query_UniqueEQ(b *testing.B) {
	db := openBenchDBUint64Unique(b, 20_000)
	q := qx.Query(qx.EQ("email", "u010000@example.com"))

	b.ReportAllocs()
	if _, err := db.QueryKeys(q); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for b.Loop() {
		ids, err := db.QueryKeys(q)
		if err != nil {
			b.Fatal(err)
		}
		if len(ids) != 1 {
			b.Fatalf("expected 1 id, got %d", len(ids))
		}
	}
}

func TestStringKeys_ExoticCharacters(t *testing.T) {
	db, _ := openTempDBString(t)

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
	dir := t.TempDir()
	path := filepath.Join(dir, "string_keys_persist.db")

	db, raw := openBoltAndNew[string, Rec](t, path)

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
	if err := raw.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}

	// reopen
	db2, raw2 := openBoltAndNew[string, Rec](t, path)
	defer func() {
		if err := db2.Close(); err != nil {
			t.Fatalf("db2 close: %v", err)
		}
		if err := raw2.Close(); err != nil {
			t.Fatalf("raw2 close: %v", err)
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
	dir := t.TempDir()
	path := filepath.Join(dir, "string_keys_rebuild.db")

	db, raw := openBoltAndNew[string, Rec](t, path)

	keys := []string{"k1", "k2", "k3"}
	for i, k := range keys {
		if err := db.Set(k, &Rec{Age: i * 10}); err != nil {
			t.Fatalf("Set (%q): %v", k, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db close: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}

	// remove file to force rebuild
	rbiFile := path + ".Rec.rbi"
	if err := os.Remove(rbiFile); err != nil && !os.IsNotExist(err) {
		t.Logf("Index file removal failed: %v", err)
	}

	// reopen and rebuild
	db2, raw2 := openBoltAndNew[string, Rec](t, path)
	defer func() {
		if err := db2.Close(); err != nil {
			t.Fatalf("db2 close: %v", err)
		}
		if err := raw2.Close(); err != nil {
			t.Fatalf("raw2 close: %v", err)
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
	db, _ := openTempDBString(t)

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

func TestStringKeys_BatchMutations_ModelReplayConsistency(t *testing.T) {
	db, _ := openTempDBString(t)

	type modelRec struct {
		name   string
		age    int
		active bool
	}

	const idSpace = 96

	makeKeys := func(base, size int) []string {
		out := make([]string, 0, size)
		used := make(map[string]struct{}, size)
		for step := 0; len(out) < size; step++ {
			idx := (base + step*11 + step*step + size*7) % idSpace
			key := fmt.Sprintf("k-%03d", idx)
			if _, ok := used[key]; ok {
				continue
			}
			used[key] = struct{}{}
			out = append(out, key)
		}
		return out
	}

	model := make(map[string]modelRec, idSpace)
	allNames := make(map[string]struct{}, 1024)

	for i := 0; i < 220; i++ {
		switch i % 3 {
		case 0: // BatchSet
			keys := makeKeys(i*13+17, 2+(i%4))
			vals := make([]*Rec, 0, len(keys))
			for pos, key := range keys {
				name := fmt.Sprintf("u-%s-v%03d-p%02d", key, i, pos)
				age := 20 + i + pos
				active := (i+pos)%2 == 0
				allNames[name] = struct{}{}
				vals = append(vals, &Rec{
					Name:   name,
					Age:    age,
					Active: active,
					Tags:   []string{fmt.Sprintf("g-%d", i%5)},
					Meta:   Meta{Country: "NL"},
				})
				model[key] = modelRec{name: name, age: age, active: active}
			}
			if err := db.BatchSet(keys, vals); err != nil {
				t.Fatalf("BatchSet(step=%d): %v", i, err)
			}

		case 1: // BatchPatch
			keys := makeKeys(i*7+31, 3+(i%3))
			age := 900 + i
			active := i%2 == 0
			patch := []Field{
				{Name: "age", Value: age},
				{Name: "active", Value: active},
			}
			if err := db.BatchPatch(keys, patch); err != nil {
				t.Fatalf("BatchPatch(step=%d): %v", i, err)
			}
			for _, key := range keys {
				if cur, ok := model[key]; ok {
					cur.age = age
					cur.active = active
					model[key] = cur
				}
			}

		default: // BatchDelete
			keys := makeKeys(i*5+19, 1+(i%4))
			if err := db.BatchDelete(keys); err != nil {
				t.Fatalf("BatchDelete(step=%d): %v", i, err)
			}
			for _, key := range keys {
				delete(model, key)
			}
		}
	}

	count, err := db.Count(nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if int(count) != len(model) {
		t.Fatalf("count mismatch: got=%d want=%d", count, len(model))
	}

	liveNames := make(map[string]string, len(model))
	wantActive := make(map[string]struct{}, len(model))

	for key, exp := range model {
		v, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if v == nil {
			t.Fatalf("Get(%q): nil value", key)
		}
		if v.Name != exp.name || v.Age != exp.age || v.Active != exp.active {
			t.Fatalf("value mismatch for %q: got={name:%q age:%d active:%v} want={name:%q age:%d active:%v}",
				key, v.Name, v.Age, v.Active, exp.name, exp.age, exp.active)
		}

		ids, err := db.QueryKeys(qx.Query(qx.EQ("name", exp.name)))
		if err != nil {
			t.Fatalf("QueryKeys(name=%q): %v", exp.name, err)
		}
		if len(ids) != 1 || ids[0] != key {
			t.Fatalf("name index mismatch for %q: got=%v want=[%q]", exp.name, ids, key)
		}

		liveNames[exp.name] = key
		if exp.active {
			wantActive[key] = struct{}{}
		}
	}

	gotActive, err := db.QueryKeys(qx.Query(qx.EQ("active", true)))
	if err != nil {
		t.Fatalf("QueryKeys(active=true): %v", err)
	}
	gotActiveSet := make(map[string]struct{}, len(gotActive))
	for _, key := range gotActive {
		gotActiveSet[key] = struct{}{}
	}
	if len(gotActiveSet) != len(wantActive) {
		t.Fatalf("active set size mismatch: got=%d want=%d", len(gotActiveSet), len(wantActive))
	}
	for key := range wantActive {
		if _, ok := gotActiveSet[key]; !ok {
			t.Fatalf("active set missing key %q", key)
		}
	}

	const staleProbe = 320
	staleChecked := 0
	for name := range allNames {
		if _, live := liveNames[name]; live {
			continue
		}
		ids, err := db.QueryKeys(qx.Query(qx.EQ("name", name)))
		if err != nil {
			t.Fatalf("QueryKeys(stale name=%q): %v", name, err)
		}
		if len(ids) != 0 {
			t.Fatalf("stale name %q still indexed: ids=%v", name, ids)
		}
		staleChecked++
		if staleChecked >= staleProbe {
			break
		}
	}
}

func TestStringKeys_ConcurrentMixedOps_FinalIndexConsistency(t *testing.T) {
	db, _ := openTempDBString(t)

	const (
		keySpace     = 144
		writers      = 4
		readers      = 3
		opsPerWriter = 220
		staleProbe   = 320
	)

	historicNames := sync.Map{}
	for i := 0; i < keySpace; i++ {
		key := fmt.Sprintf("id-%03d", i)
		name := fmt.Sprintf("seed-%03d", i)
		if err := db.Set(key, &Rec{
			Name:   name,
			Age:    18 + (i % 40),
			Active: i%2 == 0,
			Tags:   []string{"seed"},
			Meta:   Meta{Country: "NL"},
		}); err != nil {
			t.Fatalf("seed Set(%q): %v", key, err)
		}
		historicNames.Store(name, struct{}{})
	}

	errCh := make(chan error, writers+readers+16)
	stopReaders := make(chan struct{})

	readQueries := []*qx.QX{
		qx.Query(qx.EQ("active", true)),
		qx.Query(qx.PREFIX("name", "live-")),
		qx.Query(qx.GTE("age", 0)),
		qx.Query(qx.HASANY("tags", []string{"seed", "w0", "w1", "w2", "w3"})),
	}

	var readersWG sync.WaitGroup
	for r := 0; r < readers; r++ {
		readersWG.Add(1)
		go func(readerID int) {
			defer readersWG.Done()
			i := 0
			for {
				select {
				case <-stopReaders:
					return
				default:
				}

				q := readQueries[(readerID+i)%len(readQueries)]
				i++

				if _, err := db.QueryKeys(q); err != nil {
					errCh <- fmt.Errorf("reader=%d QueryKeys: %w", readerID, err)
					return
				}
				if _, err := db.Query(q); err != nil {
					errCh <- fmt.Errorf("reader=%d Query: %w", readerID, err)
					return
				}
				if _, err := db.Count(q); err != nil {
					errCh <- fmt.Errorf("reader=%d Count: %w", readerID, err)
					return
				}
			}
		}(r)
	}

	var writersWG sync.WaitGroup
	for w := 0; w < writers; w++ {
		w := w
		writersWG.Add(1)
		go func() {
			defer writersWG.Done()
			for i := 0; i < opsPerWriter; i++ {
				key := fmt.Sprintf("id-%03d", (w*1009+i*37+i*i)%keySpace)
				switch (w + i) % 3 {
				case 0:
					name := fmt.Sprintf("live-w%02d-i%04d-%s", w, i, key)
					historicNames.Store(name, struct{}{})
					if err := db.Set(key, &Rec{
						Name:   name,
						Age:    30 + w + i,
						Active: (w+i)%2 == 0,
						Tags: []string{
							fmt.Sprintf("w%d", w),
							fmt.Sprintf("grp-%d", i%7),
						},
						Meta: Meta{Country: "NL"},
					}); err != nil {
						errCh <- fmt.Errorf("writer=%d Set(%q): %w", w, key, err)
						return
					}

				case 1:
					patch := []Field{
						{Name: "age", Value: 700 + w*10 + i},
						{Name: "active", Value: i%2 == 0},
					}
					if err := db.BatchPatch([]string{key}, patch); err != nil {
						errCh <- fmt.Errorf("writer=%d BatchPatch(%q): %w", w, key, err)
						return
					}

				default:
					if err := db.Delete(key); err != nil {
						errCh <- fmt.Errorf("writer=%d Delete(%q): %w", w, key, err)
						return
					}
				}
			}
		}()
	}

	writersWG.Wait()
	close(stopReaders)
	readersWG.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("%v", err)
	}
	if t.Failed() {
		t.FailNow()
	}

	type liveRec struct {
		key  string
		name string
	}
	live := make(map[string]liveRec, keySpace)

	err := db.SeqScan("", func(key string, rec *Rec) (bool, error) {
		if rec == nil {
			return false, fmt.Errorf("nil record for key=%q", key)
		}
		if rec.Name == "" {
			return false, fmt.Errorf("empty name for key=%q", key)
		}
		live[key] = liveRec{key: key, name: rec.Name}
		return true, nil
	})
	if err != nil {
		t.Fatalf("SeqScan: %v", err)
	}

	count, err := db.Count(nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if int(count) != len(live) {
		t.Fatalf("count mismatch after concurrent ops: got=%d want=%d", count, len(live))
	}

	liveByName := make(map[string]string, len(live))
	for _, rec := range live {
		ids, err := db.QueryKeys(qx.Query(qx.EQ("name", rec.name)))
		if err != nil {
			t.Fatalf("QueryKeys(name=%q): %v", rec.name, err)
		}
		if len(ids) != 1 || ids[0] != rec.key {
			t.Fatalf("name index mismatch: name=%q got=%v want=[%q]", rec.name, ids, rec.key)
		}
		liveByName[rec.name] = rec.key
	}

	staleChecked := 0
	historicNames.Range(func(k, _ any) bool {
		if staleChecked >= staleProbe {
			return false
		}
		name, ok := k.(string)
		if !ok {
			return true
		}
		if _, liveNow := liveByName[name]; liveNow {
			return true
		}
		ids, err := db.QueryKeys(qx.Query(qx.EQ("name", name)))
		if err != nil {
			t.Fatalf("QueryKeys(stale name=%q): %v", name, err)
		}
		if len(ids) != 0 {
			t.Fatalf("stale name %q still indexed: ids=%v", name, ids)
		}
		staleChecked++
		return true
	})
}

func TestConcurrentWriters_FinalStateAndIndexConsistency(t *testing.T) {
	db, _ := openTempDBUint64(t)

	const (
		writers    = 8
		opsPerW    = 320
		idSpace    = 96
		readers    = 4
		staleProbe = 256
	)

	var historicNames sync.Map

	errCh := make(chan error, writers+readers+16)
	stopReaders := make(chan struct{})

	readQueries := []*qx.QX{
		qx.Query(qx.PREFIX("name", "id-")),
		qx.Query(qx.GTE("age", 0)),
		qx.Query(qx.HASANY("tags", []string{"w0", "w1", "w2", "w3"})),
		qx.Query(qx.EQ("active", true)),
	}

	var readersWG sync.WaitGroup
	for r := 0; r < readers; r++ {
		readersWG.Add(1)
		go func(readerID int) {
			defer readersWG.Done()
			i := 0
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				q := readQueries[(readerID+i)%len(readQueries)]
				i++
				if _, err := db.QueryKeys(q); err != nil {
					errCh <- fmt.Errorf("reader QueryKeys error: %w", err)
					return
				}
				if _, err := db.Query(q); err != nil {
					errCh <- fmt.Errorf("reader Query error: %w", err)
					return
				}
				if _, err := db.Count(q); err != nil {
					errCh <- fmt.Errorf("reader Count error: %w", err)
					return
				}
			}
		}(r)
	}

	var writersWG sync.WaitGroup
	for w := 0; w < writers; w++ {
		w := w
		writersWG.Add(1)
		go func() {
			defer writersWG.Done()
			for i := 0; i < opsPerW; i++ {
				id := uint64((w*131+i*17)%idSpace + 1)
				if (w+i)%5 == 0 {
					if err := db.Delete(id); err != nil {
						errCh <- fmt.Errorf("writer=%d Delete(%d): %w", w, id, err)
						return
					}
					continue
				}

				name := fmt.Sprintf("id-%03d-w%02d-op%04d", id, w, i)
				age := w*10_000 + i
				historicNames.Store(name, struct{}{})
				rec := &Rec{
					Name:   name,
					Age:    age,
					Active: i%2 == 0,
					Tags: []string{
						fmt.Sprintf("w%d", w%4),
						fmt.Sprintf("slot-%d", i%7),
					},
					Meta: Meta{Country: "NL"},
				}
				if err := db.Set(id, rec); err != nil {
					errCh <- fmt.Errorf("writer=%d Set(%d): %w", w, id, err)
					return
				}
			}
		}()
	}

	writersWG.Wait()
	close(stopReaders)
	readersWG.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("%v", err)
	}
	if t.Failed() {
		t.FailNow()
	}

	type liveRec struct {
		id   uint64
		name string
		age  int
	}
	live := make(map[uint64]liveRec, idSpace)
	err := db.SeqScan(0, func(id uint64, rec *Rec) (bool, error) {
		if rec == nil {
			return false, fmt.Errorf("nil record for id=%d", id)
		}
		if rec.Name == "" {
			return false, fmt.Errorf("empty name for id=%d", id)
		}
		live[id] = liveRec{id: id, name: rec.Name, age: rec.Age}
		return true, nil
	})
	if err != nil {
		t.Fatalf("SeqScan: %v", err)
	}

	total, err := db.Count(nil)
	if err != nil {
		t.Fatalf("Count(nil): %v", err)
	}
	if total != uint64(len(live)) {
		t.Fatalf("count mismatch: got=%d want=%d", total, len(live))
	}

	liveNames := make(map[string]uint64, len(live))
	for id, rec := range live {
		ids, err := db.QueryKeys(qx.Query(qx.EQ("name", rec.name)))
		if err != nil {
			t.Fatalf("QueryKeys(name=%q): %v", rec.name, err)
		}
		if len(ids) != 1 || ids[0] != id {
			t.Fatalf("name index mismatch for %q: got=%v want=[%d]", rec.name, ids, id)
		}
		liveNames[rec.name] = id
	}

	checked := 0
	historicNames.Range(func(k, _ any) bool {
		if checked >= staleProbe {
			return false
		}
		name, ok := k.(string)
		if !ok {
			return true
		}
		if _, stillLive := liveNames[name]; stillLive {
			return true
		}
		ids, err := db.QueryKeys(qx.Query(qx.EQ("name", name)))
		if err != nil {
			t.Fatalf("QueryKeys(stale name=%q): %v", name, err)
		}
		if len(ids) != 0 {
			t.Fatalf("stale name %q still indexed: ids=%v", name, ids)
		}
		checked++
		return true
	})
}

func TestConcurrentBatchWriters_ModelReplayConsistency(t *testing.T) {
	db, _ := openTempDBUint64(t)

	const (
		writers    = 6
		opsPerW    = 220
		idSpace    = 128
		readers    = 3
		staleProbe = 320
	)

	makeBatchIDs := func(writer, iter, size int) []uint64 {
		out := make([]uint64, 0, size)
		used := make(map[uint64]struct{}, size)
		base := writer*1009 + iter*67 + 11
		for len(out) < size {
			cand := uint64((base+len(out)*17+writer*13+iter*7)%idSpace + 1)
			if _, ok := used[cand]; ok {
				base++
				continue
			}
			used[cand] = struct{}{}
			out = append(out, cand)
		}
		return out
	}

	var historicNames sync.Map

	errCh := make(chan error, writers+readers+16)
	stopReaders := make(chan struct{})

	readQueries := []*qx.QX{
		qx.Query(qx.PREFIX("name", "bm-id")),
		qx.Query(qx.GTE("age", 0)),
		qx.Query(qx.EQ("active", true)),
		qx.Query(qx.HASANY("tags", []string{"bw0", "bw1", "bw2", "bw3"})),
	}

	var readersWG sync.WaitGroup
	for r := 0; r < readers; r++ {
		readersWG.Add(1)
		go func(readerID int) {
			defer readersWG.Done()
			i := 0
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				q := readQueries[(readerID+i)%len(readQueries)]
				i++
				if _, err := db.QueryKeys(q); err != nil {
					errCh <- fmt.Errorf("reader QueryKeys error: %w", err)
					return
				}
				if _, err := db.Query(q); err != nil {
					errCh <- fmt.Errorf("reader Query error: %w", err)
					return
				}
				if _, err := db.Count(q); err != nil {
					errCh <- fmt.Errorf("reader Count error: %w", err)
					return
				}
			}
		}(r)
	}

	var writersWG sync.WaitGroup
	for w := 0; w < writers; w++ {
		w := w
		writersWG.Add(1)
		go func() {
			defer writersWG.Done()
			for i := 0; i < opsPerW; i++ {
				opType := (w*37 + i*13) % 3
				switch opType {
				case 0: // BatchSet
					size := 2 + ((w + i) % 4)
					ids := makeBatchIDs(w, i, size)
					vals := make([]*Rec, 0, len(ids))
					for p, id := range ids {
						name := fmt.Sprintf("bm-id%03d-w%02d-i%04d-p%02d", id, w, i, p)
						age := w*100_000 + i*10 + p
						active := (i+p)%2 == 0
						historicNames.Store(name, struct{}{})
						vals = append(vals, &Rec{
							Name:   name,
							Age:    age,
							Active: active,
							Tags: []string{
								fmt.Sprintf("bw%d", w%4),
								fmt.Sprintf("grp-%d", i%9),
							},
							Meta: Meta{Country: "NL"},
						})
					}
					if err := db.BatchSet(ids, vals); err != nil {
						errCh <- fmt.Errorf("writer=%d BatchSet: %w", w, err)
						return
					}

				case 1: // BatchPatch
					size := 2 + ((w + i + 1) % 4)
					ids := makeBatchIDs(w+19, i+7, size)
					patchAge := 900_000 + w*1000 + i
					patchActive := i%2 == 0
					patch := []Field{
						{Name: "age", Value: patchAge},
						{Name: "active", Value: patchActive},
					}
					if err := db.BatchPatch(ids, patch); err != nil {
						errCh <- fmt.Errorf("writer=%d BatchPatch: %w", w, err)
						return
					}

				default: // BatchDelete
					size := 1 + ((w + i + 2) % 4)
					ids := makeBatchIDs(w+41, i+3, size)
					if err := db.BatchDelete(ids); err != nil {
						errCh <- fmt.Errorf("writer=%d BatchDelete: %w", w, err)
						return
					}
				}
			}
		}()
	}

	writersWG.Wait()
	close(stopReaders)
	readersWG.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("%v", err)
	}
	if t.Failed() {
		t.FailNow()
	}

	type liveRec struct {
		name   string
		age    int
		active bool
	}
	live := make(map[uint64]liveRec, idSpace)
	err := db.SeqScan(0, func(id uint64, rec *Rec) (bool, error) {
		if rec == nil {
			return false, fmt.Errorf("nil record for id=%d", id)
		}
		live[id] = liveRec{name: rec.Name, age: rec.Age, active: rec.Active}
		return true, nil
	})
	if err != nil {
		t.Fatalf("SeqScan: %v", err)
	}

	total, err := db.Count(nil)
	if err != nil {
		t.Fatalf("Count(nil): %v", err)
	}
	if total != uint64(len(live)) {
		t.Fatalf("count mismatch: got=%d want=%d", total, len(live))
	}

	liveNames := make(map[string]uint64, len(live))
	for id, exp := range live {
		v, err := db.Get(id)
		if err != nil {
			t.Fatalf("Get(%d): %v", id, err)
		}
		if v == nil {
			t.Fatalf("Get(%d): expected non-nil value", id)
		}
		if v.Name != exp.name || v.Age != exp.age || v.Active != exp.active {
			t.Fatalf(
				"record mismatch for id=%d: got={name:%q age:%d active:%v} want={name:%q age:%d active:%v}",
				id, v.Name, v.Age, v.Active, exp.name, exp.age, exp.active,
			)
		}

		ids, err := db.QueryKeys(qx.Query(qx.EQ("name", exp.name)))
		if err != nil {
			t.Fatalf("QueryKeys(name=%q): %v", exp.name, err)
		}
		if len(ids) != 1 || ids[0] != id {
			t.Fatalf("name index mismatch for %q: got=%v want=[%d]", exp.name, ids, id)
		}
		liveNames[exp.name] = id
	}

	checked := 0
	historicNames.Range(func(k, _ any) bool {
		if checked >= staleProbe {
			return false
		}
		name, ok := k.(string)
		if !ok {
			return true
		}
		if _, stillLive := liveNames[name]; stillLive {
			return true
		}
		ids, err := db.QueryKeys(qx.Query(qx.EQ("name", name)))
		if err != nil {
			t.Fatalf("QueryKeys(stale name=%q): %v", name, err)
		}
		if len(ids) != 0 {
			t.Fatalf("stale name %q still indexed: ids=%v", name, ids)
		}
		checked++
		return true
	})
}

func TestBatchConcurrentSingleOps_ModelReplayConsistency(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchMax:      32,
		AutoBatchMaxQueue: 0, // unlimited to avoid queue backpressure in the test
	})

	const (
		writers    = 8
		opsPerW    = 220
		idSpace    = 96
		staleProbe = 512
	)

	type op struct {
		kind  uint8 // 0=set, 1=patch_if_exists, 2=delete
		id    uint64
		rec   *Rec
		patch []Field
	}

	cloneRec := func(v *Rec) *Rec {
		if v == nil {
			return nil
		}
		cp := *v
		cp.Tags = slices.Clone(v.Tags)
		if v.Opt != nil {
			s := *v.Opt
			cp.Opt = &s
		}
		return &cp
	}
	clonePatch := func(in []Field) []Field {
		out := make([]Field, len(in))
		for i := range in {
			out[i] = in[i]
			if tags, ok := in[i].Value.([]string); ok {
				out[i].Value = slices.Clone(tags)
			}
		}
		return out
	}
	var (
		logMu sync.Mutex
		logs  = make([]op, 0, writers*opsPerW)
	)

	errCh := make(chan error, writers)
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(7300 + int64(w)*101))
			countries := []string{"NL", "PL", "DE", "FI", "IS", "TH", "US"}
			names := []string{"alice", "bob", "carol", "dave", "eve", "nik"}
			tags := []string{"go", "db", "ops", "rust", "java", "infra", "ml"}

			randTags := func() []string {
				n := 1 + r.Intn(4)
				out := make([]string, 0, n)
				for i := 0; i < n; i++ {
					out = append(out, tags[r.Intn(len(tags))])
				}
				return out
			}

			for i := 0; i < opsPerW; i++ {
				id := uint64(1 + r.Intn(idSpace))
				switch r.Intn(3) {
				case 0: // Set
					rec := &Rec{
						Meta:     Meta{Country: countries[r.Intn(len(countries))]},
						Name:     names[r.Intn(len(names))],
						Email:    fmt.Sprintf("w%02d-id%03d-i%04d@example.test", w, id, i),
						Age:      18 + r.Intn(62),
						Score:    float64(r.Intn(5000)) / 10.0,
						Active:   r.Intn(2) == 0,
						Tags:     randTags(),
						FullName: fmt.Sprintf("ID-%03d", id),
					}
					if err := db.Set(id, rec); err != nil {
						errCh <- fmt.Errorf("writer=%d set id=%d: %w", w, id, err)
						return
					}
					logMu.Lock()
					logs = append(logs, op{
						kind: 0,
						id:   id,
						rec:  cloneRec(rec),
					})
					logMu.Unlock()

				case 1: // Patch
					var patch []Field
					switch r.Intn(6) {
					case 0:
						patch = []Field{{Name: "name", Value: names[r.Intn(len(names))]}}
					case 1:
						patch = []Field{{Name: "age", Value: 18 + r.Intn(62)}}
					case 2:
						patch = []Field{{Name: "country", Value: countries[r.Intn(len(countries))]}}
					case 3:
						patch = []Field{{Name: "active", Value: r.Intn(2) == 0}}
					case 4:
						patch = []Field{{Name: "score", Value: float64(r.Intn(5000)) / 10.0}}
					default:
						patch = []Field{{Name: "tags", Value: randTags()}}
					}
					if err := db.Patch(id, patch); err != nil {
						errCh <- fmt.Errorf("writer=%d patch id=%d patch=%v: %w", w, id, patch, err)
						return
					}
					logMu.Lock()
					logs = append(logs, op{
						kind:  1,
						id:    id,
						patch: clonePatch(patch),
					})
					logMu.Unlock()

				default: // Delete
					if err := db.Delete(id); err != nil {
						errCh <- fmt.Errorf("writer=%d delete id=%d: %w", w, id, err)
						return
					}
					logMu.Lock()
					logs = append(logs, op{
						kind: 2,
						id:   id,
					})
					logMu.Unlock()
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("%v", err)
	}
	if t.Failed() {
		t.FailNow()
	}

	bs := db.AutoBatchStats()
	if bs.FallbackClosed != 0 {
		t.Fatalf("unexpected auto-batcher fallback stats: %+v", bs)
	}
	if bs.MultiRequestBatches == 0 {
		t.Fatalf("expected at least one multi-request batch, stats=%+v", bs)
	}

	logMu.Lock()
	ops := slices.Clone(logs)
	logMu.Unlock()

	type singleOpHistory struct {
		names     map[string]struct{}
		emails    map[string]struct{}
		countries map[string]struct{}
		ages      map[int]struct{}
		scores    map[float64]struct{}
		actives   map[bool]struct{}
		tags      map[string]struct{}
	}
	getHistory := func(m map[uint64]*singleOpHistory, id uint64) *singleOpHistory {
		h := m[id]
		if h != nil {
			return h
		}
		h = &singleOpHistory{
			names:     make(map[string]struct{}),
			emails:    make(map[string]struct{}),
			countries: make(map[string]struct{}),
			ages:      make(map[int]struct{}),
			scores:    make(map[float64]struct{}),
			actives:   make(map[bool]struct{}),
			tags:      make(map[string]struct{}),
		}
		m[id] = h
		return h
	}

	historyByID := make(map[uint64]*singleOpHistory, idSpace)
	for _, o := range ops {
		h := getHistory(historyByID, o.id)
		switch o.kind {
		case 0:
			if o.rec == nil {
				continue
			}
			h.names[o.rec.Name] = struct{}{}
			h.emails[o.rec.Email] = struct{}{}
			h.countries[o.rec.Country] = struct{}{}
			h.ages[o.rec.Age] = struct{}{}
			h.scores[o.rec.Score] = struct{}{}
			h.actives[o.rec.Active] = struct{}{}
			for _, tag := range o.rec.Tags {
				h.tags[tag] = struct{}{}
			}
		case 1:
			for _, f := range o.patch {
				switch f.Name {
				case "name":
					h.names[f.Value.(string)] = struct{}{}
				case "email":
					h.emails[f.Value.(string)] = struct{}{}
				case "country":
					h.countries[f.Value.(string)] = struct{}{}
				case "age":
					h.ages[f.Value.(int)] = struct{}{}
				case "score":
					h.scores[f.Value.(float64)] = struct{}{}
				case "active":
					h.actives[f.Value.(bool)] = struct{}{}
				case "tags":
					for _, tag := range f.Value.([]string) {
						h.tags[tag] = struct{}{}
					}
				}
			}
		}
	}

	live := make(map[uint64]*Rec, idSpace)
	scanErr := db.SeqScan(0, func(id uint64, rec *Rec) (bool, error) {
		if rec == nil {
			return false, fmt.Errorf("nil record for id=%d", id)
		}
		live[id] = cloneRec(rec)
		return true, nil
	})
	if scanErr != nil {
		t.Fatalf("SeqScan: %v", scanErr)
	}

	count, err := db.Count(nil)
	if err != nil {
		t.Fatalf("Count(nil): %v", err)
	}
	if count != uint64(len(live)) {
		t.Fatalf("count mismatch: got=%d want=%d", count, len(live))
	}

	queryContainsID := func(q *qx.QX, id uint64) bool {
		ids, qerr := db.QueryKeys(q)
		if qerr != nil {
			t.Fatalf("QueryKeys(%v): %v", q, qerr)
		}
		return slices.Contains(ids, id)
	}
	assertIndexContains := func(q *qx.QX, id uint64, desc string) {
		if !queryContainsID(q, id) {
			t.Fatalf("%s index missing id=%d", desc, id)
		}
	}
	assertIndexOmits := func(q *qx.QX, id uint64, desc string) {
		if queryContainsID(q, id) {
			t.Fatalf("stale %s index still contains id=%d", desc, id)
		}
	}

	for id := uint64(1); id <= idSpace; id++ {
		got, err := db.Get(id)
		if err != nil {
			t.Fatalf("Get(%d): %v", id, err)
		}
		want := live[id]
		switch {
		case want == nil && got != nil:
			t.Fatalf("id=%d expected nil, got %#v", id, got)
		case want != nil && got == nil:
			t.Fatalf("id=%d expected value, got nil", id)
		case want != nil:
			if !reflect.DeepEqual(*got, *want) {
				t.Fatalf("id=%d payload mismatch\n got=%#v\nwant=%#v", id, got, want)
			}
		}

		fullName := fmt.Sprintf("ID-%03d", id)
		if want == nil {
			assertIndexOmits(qx.Query(qx.EQ("full_name", fullName)), id, fmt.Sprintf("full_name=%q", fullName))
			continue
		}

		assertIndexContains(qx.Query(qx.EQ("full_name", fullName)), id, fmt.Sprintf("full_name=%q", fullName))
		assertIndexContains(qx.Query(qx.EQ("name", want.Name)), id, fmt.Sprintf("name=%q", want.Name))
		assertIndexContains(qx.Query(qx.EQ("email", want.Email)), id, fmt.Sprintf("email=%q", want.Email))
		assertIndexContains(qx.Query(qx.EQ("country", want.Country)), id, fmt.Sprintf("country=%q", want.Country))
		assertIndexContains(qx.Query(qx.EQ("age", want.Age)), id, fmt.Sprintf("age=%d", want.Age))
		assertIndexContains(qx.Query(qx.EQ("score", want.Score)), id, fmt.Sprintf("score=%v", want.Score))
		assertIndexContains(qx.Query(qx.EQ("active", want.Active)), id, fmt.Sprintf("active=%v", want.Active))
		for _, tag := range want.Tags {
			assertIndexContains(qx.Query(qx.HAS("tags", []string{tag})), id, fmt.Sprintf("tag=%q", tag))
		}
	}

	staleChecked := 0
	for id := uint64(1); id <= idSpace && staleChecked < staleProbe; id++ {
		h := historyByID[id]
		if h == nil {
			continue
		}
		cur := live[id]

		for name := range h.names {
			if staleChecked >= staleProbe {
				break
			}
			if cur != nil && cur.Name == name {
				continue
			}
			assertIndexOmits(qx.Query(qx.EQ("name", name)), id, fmt.Sprintf("name=%q", name))
			staleChecked++
		}
		for email := range h.emails {
			if staleChecked >= staleProbe {
				break
			}
			if cur != nil && cur.Email == email {
				continue
			}
			assertIndexOmits(qx.Query(qx.EQ("email", email)), id, fmt.Sprintf("email=%q", email))
			staleChecked++
		}
		for country := range h.countries {
			if staleChecked >= staleProbe {
				break
			}
			if cur != nil && cur.Country == country {
				continue
			}
			assertIndexOmits(qx.Query(qx.EQ("country", country)), id, fmt.Sprintf("country=%q", country))
			staleChecked++
		}
		for age := range h.ages {
			if staleChecked >= staleProbe {
				break
			}
			if cur != nil && cur.Age == age {
				continue
			}
			assertIndexOmits(qx.Query(qx.EQ("age", age)), id, fmt.Sprintf("age=%d", age))
			staleChecked++
		}
		for score := range h.scores {
			if staleChecked >= staleProbe {
				break
			}
			if cur != nil && cur.Score == score {
				continue
			}
			assertIndexOmits(qx.Query(qx.EQ("score", score)), id, fmt.Sprintf("score=%v", score))
			staleChecked++
		}
		for active := range h.actives {
			if staleChecked >= staleProbe {
				break
			}
			if cur != nil && cur.Active == active {
				continue
			}
			assertIndexOmits(qx.Query(qx.EQ("active", active)), id, fmt.Sprintf("active=%v", active))
			staleChecked++
		}
		for tag := range h.tags {
			if staleChecked >= staleProbe {
				break
			}
			if cur != nil && slices.Contains(cur.Tags, tag) {
				continue
			}
			assertIndexOmits(qx.Query(qx.HAS("tags", []string{tag})), id, fmt.Sprintf("tag=%q", tag))
			staleChecked++
		}
	}
}

func TestStringKeys_QueryOrder_FollowsInternalIndex(t *testing.T) {
	db, _ := openTempDBString(t)

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
	db, _ := openTempDBString(t)

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

func TestStringKeys_QueryResultKey_RemainsValidAfterClose(t *testing.T) {
	db, _ := openTempDBString(t)
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

	db1, err := New[uint64, Rec](rawDB, Options{BucketName: "users"})
	if err != nil {
		t.Fatalf("first open failed: %v", err)
	}

	_, err = New[uint64, Rec](rawDB, Options{BucketName: "users"})
	if err == nil {
		t.Fatal("expected error on double open, got nil")
	}

	db2, err := New[uint64, Rec](rawDB, Options{BucketName: "admins"})
	if err != nil {
		t.Fatalf("different bucket open failed: %v", err)
	}

	if err = db1.Close(); err != nil {
		t.Fatalf("db1 close: %v", err)
	}

	db1Reopen, err := New[uint64, Rec](rawDB, Options{BucketName: "users"})
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

func TestFailpoint_CommitSetRollsBackAndKeepsState(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchMax: 1,
	})

	db.testHooks.beforeCommit = func(op string) error {
		if op == "set" {
			return fmt.Errorf("failpoint: commit set")
		}
		return nil
	}
	err := db.Set(1, &Rec{Name: "alice", Age: 30})
	if err == nil || !strings.Contains(err.Error(), "failpoint: commit set") {
		t.Fatalf("expected failpoint commit error, got: %v", err)
	}
	db.testHooks.beforeCommit = nil

	v, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v != nil {
		t.Fatalf("expected no value after failed commit, got: %#v", v)
	}

	if cnt, err := db.Count(nil); err != nil {
		t.Fatalf("Count: %v", err)
	} else if cnt != 0 {
		t.Fatalf("expected Count=0 after failed commit, got %d", cnt)
	}
}

func TestFailpoint_CommitBatchRollsBackAndKeepsState(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   10 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 1024,
	})

	var once atomic.Bool
	db.testHooks.beforeCommit = func(op string) error {
		if op == "batch" && once.CompareAndSwap(false, true) {
			return fmt.Errorf("failpoint: commit batch")
		}
		return nil
	}

	err := db.Set(1, &Rec{Name: "alice", Age: 30})
	if err == nil || !strings.Contains(err.Error(), "failpoint: commit batch") {
		t.Fatalf("expected failpoint batch commit error, got: %v", err)
	}
	db.testHooks.beforeCommit = nil

	v, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v != nil {
		t.Fatalf("expected no value after failed batch commit, got: %#v", v)
	}

	if st := db.AutoBatchStats(); st.TxCommitErrors == 0 {
		t.Fatalf("expected AutoBatchStats.TxCommitErrors > 0 after failpoint commit")
	}
}

func TestFailpoint_PostCommitPublishSet_BreaksDBAndSkipsIndexStore(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	db.testHooks.afterCommitPublish = func(op string) {
		if op == "set" {
			panic("failpoint: publish set")
		}
	}
	err := db.Set(1, &Rec{Name: "alice", Age: 30})
	if !errors.Is(err, ErrBroken) {
		t.Fatalf("expected ErrBroken from post-commit publish panic, got: %v", err)
	}

	assertNoFutureSnapshotRefs(t, db)

	var got *Rec
	if viewErr := db.bolt.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(db.bucket)
		if bucket == nil {
			return fmt.Errorf("bucket does not exist")
		}
		raw := bucket.Get(db.keyFromID(1))
		if raw == nil {
			return fmt.Errorf("record not committed")
		}
		var err error
		got, err = db.decode(raw)
		return err
	}); viewErr != nil {
		t.Fatalf("bolt view after broken publish: %v", viewErr)
	}
	if got == nil || got.Name != "alice" || got.Age != 30 {
		t.Fatalf("unexpected committed value after broken publish: %#v", got)
	}

	if err = db.Set(2, &Rec{Name: "bob", Age: 20}); !errors.Is(err, ErrBroken) {
		t.Fatalf("expected DB to reject subsequent writes with ErrBroken, got: %v", err)
	}
	if _, err = db.Query(qx.Query(qx.EQ("name", "alice"))); !errors.Is(err, ErrBroken) {
		t.Fatalf("expected Query to fail with ErrBroken after broken publish, got: %v", err)
	}

	db.testHooks.beforeStoreIndex = func() error {
		return fmt.Errorf("storeIndex must be skipped for broken db")
	}
	if err = db.Close(); !errors.Is(err, ErrBroken) {
		t.Fatalf("expected Close to return ErrBroken for broken db, got: %v", err)
	}
}

func TestFailpoint_PostCommitPublishBatch_BreaksDBAndClearsStagedSnapshot(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   10 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 1024,
	})

	db.testHooks.afterCommitPublish = func(op string) {
		if op == "batch" {
			panic("failpoint: publish batch")
		}
	}
	err := db.Set(1, &Rec{Name: "alice", Age: 30})
	if !errors.Is(err, ErrBroken) {
		t.Fatalf("expected ErrBroken from auto-batch publish panic, got: %v", err)
	}

	assertNoFutureSnapshotRefs(t, db)
}

func TestFailpoint_CommitTruncateRollsBackAndKeepsData(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	db.testHooks.beforeCommit = func(op string) error {
		if op == "truncate" {
			return fmt.Errorf("failpoint: commit truncate")
		}
		return nil
	}
	err := db.Truncate()
	if err == nil || !strings.Contains(err.Error(), "failpoint: commit truncate") {
		t.Fatalf("expected truncate commit failpoint error, got: %v", err)
	}
	db.testHooks.beforeCommit = nil

	v, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v == nil || v.Email != "a@x" || v.Code != 1 {
		t.Fatalf("expected original value to remain after failed truncate, got: %#v", v)
	}
}

func TestTruncate_PreservesSequenceMonotonicityAcrossBucketRecreate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "truncate_sequence.db")

	db1, raw1 := openBoltAndNew[uint64, Rec](t, path)
	if err := db1.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	persistedPath := path + ".Rec.rbi"
	initialSeq := readBucketSequence(t, raw1, db1.bucket)
	if initialSeq == 0 {
		t.Fatalf("expected sequence to advance after write, got %d", initialSeq)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("Close(db1): %v", err)
	}
	if err := raw1.Close(); err != nil {
		t.Fatalf("Close(raw1): %v", err)
	}

	storedSeq := readPersistedIndexSequence(t, persistedPath)
	if storedSeq != initialSeq {
		t.Fatalf("persisted sequence mismatch: stored=%d bucket=%d", storedSeq, initialSeq)
	}

	db2, raw2 := openBoltAndNew[uint64, Rec](t, path, Options{DisableIndexStore: true})
	if err := db2.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	truncateSeq := readBucketSequence(t, raw2, db2.bucket)
	if truncateSeq <= storedSeq {
		t.Fatalf("truncate must preserve monotonic sequence: stored=%d truncate=%d", storedSeq, truncateSeq)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("Close(db2): %v", err)
	}
	if err := raw2.Close(); err != nil {
		t.Fatalf("Close(raw2): %v", err)
	}

	if got := readPersistedIndexSequence(t, persistedPath); got != storedSeq {
		t.Fatalf("DisableIndexStore should keep old sidecar untouched: got=%d want=%d", got, storedSeq)
	}

	db3, raw3 := openBoltAndNew[uint64, Rec](t, path)
	defer func() { _ = db3.Close() }()
	defer func() { _ = raw3.Close() }()

	if got := readBucketSequence(t, raw3, db3.bucket); got != truncateSeq {
		t.Fatalf("reopened bucket sequence mismatch: got=%d want=%d", got, truncateSeq)
	}
	if snap := db3.getSnapshot(); snap == nil || snap.seq != truncateSeq {
		if snap == nil {
			t.Fatalf("expected published snapshot after reopen")
		}
		t.Fatalf("snapshot sequence mismatch after reopen: got=%d want=%d", snap.seq, truncateSeq)
	}

	ids, err := db3.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "age", Value: 30}})
	if err != nil {
		t.Fatalf("QueryKeys(after reopen): %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected rebuilt empty index after truncate, got ids=%v", ids)
	}
}

func TestFailpoint_CommitPatchAndDelete_RollbackAndNoStagedSnapshots(t *testing.T) {
	t.Run("patch", func(t *testing.T) {
		db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
		if err := db.Set(1, &Rec{Name: "alice", Age: 30, Meta: Meta{Country: "NL"}}); err != nil {
			t.Fatalf("Set(1): %v", err)
		}

		db.testHooks.beforeCommit = func(op string) error {
			if op == "patch" {
				return fmt.Errorf("failpoint: patch")
			}
			return nil
		}
		err := db.Patch(1, []Field{{Name: "age", Value: 99}})
		if err == nil || !strings.Contains(err.Error(), "failpoint: patch") {
			t.Fatalf("expected failpoint patch error, got: %v", err)
		}
		db.testHooks.beforeCommit = nil

		assertNoFutureSnapshotRefs(t, db)
		v, err := db.Get(1)
		if err != nil {
			t.Fatalf("Get(1): %v", err)
		}
		if v == nil || v.Age != 30 {
			t.Fatalf("expected record unchanged after failed patch commit, got %#v", v)
		}
	})

	t.Run("delete", func(t *testing.T) {
		db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
		if err := db.Set(1, &Rec{Name: "alice", Age: 30, Meta: Meta{Country: "NL"}}); err != nil {
			t.Fatalf("Set(1): %v", err)
		}

		db.testHooks.beforeCommit = func(op string) error {
			if op == "delete" {
				return fmt.Errorf("failpoint: delete")
			}
			return nil
		}
		err := db.Delete(1)
		if err == nil || !strings.Contains(err.Error(), "failpoint: delete") {
			t.Fatalf("expected failpoint delete error, got: %v", err)
		}
		db.testHooks.beforeCommit = nil

		assertNoFutureSnapshotRefs(t, db)
		v, err := db.Get(1)
		if err != nil {
			t.Fatalf("Get(1): %v", err)
		}
		if v == nil {
			t.Fatalf("expected record to remain after failed delete commit")
		}
	})
}

func TestFailpoint_CommitMultiWritePaths_RollbackAndNoStagedSnapshots(t *testing.T) {
	type tc struct {
		name   string
		op     string
		setup  func(t *testing.T, db *DB[uint64, Rec])
		run    func(db *DB[uint64, Rec]) error
		verify func(t *testing.T, db *DB[uint64, Rec])
	}

	cases := []tc{
		{
			name: "batch_set",
			op:   "batch_set",
			run: func(db *DB[uint64, Rec]) error {
				return db.BatchSet(
					[]uint64{1, 2},
					[]*Rec{
						{Name: "a", Age: 10, Meta: Meta{Country: "NL"}},
						{Name: "b", Age: 11, Meta: Meta{Country: "DE"}},
					},
				)
			},
			verify: func(t *testing.T, db *DB[uint64, Rec]) {
				t.Helper()
				v1, err := db.Get(1)
				if err != nil {
					t.Fatalf("Get(1): %v", err)
				}
				v2, err := db.Get(2)
				if err != nil {
					t.Fatalf("Get(2): %v", err)
				}
				if v1 != nil || v2 != nil {
					t.Fatalf("expected no committed values after failed set_many, got v1=%#v v2=%#v", v1, v2)
				}
			},
		},
		{
			name: "batch_patch",
			op:   "batch_patch",
			setup: func(t *testing.T, db *DB[uint64, Rec]) {
				t.Helper()
				if err := db.Set(1, &Rec{Name: "a", Age: 10, Meta: Meta{Country: "NL"}}); err != nil {
					t.Fatalf("Set(1): %v", err)
				}
				if err := db.Set(2, &Rec{Name: "b", Age: 11, Meta: Meta{Country: "DE"}}); err != nil {
					t.Fatalf("Set(2): %v", err)
				}
			},
			run: func(db *DB[uint64, Rec]) error {
				return db.BatchPatch([]uint64{1, 2}, []Field{{Name: "age", Value: 99}})
			},
			verify: func(t *testing.T, db *DB[uint64, Rec]) {
				t.Helper()
				v1, err := db.Get(1)
				if err != nil {
					t.Fatalf("Get(1): %v", err)
				}
				v2, err := db.Get(2)
				if err != nil {
					t.Fatalf("Get(2): %v", err)
				}
				if v1 == nil || v2 == nil {
					t.Fatalf("expected records to remain after failed patch_many, got v1=%#v v2=%#v", v1, v2)
				}
				if v1.Age != 10 || v2.Age != 11 {
					t.Fatalf("expected ages unchanged after failed patch_many, got v1=%d v2=%d", v1.Age, v2.Age)
				}
			},
		},
		{
			name: "batch_delete",
			op:   "batch_delete",
			setup: func(t *testing.T, db *DB[uint64, Rec]) {
				t.Helper()
				if err := db.Set(1, &Rec{Name: "a", Age: 10, Meta: Meta{Country: "NL"}}); err != nil {
					t.Fatalf("Set(1): %v", err)
				}
				if err := db.Set(2, &Rec{Name: "b", Age: 11, Meta: Meta{Country: "DE"}}); err != nil {
					t.Fatalf("Set(2): %v", err)
				}
			},
			run: func(db *DB[uint64, Rec]) error {
				return db.BatchDelete([]uint64{1, 2})
			},
			verify: func(t *testing.T, db *DB[uint64, Rec]) {
				t.Helper()
				v1, err := db.Get(1)
				if err != nil {
					t.Fatalf("Get(1): %v", err)
				}
				v2, err := db.Get(2)
				if err != nil {
					t.Fatalf("Get(2): %v", err)
				}
				if v1 == nil || v2 == nil {
					t.Fatalf("expected records to remain after failed delete_many, got v1=%#v v2=%#v", v1, v2)
				}
			},
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
			if c.setup != nil {
				c.setup(t, db)
			}

			db.testHooks.beforeCommit = func(op string) error {
				if op == c.op {
					return fmt.Errorf("failpoint: %s", c.op)
				}
				return nil
			}
			err := c.run(db)
			if err == nil || !strings.Contains(err.Error(), "failpoint: "+c.op) {
				t.Fatalf("expected failpoint error for %s, got: %v", c.op, err)
			}
			db.testHooks.beforeCommit = nil

			assertNoFutureSnapshotRefs(t, db)

			c.verify(t, db)
		})
	}
}

func TestBatchSet_DuplicateIDs_LastWriteWinsAndIndexConsistent(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	if err := db.Set(1, &Rec{Name: "seed", Age: 21, Tags: []string{"go"}, Meta: Meta{Country: "NL"}}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	valA := &Rec{Name: "first", Age: 30, Tags: []string{"rust"}, Meta: Meta{Country: "DE"}}
	valB := &Rec{Name: "second", Age: 40, Tags: []string{"db", "ops"}, Meta: Meta{Country: "PL"}}
	if err := db.BatchSet([]uint64{1, 1}, []*Rec{valA, valB}); err != nil {
		t.Fatalf("BatchSet duplicate ids: %v", err)
	}

	got, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil || got.Name != "second" || got.Age != 40 || got.Country != "PL" {
		t.Fatalf("expected last value to win, got %#v", got)
	}

	assertHas := func(field string, value string, want bool) {
		t.Helper()
		expr := qx.EQ(field, value)
		if field == "tags" {
			expr = qx.HASANY(field, []string{value})
		}
		ids, err := db.QueryKeys(qx.Query(expr))
		if err != nil {
			t.Fatalf("QueryKeys(%s=%s): %v", field, value, err)
		}
		has := false
		for _, id := range ids {
			if id == 1 {
				has = true
				break
			}
		}
		if has != want {
			t.Fatalf("unexpected index membership for %s=%s: has=%v want=%v ids=%v", field, value, has, want, ids)
		}
	}

	assertHas("name", "first", false)
	assertHas("name", "second", true)
	assertHas("country", "DE", false)
	assertHas("country", "PL", true)
	assertHas("tags", "rust", false)
	assertHas("tags", "db", true)
	assertHas("tags", "ops", true)

	if cnt, err := db.Count(nil); err != nil {
		t.Fatalf("Count: %v", err)
	} else if cnt != 1 {
		t.Fatalf("expected Count=1, got %d", cnt)
	}
}

func TestBatchSet_DuplicateIDs_BeforeCommit_SeesPerStepTxState(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	if err := db.Set(1, &Rec{Name: "seed", Age: 21}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	var seen []string
	err := db.BatchSet(
		[]uint64{1, 1},
		[]*Rec{
			{Name: "first", Age: 30},
			{Name: "second", Age: 40},
		},
		BeforeCommit(func(tx *bbolt.Tx, key uint64, oldValue, newValue *Rec) error {
			raw := tx.Bucket(db.bucket).Get(db.keyFromID(key))
			if raw == nil {
				return fmt.Errorf("missing value for id=%d inside BeforeCommit", key)
			}
			current, err := db.decode(raw)
			if err != nil {
				return fmt.Errorf("decode current tx value: %w", err)
			}
			if current.Name != newValue.Name || current.Age != newValue.Age {
				return fmt.Errorf("tx value mismatch: got=%#v want=%#v", current, newValue)
			}
			seen = append(seen, current.Name)
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("BatchSet duplicate ids with BeforeCommit: %v", err)
	}

	if !slices.Equal(seen, []string{"first", "second"}) {
		t.Fatalf("unexpected BeforeCommit observation order: %v", seen)
	}
}

func TestSet_NilValue_ReturnsErrNilValueAndNoWrites(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	err := db.Set(1, nil)
	if err == nil || !errors.Is(err, ErrNilValue) {
		t.Fatalf("expected ErrNilValue, got: %v", err)
	}

	v, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v != nil {
		t.Fatalf("expected nil value for id=1, got %#v", v)
	}

	if cnt, err := db.Count(nil); err != nil {
		t.Fatalf("Count(nil): %v", err)
	} else if cnt != 0 {
		t.Fatalf("expected Count=0, got %d", cnt)
	}

	ids, err := db.QueryKeys(qx.Query())
	if err != nil {
		t.Fatalf("QueryKeys(all): %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected no indexed ids, got %v", ids)
	}

	assertNoFutureSnapshotRefs(t, db)
}

func TestBatchSet_NilValue_ReturnsErrNilValueAndAtomic(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	base := &Rec{Name: "base", Age: 21, Tags: []string{"go"}, Meta: Meta{Country: "NL"}}
	if err := db.Set(1, base); err != nil {
		t.Fatalf("seed Set(1): %v", err)
	}

	err := db.BatchSet(
		[]uint64{1, 2},
		[]*Rec{
			{Name: "changed", Age: 55, Tags: []string{"ops"}, Meta: Meta{Country: "DE"}},
			nil,
		},
	)
	if err == nil || !errors.Is(err, ErrNilValue) {
		t.Fatalf("expected ErrNilValue from BatchSet, got: %v", err)
	}

	v1, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v1 == nil || v1.Name != "base" || v1.Age != 21 || v1.Country != "NL" {
		t.Fatalf("id=1 changed after rejected BatchSet: %#v", v1)
	}

	v2, err := db.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if v2 != nil {
		t.Fatalf("expected id=2 to remain absent, got %#v", v2)
	}

	ids, err := db.QueryKeys(qx.Query(qx.EQ("name", "changed")))
	if err != nil {
		t.Fatalf("QueryKeys(name=changed): %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("unexpected index entry for rejected value: %v", ids)
	}

	if cnt, err := db.Count(nil); err != nil {
		t.Fatalf("Count(nil): %v", err)
	} else if cnt != 1 {
		t.Fatalf("expected Count=1 after rejected BatchSet, got %d", cnt)
	}

	assertNoFutureSnapshotRefs(t, db)
}

func TestBatchPatchBatchDelete_DuplicateIDs_IndexConsistency(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	if err := db.Set(1, &Rec{Name: "n1", Age: 10, Tags: []string{"go"}, Meta: Meta{Country: "NL"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "n2", Age: 20, Tags: []string{"db"}, Meta: Meta{Country: "DE"}}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	if err := db.BatchPatch(
		[]uint64{1, 1, 2},
		[]Field{
			{Name: "age", Value: 99},
			{Name: "tags", Value: []string{"ops"}},
			{Name: "country", Value: "PL"},
		},
	); err != nil {
		t.Fatalf("BatchPatch duplicate ids: %v", err)
	}

	assertRec := func(id uint64, age int, country string) {
		t.Helper()
		v, err := db.Get(id)
		if err != nil {
			t.Fatalf("Get(%d): %v", id, err)
		}
		if v == nil {
			t.Fatalf("Get(%d): nil", id)
		}
		if v.Age != age || v.Country != country || len(v.Tags) != 1 || v.Tags[0] != "ops" {
			t.Fatalf("unexpected value for id=%d: %#v", id, v)
		}
	}
	assertRec(1, 99, "PL")
	assertRec(2, 99, "PL")

	ids, err := db.QueryKeys(qx.Query(qx.HASANY("tags", []string{"go"})))
	if err != nil {
		t.Fatalf("QueryKeys(tags has go): %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("stale tags=go index entries: %v", ids)
	}

	ids, err = db.QueryKeys(qx.Query(qx.HASANY("tags", []string{"db"})))
	if err != nil {
		t.Fatalf("QueryKeys(tags has db): %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("stale tags=db index entries: %v", ids)
	}

	ids, err = db.QueryKeys(qx.Query(qx.HASANY("tags", []string{"ops"})))
	if err != nil {
		t.Fatalf("QueryKeys(tags has ops): %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected both ids for tags=ops, got %v", ids)
	}

	if err := db.BatchDelete([]uint64{1, 1, 2, 2}); err != nil {
		t.Fatalf("BatchDelete duplicate ids: %v", err)
	}

	if cnt, err := db.Count(nil); err != nil {
		t.Fatalf("Count: %v", err)
	} else if cnt != 0 {
		t.Fatalf("expected Count=0 after duplicate BatchDelete, got %d", cnt)
	}

	ids, err = db.QueryKeys(qx.Query(qx.HASANY("tags", []string{"ops"})))
	if err != nil {
		t.Fatalf("QueryKeys(tags has ops) after delete: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected empty tags=ops index after delete, got %v", ids)
	}
}

func TestBatchPatch_DuplicateIDs_BeforeCommit_SeesPerStepTxState(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	if err := db.Set(1, &Rec{Name: "seed", Age: 21}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	call := 0
	var seen []string
	err := db.BatchPatch(
		[]uint64{1, 1},
		[]Field{{Name: "age", Value: 99}},
		BeforeProcess(func(_ uint64, value *Rec) error {
			call++
			value.Name = fmt.Sprintf("patched-%d", call)
			return nil
		}),
		BeforeCommit(func(tx *bbolt.Tx, key uint64, oldValue, newValue *Rec) error {
			raw := tx.Bucket(db.bucket).Get(db.keyFromID(key))
			if raw == nil {
				return fmt.Errorf("missing value for id=%d inside BeforeCommit", key)
			}
			current, err := db.decode(raw)
			if err != nil {
				return fmt.Errorf("decode current tx value: %w", err)
			}
			if current.Name != newValue.Name || current.Age != newValue.Age {
				return fmt.Errorf("tx value mismatch: got=%#v want=%#v", current, newValue)
			}
			seen = append(seen, current.Name)
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("BatchPatch duplicate ids with BeforeCommit: %v", err)
	}

	if !slices.Equal(seen, []string{"patched-1", "patched-2"}) {
		t.Fatalf("unexpected BeforeCommit observation order: %v", seen)
	}
}

func TestBatchPatch_DecodeError_RollsBackEarlierWrites(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	if err := db.Set(1, &Rec{Name: "a", Age: 10, Meta: Meta{Country: "NL"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "b", Age: 20, Meta: Meta{Country: "DE"}}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	var beforeRaw []byte
	if err := db.bolt.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.bucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		v := b.Get(db.keyFromID(1))
		if v == nil {
			return fmt.Errorf("missing raw value for id=1")
		}
		beforeRaw = append([]byte(nil), v...)
		return nil
	}); err != nil {
		t.Fatalf("snapshot raw(id=1): %v", err)
	}

	if err := db.bolt.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.bucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		return b.Put(db.keyFromID(2), []byte{0xff, 0x00, 0x7f, 0x42})
	}); err != nil {
		t.Fatalf("corrupt id=2 payload: %v", err)
	}

	err := db.BatchPatch([]uint64{1, 2}, []Field{{Name: "age", Value: 99}})
	if err == nil {
		t.Fatalf("expected BatchPatch decode error, got nil")
	}
	if !strings.Contains(err.Error(), "failed to decode existing value") {
		t.Fatalf("unexpected BatchPatch error: %v", err)
	}

	var afterRaw []byte
	if err := db.bolt.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.bucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		v := b.Get(db.keyFromID(1))
		if v == nil {
			return fmt.Errorf("missing raw value for id=1 after failed batch")
		}
		afterRaw = append([]byte(nil), v...)
		return nil
	}); err != nil {
		t.Fatalf("snapshot raw(id=1) after: %v", err)
	}
	if !slices.Equal(beforeRaw, afterRaw) {
		t.Fatalf("id=1 raw payload changed despite rollback")
	}

	v1, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v1 == nil || v1.Age != 10 || v1.Name != "a" || v1.Country != "NL" {
		t.Fatalf("id=1 changed after failed BatchPatch: %#v", v1)
	}
}

func TestBatchDelete_DecodeError_RollsBackEarlierDeletes(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	if err := db.Set(1, &Rec{Name: "a", Age: 10, Meta: Meta{Country: "NL"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "b", Age: 20, Meta: Meta{Country: "DE"}}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	var beforeRaw []byte
	if err := db.bolt.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.bucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		v := b.Get(db.keyFromID(1))
		if v == nil {
			return fmt.Errorf("missing raw value for id=1")
		}
		beforeRaw = append([]byte(nil), v...)
		return nil
	}); err != nil {
		t.Fatalf("snapshot raw(id=1): %v", err)
	}

	if err := db.bolt.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.bucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		return b.Put(db.keyFromID(2), []byte{0xff, 0x00, 0x7f, 0x42})
	}); err != nil {
		t.Fatalf("corrupt id=2 payload: %v", err)
	}

	err := db.BatchDelete([]uint64{1, 2})
	if err == nil {
		t.Fatalf("expected BatchDelete decode error, got nil")
	}
	if !strings.Contains(err.Error(), "decode") {
		t.Fatalf("unexpected BatchDelete error: %v", err)
	}

	var afterRaw []byte
	if err := db.bolt.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.bucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		v := b.Get(db.keyFromID(1))
		if v == nil {
			return fmt.Errorf("id=1 was deleted despite rollback")
		}
		afterRaw = append([]byte(nil), v...)
		return nil
	}); err != nil {
		t.Fatalf("snapshot raw(id=1) after: %v", err)
	}
	if !slices.Equal(beforeRaw, afterRaw) {
		t.Fatalf("id=1 raw payload changed despite rollback")
	}

	v1, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v1 == nil || v1.Name != "a" || v1.Age != 10 || v1.Country != "NL" {
		t.Fatalf("id=1 changed after failed BatchDelete: %#v", v1)
	}
}

func TestMultiWrite_CallbackError_RollbackDataAndIndex(t *testing.T) {
	type tc struct {
		name string
		run  func(db *DB[uint64, Rec], cb func(*bbolt.Tx, uint64, *Rec, *Rec) error) error
	}

	cases := []tc{
		{
			name: "batch_set",
			run: func(db *DB[uint64, Rec], cb func(*bbolt.Tx, uint64, *Rec, *Rec) error) error {
				return db.BatchSet(
					[]uint64{1, 2},
					[]*Rec{
						{Name: "new-1", Age: 100, Tags: []string{"x"}, Meta: Meta{Country: "PL"}},
						{Name: "new-2", Age: 200, Tags: []string{"y"}, Meta: Meta{Country: "DE"}},
					},
					BeforeCommit(cb),
				)
			},
		},
		{
			name: "batch_patch",
			run: func(db *DB[uint64, Rec], cb func(*bbolt.Tx, uint64, *Rec, *Rec) error) error {
				return db.BatchPatch(
					[]uint64{1, 2},
					[]Field{
						{Name: "age", Value: 777},
						{Name: "country", Value: "US"},
						{Name: "tags", Value: []string{"patched"}},
					},
					BeforeCommit(cb),
				)
			},
		},
		{
			name: "batch_delete",
			run: func(db *DB[uint64, Rec], cb func(*bbolt.Tx, uint64, *Rec, *Rec) error) error {
				return db.BatchDelete([]uint64{1, 2}, BeforeCommit(cb))
			},
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

			base := map[uint64]*Rec{
				1: {Name: "base-1", Age: 21, Tags: []string{"go"}, Meta: Meta{Country: "NL"}},
				2: {Name: "base-2", Age: 22, Tags: []string{"db"}, Meta: Meta{Country: "DE"}},
				3: {Name: "base-3", Age: 23, Tags: []string{"ops"}, Meta: Meta{Country: "PL"}},
			}
			for id, v := range base {
				cp := *v
				if err := db.Set(id, &cp); err != nil {
					t.Fatalf("seed Set(%d): %v", id, err)
				}
			}

			cbCalls := 0
			cb := func(_ *bbolt.Tx, _ uint64, _ *Rec, _ *Rec) error {
				cbCalls++
				return fmt.Errorf("callback fail")
			}

			err := c.run(db, cb)
			if err == nil || !strings.Contains(err.Error(), "callback fail") {
				t.Fatalf("expected callback error, got: %v", err)
			}
			if cbCalls == 0 {
				t.Fatalf("expected callback to be called at least once")
			}

			assertNoFutureSnapshotRefs(t, db)

			if cnt, err := db.Count(nil); err != nil {
				t.Fatalf("Count: %v", err)
			} else if cnt != 3 {
				t.Fatalf("expected Count=3 after rollback, got %d", cnt)
			}

			for id, exp := range base {
				v, err := db.Get(id)
				if err != nil {
					t.Fatalf("Get(%d): %v", id, err)
				}
				if v == nil {
					t.Fatalf("Get(%d): nil", id)
				}
				if v.Name != exp.Name || v.Age != exp.Age || v.Country != exp.Country || !slices.Equal(v.Tags, exp.Tags) {
					t.Fatalf("value changed after rollback for id=%d: got=%#v want=%#v", id, v, exp)
				}

				ids, err := db.QueryKeys(qx.Query(qx.EQ("name", exp.Name)))
				if err != nil {
					t.Fatalf("QueryKeys(name=%q): %v", exp.Name, err)
				}
				if len(ids) != 1 || ids[0] != id {
					t.Fatalf("name index mismatch after rollback for %q: %v", exp.Name, ids)
				}
			}
		})
	}
}

func TestNew_DefaultExecOptions_ApplyToWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "default_before_commit.db")

	raw, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}
	defer func() { _ = raw.Close() }()

	var calls []string
	db, err := New[uint64, Rec](
		raw,
		Options{AutoBatchMax: 1},
		PatchStrict,
		BeforeProcess(func(_ uint64, value *Rec) error {
			value.Name = "pre-" + value.Name
			value.Country = "US"
			return nil
		}),
		BeforeStore(func(_ uint64, _ *Rec, newValue *Rec) error {
			newValue.Age++
			return nil
		}),
		BeforeCommit(func(_ *bbolt.Tx, _ uint64, _, _ *Rec) error {
			calls = append(calls, "default")
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = db.Close() }()

	err = db.Set(
		1,
		&Rec{Name: "alice", Age: 30},
		BeforeCommit(func(_ *bbolt.Tx, _ uint64, _, _ *Rec) error {
			calls = append(calls, "call")
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("Set with default exec options: %v", err)
	}

	if !slices.Equal(calls, []string{"default", "call"}) {
		t.Fatalf("unexpected callback order: %v", calls)
	}

	v, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v == nil || v.Name != "pre-alice" || v.Age != 31 {
		t.Fatalf("unexpected stored value: %#v", v)
	}

	err = db.Patch(1, []Field{{Name: "country", Value: "CA"}})
	if err != nil {
		t.Fatalf("Patch with default BeforeProcess: %v", err)
	}

	v, err = db.Get(1)
	if err != nil {
		t.Fatalf("Get(1) after Patch: %v", err)
	}
	if v == nil || v.Country != "US" {
		t.Fatalf("expected Patch to inherit default BeforeProcess, got %#v", v)
	}

	err = db.Patch(1, []Field{{Name: "does_not_exist", Value: 1}})
	if err == nil {
		t.Fatal("expected Patch to inherit default PatchStrict option")
	}
}

func TestMultiWrite_CallbackError_RandomizedAtomicRollback(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	r := newRand(20260327)
	countries := []string{"NL", "PL", "DE", "US"}
	names := []string{"alice", "bob", "carol", "dave", "eve"}
	tagPool := []string{"go", "db", "ops", "rust", "java"}

	randomRec := func(id uint64) *Rec {
		name := names[r.IntN(len(names))]
		return &Rec{
			Meta:     Meta{Country: countries[r.IntN(len(countries))]},
			Name:     fmt.Sprintf("%s-%03d-%02d", name, id, r.IntN(64)),
			Age:      18 + r.IntN(62),
			Score:    float64(r.IntN(1000)) / 10.0,
			Active:   r.IntN(2) == 0,
			Tags:     []string{tagPool[r.IntN(len(tagPool))], tagPool[r.IntN(len(tagPool))]},
			FullName: fmt.Sprintf("FN-%05d", id),
		}
	}

	for i := 1; i <= 70; i++ {
		id := uint64(i)
		if err := db.Set(id, randomRec(id)); err != nil {
			t.Fatalf("seed Set(%d): %v", id, err)
		}
	}

	snapshotState := func() (map[uint64]Rec, error) {
		out := make(map[uint64]Rec, 128)
		err := db.SeqScan(0, func(id uint64, rec *Rec) (bool, error) {
			if rec == nil {
				return false, fmt.Errorf("nil rec for id=%d", id)
			}
			out[id] = *rec
			return true, nil
		})
		return out, err
	}

	verifyStateEquals := func(label string, want map[uint64]Rec) {
		t.Helper()
		got, err := snapshotState()
		if err != nil {
			t.Fatalf("%s SeqScan: %v", label, err)
		}
		if len(got) != len(want) {
			t.Fatalf("%s size mismatch: got=%d want=%d", label, len(got), len(want))
		}
		for id, w := range want {
			g, ok := got[id]
			if !ok {
				t.Fatalf("%s missing id=%d", label, id)
			}
			if !reflect.DeepEqual(g, w) {
				t.Fatalf("%s value mismatch id=%d got=%#v want=%#v", label, id, g, w)
			}
		}
		// Extra check: index path should remain aligned with data for sampled names.
		checked := 0
		for id, rec := range want {
			ids, err := db.QueryKeys(qx.Query(qx.EQ("name", rec.Name)))
			if err != nil {
				t.Fatalf("%s QueryKeys(name=%q): %v", label, rec.Name, err)
			}
			found := false
			for _, x := range ids {
				if x == id {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("%s index drift for name=%q id=%d ids=%v", label, rec.Name, id, ids)
			}
			checked++
			if checked >= 18 {
				break
			}
		}
	}

	for step := 0; step < 80; step++ {
		before, err := snapshotState()
		if err != nil {
			t.Fatalf("step=%d snapshot before: %v", step, err)
		}

		failAt := 1
		cbCalls := 0
		cb := func(_ *bbolt.Tx, _ uint64, _ *Rec, _ *Rec) error {
			cbCalls++
			if cbCalls == failAt {
				return fmt.Errorf("callback fail at %d", failAt)
			}
			return nil
		}

		var opErr error
		switch step % 3 {
		case 0: // BatchSet with potential duplicate ids
			n := 3 + r.IntN(6)
			ids := make([]uint64, n)
			vals := make([]*Rec, n)
			for i := 0; i < n; i++ {
				id := uint64(1 + r.IntN(96))
				ids[i] = id
				vals[i] = randomRec(id)
			}
			opErr = db.BatchSet(ids, vals, BeforeCommit(cb))
		case 1: // BatchPatch
			n := 3 + r.IntN(6)
			ids := make([]uint64, n)
			for i := 0; i < n; i++ {
				ids[i] = uint64(1 + r.IntN(96))
			}
			patch := []Field{
				{Name: "age", Value: 18 + r.IntN(62)},
				{Name: "country", Value: countries[r.IntN(len(countries))]},
			}
			opErr = db.BatchPatch(ids, patch, BeforeCommit(cb))
		default: // BatchDelete
			n := 2 + r.IntN(6)
			ids := make([]uint64, n)
			for i := 0; i < n; i++ {
				ids[i] = uint64(1 + r.IntN(96))
			}
			opErr = db.BatchDelete(ids, BeforeCommit(cb))
		}

		if cbCalls == 0 {
			if opErr != nil {
				t.Fatalf("step=%d expected nil error when no callback was invoked, got: %v", step, opErr)
			}
		} else {
			if opErr == nil || !strings.Contains(opErr.Error(), "callback fail") {
				t.Fatalf("step=%d expected callback error, got: %v (cbCalls=%d)", step, opErr, cbCalls)
			}
		}
		assertNoFutureSnapshotRefs(t, db)

		verifyStateEquals(fmt.Sprintf("step=%d", step), before)
	}
}

func TestFailpoint_CloseStoreIndexErrorStillCloses(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "close_store_failpoint.db")
	db, raw := openBoltAndNew[uint64, Rec](t, path)
	defer func() { _ = raw.Close() }()

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	db.testHooks.beforeStoreIndex = func() error {
		return fmt.Errorf("failpoint: store index")
	}
	err := db.Close()
	if err == nil || !strings.Contains(err.Error(), "failpoint: store index") {
		t.Fatalf("expected failpoint store index error on Close, got: %v", err)
	}

	if !db.closed.Load() {
		t.Fatal("expected db to be marked closed even when storeIndex fails")
	}
	if _, statErr := os.Stat(db.rbiFile); !os.IsNotExist(statErr) {
		t.Fatalf("expected persisted index to stay absent after failed Close, statErr=%v", statErr)
	}
	if err = db.Close(); err != nil {
		t.Fatalf("second Close must be no-op, got: %v", err)
	}

	if err = raw.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.bucket)
		if b == nil {
			return fmt.Errorf("bucket missing after close")
		}
		if b.Get(db.keyFromID(1)) == nil {
			return fmt.Errorf("record missing after close")
		}
		return nil
	}); err != nil {
		t.Fatalf("raw.View: %v", err)
	}
}

func TestPersistedIndex_RemainsPresentUntilClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persisted_index_sequence.db")

	db, raw := openBoltAndNew[uint64, Rec](t, path)
	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("seed Close: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("seed raw close: %v", err)
	}

	db2, raw2 := openBoltAndNew[uint64, Rec](t, path)
	defer func() {
		_ = db2.Close()
		_ = raw2.Close()
	}()

	if _, err := os.Stat(db2.rbiFile); err != nil {
		t.Fatalf("expected persisted index before write, stat err=%v", err)
	}
	storedSeq := readPersistedIndexSequence(t, db2.rbiFile)
	if currentSeq := readBucketSequence(t, raw2, db2.bucket); currentSeq != storedSeq {
		t.Fatalf("expected persisted index sequence=%d before write, got bucket sequence=%d", storedSeq, currentSeq)
	}
	if err := db2.Set(2, &Rec{Name: "bob", Age: 31}); err != nil {
		t.Fatalf("Set after reopen: %v", err)
	}
	if _, err := os.Stat(db2.rbiFile); err != nil {
		t.Fatalf("expected persisted index to remain present after write, stat err=%v", err)
	}
	if currentSeq := readBucketSequence(t, raw2, db2.bucket); currentSeq <= storedSeq {
		t.Fatalf("expected bucket sequence to advance after write, before=%d after=%d", storedSeq, currentSeq)
	}
	if staleSeq := readPersistedIndexSequence(t, db2.rbiFile); staleSeq != storedSeq {
		t.Fatalf("expected persisted index sequence to stay stale until Close, before=%d after=%d", storedSeq, staleSeq)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("Close after write: %v", err)
	}
	if freshSeq := readPersistedIndexSequence(t, db2.rbiFile); freshSeq != readBucketSequence(t, raw2, db2.bucket) {
		t.Fatalf("expected Close to refresh persisted index sequence, got %d", freshSeq)
	}
}

func TestPersistedIndex_RebuildsAfterCloseFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "close_store_failure_rebuild.db")

	db, raw := openBoltAndNew[uint64, Rec](t, path)
	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("seed Close: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("seed raw close: %v", err)
	}

	db2, raw2 := openBoltAndNew[uint64, Rec](t, path)
	if err := db2.Set(2, &Rec{Name: "bob", Age: 40}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}
	db2.testHooks.beforeStoreIndex = func() error {
		return fmt.Errorf("failpoint: store index")
	}
	if err := db2.Close(); err == nil || !strings.Contains(err.Error(), "failpoint: store index") {
		t.Fatalf("expected failpoint store index error on Close, got: %v", err)
	}
	if _, err := os.Stat(db2.rbiFile); err != nil {
		t.Fatalf("expected stale persisted index file to remain after failed Close, stat err=%v", err)
	}
	staleSeq := readPersistedIndexSequence(t, db2.rbiFile)
	currentSeq := readBucketSequence(t, raw2, db2.bucket)
	if staleSeq == currentSeq {
		t.Fatalf("expected persisted index sequence to stay stale after failed Close, seq=%d", staleSeq)
	}
	if err := raw2.Close(); err != nil {
		t.Fatalf("raw2 close: %v", err)
	}

	db3, raw3 := openBoltAndNew[uint64, Rec](t, path)
	defer func() {
		_ = db3.Close()
		_ = raw3.Close()
	}()

	ids, err := db3.QueryKeys(qx.Query())
	if err != nil {
		t.Fatalf("QueryKeys after reopen: %v", err)
	}
	slices.Sort(ids)
	if !slices.Equal(ids, []uint64{1, 2}) {
		t.Fatalf("unexpected ids after rebuild-on-open: %v", ids)
	}
}

func TestPersistedIndex_NoOpDeleteKeepsSequence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persisted_index_noop_delete.db")

	db, raw := openBoltAndNew[uint64, Rec](t, path)
	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("seed Close: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("seed raw close: %v", err)
	}

	db2, raw2 := openBoltAndNew[uint64, Rec](t, path)
	defer func() {
		_ = db2.Close()
		_ = raw2.Close()
	}()

	storedSeq := readPersistedIndexSequence(t, db2.rbiFile)
	beforeSeq := readBucketSequence(t, raw2, db2.bucket)
	if storedSeq != beforeSeq {
		t.Fatalf("expected persisted index sequence=%d before no-op delete, got bucket sequence=%d", storedSeq, beforeSeq)
	}

	if err := db2.Delete(999); err != nil {
		t.Fatalf("Delete missing: %v", err)
	}

	afterStoredSeq := readPersistedIndexSequence(t, db2.rbiFile)
	afterSeq := readBucketSequence(t, raw2, db2.bucket)
	if afterSeq != beforeSeq {
		t.Fatalf("expected missing Delete to keep bucket sequence=%d, got %d", beforeSeq, afterSeq)
	}
	if afterStoredSeq != storedSeq {
		t.Fatalf("expected missing Delete to keep persisted index sequence=%d, got %d", storedSeq, afterStoredSeq)
	}
}

func TestLargeBatch_AtomicFailure(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	if err := db.Set(1, &UniqueTestRec{Email: "exists@x", Code: 1}); err != nil {
		t.Fatal(err)
	}

	ids := []uint64{2, 3}
	vals := []*UniqueTestRec{
		{Email: "new@x", Code: 2},
		{Email: "exists@x", Code: 3}, // must fail
	}

	err := db.BatchSet(ids, vals)
	if err == nil {
		t.Fatal("Expected error in BatchSet")
	}

	// verify atomicity: id 2 should not exist
	v, err := db.Get(2)
	if err != nil {
		t.Fatal(err)
	}
	if v != nil {
		t.Fatal("BatchSet was not atomic: ID 2 was inserted despite batch failure")
	}
}

func TestBatchPatch_MissingIDs_CallbacksOnlyForExisting(t *testing.T) {
	db, _ := openTempDBStringProduct(t)

	if err := db.Set("p1", &Product{SKU: "p1", Price: 10}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	var called []string
	cb := func(tx *bbolt.Tx, key string, oldValue, newValue *Product) error {
		called = append(called, key)
		return nil
	}

	err := db.BatchPatch([]string{"missing", "p1", "missing2"}, []Field{{Name: "price", Value: 20.0}}, BeforeCommit(cb))
	if err != nil {
		t.Fatalf("BatchPatch: %v", err)
	}
	if len(called) != 1 || called[0] != "p1" {
		t.Fatalf("expected callback for p1 only, got: %v", called)
	}

	v, err := db.Get("p1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if v == nil || v.Price != 20.0 {
		t.Fatalf("expected price 20.0, got: %#v", v)
	}
}

func TestBatchPatch_WithPatchStrict_ValidationError_IsAtomic(t *testing.T) {
	type tc struct {
		name  string
		patch []Field
	}

	cases := []tc{
		{
			name:  "unknown_field",
			patch: []Field{{Name: "does_not_exist", Value: 123}},
		},
		{
			name:  "type_mismatch",
			patch: []Field{{Name: "age", Value: "not-int"}},
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

			if err := db.Set(1, &Rec{Name: "n1", Age: 10, Meta: Meta{Country: "NL"}}); err != nil {
				t.Fatalf("Set(1): %v", err)
			}
			if err := db.Set(2, &Rec{Name: "n2", Age: 20, Meta: Meta{Country: "DE"}}); err != nil {
				t.Fatalf("Set(2): %v", err)
			}

			err := db.BatchPatch([]uint64{1, 2}, c.patch, PatchStrict)
			if err == nil {
				t.Fatalf("expected BatchPatch(..., PatchStrict) error, got nil")
			}

			v1, err := db.Get(1)
			if err != nil {
				t.Fatalf("Get(1): %v", err)
			}
			if v1 == nil || v1.Name != "n1" || v1.Age != 10 || v1.Country != "NL" {
				t.Fatalf("id=1 changed after failed BatchPatch(..., PatchStrict): %#v", v1)
			}

			v2, err := db.Get(2)
			if err != nil {
				t.Fatalf("Get(2): %v", err)
			}
			if v2 == nil || v2.Name != "n2" || v2.Age != 20 || v2.Country != "DE" {
				t.Fatalf("id=2 changed after failed BatchPatch(..., PatchStrict): %#v", v2)
			}

			ids, err := db.QueryKeys(qx.Query(qx.EQ("name", "n1")))
			if err != nil {
				t.Fatalf("QueryKeys(name=n1): %v", err)
			}
			if len(ids) != 1 || ids[0] != 1 {
				t.Fatalf("unexpected name index for n1 after failed BatchPatch(..., PatchStrict): %v", ids)
			}
		})
	}
}

func TestMissingIDs_DoNotGrowStrMap(t *testing.T) {
	db, _ := openTempDBStringProduct(t)

	if err := db.Set("p1", &Product{SKU: "p1", Price: 10}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	initial := len(db.strmap.Keys)

	if err := db.Patch("missing", []Field{{Name: "price", Value: 1.0}}); err != nil {
		t.Fatalf("Patch(missing): %v", err)
	}
	if err := db.Delete("missing"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := db.BatchDelete([]string{"missing2", "missing3"}); err != nil {
		t.Fatalf("BatchDelete: %v", err)
	}
	if err := db.BatchPatch([]string{"missing4"}, []Field{{Name: "price", Value: 2.0}}); err != nil {
		t.Fatalf("BatchPatch: %v", err)
	}

	after := len(db.strmap.Keys)

	if after != initial {
		t.Fatalf("expected strmap size %d, got %d", initial, after)
	}
}

func TestReadPaths_MissingKeys_DoNotGrowStrMap(t *testing.T) {
	db, _ := openTempDBStringProduct(t)

	if err := db.Set("p1", &Product{SKU: "p1", Price: 10, Tags: []string{"a"}}); err != nil {
		t.Fatalf("Set(p1): %v", err)
	}
	if err := db.Set("p2", &Product{SKU: "p2", Price: 20, Tags: []string{"b"}}); err != nil {
		t.Fatalf("Set(p2): %v", err)
	}

	initial := len(db.strmap.Keys)
	assertNoGrow := func(label string) {
		t.Helper()
		if after := len(db.strmap.Keys); after != initial {
			t.Fatalf("%s grew strmap: initial=%d after=%d", label, initial, after)
		}
	}

	if v, err := db.Get("missing-get"); err != nil {
		t.Fatalf("Get(missing): %v", err)
	} else if v != nil {
		t.Fatalf("Get(missing) expected nil, got %#v", v)
	}
	assertNoGrow("Get(missing)")

	values, err := db.BatchGet("missing-1", "p1", "missing-2")
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	if len(values) != 3 || values[0] != nil || values[1] == nil || values[2] != nil {
		t.Fatalf("unexpected BatchGet result: %#v", values)
	}
	assertNoGrow("BatchGet(missing)")

	if err := db.ScanKeys("zzzz", func(_ string) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("ScanKeys: %v", err)
	}
	assertNoGrow("ScanKeys(missing seek)")

	seenSeq := 0
	if err := db.SeqScan("zzzz", func(_ string, _ *Product) (bool, error) {
		seenSeq++
		return true, nil
	}); err != nil {
		t.Fatalf("SeqScan: %v", err)
	}
	if seenSeq != 0 {
		t.Fatalf("SeqScan expected 0 rows, got %d", seenSeq)
	}
	assertNoGrow("SeqScan(missing seek)")

	seenRaw := 0
	if err := db.SeqScanRaw("zzzz", func(_ string, _ []byte) (bool, error) {
		seenRaw++
		return true, nil
	}); err != nil {
		t.Fatalf("SeqScanRaw: %v", err)
	}
	if seenRaw != 0 {
		t.Fatalf("SeqScanRaw expected 0 rows, got %d", seenRaw)
	}
	assertNoGrow("SeqScanRaw(missing seek)")

	qMissing := qx.Query(qx.EQ("sku", "missing-sku"))
	if ids, err := db.QueryKeys(qMissing); err != nil {
		t.Fatalf("QueryKeys(missing sku): %v", err)
	} else if len(ids) != 0 {
		t.Fatalf("QueryKeys(missing sku) expected empty, got %v", ids)
	}
	assertNoGrow("QueryKeys(missing)")

	if items, err := db.Query(qMissing); err != nil {
		t.Fatalf("Query(missing sku): %v", err)
	} else if len(items) != 0 {
		t.Fatalf("Query(missing sku) expected empty, got %d", len(items))
	}
	assertNoGrow("Query(missing)")

	if cnt, err := db.Count(qMissing); err != nil {
		t.Fatalf("Count(missing sku): %v", err)
	} else if cnt != 0 {
		t.Fatalf("Count(missing sku) expected 0, got %d", cnt)
	}
	assertNoGrow("Count(missing)")
}

func TestFailedSetPaths_DoNotGrowStrMap(t *testing.T) {
	db, _ := openTempDBStringProduct(t)

	if err := db.Set("p1", &Product{SKU: "p1", Price: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	initial := len(db.strmap.Keys)
	cbErr := errors.New("before commit fail")
	cb := func(_ *bbolt.Tx, _ string, _ *Product, _ *Product) error { return cbErr }

	if err := db.Set("ghost-set", &Product{SKU: "ghost-set", Price: 11}, BeforeCommit(cb)); !errors.Is(err, cbErr) {
		t.Fatalf("Set callback error mismatch: %v", err)
	}
	if v, err := db.Get("ghost-set"); err != nil {
		t.Fatalf("Get(ghost-set): %v", err)
	} else if v != nil {
		t.Fatalf("ghost-set should not persist after rollback, got %#v", v)
	}
	if after := len(db.strmap.Keys); after != initial {
		t.Fatalf("strmap grew after failed Set: initial=%d after=%d", initial, after)
	}

	err := db.BatchSet(
		[]string{"ghost-many-1", "ghost-many-2"},
		[]*Product{
			{SKU: "ghost-many-1", Price: 12},
			{SKU: "ghost-many-2", Price: 13},
		},
		BeforeCommit(cb),
	)
	if !errors.Is(err, cbErr) {
		t.Fatalf("BatchSet callback error mismatch: %v", err)
	}
	if v, err := db.Get("ghost-many-1"); err != nil {
		t.Fatalf("Get(ghost-many-1): %v", err)
	} else if v != nil {
		t.Fatalf("ghost-many-1 should not persist after rollback, got %#v", v)
	}
	if v, err := db.Get("ghost-many-2"); err != nil {
		t.Fatalf("Get(ghost-many-2): %v", err)
	} else if v != nil {
		t.Fatalf("ghost-many-2 should not persist after rollback, got %#v", v)
	}
	if after := len(db.strmap.Keys); after != initial {
		t.Fatalf("strmap grew after failed BatchSet: initial=%d after=%d", initial, after)
	}
}

type StringUniqueTestRec struct {
	Email string `db:"email" rbi:"unique"`
	Code  int    `db:"code"  rbi:"unique"`
}

func openTempDBStringUnique(t *testing.T, options ...Options) (*DB[string, StringUniqueTestRec], string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_string_unique.db")
	db, raw := openBoltAndNew[string, StringUniqueTestRec](t, path, options...)
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	return db, path
}

func TestBatchSet_CallbackError_DoesNotGrowStrMap(t *testing.T) {
	db, _ := openTempDBStringProduct(t)

	if err := db.Set("p1", &Product{SKU: "p1", Price: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	initial := len(db.strmap.Keys)

	cbErr := errors.New("cb fail")
	err := db.Set("ghost-cb", &Product{SKU: "ghost-cb", Price: 11}, BeforeCommit(func(_ *bbolt.Tx, _ string, _ *Product, _ *Product) error {
		return cbErr
	}))
	if !errors.Is(err, cbErr) {
		t.Fatalf("Set callback error mismatch: %v", err)
	}
	if v, err := db.Get("ghost-cb"); err != nil {
		t.Fatalf("Get(ghost-cb): %v", err)
	} else if v != nil {
		t.Fatalf("ghost-cb should not persist after rollback, got %#v", v)
	}
	if after := len(db.strmap.Keys); after != initial {
		t.Fatalf("strmap grew after batch callback rollback: initial=%d after=%d", initial, after)
	}

	bs := db.AutoBatchStats()
	if bs.CallbackOps == 0 || bs.CallbackErrors == 0 {
		t.Fatalf("expected callback error via batch path, stats=%+v", bs)
	}
}

func TestBatchSet_UniqueReject_DoesNotGrowStrMap(t *testing.T) {
	db, _ := openTempDBStringUnique(t)

	if err := db.Set("u1", &StringUniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	initial := len(db.strmap.Keys)

	err := db.Set("u-dup", &StringUniqueTestRec{Email: "a@x", Code: 2})
	if err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}
	if v, err := db.Get("u-dup"); err != nil {
		t.Fatalf("Get(u-dup): %v", err)
	} else if v != nil {
		t.Fatalf("u-dup should not persist after unique reject, got %#v", v)
	}
	if after := len(db.strmap.Keys); after != initial {
		t.Fatalf("strmap grew after batch unique reject: initial=%d after=%d", initial, after)
	}

	bs := db.AutoBatchStats()
	if bs.UniqueRejected == 0 {
		t.Fatalf("expected unique rejection in batch stats, got %+v", bs)
	}
}

/**/

func TestComponentAccessors_ExposePlannerCalibrationAndSnapshotDiagnostics(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		CalibrationEnabled:     true,
		CalibrationSampleEvery: 1,
		AnalyzeInterval:        -1,
	})

	if err := db.Set(1, &Rec{Name: "alice", Age: 10, Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "bob", Age: 20, Tags: []string{"db"}}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	db.observeCalibration(TraceEvent{
		Plan:          string(PlanOrdered),
		EstimatedRows: 64,
		RowsExamined:  96,
	})

	st := db.Stats()
	if st.KeyCount != 2 {
		t.Fatalf("expected Stats.KeyCount=2, got %d", st.KeyCount)
	}
	if st.LastKey != 2 {
		t.Fatalf("expected Stats.LastKey=2, got %d", st.LastKey)
	}
	if st.FieldCount == 0 {
		t.Fatalf("expected Stats.FieldCount > 0")
	}
	if st.AutoBatchCount == 0 {
		t.Fatalf("expected stats auto-batch count > 0")
	}
	if st.SnapshotSequence == 0 {
		t.Fatalf("expected stats snapshot sequence > 0")
	}

	idx := db.IndexStats()
	if idx.EntryCount == 0 {
		t.Fatalf("expected index diagnostics entry_count > 0")
	}
	if idx.ApproxHeapBytes < idx.Size {
		t.Fatalf("expected approx heap bytes >= index size, got approx=%d index=%d", idx.ApproxHeapBytes, idx.Size)
	}

	snap := db.SnapshotStats()
	if snap.Sequence == 0 {
		t.Fatalf("expected snapshot sequence > 0")
	}
	if snap.RegistrySize == 0 {
		t.Fatalf("expected snapshot registry to be non-empty")
	}
	if snap.UniverseCard == 0 {
		t.Fatalf("expected snapshot universe cardinality > 0")
	}

	pl := db.PlannerStats()
	if pl.Version == 0 {
		t.Fatalf("expected planner stats version > 0")
	}
	if pl.GeneratedAt.IsZero() {
		t.Fatalf("expected planner generated_at to be set")
	}
	if pl.FieldCount == 0 {
		t.Fatalf("expected planner field_count > 0")
	}
	if pl.AnalyzeInterval != 0 {
		t.Fatalf("expected disabled analyze interval (0), got %v", pl.AnalyzeInterval)
	}

	cal := db.CalibrationStats()
	if !cal.Enabled {
		t.Fatalf("expected calibration to be enabled in stats")
	}
	if cal.SampleEvery != 1 {
		t.Fatalf("expected calibration sample_every=1, got %d", cal.SampleEvery)
	}
	if cal.UpdatedAt.IsZero() {
		t.Fatalf("expected calibration updated_at to be set")
	}
	if cal.SamplesTotal == 0 {
		t.Fatalf("expected calibration samples_total > 0")
	}
	if cal.Samples[string(PlanOrdered)] == 0 {
		t.Fatalf("expected calibration samples for %q > 0", PlanOrdered)
	}
	if _, ok := cal.Multipliers[string(PlanOrdered)]; !ok {
		t.Fatalf("expected calibration multiplier for %q to be present", PlanOrdered)
	}

	bs := db.AutoBatchStats()
	if bs.Window <= 0 {
		t.Fatalf("expected positive auto-batch window, got %v", bs.Window)
	}
	if bs.Enqueued == 0 {
		t.Fatalf("expected auto-batch stats to observe enqueued writes")
	}
}

func TestStats_PreservesIndexTimingFields(t *testing.T) {
	db, _ := openTempDBUint64(t)

	db.stats.BuildTime = 123 * time.Millisecond
	db.stats.BuildRPS = 456
	db.stats.LoadTime = 789 * time.Millisecond

	st := db.Stats()

	if st.BuildTime != db.stats.BuildTime {
		t.Fatalf("expected BuildTime=%v, got %v", db.stats.BuildTime, st.BuildTime)
	}
	if st.BuildRPS != db.stats.BuildRPS {
		t.Fatalf("expected BuildRPS=%d, got %d", db.stats.BuildRPS, st.BuildRPS)
	}
	if st.LoadTime != db.stats.LoadTime {
		t.Fatalf("expected LoadTime=%v, got %v", db.stats.LoadTime, st.LoadTime)
	}
}

func TestComponentAccessors(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		CalibrationEnabled:     true,
		CalibrationSampleEvery: 1,
		AnalyzeInterval:        -1,
	})

	if err := db.Set(1, &Rec{Name: "alice", Age: 10, Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "bob", Age: 20, Tags: []string{"db"}}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	db.observeCalibration(TraceEvent{
		Plan:          string(PlanOrdered),
		EstimatedRows: 64,
		RowsExamined:  96,
	})

	st := db.Stats()
	if st.KeyCount != 2 {
		t.Fatalf("expected Stats.KeyCount=2, got %d", st.KeyCount)
	}
	if st.SnapshotSequence == 0 {
		t.Fatalf("expected Stats.SnapshotSequence > 0")
	}

	idx := db.IndexStats()
	if idx.Size == 0 {
		t.Fatalf("expected IndexStats.Size > 0")
	}
	if len(idx.UniqueFieldKeys) == 0 {
		t.Fatalf("expected IndexStats.UniqueFieldKeys to be populated")
	}

	pl := db.PlannerStats()
	if pl.Version == 0 {
		t.Fatalf("expected PlannerStats.Version > 0")
	}
	if pl.GeneratedAt.IsZero() {
		t.Fatalf("expected PlannerStats.GeneratedAt to be set")
	}

	cal := db.CalibrationStats()
	if !cal.Enabled {
		t.Fatalf("expected CalibrationStats.Enabled=true")
	}
	if cal.SamplesTotal == 0 {
		t.Fatalf("expected CalibrationStats.SamplesTotal > 0")
	}

	snap := db.SnapshotStats()
	if snap.Sequence == 0 {
		t.Fatalf("expected SnapshotStats.Sequence > 0")
	}

	bs := db.AutoBatchStats()
	if bs.Window <= 0 {
		t.Fatalf("expected AutoBatchStats.Window > 0")
	}
}
