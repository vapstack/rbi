package rbi

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/persist"
	"github.com/vapstack/rbi/rbierrors"
	"go.etcd.io/bbolt"
)

type invalidUniqueSliceRec struct {
	Tags []string `rbi:"unique"`
}

type startupAbortMetaBucketBreaker struct {
	root       *rootStore
	bucket     string
	collection *collection
	tripped    bool
}

func (w *startupAbortMetaBucketBreaker) Write(p []byte) (int, error) {
	if !w.tripped && bytes.Contains(p, []byte("index build completed")) {
		w.root.mu.Lock()
		w.collection = w.root.collections[w.bucket]
		w.root.mu.Unlock()
		w.root.metaBucket = []byte(".rbi_missing_startup_abort_meta")
		w.tripped = true
	}
	return len(p), nil
}

func TestMultiWrap_DifferentStructs(t *testing.T) {
	bolt, path := openRawBolt(t)

	defer func() {
		if err := bolt.Close(); err != nil {
			t.Fatalf("raw bolt close: %v", err)
		}
	}()

	recDB, err := Open[uint64, Rec](bolt, testOptions(Options{}))
	if err != nil {
		t.Fatalf("Wrap 1 (Rec): %v", err)
	}

	productDB, err := Open[string, Product](bolt, testOptions(Options{}))
	if err != nil {
		t.Fatalf("Wrap 2 (Product): %v", err)
	}

	if err = writeSet(recDB, 1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatal(err)
	}

	if err = writeSet(productDB, "p1", &Product{SKU: "p1", Price: 100.0}); err != nil {
		t.Fatal(err)
	}

	ids, err := readQueryKeys(recDB, qx.Query(qx.EQ("age", 30)))
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 {
		t.Errorf("expected 1 record, got: %v", len(ids))
	}

	sids, err := readQueryKeys(productDB, qx.Query(qx.EQ("price", 100.0)))
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
	bolt, _ := openRawBolt(t)
	defer func() { _ = bolt.Close() }()

	const bucket = "registry_cleanup_check"

	_, err := Open[uint64, invalidUniqueSliceRec](bolt, testOptions(Options{BucketName: bucket}))
	if err == nil {
		t.Fatalf("expected Wrap failure for invalidUniqueSliceRec")
	}

	c, err := Open[uint64, Rec](bolt, testOptions(Options{BucketName: bucket}))
	if err != nil {
		t.Fatalf("Wrap after failed Wrap must succeed, got: %v", err)
	}
	defer func() { _ = c.Close() }()
}

func TestWrap_BuildIndexError_DoesNotCloseCallerBolt(t *testing.T) {
	bolt, _ := openRawBolt(t)
	defer func() { _ = bolt.Close() }()

	const bucket = "broken_wrap_bucket"
	if err := bolt.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		// Invalid codec payload to force buildIndex decode error in Wrap.
		var key [8]byte
		return b.Put(keycodec.U64BytesWithBuf(1, &key), []byte{0xc1})
	}); err != nil {
		t.Fatalf("seed invalid payload: %v", err)
	}

	_, err := Open[uint64, Rec](bolt, testOptions(Options{BucketName: bucket}))
	if err == nil {
		t.Fatalf("expected Wrap failure on malformed payload")
	}

	if err = bolt.View(func(tx *bbolt.Tx) error {
		if tx.Bucket([]byte(bucket)) == nil {
			return fmt.Errorf("bucket is missing")
		}
		return nil
	}); err != nil {
		t.Fatalf("raw bbolt must stay open after Wrap error, got: %v", err)
	}
}

func TestWrap_BrokenRootRejectsNewCollections(t *testing.T) {
	bolt, _ := openRawBolt(t)
	defer func() { _ = bolt.Close() }()

	c, err := Open[uint64, Rec](bolt, testOptions(Options{
		BucketName:        "broken_root_source",
		DisableIndexStore: true,
	}))
	if err != nil {
		t.Fatalf("New source: %v", err)
	}
	root := c.root
	root.markBroken("test", errors.New("boom"))
	before := rootOpenCollectionCount(root)

	got, err := Open[string, Product](bolt, testOptions(Options{
		BucketName:        "broken_root_rejected",
		DisableIndexStore: true,
	}))
	if !errors.Is(err, rbierrors.ErrBroken) {
		t.Fatalf("New on broken root err=%v, want ErrBroken", err)
	}
	if got != nil {
		t.Fatalf("New on broken root returned db=%v", got)
	}
	if after := rootOpenCollectionCount(root); after != before {
		t.Fatalf("broken root collection count=%d want %d", after, before)
	}

	if err := c.Close(); !errors.Is(err, rbierrors.ErrBroken) {
		t.Fatalf("Close broken source err=%v, want ErrBroken", err)
	}
	if rootOpenForBolt(bolt) {
		t.Fatal("root remained registered after closing broken source")
	}
}

func TestWrap_TransparentOpenRejectsRootBrokenAfterReserve(t *testing.T) {
	bolt, _ := openRawBolt(t)
	defer func() { _ = bolt.Close() }()

	c, err := Open[uint64, Rec](bolt, testOptions(Options{
		BucketName:        "broken_after_reserve_source",
		DisableIndexStore: true,
	}))
	if err != nil {
		t.Fatalf("New source: %v", err)
	}
	root := c.root

	tx, err := bolt.Begin(true)
	if err != nil {
		t.Fatalf("begin blocking write tx: %v", err)
	}
	defer rollback(tx)

	type openResult struct {
		clc *Collection[uint64, noIndexRec]
		err error
	}
	done := make(chan openResult, 1)
	go func() {
		next, e := Open[uint64, noIndexRec](bolt, testOptions(Options{
			BucketName: "broken_after_reserve_transparent",
		}))
		done <- openResult{clc: next, err: e}
	}()

	deadline := time.Now().Add(2 * time.Second)
	for rootOpenCollectionCount(root) != 2 {
		if time.Now().After(deadline) {
			t.Fatal("transparent New did not reserve collection before blocking write tx")
		}
		time.Sleep(time.Millisecond)
	}

	root.markBroken("test", errors.New("boom"))
	if err = tx.Rollback(); err != nil {
		t.Fatalf("rollback blocking write tx: %v", err)
	}

	select {
	case result := <-done:
		if !errors.Is(result.err, rbierrors.ErrBroken) {
			t.Fatalf("transparent New err=%v, want ErrBroken", result.err)
		}
		if result.clc != nil {
			t.Fatalf("transparent New on broken root returned db=%v", result.clc)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("transparent New did not finish after blocking write tx rollback")
	}

	if after := rootOpenCollectionCount(root); after != 1 {
		t.Fatalf("broken root collection count=%d want 1", after)
	}
	if err = c.Close(); !errors.Is(err, rbierrors.ErrBroken) {
		t.Fatalf("Close broken source err=%v, want ErrBroken", err)
	}
	if rootOpenForBolt(bolt) {
		t.Fatal("root remained registered after closing broken source")
	}
}

func TestWrap_ReleasesInitialSnapshotWhenRegistrationFailsBeforeReadState(t *testing.T) {
	bolt, _ := openRawBolt(t)
	defer func() { _ = bolt.Close() }()

	c, err := Open[uint64, Rec](bolt, testOptions(Options{
		BucketName:        "startup_abort_source",
		DisableIndexStore: true,
	}))
	if err != nil {
		t.Fatalf("New source: %v", err)
	}
	root := c.root
	metaBucket := root.metaBucket
	defer func() {
		root.metaBucket = metaBucket
	}()

	breaker := &startupAbortMetaBucketBreaker{
		root:   root,
		bucket: "startup_abort_failed",
	}
	got, err := Open[uint64, Rec](bolt, testOptions(Options{
		BucketName:        breaker.bucket,
		DisableIndexStore: true,
		Logger:            log.New(breaker, "", 0),
	}))
	root.metaBucket = metaBucket
	if err == nil {
		if got != nil {
			_ = got.Close()
		}
		t.Fatal("New succeeded after root metadata bucket was hidden")
	}
	if got != nil {
		t.Fatalf("failed New returned db=%v", got)
	}
	if !breaker.tripped {
		t.Fatal("test did not reach post-build registration failure path")
	}
	if breaker.collection == nil || breaker.collection.index == nil {
		t.Fatalf("failed collection was not captured: %+v", breaker.collection)
	}
	if snap := breaker.collection.index.CurrentSnapshot(); snap != nil {
		t.Fatalf("failed startup retained unpublished current snapshot seq=%d", snap.Seq)
	}
	if after := rootOpenCollectionCount(root); after != 1 {
		t.Fatalf("collection count after failed New=%d want 1", after)
	}

	if err = c.Close(); err != nil {
		t.Fatalf("Close source: %v", err)
	}
}

func TestRootMetadataCollectionUID(t *testing.T) {
	bolt, _ := openRawBolt(t)
	defer func() { _ = bolt.Close() }()

	const bucket = "root_meta_uid"
	c, err := Open[uint64, Rec](bolt, testOptions(Options{
		BucketName:        bucket,
		DisableIndexStore: true,
	}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	uid := c.rbiUID

	seq := assertRootMetadataUID(t, bolt, []byte(bucket), uid)
	if seq != 1 {
		t.Fatalf("root metadata sequence=%d want 1", seq)
	}
	if err = writeSet(c, 1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	seq = assertRootMetadataUID(t, bolt, []byte(bucket), uid)
	if seq != 2 {
		t.Fatalf("root metadata sequence after write=%d want 2", seq)
	}
	if err = c.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	seq = assertRootMetadataUID(t, bolt, []byte(bucket), uid)
	if seq != 3 {
		t.Fatalf("root metadata sequence after truncate=%d want 3", seq)
	}
	if err = c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if rootOpenForBolt(bolt) {
		t.Fatal("root remained registered after closing last collection")
	}

	c, err = Open[uint64, Rec](bolt, testOptions(Options{
		BucketName:        bucket,
		DisableIndexStore: true,
	}))
	if err != nil {
		t.Fatalf("reopen New: %v", err)
	}
	if c.rbiUID != uid {
		t.Fatalf("collection uid changed on reopen: got %x want %x", c.rbiUID, uid)
	}
	seq = assertRootMetadataUID(t, bolt, []byte(bucket), uid)
	if seq != 4 {
		t.Fatalf("root metadata sequence after reopen=%d want 4", seq)
	}
	if err = c.Close(); err != nil {
		t.Fatalf("reopen Close: %v", err)
	}
	if rootOpenForBolt(bolt) {
		t.Fatal("root remained registered after closing reopened collection")
	}
}

func TestCloseWithPinnedReadReapsRootAfterTxClose(t *testing.T) {
	bolt, _ := openRawBolt(t)
	c, err := Open[uint64, Rec](bolt, testOptions(Options{
		BucketName:        "root_reap_pinned_read",
		DisableIndexStore: true,
	}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	tx := BeginView()
	defer func() {
		tx.Close()
		_ = c.Close()
		_ = bolt.Close()
	}()

	if _, err = c.Get(tx, 1); err != nil {
		t.Fatalf("bind read tx: %v", err)
	}
	root := c.root
	if err = c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !rootOpenForBolt(bolt) {
		t.Fatal("root reaped while read tx was still pinned")
	}
	if !root.reapPending.Load() {
		t.Fatal("root did not record pending reap after close with pinned read tx")
	}
	tx.Close()
	if rootOpenForBolt(bolt) {
		t.Fatal("root remained registered after pinned read tx closed")
	}
	if root.reapPending.Load() {
		t.Fatal("root kept pending reap flag after successful reap")
	}
}

func TestCloseWithPinnedIndexReapsRootAfterTxClose(t *testing.T) {
	bolt, _ := openRawBolt(t)
	c, err := Open[uint64, Rec](bolt, testOptions(Options{
		BucketName:        "root_reap_pinned_index",
		DisableIndexStore: true,
	}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	tx := BeginIndexView()
	defer func() {
		tx.Close()
		_ = c.Close()
		_ = bolt.Close()
	}()

	if _, err = c.Count(tx); err != nil {
		t.Fatalf("bind index tx: %v", err)
	}
	root := c.root
	if err = c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !rootOpenForBolt(bolt) {
		t.Fatal("root reaped while index tx was still pinned")
	}
	if !root.reapPending.Load() {
		t.Fatal("root did not record pending reap after close with pinned index tx")
	}
	tx.Close()
	if rootOpenForBolt(bolt) {
		t.Fatal("root remained registered after pinned index tx closed")
	}
	if root.reapPending.Load() {
		t.Fatal("root kept pending reap flag after successful reap")
	}
}

func TestReopenWithPinnedReadClearsRootReapPending(t *testing.T) {
	bolt, _ := openRawBolt(t)
	c, err := Open[uint64, Rec](bolt, testOptions(Options{
		BucketName:        "root_reopen_pinned_read",
		DisableIndexStore: true,
	}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	tx := BeginView()
	var reopened *Collection[uint64, Rec]
	defer func() {
		tx.Close()
		if reopened != nil {
			_ = reopened.Close()
		}
		_ = c.Close()
		_ = bolt.Close()
	}()

	type reopenResult struct {
		clc *Collection[uint64, Rec]
		err error
	}
	reopenDone := make(chan reopenResult, 1)

	if _, err = c.Get(tx, 1); err != nil {
		t.Fatalf("bind read tx: %v", err)
	}
	root := c.root
	ordinal := c.ordinal
	if err = c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !root.reapPending.Load() {
		t.Fatal("root did not record pending reap after close with pinned read tx")
	}

	go func() {
		next, e := Open[uint64, Rec](bolt, testOptions(Options{
			BucketName:        "root_reopen_pinned_read",
			DisableIndexStore: true,
		}))
		reopenDone <- reopenResult{clc: next, err: e}
	}()

	deadline := time.Now().Add(2 * time.Second)
	for root.reapPending.Load() || rootOpenCollectionCount(root) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("reopen did not reserve collection while old read tx was pinned")
		}
		time.Sleep(time.Millisecond)
	}

	tx.Close()

	select {
	case result := <-reopenDone:
		reopened, err = result.clc, result.err
	case <-time.After(2 * time.Second):
		t.Fatal("reopen did not finish after old read tx closed")
	}
	if err != nil {
		t.Fatalf("reopen New: %v", err)
	}
	if reopened.root != root {
		t.Fatal("reopen did not reuse pinned root")
	}
	if reopened.ordinal == ordinal {
		t.Fatalf("reopen reused old collection ordinal %d", ordinal)
	}
	if root.reapPending.Load() {
		t.Fatal("reopen left root reap pending while collection is active")
	}

	if !rootOpenForBolt(bolt) {
		t.Fatal("old read tx close reaped active reopened root")
	}
	if err = reopened.Close(); err != nil {
		t.Fatalf("reopened Close: %v", err)
	}
	if rootOpenForBolt(bolt) {
		t.Fatal("root remained registered after reopened collection closed")
	}
}

func TestRootGenerationCarriesIndexedSnapshots(t *testing.T) {
	bolt, _ := openRawBolt(t)
	defer func() { _ = bolt.Close() }()

	c, err := Open[uint64, Rec](bolt, testOptions(Options{
		BucketName:        "root_indexed_snapshots",
		DisableIndexStore: true,
	}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	assertRootCollectionSnapshotSeq(t, c, 0)

	if err = writeSet(c, 1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	assertRootCollectionSnapshotSeq(t, c, 1)

	if err = c.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	assertRootCollectionSnapshotSeq(t, c, 2)

	if err = c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func assertRootCollectionSnapshotSeq[K ~string | ~uint64, V any](t *testing.T, c *Collection[K, V], seq uint64) {
	t.Helper()

	gen, epoch, pin := c.root.registry.pinCurrent()
	if gen == nil || pin.ref == nil {
		t.Fatalf("PinCurrent returned gen=%v pin=%v", gen, pin)
	}
	defer c.root.registry.unpin(epoch, pin)

	entry := gen.entries[c.ordinal]
	if entry.collection != c.collection {
		t.Fatalf("root entry collection=%p want %p", entry.collection, c.collection)
	}
	if entry.read == nil || entry.read.snap == nil {
		t.Fatalf("root entry missing indexed snapshot: %+v", entry)
	}
	if entry.read.dataSeq != seq || entry.read.snap.Seq != seq {
		t.Fatalf("root entry seq data=%d snap=%d want %d", entry.read.dataSeq, entry.read.snap.Seq, seq)
	}
}

func assertRootMetadataUID(t *testing.T, bolt *bbolt.DB, bucket []byte, uid [persist.UIDLen]byte) uint64 {
	t.Helper()

	var seq uint64
	if err := bolt.View(func(tx *bbolt.Tx) error {
		if oldMeta := tx.Bucket(append(append([]byte(nil), bucket...), ".rbidata"...)); oldMeta != nil {
			return fmt.Errorf("old collection metadata bucket still exists")
		}
		meta := tx.Bucket(rootMetadataBucketName)
		if meta == nil {
			return fmt.Errorf("root metadata bucket does not exist")
		}
		seq = meta.Sequence()
		got := meta.Get(rootCollectionUIDKey(bucket))
		if got == nil {
			return fmt.Errorf("collection uid record missing")
		}
		if !bytes.Equal(got, uid[:]) {
			return fmt.Errorf("collection uid=%x want %x", got, uid)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return seq
}

func rootOpenForBolt(bolt *bbolt.DB) bool {
	rootRegistryMu.Lock()
	_, ok := rootsByBolt[bolt]
	rootRegistryMu.Unlock()
	return ok
}

func rootOpenCollectionCount(r *rootStore) int {
	r.mu.Lock()
	open := len(r.collections)
	r.mu.Unlock()
	return open
}

func TestWrap_InvalidBucketName_RejectsUnsafeCharacters(t *testing.T) {
	bolt, _ := openRawBolt(t)
	defer func() { _ = bolt.Close() }()

	for _, bucket := range []string{
		"bad-name",
		"bad.name",
		"bad name",
		"1bucket",
	} {
		_, err := Open[uint64, Rec](bolt, testOptions(Options{BucketName: bucket}))
		if !errors.Is(err, rbierrors.ErrInvalidBucketName) {
			t.Fatalf("bucket=%q: expected rbierrors.ErrInvalidBucketName, got=%v", bucket, err)
		}
	}

	c, err := Open[uint64, Rec](bolt, testOptions(Options{BucketName: "bucket_name_ok"}))
	if err != nil {
		t.Fatalf("expected valid bucket name to succeed, got: %v", err)
	}
	defer func() { _ = c.Close() }()
}

func TestMultiWrap_SameStruct_DifferentBuckets(t *testing.T) {
	bolt, _ := openRawBolt(t)
	defer func() {
		if err := bolt.Close(); err != nil {
			t.Fatalf("raw db close: %v", err)
		}
	}()

	dbUS, err := Open[uint64, Rec](bolt, testOptions(Options{BucketName: "users_us"}))
	if err != nil {
		t.Fatal(err)
	}

	dbEU, err := Open[uint64, Rec](bolt, testOptions(Options{BucketName: "users_eu"}))
	if err != nil {
		t.Fatal(err)
	}

	if err = writeSet(dbUS, 1, &Rec{Name: "john", Meta: Meta{Country: "US"}}); err != nil {
		t.Fatal(err)
	}

	// same id, another bucket
	if err = writeSet(dbEU, 1, &Rec{Name: "hans", Meta: Meta{Country: "DE"}}); err != nil {
		t.Fatal(err)
	}

	ids, err := readQueryKeys(dbUS, qx.Query(qx.EQ("name", "john")))
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 item, got: %v", len(ids))
	}

	// must not contain
	ids, err = readQueryKeys(dbUS, qx.Query(qx.EQ("name", "hans")))
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 0 {
		t.Fatal("bucket leaked")
	}

	ids, err = readQueryKeys(dbEU, qx.Query(qx.EQ("name", "hans")))
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 item, got: %v", len(ids))
	}
}

func TestMultiWrap_ConcurrentWrites(t *testing.T) {
	bolt, _ := openRawBolt(t)
	defer func() {
		if err := bolt.Close(); err != nil {
			t.Logf("raw db close error: %v", err)
		}
	}()

	db1, err := Open[uint64, Rec](bolt, testOptions(Options{BucketName: "b1"}))
	if err != nil {
		t.Fatal(err)
	}
	db2, err := Open[uint64, Rec](bolt, testOptions(Options{BucketName: "b2"}))
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
			if err := writeSet(db1, uint64(i), &Rec{Name: "w1", Age: 10}); err != nil {
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
			if err := writeSet(db2, uint64(i), &Rec{Name: "w2", Age: 20}); err != nil {
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

	c1, err := readCount(db1)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := readCount(db2)
	if err != nil {
		t.Fatal(err)
	}

	if int(c1) != count {
		t.Errorf("db1 count mismatch: got %d, expected %d", c1, count)
	}
	if int(c2) != count {
		t.Errorf("db2 count mismatch: got %d, expected %d", c2, count)
	}

	idsLeak, err := readQueryKeys(db1, qx.Query(qx.EQ("age", 20)))
	if err != nil {
		t.Fatal(err)
	}
	if len(idsLeak) > 0 {
		t.Fatalf("isolation failed: db1 contains %d records from db2 (Age=20)", len(idsLeak))
	}

	idsCorrect, err := readQueryKeys(db1, qx.Query(qx.EQ("age", 10)))
	if err != nil {
		t.Fatal(err)
	}
	if len(idsCorrect) != count {
		t.Errorf("db1 integrity failure: found %d records with Age=10, expected %d", len(idsCorrect), count)
	}
}

func TestMultiWrap_ReopenPersistence(t *testing.T) {
	bolt, path := openRawBolt(t)

	opts1 := testOptions(Options{BucketName: "t1"})
	opts2 := testOptions(Options{BucketName: "t2"})

	c1, _ := Open[uint64, Rec](bolt, opts1)
	c2, _ := Open[uint64, Rec](bolt, opts2)

	if err := writeSet(c1, 1, &Rec{Name: "one"}); err != nil {
		t.Fatal(err)
	}
	if err := writeSet(c2, 2, &Rec{Name: "two"}); err != nil {
		t.Fatal(err)
	}
	if err := c1.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c2.Close(); err != nil {
		t.Fatal(err)
	}
	if err := bolt.Close(); err != nil {
		t.Fatal(err)
	}

	bolt2, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := bolt2.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	db1New, err := Open[uint64, Rec](bolt2, opts1)
	if err != nil {
		t.Fatal(err)
	}

	db2New, err := Open[uint64, Rec](bolt2, opts2)
	if err != nil {
		t.Fatal(err)
	}

	ids1, err := readQueryKeys(db1New, qx.Query(qx.EQ("name", "one")))
	if err != nil {
		t.Fatal(err)
	}
	if len(ids1) != 1 {
		t.Error("t1 persistence failed")
	}

	ids2, err := readQueryKeys(db2New, qx.Query(qx.EQ("name", "two")))
	if err != nil {
		t.Fatal(err)
	}
	if len(ids2) != 1 {
		t.Error("t2 persistence failed")
	}

	idsCross, err := readQueryKeys(db1New, qx.Query(qx.EQ("name", "two")))
	if err != nil {
		t.Fatal(err)
	}
	if len(idsCross) != 0 {
		t.Error("cross contamination after reload")
	}
}

func TestMultiWrap_CloseBehavior(t *testing.T) {
	bolt, _ := openRawBolt(t)
	// Wrap does not transfer ownership of bolt to wrapped instances.

	db1, err := Open[uint64, Rec](bolt, testOptions(Options{BucketName: "b1"}))
	if err != nil {
		t.Fatalf("Wrap 1: %v", err)
	}

	db2, err := Open[uint64, Rec](bolt, testOptions(Options{BucketName: "b2"}))
	if err != nil {
		t.Fatalf("Wrap 2: %v", err)
	}

	if err = writeSet(db1, 1, &Rec{}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err = writeSet(db2, 1, &Rec{}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err = db1.Close(); err != nil {
		t.Fatalf("db1 close: %v", err)
	}

	// db2 must work
	if err = writeSet(db2, 2, &Rec{}); err != nil {
		t.Fatalf("db2 failed after db1 closed: %v", err)
	}

	// db1 must not
	if err = writeSet(db1, 3, &Rec{}); !errors.Is(err, rbierrors.ErrClosed) {
		t.Fatalf("db1 should be closed, got: %v", err)
	}

	if err = db2.Close(); err != nil {
		t.Fatalf("db2 close: %v", err)
	}
	if err = bolt.Close(); err != nil {
		t.Fatalf("bolt close: %v", err)
	}
}

func TestWrap_DoubleOpenCheck(t *testing.T) {
	bolt, _ := openRawBolt(t)
	defer func() {
		if err := bolt.Close(); err != nil {
			t.Fatalf("raw db close: %v", err)
		}
	}()

	db1, err := Open[uint64, Rec](bolt, testOptions(Options{BucketName: "users"}))
	if err != nil {
		t.Fatalf("first open failed: %v", err)
	}

	_, err = Open[uint64, Rec](bolt, testOptions(Options{BucketName: "users"}))
	if err == nil {
		t.Fatal("expected error on double open, got nil")
	}

	db2, err := Open[uint64, Rec](bolt, testOptions(Options{BucketName: "admins"}))
	if err != nil {
		t.Fatalf("different bucket open failed: %v", err)
	}

	if err = db1.Close(); err != nil {
		t.Fatalf("db1 close: %v", err)
	}

	db1Reopen, err := Open[uint64, Rec](bolt, testOptions(Options{BucketName: "users"}))
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
	c, _ := openTempUint64CollectionUnique(t)

	if err := writeSet(c, 1, &UniqueTestRec{Email: "a@x", Code: 1, Tags: []string{"t1"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := writeSet(c, 2, &UniqueTestRec{Email: "b@x", Code: 2, Tags: []string{"t2"}}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	if cnt, err := readCount(c); err != nil || cnt != 2 {
		t.Fatalf("Count(before): cnt=%d err=%v", cnt, err)
	}
	if ids, err := readQueryKeys(c, qx.Query(qx.EQ("email", "a@x"))); err != nil || len(ids) != 1 || ids[0] != 1 {
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
			_, _ = readGet(c, 1)
			_, _ = readValues(c, 1, 2, 999)

			_ = readSeqScan(c, 0, func(_ uint64, _ *UniqueTestRec) (bool, error) { return true, nil })
			_ = scanRawBolt(t, c, 0, func(_ uint64, _ []byte) (bool, error) { return true, nil })

			_, _ = readQueryKeys(c, qx.Query())
			_, _ = readCount(c)
		}
	}

	wg.Add(2)
	go reader("r1")
	go reader("r2")

	if err := c.Truncate(); err != nil {
		close(done)
		wg.Wait()
		t.Fatalf("Truncate #1: %v", err)
	}
	if err := c.Truncate(); err != nil {
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

	if v, err := readGet(c, 1); err != nil {
		t.Fatalf("Get(after): %v", err)
	} else if v != nil {
		t.Fatalf("expected Get(1)==nil after truncate, got %#v", v)
	}
	if vals, err := readValues(c, 1, 2); err != nil {
		t.Fatalf("readValues(after): %v", err)
	} else if len(vals) != 2 || vals[0] != nil || vals[1] != nil {
		t.Fatalf("expected readValues to return [nil nil], got %#v", vals)
	}

	if cnt, err := readCount(c); err != nil {
		t.Fatalf("Count(after): %v", err)
	} else if cnt != 0 {
		t.Fatalf("expected 0 records after truncate, got %d", cnt)
	}
	if ids, err := readQueryKeys(c, qx.Query()); err != nil {
		t.Fatalf("QueryKeys(NOOP after): %v", err)
	} else if len(ids) != 0 {
		t.Fatalf("expected no keys after truncate, got %v", ids)
	}

	st, err := c.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if st.KeyCount != 0 {
		t.Fatalf("expected Stats.KeyCount=0 after truncate, got %d", st.KeyCount)
	}
	var zero uint64
	if st.LastKey != zero {
		t.Fatalf("expected Stats.LastKey=%v after truncate, got %v", zero, st.LastKey)
	}

	if err := writeSet(c, 10, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("expected unique values reusable after truncate, got: %v", err)
	}
	if err := writeSet(c, 11, &UniqueTestRec{Email: "a@x", Code: 2}); err == nil || !errors.Is(err, rbierrors.ErrUniqueViolation) {
		t.Fatalf("expected rbierrors.ErrUniqueViolation after truncate+reuse, got: %v", err)
	}

	if ids, err := readQueryKeys(c, qx.Query(qx.EQ("email", "a@x"))); err != nil {
		t.Fatalf("QueryKeys(after reinsert): %v", err)
	} else if len(ids) != 1 || ids[0] != 10 {
		t.Fatalf("expected [10] after reinsert, got %v", ids)
	}
}

func TestTruncate_NoDeadlock_WithConcurrentWriteRootBarrier(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "truncate_deadlock.db")
	c, bolt := openBoltAndCollection[uint64, Rec](t, path)

	if err := writeSet(c, 1, &Rec{Name: "seed", Age: 1}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	// Force a root publication contention window while one operation may hold
	// the bbolt writer transaction and the other waits for it.
	c.root.generationMu.Lock()
	truncateDone := make(chan error, 1)
	go func() {
		truncateDone <- c.Truncate()
	}()
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- writeSet(c, 1, &Rec{Name: "after", Age: 2})
	}()
	time.Sleep(20 * time.Millisecond)
	c.root.generationMu.Unlock()

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

	if err := c.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}
	if err := bolt.Close(); err != nil {
		t.Fatalf("raw.Close: %v", err)
	}
}

func TestTruncateFIFOWithRootWrites(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	entered := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- writeSet(c, 1, &Rec{Name: "first", Age: 1}, OnChange(func(_ *Tx, _ uint64, _, _ *Rec) error {
			close(entered)
			<-release
			return nil
		}))
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatalf("first write did not enter OnChange")
	}

	epochBefore := c.root.registry.current.Load().epoch

	truncateDone := make(chan error, 1)
	go func() {
		truncateDone <- c.Truncate()
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		c.root.scheduler.mu.Lock()
		queued := c.root.scheduler.queueLen
		c.root.scheduler.mu.Unlock()
		if queued != 0 {
			break
		}
		if time.Now().After(deadline) {
			close(release)
			t.Fatalf("truncate did not queue behind in-flight write")
		}
		time.Sleep(time.Millisecond)
	}

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- writeSet(c, 2, &Rec{Name: "second", Age: 2})
	}()

	close(release)

	if err := <-firstDone; err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := <-truncateDone; err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second write: %v", err)
	}

	if got := c.root.registry.current.Load().epoch; got != epochBefore+3 {
		t.Fatalf("root epoch after write+truncate+write=%d want %d", got, epochBefore+3)
	}
	if rec, err := readGet(c, 1); err != nil {
		t.Fatalf("read first: %v", err)
	} else if rec != nil {
		t.Fatalf("first write survived truncate: %#v", rec)
	}
	rec, err := readGet(c, 2)
	if err != nil {
		t.Fatalf("read second: %v", err)
	}
	defer releaseUniqueRecords(c, rec)
	if rec == nil || rec.Name != "second" {
		t.Fatalf("second write missing after truncate FIFO: %#v", rec)
	}
}

func TestAPI_New_InvalidBucketFillPercent_NegativeRejected(t *testing.T) {
	raw, _ := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	c, err := Open[uint64, Rec](raw, testOptions(Options{
		BucketName:        "api_invalid_fill_negative",
		BucketFillPercent: -0.25,
	}))
	if err == nil {
		_ = c.Close()
		t.Fatalf("expected negative BucketFillPercent to be rejected")
	}
}

func TestAPI_New_InvalidBucketFillPercent_AboveOneRejected(t *testing.T) {
	raw, _ := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	c, err := Open[uint64, Rec](raw, testOptions(Options{
		BucketName:        "api_invalid_fill_above_one",
		BucketFillPercent: 1.25,
	}))
	if err == nil {
		_ = c.Close()
		t.Fatalf("expected BucketFillPercent > 1 to be rejected")
	}
}

func TestAPI_BucketName_ReturnsClone(t *testing.T) {
	raw, _ := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	c, err := Open[uint64, Rec](raw, testOptions(Options{BucketName: "api_bucket_clone"}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = c.Close() }()

	orig := c.BucketName()
	mutated := c.BucketName()
	mutated[0] ^= 0xff

	again := c.BucketName()
	if !bytes.Equal(orig, again) {
		t.Fatalf("bucket name was mutated through returned slice: before=%q after=%q", orig, again)
	}
}

func TestAPI_QueryKeysQueryCountReturnErrClosedAfterClose(t *testing.T) {
	c, _ := openTempUint64Collection(t)
	mustSetAPIRec(t, c, 1, &Rec{Name: "alice", Age: 30})

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	q := qx.Query(qx.EQ("age", 30))

	if _, err := readQueryKeys(c, q); !errors.Is(err, rbierrors.ErrClosed) {
		t.Fatalf("QueryKeys after Close: expected rbierrors.ErrClosed, got %v", err)
	}
	if _, err := readQuery(c, q); !errors.Is(err, rbierrors.ErrClosed) {
		t.Fatalf("Query after Close: expected rbierrors.ErrClosed, got %v", err)
	}
	if _, err := readCount(c, q.Filter); !errors.Is(err, rbierrors.ErrClosed) {
		t.Fatalf("Count after Close: expected rbierrors.ErrClosed, got %v", err)
	}
}
