package rbi

import (
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/rbierrors"
	"go.etcd.io/bbolt"
)

func testRootRegistered(raw *bbolt.DB) bool {
	rootRegistryMu.Lock()
	_, ok := rootsByBolt[raw]
	rootRegistryMu.Unlock()
	return ok
}

func TestReadTxBindsOnceAcrossCollections(t *testing.T) {
	raw, _ := openRawBolt(t)
	recDB, err := Open[uint64, Rec](raw, testOptions(Options{}))
	if err != nil {
		t.Fatalf("New Rec: %v", err)
	}
	productDB, err := Open[string, Product](raw, testOptions(Options{}))
	if err != nil {
		t.Fatalf("New Product: %v", err)
	}
	t.Cleanup(func() {
		_ = recDB.Close()
		_ = productDB.Close()
		_ = raw.Close()
	})

	mustSetAPIRec(t, recDB, 1, &Rec{Name: "one", Age: 1})
	ioExtMustSetProduct(t, productDB, "p1", &Product{SKU: "p1", Price: 10})

	tx := BeginView()
	rec, err := recDB.Get(tx, 1)
	if err != nil {
		t.Fatalf("read rec: %v", err)
	}
	defer tx.Release()
	defer releaseUniqueRecords(recDB, rec)
	if rec == nil || rec.Name != "one" {
		t.Fatalf("read rec got %#v", rec)
	}
	boltTx := tx.boltTx

	product, err := productDB.Get(tx, "p1")
	if err != nil {
		t.Fatalf("read product: %v", err)
	}
	defer releaseUniqueRecords(productDB, product)
	if product == nil || product.SKU != "p1" {
		t.Fatalf("read product got %#v", product)
	}
	if tx.boltTx != boltTx {
		t.Fatalf("Tx rebound bbolt tx")
	}

	other, _ := openTempUint64Collection(t)
	if _, err = other.Get(tx, 1); !errors.Is(err, rbierrors.ErrStoreMismatch) {
		t.Fatalf("other root err=%v, want ErrRootMismatch", err)
	}

	tx.Close()
	mustSetAPIRec(t, recDB, 2, &Rec{Name: "two", Age: 2})
	rec2, err := readGet(recDB, 2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	defer releaseUniqueRecords(recDB, rec2)
	if rec2 == nil || rec2.Name != "two" {
		t.Fatalf("new read did not see committed write: %#v", rec2)
	}
}

func TestIndexTxBindsCurrentAndRejectsOtherRoot(t *testing.T) {
	c, _ := openTempUint64Collection(t)
	mustSetAPIRec(t, c, 1, &Rec{Name: "one", Age: 1})

	tx := BeginIndexView()
	snap, err := tx.collectionSnapshot(c.collection)
	if err != nil {
		t.Fatalf("collectionSnapshot: %v", err)
	}
	defer tx.Release()
	if snap == nil || snap.Seq == 0 {
		t.Fatalf("invalid snapshot: %#v", snap)
	}
	if tx.root != c.root || tx.gen == nil || tx.pin.ref == nil {
		t.Fatalf("Tx did not bind root generation")
	}

	other, _ := openTempUint64Collection(t)
	if _, err = tx.collectionSnapshot(other.collection); !errors.Is(err, rbierrors.ErrStoreMismatch) {
		t.Fatalf("other root err=%v, want ErrRootMismatch", err)
	}
	tx.Close()
	tx.Close()
}

func TestCountAcceptsReadAndIndexTx(t *testing.T) {
	c, _ := openTempUint64Collection(t)
	mustSetAPIRec(t, c, 1, &Rec{Name: "one", Age: 1})
	mustSetAPIRec(t, c, 2, &Rec{Name: "two", Age: 2})

	readTx := BeginView()
	count, err := c.Count(readTx)
	if err != nil {
		t.Fatalf("count with Tx: %v", err)
	}
	if count != 2 {
		t.Fatalf("count with Tx=%d want 2", count)
	}
	if readTx.boltTx == nil || readTx.pin.ref == nil {
		t.Fatalf("Tx was not bound for count")
	}
	readTx.Release()

	indexTx := BeginIndexView()
	count, err = c.Count(indexTx)
	if err != nil {
		t.Fatalf("count with Tx: %v", err)
	}
	if count != 2 {
		t.Fatalf("count with Tx=%d want 2", count)
	}
	if indexTx.root != c.root || indexTx.gen == nil || indexTx.pin.ref == nil {
		t.Fatalf("Tx was not bound for count")
	}
	indexTx.Release()

	if _, err = c.Count(nil); !errors.Is(err, rbierrors.ErrNilTx) {
		t.Fatalf("nil Tx err=%v, want ErrNilTx", err)
	}
	var nilReadTx *Tx
	if _, err = c.Count(nilReadTx); !errors.Is(err, rbierrors.ErrNilTx) {
		t.Fatalf("typed nil Tx err=%v, want ErrNilTx", err)
	}
	var nilIndexTx *Tx
	if _, err = c.Count(nilIndexTx); !errors.Is(err, rbierrors.ErrNilTx) {
		t.Fatalf("typed nil Tx err=%v, want ErrNilTx", err)
	}
}

func TestClosedReadTxRejectsCachedBindings(t *testing.T) {
	c, _ := openTempUint64Collection(t)
	mustSetAPIRec(t, c, 1, &Rec{Name: "one", Age: 1})

	tx := BeginView()
	rec, err := c.Get(tx, 1)
	if err != nil {
		t.Fatalf("bind Get: %v", err)
	}
	releaseUniqueRecords(c, rec)
	if _, err = c.Count(tx); err != nil {
		t.Fatalf("bind Count: %v", err)
	}

	tx.Close()
	if _, err = c.Get(tx, 1); !errors.Is(err, rbierrors.ErrTxDone) {
		t.Fatalf("Get after Close err=%v, want ErrTxDone", err)
	}
	if _, err = c.Count(tx); !errors.Is(err, rbierrors.ErrTxDone) {
		t.Fatalf("Count after Close err=%v, want ErrTxDone", err)
	}
	tx.Release()
}

func TestQueryKeysAcceptsIndexTxOnlyForNumericKeys(t *testing.T) {
	c, _ := openTempUint64Collection(t)
	mustSetAPIRec(t, c, 1, &Rec{Name: "one", Age: 1})
	mustSetAPIRec(t, c, 2, &Rec{Name: "two", Age: 2})

	tx := BeginIndexView()
	keys, err := c.QueryKeys(tx, qx.Query(qx.GTE("age", 1)))
	if err != nil {
		t.Fatalf("numeric QueryKeys with index tx: %v", err)
	}
	if tx.boltTx != nil {
		t.Fatalf("numeric QueryKeys with index tx opened bbolt transaction")
	}
	tx.Release()
	assertUint64Slice(t, keys, []uint64{1, 2})

	productDB, _ := openTempCollectionStringProduct(t)
	ioExtMustSetProduct(t, productDB, "p1", &Product{SKU: "p1", Price: 10})

	tx = BeginIndexView()
	_, err = productDB.QueryKeys(tx, qx.Query(qx.EQ("sku", "p1")))
	if !errors.Is(err, rbierrors.ErrWrongTx) {
		t.Fatalf("string QueryKeys with index tx err=%v, want ErrTxKind", err)
	}
	tx.Release()
}

func TestBoundReadAndIndexTxOperationsDoNotRepinRoot(t *testing.T) {
	c, _ := openTempUint64Collection(t)
	mustSetAPIRec(t, c, 1, &Rec{Name: "one", Age: 1})
	mustSetAPIRec(t, c, 2, &Rec{Name: "two", Age: 2})

	readTx := BeginView()
	rec, err := c.Get(readTx, 1)
	if err != nil {
		t.Fatalf("bind Tx: %v", err)
	}
	defer readTx.Release()
	releaseUniqueRecords(c, rec)
	if readTx.pin.ref == nil {
		t.Fatal("Tx did not pin root")
	}
	readRefs := readTx.pin.ref.refs.Load()
	for i := 0; i < 8; i++ {
		rec, err = c.Get(readTx, uint64(i%2+1))
		if err != nil {
			t.Fatalf("bound Tx Get(%d): %v", i, err)
		}
		releaseUniqueRecords(c, rec)
		if _, err = c.Count(readTx); err != nil {
			t.Fatalf("bound Tx Count(%d): %v", i, err)
		}
	}
	if got := readTx.pin.ref.refs.Load(); got != readRefs {
		t.Fatalf("bound Tx root refs=%d want %d", got, readRefs)
	}
	readTx.Close()

	indexTx := BeginIndexView()
	if _, err = c.Count(indexTx); err != nil {
		t.Fatalf("bind Tx: %v", err)
	}
	defer indexTx.Release()
	if indexTx.pin.ref == nil {
		t.Fatal("Tx did not pin root")
	}
	indexRefs := indexTx.pin.ref.refs.Load()
	for i := 0; i < 8; i++ {
		if _, err = c.Count(indexTx); err != nil {
			t.Fatalf("bound Tx Count(%d): %v", i, err)
		}
	}
	if got := indexTx.pin.ref.refs.Load(); got != indexRefs {
		t.Fatalf("bound Tx root refs=%d want %d", got, indexRefs)
	}
	indexTx.Close()
}

func TestReadTxNilAndCollectionVisibilityErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared.db")
	raw, err := bbolt.Open(path, 0o600, &bbolt.Options{InitialMmapSize: 1 << 20})
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}
	recDB, err := Open[uint64, Rec](raw, testOptions(Options{}))
	if err != nil {
		t.Fatalf("New Rec: %v", err)
	}
	t.Cleanup(func() {
		_ = recDB.Close()
		_ = raw.Close()
	})

	mustSetAPIRec(t, recDB, 1, &Rec{Name: "one", Age: 1})

	var nilTx *Tx
	if _, err = recDB.Get(nilTx, 1); !errors.Is(err, rbierrors.ErrNilTx) {
		t.Fatalf("nil Tx get err=%v, want ErrNilTx", err)
	}

	tx := BeginView()
	rec, err := recDB.Get(tx, 1)
	if err != nil {
		t.Fatalf("bind read tx: %v", err)
	}
	defer tx.Release()
	releaseUniqueRecords(recDB, rec)

	productDB, err := Open[string, Product](raw, testOptions(Options{}))
	if err != nil {
		t.Fatalf("New Product: %v", err)
	}
	t.Cleanup(func() { _ = productDB.Close() })

	if _, err = productDB.Get(tx, "missing"); !errors.Is(err, rbierrors.ErrCollectionNotVisible) {
		t.Fatalf("new collection through old Tx err=%v, want ErrCollectionNotVisible", err)
	}
	tx.Close()
}

func TestReadTxRetriesWhenBboltEpochIsOnlyStaged(t *testing.T) {
	raw, _ := openRawBolt(t)
	c, err := Open[uint64, Rec](raw, testOptions(Options{}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = c.Close()
		_ = raw.Close()
	})
	mustSetAPIRec(t, c, 1, &Rec{Name: "one", Age: 1})

	tx, err := raw.Begin(true)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	meta := tx.Bucket(c.root.metaBucket)
	if meta == nil {
		_ = tx.Rollback()
		t.Fatalf("root metadata bucket missing")
	}
	epoch, err := meta.NextSequence()
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("NextSequence: %v", err)
	}
	current := c.root.registry.current.Load()
	gen := c.root.registry.buildFromCurrent(epoch, len(current.entries))
	c.root.registry.stage(epoch, gen)
	if err = tx.Commit(); err != nil {
		c.root.registry.dropStaged(epoch)
		t.Fatalf("Commit: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		readTx := BeginView()
		rec, readErr := c.Get(readTx, 1)
		if rec != nil {
			releaseUniqueRecords(c, rec)
		}
		if readErr == nil && readTx.epoch != epoch {
			readErr = errors.New("Tx bound wrong root epoch")
		}
		readTx.Release()
		done <- readErr
	}()

	select {
	case err = <-done:
		t.Fatalf("Tx completed while root generation was only staged: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	c.root.registry.publish(epoch)

	select {
	case err = <-done:
		if err != nil {
			t.Fatalf("Tx after publish: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Tx did not complete after root generation publish")
	}
}

func TestTxClosedDBHandleKeepsPinnedReadState(t *testing.T) {
	raw, _ := openRawBolt(t)
	c, err := Open[uint64, Rec](raw, testOptions(Options{}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = c.Close()
		_ = raw.Close()
	})

	mustSetAPIRec(t, c, 1, &Rec{Name: "one", Age: 1})

	readTx := BeginView()
	rec, err := c.Get(readTx, 1)
	if err != nil {
		t.Fatalf("bind read tx: %v", err)
	}
	defer readTx.Release()
	releaseUniqueRecords(c, rec)

	indexTx := BeginIndexView()
	if _, err = indexTx.collectionSnapshot(c.collection); err != nil {
		t.Fatalf("bind index tx: %v", err)
	}
	defer indexTx.Release()

	if err = c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	rec, err = c.Get(readTx, 1)
	if err != nil {
		t.Fatalf("closed DB read through pinned Tx: %v", err)
	}
	if rec == nil || rec.Name != "one" {
		t.Fatalf("closed DB read through pinned Tx rec=%#v", rec)
	}
	releaseUniqueRecords(c, rec)
	if snap, err := indexTx.collectionSnapshot(c.collection); err != nil {
		t.Fatalf("closed DB index through pinned Tx: %v", err)
	} else if snap == nil {
		t.Fatal("closed DB index through pinned Tx returned nil snapshot")
	}
	fresh := BeginView()
	if _, err = c.Get(fresh, 1); !errors.Is(err, rbierrors.ErrClosed) {
		t.Fatalf("closed DB read through fresh Tx err=%v, want ErrClosed", err)
	}
	fresh.Release()
	readTx.Close()
	indexTx.Close()
}

func TestTxRootMismatchPrecedesOtherRootClosed(t *testing.T) {
	c, _ := openTempUint64Collection(t)
	other, _ := openTempUint64Collection(t)
	mustSetAPIRec(t, c, 1, &Rec{Name: "one", Age: 1})

	readTx := BeginView()
	rec, err := c.Get(readTx, 1)
	if err != nil {
		t.Fatalf("bind read tx: %v", err)
	}
	defer readTx.Release()
	releaseUniqueRecords(c, rec)

	indexTx := BeginIndexView()
	if _, err = indexTx.collectionSnapshot(c.collection); err != nil {
		t.Fatalf("bind index tx: %v", err)
	}
	defer indexTx.Release()

	writeTx := BeginUpdate()
	if err = c.Set(writeTx, 2, &Rec{Name: "two", Age: 2}); err != nil {
		t.Fatalf("bind write tx: %v", err)
	}
	defer writeTx.Release()

	if err = other.Close(); err != nil {
		t.Fatalf("Close other: %v", err)
	}

	if _, err = other.Get(readTx, 1); !errors.Is(err, rbierrors.ErrStoreMismatch) {
		t.Fatalf("closed other root read err=%v, want ErrRootMismatch", err)
	}
	if _, err = indexTx.collectionSnapshot(other.collection); !errors.Is(err, rbierrors.ErrStoreMismatch) {
		t.Fatalf("closed other root index err=%v, want ErrRootMismatch", err)
	}
	if err = other.Set(writeTx, 1, &Rec{Name: "other", Age: 1}); !errors.Is(err, rbierrors.ErrStoreMismatch) {
		t.Fatalf("closed other root write err=%v, want ErrRootMismatch", err)
	}
	if err = writeTx.Commit(); !errors.Is(err, rbierrors.ErrStoreMismatch) {
		t.Fatalf("Commit err=%v, want ErrRootMismatch", err)
	}
}

func TestIndexTxCollectionNotVisibleAfterRegistration(t *testing.T) {
	raw, _ := openRawBolt(t)
	recDB, err := Open[uint64, Rec](raw, testOptions(Options{}))
	if err != nil {
		t.Fatalf("New Rec: %v", err)
	}
	t.Cleanup(func() {
		_ = recDB.Close()
		_ = raw.Close()
	})

	tx := BeginIndexView()
	if _, err = tx.collectionSnapshot(recDB.collection); err != nil {
		t.Fatalf("bind index tx: %v", err)
	}
	defer tx.Release()

	productDB, err := Open[string, Product](raw, testOptions(Options{}))
	if err != nil {
		t.Fatalf("New Product: %v", err)
	}
	t.Cleanup(func() { _ = productDB.Close() })

	if _, err = tx.collectionSnapshot(productDB.collection); !errors.Is(err, rbierrors.ErrCollectionNotVisible) {
		t.Fatalf("new collection through old Tx err=%v, want ErrCollectionNotVisible", err)
	}
	tx.Close()
}

func expectTxLifecyclePanic(t testing.TB, fn func()) {
	t.Helper()
	defer func() {
		v := recover()
		if v == nil {
			t.Fatalf("expected transaction lifecycle panic")
		}
		if _, ok := v.(txLifecyclePanic); !ok {
			t.Fatalf("panic=%v (%T), want txLifecyclePanic", v, v)
		}
	}()
	fn()
}

func TestManagedViewLifecyclePanics(t *testing.T) {
	c, _ := openTempUint64Collection(t)
	mustSetAPIRec(t, c, 1, &Rec{Name: "one", Age: 1})

	expectTxLifecyclePanic(t, func() {
		_ = View(func(tx *Tx) error {
			tx.Close()
			return nil
		})
	})
	expectTxLifecyclePanic(t, func() {
		_ = View(func(tx *Tx) error {
			tx.Release()
			return nil
		})
	})
	expectTxLifecyclePanic(t, func() {
		_ = View(func(tx *Tx) error {
			return tx.Commit()
		})
	})
	expectTxLifecyclePanic(t, func() {
		_ = IndexView(func(tx *Tx) error {
			tx.Close()
			return nil
		})
	})
	expectTxLifecyclePanic(t, func() {
		_ = IndexView(func(tx *Tx) error {
			tx.Release()
			return nil
		})
	})
	expectTxLifecyclePanic(t, func() {
		_ = IndexView(func(tx *Tx) error {
			return tx.Commit()
		})
	})

	if got, err := readCount(c); err != nil {
		t.Fatalf("count after managed lifecycle panics: %v", err)
	} else if got != 1 {
		t.Fatalf("count after managed lifecycle panics=%d want 1", got)
	}
}

func TestManagedViewPanicClosesTx(t *testing.T) {
	c, _ := openTempUint64Collection(t)
	mustSetAPIRec(t, c, 1, &Rec{Name: "one", Age: 1})

	readPanicked := false
	func() {
		defer func() {
			readPanicked = recover() != nil
		}()
		_ = View(func(tx *Tx) error {
			if _, err := c.Count(tx); err != nil {
				return err
			}
			panic("managed view panic")
		})
	}()
	if !readPanicked {
		t.Fatal("expected managed View callback panic")
	}

	indexPanicked := false
	func() {
		defer func() {
			indexPanicked = recover() != nil
		}()
		_ = IndexView(func(tx *Tx) error {
			if _, err := c.Count(tx); err != nil {
				return err
			}
			panic("managed index view panic")
		})
	}()
	if !indexPanicked {
		t.Fatal("expected managed IndexView callback panic")
	}
}

func TestReadTxCloseReapsClosedRoot(t *testing.T) {
	raw, _ := openRawBolt(t)
	c, err := Open[uint64, Rec](raw, testOptions(Options{Index: map[string]IndexKind{}}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = c.Close()
		_ = raw.Close()
	})

	mustSetAPIRec(t, c, 1, &Rec{Name: "one", Age: 1})
	tx := BeginView()
	rec, err := c.Get(tx, 1)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	defer tx.Release()
	defer releaseUniqueRecords(c, rec)

	if err = c.Close(); err != nil {
		t.Fatalf("c.Close: %v", err)
	}
	if !testRootRegistered(raw) {
		t.Fatalf("root was reaped while Tx was still pinned")
	}
	tx.Close()
	if testRootRegistered(raw) {
		t.Fatalf("root was not reaped after final Tx close")
	}
}

func TestIndexTxCloseReapsClosedRoot(t *testing.T) {
	raw, _ := openRawBolt(t)
	c, err := Open[uint64, Rec](raw, testOptions(Options{}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = c.Close()
		_ = raw.Close()
	})

	mustSetAPIRec(t, c, 1, &Rec{Name: "one", Age: 1})
	tx := BeginIndexView()
	if _, err = tx.collectionSnapshot(c.collection); err != nil {
		t.Fatalf("index snapshot: %v", err)
	}
	defer tx.Release()

	if err = c.Close(); err != nil {
		t.Fatalf("c.Close: %v", err)
	}
	if !testRootRegistered(raw) {
		t.Fatalf("root was reaped while Tx was still pinned")
	}
	tx.Close()
	if testRootRegistered(raw) {
		t.Fatalf("root was not reaped after final Tx close")
	}
}

func TestWriteTxLifecycle(t *testing.T) {
	if tx := BeginUpdate(); tx == nil {
		t.Fatalf("BeginUpdate returned nil")
	} else {
		tx.Release()
	}

	tx := BeginUpdate()
	if err := tx.Commit(); err != nil {
		t.Fatalf("empty Commit: %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, rbierrors.ErrTxDone) {
		t.Fatalf("second Commit err=%v, want ErrTxDone", err)
	}
	tx.Close()
	tx.Close()
	tx.Release()

	tx = BeginUpdate()
	tx.Close()
	tx.Close()
	if err := tx.Commit(); !errors.Is(err, rbierrors.ErrTxDone) {
		t.Fatalf("Commit after Close err=%v, want ErrTxDone", err)
	}
	tx.Release()

	readTx := BeginView()
	if err := readTx.Commit(); !errors.Is(err, rbierrors.ErrWrongTx) {
		t.Fatalf("read Commit err=%v, want ErrTxKind", err)
	}
	readTx.Close()
	readTx.Close()
	readTx.Release()
}

func TestWriteTxCollectionRetainLifecycle(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	tx := BeginUpdate()
	if err := tx.bindCollection(c.collection); err != nil {
		t.Fatalf("bind collection: %v", err)
	}
	if got := collectionRetainedForTest(c.collection); got != 1 {
		t.Fatalf("retained=%d want 1", got)
	}
	if err := tx.bindCollection(c.collection); err != nil {
		t.Fatalf("bind same collection: %v", err)
	}
	if got := collectionRetainedForTest(c.collection); got != 1 {
		t.Fatalf("duplicate retain count=%d want 1", got)
	}
	tx.Close()
	tx.Release()
	if got := collectionRetainedForTest(c.collection); got != 0 {
		t.Fatalf("retained after Close=%d want 0", got)
	}

	tx = BeginUpdate()
	if err := tx.bindCollection(c.collection); err != nil {
		t.Fatalf("bind collection before commit: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	tx.Release()
	if got := collectionRetainedForTest(c.collection); got != 0 {
		t.Fatalf("retained after commit=%d want 0", got)
	}
}

func collectionRetainedForTest(c *collection) int64 {
	c.retainMu.Lock()
	retained := c.retained
	c.retainMu.Unlock()
	return retained
}

func TestWriteTxQueuedWritesCommitTogether(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	tx := BeginUpdate()
	defer tx.Release()
	if err := c.Set(tx, 1, &Rec{Name: "one", Age: 1}); err != nil {
		t.Fatalf("set 1: %v", err)
	}
	if err := c.Set(tx, 2, &Rec{Name: "two", Age: 2}); err != nil {
		t.Fatalf("set 2: %v", err)
	}
	if got, err := readCount(c); err != nil {
		t.Fatalf("count before commit: %v", err)
	} else if got != 0 {
		t.Fatalf("count before commit=%d want 0", got)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if got, err := readCount(c); err != nil {
		t.Fatalf("count after commit: %v", err)
	} else if got != 2 {
		t.Fatalf("count after commit=%d want 2", got)
	}
}

func TestWriteMethodsRequireWriteTx(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	var tx *Tx
	if err := c.Set(tx, 1, &Rec{Name: "one"}); !errors.Is(err, rbierrors.ErrNilTx) {
		t.Fatalf("Set nil tx err=%v, want ErrNilTx", err)
	}
	if err := c.Patch(tx, 1, []Field{{Name: "age", Value: 1}}); !errors.Is(err, rbierrors.ErrNilTx) {
		t.Fatalf("Patch nil tx err=%v, want ErrNilTx", err)
	}
	if err := c.Delete(tx, 1); !errors.Is(err, rbierrors.ErrNilTx) {
		t.Fatalf("Delete nil tx err=%v, want ErrNilTx", err)
	}
	readTx := BeginView()
	if err := c.Set(readTx, 1, &Rec{Name: "one"}); !errors.Is(err, rbierrors.ErrWrongTx) {
		t.Fatalf("Set read tx err=%v, want ErrTxKind", err)
	}
	readTx.Release()
	indexTx := BeginIndexView()
	if err := c.Delete(indexTx, 1); !errors.Is(err, rbierrors.ErrWrongTx) {
		t.Fatalf("Delete index tx err=%v, want ErrTxKind", err)
	}
	indexTx.Release()
	if got, err := readCount(c); err != nil {
		t.Fatalf("count after nil write tx calls: %v", err)
	} else if got != 0 {
		t.Fatalf("count after nil write tx calls=%d want 0", got)
	}
}

func TestWriteTxNoOpWritesDoNotBindOrRetain(t *testing.T) {
	c, _ := openTempUint64Collection(t)
	other, _ := openTempUint64Collection(t)

	tx := BeginUpdate()
	defer tx.Release()
	if err := c.Patch(tx, 1, nil); err != nil {
		t.Fatalf("empty Patch: %v", err)
	}
	if tx.root != nil || len(tx.collections) != 0 || len(tx.unit.segments) != 0 {
		t.Fatalf("no-op writes bound tx: root=%p collections=%d segments=%d", tx.root, len(tx.collections), len(tx.unit.segments))
	}
	if got := collectionRetainedForTest(c.collection); got != 0 {
		t.Fatalf("collection retained after no-op writes=%d want 0", got)
	}
	if err := other.Set(tx, 1, &Rec{Name: "other", Age: 1}); err != nil {
		t.Fatalf("Set on other root after no-op writes: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if got, err := readCount(c); err != nil {
		t.Fatalf("db count: %v", err)
	} else if got != 0 {
		t.Fatalf("db count=%d want 0", got)
	}
	if got, err := readCount(other); err != nil {
		t.Fatalf("other count: %v", err)
	} else if got != 1 {
		t.Fatalf("other count=%d want 1", got)
	}
}

func TestWriteTxRetainedCollectionAllowsFurtherWritesAfterCloseStarts(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	tx := BeginUpdate()
	defer tx.Release()
	if err := c.Set(tx, 1, &Rec{Name: "one", Age: 1}); err != nil {
		t.Fatalf("initial Set: %v", err)
	}
	c.collection.state.Or(collectionClosed)
	if err := c.Set(tx, 2, &Rec{Name: "two", Age: 2}); err != nil {
		c.collection.state.And(^collectionClosed)
		t.Fatalf("second Set after close start: %v", err)
	}
	if err := c.Patch(tx, 1, nil); err != nil {
		c.collection.state.And(^collectionClosed)
		t.Fatalf("no-op Patch after close start: %v", err)
	}
	c.collection.state.And(^collectionClosed)
	tx.Close()
}

func TestWriteTxNoHookSetDetachesCallerValueBeforeCommit(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	one := &Rec{Name: "one", Age: 1}
	two := &Rec{Name: "two", Age: 2}
	three := &Rec{Name: "three", Age: 3}

	tx := BeginUpdate()
	defer tx.Release()
	if err := c.Set(tx, 1, one); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := c.Set(tx, 2, two); err != nil {
		t.Fatalf("Set(2): %v", err)
	}
	if err := c.Set(tx, 3, three); err != nil {
		t.Fatalf("Set(3): %v", err)
	}

	one.Name, one.Age = "mutated", 10
	two.Name, two.Age = "mutated", 20
	three.Name, three.Age = "mutated", 30

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	for id, want := range map[uint64]Rec{
		1: {Name: "one", Age: 1},
		2: {Name: "two", Age: 2},
		3: {Name: "three", Age: 3},
	} {
		got, err := readGet(c, id)
		if err != nil {
			t.Fatalf("Get(%d): %v", id, err)
		}
		if got == nil || got.Name != want.Name || got.Age != want.Age {
			t.Fatalf("Get(%d)=%+v want name=%q age=%d", id, got, want.Name, want.Age)
		}
	}

	for name, want := range map[string]uint64{"one": 1, "two": 2, "three": 3} {
		keys, err := readQueryKeys(c, qx.Query(qx.EQ("name", name)))
		if err != nil {
			t.Fatalf("QueryKeys(name=%q): %v", name, err)
		}
		if len(keys) != 1 || keys[0] != want {
			t.Fatalf("QueryKeys(name=%q)=%v want [%d]", name, keys, want)
		}
	}
	keys, err := readQueryKeys(c, qx.Query(qx.EQ("name", "mutated")))
	if err != nil {
		t.Fatalf("QueryKeys(mutated): %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("QueryKeys(mutated)=%v want empty", keys)
	}
}

func TestWriteTxCommitsAcrossCollectionsOnOneRoot(t *testing.T) {
	raw, _ := openRawBolt(t)
	recDB, err := Open[uint64, Rec](raw, testOptions(Options{}))
	if err != nil {
		t.Fatalf("New Rec: %v", err)
	}
	productDB, err := Open[string, Product](raw, testOptions(Options{}))
	if err != nil {
		t.Fatalf("New Product: %v", err)
	}
	t.Cleanup(func() {
		_ = recDB.Close()
		_ = productDB.Close()
		_ = raw.Close()
	})

	tx := BeginUpdate()
	defer tx.Release()
	if err = recDB.Set(tx, 1, &Rec{Name: "one", Age: 1}); err != nil {
		t.Fatalf("set rec: %v", err)
	}
	if err = productDB.Set(tx, "p1", &Product{SKU: "p1", Price: 10}); err != nil {
		t.Fatalf("set product: %v", err)
	}
	epochBefore := recDB.root.registry.current.Load().epoch
	if got, err := readCount(recDB); err != nil {
		t.Fatalf("rec count before commit: %v", err)
	} else if got != 0 {
		t.Fatalf("rec count before commit=%d want 0", got)
	}
	if got, err := readCount(productDB); err != nil {
		t.Fatalf("product count before commit: %v", err)
	} else if got != 0 {
		t.Fatalf("product count before commit=%d want 0", got)
	}
	if err = tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	epochAfter := recDB.root.registry.current.Load().epoch
	if epochAfter != epochBefore+1 {
		t.Fatalf("root epoch after commit=%d want %d", epochAfter, epochBefore+1)
	}
	if got, err := readCount(recDB); err != nil {
		t.Fatalf("rec count after commit: %v", err)
	} else if got != 1 {
		t.Fatalf("rec count after commit=%d want 1", got)
	}
	if got, err := readCount(productDB); err != nil {
		t.Fatalf("product count after commit: %v", err)
	} else if got != 1 {
		t.Fatalf("product count after commit=%d want 1", got)
	}

	readTx := BeginView()
	defer readTx.Release()
	rec, err := recDB.Get(readTx, 1)
	if err != nil {
		t.Fatalf("read rec through shared Tx: %v", err)
	}
	product, err := productDB.Get(readTx, "p1")
	if err != nil {
		t.Fatalf("read product through shared Tx: %v", err)
	}
	readTx.Close()
	defer releaseUniqueRecords(recDB, rec)
	defer releaseUniqueRecords(productDB, product)
	if rec == nil || rec.Name != "one" {
		t.Fatalf("shared Tx rec=%#v", rec)
	}
	if product == nil || product.SKU != "p1" {
		t.Fatalf("shared Tx product=%#v", product)
	}
}

func TestWriteTxCrossCollectionPreservesQueuedHookOrder(t *testing.T) {
	raw, _ := openRawBolt(t)
	recDB, err := Open[uint64, Rec](raw, testOptions(Options{}))
	if err != nil {
		t.Fatalf("New Rec: %v", err)
	}
	productDB, err := Open[string, Product](raw, testOptions(Options{}))
	if err != nil {
		t.Fatalf("New Product: %v", err)
	}
	t.Cleanup(func() {
		_ = recDB.Close()
		_ = productDB.Close()
		_ = raw.Close()
	})

	var events []string
	tx := BeginUpdate()
	defer tx.Release()
	if err = recDB.Set(tx, 1, &Rec{Name: "one"}, OnChange(func(*Tx, uint64, *Rec, *Rec) error {
		events = append(events, "rec-1")
		return nil
	})); err != nil {
		t.Fatalf("set rec 1: %v", err)
	}
	if err = productDB.Set(tx, "p1", &Product{SKU: "p1", Price: 10}, OnChange(func(*Tx, string, *Product, *Product) error {
		events = append(events, "product")
		return nil
	})); err != nil {
		t.Fatalf("set product: %v", err)
	}
	if err = recDB.Set(tx, 2, &Rec{Name: "two"}, OnChange(func(*Tx, uint64, *Rec, *Rec) error {
		events = append(events, "rec-2")
		return nil
	})); err != nil {
		t.Fatalf("set rec 2: %v", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if want := []string{"rec-1", "product", "rec-2"}; !slices.Equal(events, want) {
		t.Fatalf("hook order=%v want %v", events, want)
	}
}

func TestWriteTxRepeatedCollectionSegmentsKeepIndexSnapshot(t *testing.T) {
	raw, _ := openRawBolt(t)
	recDB, err := Open[uint64, Rec](raw, testOptions(Options{}))
	if err != nil {
		t.Fatalf("New Rec: %v", err)
	}
	productDB, err := Open[string, Product](raw, testOptions(Options{}))
	if err != nil {
		t.Fatalf("New Product: %v", err)
	}
	t.Cleanup(func() {
		_ = recDB.Close()
		_ = productDB.Close()
		_ = raw.Close()
	})

	tx := BeginUpdate()
	defer tx.Release()
	if err = recDB.Set(tx, 1, &Rec{Name: "first", Age: 1}); err != nil {
		t.Fatalf("set rec 1: %v", err)
	}
	if err = productDB.Set(tx, "p1", &Product{SKU: "p1", Price: 10}); err != nil {
		t.Fatalf("set product: %v", err)
	}
	if err = recDB.Set(tx, 2, &Rec{Name: "second", Age: 2}); err != nil {
		t.Fatalf("set rec 2: %v", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	records, err := readValues(recDB, 1, 2)
	if err != nil {
		t.Fatalf("readValues records: %v", err)
	}
	if records[0] == nil || records[0].Name != "first" || records[1] == nil || records[1].Name != "second" {
		t.Fatalf("stored records=%#v", records)
	}
	count, err := readCount(recDB)
	if err != nil {
		t.Fatalf("Count records: %v", err)
	}
	if count != 2 {
		t.Fatalf("indexed count=%d want 2", count)
	}
	firstKeys, err := readQueryKeys(recDB, qx.Query(qx.EQ("name", "first")))
	if err != nil {
		t.Fatalf("QueryKeys(first): %v", err)
	}
	if !slices.Equal(firstKeys, []uint64{1}) {
		t.Fatalf("QueryKeys(first)=%v want [1]", firstKeys)
	}
	secondKeys, err := readQueryKeys(recDB, qx.Query(qx.EQ("name", "second")))
	if err != nil {
		t.Fatalf("QueryKeys(second): %v", err)
	}
	if !slices.Equal(secondKeys, []uint64{2}) {
		t.Fatalf("QueryKeys(second)=%v want [2]", secondKeys)
	}
}

func TestWriteTxGeneratedWriteRunsFIFOBeforeLaterQueuedSegment(t *testing.T) {
	raw, _ := openRawBolt(t)
	recDB, err := Open[uint64, Rec](raw, testOptions(Options{}))
	if err != nil {
		t.Fatalf("New Rec: %v", err)
	}
	productDB, err := Open[string, Product](raw, testOptions(Options{}))
	if err != nil {
		t.Fatalf("New Product: %v", err)
	}
	t.Cleanup(func() {
		_ = recDB.Close()
		_ = productDB.Close()
		_ = raw.Close()
	})

	var events []string
	tx := BeginUpdate()
	defer tx.Release()
	if err = recDB.Set(tx, 1, &Rec{Name: "owner"}, OnChange(func(tx *Tx, _ uint64, _, _ *Rec) error {
		events = append(events, "owner")
		return productDB.Set(tx, "generated", &Product{SKU: "generated"}, OnChange(func(*Tx, string, *Product, *Product) error {
			events = append(events, "generated")
			return nil
		}))
	})); err != nil {
		t.Fatalf("set owner: %v", err)
	}
	if err = productDB.Set(tx, "original", &Product{SKU: "original"}, OnChange(func(*Tx, string, *Product, *Product) error {
		events = append(events, "original")
		return nil
	})); err != nil {
		t.Fatalf("set original product: %v", err)
	}
	// The generated insert must not depend on the current segment slice backing array staying stable.
	tx.unit.segments = tx.unit.segments[:len(tx.unit.segments):len(tx.unit.segments)]

	if err = tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if want := []string{"owner", "generated", "original"}; !slices.Equal(events, want) {
		t.Fatalf("hook order=%v want %v", events, want)
	}
	products, err := readValues(productDB, "generated", "original")
	if err != nil {
		t.Fatalf("readValues products: %v", err)
	}
	if products[0] == nil || products[0].SKU != "generated" || products[1] == nil || products[1].SKU != "original" {
		t.Fatalf("products after generated write=%#v", products)
	}
}

func TestWriteTxGeneratedWriteRunsFIFOBeforeLaterSameCollectionRequest(t *testing.T) {
	raw, _ := openRawBolt(t)
	c, err := Open[uint64, Rec](raw, testOptions(Options{}))
	if err != nil {
		t.Fatalf("New Rec: %v", err)
	}
	t.Cleanup(func() {
		_ = c.Close()
		_ = raw.Close()
	})

	var events []string
	tx := BeginUpdate()
	defer tx.Release()
	if err = c.Set(tx, 1, &Rec{Name: "owner"}, OnChange(func(tx *Tx, _ uint64, _, _ *Rec) error {
		events = append(events, "owner")
		return c.Set(tx, 2, &Rec{Name: "generated"}, OnChange(func(*Tx, uint64, *Rec, *Rec) error {
			events = append(events, "generated")
			return nil
		}))
	})); err != nil {
		t.Fatalf("set owner: %v", err)
	}
	if err = c.Set(tx, 2, &Rec{Name: "user"}, OnChange(func(*Tx, uint64, *Rec, *Rec) error {
		events = append(events, "user")
		return nil
	})); err != nil {
		t.Fatalf("set user: %v", err)
	}

	if err = tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if want := []string{"owner", "generated", "user"}; !slices.Equal(events, want) {
		t.Fatalf("hook order=%v want %v", events, want)
	}
	got, err := readValues(c, 1, 2)
	if err != nil {
		t.Fatalf("readValues: %v", err)
	}
	if got[0] == nil || got[0].Name != "owner" || got[1] == nil || got[1].Name != "user" {
		t.Fatalf("records after generated write=%#v", got)
	}
}

func TestWriteTxCrossCollectionCommitErrorRollsBackAll(t *testing.T) {
	raw, _ := openRawBolt(t)
	recDB, err := Open[uint64, Rec](raw, testOptions(Options{}))
	if err != nil {
		t.Fatalf("New Rec: %v", err)
	}
	productDB, err := Open[string, Product](raw, testOptions(Options{}))
	if err != nil {
		t.Fatalf("New Product: %v", err)
	}
	t.Cleanup(func() {
		_ = recDB.Close()
		_ = productDB.Close()
		_ = raw.Close()
	})
	fail := errors.New("fail collection commit")

	tx := BeginUpdate()
	defer tx.Release()
	if err = recDB.Set(tx, 1, &Rec{Name: "one", Age: 1}); err != nil {
		t.Fatalf("set rec: %v", err)
	}
	if err = productDB.Set(tx, "p1", &Product{SKU: "p1", Price: 10}, OnChange(func(*Tx, string, *Product, *Product) error {
		return fail
	})); err != nil {
		t.Fatalf("set product: %v", err)
	}
	if err = tx.Commit(); !errors.Is(err, fail) {
		t.Fatalf("Commit err=%v want %v", err, fail)
	}
	if got, err := readCount(recDB); err != nil {
		t.Fatalf("rec count after failed commit: %v", err)
	} else if got != 0 {
		t.Fatalf("rec count after failed commit=%d want 0", got)
	}
	if got, err := readCount(productDB); err != nil {
		t.Fatalf("product count after failed commit: %v", err)
	} else if got != 0 {
		t.Fatalf("product count after failed commit=%d want 0", got)
	}
}

func TestWriteTxCloseCancelsQueuedWrites(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	tx := BeginUpdate()
	defer tx.Release()
	if err := c.Set(tx, 1, &Rec{Name: "one", Age: 1}); err != nil {
		t.Fatalf("set: %v", err)
	}
	tx.Close()
	if got, err := readCount(c); err != nil {
		t.Fatalf("count after Close: %v", err)
	} else if got != 0 {
		t.Fatalf("count after Close=%d want 0", got)
	}
}

func TestWriteTxCommitErrorRollsBackQueuedBatch(t *testing.T) {
	c, _ := openTempUint64Collection(t)
	fail := errors.New("fail commit")

	tx := BeginUpdate()
	defer tx.Release()
	if err := c.Set(tx, 1, &Rec{Name: "one", Age: 1}); err != nil {
		t.Fatalf("set 1: %v", err)
	}
	if err := c.Set(tx, 2, &Rec{Name: "two", Age: 2}, OnChange(func(*Tx, uint64, *Rec, *Rec) error {
		return fail
	})); err != nil {
		t.Fatalf("set 2: %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, fail) {
		t.Fatalf("Commit err=%v want %v", err, fail)
	}
	if got, err := readCount(c); err != nil {
		t.Fatalf("count after failed commit: %v", err)
	} else if got != 0 {
		t.Fatalf("count after failed commit=%d want 0", got)
	}
}

func TestWriteTxRootMismatchReleasesRetains(t *testing.T) {
	db1, _ := openTempUint64Collection(t)
	db2, _ := openTempUint64Collection(t)

	tx := BeginUpdate()
	defer tx.Release()
	if err := tx.bindCollection(db1.collection); err != nil {
		t.Fatalf("bind db1: %v", err)
	}
	if err := tx.bindCollection(db2.collection); !errors.Is(err, rbierrors.ErrStoreMismatch) {
		t.Fatalf("bind db2 err=%v, want ErrRootMismatch", err)
	}
	if got := collectionRetainedForTest(db1.collection); got != 0 {
		t.Fatalf("db1 retained after mismatch=%d want 0", got)
	}
	if got := collectionRetainedForTest(db2.collection); got != 0 {
		t.Fatalf("db2 retained after mismatch=%d want 0", got)
	}
	if err := tx.Commit(); !errors.Is(err, rbierrors.ErrStoreMismatch) {
		t.Fatalf("Commit err=%v, want ErrRootMismatch", err)
	}
}

func TestDBCloseWaitsForRetainedWriteTx(t *testing.T) {
	raw, _ := openRawBolt(t)
	c, err := Open[uint64, Rec](raw, testOptions(Options{}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = c.Close()
		_ = raw.Close()
	})

	tx := BeginUpdate()
	defer tx.Release()
	if err = tx.bindCollection(c.collection); err != nil {
		t.Fatalf("bind collection: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		done <- c.Close()
	}()

	deadline := time.After(2 * time.Second)
	for c.state.Load()&collectionClosed == 0 {
		select {
		case err = <-done:
			t.Fatalf("Close returned before marking closed: %v", err)
		case <-deadline:
			t.Fatalf("Close did not mark collection closed")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	select {
	case err = <-done:
		t.Fatalf("Close returned while Tx retained collection: %v", err)
	default:
	}

	tx.Close()
	select {
	case err = <-done:
		if err != nil {
			t.Fatalf("Close after Tx close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Close did not finish after Tx close")
	}
}

func TestWriteTxBindClosedCollection(t *testing.T) {
	c, _ := openTempUint64Collection(t)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	tx := BeginUpdate()
	defer tx.Release()
	if err := tx.bindCollection(c.collection); !errors.Is(err, rbierrors.ErrClosed) {
		t.Fatalf("bind closed collection err=%v, want ErrClosed", err)
	}
	if err := tx.Commit(); !errors.Is(err, rbierrors.ErrClosed) {
		t.Fatalf("Commit err=%v, want ErrClosed", err)
	}
}

func TestUpdateManagedWriteTxLifecycle(t *testing.T) {
	if err := Update(func(*Tx) error { return nil }); err != nil {
		t.Fatalf("empty Update: %v", err)
	}

	c, _ := openTempUint64Collection(t)
	if err := Update(func(tx *Tx) error {
		return c.Set(tx, 1, &Rec{Name: "one", Age: 1})
	}); err != nil {
		t.Fatalf("Update Set: %v", err)
	}
	if got, err := readCount(c); err != nil {
		t.Fatalf("count after Update Set: %v", err)
	} else if got != 1 {
		t.Fatalf("count after Update Set=%d want 1", got)
	}

	expectTxLifecyclePanic(t, func() {
		_ = Update(func(tx *Tx) error {
			return tx.Commit()
		})
	})
	expectTxLifecyclePanic(t, func() {
		_ = Update(func(tx *Tx) error {
			tx.Release()
			return nil
		})
	})
	expectTxLifecyclePanic(t, func() {
		_ = Update(func(tx *Tx) error {
			if err := c.Set(tx, 2, &Rec{Name: "two", Age: 2}); err != nil {
				return err
			}
			tx.Close()
			return nil
		})
	})
	if got, err := readCount(c); err != nil {
		t.Fatalf("count after managed lifecycle panic: %v", err)
	} else if got != 1 {
		t.Fatalf("count after managed lifecycle panic=%d want 1", got)
	}

	want := errors.New("callback")
	if err := Update(func(*Tx) error {
		return want
	}); !errors.Is(err, want) {
		t.Fatalf("Update error precedence err=%v, want callback error", err)
	}
}

func TestUpdatePanicRollsBackQueuedWrites(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	panicked := false
	func() {
		defer func() {
			panicked = recover() != nil
		}()
		_ = Update(func(tx *Tx) error {
			if err := c.Set(tx, 1, &Rec{Name: "panic", Age: 1}); err != nil {
				t.Fatalf("Set: %v", err)
			}
			panic("managed update panic")
		})
	}()
	if !panicked {
		t.Fatal("expected managed Update callback panic")
	}
	if got := collectionRetainedForTest(c.collection); got != 0 {
		t.Fatalf("collection retained after panic=%d want 0", got)
	}
	if count, err := readCount(c); err != nil {
		t.Fatalf("count after panic: %v", err)
	} else if count != 0 {
		t.Fatalf("count after panic=%d want 0", count)
	}
}
