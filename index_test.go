package rbi

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/strmap"
	"go.etcd.io/bbolt"
)

func TestReadSidecarString_RoundTrip(t *testing.T) {
	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	const want = "plain-string"
	if err := writeSidecarString(writer, want); err != nil {
		t.Fatalf("writeSidecarString: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got, err := readSidecarString(bufio.NewReader(bytes.NewReader(payload.Bytes())))
	if err != nil {
		t.Fatalf("readSidecarString: %v", err)
	}
	if got != want {
		t.Fatalf("readSidecarString mismatch: got %q", got)
	}
}

func TestQueryViewIdxMapping_UsesPinnedStrMapSnapshot(t *testing.T) {
	db := &DB[string, UserBench]{
		strKey:  true,
		strMap:  strmap.New(0, defaultSnapshotStrMapCompactDepth),
		options: &Options{},
		engine: &queryEngine{
			schema:  &schema.Runtime{Fields: map[string]*schema.Field{}},
			planner: &planner{},
			viewPool: &pooled.Pointers[queryView]{
				Clear: true,
			},
		},
	}
	snapMapper := strmap.New(0, defaultSnapshotStrMapCompactDepth)
	if idx, created := snapMapper.Create("snap-key"); idx != 1 || !created {
		t.Fatalf("snap Create = %d/%v, want 1/true", idx, created)
	}
	strMapSnap := snapMapper.Snapshot()
	indexSnap := &indexSnapshot{
		strmap:            strMapSnap,
		lenZeroComplement: snapshotTestBoolSlots(nil, nil),
	}
	view := &queryView{
		engine:            db.engine,
		snap:              indexSnap,
		strKey:            true,
		strMapView:        strMapSnap,
		planner:           db.engine.planner,
		lenZeroComplement: indexSnap.lenZeroComplement,
	}

	db.strMap.Create("live-key")

	if got, ok := view.strMapView.String(1); !ok || got != "snap-key" {
		t.Fatalf("strmap snapshot mismatch: got=%q ok=%v want=%q", got, ok, "snap-key")
	}
}

func indexTestSingleton(id uint64) posting.List {
	return (posting.List{}).BuildAdded(id)
}

func TestIndexPersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persist.db")

	db, raw := openBoltAndNew[uint64, Rec](t, path)

	if err := db.Set(1, &Rec{Name: "alice", Age: 10, Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(2, &Rec{Name: "bob", Age: 20, Tags: []string{"java"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}

	db2, raw2 := openBoltAndNew[uint64, Rec](t, path)
	t.Cleanup(func() {
		if err := db2.Close(); err != nil {
			t.Fatal(err)
		}
		if err := raw2.Close(); err != nil {
			t.Fatal(err)
		}
	})

	ids, err := db2.QueryKeys(qx.Query(qx.EQ("name", "alice")))
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

func TestIndexPersistence_ChunkedFieldRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persist_chunked.db")

	db, raw := openBoltAndNew[uint64, Rec](t, path)
	for i := 0; i < indexdata.FieldChunkThreshold+64; i++ {
		if err := db.Set(uint64(i+1), &Rec{
			Name:  fmt.Sprintf("user_%04d", i),
			Email: fmt.Sprintf("user_%04d@example.test", i),
			Age:   i,
		}); err != nil {
			t.Fatalf("Set(%d): %v", i+1, err)
		}
	}

	if storage, ok := db.engine.getSnapshot().fieldIndexStorage("name"); !ok || !storage.IsChunked() {
		t.Fatalf("expected chunked name index before close")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}

	db2, raw2 := openBoltAndNew[uint64, Rec](t, path)
	t.Cleanup(func() {
		if err := db2.Close(); err != nil {
			t.Fatal(err)
		}
		if err := raw2.Close(); err != nil {
			t.Fatal(err)
		}
	})

	if storage, ok := db2.engine.getSnapshot().fieldIndexStorage("name"); !ok || !storage.IsChunked() {
		t.Fatalf("expected chunked name index after reopen")
	}

	ids, err := db2.QueryKeys(qx.Query(qx.EQ("name", "user_0007")))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) != 1 || ids[0] != 8 {
		t.Fatalf("expected [8], got %v", ids)
	}
}

func TestIndexPersistence_LenZeroComplement_AllEmptyAfterReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persist_len_zero_complement_all_empty.db")

	db, raw := openBoltAndNew[uint64, Rec](t, path)
	for i := 1; i <= 90; i++ {
		rec := &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Email: fmt.Sprintf("u_%d@example.test", i),
			Age:   i,
		}
		if i%5 == 0 {
			rec.Tags = []string{"go"}
		}
		if err := db.Set(uint64(i), rec); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}
	if !db.isLenZeroComplementField("tags") {
		t.Fatalf("expected zero-complement mode before patching to empty")
	}

	for i := 1; i <= 90; i++ {
		if err := db.Patch(uint64(i), []Field{{Name: "tags", Value: []string(nil)}}); err != nil {
			t.Fatalf("Patch(%d): %v", i, err)
		}
	}

	want := make([]uint64, 0, 90)
	for i := 1; i <= 90; i++ {
		want = append(want, uint64(i))
	}
	gotBeforeClose, err := db.QueryKeys(qx.Query(qx.EQ("tags", []string{})))
	if err != nil {
		t.Fatalf("QueryKeys(empty tags) before close: %v", err)
	}
	if !slices.Equal(gotBeforeClose, want) {
		t.Fatalf("unexpected empty-tags ids before reopen: got=%v want=%v", gotBeforeClose, want)
	}

	if err = db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err = raw.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}

	db2, raw2 := openBoltAndNew[uint64, Rec](t, path)
	t.Cleanup(func() {
		if err = db2.Close(); err != nil {
			t.Fatal(err)
		}
		if err = raw2.Close(); err != nil {
			t.Fatal(err)
		}
	})

	gotAfterReopen, err := db2.QueryKeys(qx.Query(qx.EQ("tags", []string{})))
	if err != nil {
		t.Fatalf("QueryKeys(empty tags) after reopen: %v", err)
	}
	if !slices.Equal(gotAfterReopen, want) {
		t.Fatalf("unexpected empty-tags ids after reopen: got=%v want=%v", gotAfterReopen, want)
	}
}

func TestRebuildIndex_LenZeroComplementClearsStaleFlags(t *testing.T) {
	db, _ := openTempDBUint64(t)

	for i := 1; i <= 90; i++ {
		rec := &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Email: fmt.Sprintf("u_%d@example.test", i),
			Age:   i,
		}
		if i%5 == 0 {
			rec.Tags = []string{"go"}
		}
		if err := db.Set(uint64(i), rec); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}

	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex(initial): %v", err)
	}
	if !db.isLenZeroComplementField("tags") {
		t.Fatalf("expected zero-complement mode before rebuild transition")
	}

	for i := 1; i <= 90; i++ {
		if i%5 == 0 {
			continue
		}
		if err := db.Patch(uint64(i), []Field{{Name: "tags", Value: []string{"go"}}}); err != nil {
			t.Fatalf("Patch(%d): %v", i, err)
		}
	}

	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex(after patch): %v", err)
	}
	if db.isLenZeroComplementField("tags") {
		t.Fatalf("expected zero-complement mode to be cleared after rebuild")
	}

	got, err := db.QueryKeys(qx.Query(qx.EQ("tags", []string{})))
	if err != nil {
		t.Fatalf("QueryKeys(empty tags): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no empty-tags ids after rebuild, got %v", got)
	}
}

func TestRebuildIndex_CleanState(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(2, &Rec{Name: "bob", Age: 30}); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(3, &Rec{Name: "charlie", Age: 30}); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(2); err != nil {
		t.Fatal(err)
	}
	cnt, err := db.Count(qx.EQ("age", 30))
	if err != nil {
		t.Fatal(err)
	}
	if cnt != 2 {
		t.Fatalf("before rebuild: expected 2, got %d", cnt)
	}

	if err = db.RebuildIndex(); err != nil {
		t.Fatal(err)
	}

	// verify state (1, 3)
	ids, err := db.QueryKeys(qx.Query(qx.EQ("age", 30)))
	if err != nil {
		t.Fatal(err)
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 3 {
		t.Fatalf("after rebuild: expected [1, 3], got %v", ids)
	}
}

func TestRebuildIndex_ClosedReturnsErrClosed(t *testing.T) {
	db, _ := openTempDBUint64(t)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := db.RebuildIndex(); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got: %v", err)
	}
}

func TestReadMethods_ReturnErrorWhenBucketMissing(t *testing.T) {
	db, _ := openTempDBUint64(t)
	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.Bolt().Update(func(tx *bbolt.Tx) error {
		if tx.Bucket(db.BucketName()) == nil {
			return nil
		}
		return tx.DeleteBucket(db.BucketName())
	}); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}

	expectBucketErr := func(op string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s: expected error, got nil", op)
		}
		if !strings.Contains(err.Error(), "bucket does not exist") {
			t.Fatalf("%s: unexpected error: %v", op, err)
		}
	}

	_, err := db.Get(1)
	expectBucketErr("Get", err)

	_, err = db.BatchGet(1, 2)
	expectBucketErr("BatchGet", err)

	err = db.SeqScan(0, func(_ uint64, _ *Rec) (bool, error) {
		return true, nil
	})
	expectBucketErr("SeqScan", err)

	err = db.SeqScanRaw(0, func(_ uint64, _ []byte) (bool, error) {
		return true, nil
	})
	expectBucketErr("SeqScanRaw", err)

	_, err = db.Query(qx.Query())
	expectBucketErr("Query", err)
}

func TestQuery_MissingBucket_EmptyIndexResultStillRequiresSequenceTx(t *testing.T) {
	db, _ := openTempDBUint64(t)
	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.Bolt().Update(func(tx *bbolt.Tx) error {
		if tx.Bucket(db.BucketName()) == nil {
			return nil
		}
		return tx.DeleteBucket(db.BucketName())
	}); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}

	items, err := db.Query(qx.Query(qx.EQ("age", 999_999)))
	if err == nil {
		t.Fatalf("expected missing bucket error, got items=%#v", items)
	}
}

func TestRebuildIndex_ScanErrorClearsActiveFlag(t *testing.T) {
	db, _ := openTempDBUint64(t)
	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.Bolt().Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.BucketName())
		if b == nil {
			return fmt.Errorf("bucket missing")
		}
		var keyBuf [8]byte
		return b.Put(keycodec.UserKeyBytesWithBuf(uint64(1), db.strKey, &keyBuf), []byte{0xff})
	}); err != nil {
		t.Fatalf("corrupt value: %v", err)
	}

	err := db.RebuildIndex()
	if err == nil {
		t.Fatal("expected rebuild error on corrupted value")
	}
	if !strings.Contains(err.Error(), "scan error") {
		t.Fatalf("expected scan error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "id=1") {
		t.Fatalf("expected scan error to include key id, got: %v", err)
	}
	if !strings.Contains(err.Error(), "idx=1") {
		t.Fatalf("expected scan error to include idx, got: %v", err)
	}
	if !strings.Contains(err.Error(), "value_len=1") {
		t.Fatalf("expected scan error to include value len, got: %v", err)
	}

	// Rebuild must clear active flag even when build fails.
	if err := db.Set(2, &Rec{Name: "bob", Age: 31}); err != nil {
		t.Fatalf("Set after failed rebuild should not see busy flag, got: %v", err)
	}

	err = db.RebuildIndex()
	if err == nil || !strings.Contains(err.Error(), "scan error") {
		t.Fatalf("second RebuildIndex: expected scan error, got: %v", err)
	}
}

func TestRebuildIndex_StopTheWorld(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	scanStarted := make(chan struct{})
	releaseScan := make(chan struct{})
	scanDone := make(chan error, 1)

	go func() {
		err := db.SeqScan(0, func(_ uint64, _ *Rec) (bool, error) {
			close(scanStarted)
			<-releaseScan
			return false, nil
		})
		scanDone <- err
	}()

	select {
	case <-scanStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("scan did not start in time")
	}

	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- db.RebuildIndex()
	}()

	deadline := time.Now().Add(2 * time.Second)
	sawRebuildBusy := false
	for time.Now().Before(deadline) {
		_, err := db.Get(1)
		if errors.Is(err, ErrRebuildInProgress) {
			sawRebuildBusy = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !sawRebuildBusy {
		t.Fatal("expected ErrRebuildInProgress while rebuild waits for in-flight reader")
	}

	select {
	case err := <-rebuildDone:
		t.Fatalf("rebuild must wait for in-flight reader, got early result: %v", err)
	default:
	}

	close(releaseScan)
	if err := <-scanDone; err != nil {
		t.Fatalf("SeqScan: %v", err)
	}
	if err := <-rebuildDone; err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}
}

func TestRebuildIndex_ConcurrentCallReturnsErrRebuildInProgress(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	scanStarted := make(chan struct{})
	releaseScan := make(chan struct{})
	scanDone := make(chan error, 1)

	go func() {
		err := db.SeqScan(0, func(_ uint64, _ *Rec) (bool, error) {
			close(scanStarted)
			<-releaseScan
			return false, nil
		})
		scanDone <- err
	}()

	select {
	case <-scanStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("scan did not start in time")
	}

	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- db.RebuildIndex()
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, err := db.Get(1)
		if errors.Is(err, ErrRebuildInProgress) {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	if err := db.RebuildIndex(); !errors.Is(err, ErrRebuildInProgress) {
		close(releaseScan)
		<-scanDone
		<-rebuildDone
		t.Fatalf("expected ErrRebuildInProgress from concurrent rebuild, got: %v", err)
	}

	close(releaseScan)
	if err := <-scanDone; err != nil {
		t.Fatalf("SeqScan: %v", err)
	}
	if err := <-rebuildDone; err != nil {
		t.Fatalf("RebuildIndex(first): %v", err)
	}
}

func TestRebuildIndex_StopsTruncateWhileActive(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	scanStarted := make(chan struct{})
	releaseScan := make(chan struct{})
	scanDone := make(chan error, 1)

	go func() {
		err := db.SeqScan(0, func(_ uint64, _ *UniqueTestRec) (bool, error) {
			close(scanStarted)
			<-releaseScan
			return false, nil
		})
		scanDone <- err
	}()

	select {
	case <-scanStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("scan did not start in time")
	}

	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- db.RebuildIndex()
	}()

	deadline := time.Now().Add(2 * time.Second)
	sawBusy := false
	for time.Now().Before(deadline) {
		_, err := db.Get(1)
		if errors.Is(err, ErrRebuildInProgress) {
			sawBusy = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !sawBusy {
		close(releaseScan)
		<-scanDone
		<-rebuildDone
		t.Fatal("expected rebuild to become active")
	}

	if err := db.Truncate(); !errors.Is(err, ErrRebuildInProgress) {
		close(releaseScan)
		<-scanDone
		<-rebuildDone
		t.Fatalf("expected ErrRebuildInProgress from Truncate during rebuild, got: %v", err)
	}

	close(releaseScan)
	if err := <-scanDone; err != nil {
		t.Fatalf("SeqScan: %v", err)
	}
	if err := <-rebuildDone; err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	if err := db.Truncate(); err != nil {
		t.Fatalf("Truncate(after rebuild): %v", err)
	}
}

func TestRebuildIndex_RejectsSetWhileActive(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	scanStarted := make(chan struct{})
	releaseScan := make(chan struct{})
	scanDone := make(chan error, 1)

	go func() {
		err := db.SeqScan(0, func(_ uint64, _ *Rec) (bool, error) {
			close(scanStarted)
			<-releaseScan
			return false, nil
		})
		scanDone <- err
	}()

	select {
	case <-scanStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("scan did not start in time")
	}

	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- db.RebuildIndex()
	}()

	deadline := time.Now().Add(2 * time.Second)
	sawBusy := false
	for time.Now().Before(deadline) {
		_, err := db.Get(1)
		if errors.Is(err, ErrRebuildInProgress) {
			sawBusy = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !sawBusy {
		close(releaseScan)
		<-scanDone
		<-rebuildDone
		t.Fatal("expected rebuild to become active")
	}

	if err := db.Set(2, &Rec{Name: "bob", Age: 31}); !errors.Is(err, ErrRebuildInProgress) {
		close(releaseScan)
		<-scanDone
		<-rebuildDone
		t.Fatalf("expected ErrRebuildInProgress from Set during rebuild, got: %v", err)
	}

	close(releaseScan)
	if err := <-scanDone; err != nil {
		t.Fatalf("SeqScan: %v", err)
	}
	if err := <-rebuildDone; err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	if err := db.Set(2, &Rec{Name: "bob", Age: 31}); err != nil {
		t.Fatalf("Set(after rebuild): %v", err)
	}
}

func TestRebuildIndex_ConcurrentCloseReturnsErrClosed(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	scanStarted := make(chan struct{})
	releaseScan := make(chan struct{})
	scanDone := make(chan error, 1)

	go func() {
		err := db.SeqScan(0, func(_ uint64, _ *Rec) (bool, error) {
			close(scanStarted)
			<-releaseScan
			return false, nil
		})
		scanDone <- err
	}()

	select {
	case <-scanStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("scan did not start in time")
	}

	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- db.RebuildIndex()
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, err := db.Get(1)
		if errors.Is(err, ErrRebuildInProgress) {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- db.Close()
	}()

	closedDeadline := time.Now().Add(2 * time.Second)
	for !db.closed.Load() && time.Now().Before(closedDeadline) {
		time.Sleep(1 * time.Millisecond)
	}
	if !db.closed.Load() {
		close(releaseScan)
		<-scanDone
		<-closeDone
		<-rebuildDone
		t.Fatal("db.closed was not set by Close in time")
	}

	close(releaseScan)
	if err := <-scanDone; err != nil {
		t.Fatalf("SeqScan: %v", err)
	}

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close timed out")
	}

	select {
	case err := <-rebuildDone:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("expected ErrClosed from rebuild racing with close, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RebuildIndex timed out")
	}
}

func TestRebuildIndex_WaitsForInFlightBatchedSet(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 1024,
	})

	setStarted := make(chan struct{})
	releaseSet := make(chan struct{})
	setDone := make(chan error, 1)

	go func() {
		setDone <- db.Set(1, &Rec{Name: "alice", Age: 30}, BeforeCommit(func(_ *bbolt.Tx, _ uint64, _, _ *Rec) error {
			close(setStarted)
			<-releaseSet
			return nil
		}))
	}()

	select {
	case <-setStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("batched set callback did not start in time")
	}

	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- db.RebuildIndex()
	}()

	deadline := time.Now().Add(2 * time.Second)
	sawBusy := false
	for time.Now().Before(deadline) {
		_, err := db.Get(1)
		if errors.Is(err, ErrRebuildInProgress) {
			sawBusy = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !sawBusy {
		close(releaseSet)
		<-setDone
		<-rebuildDone
		t.Fatal("expected rebuild to become active while batched set is in flight")
	}

	select {
	case err := <-rebuildDone:
		close(releaseSet)
		<-setDone
		t.Fatalf("rebuild must wait for in-flight batched set, got early result: %v", err)
	default:
	}

	close(releaseSet)
	if err := <-setDone; err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := <-rebuildDone; err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	if st := db.AutoBatchStats(); st.Enqueued == 0 {
		t.Fatalf("expected auto-batch enqueue for Set path, got stats: %+v", st)
	}

	v, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v == nil || v.Age != 30 || v.Name != "alice" {
		t.Fatalf("unexpected value after rebuild: %#v", v)
	}

	ids, err := db.QueryKeys(qx.Query(qx.EQ("age", 30)))
	if err != nil {
		t.Fatalf("QueryKeys(age=30): %v", err)
	}
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("expected [1] after batched set + rebuild, got %v", ids)
	}
}

func TestRebuildIndex_StormConcurrentMixedOps_FinalConsistency(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   2 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 2048,
	})

	countries := []string{"NL", "PL", "DE", "US"}
	for i := 1; i <= 220; i++ {
		id := uint64(i)
		if err := db.Set(id, &Rec{
			Meta:     Meta{Country: countries[i%len(countries)]},
			Name:     fmt.Sprintf("seed-%03d", i),
			Age:      18 + (i % 60),
			Score:    float64(i%1000) / 10.0,
			Active:   i%2 == 0,
			Tags:     []string{fmt.Sprintf("grp-%d", i%7), "seed"},
			FullName: fmt.Sprintf("FN-%05d", i),
		}); err != nil {
			t.Fatalf("seed Set(%d): %v", i, err)
		}
	}

	const (
		writers   = 4
		readers   = 3
		writerOps = 220
		readerOps = 320
		rebuilds  = 48
	)

	queries := []*qx.QX{
		qx.Query(qx.GTE("age", 25)).Sort("age", qx.ASC).Offset(2).Limit(40),
		qx.Query(qx.NOT(qx.EQ("active", false))).SortBy(qx.LEN("tags"), qx.DESC).Offset(1).Limit(55),
		qx.Query(qx.HASANY("tags", []string{"seed", "w0", "w1", "grp-2"})).SortBy(qx.POS("country", []string{"NL", "DE", "PL", "US"}), qx.ASC).Limit(70),
		qx.Query(qx.PREFIX("name", "rw-")).Sort("name", qx.ASC).Limit(80),
		qx.Query(qx.IN("country", []string{"NL", "DE"})).SortBy(qx.POS("tags", []string{"seed", "w0", "grp-1"}), qx.DESC).Offset(3).Limit(60),
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 1+writers+readers)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rebuilds; i++ {
			if err := db.RebuildIndex(); err != nil {
				errCh <- fmt.Errorf("rebuild iter=%d: %w", i, err)
				return
			}
			if i%3 == 0 {
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	for w := 0; w < writers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < writerOps; i++ {
				id := uint64((w*997+i*31+i*i)%260 + 1)
				switch (w + i) % 3 {
				case 0:
					rec := &Rec{
						Meta:     Meta{Country: countries[(w+i)%len(countries)]},
						Name:     fmt.Sprintf("rw-%02d-%04d-%03d", w, i, id),
						Age:      18 + ((w*13 + i) % 65),
						Score:    float64((w*1000+i*17)%1200) / 10.0,
						Active:   (w+i)%2 == 0,
						Tags:     []string{fmt.Sprintf("w%d", w%4), fmt.Sprintf("grp-%d", i%7)},
						FullName: fmt.Sprintf("FN-%05d", id),
					}
					if err := db.Set(id, rec); err != nil && !errors.Is(err, ErrRebuildInProgress) {
						errCh <- fmt.Errorf("writer=%d Set(id=%d): %w", w, id, err)
						return
					}
				case 1:
					patch := []Field{
						{Name: "age", Value: 21 + ((w*7 + i) % 60)},
						{Name: "active", Value: i%2 == 0},
						{Name: "country", Value: countries[(w+i)%len(countries)]},
					}
					if err := db.Patch(id, patch); err != nil && !errors.Is(err, ErrRebuildInProgress) {
						errCh <- fmt.Errorf("writer=%d Patch(id=%d): %w", w, id, err)
						return
					}
				default:
					if err := db.Delete(id); err != nil && !errors.Is(err, ErrRebuildInProgress) {
						errCh <- fmt.Errorf("writer=%d Delete(id=%d): %w", w, id, err)
						return
					}
				}
			}
		}()
	}

	for r := 0; r < readers; r++ {
		r := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < readerOps; i++ {
				q := queries[(r+i)%len(queries)]
				if _, err := db.QueryKeys(q); err != nil && !errors.Is(err, ErrRebuildInProgress) {
					errCh <- fmt.Errorf("reader=%d QueryKeys(i=%d): %w", r, i, err)
					return
				}
				if _, err := db.Query(q); err != nil && !errors.Is(err, ErrRebuildInProgress) {
					errCh <- fmt.Errorf("reader=%d Query(i=%d): %w", r, i, err)
					return
				}
				if _, err := db.Count(q.Filter); err != nil && !errors.Is(err, ErrRebuildInProgress) {
					errCh <- fmt.Errorf("reader=%d Count(i=%d): %w", r, i, err)
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		t.FailNow()
	}

	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex(final): %v", err)
	}

	checkQueries := []*qx.QX{
		qx.Query(qx.GTE("age", 18)).Sort("age", qx.DESC).Offset(5).Limit(70),
		qx.Query(qx.HASANY("tags", []string{"w0", "w1", "grp-3", "seed"})).SortBy(qx.LEN("tags"), qx.ASC).Offset(4).Limit(90),
		qx.Query(qx.IN("country", []string{"NL", "DE", "US"})).SortBy(qx.POS("country", []string{"DE", "NL", "US", "PL"}), qx.ASC).Limit(120),
		qx.Query(qx.NOT(qx.EQ("active", false))).SortBy(qx.POS("tags", []string{"w0", "w1", "seed", "grp-1"}), qx.DESC).Offset(2).Limit(85),
	}
	for i, q := range checkQueries {
		got, err := db.QueryKeys(q)
		if err != nil {
			t.Fatalf("check QueryKeys(%d): %v", i, err)
		}
		want, err := expectedKeysUint64(t, db, q)
		if err != nil {
			t.Fatalf("check expectedKeysUint64(%d): %v", i, err)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("check mismatch(%d): got=%v want=%v q=%+v", i, got, want, q)
		}
	}

	var seqCount uint64
	if err := db.SeqScan(0, func(_ uint64, _ *Rec) (bool, error) {
		seqCount++
		return true, nil
	}); err != nil {
		t.Fatalf("SeqScan(final): %v", err)
	}
	cnt, err := db.Count()
	if err != nil {
		t.Fatalf("Count(): %v", err)
	}
	if cnt != seqCount {
		t.Fatalf("final count mismatch: count=%d seqscan=%d", cnt, seqCount)
	}
}

func TestRebuildIndex_StatsBlockUntilRebuildCompletes(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Age: 30, Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	scanStarted := make(chan struct{})
	releaseScan := make(chan struct{})
	scanDone := make(chan error, 1)
	go func() {
		scanDone <- db.SeqScan(0, func(_ uint64, _ *Rec) (bool, error) {
			close(scanStarted)
			<-releaseScan
			return false, nil
		})
	}()

	select {
	case <-scanStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("scan did not start in time")
	}

	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- db.RebuildIndex()
	}()

	deadline := time.Now().Add(2 * time.Second)
	sawBusy := false
	for time.Now().Before(deadline) {
		_, err := db.Get(1)
		if errors.Is(err, ErrRebuildInProgress) {
			sawBusy = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !sawBusy {
		close(releaseScan)
		<-scanDone
		<-rebuildDone
		t.Fatal("expected rebuild to become active")
	}

	statsDone := make(chan IndexStats, 1)
	go func() {
		statsDone <- db.IndexStats()
	}()

	select {
	case st := <-statsDone:
		close(releaseScan)
		<-scanDone
		<-rebuildDone
		t.Fatalf("Stats returned while rebuild still active: %+v", st)
	case <-time.After(80 * time.Millisecond):
	}

	close(releaseScan)
	if err := <-scanDone; err != nil {
		t.Fatalf("SeqScan: %v", err)
	}
	if err := <-rebuildDone; err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	select {
	case st := <-statsDone:
		if st.Size == 0 {
			t.Fatalf("expected IndexStats.Size > 0, got %+v", st)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("IndexStats did not return after rebuild completion")
	}
}

func TestRebuildIndex_RejectsCoreOpsWhileActive(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Age: 30, Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	scanStarted := make(chan struct{})
	releaseScan := make(chan struct{})
	scanDone := make(chan error, 1)
	go func() {
		scanDone <- db.SeqScan(0, func(_ uint64, _ *Rec) (bool, error) {
			close(scanStarted)
			<-releaseScan
			return false, nil
		})
	}()

	select {
	case <-scanStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("scan did not start in time")
	}

	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- db.RebuildIndex()
	}()

	deadline := time.Now().Add(2 * time.Second)
	sawBusy := false
	for time.Now().Before(deadline) {
		_, err := db.Get(1)
		if errors.Is(err, ErrRebuildInProgress) {
			sawBusy = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !sawBusy {
		close(releaseScan)
		<-scanDone
		<-rebuildDone
		t.Fatal("expected rebuild to become active")
	}

	expectBusy := func(op string, err error) {
		t.Helper()
		if !errors.Is(err, ErrRebuildInProgress) {
			t.Fatalf("%s: expected ErrRebuildInProgress, got: %v", op, err)
		}
	}

	_, err := db.Get(1)
	expectBusy("Get", err)

	_, err = db.BatchGet(1, 2)
	expectBusy("BatchGet", err)

	err = db.ScanKeys(0, func(_ uint64) (bool, error) {
		return true, nil
	})
	expectBusy("ScanKeys", err)

	err = db.SeqScan(0, func(_ uint64, _ *Rec) (bool, error) {
		return true, nil
	})
	expectBusy("SeqScan", err)

	err = db.SeqScanRaw(0, func(_ uint64, _ []byte) (bool, error) {
		return true, nil
	})
	expectBusy("SeqScanRaw", err)

	_, err = db.QueryKeys(qx.Query())
	expectBusy("QueryKeys", err)

	_, err = db.Query(qx.Query())
	expectBusy("Query", err)

	_, err = db.Count()
	expectBusy("Count", err)

	err = db.Set(2, &Rec{Name: "bob", Age: 31})
	expectBusy("Set", err)

	err = db.BatchSet(
		[]uint64{2, 3},
		[]*Rec{
			{Name: "setmany-2", Age: 21},
			{Name: "setmany-3", Age: 22},
		},
	)
	expectBusy("BatchSet", err)

	err = db.Patch(1, []Field{{Name: "age", Value: 35}})
	expectBusy("Patch", err)

	err = db.BatchPatch([]uint64{1, 2}, []Field{{Name: "age", Value: 37}})
	expectBusy("BatchPatch", err)

	err = db.Delete(1)
	expectBusy("Delete", err)

	err = db.BatchDelete([]uint64{1, 2})
	expectBusy("BatchDelete", err)

	err = db.Truncate()
	expectBusy("Truncate", err)

	err = db.RebuildIndex()
	expectBusy("RebuildIndex(second)", err)

	close(releaseScan)
	if err = <-scanDone; err != nil {
		t.Fatalf("SeqScan: %v", err)
	}
	if err = <-rebuildDone; err != nil {
		t.Fatalf("RebuildIndex(first): %v", err)
	}
}

/**/

type PtrIntRec struct {
	Name   string `db:"name"   rbi:"index"`
	Rank   *int   `db:"rank"   rbi:"index"`
	Active bool   `db:"active" rbi:"index"`
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
		qx.Query().Sort("opt", qx.ASC),
		qx.Query().Sort("opt", qx.DESC),
		qx.Query(qx.EQ("active", true)).Sort("opt", qx.ASC).Offset(1).Limit(2),
		qx.Query(qx.EQ("active", true)).Sort("opt", qx.DESC).Offset(1).Limit(2),
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
	opts := testOptions(Options{
		EnableAutoBatchStats: true,
		EnableSnapshotStats:  true,
	})

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

	check := func(stage string, wantAsc, wantDesc, wantNil, wantIn, wantGT []uint64, wantCountNil, wantCountIn uint64) {
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
		assertSameSet(t, gotGT, wantGT)

		gotAsc, err := db.QueryKeys(qx.Query().Sort("rank", qx.ASC))
		if err != nil {
			t.Fatalf("%s QueryKeys(By rank ASC): %v", stage, err)
		}
		assertSameSlice(t, gotAsc, wantAsc)

		gotDesc, err := db.QueryKeys(qx.Query().Sort("rank", qx.DESC))
		if err != nil {
			t.Fatalf("%s QueryKeys(By rank DESC): %v", stage, err)
		}
		assertSameSlice(t, gotDesc, wantDesc)

		gotAscPage, err := db.QueryKeys(qx.Query().Sort("rank", qx.ASC).Offset(1).Limit(3))
		if err != nil {
			t.Fatalf("%s QueryKeys(By rank ASC page): %v", stage, err)
		}
		assertSameSlice(t, gotAscPage, wantAsc[1:])

		cntNil, err := db.Count(qx.Query(qx.EQ("rank", nil)).Filter)
		if err != nil {
			t.Fatalf("%s Count(EQ nil): %v", stage, err)
		}
		if cntNil != wantCountNil {
			t.Fatalf("%s Count(EQ nil) mismatch: got=%d want=%d", stage, cntNil, wantCountNil)
		}

		cntIn, err := db.Count(qx.Query(qx.IN("rank", []any{nil, 10})).Filter)
		if err != nil {
			t.Fatalf("%s Count(IN nil,10): %v", stage, err)
		}
		if cntIn != wantCountIn {
			t.Fatalf("%s Count(IN nil,10) mismatch: got=%d want=%d", stage, cntIn, wantCountIn)
		}
	}

	check("base", []uint64{2, 3, 4, 1}, []uint64{4, 3, 2, 1}, []uint64{1}, []uint64{1, 3}, []uint64{3, 4}, 1, 2)

	if err := db.Patch(1, []Field{{Name: "rank", Value: 15}}); err != nil {
		t.Fatalf("Patch(1 rank=15): %v", err)
	}
	if err := db.Patch(2, []Field{{Name: "rank", Value: (*int)(nil)}}); err != nil {
		t.Fatalf("Patch(2 rank=nil): %v", err)
	}
	if err := db.Patch(4, []Field{{Name: "rank", Value: 5}}); err != nil {
		t.Fatalf("Patch(4 rank=5): %v", err)
	}

	check("delta", []uint64{4, 3, 1, 2}, []uint64{1, 3, 4, 2}, []uint64{2}, []uint64{2, 3}, []uint64{1, 3, 4}, 1, 2)

	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}
	check("rebuild", []uint64{4, 3, 1, 2}, []uint64{1, 3, 4, 2}, []uint64{2}, []uint64{2, 3}, []uint64{1, 3, 4}, 1, 2)

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
	defer func() {
		if err := raw.Close(); err != nil {
			t.Fatalf("raw.Close: %v", err)
		}
	}()

	reopened, err := New[uint64, PtrIntRec](raw, opts)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Fatalf("reopened.Close: %v", err)
		}
	}()
	db = reopened

	check("reopen", []uint64{4, 3, 1, 2}, []uint64{1, 3, 4, 2}, []uint64{2}, []uint64{2, 3}, []uint64{1, 3, 4}, 1, 2)
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
	).Sort("opt", qx.ASC).Limit(10)

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
	planOut, ok, err := db.engine.tryPlan(nq, nil)
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

	qLimit := qx.Query(qx.EQ("active", true)).Sort("opt", qx.ASC).Limit(3)
	limitLeaves := mustExtractAndLeaves(t, qLimit.Filter)
	out, used, err := db.engine.tryLimitQueryOrderBasic(qLimit, limitLeaves, nil)
	if err != nil {
		t.Fatalf("tryLimitQueryOrderBasic: %v", err)
	}
	want, err := expectedKeysUint64(t, db, qLimit)
	if err != nil {
		t.Fatalf("expectedKeysUint64(limit): %v", err)
	}
	if used {
		assertSameSlice(t, out, want)
	} else {
		got, err := db.QueryKeys(qLimit)
		if err != nil {
			t.Fatalf("QueryKeys(limit): %v", err)
		}
		assertSameSlice(t, got, want)
	}

	qOffset := qx.Query(qx.EQ("active", true)).Sort("opt", qx.ASC).Offset(1).Limit(2)
	out, used, err = db.engine.tryQueryOrderBasicWithLimit(qOffset, nil)
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

	qEqNilLimit := qx.Query(qx.EQ("opt", nil)).Sort("opt", qx.ASC).Limit(10)
	eqNilLeaves := mustExtractAndLeaves(t, qEqNilLimit.Filter)
	out, used, err = db.engine.tryLimitQueryOrderBasic(qEqNilLimit, eqNilLeaves, nil)
	if err != nil {
		t.Fatalf("tryLimitQueryOrderBasic(eq nil): %v", err)
	}
	want, err = expectedKeysUint64(t, db, qEqNilLimit)
	if err != nil {
		t.Fatalf("expectedKeysUint64(eq nil limit): %v", err)
	}
	if used {
		assertSameSlice(t, out, want)
	}
	planOut, used, err := db.engine.tryExecutionPlan(qEqNilLimit, nil)
	if err != nil {
		t.Fatalf("tryExecutionPlan(eq nil limit): %v", err)
	}
	if !used {
		t.Fatalf("expected tryExecutionPlan to use ORDER+LIMIT fast path for eq nil")
	}
	assertSameSlice(t, planOut, want)

	qPrefix := qx.Query(qx.PREFIX("opt", "")).Sort("opt", qx.ASC).Offset(1).Limit(2)
	out, used, err = db.engine.tryQueryOrderPrefixWithLimit(qPrefix, nil)
	if err != nil {
		t.Fatalf("tryQueryOrderPrefixWithLimit: %v", err)
	}
	want, err = expectedKeysUint64(t, db, qPrefix)
	if err != nil {
		t.Fatalf("expectedKeysUint64(prefix): %v", err)
	}
	if used {
		assertSameSlice(t, out, want)
	} else {
		got, err := db.QueryKeys(qPrefix)
		if err != nil {
			t.Fatalf("QueryKeys(prefix): %v", err)
		}
		assertSameSlice(t, got, want)
	}

	qPrefixDesc := qx.Query(qx.PREFIX("opt", "")).Sort("opt", qx.DESC).Offset(1).Limit(2)
	out, used, err = db.engine.tryQueryOrderPrefixWithLimit(qPrefixDesc, nil)
	if err != nil {
		t.Fatalf("tryQueryOrderPrefixWithLimit(desc): %v", err)
	}
	want, err = expectedKeysUint64(t, db, qPrefixDesc)
	if err != nil {
		t.Fatalf("expectedKeysUint64(prefix desc): %v", err)
	}
	if used {
		assertSameSlice(t, out, want)
	} else {
		got, err := db.QueryKeys(qPrefixDesc)
		if err != nil {
			t.Fatalf("QueryKeys(prefix desc): %v", err)
		}
		assertSameSlice(t, got, want)
	}
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

	q := qx.Query().Sort("rank", qx.ASC).Limit(2)
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
	).Sort("opt", qx.ASC).Offset(1).Limit(3))

	preparedQ, viewQ, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer preparedQ.Release()

	leaves := mustExtractAndLeaves(t, q.Filter)
	window, _ := orderWindow(&viewQ)
	preds, ok := db.engine.buildPredicatesOrderedWithMode(leaves, "opt", false, window, q.Window.Offset, true, true)
	if !ok {
		t.Fatalf("buildPredicatesOrderedWithMode: ok=false")
	}
	defer releasePredicates(preds)

	got, ok := db.engine.execPlanOrderedBasic(q, preds, nil)
	if !ok {
		t.Fatalf("execPlanOrderedBasic: ok=false")
	}
	want, err := db.engine.currentQueryViewForTests().execPreparedQuery(&viewQ)
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
	).Sort("opt", qx.DESC).Offset(1).Limit(2))

	got, ok, err := db.engine.tryPlan(q, nil)
	if err != nil {
		t.Fatalf("tryPlan: %v", err)
	}
	if !ok {
		t.Fatalf("expected tryPlan to use ordered planner path for pointer sort field")
	}
	preparedQ, viewQ, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer preparedQ.Release()

	want, err := db.engine.currentQueryViewForTests().execPreparedQuery(&viewQ)
	if err != nil {
		t.Fatalf("execPreparedQuery: %v", err)
	}
	assertSameSlice(t, got, want)
}

/**/

func indexExtBatchSetGenerated(t *testing.T, db *DB[uint64, Rec], start, end int, gen func(i int) *Rec) {
	t.Helper()
	if start > end {
		return
	}

	batchIDs := make([]uint64, 0, min(128, end-start+1))
	batchVals := make([]*Rec, 0, cap(batchIDs))
	flush := func() {
		if len(batchIDs) == 0 {
			return
		}
		if err := db.BatchSet(batchIDs, batchVals); err != nil {
			t.Fatalf("BatchSet(start=%d,end=%d,size=%d): %v", start, end, len(batchIDs), err)
		}
		batchIDs = batchIDs[:0]
		batchVals = batchVals[:0]
	}

	for i := start; i <= end; i++ {
		batchIDs = append(batchIDs, uint64(i))
		batchVals = append(batchVals, gen(i))
		if len(batchIDs) == cap(batchIDs) {
			flush()
		}
	}
	flush()
}

func indexExtAssertQueryKeysExpected(t *testing.T, db *DB[uint64, Rec], q *qx.QX) []uint64 {
	t.Helper()
	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertSameSlice(t, got, want)
	return want
}

func indexExtAssertCountExpected(t *testing.T, db *DB[uint64, Rec], q *qx.QX) {
	t.Helper()
	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	got, err := db.Count(q.Filter)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if got != uint64(len(want)) {
		t.Fatalf("Count mismatch: got=%d want=%d", got, len(want))
	}
}

func indexExtFloatSignedZeroDB(t *testing.T) *DB[uint64, Rec] {
	t.Helper()

	db, _ := openTempDBUint64(t)
	indexExtBatchSetGenerated(t, db, 1, 420, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("fzero/%03d", i),
			Age:    i,
			Score:  float64(i + 10),
			Active: i%2 == 0,
		}
	})

	for _, tc := range []struct {
		id    uint64
		score float64
	}{
		{id: 1, score: 0.0},
		{id: 2, score: math.Copysign(0, -1)},
		{id: 3, score: -1.0},
		{id: 4, score: 1.0},
	} {
		if err := db.Patch(tc.id, []Field{{Name: "score", Value: tc.score}}); err != nil {
			t.Fatalf("Patch(score %d): %v", tc.id, err)
		}
	}

	return db
}

func indexExtFloatNaNDB(t *testing.T) *DB[uint64, Rec] {
	t.Helper()

	db, _ := openTempDBUint64(t)
	indexExtBatchSetGenerated(t, db, 1, 420, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("fnan/%03d", i),
			Age:    i,
			Score:  float64(i + 1000),
			Active: i%2 == 0,
		}
	})

	for _, tc := range []struct {
		id    uint64
		score float64
	}{
		{id: 1, score: math.NaN()},
		{id: 2, score: math.Inf(-1)},
		{id: 3, score: -1.0},
		{id: 4, score: 0.0},
		{id: 5, score: 1.0},
		{id: 6, score: math.Inf(1)},
	} {
		if err := db.Patch(tc.id, []Field{{Name: "score", Value: tc.score}}); err != nil {
			t.Fatalf("Patch(score %d): %v", tc.id, err)
		}
	}

	return db
}

func indexExtNumericCoercionDB(t *testing.T) *DB[uint64, Rec] {
	t.Helper()

	db, _ := openTempDBUint64(t)
	indexExtBatchSetGenerated(t, db, 1, 420, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("num/%03d", i),
			Age:    i,
			Score:  float64(i),
			Active: i%2 == 0,
		}
	})

	return db
}

func TestIndexExt_DBQueryAfterDistinctGrowthMatchesExpected(t *testing.T) {
	db, _ := openTempDBUint64(t)

	indexExtBatchSetGenerated(t, db, 1, 300, func(i int) *Rec {
		return &Rec{Name: fmt.Sprintf("prom/%03d", i), Age: i}
	})
	indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.GTE("age", 0)).Sort("age", qx.ASC))

	indexExtBatchSetGenerated(t, db, 301, 420, func(i int) *Rec {
		return &Rec{Name: fmt.Sprintf("prom/%03d", i), Age: i}
	})
	indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.GTE("age", 0)).Sort("age", qx.ASC))
}

func TestIndexExt_DBQueryAfterDistinctCollapseMatchesExpected(t *testing.T) {
	db, _ := openTempDBUint64(t)

	indexExtBatchSetGenerated(t, db, 1, 420, func(i int) *Rec {
		return &Rec{Name: fmt.Sprintf("dem/%03d", i), Age: i}
	})
	indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.GTE("age", 0)).Sort("age", qx.ASC))

	for i := 1; i <= 50; i++ {
		if err := db.Delete(uint64(i)); err != nil {
			t.Fatalf("Delete(%d): %v", i, err)
		}
	}
	indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.GTE("age", 0)).Sort("age", qx.ASC))
}

func TestIndexExt_DBPrefixQueryAfterSetPatchDeleteMatchesExpected(t *testing.T) {
	db, _ := openTempDBUint64(t)

	indexExtBatchSetGenerated(t, db, 1, 450, func(i int) *Rec {
		name := fmt.Sprintf("b/%03d", i)
		switch {
		case i <= 170:
			name = fmt.Sprintf("aa/%03d", i)
		case i <= 330:
			name = fmt.Sprintf("ab/%03d", i)
		}
		return &Rec{Name: name, Age: i}
	})

	for _, id := range []uint64{171, 172, 173, 331, 332, 333} {
		if err := db.Patch(id, []Field{{Name: "name", Value: fmt.Sprintf("aa/mut/%03d", id)}}); err != nil {
			t.Fatalf("Patch(%d): %v", id, err)
		}
	}
	for _, id := range []uint64{10, 11, 12, 13, 14, 15} {
		if err := db.Delete(id); err != nil {
			t.Fatalf("Delete(%d): %v", id, err)
		}
	}
	indexExtBatchSetGenerated(t, db, 451, 470, func(i int) *Rec {
		return &Rec{Name: fmt.Sprintf("aa/new/%03d", i), Age: i}
	})

	q := qx.Query(qx.PREFIX("name", "aa/")).Sort("name", qx.ASC)
	indexExtAssertQueryKeysExpected(t, db, q)
}

func TestIndexExt_DBRangeQueryAfterBoundaryPatchesMatchesExpected(t *testing.T) {
	db, _ := openTempDBUint64(t)

	indexExtBatchSetGenerated(t, db, 1, 500, func(i int) *Rec {
		return &Rec{Name: fmt.Sprintf("rng/%03d", i), Age: i}
	})

	for _, tc := range []struct {
		id  uint64
		age int
	}{
		{id: 50, age: 200},
		{id: 51, age: 199},
		{id: 52, age: 350},
		{id: 53, age: 349},
		{id: 54, age: 200},
		{id: 55, age: 349},
	} {
		if err := db.Patch(tc.id, []Field{{Name: "age", Value: tc.age}}); err != nil {
			t.Fatalf("Patch(%d): %v", tc.id, err)
		}
	}

	q := qx.Query(qx.GTE("age", 200), qx.LT("age", 350)).Sort("age", qx.ASC)
	indexExtAssertQueryKeysExpected(t, db, q)
}

func TestIndexExt_DBCountAfterBoundaryPatchesAndDeletesMatchesExpected(t *testing.T) {
	db, _ := openTempDBUint64(t)

	indexExtBatchSetGenerated(t, db, 1, 500, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("cnt/%03d", i),
			Age:    1000 + i,
			Active: i%3 == 0,
		}
	})

	for _, id := range []uint64{110, 111, 112, 113, 114, 115} {
		if err := db.Delete(id); err != nil {
			t.Fatalf("Delete(%d): %v", id, err)
		}
	}
	for _, tc := range []struct {
		id     uint64
		age    int
		active bool
	}{
		{id: 30, age: 1100, active: true},
		{id: 31, age: 1099, active: true},
		{id: 32, age: 1260, active: true},
		{id: 33, age: 1259, active: false},
		{id: 34, age: 1180, active: true},
	} {
		if err := db.Patch(tc.id, []Field{
			{Name: "age", Value: tc.age},
			{Name: "active", Value: tc.active},
		}); err != nil {
			t.Fatalf("Patch(%d): %v", tc.id, err)
		}
	}

	q := qx.Query(qx.GTE("age", 1100), qx.LT("age", 1260), qx.EQ("active", true))
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBOrderAscAfterChurnMatchesExpected(t *testing.T) {
	db, _ := openTempDBUint64(t)

	indexExtBatchSetGenerated(t, db, 1, 500, func(i int) *Rec {
		return &Rec{
			Name: fmt.Sprintf("orda/%03d", i),
			Age:  i % 40,
		}
	})

	for _, tc := range []struct {
		id  uint64
		age int
	}{
		{id: 7, age: 0},
		{id: 8, age: 0},
		{id: 9, age: 99},
		{id: 10, age: 99},
		{id: 191, age: 17},
		{id: 192, age: 17},
		{id: 193, age: 17},
	} {
		if err := db.Patch(tc.id, []Field{{Name: "age", Value: tc.age}}); err != nil {
			t.Fatalf("Patch(%d): %v", tc.id, err)
		}
	}
	for _, id := range []uint64{44, 45, 46, 47, 48} {
		if err := db.Delete(id); err != nil {
			t.Fatalf("Delete(%d): %v", id, err)
		}
	}

	q := qx.Query(qx.PREFIX("name", "orda/")).Sort("age", qx.ASC)
	indexExtAssertQueryKeysExpected(t, db, q)
}

func TestIndexExt_DBOrderDescLimitOffsetAfterChurnMatchesExpected(t *testing.T) {
	db, _ := openTempDBUint64(t)

	indexExtBatchSetGenerated(t, db, 1, 520, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("ordd/%03d", i),
			Age:    100 + i%70,
			Active: i%2 == 0,
		}
	})

	for _, tc := range []struct {
		id     uint64
		age    int
		active bool
	}{
		{id: 5, age: 500, active: true},
		{id: 6, age: 499, active: true},
		{id: 7, age: 101, active: false},
		{id: 8, age: 500, active: true},
		{id: 250, age: 450, active: true},
		{id: 251, age: 450, active: true},
	} {
		if err := db.Patch(tc.id, []Field{
			{Name: "age", Value: tc.age},
			{Name: "active", Value: tc.active},
		}); err != nil {
			t.Fatalf("Patch(%d): %v", tc.id, err)
		}
	}
	for _, id := range []uint64{60, 61, 62, 63, 64, 65, 66} {
		if err := db.Delete(id); err != nil {
			t.Fatalf("Delete(%d): %v", id, err)
		}
	}

	q := qx.Query(qx.EQ("active", true)).Sort("age", qx.DESC).Offset(17).Limit(61)
	indexExtAssertQueryKeysExpected(t, db, q)
}

func TestIndexExt_DBNilPointerTransitionsPreserveIndexes(t *testing.T) {
	db, _ := openTempDBUint64(t)

	indexExtBatchSetGenerated(t, db, 1, 500, func(i int) *Rec {
		var opt *string
		if i%7 != 0 {
			s := fmt.Sprintf("opt-%03d", i)
			opt = &s
		}
		return &Rec{
			Name: fmt.Sprintf("opt/%03d", i),
			Opt:  opt,
		}
	})

	for _, id := range []uint64{7, 14, 21, 28, 35, 42} {
		v := fmt.Sprintf("opt-hot-%03d", id)
		if err := db.Patch(id, []Field{{Name: "opt", Value: v}}); err != nil {
			t.Fatalf("Patch(%d nil->value): %v", id, err)
		}
	}
	for _, id := range []uint64{1, 2, 3, 4, 5, 6} {
		if err := db.Patch(id, []Field{{Name: "opt", Value: (*string)(nil)}}); err != nil {
			t.Fatalf("Patch(%d value->nil): %v", id, err)
		}
	}

	indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.PREFIX("opt", "opt-hot-")).Sort("opt", qx.ASC))
	indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.EQ("opt", nil)))
}

func TestIndexExt_DBSliceReplaceRemovesStaleTermsAndLenBuckets(t *testing.T) {
	db, _ := openTempDBUint64(t)

	indexExtBatchSetGenerated(t, db, 1, 450, func(i int) *Rec {
		return &Rec{
			Name: fmt.Sprintf("tag/%03d", i),
			Tags: []string{"shared", fmt.Sprintf("tag-%03d", i)},
		}
	})

	for i := 1; i <= 30; i++ {
		if err := db.Patch(uint64(i), []Field{{Name: "tags", Value: []string{}}}); err != nil {
			t.Fatalf("Patch(empty %d): %v", i, err)
		}
	}
	for i := 31; i <= 60; i++ {
		if err := db.Patch(uint64(i), []Field{{Name: "tags", Value: []string{"hot", "shared", "hot"}}}); err != nil {
			t.Fatalf("Patch(hot %d): %v", i, err)
		}
	}
	for i := 61; i <= 75; i++ {
		if err := db.Patch(uint64(i), []Field{{Name: "tags", Value: []string{"solo"}}}); err != nil {
			t.Fatalf("Patch(solo %d): %v", i, err)
		}
	}
	for _, id := range []uint64{76, 77, 78, 79, 80} {
		if err := db.Delete(id); err != nil {
			t.Fatalf("Delete(%d): %v", id, err)
		}
	}

	indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.HASANY("tags", []string{"hot"})))
	indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.HASALL("tags", []string{"shared", "hot"})))
	indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.HASANY("tags", []string{"tag-005"})))

	var wantZero []uint64
	for i := 1; i <= 30; i++ {
		wantZero = append(wantZero, uint64(i))
	}
	gotZero, err := db.QueryKeys(qx.Query(qx.EQ("tags", []string{})))
	if err != nil {
		t.Fatalf("QueryKeys(empty tags): %v", err)
	}
	assertSameSlice(t, gotZero, wantZero)
}

func TestIndexExt_SnapshotQueryStableDuringConcurrentWrites(t *testing.T) {
	db, _ := openTempDBUint64(t)

	indexExtBatchSetGenerated(t, db, 1, 420, func(i int) *Rec {
		name := fmt.Sprintf("zz/%03d", i)
		if i <= 180 {
			name = fmt.Sprintf("aa/%03d", i)
		} else if i <= 320 {
			name = fmt.Sprintf("ab/%03d", i)
		}
		return &Rec{Name: name, Age: i}
	})

	snap := db.engine.getSnapshot()
	pinnedSnap, pinnedRef, ok := db.engine.snapshot.pinRefBySeq(snap.seq)
	if !ok {
		t.Fatalf("pinSnapshotRefBySeq(%d): false", snap.seq)
	}
	defer db.engine.snapshot.unpinRef(snap.seq, pinnedRef)
	snap = pinnedSnap

	prepared, viewQ, err := prepareTestQuery(db.engine, qx.Query(qx.PREFIX("name", "aa/")).Sort("name", qx.ASC))
	if err != nil {
		t.Fatalf("prepareTestQuery: %v", err)
	}
	defer prepared.Release()

	view := db.engine.makeQueryView(snap)
	wantKeys, err := view.execQuery(&viewQ, false, false)
	db.engine.releaseQueryView(view)
	if err != nil {
		t.Fatalf("pinned snapshot query: %v", err)
	}

	var failed atomic.Pointer[string]
	setFailed := func(msg string) {
		if failed.Load() != nil {
			return
		}
		copyMsg := msg
		failed.CompareAndSwap(nil, &copyMsg)
	}

	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				view := db.engine.makeQueryView(snap)
				gotKeys, err := view.execQuery(&viewQ, false, false)
				db.engine.releaseQueryView(view)
				if err != nil {
					setFailed(fmt.Sprintf("pinned snapshot query: %v", err))
					return
				}
				if !slices.Equal(gotKeys, wantKeys) {
					setFailed("old snapshot query changed under concurrent writes")
					return
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			id := uint64(i%420 + 1)
			if err := db.Patch(id, []Field{{Name: "name", Value: fmt.Sprintf("mut/%03d/%03d", i, id)}}); err != nil {
				setFailed(fmt.Sprintf("Patch(%d): %v", id, err))
				return
			}
		}
	}()

	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}
}

func TestIndexExt_DuplicateIDBatchPatchNetDiffKeepsIndexesConsistent(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	indexExtBatchSetGenerated(t, db, 1, 420, func(i int) *Rec {
		opt := fmt.Sprintf("seed-opt/%03d", i)
		return &Rec{
			Name: fmt.Sprintf("seed/%03d", i),
			Age:  i,
			Tags: []string{"shared", fmt.Sprintf("seed-tag/%03d", i)},
			Opt:  &opt,
		}
	})

	var step atomic.Int32
	err := db.BatchPatch(
		[]uint64{77, 77, 77},
		[]Field{{Name: "age", Value: 9_001}},
		BeforeProcess(func(_ uint64, v *Rec) error {
			switch step.Add(1) {
			case 1:
				v.Name = "dup/first"
				v.Age = 9_001
				v.Tags = []string{"first", "shared", "shared"}
				v.Opt = nil
			case 2:
				live := "dup-live"
				v.Name = "dup/second"
				v.Age = 9_002
				v.Tags = []string{}
				v.Opt = &live
			case 3:
				v.Name = "dup/final"
				v.Age = 9_003
				v.Tags = []string{"final", "shared", "final"}
				v.Opt = nil
			default:
				t.Fatalf("unexpected duplicate patch step")
			}
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("BatchPatch duplicate ids: %v", err)
	}

	assertState := func(stage string) {
		t.Helper()

		got, err := db.Get(77)
		if err != nil {
			t.Fatalf("%s Get(77): %v", stage, err)
		}
		if got == nil {
			t.Fatalf("%s Get(77): nil", stage)
		}
		if got.Name != "dup/final" || got.Age != 9_003 || !slices.Equal(got.Tags, []string{"final", "shared", "final"}) || got.Opt != nil {
			t.Fatalf("%s unexpected final value: %#v", stage, got)
		}

		indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.EQ("name", "dup/final")))
		indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.EQ("name", "seed/077")))
		indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.EQ("name", "dup/first")))
		indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.EQ("name", "dup/second")))
		indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.HASANY("tags", []string{"final"})))
		indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.HASANY("tags", []string{"seed-tag/077"})))
		indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.HASANY("tags", []string{"first"})))
		indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.EQ("opt", nil)))
		indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.EQ("opt", "seed-opt/077")))
		indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.EQ("opt", "dup-live")))

		gotEmptyTags, err := db.QueryKeys(qx.Query(qx.EQ("tags", []string{})))
		if err != nil {
			t.Fatalf("%s QueryKeys(empty tags): %v", stage, err)
		}
		assertSameSlice(t, gotEmptyTags, nil)
	}

	assertState("incremental")

	if err := db.buildIndex(nil, nil); err != nil {
		t.Fatalf("buildIndex: %v", err)
	}
	assertState("rebuilt")
}

func TestIndexExt_DBFloatSignedZeroBetweenBoundsMatchesExpected(t *testing.T) {
	db := indexExtFloatSignedZeroDB(t)

	q := qx.Query(
		qx.GTE("score", 0.0),
		qx.LTE("score", 0.0),
	).Sort("score", qx.ASC)

	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBFloatSignedZeroOrderAscMatchesExpected(t *testing.T) {
	db := indexExtFloatSignedZeroDB(t)

	q := qx.Query(
		qx.GTE("score", -1.0),
	).Sort("score", qx.ASC).Limit(8)

	indexExtAssertQueryKeysExpected(t, db, q)
}

func TestIndexExt_DBFloatSignedZeroEqualityMatchesExpected(t *testing.T) {
	db := indexExtFloatSignedZeroDB(t)

	q := qx.Query(qx.EQ("score", 0.0))
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBFloatSignedZeroINMatchesExpected(t *testing.T) {
	db := indexExtFloatSignedZeroDB(t)

	q := qx.Query(qx.IN("score", []float64{0.0}))
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBFloatNegativeZeroEqualityMatchesExpected(t *testing.T) {
	db := indexExtFloatSignedZeroDB(t)

	q := qx.Query(qx.EQ("score", math.Copysign(0, -1)))
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBFloatSignedZeroNotEqualCountMatchesExpected(t *testing.T) {
	db := indexExtFloatSignedZeroDB(t)

	q := qx.Query(qx.NOT(qx.EQ("score", 0.0)))
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBFloatNaNEqualityMatchesExpected(t *testing.T) {
	db := indexExtFloatNaNDB(t)

	q := qx.Query(qx.EQ("score", math.NaN()))
	indexExtAssertQueryKeysExpected(t, db, q)
}

func TestIndexExt_DBFloatNaNCountMatchesExpected(t *testing.T) {
	db := indexExtFloatNaNDB(t)

	q := qx.Query(qx.EQ("score", math.NaN()))
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBFloatNaNLessEqualMatchesExpected(t *testing.T) {
	db := indexExtFloatNaNDB(t)

	q := qx.Query(qx.LTE("score", math.NaN())).Sort("score", qx.ASC).Limit(16)
	indexExtAssertQueryKeysExpected(t, db, q)
}

func TestIndexExt_DBFloatNaNGreaterEqualMatchesExpected(t *testing.T) {
	db := indexExtFloatNaNDB(t)

	q := qx.Query(qx.GTE("score", math.NaN())).Sort("score", qx.ASC).Limit(16)
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBFloatNaNNotEqualCountMatchesExpected(t *testing.T) {
	db := indexExtFloatNaNDB(t)

	q := qx.Query(qx.NOT(qx.EQ("score", math.NaN())))
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBFloatNaNINMatchesExpected(t *testing.T) {
	db := indexExtFloatNaNDB(t)

	q := qx.Query(qx.IN("score", []float64{math.NaN(), 1.0}))
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBIntFieldFloatEqualityMatchesExpected(t *testing.T) {
	db := indexExtNumericCoercionDB(t)

	q := qx.Query(qx.EQ("age", 42.0))
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBIntFieldFloatRangeMatchesExpected(t *testing.T) {
	db := indexExtNumericCoercionDB(t)

	q := qx.Query(qx.GTE("age", 200.0), qx.LT("age", 205.0)).Sort("age", qx.ASC)
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBFloatFieldIntEqualityMatchesExpected(t *testing.T) {
	db := indexExtNumericCoercionDB(t)

	q := qx.Query(qx.EQ("score", 42))
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBFloatFieldIntRangeMatchesExpected(t *testing.T) {
	db := indexExtNumericCoercionDB(t)

	q := qx.Query(qx.GTE("score", 200), qx.LT("score", 205)).Sort("score", qx.ASC)
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBIntFieldUintEqualityMatchesExpected(t *testing.T) {
	db := indexExtNumericCoercionDB(t)

	q := qx.Query(qx.EQ("age", uint64(42)))
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBIntFieldUintRangeMatchesExpected(t *testing.T) {
	db := indexExtNumericCoercionDB(t)

	q := qx.Query(qx.GTE("age", uint64(200)), qx.LT("age", uint64(205))).Sort("age", qx.ASC)
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBIntFieldBinaryStringEqualityMatchesExpected(t *testing.T) {
	db := indexExtNumericCoercionDB(t)

	q := qx.Query(qx.EQ("age", keycodec.Int64ByteString(42)))
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBFloatFieldBinaryStringEqualityMatchesExpected(t *testing.T) {
	db := indexExtNumericCoercionDB(t)

	q := qx.Query(qx.EQ("score", keycodec.Float64ByteString(42.0)))
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

/**/

func postingConsumerExpected(ids posting.List) []uint64 {
	return ids.ToArray()
}
