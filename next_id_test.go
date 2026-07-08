package rbi

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/vapstack/monotime"
	"github.com/vapstack/rbi/internal/persist"
	"github.com/vapstack/rbi/rbierrors"
	"go.etcd.io/bbolt"
)

func TestNextIDUint64IncludesNodeAndIncrements(t *testing.T) {
	c, bolt := openBoltAndCollection[uint64, Rec](t, filepath.Join(t.TempDir(), "nextid_uint.db"), Options{
		NodeID: 0x1234,
	})
	defer func() {
		_ = c.Close()
		_ = bolt.Close()
	}()

	tx := BeginUpdate()
	defer tx.Release()

	id1, err := c.NextID(tx)
	if err != nil {
		t.Fatalf("NextID #1: %v", err)
	}
	id2, err := c.NextID(tx)
	if err != nil {
		t.Fatalf("NextID #2: %v", err)
	}
	if id1>>autoUintSeqBits != 0x1234 || id1&autoUintSeqMask != 1 {
		t.Fatalf("id1=%#x, want node 0x1234 seq 1", id1)
	}
	if id2 != id1+1 {
		t.Fatalf("id2=%#x want id1+1=%#x", id2, id1+1)
	}
	if err = c.Set(tx, id1, &Rec{Name: "alice"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	watermark := readNextIDUintWatermark(t, bolt, c.collection)
	if watermark < id2 {
		t.Fatalf("watermark=%#x lower than issued id2=%#x", watermark, id2)
	}
}

func TestNextIDUint64RollbackDoesNotPersistReservation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nextid_rollback.db")
	opts := Options{BucketName: "nextid_rollback", NodeID: 9}

	c, bolt := openBoltAndCollection[uint64, Rec](t, path, opts)
	tx := BeginUpdate()
	id1, err := c.NextID(tx)
	if err != nil {
		t.Fatalf("NextID before rollback: %v", err)
	}
	tx.Release()
	if err = c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err = bolt.Close(); err != nil {
		t.Fatalf("bolt Close: %v", err)
	}

	c, bolt = openBoltAndCollection[uint64, Rec](t, path, opts)
	defer func() {
		_ = c.Close()
		_ = bolt.Close()
	}()
	tx = BeginUpdate()
	defer tx.Release()
	id2, err := c.NextID(tx)
	if err != nil {
		t.Fatalf("NextID after reopen: %v", err)
	}
	if id2 != id1 {
		t.Fatalf("id after rollback/reopen=%#x want original uncommitted id=%#x", id2, id1)
	}
}

func TestNextIDUint64CommittedSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nextid_reopen.db")
	opts := Options{BucketName: "nextid_reopen", NodeID: 3}

	c, bolt := openBoltAndCollection[uint64, Rec](t, path, opts)
	tx := BeginUpdate()
	id1, err := c.NextID(tx)
	if err != nil {
		t.Fatalf("NextID before commit: %v", err)
	}
	if err = c.Set(tx, id1, &Rec{Name: "persisted"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	tx.Release()
	if err = c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err = bolt.Close(); err != nil {
		t.Fatalf("bolt Close: %v", err)
	}

	c, bolt = openBoltAndCollection[uint64, Rec](t, path, opts)
	defer func() {
		_ = c.Close()
		_ = bolt.Close()
	}()
	tx = BeginUpdate()
	defer tx.Release()
	id2, err := c.NextID(tx)
	if err != nil {
		t.Fatalf("NextID after reopen: %v", err)
	}
	if id2 <= id1 {
		t.Fatalf("id after reopen=%#x want greater than committed id=%#x", id2, id1)
	}
	if got, err := readGet(c, id1); err != nil || got == nil {
		t.Fatalf("committed record after reopen = %#v/%v", got, err)
	}
}

func TestNextIDUint64MissingMetadataUsesMaxKeyInNodeNamespace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nextid_scan.db")
	opts := Options{BucketName: "nextid_scan", NodeID: 5}
	nodeKey := uint64(opts.NodeID) << autoUintSeqBits
	otherNodeKey := uint64(opts.NodeID+1) << autoUintSeqBits

	c, bolt := openBoltAndCollection[uint64, Rec](t, path, opts)
	if err := writeSet(c, nodeKey|10, &Rec{Name: "node"}); err != nil {
		t.Fatalf("Set node key: %v", err)
	}
	if err := writeSet(c, otherNodeKey|1000, &Rec{Name: "other"}); err != nil {
		t.Fatalf("Set other node key: %v", err)
	}
	if got := readNextIDAutoValue(t, bolt, rootAutoUintKeyKind, c.collection); got != nil {
		t.Fatalf("automatic metadata exists before NextID: %x", got)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := bolt.Close(); err != nil {
		t.Fatalf("bolt Close: %v", err)
	}

	c, bolt = openBoltAndCollection[uint64, Rec](t, path, opts)
	defer func() {
		_ = c.Close()
		_ = bolt.Close()
	}()
	tx := BeginUpdate()
	defer tx.Release()
	id, err := c.NextID(tx)
	if err != nil {
		t.Fatalf("NextID: %v", err)
	}
	if want := nodeKey | 11; id != want {
		t.Fatalf("NextID=%#x want max node key + 1 = %#x", id, want)
	}
}

func TestNextIDUint64NodeZeroMissingMetadataUsesMaxKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nextid_node_zero_scan.db")
	opts := Options{BucketName: "nextid_node_zero_scan", NodeID: 0}
	highKey := uint64(3) << autoUintSeqBits

	c, bolt := openBoltAndCollection[uint64, Rec](t, path, opts)
	if err := writeSet(c, highKey|10, &Rec{Name: "high"}); err != nil {
		t.Fatalf("Set high key: %v", err)
	}
	if got := readNextIDAutoValue(t, bolt, rootAutoUintKeyKind, c.collection); got != nil {
		t.Fatalf("automatic metadata exists before NextID: %x", got)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := bolt.Close(); err != nil {
		t.Fatalf("bolt Close: %v", err)
	}

	c, bolt = openBoltAndCollection[uint64, Rec](t, path, opts)
	defer func() {
		_ = c.Close()
		_ = bolt.Close()
	}()
	tx := BeginUpdate()
	defer tx.Release()
	id, err := c.NextID(tx)
	if err != nil {
		t.Fatalf("NextID: %v", err)
	}
	if want := highKey | 11; id != want {
		t.Fatalf("NextID=%#x want max key + 1 = %#x", id, want)
	}
}

func TestNextIDMetadataUIDMismatchIgnoredAfterBucketRecreate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nextid_uid.db")
	opts := Options{BucketName: "nextid_uid", NodeID: 6}
	nodeKey := uint64(opts.NodeID) << autoUintSeqBits

	c, bolt := openBoltAndCollection[uint64, Rec](t, path, opts)
	tx := BeginUpdate()
	id1, err := c.NextID(tx)
	if err != nil {
		t.Fatalf("NextID before recreate: %v", err)
	}
	if err = c.Set(tx, id1, &Rec{Name: "old"}); err != nil {
		t.Fatalf("Set before recreate: %v", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatalf("Commit before recreate: %v", err)
	}
	tx.Release()
	if err = c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err = bolt.Close(); err != nil {
		t.Fatalf("bolt Close: %v", err)
	}

	raw, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("raw reopen: %v", err)
	}
	if err = raw.Update(func(tx *bbolt.Tx) error {
		return tx.DeleteBucket([]byte(opts.BucketName))
	}); err != nil {
		_ = raw.Close()
		t.Fatalf("DeleteBucket: %v", err)
	}
	if err = raw.Close(); err != nil {
		t.Fatalf("raw Close: %v", err)
	}

	c, bolt = openBoltAndCollection[uint64, Rec](t, path, opts)
	defer func() {
		_ = c.Close()
		_ = bolt.Close()
	}()
	tx = BeginUpdate()
	defer tx.Release()
	id2, err := c.NextID(tx)
	if err != nil {
		t.Fatalf("NextID after recreate: %v", err)
	}
	if want := nodeKey | 1; id2 != want {
		t.Fatalf("NextID after recreate=%#x want fresh id=%#x", id2, want)
	}
}

func TestNextIDUint64OverflowReturnsError(t *testing.T) {
	c, bolt := openBoltAndCollection[uint64, Rec](t, filepath.Join(t.TempDir(), "nextid_overflow.db"), Options{
		NodeID: 11,
	})
	defer func() {
		_ = c.Close()
		_ = bolt.Close()
	}()
	c.autoUint.Store(uint64(c.options.NodeID)<<autoUintSeqBits | autoUintSeqMask)

	tx := BeginUpdate()
	defer tx.Release()
	if _, err := c.NextID(tx); !errors.Is(err, rbierrors.ErrSequenceExhausted) {
		t.Fatalf("NextID overflow err=%v want %v", err, rbierrors.ErrSequenceExhausted)
	}
}

func TestNextIDUint64NodeZeroAllowsSequenceAbove48Bits(t *testing.T) {
	c, bolt := openBoltAndCollection[uint64, Rec](t, filepath.Join(t.TempDir(), "nextid_node_zero_48.db"), Options{
		NodeID: 0,
	})
	defer func() {
		_ = c.Close()
		_ = bolt.Close()
	}()

	c.autoUint.Store(autoUintSeqMask)
	tx := BeginUpdate()
	defer tx.Release()
	id, err := c.NextID(tx)
	if err != nil {
		t.Fatalf("NextID: %v", err)
	}
	if want := autoUintSeqMask + 1; id != want {
		t.Fatalf("NextID node zero=%#x want %#x", id, want)
	}
}

func TestNextIDUint64NodeZeroOverflowReturnsErrorAtMaxUint64(t *testing.T) {
	c, bolt := openBoltAndCollection[uint64, Rec](t, filepath.Join(t.TempDir(), "nextid_node_zero_overflow.db"), Options{
		NodeID: 0,
	})
	defer func() {
		_ = c.Close()
		_ = bolt.Close()
	}()

	c.autoUint.Store(math.MaxUint64)
	tx := BeginUpdate()
	defer tx.Release()
	if _, err := c.NextID(tx); !errors.Is(err, rbierrors.ErrSequenceExhausted) {
		t.Fatalf("NextID overflow err=%v want %v", err, rbierrors.ErrSequenceExhausted)
	}
}

func TestAdvanceIDUint64CommitAdvancesSequence(t *testing.T) {
	c, bolt := openBoltAndCollection[uint64, Rec](t, filepath.Join(t.TempDir(), "advanceid_commit.db"), Options{
		NodeID: 12,
	})
	defer func() {
		_ = c.Close()
		_ = bolt.Close()
	}()

	tx := BeginUpdate()
	if err := c.AdvanceID(tx, 99); err != nil {
		t.Fatalf("AdvanceID: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	tx.Release()

	watermark := readNextIDUintWatermark(t, bolt, c.collection)
	if want := uint64(c.options.NodeID)<<autoUintSeqBits | 99; watermark != want {
		t.Fatalf("watermark=%#x want %#x", watermark, want)
	}

	tx = BeginUpdate()
	defer tx.Release()
	id, err := c.NextID(tx)
	if err != nil {
		t.Fatalf("NextID: %v", err)
	}
	if want := uint64(c.options.NodeID)<<autoUintSeqBits | 100; id != want {
		t.Fatalf("NextID after AdvanceID=%#x want %#x", id, want)
	}
}

func TestAdvanceIDUint64NodeZeroAllowsSequenceAbove48Bits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "advanceid_node_zero_48.db")
	opts := Options{BucketName: "advanceid_node_zero_48", NodeID: 0}
	advanced := autoUintSeqMask + 100

	c, bolt := openBoltAndCollection[uint64, Rec](t, path, opts)
	tx := BeginUpdate()
	if err := c.AdvanceID(tx, advanced); err != nil {
		t.Fatalf("AdvanceID: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	tx.Release()

	watermark := readNextIDUintWatermark(t, bolt, c.collection)
	if watermark != advanced {
		t.Fatalf("watermark=%#x want %#x", watermark, advanced)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := bolt.Close(); err != nil {
		t.Fatalf("bolt Close: %v", err)
	}

	c, bolt = openBoltAndCollection[uint64, Rec](t, path, opts)
	defer func() {
		_ = c.Close()
		_ = bolt.Close()
	}()
	tx = BeginUpdate()
	defer tx.Release()
	id, err := c.NextID(tx)
	if err != nil {
		t.Fatalf("NextID after reopen: %v", err)
	}
	if want := advanced + 1; id != want {
		t.Fatalf("NextID after reopen=%#x want %#x", id, want)
	}
}

func TestAdvanceIDUint64RollbackKeepsOnlyInMemoryAdvance(t *testing.T) {
	path := filepath.Join(t.TempDir(), "advanceid_rollback.db")
	opts := Options{BucketName: "advanceid_rollback", NodeID: 13}

	c, bolt := openBoltAndCollection[uint64, Rec](t, path, opts)
	tx := BeginUpdate()
	if err := c.AdvanceID(tx, 20); err != nil {
		t.Fatalf("AdvanceID: %v", err)
	}
	tx.Release()

	tx = BeginUpdate()
	id, err := c.NextID(tx)
	if err != nil {
		t.Fatalf("NextID after rollback in same process: %v", err)
	}
	if want := uint64(opts.NodeID)<<autoUintSeqBits | 21; id != want {
		t.Fatalf("NextID after rollback in same process=%#x want %#x", id, want)
	}
	tx.Release()

	if err = c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err = bolt.Close(); err != nil {
		t.Fatalf("bolt Close: %v", err)
	}

	c, bolt = openBoltAndCollection[uint64, Rec](t, path, opts)
	defer func() {
		_ = c.Close()
		_ = bolt.Close()
	}()
	tx = BeginUpdate()
	defer tx.Release()
	id, err = c.NextID(tx)
	if err != nil {
		t.Fatalf("NextID after reopen: %v", err)
	}
	if want := uint64(opts.NodeID)<<autoUintSeqBits | 1; id != want {
		t.Fatalf("NextID after reopen=%#x want %#x", id, want)
	}
}

func TestAdvanceIDUint64IgnoresLowerSequence(t *testing.T) {
	c, bolt := openBoltAndCollection[uint64, Rec](t, filepath.Join(t.TempDir(), "advanceid_lower.db"), Options{
		NodeID: 14,
	})
	defer func() {
		_ = c.Close()
		_ = bolt.Close()
	}()

	tx := BeginUpdate()
	if err := c.AdvanceID(tx, 10); err != nil {
		t.Fatalf("AdvanceID high: %v", err)
	}
	if err := c.AdvanceID(tx, 3); err != nil {
		t.Fatalf("AdvanceID low: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	tx.Release()

	tx = BeginUpdate()
	defer tx.Release()
	id, err := c.NextID(tx)
	if err != nil {
		t.Fatalf("NextID: %v", err)
	}
	if want := uint64(c.options.NodeID)<<autoUintSeqBits | 11; id != want {
		t.Fatalf("NextID after lower AdvanceID=%#x want %#x", id, want)
	}
}

func TestAdvanceIDUint64OverflowReturnsError(t *testing.T) {
	c, bolt := openBoltAndCollection[uint64, Rec](t, filepath.Join(t.TempDir(), "advanceid_overflow.db"), Options{
		NodeID: 15,
	})
	defer func() {
		_ = c.Close()
		_ = bolt.Close()
	}()

	tx := BeginUpdate()
	defer tx.Release()
	if err := c.AdvanceID(tx, autoUintSeqMask+1); !errors.Is(err, rbierrors.ErrSequenceExhausted) {
		t.Fatalf("AdvanceID overflow err=%v want %v", err, rbierrors.ErrSequenceExhausted)
	}
}

func TestAdvanceIDStringNoop(t *testing.T) {
	c, bolt := openBoltAndCollection[string, Rec](t, filepath.Join(t.TempDir(), "advanceid_string.db"), Options{
		NodeID: 16,
	})
	defer func() {
		_ = c.Close()
		_ = bolt.Close()
	}()

	tx := BeginUpdate()
	if err := c.AdvanceID(tx, 100); err != nil {
		t.Fatalf("AdvanceID string: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	tx.Release()

	if got := readNextIDAutoValue(t, bolt, rootAutoStringKeyKind, c.collection); got != nil {
		t.Fatalf("automatic string metadata after AdvanceID noop: %x", got)
	}
}

func TestNextIDStringReturnsMonotimeUUIDWithNode(t *testing.T) {
	c, bolt := openBoltAndCollection[string, Rec](t, filepath.Join(t.TempDir(), "nextid_string.db"), Options{
		NodeID: 42,
	})
	defer func() {
		_ = c.Close()
		_ = bolt.Close()
	}()

	tx := BeginUpdate()
	defer tx.Release()
	id, err := c.NextID(tx)
	if err != nil {
		t.Fatalf("NextID: %v", err)
	}
	u := parseNextIDUUID(t, id)
	if _, node, ok := u.Parse(); !ok || node != 42 {
		t.Fatalf("uuid node parse=(%d,%v), want 42,true", node, ok)
	}
	if len(id) != 36 {
		t.Fatalf("uuid string len=%d want 36", len(id))
	}
}

func TestNextIDStringCommittedWatermarkSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nextid_string_reopen.db")
	opts := Options{BucketName: "nextid_string_reopen", NodeID: 77}

	c, bolt := openBoltAndCollection[string, Rec](t, path, opts)
	tx := BeginUpdate()
	id1, err := c.NextID(tx)
	if err != nil {
		t.Fatalf("NextID before commit: %v", err)
	}
	if err = c.Set(tx, id1, &Rec{Name: "persisted"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	tx.Release()
	if err = c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err = bolt.Close(); err != nil {
		t.Fatalf("bolt Close: %v", err)
	}

	c, bolt = openBoltAndCollection[string, Rec](t, path, opts)
	defer func() {
		_ = c.Close()
		_ = bolt.Close()
	}()
	tx = BeginUpdate()
	defer tx.Release()
	id2, err := c.NextID(tx)
	if err != nil {
		t.Fatalf("NextID after reopen: %v", err)
	}
	if id2 <= id1 {
		t.Fatalf("uuid after reopen=%q want greater than committed id=%q", id2, id1)
	}
	if got, err := readGet(c, id1); err != nil || got == nil {
		t.Fatalf("committed string record after reopen = %#v/%v", got, err)
	}
}

func TestNextIDMarkerOnlyCommitDoesNotBreakReadTransactions(t *testing.T) {
	c, bolt := openBoltAndCollection[uint64, Rec](t, filepath.Join(t.TempDir(), "nextid_marker_only.db"), Options{
		NodeID: 2,
	})
	defer func() {
		_ = c.Close()
		_ = bolt.Close()
	}()

	tx := BeginUpdate()
	if _, err := c.NextID(tx); err != nil {
		t.Fatalf("NextID: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit marker-only: %v", err)
	}
	tx.Release()

	done := make(chan error, 1)
	go func() {
		readTx := BeginView()
		defer readTx.Release()
		_, err := c.Get(readTx, uint64(c.options.NodeID)<<autoUintSeqBits|1)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("read after marker-only commit: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("read after marker-only commit timed out")
	}
}

func TestNextIDAutoOnlyMultiCollectionCountsMarkersAsBatchWork(t *testing.T) {
	enableStoreStatsForTest(t)

	raw, _ := openRawBolt(t)
	blocker, err := Open[uint64, Rec](raw, testOptions(Options{
		BucketName:     "nextid_work_blocker",
		BatchSoftLimit: 16,
	}))
	if err != nil {
		t.Fatalf("Open blocker: %v", err)
	}
	c1, err := Open[uint64, Rec](raw, testOptions(Options{
		BucketName:     "nextid_work_1",
		BatchSoftLimit: 4,
		NodeID:         21,
	}))
	if err != nil {
		t.Fatalf("Open c1: %v", err)
	}
	c2, err := Open[uint64, Rec](raw, testOptions(Options{
		BucketName:     "nextid_work_2",
		BatchSoftLimit: 4,
		NodeID:         22,
	}))
	if err != nil {
		t.Fatalf("Open c2: %v", err)
	}
	t.Cleanup(func() {
		_ = blocker.Close()
		_ = c1.Close()
		_ = c2.Close()
		_ = raw.Close()
	})

	before := c1.StoreStats()
	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	blockerDone := make(chan error, 1)
	go func() {
		blockerDone <- writeSet(blocker, 1, &Rec{Name: "blocker"}, OnChange(func(*Tx, uint64, *Rec, *Rec) error {
			close(entered)
			<-release
			return nil
		}))
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for blocker hook")
	}

	commitAutoOnly := func() error {
		tx := BeginUpdate()
		defer tx.Release()
		if _, err := c1.NextID(tx); err != nil {
			return err
		}
		if _, err := c2.NextID(tx); err != nil {
			return err
		}
		return tx.Commit()
	}

	done := [3]chan error{make(chan error, 1), make(chan error, 1), make(chan error, 1)}
	for i := range done {
		go func(ch chan<- error) {
			ch <- commitAutoOnly()
		}(done[i])
	}

	waitAutoBatchExtraStats(t, c1.root, "auto-only writes queued behind blocker", func(st rootSchedulerSnapshot) bool {
		return st.Submitted == before.LogicalUnitsSubmitted+4 &&
			st.Enqueued == before.LogicalUnitsEnqueued+4 &&
			st.Dequeued == before.LogicalUnitsDequeued+1 &&
			st.QueueLen == 3
	})

	close(release)
	select {
	case err := <-blockerDone:
		if err != nil {
			t.Fatalf("blocker write: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for blocker write")
	}
	for i := range done {
		select {
		case err := <-done[i]:
			if err != nil {
				t.Fatalf("auto-only write %d: %v", i, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for auto-only write %d", i)
		}
	}

	after := waitAutoBatchExtraStats(t, c1.root, "auto-only batch accounting settled", func(st rootSchedulerSnapshot) bool {
		return st.Dequeued == before.LogicalUnitsDequeued+4 &&
			st.ExecutedBatches == before.ExecutedBatches+3 &&
			st.BatchSize1 == before.BatchSize1+2 &&
			st.BatchSize2To4 == before.BatchSize2To4+1
	})
	if after.MultiUnitBatches != before.MultiUnitBatches+1 || after.MultiUnitOps != before.MultiUnitOps+2 {
		t.Fatalf("expected one two-unit auto-only batch, before=%+v after=%+v", before, after)
	}
}

func TestNextIDNormalWriteDoesNotCreateAutoMetadata(t *testing.T) {
	c, bolt := openBoltAndCollection[uint64, Rec](t, filepath.Join(t.TempDir(), "nextid_no_metadata.db"), Options{
		NodeID: 4,
	})
	defer func() {
		_ = c.Close()
		_ = bolt.Close()
	}()

	if err := writeSet(c, 1, &Rec{Name: "manual"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got := readNextIDAutoValue(t, bolt, rootAutoUintKeyKind, c.collection); got != nil {
		t.Fatalf("automatic metadata exists after normal write: %x", got)
	}
}

func readNextIDUintWatermark(t *testing.T, bolt *bbolt.DB, c *collection) uint64 {
	t.Helper()
	value := readNextIDAutoValue(t, bolt, rootAutoUintKeyKind, c)
	if len(value) != persist.UIDLen+8 {
		t.Fatalf("automatic uint64 metadata length=%d", len(value))
	}
	if string(value[:persist.UIDLen]) != string(c.rbiUID[:]) {
		t.Fatalf("automatic uint64 metadata uid=%x want %x", value[:persist.UIDLen], c.rbiUID)
	}
	return binary.BigEndian.Uint64(value[persist.UIDLen:])
}

func readNextIDAutoValue(t *testing.T, bolt *bbolt.DB, kind byte, c *collection) []byte {
	t.Helper()
	var out []byte
	if err := bolt.View(func(tx *bbolt.Tx) error {
		meta := tx.Bucket(rootMetadataBucketName)
		if meta == nil {
			return fmt.Errorf("root metadata bucket does not exist")
		}
		value := meta.Get(rootAutoIncIDKey(kind, c.dataBucket, c.options.NodeID))
		if value != nil {
			out = append([]byte(nil), value...)
		}
		return nil
	}); err != nil {
		t.Fatalf("read automatic metadata: %v", err)
	}
	return out
}

func parseNextIDUUID(t *testing.T, s string) monotime.UUID {
	t.Helper()
	var id monotime.UUID
	if err := id.UnmarshalText([]byte(s)); err != nil {
		t.Fatalf("parse monotime UUID %q: %v", s, err)
	}
	if !id.Valid() {
		t.Fatalf("UUID %q is not a valid monotime UUID", s)
	}
	return id
}
