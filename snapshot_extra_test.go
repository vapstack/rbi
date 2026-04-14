package rbi

import (
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/posting"
	"go.etcd.io/bbolt"
)

type snapshotExtraRec struct {
	Name  string `db:"name" dbi:"default"`
	Email string `db:"email" dbi:"default"`
	Age   int    `db:"age" dbi:"default"`
}

func snapshotExtraOptions() Options {
	return Options{
		AutoBatchMax:    1,
		AnalyzeInterval: -1,
	}
}

func snapshotExtraOpenTempDBUint64(t *testing.T, opts Options) (*DB[uint64, snapshotExtraRec], string) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot_extra_uint64.db")
	raw, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}

	opts.EnableAutoBatchStats = true
	opts.EnableSnapshotStats = true

	db, err := New[uint64, snapshotExtraRec](raw, opts)
	if err != nil {
		_ = raw.Close()
		t.Fatalf("New: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	return db, path
}

func snapshotExtraOpenTempDBString(t *testing.T, opts Options) (*DB[string, snapshotExtraRec], string) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot_extra_string.db")
	raw, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}

	opts.EnableAutoBatchStats = true
	opts.EnableSnapshotStats = true

	db, err := New[string, snapshotExtraRec](raw, opts)
	if err != nil {
		_ = raw.Close()
		t.Fatalf("New: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	return db, path
}

func snapshotExtraOpenRawBolt(t *testing.T) (*bbolt.DB, string) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot_extra_shared.db")
	raw, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}
	return raw, path
}

func snapshotExtraSeedGeneratedUint64Data(t *testing.T, db *DB[uint64, snapshotExtraRec], n int, gen func(i int) *snapshotExtraRec) {
	t.Helper()

	db.DisableSync()
	defer db.EnableSync()

	batchSize := 32 << 10
	if n > 0 && n < batchSize {
		batchSize = n
	}
	batchIDs := make([]uint64, 0, batchSize)
	batchVals := make([]*snapshotExtraRec, 0, batchSize)

	flush := func() {
		if len(batchIDs) == 0 {
			return
		}
		if err := db.BatchSet(batchIDs, batchVals); err != nil {
			t.Fatalf("BatchSet(seed batch=%d): %v", len(batchIDs), err)
		}
		batchIDs = batchIDs[:0]
		batchVals = batchVals[:0]
	}

	for i := 1; i <= n; i++ {
		batchIDs = append(batchIDs, uint64(i))
		batchVals = append(batchVals, gen(i))
		if len(batchIDs) == cap(batchIDs) {
			flush()
		}
	}
	flush()
}

func snapshotExtraWait(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func snapshotExtraHasFutureSnapshot[K ~uint64 | ~string, V any](db *DB[K, V], currentSeq uint64) bool {
	db.snapshot.mu.RLock()
	defer db.snapshot.mu.RUnlock()

	for seq, ref := range db.snapshot.bySeq {
		if seq > currentSeq && ref != nil && ref.snap != nil {
			return true
		}
	}
	return false
}

func snapshotExtraAssertNoFutureSnapshotRefs[K ~uint64 | ~string, V any](tb testing.TB, db *DB[K, V]) {
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

func snapshotExtraPosting(ids ...uint64) posting.List {
	var out posting.List
	for _, id := range ids {
		out = out.BuildAdded(id)
	}
	return out
}

func snapshotExtraQuerySnapshotKeys[K ~string | ~uint64](t *testing.T, db *DB[K, snapshotExtraRec], snap *indexSnapshot, q *qx.QX) []K {
	t.Helper()

	view := db.makeQueryView(snap)
	defer db.releaseQueryView(view)

	ids, err := view.execQuery(q, false, false)
	if err != nil {
		t.Fatalf("snapshot query: %v", err)
	}
	return ids
}

func snapshotExtraScanSnapshotStringKeys(t *testing.T, db *DB[string, snapshotExtraRec], snap *indexSnapshot, seek string) []string {
	t.Helper()

	if snap == nil {
		t.Fatal("snapshot is nil")
	}

	iter := snap.universe.Iter()
	defer iter.Release()

	var out []string
	if err := db.scanStringKeys(snap.strmap, snap.universe, iter, seek, func(id string) (bool, error) {
		out = append(out, id)
		return true, nil
	}); err != nil {
		t.Fatalf("scanStringKeys: %v", err)
	}
	return out
}

func snapshotExtraMaterializedPredCacheKey(t *testing.T, db *DB[uint64, snapshotExtraRec], e qx.Expr) string {
	t.Helper()

	view := db.makeQueryView(db.getSnapshot())
	defer db.releaseQueryView(view)

	return view.materializedPredCacheKey(e)
}

func snapshotExtraQueryTxRecords(t *testing.T, db *DB[uint64, snapshotExtraRec], tx *bbolt.Tx, snap *indexSnapshot, q *qx.QX) []*snapshotExtraRec {
	t.Helper()

	items, err := db.queryRecords(tx, snap, q)
	if err != nil {
		t.Fatalf("queryRecords: %v", err)
	}
	return items
}

func snapshotExtraRequireNumericRangeBucketCacheEntry(t *testing.T, snap *indexSnapshot, field string) *numericRangeBucketCacheEntry {
	t.Helper()
	if snap == nil || snap.numericRangeBucketCache == nil {
		t.Fatalf("expected non-nil numeric range bucket cache for field %q", field)
	}
	e, ok := snap.numericRangeBucketCache.loadField(field)
	if !ok {
		t.Fatalf("expected numeric range bucket cache entry for field %q", field)
	}
	if e == nil || e.idx.bucketSize <= 0 {
		t.Fatalf("expected non-nil numeric range bucket index for field %q", field)
	}
	return e
}

func snapshotExtraSetNumericBucketKnobs(t *testing.T, db *DB[uint64, snapshotExtraRec], size, minFieldKeys, minSpan int) {
	t.Helper()

	prevSize := db.options.NumericRangeBucketSize
	prevMinField := db.options.NumericRangeBucketMinFieldKeys
	prevMinSpan := db.options.NumericRangeBucketMinSpanKeys

	db.options.NumericRangeBucketSize = size
	db.options.NumericRangeBucketMinFieldKeys = minFieldKeys
	db.options.NumericRangeBucketMinSpanKeys = minSpan

	t.Cleanup(func() {
		db.options.NumericRangeBucketSize = prevSize
		db.options.NumericRangeBucketMinFieldKeys = prevMinField
		db.options.NumericRangeBucketMinSpanKeys = prevMinSpan
	})
}

func snapshotExtraLiveStrMapLookup(t *testing.T, db *DB[string, snapshotExtraRec], key string) (uint64, bool) {
	t.Helper()

	db.strmap.Lock()
	defer db.strmap.Unlock()

	idx, ok := db.strmap.Keys[key]
	return idx, ok
}

func snapshotExtraLiveStrMapNext(t *testing.T, db *DB[string, snapshotExtraRec]) uint64 {
	t.Helper()

	db.strmap.Lock()
	defer db.strmap.Unlock()
	return db.strmap.Next
}

func TestSnapshotExtra_BeginQueryTxSnapshotIgnoresRealStagedSetBeforeCommit(t *testing.T) {
	db, _ := snapshotExtraOpenTempDBUint64(t, snapshotExtraOptions())

	if err := db.Set(1, &snapshotExtraRec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &snapshotExtraRec{Name: "bob", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	old := db.getSnapshot()
	pinned, holdRef, ok := db.pinSnapshotRefBySeq(old.seq)
	if !ok || pinned != old {
		t.Fatalf("expected current snapshot to be pinnable")
	}
	defer db.unpinSnapshotRef(old.seq, holdRef)

	entered := make(chan struct{})
	release := make(chan struct{})
	db.testHooks.beforeCommit = func(op string) error {
		if op != "set" {
			return nil
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return nil
	}
	t.Cleanup(func() {
		db.testHooks.beforeCommit = nil
		select {
		case <-release:
		default:
			close(release)
		}
	})

	done := make(chan error, 1)
	go func() {
		done <- db.Set(2, &snapshotExtraRec{Name: "charlie", Age: 20})
	}()

	snapshotExtraWait(t, entered, "staged set commit hook")
	if !snapshotExtraHasFutureSnapshot(db, old.seq) {
		t.Fatalf("expected staged future snapshot while commit is blocked")
	}
	if got := db.getSnapshot(); got != old {
		t.Fatalf("current snapshot changed before commit publish")
	}

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}
	if seq != old.seq || snap != old {
		t.Fatalf("beginQueryTxSnapshot pinned wrong snapshot: seq=%d want=%d", seq, old.seq)
	}

	beforeBob := snapshotExtraQueryTxRecords(t, db, tx, snap, qx.Query(qx.EQ("name", "bob")))
	if len(beforeBob) != 1 || beforeBob[0] == nil || beforeBob[0].Name != "bob" {
		t.Fatalf("old tx/snapshot lost committed row before publish: %#v", beforeBob)
	}
	beforeCharlie := snapshotExtraQueryTxRecords(t, db, tx, snap, qx.Query(qx.EQ("name", "charlie")))
	if len(beforeCharlie) != 0 {
		t.Fatalf("old tx/snapshot observed staged future row: %#v", beforeCharlie)
	}

	rollback(tx)
	db.unpinSnapshotRef(seq, ref)

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Set(update): %v", err)
	}

	oldBob := snapshotExtraQuerySnapshotKeys(t, db, pinned, qx.Query(qx.EQ("name", "bob")))
	if !slices.Equal(oldBob, []uint64{2}) {
		t.Fatalf("pinned old snapshot changed after publish: got=%v want=[2]", oldBob)
	}
	oldCharlie := snapshotExtraQuerySnapshotKeys(t, db, pinned, qx.Query(qx.EQ("name", "charlie")))
	if len(oldCharlie) != 0 {
		t.Fatalf("pinned old snapshot observed future row after publish: %v", oldCharlie)
	}

	currentCharlie, err := db.QueryKeys(qx.Query(qx.EQ("name", "charlie")))
	if err != nil {
		t.Fatalf("QueryKeys(charlie): %v", err)
	}
	if !slices.Equal(currentCharlie, []uint64{2}) {
		t.Fatalf("current snapshot missed committed update: got=%v want=[2]", currentCharlie)
	}

	currentBob, err := db.QueryKeys(qx.Query(qx.EQ("name", "bob")))
	if err != nil {
		t.Fatalf("QueryKeys(bob): %v", err)
	}
	if len(currentBob) != 0 {
		t.Fatalf("current snapshot kept stale pre-update row: %v", currentBob)
	}

	db.unpinSnapshotRef(old.seq, holdRef)
	snapshotExtraAssertNoFutureSnapshotRefs(t, db)
}

func TestSnapshotExtra_BeginQueryTxSnapshotSurvivesRealStagedSetRollback(t *testing.T) {
	db, _ := snapshotExtraOpenTempDBUint64(t, snapshotExtraOptions())

	if err := db.Set(1, &snapshotExtraRec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &snapshotExtraRec{Name: "bob", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	old := db.getSnapshot()
	pinned, holdRef, ok := db.pinSnapshotRefBySeq(old.seq)
	if !ok || pinned != old {
		t.Fatalf("expected current snapshot to be pinnable")
	}
	defer db.unpinSnapshotRef(old.seq, holdRef)

	entered := make(chan struct{})
	release := make(chan struct{})
	db.testHooks.beforeCommit = func(op string) error {
		if op != "set" {
			return nil
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return fmt.Errorf("failpoint: rollback set")
	}
	t.Cleanup(func() {
		db.testHooks.beforeCommit = nil
		select {
		case <-release:
		default:
			close(release)
		}
	})

	done := make(chan error, 1)
	go func() {
		done <- db.Set(2, &snapshotExtraRec{Name: "charlie", Age: 20})
	}()

	snapshotExtraWait(t, entered, "rollback set commit hook")
	if !snapshotExtraHasFutureSnapshot(db, old.seq) {
		t.Fatalf("expected staged future snapshot while commit is blocked")
	}

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}
	if seq != old.seq || snap != old {
		t.Fatalf("beginQueryTxSnapshot pinned wrong snapshot: seq=%d want=%d", seq, old.seq)
	}

	beforeBob := snapshotExtraQueryTxRecords(t, db, tx, snap, qx.Query(qx.EQ("name", "bob")))
	if len(beforeBob) != 1 || beforeBob[0] == nil || beforeBob[0].Name != "bob" {
		t.Fatalf("old tx/snapshot lost committed row before rollback: %#v", beforeBob)
	}
	beforeCharlie := snapshotExtraQueryTxRecords(t, db, tx, snap, qx.Query(qx.EQ("name", "charlie")))
	if len(beforeCharlie) != 0 {
		t.Fatalf("old tx/snapshot observed staged future row before rollback: %#v", beforeCharlie)
	}

	rollback(tx)
	db.unpinSnapshotRef(seq, ref)

	close(release)
	err = <-done
	if err == nil || !strings.Contains(err.Error(), "failpoint: rollback set") {
		t.Fatalf("expected rollback failpoint error, got: %v", err)
	}

	if got := db.getSnapshot(); got != old {
		t.Fatalf("rollback replaced published snapshot: got=%p want=%p", got, old)
	}

	oldBob := snapshotExtraQuerySnapshotKeys(t, db, pinned, qx.Query(qx.EQ("name", "bob")))
	if !slices.Equal(oldBob, []uint64{2}) {
		t.Fatalf("pinned old snapshot changed after rollback: got=%v want=[2]", oldBob)
	}
	oldCharlie := snapshotExtraQuerySnapshotKeys(t, db, pinned, qx.Query(qx.EQ("name", "charlie")))
	if len(oldCharlie) != 0 {
		t.Fatalf("pinned old snapshot observed rolled-back row: %v", oldCharlie)
	}

	currentBob, err := db.QueryKeys(qx.Query(qx.EQ("name", "bob")))
	if err != nil {
		t.Fatalf("QueryKeys(bob): %v", err)
	}
	if !slices.Equal(currentBob, []uint64{2}) {
		t.Fatalf("current snapshot lost pre-rollback row: got=%v want=[2]", currentBob)
	}

	currentCharlie, err := db.QueryKeys(qx.Query(qx.EQ("name", "charlie")))
	if err != nil {
		t.Fatalf("QueryKeys(charlie): %v", err)
	}
	if len(currentCharlie) != 0 {
		t.Fatalf("current snapshot observed rolled-back row: %v", currentCharlie)
	}

	db.unpinSnapshotRef(old.seq, holdRef)
	snapshotExtraAssertNoFutureSnapshotRefs(t, db)
}

func TestSnapshotExtra_BeginQueryTxSnapshotIgnoresRealStagedBatchSetBeforeCommit(t *testing.T) {
	db, _ := snapshotExtraOpenTempDBUint64(t, snapshotExtraOptions())

	if err := db.Set(1, &snapshotExtraRec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &snapshotExtraRec{Name: "bob", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	old := db.getSnapshot()
	pinned, holdRef, ok := db.pinSnapshotRefBySeq(old.seq)
	if !ok || pinned != old {
		t.Fatalf("expected current snapshot to be pinnable")
	}
	defer db.unpinSnapshotRef(old.seq, holdRef)

	entered := make(chan struct{})
	release := make(chan struct{})
	db.testHooks.beforeCommit = func(op string) error {
		if op != "batch_set" {
			return nil
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return nil
	}
	t.Cleanup(func() {
		db.testHooks.beforeCommit = nil
		select {
		case <-release:
		default:
			close(release)
		}
	})

	done := make(chan error, 1)
	go func() {
		done <- db.BatchSet(
			[]uint64{2, 3},
			[]*snapshotExtraRec{
				{Name: "charlie", Age: 20},
				{Name: "dave", Age: 40},
			},
		)
	}()

	snapshotExtraWait(t, entered, "staged batch_set commit hook")
	if !snapshotExtraHasFutureSnapshot(db, old.seq) {
		t.Fatalf("expected staged future snapshot while batch_set commit is blocked")
	}
	if got := db.getSnapshot(); got != old {
		t.Fatalf("current snapshot changed before batch_set publish")
	}

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}
	if seq != old.seq || snap != old {
		t.Fatalf("beginQueryTxSnapshot pinned wrong snapshot: seq=%d want=%d", seq, old.seq)
	}

	beforeBob := snapshotExtraQueryTxRecords(t, db, tx, snap, qx.Query(qx.EQ("name", "bob")))
	if len(beforeBob) != 1 || beforeBob[0] == nil || beforeBob[0].Name != "bob" {
		t.Fatalf("old tx/snapshot lost committed batch row before publish: %#v", beforeBob)
	}
	beforeCharlie := snapshotExtraQueryTxRecords(t, db, tx, snap, qx.Query(qx.EQ("name", "charlie")))
	if len(beforeCharlie) != 0 {
		t.Fatalf("old tx/snapshot observed staged future batch row: %#v", beforeCharlie)
	}
	beforeDave := snapshotExtraQueryTxRecords(t, db, tx, snap, qx.Query(qx.EQ("name", "dave")))
	if len(beforeDave) != 0 {
		t.Fatalf("old tx/snapshot observed staged inserted batch row: %#v", beforeDave)
	}

	rollback(tx)
	db.unpinSnapshotRef(seq, ref)

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("BatchSet(update+insert): %v", err)
	}

	oldBob := snapshotExtraQuerySnapshotKeys(t, db, pinned, qx.Query(qx.EQ("name", "bob")))
	if !slices.Equal(oldBob, []uint64{2}) {
		t.Fatalf("pinned old snapshot changed after batch publish: got=%v want=[2]", oldBob)
	}
	oldCharlie := snapshotExtraQuerySnapshotKeys(t, db, pinned, qx.Query(qx.EQ("name", "charlie")))
	if len(oldCharlie) != 0 {
		t.Fatalf("pinned old snapshot observed future updated row after batch publish: %v", oldCharlie)
	}
	oldDave := snapshotExtraQuerySnapshotKeys(t, db, pinned, qx.Query(qx.EQ("name", "dave")))
	if len(oldDave) != 0 {
		t.Fatalf("pinned old snapshot observed future inserted row after batch publish: %v", oldDave)
	}

	currentCharlie, err := db.QueryKeys(qx.Query(qx.EQ("name", "charlie")))
	if err != nil {
		t.Fatalf("QueryKeys(charlie): %v", err)
	}
	if !slices.Equal(currentCharlie, []uint64{2}) {
		t.Fatalf("current snapshot missed committed batch update: got=%v want=[2]", currentCharlie)
	}
	currentDave, err := db.QueryKeys(qx.Query(qx.EQ("name", "dave")))
	if err != nil {
		t.Fatalf("QueryKeys(dave): %v", err)
	}
	if !slices.Equal(currentDave, []uint64{3}) {
		t.Fatalf("current snapshot missed committed batch insert: got=%v want=[3]", currentDave)
	}

	db.unpinSnapshotRef(old.seq, holdRef)
	snapshotExtraAssertNoFutureSnapshotRefs(t, db)
}

func TestSnapshotExtra_NumericRangeCacheDoesNotLeakAcrossChangedSnapshot(t *testing.T) {
	opts := snapshotExtraOptions()
	opts.SnapshotMaterializedPredCacheMaxEntries = 64
	db, _ := snapshotExtraOpenTempDBUint64(t, opts)

	snapshotExtraSeedGeneratedUint64Data(t, db, 2048, func(i int) *snapshotExtraRec {
		return &snapshotExtraRec{
			Name: fmt.Sprintf("user-%04d", i),
			Age:  i,
		}
	})
	snapshotExtraSetNumericBucketKnobs(t, db, 64, 1, 1)

	old := db.getSnapshot()
	q := qx.Query(qx.GTE("age", 1024))

	if _, err := db.QueryKeys(q); err != nil {
		t.Fatalf("QueryKeys(warm old range cache): %v", err)
	}
	snapshotExtraRequireNumericRangeBucketCacheEntry(t, old, "age")

	pinned, ref, ok := db.pinSnapshotRefBySeq(old.seq)
	if !ok || pinned != old {
		t.Fatalf("expected old snapshot to be pinnable")
	}
	defer db.unpinSnapshotRef(old.seq, ref)

	if err := db.Patch(900, []Field{{Name: "age", Value: 1200}}); err != nil {
		t.Fatalf("Patch(age): %v", err)
	}

	oldIDs := snapshotExtraQuerySnapshotKeys(t, db, pinned, q)
	currentIDs, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys(current range): %v", err)
	}

	if slices.Contains(oldIDs, uint64(900)) {
		t.Fatalf("old snapshot range query leaked patched id=900: %v", oldIDs)
	}
	if !slices.Contains(currentIDs, uint64(900)) {
		t.Fatalf("current snapshot range query missed patched id=900: %v", currentIDs)
	}
	if len(currentIDs) != len(oldIDs)+1 {
		t.Fatalf("unexpected range cardinality delta after threshold-crossing patch: old=%d new=%d", len(oldIDs), len(currentIDs))
	}
}

func TestSnapshotExtra_StringKeyPinnedSnapshotDoesNotSeeRealStagedFutureKey(t *testing.T) {
	db, _ := snapshotExtraOpenTempDBString(t, snapshotExtraOptions())

	if err := db.Set("k1", &snapshotExtraRec{Name: "one", Age: 10}); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	if err := db.Set("k2", &snapshotExtraRec{Name: "two", Age: 20}); err != nil {
		t.Fatalf("Set(k2): %v", err)
	}

	old := db.getSnapshot()
	pinned, holdRef, ok := db.pinSnapshotRefBySeq(old.seq)
	if !ok || pinned != old {
		t.Fatalf("expected current string snapshot to be pinnable")
	}
	defer db.unpinSnapshotRef(old.seq, holdRef)

	entered := make(chan struct{})
	release := make(chan struct{})
	db.testHooks.beforeCommit = func(op string) error {
		if op != "set" {
			return nil
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return nil
	}
	t.Cleanup(func() {
		db.testHooks.beforeCommit = nil
		select {
		case <-release:
		default:
			close(release)
		}
	})

	done := make(chan error, 1)
	go func() {
		done <- db.Set("future", &snapshotExtraRec{Name: "future", Age: 99})
	}()

	snapshotExtraWait(t, entered, "string staged set commit hook")
	if !snapshotExtraHasFutureSnapshot(db, old.seq) {
		t.Fatalf("expected staged future string snapshot while commit is blocked")
	}
	if got := db.getSnapshot(); got != old {
		t.Fatalf("current string snapshot changed before publish")
	}
	if _, ok := old.strmap.getIdxNoLock("future"); ok {
		t.Fatalf("old published strmap observed staged future key before publish")
	}

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}
	if seq != old.seq || snap != old {
		t.Fatalf("beginQueryTxSnapshot pinned wrong string snapshot: seq=%d want=%d", seq, old.seq)
	}

	beforeFuture, err := db.queryRecords(tx, snap, qx.Query(qx.EQ("name", "future")))
	if err != nil {
		t.Fatalf("queryRecords(future before publish): %v", err)
	}
	if len(beforeFuture) != 0 {
		t.Fatalf("old string tx/snapshot observed staged future row: %#v", beforeFuture)
	}

	rollback(tx)
	db.unpinSnapshotRef(seq, ref)

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Set(future): %v", err)
	}

	if _, ok := pinned.strmap.getIdxNoLock("future"); ok {
		t.Fatalf("pinned old string snapshot observed future key after publish")
	}
	oldFuture := snapshotExtraQuerySnapshotKeys(t, db, pinned, qx.Query(qx.EQ("name", "future")))
	if len(oldFuture) != 0 {
		t.Fatalf("pinned old string snapshot observed future row after publish: %v", oldFuture)
	}

	currentFuture, err := db.QueryKeys(qx.Query(qx.EQ("name", "future")))
	if err != nil {
		t.Fatalf("QueryKeys(future): %v", err)
	}
	if !slices.Equal(currentFuture, []string{"future"}) {
		t.Fatalf("current string snapshot missed committed future key: got=%v want=[future]", currentFuture)
	}

	db.unpinSnapshotRef(old.seq, holdRef)
	snapshotExtraAssertNoFutureSnapshotRefs(t, db)
}

func TestSnapshotExtra_StringKeyRollbackRemovesLiveCreatedIdxAndReusesIt(t *testing.T) {
	db, _ := snapshotExtraOpenTempDBString(t, snapshotExtraOptions())

	if err := db.Set("k1", &snapshotExtraRec{Name: "one", Age: 10}); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	if err := db.Set("k2", &snapshotExtraRec{Name: "two", Age: 20}); err != nil {
		t.Fatalf("Set(k2): %v", err)
	}

	old := db.getSnapshot()
	pinned, holdRef, ok := db.pinSnapshotRefBySeq(old.seq)
	if !ok || pinned != old {
		t.Fatalf("expected current string snapshot to be pinnable")
	}
	defer db.unpinSnapshotRef(old.seq, holdRef)

	entered := make(chan struct{})
	release := make(chan struct{})
	db.testHooks.beforeCommit = func(op string) error {
		if op != "set" {
			return nil
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return fmt.Errorf("failpoint: rollback string set")
	}
	t.Cleanup(func() {
		db.testHooks.beforeCommit = nil
		select {
		case <-release:
		default:
			close(release)
		}
	})

	done := make(chan error, 1)
	go func() {
		done <- db.Set("ghost", &snapshotExtraRec{Name: "ghost", Age: 99})
	}()

	snapshotExtraWait(t, entered, "string rollback set commit hook")

	if _, ok := old.strmap.getIdxNoLock("ghost"); ok {
		t.Fatalf("published old snapshot observed ghost key before rollback")
	}

	ghostIdx, ok := snapshotExtraLiveStrMapLookup(t, db, "ghost")
	if !ok {
		t.Fatalf("live strmap missing pre-commit ghost key")
	}
	if ghostIdx != 3 {
		t.Fatalf("unexpected pre-commit ghost idx: got=%d want=3", ghostIdx)
	}
	if next := snapshotExtraLiveStrMapNext(t, db); next != 3 {
		t.Fatalf("unexpected pre-commit strmap.Next: got=%d want=3", next)
	}

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}
	if seq != old.seq || snap != old {
		t.Fatalf("beginQueryTxSnapshot pinned wrong string snapshot: seq=%d want=%d", seq, old.seq)
	}
	beforeGhost, err := db.queryRecords(tx, snap, qx.Query(qx.EQ("name", "ghost")))
	if err != nil {
		t.Fatalf("queryRecords(ghost before rollback): %v", err)
	}
	if len(beforeGhost) != 0 {
		t.Fatalf("old string tx/snapshot observed ghost row before rollback: %#v", beforeGhost)
	}
	rollback(tx)
	db.unpinSnapshotRef(seq, ref)

	close(release)
	err = <-done
	if err == nil || !strings.Contains(err.Error(), "failpoint: rollback string set") {
		t.Fatalf("expected rollback failpoint error, got: %v", err)
	}
	db.testHooks.beforeCommit = nil

	if got := db.getSnapshot(); got != old {
		t.Fatalf("rollback replaced published string snapshot: got=%p want=%p", got, old)
	}
	if _, ok := snapshotExtraLiveStrMapLookup(t, db, "ghost"); ok {
		t.Fatalf("live strmap retained ghost key after rollback")
	}
	if next := snapshotExtraLiveStrMapNext(t, db); next != 2 {
		t.Fatalf("strmap.Next not restored after rollback: got=%d want=2", next)
	}
	if _, ok := pinned.strmap.getIdxNoLock("ghost"); ok {
		t.Fatalf("pinned old string snapshot observed ghost key after rollback")
	}

	if err := db.Set("later", &snapshotExtraRec{Name: "later", Age: 50}); err != nil {
		t.Fatalf("Set(later): %v", err)
	}
	liveLaterIdx, ok := snapshotExtraLiveStrMapLookup(t, db, "later")
	if !ok {
		t.Fatalf("live strmap missing later after rollback recovery publish")
	}
	if liveLaterIdx != 3 {
		t.Fatalf("expected live later key to reuse rolled-back idx=3, got=%d", liveLaterIdx)
	}
	queryLater, err := db.QueryKeys(qx.Query(qx.EQ("name", "later")))
	if err != nil {
		t.Fatalf("QueryKeys(name=later): %v", err)
	}
	if !slices.Equal(queryLater, []string{"later"}) {
		t.Fatalf("query path missed later key after rollback recovery: got=%v want=[later]", queryLater)
	}

	current := db.getSnapshot()
	laterIdx, ok := current.strmap.getIdxNoLock("later")
	if !ok {
		t.Fatalf("current snapshot missing later key after rollback recovery; liveIdx=%d seq=%d", liveLaterIdx, current.seq)
	}
	if laterIdx != 3 {
		t.Fatalf("expected later key to reuse rolled-back idx=3, got=%d", laterIdx)
	}
	if _, ok := current.strmap.getIdxNoLock("ghost"); ok {
		t.Fatalf("current snapshot retained ghost key after successful later publish")
	}

	db.unpinSnapshotRef(old.seq, holdRef)
	snapshotExtraAssertNoFutureSnapshotRefs(t, db)
}

func TestSnapshotExtra_StringKeyBatchRollbackRemovesAllLiveCreatedIdxs(t *testing.T) {
	db, _ := snapshotExtraOpenTempDBString(t, snapshotExtraOptions())

	if err := db.Set("k1", &snapshotExtraRec{Name: "one", Age: 10}); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}

	old := db.getSnapshot()
	pinned, holdRef, ok := db.pinSnapshotRefBySeq(old.seq)
	if !ok || pinned != old {
		t.Fatalf("expected current string snapshot to be pinnable")
	}
	defer db.unpinSnapshotRef(old.seq, holdRef)

	entered := make(chan struct{})
	release := make(chan struct{})
	db.testHooks.beforeCommit = func(op string) error {
		if op != "batch_set" {
			return nil
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return fmt.Errorf("failpoint: rollback string batch_set")
	}
	t.Cleanup(func() {
		db.testHooks.beforeCommit = nil
		select {
		case <-release:
		default:
			close(release)
		}
	})

	done := make(chan error, 1)
	go func() {
		done <- db.BatchSet(
			[]string{"ghostA", "ghostB"},
			[]*snapshotExtraRec{
				{Name: "ghostA", Age: 11},
				{Name: "ghostB", Age: 12},
			},
		)
	}()

	snapshotExtraWait(t, entered, "string rollback batch_set commit hook")

	if _, ok := old.strmap.getIdxNoLock("ghostA"); ok {
		t.Fatalf("published old snapshot observed ghostA before rollback")
	}
	if _, ok := old.strmap.getIdxNoLock("ghostB"); ok {
		t.Fatalf("published old snapshot observed ghostB before rollback")
	}

	ghostAIdx, ok := snapshotExtraLiveStrMapLookup(t, db, "ghostA")
	if !ok {
		t.Fatalf("live strmap missing pre-commit ghostA key")
	}
	ghostBIdx, ok := snapshotExtraLiveStrMapLookup(t, db, "ghostB")
	if !ok {
		t.Fatalf("live strmap missing pre-commit ghostB key")
	}
	if ghostAIdx != 2 || ghostBIdx != 3 {
		t.Fatalf("unexpected pre-commit batch ghost idxs: ghostA=%d ghostB=%d want=2,3", ghostAIdx, ghostBIdx)
	}
	if next := snapshotExtraLiveStrMapNext(t, db); next != 3 {
		t.Fatalf("unexpected pre-commit batch strmap.Next: got=%d want=3", next)
	}

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}
	if seq != old.seq || snap != old {
		t.Fatalf("beginQueryTxSnapshot pinned wrong string snapshot: seq=%d want=%d", seq, old.seq)
	}
	beforeGhostA, err := db.queryRecords(tx, snap, qx.Query(qx.EQ("name", "ghostA")))
	if err != nil {
		t.Fatalf("queryRecords(ghostA before rollback): %v", err)
	}
	if len(beforeGhostA) != 0 {
		t.Fatalf("old string tx/snapshot observed ghostA row before rollback: %#v", beforeGhostA)
	}
	beforeGhostB, err := db.queryRecords(tx, snap, qx.Query(qx.EQ("name", "ghostB")))
	if err != nil {
		t.Fatalf("queryRecords(ghostB before rollback): %v", err)
	}
	if len(beforeGhostB) != 0 {
		t.Fatalf("old string tx/snapshot observed ghostB row before rollback: %#v", beforeGhostB)
	}
	rollback(tx)
	db.unpinSnapshotRef(seq, ref)

	close(release)
	err = <-done
	if err == nil || !strings.Contains(err.Error(), "failpoint: rollback string batch_set") {
		t.Fatalf("expected rollback failpoint error, got: %v", err)
	}
	db.testHooks.beforeCommit = nil

	if got := db.getSnapshot(); got != old {
		t.Fatalf("rollback replaced published string snapshot: got=%p want=%p", got, old)
	}
	if _, ok := snapshotExtraLiveStrMapLookup(t, db, "ghostA"); ok {
		t.Fatalf("live strmap retained ghostA after rollback")
	}
	if _, ok := snapshotExtraLiveStrMapLookup(t, db, "ghostB"); ok {
		t.Fatalf("live strmap retained ghostB after rollback")
	}
	if next := snapshotExtraLiveStrMapNext(t, db); next != 1 {
		t.Fatalf("strmap.Next not restored after batch rollback: got=%d want=1", next)
	}
	if _, ok := pinned.strmap.getIdxNoLock("ghostA"); ok {
		t.Fatalf("pinned old string snapshot observed ghostA after rollback")
	}
	if _, ok := pinned.strmap.getIdxNoLock("ghostB"); ok {
		t.Fatalf("pinned old string snapshot observed ghostB after rollback")
	}

	if err := db.BatchSet(
		[]string{"realA", "realB"},
		[]*snapshotExtraRec{
			{Name: "realA", Age: 21},
			{Name: "realB", Age: 22},
		},
	); err != nil {
		t.Fatalf("BatchSet(realA,realB): %v", err)
	}
	liveRealAIdx, ok := snapshotExtraLiveStrMapLookup(t, db, "realA")
	if !ok {
		t.Fatalf("live strmap missing realA after rollback recovery publish")
	}
	liveRealBIdx, ok := snapshotExtraLiveStrMapLookup(t, db, "realB")
	if !ok {
		t.Fatalf("live strmap missing realB after rollback recovery publish")
	}
	if liveRealAIdx != 2 || liveRealBIdx != 3 {
		t.Fatalf("expected live real keys to reuse rolled-back idxs 2,3; got realA=%d realB=%d", liveRealAIdx, liveRealBIdx)
	}
	queryReal, err := db.QueryKeys(qx.Query().By("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(real recovery): %v", err)
	}
	if !slices.Equal(queryReal, []string{"k1", "realA", "realB"}) {
		t.Fatalf("query path drift after batch rollback recovery: got=%v want=[k1 realA realB]", queryReal)
	}

	current := db.getSnapshot()
	realAIdx, ok := current.strmap.getIdxNoLock("realA")
	if !ok {
		t.Fatalf("current snapshot missing realA after rollback recovery; liveRealA=%d liveRealB=%d seq=%d", liveRealAIdx, liveRealBIdx, current.seq)
	}
	realBIdx, ok := current.strmap.getIdxNoLock("realB")
	if !ok {
		t.Fatalf("current snapshot missing realB after rollback recovery")
	}
	if realAIdx != 2 || realBIdx != 3 {
		t.Fatalf("expected real keys to reuse rolled-back idxs 2,3; got realA=%d realB=%d", realAIdx, realBIdx)
	}
	if _, ok := current.strmap.getIdxNoLock("ghostA"); ok {
		t.Fatalf("current snapshot retained ghostA after successful recovery publish")
	}
	if _, ok := current.strmap.getIdxNoLock("ghostB"); ok {
		t.Fatalf("current snapshot retained ghostB after successful recovery publish")
	}

	db.unpinSnapshotRef(old.seq, holdRef)
	snapshotExtraAssertNoFutureSnapshotRefs(t, db)
}

func TestSnapshotExtra_MaterializedPredCacheDoesNotLeakAcrossTouchedSnapshot(t *testing.T) {
	opts := snapshotExtraOptions()
	opts.SnapshotMaterializedPredCacheMaxEntries = 64
	db, _ := snapshotExtraOpenTempDBUint64(t, opts)

	if err := db.Set(1, &snapshotExtraRec{Name: "alice", Email: "alpha@example.com"}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &snapshotExtraRec{Name: "bob", Email: "beta@example.com"}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	old := db.getSnapshot()
	posExpr := qx.Expr{Op: qx.OpPREFIX, Field: "email", Value: "alpha"}
	negExpr := qx.Expr{Op: qx.OpPREFIX, Field: "email", Value: "future"}

	posKey := snapshotExtraMaterializedPredCacheKey(t, db, posExpr)
	negKey := snapshotExtraMaterializedPredCacheKey(t, db, negExpr)
	if posKey == "" || negKey == "" {
		t.Fatalf("expected non-empty materialized predicate cache keys")
	}

	old.storeMaterializedPred(posKey, snapshotExtraPosting(1))
	old.storeMaterializedPred(negKey, posting.List{})
	if _, ok := old.loadMaterializedPred(posKey); !ok {
		t.Fatalf("expected old positive cache entry")
	}
	if _, ok := old.loadMaterializedPred(negKey); !ok {
		t.Fatalf("expected old negative cache entry")
	}

	pinned, ref, ok := db.pinSnapshotRefBySeq(old.seq)
	if !ok || pinned != old {
		t.Fatalf("expected old snapshot to be pinnable")
	}
	defer db.unpinSnapshotRef(old.seq, ref)

	if err := db.Patch(1, []Field{{Name: "email", Value: "gamma@example.com"}}); err != nil {
		t.Fatalf("Patch(email): %v", err)
	}
	if err := db.Set(3, &snapshotExtraRec{Name: "carol", Email: "future@example.com"}); err != nil {
		t.Fatalf("Set(3): %v", err)
	}

	current := db.getSnapshot()
	if _, ok := current.loadMaterializedPred(posKey); ok {
		t.Fatalf("current snapshot inherited stale positive cache entry for touched field")
	}
	if _, ok := current.loadMaterializedPred(negKey); ok {
		t.Fatalf("current snapshot inherited stale negative cache entry for touched field")
	}

	oldAlpha := snapshotExtraQuerySnapshotKeys(t, db, pinned, qx.Query(qx.PREFIX("email", "alpha")))
	if !slices.Equal(oldAlpha, []uint64{1}) {
		t.Fatalf("pinned old snapshot lost cached positive match: got=%v want=[1]", oldAlpha)
	}
	oldFuture := snapshotExtraQuerySnapshotKeys(t, db, pinned, qx.Query(qx.PREFIX("email", "future")))
	if len(oldFuture) != 0 {
		t.Fatalf("pinned old snapshot lost cached negative result: %v", oldFuture)
	}

	currentAlpha, err := db.QueryKeys(qx.Query(qx.PREFIX("email", "alpha")))
	if err != nil {
		t.Fatalf("QueryKeys(alpha): %v", err)
	}
	if len(currentAlpha) != 0 {
		t.Fatalf("current snapshot kept stale alpha match: %v", currentAlpha)
	}

	currentFuture, err := db.QueryKeys(qx.Query(qx.PREFIX("email", "future")))
	if err != nil {
		t.Fatalf("QueryKeys(future): %v", err)
	}
	if !slices.Equal(currentFuture, []uint64{3}) {
		t.Fatalf("current snapshot missed new future match: got=%v want=[3]", currentFuture)
	}
}

func TestSnapshotExtra_BeginQueryTxSnapshotUnaffectedByOtherBucketWrites(t *testing.T) {
	raw, _ := snapshotExtraOpenRawBolt(t)
	defer func() { _ = raw.Close() }()

	opts := snapshotExtraOptions()
	opts.EnableAutoBatchStats = true
	opts.EnableSnapshotStats = true

	dbA, err := New[uint64, snapshotExtraRec](raw, opts)
	if err != nil {
		t.Fatalf("New(bucket A): %v", err)
	}
	defer func() { _ = dbA.Close() }()

	opts.BucketName = "snapshot_extra_b"
	dbB, err := New[uint64, snapshotExtraRec](raw, opts)
	if err != nil {
		t.Fatalf("New(bucket B): %v", err)
	}
	defer func() { _ = dbB.Close() }()

	if err := dbA.Set(1, &snapshotExtraRec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("dbA Set(1): %v", err)
	}
	if err := dbA.Set(2, &snapshotExtraRec{Name: "bob", Age: 20}); err != nil {
		t.Fatalf("dbA Set(2): %v", err)
	}

	beforeSeq := dbA.getSnapshot().seq

	writerDone := make(chan error, 1)
	go func() {
		for i := 0; i < 64; i++ {
			if err := dbB.Set(uint64(i+1), &snapshotExtraRec{
				Name: fmt.Sprintf("other-%02d", i),
				Age:  100 + i,
			}); err != nil {
				writerDone <- err
				return
			}
		}
		writerDone <- nil
	}()

	for i := 0; i < 64; i++ {
		tx, snap, seq, ref, err := dbA.beginQueryTxSnapshot()
		if err != nil {
			t.Fatalf("beginQueryTxSnapshot(iter=%d): %v", i, err)
		}
		if seq != beforeSeq || snap.seq != beforeSeq {
			t.Fatalf("dbA query pinned wrong snapshot during other-bucket writes: iter=%d seq=%d want=%d", i, seq, beforeSeq)
		}

		items, err := dbA.queryRecords(tx, snap, qx.Query().By("name", qx.ASC))
		rollback(tx)
		dbA.unpinSnapshotRef(seq, ref)
		if err != nil {
			t.Fatalf("queryRecords(iter=%d): %v", i, err)
		}
		if len(items) != 2 || items[0] == nil || items[1] == nil || items[0].Name != "alice" || items[1].Name != "bob" {
			t.Fatalf("dbA query drift during other-bucket writes at iter=%d: %#v", i, items)
		}
	}

	if err := <-writerDone; err != nil {
		t.Fatalf("dbB writer: %v", err)
	}

	afterSeq := dbA.getSnapshot().seq
	if afterSeq != beforeSeq {
		t.Fatalf("dbA snapshot sequence changed due to other bucket writes: before=%d after=%d", beforeSeq, afterSeq)
	}

	keys, err := dbA.QueryKeys(qx.Query().By("name", qx.ASC))
	if err != nil {
		t.Fatalf("dbA QueryKeys(final): %v", err)
	}
	if !slices.Equal(keys, []uint64{1, 2}) {
		t.Fatalf("dbA final keys drifted after other bucket writes: %v", keys)
	}
}

func TestSnapshotExtra_BeginQueryTxSnapshotIgnoresRealStagedDeleteBeforeCommit(t *testing.T) {
	db, _ := snapshotExtraOpenTempDBUint64(t, snapshotExtraOptions())

	if err := db.BatchSet(
		[]uint64{1, 2, 3},
		[]*snapshotExtraRec{
			{Name: "alice", Age: 30},
			{Name: "bob", Age: 20},
			{Name: "carol", Age: 40},
		},
	); err != nil {
		t.Fatalf("BatchSet(seed): %v", err)
	}

	old := db.getSnapshot()
	pinned, holdRef, ok := db.pinSnapshotRefBySeq(old.seq)
	if !ok || pinned != old {
		t.Fatalf("expected current snapshot to be pinnable")
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	db.testHooks.beforeCommit = func(op string) error {
		if op != "delete" {
			return nil
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return nil
	}
	t.Cleanup(func() {
		db.testHooks.beforeCommit = nil
		select {
		case <-release:
		default:
			close(release)
		}
	})

	done := make(chan error, 1)
	go func() {
		done <- db.Delete(2)
	}()

	snapshotExtraWait(t, entered, "staged delete commit hook")
	if !snapshotExtraHasFutureSnapshot(db, old.seq) {
		t.Fatalf("expected staged future snapshot while delete commit is blocked")
	}
	if got := db.getSnapshot(); got != old {
		t.Fatalf("current snapshot changed before delete publish")
	}

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}
	if seq != old.seq || snap != old {
		t.Fatalf("beginQueryTxSnapshot pinned wrong snapshot: seq=%d want=%d", seq, old.seq)
	}

	beforeAll := snapshotExtraQueryTxRecords(t, db, tx, snap, qx.Query().By("name", qx.ASC))
	if len(beforeAll) != 3 || beforeAll[0] == nil || beforeAll[1] == nil || beforeAll[2] == nil ||
		beforeAll[0].Name != "alice" || beforeAll[1].Name != "bob" || beforeAll[2].Name != "carol" {
		t.Fatalf("old tx/snapshot drifted before delete publish: %#v", beforeAll)
	}
	beforeBob := snapshotExtraQueryTxRecords(t, db, tx, snap, qx.Query(qx.EQ("name", "bob")))
	if len(beforeBob) != 1 || beforeBob[0] == nil || beforeBob[0].Name != "bob" {
		t.Fatalf("old tx/snapshot lost soon-to-be-deleted row before publish: %#v", beforeBob)
	}

	rollback(tx)
	db.unpinSnapshotRef(seq, ref)

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Delete(2): %v", err)
	}

	oldKeys := snapshotExtraQuerySnapshotKeys(t, db, pinned, qx.Query().By("name", qx.ASC))
	if !slices.Equal(oldKeys, []uint64{1, 2, 3}) {
		t.Fatalf("pinned old snapshot changed after delete publish: got=%v want=[1 2 3]", oldKeys)
	}
	oldBob := snapshotExtraQuerySnapshotKeys(t, db, pinned, qx.Query(qx.EQ("name", "bob")))
	if !slices.Equal(oldBob, []uint64{2}) {
		t.Fatalf("pinned old snapshot lost deleted row after publish: got=%v want=[2]", oldBob)
	}

	currentKeys, err := db.QueryKeys(qx.Query().By("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(current after delete): %v", err)
	}
	if !slices.Equal(currentKeys, []uint64{1, 3}) {
		t.Fatalf("current snapshot missed committed delete: got=%v want=[1 3]", currentKeys)
	}
	currentBob, err := db.QueryKeys(qx.Query(qx.EQ("name", "bob")))
	if err != nil {
		t.Fatalf("QueryKeys(bob after delete): %v", err)
	}
	if len(currentBob) != 0 {
		t.Fatalf("current snapshot retained deleted row: %v", currentBob)
	}

	db.unpinSnapshotRef(old.seq, holdRef)
	snapshotExtraAssertNoFutureSnapshotRefs(t, db)
}

func TestSnapshotExtra_BeginQueryTxSnapshotSurvivesRealStagedDeleteRollback(t *testing.T) {
	db, _ := snapshotExtraOpenTempDBUint64(t, snapshotExtraOptions())

	if err := db.BatchSet(
		[]uint64{1, 2, 3},
		[]*snapshotExtraRec{
			{Name: "alice", Age: 30},
			{Name: "bob", Age: 20},
			{Name: "carol", Age: 40},
		},
	); err != nil {
		t.Fatalf("BatchSet(seed): %v", err)
	}

	old := db.getSnapshot()
	pinned, holdRef, ok := db.pinSnapshotRefBySeq(old.seq)
	if !ok || pinned != old {
		t.Fatalf("expected current snapshot to be pinnable")
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	db.testHooks.beforeCommit = func(op string) error {
		if op != "delete" {
			return nil
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return fmt.Errorf("failpoint: rollback delete")
	}
	t.Cleanup(func() {
		db.testHooks.beforeCommit = nil
		select {
		case <-release:
		default:
			close(release)
		}
	})

	done := make(chan error, 1)
	go func() {
		done <- db.Delete(2)
	}()

	snapshotExtraWait(t, entered, "rollback delete commit hook")
	if !snapshotExtraHasFutureSnapshot(db, old.seq) {
		t.Fatalf("expected staged future snapshot while delete rollback is blocked")
	}

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}
	if seq != old.seq || snap != old {
		t.Fatalf("beginQueryTxSnapshot pinned wrong snapshot: seq=%d want=%d", seq, old.seq)
	}

	beforeAll := snapshotExtraQueryTxRecords(t, db, tx, snap, qx.Query().By("name", qx.ASC))
	if len(beforeAll) != 3 || beforeAll[0] == nil || beforeAll[1] == nil || beforeAll[2] == nil ||
		beforeAll[0].Name != "alice" || beforeAll[1].Name != "bob" || beforeAll[2].Name != "carol" {
		t.Fatalf("old tx/snapshot drifted before delete rollback: %#v", beforeAll)
	}

	rollback(tx)
	db.unpinSnapshotRef(seq, ref)

	close(release)
	err = <-done
	if err == nil || !strings.Contains(err.Error(), "failpoint: rollback delete") {
		t.Fatalf("expected rollback failpoint error, got: %v", err)
	}

	if got := db.getSnapshot(); got != old {
		t.Fatalf("rollback replaced published snapshot: got=%p want=%p", got, old)
	}

	oldKeys := snapshotExtraQuerySnapshotKeys(t, db, pinned, qx.Query().By("name", qx.ASC))
	if !slices.Equal(oldKeys, []uint64{1, 2, 3}) {
		t.Fatalf("pinned old snapshot changed after delete rollback: got=%v want=[1 2 3]", oldKeys)
	}

	currentKeys, err := db.QueryKeys(qx.Query().By("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(current after rollback): %v", err)
	}
	if !slices.Equal(currentKeys, []uint64{1, 2, 3}) {
		t.Fatalf("current snapshot changed after rolled-back delete: got=%v want=[1 2 3]", currentKeys)
	}
	currentBob, err := db.QueryKeys(qx.Query(qx.EQ("name", "bob")))
	if err != nil {
		t.Fatalf("QueryKeys(bob after rollback): %v", err)
	}
	if !slices.Equal(currentBob, []uint64{2}) {
		t.Fatalf("current snapshot lost rolled-back row: got=%v want=[2]", currentBob)
	}

	db.unpinSnapshotRef(old.seq, holdRef)
	snapshotExtraAssertNoFutureSnapshotRefs(t, db)
}

func TestSnapshotExtra_BeginQueryTxSnapshotIgnoresRealStagedBatchDeleteBeforeCommit(t *testing.T) {
	db, _ := snapshotExtraOpenTempDBUint64(t, snapshotExtraOptions())

	if err := db.BatchSet(
		[]uint64{1, 2, 3, 4},
		[]*snapshotExtraRec{
			{Name: "alice", Age: 30},
			{Name: "bob", Age: 20},
			{Name: "carol", Age: 40},
			{Name: "dave", Age: 50},
		},
	); err != nil {
		t.Fatalf("BatchSet(seed): %v", err)
	}

	old := db.getSnapshot()
	pinned, holdRef, ok := db.pinSnapshotRefBySeq(old.seq)
	if !ok || pinned != old {
		t.Fatalf("expected current snapshot to be pinnable")
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	db.testHooks.beforeCommit = func(op string) error {
		if op != "batch_delete" {
			return nil
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return nil
	}
	t.Cleanup(func() {
		db.testHooks.beforeCommit = nil
		select {
		case <-release:
		default:
			close(release)
		}
	})

	done := make(chan error, 1)
	go func() {
		done <- db.BatchDelete([]uint64{2, 2, 3, 3})
	}()

	snapshotExtraWait(t, entered, "staged batch_delete commit hook")
	if !snapshotExtraHasFutureSnapshot(db, old.seq) {
		t.Fatalf("expected staged future snapshot while batch_delete commit is blocked")
	}
	if got := db.getSnapshot(); got != old {
		t.Fatalf("current snapshot changed before batch_delete publish")
	}

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}
	if seq != old.seq || snap != old {
		t.Fatalf("beginQueryTxSnapshot pinned wrong snapshot: seq=%d want=%d", seq, old.seq)
	}

	beforeAll := snapshotExtraQueryTxRecords(t, db, tx, snap, qx.Query().By("name", qx.ASC))
	if len(beforeAll) != 4 || beforeAll[0] == nil || beforeAll[1] == nil || beforeAll[2] == nil || beforeAll[3] == nil ||
		beforeAll[0].Name != "alice" || beforeAll[1].Name != "bob" || beforeAll[2].Name != "carol" || beforeAll[3].Name != "dave" {
		t.Fatalf("old tx/snapshot drifted before batch_delete publish: %#v", beforeAll)
	}

	rollback(tx)
	db.unpinSnapshotRef(seq, ref)

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("BatchDelete(2,2,3,3): %v", err)
	}

	oldKeys := snapshotExtraQuerySnapshotKeys(t, db, pinned, qx.Query().By("name", qx.ASC))
	if !slices.Equal(oldKeys, []uint64{1, 2, 3, 4}) {
		t.Fatalf("pinned old snapshot changed after batch_delete publish: got=%v want=[1 2 3 4]", oldKeys)
	}
	oldDeleted := snapshotExtraQuerySnapshotKeys(t, db, pinned, qx.Query(qx.IN("name", []string{"bob", "carol"})))
	if !slices.Equal(oldDeleted, []uint64{2, 3}) {
		t.Fatalf("pinned old snapshot lost batch-deleted rows after publish: got=%v want=[2 3]", oldDeleted)
	}

	currentKeys, err := db.QueryKeys(qx.Query().By("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(current after batch_delete): %v", err)
	}
	if !slices.Equal(currentKeys, []uint64{1, 4}) {
		t.Fatalf("current snapshot missed committed batch_delete: got=%v want=[1 4]", currentKeys)
	}
	currentDeleted, err := db.QueryKeys(qx.Query(qx.IN("name", []string{"bob", "carol"})))
	if err != nil {
		t.Fatalf("QueryKeys(deleted names after batch_delete): %v", err)
	}
	if len(currentDeleted) != 0 {
		t.Fatalf("current snapshot retained batch-deleted rows: %v", currentDeleted)
	}

	db.unpinSnapshotRef(old.seq, holdRef)
	snapshotExtraAssertNoFutureSnapshotRefs(t, db)
}

func TestSnapshotExtra_BeginQueryTxSnapshotSurvivesRealStagedBatchDeleteRollback(t *testing.T) {
	db, _ := snapshotExtraOpenTempDBUint64(t, snapshotExtraOptions())

	if err := db.BatchSet(
		[]uint64{1, 2, 3, 4},
		[]*snapshotExtraRec{
			{Name: "alice", Age: 30},
			{Name: "bob", Age: 20},
			{Name: "carol", Age: 40},
			{Name: "dave", Age: 50},
		},
	); err != nil {
		t.Fatalf("BatchSet(seed): %v", err)
	}

	old := db.getSnapshot()
	pinned, holdRef, ok := db.pinSnapshotRefBySeq(old.seq)
	if !ok || pinned != old {
		t.Fatalf("expected current snapshot to be pinnable")
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	db.testHooks.beforeCommit = func(op string) error {
		if op != "batch_delete" {
			return nil
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return fmt.Errorf("failpoint: rollback batch_delete")
	}
	t.Cleanup(func() {
		db.testHooks.beforeCommit = nil
		select {
		case <-release:
		default:
			close(release)
		}
	})

	done := make(chan error, 1)
	go func() {
		done <- db.BatchDelete([]uint64{2, 2, 3, 3})
	}()

	snapshotExtraWait(t, entered, "rollback batch_delete commit hook")
	if !snapshotExtraHasFutureSnapshot(db, old.seq) {
		t.Fatalf("expected staged future snapshot while batch_delete rollback is blocked")
	}

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}
	if seq != old.seq || snap != old {
		t.Fatalf("beginQueryTxSnapshot pinned wrong snapshot: seq=%d want=%d", seq, old.seq)
	}

	beforeAll := snapshotExtraQueryTxRecords(t, db, tx, snap, qx.Query().By("name", qx.ASC))
	if len(beforeAll) != 4 || beforeAll[0] == nil || beforeAll[1] == nil || beforeAll[2] == nil || beforeAll[3] == nil ||
		beforeAll[0].Name != "alice" || beforeAll[1].Name != "bob" || beforeAll[2].Name != "carol" || beforeAll[3].Name != "dave" {
		t.Fatalf("old tx/snapshot drifted before batch_delete rollback: %#v", beforeAll)
	}

	rollback(tx)
	db.unpinSnapshotRef(seq, ref)

	close(release)
	err = <-done
	if err == nil || !strings.Contains(err.Error(), "failpoint: rollback batch_delete") {
		t.Fatalf("expected rollback failpoint error, got: %v", err)
	}

	if got := db.getSnapshot(); got != old {
		t.Fatalf("rollback replaced published snapshot: got=%p want=%p", got, old)
	}

	oldKeys := snapshotExtraQuerySnapshotKeys(t, db, pinned, qx.Query().By("name", qx.ASC))
	if !slices.Equal(oldKeys, []uint64{1, 2, 3, 4}) {
		t.Fatalf("pinned old snapshot changed after batch_delete rollback: got=%v want=[1 2 3 4]", oldKeys)
	}

	currentKeys, err := db.QueryKeys(qx.Query().By("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(current after rollback): %v", err)
	}
	if !slices.Equal(currentKeys, []uint64{1, 2, 3, 4}) {
		t.Fatalf("current snapshot changed after rolled-back batch_delete: got=%v want=[1 2 3 4]", currentKeys)
	}

	db.unpinSnapshotRef(old.seq, holdRef)
	snapshotExtraAssertNoFutureSnapshotRefs(t, db)
}

func TestSnapshotExtra_MultiGenerationPinnedSnapshotsRemainExactAcrossRotationAndUnpinOrder(t *testing.T) {
	db, _ := snapshotExtraOpenTempDBUint64(t, snapshotExtraOptions())

	if err := db.BatchSet(
		[]uint64{1, 2},
		[]*snapshotExtraRec{
			{Name: "alice", Age: 30},
			{Name: "bob", Age: 20},
		},
	); err != nil {
		t.Fatalf("BatchSet(seed): %v", err)
	}

	snapA := db.getSnapshot()
	pinnedA, refA, ok := db.pinSnapshotRefBySeq(snapA.seq)
	if !ok || pinnedA != snapA {
		t.Fatalf("expected snapshot A to be pinnable")
	}

	if err := db.Patch(2, []Field{{Name: "name", Value: "carol"}}); err != nil {
		t.Fatalf("Patch(2 name): %v", err)
	}

	snapB := db.getSnapshot()
	pinnedB, refB, ok := db.pinSnapshotRefBySeq(snapB.seq)
	if !ok || pinnedB != snapB {
		t.Fatalf("expected snapshot B to be pinnable")
	}

	if err := db.Delete(1); err != nil {
		t.Fatalf("Delete(1): %v", err)
	}

	snapC := db.getSnapshot()
	pinnedC, refC, ok := db.pinSnapshotRefBySeq(snapC.seq)
	if !ok || pinnedC != snapC {
		t.Fatalf("expected snapshot C to be pinnable")
	}

	if err := db.Set(3, &snapshotExtraRec{Name: "dave", Age: 50}); err != nil {
		t.Fatalf("Set(3): %v", err)
	}

	current := db.getSnapshot()
	if current.seq == snapC.seq {
		t.Fatalf("expected a newer snapshot after final Set")
	}

	checkKeys := func(label string, snap *indexSnapshot, want []uint64) {
		t.Helper()
		got := snapshotExtraQuerySnapshotKeys(t, db, snap, qx.Query().By("name", qx.ASC))
		if !slices.Equal(got, want) {
			t.Fatalf("%s snapshot mismatch: got=%v want=%v", label, got, want)
		}
	}

	checkKeys("A", pinnedA, []uint64{1, 2})
	checkKeys("B", pinnedB, []uint64{1, 2})
	checkKeys("C", pinnedC, []uint64{2})

	currentKeys, err := db.QueryKeys(qx.Query().By("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(current): %v", err)
	}
	if !slices.Equal(currentKeys, []uint64{2, 3}) {
		t.Fatalf("current snapshot mismatch: got=%v want=[2 3]", currentKeys)
	}

	db.snapshot.mu.RLock()
	registrySize := len(db.snapshot.bySeq)
	heldA := db.snapshot.bySeq[snapA.seq]
	heldB := db.snapshot.bySeq[snapB.seq]
	heldC := db.snapshot.bySeq[snapC.seq]
	heldCurrent := db.snapshot.bySeq[current.seq]
	db.snapshot.mu.RUnlock()

	if registrySize != 4 {
		t.Fatalf("expected 4 snapshot refs after three pins and one current, got=%d", registrySize)
	}
	if heldA != refA || heldA == nil || heldA.snap != nil {
		t.Fatalf("expected snapshot A to stay retired but pinned")
	}
	if heldB != refB || heldB == nil || heldB.snap != nil {
		t.Fatalf("expected snapshot B to stay retired but pinned")
	}
	if heldC != refC || heldC == nil || heldC.snap != nil {
		t.Fatalf("expected snapshot C to stay retired but pinned")
	}
	if heldCurrent == nil || heldCurrent.snap != current {
		t.Fatalf("expected current snapshot ref to remain published")
	}

	db.unpinSnapshotRef(snapB.seq, refB)
	checkKeys("A after unpin B", pinnedA, []uint64{1, 2})
	checkKeys("C after unpin B", pinnedC, []uint64{2})

	db.snapshot.mu.RLock()
	_, hasAAfterB := db.snapshot.bySeq[snapA.seq]
	_, hasBAfterB := db.snapshot.bySeq[snapB.seq]
	_, hasCAfterB := db.snapshot.bySeq[snapC.seq]
	registryAfterB := len(db.snapshot.bySeq)
	db.snapshot.mu.RUnlock()
	if !hasAAfterB || hasBAfterB || !hasCAfterB || registryAfterB != 3 {
		t.Fatalf("unexpected registry after unpin B: hasA=%v hasB=%v hasC=%v size=%d", hasAAfterB, hasBAfterB, hasCAfterB, registryAfterB)
	}

	db.unpinSnapshotRef(snapA.seq, refA)
	checkKeys("C after unpin A", pinnedC, []uint64{2})

	db.snapshot.mu.RLock()
	_, hasAAfterA := db.snapshot.bySeq[snapA.seq]
	_, hasCAfterA := db.snapshot.bySeq[snapC.seq]
	registryAfterA := len(db.snapshot.bySeq)
	db.snapshot.mu.RUnlock()
	if hasAAfterA || !hasCAfterA || registryAfterA != 2 {
		t.Fatalf("unexpected registry after unpin A: hasA=%v hasC=%v size=%d", hasAAfterA, hasCAfterA, registryAfterA)
	}

	db.unpinSnapshotRef(snapC.seq, refC)
	db.snapshot.mu.RLock()
	_, hasCAfterC := db.snapshot.bySeq[snapC.seq]
	_, hasCurrentAfterC := db.snapshot.bySeq[current.seq]
	registryAfterC := len(db.snapshot.bySeq)
	db.snapshot.mu.RUnlock()
	if hasCAfterC || !hasCurrentAfterC || registryAfterC != 1 {
		t.Fatalf("unexpected registry after unpin C: hasC=%v hasCurrent=%v size=%d", hasCAfterC, hasCurrentAfterC, registryAfterC)
	}

	snapshotExtraAssertNoFutureSnapshotRefs(t, db)
}

func TestSnapshotExtra_BeginQueryTxSnapshotIgnoresRealStagedTruncateBeforeCommit(t *testing.T) {
	db, _ := snapshotExtraOpenTempDBUint64(t, snapshotExtraOptions())

	if err := db.BatchSet(
		[]uint64{1, 2, 3},
		[]*snapshotExtraRec{
			{Name: "alice", Age: 30},
			{Name: "bob", Age: 20},
			{Name: "carol", Age: 40},
		},
	); err != nil {
		t.Fatalf("BatchSet(seed): %v", err)
	}

	old := db.getSnapshot()
	pinned, holdRef, ok := db.pinSnapshotRefBySeq(old.seq)
	if !ok || pinned != old {
		t.Fatalf("expected current snapshot to be pinnable")
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	db.testHooks.beforeCommit = func(op string) error {
		if op != "truncate" {
			return nil
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return nil
	}
	t.Cleanup(func() {
		db.testHooks.beforeCommit = nil
		select {
		case <-release:
		default:
			close(release)
		}
	})

	done := make(chan error, 1)
	go func() {
		done <- db.Truncate()
	}()

	snapshotExtraWait(t, entered, "staged truncate commit hook")
	if !snapshotExtraHasFutureSnapshot(db, old.seq) {
		t.Fatalf("expected staged future snapshot while truncate commit is blocked")
	}
	if got := db.getSnapshot(); got != old {
		t.Fatalf("current snapshot changed before truncate publish")
	}

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}
	if seq != old.seq || snap != old {
		t.Fatalf("beginQueryTxSnapshot pinned wrong snapshot: seq=%d want=%d", seq, old.seq)
	}

	beforeAll := snapshotExtraQueryTxRecords(t, db, tx, snap, qx.Query().By("name", qx.ASC))
	if len(beforeAll) != 3 || beforeAll[0] == nil || beforeAll[1] == nil || beforeAll[2] == nil ||
		beforeAll[0].Name != "alice" || beforeAll[1].Name != "bob" || beforeAll[2].Name != "carol" {
		t.Fatalf("old tx/snapshot drifted before truncate publish: %#v", beforeAll)
	}

	rollback(tx)
	db.unpinSnapshotRef(seq, ref)

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	oldKeys := snapshotExtraQuerySnapshotKeys(t, db, pinned, qx.Query().By("name", qx.ASC))
	if !slices.Equal(oldKeys, []uint64{1, 2, 3}) {
		t.Fatalf("pinned old snapshot changed after truncate publish: got=%v want=[1 2 3]", oldKeys)
	}

	currentKeys, err := db.QueryKeys(qx.Query().By("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(current after truncate): %v", err)
	}
	if len(currentKeys) != 0 {
		t.Fatalf("current snapshot missed committed truncate: got=%v want=[]", currentKeys)
	}

	db.unpinSnapshotRef(old.seq, holdRef)
	snapshotExtraAssertNoFutureSnapshotRefs(t, db)
}

func TestSnapshotExtra_BeginQueryTxSnapshotSurvivesRealStagedTruncateRollback(t *testing.T) {
	db, _ := snapshotExtraOpenTempDBUint64(t, snapshotExtraOptions())

	if err := db.BatchSet(
		[]uint64{1, 2, 3},
		[]*snapshotExtraRec{
			{Name: "alice", Age: 30},
			{Name: "bob", Age: 20},
			{Name: "carol", Age: 40},
		},
	); err != nil {
		t.Fatalf("BatchSet(seed): %v", err)
	}

	old := db.getSnapshot()
	pinned, holdRef, ok := db.pinSnapshotRefBySeq(old.seq)
	if !ok || pinned != old {
		t.Fatalf("expected current snapshot to be pinnable")
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	db.testHooks.beforeCommit = func(op string) error {
		if op != "truncate" {
			return nil
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return fmt.Errorf("failpoint: rollback truncate")
	}
	t.Cleanup(func() {
		db.testHooks.beforeCommit = nil
		select {
		case <-release:
		default:
			close(release)
		}
	})

	done := make(chan error, 1)
	go func() {
		done <- db.Truncate()
	}()

	snapshotExtraWait(t, entered, "rollback truncate commit hook")
	if !snapshotExtraHasFutureSnapshot(db, old.seq) {
		t.Fatalf("expected staged future snapshot while truncate rollback is blocked")
	}

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}
	if seq != old.seq || snap != old {
		t.Fatalf("beginQueryTxSnapshot pinned wrong snapshot: seq=%d want=%d", seq, old.seq)
	}

	beforeAll := snapshotExtraQueryTxRecords(t, db, tx, snap, qx.Query().By("name", qx.ASC))
	if len(beforeAll) != 3 || beforeAll[0] == nil || beforeAll[1] == nil || beforeAll[2] == nil ||
		beforeAll[0].Name != "alice" || beforeAll[1].Name != "bob" || beforeAll[2].Name != "carol" {
		t.Fatalf("old tx/snapshot drifted before truncate rollback: %#v", beforeAll)
	}

	rollback(tx)
	db.unpinSnapshotRef(seq, ref)

	close(release)
	err = <-done
	if err == nil || !strings.Contains(err.Error(), "failpoint: rollback truncate") {
		t.Fatalf("expected rollback failpoint error, got: %v", err)
	}

	if got := db.getSnapshot(); got != old {
		t.Fatalf("rollback replaced published snapshot: got=%p want=%p", got, old)
	}

	oldKeys := snapshotExtraQuerySnapshotKeys(t, db, pinned, qx.Query().By("name", qx.ASC))
	if !slices.Equal(oldKeys, []uint64{1, 2, 3}) {
		t.Fatalf("pinned old snapshot changed after truncate rollback: got=%v want=[1 2 3]", oldKeys)
	}

	currentKeys, err := db.QueryKeys(qx.Query().By("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(current after rollback): %v", err)
	}
	if !slices.Equal(currentKeys, []uint64{1, 2, 3}) {
		t.Fatalf("current snapshot changed after rolled-back truncate: got=%v want=[1 2 3]", currentKeys)
	}

	db.unpinSnapshotRef(old.seq, holdRef)
	snapshotExtraAssertNoFutureSnapshotRefs(t, db)
}

func TestSnapshotExtra_DuplicatePinsOnSameRetiredSnapshotPruneOnlyOnLastUnpin(t *testing.T) {
	db, _ := snapshotExtraOpenTempDBUint64(t, snapshotExtraOptions())

	if err := db.BatchSet(
		[]uint64{1, 2},
		[]*snapshotExtraRec{
			{Name: "alice", Age: 30},
			{Name: "bob", Age: 20},
		},
	); err != nil {
		t.Fatalf("BatchSet(seed): %v", err)
	}

	old := db.getSnapshot()
	pinned1, ref1, ok := db.pinSnapshotRefBySeq(old.seq)
	if !ok || pinned1 != old {
		t.Fatalf("expected first pin to succeed")
	}
	pinned2, ref2, ok := db.pinSnapshotRefBySeq(old.seq)
	if !ok || pinned2 != old {
		t.Fatalf("expected second pin to succeed")
	}
	if ref1 != ref2 {
		t.Fatalf("expected duplicate pins to reuse same snapshot ref")
	}

	before := db.SnapshotStats()
	if before.Sequence != old.seq || before.RegistrySize != 1 || before.PinnedRefs != 1 {
		t.Fatalf("unexpected stats before publish with duplicate pins: %+v", before)
	}

	if err := db.Set(3, &snapshotExtraRec{Name: "carol", Age: 40}); err != nil {
		t.Fatalf("Set(3): %v", err)
	}

	current := db.getSnapshot()
	if current.seq == old.seq {
		t.Fatalf("expected publish to advance snapshot sequence")
	}

	oldKeys := snapshotExtraQuerySnapshotKeys(t, db, pinned1, qx.Query().By("name", qx.ASC))
	if !slices.Equal(oldKeys, []uint64{1, 2}) {
		t.Fatalf("pinned old snapshot changed after publish: got=%v want=[1 2]", oldKeys)
	}

	db.snapshot.mu.RLock()
	held := db.snapshot.bySeq[old.seq]
	db.snapshot.mu.RUnlock()
	if held != ref1 || held == nil || held.snap != nil || held.refs.Load() != 2 {
		t.Fatalf("expected retired snapshot ref to remain held by two pins")
	}

	mid := db.SnapshotStats()
	if mid.Sequence != current.seq || mid.RegistrySize != 2 || mid.PinnedRefs != 1 {
		t.Fatalf("unexpected stats after publish with duplicate retired pins: %+v", mid)
	}

	db.unpinSnapshotRef(old.seq, ref1)

	stillKeys := snapshotExtraQuerySnapshotKeys(t, db, pinned2, qx.Query().By("name", qx.ASC))
	if !slices.Equal(stillKeys, []uint64{1, 2}) {
		t.Fatalf("remaining pin lost old snapshot view after first unpin: got=%v want=[1 2]", stillKeys)
	}

	db.snapshot.mu.RLock()
	held = db.snapshot.bySeq[old.seq]
	registryAfterOne := len(db.snapshot.bySeq)
	db.snapshot.mu.RUnlock()
	heldRefs := int64(-1)
	if held != nil {
		heldRefs = held.refs.Load()
	}
	if held != ref2 || held == nil || held.snap != nil || heldRefs != 1 || registryAfterOne != 2 {
		t.Fatalf("unexpected state after first unpin: held=%p refs=%d registry=%d", held, heldRefs, registryAfterOne)
	}

	afterOne := db.SnapshotStats()
	if afterOne.RegistrySize != 2 || afterOne.PinnedRefs != 1 {
		t.Fatalf("unexpected stats after first unpin: %+v", afterOne)
	}

	db.unpinSnapshotRef(old.seq, ref2)

	db.snapshot.mu.RLock()
	_, oldPresent := db.snapshot.bySeq[old.seq]
	_, currentPresent := db.snapshot.bySeq[current.seq]
	registryAfterTwo := len(db.snapshot.bySeq)
	db.snapshot.mu.RUnlock()
	if oldPresent || !currentPresent || registryAfterTwo != 1 {
		t.Fatalf("unexpected registry after last unpin: old=%v current=%v size=%d", oldPresent, currentPresent, registryAfterTwo)
	}

	afterTwo := db.SnapshotStats()
	if afterTwo.Sequence != current.seq || afterTwo.RegistrySize != 1 || afterTwo.PinnedRefs != 0 {
		t.Fatalf("unexpected stats after last unpin: %+v", afterTwo)
	}
}

func TestSnapshotExtra_PinnedOldMaterializedPredCacheSurvivesTruncateRebuild(t *testing.T) {
	opts := snapshotExtraOptions()
	opts.SnapshotMaterializedPredCacheMaxEntries = 64
	db, _ := snapshotExtraOpenTempDBUint64(t, opts)

	if err := db.BatchSet(
		[]uint64{1, 2, 3},
		[]*snapshotExtraRec{
			{Name: "alice", Email: "alpha-one@example.com"},
			{Name: "bob", Email: "beta@example.com"},
			{Name: "carol", Email: "alpha-two@example.com"},
		},
	); err != nil {
		t.Fatalf("BatchSet(seed): %v", err)
	}

	old := db.getSnapshot()
	alphaQ := qx.Query(qx.PREFIX("email", "alpha"))
	futureQ := qx.Query(qx.PREFIX("email", "future"))
	alphaKey := snapshotExtraMaterializedPredCacheKey(t, db, qx.Expr{Op: qx.OpPREFIX, Field: "email", Value: "alpha"})
	futureKey := snapshotExtraMaterializedPredCacheKey(t, db, qx.Expr{Op: qx.OpPREFIX, Field: "email", Value: "future"})

	oldAlpha, err := db.QueryKeys(alphaQ)
	if err != nil {
		t.Fatalf("QueryKeys(alpha warm): %v", err)
	}
	if !slices.Equal(oldAlpha, []uint64{1, 3}) {
		t.Fatalf("unexpected alpha warm result: got=%v want=[1 3]", oldAlpha)
	}
	oldFuture, err := db.QueryKeys(futureQ)
	if err != nil {
		t.Fatalf("QueryKeys(future warm): %v", err)
	}
	if len(oldFuture) != 0 {
		t.Fatalf("unexpected future warm result: %v", oldFuture)
	}
	if ids, ok := old.loadMaterializedPred(alphaKey); !ok || ids.Cardinality() != 2 || !ids.Contains(1) || !ids.Contains(3) {
		t.Fatalf("expected warmed alpha cache on old snapshot")
	}
	if ids, ok := old.loadMaterializedPred(futureKey); !ok || !ids.IsEmpty() {
		t.Fatalf("expected warmed negative future cache on old snapshot")
	}

	pinned, ref, ok := db.pinSnapshotRefBySeq(old.seq)
	if !ok || pinned != old {
		t.Fatalf("expected old snapshot to be pinnable")
	}

	if err := db.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if err := db.BatchSet(
		[]uint64{1, 2},
		[]*snapshotExtraRec{
			{Name: "new-future", Email: "future@example.com"},
			{Name: "new-gamma", Email: "gamma@example.com"},
		},
	); err != nil {
		t.Fatalf("BatchSet(rebuild): %v", err)
	}

	current := db.getSnapshot()
	if _, ok := current.loadMaterializedPred(alphaKey); ok {
		t.Fatalf("current snapshot inherited stale alpha cache across truncate rebuild")
	}
	if _, ok := current.loadMaterializedPred(futureKey); ok {
		t.Fatalf("current snapshot inherited stale future cache across truncate rebuild")
	}

	pinnedAlpha := snapshotExtraQuerySnapshotKeys(t, db, pinned, alphaQ)
	if !slices.Equal(pinnedAlpha, []uint64{1, 3}) {
		t.Fatalf("pinned old snapshot lost alpha cache result after truncate rebuild: got=%v want=[1 3]", pinnedAlpha)
	}
	pinnedFuture := snapshotExtraQuerySnapshotKeys(t, db, pinned, futureQ)
	if len(pinnedFuture) != 0 {
		t.Fatalf("pinned old snapshot lost negative future result after truncate rebuild: %v", pinnedFuture)
	}

	currentAlpha, err := db.QueryKeys(alphaQ)
	if err != nil {
		t.Fatalf("QueryKeys(current alpha): %v", err)
	}
	if len(currentAlpha) != 0 {
		t.Fatalf("current snapshot retained stale alpha match after truncate rebuild: %v", currentAlpha)
	}
	currentFuture, err := db.QueryKeys(futureQ)
	if err != nil {
		t.Fatalf("QueryKeys(current future): %v", err)
	}
	if !slices.Equal(currentFuture, []uint64{1}) {
		t.Fatalf("current snapshot missed rebuilt future match: got=%v want=[1]", currentFuture)
	}

	if ids, ok := pinned.loadMaterializedPred(alphaKey); !ok || ids.Cardinality() != 2 || !ids.Contains(1) || !ids.Contains(3) {
		t.Fatalf("old alpha cache entry was corrupted by current rebuild queries")
	}
	if ids, ok := pinned.loadMaterializedPred(futureKey); !ok || !ids.IsEmpty() {
		t.Fatalf("old future negative cache entry was corrupted by current rebuild queries")
	}
	if ids, ok := current.loadMaterializedPred(alphaKey); !ok || !ids.IsEmpty() {
		t.Fatalf("current alpha cache entry not rebuilt as empty after query")
	}
	if ids, ok := current.loadMaterializedPred(futureKey); !ok || ids.Cardinality() != 1 || !ids.Contains(1) {
		t.Fatalf("current future cache entry not rebuilt correctly after query")
	}

	db.unpinSnapshotRef(old.seq, ref)
}

func TestSnapshotExtra_PinnedOldNumericRangeBucketCacheSurvivesTruncateRebuild(t *testing.T) {
	db, _ := snapshotExtraOpenTempDBUint64(t, snapshotExtraOptions())

	snapshotExtraSeedGeneratedUint64Data(t, db, 512, func(i int) *snapshotExtraRec {
		return &snapshotExtraRec{
			Name: fmt.Sprintf("user-%04d", i),
			Age:  i,
		}
	})
	snapshotExtraSetNumericBucketKnobs(t, db, 64, 1, 1)

	old := db.getSnapshot()
	oldHighQ := qx.Query(qx.GTE("age", 400))
	oldHigh, err := db.QueryKeys(oldHighQ)
	if err != nil {
		t.Fatalf("QueryKeys(old high warm): %v", err)
	}
	if len(oldHigh) != 113 || !slices.Contains(oldHigh, uint64(400)) || !slices.Contains(oldHigh, uint64(512)) {
		t.Fatalf("unexpected old high range result: len=%d ids=%v", len(oldHigh), oldHigh)
	}
	oldEntry := snapshotExtraRequireNumericRangeBucketCacheEntry(t, old, "age")

	pinned, ref, ok := db.pinSnapshotRefBySeq(old.seq)
	if !ok || pinned != old {
		t.Fatalf("expected old snapshot to be pinnable")
	}

	if err := db.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	snapshotExtraSeedGeneratedUint64Data(t, db, 128, func(i int) *snapshotExtraRec {
		return &snapshotExtraRec{
			Name: fmt.Sprintf("rebuild-%03d", i),
			Age:  i,
		}
	})

	current := db.getSnapshot()
	if _, ok := current.numericRangeBucketCache.loadField("age"); ok {
		t.Fatalf("current snapshot inherited stale numeric range cache across truncate rebuild")
	}

	pinnedHigh := snapshotExtraQuerySnapshotKeys(t, db, pinned, oldHighQ)
	if len(pinnedHigh) != 113 || !slices.Contains(pinnedHigh, uint64(400)) || !slices.Contains(pinnedHigh, uint64(512)) {
		t.Fatalf("pinned old numeric range drift after truncate rebuild: len=%d ids=%v", len(pinnedHigh), pinnedHigh)
	}

	currentHigh, err := db.QueryKeys(oldHighQ)
	if err != nil {
		t.Fatalf("QueryKeys(current high): %v", err)
	}
	if len(currentHigh) != 0 {
		t.Fatalf("current snapshot retained stale high-age range after rebuild: %v", currentHigh)
	}

	currentMidQ := qx.Query(qx.GTE("age", 64))
	currentMid, err := db.QueryKeys(currentMidQ)
	if err != nil {
		t.Fatalf("QueryKeys(current mid): %v", err)
	}
	if len(currentMid) != 65 || !slices.Contains(currentMid, uint64(64)) || !slices.Contains(currentMid, uint64(128)) {
		t.Fatalf("unexpected current mid range result after rebuild: len=%d ids=%v", len(currentMid), currentMid)
	}
	currentEntry := snapshotExtraRequireNumericRangeBucketCacheEntry(t, db.getSnapshot(), "age")
	if currentEntry == oldEntry {
		t.Fatalf("current numeric range cache reused old pinned snapshot entry across rebuild")
	}

	pinnedHighAgain := snapshotExtraQuerySnapshotKeys(t, db, pinned, oldHighQ)
	if len(pinnedHighAgain) != 113 || !slices.Contains(pinnedHighAgain, uint64(400)) || !slices.Contains(pinnedHighAgain, uint64(512)) {
		t.Fatalf("old numeric range cache was corrupted by current rebuild queries: len=%d ids=%v", len(pinnedHighAgain), pinnedHighAgain)
	}

	db.unpinSnapshotRef(old.seq, ref)
}

func TestSnapshotExtra_SnapshotStatsCountsDistinctPinnedSnapshotsAcrossRotation(t *testing.T) {
	db, _ := snapshotExtraOpenTempDBUint64(t, snapshotExtraOptions())

	if err := db.BatchSet(
		[]uint64{1, 2},
		[]*snapshotExtraRec{
			{Name: "alice", Age: 30},
			{Name: "bob", Age: 20},
		},
	); err != nil {
		t.Fatalf("BatchSet(seed): %v", err)
	}

	snapA := db.getSnapshot()
	_, refA, ok := db.pinSnapshotRefBySeq(snapA.seq)
	if !ok {
		t.Fatalf("expected snapshot A to be pinnable")
	}

	if err := db.Patch(2, []Field{{Name: "name", Value: "carol"}}); err != nil {
		t.Fatalf("Patch(2): %v", err)
	}

	snapB := db.getSnapshot()
	_, refB, ok := db.pinSnapshotRefBySeq(snapB.seq)
	if !ok {
		t.Fatalf("expected snapshot B to be pinnable")
	}

	if err := db.Set(3, &snapshotExtraRec{Name: "dave", Age: 40}); err != nil {
		t.Fatalf("Set(3): %v", err)
	}

	current := db.getSnapshot()
	mid := db.SnapshotStats()
	if mid.Sequence != current.seq || mid.RegistrySize != 3 || mid.PinnedRefs != 2 {
		t.Fatalf("unexpected stats with two distinct retired pinned snapshots: %+v", mid)
	}

	db.unpinSnapshotRef(snapA.seq, refA)
	afterA := db.SnapshotStats()
	if afterA.Sequence != current.seq || afterA.RegistrySize != 2 || afterA.PinnedRefs != 1 {
		t.Fatalf("unexpected stats after unpin snapshot A: %+v", afterA)
	}

	db.unpinSnapshotRef(snapB.seq, refB)
	afterB := db.SnapshotStats()
	if afterB.Sequence != current.seq || afterB.RegistrySize != 1 || afterB.PinnedRefs != 0 {
		t.Fatalf("unexpected stats after unpin snapshot B: %+v", afterB)
	}
}

func TestSnapshotExtra_PinnedOldMaterializedPredCacheStaysExactAcrossConcurrentTouchedPublishes(t *testing.T) {
	opts := snapshotExtraOptions()
	opts.SnapshotMaterializedPredCacheMaxEntries = 64
	db, _ := snapshotExtraOpenTempDBUint64(t, opts)

	const total = 512
	snapshotExtraSeedGeneratedUint64Data(t, db, total, func(i int) *snapshotExtraRec {
		email := fmt.Sprintf("beta-%04d@example.com", i)
		if i%2 == 1 {
			email = fmt.Sprintf("alpha-%04d@example.com", i)
		}
		return &snapshotExtraRec{
			Name:  fmt.Sprintf("user-%04d", i),
			Email: email,
			Age:   i,
		}
	})

	old := db.getSnapshot()
	alphaExpr := qx.Expr{Op: qx.OpPREFIX, Field: "email", Value: "alpha"}
	alphaQ := qx.Query(qx.PREFIX("email", "alpha"))
	wantOld, err := db.QueryKeys(alphaQ)
	if err != nil {
		t.Fatalf("QueryKeys(alpha warm): %v", err)
	}
	alphaKey := snapshotExtraMaterializedPredCacheKey(t, db, alphaExpr)
	cachedOld, ok := old.loadMaterializedPred(alphaKey)
	if !ok {
		t.Fatalf("expected warmed old alpha cache entry")
	}
	defer cachedOld.Release()

	pinned, ref, ok := db.pinSnapshotRefBySeq(old.seq)
	if !ok || pinned != old {
		t.Fatalf("expected old snapshot to be pinnable")
	}
	defer db.unpinSnapshotRef(old.seq, ref)

	queryPinnedAlpha := func() ([]uint64, error) {
		view := db.makeQueryView(pinned)
		defer db.releaseQueryView(view)
		return view.execQuery(alphaQ, false, false)
	}

	var failed atomic.Pointer[string]
	setFailed := func(msg string) {
		if failed.Load() != nil {
			return
		}
		copyMsg := msg
		failed.CompareAndSwap(nil, &copyMsg)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup

	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			<-start
			for i := 0; i < 80; i++ {
				if failed.Load() != nil {
					return
				}
				id := uint64(1 + r.IntN(total))
				email := fmt.Sprintf("gamma-%d-%d@example.com", seed, i)
				if i%3 == 0 {
					email = fmt.Sprintf("alpha-live-%d-%d@example.com", seed, i)
				}
				if err := db.Patch(id, []Field{{Name: "email", Value: email}}); err != nil {
					setFailed(fmt.Sprintf("Patch(email) failed: %v", err))
					return
				}
			}
		}(int64(20261001 + w))
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 120; i++ {
			if failed.Load() != nil {
				return
			}
			if _, err := db.QueryKeys(alphaQ); err != nil {
				setFailed(fmt.Sprintf("current QueryKeys(alpha) failed: %v", err))
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 120; i++ {
			if failed.Load() != nil {
				return
			}
			got, err := queryPinnedAlpha()
			if err != nil {
				setFailed(fmt.Sprintf("pinned old alpha query failed: %v", err))
				return
			}
			if !slices.Equal(got, wantOld) {
				setFailed(fmt.Sprintf("pinned old alpha query mismatch: got=%v want=%v", got, wantOld))
				return
			}
		}
	}()

	close(start)
	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}

	gotFinal, err := queryPinnedAlpha()
	if err != nil {
		t.Fatalf("pinned old alpha query after churn: %v", err)
	}
	if !slices.Equal(gotFinal, wantOld) {
		t.Fatalf("pinned old alpha query drift after churn: got=%v want=%v", gotFinal, wantOld)
	}

	cachedPinned, ok := pinned.loadMaterializedPred(alphaKey)
	if !ok {
		t.Fatalf("expected old pinned alpha cache entry after churn")
	}
	defer cachedPinned.Release()
	if cachedPinned.Cardinality() != cachedOld.Cardinality() {
		t.Fatalf("old pinned alpha cache cardinality drift: got=%d want=%d", cachedPinned.Cardinality(), cachedOld.Cardinality())
	}
	for _, id := range wantOld {
		if !cachedPinned.Contains(id) {
			t.Fatalf("old pinned alpha cache lost id=%d after churn", id)
		}
	}
}

func TestSnapshotExtra_PinnedOldNumericRangeCacheStaysExactAcrossConcurrentAgeChurn(t *testing.T) {
	db, _ := snapshotExtraOpenTempDBUint64(t, snapshotExtraOptions())

	const total = 2048
	snapshotExtraSeedGeneratedUint64Data(t, db, total, func(i int) *snapshotExtraRec {
		return &snapshotExtraRec{
			Name: fmt.Sprintf("user-%04d", i),
			Age:  i,
		}
	})
	snapshotExtraSetNumericBucketKnobs(t, db, 64, 1, 1)

	old := db.getSnapshot()
	rangeQ := qx.Query(qx.GTE("age", 1536))
	wantOld, err := db.QueryKeys(rangeQ)
	if err != nil {
		t.Fatalf("QueryKeys(range warm): %v", err)
	}
	oldEntry := snapshotExtraRequireNumericRangeBucketCacheEntry(t, old, "age")

	pinned, ref, ok := db.pinSnapshotRefBySeq(old.seq)
	if !ok || pinned != old {
		t.Fatalf("expected old snapshot to be pinnable")
	}
	defer db.unpinSnapshotRef(old.seq, ref)

	queryPinnedRange := func() ([]uint64, error) {
		view := db.makeQueryView(pinned)
		defer db.releaseQueryView(view)
		return view.execQuery(rangeQ, false, false)
	}

	var failed atomic.Pointer[string]
	setFailed := func(msg string) {
		if failed.Load() != nil {
			return
		}
		copyMsg := msg
		failed.CompareAndSwap(nil, &copyMsg)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup

	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			<-start
			for i := 0; i < 80; i++ {
				if failed.Load() != nil {
					return
				}
				id := uint64(1 + r.IntN(total))
				age := 1 + r.IntN(total*2)
				if err := db.Patch(id, []Field{{Name: "age", Value: age}}); err != nil {
					setFailed(fmt.Sprintf("Patch(age) failed: %v", err))
					return
				}
			}
		}(int64(20261011 + w))
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 120; i++ {
			if failed.Load() != nil {
				return
			}
			if _, err := db.QueryKeys(rangeQ); err != nil {
				setFailed(fmt.Sprintf("current QueryKeys(range) failed: %v", err))
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 120; i++ {
			if failed.Load() != nil {
				return
			}
			got, err := queryPinnedRange()
			if err != nil {
				setFailed(fmt.Sprintf("pinned old range query failed: %v", err))
				return
			}
			if !slices.Equal(got, wantOld) {
				setFailed(fmt.Sprintf("pinned old range query mismatch: got=%v want=%v", got, wantOld))
				return
			}
		}
	}()

	close(start)
	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}

	gotFinal, err := queryPinnedRange()
	if err != nil {
		t.Fatalf("pinned old range query after churn: %v", err)
	}
	if !slices.Equal(gotFinal, wantOld) {
		t.Fatalf("pinned old range query drift after age churn: got=%v want=%v", gotFinal, wantOld)
	}

	pinnedEntry := snapshotExtraRequireNumericRangeBucketCacheEntry(t, pinned, "age")
	if pinnedEntry != oldEntry {
		t.Fatalf("old pinned numeric range cache entry changed unexpectedly during churn")
	}
}

func TestSnapshotExtra_PinnedStringSnapshotScanStaysExactAcrossConcurrentTruncateRebuilds(t *testing.T) {
	db, _ := snapshotExtraOpenTempDBString(t, snapshotExtraOptions())

	const seedN = 128
	keys := make([]string, 0, seedN)
	vals := make([]*snapshotExtraRec, 0, seedN)
	for i := 1; i <= seedN; i++ {
		key := fmt.Sprintf("k%03d", i)
		keys = append(keys, key)
		vals = append(vals, &snapshotExtraRec{
			Name:  fmt.Sprintf("seed-%03d", i),
			Email: fmt.Sprintf("%s@example.com", key),
			Age:   i,
		})
	}
	if err := db.BatchSet(keys, vals); err != nil {
		t.Fatalf("BatchSet(seed): %v", err)
	}

	old := db.getSnapshot()
	pinned, ref, ok := db.pinSnapshotRefBySeq(old.seq)
	if !ok || pinned != old {
		t.Fatalf("expected old snapshot to be pinnable")
	}
	defer db.unpinSnapshotRef(old.seq, ref)

	wantOld := snapshotExtraScanSnapshotStringKeys(t, db, pinned, "")

	scanPinned := func() ([]string, error) {
		iter := pinned.universe.Iter()
		defer iter.Release()

		var out []string
		if err := db.scanStringKeys(pinned.strmap, pinned.universe, iter, "", func(id string) (bool, error) {
			out = append(out, id)
			return true, nil
		}); err != nil {
			return nil, err
		}
		return out, nil
	}

	var failed atomic.Pointer[string]
	setFailed := func(msg string) {
		if failed.Load() != nil {
			return
		}
		copyMsg := msg
		failed.CompareAndSwap(nil, &copyMsg)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for round := 0; round < 12; round++ {
			if failed.Load() != nil {
				return
			}
			if err := db.Truncate(); err != nil {
				setFailed(fmt.Sprintf("Truncate failed: %v", err))
				return
			}
			rebuildKeys := make([]string, 0, 16)
			rebuildVals := make([]*snapshotExtraRec, 0, 16)
			for i := 0; i < 16; i++ {
				key := fmt.Sprintf("live-%02d-%02d", round, i)
				rebuildKeys = append(rebuildKeys, key)
				rebuildVals = append(rebuildVals, &snapshotExtraRec{
					Name:  key,
					Email: fmt.Sprintf("%s@example.com", key),
					Age:   round*100 + i,
				})
			}
			if err := db.BatchSet(rebuildKeys, rebuildVals); err != nil {
				setFailed(fmt.Sprintf("BatchSet(rebuild) failed: %v", err))
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 180; i++ {
			if failed.Load() != nil {
				return
			}
			got, err := scanPinned()
			if err != nil {
				setFailed(fmt.Sprintf("pinned old string scan failed: %v", err))
				return
			}
			if !slices.Equal(got, wantOld) {
				setFailed(fmt.Sprintf("pinned old string scan mismatch: got=%v want=%v", got, wantOld))
				return
			}
		}
	}()

	close(start)
	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}

	gotFinal, err := scanPinned()
	if err != nil {
		t.Fatalf("pinned old string scan after truncate churn: %v", err)
	}
	if !slices.Equal(gotFinal, wantOld) {
		t.Fatalf("pinned old string scan drift after truncate churn: got=%v want=%v", gotFinal, wantOld)
	}
}

func TestSnapshotExtra_RebuildIndexSameSeqPinnedOldSnapshotIsReleasedAfterLastUnpin(t *testing.T) {
	opts := snapshotExtraOptions()
	opts.SnapshotMaterializedPredCacheMaxEntries = 64
	db, _ := snapshotExtraOpenTempDBUint64(t, opts)

	snapshotExtraSeedGeneratedUint64Data(t, db, 512, func(i int) *snapshotExtraRec {
		return &snapshotExtraRec{
			Name:  fmt.Sprintf("user-%04d", i),
			Email: fmt.Sprintf("user-%04d@example.com", i),
			Age:   i,
		}
	})
	snapshotExtraSetNumericBucketKnobs(t, db, 64, 1, 1)

	old := db.getSnapshot()
	seq := old.seq

	if _, err := db.QueryKeys(qx.Query(qx.PREFIX("email", "user-00"))); err != nil {
		t.Fatalf("QueryKeys(warm materialized pred): %v", err)
	}
	if _, err := db.QueryKeys(qx.Query(qx.GTE("age", 400))); err != nil {
		t.Fatalf("QueryKeys(warm numeric range): %v", err)
	}
	if old.matPredCache == nil || old.matPredCacheCount.Load() == 0 {
		t.Fatalf("expected warmed materialized predicate cache on old snapshot")
	}
	if old.numericRangeBucketCache == nil || old.numericRangeBucketCache.entryCount() == 0 {
		t.Fatalf("expected warmed numeric range cache on old snapshot")
	}

	pinned, ref, ok := db.pinSnapshotRefBySeq(seq)
	if !ok || pinned != old {
		t.Fatalf("expected old snapshot to be pinnable")
	}

	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	current := db.getSnapshot()
	if current == old {
		t.Fatalf("expected RebuildIndex to publish a new snapshot instance")
	}
	if current.seq != seq {
		t.Fatalf("expected same-seq republish on RebuildIndex: old=%d new=%d", seq, current.seq)
	}

	db.unpinSnapshotRef(seq, ref)

	matPredAlive := old.matPredCache != nil
	rangeAlive := old.numericRangeBucketCache != nil
	if matPredAlive || rangeAlive {
		t.Fatalf(
			"old same-seq snapshot runtime caches survived after last unpin: matPredAlive=%v rangeAlive=%v oldSeq=%d currentSeq=%d",
			matPredAlive,
			rangeAlive,
			old.seq,
			current.seq,
		)
	}
}

func TestSnapshotExtra_StringKeyPinnedSnapshotChainSurvivesTruncateRotationAndPrunesInUnpinOrder(t *testing.T) {
	db, _ := snapshotExtraOpenTempDBString(t, snapshotExtraOptions())

	if err := db.BatchSet(
		[]string{"k1", "k2"},
		[]*snapshotExtraRec{
			{Name: "one", Age: 10},
			{Name: "two", Age: 20},
		},
	); err != nil {
		t.Fatalf("BatchSet(seed): %v", err)
	}

	snapA := db.getSnapshot()
	pinnedA, refA, ok := db.pinSnapshotRefBySeq(snapA.seq)
	if !ok || pinnedA != snapA {
		t.Fatalf("expected snapshot A to be pinnable")
	}

	if err := db.Set("k3", &snapshotExtraRec{Name: "three", Age: 30}); err != nil {
		t.Fatalf("Set(k3): %v", err)
	}

	snapB := db.getSnapshot()
	pinnedB, refB, ok := db.pinSnapshotRefBySeq(snapB.seq)
	if !ok || pinnedB != snapB {
		t.Fatalf("expected snapshot B to be pinnable")
	}

	if err := db.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if err := db.Set("k4", &snapshotExtraRec{Name: "four", Age: 40}); err != nil {
		t.Fatalf("Set(k4): %v", err)
	}

	current := db.getSnapshot()
	if _, ok := current.strmap.getIdxNoLock("k1"); ok {
		t.Fatalf("current snapshot retained pre-truncate key k1")
	}
	if _, ok := current.strmap.getIdxNoLock("k2"); ok {
		t.Fatalf("current snapshot retained pre-truncate key k2")
	}
	if _, ok := current.strmap.getIdxNoLock("k3"); ok {
		t.Fatalf("current snapshot retained pre-truncate key k3")
	}
	if idx, ok := current.strmap.getIdxNoLock("k4"); !ok || idx != 1 {
		t.Fatalf("current snapshot failed to rebuild post-truncate strmap: idx=%d ok=%v", idx, ok)
	}

	checkScan := func(label string, snap *indexSnapshot, want []string) {
		t.Helper()
		got := snapshotExtraScanSnapshotStringKeys(t, db, snap, "")
		if !slices.Equal(got, want) {
			t.Fatalf("%s scan mismatch: got=%v want=%v", label, got, want)
		}
	}

	checkScan("A", pinnedA, []string{"k1", "k2"})
	checkScan("B", pinnedB, []string{"k1", "k2", "k3"})

	currentKeys, err := db.QueryKeys(qx.Query().By("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(current): %v", err)
	}
	if !slices.Equal(currentKeys, []string{"k4"}) {
		t.Fatalf("current key set mismatch after truncate+set: got=%v want=[k4]", currentKeys)
	}

	db.snapshot.mu.RLock()
	registrySize := len(db.snapshot.bySeq)
	heldA := db.snapshot.bySeq[snapA.seq]
	heldB := db.snapshot.bySeq[snapB.seq]
	heldCurrent := db.snapshot.bySeq[current.seq]
	db.snapshot.mu.RUnlock()
	if registrySize != 3 {
		t.Fatalf("expected 3 snapshot refs after truncate rotation, got=%d", registrySize)
	}
	if heldA != refA || heldA == nil || heldA.snap != nil {
		t.Fatalf("expected snapshot A to stay retired but pinned")
	}
	if heldB != refB || heldB == nil || heldB.snap != nil {
		t.Fatalf("expected snapshot B to stay retired but pinned")
	}
	if heldCurrent == nil || heldCurrent.snap != current {
		t.Fatalf("expected current snapshot ref to remain published")
	}

	db.unpinSnapshotRef(snapB.seq, refB)
	checkScan("A after unpin B", pinnedA, []string{"k1", "k2"})

	db.snapshot.mu.RLock()
	_, hasAAfterB := db.snapshot.bySeq[snapA.seq]
	_, hasBAfterB := db.snapshot.bySeq[snapB.seq]
	registryAfterB := len(db.snapshot.bySeq)
	db.snapshot.mu.RUnlock()
	if !hasAAfterB || hasBAfterB || registryAfterB != 2 {
		t.Fatalf("unexpected registry after unpin B: hasA=%v hasB=%v size=%d", hasAAfterB, hasBAfterB, registryAfterB)
	}

	db.unpinSnapshotRef(snapA.seq, refA)
	db.snapshot.mu.RLock()
	_, hasAAfterA := db.snapshot.bySeq[snapA.seq]
	_, hasCurrentAfterA := db.snapshot.bySeq[current.seq]
	registryAfterA := len(db.snapshot.bySeq)
	db.snapshot.mu.RUnlock()
	if hasAAfterA || !hasCurrentAfterA || registryAfterA != 1 {
		t.Fatalf("unexpected registry after unpin A: hasA=%v hasCurrent=%v size=%d", hasAAfterA, hasCurrentAfterA, registryAfterA)
	}

	snapshotExtraAssertNoFutureSnapshotRefs(t, db)
}
