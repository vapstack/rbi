package rbi

import (
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/posting"
	"go.etcd.io/bbolt"
)

type snapshotExtraRec struct {
	Name  string `db:"name"`
	Email string `db:"email"`
	Age   int    `db:"age"`
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
	raw, ok := snap.numericRangeBucketCache.Load(field)
	if !ok {
		t.Fatalf("expected numeric range bucket cache entry for field %q", field)
	}
	e, ok := raw.(*numericRangeBucketCacheEntry)
	if !ok || e == nil || e.idx.bucketSize <= 0 {
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
