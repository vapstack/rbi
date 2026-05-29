package rbi

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"go.etcd.io/bbolt"
)

func mustCurrentBucketSequence(t *testing.T, db *DB[uint64, Rec]) uint64 {
	t.Helper()

	seq, err := currentBucketSequence(db.bolt, db.bucket)
	if err != nil {
		t.Fatalf("currentBucketSequence: %v", err)
	}
	return seq
}

func TestSnapshotSequence_PublishedOnWrite(t *testing.T) {
	db, _ := openTempDBUint64(t)

	before := db.SnapshotStats().Sequence

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	after := db.SnapshotStats().Sequence
	if after <= before {
		t.Fatalf("snapshot sequence did not advance: before=%d after=%d", before, after)
	}

	cur := mustCurrentBucketSequence(t, db)
	if after != cur {
		t.Fatalf("snapshot sequence mismatch with bolt Seq: snapshot=%d bolt=%d", after, cur)
	}
}

func TestSnapshotSequence_PreviousSnapshotIsRetiredAfterPublishWithoutPins(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "seed", Age: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	oldSequence := db.SnapshotStats().Sequence

	if err := db.Set(2, &Rec{Name: "new", Age: 20}); err != nil {
		t.Fatalf("concurrent Set: %v", err)
	}

	latest := db.SnapshotStats()
	if latest.Sequence <= oldSequence {
		t.Fatalf("latest sequence did not advance: old=%d latest=%d", oldSequence, latest.Sequence)
	}
	if latest.RegistrySize != 1 {
		t.Fatalf("previous snapshot unexpectedly remained tracked: stats=%+v", latest)
	}
}

func TestSnapshotSequence_AdvancesOnWrite(t *testing.T) {
	db, _ := openTempDBUint64(t)

	before := db.SnapshotStats().Sequence
	if err := db.Set(1, &Rec{Name: "x", Age: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	after := db.SnapshotStats().Sequence
	if after <= before {
		t.Fatalf("snapshot sequence did not advance on write: before=%d after=%d", before, after)
	}
}

func TestSnapshot_PublishedAsFullState(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Tags: []string{"go", "db"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err := db.QueryKeys(qx.Query(qx.EQ("name", "alice")))
	if err != nil {
		t.Fatalf("QueryKeys(name): %v", err)
	}
	if !slices.Equal(got, []uint64{1}) {
		t.Fatalf("name query mismatch: got=%v want=[1]", got)
	}

	got, err = db.QueryKeys(qx.Query(qx.HASANY("tags", []string{"go"})))
	if err != nil {
		t.Fatalf("QueryKeys(tags): %v", err)
	}
	if !slices.Equal(got, []uint64{1}) {
		t.Fatalf("tags query mismatch: got=%v want=[1]", got)
	}

	orderQ := queryOrderSortByArrayCount(qx.Query(), "tags", qx.ASC)
	got, err = db.QueryKeys(orderQ)
	if err != nil {
		t.Fatalf("QueryKeys(array count): %v", err)
	}
	if !slices.Equal(got, []uint64{1}) {
		t.Fatalf("array-count order mismatch: got=%v want=[1]", got)
	}
}

func TestSnapshotStrMap_LatestSnapshotRetainsOldMappingsAcrossChain(t *testing.T) {
	db, _ := openTempDBString(t)

	want := []string{"k1"}
	if err := db.Set("k1", &Rec{Name: "one"}); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}

	for i := 2; i <= 12; i++ {
		key := fmt.Sprintf("k%d", i)
		want = append(want, key)
		if err := db.Set(key, &Rec{Name: key}); err != nil {
			t.Fatalf("Set(%s): %v", key, err)
		}
	}

	got, err := db.QueryKeys(qx.Query())
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("latest query lost old mappings: got=%v want=%v", got, want)
	}
}

func TestSnapshot_EmptyBaseBatchSetBuildsDistinctLenIndex(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	ids := []uint64{1, 2, 3, 4}
	vals := []*Rec{
		{Name: "u1", Tags: nil},
		{Name: "u2", Tags: []string{}},
		{Name: "u3", Tags: []string{"go", "go"}},
		{Name: "u4", Tags: []string{"go", "db", "db"}},
	}
	if err := db.BatchSet(ids, vals); err != nil {
		t.Fatalf("BatchSet: %v", err)
	}

	emptyQ := qx.Query(qx.EQ("tags", []string{}))
	gotEmpty, err := db.QueryKeys(emptyQ)
	if err != nil {
		t.Fatalf("QueryKeys(empty tags): %v", err)
	}
	if want := []uint64{1, 2}; !slices.Equal(gotEmpty, want) {
		t.Fatalf("unexpected empty-tags result: got=%v want=%v", gotEmpty, want)
	}

	orderQ := queryOrderSortByArrayCount(qx.Query(), "tags", qx.ASC)
	gotOrder, err := db.QueryKeys(orderQ)
	if err != nil {
		t.Fatalf("QueryKeys(array count): %v", err)
	}
	wantOrder, err := expectedKeysUint64(t, db, orderQ)
	if err != nil {
		t.Fatalf("expectedKeysUint64(array count): %v", err)
	}
	if !slices.Equal(gotOrder, wantOrder) {
		t.Fatalf("unexpected array-count order: got=%v want=%v", gotOrder, wantOrder)
	}
}

func TestSnapshotFromEmptyBase_ScanKeysStringStaysStableAcrossPatches(t *testing.T) {
	db, _ := openTempDBString(t, Options{
		AnalyzeInterval: -1,
		AutoBatchMax:    1,
	})

	const seedN = 256
	keys := make([]string, 0, seedN)
	vals := make([]*Rec, 0, seedN)
	for i := 1; i <= seedN; i++ {
		key := fmt.Sprintf("k%04d", i)
		keys = append(keys, key)
		vals = append(vals, &Rec{
			Meta:     Meta{Country: "NL"},
			Name:     key,
			Email:    fmt.Sprintf("%s@example.test", key),
			Age:      i,
			Score:    float64(i),
			Active:   i%2 == 0,
			Tags:     []string{"go", "db"},
			FullName: key,
		})
	}

	if err := db.BatchSet(keys, vals); err != nil {
		t.Fatalf("BatchSet(seed): %v", err)
	}

	expect := slices.Clone(keys)
	got := make([]string, 0, len(expect))
	patched := false
	if err := db.ScanKeys("", func(id string) (bool, error) {
		got = append(got, id)
		if patched {
			return true, nil
		}
		patched = true
		for i := 0; i < 32; i++ {
			key := keys[i%len(keys)]
			if err := db.Patch(key, []Field{
				{Name: "active", Value: i%2 == 0},
				{Name: "country", Value: "DE"},
				{Name: "name", Value: fmt.Sprintf("patched-%03d", i)},
			}); err != nil {
				return false, err
			}
		}
		return true, nil
	}); err != nil {
		t.Fatalf("ScanKeys: %v", err)
	}
	if !patched {
		t.Fatalf("ScanKeys callback was not called")
	}
	if !slices.Equal(got, expect) {
		t.Fatalf("scan changed while newer snapshots were published: got=%v want=%v", got, expect)
	}
}

/**/

func snapshotExtOptions() Options {
	return Options{
		AutoBatchMax:    1,
		AnalyzeInterval: -1,
	}
}

func TestSnapshotExt_SnapshotStatsTracksPinnedRefAcrossPublish(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	var oldSeq uint64
	if err := db.ScanKeys(0, func(uint64) (bool, error) {
		before := db.SnapshotStats()
		oldSeq = before.Sequence
		if oldSeq == 0 || before.RegistrySize != 1 || before.PinnedRefs != 1 {
			return false, fmt.Errorf("unexpected stats before publish: %+v", before)
		}

		if err := db.Set(2, &Rec{Name: "bob", Age: 20}); err != nil {
			return false, fmt.Errorf("Set(2): %w", err)
		}

		mid := db.SnapshotStats()
		if mid.Sequence <= oldSeq {
			return false, fmt.Errorf("expected stats sequence to advance after publish: %+v", mid)
		}
		if mid.RegistrySize != 2 || mid.PinnedRefs != 1 {
			return false, fmt.Errorf("unexpected stats with pinned retired snapshot: %+v", mid)
		}

		return false, nil
	}); err != nil {
		t.Fatal(err)
	}

	if oldSeq == 0 {
		t.Fatalf("ScanKeys callback was not called")
	}

	after := db.SnapshotStats()
	if after.RegistrySize != 1 || after.PinnedRefs != 0 {
		t.Fatalf("unexpected stats after scan pin release: %+v", after)
	}
}

func TestSnapshotExt_QuerySeesPublishedStateWhileSetCommitBlocked(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	blockCommit := snapshotExtraBlockBeforeCommitUint64[Rec](entered, release, nil)

	done := make(chan error, 1)
	go func() {
		done <- db.Set(2, &Rec{Name: "bob", Age: 20}, blockCommit)
	}()

	snapshotExtraWait(t, entered, "staged set commit hook")

	oldItems, err := db.Query(qx.Query(qx.EQ("name", "bob")))
	if err != nil {
		t.Fatalf("Query(bob before publish): %v", err)
	}
	if len(oldItems) != 0 {
		t.Fatalf("public query unexpectedly sees future write: %#v", oldItems)
	}

	oldItems, err = db.Query(qx.Query(qx.EQ("name", "alice")))
	if err != nil {
		t.Fatalf("Query(alice before publish): %v", err)
	}
	if len(oldItems) != 1 || oldItems[0] == nil || oldItems[0].Name != "alice" {
		t.Fatalf("unexpected published query result: %#v", oldItems)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	currentItems, err := db.Query(qx.Query(qx.EQ("name", "bob")))
	if err != nil {
		t.Fatalf("Query(current snapshot): %v", err)
	}
	if len(currentItems) != 1 || currentItems[0] == nil || currentItems[0].Name != "bob" {
		t.Fatalf("unexpected current snapshot query result: %#v", currentItems)
	}
}

func TestSnapshotExt_PinnedSnapshotSurvivesTruncateAndPrunesAfterUnpin(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "bob", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	oldStats := db.SnapshotStats()
	var got []uint64
	truncated := false
	if err := db.ScanKeys(0, func(id uint64) (bool, error) {
		got = append(got, id)
		if truncated {
			return true, nil
		}
		truncated = true
		if err := db.Truncate(); err != nil {
			return false, err
		}
		mid := db.SnapshotStats()
		if mid.Sequence <= oldStats.Sequence {
			return false, fmt.Errorf("expected truncate to publish newer snapshot: old=%d new=%d", oldStats.Sequence, mid.Sequence)
		}
		if mid.RegistrySize != 2 || mid.PinnedRefs != 1 {
			return false, fmt.Errorf("unexpected stats while scan pins pre-truncate snapshot: %+v", mid)
		}
		return true, nil
	}); err != nil {
		t.Fatalf("ScanKeys: %v", err)
	}
	if !truncated {
		t.Fatalf("ScanKeys callback was not called")
	}
	if !slices.Equal(got, []uint64{1, 2}) {
		t.Fatalf("scan did not stay on pre-truncate snapshot: got=%v want=[1 2]", got)
	}

	keys, err := db.QueryKeys(qx.Query(qx.EQ("name", "alice")))
	if err != nil {
		t.Fatalf("QueryKeys(after truncate): %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("expected current snapshot to be empty after truncate, got=%v", keys)
	}

	registrySize := db.SnapshotStats().RegistrySize
	if registrySize != 1 {
		t.Fatalf("expected registry to retain only latest snapshot after truncate prune, got=%d", registrySize)
	}
}

func TestSnapshotExt_ScanKeysStringShouldStayOnSingleSnapshotAcrossTruncate(t *testing.T) {
	db, _ := openTempDBString(t, snapshotExtOptions())

	if err := db.Set("k1", &Rec{Name: "one"}); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	if err := db.Set("k2", &Rec{Name: "two"}); err != nil {
		t.Fatalf("Set(k2): %v", err)
	}

	want := []string{"k1", "k2"}
	var got []string
	truncated := false
	err := db.ScanKeys("", func(id string) (bool, error) {
		got = append(got, id)
		if !truncated {
			truncated = true
			if err := db.Truncate(); err != nil {
				return false, err
			}
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("string ScanKeys split snapshot across truncate: err=%v want=%v got=%v", err, want, got)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("string ScanKeys mixed snapshots: got=%v want=%v", got, want)
	}
}

func TestSnapshotExt_ScanKeysStringSplitSnapshotMustNotEmitFutureKeysAfterTruncateReuse(t *testing.T) {
	db, _ := openTempDBString(t, snapshotExtOptions())

	if err := db.Set("k1", &Rec{Name: "one"}); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	if err := db.Set("k2", &Rec{Name: "two"}); err != nil {
		t.Fatalf("Set(k2): %v", err)
	}

	var got []string
	reused := false
	err := db.ScanKeys("", func(id string) (bool, error) {
		got = append(got, id)
		if !reused {
			reused = true
			if err := db.Truncate(); err != nil {
				return false, err
			}
			if err := db.Set("future", &Rec{Name: "future"}); err != nil {
				return false, err
			}
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("split snapshot emitted inconsistent string scan state: err=%v got=%v", err, got)
	}
	if slices.Contains(got, "future") {
		t.Fatalf("string scan leaked future key from newer snapshot: got=%v", got)
	}
	if !slices.Equal(got, []string{"k1", "k2"}) {
		t.Fatalf("unexpected string scan result from old snapshot: got=%v", got)
	}
}

func TestSnapshotExt_QueryKeysReleasesCurrentSnapshotPinOnError(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	if _, err := db.QueryKeys(qx.Query(qx.EQ("does_not_exist", 1))); err == nil {
		t.Fatalf("expected QueryKeys to fail for unknown field")
	}
	stats := db.SnapshotStats()
	if stats.RegistrySize != 1 {
		t.Fatalf("expected QueryKeys snapshot ref to remain registered")
	}
	if stats.PinnedRefs != 0 {
		t.Fatalf("expected QueryKeys pin release on error, stats=%+v", stats)
	}
}

func TestSnapshotExt_CountReleasesCurrentSnapshotPinOnError(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	if _, err := db.Count(qx.EQ("does_not_exist", 1)); err == nil {
		t.Fatalf("expected Count to fail for unknown field")
	}
	stats := db.SnapshotStats()
	if stats.RegistrySize != 1 {
		t.Fatalf("expected Count snapshot ref to remain registered")
	}
	if stats.PinnedRefs != 0 {
		t.Fatalf("expected Count pin release on error, stats=%+v", stats)
	}
}

func TestSnapshotExt_ScanKeysReleasesCurrentSnapshotPinOnCallbackError(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	wantErr := errors.New("stop")
	err := db.ScanKeys(0, func(uint64) (bool, error) {
		return false, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("ScanKeys err=%v want %v", err, wantErr)
	}
	stats := db.SnapshotStats()
	if stats.RegistrySize != 1 {
		t.Fatalf("expected ScanKeys snapshot ref to remain registered")
	}
	if stats.PinnedRefs != 0 {
		t.Fatalf("expected ScanKeys pin release on callback error, stats=%+v", stats)
	}
}

/**/

type snapshotExtraRec struct {
	Name  string `db:"name" rbi:"index"`
	Email string `db:"email" rbi:"index"`
	Age   int    `db:"age" rbi:"index"`
}

func snapshotExtraOptions() Options {
	return testOptions(Options{
		AutoBatchMax:    1,
		AnalyzeInterval: -1,
	})
}

func snapshotExtraOpenTempDBUint64(t *testing.T, opts Options) (*DB[uint64, snapshotExtraRec], string) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot_extra_uint64.db")
	raw, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}

	opts = testOptions(opts)
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

	opts = testOptions(opts)
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

func snapshotExtraBlockBeforeCommitUint64[V any](entered, release chan struct{}, err error) ExecOption[uint64, V] {
	var once sync.Once
	return BeforeCommit(func(*bbolt.Tx, uint64, *V, *V) error {
		once.Do(func() {
			close(entered)
			<-release
		})
		return err
	})
}

func snapshotExtraBlockBeforeCommitString[V any](entered, release chan struct{}, err error) ExecOption[string, V] {
	var once sync.Once
	return BeforeCommit(func(*bbolt.Tx, string, *V, *V) error {
		once.Do(func() {
			close(entered)
			<-release
		})
		return err
	})
}

func snapshotExtraAssertNoFutureSnapshotRefs[K ~uint64 | ~string, V any](tb testing.TB, db *DB[K, V]) {
	tb.Helper()

	stats := db.SnapshotStats()
	if stats.RegistrySize != 1 {
		tb.Fatalf("snapshot registry contains staged or retired refs: stats=%+v", stats)
	}
}

func TestSnapshotExtra_PublicQueriesIgnoreInFlightSetBeforeCommit(t *testing.T) {
	db, _ := snapshotExtraOpenTempDBUint64(t, snapshotExtraOptions())

	if err := db.Set(1, &snapshotExtraRec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &snapshotExtraRec{Name: "bob", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	oldSeq := db.SnapshotStats().Sequence

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	blockCommit := snapshotExtraBlockBeforeCommitUint64[snapshotExtraRec](entered, release, nil)

	done := make(chan error, 1)
	go func() {
		done <- db.Set(2, &snapshotExtraRec{Name: "charlie", Age: 20}, blockCommit)
	}()

	snapshotExtraWait(t, entered, "set before commit hook")
	if got := db.SnapshotStats().Sequence; got != oldSeq {
		t.Fatalf("current snapshot changed before commit publish: got=%d want=%d", got, oldSeq)
	}

	beforeBob, err := db.QueryKeys(qx.Query(qx.EQ("name", "bob")))
	if err != nil {
		t.Fatalf("QueryKeys(bob before publish): %v", err)
	}
	if !slices.Equal(beforeBob, []uint64{2}) {
		t.Fatalf("public query lost committed row before publish: got=%v want=[2]", beforeBob)
	}
	beforeCharlie, err := db.QueryKeys(qx.Query(qx.EQ("name", "charlie")))
	if err != nil {
		t.Fatalf("QueryKeys(charlie before publish): %v", err)
	}
	if len(beforeCharlie) != 0 {
		t.Fatalf("public query observed staged future row: %v", beforeCharlie)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Set(update): %v", err)
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

	snapshotExtraAssertNoFutureSnapshotRefs(t, db)
}

func TestSnapshotExtra_PublicQueriesIgnoreInFlightSetRollback(t *testing.T) {
	db, _ := snapshotExtraOpenTempDBUint64(t, snapshotExtraOptions())

	if err := db.Set(1, &snapshotExtraRec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &snapshotExtraRec{Name: "bob", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	oldSeq := db.SnapshotStats().Sequence

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	blockCommit := snapshotExtraBlockBeforeCommitUint64[snapshotExtraRec](entered, release, fmt.Errorf("failpoint: rollback set"))

	done := make(chan error, 1)
	go func() {
		done <- db.Set(2, &snapshotExtraRec{Name: "charlie", Age: 20}, blockCommit)
	}()

	snapshotExtraWait(t, entered, "rollback set before commit hook")

	beforeBob, err := db.QueryKeys(qx.Query(qx.EQ("name", "bob")))
	if err != nil {
		t.Fatalf("QueryKeys(bob before rollback): %v", err)
	}
	if !slices.Equal(beforeBob, []uint64{2}) {
		t.Fatalf("public query lost committed row before rollback: got=%v want=[2]", beforeBob)
	}
	beforeCharlie, err := db.QueryKeys(qx.Query(qx.EQ("name", "charlie")))
	if err != nil {
		t.Fatalf("QueryKeys(charlie before rollback): %v", err)
	}
	if len(beforeCharlie) != 0 {
		t.Fatalf("public query observed staged future row before rollback: %v", beforeCharlie)
	}

	close(release)
	err = <-done
	if err == nil || !strings.Contains(err.Error(), "failpoint: rollback set") {
		t.Fatalf("expected rollback failpoint error, got: %v", err)
	}

	if got := db.SnapshotStats().Sequence; got != oldSeq {
		t.Fatalf("rollback replaced published snapshot: got=%d want=%d", got, oldSeq)
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

	snapshotExtraAssertNoFutureSnapshotRefs(t, db)
}

func TestSnapshotExtra_PublicQueriesIgnoreInFlightBatchSetBeforeCommit(t *testing.T) {
	db, _ := snapshotExtraOpenTempDBUint64(t, snapshotExtraOptions())

	if err := db.Set(1, &snapshotExtraRec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &snapshotExtraRec{Name: "bob", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	oldSeq := db.SnapshotStats().Sequence

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	blockCommit := snapshotExtraBlockBeforeCommitUint64[snapshotExtraRec](entered, release, nil)

	done := make(chan error, 1)
	go func() {
		done <- db.BatchSet(
			[]uint64{2, 3},
			[]*snapshotExtraRec{
				{Name: "charlie", Age: 20},
				{Name: "dave", Age: 40},
			},
			blockCommit,
		)
	}()

	snapshotExtraWait(t, entered, "batch_set before commit hook")
	if got := db.SnapshotStats().Sequence; got != oldSeq {
		t.Fatalf("current snapshot changed before batch_set publish: got=%d want=%d", got, oldSeq)
	}

	beforeBob, err := db.QueryKeys(qx.Query(qx.EQ("name", "bob")))
	if err != nil {
		t.Fatalf("QueryKeys(bob before batch publish): %v", err)
	}
	if !slices.Equal(beforeBob, []uint64{2}) {
		t.Fatalf("public query lost committed batch row before publish: got=%v want=[2]", beforeBob)
	}
	beforeCharlie, err := db.QueryKeys(qx.Query(qx.EQ("name", "charlie")))
	if err != nil {
		t.Fatalf("QueryKeys(charlie before batch publish): %v", err)
	}
	if len(beforeCharlie) != 0 {
		t.Fatalf("public query observed staged future batch row: %v", beforeCharlie)
	}
	beforeDave, err := db.QueryKeys(qx.Query(qx.EQ("name", "dave")))
	if err != nil {
		t.Fatalf("QueryKeys(dave before batch publish): %v", err)
	}
	if len(beforeDave) != 0 {
		t.Fatalf("public query observed staged inserted batch row: %v", beforeDave)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("BatchSet(update+insert): %v", err)
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

	snapshotExtraAssertNoFutureSnapshotRefs(t, db)
}

func TestSnapshotExtra_PublicQueriesIgnoreInFlightStringSetBeforeCommit(t *testing.T) {
	db, _ := snapshotExtraOpenTempDBString(t, snapshotExtraOptions())

	if err := db.Set("k1", &snapshotExtraRec{Name: "one", Age: 10}); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	if err := db.Set("k2", &snapshotExtraRec{Name: "two", Age: 20}); err != nil {
		t.Fatalf("Set(k2): %v", err)
	}

	oldSeq := db.SnapshotStats().Sequence

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	blockCommit := snapshotExtraBlockBeforeCommitString[snapshotExtraRec](entered, release, nil)

	done := make(chan error, 1)
	go func() {
		done <- db.Set("future", &snapshotExtraRec{Name: "future", Age: 99}, blockCommit)
	}()

	snapshotExtraWait(t, entered, "string set before commit hook")
	if got := db.SnapshotStats().Sequence; got != oldSeq {
		t.Fatalf("current string snapshot changed before publish: got=%d want=%d", got, oldSeq)
	}

	beforeFuture, err := db.QueryKeys(qx.Query(qx.EQ("name", "future")))
	if err != nil {
		t.Fatalf("QueryKeys(future before publish): %v", err)
	}
	if len(beforeFuture) != 0 {
		t.Fatalf("public query observed staged future string row: %v", beforeFuture)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Set(future): %v", err)
	}

	currentFuture, err := db.QueryKeys(qx.Query(qx.EQ("name", "future")))
	if err != nil {
		t.Fatalf("QueryKeys(future): %v", err)
	}
	if !slices.Equal(currentFuture, []string{"future"}) {
		t.Fatalf("current string snapshot missed committed future key: got=%v want=[future]", currentFuture)
	}

	snapshotExtraAssertNoFutureSnapshotRefs(t, db)
}

func TestSnapshotExtra_StringKeyRollbackLeavesPublicStateCleanAndRecovers(t *testing.T) {
	db, _ := snapshotExtraOpenTempDBString(t, snapshotExtraOptions())

	if err := db.Set("k1", &snapshotExtraRec{Name: "one", Age: 10}); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	if err := db.Set("k2", &snapshotExtraRec{Name: "two", Age: 20}); err != nil {
		t.Fatalf("Set(k2): %v", err)
	}

	oldSeq := db.SnapshotStats().Sequence

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	blockCommit := snapshotExtraBlockBeforeCommitString[snapshotExtraRec](entered, release, fmt.Errorf("failpoint: rollback string set"))

	done := make(chan error, 1)
	go func() {
		done <- db.Set("ghost", &snapshotExtraRec{Name: "ghost", Age: 99}, blockCommit)
	}()

	snapshotExtraWait(t, entered, "string rollback set before commit hook")

	beforeGhost, err := db.QueryKeys(qx.Query(qx.EQ("name", "ghost")))
	if err != nil {
		t.Fatalf("QueryKeys(ghost before rollback): %v", err)
	}
	if len(beforeGhost) != 0 {
		t.Fatalf("public query observed ghost row before rollback: %v", beforeGhost)
	}

	close(release)
	err = <-done
	if err == nil || !strings.Contains(err.Error(), "failpoint: rollback string set") {
		t.Fatalf("expected rollback failpoint error, got: %v", err)
	}

	if got := db.SnapshotStats().Sequence; got != oldSeq {
		t.Fatalf("rollback replaced published string snapshot: got=%d want=%d", got, oldSeq)
	}
	if got, err := db.Get("ghost"); err != nil {
		t.Fatalf("Get(ghost): %v", err)
	} else if got != nil {
		t.Fatalf("rolled-back ghost key became readable: %#v", got)
	}
	ghostKeys, err := db.QueryKeys(qx.Query(qx.EQ("name", "ghost")))
	if err != nil {
		t.Fatalf("QueryKeys(name=ghost): %v", err)
	}
	if len(ghostKeys) != 0 {
		t.Fatalf("rolled-back ghost key remained queryable: %v", ghostKeys)
	}

	if err := db.Set("later", &snapshotExtraRec{Name: "later", Age: 50}); err != nil {
		t.Fatalf("Set(later): %v", err)
	}
	queryLater, err := db.QueryKeys(qx.Query(qx.EQ("name", "later")))
	if err != nil {
		t.Fatalf("QueryKeys(name=later): %v", err)
	}
	if !slices.Equal(queryLater, []string{"later"}) {
		t.Fatalf("query path missed later key after rollback recovery: got=%v want=[later]", queryLater)
	}
	ghostKeys, err = db.QueryKeys(qx.Query(qx.EQ("name", "ghost")))
	if err != nil {
		t.Fatalf("QueryKeys(name=ghost after recovery): %v", err)
	}
	if len(ghostKeys) != 0 {
		t.Fatalf("rolled-back ghost key became queryable after recovery: %v", ghostKeys)
	}

	snapshotExtraAssertNoFutureSnapshotRefs(t, db)
}

func TestSnapshotExtra_StringKeyBatchRollbackLeavesPublicStateCleanAndRecovers(t *testing.T) {
	db, _ := snapshotExtraOpenTempDBString(t, snapshotExtraOptions())

	if err := db.Set("k1", &snapshotExtraRec{Name: "one", Age: 10}); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}

	oldSeq := db.SnapshotStats().Sequence

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	blockCommit := snapshotExtraBlockBeforeCommitString[snapshotExtraRec](entered, release, fmt.Errorf("failpoint: rollback string batch_set"))

	done := make(chan error, 1)
	go func() {
		done <- db.BatchSet(
			[]string{"ghostA", "ghostB"},
			[]*snapshotExtraRec{
				{Name: "ghostA", Age: 11},
				{Name: "ghostB", Age: 12},
			},
			blockCommit,
		)
	}()

	snapshotExtraWait(t, entered, "string rollback batch_set before commit hook")

	beforeGhostA, err := db.QueryKeys(qx.Query(qx.EQ("name", "ghostA")))
	if err != nil {
		t.Fatalf("QueryKeys(ghostA before rollback): %v", err)
	}
	if len(beforeGhostA) != 0 {
		t.Fatalf("public query observed ghostA row before rollback: %v", beforeGhostA)
	}
	beforeGhostB, err := db.QueryKeys(qx.Query(qx.EQ("name", "ghostB")))
	if err != nil {
		t.Fatalf("QueryKeys(ghostB before rollback): %v", err)
	}
	if len(beforeGhostB) != 0 {
		t.Fatalf("public query observed ghostB row before rollback: %v", beforeGhostB)
	}

	close(release)
	err = <-done
	if err == nil || !strings.Contains(err.Error(), "failpoint: rollback string batch_set") {
		t.Fatalf("expected rollback failpoint error, got: %v", err)
	}

	if got := db.SnapshotStats().Sequence; got != oldSeq {
		t.Fatalf("rollback replaced published string snapshot: got=%d want=%d", got, oldSeq)
	}
	if got, err := db.Get("ghostA"); err != nil {
		t.Fatalf("Get(ghostA): %v", err)
	} else if got != nil {
		t.Fatalf("rolled-back ghostA key became readable: %#v", got)
	}
	if got, err := db.Get("ghostB"); err != nil {
		t.Fatalf("Get(ghostB): %v", err)
	} else if got != nil {
		t.Fatalf("rolled-back ghostB key became readable: %#v", got)
	}
	ghostAKeys, err := db.QueryKeys(qx.Query(qx.EQ("name", "ghostA")))
	if err != nil {
		t.Fatalf("QueryKeys(name=ghostA): %v", err)
	}
	if len(ghostAKeys) != 0 {
		t.Fatalf("rolled-back ghostA key remained queryable: %v", ghostAKeys)
	}
	ghostBKeys, err := db.QueryKeys(qx.Query(qx.EQ("name", "ghostB")))
	if err != nil {
		t.Fatalf("QueryKeys(name=ghostB): %v", err)
	}
	if len(ghostBKeys) != 0 {
		t.Fatalf("rolled-back ghostB key remained queryable: %v", ghostBKeys)
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
	queryReal, err := db.QueryKeys(qx.Query().Sort("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(real recovery): %v", err)
	}
	if !slices.Equal(queryReal, []string{"k1", "realA", "realB"}) {
		t.Fatalf("query path drift after batch rollback recovery: got=%v want=[k1 realA realB]", queryReal)
	}
	ghostAKeys, err = db.QueryKeys(qx.Query(qx.EQ("name", "ghostA")))
	if err != nil {
		t.Fatalf("QueryKeys(name=ghostA after recovery): %v", err)
	}
	if len(ghostAKeys) != 0 {
		t.Fatalf("rolled-back ghostA key became queryable after recovery: %v", ghostAKeys)
	}
	ghostBKeys, err = db.QueryKeys(qx.Query(qx.EQ("name", "ghostB")))
	if err != nil {
		t.Fatalf("QueryKeys(name=ghostB after recovery): %v", err)
	}
	if len(ghostBKeys) != 0 {
		t.Fatalf("rolled-back ghostB key became queryable after recovery: %v", ghostBKeys)
	}

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

	alphaQ := qx.Query(qx.PREFIX("email", "alpha"))
	futureQ := qx.Query(qx.PREFIX("email", "future"))
	oldAlpha, err := db.QueryKeys(alphaQ)
	if err != nil {
		t.Fatalf("QueryKeys(alpha warm): %v", err)
	}
	if !slices.Equal(oldAlpha, []uint64{1}) {
		t.Fatalf("unexpected warm alpha result: got=%v want=[1]", oldAlpha)
	}
	oldFuture, err := db.QueryKeys(futureQ)
	if err != nil {
		t.Fatalf("QueryKeys(future warm): %v", err)
	}
	if len(oldFuture) != 0 {
		t.Fatalf("unexpected warm future result: %v", oldFuture)
	}
	if err = db.Patch(1, []Field{{Name: "email", Value: "gamma@example.com"}}); err != nil {
		t.Fatalf("Patch(email): %v", err)
	}
	if err := db.Set(3, &snapshotExtraRec{Name: "carol", Email: "future@example.com"}); err != nil {
		t.Fatalf("Set(3): %v", err)
	}

	currentAlpha, err := db.QueryKeys(alphaQ)
	if err != nil {
		t.Fatalf("QueryKeys(alpha): %v", err)
	}
	if len(currentAlpha) != 0 {
		t.Fatalf("current snapshot kept stale alpha match: %v", currentAlpha)
	}

	currentFuture, err := db.QueryKeys(futureQ)
	if err != nil {
		t.Fatalf("QueryKeys(future): %v", err)
	}
	if !slices.Equal(currentFuture, []uint64{3}) {
		t.Fatalf("current snapshot missed new future match: got=%v want=[3]", currentFuture)
	}
}

func TestSnapshotExtra_PublicQueriesUnaffectedByOtherBucketWrites(t *testing.T) {
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

	beforeSeq := dbA.SnapshotStats().Sequence

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
		items, err := dbA.QueryKeys(qx.Query().Sort("name", qx.ASC))
		if err != nil {
			t.Fatalf("dbA QueryKeys(iter=%d): %v", i, err)
		}
		if !slices.Equal(items, []uint64{1, 2}) {
			t.Fatalf("dbA query drift during other-bucket writes at iter=%d: %v", i, items)
		}
	}

	if err := <-writerDone; err != nil {
		t.Fatalf("dbB writer: %v", err)
	}

	afterSeq := dbA.SnapshotStats().Sequence
	if afterSeq != beforeSeq {
		t.Fatalf("dbA snapshot sequence changed due to other bucket writes: before=%d after=%d", beforeSeq, afterSeq)
	}

	keys, err := dbA.QueryKeys(qx.Query().Sort("name", qx.ASC))
	if err != nil {
		t.Fatalf("dbA QueryKeys(final): %v", err)
	}
	if !slices.Equal(keys, []uint64{1, 2}) {
		t.Fatalf("dbA final keys drifted after other bucket writes: %v", keys)
	}
}

func TestSnapshotExtra_PublicQueriesIgnoreInFlightDeleteBeforeCommit(t *testing.T) {
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

	oldSeq := db.SnapshotStats().Sequence

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	blockCommit := snapshotExtraBlockBeforeCommitUint64[snapshotExtraRec](entered, release, nil)

	done := make(chan error, 1)
	go func() {
		done <- db.Delete(2, blockCommit)
	}()

	snapshotExtraWait(t, entered, "delete before commit hook")
	if got := db.SnapshotStats().Sequence; got != oldSeq {
		t.Fatalf("current snapshot changed before delete publish: got=%d want=%d", got, oldSeq)
	}

	beforeAll, err := db.QueryKeys(qx.Query().Sort("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(all before delete publish): %v", err)
	}
	if !slices.Equal(beforeAll, []uint64{1, 2, 3}) {
		t.Fatalf("public query drifted before delete publish: got=%v want=[1 2 3]", beforeAll)
	}
	beforeBob, err := db.QueryKeys(qx.Query(qx.EQ("name", "bob")))
	if err != nil {
		t.Fatalf("QueryKeys(bob before delete publish): %v", err)
	}
	if !slices.Equal(beforeBob, []uint64{2}) {
		t.Fatalf("public query lost soon-to-be-deleted row before publish: got=%v want=[2]", beforeBob)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Delete(2): %v", err)
	}

	currentKeys, err := db.QueryKeys(qx.Query().Sort("name", qx.ASC))
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

	snapshotExtraAssertNoFutureSnapshotRefs(t, db)
}

func TestSnapshotExtra_PublicQueriesIgnoreInFlightDeleteRollback(t *testing.T) {
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

	oldSeq := db.SnapshotStats().Sequence

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	blockCommit := snapshotExtraBlockBeforeCommitUint64[snapshotExtraRec](entered, release, fmt.Errorf("failpoint: rollback delete"))

	done := make(chan error, 1)
	go func() {
		done <- db.Delete(2, blockCommit)
	}()

	snapshotExtraWait(t, entered, "rollback delete before commit hook")

	beforeAll, err := db.QueryKeys(qx.Query().Sort("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(all before delete rollback): %v", err)
	}
	if !slices.Equal(beforeAll, []uint64{1, 2, 3}) {
		t.Fatalf("public query drifted before delete rollback: got=%v want=[1 2 3]", beforeAll)
	}

	close(release)
	err = <-done
	if err == nil || !strings.Contains(err.Error(), "failpoint: rollback delete") {
		t.Fatalf("expected rollback failpoint error, got: %v", err)
	}

	if got := db.SnapshotStats().Sequence; got != oldSeq {
		t.Fatalf("rollback replaced published snapshot: got=%d want=%d", got, oldSeq)
	}

	currentKeys, err := db.QueryKeys(qx.Query().Sort("name", qx.ASC))
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

	snapshotExtraAssertNoFutureSnapshotRefs(t, db)
}

func TestSnapshotExtra_PublicQueriesIgnoreInFlightBatchDeleteBeforeCommit(t *testing.T) {
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

	oldSeq := db.SnapshotStats().Sequence

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	blockCommit := snapshotExtraBlockBeforeCommitUint64[snapshotExtraRec](entered, release, nil)

	done := make(chan error, 1)
	go func() {
		done <- db.BatchDelete([]uint64{2, 2, 3, 3}, blockCommit)
	}()

	snapshotExtraWait(t, entered, "batch_delete before commit hook")
	if got := db.SnapshotStats().Sequence; got != oldSeq {
		t.Fatalf("current snapshot changed before batch_delete publish: got=%d want=%d", got, oldSeq)
	}

	beforeAll, err := db.QueryKeys(qx.Query().Sort("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(all before batch_delete publish): %v", err)
	}
	if !slices.Equal(beforeAll, []uint64{1, 2, 3, 4}) {
		t.Fatalf("public query drifted before batch_delete publish: got=%v want=[1 2 3 4]", beforeAll)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("BatchDelete(2,2,3,3): %v", err)
	}

	currentKeys, err := db.QueryKeys(qx.Query().Sort("name", qx.ASC))
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

	snapshotExtraAssertNoFutureSnapshotRefs(t, db)
}

func TestSnapshotExtra_PublicQueriesIgnoreInFlightBatchDeleteRollback(t *testing.T) {
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

	oldSeq := db.SnapshotStats().Sequence

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	blockCommit := snapshotExtraBlockBeforeCommitUint64[snapshotExtraRec](entered, release, fmt.Errorf("failpoint: rollback batch_delete"))

	done := make(chan error, 1)
	go func() {
		done <- db.BatchDelete([]uint64{2, 2, 3, 3}, blockCommit)
	}()

	snapshotExtraWait(t, entered, "rollback batch_delete before commit hook")

	beforeAll, err := db.QueryKeys(qx.Query().Sort("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(all before batch_delete rollback): %v", err)
	}
	if !slices.Equal(beforeAll, []uint64{1, 2, 3, 4}) {
		t.Fatalf("public query drifted before batch_delete rollback: got=%v want=[1 2 3 4]", beforeAll)
	}

	close(release)
	err = <-done
	if err == nil || !strings.Contains(err.Error(), "failpoint: rollback batch_delete") {
		t.Fatalf("expected rollback failpoint error, got: %v", err)
	}

	if got := db.SnapshotStats().Sequence; got != oldSeq {
		t.Fatalf("rollback replaced published snapshot: got=%d want=%d", got, oldSeq)
	}

	currentKeys, err := db.QueryKeys(qx.Query().Sort("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(current after rollback): %v", err)
	}
	if !slices.Equal(currentKeys, []uint64{1, 2, 3, 4}) {
		t.Fatalf("current snapshot changed after rolled-back batch_delete: got=%v want=[1 2 3 4]", currentKeys)
	}

	snapshotExtraAssertNoFutureSnapshotRefs(t, db)
}

func TestSnapshotExtra_PrefixQueriesAfterTruncateRebuild(t *testing.T) {
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

	alphaQ := qx.Query(qx.PREFIX("email", "alpha"))
	futureQ := qx.Query(qx.PREFIX("email", "future"))

	oldAlpha, err := db.QueryKeys(alphaQ)
	if err != nil {
		t.Fatalf("QueryKeys(alpha warm): %v", err)
	}
	if !queryIDsEqual(alphaQ, oldAlpha, []uint64{1, 3}) {
		t.Fatalf("unexpected alpha warm result: got=%v want=[1 3]", oldAlpha)
	}
	oldFuture, err := db.QueryKeys(futureQ)
	if err != nil {
		t.Fatalf("QueryKeys(future warm): %v", err)
	}
	if len(oldFuture) != 0 {
		t.Fatalf("unexpected future warm result: %v", oldFuture)
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
}

func TestSnapshotExtra_NumericRangeQueriesAfterTruncateRebuild(t *testing.T) {
	opts := snapshotExtraOptions()
	opts.NumericRangeBucketSize = 64
	opts.NumericRangeBucketMinFieldKeys = 1
	opts.NumericRangeBucketMinSpanKeys = 1
	db, _ := snapshotExtraOpenTempDBUint64(t, opts)

	snapshotExtraSeedGeneratedUint64Data(t, db, 512, func(i int) *snapshotExtraRec {
		return &snapshotExtraRec{
			Name: fmt.Sprintf("user-%04d", i),
			Age:  i,
		}
	})

	oldHighQ := qx.Query(qx.GTE("age", 400))
	oldHigh, err := db.QueryKeys(oldHighQ)
	if err != nil {
		t.Fatalf("QueryKeys(old high warm): %v", err)
	}
	if len(oldHigh) != 113 || !slices.Contains(oldHigh, uint64(400)) || !slices.Contains(oldHigh, uint64(512)) {
		t.Fatalf("unexpected old high range result: len=%d ids=%v", len(oldHigh), oldHigh)
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
}

func TestSnapshotExtra_StringScanKeysStaySingleSnapshotAcrossConcurrentTruncateRebuilds(t *testing.T) {
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

	wantOld := slices.Clone(keys)
	scanKeys := func() ([]string, error) {
		var out []string
		if err := db.ScanKeys("", func(id string) (bool, error) {
			out = append(out, id)
			return true, nil
		}); err != nil {
			return nil, err
		}
		return out, nil
	}
	validScan := func(got []string) bool {
		if len(got) == 0 || slices.Equal(got, wantOld) {
			return true
		}
		if len(got) != 16 {
			return false
		}
		for round := 0; round < 12; round++ {
			ok := true
			for i := 0; i < 16; i++ {
				if got[i] != fmt.Sprintf("live-%02d-%02d", round, i) {
					ok = false
					break
				}
			}
			if ok {
				return true
			}
		}
		return false
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
			got, err := scanKeys()
			if err != nil {
				setFailed(fmt.Sprintf("ScanKeys failed: %v", err))
				return
			}
			if !validScan(got) {
				setFailed(fmt.Sprintf("ScanKeys returned hybrid snapshot: got=%v", got))
				return
			}
		}
	}()

	close(start)
	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}
}
