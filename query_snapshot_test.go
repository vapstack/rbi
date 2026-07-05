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

func mustCurrentBucketSequence(t *testing.T, c *Collection[uint64, Rec]) uint64 {
	t.Helper()

	seq, err := currentBucketSequence(c.root.bolt, c.dataBucket)
	if err != nil {
		t.Fatalf("currentBucketSequence: %v", err)
	}
	return seq
}

func TestSnapshotSequence_PublishedOnWrite(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	before := c.SnapshotStats().Sequence

	if err := writeSet(c, 1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	after := c.SnapshotStats().Sequence
	if after <= before {
		t.Fatalf("snapshot sequence did not advance: before=%d after=%d", before, after)
	}

	cur := mustCurrentBucketSequence(t, c)
	if after != cur {
		t.Fatalf("snapshot sequence mismatch with bolt Seq: snapshot=%d bolt=%d", after, cur)
	}
}

func TestSnapshotSequence_PreviousSnapshotIsRetiredAfterPublishWithoutPins(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	if err := writeSet(c, 1, &Rec{Name: "seed", Age: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	oldSequence := c.SnapshotStats().Sequence

	if err := writeSet(c, 2, &Rec{Name: "new", Age: 20}); err != nil {
		t.Fatalf("concurrent Set: %v", err)
	}

	latest := c.SnapshotStats()
	if latest.Sequence <= oldSequence {
		t.Fatalf("latest sequence did not advance: old=%d latest=%d", oldSequence, latest.Sequence)
	}
	assertNoFutureSnapshotRefs(t, c)
}

func TestSnapshotSequence_AdvancesOnWrite(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	before := c.SnapshotStats().Sequence
	if err := writeSet(c, 1, &Rec{Name: "x", Age: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	after := c.SnapshotStats().Sequence
	if after <= before {
		t.Fatalf("snapshot sequence did not advance on write: before=%d after=%d", before, after)
	}
}

func TestSnapshot_PublishedAsFullState(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	if err := writeSet(c, 1, &Rec{Name: "alice", Tags: []string{"go", "db"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err := readQueryKeys(c, qx.Query(qx.EQ("name", "alice")))
	if err != nil {
		t.Fatalf("QueryKeys(name): %v", err)
	}
	if !slices.Equal(got, []uint64{1}) {
		t.Fatalf("name query mismatch: got=%v want=[1]", got)
	}

	got, err = readQueryKeys(c, qx.Query(qx.HASANY("tags", []string{"go"})))
	if err != nil {
		t.Fatalf("QueryKeys(tags): %v", err)
	}
	if !slices.Equal(got, []uint64{1}) {
		t.Fatalf("tags query mismatch: got=%v want=[1]", got)
	}

	orderQ := queryOrderSortByArrayCount(qx.Query(), "tags", qx.ASC)
	got, err = readQueryKeys(c, orderQ)
	if err != nil {
		t.Fatalf("QueryKeys(array count): %v", err)
	}
	if !slices.Equal(got, []uint64{1}) {
		t.Fatalf("array-count order mismatch: got=%v want=[1]", got)
	}
}

func TestSnapshotStrMap_LatestSnapshotRetainsOldMappingsAcrossChain(t *testing.T) {
	c, _ := openTempStringCollection(t)

	want := []string{"k1"}
	if err := writeSet(c, "k1", &Rec{Name: "one"}); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}

	for i := 2; i <= 12; i++ {
		key := fmt.Sprintf("k%d", i)
		want = append(want, key)
		if err := writeSet(c, key, &Rec{Name: key}); err != nil {
			t.Fatalf("Set(%s): %v", key, err)
		}
	}

	got, err := readQueryKeys(c, qx.Query())
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("latest query lost old mappings: got=%v want=%v", got, want)
	}
}

func TestSnapshot_EmptyBaseMultiSetBuildsDistinctLenIndex(t *testing.T) {
	c, _ := openTempUint64Collection(t, snapshotExtOptions())

	ids := []uint64{1, 2, 3, 4}
	vals := []*Rec{
		{Name: "u1", Tags: nil},
		{Name: "u2", Tags: []string{}},
		{Name: "u3", Tags: []string{"go", "go"}},
		{Name: "u4", Tags: []string{"go", "db", "db"}},
	}
	if err := writeSets(c, ids, vals); err != nil {
		t.Fatalf("MultiSet: %v", err)
	}

	emptyQ := qx.Query(qx.EQ("tags", []string{}))
	gotEmpty, err := readQueryKeys(c, emptyQ)
	if err != nil {
		t.Fatalf("QueryKeys(empty tags): %v", err)
	}
	if want := []uint64{1, 2}; !slices.Equal(gotEmpty, want) {
		t.Fatalf("unexpected empty-tags result: got=%v want=%v", gotEmpty, want)
	}

	orderQ := queryOrderSortByArrayCount(qx.Query(), "tags", qx.ASC)
	gotOrder, err := readQueryKeys(c, orderQ)
	if err != nil {
		t.Fatalf("QueryKeys(array count): %v", err)
	}
	wantOrder, err := expectedKeysUint64(t, c, orderQ)
	if err != nil {
		t.Fatalf("expectedKeysUint64(array count): %v", err)
	}
	if !slices.Equal(gotOrder, wantOrder) {
		t.Fatalf("unexpected array-count order: got=%v want=%v", gotOrder, wantOrder)
	}
}

func TestSnapshotFromEmptyBase_StringScanKeysReadsDataBucket(t *testing.T) {
	c, _ := openTempStringCollection(t, Options{
		AnalyzeInterval: -1,
		BatchSoftLimit:  1,
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

	if err := writeSets(c, keys, vals); err != nil {
		t.Fatalf("MultiSet(seed): %v", err)
	}

	var got []string
	if err := readScanKeys(c, "k0254", func(id string) (bool, error) {
		got = append(got, id)
		return true, nil
	}); err != nil {
		t.Fatalf("ScanKeys: %v", err)
	}
	want := []string{"k0254", "k0255", "k0256"}
	if !slices.Equal(got, want) {
		t.Fatalf("ScanKeys=%v want %v", got, want)
	}
}

/**/

func snapshotExtOptions() Options {
	return Options{
		BatchSoftLimit:  1,
		AnalyzeInterval: -1,
	}
}

func TestSnapshotExt_ScanKeysPinsRuntimeSnapshotDuringScan(t *testing.T) {
	enableStoreStatsForTest(t)
	c, _ := openTempUint64Collection(t, snapshotExtOptions())

	if err := writeSet(c, 1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := writeSet(c, 2, &Rec{Name: "bob", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	before := c.StoreStats()
	if before.RegistrySize != 1 || before.PinnedRefs != 0 {
		t.Fatalf("unexpected stats before ScanKeys: %+v", before)
	}

	var got []uint64
	if err := readScanKeys(c, 0, func(id uint64) (bool, error) {
		during := c.StoreStats()
		if during.PinnedRefs != 1 {
			t.Fatalf("expected ScanKeys runtime snapshot pin during callback, stats=%+v", during)
		}
		got = append(got, id)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []uint64{1, 2}) {
		t.Fatalf("ScanKeys=%v want [1 2]", got)
	}

	after := c.StoreStats()
	if after.RegistrySize != 1 || after.PinnedRefs != 0 {
		t.Fatalf("unexpected stats after ScanKeys: %+v", after)
	}
}

func TestSnapshotExt_QuerySeesPublishedStateWhileSetCommitBlocked(t *testing.T) {
	c, _ := openTempUint64Collection(t, snapshotExtOptions())

	if err := writeSet(c, 1, &Rec{Name: "alice", Age: 30}); err != nil {
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
	blockChange := snapshotExtraBlockOnChangeUint64[Rec](entered, release, nil)

	done := make(chan error, 1)
	go func() {
		done <- writeSet(c, 2, &Rec{Name: "bob", Age: 20}, blockChange)
	}()

	snapshotExtraWait(t, entered, "staged set change hook")

	oldItems, err := readQuery(c, qx.Query(qx.EQ("name", "bob")))
	if err != nil {
		t.Fatalf("Query(bob before publish): %v", err)
	}
	if len(oldItems) != 0 {
		t.Fatalf("public query unexpectedly sees future write: %#v", oldItems)
	}

	oldItems, err = readQuery(c, qx.Query(qx.EQ("name", "alice")))
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

	currentItems, err := readQuery(c, qx.Query(qx.EQ("name", "bob")))
	if err != nil {
		t.Fatalf("Query(current snapshot): %v", err)
	}
	if len(currentItems) != 1 || currentItems[0] == nil || currentItems[0].Name != "bob" {
		t.Fatalf("unexpected current snapshot query result: %#v", currentItems)
	}
}

func TestSnapshotExt_TruncatePublishesEmptySnapshot(t *testing.T) {
	c, _ := openTempUint64Collection(t, snapshotExtOptions())

	if err := writeSet(c, 1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := writeSet(c, 2, &Rec{Name: "bob", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	oldStats := c.SnapshotStats()
	if err := c.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	mid := c.SnapshotStats()
	if mid.Sequence <= oldStats.Sequence {
		t.Fatalf("expected truncate to publish newer snapshot: old=%d new=%d", oldStats.Sequence, mid.Sequence)
	}

	keys, err := readQueryKeys(c, qx.Query(qx.EQ("name", "alice")))
	if err != nil {
		t.Fatalf("QueryKeys(after truncate): %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("expected current snapshot to be empty after truncate, got=%v", keys)
	}

	stats := c.SnapshotStats()
	if stats.Sequence == 0 {
		t.Fatalf("unexpected zero snapshot stats after truncate: %+v", stats)
	}
	assertNoFutureSnapshotRefs(t, c)
}

func TestSnapshotExt_StringScanKeysAcrossTruncate(t *testing.T) {
	c, _ := openTempStringCollection(t, snapshotExtOptions())

	if err := writeSet(c, "k1", &Rec{Name: "one"}); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	if err := writeSet(c, "k2", &Rec{Name: "two"}); err != nil {
		t.Fatalf("Set(k2): %v", err)
	}

	var got []string
	err := readScanKeys(c, "", func(id string) (bool, error) {
		got = append(got, id)
		return true, nil
	})
	if err != nil {
		t.Fatalf("ScanKeys: %v", err)
	}
	if !slices.Equal(got, []string{"k1", "k2"}) {
		t.Fatalf("ScanKeys=%v want [k1 k2]", got)
	}
	if err := c.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	got = got[:0]
	err = readScanKeys(c, "", func(id string) (bool, error) {
		got = append(got, id)
		return true, nil
	})
	if err != nil {
		t.Fatalf("ScanKeys after truncate: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("ScanKeys after truncate=%v want []", got)
	}
}

func TestSnapshotExt_StringScanKeysAfterTruncateReuse(t *testing.T) {
	c, _ := openTempStringCollection(t, snapshotExtOptions())

	if err := writeSet(c, "k1", &Rec{Name: "one"}); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	if err := writeSet(c, "k2", &Rec{Name: "two"}); err != nil {
		t.Fatalf("Set(k2): %v", err)
	}
	if err := c.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if err := writeSet(c, "a", &Rec{Name: "a"}); err != nil {
		t.Fatalf("Set(a): %v", err)
	}
	if err := writeSet(c, "z", &Rec{Name: "z"}); err != nil {
		t.Fatalf("Set(z): %v", err)
	}

	var got []string
	err := readScanKeys(c, "b", func(id string) (bool, error) {
		got = append(got, id)
		return true, nil
	})
	if err != nil {
		t.Fatalf("ScanKeys: %v", err)
	}
	if !slices.Equal(got, []string{"z"}) {
		t.Fatalf("ScanKeys=%v want [z]", got)
	}
}

func TestSnapshotExt_QueryKeysReleasesCurrentSnapshotPinOnError(t *testing.T) {
	enableStoreStatsForTest(t)
	c, _ := openTempUint64Collection(t, snapshotExtOptions())

	if err := writeSet(c, 1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	if _, err := readQueryKeys(c, qx.Query(qx.EQ("does_not_exist", 1))); err == nil {
		t.Fatalf("expected QueryKeys to fail for unknown field")
	}
	stats := c.StoreStats()
	if stats.RegistrySize != 1 {
		t.Fatalf("expected QueryKeys snapshot ref to remain registered")
	}
	if stats.PinnedRefs != 0 {
		t.Fatalf("expected QueryKeys pin release on error, stats=%+v", stats)
	}
}

func TestSnapshotExt_CountReleasesCurrentSnapshotPinOnError(t *testing.T) {
	enableStoreStatsForTest(t)
	c, _ := openTempUint64Collection(t, snapshotExtOptions())

	if err := writeSet(c, 1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	if _, err := readCount(c, qx.EQ("does_not_exist", 1)); err == nil {
		t.Fatalf("expected Count to fail for unknown field")
	}
	stats := c.StoreStats()
	if stats.RegistrySize != 1 {
		t.Fatalf("expected Count snapshot ref to remain registered")
	}
	if stats.PinnedRefs != 0 {
		t.Fatalf("expected Count pin release on error, stats=%+v", stats)
	}
}

func TestSnapshotExt_ScanKeysCallbackErrorReleasesRuntimeSnapshotPin(t *testing.T) {
	enableStoreStatsForTest(t)
	c, _ := openTempUint64Collection(t, snapshotExtOptions())

	if err := writeSet(c, 1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	wantErr := errors.New("stop")
	err := readScanKeys(c, 0, func(uint64) (bool, error) {
		stats := c.StoreStats()
		if stats.PinnedRefs != 1 {
			t.Fatalf("expected ScanKeys runtime snapshot pin during callback, stats=%+v", stats)
		}
		return false, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("ScanKeys err=%v want %v", err, wantErr)
	}
	stats := c.StoreStats()
	if stats.RegistrySize != 1 {
		t.Fatalf("expected current snapshot ref to remain registered")
	}
	if stats.PinnedRefs != 0 {
		t.Fatalf("expected ScanKeys to release runtime snapshot pin, stats=%+v", stats)
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
		BatchSoftLimit:  1,
		AnalyzeInterval: -1,
	})
}

func snapshotExtraOpenTempUint64Collection(t *testing.T, opts Options) (*Collection[uint64, snapshotExtraRec], string) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot_extra_uint64.db")
	bolt, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}

	opts = testOptions(opts)

	c, err := Open[uint64, snapshotExtraRec](bolt, opts)
	if err != nil {
		_ = bolt.Close()
		t.Fatalf("New: %v", err)
	}

	t.Cleanup(func() {
		_ = c.Close()
		_ = bolt.Close()
	})

	return c, path
}

func snapshotExtraOpenTempStringCollection(t *testing.T, opts Options) (*Collection[string, snapshotExtraRec], string) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot_extra_string.db")
	bolt, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}

	opts = testOptions(opts)

	c, err := Open[string, snapshotExtraRec](bolt, opts)
	if err != nil {
		_ = bolt.Close()
		t.Fatalf("New: %v", err)
	}

	t.Cleanup(func() {
		_ = c.Close()
		_ = bolt.Close()
	})

	return c, path
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

func snapshotExtraSeedGeneratedUint64Data(t *testing.T, c *Collection[uint64, snapshotExtraRec], n int, gen func(i int) *snapshotExtraRec) {
	t.Helper()

	c.disableSync()
	defer c.enableSync()

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
		if err := writeSets(c, batchIDs, batchVals); err != nil {
			t.Fatalf("MultiSet(seed batch=%d): %v", len(batchIDs), err)
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

func snapshotExtraBlockOnChangeUint64[V any](entered, release chan struct{}, err error) ExecOption[uint64, V] {
	var once sync.Once
	return OnChange(func(*Tx, uint64, *V, *V) error {
		once.Do(func() {
			close(entered)
			<-release
		})
		return err
	})
}

func snapshotExtraBlockOnChangeString[V any](entered, release chan struct{}, err error) ExecOption[string, V] {
	var once sync.Once
	return OnChange(func(*Tx, string, *V, *V) error {
		once.Do(func() {
			close(entered)
			<-release
		})
		return err
	})
}

func snapshotExtraAssertNoFutureSnapshotRefs[K ~uint64 | ~string, V any](tb testing.TB, c *Collection[K, V]) {
	tb.Helper()

	assertNoFutureSnapshotRefs(tb, c)
}

func TestSnapshotExtra_PublicQueriesIgnoreInFlightSetOnChange(t *testing.T) {
	c, _ := snapshotExtraOpenTempUint64Collection(t, snapshotExtraOptions())

	if err := writeSet(c, 1, &snapshotExtraRec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := writeSet(c, 2, &snapshotExtraRec{Name: "bob", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	oldSeq := c.SnapshotStats().Sequence

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	blockChange := snapshotExtraBlockOnChangeUint64[snapshotExtraRec](entered, release, nil)

	done := make(chan error, 1)
	go func() {
		done <- writeSet(c, 2, &snapshotExtraRec{Name: "charlie", Age: 20}, blockChange)
	}()

	snapshotExtraWait(t, entered, "set before change hook")
	if got := c.SnapshotStats().Sequence; got != oldSeq {
		t.Fatalf("current snapshot changed before commit publish: got=%d want=%d", got, oldSeq)
	}

	beforeBob, err := readQueryKeys(c, qx.Query(qx.EQ("name", "bob")))
	if err != nil {
		t.Fatalf("QueryKeys(bob before publish): %v", err)
	}
	if !slices.Equal(beforeBob, []uint64{2}) {
		t.Fatalf("public query lost committed row before publish: got=%v want=[2]", beforeBob)
	}
	beforeCharlie, err := readQueryKeys(c, qx.Query(qx.EQ("name", "charlie")))
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

	currentCharlie, err := readQueryKeys(c, qx.Query(qx.EQ("name", "charlie")))
	if err != nil {
		t.Fatalf("QueryKeys(charlie): %v", err)
	}
	if !slices.Equal(currentCharlie, []uint64{2}) {
		t.Fatalf("current snapshot missed committed update: got=%v want=[2]", currentCharlie)
	}

	currentBob, err := readQueryKeys(c, qx.Query(qx.EQ("name", "bob")))
	if err != nil {
		t.Fatalf("QueryKeys(bob): %v", err)
	}
	if len(currentBob) != 0 {
		t.Fatalf("current snapshot kept stale pre-update row: %v", currentBob)
	}

	snapshotExtraAssertNoFutureSnapshotRefs(t, c)
}

func TestSnapshotExtra_PublicQueriesIgnoreInFlightSetRollback(t *testing.T) {
	c, _ := snapshotExtraOpenTempUint64Collection(t, snapshotExtraOptions())

	if err := writeSet(c, 1, &snapshotExtraRec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := writeSet(c, 2, &snapshotExtraRec{Name: "bob", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	oldSeq := c.SnapshotStats().Sequence

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	blockChange := snapshotExtraBlockOnChangeUint64[snapshotExtraRec](entered, release, fmt.Errorf("failpoint: rollback set"))

	done := make(chan error, 1)
	go func() {
		done <- writeSet(c, 2, &snapshotExtraRec{Name: "charlie", Age: 20}, blockChange)
	}()

	snapshotExtraWait(t, entered, "rollback set before change hook")

	beforeBob, err := readQueryKeys(c, qx.Query(qx.EQ("name", "bob")))
	if err != nil {
		t.Fatalf("QueryKeys(bob before rollback): %v", err)
	}
	if !slices.Equal(beforeBob, []uint64{2}) {
		t.Fatalf("public query lost committed row before rollback: got=%v want=[2]", beforeBob)
	}
	beforeCharlie, err := readQueryKeys(c, qx.Query(qx.EQ("name", "charlie")))
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

	if got := c.SnapshotStats().Sequence; got != oldSeq {
		t.Fatalf("rollback replaced published snapshot: got=%d want=%d", got, oldSeq)
	}

	currentBob, err := readQueryKeys(c, qx.Query(qx.EQ("name", "bob")))
	if err != nil {
		t.Fatalf("QueryKeys(bob): %v", err)
	}
	if !slices.Equal(currentBob, []uint64{2}) {
		t.Fatalf("current snapshot lost pre-rollback row: got=%v want=[2]", currentBob)
	}

	currentCharlie, err := readQueryKeys(c, qx.Query(qx.EQ("name", "charlie")))
	if err != nil {
		t.Fatalf("QueryKeys(charlie): %v", err)
	}
	if len(currentCharlie) != 0 {
		t.Fatalf("current snapshot observed rolled-back row: %v", currentCharlie)
	}

	snapshotExtraAssertNoFutureSnapshotRefs(t, c)
}

func TestSnapshotExtra_PublicQueriesIgnoreInFlightMultiSetOnChange(t *testing.T) {
	c, _ := snapshotExtraOpenTempUint64Collection(t, snapshotExtraOptions())

	if err := writeSet(c, 1, &snapshotExtraRec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := writeSet(c, 2, &snapshotExtraRec{Name: "bob", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	oldSeq := c.SnapshotStats().Sequence

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	blockChange := snapshotExtraBlockOnChangeUint64[snapshotExtraRec](entered, release, nil)

	done := make(chan error, 1)
	go func() {
		done <- writeSets(c,
			[]uint64{2, 3},
			[]*snapshotExtraRec{
				{Name: "charlie", Age: 20},
				{Name: "dave", Age: 40},
			},
			blockChange,
		)
	}()

	snapshotExtraWait(t, entered, "batch_set before change hook")
	if got := c.SnapshotStats().Sequence; got != oldSeq {
		t.Fatalf("current snapshot changed before batch_set publish: got=%d want=%d", got, oldSeq)
	}

	beforeBob, err := readQueryKeys(c, qx.Query(qx.EQ("name", "bob")))
	if err != nil {
		t.Fatalf("QueryKeys(bob before batch publish): %v", err)
	}
	if !slices.Equal(beforeBob, []uint64{2}) {
		t.Fatalf("public query lost committed batch row before publish: got=%v want=[2]", beforeBob)
	}
	beforeCharlie, err := readQueryKeys(c, qx.Query(qx.EQ("name", "charlie")))
	if err != nil {
		t.Fatalf("QueryKeys(charlie before batch publish): %v", err)
	}
	if len(beforeCharlie) != 0 {
		t.Fatalf("public query observed staged future batch row: %v", beforeCharlie)
	}
	beforeDave, err := readQueryKeys(c, qx.Query(qx.EQ("name", "dave")))
	if err != nil {
		t.Fatalf("QueryKeys(dave before batch publish): %v", err)
	}
	if len(beforeDave) != 0 {
		t.Fatalf("public query observed staged inserted batch row: %v", beforeDave)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("MultiSet(update+insert): %v", err)
	}

	currentCharlie, err := readQueryKeys(c, qx.Query(qx.EQ("name", "charlie")))
	if err != nil {
		t.Fatalf("QueryKeys(charlie): %v", err)
	}
	if !slices.Equal(currentCharlie, []uint64{2}) {
		t.Fatalf("current snapshot missed committed batch update: got=%v want=[2]", currentCharlie)
	}
	currentDave, err := readQueryKeys(c, qx.Query(qx.EQ("name", "dave")))
	if err != nil {
		t.Fatalf("QueryKeys(dave): %v", err)
	}
	if !slices.Equal(currentDave, []uint64{3}) {
		t.Fatalf("current snapshot missed committed batch insert: got=%v want=[3]", currentDave)
	}

	snapshotExtraAssertNoFutureSnapshotRefs(t, c)
}

func TestSnapshotExtra_PublicQueriesIgnoreInFlightStringSetOnChange(t *testing.T) {
	c, _ := snapshotExtraOpenTempStringCollection(t, snapshotExtraOptions())

	if err := writeSet(c, "k1", &snapshotExtraRec{Name: "one", Age: 10}); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	if err := writeSet(c, "k2", &snapshotExtraRec{Name: "two", Age: 20}); err != nil {
		t.Fatalf("Set(k2): %v", err)
	}

	oldSeq := c.SnapshotStats().Sequence

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	blockChange := snapshotExtraBlockOnChangeString[snapshotExtraRec](entered, release, nil)

	done := make(chan error, 1)
	go func() {
		done <- writeSet(c, "future", &snapshotExtraRec{Name: "future", Age: 99}, blockChange)
	}()

	snapshotExtraWait(t, entered, "string set before change hook")
	if got := c.SnapshotStats().Sequence; got != oldSeq {
		t.Fatalf("current string snapshot changed before publish: got=%d want=%d", got, oldSeq)
	}

	beforeFuture, err := readQueryKeys(c, qx.Query(qx.EQ("name", "future")))
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

	currentFuture, err := readQueryKeys(c, qx.Query(qx.EQ("name", "future")))
	if err != nil {
		t.Fatalf("QueryKeys(future): %v", err)
	}
	if !slices.Equal(currentFuture, []string{"future"}) {
		t.Fatalf("current string snapshot missed committed future key: got=%v want=[future]", currentFuture)
	}

	snapshotExtraAssertNoFutureSnapshotRefs(t, c)
}

func TestSnapshotExtra_StringKeyRollbackLeavesPublicStateCleanAndRecovers(t *testing.T) {
	c, _ := snapshotExtraOpenTempStringCollection(t, snapshotExtraOptions())

	if err := writeSet(c, "k1", &snapshotExtraRec{Name: "one", Age: 10}); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	if err := writeSet(c, "k2", &snapshotExtraRec{Name: "two", Age: 20}); err != nil {
		t.Fatalf("Set(k2): %v", err)
	}

	oldSeq := c.SnapshotStats().Sequence

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	blockChange := snapshotExtraBlockOnChangeString[snapshotExtraRec](entered, release, fmt.Errorf("failpoint: rollback string set"))

	done := make(chan error, 1)
	go func() {
		done <- writeSet(c, "ghost", &snapshotExtraRec{Name: "ghost", Age: 99}, blockChange)
	}()

	snapshotExtraWait(t, entered, "string rollback set before change hook")

	beforeGhost, err := readQueryKeys(c, qx.Query(qx.EQ("name", "ghost")))
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

	if got := c.SnapshotStats().Sequence; got != oldSeq {
		t.Fatalf("rollback replaced published string snapshot: got=%d want=%d", got, oldSeq)
	}
	if got, err := readGet(c, "ghost"); err != nil {
		t.Fatalf("Get(ghost): %v", err)
	} else if got != nil {
		t.Fatalf("rolled-back ghost key became readable: %#v", got)
	}
	ghostKeys, err := readQueryKeys(c, qx.Query(qx.EQ("name", "ghost")))
	if err != nil {
		t.Fatalf("QueryKeys(name=ghost): %v", err)
	}
	if len(ghostKeys) != 0 {
		t.Fatalf("rolled-back ghost key remained queryable: %v", ghostKeys)
	}

	if err := writeSet(c, "later", &snapshotExtraRec{Name: "later", Age: 50}); err != nil {
		t.Fatalf("Set(later): %v", err)
	}
	queryLater, err := readQueryKeys(c, qx.Query(qx.EQ("name", "later")))
	if err != nil {
		t.Fatalf("QueryKeys(name=later): %v", err)
	}
	if !slices.Equal(queryLater, []string{"later"}) {
		t.Fatalf("query path missed later key after rollback recovery: got=%v want=[later]", queryLater)
	}
	ghostKeys, err = readQueryKeys(c, qx.Query(qx.EQ("name", "ghost")))
	if err != nil {
		t.Fatalf("QueryKeys(name=ghost after recovery): %v", err)
	}
	if len(ghostKeys) != 0 {
		t.Fatalf("rolled-back ghost key became queryable after recovery: %v", ghostKeys)
	}

	snapshotExtraAssertNoFutureSnapshotRefs(t, c)
}

func TestSnapshotExtra_StringKeyBatchRollbackLeavesPublicStateCleanAndRecovers(t *testing.T) {
	c, _ := snapshotExtraOpenTempStringCollection(t, snapshotExtraOptions())

	if err := writeSet(c, "k1", &snapshotExtraRec{Name: "one", Age: 10}); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}

	oldSeq := c.SnapshotStats().Sequence

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	blockChange := snapshotExtraBlockOnChangeString[snapshotExtraRec](entered, release, fmt.Errorf("failpoint: rollback string batch_set"))

	done := make(chan error, 1)
	go func() {
		done <- writeSets(c,
			[]string{"ghostA", "ghostB"},
			[]*snapshotExtraRec{
				{Name: "ghostA", Age: 11},
				{Name: "ghostB", Age: 12},
			},
			blockChange,
		)
	}()

	snapshotExtraWait(t, entered, "string rollback batch_set before change hook")

	beforeGhostA, err := readQueryKeys(c, qx.Query(qx.EQ("name", "ghostA")))
	if err != nil {
		t.Fatalf("QueryKeys(ghostA before rollback): %v", err)
	}
	if len(beforeGhostA) != 0 {
		t.Fatalf("public query observed ghostA row before rollback: %v", beforeGhostA)
	}
	beforeGhostB, err := readQueryKeys(c, qx.Query(qx.EQ("name", "ghostB")))
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

	if got := c.SnapshotStats().Sequence; got != oldSeq {
		t.Fatalf("rollback replaced published string snapshot: got=%d want=%d", got, oldSeq)
	}
	if got, err := readGet(c, "ghostA"); err != nil {
		t.Fatalf("Get(ghostA): %v", err)
	} else if got != nil {
		t.Fatalf("rolled-back ghostA key became readable: %#v", got)
	}
	if got, err := readGet(c, "ghostB"); err != nil {
		t.Fatalf("Get(ghostB): %v", err)
	} else if got != nil {
		t.Fatalf("rolled-back ghostB key became readable: %#v", got)
	}
	ghostAKeys, err := readQueryKeys(c, qx.Query(qx.EQ("name", "ghostA")))
	if err != nil {
		t.Fatalf("QueryKeys(name=ghostA): %v", err)
	}
	if len(ghostAKeys) != 0 {
		t.Fatalf("rolled-back ghostA key remained queryable: %v", ghostAKeys)
	}
	ghostBKeys, err := readQueryKeys(c, qx.Query(qx.EQ("name", "ghostB")))
	if err != nil {
		t.Fatalf("QueryKeys(name=ghostB): %v", err)
	}
	if len(ghostBKeys) != 0 {
		t.Fatalf("rolled-back ghostB key remained queryable: %v", ghostBKeys)
	}

	if err := writeSets(c,
		[]string{"realA", "realB"},
		[]*snapshotExtraRec{
			{Name: "realA", Age: 21},
			{Name: "realB", Age: 22},
		},
	); err != nil {
		t.Fatalf("MultiSet(realA,realB): %v", err)
	}
	queryReal, err := readQueryKeys(c, qx.Query().Sort("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(real recovery): %v", err)
	}
	if !slices.Equal(queryReal, []string{"k1", "realA", "realB"}) {
		t.Fatalf("query path drift after batch rollback recovery: got=%v want=[k1 realA realB]", queryReal)
	}
	ghostAKeys, err = readQueryKeys(c, qx.Query(qx.EQ("name", "ghostA")))
	if err != nil {
		t.Fatalf("QueryKeys(name=ghostA after recovery): %v", err)
	}
	if len(ghostAKeys) != 0 {
		t.Fatalf("rolled-back ghostA key became queryable after recovery: %v", ghostAKeys)
	}
	ghostBKeys, err = readQueryKeys(c, qx.Query(qx.EQ("name", "ghostB")))
	if err != nil {
		t.Fatalf("QueryKeys(name=ghostB after recovery): %v", err)
	}
	if len(ghostBKeys) != 0 {
		t.Fatalf("rolled-back ghostB key became queryable after recovery: %v", ghostBKeys)
	}

	snapshotExtraAssertNoFutureSnapshotRefs(t, c)
}

func TestSnapshotExtra_MaterializedPredCacheDoesNotLeakAcrossTouchedSnapshot(t *testing.T) {
	opts := snapshotExtraOptions()
	opts.MaterializedPredicateCacheMaxEntries = 64
	c, _ := snapshotExtraOpenTempUint64Collection(t, opts)

	if err := writeSet(c, 1, &snapshotExtraRec{Name: "alice", Email: "alpha@example.com"}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := writeSet(c, 2, &snapshotExtraRec{Name: "bob", Email: "beta@example.com"}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	alphaQ := qx.Query(qx.PREFIX("email", "alpha"))
	futureQ := qx.Query(qx.PREFIX("email", "future"))
	oldAlpha, err := readQueryKeys(c, alphaQ)
	if err != nil {
		t.Fatalf("QueryKeys(alpha warm): %v", err)
	}
	if !slices.Equal(oldAlpha, []uint64{1}) {
		t.Fatalf("unexpected warm alpha result: got=%v want=[1]", oldAlpha)
	}
	oldFuture, err := readQueryKeys(c, futureQ)
	if err != nil {
		t.Fatalf("QueryKeys(future warm): %v", err)
	}
	if len(oldFuture) != 0 {
		t.Fatalf("unexpected warm future result: %v", oldFuture)
	}
	if err = writePatch(c, 1, []Field{{Name: "email", Value: "gamma@example.com"}}); err != nil {
		t.Fatalf("Patch(email): %v", err)
	}
	if err := writeSet(c, 3, &snapshotExtraRec{Name: "carol", Email: "future@example.com"}); err != nil {
		t.Fatalf("Set(3): %v", err)
	}

	currentAlpha, err := readQueryKeys(c, alphaQ)
	if err != nil {
		t.Fatalf("QueryKeys(alpha): %v", err)
	}
	if len(currentAlpha) != 0 {
		t.Fatalf("current snapshot kept stale alpha match: %v", currentAlpha)
	}

	currentFuture, err := readQueryKeys(c, futureQ)
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

	dbA, err := Open[uint64, snapshotExtraRec](raw, opts)
	if err != nil {
		t.Fatalf("New(bucket A): %v", err)
	}
	defer func() { _ = dbA.Close() }()

	opts.BucketName = "snapshot_extra_b"
	dbB, err := Open[uint64, snapshotExtraRec](raw, opts)
	if err != nil {
		t.Fatalf("New(bucket B): %v", err)
	}
	defer func() { _ = dbB.Close() }()

	if err := writeSet(dbA, 1, &snapshotExtraRec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("dbA Set(1): %v", err)
	}
	if err := writeSet(dbA, 2, &snapshotExtraRec{Name: "bob", Age: 20}); err != nil {
		t.Fatalf("dbA Set(2): %v", err)
	}

	beforeSeq := dbA.SnapshotStats().Sequence

	writerDone := make(chan error, 1)
	go func() {
		for i := 0; i < 64; i++ {
			if err := writeSet(dbB, uint64(i+1), &snapshotExtraRec{
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
		items, err := readQueryKeys(dbA, qx.Query().Sort("name", qx.ASC))
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

	keys, err := readQueryKeys(dbA, qx.Query().Sort("name", qx.ASC))
	if err != nil {
		t.Fatalf("dbA QueryKeys(final): %v", err)
	}
	if !slices.Equal(keys, []uint64{1, 2}) {
		t.Fatalf("dbA final keys drifted after other bucket writes: %v", keys)
	}
}

func TestSnapshotExtra_PublicQueriesIgnoreInFlightDeleteOnChange(t *testing.T) {
	c, _ := snapshotExtraOpenTempUint64Collection(t, snapshotExtraOptions())

	if err := writeSets(c,
		[]uint64{1, 2, 3},
		[]*snapshotExtraRec{
			{Name: "alice", Age: 30},
			{Name: "bob", Age: 20},
			{Name: "carol", Age: 40},
		},
	); err != nil {
		t.Fatalf("MultiSet(seed): %v", err)
	}

	oldSeq := c.SnapshotStats().Sequence

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	blockChange := snapshotExtraBlockOnChangeUint64[snapshotExtraRec](entered, release, nil)

	done := make(chan error, 1)
	go func() {
		done <- writeDelete(c, 2, blockChange)
	}()

	snapshotExtraWait(t, entered, "delete before change hook")
	if got := c.SnapshotStats().Sequence; got != oldSeq {
		t.Fatalf("current snapshot changed before delete publish: got=%d want=%d", got, oldSeq)
	}

	beforeAll, err := readQueryKeys(c, qx.Query().Sort("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(all before delete publish): %v", err)
	}
	if !slices.Equal(beforeAll, []uint64{1, 2, 3}) {
		t.Fatalf("public query drifted before delete publish: got=%v want=[1 2 3]", beforeAll)
	}
	beforeBob, err := readQueryKeys(c, qx.Query(qx.EQ("name", "bob")))
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

	currentKeys, err := readQueryKeys(c, qx.Query().Sort("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(current after delete): %v", err)
	}
	if !slices.Equal(currentKeys, []uint64{1, 3}) {
		t.Fatalf("current snapshot missed committed delete: got=%v want=[1 3]", currentKeys)
	}
	currentBob, err := readQueryKeys(c, qx.Query(qx.EQ("name", "bob")))
	if err != nil {
		t.Fatalf("QueryKeys(bob after delete): %v", err)
	}
	if len(currentBob) != 0 {
		t.Fatalf("current snapshot retained deleted row: %v", currentBob)
	}

	snapshotExtraAssertNoFutureSnapshotRefs(t, c)
}

func TestSnapshotExtra_PublicQueriesIgnoreInFlightDeleteRollback(t *testing.T) {
	c, _ := snapshotExtraOpenTempUint64Collection(t, snapshotExtraOptions())

	if err := writeSets(c,
		[]uint64{1, 2, 3},
		[]*snapshotExtraRec{
			{Name: "alice", Age: 30},
			{Name: "bob", Age: 20},
			{Name: "carol", Age: 40},
		},
	); err != nil {
		t.Fatalf("MultiSet(seed): %v", err)
	}

	oldSeq := c.SnapshotStats().Sequence

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	blockChange := snapshotExtraBlockOnChangeUint64[snapshotExtraRec](entered, release, fmt.Errorf("failpoint: rollback delete"))

	done := make(chan error, 1)
	go func() {
		done <- writeDelete(c, 2, blockChange)
	}()

	snapshotExtraWait(t, entered, "rollback delete before change hook")

	beforeAll, err := readQueryKeys(c, qx.Query().Sort("name", qx.ASC))
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

	if got := c.SnapshotStats().Sequence; got != oldSeq {
		t.Fatalf("rollback replaced published snapshot: got=%d want=%d", got, oldSeq)
	}

	currentKeys, err := readQueryKeys(c, qx.Query().Sort("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(current after rollback): %v", err)
	}
	if !slices.Equal(currentKeys, []uint64{1, 2, 3}) {
		t.Fatalf("current snapshot changed after rolled-back delete: got=%v want=[1 2 3]", currentKeys)
	}
	currentBob, err := readQueryKeys(c, qx.Query(qx.EQ("name", "bob")))
	if err != nil {
		t.Fatalf("QueryKeys(bob after rollback): %v", err)
	}
	if !slices.Equal(currentBob, []uint64{2}) {
		t.Fatalf("current snapshot lost rolled-back row: got=%v want=[2]", currentBob)
	}

	snapshotExtraAssertNoFutureSnapshotRefs(t, c)
}

func TestSnapshotExtra_PublicQueriesIgnoreInFlightMultiDeleteOnChange(t *testing.T) {
	c, _ := snapshotExtraOpenTempUint64Collection(t, snapshotExtraOptions())

	if err := writeSets(c,
		[]uint64{1, 2, 3, 4},
		[]*snapshotExtraRec{
			{Name: "alice", Age: 30},
			{Name: "bob", Age: 20},
			{Name: "carol", Age: 40},
			{Name: "dave", Age: 50},
		},
	); err != nil {
		t.Fatalf("MultiSet(seed): %v", err)
	}

	oldSeq := c.SnapshotStats().Sequence

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	blockChange := snapshotExtraBlockOnChangeUint64[snapshotExtraRec](entered, release, nil)

	done := make(chan error, 1)
	go func() {
		done <- writeDeletes(c, []uint64{2, 2, 3, 3}, blockChange)
	}()

	snapshotExtraWait(t, entered, "batch_delete before change hook")
	if got := c.SnapshotStats().Sequence; got != oldSeq {
		t.Fatalf("current snapshot changed before batch_delete publish: got=%d want=%d", got, oldSeq)
	}

	beforeAll, err := readQueryKeys(c, qx.Query().Sort("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(all before batch_delete publish): %v", err)
	}
	if !slices.Equal(beforeAll, []uint64{1, 2, 3, 4}) {
		t.Fatalf("public query drifted before batch_delete publish: got=%v want=[1 2 3 4]", beforeAll)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("MultiDelete(2,2,3,3): %v", err)
	}

	currentKeys, err := readQueryKeys(c, qx.Query().Sort("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(current after batch_delete): %v", err)
	}
	if !slices.Equal(currentKeys, []uint64{1, 4}) {
		t.Fatalf("current snapshot missed committed batch_delete: got=%v want=[1 4]", currentKeys)
	}
	currentDeleted, err := readQueryKeys(c, qx.Query(qx.IN("name", []string{"bob", "carol"})))
	if err != nil {
		t.Fatalf("QueryKeys(deleted names after batch_delete): %v", err)
	}
	if len(currentDeleted) != 0 {
		t.Fatalf("current snapshot retained batch-deleted rows: %v", currentDeleted)
	}

	snapshotExtraAssertNoFutureSnapshotRefs(t, c)
}

func TestSnapshotExtra_PublicQueriesIgnoreInFlightMultiDeleteRollback(t *testing.T) {
	c, _ := snapshotExtraOpenTempUint64Collection(t, snapshotExtraOptions())

	if err := writeSets(c,
		[]uint64{1, 2, 3, 4},
		[]*snapshotExtraRec{
			{Name: "alice", Age: 30},
			{Name: "bob", Age: 20},
			{Name: "carol", Age: 40},
			{Name: "dave", Age: 50},
		},
	); err != nil {
		t.Fatalf("MultiSet(seed): %v", err)
	}

	oldSeq := c.SnapshotStats().Sequence

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	blockChange := snapshotExtraBlockOnChangeUint64[snapshotExtraRec](entered, release, fmt.Errorf("failpoint: rollback batch_delete"))

	done := make(chan error, 1)
	go func() {
		done <- writeDeletes(c, []uint64{2, 2, 3, 3}, blockChange)
	}()

	snapshotExtraWait(t, entered, "rollback batch_delete before change hook")

	beforeAll, err := readQueryKeys(c, qx.Query().Sort("name", qx.ASC))
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

	if got := c.SnapshotStats().Sequence; got != oldSeq {
		t.Fatalf("rollback replaced published snapshot: got=%d want=%d", got, oldSeq)
	}

	currentKeys, err := readQueryKeys(c, qx.Query().Sort("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(current after rollback): %v", err)
	}
	if !slices.Equal(currentKeys, []uint64{1, 2, 3, 4}) {
		t.Fatalf("current snapshot changed after rolled-back batch_delete: got=%v want=[1 2 3 4]", currentKeys)
	}

	snapshotExtraAssertNoFutureSnapshotRefs(t, c)
}

func TestSnapshotExtra_PrefixQueriesAfterTruncateRebuild(t *testing.T) {
	opts := snapshotExtraOptions()
	opts.MaterializedPredicateCacheMaxEntries = 64
	c, _ := snapshotExtraOpenTempUint64Collection(t, opts)

	if err := writeSets(c,
		[]uint64{1, 2, 3},
		[]*snapshotExtraRec{
			{Name: "alice", Email: "alpha-one@example.com"},
			{Name: "bob", Email: "beta@example.com"},
			{Name: "carol", Email: "alpha-two@example.com"},
		},
	); err != nil {
		t.Fatalf("MultiSet(seed): %v", err)
	}

	alphaQ := qx.Query(qx.PREFIX("email", "alpha"))
	futureQ := qx.Query(qx.PREFIX("email", "future"))

	oldAlpha, err := readQueryKeys(c, alphaQ)
	if err != nil {
		t.Fatalf("QueryKeys(alpha warm): %v", err)
	}
	if !queryIDsEqual(alphaQ, oldAlpha, []uint64{1, 3}) {
		t.Fatalf("unexpected alpha warm result: got=%v want=[1 3]", oldAlpha)
	}
	oldFuture, err := readQueryKeys(c, futureQ)
	if err != nil {
		t.Fatalf("QueryKeys(future warm): %v", err)
	}
	if len(oldFuture) != 0 {
		t.Fatalf("unexpected future warm result: %v", oldFuture)
	}

	if err := c.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if err := writeSets(c,
		[]uint64{1, 2},
		[]*snapshotExtraRec{
			{Name: "new-future", Email: "future@example.com"},
			{Name: "new-gamma", Email: "gamma@example.com"},
		},
	); err != nil {
		t.Fatalf("MultiSet(rebuild): %v", err)
	}

	currentAlpha, err := readQueryKeys(c, alphaQ)
	if err != nil {
		t.Fatalf("QueryKeys(current alpha): %v", err)
	}
	if len(currentAlpha) != 0 {
		t.Fatalf("current snapshot retained stale alpha match after truncate rebuild: %v", currentAlpha)
	}
	currentFuture, err := readQueryKeys(c, futureQ)
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
	c, _ := snapshotExtraOpenTempUint64Collection(t, opts)

	snapshotExtraSeedGeneratedUint64Data(t, c, 512, func(i int) *snapshotExtraRec {
		return &snapshotExtraRec{
			Name: fmt.Sprintf("user-%04d", i),
			Age:  i,
		}
	})

	oldHighQ := qx.Query(qx.GTE("age", 400))
	oldHigh, err := readQueryKeys(c, oldHighQ)
	if err != nil {
		t.Fatalf("QueryKeys(old high warm): %v", err)
	}
	if len(oldHigh) != 113 || !slices.Contains(oldHigh, uint64(400)) || !slices.Contains(oldHigh, uint64(512)) {
		t.Fatalf("unexpected old high range result: len=%d ids=%v", len(oldHigh), oldHigh)
	}

	if err := c.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	snapshotExtraSeedGeneratedUint64Data(t, c, 128, func(i int) *snapshotExtraRec {
		return &snapshotExtraRec{
			Name: fmt.Sprintf("rebuild-%03d", i),
			Age:  i,
		}
	})

	currentHigh, err := readQueryKeys(c, oldHighQ)
	if err != nil {
		t.Fatalf("QueryKeys(current high): %v", err)
	}
	if len(currentHigh) != 0 {
		t.Fatalf("current snapshot retained stale high-age range after rebuild: %v", currentHigh)
	}

	currentMidQ := qx.Query(qx.GTE("age", 64))
	currentMid, err := readQueryKeys(c, currentMidQ)
	if err != nil {
		t.Fatalf("QueryKeys(current mid): %v", err)
	}
	if len(currentMid) != 65 || !slices.Contains(currentMid, uint64(64)) || !slices.Contains(currentMid, uint64(128)) {
		t.Fatalf("unexpected current mid range result after rebuild: len=%d ids=%v", len(currentMid), currentMid)
	}
}

func TestSnapshotExtra_StringQueryKeysStaySingleSnapshotAcrossConcurrentTruncateRebuilds(t *testing.T) {
	c, _ := snapshotExtraOpenTempStringCollection(t, snapshotExtraOptions())

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
	if err := writeSets(c, keys, vals); err != nil {
		t.Fatalf("MultiSet(seed): %v", err)
	}

	wantOld := slices.Clone(keys)
	queryKeys := func() ([]string, error) {
		return readQueryKeys(c, qx.Query())
	}
	validKeys := func(got []string) bool {
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
			if err := c.Truncate(); err != nil {
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
			if err := writeSets(c, rebuildKeys, rebuildVals); err != nil {
				setFailed(fmt.Sprintf("MultiSet(rebuild) failed: %v", err))
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
			got, err := queryKeys()
			if err != nil {
				setFailed(fmt.Sprintf("QueryKeys failed: %v", err))
				return
			}
			if !validKeys(got) {
				setFailed(fmt.Sprintf("QueryKeys returned hybrid snapshot: got=%v", got))
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
