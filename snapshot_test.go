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
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
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

func snapshotPostingContainsAll(t *testing.T, ids posting.List, want ...uint64) {
	t.Helper()
	if ids.IsEmpty() {
		t.Fatalf("posting is empty")
	}
	for _, id := range want {
		if !ids.Contains(id) {
			t.Fatalf("posting is missing id=%d", id)
		}
	}
}

func snapshotExtMatPredKey(key string) qcache.MaterializedPredKey {
	if parsed, ok := qcache.MaterializedPredKeyFromEncoded(key); ok {
		return parsed
	}
	if key == "" {
		return qcache.MaterializedPredKey{}
	}
	return qcache.MaterializedPredKeyFromOpaque(key)
}

func snapshotExtLoadMaterializedPred(s *snapshot.View, key string) (posting.List, bool) {
	return s.LoadMaterializedPredKey(snapshotExtMatPredKey(key))
}

func snapshotExtStoreMaterializedPred(s *snapshot.View, key string, ids posting.List) {
	s.StoreMaterializedPredKey(snapshotExtMatPredKey(key), ids)
}

func (db *DB[K, V]) clearCurrentSnapshotCachesForTesting() {
	db.engine.snapshot.Current().ClearRuntimeCaches()
}

func TestSnapshotSequence_PublishedOnWrite(t *testing.T) {
	db, _ := openTempDBUint64(t)

	before := db.engine.snapshot.Current().Seq

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	after := db.engine.snapshot.Current().Seq
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

	oldSequence := db.engine.snapshot.Current().Seq

	if err := db.Set(2, &Rec{Name: "new", Age: 20}); err != nil {
		t.Fatalf("concurrent Set: %v", err)
	}

	latest := db.engine.snapshot.Current().Seq
	if latest <= oldSequence {
		t.Fatalf("latest sequence did not advance: old=%d latest=%d", oldSequence, latest)
	}

	if _, _, ok := db.engine.snapshot.PinBySeq(oldSequence); ok {
		t.Fatalf("previous snapshot unexpectedly remained pinable: sequence=%d", oldSequence)
	}
}

func TestSnapshotSequence_AdvancesOnWrite(t *testing.T) {
	db, _ := openTempDBUint64(t)

	before := db.engine.snapshot.Current().Seq
	if err := db.Set(1, &Rec{Name: "x", Age: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	after := db.engine.snapshot.Current().Seq
	if after <= before {
		t.Fatalf("snapshot sequence did not advance on write: before=%d after=%d", before, after)
	}
}

func TestSnapshot_PublishedAsFullState(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Tags: []string{"go", "db"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	s := db.engine.snapshot.Current()
	nameIDs := s.FieldLookupPostingRetained("name", "alice")
	snapshotPostingContainsAll(t, nameIDs, 1)

	tagsIDs := s.FieldLookupPostingRetained("tags", "go")
	snapshotPostingContainsAll(t, tagsIDs, 1)

	tagsAcc := db.engine.schema.IndexedByName["tags"]
	lenIDs := indexdata.NewFieldIndexViewFromStorage(s.LenIndex[tagsAcc.Ordinal]).LookupPostingRetained(keycodec.U64ByteString(2))
	snapshotPostingContainsAll(t, lenIDs, 1)
}

func TestSnapshot_PreviousSnapshotRemainsImmutable(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice"}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	s1 := db.engine.snapshot.Current()

	if err := db.Set(2, &Rec{Name: "bob"}); err != nil {
		t.Fatalf("second Set: %v", err)
	}
	s2 := db.engine.snapshot.Current()
	if s1 == s2 {
		t.Fatalf("expected a newly published snapshot instance")
	}

	oldBob := s1.FieldLookupPostingRetained("name", "bob")
	if !oldBob.IsEmpty() {
		t.Fatalf("old snapshot unexpectedly sees future write")
	}

	newBob := s2.FieldLookupPostingRetained("name", "bob")
	snapshotPostingContainsAll(t, newBob, 2)
}

func TestSnapshotStrMap_OldSnapshotDoesNotSeeFutureKeyMappings(t *testing.T) {
	db, _ := openTempDBString(t)

	if err := db.Set("k1", &Rec{Name: "one"}); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	s1 := db.engine.snapshot.Current()
	idx1, ok := s1.StrMap.Index("k1")
	if !ok || idx1 == 0 {
		t.Fatalf("expected k1 in first snapshot")
	}

	if err := db.Set("k2", &Rec{Name: "two"}); err != nil {
		t.Fatalf("Set(k2): %v", err)
	}
	s2 := db.engine.snapshot.Current()
	if _, ok := s1.StrMap.Index("k2"); ok {
		t.Fatalf("old snapshot unexpectedly sees k2 mapping")
	}

	idx2, ok := s2.StrMap.Index("k2")
	if !ok || idx2 == 0 {
		t.Fatalf("expected k2 in latest snapshot")
	}
	if got, ok := s1.StrMap.String(idx1); !ok || got != "k1" {
		t.Fatalf("old snapshot lost k1 mapping: got=%q ok=%v", got, ok)
	}
	if got, ok := s2.StrMap.String(idx2); !ok || got != "k2" {
		t.Fatalf("latest snapshot missing k2 mapping: got=%q ok=%v", got, ok)
	}
}

func TestSnapshotStrMap_LatestSnapshotRetainsOldMappingsAcrossChain(t *testing.T) {
	db, _ := openTempDBString(t)

	if err := db.Set("k1", &Rec{Name: "one"}); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	idx1, ok := db.engine.snapshot.Current().StrMap.Index("k1")
	if !ok || idx1 == 0 {
		t.Fatalf("expected k1 mapping in first snapshot")
	}

	for i := 2; i <= 12; i++ {
		key := fmt.Sprintf("k%d", i)
		if err := db.Set(key, &Rec{Name: key}); err != nil {
			t.Fatalf("Set(%s): %v", key, err)
		}
	}

	latest := db.engine.snapshot.Current().StrMap
	if got, ok := latest.Index("k1"); !ok || got != idx1 {
		t.Fatalf("latest snapshot lost k1 idx: got=%d ok=%v want=%d", got, ok, idx1)
	}
	if got, ok := latest.String(idx1); !ok || got != "k1" {
		t.Fatalf("latest snapshot lost k1 reverse mapping: got=%q ok=%v", got, ok)
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

	s := db.engine.snapshot.Current()
	if snapshotTestLenZeroComplementField(s, "tags") {
		t.Fatalf("unexpected zero-complement mode for balanced empty/non-empty tags")
	}
	if !snapshotExtLenContainsID(t, s, "tags", 0, 1) || !snapshotExtLenContainsID(t, s, "tags", 0, 2) {
		t.Fatalf("expected len=0 bucket to contain ids 1 and 2")
	}
	if !snapshotExtLenContainsID(t, s, "tags", 1, 3) {
		t.Fatalf("expected len=1 bucket to contain id 3")
	}
	if !snapshotExtLenContainsID(t, s, "tags", 2, 4) {
		t.Fatalf("expected len=2 bucket to contain id 4")
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

func TestSnapshot_UnpinPrunesRegistryWhenLastReaderExits(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "seed", Age: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	oldSeq := db.engine.snapshot.Current().Seq

	if _, ref, ok := db.engine.snapshot.PinBySeq(oldSeq); !ok {
		t.Fatalf("expected to pin previous snapshot")
	} else {
		if err := db.Set(2, &Rec{Name: "next", Age: 20}); err != nil {
			t.Fatalf("second Set: %v", err)
		}

		current := db.engine.snapshot.Current()
		latestSeq := current.Seq
		registryWhilePinned := db.engine.snapshot.Stats(current, nil).RegistrySize
		if registryWhilePinned < 2 {
			t.Fatalf("expected pinned snapshot to keep registry entry alive before unpin")
		}

		db.engine.snapshot.Unpin(oldSeq, ref)

		current = db.engine.snapshot.Current()
		if current == nil || current.Seq != latestSeq {
			t.Fatalf("expected latest snapshot to remain current after prune")
		}
		registryAfterUnpin := db.engine.snapshot.Stats(current, nil).RegistrySize

		if registryAfterUnpin != 1 {
			t.Fatalf("expected registry to retain only latest snapshot after idle unpin, got=%d", registryAfterUnpin)
		}
	}
}

func TestSnapshotFromEmptyBase_PublishedUniverseGetsOwnerAndPinnedScanStaysStable(t *testing.T) {
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

	old := db.engine.snapshot.Current()
	pinned, ref, ok := db.engine.snapshot.PinBySeq(old.Seq)
	if !ok || pinned != old {
		t.Fatal("expected seeded snapshot to be pinnable")
	}
	defer db.engine.snapshot.Unpin(old.Seq, ref)

	expect := slices.Clone(keys)
	checkPinned := func() {
		t.Helper()
		iter := pinned.Universe.Iter()
		defer iter.Release()

		got := make([]string, 0, len(expect))
		if err := db.scanStringKeys(pinned.StrMap, pinned.Universe, iter, "", func(id string) (bool, error) {
			got = append(got, id)
			return true, nil
		}); err != nil {
			t.Fatalf("scanStringKeys(pinned): %v", err)
		}
		if !slices.Equal(got, expect) {
			t.Fatalf("pinned scan mismatch: got=%v want=%v", got, expect)
		}
	}

	checkPinned()

	for i := 0; i < 32; i++ {
		key := keys[i%len(keys)]
		if err := db.Patch(key, []Field{
			{Name: "active", Value: i%2 == 0},
			{Name: "country", Value: "DE"},
			{Name: "name", Value: fmt.Sprintf("patched-%03d", i)},
		}); err != nil {
			t.Fatalf("Patch(%s): %v", key, err)
		}
		checkPinned()
	}
}

/**/

func snapshotExtOptions() Options {
	return Options{
		AutoBatchMax:    1,
		AnalyzeInterval: -1,
	}
}

func snapshotExtPosting(ids ...uint64) posting.List {
	var out posting.List
	for _, id := range ids {
		out = out.BuildAdded(id)
	}
	return out
}

func snapshotExtFieldContainsID(tb testing.TB, s *snapshot.View, field, key string, id uint64) bool {
	tb.Helper()
	return s.FieldLookupPostingRetained(field, key).Contains(id)
}

func snapshotExtNilContainsID(tb testing.TB, s *snapshot.View, field string, id uint64) bool {
	tb.Helper()
	acc := s.IndexedFieldByName[field]
	return indexdata.NewFieldIndexViewFromStorage(s.NilIndex[acc.Ordinal]).LookupPostingRetained(nilIndexEntryKey).Contains(id)
}

func snapshotExtLenContainsID(tb testing.TB, s *snapshot.View, field string, ln uint64, id uint64) bool {
	tb.Helper()
	acc := s.IndexedFieldByName[field]
	return indexdata.NewFieldIndexViewFromStorage(s.LenIndex[acc.Ordinal]).LookupPostingRetained(keycodec.U64ByteString(ln)).Contains(id)
}

func snapshotExtQueryTxRecords[K ~string | ~uint64, V any](t *testing.T, db *DB[K, V], tx *bbolt.Tx, snap *snapshot.View, q *qx.QX) []*V {
	t.Helper()

	prepared, viewQ, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery(queryRecords): %v", err)
	}
	defer prepared.Release()

	items, err := db.queryRecords(tx, snap, &viewQ)
	if err != nil {
		t.Fatalf("queryRecords: %v", err)
	}
	return items
}

func TestSnapshotExt_SnapshotStatsTracksPinnedRefAcrossPublish(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	oldSeq := db.engine.snapshot.Current().Seq
	if _, ref, ok := db.engine.snapshot.PinBySeq(oldSeq); !ok {
		t.Fatalf("expected current snapshot to be pinnable")
	} else {
		before := db.SnapshotStats()
		if before.Sequence != oldSeq || before.RegistrySize != 1 || before.PinnedRefs != 1 {
			t.Fatalf("unexpected stats before publish: %+v", before)
		}

		if err := db.Set(2, &Rec{Name: "bob", Age: 20}); err != nil {
			t.Fatalf("Set(2): %v", err)
		}

		mid := db.SnapshotStats()
		if mid.Sequence != db.engine.snapshot.Current().Seq || mid.Sequence <= oldSeq {
			t.Fatalf("expected stats sequence to advance after publish: %+v", mid)
		}
		if mid.RegistrySize != 2 || mid.PinnedRefs != 1 {
			t.Fatalf("unexpected stats with pinned retired snapshot: %+v", mid)
		}

		db.engine.snapshot.Unpin(oldSeq, ref)

		after := db.SnapshotStats()
		if after.RegistrySize != 1 || after.PinnedRefs != 0 {
			t.Fatalf("unexpected stats after last unpin: %+v", after)
		}
	}
}

func TestSnapshotExt_BeginQueryTxSnapshotIgnoresStagedFutureSnapshot(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	current := db.engine.snapshot.Current()
	staged := &snapshot.View{Seq: current.Seq + 1}
	db.engine.snapshot.Stage(staged)
	defer db.engine.snapshot.DropStaged(staged.Seq)

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}
	defer rollback(tx)
	defer db.engine.snapshot.Unpin(seq, ref)

	if seq != current.Seq {
		t.Fatalf("query tx pinned wrong sequence: got=%d want=%d", seq, current.Seq)
	}
	if snap != current {
		t.Fatalf("query tx pinned staged future snapshot instead of current snapshot")
	}
}

func TestSnapshotExt_BeginQueryTxSnapshotSeesOldStateWhileNewSnapshotPublished(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		done <- db.Set(2, &Rec{Name: "bob", Age: 20})
	}()

	oldItems := snapshotExtQueryTxRecords(t, db, tx, snap, qx.Query(qx.EQ("name", "bob")))
	if len(oldItems) != 0 {
		t.Fatalf("old tx/snapshot unexpectedly sees future write: %#v", oldItems)
	}

	oldItems = snapshotExtQueryTxRecords(t, db, tx, snap, qx.Query(qx.EQ("name", "alice")))
	if len(oldItems) != 1 || oldItems[0] == nil || oldItems[0].Name != "alice" {
		t.Fatalf("unexpected old snapshot query result: %#v", oldItems)
	}

	rollback(tx)
	db.engine.snapshot.Unpin(seq, ref)

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

func TestSnapshotExt_OldSnapshotFieldLookupImmutableAcrossSetUpdate(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "bob", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	s1 := db.engine.snapshot.Current()
	pinnedS1, ref, ok := db.engine.snapshot.PinBySeq(s1.Seq)
	if !ok || pinnedS1 != s1 {
		t.Fatalf("expected old snapshot to be pinnable")
	}
	defer db.engine.snapshot.Unpin(s1.Seq, ref)

	if err := db.Set(2, &Rec{Name: "charlie", Age: 20}); err != nil {
		t.Fatalf("Set(update): %v", err)
	}

	s2 := db.engine.snapshot.Current()
	if !snapshotExtFieldContainsID(t, pinnedS1, "name", "bob", 2) {
		t.Fatalf("old snapshot lost original scalar posting")
	}
	if snapshotExtFieldContainsID(t, pinnedS1, "name", "charlie", 2) {
		t.Fatalf("old snapshot unexpectedly sees updated scalar posting")
	}
	if !snapshotExtFieldContainsID(t, s2, "name", "charlie", 2) {
		t.Fatalf("new snapshot is missing updated scalar posting")
	}
	if snapshotExtFieldContainsID(t, s2, "name", "bob", 2) {
		t.Fatalf("new snapshot kept stale scalar posting")
	}
}

func TestSnapshotExt_OldSnapshotLenIndexImmutableAcrossSetUpdate(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Tags: []string{"go", "db"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	s1 := db.engine.snapshot.Current()
	pinnedS1, ref, ok := db.engine.snapshot.PinBySeq(s1.Seq)
	if !ok || pinnedS1 != s1 {
		t.Fatalf("expected old snapshot to be pinnable")
	}
	defer db.engine.snapshot.Unpin(s1.Seq, ref)

	if err := db.Set(1, &Rec{Name: "alice", Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set(update): %v", err)
	}

	s2 := db.engine.snapshot.Current()
	if !snapshotExtLenContainsID(t, pinnedS1, "tags", 2, 1) {
		t.Fatalf("old snapshot lost original len posting")
	}
	if snapshotExtLenContainsID(t, pinnedS1, "tags", 1, 1) {
		t.Fatalf("old snapshot unexpectedly sees updated len posting")
	}
	if !snapshotExtLenContainsID(t, s2, "tags", 1, 1) {
		t.Fatalf("new snapshot is missing updated len posting")
	}
	if snapshotExtLenContainsID(t, s2, "tags", 2, 1) {
		t.Fatalf("new snapshot kept stale len posting")
	}
}

func TestSnapshotExt_ZeroComplementLenIndexImmutableAcrossPatchToEmpty(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	ids := []uint64{1, 2, 3, 4}
	vals := []*Rec{
		{Name: "u1", Tags: []string{"go"}},
		{Name: "u2"},
		{Name: "u3"},
		{Name: "u4"},
	}
	if err := db.BatchSet(ids, vals); err != nil {
		t.Fatalf("BatchSet: %v", err)
	}

	s1 := db.engine.snapshot.Current()
	pinnedS1, ref, ok := db.engine.snapshot.PinBySeq(s1.Seq)
	if !ok || pinnedS1 != s1 {
		t.Fatalf("expected old snapshot to be pinnable")
	}
	defer db.engine.snapshot.Unpin(s1.Seq, ref)
	if !snapshotTestLenZeroComplementField(pinnedS1, "tags") {
		t.Fatalf("expected zero-complement len index for sparse non-empty tags")
	}
	if !snapshotExtLenContainsID(t, pinnedS1, "tags", 1, 1) {
		t.Fatalf("expected old snapshot len=1 posting to include id=1")
	}

	if err := db.Patch(1, []Field{{Name: "tags", Value: []string{}}}); err != nil {
		t.Fatalf("Patch(tags): %v", err)
	}

	s2 := db.engine.snapshot.Current()
	if !snapshotExtLenContainsID(t, pinnedS1, "tags", 1, 1) {
		t.Fatalf("old snapshot lost len=1 posting")
	}
	if snapshotExtLenContainsID(t, s2, "tags", 1, 1) {
		t.Fatalf("new snapshot kept stale len=1 posting")
	}
	got, err := db.QueryKeys(qx.Query(qx.EQ("tags", []string{})))
	if err != nil {
		t.Fatalf("QueryKeys(empty tags): %v", err)
	}
	want := []uint64{1, 2, 3, 4}
	if !slices.Equal(got, want) {
		t.Fatalf("empty-tags query mismatch: got=%v want=%v", got, want)
	}
}

func TestSnapshotExt_OldSnapshotNilIndexImmutableAcrossPatch(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Email: "alice@example.com", Opt: nil}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	s1 := db.engine.snapshot.Current()
	pinnedS1, ref, ok := db.engine.snapshot.PinBySeq(s1.Seq)
	if !ok || pinnedS1 != s1 {
		t.Fatalf("expected old snapshot to be pinnable")
	}
	defer db.engine.snapshot.Unpin(s1.Seq, ref)
	val := "zzz"
	if err := db.Patch(1, []Field{{Name: "opt", Value: &val}}); err != nil {
		t.Fatalf("Patch(opt): %v", err)
	}

	s2 := db.engine.snapshot.Current()
	if !snapshotExtNilContainsID(t, pinnedS1, "opt", 1) {
		t.Fatalf("old snapshot lost nil-index membership")
	}
	if snapshotExtNilContainsID(t, s2, "opt", 1) {
		t.Fatalf("new snapshot kept stale nil-index membership")
	}
	if !snapshotExtFieldContainsID(t, s2, "opt", val, 1) {
		t.Fatalf("new snapshot is missing updated opt posting")
	}
}

func TestSnapshotExt_OldSnapshotUniverseImmutableAcrossDelete(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "bob", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	s1 := db.engine.snapshot.Current()
	pinnedS1, ref, ok := db.engine.snapshot.PinBySeq(s1.Seq)
	if !ok || pinnedS1 != s1 {
		t.Fatalf("expected old snapshot to be pinnable")
	}
	defer db.engine.snapshot.Unpin(s1.Seq, ref)
	if err := db.Delete(2); err != nil {
		t.Fatalf("Delete(2): %v", err)
	}

	s2 := db.engine.snapshot.Current()
	if !pinnedS1.Universe.Contains(2) || pinnedS1.Universe.Cardinality() != 2 {
		t.Fatalf("old snapshot universe mutated after delete")
	}
	if s2.Universe.Contains(2) || s2.Universe.Cardinality() != 1 {
		t.Fatalf("new snapshot universe is inconsistent after delete")
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

	old := db.engine.snapshot.Current()
	oldSeq := old.Seq
	pinned, ref, ok := db.engine.snapshot.PinBySeq(oldSeq)
	if !ok || pinned != old {
		t.Fatalf("expected current snapshot to be pinnable before truncate")
	}

	if err := db.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	current := db.engine.snapshot.Current()
	if current.Seq <= oldSeq {
		t.Fatalf("expected truncate to publish newer snapshot: old=%d new=%d", oldSeq, current.Seq)
	}
	if !current.Universe.IsEmpty() {
		t.Fatalf("expected truncate snapshot universe to be empty")
	}
	if pinned.Universe.Cardinality() != 2 {
		t.Fatalf("pinned pre-truncate snapshot lost universe")
	}
	if !snapshotExtFieldContainsID(t, pinned, "name", "alice", 1) {
		t.Fatalf("pinned pre-truncate snapshot lost field postings")
	}

	keys, err := db.QueryKeys(qx.Query(qx.EQ("name", "alice")))
	if err != nil {
		t.Fatalf("QueryKeys(after truncate): %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("expected current snapshot to be empty after truncate, got=%v", keys)
	}

	db.engine.snapshot.Unpin(oldSeq, ref)

	registrySize := db.engine.snapshot.Stats(db.engine.snapshot.Current(), nil).RegistrySize
	if registrySize != 1 {
		t.Fatalf("expected registry to retain only latest snapshot after truncate prune, got=%d", registrySize)
	}
}

func TestSnapshotExt_StringKeyPinnedSnapshotStrMapSurvivesTruncate(t *testing.T) {
	db, _ := openTempDBString(t, snapshotExtOptions())

	if err := db.Set("k1", &Rec{Name: "one"}); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	if err := db.Set("k2", &Rec{Name: "two"}); err != nil {
		t.Fatalf("Set(k2): %v", err)
	}

	old := db.engine.snapshot.Current()
	idx1, ok := old.StrMap.Index("k1")
	if !ok || idx1 == 0 {
		t.Fatalf("expected k1 mapping before truncate")
	}
	pinned, ref, ok := db.engine.snapshot.PinBySeq(old.Seq)
	if !ok || pinned != old {
		t.Fatalf("expected pre-truncate snapshot to be pinnable")
	}

	if err := db.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	current := db.engine.snapshot.Current()
	if _, ok := current.StrMap.Index("k1"); ok {
		t.Fatalf("current truncated snapshot unexpectedly retained k1 mapping")
	}
	if got, ok := pinned.StrMap.String(idx1); !ok || got != "k1" {
		t.Fatalf("pinned pre-truncate snapshot lost k1 mapping: got=%q ok=%v", got, ok)
	}

	db.engine.snapshot.Unpin(old.Seq, ref)
}

func TestSnapshotExt_StringKeyOldSnapshotMappingsSurviveDeleteAndNewKeys(t *testing.T) {
	db, _ := openTempDBString(t, snapshotExtOptions())

	if err := db.Set("k1", &Rec{Name: "one"}); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	if err := db.Set("k2", &Rec{Name: "two"}); err != nil {
		t.Fatalf("Set(k2): %v", err)
	}

	s1 := db.engine.snapshot.Current()
	idx1, ok := s1.StrMap.Index("k1")
	if !ok || idx1 == 0 {
		t.Fatalf("expected k1 mapping in old snapshot")
	}

	if err := db.Delete("k1"); err != nil {
		t.Fatalf("Delete(k1): %v", err)
	}
	for i := 3; i <= 10; i++ {
		key := fmt.Sprintf("k%d", i)
		if err := db.Set(key, &Rec{Name: key}); err != nil {
			t.Fatalf("Set(%s): %v", key, err)
		}
	}

	s2 := db.engine.snapshot.Current()
	if _, ok := s1.StrMap.Index("k3"); ok {
		t.Fatalf("old snapshot unexpectedly sees future string key mapping")
	}
	if got, ok := s1.StrMap.String(idx1); !ok || got != "k1" {
		t.Fatalf("old snapshot lost original reverse mapping: got=%q ok=%v", got, ok)
	}
	if _, ok := s2.StrMap.Index("k3"); !ok {
		t.Fatalf("new snapshot is missing future string key mapping")
	}
}

/**/

func TestSpanshotExt_ScanKeysStringShouldStayOnSingleSnapshotAcrossTruncate(t *testing.T) {
	db, _ := openTempDBString(t, snapshotExtOptions())

	if err := db.Set("k1", &Rec{Name: "one"}); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	if err := db.Set("k2", &Rec{Name: "two"}); err != nil {
		t.Fatalf("Set(k2): %v", err)
	}

	old := db.engine.snapshot.Current()
	if old == nil || old.StrMap == nil {
		t.Fatalf("expected published string-key snapshot")
	}
	pinned, ref, ok := db.engine.snapshot.PinBySeq(old.Seq)
	if !ok || pinned != old {
		t.Fatalf("expected current string snapshot to be pinnable")
	}
	defer db.engine.snapshot.Unpin(old.Seq, ref)

	var want []string
	iterOld := pinned.Universe.Iter()
	defer iterOld.Release()
	for iterOld.HasNext() {
		idx := iterOld.Next()
		key, ok := pinned.StrMap.String(idx)
		if !ok {
			t.Fatalf("old snapshot lost key mapping for idx=%d", idx)
		}
		want = append(want, key)
	}
	if !slices.Equal(want, []string{"k1", "k2"}) {
		t.Fatalf("unexpected old snapshot key order: %v", want)
	}

	universe := pinned.Universe
	iter := universe.Iter()
	defer iter.Release()

	if err := db.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	var got []string
	err := db.scanStringKeys(pinned.StrMap, universe, iter, "", func(id string) (bool, error) {
		got = append(got, id)
		return true, nil
	})
	if err != nil {
		t.Fatalf("string ScanKeys split snapshot across truncate: err=%v want=%v got=%v", err, want, got)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("string ScanKeys mixed snapshots: got=%v want=%v", got, want)
	}
}

func TestSpanshotExt_ScanKeysStringSplitSnapshotMustNotEmitFutureKeysAfterTruncateReuse(t *testing.T) {
	db, _ := openTempDBString(t, snapshotExtOptions())

	if err := db.Set("k1", &Rec{Name: "one"}); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	if err := db.Set("k2", &Rec{Name: "two"}); err != nil {
		t.Fatalf("Set(k2): %v", err)
	}

	old := db.engine.snapshot.Current()
	pinned, ref, ok := db.engine.snapshot.PinBySeq(old.Seq)
	if !ok || pinned != old {
		t.Fatalf("expected current string snapshot to be pinnable")
	}
	defer db.engine.snapshot.Unpin(old.Seq, ref)
	universe := pinned.Universe
	iter := universe.Iter()
	defer iter.Release()

	if err := db.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if err := db.Set("future", &Rec{Name: "future"}); err != nil {
		t.Fatalf("Set(future): %v", err)
	}

	var got []string
	err := db.scanStringKeys(pinned.StrMap, universe, iter, "", func(id string) (bool, error) {
		got = append(got, id)
		return true, nil
	})
	if err != nil {
		t.Fatalf("split snapshot emitted inconsistent string scan state: err=%v got=%v oldSeq=%d currentSeq=%d", err, got, old.Seq, db.engine.snapshot.Current().Seq)
	}
	if slices.Contains(got, "future") {
		t.Fatalf("string scan leaked future key from newer snapshot: got=%v", got)
	}
	if !slices.Equal(got, []string{"k1", "k2"}) {
		t.Fatalf("unexpected string scan result from old snapshot: got=%v", got)
	}
}

func TestSpanshotExt_BeginQueryTxSnapshotSurvivesConcurrentTruncate(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "bob", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	before := db.engine.snapshot.Current()
	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- db.Truncate()
	}()

	alice := snapshotExtQueryTxRecords(t, db, tx, snap, qx.Query(qx.EQ("name", "alice")))
	if len(alice) != 1 || alice[0] == nil || alice[0].Name != "alice" {
		t.Fatalf("unexpected old snapshot alice result: %#v", alice)
	}

	all := snapshotExtQueryTxRecords(t, db, tx, snap, qx.Query())
	if len(all) != 2 {
		t.Fatalf("old tx/snapshot lost rows across truncate: %#v", all)
	}

	rollback(tx)
	db.engine.snapshot.Unpin(seq, ref)

	if err := <-done; err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	after := db.engine.snapshot.Current()
	if after.Seq <= before.Seq {
		t.Fatalf("truncate did not publish newer snapshot: before=%d after=%d", before.Seq, after.Seq)
	}

	current, err := db.Query(qx.Query())
	if err != nil {
		t.Fatalf("Query(current after truncate): %v", err)
	}
	if len(current) != 0 {
		t.Fatalf("current snapshot should be empty after truncate, got=%#v", current)
	}
}

func TestSpanshotExt_BeginQueryTxSnapshotSurvivesBrokenTruncatePublish(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "bob", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	before := db.engine.snapshot.Current()
	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}

	db.testHooks = &testHooks{
		afterCommitPublish: func(op string) {
			if op == "truncate" {
				panic("failpoint: publish truncate")
			}
		},
	}
	t.Cleanup(func() {
		db.testHooks = nil
	})

	done := make(chan error, 1)
	go func() {
		done <- db.Truncate()
	}()

	all := snapshotExtQueryTxRecords(t, db, tx, snap, qx.Query())
	if len(all) != 2 {
		t.Fatalf("old tx/snapshot lost rows during broken truncate publish: %#v", all)
	}

	rollback(tx)
	db.engine.snapshot.Unpin(seq, ref)

	err = <-done
	if !errors.Is(err, ErrBroken) {
		t.Fatalf("expected ErrBroken from truncate publish failpoint, got: %v", err)
	}

	assertNoFutureSnapshotRefs(t, db)
	if got := db.engine.snapshot.Current(); got != before {
		t.Fatalf("broken truncate publish should keep previously published snapshot current")
	}
}

func assertPostingConsumerSet(t *testing.T, got posting.List, want []uint64) {
	t.Helper()
	gotIDs := got.ToArray()
	if !slices.Equal(gotIDs, want) {
		t.Fatalf("posting mismatch: got=%v want=%v", gotIDs, want)
	}
}

/**/

func TestSnapshotExt_PinCurrentViewTracksReaderPinsAcrossPublish(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	current := db.engine.snapshot.Current()
	snap, seq, ref := db.engine.snapshot.PinCurrent()
	if snap != current {
		t.Fatalf("expected current snapshot pin")
	}
	if seq != current.Seq || ref == nil {
		t.Fatalf("expected current snapshot ref pin")
	}
	if stats := db.engine.snapshot.Stats(snap, nil); stats.PinnedRefs != 1 {
		t.Fatalf("expected one current snapshot ref, stats=%+v", stats)
	}

	if err := db.Set(2, &Rec{Name: "bob", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	if stats := db.engine.snapshot.Stats(db.engine.snapshot.Current(), nil); stats.RegistrySize != 2 || stats.PinnedRefs != 1 {
		t.Fatalf("expected old snapshot ref pin to survive publish, stats=%+v", stats)
	}

	db.engine.snapshot.Unpin(seq, ref)
	if stats := db.engine.snapshot.Stats(db.engine.snapshot.Current(), nil); stats.RegistrySize != 1 || stats.PinnedRefs != 0 {
		t.Fatalf("expected current snapshot ref pins to drop after unpin, stats=%+v", stats)
	}
}

func TestSnapshotExt_QueryKeysReleasesCurrentSnapshotPinOnError(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	snap := db.engine.snapshot.Current()
	if _, err := db.QueryKeys(qx.Query(qx.EQ("does_not_exist", 1))); err == nil {
		t.Fatalf("expected QueryKeys to fail for unknown field")
	}
	stats := db.engine.snapshot.Stats(snap, nil)
	if stats.Sequence != snap.Seq || stats.RegistrySize != 1 {
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

	snap := db.engine.snapshot.Current()
	if _, err := db.Count(qx.EQ("does_not_exist", 1)); err == nil {
		t.Fatalf("expected Count to fail for unknown field")
	}
	stats := db.engine.snapshot.Stats(snap, nil)
	if stats.Sequence != snap.Seq || stats.RegistrySize != 1 {
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

	snap := db.engine.snapshot.Current()
	wantErr := errors.New("stop")
	err := db.ScanKeys(0, func(uint64) (bool, error) {
		return false, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("ScanKeys err=%v want %v", err, wantErr)
	}
	stats := db.engine.snapshot.Stats(snap, nil)
	if stats.Sequence != snap.Seq || stats.RegistrySize != 1 {
		t.Fatalf("expected ScanKeys snapshot ref to remain registered")
	}
	if stats.PinnedRefs != 0 {
		t.Fatalf("expected ScanKeys pin release on callback error, stats=%+v", stats)
	}
}

func snapshotTestIndexedFieldByName(
	index map[string]indexdata.FieldStorage,
	nilIndex map[string]indexdata.FieldStorage,
	lenIndex map[string]indexdata.FieldStorage,
	lenZeroComplement map[string]bool,
) map[string]schema.IndexedFieldAccessor {
	keys := make([]string, 0, len(index)+len(nilIndex)+len(lenIndex)+len(lenZeroComplement))
	seen := make(map[string]struct{}, cap(keys))
	add := func(name string) {
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		keys = append(keys, name)
	}
	for name := range index {
		add(name)
	}
	for name := range nilIndex {
		add(name)
	}
	for name := range lenIndex {
		add(name)
	}
	for name := range lenZeroComplement {
		add(name)
	}
	slices.Sort(keys)
	out := make(map[string]schema.IndexedFieldAccessor, len(keys))
	for i, name := range keys {
		out[name] = schema.IndexedFieldAccessor{
			Ordinal: i,
			Name:    name,
			Field:   &schema.Field{Slice: lenIndex[name].KeyCount() > 0 || lenZeroComplement[name]},
		}
	}
	return out
}

func snapshotTestFieldIndexSlots(
	src map[string]indexdata.FieldStorage,
	byName map[string]schema.IndexedFieldAccessor,
) []indexdata.FieldStorage {
	out := indexdata.GetFieldStorageSlice(len(byName))[:len(byName)]
	for name, storage := range src {
		out[byName[name].Ordinal] = storage
	}
	return out
}

func snapshotTestBoolSlots(src map[string]bool, byName map[string]schema.IndexedFieldAccessor) []bool {
	out := pooled.GetBoolSlice(len(byName))[:len(byName)]
	clear(out)
	for name, value := range src {
		if value {
			out[byName[name].Ordinal] = true
		}
	}
	return out
}

func snapshotTestNewSnapshot(
	index map[string]indexdata.FieldStorage,
	nilIndex map[string]indexdata.FieldStorage,
	lenIndex map[string]indexdata.FieldStorage,
	lenZeroComplement map[string]bool,
) *snapshot.View {
	byName := snapshotTestIndexedFieldByName(index, nilIndex, lenIndex, lenZeroComplement)
	return &snapshot.View{
		Index:              snapshotTestFieldIndexSlots(index, byName),
		NilIndex:           snapshotTestFieldIndexSlots(nilIndex, byName),
		LenIndex:           snapshotTestFieldIndexSlots(lenIndex, byName),
		LenZeroComplement:  snapshotTestBoolSlots(lenZeroComplement, byName),
		IndexedFieldByName: byName,
	}
}

func snapshotTestLenZeroComplementField(s *snapshot.View, name string) bool {
	acc, ok := s.IndexedFieldByName[name]
	if !ok || acc.Ordinal >= len(s.LenZeroComplement) {
		return false
	}
	return s.LenZeroComplement[acc.Ordinal]
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

func snapshotExtraHasFutureSnapshot[K ~uint64 | ~string, V any](db *DB[K, V], currentSeq uint64) bool {
	current := db.engine.snapshot.Current()
	return current != nil && current.Seq == currentSeq && db.engine.snapshot.Stats(current, nil).RegistrySize > 1
}

func snapshotExtraAssertNoFutureSnapshotRefs[K ~uint64 | ~string, V any](tb testing.TB, db *DB[K, V]) {
	tb.Helper()

	current := db.engine.snapshot.Current()
	currentSeq := uint64(0)
	if current != nil {
		currentSeq = current.Seq
	}

	stats := db.engine.snapshot.Stats(current, nil)
	if stats.RegistrySize != 1 {
		tb.Fatalf("snapshot registry contains staged or retired refs: current=%d stats=%+v", currentSeq, stats)
	}
}

func snapshotExtraPosting(ids ...uint64) posting.List {
	var out posting.List
	for _, id := range ids {
		out = out.BuildAdded(id)
	}
	return out
}

func snapshotExtraQuerySnapshotKeys[K ~string | ~uint64](t *testing.T, db *DB[K, snapshotExtraRec], snap *snapshot.View, q *qx.QX) []K {
	t.Helper()

	prepared, viewQ, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery(snapshot query): %v", err)
	}
	defer prepared.Release()

	view := db.engine.exec.AcquireView(snap)
	ids, err := view.Query(&viewQ, false, false)
	db.engine.exec.ReleaseView(view)
	if err != nil {
		t.Fatalf("snapshot query: %v", err)
	}
	return db.queryKeysFromIDs(snap, ids)
}

func snapshotExtraScanSnapshotStringKeys(t *testing.T, db *DB[string, snapshotExtraRec], snap *snapshot.View, seek string) []string {
	t.Helper()

	if snap == nil {
		t.Fatal("snapshot is nil")
	}

	iter := snap.Universe.Iter()
	defer iter.Release()

	var out []string
	if err := db.scanStringKeys(snap.StrMap, snap.Universe, iter, seek, func(id string) (bool, error) {
		out = append(out, id)
		return true, nil
	}); err != nil {
		t.Fatalf("scanStringKeys: %v", err)
	}
	return out
}

func snapshotExtraMaterializedPredCacheKey(t *testing.T, db *DB[uint64, snapshotExtraRec], e qx.Expr) string {
	t.Helper()

	return db.engine.materializedPredCacheKey(e)
}

func snapshotExtraQueryTxRecords[K ~string | ~uint64](t *testing.T, db *DB[K, snapshotExtraRec], tx *bbolt.Tx, snap *snapshot.View, q *qx.QX) []*snapshotExtraRec {
	t.Helper()

	prepared, viewQ, err := prepareTestQuery(db.engine, q)
	if err != nil {
		t.Fatalf("prepareTestQuery(queryRecords): %v", err)
	}
	defer prepared.Release()

	items, err := db.queryRecords(tx, snap, &viewQ)
	if err != nil {
		t.Fatalf("queryRecords: %v", err)
	}
	return items
}

func snapshotExtraRequireNumericRangeBucketCacheEntry(t *testing.T, snap *snapshot.View, field string) *qcache.NumericRangeBucketEntry {
	t.Helper()
	if snap == nil || snap.NumericRangeBucketCache() == nil {
		t.Fatalf("expected non-nil numeric range bucket cache for field %q", field)
	}
	e, ok := snap.NumericRangeBucketCache().LoadField(field)
	if !ok {
		t.Fatalf("expected numeric range bucket cache entry for field %q", field)
	}
	if e == nil || e.Index().BucketSize() <= 0 {
		t.Fatalf("expected non-nil numeric range bucket index for field %q", field)
	}
	return e
}

func snapshotExtraSetNumericBucketKnobs(t *testing.T, db *DB[uint64, snapshotExtraRec], size, minFieldKeys, minSpan int) {
	t.Helper()

	prevSize := db.options.NumericRangeBucketSize
	prevMinField := db.options.NumericRangeBucketMinFieldKeys
	prevMinSpan := db.options.NumericRangeBucketMinSpanKeys
	prevEngineSize := db.engine.exec.NumericRangeBucketSize
	prevEngineMinField := db.engine.exec.NumericRangeBucketMinFieldKeys
	prevEngineMinSpan := db.engine.exec.NumericRangeBucketMinSpanKeys

	db.options.NumericRangeBucketSize = size
	db.options.NumericRangeBucketMinFieldKeys = minFieldKeys
	db.options.NumericRangeBucketMinSpanKeys = minSpan
	db.engine.exec.NumericRangeBucketSize = size
	db.engine.exec.NumericRangeBucketMinFieldKeys = minFieldKeys
	db.engine.exec.NumericRangeBucketMinSpanKeys = minSpan

	t.Cleanup(func() {
		db.options.NumericRangeBucketSize = prevSize
		db.options.NumericRangeBucketMinFieldKeys = prevMinField
		db.options.NumericRangeBucketMinSpanKeys = prevMinSpan
		db.engine.exec.NumericRangeBucketSize = prevEngineSize
		db.engine.exec.NumericRangeBucketMinFieldKeys = prevEngineMinField
		db.engine.exec.NumericRangeBucketMinSpanKeys = prevEngineMinSpan
	})
}

func snapshotExtraLiveStrMapLookup(t *testing.T, db *DB[string, snapshotExtraRec], key string) (uint64, bool) {
	t.Helper()
	return db.strMap.Snapshot().Index(key)
}

func snapshotExtraLiveStrMapNext(t *testing.T, db *DB[string, snapshotExtraRec]) uint64 {
	t.Helper()
	return db.strMap.Snapshot().Next()
}

func TestSnapshotExtra_BeginQueryTxSnapshotIgnoresRealStagedSetBeforeCommit(t *testing.T) {
	db, _ := snapshotExtraOpenTempDBUint64(t, snapshotExtraOptions())

	if err := db.Set(1, &snapshotExtraRec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &snapshotExtraRec{Name: "bob", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	old := db.engine.snapshot.Current()
	pinned, holdRef, ok := db.engine.snapshot.PinBySeq(old.Seq)
	if !ok || pinned != old {
		t.Fatalf("expected current snapshot to be pinnable")
	}
	defer db.engine.snapshot.Unpin(old.Seq, holdRef)

	entered := make(chan struct{})
	release := make(chan struct{})
	db.testHooks = &testHooks{
		beforeCommit: func(op string) error {
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
		},
	}
	t.Cleanup(func() {
		db.testHooks = nil
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
	if !snapshotExtraHasFutureSnapshot(db, old.Seq) {
		t.Fatalf("expected staged future snapshot while commit is blocked")
	}
	if got := db.engine.snapshot.Current(); got != old {
		t.Fatalf("current snapshot changed before commit publish")
	}

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}
	if seq != old.Seq || snap != old {
		t.Fatalf("beginQueryTxSnapshot pinned wrong snapshot: seq=%d want=%d", seq, old.Seq)
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
	db.engine.snapshot.Unpin(seq, ref)

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

	db.engine.snapshot.Unpin(old.Seq, holdRef)
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

	old := db.engine.snapshot.Current()
	pinned, holdRef, ok := db.engine.snapshot.PinBySeq(old.Seq)
	if !ok || pinned != old {
		t.Fatalf("expected current snapshot to be pinnable")
	}
	defer db.engine.snapshot.Unpin(old.Seq, holdRef)

	entered := make(chan struct{})
	release := make(chan struct{})
	db.testHooks = &testHooks{
		beforeCommit: func(op string) error {
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
		},
	}
	t.Cleanup(func() {
		db.testHooks = nil
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
	if !snapshotExtraHasFutureSnapshot(db, old.Seq) {
		t.Fatalf("expected staged future snapshot while commit is blocked")
	}

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}
	if seq != old.Seq || snap != old {
		t.Fatalf("beginQueryTxSnapshot pinned wrong snapshot: seq=%d want=%d", seq, old.Seq)
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
	db.engine.snapshot.Unpin(seq, ref)

	close(release)
	err = <-done
	if err == nil || !strings.Contains(err.Error(), "failpoint: rollback set") {
		t.Fatalf("expected rollback failpoint error, got: %v", err)
	}

	if got := db.engine.snapshot.Current(); got != old {
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

	db.engine.snapshot.Unpin(old.Seq, holdRef)
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

	old := db.engine.snapshot.Current()
	pinned, holdRef, ok := db.engine.snapshot.PinBySeq(old.Seq)
	if !ok || pinned != old {
		t.Fatalf("expected current snapshot to be pinnable")
	}
	defer db.engine.snapshot.Unpin(old.Seq, holdRef)

	entered := make(chan struct{})
	release := make(chan struct{})
	db.testHooks = &testHooks{
		beforeCommit: func(op string) error {
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
		},
	}
	t.Cleanup(func() {
		db.testHooks = nil
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
	if !snapshotExtraHasFutureSnapshot(db, old.Seq) {
		t.Fatalf("expected staged future snapshot while batch_set commit is blocked")
	}
	if got := db.engine.snapshot.Current(); got != old {
		t.Fatalf("current snapshot changed before batch_set publish")
	}

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}
	if seq != old.Seq || snap != old {
		t.Fatalf("beginQueryTxSnapshot pinned wrong snapshot: seq=%d want=%d", seq, old.Seq)
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
	db.engine.snapshot.Unpin(seq, ref)

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

	db.engine.snapshot.Unpin(old.Seq, holdRef)
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

	old := db.engine.snapshot.Current()
	q := qx.Query(qx.GTE("age", 1024))

	if _, err := db.QueryKeys(q); err != nil {
		t.Fatalf("QueryKeys(warm old range cache): %v", err)
	}
	snapshotExtraRequireNumericRangeBucketCacheEntry(t, old, "age")

	pinned, ref, ok := db.engine.snapshot.PinBySeq(old.Seq)
	if !ok || pinned != old {
		t.Fatalf("expected old snapshot to be pinnable")
	}
	defer db.engine.snapshot.Unpin(old.Seq, ref)

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

	old := db.engine.snapshot.Current()
	pinned, holdRef, ok := db.engine.snapshot.PinBySeq(old.Seq)
	if !ok || pinned != old {
		t.Fatalf("expected current string snapshot to be pinnable")
	}
	defer db.engine.snapshot.Unpin(old.Seq, holdRef)

	entered := make(chan struct{})
	release := make(chan struct{})
	db.testHooks = &testHooks{
		beforeCommit: func(op string) error {
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
		},
	}
	t.Cleanup(func() {
		db.testHooks = nil
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
	if !snapshotExtraHasFutureSnapshot(db, old.Seq) {
		t.Fatalf("expected staged future string snapshot while commit is blocked")
	}
	if got := db.engine.snapshot.Current(); got != old {
		t.Fatalf("current string snapshot changed before publish")
	}
	if _, ok := old.StrMap.Index("future"); ok {
		t.Fatalf("old published strmap observed staged future key before publish")
	}

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}
	if seq != old.Seq || snap != old {
		t.Fatalf("beginQueryTxSnapshot pinned wrong string snapshot: seq=%d want=%d", seq, old.Seq)
	}

	beforeFuture := snapshotExtraQueryTxRecords(t, db, tx, snap, qx.Query(qx.EQ("name", "future")))
	if len(beforeFuture) != 0 {
		t.Fatalf("old string tx/snapshot observed staged future row: %#v", beforeFuture)
	}

	rollback(tx)
	db.engine.snapshot.Unpin(seq, ref)

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Set(future): %v", err)
	}

	if _, ok := pinned.StrMap.Index("future"); ok {
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

	db.engine.snapshot.Unpin(old.Seq, holdRef)
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

	old := db.engine.snapshot.Current()
	pinned, holdRef, ok := db.engine.snapshot.PinBySeq(old.Seq)
	if !ok || pinned != old {
		t.Fatalf("expected current string snapshot to be pinnable")
	}
	defer db.engine.snapshot.Unpin(old.Seq, holdRef)

	entered := make(chan struct{})
	release := make(chan struct{})
	db.testHooks = &testHooks{
		beforeCommit: func(op string) error {
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
		},
	}
	t.Cleanup(func() {
		db.testHooks = nil
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

	if _, ok := old.StrMap.Index("ghost"); ok {
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
	if seq != old.Seq || snap != old {
		t.Fatalf("beginQueryTxSnapshot pinned wrong string snapshot: seq=%d want=%d", seq, old.Seq)
	}
	beforeGhost := snapshotExtraQueryTxRecords(t, db, tx, snap, qx.Query(qx.EQ("name", "ghost")))
	if len(beforeGhost) != 0 {
		t.Fatalf("old string tx/snapshot observed ghost row before rollback: %#v", beforeGhost)
	}
	rollback(tx)
	db.engine.snapshot.Unpin(seq, ref)

	close(release)
	err = <-done
	if err == nil || !strings.Contains(err.Error(), "failpoint: rollback string set") {
		t.Fatalf("expected rollback failpoint error, got: %v", err)
	}
	db.testHooks = nil

	if got := db.engine.snapshot.Current(); got != old {
		t.Fatalf("rollback replaced published string snapshot: got=%p want=%p", got, old)
	}
	if _, ok := snapshotExtraLiveStrMapLookup(t, db, "ghost"); ok {
		t.Fatalf("live strmap retained ghost key after rollback")
	}
	if next := snapshotExtraLiveStrMapNext(t, db); next != 2 {
		t.Fatalf("strmap.Next not restored after rollback: got=%d want=2", next)
	}
	if _, ok := pinned.StrMap.Index("ghost"); ok {
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

	current := db.engine.snapshot.Current()
	laterIdx, ok := current.StrMap.Index("later")
	if !ok {
		t.Fatalf("current snapshot missing later key after rollback recovery; liveIdx=%d seq=%d", liveLaterIdx, current.Seq)
	}
	if laterIdx != 3 {
		t.Fatalf("expected later key to reuse rolled-back idx=3, got=%d", laterIdx)
	}
	if _, ok := current.StrMap.Index("ghost"); ok {
		t.Fatalf("current snapshot retained ghost key after successful later publish")
	}

	db.engine.snapshot.Unpin(old.Seq, holdRef)
	snapshotExtraAssertNoFutureSnapshotRefs(t, db)
}

func TestSnapshotExtra_StringKeyBatchRollbackRemovesAllLiveCreatedIdxs(t *testing.T) {
	db, _ := snapshotExtraOpenTempDBString(t, snapshotExtraOptions())

	if err := db.Set("k1", &snapshotExtraRec{Name: "one", Age: 10}); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}

	old := db.engine.snapshot.Current()
	pinned, holdRef, ok := db.engine.snapshot.PinBySeq(old.Seq)
	if !ok || pinned != old {
		t.Fatalf("expected current string snapshot to be pinnable")
	}
	defer db.engine.snapshot.Unpin(old.Seq, holdRef)

	entered := make(chan struct{})
	release := make(chan struct{})
	db.testHooks = &testHooks{
		beforeCommit: func(op string) error {
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
		},
	}
	t.Cleanup(func() {
		db.testHooks = nil
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

	if _, ok := old.StrMap.Index("ghostA"); ok {
		t.Fatalf("published old snapshot observed ghostA before rollback")
	}
	if _, ok := old.StrMap.Index("ghostB"); ok {
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
	if seq != old.Seq || snap != old {
		t.Fatalf("beginQueryTxSnapshot pinned wrong string snapshot: seq=%d want=%d", seq, old.Seq)
	}
	beforeGhostA := snapshotExtraQueryTxRecords(t, db, tx, snap, qx.Query(qx.EQ("name", "ghostA")))
	if len(beforeGhostA) != 0 {
		t.Fatalf("old string tx/snapshot observed ghostA row before rollback: %#v", beforeGhostA)
	}
	beforeGhostB := snapshotExtraQueryTxRecords(t, db, tx, snap, qx.Query(qx.EQ("name", "ghostB")))
	if len(beforeGhostB) != 0 {
		t.Fatalf("old string tx/snapshot observed ghostB row before rollback: %#v", beforeGhostB)
	}
	rollback(tx)
	db.engine.snapshot.Unpin(seq, ref)

	close(release)
	err = <-done
	if err == nil || !strings.Contains(err.Error(), "failpoint: rollback string batch_set") {
		t.Fatalf("expected rollback failpoint error, got: %v", err)
	}
	db.testHooks = nil

	if got := db.engine.snapshot.Current(); got != old {
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
	if _, ok := pinned.StrMap.Index("ghostA"); ok {
		t.Fatalf("pinned old string snapshot observed ghostA after rollback")
	}
	if _, ok := pinned.StrMap.Index("ghostB"); ok {
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
	queryReal, err := db.QueryKeys(qx.Query().Sort("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(real recovery): %v", err)
	}
	if !slices.Equal(queryReal, []string{"k1", "realA", "realB"}) {
		t.Fatalf("query path drift after batch rollback recovery: got=%v want=[k1 realA realB]", queryReal)
	}

	current := db.engine.snapshot.Current()
	realAIdx, ok := current.StrMap.Index("realA")
	if !ok {
		t.Fatalf("current snapshot missing realA after rollback recovery; liveRealA=%d liveRealB=%d seq=%d", liveRealAIdx, liveRealBIdx, current.Seq)
	}
	realBIdx, ok := current.StrMap.Index("realB")
	if !ok {
		t.Fatalf("current snapshot missing realB after rollback recovery")
	}
	if realAIdx != 2 || realBIdx != 3 {
		t.Fatalf("expected real keys to reuse rolled-back idxs 2,3; got realA=%d realB=%d", realAIdx, realBIdx)
	}
	if _, ok := current.StrMap.Index("ghostA"); ok {
		t.Fatalf("current snapshot retained ghostA after successful recovery publish")
	}
	if _, ok := current.StrMap.Index("ghostB"); ok {
		t.Fatalf("current snapshot retained ghostB after successful recovery publish")
	}

	db.engine.snapshot.Unpin(old.Seq, holdRef)
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

	old := db.engine.snapshot.Current()
	posExpr := qx.PREFIX("email", "alpha")
	negExpr := qx.PREFIX("email", "future")

	posKey := snapshotExtraMaterializedPredCacheKey(t, db, posExpr)
	negKey := snapshotExtraMaterializedPredCacheKey(t, db, negExpr)
	if posKey == "" || negKey == "" {
		t.Fatalf("expected non-empty materialized predicate cache keys")
	}

	snapshotExtStoreMaterializedPred(old, posKey, snapshotExtraPosting(1))
	snapshotExtStoreMaterializedPred(old, negKey, posting.List{})
	if _, ok := snapshotExtLoadMaterializedPred(old, posKey); !ok {
		t.Fatalf("expected old positive cache entry")
	}
	if _, ok := snapshotExtLoadMaterializedPred(old, negKey); !ok {
		t.Fatalf("expected old negative cache entry")
	}

	pinned, ref, ok := db.engine.snapshot.PinBySeq(old.Seq)
	if !ok || pinned != old {
		t.Fatalf("expected old snapshot to be pinnable")
	}
	defer db.engine.snapshot.Unpin(old.Seq, ref)

	if err := db.Patch(1, []Field{{Name: "email", Value: "gamma@example.com"}}); err != nil {
		t.Fatalf("Patch(email): %v", err)
	}
	if err := db.Set(3, &snapshotExtraRec{Name: "carol", Email: "future@example.com"}); err != nil {
		t.Fatalf("Set(3): %v", err)
	}

	current := db.engine.snapshot.Current()
	if _, ok := snapshotExtLoadMaterializedPred(current, posKey); ok {
		t.Fatalf("current snapshot inherited stale positive cache entry for touched field")
	}
	if _, ok := snapshotExtLoadMaterializedPred(current, negKey); ok {
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

	beforeSeq := dbA.engine.snapshot.Current().Seq

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
		if seq != beforeSeq || snap.Seq != beforeSeq {
			t.Fatalf("dbA query pinned wrong snapshot during other-bucket writes: iter=%d seq=%d want=%d", i, seq, beforeSeq)
		}

		items := snapshotExtraQueryTxRecords(t, dbA, tx, snap, qx.Query().Sort("name", qx.ASC))
		rollback(tx)
		dbA.engine.snapshot.Unpin(seq, ref)
		if len(items) != 2 || items[0] == nil || items[1] == nil || items[0].Name != "alice" || items[1].Name != "bob" {
			t.Fatalf("dbA query drift during other-bucket writes at iter=%d: %#v", i, items)
		}
	}

	if err := <-writerDone; err != nil {
		t.Fatalf("dbB writer: %v", err)
	}

	afterSeq := dbA.engine.snapshot.Current().Seq
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

	old := db.engine.snapshot.Current()
	pinned, holdRef, ok := db.engine.snapshot.PinBySeq(old.Seq)
	if !ok || pinned != old {
		t.Fatalf("expected current snapshot to be pinnable")
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	db.testHooks = &testHooks{
		beforeCommit: func(op string) error {
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
		},
	}
	t.Cleanup(func() {
		db.testHooks = nil
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
	if !snapshotExtraHasFutureSnapshot(db, old.Seq) {
		t.Fatalf("expected staged future snapshot while delete commit is blocked")
	}
	if got := db.engine.snapshot.Current(); got != old {
		t.Fatalf("current snapshot changed before delete publish")
	}

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}
	if seq != old.Seq || snap != old {
		t.Fatalf("beginQueryTxSnapshot pinned wrong snapshot: seq=%d want=%d", seq, old.Seq)
	}

	beforeAll := snapshotExtraQueryTxRecords(t, db, tx, snap, qx.Query().Sort("name", qx.ASC))
	if len(beforeAll) != 3 || beforeAll[0] == nil || beforeAll[1] == nil || beforeAll[2] == nil ||
		beforeAll[0].Name != "alice" || beforeAll[1].Name != "bob" || beforeAll[2].Name != "carol" {
		t.Fatalf("old tx/snapshot drifted before delete publish: %#v", beforeAll)
	}
	beforeBob := snapshotExtraQueryTxRecords(t, db, tx, snap, qx.Query(qx.EQ("name", "bob")))
	if len(beforeBob) != 1 || beforeBob[0] == nil || beforeBob[0].Name != "bob" {
		t.Fatalf("old tx/snapshot lost soon-to-be-deleted row before publish: %#v", beforeBob)
	}

	rollback(tx)
	db.engine.snapshot.Unpin(seq, ref)

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Delete(2): %v", err)
	}

	oldKeys := snapshotExtraQuerySnapshotKeys(t, db, pinned, qx.Query().Sort("name", qx.ASC))
	if !slices.Equal(oldKeys, []uint64{1, 2, 3}) {
		t.Fatalf("pinned old snapshot changed after delete publish: got=%v want=[1 2 3]", oldKeys)
	}
	oldBob := snapshotExtraQuerySnapshotKeys(t, db, pinned, qx.Query(qx.EQ("name", "bob")))
	if !slices.Equal(oldBob, []uint64{2}) {
		t.Fatalf("pinned old snapshot lost deleted row after publish: got=%v want=[2]", oldBob)
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

	db.engine.snapshot.Unpin(old.Seq, holdRef)
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

	old := db.engine.snapshot.Current()
	pinned, holdRef, ok := db.engine.snapshot.PinBySeq(old.Seq)
	if !ok || pinned != old {
		t.Fatalf("expected current snapshot to be pinnable")
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	db.testHooks = &testHooks{
		beforeCommit: func(op string) error {
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
		},
	}
	t.Cleanup(func() {
		db.testHooks = nil
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
	if !snapshotExtraHasFutureSnapshot(db, old.Seq) {
		t.Fatalf("expected staged future snapshot while delete rollback is blocked")
	}

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}
	if seq != old.Seq || snap != old {
		t.Fatalf("beginQueryTxSnapshot pinned wrong snapshot: seq=%d want=%d", seq, old.Seq)
	}

	beforeAll := snapshotExtraQueryTxRecords(t, db, tx, snap, qx.Query().Sort("name", qx.ASC))
	if len(beforeAll) != 3 || beforeAll[0] == nil || beforeAll[1] == nil || beforeAll[2] == nil ||
		beforeAll[0].Name != "alice" || beforeAll[1].Name != "bob" || beforeAll[2].Name != "carol" {
		t.Fatalf("old tx/snapshot drifted before delete rollback: %#v", beforeAll)
	}

	rollback(tx)
	db.engine.snapshot.Unpin(seq, ref)

	close(release)
	err = <-done
	if err == nil || !strings.Contains(err.Error(), "failpoint: rollback delete") {
		t.Fatalf("expected rollback failpoint error, got: %v", err)
	}

	if got := db.engine.snapshot.Current(); got != old {
		t.Fatalf("rollback replaced published snapshot: got=%p want=%p", got, old)
	}

	oldKeys := snapshotExtraQuerySnapshotKeys(t, db, pinned, qx.Query().Sort("name", qx.ASC))
	if !slices.Equal(oldKeys, []uint64{1, 2, 3}) {
		t.Fatalf("pinned old snapshot changed after delete rollback: got=%v want=[1 2 3]", oldKeys)
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

	db.engine.snapshot.Unpin(old.Seq, holdRef)
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

	old := db.engine.snapshot.Current()
	pinned, holdRef, ok := db.engine.snapshot.PinBySeq(old.Seq)
	if !ok || pinned != old {
		t.Fatalf("expected current snapshot to be pinnable")
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	db.testHooks = &testHooks{
		beforeCommit: func(op string) error {
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
		},
	}
	t.Cleanup(func() {
		db.testHooks = nil
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
	if !snapshotExtraHasFutureSnapshot(db, old.Seq) {
		t.Fatalf("expected staged future snapshot while batch_delete commit is blocked")
	}
	if got := db.engine.snapshot.Current(); got != old {
		t.Fatalf("current snapshot changed before batch_delete publish")
	}

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}
	if seq != old.Seq || snap != old {
		t.Fatalf("beginQueryTxSnapshot pinned wrong snapshot: seq=%d want=%d", seq, old.Seq)
	}

	beforeAll := snapshotExtraQueryTxRecords(t, db, tx, snap, qx.Query().Sort("name", qx.ASC))
	if len(beforeAll) != 4 || beforeAll[0] == nil || beforeAll[1] == nil || beforeAll[2] == nil || beforeAll[3] == nil ||
		beforeAll[0].Name != "alice" || beforeAll[1].Name != "bob" || beforeAll[2].Name != "carol" || beforeAll[3].Name != "dave" {
		t.Fatalf("old tx/snapshot drifted before batch_delete publish: %#v", beforeAll)
	}

	rollback(tx)
	db.engine.snapshot.Unpin(seq, ref)

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("BatchDelete(2,2,3,3): %v", err)
	}

	oldKeys := snapshotExtraQuerySnapshotKeys(t, db, pinned, qx.Query().Sort("name", qx.ASC))
	if !slices.Equal(oldKeys, []uint64{1, 2, 3, 4}) {
		t.Fatalf("pinned old snapshot changed after batch_delete publish: got=%v want=[1 2 3 4]", oldKeys)
	}
	oldDeleted := snapshotExtraQuerySnapshotKeys(t, db, pinned, qx.Query(qx.IN("name", []string{"bob", "carol"})))
	if !slices.Equal(oldDeleted, []uint64{2, 3}) {
		t.Fatalf("pinned old snapshot lost batch-deleted rows after publish: got=%v want=[2 3]", oldDeleted)
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

	db.engine.snapshot.Unpin(old.Seq, holdRef)
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

	old := db.engine.snapshot.Current()
	pinned, holdRef, ok := db.engine.snapshot.PinBySeq(old.Seq)
	if !ok || pinned != old {
		t.Fatalf("expected current snapshot to be pinnable")
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	db.testHooks = &testHooks{
		beforeCommit: func(op string) error {
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
		},
	}
	t.Cleanup(func() {
		db.testHooks = nil
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
	if !snapshotExtraHasFutureSnapshot(db, old.Seq) {
		t.Fatalf("expected staged future snapshot while batch_delete rollback is blocked")
	}

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}
	if seq != old.Seq || snap != old {
		t.Fatalf("beginQueryTxSnapshot pinned wrong snapshot: seq=%d want=%d", seq, old.Seq)
	}

	beforeAll := snapshotExtraQueryTxRecords(t, db, tx, snap, qx.Query().Sort("name", qx.ASC))
	if len(beforeAll) != 4 || beforeAll[0] == nil || beforeAll[1] == nil || beforeAll[2] == nil || beforeAll[3] == nil ||
		beforeAll[0].Name != "alice" || beforeAll[1].Name != "bob" || beforeAll[2].Name != "carol" || beforeAll[3].Name != "dave" {
		t.Fatalf("old tx/snapshot drifted before batch_delete rollback: %#v", beforeAll)
	}

	rollback(tx)
	db.engine.snapshot.Unpin(seq, ref)

	close(release)
	err = <-done
	if err == nil || !strings.Contains(err.Error(), "failpoint: rollback batch_delete") {
		t.Fatalf("expected rollback failpoint error, got: %v", err)
	}

	if got := db.engine.snapshot.Current(); got != old {
		t.Fatalf("rollback replaced published snapshot: got=%p want=%p", got, old)
	}

	oldKeys := snapshotExtraQuerySnapshotKeys(t, db, pinned, qx.Query().Sort("name", qx.ASC))
	if !slices.Equal(oldKeys, []uint64{1, 2, 3, 4}) {
		t.Fatalf("pinned old snapshot changed after batch_delete rollback: got=%v want=[1 2 3 4]", oldKeys)
	}

	currentKeys, err := db.QueryKeys(qx.Query().Sort("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(current after rollback): %v", err)
	}
	if !slices.Equal(currentKeys, []uint64{1, 2, 3, 4}) {
		t.Fatalf("current snapshot changed after rolled-back batch_delete: got=%v want=[1 2 3 4]", currentKeys)
	}

	db.engine.snapshot.Unpin(old.Seq, holdRef)
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

	snapA := db.engine.snapshot.Current()
	pinnedA, refA, ok := db.engine.snapshot.PinBySeq(snapA.Seq)
	if !ok || pinnedA != snapA {
		t.Fatalf("expected snapshot A to be pinnable")
	}

	if err := db.Patch(2, []Field{{Name: "name", Value: "carol"}}); err != nil {
		t.Fatalf("Patch(2 name): %v", err)
	}

	snapB := db.engine.snapshot.Current()
	pinnedB, refB, ok := db.engine.snapshot.PinBySeq(snapB.Seq)
	if !ok || pinnedB != snapB {
		t.Fatalf("expected snapshot B to be pinnable")
	}

	if err := db.Delete(1); err != nil {
		t.Fatalf("Delete(1): %v", err)
	}

	snapC := db.engine.snapshot.Current()
	pinnedC, refC, ok := db.engine.snapshot.PinBySeq(snapC.Seq)
	if !ok || pinnedC != snapC {
		t.Fatalf("expected snapshot C to be pinnable")
	}

	if err := db.Set(3, &snapshotExtraRec{Name: "dave", Age: 50}); err != nil {
		t.Fatalf("Set(3): %v", err)
	}

	current := db.engine.snapshot.Current()
	if current.Seq == snapC.Seq {
		t.Fatalf("expected a newer snapshot after final Set")
	}

	checkKeys := func(label string, snap *snapshot.View, want []uint64) {
		t.Helper()
		got := snapshotExtraQuerySnapshotKeys(t, db, snap, qx.Query().Sort("name", qx.ASC))
		if !slices.Equal(got, want) {
			t.Fatalf("%s snapshot mismatch: got=%v want=%v", label, got, want)
		}
	}

	checkKeys("A", pinnedA, []uint64{1, 2})
	checkKeys("B", pinnedB, []uint64{1, 2})
	checkKeys("C", pinnedC, []uint64{2})

	currentKeys, err := db.QueryKeys(qx.Query().Sort("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(current): %v", err)
	}
	if !slices.Equal(currentKeys, []uint64{2, 3}) {
		t.Fatalf("current snapshot mismatch: got=%v want=[2 3]", currentKeys)
	}

	stats := db.engine.snapshot.Stats(current, nil)
	if stats.RegistrySize != 4 || stats.PinnedRefs != 3 {
		t.Fatalf("expected 4 snapshot refs after three pins and one current, stats=%+v", stats)
	}
	if got := db.engine.snapshot.Current(); got != current {
		t.Fatalf("expected current snapshot ref to remain published")
	}

	db.engine.snapshot.Unpin(snapB.Seq, refB)
	checkKeys("A after unpin B", pinnedA, []uint64{1, 2})
	checkKeys("C after unpin B", pinnedC, []uint64{2})

	stats = db.engine.snapshot.Stats(current, nil)
	if stats.RegistrySize != 3 || stats.PinnedRefs != 2 {
		t.Fatalf("unexpected registry after unpin B: stats=%+v", stats)
	}

	db.engine.snapshot.Unpin(snapA.Seq, refA)
	checkKeys("C after unpin A", pinnedC, []uint64{2})

	stats = db.engine.snapshot.Stats(current, nil)
	if stats.RegistrySize != 2 || stats.PinnedRefs != 1 {
		t.Fatalf("unexpected registry after unpin A: stats=%+v", stats)
	}

	db.engine.snapshot.Unpin(snapC.Seq, refC)
	stats = db.engine.snapshot.Stats(current, nil)
	if stats.RegistrySize != 1 || stats.PinnedRefs != 0 || stats.Sequence != current.Seq {
		t.Fatalf("unexpected registry after unpin C: stats=%+v", stats)
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

	old := db.engine.snapshot.Current()
	pinned, holdRef, ok := db.engine.snapshot.PinBySeq(old.Seq)
	if !ok || pinned != old {
		t.Fatalf("expected current snapshot to be pinnable")
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	db.testHooks = &testHooks{
		beforeCommit: func(op string) error {
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
		},
	}
	t.Cleanup(func() {
		db.testHooks = nil
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
	if !snapshotExtraHasFutureSnapshot(db, old.Seq) {
		t.Fatalf("expected staged future snapshot while truncate commit is blocked")
	}
	if got := db.engine.snapshot.Current(); got != old {
		t.Fatalf("current snapshot changed before truncate publish")
	}

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}
	if seq != old.Seq || snap != old {
		t.Fatalf("beginQueryTxSnapshot pinned wrong snapshot: seq=%d want=%d", seq, old.Seq)
	}

	beforeAll := snapshotExtraQueryTxRecords(t, db, tx, snap, qx.Query().Sort("name", qx.ASC))
	if len(beforeAll) != 3 || beforeAll[0] == nil || beforeAll[1] == nil || beforeAll[2] == nil ||
		beforeAll[0].Name != "alice" || beforeAll[1].Name != "bob" || beforeAll[2].Name != "carol" {
		t.Fatalf("old tx/snapshot drifted before truncate publish: %#v", beforeAll)
	}

	rollback(tx)
	db.engine.snapshot.Unpin(seq, ref)

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	oldKeys := snapshotExtraQuerySnapshotKeys(t, db, pinned, qx.Query().Sort("name", qx.ASC))
	if !slices.Equal(oldKeys, []uint64{1, 2, 3}) {
		t.Fatalf("pinned old snapshot changed after truncate publish: got=%v want=[1 2 3]", oldKeys)
	}

	currentKeys, err := db.QueryKeys(qx.Query().Sort("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(current after truncate): %v", err)
	}
	if len(currentKeys) != 0 {
		t.Fatalf("current snapshot missed committed truncate: got=%v want=[]", currentKeys)
	}

	db.engine.snapshot.Unpin(old.Seq, holdRef)
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

	old := db.engine.snapshot.Current()
	pinned, holdRef, ok := db.engine.snapshot.PinBySeq(old.Seq)
	if !ok || pinned != old {
		t.Fatalf("expected current snapshot to be pinnable")
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	db.testHooks = &testHooks{
		beforeCommit: func(op string) error {
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
		},
	}
	t.Cleanup(func() {
		db.testHooks = nil
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
	if !snapshotExtraHasFutureSnapshot(db, old.Seq) {
		t.Fatalf("expected staged future snapshot while truncate rollback is blocked")
	}

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}
	if seq != old.Seq || snap != old {
		t.Fatalf("beginQueryTxSnapshot pinned wrong snapshot: seq=%d want=%d", seq, old.Seq)
	}

	beforeAll := snapshotExtraQueryTxRecords(t, db, tx, snap, qx.Query().Sort("name", qx.ASC))
	if len(beforeAll) != 3 || beforeAll[0] == nil || beforeAll[1] == nil || beforeAll[2] == nil ||
		beforeAll[0].Name != "alice" || beforeAll[1].Name != "bob" || beforeAll[2].Name != "carol" {
		t.Fatalf("old tx/snapshot drifted before truncate rollback: %#v", beforeAll)
	}

	rollback(tx)
	db.engine.snapshot.Unpin(seq, ref)

	close(release)
	err = <-done
	if err == nil || !strings.Contains(err.Error(), "failpoint: rollback truncate") {
		t.Fatalf("expected rollback failpoint error, got: %v", err)
	}

	if got := db.engine.snapshot.Current(); got != old {
		t.Fatalf("rollback replaced published snapshot: got=%p want=%p", got, old)
	}

	oldKeys := snapshotExtraQuerySnapshotKeys(t, db, pinned, qx.Query().Sort("name", qx.ASC))
	if !slices.Equal(oldKeys, []uint64{1, 2, 3}) {
		t.Fatalf("pinned old snapshot changed after truncate rollback: got=%v want=[1 2 3]", oldKeys)
	}

	currentKeys, err := db.QueryKeys(qx.Query().Sort("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(current after rollback): %v", err)
	}
	if !slices.Equal(currentKeys, []uint64{1, 2, 3}) {
		t.Fatalf("current snapshot changed after rolled-back truncate: got=%v want=[1 2 3]", currentKeys)
	}

	db.engine.snapshot.Unpin(old.Seq, holdRef)
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

	old := db.engine.snapshot.Current()
	pinned1, ref1, ok := db.engine.snapshot.PinBySeq(old.Seq)
	if !ok || pinned1 != old {
		t.Fatalf("expected first pin to succeed")
	}
	pinned2, ref2, ok := db.engine.snapshot.PinBySeq(old.Seq)
	if !ok || pinned2 != old {
		t.Fatalf("expected second pin to succeed")
	}
	if ref1 != ref2 {
		t.Fatalf("expected duplicate pins to reuse same snapshot ref")
	}

	before := db.SnapshotStats()
	if before.Sequence != old.Seq || before.RegistrySize != 1 || before.PinnedRefs != 1 {
		t.Fatalf("unexpected stats before publish with duplicate pins: %+v", before)
	}

	if err := db.Set(3, &snapshotExtraRec{Name: "carol", Age: 40}); err != nil {
		t.Fatalf("Set(3): %v", err)
	}

	current := db.engine.snapshot.Current()
	if current.Seq == old.Seq {
		t.Fatalf("expected publish to advance snapshot sequence")
	}

	oldKeys := snapshotExtraQuerySnapshotKeys(t, db, pinned1, qx.Query().Sort("name", qx.ASC))
	if !slices.Equal(oldKeys, []uint64{1, 2}) {
		t.Fatalf("pinned old snapshot changed after publish: got=%v want=[1 2]", oldKeys)
	}

	mid := db.SnapshotStats()
	if mid.Sequence != current.Seq || mid.RegistrySize != 2 || mid.PinnedRefs != 1 {
		t.Fatalf("unexpected stats after publish with duplicate retired pins: %+v", mid)
	}

	db.engine.snapshot.Unpin(old.Seq, ref1)

	stillKeys := snapshotExtraQuerySnapshotKeys(t, db, pinned2, qx.Query().Sort("name", qx.ASC))
	if !slices.Equal(stillKeys, []uint64{1, 2}) {
		t.Fatalf("remaining pin lost old snapshot view after first unpin: got=%v want=[1 2]", stillKeys)
	}

	afterOne := db.SnapshotStats()
	if afterOne.RegistrySize != 2 || afterOne.PinnedRefs != 1 {
		t.Fatalf("unexpected stats after first unpin: %+v", afterOne)
	}

	db.engine.snapshot.Unpin(old.Seq, ref2)

	if got := db.engine.snapshot.Current(); got != current {
		t.Fatalf("expected current snapshot after last unpin")
	}

	afterTwo := db.SnapshotStats()
	if afterTwo.Sequence != current.Seq || afterTwo.RegistrySize != 1 || afterTwo.PinnedRefs != 0 {
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

	old := db.engine.snapshot.Current()
	alphaQ := qx.Query(qx.PREFIX("email", "alpha"))
	futureQ := qx.Query(qx.PREFIX("email", "future"))
	alphaKey := snapshotExtraMaterializedPredCacheKey(t, db, qx.PREFIX("email", "alpha"))
	futureKey := snapshotExtraMaterializedPredCacheKey(t, db, qx.PREFIX("email", "future"))

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
	if ids, ok := snapshotExtLoadMaterializedPred(old, alphaKey); !ok || ids.Cardinality() != 2 || !ids.Contains(1) || !ids.Contains(3) {
		t.Fatalf("expected warmed alpha cache on old snapshot")
	}
	if ids, ok := snapshotExtLoadMaterializedPred(old, futureKey); !ok || !ids.IsEmpty() {
		t.Fatalf("expected warmed negative future cache on old snapshot")
	}

	pinned, ref, ok := db.engine.snapshot.PinBySeq(old.Seq)
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

	current := db.engine.snapshot.Current()
	if _, ok := snapshotExtLoadMaterializedPred(current, alphaKey); ok {
		t.Fatalf("current snapshot inherited stale alpha cache across truncate rebuild")
	}
	if _, ok := snapshotExtLoadMaterializedPred(current, futureKey); ok {
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

	if ids, ok := snapshotExtLoadMaterializedPred(pinned, alphaKey); !ok || ids.Cardinality() != 2 || !ids.Contains(1) || !ids.Contains(3) {
		t.Fatalf("old alpha cache entry was corrupted by current rebuild queries")
	}
	if ids, ok := snapshotExtLoadMaterializedPred(pinned, futureKey); !ok || !ids.IsEmpty() {
		t.Fatalf("old future negative cache entry was corrupted by current rebuild queries")
	}
	if ids, ok := snapshotExtLoadMaterializedPred(current, alphaKey); !ok || !ids.IsEmpty() {
		t.Fatalf("current alpha cache entry not rebuilt as empty after query")
	}
	if ids, ok := snapshotExtLoadMaterializedPred(current, futureKey); !ok || ids.Cardinality() != 1 || !ids.Contains(1) {
		t.Fatalf("current future cache entry not rebuilt correctly after query")
	}

	db.engine.snapshot.Unpin(old.Seq, ref)
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

	old := db.engine.snapshot.Current()
	oldHighQ := qx.Query(qx.GTE("age", 400))
	oldHigh, err := db.QueryKeys(oldHighQ)
	if err != nil {
		t.Fatalf("QueryKeys(old high warm): %v", err)
	}
	if len(oldHigh) != 113 || !slices.Contains(oldHigh, uint64(400)) || !slices.Contains(oldHigh, uint64(512)) {
		t.Fatalf("unexpected old high range result: len=%d ids=%v", len(oldHigh), oldHigh)
	}
	oldEntry := snapshotExtraRequireNumericRangeBucketCacheEntry(t, old, "age")

	pinned, ref, ok := db.engine.snapshot.PinBySeq(old.Seq)
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

	current := db.engine.snapshot.Current()
	if _, ok := current.NumericRangeBucketCache().LoadField("age"); ok {
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
	currentEntry := snapshotExtraRequireNumericRangeBucketCacheEntry(t, db.engine.snapshot.Current(), "age")
	if currentEntry == oldEntry {
		t.Fatalf("current numeric range cache reused old pinned snapshot entry across rebuild")
	}

	pinnedHighAgain := snapshotExtraQuerySnapshotKeys(t, db, pinned, oldHighQ)
	if len(pinnedHighAgain) != 113 || !slices.Contains(pinnedHighAgain, uint64(400)) || !slices.Contains(pinnedHighAgain, uint64(512)) {
		t.Fatalf("old numeric range cache was corrupted by current rebuild queries: len=%d ids=%v", len(pinnedHighAgain), pinnedHighAgain)
	}

	db.engine.snapshot.Unpin(old.Seq, ref)
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

	snapA := db.engine.snapshot.Current()
	_, refA, ok := db.engine.snapshot.PinBySeq(snapA.Seq)
	if !ok {
		t.Fatalf("expected snapshot A to be pinnable")
	}

	if err := db.Patch(2, []Field{{Name: "name", Value: "carol"}}); err != nil {
		t.Fatalf("Patch(2): %v", err)
	}

	snapB := db.engine.snapshot.Current()
	_, refB, ok := db.engine.snapshot.PinBySeq(snapB.Seq)
	if !ok {
		t.Fatalf("expected snapshot B to be pinnable")
	}

	if err := db.Set(3, &snapshotExtraRec{Name: "dave", Age: 40}); err != nil {
		t.Fatalf("Set(3): %v", err)
	}

	current := db.engine.snapshot.Current()
	mid := db.SnapshotStats()
	if mid.Sequence != current.Seq || mid.RegistrySize != 3 || mid.PinnedRefs != 2 {
		t.Fatalf("unexpected stats with two distinct retired pinned snapshots: %+v", mid)
	}

	db.engine.snapshot.Unpin(snapA.Seq, refA)
	afterA := db.SnapshotStats()
	if afterA.Sequence != current.Seq || afterA.RegistrySize != 2 || afterA.PinnedRefs != 1 {
		t.Fatalf("unexpected stats after unpin snapshot A: %+v", afterA)
	}

	db.engine.snapshot.Unpin(snapB.Seq, refB)
	afterB := db.SnapshotStats()
	if afterB.Sequence != current.Seq || afterB.RegistrySize != 1 || afterB.PinnedRefs != 0 {
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

	old := db.engine.snapshot.Current()
	alphaExpr := qx.PREFIX("email", "alpha")
	alphaQ := qx.Query(qx.PREFIX("email", "alpha"))
	wantOld, err := db.QueryKeys(alphaQ)
	if err != nil {
		t.Fatalf("QueryKeys(alpha warm): %v", err)
	}
	alphaKey := snapshotExtraMaterializedPredCacheKey(t, db, alphaExpr)
	cachedOld, ok := snapshotExtLoadMaterializedPred(old, alphaKey)
	if !ok {
		t.Fatalf("expected warmed old alpha cache entry")
	}
	defer cachedOld.Release()

	pinned, ref, ok := db.engine.snapshot.PinBySeq(old.Seq)
	if !ok || pinned != old {
		t.Fatalf("expected old snapshot to be pinnable")
	}
	defer db.engine.snapshot.Unpin(old.Seq, ref)

	queryPinnedAlpha := func() ([]uint64, error) {
		prepared, viewQ, err := prepareTestQuery(db.engine, alphaQ)
		if err != nil {
			return nil, err
		}
		view := db.engine.exec.AcquireView(pinned)
		out, err := view.Query(&viewQ, false, false)
		db.engine.exec.ReleaseView(view)
		prepared.Release()
		return out, err
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

	cachedPinned, ok := snapshotExtLoadMaterializedPred(pinned, alphaKey)
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

	old := db.engine.snapshot.Current()
	rangeQ := qx.Query(qx.GTE("age", 1536))
	wantOld, err := db.QueryKeys(rangeQ)
	if err != nil {
		t.Fatalf("QueryKeys(range warm): %v", err)
	}
	oldEntry := snapshotExtraRequireNumericRangeBucketCacheEntry(t, old, "age")

	pinned, ref, ok := db.engine.snapshot.PinBySeq(old.Seq)
	if !ok || pinned != old {
		t.Fatalf("expected old snapshot to be pinnable")
	}
	defer db.engine.snapshot.Unpin(old.Seq, ref)

	queryPinnedRange := func() ([]uint64, error) {
		prepared, viewQ, err := prepareTestQuery(db.engine, rangeQ)
		if err != nil {
			return nil, err
		}
		view := db.engine.exec.AcquireView(pinned)
		out, err := view.Query(&viewQ, false, false)
		db.engine.exec.ReleaseView(view)
		prepared.Release()
		return out, err
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

	old := db.engine.snapshot.Current()
	pinned, ref, ok := db.engine.snapshot.PinBySeq(old.Seq)
	if !ok || pinned != old {
		t.Fatalf("expected old snapshot to be pinnable")
	}
	defer db.engine.snapshot.Unpin(old.Seq, ref)

	wantOld := snapshotExtraScanSnapshotStringKeys(t, db, pinned, "")

	scanPinned := func() ([]string, error) {
		iter := pinned.Universe.Iter()
		defer iter.Release()

		var out []string
		if err := db.scanStringKeys(pinned.StrMap, pinned.Universe, iter, "", func(id string) (bool, error) {
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

	old := db.engine.snapshot.Current()
	seq := old.Seq

	if _, err := db.QueryKeys(qx.Query(qx.PREFIX("email", "user-00"))); err != nil {
		t.Fatalf("QueryKeys(warm materialized pred): %v", err)
	}
	if _, err := db.QueryKeys(qx.Query(qx.GTE("age", 400))); err != nil {
		t.Fatalf("QueryKeys(warm numeric range): %v", err)
	}
	if old.MaterializedPredCache() == nil || old.MaterializedPredCache().EntryCount() == 0 {
		t.Fatalf("expected warmed materialized predicate cache on old snapshot")
	}
	if old.NumericRangeBucketCache() == nil || old.NumericRangeBucketCache().EntryCount() == 0 {
		t.Fatalf("expected warmed numeric range cache on old snapshot")
	}

	pinned, ref, ok := db.engine.snapshot.PinBySeq(seq)
	if !ok || pinned != old {
		t.Fatalf("expected old snapshot to be pinnable")
	}

	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	current := db.engine.snapshot.Current()
	if current == old {
		t.Fatalf("expected RebuildIndex to publish a new snapshot instance")
	}
	if current.Seq != seq {
		t.Fatalf("expected same-seq republish on RebuildIndex: old=%d new=%d", seq, current.Seq)
	}

	db.engine.snapshot.Unpin(seq, ref)

	matPredAlive := old.MaterializedPredCache() != nil
	rangeAlive := old.NumericRangeBucketCache() != nil
	if matPredAlive || rangeAlive {
		t.Fatalf(
			"old same-seq snapshot runtime caches survived after last unpin: matPredAlive=%v rangeAlive=%v oldSeq=%d currentSeq=%d",
			matPredAlive,
			rangeAlive,
			old.Seq,
			current.Seq,
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

	snapA := db.engine.snapshot.Current()
	pinnedA, refA, ok := db.engine.snapshot.PinBySeq(snapA.Seq)
	if !ok || pinnedA != snapA {
		t.Fatalf("expected snapshot A to be pinnable")
	}

	if err := db.Set("k3", &snapshotExtraRec{Name: "three", Age: 30}); err != nil {
		t.Fatalf("Set(k3): %v", err)
	}

	snapB := db.engine.snapshot.Current()
	pinnedB, refB, ok := db.engine.snapshot.PinBySeq(snapB.Seq)
	if !ok || pinnedB != snapB {
		t.Fatalf("expected snapshot B to be pinnable")
	}

	if err := db.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if err := db.Set("k4", &snapshotExtraRec{Name: "four", Age: 40}); err != nil {
		t.Fatalf("Set(k4): %v", err)
	}

	current := db.engine.snapshot.Current()
	if _, ok := current.StrMap.Index("k1"); ok {
		t.Fatalf("current snapshot retained pre-truncate key k1")
	}
	if _, ok := current.StrMap.Index("k2"); ok {
		t.Fatalf("current snapshot retained pre-truncate key k2")
	}
	if _, ok := current.StrMap.Index("k3"); ok {
		t.Fatalf("current snapshot retained pre-truncate key k3")
	}
	if idx, ok := current.StrMap.Index("k4"); !ok || idx != 1 {
		t.Fatalf("current snapshot failed to rebuild post-truncate strmap: idx=%d ok=%v", idx, ok)
	}

	checkScan := func(label string, snap *snapshot.View, want []string) {
		t.Helper()
		got := snapshotExtraScanSnapshotStringKeys(t, db, snap, "")
		if !slices.Equal(got, want) {
			t.Fatalf("%s scan mismatch: got=%v want=%v", label, got, want)
		}
	}

	checkScan("A", pinnedA, []string{"k1", "k2"})
	checkScan("B", pinnedB, []string{"k1", "k2", "k3"})

	currentKeys, err := db.QueryKeys(qx.Query().Sort("name", qx.ASC))
	if err != nil {
		t.Fatalf("QueryKeys(current): %v", err)
	}
	if !slices.Equal(currentKeys, []string{"k4"}) {
		t.Fatalf("current key set mismatch after truncate+set: got=%v want=[k4]", currentKeys)
	}

	stats := db.engine.snapshot.Stats(current, nil)
	if stats.RegistrySize != 3 || stats.PinnedRefs != 2 {
		t.Fatalf("expected 3 snapshot refs after truncate rotation, stats=%+v", stats)
	}
	if got := db.engine.snapshot.Current(); got != current {
		t.Fatalf("expected current snapshot ref to remain published")
	}

	db.engine.snapshot.Unpin(snapB.Seq, refB)
	checkScan("A after unpin B", pinnedA, []string{"k1", "k2"})

	stats = db.engine.snapshot.Stats(current, nil)
	if stats.RegistrySize != 2 || stats.PinnedRefs != 1 {
		t.Fatalf("unexpected registry after unpin B: stats=%+v", stats)
	}

	db.engine.snapshot.Unpin(snapA.Seq, refA)
	stats = db.engine.snapshot.Stats(current, nil)
	if stats.RegistrySize != 1 || stats.PinnedRefs != 0 || stats.Sequence != current.Seq {
		t.Fatalf("unexpected registry after unpin A: stats=%+v", stats)
	}

	snapshotExtraAssertNoFutureSnapshotRefs(t, db)
}
