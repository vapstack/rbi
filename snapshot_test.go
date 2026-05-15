package rbi

import (
	"errors"
	"fmt"
	"slices"
	"testing"

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

	lenIDs := db.engine.currentQueryViewForTests().lenFieldIndexView("tags").LookupPostingRetained(keycodec.U64ByteString(2))
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
