package rbi

import (
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/posting"
)

func mustCurrentBucketSequence(t *testing.T, db *DB[uint64, Rec]) uint64 {
	t.Helper()

	seq, err := db.currentBucketSequence()
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

func TestSnapshotSequence_PublishedOnWrite(t *testing.T) {
	db, _ := openTempDBUint64(t)

	before := db.getSnapshot().seq

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	after := db.getSnapshot().seq
	if after <= before {
		t.Fatalf("snapshot sequence did not advance: before=%d after=%d", before, after)
	}

	cur := mustCurrentBucketSequence(t, db)
	if after != cur {
		t.Fatalf("snapshot sequence mismatch with bolt seq: snapshot=%d bolt=%d", after, cur)
	}
}

func TestSnapshotSequence_PreviousSnapshotIsRetiredAfterPublishWithoutPins(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "seed", Age: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	oldSequence := db.getSnapshot().seq

	if err := db.Set(2, &Rec{Name: "new", Age: 20}); err != nil {
		t.Fatalf("concurrent Set: %v", err)
	}

	latest := db.getSnapshot().seq
	if latest <= oldSequence {
		t.Fatalf("latest sequence did not advance: old=%d latest=%d", oldSequence, latest)
	}

	if _, _, ok := db.pinSnapshotRefBySeq(oldSequence); ok {
		t.Fatalf("previous snapshot unexpectedly remained pinable: sequence=%d", oldSequence)
	}
}

func TestSnapshotSequence_AdvancesOnWrite(t *testing.T) {
	db, _ := openTempDBUint64(t)

	before := db.getSnapshot().seq
	if err := db.Set(1, &Rec{Name: "x", Age: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	after := db.getSnapshot().seq
	if after <= before {
		t.Fatalf("snapshot sequence did not advance on write: before=%d after=%d", before, after)
	}
}

func TestSnapshot_PublishedAsFullState(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Tags: []string{"go", "db"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	s := db.getSnapshot()
	nameIDs := s.fieldLookupPostingRetained("name", "alice")
	snapshotPostingContainsAll(t, nameIDs, 1)

	tagsIDs := s.fieldLookupPostingRetained("tags", "go")
	snapshotPostingContainsAll(t, tagsIDs, 1)

	lenIDs := db.lenFieldOverlay("tags").lookupPostingRetained(uint64ByteStr(2))
	snapshotPostingContainsAll(t, lenIDs, 1)
}

func TestSnapshot_PreviousSnapshotRemainsImmutable(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice"}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	s1 := db.getSnapshot()

	if err := db.Set(2, &Rec{Name: "bob"}); err != nil {
		t.Fatalf("second Set: %v", err)
	}
	s2 := db.getSnapshot()
	if s1 == s2 {
		t.Fatalf("expected a newly published snapshot instance")
	}

	oldBob := s1.fieldLookupPostingRetained("name", "bob")
	if !oldBob.IsEmpty() {
		t.Fatalf("old snapshot unexpectedly sees future write")
	}

	newBob := s2.fieldLookupPostingRetained("name", "bob")
	snapshotPostingContainsAll(t, newBob, 2)
}

func TestSnapshotStrMap_OldSnapshotDoesNotSeeFutureKeyMappings(t *testing.T) {
	db, _ := openTempDBString(t)

	if err := db.Set("k1", &Rec{Name: "one"}); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	s1 := db.getSnapshot()
	idx1, ok := s1.strmap.getIdxNoLock("k1")
	if !ok || idx1 == 0 {
		t.Fatalf("expected k1 in first snapshot")
	}

	if err := db.Set("k2", &Rec{Name: "two"}); err != nil {
		t.Fatalf("Set(k2): %v", err)
	}
	s2 := db.getSnapshot()
	if _, ok := s1.strmap.getIdxNoLock("k2"); ok {
		t.Fatalf("old snapshot unexpectedly sees k2 mapping")
	}

	idx2, ok := s2.strmap.getIdxNoLock("k2")
	if !ok || idx2 == 0 {
		t.Fatalf("expected k2 in latest snapshot")
	}
	if got, ok := s1.strmap.getStringNoLock(idx1); !ok || got != "k1" {
		t.Fatalf("old snapshot lost k1 mapping: got=%q ok=%v", got, ok)
	}
	if got, ok := s2.strmap.getStringNoLock(idx2); !ok || got != "k2" {
		t.Fatalf("latest snapshot missing k2 mapping: got=%q ok=%v", got, ok)
	}
}

func TestSnapshotStrMap_LatestSnapshotRetainsOldMappingsAcrossChain(t *testing.T) {
	db, _ := openTempDBString(t)

	if err := db.Set("k1", &Rec{Name: "one"}); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	idx1, ok := db.getSnapshot().strmap.getIdxNoLock("k1")
	if !ok || idx1 == 0 {
		t.Fatalf("expected k1 mapping in first snapshot")
	}

	for i := 2; i <= 12; i++ {
		key := fmt.Sprintf("k%d", i)
		if err := db.Set(key, &Rec{Name: key}); err != nil {
			t.Fatalf("Set(%s): %v", key, err)
		}
	}

	latest := db.getSnapshot().strmap
	if got, ok := latest.getIdxNoLock("k1"); !ok || got != idx1 {
		t.Fatalf("latest snapshot lost k1 idx: got=%d ok=%v want=%d", got, ok, idx1)
	}
	if got, ok := latest.getStringNoLock(idx1); !ok || got != "k1" {
		t.Fatalf("latest snapshot lost k1 reverse mapping: got=%q ok=%v", got, ok)
	}
}

func TestSnapshot_EmptyBaseWriteInvalidatesTouchedMaterializedPredicateCache(t *testing.T) {
	db, _ := openTempDBUint64(t)

	expr := qx.Expr{Op: qx.OpPREFIX, Field: "email", Value: "user"}
	cacheKey := db.materializedPredCacheKey(expr)
	if cacheKey == "" {
		t.Fatalf("expected materialized predicate cache key for prefix predicate")
	}

	emptySnap := db.getSnapshot()
	emptySnap.storeMaterializedPred(cacheKey, posting.List{})
	if _, ok := emptySnap.loadMaterializedPred(cacheKey); !ok {
		t.Fatalf("expected cached negative predicate result on empty snapshot")
	}

	if err := db.Set(1, &Rec{
		Name:  "alice",
		Email: "user1@example.com",
	}); err != nil {
		t.Fatalf("Set(first row): %v", err)
	}

	if _, ok := db.getSnapshot().loadMaterializedPred(cacheKey); ok {
		t.Fatalf("expected first write from empty base to invalidate touched predicate cache")
	}

	items, err := db.Query(qx.Query(expr))
	if err != nil {
		t.Fatalf("Query(prefix): %v", err)
	}
	if len(items) != 1 || items[0] == nil || items[0].Email != "user1@example.com" {
		t.Fatalf("unexpected query result after empty-base cache invalidation: %#v", items)
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

	s := db.getSnapshot()
	if s.lenZeroComplement["tags"] {
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

	orderQ := qx.Query().ByArrayCount("tags", qx.ASC)
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
	oldSeq := db.getSnapshot().seq

	if _, ref, ok := db.pinSnapshotRefBySeq(oldSeq); !ok {
		t.Fatalf("expected to pin previous snapshot")
	} else {
		if err := db.Set(2, &Rec{Name: "next", Age: 20}); err != nil {
			t.Fatalf("second Set: %v", err)
		}

		latestSeq := db.getSnapshot().seq
		db.snapshot.mu.RLock()
		_, oldPresentWhilePinned := db.snapshot.bySeq[oldSeq]
		registryWhilePinned := len(db.snapshot.bySeq)
		db.snapshot.mu.RUnlock()
		if !oldPresentWhilePinned || registryWhilePinned < 2 {
			t.Fatalf("expected pinned snapshot to keep registry entry alive before unpin")
		}

		db.unpinSnapshotRef(oldSeq, ref)

		db.snapshot.mu.RLock()
		_, oldPresentAfterUnpin := db.snapshot.bySeq[oldSeq]
		_, latestPresent := db.snapshot.bySeq[latestSeq]
		registryAfterUnpin := len(db.snapshot.bySeq)
		db.snapshot.mu.RUnlock()

		if oldPresentAfterUnpin {
			t.Fatalf("expected old snapshot to be pruned after last unpin")
		}
		if !latestPresent {
			t.Fatalf("expected latest snapshot to remain registered after prune")
		}
		if registryAfterUnpin != 1 {
			t.Fatalf("expected registry to retain only latest snapshot after idle unpin, got=%d", registryAfterUnpin)
		}
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
		out.Add(id)
	}
	return out
}

func snapshotExtSyncMapLen(m *sync.Map) int {
	if m == nil {
		return 0
	}
	var n int
	m.Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
}

func snapshotExtStorage(keys ...string) fieldIndexStorage {
	if len(keys) == 0 {
		return fieldIndexStorage{}
	}
	m := getPostingMap()
	for i, key := range keys {
		m[key] = (posting.List{}).BuildAdded(uint64(i + 1))
	}
	return newFlatFieldIndexStorageFromPostingMapOwned(m, false)
}

func snapshotExtFieldContainsID(tb testing.TB, s *indexSnapshot, field, key string, id uint64) bool {
	tb.Helper()
	return s.fieldLookupPostingRetained(field, key).Contains(id)
}

func snapshotExtNilContainsID(tb testing.TB, s *indexSnapshot, field string, id uint64) bool {
	tb.Helper()
	return newFieldOverlay(s.nilFieldIndexSlice(field)).lookupPostingRetained(nilIndexEntryKey).Contains(id)
}

func snapshotExtLenContainsID(tb testing.TB, s *indexSnapshot, field string, ln uint64, id uint64) bool {
	tb.Helper()
	return newFieldOverlayStorage(s.lenIndex[field]).lookupPostingRetained(uint64ByteStr(ln)).Contains(id)
}

func snapshotExtLenNonEmptyContainsID(tb testing.TB, s *indexSnapshot, field string, id uint64) bool {
	tb.Helper()
	return newFieldOverlayStorage(s.lenIndex[field]).lookupPostingRetained(lenIndexNonEmptyKey).Contains(id)
}

func snapshotExtRunConcurrent(n int, fn func(i int)) {
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			runtime.Gosched()
			fn(i)
		}(i)
	}
	close(start)
	wg.Wait()
}

func snapshotExtEvalNumericRangeBuckets(t *testing.T, db *DB[uint64, Rec], expr qx.Expr) {
	t.Helper()

	fm := db.fields[expr.Field]
	if fm == nil {
		t.Fatalf("expected %q field metadata", expr.Field)
	}
	ov := db.fieldOverlay(expr.Field)
	if !ov.hasData() {
		t.Fatalf("expected %q field index data", expr.Field)
	}

	key, isSlice, isNil, err := db.exprValueToIdxScalar(expr)
	if err != nil {
		t.Fatalf("exprValueToIdxScalar(%s): %v", expr.Field, err)
	}
	if isSlice || isNil {
		t.Fatalf("unexpected scalar flags: isSlice=%v isNil=%v", isSlice, isNil)
	}

	rb, ok := rangeBoundsForOp(expr.Op, key)
	if !ok {
		t.Fatalf("rangeBoundsForOp(%v) failed", expr.Op)
	}

	out, ok := db.tryEvalNumericRangeBuckets(expr.Field, fm, ov, ov.rangeForBounds(rb))
	if !ok {
		t.Fatalf("expected numeric range bucket path for %q", expr.Field)
	}
	out.release()
}

type snapshotReorderedSliceRec struct {
	Tags   []string `db:"tags"`
	Scores []int    `db:"scores"`
}

func TestSnapshotExt_CollectSnapshotBatchDiff_ReorderedSliceValuesProduceNoDeltas(t *testing.T) {
	db := openTempDBUint64Reflect[snapshotReorderedSliceRec](t, "snapshot_reordered_slice_diff.db", snapshotExtOptions())

	oldVal := &snapshotReorderedSliceRec{
		Tags:   []string{"go", "db", "go"},
		Scores: []int{3, 1, 3},
	}
	newVal := &snapshotReorderedSliceRec{
		Tags:   []string{"db", "go"},
		Scores: []int{1, 3},
	}

	for _, f := range []string{"tags", "scores"} {
		acc, ok := db.indexedFieldByName[f]
		if !ok {
			t.Fatalf("missing accessor for %q", f)
		}

		var state snapshotFieldBatchState
		acc.collectSnapshotBatchDiff(1, unsafe.Pointer(oldVal), unsafe.Pointer(newVal), false, &state)

		if state.changed {
			t.Fatalf("%s: reorder-only diff unexpectedly marked field as changed", f)
		}
		if state.index != nil {
			t.Fatalf("%s: reorder-only diff produced string deltas: %#v", f, state.index)
		}
		if state.fixed != nil {
			t.Fatalf("%s: reorder-only diff produced fixed deltas: %#v", f, state.fixed)
		}
		if state.nils != nil {
			t.Fatalf("%s: reorder-only diff produced nil deltas: %#v", f, state.nils)
		}
		if state.lengths != nil {
			t.Fatalf("%s: reorder-only diff produced len deltas: %#v", f, state.lengths)
		}
	}
}

func TestSnapshotExt_PublishSameSeqReusesRefAndUpdatesCurrent(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	first := &indexSnapshot{seq: 7, universe: posting.List{}}
	db.publishSnapshotRef(first)

	gotFirst, ref, ok := db.pinSnapshotRefBySeq(first.seq)
	if !ok || gotFirst != first {
		t.Fatalf("expected first snapshot to be pinnable")
	}

	second := &indexSnapshot{seq: first.seq, universe: posting.List{}}
	db.publishSnapshotRef(second)

	if got := db.getSnapshot(); got != second {
		t.Fatalf("expected current snapshot to be replaced on same-seq publish")
	}

	db.snapshot.mu.RLock()
	held := db.snapshot.bySeq[first.seq]
	registrySize := len(db.snapshot.bySeq)
	db.snapshot.mu.RUnlock()

	if held != ref {
		t.Fatalf("expected registry entry to reuse existing ref")
	}
	if registrySize != 1 {
		t.Fatalf("expected single registry entry after same-seq publish, got=%d", registrySize)
	}

	gotSecond, ref2, ok := db.pinSnapshotRefBySeq(first.seq)
	if !ok || gotSecond != second || ref2 != ref {
		t.Fatalf("expected second snapshot to reuse same ref")
	}

	db.unpinSnapshotRef(first.seq, ref)
	db.unpinSnapshotRef(first.seq, ref2)

	db.snapshot.mu.RLock()
	_, stillPresent := db.snapshot.bySeq[first.seq]
	db.snapshot.mu.RUnlock()
	if !stillPresent {
		t.Fatalf("expected current same-seq snapshot to remain registered after unpin")
	}
}

func TestSnapshotExt_DropStagedSnapshotPinnedEntryBecomesUnpinnableUntilUnpin(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	staged := &indexSnapshot{seq: 11}
	db.stageSnapshot(staged)

	got, ref, ok := db.pinSnapshotRefBySeq(staged.seq)
	if !ok || got != staged {
		t.Fatalf("expected staged snapshot to be pinnable before drop")
	}

	db.dropStagedSnapshot(staged.seq)

	db.snapshot.mu.RLock()
	held := db.snapshot.bySeq[staged.seq]
	db.snapshot.mu.RUnlock()
	if held != ref || held == nil || held.snap != nil {
		t.Fatalf("expected dropped pinned staged snapshot to keep ref but clear snap")
	}

	if _, _, ok := db.pinSnapshotRefBySeq(staged.seq); ok {
		t.Fatalf("expected dropped staged snapshot to become unpinnable")
	}

	db.unpinSnapshotRef(staged.seq, ref)

	db.snapshot.mu.RLock()
	_, stillPresent := db.snapshot.bySeq[staged.seq]
	db.snapshot.mu.RUnlock()
	if stillPresent {
		t.Fatalf("expected staged snapshot registry entry to be pruned after last unpin")
	}
}

func TestSnapshotExt_DropStagedSnapshotUnpinnedDeletesEntry(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	staged := &indexSnapshot{seq: 12}
	db.stageSnapshot(staged)
	db.dropStagedSnapshot(staged.seq)

	db.snapshot.mu.RLock()
	_, present := db.snapshot.bySeq[staged.seq]
	db.snapshot.mu.RUnlock()
	if present {
		t.Fatalf("expected unpinned staged snapshot to be removed immediately")
	}
}

func TestSnapshotExt_RetirePinnedPublishedSnapshotNilsEntryUntilUnpin(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	first := &indexSnapshot{seq: 1, universe: posting.List{}}
	second := &indexSnapshot{seq: 2, universe: posting.List{}}
	db.publishSnapshotRef(first)

	got, ref, ok := db.pinSnapshotRefBySeq(first.seq)
	if !ok || got != first {
		t.Fatalf("expected first snapshot to be pinnable")
	}

	db.publishSnapshotRef(second)

	db.snapshot.mu.RLock()
	oldHeld := db.snapshot.bySeq[first.seq]
	_, nextPresent := db.snapshot.bySeq[second.seq]
	db.snapshot.mu.RUnlock()

	if oldHeld != ref || oldHeld == nil || oldHeld.snap != nil {
		t.Fatalf("expected pinned retired snapshot to keep ref with nil snap")
	}
	if !nextPresent {
		t.Fatalf("expected latest snapshot to remain registered")
	}
	if _, _, ok := db.pinSnapshotRefBySeq(first.seq); ok {
		t.Fatalf("expected retired pinned snapshot to stop accepting new pins")
	}

	db.unpinSnapshotRef(first.seq, ref)

	db.snapshot.mu.RLock()
	_, oldPresent := db.snapshot.bySeq[first.seq]
	_, nextPresent = db.snapshot.bySeq[second.seq]
	db.snapshot.mu.RUnlock()
	if oldPresent {
		t.Fatalf("expected retired snapshot to be pruned after last unpin")
	}
	if !nextPresent {
		t.Fatalf("expected latest snapshot to survive old-snapshot prune")
	}
}

func TestSnapshotExt_SnapshotStatsTracksPinnedRefAcrossPublish(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	oldSeq := db.getSnapshot().seq
	if _, ref, ok := db.pinSnapshotRefBySeq(oldSeq); !ok {
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
		if mid.Sequence != db.getSnapshot().seq || mid.Sequence <= oldSeq {
			t.Fatalf("expected stats sequence to advance after publish: %+v", mid)
		}
		if mid.RegistrySize != 2 || mid.PinnedRefs != 1 {
			t.Fatalf("unexpected stats with pinned retired snapshot: %+v", mid)
		}

		db.unpinSnapshotRef(oldSeq, ref)

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

	current := db.getSnapshot()
	staged := &indexSnapshot{seq: current.seq + 1}
	db.stageSnapshot(staged)
	defer db.dropStagedSnapshot(staged.seq)

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}
	defer rollback(tx)
	defer db.unpinSnapshotRef(seq, ref)

	if seq != current.seq {
		t.Fatalf("query tx pinned wrong sequence: got=%d want=%d", seq, current.seq)
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

	oldItems, err := db.queryRecords(tx, snap, qx.Query(qx.EQ("name", "bob")))
	if err != nil {
		t.Fatalf("queryRecords(old snapshot): %v", err)
	}
	if len(oldItems) != 0 {
		t.Fatalf("old tx/snapshot unexpectedly sees future write: %#v", oldItems)
	}

	oldItems, err = db.queryRecords(tx, snap, qx.Query(qx.EQ("name", "alice")))
	if err != nil {
		t.Fatalf("queryRecords(old snapshot for alice): %v", err)
	}
	if len(oldItems) != 1 || oldItems[0] == nil || oldItems[0].Name != "alice" {
		t.Fatalf("unexpected old snapshot query result: %#v", oldItems)
	}

	rollback(tx)
	db.unpinSnapshotRef(seq, ref)

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

	s1 := db.getSnapshot()

	if err := db.Set(2, &Rec{Name: "charlie", Age: 20}); err != nil {
		t.Fatalf("Set(update): %v", err)
	}

	s2 := db.getSnapshot()
	if !snapshotExtFieldContainsID(t, s1, "name", "bob", 2) {
		t.Fatalf("old snapshot lost original scalar posting")
	}
	if snapshotExtFieldContainsID(t, s1, "name", "charlie", 2) {
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

	s1 := db.getSnapshot()

	if err := db.Set(1, &Rec{Name: "alice", Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set(update): %v", err)
	}

	s2 := db.getSnapshot()
	if !snapshotExtLenContainsID(t, s1, "tags", 2, 1) {
		t.Fatalf("old snapshot lost original len posting")
	}
	if snapshotExtLenContainsID(t, s1, "tags", 1, 1) {
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

	s1 := db.getSnapshot()
	if !s1.lenZeroComplement["tags"] {
		t.Fatalf("expected zero-complement len index for sparse non-empty tags")
	}
	if !snapshotExtLenNonEmptyContainsID(t, s1, "tags", 1) {
		t.Fatalf("expected old snapshot non-empty sentinel to include id=1")
	}

	if err := db.Patch(1, []Field{{Name: "tags", Value: []string{}}}); err != nil {
		t.Fatalf("Patch(tags): %v", err)
	}

	s2 := db.getSnapshot()
	if !snapshotExtLenNonEmptyContainsID(t, s1, "tags", 1) {
		t.Fatalf("old snapshot lost non-empty sentinel membership")
	}
	if snapshotExtLenNonEmptyContainsID(t, s2, "tags", 1) {
		t.Fatalf("new snapshot kept stale non-empty sentinel membership")
	}
}

func TestSnapshotExt_OldSnapshotNilIndexImmutableAcrossPatch(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Email: "alice@example.com", Opt: nil}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	s1 := db.getSnapshot()
	val := "zzz"
	if err := db.Patch(1, []Field{{Name: "opt", Value: &val}}); err != nil {
		t.Fatalf("Patch(opt): %v", err)
	}

	s2 := db.getSnapshot()
	if !snapshotExtNilContainsID(t, s1, "opt", 1) {
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

	s1 := db.getSnapshot()
	if err := db.Delete(2); err != nil {
		t.Fatalf("Delete(2): %v", err)
	}

	s2 := db.getSnapshot()
	if !s1.universe.Contains(2) || s1.universe.Cardinality() != 2 {
		t.Fatalf("old snapshot universe mutated after delete")
	}
	if s2.universe.Contains(2) || s2.universe.Cardinality() != 1 {
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

	old := db.getSnapshot()
	oldSeq := old.seq
	pinned, ref, ok := db.pinSnapshotRefBySeq(oldSeq)
	if !ok || pinned != old {
		t.Fatalf("expected current snapshot to be pinnable before truncate")
	}

	if err := db.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	current := db.getSnapshot()
	if current.seq <= oldSeq {
		t.Fatalf("expected truncate to publish newer snapshot: old=%d new=%d", oldSeq, current.seq)
	}
	if !current.universe.IsEmpty() {
		t.Fatalf("expected truncate snapshot universe to be empty")
	}
	if pinned.universe.Cardinality() != 2 {
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

	db.unpinSnapshotRef(oldSeq, ref)

	db.snapshot.mu.RLock()
	_, oldPresent := db.snapshot.bySeq[oldSeq]
	registrySize := len(db.snapshot.bySeq)
	db.snapshot.mu.RUnlock()
	if oldPresent {
		t.Fatalf("expected pinned pre-truncate snapshot to be pruned after unpin")
	}
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

	old := db.getSnapshot()
	idx1, ok := old.strmap.getIdxNoLock("k1")
	if !ok || idx1 == 0 {
		t.Fatalf("expected k1 mapping before truncate")
	}
	pinned, ref, ok := db.pinSnapshotRefBySeq(old.seq)
	if !ok || pinned != old {
		t.Fatalf("expected pre-truncate snapshot to be pinnable")
	}

	if err := db.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	current := db.getSnapshot()
	if _, ok := current.strmap.getIdxNoLock("k1"); ok {
		t.Fatalf("current truncated snapshot unexpectedly retained k1 mapping")
	}
	if got, ok := pinned.strmap.getStringNoLock(idx1); !ok || got != "k1" {
		t.Fatalf("pinned pre-truncate snapshot lost k1 mapping: got=%q ok=%v", got, ok)
	}

	db.unpinSnapshotRef(old.seq, ref)
}

func TestSnapshotExt_StringKeyOldSnapshotMappingsSurviveDeleteAndNewKeys(t *testing.T) {
	db, _ := openTempDBString(t, snapshotExtOptions())

	if err := db.Set("k1", &Rec{Name: "one"}); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	if err := db.Set("k2", &Rec{Name: "two"}); err != nil {
		t.Fatalf("Set(k2): %v", err)
	}

	s1 := db.getSnapshot()
	idx1, ok := s1.strmap.getIdxNoLock("k1")
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

	s2 := db.getSnapshot()
	if _, ok := s1.strmap.getIdxNoLock("k3"); ok {
		t.Fatalf("old snapshot unexpectedly sees future string key mapping")
	}
	if got, ok := s1.strmap.getStringNoLock(idx1); !ok || got != "k1" {
		t.Fatalf("old snapshot lost original reverse mapping: got=%q ok=%v", got, ok)
	}
	if _, ok := s2.strmap.getIdxNoLock("k3"); !ok {
		t.Fatalf("new snapshot is missing future string key mapping")
	}
}

func TestSnapshotExt_PatchUnchangedFieldInheritsPositiveMaterializedPredCache(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Email: "alice@example.com", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	key := db.materializedPredCacheKey(qx.Expr{Op: qx.OpPREFIX, Field: "email", Value: "ali"})
	if key == "" {
		t.Fatalf("expected non-empty cache key")
	}

	prev := db.getSnapshot()
	ids := snapshotExtPosting(1)
	prev.storeMaterializedPred(key, ids)

	if err := db.Patch(1, []Field{{Name: "age", Value: 31}}); err != nil {
		t.Fatalf("Patch(age): %v", err)
	}

	next := db.getSnapshot()
	cached, ok := next.loadMaterializedPred(key)
	if !ok || cached != ids {
		t.Fatalf("expected positive untouched cache entry to be inherited")
	}
}

func TestSnapshotExt_PatchUnchangedFieldInheritsNegativeMaterializedPredCache(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Email: "alice@example.com", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	key := db.materializedPredCacheKey(qx.Expr{Op: qx.OpPREFIX, Field: "email", Value: "zz"})
	if key == "" {
		t.Fatalf("expected non-empty cache key")
	}

	prev := db.getSnapshot()
	prev.storeMaterializedPred(key, posting.List{})

	if err := db.Patch(1, []Field{{Name: "age", Value: 31}}); err != nil {
		t.Fatalf("Patch(age): %v", err)
	}

	next := db.getSnapshot()
	cached, ok := next.loadMaterializedPred(key)
	if !ok || !cached.IsEmpty() {
		t.Fatalf("expected negative untouched cache entry to be inherited")
	}
}

func TestSnapshotExt_PatchTouchedFieldDropsOnlyTouchedMaterializedPredCache(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Email: "alice@example.com"}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	nameKey := db.materializedPredCacheKey(qx.Expr{Op: qx.OpPREFIX, Field: "name", Value: "ali"})
	emailKey := db.materializedPredCacheKey(qx.Expr{Op: qx.OpPREFIX, Field: "email", Value: "ali"})
	if nameKey == "" || emailKey == "" {
		t.Fatalf("expected non-empty cache keys")
	}

	prev := db.getSnapshot()
	nameIDs := snapshotExtPosting(1)
	emailIDs := snapshotExtPosting(1)
	prev.storeMaterializedPred(nameKey, nameIDs)
	prev.storeMaterializedPred(emailKey, emailIDs)

	if err := db.Patch(1, []Field{{Name: "name", Value: "alicia"}}); err != nil {
		t.Fatalf("Patch(name): %v", err)
	}

	next := db.getSnapshot()
	if _, ok := next.loadMaterializedPred(nameKey); ok {
		t.Fatalf("expected touched field cache entry to be dropped")
	}
	cached, ok := next.loadMaterializedPred(emailKey)
	if !ok || cached != emailIDs {
		t.Fatalf("expected untouched field cache entry to be inherited")
	}
}

func TestSnapshotExt_PatchDuplicateTouchedFieldStillDropsTouchedCacheAndKeepsOthers(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Email: "alice@example.com"}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	nameKey := db.materializedPredCacheKey(qx.Expr{Op: qx.OpPREFIX, Field: "name", Value: "ali"})
	emailKey := db.materializedPredCacheKey(qx.Expr{Op: qx.OpPREFIX, Field: "email", Value: "ali"})
	if nameKey == "" || emailKey == "" {
		t.Fatalf("expected non-empty cache keys")
	}

	prev := db.getSnapshot()
	emailIDs := snapshotExtPosting(1)
	prev.storeMaterializedPred(nameKey, snapshotExtPosting(1))
	prev.storeMaterializedPred(emailKey, emailIDs)

	if err := db.Patch(1, []Field{
		{Name: "name", Value: "alicia"},
		{Name: "name", Value: "ally"},
	}); err != nil {
		t.Fatalf("Patch(duplicate name): %v", err)
	}

	next := db.getSnapshot()
	if _, ok := next.loadMaterializedPred(nameKey); ok {
		t.Fatalf("expected duplicate touched field cache entry to be dropped")
	}
	cached, ok := next.loadMaterializedPred(emailKey)
	if !ok || cached != emailIDs {
		t.Fatalf("expected duplicate touched patch to keep unrelated cache entry")
	}
}

func TestSnapshotExt_PatchTouchedNilFieldDropsNilPredicateCache(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Email: "alice@example.com", Opt: nil}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	optKey := db.materializedPredCacheKey(qx.Expr{Op: qx.OpPREFIX, Field: "opt", Value: "z"})
	emailKey := db.materializedPredCacheKey(qx.Expr{Op: qx.OpPREFIX, Field: "email", Value: "ali"})
	if optKey == "" || emailKey == "" {
		t.Fatalf("expected non-empty cache keys")
	}

	prev := db.getSnapshot()
	emailIDs := snapshotExtPosting(1)
	prev.storeMaterializedPred(optKey, posting.List{})
	prev.storeMaterializedPred(emailKey, emailIDs)

	val := "zzz"
	if err := db.Patch(1, []Field{{Name: "opt", Value: &val}}); err != nil {
		t.Fatalf("Patch(opt): %v", err)
	}

	next := db.getSnapshot()
	if _, ok := next.loadMaterializedPred(optKey); ok {
		t.Fatalf("expected touched nil-field cache entry to be dropped")
	}
	cached, ok := next.loadMaterializedPred(emailKey)
	if !ok || cached != emailIDs {
		t.Fatalf("expected untouched cache entry to survive opt patch")
	}
}

func TestSnapshotExt_ClearRuntimeCachesForTestingClearsCurrentSnapshotCaches(t *testing.T) {
	opts := snapshotExtOptions()
	opts.SnapshotMaterializedPredCacheMaxEntries = 64

	db, _ := openTempDBUint64(t, opts)
	seedGeneratedUint64Data(t, db, 2_000, func(i int) *Rec {
		return &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Age:   i,
			Score: float64(i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	setNumericBucketKnobs(t, db, 128, 1, 1)
	snap := db.getSnapshot()
	snapshotExtEvalNumericRangeBuckets(t, db, qx.Expr{Op: qx.OpGTE, Field: "age", Value: 500})
	if snap.numericRangeBucketCache == nil || snapshotExtSyncMapLen(snap.numericRangeBucketCache) == 0 {
		t.Fatalf("expected numeric range bucket cache entries after evaluation")
	}
	expr := qx.Expr{Op: qx.OpPREFIX, Field: "name", Value: "u_1"}
	cacheKey := db.materializedPredCacheKey(expr)
	if cacheKey == "" {
		t.Fatalf("expected non-empty materialized predicate cache key")
	}
	out, err := db.evalSimple(expr)
	if err != nil {
		t.Fatalf("evalSimple(prefix): %v", err)
	}
	out.release()
	if _, ok := snap.loadMaterializedPred(cacheKey); !ok {
		t.Fatalf("expected materialized predicate cache entry after evalSimple")
	}
	if got := snap.matPredCacheCount.Load(); got == 0 {
		t.Fatalf("expected materialized predicate cache count > 0")
	}

	db.clearCurrentSnapshotCachesForTesting()

	if db.getSnapshot() != snap {
		t.Fatalf("expected cache clear to keep current snapshot published")
	}
	if got := snapshotExtSyncMapLen(snap.numericRangeBucketCache); got != 0 {
		t.Fatalf("expected numeric range bucket cache to be empty after clear, got=%d", got)
	}
	if got := snapshotExtSyncMapLen(&snap.matPredCache); got != 0 {
		t.Fatalf("expected materialized predicate cache to be empty after clear, got=%d", got)
	}
	if got := snap.matPredCacheCount.Load(); got != 0 {
		t.Fatalf("expected materialized predicate cache count reset, got=%d", got)
	}
	if got := snap.matPredCacheOversizedCount.Load(); got != 0 {
		t.Fatalf("expected oversized materialized predicate cache count reset, got=%d", got)
	}
	if _, ok := snap.loadMaterializedPred(cacheKey); ok {
		t.Fatalf("expected cleared materialized predicate cache entry to be absent")
	}
}

func TestSnapshotExt_StoreMaterializedPredConcurrentDistinctKeysRespectsLimit(t *testing.T) {
	n := max(16, runtime.GOMAXPROCS(0)*8)
	for round := 0; round < 16; round++ {
		s := &indexSnapshot{matPredCacheMaxEntries: 1}
		snapshotExtRunConcurrent(n, func(i int) {
			s.storeMaterializedPred(fmt.Sprintf("email\x1f%d\x1f%d", qx.OpPREFIX, i), posting.List{})
		})
		if got := s.matPredCacheCount.Load(); got > 1 {
			t.Fatalf("round=%d materialized cache count exceeded limit: got=%d", round, got)
		}
		if got := snapshotExtSyncMapLen(&s.matPredCache); got > 1 {
			t.Fatalf("round=%d materialized cache entries exceeded limit: got=%d", round, got)
		}
	}
}

func TestSnapshotExt_StoreMaterializedPredConcurrentSameKeyStoresSingleEntry(t *testing.T) {
	n := max(16, runtime.GOMAXPROCS(0)*8)
	s := &indexSnapshot{matPredCacheMaxEntries: 8}

	snapshotExtRunConcurrent(n, func(i int) {
		s.storeMaterializedPred("email\x1f1\x1fsame", snapshotExtPosting(uint64(i+1)))
	})

	if got := s.matPredCacheCount.Load(); got != 1 {
		t.Fatalf("expected single cache count for duplicate key, got=%d", got)
	}
	if got := snapshotExtSyncMapLen(&s.matPredCache); got != 1 {
		t.Fatalf("expected single cache entry for duplicate key, got=%d", got)
	}
}

func TestSnapshotExt_TryStoreMaterializedPredOversizedConcurrentDistinctKeysRespectsTotalLimit(t *testing.T) {
	n := max(16, runtime.GOMAXPROCS(0)*8)
	bm := snapshotExtPosting(1, 2)
	for round := 0; round < 16; round++ {
		s := &indexSnapshot{
			matPredCacheMaxEntries: 1,
			matPredCacheMaxCard:    1,
		}
		snapshotExtRunConcurrent(n, func(i int) {
			s.tryStoreMaterializedPredOversized(fmt.Sprintf("age\x1frange_bucket\x1f%d", i), bm)
		})
		if got := s.matPredCacheCount.Load(); got > 1 {
			t.Fatalf("round=%d oversized cache count exceeded limit: got=%d", round, got)
		}
		if got := snapshotExtSyncMapLen(&s.matPredCache); got > 1 {
			t.Fatalf("round=%d oversized cache entries exceeded limit: got=%d", round, got)
		}
		if got := s.matPredCacheOversizedCount.Load(); got > 1 {
			t.Fatalf("round=%d oversized counter exceeded total limit: got=%d", round, got)
		}
	}
}

func TestSnapshotExt_TryStoreMaterializedPredOversizedConcurrentSameKeyDoesNotLeakCounters(t *testing.T) {
	n := max(16, runtime.GOMAXPROCS(0)*8)
	bm := snapshotExtPosting(1, 2)
	s := &indexSnapshot{
		matPredCacheMaxEntries: 8,
		matPredCacheMaxCard:    1,
	}

	snapshotExtRunConcurrent(n, func(i int) {
		s.tryStoreMaterializedPredOversized("age\x1frange_bucket\x1fsame", bm)
	})

	if got := s.matPredCacheCount.Load(); got != 1 {
		t.Fatalf("expected single oversized cache count for duplicate key, got=%d", got)
	}
	if got := snapshotExtSyncMapLen(&s.matPredCache); got != 1 {
		t.Fatalf("expected single oversized cache entry for duplicate key, got=%d", got)
	}
	if got := s.matPredCacheOversizedCount.Load(); got != 1 {
		t.Fatalf("expected oversized counter to settle at 1, got=%d", got)
	}
}

func TestSnapshotExt_MaterializedPredCacheOversizedAdmissionTurnsOverEntries(t *testing.T) {
	s := &indexSnapshot{
		matPredCacheMaxEntries: 8,
		matPredCacheMaxCard:    1,
	}

	small := snapshotExtPosting(1)
	large := snapshotExtPosting(1, 2)

	for i := 0; i < 8; i++ {
		s.storeMaterializedPred(fmt.Sprintf("email\x1f%d\x1f%d", qx.OpPREFIX, i), small)
	}
	if got := s.matPredCacheCount.Load(); got != 8 {
		t.Fatalf("expected regular cache to fill total limit, got=%d", got)
	}
	if got := snapshotExtSyncMapLen(&s.matPredCache); got != 8 {
		t.Fatalf("expected eight regular cache entries before oversized admission, got=%d", got)
	}

	if !s.tryStoreMaterializedPredOversized("age\x1frange_bucket\x1f0", large) {
		t.Fatalf("expected first oversized cache entry to replace an older entry")
	}
	if !s.tryStoreMaterializedPredOversized("age\x1frange_bucket\x1f1", large) {
		t.Fatalf("expected second oversized cache entry to replace an older entry")
	}
	if !s.tryStoreMaterializedPredOversized("age\x1frange_bucket\x1f2", large) {
		t.Fatalf("expected oversized cache admission to replace an older oversized entry at limit")
	}

	if got := s.matPredCacheCount.Load(); got != 8 {
		t.Fatalf("expected total cache count to remain bounded by limit, got=%d", got)
	}
	if got := snapshotExtSyncMapLen(&s.matPredCache); got != 8 {
		t.Fatalf("expected total cache entries to remain bounded by limit, got=%d", got)
	}
	if got := s.matPredCacheOversizedCount.Load(); got != 2 {
		t.Fatalf("expected oversized counter to settle at 2, got=%d", got)
	}
	if _, ok := s.loadMaterializedPred("age\x1frange_bucket\x1f2"); !ok {
		t.Fatalf("expected newest oversized entry to remain cached")
	}
	kept := 0
	for _, key := range []string{
		"age\x1frange_bucket\x1f0",
		"age\x1frange_bucket\x1f1",
		"age\x1frange_bucket\x1f2",
	} {
		if _, ok := s.loadMaterializedPred(key); ok {
			kept++
		}
	}
	if kept != 2 {
		t.Fatalf("expected oversized cache to keep exactly two entries after replacement, got=%d", kept)
	}
}

func TestSnapshotExt_MaterializedPredCacheRegularAdmissionTurnsOverOlderRegularEntries(t *testing.T) {
	s := &indexSnapshot{
		matPredCacheMaxEntries: 8,
		matPredCacheMaxCard:    1,
	}

	small := snapshotExtPosting(1)
	large := snapshotExtPosting(1, 2)

	for i := 0; i < 8; i++ {
		s.storeMaterializedPred(fmt.Sprintf("email\x1f%d\x1f%d", qx.OpPREFIX, i), small)
	}
	if !s.tryStoreMaterializedPredOversized("age\x1frange_bucket\x1f0", large) {
		t.Fatalf("expected first oversized cache entry to be admitted")
	}
	if !s.tryStoreMaterializedPredOversized("age\x1frange_bucket\x1f1", large) {
		t.Fatalf("expected second oversized cache entry to be admitted")
	}

	s.storeMaterializedPred("email\x1f1\x1flate", small)

	if got := s.matPredCacheCount.Load(); got != 8 {
		t.Fatalf("expected total cache count to remain bounded by limit, got=%d", got)
	}
	if got := snapshotExtSyncMapLen(&s.matPredCache); got != 8 {
		t.Fatalf("expected total cache entries to remain bounded by limit, got=%d", got)
	}
	if got := s.matPredCacheOversizedCount.Load(); got != 2 {
		t.Fatalf("expected regular turnover to keep oversized entries resident, got=%d", got)
	}
	if _, ok := s.loadMaterializedPred("email\x1f1\x1flate"); !ok {
		t.Fatalf("expected newest regular entry to replace an older regular entry")
	}
}

func TestSnapshotExt_InheritMaterializedPredCacheSkipsChangedFieldsAndKeepsOthers(t *testing.T) {
	prev := &indexSnapshot{matPredCacheMaxEntries: 8}
	nameIDs := snapshotExtPosting(1)
	emailIDs := snapshotExtPosting(2)
	prev.matPredCache.Store("name\x1f1\x1fa", &materializedPredCacheEntry{ids: nameIDs})
	prev.matPredCache.Store("email\x1f1\x1fb", &materializedPredCacheEntry{ids: emailIDs})
	prev.matPredCacheCount.Store(2)

	next := &indexSnapshot{matPredCacheMaxEntries: 8}
	inheritMaterializedPredCache(next, prev, map[string]struct{}{
		"name": {},
	})

	if _, ok := next.loadMaterializedPred("name\x1f1\x1fa"); ok {
		t.Fatalf("expected changed-field cache entry to be skipped")
	}
	cached, ok := next.loadMaterializedPred("email\x1f1\x1fb")
	if !ok || cached != emailIDs {
		t.Fatalf("expected untouched-field cache entry to be inherited")
	}
}

func TestSnapshotExt_InheritMaterializedPredCacheSkipsMalformedKeysAndBadEntries(t *testing.T) {
	prev := &indexSnapshot{matPredCacheMaxEntries: 8}
	prev.matPredCache.Store("", &materializedPredCacheEntry{ids: snapshotExtPosting(1)})
	prev.matPredCache.Store("\x1fbroken", &materializedPredCacheEntry{ids: snapshotExtPosting(1)})
	prev.matPredCache.Store(123, &materializedPredCacheEntry{ids: snapshotExtPosting(1)})
	prev.matPredCache.Store("email\x1f1\x1fa", "bad")
	prev.matPredCache.Store("name\x1f1\x1fb", &materializedPredCacheEntry{ids: posting.List{}})
	prev.matPredCacheCount.Store(5)

	next := &indexSnapshot{matPredCacheMaxEntries: 8}
	inheritMaterializedPredCache(next, prev, nil)

	if got := next.matPredCacheCount.Load(); got != 1 {
		t.Fatalf("expected exactly one valid inherited entry, got=%d", got)
	}
	cached, ok := next.loadMaterializedPred("name\x1f1\x1fb")
	if !ok || !cached.IsEmpty() {
		t.Fatalf("expected valid negative cache entry to survive malformed-entry filtering")
	}
}

func TestSnapshotExt_InheritMaterializedPredCacheAllowsTypedNilEntries(t *testing.T) {
	prev := &indexSnapshot{matPredCacheMaxEntries: 8}
	var nilEntry *materializedPredCacheEntry
	prev.matPredCache.Store("name\x1f1\x1fa", nilEntry)
	prev.matPredCacheCount.Store(1)

	next := &indexSnapshot{matPredCacheMaxEntries: 8}
	inheritMaterializedPredCache(next, prev, nil)

	if got := next.matPredCacheCount.Load(); got != 1 {
		t.Fatalf("expected typed-nil entry to be inherited as negative cache entry, got=%d", got)
	}
	cached, ok := next.loadMaterializedPred("name\x1f1\x1fa")
	if !ok || !cached.IsEmpty() {
		t.Fatalf("expected typed-nil entry to survive as empty negative cache entry")
	}
}

func TestSnapshotExt_InheritMaterializedPredCacheClampsOversizedCount(t *testing.T) {
	prev := &indexSnapshot{matPredCacheMaxEntries: 8}
	oversized := snapshotExtPosting(1, 2)
	prev.matPredCache.Store("a\x1f1\x1f1", &materializedPredCacheEntry{ids: oversized})
	prev.matPredCache.Store("b\x1f1\x1f2", &materializedPredCacheEntry{ids: oversized})
	prev.matPredCache.Store("c\x1f1\x1f3", &materializedPredCacheEntry{ids: oversized})
	prev.matPredCacheCount.Store(3)

	next := &indexSnapshot{
		matPredCacheMaxEntries: 8,
		matPredCacheMaxCard:    1,
	}
	inheritMaterializedPredCache(next, prev, nil)

	if got := next.matPredCacheCount.Load(); got != 3 {
		t.Fatalf("expected all entries to be inherited before oversized clamp, got=%d", got)
	}
	if want, got := materializedPredCacheOversizedLimit(next.matPredCacheMaxEntries), next.matPredCacheOversizedCount.Load(); got != want {
		t.Fatalf("expected oversized counter clamp to %d, got=%d", want, got)
	}
}

func TestSnapshotExt_InheritMaterializedPredCachePreservesRecencyStamps(t *testing.T) {
	prev := &indexSnapshot{matPredCacheMaxEntries: 8}
	a := &materializedPredCacheEntry{ids: snapshotExtPosting(1)}
	a.stamp.Store(7)
	b := &materializedPredCacheEntry{ids: snapshotExtPosting(2)}
	b.stamp.Store(13)
	prev.matPredCache.Store("email\x1f1\x1fa", a)
	prev.matPredCache.Store("name\x1f1\x1fb", b)
	prev.matPredCacheCount.Store(2)
	prev.matPredCacheClock.Store(13)

	next := &indexSnapshot{matPredCacheMaxEntries: 8}
	inheritMaterializedPredCache(next, prev, nil)

	va, ok := next.matPredCache.Load("email\x1f1\x1fa")
	if !ok {
		t.Fatalf("expected first cache entry to be inherited")
	}
	gotA, _ := va.(*materializedPredCacheEntry)
	if gotA == nil || gotA.stamp.Load() != 7 {
		t.Fatalf("expected first inherited stamp to stay at 7, got=%d", gotA.stamp.Load())
	}

	vb, ok := next.matPredCache.Load("name\x1f1\x1fb")
	if !ok {
		t.Fatalf("expected second cache entry to be inherited")
	}
	gotB, _ := vb.(*materializedPredCacheEntry)
	if gotB == nil || gotB.stamp.Load() != 13 {
		t.Fatalf("expected second inherited stamp to stay at 13, got=%d", gotB.stamp.Load())
	}

	if got := next.matPredCacheClock.Load(); got != 13 {
		t.Fatalf("expected next snapshot clock to continue from max inherited stamp, got=%d", got)
	}
}

func TestSnapshotExt_InheritNumericRangeBucketCacheCopiesOnlyMatchingStorage(t *testing.T) {
	shared := snapshotExtStorage("10", "20")
	prevChanged := snapshotExtStorage("30")
	nextChanged := snapshotExtStorage("30")

	ageEntry := &numericRangeBucketCacheEntry{
		storage: shared,
		idx: &numericRangeBucketIndex{
			bucketSize: 1,
			buckets:    []numericRangeBucket{{start: 0, end: 1}},
		},
	}
	scoreEntry := &numericRangeBucketCacheEntry{
		storage: prevChanged,
		idx: &numericRangeBucketIndex{
			bucketSize: 1,
			buckets:    []numericRangeBucket{{start: 0, end: 1}},
		},
	}

	prev := &indexSnapshot{
		index:                   map[string]fieldIndexStorage{"age": shared, "score": prevChanged},
		numericRangeBucketCache: newNumericRangeBucketCache(),
	}
	prev.numericRangeBucketCache.Store("age", ageEntry)
	prev.numericRangeBucketCache.Store("score", scoreEntry)

	next := &indexSnapshot{
		index: map[string]fieldIndexStorage{"age": shared, "score": nextChanged},
	}
	inheritNumericRangeBucketCache(next, prev)

	if next.numericRangeBucketCache == nil {
		t.Fatalf("expected next snapshot to own a numeric range cache map")
	}
	if got := snapshotExtSyncMapLen(next.numericRangeBucketCache); got != 1 {
		t.Fatalf("expected exactly one inherited numeric range entry, got=%d", got)
	}
	raw, ok := next.numericRangeBucketCache.Load("age")
	if !ok || raw != ageEntry {
		t.Fatalf("expected matching-storage entry to be inherited by pointer")
	}
	if _, ok := next.numericRangeBucketCache.Load("score"); ok {
		t.Fatalf("expected changed-storage entry to be skipped")
	}
}

func TestSnapshotExt_InheritNumericRangeBucketCacheSkipsMalformedAndEmptyEntries(t *testing.T) {
	valid := snapshotExtStorage("10")
	validEntry := &numericRangeBucketCacheEntry{
		storage: valid,
		idx: &numericRangeBucketIndex{
			bucketSize: 1,
			buckets:    []numericRangeBucket{{start: 0, end: 1}},
		},
	}

	prev := &indexSnapshot{
		index:                   map[string]fieldIndexStorage{"age": valid},
		numericRangeBucketCache: newNumericRangeBucketCache(),
	}
	prev.numericRangeBucketCache.Store("", validEntry)
	prev.numericRangeBucketCache.Store(123, validEntry)
	prev.numericRangeBucketCache.Store("score", &numericRangeBucketCacheEntry{
		storage: fieldIndexStorage{},
		idx: &numericRangeBucketIndex{
			bucketSize: 1,
			buckets:    []numericRangeBucket{{start: 0, end: 1}},
		},
	})
	prev.numericRangeBucketCache.Store("age", validEntry)

	next := &indexSnapshot{
		index: map[string]fieldIndexStorage{"age": valid},
	}
	inheritNumericRangeBucketCache(next, prev)

	if next.numericRangeBucketCache == nil {
		t.Fatalf("expected next snapshot to own numeric cache map")
	}
	if got := snapshotExtSyncMapLen(next.numericRangeBucketCache); got != 1 {
		t.Fatalf("expected malformed/empty entries to be filtered, got=%d", got)
	}
	raw, ok := next.numericRangeBucketCache.Load("age")
	if !ok || raw != validEntry {
		t.Fatalf("expected only valid age entry to survive filtering")
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

	old := db.getSnapshot()
	if old == nil || old.strmap == nil {
		t.Fatalf("expected published string-key snapshot")
	}

	var want []string
	iterOld := old.universe.Iter()
	defer iterOld.Release()
	for iterOld.HasNext() {
		idx := iterOld.Next()
		key, ok := old.strmap.getStringNoLock(idx)
		if !ok {
			t.Fatalf("old snapshot lost key mapping for idx=%d", idx)
		}
		want = append(want, key)
	}
	if !slices.Equal(want, []string{"k1", "k2"}) {
		t.Fatalf("unexpected old snapshot key order: %v", want)
	}

	universe := db.snapshotUniverseView()
	iter := universe.Iter()
	defer iter.Release()

	if err := db.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	var got []string
	err := db.scanStringKeys(old.strmap, universe, iter, "", func(id string) (bool, error) {
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

	old := db.getSnapshot()
	universe := db.snapshotUniverseView()
	iter := universe.Iter()
	defer iter.Release()

	if err := db.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if err := db.Set("future", &Rec{Name: "future"}); err != nil {
		t.Fatalf("Set(future): %v", err)
	}

	var got []string
	err := db.scanStringKeys(old.strmap, universe, iter, "", func(id string) (bool, error) {
		got = append(got, id)
		return true, nil
	})
	if err != nil {
		t.Fatalf("split snapshot emitted inconsistent string scan state: err=%v got=%v oldSeq=%d currentSeq=%d", err, got, old.seq, db.getSnapshot().seq)
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

	before := db.getSnapshot()
	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- db.Truncate()
	}()

	alice, err := db.queryRecords(tx, snap, qx.Query(qx.EQ("name", "alice")))
	if err != nil {
		t.Fatalf("queryRecords(old snapshot alice): %v", err)
	}
	if len(alice) != 1 || alice[0] == nil || alice[0].Name != "alice" {
		t.Fatalf("unexpected old snapshot alice result: %#v", alice)
	}

	all, err := db.queryRecords(tx, snap, qx.Query())
	if err != nil {
		t.Fatalf("queryRecords(old snapshot all): %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("old tx/snapshot lost rows across truncate: %#v", all)
	}

	rollback(tx)
	db.unpinSnapshotRef(seq, ref)

	if err := <-done; err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	after := db.getSnapshot()
	if after.seq <= before.seq {
		t.Fatalf("truncate did not publish newer snapshot: before=%d after=%d", before.seq, after.seq)
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

	before := db.getSnapshot()
	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}

	db.testHooks.afterCommitPublish = func(op string) {
		if op == "truncate" {
			panic("failpoint: publish truncate")
		}
	}
	t.Cleanup(func() {
		db.testHooks.afterCommitPublish = nil
	})

	done := make(chan error, 1)
	go func() {
		done <- db.Truncate()
	}()

	all, err := db.queryRecords(tx, snap, qx.Query())
	if err != nil {
		t.Fatalf("queryRecords(old snapshot all): %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("old tx/snapshot lost rows during broken truncate publish: %#v", all)
	}

	rollback(tx)
	db.unpinSnapshotRef(seq, ref)

	err = <-done
	if !errors.Is(err, ErrBroken) {
		t.Fatalf("expected ErrBroken from truncate publish failpoint, got: %v", err)
	}

	assertNoFutureSnapshotRefs(t, db)
	if got := db.getSnapshot(); got != before {
		t.Fatalf("broken truncate publish should keep previously published snapshot current")
	}
}

func TestSpanshotExt_MaterializedPredMixedRegularAndOversizedConcurrentRespectsGlobalLimit(t *testing.T) {
	n := max(16, runtime.GOMAXPROCS(0)*8)
	small := snapshotExtPosting(1)
	large := snapshotExtPosting(1, 2)

	for round := 0; round < 32; round++ {
		s := &indexSnapshot{
			matPredCacheMaxEntries: 1,
			matPredCacheMaxCard:    1,
		}
		snapshotExtRunConcurrent(n, func(i int) {
			if i%2 == 0 {
				s.storeMaterializedPred(fmt.Sprintf("email\x1f%d\x1f%d", qx.OpPREFIX, i), small)
				return
			}
			s.tryStoreMaterializedPredOversized(fmt.Sprintf("age\x1frange_bucket\x1f%d", i), large)
		})

		if got := s.matPredCacheCount.Load(); got > 1 {
			t.Fatalf("round=%d mixed cache count exceeded global limit: got=%d", round, got)
		}
		if got := snapshotExtSyncMapLen(&s.matPredCache); got > 1 {
			t.Fatalf("round=%d mixed cache entries exceeded global limit: got=%d", round, got)
		}
	}
}

func TestIndexSnapshotMaterializedPredCacheDetachedLoadsUnderConcurrency(t *testing.T) {
	snap := &indexSnapshot{matPredCacheMaxEntries: 8}

	base := snapshotExtPosting()
	for i := uint64(1); i <= 40; i++ {
		base.Add(i * 5)
	}
	want := base.ToArray()

	snap.storeMaterializedPred("email\x1f1\xfal", base.Borrow())
	base.Release()

	remove := snapshotExtPosting(want[0], want[1], want[2])
	add := snapshotExtPosting(1<<32|3, 1<<32|7)
	defer remove.Release()
	defer add.Release()

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

	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 1000; i++ {
				cached, ok := snap.loadMaterializedPred("email\x1f1\xfal")
				if !ok {
					setFailed("cache entry unexpectedly missing")
					return
				}
				if !slices.Equal(cached.ToArray(), want) {
					setFailed(fmt.Sprintf("cached view mismatch: got=%v want=%v", cached.ToArray(), want))
					return
				}
				cached.Release()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 300; i++ {
			cached, ok := snap.loadMaterializedPred("email\x1f1\xfal")
			if !ok {
				setFailed("writer load unexpectedly missed cache")
				return
			}
			cached.AndNotInPlace(remove)
			cached.OrInPlace(add)
			cached.Add(6<<32 | uint64(i))
			cached.Optimize()
			cached.Release()
		}
	}()

	close(start)
	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}

	cached, ok := snap.loadMaterializedPred("email\x1f1\xfal")
	if !ok {
		t.Fatalf("cache entry missing after concurrent mutation")
	}
	defer cached.Release()
	assertPostingConsumerSet(t, cached, want)
}

func TestApplyBatchPostingDeltaOwnedDetachedBorrowedBase(t *testing.T) {
	base := snapshotExtPosting()
	for i := uint64(1); i <= 48; i++ {
		base.Add(i * 2)
	}
	wantBase := base.ToArray()

	removeIDs := []uint64{wantBase[0], wantBase[1], wantBase[2], wantBase[3]}
	addIDs := []uint64{1<<32 | 5, 1<<32 | 9, 2<<32 | 11}

	expected := make([]uint64, 0, len(wantBase))
	removeSet := make(map[uint64]struct{}, len(removeIDs))
	for _, id := range removeIDs {
		removeSet[id] = struct{}{}
	}
	for _, id := range wantBase {
		if _, drop := removeSet[id]; !drop {
			expected = append(expected, id)
		}
	}
	expected = append(expected, addIDs...)
	slices.Sort(expected)

	var failed atomic.Pointer[string]
	setFailed := func(msg string) {
		if failed.Load() != nil {
			return
		}
		copyMsg := msg
		failed.CompareAndSwap(nil, &copyMsg)
	}

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				delta := batchPostingDelta{
					remove: snapshotExtPosting(removeIDs...),
					add:    snapshotExtPosting(addIDs...),
				}
				out := applyBatchPostingDeltaOwned(base.Borrow(), &delta)
				if !slices.Equal(out.ToArray(), expected) {
					setFailed(fmt.Sprintf("delta result mismatch: got=%v want=%v", out.ToArray(), expected))
					out.Release()
					return
				}
				if !delta.add.IsEmpty() || !delta.remove.IsEmpty() {
					setFailed("applyBatchPostingDeltaOwned must consume delta buffers")
					out.Release()
					return
				}
				out.Release()
			}
		}()
	}

	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}

	defer base.Release()
	assertPostingConsumerSet(t, base, wantBase)
}

func assertPostingConsumerSet(t *testing.T, got posting.List, want []uint64) {
	t.Helper()
	gotIDs := got.ToArray()
	if !slices.Equal(gotIDs, want) {
		t.Fatalf("posting mismatch: got=%v want=%v", gotIDs, want)
	}
}
