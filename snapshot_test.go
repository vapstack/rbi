package rbi

import (
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/schema"
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

func TestSnapshotSequence_PublishedOnWrite(t *testing.T) {
	db, _ := openTempDBUint64(t)

	before := db.engine.getSnapshot().seq

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	after := db.engine.getSnapshot().seq
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

	oldSequence := db.engine.getSnapshot().seq

	if err := db.Set(2, &Rec{Name: "new", Age: 20}); err != nil {
		t.Fatalf("concurrent Set: %v", err)
	}

	latest := db.engine.getSnapshot().seq
	if latest <= oldSequence {
		t.Fatalf("latest sequence did not advance: old=%d latest=%d", oldSequence, latest)
	}

	if _, _, ok := db.engine.snapshot.pinRefBySeq(oldSequence); ok {
		t.Fatalf("previous snapshot unexpectedly remained pinable: sequence=%d", oldSequence)
	}
}

func TestSnapshotSequence_AdvancesOnWrite(t *testing.T) {
	db, _ := openTempDBUint64(t)

	before := db.engine.getSnapshot().seq
	if err := db.Set(1, &Rec{Name: "x", Age: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	after := db.engine.getSnapshot().seq
	if after <= before {
		t.Fatalf("snapshot sequence did not advance on write: before=%d after=%d", before, after)
	}
}

func TestSnapshot_PublishedAsFullState(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Tags: []string{"go", "db"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	s := db.engine.getSnapshot()
	nameIDs := s.fieldLookupPostingRetained("name", "alice")
	snapshotPostingContainsAll(t, nameIDs, 1)

	tagsIDs := s.fieldLookupPostingRetained("tags", "go")
	snapshotPostingContainsAll(t, tagsIDs, 1)

	lenIDs := db.engine.currentQueryViewForTests().lenFieldOverlay("tags").LookupPostingRetained(keycodec.U64ByteString(2))
	snapshotPostingContainsAll(t, lenIDs, 1)
}

func TestSnapshot_PreviousSnapshotRemainsImmutable(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice"}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	s1 := db.engine.getSnapshot()

	if err := db.Set(2, &Rec{Name: "bob"}); err != nil {
		t.Fatalf("second Set: %v", err)
	}
	s2 := db.engine.getSnapshot()
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
	s1 := db.engine.getSnapshot()
	idx1, ok := s1.strmap.Index("k1")
	if !ok || idx1 == 0 {
		t.Fatalf("expected k1 in first snapshot")
	}

	if err := db.Set("k2", &Rec{Name: "two"}); err != nil {
		t.Fatalf("Set(k2): %v", err)
	}
	s2 := db.engine.getSnapshot()
	if _, ok := s1.strmap.Index("k2"); ok {
		t.Fatalf("old snapshot unexpectedly sees k2 mapping")
	}

	idx2, ok := s2.strmap.Index("k2")
	if !ok || idx2 == 0 {
		t.Fatalf("expected k2 in latest snapshot")
	}
	if got, ok := s1.strmap.String(idx1); !ok || got != "k1" {
		t.Fatalf("old snapshot lost k1 mapping: got=%q ok=%v", got, ok)
	}
	if got, ok := s2.strmap.String(idx2); !ok || got != "k2" {
		t.Fatalf("latest snapshot missing k2 mapping: got=%q ok=%v", got, ok)
	}
}

func TestSnapshotStrMap_LatestSnapshotRetainsOldMappingsAcrossChain(t *testing.T) {
	db, _ := openTempDBString(t)

	if err := db.Set("k1", &Rec{Name: "one"}); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	idx1, ok := db.engine.getSnapshot().strmap.Index("k1")
	if !ok || idx1 == 0 {
		t.Fatalf("expected k1 mapping in first snapshot")
	}

	for i := 2; i <= 12; i++ {
		key := fmt.Sprintf("k%d", i)
		if err := db.Set(key, &Rec{Name: key}); err != nil {
			t.Fatalf("Set(%s): %v", key, err)
		}
	}

	latest := db.engine.getSnapshot().strmap
	if got, ok := latest.Index("k1"); !ok || got != idx1 {
		t.Fatalf("latest snapshot lost k1 idx: got=%d ok=%v want=%d", got, ok, idx1)
	}
	if got, ok := latest.String(idx1); !ok || got != "k1" {
		t.Fatalf("latest snapshot lost k1 reverse mapping: got=%q ok=%v", got, ok)
	}
}

func TestSnapshot_EmptyBaseWriteInvalidatesTouchedMaterializedPredicateCache(t *testing.T) {
	db, _ := openTempDBUint64(t)

	expr := qx.PREFIX("email", "user")
	cacheKey := db.engine.materializedPredCacheKey(expr)
	if cacheKey == "" {
		t.Fatalf("expected materialized predicate cache key for prefix predicate")
	}

	emptySnap := db.engine.getSnapshot()
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

	if _, ok := db.engine.getSnapshot().loadMaterializedPred(cacheKey); ok {
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

	s := db.engine.getSnapshot()
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
	oldSeq := db.engine.getSnapshot().seq

	if _, ref, ok := db.engine.snapshot.pinRefBySeq(oldSeq); !ok {
		t.Fatalf("expected to pin previous snapshot")
	} else {
		if err := db.Set(2, &Rec{Name: "next", Age: 20}); err != nil {
			t.Fatalf("second Set: %v", err)
		}

		latestSeq := db.engine.getSnapshot().seq
		db.engine.snapshot.mu.RLock()
		_, oldPresentWhilePinned := db.engine.snapshot.bySeq[oldSeq]
		registryWhilePinned := len(db.engine.snapshot.bySeq)
		db.engine.snapshot.mu.RUnlock()
		if !oldPresentWhilePinned || registryWhilePinned < 2 {
			t.Fatalf("expected pinned snapshot to keep registry entry alive before unpin")
		}

		db.engine.snapshot.unpinRef(oldSeq, ref)

		db.engine.snapshot.mu.RLock()
		_, oldPresentAfterUnpin := db.engine.snapshot.bySeq[oldSeq]
		_, latestPresent := db.engine.snapshot.bySeq[latestSeq]
		registryAfterUnpin := len(db.engine.snapshot.bySeq)
		db.engine.snapshot.mu.RUnlock()

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

func TestSnapshotReleaseStorage_SkipsLiveSharedFlatRoot(t *testing.T) {
	shared := snapshotExtPosting()
	for i := uint64(0); i < 96; i++ {
		shared = shared.BuildAdded((i << 1) + 1)
	}
	sharedMap := indexdata.GetPostingMap()
	sharedMap["shared"] = shared
	sharedStorage := indexdata.NewFlatFieldStorageFromPostingMapOwned(sharedMap, false)

	old := snapshotTestNewSnapshot(map[string]indexdata.FieldStorage{"f": sharedStorage}, nil, nil, nil)
	old.seq = 1
	current := snapshotTestNewSnapshot(map[string]indexdata.FieldStorage{"f": sharedStorage}, nil, nil, nil)
	current.seq = 2
	current.retainSharedOwnedStorageFrom(old)

	oldRef := &snapshotRef{snap: old}
	currentRef := &snapshotRef{snap: current}
	db := &DB[uint64, struct{}]{
		engine: &queryEngine{
			snapshot: new(snapshotManager),
		},
	}
	db.engine.snapshot.bySeq = map[uint64]*snapshotRef{
		old.seq:     oldRef,
		current.seq: currentRef,
	}
	db.engine.snapshot.current.Store(current)
	db.engine.snapshot.currentRef.Store(currentRef)

	db.engine.snapshot.mu.Lock()
	retired := db.engine.snapshot.releaseRetiredSnapshotRefLocked(old.seq, oldRef)
	db.engine.snapshot.mu.Unlock()
	releaseRetiredSnapshots(retired)
	if !snapshotExtFieldContainsID(t, current, "f", "shared", 1) {
		t.Fatalf("current snapshot lost shared posting after old prune")
	}
	if !snapshotExtFieldContainsID(t, current, "f", "shared", 191) {
		t.Fatalf("current snapshot shared posting corrupted after old prune")
	}

	shared.Release()
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

	old := db.engine.getSnapshot()
	if old.universeOwner == nil {
		t.Fatal("expected from-empty published snapshot to own universe")
	}
	pinned, ref, ok := db.engine.snapshot.pinRefBySeq(old.seq)
	if !ok || pinned != old {
		t.Fatal("expected seeded snapshot to be pinnable")
	}
	defer db.engine.snapshot.unpinRef(old.seq, ref)

	expect := slices.Clone(keys)
	checkPinned := func() {
		t.Helper()
		iter := pinned.universe.Iter()
		defer iter.Release()

		got := make([]string, 0, len(expect))
		if err := db.scanStringKeys(pinned.strmap, pinned.universe, iter, "", func(id string) (bool, error) {
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

func snapshotExtNewMaterializedPredSnapshot(limit int, maxCardinality uint64) *indexSnapshot {
	s := &indexSnapshot{}
	if limit > 0 {
		s.matPredCache = qcache.GetMaterializedPredCache(limit, maxCardinality)
	}
	return s
}

func snapshotExtMaterializedPredEntryCount(s *indexSnapshot) int {
	if s == nil || s.matPredCache == nil {
		return 0
	}
	return s.matPredCache.EntryCount()
}

func snapshotExtStorage(keys ...string) indexdata.FieldStorage {
	if len(keys) == 0 {
		return indexdata.FieldStorage{}
	}
	m := indexdata.GetPostingMap()
	for i, key := range keys {
		m[key] = (posting.List{}).BuildAdded(uint64(i + 1))
	}
	return indexdata.NewFlatFieldStorageFromPostingMapOwned(m, false)
}

func snapshotExtFieldContainsID(tb testing.TB, s *indexSnapshot, field, key string, id uint64) bool {
	tb.Helper()
	return s.fieldLookupPostingRetained(field, key).Contains(id)
}

func snapshotExtNilContainsID(tb testing.TB, s *indexSnapshot, field string, id uint64) bool {
	tb.Helper()
	acc := s.indexedFieldByName[field]
	return indexdata.NewFieldOverlayStorage(s.nilIndex[acc.Ordinal]).LookupPostingRetained(nilIndexEntryKey).Contains(id)
}

func snapshotExtLenContainsID(tb testing.TB, s *indexSnapshot, field string, ln uint64, id uint64) bool {
	tb.Helper()
	acc := s.indexedFieldByName[field]
	return indexdata.NewFieldOverlayStorage(s.lenIndex[acc.Ordinal]).LookupPostingRetained(keycodec.U64ByteString(ln)).Contains(id)
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

func snapshotExtQueryTxRecords[K ~string | ~uint64, V any](t *testing.T, db *DB[K, V], tx *bbolt.Tx, snap *indexSnapshot, q *qx.QX) []*V {
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

func snapshotExtEvalNumericRangeBuckets(t *testing.T, db *DB[uint64, Rec], expr qx.Expr) {
	t.Helper()

	prepared, compiled, err := prepareTestExpr(db.engine, expr)
	if err != nil {
		t.Fatalf("prepareTestExpr(numeric range): %v", err)
	}
	defer prepared.Release()

	f := db.engine.fieldNameByOrdinal(compiled.FieldOrdinal)
	fm := db.engine.schema.Fields[f]
	if fm == nil {
		t.Fatalf("expected %q field metadata", f)
	}
	ov := db.engine.currentQueryViewForTests().fieldOverlay(f)
	if !ov.HasData() {
		t.Fatalf("expected %q field index data", f)
	}

	key, isSlice, isNil, err := db.engine.exprValueToIdxScalar(expr)
	if err != nil {
		t.Fatalf("exprValueToIdxScalar(%s): %v", f, err)
	}
	if isSlice || isNil {
		t.Fatalf("unexpected scalar flags: isSlice=%v isNil=%v", isSlice, isNil)
	}

	rb, ok := rangeBoundsForOp(compiled.Op, key)
	if !ok {
		t.Fatalf("rangeBoundsForOp(%v) failed", compiled.Op)
	}

	out, ok := db.engine.currentQueryViewForTests().tryEvalNumericRangeBuckets(f, fm, ov, ov.RangeForBounds(rb))
	if !ok {
		t.Fatalf("expected numeric range bucket path for %q", f)
	}
	out.release()
}

func TestSnapshotExt_PublishSameSeqReusesRefAndUpdatesCurrent(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	first := &indexSnapshot{seq: 7, universe: posting.List{}}
	db.engine.snapshot.publishRef(first)

	gotFirst, ref, ok := db.engine.snapshot.pinRefBySeq(first.seq)
	if !ok || gotFirst != first {
		t.Fatalf("expected first snapshot to be pinnable")
	}

	second := &indexSnapshot{seq: first.seq, universe: posting.List{}}
	db.engine.snapshot.publishRef(second)

	if got := db.engine.getSnapshot(); got != second {
		t.Fatalf("expected current snapshot to be replaced on same-seq publish")
	}

	db.engine.snapshot.mu.RLock()
	held := db.engine.snapshot.bySeq[first.seq]
	registrySize := len(db.engine.snapshot.bySeq)
	db.engine.snapshot.mu.RUnlock()

	if held != ref {
		t.Fatalf("expected registry entry to reuse existing ref")
	}
	if registrySize != 1 {
		t.Fatalf("expected single registry entry after same-seq publish, got=%d", registrySize)
	}

	gotSecond, ref2, ok := db.engine.snapshot.pinRefBySeq(first.seq)
	if !ok || gotSecond != second || ref2 != ref {
		t.Fatalf("expected second snapshot to reuse same ref")
	}

	db.engine.snapshot.unpinRef(first.seq, ref)
	db.engine.snapshot.unpinRef(first.seq, ref2)

	db.engine.snapshot.mu.RLock()
	_, stillPresent := db.engine.snapshot.bySeq[first.seq]
	db.engine.snapshot.mu.RUnlock()
	if !stillPresent {
		t.Fatalf("expected current same-seq snapshot to remain registered after unpin")
	}
}

func TestSnapshotExt_DropStagedSnapshotPinnedEntryBecomesUnpinnableUntilUnpin(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	staged := &indexSnapshot{seq: 11}
	db.engine.snapshot.stage(staged)

	got, ref, ok := db.engine.snapshot.pinRefBySeq(staged.seq)
	if !ok || got != staged {
		t.Fatalf("expected staged snapshot to be pinnable before drop")
	}

	db.engine.snapshot.dropStaged(staged.seq)

	db.engine.snapshot.mu.RLock()
	held := db.engine.snapshot.bySeq[staged.seq]
	db.engine.snapshot.mu.RUnlock()
	if held != ref || held == nil || held.snap != nil {
		t.Fatalf("expected dropped pinned staged snapshot to keep ref but clear snap")
	}

	if _, _, ok = db.engine.snapshot.pinRefBySeq(staged.seq); ok {
		t.Fatalf("expected dropped staged snapshot to become unpinnable")
	}

	db.engine.snapshot.unpinRef(staged.seq, ref)

	db.engine.snapshot.mu.RLock()
	_, stillPresent := db.engine.snapshot.bySeq[staged.seq]
	db.engine.snapshot.mu.RUnlock()
	if stillPresent {
		t.Fatalf("expected staged snapshot registry entry to be pruned after last unpin")
	}
}

func TestSnapshotExt_DropStagedSnapshotUnpinnedDeletesEntry(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	staged := &indexSnapshot{seq: 12}
	db.engine.snapshot.stage(staged)
	db.engine.snapshot.dropStaged(staged.seq)

	db.engine.snapshot.mu.RLock()
	_, present := db.engine.snapshot.bySeq[staged.seq]
	db.engine.snapshot.mu.RUnlock()
	if present {
		t.Fatalf("expected unpinned staged snapshot to be removed immediately")
	}
}

func TestSnapshotExt_RetirePinnedPublishedSnapshotNilsEntryUntilUnpin(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	first := &indexSnapshot{seq: 1, universe: posting.List{}}
	second := &indexSnapshot{seq: 2, universe: posting.List{}}
	db.engine.snapshot.publishRef(first)

	got, ref, ok := db.engine.snapshot.pinRefBySeq(first.seq)
	if !ok || got != first {
		t.Fatalf("expected first snapshot to be pinnable")
	}

	db.engine.snapshot.publishRef(second)

	db.engine.snapshot.mu.RLock()
	oldHeld := db.engine.snapshot.bySeq[first.seq]
	_, nextPresent := db.engine.snapshot.bySeq[second.seq]
	db.engine.snapshot.mu.RUnlock()

	if oldHeld != ref || oldHeld == nil || oldHeld.snap != nil {
		t.Fatalf("expected pinned retired snapshot to keep ref with nil snap")
	}
	if !nextPresent {
		t.Fatalf("expected latest snapshot to remain registered")
	}
	if _, _, ok = db.engine.snapshot.pinRefBySeq(first.seq); ok {
		t.Fatalf("expected retired pinned snapshot to stop accepting new pins")
	}

	db.engine.snapshot.unpinRef(first.seq, ref)

	db.engine.snapshot.mu.RLock()
	_, oldPresent := db.engine.snapshot.bySeq[first.seq]
	_, nextPresent = db.engine.snapshot.bySeq[second.seq]
	db.engine.snapshot.mu.RUnlock()
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
	oldSeq := db.engine.getSnapshot().seq
	if _, ref, ok := db.engine.snapshot.pinRefBySeq(oldSeq); !ok {
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
		if mid.Sequence != db.engine.getSnapshot().seq || mid.Sequence <= oldSeq {
			t.Fatalf("expected stats sequence to advance after publish: %+v", mid)
		}
		if mid.RegistrySize != 2 || mid.PinnedRefs != 1 {
			t.Fatalf("unexpected stats with pinned retired snapshot: %+v", mid)
		}

		db.engine.snapshot.unpinRef(oldSeq, ref)

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

	current := db.engine.getSnapshot()
	staged := &indexSnapshot{seq: current.seq + 1}
	db.engine.snapshot.stage(staged)
	defer db.engine.snapshot.dropStaged(staged.seq)

	tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
	if err != nil {
		t.Fatalf("beginQueryTxSnapshot: %v", err)
	}
	defer rollback(tx)
	defer db.engine.snapshot.unpinRef(seq, ref)

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

	oldItems := snapshotExtQueryTxRecords(t, db, tx, snap, qx.Query(qx.EQ("name", "bob")))
	if len(oldItems) != 0 {
		t.Fatalf("old tx/snapshot unexpectedly sees future write: %#v", oldItems)
	}

	oldItems = snapshotExtQueryTxRecords(t, db, tx, snap, qx.Query(qx.EQ("name", "alice")))
	if len(oldItems) != 1 || oldItems[0] == nil || oldItems[0].Name != "alice" {
		t.Fatalf("unexpected old snapshot query result: %#v", oldItems)
	}

	rollback(tx)
	db.engine.snapshot.unpinRef(seq, ref)

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

	s1 := db.engine.getSnapshot()
	pinnedS1, ref, ok := db.engine.snapshot.pinRefBySeq(s1.seq)
	if !ok || pinnedS1 != s1 {
		t.Fatalf("expected old snapshot to be pinnable")
	}
	defer db.engine.snapshot.unpinRef(s1.seq, ref)

	if err := db.Set(2, &Rec{Name: "charlie", Age: 20}); err != nil {
		t.Fatalf("Set(update): %v", err)
	}

	s2 := db.engine.getSnapshot()
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

	s1 := db.engine.getSnapshot()
	pinnedS1, ref, ok := db.engine.snapshot.pinRefBySeq(s1.seq)
	if !ok || pinnedS1 != s1 {
		t.Fatalf("expected old snapshot to be pinnable")
	}
	defer db.engine.snapshot.unpinRef(s1.seq, ref)

	if err := db.Set(1, &Rec{Name: "alice", Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set(update): %v", err)
	}

	s2 := db.engine.getSnapshot()
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

	s1 := db.engine.getSnapshot()
	pinnedS1, ref, ok := db.engine.snapshot.pinRefBySeq(s1.seq)
	if !ok || pinnedS1 != s1 {
		t.Fatalf("expected old snapshot to be pinnable")
	}
	defer db.engine.snapshot.unpinRef(s1.seq, ref)
	if !snapshotTestLenZeroComplementField(pinnedS1, "tags") {
		t.Fatalf("expected zero-complement len index for sparse non-empty tags")
	}
	if !snapshotExtLenContainsID(t, pinnedS1, "tags", 1, 1) {
		t.Fatalf("expected old snapshot len=1 posting to include id=1")
	}

	if err := db.Patch(1, []Field{{Name: "tags", Value: []string{}}}); err != nil {
		t.Fatalf("Patch(tags): %v", err)
	}

	s2 := db.engine.getSnapshot()
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

	s1 := db.engine.getSnapshot()
	pinnedS1, ref, ok := db.engine.snapshot.pinRefBySeq(s1.seq)
	if !ok || pinnedS1 != s1 {
		t.Fatalf("expected old snapshot to be pinnable")
	}
	defer db.engine.snapshot.unpinRef(s1.seq, ref)
	val := "zzz"
	if err := db.Patch(1, []Field{{Name: "opt", Value: &val}}); err != nil {
		t.Fatalf("Patch(opt): %v", err)
	}

	s2 := db.engine.getSnapshot()
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

	s1 := db.engine.getSnapshot()
	pinnedS1, ref, ok := db.engine.snapshot.pinRefBySeq(s1.seq)
	if !ok || pinnedS1 != s1 {
		t.Fatalf("expected old snapshot to be pinnable")
	}
	defer db.engine.snapshot.unpinRef(s1.seq, ref)
	if err := db.Delete(2); err != nil {
		t.Fatalf("Delete(2): %v", err)
	}

	s2 := db.engine.getSnapshot()
	if !pinnedS1.universe.Contains(2) || pinnedS1.universe.Cardinality() != 2 {
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

	old := db.engine.getSnapshot()
	oldSeq := old.seq
	pinned, ref, ok := db.engine.snapshot.pinRefBySeq(oldSeq)
	if !ok || pinned != old {
		t.Fatalf("expected current snapshot to be pinnable before truncate")
	}

	if err := db.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	current := db.engine.getSnapshot()
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

	db.engine.snapshot.unpinRef(oldSeq, ref)

	db.engine.snapshot.mu.RLock()
	_, oldPresent := db.engine.snapshot.bySeq[oldSeq]
	registrySize := len(db.engine.snapshot.bySeq)
	db.engine.snapshot.mu.RUnlock()
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

	old := db.engine.getSnapshot()
	idx1, ok := old.strmap.Index("k1")
	if !ok || idx1 == 0 {
		t.Fatalf("expected k1 mapping before truncate")
	}
	pinned, ref, ok := db.engine.snapshot.pinRefBySeq(old.seq)
	if !ok || pinned != old {
		t.Fatalf("expected pre-truncate snapshot to be pinnable")
	}

	if err := db.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	current := db.engine.getSnapshot()
	if _, ok := current.strmap.Index("k1"); ok {
		t.Fatalf("current truncated snapshot unexpectedly retained k1 mapping")
	}
	if got, ok := pinned.strmap.String(idx1); !ok || got != "k1" {
		t.Fatalf("pinned pre-truncate snapshot lost k1 mapping: got=%q ok=%v", got, ok)
	}

	db.engine.snapshot.unpinRef(old.seq, ref)
}

func TestSnapshotExt_StringKeyOldSnapshotMappingsSurviveDeleteAndNewKeys(t *testing.T) {
	db, _ := openTempDBString(t, snapshotExtOptions())

	if err := db.Set("k1", &Rec{Name: "one"}); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	if err := db.Set("k2", &Rec{Name: "two"}); err != nil {
		t.Fatalf("Set(k2): %v", err)
	}

	s1 := db.engine.getSnapshot()
	idx1, ok := s1.strmap.Index("k1")
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

	s2 := db.engine.getSnapshot()
	if _, ok := s1.strmap.Index("k3"); ok {
		t.Fatalf("old snapshot unexpectedly sees future string key mapping")
	}
	if got, ok := s1.strmap.String(idx1); !ok || got != "k1" {
		t.Fatalf("old snapshot lost original reverse mapping: got=%q ok=%v", got, ok)
	}
	if _, ok := s2.strmap.Index("k3"); !ok {
		t.Fatalf("new snapshot is missing future string key mapping")
	}
}

func TestSnapshotExt_PatchUnchangedFieldInheritsPositiveMaterializedPredCache(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Email: "alice@example.com", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	key := db.engine.materializedPredCacheKey(qx.PREFIX("email", "ali"))
	if key == "" {
		t.Fatalf("expected non-empty cache key")
	}

	prev := db.engine.getSnapshot()
	ids := snapshotExtPosting(1)
	prev.storeMaterializedPred(key, ids)

	if err := db.Patch(1, []Field{{Name: "age", Value: 31}}); err != nil {
		t.Fatalf("Patch(age): %v", err)
	}

	next := db.engine.getSnapshot()
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

	key := db.engine.materializedPredCacheKey(qx.PREFIX("email", "zz"))
	if key == "" {
		t.Fatalf("expected non-empty cache key")
	}

	prev := db.engine.getSnapshot()
	prev.storeMaterializedPred(key, posting.List{})

	if err := db.Patch(1, []Field{{Name: "age", Value: 31}}); err != nil {
		t.Fatalf("Patch(age): %v", err)
	}

	next := db.engine.getSnapshot()
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

	nameKey := db.engine.materializedPredCacheKey(qx.PREFIX("name", "ali"))
	emailKey := db.engine.materializedPredCacheKey(qx.PREFIX("email", "ali"))
	if nameKey == "" || emailKey == "" {
		t.Fatalf("expected non-empty cache keys")
	}

	prev := db.engine.getSnapshot()
	nameIDs := snapshotExtPosting(1)
	emailIDs := snapshotExtPosting(1)
	prev.storeMaterializedPred(nameKey, nameIDs)
	prev.storeMaterializedPred(emailKey, emailIDs)

	if err := db.Patch(1, []Field{{Name: "name", Value: "alicia"}}); err != nil {
		t.Fatalf("Patch(name): %v", err)
	}

	next := db.engine.getSnapshot()
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

	nameKey := db.engine.materializedPredCacheKey(qx.PREFIX("name", "ali"))
	emailKey := db.engine.materializedPredCacheKey(qx.PREFIX("email", "ali"))
	if nameKey == "" || emailKey == "" {
		t.Fatalf("expected non-empty cache keys")
	}

	prev := db.engine.getSnapshot()
	emailIDs := snapshotExtPosting(1)
	prev.storeMaterializedPred(nameKey, snapshotExtPosting(1))
	prev.storeMaterializedPred(emailKey, emailIDs)

	if err := db.Patch(1, []Field{
		{Name: "name", Value: "alicia"},
		{Name: "name", Value: "ally"},
	}); err != nil {
		t.Fatalf("Patch(duplicate name): %v", err)
	}

	next := db.engine.getSnapshot()
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

	optKey := db.engine.materializedPredCacheKey(qx.PREFIX("opt", "z"))
	emailKey := db.engine.materializedPredCacheKey(qx.PREFIX("email", "ali"))
	if optKey == "" || emailKey == "" {
		t.Fatalf("expected non-empty cache keys")
	}

	prev := db.engine.getSnapshot()
	emailIDs := snapshotExtPosting(1)
	prev.storeMaterializedPred(optKey, posting.List{})
	prev.storeMaterializedPred(emailKey, emailIDs)

	val := "zzz"
	if err := db.Patch(1, []Field{{Name: "opt", Value: &val}}); err != nil {
		t.Fatalf("Patch(opt): %v", err)
	}

	next := db.engine.getSnapshot()
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
	snap := db.engine.getSnapshot()
	snapshotExtEvalNumericRangeBuckets(t, db, qx.GTE("age", 500))
	if snap.numericRangeBucketCache == nil || snap.numericRangeBucketCache.EntryCount() == 0 {
		t.Fatalf("expected numeric range bucket cache entries after evaluation")
	}
	if got := snap.matPredCache.EntryCount(); got != 0 {
		t.Fatalf("expected numeric range bucket helper to keep shared materialized predicate cache empty, got=%d", got)
	}
	expr := qx.PREFIX("name", "u_1")
	cacheKey := db.engine.materializedPredCacheKey(expr)
	if cacheKey == "" {
		t.Fatalf("expected non-empty materialized predicate cache key")
	}
	out, err := db.engine.evalSimple(expr)
	if err != nil {
		t.Fatalf("evalSimple(prefix): %v", err)
	}
	out.release()
	if _, ok := snap.loadMaterializedPred(cacheKey); !ok {
		t.Fatalf("expected materialized predicate cache entry after evalSimple")
	}
	if got := snap.matPredCache.EntryCount(); got == 0 {
		t.Fatalf("expected materialized predicate cache count > 0")
	}
	if snap.shouldPromoteRuntimeMaterializedPred("age\x1f5\x1f500") {
		t.Fatalf("expected first runtime promotion sight to stay local")
	}
	if got := snap.runtimeMatPredSeen.EntryCount(); got != 1 {
		t.Fatalf("expected one runtime seen-key entry before clear, got=%d", got)
	}

	db.clearCurrentSnapshotCachesForTesting()

	if db.engine.getSnapshot() != snap {
		t.Fatalf("expected cache clear to keep current snapshot published")
	}
	if got := snap.numericRangeBucketCache.EntryCount(); got != 0 {
		t.Fatalf("expected numeric range bucket cache to be empty after clear, got=%d", got)
	}
	if got := snapshotExtMaterializedPredEntryCount(snap); got != 0 {
		t.Fatalf("expected materialized predicate cache to be empty after clear, got=%d", got)
	}
	if got := snap.matPredCache.EntryCount(); got != 0 {
		t.Fatalf("expected materialized predicate cache count reset, got=%d", got)
	}
	if got := snap.matPredCache.OversizedCount(); got != 0 {
		t.Fatalf("expected oversized materialized predicate cache count reset, got=%d", got)
	}
	if _, ok := snap.loadMaterializedPred(cacheKey); ok {
		t.Fatalf("expected cleared materialized predicate cache entry to be absent")
	}
	if got := snap.runtimeMatPredSeen.EntryCount(); got != 0 {
		t.Fatalf("expected runtime seen-key cache to be empty after clear, got=%d", got)
	}
	if got := snap.orderORMatPredObserved.EntryCount(); got != 0 {
		t.Fatalf("expected ordered OR observed-work cache to be empty after clear, got=%d", got)
	}
}

func TestSnapshotExt_RuntimeSeenPromotionBounded(t *testing.T) {
	s := snapshotExtNewMaterializedPredSnapshot(2, 0)
	limit := qcache.RecentKeyLimit(s.materializedPredCacheLimit())
	if limit <= 0 {
		t.Fatalf("expected positive recent-key limit")
	}

	for i := 0; i < limit+6; i++ {
		key := fmt.Sprintf("age\x1f5\x1f%d", i)
		if s.shouldPromoteRuntimeMaterializedPred(key) {
			t.Fatalf("expected first runtime sight for %q to stay local", key)
		}
	}
	if got := s.runtimeMatPredSeen.EntryCount(); got > limit {
		t.Fatalf("runtime seen-key cache exceeded limit: got=%d want<=%d", got, limit)
	}

	lastKey := fmt.Sprintf("age\x1f5\x1f%d", limit+5)
	if !s.shouldPromoteRuntimeMaterializedPred(lastKey) {
		t.Fatalf("expected recent runtime key %q to promote on second sight", lastKey)
	}
}

func TestSnapshotExt_OrderedORObservedPromotionAccumulates(t *testing.T) {
	s := snapshotExtNewMaterializedPredSnapshot(2, 0)
	key := "age\x1fcount_range_complement\x1f5\x1f30"
	if s.shouldPromoteObservedOrderedORMaterializedPred(key, 10, 25) {
		t.Fatalf("expected first observed work below threshold to stay local")
	}
	if got := s.orderORMatPredObserved.EntryCount(); got != 1 {
		t.Fatalf("expected one ordered OR observed-work entry, got=%d", got)
	}
	if !s.shouldPromoteObservedOrderedORMaterializedPred(key, 15, 25) {
		t.Fatalf("expected accumulated observed work to trigger promotion")
	}
	if got := s.orderORMatPredObserved.EntryCount(); got != 0 {
		t.Fatalf("expected promoted ordered OR observed-work entry to be removed, got=%d", got)
	}
}

func TestSnapshotExt_StoreMaterializedPredConcurrentDistinctKeysRespectsLimit(t *testing.T) {
	n := max(16, runtime.GOMAXPROCS(0)*8)
	for round := 0; round < 16; round++ {
		s := snapshotExtNewMaterializedPredSnapshot(1, 0)
		snapshotExtRunConcurrent(n, func(i int) {
			s.storeMaterializedPred(fmt.Sprintf("email\x1f%s\x1f%d", qx.OpPREFIX, i), posting.List{})
		})
		if got := s.matPredCache.EntryCount(); got > 1 {
			t.Fatalf("round=%d materialized cache count exceeded limit: got=%d", round, got)
		}
		if got := snapshotExtMaterializedPredEntryCount(s); got > 1 {
			t.Fatalf("round=%d materialized cache entries exceeded limit: got=%d", round, got)
		}
	}
}

func TestSnapshotExt_StoreMaterializedPredConcurrentSameKeyStoresSingleEntry(t *testing.T) {
	n := max(16, runtime.GOMAXPROCS(0)*8)
	s := snapshotExtNewMaterializedPredSnapshot(8, 0)

	snapshotExtRunConcurrent(n, func(i int) {
		s.storeMaterializedPred("email\x1f1\x1fsame", snapshotExtPosting(uint64(i+1)))
	})

	if got := s.matPredCache.EntryCount(); got != 1 {
		t.Fatalf("expected single cache count for duplicate key, got=%d", got)
	}
	if got := snapshotExtMaterializedPredEntryCount(s); got != 1 {
		t.Fatalf("expected single cache entry for duplicate key, got=%d", got)
	}
}

func TestSnapshotExt_TryStoreMaterializedPredOversizedConcurrentDistinctKeysRespectsTotalLimit(t *testing.T) {
	n := max(16, runtime.GOMAXPROCS(0)*8)
	bm := snapshotExtPosting(1, 2)
	for round := 0; round < 16; round++ {
		s := snapshotExtNewMaterializedPredSnapshot(1, 1)
		snapshotExtRunConcurrent(n, func(i int) {
			s.tryStoreMaterializedPredOversized(fmt.Sprintf("age\x1frange_bucket\x1f%d", i), bm)
		})
		if got := s.matPredCache.EntryCount(); got > 1 {
			t.Fatalf("round=%d oversized cache count exceeded limit: got=%d", round, got)
		}
		if got := snapshotExtMaterializedPredEntryCount(s); got > 1 {
			t.Fatalf("round=%d oversized cache entries exceeded limit: got=%d", round, got)
		}
		if got := s.matPredCache.OversizedCount(); got > 1 {
			t.Fatalf("round=%d oversized counter exceeded total limit: got=%d", round, got)
		}
	}
}

func TestSnapshotExt_TryStoreMaterializedPredOversizedConcurrentSameKeyDoesNotLeakCounters(t *testing.T) {
	n := max(16, runtime.GOMAXPROCS(0)*8)
	bm := snapshotExtPosting(1, 2)
	s := snapshotExtNewMaterializedPredSnapshot(8, 1)

	snapshotExtRunConcurrent(n, func(i int) {
		s.tryStoreMaterializedPredOversized("age\x1frange_bucket\x1fsame", bm)
	})

	if got := s.matPredCache.EntryCount(); got != 1 {
		t.Fatalf("expected single oversized cache count for duplicate key, got=%d", got)
	}
	if got := snapshotExtMaterializedPredEntryCount(s); got != 1 {
		t.Fatalf("expected single oversized cache entry for duplicate key, got=%d", got)
	}
	if got := s.matPredCache.OversizedCount(); got != 1 {
		t.Fatalf("expected oversized counter to settle at 1, got=%d", got)
	}
}

func TestSnapshotExt_MaterializedPredCacheOversizedAdmissionTurnsOverEntries(t *testing.T) {
	s := snapshotExtNewMaterializedPredSnapshot(8, 1)

	small := snapshotExtPosting(1)
	large := snapshotExtPosting(1, 2)

	for i := 0; i < 8; i++ {
		s.storeMaterializedPred(fmt.Sprintf("email\x1f%s\x1f%d", qx.OpPREFIX, i), small)
	}
	if got := s.matPredCache.EntryCount(); got != 8 {
		t.Fatalf("expected regular cache to fill total limit, got=%d", got)
	}
	if got := snapshotExtMaterializedPredEntryCount(s); got != 8 {
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

	if got := s.matPredCache.EntryCount(); got != 8 {
		t.Fatalf("expected total cache count to remain bounded by limit, got=%d", got)
	}
	if got := snapshotExtMaterializedPredEntryCount(s); got != 8 {
		t.Fatalf("expected total cache entries to remain bounded by limit, got=%d", got)
	}
	if got := s.matPredCache.OversizedCount(); got != 2 {
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
	s := snapshotExtNewMaterializedPredSnapshot(8, 1)

	small := snapshotExtPosting(1)
	large := snapshotExtPosting(1, 2)

	for i := 0; i < 8; i++ {
		s.storeMaterializedPred(fmt.Sprintf("email\x1f%s\x1f%d", qx.OpPREFIX, i), small)
	}
	if !s.tryStoreMaterializedPredOversized("age\x1frange_bucket\x1f0", large) {
		t.Fatalf("expected first oversized cache entry to be admitted")
	}
	if !s.tryStoreMaterializedPredOversized("age\x1frange_bucket\x1f1", large) {
		t.Fatalf("expected second oversized cache entry to be admitted")
	}

	s.storeMaterializedPred("email\x1f1\x1flate", small)

	if got := s.matPredCache.EntryCount(); got != 8 {
		t.Fatalf("expected total cache count to remain bounded by limit, got=%d", got)
	}
	if got := snapshotExtMaterializedPredEntryCount(s); got != 8 {
		t.Fatalf("expected total cache entries to remain bounded by limit, got=%d", got)
	}
	if got := s.matPredCache.OversizedCount(); got != 2 {
		t.Fatalf("expected regular turnover to keep oversized entries resident, got=%d", got)
	}
	if _, ok := s.loadMaterializedPred("email\x1f1\x1flate"); !ok {
		t.Fatalf("expected newest regular entry to replace an older regular entry")
	}
}

func TestSnapshotExt_InheritMaterializedPredCacheSkipsChangedFieldsAndKeepsOthers(t *testing.T) {
	prev := snapshotExtNewMaterializedPredSnapshot(8, 0)
	nameIDs := snapshotExtPosting(1)
	emailIDs := snapshotExtPosting(2)
	prev.storeMaterializedPred("name\x1f1\x1fa", nameIDs)
	prev.storeMaterializedPred("email\x1f1\x1fb", emailIDs)

	next := snapshotExtNewMaterializedPredSnapshot(8, 0)
	db := &DB[uint64, struct{}]{
		engine: &queryEngine{
			schema: &schema.Runtime{
				IndexedByName: map[string]schema.IndexedFieldAccessor{
					"name":  {Ordinal: 0},
					"email": {Ordinal: 1},
				},
			},
		},
	}
	changed := snapshotTestBoolSlots(map[string]bool{"name": true}, db.engine.schema.IndexedByName)
	defer pooled.ReleaseBoolSlice(changed)

	inheritMaterializedPredCache(next, prev, db.engine.schema.IndexedByName, changed)

	if _, ok := next.loadMaterializedPred("name\x1f1\x1fa"); ok {
		t.Fatalf("expected changed-field cache entry to be skipped")
	}
	cached, ok := next.loadMaterializedPred("email\x1f1\x1fb")
	if !ok || cached != emailIDs {
		t.Fatalf("expected untouched-field cache entry to be inherited")
	}
}

func TestSnapshotExt_InheritMaterializedPredCacheSkipsFieldlessKeys(t *testing.T) {
	prev := snapshotExtNewMaterializedPredSnapshot(8, 0)
	prev.storeMaterializedPred("opaque", snapshotExtPosting(1))
	prev.storeMaterializedPred("name\x1f1\x1fb", posting.List{})

	next := snapshotExtNewMaterializedPredSnapshot(8, 0)
	inheritMaterializedPredCache(next, prev, nil, nil)

	if got := next.matPredCache.EntryCount(); got != 1 {
		t.Fatalf("expected exactly one valid inherited entry, got=%d", got)
	}
	cached, ok := next.loadMaterializedPred("name\x1f1\x1fb")
	if !ok || !cached.IsEmpty() {
		t.Fatalf("expected valid negative cache entry to survive malformed-entry filtering")
	}
}

func TestSnapshotExt_InheritMaterializedPredCacheKeepsOversizedCount(t *testing.T) {
	prev := snapshotExtNewMaterializedPredSnapshot(8, 1)
	oversized := snapshotExtPosting(1, 2)
	prev.tryStoreMaterializedPredOversized("a\x1f1\x1f1", oversized)
	prev.tryStoreMaterializedPredOversized("b\x1f1\x1f2", oversized)

	next := snapshotExtNewMaterializedPredSnapshot(8, 1)
	inheritMaterializedPredCache(next, prev, nil, nil)

	if got := next.matPredCache.EntryCount(); got != 2 {
		t.Fatalf("expected oversized entries to be inherited, got=%d", got)
	}
	if got := next.matPredCache.OversizedCount(); got != 2 {
		t.Fatalf("expected oversized counter to be inherited, got=%d", got)
	}
}

func TestSnapshotExt_InheritMaterializedPredCachePreservesRecencyStamps(t *testing.T) {
	prev := snapshotExtNewMaterializedPredSnapshot(8, 0)
	prev.storeMaterializedPred("email\x1f1\x1fa", snapshotExtPosting(1))
	prev.storeMaterializedPred("name\x1f1\x1fb", snapshotExtPosting(2))
	if ids, ok := prev.loadMaterializedPred("email\x1f1\x1fa"); ok {
		ids.Release()
	}

	next := snapshotExtNewMaterializedPredSnapshot(8, 0)
	inheritMaterializedPredCache(next, prev, nil, nil)

	inheritedClock := next.matPredCache.Clock()
	if _, ok := next.loadMaterializedPred("email\x1f1\x1fa"); !ok {
		t.Fatalf("expected first cache entry to be inherited")
	}
	if _, ok := next.loadMaterializedPred("name\x1f1\x1fb"); !ok {
		t.Fatalf("expected second cache entry to be inherited")
	}
	if want, got := prev.matPredCache.Clock(), inheritedClock; got != want {
		t.Fatalf("expected next snapshot clock to continue from inherited entries, got=%d want=%d", got, want)
	}
}

func TestSnapshotExt_InheritNumericRangeBucketCacheCopiesOnlyMatchingStorage(t *testing.T) {
	shared := snapshotExtStorage("10", "20")
	prevChanged := snapshotExtStorage("30")
	nextChanged := snapshotExtStorage("30")

	ageEntry := qcache.GetNumericRangeBucketEntry(shared, qcache.NumericRangeBucketIndex{}, 0)
	scoreEntry := qcache.GetNumericRangeBucketEntry(prevChanged, qcache.NumericRangeBucketIndex{}, 0)

	prev := snapshotTestNewSnapshot(
		map[string]indexdata.FieldStorage{"age": shared, "score": prevChanged},
		nil,
		nil,
		nil,
	)
	prev.numericRangeBucketCache = qcache.GetNumericRangeBucketCache(2, 0)
	prev.numericRangeBucketCache.StoreSlot("age", 0, ageEntry)
	prev.numericRangeBucketCache.StoreSlot("score", 1, scoreEntry)
	defer prev.releaseRuntimeCaches()

	next := snapshotTestNewSnapshot(
		map[string]indexdata.FieldStorage{"age": shared, "score": nextChanged},
		nil,
		nil,
		nil,
	)
	next.numericRangeBucketCache = qcache.GetNumericRangeBucketCache(2, 0)
	defer next.releaseRuntimeCaches()
	inheritNumericRangeBucketCache(next, prev)

	if next.numericRangeBucketCache == nil {
		t.Fatalf("expected next snapshot to own a numeric range cache")
	}
	if got := next.numericRangeBucketCache.EntryCount(); got != 1 {
		t.Fatalf("expected exactly one inherited numeric range entry, got=%d", got)
	}
	raw, ok := next.numericRangeBucketCache.LoadField("age")
	if !ok || raw != ageEntry {
		t.Fatalf("expected matching-storage entry to be inherited by pointer")
	}
	if _, ok := next.numericRangeBucketCache.LoadField("score"); ok {
		t.Fatalf("expected changed-storage entry to be skipped")
	}
}

func TestSnapshotExt_InheritNumericRangeBucketCacheSkipsMalformedAndEmptyEntries(t *testing.T) {
	valid := snapshotExtStorage("10")
	validEntry := qcache.GetNumericRangeBucketEntry(valid, qcache.NumericRangeBucketIndex{}, 0)

	prev := snapshotTestNewSnapshot(map[string]indexdata.FieldStorage{"age": valid}, nil, nil, nil)
	prev.numericRangeBucketCache = qcache.GetNumericRangeBucketCache(3, 0)
	prev.numericRangeBucketCache.StoreSlot("", 0, validEntry)
	prev.numericRangeBucketCache.StoreSlot("score", 1, qcache.GetNumericRangeBucketEntry(indexdata.FieldStorage{}, qcache.NumericRangeBucketIndex{}, 0))
	validEntry.Retain()
	prev.numericRangeBucketCache.StoreSlot("age", 2, validEntry)
	defer prev.releaseRuntimeCaches()

	next := snapshotTestNewSnapshot(map[string]indexdata.FieldStorage{"age": valid}, nil, nil, nil)
	next.numericRangeBucketCache = qcache.GetNumericRangeBucketCache(3, 0)
	defer next.releaseRuntimeCaches()
	inheritNumericRangeBucketCache(next, prev)

	if next.numericRangeBucketCache == nil {
		t.Fatalf("expected next snapshot to own numeric cache map")
	}
	if got := next.numericRangeBucketCache.EntryCount(); got != 1 {
		t.Fatalf("expected malformed/empty entries to be filtered, got=%d", got)
	}
	raw, ok := next.numericRangeBucketCache.LoadField("age")
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

	old := db.engine.getSnapshot()
	if old == nil || old.strmap == nil {
		t.Fatalf("expected published string-key snapshot")
	}
	pinned, ref, ok := db.engine.snapshot.pinRefBySeq(old.seq)
	if !ok || pinned != old {
		t.Fatalf("expected current string snapshot to be pinnable")
	}
	defer db.engine.snapshot.unpinRef(old.seq, ref)

	var want []string
	iterOld := pinned.universe.Iter()
	defer iterOld.Release()
	for iterOld.HasNext() {
		idx := iterOld.Next()
		key, ok := pinned.strmap.String(idx)
		if !ok {
			t.Fatalf("old snapshot lost key mapping for idx=%d", idx)
		}
		want = append(want, key)
	}
	if !slices.Equal(want, []string{"k1", "k2"}) {
		t.Fatalf("unexpected old snapshot key order: %v", want)
	}

	universe := pinned.universe
	iter := universe.Iter()
	defer iter.Release()

	if err := db.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	var got []string
	err := db.scanStringKeys(pinned.strmap, universe, iter, "", func(id string) (bool, error) {
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

	old := db.engine.getSnapshot()
	pinned, ref, ok := db.engine.snapshot.pinRefBySeq(old.seq)
	if !ok || pinned != old {
		t.Fatalf("expected current string snapshot to be pinnable")
	}
	defer db.engine.snapshot.unpinRef(old.seq, ref)
	universe := pinned.universe
	iter := universe.Iter()
	defer iter.Release()

	if err := db.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if err := db.Set("future", &Rec{Name: "future"}); err != nil {
		t.Fatalf("Set(future): %v", err)
	}

	var got []string
	err := db.scanStringKeys(pinned.strmap, universe, iter, "", func(id string) (bool, error) {
		got = append(got, id)
		return true, nil
	})
	if err != nil {
		t.Fatalf("split snapshot emitted inconsistent string scan state: err=%v got=%v oldSeq=%d currentSeq=%d", err, got, old.seq, db.engine.getSnapshot().seq)
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

	before := db.engine.getSnapshot()
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
	db.engine.snapshot.unpinRef(seq, ref)

	if err := <-done; err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	after := db.engine.getSnapshot()
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

	before := db.engine.getSnapshot()
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
	db.engine.snapshot.unpinRef(seq, ref)

	err = <-done
	if !errors.Is(err, ErrBroken) {
		t.Fatalf("expected ErrBroken from truncate publish failpoint, got: %v", err)
	}

	assertNoFutureSnapshotRefs(t, db)
	if got := db.engine.getSnapshot(); got != before {
		t.Fatalf("broken truncate publish should keep previously published snapshot current")
	}
}

func TestSpanshotExt_MaterializedPredMixedRegularAndOversizedConcurrentRespectsGlobalLimit(t *testing.T) {
	n := max(16, runtime.GOMAXPROCS(0)*8)
	small := snapshotExtPosting(1)
	large := snapshotExtPosting(1, 2)

	for round := 0; round < 32; round++ {
		s := snapshotExtNewMaterializedPredSnapshot(1, 1)
		snapshotExtRunConcurrent(n, func(i int) {
			if i%2 == 0 {
				s.storeMaterializedPred(fmt.Sprintf("email\x1f%s\x1f%d", qx.OpPREFIX, i), small)
				return
			}
			s.tryStoreMaterializedPredOversized(fmt.Sprintf("age\x1frange_bucket\x1f%d", i), large)
		})

		if got := s.matPredCache.EntryCount(); got > 1 {
			t.Fatalf("round=%d mixed cache count exceeded global limit: got=%d", round, got)
		}
		if got := snapshotExtMaterializedPredEntryCount(s); got > 1 {
			t.Fatalf("round=%d mixed cache entries exceeded global limit: got=%d", round, got)
		}
	}
}

func TestIndexSnapshotMaterializedPredCacheDetachedLoadsUnderConcurrency(t *testing.T) {
	snap := snapshotExtNewMaterializedPredSnapshot(8, 0)

	base := snapshotExtPosting()
	for i := uint64(1); i <= 40; i++ {
		base = base.BuildAdded(i * 5)
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
			cached = cached.BuildAndNot(remove)
			cached = cached.BuildOr(add)
			cached = cached.BuildAdded(6<<32 | uint64(i))
			cached = cached.BuildOptimized()
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

func assertPostingConsumerSet(t *testing.T, got posting.List, want []uint64) {
	t.Helper()
	gotIDs := got.ToArray()
	if !slices.Equal(gotIDs, want) {
		t.Fatalf("posting mismatch: got=%v want=%v", gotIDs, want)
	}
}

/**/

func TestSnapshotExt_PinCurrentSnapshotTracksReaderPinsAcrossPublish(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	current := db.engine.getSnapshot()
	snap, seq, ref, pinned := db.engine.pinCurrentSnapshot()
	if !pinned || snap != current {
		t.Fatalf("expected current snapshot pin")
	}
	if seq != current.seq || ref == nil {
		t.Fatalf("expected current snapshot ref pin")
	}
	if got := ref.refs.Load(); got != 1 {
		t.Fatalf("expected one current snapshot ref, got=%d", got)
	}

	if err := db.Set(2, &Rec{Name: "bob", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	if got := ref.refs.Load(); got != 1 {
		t.Fatalf("expected old snapshot ref pin to survive publish, got=%d", got)
	}

	db.engine.unpinCurrentSnapshot(seq, ref, pinned)
	if got := ref.refs.Load(); got != 0 {
		t.Fatalf("expected current snapshot ref pins to drop after unpin, got=%d", got)
	}
}

func TestSnapshotExt_QueryKeysReleasesCurrentSnapshotPinOnError(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	snap := db.engine.getSnapshot()
	if _, err := db.QueryKeys(qx.Query(qx.EQ("does_not_exist", 1))); err == nil {
		t.Fatalf("expected QueryKeys to fail for unknown field")
	}
	ref := db.engine.snapshot.bySeq[snap.seq]
	if ref == nil {
		t.Fatalf("expected QueryKeys snapshot ref to remain registered")
	}
	if got := ref.refs.Load(); got != 0 {
		t.Fatalf("expected QueryKeys pin release on error, got=%d", got)
	}
}

func TestSnapshotExt_CountReleasesCurrentSnapshotPinOnError(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	snap := db.engine.getSnapshot()
	if _, err := db.Count(qx.EQ("does_not_exist", 1)); err == nil {
		t.Fatalf("expected Count to fail for unknown field")
	}
	ref := db.engine.snapshot.bySeq[snap.seq]
	if ref == nil {
		t.Fatalf("expected Count snapshot ref to remain registered")
	}
	if got := ref.refs.Load(); got != 0 {
		t.Fatalf("expected Count pin release on error, got=%d", got)
	}
}

func TestSnapshotExt_ScanKeysReleasesCurrentSnapshotPinOnCallbackError(t *testing.T) {
	db, _ := openTempDBUint64(t, snapshotExtOptions())

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	snap := db.engine.getSnapshot()
	wantErr := errors.New("stop")
	err := db.ScanKeys(0, func(uint64) (bool, error) {
		return false, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("ScanKeys err=%v want %v", err, wantErr)
	}
	ref := db.engine.snapshot.bySeq[snap.seq]
	if ref == nil {
		t.Fatalf("expected ScanKeys snapshot ref to remain registered")
	}
	if got := ref.refs.Load(); got != 0 {
		t.Fatalf("expected ScanKeys pin release on callback error, got=%d", got)
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
) *indexSnapshot {
	byName := snapshotTestIndexedFieldByName(index, nilIndex, lenIndex, lenZeroComplement)
	return &indexSnapshot{
		index:              snapshotTestFieldIndexSlots(index, byName),
		nilIndex:           snapshotTestFieldIndexSlots(nilIndex, byName),
		lenIndex:           snapshotTestFieldIndexSlots(lenIndex, byName),
		lenZeroComplement:  snapshotTestBoolSlots(lenZeroComplement, byName),
		indexedFieldByName: byName,
	}
}

func snapshotTestLenZeroComplementField(s *indexSnapshot, name string) bool {
	acc, ok := s.indexedFieldByName[name]
	if !ok || acc.Ordinal >= len(s.lenZeroComplement) {
		return false
	}
	return s.lenZeroComplement[acc.Ordinal]
}
