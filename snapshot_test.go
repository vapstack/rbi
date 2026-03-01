package rbi

import (
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/vapstack/qx"
	"go.etcd.io/bbolt"
)

func waitForSnapshotState(t *testing.T, timeout time.Duration, reason string, fn func() bool) {
	t.Helper()
	timeout = snapshotTestWaitTimeout(timeout)
	deadline := time.Now().Add(timeout)
	for {
		if fn() {
			return
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	// One final check avoids edge misses right at deadline.
	if fn() {
		return
	}
	t.Fatalf("timeout waiting for snapshot state: %s", reason)
}

func snapshotTestWaitTimeout(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return timeout
	}
	if !testRaceEnabled {
		return timeout
	}

	scaled := timeout * 4
	const minRaceTimeout = 2 * time.Second
	if scaled < minRaceTimeout {
		return minRaceTimeout
	}
	return scaled
}

func TestSnapshotTxID_PublishedOnWrite(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	before := db.getSnapshot().txID

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	after := db.getSnapshot().txID
	if after <= before {
		t.Fatalf("snapshot txID did not advance: before=%d after=%d", before, after)
	}

	cur := db.currentBoltTxID()
	if after != cur {
		t.Fatalf("snapshot txID mismatch with bolt txID: snapshot=%d bolt=%d", after, cur)
	}
}

func TestSnapshotTxID_PreviousTxRemainsPinable(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	if err := db.Set(1, &Rec{Name: "seed", Age: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	oldTxID := db.getSnapshot().txID

	if err := db.Set(2, &Rec{Name: "new", Age: 20}); err != nil {
		t.Fatalf("concurrent Set: %v", err)
	}

	latest := db.getSnapshot().txID
	if latest <= oldTxID {
		t.Fatalf("latest txID did not advance: old=%d latest=%d", oldTxID, latest)
	}

	if _, ok := db.pinByTxID(oldTxID); !ok {
		t.Fatalf("previous snapshot disappeared: txID=%d", oldTxID)
	}
	db.unpinByTxID(oldTxID)
}

func TestSnapshotTxID_AdvancesWhenIndexingDisabled(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	db.DisableIndexing()

	before := db.getSnapshot().txID
	if err := db.Set(1, &Rec{Name: "x", Age: 1}); err != nil {
		t.Fatalf("Set with indexing disabled: %v", err)
	}
	after := db.getSnapshot().txID
	if after <= before {
		t.Fatalf("snapshot txID did not advance with indexing disabled: before=%d after=%d", before, after)
	}
}

func TestSnapshotDelta_PublishedOnSet(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	if err := db.Set(1, &Rec{Name: "alice", Tags: []string{"go", "db"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	s := db.getSnapshot()
	if s.indexDeltaCount() == 0 {
		t.Fatalf("index delta is empty")
	}
	if s.universeAdd == nil || !s.universeAdd.Contains(1) {
		t.Fatalf("universeAdd must contain id=1")
	}

	nameDelta := s.fieldDelta("name")
	if nameDelta == nil {
		t.Fatalf("name delta is nil")
	}
	nameEntry, ok := nameDelta.get("alice")
	if !ok {
		t.Fatalf("name/alice delta is missing")
	}
	if !deltaEntryAddContains(nameEntry, 1) {
		t.Fatalf("name add delta must contain id=1")
	}

	tagsDelta := s.fieldDelta("tags")
	if tagsDelta == nil {
		t.Fatalf("tags delta is nil")
	}
	e, ok := tagsDelta.get("go")
	if !ok || !deltaEntryAddContains(e, 1) {
		t.Fatalf("tags/go add delta must contain id=1")
	}
	e, ok = tagsDelta.get("db")
	if !ok || !deltaEntryAddContains(e, 1) {
		t.Fatalf("tags/db add delta must contain id=1")
	}

	if s.lenDeltaCount() == 0 {
		t.Fatalf("len delta is empty")
	}
	tagsLenDelta := s.lenFieldDelta("tags")
	if tagsLenDelta == nil {
		t.Fatalf("tags len delta is nil")
	}
	key := uint64ByteStr(2)
	e, ok = tagsLenDelta.get(key)
	if !ok || !deltaEntryAddContains(e, 1) {
		t.Fatalf("tags len add delta must contain id=1")
	}
}

func TestSnapshotDelta_UpdateNeutralizesLenNoop(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	if err := db.Set(1, &Rec{Name: "alice", Tags: []string{"go"}}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	if err := db.Set(1, &Rec{Name: "alice", Tags: []string{"rust"}}); err != nil {
		t.Fatalf("update Set: %v", err)
	}

	s := db.getSnapshot()
	if s.indexDeltaCount() == 0 {
		t.Fatalf("index delta is empty")
	}
	tagsDelta := s.fieldDelta("tags")
	if tagsDelta == nil {
		t.Fatalf("tags delta is nil")
	}
	e, _ := tagsDelta.get("go")
	if !deltaEntryIsEmpty(e) {
		t.Fatalf("tags/go must be neutralized in accumulated delta")
	}
	e, ok := tagsDelta.get("rust")
	if !ok || !deltaEntryAddContains(e, 1) {
		t.Fatalf("tags/rust add delta must contain id=1")
	}

	// net state relative to empty base still has id=1 in len=1 bucket.
	d := s.lenFieldDelta("tags")
	if d == nil {
		t.Fatalf("tags len delta is nil")
	}
	keyLen1 := uint64ByteStr(1)
	e, ok = d.get(keyLen1)
	if !ok || !deltaEntryAddContains(e, 1) {
		t.Fatalf("expected len(tags)=1 add delta to contain id=1, got entry=%+v", e)
	}
}

func TestSnapshotDelta_WritePublishesDeltaByDefault(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	if err := db.Set(1, &Rec{Name: "alice", Age: 10}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	s := db.getSnapshot()
	if s.indexDeltaCount() == 0 && s.lenDeltaCount() == 0 && (s.universeAdd == nil || s.universeAdd.IsEmpty()) {
		t.Fatalf("expected snapshot delta to be present after write")
	}
}

func TestSnapshotStrMap_OldSnapshotDoesNotSeeFutureKeyMappings(t *testing.T) {
	db, _ := openTempDBString(t, nil)

	if err := db.Set("k1", &Rec{Name: "a", Age: 10}); err != nil {
		t.Fatalf("Set k1: %v", err)
	}
	oldSnap := db.getSnapshot()
	if oldSnap == nil || oldSnap.strmap == nil {
		t.Fatalf("old snapshot strmap is nil")
	}
	if _, ok := oldSnap.strmap.getIdxNoLock("k1"); !ok {
		t.Fatalf("old snapshot must contain k1 mapping")
	}
	if _, ok := oldSnap.strmap.getIdxNoLock("k2"); ok {
		t.Fatalf("old snapshot must not contain k2 mapping before write")
	}

	if err := db.Set("k2", &Rec{Name: "b", Age: 20}); err != nil {
		t.Fatalf("Set k2: %v", err)
	}

	if _, ok := oldSnap.strmap.getIdxNoLock("k2"); ok {
		t.Fatalf("old snapshot unexpectedly observed future mapping k2")
	}

	newSnap := db.getSnapshot()
	if newSnap == nil || newSnap.strmap == nil {
		t.Fatalf("new snapshot strmap is nil")
	}
	if _, ok := newSnap.strmap.getIdxNoLock("k2"); !ok {
		t.Fatalf("new snapshot must contain k2 mapping")
	}
}

func TestSnapshotStrMap_CompactsOverlayDepth(t *testing.T) {
	db, _ := openTempDBString(t, nil)
	db.strmap.compactAt = 2

	if err := db.Set("k1", &Rec{Name: "a", Age: 10}); err != nil {
		t.Fatalf("Set k1: %v", err)
	}
	s1 := db.getSnapshot()
	if s1 == nil || s1.strmap == nil {
		t.Fatalf("snapshot after k1 is nil")
	}
	if s1.strmap.depth < 1 {
		t.Fatalf("unexpected snapshot depth after k1: %d", s1.strmap.depth)
	}
	if s1.strmap.DenseStrs != nil {
		t.Fatalf("first snapshot should stay delta-backed before compaction")
	}
	if len(s1.strmap.Strs) == 0 {
		t.Fatalf("first snapshot must contain sparse delta entries")
	}

	if err := db.Set("k2", &Rec{Name: "b", Age: 20}); err != nil {
		t.Fatalf("Set k2: %v", err)
	}
	s2 := db.getSnapshot()
	if s2 == nil || s2.strmap == nil {
		t.Fatalf("snapshot after k2 is nil")
	}
	if s2.strmap.depth != 1 {
		t.Fatalf("expected compacted depth=1, got %d", s2.strmap.depth)
	}
	if _, ok := s2.strmap.getIdxNoLock("k1"); !ok {
		t.Fatalf("compacted snapshot lost k1 mapping")
	}
	if _, ok := s2.strmap.getIdxNoLock("k2"); !ok {
		t.Fatalf("compacted snapshot lost k2 mapping")
	}
	if len(s2.strmap.DenseStrs) == 0 {
		t.Fatalf("compacted snapshot must be dense-backed")
	}
	if s2.strmap.Strs != nil {
		t.Fatalf("compacted snapshot should not keep sparse str map")
	}
}

func TestSnapshotStrMap_DeltaOverDenseBaseLookup(t *testing.T) {
	db, _ := openTempDBString(t, nil)
	db.strmap.compactAt = 3

	if err := db.Set("k1", &Rec{Name: "a", Age: 10}); err != nil {
		t.Fatalf("Set k1: %v", err)
	}
	if err := db.Set("k2", &Rec{Name: "b", Age: 11}); err != nil {
		t.Fatalf("Set k2: %v", err)
	}
	if err := db.Set("k3", &Rec{Name: "c", Age: 12}); err != nil {
		t.Fatalf("Set k3: %v", err)
	}
	s3 := db.getSnapshot()
	if s3 == nil || s3.strmap == nil {
		t.Fatalf("snapshot after k3 is nil")
	}
	if s3.strmap.depth != 1 {
		t.Fatalf("expected compacted depth=1 after k3, got %d", s3.strmap.depth)
	}
	if s3.strmap.DenseStrs == nil {
		t.Fatalf("snapshot after compaction must be dense-backed")
	}

	if err := db.Set("k4", &Rec{Name: "d", Age: 13}); err != nil {
		t.Fatalf("Set k4: %v", err)
	}
	s4 := db.getSnapshot()
	if s4 == nil || s4.strmap == nil {
		t.Fatalf("snapshot after k4 is nil")
	}
	if s4.strmap.depth != 2 {
		t.Fatalf("expected delta-over-dense chain depth=2, got %d", s4.strmap.depth)
	}
	if s4.strmap.DenseStrs != nil {
		t.Fatalf("top snapshot layer should stay sparse delta-backed")
	}
	if len(s4.strmap.Strs) == 0 {
		t.Fatalf("top snapshot delta layer must contain sparse entries")
	}
	if s4.strmap.base == nil || s4.strmap.base.DenseStrs == nil {
		t.Fatalf("base layer must be dense-backed")
	}

	idx1, ok := s4.strmap.getIdxNoLock("k1")
	if !ok {
		t.Fatalf("missing idx for k1")
	}
	idx4, ok := s4.strmap.getIdxNoLock("k4")
	if !ok {
		t.Fatalf("missing idx for k4")
	}

	if got, ok := s4.strmap.getStringNoLock(idx1); !ok || got != "k1" {
		t.Fatalf("idx1 lookup mismatch: got=%q ok=%v", got, ok)
	}
	if got, ok := s4.strmap.getStringNoLock(idx4); !ok || got != "k4" {
		t.Fatalf("idx4 lookup mismatch: got=%q ok=%v", got, ok)
	}
}

func TestStrMapSnapshot_DenseLookupHonorsUsedBits(t *testing.T) {
	s := &strMapSnapshot{
		Next:      3,
		DenseStrs: []string{"", "k1", "", ""},
		DenseUsed: []bool{false, true, false, true},
	}

	if got, ok := s.getStringNoLock(1); !ok || got != "k1" {
		t.Fatalf("idx=1 mismatch: got=%q ok=%v", got, ok)
	}
	if got, ok := s.getStringNoLock(2); ok || got != "" {
		t.Fatalf("idx=2 should be missing hole, got=%q ok=%v", got, ok)
	}
	if got, ok := s.getStringNoLock(3); !ok || got != "" {
		t.Fatalf("idx=3 should resolve to empty-string key, got=%q ok=%v", got, ok)
	}
}

func TestSnapshotDelta_StoreIndexPersistsOverlayState(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persist_snapshot_delta.db")

	db, raw := openBoltAndNew[uint64, Rec](t, path, nil)

	if err := db.Set(1, &Rec{Name: "alice", Age: 10}); err != nil {
		_ = db.Close()
		t.Fatalf("Set alice: %v", err)
	}
	if err := db.Set(1, &Rec{Name: "bob", Age: 11}); err != nil {
		_ = db.Close()
		t.Fatalf("Set bob: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("raw close 1: %v", err)
	}

	db2, raw2 := openBoltAndNew[uint64, Rec](t, path, optsWithDefaults(&Options{
		DisableIndexRebuild: true,
	}))
	defer func() {
		_ = db2.Close()
		_ = raw2.Close()
	}()

	ids, err := db2.QueryKeys(qx.Query(qx.EQ("name", "bob")))
	if err != nil {
		t.Fatalf("Query bob: %v", err)
	}
	if !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("expected bob -> [1], got %v", ids)
	}

	ids, err = db2.QueryKeys(qx.Query(qx.EQ("name", "alice")))
	if err != nil {
		t.Fatalf("Query alice: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected alice to be absent after reopen, got %v", ids)
	}
}

func TestQueryWorksWhenSnapshotDeltaEnabled(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	if err := db.Set(1, &Rec{Name: "alice", Age: 20, Tags: []string{"go", "db"}}); err != nil {
		t.Fatalf("Set 1: %v", err)
	}
	if err := db.Set(2, &Rec{Name: "albert", Age: 30, Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set 2: %v", err)
	}
	if err := db.Set(3, &Rec{Name: "bob", Age: 40, Tags: []string{"rust"}}); err != nil {
		t.Fatalf("Set 3: %v", err)
	}

	if !db.snapshotHasAnyDelta() {
		t.Fatalf("expected snapshot delta to be present")
	}

	eq, err := db.QueryKeys(qx.Query(qx.EQ("name", "alice")))
	if err != nil {
		t.Fatalf("eq query: %v", err)
	}
	if !slices.Equal(eq, []uint64{1}) {
		t.Fatalf("eq mismatch: %v", eq)
	}

	prefix, err := db.QueryKeys(qx.Query(qx.PREFIX("name", "al")).By("name", qx.ASC).Max(10))
	if err != nil {
		t.Fatalf("prefix query: %v", err)
	}
	if !slices.Equal(prefix, []uint64{2, 1}) && !slices.Equal(prefix, []uint64{1, 2}) {
		t.Fatalf("prefix mismatch: %v", prefix)
	}

	rng, err := db.QueryKeys(qx.Query(qx.GTE("age", 30)).By("age", qx.ASC).Max(10))
	if err != nil {
		t.Fatalf("range query: %v", err)
	}
	if !slices.Equal(rng, []uint64{2, 3}) && !slices.Equal(rng, []uint64{3, 2}) {
		t.Fatalf("range mismatch: %v", rng)
	}

	has, err := db.QueryKeys(qx.Query(qx.HAS("tags", []string{"go", "db"})).Max(10))
	if err != nil {
		t.Fatalf("has query: %v", err)
	}
	if !slices.Equal(has, []uint64{1}) {
		t.Fatalf("has mismatch: %v", has)
	}

	hasAny, err := db.QueryKeys(qx.Query(qx.HASANY("tags", []string{"db", "rust"})).Max(10))
	if err != nil {
		t.Fatalf("hasany query: %v", err)
	}
	if !slices.Equal(hasAny, []uint64{1, 3}) && !slices.Equal(hasAny, []uint64{3, 1}) {
		t.Fatalf("hasany mismatch: %v", hasAny)
	}
}

func TestSnapshotDelta_UsesCandidateOrderWithoutMaterializedFallback(t *testing.T) {
	var events []TraceEvent
	db, _ := openTempDBUint64(t, &Options{
		TraceSink: func(ev TraceEvent) {
			events = append(events, ev)
		},
		TraceSampleEvery: 1,
	})

	if err := db.Set(1, &Rec{Meta: Meta{Country: "NL"}, Active: true, Age: 30}); err != nil {
		t.Fatalf("Set 1: %v", err)
	}
	if err := db.Set(2, &Rec{Meta: Meta{Country: "DE"}, Active: true, Age: 20}); err != nil {
		t.Fatalf("Set 2: %v", err)
	}
	if err := db.Set(3, &Rec{Meta: Meta{Country: "NL"}, Active: false, Age: 10}); err != nil {
		t.Fatalf("Set 3: %v", err)
	}
	if err := db.Set(4, &Rec{Meta: Meta{Country: "US"}, Active: true, Age: 40}); err != nil {
		t.Fatalf("Set 4: %v", err)
	}

	q := qx.Query(
		qx.NOT(qx.EQ("country", "NL")),
		qx.EQ("active", true),
	).By("age", qx.ASC).Max(10)

	ids, err := db.queryInternal(q, true)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !slices.Equal(ids, []uint64{2, 4}) {
		t.Fatalf("query mismatch: %v", ids)
	}

	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	gotPlan := events[len(events)-1].Plan
	if gotPlan != string(PlanCandidateOrder) {
		t.Fatalf("unexpected plan: got=%q want=%q", gotPlan, PlanCandidateOrder)
	}
}

func TestSnapshotDelta_UsesOrderedPlanWithoutMaterializedFallback(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	if err := db.Set(1, &Rec{Meta: Meta{Country: "NL"}, Age: 30}); err != nil {
		t.Fatalf("Set 1: %v", err)
	}
	if err := db.Set(2, &Rec{Meta: Meta{Country: "DE"}, Age: 20}); err != nil {
		t.Fatalf("Set 2: %v", err)
	}
	if err := db.Set(3, &Rec{Meta: Meta{Country: "NL"}, Age: 10}); err != nil {
		t.Fatalf("Set 3: %v", err)
	}
	if err := db.Set(4, &Rec{Meta: Meta{Country: "US"}, Age: 40}); err != nil {
		t.Fatalf("Set 4: %v", err)
	}
	if err := db.Set(5, &Rec{Meta: Meta{Country: "US"}, Age: 50}); err != nil {
		t.Fatalf("Set 5: %v", err)
	}

	q := qx.Query(
		qx.NOT(qx.EQ("country", "NL")),
		qx.GTE("age", 15),
		qx.LTE("age", 45),
	).By("age", qx.ASC).Max(10)

	leaves, ok := collectAndLeaves(q.Expr)
	if !ok {
		t.Fatalf("collectAndLeaves failed")
	}
	preds, ok := db.buildPredicates(leaves)
	if !ok {
		t.Fatalf("buildPredicates failed")
	}
	defer releasePredicates(preds)

	ids, ok := db.execPlanOrderedBasic(q, preds, nil)
	if !ok {
		t.Fatalf("execPlanOrderedBasic must be applicable")
	}
	if !slices.Equal(ids, []uint64{2, 4}) {
		t.Fatalf("ordered basic mismatch: %v", ids)
	}

	ids2, err := db.queryInternal(q, true)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !slices.Equal(ids2, []uint64{2, 4}) {
		t.Fatalf("query mismatch: %v", ids2)
	}
}

func TestSnapshotDelta_UsesORNoOrderPlanWithoutMaterializedFallback(t *testing.T) {
	var events []TraceEvent
	db, _ := openTempDBUint64(t, &Options{
		AnalyzeInterval:  -1,
		TraceSink:        func(ev TraceEvent) { events = append(events, ev) },
		TraceSampleEvery: 1,
	})
	_ = seedData(t, db, 5_000)

	// Force fresh delta on top of loaded base to ensure delta-safe pipeline is used.
	if err := db.Set(9_001, &Rec{Meta: Meta{Country: "NL"}, Name: "alice", Active: true, Age: 33}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if !db.snapshotHasAnyDelta() {
		t.Fatalf("expected snapshot delta to be present")
	}

	q := qx.Query(
		qx.OR(
			qx.EQ("active", true),
			qx.EQ("name", "alice"),
		),
	).Max(120)

	ids, err := db.queryInternal(q, true)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(ids) == 0 {
		t.Fatalf("expected non-empty result")
	}
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	plan := events[len(events)-1].Plan
	if !strings.HasPrefix(plan, "plan_or_merge_") {
		t.Fatalf("unexpected plan: got=%q want prefix %q", plan, "plan_or_merge_")
	}
}

func TestSnapshotDelta_UsesOROrderStreamPlanWithoutMaterializedFallback(t *testing.T) {
	var events []TraceEvent
	db, _ := openTempDBUint64(t, &Options{
		AnalyzeInterval:        -1,
		CalibrationEnabled:     true,
		CalibrationSampleEvery: 1,
		TraceSink:              func(ev TraceEvent) { events = append(events, ev) },
		TraceSampleEvery:       1,
	})

	if err := db.Set(1, &Rec{Name: "alice", Meta: Meta{Country: "NL"}, Age: 30}); err != nil {
		t.Fatalf("Set 1: %v", err)
	}
	if err := db.Set(2, &Rec{Name: "bob", Meta: Meta{Country: "DE"}, Age: 20}); err != nil {
		t.Fatalf("Set 2: %v", err)
	}
	if err := db.Set(3, &Rec{Name: "alina", Meta: Meta{Country: "US"}, Age: 10}); err != nil {
		t.Fatalf("Set 3: %v", err)
	}
	if err := db.Set(4, &Rec{Name: "tom", Meta: Meta{Country: "RU"}, Age: 40}); err != nil {
		t.Fatalf("Set 4: %v", err)
	}
	if !db.snapshotHasAnyDelta() {
		t.Fatalf("expected snapshot delta to be present")
	}

	err := db.SetCalibrationSnapshot(CalibrationSnapshot{
		UpdatedAt: time.Now(),
		Multipliers: map[string]float64{
			string(PlanORMergeOrderStream): 0.05,
			string(PlanORMergeOrderMerge):  3.0,
		},
	})
	if err != nil {
		t.Fatalf("SetCalibrationSnapshot: %v", err)
	}

	q := qx.Query(
		qx.OR(
			qx.PREFIX("name", "ali"),
			qx.EQ("country", "DE"),
		),
	).By("age", qx.ASC).Max(10)

	ids, err := db.queryInternal(q, true)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !slices.Equal(ids, []uint64{3, 2, 1}) {
		t.Fatalf("query mismatch: %v", ids)
	}
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	plan := events[len(events)-1].Plan
	if plan != string(PlanORMergeOrderStream) {
		t.Fatalf("unexpected plan: got=%q want=%q", plan, PlanORMergeOrderStream)
	}
}

func TestSnapshotDelta_UsesBitmapFallbackForSafeOverlayShape(t *testing.T) {
	var events []TraceEvent
	db, _ := openTempDBUint64(t, &Options{
		AnalyzeInterval:  -1,
		TraceSink:        func(ev TraceEvent) { events = append(events, ev) },
		TraceSampleEvery: 1,
	})

	if err := db.Set(1, &Rec{Meta: Meta{Country: "NL"}, Age: 10}); err != nil {
		t.Fatalf("Set 1: %v", err)
	}
	if err := db.Set(2, &Rec{Meta: Meta{Country: "DE"}, Age: 20}); err != nil {
		t.Fatalf("Set 2: %v", err)
	}
	if !db.snapshotHasAnyDelta() {
		t.Fatalf("expected snapshot delta to be present")
	}

	ids, err := db.queryInternal(qx.Query(qx.EQ("country", "NL")), true)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("query mismatch: %v", ids)
	}
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	plan := events[len(events)-1].Plan
	if plan != string(PlanBitmap) {
		t.Fatalf("unexpected plan: got=%q want=%q", plan, PlanBitmap)
	}
}

func TestSnapshotDelta_UsesBitmapFallbackForArrayOrder(t *testing.T) {
	var events []TraceEvent
	db, _ := openTempDBUint64(t, &Options{
		AnalyzeInterval:  -1,
		TraceSink:        func(ev TraceEvent) { events = append(events, ev) },
		TraceSampleEvery: 1,
	})

	if err := db.Set(1, &Rec{Name: "alice", Tags: []string{"go"}, Age: 10}); err != nil {
		t.Fatalf("Set 1: %v", err)
	}
	if err := db.Set(2, &Rec{Name: "bob", Tags: []string{"go", "db"}, Age: 20}); err != nil {
		t.Fatalf("Set 2: %v", err)
	}
	if !db.snapshotHasAnyDelta() {
		t.Fatalf("expected snapshot delta to be present")
	}

	q := qx.Query(qx.GTE("age", 0)).ByArrayCount("tags", qx.DESC)
	ids, err := db.queryInternal(q, true)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !slices.Equal(ids, []uint64{2, 1}) {
		t.Fatalf("query mismatch: %v", ids)
	}
	if len(events) == 0 {
		t.Fatalf("expected trace event")
	}
	plan := events[len(events)-1].Plan
	if plan != string(PlanBitmap) {
		t.Fatalf("unexpected plan: got=%q want=%q", plan, PlanBitmap)
	}
}

func TestSnapshotDelta_DecideOrderedByCost_UsesOverlayWhenBaseOrderSliceEmpty(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options{
		AnalyzeInterval: -1,
	})

	if err := db.Set(1, &Rec{Meta: Meta{Country: "NL"}, Age: 10}); err != nil {
		t.Fatalf("Set 1: %v", err)
	}
	if err := db.Set(2, &Rec{Meta: Meta{Country: "DE"}, Age: 20}); err != nil {
		t.Fatalf("Set 2: %v", err)
	}
	if !db.snapshotHasAnyDelta() {
		t.Fatalf("expected snapshot delta to be present")
	}

	s := db.getSnapshot()
	ageDelta := s.fieldDelta("age")
	if ageDelta == nil || ageDelta.keyCount() == 0 {
		t.Fatalf("expected non-empty age delta")
	}

	emptyAgeBase := []index{}
	viewSnap := &indexSnapshot{
		txID:         s.txID,
		index:        s.index,
		indexView:    map[string]*[]index{"age": &emptyAgeBase},
		lenIndex:     s.lenIndex,
		lenIndexView: s.lenIndexView,
		indexDelta:   s.indexDelta,
		lenIdxDelta:  s.lenIdxDelta,
		indexLayer:   s.indexLayer,
		lenLayer:     s.lenLayer,
		indexDCount:  s.indexDCount,
		lenDCount:    s.lenDCount,
		universe:     s.universe,
		universeAdd:  s.universeAdd,
		universeRem:  s.universeRem,
		strmap:       s.strmap,

		matPredCacheMaxEntries:          s.matPredCacheMaxEntries,
		matPredCacheMaxEntriesWithDelta: s.matPredCacheMaxEntriesWithDelta,
		matPredCacheMaxBitmapCard:       s.matPredCacheMaxBitmapCard,
	}
	view := db.makeQueryView(viewSnap)

	q := qx.Query(
		qx.NOT(qx.EQ("country", "US")),
		qx.GTE("age", 5),
		qx.LTE("age", 30),
	).By("age", qx.ASC).Max(10)
	leaves, ok := collectAndLeaves(q.Expr)
	if !ok {
		t.Fatalf("collectAndLeaves failed")
	}

	decision := view.decideOrderedByCost(q, leaves)
	if !decision.use && decision.orderedCost == 0 && decision.fallbackCost == 0 && decision.expectedProbeRows == 0 {
		t.Fatalf("expected non-zero ordered decision with overlay delta and empty base order slice")
	}
}

func TestSnapshotDelta_CompactionIntoBase(t *testing.T) {
	opts := optsWithDefaults(&Options{
		SnapshotDeltaCompactFieldKeys:           1,
		SnapshotDeltaCompactMaxFieldsPerPublish: 16,
		SnapshotDeltaCompactUniverseOps:         1,
	})

	db, _ := openTempDBUint64(t, opts)

	waitForSnapshotState(t, 500*time.Millisecond, "options propagated", func() bool {
		return db.options.SnapshotDeltaCompactFieldKeys == 1 &&
			db.options.SnapshotDeltaCompactMaxFieldsPerPublish == 16 &&
			db.options.SnapshotDeltaCompactUniverseOps == 1
	})

	if err := db.Set(1, &Rec{Name: "alice", Age: 10}); err != nil {
		t.Fatalf("Set 1: %v", err)
	}
	if err := db.Set(1, &Rec{Name: "bob", Age: 20}); err != nil {
		t.Fatalf("Set 2: %v", err)
	}

	waitForSnapshotState(t, 500*time.Millisecond, "delta compacted into base", func() bool {
		s := db.getSnapshot()
		if s.index["name"] == nil {
			return false
		}
		bm := findIndex(s.index["name"], "bob")
		if bm.IsEmpty() || !bm.Contains(1) {
			return false
		}
		if bm := findIndex(s.index["name"], "alice"); !bm.IsEmpty() && bm.Contains(1) {
			return false
		}
		if d := s.fieldDelta("name"); d != nil && d.keyCount() > 0 {
			return false
		}
		return s.universeAdd == nil && s.universeRem == nil
	})

	ids, err := db.QueryKeys(qx.Query(qx.EQ("name", "bob")))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("query mismatch: %v", ids)
	}
}

func TestSnapshotDelta_UniverseBasePreservedAcrossNonUniverseWriteAfterCompaction(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options{
		SnapshotDeltaCompactUniverseOps: 1,
	})
	if err := db.Set(1, &Rec{Name: "a", Age: 10}); err != nil {
		t.Fatalf("Set insert: %v", err)
	}

	if got, err := db.Count(nil); err != nil {
		t.Fatalf("Count after insert: %v", err)
	} else if got != 1 {
		t.Fatalf("count after insert mismatch: got=%d want=1", got)
	}

	if err := db.Set(1, &Rec{Name: "b", Age: 10}); err != nil {
		t.Fatalf("Set update: %v", err)
	}

	if got, err := db.Count(nil); err != nil {
		t.Fatalf("Count after update: %v", err)
	} else if got != 1 {
		t.Fatalf("count after update mismatch: got=%d want=1", got)
	}

	ids, err := db.QueryKeys(qx.Query(qx.EQ("name", "b")))
	if err != nil {
		t.Fatalf("Query name=b: %v", err)
	}
	if !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("expected keys(name=b)=[1], got %v", ids)
	}
}

func TestSnapshotDelta_DefaultLimitsEnabled(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	if db.options.SnapshotDeltaLayerMaxDepth <= 0 {
		t.Fatalf("SnapshotDeltaLayerMaxDepth must be enabled by default")
	}
}

func TestSnapshotDelta_LayerDepthIsBounded(t *testing.T) {
	maxDepth := 2
	db, _ := openTempDBUint64(t, &Options{
		SnapshotDeltaLayerMaxDepth:              maxDepth,
		SnapshotDeltaCompactMaxFieldsPerPublish: -1,
	})
	for i := 0; i < 6; i++ {
		if err := db.Set(1, &Rec{Name: string(rune('a' + i)), Age: 20}); err != nil {
			t.Fatalf("Set #%d: %v", i, err)
		}
	}

	waitForSnapshotState(t, 500*time.Millisecond, "delta layer depth compaction", func() bool {
		s := db.getSnapshot()
		return s.indexLayer == nil || s.indexLayer.depth <= maxDepth
	})
}

func TestSnapshotCompactor_IdleForceCompactsDelta(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options{
		SnapshotDeltaCompactFieldKeys:                    1 << 20,
		SnapshotDeltaCompactFieldOps:                     1 << 20,
		SnapshotDeltaCompactUniverseOps:                  1 << 20,
		SnapshotDeltaCompactMaxFieldsPerPublish:          64,
		SnapshotCompactorRequestEveryNWrites:             1_000_000,
		SnapshotCompactorIdleInterval:                    20 * time.Millisecond,
		SnapshotCompactorMaxIterationsPerRun:             4,
		SnapshotMaterializedPredCacheMaxEntries:          -1,
		SnapshotMaterializedPredCacheMaxEntriesWithDelta: -1,
	})

	if err := db.Set(1, &Rec{Name: "alice", Age: 10}); err != nil {
		t.Fatalf("Set 1: %v", err)
	}
	if !db.snapshotHasAnyDelta() {
		t.Fatalf("expected layered delta right after write")
	}

	waitForSnapshotState(t, 500*time.Millisecond, "idle force-drain compaction", func() bool {
		return !db.snapshotHasAnyDelta()
	})

	ids, err := db.QueryKeys(qx.Query(qx.EQ("name", "alice")))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("query mismatch: got=%v want=[1]", ids)
	}
}

func TestSnapshotCompactor_IdlePrunesRegistryToLatest(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options{
		SnapshotDeltaCompactFieldKeys:           1 << 20,
		SnapshotDeltaCompactFieldOps:            1 << 20,
		SnapshotDeltaCompactUniverseOps:         1 << 20,
		SnapshotDeltaCompactMaxFieldsPerPublish: 64,
		SnapshotCompactorRequestEveryNWrites:    1_000_000,
		SnapshotCompactorIdleInterval:           20 * time.Millisecond,
		SnapshotCompactorMaxIterationsPerRun:    4,
		SnapshotRegistryMax:                     1024,
	})

	for i := 1; i <= 8; i++ {
		if err := db.Set(uint64(i), &Rec{Name: fmt.Sprintf("u-%d", i), Age: i}); err != nil {
			t.Fatalf("Set #%d: %v", i, err)
		}
	}

	waitForSnapshotState(t, 700*time.Millisecond, "registry collapsed to latest snapshot", func() bool {
		db.snapshot.mu.RLock()
		defer db.snapshot.mu.RUnlock()

		if len(db.snapshot.byTx) != 1 {
			return false
		}
		latest := db.getSnapshot().txID
		ref := db.snapshot.byTx[latest]
		return ref != nil && ref.snap != nil && ref.refs.Load() == 0
	})
}

func TestSnapshotCompactor_IdlePrunesAfterPinnedSnapshotReleased(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options{
		SnapshotDeltaCompactFieldKeys:           1 << 20,
		SnapshotDeltaCompactFieldOps:            1 << 20,
		SnapshotDeltaCompactUniverseOps:         1 << 20,
		SnapshotDeltaCompactMaxFieldsPerPublish: 64,
		SnapshotCompactorRequestEveryNWrites:    1_000_000,
		SnapshotCompactorIdleInterval:           20 * time.Millisecond,
		SnapshotCompactorMaxIterationsPerRun:    4,
		SnapshotRegistryMax:                     1024,
	})

	if err := db.Set(1, &Rec{Name: "u-1", Age: 1}); err != nil {
		t.Fatalf("Set #1: %v", err)
	}
	pinnedTx := db.getSnapshot().txID
	if _, ok := db.pinByTxID(pinnedTx); !ok {
		t.Fatalf("pinByTxID(%d) failed", pinnedTx)
	}

	if err := db.Set(2, &Rec{Name: "u-2", Age: 2}); err != nil {
		t.Fatalf("Set #2: %v", err)
	}

	waitForSnapshotState(t, 700*time.Millisecond, "force pass observed pinned snapshot", func() bool {
		return db.snapshot.compactPinsBlocked.Load()
	})

	db.unpinByTxID(pinnedTx)

	waitForSnapshotState(t, 700*time.Millisecond, "registry pruned after pinned release", func() bool {
		db.snapshot.mu.RLock()
		defer db.snapshot.mu.RUnlock()
		latest := db.getSnapshot().txID
		return len(db.snapshot.byTx) == 1 && db.snapshot.byTx[pinnedTx] == nil && db.snapshot.byTx[latest] != nil
	})
}

func TestMergeUniverseDelta_DoesNotMutatePreviousBitmaps(t *testing.T) {
	prevAdd := roaring64.BitmapOf(1)
	prevDrop := roaring64.BitmapOf(3)
	add := roaring64.BitmapOf(2, 3)
	drop := roaring64.BitmapOf(1, 4)

	gotAdd, gotDrop := mergeUniverseDelta(prevAdd, prevDrop, add, drop)

	if !gotAdd.Equals(roaring64.BitmapOf(2)) {
		t.Fatalf("unexpected add bitmap: %v", gotAdd.ToArray())
	}
	if !gotDrop.Equals(roaring64.BitmapOf(4)) {
		t.Fatalf("unexpected drop bitmap: %v", gotDrop.ToArray())
	}

	if !prevAdd.Equals(roaring64.BitmapOf(1)) {
		t.Fatalf("prevAdd mutated: %v", prevAdd.ToArray())
	}
	if !prevDrop.Equals(roaring64.BitmapOf(3)) {
		t.Fatalf("prevDrop mutated: %v", prevDrop.ToArray())
	}
}

func TestOverlayDistinctCount_ExcludesFullyDeletedBucket(t *testing.T) {
	base := []index{
		{Key: indexKeyFromString("a"), IDs: postingOf(1)},
		{Key: indexKeyFromString("b"), IDs: postingOf(2)},
	}
	delta := &fieldIndexDelta{
		byKey: map[string]indexDeltaEntry{
			"a": {delSingle: 1, delSingleSet: true},
		},
	}
	ov := newFieldOverlay(&base, delta)

	total := overlayDistinctTotalCount(ov)
	if total != 1 {
		t.Fatalf("total distinct mismatch: got=%d want=1", total)
	}

	rangeA := ov.rangeForBounds(rangeBounds{
		has:   true,
		hasLo: true,
		loKey: "a",
		loInc: true,
		hasHi: true,
		hiKey: "a",
		hiInc: true,
	})
	if got := overlayDistinctRangeCount(ov, rangeA); got != 0 {
		t.Fatalf("range distinct for key=a mismatch: got=%d want=0", got)
	}
}

func TestPinSnapshotByTxIDWait_FailsFastWhenLatestPassedTarget(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	if err := db.Set(1, &Rec{Name: "a", Age: 10}); err != nil {
		t.Fatalf("Set 1: %v", err)
	}
	targetTx := db.getSnapshot().txID

	if err := db.Set(2, &Rec{Name: "b", Age: 20}); err != nil {
		t.Fatalf("Set 2: %v", err)
	}
	latestTx := db.getSnapshot().txID
	if latestTx <= targetTx {
		t.Fatalf("expected latest txID > target txID, got latest=%d target=%d", latestTx, targetTx)
	}

	db.snapshot.mu.Lock()
	delete(db.snapshot.byTx, targetTx)
	db.snapshot.mu.Unlock()

	start := time.Now()
	snap, ok := db.pinByTxIDWait(targetTx, 500*time.Millisecond)
	elapsed := time.Since(start)
	if ok || snap != nil {
		t.Fatalf("expected pin wait to fail for missing old txID=%d", targetTx)
	}
	if elapsed >= 250*time.Millisecond {
		t.Fatalf("expected fast failure, elapsed=%v", elapsed)
	}
}

func TestQuery_RetriesWithNewTxWhenSnapshotMissingForCurrentTx(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options{
		SnapshotPinWaitTimeout: 100 * time.Millisecond,
	})
	if err := db.Set(1, &Rec{Name: "alice", Age: 10}); err != nil {
		t.Fatalf("Set seed: %v", err)
	}

	missingTx := db.getSnapshot().txID
	db.snapshot.mu.Lock()
	delete(db.snapshot.byTx, missingTx)
	db.snapshot.mu.Unlock()

	writeErr := make(chan error, 1)
	go func() {
		time.Sleep(20 * time.Millisecond)
		writeErr <- db.Set(2, &Rec{Name: "bob", Age: 30})
	}()

	items, err := db.Query(qx.Query(qx.EQ("name", "alice")))
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(items) != 1 || items[0] == nil || items[0].Name != "alice" {
		t.Fatalf("unexpected Query result: %#v", items)
	}

	if err = <-writeErr; err != nil {
		t.Fatalf("writer Set: %v", err)
	}
}

func TestQuery_UsesLatestSnapshotWhenBoltTxAheadAndNotPending(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options{
		SnapshotPinWaitTimeout: 200 * time.Millisecond,
	})
	if err := db.Set(1, &Rec{Name: "alice", Age: 10}); err != nil {
		t.Fatalf("Set seed: %v", err)
	}

	if err := db.bolt.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("__ext_tx__"))
		if err != nil {
			return err
		}
		return b.Put([]byte("k"), []byte("v"))
	}); err != nil {
		t.Fatalf("external update: %v", err)
	}

	start := time.Now()
	items, err := db.Query(qx.Query(qx.EQ("name", "alice")))
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(items) != 1 || items[0] == nil || items[0].Name != "alice" {
		t.Fatalf("unexpected Query result: %#v", items)
	}
	if elapsed >= 150*time.Millisecond {
		t.Fatalf("expected fast fallback, elapsed=%v", elapsed)
	}
}

func TestQuery_UsesLatestSnapshotWhenRegistryHasHoleForCurrentTx(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options{
		SnapshotPinWaitTimeout: 200 * time.Millisecond,
	})
	if err := db.Set(1, &Rec{Name: "alice", Age: 10}); err != nil {
		t.Fatalf("Set alice: %v", err)
	}
	if err := db.Set(2, &Rec{Name: "bob", Age: 20}); err != nil {
		t.Fatalf("Set bob: %v", err)
	}
	targetTx := db.getSnapshot().txID

	db.snapshot.mu.Lock()
	delete(db.snapshot.byTx, targetTx)
	db.snapshot.mu.Unlock()

	start := time.Now()
	items, err := db.Query(qx.Query(qx.EQ("name", "bob")))
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(items) != 1 || items[0] == nil || items[0].Name != "bob" {
		t.Fatalf("unexpected Query result: %#v", items)
	}
	if elapsed >= 150*time.Millisecond {
		t.Fatalf("expected fast fallback for registry hole, elapsed=%v", elapsed)
	}
}

func TestPinSnapshotByTxIDWait_FailsFastWhenTargetAheadAndNotPending(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	if err := db.Set(1, &Rec{Name: "seed", Age: 10}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	latest := db.getSnapshot().txID
	target := latest + 100

	start := time.Now()
	snap, ok := db.pinByTxIDWait(target, 500*time.Millisecond)
	elapsed := time.Since(start)
	if ok || snap != nil {
		t.Fatalf("expected fast fail for non-pending future txID=%d", target)
	}
	if elapsed >= 250*time.Millisecond {
		t.Fatalf("expected fast failure, elapsed=%v", elapsed)
	}
}

func TestPinSnapshotByTxIDWait_WaitsForPendingAndSucceeds(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	if err := db.Set(1, &Rec{Name: "seed", Age: 10}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	latest := db.getSnapshot().txID
	target := latest + 1
	db.markPending(target)
	defer db.clearPending(target)

	done := make(chan struct{})
	go func() {
		time.Sleep(30 * time.Millisecond)
		db.registerSnapshot(&indexSnapshot{txID: target})
		close(done)
	}()

	snap, ok := db.pinByTxIDWait(target, 500*time.Millisecond)
	if !ok || snap == nil {
		t.Fatalf("expected pending txID pin to succeed")
	}
	db.unpinByTxID(target)
	<-done
}

func TestPinSnapshotByTxIDWait_FailsFastWhenLatestEqualsTargetButRegistryMissing(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	if err := db.Set(1, &Rec{Name: "seed", Age: 10}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	target := db.getSnapshot().txID

	db.snapshot.mu.Lock()
	delete(db.snapshot.byTx, target)
	db.snapshot.mu.Unlock()

	start := time.Now()
	snap, ok := db.pinByTxIDWait(target, 500*time.Millisecond)
	elapsed := time.Since(start)
	if ok || snap != nil {
		t.Fatalf("expected fast fail for missing current txID=%d", target)
	}
	if elapsed >= 250*time.Millisecond {
		t.Fatalf("expected fast failure, elapsed=%v", elapsed)
	}
}

func TestPinSnapshotFloorByTxID_ReturnsNearestAvailableWhenExactMissing(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)
	if err := db.Set(1, &Rec{Name: "a", Age: 10}); err != nil {
		t.Fatalf("Set 1: %v", err)
	}
	tx1 := db.getSnapshot().txID
	if err := db.Set(2, &Rec{Name: "b", Age: 20}); err != nil {
		t.Fatalf("Set 2: %v", err)
	}
	tx2 := db.getSnapshot().txID
	if err := db.Set(3, &Rec{Name: "c", Age: 30}); err != nil {
		t.Fatalf("Set 3: %v", err)
	}
	tx3 := db.getSnapshot().txID
	if tx1 >= tx2 || tx2 >= tx3 {
		t.Fatalf("unexpected txID order: tx1=%d tx2=%d tx3=%d", tx1, tx2, tx3)
	}

	db.snapshot.mu.Lock()
	delete(db.snapshot.byTx, tx2)
	db.snapshot.mu.Unlock()

	snap, pinnedTx, ok := db.pinFloorByTxID(tx2)
	if !ok || snap == nil {
		t.Fatalf("expected floor snapshot for txID=%d", tx2)
	}
	if pinnedTx > tx2 {
		t.Fatalf("expected pinned tx <= target: pinned=%d target=%d", pinnedTx, tx2)
	}
	if pinnedTx == tx2 {
		t.Fatalf("expected exact missing txID, got pinned exact txID=%d", pinnedTx)
	}
	if snap.txID != pinnedTx {
		t.Fatalf("pinned snapshot tx mismatch: snap=%d pinned=%d", snap.txID, pinnedTx)
	}
	db.unpinByTxID(pinnedTx)
}

func TestSnapshotRegistry_PrunesPastPinnedHead(t *testing.T) {
	maxRegistry := 8
	db, _ := openTempDBUint64(t, &Options{
		SnapshotRegistryMax: uint(maxRegistry),
	})
	if err := db.Set(1, &Rec{Name: "seed", Age: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	pinnedTx := db.getSnapshot().txID
	if _, ok := db.pinByTxID(pinnedTx); !ok {
		t.Fatalf("failed to pin snapshot txID=%d", pinnedTx)
	}
	defer db.unpinByTxID(pinnedTx)

	for i := 2; i <= 220; i++ {
		if err := db.Set(uint64(i), &Rec{Name: "u", Age: i}); err != nil {
			t.Fatalf("Set #%d: %v", i, err)
		}
	}

	db.snapshot.mu.Lock()
	mapLen := len(db.snapshot.byTx)
	_, pinnedStillExists := db.snapshot.byTx[pinnedTx]
	db.snapshot.mu.Unlock()

	if !pinnedStillExists {
		t.Fatalf("pinned snapshot disappeared: txID=%d", pinnedTx)
	}
	if mapLen > maxRegistry+2 {
		t.Fatalf("snapshot registry grew unexpectedly: len=%d max=%d", mapLen, maxRegistry)
	}
}

func TestSnapshotRegistry_CompactsHugeOrderWithPinnedHead(t *testing.T) {
	maxRegistry := 4
	db, _ := openTempDBUint64(t, &Options{
		SnapshotRegistryMax: uint(maxRegistry),
	})

	db.snapshot.mu.Lock()
	db.snapshot.byTx = make(map[uint64]*snapshotRef, 8)
	db.snapshot.order = db.snapshot.order[:0]
	db.snapshot.head = 0

	for tx := uint64(1); tx <= 2048; tx++ {
		db.snapshot.order = append(db.snapshot.order, tx)
	}

	head := &snapshotRef{snap: &indexSnapshot{txID: 1}}
	head.refs.Add(1) // keep head pinned
	db.snapshot.byTx[1] = head
	db.snapshot.byTx[2045] = &snapshotRef{snap: &indexSnapshot{txID: 2045}}
	db.snapshot.byTx[2046] = &snapshotRef{snap: &indexSnapshot{txID: 2046}}
	db.snapshot.byTx[2047] = &snapshotRef{snap: &indexSnapshot{txID: 2047}}
	db.snapshot.byTx[2048] = &snapshotRef{snap: &indexSnapshot{txID: 2048}}
	db.snapshot.current.Store(&indexSnapshot{txID: 2048})

	before := len(db.snapshot.order)
	db.pruneSnapshotsLocked()
	after := len(db.snapshot.order)
	mapLen := len(db.snapshot.byTx)
	db.snapshot.mu.Unlock()

	if before < 2000 {
		t.Fatalf("unexpected setup: before=%d", before)
	}
	if after > mapLen+1 {
		t.Fatalf("snapshot order was not compacted enough: before=%d after=%d map=%d", before, after, mapLen)
	}
	if mapLen > maxRegistry+1 {
		t.Fatalf("snapshot map too large after prune: len=%d max=%d", mapLen, maxRegistry)
	}
}

func TestAccumulateDeltaLayerState_SkipsCompactionForLargeBaseSmallDelta(t *testing.T) {
	opt := DefaultOptions()
	opt.SnapshotDeltaCompactFieldKeys = 1
	opt.SnapshotDeltaCompactFieldOps = 0
	opt.SnapshotDeltaCompactMaxFieldsPerPublish = 4

	baseSlice := make([]index, 0, defaultSnapshotDeltaCompactLargeBaseFieldKeys+1)
	for i := 0; i <= defaultSnapshotDeltaCompactLargeBaseFieldKeys; i++ {
		baseSlice = append(baseSlice, index{Key: indexKeyFromU64(uint64(i + 1))})
	}
	base := map[string]*[]index{"hot": &baseSlice}
	origPtr := base["hot"]

	changes := map[string]map[string]indexDeltaEntry{
		"hot": {
			"k999": {addSingle: 999, addSingleSet: true},
		},
	}

	nextBase, nextLayer, _ := accumulateDeltaLayerState(base, nil, 0, changes, opt)
	if nextBase["hot"] != origPtr {
		t.Fatalf("expected no base compaction for large base with tiny delta")
	}

	d := lookupLayerFieldDelta(nextLayer, "hot")
	if d == nil || d.keyCount() == 0 {
		t.Fatalf("expected delta to remain layered for large base")
	}
}

func TestAccumulateDeltaLayerState_ForceCompactionByOps(t *testing.T) {
	opt := DefaultOptions()
	opt.SnapshotDeltaCompactFieldKeys = 1
	opt.SnapshotDeltaCompactFieldOps = 1
	opt.SnapshotDeltaCompactMaxFieldsPerPublish = 4

	baseSlice := make([]index, 0, defaultSnapshotDeltaCompactLargeBaseFieldKeys+1)
	for i := 0; i <= defaultSnapshotDeltaCompactLargeBaseFieldKeys; i++ {
		baseSlice = append(baseSlice, index{Key: indexKeyFromU64(uint64(i + 1))})
	}
	base := map[string]*[]index{"hot": &baseSlice}
	origPtr := base["hot"]

	forceOps := roaring64.NewBitmap()
	forceOps.AddRange(1, defaultSnapshotDeltaCompactForceFieldOps+1)

	changes := map[string]map[string]indexDeltaEntry{
		"hot": {
			"k999": {add: forceOps},
		},
	}

	nextBase, nextLayer, _ := accumulateDeltaLayerState(base, nil, 0, changes, opt)
	if nextBase["hot"] == origPtr {
		t.Fatalf("expected force-compaction to materialize new base field")
	}
	if d := lookupLayerFieldDelta(nextLayer, "hot"); d != nil {
		t.Fatalf("expected no effective layered delta after force-compaction")
	}
}

func TestLookupLayerFieldDeltaWithScratch_MaterializedResultIndependentFromScratch(t *testing.T) {
	baseHot := buildFieldDeltaPatch(map[string]indexDeltaEntry{
		"a": {addSingle: 1, addSingleSet: true},
	}, false)
	topHot := buildFieldDeltaPatch(map[string]indexDeltaEntry{
		"b": {addSingle: 2, addSingleSet: true},
	}, false)
	baseCold := buildFieldDeltaPatch(map[string]indexDeltaEntry{
		"x": {addSingle: 10, addSingleSet: true},
	}, false)
	topCold := buildFieldDeltaPatch(map[string]indexDeltaEntry{
		"y": {addSingle: 20, addSingleSet: true},
	}, false)

	layer := &fieldDeltaLayer{
		fields: map[string]*fieldIndexDelta{
			"hot":  baseHot,
			"cold": baseCold,
		},
		depth: 1,
	}
	layer = &fieldDeltaLayer{
		parent: layer,
		fields: map[string]*fieldIndexDelta{
			"hot":  topHot,
			"cold": topCold,
		},
		depth: 2,
	}

	scratch := getLayerFieldDeltaMergeScratch()
	defer releaseLayerFieldDeltaMergeScratch(scratch)

	materialized := lookupLayerFieldDeltaWithScratch(layer, "hot", scratch)
	if materialized == nil || materialized.keyCount() != 2 {
		t.Fatalf("expected merged materialized delta for hot")
	}
	if _, ok := materialized.get("a"); !ok {
		t.Fatalf("expected key a in materialized delta")
	}
	if _, ok := materialized.get("b"); !ok {
		t.Fatalf("expected key b in materialized delta")
	}

	_, _ = lookupLayerFieldDeltaBorrowedWithScratch(layer, "cold", scratch)
	if _, ok := materialized.get("a"); !ok {
		t.Fatalf("materialized delta unexpectedly changed after scratch reuse")
	}
	if _, ok := materialized.get("b"); !ok {
		t.Fatalf("materialized delta unexpectedly changed after scratch reuse")
	}
}

func TestSnapshotMaterializedPredCache_DeltaSnapshotUsesReducedLimit(t *testing.T) {
	s := &indexSnapshot{
		indexDCount: 1,
		indexDelta: map[string]*fieldIndexDelta{
			"f": {
				byKey: map[string]indexDeltaEntry{
					"x": {addSingle: 1, addSingleSet: true},
				},
			},
		},
		matPredCacheMaxEntries:          4,
		matPredCacheMaxEntriesWithDelta: 1,
		matPredCacheMaxBitmapCard:       0,
	}
	s.storeMaterializedPred("k1", roaring64.BitmapOf(1))
	s.storeMaterializedPred("k2", roaring64.BitmapOf(2))

	if _, ok := s.loadMaterializedPred("k1"); !ok {
		t.Fatalf("expected cache hit for first entry")
	}
	if _, ok := s.loadMaterializedPred("k2"); ok {
		t.Fatalf("unexpected cache hit beyond delta cache limit")
	}
}

func TestSnapshotMaterializedPredCache_EnabledWhenNoDelta(t *testing.T) {
	s := &indexSnapshot{
		matPredCacheMaxEntries:          4,
		matPredCacheMaxEntriesWithDelta: 1,
		matPredCacheMaxBitmapCard:       0,
	}
	bm := roaring64.BitmapOf(1, 2, 3)

	s.storeMaterializedPred("k", bm)
	got, ok := s.loadMaterializedPred("k")
	if !ok || got == nil {
		t.Fatalf("expected cache hit for stable snapshot")
	}
	if got.GetCardinality() != 3 {
		t.Fatalf("unexpected cached cardinality: %d", got.GetCardinality())
	}
}

func TestSnapshotMaterializedPredCache_StoresNilOnFirstTouch(t *testing.T) {
	s := &indexSnapshot{
		matPredCacheMaxEntries:          4,
		matPredCacheMaxEntriesWithDelta: 1,
		matPredCacheMaxBitmapCard:       0,
	}
	s.storeMaterializedPred("empty", nil)

	got, ok := s.loadMaterializedPred("empty")
	if !ok {
		t.Fatalf("expected nil marker to be cached immediately")
	}
	if got != nil {
		t.Fatalf("expected nil marker in cache, got non-nil bitmap")
	}
}

func TestSnapshotMaterializedPredCache_SkipsHugeBitmap(t *testing.T) {
	s := &indexSnapshot{
		matPredCacheMaxEntries:          4,
		matPredCacheMaxEntriesWithDelta: 1,
		matPredCacheMaxBitmapCard:       2,
	}
	s.storeMaterializedPred("big", roaring64.BitmapOf(1, 2, 3))
	if _, ok := s.loadMaterializedPred("big"); ok {
		t.Fatalf("unexpected cache hit for oversized bitmap")
	}
}

func TestSnapshotMaterializedPredCache_OversizedHotSlotStoresBoundedStableEntries(t *testing.T) {
	s := &indexSnapshot{
		matPredCacheMaxEntries:          4,
		matPredCacheMaxEntriesWithDelta: 1,
		matPredCacheMaxBitmapCard:       2,
	}
	big1 := roaring64.BitmapOf(1, 2, 3)
	big2 := roaring64.BitmapOf(4, 5, 6)
	big3 := roaring64.BitmapOf(7, 8, 9)

	if !s.tryStoreMaterializedPredOversized("big1", big1) {
		t.Fatalf("expected first oversized bitmap to be accepted into hot-slot cache")
	}
	got1, ok := s.loadMaterializedPred("big1")
	if !ok || got1 != big1 {
		t.Fatalf("expected cached oversized bitmap for first hot slot entry")
	}

	if !s.tryStoreMaterializedPredOversized("big2", big2) {
		t.Fatalf("expected second oversized bitmap to be accepted into hot-slot cache")
	}
	got2, ok := s.loadMaterializedPred("big2")
	if !ok || got2 != big2 {
		t.Fatalf("expected cached oversized bitmap for second hot-slot entry")
	}

	if s.tryStoreMaterializedPredOversized("big3", big3) {
		t.Fatalf("expected oversized hot-slot cache to reject entries beyond the bounded limit")
	}
	if _, ok := s.loadMaterializedPred("big3"); ok {
		t.Fatalf("unexpected cache hit beyond oversized hot-slot limit")
	}
}

func TestSnapshotMaterializedPredCache_OversizedHotSlotDisabledForDeltaSnapshot(t *testing.T) {
	s := &indexSnapshot{
		indexDCount: 1,
		indexDelta: map[string]*fieldIndexDelta{
			"f": {
				byKey: map[string]indexDeltaEntry{
					"x": {addSingle: 1, addSingleSet: true},
				},
			},
		},
		matPredCacheMaxEntries:          4,
		matPredCacheMaxEntriesWithDelta: 1,
		matPredCacheMaxBitmapCard:       2,
	}
	big := roaring64.BitmapOf(1, 2, 3)

	if s.tryStoreMaterializedPredOversized("big", big) {
		t.Fatalf("unexpected oversized hot-slot store for delta snapshot")
	}
	if _, ok := s.loadMaterializedPred("big"); ok {
		t.Fatalf("unexpected cache hit for oversized bitmap on delta snapshot")
	}
}
