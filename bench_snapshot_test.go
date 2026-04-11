package rbi

import (
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/vapstack/qx"
)

type benchCacheModeKind uint8

const (
	benchCacheModeHot benchCacheModeKind = iota
	benchCacheModeColdFresh
	benchCacheModeColdTurnover
	benchCacheModeColdPinned
)

const benchPinnedSnapshotLimit = 4
const benchTurnoverRingSize = 64
const benchColdPoolWarmCycles = 2
const benchHotWarmMaxCycles = 6
const benchHotWarmStableCycles = 2

type benchCacheMode struct {
	suffix string
	kind   benchCacheModeKind
}

type benchTurnoverEntry[K ~string | ~uint64] struct {
	id        K
	basePatch []Field
	altPatch  []Field
	altActive bool
}

type benchTurnoverRing[K ~string | ~uint64] struct {
	next    int
	entries []benchTurnoverEntry[K]
}

type benchPinnedSnapshot struct {
	seq uint64
	ref *snapshotRef
}

type benchReadRunner[K ~string | ~uint64, V any] func(*testing.B, *DB[K, V], *qx.QX)

type benchReadModeState[K ~string | ~uint64, V any] struct {
	mode   benchCacheMode
	ring   *benchTurnoverRing[K]
	pinned []benchPinnedSnapshot
}

var benchCacheModes = []benchCacheMode{
	{suffix: "Hot", kind: benchCacheModeHot},
	{suffix: "ColdFresh", kind: benchCacheModeColdFresh},
	{suffix: "ColdTurnover", kind: benchCacheModeColdTurnover},
	{suffix: "ColdPinned", kind: benchCacheModeColdPinned},
}

var benchCacheModesShort = []benchCacheMode{
	{suffix: "Hot", kind: benchCacheModeHot},
	{suffix: "ColdTurnover", kind: benchCacheModeColdTurnover},
}

var userBenchTurnoverCountries = [...]string{"US", "NL", "DE", "PL", "SE", "FR", "GB", "ES"}
var userBenchTurnoverPlans = [...]string{"free", "basic", "pro", "enterprise"}
var userBenchTurnoverStatuses = [...]string{"active", "trial", "paused", "banned"}

var stressBenchTurnoverPlans = [...]string{"free", "starter", "pro", "enterprise"}
var stressBenchTurnoverStatuses = [...]string{"active", "inactive", "pending", "suspended", "banned"}

func activeBenchCacheModes() []benchCacheMode {
	if testing.Short() {
		return benchCacheModesShort
	}
	return benchCacheModes
}

func newBenchReadModeState[K ~string | ~uint64, V any](
	tb testing.TB,
	mode benchCacheMode,
	ring *benchTurnoverRing[K],
) *benchReadModeState[K, V] {
	tb.Helper()
	if mode.kind != benchCacheModeHot && (ring == nil || len(ring.entries) == 0) {
		tb.Fatalf("%s mode requires a non-empty turnover ring", mode.suffix)
	}
	return &benchReadModeState[K, V]{
		mode: mode,
		ring: ring,
	}
}

func (s *benchReadModeState[K, V]) beforeQuery(b *testing.B, db *DB[K, V]) {
	if s == nil || s.mode.kind == benchCacheModeHot {
		return
	}
	b.StopTimer()
	switch s.mode.kind {
	case benchCacheModeColdFresh:
		s.applyTurnover(b, db)
		db.getSnapshot().clearRuntimeCachesForTesting()
	case benchCacheModeColdTurnover:
		s.applyTurnover(b, db)
	case benchCacheModeColdPinned:
		s.pinCurrentSnapshot(b, db)
		s.applyTurnover(b, db)
		if len(s.pinned) > benchPinnedSnapshotLimit {
			oldest := s.pinned[0]
			s.pinned = s.pinned[1:]
			db.unpinSnapshotRef(oldest.seq, oldest.ref)
		}
	}
	b.StartTimer()
}

func (s *benchReadModeState[K, V]) close(tb testing.TB, db *DB[K, V]) {
	if s == nil {
		return
	}
	for i := range s.pinned {
		db.unpinSnapshotRef(s.pinned[i].seq, s.pinned[i].ref)
	}
	s.pinned = s.pinned[:0]
}

func (s *benchReadModeState[K, V]) applyTurnover(tb testing.TB, db *DB[K, V]) {
	tb.Helper()
	if s == nil || s.ring == nil || len(s.ring.entries) == 0 {
		return
	}
	entry := s.ring.entry()
	patch := entry.altPatch
	if entry.altActive {
		patch = entry.basePatch
	}
	if err := db.Patch(entry.id, patch); err != nil {
		tb.Fatalf("Patch(turnover %v): %v", entry.id, err)
	}
	entry.altActive = !entry.altActive
}

func (s *benchReadModeState[K, V]) pinCurrentSnapshot(tb testing.TB, db *DB[K, V]) {
	tb.Helper()
	snap := db.getSnapshot()
	got, ref, ok := db.pinSnapshotRefBySeq(snap.seq)
	if !ok || got != snap || ref == nil {
		tb.Fatalf("pinSnapshotRefBySeq(seq=%d) failed", snap.seq)
	}
	s.pinned = append(s.pinned, benchPinnedSnapshot{seq: snap.seq, ref: ref})
}

func (s *benchReadModeState[K, V]) warmPools(
	b *testing.B,
	db *DB[K, V],
	q *qx.QX,
	run benchReadRunner[K, V],
) {
	if s == nil || s.mode.kind == benchCacheModeHot || run == nil {
		return
	}
	cycles := benchColdPoolWarmCycles
	if s.mode.kind == benchCacheModeColdPinned {
		cycles = benchPinnedSnapshotLimit + 2
	}
	for i := 0; i < cycles; i++ {
		s.beforeQuery(b, db)
		run(b, db, q)
	}
}

func warmBenchReadHotSteady[K ~string | ~uint64, V any](
	b *testing.B,
	db *DB[K, V],
	q *qx.QX,
	run benchReadRunner[K, V],
) {
	if run == nil {
		return
	}
	stable := 0
	prevCount := db.getSnapshot().matPredCacheCount.Load()
	for i := 0; i < benchHotWarmMaxCycles; i++ {
		run(b, db, q)
		count := db.getSnapshot().matPredCacheCount.Load()
		if count == prevCount {
			stable++
			if stable >= benchHotWarmStableCycles {
				return
			}
			continue
		}
		prevCount = count
		stable = 0
	}
}

func (r *benchTurnoverRing[K]) entry() *benchTurnoverEntry[K] {
	entry := &r.entries[r.next]
	r.next++
	if r.next == len(r.entries) {
		r.next = 0
	}
	return entry
}

func benchTurnoverSampleOrdinals(total, limit int) []int {
	if total <= 0 || limit <= 0 {
		return nil
	}
	if total < limit {
		limit = total
	}
	out := make([]int, 0, limit)
	last := 0
	for i := 0; i < limit; i++ {
		ord := 1
		if limit > 1 {
			ord = 1 + (i*(total-1))/(limit-1)
		}
		if ord == last {
			continue
		}
		out = append(out, ord)
		last = ord
	}
	return out
}

func benchCloneStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

func benchToggleSliceValue(in []string, value string) []string {
	count := 0
	for _, v := range in {
		if v != value {
			count++
		}
	}
	if count != len(in) {
		out := make([]string, 0, count)
		for _, v := range in {
			if v != value {
				out = append(out, v)
			}
		}
		return out
	}
	out := benchCloneStrings(in)
	out = append(out, value)
	return out
}

func benchNextStringCycle(values []string, cur string) string {
	for i := range values {
		if values[i] == cur {
			return values[(i+1)%len(values)]
		}
	}
	return values[0]
}

func benchToggleInt(cur, minValue, maxValue int) int {
	if cur < maxValue {
		return cur + 1
	}
	if cur > minValue {
		return cur - 1
	}
	return cur
}

func benchToggleFloat(cur, delta, minValue, maxValue float64) float64 {
	next := cur + delta
	if next > maxValue {
		next = cur - delta
	}
	if next < minValue {
		next = cur
	}
	return math.Round(next*100) / 100
}

func userBenchBasePatch(rec *UserBench) []Field {
	return []Field{
		{Name: "country", Value: rec.Country},
		{Name: "plan", Value: rec.Plan},
		{Name: "status", Value: rec.Status},
		{Name: "age", Value: rec.Age},
		{Name: "score", Value: rec.Score},
		{Name: "tags", Value: benchCloneStrings(rec.Tags)},
		{Name: "roles", Value: benchCloneStrings(rec.Roles)},
	}
}

func userBenchAltPatch(rec *UserBench) []Field {
	return []Field{
		{Name: "country", Value: benchNextStringCycle(userBenchTurnoverCountries[:], rec.Country)},
		{Name: "plan", Value: benchNextStringCycle(userBenchTurnoverPlans[:], rec.Plan)},
		{Name: "status", Value: benchNextStringCycle(userBenchTurnoverStatuses[:], rec.Status)},
		{Name: "age", Value: benchToggleInt(rec.Age, 18, 77)},
		{Name: "score", Value: benchToggleFloat(rec.Score, 3.25, 0, 1000)},
		{Name: "tags", Value: benchToggleSliceValue(rec.Tags, "ops")},
		{Name: "roles", Value: benchToggleSliceValue(rec.Roles, "admin")},
	}
}

func stressBenchBasePatch(rec *StressBenchUser) []Field {
	return []Field{
		{Name: "country", Value: rec.Country},
		{Name: "plan", Value: rec.Plan},
		{Name: "status", Value: rec.Status},
		{Name: "age", Value: rec.Age},
		{Name: "score", Value: rec.Score},
		{Name: "is_verified", Value: rec.IsVerified},
		{Name: "created_at", Value: rec.CreatedAt},
		{Name: "last_login", Value: rec.LastLogin},
		{Name: "tags", Value: benchCloneStrings(rec.Tags)},
		{Name: "roles", Value: benchCloneStrings(rec.Roles)},
	}
}

func stressBenchAltPatch(rec *StressBenchUser) []Field {
	verified := true
	if rec.IsVerified {
		verified = false
	}
	return []Field{
		{Name: "country", Value: benchNextStringCycle(benchStressCountries, rec.Country)},
		{Name: "plan", Value: benchNextStringCycle(stressBenchTurnoverPlans[:], rec.Plan)},
		{Name: "status", Value: benchNextStringCycle(stressBenchTurnoverStatuses[:], rec.Status)},
		{Name: "age", Value: benchToggleInt(rec.Age, 13, 70)},
		{Name: "score", Value: benchToggleFloat(rec.Score, 7.5, 0, 1_500)},
		{Name: "is_verified", Value: verified},
		{Name: "created_at", Value: rec.CreatedAt + 3_600},
		{Name: "last_login", Value: rec.LastLogin + 1_800},
		{Name: "tags", Value: benchToggleSliceValue(rec.Tags, "kubernetes")},
		{Name: "roles", Value: benchToggleSliceValue(rec.Roles, "admin")},
	}
}

func buildUserBenchTurnoverRingUint64(tb testing.TB, db *DB[uint64, UserBench]) *benchTurnoverRing[uint64] {
	tb.Helper()
	total := int(db.getSnapshot().universe.Cardinality())
	ords := benchTurnoverSampleOrdinals(total, benchTurnoverRingSize)
	entries := make([]benchTurnoverEntry[uint64], 0, len(ords))
	for _, ord := range ords {
		id := uint64(ord)
		rec, err := db.Get(id)
		if err != nil {
			tb.Fatalf("Get(turnover %d): %v", ord, err)
		}
		if rec == nil {
			tb.Fatalf("missing turnover record id=%d", ord)
		}
		entries = append(entries, benchTurnoverEntry[uint64]{
			id:        id,
			basePatch: userBenchBasePatch(rec),
			altPatch:  userBenchAltPatch(rec),
		})
		db.ReleaseRecords(rec)
	}
	return &benchTurnoverRing[uint64]{entries: entries}
}

func buildUserBenchTurnoverRingString(tb testing.TB, db *DB[string, UserBench]) *benchTurnoverRing[string] {
	tb.Helper()
	total := int(db.getSnapshot().universe.Cardinality())
	ords := benchTurnoverSampleOrdinals(total, benchTurnoverRingSize)
	entries := make([]benchTurnoverEntry[string], 0, len(ords))
	for _, ord := range ords {
		id := "id-" + strconv.Itoa(ord)
		rec, err := db.Get(id)
		if err != nil {
			tb.Fatalf("Get(turnover %s): %v", id, err)
		}
		if rec == nil {
			tb.Fatalf("missing turnover record id=%s", id)
		}
		entries = append(entries, benchTurnoverEntry[string]{
			id:        id,
			basePatch: userBenchBasePatch(rec),
			altPatch:  userBenchAltPatch(rec),
		})
		db.ReleaseRecords(rec)
	}
	return &benchTurnoverRing[string]{entries: entries}
}

func buildStressBenchTurnoverRing(tb testing.TB, db *DB[uint64, StressBenchUser]) *benchTurnoverRing[uint64] {
	tb.Helper()
	total := int(db.getSnapshot().universe.Cardinality())
	ords := benchTurnoverSampleOrdinals(total, benchTurnoverRingSize)
	entries := make([]benchTurnoverEntry[uint64], 0, len(ords))
	for _, ord := range ords {
		id := uint64(ord)
		rec, err := db.Get(id)
		if err != nil {
			tb.Fatalf("Get(turnover %d): %v", ord, err)
		}
		if rec == nil {
			tb.Fatalf("missing turnover record id=%d", ord)
		}
		entries = append(entries, benchTurnoverEntry[uint64]{
			id:        id,
			basePatch: stressBenchBasePatch(rec),
			altPatch:  stressBenchAltPatch(rec),
		})
		db.ReleaseRecords(rec)
	}
	return &benchTurnoverRing[uint64]{entries: entries}
}

func prepareReadBenchWithMode[K ~string | ~uint64, V any](
	b *testing.B,
	db *DB[K, V],
	q *qx.QX,
	mode benchCacheMode,
	warm func(*testing.B, *DB[K, V], *qx.QX),
	run benchReadRunner[K, V],
	buildRing func(testing.TB, *DB[K, V]) *benchTurnoverRing[K],
) *benchReadModeState[K, V] {
	b.Helper()
	prepareReadBenchSnapshot(b, db)
	var ring *benchTurnoverRing[K]
	if mode.kind != benchCacheModeHot && buildRing != nil {
		ring = buildRing(b, db)
	}
	state := newBenchReadModeState[K, V](b, mode, ring)
	b.Cleanup(func() { state.close(b, db) })
	warm(b, db, q)
	if mode.kind == benchCacheModeHot {
		warmBenchReadHotSteady(b, db, q, run)
	} else {
		state.warmPools(b, db, q, run)
	}
	b.ResetTimer()
	return state
}

func TestBenchMode_ColdFreshPublishesAndClearsNewSnapshotCaches(t *testing.T) {
	path := t.TempDir() + "/bench.db"
	db, raw := openBoltAndNew[uint64, UserBench](t, path, benchOptions())
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	seedBenchData(t, db, 10_000)
	q := qx.Query(qx.PREFIX("email", "user1"))
	if _, err := db.QueryKeys(q); err != nil {
		t.Fatalf("warm QueryKeys: %v", err)
	}
	oldSnap := db.getSnapshot()
	if oldSnap.matPredCacheCount.Load() == 0 {
		t.Fatalf("expected warm snapshot to populate materialized predicate cache")
	}
	heldSnap, heldRef, ok := db.pinSnapshotRefBySeq(oldSnap.seq)
	if !ok || heldSnap != oldSnap || heldRef == nil {
		t.Fatalf("pinSnapshotRefBySeq(seq=%d) failed", oldSnap.seq)
	}
	defer db.unpinSnapshotRef(oldSnap.seq, heldRef)
	state := newBenchReadModeState[uint64, UserBench](
		t,
		benchCacheMode{suffix: "ColdFresh", kind: benchCacheModeColdFresh},
		buildUserBenchTurnoverRingUint64(t, db),
	)
	t.Cleanup(func() { state.close(t, db) })
	runBenchModeBeforeQueryForTest(t, db, state)

	newSnap := db.getSnapshot()
	if newSnap == oldSnap || newSnap.seq == oldSnap.seq {
		t.Fatalf("expected ColdFresh to publish a new snapshot")
	}
	if newSnap.matPredCacheCount.Load() != 0 {
		t.Fatalf("expected ColdFresh to clear new snapshot caches, got=%d", newSnap.matPredCacheCount.Load())
	}
	if oldSnap.matPredCacheCount.Load() == 0 {
		t.Fatalf("expected old snapshot caches to remain intact")
	}
}

func TestBenchMode_ColdTurnoverInheritsUnchangedFieldCaches(t *testing.T) {
	path := t.TempDir() + "/bench.db"
	db, raw := openBoltAndNew[uint64, UserBench](t, path, benchOptions())
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	seedBenchData(t, db, 10_000)
	expr := qx.Expr{Op: qx.OpPREFIX, Field: "email", Value: "user1"}
	cacheKey := db.materializedPredCacheKey(expr)
	if cacheKey == "" {
		t.Fatalf("expected non-empty cache key")
	}
	if _, err := db.QueryKeys(qx.Query(expr)); err != nil {
		t.Fatalf("warm QueryKeys: %v", err)
	}
	oldSnap := db.getSnapshot()
	if _, ok := oldSnap.loadMaterializedPred(cacheKey); !ok {
		t.Fatalf("expected old snapshot cache hit before turnover")
	}
	state := newBenchReadModeState[uint64, UserBench](
		t,
		benchCacheMode{suffix: "ColdTurnover", kind: benchCacheModeColdTurnover},
		buildUserBenchTurnoverRingUint64(t, db),
	)
	t.Cleanup(func() { state.close(t, db) })
	runBenchModeBeforeQueryForTest(t, db, state)

	newSnap := db.getSnapshot()
	if newSnap == oldSnap || newSnap.seq == oldSnap.seq {
		t.Fatalf("expected ColdTurnover to publish a new snapshot")
	}
	if _, ok := newSnap.loadMaterializedPred(cacheKey); !ok {
		t.Fatalf("expected unchanged-field materialized cache to survive turnover")
	}
}

func TestBenchMode_ColdPinnedKeepsRetiredSnapshotsPinned(t *testing.T) {
	path := t.TempDir() + "/bench.db"
	db, raw := openBoltAndNew[uint64, UserBench](t, path, benchOptions())
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	seedBenchData(t, db, 10_000)
	state := newBenchReadModeState[uint64, UserBench](
		t,
		benchCacheMode{suffix: "ColdPinned", kind: benchCacheModeColdPinned},
		buildUserBenchTurnoverRingUint64(t, db),
	)
	t.Cleanup(func() { state.close(t, db) })
	for i := 0; i < benchPinnedSnapshotLimit+1; i++ {
		runBenchModeBeforeQueryForTest(t, db, state)
	}
	stats := db.SnapshotStats()
	if stats.PinnedRefs == 0 {
		t.Fatalf("expected ColdPinned to keep retired snapshots pinned")
	}
	if stats.RegistrySize < 2 {
		t.Fatalf("expected retired snapshots to remain in registry, got=%d", stats.RegistrySize)
	}
	state.close(t, db)
	stats = db.SnapshotStats()
	if stats.PinnedRefs != 0 {
		t.Fatalf("expected pinned refs to drop after cleanup, got=%d", stats.PinnedRefs)
	}
}

func runBenchModeBeforeQueryForTest[K ~string | ~uint64, V any](tb testing.TB, db *DB[K, V], state *benchReadModeState[K, V]) {
	tb.Helper()
	switch state.mode.kind {
	case benchCacheModeColdFresh:
		state.applyTurnover(tb, db)
		db.getSnapshot().clearRuntimeCachesForTesting()
	case benchCacheModeColdTurnover:
		state.applyTurnover(tb, db)
	case benchCacheModeColdPinned:
		state.pinCurrentSnapshot(tb, db)
		state.applyTurnover(tb, db)
		if len(state.pinned) > benchPinnedSnapshotLimit {
			oldest := state.pinned[0]
			state.pinned = state.pinned[1:]
			db.unpinSnapshotRef(oldest.seq, oldest.ref)
		}
	}
}

func TestBenchTurnoverRingIsReversible(t *testing.T) {
	path := t.TempDir() + "/bench.db"
	db, raw := openBoltAndNew[uint64, UserBench](t, path, benchOptions())
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	seedBenchData(t, db, 1_000)
	ring := buildUserBenchTurnoverRingUint64(t, db)
	if len(ring.entries) == 0 {
		t.Fatalf("expected non-empty turnover ring")
	}
	id := ring.entries[0].id
	before, err := db.Get(id)
	if err != nil {
		t.Fatalf("Get(before): %v", err)
	}
	if before == nil {
		t.Fatalf("missing record before turnover")
	}
	base := fmt.Sprintf("%s|%s|%s|%d|%.2f|%v|%v", before.Country, before.Plan, before.Status, before.Age, before.Score, before.Tags, before.Roles)
	db.ReleaseRecords(before)

	state := newBenchReadModeState[uint64, UserBench](
		t,
		benchCacheMode{suffix: "ColdTurnover", kind: benchCacheModeColdTurnover},
		&benchTurnoverRing[uint64]{entries: []benchTurnoverEntry[uint64]{ring.entries[0]}},
	)
	runBenchModeBeforeQueryForTest(t, db, state)
	runBenchModeBeforeQueryForTest(t, db, state)

	after, err := db.Get(id)
	if err != nil {
		t.Fatalf("Get(after): %v", err)
	}
	if after == nil {
		t.Fatalf("missing record after turnover")
	}
	got := fmt.Sprintf("%s|%s|%s|%d|%.2f|%v|%v", after.Country, after.Plan, after.Status, after.Age, after.Score, after.Tags, after.Roles)
	db.ReleaseRecords(after)
	if got != base {
		t.Fatalf("expected reversible turnover, got=%s want=%s", got, base)
	}
}
