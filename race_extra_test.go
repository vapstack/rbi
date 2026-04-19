package rbi

import (
	"fmt"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/posting"
	"go.etcd.io/bbolt"
)

type raceExtraMeta struct {
	Country string `db:"country" dbi:"default"`
}

type raceExtraRec struct {
	raceExtraMeta

	Name     string   `db:"name"      dbi:"default"`
	Email    string   `db:"email"     dbi:"default"`
	Age      int      `db:"age"       dbi:"default"`
	Score    float64  `db:"score"     dbi:"default"`
	Active   bool     `db:"active"    dbi:"default"`
	Tags     []string `db:"tags"      dbi:"default"`
	FullName string   `db:"full_name" dbi:"default"`
	Opt      *string  `db:"opt"       dbi:"default"`
}

func raceExtraOpenTempDBUint64(t *testing.T, opts Options) *DB[uint64, raceExtraRec] {
	t.Helper()

	path := filepath.Join(t.TempDir(), "race_extra.db")
	raw, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}

	opts.EnableAutoBatchStats = true
	opts.EnableSnapshotStats = true

	db, err := New[uint64, raceExtraRec](raw, opts)
	if err != nil {
		_ = raw.Close()
		t.Fatalf("New: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	return db
}

func raceExtraOpenTempDBString(t *testing.T, opts Options) *DB[string, raceExtraRec] {
	t.Helper()

	path := filepath.Join(t.TempDir(), "race_extra_string.db")
	raw, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}

	opts.EnableAutoBatchStats = true
	opts.EnableSnapshotStats = true

	db, err := New[string, raceExtraRec](raw, opts)
	if err != nil {
		_ = raw.Close()
		t.Fatalf("New: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	return db
}

func raceExtraSeedGeneratedUint64Data(t *testing.T, db *DB[uint64, raceExtraRec], n int, gen func(i int) *raceExtraRec) {
	t.Helper()

	db.DisableSync()
	defer db.EnableSync()

	batchSize := 32 << 10
	if n > 0 && n < batchSize {
		batchSize = n
	}
	batchIDs := make([]uint64, 0, batchSize)
	batchVals := make([]*raceExtraRec, 0, batchSize)

	flush := func() {
		t.Helper()
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

func raceExtraSeedGeneratedStringData(t *testing.T, db *DB[string, raceExtraRec], keys []string, gen func(i int, key string) *raceExtraRec) {
	t.Helper()

	db.DisableSync()
	defer db.EnableSync()

	batchSize := 32 << 10
	if len(keys) > 0 && len(keys) < batchSize {
		batchSize = len(keys)
	}
	batchKeys := make([]string, 0, batchSize)
	batchVals := make([]*raceExtraRec, 0, batchSize)

	flush := func() {
		t.Helper()
		if len(batchKeys) == 0 {
			return
		}
		if err := db.BatchSet(batchKeys, batchVals); err != nil {
			t.Fatalf("BatchSet(seed batch=%d): %v", len(batchKeys), err)
		}
		batchKeys = batchKeys[:0]
		batchVals = batchVals[:0]
	}

	for i, key := range keys {
		batchKeys = append(batchKeys, key)
		batchVals = append(batchVals, gen(i+1, key))
		if len(batchKeys) == cap(batchKeys) {
			flush()
		}
	}
	flush()
}

func raceExtraSetNumericBucketKnobs(t *testing.T, db *DB[uint64, raceExtraRec], size, minFieldKeys, minSpan int) {
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

func raceExtraRequireNumericRangeBucketCacheEntry(t *testing.T, snap *indexSnapshot, field string) *numericRangeBucketCacheEntry {
	t.Helper()

	if snap == nil || snap.numericRangeBucketCache == nil {
		t.Fatalf("expected non-nil numeric range bucket cache for field %q", field)
	}
	entry, ok := snap.numericRangeBucketCache.loadField(field)
	if !ok {
		t.Fatalf("expected numeric range bucket cache entry for field %q", field)
	}
	if entry == nil || entry.idx.bucketSize <= 0 {
		t.Fatalf("expected non-nil numeric range bucket entry for field %q", field)
	}
	return entry
}

func raceExtraPosting(ids ...uint64) posting.List {
	var out posting.List
	for _, id := range ids {
		out = out.BuildAdded(id)
	}
	return out
}

func raceExtraAssertPostingSet(t *testing.T, got posting.List, want []uint64) {
	t.Helper()
	gotIDs := got.ToArray()
	if !slices.Equal(gotIDs, want) {
		t.Fatalf("posting mismatch: got=%v want=%v", gotIDs, want)
	}
}

func raceExtraRangeKeys(startInclusive, endExclusive, total int) []uint64 {
	if startInclusive < 1 {
		startInclusive = 1
	}
	if endExclusive < startInclusive {
		return nil
	}
	if endExclusive > total+1 {
		endExclusive = total + 1
	}
	out := make([]uint64, 0, endExclusive-startInclusive)
	for i := startInclusive; i < endExclusive; i++ {
		out = append(out, uint64(i))
	}
	return out
}

func TestRaceExtra_NumericRangeBucketSpanCacheDetachedLoadsUnderConcurrency(t *testing.T) {
	entry := &numericRangeBucketCacheEntry{}

	base := raceExtraPosting()
	for i := uint64(1); i <= 48; i++ {
		base = base.BuildAdded(i * 5)
	}
	want := base.ToArray()

	stored := base.Borrow()
	var ok bool
	stored, ok = entry.tryStoreFullSpan(11, 23, stored)
	if !ok {
		t.Fatal("expected initial full-span store to succeed")
	}
	stored.Release()
	base.Release()

	remove := raceExtraPosting(want[0], want[1], want[2])
	add := raceExtraPosting(1<<32|3, 1<<32|7)
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

	readerN := max(6, runtime.GOMAXPROCS(0))
	for g := 0; g < readerN; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 1000; i++ {
				cached, ok := entry.loadFullSpan(11, 23)
				if !ok {
					setFailed("full-span cache entry unexpectedly missing")
					return
				}
				if !slices.Equal(cached.ToArray(), want) {
					setFailed(fmt.Sprintf("cached full-span mismatch: got=%v want=%v", cached.ToArray(), want))
					cached.Release()
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
			cached, ok := entry.loadFullSpan(11, 23)
			if !ok {
				setFailed("writer unexpectedly missed full-span cache entry")
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

	cached, ok := entry.loadFullSpan(11, 23)
	if !ok {
		t.Fatal("full-span cache entry missing after concurrent mutations")
	}
	defer cached.Release()
	raceExtraAssertPostingSet(t, cached, want)
}

func TestRaceExtra_MaterializedPredBorrowedViewSurvivesConcurrentEviction(t *testing.T) {
	snap := &indexSnapshot{matPredCacheMaxEntries: 1}
	snapshotExtInitMaterializedPredCache(snap)

	base := raceExtraPosting()
	for i := uint64(1); i <= 48; i++ {
		base = base.BuildAdded(i * 7)
	}
	want := base.ToArray()

	snap.storeMaterializedPred("hold", base.Borrow())
	base.Release()

	held, ok := snap.loadMaterializedPred("hold")
	if !ok || held.IsEmpty() {
		t.Fatal("expected held cache entry")
	}
	defer held.Release()

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

	readerN := max(6, runtime.GOMAXPROCS(0))
	for g := 0; g < readerN; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 1000; i++ {
				if !slices.Equal(held.ToArray(), want) {
					setFailed(fmt.Sprintf("held materialized posting changed under eviction: got=%v want=%v", held.ToArray(), want))
					return
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 400; i++ {
			ids := raceExtraPosting(uint64(i+1), 1<<32|uint64(i+1))
			snap.storeMaterializedPred(fmt.Sprintf("evict-%d", i), ids)
		}
	}()

	close(start)
	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}

	raceExtraAssertPostingSet(t, held, want)
}

func TestRaceExtra_NumericRangeFullSpanBorrowedViewSurvivesConcurrentEviction(t *testing.T) {
	entry := &numericRangeBucketCacheEntry{}

	base := raceExtraPosting()
	for i := uint64(1); i <= 48; i++ {
		base = base.BuildAdded(i * 9)
	}
	want := base.ToArray()

	stored := base.Borrow()
	stored, ok := entry.tryStoreFullSpan(0, 0, stored)
	if !ok {
		t.Fatal("expected initial full-span store to succeed")
	}
	stored.Release()
	base.Release()

	held, ok := entry.loadFullSpan(0, 0)
	if !ok || held.IsEmpty() {
		t.Fatal("expected held full-span cache entry")
	}
	defer held.Release()

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

	readerN := max(6, runtime.GOMAXPROCS(0))
	for g := 0; g < readerN; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 1000; i++ {
				if !slices.Equal(held.ToArray(), want) {
					setFailed(fmt.Sprintf("held full-span posting changed under eviction: got=%v want=%v", held.ToArray(), want))
					return
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 1; i <= 256; i++ {
			ids := raceExtraPosting(uint64(i), 1<<32|uint64(i))
			var ok bool
			ids, ok = entry.tryStoreFullSpan(i, i, ids)
			if !ok {
				setFailed(fmt.Sprintf("tryStoreFullSpan(%d,%d) failed", i, i))
				return
			}
			ids.Release()
		}
	}()

	close(start)
	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}

	raceExtraAssertPostingSet(t, held, want)
}

func TestRaceExtra_PublicQueriesStayExactUnderConcurrentMaterializedCacheThrash(t *testing.T) {
	db := raceExtraOpenTempDBUint64(t, Options{
		AnalyzeInterval:                             -1,
		SnapshotMaterializedPredCacheMaxEntries:     2,
		SnapshotMaterializedPredCacheMaxCardinality: 64,
	})

	const total = 6_000
	raceExtraSeedGeneratedUint64Data(t, db, total, func(i int) *raceExtraRec {
		return &raceExtraRec{
			raceExtraMeta: raceExtraMeta{Country: "NL"},
			Name:          fmt.Sprintf("user-%d", i),
			Email:         fmt.Sprintf("user-%04d@example.test", i),
			Age:           i,
			Score:         float64(i),
			Active:        i%2 == 0,
			Tags:          []string{"go", "db"},
			FullName:      fmt.Sprintf("FN-%04d", i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	raceExtraSetNumericBucketKnobs(t, db, 128, 1, 1)

	type tc struct {
		q    *qx.QX
		want []uint64
	}

	cases := []tc{
		{q: qx.Query(qx.GTE("age", 5950)), want: raceExtraRangeKeys(5950, total+1, total)},
		{q: qx.Query(qx.GTE("age", 5900)), want: raceExtraRangeKeys(5900, total+1, total)},
		{q: qx.Query(qx.LT("age", 50)), want: raceExtraRangeKeys(1, 50, total)},
		{q: qx.Query(qx.LT("age", 120)), want: raceExtraRangeKeys(1, 120, total)},
		{q: qx.Query(qx.GTE("age", 2500), qx.LT("age", 2560)), want: raceExtraRangeKeys(2500, 2560, total)},
		{q: qx.Query(qx.GTE("age", 2500), qx.LT("age", 2580)), want: raceExtraRangeKeys(2500, 2580, total)},
	}

	for i, tc := range cases {
		got, err := db.QueryKeys(tc.q)
		if err != nil {
			t.Fatalf("warm QueryKeys(%d): %v", i, err)
		}
		if !slices.Equal(got, tc.want) {
			t.Fatalf("warm QueryKeys(%d) mismatch: got=%v want=%v", i, got, tc.want)
		}
	}

	var failed atomic.Pointer[string]
	setFailed := func(msg string) {
		if failed.Load() != nil {
			return
		}
		copyMsg := msg
		failed.CompareAndSwap(nil, &copyMsg)
	}

	recordAges := func(values []*raceExtraRec) []uint64 {
		out := make([]uint64, 0, len(values))
		for _, value := range values {
			if value == nil {
				return nil
			}
			out = append(out, uint64(value.Age))
		}
		slices.Sort(out)
		return out
	}

	start := make(chan struct{})
	var wg sync.WaitGroup

	for g := 0; g < max(8, runtime.GOMAXPROCS(0)); g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			<-start
			for i := 0; i < 200; i++ {
				if failed.Load() != nil {
					return
				}

				tc := cases[r.IntN(len(cases))]

				got, err := db.QueryKeys(tc.q)
				if err != nil {
					setFailed(fmt.Sprintf("QueryKeys failed: %v", err))
					return
				}
				if !slices.Equal(got, tc.want) {
					setFailed(fmt.Sprintf("QueryKeys mismatch: got=%v want=%v", got, tc.want))
					return
				}

				count, err := db.Count(tc.q.Filter)
				if err != nil {
					setFailed(fmt.Sprintf("Count failed: %v", err))
					return
				}
				if count != uint64(len(tc.want)) {
					setFailed(fmt.Sprintf("Count mismatch: got=%d want=%d", count, len(tc.want)))
					return
				}

				if i%4 == 0 {
					values, err := db.Query(tc.q)
					if err != nil {
						setFailed(fmt.Sprintf("Query failed: %v", err))
						return
					}
					gotAges := recordAges(values)
					if gotAges == nil {
						setFailed("Query returned nil record")
						return
					}
					if !slices.Equal(gotAges, tc.want) {
						setFailed(fmt.Sprintf("Query ages mismatch: got=%v want=%v", gotAges, tc.want))
						return
					}
				}
			}
		}(int64(20260629 + g))
	}

	close(start)
	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}

	snap := db.getSnapshot()
	if got := snap.matPredCacheCount.Load(); got > 2 {
		t.Fatalf("materialized predicate cache exceeded global limit: got=%d", got)
	}
	if got := snap.matPredCacheOversizedCount.Load(); got > materializedPredCacheOversizedLimit(snap.matPredCacheMaxEntries) {
		t.Fatalf("oversized materialized predicate cache exceeded limit: got=%d", got)
	}
}

func TestRaceExtra_PinnedSnapshotQueryViewStaysExactAcrossConcurrentPublishes(t *testing.T) {
	db := raceExtraOpenTempDBUint64(t, Options{
		AnalyzeInterval: -1,
		AutoBatchMax:    1,
	})

	const total = 6_000
	raceExtraSeedGeneratedUint64Data(t, db, total, func(i int) *raceExtraRec {
		return &raceExtraRec{
			raceExtraMeta: raceExtraMeta{Country: "NL"},
			Name:          fmt.Sprintf("user-%d", i),
			Email:         fmt.Sprintf("user-%04d@example.test", i),
			Age:           i,
			Score:         float64(i),
			Active:        i%2 == 0,
			Tags:          []string{"go", "db"},
			FullName:      fmt.Sprintf("FN-%04d", i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	seq := db.getSnapshot().seq
	snap, ref, ok := db.pinSnapshotRefBySeq(seq)
	if !ok || snap == nil {
		t.Fatal("expected current snapshot to be pinnable")
	}
	cleanupDone := false
	defer func() {
		if cleanupDone {
			return
		}
		db.unpinSnapshotRef(seq, ref)
	}()

	q := qx.Query(
		qx.GTE("age", 2500),
		qx.LT("age", 2600),
	)
	want := raceExtraRangeKeys(2500, 2600, total)

	checkPinnedSnapshot := func() error {
		view := db.makeQueryView(snap)
		defer db.releaseQueryView(view)

		prepared, viewQ, err := prepareTestQuery(db, q)
		if err != nil {
			return err
		}
		defer prepared.Release()

		got, err := view.execQuery(&viewQ, false, false)
		if err != nil {
			return err
		}
		if !slices.Equal(got, want) {
			return fmt.Errorf("pinned snapshot query mismatch: got=%v want=%v", got, want)
		}
		return nil
	}

	if err := checkPinnedSnapshot(); err != nil {
		t.Fatal(err)
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

	names := []string{"alice", "bob", "carol", "dave"}
	countries := []string{"NL", "PL", "DE", "US"}
	tags := []string{"go", "db", "ops", "rust"}

	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			<-start
			for i := 0; i < 30; i++ {
				if failed.Load() != nil {
					return
				}

				id := uint64(1 + r.IntN(total))
				patch := []Field{
					{Name: "active", Value: r.IntN(2) == 0},
					{Name: "country", Value: countries[r.IntN(len(countries))]},
					{Name: "tags", Value: []string{tags[r.IntN(len(tags))], tags[r.IntN(len(tags))]}},
					{Name: "name", Value: names[r.IntN(len(names))]},
					{Name: "opt", Value: fmt.Sprintf("opt-%d-%d", seed, i)},
				}
				if i%5 == 0 {
					patch[len(patch)-1] = Field{Name: "opt", Value: nil}
				}
				if err := db.Patch(id, patch); err != nil {
					setFailed(fmt.Sprintf("Patch(existing) failed: %v", err))
					return
				}
			}
		}(int64(20260729 + w))
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 60; i++ {
			if failed.Load() != nil {
				return
			}
			if err := checkPinnedSnapshot(); err != nil {
				setFailed(err.Error())
				return
			}
		}
	}()

	close(start)
	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}

	if err := checkPinnedSnapshot(); err != nil {
		t.Fatal(err)
	}

	db.snapshot.mu.RLock()
	held := db.snapshot.bySeq[seq]
	db.snapshot.mu.RUnlock()
	if held != ref || held == nil {
		t.Fatal("expected old snapshot ref to remain registered while pinned")
	}
	if held.refs.Load() <= 0 {
		t.Fatal("expected old snapshot ref to stay pinned across publishes")
	}

	db.unpinSnapshotRef(seq, ref)
	cleanupDone = true

	db.snapshot.mu.RLock()
	_, stillPresent := db.snapshot.bySeq[seq]
	db.snapshot.mu.RUnlock()
	if stillPresent {
		t.Fatal("expected old snapshot ref to be pruned after unpin")
	}
}

func TestRaceExtra_PinnedStringSnapshotScanStaysStableAcrossConcurrentPublishes(t *testing.T) {
	db := raceExtraOpenTempDBString(t, Options{
		AnalyzeInterval: -1,
		AutoBatchMax:    1,
	})

	const seedN = 1_000
	keys := make([]string, 0, seedN)
	for i := 1; i <= seedN; i++ {
		keys = append(keys, fmt.Sprintf("k%04d", i))
	}
	raceExtraSeedGeneratedStringData(t, db, keys, func(i int, key string) *raceExtraRec {
		return &raceExtraRec{
			raceExtraMeta: raceExtraMeta{Country: "NL"},
			Name:          fmt.Sprintf("user-%d", i),
			Email:         fmt.Sprintf("%s@example.test", key),
			Age:           i,
			Score:         float64(i),
			Active:        i%2 == 0,
			Tags:          []string{"go", "db"},
			FullName:      fmt.Sprintf("FN-%04d", i),
		}
	})

	old := db.getSnapshot()
	pinned, ref, ok := db.pinSnapshotRefBySeq(old.seq)
	if !ok || pinned != old {
		t.Fatal("expected current string snapshot to be pinnable")
	}
	defer db.unpinSnapshotRef(old.seq, ref)

	expected := slices.Clone(keys)
	checkPinnedScan := func() error {
		iter := pinned.universe.Iter()
		defer iter.Release()

		got := make([]string, 0, len(expected))
		if err := db.scanStringKeys(pinned.strmap, pinned.universe, iter, "", func(id string) (bool, error) {
			got = append(got, id)
			return true, nil
		}); err != nil {
			return err
		}
		if !slices.Equal(got, expected) {
			return fmt.Errorf("pinned string scan mismatch: got=%v want=%v", got, expected)
		}
		return nil
	}

	if err := checkPinnedScan(); err != nil {
		t.Fatal(err)
	}

	var failed atomic.Pointer[string]
	setFailed := func(msg string) {
		if failed.Load() != nil {
			return
		}
		copyMsg := msg
		failed.CompareAndSwap(nil, &copyMsg)
	}

	var nextID atomic.Uint64
	nextID.Store(seedN)

	start := make(chan struct{})
	var wg sync.WaitGroup

	names := []string{"alice", "bob", "carol", "dave"}
	countries := []string{"NL", "PL", "DE", "US"}
	tags := []string{"go", "db", "ops", "rust"}

	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			<-start
			for i := 0; i < 220; i++ {
				if failed.Load() != nil {
					return
				}

				switch r.IntN(3) {
				case 0:
					key := fmt.Sprintf("k%04d", 1+r.IntN(seedN))
					patch := []Field{
						{Name: "active", Value: r.IntN(2) == 0},
						{Name: "country", Value: countries[r.IntN(len(countries))]},
						{Name: "tags", Value: []string{tags[r.IntN(len(tags))], tags[r.IntN(len(tags))]}},
						{Name: "name", Value: names[r.IntN(len(names))]},
					}
					if err := db.Patch(key, patch); err != nil {
						setFailed(fmt.Sprintf("Patch(seed key) failed: %v", err))
						return
					}
				case 1:
					id := nextID.Add(1)
					key := fmt.Sprintf("future-%04d", id)
					rec := &raceExtraRec{
						raceExtraMeta: raceExtraMeta{Country: countries[r.IntN(len(countries))]},
						Name:          names[r.IntN(len(names))],
						Email:         fmt.Sprintf("%s@example.test", key),
						Age:           10_000 + int(id),
						Score:         float64(id),
						Active:        r.IntN(2) == 0,
						Tags:          []string{tags[r.IntN(len(tags))]},
						FullName:      fmt.Sprintf("FN-future-%d", id),
					}
					if err := db.Set(key, rec); err != nil {
						setFailed(fmt.Sprintf("Set(future key) failed: %v", err))
						return
					}
				default:
					hi := nextID.Load()
					if hi <= seedN {
						continue
					}
					key := fmt.Sprintf("future-%04d", seedN+1+r.IntN(int(hi-seedN)))
					if err := db.Delete(key); err != nil {
						setFailed(fmt.Sprintf("Delete(future key) failed: %v", err))
						return
					}
				}
			}
		}(int64(20260829 + w))
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 180; i++ {
			if failed.Load() != nil {
				return
			}
			if err := checkPinnedScan(); err != nil {
				setFailed(err.Error())
				return
			}
		}
	}()

	close(start)
	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}

	if err := checkPinnedScan(); err != nil {
		t.Fatal(err)
	}
}

func TestRaceExtra_PublicNumericRangeQueriesStayExactAcrossConcurrentUnchangedFieldPublishes(t *testing.T) {
	db := raceExtraOpenTempDBUint64(t, Options{
		AnalyzeInterval:                         -1,
		AutoBatchMax:                            1,
		SnapshotMaterializedPredCacheMaxEntries: 64,
	})

	const total = 6_000
	raceExtraSeedGeneratedUint64Data(t, db, total, func(i int) *raceExtraRec {
		return &raceExtraRec{
			raceExtraMeta: raceExtraMeta{Country: "NL"},
			Name:          fmt.Sprintf("user-%d", i),
			Email:         fmt.Sprintf("user-%04d@example.test", i),
			Age:           i,
			Score:         float64(i),
			Active:        i%2 == 0,
			Tags:          []string{"go", "db"},
			FullName:      fmt.Sprintf("FN-%04d", i),
		}
	})
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	raceExtraSetNumericBucketKnobs(t, db, 128, 1, 1)

	makeView := func() *queryView[uint64, raceExtraRec] {
		return db.makeQueryView(db.getSnapshot())
	}

	spanFor := func(expr qx.Expr) (overlayRange, *numericRangeBucketCacheEntry) {
		t.Helper()

		view := makeView()
		defer db.releaseQueryView(view)

		prepared, compiled, err := prepareTestExpr(db, expr)
		if err != nil {
			t.Fatalf("prepareTestExpr(%v): %v", expr, err)
		}
		defer prepared.Release()
		field := db.fieldNameByOrdinal(compiled.FieldOrdinal)
		fm := view.fields[field]
		if fm == nil {
			t.Fatalf("expected field metadata for %q", field)
		}
		ov := view.fieldOverlay(field)
		if !ov.hasData() {
			t.Fatalf("expected field overlay for %q", field)
		}

		key, isSlice, isNil, err := view.exprValueToIdxScalar(compiled)
		if err != nil {
			t.Fatalf("exprValueToIdxScalar(%v): %v", expr, err)
		}
		if isSlice || isNil {
			t.Fatalf("unexpected scalar flags for %v: isSlice=%v isNil=%v", expr, isSlice, isNil)
		}

		rb, ok := rangeBoundsForOp(compiled.Op, key)
		if !ok {
			t.Fatalf("rangeBoundsForOp(%v) failed", compiled.Op)
		}

		br := ov.rangeForBounds(rb)
		out, ok := view.tryEvalNumericRangeBuckets(field, fm, ov, br)
		if !ok {
			t.Fatalf("expected numeric range bucket path for %v", expr)
		}
		out.release()

		return br, raceExtraRequireNumericRangeBucketCacheEntry(t, db.getSnapshot(), field)
	}

	evalSimple := func(expr qx.Expr) (postingResult, error) {
		view := makeView()
		defer db.releaseQueryView(view)
		prepared, compiled, err := prepareTestExpr(db, expr)
		if err != nil {
			return postingResult{}, err
		}
		defer prepared.Release()
		return view.evalSimple(compiled)
	}

	exprValueToIdxScalar := func(expr qx.Expr) (string, bool, bool, error) {
		view := makeView()
		defer db.releaseQueryView(view)
		prepared, compiled, err := prepareTestExpr(db, expr)
		if err != nil {
			return "", false, false, err
		}
		defer prepared.Release()
		return view.exprValueToIdxScalar(compiled)
	}

	materializedPredCacheKeyForScalar := func(field string, op qx.Op, key string) string {
		view := makeView()
		defer db.releaseQueryView(view)
		return view.materializedPredCacheKeyForScalar(field, compileScalarOpForTest(op), key)
	}

	exprGTE2500 := qx.GTE("age", 2500)
	exprGTE2501 := qx.GTE("age", 2501)
	exprLT4500 := qx.LT("age", 4500)
	exprLT4499 := qx.LT("age", 4499)

	br1, entry1 := spanFor(exprGTE2500)
	br2, entry2 := spanFor(exprGTE2501)
	start1, end1, ok := entry1.idx.fullBucketSpan(br1)
	if !ok {
		t.Fatal("expected full bucket span for GTE age>=2500")
	}
	start2, end2, ok := entry2.idx.fullBucketSpan(br2)
	if !ok {
		t.Fatal("expected full bucket span for GTE age>=2501")
	}
	if start1 != start2 || end1 != end2 {
		t.Fatalf("expected adjacent GTE bounds to share full span: first=%d..%d second=%d..%d", start1, end1, start2, end2)
	}

	br3, entry3 := spanFor(exprLT4500)
	br4, entry4 := spanFor(exprLT4499)
	start3, end3, ok := entry3.idx.fullBucketSpan(br3)
	if !ok {
		t.Fatal("expected full bucket span for LT age<4500")
	}
	start4, end4, ok := entry4.idx.fullBucketSpan(br4)
	if !ok {
		t.Fatal("expected full bucket span for LT age<4499")
	}
	if start3 != start4 || end3 != end4 {
		t.Fatalf("expected adjacent LT bounds to share full span: first=%d..%d second=%d..%d", start3, end3, start4, end4)
	}

	warmExact, err := evalSimple(exprGTE2500)
	if err != nil {
		t.Fatalf("evalSimple warm(age>=2500): %v", err)
	}
	warmExact.release()

	prevSnap := db.getSnapshot()
	prevEntry := raceExtraRequireNumericRangeBucketCacheEntry(t, prevSnap, "age")
	key2500, isSlice, isNil, err := exprValueToIdxScalar(exprGTE2500)
	if err != nil {
		t.Fatalf("exprValueToIdxScalar(GTE 2500): %v", err)
	}
	if isSlice || isNil {
		t.Fatalf("unexpected scalar flags for GTE 2500: isSlice=%v isNil=%v", isSlice, isNil)
	}
	cacheKey2500 := materializedPredCacheKeyForScalar("age", qx.OpGTE, key2500)
	if cacheKey2500 == "" {
		t.Fatal("expected non-empty materialized cache key for age>=2500")
	}
	prevCached2500, ok := prevSnap.loadMaterializedPred(cacheKey2500)
	if !ok || prevCached2500.IsEmpty() {
		t.Fatal("expected warmed materialized predicate cache for age>=2500")
	}
	defer prevCached2500.Release()

	if err := db.Patch(1, []Field{{Name: "active", Value: false}}); err != nil {
		t.Fatalf("Patch(active warm publish): %v", err)
	}

	nextSnap := db.getSnapshot()
	nextEntry := raceExtraRequireNumericRangeBucketCacheEntry(t, nextSnap, "age")
	if nextEntry != prevEntry {
		t.Fatal("expected unrelated patch to inherit numeric range bucket cache entry")
	}
	nextCached2500, ok := nextSnap.loadMaterializedPred(cacheKey2500)
	if !ok || nextCached2500.IsEmpty() {
		t.Fatal("expected unrelated patch to inherit materialized predicate cache entry")
	}
	defer nextCached2500.Release()
	if nextCached2500 != prevCached2500 {
		t.Fatal("expected unrelated patch to reuse the same cached posting for age>=2500")
	}

	queries := []*qx.QX{
		qx.Query(qx.GTE("age", 2500)),
		qx.Query(qx.GTE("age", 2501)),
		qx.Query(qx.LT("age", 4500)),
		qx.Query(qx.LT("age", 4499)),
		qx.Query(qx.GTE("age", 2500), qx.LT("age", 4500)),
		qx.Query(qx.GTE("age", 2501), qx.LT("age", 4499)),
	}

	wants := [][]uint64{
		raceExtraRangeKeys(2500, total+1, total),
		raceExtraRangeKeys(2501, total+1, total),
		raceExtraRangeKeys(1, 4500, total),
		raceExtraRangeKeys(1, 4499, total),
		raceExtraRangeKeys(2500, 4500, total),
		raceExtraRangeKeys(2501, 4499, total),
	}

	for i, q := range queries {
		got, err := db.QueryKeys(q)
		if err != nil {
			t.Fatalf("warm QueryKeys(%d): %v", i, err)
		}
		if !slices.Equal(got, wants[i]) {
			t.Fatalf("warm QueryKeys(%d) mismatch: got=%v want=%v", i, got, wants[i])
		}
	}

	var failed atomic.Pointer[string]
	setFailed := func(msg string) {
		if failed.Load() != nil {
			return
		}
		copyMsg := msg
		failed.CompareAndSwap(nil, &copyMsg)
	}

	recordAges := func(values []*raceExtraRec) []uint64 {
		out := make([]uint64, 0, len(values))
		for _, value := range values {
			if value == nil {
				return nil
			}
			out = append(out, uint64(value.Age))
		}
		slices.Sort(out)
		return out
	}

	start := make(chan struct{})
	var wg sync.WaitGroup

	names := []string{"alice", "bob", "carol", "dave"}
	countries := []string{"NL", "PL", "DE", "US"}
	tags := []string{"go", "db", "ops", "rust"}

	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			<-start
			for i := 0; i < 250; i++ {
				if failed.Load() != nil {
					return
				}

				id := uint64(1 + r.IntN(total))
				var patch []Field
				switch r.IntN(5) {
				case 0:
					patch = []Field{{Name: "active", Value: r.IntN(2) == 0}}
				case 1:
					patch = []Field{{Name: "country", Value: countries[r.IntN(len(countries))]}}
				case 2:
					patch = []Field{
						{Name: "name", Value: names[r.IntN(len(names))]},
						{Name: "full_name", Value: fmt.Sprintf("FN-extra-%d-%d", seed, i)},
					}
				case 3:
					patch = []Field{{Name: "tags", Value: []string{tags[r.IntN(len(tags))], tags[r.IntN(len(tags))]}}}
				default:
					if r.IntN(2) == 0 {
						patch = []Field{{Name: "opt", Value: nil}}
					} else {
						patch = []Field{{Name: "opt", Value: fmt.Sprintf("opt-%d-%d", seed, i)}}
					}
				}

				if err := db.Patch(id, patch); err != nil {
					setFailed(fmt.Sprintf("writer patch failed: %v", err))
					return
				}
			}
		}(int64(20260329 + w))
	}

	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			<-start
			for i := 0; i < 160; i++ {
				if failed.Load() != nil {
					return
				}

				idx := r.IntN(len(queries))
				q := queries[idx]
				want := wants[idx]

				got, err := db.QueryKeys(q)
				if err != nil {
					setFailed(fmt.Sprintf("QueryKeys(%d) failed: %v", idx, err))
					return
				}
				if !slices.Equal(got, want) {
					setFailed(fmt.Sprintf("QueryKeys(%d) mismatch: got=%v want=%v", idx, got, want))
					return
				}

				count, err := db.Count(q.Filter)
				if err != nil {
					setFailed(fmt.Sprintf("Count(%d) failed: %v", idx, err))
					return
				}
				if count != uint64(len(want)) {
					setFailed(fmt.Sprintf("Count(%d) mismatch: got=%d want=%d", idx, count, len(want)))
					return
				}
			}
		}(int64(20260429 + g))
	}

	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			<-start
			for i := 0; i < 80; i++ {
				if failed.Load() != nil {
					return
				}

				idx := r.IntN(len(queries))
				q := queries[idx]
				want := wants[idx]

				values, err := db.Query(q)
				if err != nil {
					setFailed(fmt.Sprintf("Query(%d) failed: %v", idx, err))
					return
				}
				gotAges := recordAges(values)
				if gotAges == nil {
					setFailed(fmt.Sprintf("Query(%d) returned nil record", idx))
					return
				}
				if !slices.Equal(gotAges, want) {
					setFailed(fmt.Sprintf("Query(%d) ages mismatch: got=%v want=%v", idx, gotAges, want))
					return
				}
			}
		}(int64(20260529 + g))
	}

	close(start)
	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}
}
