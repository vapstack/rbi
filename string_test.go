package rbi

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/vapstack/qx"
)

func stringTestScanSnapshotKeys(db *DB[string, Rec], snap *indexSnapshot, seek string) ([]string, error) {
	if snap == nil {
		return nil, fmt.Errorf("snapshot is nil")
	}

	iter := snap.universe.Iter()
	defer iter.Release()

	var out []string
	if err := db.scanStringKeys(snap.strmap, snap.universe, iter, seek, func(id string) (bool, error) {
		out = append(out, id)
		return true, nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}

func TestStringExt_ReopenReinsertDeletedKeysPreserveOriginalInternalOrder(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "string_reinsert_reopen.db")

	db, raw := openBoltAndNew[string, Rec](t, path)
	closeCurrent := func() {
		if db != nil {
			if err := db.Close(); err != nil {
				t.Fatalf("db close: %v", err)
			}
			db = nil
		}
		if raw != nil {
			if err := raw.Close(); err != nil {
				t.Fatalf("raw close: %v", err)
			}
			raw = nil
		}
	}
	t.Cleanup(closeCurrent)

	order := []string{
		"k-07", "k-02", "k-09", "k-01", "k-08",
		"k-03", "k-06", "k-04", "k-05",
	}
	for _, key := range order {
		if err := db.Set(key, &Rec{Name: key, Age: 1}); err != nil {
			t.Fatalf("seed Set(%q): %v", key, err)
		}
	}

	initial := db.getSnapshot()
	wantIdx := make(map[string]uint64, len(order))
	for _, key := range order {
		idx, ok := initial.strmap.getIdxNoLock(key)
		if !ok || idx == 0 {
			t.Fatalf("initial snapshot missing idx for %q", key)
		}
		wantIdx[key] = idx
	}

	deleted := map[string]struct{}{
		"k-02": {},
		"k-08": {},
		"k-04": {},
	}
	for key := range deleted {
		if err := db.Delete(key); err != nil {
			t.Fatalf("Delete(%q): %v", key, err)
		}
	}

	liveOrder := make([]string, 0, len(order)-len(deleted))
	for _, key := range order {
		if _, drop := deleted[key]; drop {
			continue
		}
		liveOrder = append(liveOrder, key)
	}

	gotLive, err := db.QueryKeys(qx.Query(qx.EQ("age", 1)))
	if err != nil {
		t.Fatalf("QueryKeys(live before reopen): %v", err)
	}
	if !slices.Equal(gotLive, liveOrder) {
		t.Fatalf("live order before reopen mismatch: got=%v want=%v", gotLive, liveOrder)
	}

	afterDelete := db.getSnapshot()
	for key := range deleted {
		idx, ok := afterDelete.strmap.getIdxNoLock(key)
		if !ok || idx != wantIdx[key] {
			t.Fatalf("deleted key %q lost retained idx: got=%d ok=%v want=%d", key, idx, ok, wantIdx[key])
		}
	}

	closeCurrent()

	db, raw = openBoltAndNew[string, Rec](t, path)

	reopened := db.getSnapshot()
	for _, key := range order {
		idx, ok := reopened.strmap.getIdxNoLock(key)
		if !ok || idx != wantIdx[key] {
			t.Fatalf("reopened snapshot changed idx for %q: got=%d ok=%v want=%d", key, idx, ok, wantIdx[key])
		}
	}

	gotLive, err = db.QueryKeys(qx.Query(qx.EQ("age", 1)))
	if err != nil {
		t.Fatalf("QueryKeys(live after reopen): %v", err)
	}
	if !slices.Equal(gotLive, liveOrder) {
		t.Fatalf("live order after reopen mismatch: got=%v want=%v", gotLive, liveOrder)
	}

	for key := range deleted {
		v, getErr := db.Get(key)
		if getErr != nil {
			t.Fatalf("Get(%q) after reopen: %v", key, getErr)
		}
		if v != nil {
			t.Fatalf("deleted key %q unexpectedly reappeared after reopen: %#v", key, v)
		}
	}

	for _, key := range order {
		if _, drop := deleted[key]; !drop {
			continue
		}
		if err := db.Set(key, &Rec{Name: key, Age: 1}); err != nil {
			t.Fatalf("reinsert Set(%q): %v", key, err)
		}
	}

	gotAll, err := db.QueryKeys(qx.Query(qx.EQ("age", 1)))
	if err != nil {
		t.Fatalf("QueryKeys(all after reinsert): %v", err)
	}
	if !slices.Equal(gotAll, order) {
		t.Fatalf("reinsert changed internal order: got=%v want=%v", gotAll, order)
	}
}

func TestStringExt_SharedAutoBatchUniqueRejectHolePersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "string_unique_hole.db")

	db, raw := openBoltAndNew[string, StringUniqueTestRec](t, path)
	closeCurrent := func() {
		if db != nil {
			if err := db.Close(); err != nil {
				t.Fatalf("db close: %v", err)
			}
			db = nil
		}
		if raw != nil {
			if err := raw.Close(); err != nil {
				t.Fatalf("raw close: %v", err)
			}
			raw = nil
		}
	}
	t.Cleanup(closeCurrent)

	if err := db.Set("seed", &StringUniqueTestRec{Email: "seed@x", Code: 1}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	badReq, err := db.buildSetAutoBatchRequest("ghost-hole", &StringUniqueTestRec{
		Email: "seed@x",
		Code:  2,
	}, nil, nil, nil)
	if err != nil {
		t.Fatalf("build bad request: %v", err)
	}
	goodReq, err := db.buildSetAutoBatchRequest("real-hole", &StringUniqueTestRec{
		Email: "real@x",
		Code:  3,
	}, nil, nil, nil)
	if err != nil {
		t.Fatalf("build good request: %v", err)
	}

	db.executeAutoBatch([]*autoBatchRequest[string, StringUniqueTestRec]{badReq, goodReq})

	if !errors.Is(badReq.err, ErrUniqueViolation) {
		t.Fatalf("bad request error = %v, want %v", badReq.err, ErrUniqueViolation)
	}
	if goodReq.err != nil {
		t.Fatalf("good request failed: %v", goodReq.err)
	}

	assertHole := func(label string) {
		t.Helper()

		snap := db.getSnapshot()
		if snap == nil || snap.strmap == nil {
			t.Fatalf("%s: expected published strmap snapshot", label)
		}
		if snap.strmap.Next != 3 {
			t.Fatalf("%s: strmap.Next = %d, want 3", label, snap.strmap.Next)
		}

		seedIdx, ok := snap.strmap.getIdxNoLock("seed")
		if !ok || seedIdx != 1 {
			t.Fatalf("%s: seed idx = %d ok=%v, want 1", label, seedIdx, ok)
		}
		if _, ok := snap.strmap.getIdxNoLock("ghost-hole"); ok {
			t.Fatalf("%s: rejected key unexpectedly remained in strmap", label)
		}
		realIdx, ok := snap.strmap.getIdxNoLock("real-hole")
		if !ok || realIdx != 3 {
			t.Fatalf("%s: real-hole idx = %d ok=%v, want 3", label, realIdx, ok)
		}
		if s, ok := snap.strmap.getStringNoLock(2); ok {
			t.Fatalf("%s: hole idx 2 unexpectedly mapped to %q", label, s)
		}
		if s, ok := snap.strmap.getStringNoLock(3); !ok || s != "real-hole" {
			t.Fatalf("%s: idx 3 reverse mapping = %q ok=%v, want real-hole", label, s, ok)
		}

		v, getErr := db.Get("ghost-hole")
		if getErr != nil {
			t.Fatalf("%s: Get(ghost-hole): %v", label, getErr)
		}
		if v != nil {
			t.Fatalf("%s: rejected key persisted: %#v", label, v)
		}

		v, getErr = db.Get("real-hole")
		if getErr != nil {
			t.Fatalf("%s: Get(real-hole): %v", label, getErr)
		}
		if v == nil || v.Email != "real@x" || v.Code != 3 {
			t.Fatalf("%s: real-hole payload mismatch: %#v", label, v)
		}

		gotAll, queryErr := db.QueryKeys(qx.Query())
		if queryErr != nil {
			t.Fatalf("%s: QueryKeys(NOOP): %v", label, queryErr)
		}
		if !slices.Equal(gotAll, []string{"seed", "real-hole"}) {
			t.Fatalf("%s: NOOP query mismatch: got=%v want=[seed real-hole]", label, gotAll)
		}

		gotReal, queryErr := db.QueryKeys(qx.Query(qx.EQ("email", "real@x")))
		if queryErr != nil {
			t.Fatalf("%s: QueryKeys(real email): %v", label, queryErr)
		}
		if !slices.Equal(gotReal, []string{"real-hole"}) {
			t.Fatalf("%s: real email query mismatch: got=%v want=[real-hole]", label, gotReal)
		}
	}

	assertHole("live")

	closeCurrent()

	db, raw = openBoltAndNew[string, StringUniqueTestRec](t, path)

	assertHole("reopen")
}

func TestStringExt_RollbackCreatedStrIdxRestoresCommittedSnapshotBase(t *testing.T) {
	db, _ := openTempDBStringUnique(t)

	seed := []struct {
		key   string
		email string
		code  int
	}{
		{key: "seed-a", email: "a@x", code: 1},
		{key: "seed-b", email: "b@x", code: 2},
		{key: "seed-c", email: "c@x", code: 3},
	}
	for _, item := range seed {
		if err := db.Set(item.key, &StringUniqueTestRec{Email: item.email, Code: item.code}); err != nil {
			t.Fatalf("seed Set(%q): %v", item.key, err)
		}
	}

	db.strmap.Lock()
	before := db.strmap.committed
	beforePublished := db.strmap.committedPub
	db.strmap.Unlock()
	if before == nil || before.Next != uint64(len(seed)) {
		t.Fatalf("unexpected committed base before reject: %#v", before)
	}
	if beforePublished == nil || beforePublished.Next != before.Next {
		t.Fatalf("unexpected committed published snapshot before reject: %#v", beforePublished)
	}

	err := db.Set("ghost-dup", &StringUniqueTestRec{Email: "a@x", Code: 99})
	if !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("duplicate Set error = %v, want %v", err, ErrUniqueViolation)
	}

	db.strmap.Lock()
	if db.strmap.snap != before {
		db.strmap.Unlock()
		t.Fatalf("rollback did not restore committed state snapshot base")
	}
	if db.strmap.published != beforePublished {
		db.strmap.Unlock()
		t.Fatalf("rollback did not restore committed published snapshot")
	}
	if db.strmap.pubSource != before {
		db.strmap.Unlock()
		t.Fatalf("rollback did not restore committed publish source")
	}
	if db.strmap.dirty {
		db.strmap.Unlock()
		t.Fatalf("rollback left strmap dirty")
	}
	db.strmap.Unlock()

	if err := db.Set("real-ok", &StringUniqueTestRec{Email: "real@x", Code: 100}); err != nil {
		t.Fatalf("good Set: %v", err)
	}

	db.strmap.Lock()
	latest := db.strmap.snap
	db.strmap.Unlock()
	if latest == nil {
		t.Fatalf("missing latest state snapshot after successful insert")
	}
	if latest.base != before {
		t.Fatalf("next successful publish did not reuse committed base: got=%p want=%p", latest.base, before)
	}
	if latest.baseNextNoLock() != before.Next {
		t.Fatalf("latest delta base next = %d, want %d", latest.baseNextNoLock(), before.Next)
	}
	if got := strMapSnapshotOwnUsedCount(latest); got != 1 {
		t.Fatalf("latest delta own used count = %d, want 1", got)
	}
	if got, ok := latest.getStringNoLock(before.Next + 1); !ok || got != "real-ok" {
		t.Fatalf("latest delta reverse mapping mismatch: got=%q ok=%v", got, ok)
	}
}

func TestStringExt_ConcurrentLazySnapshotKeyLookup(t *testing.T) {
	db, _ := openTempDBString(t)

	const n = 64
	keys := make([]string, 0, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%02d", i)
		keys = append(keys, key)
		if err := db.Set(key, &Rec{Name: key}); err != nil {
			t.Fatalf("Set(%q): %v", key, err)
		}
	}

	snap := db.getSnapshot()
	if snap == nil || snap.strmap == nil {
		t.Fatalf("expected published string snapshot")
	}
	if snap.strmap.Keys != nil {
		t.Fatalf("expected lazy Keys materialization on published snapshot")
	}

	start := make(chan struct{})
	errCh := make(chan error, len(keys))
	var wg sync.WaitGroup
	for _, key := range keys {
		key := key
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			idx, ok := snap.strmap.getIdxNoLock(key)
			if !ok {
				errCh <- fmt.Errorf("missing idx for %q", key)
				return
			}
			back, ok := snap.strmap.getStringNoLock(idx)
			if !ok || back != key {
				errCh <- fmt.Errorf("round-trip mismatch for %q: idx=%d back=%q ok=%v", key, idx, back, ok)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	if snap.strmap.Keys == nil {
		t.Fatalf("expected concurrent lookup to materialize Keys")
	}
}

func TestStringExt_PublishedReadPagesPreserveQueryAndScanOrder(t *testing.T) {
	db, _ := openTempDBString(t)

	const n = 320
	want := make([]string, 0, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%03d", i)
		want = append(want, key)
		if err := db.Set(key, &Rec{Name: key, Age: i}); err != nil {
			t.Fatalf("Set(%q): %v", key, err)
		}
	}

	snap := db.getSnapshot()
	if snap == nil || snap.strmap == nil {
		t.Fatalf("expected published string snapshot")
	}
	if len(snap.strmap.readDirs) == 0 {
		t.Fatalf("expected multi-page published read layout")
	}

	gotKeys, err := db.QueryKeys(qx.Query())
	if err != nil {
		t.Fatalf("QueryKeys(NOOP): %v", err)
	}
	if !slices.Equal(gotKeys, want) {
		t.Fatalf("query order mismatch: got=%v want=%v", gotKeys[:min(len(gotKeys), 8)], want[:8])
	}

	gotScan, err := stringTestScanSnapshotKeys(db, snap, "")
	if err != nil {
		t.Fatalf("scan snapshot keys: %v", err)
	}
	if !slices.Equal(gotScan, want) {
		t.Fatalf("scan order mismatch: got=%v want=%v", gotScan[:min(len(gotScan), 8)], want[:8])
	}
}

func TestStringExt_BeginQueryTxSnapshotScanAndQueryStayConsistentDuringDeleteReinsertChurn(t *testing.T) {
	db, _ := openTempDBString(t)

	const (
		keySpace  = 32
		writers   = 4
		readers   = 4
		writerOps = 180
		readerOps = 220
	)

	for i := 0; i < keySpace; i++ {
		key := fmt.Sprintf("key-%02d", i)
		if err := db.Set(key, &Rec{
			Name:   key,
			Age:    i,
			Active: i%2 == 0,
			Tags:   []string{"seed"},
			Meta:   Meta{Country: "NL"},
		}); err != nil {
			t.Fatalf("seed Set(%q): %v", key, err)
		}
	}

	errCh := make(chan error, writers+readers)
	var wg sync.WaitGroup

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < writerOps; i++ {
				key := fmt.Sprintf("key-%02d", (worker*17+i*7+i*i)%keySpace)
				if err := db.Delete(key); err != nil {
					errCh <- fmt.Errorf("writer=%d Delete(%q): %w", worker, key, err)
					return
				}
				if err := db.Set(key, &Rec{
					Name:   key,
					Age:    1000 + worker*writerOps + i,
					Active: (worker+i)%2 == 0,
					Tags: []string{
						fmt.Sprintf("worker-%d", worker),
						fmt.Sprintf("step-%d", i%5),
					},
					Meta: Meta{Country: "NL"},
				}); err != nil {
					errCh <- fmt.Errorf("writer=%d Set(%q): %w", worker, key, err)
					return
				}
			}
		}(w)
	}

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(reader int) {
			defer wg.Done()

			for i := 0; i < readerOps; i++ {
				tx, snap, seq, ref, err := db.beginQueryTxSnapshot()
				if err != nil {
					errCh <- fmt.Errorf("reader=%d beginQueryTxSnapshot: %w", reader, err)
					return
				}

				scanned, scanErr := stringTestScanSnapshotKeys(db, snap, "")
				var queried []string
				if scanErr == nil {
					view := db.makeQueryView(snap)
					prepared, viewQ, prepErr := prepareTestQuery(db, qx.Query())
					if prepErr != nil {
						scanErr = prepErr
					} else {
						queried, scanErr = view.execQuery(&viewQ, false, false)
						prepared.Release()
					}
					db.releaseQueryView(view)
				}
				if scanErr == nil && !slices.Equal(scanned, queried) {
					scanErr = fmt.Errorf("scan/query mismatch: scanned=%v queried=%v", scanned, queried)
				}
				if scanErr == nil {
					values, getErr := db.batchGetTx(tx, scanned...)
					if getErr != nil {
						scanErr = getErr
					} else if len(values) != len(scanned) {
						scanErr = fmt.Errorf("batchGetTx len mismatch: got=%d want=%d", len(values), len(scanned))
					} else {
						for j, key := range scanned {
							if values[j] == nil {
								scanErr = fmt.Errorf("snapshot returned missing value for key=%q", key)
								break
							}
							if values[j].Name != key {
								scanErr = fmt.Errorf("snapshot returned wrong value for key=%q name=%q", key, values[j].Name)
								break
							}
						}
					}
				}

				rollback(tx)
				db.unpinSnapshotRef(seq, ref)

				if scanErr != nil {
					errCh <- fmt.Errorf("reader=%d iter=%d: %w", reader, i, scanErr)
					return
				}
			}
		}(r)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		t.FailNow()
	}

	gotAll, err := db.QueryKeys(qx.Query())
	if err != nil {
		t.Fatalf("final QueryKeys(NOOP): %v", err)
	}
	if len(gotAll) != keySpace {
		t.Fatalf("final key count mismatch: got=%d want=%d", len(gotAll), keySpace)
	}
	for _, key := range gotAll {
		v, getErr := db.Get(key)
		if getErr != nil {
			t.Fatalf("final Get(%q): %v", key, getErr)
		}
		if v == nil || v.Name != key {
			t.Fatalf("final value mismatch for %q: %#v", key, v)
		}
	}
}
