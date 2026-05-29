package rbi

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"go.etcd.io/bbolt"
)

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

	closeCurrent()

	db, raw = openBoltAndNew[string, Rec](t, path)

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

func TestStringExt_SharedAutoBatchUniqueRejectKeepsCommittedKeysAcrossReopen(t *testing.T) {
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

	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	blockCommit := BeforeCommit(func(*bbolt.Tx, string, *StringUniqueTestRec, *StringUniqueTestRec) error {
		close(entered)
		<-release
		return nil
	})

	blockerDone := make(chan error, 1)
	go func() {
		blockerDone <- db.Set("blocker", &StringUniqueTestRec{Email: "blocker@x", Code: 2}, blockCommit)
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for blocked commit")
	}
	baseline := db.AutoBatchStats()

	badDone := make(chan error, 1)
	go func() {
		badDone <- db.Set("ghost-hole", &StringUniqueTestRec{Email: "seed@x", Code: 3})
	}()
	waitAutoBatchExtraStats(t, db, "queued rejected string set", func(st AutoBatchStats) bool {
		return st.Submitted == baseline.Submitted+1 && st.QueueLen == 1
	})

	goodDone := make(chan error, 1)
	go func() {
		goodDone <- db.Set("real-hole", &StringUniqueTestRec{Email: "real@x", Code: 4})
	}()
	waitAutoBatchExtraStats(t, db, "queued rejected+accepted string sets", func(st AutoBatchStats) bool {
		return st.Submitted == baseline.Submitted+2 && st.QueueLen == 2
	})

	close(release)

	if err := <-blockerDone; err != nil {
		t.Fatalf("blocker Set: %v", err)
	}
	if err := <-badDone; !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("bad Set error = %v, want %v", err, ErrUniqueViolation)
	}
	if err := <-goodDone; err != nil {
		t.Fatalf("good Set: %v", err)
	}

	assertHole := func(label string) {
		t.Helper()

		v, getErr := db.Get("ghost-hole")
		if getErr != nil {
			t.Fatalf("%s: Get(ghost-hole): %v", label, getErr)
		}
		if v != nil {
			t.Fatalf("%s: rejected key persisted: %#v", label, v)
		}
		v, getErr = db.Get("blocker")
		if getErr != nil {
			t.Fatalf("%s: Get(blocker): %v", label, getErr)
		}
		if v == nil || v.Email != "blocker@x" || v.Code != 2 {
			t.Fatalf("%s: blocker payload mismatch: %#v", label, v)
		}

		v, getErr = db.Get("real-hole")
		if getErr != nil {
			t.Fatalf("%s: Get(real-hole): %v", label, getErr)
		}
		if v == nil || v.Email != "real@x" || v.Code != 4 {
			t.Fatalf("%s: real-hole payload mismatch: %#v", label, v)
		}

		allQ := qx.Query()
		gotAll, queryErr := db.QueryKeys(allQ)
		if queryErr != nil {
			t.Fatalf("%s: QueryKeys(NOOP): %v", label, queryErr)
		}
		if !queryStringIDsEqual(allQ, gotAll, []string{"seed", "blocker", "real-hole"}) {
			t.Fatalf("%s: NOOP query mismatch: got=%v want=[seed blocker real-hole]", label, gotAll)
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

func TestStringExt_UniqueRejectDoesNotLeakRejectedStringKey(t *testing.T) {
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

	err := db.Set("ghost-dup", &StringUniqueTestRec{Email: "a@x", Code: 99})
	if !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("duplicate Set error = %v, want %v", err, ErrUniqueViolation)
	}
	if got, getErr := db.Get("ghost-dup"); getErr != nil || got != nil {
		t.Fatalf("rejected key lookup = %#v/%v, want nil/<nil>", got, getErr)
	}

	if err := db.Set("real-ok", &StringUniqueTestRec{Email: "real@x", Code: 100}); err != nil {
		t.Fatalf("good Set: %v", err)
	}

	gotAll, err := db.QueryKeys(qx.Query())
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	wantAll := []string{"seed-a", "seed-b", "seed-c", "real-ok"}
	if !queryStringIDsEqual(qx.Query(), gotAll, wantAll) {
		t.Fatalf("query set mismatch: got=%v want=%v", gotAll, wantAll)
	}
}

func TestStringExt_PublishedReadPagesPreserveQuerySetAndScanOrder(t *testing.T) {
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

	allQ := qx.Query()
	gotKeys, err := db.QueryKeys(allQ)
	if err != nil {
		t.Fatalf("QueryKeys(NOOP): %v", err)
	}
	if !queryStringIDsEqual(allQ, gotKeys, want) {
		t.Fatalf("query set mismatch: got=%v want=%v", gotKeys[:min(len(gotKeys), 8)], want[:8])
	}

	gotScan := make([]string, 0, n)
	err = db.ScanKeys("", func(id string) (bool, error) {
		gotScan = append(gotScan, id)
		return true, nil
	})
	if err != nil {
		t.Fatalf("ScanKeys: %v", err)
	}
	if !slices.Equal(gotScan, want) {
		t.Fatalf("scan order mismatch: got=%v want=%v", gotScan[:min(len(gotScan), 8)], want[:8])
	}
}

func TestStringExt_QueryStaysConsistentDuringDeleteReinsertChurn(t *testing.T) {
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
				values, err := db.Query(qx.Query())
				if err != nil {
					errCh <- fmt.Errorf("reader=%d Query: %w", reader, err)
					return
				}
				if len(values) > keySpace {
					errCh <- fmt.Errorf("reader=%d iter=%d too many values: got=%d want<=%d", reader, i, len(values), keySpace)
					return
				}

				var seen [keySpace]bool
				for _, v := range values {
					if v == nil {
						errCh <- fmt.Errorf("reader=%d iter=%d query returned nil value", reader, i)
						return
					}
					if len(v.Name) != len("key-00") || v.Name[:4] != "key-" || v.Name[4] < '0' || v.Name[4] > '9' || v.Name[5] < '0' || v.Name[5] > '9' {
						errCh <- fmt.Errorf("reader=%d iter=%d query returned malformed key name %q", reader, i, v.Name)
						return
					}
					idx := int(v.Name[4]-'0')*10 + int(v.Name[5]-'0')
					if idx >= keySpace {
						errCh <- fmt.Errorf("reader=%d iter=%d query returned key outside seed space %q", reader, i, v.Name)
						return
					}
					if seen[idx] {
						errCh <- fmt.Errorf("reader=%d iter=%d query returned duplicate key %q", reader, i, v.Name)
						return
					}
					seen[idx] = true
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
