package rbi

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
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

func TestStringKeys_ExoticCharacters(t *testing.T) {
	db, _ := openTempDBString(t)

	keys := []string{
		"simple",
		"With Space",
		"Кириллица",
		"🔥emoji🔥",
		"user/123/profile",
		"\x00\x01\x02",
	}

	for i, k := range keys {
		rec := &Rec{Name: fmt.Sprintf("rec-%d", i), Age: i}
		if err := db.Set(k, rec); err != nil {
			t.Fatalf("Set(%q) failed: %v", k, err)
		}
	}

	for i, k := range keys {
		v, err := db.Get(k)
		if err != nil {
			t.Fatalf("Get(%q) failed: %v", k, err)
		}
		if v == nil {
			t.Fatalf("Get(%q) returned nil", k)
		}

		expectedName := fmt.Sprintf("rec-%d", i)
		if v.Name != expectedName {
			t.Errorf("Key %q: expected name %q, got %q", k, expectedName, v.Name)
		}

		// validate index lookup via string mapper
		q := qx.Query(qx.EQ("name", expectedName))
		ids, err := db.QueryKeys(q)
		if err != nil {
			t.Fatalf("QueryKeys for %q failed: %v", k, err)
		}
		if len(ids) != 1 {
			t.Fatalf("QueryKeys for %q returned %d results, expected 1", k, len(ids))
		}
		if ids[0] != k {
			t.Errorf("QueryKeys mismatch: expected key %q, got %q", k, ids[0])
		}
	}
}

func TestStringKeys_Persistence_MappingStability(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "string_keys_persist.db")

	db, raw := openBoltAndNew[string, Rec](t, path)

	if err := db.Set("alice", &Rec{Age: 20}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set("bob", &Rec{Age: 30}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// force index and string map persistence
	if err := db.Close(); err != nil {
		t.Fatalf("db close: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}

	// reopen
	db2, raw2 := openBoltAndNew[string, Rec](t, path)
	defer func() {
		if err := db2.Close(); err != nil {
			t.Fatalf("db2 close: %v", err)
		}
		if err := raw2.Close(); err != nil {
			t.Fatalf("raw2 close: %v", err)
		}
	}()

	// validate correctness after reopen
	q := qx.Query(qx.EQ("age", 20))
	ids, err := db2.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}

	if len(ids) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(ids))
	}
	if ids[0] != "alice" {
		t.Fatalf("Expected key 'alice', got %q (string-ID mapping corrupted)", ids[0])
	}

	// validate ID counter restoration
	if err = db2.Set("charlie", &Rec{Age: 40}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	ids, _ = db2.QueryKeys(qx.Query(qx.EQ("age", 40)))
	if len(ids) != 1 || ids[0] != "charlie" {
		t.Errorf("Insert after reopen failed: %v", ids)
	}
}

func TestStringKeys_StartupBuild_FromScratch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "string_keys_rebuild.db")

	db, raw := openBoltAndNew[string, Rec](t, path)

	keys := []string{"k1", "k2", "k3"}
	for i, k := range keys {
		if err := db.Set(k, &Rec{Age: i * 10}); err != nil {
			t.Fatalf("Set (%q): %v", k, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db close: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}

	// Remove file to force startup build from bbolt.
	rbiFile := path + ".Rec.rbi"
	if err := os.Remove(rbiFile); err != nil && !os.IsNotExist(err) {
		t.Logf("Index file removal failed: %v", err)
	}

	// Reopen and build the index from stored records.
	db2, raw2 := openBoltAndNew[string, Rec](t, path)
	defer func() {
		if err := db2.Close(); err != nil {
			t.Fatalf("db2 close: %v", err)
		}
		if err := raw2.Close(); err != nil {
			t.Fatalf("raw2 close: %v", err)
		}
	}()

	// validate rebuilt index
	for i, k := range keys {
		age := i * 10
		ids, err := db2.QueryKeys(qx.Query(qx.EQ("age", age)))
		if err != nil {
			t.Fatalf("QueryKeys: %v", err)
		}
		if len(ids) != 1 || ids[0] != k {
			t.Errorf("startup build mismatch for key %q: %v", k, ids)
		}
	}
}

func TestStringKeys_Concurrency_MapperStress(t *testing.T) {
	db, _ := openTempDBString(t)

	var wg sync.WaitGroup
	workers := 10
	writes := 100

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < writes; j++ {
				key := fmt.Sprintf("w-%d-k-%d", workerID, j)
				rec := &Rec{Name: "worker", Age: workerID}

				if err := db.Set(key, rec); err != nil {
					t.Errorf("Set failed: %v", err)
					return
				}

				// immediate read to stress RLock paths
				v, err := db.Get(key)
				if err != nil || v == nil {
					t.Errorf("Get failed: %v", err)
				}
			}
		}(i)
	}
	wg.Wait()

	// validate total count
	totalExpected := workers * writes
	count, _ := db.Count()
	if int(count) != totalExpected {
		t.Errorf("expected %d records, got %d", totalExpected, count)
	}

	// validate index query under load
	ids, _ := db.QueryKeys(qx.Query(qx.EQ("name", "worker")))
	if len(ids) != totalExpected {
		t.Errorf("expected %d keys, got %d", totalExpected, len(ids))
	}
}

func TestStringKeys_BatchMutations_ModelReplayConsistency(t *testing.T) {
	db, _ := openTempDBString(t)

	type modelRec struct {
		name   string
		age    int
		active bool
	}

	const idSpace = 96

	makeKeys := func(base, size int) []string {
		out := make([]string, 0, size)
		used := make(map[string]struct{}, size)
		for step := 0; len(out) < size; step++ {
			idx := (base + step*11 + step*step + size*7) % idSpace
			key := fmt.Sprintf("k-%03d", idx)
			if _, ok := used[key]; ok {
				continue
			}
			used[key] = struct{}{}
			out = append(out, key)
		}
		return out
	}

	model := make(map[string]modelRec, idSpace)
	allNames := make(map[string]struct{}, 1024)

	for i := 0; i < 220; i++ {
		switch i % 3 {
		case 0: // BatchSet
			keys := makeKeys(i*13+17, 2+(i%4))
			vals := make([]*Rec, 0, len(keys))
			for pos, key := range keys {
				name := fmt.Sprintf("u-%s-v%03d-p%02d", key, i, pos)
				age := 20 + i + pos
				active := (i+pos)%2 == 0
				allNames[name] = struct{}{}
				vals = append(vals, &Rec{
					Name:   name,
					Age:    age,
					Active: active,
					Tags:   []string{fmt.Sprintf("g-%d", i%5)},
					Meta:   Meta{Country: "NL"},
				})
				model[key] = modelRec{name: name, age: age, active: active}
			}
			if err := db.BatchSet(keys, vals); err != nil {
				t.Fatalf("BatchSet(step=%d): %v", i, err)
			}

		case 1: // BatchPatch
			keys := makeKeys(i*7+31, 3+(i%3))
			age := 900 + i
			active := i%2 == 0
			patch := []Field{
				{Name: "age", Value: age},
				{Name: "active", Value: active},
			}
			if err := db.BatchPatch(keys, patch); err != nil {
				t.Fatalf("BatchPatch(step=%d): %v", i, err)
			}
			for _, key := range keys {
				if cur, ok := model[key]; ok {
					cur.age = age
					cur.active = active
					model[key] = cur
				}
			}

		default: // BatchDelete
			keys := makeKeys(i*5+19, 1+(i%4))
			if err := db.BatchDelete(keys); err != nil {
				t.Fatalf("BatchDelete(step=%d): %v", i, err)
			}
			for _, key := range keys {
				delete(model, key)
			}
		}
	}

	count, err := db.Count()
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if int(count) != len(model) {
		t.Fatalf("count mismatch: got=%d want=%d", count, len(model))
	}

	liveNames := make(map[string]string, len(model))
	wantActive := make(map[string]struct{}, len(model))

	for key, exp := range model {
		v, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if v == nil {
			t.Fatalf("Get(%q): nil value", key)
		}
		if v.Name != exp.name || v.Age != exp.age || v.Active != exp.active {
			t.Fatalf("value mismatch for %q: got={name:%q age:%d active:%v} want={name:%q age:%d active:%v}",
				key, v.Name, v.Age, v.Active, exp.name, exp.age, exp.active)
		}

		ids, err := db.QueryKeys(qx.Query(qx.EQ("name", exp.name)))
		if err != nil {
			t.Fatalf("QueryKeys(name=%q): %v", exp.name, err)
		}
		if len(ids) != 1 || ids[0] != key {
			t.Fatalf("name index mismatch for %q: got=%v want=[%q]", exp.name, ids, key)
		}

		liveNames[exp.name] = key
		if exp.active {
			wantActive[key] = struct{}{}
		}
	}

	gotActive, err := db.QueryKeys(qx.Query(qx.EQ("active", true)))
	if err != nil {
		t.Fatalf("QueryKeys(active=true): %v", err)
	}
	gotActiveSet := make(map[string]struct{}, len(gotActive))
	for _, key := range gotActive {
		gotActiveSet[key] = struct{}{}
	}
	if len(gotActiveSet) != len(wantActive) {
		t.Fatalf("active set size mismatch: got=%d want=%d", len(gotActiveSet), len(wantActive))
	}
	for key := range wantActive {
		if _, ok := gotActiveSet[key]; !ok {
			t.Fatalf("active set missing key %q", key)
		}
	}

	const staleProbe = 320
	staleChecked := 0
	for name := range allNames {
		if _, live := liveNames[name]; live {
			continue
		}
		ids, err := db.QueryKeys(qx.Query(qx.EQ("name", name)))
		if err != nil {
			t.Fatalf("QueryKeys(stale name=%q): %v", name, err)
		}
		if len(ids) != 0 {
			t.Fatalf("stale name %q still indexed: ids=%v", name, ids)
		}
		staleChecked++
		if staleChecked >= staleProbe {
			break
		}
	}
}

func TestStringKeys_ConcurrentMixedOps_FinalIndexConsistency(t *testing.T) {
	db, _ := openTempDBString(t)

	const (
		keySpace     = 144
		writers      = 4
		readers      = 3
		opsPerWriter = 220
		staleProbe   = 320
	)

	historicNames := sync.Map{}
	for i := 0; i < keySpace; i++ {
		key := fmt.Sprintf("id-%03d", i)
		name := fmt.Sprintf("seed-%03d", i)
		if err := db.Set(key, &Rec{
			Name:   name,
			Age:    18 + (i % 40),
			Active: i%2 == 0,
			Tags:   []string{"seed"},
			Meta:   Meta{Country: "NL"},
		}); err != nil {
			t.Fatalf("seed Set(%q): %v", key, err)
		}
		historicNames.Store(name, struct{}{})
	}

	errCh := make(chan error, writers+readers+16)
	stopReaders := make(chan struct{})

	readQueries := []*qx.QX{
		qx.Query(qx.EQ("active", true)),
		qx.Query(qx.PREFIX("name", "live-")),
		qx.Query(qx.GTE("age", 0)),
		qx.Query(qx.HASANY("tags", []string{"seed", "w0", "w1", "w2", "w3"})),
	}

	var readersWG sync.WaitGroup
	for r := 0; r < readers; r++ {
		readersWG.Add(1)
		go func(readerID int) {
			defer readersWG.Done()
			i := 0
			for {
				select {
				case <-stopReaders:
					return
				default:
				}

				q := readQueries[(readerID+i)%len(readQueries)]
				i++

				if _, err := db.QueryKeys(q); err != nil {
					errCh <- fmt.Errorf("reader=%d QueryKeys: %w", readerID, err)
					return
				}
				if _, err := db.Query(q); err != nil {
					errCh <- fmt.Errorf("reader=%d Query: %w", readerID, err)
					return
				}
				if _, err := db.Count(q.Filter); err != nil {
					errCh <- fmt.Errorf("reader=%d Count: %w", readerID, err)
					return
				}
			}
		}(r)
	}

	var writersWG sync.WaitGroup
	for w := 0; w < writers; w++ {
		w := w
		writersWG.Add(1)
		go func() {
			defer writersWG.Done()
			for i := 0; i < opsPerWriter; i++ {
				key := fmt.Sprintf("id-%03d", (w*1009+i*37+i*i)%keySpace)
				switch (w + i) % 3 {
				case 0:
					name := fmt.Sprintf("live-w%02d-i%04d-%s", w, i, key)
					historicNames.Store(name, struct{}{})
					if err := db.Set(key, &Rec{
						Name:   name,
						Age:    30 + w + i,
						Active: (w+i)%2 == 0,
						Tags: []string{
							fmt.Sprintf("w%d", w),
							fmt.Sprintf("grp-%d", i%7),
						},
						Meta: Meta{Country: "NL"},
					}); err != nil {
						errCh <- fmt.Errorf("writer=%d Set(%q): %w", w, key, err)
						return
					}

				case 1:
					patch := []Field{
						{Name: "age", Value: 700 + w*10 + i},
						{Name: "active", Value: i%2 == 0},
					}
					if err := db.BatchPatch([]string{key}, patch); err != nil {
						errCh <- fmt.Errorf("writer=%d BatchPatch(%q): %w", w, key, err)
						return
					}

				default:
					if err := db.Delete(key); err != nil {
						errCh <- fmt.Errorf("writer=%d Delete(%q): %w", w, key, err)
						return
					}
				}
			}
		}()
	}

	writersWG.Wait()
	close(stopReaders)
	readersWG.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("%v", err)
	}
	if t.Failed() {
		t.FailNow()
	}

	type liveRec struct {
		key  string
		name string
	}
	live := make(map[string]liveRec, keySpace)

	err := db.SeqScan("", func(key string, rec *Rec) (bool, error) {
		if rec == nil {
			return false, fmt.Errorf("nil record for key=%q", key)
		}
		if rec.Name == "" {
			return false, fmt.Errorf("empty name for key=%q", key)
		}
		live[key] = liveRec{key: key, name: rec.Name}
		return true, nil
	})
	if err != nil {
		t.Fatalf("SeqScan: %v", err)
	}

	count, err := db.Count()
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if int(count) != len(live) {
		t.Fatalf("count mismatch after concurrent ops: got=%d want=%d", count, len(live))
	}

	liveByName := make(map[string]string, len(live))
	for _, rec := range live {
		ids, err := db.QueryKeys(qx.Query(qx.EQ("name", rec.name)))
		if err != nil {
			t.Fatalf("QueryKeys(name=%q): %v", rec.name, err)
		}
		if len(ids) != 1 || ids[0] != rec.key {
			t.Fatalf("name index mismatch: name=%q got=%v want=[%q]", rec.name, ids, rec.key)
		}
		liveByName[rec.name] = rec.key
	}

	staleChecked := 0
	historicNames.Range(func(k, _ any) bool {
		if staleChecked >= staleProbe {
			return false
		}
		name, ok := k.(string)
		if !ok {
			return true
		}
		if _, liveNow := liveByName[name]; liveNow {
			return true
		}
		ids, err := db.QueryKeys(qx.Query(qx.EQ("name", name)))
		if err != nil {
			t.Fatalf("QueryKeys(stale name=%q): %v", name, err)
		}
		if len(ids) != 0 {
			t.Fatalf("stale name %q still indexed: ids=%v", name, ids)
		}
		staleChecked++
		return true
	})
}

func TestStringKeys_QueryOrder_FollowsInternalIndex(t *testing.T) {
	db, _ := openTempDBString(t)

	inputs := []string{"b", "a", "d", "c"}
	for _, k := range inputs {
		err := db.Set(k, &Rec{Age: 1})
		if err != nil {
			t.Fatalf("Set (%q): %v", k, err)
		}
	}

	// returns results ordered by internal id (insertion order)
	ids, err := db.QueryKeys(qx.Query(qx.EQ("age", 1)))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}

	expectedOrder := []string{"b", "a", "d", "c"}
	if len(ids) != 4 {
		t.Fatalf("result count mismatch: want: %v, got: %v", len(expectedOrder), len(ids))
	}

	for i := range ids {
		if ids[i] != expectedOrder[i] {
			t.Errorf("order mismatch at %d: got %q, want %q", i, ids[i], expectedOrder[i])
		}
	}
}

func TestStringKeys_VeryLongKey(t *testing.T) {
	db, _ := openTempDBString(t)

	longKey := strings.Repeat("A", 4096)

	err := db.Set(longKey, &Rec{Name: "long"})
	if err != nil {
		t.Fatalf("Set: %v", err)
	}

	v, err := db.Get(longKey)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if v == nil {
		t.Fatalf("get failed: %v", v)
	}
	if v.Name != "long" {
		t.Errorf("value mismatch: %v", v.Name)
	}

	ids, err := db.QueryKeys(qx.Query(qx.EQ("name", "long")))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) != 1 || ids[0] != longKey {
		t.Errorf("index lookup failed")
	}
}

func TestStringKeys_QueryResultKey_RemainsValidAfterClose(t *testing.T) {
	db, _ := openTempDBString(t)
	key := "my-key"
	if err := db.Set(key, &Rec{}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	ids, err := db.QueryKeys(qx.Query())
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	resultKey := ids[0]

	// close to invalidate mmap regions
	if err = db.Close(); err != nil {
		t.Fatalf("db close: %v", err)
	}

	// force string access
	s := strings.Clone(resultKey)
	if s != key {
		t.Errorf("key corrupted after DB close: %q", s)
	}
}

func TestMissingIDs_DoNotCreateStringRecords(t *testing.T) {
	db, _ := openTempDBStringProduct(t)

	if err := db.Set("p1", &Product{SKU: "p1", Price: 10}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.Patch("missing", []Field{{Name: "price", Value: 1.0}}); err != nil {
		t.Fatalf("Patch(missing): %v", err)
	}
	if err := db.Delete("missing"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := db.BatchDelete([]string{"missing2", "missing3"}); err != nil {
		t.Fatalf("BatchDelete: %v", err)
	}
	if err := db.BatchPatch([]string{"missing4"}, []Field{{Name: "price", Value: 2.0}}); err != nil {
		t.Fatalf("BatchPatch: %v", err)
	}

	got, err := db.BatchGet("missing", "missing2", "missing3", "missing4")
	if err != nil {
		t.Fatalf("BatchGet(missing ids): %v", err)
	}
	for i := range got {
		if got[i] != nil {
			t.Fatalf("missing id %d unexpectedly persisted: %#v", i, got[i])
		}
	}
}

func TestFailedSetPaths_DoNotPersistStringKeys(t *testing.T) {
	db, _ := openTempDBStringProduct(t)

	if err := db.Set("p1", &Product{SKU: "p1", Price: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	cbErr := errors.New("before commit fail")
	cb := func(_ *bbolt.Tx, _ string, _ *Product, _ *Product) error { return cbErr }

	if err := db.Set("ghost-set", &Product{SKU: "ghost-set", Price: 11}, BeforeCommit(cb)); !errors.Is(err, cbErr) {
		t.Fatalf("Set callback error mismatch: %v", err)
	}
	if v, err := db.Get("ghost-set"); err != nil {
		t.Fatalf("Get(ghost-set): %v", err)
	} else if v != nil {
		t.Fatalf("ghost-set should not persist after rollback, got %#v", v)
	}

	err := db.BatchSet(
		[]string{"ghost-many-1", "ghost-many-2"},
		[]*Product{
			{SKU: "ghost-many-1", Price: 12},
			{SKU: "ghost-many-2", Price: 13},
		},
		BeforeCommit(cb),
	)
	if !errors.Is(err, cbErr) {
		t.Fatalf("BatchSet callback error mismatch: %v", err)
	}
	if v, err := db.Get("ghost-many-1"); err != nil {
		t.Fatalf("Get(ghost-many-1): %v", err)
	} else if v != nil {
		t.Fatalf("ghost-many-1 should not persist after rollback, got %#v", v)
	}
	if v, err := db.Get("ghost-many-2"); err != nil {
		t.Fatalf("Get(ghost-many-2): %v", err)
	} else if v != nil {
		t.Fatalf("ghost-many-2 should not persist after rollback, got %#v", v)
	}
}

func TestBatchSet_CallbackError_DoesNotPersistStringKey(t *testing.T) {
	db, _ := openTempDBStringProduct(t)

	if err := db.Set("p1", &Product{SKU: "p1", Price: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	cbErr := errors.New("cb fail")
	err := db.Set("ghost-cb", &Product{SKU: "ghost-cb", Price: 11}, BeforeCommit(func(_ *bbolt.Tx, _ string, _ *Product, _ *Product) error {
		return cbErr
	}))
	if !errors.Is(err, cbErr) {
		t.Fatalf("Set callback error mismatch: %v", err)
	}
	if v, err := db.Get("ghost-cb"); err != nil {
		t.Fatalf("Get(ghost-cb): %v", err)
	} else if v != nil {
		t.Fatalf("ghost-cb should not persist after rollback, got %#v", v)
	}

	bs := db.AutoBatchStats()
	if bs.CallbackOps == 0 || bs.CallbackErrors == 0 {
		t.Fatalf("expected callback error via batch path, stats=%+v", bs)
	}
}

func TestBatchSet_UniqueReject_DoesNotPersistStringKey(t *testing.T) {
	db, _ := openTempDBStringUnique(t)

	if err := db.Set("u1", &StringUniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	err := db.Set("u-dup", &StringUniqueTestRec{Email: "a@x", Code: 2})
	if err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation, got: %v", err)
	}
	if v, err := db.Get("u-dup"); err != nil {
		t.Fatalf("Get(u-dup): %v", err)
	} else if v != nil {
		t.Fatalf("u-dup should not persist after unique reject, got %#v", v)
	}

	bs := db.AutoBatchStats()
	if bs.UniqueRejected == 0 {
		t.Fatalf("expected unique rejection in batch stats, got %+v", bs)
	}
}

type StringUniqueTestRec struct {
	Email string `db:"email" rbi:"unique"`
	Code  int    `db:"code"  rbi:"unique"`
}

func openTempDBStringUnique(t *testing.T, options ...Options) (*DB[string, StringUniqueTestRec], string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_string_unique.db")
	db, raw := openBoltAndNew[string, StringUniqueTestRec](t, path, options...)
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	return db, path
}
