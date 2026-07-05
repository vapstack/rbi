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

	"github.com/vapstack/rbi/rbierrors"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/keycodec"
	"go.etcd.io/bbolt"
)

func TestStringMapBucketEnsureCreatesAndReuses(t *testing.T) {
	raw, _ := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	dataBucket := []byte("users")
	if err := raw.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucket(dataBucket); err != nil {
			return err
		}
		m, err := createStrMapBucket(tx, dataBucket)
		if err != nil {
			return err
		}
		k, v := m.Cursor().First()
		if k != nil || v != nil {
			return fmt.Errorf("new string map is not empty")
		}
		return nil
	}); err != nil {
		t.Fatalf("ensureStringMapBucket: %v", err)
	}

	if err := raw.View(func(tx *bbolt.Tx) error {
		_, err := createStrMapBucket(tx, dataBucket)
		return err
	}); err != nil {
		t.Fatalf("ensureStringMapBucket existing: %v", err)
	}
}

func TestStringMapBucketMissingForNonEmptyDataFails(t *testing.T) {
	raw, _ := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	dataBucket := []byte("users")
	err := raw.Update(func(tx *bbolt.Tx) error {
		data, err := tx.CreateBucket(dataBucket)
		if err != nil {
			return err
		}
		if err = data.Put([]byte("k"), []byte("payload")); err != nil {
			return err
		}
		_, err = createStrMapBucket(tx, dataBucket)
		return err
	})
	if !errors.Is(err, rbierrors.ErrInvalidStringStorageFormat) {
		t.Fatalf("err=%v", err)
	}
}

func TestStringMapBucketEnsureExistingIsStructuralOnly(t *testing.T) {
	raw, _ := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	dataBucket := []byte("users")
	if err := raw.Update(func(tx *bbolt.Tx) error {
		data, err := tx.CreateBucket(dataBucket)
		if err != nil {
			return err
		}
		if err = data.Put([]byte("k"), []byte("bad")); err != nil {
			return err
		}
		mapName := append(append([]byte(nil), dataBucket...), stringMapBucketSuffix...)
		m, err := tx.CreateBucket(mapName)
		if err != nil {
			return err
		}
		return m.Put([]byte("short"), []byte("stale"))
	}); err != nil {
		t.Fatalf("setup: %v", err)
	}

	if err := raw.View(func(tx *bbolt.Tx) error {
		_, err := createStrMapBucket(tx, dataBucket)
		return err
	}); err != nil {
		t.Fatalf("ensureStringMapBucket existing: %v", err)
	}
}

func putStringMapKeyTest(m *bbolt.Bucket, idx uint64, key []byte) error {
	var buf [8]byte
	return m.Put(keycodec.U64BytesWithBuf(idx, &buf), key)
}

func deleteStringMapKeyTest(m *bbolt.Bucket, idx uint64) error {
	var buf [8]byte
	return m.Delete(keycodec.U64BytesWithBuf(idx, &buf))
}

func TestStringMapPutGetDeleteAndOrdering(t *testing.T) {
	raw, _ := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	if err := raw.Update(func(tx *bbolt.Tx) error {
		m, err := tx.CreateBucket([]byte("map"))
		if err != nil {
			return err
		}
		if err = putStringMapKeyTest(m, 256, []byte("c")); err != nil {
			return err
		}
		if err = putStringMapKeyTest(m, 1, []byte("a")); err != nil {
			return err
		}
		if err = putStringMapKeyTest(m, 2, []byte("b")); err != nil {
			return err
		}
		var key [8]byte
		if !slices.Equal(m.Get(keycodec.U64BytesWithBuf(2, &key)), []byte("b")) {
			return fmt.Errorf("get idx=2 failed")
		}

		var ids []uint64
		c := m.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			ids = append(ids, keycodec.U64FromBytes(k))
		}
		if !slices.Equal(ids, []uint64{1, 2, 256}) {
			return fmt.Errorf("ids=%v", ids)
		}

		if err = deleteStringMapKeyTest(m, 2); err != nil {
			return err
		}
		if m.Get(keycodec.U64BytesWithBuf(2, &key)) != nil {
			return fmt.Errorf("idx=2 still present")
		}
		return nil
	}); err != nil {
		t.Fatalf("map helpers: %v", err)
	}
}

func TestStringExt_ReopenReinsertDeletedKeysUseNewInternalOrder(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "string_reinsert_reopen.db")

	c, bolt := openBoltAndCollection[string, Rec](t, path)
	closeCurrent := func() {
		if c != nil {
			if err := c.Close(); err != nil {
				t.Fatalf("db close: %v", err)
			}
			c = nil
		}
		if bolt != nil {
			if err := bolt.Close(); err != nil {
				t.Fatalf("raw close: %v", err)
			}
			bolt = nil
		}
	}
	t.Cleanup(closeCurrent)

	order := []string{
		"k-07", "k-02", "k-09", "k-01", "k-08",
		"k-03", "k-06", "k-04", "k-05",
	}
	for _, key := range order {
		if err := writeSet(c, key, &Rec{Name: key, Age: 1}); err != nil {
			t.Fatalf("seed Set(%q): %v", key, err)
		}
	}

	deleted := map[string]struct{}{
		"k-02": {},
		"k-08": {},
		"k-04": {},
	}
	for key := range deleted {
		if err := writeDelete(c, key); err != nil {
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

	gotLive, err := readQueryKeys(c, qx.Query(qx.EQ("age", 1)))
	if err != nil {
		t.Fatalf("QueryKeys(live before reopen): %v", err)
	}
	if !slices.Equal(gotLive, liveOrder) {
		t.Fatalf("live order before reopen mismatch: got=%v want=%v", gotLive, liveOrder)
	}

	closeCurrent()

	c, bolt = openBoltAndCollection[string, Rec](t, path)

	gotLive, err = readQueryKeys(c, qx.Query(qx.EQ("age", 1)))
	if err != nil {
		t.Fatalf("QueryKeys(live after reopen): %v", err)
	}
	if !slices.Equal(gotLive, liveOrder) {
		t.Fatalf("live order after reopen mismatch: got=%v want=%v", gotLive, liveOrder)
	}

	for key := range deleted {
		v, getErr := readGet(c, key)
		if getErr != nil {
			t.Fatalf("Get(%q) after reopen: %v", key, getErr)
		}
		if v != nil {
			t.Fatalf("deleted key %q unexpectedly reappeared after reopen: %#v", key, v)
		}
	}

	reinsertOrder := []string{"k-02", "k-08", "k-04"}
	for _, key := range reinsertOrder {
		if err := writeSet(c, key, &Rec{Name: key, Age: 1}); err != nil {
			t.Fatalf("reinsert Set(%q): %v", key, err)
		}
	}

	wantReinsert := append(slices.Clone(liveOrder), reinsertOrder...)
	gotAll, err := readQueryKeys(c, qx.Query(qx.EQ("age", 1)))
	if err != nil {
		t.Fatalf("QueryKeys(all after reinsert): %v", err)
	}
	if !slices.Equal(gotAll, wantReinsert) {
		t.Fatalf("reinsert order mismatch: got=%v want=%v", gotAll, wantReinsert)
	}
}

func TestStringExt_SharedAutoBatchUniqueRejectKeepsCommittedKeysAcrossReopen(t *testing.T) {
	enableStoreStatsForTest(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "string_unique_hole.db")

	c, raw := openBoltAndCollection[string, StringUniqueTestRec](t, path)
	closeCurrent := func() {
		if c != nil {
			if err := c.Close(); err != nil {
				t.Fatalf("db close: %v", err)
			}
			c = nil
		}
		if raw != nil {
			if err := raw.Close(); err != nil {
				t.Fatalf("raw close: %v", err)
			}
			raw = nil
		}
	}
	t.Cleanup(closeCurrent)

	if err := writeSet(c, "seed", &StringUniqueTestRec{Email: "seed@x", Code: 1}); err != nil {
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
	blockCommit := OnChange(func(*Tx, string, *StringUniqueTestRec, *StringUniqueTestRec) error {
		close(entered)
		<-release
		return nil
	})

	blockerDone := make(chan error, 1)
	go func() {
		blockerDone <- writeSet(c, "blocker", &StringUniqueTestRec{Email: "blocker@x", Code: 2}, blockCommit)
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for blocked commit")
	}
	baseline := c.root.scheduler.snapshot()

	badDone := make(chan error, 1)
	go func() {
		badDone <- writeSet(c, "ghost-hole", &StringUniqueTestRec{Email: "seed@x", Code: 3})
	}()
	waitAutoBatchExtraStats(t, c.root, "queued rejected string set", func(st rootSchedulerSnapshot) bool {
		return st.Submitted == baseline.Submitted+1 && st.QueueLen == 1
	})

	goodDone := make(chan error, 1)
	go func() {
		goodDone <- writeSet(c, "real-hole", &StringUniqueTestRec{Email: "real@x", Code: 4})
	}()
	waitAutoBatchExtraStats(t, c.root, "queued rejected+accepted string sets", func(st rootSchedulerSnapshot) bool {
		return st.Submitted == baseline.Submitted+2 && st.QueueLen == 2
	})

	close(release)

	if err := <-blockerDone; err != nil {
		t.Fatalf("blocker Set: %v", err)
	}
	if err := <-badDone; !errors.Is(err, rbierrors.ErrUniqueViolation) {
		t.Fatalf("bad Set error = %v, want %v", err, rbierrors.ErrUniqueViolation)
	}
	if err := <-goodDone; err != nil {
		t.Fatalf("good Set: %v", err)
	}

	assertHole := func(label string) {
		t.Helper()

		v, getErr := readGet(c, "ghost-hole")
		if getErr != nil {
			t.Fatalf("%s: Get(ghost-hole): %v", label, getErr)
		}
		if v != nil {
			t.Fatalf("%s: rejected key persisted: %#v", label, v)
		}
		v, getErr = readGet(c, "blocker")
		if getErr != nil {
			t.Fatalf("%s: Get(blocker): %v", label, getErr)
		}
		if v == nil || v.Email != "blocker@x" || v.Code != 2 {
			t.Fatalf("%s: blocker payload mismatch: %#v", label, v)
		}

		v, getErr = readGet(c, "real-hole")
		if getErr != nil {
			t.Fatalf("%s: Get(real-hole): %v", label, getErr)
		}
		if v == nil || v.Email != "real@x" || v.Code != 4 {
			t.Fatalf("%s: real-hole payload mismatch: %#v", label, v)
		}

		allQ := qx.Query()
		gotAll, queryErr := readQueryKeys(c, allQ)
		if queryErr != nil {
			t.Fatalf("%s: QueryKeys(NOOP): %v", label, queryErr)
		}
		if !queryStringIDsEqual(allQ, gotAll, []string{"seed", "blocker", "real-hole"}) {
			t.Fatalf("%s: NOOP query mismatch: got=%v want=[seed blocker real-hole]", label, gotAll)
		}

		gotReal, queryErr := readQueryKeys(c, qx.Query(qx.EQ("email", "real@x")))
		if queryErr != nil {
			t.Fatalf("%s: QueryKeys(real email): %v", label, queryErr)
		}
		if !slices.Equal(gotReal, []string{"real-hole"}) {
			t.Fatalf("%s: real email query mismatch: got=%v want=[real-hole]", label, gotReal)
		}
	}

	assertHole("live")

	closeCurrent()

	c, raw = openBoltAndCollection[string, StringUniqueTestRec](t, path)

	assertHole("reopen")
}

func TestStringExt_TruncateRecreatesEmptyStringMap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "string_truncate_map.db")

	c, bolt := openBoltAndCollection[string, Rec](t, path)
	defer func() {
		_ = c.Close()
		_ = bolt.Close()
	}()

	if err := writeSet(c, "a", &Rec{Name: "a"}); err != nil {
		t.Fatalf("Set(a): %v", err)
	}
	if err := writeSet(c, "b", &Rec{Name: "b"}); err != nil {
		t.Fatalf("Set(b): %v", err)
	}

	if err := c.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	if err := bolt.View(func(tx *bbolt.Tx) error {
		m := tx.Bucket(c.strmapBucket)
		if m == nil {
			return fmt.Errorf("string map bucket missing")
		}
		if seq := m.Sequence(); seq != 0 {
			return fmt.Errorf("string map sequence=%d want 0", seq)
		}
		k, v := m.Cursor().First()
		if k != nil || v != nil {
			return fmt.Errorf("string map retained entry key=%x value=%q", k, v)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify empty string map: %v", err)
	}

	if err := writeSet(c, "c", &Rec{Name: "c"}); err != nil {
		t.Fatalf("Set(c): %v", err)
	}

	if err := bolt.View(func(tx *bbolt.Tx) error {
		m := tx.Bucket(c.strmapBucket)
		if m == nil {
			return fmt.Errorf("string map bucket missing")
		}
		var mapKey [8]byte
		if got := m.Get(keycodec.U64BytesWithBuf(1, &mapKey)); !slices.Equal(got, []byte("c")) {
			return fmt.Errorf("map[1]=%q want c", got)
		}
		v := tx.Bucket(c.dataBucket).Get([]byte("c"))
		idx := keycodec.U64FromBytes(v[:8])
		if idx != 1 {
			return fmt.Errorf("idx after truncate=%d want 1", idx)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify reused id after truncate: %v", err)
	}
}

func TestStringExt_UniqueRejectDoesNotLeakRejectedStringKey(t *testing.T) {
	c, _ := openTempDBStringUnique(t)

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
		if err := writeSet(c, item.key, &StringUniqueTestRec{Email: item.email, Code: item.code}); err != nil {
			t.Fatalf("seed Set(%q): %v", item.key, err)
		}
	}

	err := writeSet(c, "ghost-dup", &StringUniqueTestRec{Email: "a@x", Code: 99})
	if !errors.Is(err, rbierrors.ErrUniqueViolation) {
		t.Fatalf("duplicate Set error = %v, want %v", err, rbierrors.ErrUniqueViolation)
	}
	if got, getErr := readGet(c, "ghost-dup"); getErr != nil || got != nil {
		t.Fatalf("rejected key lookup = %#v/%v, want nil/<nil>", got, getErr)
	}

	if err := writeSet(c, "real-ok", &StringUniqueTestRec{Email: "real@x", Code: 100}); err != nil {
		t.Fatalf("good Set: %v", err)
	}

	gotAll, err := readQueryKeys(c, qx.Query())
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	wantAll := []string{"seed-a", "seed-b", "seed-c", "real-ok"}
	if !queryStringIDsEqual(qx.Query(), gotAll, wantAll) {
		t.Fatalf("query set mismatch: got=%v want=%v", gotAll, wantAll)
	}
}

func TestStringExt_PublishedReadPagesPreserveQuerySetAndScanKeys(t *testing.T) {
	c, _ := openTempStringCollection(t)

	const n = 320
	want := make([]string, 0, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%03d", i)
		want = append(want, key)
		if err := writeSet(c, key, &Rec{Name: key, Age: i}); err != nil {
			t.Fatalf("Set(%q): %v", key, err)
		}
	}

	allQ := qx.Query()
	gotKeys, err := readQueryKeys(c, allQ)
	if err != nil {
		t.Fatalf("QueryKeys(NOOP): %v", err)
	}
	if !queryStringIDsEqual(allQ, gotKeys, want) {
		t.Fatalf("query set mismatch: got=%v want=%v", gotKeys[:min(len(gotKeys), 8)], want[:8])
	}

	var scanned []string
	err = readScanKeys(c, "", func(id string) (bool, error) {
		scanned = append(scanned, id)
		return true, nil
	})
	if err != nil {
		t.Fatalf("ScanKeys: %v", err)
	}
	if !slices.Equal(scanned, want) {
		t.Fatalf("scan set mismatch: got=%v want=%v", scanned[:min(len(scanned), 8)], want[:8])
	}
}

func TestStringExt_QueryStaysConsistentDuringDeleteReinsertChurn(t *testing.T) {
	c, _ := openTempStringCollection(t)

	const (
		keySpace  = 32
		writers   = 4
		readers   = 4
		writerOps = 180
		readerOps = 220
	)

	for i := 0; i < keySpace; i++ {
		key := fmt.Sprintf("key-%02d", i)
		if err := writeSet(c, key, &Rec{
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
				if err := writeDelete(c, key); err != nil {
					errCh <- fmt.Errorf("writer=%d Delete(%q): %w", worker, key, err)
					return
				}
				if err := writeSet(c, key, &Rec{
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
				values, err := readQuery(c, qx.Query())
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

	gotAll, err := readQueryKeys(c, qx.Query())
	if err != nil {
		t.Fatalf("final QueryKeys(NOOP): %v", err)
	}
	if len(gotAll) != keySpace {
		t.Fatalf("final key count mismatch: got=%d want=%d", len(gotAll), keySpace)
	}
	for _, key := range gotAll {
		v, getErr := readGet(c, key)
		if getErr != nil {
			t.Fatalf("final Get(%q): %v", key, getErr)
		}
		if v == nil || v.Name != key {
			t.Fatalf("final value mismatch for %q: %#v", key, v)
		}
	}
}

func TestStringKeys_ExoticCharacters(t *testing.T) {
	c, _ := openTempStringCollection(t)

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
		if err := writeSet(c, k, rec); err != nil {
			t.Fatalf("Set(%q) failed: %v", k, err)
		}
	}

	for i, k := range keys {
		v, err := readGet(c, k)
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

		// validate index lookup through durable string ids
		q := qx.Query(qx.EQ("name", expectedName))
		ids, err := readQueryKeys(c, q)
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

	c, raw := openBoltAndCollection[string, Rec](t, path)

	if err := writeSet(c, "alice", &Rec{Age: 20}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := writeSet(c, "bob", &Rec{Age: 30}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// force index and string map persistence
	if err := c.Close(); err != nil {
		t.Fatalf("db close: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}

	// reopen
	db2, raw2 := openBoltAndCollection[string, Rec](t, path)
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
	ids, err := readQueryKeys(db2, q)
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
	if err = writeSet(db2, "charlie", &Rec{Age: 40}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	ids, _ = readQueryKeys(db2, qx.Query(qx.EQ("age", 40)))
	if len(ids) != 1 || ids[0] != "charlie" {
		t.Errorf("Insert after reopen failed: %v", ids)
	}
}

func TestStringKeys_StartupBuild_FromScratch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "string_keys_rebuild.db")

	c, raw := openBoltAndCollection[string, Rec](t, path)

	keys := []string{"k1", "k2", "k3"}
	for i, k := range keys {
		if err := writeSet(c, k, &Rec{Age: i * 10}); err != nil {
			t.Fatalf("Set (%q): %v", k, err)
		}
	}
	if err := c.Close(); err != nil {
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
	db2, raw2 := openBoltAndCollection[string, Rec](t, path)
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
		ids, err := readQueryKeys(db2, qx.Query(qx.EQ("age", age)))
		if err != nil {
			t.Fatalf("QueryKeys: %v", err)
		}
		if len(ids) != 1 || ids[0] != k {
			t.Errorf("startup build mismatch for key %q: %v", k, ids)
		}
	}
}

func TestStringKeys_Concurrency_MapperStress(t *testing.T) {
	c, _ := openTempStringCollection(t)

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

				if err := writeSet(c, key, rec); err != nil {
					t.Errorf("Set failed: %v", err)
					return
				}

				// immediate read to stress RLock paths
				v, err := readGet(c, key)
				if err != nil || v == nil {
					t.Errorf("Get failed: %v", err)
				}
			}
		}(i)
	}
	wg.Wait()

	// validate total count
	totalExpected := workers * writes
	count, _ := readCount(c)
	if int(count) != totalExpected {
		t.Errorf("expected %d records, got %d", totalExpected, count)
	}

	// validate index query under load
	ids, _ := readQueryKeys(c, qx.Query(qx.EQ("name", "worker")))
	if len(ids) != totalExpected {
		t.Errorf("expected %d keys, got %d", totalExpected, len(ids))
	}
}

func TestStringKeys_BatchMutations_ModelReplayConsistency(t *testing.T) {
	c, _ := openTempStringCollection(t)

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
		case 0: // MultiSet
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
			if err := writeSets(c, keys, vals); err != nil {
				t.Fatalf("MultiSet(step=%d): %v", i, err)
			}

		case 1: // MultiPatch
			keys := makeKeys(i*7+31, 3+(i%3))
			age := 900 + i
			active := i%2 == 0
			patch := []Field{
				{Name: "age", Value: age},
				{Name: "active", Value: active},
			}
			if err := writePatches(c, keys, patch); err != nil {
				t.Fatalf("MultiPatch(step=%d): %v", i, err)
			}
			for _, key := range keys {
				if cur, ok := model[key]; ok {
					cur.age = age
					cur.active = active
					model[key] = cur
				}
			}

		default: // MultiDelete
			keys := makeKeys(i*5+19, 1+(i%4))
			if err := writeDeletes(c, keys); err != nil {
				t.Fatalf("MultiDelete(step=%d): %v", i, err)
			}
			for _, key := range keys {
				delete(model, key)
			}
		}
	}

	count, err := readCount(c)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if int(count) != len(model) {
		t.Fatalf("count mismatch: got=%d want=%d", count, len(model))
	}

	liveNames := make(map[string]string, len(model))
	wantActive := make(map[string]struct{}, len(model))

	for key, exp := range model {
		v, err := readGet(c, key)
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

		ids, err := readQueryKeys(c, qx.Query(qx.EQ("name", exp.name)))
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

	gotActive, err := readQueryKeys(c, qx.Query(qx.EQ("active", true)))
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
		ids, err := readQueryKeys(c, qx.Query(qx.EQ("name", name)))
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
	c, _ := openTempStringCollection(t)

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
		if err := writeSet(c, key, &Rec{
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

				if _, err := readQueryKeys(c, q); err != nil {
					errCh <- fmt.Errorf("reader=%d QueryKeys: %w", readerID, err)
					return
				}
				if _, err := readQuery(c, q); err != nil {
					errCh <- fmt.Errorf("reader=%d Query: %w", readerID, err)
					return
				}
				if _, err := readCount(c, q.Filter); err != nil {
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
					if err := writeSet(c, key, &Rec{
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
					if err := writePatches(c, []string{key}, patch); err != nil {
						errCh <- fmt.Errorf("writer=%d MultiPatch(%q): %w", w, key, err)
						return
					}

				default:
					if err := writeDelete(c, key); err != nil {
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

	err := readSeqScan(c, "", func(key string, rec *Rec) (bool, error) {
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

	count, err := readCount(c)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if int(count) != len(live) {
		t.Fatalf("count mismatch after concurrent ops: got=%d want=%d", count, len(live))
	}

	liveByName := make(map[string]string, len(live))
	for _, rec := range live {
		ids, err := readQueryKeys(c, qx.Query(qx.EQ("name", rec.name)))
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
		ids, err := readQueryKeys(c, qx.Query(qx.EQ("name", name)))
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
	c, _ := openTempStringCollection(t)

	inputs := []string{"b", "a", "d", "c"}
	for _, k := range inputs {
		err := writeSet(c, k, &Rec{Age: 1})
		if err != nil {
			t.Fatalf("Set (%q): %v", k, err)
		}
	}

	// returns results ordered by internal id (insertion order)
	ids, err := readQueryKeys(c, qx.Query(qx.EQ("age", 1)))
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
	c, _ := openTempStringCollection(t)

	longKey := strings.Repeat("A", 4096)

	err := writeSet(c, longKey, &Rec{Name: "long"})
	if err != nil {
		t.Fatalf("Set: %v", err)
	}

	v, err := readGet(c, longKey)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if v == nil {
		t.Fatalf("get failed: %v", v)
	}
	if v.Name != "long" {
		t.Errorf("value mismatch: %v", v.Name)
	}

	ids, err := readQueryKeys(c, qx.Query(qx.EQ("name", "long")))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) != 1 || ids[0] != longKey {
		t.Errorf("index lookup failed")
	}
}

func TestStringKeys_QueryResultKey_RemainsValidAfterClose(t *testing.T) {
	c, _ := openTempStringCollection(t)
	key := "my-key"
	if err := writeSet(c, key, &Rec{}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	ids, err := readQueryKeys(c, qx.Query())
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	resultKey := ids[0]

	// close to invalidate mmap regions
	if err = c.Close(); err != nil {
		t.Fatalf("db close: %v", err)
	}

	// force string access
	s := strings.Clone(resultKey)
	if s != key {
		t.Errorf("key corrupted after DB close: %q", s)
	}
}

func TestMissingIDs_DoNotCreateStringRecords(t *testing.T) {
	c, _ := openTempCollectionStringProduct(t)

	if err := writeSet(c, "p1", &Product{SKU: "p1", Price: 10}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := writePatch(c, "missing", []Field{{Name: "price", Value: 1.0}}); err != nil {
		t.Fatalf("Patch(missing): %v", err)
	}
	if err := writeDelete(c, "missing"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := writeDeletes(c, []string{"missing2", "missing3"}); err != nil {
		t.Fatalf("MultiDelete: %v", err)
	}
	if err := writePatches(c, []string{"missing4"}, []Field{{Name: "price", Value: 2.0}}); err != nil {
		t.Fatalf("MultiPatch: %v", err)
	}

	got, err := readValues(c, "missing", "missing2", "missing3", "missing4")
	if err != nil {
		t.Fatalf("readValues(missing ids): %v", err)
	}
	for i := range got {
		if got[i] != nil {
			t.Fatalf("missing id %d unexpectedly persisted: %#v", i, got[i])
		}
	}
}

func TestFailedSetPaths_DoNotPersistStringKeys(t *testing.T) {
	c, _ := openTempCollectionStringProduct(t)

	if err := writeSet(c, "p1", &Product{SKU: "p1", Price: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	cbErr := errors.New("on change fail")
	cb := func(_ *Tx, _ string, _ *Product, _ *Product) error { return cbErr }

	if err := writeSet(c, "ghost-set", &Product{SKU: "ghost-set", Price: 11}, OnChange(cb)); !errors.Is(err, cbErr) {
		t.Fatalf("Set callback error mismatch: %v", err)
	}
	if v, err := readGet(c, "ghost-set"); err != nil {
		t.Fatalf("Get(ghost-set): %v", err)
	} else if v != nil {
		t.Fatalf("ghost-set should not persist after rollback, got %#v", v)
	}

	err := writeSets(c,
		[]string{"ghost-many-1", "ghost-many-2"},
		[]*Product{
			{SKU: "ghost-many-1", Price: 12},
			{SKU: "ghost-many-2", Price: 13},
		},
		OnChange(cb),
	)
	if !errors.Is(err, cbErr) {
		t.Fatalf("MultiSet callback error mismatch: %v", err)
	}
	if v, err := readGet(c, "ghost-many-1"); err != nil {
		t.Fatalf("Get(ghost-many-1): %v", err)
	} else if v != nil {
		t.Fatalf("ghost-many-1 should not persist after rollback, got %#v", v)
	}
	if v, err := readGet(c, "ghost-many-2"); err != nil {
		t.Fatalf("Get(ghost-many-2): %v", err)
	} else if v != nil {
		t.Fatalf("ghost-many-2 should not persist after rollback, got %#v", v)
	}
}

func TestMultiSet_CallbackError_DoesNotPersistStringKey(t *testing.T) {
	enableStoreStatsForTest(t)

	c, _ := openTempCollectionStringProduct(t)

	if err := writeSet(c, "p1", &Product{SKU: "p1", Price: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	cbErr := errors.New("cb fail")
	err := writeSet(c, "ghost-cb", &Product{SKU: "ghost-cb", Price: 11}, OnChange(func(_ *Tx, _ string, _ *Product, _ *Product) error {
		return cbErr
	}))
	if !errors.Is(err, cbErr) {
		t.Fatalf("Set callback error mismatch: %v", err)
	}
	if v, err := readGet(c, "ghost-cb"); err != nil {
		t.Fatalf("Get(ghost-cb): %v", err)
	} else if v != nil {
		t.Fatalf("ghost-cb should not persist after rollback, got %#v", v)
	}

	bs := c.StoreStats()
	if bs.CallbackOps == 0 || bs.CallbackErrors == 0 {
		t.Fatalf("expected callback error via batch path, stats=%+v", bs)
	}
}

func TestMultiSet_UniqueReject_DoesNotPersistStringKey(t *testing.T) {
	enableStoreStatsForTest(t)

	c, _ := openTempDBStringUnique(t)

	if err := writeSet(c, "u1", &StringUniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	err := writeSet(c, "u-dup", &StringUniqueTestRec{Email: "a@x", Code: 2})
	if err == nil || !errors.Is(err, rbierrors.ErrUniqueViolation) {
		t.Fatalf("expected rbierrors.ErrUniqueViolation, got: %v", err)
	}
	if v, err := readGet(c, "u-dup"); err != nil {
		t.Fatalf("Get(u-dup): %v", err)
	} else if v != nil {
		t.Fatalf("u-dup should not persist after unique reject, got %#v", v)
	}

	bs := c.StoreStats()
	if bs.UniqueRejected == 0 {
		t.Fatalf("expected unique rejection in batch stats, got %+v", bs)
	}
}

type StringUniqueTestRec struct {
	Email string `db:"email" rbi:"unique"`
	Code  int    `db:"code"  rbi:"unique"`
}

func openTempDBStringUnique(t *testing.T, options ...Options) (*Collection[string, StringUniqueTestRec], string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_string_unique.db")
	c, bolt := openBoltAndCollection[string, StringUniqueTestRec](t, path, options...)
	t.Cleanup(func() {
		_ = c.Close()
		_ = bolt.Close()
	})
	return c, path
}
