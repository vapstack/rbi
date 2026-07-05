package rbi

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/keycodec"
	"go.etcd.io/bbolt"
)

func TestTransparentMode_IgnoresPersistedIndexAndDoesNotStoreSidecar(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "transparent_ignored_sidecar.db")
	sidecar := path + ".noIndexRec.rbi"
	if err := os.WriteFile(sidecar, []byte("invalid-sidecar"), 0o600); err != nil {
		t.Fatalf("write invalid sidecar: %v", err)
	}

	var logBuf bytes.Buffer
	c, bolt := openBoltAndCollection[uint64, noIndexRec](t, path, Options{
		Logger: log.New(&logBuf, "", 0),
	})
	defer func() {
		if c != nil {
			_ = c.Close()
		}
		if bolt != nil {
			_ = bolt.Close()
		}
	}()

	if err := writeSet(c, 1, &noIndexRec{Name: "one", Age: 10}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := writeSet(c, 2, &noIndexRec{Name: "two", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	if strings.Contains(logBuf.String(), "persisted index") {
		t.Fatalf("transparent mode must ignore sidecar load/store logs, got %q", logBuf.String())
	}

	if got, err := readGet(c, 2); err != nil {
		t.Fatalf("Get(2): %v", err)
	} else if got == nil || got.Name != "two" || got.Age != 20 {
		t.Fatalf("Get(2)=%#v", got)
	}

	if err := writePatch(c, 2, []Field{{Name: "Age", Value: 21}}); err != nil {
		t.Fatalf("Patch(2): %v", err)
	}
	if got, err := readGet(c, 2); err != nil {
		t.Fatalf("Get(2 after patch): %v", err)
	} else if got == nil || got.Age != 21 {
		t.Fatalf("patched Get(2)=%#v", got)
	}
	if err := writeDelete(c, 1); err != nil {
		t.Fatalf("Delete(1): %v", err)
	}
	if got, err := readGet(c, 1); err != nil {
		t.Fatalf("Get(1 after delete): %v", err)
	} else if got != nil {
		t.Fatalf("Get(1)=%#v want nil", got)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	c = nil
	if err := bolt.Close(); err != nil {
		t.Fatalf("bolt close: %v", err)
	}
	bolt = nil

	data, err := os.ReadFile(sidecar)
	if err != nil {
		t.Fatalf("read sidecar: %v", err)
	}
	if got := string(data); got != "invalid-sidecar" {
		t.Fatalf("transparent close rewrote sidecar: %q", got)
	}
	if strings.Contains(logBuf.String(), "persisted index") {
		t.Fatalf("transparent close must not emit sidecar logs, got %q", logBuf.String())
	}
}

func TestTransparentMode_WritesAdvanceBucketSequenceAndInvalidateStaleSidecar(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "transparent_write_seq.db")
	opts := Options{BucketName: "transparent_write_seq"}

	cIndexed, boltIndexed := openBoltAndCollection[uint64, schemaSubsetRec](t, path, opts)
	if err := writeSet(cIndexed, 1, &schemaSubsetRec{Name: "one", Age: 10}); err != nil {
		t.Fatalf("indexed Set(1): %v", err)
	}
	if err := writeSet(cIndexed, 2, &schemaSubsetRec{Name: "two", Age: 20}); err != nil {
		t.Fatalf("indexed Set(2): %v", err)
	}
	sidecar := cIndexed.rbiFile
	if err := cIndexed.Close(); err != nil {
		t.Fatalf("indexed Close: %v", err)
	}
	if err := boltIndexed.Close(); err != nil {
		t.Fatalf("indexed bolt Close: %v", err)
	}

	storedSeq := readPersistedIndexSequence(t, sidecar)

	cTransparent, boltTransparent := openBoltAndCollection[uint64, noIndexRec](t, path, opts)
	if err := writeDelete(cTransparent, 1); err != nil {
		t.Fatalf("transparent Delete(1): %v", err)
	}
	if err := writeSet(cTransparent, 3, &noIndexRec{Name: "three", Age: 30}); err != nil {
		t.Fatalf("transparent Set(3): %v", err)
	}
	if err := cTransparent.Close(); err != nil {
		t.Fatalf("transparent Close: %v", err)
	}
	if err := boltTransparent.Close(); err != nil {
		t.Fatalf("transparent bolt Close: %v", err)
	}

	if got := readPersistedIndexSequence(t, sidecar); got != storedSeq {
		t.Fatalf("transparent mode rewrote sidecar sequence: got=%d want=%d", got, storedSeq)
	}

	cReopen, boltReopen := openBoltAndCollection[uint64, schemaSubsetRec](t, path, opts)
	defer func() {
		_ = cReopen.Close()
		_ = boltReopen.Close()
	}()

	currentSeq := readBucketSequence(t, boltReopen, cReopen.dataBucket)
	if currentSeq <= storedSeq {
		t.Fatalf("bucket sequence did not advance across transparent writes: current=%d stored=%d", currentSeq, storedSeq)
	}

	allQ := qx.Query()
	keys, err := readQueryKeys(cReopen, allQ)
	if err != nil {
		t.Fatalf("reopen QueryKeys(all): %v", err)
	}
	if !queryIDsEqual(allQ, keys, []uint64{2, 3}) {
		t.Fatalf("reopen QueryKeys(all)=%v want [2 3]", keys)
	}
}

func TestTransparentMode_TruncateAdvancesBucketSequenceAndInvalidatesStaleSidecar(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "transparent_truncate_seq.db")
	opts := Options{BucketName: "transparent_truncate_seq"}

	cIndexed, rawIndexed := openBoltAndCollection[uint64, schemaSubsetRec](t, path, opts)
	if err := writeSet(cIndexed, 1, &schemaSubsetRec{Name: "one", Age: 10}); err != nil {
		t.Fatalf("indexed Set(1): %v", err)
	}
	if err := writeSet(cIndexed, 2, &schemaSubsetRec{Name: "two", Age: 20}); err != nil {
		t.Fatalf("indexed Set(2): %v", err)
	}
	sidecar := cIndexed.rbiFile
	if err := cIndexed.Close(); err != nil {
		t.Fatalf("indexed Close: %v", err)
	}
	if err := rawIndexed.Close(); err != nil {
		t.Fatalf("indexed raw Close: %v", err)
	}

	storedSeq := readPersistedIndexSequence(t, sidecar)

	dbTransparent, rawTransparent := openBoltAndCollection[uint64, noIndexRec](t, path, opts)
	if err := dbTransparent.Truncate(); err != nil {
		t.Fatalf("transparent Truncate: %v", err)
	}
	if err := dbTransparent.Close(); err != nil {
		t.Fatalf("transparent Close: %v", err)
	}
	if err := rawTransparent.Close(); err != nil {
		t.Fatalf("transparent raw Close: %v", err)
	}

	if got := readPersistedIndexSequence(t, sidecar); got != storedSeq {
		t.Fatalf("transparent mode rewrote sidecar sequence: got=%d want=%d", got, storedSeq)
	}

	dbReopen, rawReopen := openBoltAndCollection[uint64, schemaSubsetRec](t, path, opts)
	defer func() {
		_ = dbReopen.Close()
		_ = rawReopen.Close()
	}()

	currentSeq := readBucketSequence(t, rawReopen, dbReopen.dataBucket)
	if currentSeq <= storedSeq {
		t.Fatalf("bucket sequence did not advance across transparent truncate: current=%d stored=%d", currentSeq, storedSeq)
	}

	if cnt, err := readCount(dbReopen); err != nil {
		t.Fatalf("reopen Count(): %v", err)
	} else if cnt != 0 {
		t.Fatalf("reopen Count()=%d want 0", cnt)
	}

	keys, err := readQueryKeys(dbReopen, qx.Query())
	if err != nil {
		t.Fatalf("reopen QueryKeys(all): %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("reopen QueryKeys(all)=%v want empty", keys)
	}
}

func TestIndexPersistence_RejectsSidecarAfterBoltReplacementSameSequence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "replace_same_seq.db")
	opts := Options{BucketName: "replace_same_seq"}

	c1, raw1 := openBoltAndCollection[uint64, schemaSubsetRec](t, path, opts)
	if err := writeSet(c1, 1, &schemaSubsetRec{Name: "old", Age: 10}); err != nil {
		t.Fatalf("old Set: %v", err)
	}
	sidecar := c1.rbiFile
	oldSeq := readBucketSequence(t, raw1, c1.dataBucket)
	if err := c1.Close(); err != nil {
		t.Fatalf("old Close: %v", err)
	}
	if err := raw1.Close(); err != nil {
		t.Fatalf("old raw Close: %v", err)
	}

	if err := os.Remove(path); err != nil {
		t.Fatalf("remove old bolt: %v", err)
	}

	c2, bolt2 := openBoltAndCollection[uint64, schemaSubsetRec](t, path, Options{
		BucketName:        opts.BucketName,
		DisableIndexLoad:  true,
		DisableIndexStore: true,
	})
	if err := writeSet(c2, 1, &schemaSubsetRec{Name: "new", Age: 20}); err != nil {
		t.Fatalf("new Set: %v", err)
	}
	newSeq := readBucketSequence(t, bolt2, c2.dataBucket)
	if newSeq != oldSeq {
		t.Fatalf("test requires matching sequence: old=%d new=%d", oldSeq, newSeq)
	}
	if err := c2.Close(); err != nil {
		t.Fatalf("new Close: %v", err)
	}
	if err := bolt2.Close(); err != nil {
		t.Fatalf("new raw Close: %v", err)
	}
	if got := readPersistedIndexSequence(t, sidecar); got != oldSeq {
		t.Fatalf("old sidecar sequence changed: got=%d want %d", got, oldSeq)
	}

	var logBuf bytes.Buffer
	db3, raw3 := openBoltAndCollection[uint64, schemaSubsetRec](t, path, Options{
		BucketName: opts.BucketName,
		Logger:     log.New(&logBuf, "", 0),
	})
	defer func() {
		_ = db3.Close()
		_ = raw3.Close()
	}()

	if gotLog := logBuf.String(); !strings.Contains(gotLog, "bucket id mismatch") {
		t.Fatalf("expected stale sidecar id mismatch log, got %q", gotLog)
	}
	if cnt, err := readCount(db3, qx.EQ("age", 10)); err != nil {
		t.Fatalf("Count(old age): %v", err)
	} else if cnt != 0 {
		t.Fatalf("Count(old age)=%d want 0", cnt)
	}
	ids, err := readQueryKeys(db3, qx.Query(qx.EQ("age", 20)))
	if err != nil {
		t.Fatalf("QueryKeys(new age): %v", err)
	}
	if !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("QueryKeys(new age)=%v want [1]", ids)
	}
}

func TestIndexPersistence_RegeneratesUIDAfterDataBucketRecreateSameSequence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "recreate_bucket_same_seq.db")
	opts := Options{BucketName: "recreate_bucket_same_seq"}

	c1, raw1 := openBoltAndCollection[uint64, schemaSubsetRec](t, path, opts)
	if err := writeSet(c1, 1, &schemaSubsetRec{Name: "old", Age: 10}); err != nil {
		t.Fatalf("old Set: %v", err)
	}
	sidecar := c1.rbiFile
	oldSeq := readBucketSequence(t, raw1, c1.dataBucket)
	if err := c1.Close(); err != nil {
		t.Fatalf("old Close: %v", err)
	}
	if err := raw1.Close(); err != nil {
		t.Fatalf("old raw Close: %v", err)
	}

	rawDelete, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("delete reopen bbolt.Open: %v", err)
	}
	if err = rawDelete.Update(func(tx *bbolt.Tx) error {
		return tx.DeleteBucket([]byte(opts.BucketName))
	}); err != nil {
		_ = rawDelete.Close()
		t.Fatalf("DeleteBucket: %v", err)
	}
	if err = rawDelete.Close(); err != nil {
		t.Fatalf("delete raw Close: %v", err)
	}

	c2, bolt2 := openBoltAndCollection[uint64, schemaSubsetRec](t, path, Options{
		BucketName:        opts.BucketName,
		DisableIndexLoad:  true,
		DisableIndexStore: true,
	})
	if err = writeSet(c2, 1, &schemaSubsetRec{Name: "new", Age: 20}); err != nil {
		t.Fatalf("new Set: %v", err)
	}
	newSeq := readBucketSequence(t, bolt2, c2.dataBucket)
	if newSeq != oldSeq {
		t.Fatalf("test requires matching sequence: old=%d new=%d", oldSeq, newSeq)
	}
	if err := c2.Close(); err != nil {
		t.Fatalf("new Close: %v", err)
	}
	if err := bolt2.Close(); err != nil {
		t.Fatalf("new raw Close: %v", err)
	}
	if got := readPersistedIndexSequence(t, sidecar); got != oldSeq {
		t.Fatalf("old sidecar sequence changed: got=%d want %d", got, oldSeq)
	}

	var logBuf bytes.Buffer
	db3, raw3 := openBoltAndCollection[uint64, schemaSubsetRec](t, path, Options{
		BucketName: opts.BucketName,
		Logger:     log.New(&logBuf, "", 0),
	})
	defer func() {
		_ = db3.Close()
		_ = raw3.Close()
	}()

	if gotLog := logBuf.String(); !strings.Contains(gotLog, "bucket id mismatch") {
		t.Fatalf("expected stale sidecar id mismatch log, got %q", gotLog)
	}
	if cnt, err := readCount(db3, qx.EQ("age", 10)); err != nil {
		t.Fatalf("Count(old age): %v", err)
	} else if cnt != 0 {
		t.Fatalf("Count(old age)=%d want 0", cnt)
	}
	ids, err := readQueryKeys(db3, qx.Query(qx.EQ("age", 20)))
	if err != nil {
		t.Fatalf("QueryKeys(new age): %v", err)
	}
	if !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("QueryKeys(new age)=%v want [1]", ids)
	}
}

func TestIndexPersistence_RejectsPersistedIndexPathReusedForAnotherBucketSameSequence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "reuse_sidecar.db")
	sidecar := filepath.Join(dir, "shared.rbi")

	dbA, rawA := openBoltAndCollection[uint64, schemaSubsetRec](t, path, Options{
		BucketName:         "reuse_sidecar_a",
		PersistedIndexPath: sidecar,
	})
	if err := writeSet(dbA, 1, &schemaSubsetRec{Name: "a", Age: 10}); err != nil {
		t.Fatalf("Set bucket A: %v", err)
	}
	seqA := readBucketSequence(t, rawA, dbA.dataBucket)
	if err := dbA.Close(); err != nil {
		t.Fatalf("Close bucket A: %v", err)
	}
	if err := rawA.Close(); err != nil {
		t.Fatalf("raw Close bucket A: %v", err)
	}

	dbB, rawB := openBoltAndCollection[uint64, schemaSubsetRec](t, path, Options{
		BucketName:         "reuse_sidecar_b",
		PersistedIndexPath: sidecar,
		DisableIndexLoad:   true,
		DisableIndexStore:  true,
	})
	if err := writeSet(dbB, 1, &schemaSubsetRec{Name: "b", Age: 20}); err != nil {
		t.Fatalf("Set bucket B: %v", err)
	}
	seqB := readBucketSequence(t, rawB, dbB.dataBucket)
	if seqB != seqA {
		t.Fatalf("test requires matching sequence: bucketA=%d bucketB=%d", seqA, seqB)
	}
	if err := dbB.Close(); err != nil {
		t.Fatalf("Close bucket B seed: %v", err)
	}
	if err := rawB.Close(); err != nil {
		t.Fatalf("raw Close bucket B seed: %v", err)
	}

	var logBuf bytes.Buffer
	dbB2, rawB2 := openBoltAndCollection[uint64, schemaSubsetRec](t, path, Options{
		BucketName:         "reuse_sidecar_b",
		PersistedIndexPath: sidecar,
		Logger:             log.New(&logBuf, "", 0),
	})
	defer func() {
		_ = dbB2.Close()
		_ = rawB2.Close()
	}()

	if gotLog := logBuf.String(); !strings.Contains(gotLog, "bucket id mismatch") {
		t.Fatalf("expected stale sidecar id mismatch log, got %q", gotLog)
	}
	if cnt, err := readCount(dbB2, qx.EQ("age", 10)); err != nil {
		t.Fatalf("Count(bucket A age): %v", err)
	} else if cnt != 0 {
		t.Fatalf("Count(bucket A age)=%d want 0", cnt)
	}
	ids, err := readQueryKeys(dbB2, qx.Query(qx.EQ("age", 20)))
	if err != nil {
		t.Fatalf("QueryKeys(bucket B age): %v", err)
	}
	if !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("QueryKeys(bucket B age)=%v want [1]", ids)
	}
}

func TestWrap_CorruptedPersistedIndex_RebuildsInsteadOfPanicking(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corrupted_index.db")

	bolt, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}

	c, err := Open[uint64, Rec](bolt, testOptions(Options{}))
	if err != nil {
		t.Fatalf("initial New: %v", err)
	}
	rbiPath := c.rbiFile

	if err = writeSet(c, 1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	if err = c.Close(); err != nil {
		t.Fatalf("initial Close: %v", err)
	}
	seq := readBucketSequence(t, bolt, c.dataBucket)
	if err = bolt.Close(); err != nil {
		t.Fatalf("initial raw Close: %v", err)
	}

	var enc [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(enc[:], seq)
	corrupted := append([]byte{'R', 'B', 'I', readPersistedIndexFormatByte(t, rbiPath)}, enc[:n]...)
	corrupted = append(corrupted, 0xff, 0xff, 0xff, 0xff)
	if err = os.WriteFile(rbiPath, corrupted, 0o600); err != nil {
		t.Fatalf("corrupt .rbi: %v", err)
	}

	bolt2, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("reopen bbolt.Open: %v", err)
	}
	defer func() { _ = bolt2.Close() }()

	var logBuf bytes.Buffer

	var c2 *Collection[uint64, Rec]
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("reopen New panicked on corrupted persisted index: %v", r)
			}
		}()
		c2, err = Open[uint64, Rec](bolt2, Options{
			Logger: log.New(&logBuf, "", 0),
		})
	}()
	if err != nil {
		t.Fatalf("reopen New: %v", err)
	}
	defer func() { _ = c2.Close() }()
	gotLog := logBuf.String()
	if !strings.Contains(gotLog, "persisted index unavailable") {
		t.Fatalf("expected corrupted persisted index reason in log, got: %q", gotLog)
	}
	if !strings.Contains(gotLog, "persisted index is invalid") {
		t.Fatalf("expected corrupted persisted index invalid marker in log, got: %q", gotLog)
	}
	if !strings.Contains(gotLog, "persisted index file=") {
		t.Fatalf("expected corrupted persisted index log to include file context, got: %q", gotLog)
	}
	if !strings.Contains(gotLog, "bucket=\"Rec\"") {
		t.Fatalf("expected corrupted persisted index log to include bucket context, got: %q", gotLog)
	}
	if !strings.Contains(gotLog, "stage=load_index") {
		t.Fatalf("expected corrupted persisted index log to include load stage, got: %q", gotLog)
	}
	if !strings.Contains(gotLog, "version=") {
		t.Fatalf("expected corrupted persisted index log to include format version, got: %q", gotLog)
	}
	if !strings.Contains(gotLog, "rbi: rebuilding index from bbolt") {
		t.Fatalf("expected rebuild start in log, got: %q", gotLog)
	}
	if !strings.Contains(gotLog, "mode=full") {
		t.Fatalf("expected full rebuild mode in log, got: %q", gotLog)
	}
	if !strings.Contains(gotLog, "rbi: index build completed (mode=full duration=") {
		t.Fatalf("expected full rebuild completion duration in log, got: %q", gotLog)
	}

	got, err := readGet(c2, 1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil || got.Name != "alice" || got.Age != 30 {
		t.Fatalf("expected rebuilt record after corrupted persisted index, got %#v", got)
	}
}

func TestWrap_MissingPersistedIndex_LogsFullRebuildReason(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "missing_index.db")

	bolt, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}

	c, err := Open[uint64, Rec](bolt, testOptions(Options{}))
	if err != nil {
		t.Fatalf("initial New: %v", err)
	}
	rbiPath := c.rbiFile

	if err = writeSet(c, 1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	if err = c.Close(); err != nil {
		t.Fatalf("initial Close: %v", err)
	}
	if err = bolt.Close(); err != nil {
		t.Fatalf("initial raw Close: %v", err)
	}
	if err = os.Remove(rbiPath); err != nil {
		t.Fatalf("remove .rbi: %v", err)
	}

	bolt2, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("reopen bbolt.Open: %v", err)
	}
	defer func() { _ = bolt2.Close() }()

	var logBuf bytes.Buffer

	c2, err := Open[uint64, Rec](bolt2, Options{
		Logger: log.New(&logBuf, "", 0),
	})
	if err != nil {
		t.Fatalf("reopen New: %v", err)
	}
	defer func() { _ = c2.Close() }()

	gotLog := logBuf.String()
	if !strings.Contains(gotLog, "persisted index missing") {
		t.Fatalf("expected missing persisted index reason in log, got: %q", gotLog)
	}
	if !strings.Contains(gotLog, fmt.Sprintf("file=%q", rbiPath)) {
		t.Fatalf("expected missing persisted index log to include file path, got: %q", gotLog)
	}
	if !strings.Contains(gotLog, "rbi: rebuilding index from bbolt") {
		t.Fatalf("expected full rebuild start in log, got: %q", gotLog)
	}
	if !strings.Contains(gotLog, "mode=full") {
		t.Fatalf("expected full rebuild mode in log, got: %q", gotLog)
	}
	if !strings.Contains(gotLog, "rbi: index build completed (mode=full duration=") {
		t.Fatalf("expected full rebuild completion duration in log, got: %q", gotLog)
	}

	got, err := readGet(c2, 1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil || got.Name != "alice" || got.Age != 30 {
		t.Fatalf("expected rebuilt record after missing persisted index, got %#v", got)
	}
}

func TestWrap_LoggerOptionReceivesIndexBuildLogs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "custom_logger.db")

	bolt, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}
	defer func() { _ = bolt.Close() }()

	var customLogBuf bytes.Buffer
	c, err := Open[uint64, Rec](bolt, Options{
		Logger: log.New(&customLogBuf, "", 0),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = c.Close() }()

	gotLog := customLogBuf.String()
	if !strings.Contains(gotLog, "persisted index missing") {
		t.Fatalf("expected custom logger to receive rebuild reason, got: %q", gotLog)
	}
}

func TestWrap_MissingPersistedIndex_WithIndexStoreDisabled_StillLogsRebuild(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "missing_index_store_disabled.db")

	bolt, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}
	defer func() { _ = bolt.Close() }()

	var logBuf bytes.Buffer

	c, err := Open[uint64, Rec](bolt, Options{
		DisableIndexStore: true,
		Logger:            log.New(&logBuf, "", 0),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = c.Close() }()

	gotLog := logBuf.String()
	if !strings.Contains(gotLog, "persisted index missing") {
		t.Fatalf("expected missing persisted index log with DisableIndexStore, got: %q", gotLog)
	}
	if !strings.Contains(gotLog, "rbi: rebuilding index from bbolt") {
		t.Fatalf("expected rebuild log with DisableIndexStore, got: %q", gotLog)
	}
	if !strings.Contains(gotLog, "rbi: index build completed") {
		t.Fatalf("expected rebuild completion log with DisableIndexStore, got: %q", gotLog)
	}
}

func TestWrap_PersistedIndexSchemaNarrowing_LogsFullRebuildWhenNoCompatibleIndexes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "schema_mismatch.db")

	const bucket = "schema_mismatch"

	bolt, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}

	c, err := Open[uint64, Rec](bolt, testOptions(Options{
		AnalyzeInterval: -1,
		BucketName:      bucket,
	}))
	if err != nil {
		t.Fatalf("initial New: %v", err)
	}
	if err = writeSet(c, 1, &Rec{Name: "alice", Age: 30, Email: "alice@example.test"}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	if err = c.Close(); err != nil {
		t.Fatalf("initial Close: %v", err)
	}
	if err = bolt.Close(); err != nil {
		t.Fatalf("initial raw Close: %v", err)
	}

	bolt2, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("reopen bbolt.Open: %v", err)
	}
	defer func() { _ = bolt2.Close() }()

	var logBuf bytes.Buffer

	c2, err := Open[uint64, schemaSubsetRec](bolt2, Options{
		AnalyzeInterval: -1,
		BucketName:      bucket,
		Logger:          log.New(&logBuf, "", 0),
	})
	if err != nil {
		t.Fatalf("reopen New: %v", err)
	}
	defer func() { _ = c2.Close() }()

	gotLog := logBuf.String()
	if strings.Contains(gotLog, "persisted index unavailable") {
		t.Fatalf("expected successful persisted index read without availability failure, got log: %q", gotLog)
	}
	if !strings.Contains(gotLog, "persisted index has no compatible field indexes") {
		t.Fatalf("expected explicit no-compatible-indexes reason in log, got: %q", gotLog)
	}
	if !strings.Contains(gotLog, "rbi: rebuilding index from bbolt") {
		t.Fatalf("expected full rebuild log for no-compatible-indexes path, got: %q", gotLog)
	}
	if !strings.Contains(gotLog, "mode=full") {
		t.Fatalf("expected full rebuild mode in log, got: %q", gotLog)
	}
	if !strings.Contains(gotLog, "rbi: index build completed (mode=full duration=") {
		t.Fatalf("expected full rebuild completion duration in log, got: %q", gotLog)
	}

	got, err := readGet(c2, 1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil || got.Name != "alice" || got.Age != 30 {
		t.Fatalf("expected record after partial persisted load, got %#v", got)
	}

	ids, err := readQueryKeys(c2, qx.Query(qx.EQ("age", 30)))
	if err != nil {
		t.Fatalf("QueryKeys(age=30): %v", err)
	}
	if !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("unexpected rebuilt query result: got=%v want=[1]", ids)
	}
}

func TestWrap_PartialPersistedLoad_RefreshesPlannerStats(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "planner_partial.db")

	const bucket = "planner_partial"

	bolt, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}

	c, err := Open[uint64, plannerStatsPartialBaseRec](bolt, testOptions(Options{
		AnalyzeInterval: -1,
		BucketName:      bucket,
	}))
	if err != nil {
		t.Fatalf("initial New: %v", err)
	}
	if err = writeSet(c, 1, &plannerStatsPartialBaseRec{Name: "alice"}); err != nil {
		t.Fatalf("seed Set(1): %v", err)
	}
	if err = writeSet(c, 2, &plannerStatsPartialBaseRec{Name: "bob"}); err != nil {
		t.Fatalf("seed Set(2): %v", err)
	}
	if err = c.RefreshPlannerStats(); err != nil {
		t.Fatalf("initial RefreshPlannerStats: %v", err)
	}
	if err = c.Close(); err != nil {
		t.Fatalf("initial Close: %v", err)
	}
	if err = bolt.Close(); err != nil {
		t.Fatalf("initial raw Close: %v", err)
	}

	bolt2, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("reopen bbolt.Open: %v", err)
	}
	defer func() { _ = bolt2.Close() }()

	c2, err := Open[uint64, plannerStatsPartialNextRec](bolt2, testOptions(Options{
		AnalyzeInterval: -1,
		BucketName:      bucket,
	}))
	if err != nil {
		t.Fatalf("reopen New: %v", err)
	}
	defer func() { _ = c2.Close() }()

	got := c2.PlannerStats()
	if got.UniverseCardinality != 2 {
		t.Fatalf("planner universe=%d want=2", got.UniverseCardinality)
	}
	if got.FieldCount != 3 {
		t.Fatalf("planner field count=%d want=3", got.FieldCount)
	}
	if stats, ok := got.Fields["name"]; !ok || stats.DistinctKeys != 2 {
		t.Fatalf("planner name stats=%+v want distinct=2", stats)
	}
	if stats, ok := got.Fields["age"]; !ok || stats.DistinctKeys == 0 {
		t.Fatalf("planner age stats=%+v want non-zero rebuilt stats", stats)
	}
	if stats, ok := got.Fields["$key"]; !ok || stats.DistinctKeys != 2 || stats.TotalBucketCard != 2 || stats.MaxBucketCard != 1 {
		t.Fatalf("planner $key stats=%+v want unique rows", stats)
	}
}

func TestWrap_PartialPersistedLoad_PreservesLenZeroComplementFlags(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "len_zero_partial.db")

	const bucket = "len_zero_partial"

	bolt, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}

	c, err := Open[uint64, lenZeroComplementPartialBaseRec](bolt, testOptions(Options{
		AnalyzeInterval: -1,
		BucketName:      bucket,
	}))
	if err != nil {
		t.Fatalf("initial New: %v", err)
	}
	for i := 1; i <= 90; i++ {
		rec := &lenZeroComplementPartialBaseRec{
			Name: fmt.Sprintf("u_%d", i),
		}
		if i%5 == 0 {
			rec.Tags = []string{"go"}
		}
		if err = writeSet(c, uint64(i), rec); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}
	if err = c.Close(); err != nil {
		t.Fatalf("initial Close: %v", err)
	}
	if err = bolt.Close(); err != nil {
		t.Fatalf("initial raw Close: %v", err)
	}

	bolt2, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("reopen bbolt.Open: %v", err)
	}
	defer func() { _ = bolt2.Close() }()

	var logBuf bytes.Buffer

	c2, err := Open[uint64, lenZeroComplementPartialNextRec](bolt2, Options{
		AnalyzeInterval: -1,
		BucketName:      bucket,
		Logger:          log.New(&logBuf, "", 0),
	})
	if err != nil {
		t.Fatalf("reopen New: %v", err)
	}
	defer func() { _ = c2.Close() }()

	gotLog := logBuf.String()
	if strings.Contains(gotLog, "persisted index unavailable") {
		t.Fatalf("expected partial persisted load, got log: %q", gotLog)
	}
	if strings.Contains(gotLog, "rbi: rebuilding index from bbolt") {
		t.Fatalf("expected partial rebuild instead of full rebuild, got log: %q", gotLog)
	}
	if !strings.Contains(gotLog, "rbi: partially rebuilding index from bbolt") {
		t.Fatalf("expected partial rebuild log, got: %q", gotLog)
	}
	if !strings.Contains(gotLog, "loaded_fields=2/3") {
		t.Fatalf("expected partial rebuild loaded field count in log, got: %q", gotLog)
	}
	if !strings.Contains(gotLog, "missing_fields=1") {
		t.Fatalf("expected partial rebuild missing field count in log, got: %q", gotLog)
	}
	if !strings.Contains(gotLog, "rbi: index build completed (mode=partial duration=") {
		t.Fatalf("expected partial rebuild completion duration in log, got: %q", gotLog)
	}

	want := make([]uint64, 0, 72)
	for i := 1; i <= 90; i++ {
		if i%5 != 0 {
			want = append(want, uint64(i))
		}
	}
	emptyTagsQ := qx.Query(qx.EQ("tags", []string{}))
	got, err := readQueryKeys(c2, emptyTagsQ)
	if err != nil {
		t.Fatalf("QueryKeys(empty tags): %v", err)
	}
	if !queryIDsEqual(emptyTagsQ, got, want) {
		t.Fatalf("unexpected empty-tags result after partial load: got=%v want=%v", got, want)
	}
}

func TestFailpoint_CloseStoreIndexErrorStillCloses(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "close_store_failpoint.db")
	c, bolt := openBoltAndCollection[uint64, Rec](t, path)
	defer func() { _ = bolt.Close() }()

	if err := writeSet(c, 1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	c.rbiFile = filepath.Join(t.TempDir(), "missing", "index.rbi")
	err := c.Close()
	if err == nil {
		t.Fatalf("expected store index error on Close")
	}

	if c.state.Load()&collectionClosed == 0 {
		t.Fatal("expected db to be marked closed even when storeIndex fails")
	}
	if _, statErr := os.Stat(c.rbiFile); !os.IsNotExist(statErr) {
		t.Fatalf("expected persisted index to stay absent after failed Close, statErr=%v", statErr)
	}
	if err = c.Close(); err != nil {
		t.Fatalf("second Close must be no-op, got: %v", err)
	}

	if err = bolt.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(c.dataBucket)
		if b == nil {
			return fmt.Errorf("bucket missing after close")
		}
		var keyBuf [8]byte
		if b.Get(keycodec.UserKeyBytesWithBuf(uint64(1), c.strKey, &keyBuf)) == nil {
			return fmt.Errorf("record missing after close")
		}
		return nil
	}); err != nil {
		t.Fatalf("raw.View: %v", err)
	}
}

func TestPersistedIndex_RemainsPresentUntilClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persisted_index_sequence.db")

	c, bolt := openBoltAndCollection[uint64, Rec](t, path)
	if err := writeSet(c, 1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("seed Close: %v", err)
	}
	if err := bolt.Close(); err != nil {
		t.Fatalf("seed raw close: %v", err)
	}

	c2, bolt2 := openBoltAndCollection[uint64, Rec](t, path)
	defer func() {
		_ = c2.Close()
		_ = bolt2.Close()
	}()

	if _, err := os.Stat(c2.rbiFile); err != nil {
		t.Fatalf("expected persisted index before write, stat err=%v", err)
	}
	storedSeq := readPersistedIndexSequence(t, c2.rbiFile)
	if currentSeq := readBucketSequence(t, bolt2, c2.dataBucket); currentSeq != storedSeq {
		t.Fatalf("expected persisted index sequence=%d before write, got bucket sequence=%d", storedSeq, currentSeq)
	}
	if err := writeSet(c2, 2, &Rec{Name: "bob", Age: 31}); err != nil {
		t.Fatalf("Set after reopen: %v", err)
	}
	if _, err := os.Stat(c2.rbiFile); err != nil {
		t.Fatalf("expected persisted index to remain present after write, stat err=%v", err)
	}
	if currentSeq := readBucketSequence(t, bolt2, c2.dataBucket); currentSeq <= storedSeq {
		t.Fatalf("expected bucket sequence to advance after write, before=%d after=%d", storedSeq, currentSeq)
	}
	if staleSeq := readPersistedIndexSequence(t, c2.rbiFile); staleSeq != storedSeq {
		t.Fatalf("expected persisted index sequence to stay stale until Close, before=%d after=%d", storedSeq, staleSeq)
	}
	if err := c2.Close(); err != nil {
		t.Fatalf("Close after write: %v", err)
	}
	if freshSeq := readPersistedIndexSequence(t, c2.rbiFile); freshSeq != readBucketSequence(t, bolt2, c2.dataBucket) {
		t.Fatalf("expected Close to refresh persisted index sequence, got %d", freshSeq)
	}
}

func TestPersistedIndex_RebuildsAfterCloseFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "close_store_failure_rebuild.db")

	c, bolt := openBoltAndCollection[uint64, Rec](t, path)
	if err := writeSet(c, 1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("seed Close: %v", err)
	}
	if err := bolt.Close(); err != nil {
		t.Fatalf("seed raw close: %v", err)
	}

	c2, bolt2 := openBoltAndCollection[uint64, Rec](t, path)
	if err := writeSet(c2, 2, &Rec{Name: "bob", Age: 40}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}
	persistedPath := c2.rbiFile
	c2.rbiFile = filepath.Join(t.TempDir(), "missing", "index.rbi")
	if err := c2.Close(); err == nil {
		t.Fatalf("expected store index error on Close")
	}
	if _, err := os.Stat(persistedPath); err != nil {
		t.Fatalf("expected stale persisted index file to remain after failed Close, stat err=%v", err)
	}
	staleSeq := readPersistedIndexSequence(t, persistedPath)
	currentSeq := readBucketSequence(t, bolt2, c2.dataBucket)
	if staleSeq == currentSeq {
		t.Fatalf("expected persisted index sequence to stay stale after failed Close, seq=%d", staleSeq)
	}
	if err := bolt2.Close(); err != nil {
		t.Fatalf("raw2 close: %v", err)
	}

	db3, raw3 := openBoltAndCollection[uint64, Rec](t, path)
	defer func() {
		_ = db3.Close()
		_ = raw3.Close()
	}()

	ids, err := readQueryKeys(db3, qx.Query())
	if err != nil {
		t.Fatalf("QueryKeys after reopen: %v", err)
	}
	slices.Sort(ids)
	if !slices.Equal(ids, []uint64{1, 2}) {
		t.Fatalf("unexpected ids after rebuild-on-open: %v", ids)
	}
}

func TestPersistedIndex_NoOpDeleteKeepsSequence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persisted_index_noop_delete.db")

	c, bolt := openBoltAndCollection[uint64, Rec](t, path)
	if err := writeSet(c, 1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("seed Close: %v", err)
	}
	if err := bolt.Close(); err != nil {
		t.Fatalf("seed raw close: %v", err)
	}

	c2, bolt2 := openBoltAndCollection[uint64, Rec](t, path)
	defer func() {
		_ = c2.Close()
		_ = bolt2.Close()
	}()

	storedSeq := readPersistedIndexSequence(t, c2.rbiFile)
	beforeSeq := readBucketSequence(t, bolt2, c2.dataBucket)
	if storedSeq != beforeSeq {
		t.Fatalf("expected persisted index sequence=%d before no-op delete, got bucket sequence=%d", storedSeq, beforeSeq)
	}

	if err := writeDelete(c2, 999); err != nil {
		t.Fatalf("Delete missing: %v", err)
	}

	afterStoredSeq := readPersistedIndexSequence(t, c2.rbiFile)
	afterSeq := readBucketSequence(t, bolt2, c2.dataBucket)
	if afterSeq != beforeSeq {
		t.Fatalf("expected missing Delete to keep bucket sequence=%d, got %d", beforeSeq, afterSeq)
	}
	if afterStoredSeq != storedSeq {
		t.Fatalf("expected missing Delete to keep persisted index sequence=%d, got %d", storedSeq, afterStoredSeq)
	}
}

func TestIndexPersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persist.db")

	c, bolt := openBoltAndCollection[uint64, Rec](t, path)

	if err := writeSet(c, 1, &Rec{Name: "alice", Age: 10, Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := writeSet(c, 2, &Rec{Name: "bob", Age: 20, Tags: []string{"java"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := bolt.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}

	c2, bolt2 := openBoltAndCollection[uint64, Rec](t, path)
	t.Cleanup(func() {
		if err := c2.Close(); err != nil {
			t.Fatal(err)
		}
		if err := bolt2.Close(); err != nil {
			t.Fatal(err)
		}
	})

	ids, err := readQueryKeys(c2, qx.Query(qx.EQ("name", "alice")))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("expected [1], got %v", ids)
	}

	st, err := c2.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if st.KeyCount != 2 {
		t.Fatalf("expected Stats.KeyCount=2, got %d", st.KeyCount)
	}
}

func TestIndexPersistence_RelativeBoltPathKeepsSidecarWithOriginalCWD(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	defer func() {
		if err := os.Chdir(cwd); err != nil {
			t.Errorf("restore cwd: %v", err)
		}
	}()

	dir := t.TempDir()
	dbDir := filepath.Join(dir, "db")
	otherDir := filepath.Join(dir, "other")
	if err := os.Mkdir(dbDir, 0o755); err != nil {
		t.Fatalf("mkdir db dir: %v", err)
	}
	if err := os.Mkdir(otherDir, 0o755); err != nil {
		t.Fatalf("mkdir other dir: %v", err)
	}

	name := "relative_sidecar.db"
	opts := testOptions(Options{BucketName: "relative_sidecar"})
	if err := os.Chdir(dbDir); err != nil {
		t.Fatalf("chdir db dir: %v", err)
	}

	bolt, err := bbolt.Open(name, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}
	c, err := Open[uint64, schemaSubsetRec](bolt, opts)
	if err != nil {
		_ = bolt.Close()
		t.Fatalf("New: %v", err)
	}

	wantBoltPath := filepath.Join(dbDir, name)
	if c.boltPath != wantBoltPath {
		_ = c.Close()
		_ = bolt.Close()
		t.Fatalf("boltPath=%q want %q", c.boltPath, wantBoltPath)
	}
	if err = writeSet(c, 1, &schemaSubsetRec{Name: "alice", Age: 10}); err != nil {
		_ = c.Close()
		_ = bolt.Close()
		t.Fatalf("Set: %v", err)
	}

	if err = os.Chdir(otherDir); err != nil {
		_ = c.Close()
		_ = bolt.Close()
		t.Fatalf("chdir other dir: %v", err)
	}
	if err = c.Close(); err != nil {
		_ = bolt.Close()
		t.Fatalf("Close: %v", err)
	}
	if err = bolt.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}

	wantSidecar := filepath.Join(dbDir, name+"."+opts.BucketName+".rbi")
	if _, err = os.Stat(wantSidecar); err != nil {
		t.Fatalf("stat original cwd sidecar: %v", err)
	}
	wrongSidecar := filepath.Join(otherDir, name+"."+opts.BucketName+".rbi")
	if _, err = os.Stat(wrongSidecar); !os.IsNotExist(err) {
		t.Fatalf("sidecar resolved from changed cwd: statErr=%v", err)
	}

	if err = os.Chdir(dbDir); err != nil {
		t.Fatalf("chdir db dir before reopen: %v", err)
	}
	bolt2, err := bbolt.Open(name, 0o600, nil)
	if err != nil {
		t.Fatalf("reopen bbolt.Open: %v", err)
	}
	c2, err := Open[uint64, schemaSubsetRec](bolt2, opts)
	if err != nil {
		_ = bolt2.Close()
		t.Fatalf("reopen New: %v", err)
	}
	defer func() {
		_ = c2.Close()
		_ = bolt2.Close()
	}()

	ids, err := readQueryKeys(c2, qx.Query(qx.EQ("name", "alice")))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("QueryKeys=%v want [1]", ids)
	}
}

func TestRegistry_RelativeBoltPathUsesBoltHandleIdentity(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	defer func() {
		if err := os.Chdir(cwd); err != nil {
			t.Errorf("restore cwd: %v", err)
		}
	}()

	dir := t.TempDir()
	dbDir := filepath.Join(dir, "db")
	otherDir := filepath.Join(dir, "other")
	if err := os.Mkdir(dbDir, 0o755); err != nil {
		t.Fatalf("mkdir db dir: %v", err)
	}
	if err := os.Mkdir(otherDir, 0o755); err != nil {
		t.Fatalf("mkdir other dir: %v", err)
	}

	name := "relative_registry.db"
	opts := testOptions(Options{BucketName: "relative_registry", DisableIndexStore: true})
	if err := os.Chdir(dbDir); err != nil {
		t.Fatalf("chdir db dir: %v", err)
	}

	bolt, err := bbolt.Open(name, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}
	c, err := Open[uint64, schemaSubsetRec](bolt, opts)
	if err != nil {
		_ = bolt.Close()
		t.Fatalf("New: %v", err)
	}

	if err = os.Chdir(otherDir); err != nil {
		_ = c.Close()
		_ = bolt.Close()
		t.Fatalf("chdir other dir: %v", err)
	}
	c2, err := Open[uint64, schemaSubsetRec](bolt, opts)
	if err == nil {
		_ = c2.Close()
		_ = c.Close()
		_ = bolt.Close()
		t.Fatalf("New accepted second DB wrapper for same Bolt handle and bucket")
	}
	if !strings.Contains(err.Error(), "already open") {
		_ = c.Close()
		_ = bolt.Close()
		t.Fatalf("New err=%v want already open", err)
	}

	if err = c.Close(); err != nil {
		_ = bolt.Close()
		t.Fatalf("Close: %v", err)
	}
	if err = bolt.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}
}

func TestIndexPersistence_PersistedIndexPathOverridesRelativeBoltPathCWD(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	defer func() {
		if err := os.Chdir(cwd); err != nil {
			t.Errorf("restore cwd: %v", err)
		}
	}()

	dir := t.TempDir()
	dbDir := filepath.Join(dir, "db")
	runDir := filepath.Join(dir, "run")
	sidecarDir := filepath.Join(dir, "sidecar")
	if err := os.Mkdir(dbDir, 0o755); err != nil {
		t.Fatalf("mkdir db dir: %v", err)
	}
	if err := os.Mkdir(runDir, 0o755); err != nil {
		t.Fatalf("mkdir run dir: %v", err)
	}
	if err := os.Mkdir(sidecarDir, 0o755); err != nil {
		t.Fatalf("mkdir sidecar dir: %v", err)
	}

	name := "relative_override.db"
	sidecar := filepath.Join(sidecarDir, "relative_override.rbi")
	opts := testOptions(Options{
		BucketName:         "relative_override",
		PersistedIndexPath: sidecar,
	})

	if err := os.Chdir(dbDir); err != nil {
		t.Fatalf("chdir db dir: %v", err)
	}
	bolt, err := bbolt.Open(name, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}
	if err := os.Chdir(runDir); err != nil {
		_ = bolt.Close()
		t.Fatalf("chdir run dir: %v", err)
	}
	c, err := Open[uint64, schemaSubsetRec](bolt, opts)
	if err != nil {
		_ = bolt.Close()
		t.Fatalf("New: %v", err)
	}
	if c.rbiFile != sidecar {
		_ = c.Close()
		_ = bolt.Close()
		t.Fatalf("rbiFile=%q want %q", c.rbiFile, sidecar)
	}
	if err = writeSet(c, 1, &schemaSubsetRec{Name: "alice", Age: 10}); err != nil {
		_ = c.Close()
		_ = bolt.Close()
		t.Fatalf("Set: %v", err)
	}
	if err = c.Close(); err != nil {
		_ = bolt.Close()
		t.Fatalf("Close: %v", err)
	}
	if err = bolt.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}

	if _, err = os.Stat(sidecar); err != nil {
		t.Fatalf("stat explicit sidecar: %v", err)
	}
	defaultSidecar := filepath.Join(runDir, name+"."+opts.BucketName+".rbi")
	if _, err = os.Stat(defaultSidecar); !os.IsNotExist(err) {
		t.Fatalf("default sidecar was written despite override: statErr=%v", err)
	}

	if err = os.Chdir(dbDir); err != nil {
		t.Fatalf("chdir db dir before reopen: %v", err)
	}
	bolt2, err := bbolt.Open(name, 0o600, nil)
	if err != nil {
		t.Fatalf("reopen bbolt.Open: %v", err)
	}
	if err = os.Chdir(runDir); err != nil {
		_ = bolt2.Close()
		t.Fatalf("chdir run dir before reopen New: %v", err)
	}
	c2, err := Open[uint64, schemaSubsetRec](bolt2, opts)
	if err != nil {
		_ = bolt2.Close()
		t.Fatalf("reopen New: %v", err)
	}
	defer func() {
		_ = c2.Close()
		_ = bolt2.Close()
	}()

	ids, err := readQueryKeys(c2, qx.Query(qx.EQ("name", "alice")))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("QueryKeys=%v want [1]", ids)
	}
}

func TestIndexPersistence_PersistedIndexPathRejectsBoltPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reject_same_path.db")
	bolt, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}
	defer func() {
		_ = bolt.Close()
	}()

	c, err := Open[uint64, schemaSubsetRec](bolt, testOptions(Options{
		BucketName:         "reject_same_path",
		PersistedIndexPath: path,
	}))
	if err == nil {
		_ = c.Close()
		t.Fatalf("New accepted PersistedIndexPath matching Bolt database path")
	}
	if !strings.Contains(err.Error(), "PersistedIndexPath cannot match Bolt database path") {
		t.Fatalf("New err=%v want PersistedIndexPath rejection", err)
	}
}

func TestIndexPersistence_LargeFieldRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persist_large.db")

	c, bolt := openBoltAndCollection[uint64, Rec](t, path)
	const rows = 4096
	for i := 0; i < rows; i++ {
		if err := writeSet(c, uint64(i+1), &Rec{
			Name:  fmt.Sprintf("user_%04d", i),
			Email: fmt.Sprintf("user_%04d@example.test", i),
			Age:   i,
		}); err != nil {
			t.Fatalf("Set(%d): %v", i+1, err)
		}
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := bolt.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}

	c2, bolt2 := openBoltAndCollection[uint64, Rec](t, path)
	t.Cleanup(func() {
		if err := c2.Close(); err != nil {
			t.Fatal(err)
		}
		if err := bolt2.Close(); err != nil {
			t.Fatal(err)
		}
	})

	ids, err := readQueryKeys(c2, qx.Query(qx.EQ("name", "user_0007")))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) != 1 || ids[0] != 8 {
		t.Fatalf("expected [8], got %v", ids)
	}

	ids, err = readQueryKeys(c2, qx.Query(qx.EQ("name", "user_4095")))
	if err != nil {
		t.Fatalf("QueryKeys(last): %v", err)
	}
	if len(ids) != 1 || ids[0] != rows {
		t.Fatalf("expected [%d], got %v", rows, ids)
	}
}

func TestIndexPersistence_LenZeroComplement_AllEmptyAfterReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persist_len_zero_complement_all_empty.db")

	c, bolt := openBoltAndCollection[uint64, Rec](t, path)
	for i := 1; i <= 90; i++ {
		rec := &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Email: fmt.Sprintf("u_%d@example.test", i),
			Age:   i,
		}
		if i%5 == 0 {
			rec.Tags = []string{"go"}
		}
		if err := writeSet(c, uint64(i), rec); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}
	for i := 1; i <= 90; i++ {
		if err := writePatch(c, uint64(i), []Field{{Name: "tags", Value: []string(nil)}}); err != nil {
			t.Fatalf("Patch(%d): %v", i, err)
		}
	}

	want := make([]uint64, 0, 90)
	for i := 1; i <= 90; i++ {
		want = append(want, uint64(i))
	}
	gotBeforeClose, err := readQueryKeys(c, qx.Query(qx.EQ("tags", []string{})))
	if err != nil {
		t.Fatalf("QueryKeys(empty tags) before close: %v", err)
	}
	if !slices.Equal(gotBeforeClose, want) {
		t.Fatalf("unexpected empty-tags ids before reopen: got=%v want=%v", gotBeforeClose, want)
	}

	if err = c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err = bolt.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}

	c2, bolt2 := openBoltAndCollection[uint64, Rec](t, path)
	t.Cleanup(func() {
		if err = c2.Close(); err != nil {
			t.Fatal(err)
		}
		if err = bolt2.Close(); err != nil {
			t.Fatal(err)
		}
	})

	gotAfterReopen, err := readQueryKeys(c2, qx.Query(qx.EQ("tags", []string{})))
	if err != nil {
		t.Fatalf("QueryKeys(empty tags) after reopen: %v", err)
	}
	if !slices.Equal(gotAfterReopen, want) {
		t.Fatalf("unexpected empty-tags ids after reopen: got=%v want=%v", gotAfterReopen, want)
	}
}

type persistedVIOld int

func (v persistedVIOld) IndexingValue() string {
	return fmt.Sprintf("old:%d", v)
}

type persistedVINew int

func (v persistedVINew) IndexingValue() string {
	return fmt.Sprintf("new:%d", v)
}

type persistedVIOldRec struct {
	Code persistedVIOld `db:"code" rbi:"index"`
}

type persistedVINewRec struct {
	Code persistedVINew `db:"code" rbi:"index"`
}

func TestIndexPersistence_RebuildsValueIndexerFieldAfterTypeChange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vi_type_change.db")
	opts := testOptions(Options{BucketName: "vi_type_change"})

	bolt, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("initial bbolt.Open: %v", err)
	}
	c, err := Open[uint64, persistedVIOldRec](bolt, opts)
	if err != nil {
		_ = bolt.Close()
		t.Fatalf("initial New: %v", err)
	}
	if err = writeSet(c, 1, &persistedVIOldRec{Code: 7}); err != nil {
		_ = c.Close()
		_ = bolt.Close()
		t.Fatalf("Set: %v", err)
	}
	if err = c.Close(); err != nil {
		_ = bolt.Close()
		t.Fatalf("initial Close: %v", err)
	}
	if err = bolt.Close(); err != nil {
		t.Fatalf("initial bolt Close: %v", err)
	}

	bolt2, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("reopen bbolt.Open: %v", err)
	}
	c2, err := Open[uint64, persistedVINewRec](bolt2, opts)
	if err != nil {
		_ = bolt2.Close()
		t.Fatalf("reopen New: %v", err)
	}
	defer func() { _ = c2.Close() }()
	defer func() { _ = bolt2.Close() }()

	ids, err := readQueryKeys(c2, qx.Query(qx.EQ("code", persistedVINew(7))))
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("ids=%v want [1]", ids)
	}
}

type schemaSubsetRec struct {
	Name string `db:"name" rbi:"index"`
	Age  int    `db:"age"  rbi:"index"`
}

type plannerStatsPartialBaseRec struct {
	Name string `db:"name" rbi:"index"`
}

type plannerStatsPartialNextRec struct {
	Name string `db:"name" rbi:"index"`
	Age  int    `db:"age"  rbi:"index"`
}

type lenZeroComplementPartialBaseRec struct {
	Name string   `db:"name" rbi:"index"`
	Tags []string `db:"tags" rbi:"index"`
}

type lenZeroComplementPartialNextRec struct {
	Name string   `db:"name" rbi:"index"`
	Tags []string `db:"tags" rbi:"index"`
	Age  int      `db:"age"  rbi:"index"`
}

func readPersistedIndexSequence(tb testing.TB, path string) uint64 {
	tb.Helper()

	f, err := os.Open(path)
	if err != nil {
		tb.Fatalf("open persisted index: %v", err)
	}
	defer func() { _ = f.Close() }()

	reader := bufio.NewReader(f)
	readPersistedIndexFormatByteFromReader(tb, reader)
	seq, err := binary.ReadUvarint(reader)
	if err != nil {
		tb.Fatalf("read persisted index sequence: %v", err)
	}
	return seq
}

func readPersistedIndexFormatByte(tb testing.TB, path string) byte {
	tb.Helper()

	f, err := os.Open(path)
	if err != nil {
		tb.Fatalf("open persisted index: %v", err)
	}
	defer func() { _ = f.Close() }()

	reader := bufio.NewReader(f)
	return readPersistedIndexFormatByteFromReader(tb, reader)
}

func readPersistedIndexFormatByteFromReader(tb testing.TB, reader *bufio.Reader) byte {
	tb.Helper()

	for _, want := range []byte("RBI") {
		got, err := reader.ReadByte()
		if err != nil {
			tb.Fatalf("read persisted index magic: %v", err)
		}
		if got != want {
			tb.Fatalf("persisted index magic byte=%q want %q", got, want)
		}
	}
	ver, err := reader.ReadByte()
	if err != nil {
		tb.Fatalf("read persisted index format byte: %v", err)
	}
	return ver
}

func TestTruncate_PreservesSequenceMonotonicityAcrossBucketRecreate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "truncate_sequence.db")

	c1, bolt1 := openBoltAndCollection[uint64, Rec](t, path)
	if err := writeSet(c1, 1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	persistedPath := path + ".Rec.rbi"
	initialSeq := readBucketSequence(t, bolt1, c1.dataBucket)
	if initialSeq == 0 {
		t.Fatalf("expected sequence to advance after write, got %d", initialSeq)
	}
	if err := c1.Close(); err != nil {
		t.Fatalf("Close(db1): %v", err)
	}
	if err := bolt1.Close(); err != nil {
		t.Fatalf("Close(bolt1): %v", err)
	}

	storedSeq := readPersistedIndexSequence(t, persistedPath)
	if storedSeq != initialSeq {
		t.Fatalf("persisted sequence mismatch: stored=%d bucket=%d", storedSeq, initialSeq)
	}

	c2, bolt2 := openBoltAndCollection[uint64, Rec](t, path, Options{DisableIndexStore: true})
	if err := c2.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	truncateSeq := readBucketSequence(t, bolt2, c2.dataBucket)
	if truncateSeq <= storedSeq {
		t.Fatalf("truncate must preserve monotonic sequence: stored=%d truncate=%d", storedSeq, truncateSeq)
	}
	if err := c2.Close(); err != nil {
		t.Fatalf("Close(c2): %v", err)
	}
	if err := bolt2.Close(); err != nil {
		t.Fatalf("Close(bolt2): %v", err)
	}

	if got := readPersistedIndexSequence(t, persistedPath); got != storedSeq {
		t.Fatalf("DisableIndexStore should keep old sidecar untouched: got=%d want=%d", got, storedSeq)
	}

	c3, bolt3 := openBoltAndCollection[uint64, Rec](t, path)
	defer func() { _ = c3.Close() }()
	defer func() { _ = bolt3.Close() }()

	if got := readBucketSequence(t, bolt3, c3.dataBucket); got != truncateSeq {
		t.Fatalf("reopened bucket sequence mismatch: got=%d want=%d", got, truncateSeq)
	}
	if snap := c3.SnapshotStats(); snap.Sequence != truncateSeq {
		t.Fatalf("snapshot sequence mismatch after reopen: got=%d want=%d", snap.Sequence, truncateSeq)
	}

	ids, err := readQueryKeys(c3, qx.Query(qx.EQ("age", 30)))
	if err != nil {
		t.Fatalf("QueryKeys(after reopen): %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected rebuilt empty index after truncate, got ids=%v", ids)
	}
}
