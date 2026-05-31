package wexec

import (
	"bytes"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/internal/strmap"
	"go.etcd.io/bbolt"
)

type attemptRec struct {
	V byte `db:"v" rbi:"unique"`
}

type attemptPairRec struct {
	A byte `db:"a"`
	B byte `db:"b"`
}

func executeBatchForTest(ex *Batcher, batch []*request) {
	ex.sched.stats.recordExecuted(len(batch))
	stats := ex.sched.stats.Enabled
	var started time.Time
	if stats {
		started = time.Now()
	}
	reqScratch := requestScratchPool.Get(len(batch))
	reqScratch = append(reqScratch, batch...)
	ex.runShared(reqScratch)
	requestScratchPool.Put(reqScratch)
	finishRequestsForTest(batch)
	if stats {
		ex.sched.stats.ExecuteNanos.Add(uint64(time.Since(started)))
	}
}

func finishRequestsForTest(batch []*request) {
	resolveRequestErrs(batch)
	for _, req := range batch {
		if req.setPayload != nil {
			encodePool.Put(req.setPayload)
			req.setPayload = nil
		}
		req.setValue = nil
		req.setBaseline = nil
		clear(req.patch)
		req.patch = req.patch[:0]
		req.patchIgnoreUnknown = false
		req.beforeProcess = nil
		req.beforeStore = nil
		req.beforeCommit = nil
		req.cloneValue = nil
		req.policy = 0
		req.replacedBy = nil
		req.Done <- req.Err
	}
}

func setAttemptReq(id uint64, v byte) *request {
	rec := &attemptRec{V: v}
	payload := encodePool.Get()
	_ = payload.WriteByte(v)
	return &request{
		op:         opSet,
		id:         keycodec.DataKeyFromUserKey(id, false),
		setValue:   unsafe.Pointer(rec),
		setPayload: payload,
		Done:       make(chan error, 1),
	}
}

func stringSetAttemptReq(id string, v byte) *request {
	req := setAttemptReq(0, v)
	req.id = keycodec.DataKeyFromUserKey(id, true)
	return req
}

func cloneSetAttemptReq(id uint64, baseline *attemptRec) *request {
	return &request{
		op:          opSet,
		id:          keycodec.DataKeyFromUserKey(id, false),
		setBaseline: unsafe.Pointer(baseline),
		cloneValue: func(_ keycodec.DataKey, value unsafe.Pointer) (unsafe.Pointer, error) {
			cp := *(*attemptRec)(value)
			return unsafe.Pointer(&cp), nil
		},
		Done: make(chan error, 1),
	}
}

func deleteAttemptReq(id uint64) *request {
	return &request{
		op:   opDelete,
		id:   keycodec.DataKeyFromUserKey(id, false),
		Done: make(chan error, 1),
	}
}

func patchAttemptReq(id uint64, patch []schema.PatchItem, ignoreUnknown bool) *request {
	return &request{
		op:                 opPatch,
		id:                 keycodec.DataKeyFromUserKey(id, false),
		patch:              patch,
		patchIgnoreUnknown: ignoreUnknown,
		Done:               make(chan error, 1),
	}
}

func newPatchAttemptTestExecutor(t *testing.T, events *[]string, commit func(*bbolt.Tx, string) error) (*Batcher, *bbolt.DB, []byte) {
	t.Helper()

	ex, raw, bucket := newAttemptTestExecutor(t, events, commit)
	rt, err := schema.Compile(reflect.TypeFor[attemptRec](), schema.Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	ex.schema = rt
	ex.indexed = false
	ex.snapshotOps = SnapshotOps{}
	return ex, raw, bucket
}

func newPairAttemptTestExecutor(t *testing.T, events *[]string, commit func(*bbolt.Tx, string) error) (*Batcher, *bbolt.DB, []byte) {
	t.Helper()

	ex, raw, bucket := newAttemptTestExecutor(t, events, commit)
	rt, err := schema.Compile(reflect.TypeFor[attemptPairRec](), schema.Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	ops := RecordOps{
		Encode: func(ptr unsafe.Pointer, buf *bytes.Buffer) error {
			rec := (*attemptPairRec)(ptr)
			_ = buf.WriteByte(rec.A)
			_ = buf.WriteByte(rec.B)
			return nil
		},
		Decode: func(data []byte) (unsafe.Pointer, error) {
			return unsafe.Pointer(&attemptPairRec{A: data[0], B: data[1]}), nil
		},
		Release:       func(unsafe.Pointer) {},
		ValidateIndex: func(unsafe.Pointer) error { return nil },
	}
	ex.ops = &ops
	ex.schema = rt
	ex.indexed = false
	ex.snapshotOps = SnapshotOps{}
	return ex, raw, bucket
}

func newUniqueAttemptTestExecutor(t *testing.T, events *[]string, uniqueErr error, seed []snapshot.BatchEntry, commit func(*bbolt.Tx, string) error) (*Batcher, *bbolt.DB, []byte) {
	t.Helper()

	ex, raw, bucket := newAttemptTestExecutor(t, events, commit)
	rt, err := schema.Compile(reflect.TypeFor[attemptRec](), schema.Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	current := snapshot.NewView(0, nil, rt, snapshot.CacheConfig{}, snapshot.Storage{})
	if len(seed) != 0 {
		current = snapshot.BuildPrepared(1, nil, rt, snapshot.CacheConfig{}, nil, rt.Patch.Fields, seed)
	}
	manager := snapshot.NewRegistry(false)
	manager.Publish(current)
	ex.schema = rt
	ex.indexed = true
	ex.unique = UniqueContext{
		Schema:          rt,
		Current:         manager.Current,
		UniqueViolation: uniqueErr,
	}
	ex.errs = ErrorSet{UniqueViolation: uniqueErr}
	ex.snapshotOps = SnapshotOps{
		Manager:     manager,
		Schema:      rt,
		CacheConfig: func() snapshot.CacheConfig { return snapshot.CacheConfig{} },
		PatchFields: rt.Patch.Fields,
	}
	ex.indexPublishOps = IndexPublishOps{
		PublishCommitted: func(seq uint64, op string, snap *snapshot.View) error {
			manager.Publish(snap)
			return nil
		},
	}
	return ex, raw, bucket
}

func newStringAttemptTestExecutor(t *testing.T, events *[]string, seedKey string, seedValue byte, uniqueErr error, commit func(*bbolt.Tx, string) error) (*Batcher, *bbolt.DB, []byte, *strmap.Mapper) {
	t.Helper()

	ex, raw, bucket := newAttemptTestExecutor(t, events, commit)
	rt, err := schema.Compile(reflect.TypeFor[attemptRec](), schema.Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	sm := strmap.New(0, 64)
	seed := attemptRec{V: seedValue}
	seedIdx, _ := sm.Create(seedKey)
	current := snapshot.BuildPrepared(1, nil, rt, snapshot.CacheConfig{}, sm, rt.Patch.Fields, []snapshot.BatchEntry{
		{ID: seedIdx, New: unsafe.Pointer(&seed)},
	})
	manager := snapshot.NewRegistry(false)
	manager.Publish(current)
	sm.MarkCommittedPublished(current.StrMap)
	putStringAttemptPayload(t, raw, bucket, seedKey, []byte{seedValue})

	ex.strKey = true
	ex.strMap = sm
	ex.strKey = true
	ex.strMap = sm
	ex.schema = rt
	ex.indexed = true
	ex.unique = UniqueContext{
		Schema:          rt,
		Current:         manager.Current,
		UniqueViolation: uniqueErr,
	}
	ex.errs = ErrorSet{UniqueViolation: uniqueErr}
	ex.snapshotOps = SnapshotOps{
		Manager:     manager,
		Schema:      rt,
		CacheConfig: func() snapshot.CacheConfig { return snapshot.CacheConfig{} },
		StrMap:      sm,
		PatchFields: rt.Patch.Fields,
	}
	ex.indexPublishOps = IndexPublishOps{
		PublishCommitted: func(seq uint64, op string, snap *snapshot.View) error {
			manager.Publish(snap)
			sm.MarkCommittedPublished(snap.StrMap)
			return nil
		},
	}
	return ex, raw, bucket, sm
}

func newAttemptTestExecutor(t *testing.T, events *[]string, commit func(*bbolt.Tx, string) error) (*Batcher, *bbolt.DB, []byte) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "wexec.db")
	raw, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}
	t.Cleanup(func() {
		_ = raw.Close()
	})

	bucket := []byte("records")
	if err = raw.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucket(bucket)
		return err
	}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	ops := RecordOps{
		Encode: func(ptr unsafe.Pointer, buf *bytes.Buffer) error {
			_ = buf.WriteByte((*attemptRec)(ptr).V)
			return nil
		},
		Decode: func(data []byte) (unsafe.Pointer, error) {
			return unsafe.Pointer(&attemptRec{V: data[0]}), nil
		},
		Release:       func(unsafe.Pointer) {},
		ValidateIndex: func(unsafe.Pointer) error { return nil },
	}
	rt, err := schema.Compile(reflect.TypeFor[attemptRec](), schema.Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	manager := snapshot.NewRegistry(false)
	var mu sync.RWMutex
	snapOps := SnapshotOps{
		Manager:     manager,
		Schema:      rt,
		CacheConfig: func() snapshot.CacheConfig { return snapshot.CacheConfig{} },
		PatchFields: rt.Patch.Fields,
	}
	publishOps := IndexPublishOps{
		PublishCommitted: func(seq uint64, op string, snap *snapshot.View) error {
			manager.Publish(snap)
			*events = append(*events, "publish")
			return nil
		},
	}
	ex := NewBatcher(Config{
		MaxOps:       8,
		StatsEnabled: true,
		Unavailable:  func() error { return nil },

		Bolt:               raw,
		Bucket:             bucket,
		BucketFillPercent:  0.8,
		RejectEmptyPayload: true,
		PublishMu:          &mu,
		Commit:             commit,
		Indexed:            true,
		Ops:                &ops,
		Schema:             rt,
		SnapshotOps:        snapOps,
		IndexPublishOps:    publishOps,
	})
	return ex, raw, bucket
}

func putAttemptPayload(t *testing.T, raw *bbolt.DB, bucket []byte, id uint64, payload []byte) {
	t.Helper()

	err := raw.Update(func(tx *bbolt.Tx) error {
		var keyBuf [8]byte
		return tx.Bucket(bucket).Put(keycodec.DataKeyFromUserKey(id, false).Bytes(false, &keyBuf), payload)
	})
	if err != nil {
		t.Fatalf("put payload: %v", err)
	}
}

func putStringAttemptPayload(t *testing.T, raw *bbolt.DB, bucket []byte, id string, payload []byte) {
	t.Helper()

	err := raw.Update(func(tx *bbolt.Tx) error {
		var keyBuf [8]byte
		key := keycodec.DataKeyFromUserKey(id, true)
		return tx.Bucket(bucket).Put(key.Bytes(true, &keyBuf), payload)
	})
	if err != nil {
		t.Fatalf("put string payload: %v", err)
	}
}

func readAttemptPayload(t *testing.T, raw *bbolt.DB, bucket []byte, id uint64) []byte {
	t.Helper()

	var out []byte
	err := raw.View(func(tx *bbolt.Tx) error {
		var keyBuf [8]byte
		v := tx.Bucket(bucket).Get(keycodec.DataKeyFromUserKey(id, false).Bytes(false, &keyBuf))
		if v != nil {
			out = append(out, v...)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read payload: %v", err)
	}
	return out
}

func readStringAttemptPayload(t *testing.T, raw *bbolt.DB, bucket []byte, id string) []byte {
	t.Helper()

	var out []byte
	err := raw.View(func(tx *bbolt.Tx) error {
		var keyBuf [8]byte
		key := keycodec.DataKeyFromUserKey(id, true)
		v := tx.Bucket(bucket).Get(key.Bytes(true, &keyBuf))
		if v != nil {
			out = append([]byte(nil), v...)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read string payload: %v", err)
	}
	return out
}
