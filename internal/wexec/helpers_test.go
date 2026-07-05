package wexec

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"go.etcd.io/bbolt"
)

type attemptRec struct {
	V byte `db:"v" rbi:"unique"`
}

type attemptPairRec struct {
	A byte `db:"a"`
	B byte `db:"b"`
}

var testSnapshotManagers sync.Map
var testCommitFns sync.Map
var testPublishFns sync.Map
var testBoltDBs sync.Map

type testSnapshotManager struct {
	mu      sync.RWMutex
	current *snapshot.View
}

func newTestSnapshotManager() *testSnapshotManager {
	return &testSnapshotManager{}
}

func (m *testSnapshotManager) Current() *snapshot.View {
	m.mu.RLock()
	current := m.current
	m.mu.RUnlock()
	return current
}

func (m *testSnapshotManager) Publish(snap *snapshot.View) {
	m.mu.Lock()
	m.current = snap
	m.mu.Unlock()
}

func setSnapshotManagerForTest(ex *Executor, manager *testSnapshotManager) {
	testSnapshotManagers.Store(ex, manager)
}

func snapshotManagerForTest(ex *Executor) *testSnapshotManager {
	v, _ := testSnapshotManagers.Load(ex)
	return v.(*testSnapshotManager)
}

func executeBatchForTest(ex *Executor, batch []*request) {
	for i := 0; i < len(batch); i++ {
		batch[i].Err = nil
	}

	tx, err := boltForTest(ex).Begin(true)
	if err != nil {
		assignRequestErr(batch, fmt.Errorf("tx error: %w", err))
		finishRequestsForTest(ex, batch)
		return
	}
	attempt, err := ex.NewFrameAttempt(tx, len(batch))
	if err != nil {
		_ = tx.Rollback()
		assignRequestErr(batch, err)
		finishRequestsForTest(ex, batch)
		return
	}

	var fatal error
	for i := range batch {
		reqs := requestScratchPool.Get(1)
		reqs = append(reqs, *batch[i])
		batch[i].setBaseline = nil
		batch[i].setPayload = nil
		batch[i].patch = nil
		ops := Batch{ex: ex, reqs: reqs}
		if _, err = attempt.Prepare(&ops, nil, nil); err != nil {
			attempt.DiscardCurrent()
			batch[i].Err = err
			if rootWriteBatchWideErrForTest(err) {
				assignRequestErr(batch, err)
				fatal = err
				break
			}
			continue
		}
		if err = attempt.ValidateCurrent(); err != nil {
			attempt.DiscardCurrent()
			batch[i].Err = err
			continue
		}
		attempt.AcceptValidatedCurrent()
	}
	if fatal != nil {
		_ = tx.Rollback()
		attempt.Cancel()
		finishRequestsForTest(ex, batch)
		return
	}

	applied, err := attempt.Apply()
	if err != nil {
		_ = tx.Rollback()
		attempt.Cancel()
		assignRequestErr(batch, err)
		finishRequestsForTest(ex, batch)
		return
	}
	if applied.Seq == 0 {
		_ = tx.Rollback()
		attempt.Cancel()
		finishRequestsForTest(ex, batch)
		return
	}
	if err = finishAppliedForTest(ex, tx, applied); err != nil {
		assignRequestErr(batch, err)
	}
	finishRequestsForTest(ex, batch)
}

func executeAtomicRequestsForTest(ex *Executor, batch []*request) {
	for i := 0; i < len(batch); i++ {
		batch[i].Err = nil
	}
	tx, err := boltForTest(ex).Begin(true)
	if err != nil {
		assignRequestErr(batch, fmt.Errorf("tx error: %w", err))
		return
	}
	active := requestScratchPool.Get(len(batch))[:len(batch)]
	for i := range batch {
		active[i] = *batch[i]
	}
	applied, err := ex.applyAtomic(tx, active)
	if err != nil {
		_ = tx.Rollback()
		for i := range active {
			batch[i].Err = active[i].Err
		}
		requestScratchPool.Put(active)
		assignRequestErr(batch, err)
		return
	}
	if applied.Seq == 0 {
		_ = tx.Rollback()
		for i := range active {
			batch[i].Err = active[i].Err
		}
		requestScratchPool.Put(active)
		return
	}
	_ = finishAppliedForTest(ex, tx, applied)
	for i := range active {
		batch[i].Err = active[i].Err
	}
	requestScratchPool.Put(active)
}

func finishAppliedForTest(ex *Executor, tx *bbolt.Tx, applied AppliedBatch) error {
	if err := commitForTest(ex, tx); err != nil {
		_ = tx.Rollback()
		assignPreparedErr(applied.cleanup.att.accepted, err)
		if applied.Snapshot != nil {
			applied.Snapshot.Release()
		}
		applied.Release()
		return err
	}
	if err := publishForTest(ex, applied.Snapshot); err != nil {
		assignPreparedErr(applied.cleanup.att.accepted, err)
		applied.Release()
		return err
	}
	applied.Release()
	return nil
}

func publishForTest(ex *Executor, snap *snapshot.View) error {
	v, _ := testPublishFns.Load(ex)
	return v.(func(*snapshot.View) error)(snap)
}

func commitForTest(ex *Executor, tx *bbolt.Tx) error {
	v, _ := testCommitFns.Load(ex)
	return v.(func(*bbolt.Tx) error)(tx)
}

func boltForTest(ex *Executor) *bbolt.DB {
	v, _ := testBoltDBs.Load(ex)
	return v.(*bbolt.DB)
}

func finishRequestsForTest(ex *Executor, batch []*request) {
	for _, req := range batch {
		err := req.Err
		ex.releaseRequest(req)
		req.Err = err
	}
}

func rootWriteBatchWideErrForTest(err error) bool {
	return errors.Is(err, ErrBucketMissing) ||
		errors.Is(err, ErrStringMapBucketMissing) ||
		errors.Is(err, ErrAdvanceBucketSequence) ||
		errors.Is(err, ErrAdvanceStringMapSequence)
}

func assignRequestErr(reqs []*request, err error) {
	for i := 0; i < len(reqs); i++ {
		req := reqs[i]
		if req.Err == nil {
			req.Err = err
		}
	}
}

func setAttemptReq(id uint64, v byte) *request {
	payload := encodePool.Get()
	_ = payload.WriteByte(v)
	return &request{
		op:         opSet,
		id:         keycodec.DataKeyFromUserKey(id, false),
		setPayload: payload,
	}
}

func stringSetAttemptReq(id string, v byte) *request {
	req := setAttemptReq(0, v)
	req.id = keycodec.DataKeyFromUserKey(id, true)
	req.setPayload.Reset()
	req.payloadOff = reserveStringValuePrefix(req.setPayload, true)
	_ = req.setPayload.WriteByte(v)
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
	}
}

func deleteAttemptReq(id uint64) *request {
	return &request{
		op: opDelete,
		id: keycodec.DataKeyFromUserKey(id, false),
	}
}

func patchAttemptReq(id uint64, patch []schema.PatchItem, ignoreUnknown bool) *request {
	return &request{
		op:                 opPatch,
		id:                 keycodec.DataKeyFromUserKey(id, false),
		patch:              patch,
		patchIgnoreUnknown: ignoreUnknown,
	}
}

func newPatchAttemptTestExecutor(t *testing.T, events *[]string, commit func(*bbolt.Tx) error) (*Executor, *bbolt.DB, []byte) {
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

func newTransparentPatchAttemptTestExecutor(t *testing.T, events *[]string, commit func(*bbolt.Tx) error) (*Executor, *bbolt.DB, []byte) {
	t.Helper()

	ex, raw, bucket := newAttemptTestExecutor(t, events, commit)
	rt, err := schema.Compile(reflect.TypeFor[attemptRec](), schema.Config{Index: map[string]schema.IndexKind{}})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	ex.schema = rt
	ex.indexed = false
	ex.unique = UniqueContext{}
	ex.snapshotOps = SnapshotOps{}
	return ex, raw, bucket
}

func newPairAttemptTestExecutor(t *testing.T, events *[]string, commit func(*bbolt.Tx) error) (*Executor, *bbolt.DB, []byte) {
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

func newUniqueAttemptTestExecutor(t *testing.T, events *[]string, seed []snapshot.BatchEntry, commit func(*bbolt.Tx) error) (*Executor, *bbolt.DB, []byte) {
	t.Helper()

	ex, raw, bucket := newAttemptTestExecutor(t, events, commit)
	rt, err := schema.Compile(reflect.TypeFor[attemptRec](), schema.Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	current := snapshot.NewView(0, nil, rt, snapshot.CacheConfig{}, snapshot.Storage{})
	if len(seed) != 0 {
		current = snapshot.Build(1, nil, rt, snapshot.CacheConfig{}, rt.Patch.Fields, seed)
	}
	manager := newTestSnapshotManager()
	manager.Publish(current)
	ex.schema = rt
	ex.indexed = true
	ex.unique = UniqueContext{
		Schema:  rt,
		Current: manager.Current,
	}
	ex.snapshotOps = SnapshotOps{
		Current:     manager.Current,
		Schema:      rt,
		CacheConfig: snapshot.CacheConfig{},
		PatchFields: rt.Patch.Fields,
	}
	setSnapshotManagerForTest(ex, manager)
	return ex, raw, bucket
}

func newStringAttemptTestExecutor(t *testing.T, events *[]string, seedKey string, seedValue byte, commit func(*bbolt.Tx) error) (*Executor, *bbolt.DB, []byte, []byte) {
	t.Helper()

	ex, raw, bucket := newAttemptTestExecutor(t, events, commit)
	rt, err := schema.Compile(reflect.TypeFor[attemptRec](), schema.Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	mapBucket := createStringAttemptMap(t, raw, bucket)
	seed := attemptRec{V: seedValue}
	seedIdx := putStringAttemptPayload(t, raw, bucket, mapBucket, seedKey, []byte{seedValue})
	current := snapshot.Build(1, nil, rt, snapshot.CacheConfig{}, rt.Patch.Fields, []snapshot.BatchEntry{
		{ID: seedIdx, New: unsafe.Pointer(&seed)},
	})
	manager := newTestSnapshotManager()
	manager.Publish(current)

	ex.strKey = true
	ex.strmapBucket = mapBucket
	ex.schema = rt
	ex.indexed = true
	ex.unique = UniqueContext{
		Schema:  rt,
		Current: manager.Current,
	}
	ex.snapshotOps = SnapshotOps{
		Current:     manager.Current,
		Schema:      rt,
		CacheConfig: snapshot.CacheConfig{},
		PatchFields: rt.Patch.Fields,
	}
	setSnapshotManagerForTest(ex, manager)
	return ex, raw, bucket, mapBucket
}

func newAttemptTestExecutor(t *testing.T, events *[]string, commit func(*bbolt.Tx) error) (*Executor, *bbolt.DB, []byte) {
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
	manager := newTestSnapshotManager()
	snapOps := SnapshotOps{
		Current:     manager.Current,
		Schema:      rt,
		CacheConfig: snapshot.CacheConfig{},
		PatchFields: rt.Patch.Fields,
	}
	var ex *Executor
	publish := func(snap *snapshot.View) error {
		snapshotManagerForTest(ex).Publish(snap)
		*events = append(*events, "publish")
		return nil
	}
	ex = NewExecutor(Config{
		StatsEnabled: true,

		DataBucket:         bucket,
		BucketFillPercent:  0.8,
		RejectEmptyPayload: true,
		Indexed:            true,
		Ops:                &ops,
		Schema:             rt,
		SnapshotOps:        snapOps,
	})
	setSnapshotManagerForTest(ex, manager)
	testCommitFns.Store(ex, commit)
	testPublishFns.Store(ex, publish)
	testBoltDBs.Store(ex, raw)
	t.Cleanup(func() {
		testCommitFns.Delete(ex)
		testPublishFns.Delete(ex)
		testBoltDBs.Delete(ex)
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

func putStringAttemptPayload(t *testing.T, raw *bbolt.DB, bucket []byte, mapBucket []byte, id string, payload []byte) uint64 {
	t.Helper()

	var idx uint64
	err := raw.Update(func(tx *bbolt.Tx) error {
		var keyBuf [8]byte
		key := keycodec.DataKeyFromUserKey(id, true)
		m := tx.Bucket(mapBucket)
		var err error
		idx, err = m.NextSequence()
		if err != nil {
			return err
		}
		var mapKey [8]byte
		if err = m.Put(keycodec.U64BytesWithBuf(idx, &mapKey), key.Bytes(true, &keyBuf)); err != nil {
			return err
		}
		value := keycodec.AppendU64Bytes(nil, idx)
		value = append(value, payload...)
		return tx.Bucket(bucket).Put(key.Bytes(true, &keyBuf), value)
	})
	if err != nil {
		t.Fatalf("put string payload: %v", err)
	}
	return idx
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
			if len(v) < 8 {
				return errors.New("short string value")
			}
			out = append([]byte(nil), v[8:]...)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read string payload: %v", err)
	}
	return out
}

func createStringAttemptMap(t testing.TB, raw *bbolt.DB, bucket []byte) []byte {
	t.Helper()

	mapBucket := bucketMapName(bucket)
	err := raw.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucket(mapBucket)
		return err
	})
	if err != nil {
		t.Fatalf("create string map: %v", err)
	}
	return mapBucket
}

func bucketMapName(bucket []byte) []byte {
	return append(append([]byte(nil), bucket...), ".rbimap"...)
}

func stringAttemptMapSequence(t *testing.T, raw *bbolt.DB, mapBucket []byte) uint64 {
	t.Helper()

	var seq uint64
	err := raw.View(func(tx *bbolt.Tx) error {
		seq = tx.Bucket(mapBucket).Sequence()
		return nil
	})
	if err != nil {
		t.Fatalf("read string map sequence: %v", err)
	}
	return seq
}

func readStringAttemptMap(t *testing.T, raw *bbolt.DB, mapBucket []byte, idx uint64) string {
	t.Helper()

	var out string
	err := raw.View(func(tx *bbolt.Tx) error {
		var key [8]byte
		v := tx.Bucket(mapBucket).Get(keycodec.U64BytesWithBuf(idx, &key))
		if v != nil {
			out = string(v)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read string map: %v", err)
	}
	return out
}
