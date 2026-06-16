package wexec

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"go.etcd.io/bbolt"
)

func TestAttemptCommitFailureDropsStagedSnapshotAndSkipsPublish(t *testing.T) {
	commitErr := errors.New("commit failed")
	var events []string
	var ex *Batcher
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(*bbolt.Tx) error {
		snap, ref, ok := ex.snapshotOps.Manager.PinBySeq(1)
		if !ok || snap == nil || snap.Seq != 1 {
			t.Fatalf("snapshot was not staged before commit: snap=%#v ok=%v", snap, ok)
		}
		ex.snapshotOps.Manager.Unpin(1, ref)
		events = append(events, "commit")
		return commitErr
	})

	req := setAttemptReq(1, 9)
	defer encodePool.Put(req.setPayload)

	retry, done, fatalErr := ex.attempt([]*request{req}, true)
	if retry != nil || !done || fatalErr != nil {
		t.Fatalf("attempt = retry %p done %v fatal %v", retry, done, fatalErr)
	}
	if !errors.Is(req.Err, commitErr) {
		t.Fatalf("request error = %v, want %v", req.Err, commitErr)
	}
	want := []string{"commit"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	if snap, ref, ok := ex.snapshotOps.Manager.PinBySeq(1); ok {
		ex.snapshotOps.Manager.Unpin(1, ref)
		t.Fatalf("staged snapshot remained after failed commit: %#v", snap)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); got != nil {
		t.Fatalf("payload persisted after failed commit: %v", got)
	}
	if st := ex.Stats(); st.TxCommitErrors == 0 {
		t.Fatalf("TxCommitErrors = 0 after failed commit")
	}
}

func TestAttemptPublishRunsAfterSuccessfulCommit(t *testing.T) {
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		events = append(events, "commit")
		return tx.Commit()
	})
	ex.publishCommitted = func(seq uint64, op string, snap *snapshot.View) error {
		if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{9}) {
			t.Fatalf("publish ran before committed payload was visible: %v", got)
		}
		ex.snapshotOps.Manager.Publish(snap)
		events = append(events, "publish")
		return nil
	}

	req := setAttemptReq(1, 9)
	defer encodePool.Put(req.setPayload)

	retry, done, fatalErr := ex.attempt([]*request{req}, true)
	if retry != nil || !done || fatalErr != nil || req.Err != nil {
		t.Fatalf("attempt = retry %p done %v fatal %v reqErr %v", retry, done, fatalErr, req.Err)
	}
	want := []string{"commit", "publish"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestAttemptPublishErrorAssignsRequestAfterCommit(t *testing.T) {
	publishErr := errors.New("publish failed")
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		events = append(events, "commit")
		return tx.Commit()
	})
	ex.publishCommitted = func(seq uint64, op string, snap *snapshot.View) error {
		if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{9}) {
			t.Fatalf("publish ran before committed payload was visible: %v", got)
		}
		events = append(events, "publish")
		return publishErr
	}

	req := setAttemptReq(1, 9)
	defer encodePool.Put(req.setPayload)

	retry, done, fatalErr := ex.attempt([]*request{req}, true)
	if retry != nil || !done || fatalErr != nil {
		t.Fatalf("attempt = retry %p done %v fatal %v", retry, done, fatalErr)
	}
	if !errors.Is(req.Err, publishErr) {
		t.Fatalf("request error = %v, want %v", req.Err, publishErr)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{9}) {
		t.Fatalf("committed payload after publish error = %v, want [9]", got)
	}
	want := []string{"commit", "publish"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestStringSetPrepareUsesRequestPhysicalPayloadBuffer(t *testing.T) {
	var events []string
	ex, raw, bucketName, mapBucketName := newStringAttemptTestExecutor(t, &events, "seed", 1, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	rec := attemptRec{V: 9}
	req, err := ex.buildSetRequest(keycodec.DataKeyFromUserKey("new", true), unsafe.Pointer(&rec), nil, nil, nil)
	if err != nil {
		t.Fatalf("buildSetRequest: %v", err)
	}
	defer requestPool.Put(req)

	tx, err := raw.Begin(true)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	att := attemptStatePool.Get()
	defer attemptStatePool.Put(att)
	att.prepare(tx.Bucket(bucketName), false, ex.ops.Release, 1, true, true, len(ex.schema.Unique))
	att.strmapBucket = tx.Bucket(mapBucketName)
	ex.prepareSet(att, req)
	if req.Err != nil {
		t.Fatalf("prepareSet req error: %v", req.Err)
	}
	if len(att.ownedPayloads) != 0 {
		t.Fatalf("prepareSet created attempt-owned payloads: %d", len(att.ownedPayloads))
	}
	if len(att.prepared) != 1 {
		t.Fatalf("prepared len=%d want 1", len(att.prepared))
	}
	op := att.prepared[0]
	if !reflect.DeepEqual(op.payload, []byte{9}) {
		t.Fatalf("logical payload=%v want [9]", op.payload)
	}
	if len(op.physical) != stringValuePrefixLen+len(op.payload) {
		t.Fatalf("physical len=%d want %d", len(op.physical), stringValuePrefixLen+len(op.payload))
	}
	if &op.payload[0] != &op.physical[stringValuePrefixLen] {
		t.Fatalf("logical payload does not share physical buffer")
	}
}

func TestStringKeyIndexDeleteThenReinsertUsesNewDurableID(t *testing.T) {
	var events []string
	ex, raw, bucketName, mapBucketName := newStringAttemptTestExecutor(t, &events, "user", 1, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	ex.snapshotOps.StrKeyIndex = true
	oldIdx := uint64(1)
	keyMap := indexdata.GetPostingMap()
	keyMap["user"] = (posting.List{}).BuildAdded(oldIdx)
	ex.snapshotOps.Manager.Current().KeyIndex = indexdata.NewRegularFieldStorageFromPostingMapOwned(keyMap)

	delReq := ex.buildDeleteRequest(keycodec.DataKeyFromUserKey("user", true), nil)
	setReq := stringSetAttemptReq("user", 9)
	executeBatchForTest(ex, []*request{delReq, setReq})

	if err := <-delReq.Done; err != nil {
		t.Fatalf("delete request error = %v", err)
	}
	if err := <-setReq.Done; err != nil {
		t.Fatalf("set request error = %v", err)
	}

	var newIdx uint64
	if err := raw.View(func(tx *bbolt.Tx) error {
		v := tx.Bucket(bucketName).Get(keycodec.StringBytes("user"))
		if len(v) < stringValuePrefixLen {
			return errors.New("short string value")
		}
		newIdx = keycodec.U64FromBytes(v[:stringValuePrefixLen])
		return nil
	}); err != nil {
		t.Fatalf("read string idx: %v", err)
	}
	if newIdx == oldIdx {
		t.Fatalf("reinsert reused durable id %d", oldIdx)
	}
	if got := readStringAttemptMap(t, raw, mapBucketName, oldIdx); got != "" {
		t.Fatalf("old string map entry=%q, want empty", got)
	}
	if got := readStringAttemptMap(t, raw, mapBucketName, newIdx); got != "user" {
		t.Fatalf("new string map entry=%q, want user", got)
	}
	ids := indexdata.NewFieldIndexViewFromStorage(ex.snapshotOps.Manager.Current().KeyIndex).LookupPostingRetained("user")
	if ids.Cardinality() != 1 || ids.Contains(oldIdx) || !ids.Contains(newIdx) {
		t.Fatalf("key index cardinality=%d old=%v new=%v", ids.Cardinality(), ids.Contains(oldIdx), ids.Contains(newIdx))
	}
	ids.Release()
}

func TestStringKeyIndexSetThenDeleteSameBatchLeavesNoKeyDelta(t *testing.T) {
	var events []string
	ex, raw, bucketName, mapBucketName := newStringAttemptTestExecutor(t, &events, "seed", 1, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	ex.snapshotOps.StrKeyIndex = true

	setReq := stringSetAttemptReq("ghost", 9)
	delReq := ex.buildDeleteRequest(keycodec.DataKeyFromUserKey("ghost", true), nil)
	executeBatchForTest(ex, []*request{setReq, delReq})

	if err := <-setReq.Done; err != nil {
		t.Fatalf("set request error = %v", err)
	}
	if err := <-delReq.Done; err != nil {
		t.Fatalf("delete request error = %v", err)
	}

	if err := raw.View(func(tx *bbolt.Tx) error {
		if v := tx.Bucket(bucketName).Get(keycodec.StringBytes("ghost")); v != nil {
			return fmt.Errorf("ghost data remained: %x", v)
		}
		c := tx.Bucket(mapBucketName).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if string(v) == "ghost" {
				return fmt.Errorf("ghost reverse map remained at idx %d", keycodec.U64FromBytes(k))
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("read committed state: %v", err)
	}

	ids := indexdata.NewFieldIndexViewFromStorage(ex.snapshotOps.Manager.Current().KeyIndex).LookupPostingRetained("ghost")
	if !ids.IsEmpty() {
		t.Fatalf("ghost key index posting cardinality=%d", ids.Cardinality())
	}
	ids.Release()
}

func TestStringKeyIndexKeyOnlyExistingSetThenDeleteRemovesSnapshotState(t *testing.T) {
	var events []string
	ex, raw, bucketName, mapBucketName := newStringAttemptTestExecutor(t, &events, "user", 1, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	rt, err := schema.Compile(reflect.TypeFor[attemptRec](), schema.Config{Index: map[string]schema.IndexKind{}})
	if err != nil {
		t.Fatalf("Compile key-only schema: %v", err)
	}

	var oldIdx uint64
	if err = raw.View(func(tx *bbolt.Tx) error {
		v := tx.Bucket(bucketName).Get(keycodec.StringBytes("user"))
		if len(v) < stringValuePrefixLen {
			return fmt.Errorf("short string value")
		}
		oldIdx = keycodec.U64FromBytes(v[:stringValuePrefixLen])
		return nil
	}); err != nil {
		t.Fatalf("read old string idx: %v", err)
	}

	seed := attemptRec{V: 1}
	current := snapshot.BuildWithKeyDeltas(1, nil, rt, snapshot.CacheConfig{}, rt.Patch.Fields, []snapshot.BatchEntry{
		{ID: oldIdx, New: unsafe.Pointer(&seed)},
	}, []snapshot.KeyDelta{{ID: oldIdx, Key: "user", Add: true}})
	ex.snapshotOps.Manager.Publish(current)
	ex.schema = rt
	ex.unique = UniqueContext{}
	ex.snapshotOps.Schema = rt
	ex.snapshotOps.PatchFields = rt.Patch.Fields
	ex.snapshotOps.StrKeyIndex = true

	setReq := stringSetAttemptReq("user", 9)
	delReq := ex.buildDeleteRequest(keycodec.DataKeyFromUserKey("user", true), nil)
	executeBatchForTest(ex, []*request{setReq, delReq})

	if err := <-setReq.Done; err != nil {
		t.Fatalf("set request error = %v", err)
	}
	if err := <-delReq.Done; err != nil {
		t.Fatalf("delete request error = %v", err)
	}

	if err = raw.View(func(tx *bbolt.Tx) error {
		if v := tx.Bucket(bucketName).Get(keycodec.StringBytes("user")); v != nil {
			return fmt.Errorf("user data remained: %x", v)
		}
		var mapKey [8]byte
		if v := tx.Bucket(mapBucketName).Get(keycodec.U64BytesWithBuf(oldIdx, &mapKey)); v != nil {
			return fmt.Errorf("user reverse map remained: %q", v)
		}
		return nil
	}); err != nil {
		t.Fatalf("read committed state: %v", err)
	}

	snap := ex.snapshotOps.Manager.Current()
	if got := snap.Universe.Cardinality(); got != 0 {
		t.Fatalf("snapshot universe cardinality=%d want 0", got)
	}
	ids := indexdata.NewFieldIndexViewFromStorage(snap.KeyIndex).LookupPostingRetained("user")
	if !ids.IsEmpty() {
		t.Fatalf("user key index posting cardinality=%d", ids.Cardinality())
	}
	ids.Release()
}

func TestSharedRetrySkipsBeforeCommitFailureAndCommitsRest(t *testing.T) {
	callbackErr := errors.New("callback failed")
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	ex.snapshotOps = SnapshotOps{}

	badReq := setAttemptReq(1, 1)
	badReq.beforeCommit = []BeforeCommitHook{
		func(*bbolt.Tx, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error {
			return callbackErr
		},
	}

	goodReq := setAttemptReq(2, 2)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := <-badReq.Done; !errors.Is(err, callbackErr) {
		t.Fatalf("bad request error = %v, want callback error", err)
	}
	if err := <-goodReq.Done; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); got != nil {
		t.Fatalf("bad request payload persisted: %v", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{2}) {
		t.Fatalf("good request payload = %v, want [2]", got)
	}
}

func TestSharedSetBeforeStoreFailureCommitsRest(t *testing.T) {
	hookErr := errors.New("before store failed")
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	ex.snapshotOps = SnapshotOps{}

	badReq := setAttemptReq(1, 1)
	badReq.beforeStore = []BeforeStoreHook{
		func(keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error {
			return hookErr
		},
	}
	goodReq := setAttemptReq(2, 2)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := <-badReq.Done; !errors.Is(err, hookErr) {
		t.Fatalf("bad request error = %v, want before store error", err)
	}
	if err := <-goodReq.Done; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); got != nil {
		t.Fatalf("bad request payload persisted: %v", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{2}) {
		t.Fatalf("good request payload = %v, want [2]", got)
	}
}

func TestSharedSetBeforeStoreRestartsFromPayloadOnRetry(t *testing.T) {
	callbackErr := errors.New("before commit failed")
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	ex.snapshotOps = SnapshotOps{}

	goodReq := setAttemptReq(1, 10)
	beforeStoreCalls := 0
	goodReq.beforeStore = []BeforeStoreHook{
		func(_ keycodec.DataKey, _, newValue unsafe.Pointer) error {
			beforeStoreCalls++
			(*attemptRec)(newValue).V++
			return nil
		},
	}
	badReq := setAttemptReq(2, 20)
	badReq.beforeCommit = []BeforeCommitHook{
		func(*bbolt.Tx, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error {
			return callbackErr
		},
	}

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := <-badReq.Done; !errors.Is(err, callbackErr) {
		t.Fatalf("bad request error = %v, want callback error", err)
	}
	if err := <-goodReq.Done; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if beforeStoreCalls != 2 {
		t.Fatalf("BeforeStore calls = %d, want 2", beforeStoreCalls)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{11}) {
		t.Fatalf("good request payload = %v, want [11]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); got != nil {
		t.Fatalf("bad request payload persisted: %v", got)
	}
}

func TestSharedSetCloneValueRestartsFromBaselineOnRetry(t *testing.T) {
	callbackErr := errors.New("before commit failed")
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	ex.snapshotOps = SnapshotOps{}
	released := 0
	ex.ops.Release = func(ptr unsafe.Pointer) {
		released++
		(*attemptRec)(ptr).V = 0
	}

	goodBaseline := &attemptRec{V: 10}
	goodReq := cloneSetAttemptReq(1, goodBaseline)
	goodBeforeStoreCalls := 0
	goodReq.beforeStore = []BeforeStoreHook{
		func(_ keycodec.DataKey, _, newValue unsafe.Pointer) error {
			goodBeforeStoreCalls++
			(*attemptRec)(newValue).V++
			return nil
		},
	}

	badBaseline := &attemptRec{V: 20}
	badReq := cloneSetAttemptReq(2, badBaseline)
	badReq.beforeStore = []BeforeStoreHook{
		func(_ keycodec.DataKey, _, newValue unsafe.Pointer) error {
			(*attemptRec)(newValue).V++
			return nil
		},
	}
	badReq.beforeCommit = []BeforeCommitHook{
		func(*bbolt.Tx, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error {
			return callbackErr
		},
	}

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := <-badReq.Done; !errors.Is(err, callbackErr) {
		t.Fatalf("bad request error = %v, want callback error", err)
	}
	if err := <-goodReq.Done; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if goodBeforeStoreCalls != 2 {
		t.Fatalf("good BeforeStore calls = %d, want 2", goodBeforeStoreCalls)
	}
	if released != 3 {
		t.Fatalf("released clone values = %d, want 3", released)
	}
	if goodBaseline.V != 10 {
		t.Fatalf("good baseline mutated: %v", goodBaseline.V)
	}
	if badBaseline.V != 20 {
		t.Fatalf("bad baseline mutated: %v", badBaseline.V)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{11}) {
		t.Fatalf("good request payload = %v, want [11]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); got != nil {
		t.Fatalf("bad request payload persisted: %v", got)
	}
}

func TestSharedSetDecodePreparedValueFailureCommitsRest(t *testing.T) {
	decodeErr := errors.New("decode failed")
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	ex.snapshotOps = SnapshotOps{}
	origDecode := ex.ops.Decode
	ex.ops.Decode = func(data []byte) (unsafe.Pointer, error) {
		if len(data) == 1 && data[0] == 0xc1 {
			return nil, decodeErr
		}
		return origDecode(data)
	}

	badReq := setAttemptReq(1, 1)
	badReq.beforeStore = []BeforeStoreHook{
		func(keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error {
			return nil
		},
	}
	badReq.setPayload.Reset()
	_ = badReq.setPayload.WriteByte(0xc1)
	goodReq := setAttemptReq(2, 2)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := <-badReq.Done; !errors.Is(err, decodeErr) {
		t.Fatalf("bad request error = %v, want decode error", err)
	}
	if err := <-goodReq.Done; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); got != nil {
		t.Fatalf("bad request payload persisted: %v", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{2}) {
		t.Fatalf("good request payload = %v, want [2]", got)
	}
}

func TestSharedSetEmptyPayloadCommitsRest(t *testing.T) {
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	ex.snapshotOps = SnapshotOps{}

	badReq := setAttemptReq(1, 1)
	badReq.setPayload.Reset()
	goodReq := setAttemptReq(2, 2)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := <-badReq.Done; err == nil || !strings.Contains(err.Error(), "empty msgpack payload") {
		t.Fatalf("bad request error = %v, want empty payload error", err)
	}
	if err := <-goodReq.Done; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); got != nil {
		t.Fatalf("bad request payload persisted: %v", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{2}) {
		t.Fatalf("good request payload = %v, want [2]", got)
	}
}

func TestAtomicSetEmptyPayloadThenPatchSameBatch(t *testing.T) {
	var events []string
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	ex.rejectEmptyPayload = false
	ex.ops.Encode = func(ptr unsafe.Pointer, buf *bytes.Buffer) error {
		v := (*attemptRec)(ptr).V
		if v != 0 {
			_ = buf.WriteByte(v)
		}
		return nil
	}
	ex.ops.Decode = func(data []byte) (unsafe.Pointer, error) {
		if len(data) == 0 {
			return unsafe.Pointer(&attemptRec{}), nil
		}
		return unsafe.Pointer(&attemptRec{V: data[0]}), nil
	}

	rec := attemptRec{}
	setReq, err := ex.buildSetRequest(keycodec.DataKeyFromUserKey(uint64(1), false), unsafe.Pointer(&rec), nil, nil, nil)
	if err != nil {
		t.Fatalf("buildSetRequest: %v", err)
	}
	defer ex.releaseRequest(setReq)
	// A pooled empty buffer may keep capacity; this forces the nil-backed empty payload path.
	*setReq.setPayload = bytes.Buffer{}
	patchReq := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: byte(7)}}, true)

	ex.runAtomic([]*request{setReq, patchReq})

	if setReq.Err != nil {
		t.Fatalf("set request error = %v", setReq.Err)
	}
	if patchReq.Err != nil {
		t.Fatalf("patch request error = %v", patchReq.Err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{7}) {
		t.Fatalf("payload after set+patch = %v, want [7]", got)
	}
}

func TestSharedSetValidateIndexFailureCommitsRest(t *testing.T) {
	validateErr := errors.New("validate index failed")
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	ex.snapshotOps = SnapshotOps{}
	ex.ops.ValidateIndex = func(ptr unsafe.Pointer) error {
		if (*attemptRec)(ptr).V == 1 {
			return validateErr
		}
		return nil
	}

	badReq := setAttemptReq(1, 1)
	goodReq := setAttemptReq(2, 2)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := <-badReq.Done; !errors.Is(err, validateErr) {
		t.Fatalf("bad request error = %v, want validate error", err)
	}
	if err := <-goodReq.Done; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); got != nil {
		t.Fatalf("bad request payload persisted: %v", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{2}) {
		t.Fatalf("good request payload = %v, want [2]", got)
	}
}

func TestSharedSetExistingDecodeFailureCommitsRest(t *testing.T) {
	decodeErr := errors.New("decode failed")
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	ex.snapshotOps = SnapshotOps{}
	origDecode := ex.ops.Decode
	ex.ops.Decode = func(data []byte) (unsafe.Pointer, error) {
		if len(data) == 1 && data[0] == 0xc1 {
			return nil, decodeErr
		}
		return origDecode(data)
	}
	putAttemptPayload(t, raw, bucket, 1, []byte{0xc1})

	badReq := setAttemptReq(1, 9)
	goodReq := setAttemptReq(2, 2)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := <-badReq.Done; !errors.Is(err, decodeErr) {
		t.Fatalf("bad request error = %v, want decode error", err)
	}
	if err := <-goodReq.Done; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{0xc1}) {
		t.Fatalf("bad request payload = %v, want [193]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{2}) {
		t.Fatalf("good request payload = %v, want [2]", got)
	}
}

func TestSharedSetBeforeStoreEncodeFailureCommitsRest(t *testing.T) {
	encodeErr := errors.New("encode failed")
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	ex.snapshotOps = SnapshotOps{}
	origEncode := ex.ops.Encode
	ex.ops.Encode = func(ptr unsafe.Pointer, buf *bytes.Buffer) error {
		if (*attemptRec)(ptr).V == 99 {
			return encodeErr
		}
		return origEncode(ptr, buf)
	}

	badReq := setAttemptReq(1, 1)
	badReq.beforeStore = []BeforeStoreHook{
		func(_ keycodec.DataKey, _, newValue unsafe.Pointer) error {
			(*attemptRec)(newValue).V = 99
			return nil
		},
	}
	goodReq := setAttemptReq(2, 2)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := <-badReq.Done; !errors.Is(err, encodeErr) {
		t.Fatalf("bad request error = %v, want encode error", err)
	}
	if err := <-goodReq.Done; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); got != nil {
		t.Fatalf("bad request payload persisted: %v", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{2}) {
		t.Fatalf("good request payload = %v, want [2]", got)
	}
}

func TestSharedDeleteBeforeCommitFailureCommitsRest(t *testing.T) {
	callbackErr := errors.New("before commit failed")
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	ex.snapshotOps = SnapshotOps{}
	putAttemptPayload(t, raw, bucket, 1, []byte{1})
	putAttemptPayload(t, raw, bucket, 2, []byte{2})

	badReq := deleteAttemptReq(1)
	badReq.beforeCommit = []BeforeCommitHook{
		func(*bbolt.Tx, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error {
			return callbackErr
		},
	}
	goodReq := deleteAttemptReq(2)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := <-badReq.Done; !errors.Is(err, callbackErr) {
		t.Fatalf("bad request error = %v, want callback error", err)
	}
	if err := <-goodReq.Done; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{1}) {
		t.Fatalf("bad request payload = %v, want [1]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); got != nil {
		t.Fatalf("good request payload still present: %v", got)
	}
}

func TestSharedDeleteDecodeFailureCommitsRest(t *testing.T) {
	decodeErr := errors.New("decode failed")
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	ex.snapshotOps = SnapshotOps{}
	origDecode := ex.ops.Decode
	ex.ops.Decode = func(data []byte) (unsafe.Pointer, error) {
		if len(data) == 1 && data[0] == 0xc1 {
			return nil, decodeErr
		}
		return origDecode(data)
	}
	putAttemptPayload(t, raw, bucket, 1, []byte{0xc1})
	putAttemptPayload(t, raw, bucket, 2, []byte{2})

	badReq := deleteAttemptReq(1)
	goodReq := deleteAttemptReq(2)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := <-badReq.Done; !errors.Is(err, decodeErr) {
		t.Fatalf("bad request error = %v, want decode error", err)
	}
	if err := <-goodReq.Done; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{0xc1}) {
		t.Fatalf("bad request payload = %v, want [193]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); got != nil {
		t.Fatalf("good request payload still present: %v", got)
	}
}

func TestSharedPatchBeforeProcessFailureCommitsRest(t *testing.T) {
	hookErr := errors.New("before process failed")
	var events []string
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{10})
	putAttemptPayload(t, raw, bucket, 2, []byte{20})

	badReq := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: byte(55)}}, true)
	badReq.beforeProcess = []BeforeProcessHook{
		func(keycodec.DataKey, unsafe.Pointer) error {
			return hookErr
		},
	}
	goodReq := patchAttemptReq(2, []schema.PatchItem{{Name: "v", Value: byte(66)}}, true)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := <-badReq.Done; !errors.Is(err, hookErr) {
		t.Fatalf("bad request error = %v, want before process error", err)
	}
	if err := <-goodReq.Done; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{10}) {
		t.Fatalf("bad request payload = %v, want [10]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{66}) {
		t.Fatalf("good request payload = %v, want [66]", got)
	}
}

func TestSharedPatchExistingDecodeFailureCommitsRest(t *testing.T) {
	decodeErr := errors.New("decode failed")
	var events []string
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	origDecode := ex.ops.Decode
	ex.ops.Decode = func(data []byte) (unsafe.Pointer, error) {
		if len(data) == 1 && data[0] == 0xc1 {
			return nil, decodeErr
		}
		return origDecode(data)
	}
	putAttemptPayload(t, raw, bucket, 1, []byte{0xc1})

	badReq := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: byte(55)}}, true)
	goodReq := setAttemptReq(2, 2)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := <-badReq.Done; !errors.Is(err, decodeErr) {
		t.Fatalf("bad request error = %v, want decode error", err)
	}
	if err := <-goodReq.Done; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{0xc1}) {
		t.Fatalf("bad request payload = %v, want [193]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{2}) {
		t.Fatalf("good request payload = %v, want [2]", got)
	}
}

func TestLoadStateDecodeErrorClearsDiscardedStateSlot(t *testing.T) {
	decodeErr := errors.New("decode failed")
	var events []string
	ex, raw, bucket, mapBucket := newStringAttemptTestExecutor(t, &events, "seed", 1, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	ex.ops.Decode = func(data []byte) (unsafe.Pointer, error) {
		if len(data) == 1 && data[0] == 0xc1 {
			return nil, decodeErr
		}
		return unsafe.Pointer(&attemptRec{V: data[0]}), nil
	}
	putStringAttemptPayload(t, raw, bucket, mapBucket, "bad", []byte{0xc1})

	err := raw.View(func(tx *bbolt.Tx) error {
		st := attemptState{
			dataBucket: tx.Bucket(bucket),
			states:     make([]recordState, 0, 1),
		}
		req := request{id: keycodec.DataKeyFromUserKey("bad", true)}
		if _, err := ex.loadState(&st, &req, true, true); !errors.Is(err, decodeErr) {
			return fmt.Errorf("loadState error = %v, want decode error", err)
		}
		if len(st.states) != 0 {
			return fmt.Errorf("states len = %d, want 0", len(st.states))
		}
		state := st.states[:cap(st.states)][0]
		if state.key != nil || state.value != nil || state.ownedPayload != nil || state.borrowedPayload != nil {
			return fmt.Errorf("discarded state kept references: %+v", state)
		}
		if state.idx != 0 || state.idxKnown || state.idxNew || state.exists || state.payloadOff != 0 || state.payloadKnown {
			return fmt.Errorf("discarded state kept scalar state: %+v", state)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSharedPatchBeforeStoreFailureCommitsRest(t *testing.T) {
	hookErr := errors.New("before store failed")
	var events []string
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{10})
	putAttemptPayload(t, raw, bucket, 2, []byte{20})

	badReq := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: byte(55)}}, true)
	badReq.beforeStore = []BeforeStoreHook{
		func(keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error {
			return hookErr
		},
	}
	goodReq := patchAttemptReq(2, []schema.PatchItem{{Name: "v", Value: byte(66)}}, true)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := <-badReq.Done; !errors.Is(err, hookErr) {
		t.Fatalf("bad request error = %v, want before store error", err)
	}
	if err := <-goodReq.Done; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{10}) {
		t.Fatalf("bad request payload = %v, want [10]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{66}) {
		t.Fatalf("good request payload = %v, want [66]", got)
	}
}

func TestSharedPatchStrictUnknownFieldFailureCommitsRest(t *testing.T) {
	var events []string
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{10})
	putAttemptPayload(t, raw, bucket, 2, []byte{20})

	badReq := patchAttemptReq(1, []schema.PatchItem{{Name: "missing", Value: byte(55)}}, false)
	goodReq := patchAttemptReq(2, []schema.PatchItem{{Name: "v", Value: byte(66)}}, true)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := <-badReq.Done; err == nil || !strings.Contains(err.Error(), "cannot patch field") {
		t.Fatalf("bad request error = %v, want strict patch error", err)
	}
	if err := <-goodReq.Done; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{10}) {
		t.Fatalf("bad request payload = %v, want [10]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{66}) {
		t.Fatalf("good request payload = %v, want [66]", got)
	}
}

func TestSharedPatchMissingTargetCommitsRest(t *testing.T) {
	var events []string
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})

	missingReq := patchAttemptReq(999, []schema.PatchItem{{Name: "v", Value: byte(55)}}, true)
	goodReq := setAttemptReq(2, 2)

	executeBatchForTest(ex, []*request{missingReq, goodReq})

	if err := <-missingReq.Done; err != nil {
		t.Fatalf("missing request error = %v", err)
	}
	if err := <-goodReq.Done; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 999); got != nil {
		t.Fatalf("missing patch created payload: %v", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{2}) {
		t.Fatalf("good request payload = %v, want [2]", got)
	}
}

func TestSharedPatchApplyFailureCommitsRest(t *testing.T) {
	var events []string
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{10})

	badReq := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: "not-byte"}}, true)
	goodReq := setAttemptReq(2, 2)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := <-badReq.Done; err == nil || !strings.Contains(err.Error(), "failed to apply patch") {
		t.Fatalf("bad request error = %v, want apply patch error", err)
	}
	if err := <-goodReq.Done; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{10}) {
		t.Fatalf("bad request payload = %v, want [10]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{2}) {
		t.Fatalf("good request payload = %v, want [2]", got)
	}
}

func TestSharedPatchMissingTargetSkipsBeforeCommit(t *testing.T) {
	var events []string
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 2, []byte{20})

	var calls []uint64
	hook := func(_ *bbolt.Tx, key keycodec.DataKey, _, _ unsafe.Pointer) error {
		calls = append(calls, key.Uint())
		return nil
	}
	missingReq := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: byte(55)}}, true)
	missingReq.beforeCommit = []BeforeCommitHook{hook}
	presentReq := patchAttemptReq(2, []schema.PatchItem{{Name: "v", Value: byte(99)}}, true)
	presentReq.beforeCommit = []BeforeCommitHook{hook}

	executeBatchForTest(ex, []*request{missingReq, presentReq})

	if err := <-missingReq.Done; err != nil {
		t.Fatalf("missing request error = %v", err)
	}
	if err := <-presentReq.Done; err != nil {
		t.Fatalf("present request error = %v", err)
	}
	if !reflect.DeepEqual(calls, []uint64{2}) {
		t.Fatalf("BeforeCommit keys = %v, want [2]", calls)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{99}) {
		t.Fatalf("present request payload = %v, want [99]", got)
	}
}

func TestSharedDeleteMissingTargetSkipsBeforeCommit(t *testing.T) {
	var events []string
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 2, []byte{20})

	var calls []uint64
	hook := func(_ *bbolt.Tx, key keycodec.DataKey, _, _ unsafe.Pointer) error {
		calls = append(calls, key.Uint())
		return nil
	}
	missingReq := deleteAttemptReq(1)
	missingReq.beforeCommit = []BeforeCommitHook{hook}
	presentReq := deleteAttemptReq(2)
	presentReq.beforeCommit = []BeforeCommitHook{hook}

	executeBatchForTest(ex, []*request{missingReq, presentReq})

	if err := <-missingReq.Done; err != nil {
		t.Fatalf("missing request error = %v", err)
	}
	if err := <-presentReq.Done; err != nil {
		t.Fatalf("present request error = %v", err)
	}
	if !reflect.DeepEqual(calls, []uint64{2}) {
		t.Fatalf("BeforeCommit keys = %v, want [2]", calls)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); got != nil {
		t.Fatalf("present request payload = %v, want nil", got)
	}
}
