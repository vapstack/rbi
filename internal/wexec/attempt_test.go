package wexec

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"go.etcd.io/bbolt"
)

func TestAttemptCommitFailureDropsStagedSnapshotAndSkipsPublish(t *testing.T) {
	commitErr := errors.New("commit failed")
	var events []string
	var ex *Batcher
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(*bbolt.Tx, string) error {
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
}

func TestAttemptPublishRunsAfterSuccessfulCommit(t *testing.T) {
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
		events = append(events, "commit")
		return tx.Commit()
	})
	ex.indexPublishOps.PublishCommitted = func(seq uint64, op string, snap *snapshot.View) error {
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
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
		events = append(events, "commit")
		return tx.Commit()
	})
	ex.indexPublishOps.PublishCommitted = func(seq uint64, op string, snap *snapshot.View) error {
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

func TestSharedRetrySkipsBeforeCommitFailureAndCommitsRest(t *testing.T) {
	callbackErr := errors.New("callback failed")
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
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
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
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
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
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
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})
	ex.snapshotOps = SnapshotOps{}

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
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
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
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
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

func TestSharedSetValidateIndexFailureCommitsRest(t *testing.T) {
	validateErr := errors.New("validate index failed")
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
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

func TestSharedDeleteBeforeCommitFailureCommitsRest(t *testing.T) {
	callbackErr := errors.New("before commit failed")
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
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
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
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
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
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

func TestSharedPatchBeforeStoreFailureCommitsRest(t *testing.T) {
	hookErr := errors.New("before store failed")
	var events []string
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
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
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
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
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
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
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
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
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
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
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
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
