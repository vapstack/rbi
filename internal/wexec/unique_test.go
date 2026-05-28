package wexec

import (
	"errors"
	"reflect"
	"testing"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"go.etcd.io/bbolt"
)

func TestSharedSetUniqueRejectRecheckedAfterRetry(t *testing.T) {
	callbackErr := errors.New("before commit failed")
	uniqueErr := errors.New("unique violation")
	var events []string
	ex, raw, bucket := newUniqueAttemptTestExecutor(t, &events, uniqueErr, nil, func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})

	badReq := setAttemptReq(1, 7)
	badReq.beforeCommit = []BeforeCommitHook{
		func(*bbolt.Tx, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error {
			return callbackErr
		},
	}
	goodReq := setAttemptReq(2, 7)

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
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{7}) {
		t.Fatalf("good request payload = %v, want [7]", got)
	}
}

func TestSharedSetUniqueRejectPreservesAcceptedWritesAndIndex(t *testing.T) {
	uniqueErr := errors.New("unique violation")
	seed := []attemptRec{{V: 1}, {V: 2}, {V: 3}}
	var events []string
	ex, raw, bucket := newUniqueAttemptTestExecutor(t, &events, uniqueErr, []snapshot.BatchEntry{
		{ID: 1, New: unsafe.Pointer(&seed[0])},
		{ID: 2, New: unsafe.Pointer(&seed[1])},
		{ID: 3, New: unsafe.Pointer(&seed[2])},
	}, func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{1})
	putAttemptPayload(t, raw, bucket, 2, []byte{2})
	putAttemptPayload(t, raw, bucket, 3, []byte{3})

	badReq := setAttemptReq(3, 1)
	goodReq := setAttemptReq(2, 4)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := <-badReq.Done; !errors.Is(err, uniqueErr) {
		t.Fatalf("bad request error = %v, want unique error", err)
	}
	if err := <-goodReq.Done; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{4}) {
		t.Fatalf("id=2 payload = %v, want [4]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 3); !reflect.DeepEqual(got, []byte{3}) {
		t.Fatalf("id=3 payload = %v, want [3]", got)
	}

	snap := ex.unique.Current()
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(1)); !ids.Contains(1) || ids.Contains(3) {
		t.Fatalf("unique value 1 ids mismatch")
	}
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(3)); !ids.Contains(3) {
		t.Fatalf("unique value 3 lost id=3")
	}
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(4)); !ids.Contains(2) {
		t.Fatalf("unique value 4 missing id=2")
	}
}

func TestRunAtomicUniqueDeleteThenSetReusesFreedValue(t *testing.T) {
	uniqueErr := errors.New("unique violation")
	seed := []attemptRec{{V: 1}, {V: 2}}
	var events []string
	ex, raw, bucket := newUniqueAttemptTestExecutor(t, &events, uniqueErr, []snapshot.BatchEntry{
		{ID: 1, New: unsafe.Pointer(&seed[0])},
		{ID: 2, New: unsafe.Pointer(&seed[1])},
	}, func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{1})
	putAttemptPayload(t, raw, bucket, 2, []byte{2})

	deleteReq := deleteAttemptReq(1)
	setReq := setAttemptReq(2, 1)
	defer encodePool.Put(setReq.setPayload)

	ex.runAtomic([]*request{deleteReq, setReq})

	if deleteReq.Err != nil {
		t.Fatalf("delete request error = %v", deleteReq.Err)
	}
	if setReq.Err != nil {
		t.Fatalf("set request error = %v", setReq.Err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); got != nil {
		t.Fatalf("id=1 payload after delete = %v", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{1}) {
		t.Fatalf("id=2 payload = %v, want [1]", got)
	}

	snap := ex.unique.Current()
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(1)); ids.Contains(1) || !ids.Contains(2) {
		t.Fatalf("unique value 1 ids mismatch after reuse")
	}
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(2)); ids.Contains(2) {
		t.Fatalf("stale unique value 2 owner remained")
	}
}

func TestRunAtomicUniqueDuplicateIDUsesFinalValue(t *testing.T) {
	uniqueErr := errors.New("unique violation")
	seed := []attemptRec{{V: 1}, {V: 2}}
	var events []string
	ex, raw, bucket := newUniqueAttemptTestExecutor(t, &events, uniqueErr, []snapshot.BatchEntry{
		{ID: 1, New: unsafe.Pointer(&seed[0])},
		{ID: 2, New: unsafe.Pointer(&seed[1])},
	}, func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{1})
	putAttemptPayload(t, raw, bucket, 2, []byte{2})

	transientReq := setAttemptReq(1, 2)
	defer encodePool.Put(transientReq.setPayload)
	finalReq := setAttemptReq(1, 3)
	defer encodePool.Put(finalReq.setPayload)

	ex.runAtomic([]*request{transientReq, finalReq})

	if transientReq.Err != nil {
		t.Fatalf("transient request error = %v", transientReq.Err)
	}
	if finalReq.Err != nil {
		t.Fatalf("final request error = %v", finalReq.Err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{3}) {
		t.Fatalf("id=1 payload = %v, want [3]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{2}) {
		t.Fatalf("id=2 payload = %v, want [2]", got)
	}

	snap := ex.unique.Current()
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(1)); ids.Contains(1) {
		t.Fatalf("stale unique value 1 owner remained")
	}
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(2)); ids.Contains(1) || !ids.Contains(2) {
		t.Fatalf("unique value 2 ids mismatch")
	}
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(3)); !ids.Contains(1) {
		t.Fatalf("unique value 3 missing id=1")
	}
}

func TestSharedPatchUniqueRejectPreservesAcceptedWritesAndIndex(t *testing.T) {
	uniqueErr := errors.New("unique violation")
	seed := []attemptRec{{V: 1}, {V: 2}, {V: 3}}
	var events []string
	ex, raw, bucket := newUniqueAttemptTestExecutor(t, &events, uniqueErr, []snapshot.BatchEntry{
		{ID: 1, New: unsafe.Pointer(&seed[0])},
		{ID: 2, New: unsafe.Pointer(&seed[1])},
		{ID: 3, New: unsafe.Pointer(&seed[2])},
	}, func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{1})
	putAttemptPayload(t, raw, bucket, 2, []byte{2})
	putAttemptPayload(t, raw, bucket, 3, []byte{3})

	badReq := patchAttemptReq(3, []schema.PatchItem{{Name: "v", Value: byte(2)}}, true)
	goodReq := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: byte(4)}}, true)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := <-badReq.Done; !errors.Is(err, uniqueErr) {
		t.Fatalf("bad request error = %v, want unique error", err)
	}
	if err := <-goodReq.Done; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{4}) {
		t.Fatalf("id=1 payload = %v, want [4]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 3); !reflect.DeepEqual(got, []byte{3}) {
		t.Fatalf("id=3 payload = %v, want [3]", got)
	}

	snap := ex.unique.Current()
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(2)); !ids.Contains(2) || ids.Contains(3) {
		t.Fatalf("unique value 2 ids mismatch")
	}
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(3)); !ids.Contains(3) {
		t.Fatalf("unique value 3 lost id=3")
	}
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(4)); !ids.Contains(1) {
		t.Fatalf("unique value 4 missing id=1")
	}
}

func TestSharedStringSetUniqueRejectRollsBackCreatedKey(t *testing.T) {
	uniqueErr := errors.New("unique violation")
	var events []string
	ex, raw, bucket, _ := newStringAttemptTestExecutor(t, &events, "seed", 1, uniqueErr, func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})

	badReq := stringSetAttemptReq("ghost", 1)
	goodReq := stringSetAttemptReq("real", 2)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := <-badReq.Done; !errors.Is(err, uniqueErr) {
		t.Fatalf("bad request error = %v, want unique error", err)
	}
	if err := <-goodReq.Done; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readStringAttemptPayload(t, raw, bucket, "ghost"); got != nil {
		t.Fatalf("ghost payload persisted: %v", got)
	}
	if got := readStringAttemptPayload(t, raw, bucket, "real"); !reflect.DeepEqual(got, []byte{2}) {
		t.Fatalf("real payload = %v, want [2]", got)
	}

	snap := ex.unique.Current()
	if snap.StrMap.Next() != 3 {
		t.Fatalf("strmap next = %d, want 3", snap.StrMap.Next())
	}
	if idx, ok := snap.StrMap.Index("seed"); !ok || idx != 1 {
		t.Fatalf("seed idx = %d ok=%v, want 1", idx, ok)
	}
	if _, ok := snap.StrMap.Index("ghost"); ok {
		t.Fatalf("rejected key remained in strmap")
	}
	if idx, ok := snap.StrMap.Index("real"); !ok || idx != 3 {
		t.Fatalf("real idx = %d ok=%v, want 3", idx, ok)
	}
	if s, ok := snap.StrMap.String(2); ok {
		t.Fatalf("hole idx 2 mapped to %q", s)
	}
}

func TestSharedStringSetUnavailableAfterPrepareRollsBackCreatedKeys(t *testing.T) {
	closedErr := errors.New("closed")
	var events []string
	ex, raw, bucket, sm := newStringAttemptTestExecutor(t, &events, "seed", 1, errors.New("unique violation"), func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})
	ex.unavailable = func() error {
		return closedErr
	}

	req1 := stringSetAttemptReq("ghost-a", 2)
	req2 := stringSetAttemptReq("ghost-b", 3)

	executeBatchForTest(ex, []*request{req1, req2})

	if err := <-req1.Done; !errors.Is(err, closedErr) {
		t.Fatalf("req1 error = %v, want closed error", err)
	}
	if err := <-req2.Done; !errors.Is(err, closedErr) {
		t.Fatalf("req2 error = %v, want closed error", err)
	}
	if got := readStringAttemptPayload(t, raw, bucket, "ghost-a"); got != nil {
		t.Fatalf("ghost-a payload persisted: %v", got)
	}
	if got := readStringAttemptPayload(t, raw, bucket, "ghost-b"); got != nil {
		t.Fatalf("ghost-b payload persisted: %v", got)
	}

	snap := sm.Snapshot()
	if snap.Next() != 1 {
		t.Fatalf("strmap next = %d, want 1", snap.Next())
	}
	if idx, ok := snap.Index("seed"); !ok || idx != 1 {
		t.Fatalf("seed idx = %d ok=%v, want 1", idx, ok)
	}
	if _, ok := snap.Index("ghost-a"); ok {
		t.Fatalf("ghost-a remained in strmap")
	}
	if _, ok := snap.Index("ghost-b"); ok {
		t.Fatalf("ghost-b remained in strmap")
	}
}

func TestSharedStringSetBeforeStoreFailureDoesNotCreateStringKey(t *testing.T) {
	hookErr := errors.New("before store failed")
	var events []string
	ex, raw, bucket, _ := newStringAttemptTestExecutor(t, &events, "seed", 1, errors.New("unique violation"), func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})

	badReq := stringSetAttemptReq("ghost", 2)
	badReq.beforeStore = []BeforeStoreHook{
		func(keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error {
			return hookErr
		},
	}
	goodReq := stringSetAttemptReq("real", 3)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := <-badReq.Done; !errors.Is(err, hookErr) {
		t.Fatalf("bad request error = %v, want hook error", err)
	}
	if err := <-goodReq.Done; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readStringAttemptPayload(t, raw, bucket, "ghost"); got != nil {
		t.Fatalf("ghost payload persisted: %v", got)
	}
	if got := readStringAttemptPayload(t, raw, bucket, "real"); !reflect.DeepEqual(got, []byte{3}) {
		t.Fatalf("real payload = %v, want [3]", got)
	}

	snap := ex.unique.Current()
	if snap.StrMap.Next() != 2 {
		t.Fatalf("strmap next = %d, want 2", snap.StrMap.Next())
	}
	if _, ok := snap.StrMap.Index("ghost"); ok {
		t.Fatalf("failed key appeared in strmap")
	}
	if idx, ok := snap.StrMap.Index("real"); !ok || idx != 2 {
		t.Fatalf("real idx = %d ok=%v, want 2", idx, ok)
	}
}

func TestSharedStringSetDecodePreparedValueFailureDoesNotCreateStringKey(t *testing.T) {
	decodeErr := errors.New("decode marker")
	var events []string
	ex, raw, bucket, _ := newStringAttemptTestExecutor(t, &events, "seed", 1, errors.New("unique violation"), func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})
	ex.ops.Decode = func(data []byte) (unsafe.Pointer, error) {
		if len(data) == 1 && data[0] == 0xc1 {
			return nil, decodeErr
		}
		return unsafe.Pointer(&attemptRec{V: data[0]}), nil
	}

	badReq := stringSetAttemptReq("ghost", 2)
	badReq.setPayload.Reset()
	_ = badReq.setPayload.WriteByte(0xc1)
	badReq.beforeStore = []BeforeStoreHook{
		func(keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error {
			return nil
		},
	}
	goodReq := stringSetAttemptReq("real", 3)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := <-badReq.Done; !errors.Is(err, decodeErr) {
		t.Fatalf("bad request error = %v, want decode error", err)
	}
	if err := <-goodReq.Done; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readStringAttemptPayload(t, raw, bucket, "ghost"); got != nil {
		t.Fatalf("ghost payload persisted: %v", got)
	}
	if got := readStringAttemptPayload(t, raw, bucket, "real"); !reflect.DeepEqual(got, []byte{3}) {
		t.Fatalf("real payload = %v, want [3]", got)
	}

	snap := ex.unique.Current()
	if snap.StrMap.Next() != 2 {
		t.Fatalf("strmap next = %d, want 2", snap.StrMap.Next())
	}
	if _, ok := snap.StrMap.Index("ghost"); ok {
		t.Fatalf("failed key appeared in strmap")
	}
	if idx, ok := snap.StrMap.Index("real"); !ok || idx != 2 {
		t.Fatalf("real idx = %d ok=%v, want 2", idx, ok)
	}
}

func TestSharedStringSetBeforeCommitFailureRetriesNeighborWithRolledBackStringKeys(t *testing.T) {
	callbackErr := errors.New("before commit failed")
	var events []string
	ex, raw, bucket, _ := newStringAttemptTestExecutor(t, &events, "seed", 1, errors.New("unique violation"), func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})

	badReq := stringSetAttemptReq("ghost", 2)
	badReq.beforeCommit = []BeforeCommitHook{
		func(*bbolt.Tx, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error {
			return callbackErr
		},
	}
	goodReq := stringSetAttemptReq("real", 3)

	executeBatchForTest(ex, []*request{badReq, goodReq})

	if err := <-badReq.Done; !errors.Is(err, callbackErr) {
		t.Fatalf("bad request error = %v, want callback error", err)
	}
	if err := <-goodReq.Done; err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got := readStringAttemptPayload(t, raw, bucket, "ghost"); got != nil {
		t.Fatalf("ghost payload persisted: %v", got)
	}
	if got := readStringAttemptPayload(t, raw, bucket, "real"); !reflect.DeepEqual(got, []byte{3}) {
		t.Fatalf("real payload = %v, want [3]", got)
	}

	snap := ex.unique.Current()
	if snap.StrMap.Next() != 2 {
		t.Fatalf("strmap next = %d, want 2", snap.StrMap.Next())
	}
	if _, ok := snap.StrMap.Index("ghost"); ok {
		t.Fatalf("failed key appeared in strmap")
	}
	if idx, ok := snap.StrMap.Index("real"); !ok || idx != 2 {
		t.Fatalf("real idx = %d ok=%v, want 2", idx, ok)
	}
}

func TestExecuteJobsRepeatedPatchUniqueFirstBeforeCommitErrorRetriesFollowerFromOriginalState(t *testing.T) {
	callbackErr := errors.New("before commit failed")
	uniqueErr := errors.New("unique violation")
	seed := attemptRec{V: 1}
	var events []string
	ex, raw, bucket := newUniqueAttemptTestExecutor(t, &events, uniqueErr, []snapshot.BatchEntry{
		{ID: 1, New: unsafe.Pointer(&seed)},
	}, func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{1})

	req1 := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: byte(2)}}, true)
	req2 := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: byte(3)}}, true)
	req1.policy = reqRepeatIDSafeShared
	req2.policy = reqRepeatIDSafeShared
	req1.beforeCommit = []BeforeCommitHook{
		func(*bbolt.Tx, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error {
			return callbackErr
		},
	}
	var oldSeen []byte
	req2.beforeCommit = []BeforeCommitHook{
		func(_ *bbolt.Tx, _ keycodec.DataKey, oldValue, _ unsafe.Pointer) error {
			oldSeen = append(oldSeen, (*attemptRec)(oldValue).V)
			return nil
		},
	}

	ex.sched.mu.Lock()
	ex.sched.window = 0
	ex.sched.maxOps = 16
	ex.sched.running = true
	ex.sched.enqueue(&writeJob{reqs: []*request{req1}, done: req1.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req2}, done: req2.Done})
	ex.sched.mu.Unlock()

	batch := ex.sched.popBatch(false)
	if len(batch) != 2 {
		t.Fatalf("batch len = %d, want 2", len(batch))
	}

	ex.executeJobs(batch)

	if err := <-req1.Done; !errors.Is(err, callbackErr) {
		t.Fatalf("req1 error = %v, want callback error", err)
	}
	if err := <-req2.Done; err != nil {
		t.Fatalf("req2 error = %v", err)
	}
	if !reflect.DeepEqual(oldSeen, []byte{1}) {
		t.Fatalf("follower old values = %v, want [1]", oldSeen)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{3}) {
		t.Fatalf("id=1 payload = %v, want [3]", got)
	}
	if ids := ex.unique.Current().FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(3)); !ids.Contains(1) {
		t.Fatalf("unique value 3 missing id=1")
	}
}
