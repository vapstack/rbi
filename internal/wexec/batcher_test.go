package wexec

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"go.etcd.io/bbolt"
)

func TestSingleRequestBatchesUseConfiguredExecutor(t *testing.T) {
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
		events = append(events, "commit:"+op)
		return tx.Commit()
	})

	rec := attemptRec{V: 10}
	batch := ex.NewBatch(1)
	if err := batch.AddSet(keycodec.DataKeyFromUserKey(uint64(1), false), unsafe.Pointer(&rec), nil, nil, nil); err != nil {
		batch.Cancel()
		t.Fatalf("AddSet: %v", err)
	}
	if err := batch.Submit(false); err != nil {
		t.Fatalf("set Submit: %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !bytes.Equal(got, []byte{10}) {
		t.Fatalf("payload after set batch = %v, want [10]", got)
	}

	batch = ex.NewBatch(1)
	batch.AddPatch(
		keycodec.DataKeyFromUserKey(uint64(1), false),
		[]schema.PatchItem{{Name: "v", Value: byte(11)}},
		true,
		nil,
		nil,
		nil,
	)
	if err := batch.Submit(false); err != nil {
		t.Fatalf("patch Submit: %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !bytes.Equal(got, []byte{11}) {
		t.Fatalf("payload after patch batch = %v, want [11]", got)
	}

	batch = ex.NewBatch(1)
	batch.AddDelete(keycodec.DataKeyFromUserKey(uint64(1), false), nil)
	if err := batch.Submit(false); err != nil {
		t.Fatalf("delete Submit: %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); got != nil {
		t.Fatalf("payload after delete batch = %v, want nil", got)
	}

	queueLen, queueMax, executed, dequeued := ex.BasicStats()
	if queueLen != 0 || queueMax == 0 || executed == 0 || dequeued == 0 {
		t.Fatalf("BasicStats = queueLen=%d queueMax=%d executed=%d dequeued=%d", queueLen, queueMax, executed, dequeued)
	}
	if st := ex.Stats(); st.QueueLen != 0 || st.ExecutedBatches == 0 || st.Dequeued == 0 {
		t.Fatalf("Snapshot = %+v", st)
	}
	if len(events) < 6 {
		t.Fatalf("expected commit/publish events for single-request batches, got %v", events)
	}
}

func TestBatchAPISubmitAndCancel(t *testing.T) {
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, _ string) error {
		return tx.Commit()
	})
	ex.snapshotOps = SnapshotOps{}

	putAttemptPayload(t, raw, bucket, 2, []byte{7})

	rec := attemptRec{V: 1}
	batch := ex.NewBatch(3)
	if err := batch.AddSet(keycodec.DataKeyFromUserKey(uint64(1), false), unsafe.Pointer(&rec), nil, nil, nil); err != nil {
		t.Fatalf("AddSet: %v", err)
	}
	batch.AddPatch(
		keycodec.DataKeyFromUserKey(uint64(1), false),
		[]schema.PatchItem{{Name: "v", Value: byte(2)}},
		true,
		nil,
		nil,
		nil,
	)
	batch.AddDelete(keycodec.DataKeyFromUserKey(uint64(2), false), nil)
	if err := batch.Submit(true); err != nil {
		t.Fatalf("Batch Submit: %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !bytes.Equal(got, []byte{2}) {
		t.Fatalf("payload after batch set+patch = %v, want [2]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); got != nil {
		t.Fatalf("payload after batch delete = %v, want nil", got)
	}

	rec = attemptRec{V: 3}
	cancel := ex.NewBatch(1)
	if err := cancel.AddSet(keycodec.DataKeyFromUserKey(uint64(3), false), unsafe.Pointer(&rec), nil, nil, nil); err != nil {
		t.Fatalf("cancel AddSet: %v", err)
	}
	cancel.Cancel()
	if got := readAttemptPayload(t, raw, bucket, 3); got != nil {
		t.Fatalf("payload after canceled batch = %v, want nil", got)
	}
}

func TestDirectSubmitDrainsConcurrentQueuedJob(t *testing.T) {
	var events []string
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var first atomic.Bool
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, _ string) error {
		if first.CompareAndSwap(false, true) {
			close(firstStarted)
			<-releaseFirst
		}
		return tx.Commit()
	})
	ex.snapshotOps = SnapshotOps{}
	ex.sched.maxOps = 1
	ex.sched.window = 0
	ex.sched.maxQ = 0

	rec1 := attemptRec{V: 1}
	batch1 := ex.NewBatch(1)
	if err := batch1.AddSet(keycodec.DataKeyFromUserKey(uint64(1), false), unsafe.Pointer(&rec1), nil, nil, nil); err != nil {
		t.Fatalf("AddSet first: %v", err)
	}
	done1 := make(chan error, 1)
	go func() {
		done1 <- batch1.Submit(false)
	}()

	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first submit did not reach commit")
	}

	rec2 := attemptRec{V: 2}
	batch2 := ex.NewBatch(1)
	if err := batch2.AddSet(keycodec.DataKeyFromUserKey(uint64(2), false), unsafe.Pointer(&rec2), nil, nil, nil); err != nil {
		t.Fatalf("AddSet second: %v", err)
	}
	done2 := make(chan error, 1)
	go func() {
		done2 <- batch2.Submit(false)
	}()

	deadline := time.After(time.Second)
	for {
		ex.sched.mu.Lock()
		queued := ex.sched.queueLen
		running := ex.sched.running
		ex.sched.mu.Unlock()
		if queued == 1 && running {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("second submit was not queued while direct submit was running: queue=%d running=%v", queued, running)
		default:
			time.Sleep(time.Millisecond)
		}
	}

	close(releaseFirst)

	select {
	case err := <-done1:
		if err != nil {
			t.Fatalf("first submit error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first submit did not finish")
	}
	select {
	case err := <-done2:
		if err != nil {
			t.Fatalf("second submit error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued submit was not drained")
	}

	if got := readAttemptPayload(t, raw, bucket, 1); !bytes.Equal(got, []byte{1}) {
		t.Fatalf("id=1 payload = %v, want [1]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !bytes.Equal(got, []byte{2}) {
		t.Fatalf("id=2 payload = %v, want [2]", got)
	}
}

func TestExecuteJobsCoalescedSetDeleteUsesTerminalWrite(t *testing.T) {
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})
	ex.snapshotOps = SnapshotOps{}
	putAttemptPayload(t, raw, bucket, 1, []byte{1})

	req1 := setAttemptReq(1, 2)
	req2 := deleteAttemptReq(1)
	req3 := setAttemptReq(1, 3)
	req4 := setAttemptReq(2, 4)
	req1.policy = reqSetDeleteCoalescible
	req2.policy = reqSetDeleteCoalescible
	req3.policy = reqSetDeleteCoalescible
	req4.policy = reqSetDeleteCoalescible

	ex.sched.mu.Lock()
	ex.sched.window = 0
	ex.sched.maxOps = 16
	ex.sched.running = true
	ex.sched.enqueue(&writeJob{reqs: []*request{req1}, done: req1.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req2}, done: req2.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req3}, done: req3.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req4}, done: req4.Done})
	ex.sched.mu.Unlock()

	batch := ex.sched.popBatch(false)
	if len(batch) != 4 {
		t.Fatalf("batch len = %d, want 4", len(batch))
	}
	if req1.replacedBy != req2 || req2.replacedBy != req3 || req3.replacedBy != nil {
		t.Fatalf("replacement chain req1=%p req2=%p req3=%p", req1.replacedBy, req2.replacedBy, req3.replacedBy)
	}

	ex.executeJobs(batch)

	for i, req := range []*request{req1, req2, req3, req4} {
		if err := <-req.Done; err != nil {
			t.Fatalf("request #%d error = %v", i+1, err)
		}
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{3}) {
		t.Fatalf("id=1 payload = %v, want [3]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{4}) {
		t.Fatalf("id=2 payload = %v, want [4]", got)
	}
	if got := ex.sched.stats.CoalescedSetDelete.Load(); got != 2 {
		t.Fatalf("coalesced counter = %d, want 2", got)
	}
}

func TestExecuteJobsCoalescedChainPropagatesTerminalError(t *testing.T) {
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

	req1 := setAttemptReq(1, 10)
	req2 := deleteAttemptReq(1)
	req3 := setAttemptReq(1, 2)
	req4 := setAttemptReq(3, 3)
	req1.policy = reqSetDeleteCoalescible
	req2.policy = reqSetDeleteCoalescible
	req3.policy = reqSetDeleteCoalescible
	req4.policy = reqSetDeleteCoalescible

	ex.sched.mu.Lock()
	ex.sched.window = 0
	ex.sched.maxOps = 16
	ex.sched.running = true
	ex.sched.enqueue(&writeJob{reqs: []*request{req1}, done: req1.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req2}, done: req2.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req3}, done: req3.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req4}, done: req4.Done})
	ex.sched.mu.Unlock()

	batch := ex.sched.popBatch(false)
	if len(batch) != 4 {
		t.Fatalf("batch len = %d, want 4", len(batch))
	}
	if req1.replacedBy != req2 || req2.replacedBy != req3 || req3.replacedBy != nil {
		t.Fatalf("replacement chain req1=%p req2=%p req3=%p", req1.replacedBy, req2.replacedBy, req3.replacedBy)
	}

	ex.executeJobs(batch)

	if err := <-req1.Done; !errors.Is(err, uniqueErr) {
		t.Fatalf("req1 error = %v, want unique error", err)
	}
	if err := <-req2.Done; !errors.Is(err, uniqueErr) {
		t.Fatalf("req2 error = %v, want unique error", err)
	}
	if err := <-req3.Done; !errors.Is(err, uniqueErr) {
		t.Fatalf("req3 error = %v, want unique error", err)
	}
	if err := <-req4.Done; err != nil {
		t.Fatalf("req4 error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{1}) {
		t.Fatalf("id=1 payload = %v, want [1]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 3); !reflect.DeepEqual(got, []byte{3}) {
		t.Fatalf("id=3 payload = %v, want [3]", got)
	}
}

func TestExecuteJobsCoalescedChainsPropagateCommitError(t *testing.T) {
	commitErr := errors.New("commit failed")
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(*bbolt.Tx, string) error {
		return commitErr
	})
	ex.snapshotOps = SnapshotOps{}
	putAttemptPayload(t, raw, bucket, 1, []byte{1})
	putAttemptPayload(t, raw, bucket, 2, []byte{2})

	req1 := setAttemptReq(1, 10)
	req2 := setAttemptReq(2, 20)
	req3 := deleteAttemptReq(1)
	req4 := deleteAttemptReq(2)
	req1.policy = reqSetDeleteCoalescible
	req2.policy = reqSetDeleteCoalescible
	req3.policy = reqSetDeleteCoalescible
	req4.policy = reqSetDeleteCoalescible

	ex.sched.mu.Lock()
	ex.sched.window = 0
	ex.sched.maxOps = 16
	ex.sched.running = true
	ex.sched.enqueue(&writeJob{reqs: []*request{req1}, done: req1.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req2}, done: req2.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req3}, done: req3.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req4}, done: req4.Done})
	ex.sched.mu.Unlock()

	batch := ex.sched.popBatch(false)
	if len(batch) != 4 {
		t.Fatalf("batch len = %d, want 4", len(batch))
	}

	ex.executeJobs(batch)

	for i, req := range []*request{req1, req2, req3, req4} {
		if err := <-req.Done; !errors.Is(err, commitErr) {
			t.Fatalf("request #%d error = %v, want commit error", i+1, err)
		}
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{1}) {
		t.Fatalf("id=1 payload = %v, want [1]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{2}) {
		t.Fatalf("id=2 payload = %v, want [2]", got)
	}
}

func TestExecuteJobsCoalescedChainSurvivesNeighborCallbackFailure(t *testing.T) {
	callbackErr := errors.New("neighbor callback failed")
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})
	ex.snapshotOps = SnapshotOps{}
	putAttemptPayload(t, raw, bucket, 1, []byte{1})

	req1 := setAttemptReq(1, 2)
	req2 := setAttemptReq(2, 3)
	req3 := deleteAttemptReq(1)
	req1.policy = reqSetDeleteCoalescible
	req3.policy = reqSetDeleteCoalescible
	req2.beforeCommit = []BeforeCommitHook{
		func(*bbolt.Tx, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error {
			return callbackErr
		},
	}

	ex.sched.mu.Lock()
	ex.sched.window = 0
	ex.sched.maxOps = 16
	ex.sched.running = true
	ex.sched.enqueue(&writeJob{reqs: []*request{req1}, done: req1.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req2}, done: req2.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req3}, done: req3.Done})
	ex.sched.mu.Unlock()

	batch := ex.sched.popBatch(false)
	if len(batch) != 3 {
		t.Fatalf("batch len = %d, want 3", len(batch))
	}
	if req1.replacedBy != req3 {
		t.Fatalf("req1 replacement = %p, want req3 %p", req1.replacedBy, req3)
	}

	ex.executeJobs(batch)

	if err := <-req1.Done; err != nil {
		t.Fatalf("req1 error = %v", err)
	}
	if err := <-req2.Done; !errors.Is(err, callbackErr) {
		t.Fatalf("req2 error = %v, want callback error", err)
	}
	if err := <-req3.Done; err != nil {
		t.Fatalf("req3 error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); got != nil {
		t.Fatalf("id=1 payload = %v, want nil", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); got != nil {
		t.Fatalf("id=2 payload = %v, want nil", got)
	}
}

func TestExecuteJobsCoalescedInterleavedChainsPreserveFinalStateAndIndex(t *testing.T) {
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

	req1 := setAttemptReq(1, 3)
	req2 := setAttemptReq(2, 4)
	req3 := deleteAttemptReq(1)
	req4 := deleteAttemptReq(2)
	req5 := setAttemptReq(1, 5)
	req6 := setAttemptReq(2, 6)
	for _, req := range []*request{req1, req2, req3, req4, req5, req6} {
		req.policy = reqSetDeleteCoalescible
	}

	ex.sched.mu.Lock()
	ex.sched.window = 0
	ex.sched.maxOps = 16
	ex.sched.running = true
	ex.sched.enqueue(&writeJob{reqs: []*request{req1}, done: req1.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req2}, done: req2.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req3}, done: req3.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req4}, done: req4.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req5}, done: req5.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req6}, done: req6.Done})
	ex.sched.mu.Unlock()

	batch := ex.sched.popBatch(false)
	if len(batch) != 6 {
		t.Fatalf("batch len = %d, want 6", len(batch))
	}
	if req1.replacedBy != req3 || req3.replacedBy != req5 {
		t.Fatalf("id=1 chain req1=%p req3=%p", req1.replacedBy, req3.replacedBy)
	}
	if req2.replacedBy != req4 || req4.replacedBy != req6 {
		t.Fatalf("id=2 chain req2=%p req4=%p", req2.replacedBy, req4.replacedBy)
	}

	ex.executeJobs(batch)

	for i, req := range []*request{req1, req2, req3, req4, req5, req6} {
		if err := <-req.Done; err != nil {
			t.Fatalf("request #%d error = %v", i+1, err)
		}
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{5}) {
		t.Fatalf("id=1 payload = %v, want [5]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{6}) {
		t.Fatalf("id=2 payload = %v, want [6]", got)
	}

	snap := ex.unique.Current()
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(3)); !ids.IsEmpty() {
		t.Fatalf("stale value 3 ids present")
	}
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(4)); !ids.IsEmpty() {
		t.Fatalf("stale value 4 ids present")
	}
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(5)); !ids.Contains(1) {
		t.Fatalf("value 5 missing id=1")
	}
	if ids := snap.FieldLookupPostingRetainedKey("v", keycodec.IndexLookupU64(6)); !ids.Contains(2) {
		t.Fatalf("value 6 missing id=2")
	}
}

func TestExecuteJobsRepeatedPatchAppliesSequentialState(t *testing.T) {
	var events []string
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{1})
	putAttemptPayload(t, raw, bucket, 2, []byte{2})

	req1 := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: byte(4)}}, true)
	req2 := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: byte(5)}}, true)
	req3 := patchAttemptReq(2, []schema.PatchItem{{Name: "v", Value: byte(6)}}, true)
	req1.policy = reqRepeatIDSafeShared
	req2.policy = reqRepeatIDSafeShared
	req3.policy = reqRepeatIDSafeShared

	ex.sched.mu.Lock()
	ex.sched.window = 0
	ex.sched.maxOps = 16
	ex.sched.running = true
	ex.sched.enqueue(&writeJob{reqs: []*request{req1}, done: req1.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req2}, done: req2.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req3}, done: req3.Done})
	ex.sched.mu.Unlock()

	batch := ex.sched.popBatch(false)
	if len(batch) != 3 {
		t.Fatalf("batch len = %d, want 3", len(batch))
	}

	ex.executeJobs(batch)

	for i, req := range []*request{req1, req2, req3} {
		if err := <-req.Done; err != nil {
			t.Fatalf("request #%d error = %v", i+1, err)
		}
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{5}) {
		t.Fatalf("id=1 payload = %v, want [5]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{6}) {
		t.Fatalf("id=2 payload = %v, want [6]", got)
	}
}

func TestExecuteJobsRepeatedPatchFirstBeforeCommitErrorRetriesFollowerFromOriginalState(t *testing.T) {
	callbackErr := errors.New("before commit failed")
	var events []string
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{10})

	req1 := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: byte(20)}}, true)
	req2 := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: byte(30)}}, true)
	req1.policy = reqRepeatIDSafeShared
	req2.policy = reqRepeatIDSafeShared
	req1.beforeCommit = []BeforeCommitHook{
		func(*bbolt.Tx, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error {
			return callbackErr
		},
	}
	var seen []byte
	req2.beforeStore = []BeforeStoreHook{
		func(_ keycodec.DataKey, oldValue, newValue unsafe.Pointer) error {
			seen = append(seen, (*attemptRec)(oldValue).V, (*attemptRec)(newValue).V)
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
	if !reflect.DeepEqual(seen, []byte{20, 30, 10, 30}) {
		t.Fatalf("BeforeStore seen = %v, want [20 30 10 30]", seen)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{30}) {
		t.Fatalf("id=1 payload = %v, want [30]", got)
	}
}

func TestExecuteJobsRepeatedPatchMiddleBeforeCommitErrorRetriesNeighborsSequentially(t *testing.T) {
	callbackErr := errors.New("before commit failed")
	var events []string
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{10})

	req1 := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: byte(20)}}, true)
	req2 := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: byte(30)}}, true)
	req3 := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: byte(40)}}, true)
	req1.policy = reqRepeatIDSafeShared
	req2.policy = reqRepeatIDSafeShared
	req3.policy = reqRepeatIDSafeShared
	req2.beforeCommit = []BeforeCommitHook{
		func(*bbolt.Tx, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error {
			return callbackErr
		},
	}
	var seen []byte
	req3.beforeStore = []BeforeStoreHook{
		func(_ keycodec.DataKey, oldValue, newValue unsafe.Pointer) error {
			seen = append(seen, (*attemptRec)(oldValue).V, (*attemptRec)(newValue).V)
			return nil
		},
	}

	ex.sched.mu.Lock()
	ex.sched.window = 0
	ex.sched.maxOps = 16
	ex.sched.running = true
	ex.sched.enqueue(&writeJob{reqs: []*request{req1}, done: req1.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req2}, done: req2.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req3}, done: req3.Done})
	ex.sched.mu.Unlock()

	batch := ex.sched.popBatch(false)
	if len(batch) != 3 {
		t.Fatalf("batch len = %d, want 3", len(batch))
	}

	ex.executeJobs(batch)

	if err := <-req1.Done; err != nil {
		t.Fatalf("req1 error = %v", err)
	}
	if err := <-req2.Done; !errors.Is(err, callbackErr) {
		t.Fatalf("req2 error = %v, want callback error", err)
	}
	if err := <-req3.Done; err != nil {
		t.Fatalf("req3 error = %v", err)
	}
	if !reflect.DeepEqual(seen, []byte{30, 40, 20, 40}) {
		t.Fatalf("BeforeStore seen = %v, want [30 40 20 40]", seen)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{40}) {
		t.Fatalf("id=1 payload = %v, want [40]", got)
	}
}

func TestExecuteJobsRepeatedPatchValidateIndexFailureIsolatesOnlyBadRequest(t *testing.T) {
	validateErr := errors.New("validate index failed")
	var events []string
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})
	ex.ops.ValidateIndex = func(ptr unsafe.Pointer) error {
		if (*attemptRec)(ptr).V == 99 {
			return validateErr
		}
		return nil
	}
	putAttemptPayload(t, raw, bucket, 1, []byte{10})
	putAttemptPayload(t, raw, bucket, 2, []byte{2})

	req1 := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: byte(20)}}, true)
	req2 := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: byte(99)}}, true)
	req3 := patchAttemptReq(2, []schema.PatchItem{{Name: "v", Value: byte(30)}}, true)
	req1.policy = reqRepeatIDSafeShared
	req2.policy = reqRepeatIDSafeShared
	req3.policy = reqRepeatIDSafeShared

	ex.sched.mu.Lock()
	ex.sched.window = 0
	ex.sched.maxOps = 16
	ex.sched.running = true
	ex.sched.enqueue(&writeJob{reqs: []*request{req1}, done: req1.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req2}, done: req2.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req3}, done: req3.Done})
	ex.sched.mu.Unlock()

	batch := ex.sched.popBatch(false)
	if len(batch) != 3 {
		t.Fatalf("batch len = %d, want 3", len(batch))
	}

	ex.executeJobs(batch)

	if err := <-req1.Done; err != nil {
		t.Fatalf("req1 error = %v", err)
	}
	if err := <-req2.Done; !errors.Is(err, validateErr) {
		t.Fatalf("req2 error = %v, want validate error", err)
	}
	if err := <-req3.Done; err != nil {
		t.Fatalf("req3 error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{20}) {
		t.Fatalf("id=1 payload = %v, want [20]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{30}) {
		t.Fatalf("id=2 payload = %v, want [30]", got)
	}
}

func TestExecuteJobsRepeatedPatchBeforeStoreSeesSteppedState(t *testing.T) {
	var events []string
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{10})

	var seen []byte
	hook := func(_ keycodec.DataKey, oldValue, newValue unsafe.Pointer) error {
		seen = append(seen, (*attemptRec)(oldValue).V, (*attemptRec)(newValue).V)
		return nil
	}
	req1 := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: byte(20)}}, true)
	req2 := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: byte(30)}}, true)
	req1.policy = reqRepeatIDSafeShared
	req2.policy = reqRepeatIDSafeShared
	req1.beforeStore = []BeforeStoreHook{hook}
	req2.beforeStore = []BeforeStoreHook{hook}

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

	if err := <-req1.Done; err != nil {
		t.Fatalf("req1 error = %v", err)
	}
	if err := <-req2.Done; err != nil {
		t.Fatalf("req2 error = %v", err)
	}
	if !reflect.DeepEqual(seen, []byte{10, 20, 20, 30}) {
		t.Fatalf("BeforeStore seen = %v, want [10 20 20 30]", seen)
	}
}

func TestExecuteJobsRepeatedPatchBeforeCommitSeesSteppedState(t *testing.T) {
	var events []string
	ex, raw, bucket := newPatchAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{10})

	var seen []byte
	hook := func(tx *bbolt.Tx, key keycodec.DataKey, oldValue, newValue unsafe.Pointer) error {
		var keyBuf [8]byte
		payload := tx.Bucket(bucket).Get(key.Bytes(false, &keyBuf))
		if len(payload) != 1 {
			return errors.New("unexpected payload length")
		}
		seen = append(seen, (*attemptRec)(oldValue).V, (*attemptRec)(newValue).V, payload[0])
		return nil
	}
	req1 := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: byte(20)}}, true)
	req2 := patchAttemptReq(1, []schema.PatchItem{{Name: "v", Value: byte(30)}}, true)
	req1.policy = reqRepeatIDSafeShared
	req2.policy = reqRepeatIDSafeShared
	req1.beforeCommit = []BeforeCommitHook{hook}
	req2.beforeCommit = []BeforeCommitHook{hook}

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

	if err := <-req1.Done; err != nil {
		t.Fatalf("req1 error = %v", err)
	}
	if err := <-req2.Done; err != nil {
		t.Fatalf("req2 error = %v", err)
	}
	if !reflect.DeepEqual(seen, []byte{10, 20, 20, 20, 30, 30}) {
		t.Fatalf("BeforeCommit seen = %v, want [10 20 20 20 30 30]", seen)
	}
}

func TestExecuteJobsRepeatedPatchBeforeProcessMutationsAreSequential(t *testing.T) {
	var events []string
	ex, raw, bucket := newPairAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{10, 0})

	call := 0
	hook := func(_ keycodec.DataKey, value unsafe.Pointer) error {
		call++
		rec := (*attemptPairRec)(value)
		switch call {
		case 1:
			rec.B = 1
		case 2:
			if rec.B != 1 {
				return errors.New("second before process missed first mutation")
			}
			rec.B = 2
		default:
			return errors.New("unexpected before process call")
		}
		return nil
	}

	req1 := patchAttemptReq(1, []schema.PatchItem{{Name: "a", Value: byte(20)}}, true)
	req2 := patchAttemptReq(1, []schema.PatchItem{{Name: "a", Value: byte(30)}}, true)
	req1.policy = reqRepeatIDSafeShared
	req2.policy = reqRepeatIDSafeShared
	req1.beforeProcess = []BeforeProcessHook{hook}
	req2.beforeProcess = []BeforeProcessHook{hook}

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

	if err := <-req1.Done; err != nil {
		t.Fatalf("req1 error = %v", err)
	}
	if err := <-req2.Done; err != nil {
		t.Fatalf("req2 error = %v", err)
	}
	if call != 2 {
		t.Fatalf("BeforeProcess calls = %d, want 2", call)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{30, 2}) {
		t.Fatalf("id=1 payload = %v, want [30 2]", got)
	}
}

func TestExecuteJobsInterleavedIsolatedSameIDPreservesSequentialState(t *testing.T) {
	var events []string
	ex, raw, bucket := newPairAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{10, 15})

	req1 := patchAttemptReq(1, []schema.PatchItem{{Name: "a", Value: byte(20)}}, true)
	req2 := patchAttemptReq(1, []schema.PatchItem{{Name: "b", Value: byte(95)}}, true)
	req3 := patchAttemptReq(1, []schema.PatchItem{{Name: "a", Value: byte(30)}}, true)
	req1.policy = reqRepeatIDSafeShared
	req2.policy = reqRepeatIDSafeShared
	req3.policy = reqRepeatIDSafeShared

	seen := make([]byte, 0, 8)
	req2.beforeCommit = []BeforeCommitHook{
		func(_ *bbolt.Tx, _ keycodec.DataKey, oldValue, newValue unsafe.Pointer) error {
			oldRec := (*attemptPairRec)(oldValue)
			newRec := (*attemptPairRec)(newValue)
			seen = append(seen, oldRec.A, oldRec.B, newRec.A, newRec.B)
			return nil
		},
	}
	req3.beforeCommit = []BeforeCommitHook{
		func(_ *bbolt.Tx, _ keycodec.DataKey, oldValue, newValue unsafe.Pointer) error {
			oldRec := (*attemptPairRec)(oldValue)
			newRec := (*attemptPairRec)(newValue)
			seen = append(seen, oldRec.A, oldRec.B, newRec.A, newRec.B)
			return nil
		},
	}

	ex.sched.mu.Lock()
	ex.sched.window = 0
	ex.sched.maxOps = 16
	ex.sched.running = true
	ex.sched.hotUntil = time.Time{}
	ex.sched.enqueue(&writeJob{reqs: []*request{req1}, done: req1.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req2}, isolated: true, done: req2.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req3}, done: req3.Done})
	ex.sched.mu.Unlock()

	var popped []int
	for {
		batch := ex.sched.popBatch(false)
		if len(batch) == 0 {
			break
		}
		popped = append(popped, len(batch))
		ex.executeJobs(batch)
	}

	if !reflect.DeepEqual(popped, []int{1, 1, 1}) {
		t.Fatalf("popped batch sizes = %v, want [1 1 1]", popped)
	}
	for i, req := range []*request{req1, req2, req3} {
		if err := <-req.Done; err != nil {
			t.Fatalf("req%d error = %v", i+1, err)
		}
	}
	if !reflect.DeepEqual(seen, []byte{20, 15, 20, 95, 20, 95, 30, 95}) {
		t.Fatalf("BeforeCommit sequence = %v, want [20 15 20 95 20 95 30 95]", seen)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{30, 95}) {
		t.Fatalf("id=1 payload = %v, want [30 95]", got)
	}
}

func TestExecuteJobsGroupedJobBetweenSharedRequestsStaysIsolatedAndOrdered(t *testing.T) {
	var events []string
	ex, raw, bucket := newPairAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})
	putAttemptPayload(t, raw, bucket, 1, []byte{10, 15})
	putAttemptPayload(t, raw, bucket, 2, []byte{20, 25})

	headReq := patchAttemptReq(1, []schema.PatchItem{{Name: "a", Value: byte(30)}}, true)
	groupReq1 := patchAttemptReq(1, []schema.PatchItem{{Name: "b", Value: byte(75)}}, true)
	groupReq2 := patchAttemptReq(2, []schema.PatchItem{{Name: "a", Value: byte(40)}}, true)
	tailReq := patchAttemptReq(1, []schema.PatchItem{{Name: "a", Value: byte(50)}}, true)
	headReq.policy = reqRepeatIDSafeShared
	groupReq1.policy = reqRepeatIDSafeShared
	groupReq2.policy = reqRepeatIDSafeShared
	tailReq.policy = reqRepeatIDSafeShared

	seen := make([]byte, 0, 12)
	hook := func(_ *bbolt.Tx, _ keycodec.DataKey, oldValue, newValue unsafe.Pointer) error {
		oldRec := (*attemptPairRec)(oldValue)
		newRec := (*attemptPairRec)(newValue)
		seen = append(seen, oldRec.A, oldRec.B, newRec.A, newRec.B)
		return nil
	}
	groupReq1.beforeCommit = []BeforeCommitHook{hook}
	groupReq2.beforeCommit = []BeforeCommitHook{hook}
	tailReq.beforeCommit = []BeforeCommitHook{hook}
	groupDone := make(chan error, 1)

	ex.sched.mu.Lock()
	ex.sched.window = 0
	ex.sched.maxOps = 16
	ex.sched.running = true
	ex.sched.hotUntil = time.Time{}
	ex.sched.enqueue(&writeJob{reqs: []*request{headReq}, done: headReq.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{groupReq1, groupReq2}, isolated: true, done: groupDone})
	ex.sched.enqueue(&writeJob{reqs: []*request{tailReq}, done: tailReq.Done})
	ex.sched.mu.Unlock()

	var popped []string
	for {
		batch := ex.sched.popBatch(false)
		if len(batch) == 0 {
			break
		}
		if len(batch) != 1 {
			popped = append(popped, "shared")
		} else if batch[0].isolated && len(batch[0].reqs) == 2 {
			popped = append(popped, "group")
		} else if batch[0].isolated {
			popped = append(popped, "isolated")
		} else {
			popped = append(popped, "shared")
		}
		ex.executeJobs(batch)
	}

	if !reflect.DeepEqual(popped, []string{"shared", "group", "shared"}) {
		t.Fatalf("popped batches = %v, want [shared group shared]", popped)
	}
	if err := <-headReq.Done; err != nil {
		t.Fatalf("headReq error = %v", err)
	}
	if err := <-groupDone; err != nil {
		t.Fatalf("grouped job error = %v", err)
	}
	if err := <-tailReq.Done; err != nil {
		t.Fatalf("tailReq error = %v", err)
	}
	if !reflect.DeepEqual(seen, []byte{30, 15, 30, 75, 20, 25, 40, 25, 30, 75, 50, 75}) {
		t.Fatalf("BeforeCommit sequence = %v, want [30 15 30 75 20 25 40 25 30 75 50 75]", seen)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{50, 75}) {
		t.Fatalf("id=1 payload = %v, want [50 75]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{40, 25}) {
		t.Fatalf("id=2 payload = %v, want [40 25]", got)
	}
}

func TestExecuteJobsRepeatedDeleteRunsBeforeCommitOnce(t *testing.T) {
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})
	ex.snapshotOps = SnapshotOps{}
	putAttemptPayload(t, raw, bucket, 1, []byte{1})

	calls := 0
	hook := func(*bbolt.Tx, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error {
		calls++
		return nil
	}
	req1 := deleteAttemptReq(1)
	req2 := deleteAttemptReq(1)
	req1.beforeCommit = []BeforeCommitHook{hook}
	req2.beforeCommit = []BeforeCommitHook{hook}
	req1.policy = reqRepeatIDSafeShared
	req2.policy = reqRepeatIDSafeShared

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

	if err := <-req1.Done; err != nil {
		t.Fatalf("req1 error = %v", err)
	}
	if err := <-req2.Done; err != nil {
		t.Fatalf("req2 error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("BeforeCommit calls = %d, want 1", calls)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); got != nil {
		t.Fatalf("id=1 payload = %v, want nil", got)
	}
}

func TestRunAtomicEmptyPayloadFailsWholeBatch(t *testing.T) {
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})
	ex.snapshotOps = SnapshotOps{}

	badRec := &attemptRec{V: 1}
	badPayload := encodePool.Get()
	badPayload.Reset()
	defer encodePool.Put(badPayload)
	badReq := &request{
		op:         opSet,
		id:         keycodec.DataKeyFromUserKey(uint64(1), false),
		setValue:   unsafe.Pointer(badRec),
		setPayload: badPayload,
		Done:       make(chan error, 1),
	}

	goodReq := setAttemptReq(2, 2)
	defer encodePool.Put(goodReq.setPayload)

	ex.runAtomic([]*request{badReq, goodReq})

	if badReq.Err == nil || !strings.Contains(badReq.Err.Error(), "empty msgpack payload") {
		t.Fatalf("bad request error = %v, want empty payload", badReq.Err)
	}
	if goodReq.Err == nil || !strings.Contains(goodReq.Err.Error(), "empty msgpack payload") {
		t.Fatalf("good request error = %v, want atomic batch failure", goodReq.Err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); got != nil {
		t.Fatalf("bad payload persisted: %v", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); got != nil {
		t.Fatalf("good payload persisted after atomic failure: %v", got)
	}
}

func TestExecuteBatchCleansCompletedRequest(t *testing.T) {
	var events []string
	ex, _, _ := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})
	ex.snapshotOps = SnapshotOps{}

	req := setAttemptReq(1, 1)
	req.beforeCommit = []BeforeCommitHook{
		func(*bbolt.Tx, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error {
			return nil
		},
	}

	executeBatchForTest(ex, []*request{req})

	if err := <-req.Done; err != nil {
		t.Fatalf("request error = %v", err)
	}
	if req.setPayload != nil || req.setValue != nil || req.setBaseline != nil {
		t.Fatalf("set state was not cleared: payload=%v value=%v baseline=%v", req.setPayload, req.setValue, req.setBaseline)
	}
	if req.beforeProcess != nil || req.beforeStore != nil || req.beforeCommit != nil || req.cloneValue != nil {
		t.Fatalf("request callbacks were not cleared")
	}
	if req.policy != 0 || req.replacedBy != nil {
		t.Fatalf("request policy/coalesce state was not cleared: policy=%v replacedBy=%p", req.policy, req.replacedBy)
	}
}

func TestSubmitQueueFullWaitsAndEnqueuesAfterDrain(t *testing.T) {
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})
	ex.snapshotOps = SnapshotOps{}

	blockedReq := setAttemptReq(1, 11)
	defer encodePool.Put(blockedReq.setPayload)

	ex.sched.mu.Lock()
	ex.sched.maxQ = 1
	ex.sched.queueHead = 0
	ex.sched.queueLen = 0
	ex.sched.queue = make([]*writeJob, 1)
	ex.sched.enqueue(&writeJob{reqs: []*request{blockedReq}, done: blockedReq.Done})
	ex.sched.running = false
	ex.sched.mu.Unlock()

	req := setAttemptReq(2, 42)
	done := make(chan error, 1)
	go func() {
		done <- ex.submit([]*request{req}, false)
	}()

	if err := <-done; err != nil {
		t.Fatalf("Submit error = %v", err)
	}
	if err := <-blockedReq.Done; err != nil {
		t.Fatalf("blocked request error = %v", err)
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{11}) {
		t.Fatalf("blocked payload = %v, want [11]", got)
	}
	if got := readAttemptPayload(t, raw, bucket, 2); !reflect.DeepEqual(got, []byte{42}) {
		t.Fatalf("submitted payload = %v, want [42]", got)
	}
	if got := ex.sched.stats.Submitted.Load(); got != 1 {
		t.Fatalf("submitted counter = %d, want 1", got)
	}
	if got := ex.sched.stats.Enqueued.Load(); got != 1 {
		t.Fatalf("enqueued counter = %d, want 1", got)
	}
	if got := ex.sched.stats.FallbackClosed.Load(); got != 0 {
		t.Fatalf("fallback-closed counter = %d, want 0", got)
	}
}

func TestRunLoopUnavailableFailsQueuedJobsWithoutExecuting(t *testing.T) {
	closedErr := errors.New("closed")
	var events []string
	ex, raw, bucket := newAttemptTestExecutor(t, &events, func(tx *bbolt.Tx, op string) error {
		return tx.Commit()
	})
	ex.snapshotOps = SnapshotOps{}
	ex.unavailable = func() error {
		return closedErr
	}
	putAttemptPayload(t, raw, bucket, 1, []byte{1})

	req1 := setAttemptReq(1, 2)
	req2 := deleteAttemptReq(1)
	req3 := setAttemptReq(2, 3)
	req4 := setAttemptReq(3, 4)
	req5 := deleteAttemptReq(4)
	for _, req := range []*request{req1, req2, req3} {
		req.policy = reqSetDeleteCoalescible
	}
	defer encodePool.Put(req1.setPayload)
	defer encodePool.Put(req3.setPayload)
	defer encodePool.Put(req4.setPayload)

	groupedDone := make(chan error, 1)
	ex.sched.mu.Lock()
	ex.sched.window = 0
	ex.sched.maxOps = 16
	ex.sched.running = true
	ex.sched.enqueue(&writeJob{reqs: []*request{req1}, done: req1.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req2}, done: req2.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req3}, done: req3.Done})
	ex.sched.enqueue(&writeJob{reqs: []*request{req4, req5}, isolated: true, done: groupedDone})
	ex.sched.mu.Unlock()

	ex.runLoop()

	for i, req := range []*request{req1, req2, req3} {
		if err := <-req.Done; !errors.Is(err, closedErr) {
			t.Fatalf("shared request #%d error = %v, want closed", i+1, err)
		}
	}
	if err := <-groupedDone; !errors.Is(err, closedErr) {
		t.Fatalf("grouped job error = %v, want closed", err)
	}
	for i, req := range []*request{req1, req2, req3, req4, req5} {
		if !errors.Is(req.Err, closedErr) {
			t.Fatalf("request #%d stored error = %v, want closed", i+1, req.Err)
		}
	}
	if got := readAttemptPayload(t, raw, bucket, 1); !reflect.DeepEqual(got, []byte{1}) {
		t.Fatalf("id=1 payload after unavailable runLoop = %v, want [1]", got)
	}
	for _, id := range []uint64{2, 3, 4} {
		if got := readAttemptPayload(t, raw, bucket, id); got != nil {
			t.Fatalf("id=%d payload persisted after unavailable runLoop: %v", id, got)
		}
	}
	if got := ex.sched.stats.ExecutedBatches.Load(); got != 0 {
		t.Fatalf("executed batches = %d, want 0", got)
	}
	if got := ex.sched.stats.Dequeued.Load(); got != 4 {
		t.Fatalf("dequeued jobs = %d, want 4", got)
	}
}
