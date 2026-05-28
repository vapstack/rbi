package wexec

import (
	"errors"
	"testing"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/schema"
	"go.etcd.io/bbolt"
)

func TestRequestPoolCleanupReleasesPayloadAndDrainsDone(t *testing.T) {
	req := requestPool.Get()
	payload := encodePool.Get()
	_, _ = payload.Write([]byte{1, 2, 3})

	v := attemptRec{V: 7}
	errSentinel := errors.New("request failed")
	req.op = opSet
	req.id = keycodec.DataKeyFromUserKey(uint64(42), false)
	req.setValue = unsafe.Pointer(&v)
	req.setBaseline = unsafe.Pointer(&v)
	req.setPayload = payload
	req.patch = append(req.patch, schema.PatchItem{Name: "x"})
	req.patchIgnoreUnknown = true
	req.beforeProcess = append(req.beforeProcess, func(keycodec.DataKey, unsafe.Pointer) error { return nil })
	req.beforeStore = append(req.beforeStore, func(keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error { return nil })
	req.beforeCommit = append(req.beforeCommit, func(*bbolt.Tx, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error { return nil })
	req.cloneValue = func(keycodec.DataKey, unsafe.Pointer) (unsafe.Pointer, error) { return unsafe.Pointer(&v), nil }
	req.policy = reqRepeatIDSafeShared | reqSetDeleteCoalescible
	req.replacedBy = &request{}
	req.Err = errSentinel
	req.Done <- errSentinel

	requestPool.Put(req)

	if req.op != 0 || req.id != (keycodec.DataKey{}) || req.Err != nil {
		t.Fatalf("request identity was not cleared: op=%v id=%v err=%v", req.op, req.id, req.Err)
	}
	if req.setPayload != nil || req.setValue != nil || req.setBaseline != nil {
		t.Fatalf("set state was not cleared: payload=%v value=%v baseline=%v", req.setPayload, req.setValue, req.setBaseline)
	}
	if len(req.patch) != 0 || req.patchIgnoreUnknown {
		t.Fatalf("patch state was not cleared: len=%d ignore=%v", len(req.patch), req.patchIgnoreUnknown)
	}
	if req.beforeProcess != nil || req.beforeStore != nil || req.beforeCommit != nil || req.cloneValue != nil {
		t.Fatalf("hooks were not cleared")
	}
	if req.policy != 0 || req.replacedBy != nil {
		t.Fatalf("policy/coalescing state was not cleared: policy=%v replacedBy=%p", req.policy, req.replacedBy)
	}
	select {
	case err := <-req.Done:
		t.Fatalf("done channel was not drained: %v", err)
	default:
	}
}
