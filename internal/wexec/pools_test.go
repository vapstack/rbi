package wexec

import (
	"errors"
	"testing"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
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

func TestAttemptStatePoolCleanupReleasesDecodedValuesAndClearsScratch(t *testing.T) {
	st := attemptStatePool.Get()
	decodedA := &attemptRec{V: 1}
	decodedB := &attemptRec{V: 2}
	released := 0
	st.release = func(ptr unsafe.Pointer) {
		released++
		(*attemptRec)(ptr).V = 0
	}
	st.decodedValues = append(st.decodedValues, unsafe.Pointer(decodedA), unsafe.Pointer(decodedB))

	payload := encodePool.Get()
	_, _ = payload.Write([]byte{1, 2, 3})
	st.ownedPayloads = append(st.ownedPayloads, payload)

	req := &request{op: opPatch}
	st.prepared = append(st.prepared, prepared{req: req, key: []byte{1}, payload: []byte{2}, idx: 7})
	st.accepted = append(st.accepted, prepared{req: req, key: []byte{3}, payload: []byte{4}, idx: 8})
	st.preparedSnapshots = append(st.preparedSnapshots, snapshot.BatchEntry{
		ID:    7,
		Patch: []schema.PatchItem{{Name: "v", Value: []byte{1}}},
	})
	st.acceptedSnapshots = append(st.acceptedSnapshots, snapshot.BatchEntry{ID: 8})
	st.states = append(st.states, recordState{idx: 7, value: unsafe.Pointer(decodedA), borrowedPayload: []byte{9}})
	st.state = recordState{idx: 9, value: unsafe.Pointer(decodedB), borrowedPayload: []byte{10}}
	st.stateByUintID = map[uint64]int{7: 1}
	st.stateByStringID = map[string]int{"k": 1}

	attemptStatePool.Put(st)

	if released != 2 || decodedA.V != 0 || decodedB.V != 0 {
		t.Fatalf("decoded release count=%d decoded=(%d,%d), want released zeroed values", released, decodedA.V, decodedB.V)
	}
	if payload.Len() != 0 {
		t.Fatalf("owned payload buffer was not reset: len=%d", payload.Len())
	}
	if len(st.decodedValues) != 0 || len(st.ownedPayloads) != 0 {
		t.Fatalf("attempt cleanup kept decoded/payload slices: decoded=%d payloads=%d", len(st.decodedValues), len(st.ownedPayloads))
	}
	if len(st.prepared) != 0 || len(st.accepted) != 0 || len(st.states) != 0 {
		t.Fatalf("attempt cleanup kept prepared/accepted/states: %d/%d/%d", len(st.prepared), len(st.accepted), len(st.states))
	}
	if len(st.preparedSnapshots) != 0 || len(st.acceptedSnapshots) != 0 {
		t.Fatalf("attempt cleanup kept snapshot entries: %d/%d", len(st.preparedSnapshots), len(st.acceptedSnapshots))
	}
	if len(st.stateByUintID) != 0 || len(st.stateByStringID) != 0 {
		t.Fatalf("attempt cleanup kept state maps: uint=%d string=%d", len(st.stateByUintID), len(st.stateByStringID))
	}
	if st.release != nil || st.state.value != nil || st.state.borrowedPayload != nil {
		t.Fatalf("attempt cleanup kept current state: release=%p state=%+v", st.release, st.state)
	}
}
