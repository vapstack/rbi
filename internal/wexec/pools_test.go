package wexec

import (
	"errors"
	"testing"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
)

func TestReleaseRequestReleasesPayloadAndClearsState(t *testing.T) {
	ex := NewExecutor(Config{})
	payload := encodePool.Get()
	_, _ = payload.Write([]byte{1, 2, 3})

	v := attemptRec{V: 7}
	released := false
	ex.ops = &RecordOps{Release: func(ptr unsafe.Pointer) {
		released = true
		if ptr != unsafe.Pointer(&v) {
			t.Fatalf("released ptr=%p want %p", ptr, &v)
		}
	}}
	errSentinel := errors.New("request failed")
	req := request{}
	req.op = opSet
	req.id = keycodec.DataKeyFromUserKey(uint64(42), false)
	req.setValue = unsafe.Pointer(&v)
	req.setPayload = payload
	req.payloadOff = 8
	req.patch = append(req.patch, schema.PatchItem{Name: "x"})
	req.patchIgnoreUnknown = true
	req.onChange = append(req.onChange, func(unsafe.Pointer, uint8, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error { return nil })
	req.Err = errSentinel

	ex.releaseRequest(&req)

	if req.op != 0 || req.id != (keycodec.DataKey{}) || req.Err != nil {
		t.Fatalf("request identity was not cleared: op=%v id=%v err=%v", req.op, req.id, req.Err)
	}
	if req.setPayload != nil || req.setValue != nil {
		t.Fatalf("set state was not cleared: payload=%v value=%v", req.setPayload, req.setValue)
	}
	if req.payloadOff != 0 {
		t.Fatalf("payload offset was not cleared: %d", req.payloadOff)
	}
	if len(req.patch) != 0 || req.patchIgnoreUnknown {
		t.Fatalf("patch state was not cleared: len=%d ignore=%v", len(req.patch), req.patchIgnoreUnknown)
	}
	if req.onChange != nil {
		t.Fatalf("callbacks were not cleared")
	}
	if !released {
		t.Fatalf("set value was not released")
	}
}

func TestAttemptStatePoolCleanupReleasesValuesAndClearsScratch(t *testing.T) {
	st := attemptStatePool.Get()
	valueA := &attemptRec{V: 1}
	valueB := &attemptRec{V: 2}
	released := 0
	st.release = func(ptr unsafe.Pointer) {
		released++
		(*attemptRec)(ptr).V = 0
	}
	st.releaseValues = append(st.releaseValues, unsafe.Pointer(valueA), unsafe.Pointer(valueB))

	payload := encodePool.Get()
	_, _ = payload.Write([]byte{1, 2, 3})
	st.ownedPayloads = append(st.ownedPayloads, payload)

	req := &request{op: opPatch}
	st.prepared = append(st.prepared, prepared{req: req, payload: []byte{2}, idx: 7})
	st.accepted = append(st.accepted, prepared{req: req, payload: []byte{4}, idx: 8})
	st.preparedSnapshots = append(st.preparedSnapshots, snapshot.BatchEntry{
		ID:    7,
		Patch: []schema.PatchItem{{Name: "v", Value: []byte{1}}},
	})
	st.acceptedSnapshots = append(st.acceptedSnapshots, snapshot.BatchEntry{ID: 8})
	st.states = append(st.states, recordState{idx: 7, value: unsafe.Pointer(valueA), borrowedPayload: []byte{9}})
	st.state = recordState{idx: 9, value: unsafe.Pointer(valueB), borrowedPayload: []byte{10}}
	st.stateByUintID = map[uint64]int{7: 1}
	st.stateByStringID = map[string]int{"k": 1}

	attemptStatePool.Put(st)

	if released != 2 || valueA.V != 0 || valueB.V != 0 {
		t.Fatalf("release count=%d values=(%d,%d), want released zeroed values", released, valueA.V, valueB.V)
	}
	if payload.Len() != 0 {
		t.Fatalf("owned payload buffer was not reset: len=%d", payload.Len())
	}
	if len(st.releaseValues) != 0 || len(st.ownedPayloads) != 0 {
		t.Fatalf("attempt cleanup kept value/payload slices: values=%d payloads=%d", len(st.releaseValues), len(st.ownedPayloads))
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
	if st.release != nil || st.state.value != nil || st.state.payloadKnown || st.state.borrowedPayload != nil {
		t.Fatalf("attempt cleanup kept current state: release=%p state=%+v", st.release, st.state)
	}
}
