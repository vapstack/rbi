package wexec

import (
	"bytes"
	"errors"
	"testing"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
)

func TestBuildSetRequestRejectEmptyPayloadBeforeReturningRequest(t *testing.T) {
	ex := NewExecutor(Config{
		RejectEmptyPayload: true,
	})
	ex.ops = &RecordOps{
		Encode: func(unsafe.Pointer, *bytes.Buffer) error {
			return nil
		},
	}

	v := attemptRec{V: 1}
	req, err := ex.buildSetRequest(keycodec.DataKeyFromUserKey(uint64(1), false), unsafe.Pointer(&v), nil, nil, 0)
	if req.op != 0 {
		t.Fatalf("buildSetRequest returned request for empty payload: %#v", req)
	}
	if !errors.Is(err, errEmptyPayload) {
		t.Fatalf("buildSetRequest error = %v, want empty payload", err)
	}

	ex.ops.Encode = func(unsafe.Pointer, *bytes.Buffer) error {
		return nil
	}
	ex.rejectEmptyPayload = false
	req, err = ex.buildSetRequest(keycodec.DataKeyFromUserKey(uint64(2), false), unsafe.Pointer(&v), nil, nil, 0)
	if err != nil {
		t.Fatalf("buildSetRequest without reject-empty: %v", err)
	}
	ex.releaseRequest(&req)

	ex.ops.Encode = func(_ unsafe.Pointer, buf *bytes.Buffer) error {
		_ = buf.WriteByte(1)
		return nil
	}
	ex.rejectEmptyPayload = true
	req, err = ex.buildSetRequest(keycodec.DataKeyFromUserKey(uint64(3), false), unsafe.Pointer(&v), nil, nil, 0)
	if err != nil {
		t.Fatalf("buildSetRequest with non-empty payload: %v", err)
	}
	ex.releaseRequest(&req)
}

func TestBuildSetRequestEncodeFailureReturnsError(t *testing.T) {
	encodeErr := errors.New("encode failed")
	ex := NewExecutor(Config{})
	ex.ops = &RecordOps{
		Encode: func(unsafe.Pointer, *bytes.Buffer) error {
			return encodeErr
		},
	}

	v := attemptRec{V: 1}
	req, err := ex.buildSetRequest(keycodec.DataKeyFromUserKey(uint64(1), false), unsafe.Pointer(&v), nil, nil, 0)
	if req.op != 0 {
		t.Fatalf("buildSetRequest returned request on encode failure: %#v", req)
	}
	if !errors.Is(err, encodeErr) {
		t.Fatalf("buildSetRequest error = %v, want encode error", err)
	}
}

func TestBuildSetRequestCloneFailureReturnsError(t *testing.T) {
	cloneErr := errors.New("clone failed")
	ex := NewExecutor(Config{})

	v := attemptRec{V: 1}
	onChange := []OnChangeHook{func(unsafe.Pointer, uint8, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error { return nil }}
	cloneValue := func(keycodec.DataKey, unsafe.Pointer) (unsafe.Pointer, error) {
		return nil, cloneErr
	}

	req, err := ex.buildSetRequest(keycodec.DataKeyFromUserKey(uint64(1), false), unsafe.Pointer(&v), onChange, cloneValue, 0)
	if req.op != 0 {
		t.Fatalf("buildSetRequest returned request on clone failure: %#v", req)
	}
	if !errors.Is(err, cloneErr) {
		t.Fatalf("buildSetRequest error = %v, want clone error", err)
	}
}

func TestBuildSetRequestCloneBaselineReleasedOnRequestCleanup(t *testing.T) {
	ex := NewExecutor(Config{})
	released := 0
	var releasedPtr unsafe.Pointer
	ex.ops = &RecordOps{
		Release: func(ptr unsafe.Pointer) {
			released++
			releasedPtr = ptr
			(*attemptRec)(ptr).V = 0
		},
	}

	v := attemptRec{V: 7}
	var baseline *attemptRec
	onChange := []OnChangeHook{func(unsafe.Pointer, uint8, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error { return nil }}
	cloneValue := func(_ keycodec.DataKey, value unsafe.Pointer) (unsafe.Pointer, error) {
		cp := *(*attemptRec)(value)
		baseline = &cp
		return unsafe.Pointer(baseline), nil
	}

	req, err := ex.buildSetRequest(keycodec.DataKeyFromUserKey(uint64(1), false), unsafe.Pointer(&v), onChange, cloneValue, 0)
	if err != nil {
		t.Fatalf("buildSetRequest: %v", err)
	}
	if req.setBaseline != unsafe.Pointer(baseline) {
		t.Fatalf("baseline pointer = %p, want %p", req.setBaseline, baseline)
	}

	ex.releaseRequest(&req)

	if released != 1 || releasedPtr != unsafe.Pointer(baseline) || baseline.V != 0 {
		t.Fatalf("baseline release count=%d ptr=%p value=%d", released, releasedPtr, baseline.V)
	}
}
