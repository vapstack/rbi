package wexec

import (
	"bytes"
	"errors"
	"testing"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
)

func TestBuildSetRequestRejectEmptyPayloadBeforeReturningRequest(t *testing.T) {
	ex := NewExecutor(Config{})
	ex.ops = &RecordOps{
		Encode: func(unsafe.Pointer, *bytes.Buffer) {},
	}

	v := attemptRec{V: 1}
	req, err := ex.buildSetRequest(keycodec.DataKeyFromUserKey(uint64(1), false), unsafe.Pointer(&v), nil, 0)
	if req.op != 0 {
		t.Fatalf("buildSetRequest returned request for empty payload: %#v", req)
	}
	if !errors.Is(err, errEmptyPayload) {
		t.Fatalf("buildSetRequest error = %v, want empty payload", err)
	}

	ex.ops.Encode = func(_ unsafe.Pointer, buf *bytes.Buffer) {
		_ = buf.WriteByte(1)
	}
	req, err = ex.buildSetRequest(keycodec.DataKeyFromUserKey(uint64(3), false), unsafe.Pointer(&v), nil, 0)
	if err != nil {
		t.Fatalf("buildSetRequest with non-empty payload: %v", err)
	}
	ex.releaseRequest(&req)
}

func TestBuildSetRequestOnChangeDetachesValue(t *testing.T) {
	ex := NewExecutor(Config{})
	ex.ops = &RecordOps{
		Acquire: func() unsafe.Pointer {
			return unsafe.Pointer(&attemptRec{})
		},
		CloneInto: func(src unsafe.Pointer, dst unsafe.Pointer) {
			*(*attemptRec)(dst) = *(*attemptRec)(src)
		},
		Release: func(unsafe.Pointer) {},
	}

	v := attemptRec{V: 1}
	onChange := []OnChangeHook{func(unsafe.Pointer, uint8, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error { return nil }}

	req, err := ex.buildSetRequest(keycodec.DataKeyFromUserKey(uint64(1), false), unsafe.Pointer(&v), onChange, 0)
	if err != nil {
		t.Fatalf("buildSetRequest: %v", err)
	}
	defer ex.releaseRequest(&req)
	v.V = 9
	if got := (*attemptRec)(req.setValue).V; got != 1 {
		t.Fatalf("detached value=%d want 1", got)
	}
}

func TestBuildSetRequestDetachedValueReleasedOnRequestCleanup(t *testing.T) {
	ex := NewExecutor(Config{})
	released := 0
	var releasedPtr unsafe.Pointer
	baseline := &attemptRec{}
	ex.ops = &RecordOps{
		Acquire: func() unsafe.Pointer {
			return unsafe.Pointer(baseline)
		},
		CloneInto: func(src unsafe.Pointer, dst unsafe.Pointer) {
			*(*attemptRec)(dst) = *(*attemptRec)(src)
		},
		Release: func(ptr unsafe.Pointer) {
			released++
			releasedPtr = ptr
			(*attemptRec)(ptr).V = 0
		},
	}

	v := attemptRec{V: 7}
	onChange := []OnChangeHook{func(unsafe.Pointer, uint8, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error { return nil }}

	req, err := ex.buildSetRequest(keycodec.DataKeyFromUserKey(uint64(1), false), unsafe.Pointer(&v), onChange, 0)
	if err != nil {
		t.Fatalf("buildSetRequest: %v", err)
	}
	if req.setValue != unsafe.Pointer(baseline) {
		t.Fatalf("detached value pointer = %p, want %p", req.setValue, baseline)
	}

	ex.releaseRequest(&req)

	if released != 1 || releasedPtr != unsafe.Pointer(baseline) || baseline.V != 0 {
		t.Fatalf("detached value release count=%d ptr=%p value=%d", released, releasedPtr, baseline.V)
	}
}
