package wexec

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/schema"
	"go.etcd.io/bbolt"
)

type requestPolicyRec struct {
	Email string   `db:"email" rbi:"unique"`
	Tags  []string `db:"tags" rbi:"index"`
}

func TestBuildPatchRequestRepeatIDPolicy(t *testing.T) {
	rt, err := schema.Compile(reflect.TypeFor[requestPolicyRec](), schema.Config{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	ex := NewBatcher(Config{
		MaxOps:      8,
		Unavailable: func() error { return nil },
	})
	ex.schema = rt
	ex.indexed = true

	req := ex.buildPatchRequest(keycodec.DataKeyFromUserKey(uint64(1), false), []schema.PatchItem{{Name: "tags"}}, true, nil, nil, nil)
	if !req.canShareRepeatedID() {
		t.Fatalf("non-unique patch was not marked repeat-id safe")
	}
	requestPool.Put(req)

	req = ex.buildPatchRequest(keycodec.DataKeyFromUserKey(uint64(1), false), []schema.PatchItem{{Name: "email"}}, true, nil, nil, nil)
	if req.canShareRepeatedID() {
		t.Fatalf("unique patch was marked repeat-id safe")
	}
	requestPool.Put(req)

	beforeProcess := []BeforeProcessHook{func(keycodec.DataKey, unsafe.Pointer) error { return nil }}
	req = ex.buildPatchRequest(keycodec.DataKeyFromUserKey(uint64(1), false), []schema.PatchItem{{Name: "tags"}}, true, beforeProcess, nil, nil)
	if req.canShareRepeatedID() {
		t.Fatalf("BeforeProcess patch on unique schema was marked repeat-id safe")
	}
	requestPool.Put(req)

	beforeStore := []BeforeStoreHook{func(keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error { return nil }}
	req = ex.buildPatchRequest(keycodec.DataKeyFromUserKey(uint64(1), false), []schema.PatchItem{{Name: "tags"}}, true, nil, beforeStore, nil)
	if req.canShareRepeatedID() {
		t.Fatalf("BeforeStore patch on unique schema was marked repeat-id safe")
	}
	requestPool.Put(req)

	ex.indexed = false
	req = ex.buildPatchRequest(keycodec.DataKeyFromUserKey(uint64(1), false), []schema.PatchItem{{Name: "email"}}, true, nil, nil, nil)
	if !req.canShareRepeatedID() {
		t.Fatalf("patch on non-indexed executor was not marked repeat-id safe")
	}
	requestPool.Put(req)
}

func TestBuildPatchRequestCopiesCallerPatchItemsShallow(t *testing.T) {
	ex := NewBatcher(Config{
		MaxOps:      8,
		Unavailable: func() error { return nil },
	})

	tags := []string{"go", "db"}
	patch := []schema.PatchItem{
		{Name: "tags", Value: tags},
	}
	req := ex.buildPatchRequest(keycodec.DataKeyFromUserKey(uint64(1), false), patch, true, nil, nil, nil)
	defer requestPool.Put(req)

	patch[0].Name = "name"
	patch[0].Value = []string{"other"}
	tags[0] = "mutated"

	if req.patch[0].Name != "tags" {
		t.Fatalf("request patch item name aliases caller slice: %q", req.patch[0].Name)
	}
	gotTags := req.patch[0].Value.([]string)
	if !reflect.DeepEqual(gotTags, []string{"mutated", "db"}) {
		t.Fatalf("request patch value was not used directly: %v", gotTags)
	}
}

func TestBuildSetRequestHookSuppressesCoalescingPolicy(t *testing.T) {
	ex := NewBatcher(Config{
		MaxOps:      8,
		Unavailable: func() error { return nil },
	})
	ex.ops = &RecordOps{
		Encode: func(unsafe.Pointer, *bytes.Buffer) error {
			return nil
		},
	}

	v := attemptRec{V: 1}
	req, err := ex.buildSetRequest(keycodec.DataKeyFromUserKey(uint64(1), false), unsafe.Pointer(&v), nil, nil, nil)
	if err != nil {
		t.Fatalf("buildSetRequest: %v", err)
	}
	if !req.canCoalesceSetDelete() {
		t.Fatalf("plain set request was not marked coalescible")
	}
	requestPool.Put(req)

	hooks := []BeforeCommitHook{func(*bbolt.Tx, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error { return nil }}
	req, err = ex.buildSetRequest(keycodec.DataKeyFromUserKey(uint64(2), false), unsafe.Pointer(&v), nil, hooks, nil)
	if err != nil {
		t.Fatalf("buildSetRequest with hook: %v", err)
	}
	if req.canCoalesceSetDelete() || req.canShareRepeatedID() {
		t.Fatalf("hooked set request inherited batching policy: %08b", req.policy)
	}
	if req.beforeCommit == nil {
		t.Fatalf("beforeCommit hooks were not attached")
	}
	requestPool.Put(req)
}

func TestBuildSetRequestRejectEmptyPayloadValidatesBeforePolicy(t *testing.T) {
	ex := NewBatcher(Config{
		MaxOps:             8,
		Unavailable:        func() error { return nil },
		RejectEmptyPayload: true,
	})
	ex.ops = &RecordOps{
		Encode: func(unsafe.Pointer, *bytes.Buffer) error {
			return nil
		},
	}

	v := attemptRec{V: 1}
	req, err := ex.buildSetRequest(keycodec.DataKeyFromUserKey(uint64(1), false), unsafe.Pointer(&v), nil, nil, nil)
	if req != nil {
		t.Fatalf("buildSetRequest returned request for empty payload: %#v", req)
	}
	if !errors.Is(err, errEmptyPayload) {
		t.Fatalf("buildSetRequest error = %v, want empty payload", err)
	}

	ex.ops.Encode = func(unsafe.Pointer, *bytes.Buffer) error {
		return nil
	}
	ex.rejectEmptyPayload = false
	req, err = ex.buildSetRequest(keycodec.DataKeyFromUserKey(uint64(2), false), unsafe.Pointer(&v), nil, nil, nil)
	if err != nil {
		t.Fatalf("buildSetRequest without reject-empty: %v", err)
	}
	if !req.canCoalesceSetDelete() {
		t.Fatalf("non-indexed set request was not marked coalescible")
	}
	requestPool.Put(req)

	ex.ops.Encode = func(_ unsafe.Pointer, buf *bytes.Buffer) error {
		_ = buf.WriteByte(1)
		return nil
	}
	ex.rejectEmptyPayload = true
	req, err = ex.buildSetRequest(keycodec.DataKeyFromUserKey(uint64(3), false), unsafe.Pointer(&v), nil, nil, nil)
	if err != nil {
		t.Fatalf("buildSetRequest with non-empty payload: %v", err)
	}
	if !req.canCoalesceSetDelete() {
		t.Fatalf("non-empty reject-empty set request was not marked coalescible")
	}
	requestPool.Put(req)
}

func TestBuildDeleteRequestStringKeySuppressesCoalescingPolicy(t *testing.T) {
	ex := NewBatcher(Config{
		MaxOps:      8,
		Unavailable: func() error { return nil },
	})

	req := ex.buildDeleteRequest(keycodec.DataKeyFromUserKey(uint64(1), false), nil)
	if !req.canShareRepeatedID() || !req.canCoalesceSetDelete() {
		t.Fatalf("numeric delete policy = %08b, want repeat-safe and coalescible", req.policy)
	}
	requestPool.Put(req)

	ex.rejectEmptyPayload = true
	req = ex.buildDeleteRequest(keycodec.DataKeyFromUserKey(uint64(2), false), nil)
	if !req.canShareRepeatedID() || !req.canCoalesceSetDelete() {
		t.Fatalf("reject-empty numeric delete policy = %08b, want repeat-safe and coalescible", req.policy)
	}
	requestPool.Put(req)

	ex.strKey = true
	req = ex.buildDeleteRequest(keycodec.DataKeyFromUserKey("alpha", true), nil)
	if !req.canShareRepeatedID() {
		t.Fatalf("string delete was not marked repeat-id safe")
	}
	if req.canCoalesceSetDelete() {
		t.Fatalf("string delete was marked set/delete coalescible: %08b", req.policy)
	}
	requestPool.Put(req)
}

func TestBuildSetRequestEncodeFailureReturnsError(t *testing.T) {
	encodeErr := errors.New("encode failed")
	ex := NewBatcher(Config{
		MaxOps:      8,
		Unavailable: func() error { return nil },
	})
	ex.ops = &RecordOps{
		Encode: func(unsafe.Pointer, *bytes.Buffer) error {
			return encodeErr
		},
	}

	v := attemptRec{V: 1}
	req, err := ex.buildSetRequest(keycodec.DataKeyFromUserKey(uint64(1), false), unsafe.Pointer(&v), nil, nil, nil)
	if req != nil {
		t.Fatalf("buildSetRequest returned request on encode failure: %#v", req)
	}
	if !errors.Is(err, encodeErr) {
		t.Fatalf("buildSetRequest error = %v, want encode error", err)
	}
}

func TestBuildSetRequestCloneFailureReturnsError(t *testing.T) {
	cloneErr := errors.New("clone failed")
	ex := NewBatcher(Config{
		MaxOps:      8,
		Unavailable: func() error { return nil },
	})

	v := attemptRec{V: 1}
	beforeStore := []BeforeStoreHook{func(keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error { return nil }}
	cloneValue := func(keycodec.DataKey, unsafe.Pointer) (unsafe.Pointer, error) {
		return nil, cloneErr
	}

	req, err := ex.buildSetRequest(keycodec.DataKeyFromUserKey(uint64(1), false), unsafe.Pointer(&v), beforeStore, nil, cloneValue)
	if req != nil {
		t.Fatalf("buildSetRequest returned request on clone failure: %#v", req)
	}
	if !errors.Is(err, cloneErr) {
		t.Fatalf("buildSetRequest error = %v, want clone error", err)
	}
}

func TestBuildSetRequestCloneBaselineReleasedOnRequestCleanup(t *testing.T) {
	ex := NewBatcher(Config{
		MaxOps:      8,
		Unavailable: func() error { return nil },
	})
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
	beforeStore := []BeforeStoreHook{func(keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error { return nil }}
	cloneValue := func(_ keycodec.DataKey, value unsafe.Pointer) (unsafe.Pointer, error) {
		cp := *(*attemptRec)(value)
		baseline = &cp
		return unsafe.Pointer(baseline), nil
	}

	req, err := ex.buildSetRequest(keycodec.DataKeyFromUserKey(uint64(1), false), unsafe.Pointer(&v), beforeStore, nil, cloneValue)
	if err != nil {
		t.Fatalf("buildSetRequest: %v", err)
	}
	if req.setBaseline != unsafe.Pointer(baseline) {
		t.Fatalf("baseline pointer = %p, want %p", req.setBaseline, baseline)
	}

	ex.releaseRequest(req)

	if released != 1 || releasedPtr != unsafe.Pointer(baseline) || baseline.V != 0 {
		t.Fatalf("baseline release count=%d ptr=%p value=%d", released, releasedPtr, baseline.V)
	}
}

func TestOpNamesMatchExecutionMode(t *testing.T) {
	if got := opName(opSet, 1, false, 16); got != "batch" {
		t.Fatalf("shared single request op = %q, want batch", got)
	}
	if got := opName(opSet, 1, true, 16); got != "set" {
		t.Fatalf("isolated single request op = %q, want set", got)
	}
	if got := opName(opSet, 2, true, 16); got != "batch_set" {
		t.Fatalf("isolated multi set op = %q, want batch_set", got)
	}
	if got := opName(opPatch, 1, false, 1); got != "patch" {
		t.Fatalf("single-only patch op = %q, want patch", got)
	}
}
