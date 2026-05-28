package wexec

import (
	"bytes"
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

func TestBuildSetRequestHookSuppressesCoalescingPolicy(t *testing.T) {
	ex := NewBatcher(Config{
		MaxOps:      8,
		Unavailable: func() error { return nil },
	})
	ex.ops = &RecordOps{
		Encode: func(unsafe.Pointer, *bytes.Buffer) error { return nil },
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
