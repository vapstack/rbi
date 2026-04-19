package rbi

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"github.com/vmihailenco/msgpack/v5"
	"go.etcd.io/bbolt"
)

type beforeStoreCloneRec struct {
	Name  string `db:"name"  dbi:"default"`
	Ready bool   `db:"ready" dbi:"default"`
}

func (r *beforeStoreCloneRec) Clone() *beforeStoreCloneRec {
	if r == nil {
		return nil
	}
	cp := *r
	return &cp
}

func (r *beforeStoreCloneRec) MarshalMsgpack() ([]byte, error) {
	if !r.Ready {
		return nil, errors.New("beforeStoreCloneRec: not ready")
	}
	type alias beforeStoreCloneRec
	return msgpack.Marshal((*alias)(r))
}

func (r *beforeStoreCloneRec) UnmarshalMsgpack(b []byte) error {
	type alias beforeStoreCloneRec
	var tmp alias
	if err := msgpack.Unmarshal(b, &tmp); err != nil {
		return err
	}
	*r = beforeStoreCloneRec(tmp)
	return nil
}

func openTempDBUint64BeforeStoreCloneRec(t *testing.T, options ...Options) *DB[uint64, beforeStoreCloneRec] {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_before_store_clone.db")
	db, raw := openBoltAndNew[uint64, beforeStoreCloneRec](t, path, options...)
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	return db
}

func TestPatchStrictOption_StructTags_NumConversion(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{
		Name:     "alice",
		Age:      10,
		Tags:     []string{"go"},
		FullName: "Alice A.",
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.Patch(1, []Field{{Name: "age", Value: 42.0}}, PatchStrict); err != nil {
		t.Fatalf("Patch(..., PatchStrict) age float->int: %v", err)
	}
	v, err := db.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if v.Age != 42 {
		t.Fatalf("age not patched: got %d", v.Age)
	}

	if err = db.Patch(1, []Field{{Name: "fullName", Value: "Alice Alpha"}}, PatchStrict); err != nil {
		t.Fatalf("Patch(..., PatchStrict) json tag: %v", err)
	}
	v, err = db.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if v.FullName != "Alice Alpha" {
		t.Fatalf("full name not patched: got %q", v.FullName)
	}

	err = db.Patch(1, []Field{{Name: "age", Value: 1.25}}, PatchStrict)
	if err == nil {
		t.Fatalf("expected error on float->int with fraction")
	}
	v, err = db.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if v.Age != 42 {
		t.Fatalf("age changed despite failed patch: got %d", v.Age)
	}
}

func TestPatchStrictOption_NilRules(t *testing.T) {
	db, _ := openTempDBUint64(t)

	s := "opt"
	if err := db.Set(1, &Rec{
		Name: "alice", Age: 10, Opt: &s,
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.Patch(1, []Field{{Name: "opt", Value: nil}}, PatchStrict); err != nil {
		t.Fatalf("Patch(..., PatchStrict) opt=nil: %v", err)
	}
	v, err := db.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if v.Opt != nil {
		t.Fatalf("expected opt=nil after patch")
	}

	err = db.Patch(1, []Field{{Name: "age", Value: nil}}, PatchStrict)
	if err == nil {
		t.Fatalf("expected error for age=nil")
	}
}

func TestBeforeStore_Patch_MutatesFinalStoredValue(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Age: 10, Meta: Meta{Country: "NL"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	calls := 0
	err := db.Patch(1, []Field{{Name: "age", Value: 20}}, BeforeStore(func(key uint64, oldValue, newValue *Rec) error {
		calls++
		if key != 1 {
			t.Fatalf("unexpected key: %d", key)
		}
		if oldValue == nil || oldValue.Age != 10 || oldValue.Country != "NL" {
			t.Fatalf("unexpected old value: %#v", oldValue)
		}
		if newValue == nil || newValue.Age != 20 {
			t.Fatalf("unexpected patched value before BeforeStore mutation: %#v", newValue)
		}
		newValue.Country = "US"
		newValue.Name = "alice-updated"
		return nil
	}))
	if err != nil {
		t.Fatalf("Patch with BeforeStore: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected BeforeStore to run once, got %d", calls)
	}

	v, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v == nil || v.Name != "alice-updated" || v.Age != 20 || v.Country != "US" {
		t.Fatalf("unexpected stored value: %#v", v)
	}
}

func TestBeforeProcess_Set_MutatesCallerOwnedValue(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	input := &Rec{Name: "alice", Age: 10, Meta: Meta{Country: "NL"}}
	calls := 0
	err := db.Set(1, input, BeforeProcess(func(key uint64, value *Rec) error {
		calls++
		if key != 1 {
			t.Fatalf("unexpected key: %d", key)
		}
		value.Name = "alice-before-insert"
		value.Country = "US"
		value.Age++
		return nil
	}))
	if err != nil {
		t.Fatalf("Set with BeforeProcess: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected BeforeProcess to run once, got %d", calls)
	}
	if input.Name != "alice-before-insert" || input.Country != "US" || input.Age != 11 {
		t.Fatalf("expected caller-owned value to be mutated in place, got %#v", input)
	}

	got, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil || got.Name != "alice-before-insert" || got.Country != "US" || got.Age != 11 {
		t.Fatalf("unexpected stored value: %#v", got)
	}
}

func TestBeforeProcess_Patch_MutatesFinalStoredValue(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Age: 10, Meta: Meta{Country: "NL"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	calls := 0
	err := db.Patch(1, []Field{{Name: "age", Value: 20}}, BeforeProcess(func(key uint64, newValue *Rec) error {
		calls++
		if key != 1 {
			t.Fatalf("unexpected key: %d", key)
		}
		if newValue == nil || newValue.Age != 20 {
			t.Fatalf("unexpected patched value before BeforeProcess mutation: %#v", newValue)
		}
		newValue.Country = "US"
		newValue.Name = "alice-updated"
		return nil
	}))
	if err != nil {
		t.Fatalf("Patch with BeforeProcess: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected BeforeProcess to run once, got %d", calls)
	}

	v, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v == nil || v.Name != "alice-updated" || v.Age != 20 || v.Country != "US" {
		t.Fatalf("unexpected stored value: %#v", v)
	}
}

func TestBeforeProcess_MissingPatch_IsNoOp(t *testing.T) {
	db, _ := openTempDBUint64(t)

	calls := 0
	err := db.Patch(42, []Field{{Name: "age", Value: 99}}, BeforeProcess(func(_ uint64, _ *Rec) error {
		calls++
		return nil
	}))
	if err != nil {
		t.Fatalf("Patch(missing): %v", err)
	}
	if calls != 0 {
		t.Fatalf("expected BeforeProcess not to run for missing Patch target, got %d", calls)
	}
}

func TestBeforeStore_BatchSet_SharedPointerUsesIndependentWorkingCopies(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	shared := &Rec{Name: "shared", Age: 10, Meta: Meta{Country: "NL"}}
	err := db.BatchSet(
		[]uint64{1, 2},
		[]*Rec{shared, shared},
		BeforeStore(func(key uint64, oldValue, newValue *Rec) error {
			if oldValue != nil {
				t.Fatalf("expected insert oldValue=nil, got %#v", oldValue)
			}
			newValue.Name = fmt.Sprintf("user-%d", key)
			newValue.Age += int(key)
			newValue.Country = fmt.Sprintf("C%d", key)
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("BatchSet with BeforeStore: %v", err)
	}

	if shared.Name != "shared" || shared.Age != 10 || shared.Country != "NL" {
		t.Fatalf("caller-owned shared value was mutated: %#v", shared)
	}

	v1, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	v2, err := db.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if v1 == nil || v1.Name != "user-1" || v1.Age != 11 || v1.Country != "C1" {
		t.Fatalf("unexpected value for id=1: %#v", v1)
	}
	if v2 == nil || v2.Name != "user-2" || v2.Age != 12 || v2.Country != "C2" {
		t.Fatalf("unexpected value for id=2: %#v", v2)
	}
}

func TestBeforeStore_MissingPatch_IsNoOp(t *testing.T) {
	db, _ := openTempDBUint64(t)

	calls := 0
	err := db.Patch(42, []Field{{Name: "age", Value: 99}}, BeforeStore(func(_ uint64, _ *Rec, _ *Rec) error {
		calls++
		return nil
	}))
	if err != nil {
		t.Fatalf("Patch(missing): %v", err)
	}
	if calls != 0 {
		t.Fatalf("expected BeforeStore not to run for missing Patch target, got %d", calls)
	}
}

func TestBeforeStore_CloneFunc_Set_AllowsNormalizationBeforeEncode(t *testing.T) {
	db := openTempDBUint64BeforeStoreCloneRec(t, Options{AutoBatchMax: 1})

	calls := 0
	input := &beforeStoreCloneRec{}
	err := db.Set(
		1,
		input,
		CloneFunc(func(_ uint64, v *beforeStoreCloneRec) *beforeStoreCloneRec {
			cp := *v
			return &cp
		}),
		BeforeStore(func(key uint64, oldValue, newValue *beforeStoreCloneRec) error {
			calls++
			if key != 1 {
				t.Fatalf("unexpected key: %d", key)
			}
			if oldValue != nil {
				t.Fatalf("expected insert oldValue=nil, got %#v", oldValue)
			}
			newValue.Name = "normalized"
			newValue.Ready = true
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("Set with CloneFunc: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected BeforeStore to run once, got %d", calls)
	}
	if input.Name != "" || input.Ready {
		t.Fatalf("caller-owned input was mutated: %#v", input)
	}

	got, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil || got.Name != "normalized" || !got.Ready {
		t.Fatalf("unexpected stored value: %#v", got)
	}
}

func TestBeforeStore_AutoCloneMethod_Set_AllowsNormalizationBeforeEncode(t *testing.T) {
	db := openTempDBUint64BeforeStoreCloneRec(t, Options{AutoBatchMax: 1})

	calls := 0
	input := &beforeStoreCloneRec{}
	err := db.Set(
		1,
		input,
		BeforeStore(func(key uint64, oldValue, newValue *beforeStoreCloneRec) error {
			calls++
			if key != 1 {
				t.Fatalf("unexpected key: %d", key)
			}
			if oldValue != nil {
				t.Fatalf("expected insert oldValue=nil, got %#v", oldValue)
			}
			newValue.Name = "normalized"
			newValue.Ready = true
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("Set with Clone() method: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected BeforeStore to run once, got %d", calls)
	}
	if input.Name != "" || input.Ready {
		t.Fatalf("caller-owned input was mutated: %#v", input)
	}

	got, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil || got.Name != "normalized" || !got.Ready {
		t.Fatalf("unexpected stored value: %#v", got)
	}
}

func TestBeforeStore_CloneFunc_BatchSet_AllowsNormalizationBeforeEncode(t *testing.T) {
	db := openTempDBUint64BeforeStoreCloneRec(t, Options{AutoBatchMax: 1})

	inputA := &beforeStoreCloneRec{}
	inputB := &beforeStoreCloneRec{}
	err := db.BatchSet(
		[]uint64{1, 2},
		[]*beforeStoreCloneRec{inputA, inputB},
		CloneFunc(func(_ uint64, v *beforeStoreCloneRec) *beforeStoreCloneRec {
			cp := *v
			return &cp
		}),
		BeforeStore(func(key uint64, oldValue, newValue *beforeStoreCloneRec) error {
			if oldValue != nil {
				t.Fatalf("expected insert oldValue=nil, got %#v", oldValue)
			}
			newValue.Name = fmt.Sprintf("normalized-%d", key)
			newValue.Ready = true
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("BatchSet with CloneFunc: %v", err)
	}
	if inputA.Name != "" || inputA.Ready || inputB.Name != "" || inputB.Ready {
		t.Fatalf("caller-owned inputs were mutated: %#v %#v", inputA, inputB)
	}

	got1, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	got2, err := db.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if got1 == nil || got1.Name != "normalized-1" || !got1.Ready {
		t.Fatalf("unexpected stored value for id=1: %#v", got1)
	}
	if got2 == nil || got2.Name != "normalized-2" || !got2.Ready {
		t.Fatalf("unexpected stored value for id=2: %#v", got2)
	}
}

func TestBeforeStore_AutoCloneMethod_BatchSet_AllowsNormalizationBeforeEncode(t *testing.T) {
	db := openTempDBUint64BeforeStoreCloneRec(t, Options{AutoBatchMax: 1})

	inputA := &beforeStoreCloneRec{}
	inputB := &beforeStoreCloneRec{}
	err := db.BatchSet(
		[]uint64{1, 2},
		[]*beforeStoreCloneRec{inputA, inputB},
		BeforeStore(func(key uint64, oldValue, newValue *beforeStoreCloneRec) error {
			if oldValue != nil {
				t.Fatalf("expected insert oldValue=nil, got %#v", oldValue)
			}
			newValue.Name = fmt.Sprintf("normalized-%d", key)
			newValue.Ready = true
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("BatchSet with Clone() method: %v", err)
	}
	if inputA.Name != "" || inputA.Ready || inputB.Name != "" || inputB.Ready {
		t.Fatalf("caller-owned inputs were mutated: %#v %#v", inputA, inputB)
	}

	got1, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	got2, err := db.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if got1 == nil || got1.Name != "normalized-1" || !got1.Ready {
		t.Fatalf("unexpected stored value for id=1: %#v", got1)
	}
	if got2 == nil || got2.Name != "normalized-2" || !got2.Ready {
		t.Fatalf("unexpected stored value for id=2: %#v", got2)
	}
}

func TestBatchSet_FailedBuild_ReleasesPreparedRequestsBeforePooling(t *testing.T) {
	db := openTempDBUint64BeforeStoreCloneRec(t, Options{AutoBatchMax: 1})

	err := db.BatchSet(
		[]uint64{1, 2},
		[]*beforeStoreCloneRec{
			{Name: "ok", Ready: true},
			{Name: "bad", Ready: false},
		},
	)
	if err == nil || !strings.Contains(err.Error(), "encode") {
		t.Fatalf("BatchSet build error = %v, want encode failure", err)
	}

	req, err := db.buildSetAutoBatchRequest(
		3,
		&beforeStoreCloneRec{Name: "hooked", Ready: true},
		nil,
		[]beforeCommitFunc[uint64, beforeStoreCloneRec]{
			func(_ *bbolt.Tx, _ uint64, _ *beforeStoreCloneRec, _ *beforeStoreCloneRec) error { return nil },
		},
		nil,
	)
	if err != nil {
		t.Fatalf("buildSetAutoBatchRequest after failed BatchSet: %v", err)
	}
	defer func() {
		db.autoBatcher.requestPool.Put(req)
	}()

	if req.policy != 0 {
		t.Fatalf("request policy after failed BatchSet = %08b, want 0", req.policy)
	}
	if req.canCoalesceSetDelete() {
		t.Fatal("hooked set request inherited coalescing flag after failed BatchSet")
	}
	if req.hasPolicy(autoBatchReqRepeatIDSafeShared) {
		t.Fatal("hooked set request inherited repeated-id flag after failed BatchSet")
	}
	if req.beforeCommit == nil {
		t.Fatal("beforeCommit hooks were not attached")
	}
}

func TestBeforeStore_CloneFunc_AutoBatch_AllowsNormalizationBeforeEncode(t *testing.T) {
	db := openTempDBUint64BeforeStoreCloneRec(t)

	calls := 0
	err := db.Set(
		1,
		&beforeStoreCloneRec{},
		CloneFunc(func(_ uint64, v *beforeStoreCloneRec) *beforeStoreCloneRec {
			cp := *v
			return &cp
		}),
		BeforeStore(func(_ uint64, _ *beforeStoreCloneRec, newValue *beforeStoreCloneRec) error {
			calls++
			newValue.Name = "combined"
			newValue.Ready = true
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("Set with CloneFunc via auto-batcher: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected BeforeStore to run once, got %d", calls)
	}

	got, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil || got.Name != "combined" || !got.Ready {
		t.Fatalf("unexpected stored value: %#v", got)
	}

	if st := db.AutoBatchStats(); st.Enqueued == 0 {
		t.Fatalf("expected Set with CloneFunc to use auto-batcher path, stats=%+v", st)
	}
}

func TestBeforeStore_AutoCloneMethod_AutoBatch_AllowsNormalizationBeforeEncode(t *testing.T) {
	db := openTempDBUint64BeforeStoreCloneRec(t)

	calls := 0
	err := db.Set(
		1,
		&beforeStoreCloneRec{},
		BeforeStore(func(_ uint64, _ *beforeStoreCloneRec, newValue *beforeStoreCloneRec) error {
			calls++
			newValue.Name = "combined"
			newValue.Ready = true
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("Set with Clone() method via auto-batcher: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected BeforeStore to run once, got %d", calls)
	}

	got, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil || got.Name != "combined" || !got.Ready {
		t.Fatalf("unexpected stored value: %#v", got)
	}

	if st := db.AutoBatchStats(); st.Enqueued == 0 {
		t.Fatalf("expected Set with Clone() method to use auto-batcher path, stats=%+v", st)
	}
}

func TestBeforeStore_CloneFunc_OverridesAutoCloneMethod(t *testing.T) {
	db := openTempDBUint64BeforeStoreCloneRec(t, Options{AutoBatchMax: 1})

	err := db.Set(
		1,
		&beforeStoreCloneRec{},
		CloneFunc(func(_ uint64, v *beforeStoreCloneRec) *beforeStoreCloneRec {
			cp := *v
			cp.Name = "clone-func"
			return &cp
		}),
		BeforeStore(func(_ uint64, _ *beforeStoreCloneRec, newValue *beforeStoreCloneRec) error {
			if newValue.Name != "clone-func" {
				t.Fatalf("expected explicit CloneFunc to win, got name=%q", newValue.Name)
			}
			newValue.Ready = true
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("Set with explicit CloneFunc override: %v", err)
	}
}

func TestMakePatch_BeforeCommit_DeepCopy_SliceValues(t *testing.T) {
	db, _ := openTempDBUint64(t)

	rec := &Rec{
		Name: "alice",
		Age:  10,
		Tags: []string{"a"},
	}
	if err := db.Set(1, rec); err != nil {
		t.Fatalf("Set: %v", err)
	}

	patch := make([]Field, 0, 8)
	// makePatch := db.CollectPatch(&patch)

	origTags := []string{"x", "y"} // will be mutated later to validate deep copy
	updated := &Rec{
		Name: "bob", // changed
		Age:  10,    // unchanged
		Tags: origTags,
	}

	if err := db.Set(1, updated, BeforeCommit(func(_ *bbolt.Tx, _ uint64, oldValue, newValue *Rec) error {
		patch = db.MakePatch(oldValue, newValue)
		return nil
	})); err != nil {
		t.Fatalf("Set(update): %v", err)
	}

	got := make(map[string]any, len(patch))
	for _, f := range patch {
		got[f.Name] = f.Value
	}

	// expect changed fields: name and tags

	if _, ok := got["name"]; !ok {
		t.Fatalf("expected patch to include %q, got %#v", "name", patch)
	}
	if _, ok := got["tags"]; !ok {
		t.Fatalf("expected patch to include %q, got %#v", "tags", patch)
	}
	if _, ok := got["age"]; ok {
		t.Fatalf("did not expect patch to include unchanged field %q, got %#v", "age", patch)
	}

	if v, _ := got["name"].(string); v != "bob" {
		t.Fatalf("expected patched name %q, got %#v", "bob", got["name"])
	}

	gotTags, _ := got["tags"].([]string)
	if len(gotTags) != 2 || gotTags[0] != "x" || gotTags[1] != "y" {
		t.Fatalf("expected patched tags [x y], got %#v", gotTags)
	}

	origTags[0] = "MUTATED"

	gotTags2, _ := got["tags"].([]string)
	if len(gotTags2) != 2 || gotTags2[0] != "x" || gotTags2[1] != "y" {
		t.Fatalf("expected deep-copied tags [x y], got %#v", gotTags2)
	}
}

func TestMakePatch_EmitsNilToEmptySliceTransition(t *testing.T) {
	db, _ := openTempDBUint64(t)

	oldVal := &Rec{Name: "alice"}
	newVal := &Rec{Name: "alice", Tags: []string{}}

	patch := db.MakePatch(oldVal, newVal)
	fields := patchFieldsByName(patch)

	gotTags, ok := fields["tags"].([]string)
	if !ok {
		t.Fatalf("patch must contain []string value for tags, got %#v", fields["tags"])
	}
	if gotTags == nil || len(gotTags) != 0 {
		t.Fatalf("patch must preserve non-nil empty slice, got %#v", gotTags)
	}
	if len(patch) != 1 {
		t.Fatalf("expected exactly one changed field, got %#v", patch)
	}

	applied := *oldVal
	if err := db.applyPatch(&applied, patch, false); err != nil {
		t.Fatalf("applyPatch: %v", err)
	}
	if applied.Tags == nil || len(applied.Tags) != 0 {
		t.Fatalf("patched record lost non-nil empty slice: %#v", applied.Tags)
	}
}

func TestMakePatch_DefaultUsesDBNamesAndRoundTripsViaPatch(t *testing.T) {
	db, _ := openTempDBUint64(t)

	oldVal := &Rec{
		Name:     "alice",
		FullName: "Alice A.",
	}
	newVal := &Rec{
		Name:     "bob",
		FullName: "Bob B.",
	}

	mustSetAPIRec(t, db, 1, oldVal)

	patch := db.MakePatch(oldVal, newVal)
	fields := patchFieldsByName(patch)

	if _, ok := fields["name"]; !ok {
		t.Fatalf("expected patch to include %q, got %#v", "name", patch)
	}
	if _, ok := fields["full_name"]; !ok {
		t.Fatalf("expected patch to include %q, got %#v", "full_name", patch)
	}
	if _, ok := fields["Name"]; ok {
		t.Fatalf("did not expect patch to include Go field name %q, got %#v", "Name", patch)
	}
	if _, ok := fields["fullName"]; ok {
		t.Fatalf("did not expect patch to include json field name %q, got %#v", "fullName", patch)
	}

	if err := db.Patch(1, patch, PatchStrict); err != nil {
		t.Fatalf("Patch(MakePatch(...)): %v", err)
	}

	got, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil {
		t.Fatalf("Get(1): got nil")
	}
	defer releaseUniqueRecords(db, got)

	if got.Name != "bob" || got.FullName != "Bob B." {
		t.Fatalf("unexpected record after patch: %#v", got)
	}
}

func TestMakePatch_PatchJSON_UsesJSONOrGoNamesAndRoundTripsViaPatch(t *testing.T) {
	db, _ := openTempDBUint64(t)

	oldVal := &Rec{
		Name:     "alice",
		FullName: "Alice A.",
	}
	newVal := &Rec{
		Name:     "bob",
		FullName: "Bob B.",
	}

	mustSetAPIRec(t, db, 1, oldVal)

	patch := db.MakePatch(oldVal, newVal, PatchJSON)
	fields := patchFieldsByName(patch)

	if _, ok := fields["Name"]; !ok {
		t.Fatalf("expected patch to include Go field name fallback %q, got %#v", "Name", patch)
	}
	if _, ok := fields["fullName"]; !ok {
		t.Fatalf("expected patch to include json field name %q, got %#v", "fullName", patch)
	}
	if _, ok := fields["name"]; ok {
		t.Fatalf("did not expect patch to include db field name %q, got %#v", "name", patch)
	}
	if _, ok := fields["full_name"]; ok {
		t.Fatalf("did not expect patch to include db field name %q, got %#v", "full_name", patch)
	}

	if err := db.Patch(1, patch, PatchStrict); err != nil {
		t.Fatalf("Patch(MakePatch(..., PatchJSON)): %v", err)
	}

	got, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil {
		t.Fatalf("Get(1): got nil")
	}
	defer releaseUniqueRecords(db, got)

	if got.Name != "bob" || got.FullName != "Bob B." {
		t.Fatalf("unexpected record after patch: %#v", got)
	}
}

func TestCollectBatchPatch_BeforeCommit_CollectsAndDeepCopies_SliceValues(t *testing.T) {
	db, _ := openTempDBUint64(t)

	ids := []uint64{1, 2}
	base := []*Rec{
		{Name: "alice", Age: 10, Tags: []string{"a"}},
		{Name: "carol", Age: 20, Tags: []string{"c"}},
	}
	if err := db.BatchSet(ids, base); err != nil {
		t.Fatalf("BatchSet(base): %v", err)
	}

	patchByID := make(map[uint64][]Field)
	// makePatchMany := db.CollectPatchMany(patchByID)

	origTags1 := []string{"x", "y"} // will mutate later
	origTags2 := []string{"p", "q"} // will mutate later

	updated := []*Rec{
		{Name: "bob", Age: 10, Tags: origTags1},   // name+tags changed, age unchanged
		{Name: "carol", Age: 21, Tags: origTags2}, // age+tags changed, name unchanged
	}

	if err := db.BatchSet(ids, updated, BeforeCommit(func(_ *bbolt.Tx, key uint64, oldValue, newValue *Rec) error {
		patchByID[key] = db.MakePatch(oldValue, newValue)
		return nil
	})); err != nil {
		t.Fatalf("BatchSet(update): %v", err)
	}

	// to map []Field -> map[name]value for assertions.
	toMap := func(fs []Field) map[string]any {
		m := make(map[string]any, len(fs))
		for _, f := range fs {
			m[f.Name] = f.Value
		}
		return m
	}

	p1, ok := patchByID[1]
	if !ok {
		t.Fatalf("expected patch for id=1, got keys: %#v", keysOfMap(patchByID))
	}
	m1 := toMap(p1)

	if _, ok = m1["name"]; !ok {
		t.Fatalf("id=1: expected patch to include %q, got %#v", "name", p1)
	}
	if _, ok = m1["tags"]; !ok {
		t.Fatalf("id=1: expected patch to include %q, got %#v", "tags", p1)
	}
	if _, ok = m1["age"]; ok {
		t.Fatalf("id=1: did not expect patch to include unchanged field %q, got %#v", "age", p1)
	}

	if v, _ := m1["name"].(string); v != "bob" {
		t.Fatalf("id=1: expected patched name %q, got %#v", "bob", m1["name"])
	}
	tags1, _ := m1["tags"].([]string)
	if len(tags1) != 2 || tags1[0] != "x" || tags1[1] != "y" {
		t.Fatalf("id=1: expected patched tags [x y], got %#v", tags1)
	}

	p2, ok := patchByID[2]
	if !ok {
		t.Fatalf("expected patch for id=2, got keys: %#v", keysOfMap(patchByID))
	}
	m2 := toMap(p2)

	if _, ok = m2["age"]; !ok {
		t.Fatalf("id=2: expected patch to include %q, got %#v", "age", p2)
	}
	if _, ok = m2["tags"]; !ok {
		t.Fatalf("id=2: expected patch to include %q, got %#v", "tags", p2)
	}
	if _, ok = m2["name"]; ok {
		t.Fatalf("id=2: did not expect patch to include unchanged field %q, got %#v", "name", p2)
	}

	if v, _ := m2["age"].(int); v != 21 {
		t.Fatalf("id=2: expected patched age %d, got %#v", 21, m2["age"])
	}
	tags2, _ := m2["tags"].([]string)
	if len(tags2) != 2 || tags2[0] != "p" || tags2[1] != "q" {
		t.Fatalf("id=2: expected patched tags [p q], got %#v", tags2)
	}

	origTags1[0] = "MUTATED1"
	origTags2[0] = "MUTATED2"

	tags1b, _ := m1["tags"].([]string)
	if len(tags1b) != 2 || tags1b[0] != "x" || tags1b[1] != "y" {
		t.Fatalf("id=1: expected deep-copied tags [x y], got %#v", tags1b)
	}
	tags2b, _ := m2["tags"].([]string)
	if len(tags2b) != 2 || tags2b[0] != "p" || tags2b[1] != "q" {
		t.Fatalf("id=2: expected deep-copied tags [p q], got %#v", tags2b)
	}
}

/**/

func TestTruncate(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1, Tags: []string{"t1"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2, Tags: []string{"t2"}}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	if cnt, err := db.Count(); err != nil || cnt != 2 {
		t.Fatalf("Count(before): cnt=%d err=%v", cnt, err)
	}
	if ids, err := db.QueryKeys(qx.Query(qx.EQ("email", "a@x"))); err != nil || len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("QueryKeys(before): ids=%v err=%v", ids, err)
	}

	// concurrent readers while truncate happens should not panic
	done := make(chan struct{})
	errCh := make(chan error, 16)

	var wg sync.WaitGroup
	reader := func(name string) {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				errCh <- fmt.Errorf("panic in %s: %v", name, r)
			}
		}()
		for {
			select {
			case <-done:
				return
			default:
			}
			_, _ = db.Get(1)
			_, _ = db.BatchGet(1, 2, 999)

			_ = db.SeqScan(0, func(_ uint64, _ *UniqueTestRec) (bool, error) { return true, nil })
			_ = db.SeqScanRaw(0, func(_ uint64, _ []byte) (bool, error) { return true, nil })

			_, _ = db.QueryKeys(qx.Query())
			_, _ = db.Count()
		}
	}

	wg.Add(2)
	go reader("r1")
	go reader("r2")

	if err := db.Truncate(); err != nil {
		close(done)
		wg.Wait()
		t.Fatalf("Truncate #1: %v", err)
	}
	if err := db.Truncate(); err != nil {
		close(done)
		wg.Wait()
		t.Fatalf("Truncate #2: %v", err)
	}

	close(done)
	wg.Wait()
	close(errCh)

	for e := range errCh {
		t.Errorf("%v", e)
	}
	if t.Failed() {
		t.FailNow()
	}

	if v, err := db.Get(1); err != nil {
		t.Fatalf("Get(after): %v", err)
	} else if v != nil {
		t.Fatalf("expected Get(1)==nil after truncate, got %#v", v)
	}
	if vals, err := db.BatchGet(1, 2); err != nil {
		t.Fatalf("BatchGet(after): %v", err)
	} else if len(vals) != 2 || vals[0] != nil || vals[1] != nil {
		t.Fatalf("expected BatchGet to return [nil nil], got %#v", vals)
	}

	if cnt, err := db.Count(); err != nil {
		t.Fatalf("Count(after): %v", err)
	} else if cnt != 0 {
		t.Fatalf("expected 0 records after truncate, got %d", cnt)
	}
	if ids, err := db.QueryKeys(qx.Query()); err != nil {
		t.Fatalf("QueryKeys(NOOP after): %v", err)
	} else if len(ids) != 0 {
		t.Fatalf("expected no keys after truncate, got %v", ids)
	}

	st := db.Stats()
	if st.KeyCount != 0 {
		t.Fatalf("expected Stats.KeyCount=0 after truncate, got %d", st.KeyCount)
	}
	var zero uint64
	if st.LastKey != zero {
		t.Fatalf("expected Stats.LastKey=%v after truncate, got %v", zero, st.LastKey)
	}

	if err := db.Set(10, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("expected unique values reusable after truncate, got: %v", err)
	}
	if err := db.Set(11, &UniqueTestRec{Email: "a@x", Code: 2}); err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation after truncate+reuse, got: %v", err)
	}

	if ids, err := db.QueryKeys(qx.Query(qx.EQ("email", "a@x"))); err != nil {
		t.Fatalf("QueryKeys(after reinsert): %v", err)
	} else if len(ids) != 1 || ids[0] != 10 {
		t.Fatalf("expected [10] after reinsert, got %v", ids)
	}
}

func TestTruncate_NoDeadlock_WithConcurrentExecuteBatchWriter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "truncate_deadlock.db")
	db, raw := openBoltAndNew[uint64, Rec](t, path)

	if err := db.Set(1, &Rec{Name: "seed", Age: 1}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	newVal := &Rec{Name: "after", Age: 2}
	payload := mustEncodeAutoBatchPayload(t, db, newVal)

	req := &autoBatchRequest[uint64, Rec]{
		op:         autoBatchSet,
		id:         1,
		setValue:   newVal,
		setPayload: payload,
		done:       make(chan error, 1),
	}

	// Force lock-order contention window:
	// 1) truncate goroutine waits on db.mu
	// 2) executeAutoBatch goroutine acquires writer tx and then waits on db.mu
	// 3) release db.mu and assert both operations complete.
	db.mu.Lock()
	truncateDone := make(chan error, 1)
	go func() {
		truncateDone <- db.Truncate()
	}()
	go db.executeAutoBatch([]*autoBatchRequest[uint64, Rec]{req})
	time.Sleep(20 * time.Millisecond)
	db.mu.Unlock()

	select {
	case err := <-truncateDone:
		if err != nil {
			t.Fatalf("Truncate: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("truncate timed out (possible deadlock)")
	}

	select {
	case err := <-req.done:
		if err != nil {
			t.Fatalf("executeAutoBatch req error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("executeAutoBatch timed out (possible deadlock)")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("raw.Close: %v", err)
	}
}

/**/

func releaseUniqueRecords[K ~string | ~uint64, V any](db *DB[K, V], vals ...*V) {
	if db == nil || len(vals) == 0 {
		return
	}
	seen := make(map[*V]struct{}, len(vals))
	unique := make([]*V, 0, len(vals))
	for _, v := range vals {
		if v == nil {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		unique = append(unique, v)
	}
	db.ReleaseRecords(unique...)
}

func mustSetAPIRec(tb testing.TB, db *DB[uint64, Rec], id uint64, rec *Rec) {
	tb.Helper()
	if err := db.Set(id, rec); err != nil {
		tb.Fatalf("Set(%d): %v", id, err)
	}
}

func mustSetAPIRecs(tb testing.TB, db *DB[uint64, Rec], recs map[uint64]*Rec) {
	tb.Helper()
	for id, rec := range recs {
		mustSetAPIRec(tb, db, id, rec)
	}
}

func TestAPI_New_InvalidBucketFillPercent_NegativeRejected(t *testing.T) {
	raw, _ := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	db, err := New[uint64, Rec](raw, Options{
		BucketName:        "api_invalid_fill_negative",
		BucketFillPercent: -0.25,
	})
	if err == nil {
		_ = db.Close()
		t.Fatalf("expected negative BucketFillPercent to be rejected")
	}
}

func TestAPI_New_InvalidBucketFillPercent_AboveOneRejected(t *testing.T) {
	raw, _ := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	db, err := New[uint64, Rec](raw, Options{
		BucketName:        "api_invalid_fill_above_one",
		BucketFillPercent: 1.25,
	})
	if err == nil {
		_ = db.Close()
		t.Fatalf("expected BucketFillPercent > 1 to be rejected")
	}
}

func TestAPI_BucketName_ReturnsClone(t *testing.T) {
	raw, _ := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	db, err := New[uint64, Rec](raw, Options{BucketName: "api_bucket_clone"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = db.Close() }()

	orig := db.BucketName()
	mutated := db.BucketName()
	mutated[0] ^= 0xff

	again := db.BucketName()
	if !bytes.Equal(orig, again) {
		t.Fatalf("bucket name was mutated through returned slice: before=%q after=%q", orig, again)
	}
}

func TestAPI_Get_ReturnedRecordDetachedFromStore(t *testing.T) {
	db, _ := openTempDBUint64(t)

	opt := "keep"
	mustSetAPIRec(t, db, 1, &Rec{
		Name: "alice", Age: 30, Tags: []string{"go", "db"}, Opt: &opt,
	})

	got, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil {
		t.Fatalf("Get(1): got nil")
	}
	defer releaseUniqueRecords(db, got)

	got.Name = "mutated"
	got.Tags[0] = "changed"
	got.Opt = nil

	again, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1) again: %v", err)
	}
	if again == nil {
		t.Fatalf("Get(1) again: got nil")
	}
	defer releaseUniqueRecords(db, again)

	if again.Name != "alice" || again.Tags[0] != "go" || again.Opt == nil || *again.Opt != "keep" {
		t.Fatalf("stored record was mutated through Get result: %#v", again)
	}
}

func TestAPI_BatchGet_PreservesOrderAndNilHolesWithDuplicates(t *testing.T) {
	db, _ := openTempDBUint64(t)

	mustSetAPIRecs(t, db, map[uint64]*Rec{
		1: {Name: "one", Age: 10},
		3: {Name: "three", Age: 30},
	})

	got, err := db.BatchGet(3, 2, 1, 3, 99)
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	defer releaseUniqueRecords(db, got...)

	if len(got) != 5 {
		t.Fatalf("expected len=5, got %d", len(got))
	}
	if got[0] == nil || got[0].Name != "three" {
		t.Fatalf("slot 0 mismatch: %#v", got[0])
	}
	if got[1] != nil {
		t.Fatalf("slot 1 must be nil, got %#v", got[1])
	}
	if got[2] == nil || got[2].Name != "one" {
		t.Fatalf("slot 2 mismatch: %#v", got[2])
	}
	if got[3] == nil || got[3].Name != "three" {
		t.Fatalf("slot 3 mismatch: %#v", got[3])
	}
	if got[4] != nil {
		t.Fatalf("slot 4 must be nil, got %#v", got[4])
	}
}

func TestAPI_BatchGet_DuplicateIDsReturnIndependentCopies(t *testing.T) {
	db, _ := openTempDBUint64(t)

	mustSetAPIRec(t, db, 1, &Rec{Name: "alice", Age: 30, Tags: []string{"go", "db"}})

	got, err := db.BatchGet(1, 1)
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	defer releaseUniqueRecords(db, got...)

	if len(got) != 2 || got[0] == nil || got[1] == nil {
		t.Fatalf("unexpected BatchGet result: %#v", got)
	}
	if got[0] == got[1] {
		t.Fatalf("duplicate BatchGet entries alias the same record pointer")
	}

	got[0].Name = "mutated"
	got[0].Tags[0] = "changed"

	if got[1].Name != "alice" || got[1].Tags[0] != "go" {
		t.Fatalf("mutating one BatchGet result changed the duplicate copy: %#v", got[1])
	}

	again, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	defer releaseUniqueRecords(db, again)
	if again == nil || again.Name != "alice" || again.Tags[0] != "go" {
		t.Fatalf("stored value changed after BatchGet mutation: %#v", again)
	}
}

func TestAPI_Query_ReturnedRecordsDetachedFromStore(t *testing.T) {
	db, _ := openTempDBUint64(t)

	mustSetAPIRec(t, db, 1, &Rec{Name: "alice", Age: 30, Tags: []string{"go", "db"}})

	items, err := db.Query(qx.Query(qx.EQ("age", 30)))
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer releaseUniqueRecords(db, items...)
	if len(items) != 1 || items[0] == nil {
		t.Fatalf("unexpected query result: %#v", items)
	}

	items[0].Name = "mutated"
	items[0].Tags[0] = "changed"

	again, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	defer releaseUniqueRecords(db, again)
	if again == nil || again.Name != "alice" || again.Tags[0] != "go" {
		t.Fatalf("stored value changed after Query result mutation: %#v", again)
	}
}

func TestAPI_Query_ReturnOrderMatchesQueryKeys(t *testing.T) {
	db, _ := openTempDBUint64(t)

	mustSetAPIRecs(t, db, map[uint64]*Rec{
		1: {Name: "id-1", Age: 20},
		2: {Name: "id-2", Age: 40},
		3: {Name: "id-3", Age: 30},
	})

	q := qx.Query(qx.GTE("age", 20)).Sort("age", qx.DESC)

	keys, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	items, err := db.Query(q)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer releaseUniqueRecords(db, items...)

	if len(keys) != len(items) {
		t.Fatalf("Query/QueryKeys length mismatch: keys=%v items=%d", keys, len(items))
	}

	wantNames := map[uint64]string{
		1: "id-1",
		2: "id-2",
		3: "id-3",
	}
	for i, id := range keys {
		if items[i] == nil || items[i].Name != wantNames[id] {
			t.Fatalf("position %d mismatch: key=%d item=%#v", i, id, items[i])
		}
	}
}

func TestAPI_SeqScan_CallbackMutationDoesNotPersist(t *testing.T) {
	db, _ := openTempDBUint64(t)

	mustSetAPIRec(t, db, 1, &Rec{Name: "alice", Age: 30, Tags: []string{"go", "db"}})

	if err := db.SeqScan(0, func(id uint64, rec *Rec) (bool, error) {
		if id != 1 {
			return true, nil
		}
		rec.Name = "mutated"
		rec.Tags[0] = "changed"
		return false, nil
	}); err != nil {
		t.Fatalf("SeqScan: %v", err)
	}

	again, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	defer releaseUniqueRecords(db, again)
	if again == nil || again.Name != "alice" || again.Tags[0] != "go" {
		t.Fatalf("stored value changed after SeqScan callback mutation: %#v", again)
	}
}

func TestAPI_Count_IgnoresOrderOffsetLimit(t *testing.T) {
	db, _ := openTempDBUint64(t)

	mustSetAPIRecs(t, db, map[uint64]*Rec{
		1: {Name: "a", Age: 18},
		2: {Name: "b", Age: 25},
		3: {Name: "c", Age: 30},
		4: {Name: "d", Age: 35},
	})

	got, err := db.Count(qx.Query(qx.GTE("age", 25)).Sort("age", qx.ASC).Offset(2).Limit(1).Filter)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if got != 3 {
		t.Fatalf("expected full match count=3, got %d", got)
	}
}

func TestAPI_Count_IgnoresInvalidOrderField(t *testing.T) {
	db, _ := openTempDBUint64(t)

	mustSetAPIRecs(t, db, map[uint64]*Rec{
		1: {Name: "a", Age: 25},
		2: {Name: "b", Age: 30},
	})

	got, err := db.Count(qx.Query(qx.GTE("age", 25)).Sort("does_not_exist", qx.ASC).Offset(1).Limit(1).Filter)
	if err != nil {
		t.Fatalf("Count should ignore order fields entirely, got err=%v", err)
	}
	if got != 2 {
		t.Fatalf("expected count=2, got %d", got)
	}
}

func TestAPI_ScanKeys_StopOnFalse(t *testing.T) {
	db, _ := openTempDBUint64(t)
	for i := 1; i <= 5; i++ {
		mustSetAPIRec(t, db, uint64(i), &Rec{Name: "x", Age: i})
	}

	var got []uint64
	err := db.ScanKeys(2, func(id uint64) (bool, error) {
		got = append(got, id)
		return len(got) < 2, nil
	})
	if err != nil {
		t.Fatalf("ScanKeys: %v", err)
	}
	if !slices.Equal(got, []uint64{2, 3}) {
		t.Fatalf("unexpected keys: %v", got)
	}
}

func TestAPI_ScanKeys_PropagatesCallbackError(t *testing.T) {
	db, _ := openTempDBUint64(t)
	for i := 1; i <= 5; i++ {
		mustSetAPIRec(t, db, uint64(i), &Rec{Name: "x", Age: i})
	}

	sentinel := errors.New("scan stop")
	var calls int
	err := db.ScanKeys(1, func(id uint64) (bool, error) {
		calls++
		if id == 3 {
			return false, sentinel
		}
		return true, nil
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got: %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 callback calls, got %d", calls)
	}
}

func TestAPI_SeqScan_StopOnFalse(t *testing.T) {
	db, _ := openTempDBUint64(t)
	for i := 1; i <= 4; i++ {
		mustSetAPIRec(t, db, uint64(i), &Rec{Name: "x", Age: i})
	}

	var got []uint64
	err := db.SeqScan(0, func(id uint64, rec *Rec) (bool, error) {
		got = append(got, id)
		return len(got) < 2, nil
	})
	if err != nil {
		t.Fatalf("SeqScan: %v", err)
	}
	if !slices.Equal(got, []uint64{1, 2}) {
		t.Fatalf("unexpected ids: %v", got)
	}
}

func TestAPI_SeqScan_PropagatesCallbackError(t *testing.T) {
	db, _ := openTempDBUint64(t)
	for i := 1; i <= 4; i++ {
		mustSetAPIRec(t, db, uint64(i), &Rec{Name: "x", Age: i})
	}

	sentinel := errors.New("seqscan stop")
	var calls int
	err := db.SeqScan(0, func(id uint64, rec *Rec) (bool, error) {
		calls++
		if id == 2 {
			return false, sentinel
		}
		return true, nil
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 callback calls, got %d", calls)
	}
}

func TestAPI_SeqScanRaw_StopOnFalse(t *testing.T) {
	db, _ := openTempDBUint64(t)
	for i := 1; i <= 4; i++ {
		mustSetAPIRec(t, db, uint64(i), &Rec{Name: "x", Age: i})
	}

	var got []uint64
	err := db.SeqScanRaw(0, func(id uint64, raw []byte) (bool, error) {
		if len(raw) == 0 {
			t.Fatalf("expected non-empty raw value for id=%d", id)
		}
		got = append(got, id)
		return len(got) < 2, nil
	})
	if err != nil {
		t.Fatalf("SeqScanRaw: %v", err)
	}
	if !slices.Equal(got, []uint64{1, 2}) {
		t.Fatalf("unexpected ids: %v", got)
	}
}

func TestAPI_SeqScanRaw_PropagatesCallbackError(t *testing.T) {
	db, _ := openTempDBUint64(t)
	for i := 1; i <= 4; i++ {
		mustSetAPIRec(t, db, uint64(i), &Rec{Name: "x", Age: i})
	}

	sentinel := errors.New("raw stop")
	var calls int
	err := db.SeqScanRaw(0, func(id uint64, raw []byte) (bool, error) {
		calls++
		if id == 2 {
			return false, sentinel
		}
		return true, nil
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 callback calls, got %d", calls)
	}
}

func TestAPI_Stats_ZeroAfterClose(t *testing.T) {
	db, _ := openTempDBUint64(t)
	mustSetAPIRec(t, db, 1, &Rec{Name: "alice", Age: 30})

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var zero Stats[uint64]
	if got := db.Stats(); got != zero {
		t.Fatalf("expected zero Stats after Close, got %+v", got)
	}
}

func TestAPI_IndexStats_ZeroAfterClose(t *testing.T) {
	db, _ := openTempDBUint64(t)
	mustSetAPIRec(t, db, 1, &Rec{Name: "alice", Age: 30})

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := db.IndexStats(); !reflect.DeepEqual(got, IndexStats{}) {
		t.Fatalf("expected zero IndexStats after Close, got %+v", got)
	}
}

func TestAPI_SnapshotStats_ZeroAfterClose(t *testing.T) {
	db, _ := openTempDBUint64(t)
	mustSetAPIRec(t, db, 1, &Rec{Name: "alice", Age: 30})

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var zero SnapshotStats
	if got := db.SnapshotStats(); got != zero {
		t.Fatalf("expected zero SnapshotStats after Close, got %+v", got)
	}
}

func TestAPI_IndexStats_ReturnMapsAreCallerOwned(t *testing.T) {
	db, _ := openTempDBUint64(t)
	mustSetAPIRecs(t, db, map[uint64]*Rec{
		1: {Name: "alice", Age: 30, Tags: []string{"go"}},
		2: {Name: "bob", Age: 35, Tags: []string{"db"}},
	})

	s1 := db.IndexStats()
	if s1.FieldSize["age"] == 0 {
		t.Fatalf("expected age field stats to exist: %+v", s1)
	}

	s1.FieldSize["age"] = 0
	delete(s1.UniqueFieldKeys, "age")
	delete(s1.FieldTotalCardinality, "age")
	delete(s1.FieldApproxStructBytes, "age")
	delete(s1.FieldApproxHeapBytes, "age")

	s2 := db.IndexStats()
	if s2.FieldSize["age"] == 0 {
		t.Fatalf("caller mutation leaked into IndexStats.FieldSize")
	}
	if _, ok := s2.UniqueFieldKeys["age"]; !ok {
		t.Fatalf("caller mutation leaked into IndexStats.UniqueFieldKeys")
	}
	if _, ok := s2.FieldTotalCardinality["age"]; !ok {
		t.Fatalf("caller mutation leaked into IndexStats.FieldTotalCardinality")
	}
	if _, ok := s2.FieldApproxStructBytes["age"]; !ok {
		t.Fatalf("caller mutation leaked into IndexStats.FieldApproxStructBytes")
	}
	if _, ok := s2.FieldApproxHeapBytes["age"]; !ok {
		t.Fatalf("caller mutation leaked into IndexStats.FieldApproxHeapBytes")
	}
}

func TestAPI_CalibrationStats_ReturnMapsAreCallerOwned(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:    -1,
		CalibrationEnabled: true,
	})

	if err := db.SetCalibrationSnapshot(CalibrationSnapshot{
		UpdatedAt: time.Now(),
		Multipliers: map[string]float64{
			"plan_ordered": 1.25,
		},
		Samples: map[string]uint64{
			"plan_ordered": 7,
		},
	}); err != nil {
		t.Fatalf("SetCalibrationSnapshot: %v", err)
	}

	s1 := db.CalibrationStats()
	s1.Multipliers["plan_ordered"] = 99
	delete(s1.Samples, "plan_ordered")

	s2 := db.CalibrationStats()
	if s2.Multipliers["plan_ordered"] != 1.25 {
		t.Fatalf("caller mutation leaked into CalibrationStats.Multipliers: %+v", s2)
	}
	if s2.Samples["plan_ordered"] != 7 {
		t.Fatalf("caller mutation leaked into CalibrationStats.Samples: %+v", s2)
	}
}

func TestAPI_GetCalibrationSnapshot_ReturnMapsAreCallerOwned(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:    -1,
		CalibrationEnabled: true,
	})

	if err := db.SetCalibrationSnapshot(CalibrationSnapshot{
		UpdatedAt: time.Now(),
		Multipliers: map[string]float64{
			"plan_ordered": 1.5,
		},
		Samples: map[string]uint64{
			"plan_ordered": 11,
		},
	}); err != nil {
		t.Fatalf("SetCalibrationSnapshot: %v", err)
	}

	s1, ok := db.GetCalibrationSnapshot()
	if !ok {
		t.Fatalf("expected calibration snapshot")
	}
	s1.Multipliers["plan_ordered"] = 99
	delete(s1.Samples, "plan_ordered")

	s2, ok := db.GetCalibrationSnapshot()
	if !ok {
		t.Fatalf("expected calibration snapshot on second read")
	}
	if s2.Multipliers["plan_ordered"] != 1.5 {
		t.Fatalf("caller mutation leaked into GetCalibrationSnapshot.Multipliers: %+v", s2)
	}
	if s2.Samples["plan_ordered"] != 11 {
		t.Fatalf("caller mutation leaked into GetCalibrationSnapshot.Samples: %+v", s2)
	}
}

func TestAPI_QueryKeysQueryCountReturnErrClosedAfterClose(t *testing.T) {
	db, _ := openTempDBUint64(t)
	mustSetAPIRec(t, db, 1, &Rec{Name: "alice", Age: 30})

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	q := qx.Query(qx.EQ("age", 30))

	if _, err := db.QueryKeys(q); !errors.Is(err, ErrClosed) {
		t.Fatalf("QueryKeys after Close: expected ErrClosed, got %v", err)
	}
	if _, err := db.Query(q); !errors.Is(err, ErrClosed) {
		t.Fatalf("Query after Close: expected ErrClosed, got %v", err)
	}
	if _, err := db.Count(q.Filter); !errors.Is(err, ErrClosed) {
		t.Fatalf("Count after Close: expected ErrClosed, got %v", err)
	}
}

func TestAPI_SetCalibrationSnapshot_RejectsUnknownPlanInMultipliers(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:    -1,
		CalibrationEnabled: true,
	})

	err := db.SetCalibrationSnapshot(CalibrationSnapshot{
		UpdatedAt: time.Now(),
		Multipliers: map[string]float64{
			"unknown_plan": 1.5,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "unknown planner calibration plan") {
		t.Fatalf("expected unknown plan error, got: %v", err)
	}
}

func TestAPI_SetCalibrationSnapshot_RejectsUnknownPlanInSamples(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:    -1,
		CalibrationEnabled: true,
	})

	err := db.SetCalibrationSnapshot(CalibrationSnapshot{
		UpdatedAt: time.Now(),
		Samples: map[string]uint64{
			"unknown_plan": 7,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "unknown planner calibration plan") {
		t.Fatalf("expected unknown plan error, got: %v", err)
	}
}

func TestAPI_SaveAndLoadCalibration_EmptyPathRejected(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:    -1,
		CalibrationEnabled: true,
	})

	if err := db.SaveCalibration(""); err == nil || !strings.Contains(err.Error(), "path is empty") {
		t.Fatalf("expected SaveCalibration empty-path error, got: %v", err)
	}
	if err := db.LoadCalibration(""); err == nil || !strings.Contains(err.Error(), "path is empty") {
		t.Fatalf("expected LoadCalibration empty-path error, got: %v", err)
	}
}

func TestAPI_SetCalibrationSnapshot_ClampsInvalidMultipliers(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:    -1,
		CalibrationEnabled: true,
	})

	err := db.SetCalibrationSnapshot(CalibrationSnapshot{
		UpdatedAt: time.Now(),
		Multipliers: map[string]float64{
			"plan_ordered":            -1,
			"plan_or_merge_no_order":  99,
			"plan_limit_order_basic":  math.NaN(),
			"plan_limit_order_prefix": math.Inf(1),
		},
	})
	if err != nil {
		t.Fatalf("SetCalibrationSnapshot: %v", err)
	}

	got, ok := db.GetCalibrationSnapshot()
	if !ok {
		t.Fatalf("expected calibration snapshot")
	}

	if math.Abs(got.Multipliers["plan_ordered"]-calibrationMultiplierMin) > 0.0001 {
		t.Fatalf("plan_ordered not clamped to min: %v", got.Multipliers["plan_ordered"])
	}
	if math.Abs(got.Multipliers["plan_or_merge_no_order"]-calibrationMultiplierMax) > 0.0001 {
		t.Fatalf("plan_or_merge_no_order not clamped to max: %v", got.Multipliers["plan_or_merge_no_order"])
	}
	if math.Abs(got.Multipliers["plan_limit_order_basic"]-calibrationMultiplierMin) > 0.0001 {
		t.Fatalf("plan_limit_order_basic NaN was not clamped to min: %v", got.Multipliers["plan_limit_order_basic"])
	}
	if math.Abs(got.Multipliers["plan_limit_order_prefix"]-calibrationMultiplierMin) > 0.0001 {
		t.Fatalf("plan_limit_order_prefix +Inf was not clamped to min: %v", got.Multipliers["plan_limit_order_prefix"])
	}
}

func TestAPI_ConcurrentStatsAccessAndWrites(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:        -1,
		CalibrationEnabled:     true,
		CalibrationSampleEvery: 1,
	})

	for i := 1; i <= 16; i++ {
		mustSetAPIRec(t, db, uint64(i), &Rec{
			Name:   "seed",
			Age:    20 + i,
			Active: i%2 == 0,
			Tags:   []string{"seed"},
		})
	}

	errCh := make(chan error, 16)
	var wg sync.WaitGroup

	for w := 0; w < 2; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 80; i++ {
				id := uint64((w*37+i)%24 + 1)
				rec := &Rec{
					Name:   "writer",
					Age:    18 + ((w + i) % 50),
					Active: (w+i)%2 == 0,
					Tags:   []string{"w", "api"},
				}
				if err := db.Set(id, rec); err != nil {
					errCh <- err
					return
				}
				if err := db.Patch(id, []Field{{Name: "age", Value: 30 + ((w + i) % 20)}}); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}

	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 80; i++ {
				_ = db.Stats()
				_ = db.IndexStats()
				_ = db.SnapshotStats()
				_ = db.PlannerStats()
				_ = db.CalibrationStats()
				_ = db.AutoBatchStats()
				_ = db.BucketName()
				if _, ok := db.GetCalibrationSnapshot(); !ok {
					errCh <- errors.New("missing calibration snapshot")
					return
				}
				items, err := db.Query(qx.Query(qx.GTE("age", 18)).Limit(8))
				if err != nil {
					errCh <- err
					return
				}
				releaseUniqueRecords(db, items...)
				if _, err := db.QueryKeys(qx.Query(qx.EQ("active", true)).Limit(8)); err != nil {
					errCh <- err
					return
				}
				if _, err := db.Count(); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent API operation failed: %v", err)
	}
}

func TestAPI_ConcurrentCalibrationAccessAndQueries(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:        -1,
		CalibrationEnabled:     true,
		CalibrationSampleEvery: 1,
		TraceSampleEvery:       1,
	})

	for i := 1; i <= 24; i++ {
		mustSetAPIRec(t, db, uint64(i), &Rec{
			Name:   "seed",
			Age:    20 + i,
			Active: i%2 == 0,
			Tags:   []string{"seed", "api"},
		})
	}

	errCh := make(chan error, 8)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 120; i++ {
			err := db.SetCalibrationSnapshot(CalibrationSnapshot{
				UpdatedAt: time.Now(),
				Multipliers: map[string]float64{
					"plan_ordered":           1.0 + float64(i%5)/10.0,
					"plan_or_merge_no_order": 1.2 + float64(i%3)/10.0,
				},
				Samples: map[string]uint64{
					"plan_ordered":           uint64(i),
					"plan_or_merge_no_order": uint64(i / 2),
				},
			})
			if err != nil {
				errCh <- err
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 120; i++ {
			snap, ok := db.GetCalibrationSnapshot()
			if !ok {
				errCh <- errors.New("missing calibration snapshot")
				return
			}
			snap.Multipliers["plan_ordered"] = 999
			delete(snap.Samples, "plan_ordered")
			_ = db.CalibrationStats()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 120; i++ {
			q := qx.Query(qx.GTE("age", 20)).Sort("age", qx.ASC).Limit(10)
			items, err := db.Query(q)
			if err != nil {
				errCh <- err
				return
			}
			releaseUniqueRecords(db, items...)
			if _, err := db.QueryKeys(q); err != nil {
				errCh <- err
				return
			}
			if _, err := db.Count(qx.Query(qx.EQ("active", true)).Filter); err != nil {
				errCh <- err
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent calibration/query API failed: %v", err)
	}
}

func TestAPI_PlannerStats_ReturnMapsAreCallerOwned(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	mustSetAPIRecs(t, db, map[uint64]*Rec{
		1: {Name: "alice", Age: 30, Tags: []string{"go"}},
		2: {Name: "bob", Age: 35, Tags: []string{"db"}},
		3: {Name: "carol", Age: 35, Tags: []string{"ops"}},
	})

	if err := db.RefreshPlannerStats(); err != nil {
		t.Fatalf("RefreshPlannerStats: %v", err)
	}

	s1 := db.PlannerStats()
	age, ok := s1.Fields["age"]
	if !ok || age.DistinctKeys == 0 {
		t.Fatalf("expected age planner stats to exist: %+v", s1)
	}

	s1.Fields["age"] = PlannerFieldStats{}
	delete(s1.Fields, "name")

	s2 := db.PlannerStats()
	if got := s2.Fields["age"]; got.DistinctKeys == 0 {
		t.Fatalf("caller mutation leaked into PlannerStats.Fields[age]: %+v", got)
	}
	if _, ok := s2.Fields["name"]; !ok {
		t.Fatalf("caller mutation leaked into PlannerStats.Fields[name]")
	}
}

func TestAPI_MakePatchInto_ResetsDstAndDeepCopiesMutableValues(t *testing.T) {
	db, _ := openTempDBUint64(t)

	oldOpt := "keep"
	newOpt := "next"

	oldVal := &Rec{
		Name: "alice",
		Age:  30,
		Tags: []string{"go"},
		Opt:  &oldOpt,
	}
	newVal := &Rec{
		Name: "bob",
		Age:  31,
		Tags: []string{"db", "ops"},
		Opt:  &newOpt,
	}

	mustSetAPIRec(t, db, 1, oldVal)

	scratch := []Field{{Name: "stale", Value: "sentinel"}}
	patch := db.MakePatchInto(oldVal, newVal, scratch)
	if len(patch) == 0 {
		t.Fatalf("expected non-empty patch")
	}
	for _, f := range patch {
		if f.Name == "stale" {
			t.Fatalf("MakePatchInto leaked stale dst contents into result: %#v", patch)
		}
	}

	newVal.Tags[0] = "mutated"
	*newVal.Opt = "changed"

	if err := db.Patch(1, patch, PatchStrict); err != nil {
		t.Fatalf("Patch(MakePatchInto(...)): %v", err)
	}

	got, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil {
		t.Fatalf("Get(1): got nil")
	}
	defer releaseUniqueRecords(db, got)

	if got.Name != "bob" || got.Age != 31 {
		t.Fatalf("unexpected scalar fields after patch: %#v", got)
	}
	if !slices.Equal(got.Tags, []string{"db", "ops"}) {
		t.Fatalf("patch aliased mutated slice from newVal: %#v", got.Tags)
	}
	if got.Opt == nil || *got.Opt != "next" {
		t.Fatalf("patch aliased mutated pointer value from newVal: %#v", got.Opt)
	}
}

func TestAPI_SetCalibrationSnapshot_AfterCloseRejected(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:    -1,
		CalibrationEnabled: true,
	})

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	err := db.SetCalibrationSnapshot(CalibrationSnapshot{
		UpdatedAt: time.Now(),
		Multipliers: map[string]float64{
			"plan_ordered": 1.5,
		},
	})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("SetCalibrationSnapshot after Close: expected ErrClosed, got %v", err)
	}
}

func TestAPI_SetCalibrationSnapshot_AfterCloseDoesNotMutateState(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:    -1,
		CalibrationEnabled: true,
	})

	if err := db.SetCalibrationSnapshot(CalibrationSnapshot{
		UpdatedAt: time.Now(),
		Multipliers: map[string]float64{
			"plan_ordered": 1.25,
		},
		Samples: map[string]uint64{
			"plan_ordered": 7,
		},
	}); err != nil {
		t.Fatalf("SetCalibrationSnapshot(initial): %v", err)
	}

	before, ok := db.GetCalibrationSnapshot()
	if !ok {
		t.Fatalf("expected initial calibration snapshot")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_ = db.SetCalibrationSnapshot(CalibrationSnapshot{
		UpdatedAt: time.Now(),
		Multipliers: map[string]float64{
			"plan_ordered": 2.5,
		},
		Samples: map[string]uint64{
			"plan_ordered": 99,
		},
	})

	after, ok := db.GetCalibrationSnapshot()
	if !ok {
		t.Fatalf("expected calibration snapshot after Close")
	}
	if after.Multipliers["plan_ordered"] != before.Multipliers["plan_ordered"] {
		t.Fatalf("SetCalibrationSnapshot after Close mutated multiplier: before=%v after=%v", before.Multipliers["plan_ordered"], after.Multipliers["plan_ordered"])
	}
	if after.Samples["plan_ordered"] != before.Samples["plan_ordered"] {
		t.Fatalf("SetCalibrationSnapshot after Close mutated samples: before=%v after=%v", before.Samples["plan_ordered"], after.Samples["plan_ordered"])
	}
}

func TestAPI_SaveCalibration_AfterCloseAllowed(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:    -1,
		CalibrationEnabled: true,
	})

	if err := db.SetCalibrationSnapshot(CalibrationSnapshot{
		UpdatedAt: time.Now(),
		Multipliers: map[string]float64{
			"plan_ordered": 1.25,
		},
		Samples: map[string]uint64{
			"plan_ordered": 7,
		},
	}); err != nil {
		t.Fatalf("SetCalibrationSnapshot: %v", err)
	}

	savePath := t.TempDir() + "/closed-save-side-effect.json"

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := db.SaveCalibration(savePath); err != nil {
		t.Fatalf("SaveCalibration after Close: %v", err)
	}

	raw, err := os.ReadFile(savePath)
	if err != nil {
		t.Fatalf("ReadFile(savePath): %v", err)
	}
	if len(raw) == 0 {
		t.Fatalf("SaveCalibration after Close wrote empty file")
	}
}

func TestAPI_LoadCalibration_AfterCloseRejected(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:    -1,
		CalibrationEnabled: true,
	})

	if err := db.SetCalibrationSnapshot(CalibrationSnapshot{
		UpdatedAt: time.Now(),
		Multipliers: map[string]float64{
			"plan_ordered": 1.25,
		},
		Samples: map[string]uint64{
			"plan_ordered": 7,
		},
	}); err != nil {
		t.Fatalf("SetCalibrationSnapshot: %v", err)
	}

	loadPath := t.TempDir() + "/closed-load.json"
	if err := os.WriteFile(loadPath, []byte("{\"multipliers\":{\"plan_ordered\":1.5},\"samples\":{\"plan_ordered\":9}}\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(loadPath): %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := db.LoadCalibration(loadPath); !errors.Is(err, ErrClosed) {
		t.Fatalf("LoadCalibration after Close: expected ErrClosed, got %v", err)
	}
}

func TestAPI_LoadCalibration_AfterCloseDoesNotMutateState(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:    -1,
		CalibrationEnabled: true,
	})

	if err := db.SetCalibrationSnapshot(CalibrationSnapshot{
		UpdatedAt: time.Now(),
		Multipliers: map[string]float64{
			"plan_ordered": 1.25,
		},
		Samples: map[string]uint64{
			"plan_ordered": 7,
		},
	}); err != nil {
		t.Fatalf("SetCalibrationSnapshot(initial): %v", err)
	}

	before, ok := db.GetCalibrationSnapshot()
	if !ok {
		t.Fatalf("expected initial calibration snapshot")
	}

	loadPath := t.TempDir() + "/closed-load-mutate.json"
	if err := os.WriteFile(loadPath, []byte("{\"multipliers\":{\"plan_ordered\":2.5},\"samples\":{\"plan_ordered\":99}}\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(loadPath): %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_ = db.LoadCalibration(loadPath)

	after, ok := db.GetCalibrationSnapshot()
	if !ok {
		t.Fatalf("expected calibration snapshot after Close")
	}
	if after.Multipliers["plan_ordered"] != before.Multipliers["plan_ordered"] {
		t.Fatalf("LoadCalibration after Close mutated multiplier: before=%v after=%v", before.Multipliers["plan_ordered"], after.Multipliers["plan_ordered"])
	}
	if after.Samples["plan_ordered"] != before.Samples["plan_ordered"] {
		t.Fatalf("LoadCalibration after Close mutated samples: before=%v after=%v", before.Samples["plan_ordered"], after.Samples["plan_ordered"])
	}
}

func TestAPI_CalibrationStats_DisabledExposesManualSnapshot(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	if err := db.SetCalibrationSnapshot(CalibrationSnapshot{
		UpdatedAt: time.Now(),
		Multipliers: map[string]float64{
			"plan_ordered": 1.5,
		},
		Samples: map[string]uint64{
			"plan_ordered": 11,
		},
	}); err != nil {
		t.Fatalf("SetCalibrationSnapshot: %v", err)
	}

	got := db.CalibrationStats()
	if got.Enabled {
		t.Fatalf("expected calibration to remain disabled")
	}
	if got.UpdatedAt.IsZero() {
		t.Fatalf("expected UpdatedAt from manual calibration snapshot")
	}
	if got.SamplesTotal != 11 {
		t.Fatalf("expected SamplesTotal=11, got %d", got.SamplesTotal)
	}
	if got.Multipliers["plan_ordered"] != 1.5 {
		t.Fatalf("expected plan_ordered multiplier=1.5, got %v", got.Multipliers["plan_ordered"])
	}
	if got.Samples["plan_ordered"] != 11 {
		t.Fatalf("expected plan_ordered samples=11, got %d", got.Samples["plan_ordered"])
	}
}

/**/

func ioExtCopyRec(v *Rec) Rec {
	if v == nil {
		return Rec{}
	}
	cp := *v
	cp.Tags = slices.Clone(v.Tags)
	if v.Opt != nil {
		s := *v.Opt
		cp.Opt = &s
	}
	return cp
}

func ioExtCopyProduct(v *Product) Product {
	if v == nil {
		return Product{}
	}
	cp := *v
	cp.Tags = slices.Clone(v.Tags)
	return cp
}

func ioExtMustSetRec(t *testing.T, db *DB[uint64, Rec], id uint64, v *Rec) {
	t.Helper()
	if err := db.Set(id, v); err != nil {
		t.Fatalf("Set(%d): %v", id, err)
	}
}

func ioExtMustSetProduct(t *testing.T, db *DB[string, Product], id string, v *Product) {
	t.Helper()
	if err := db.Set(id, v); err != nil {
		t.Fatalf("Set(%q): %v", id, err)
	}
}

func ioExtMustGetRec(t *testing.T, db *DB[uint64, Rec], id uint64) *Rec {
	t.Helper()
	v, err := db.Get(id)
	if err != nil {
		t.Fatalf("Get(%d): %v", id, err)
	}
	if v == nil {
		t.Fatalf("Get(%d): nil", id)
	}
	return v
}

func ioExtMustReadUint64Raw(t *testing.T, db *DB[uint64, Rec], id uint64) []byte {
	t.Helper()
	var raw []byte
	if err := db.bolt.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.bucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		v := b.Get(db.keyFromID(id))
		if v == nil {
			return fmt.Errorf("missing raw value for id=%d", id)
		}
		raw = append([]byte(nil), v...)
		return nil
	}); err != nil {
		t.Fatalf("read raw(%d): %v", id, err)
	}
	return raw
}

func ioExtMustReadStringRaw(t *testing.T, db *DB[string, Product], id string) []byte {
	t.Helper()
	var raw []byte
	if err := db.bolt.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.bucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		v := b.Get(db.keyFromID(id))
		if v == nil {
			return fmt.Errorf("missing raw value for id=%q", id)
		}
		raw = append([]byte(nil), v...)
		return nil
	}); err != nil {
		t.Fatalf("read raw(%q): %v", id, err)
	}
	return raw
}

func ioExtMustCorruptUint64Raw(t *testing.T, db *DB[uint64, Rec], id uint64, raw []byte) {
	t.Helper()
	if err := db.bolt.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.bucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		return b.Put(db.keyFromID(id), raw)
	}); err != nil {
		t.Fatalf("corrupt raw(%d): %v", id, err)
	}
}

func ioExtReadBucketValue(t testing.TB, raw *bbolt.DB, bucketName, key string) ([]byte, bool) {
	t.Helper()
	var out []byte
	var ok bool
	if err := raw.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return nil
		}
		v := b.Get([]byte(key))
		if v == nil {
			return nil
		}
		out = append([]byte(nil), v...)
		ok = true
		return nil
	}); err != nil {
		t.Fatalf("read bucket %q key %q: %v", bucketName, key, err)
	}
	return out, ok
}

func ioExtMustQueryName(t *testing.T, db *DB[uint64, Rec], name string) []uint64 {
	t.Helper()
	ids, err := db.QueryKeys(qx.Query(qx.EQ("name", name)))
	if err != nil {
		t.Fatalf("QueryKeys(name=%q): %v", name, err)
	}
	return ids
}

func ioExtMustCountUint64(t *testing.T, db *DB[uint64, Rec]) uint64 {
	t.Helper()
	cnt, err := db.Count()
	if err != nil {
		t.Fatalf("Count(): %v", err)
	}
	return cnt
}

func ioExtCollectSeqScanRec(t *testing.T, db *DB[uint64, Rec], seek uint64) map[uint64]Rec {
	t.Helper()
	out := make(map[uint64]Rec)
	if err := db.SeqScan(seek, func(id uint64, v *Rec) (bool, error) {
		out[id] = ioExtCopyRec(v)
		return true, nil
	}); err != nil {
		t.Fatalf("SeqScan(%d): %v", seek, err)
	}
	return out
}

func ioExtCollectSeqScanProduct(t *testing.T, db *DB[string, Product], seek string) map[string]Product {
	t.Helper()
	out := make(map[string]Product)
	if err := db.SeqScan(seek, func(id string, v *Product) (bool, error) {
		out[id] = ioExtCopyProduct(v)
		return true, nil
	}); err != nil {
		t.Fatalf("SeqScan(%q): %v", seek, err)
	}
	return out
}

func TestIOExt_Get_ReturnsDetachedCopy(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	ioExtMustSetRec(t, db, 1, &Rec{Name: "alice", Age: 30, Tags: []string{"go"}, Meta: Meta{Country: "NL"}})

	got := ioExtMustGetRec(t, db, 1)
	got.Name = "mutated"
	got.Tags[0] = "oops"
	got.Country = "US"

	again := ioExtMustGetRec(t, db, 1)
	if again.Name != "alice" || again.Tags[0] != "go" || again.Country != "NL" {
		t.Fatalf("stored value changed after mutating Get result: %#v", again)
	}
}

func TestIOExt_BatchGet_ReturnsDetachedCopies(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	ioExtMustSetRec(t, db, 1, &Rec{Name: "alice", Age: 30, Tags: []string{"go"}})
	ioExtMustSetRec(t, db, 2, &Rec{Name: "bob", Age: 40, Tags: []string{"db"}})

	vals, err := db.BatchGet(1, 2)
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	if len(vals) != 2 || vals[0] == nil || vals[1] == nil {
		t.Fatalf("unexpected BatchGet result: %#v", vals)
	}

	vals[0].Name = "mutated-1"
	vals[1].Tags[0] = "mutated-2"

	v1 := ioExtMustGetRec(t, db, 1)
	if v1.Name != "alice" || v1.Tags[0] != "go" {
		t.Fatalf("id=1 changed after mutating BatchGet result: %#v", v1)
	}
	v2 := ioExtMustGetRec(t, db, 2)
	if v2.Name != "bob" || v2.Tags[0] != "db" {
		t.Fatalf("id=2 changed after mutating BatchGet result: %#v", v2)
	}
}

func TestIOExt_BatchGet_DuplicateIDsProduceIndependentCopies(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	ioExtMustSetRec(t, db, 1, &Rec{Name: "alice", Age: 30, Tags: []string{"go"}})

	vals, err := db.BatchGet(1, 1)
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	if len(vals) != 2 || vals[0] == nil || vals[1] == nil {
		t.Fatalf("unexpected BatchGet result: %#v", vals)
	}
	if vals[0] == vals[1] {
		t.Fatal("duplicate BatchGet ids returned the same pointer")
	}

	vals[0].Name = "mutated"
	vals[0].Tags[0] = "oops"
	if vals[1].Name != "alice" || vals[1].Tags[0] != "go" {
		t.Fatalf("duplicate BatchGet result aliased unexpectedly: %#v", vals[1])
	}

	again := ioExtMustGetRec(t, db, 1)
	if again.Name != "alice" || again.Tags[0] != "go" {
		t.Fatalf("stored value changed after mutating duplicate BatchGet result: %#v", again)
	}
}

func TestIOExt_BatchGetTxCompact_PreservesExistingOrderAndDuplicates(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	ioExtMustSetRec(t, db, 1, &Rec{Name: "one"})
	ioExtMustSetRec(t, db, 2, &Rec{Name: "two"})
	ioExtMustSetRec(t, db, 3, &Rec{Name: "three"})

	var vals []*Rec
	if err := db.bolt.View(func(tx *bbolt.Tx) error {
		var err error
		vals, err = db.batchGetTxCompact(tx, []uint64{3, 9, 1, 3, 2})
		return err
	}); err != nil {
		t.Fatalf("batchGetTxCompact: %v", err)
	}
	if len(vals) != 4 {
		t.Fatalf("expected 4 existing values, got %d", len(vals))
	}
	got := []string{vals[0].Name, vals[1].Name, vals[2].Name, vals[3].Name}
	want := []string{"three", "one", "three", "two"}
	if !slices.Equal(got, want) {
		t.Fatalf("unexpected compact order: got=%v want=%v", got, want)
	}
	if vals[0] == vals[2] {
		t.Fatal("duplicate id in compact BatchGet reused the same pointer")
	}
}

func TestIOExt_SeqScan_ReturnsDetachedCopies(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	ioExtMustSetRec(t, db, 1, &Rec{Name: "alice", Tags: []string{"go"}, Meta: Meta{Country: "NL"}})
	ioExtMustSetRec(t, db, 2, &Rec{Name: "bob", Tags: []string{"db"}, Meta: Meta{Country: "DE"}})

	if err := db.SeqScan(0, func(id uint64, v *Rec) (bool, error) {
		if id == 1 {
			v.Name = "mutated"
			v.Tags[0] = "oops"
			v.Country = "US"
		}
		return true, nil
	}); err != nil {
		t.Fatalf("SeqScan: %v", err)
	}

	again := ioExtMustGetRec(t, db, 1)
	if again.Name != "alice" || again.Tags[0] != "go" || again.Country != "NL" {
		t.Fatalf("stored value changed after mutating SeqScan value: %#v", again)
	}
}

func TestIOExt_SeqScan_StopOnFalse(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	for i := 1; i <= 3; i++ {
		ioExtMustSetRec(t, db, uint64(i), &Rec{Name: fmt.Sprintf("n%d", i)})
	}

	var seen []uint64
	if err := db.SeqScan(0, func(id uint64, _ *Rec) (bool, error) {
		seen = append(seen, id)
		return id != 2, nil
	}); err != nil {
		t.Fatalf("SeqScan: %v", err)
	}
	if !slices.Equal(seen, []uint64{1, 2}) {
		t.Fatalf("unexpected SeqScan stop set: %v", seen)
	}
}

func TestIOExt_SeqScan_PropagatesCallbackError(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	for i := 1; i <= 3; i++ {
		ioExtMustSetRec(t, db, uint64(i), &Rec{Name: fmt.Sprintf("n%d", i)})
	}

	wantErr := errors.New("stop scan")
	var seen []uint64
	err := db.SeqScan(0, func(id uint64, _ *Rec) (bool, error) {
		seen = append(seen, id)
		if id == 2 {
			return false, wantErr
		}
		return true, nil
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}
	if !slices.Equal(seen, []uint64{1, 2}) {
		t.Fatalf("unexpected SeqScan callback order before error: %v", seen)
	}
}

func TestIOExt_SeqScanRaw_StopOnFalse(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	for i := 1; i <= 3; i++ {
		ioExtMustSetRec(t, db, uint64(i), &Rec{Name: fmt.Sprintf("n%d", i)})
	}

	var seen []uint64
	if err := db.SeqScanRaw(0, func(id uint64, _ []byte) (bool, error) {
		seen = append(seen, id)
		return id != 2, nil
	}); err != nil {
		t.Fatalf("SeqScanRaw: %v", err)
	}
	if !slices.Equal(seen, []uint64{1, 2}) {
		t.Fatalf("unexpected SeqScanRaw stop set: %v", seen)
	}
}

func TestIOExt_SeqScanRaw_MatchesPersistedPayloads(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	for i := 1; i <= 3; i++ {
		ioExtMustSetRec(t, db, uint64(i), &Rec{Name: fmt.Sprintf("n%d", i), Age: i})
	}

	got := make(map[uint64][]byte)
	if err := db.SeqScanRaw(0, func(id uint64, raw []byte) (bool, error) {
		got[id] = append([]byte(nil), raw...)
		return true, nil
	}); err != nil {
		t.Fatalf("SeqScanRaw: %v", err)
	}

	for i := 1; i <= 3; i++ {
		id := uint64(i)
		want := ioExtMustReadUint64Raw(t, db, id)
		if !slices.Equal(got[id], want) {
			t.Fatalf("raw payload mismatch for id=%d", id)
		}
	}
}

func TestIOExt_ScanKeys_StopOnFalse(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	for i := 1; i <= 5; i++ {
		ioExtMustSetRec(t, db, uint64(i), &Rec{Name: fmt.Sprintf("n%d", i)})
	}

	var seen []uint64
	if err := db.ScanKeys(2, func(id uint64) (bool, error) {
		seen = append(seen, id)
		return id != 3, nil
	}); err != nil {
		t.Fatalf("ScanKeys: %v", err)
	}
	if !slices.Equal(seen, []uint64{2, 3}) {
		t.Fatalf("unexpected ScanKeys stop set: %v", seen)
	}
}

func TestIOExt_ScanKeys_PropagatesCallbackError(t *testing.T) {
	db, _ := openTempDBString(t, Options{AutoBatchMax: 1})
	for _, id := range []string{"b-key", "a-key", "c-key"} {
		if err := db.Set(id, &Rec{Name: id}); err != nil {
			t.Fatalf("Set(%q): %v", id, err)
		}
	}

	wantErr := errors.New("stop keys")
	var seen []string
	err := db.ScanKeys("b", func(id string) (bool, error) {
		seen = append(seen, id)
		return false, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}
	if len(seen) != 1 {
		t.Fatalf("expected one callback before error, got %v", seen)
	}
	if seen[0] < "b" {
		t.Fatalf("unexpected key below seek: %q", seen[0])
	}
}

func TestIOExt_Get_CorruptPayloadReturnsDecodeError(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	ioExtMustSetRec(t, db, 1, &Rec{Name: "good-1"})
	ioExtMustSetRec(t, db, 2, &Rec{Name: "good-2"})
	ioExtMustCorruptUint64Raw(t, db, 1, []byte{0xff, 0x00, 0x7f})

	if _, err := db.Get(1); err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("expected Get decode error, got %v", err)
	}
	v2 := ioExtMustGetRec(t, db, 2)
	if v2.Name != "good-2" {
		t.Fatalf("unexpected unaffected record: %#v", v2)
	}
}

func TestIOExt_BatchGet_CorruptPayloadReturnsDecodeErrorWithoutPoisoningOtherReads(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	ioExtMustSetRec(t, db, 1, &Rec{Name: "one"})
	ioExtMustSetRec(t, db, 2, &Rec{Name: "two"})
	ioExtMustSetRec(t, db, 3, &Rec{Name: "three"})
	ioExtMustCorruptUint64Raw(t, db, 2, []byte{0xff, 0x00, 0x7f})

	vals, err := db.BatchGet(1, 2, 3)
	if err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("expected BatchGet decode error, got %v", err)
	}
	if len(vals) != 3 || vals[0] == nil || vals[0].Name != "one" || vals[1] != nil || vals[2] != nil {
		t.Fatalf("unexpected partial BatchGet result: %#v", vals)
	}

	v1 := ioExtMustGetRec(t, db, 1)
	if v1.Name != "one" {
		t.Fatalf("unexpected id=1 after failed BatchGet: %#v", v1)
	}
	v3 := ioExtMustGetRec(t, db, 3)
	if v3.Name != "three" {
		t.Fatalf("unexpected id=3 after failed BatchGet: %#v", v3)
	}
}

func TestIOExt_SeqScan_CorruptPayloadStopsAtBadRecord(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	ioExtMustSetRec(t, db, 1, &Rec{Name: "one"})
	ioExtMustSetRec(t, db, 2, &Rec{Name: "two"})
	ioExtMustSetRec(t, db, 3, &Rec{Name: "three"})
	ioExtMustCorruptUint64Raw(t, db, 2, []byte{0xff, 0x00, 0x7f})

	var seen []uint64
	err := db.SeqScan(0, func(id uint64, _ *Rec) (bool, error) {
		seen = append(seen, id)
		return true, nil
	})
	if err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("expected SeqScan decode error, got %v", err)
	}
	if !slices.Equal(seen, []uint64{1}) {
		t.Fatalf("unexpected rows seen before decode error: %v", seen)
	}

	v3 := ioExtMustGetRec(t, db, 3)
	if v3.Name != "three" {
		t.Fatalf("unexpected id=3 after failed SeqScan: %#v", v3)
	}
}

func TestIOExt_SeqScanRaw_CorruptPayloadStillReturnsRawBytes(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	ioExtMustSetRec(t, db, 1, &Rec{Name: "one"})
	ioExtMustSetRec(t, db, 2, &Rec{Name: "two"})
	ioExtMustSetRec(t, db, 3, &Rec{Name: "three"})
	corrupt := []byte{0xff, 0x00, 0x7f, 0x42}
	ioExtMustCorruptUint64Raw(t, db, 2, corrupt)

	got := make(map[uint64][]byte)
	if err := db.SeqScanRaw(0, func(id uint64, raw []byte) (bool, error) {
		got[id] = append([]byte(nil), raw...)
		return true, nil
	}); err != nil {
		t.Fatalf("SeqScanRaw: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 rows from SeqScanRaw, got %d", len(got))
	}
	if !slices.Equal(got[2], corrupt) {
		t.Fatalf("SeqScanRaw did not return corrupt raw payload: %v", got[2])
	}
}

func TestIOExt_Set_BeforeStoreCallbackError_DoesNotPersist(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	wantErr := errors.New("before store failed")

	err := db.Set(1, &Rec{Name: "alice"}, BeforeStore(func(_ uint64, _ *Rec, newValue *Rec) error {
		newValue.Name = "mutated"
		return wantErr
	}))
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}

	if v, err := db.Get(1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if v != nil {
		t.Fatalf("value persisted after BeforeStore error: %#v", v)
	}
	if cnt := ioExtMustCountUint64(t, db); cnt != 0 {
		t.Fatalf("expected Count=0, got %d", cnt)
	}
}

func TestIOExt_Set_BeforeCommitError_DoesNotMutateCallerWithBeforeStore(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	wantErr := errors.New("before commit failed")

	input := &Rec{Name: "alice", Tags: []string{"go"}, Meta: Meta{Country: "NL"}}
	err := db.Set(
		1,
		input,
		BeforeStore(func(_ uint64, _ *Rec, newValue *Rec) error {
			newValue.Name = "changed"
			newValue.Tags = append(newValue.Tags, "db")
			newValue.Country = "US"
			return nil
		}),
		BeforeCommit(func(_ *bbolt.Tx, _ uint64, _ *Rec, _ *Rec) error {
			return wantErr
		}),
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}

	if input.Name != "alice" || input.Country != "NL" || !slices.Equal(input.Tags, []string{"go"}) {
		t.Fatalf("caller-owned value mutated despite rollback: %#v", input)
	}
	if v, err := db.Get(1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if v != nil {
		t.Fatalf("value persisted after BeforeCommit error: %#v", v)
	}
}

func TestIOExt_Set_BeforeCommit_CanAtomicallyWriteNeighborBucket(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	const auditBucket = "io_ext_audit_set"
	const auditKey = "set/1"

	err := db.Set(1, &Rec{Name: "alice", Age: 10}, BeforeCommit(func(tx *bbolt.Tx, key uint64, oldValue, newValue *Rec) error {
		if oldValue != nil || newValue == nil {
			return fmt.Errorf("unexpected callback values: old=%#v new=%#v", oldValue, newValue)
		}

		managed := tx.Bucket(db.bucket)
		if managed == nil {
			return fmt.Errorf("managed bucket missing")
		}
		raw := managed.Get(db.keyFromID(key))
		if raw == nil {
			return fmt.Errorf("managed key missing inside BeforeCommit")
		}
		current, err := db.decode(raw)
		if err != nil {
			return fmt.Errorf("decode current tx value: %w", err)
		}
		if current.Name != newValue.Name || current.Age != newValue.Age {
			return fmt.Errorf("tx value mismatch: got=%#v want=%#v", current, newValue)
		}

		audit, err := tx.CreateBucketIfNotExists([]byte(auditBucket))
		if err != nil {
			return fmt.Errorf("create audit bucket: %w", err)
		}
		return audit.Put([]byte(auditKey), []byte(fmt.Sprintf("%s/%d", current.Name, current.Age)))
	}))
	if err != nil {
		t.Fatalf("Set with audit BeforeCommit: %v", err)
	}

	got := ioExtMustGetRec(t, db, 1)
	if got.Name != "alice" || got.Age != 10 {
		t.Fatalf("unexpected stored value: %#v", got)
	}

	auditRaw, ok := ioExtReadBucketValue(t, db.Bolt(), auditBucket, auditKey)
	if !ok {
		t.Fatalf("expected audit entry %q/%q", auditBucket, auditKey)
	}
	if string(auditRaw) != "alice/10" {
		t.Fatalf("unexpected audit payload: %q", auditRaw)
	}
}

func TestIOExt_Set_CommitFailure_RollsBackNeighborBucketAndKeepsSequence(t *testing.T) {
	db, _ := openTempDBStringProduct(t, Options{AutoBatchMax: 1})
	ioExtMustSetProduct(t, db, "seed", &Product{SKU: "seed", Price: 1})

	const auditBucket = "io_ext_audit_commit_fail"
	const auditKey = "set/ghost"

	beforeSeq := readBucketSequence(t, db.Bolt(), db.BucketName())
	injected := errors.New("inject commit fail")
	db.testHooks.beforeCommit = func(op string) error {
		if op == "set" {
			return injected
		}
		return nil
	}
	defer func() {
		db.testHooks.beforeCommit = nil
	}()

	err := db.Set("ghost", &Product{SKU: "ghost", Price: 11}, BeforeCommit(func(tx *bbolt.Tx, key string, oldValue, newValue *Product) error {
		if oldValue != nil || newValue == nil || key != "ghost" {
			return fmt.Errorf("unexpected callback state: key=%q old=%#v new=%#v", key, oldValue, newValue)
		}
		audit, err := tx.CreateBucketIfNotExists([]byte(auditBucket))
		if err != nil {
			return fmt.Errorf("create audit bucket: %w", err)
		}
		return audit.Put([]byte(auditKey), []byte(newValue.SKU))
	}))
	if !errors.Is(err, injected) {
		t.Fatalf("expected %v, got %v", injected, err)
	}

	afterSeq := readBucketSequence(t, db.Bolt(), db.BucketName())
	if afterSeq != beforeSeq {
		t.Fatalf("bucket sequence changed after rolled back Set: before=%d after=%d", beforeSeq, afterSeq)
	}

	if v, err := db.Get("ghost"); err != nil {
		t.Fatalf("Get(ghost): %v", err)
	} else if v != nil {
		t.Fatalf("ghost value persisted after commit failure: %#v", v)
	}
	if _, ok := ioExtReadBucketValue(t, db.Bolt(), auditBucket, auditKey); ok {
		t.Fatalf("audit entry persisted after commit failure")
	}
	assertNoFutureSnapshotRefs(t, db)
}

func TestIOExt_BatchSet_BeforeStoreCallbackError_IsAtomicAndKeepsCallerUntouched(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	wantErr := errors.New("before store failed")

	shared := &Rec{Name: "shared", Tags: []string{"go"}, Meta: Meta{Country: "NL"}}
	err := db.BatchSet(
		[]uint64{1, 2},
		[]*Rec{shared, shared},
		BeforeStore(func(key uint64, _ *Rec, newValue *Rec) error {
			newValue.Name = fmt.Sprintf("user-%d", key)
			newValue.Tags = append(newValue.Tags, fmt.Sprintf("tag-%d", key))
			if key == 2 {
				return wantErr
			}
			return nil
		}),
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}

	if shared.Name != "shared" || shared.Country != "NL" || !slices.Equal(shared.Tags, []string{"go"}) {
		t.Fatalf("caller-owned shared value mutated: %#v", shared)
	}
	if v, err := db.Get(1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if v != nil {
		t.Fatalf("id=1 persisted after batch failure: %#v", v)
	}
	if v, err := db.Get(2); err != nil {
		t.Fatalf("Get(2): %v", err)
	} else if v != nil {
		t.Fatalf("id=2 persisted after batch failure: %#v", v)
	}
	if cnt := ioExtMustCountUint64(t, db); cnt != 0 {
		t.Fatalf("expected Count=0, got %d", cnt)
	}
}

func TestIOExt_Patch_CorruptPayloadReturnsErrorAndLeavesOtherRowsReadable(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	ioExtMustSetRec(t, db, 1, &Rec{Name: "one", Age: 10})
	ioExtMustSetRec(t, db, 2, &Rec{Name: "two", Age: 20})
	ioExtMustCorruptUint64Raw(t, db, 2, []byte{0xff, 0x00, 0x7f})

	err := db.Patch(2, []Field{{Name: "age", Value: 99}})
	if err == nil || !strings.Contains(err.Error(), "failed to decode existing value") {
		t.Fatalf("expected Patch decode error, got %v", err)
	}

	v1 := ioExtMustGetRec(t, db, 1)
	if v1.Name != "one" || v1.Age != 10 {
		t.Fatalf("id=1 changed after failed Patch: %#v", v1)
	}
}

func TestIOExt_Delete_CorruptPayloadReturnsErrorAndLeavesRecordPresent(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	ioExtMustSetRec(t, db, 1, &Rec{Name: "alice", Age: 10})
	ioExtMustCorruptUint64Raw(t, db, 1, []byte{0xff, 0x00, 0x7f})

	err := db.Delete(1)
	if err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("expected Delete decode error, got %v", err)
	}

	raw := ioExtMustReadUint64Raw(t, db, 1)
	if len(raw) == 0 {
		t.Fatal("raw payload disappeared after failed Delete")
	}
	if cnt := ioExtMustCountUint64(t, db); cnt != 1 {
		t.Fatalf("expected Count=1 after failed Delete, got %d", cnt)
	}
	ids := ioExtMustQueryName(t, db, "alice")
	if !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("unexpected index state after failed Delete: %v", ids)
	}
}

func TestIOExt_Delete_BeforeCommitError_RollsBackDataAndIndex(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	ioExtMustSetRec(t, db, 1, &Rec{Name: "alice", Age: 10})
	wantErr := errors.New("before commit failed")

	err := db.Delete(1, BeforeCommit(func(_ *bbolt.Tx, _ uint64, oldValue, newValue *Rec) error {
		if oldValue == nil || newValue != nil || oldValue.Name != "alice" {
			t.Fatalf("unexpected callback values: old=%#v new=%#v", oldValue, newValue)
		}
		return wantErr
	}))
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}

	v := ioExtMustGetRec(t, db, 1)
	if v.Name != "alice" || v.Age != 10 {
		t.Fatalf("record changed after failed Delete: %#v", v)
	}
	if cnt := ioExtMustCountUint64(t, db); cnt != 1 {
		t.Fatalf("expected Count=1, got %d", cnt)
	}
	ids := ioExtMustQueryName(t, db, "alice")
	if !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("unexpected name index after failed Delete: %v", ids)
	}
}

func TestIOExt_Delete_BeforeCommit_SeesKeyRemovedAndRollbackKeepsNeighborBucketEmpty(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	ioExtMustSetRec(t, db, 1, &Rec{Name: "alice", Age: 10})

	const auditBucket = "io_ext_audit_delete"
	const auditKey = "delete/1"

	beforeSeq := readBucketSequence(t, db.Bolt(), db.BucketName())
	wantErr := errors.New("stop delete")

	err := db.Delete(1, BeforeCommit(func(tx *bbolt.Tx, key uint64, oldValue, newValue *Rec) error {
		if oldValue == nil || newValue != nil || oldValue.Name != "alice" {
			return fmt.Errorf("unexpected callback values: old=%#v new=%#v", oldValue, newValue)
		}

		managed := tx.Bucket(db.bucket)
		if managed == nil {
			return fmt.Errorf("managed bucket missing")
		}
		if raw := managed.Get(db.keyFromID(key)); raw != nil {
			return fmt.Errorf("managed key still present inside delete BeforeCommit")
		}

		audit, err := tx.CreateBucketIfNotExists([]byte(auditBucket))
		if err != nil {
			return fmt.Errorf("create audit bucket: %w", err)
		}
		if err := audit.Put([]byte(auditKey), []byte(oldValue.Name)); err != nil {
			return fmt.Errorf("put audit entry: %w", err)
		}
		return wantErr
	}))
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}

	afterSeq := readBucketSequence(t, db.Bolt(), db.BucketName())
	if afterSeq != beforeSeq {
		t.Fatalf("bucket sequence changed after rolled back Delete: before=%d after=%d", beforeSeq, afterSeq)
	}

	v := ioExtMustGetRec(t, db, 1)
	if v.Name != "alice" || v.Age != 10 {
		t.Fatalf("record changed after failed Delete: %#v", v)
	}
	if ids := ioExtMustQueryName(t, db, "alice"); !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("unexpected index state after failed Delete: %v", ids)
	}
	if _, ok := ioExtReadBucketValue(t, db.Bolt(), auditBucket, auditKey); ok {
		t.Fatalf("audit entry persisted after rolled back Delete")
	}
}

func TestIOExt_BatchDelete_BeforeCommitErrorOnLaterRecord_RollsBackEarlierDelete(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	ioExtMustSetRec(t, db, 1, &Rec{Name: "one"})
	ioExtMustSetRec(t, db, 2, &Rec{Name: "two"})
	wantErr := errors.New("stop batch delete")

	err := db.BatchDelete([]uint64{1, 2}, BeforeCommit(func(_ *bbolt.Tx, key uint64, _ *Rec, _ *Rec) error {
		if key == 2 {
			return wantErr
		}
		return nil
	}))
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}

	v1 := ioExtMustGetRec(t, db, 1)
	v2 := ioExtMustGetRec(t, db, 2)
	if v1.Name != "one" || v2.Name != "two" {
		t.Fatalf("records changed after rolled back BatchDelete: v1=%#v v2=%#v", v1, v2)
	}
	if cnt := ioExtMustCountUint64(t, db); cnt != 2 {
		t.Fatalf("expected Count=2, got %d", cnt)
	}
}

func TestIOExt_BatchDelete_DuplicateIDs_CallbackRunsOncePerExistingRecord(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	ioExtMustSetRec(t, db, 1, &Rec{Name: "one"})
	ioExtMustSetRec(t, db, 2, &Rec{Name: "two"})

	var calls []uint64
	err := db.BatchDelete([]uint64{1, 1, 2, 2}, BeforeCommit(func(_ *bbolt.Tx, key uint64, oldValue, newValue *Rec) error {
		if oldValue == nil || newValue != nil {
			t.Fatalf("unexpected delete callback values: old=%#v new=%#v", oldValue, newValue)
		}
		calls = append(calls, key)
		return nil
	}))
	if err != nil {
		t.Fatalf("BatchDelete: %v", err)
	}
	if !slices.Equal(calls, []uint64{1, 2}) {
		t.Fatalf("unexpected callback order/count: %v", calls)
	}
	if cnt := ioExtMustCountUint64(t, db); cnt != 0 {
		t.Fatalf("expected Count=0, got %d", cnt)
	}
}

func TestIOExt_BatchSet_DuplicateIDs_BeforeStoreSeesPerStepOldValue(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	ioExtMustSetRec(t, db, 1, &Rec{Name: "seed", Age: 1})

	var seen []string
	err := db.BatchSet(
		[]uint64{1, 1},
		[]*Rec{
			{Name: "first", Age: 2},
			{Name: "second", Age: 3},
		},
		BeforeStore(func(_ uint64, oldValue, newValue *Rec) error {
			oldName := "<nil>"
			if oldValue != nil {
				oldName = oldValue.Name
			}
			seen = append(seen, oldName+"->"+newValue.Name)
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("BatchSet: %v", err)
	}
	if !slices.Equal(seen, []string{"seed->first", "first->second"}) {
		t.Fatalf("unexpected BeforeStore old/new sequence: %v", seen)
	}

	v := ioExtMustGetRec(t, db, 1)
	if v.Name != "second" || v.Age != 3 {
		t.Fatalf("unexpected final value: %#v", v)
	}
}

func TestIOExt_BatchPatch_DuplicateIDs_BeforeStoreSeesPerStepOldValue(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	ioExtMustSetRec(t, db, 1, &Rec{Name: "seed", Age: 10, Meta: Meta{Country: "NL"}})

	call := 0
	var seen []string
	err := db.BatchPatch(
		[]uint64{1, 1},
		[]Field{{Name: "country", Value: "PL"}},
		BeforeProcess(func(_ uint64, value *Rec) error {
			call++
			value.Name = fmt.Sprintf("patched-%d", call)
			return nil
		}),
		BeforeStore(func(_ uint64, oldValue, newValue *Rec) error {
			seen = append(seen, oldValue.Name+"->"+newValue.Name)
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("BatchPatch: %v", err)
	}
	if !slices.Equal(seen, []string{"seed->patched-1", "patched-1->patched-2"}) {
		t.Fatalf("unexpected BeforeStore old/new sequence: %v", seen)
	}

	v := ioExtMustGetRec(t, db, 1)
	if v.Name != "patched-2" || v.Country != "PL" {
		t.Fatalf("unexpected final value: %#v", v)
	}
}

func TestIOExt_BatchPatch_DuplicateIDs_BeforeProcessSeesPerStepCurrentValue(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	ioExtMustSetRec(t, db, 1, &Rec{Name: "seed", Age: 10, Meta: Meta{Country: "NL"}})

	var ages []int
	err := db.BatchPatch(
		[]uint64{1, 1},
		[]Field{{Name: "country", Value: "PL"}},
		BeforeProcess(func(_ uint64, value *Rec) error {
			value.Age++
			ages = append(ages, value.Age)
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("BatchPatch: %v", err)
	}
	if !slices.Equal(ages, []int{11, 12}) {
		t.Fatalf("BeforeProcess did not observe per-step current value: %v", ages)
	}

	v := ioExtMustGetRec(t, db, 1)
	if v.Age != 12 || v.Country != "PL" {
		t.Fatalf("unexpected final value: %#v", v)
	}
}

func TestIOExt_BatchSet_DuplicateIDs_FinalValueMatchesLastWriteAcrossReadPaths(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	err := db.BatchSet(
		[]uint64{1, 1, 2},
		[]*Rec{
			{Name: "first", Age: 1, Meta: Meta{Country: "NL"}},
			{Name: "second", Age: 2, Meta: Meta{Country: "PL"}},
			{Name: "other", Age: 3, Meta: Meta{Country: "DE"}},
		},
	)
	if err != nil {
		t.Fatalf("BatchSet: %v", err)
	}

	v1 := ioExtMustGetRec(t, db, 1)
	v2 := ioExtMustGetRec(t, db, 2)
	if v1.Name != "second" || v1.Age != 2 || v1.Country != "PL" {
		t.Fatalf("unexpected final value for id=1: %#v", v1)
	}
	if v2.Name != "other" || v2.Age != 3 || v2.Country != "DE" {
		t.Fatalf("unexpected final value for id=2: %#v", v2)
	}

	vals, err := db.BatchGet(1, 2)
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	if vals[0] == nil || vals[0].Name != "second" || vals[1] == nil || vals[1].Name != "other" {
		t.Fatalf("unexpected BatchGet state: %#v", vals)
	}

	scanned := ioExtCollectSeqScanRec(t, db, 0)
	if scanned[1].Name != "second" || scanned[2].Name != "other" {
		t.Fatalf("unexpected SeqScan state: %#v", scanned)
	}

	if ids := ioExtMustQueryName(t, db, "first"); len(ids) != 0 {
		t.Fatalf("stale name index for overwritten value: %v", ids)
	}
	if ids := ioExtMustQueryName(t, db, "second"); !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("unexpected name index for second: %v", ids)
	}
}

func TestIOExt_BatchPatch_DuplicateIDs_FinalValueMatchesLastPatchAcrossReadPaths(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	ioExtMustSetRec(t, db, 1, &Rec{Name: "seed-1", Age: 10, Meta: Meta{Country: "NL"}})
	ioExtMustSetRec(t, db, 2, &Rec{Name: "seed-2", Age: 20, Meta: Meta{Country: "DE"}})

	call := 0
	err := db.BatchPatch(
		[]uint64{1, 1, 2},
		[]Field{{Name: "country", Value: "US"}},
		BeforeProcess(func(_ uint64, value *Rec) error {
			call++
			value.Name = fmt.Sprintf("step-%d", call)
			value.Age += call
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("BatchPatch: %v", err)
	}

	v1 := ioExtMustGetRec(t, db, 1)
	v2 := ioExtMustGetRec(t, db, 2)
	if v1.Name != "step-2" || v1.Age != 13 || v1.Country != "US" {
		t.Fatalf("unexpected final value for id=1: %#v", v1)
	}
	if v2.Name != "step-3" || v2.Age != 23 || v2.Country != "US" {
		t.Fatalf("unexpected final value for id=2: %#v", v2)
	}

	vals, err := db.BatchGet(1, 2)
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	if vals[0] == nil || vals[0].Name != "step-2" || vals[1] == nil || vals[1].Name != "step-3" {
		t.Fatalf("unexpected BatchGet state: %#v", vals)
	}

	scanned := ioExtCollectSeqScanRec(t, db, 0)
	if scanned[1].Name != "step-2" || scanned[2].Name != "step-3" {
		t.Fatalf("unexpected SeqScan state: %#v", scanned)
	}

	if ids := ioExtMustQueryName(t, db, "step-1"); len(ids) != 0 {
		t.Fatalf("stale name index for intermediate patch value: %v", ids)
	}
}

func TestIOExt_ConcurrentUint64ReadersWriters_NoPanicsOrDecodeErrors(t *testing.T) {
	db, _ := openTempDBUint64(t)
	db.DisableSync()
	defer db.EnableSync()

	const keys = 8
	for i := 1; i <= keys; i++ {
		ioExtMustSetRec(t, db, uint64(i), &Rec{
			Name: fmt.Sprintf("seed-%d", i),
			Age:  i,
			Tags: []string{fmt.Sprintf("t%d", i%3)},
			Meta: Meta{Country: "NL"},
		})
	}

	ids := make([]uint64, keys)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}

	errCh := make(chan error, 128)
	var writers sync.WaitGroup
	var readers sync.WaitGroup

	for w := 0; w < 2; w++ {
		w := w
		writers.Add(1)
		go func() {
			defer writers.Done()
			defer func() {
				if r := recover(); r != nil {
					errCh <- fmt.Errorf("writer panic: %v", r)
				}
			}()
			for i := 0; i < 150; i++ {
				id := uint64((i+w)%keys + 1)
				switch i % 3 {
				case 0:
					if err := db.Set(id, &Rec{
						Name: fmt.Sprintf("w%d-set-%d", w, i),
						Age:  i,
						Tags: []string{fmt.Sprintf("set-%d", i%5)},
						Meta: Meta{Country: "PL"},
					}); err != nil {
						errCh <- fmt.Errorf("writer=%d Set(%d): %w", w, id, err)
						return
					}
				case 1:
					if err := db.Patch(id, []Field{
						{Name: "age", Value: 1000 + i},
						{Name: "country", Value: "DE"},
					}); err != nil {
						errCh <- fmt.Errorf("writer=%d Patch(%d): %w", w, id, err)
						return
					}
				default:
					if err := db.Delete(id); err != nil {
						errCh <- fmt.Errorf("writer=%d Delete(%d): %w", w, id, err)
						return
					}
				}
			}
		}()
	}

	for r := 0; r < 3; r++ {
		r := r
		readers.Add(1)
		go func() {
			defer readers.Done()
			defer func() {
				if rr := recover(); rr != nil {
					errCh <- fmt.Errorf("reader panic: %v", rr)
				}
			}()
			for i := 0; i < 120; i++ {
				id := uint64((i+r)%keys + 1)
				v, err := db.Get(id)
				if err != nil {
					errCh <- fmt.Errorf("reader=%d Get(%d): %w", r, id, err)
					return
				}
				if v != nil && v.Name == "" {
					errCh <- fmt.Errorf("reader=%d Get(%d): empty name", r, id)
					return
				}

				vals, err := db.BatchGet(ids...)
				if err != nil {
					errCh <- fmt.Errorf("reader=%d BatchGet: %w", r, err)
					return
				}
				for i, v := range vals {
					if v != nil && v.Name == "" {
						errCh <- fmt.Errorf("reader=%d BatchGet[%d]: empty name", r, i)
						return
					}
				}

				if err := db.SeqScan(0, func(id uint64, v *Rec) (bool, error) {
					if v.Name == "" {
						return false, fmt.Errorf("SeqScan id=%d empty name", id)
					}
					return true, nil
				}); err != nil {
					errCh <- fmt.Errorf("reader=%d SeqScan: %w", r, err)
					return
				}

				if err := db.SeqScanRaw(0, func(id uint64, raw []byte) (bool, error) {
					cp := append([]byte(nil), raw...)
					v, err := db.decode(cp)
					if err != nil {
						return false, fmt.Errorf("decode raw id=%d: %w", id, err)
					}
					defer db.ReleaseRecords(v)
					if v.Name == "" {
						return false, fmt.Errorf("raw id=%d empty name", id)
					}
					return true, nil
				}); err != nil {
					errCh <- fmt.Errorf("reader=%d SeqScanRaw: %w", r, err)
					return
				}

				if err := db.ScanKeys(0, func(id uint64) (bool, error) {
					if id == 0 {
						return false, fmt.Errorf("zero key")
					}
					return true, nil
				}); err != nil {
					errCh <- fmt.Errorf("reader=%d ScanKeys: %w", r, err)
					return
				}
			}
		}()
	}

	writers.Wait()
	readers.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		t.FailNow()
	}
	if _, err := db.BatchGet(ids...); err != nil {
		t.Fatalf("final BatchGet: %v", err)
	}
}

func TestIOExt_ConcurrentStringReadersWriters_NoPanicsOrDecodeErrors(t *testing.T) {
	db, _ := openTempDBStringProduct(t)
	db.DisableSync()
	defer db.EnableSync()

	keys := []string{"k1", "k2", "k3", "k4", "k5", "k6"}
	for i, key := range keys {
		ioExtMustSetProduct(t, db, key, &Product{
			SKU:   key,
			Price: float64(i + 1),
			Tags:  []string{fmt.Sprintf("t%d", i%3)},
		})
	}

	errCh := make(chan error, 128)
	var writers sync.WaitGroup
	var readers sync.WaitGroup

	for w := 0; w < 2; w++ {
		w := w
		writers.Add(1)
		go func() {
			defer writers.Done()
			defer func() {
				if r := recover(); r != nil {
					errCh <- fmt.Errorf("writer panic: %v", r)
				}
			}()
			for i := 0; i < 150; i++ {
				key := keys[(i+w)%len(keys)]
				switch i % 3 {
				case 0:
					if err := db.Set(key, &Product{
						SKU:   key,
						Price: float64(i + 1),
						Tags:  []string{fmt.Sprintf("set-%d", i%5)},
					}); err != nil {
						errCh <- fmt.Errorf("writer=%d Set(%q): %w", w, key, err)
						return
					}
				case 1:
					if err := db.Patch(key, []Field{
						{Name: "price", Value: float64(1000 + i)},
						{Name: "tags", Value: []string{fmt.Sprintf("patch-%d", i%5)}},
					}); err != nil {
						errCh <- fmt.Errorf("writer=%d Patch(%q): %w", w, key, err)
						return
					}
				default:
					if err := db.Delete(key); err != nil {
						errCh <- fmt.Errorf("writer=%d Delete(%q): %w", w, key, err)
						return
					}
				}
			}
		}()
	}

	for r := 0; r < 3; r++ {
		r := r
		readers.Add(1)
		go func() {
			defer readers.Done()
			defer func() {
				if rr := recover(); rr != nil {
					errCh <- fmt.Errorf("reader panic: %v", rr)
				}
			}()
			for i := 0; i < 120; i++ {
				key := keys[(i+r)%len(keys)]
				v, err := db.Get(key)
				if err != nil {
					errCh <- fmt.Errorf("reader=%d Get(%q): %w", r, key, err)
					return
				}
				if v != nil && v.SKU == "" {
					errCh <- fmt.Errorf("reader=%d Get(%q): empty sku", r, key)
					return
				}

				vals, err := db.BatchGet(keys...)
				if err != nil {
					errCh <- fmt.Errorf("reader=%d BatchGet: %w", r, err)
					return
				}
				for i, v := range vals {
					if v != nil && v.SKU == "" {
						errCh <- fmt.Errorf("reader=%d BatchGet[%d]: empty sku", r, i)
						return
					}
				}

				if err := db.SeqScan("", func(id string, v *Product) (bool, error) {
					if id == "" || v.SKU == "" {
						return false, fmt.Errorf("SeqScan empty id/sku")
					}
					return true, nil
				}); err != nil {
					errCh <- fmt.Errorf("reader=%d SeqScan: %w", r, err)
					return
				}

				if err := db.SeqScanRaw("", func(id string, raw []byte) (bool, error) {
					cp := append([]byte(nil), raw...)
					v, err := db.decode(cp)
					if err != nil {
						return false, fmt.Errorf("decode raw id=%q: %w", id, err)
					}
					defer db.ReleaseRecords(v)
					if id == "" || v.SKU == "" {
						return false, fmt.Errorf("raw empty id/sku")
					}
					return true, nil
				}); err != nil {
					errCh <- fmt.Errorf("reader=%d SeqScanRaw: %w", r, err)
					return
				}

				if err := db.ScanKeys("", func(id string) (bool, error) {
					if id == "" {
						return false, fmt.Errorf("empty key")
					}
					return true, nil
				}); err != nil {
					errCh <- fmt.Errorf("reader=%d ScanKeys: %w", r, err)
					return
				}
			}
		}()
	}

	writers.Wait()
	readers.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		t.FailNow()
	}

	scanned := ioExtCollectSeqScanProduct(t, db, "")
	for key, v := range scanned {
		if key == "" || v.SKU == "" {
			t.Fatalf("invalid final scanned product: key=%q value=%#v", key, v)
		}
		raw := ioExtMustReadStringRaw(t, db, key)
		decoded, err := db.decode(raw)
		if err != nil {
			t.Fatalf("final decode(%q): %v", key, err)
		}
		if decoded.SKU == "" {
			db.ReleaseRecords(decoded)
			t.Fatalf("final decoded product has empty sku for key=%q", key)
		}
		db.ReleaseRecords(decoded)
	}
}
