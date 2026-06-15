package rbi

import (
	"io"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/vapstack/qx"
	"github.com/vmihailenco/msgpack/v5"
	"go.etcd.io/bbolt"
)

type patchQueuedOwnershipRec struct {
	Name string   `db:"name" rbi:"index"`
	Tags []string `db:"tags" rbi:"index"`
}

type patchQueuedOwnershipPayload struct {
	Name string
	Tags []string
}

var (
	patchQueuedOwnershipDecodeArmed   atomic.Bool
	patchQueuedOwnershipDecodeStarted chan struct{}
	patchQueuedOwnershipDecodeResume  chan struct{}
)

func (r *patchQueuedOwnershipRec) EncodeRBI(w io.Writer) error {
	return msgpack.NewEncoder(w).Encode(patchQueuedOwnershipPayload{Name: r.Name, Tags: r.Tags})
}

func (r *patchQueuedOwnershipRec) DecodeRBI(rd io.Reader) error {
	if patchQueuedOwnershipDecodeArmed.CompareAndSwap(true, false) {
		close(patchQueuedOwnershipDecodeStarted)
		<-patchQueuedOwnershipDecodeResume
	}
	var payload patchQueuedOwnershipPayload
	if err := msgpack.NewDecoder(rd).Decode(&payload); err != nil {
		return err
	}
	r.Name = payload.Name
	r.Tags = payload.Tags
	return nil
}

func mustMakePatch[V any](t testing.TB, db *DB[uint64, V], oldVal, newVal *V, opts ...PatchOption) []Field {
	t.Helper()
	patch, err := db.MakePatch(oldVal, newVal, opts...)
	if err != nil {
		t.Fatalf("MakePatch: %v", err)
	}
	return patch
}

func mustMakePatchInto[V any](t testing.TB, db *DB[uint64, V], oldVal, newVal *V, dst []Field, opts ...PatchOption) []Field {
	t.Helper()
	patch, err := db.MakePatchInto(oldVal, newVal, dst, opts...)
	if err != nil {
		t.Fatalf("MakePatchInto: %v", err)
	}
	return patch
}

type PatchAnonymousID string

type patchAnonymousTaggedIndexRec struct {
	PatchAnonymousID `rbi:"index"`
}

type patchAnonymousOptionIndexRec struct {
	PatchAnonymousID
}

type PatchAnonymousToken struct {
	Value string `db:"value"`
}

func (v PatchAnonymousToken) IndexingValue() string {
	return strings.ToLower(v.Value)
}

type patchAnonymousTaggedTokenRec struct {
	PatchAnonymousToken `rbi:"index"`
}

type patchAnonymousOptionTokenRec struct {
	PatchAnonymousToken
}

type PatchAnonymousShadowToken struct {
	ID string
}

func (v PatchAnonymousShadowToken) IndexingValue() string {
	return strings.ToLower(v.ID)
}

type patchAnonymousShadowTokenRec struct {
	PatchAnonymousShadowToken `rbi:"index"`
	ID                        string
}

type PatchAnonymousJSONHiddenToken struct {
	Value string `db:"value" json:"-"`
}

func (v PatchAnonymousJSONHiddenToken) IndexingValue() string {
	return strings.ToLower(v.Value)
}

type patchAnonymousJSONHiddenTokenRec struct {
	PatchAnonymousJSONHiddenToken `rbi:"index"`
}

type PatchAnonymousEarlyChildToken struct {
	Value string `db:"AAA" json:"-"`
}

func (v PatchAnonymousEarlyChildToken) IndexingValue() string {
	return strings.ToLower(v.Value)
}

type patchAnonymousEarlyChildTokenRec struct {
	PatchAnonymousEarlyChildToken
}

func TestMakePatch_AnonymousIndexedFieldRoundTrips(t *testing.T) {
	db := openTempDBUint64Reflect[patchAnonymousTaggedIndexRec](t, "patch_anonymous_tagged_index.db")

	oldVal := &patchAnonymousTaggedIndexRec{PatchAnonymousID: "old"}
	newVal := &patchAnonymousTaggedIndexRec{PatchAnonymousID: "new"}
	if err := db.Set(1, oldVal); err != nil {
		t.Fatalf("Set: %v", err)
	}
	ids, err := db.QueryKeys(qx.Query(qx.EQ("PatchAnonymousID", PatchAnonymousID("old"))))
	if err != nil {
		t.Fatalf("QueryKeys old anonymous ID: %v", err)
	}
	if !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("old anonymous ID ids=%v want [1]", ids)
	}

	patch := mustMakePatch(t, db, oldVal, newVal)
	if len(patch) != 1 || patch[0].Name != "PatchAnonymousID" || patch[0].Value != PatchAnonymousID("new") {
		t.Fatalf("anonymous ID patch=%#v", patch)
	}
	if err = db.Patch(1, patch, PatchStrict); err != nil {
		t.Fatalf("Patch anonymous ID: %v", err)
	}
	ids, err = db.QueryKeys(qx.Query(qx.EQ("PatchAnonymousID", PatchAnonymousID("new"))))
	if err != nil {
		t.Fatalf("QueryKeys new anonymous ID: %v", err)
	}
	if !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("new anonymous ID ids=%v want [1]", ids)
	}
	ids, err = db.QueryKeys(qx.Query(qx.EQ("PatchAnonymousID", PatchAnonymousID("old"))))
	if err != nil {
		t.Fatalf("QueryKeys stale anonymous ID: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("stale anonymous ID ids=%v want []", ids)
	}
}

func TestMakePatch_AnonymousOptionIndexedFieldRoundTrips(t *testing.T) {
	db := openTempDBUint64Reflect[patchAnonymousOptionIndexRec](t, "patch_anonymous_option_index.db", Options{
		Index: map[string]IndexKind{"PatchAnonymousID": IndexDefault},
	})

	oldVal := &patchAnonymousOptionIndexRec{PatchAnonymousID: "old"}
	newVal := &patchAnonymousOptionIndexRec{PatchAnonymousID: "new"}
	if err := db.Set(1, oldVal); err != nil {
		t.Fatalf("Set: %v", err)
	}

	patch := mustMakePatch(t, db, oldVal, newVal)
	if len(patch) != 1 || patch[0].Name != "PatchAnonymousID" || patch[0].Value != PatchAnonymousID("new") {
		t.Fatalf("anonymous option ID patch=%#v", patch)
	}
	if err := db.Patch(1, patch, PatchStrict); err != nil {
		t.Fatalf("Patch anonymous option ID: %v", err)
	}
	ids, err := db.QueryKeys(qx.Query(qx.EQ("PatchAnonymousID", PatchAnonymousID("new"))))
	if err != nil {
		t.Fatalf("QueryKeys anonymous option ID: %v", err)
	}
	if !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("anonymous option ID ids=%v want [1]", ids)
	}
}

func TestMakePatch_IndexedAnonymousParentSuppressesUnsafeDescendant(t *testing.T) {
	db := openTempDBUint64Reflect[patchAnonymousShadowTokenRec](t, "patch_anonymous_shadow_token.db")

	oldVal := &patchAnonymousShadowTokenRec{
		PatchAnonymousShadowToken: PatchAnonymousShadowToken{ID: "Old"},
		ID:                        "outer",
	}
	newVal := &patchAnonymousShadowTokenRec{
		PatchAnonymousShadowToken: PatchAnonymousShadowToken{ID: "New"},
		ID:                        "outer",
	}

	patch := mustMakePatch(t, db, oldVal, newVal)
	if len(patch) != 1 || patch[0].Name != "PatchAnonymousShadowToken" {
		t.Fatalf("shadowed child patch=%#v", patch)
	}
	if v, ok := patch[0].Value.(PatchAnonymousShadowToken); !ok || v.ID != "New" {
		t.Fatalf("shadowed child parent patch value=%#v", patch[0].Value)
	}

	applied := *oldVal
	if err := db.schema.Patch.Apply(unsafe.Pointer(&applied), patchItemsForWrite(patch), false); err != nil {
		t.Fatalf("Apply parent patch: %v", err)
	}
	if applied.PatchAnonymousShadowToken.ID != "New" || applied.ID != "outer" {
		t.Fatalf("parent patch applied wrong value: %+v", applied)
	}

	patch = mustMakePatch(t, db, oldVal, newVal, PatchJSON)
	if len(patch) != 1 || patch[0].Name != "PatchAnonymousShadowToken" {
		t.Fatalf("shadowed child JSON patch=%#v", patch)
	}
}

func TestMakePatch_PatchJSONIndexedAnonymousParentSuppressesJSONHiddenDescendant(t *testing.T) {
	db := openTempDBUint64Reflect[patchAnonymousJSONHiddenTokenRec](t, "patch_anonymous_json_hidden_token.db")

	oldVal := &patchAnonymousJSONHiddenTokenRec{PatchAnonymousJSONHiddenToken: PatchAnonymousJSONHiddenToken{Value: "Old"}}
	newVal := &patchAnonymousJSONHiddenTokenRec{PatchAnonymousJSONHiddenToken: PatchAnonymousJSONHiddenToken{Value: "old"}}

	patch := mustMakePatch(t, db, oldVal, newVal, PatchJSON)
	if len(patch) != 1 || patch[0].Name != "PatchAnonymousJSONHiddenToken" {
		t.Fatalf("JSON hidden child patch=%#v", patch)
	}
	if v, ok := patch[0].Value.(PatchAnonymousJSONHiddenToken); !ok || v.Value != "old" {
		t.Fatalf("JSON hidden child parent patch value=%#v", patch[0].Value)
	}

	applied := *oldVal
	if err := db.schema.Patch.Apply(unsafe.Pointer(&applied), patchItemsForWrite(patch), false); err != nil {
		t.Fatalf("Apply JSON parent patch: %v", err)
	}
	if applied.Value != "old" {
		t.Fatalf("JSON parent patch applied value=%q", applied.Value)
	}
}

func TestMakePatch_PatchJSONIndexedAnonymousParentSuppressesEarlierIndexedChild(t *testing.T) {
	db := openTempDBUint64Reflect[patchAnonymousEarlyChildTokenRec](t, "patch_anonymous_early_child_token.db", Options{
		Index: map[string]IndexKind{
			"AAA":                           IndexDefault,
			"PatchAnonymousEarlyChildToken": IndexDefault,
		},
	})

	oldVal := &patchAnonymousEarlyChildTokenRec{PatchAnonymousEarlyChildToken: PatchAnonymousEarlyChildToken{Value: "Old"}}
	newVal := &patchAnonymousEarlyChildTokenRec{PatchAnonymousEarlyChildToken: PatchAnonymousEarlyChildToken{Value: "New"}}

	patch := mustMakePatch(t, db, oldVal, newVal, PatchJSON)
	if len(patch) != 1 || patch[0].Name != "PatchAnonymousEarlyChildToken" {
		t.Fatalf("early indexed child patch=%#v", patch)
	}
	if v, ok := patch[0].Value.(PatchAnonymousEarlyChildToken); !ok || v.Value != "New" {
		t.Fatalf("early indexed child parent patch value=%#v", patch[0].Value)
	}
}

func TestPatch_AnonymousValueIndexerChildFieldUpdatesParentIndex(t *testing.T) {
	t.Run("tagged_parent", func(t *testing.T) {
		db := openTempDBUint64Reflect[patchAnonymousTaggedTokenRec](t, "patch_anonymous_tagged_token.db")
		if err := db.Set(1, &patchAnonymousTaggedTokenRec{PatchAnonymousToken: PatchAnonymousToken{Value: "Old"}}); err != nil {
			t.Fatalf("Set: %v", err)
		}
		ids, err := db.QueryKeys(qx.Query(qx.EQ("PatchAnonymousToken", "old")))
		if err != nil {
			t.Fatalf("QueryKeys old PatchAnonymousToken: %v", err)
		}
		if !slices.Equal(ids, []uint64{1}) {
			t.Fatalf("old PatchAnonymousToken ids=%v want [1]", ids)
		}

		if err := db.Patch(1, []Field{{Name: "value", Value: "New"}}, PatchStrict); err != nil {
			t.Fatalf("Patch child field: %v", err)
		}
		ids, err = db.QueryKeys(qx.Query(qx.EQ("PatchAnonymousToken", "new")))
		if err != nil {
			t.Fatalf("QueryKeys new PatchAnonymousToken: %v", err)
		}
		if !slices.Equal(ids, []uint64{1}) {
			t.Fatalf("new PatchAnonymousToken ids=%v want [1]", ids)
		}
		ids, err = db.QueryKeys(qx.Query(qx.EQ("PatchAnonymousToken", "old")))
		if err != nil {
			t.Fatalf("QueryKeys stale PatchAnonymousToken: %v", err)
		}
		if len(ids) != 0 {
			t.Fatalf("stale PatchAnonymousToken ids=%v want []", ids)
		}
	})

	t.Run("options_parent_and_child", func(t *testing.T) {
		db := openTempDBUint64Reflect[patchAnonymousOptionTokenRec](t, "patch_anonymous_option_token.db", Options{
			Index: map[string]IndexKind{
				"PatchAnonymousToken": IndexDefault,
				"value":               IndexDefault,
			},
		})
		if err := db.Set(1, &patchAnonymousOptionTokenRec{PatchAnonymousToken: PatchAnonymousToken{Value: "Old"}}); err != nil {
			t.Fatalf("Set: %v", err)
		}
		for _, indexed := range []struct {
			field string
			old   string
			new   string
		}{
			{field: "PatchAnonymousToken", old: "old", new: "new"},
			{field: "value", old: "Old", new: "New"},
		} {
			ids, err := db.QueryKeys(qx.Query(qx.EQ(indexed.field, indexed.old)))
			if err != nil {
				t.Fatalf("QueryKeys old %s: %v", indexed.field, err)
			}
			if !slices.Equal(ids, []uint64{1}) {
				t.Fatalf("old %s ids=%v want [1]", indexed.field, ids)
			}
		}

		if err := db.Patch(1, []Field{{Name: "value", Value: "New"}}, PatchStrict); err != nil {
			t.Fatalf("Patch child field: %v", err)
		}
		for _, indexed := range []struct {
			field string
			old   string
			new   string
		}{
			{field: "PatchAnonymousToken", old: "old", new: "new"},
			{field: "value", old: "Old", new: "New"},
		} {
			ids, err := db.QueryKeys(qx.Query(qx.EQ(indexed.field, indexed.new)))
			if err != nil {
				t.Fatalf("QueryKeys new %s: %v", indexed.field, err)
			}
			if !slices.Equal(ids, []uint64{1}) {
				t.Fatalf("new %s ids=%v want [1]", indexed.field, ids)
			}
			ids, err = db.QueryKeys(qx.Query(qx.EQ(indexed.field, indexed.old)))
			if err != nil {
				t.Fatalf("QueryKeys stale %s: %v", indexed.field, err)
			}
			if len(ids) != 0 {
				t.Fatalf("stale %s ids=%v want []", indexed.field, ids)
			}
		}
	})
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
		var err error
		patch, err = db.MakePatch(oldValue, newValue)
		return err
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

type patchInterfacePointerRec struct {
	Name   string `db:"name" rbi:"index"`
	Value  any
	Values []any
}

type patchPointerShapeRec struct {
	Name  string `db:"name" rbi:"index"`
	Tags  *[]int
	Next  **int
	Value *any
}

func TestMakePatch_RoundTripPreservesInterfacePointerValues(t *testing.T) {
	db := openTempDBUint64Reflect[patchInterfacePointerRec](t, "patch_interface_pointer.db")

	ptr := 11
	var nilPtr *int
	oldVal := &patchInterfacePointerRec{Name: "alice"}
	newVal := &patchInterfacePointerRec{
		Name:   "alice",
		Value:  &ptr,
		Values: []any{&ptr, nilPtr},
	}

	patch := mustMakePatch(t, db, oldVal, newVal)
	applied := *oldVal
	if err := db.schema.Patch.Apply(unsafe.Pointer(&applied), patchItemsForWrite(patch), false); err != nil {
		t.Fatalf("Patch.Apply: %v", err)
	}

	if got, ok := applied.Value.(*int); !ok || got == nil || *got != 11 {
		t.Fatalf("interface pointer round-trip failed: %#v", applied.Value)
	}
	if len(applied.Values) != 2 {
		t.Fatalf("interface slice length=%d want 2", len(applied.Values))
	}
	if got, ok := applied.Values[0].(*int); !ok || got == nil || *got != 11 {
		t.Fatalf("interface slice pointer round-trip failed: %#v", applied.Values)
	}
	if got, ok := applied.Values[1].(*int); !ok || got != nil {
		t.Fatalf("interface slice typed nil pointer round-trip failed: %#v", applied.Values)
	}
}

func TestMakePatch_RoundTripPreservesPointerValueShape(t *testing.T) {
	db := openTempDBUint64Reflect[patchPointerShapeRec](t, "patch_pointer_shape.db")

	tags := []int{1, 2}
	nextValue := 33
	next := &nextValue
	anyValue := any(44)
	oldVal := &patchPointerShapeRec{Name: "alice"}
	newVal := &patchPointerShapeRec{
		Name:  "alice",
		Tags:  &tags,
		Next:  &next,
		Value: &anyValue,
	}

	patch := mustMakePatch(t, db, oldVal, newVal)
	applied := *oldVal
	if err := db.schema.Patch.Apply(unsafe.Pointer(&applied), patchItemsForWrite(patch), false); err != nil {
		t.Fatalf("Patch.Apply: %v", err)
	}

	if applied.Tags == nil || !slices.Equal(*applied.Tags, []int{1, 2}) {
		t.Fatalf("pointer-to-slice round-trip failed: %#v", applied.Tags)
	}
	if applied.Next == nil || *applied.Next == nil || **applied.Next != 33 {
		t.Fatalf("nested pointer round-trip failed: %#v", applied.Next)
	}
	if applied.Value == nil {
		t.Fatal("pointer-to-interface round-trip produced nil pointer")
	}
	if got, ok := (*applied.Value).(int); !ok || got != 44 {
		t.Fatalf("pointer-to-interface round-trip failed: %#v", *applied.Value)
	}
}

func TestMakePatch_EmitsNilToEmptySliceTransition(t *testing.T) {
	db, _ := openTempDBUint64(t)

	oldVal := &Rec{Name: "alice"}
	newVal := &Rec{Name: "alice", Tags: []string{}}

	patch := mustMakePatch(t, db, oldVal, newVal)
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

	applied := applyPatchForTest(t, db, oldVal, patch)
	if applied.Tags == nil || len(applied.Tags) != 0 {
		t.Fatalf("patched record lost non-nil empty slice: %#v", applied.Tags)
	}
}

func TestMakePatch_EmitsEmptyToNilSliceTransition(t *testing.T) {
	db, _ := openTempDBUint64(t)

	oldVal := &Rec{Name: "alice", Tags: []string{}}
	newVal := &Rec{Name: "alice"}

	patch := mustMakePatch(t, db, oldVal, newVal)
	fields := patchFieldsByName(patch)

	gotTags, ok := fields["tags"].([]string)
	if !ok {
		t.Fatalf("patch must contain []string value for tags, got %#v", fields["tags"])
	}
	if gotTags != nil {
		t.Fatalf("patch must preserve nil slice, got %#v", gotTags)
	}
	if len(patch) != 1 {
		t.Fatalf("expected exactly one changed field, got %#v", patch)
	}

	applied := applyPatchForTest(t, db, oldVal, patch)
	if applied.Tags != nil {
		t.Fatalf("patched record lost nil slice: %#v", applied.Tags)
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

	patch := mustMakePatch(t, db, oldVal, newVal)
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

	patch := mustMakePatch(t, db, oldVal, newVal, PatchJSON)
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

type PatchJSONPromotedLeftRec struct {
	ID string `db:"left_id"`
}

type PatchJSONPromotedRightRec struct {
	ID string `db:"right_id"`
}

type patchJSONPromotedRec struct {
	PatchJSONPromotedLeftRec
	PatchJSONPromotedRightRec
}

func TestMakePatch_PatchJSON_RejectsAmbiguousPromotedGoNameWithoutJSONTags(t *testing.T) {
	db := openTempDBUint64Reflect[patchJSONPromotedRec](t, "patch_json_promoted_db_names.db")

	oldVal := &patchJSONPromotedRec{
		PatchJSONPromotedLeftRec:  PatchJSONPromotedLeftRec{ID: "left-old"},
		PatchJSONPromotedRightRec: PatchJSONPromotedRightRec{ID: "right-old"},
	}
	newVal := &patchJSONPromotedRec{
		PatchJSONPromotedLeftRec:  PatchJSONPromotedLeftRec{ID: "left-new"},
		PatchJSONPromotedRightRec: PatchJSONPromotedRightRec{ID: "right-new"},
	}

	patch := mustMakePatch(t, db, oldVal, newVal)
	fields := patchFieldsByName(patch)
	if fields["left_id"] != "left-new" || fields["right_id"] != "right-new" {
		t.Fatalf("MakePatch did not use db names for promoted fields: %#v", patch)
	}

	patch, err := db.MakePatch(oldVal, newVal, PatchJSON)
	if err == nil || !strings.Contains(err.Error(), "cannot be emitted with PatchJSON") {
		t.Fatalf("MakePatch PatchJSON err=%v want unsafe json name error", err)
	}
	if len(patch) != 0 {
		t.Fatalf("MakePatch PatchJSON returned partial patch after error: %#v", patch)
	}
	scratch := []Field{{Name: "stale", Value: "value"}}
	patch, err = db.MakePatchInto(oldVal, newVal, scratch, PatchJSON)
	if err == nil || !strings.Contains(err.Error(), "cannot be emitted with PatchJSON") {
		t.Fatalf("MakePatchInto PatchJSON err=%v want unsafe json name error", err)
	}
	if len(patch) != 0 {
		t.Fatalf("MakePatchInto PatchJSON returned partial patch after error: %#v", patch)
	}
}

type PatchUnaliasedPromotedLeftRec struct {
	ID string
}

type PatchUnaliasedPromotedRightRec struct {
	ID string
}

type patchUnaliasedPromotedRec struct {
	PatchUnaliasedPromotedLeftRec
	PatchUnaliasedPromotedRightRec
}

type PatchShadowedIndexedJSONOnlyEmbeddedRec struct {
	ID string `json:"inner" rbi:"index"`
}

type patchShadowedIndexedJSONOnlyRec struct {
	PatchShadowedIndexedJSONOnlyEmbeddedRec
	ID string
}

type PatchShadowedMeasureJSONOnlyEmbeddedRec struct {
	Score int64 `json:"innerScore" rbi:"measure"`
}

type patchShadowedMeasureJSONOnlyRec struct {
	PatchShadowedMeasureJSONOnlyEmbeddedRec
	Score int64
}

type patchMeasureFloatZeroRec struct {
	Name      string   `db:"name" rbi:"index"`
	Measure   float64  `db:"measure" rbi:"measure"`
	Measure32 float32  `db:"measure32" rbi:"measure"`
	Ptr       *float64 `db:"ptr" rbi:"measure"`
}

type patchFloat64s []float64

type patchFloatSliceZeroRec struct {
	Name     string    `db:"name"`
	Values64 []float64 `db:"values64"`
	Values32 []float32 `db:"values32"`
	Named64  patchFloat64s
}

type patchFloatNestedZeroPayload struct {
	Value64 float64
	Value32 float32
	Values  [2]float64
}

type patchFloatNestedZeroRec struct {
	Name    string                      `db:"name"`
	Payload patchFloatNestedZeroPayload `db:"payload"`
}

type patchFloatPointerFallbackRec struct {
	Name   string
	Hidden *float64 `json:"-"`
}

type patchIndexedFloatHiddenRec struct {
	Name    string   `db:"name"`
	Score64 float64  `db:"score64" json:"-" rbi:"index"`
	Score32 float32  `db:"score32" json:"-" rbi:"index"`
	Ptr     *float64 `db:"ptr" json:"-" rbi:"index"`
}

type patchIndexedFloatSliceHiddenRec struct {
	Name   string    `db:"name"`
	Hidden []float64 `db:"hidden" json:"-" rbi:"index"`
}

func TestMakePatch_RejectsUnaliasedAmbiguousPromotedGoName(t *testing.T) {
	db := openTempDBUint64Reflect[patchUnaliasedPromotedRec](t, "patch_unaliased_promoted_names.db")

	oldVal := &patchUnaliasedPromotedRec{
		PatchUnaliasedPromotedLeftRec:  PatchUnaliasedPromotedLeftRec{ID: "left-old"},
		PatchUnaliasedPromotedRightRec: PatchUnaliasedPromotedRightRec{ID: "right-old"},
	}
	newVal := &patchUnaliasedPromotedRec{
		PatchUnaliasedPromotedLeftRec:  PatchUnaliasedPromotedLeftRec{ID: "left-new"},
		PatchUnaliasedPromotedRightRec: PatchUnaliasedPromotedRightRec{ID: "right-new"},
	}

	patch, err := db.MakePatch(oldVal, newVal)
	if err == nil || !strings.Contains(err.Error(), "cannot be emitted by MakePatch") {
		t.Fatalf("MakePatch err=%v want unsafe default name error", err)
	}
	if len(patch) != 0 {
		t.Fatalf("MakePatch returned partial patch after error: %#v", patch)
	}

	patch, err = db.MakePatch(oldVal, newVal, PatchJSON)
	if err == nil || !strings.Contains(err.Error(), "cannot be emitted with PatchJSON") {
		t.Fatalf("MakePatch PatchJSON err=%v want unsafe json name error", err)
	}
	if len(patch) != 0 {
		t.Fatalf("MakePatch PatchJSON returned partial patch after error: %#v", patch)
	}
}

func TestMakePatch_RejectsShadowedIndexedDefaultNameAndUsesJSONAlias(t *testing.T) {
	db := openTempDBUint64Reflect[patchShadowedIndexedJSONOnlyRec](t, "patch_shadowed_indexed_json_only.db")

	oldVal := &patchShadowedIndexedJSONOnlyRec{
		PatchShadowedIndexedJSONOnlyEmbeddedRec: PatchShadowedIndexedJSONOnlyEmbeddedRec{ID: "old"},
		ID:                                      "outer",
	}
	newVal := &patchShadowedIndexedJSONOnlyRec{
		PatchShadowedIndexedJSONOnlyEmbeddedRec: PatchShadowedIndexedJSONOnlyEmbeddedRec{ID: "new"},
		ID:                                      "outer",
	}

	patch, err := db.MakePatch(oldVal, newVal)
	if err == nil || !strings.Contains(err.Error(), "cannot be emitted by MakePatch") {
		t.Fatalf("MakePatch err=%v want unsafe default name error", err)
	}
	if len(patch) != 0 {
		t.Fatalf("MakePatch returned partial patch after error: %#v", patch)
	}

	patch = mustMakePatch(t, db, oldVal, newVal, PatchJSON)
	if len(patch) != 1 || patch[0].Name != "inner" || patch[0].Value != "new" {
		t.Fatalf("PatchJSON patch=%#v want inner=new", patch)
	}
}

func TestMakePatch_RejectsShadowedMeasureDefaultNameAndUsesJSONAlias(t *testing.T) {
	db := openTempDBUint64Reflect[patchShadowedMeasureJSONOnlyRec](t, "patch_shadowed_measure_json_only.db")

	oldVal := &patchShadowedMeasureJSONOnlyRec{
		PatchShadowedMeasureJSONOnlyEmbeddedRec: PatchShadowedMeasureJSONOnlyEmbeddedRec{Score: 10},
		Score:                                   100,
	}
	newVal := &patchShadowedMeasureJSONOnlyRec{
		PatchShadowedMeasureJSONOnlyEmbeddedRec: PatchShadowedMeasureJSONOnlyEmbeddedRec{Score: 20},
		Score:                                   100,
	}

	patch, err := db.MakePatch(oldVal, newVal)
	if err == nil || !strings.Contains(err.Error(), "cannot be emitted by MakePatch") {
		t.Fatalf("MakePatch err=%v want unsafe default name error", err)
	}
	if len(patch) != 0 {
		t.Fatalf("MakePatch returned partial patch after error: %#v", patch)
	}

	patch = mustMakePatch(t, db, oldVal, newVal, PatchJSON)
	if len(patch) != 1 || patch[0].Name != "innerScore" || patch[0].Value != int64(20) {
		t.Fatalf("PatchJSON patch=%#v want innerScore=20", patch)
	}
}

func TestMakePatch_SkipsCanonicalMeasureFloatZeroTransitions(t *testing.T) {
	db := openTempDBUint64Reflect[patchMeasureFloatZeroRec](t, "patch_measure_float_zero.db")

	negZero := math.Copysign(0, -1)
	posZero := 0.0
	oldVal := &patchMeasureFloatZeroRec{Name: "same", Measure: negZero, Measure32: float32(negZero), Ptr: &negZero}
	newVal := &patchMeasureFloatZeroRec{Name: "same", Measure: posZero, Measure32: float32(posZero), Ptr: &posZero}

	patch := mustMakePatch(t, db, oldVal, newVal)
	if len(patch) != 0 {
		t.Fatalf("patch fields=%#v want none", patch)
	}
}

func TestMakePatch_EmitsCanonicalMeasureFloatValueChanges(t *testing.T) {
	db := openTempDBUint64Reflect[patchMeasureFloatZeroRec](t, "patch_measure_float_change.db")

	oldPtr := 2.5
	newPtr := 3.5
	oldVal := &patchMeasureFloatZeroRec{Name: "same", Measure: 1.25, Measure32: 1.5, Ptr: &oldPtr}
	newVal := &patchMeasureFloatZeroRec{Name: "same", Measure: 2.25, Measure32: 2.5, Ptr: &newPtr}

	patch := mustMakePatch(t, db, oldVal, newVal)
	fields := patchFieldsByName(patch)
	if len(patch) != 3 || fields["measure"] != 2.25 || fields["measure32"] != float32(2.5) {
		t.Fatalf("patch fields=%#v want changed float measure fields", patch)
	}
	gotPtr, ok := fields["ptr"].(*float64)
	if !ok || gotPtr == nil || *gotPtr != 3.5 {
		t.Fatalf("ptr patch value=%#v", fields["ptr"])
	}
}

func TestMakePatch_SkipsCanonicalFloatSliceZeroTransitions(t *testing.T) {
	db := openTempDBUint64Reflect[patchFloatSliceZeroRec](t, "patch_float_slice_zero.db")

	negZero := math.Copysign(0, -1)
	posZero := 0.0
	oldVal := &patchFloatSliceZeroRec{
		Name:     "same",
		Values64: []float64{negZero, 1.5},
		Values32: []float32{float32(negZero), 2.5},
		Named64:  patchFloat64s{negZero, 3.5},
	}
	newVal := &patchFloatSliceZeroRec{
		Name:     "same",
		Values64: []float64{posZero, 1.5},
		Values32: []float32{float32(posZero), 2.5},
		Named64:  patchFloat64s{posZero, 3.5},
	}

	patch := mustMakePatch(t, db, oldVal, newVal)
	if len(patch) != 0 {
		t.Fatalf("patch fields=%#v want none", patch)
	}
}

func TestMakePatch_SkipsCanonicalNestedFloatZeroTransitions(t *testing.T) {
	db := openTempDBUint64Reflect[patchFloatNestedZeroRec](t, "patch_float_nested_zero.db")

	negZero := math.Copysign(0, -1)
	posZero := 0.0
	oldVal := &patchFloatNestedZeroRec{
		Name:    "same",
		Payload: patchFloatNestedZeroPayload{Value64: negZero, Value32: float32(negZero), Values: [2]float64{1.5, negZero}},
	}
	newVal := &patchFloatNestedZeroRec{
		Name:    "same",
		Payload: patchFloatNestedZeroPayload{Value64: posZero, Value32: float32(posZero), Values: [2]float64{1.5, posZero}},
	}

	patch := mustMakePatch(t, db, oldVal, newVal)
	if len(patch) != 0 {
		t.Fatalf("patch fields=%#v want none", patch)
	}
}

func TestMakePatch_SkipsCanonicalNestedFloatNaNTransitions(t *testing.T) {
	db := openTempDBUint64Reflect[patchFloatNestedZeroRec](t, "patch_float32_nan_nested.db")

	oldBits := uint32(0x7f800001)
	newBits := uint32(0x7fc00001)
	oldVal := &patchFloatNestedZeroRec{
		Name:    "same",
		Payload: patchFloatNestedZeroPayload{Value64: 1, Value32: math.Float32frombits(oldBits)},
	}
	newVal := &patchFloatNestedZeroRec{
		Name:    "same",
		Payload: patchFloatNestedZeroPayload{Value64: 1, Value32: math.Float32frombits(newBits)},
	}

	patch := mustMakePatch(t, db, oldVal, newVal)
	if len(patch) != 0 {
		t.Fatalf("patch fields=%#v want none", patch)
	}
}

func TestMakePatch_SkipsCanonicalFloatPointerNaNTransitions(t *testing.T) {
	db := openTempDBUint64Reflect[patchFloatPointerFallbackRec](t, "patch_float_nan_pointer_fallback.db")

	oldNaN := math.Float64frombits(0x7ff0000000000001)
	newNaN := math.Float64frombits(0x7ff8000000000001)
	oldVal := &patchFloatPointerFallbackRec{Name: "same", Hidden: &oldNaN}
	newVal := &patchFloatPointerFallbackRec{Name: "same", Hidden: &newNaN}

	patch := mustMakePatch(t, db, oldVal, newVal, PatchJSON)
	if len(patch) != 0 {
		t.Fatalf("patch fields=%#v want none", patch)
	}
}

func TestMakePatch_SkipsCanonicalIndexedFloatNaNTransitions(t *testing.T) {
	db := openTempDBUint64Reflect[patchIndexedFloatHiddenRec](t, "patch_indexed_float_nan.db")

	oldNaN := math.Float64frombits(0x7ff0000000000001)
	newNaN := math.Float64frombits(0x7ff8000000000001)
	oldVal := &patchIndexedFloatHiddenRec{
		Name:    "same",
		Score64: oldNaN,
		Score32: math.Float32frombits(0x7f800001),
		Ptr:     &oldNaN,
	}
	newVal := &patchIndexedFloatHiddenRec{
		Name:    "same",
		Score64: newNaN,
		Score32: math.Float32frombits(0x7fc00001),
		Ptr:     &newNaN,
	}

	patch := mustMakePatch(t, db, oldVal, newVal, PatchJSON)
	if len(patch) != 0 {
		t.Fatalf("patch fields=%#v want none", patch)
	}
}

func TestMakePatch_SkipsCanonicalIndexedFloatZeroTransitions(t *testing.T) {
	db := openTempDBUint64Reflect[patchIndexedFloatHiddenRec](t, "patch_indexed_float_zero.db")

	negZero := math.Copysign(0, -1)
	posZero := 0.0
	oldVal := &patchIndexedFloatHiddenRec{Name: "same", Score64: negZero, Score32: float32(negZero), Ptr: &negZero}
	newVal := &patchIndexedFloatHiddenRec{Name: "same", Score64: posZero, Score32: float32(posZero), Ptr: &posZero}

	patch := mustMakePatch(t, db, oldVal, newVal, PatchJSON)
	if len(patch) != 0 {
		t.Fatalf("patch fields=%#v want none", patch)
	}
}

func TestMakePatch_SkipsCanonicalIndexedFloatSliceNaNTransitions(t *testing.T) {
	db := openTempDBUint64Reflect[patchIndexedFloatSliceHiddenRec](t, "patch_indexed_float_slice_nan.db")

	oldVal := &patchIndexedFloatSliceHiddenRec{
		Name:   "same",
		Hidden: []float64{math.Float64frombits(0x7ff0000000000001)},
	}
	newVal := &patchIndexedFloatSliceHiddenRec{
		Name:   "same",
		Hidden: []float64{math.Float64frombits(0x7ff8000000000001)},
	}

	patch := mustMakePatch(t, db, oldVal, newVal, PatchJSON)
	if len(patch) != 0 {
		t.Fatalf("patch fields=%#v want none", patch)
	}
}

type PatchJSONPromotedTaggedLeftRec struct {
	ID string `db:"left_id" json:"leftId"`
}

type PatchJSONPromotedTaggedRightRec struct {
	ID string `db:"right_id" json:"rightId"`
}

type patchJSONPromotedTaggedRec struct {
	PatchJSONPromotedTaggedLeftRec
	PatchJSONPromotedTaggedRightRec
}

func TestMakePatch_PatchJSON_UsesExplicitJSONTagsForPromotedGoName(t *testing.T) {
	db := openTempDBUint64Reflect[patchJSONPromotedTaggedRec](t, "patch_json_promoted_json_names.db")

	oldVal := &patchJSONPromotedTaggedRec{
		PatchJSONPromotedTaggedLeftRec:  PatchJSONPromotedTaggedLeftRec{ID: "left-old"},
		PatchJSONPromotedTaggedRightRec: PatchJSONPromotedTaggedRightRec{ID: "right-old"},
	}
	newVal := &patchJSONPromotedTaggedRec{
		PatchJSONPromotedTaggedLeftRec:  PatchJSONPromotedTaggedLeftRec{ID: "left-new"},
		PatchJSONPromotedTaggedRightRec: PatchJSONPromotedTaggedRightRec{ID: "right-new"},
	}

	patch := mustMakePatch(t, db, oldVal, newVal, PatchJSON)
	fields := patchFieldsByName(patch)
	if fields["leftId"] != "left-new" || fields["rightId"] != "right-new" {
		t.Fatalf("PatchJSON patch did not use explicit json names for promoted fields: %#v", patch)
	}

	got := applyPatchForTest(t, db, oldVal, patch)
	if got.PatchJSONPromotedTaggedLeftRec.ID != "left-new" || got.PatchJSONPromotedTaggedRightRec.ID != "right-new" {
		t.Fatalf("PatchJSON patch round-trip failed: %#v", got)
	}
}

type PatchJSONShadowedEmptyTagEmbeddedRec struct {
	ID string `db:"inner_id" json:",omitempty"`
}

type patchJSONShadowedEmptyTagRec struct {
	ID string
	PatchJSONShadowedEmptyTagEmbeddedRec
}

func TestMakePatch_PatchJSON_RejectsShadowedEmptyNameJSONTag(t *testing.T) {
	db := openTempDBUint64Reflect[patchJSONShadowedEmptyTagRec](t, "patch_json_shadowed_empty_name_tag.db")

	oldVal := &patchJSONShadowedEmptyTagRec{
		ID:                                   "outer",
		PatchJSONShadowedEmptyTagEmbeddedRec: PatchJSONShadowedEmptyTagEmbeddedRec{ID: "inner-old"},
	}
	newVal := &patchJSONShadowedEmptyTagRec{
		ID:                                   "outer",
		PatchJSONShadowedEmptyTagEmbeddedRec: PatchJSONShadowedEmptyTagEmbeddedRec{ID: "inner-new"},
	}

	patch, err := db.MakePatch(oldVal, newVal, PatchJSON)
	if err == nil || !strings.Contains(err.Error(), "cannot be emitted with PatchJSON") {
		t.Fatalf("MakePatch PatchJSON err=%v want unsafe json name error", err)
	}
	if len(patch) != 0 {
		t.Fatalf("MakePatch PatchJSON returned partial patch after error: %#v", patch)
	}
}

type patchJSONOmittedRec struct {
	Visible string
	Hidden  string `json:"-"`
}

type PatchJSONHiddenAnonymousEmbeddedRec struct {
	Value string `json:"value"`
}

type patchJSONHiddenAnonymousRec struct {
	PatchJSONHiddenAnonymousEmbeddedRec `json:"-"`
}

func TestMakePatch_PatchJSON_RejectsChangedJSONOmittedField(t *testing.T) {
	db := openTempDBUint64Reflect[patchJSONOmittedRec](t, "patch_json_omitted_field.db")

	oldVal := &patchJSONOmittedRec{Visible: "same", Hidden: "old"}
	newVal := &patchJSONOmittedRec{Visible: "same", Hidden: "new"}

	patch, err := db.MakePatch(oldVal, newVal, PatchJSON)
	if err == nil || !strings.Contains(err.Error(), "cannot be emitted with PatchJSON") {
		t.Fatalf("MakePatch PatchJSON err=%v want omitted field error", err)
	}
	if len(patch) != 0 {
		t.Fatalf("MakePatch PatchJSON returned partial patch after error: %#v", patch)
	}
}

func TestMakePatch_PatchJSON_RejectsChangedJSONHiddenAnonymousSubtree(t *testing.T) {
	db := openTempDBUint64Reflect[patchJSONHiddenAnonymousRec](t, "patch_json_hidden_anonymous.db")

	oldVal := &patchJSONHiddenAnonymousRec{
		PatchJSONHiddenAnonymousEmbeddedRec: PatchJSONHiddenAnonymousEmbeddedRec{Value: "old"},
	}
	newVal := &patchJSONHiddenAnonymousRec{
		PatchJSONHiddenAnonymousEmbeddedRec: PatchJSONHiddenAnonymousEmbeddedRec{Value: "new"},
	}

	patch, err := db.MakePatch(oldVal, newVal, PatchJSON)
	if err == nil || !strings.Contains(err.Error(), "cannot be emitted with PatchJSON") {
		t.Fatalf("MakePatch PatchJSON err=%v want hidden anonymous subtree error", err)
	}
	if len(patch) != 0 {
		t.Fatalf("MakePatch PatchJSON returned partial patch after error: %#v", patch)
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
		patch, err := db.MakePatch(oldValue, newValue)
		if err != nil {
			return err
		}
		patchByID[key] = patch
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
	patch := mustMakePatchInto(t, db, oldVal, newVal, scratch)
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

func TestPatchQueuedRequestCopiesCallerPatchItemsBeforeApply(t *testing.T) {
	db := openTempDBUint64Reflect[patchQueuedOwnershipRec](t, "patch_queued_ownership.db")

	if err := db.Set(1, &patchQueuedOwnershipRec{Name: "base", Tags: []string{"base"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	patchQueuedOwnershipDecodeStarted = make(chan struct{})
	patchQueuedOwnershipDecodeResume = make(chan struct{})
	patchQueuedOwnershipDecodeArmed.Store(true)
	defer func() {
		patchQueuedOwnershipDecodeArmed.Store(false)
		patchQueuedOwnershipDecodeStarted = nil
		patchQueuedOwnershipDecodeResume = nil
	}()

	tags := []string{"owned", "keep"}
	patch := []Field{{Name: "tags", Value: tags}}
	done := make(chan error, 1)
	go func() {
		done <- db.Patch(1, patch, PatchStrict)
	}()

	select {
	case <-patchQueuedOwnershipDecodeStarted:
	case <-time.After(time.Second):
		t.Fatal("Patch did not reach blocked decode")
	}

	patch[0].Name = "name"
	close(patchQueuedOwnershipDecodeResume)

	if err := <-done; err != nil {
		t.Fatalf("Patch: %v", err)
	}

	got, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil {
		t.Fatalf("Get(1): got nil")
	}
	defer db.ReleaseRecords(got)

	if got.Name != "base" {
		t.Fatalf("queued patch item name aliased caller mutation: name=%q", got.Name)
	}
	if !slices.Equal(got.Tags, []string{"owned", "keep"}) {
		t.Fatalf("queued patch value was not applied: tags=%v", got.Tags)
	}
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

func TestPatchStrictOption_UnknownFieldOnMissingTarget(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	err := db.Patch(999, []Field{{Name: "does_not_exist", Value: 123}}, PatchStrict)
	if err == nil || !strings.Contains(err.Error(), "cannot patch field does_not_exist") {
		t.Fatalf("Patch missing target error=%v, want strict unknown field error", err)
	}

	err = db.BatchPatch([]uint64{998, 999}, []Field{{Name: "does_not_exist", Value: 123}}, PatchStrict)
	if err == nil || !strings.Contains(err.Error(), "cannot patch field does_not_exist") {
		t.Fatalf("BatchPatch missing targets error=%v, want strict unknown field error", err)
	}
}

func TestBatchPatch_WithPatchStrict_ValidationError_IsAtomic(t *testing.T) {
	type tc struct {
		name  string
		patch []Field
	}

	cases := []tc{
		{
			name:  "unknown_field",
			patch: []Field{{Name: "does_not_exist", Value: 123}},
		},
		{
			name:  "type_mismatch",
			patch: []Field{{Name: "age", Value: "not-int"}},
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

			if err := db.Set(1, &Rec{Name: "n1", Age: 10, Meta: Meta{Country: "NL"}}); err != nil {
				t.Fatalf("Set(1): %v", err)
			}
			if err := db.Set(2, &Rec{Name: "n2", Age: 20, Meta: Meta{Country: "DE"}}); err != nil {
				t.Fatalf("Set(2): %v", err)
			}

			err := db.BatchPatch([]uint64{1, 2}, c.patch, PatchStrict)
			if err == nil {
				t.Fatalf("expected BatchPatch(..., PatchStrict) error, got nil")
			}

			v1, err := db.Get(1)
			if err != nil {
				t.Fatalf("Get(1): %v", err)
			}
			if v1 == nil || v1.Name != "n1" || v1.Age != 10 || v1.Country != "NL" {
				t.Fatalf("id=1 changed after failed BatchPatch(..., PatchStrict): %#v", v1)
			}

			v2, err := db.Get(2)
			if err != nil {
				t.Fatalf("Get(2): %v", err)
			}
			if v2 == nil || v2.Name != "n2" || v2.Age != 20 || v2.Country != "DE" {
				t.Fatalf("id=2 changed after failed BatchPatch(..., PatchStrict): %#v", v2)
			}

			ids, err := db.QueryKeys(qx.Query(qx.EQ("name", "n1")))
			if err != nil {
				t.Fatalf("QueryKeys(name=n1): %v", err)
			}
			if len(ids) != 1 || ids[0] != 1 {
				t.Fatalf("unexpected name index for n1 after failed BatchPatch(..., PatchStrict): %v", ids)
			}
		})
	}
}

func TestReflectExt_MakePatch_RoundTripPreservesTimeValue(t *testing.T) {
	db := openTempDBUint64Reflect[reflectPatchTimeRec](t, "reflect_patch_time_value.db")

	loc := time.FixedZone("MSK", 3*60*60)
	oldVal := &reflectPatchTimeRec{Name: "alice"}
	newVal := &reflectPatchTimeRec{
		Name: "alice",
		When: time.Date(2025, time.January, 2, 3, 4, 5, 678901234, loc),
	}

	patch := mustMakePatch(t, db, oldVal, newVal)
	fields := patchFieldsByName(patch)

	gotWhen, ok := fields["When"].(time.Time)
	if !ok {
		t.Fatalf("patch must contain time.Time value for When, got %#v", fields["When"])
	}
	if gotWhen != newVal.When {
		t.Fatalf("patch lost time value: got=%#v want=%#v", gotWhen, newVal.When)
	}

	applied := applyPatchForTest(t, db, oldVal, patch)
	if !applied.When.Equal(newVal.When) {
		t.Fatalf("patched record lost time value: got=%#v want=%#v", applied.When, newVal.When)
	}
}

func TestReflectExt_MakePatch_RoundTripPreservesTimeSlice(t *testing.T) {
	db := openTempDBUint64Reflect[reflectPatchTimeRec](t, "reflect_patch_time_slice.db")

	loc := time.FixedZone("CET", 1*60*60)
	oldVal := &reflectPatchTimeRec{Name: "alice"}
	newVal := &reflectPatchTimeRec{
		Name: "alice",
		Slots: []time.Time{
			time.Date(2024, time.March, 1, 10, 11, 12, 123456789, loc),
			time.Date(2026, time.July, 4, 5, 6, 7, 987654321, time.UTC),
		},
	}

	patch := mustMakePatch(t, db, oldVal, newVal)
	fields := patchFieldsByName(patch)

	gotSlots, ok := fields["Slots"].([]time.Time)
	if !ok {
		t.Fatalf("patch must contain []time.Time value for Slots, got %#v", fields["Slots"])
	}
	if !reflect.DeepEqual(gotSlots, newVal.Slots) {
		t.Fatalf("patch lost time slice contents: got=%#v want=%#v", gotSlots, newVal.Slots)
	}

	applied := applyPatchForTest(t, db, oldVal, patch)
	if len(applied.Slots) != len(newVal.Slots) {
		t.Fatalf("patched record lost time slice contents: got=%#v want=%#v", applied.Slots, newVal.Slots)
	}
	for i := range newVal.Slots {
		if !applied.Slots[i].Equal(newVal.Slots[i]) {
			t.Fatalf("patched time slot %d mismatch: got=%#v want=%#v", i, applied.Slots[i], newVal.Slots[i])
		}
	}
}

func TestReflectExt_MakePatch_RoundTripPreservesTimeMapKeys(t *testing.T) {
	db := openTempDBUint64Reflect[reflectPatchTimeRec](t, "reflect_patch_time_map.db")

	loc := time.FixedZone("EET", 2*60*60)
	oldVal := &reflectPatchTimeRec{Name: "alice"}
	newVal := &reflectPatchTimeRec{
		Name: "alice",
		Windows: map[time.Time]string{
			time.Date(2024, time.February, 10, 1, 2, 3, 4, loc):      "first",
			time.Date(2024, time.February, 11, 5, 6, 7, 8, time.UTC): "second",
		},
	}

	patch := mustMakePatch(t, db, oldVal, newVal)
	fields := patchFieldsByName(patch)

	gotWindows, ok := fields["Windows"].(map[time.Time]string)
	if !ok {
		t.Fatalf("patch must contain map[time.Time]string value for Windows, got %#v", fields["Windows"])
	}
	if !reflect.DeepEqual(gotWindows, newVal.Windows) {
		t.Fatalf("patch lost time map contents: got=%#v want=%#v", gotWindows, newVal.Windows)
	}

	applied := applyPatchForTest(t, db, oldVal, patch)
	if len(applied.Windows) != len(newVal.Windows) {
		t.Fatalf("patched record lost time map contents: got=%#v want=%#v", applied.Windows, newVal.Windows)
	}
	for wantTime, wantValue := range newVal.Windows {
		found := false
		for gotTime, gotValue := range applied.Windows {
			if gotTime.Equal(wantTime) {
				found = true
				if gotValue != wantValue {
					t.Fatalf("patched time map value mismatch at %#v: got=%q want=%q", gotTime, gotValue, wantValue)
				}
				break
			}
		}
		if !found {
			t.Fatalf("patched time map missing instant %#v in %#v", wantTime, applied.Windows)
		}
	}
}

func TestReflectExt_MakePatch_PreservesNamedSliceType(t *testing.T) {
	db := openTempDBUint64Reflect[reflectNamedSlicePatchRec](t, "reflect_patch_named_slice.db")

	oldVal := &reflectNamedSlicePatchRec{Name: "alice"}
	newVal := &reflectNamedSlicePatchRec{
		Name: "alice",
		Tags: reflectNamedTags{"go", "db"},
	}

	patch := mustMakePatch(t, db, oldVal, newVal)
	fields := patchFieldsByName(patch)

	gotTags, ok := fields["tags"].(reflectNamedTags)
	if !ok {
		t.Fatalf("patch must contain reflectNamedTags value for tags, got %#v", fields["tags"])
	}
	if !reflect.DeepEqual(gotTags, newVal.Tags) {
		t.Fatalf("patch lost named slice contents: got=%#v want=%#v", gotTags, newVal.Tags)
	}

	newVal.Tags[0] = "mutated"
	if !reflect.DeepEqual(gotTags, reflectNamedTags{"go", "db"}) {
		t.Fatalf("patch aliased named slice data: %#v", gotTags)
	}

	applied := applyPatchForTest(t, db, oldVal, patch)
	if !reflect.DeepEqual(applied.Tags, reflectNamedTags{"go", "db"}) {
		t.Fatalf("patched record lost named slice type/content: %#v", applied.Tags)
	}
}

func TestReflectExt_MakePatch_DeepCopyAliasedSliceFieldsByHeader(t *testing.T) {
	type shortTags []string
	type longTags []string
	type payload struct {
		Short shortTags
		Long  longTags
	}
	type rec struct {
		Name    string `db:"name" rbi:"index"`
		Payload payload
	}

	db := openTempDBUint64Reflect[rec](t, "reflect_patch_aliased_slice_headers.db")

	tags := []string{"go", "db", "rbi"}
	oldVal := &rec{Name: "alice"}
	newVal := &rec{
		Name: "alice",
		Payload: payload{
			Short: shortTags(tags[:2]),
			Long:  longTags(tags[:3]),
		},
	}

	patch := mustMakePatch(t, db, oldVal, newVal)
	fields := patchFieldsByName(patch)

	gotPayload, ok := fields["Payload"].(payload)
	if !ok {
		t.Fatalf("patch must contain payload value for Payload, got %#v", fields["Payload"])
	}
	if !reflect.DeepEqual(gotPayload.Short, shortTags{"go", "db"}) {
		t.Fatalf("patch lost short slice contents: %#v", gotPayload.Short)
	}
	if !reflect.DeepEqual(gotPayload.Long, longTags{"go", "db", "rbi"}) {
		t.Fatalf("patch lost long slice contents: %#v", gotPayload.Long)
	}

	tags[0] = "mutated"
	if !reflect.DeepEqual(gotPayload.Short, shortTags{"go", "db"}) || !reflect.DeepEqual(gotPayload.Long, longTags{"go", "db", "rbi"}) {
		t.Fatalf("patch aliased source slices: %#v", gotPayload)
	}

	applied := applyPatchForTest(t, db, oldVal, patch)
	if !reflect.DeepEqual(applied.Payload.Short, shortTags{"go", "db"}) {
		t.Fatalf("patched record lost short slice contents: %#v", applied.Payload.Short)
	}
	if !reflect.DeepEqual(applied.Payload.Long, longTags{"go", "db", "rbi"}) {
		t.Fatalf("patched record lost long slice contents: %#v", applied.Payload.Long)
	}
}

func TestReflectExt_MakePatch_DeepCopyUnexportedStructFields(t *testing.T) {
	type child struct {
		values []int
	}
	type payload struct {
		tags  []string
		attrs map[string]int
		child *child
	}
	type rec struct {
		Name    string  `db:"name" rbi:"index"`
		Payload payload `db:"payload"`
	}

	db := openTempDBUint64Reflect[rec](t, "reflect_patch_unexported_struct_fields.db")

	oldVal := &rec{Name: "alice"}
	newVal := &rec{
		Name: "alice",
		Payload: payload{
			tags:  []string{"go", "db"},
			attrs: map[string]int{"x": 1},
			child: &child{values: []int{1, 2}},
		},
	}

	patch := mustMakePatch(t, db, oldVal, newVal)
	fields := patchFieldsByName(patch)

	gotPayload, ok := fields["payload"].(payload)
	if !ok {
		t.Fatalf("patch must contain payload value for payload, got %#v", fields["payload"])
	}

	newVal.Payload.tags[0] = "mutated"
	newVal.Payload.attrs["x"] = 9
	newVal.Payload.child.values[0] = 7

	if !reflect.DeepEqual(gotPayload.tags, []string{"go", "db"}) {
		t.Fatalf("patch aliased unexported slice field: %#v", gotPayload.tags)
	}
	if !reflect.DeepEqual(gotPayload.attrs, map[string]int{"x": 1}) {
		t.Fatalf("patch aliased unexported map field: %#v", gotPayload.attrs)
	}
	if gotPayload.child == nil || gotPayload.child == newVal.Payload.child {
		t.Fatalf("patch did not detach unexported pointer field: %#v", gotPayload.child)
	}
	if !reflect.DeepEqual(gotPayload.child.values, []int{1, 2}) {
		t.Fatalf("patch aliased unexported pointer data: %#v", gotPayload.child)
	}
}

func TestReflectExt_MakePatch_UsesFullFieldEqualityForValueIndexer(t *testing.T) {
	db := openTempDBUint64Reflect[reflectMapVIRec](t, "reflect_patch_value_indexer_full_equality.db")

	oldVal := &reflectMapVIRec{Key: reflectMapVI{"id": "A", "note": "old"}}
	newVal := &reflectMapVIRec{Key: reflectMapVI{"id": "a", "note": "new"}}

	patch := mustMakePatch(t, db, oldVal, newVal)
	fields := patchFieldsByName(patch)

	gotKey, ok := fields["key"].(reflectMapVI)
	if !ok {
		t.Fatalf("patch must contain reflectMapVI value for key, got %#v", fields["key"])
	}
	if !reflect.DeepEqual(gotKey, reflectMapVI{"id": "a", "note": "new"}) {
		t.Fatalf("patch lost ValueIndexer-backed field contents: got=%#v", gotKey)
	}

	newVal.Key["note"] = "mutated"
	if !reflect.DeepEqual(gotKey, reflectMapVI{"id": "a", "note": "new"}) {
		t.Fatalf("patch aliased ValueIndexer-backed map: %#v", gotKey)
	}

	applied := applyPatchForTest(t, db, oldVal, patch)
	if !reflect.DeepEqual(applied.Key, reflectMapVI{"id": "a", "note": "new"}) {
		t.Fatalf("patched record lost ValueIndexer-backed field contents: %#v", applied.Key)
	}
}

func TestReflectExt_MakePatch_RoundTripDetachesStructReferences(t *testing.T) {
	db := openTempDBUint64Reflect[reflectPatchNestedRec](t, "reflect_patch_nested_struct.db")

	oldVal := &reflectPatchNestedRec{Name: "alice"}
	newVal := &reflectPatchNestedRec{
		Name: "alice",
		Nested: reflectPatchNested{
			Name:  "node",
			Tags:  []string{"before"},
			Attrs: map[string]int{"x": 1},
			Child: &reflectPatchNestedChild{
				Label:  "child",
				Values: []int{1, 2},
			},
		},
	}

	patch := mustMakePatch(t, db, oldVal, newVal)
	newVal.Nested.Tags[0] = "after"
	newVal.Nested.Attrs["x"] = 9
	newVal.Nested.Child.Label = "mutated"
	newVal.Nested.Child.Values[0] = 7

	fields := patchFieldsByName(patch)
	gotNested, ok := fields["Nested"].(reflectPatchNested)
	if !ok {
		t.Fatalf("patch must contain reflectPatchNested for Nested, got %#v", fields["Nested"])
	}
	if !reflect.DeepEqual(gotNested.Tags, []string{"before"}) {
		t.Fatalf("patch aliased struct slice field: %#v", gotNested.Tags)
	}
	if !reflect.DeepEqual(gotNested.Attrs, map[string]int{"x": 1}) {
		t.Fatalf("patch aliased struct map field: %#v", gotNested.Attrs)
	}
	if gotNested.Child == nil || gotNested.Child == newVal.Nested.Child {
		t.Fatalf("patch did not detach nested child pointer: %#v", gotNested.Child)
	}
	if gotNested.Child.Label != "child" || !reflect.DeepEqual(gotNested.Child.Values, []int{1, 2}) {
		t.Fatalf("patch aliased nested child data: %#v", gotNested.Child)
	}

	applied := applyPatchForTest(t, db, oldVal, patch)
	if !reflect.DeepEqual(applied.Nested.Tags, []string{"before"}) {
		t.Fatalf("patched record aliased struct slice field: %#v", applied.Nested.Tags)
	}
	if !reflect.DeepEqual(applied.Nested.Attrs, map[string]int{"x": 1}) {
		t.Fatalf("patched record aliased struct map field: %#v", applied.Nested.Attrs)
	}
	if applied.Nested.Child == nil || applied.Nested.Child.Label != "child" || !reflect.DeepEqual(applied.Nested.Child.Values, []int{1, 2}) {
		t.Fatalf("patched record aliased nested child data: %#v", applied.Nested.Child)
	}
}

func TestReflectExt_MakePatch_RoundTripDetachesPointerStructReferences(t *testing.T) {
	db := openTempDBUint64Reflect[reflectPatchNestedRec](t, "reflect_patch_nested_ptr.db")

	oldVal := &reflectPatchNestedRec{Name: "alice"}
	newVal := &reflectPatchNestedRec{
		Name: "alice",
		NestedPtr: &reflectPatchNested{
			Name:  "node",
			Tags:  []string{"before"},
			Attrs: map[string]int{"x": 1},
			Child: &reflectPatchNestedChild{
				Label:  "child",
				Values: []int{1, 2},
			},
		},
	}

	patch := mustMakePatch(t, db, oldVal, newVal)
	newVal.NestedPtr.Tags[0] = "after"
	newVal.NestedPtr.Attrs["x"] = 9
	newVal.NestedPtr.Child.Label = "mutated"
	newVal.NestedPtr.Child.Values[0] = 7

	fields := patchFieldsByName(patch)
	gotNested, ok := fields["NestedPtr"].(*reflectPatchNested)
	if !ok {
		t.Fatalf("patch must contain *reflectPatchNested for NestedPtr, got %#v", fields["NestedPtr"])
	}
	if gotNested == nil || gotNested == newVal.NestedPtr {
		t.Fatalf("patch did not detach struct pointer: %#v", gotNested)
	}
	if !reflect.DeepEqual(gotNested.Tags, []string{"before"}) {
		t.Fatalf("patch aliased pointer struct slice field: %#v", gotNested.Tags)
	}
	if !reflect.DeepEqual(gotNested.Attrs, map[string]int{"x": 1}) {
		t.Fatalf("patch aliased pointer struct map field: %#v", gotNested.Attrs)
	}
	if gotNested.Child == nil || gotNested.Child == newVal.NestedPtr.Child {
		t.Fatalf("patch did not detach pointer child: %#v", gotNested.Child)
	}
	if gotNested.Child.Label != "child" || !reflect.DeepEqual(gotNested.Child.Values, []int{1, 2}) {
		t.Fatalf("patch aliased pointer child data: %#v", gotNested.Child)
	}

	applied := applyPatchForTest(t, db, oldVal, patch)
	if applied.NestedPtr == nil || !reflect.DeepEqual(applied.NestedPtr.Tags, []string{"before"}) {
		t.Fatalf("patched record aliased pointer struct slice field: %#v", applied.NestedPtr)
	}
	if !reflect.DeepEqual(applied.NestedPtr.Attrs, map[string]int{"x": 1}) {
		t.Fatalf("patched record aliased pointer struct map field: %#v", applied.NestedPtr.Attrs)
	}
	if applied.NestedPtr.Child == nil || applied.NestedPtr.Child.Label != "child" || !reflect.DeepEqual(applied.NestedPtr.Child.Values, []int{1, 2}) {
		t.Fatalf("patched record aliased pointer child data: %#v", applied.NestedPtr.Child)
	}
}

func TestReflectExt_MakePatch_RoundTripDetachesSliceStructReferences(t *testing.T) {
	db := openTempDBUint64Reflect[reflectPatchNestedSliceRec](t, "reflect_patch_nested_slice.db")

	oldVal := &reflectPatchNestedSliceRec{Name: "alice"}
	newVal := &reflectPatchNestedSliceRec{
		Name: "alice",
		Items: []reflectPatchNestedRec{
			{
				Name: "first",
				Nested: reflectPatchNested{
					Name:  "node",
					Tags:  []string{"before"},
					Attrs: map[string]int{"x": 1},
					Child: &reflectPatchNestedChild{
						Label:  "child",
						Values: []int{1, 2},
					},
				},
			},
		},
	}

	patch := mustMakePatch(t, db, oldVal, newVal)
	newVal.Items[0].Nested.Tags[0] = "after"
	newVal.Items[0].Nested.Attrs["x"] = 9
	newVal.Items[0].Nested.Child.Label = "mutated"
	newVal.Items[0].Nested.Child.Values[0] = 7

	fields := patchFieldsByName(patch)
	gotItems, ok := fields["Items"].([]reflectPatchNestedRec)
	if !ok {
		t.Fatalf("patch must contain []reflectPatchNestedRec for Items, got %#v", fields["Items"])
	}
	if len(gotItems) != 1 {
		t.Fatalf("unexpected patch Items length: %#v", gotItems)
	}
	if !reflect.DeepEqual(gotItems[0].Nested.Tags, []string{"before"}) {
		t.Fatalf("patch aliased slice element nested slice field: %#v", gotItems[0].Nested.Tags)
	}
	if !reflect.DeepEqual(gotItems[0].Nested.Attrs, map[string]int{"x": 1}) {
		t.Fatalf("patch aliased slice element nested map field: %#v", gotItems[0].Nested.Attrs)
	}
	if gotItems[0].Nested.Child == nil || gotItems[0].Nested.Child == newVal.Items[0].Nested.Child {
		t.Fatalf("patch did not detach slice element nested child: %#v", gotItems[0].Nested.Child)
	}
	if gotItems[0].Nested.Child.Label != "child" || !reflect.DeepEqual(gotItems[0].Nested.Child.Values, []int{1, 2}) {
		t.Fatalf("patch aliased slice element nested child data: %#v", gotItems[0].Nested.Child)
	}

	applied := applyPatchForTest(t, db, oldVal, patch)
	if len(applied.Items) != 1 {
		t.Fatalf("patched record lost slice contents: %#v", applied.Items)
	}
	if !reflect.DeepEqual(applied.Items[0].Nested.Tags, []string{"before"}) {
		t.Fatalf("patched record aliased slice element nested slice field: %#v", applied.Items[0].Nested.Tags)
	}
	if !reflect.DeepEqual(applied.Items[0].Nested.Attrs, map[string]int{"x": 1}) {
		t.Fatalf("patched record aliased slice element nested map field: %#v", applied.Items[0].Nested.Attrs)
	}
	if applied.Items[0].Nested.Child == nil || applied.Items[0].Nested.Child.Label != "child" || !reflect.DeepEqual(applied.Items[0].Nested.Child.Values, []int{1, 2}) {
		t.Fatalf("patched record aliased slice element nested child data: %#v", applied.Items[0].Nested.Child)
	}
}

func TestReflectExt_PatchRejectsIntOverflowIntoInt8(t *testing.T) {
	db := openTempDBUint64Reflect[reflectNumericPatchRec](t, "reflect_patch_i8_overflow.db")

	if err := db.Set(1, &reflectNumericPatchRec{Name: "alice", I8: 7}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	value := int64(300)
	err := db.Patch(1, []Field{{Name: "I8", Value: value}}, PatchStrict)
	if err == nil {
		t.Fatalf("expected int64->int8 overflow error")
	}

	got, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil || got.I8 != 7 {
		t.Fatalf("record changed after rejected patch: %#v", got)
	}
}

func TestReflectExt_PatchRejectsUintOverflowIntoUint8(t *testing.T) {
	db := openTempDBUint64Reflect[reflectNumericPatchRec](t, "reflect_patch_u8_overflow.db")

	if err := db.Set(1, &reflectNumericPatchRec{Name: "alice", U8: 9}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	value := uint64(300)
	err := db.Patch(1, []Field{{Name: "U8", Value: value}}, PatchStrict)
	if err == nil {
		t.Fatalf("expected uint64->uint8 overflow error")
	}

	got, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil || got.U8 != 9 {
		t.Fatalf("record changed after rejected patch: %#v", got)
	}
}

func TestReflectExt_PatchRejectsNegativeIntIntoUint64(t *testing.T) {
	db := openTempDBUint64Reflect[reflectNumericPatchRec](t, "reflect_patch_negative_uint.db")

	if err := db.Set(1, &reflectNumericPatchRec{Name: "alice", U64: 11}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	value := -1
	err := db.Patch(1, []Field{{Name: "U64", Value: value}}, PatchStrict)
	if err == nil {
		t.Fatalf("expected negative int->uint64 conversion error")
	}

	got, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil || got.U64 != 11 {
		t.Fatalf("record changed after rejected patch: %#v", got)
	}
}

func TestReflectExt_PatchRejectsInfIntoInt64(t *testing.T) {
	db := openTempDBUint64Reflect[reflectNumericPatchRec](t, "reflect_patch_inf_i64.db")

	if err := db.Set(1, &reflectNumericPatchRec{Name: "alice", I64: 17}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	value := math.Inf(1)
	err := db.Patch(1, []Field{{Name: "I64", Value: value}}, PatchStrict)
	if err == nil {
		t.Fatalf("expected +Inf->int64 conversion error")
	}

	got, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil || got.I64 != 17 {
		t.Fatalf("record changed after rejected patch: %#v", got)
	}
}

func TestReflectExt_PatchRejectsInfIntoUint64(t *testing.T) {
	db := openTempDBUint64Reflect[reflectNumericPatchRec](t, "reflect_patch_inf_u64.db")

	if err := db.Set(1, &reflectNumericPatchRec{Name: "alice", U64: 19}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	value := math.Inf(1)
	err := db.Patch(1, []Field{{Name: "U64", Value: value}}, PatchStrict)
	if err == nil {
		t.Fatalf("expected +Inf->uint64 conversion error")
	}

	got, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil || got.U64 != 19 {
		t.Fatalf("record changed after rejected patch: %#v", got)
	}
}

func TestReflectExt_PatchRejectsSliceElementOverflow(t *testing.T) {
	db := openTempDBUint64Reflect[reflectNumericPatchRec](t, "reflect_patch_slice_overflow.db")

	if err := db.Set(1, &reflectNumericPatchRec{Name: "alice", Bytes: []uint8{1, 2}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	err := db.Patch(1, []Field{{Name: "Bytes", Value: []int{1, 300}}}, PatchStrict)
	if err == nil {
		t.Fatalf("expected []int->[]uint8 overflow error")
	}

	got, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil || !reflect.DeepEqual(got.Bytes, []uint8{1, 2}) {
		t.Fatalf("record changed after rejected patch: %#v", got)
	}
}
