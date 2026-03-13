package rbi

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"github.com/vmihailenco/msgpack/v5"
	"go.etcd.io/bbolt"
)

type beforeStoreCloneRec struct {
	Name  string `db:"name"`
	Ready bool   `db:"ready"`
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

	if _, ok := got["Name"]; !ok {
		t.Fatalf("expected patch to include %q, got %#v", "Name", patch)
	}
	if _, ok := got["Tags"]; !ok {
		t.Fatalf("expected patch to include %q, got %#v", "Tags", patch)
	}
	if _, ok := got["Age"]; ok {
		t.Fatalf("did not expect patch to include unchanged field %q, got %#v", "Age", patch)
	}

	if v, _ := got["Name"].(string); v != "bob" {
		t.Fatalf("expected patched name %q, got %#v", "bob", got["Name"])
	}

	gotTags, _ := got["Tags"].([]string)
	if len(gotTags) != 2 || gotTags[0] != "x" || gotTags[1] != "y" {
		t.Fatalf("expected patched tags [x y], got %#v", gotTags)
	}

	origTags[0] = "MUTATED"

	gotTags2, _ := got["Tags"].([]string)
	if len(gotTags2) != 2 || gotTags2[0] != "x" || gotTags2[1] != "y" {
		t.Fatalf("expected deep-copied tags [x y], got %#v", gotTags2)
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

	if _, ok = m1["Name"]; !ok {
		t.Fatalf("id=1: expected patch to include %q, got %#v", "Name", p1)
	}
	if _, ok = m1["Tags"]; !ok {
		t.Fatalf("id=1: expected patch to include %q, got %#v", "Tags", p1)
	}
	if _, ok = m1["Age"]; ok {
		t.Fatalf("id=1: did not expect patch to include unchanged field %q, got %#v", "Age", p1)
	}

	if v, _ := m1["Name"].(string); v != "bob" {
		t.Fatalf("id=1: expected patched name %q, got %#v", "bob", m1["Name"])
	}
	tags1, _ := m1["Tags"].([]string)
	if len(tags1) != 2 || tags1[0] != "x" || tags1[1] != "y" {
		t.Fatalf("id=1: expected patched tags [x y], got %#v", tags1)
	}

	p2, ok := patchByID[2]
	if !ok {
		t.Fatalf("expected patch for id=2, got keys: %#v", keysOfMap(patchByID))
	}
	m2 := toMap(p2)

	if _, ok = m2["Age"]; !ok {
		t.Fatalf("id=2: expected patch to include %q, got %#v", "Age", p2)
	}
	if _, ok = m2["Tags"]; !ok {
		t.Fatalf("id=2: expected patch to include %q, got %#v", "Tags", p2)
	}
	if _, ok = m2["Name"]; ok {
		t.Fatalf("id=2: did not expect patch to include unchanged field %q, got %#v", "Name", p2)
	}

	if v, _ := m2["Age"].(int); v != 21 {
		t.Fatalf("id=2: expected patched age %d, got %#v", 21, m2["Age"])
	}
	tags2, _ := m2["Tags"].([]string)
	if len(tags2) != 2 || tags2[0] != "p" || tags2[1] != "q" {
		t.Fatalf("id=2: expected patched tags [p q], got %#v", tags2)
	}

	origTags1[0] = "MUTATED1"
	origTags2[0] = "MUTATED2"

	tags1b, _ := m1["Tags"].([]string)
	if len(tags1b) != 2 || tags1b[0] != "x" || tags1b[1] != "y" {
		t.Fatalf("id=1: expected deep-copied tags [x y], got %#v", tags1b)
	}
	tags2b, _ := m2["Tags"].([]string)
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

	if cnt, err := db.Count(nil); err != nil || cnt != 2 {
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

			_, _ = db.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpNOOP}})
			_, _ = db.Count(nil)
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

	if cnt, err := db.Count(nil); err != nil {
		t.Fatalf("Count(after): %v", err)
	} else if cnt != 0 {
		t.Fatalf("expected 0 records after truncate, got %d", cnt)
	}
	if ids, err := db.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpNOOP}}); err != nil {
		t.Fatalf("QueryKeys(NOOP after): %v", err)
	} else if len(ids) != 0 {
		t.Fatalf("expected no keys after truncate, got %v", ids)
	}

	st := db.Stats()
	if st.Index.KeyCount != 0 {
		t.Fatalf("expected Stats.Index.KeyCount=0 after truncate, got %d", st.Index.KeyCount)
	}
	var zero uint64
	if st.Index.LastKey != zero {
		t.Fatalf("expected Stats.Index.LastKey=%v after truncate, got %v", zero, st.Index.LastKey)
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
	enc := getEncodeBuf()
	if err := db.encode(newVal, enc); err != nil {
		releaseEncodeBuf(enc)
		t.Fatalf("encode payload: %v", err)
	}
	payload := append([]byte(nil), enc.Bytes()...)
	releaseEncodeBuf(enc)

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
