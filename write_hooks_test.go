package rbi

import (
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"unsafe"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/internal/wexec"
	"github.com/vmihailenco/msgpack/v5"
	"go.etcd.io/bbolt"
)

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
		var keyBuf [8]byte
		raw := managed.Get(keycodec.UserKeyBytesWithBuf(key, db.strKey, &keyBuf))
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

	err := db.Set("ghost", &Product{SKU: "ghost", Price: 11}, BeforeCommit(func(tx *bbolt.Tx, key string, oldValue, newValue *Product) error {
		if oldValue != nil || newValue == nil || key != "ghost" {
			return fmt.Errorf("unexpected callback state: key=%q old=%#v new=%#v", key, oldValue, newValue)
		}
		audit, err := tx.CreateBucketIfNotExists([]byte(auditBucket))
		if err != nil {
			return fmt.Errorf("create audit bucket: %w", err)
		}
		if err = audit.Put([]byte(auditKey), []byte(newValue.SKU)); err != nil {
			return err
		}
		return injected
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
		var keyBuf [8]byte
		if raw := managed.Get(keycodec.UserKeyBytesWithBuf(key, db.strKey, &keyBuf)); raw != nil {
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

func TestBeforeCommit_SetRollsBackAndKeepsState(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchMax: 1,
	})

	err := db.Set(1, &Rec{Name: "alice", Age: 30}, BeforeCommit(func(*bbolt.Tx, uint64, *Rec, *Rec) error {
		return fmt.Errorf("before commit set")
	}))
	if err == nil || !strings.Contains(err.Error(), "before commit set") {
		t.Fatalf("expected before commit error, got: %v", err)
	}

	v, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v != nil {
		t.Fatalf("expected no value after failed commit, got: %#v", v)
	}

	if cnt, err := db.Count(); err != nil {
		t.Fatalf("Count: %v", err)
	} else if cnt != 0 {
		t.Fatalf("expected Count=0 after failed commit, got %d", cnt)
	}
}

func TestPublishAfterCommitFailureBreaksDB(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	publishField := reflect.ValueOf(db.batcher).Elem().FieldByName("indexPublishOps")
	publishOps := (*wexec.IndexPublishOps)(unsafe.Pointer(publishField.UnsafeAddr()))
	oldPublish := publishOps.PublishCommitted
	publishOps.PublishCommitted = func(seq uint64, op string, _ *snapshot.View) error {
		return db.index.PublishCommittedView(&db.broken, db.logger, ErrBroken, seq, op, nil)
	}
	defer func() {
		publishOps.PublishCommitted = oldPublish
	}()

	err := db.Set(1, &Rec{Name: "alice", Age: 30})
	if !errors.Is(err, ErrBroken) {
		t.Fatalf("expected Set to fail with ErrBroken after publish failure, got: %v", err)
	}

	var got *Rec
	if viewErr := db.bolt.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(db.bucket)
		if bucket == nil {
			return fmt.Errorf("bucket does not exist")
		}
		var keyBuf [8]byte
		raw := bucket.Get(keycodec.UserKeyBytesWithBuf(uint64(1), db.strKey, &keyBuf))
		if raw == nil {
			return fmt.Errorf("record was not committed")
		}
		var decodeErr error
		got, decodeErr = db.decode(raw)
		return decodeErr
	}); viewErr != nil {
		t.Fatalf("bolt view after publish failure: %v", viewErr)
	}
	if got == nil || got.Name != "alice" || got.Age != 30 {
		t.Fatalf("unexpected committed value after publish failure: %#v", got)
	}

	if err = db.Set(2, &Rec{Name: "bob", Age: 20}); !errors.Is(err, ErrBroken) {
		t.Fatalf("expected DB to reject subsequent writes with ErrBroken, got: %v", err)
	}
	if _, err := db.Query(qx.Query(qx.EQ("name", "alice"))); !errors.Is(err, ErrBroken) {
		t.Fatalf("expected Query to fail with ErrBroken after publish failure, got: %v", err)
	}

	if err := db.Close(); !errors.Is(err, ErrBroken) {
		t.Fatalf("expected Close to return ErrBroken for broken db, got: %v", err)
	}
}

func TestBeforeCommit_PatchAndDeleteRollbackAndKeepState(t *testing.T) {
	t.Run("patch", func(t *testing.T) {
		db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
		if err := db.Set(1, &Rec{Name: "alice", Age: 30, Meta: Meta{Country: "NL"}}); err != nil {
			t.Fatalf("Set(1): %v", err)
		}

		err := db.Patch(1, []Field{{Name: "age", Value: 99}}, BeforeCommit(func(*bbolt.Tx, uint64, *Rec, *Rec) error {
			return fmt.Errorf("before commit patch")
		}))
		if err == nil || !strings.Contains(err.Error(), "before commit patch") {
			t.Fatalf("expected before commit patch error, got: %v", err)
		}

		assertNoFutureSnapshotRefs(t, db)
		v, err := db.Get(1)
		if err != nil {
			t.Fatalf("Get(1): %v", err)
		}
		if v == nil || v.Age != 30 {
			t.Fatalf("expected record unchanged after failed patch commit, got %#v", v)
		}
	})

	t.Run("delete", func(t *testing.T) {
		db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
		if err := db.Set(1, &Rec{Name: "alice", Age: 30, Meta: Meta{Country: "NL"}}); err != nil {
			t.Fatalf("Set(1): %v", err)
		}

		err := db.Delete(1, BeforeCommit(func(*bbolt.Tx, uint64, *Rec, *Rec) error {
			return fmt.Errorf("before commit delete")
		}))
		if err == nil || !strings.Contains(err.Error(), "before commit delete") {
			t.Fatalf("expected before commit delete error, got: %v", err)
		}

		assertNoFutureSnapshotRefs(t, db)
		v, err := db.Get(1)
		if err != nil {
			t.Fatalf("Get(1): %v", err)
		}
		if v == nil {
			t.Fatalf("expected record to remain after failed delete commit")
		}
	})
}

func TestBeforeCommit_MultiWritePathsRollbackAndKeepState(t *testing.T) {
	type tc struct {
		name   string
		setup  func(t *testing.T, db *DB[uint64, Rec])
		run    func(db *DB[uint64, Rec], opt ExecOption[uint64, Rec]) error
		verify func(t *testing.T, db *DB[uint64, Rec])
	}

	cases := []tc{
		{
			name: "batch_set",
			run: func(db *DB[uint64, Rec], opt ExecOption[uint64, Rec]) error {
				return db.BatchSet(
					[]uint64{1, 2},
					[]*Rec{
						{Name: "a", Age: 10, Meta: Meta{Country: "NL"}},
						{Name: "b", Age: 11, Meta: Meta{Country: "DE"}},
					},
					opt,
				)
			},
			verify: func(t *testing.T, db *DB[uint64, Rec]) {
				t.Helper()
				v1, err := db.Get(1)
				if err != nil {
					t.Fatalf("Get(1): %v", err)
				}
				v2, err := db.Get(2)
				if err != nil {
					t.Fatalf("Get(2): %v", err)
				}
				if v1 != nil || v2 != nil {
					t.Fatalf("expected no committed values after failed set_many, got v1=%#v v2=%#v", v1, v2)
				}
			},
		},
		{
			name: "batch_patch",
			setup: func(t *testing.T, db *DB[uint64, Rec]) {
				t.Helper()
				if err := db.Set(1, &Rec{Name: "a", Age: 10, Meta: Meta{Country: "NL"}}); err != nil {
					t.Fatalf("Set(1): %v", err)
				}
				if err := db.Set(2, &Rec{Name: "b", Age: 11, Meta: Meta{Country: "DE"}}); err != nil {
					t.Fatalf("Set(2): %v", err)
				}
			},
			run: func(db *DB[uint64, Rec], opt ExecOption[uint64, Rec]) error {
				return db.BatchPatch([]uint64{1, 2}, []Field{{Name: "age", Value: 99}}, opt)
			},
			verify: func(t *testing.T, db *DB[uint64, Rec]) {
				t.Helper()
				v1, err := db.Get(1)
				if err != nil {
					t.Fatalf("Get(1): %v", err)
				}
				v2, err := db.Get(2)
				if err != nil {
					t.Fatalf("Get(2): %v", err)
				}
				if v1 == nil || v2 == nil {
					t.Fatalf("expected records to remain after failed patch_many, got v1=%#v v2=%#v", v1, v2)
				}
				if v1.Age != 10 || v2.Age != 11 {
					t.Fatalf("expected ages unchanged after failed patch_many, got v1=%d v2=%d", v1.Age, v2.Age)
				}
			},
		},
		{
			name: "batch_delete",
			setup: func(t *testing.T, db *DB[uint64, Rec]) {
				t.Helper()
				if err := db.Set(1, &Rec{Name: "a", Age: 10, Meta: Meta{Country: "NL"}}); err != nil {
					t.Fatalf("Set(1): %v", err)
				}
				if err := db.Set(2, &Rec{Name: "b", Age: 11, Meta: Meta{Country: "DE"}}); err != nil {
					t.Fatalf("Set(2): %v", err)
				}
			},
			run: func(db *DB[uint64, Rec], opt ExecOption[uint64, Rec]) error {
				return db.BatchDelete([]uint64{1, 2}, opt)
			},
			verify: func(t *testing.T, db *DB[uint64, Rec]) {
				t.Helper()
				v1, err := db.Get(1)
				if err != nil {
					t.Fatalf("Get(1): %v", err)
				}
				v2, err := db.Get(2)
				if err != nil {
					t.Fatalf("Get(2): %v", err)
				}
				if v1 == nil || v2 == nil {
					t.Fatalf("expected records to remain after failed delete_many, got v1=%#v v2=%#v", v1, v2)
				}
			},
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
			if c.setup != nil {
				c.setup(t, db)
			}

			wantErr := "before commit " + c.name
			err := c.run(db, BeforeCommit(func(*bbolt.Tx, uint64, *Rec, *Rec) error {
				return errors.New(wantErr)
			}))
			if err == nil || !strings.Contains(err.Error(), wantErr) {
				t.Fatalf("expected before commit error for %s, got: %v", c.name, err)
			}

			assertNoFutureSnapshotRefs(t, db)

			c.verify(t, db)
		})
	}
}

func TestNew_DefaultExecOptions_ApplyToWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "default_before_commit.db")

	raw, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}
	defer func() { _ = raw.Close() }()

	var calls []string
	db, err := New[uint64, Rec](
		raw,
		testOptions(Options{AutoBatchMax: 1}),
		PatchStrict,
		BeforeProcess(func(_ uint64, value *Rec) error {
			value.Name = "pre-" + value.Name
			value.Country = "US"
			return nil
		}),
		BeforeStore(func(_ uint64, _ *Rec, newValue *Rec) error {
			newValue.Age++
			return nil
		}),
		BeforeCommit(func(_ *bbolt.Tx, _ uint64, _, _ *Rec) error {
			calls = append(calls, "default")
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = db.Close() }()

	err = db.Set(
		1,
		&Rec{Name: "alice", Age: 30},
		BeforeCommit(func(_ *bbolt.Tx, _ uint64, _, _ *Rec) error {
			calls = append(calls, "call")
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("Set with default exec options: %v", err)
	}

	if !slices.Equal(calls, []string{"default", "call"}) {
		t.Fatalf("unexpected callback order: %v", calls)
	}

	v, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v == nil || v.Name != "pre-alice" || v.Age != 31 {
		t.Fatalf("unexpected stored value: %#v", v)
	}

	err = db.Patch(1, []Field{{Name: "country", Value: "CA"}})
	if err != nil {
		t.Fatalf("Patch with default BeforeProcess: %v", err)
	}

	v, err = db.Get(1)
	if err != nil {
		t.Fatalf("Get(1) after Patch: %v", err)
	}
	if v == nil || v.Country != "US" {
		t.Fatalf("expected Patch to inherit default BeforeProcess, got %#v", v)
	}

	err = db.Patch(1, []Field{{Name: "does_not_exist", Value: 1}})
	if err == nil {
		t.Fatal("expected Patch to inherit default PatchStrict option")
	}
}

func TestBatchSet_DuplicateIDs_BeforeCommit_SeesPerStepTxState(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	if err := db.Set(1, &Rec{Name: "seed", Age: 21}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	var seen []string
	err := db.BatchSet(
		[]uint64{1, 1},
		[]*Rec{
			{Name: "first", Age: 30},
			{Name: "second", Age: 40},
		},
		BeforeCommit(func(tx *bbolt.Tx, key uint64, oldValue, newValue *Rec) error {
			var keyBuf [8]byte
			raw := tx.Bucket(db.bucket).Get(keycodec.UserKeyBytesWithBuf(key, db.strKey, &keyBuf))
			if raw == nil {
				return fmt.Errorf("missing value for id=%d inside BeforeCommit", key)
			}
			current, err := db.decode(raw)
			if err != nil {
				return fmt.Errorf("decode current tx value: %w", err)
			}
			if current.Name != newValue.Name || current.Age != newValue.Age {
				return fmt.Errorf("tx value mismatch: got=%#v want=%#v", current, newValue)
			}
			seen = append(seen, current.Name)
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("BatchSet duplicate ids with BeforeCommit: %v", err)
	}

	if !slices.Equal(seen, []string{"first", "second"}) {
		t.Fatalf("unexpected BeforeCommit observation order: %v", seen)
	}
}

func TestBatchPatch_DuplicateIDs_BeforeCommit_SeesPerStepTxState(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	if err := db.Set(1, &Rec{Name: "seed", Age: 21}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	call := 0
	var seen []string
	err := db.BatchPatch(
		[]uint64{1, 1},
		[]Field{{Name: "age", Value: 99}},
		BeforeProcess(func(_ uint64, value *Rec) error {
			call++
			value.Name = fmt.Sprintf("patched-%d", call)
			return nil
		}),
		BeforeCommit(func(tx *bbolt.Tx, key uint64, oldValue, newValue *Rec) error {
			var keyBuf [8]byte
			raw := tx.Bucket(db.bucket).Get(keycodec.UserKeyBytesWithBuf(key, db.strKey, &keyBuf))
			if raw == nil {
				return fmt.Errorf("missing value for id=%d inside BeforeCommit", key)
			}
			current, err := db.decode(raw)
			if err != nil {
				return fmt.Errorf("decode current tx value: %w", err)
			}
			if current.Name != newValue.Name || current.Age != newValue.Age {
				return fmt.Errorf("tx value mismatch: got=%#v want=%#v", current, newValue)
			}
			seen = append(seen, current.Name)
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("BatchPatch duplicate ids with BeforeCommit: %v", err)
	}

	if !slices.Equal(seen, []string{"patched-1", "patched-2"}) {
		t.Fatalf("unexpected BeforeCommit observation order: %v", seen)
	}
}

type beforeStoreCloneRec struct {
	Name  string `db:"name"  rbi:"index"`
	Ready bool   `db:"ready" rbi:"index"`
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

func TestBatchSet_BuildErrorDoesNotAffectFollowingWrite(t *testing.T) {
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

	calls := 0
	if err := db.Set(3, &beforeStoreCloneRec{Name: "hooked", Ready: true}, NoBatch[uint64, beforeStoreCloneRec], BeforeCommit(func(_ *bbolt.Tx, _ uint64, _ *beforeStoreCloneRec, _ *beforeStoreCloneRec) error {
		calls++
		return nil
	})); err != nil {
		t.Fatalf("Set after failed BatchSet: %v", err)
	}
	if calls != 1 {
		t.Fatalf("beforeCommit calls after failed BatchSet = %d, want 1", calls)
	}
	got, err := db.Get(3)
	if err != nil {
		t.Fatalf("Get(3): %v", err)
	}
	if got == nil || got.Name != "hooked" || !got.Ready {
		t.Fatalf("following write stored %#v", got)
	}
}
