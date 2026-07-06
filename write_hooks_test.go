package rbi

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/rbierrors"
	"go.etcd.io/bbolt"
)

func TestOnChange_SetPatchDeleteContract(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	var events []string
	hook := OnChange(func(tx *Tx, key uint64, oldValue, newValue *Rec) error {
		if key != 1 {
			t.Fatalf("OnChange key=%d want 1", key)
		}

		oldName := "<nil>"
		newName := "<nil>"
		if oldValue != nil {
			oldName = oldValue.Name
		}
		if newValue != nil {
			newName = newValue.Name
			newValue.Name += "-changed"
		}
		events = append(events, oldName+"->"+newName)
		return nil
	})

	if err := writeSet(c, 1, &Rec{Name: "alice", Age: 10}, hook); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := readGet(c, 1)
	if err != nil {
		t.Fatalf("Get after Set: %v", err)
	}
	if got == nil || got.Name != "alice-changed" || got.Age != 10 {
		t.Fatalf("Set stored %#v", got)
	}

	if err = writePatch(c, 1, []Field{{Name: "age", Value: 20}}, hook); err != nil {
		t.Fatalf("Patch: %v", err)
	}
	got, err = readGet(c, 1)
	if err != nil {
		t.Fatalf("Get after Patch: %v", err)
	}
	if got == nil || got.Name != "alice-changed-changed" || got.Age != 20 {
		t.Fatalf("Patch stored %#v", got)
	}

	if err = writeDelete(c, 1, hook); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	got, err = readGet(c, 1)
	if err != nil {
		t.Fatalf("Get after Delete: %v", err)
	}
	if got != nil {
		t.Fatalf("Delete left %#v", got)
	}

	if err = writePatch(c, 1, []Field{{Name: "age", Value: 30}}, hook); err != nil {
		t.Fatalf("missing Patch: %v", err)
	}
	if err = writeDelete(c, 1, hook); err != nil {
		t.Fatalf("missing Delete: %v", err)
	}

	want := []string{
		"<nil>->alice",
		"alice-changed->alice-changed",
		"alice-changed-changed-><nil>",
	}
	if !slices.Equal(events, want) {
		t.Fatalf("OnChange events=%v want %v", events, want)
	}
}

func TestOnChangeLifecyclePanicsReturnErrors(t *testing.T) {
	tests := []struct {
		name string
		call func(owner, hook *Tx)
	}{
		{
			name: "hook commit",
			call: func(_, hook *Tx) {
				_ = hook.Commit()
			},
		},
		{
			name: "hook close",
			call: func(_, hook *Tx) {
				hook.Close()
			},
		},
		{
			name: "hook release",
			call: func(_, hook *Tx) {
				hook.Release()
			},
		},
		{
			name: "owner commit",
			call: func(owner, _ *Tx) {
				_ = owner.Commit()
			},
		},
		{
			name: "owner close",
			call: func(owner, _ *Tx) {
				owner.Close()
			},
		},
		{
			name: "owner release",
			call: func(owner, _ *Tx) {
				owner.Release()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := openTempUint64Collection(t)
			err := Update(func(owner *Tx) error {
				return c.Set(owner, 1, &Rec{Name: "owner"}, OnChange(func(hook *Tx, _ uint64, _, _ *Rec) error {
					tt.call(owner, hook)
					return nil
				}))
			})
			if _, ok := err.(txLifecyclePanic); !ok {
				t.Fatalf("Update err=%v (%T), want txLifecyclePanic", err, err)
			}
			if got, err := readCount(c); err != nil {
				t.Fatalf("count after lifecycle error: %v", err)
			} else if got != 0 {
				t.Fatalf("count after lifecycle error=%d want 0", got)
			}
			if err := writeSet(c, 2, &Rec{Name: "next"}); err != nil {
				t.Fatalf("write after lifecycle error: %v", err)
			}
		})
	}
}

func TestOnChange_StringKey(t *testing.T) {
	c, _ := openTempCollectionStringProduct(t)

	var seen string
	if err := writeSet(c, "p1", &Product{SKU: "p1", Price: 10}, OnChange(func(_ *Tx, key string, oldValue, newValue *Product) error {
		if oldValue != nil {
			t.Fatalf("oldValue for insert=%#v want nil", oldValue)
		}
		seen = key
		newValue.Price = 15
		return nil
	})); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if seen != "p1" {
		t.Fatalf("OnChange key=%q want p1", seen)
	}
	got, err := readGet(c, "p1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil || got.Price != 15 {
		t.Fatalf("stored product=%#v", got)
	}
}

func TestOnChange_GeneratedWriteSameCollection(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})

	epochBefore := c.root.registry.current.Load().epoch
	if err := writeSet(c, 1, &Rec{Name: "owner"}, OnChange(func(tx *Tx, key uint64, _, _ *Rec) error {
		if key != 1 {
			t.Fatalf("owner hook key=%d want 1", key)
		}
		return c.Set(tx, 2, &Rec{Name: "generated"})
	})); err != nil {
		t.Fatalf("Set with generated write: %v", err)
	}

	got, err := readValues(c, 1, 2)
	if err != nil {
		t.Fatalf("readValues: %v", err)
	}
	if got[0] == nil || got[0].Name != "owner" || got[1] == nil || got[1].Name != "generated" {
		t.Fatalf("generated write result=%#v", got)
	}
	epochAfter := c.root.registry.current.Load().epoch
	if epochAfter != epochBefore+1 {
		t.Fatalf("root epoch after generated write=%d want %d", epochAfter, epochBefore+1)
	}
}

func TestOnChange_OuterTxWriteRejectedDuringCommit(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})

	var misuseErr error
	err := Update(func(mainTx *Tx) error {
		return c.Set(mainTx, 1, &Rec{Name: "owner"}, OnChange(func(hookTx *Tx, key uint64, _, _ *Rec) error {
			if key != 1 {
				t.Fatalf("owner hook key=%d want 1", key)
			}
			if hookTx == mainTx {
				t.Fatalf("OnChange received owner transaction")
			}
			misuseErr = c.Set(mainTx, 2, &Rec{Name: "misused"})
			return nil
		}))
	})
	if err != nil {
		t.Fatalf("owner write: %v", err)
	}
	if !errors.Is(misuseErr, rbierrors.ErrTxDone) {
		t.Fatalf("misused outer tx error=%v want %v", misuseErr, rbierrors.ErrTxDone)
	}

	got, err := readValues(c, 1, 2)
	if err != nil {
		t.Fatalf("readValues: %v", err)
	}
	if got[0] == nil || got[0].Name != "owner" || got[1] != nil {
		t.Fatalf("outer tx misuse result=%#v", got)
	}
}

func TestOnChange_GeneratedWriteOtherCollection(t *testing.T) {
	raw, _ := openRawBolt(t)
	recDB, err := Open[uint64, Rec](raw, testOptions(Options{}))
	if err != nil {
		t.Fatalf("New Rec: %v", err)
	}
	productDB, err := Open[string, Product](raw, testOptions(Options{}))
	if err != nil {
		t.Fatalf("New Product: %v", err)
	}
	t.Cleanup(func() {
		_ = recDB.Close()
		_ = productDB.Close()
		_ = raw.Close()
	})

	if err = writeSet(recDB, 1, &Rec{Name: "owner"}, OnChange(func(tx *Tx, key uint64, _, _ *Rec) error {
		if key != 1 {
			t.Fatalf("owner hook key=%d want 1", key)
		}
		return productDB.Set(tx, "audit", &Product{SKU: "audit", Price: 42})
	})); err != nil {
		t.Fatalf("Set with cross-collection generated write: %v", err)
	}

	rec, err := readGet(recDB, 1)
	if err != nil {
		t.Fatalf("Get rec: %v", err)
	}
	product, err := readGet(productDB, "audit")
	if err != nil {
		t.Fatalf("Get product: %v", err)
	}
	defer releaseUniqueRecords(recDB, rec)
	defer releaseUniqueRecords(productDB, product)
	if rec == nil || rec.Name != "owner" || product == nil || product.SKU != "audit" || product.Price != 42 {
		t.Fatalf("cross-collection generated result rec=%#v product=%#v", rec, product)
	}
}

func TestOnChange_GeneratedWriteFailureRollsBackOwner(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	fail := errors.New("generated failed")
	err := writeSet(c, 1, &Rec{Name: "owner"}, OnChange(func(tx *Tx, key uint64, _, _ *Rec) error {
		if key != 1 {
			t.Fatalf("owner hook key=%d want 1", key)
		}
		return c.Set(tx, 2, &Rec{Name: "generated"}, OnChange(func(*Tx, uint64, *Rec, *Rec) error {
			return fail
		}))
	}))
	if !errors.Is(err, fail) {
		t.Fatalf("Set error=%v want %v", err, fail)
	}
	got, err := readValues(c, 1, 2)
	if err != nil {
		t.Fatalf("readValues: %v", err)
	}
	if got[0] != nil || got[1] != nil {
		t.Fatalf("failed generated write persisted values=%#v", got)
	}
}

func TestOnChange_GeneratedUniqueFailureDoesNotDoubleReleaseRequests(t *testing.T) {
	path := filepath.Join(t.TempDir(), "generated_unique_failure.db")
	c, bolt := openBoltAndCollection[uint64, generatedUniqueHookRec](t, path, Options{BatchSoftLimit: 1})
	defer func() {
		_ = c.Close()
		_ = bolt.Close()
	}()

	err := writeSet(c, 1, &generatedUniqueHookRec{Code: "owner"}, OnChange(func(tx *Tx, key uint64, _, _ *generatedUniqueHookRec) error {
		if key != 1 {
			t.Fatalf("owner hook key=%d want 1", key)
		}
		if err := c.Set(tx, 2, &generatedUniqueHookRec{Code: "dup"}); err != nil {
			return err
		}
		return c.Set(tx, 3, &generatedUniqueHookRec{Code: "dup"})
	}))
	if !errors.Is(err, rbierrors.ErrUniqueViolation) {
		t.Fatalf("Set error=%v want ErrUniqueViolation", err)
	}

	got, err := readValues(c, 1, 2, 3)
	if err != nil {
		t.Fatalf("readValues after failed generated write: %v", err)
	}
	if got[0] != nil || got[1] != nil || got[2] != nil {
		t.Fatalf("failed generated unique write persisted values=%#v", got)
	}

	if err = writeSet(c, 4, &generatedUniqueHookRec{Code: "after"}); err != nil {
		t.Fatalf("Set after failed generated write: %v", err)
	}
	after, err := readGet(c, 4)
	if err != nil {
		t.Fatalf("Get after failed generated write: %v", err)
	}
	if after == nil || after.Code != "after" {
		t.Fatalf("after value=%#v want code=after", after)
	}
}

func TestOnChange_GeneratedWriteClosedCollectionRollsBackOwner(t *testing.T) {
	raw, _ := openRawBolt(t)
	recDB, err := Open[uint64, Rec](raw, testOptions(Options{}))
	if err != nil {
		t.Fatalf("New Rec: %v", err)
	}
	productDB, err := Open[string, Product](raw, testOptions(Options{}))
	if err != nil {
		t.Fatalf("New Product: %v", err)
	}
	t.Cleanup(func() {
		_ = recDB.Close()
		_ = raw.Close()
	})
	if err = productDB.Close(); err != nil {
		t.Fatalf("Close product: %v", err)
	}

	err = writeSet(recDB, 1, &Rec{Name: "owner"}, OnChange(func(tx *Tx, key uint64, _, _ *Rec) error {
		if key != 1 {
			t.Fatalf("owner hook key=%d want 1", key)
		}
		return productDB.Set(tx, "closed", &Product{SKU: "closed"})
	}))
	if !errors.Is(err, rbierrors.ErrClosed) {
		t.Fatalf("Set error=%v want ErrClosed", err)
	}
	if got, err := readGet(recDB, 1); err != nil {
		t.Fatalf("Get rec: %v", err)
	} else if got != nil {
		defer releaseUniqueRecords(recDB, got)
		t.Fatalf("owner persisted after closed generated write: %#v", got)
	}
}

func TestOnChange_IgnoredGeneratedWriteErrorDoesNotAbortOwner(t *testing.T) {
	raw, _ := openRawBolt(t)
	recDB, err := Open[uint64, Rec](raw, testOptions(Options{}))
	if err != nil {
		t.Fatalf("New Rec: %v", err)
	}
	productDB, err := Open[string, Product](raw, testOptions(Options{}))
	if err != nil {
		t.Fatalf("New Product: %v", err)
	}
	t.Cleanup(func() {
		_ = recDB.Close()
		_ = raw.Close()
	})
	if err = productDB.Close(); err != nil {
		t.Fatalf("Close product: %v", err)
	}

	err = writeSet(recDB, 1, &Rec{Name: "owner"}, OnChange(func(tx *Tx, key uint64, _, _ *Rec) error {
		if key != 1 {
			t.Fatalf("owner hook key=%d want 1", key)
		}
		if err := productDB.Set(tx, "closed", &Product{SKU: "closed"}); !errors.Is(err, rbierrors.ErrClosed) {
			t.Fatalf("generated closed write err=%v want ErrClosed", err)
		}
		return nil
	}))
	if err != nil {
		t.Fatalf("Set owner after ignored generated error: %v", err)
	}
	if got, err := readGet(recDB, 1); err != nil {
		t.Fatalf("Get rec: %v", err)
	} else if got == nil || got.Name != "owner" {
		t.Fatalf("owner was not persisted after ignored generated error: %#v", got)
	}
}

func TestOnChange_GeneratedWriteDepthLimit(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	var hook ExecOption[uint64, Rec]
	hook = OnChange(func(tx *Tx, key uint64, _, _ *Rec) error {
		if key < 65 {
			return c.Set(tx, key+1, &Rec{Name: fmt.Sprintf("rec-%d", key+1)}, hook)
		}
		return nil
	})
	if err := writeSet(c, 1, &Rec{Name: "rec-1"}, hook); err != nil {
		t.Fatalf("depth 64 generated writes: %v", err)
	}
	if got, err := readCount(c); err != nil {
		t.Fatalf("Count after depth 64: %v", err)
	} else if got != 65 {
		t.Fatalf("count after depth 64=%d want 65", got)
	}

	db2, _ := openTempUint64Collection(t)
	var failingHook ExecOption[uint64, Rec]
	failingHook = OnChange(func(tx *Tx, key uint64, _, _ *Rec) error {
		if key < 66 {
			return db2.Set(tx, key+1, &Rec{Name: fmt.Sprintf("rec-%d", key+1)}, failingHook)
		}
		return nil
	})
	err := writeSet(db2, 1, &Rec{Name: "rec-1"}, failingHook)
	if !errors.Is(err, rbierrors.ErrGeneratedWriteDepth) {
		t.Fatalf("depth 65 error=%v want ErrGeneratedWriteDepth", err)
	}
	if got, err := readCount(db2); err != nil {
		t.Fatalf("Count after failed depth 65: %v", err)
	} else if got != 0 {
		t.Fatalf("count after failed depth 65=%d want 0", got)
	}
}

func TestOnChange_DeleteErrorKeepsRecord(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	if err := writeSet(c, 1, &Rec{Name: "alice", Age: 10}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	hookErr := errors.New("stop delete")
	calls := 0
	err := writeDelete(c, 1, OnChange(func(_ *Tx, key uint64, oldValue, newValue *Rec) error {
		calls++
		if key != 1 || oldValue == nil || oldValue.Name != "alice" || newValue != nil {
			t.Fatalf("unexpected OnChange delete args: key=%d old=%#v new=%#v", key, oldValue, newValue)
		}
		return hookErr
	}))
	if !errors.Is(err, hookErr) {
		t.Fatalf("Delete error=%v want %v", err, hookErr)
	}
	if calls != 1 {
		t.Fatalf("OnChange delete calls=%d want 1", calls)
	}
	got, err := readGet(c, 1)
	if err != nil {
		t.Fatalf("Get after failed Delete: %v", err)
	}
	if got == nil || got.Name != "alice" || got.Age != 10 {
		t.Fatalf("record changed after failed Delete: %#v", got)
	}
	ids, err := readQueryKeys(c, qx.Query(qx.EQ("name", "alice")))
	if err != nil {
		t.Fatalf("QueryKeys after failed Delete: %v", err)
	}
	if !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("index changed after failed Delete: %v", ids)
	}
}

func TestOnChange_RunsForEqualSetCandidate(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	if err := writeSet(c, 1, &Rec{Name: "alice", Age: 10}); err != nil {
		t.Fatalf("Set seed: %v", err)
	}
	calls := 0
	err := writeSet(c, 1, &Rec{Name: "alice", Age: 10}, OnChange(func(_ *Tx, key uint64, oldValue, newValue *Rec) error {
		calls++
		if key != 1 || oldValue == nil || newValue == nil || oldValue.Name != "alice" || newValue.Name != "alice" {
			t.Fatalf("unexpected OnChange args: key=%d old=%#v new=%#v", key, oldValue, newValue)
		}
		newValue.Name = "alice-hooked"
		return nil
	}))
	if err != nil {
		t.Fatalf("equal Set with OnChange: %v", err)
	}
	if calls != 1 {
		t.Fatalf("OnChange calls=%d want 1", calls)
	}
	got, err := readGet(c, 1)
	if err != nil {
		t.Fatalf("Get after equal Set: %v", err)
	}
	if got == nil || got.Name != "alice-hooked" || got.Age != 10 {
		t.Fatalf("stored record=%#v", got)
	}
}

func TestOnChange_SetDetachesCallerValueAcrossDeferredCommit(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	input := &Rec{Name: "alice", Age: 10, Tags: []string{"go", "db"}}
	var seen Rec
	tx := BeginUpdate()
	defer tx.Release()
	if err := c.Set(tx, 1, input, OnChange(func(_ *Tx, key uint64, oldValue, newValue *Rec) error {
		if key != 1 || oldValue != nil {
			t.Fatalf("unexpected OnChange args: key=%d old=%#v", key, oldValue)
		}
		seen = *newValue
		seen.Tags = slices.Clone(newValue.Tags)
		newValue.Name = "hooked"
		return nil
	})); err != nil {
		t.Fatalf("Set: %v", err)
	}
	input.Name = "mutated"
	input.Tags[0] = "mutated"
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if seen.Name != "alice" || !slices.Equal(seen.Tags, []string{"go", "db"}) {
		t.Fatalf("OnChange saw caller mutation: %#v", seen)
	}
	got, err := readGet(c, 1)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil || got.Name != "hooked" || !slices.Equal(got.Tags, []string{"go", "db"}) {
		t.Fatalf("stored record aliased caller mutation: %#v", got)
	}
}

func TestOnChange_MultiSetDetachesCallerValuesAcrossDeferredCommit(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	a := &Rec{Name: "a", Tags: []string{"a1"}}
	b := &Rec{Name: "b", Tags: []string{"b1"}}
	seen := make(map[uint64]Rec, 2)
	tx := BeginUpdate()
	defer tx.Release()
	onChange := OnChange(func(_ *Tx, key uint64, oldValue, newValue *Rec) error {
		if oldValue != nil {
			t.Fatalf("oldValue for insert=%#v", oldValue)
		}
		cp := *newValue
		cp.Tags = slices.Clone(newValue.Tags)
		seen[key] = cp
		newValue.Name += "-hooked"
		return nil
	})
	if err := c.Set(tx, 1, a, onChange); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := c.Set(tx, 2, b, onChange); err != nil {
		t.Fatalf("Set(2): %v", err)
	}
	a.Name = "mutated-a"
	a.Tags[0] = "mutated-a"
	b.Name = "mutated-b"
	b.Tags[0] = "mutated-b"
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if seen[1].Name != "a" || !slices.Equal(seen[1].Tags, []string{"a1"}) ||
		seen[2].Name != "b" || !slices.Equal(seen[2].Tags, []string{"b1"}) {
		t.Fatalf("OnChange saw caller mutation: %#v", seen)
	}
	got, err := readValues(c, 1, 2)
	if err != nil {
		t.Fatalf("readValues: %v", err)
	}
	if got[0] == nil || got[0].Name != "a-hooked" || !slices.Equal(got[0].Tags, []string{"a1"}) ||
		got[1] == nil || got[1].Name != "b-hooked" || !slices.Equal(got[1].Tags, []string{"b1"}) {
		t.Fatalf("stored records aliased caller mutation: %#v", got)
	}
}

func TestOnChange_Patch_MutatesFinalStoredValue(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	if err := writeSet(c, 1, &Rec{Name: "alice", Age: 10, Meta: Meta{Country: "NL"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	calls := 0
	err := writePatch(c, 1, []Field{{Name: "age", Value: 20}}, OnChange(func(_ *Tx, key uint64, oldValue, newValue *Rec) error {
		calls++
		if key != 1 {
			t.Fatalf("unexpected key: %d", key)
		}
		if oldValue == nil || oldValue.Age != 10 || oldValue.Country != "NL" {
			t.Fatalf("unexpected old value: %#v", oldValue)
		}
		if newValue == nil || newValue.Age != 20 {
			t.Fatalf("unexpected patched value before OnChange mutation: %#v", newValue)
		}
		newValue.Country = "US"
		newValue.Name = "alice-updated"
		return nil
	}))
	if err != nil {
		t.Fatalf("Patch with OnChange: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected OnChange to run once, got %d", calls)
	}

	v, err := readGet(c, 1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v == nil || v.Name != "alice-updated" || v.Age != 20 || v.Country != "US" {
		t.Fatalf("unexpected stored value: %#v", v)
	}
}

func TestOnChange_ClosedDBDoesNotRunSetOrMultiSetHooks(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	setInput := &Rec{Name: "alice", Age: 10}
	setCalls := 0
	err := writeSet(c, 1, setInput, OnChange(func(_ *Tx, _ uint64, _ *Rec, value *Rec) error {
		setCalls++
		value.Name = "mutated"
		return errors.New("set on change")
	}))
	if !errors.Is(err, rbierrors.ErrClosed) {
		t.Fatalf("Set after Close error = %v, want rbierrors.ErrClosed", err)
	}
	if setCalls != 0 {
		t.Fatalf("Set OnChange calls = %d, want 0", setCalls)
	}
	if setInput.Name != "alice" || setInput.Age != 10 {
		t.Fatalf("Set mutated caller-owned input after Close: %#v", setInput)
	}

	batchInputs := []*Rec{{Name: "bob", Age: 20}, {Name: "carol", Age: 30}}
	batchCalls := 0
	err = writeSets(c, []uint64{2, 3}, batchInputs, OnChange(func(_ *Tx, _ uint64, _ *Rec, value *Rec) error {
		batchCalls++
		value.Name = "mutated"
		return errors.New("batch on change")
	}))
	if !errors.Is(err, rbierrors.ErrClosed) {
		t.Fatalf("MultiSet after Close error = %v, want rbierrors.ErrClosed", err)
	}
	if batchCalls != 0 {
		t.Fatalf("MultiSet OnChange calls = %d, want 0", batchCalls)
	}
	if batchInputs[0].Name != "bob" || batchInputs[0].Age != 20 || batchInputs[1].Name != "carol" || batchInputs[1].Age != 30 {
		t.Fatalf("MultiSet mutated caller-owned inputs after Close: %#v", batchInputs)
	}
}

func TestOnChange_MissingPatch_IsNoOp(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	calls := 0
	err := writePatch(c, 42, []Field{{Name: "age", Value: 99}}, OnChange(func(_ *Tx, _ uint64, _ *Rec, _ *Rec) error {
		calls++
		return nil
	}))
	if err != nil {
		t.Fatalf("Patch(missing): %v", err)
	}
	if calls != 0 {
		t.Fatalf("expected OnChange not to run for missing Patch target, got %d", calls)
	}
}

func TestOnChange_MultiSet_SharedPointerUsesIndependentWorkingCopies(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})

	shared := &Rec{Name: "shared", Age: 10, Meta: Meta{Country: "NL"}}
	err := writeSets(c,
		[]uint64{1, 2},
		[]*Rec{shared, shared},
		OnChange(func(_ *Tx, key uint64, oldValue, newValue *Rec) error {
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
		t.Fatalf("MultiSet with OnChange: %v", err)
	}

	if shared.Name != "shared" || shared.Age != 10 || shared.Country != "NL" {
		t.Fatalf("caller-owned shared value was mutated: %#v", shared)
	}

	v1, err := readGet(c, 1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	v2, err := readGet(c, 2)
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

type onChangeMultiPatchMapRec struct {
	Name  string            `db:"name" rbi:"index"`
	Attrs map[string]string `db:"attrs"`
}

func TestOnChange_MultiPatch_ReferencePatchValuesArePerTarget(t *testing.T) {
	c := openTempUint64CollectionReflect[onChangeMultiPatchMapRec](t, "batch_patch_map.db", Options{BatchSoftLimit: 1})

	if err := writeSets(c,
		[]uint64{1, 2},
		[]*onChangeMultiPatchMapRec{{Name: "a"}, {Name: "b"}},
	); err != nil {
		t.Fatalf("MultiSet seed: %v", err)
	}

	err := writePatches(c,
		[]uint64{1, 2},
		[]Field{{Name: "attrs", Value: map[string]string{"base": "x"}}},
		OnChange(func(_ *Tx, key uint64, _ *onChangeMultiPatchMapRec, newValue *onChangeMultiPatchMapRec) error {
			newValue.Attrs[fmt.Sprintf("hook_%d", key)] = "yes"
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("MultiPatch with OnChange: %v", err)
	}

	v1, err := readGet(c, 1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	v2, err := readGet(c, 2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if v1 == nil || v1.Attrs["base"] != "x" || v1.Attrs["hook_1"] != "yes" || v1.Attrs["hook_2"] != "" {
		t.Fatalf("unexpected value for id=1: %#v", v1)
	}
	if v2 == nil || v2.Attrs["base"] != "x" || v2.Attrs["hook_2"] != "yes" || v2.Attrs["hook_1"] != "" {
		t.Fatalf("unexpected value for id=2: %#v", v2)
	}
}

func TestOnChange_Set_AllowsNormalizationBeforeEncode(t *testing.T) {
	c := openTempUint64CollectionOnChangeCloneRec(t, Options{BatchSoftLimit: 1})

	calls := 0
	input := &onChangeCloneRec{}
	err := writeSet(c,
		1,
		input,
		OnChange(func(_ *Tx, key uint64, oldValue, newValue *onChangeCloneRec) error {
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
		t.Fatalf("Set with OnChange: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected OnChange to run once, got %d", calls)
	}
	if input.Name != "" || input.Ready {
		t.Fatalf("caller-owned input was mutated: %#v", input)
	}

	got, err := readGet(c, 1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil || got.Name != "normalized" || !got.Ready {
		t.Fatalf("unexpected stored value: %#v", got)
	}
}

func TestOnChange_Set_WithCloneMethod_AllowsNormalizationBeforeEncode(t *testing.T) {
	c := openTempUint64CollectionOnChangeCloneRec(t, Options{BatchSoftLimit: 1})

	calls := 0
	input := &onChangeCloneRec{}
	err := writeSet(c,
		1,
		input,
		OnChange(func(_ *Tx, key uint64, oldValue, newValue *onChangeCloneRec) error {
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
		t.Fatalf("Set with ignored Clone() method: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected OnChange to run once, got %d", calls)
	}
	if input.Name != "" || input.Ready {
		t.Fatalf("caller-owned input was mutated: %#v", input)
	}

	got, err := readGet(c, 1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil || got.Name != "normalized" || !got.Ready {
		t.Fatalf("unexpected stored value: %#v", got)
	}
}

func TestOnChange_MultiSet_AllowsNormalizationBeforeEncode(t *testing.T) {
	c := openTempUint64CollectionOnChangeCloneRec(t, Options{BatchSoftLimit: 1})

	inputA := &onChangeCloneRec{}
	inputB := &onChangeCloneRec{}
	err := writeSets(c,
		[]uint64{1, 2},
		[]*onChangeCloneRec{inputA, inputB},
		OnChange(func(_ *Tx, key uint64, oldValue, newValue *onChangeCloneRec) error {
			if oldValue != nil {
				t.Fatalf("expected insert oldValue=nil, got %#v", oldValue)
			}
			newValue.Name = fmt.Sprintf("normalized-%d", key)
			newValue.Ready = true
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("MultiSet with OnChange: %v", err)
	}
	if inputA.Name != "" || inputA.Ready || inputB.Name != "" || inputB.Ready {
		t.Fatalf("caller-owned inputs were mutated: %#v %#v", inputA, inputB)
	}

	got1, err := readGet(c, 1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	got2, err := readGet(c, 2)
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

func TestOnChange_MultiSet_WithCloneMethod_AllowsNormalizationBeforeEncode(t *testing.T) {
	c := openTempUint64CollectionOnChangeCloneRec(t, Options{BatchSoftLimit: 1})

	inputA := &onChangeCloneRec{}
	inputB := &onChangeCloneRec{}
	err := writeSets(c,
		[]uint64{1, 2},
		[]*onChangeCloneRec{inputA, inputB},
		OnChange(func(_ *Tx, key uint64, oldValue, newValue *onChangeCloneRec) error {
			if oldValue != nil {
				t.Fatalf("expected insert oldValue=nil, got %#v", oldValue)
			}
			newValue.Name = fmt.Sprintf("normalized-%d", key)
			newValue.Ready = true
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("MultiSet with ignored Clone() method: %v", err)
	}
	if inputA.Name != "" || inputA.Ready || inputB.Name != "" || inputB.Ready {
		t.Fatalf("caller-owned inputs were mutated: %#v %#v", inputA, inputB)
	}

	got1, err := readGet(c, 1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	got2, err := readGet(c, 2)
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

func TestOnChange_AutoBatch_AllowsNormalizationBeforeEncode(t *testing.T) {
	enableStoreStatsForTest(t)
	c := openTempUint64CollectionOnChangeCloneRec(t)

	calls := 0
	err := writeSet(c,
		1,
		&onChangeCloneRec{},
		OnChange(func(_ *Tx, _ uint64, _ *onChangeCloneRec, newValue *onChangeCloneRec) error {
			calls++
			newValue.Name = "combined"
			newValue.Ready = true
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("Set with OnChange via root scheduler: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected OnChange to run once, got %d", calls)
	}

	got, err := readGet(c, 1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil || got.Name != "combined" || !got.Ready {
		t.Fatalf("unexpected stored value: %#v", got)
	}

	if st := c.StoreStats(); st.LogicalUnitsEnqueued == 0 {
		t.Fatalf("expected Set with OnChange to use root scheduler path, stats=%+v", st)
	}
}

func TestOnChange_AutoBatch_WithCloneMethod_AllowsNormalizationBeforeEncode(t *testing.T) {
	enableStoreStatsForTest(t)
	c := openTempUint64CollectionOnChangeCloneRec(t)

	calls := 0
	err := writeSet(c,
		1,
		&onChangeCloneRec{},
		OnChange(func(_ *Tx, _ uint64, _ *onChangeCloneRec, newValue *onChangeCloneRec) error {
			calls++
			newValue.Name = "combined"
			newValue.Ready = true
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("Set with ignored Clone() method via root scheduler: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected OnChange to run once, got %d", calls)
	}

	got, err := readGet(c, 1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil || got.Name != "combined" || !got.Ready {
		t.Fatalf("unexpected stored value: %#v", got)
	}

	if st := c.StoreStats(); st.LogicalUnitsEnqueued == 0 {
		t.Fatalf("expected Set with ignored Clone() method to use root scheduler path, stats=%+v", st)
	}
}

func TestIOExt_Set_OnChangeCallbackError_DoesNotPersist(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})
	wantErr := errors.New("on change failed")

	err := writeSet(c, 1, &Rec{Name: "alice"}, OnChange(func(_ *Tx, _ uint64, _ *Rec, newValue *Rec) error {
		newValue.Name = "mutated"
		return wantErr
	}))
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}

	if v, err := readGet(c, 1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if v != nil {
		t.Fatalf("value persisted after OnChange error: %#v", v)
	}
	if cnt := ioExtMustCountUint64(t, c); cnt != 0 {
		t.Fatalf("expected Count=0, got %d", cnt)
	}
}

func TestOnChangePanicIsReturnedAsWriteError(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})
	panicErr := errors.New("hook panic")

	err := writeSet(c, 1, &Rec{Name: "alice"}, OnChange(func(_ *Tx, _ uint64, _ *Rec, newValue *Rec) error {
		newValue.Name = "mutated"
		panic(panicErr)
	}))
	if !errors.Is(err, panicErr) {
		t.Fatalf("Set err=%v, want wrapped panic error", err)
	}
	if !strings.Contains(err.Error(), "on change panic") {
		t.Fatalf("Set err=%v, want panic context", err)
	}
	if c.root.broken.Load() {
		t.Fatalf("OnChange panic marked root broken")
	}
	if v, err := readGet(c, 1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if v != nil {
		t.Fatalf("value persisted after OnChange panic: %#v", v)
	}
	if err = writeSet(c, 2, &Rec{Name: "next"}); err != nil {
		t.Fatalf("write after OnChange panic: %v", err)
	}
}

func TestOnChangeStringPanicIsReturnedAsWriteError(t *testing.T) {
	c, _ := openTempCollectionStringProduct(t, Options{BatchSoftLimit: 1})

	err := writeSet(c, "p1", &Product{SKU: "p1", Price: 10}, OnChange(func(_ *Tx, _ string, _ *Product, newValue *Product) error {
		newValue.Price = 20
		panic("string hook panic")
	}))
	if err == nil || !strings.Contains(err.Error(), "on change panic: string hook panic") {
		t.Fatalf("Set err=%v, want string panic error", err)
	}
	if c.root.broken.Load() {
		t.Fatalf("OnChange panic marked root broken")
	}
	if v, err := readGet(c, "p1"); err != nil {
		t.Fatalf("Get(p1): %v", err)
	} else if v != nil {
		t.Fatalf("value persisted after string OnChange panic: %#v", v)
	}
	if err = writeSet(c, "p2", &Product{SKU: "p2", Price: 30}); err != nil {
		t.Fatalf("string write after OnChange panic: %v", err)
	}
}

func TestIOExt_Set_OnChangeError_DoesNotMutateCaller(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})
	wantErr := errors.New("on change failed")

	input := &Rec{Name: "alice", Tags: []string{"go"}, Meta: Meta{Country: "NL"}}
	err := writeSet(c,
		1,
		input,
		OnChange(func(_ *Tx, _ uint64, _ *Rec, newValue *Rec) error {
			newValue.Name = "changed"
			newValue.Tags = append(newValue.Tags, "db")
			newValue.Country = "US"
			return wantErr
		}),
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}

	if input.Name != "alice" || input.Country != "NL" || !slices.Equal(input.Tags, []string{"go"}) {
		t.Fatalf("caller-owned value mutated despite rollback: %#v", input)
	}
	if v, err := readGet(c, 1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if v != nil {
		t.Fatalf("value persisted after OnChange error: %#v", v)
	}
}

func TestIOExt_MultiSet_OnChangeCallbackError_IsAtomicAndKeepsCallerUntouched(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})
	wantErr := errors.New("on change failed")

	shared := &Rec{Name: "shared", Tags: []string{"go"}, Meta: Meta{Country: "NL"}}
	err := writeSets(c,
		[]uint64{1, 2},
		[]*Rec{shared, shared},
		OnChange(func(_ *Tx, key uint64, _ *Rec, newValue *Rec) error {
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
	if v, err := readGet(c, 1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if v != nil {
		t.Fatalf("id=1 persisted after batch failure: %#v", v)
	}
	if v, err := readGet(c, 2); err != nil {
		t.Fatalf("Get(2): %v", err)
	} else if v != nil {
		t.Fatalf("id=2 persisted after batch failure: %#v", v)
	}
	if cnt := ioExtMustCountUint64(t, c); cnt != 0 {
		t.Fatalf("expected Count=0, got %d", cnt)
	}
}

func TestIOExt_Delete_OnChangeError_RollsBackDataAndIndex(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})
	ioExtMustSetRec(t, c, 1, &Rec{Name: "alice", Age: 10})
	wantErr := errors.New("on change failed")

	err := writeDelete(c, 1, OnChange(func(_ *Tx, _ uint64, oldValue, newValue *Rec) error {
		if oldValue == nil || newValue != nil || oldValue.Name != "alice" {
			t.Fatalf("unexpected callback values: old=%#v new=%#v", oldValue, newValue)
		}
		return wantErr
	}))
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}

	v := ioExtMustGetRec(t, c, 1)
	if v.Name != "alice" || v.Age != 10 {
		t.Fatalf("record changed after failed Delete: %#v", v)
	}
	if cnt := ioExtMustCountUint64(t, c); cnt != 1 {
		t.Fatalf("expected Count=1, got %d", cnt)
	}
	ids := ioExtMustQueryName(t, c, "alice")
	if !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("unexpected name index after failed Delete: %v", ids)
	}
}

func TestIOExt_MultiDelete_OnChangeErrorOnLaterRecord_RollsBackEarlierDelete(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})
	ioExtMustSetRec(t, c, 1, &Rec{Name: "one"})
	ioExtMustSetRec(t, c, 2, &Rec{Name: "two"})
	wantErr := errors.New("stop batch delete")

	err := writeDeletes(c, []uint64{1, 2}, OnChange(func(_ *Tx, key uint64, _ *Rec, _ *Rec) error {
		if key == 2 {
			return wantErr
		}
		return nil
	}))
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}

	v1 := ioExtMustGetRec(t, c, 1)
	v2 := ioExtMustGetRec(t, c, 2)
	if v1.Name != "one" || v2.Name != "two" {
		t.Fatalf("records changed after rolled back MultiDelete: v1=%#v v2=%#v", v1, v2)
	}
	if cnt := ioExtMustCountUint64(t, c); cnt != 2 {
		t.Fatalf("expected Count=2, got %d", cnt)
	}
}

func TestIOExt_MultiSet_DuplicateIDs_OnChangeSeesPerStepOldValue(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})
	ioExtMustSetRec(t, c, 1, &Rec{Name: "seed", Age: 1})

	var seen []string
	err := writeSets(c,
		[]uint64{1, 1},
		[]*Rec{
			{Name: "first", Age: 2},
			{Name: "second", Age: 3},
		},
		OnChange(func(_ *Tx, _ uint64, oldValue, newValue *Rec) error {
			oldName := "<nil>"
			if oldValue != nil {
				oldName = oldValue.Name
			}
			seen = append(seen, oldName+"->"+newValue.Name)
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("MultiSet: %v", err)
	}
	if !slices.Equal(seen, []string{"seed->first", "first->second"}) {
		t.Fatalf("unexpected OnChange old/new sequence: %v", seen)
	}

	v := ioExtMustGetRec(t, c, 1)
	if v.Name != "second" || v.Age != 3 {
		t.Fatalf("unexpected final value: %#v", v)
	}
}

func TestIOExt_MultiPatch_DuplicateIDs_OnChangeSeesPerStepOldValue(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})
	ioExtMustSetRec(t, c, 1, &Rec{Name: "seed", Age: 10, Meta: Meta{Country: "NL"}})

	call := 0
	var seen []string
	err := writePatches(c,
		[]uint64{1, 1},
		[]Field{{Name: "country", Value: "PL"}},
		OnChange(func(_ *Tx, _ uint64, oldValue, newValue *Rec) error {
			call++
			newValue.Name = fmt.Sprintf("patched-%d", call)
			seen = append(seen, oldValue.Name+"->"+newValue.Name)
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("MultiPatch: %v", err)
	}
	if !slices.Equal(seen, []string{"seed->patched-1", "patched-1->patched-2"}) {
		t.Fatalf("unexpected OnChange old/new sequence: %v", seen)
	}

	v := ioExtMustGetRec(t, c, 1)
	if v.Name != "patched-2" || v.Country != "PL" {
		t.Fatalf("unexpected final value: %#v", v)
	}
}

func TestIOExt_MultiPatch_DuplicateIDs_OnChangeSeesPerStepCurrentValue(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})
	ioExtMustSetRec(t, c, 1, &Rec{Name: "seed", Age: 10, Meta: Meta{Country: "NL"}})

	var ages []int
	err := writePatches(c,
		[]uint64{1, 1},
		[]Field{{Name: "country", Value: "PL"}},
		OnChange(func(_ *Tx, _ uint64, _ *Rec, value *Rec) error {
			value.Age++
			ages = append(ages, value.Age)
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("MultiPatch: %v", err)
	}
	if !slices.Equal(ages, []int{11, 12}) {
		t.Fatalf("OnChange did not observe per-step current value: %v", ages)
	}

	v := ioExtMustGetRec(t, c, 1)
	if v.Age != 12 || v.Country != "PL" {
		t.Fatalf("unexpected final value: %#v", v)
	}
}

func TestInstallCommittedPanicBreaksDB(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})

	err := writeSet(c, 1, &Rec{Name: "alice", Age: 30})
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	err = c.root.installCommitted(c.collection, nil)
	if !errors.Is(err, rbierrors.ErrBroken) {
		t.Fatalf("expected installCommitted to fail with rbierrors.ErrBroken after panic, got: %v", err)
	}

	var got *Rec
	if viewErr := c.root.bolt.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(c.dataBucket)
		if bucket == nil {
			return fmt.Errorf("bucket does not exist")
		}
		var keyBuf [8]byte
		raw := bucket.Get(keycodec.UserKeyBytesWithBuf(uint64(1), c.strKey, &keyBuf))
		if raw == nil {
			return fmt.Errorf("record was not committed")
		}
		var decodeErr error
		got, decodeErr = c.decode(raw)
		return decodeErr
	}); viewErr != nil {
		t.Fatalf("bolt view after publish failure: %v", viewErr)
	}
	if got == nil || got.Name != "alice" || got.Age != 30 {
		t.Fatalf("unexpected committed value after publish failure: %#v", got)
	}

	rejected := &Rec{Name: "bob", Age: 20}
	setCalls := 0
	if err = writeSet(c, 2, rejected, OnChange(func(_ *Tx, _ uint64, _ *Rec, value *Rec) error {
		setCalls++
		value.Name = "mutated"
		return errors.New("set on change")
	})); !errors.Is(err, rbierrors.ErrBroken) {
		t.Fatalf("expected DB to reject subsequent writes with rbierrors.ErrBroken, got: %v", err)
	}
	if setCalls != 0 {
		t.Fatalf("Set OnChange calls after broken DB = %d, want 0", setCalls)
	}
	if rejected.Name != "bob" || rejected.Age != 20 {
		t.Fatalf("Set mutated caller-owned input after broken DB: %#v", rejected)
	}

	batchRejected := []*Rec{{Name: "carol", Age: 30}, {Name: "dave", Age: 40}}
	batchCalls := 0
	if err = writeSets(c, []uint64{3, 4}, batchRejected, OnChange(func(_ *Tx, _ uint64, _ *Rec, value *Rec) error {
		batchCalls++
		value.Name = "mutated"
		return errors.New("batch on change")
	})); !errors.Is(err, rbierrors.ErrBroken) {
		t.Fatalf("expected DB to reject subsequent batch writes with rbierrors.ErrBroken, got: %v", err)
	}
	if batchCalls != 0 {
		t.Fatalf("MultiSet OnChange calls after broken DB = %d, want 0", batchCalls)
	}
	if batchRejected[0].Name != "carol" || batchRejected[0].Age != 30 || batchRejected[1].Name != "dave" || batchRejected[1].Age != 40 {
		t.Fatalf("MultiSet mutated caller-owned inputs after broken DB: %#v", batchRejected)
	}
	if _, err := readQuery(c, qx.Query(qx.EQ("name", "alice"))); !errors.Is(err, rbierrors.ErrBroken) {
		t.Fatalf("expected Query to fail with rbierrors.ErrBroken after publish failure, got: %v", err)
	}

	if err := c.Close(); !errors.Is(err, rbierrors.ErrBroken) {
		t.Fatalf("expected Close to return rbierrors.ErrBroken for broken db, got: %v", err)
	}
}

func TestInstallCommittedPanicBreaksAllRootCollections(t *testing.T) {
	raw, path := openRawBolt(t)
	defer func() { _ = raw.Close() }()

	var recLog bytes.Buffer
	recDB, err := Open[uint64, Rec](raw, testOptions(Options{
		BucketName:        "panic_users",
		DisableIndexStore: true,
		BatchSoftLimit:    1,
		Logger:            log.New(&recLog, "", 0),
	}))
	if err != nil {
		t.Fatalf("New recDB: %v", err)
	}
	defer func() { _ = recDB.Close() }()

	var productLog bytes.Buffer
	productCollection, err := Open[string, Product](raw, testOptions(Options{
		BucketName:        "panic_products",
		DisableIndexStore: true,
		Logger:            log.New(&productLog, "", 0),
	}))
	if err != nil {
		t.Fatalf("New productDB at %s: %v", path, err)
	}
	defer func() { _ = productCollection.Close() }()

	err = writeSet(recDB, 1, &Rec{Name: "alice", Age: 30})
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	err = recDB.root.installCommitted(recDB.collection, nil)
	if !errors.Is(err, rbierrors.ErrBroken) {
		t.Fatalf("installCommitted error=%v want ErrBroken", err)
	}
	if !recDB.root.broken.Load() {
		t.Fatal("root was not marked broken")
	}
	if recDB.collection.state.Load()&collectionBroken == 0 {
		t.Fatal("rec collection was not marked broken")
	}
	if productCollection.collection.state.Load()&collectionBroken == 0 {
		t.Fatal("product collection was not marked broken")
	}
	if !strings.Contains(recLog.String(), "root entered broken state") {
		t.Fatalf("rec logger did not receive root broken transition: %q", recLog.String())
	}
	if !strings.Contains(productLog.String(), "root entered broken state") {
		t.Fatalf("product logger did not receive root broken transition: %q", productLog.String())
	}
}

func TestOnChange_PatchAndDeleteRollbackAndKeepState(t *testing.T) {
	t.Run("patch", func(t *testing.T) {
		c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})
		if err := writeSet(c, 1, &Rec{Name: "alice", Age: 30, Meta: Meta{Country: "NL"}}); err != nil {
			t.Fatalf("Set(1): %v", err)
		}

		err := writePatch(c, 1, []Field{{Name: "age", Value: 99}}, OnChange(func(*Tx, uint64, *Rec, *Rec) error {
			return fmt.Errorf("on change patch")
		}))
		if err == nil || !strings.Contains(err.Error(), "on change patch") {
			t.Fatalf("expected OnChange patch error, got: %v", err)
		}

		assertNoFutureSnapshotRefs(t, c)
		v, err := readGet(c, 1)
		if err != nil {
			t.Fatalf("Get(1): %v", err)
		}
		if v == nil || v.Age != 30 {
			t.Fatalf("expected record unchanged after failed patch commit, got %#v", v)
		}
	})

	t.Run("delete", func(t *testing.T) {
		c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})
		if err := writeSet(c, 1, &Rec{Name: "alice", Age: 30, Meta: Meta{Country: "NL"}}); err != nil {
			t.Fatalf("Set(1): %v", err)
		}

		err := writeDelete(c, 1, OnChange(func(*Tx, uint64, *Rec, *Rec) error {
			return fmt.Errorf("on change delete")
		}))
		if err == nil || !strings.Contains(err.Error(), "on change delete") {
			t.Fatalf("expected OnChange delete error, got: %v", err)
		}

		assertNoFutureSnapshotRefs(t, c)
		v, err := readGet(c, 1)
		if err != nil {
			t.Fatalf("Get(1): %v", err)
		}
		if v == nil {
			t.Fatalf("expected record to remain after failed delete commit")
		}
	})
}

func TestOnChange_MultiWritePathsRollbackAndKeepState(t *testing.T) {
	type tc struct {
		name   string
		setup  func(t *testing.T, c *Collection[uint64, Rec])
		run    func(c *Collection[uint64, Rec], opt ExecOption[uint64, Rec]) error
		verify func(t *testing.T, c *Collection[uint64, Rec])
	}

	cases := []tc{
		{
			name: "batch_set",
			run: func(c *Collection[uint64, Rec], opt ExecOption[uint64, Rec]) error {
				return writeSets(c,
					[]uint64{1, 2},
					[]*Rec{
						{Name: "a", Age: 10, Meta: Meta{Country: "NL"}},
						{Name: "b", Age: 11, Meta: Meta{Country: "DE"}},
					},
					opt,
				)
			},
			verify: func(t *testing.T, c *Collection[uint64, Rec]) {
				t.Helper()
				v1, err := readGet(c, 1)
				if err != nil {
					t.Fatalf("Get(1): %v", err)
				}
				v2, err := readGet(c, 2)
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
			setup: func(t *testing.T, c *Collection[uint64, Rec]) {
				t.Helper()
				if err := writeSet(c, 1, &Rec{Name: "a", Age: 10, Meta: Meta{Country: "NL"}}); err != nil {
					t.Fatalf("Set(1): %v", err)
				}
				if err := writeSet(c, 2, &Rec{Name: "b", Age: 11, Meta: Meta{Country: "DE"}}); err != nil {
					t.Fatalf("Set(2): %v", err)
				}
			},
			run: func(c *Collection[uint64, Rec], opt ExecOption[uint64, Rec]) error {
				return writePatches(c, []uint64{1, 2}, []Field{{Name: "age", Value: 99}}, opt)
			},
			verify: func(t *testing.T, c *Collection[uint64, Rec]) {
				t.Helper()
				v1, err := readGet(c, 1)
				if err != nil {
					t.Fatalf("Get(1): %v", err)
				}
				v2, err := readGet(c, 2)
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
			setup: func(t *testing.T, c *Collection[uint64, Rec]) {
				t.Helper()
				if err := writeSet(c, 1, &Rec{Name: "a", Age: 10, Meta: Meta{Country: "NL"}}); err != nil {
					t.Fatalf("Set(1): %v", err)
				}
				if err := writeSet(c, 2, &Rec{Name: "b", Age: 11, Meta: Meta{Country: "DE"}}); err != nil {
					t.Fatalf("Set(2): %v", err)
				}
			},
			run: func(c *Collection[uint64, Rec], opt ExecOption[uint64, Rec]) error {
				return writeDeletes(c, []uint64{1, 2}, opt)
			},
			verify: func(t *testing.T, c *Collection[uint64, Rec]) {
				t.Helper()
				v1, err := readGet(c, 1)
				if err != nil {
					t.Fatalf("Get(1): %v", err)
				}
				v2, err := readGet(c, 2)
				if err != nil {
					t.Fatalf("Get(2): %v", err)
				}
				if v1 == nil || v2 == nil {
					t.Fatalf("expected records to remain after failed delete_many, got v1=%#v v2=%#v", v1, v2)
				}
			},
		},
	}

	for _, cs := range cases {
		cs := cs
		t.Run(cs.name, func(t *testing.T) {
			c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})
			if cs.setup != nil {
				cs.setup(t, c)
			}

			wantErr := "on change " + cs.name
			err := cs.run(c, OnChange(func(*Tx, uint64, *Rec, *Rec) error {
				return errors.New(wantErr)
			}))
			if err == nil || !strings.Contains(err.Error(), wantErr) {
				t.Fatalf("expected OnChange error for %s, got: %v", cs.name, err)
			}

			assertNoFutureSnapshotRefs(t, c)

			cs.verify(t, c)
		})
	}
}

func TestNew_DefaultExecOptions_ApplyToWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "default_on_change.db")

	raw, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}
	defer func() { _ = raw.Close() }()

	var calls []string
	c, err := Open[uint64, Rec](
		raw,
		testOptions(Options{BatchSoftLimit: 1}),
		PatchStrict,
		OnChange(func(_ *Tx, _ uint64, _ *Rec, newValue *Rec) error {
			newValue.Name = "pre-" + newValue.Name
			newValue.Country = "US"
			newValue.Age++
			calls = append(calls, "default")
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = c.Close() }()

	err = writeSet(c,
		1,
		&Rec{Name: "alice", Age: 30},
		OnChange(func(_ *Tx, _ uint64, _, _ *Rec) error {
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

	v, err := readGet(c, 1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v == nil || v.Name != "pre-alice" || v.Age != 31 {
		t.Fatalf("unexpected stored value: %#v", v)
	}

	err = writePatch(c, 1, []Field{{Name: "country", Value: "CA"}})
	if err != nil {
		t.Fatalf("Patch with default OnChange: %v", err)
	}

	v, err = readGet(c, 1)
	if err != nil {
		t.Fatalf("Get(1) after Patch: %v", err)
	}
	if v == nil || v.Country != "US" {
		t.Fatalf("expected Patch to inherit default OnChange, got %#v", v)
	}

	err = writePatch(c, 1, []Field{{Name: "does_not_exist", Value: 1}})
	if err == nil {
		t.Fatal("expected Patch to inherit default PatchStrict option")
	}
}

type onChangeCloneRec struct {
	Name  string `db:"name"  rbi:"index"`
	Ready bool   `db:"ready" rbi:"index"`
}

type generatedUniqueHookRec struct {
	Code string `db:"code" rbi:"unique"`
}

func (r *onChangeCloneRec) Clone() *onChangeCloneRec {
	if r == nil {
		return nil
	}
	cp := *r
	return &cp
}

func openTempUint64CollectionOnChangeCloneRec(t *testing.T, options ...Options) *Collection[uint64, onChangeCloneRec] {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_on_change_clone.db")
	c, bolt := openBoltAndCollection[uint64, onChangeCloneRec](t, path, options...)
	t.Cleanup(func() {
		_ = c.Close()
		_ = bolt.Close()
	})
	return c
}

func TestMultiSet_BuildErrorDoesNotAffectFollowingWrite(t *testing.T) {
	c := openTempUint64CollectionOnChangeCloneRec(t, Options{BatchSoftLimit: 1})
	badName := strings.Repeat("x", 70000)

	err := writeSets(c,
		[]uint64{1, 2},
		[]*onChangeCloneRec{
			{Name: "ok", Ready: true},
			{Name: badName, Ready: true},
		},
	)
	if err == nil || !strings.Contains(err.Error(), "indexed string value") {
		t.Fatalf("MultiSet build error = %v, want indexed string validation failure", err)
	}

	calls := 0
	if err := writeSet(c, 3, &onChangeCloneRec{Name: "hooked", Ready: true}, OnChange(func(_ *Tx, _ uint64, _ *onChangeCloneRec, _ *onChangeCloneRec) error {
		calls++
		return nil
	})); err != nil {
		t.Fatalf("Set after failed MultiSet: %v", err)
	}
	if calls != 1 {
		t.Fatalf("OnChange calls after failed MultiSet = %d, want 1", calls)
	}
	got, err := readGet(c, 3)
	if err != nil {
		t.Fatalf("Get(3): %v", err)
	}
	if got == nil || got.Name != "hooked" || !got.Ready {
		t.Fatalf("following write stored %#v", got)
	}
}
