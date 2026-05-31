package rbi

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/keycodec"
	"go.etcd.io/bbolt"
)

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

func TestBatchSet_DuplicateIDs_LastWriteWinsAndIndexConsistent(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	if err := db.Set(1, &Rec{Name: "seed", Age: 21, Tags: []string{"go"}, Meta: Meta{Country: "NL"}}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	valA := &Rec{Name: "first", Age: 30, Tags: []string{"rust"}, Meta: Meta{Country: "DE"}}
	valB := &Rec{Name: "second", Age: 40, Tags: []string{"db", "ops"}, Meta: Meta{Country: "PL"}}
	if err := db.BatchSet([]uint64{1, 1}, []*Rec{valA, valB}); err != nil {
		t.Fatalf("BatchSet duplicate ids: %v", err)
	}

	got, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil || got.Name != "second" || got.Age != 40 || got.Country != "PL" {
		t.Fatalf("expected last value to win, got %#v", got)
	}

	assertHas := func(field string, value string, want bool) {
		t.Helper()
		expr := qx.EQ(field, value)
		if field == "tags" {
			expr = qx.HASANY(field, []string{value})
		}
		ids, err := db.QueryKeys(qx.Query(expr))
		if err != nil {
			t.Fatalf("QueryKeys(%s=%s): %v", field, value, err)
		}
		has := false
		for _, id := range ids {
			if id == 1 {
				has = true
				break
			}
		}
		if has != want {
			t.Fatalf("unexpected index membership for %s=%s: has=%v want=%v ids=%v", field, value, has, want, ids)
		}
	}

	assertHas("name", "first", false)
	assertHas("name", "second", true)
	assertHas("country", "DE", false)
	assertHas("country", "PL", true)
	assertHas("tags", "rust", false)
	assertHas("tags", "db", true)
	assertHas("tags", "ops", true)

	if cnt, err := db.Count(); err != nil {
		t.Fatalf("Count: %v", err)
	} else if cnt != 1 {
		t.Fatalf("expected Count=1, got %d", cnt)
	}
}

func TestSet_NilValue_ReturnsErrNilValueAndNoWrites(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	err := db.Set(1, nil)
	if err == nil || !errors.Is(err, ErrNilValue) {
		t.Fatalf("expected ErrNilValue, got: %v", err)
	}

	v, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v != nil {
		t.Fatalf("expected nil value for id=1, got %#v", v)
	}

	if cnt, err := db.Count(); err != nil {
		t.Fatalf("Count(): %v", err)
	} else if cnt != 0 {
		t.Fatalf("expected Count=0, got %d", cnt)
	}

	ids, err := db.QueryKeys(qx.Query())
	if err != nil {
		t.Fatalf("QueryKeys(all): %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected no indexed ids, got %v", ids)
	}

	assertNoFutureSnapshotRefs(t, db)
}

func TestBatchSet_NilValue_ReturnsErrNilValueAndAtomic(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	base := &Rec{Name: "base", Age: 21, Tags: []string{"go"}, Meta: Meta{Country: "NL"}}
	if err := db.Set(1, base); err != nil {
		t.Fatalf("seed Set(1): %v", err)
	}

	err := db.BatchSet(
		[]uint64{1, 2},
		[]*Rec{
			{Name: "changed", Age: 55, Tags: []string{"ops"}, Meta: Meta{Country: "DE"}},
			nil,
		},
	)
	if err == nil || !errors.Is(err, ErrNilValue) {
		t.Fatalf("expected ErrNilValue from BatchSet, got: %v", err)
	}

	v1, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v1 == nil || v1.Name != "base" || v1.Age != 21 || v1.Country != "NL" {
		t.Fatalf("id=1 changed after rejected BatchSet: %#v", v1)
	}

	v2, err := db.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if v2 != nil {
		t.Fatalf("expected id=2 to remain absent, got %#v", v2)
	}

	ids, err := db.QueryKeys(qx.Query(qx.EQ("name", "changed")))
	if err != nil {
		t.Fatalf("QueryKeys(name=changed): %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("unexpected index entry for rejected value: %v", ids)
	}

	if cnt, err := db.Count(); err != nil {
		t.Fatalf("Count(): %v", err)
	} else if cnt != 1 {
		t.Fatalf("expected Count=1 after rejected BatchSet, got %d", cnt)
	}

	assertNoFutureSnapshotRefs(t, db)
}

func TestBatchPatchBatchDelete_DuplicateIDs_IndexConsistency(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	if err := db.Set(1, &Rec{Name: "n1", Age: 10, Tags: []string{"go"}, Meta: Meta{Country: "NL"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "n2", Age: 20, Tags: []string{"db"}, Meta: Meta{Country: "DE"}}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	if err := db.BatchPatch(
		[]uint64{1, 1, 2},
		[]Field{
			{Name: "age", Value: 99},
			{Name: "tags", Value: []string{"ops"}},
			{Name: "country", Value: "PL"},
		},
	); err != nil {
		t.Fatalf("BatchPatch duplicate ids: %v", err)
	}

	assertRec := func(id uint64, age int, country string) {
		t.Helper()
		v, err := db.Get(id)
		if err != nil {
			t.Fatalf("Get(%d): %v", id, err)
		}
		if v == nil {
			t.Fatalf("Get(%d): nil", id)
		}
		if v.Age != age || v.Country != country || len(v.Tags) != 1 || v.Tags[0] != "ops" {
			t.Fatalf("unexpected value for id=%d: %#v", id, v)
		}
	}
	assertRec(1, 99, "PL")
	assertRec(2, 99, "PL")

	ids, err := db.QueryKeys(qx.Query(qx.HASANY("tags", []string{"go"})))
	if err != nil {
		t.Fatalf("QueryKeys(tags has go): %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("stale tags=go index entries: %v", ids)
	}

	ids, err = db.QueryKeys(qx.Query(qx.HASANY("tags", []string{"db"})))
	if err != nil {
		t.Fatalf("QueryKeys(tags has db): %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("stale tags=db index entries: %v", ids)
	}

	ids, err = db.QueryKeys(qx.Query(qx.HASANY("tags", []string{"ops"})))
	if err != nil {
		t.Fatalf("QueryKeys(tags has ops): %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected both ids for tags=ops, got %v", ids)
	}

	if err := db.BatchDelete([]uint64{1, 1, 2, 2}); err != nil {
		t.Fatalf("BatchDelete duplicate ids: %v", err)
	}

	if cnt, err := db.Count(); err != nil {
		t.Fatalf("Count: %v", err)
	} else if cnt != 0 {
		t.Fatalf("expected Count=0 after duplicate BatchDelete, got %d", cnt)
	}

	ids, err = db.QueryKeys(qx.Query(qx.HASANY("tags", []string{"ops"})))
	if err != nil {
		t.Fatalf("QueryKeys(tags has ops) after delete: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected empty tags=ops index after delete, got %v", ids)
	}
}

func TestBatchPatch_DecodeError_RollsBackEarlierWrites(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	if err := db.Set(1, &Rec{Name: "a", Age: 10, Meta: Meta{Country: "NL"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "b", Age: 20, Meta: Meta{Country: "DE"}}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	var beforeRaw []byte
	if err := db.bolt.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.bucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		var keyBuf [8]byte
		v := b.Get(keycodec.UserKeyBytesWithBuf(uint64(1), db.strKey, &keyBuf))
		if v == nil {
			return fmt.Errorf("missing raw value for id=1")
		}
		beforeRaw = append([]byte(nil), v...)
		return nil
	}); err != nil {
		t.Fatalf("snapshot raw(id=1): %v", err)
	}

	if err := db.bolt.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.bucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		var keyBuf [8]byte
		return b.Put(keycodec.UserKeyBytesWithBuf(uint64(2), db.strKey, &keyBuf), []byte{0xff, 0x00, 0x7f, 0x42})
	}); err != nil {
		t.Fatalf("corrupt id=2 payload: %v", err)
	}

	err := db.BatchPatch([]uint64{1, 2}, []Field{{Name: "age", Value: 99}})
	if err == nil {
		t.Fatalf("expected BatchPatch decode error, got nil")
	}
	if !strings.Contains(err.Error(), "failed to decode existing value") {
		t.Fatalf("unexpected BatchPatch error: %v", err)
	}

	var afterRaw []byte
	if err := db.bolt.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.bucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		var keyBuf [8]byte
		v := b.Get(keycodec.UserKeyBytesWithBuf(uint64(1), db.strKey, &keyBuf))
		if v == nil {
			return fmt.Errorf("missing raw value for id=1 after failed batch")
		}
		afterRaw = append([]byte(nil), v...)
		return nil
	}); err != nil {
		t.Fatalf("snapshot raw(id=1) after: %v", err)
	}
	if !slices.Equal(beforeRaw, afterRaw) {
		t.Fatalf("id=1 raw payload changed despite rollback")
	}

	v1, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v1 == nil || v1.Age != 10 || v1.Name != "a" || v1.Country != "NL" {
		t.Fatalf("id=1 changed after failed BatchPatch: %#v", v1)
	}
}

func TestBatchDelete_DecodeError_RollsBackEarlierDeletes(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	if err := db.Set(1, &Rec{Name: "a", Age: 10, Meta: Meta{Country: "NL"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "b", Age: 20, Meta: Meta{Country: "DE"}}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	var beforeRaw []byte
	if err := db.bolt.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.bucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		var keyBuf [8]byte
		v := b.Get(keycodec.UserKeyBytesWithBuf(uint64(1), db.strKey, &keyBuf))
		if v == nil {
			return fmt.Errorf("missing raw value for id=1")
		}
		beforeRaw = append([]byte(nil), v...)
		return nil
	}); err != nil {
		t.Fatalf("snapshot raw(id=1): %v", err)
	}

	if err := db.bolt.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.bucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		var keyBuf [8]byte
		return b.Put(keycodec.UserKeyBytesWithBuf(uint64(2), db.strKey, &keyBuf), []byte{0xff, 0x00, 0x7f, 0x42})
	}); err != nil {
		t.Fatalf("corrupt id=2 payload: %v", err)
	}

	err := db.BatchDelete([]uint64{1, 2})
	if err == nil {
		t.Fatalf("expected BatchDelete decode error, got nil")
	}
	if !strings.Contains(err.Error(), "decode") {
		t.Fatalf("unexpected BatchDelete error: %v", err)
	}

	var afterRaw []byte
	if err := db.bolt.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.bucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		var keyBuf [8]byte
		v := b.Get(keycodec.UserKeyBytesWithBuf(uint64(1), db.strKey, &keyBuf))
		if v == nil {
			return fmt.Errorf("id=1 was deleted despite rollback")
		}
		afterRaw = append([]byte(nil), v...)
		return nil
	}); err != nil {
		t.Fatalf("snapshot raw(id=1) after: %v", err)
	}
	if !slices.Equal(beforeRaw, afterRaw) {
		t.Fatalf("id=1 raw payload changed despite rollback")
	}

	v1, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v1 == nil || v1.Name != "a" || v1.Age != 10 || v1.Country != "NL" {
		t.Fatalf("id=1 changed after failed BatchDelete: %#v", v1)
	}
}

func TestMultiWrite_CallbackError_RollbackDataAndIndex(t *testing.T) {
	type tc struct {
		name string
		run  func(db *DB[uint64, Rec], cb func(*bbolt.Tx, uint64, *Rec, *Rec) error) error
	}

	cases := []tc{
		{
			name: "batch_set",
			run: func(db *DB[uint64, Rec], cb func(*bbolt.Tx, uint64, *Rec, *Rec) error) error {
				return db.BatchSet(
					[]uint64{1, 2},
					[]*Rec{
						{Name: "new-1", Age: 100, Tags: []string{"x"}, Meta: Meta{Country: "PL"}},
						{Name: "new-2", Age: 200, Tags: []string{"y"}, Meta: Meta{Country: "DE"}},
					},
					BeforeCommit(cb),
				)
			},
		},
		{
			name: "batch_patch",
			run: func(db *DB[uint64, Rec], cb func(*bbolt.Tx, uint64, *Rec, *Rec) error) error {
				return db.BatchPatch(
					[]uint64{1, 2},
					[]Field{
						{Name: "age", Value: 777},
						{Name: "country", Value: "US"},
						{Name: "tags", Value: []string{"patched"}},
					},
					BeforeCommit(cb),
				)
			},
		},
		{
			name: "batch_delete",
			run: func(db *DB[uint64, Rec], cb func(*bbolt.Tx, uint64, *Rec, *Rec) error) error {
				return db.BatchDelete([]uint64{1, 2}, BeforeCommit(cb))
			},
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

			base := map[uint64]*Rec{
				1: {Name: "base-1", Age: 21, Tags: []string{"go"}, Meta: Meta{Country: "NL"}},
				2: {Name: "base-2", Age: 22, Tags: []string{"db"}, Meta: Meta{Country: "DE"}},
				3: {Name: "base-3", Age: 23, Tags: []string{"ops"}, Meta: Meta{Country: "PL"}},
			}
			for id, v := range base {
				cp := *v
				if err := db.Set(id, &cp); err != nil {
					t.Fatalf("seed Set(%d): %v", id, err)
				}
			}

			cbCalls := 0
			cb := func(_ *bbolt.Tx, _ uint64, _ *Rec, _ *Rec) error {
				cbCalls++
				return fmt.Errorf("callback fail")
			}

			err := c.run(db, cb)
			if err == nil || !strings.Contains(err.Error(), "callback fail") {
				t.Fatalf("expected callback error, got: %v", err)
			}
			if cbCalls == 0 {
				t.Fatalf("expected callback to be called at least once")
			}

			assertNoFutureSnapshotRefs(t, db)

			if cnt, err := db.Count(); err != nil {
				t.Fatalf("Count: %v", err)
			} else if cnt != 3 {
				t.Fatalf("expected Count=3 after rollback, got %d", cnt)
			}

			for id, exp := range base {
				v, err := db.Get(id)
				if err != nil {
					t.Fatalf("Get(%d): %v", id, err)
				}
				if v == nil {
					t.Fatalf("Get(%d): nil", id)
				}
				if v.Name != exp.Name || v.Age != exp.Age || v.Country != exp.Country || !slices.Equal(v.Tags, exp.Tags) {
					t.Fatalf("value changed after rollback for id=%d: got=%#v want=%#v", id, v, exp)
				}

				ids, err := db.QueryKeys(qx.Query(qx.EQ("name", exp.Name)))
				if err != nil {
					t.Fatalf("QueryKeys(name=%q): %v", exp.Name, err)
				}
				if len(ids) != 1 || ids[0] != id {
					t.Fatalf("name index mismatch after rollback for %q: %v", exp.Name, ids)
				}
			}
		})
	}
}

func TestMultiWrite_CallbackError_RandomizedAtomicRollback(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	r := newRand(20260327)
	countries := []string{"NL", "PL", "DE", "US"}
	names := []string{"alice", "bob", "carol", "dave", "eve"}
	tagPool := []string{"go", "db", "ops", "rust", "java"}

	randomRec := func(id uint64) *Rec {
		name := names[r.IntN(len(names))]
		return &Rec{
			Meta:     Meta{Country: countries[r.IntN(len(countries))]},
			Name:     fmt.Sprintf("%s-%03d-%02d", name, id, r.IntN(64)),
			Age:      18 + r.IntN(62),
			Score:    float64(r.IntN(1000)) / 10.0,
			Active:   r.IntN(2) == 0,
			Tags:     []string{tagPool[r.IntN(len(tagPool))], tagPool[r.IntN(len(tagPool))]},
			FullName: fmt.Sprintf("FN-%05d", id),
		}
	}

	for i := 1; i <= 70; i++ {
		id := uint64(i)
		if err := db.Set(id, randomRec(id)); err != nil {
			t.Fatalf("seed Set(%d): %v", id, err)
		}
	}

	snapshotState := func() (map[uint64]Rec, error) {
		out := make(map[uint64]Rec, 128)
		err := db.SeqScan(0, func(id uint64, rec *Rec) (bool, error) {
			if rec == nil {
				return false, fmt.Errorf("nil rec for id=%d", id)
			}
			out[id] = *rec
			return true, nil
		})
		return out, err
	}

	verifyStateEquals := func(label string, want map[uint64]Rec) {
		t.Helper()
		got, err := snapshotState()
		if err != nil {
			t.Fatalf("%s SeqScan: %v", label, err)
		}
		if len(got) != len(want) {
			t.Fatalf("%s size mismatch: got=%d want=%d", label, len(got), len(want))
		}
		for id, w := range want {
			g, ok := got[id]
			if !ok {
				t.Fatalf("%s missing id=%d", label, id)
			}
			if !reflect.DeepEqual(g, w) {
				t.Fatalf("%s value mismatch id=%d got=%#v want=%#v", label, id, g, w)
			}
		}
		// Extra check: index path should remain aligned with data for sampled names.
		checked := 0
		for id, rec := range want {
			ids, err := db.QueryKeys(qx.Query(qx.EQ("name", rec.Name)))
			if err != nil {
				t.Fatalf("%s QueryKeys(name=%q): %v", label, rec.Name, err)
			}
			found := false
			for _, x := range ids {
				if x == id {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("%s index drift for name=%q id=%d ids=%v", label, rec.Name, id, ids)
			}
			checked++
			if checked >= 18 {
				break
			}
		}
	}

	for step := 0; step < 80; step++ {
		before, err := snapshotState()
		if err != nil {
			t.Fatalf("step=%d snapshot before: %v", step, err)
		}

		failAt := 1
		cbCalls := 0
		cb := func(_ *bbolt.Tx, _ uint64, _ *Rec, _ *Rec) error {
			cbCalls++
			if cbCalls == failAt {
				return fmt.Errorf("callback fail at %d", failAt)
			}
			return nil
		}

		var opErr error
		switch step % 3 {
		case 0: // BatchSet with potential duplicate ids
			n := 3 + r.IntN(6)
			ids := make([]uint64, n)
			vals := make([]*Rec, n)
			for i := 0; i < n; i++ {
				id := uint64(1 + r.IntN(96))
				ids[i] = id
				vals[i] = randomRec(id)
			}
			opErr = db.BatchSet(ids, vals, BeforeCommit(cb))
		case 1: // BatchPatch
			n := 3 + r.IntN(6)
			ids := make([]uint64, n)
			for i := 0; i < n; i++ {
				ids[i] = uint64(1 + r.IntN(96))
			}
			patch := []Field{
				{Name: "age", Value: 18 + r.IntN(62)},
				{Name: "country", Value: countries[r.IntN(len(countries))]},
			}
			opErr = db.BatchPatch(ids, patch, BeforeCommit(cb))
		default: // BatchDelete
			n := 2 + r.IntN(6)
			ids := make([]uint64, n)
			for i := 0; i < n; i++ {
				ids[i] = uint64(1 + r.IntN(96))
			}
			opErr = db.BatchDelete(ids, BeforeCommit(cb))
		}

		if cbCalls == 0 {
			if opErr != nil {
				t.Fatalf("step=%d expected nil error when no callback was invoked, got: %v", step, opErr)
			}
		} else {
			if opErr == nil || !strings.Contains(opErr.Error(), "callback fail") {
				t.Fatalf("step=%d expected callback error, got: %v (cbCalls=%d)", step, opErr, cbCalls)
			}
		}
		assertNoFutureSnapshotRefs(t, db)

		verifyStateEquals(fmt.Sprintf("step=%d", step), before)
	}
}

func TestLargeBatch_AtomicFailure(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	if err := db.Set(1, &UniqueTestRec{Email: "exists@x", Code: 1}); err != nil {
		t.Fatal(err)
	}

	ids := []uint64{2, 3}
	vals := []*UniqueTestRec{
		{Email: "new@x", Code: 2},
		{Email: "exists@x", Code: 3}, // must fail
	}

	err := db.BatchSet(ids, vals)
	if err == nil {
		t.Fatal("Expected error in BatchSet")
	}

	// verify atomicity: id 2 should not exist
	v, err := db.Get(2)
	if err != nil {
		t.Fatal(err)
	}
	if v != nil {
		t.Fatal("BatchSet was not atomic: ID 2 was inserted despite batch failure")
	}
}

func TestBatchPatch_MissingIDs_CallbacksOnlyForExisting(t *testing.T) {
	db, _ := openTempDBStringProduct(t)

	if err := db.Set("p1", &Product{SKU: "p1", Price: 10}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	var called []string
	cb := func(tx *bbolt.Tx, key string, oldValue, newValue *Product) error {
		called = append(called, key)
		return nil
	}

	err := db.BatchPatch([]string{"missing", "p1", "missing2"}, []Field{{Name: "price", Value: 20.0}}, BeforeCommit(cb))
	if err != nil {
		t.Fatalf("BatchPatch: %v", err)
	}
	if len(called) != 1 || called[0] != "p1" {
		t.Fatalf("expected callback for p1 only, got: %v", called)
	}

	v, err := db.Get("p1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if v == nil || v.Price != 20.0 {
		t.Fatalf("expected price 20.0, got: %#v", v)
	}
}
