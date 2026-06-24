package rbi

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/keycodec"
	"go.etcd.io/bbolt"
)

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

func TestAPI_TxReadsUseCallerTransaction(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(7, &Rec{Name: "committed", Age: 42}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.Bolt().View(func(tx *bbolt.Tx) error {
		got, err := db.GetTx(tx, 7)
		if err != nil {
			return fmt.Errorf("GetTx: %w", err)
		}
		defer releaseUniqueRecords(db, got)
		if got == nil || got.Name != "committed" || got.Age != 42 {
			return fmt.Errorf("GetTx got %#v", got)
		}
		vals, err := db.BatchGetTx(tx, 7, 99)
		if err != nil {
			return fmt.Errorf("BatchGetTx: %w", err)
		}
		defer releaseUniqueRecords(db, vals...)
		if len(vals) != 2 || vals[0] == nil || vals[0].Name != "committed" || vals[1] != nil {
			return fmt.Errorf("BatchGetTx got %#v", vals)
		}
		return nil
	}); err != nil {
		t.Fatalf("fresh tx reads: %v", err)
	}
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

func TestReadPaths_MissingKeys_ReturnEmpty(t *testing.T) {
	db, _ := openTempDBStringProduct(t)

	if err := db.Set("p1", &Product{SKU: "p1", Price: 10, Tags: []string{"a"}}); err != nil {
		t.Fatalf("Set(p1): %v", err)
	}
	if err := db.Set("p2", &Product{SKU: "p2", Price: 20, Tags: []string{"b"}}); err != nil {
		t.Fatalf("Set(p2): %v", err)
	}

	if v, err := db.Get("missing-get"); err != nil {
		t.Fatalf("Get(missing): %v", err)
	} else if v != nil {
		t.Fatalf("Get(missing) expected nil, got %#v", v)
	}

	values, err := db.BatchGet("missing-1", "p1", "missing-2")
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	if len(values) != 3 || values[0] != nil || values[1] == nil || values[2] != nil {
		t.Fatalf("unexpected BatchGet result: %#v", values)
	}

	scanCalls := 0
	if err := db.ScanKeys("zzzz", func(_ string) (bool, error) {
		scanCalls++
		return true, nil
	}); err != nil {
		t.Fatalf("ScanKeys: %v", err)
	}
	if scanCalls != 0 {
		t.Fatalf("ScanKeys expected 0 rows, got %d", scanCalls)
	}

	seenSeq := 0
	if err := db.SeqScan("zzzz", func(_ string, _ *Product) (bool, error) {
		seenSeq++
		return true, nil
	}); err != nil {
		t.Fatalf("SeqScan: %v", err)
	}
	if seenSeq != 0 {
		t.Fatalf("SeqScan expected 0 rows, got %d", seenSeq)
	}

	qMissing := qx.Query(qx.EQ("sku", "missing-sku"))
	if ids, err := db.QueryKeys(qMissing); err != nil {
		t.Fatalf("QueryKeys(missing sku): %v", err)
	} else if len(ids) != 0 {
		t.Fatalf("QueryKeys(missing sku) expected empty, got %v", ids)
	}

	if items, err := db.Query(qMissing); err != nil {
		t.Fatalf("Query(missing sku): %v", err)
	} else if len(items) != 0 {
		t.Fatalf("Query(missing sku) expected empty, got %d", len(items))
	}

	if cnt, err := db.Count(qMissing.Filter); err != nil {
		t.Fatalf("Count(missing sku): %v", err)
	} else if cnt != 0 {
		t.Fatalf("Count(missing sku) expected 0, got %d", cnt)
	}
}

func TestReadMethods_ReturnErrorWhenBucketMissing(t *testing.T) {
	db, _ := openTempDBUint64(t)
	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.Bolt().Update(func(tx *bbolt.Tx) error {
		if tx.Bucket(db.BucketName()) == nil {
			return nil
		}
		return tx.DeleteBucket(db.BucketName())
	}); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}

	expectBucketErr := func(op string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s: expected error, got nil", op)
		}
		if !strings.Contains(err.Error(), "bucket does not exist") {
			t.Fatalf("%s: unexpected error: %v", op, err)
		}
	}

	_, err := db.Get(1)
	expectBucketErr("Get", err)

	_, err = db.BatchGet(1, 2)
	expectBucketErr("BatchGet", err)

	err = db.SeqScan(0, func(_ uint64, _ *Rec) (bool, error) {
		return true, nil
	})
	expectBucketErr("SeqScan", err)

	var scanKeysGot []uint64
	err = db.ScanKeys(0, func(id uint64) (bool, error) {
		scanKeysGot = append(scanKeysGot, id)
		return true, nil
	})
	if err != nil {
		t.Fatalf("ScanKeys: %v", err)
	}
	if !slices.Equal(scanKeysGot, []uint64{1}) {
		t.Fatalf("ScanKeys=%v want indexed universe [1]", scanKeysGot)
	}

	_, err = db.Query(qx.Query())
	expectBucketErr("Query", err)
}

func TestQuery_NumericIndexMissingDataRecordReturnsError(t *testing.T) {
	db, _ := openTempDBUint64(t)
	mustSetAPIRec(t, db, 1, &Rec{Name: "one", Age: 1})
	mustSetAPIRec(t, db, 2, &Rec{Name: "two", Age: 2})

	if err := db.Bolt().Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.dataBucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		var keyBuf [8]byte
		return b.Delete(keycodec.UserKeyBytesWithBuf(uint64(2), false, &keyBuf))
	}); err != nil {
		t.Fatalf("delete raw record: %v", err)
	}

	keys, err := db.QueryKeys(qx.Query())
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if !slices.Equal(keys, []uint64{1, 2}) {
		t.Fatalf("QueryKeys=%v want [1 2]", keys)
	}
	if count, err := db.Count(); err != nil {
		t.Fatalf("Count: %v", err)
	} else if count != 2 {
		t.Fatalf("Count=%d want 2", count)
	}

	items, err := db.Query(qx.Query())
	if err == nil || !strings.Contains(err.Error(), "missing numeric data") {
		releaseUniqueRecords(db, items...)
		t.Fatalf("Query err=%v want missing numeric data", err)
	}
	if len(items) != 0 {
		releaseUniqueRecords(db, items...)
		t.Fatalf("Query returned %d items with error", len(items))
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

func TestIOExt_SeqScan_InvalidFirstNumericKeyLengthReturnsError(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	ioExtMustSetRec(t, db, 1, &Rec{Name: "one"})
	raw := ioExtMustReadUint64Raw(t, db, 1)

	if err := db.Bolt().Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.dataBucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		var keyBuf [8]byte
		if err := b.Delete(keycodec.UserKeyBytesWithBuf(uint64(1), false, &keyBuf)); err != nil {
			return err
		}
		return b.Put([]byte{1}, raw)
	}); err != nil {
		t.Fatalf("corrupt key: %v", err)
	}

	err := db.SeqScan(0, func(uint64, *Rec) (bool, error) {
		t.Fatal("SeqScan callback called for invalid key")
		return true, nil
	})
	if err == nil || !strings.Contains(err.Error(), "invalid numeric data key length: 1") {
		t.Fatalf("SeqScan err=%v want invalid numeric key length", err)
	}
}

func TestIOExt_SeqScan_InvalidNextNumericKeyLengthReturnsError(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	ioExtMustSetRec(t, db, 1, &Rec{Name: "one"})
	raw := ioExtMustReadUint64Raw(t, db, 1)

	if err := db.Bolt().Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.dataBucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		return b.Put([]byte{1}, raw)
	}); err != nil {
		t.Fatalf("corrupt key: %v", err)
	}

	var seen []uint64
	err := db.SeqScan(0, func(id uint64, _ *Rec) (bool, error) {
		seen = append(seen, id)
		return true, nil
	})
	if err == nil || !strings.Contains(err.Error(), "invalid numeric data key length: 1") {
		t.Fatalf("SeqScan err=%v want invalid numeric key length", err)
	}
	if !slices.Equal(seen, []uint64{1}) {
		t.Fatalf("SeqScan seen=%v want [1]", seen)
	}
}

func TestIOExt_RawBoltScan_CorruptPayloadStillReturnsRawBytes(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	ioExtMustSetRec(t, db, 1, &Rec{Name: "one"})
	ioExtMustSetRec(t, db, 2, &Rec{Name: "two"})
	ioExtMustSetRec(t, db, 3, &Rec{Name: "three"})
	corrupt := []byte{0xff, 0x00, 0x7f, 0x42}
	ioExtMustCorruptUint64Raw(t, db, 2, corrupt)

	got := make(map[uint64][]byte)
	if err := scanRawBolt(t, db, 0, func(id uint64, raw []byte) (bool, error) {
		got[id] = append([]byte(nil), raw...)
		return true, nil
	}); err != nil {
		t.Fatalf("raw bbolt scan: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 rows from raw bbolt scan, got %d", len(got))
	}
	if !slices.Equal(got[2], corrupt) {
		t.Fatalf("raw bbolt scan did not return corrupt raw payload: %v", got[2])
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

type scanKeysNamedUint64 uint64

func TestAPI_ScanKeys_NamedUint64KeyType(t *testing.T) {
	db, raw := openBoltAndNew[scanKeysNamedUint64, Rec](t, t.TempDir()+"/named_uint64.db")
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	for i := 1; i <= 3; i++ {
		if err := db.Set(scanKeysNamedUint64(i), &Rec{Name: "x", Age: i}); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	var got []scanKeysNamedUint64
	err := db.ScanKeys(scanKeysNamedUint64(2), func(id scanKeysNamedUint64) (bool, error) {
		got = append(got, id)
		return true, nil
	})
	if err != nil {
		t.Fatalf("ScanKeys: %v", err)
	}
	if !slices.Equal(got, []scanKeysNamedUint64{2, 3}) {
		t.Fatalf("unexpected keys: %v", got)
	}
}

type scanKeysNamedString string

func TestAPI_ScanKeys_NamedStringKeyType(t *testing.T) {
	db, raw := openBoltAndNew[scanKeysNamedString, Rec](t, t.TempDir()+"/named_string.db")
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	for i, id := range []scanKeysNamedString{"a", "b", "c"} {
		if err := db.Set(id, &Rec{Name: "x", Age: i}); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	var got []scanKeysNamedString
	err := db.ScanKeys(scanKeysNamedString("b"), func(id scanKeysNamedString) (bool, error) {
		got = append(got, id)
		return true, nil
	})
	if err != nil {
		t.Fatalf("ScanKeys: %v", err)
	}
	if !slices.Equal(got, []scanKeysNamedString{"b", "c"}) {
		t.Fatalf("unexpected keys: %v", got)
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

func TestIOExt_RawBoltScan_StopOnFalse(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	for i := 1; i <= 3; i++ {
		ioExtMustSetRec(t, db, uint64(i), &Rec{Name: fmt.Sprintf("n%d", i)})
	}

	var seen []uint64
	if err := scanRawBolt(t, db, 0, func(id uint64, _ []byte) (bool, error) {
		seen = append(seen, id)
		return id != 2, nil
	}); err != nil {
		t.Fatalf("raw bbolt scan: %v", err)
	}
	if !slices.Equal(seen, []uint64{1, 2}) {
		t.Fatalf("unexpected raw bbolt scan stop set: %v", seen)
	}
}

func TestIOExt_RawBoltScan_MatchesPersistedPayloads(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	for i := 1; i <= 3; i++ {
		ioExtMustSetRec(t, db, uint64(i), &Rec{Name: fmt.Sprintf("n%d", i), Age: i})
	}

	got := make(map[uint64][]byte)
	if err := scanRawBolt(t, db, 0, func(id uint64, raw []byte) (bool, error) {
		got[id] = append([]byte(nil), raw...)
		return true, nil
	}); err != nil {
		t.Fatalf("raw bbolt scan: %v", err)
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
	if !slices.Equal(seen, []string{"b-key"}) {
		t.Fatalf("unexpected ScanKeys callback keys: %v", seen)
	}
}

func TestScanKeysUint64_SeekOrder(t *testing.T) {
	db, _ := openTempDBUint64(t)

	for i := 1; i <= 5; i++ {
		r := &Rec{Name: fmt.Sprintf("n%d", i), Age: i}
		if err := db.Set(uint64(i), r); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	var got []uint64
	err := db.ScanKeys(3, func(id uint64) (bool, error) {
		got = append(got, id)
		return true, nil
	})
	if err != nil {
		t.Fatalf("ScanKeys: %v", err)
	}

	want := []uint64{3, 4, 5}
	if !slices.Equal(want, got) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestScanKeysUint64_TransparentSeekOrder(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{Index: map[string]IndexKind{}, AutoBatchMax: 1})

	for _, id := range []uint64{5, 1, 3, 2} {
		if err := db.Set(id, &Rec{Name: fmt.Sprintf("n%d", id), Age: int(id)}); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}

	var got []uint64
	err := db.ScanKeys(2, func(id uint64) (bool, error) {
		got = append(got, id)
		return true, nil
	})
	if err != nil {
		t.Fatalf("ScanKeys: %v", err)
	}

	want := []uint64{2, 3, 5}
	if !slices.Equal(want, got) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestScanKeysUint64_IndexedUsesUniverse(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	for _, id := range []uint64{1, 3} {
		if err := db.Set(id, &Rec{Name: fmt.Sprintf("n%d", id), Age: int(id)}); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}

	if err := db.Bolt().Update(func(tx *bbolt.Tx) error {
		var key [8]byte
		return tx.Bucket(db.BucketName()).Put(keycodec.U64BytesWithBuf(2, &key), []byte{0xff})
	}); err != nil {
		t.Fatalf("raw Put: %v", err)
	}

	var got []uint64
	if err := db.ScanKeys(0, func(id uint64) (bool, error) {
		got = append(got, id)
		return true, nil
	}); err != nil {
		t.Fatalf("ScanKeys: %v", err)
	}

	want := []uint64{1, 3}
	if !slices.Equal(want, got) {
		t.Fatalf("ScanKeys=%v want universe %v", got, want)
	}

	got = got[:0]
	if err := db.ScanKeys(2, func(id uint64) (bool, error) {
		got = append(got, id)
		return true, nil
	}); err != nil {
		t.Fatalf("ScanKeys seek=2: %v", err)
	}
	want = []uint64{3}
	if !slices.Equal(want, got) {
		t.Fatalf("ScanKeys seek=2 %v want universe lower bound %v", got, want)
	}
}

func TestScanKeys_String_SeekLowerBound(t *testing.T) {
	db, _ := openTempDBString(t)

	for i := 1; i <= 5; i++ {
		id := fmt.Sprintf("id-%02d", i)
		r := &Rec{Name: "x", Age: i}
		if err := db.Set(id, r); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	var got []string
	err := db.ScanKeys("id-03", func(id string) (bool, error) {
		got = append(got, id)
		return true, nil
	})
	if err != nil {
		t.Fatalf("ScanKeys: %v", err)
	}
	want := []string{"id-03", "id-04", "id-05"}
	if !slices.Equal(got, want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}
