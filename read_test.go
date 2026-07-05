package rbi

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/rbierrors"
	"go.etcd.io/bbolt"
)

func TestAPI_Get_ReturnedRecordDetachedFromStore(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	opt := "keep"
	mustSetAPIRec(t, c, 1, &Rec{
		Name: "alice", Age: 30, Tags: []string{"go", "db"}, Opt: &opt,
	})

	got, err := readGet(c, 1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil {
		t.Fatalf("Get(1): got nil")
	}
	defer releaseUniqueRecords(c, got)

	got.Name = "mutated"
	got.Tags[0] = "changed"
	got.Opt = nil

	again, err := readGet(c, 1)
	if err != nil {
		t.Fatalf("Get(1) again: %v", err)
	}
	if again == nil {
		t.Fatalf("Get(1) again: got nil")
	}
	defer releaseUniqueRecords(c, again)

	if again.Name != "alice" || again.Tags[0] != "go" || again.Opt == nil || *again.Opt != "keep" {
		t.Fatalf("stored record was mutated through Get result: %#v", again)
	}
}

func TestAPI_ReadTxReadsUseBoundTransaction(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	if err := writeSet(c, 7, &Rec{Name: "committed", Age: 42}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	tx := BeginView()
	got, err := c.Get(tx, 7)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer tx.Release()
	defer releaseUniqueRecords(c, got)
	if got == nil || got.Name != "committed" || got.Age != 42 {
		t.Fatalf("Get got %#v", got)
	}
	missing, err := c.Get(tx, 99)
	if err != nil {
		t.Fatalf("Get missing: %v", err)
	}
	if missing != nil {
		t.Fatalf("Get missing got %#v", missing)
	}
	tx.Close()
}

func TestAPI_ReadQueryMethodsRequireTx(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	var nilRead *Tx
	if _, err := c.Get(nilRead, 1); !errors.Is(err, rbierrors.ErrNilTx) {
		t.Fatalf("Get nil tx err=%v, want ErrNilTx", err)
	}
	if err := c.ScanKeys(nilRead, 0, func(uint64) (bool, error) { return true, nil }); !errors.Is(err, rbierrors.ErrNilTx) {
		t.Fatalf("ScanKeys nil tx err=%v, want ErrNilTx", err)
	}
	if err := c.SeqScan(nilRead, 0, func(uint64, *Rec) (bool, error) { return true, nil }); !errors.Is(err, rbierrors.ErrNilTx) {
		t.Fatalf("SeqScan nil tx err=%v, want ErrNilTx", err)
	}
	if _, err := c.Query(nilRead, qx.Query()); !errors.Is(err, rbierrors.ErrNilTx) {
		t.Fatalf("Query nil tx err=%v, want ErrNilTx", err)
	}
	if _, err := c.QueryKeys(nilRead, qx.Query()); !errors.Is(err, rbierrors.ErrNilTx) {
		t.Fatalf("QueryKeys nil tx err=%v, want ErrNilTx", err)
	}

	var nilIndex *Tx
	if _, err := c.Count(nilIndex); !errors.Is(err, rbierrors.ErrNilTx) {
		t.Fatalf("Count nil tx err=%v, want ErrNilTx", err)
	}
	if _, err := c.Aggregate(nilIndex, qx.Aggregate(qx.ROWCOUNT().AS("rows"))); !errors.Is(err, rbierrors.ErrNilTx) {
		t.Fatalf("Aggregate nil tx err=%v, want ErrNilTx", err)
	}
}

func TestIOExt_Get_ReturnsDetachedCopy(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})
	ioExtMustSetRec(t, c, 1, &Rec{Name: "alice", Age: 30, Tags: []string{"go"}, Meta: Meta{Country: "NL"}})

	got := ioExtMustGetRec(t, c, 1)
	got.Name = "mutated"
	got.Tags[0] = "oops"
	got.Country = "US"

	again := ioExtMustGetRec(t, c, 1)
	if again.Name != "alice" || again.Tags[0] != "go" || again.Country != "NL" {
		t.Fatalf("stored value changed after mutating Get result: %#v", again)
	}
}

func TestReadPaths_MissingKeys_ReturnEmpty(t *testing.T) {
	c, _ := openTempCollectionStringProduct(t)

	if err := writeSet(c, "p1", &Product{SKU: "p1", Price: 10, Tags: []string{"a"}}); err != nil {
		t.Fatalf("Set(p1): %v", err)
	}
	if err := writeSet(c, "p2", &Product{SKU: "p2", Price: 20, Tags: []string{"b"}}); err != nil {
		t.Fatalf("Set(p2): %v", err)
	}

	if v, err := readGet(c, "missing-get"); err != nil {
		t.Fatalf("Get(missing): %v", err)
	} else if v != nil {
		t.Fatalf("Get(missing) expected nil, got %#v", v)
	}

	scanCalls := 0
	if err := readScanKeys(c, "zzzz", func(_ string) (bool, error) {
		scanCalls++
		return true, nil
	}); err != nil {
		t.Fatalf("ScanKeys: %v", err)
	}
	if scanCalls != 0 {
		t.Fatalf("ScanKeys expected 0 rows, got %d", scanCalls)
	}

	seenSeq := 0
	if err := readSeqScan(c, "zzzz", func(_ string, _ *Product) (bool, error) {
		seenSeq++
		return true, nil
	}); err != nil {
		t.Fatalf("SeqScan: %v", err)
	}
	if seenSeq != 0 {
		t.Fatalf("SeqScan expected 0 rows, got %d", seenSeq)
	}

	qMissing := qx.Query(qx.EQ("sku", "missing-sku"))
	if ids, err := readQueryKeys(c, qMissing); err != nil {
		t.Fatalf("QueryKeys(missing sku): %v", err)
	} else if len(ids) != 0 {
		t.Fatalf("QueryKeys(missing sku) expected empty, got %v", ids)
	}

	if items, err := readQuery(c, qMissing); err != nil {
		t.Fatalf("Query(missing sku): %v", err)
	} else if len(items) != 0 {
		t.Fatalf("Query(missing sku) expected empty, got %d", len(items))
	}

	if cnt, err := readCount(c, qMissing.Filter); err != nil {
		t.Fatalf("Count(missing sku): %v", err)
	} else if cnt != 0 {
		t.Fatalf("Count(missing sku) expected 0, got %d", cnt)
	}
}

func TestReadMethods_ReturnErrorWhenBucketMissing(t *testing.T) {
	c, _ := openTempUint64Collection(t)
	if err := writeSet(c, 1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := c.root.bolt.Update(func(tx *bbolt.Tx) error {
		if tx.Bucket(c.dataBucket) == nil {
			return nil
		}
		return tx.DeleteBucket(c.dataBucket)
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

	_, err := readGet(c, 1)
	expectBucketErr("Get", err)

	err = readSeqScan(c, 0, func(_ uint64, _ *Rec) (bool, error) {
		return true, nil
	})
	expectBucketErr("SeqScan", err)

	var scanKeysGot []uint64
	err = readScanKeys(c, 0, func(id uint64) (bool, error) {
		scanKeysGot = append(scanKeysGot, id)
		return true, nil
	})
	if err != nil {
		t.Fatalf("ScanKeys: %v", err)
	}
	if !slices.Equal(scanKeysGot, []uint64{1}) {
		t.Fatalf("ScanKeys=%v want indexed universe [1]", scanKeysGot)
	}

	_, err = readQuery(c, qx.Query())
	expectBucketErr("Query", err)
}

func TestQuery_NumericIndexMissingDataRecordReturnsError(t *testing.T) {
	c, _ := openTempUint64Collection(t)
	mustSetAPIRec(t, c, 1, &Rec{Name: "one", Age: 1})
	mustSetAPIRec(t, c, 2, &Rec{Name: "two", Age: 2})

	if err := c.root.bolt.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(c.dataBucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		var keyBuf [8]byte
		return b.Delete(keycodec.UserKeyBytesWithBuf(uint64(2), false, &keyBuf))
	}); err != nil {
		t.Fatalf("delete raw record: %v", err)
	}

	keys, err := readQueryKeys(c, qx.Query())
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if !slices.Equal(keys, []uint64{1, 2}) {
		t.Fatalf("QueryKeys=%v want [1 2]", keys)
	}
	if count, err := readCount(c); err != nil {
		t.Fatalf("Count: %v", err)
	} else if count != 2 {
		t.Fatalf("Count=%d want 2", count)
	}

	items, err := readQuery(c, qx.Query())
	if err == nil || !strings.Contains(err.Error(), "missing numeric data") {
		releaseUniqueRecords(c, items...)
		t.Fatalf("Query err=%v want missing numeric data", err)
	}
	if len(items) != 0 {
		releaseUniqueRecords(c, items...)
		t.Fatalf("Query returned %d items with error", len(items))
	}
}

func TestIOExt_Get_CorruptPayloadReturnsDecodeError(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})
	ioExtMustSetRec(t, c, 1, &Rec{Name: "good-1"})
	ioExtMustSetRec(t, c, 2, &Rec{Name: "good-2"})
	ioExtMustCorruptUint64Raw(t, c, 1, []byte{0xff, 0x00, 0x7f})

	if _, err := readGet(c, 1); err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("expected Get decode error, got %v", err)
	}
	v2 := ioExtMustGetRec(t, c, 2)
	if v2.Name != "good-2" {
		t.Fatalf("unexpected unaffected record: %#v", v2)
	}
}

func TestIOExt_SeqScan_CorruptPayloadStopsAtBadRecord(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})
	ioExtMustSetRec(t, c, 1, &Rec{Name: "one"})
	ioExtMustSetRec(t, c, 2, &Rec{Name: "two"})
	ioExtMustSetRec(t, c, 3, &Rec{Name: "three"})
	ioExtMustCorruptUint64Raw(t, c, 2, []byte{0xff, 0x00, 0x7f})

	var seen []uint64
	err := readSeqScan(c, 0, func(id uint64, _ *Rec) (bool, error) {
		seen = append(seen, id)
		return true, nil
	})
	if err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("expected SeqScan decode error, got %v", err)
	}
	if !slices.Equal(seen, []uint64{1}) {
		t.Fatalf("unexpected rows seen before decode error: %v", seen)
	}

	v3 := ioExtMustGetRec(t, c, 3)
	if v3.Name != "three" {
		t.Fatalf("unexpected id=3 after failed SeqScan: %#v", v3)
	}
}

func TestIOExt_SeqScan_InvalidFirstNumericKeyLengthReturnsError(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})
	ioExtMustSetRec(t, c, 1, &Rec{Name: "one"})
	raw := ioExtMustReadUint64Raw(t, c, 1)

	if err := c.root.bolt.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(c.dataBucket)
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

	err := readSeqScan(c, 0, func(uint64, *Rec) (bool, error) {
		t.Fatal("SeqScan callback called for invalid key")
		return true, nil
	})
	if err == nil || !strings.Contains(err.Error(), "invalid numeric data key length: 1") {
		t.Fatalf("SeqScan err=%v want invalid numeric key length", err)
	}
}

func TestIOExt_SeqScan_InvalidNextNumericKeyLengthReturnsError(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})
	ioExtMustSetRec(t, c, 1, &Rec{Name: "one"})
	raw := ioExtMustReadUint64Raw(t, c, 1)

	if err := c.root.bolt.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(c.dataBucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		return b.Put([]byte{1}, raw)
	}); err != nil {
		t.Fatalf("corrupt key: %v", err)
	}

	var seen []uint64
	err := readSeqScan(c, 0, func(id uint64, _ *Rec) (bool, error) {
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
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})
	ioExtMustSetRec(t, c, 1, &Rec{Name: "one"})
	ioExtMustSetRec(t, c, 2, &Rec{Name: "two"})
	ioExtMustSetRec(t, c, 3, &Rec{Name: "three"})
	corrupt := []byte{0xff, 0x00, 0x7f, 0x42}
	ioExtMustCorruptUint64Raw(t, c, 2, corrupt)

	got := make(map[uint64][]byte)
	if err := scanRawBolt(t, c, 0, func(id uint64, raw []byte) (bool, error) {
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
	c, _ := openTempUint64Collection(t)

	mustSetAPIRec(t, c, 1, &Rec{Name: "alice", Age: 30, Tags: []string{"go", "db"}})

	if err := readSeqScan(c, 0, func(id uint64, rec *Rec) (bool, error) {
		if id != 1 {
			return true, nil
		}
		rec.Name = "mutated"
		rec.Tags[0] = "changed"
		return false, nil
	}); err != nil {
		t.Fatalf("SeqScan: %v", err)
	}

	again, err := readGet(c, 1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	defer releaseUniqueRecords(c, again)
	if again == nil || again.Name != "alice" || again.Tags[0] != "go" {
		t.Fatalf("stored value changed after SeqScan callback mutation: %#v", again)
	}
}

func TestAPI_ScanKeys_StopOnFalse(t *testing.T) {
	c, _ := openTempUint64Collection(t)
	for i := 1; i <= 5; i++ {
		mustSetAPIRec(t, c, uint64(i), &Rec{Name: "x", Age: i})
	}

	var got []uint64
	err := readScanKeys(c, 2, func(id uint64) (bool, error) {
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
	c, _ := openTempUint64Collection(t)
	for i := 1; i <= 5; i++ {
		mustSetAPIRec(t, c, uint64(i), &Rec{Name: "x", Age: i})
	}

	sentinel := errors.New("scan stop")
	var calls int
	err := readScanKeys(c, 1, func(id uint64) (bool, error) {
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
	c, raw := openBoltAndCollection[scanKeysNamedUint64, Rec](t, t.TempDir()+"/named_uint64.db")
	t.Cleanup(func() {
		_ = c.Close()
		_ = raw.Close()
	})

	for i := 1; i <= 3; i++ {
		if err := writeSet(c, scanKeysNamedUint64(i), &Rec{Name: "x", Age: i}); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	var got []scanKeysNamedUint64
	err := readScanKeys(c, scanKeysNamedUint64(2), func(id scanKeysNamedUint64) (bool, error) {
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
	c, raw := openBoltAndCollection[scanKeysNamedString, Rec](t, t.TempDir()+"/named_string.db")
	t.Cleanup(func() {
		_ = c.Close()
		_ = raw.Close()
	})

	for i, id := range []scanKeysNamedString{"a", "b", "c"} {
		if err := writeSet(c, id, &Rec{Name: "x", Age: i}); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	var got []scanKeysNamedString
	err := readScanKeys(c, scanKeysNamedString("b"), func(id scanKeysNamedString) (bool, error) {
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
	c, _ := openTempUint64Collection(t)
	for i := 1; i <= 4; i++ {
		mustSetAPIRec(t, c, uint64(i), &Rec{Name: "x", Age: i})
	}

	var got []uint64
	err := readSeqScan(c, 0, func(id uint64, rec *Rec) (bool, error) {
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
	c, _ := openTempUint64Collection(t)
	for i := 1; i <= 4; i++ {
		mustSetAPIRec(t, c, uint64(i), &Rec{Name: "x", Age: i})
	}

	sentinel := errors.New("seqscan stop")
	var calls int
	err := readSeqScan(c, 0, func(id uint64, rec *Rec) (bool, error) {
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
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})
	ioExtMustSetRec(t, c, 1, &Rec{Name: "alice", Tags: []string{"go"}, Meta: Meta{Country: "NL"}})
	ioExtMustSetRec(t, c, 2, &Rec{Name: "bob", Tags: []string{"db"}, Meta: Meta{Country: "DE"}})

	if err := readSeqScan(c, 0, func(id uint64, v *Rec) (bool, error) {
		if id == 1 {
			v.Name = "mutated"
			v.Tags[0] = "oops"
			v.Country = "US"
		}
		return true, nil
	}); err != nil {
		t.Fatalf("SeqScan: %v", err)
	}

	again := ioExtMustGetRec(t, c, 1)
	if again.Name != "alice" || again.Tags[0] != "go" || again.Country != "NL" {
		t.Fatalf("stored value changed after mutating SeqScan value: %#v", again)
	}
}

func TestIOExt_SeqScan_StopOnFalse(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})
	for i := 1; i <= 3; i++ {
		ioExtMustSetRec(t, c, uint64(i), &Rec{Name: fmt.Sprintf("n%d", i)})
	}

	var seen []uint64
	if err := readSeqScan(c, 0, func(id uint64, _ *Rec) (bool, error) {
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
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})
	for i := 1; i <= 3; i++ {
		ioExtMustSetRec(t, c, uint64(i), &Rec{Name: fmt.Sprintf("n%d", i)})
	}

	wantErr := errors.New("stop scan")
	var seen []uint64
	err := readSeqScan(c, 0, func(id uint64, _ *Rec) (bool, error) {
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
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})
	for i := 1; i <= 3; i++ {
		ioExtMustSetRec(t, c, uint64(i), &Rec{Name: fmt.Sprintf("n%d", i)})
	}

	var seen []uint64
	if err := scanRawBolt(t, c, 0, func(id uint64, _ []byte) (bool, error) {
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
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})
	for i := 1; i <= 3; i++ {
		ioExtMustSetRec(t, c, uint64(i), &Rec{Name: fmt.Sprintf("n%d", i), Age: i})
	}

	got := make(map[uint64][]byte)
	if err := scanRawBolt(t, c, 0, func(id uint64, raw []byte) (bool, error) {
		got[id] = append([]byte(nil), raw...)
		return true, nil
	}); err != nil {
		t.Fatalf("raw bbolt scan: %v", err)
	}

	for i := 1; i <= 3; i++ {
		id := uint64(i)
		want := ioExtMustReadUint64Raw(t, c, id)
		if !slices.Equal(got[id], want) {
			t.Fatalf("raw payload mismatch for id=%d", id)
		}
	}
}

func TestIOExt_ScanKeys_StopOnFalse(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})
	for i := 1; i <= 5; i++ {
		ioExtMustSetRec(t, c, uint64(i), &Rec{Name: fmt.Sprintf("n%d", i)})
	}

	var seen []uint64
	if err := readScanKeys(c, 2, func(id uint64) (bool, error) {
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
	c, _ := openTempStringCollection(t, Options{BatchSoftLimit: 1})
	for _, id := range []string{"b-key", "a-key", "c-key"} {
		if err := writeSet(c, id, &Rec{Name: id}); err != nil {
			t.Fatalf("Set(%q): %v", id, err)
		}
	}

	wantErr := errors.New("stop keys")
	var seen []string
	err := readScanKeys(c, "b", func(id string) (bool, error) {
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
	c, _ := openTempUint64Collection(t)

	for i := 1; i <= 5; i++ {
		r := &Rec{Name: fmt.Sprintf("n%d", i), Age: i}
		if err := writeSet(c, uint64(i), r); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	var got []uint64
	err := readScanKeys(c, 3, func(id uint64) (bool, error) {
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
	c, _ := openTempUint64Collection(t, Options{Index: map[string]IndexKind{}, BatchSoftLimit: 1})

	for _, id := range []uint64{5, 1, 3, 2} {
		if err := writeSet(c, id, &Rec{Name: fmt.Sprintf("n%d", id), Age: int(id)}); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}

	var got []uint64
	err := readScanKeys(c, 2, func(id uint64) (bool, error) {
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
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})

	for _, id := range []uint64{1, 3} {
		if err := writeSet(c, id, &Rec{Name: fmt.Sprintf("n%d", id), Age: int(id)}); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}

	if err := c.root.bolt.Update(func(tx *bbolt.Tx) error {
		var key [8]byte
		return tx.Bucket(c.dataBucket).Put(keycodec.U64BytesWithBuf(2, &key), []byte{0xff})
	}); err != nil {
		t.Fatalf("raw Put: %v", err)
	}

	var got []uint64
	if err := readScanKeys(c, 0, func(id uint64) (bool, error) {
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
	if err := readScanKeys(c, 2, func(id uint64) (bool, error) {
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

func TestScanKeysUint64_IndexedAcceptsIndexView(t *testing.T) {
	c, _ := openTempUint64Collection(t, Options{BatchSoftLimit: 1})

	for _, id := range []uint64{1, 2} {
		if err := writeSet(c, id, &Rec{Name: fmt.Sprintf("n%d", id), Age: int(id)}); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}

	tx := BeginIndexView()
	var got []uint64
	if err := c.ScanKeys(tx, 0, func(id uint64) (bool, error) {
		got = append(got, id)
		return true, nil
	}); err != nil {
		t.Fatalf("ScanKeys index tx: %v", err)
	}
	if tx.boltTx != nil {
		t.Fatal("ScanKeys opened a bbolt read transaction for numeric index tx")
	}
	tx.Release()
	if !slices.Equal(got, []uint64{1, 2}) {
		t.Fatalf("ScanKeys index tx=%v want [1 2]", got)
	}

}

func TestScanKeysStringIndexAcceptsIndexView(t *testing.T) {
	c, _ := openTempCollectionStringProduct(t, Options{EnableStringKeyIndex: true, BatchSoftLimit: 1})
	if err := writeSet(c, "a", &Product{SKU: "a", Price: 1}); err != nil {
		t.Fatalf("Set(a): %v", err)
	}

	indexTx := BeginIndexView()
	var got []string
	if err := c.ScanKeys(indexTx, "", func(key string) (bool, error) {
		got = append(got, key)
		return true, nil
	}); err != nil {
		t.Fatalf("ScanKeys index tx: %v", err)
	}
	if indexTx.boltTx != nil {
		t.Fatal("string index ScanKeys opened a bbolt read transaction")
	}
	indexTx.Release()
	if !slices.Equal(got, []string{"a"}) {
		t.Fatalf("ScanKeys string index tx=%v want [a]", got)
	}

	readTx := BeginView()
	got = got[:0]
	if err := c.ScanKeys(readTx, "", func(key string) (bool, error) {
		got = append(got, key)
		return true, nil
	}); err != nil {
		t.Fatalf("ScanKeys read tx: %v", err)
	}
	if readTx.boltTx == nil {
		t.Fatal("string ScanKeys did not bind a bbolt read transaction")
	}
	readTx.Release()
	if !slices.Equal(got, []string{"a"}) {
		t.Fatalf("ScanKeys string read tx=%v want [a]", got)
	}
}

func TestScanKeys_String_SeekLowerBound(t *testing.T) {
	c, _ := openTempStringCollection(t)

	for i := 1; i <= 5; i++ {
		id := fmt.Sprintf("id-%02d", i)
		r := &Rec{Name: "x", Age: i}
		if err := writeSet(c, id, r); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	var got []string
	err := readScanKeys(c, "id-03", func(id string) (bool, error) {
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
