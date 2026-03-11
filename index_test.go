package rbi

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/vapstack/qx"
	"go.etcd.io/bbolt"
)

func TestReadString_InternsNilValue(t *testing.T) {
	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	if err := writeString(writer, nilValue); err != nil {
		t.Fatalf("writeString: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got, err := readString(bufio.NewReader(bytes.NewReader(payload.Bytes())))
	if err != nil {
		t.Fatalf("readString: %v", err)
	}
	if got != nilValue {
		t.Fatalf("readString mismatch: got %q", got)
	}
	if unsafe.StringData(got) != unsafe.StringData(nilValue) {
		t.Fatalf("readString must intern nilValue")
	}
}

func TestReadBitmap_CorruptedRoaringPayload_ReturnsMeaningfulError(t *testing.T) {
	payload := []byte{8, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	_, err := readBitmap(bufio.NewReader(bytes.NewReader(payload)))
	if err == nil {
		t.Fatal("expected corrupted bitmap error")
	}
	if !strings.Contains(err.Error(), "corrupted roaring64 bitmap payload") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReadStrMap_RejectsSparseIndexes(t *testing.T) {
	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	if err := writeUvarint(writer, 1); err != nil {
		t.Fatalf("write map count: %v", err)
	}
	if err := writeString(writer, "k"); err != nil {
		t.Fatalf("write key: %v", err)
	}
	sparseIdx := uint64(maxStoredStrMapSparseSlack*2 + 1)
	if err := writeUvarint(writer, sparseIdx); err != nil {
		t.Fatalf("write sparse index: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	_, err := readStrMap(bufio.NewReader(bytes.NewReader(payload.Bytes())), 0)
	if err == nil {
		t.Fatalf("expected sparse strmap load error")
	}
	if !strings.Contains(err.Error(), "strmap index too sparse") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func toBM(ids ...uint64) *roaring64.Bitmap {
	r := roaring64.NewBitmap()
	r.AddMany(ids)
	return r
}

func toPosting(ids ...uint64) postingList {
	return postingFromBitmapViewAdaptive(toBM(ids...))
}

func TestFieldIndexOverlay_LookupCompose(t *testing.T) {
	base := []index{
		{Key: indexKeyFromString("a"), IDs: toPosting(1, 2)},
		{Key: indexKeyFromString("c"), IDs: toPosting(10)},
	}
	delta := &fieldIndexDelta{
		keys: []string{"b", "c", "d"},
		byKey: map[string]indexDeltaEntry{
			"b": {add: toBM(3)},
			"c": {add: toBM(11), del: toBM(10)},
			"d": {del: toBM(777)},
		},
	}
	ov := newFieldOverlay(&base, delta)

	if got, _ := ov.lookupOwned("a", nil); !slices.Equal(got.ToArray(), []uint64{1, 2}) {
		t.Fatalf("lookup a mismatch: %v", got.ToArray())
	}
	if got, _ := ov.lookupOwned("b", nil); !slices.Equal(got.ToArray(), []uint64{3}) {
		t.Fatalf("lookup b mismatch: %v", got.ToArray())
	}
	if got, _ := ov.lookupOwned("c", nil); !slices.Equal(got.ToArray(), []uint64{11}) {
		t.Fatalf("lookup c mismatch: %v", got.ToArray())
	}
	if got, _ := ov.lookupOwned("d", nil); got != nil {
		t.Fatalf("lookup d expected nil/empty, got: %v", got.ToArray())
	}
	if got, _ := ov.lookupOwned("z", nil); got != nil {
		t.Fatalf("lookup z expected nil, got: %v", got.ToArray())
	}
}

func TestFieldIndexOverlay_CursorMergedKeys_ASC_DESC(t *testing.T) {
	base := []index{
		{Key: indexKeyFromString("a"), IDs: toPosting(1)},
		{Key: indexKeyFromString("c"), IDs: toPosting(3)},
		{Key: indexKeyFromString("e"), IDs: toPosting(5)},
	}
	delta := &fieldIndexDelta{
		keys: []string{"b", "c", "d", "f"},
		byKey: map[string]indexDeltaEntry{
			"b": {add: toBM(20)},
			"c": {add: toBM(30), del: toBM(3)},
			"d": {del: toBM(999)},
			"f": {add: toBM(6)},
		},
	}
	ov := newFieldOverlay(&base, delta)
	full := rangeBounds{has: true}

	asc := collectOverlayKeys(ov, full, false)
	if !slices.Equal(asc, []string{"a", "b", "c", "e", "f"}) {
		t.Fatalf("asc keys mismatch: %v", asc)
	}

	desc := collectOverlayKeys(ov, full, true)
	if !slices.Equal(desc, []string{"f", "e", "c", "b", "a"}) {
		t.Fatalf("desc keys mismatch: %v", desc)
	}
}

func TestFieldIndexOverlay_RangeAndPrefix(t *testing.T) {
	base := []index{
		{Key: indexKeyFromString("aa"), IDs: toPosting(1)},
		{Key: indexKeyFromString("ac"), IDs: toPosting(2)},
		{Key: indexKeyFromString("b1"), IDs: toPosting(3)},
	}
	delta := &fieldIndexDelta{
		keys: []string{"ab", "ac", "ad", "b0"},
		byKey: map[string]indexDeltaEntry{
			"ab": {add: toBM(10)},
			"ac": {del: toBM(2)},
			"ad": {add: toBM(11)},
			"b0": {add: toBM(12)},
		},
	}
	ov := newFieldOverlay(&base, delta)

	rb := rangeBounds{
		has:   true,
		hasLo: true,
		loKey: "ab",
		loInc: true,
		hasHi: true,
		hiKey: "ad",
		hiInc: true,
	}
	keys := collectOverlayKeys(ov, rb, false)
	if !slices.Equal(keys, []string{"ab", "ad"}) {
		t.Fatalf("range keys mismatch: %v", keys)
	}

	pb := rangeBounds{
		has:       true,
		hasPrefix: true,
		prefix:    "a",
	}
	pkeys := collectOverlayKeys(ov, pb, false)
	if !slices.Equal(pkeys, []string{"aa", "ab", "ad"}) {
		t.Fatalf("prefix keys mismatch: %v", pkeys)
	}
}

func TestMaterializeFieldOverlay_PreservesFixed8Keys(t *testing.T) {
	k1 := uint64ByteStr(1)
	k2 := uint64ByteStr(2)
	k3 := uint64ByteStr(3)

	base := []index{
		{Key: indexKeyFromU64(1), IDs: toPosting(100)},
		{Key: indexKeyFromU64(2), IDs: toPosting(200)},
	}
	delta := &fieldIndexDelta{
		byKey: map[string]indexDeltaEntry{
			k1: {add: toBM(101)},
			k3: {add: toBM(300)},
		},
		fixed8: true,
	}

	out := materializeFieldOverlay(newFieldOverlay(&base, delta))
	if out == nil || len(*out) == 0 {
		t.Fatalf("materialized overlay is empty")
	}

	var (
		seen1 bool
		seen3 bool
	)
	for _, ix := range *out {
		switch {
		case indexKeyEqualsString(ix.Key, k1):
			seen1 = true
			if !ix.Key.isNumeric() {
				t.Fatalf("existing fixed8 key must stay numeric")
			}
		case indexKeyEqualsString(ix.Key, k2):
			if !ix.Key.isNumeric() {
				t.Fatalf("base fixed8 key must stay numeric")
			}
		case indexKeyEqualsString(ix.Key, k3):
			seen3 = true
			if !ix.Key.isNumeric() {
				t.Fatalf("delta-only fixed8 key must materialize as numeric")
			}
		}
	}
	if !seen1 || !seen3 {
		t.Fatalf("missing expected keys in materialized overlay: seen1=%v seen3=%v", seen1, seen3)
	}
}

func TestFieldIndexDelta_SortedKeysSingleton_Concurrent(t *testing.T) {
	d := &fieldIndexDelta{
		singleKey:   "k",
		singleEntry: indexDeltaEntry{addSingle: 1, addSingleSet: true},
		singleSet:   true,
	}

	const goroutines = 16
	const iters = 1000

	var wg sync.WaitGroup
	var bad atomic.Bool
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				if bad.Load() {
					return
				}
				keys := d.sortedKeys()
				if len(keys) != 1 || keys[0] != "k" {
					bad.Store(true)
					return
				}
			}
		}()
	}
	wg.Wait()
	if bad.Load() {
		t.Fatalf("unexpected singleton keys under concurrent access")
	}
}

func collectOverlayKeys(ov fieldOverlay, b rangeBounds, desc bool) []string {
	span := ov.rangeForBounds(b)
	cur := ov.newCursor(span, desc)
	scratch := roaring64.NewBitmap()
	out := make([]string, 0, 16)

	for {
		key, baseBM, de, ok := cur.next()
		if !ok {
			break
		}
		bm := composePosting(baseBM, de, scratch)
		if bm == nil || bm.IsEmpty() {
			continue
		}
		out = append(out, key.asString())
	}
	return out
}

func TestApplyFieldDelta_NewAndUpdateAndRemove(t *testing.T) {
	prev := &fieldIndexDelta{
		keys: []string{"b", "d"},
		byKey: map[string]indexDeltaEntry{
			"b": {add: toBM(2), del: toBM(20)},
			"d": {del: toBM(40)},
		},
	}

	changes := map[string]indexDeltaEntry{
		"a": {add: toBM(1)},
		"b": {add: toBM(3), del: toBM(2)}, // b: add {2,3}, del {20,2} => add {3}, del {20}
		"d": {add: toBM(40)},              // cancel existing del
	}

	next := applyFieldDelta(prev, changes)
	if next == nil {
		t.Fatalf("next delta is nil")
	}

	if !slices.Equal(next.sortedKeys(), []string{"a", "b"}) {
		t.Fatalf("keys mismatch: %v", next.sortedKeys())
	}

	aEntry, ok := next.get("a")
	if !ok {
		t.Fatalf("missing key a")
	}
	if a := deltaEntryAddToArray(aEntry); !slices.Equal(a, []uint64{1}) {
		t.Fatalf("a.add mismatch: %v", a)
	}
	b, ok := next.get("b")
	if !ok {
		t.Fatalf("missing key b")
	}
	if a := deltaEntryAddToArray(b); !slices.Equal(a, []uint64{3}) {
		t.Fatalf("b.add mismatch: %v", a)
	}
	if d := deltaEntryDelToArray(b); !slices.Equal(d, []uint64{20}) {
		t.Fatalf("b.del mismatch: %v", d)
	}
	if _, ok := next.get("d"); ok {
		t.Fatalf("d must be removed after cancellation")
	}
}

func TestApplyFieldDelta_DoesNotMutatePrev(t *testing.T) {
	prev := &fieldIndexDelta{
		keys: []string{"x"},
		byKey: map[string]indexDeltaEntry{
			"x": {add: toBM(10), del: toBM(11)},
		},
	}

	_ = applyFieldDelta(prev, map[string]indexDeltaEntry{
		"x": {add: toBM(12), del: toBM(10)},
	})

	if !slices.Equal(prev.sortedKeys(), []string{"x"}) {
		t.Fatalf("prev keys mutated: %v", prev.sortedKeys())
	}
	x, ok := prev.get("x")
	if !ok {
		t.Fatalf("missing key x")
	}
	if a := deltaEntryAddToArray(x); !slices.Equal(a, []uint64{10}) {
		t.Fatalf("prev add mutated: %v", a)
	}
	if d := deltaEntryDelToArray(x); !slices.Equal(d, []uint64{11}) {
		t.Fatalf("prev del mutated: %v", d)
	}
}

func TestApplyFieldDelta_EmptyChangesReturnsPrev(t *testing.T) {
	prev := &fieldIndexDelta{
		keys: []string{"x"},
		byKey: map[string]indexDeltaEntry{
			"x": {add: toBM(1)},
		},
	}
	next := applyFieldDelta(prev, nil)
	if next != prev {
		t.Fatalf("expected same pointer for empty changes")
	}
}

func deltaEntryAddToArray(e indexDeltaEntry) []uint64 {
	if e.addSingleSet {
		return []uint64{e.addSingle}
	}
	if e.add == nil {
		return nil
	}
	return e.add.ToArray()
}

func deltaEntryDelToArray(e indexDeltaEntry) []uint64 {
	if e.delSingleSet {
		return []uint64{e.delSingle}
	}
	if e.del == nil {
		return nil
	}
	return e.del.ToArray()
}
func TestIndexPersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persist.db")

	db, raw := openBoltAndNew[uint64, Rec](t, path)

	if err := db.Set(1, &Rec{Name: "alice", Age: 10, Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(2, &Rec{Name: "bob", Age: 20, Tags: []string{"java"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}

	db2, raw2 := openBoltAndNew[uint64, Rec](t, path)
	t.Cleanup(func() {
		if err := db2.Close(); err != nil {
			t.Fatal(err)
		}
		if err := raw2.Close(); err != nil {
			t.Fatal(err)
		}
	})

	ids, err := db2.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "name", Value: "alice"}})
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("expected [1], got %v", ids)
	}

	st := db2.Stats()
	if st.Index.KeyCount != 2 {
		t.Fatalf("expected Stats.Index.KeyCount=2, got %d", st.Index.KeyCount)
	}
}

func TestIndexPersistence_LenZeroComplement_AllEmptyAfterReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persist_len_zero_complement_all_empty.db")

	db, raw := openBoltAndNew[uint64, Rec](t, path)
	for i := 1; i <= 90; i++ {
		rec := &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Email: fmt.Sprintf("u_%d@example.test", i),
			Age:   i,
		}
		if i%5 == 0 {
			rec.Tags = []string{"go"}
		}
		if err := db.Set(uint64(i), rec); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}
	if !db.isLenZeroComplementField("tags") {
		t.Fatalf("expected zero-complement mode before patching to empty")
	}

	for i := 1; i <= 90; i++ {
		if err := db.Patch(uint64(i), []Field{{Name: "tags", Value: []string(nil)}}); err != nil {
			t.Fatalf("Patch(%d): %v", i, err)
		}
	}

	want := make([]uint64, 0, 90)
	for i := 1; i <= 90; i++ {
		want = append(want, uint64(i))
	}
	gotBeforeClose, err := db.QueryKeys(qx.Query(qx.EQ("tags", []string{})))
	if err != nil {
		t.Fatalf("QueryKeys(empty tags) before close: %v", err)
	}
	if !slices.Equal(gotBeforeClose, want) {
		t.Fatalf("unexpected empty-tags ids before reopen: got=%v want=%v", gotBeforeClose, want)
	}

	if err = db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err = raw.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}

	db2, raw2 := openBoltAndNew[uint64, Rec](t, path)
	t.Cleanup(func() {
		if err = db2.Close(); err != nil {
			t.Fatal(err)
		}
		if err = raw2.Close(); err != nil {
			t.Fatal(err)
		}
	})

	gotAfterReopen, err := db2.QueryKeys(qx.Query(qx.EQ("tags", []string{})))
	if err != nil {
		t.Fatalf("QueryKeys(empty tags) after reopen: %v", err)
	}
	if !slices.Equal(gotAfterReopen, want) {
		t.Fatalf("unexpected empty-tags ids after reopen: got=%v want=%v", gotAfterReopen, want)
	}
}

func TestRebuildIndex_CleanState(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(2, &Rec{Name: "bob", Age: 30}); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(3, &Rec{Name: "charlie", Age: 30}); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(2); err != nil {
		t.Fatal(err)
	}
	cnt, err := db.Count(&qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "age", Value: 30}})
	if err != nil {
		t.Fatal(err)
	}
	if cnt != 2 {
		t.Fatalf("before rebuild: expected 2, got %d", cnt)
	}

	if err = db.RebuildIndex(); err != nil {
		t.Fatal(err)
	}

	// verify state (1, 3)
	ids, err := db.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "age", Value: 30}})
	if err != nil {
		t.Fatal(err)
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 3 {
		t.Fatalf("after rebuild: expected [1, 3], got %v", ids)
	}
}

func TestRebuildIndex_ClosedReturnsErrClosed(t *testing.T) {
	db, _ := openTempDBUint64(t)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := db.RebuildIndex(); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got: %v", err)
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

	err = db.SeqScanRaw(0, func(_ uint64, _ []byte) (bool, error) {
		return true, nil
	})
	expectBucketErr("SeqScanRaw", err)

	_, err = db.Query(qx.Query())
	expectBucketErr("Query", err)
}

func TestQuery_MissingBucket_EmptyIndexResultSkipsBucketRead(t *testing.T) {
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

	items, err := db.Query(qx.Query(qx.EQ("age", 999_999)))
	if err != nil {
		t.Fatalf("Query(empty result on missing bucket): %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("expected empty items, got: %#v", items)
	}
}

func TestRebuildIndex_ScanErrorClearsActiveFlag(t *testing.T) {
	db, _ := openTempDBUint64(t)
	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.Bolt().Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.BucketName())
		if b == nil {
			return fmt.Errorf("bucket missing")
		}
		return b.Put(db.keyFromID(1), []byte{0xff})
	}); err != nil {
		t.Fatalf("corrupt value: %v", err)
	}

	err := db.RebuildIndex()
	if err == nil {
		t.Fatal("expected rebuild error on corrupted value")
	}
	if !strings.Contains(err.Error(), "scan error") {
		t.Fatalf("expected scan error, got: %v", err)
	}

	// Rebuild must clear active flag even when build fails.
	if err := db.Set(2, &Rec{Name: "bob", Age: 31}); err != nil {
		t.Fatalf("Set after failed rebuild should not see busy flag, got: %v", err)
	}

	err = db.RebuildIndex()
	if err == nil || !strings.Contains(err.Error(), "scan error") {
		t.Fatalf("second RebuildIndex: expected scan error, got: %v", err)
	}
}

func TestRebuildIndex_StopTheWorld(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	scanStarted := make(chan struct{})
	releaseScan := make(chan struct{})
	scanDone := make(chan error, 1)

	go func() {
		err := db.SeqScan(0, func(_ uint64, _ *Rec) (bool, error) {
			close(scanStarted)
			<-releaseScan
			return false, nil
		})
		scanDone <- err
	}()

	select {
	case <-scanStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("scan did not start in time")
	}

	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- db.RebuildIndex()
	}()

	deadline := time.Now().Add(2 * time.Second)
	sawRebuildBusy := false
	for time.Now().Before(deadline) {
		_, err := db.Get(1)
		if errors.Is(err, ErrRebuildInProgress) {
			sawRebuildBusy = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !sawRebuildBusy {
		t.Fatal("expected ErrRebuildInProgress while rebuild waits for in-flight reader")
	}

	select {
	case err := <-rebuildDone:
		t.Fatalf("rebuild must wait for in-flight reader, got early result: %v", err)
	default:
	}

	close(releaseScan)
	if err := <-scanDone; err != nil {
		t.Fatalf("SeqScan: %v", err)
	}
	if err := <-rebuildDone; err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}
}

func TestRebuildIndex_ConcurrentCallReturnsErrRebuildInProgress(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	scanStarted := make(chan struct{})
	releaseScan := make(chan struct{})
	scanDone := make(chan error, 1)

	go func() {
		err := db.SeqScan(0, func(_ uint64, _ *Rec) (bool, error) {
			close(scanStarted)
			<-releaseScan
			return false, nil
		})
		scanDone <- err
	}()

	select {
	case <-scanStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("scan did not start in time")
	}

	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- db.RebuildIndex()
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, err := db.Get(1)
		if errors.Is(err, ErrRebuildInProgress) {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	if err := db.RebuildIndex(); !errors.Is(err, ErrRebuildInProgress) {
		close(releaseScan)
		<-scanDone
		<-rebuildDone
		t.Fatalf("expected ErrRebuildInProgress from concurrent rebuild, got: %v", err)
	}

	close(releaseScan)
	if err := <-scanDone; err != nil {
		t.Fatalf("SeqScan: %v", err)
	}
	if err := <-rebuildDone; err != nil {
		t.Fatalf("RebuildIndex(first): %v", err)
	}
}

func TestRebuildIndex_StopsTruncateWhileActive(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	scanStarted := make(chan struct{})
	releaseScan := make(chan struct{})
	scanDone := make(chan error, 1)

	go func() {
		err := db.SeqScan(0, func(_ uint64, _ *UniqueTestRec) (bool, error) {
			close(scanStarted)
			<-releaseScan
			return false, nil
		})
		scanDone <- err
	}()

	select {
	case <-scanStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("scan did not start in time")
	}

	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- db.RebuildIndex()
	}()

	deadline := time.Now().Add(2 * time.Second)
	sawBusy := false
	for time.Now().Before(deadline) {
		_, err := db.Get(1)
		if errors.Is(err, ErrRebuildInProgress) {
			sawBusy = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !sawBusy {
		close(releaseScan)
		<-scanDone
		<-rebuildDone
		t.Fatal("expected rebuild to become active")
	}

	if err := db.Truncate(); !errors.Is(err, ErrRebuildInProgress) {
		close(releaseScan)
		<-scanDone
		<-rebuildDone
		t.Fatalf("expected ErrRebuildInProgress from Truncate during rebuild, got: %v", err)
	}

	close(releaseScan)
	if err := <-scanDone; err != nil {
		t.Fatalf("SeqScan: %v", err)
	}
	if err := <-rebuildDone; err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	if err := db.Truncate(); err != nil {
		t.Fatalf("Truncate(after rebuild): %v", err)
	}
}

func TestRebuildIndex_RejectsSetWhileActive(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	scanStarted := make(chan struct{})
	releaseScan := make(chan struct{})
	scanDone := make(chan error, 1)

	go func() {
		err := db.SeqScan(0, func(_ uint64, _ *Rec) (bool, error) {
			close(scanStarted)
			<-releaseScan
			return false, nil
		})
		scanDone <- err
	}()

	select {
	case <-scanStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("scan did not start in time")
	}

	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- db.RebuildIndex()
	}()

	deadline := time.Now().Add(2 * time.Second)
	sawBusy := false
	for time.Now().Before(deadline) {
		_, err := db.Get(1)
		if errors.Is(err, ErrRebuildInProgress) {
			sawBusy = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !sawBusy {
		close(releaseScan)
		<-scanDone
		<-rebuildDone
		t.Fatal("expected rebuild to become active")
	}

	if err := db.Set(2, &Rec{Name: "bob", Age: 31}); !errors.Is(err, ErrRebuildInProgress) {
		close(releaseScan)
		<-scanDone
		<-rebuildDone
		t.Fatalf("expected ErrRebuildInProgress from Set during rebuild, got: %v", err)
	}

	close(releaseScan)
	if err := <-scanDone; err != nil {
		t.Fatalf("SeqScan: %v", err)
	}
	if err := <-rebuildDone; err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	if err := db.Set(2, &Rec{Name: "bob", Age: 31}); err != nil {
		t.Fatalf("Set(after rebuild): %v", err)
	}
}

func TestRebuildIndex_ConcurrentCloseReturnsErrClosed(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	scanStarted := make(chan struct{})
	releaseScan := make(chan struct{})
	scanDone := make(chan error, 1)

	go func() {
		err := db.SeqScan(0, func(_ uint64, _ *Rec) (bool, error) {
			close(scanStarted)
			<-releaseScan
			return false, nil
		})
		scanDone <- err
	}()

	select {
	case <-scanStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("scan did not start in time")
	}

	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- db.RebuildIndex()
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, err := db.Get(1)
		if errors.Is(err, ErrRebuildInProgress) {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- db.Close()
	}()

	closedDeadline := time.Now().Add(2 * time.Second)
	for !db.closed.Load() && time.Now().Before(closedDeadline) {
		time.Sleep(1 * time.Millisecond)
	}
	if !db.closed.Load() {
		close(releaseScan)
		<-scanDone
		<-closeDone
		<-rebuildDone
		t.Fatal("db.closed was not set by Close in time")
	}

	close(releaseScan)
	if err := <-scanDone; err != nil {
		t.Fatalf("SeqScan: %v", err)
	}

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close timed out")
	}

	select {
	case err := <-rebuildDone:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("expected ErrClosed from rebuild racing with close, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RebuildIndex timed out")
	}
}

func TestRebuildIndex_WaitsForInFlightBatchedSet(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		BatchWindow:         5 * time.Millisecond,
		BatchMax:            16,
		BatchMaxQueue:       1024,
		BatchAllowCallbacks: true,
	})

	setStarted := make(chan struct{})
	releaseSet := make(chan struct{})
	setDone := make(chan error, 1)

	go func() {
		setDone <- db.Set(1, &Rec{Name: "alice", Age: 30}, func(_ *bbolt.Tx, _ uint64, _, _ *Rec) error {
			close(setStarted)
			<-releaseSet
			return nil
		})
	}()

	select {
	case <-setStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("batched set callback did not start in time")
	}

	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- db.RebuildIndex()
	}()

	deadline := time.Now().Add(2 * time.Second)
	sawBusy := false
	for time.Now().Before(deadline) {
		_, err := db.Get(1)
		if errors.Is(err, ErrRebuildInProgress) {
			sawBusy = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !sawBusy {
		close(releaseSet)
		<-setDone
		<-rebuildDone
		t.Fatal("expected rebuild to become active while batched set is in flight")
	}

	select {
	case err := <-rebuildDone:
		close(releaseSet)
		<-setDone
		t.Fatalf("rebuild must wait for in-flight batched set, got early result: %v", err)
	default:
	}

	close(releaseSet)
	if err := <-setDone; err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := <-rebuildDone; err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	if st := db.BatchStats(); st.Enqueued == 0 {
		t.Fatalf("expected write-combine enqueue for Set path, got stats: %+v", st)
	}

	v, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v == nil || v.Age != 30 || v.Name != "alice" {
		t.Fatalf("unexpected value after rebuild: %#v", v)
	}

	ids, err := db.QueryKeys(qx.Query(qx.EQ("age", 30)))
	if err != nil {
		t.Fatalf("QueryKeys(age=30): %v", err)
	}
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("expected [1] after batched set + rebuild, got %v", ids)
	}
}

func TestRebuildIndex_StormConcurrentMixedOps_FinalConsistency(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		BatchWindow:   2 * time.Millisecond,
		BatchMax:      16,
		BatchMaxQueue: 2048,
	})

	countries := []string{"NL", "PL", "DE", "US"}
	for i := 1; i <= 220; i++ {
		id := uint64(i)
		if err := db.Set(id, &Rec{
			Meta:     Meta{Country: countries[i%len(countries)]},
			Name:     fmt.Sprintf("seed-%03d", i),
			Age:      18 + (i % 60),
			Score:    float64(i%1000) / 10.0,
			Active:   i%2 == 0,
			Tags:     []string{fmt.Sprintf("grp-%d", i%7), "seed"},
			FullName: fmt.Sprintf("FN-%05d", i),
		}); err != nil {
			t.Fatalf("seed Set(%d): %v", i, err)
		}
	}

	const (
		writers   = 4
		readers   = 3
		writerOps = 220
		readerOps = 320
		rebuilds  = 48
	)

	queries := []*qx.QX{
		qx.Query(qx.GTE("age", 25)).By("age", qx.ASC).Skip(2).Max(40),
		qx.Query(qx.NOT(qx.EQ("active", false))).ByArrayCount("tags", qx.DESC).Skip(1).Max(55),
		qx.Query(qx.HASANY("tags", []string{"seed", "w0", "w1", "grp-2"})).ByArrayPos("country", []string{"NL", "DE", "PL", "US"}, qx.ASC).Max(70),
		qx.Query(qx.PREFIX("name", "rw-")).By("name", qx.ASC).Max(80),
		qx.Query(qx.IN("country", []string{"NL", "DE"})).ByArrayPos("tags", []string{"seed", "w0", "grp-1"}, qx.DESC).Skip(3).Max(60),
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 1+writers+readers)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rebuilds; i++ {
			if err := db.RebuildIndex(); err != nil {
				errCh <- fmt.Errorf("rebuild iter=%d: %w", i, err)
				return
			}
			if i%3 == 0 {
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	for w := 0; w < writers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < writerOps; i++ {
				id := uint64((w*997+i*31+i*i)%260 + 1)
				switch (w + i) % 3 {
				case 0:
					rec := &Rec{
						Meta:     Meta{Country: countries[(w+i)%len(countries)]},
						Name:     fmt.Sprintf("rw-%02d-%04d-%03d", w, i, id),
						Age:      18 + ((w*13 + i) % 65),
						Score:    float64((w*1000+i*17)%1200) / 10.0,
						Active:   (w+i)%2 == 0,
						Tags:     []string{fmt.Sprintf("w%d", w%4), fmt.Sprintf("grp-%d", i%7)},
						FullName: fmt.Sprintf("FN-%05d", id),
					}
					if err := db.Set(id, rec); err != nil && !errors.Is(err, ErrRebuildInProgress) {
						errCh <- fmt.Errorf("writer=%d Set(id=%d): %w", w, id, err)
						return
					}
				case 1:
					patch := []Field{
						{Name: "age", Value: 21 + ((w*7 + i) % 60)},
						{Name: "active", Value: i%2 == 0},
						{Name: "country", Value: countries[(w+i)%len(countries)]},
					}
					if err := db.PatchIfExists(id, patch); err != nil && !errors.Is(err, ErrRebuildInProgress) {
						errCh <- fmt.Errorf("writer=%d PatchIfExists(id=%d): %w", w, id, err)
						return
					}
				default:
					if err := db.Delete(id); err != nil && !errors.Is(err, ErrRebuildInProgress) {
						errCh <- fmt.Errorf("writer=%d Delete(id=%d): %w", w, id, err)
						return
					}
				}
			}
		}()
	}

	for r := 0; r < readers; r++ {
		r := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < readerOps; i++ {
				q := queries[(r+i)%len(queries)]
				if _, err := db.QueryKeys(q); err != nil && !errors.Is(err, ErrRebuildInProgress) {
					errCh <- fmt.Errorf("reader=%d QueryKeys(i=%d): %w", r, i, err)
					return
				}
				if _, err := db.Query(q); err != nil && !errors.Is(err, ErrRebuildInProgress) {
					errCh <- fmt.Errorf("reader=%d Query(i=%d): %w", r, i, err)
					return
				}
				if _, err := db.Count(q); err != nil && !errors.Is(err, ErrRebuildInProgress) {
					errCh <- fmt.Errorf("reader=%d Count(i=%d): %w", r, i, err)
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		t.FailNow()
	}

	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex(final): %v", err)
	}

	checkQueries := []*qx.QX{
		qx.Query(qx.GTE("age", 18)).By("age", qx.DESC).Skip(5).Max(70),
		qx.Query(qx.HASANY("tags", []string{"w0", "w1", "grp-3", "seed"})).ByArrayCount("tags", qx.ASC).Skip(4).Max(90),
		qx.Query(qx.IN("country", []string{"NL", "DE", "US"})).ByArrayPos("country", []string{"DE", "NL", "US", "PL"}, qx.ASC).Max(120),
		qx.Query(qx.NOT(qx.EQ("active", false))).ByArrayPos("tags", []string{"w0", "w1", "seed", "grp-1"}, qx.DESC).Skip(2).Max(85),
	}
	for i, q := range checkQueries {
		got, err := db.QueryKeys(q)
		if err != nil {
			t.Fatalf("check QueryKeys(%d): %v", i, err)
		}
		want, err := expectedKeysUint64(t, db, q)
		if err != nil {
			t.Fatalf("check expectedKeysUint64(%d): %v", i, err)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("check mismatch(%d): got=%v want=%v q=%+v", i, got, want, q)
		}
	}

	var seqCount uint64
	if err := db.SeqScan(0, func(_ uint64, _ *Rec) (bool, error) {
		seqCount++
		return true, nil
	}); err != nil {
		t.Fatalf("SeqScan(final): %v", err)
	}
	cnt, err := db.Count(nil)
	if err != nil {
		t.Fatalf("Count(nil): %v", err)
	}
	if cnt != seqCount {
		t.Fatalf("final count mismatch: count=%d seqscan=%d", cnt, seqCount)
	}
}

func TestRebuildIndex_StatsBlockUntilRebuildCompletes(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Age: 30, Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	scanStarted := make(chan struct{})
	releaseScan := make(chan struct{})
	scanDone := make(chan error, 1)
	go func() {
		scanDone <- db.SeqScan(0, func(_ uint64, _ *Rec) (bool, error) {
			close(scanStarted)
			<-releaseScan
			return false, nil
		})
	}()

	select {
	case <-scanStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("scan did not start in time")
	}

	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- db.RebuildIndex()
	}()

	deadline := time.Now().Add(2 * time.Second)
	sawBusy := false
	for time.Now().Before(deadline) {
		_, err := db.Get(1)
		if errors.Is(err, ErrRebuildInProgress) {
			sawBusy = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !sawBusy {
		close(releaseScan)
		<-scanDone
		<-rebuildDone
		t.Fatal("expected rebuild to become active")
	}

	statsDone := make(chan Stats[uint64], 1)
	go func() {
		statsDone <- db.Stats()
	}()

	select {
	case st := <-statsDone:
		close(releaseScan)
		<-scanDone
		<-rebuildDone
		t.Fatalf("Stats returned while rebuild still active: %+v", st)
	case <-time.After(80 * time.Millisecond):
	}

	close(releaseScan)
	if err := <-scanDone; err != nil {
		t.Fatalf("SeqScan: %v", err)
	}
	if err := <-rebuildDone; err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	select {
	case st := <-statsDone:
		if st.Index.Size == 0 {
			t.Fatalf("expected IndexStats.Size > 0, got %+v", st.Index)
		}
		if st.Snapshot.TxID == 0 {
			t.Fatalf("expected SnapshotStats.TxID > 0, got %+v", st.Snapshot)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stats did not return after rebuild completion")
	}
}

func TestRebuildIndex_RejectsCoreOpsWhileActive(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Age: 30, Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	scanStarted := make(chan struct{})
	releaseScan := make(chan struct{})
	scanDone := make(chan error, 1)
	go func() {
		scanDone <- db.SeqScan(0, func(_ uint64, _ *Rec) (bool, error) {
			close(scanStarted)
			<-releaseScan
			return false, nil
		})
	}()

	select {
	case <-scanStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("scan did not start in time")
	}

	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- db.RebuildIndex()
	}()

	deadline := time.Now().Add(2 * time.Second)
	sawBusy := false
	for time.Now().Before(deadline) {
		_, err := db.Get(1)
		if errors.Is(err, ErrRebuildInProgress) {
			sawBusy = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !sawBusy {
		close(releaseScan)
		<-scanDone
		<-rebuildDone
		t.Fatal("expected rebuild to become active")
	}

	expectBusy := func(op string, err error) {
		t.Helper()
		if !errors.Is(err, ErrRebuildInProgress) {
			t.Fatalf("%s: expected ErrRebuildInProgress, got: %v", op, err)
		}
	}

	_, err := db.Get(1)
	expectBusy("Get", err)

	_, err = db.BatchGet(1, 2)
	expectBusy("BatchGet", err)

	err = db.ScanKeys(0, func(_ uint64) (bool, error) {
		return true, nil
	})
	expectBusy("ScanKeys", err)

	err = db.SeqScan(0, func(_ uint64, _ *Rec) (bool, error) {
		return true, nil
	})
	expectBusy("SeqScan", err)

	err = db.SeqScanRaw(0, func(_ uint64, _ []byte) (bool, error) {
		return true, nil
	})
	expectBusy("SeqScanRaw", err)

	_, err = db.QueryKeys(qx.Query())
	expectBusy("QueryKeys", err)

	_, err = db.Query(qx.Query())
	expectBusy("Query", err)

	_, err = db.Count(nil)
	expectBusy("Count", err)

	err = db.Set(2, &Rec{Name: "bob", Age: 31})
	expectBusy("Set", err)

	err = db.BatchSet(
		[]uint64{2, 3},
		[]*Rec{
			{Name: "setmany-2", Age: 21},
			{Name: "setmany-3", Age: 22},
		},
	)
	expectBusy("BatchSet", err)

	err = db.PatchIfExists(1, []Field{{Name: "age", Value: 35}})
	expectBusy("PatchIfExists", err)

	err = db.BatchPatch([]uint64{1, 2}, []Field{{Name: "age", Value: 37}})
	expectBusy("BatchPatch", err)

	err = db.Delete(1)
	expectBusy("Delete", err)

	err = db.BatchDelete([]uint64{1, 2})
	expectBusy("BatchDelete", err)

	err = db.Truncate()
	expectBusy("Truncate", err)

	err = db.RebuildIndex()
	expectBusy("RebuildIndex(second)", err)

	close(releaseScan)
	if err = <-scanDone; err != nil {
		t.Fatalf("SeqScan: %v", err)
	}
	if err = <-rebuildDone; err != nil {
		t.Fatalf("RebuildIndex(first): %v", err)
	}
}
